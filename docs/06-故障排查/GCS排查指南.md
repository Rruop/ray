# Ray GCS 问题排查指南

> 相关文档：[RaySyncer 资源同步机制与配置参数深度分析](./ray-syncer-resource-sync-mechanism.md)

## 问题背景

在一个 800+ 节点的 Ray 集群中，观察到以下现象：
1. Actor 启动后 dead，日志显示："The actor is dead because all references to the actor were removed including lineage ref count"
2. 节点被误判死亡："health check failed due to missing too many heartbeats"
3. Actor 调度缓慢，大量 lease 重试

---

## 一、问题现象分析

### 1.1 Actor 死亡日志

```
Exit Detail: The actor is dead because all references to the actor were removed including lineage ref count.
```

**初步分析**：这通常是 Ray 引用计数垃圾回收机制导致的，但结合后续日志发现实际原因是节点故障。

### 1.2 节点死亡日志

```
[2026-04-30 08:50:10,102] accessor.cc:436: Received address and liveness notification for node, IsAlive = 0 node_id=049310...
[2026-04-30 08:50:10,102] core_worker.cc:751: Node failure. All objects pinned on that node will be lost...
[2026-04-30 08:50:17,283] core_worker_shutdown_executor.cc:123: Executing worker exit: INTENDED_SYSTEM_EXIT - Worker exits because the actor is killed.
```

**关键发现**：短时间内多个节点连续失败，说明是集群级别问题。

### 1.3 心跳延迟告警

```
(2) raylet has lagging heartbeats due to slow network or busy workload.
(2) raylet has lagging heartbeats due to slow network or busy workload.
(2) raylet has lagging heartbeats due to slow network or busy workload.
```

**含义**：这是 GCS 在解释节点死亡原因时列出的可能原因，多个节点同时出现说明更可能是系统级问题。

---

## 二、GCS 负载分析

### 2.1 GCS 进程资源占用

```bash
top -p $(pgrep -f gcs_server)
```

**结果**：
```
PID USER  PR  NI    VIRT    RES    SHR S  %CPU  %MEM     TIME+ COMMAND
 26 root  20   0  163.9g  16.6g  18776 R 750.2   3.3 521:49.94 gcs_server
```

| 指标 | 值 | 说明 |
|------|-----|------|
| GCS CPU | **750%** | 使用了 ~7.5 个核心 |
| GCS 内存 | **16.6 GB** | 对于大集群偏高 |
| 系统空闲 | 89.1% | 机器本身不忙，但 GCS 进程很忙 |
| Head 节点核数 | 79 | 配置了 64 线程但只用了 7.5 核 |

### 2.2 GCS 线程分析

```bash
ps -T -p $(pgrep -f gcs_server) -o tid,comm,%cpu | sort -k3 -rn | head -30
```

**结果**：
```
TID   COMMAND         %CPU
103   ray_syncer_io_c 89.2   ← 状态同步线程，单线程瓶颈
78    gcs_server      20.5   ← 主线程
101   task_io_context 10.6   ← 任务 IO
102   pubsub_io_conte 10.4   ← 发布订阅
215   server.poll27    3.0   ← gRPC 线程
171   nexting_thread   2.7   ← gRPC 相关
...   server.poll*     2-3%  ← gRPC 服务器线程
```

**关键发现**：
1. `ray_syncer_io_c` 单线程打满 89.2%，是最大瓶颈
2. gRPC 线程（`server.poll*`）负载很低，每个只有 2-3%
3. 心跳处理能力是足够的，瓶颈在 syncer

---

## 三、GCS 线程架构详解

### 3.1 io_context 分配策略

根据 `gcs_server_io_context_policy.h`：

```cpp
// 专用 io_context（独立线程）
constexpr static std::array<std::string_view, 4> kAllDedicatedIOContextNames{
    "task_io_context",        // GcsTaskManager
    "pubsub_io_context",      // GcsPublisher
    "ray_syncer_io_context",  // RaySyncer
    "ray_event_io_context"    // RayEventRecorder
};

// 其他组件使用默认 io_context（主线程）
```

### 3.2 线程职责分配

| 线程名 | io_context | 职责 |
|--------|------------|------|
| `ray_syncer_io_c` | `ray_syncer_io_context` | 集群状态同步（gossip 协议） |
| `task_io_context` | `task_io_context` | GcsTaskManager 任务管理 |
| `pubsub_io_conte` | `pubsub_io_context` | GcsPublisher 发布订阅 |
| `gcs_server` (主线程) | `GetDefaultIOContext()` | 核心业务逻辑 |
| `server.poll*` | gRPC 线程池 | 接收/发送 RPC 请求 |
| `nexting_thread` | gRPC 相关 | gRPC 内部处理 |

### 3.3 主线程处理的组件

| 组件 | 职责 |
|------|------|
| `GcsHealthCheckManager` | 健康检查、判定节点死亡 |
| `GcsNodeManager` | 节点注册/注销、节点状态管理 |
| `GcsActorManager` | Actor 生命周期管理 |
| `GcsActorScheduler` | Actor 调度、lease worker |
| `GcsResourceManager` | 资源视图管理 |
| `GcsPlacementGroupManager` | Placement Group 管理 |
| `GcsJobManager` | Job 管理 |
| `ClusterResourceScheduler` | 集群资源调度 |
| `GcsTableStorage` | GCS 表存储操作 |

### 3.4 线程间关系

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              GCS Server                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐       │
│  │ server.poll*    │     │ server.poll*    │     │ server.poll*    │       │
│  │ (gRPC 线程 1)   │     │ (gRPC 线程 2)   │     │ (gRPC 线程 N)   │       │
│  └────────┬────────┘     └────────┬────────┘     └────────┬────────┘       │
│           │                       │                       │                 │
│           └───────────────────────┼───────────────────────┘                 │
│                                   │                                         │
│                                   ↓                                         │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                     主线程 (gcs_server, 默认 io_context)              │  │
│  │                                                                       │  │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐      │  │
│  │  │ GcsHealthCheck  │  │ GcsActorManager │  │ GcsNodeManager  │      │  │
│  │  │ Manager         │  │ GcsActorScheduler│  │                 │      │  │
│  │  └─────────────────┘  └─────────────────┘  └─────────────────┘      │  │
│  │                                                                       │  │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐      │  │
│  │  │ GcsResourceMgr  │  │ GcsPlacementGrp │  │ GcsJobManager   │      │  │
│  │  │                 │  │ Manager         │  │                 │      │  │
│  │  └─────────────────┘  └─────────────────┘  └─────────────────┘      │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐       │
│  │ray_syncer   │  │task_io      │  │pubsub_io    │  │ray_event    │       │
│  │_io_context  │  │_context     │  │_context     │  │_io_context  │       │
│  │(独立线程)   │  │(独立线程)   │  │(独立线程)   │  │(独立线程)   │       │
│  │             │  │             │  │             │  │             │       │
│  │ RaySyncer   │  │GcsTaskMgr   │  │GcsPublisher │  │EventRecorder│       │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘       │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 四、心跳机制分析

### 4.1 心跳流程

```
Raylet (Worker 节点)                    GCS (Head 节点)
       │                                      │
       │──── 心跳 RPC ────────────────────────→│ server.poll* 接收
       │                                      │      │
       │                                      │      ↓
       │                                      │ 主线程处理
       │                                      │ GcsHealthCheckManager
       │←──── 响应 ───────────────────────────│
       │                                      │
```

### 4.2 健康检查配置

```python
# 相关配置参数
RAY_raylet_heartbeat_period_ms=1000        # 心跳间隔 1s
RAY_num_heartbeats_timeout=30              # 30次超时判定死亡
RAY_health_check_period_ms=10000           # 健康检查间隔 10s
```

### 4.3 节点死亡判定流程

```
GcsHealthCheckManager 定时检查
       │
       ↓
向 Raylet 发送健康检查请求
       │
       ├── 响应正常 → 重置计数器
       │
       └── 响应失败 → remaining_checks--
              │
              ↓
       remaining_checks == 0 ?
              │
              ├── 否 → 等待下次检查
              │
              └── 是 → 标记节点死亡
                       │
                       ↓
                 GcsNodeManager.OnNodeFailure()
                       │
                       ↓
                 销毁该节点上的 Actor
                 重新调度 Placement Group
```

### 4.4 两种心跳相关日志的区别

| 日志 | 含义 | 场景 |
|------|------|------|
| `lagging heartbeats` | 心跳延迟，但节点可能还活着 | 网络慢或负载高 |
| `Connection refused` | 节点完全无响应 | Raylet 进程已死（OOM等） |
| `health check failed` | 健康检查多次失败后判死 | 最终结果 |

---

## 五、Actor 调度机制分析

### 5.1 Actor 创建流程

```
┌─────────────────────────────────────────────────────────────────────┐
│  Schedule(actor)                                                     │
│    └─ SelectForwardingNode() → 选择 owner 节点                       │
│       └─ LeaseWorkerFromNode(actor, owner_node)                     │
│          打印: "Leasing worker for actor ... node_id=owner..."      │
└─────────────────────────────────┬───────────────────────────────────┘
                                  ↓
┌─────────────────────────────────────────────────────────────────────┐
│  Owner 节点处理 lease 请求                                           │
│    - 选择一个 spillback 节点，返回其地址                             │
│    - reply.rejected = false                                         │
│    - reply.worker_address = 空                                      │
│    - reply.retry_at_raylet_address = spillback 节点地址             │
└─────────────────────────────────┬───────────────────────────────────┘
                                  ↓
┌─────────────────────────────────────────────────────────────────────┐
│  HandleWorkerLeaseReply()                                            │
│    - reply.rejected = false                                         │
│    - 打印: "Finished leasing worker from owner..."                  │
│    - 调用 HandleWorkerLeaseGrantedReply()                           │
│       - worker_address.empty() == true                              │
│       - 继续去 spillback 节点 lease                                 │
└─────────────────────────────────┬───────────────────────────────────┘
                                  ↓
┌─────────────────────────────────────────────────────────────────────┐
│  Spillback 节点处理 lease 请求                                       │
│    - GrantOrReject=true，必须明确回复                               │
│    - 检查本地资源                                                    │
│    - 资源够 → reply.rejected=false, worker_address=有效地址         │
│    - 资源不够 → reply.rejected = true                               │
└─────────────────────────────────┬────────────────────────────────────┘
                                  │
         ┌────────────────────────┴────────────────────────┐
         ↓                                                  ↓
    资源不够                                            资源够
         │                                                  │
         ↓                                                  ↓
"Failed to lease...                               "Finished leasing..."
 resources not enough"                            "Submitting actor creation task"
         │                                                  │
         ↓                                                  ↓
  Reschedule → 回到 owner                          CreateActorOnWorker
                                                           │
                                                           ↓
                                                  "Actor created successfully"
```

### 5.2 关键日志含义

| 日志 | 代码位置 | 含义 |
|------|----------|------|
| `Leasing worker for actor ...` | :239 | 开始对某节点发起 lease |
| `Finished leasing worker from ...` | :583 | lease RPC 完成，**但不一定拿到 worker** |
| `Failed to lease ... resources are not enough` | :577 | 该节点资源不足，reject |
| `Submitting actor creation task to worker` | :385 | **真正拿到 worker**，开始创建 |
| `Actor creation task succeeded` | :424 | 创建成功 |
| `Actor created successfully` | :1645 | Actor 最终创建成功 |

### 5.3 为什么 "Finished leasing" 后还会继续 lease？

**关键代码** (`gcs_actor_scheduler.cc:296-325`)：

```cpp
void GcsActorScheduler::HandleWorkerLeaseGrantedReply(...) {
  const auto &worker_address = reply.worker_address();

  if (worker_address.node_id().empty()) {
    // ⚠️ worker_address 是空的！
    // 虽然打印了 "Finished leasing"，但实际上没拿到 worker
    // 只是拿到了一个 spillback 节点地址
    // 继续去 spillback 节点 lease
    LeaseWorkerFromNode(actor, spill_back_node);
  } else {
    // ✅ worker_address 不为空，真正拿到了 worker
    // 才会进入 CreateActorOnWorker
    CreateActorOnWorker(actor, leased_worker);
  }
}
```

### 5.4 调度问题日志实例

```
19:19:16,132 ─ 注册 Actor b333205b...
     │
     ├─ 19:19:16 ~ 19:19:27 (约 11 秒)
     │    反复尝试 lease worker:
     │    - 500d8a37... → Finished (但没真正分配)
     │    - 其他节点 → Failed: resources not enough
     │    - 循环 20+ 次...
     │
19:19:27,900 ─ 尝试节点 e47e0ecc...
     │
19:19:29,122 ─ 从 e47e0ecc... 成功 lease worker
     │         └─ Submitting actor creation task
     │
19:19:41,754 ─ Actor 创建成功 ✓
     │         (从注册到成功: 25 秒)
```

---

## 六、节点死亡案例分析

### 6.1 完整时间线

```
19:19:41 ─ Actor 创建成功在节点 10.48.35.79

     ══════ 正常运行 16 分钟 ══════

19:35:59 ─ ⚠️ 开始异常：无法检查 worker 状态
           "Failed to check if worker is dead"

19:36:08 ─ ❌ 第一次健康检查失败
           "Connection refused" to 10.48.35.79:42541
           remaining checks: 9

19:36:09 ─ ray_syncer 连接断开
           "Connection is broken"

19:36:08 ~ 19:37:42 ─ 健康检查持续失败（每 10 秒一次）
           remaining: 9 → 8 → 7 → 6 → 5 → 4 → 3 → 2 → 1 → 0

19:37:42 ─ 💀 节点被标记为死亡
           - death reason = UNEXPECTED_TERMINATION
           - death message = health check failed due to missing too many heartbeats
           - 重建 7 个 Actor
           - 重新调度 Placement Groups
```

### 6.2 节点死亡原因判定

| 原因 | 日志特征 | 验证方式 |
|------|----------|----------|
| **Raylet OOM/崩溃** | `Connection refused` | 查看节点 `dmesg` |
| **心跳延迟** | 只有 `health check failed` | 检查网络和负载 |
| **K8s 驱逐** | K8s events 显示 Evicted | `kubectl describe pod` |
| **抢占** | `preempted = 1` | 检查日志 |

### 6.3 排查命令

```bash
# 1. 查看该节点的死亡原因
grep "<node_id>" /tmp/ray/session_latest/logs/gcs_server.out | \
  grep -E "dead|fail|timeout|heartbeat|Connection"

# 2. 在问题节点上查看 OOM
dmesg | grep -i "oom\|kill" | tail -20

# 3. 查看 raylet 退出日志
grep -E "exit|signal|fatal|abort" /tmp/ray/session_latest/logs/raylet.out

# 4. K8s 环境查看事件
kubectl get events --sort-by='.lastTimestamp' | grep -E "Evict|OOM|Kill"
```

---

## 七、Syncer 与心跳的关系

### 7.1 是否互相影响？

**直接影响：不会**

```
ray_syncer_io_c (89%)     心跳处理 (GcsHealthCheckManager)
        │                          │
        ↓                          ↓
  独立线程/io_context        主线程/默认 io_context
        │                          │
        └────── 不同线程 ───────────┘
```

### 7.2 可能的间接影响

| 间接影响路径 | 说明 |
|-------------|------|
| **资源视图延迟** | syncer 慢 → 资源视图过时 → 调度选错节点 → 大量 lease 重试 |
| **主线程过载** | 大量调度请求 → 主线程忙 → 健康检查回调延迟 |
| **CPU 竞争** | 多线程竞争 CPU 资源 |

### 7.3 syncer 打满导致的级联问题

```
ray_syncer_io_c 打满 (89%)
        ↓
资源视图同步延迟（秒级）
        ↓
GCS 基于过时的资源信息调度
        ↓
Owner 节点选错 spillback 候选
        ↓
实际 lease 时发现资源不足
        ↓
反复失败，消耗大量时间和 RPC
        ↓
主线程处理大量调度请求
        ↓
间接影响健康检查回调处理
```

---

## 八、配置参数说明

### 8.1 当前配置

```json
{
  "raylet_report_resources_period_milliseconds": 500,
  "ray_syncer_message_refresh_interval_ms": 10000,
  "health_check_period_ms": 10000,
  "gcs_server_rpc_server_thread_num": 64,
  "scheduler_avoid_gpu_nodes": false
}
```

### 8.2 参数调整建议

| 参数 | 当前值 | 建议值 | 影响 |
|------|--------|--------|------|
| `raylet_report_resources_period_milliseconds` | 500 | 1000 | 减少 GCS 请求量 |
| `ray_syncer_message_refresh_interval_ms` | 10000 | 5000-20000 | 平衡准确性和负载 |
| `num_heartbeats_timeout` | 30 | 60 | 减少误判死亡 |
| `health_check_period_ms` | 10000 | 10000 | 保持 |

### 8.3 参数调整的权衡

```
                 稳定性                    实时性
                   ↑                         ↑
   放宽间隔/超时 ──────────────────────────────── 收紧间隔/超时
                   │                         │
           减少 GCS 压力              资源视图精准
           减少误判死亡              快速检测故障
           集群更稳定                调度更准确
```

---

## 九、排查命令速查

### 9.1 GCS 状态检查

```bash
# 查看 GCS 进程 CPU/内存
ps aux | grep gcs_server

# 查看 GCS 线程分布
ps -T -p $(pgrep -f gcs_server) -o tid,comm,%cpu | sort -k3 -rn | head -30

# 查看 GCS 压力日志
grep -E "slow|timeout|backlog|lag|took [0-9]{3,}ms" \
  /tmp/ray/session_latest/logs/gcs_server.out | tail -50
```

### 9.2 心跳相关

```bash
# 查看心跳延迟日志
grep "lagging heartbeats" /tmp/ray/session_latest/logs/gcs_server.out

# 查看节点死亡日志
grep -E "Node.*dead|marked dead" /tmp/ray/session_latest/logs/gcs_server.out

# 统计心跳延迟出现频率
grep "lagging heartbeats" /tmp/ray/session_latest/logs/gcs_server.out | \
  cut -d',' -f1 | cut -d':' -f1-2 | uniq -c
```

### 9.3 Actor 调度相关

```bash
# 查看 Actor 调度日志
grep -E "Leasing worker|Finished leasing|Failed to lease|Submitting actor" \
  /tmp/ray/session_latest/logs/gcs_server.out | tail -100

# 统计 lease 失败次数
grep "Failed to lease.*resources are not enough" \
  /tmp/ray/session_latest/logs/gcs_server.out | wc -l

# 查看特定 Actor 的完整生命周期
grep "<actor_id>" /tmp/ray/session_latest/logs/gcs_server.out
```

### 9.4 节点状态

```bash
# 查看特定节点的所有日志
grep "<node_id>" /tmp/ray/session_latest/logs/gcs_server.out

# 查看节点存活状态
ray status

# 通过 API 查看
curl http://<head_node>:8265/api/cluster_status
```

---

## 十、优化建议

### 10.1 短期优化（配置调整）

```json
{
  // 减少资源上报频率
  "raylet_report_resources_period_milliseconds": 1000,

  // 放宽心跳超时
  "num_heartbeats_timeout": 60,

  // 调整 syncer 刷新间隔（权衡准确性和负载）
  "ray_syncer_message_refresh_interval_ms": 15000
}
```

### 10.2 中期优化（架构调整）

1. **使用 Placement Group 预留资源**
   ```python
   pg = placement_group([{"CPU": 1}] * num_actors, strategy="SPREAD")
   ray.get(pg.ready())
   actors = [MyActor.options(placement_group=pg).remote() for _ in range(num_actors)]
   ```

2. **批量创建 Actor 时降低并发**
   ```python
   batch_size = 10
   for i in range(0, num_actors, batch_size):
       batch = [MyActor.remote() for _ in range(min(batch_size, num_actors - i))]
       ray.get([a.ready.remote() for a in batch])
   ```

3. **使用 Actor Pool**
   ```python
   from ray.util.actor_pool import ActorPool
   pool = ActorPool([MyActor.remote() for _ in range(n)])
   ```

### 10.3 长期优化

1. **拆分集群**：800+ 节点拆成 2-3 个小集群
2. **升级 Ray 版本**：关注 syncer 优化相关的更新
3. **GCS 高可用部署**：使用外部 Redis 作为存储后端

---

## 十一、总结

### 11.1 问题根因

1. **syncer 单线程瓶颈**：`ray_syncer_io_c` 打满 89%，导致资源视图同步延迟
2. **资源视图不准确**：调度时选错节点，大量 lease 失败重试
3. **节点误判死亡**：部分因 GCS 压力间接导致，部分因节点自身问题（OOM 等）

### 11.2 关键结论

| 问题 | 结论 |
|------|------|
| GCS CPU 高但用不满 | syncer 单线程瓶颈，不是线程数问题 |
| gRPC 线程不忙 | 心跳处理能力够，不是直接原因 |
| syncer 影响心跳？ | 不直接影响，但间接通过调度压力影响 |
| 主线程处理什么？ | 健康检查、Actor 调度、节点管理等核心逻辑 |
| "Finished leasing" 含义 | 只是 RPC 完成，不代表拿到 worker |

### 11.3 排查思路

```
1. 确认现象
   └─ 查看 Actor 死亡日志、节点死亡日志

2. 分析 GCS 负载
   └─ 查看 CPU、内存、线程分布

3. 定位瓶颈
   └─ 确认是 syncer、主线程还是 gRPC 线程

4. 分析心跳机制
   └─ 确认是 GCS 端还是 Raylet 端问题

5. 分析调度流程
   └─ 查看 lease 日志，确认是资源不足还是视图过时

6. 定位具体原因
   └─ Connection refused → 节点问题
   └─ lagging heartbeats → 系统级问题

7. 制定优化方案
   └─ 配置调整 / 架构优化 / 业务层优化
```

---

## 附录A：关键源码位置

| 文件 | 关键函数/行号 | 说明 |
|------|--------------|------|
| `gcs_actor_scheduler.cc:239` | `LeaseWorkerFromNode` | 打印 "Leasing worker" |
| `gcs_actor_scheduler.cc:577` | `HandleWorkerLeaseReply` | 打印 "Failed to lease" |
| `gcs_actor_scheduler.cc:583` | `HandleWorkerLeaseReply` | 打印 "Finished leasing" |
| `gcs_actor_scheduler.cc:296` | `HandleWorkerLeaseGrantedReply` | 判断是否真正拿到 worker |
| `gcs_actor_scheduler.cc:385` | `CreateActorOnWorker` | 打印 "Submitting actor creation task" |
| `gcs_health_check_manager.cc:83` | `FailNode` | 打印 "Node is dead" |
| `gcs_node_manager.cc:686` | `InternalOnNodeFailure` | 处理节点失败 |
| `gcs_node_manager.cc:539` | `InferDeathInfo` | 推断节点死亡原因 |
| `gcs_server_io_context_policy.h:31` | `GcsServerIOContextPolicy` | io_context 分配策略 |

---

## 附录B：详细源码逻辑分析

### B.1 io_context 分配策略完整代码

**文件**: `src/ray/gcs/gcs_server_io_context_policy.h`

```cpp
struct GcsServerIOContextPolicy {
  GcsServerIOContextPolicy() = delete;

  // IOContext name for each handler.
  // If a class needs a dedicated io context, it should be specialized here.
  // If a class does NOT have a dedicated io context, returns -1;
  template <typename T>
  static constexpr int GetDedicatedIOContextIndex() {
    if constexpr (std::is_same_v<T, GcsTaskManager>) {
      return IndexOf("task_io_context");           // 索引 0
    } else if constexpr (std::is_same_v<T, pubsub::GcsPublisher>) {
      return IndexOf("pubsub_io_context");         // 索引 1
    } else if constexpr (std::is_same_v<T, syncer::RaySyncer>) {
      return IndexOf("ray_syncer_io_context");     // 索引 2
    } else if constexpr (std::is_same_v<T, observability::RayEventRecorder>) {
      return IndexOf("ray_event_io_context");      // 索引 3
    } else {
      return -1;  // ← 返回 -1 表示使用默认 io_context（主线程）
    }
  }

  // 专用 io_context 名称列表（每个对应一个独立线程）
  constexpr static std::array<std::string_view, 4> kAllDedicatedIOContextNames{
      "task_io_context",        // GcsTaskManager 专用
      "pubsub_io_context",      // GcsPublisher 专用
      "ray_syncer_io_context",  // RaySyncer 专用 ← 打满 89%的线程
      "ray_event_io_context"    // RayEventRecorder 专用
  };

  // 所有专用 io_context 都启用延迟探测
  constexpr static std::array<bool, 4> kAllDedicatedIOContextEnableLagProbe{
      true, true, true, true
  };
};
```

**关键点**：
- 只有 4 个组件有专用线程
- 其他所有组件（包括 GcsHealthCheckManager、GcsActorScheduler）都使用默认 io_context
- 默认 io_context 对应主线程 `gcs_server`

---

### B.2 GcsHealthCheckManager 初始化与回调链

**文件**: `src/ray/gcs/gcs_server.cc:367-389`

```cpp
void GcsServer::InitGcsHealthCheckManager(const GcsInitData &gcs_init_data) {
  RAY_CHECK(gcs_node_manager_);

  // 定义节点死亡回调函数
  auto node_death_callback = [this](const NodeID &node_id) {
    // 关键：将回调 post 到默认 io_context（主线程）
    this->io_context_provider_.GetDefaultIOContext().post(
        [this, node_id] {
          return gcs_node_manager_->OnNodeFailure(node_id, nullptr);
        },
        "GcsServer.NodeDeathCallback");
  };

  // 创建 GcsHealthCheckManager，使用默认 io_context
  gcs_healthcheck_manager_ =
      GcsHealthCheckManager::Create(
          io_context_provider_.GetDefaultIOContext(),  // ← 使用主线程
          node_death_callback,
          metrics_.health_check_rpc_latency_ms_histogram);

  // 为所有存活节点添加健康检查
  for (const auto &item : gcs_init_data.Nodes()) {
    if (item.second.state() == rpc::GcsNodeInfo::ALIVE) {
      auto remote_address = rpc::RayletClientPool::GenerateRayletAddress(...);
      auto raylet_client = raylet_client_pool_.GetOrConnectByAddress(remote_address);
      gcs_healthcheck_manager_->AddNode(item.first, raylet_client->GetChannel());
    }
  }
}
```

**回调链分析**：
```
健康检查失败
    ↓
GcsHealthCheckManager::FailNode()          // 在主线程
    ↓
on_node_death_callback_(node_id)           // 调用回调
    ↓
io_context_provider_.GetDefaultIOContext().post(...)  // post 到主线程
    ↓
GcsNodeManager::OnNodeFailure()            // 在主线程执行
```

---

### B.3 健康检查核心逻辑

**文件**: `src/ray/gcs/gcs_health_check_manager.cc:122-227`

```cpp
void GcsHealthCheckManager::HealthCheckContext::StartHealthCheck() {
  using ::grpc::health::v1::HealthCheckResponse;

  auto manager = manager_.lock();
  if (manager == nullptr) {
    delete this;
    return;
  }

  RAY_CHECK(manager->thread_checker_.IsOnSameThread());  // 确保在正确线程

  // 如果被请求停止，直接销毁
  if (stopped_) {
    delete this;
    return;
  }

  // 检查最新健康状态时间戳，决定是否需要发送新的 RPC
  const auto now = absl::Now();
  absl::Time next_check_time =
      latest_known_healthy_timestamp_ + absl::Milliseconds(manager->period_ms_);

  if (now <= next_check_time) {
    // 上次更新足够新鲜，跳过本次检查，稍后重新调度
    int64_t next_schedule_millisec = (next_check_time - now) / absl::Milliseconds(1);
    timer_.expires_from_now(boost::posix_time::milliseconds(next_schedule_millisec));
    timer_.async_wait([this](auto) { StartHealthCheck(); });
    return;
  }

  // 创建 gRPC 上下文和响应对象
  auto context = std::make_shared<grpc::ClientContext>();
  auto response = std::make_shared<HealthCheckResponse>();
  auto *context_ptr = context.get();
  auto *response_ptr = response.get();

  // 设置超时时间
  const auto deadline = now + absl::Milliseconds(manager->timeout_ms_);
  context->set_deadline(absl::ToChronoTime(deadline));

  // 发起异步健康检查
  stub_->async()->Check(
      context_ptr,
      &request_,
      response_ptr,
      [this, start = now, context = std::move(context),
       response = std::move(response)](::grpc::Status status) {

        auto gcs_health_check_manager = manager_.lock();
        if (gcs_health_check_manager == nullptr) {
          delete this;
          return;
        }

        // ⚠️ 这个回调在 gRPC 线程池中执行
        // 记录 RPC 延迟指标
        gcs_health_check_manager->health_check_rpc_latency_ms_histogram_.Record(
            absl::ToInt64Milliseconds(absl::Now() - start));

        // 关键：将后续处理 post 回主线程
        gcs_health_check_manager->io_service_.post(
            [this, status, response = std::move(response)]() {
              if (stopped_) {
                delete this;
                return;
              }
              auto mgr = manager_.lock();
              if (mgr == nullptr) {
                delete this;
                return;
              }

              RAY_LOG(DEBUG) << "Health check status: "
                             << HealthCheckResponse_ServingStatus_Name(response->status());

              if (status.ok() && response->status() == HealthCheckResponse::SERVING) {
                // ✅ 健康检查通过，重置剩余检查次数
                health_check_remaining_ = mgr->failure_threshold_;
              } else {
                // ❌ 健康检查失败，减少剩余次数
                --health_check_remaining_;
                RAY_LOG(WARNING)
                    << "Health check failed for node " << node_id_
                    << ", remaining checks " << health_check_remaining_
                    << ", status " << status.error_code()
                    << ", response status " << response->status()
                    << ", status message " << status.error_message()
                    << ", status details " << status.error_details();
              }

              if (health_check_remaining_ == 0) {
                // 💀 剩余次数归零，标记节点死亡
                mgr->FailNode(node_id_);
                delete this;
              } else {
                // 调度下一次健康检查
                timer_.expires_from_now(
                    boost::posix_time::milliseconds(mgr->period_ms_));
                timer_.async_wait([this](auto) { StartHealthCheck(); });
              }
            },
            "HealthCheck");
      });
}

// 标记节点死亡
void GcsHealthCheckManager::FailNode(const NodeID &node_id) {
  RAY_LOG(WARNING).WithField(node_id)
      << "Node is dead because the health check failed.";
  RAY_CHECK(thread_checker_.IsOnSameThread());

  auto iter = health_check_contexts_.find(node_id);
  if (iter != health_check_contexts_.end()) {
    on_node_death_callback_(node_id);  // 调用死亡回调
    health_check_contexts_.erase(iter);
  }
}
```

---

### B.4 节点死亡原因推断逻辑

**文件**: `src/ray/gcs/gcs_node_manager.cc:539-565`

```cpp
rpc::NodeDeathInfo GcsNodeManager::InferDeathInfo(const NodeID &node_id) {
  auto iter = draining_nodes_.find(node_id);
  rpc::NodeDeathInfo death_info;
  bool expect_force_termination;

  if (iter == draining_nodes_.end()) {
    // 节点不在 draining 列表中
    expect_force_termination = false;
  } else if (iter->second->deadline_timestamp_ms() == 0) {
    // draining 没有设置截止时间
    expect_force_termination = false;
  } else {
    // 检查是否超过截止时间且是抢占类型
    expect_force_termination =
        (current_sys_time_ms() > iter->second->deadline_timestamp_ms()) &&
        (iter->second->reason() ==
         rpc::autoscaler::DrainNodeReason::DRAIN_NODE_REASON_PREEMPTION);
  }

  if (expect_force_termination) {
    // 抢占导致的强制终止
    death_info.set_reason(rpc::NodeDeathInfo::AUTOSCALER_DRAIN_PREEMPTED);
    death_info.set_reason_message(iter->second->reason_message());
    RAY_LOG(INFO).WithField(node_id) << "Node was forcibly preempted";
  } else {
    // ⚠️ 默认情况：UNEXPECTED_TERMINATION
    death_info.set_reason(rpc::NodeDeathInfo::UNEXPECTED_TERMINATION);
    death_info.set_reason_message(
        "health check failed due to missing too many heartbeats");  // ← 这就是日志中的消息
  }
  return death_info;
}
```

**节点死亡原因枚举**：
| 原因 | 含义 |
|------|------|
| `UNEXPECTED_TERMINATION` | 意外终止（心跳超时） |
| `AUTOSCALER_DRAIN_PREEMPTED` | 被抢占 |
| `AUTOSCALER_DRAIN_IDLE` | 空闲被回收 |

---

### B.5 节点失败处理完整流程

**文件**: `src/ray/gcs/gcs_node_manager.cc:686-717`

```cpp
void GcsNodeManager::InternalOnNodeFailure(
    const NodeID &node_id,
    const std::function<void()> &node_table_updated_callback) {

  auto maybe_node = GetAliveNodeFromCache(node_id);
  if (maybe_node.has_value()) {
    // 1. 推断死亡原因
    rpc::NodeDeathInfo death_info = InferDeathInfo(node_id);

    // 2. 从缓存移除节点
    auto node = RemoveNodeFromCache(
        node_id, death_info, rpc::GcsNodeInfo::DEAD, current_sys_time_ms());

    // 3. 添加到死亡节点缓存
    AddDeadNodeToCache(node);

    // 4. 准备增量更新信息
    rpc::GcsNodeInfo node_info_delta;
    node_info_delta.set_node_id(node->node_id());
    node_info_delta.set_state(node->state());
    node_info_delta.set_end_time_ms(node->end_time_ms());
    node_info_delta.mutable_death_info()->CopyFrom(node->death_info());

    // 5. 异步更新存储并发布通知
    auto on_done = [this, node_id, node_table_updated_callback,
                    node_info_delta = std::move(node_info_delta),
                    node](const Status &status) mutable {
      WriteNodeExportEvent(*node, /*is_register_event*/ false);
      if (node_table_updated_callback != nullptr) {
        node_table_updated_callback();
      }
      // 发布节点状态变更到 pubsub
      PublishNodeInfoToPubsub(node_id, node_info_delta);
    };

    gcs_table_storage_->NodeTable().Put(
        node_id, *node, {std::move(on_done), io_context_});
  } else if (node_table_updated_callback != nullptr) {
    node_table_updated_callback();
  }
}
```

**从缓存移除节点时的广播逻辑** (`RemoveNodeFromCache` 内部)：

```cpp
if (node_death_info.reason() == rpc::NodeDeathInfo::UNEXPECTED_TERMINATION) {
  // 广播警告到所有 driver
  std::string type = "node_removed";
  std::ostringstream error_message;
  error_message << "The node with node id: " << node_id
                << " and address: " << removed_node->node_manager_address()
                << " and node name: " << removed_node->node_name()
                << " has been marked dead because the detector"
                << " has missed too many heartbeats from it. This can happen when a "
                   "\t(1) raylet crashes unexpectedly (OOM, etc.) \n"
                << "\t(2) raylet has lagging heartbeats due to slow network or busy "
                   "workload.";

  RAY_EVENT(ERROR, "RAY_NODE_REMOVED")
          .WithField("node_id", node_id.Hex())
          .WithField("ip", removed_node->node_manager_address())
      << error_message.str();

  RAY_LOG(WARNING) << error_message.str();  // ← 这就是日志中看到的 lagging heartbeats 消息

  // 发布错误信息
  auto error_data = CreateErrorTableData(
      type, error_message.str(), absl::FromUnixMillis(current_time_ms()));
  gcs_publisher_->PublishError(node_id.Hex(), std::move(error_data));
}
```

---

### B.6 Actor 调度完整流程代码

#### B.6.1 Schedule 入口

**文件**: `src/ray/gcs/actor/gcs_actor_scheduler.cc:49-81`

```cpp
void GcsActorScheduler::Schedule(std::shared_ptr<GcsActor> actor) {
  // 确保 Actor 还没有绑定到节点和 worker
  RAY_CHECK(actor->GetNodeID().IsNil() && actor->GetWorkerID().IsNil());

  // 1. 选择转发节点（优先选择 owner 节点）
  auto node_id = SelectForwardingNode(actor);

  auto node = gcs_node_manager_.GetAliveNode(node_id);
  if (!node.has_value()) {
    // 没有可用节点，触发失败处理
    schedule_failure_handler_(std::move(actor),
                              rpc::RequestWorkerLeaseReply::SCHEDULING_FAILED,
                              "No available nodes to schedule the actor");
    return;
  }

  // 2. 更新 Actor 地址（绑定到选中的节点）
  rpc::Address address;
  address.set_node_id(node.value()->node_id());
  actor->UpdateAddress(address);

  // 3. 记录到 leasing 状态 map
  RAY_CHECK(node_to_actors_when_leasing_[actor->GetNodeID()]
                .emplace(actor->GetActorID())
                .second);

  // 4. 设置 GrantOrReject 为 false（owner 节点可以返回 spillback 地址）
  actor->SetGrantOrReject(false);

  // 5. 开始 lease worker
  LeaseWorkerFromNode(actor, node.value());
}
```

#### B.6.2 节点选择逻辑

**文件**: `src/ray/gcs/actor/gcs_actor_scheduler.cc:83-99`

```cpp
NodeID GcsActorScheduler::SelectForwardingNode(std::shared_ptr<GcsActor> actor) {
  std::shared_ptr<const rpc::GcsNodeInfo> node;

  // 如果 Actor 有资源需求，优先选择 owner 节点
  const auto &lease_spec = actor->GetLeaseSpecification();
  if (!lease_spec.GetRequiredResources().IsEmpty()) {
    auto maybe_node = gcs_node_manager_.GetAliveNode(actor->GetOwnerNodeID());
    node = maybe_node.has_value() ? maybe_node.value()
                                  : gcs_node_manager_.SelectRandomAliveNode();
  } else {
    // 没有资源需求，随机选择节点
    node = gcs_node_manager_.SelectRandomAliveNode();
  }

  return node ? NodeID::FromBinary(node->node_id()) : NodeID::Nil();
}
```

#### B.6.3 发起 Lease 请求

**文件**: `src/ray/gcs/actor/gcs_actor_scheduler.cc:234-271`

```cpp
void GcsActorScheduler::LeaseWorkerFromNode(
    std::shared_ptr<GcsActor> actor,
    std::shared_ptr<const rpc::GcsNodeInfo> node) {
  RAY_CHECK(actor && node);

  auto node_id = NodeID::FromBinary(node->node_id());

  // 日志：开始 lease
  RAY_LOG(INFO)
          .WithField(actor->GetActorID())
          .WithField(actor->GetActorID().JobId())
          .WithField(node_id)
      << "Leasing worker for actor.";  // ← 这就是 "Leasing worker for actor" 日志

  // 如果节点正在释放未使用的 worker，等待后重试
  if (nodes_of_releasing_unused_workers_.contains(node_id)) {
    RetryLeasingWorkerFromNode(actor, node);
    return;
  }

  // 构建远程地址
  rpc::Address remote_address;
  remote_address.set_node_id(node->node_id());
  remote_address.set_ip_address(node->node_manager_address());
  remote_address.set_port(node->node_manager_port());

  auto raylet_client = raylet_client_pool_.GetOrConnectByAddress(remote_address);

  // 生成唯一的 lease ID
  static uint32_t lease_id_counter = 0;
  actor->GetMutableLeaseSpec()->set_lease_id(
      LeaseID::FromWorker(WorkerID::FromRandom(), lease_id_counter++).Binary());

  // 发起异步 RPC 请求
  raylet_client->RequestWorkerLease(
      actor->GetLeaseSpecification().GetMessage(),
      actor->GetGrantOrReject(),  // false = 可以返回 spillback，true = 必须明确接受或拒绝
      [this, actor, node](const Status &status,
                          const rpc::RequestWorkerLeaseReply &reply) {
        HandleWorkerLeaseReply(actor, node, status, reply);  // 处理响应
      },
      0);
}
```

#### B.6.4 处理 Lease 响应（核心逻辑）

**文件**: `src/ray/gcs/actor/gcs_actor_scheduler.cc:519-599`

```cpp
void GcsActorScheduler::HandleWorkerLeaseReply(
    std::shared_ptr<GcsActor> actor,
    std::shared_ptr<const rpc::GcsNodeInfo> node,
    const Status &status,
    const rpc::RequestWorkerLeaseReply &reply) {

  auto node_id = NodeID::FromBinary(node->node_id());
  auto iter = node_to_actors_when_leasing_.find(node_id);

  if (iter != node_to_actors_when_leasing_.end()) {
    auto actor_iter = iter->second.find(actor->GetActorID());
    if (actor_iter == iter->second.end()) {
      // Actor 已被取消
      RAY_LOG(INFO).WithField(actor->GetActorID()).WithField(actor->GetActorID().JobId())
          << "Ignoring granted lease for canceled lease request.";
      // ... 清理逻辑
      return;
    }

    if (status.ok()) {
      if (reply.canceled()) {
        // 请求被取消
        HandleRequestWorkerLeaseCanceled(actor, node_id, reply.failure_type(),
                                         reply.scheduling_failure_message());
        return;
      }

      // 检查空响应情况
      if (reply.worker_address().node_id().empty() &&
          reply.retry_at_raylet_address().node_id().empty() && !reply.rejected()) {
        RAY_LOG(DEBUG) << "Actor " << actor->GetActorID()
                       << " creation task has been cancelled.";
        return;
      }

      // 从 leasing map 移除
      iter->second.erase(actor_iter);
      if (iter->second.empty()) {
        node_to_actors_when_leasing_.erase(iter);
      }

      if (reply.rejected()) {
        // ❌ 被拒绝（资源不足）
        RAY_LOG(INFO) << "Failed to lease worker from node " << node_id
                      << " for actor " << actor->GetActorID()
                      << " as the resources are not enough, job id = "
                      << actor->GetActorID().JobId();  // ← "Failed to lease" 日志
        HandleWorkerLeaseRejectedReply(actor, reply);
      } else {
        // ✅ 成功（但可能只是 spillback）
        RAY_LOG(INFO) << "Finished leasing worker from " << node_id
                      << " for actor " << actor->GetActorID()
                      << ", job id = " << actor->GetActorID().JobId();  // ← "Finished leasing" 日志
        HandleWorkerLeaseGrantedReply(actor, reply, node);  // ← 关键：进一步处理
      }
    } else {
      // RPC 失败，重试
      RetryLeasingWorkerFromNode(actor, node);
    }
  }
  // ... 其他清理逻辑
}
```

#### B.6.5 处理 Granted 响应（判断是否真正拿到 Worker）

**文件**: `src/ray/gcs/actor/gcs_actor_scheduler.cc:296-365`

```cpp
void GcsActorScheduler::HandleWorkerLeaseGrantedReply(
    std::shared_ptr<GcsActor> actor,
    const ray::rpc::RequestWorkerLeaseReply &reply,
    std::shared_ptr<const rpc::GcsNodeInfo> node) {

  const auto &retry_at_raylet_address = reply.retry_at_raylet_address();
  const auto &worker_address = reply.worker_address();

  // ⚠️ 关键判断：worker_address 是否为空
  if (worker_address.node_id().empty()) {
    // worker_address 为空 → 没有真正拿到 worker，只是拿到了 spillback 地址
    RAY_CHECK(!retry_at_raylet_address.node_id().empty());

    auto spill_back_node_id = NodeID::FromBinary(retry_at_raylet_address.node_id());
    auto maybe_spill_back_node = gcs_node_manager_.GetAliveNode(spill_back_node_id);

    if (maybe_spill_back_node.has_value()) {
      auto spill_back_node = maybe_spill_back_node.value();

      // 更新 Actor 地址为 spillback 节点
      actor->UpdateAddress(retry_at_raylet_address);

      RAY_CHECK(node_to_actors_when_leasing_[actor->GetNodeID()]
                    .emplace(actor->GetActorID())
                    .second);

      // 关键：设置 GrantOrReject = true
      // 这意味着 spillback 节点必须明确接受或拒绝，不能再返回另一个 spillback
      actor->SetGrantOrReject(true);

      // 继续去 spillback 节点 lease
      LeaseWorkerFromNode(actor, spill_back_node);  // ← 这就是为什么会继续 lease
    } else {
      // spillback 节点已死，重新调度
      actor->UpdateAddress(rpc::Address());
      actor->GetMutableActorTableData()->clear_resource_mapping();
      Schedule(actor);  // 回到起点重新调度
    }
  } else {
    // ✅ worker_address 不为空 → 真正拿到了 worker
    std::vector<rpc::ResourceMapEntry> resources;
    for (auto &resource : reply.resource_mapping()) {
      resources.emplace_back(resource);
      actor->GetMutableActorTableData()->add_resource_mapping()->CopyFrom(resource);
    }

    // 创建 leased worker 对象
    auto leased_worker = std::make_shared<GcsLeasedWorker>(
        worker_address, std::move(resources), actor->GetActorID());

    auto node_id = leased_worker->GetNodeID();
    RAY_CHECK(node_to_workers_when_creating_[node_id]
                  .emplace(leased_worker->GetWorkerID(), leased_worker)
                  .second);

    // 更新 Actor 地址
    actor->UpdateAddress(leased_worker->GetAddress());
    actor->GetMutableActorTableData()->set_pid(reply.worker_pid());
    actor->GetMutableTaskSpec()->set_lease_grant_timestamp_ms(current_sys_time_ms());

    // 记录调度延迟指标
    actor->GetCreationTaskSpecification().EmitTaskMetrics(
        scheduler_placement_time_ms_histogram_);

    // 确保 worker 连接已建立
    worker_client_pool_.GetOrConnect(leased_worker->GetAddress());

    // 持久化 Actor 信息到 GCS 表，然后创建 Actor
    gcs_actor_table_.Put(actor->GetActorID(),
                         actor->GetActorTableData(),
                         {[this, actor, leased_worker](Status status) {
                            RAY_CHECK_OK(status);
                            if (actor->GetState() == rpc::ActorTableData::DEAD) {
                              return;  // Actor 已被 kill
                            }
                            CreateActorOnWorker(actor, leased_worker);  // ← 真正创建 Actor
                          },
                          io_context_});
  }
}
```

#### B.6.6 在 Worker 上创建 Actor

**文件**: `src/ray/gcs/actor/gcs_actor_scheduler.cc:382-452`

```cpp
void GcsActorScheduler::CreateActorOnWorker(
    std::shared_ptr<GcsActor> actor,
    std::shared_ptr<GcsLeasedWorker> worker) {
  RAY_CHECK(actor && worker);

  // 日志：提交 Actor 创建任务
  RAY_LOG(INFO)
          .WithField(actor->GetActorID())
          .WithField(worker->GetWorkerID())
          .WithField(actor->GetNodeID())
          .WithField(actor->GetActorID().JobId())
      << "Submitting actor creation task to worker.";  // ← 这意味着真正拿到了 worker

  // 构建 PushTask 请求
  auto request = std::make_unique<rpc::PushTaskRequest>();
  request->set_intended_worker_id(worker->GetWorkerID().Binary());
  request->mutable_task_spec()->CopyFrom(
      actor->GetCreationTaskSpecification().GetMessage());

  // 复制资源映射
  google::protobuf::RepeatedPtrField<rpc::ResourceMapEntry> resources;
  for (auto resource : worker->GetLeasedResources()) {
    resources.Add(std::move(resource));
  }
  request->mutable_resource_mapping()->CopyFrom(resources);

  // 获取 worker client 并发送任务
  auto client = worker_client_pool_.GetOrConnect(worker->GetAddress());
  client->PushNormalTask(
      std::move(request),
      [this, actor, worker](Status status, const rpc::PushTaskReply &reply) {
        auto iter = node_to_workers_when_creating_.find(actor->GetNodeID());
        if (iter != node_to_workers_when_creating_.end()) {
          auto worker_iter = iter->second.find(actor->GetWorkerID());
          if (worker_iter != iter->second.end()) {
            if (status.ok()) {
              // 从 creating map 移除
              iter->second.erase(worker_iter);
              if (iter->second.empty()) {
                node_to_workers_when_creating_.erase(iter);
              }

              // ✅ Actor 创建成功
              RAY_LOG(INFO)
                      .WithField(actor->GetActorID())
                      .WithField(worker->GetWorkerID())
                      .WithField(actor->GetActorID().JobId())
                      .WithField(actor->GetNodeID())
                  << "Actor creation task succeeded.";  // ← 创建成功日志

              schedule_success_handler_(actor, reply);  // 调用成功回调
            } else {
              // 创建失败，重试
              RAY_LOG(INFO)
                      .WithField(actor->GetActorID())
                      .WithField(worker->GetWorkerID())
                      .WithField(actor->GetActorID().JobId())
                      .WithField(actor->GetNodeID())
                  << "Actor creation task failed, will be retried.";
              RetryCreatingActorOnWorker(actor, worker);
            }
          }
        }
        // ... 清理逻辑
      });
}
```

---

### B.7 完整调度流程时序图

```
                    GCS                                    Owner Node                           Spillback Node
                     │                                          │                                      │
  Schedule(actor)    │                                          │                                      │
        │            │                                          │                                      │
        ▼            │                                          │                                      │
  SelectForwardingNode()                                        │                                      │
  (选择 owner 节点)  │                                          │                                      │
        │            │                                          │                                      │
        ▼            │                                          │                                      │
  LeaseWorkerFromNode()                                         │                                      │
  GrantOrReject=false│                                          │                                      │
        │            │  ─────RequestWorkerLease────────────────▶│                                      │
        │            │                                          │                                      │
        │            │                                          │ 选择 spillback 节点                  │
        │            │                                          │ worker_address = 空                  │
        │            │                                          │ retry_at_raylet_address = spillback  │
        │            │                                          │                                      │
        │            │  ◀─────────Reply─────────────────────────│                                      │
        │            │                                          │                                      │
        ▼            │                                          │                                      │
  HandleWorkerLeaseReply()                                      │                                      │
  打印 "Finished leasing"                                       │                                      │
        │            │                                          │                                      │
        ▼            │                                          │                                      │
  HandleWorkerLeaseGrantedReply()                               │                                      │
  worker_address.empty() == true                                │                                      │
        │            │                                          │                                      │
        ▼            │                                          │                                      │
  LeaseWorkerFromNode()                                         │                                      │
  GrantOrReject=true │  ─────RequestWorkerLease────────────────────────────────────────────────────────▶│
        │            │                                          │                                      │
        │            │                                          │                            检查本地资源│
        │            │                                          │                                      │
        │            │                                          │  ┌─────────────────────────────────┐ │
        │            │                                          │  │ 资源够：                        │ │
        │            │  ◀─────────Reply (worker_address=有效)──────│ rejected=false                   │ │
        │            │                                          │  │ worker_address=有效地址         │ │
        ▼            │                                          │  └─────────────────────────────────┘ │
  HandleWorkerLeaseGrantedReply()                               │                                      │
  worker_address.empty() == false                               │                                      │
        │            │                                          │                                      │
        ▼            │                                          │                                      │
  CreateActorOnWorker()                                         │                                      │
  打印 "Submitting actor creation task"                         │                                      │
        │            │                                          │                                      │
        │            │  ─────PushNormalTask─────────────────────────────────────────────────────────────▶│
        │            │                                          │                                      │
        │            │  ◀─────────Reply─────────────────────────────────────────────────────────────────│
        ▼            │                                          │                                      │
  打印 "Actor creation task succeeded"                          │                                      │
```

---

### B.8 GcsActorScheduler 初始化

**文件**: `src/ray/gcs/gcs_server.cc:477-508`

```cpp
void GcsServer::InitGcsActorManager(
    const GcsInitData &gcs_init_data,
    ray::observability::MetricInterface &actor_by_state_gauge,
    ray::observability::MetricInterface &gcs_actor_by_state_gauge) {

  RAY_CHECK(gcs_table_storage_ && gcs_publisher_ && gcs_node_manager_);

  // 定义调度失败处理器
  auto schedule_failure_handler =
      [this](std::shared_ptr<GcsActor> actor,
             const rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type,
             const std::string &scheduling_failure_message) {
        gcs_actor_manager_->OnActorSchedulingFailed(
            std::move(actor), failure_type, scheduling_failure_message);
      };

  // 定义调度成功处理器
  auto schedule_success_handler = [this](const std::shared_ptr<GcsActor> &actor,
                                         const rpc::PushTaskReply &reply) {
    gcs_actor_manager_->OnActorCreationSuccess(actor, reply);
  };

  // 创建 GcsActorScheduler，使用默认 io_context（主线程）
  scheduler = std::make_unique<GcsActorScheduler>(
      io_context_provider_.GetDefaultIOContext(),  // ← 主线程
      gcs_table_storage_->ActorTable(),
      *gcs_node_manager_,
      schedule_failure_handler,
      schedule_success_handler,
      raylet_client_pool_,
      worker_client_pool_,
      metrics_.scheduler_placement_time_ms_histogram);

  // 创建 GcsActorManager
  gcs_actor_manager_ = std::make_shared<GcsActorManager>(...);
}
```

---

### B.9 关键数据结构

```cpp
// Actor 调度器内部状态
class GcsActorScheduler {
 private:
  // 正在 leasing 的 Actor（按节点分组）
  // key: 节点 ID, value: 正在该节点 lease 的 Actor ID 集合
  absl::flat_hash_map<NodeID, absl::flat_hash_set<ActorID>>
      node_to_actors_when_leasing_;

  // 正在创建 Actor 的 Worker（按节点分组）
  // key: 节点 ID, value: (worker ID -> GcsLeasedWorker) 映射
  absl::flat_hash_map<NodeID,
      absl::flat_hash_map<WorkerID, std::shared_ptr<GcsLeasedWorker>>>
      node_to_workers_when_creating_;

  // 正在释放未使用 worker 的节点集合
  absl::flat_hash_set<NodeID> nodes_of_releasing_unused_workers_;
};

// 健康检查管理器内部状态
class GcsHealthCheckManager {
 private:
  // 每个节点的健康检查上下文
  absl::flat_hash_map<NodeID, HealthCheckContext *> health_check_contexts_;

  // 健康检查参数
  int64_t initial_delay_ms_;    // 初始延迟
  int64_t timeout_ms_;          // 超时时间
  int64_t period_ms_;           // 检查间隔
  int64_t failure_threshold_;   // 失败阈值（默认 10）
};
```

---

### B.10 日志与代码位置对照表（完整版）

| 日志消息 | 源文件:行号 | 函数 | 含义 |
|---------|------------|------|------|
| `Leasing worker for actor` | `gcs_actor_scheduler.cc:243` | `LeaseWorkerFromNode` | 开始向某节点发起 lease 请求 |
| `Finished leasing worker from` | `gcs_actor_scheduler.cc:583` | `HandleWorkerLeaseReply` | lease RPC 返回成功（但不一定拿到 worker） |
| `Failed to lease worker from node ... resources are not enough` | `gcs_actor_scheduler.cc:577` | `HandleWorkerLeaseReply` | 节点资源不足，被拒绝 |
| `Submitting actor creation task to worker` | `gcs_actor_scheduler.cc:390` | `CreateActorOnWorker` | 真正拿到 worker，开始创建 Actor |
| `Actor creation task succeeded` | `gcs_actor_scheduler.cc:429` | `CreateActorOnWorker` (callback) | Actor 创建成功 |
| `Actor creation task failed, will be retried` | `gcs_actor_scheduler.cc:437` | `CreateActorOnWorker` (callback) | Actor 创建失败，将重试 |
| `Node is dead because the health check failed` | `gcs_health_check_manager.cc:84` | `FailNode` | 健康检查失败次数归零，标记节点死亡 |
| `Health check failed for node ... remaining checks` | `gcs_health_check_manager.cc:206` | `StartHealthCheck` (callback) | 单次健康检查失败 |
| `health check failed due to missing too many heartbeats` | `gcs_node_manager.cc:562` | `InferDeathInfo` | 节点死亡原因消息（UNEXPECTED_TERMINATION） |
| `raylet has lagging heartbeats due to slow network or busy workload` | `gcs_node_manager.cc:660` | `RemoveNodeFromCache` | 广播给 driver 的警告消息 |
| `Node was forcibly preempted` | `gcs_node_manager.cc:558` | `InferDeathInfo` | 节点被抢占（AUTOSCALER_DRAIN_PREEMPTED） |

---

## 十二、健康检查 Connection refused 深度分析

> 相关文档：[RaySyncer 资源同步机制与配置参数深度分析](./ray-syncer-resource-sync-mechanism.md)

### 12.1 问题日志

```
[2026-05-07 20:52:15,051 W 78 78] (gcs_server) gcs_health_check_manager.cc:205:
Health check failed for node 28e5744f308be8815b05a659ec6b8466ea2ea61be6bae7ef3e032753,
remaining checks 1,
status 14,
response status 0,
status message failed to connect to all addresses;
last error: UNKNOWN: ipv4:10.48.32.149:41029: Failed to connect to remote host: Connection refused,
status details
```

### 12.2 日志字段逐项解析

| 字段 | 值 | 含义 |
|------|-----|------|
| `remaining checks 1` | 1 | 还剩 1 次机会，下次再失败就判定节点死亡 |
| `status 14` | gRPC UNAVAILABLE | 服务不可达，TCP 连接失败 |
| `response status 0` | UNKNOWN | 没有收到有效的 HealthCheckResponse |
| `status message` | failed to connect to all addresses | gRPC 客户端无法连接到任何地址 |
| `last error` | Connection refused | TCP RST，目标端口无进程监听 |
| 目标地址 | `ipv4:10.48.32.149:41029` | Raylet 的 gRPC 监听端口 |

**关键判断**：`Connection refused` 表示 TCP SYN 包收到了 RST 响应，说明：
1. **网络是通的**（如果网络不通，错误会是 `Connection timed out` 而非 `Connection refused`）
2. **目标端口无进程监听** → Raylet 进程已不在

### 12.3 gRPC Status Code 对照

| gRPC Status Code | 含义 | 本场景？ | 说明 |
|------------------|------|---------|------|
| **14 (UNAVAILABLE)** | 服务不可达/连接被拒 | **是** | Raylet 进程已死或端口不可达 |
| 4 (DEADLINE_EXCEEDED) | 超时 | 否 | 说明 Raylet 在但响应慢（主线程阻塞等） |
| 1 (CANCELLED) | 请求被取消 | 否 | GCS 端主动取消 |
| 13 (INTERNAL) | 内部错误 | 否 | 服务端异常 |
| 2 (UNKNOWN) | 未知错误 | 否 | 一般不应出现 |

**区分关键**：
- `status=14` + `Connection refused` = **Raylet 进程已死**
- `status=4` + `Deadline Exceeded` = **Raylet 在但响应慢**（主线程阻塞、网络延迟等）
- `status=14` + `Connection timed out` = **网络不通**（网络分区/防火墙等）

### 12.4 健康检查机制与判定流程

GCS 对每个节点定期发起 gRPC Health Check（`gcs_health_check_manager.cc:200-216`）：

```cpp
if (status.ok() && response->status() == HealthCheckResponse::SERVING) {
  // 健康检查通过，重置剩余检查次数
  health_check_remaining_ = mgr->failure_threshold_;
} else {
  // 健康检查失败，递减剩余次数
  --health_check_remaining_;
  RAY_LOG(WARNING) << "Health check failed for node " << node_id_
                   << ", remaining checks " << health_check_remaining_ << ...;
}

if (health_check_remaining_ == 0) {
  // 连续失败次数达到阈值，判定节点死亡
  mgr->FailNode(node_id_);
  delete this;
} else {
  // 还有剩余机会，安排下一次检查
  ...
}
```

**判定流程**：

```
健康检查失败 (remaining_checks 从 N 递减)
  |
  v
remaining_checks > 0?
  |
  +--> 是: 等待 health_check_period_ms 后再次检查
  |
  +--> 否: FailNode(), 标记节点死亡
              |
              v
           广播节点死亡事件
           所有在该节点上的 Actor/Task 被标记为 DEAD
```

`remaining checks 1` 意味着这是倒数第二次失败，**下一次失败将直接判定节点死亡**。

### 12.5 根因分析

`Connection refused` 最常见的原因：

| 根因 | 概率 | 诊断方式 |
|------|------|---------|
| **OOM Kill** | 最高 | `dmesg | grep -i "oom\|killed process"` |
| **容器被驱逐/重启** | 高 | K8s: `kubectl describe pod <pod>` 看 Events |
| **Segfault/崩溃** | 中 | `dmesg | grep segfault`，检查 coredump |
| **Raylet 主动退出** | 中 | Raylet 日志中有 "Exiting because" |
| **端口冲突** | 低 | 进程已不在时才会出现此错误 |
| **防火墙/iptables** | 低 | `Connection refused` 说明网络是通的，此原因概率极低 |

### 12.6 排查步骤

#### 第一步：确认 Raylet 进程状态

```bash
# SSH 到故障节点
ssh 10.48.32.149

# 检查 Raylet 进程是否还在
ps aux | grep raylet

# 如果进程不在，确认退出时间
# 查看系统日志
journalctl -u raylet --since "2026-05-07 20:50"
```

#### 第二步：检查系统级杀死原因

```bash
# OOM Kill 检查（最常见）
dmesg | grep -i "oom\|killed process" | tail -20
journalctl -k --since "2026-05-07 20:50" | grep -i "oom\|killed"

# Segfault 检查
dmesg | grep -i segfault | tail -20

# 容器驱逐（K8s 环境）
kubectl describe pod <pod-name> -n <namespace>
kubectl get events --sort-by='.lastTimestamp' -n <namespace> | tail -20
```

#### 第三步：检查 Raylet 日志

```bash
# 查看 Raylet 退出前的最后日志
tail -200 /tmp/ray/session_latest/logs/raylet.out

# 如果 Raylet 自己退出，日志中会有 "Exiting because"
grep "Exiting because\|marked as dead\|signal" /tmp/ray/session_latest/logs/raylet.out

# 关键区分：
#   有 "marked as dead" → Raylet 因为健康检查超时主动退出
#   无 "marked as dead" → Raylet 被系统杀死（OOM/信号），进程来不及输出日志
```

#### 第四步：检查 GCS 侧完整健康检查链路

```bash
# 查看该节点的所有健康检查日志
grep "28e5744f" /tmp/ray/session_latest/logs/gcs_server.out | head -30

# 观察失败序列（时间线和 remaining checks 递减过程）
# 正常模式：remaining checks 从 5->4->3->2->1->0（FailNode）
# 异常模式：直接从 1 开始（说明之前已有多次失败）
```

#### 第五步：多节点对比

```bash
# 是否只有这一个节点失败？还是多个节点同时失败？
grep "Health check failed" /tmp/ray/session_latest/logs/gcs_server.out | \
  awk '{print $NF}' | sort | uniq -c | sort -rn | head -10

# 如果多个节点同时失败 → 可能是 GCS 端问题或网络分区
# 如果只有单个节点失败 → 大概率是该节点 Raylet 崩溃
```

### 12.7 健康检查失败的不同场景对比

| 场景 | status | 错误信息 | Raylet 进程 | 根因 |
|------|--------|---------|------------|------|
| Raylet 崩溃 (OOM) | 14 | Connection refused | 不在 | OOM Kill |
| Raylet 主线程阻塞 | 4 | Deadline Exceeded | 在 | 主线程 CPU 100% 或死锁 |
| 网络分区 | 14 | Connection timed out | 在 | 网络不通 |
| 容器重启 | 14 | Connection refused | 不在 | K8s 驱逐/调度 |
| Raylet 主动退出 | 14 | Connection refused | 不在 | 健康检查超时自退出 |

### 12.8 修复与预防

#### 短期修复

```python
# 增加健康检查容忍度
ray.init(_system_config={
    "health_check_timeout_ms": 30000,       # 增加单次超时（默认 10s）
    "health_check_period_ms": 10000,        # 增加检查间隔（默认 10s）
    "health_check_failure_threshold": 10,    # 增加容忍次数（默认 5）
})
```

#### 中期预防

```bash
# 1. 增加节点内存/限制 worker 内存
# 2. 监控 Raylet 进程内存，提前预警
# 3. K8s 环境：配置合理的 requests/limits，避免 OOM Kill
```

#### 诊断脚本

```bash
#!/bin/bash
# 健康检查 Connection refused 快速诊断脚本
NODE_IP=$1
NODE_PORT=$2

echo "=== Step 1: Check Raylet process ==="
ssh $NODE_IP "ps aux | grep raylet" 2>/dev/null || echo "SSH failed - node may be down"

echo "=== Step 2: Check OOM ==="
ssh $NODE_IP "dmesg | grep -i 'oom\|killed' | tail -5" 2>/dev/null

echo "=== Step 3: Check Raylet logs ==="
ssh $NODE_IP "tail -50 /tmp/ray/session_latest/logs/raylet.out" 2>/dev/null

echo "=== Step 4: Check port ==="
ssh $NODE_IP "ss -tlnp | grep $NODE_PORT" 2>/dev/null || echo "Port not listening"

echo "=== Step 5: Check system resources ==="
ssh $NODE_IP "free -h; df -h /; uptime" 2>/dev/null
```
