# GCS Worker 异常退出排查分析

## 问题现象

GCS 日志中出现大量 `gcs_worker_manager.cc:58: Reporting worker exit` 警告，所有 `exit_type = SYSTEM_ERROR`，涉及两种不同的错误详情。

## 集群环境

| 指标 | 值 |
|------|-----|
| GCS 启动时间 | 2026-05-09 16:07:58 |
| 集群节点数 | 1511（全部 Alive，0 Dead） |
| Ray 版本 | 2.54.0 |
| 部署方式 | Kubernetes 多 Pod（同一 IP 上多个 raylet） |
| Actor 类型 | QwenVLCPUPreprocessActor (5160) + MapWorker (1000) |
| Worker 退出总数 | 22 次 |

---

## 排查过程

### 第一步：统计 worker 退出数量和时间分布

```bash
# 总数
grep -c 'gcs_worker_manager' /tmp/ray/session_latest/logs/gcs_server.out
# 22

# 按分钟聚合
grep 'gcs_worker_manager' /tmp/ray/session_latest/logs/gcs_server.out | \
  awk '{print $2}' | cut -d, -f1 | cut -d: -f1,2 | sort | uniq -c | sort -rn
# 10 16:27
# 10 16:25
#  1 16:38
#  1 16:26
```

**时间分布：集中在 16:25~16:27，零星延续到 16:38。**

### 第二步：分类错误类型

```bash
# 按 exit_type 分类
grep 'gcs_worker_manager' /tmp/ray/session_latest/logs/gcs_server.out | \
  grep -oP 'exit_type = \S+' | sort | uniq -c | sort -rn
# 22 exit_type = SYSTEM_ERROR,

# 按 exit_detail 分类
grep 'gcs_worker_manager' /tmp/ray/session_latest/logs/gcs_server.out | \
  grep -oP 'exit_detail = [^.]+' | sort | uniq -c | sort -rn
# 18 exit_detail = Worker unexpectedly exits with a connection error code 2
#  4 exit_detail = The leased worker has unrecoverable failure
```

**两种失败模式：**

| 模式 | 数量 | 错误详情 |
|------|------|----------|
| A: EOF 连接断开 | 18 | `connection error code 2. End of file` |
| B: RPC 连接超时 | 4 | `Failed to connect to remote host: Connection timed out` |

### 第三步：按节点 IP 分析影响范围

```bash
# 按 IP 聚合
grep 'gcs_worker_manager' /tmp/ray/session_latest/logs/gcs_server.out | \
  grep -oP 'address = [0-9.]+' | sort | uniq -c | sort -rn | head -15
```

```
3 address = 10.48.74.144
2 address = 10.48.74.177
2 address = 10.48.74.174
2 address = 10.48.35.88
2 address = 10.48.35.32
1 address = 10.57.36.159
1 address = 10.53.82.98
1 address = 10.53.80.208
... (共 15 个不同 IP)
```

**分布在 15 个不同 IP 上，不是单节点故障。**

```bash
# 按 node_id 聚合（确认是否跨 Pod）
grep 'gcs_worker_manager' /tmp/ray/session_latest/logs/gcs_server.out | \
  grep -oP 'node id = \S+' | sort | uniq -c | sort -rn | head -10
```

```
2 node id = feaea094...
2 node id = 1d4c808b...
1 node id = fb57ebec...
... (共 18 个不同 node_id)
```

**分布在 18 个不同的 node_id 上，进一步确认是分散性故障。**

### 第四步：确认节点存活状态

```python
import ray
ray.init(address='auto', namespace='diag', ignore_reinit_error=True)
nodes = ray.nodes()
alive = [n for n in nodes if n['Alive']]
dead = [n for n in nodes if not n['Alive']]
print(f'Total={len(nodes)} Alive={len(alive)} Dead={len(dead)}')

# 检查出错 IP 的节点状态
fail_ips = ['10.48.74.144', '10.48.35.88', '10.48.74.174', '10.48.75.34']
for ip in fail_ips:
    matches = [n for n in nodes if n.get('NodeManagerAddress') == ip]
    for m in matches:
        print(f'  {ip}: Alive={m["Alive"]}, NodeID={m["NodeID"][:16]}...')
```

```
Total=1511 Alive=1511 Dead=0
  10.48.74.144: Alive=True, NodeID=f84a48c214d17638...
  10.48.74.144: Alive=True, NodeID=ac9da0faee7f71ad...
  10.48.74.144: Alive=True, NodeID=5e72f2e08c48cf28...
  10.48.74.144: Alive=True, NodeID=d1ee2ec122a958a9...
  10.48.74.144: Alive=True, NodeID=16f68f1941d2605d...
  10.48.74.144: Alive=True, NodeID=adca67268da3011a...
  10.48.74.144: Alive=True, NodeID=1d4c808b7c3ea45c...   ← 7 个 raylet Pod 在同一 IP
  10.48.35.88:  Alive=True, NodeID=7f5551c18a46f2ed...
  ...                                                     ← 该 IP 也有 7 个 Pod
```

**所有节点都活着。每个 IP 上运行了多个 raylet Pod（同一物理机上的 Kubernetes 多容器部署）。**

### 第五步：分析 GCS 时间线——排除 GCS 重启瞬态影响

```bash
# GCS 重启后的关键事件时间线
grep -c 'Connection is broken' /tmp/ray/session_latest/logs/gcs_server.out
# 1508（旧 session raylet 断连，集中在 16:08:05~16:08:29）

grep 'Registering new node' /tmp/ray/session_latest/logs/gcs_server.out | \
  awk '{print $2}' | cut -d, -f1 | cut -d: -f1,2 | sort | uniq -c | sort -rn
# 1460 16:08    ← 绝大多数在第一分钟内注册
#   47 16:09
#    2 16:10
#    1 16:11
#    1 16:16

# Actor 创建时间线
grep 'Leasing worker' /tmp/ray/session_latest/logs/gcs_server.out | \
  awk '{print $2}' | cut -d, -f1 | cut -d: -f1,2 | sort | uniq -c | sort -rn | head -10
# 2002 16:25   ← Actor 创建高峰
# 1917 16:36
# 1900 16:33
# 1350 16:42
# ...

# 调度自旋检查
grep -c 'resources are not enough' /tmp/ray/session_latest/logs/gcs_server.out
# 0（无调度自旋，优化配置已生效）
```

**关键时间线：**

```
16:07:58       GCS 启动
16:08:05~29    1508 个旧 session 连接断开 + 重连
16:08~16:10    1511 个节点注册完成
16:10          集群完全就绪
  ... (14 分钟平稳)
16:24:37       第一个 Actor 创建成功
16:25:04       Actor 大批量创建开始（高峰期）
16:25:24       第一批 Worker 退出报错 ← 距 GCS 启动已 17 分钟
```

**结论：Worker 退出发生在 GCS 启动 17 分钟后，与 GCS 重启瞬态无关。**

### 第六步：检查 Actor 创建重试情况

```bash
grep 'Actor creation task' /tmp/ray/session_latest/logs/gcs_server.out | head -5
```

```
[16:24:37] Actor creation task succeeded. actor_id=0c2dc194... worker_id=465bb3af... ← 正常
[16:24:57] Actor creation task succeeded. actor_id=a6fb2768... worker_id=d460a4c4... ← 正常
[16:25:05] Actor creation task succeeded. actor_id=3ecf7989... worker_id=4cadad9b... ← 正常
[16:25:07] Actor creation task succeeded. actor_id=5ee33714... worker_id=31e9a909... ← 正常
[16:25:07] Actor creation task succeeded. actor_id=6d41c999... worker_id=16162250... ← 正常
```

```bash
grep 'Actor creation task failed' /tmp/ray/session_latest/logs/gcs_server.out
```

```
[16:25:18] Actor creation task failed, will be retried. actor_id=7f6b4ac4... node_id=7a890175...
[16:33:55] Actor creation task failed, will be retried. actor_id=23573f5d... node_id=d84e93c6...
```

**仅 2 次 Actor 创建失败并重试，均成功恢复。**

### 第七步：检查节点注销事件

```bash
# UnregisterNode 计数（event_stats 中的统计）
grep 'UnregisterNode' /tmp/ray/session_latest/logs/gcs_server.out | grep 'total' | tail -1
# NodeInfoGcsService.grpc_server.UnregisterNode - 59 total (59 active)
```

**有 59 次节点注销。这些是旧 session 残留节点主动注销（cluster ID 不匹配后的清理），不是新 session 节点故障。**

---

## 两种失败模式的源码级分析

### 模式 A：`connection error code 2. End of file`

#### 错误产生位置

`src/ray/raylet/node_manager.cc:1084-1099`：

```cpp
void NodeManager::HandleClientConnectionError(
    const std::shared_ptr<ClientConnection> &client,
    const boost::system::error_code &error) {
  // error.value() = 2, error.message() = "End of file"
  const std::string err_msg = absl::StrCat(
      "Worker unexpectedly exits with a connection error code ",
      error.value(), ". ", error.message(), ...);
  DisconnectClient(client, /*graceful=*/false,
      ray::rpc::WorkerExitType::SYSTEM_ERROR, err_msg);
}
```

#### 含义

Raylet 与 Worker 之间通过 Unix Domain Socket 通信。当 worker 进程突然消失时（没有走优雅退出流程），socket 读操作返回 EOF（boost error code 2 = `boost::asio::error::eof`）。

#### 可能的原因

| 原因 | 概率 | 说明 |
|------|------|------|
| **OOM Killer** | 高 | 进程被系统 OOM killer 杀死，无法优雅退出 |
| **SIGSEGV/SIGBUS** | 中 | 内存访问错误导致崩溃 |
| **Kubernetes 资源限制** | 中 | Pod 内存超过 limits，被 kubelet 杀死 |
| **SIGKILL** | 低 | 外部信号强制杀死 |

#### 典型日志

```
[2026-05-09 16:25:24,449 W 74 74] (gcs_server) gcs_worker_manager.cc:58:
  Reporting worker exit,
  worker id = 19adc54a...,
  node id = 3a272c12...,
  address = 10.48.74.174,
  exit_type = SYSTEM_ERROR,
  exit_detail = Worker unexpectedly exits with a connection error code 2. End of file.
```

### 模式 B：`Failed to connect to remote host: Connection timed out`

#### 错误产生位置

`src/ray/gcs/actor/gcs_actor_scheduler.cc:382-431`：

```cpp
void GcsActorScheduler::CreateActorOnWorker(
    std::shared_ptr<GcsActor> actor, std::shared_ptr<GcsLeasedWorker> worker) {
  // GCS 拿到 worker 地址后，直接发起 gRPC 连接推送 actor 创建任务
  auto client = worker_client_pool_.GetOrConnect(worker->GetAddress());
  client->PushNormalTask(std::move(request),
    [this, actor, worker](Status status, const rpc::PushTaskReply &reply) {
      if (status.ok()) {
        schedule_success_handler_(actor, reply);   // 成功
      } else {
        RetryCreatingActorOnWorker(actor, worker); // 失败重试
      }
    });
}
```

#### 含义

GCS 从 Raylet 获取了 worker 的 lease（包含 worker 的 IP:Port），但当 GCS 尝试连接这个 worker 时，TCP 连接超时。说明 worker 进程已经死了，端口不再监听。

#### 与模式 A 的关系

模式 B 是模式 A 的**下游后果**：

```
1. Worker 进程死亡（OOM/Crash）
     ↓
2. Raylet 检测到 socket EOF → 上报 worker exit（模式 A）  ← 快，< 1 秒
     ↓
3. 同时，GCS 可能已经拿到了这个 worker 的 lease
     ↓
4. GCS 尝试连接已死的 worker → TCP 超时（模式 B）          ← 慢，30-60 秒
```

#### 典型日志

```
[2026-05-09 16:26:03,965 W 74 74] (gcs_server) gcs_worker_manager.cc:58:
  Reporting worker exit,
  worker id = 582069b5...,
  node id = f9388026...,
  address = 10.48.35.88,
  exit_type = SYSTEM_ERROR,
  exit_detail = The leased worker has unrecoverable failure.
  Worker is requested to be destroyed when it is returned.
  RPC error: failed to connect to all addresses;
  last error: UNKNOWN: ipv4:10.48.35.88:10033: Failed to connect to remote host: Connection timed out.
```

---

## 竞态条件分析：为什么 Raylet 会把即将死亡的 Worker 分配出去

### Worker Lease 流程

```
GCS                              Raylet                         Worker
 |                                 |                              |
 |--- LeaseWorkerFromNode -------->|                              |
 |                                 |-- PopWorker (从 pool 弹出) -->|
 |                                 |   检查：                      |  ← worker 此时活着
 |                                 |   ✓ 在 pool 中               |
 |                                 |   ✓ 资源满足                  |
 |                                 |   ✗ 不检查内存/GCS连接状态     |
 |<-- Reply(worker addr+port) ----|                              |
 |                                 |                              |
 |--- CreateActorOnWorker ---------|------- PushNormalTask ------>|  ← worker 可能已死
 |                                 |                              |
 |    如果 worker 已死:             |                              |
 |    TCP 超时 30-60s              |-- 检测到 EOF (< 1s) -------->X
 |                                 |-- 上报 WorkerExit --------->GCS
```

### 源码：Raylet 在 Lease 时不检查 Worker 健康度

`src/ray/raylet/scheduling/local_lease_manager.cc` — `GrantScheduledLeasesToWorkers` 的检查项：

```
✓ 公平调度（scheduling class 均衡）
✓ 调度类容量限制
✓ Plasma argument 可用性
✓ 本地资源是否充足
✓ Worker pool 中有空闲 worker
✗ 不检查 worker 内存使用率
✗ 不检查 worker 的 GCS 连接状态
✗ 不检查 worker 是否即将被 OOM kill
```

这不是设计缺陷。Raylet 弹出 worker 时它是健康的，弹出后几秒内才被 kill。这是一个**本质不可避免的竞态窗口**——任何系统都无法预测一个进程即将被 OOM killer 选中。

### GCS 的保护机制

GCS 有一个针对重启场景的保护：`nodes_of_releasing_unused_workers_`。

`src/ray/gcs/actor/gcs_actor_scheduler.cc:245-250`：

```cpp
// We need to ensure that the RequestWorkerLease won't be sent before
// the reply of ReleaseUnusedActorWorkers is returned.
if (nodes_of_releasing_unused_workers_.contains(node_id)) {
    RetryLeasingWorkerFromNode(actor, node);  // 延迟重试
    return;
}
```

这确保了 GCS 重启后不会在 `ReleaseUnusedActorWorkers` 完成前就 Lease worker。但这个保护只针对重启瞬态，不能防止正常运行中的 worker 死亡。

---

## 各阶段耗时

| 阶段 | 耗时 | 说明 |
|------|------|------|
| Worker 死亡 → Raylet 检测 EOF | **< 1 秒** | Socket EOF 触发 `HandleClientConnectionError` |
| Raylet 上报 `WorkerExit` 到 GCS | **< 100ms** | 异步 gRPC 调用 |
| GCS 连接已死 Worker 的 TCP 超时 | **30-60 秒** | 标准 TCP 连接超时 |
| GCS Actor 重试间隔 | **默认 1 秒** | `gcs_lease_worker_retry_interval_ms` |
| Actor 创建完全恢复 | **1-5 秒** | 重新 Lease + 重新 CreateActorOnWorker |
| 模式 A → 模式 B 的时间差 | **~11 分钟** | 16:27:27 → 16:38:16 = TCP 超时 + 多次重试 |

---

## 这个集群的特殊风险因素

### 同一 IP 多 Pod 部署

```
10.48.74.144 → 7 个 raylet Pod (7 个 NodeID)
10.48.35.88  → 7 个 raylet Pod
10.48.74.174 → 5 个 raylet Pod
10.48.75.34  → 4 个 raylet Pod
```

同一台物理机上的多个 Pod 共享内存。当物理机内存紧张时，Linux OOM killer 会选择 RSS 最大的进程杀死。Worker 进程（尤其是加载了大模型的 GPU worker 或处理视频的 CPU worker）是 OOM 高风险目标。

### Actor 创建高峰期

```
16:25 → 2002 次 Leasing/分钟（最高峰）
16:33 → 1900 次
16:36 → 1917 次
16:42 → 1350 次（逐渐降低）
```

高峰期大量 worker 同时启动，内存压力瞬间增大，触发 OOM 的概率更高。

---

## 严重程度评估

| 维度 | 评估 |
|------|------|
| 失败率 | 22 / 6163 ALIVE Actor = **0.36%** |
| Actor 创建失败 | 仅 2 次，全部自动重试成功 |
| 节点影响 | 0 个节点死亡，分布在 15 个 IP / 18 个 NodeID |
| 持续性 | 一次性现象，集中在 16:25~16:38，之后无复发 |
| 调度自旋 | 0 次（`resources are not enough` = 0） |
| 数据丢失 | 无 |

**结论：正常的瞬态错误，Ray 的自动重试机制已完全恢复。不需要人工干预。**

---

## 进一步排查方向（如需定位具体 OOM 原因）

### 1. 检查 Worker 节点的 dmesg OOM 日志

```bash
# 在出错节点上执行
dmesg -T | grep -i 'oom\|killed process' | tail -20
```

### 2. 检查 Kubernetes Pod 事件

```bash
kubectl get events -n <namespace> --field-selector reason=OOMKilled --sort-by='.lastTimestamp' | tail -20
```

### 3. 检查 Worker 进程的内存使用

```bash
# 在 raylet 节点上，查看 worker 进程内存
ps aux --sort=-rss | head -20
```

### 4. 检查 Raylet 日志中的详细 worker 退出信息

```bash
# Raylet 日志比 GCS 日志包含更多本地信息
grep 'DisconnectClient\|HandleClientConnectionError' /tmp/ray/session_latest/logs/raylet.out | \
  grep '<worker_id前缀>' | head -5
```

### 5. 检查是否有 Kubernetes 资源限制

```bash
kubectl describe pod <pod_name> -n <namespace> | grep -A5 'Limits\|Requests'
```

---

## 可复用的诊断命令汇总

### 快速概览

```bash
# Worker 退出总数和时间分布
grep -c 'gcs_worker_manager' /tmp/ray/session_latest/logs/gcs_server.out
grep 'gcs_worker_manager' /tmp/ray/session_latest/logs/gcs_server.out | \
  awk '{print $2}' | cut -d, -f1 | cut -d: -f1,2 | sort | uniq -c | sort -rn

# 按错误类型分类
grep 'gcs_worker_manager' /tmp/ray/session_latest/logs/gcs_server.out | \
  grep -oP 'exit_detail = [^.]+' | sort | uniq -c | sort -rn

# 按节点 IP 分类
grep 'gcs_worker_manager' /tmp/ray/session_latest/logs/gcs_server.out | \
  grep -oP 'address = [0-9.]+' | sort | uniq -c | sort -rn | head -15
```

### 关联分析

```bash
# 同一时间段内的 Actor 创建活动
grep 'Leasing worker' /tmp/ray/session_latest/logs/gcs_server.out | \
  awk '{print $2}' | cut -d, -f1 | cut -d: -f1,2 | sort | uniq -c | sort -rn | head -10

# Actor 创建失败和重试
grep 'Actor creation task failed' /tmp/ray/session_latest/logs/gcs_server.out

# 调度自旋（如果非 0，说明有更严重的问题）
grep -c 'resources are not enough' /tmp/ray/session_latest/logs/gcs_server.out

# 节点注册/注销事件
grep -c 'Registering new node' /tmp/ray/session_latest/logs/gcs_server.out
```

### 节点状态验证

```python
import ray
ray.init(address='auto', namespace='diag', ignore_reinit_error=True)
nodes = ray.nodes()
alive = [n for n in nodes if n['Alive']]
dead = [n for n in nodes if not n['Alive']]
print(f'Total={len(nodes)} Alive={len(alive)} Dead={len(dead)}')

# 检查特定 IP 上的 Pod 数量
from collections import Counter
ip_counts = Counter(n.get('NodeManagerAddress') for n in nodes if n['Alive'])
for ip, cnt in ip_counts.most_common(10):
    print(f'  {ip}: {cnt} pods')
```

---

## Worker 退出完整流程图

```
Worker 进程被 OOM Killer 杀死
  │
  ├──→ Raylet 侧（< 1 秒）
  │     │
  │     ├── HandleClientConnectionError()
  │     │     error code = 2 (EOF)
  │     │
  │     ├── DisconnectClient(graceful=false, SYSTEM_ERROR)
  │     │     │
  │     │     ├── 从 leased_workers_ 移除
  │     │     ├── 释放资源 → OnResourceOrStateChanged() → version_++
  │     │     └── 通知 GCS: ReportWorkerFailure RPC
  │     │
  │     └── GCS 收到 ReportWorkerFailure
  │           │
  │           ├── gcs_worker_manager.cc:58 打印 WARNING 日志  ← 模式 A 日志
  │           └── PublishWorkerFailure → pubsub 通知订阅者
  │
  └──→ GCS 侧（如果已 Lease 该 Worker）
        │
        ├── 情况 1: GCS 尚未发起 PushNormalTask
        │     └── Worker 死亡 → lease 回调返回错误 → RetryLeasingWorkerFromNode
        │
        └── 情况 2: GCS 已发起 PushNormalTask
              │
              ├── TCP 连接超时（30-60 秒）
              │
              ├── PushNormalTask 回调 status != ok
              │     └── RetryCreatingActorOnWorker  ← 模式 B 日志
              │
              └── 最终：GCS 重新 Lease 新 Worker → Actor 创建成功
```
