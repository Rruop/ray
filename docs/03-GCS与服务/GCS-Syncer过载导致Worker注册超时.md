# GCS Syncer 过载导致 Worker 注册超时与幽灵 Task 问题分析

## 问题现象

Ray 作业的 `QwenVLCPUPreprocessActor` 实例已全部消失（Dashboard Actors 页面无存活记录），但 Dashboard 的 Tasks 页面中仍持续显示 `QwenVLCPUPreprocessActor.__init__` task 处于 **RUNNING** 状态。

同时 Driver 日志中出现大量如下错误（跨集群 264 次重复）：

```
(raylet, ip=10.51.155.30) ray.exceptions.RaySystemError: System error: Failed to connect to GCS.
Please check if the GCS server is running and if this node can connect to the head node.
[repeated 132x across cluster]

(raylet, ip=10.51.150.91) Failed to get cluster ID from GCS server: TimedOut:
Timed out while waiting for GCS to become available. [repeated 133x across cluster]
```

Dashboard 仍可正常刷新和访问。

---

## 环境信息

| 项目 | 值 |
|------|-----|
| 集群规模 | ~1,298 个 worker 节点 |
| Head 节点 IP | 10.81.0.20 |
| GCS 端口 | 6379 |
| Ray 版本 | 2.x (commit cc121a56b9) |
| 操作系统 | Ubuntu 22.04.4 LTS |
| 内核 | 5.14.0-3.0.3.kwai.x86_64 (快手自定义) |
| 同时运行 Job 数 | 2 个 |

### Job 关键参数

| 参数 | Job 1 (12:08 提交) | Job 2 (15:25 提交) |
|------|--------------------|--------------------|
| cpu-concurrency | 500 | 500 |
| gpu-concurrency | 2 | 2 |
| streaming-gpu-concurrency | 200 | 1000 |
| streaming-cpu-actor-pool-size | 15 | 15 |
| streaming-actor-num-workers | 16 | 16 |
| streaming-cpu-actor-num-cpus | 2 | 2 |

### GCS system-config

```json
{
  "raylet_report_resources_period_milliseconds": 10000,
  "ray_syncer_message_refresh_interval_ms": 60000,
  "gcs_resource_broadcast_max_batch_size": 1500,
  "gcs_resource_broadcast_max_batch_delay_ms": 1500,
  "health_check_period_ms": 10000,
  "gcs_server_rpc_server_thread_num": 64,
  "scheduler_avoid_gpu_nodes": true,
  "event_stats_print_interval_ms": 180000
}
```

---

## 分析过程

### Step 1: 确认 GCS 连接性

在 head 节点上执行 `ray status`：

```
[2026-05-12 17:29:28,670 W 188731 188731] rpc_client.h:153:
Failed to connect to GCS at address 10.81.0.20:6379 within 5 seconds.
```

**结论**：在 head 节点本地都无法连接 GCS，说明 GCS 进程本身有问题，而非网络问题。

### Step 2: 检查 GCS 进程状态

#### top 线程级视图

```
PID   USER  PR  NI    VIRT    RES    SHR S  %CPU  %MEM  COMMAND
103   root  20   0  226.2g  29.9g  19224 R  98.7   5.9  ray_syncer_io_c
102   root  20   0  226.2g  29.9g  19224 S  28.6   5.9  pubsub_io_conte
78    root  20   0  226.2g  29.9g  19224 S  22.9   5.9  gcs_server
225   root  20   0  226.2g  29.9g  19224 S  21.3   5.9  server.poll30
```

#### 进程级指标

| 指标 | 值 |
|------|-----|
| 进程总 CPU | 362.5% |
| RSS 内存 | 29.8 GB |
| 线程数 | 638 |
| 进程状态 | R (running) |

#### 网络连接

```bash
$ ss -tnp | grep 6379 | wc -l
50551

$ ss -tnp | grep 6379 | awk '{print $1}' | sort | uniq -c | sort -rn
47863 ESTAB
  341 CLOSE-WAIT
    2 LAST-ACK
```

**关键发现**：GCS 端口上有 **47,863 个 ESTAB 连接**，来自 1,298 个独立 IP，平均每节点 ~37 个连接。

### Step 3: 分析连接数来源

每节点 ~37 个连接的组成：

```
每个 worker 节点连接 = raylet(1) + dashboard_agent(1) + runtime_env_agent(1) + N 个 worker 进程
```

每个 worker 进程（无论是 task worker 还是 actor）都会各自建立一个 GCS gRPC 连接。节点上运行的 actor 越多，连接数越多。

头部节点连接数达 266，与大量 actor 部署一致。

### Step 4: 确认瓶颈线程

`ray_syncer_io_c` 线程占用 **98.7% CPU**（单核满载），这是 Ray Syncer 的 IO 事件循环线程，负责将集群资源状态变更**广播到所有节点**。

### Step 5: GCS 日志分析

```
gcs_actor_scheduler.cc:499: Failed to kill actor NIL_ID, return status: Invalid:
KillActor RPC failed for actor NIL_ID: RpcError: RPC error: Socket closed rpc_code: 14
```

大量 `NIL_ID` actor kill 失败，说明：
- Worker 节点已断连
- Actor 处于 PENDING_CREATION 中间态（ID 尚未完全分配）
- GCS 尝试清理但目标 raylet 已不可达

### Step 6: 排除"锁竞争"假设

#### gRPC poll 线程 (225) 的 futex 分析

多次采样 `/proc/78/task/225/syscall`：

```
采样结果 (10次):
- syscall 202 (futex): 6次, 地址固定 0x7ffbab8ea340
- running: 3次
- syscall 230 (clock_nanosleep): 1次
```

检查 futex 地址归属：
```bash
$ cat /proc/78/maps | grep 7ffbab8e
(无输出 - 地址不在任何已命名映射区域)
```

**结论**：`0x7ffbab8ea340` 位于匿名 mmap 线程栈区域，是 **gRPC CompletionQueue 自身的 condition variable**（正常 idle 等待），不是应用层共享锁。

#### Listen backlog 检查

```bash
$ ss -tlnp | grep 6379
LISTEN 0 4096 *:6379 *:*
```

Recv-Q = 0，TCP accept 队列**没有满**，TCP 层面连接可以正常建立。

### Step 7: 确认事件饥饿机制

#### 上下文切换对比

| Thread | voluntary_switches | nonvoluntary_switches | CPU% |
|--------|-------------------|----------------------|------|
| 78 (gcs_server main) | 228,181,902 | 327,250 | 22.9% |
| 103 (ray_syncer_io_c) | 118,429,933 | 302,229 | 98.7% |
| 102 (pubsub_io_conte) | 503,577,157 | 307,721 | 28.6% |
| 225 (server.poll30) | 71,597,432 | 59,609 | 21.3% |

**关键线索**：主线程 (78) 有 **2.28 亿次主动切换**但 CPU 仅 22.9%，说明它在高频处理大量小事件（每次处理很快，但队列很深）。

---

## 根因结论

### 最终瓶颈机制：GCS 主线程 io_context 事件队列饥饿

Ray GCS 使用 `boost::asio::io_context` 单线程事件循环架构：

```
                                    ┌─────────────────────────────────┐
                                    │   GCS 主线程 io_context 队列     │
                                    │   (thread 78, 单线程顺序处理)     │
                                    └──────────┬──────────────────────┘
                                               │
              ┌────────────────────────────────┼────────────────────────┐
              │                                │                        │
    ray_syncer 收到节点资源报告          gRPC 收到 RPC 请求          其他事件
    → post 资源更新回调到队列           → post handler 回调到队列    (调度/heartbeat)
              │                                │                        │
              ▼                                ▼                        ▼
    [syncer_cb][syncer_cb][syncer_cb]...[rpc_cb]...[sched_cb]...
    ←────── 队列深度大，RPC 回调排在后面 ──────→
```

1. **ray_syncer_io_c (98.7% CPU)** 持续接收 ~1,298 节点的资源报告，处理后 post 资源更新回调到主线程 io_context
2. **gRPC 线程** 收到 `GetClusterID` / `GetNode` 等 RPC 后，也 post handler 回调到主 io_context
3. **主线程** 按 FIFO 顺序处理队列事件，syncer 回调占据大量位置
4. RPC handler 回调排在大量 syncer 回调后面，等待时间超过 5 秒 → **worker 超时**

### 为什么 Dashboard 能正常工作

Dashboard 走的是 `pubsub` 通道（thread 102），它通过**事件订阅推送**模式获取 actor/task 状态变更，不依赖主线程的 RPC 处理路径。且 Dashboard 有本地缓存，即使某次请求超时也会显示历史数据。

### 为什么 `QwenVLCPUPreprocessActor.__init__` 永远 RUNNING

完整因果链：

```
1. GCS 调度 actor 创建 → 选择目标 worker 节点
2. 目标节点 raylet 收到请求 → 启动新 worker 进程
3. Worker 进程执行 default_worker.py → 调用 Node.__init__()
4. Node.__init__() 需要调用 GetClusterID / GetNode RPC 连接 GCS
5. GCS 主线程忙于处理 syncer 回调，RPC 5 秒超时
6. Worker 进程启动失败，崩溃退出
7. GCS 侧：
   - actor 状态可能标记为 DEAD
   - 但 __init__ task 的状态更新需要通过 KillActor RPC 通知 raylet
   - raylet 所在节点已断连 → kill RPC 失败 (Socket closed)
   - task 状态停留在 RUNNING
8. Dashboard 从 task table 读取 → 显示 __init__ 为 RUNNING
```

---

## 关键证据汇总

| 证据 | 结论 |
|------|------|
| ray_syncer_io_c 98.7% CPU | syncer 广播负载极重 |
| 47,863 ESTAB / 1,298 节点 | 连接数远超正常水平 |
| 主线程 228M voluntary switches, 22.9% CPU | 高频处理小事件，队列深 |
| gRPC poll futex 地址在 CQ 内部 | 排除 gRPC 层锁竞争 |
| Listen Recv-Q = 0 | 排除 TCP accept 队列满 |
| `ray status` 本地 5 秒超时 | 确认 GCS RPC 无法响应 |
| GCS log: "Failed to kill actor NIL_ID" | 节点断连后清理失败 |
| Worker 报错 "Failed to connect to GCS" ×264 | 大规模 worker 启动失败 |

---

## 恶性循环

```
                    ┌─────────────────────────────────────────┐
                    │                                         │
                    ▼                                         │
GCS 主线程队列深 → RPC 响应慢 → worker 启动超时              │
        │                              │                      │
        │                              ▼                      │
        │                    actor 创建失败                    │
        │                              │                      │
        │                              ▼                      │
        │                    GCS 重新调度 actor                │
        │                              │                      │
        │                              ▼                      │
        │                    更多调度事件 post 到主线程 ────────┘
        │
        └── ray_syncer 持续广播给 1,298 节点（98.7% CPU）
            不断 post 资源更新回调 → 加剧队列堆积
```

---

## 排查方法论

### 1. 先确认"连不上"的层次

```bash
# TCP 层能连吗？
python3 -c "import socket; s=socket.socket(); s.settimeout(3); s.connect(('GCS_IP', 6379)); print('TCP OK')"

# gRPC 层能响应吗？
ray status  # 或 ray list nodes
```

- TCP OK + gRPC 超时 → 应用层问题（本案例）
- TCP 超时 → 网络/accept queue 问题

### 2. 定位 GCS 进程热点线程

```bash
# 线程级 CPU（必须用 -H 或 top -Hp）
top -Hp <gcs_pid> -bn1 | head -20

# 关键线程名对应
# ray_syncer_io_c  → 资源广播
# pubsub_io_conte  → 状态推送
# gcs_server (主)  → 核心事件循环
# server.pollNN    → gRPC 线程池
```

### 3. 确认连接数和来源

```bash
# 总连接数
ss -tn | grep <GCS_PORT> | wc -l

# 按 peer IP 分布
ss -tn | grep <GCS_PORT> | awk '{print $5}' | grep -oP '\d+\.\d+\.\d+\.\d+' | sort | uniq -c | sort -rn | head -20

# 独立 IP 数（≈节点数）
ss -tn | grep <GCS_PORT> | awk '{print $5}' | grep -oP '\d+\.\d+\.\d+\.\d+' | sort -u | wc -l
```

### 4. 判断锁竞争 vs 事件饥饿

```bash
# 检查 gRPC 线程在等什么
cat /proc/<pid>/task/<grpc_tid>/wchan
# futex_wait_queue → 在等某个 futex

# 获取 futex 地址
cat /proc/<pid>/task/<grpc_tid>/syscall
# 第1列=202(futex), 第2列=futex地址

# 检查该地址是否在应用数据区（锁竞争）还是线程栈（CQ 内部等待）
cat /proc/<pid>/maps | grep <地址前缀>

# 如果地址在已命名区域（heap/data段）→ 可能是应用锁
# 如果地址不在 maps 中 → 线程栈上的 CQ cond_var（正常）

# Listen backlog 是否满
ss -tlnp | grep <PORT>
# Recv-Q 接近 Send-Q → accept queue 满
```

### 5. 进一步 profiling（需要 perf）

```bash
# 安装 perf（需要匹配内核版本）
apt-get install linux-tools-$(uname -r)  # 标准内核
# 或快手自定义内核需要对应的 perf 包

# 采样主线程调用栈
perf record -p <gcs_pid> -t <main_tid> -g -- sleep 10
perf report

# 看锁竞争热点
perf lock record -p <gcs_pid> -- sleep 5
perf lock report
```

---

## 解决方案

### 短期（止血）

1. **Stop 多余 Job** — 减少 actor 数量，降低连接数和调度压力
2. **重启 Ray 集群** — 如果 GCS 已完全不响应，重新提交 KML task

### 中期（参数优化）

| 参数 | 当前值 | 建议 | 原因 |
|------|--------|------|------|
| streaming-gpu-concurrency | 200/1000 | ≤100 | 减少 actor 数 → 减少 GCS 连接数 |
| ray_syncer_message_refresh_interval_ms | 60000 | 120000 | 降低 syncer 广播频率 |
| raylet_report_resources_period_milliseconds | 10000 | 30000 | 减少资源上报频率 |
| health_check_period_ms | 10000 | 30000 | 减少心跳频率 |
| 同时提交 Job 数 | 2 | 1 | 避免 actor 叠加 |

### 长期（架构改进）

1. **分集群部署** — 不同 Job 使用独立 Ray 集群，避免共享 GCS
2. **GCS HA / 多实例** — 评估 Ray 2.x 的 GCS fault tolerance 特性
3. **连接复用** — 评估 worker 进程是否可共享 GCS 连接（当前每个 worker 独立连接）
4. **升级 Ray 版本** — 新版本对 GCS 大规模场景有优化（如 gRPC thread affinity、io_context 分片）

---

## 附录：Ray GCS 线程模型

```
GCS Server Process (pid=78)
│
├── Main Thread (tid=78, "gcs_server")
│   └── boost::asio::io_context.run()
│       ├── 处理 gRPC RPC handler 回调
│       ├── 处理 ray_syncer 投递的资源更新
│       ├── 处理 actor 调度/清理
│       └── 处理 heartbeat 超时回调
│
├── RaySyncer IO Thread (tid=103, "ray_syncer_io_c")
│   └── 独立 io_context
│       ├── 接收各节点资源报告
│       ├── 计算资源 diff
│       └── 广播资源变更到所有节点 (fan-out)
│
├── PubSub IO Thread (tid=102, "pubsub_io_conte")
│   └── 独立 io_context
│       ├── actor/task 状态变更事件
│       └── 推送给订阅者 (Dashboard 等)
│
├── gRPC Server Thread Pool (64 threads, "server.pollNN")
│   └── CompletionQueue polling
│       ├── 接收 RPC 请求
│       ├── 反序列化
│       └── post handler 到主线程 io_context
│
└── 其他线程 (timer, object manager, etc.)
```

---

## 参考

- Ray GCS 源码: `src/ray/gcs/gcs_server/`
- RaySyncer: `src/ray/common/ray_syncer/`
- GCS Actor Scheduler: `src/ray/gcs/gcs_server/gcs_actor_scheduler.cc`
- Worker 启动: `python/ray/_private/workers/default_worker.py`
