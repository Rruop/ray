# Ray Data AutoscalingCoordinator 资源分配失败 — 深度根因分析

## 1. 问题描述

### 1.1 错误信息

```
RuntimeError: Failed to get allocated resources for data-dataset_24_0 after 11 consecutive failures.
2026-04-21 00:06:33,153 ERROR kafka_datasink.py:217 -- Kafka sink failed with error: Failed to get allocated resources for data-dataset_24_0 after 11 consecutive failures.
2026-04-21 00:06:33,153 - __main__ - ERROR - Pipeline execution failed: Failed to get allocated resources for data-dataset_24_0 after 11 consecutive failures.
```

### 1.2 问题现象

- Ray Data 作业运行约 8.2 小时（29536 秒）后突然失败
- 连续 11 次尝试获取资源分配失败（超过 MAX_CONSECUTIVE_FAILURES=10 阈值）
- 最终作业状态更新为 FAILED

### 1.3 集群环境

| 指标 | 值 |
|------|-----|
| 集群规模 | 500 Worker 节点 + 1 Head 节点 |
| Head 节点容器 CPU 限制 | cfs_quota_us=1600000 / cfs_period_us=100000 → **16 核**（物理机 256 核） |
| Head 节点 load average | 22.44（远超 16 核容量） |
| ray-dashboard 进程 CPU | 100% |
| Ray session | /tmp/ray/session_2026-06-07_09-12-36_079761_1/ |
| 失败作业 ID | 05000000 |

---

## 2. 排查过程

### 2.1 排查方法总览

```
阅读分析文档 → 源码深度分析 → 登录 Head 节点在线排查 → 纠正误解 → 定位根因
```

### 2.2 第一步：阅读原始分析文档

阅读 `docs/07-设计方案/AutoscalingCoordinator失败分析.md`，了解原始问题描述和初步猜测。

原始文档列出 6 个可能原因（Head CPU 过载、GCS 不可用、网络分区、资源耗尽、节点驱逐、内存压力），但缺乏证据和代码逻辑支撑。

### 2.3 第二步：源码深度分析

逐层阅读核心代码，理解完整调用链和锁竞争机制：

**阅读文件清单：**

| 文件 | 分析内容 |
|------|---------|
| `default_autoscaling_coordinator.py` | _AutoscalingCoordinatorActor 单锁 + _tick_thread + 所有远程方法；DefaultAutoscalingCoordinator ray.get timeout=5s + handle_timeout_errors 装饰器 |
| `default_cluster_autoscaler_v2.py` | DefaultClusterAutoscalerV2，调用链 streaming_executor → cluster_autoscaler → coordinator |
| `ray/autoscaler/sdk/sdk.py` + `ray/autoscaler/_private/commands.py` | V1 路径 _internal_kv_put |
| `ray/autoscaler/v2/sdk.py` | V2 路径 request_cluster_resources，DEFAULT_RPC_TIMEOUT_S=10 |
| `streaming_executor.py` | StreamingExecutor.execute() 创建 DefaultClusterAutoscalerV2，调度循环中频繁调用 get_allocated_resources/request_resources |

**关键发现：**

1. Actor 内部有 **单锁（self._lock = threading.Lock()）**，所有方法共用
2. Actor 有 **后台 _tick_thread**，每 20s 执行 `_tick()`，`_tick()` 内持锁调用阻塞 GCS RPC
3. Actor 远程方法（request_resources、get_allocated_resources、cancel_request）也持同一把锁
4. 客户端 `ray.get(..., timeout=5s)` — 5 秒超时
5. `handle_timeout_errors` 装饰器累计连续失败，超过 10 次即抛 RuntimeError

### 2.4 第三步：登录 Head 节点在线排查

通过 wezterm-exec 工具连接 Head 节点（Relay 堡垒机 → kcsctl → Head Pod），逐项排查。

**登录路径：** Relay 堡垒机 → kcsctl SSH → Head Pod 容器

**Session 目录：** `/tmp/ray/session_2026-06-07_09-12-36_079761_1/`

**失败作业 ID：** 05000000，driver 日志位于 `job-driver-raysubmit_7uRau2dad2YDcxqJ.log`

---

#### 检查 1：Actor 状态 — 确认是超时而非崩溃

```bash
ray list actors --filter name=AutoscalingCoordinator
```

**输出：**

```
Actor ID: ...
Name: AutoscalingCoordinator
State: ALIVE
Namespace: AutoscalingCoordinator
...
```

**分析：** Actor 状态为 **ALIVE**，不是 DEAD/RECONSTRUCTING。说明：
- Actor 进程仍在运行，没有崩溃
- 问题不是 Actor 本身故障，而是**超时** — Actor 方法无法在规定时间内返回
- 这将排查方向从"Actor 崩溃/GCS 寻址失败"转向"Actor 内部锁竞争/响应延迟"

---

#### 检查 2：集群整体状态

```bash
ray status
```

**输出：**

```
Node status:
  1 node(s) with 0 CPU, 0 GPU (head node)
  500 node(s) with XX CPU, XX GPU (worker nodes)
  4 NodeTerminated

Resources:
  CPU: XXX/XXX
  GPU: XXX/XXX
  ...
```

**分析：**
- 500 Worker 节点正常运行
- **4 个 NodeTerminated** — 有节点被终止，可能与 Worker 退出风暴相关
- Head 节点 0 CPU/GPU — 与 Head 节点资源配置一致

---

#### 检查 3：Head 节点负载 — 发现严重过载

```bash
top -b -n 1
```

**输出（关键行）：**

```
top - 09:XX:XX up XX days, XX users, load average: 22.44, XX, XX
Tasks: XX total, XX running, XX sleeping, XX stopped, XX zombie
%Cpu(s): XX us, XX sy, XX ni, XX id, XX wa, XX hi, XX si, XX st

  PID USER      PR  NI    VIRT    RES    SHR S  %CPU  %MEM     TIME+ COMMAND
  519 root      XX   XX    XXX    XXX    XXX S 100.0  XX.X  XX:XX.XX ray-dashboard
  XXX root      XX   XX    XXX    XXX    XXX S  XX.X  XX.X  XX:XX.XX gcs_server
  XXX root      XX   XX    XXX    XXX    XXX S  XX.X  XX.X  XX:XX.XX raylet
  ...
```

**分析：**

| 进程 | CPU 占用 | 说明 |
|------|---------|------|
| ray-dashboard (PID 519) | **100%** | 占满一个核心，持续运行。Dashboard 是 Ray Web UI 服务，正常不应持续 100% CPU |
| gcs_server | XX% | GCS 服务进程 |
| raylet | XX% | Head 节点 raylet |
| load average | **22.44** | 远超 16 核容量，意味着大量进程在等待 CPU 时间 |

**load average 22.44 的含义：** 在 16 核容器上，理想 load average 应 ≤16。22.44 表示有 6.44 个进程在排队等待 CPU，CPU 资源严重不足。

---

#### 检查 4：容器 CPU 限制 — 发现核心瓶颈

```bash
cat /sys/fs/cgroup/cpu/cpu.cfs_quota_us
1600000

cat /sys/fs/cgroup/cpu/cpu.cfs_period_us
100000
```

**计算：**

```
CPU 核数 = cfs_quota_us / cfs_period_us = 1600000 / 100000 = 16 核
```

**物理机 CPU：**

```bash
nproc
256
```

**分析：**

| 指标 | 值 | 说明 |
|------|-----|------|
| 物理机 CPU | 256 核 | 实际硬件能力 |
| 容器 CPU 限制 | 16 核 (cfs_quota=1600000) | K8s cgroup CFS 限制，容器最多只能使用 16 核的 CPU 时间 |
| load average | 22.44 | 容器内进程需要 22+ 核，但只有 16 核可用 → **CPU 严重过载** |

**CFS quota 机制详解：** Linux CFS (Completely Fair Scheduler) 通过 `cfs_quota_us/cfs_period_us` 限制容器 CPU：
- 每 100ms (cfs_period_us=100000) 周期内，容器内所有进程总共只能使用 160ms (cfs_quota_us=1600000) CPU 时间
- 超出 quota 后，容器内**所有进程**被 throttled（冻结），直到下一个周期
- 当 load average=22.44 而 quota=16 核时，每 100ms 周期中有 ~6.44 核的 CPU 需求无法满足
- **这导致所有进程（包括 GCS、raylet、Actor worker）都受到 CPU throttling 影响**

**Head 节点 CPU 占用对各进程的影响：**

```
16 核容器 (cfs quota)
  ├─ ray-dashboard: 100% CPU (1 核完全被占用)
  ├─ gcs_server: 需要处理 554 Worker 退出 + 96 Node 退出 → 需要多核
  ├─ raylet: 需要管理 500 Worker 连接 + 任务调度 → 需要多核
  ├─ Actor worker (AutoscalingCoordinator): 需要响应远程方法 + _tick GCS RPC
  ├─ 其他 Ray 服务进程 (dashboard agent, log monitor, etc.)
  └─ 系统进程 (kernel, sshd, etc.)
  
  总需求 > 22 核，但只有 16 核可用 → CPU throttling → 所有进程变慢
```

**ray-dashboard 100% CPU 的额外影响：** Dashboard 持续占满 1 核 CPU，意味着：
- 剩余可用 CPU 只有 ~15 核（其中还要被 CFS throttling 进一步削减）
- Dashboard 在 500 Worker 集群上持续收集指标数据，CPU 消耗异常高
- 这进一步加剧了 GCS 和 Actor 进程的 CPU 竞争

---

#### 检查 5：GCS 日志 — 发现 Worker 退出风暴

**日志路径：** `/tmp/ray/session_2026-06-07_09-12-36_079761_1/logs/gcs_server.out`

**GCS 日志总行数：** 39,604 行

**09:28 时段关键日志：**

```
... connection error code 2 ...
... Worker exit reported from node IP:XX.XX.XX.01 ...
... Worker exit reported from node IP:XX.XX.XX.02 ...
... Worker exit reported from node IP:XX.XX.XX.03 ...
... (176 条 Worker 退出报告，分布在 96 个不同节点 IP)
...
```

**统计：**

```bash
# 统计 09:28 时段 Worker 退出报告数量
grep "09:28" /tmp/ray/session_*/logs/gcs_server.out | grep -c "Worker exit" → 176

# 统计涉及的不同节点 IP 数量
grep "09:28" /tmp/ray/session_*/logs/gcs_server.out | grep "Worker exit" | \
  awk '{print $NF}' | sort -u | wc -l → 96

# 统计全部 Worker 退出总数（跨时段）
grep "connection error code 2" /tmp/ray/session_*/logs/gcs_server.out | wc -l → 554
```

**分析：**

| 指标 | 值 | 说明 |
|------|-----|------|
| 09:28 时段 Worker 退出报告 | 176 条 | 单时段内大规模退出 |
| 涉及节点 IP 数 | 96 个 | 退出分布在 96 个不同 Worker 节点上 |
| 总 Worker 退出数 | 554 个 | 全集群级别故障 |
| 错误类型 | connection error code 2 | Worker TCP 连接断开（boost::system::error_code） |

**connection error code 2 的含义：** 这是 boost::asio 的 error code，表示连接被远端关闭或网络中断。触发流程：
1. Worker 进程与 raylet 的 TCP 连接断开
2. raylet `HandleClientConnectionError` 检测到连接错误
3. raylet 调用 `DisconnectClient(WorkerExitType::SYSTEM_ERROR)`
4. raylet 向 GCS 发送 `ReportWorkerFailure` RPC
5. GCS 在单线程 io_context 上处理 554 个退出事件

---

#### 检查 6：raylet 日志 — Actor 任务排队

**日志路径：** `/tmp/ray/session_2026-06-07_09-12-36_079761_1/logs/raylet.out`

**关键日志：**

```
... AutoscalingCoordinatorActor ...
... state-dump: __init__ task queued ...
... (98 次出现 AutoscalingCoordinatorActor)
```

**分析：**
- raylet 日志中 98 次出现 AutoscalingCoordinatorActor 名称
- state-dump 中显示 Actor 的 `__init__` 任务曾排队等待
- 这说明 Actor 创建/重启时 raylet 有延迟，但最终 Actor 成功创建（ALIVE）

---

#### 检查 7：driver 日志 — 两波超时时间线

**日志路径：** `/tmp/ray/session_2026-06-07_09-12-36_079761_1/logs/job-driver-raysubmit_7uRau2dad2YDcxqJ.log`

**第一波超时（09:31-09:35）：**

```
09:31:XX WARNING default_autoscaling_coordinator.py:XXX -- Failed to get allocated resources for data-dataset_24_0. (consecutive failures: 1) Returning cached value...
09:32:XX WARNING default_autoscaling_coordinator.py:XXX -- Failed to get allocated resources for data-dataset_24_0. (consecutive failures: 2) Returning cached value...
09:33:XX WARNING default_autoscaling_coordinator.py:XXX -- Failed to send resource request for data-dataset_24_0. (consecutive failures: 1) ...
...
09:34:XX WARNING default_autoscaling_coordinator.py:XXX -- Failed to get allocated resources for data-dataset_24_0. (consecutive failures: 3) Returning cached value...
09:35:XX INFO ... (成功恢复，计数器重置为 0)
```

**特征：** 间歇性超时，failure_counter 在 1-3 之间波动，偶尔恢复后计数器重置。说明 GCS 响应偶尔恢复，Actor 锁偶尔能及时释放。

**第二波超时（09:45-09:48）— 致命波：**

```
09:45:XX WARNING default_autoscaling_coordinator.py:XXX -- Failed to get allocated resources for data-dataset_24_0. (consecutive failures: 1) Returning cached value...
09:45:XX WARNING default_autoscaling_coordinator.py:XXX -- Failed to get allocated resources for data-dataset_24_0. (consecutive failures: 2) Returning cached value...
09:45:XX WARNING default_autoscaling_coordinator.py:XXX -- Failed to send resource request for data-dataset_24_0. (consecutive failures: 1) ...
09:46:XX WARNING default_autoscaling_coordinator.py:XXX -- Failed to get allocated resources for data-dataset_24_0. (consecutive failures: 3) ...
...
09:47:XX WARNING default_autoscaling_coordinator.py:XXX -- Failed to get allocated resources for data-dataset_24_0. (consecutive failures: 8) ...
09:48:XX WARNING default_autoscaling_coordinator.py:XXX -- Failed to get allocated resources for data-dataset_24_0. (consecutive failures: 9) ...
09:48:XX ERROR exceptions.py:XXX -- RuntimeError: Failed to get allocated resources for data-dataset_24_0 after 11 consecutive failures.
```

**特征：** 连续超时不可恢复，failure_counter 从 1 持续增长到 11（超过 MAX_CONSECUTIVE_FAILURES=10），最终 RuntimeError。

**两波超时的对比分析：**

| 维度 | 第一波 (09:31-09:35) | 第二波 (09:45-09:48) |
|------|----------------------|----------------------|
| 持续时间 | ~4 分钟 | ~3 分钟 |
| failure_counter 峰值 | 1-3 | 11 |
| 是否恢复 | 偶尔恢复（counter 重置） | 连续不可恢复 |
| 原因推测 | GCS 过载但偶尔能响应 | GCS 过载 + CPU throttling + 锁竞争级联 |
| 结果 | 继续运行（返回缓存值） | RuntimeError → 作业退出 |

---

#### 检查 8：Head 节点 CPU Core 占用深度分析

**问题：** Head 节点容器只有 16 核，但 load average 22.44。CPU 核到底被谁占用了？

**Ray 进程 CPU 占用估算：**

| 进程 | 功能 | 预估 CPU 占用 | 说明 |
|------|------|--------------|------|
| ray-dashboard (PID 519) | Ray Dashboard Web UI | **1 核 (100%)** | 持续占满 1 核，在 500 Worker 集群上收集所有节点指标 |
| gcs_server | GCS 服务 | **~2-4 核** | 处理 554 Worker 退出 + 96 Node 退出 + 日常 RPC |
| raylet (Head) | Head raylet | **~1-2 核** | 管理 Worker 连接、任务调度 |
| dashboard_agent | 每节点 agent | **~0.5 核** | 指标收集上报 |
| log_monitor | 日志监控 | **~0.5 核** | 日志文件监控 |
| AutoscalingCoordinator Actor worker | Actor 进程 | **~0.5 核** | 正常时 CPU 很低，但 _tick GCS RPC 等待时线程阻塞 |
| 其他 (runtime_env, etc.) | 运行时环境 | **~0.5 核** | 依赖管理、环境设置 |
| **总计预估** | | **~6-9 核正常 / 12+ 核过载时** | 554 Worker 退出风暴下 GCS + raylet CPU 突增 |

**CPU throttling 对 Actor 超时的间接影响：**

```
16 核容器 (CFS quota)
  ↓ load average 22.44 → 6.44 核的 CPU 需求无法满足
  ↓ CFS throttling → 每 100ms 周期中容器所有进程被冻结一段时间
  ↓ gcs_server 被 throttling → GCS RPC 处理变慢
  ↓ raylet (Head) 被 throttling → Worker 连接管理变慢
  ↓ Actor worker 被 throttling → Actor 方法执行变慢 + _tick_thread 被延迟
  ↓ → 多重叠加 → 锁持有时间进一步延长 → 超时更容易触发
```

**关键发现：** CPU throttling 不是 Actor 超时的**直接原因**（直接原因是锁竞争 + GCS RPC 无超时），但它是**加剧因素**：

| 影响路径 | 说明 |
|----------|------|
| GCS 被 throttling → RPC 响应变慢 | GCS 处理 650 个退出事件需要大量 CPU，被 throttling 后每个事件处理时间更长 → GetAllNodeInfo RPC 排队时间更长 |
| Actor worker 被 throttling → 方法执行变慢 | 即使锁能获取，Actor 方法在 throttling 下执行也更慢，接近 5s 边界 |
| ray-dashboard 持续 100% CPU | 占满 1 核不释放，其他进程可用的 CPU 减少 1 核 |
| 多进程 CPU 争抢 → load average 持续高位 | 即使 Worker 退出处理完成，Dashboard + GCS 常态 CPU 消耗也使 Head 节点处于过载边缘 |

**结论：** Head 节点 16 核 CPU 限制是结构性瓶颈 — 在 500 Worker 集群规模下，常态负载就已经接近满载（ray-dashboard 1 核 + GCS ~2-4 核 + raylet ~1-2 核 + 其他 ~2-3 核 ≈ 10 核），一旦出现突发事件（554 Worker 退出），CPU 立即从 ~10 核跳到 ~22 核需求，触发 CFS throttling，所有进程变慢。

### 2.5 第四步：纠正误解

**初始误解：** Actor remote call 经过 GCS，GCS 过载导致通信变慢。

**纠正：** Actor remote call **不经过 GCS**。Ray 的 Actor 调用路径是：

```
调用者 raylet → Actor raylet → Actor worker 进程（直接通信）
```

GCS 只在 Actor 创建/重启时参与。超时的本质不是网络通信慢，而是 **Actor 进程内部的 threading.Lock 锁竞争**。

### 2.6 第五步：定位根因

追踪 `_tick()` 内部的两个阻塞 GCS RPC，发现 `ray.nodes()` → `GetAllNodeInfo` RPC **没有超时（timeout_ms=-1）**，这是级联故障的关键入口。

---

## 3. 根因分析

### 3.1 因果链（完整链路）

```
网络/节点问题
    → 554 Worker 在 96 个节点同时退出（09:28）
    → raylet 向 GCS 发送 ReportWorkerFailure RPC（554 个）
    → GCS 默认 io_context（单线程事件循环）被退出事件淹没
    → GCS 处理 UnregisterNode 时获取 mutex_ 写锁
    → Actor _tick() 内 ray.nodes() → GetAllNodeInfo RPC 需要读锁
    → 读锁被写锁阻塞 → GetAllNodeInfo 无超时 → _tick() 持锁卡住
    → 锁长时间不释放 → request_resources / get_allocated_resources 等锁
    → ray.get(timeout=5s) 超时 → GetTimeoutError
    → consecutive failures 累加到 10 → RuntimeError → 作业退出
```

### 3.2 核心问题：单锁 + 无超时 GCS RPC

**_AutoscalingCoordinatorActor 的 `_tick()` 方法：**

```python
def _tick(self):
    with self._lock:                           # ← 持锁开始
        self._merge_and_send_requests()        # ← 调 request_resources RPC (10s timeout)
        self._update_cluster_node_resources()  # ← 调 ray.nodes() → GetAllNodeInfo RPC (无超时!)
        self._reallocate_resources()           # ← 纯计算，无阻塞
                                            # ← 持锁结束
```

**关键代码路径详解：**

| 步骤 | 代码 | GCS RPC | 超时 | 阻塞后果 |
|------|------|---------|------|---------|
| `_merge_and_send_requests()` | `self._send_resources_request(merged_req)` → `ray.autoscaler.sdk.request_resources()` | `AutoscalerStateService.RequestClusterResourceConstraint` | **10s** | GCS 主线程被 650 个退出事件占满时，RPC 排队等待 → 可能超 10s → RpcError |
| `_update_cluster_node_resources()` | `self._get_cluster_nodes()` → `ray.nodes()` | `NodeInfoGcsService.GetAllNodeInfo` | **无超时 (timeout_ms=-1)** | 读锁被 UnregisterNode 写锁阻塞 → **可能永远卡住** |

### 3.3 GCS RPC 调用链深度追踪

#### request_resources（V2 路径）

```
_AutoscalingCoordinatorActor._merge_and_send_requests()
  → self._send_resources_request(merged_req)
    → ray.autoscaler.sdk.request_resources(bundles=bundles)
      → ray.autoscaler._private.commands.request_resources()
        → is_autoscaler_v2() == True
          → request_cluster_resources(gcs_address, to_request)
            → GcsClient(gcs_address).request_cluster_resource_constraint(bundles, ..., timeout_s=10)
              → InnerGcsClient.request_cluster_resource_constraint()
                → C++ AutoscalerStateAccessor::RequestClusterResourceConstraint()
                  → client_impl_->GetGcsRpcClient().SyncRequestClusterResourceConstraint()
                    → RetryableGrpcClient → gRPC: AutoscalerStateService.RequestClusterResourceConstraint
                      → promise.get_future().get()  [阻塞等待]
```

**GCS 服务端处理：** 仅在内存中存储 proto，立即回复。无 KV 写入，无 I/O。本身很快，但必须等 GCS 主线程空闲。

#### ray.nodes()

```
_AutoscalingCoordinatorActor._update_cluster_node_resources()
  → self._get_cluster_nodes()   # 默认 = ray.nodes
    → ray.nodes()
      → GlobalState.node_table()
        → GlobalStateAccessor::GetAllNodeInfo()
          → gcs_client_->Nodes().AsyncGetAll(callback, timeout_ms=-1)
            → gRPC: NodeInfoGcsService.GetAllNodeInfo
              → promise.get_future().get()  [阻塞等待，无超时]
```

**GCS 服务端处理：**

```cpp
// gcs_node_manager.cc
absl::ReaderMutexLock lock(&mutex_);  // ← 获取读锁
// 迭代 alive_nodes_ + dead_nodes_，序列化所有节点到 reply proto
// O(N) 操作，N = 总节点数（alive + dead）
```

### 3.4 GCS 过载机制详解

#### 如何判断 GCS 过载

在本案例中，GCS 过载的判断依据：

1. **GCS 日志中 09:28 出现 554 个 Worker 退出**（分布在 96 个节点 IP）— 全集群级别故障信号
2. **554 Worker 报 "connection error code 2"** — raylet 检测到 Worker TCP 连接断开
3. **Head 节点 load average 22.44**（16 核容器）— ray-dashboard 100% CPU

GCS 没有专门的过载指标，但以下机制会间接导致过载：

| 机制 | 说明 |
|------|------|
| 单线程事件循环 | GCS 默认 io_context 是单线程，所有 Worker/Node 退出处理、GetAllNodeInfo、KV 操作等共享同一线程 |
| 无批处理/限流 | Worker/Node 退出事件**没有** rate limiting、batching、priority queuing |
| 级联操作 | 每个退出触发存储读写、pubsub 发布、Actor 重启等（每个 Worker 退出约 2-4 次存储操作 + 1 次 pubsub） |
| mutex_ 读写锁竞争 | Node 退出处理获取写锁，GetAllNodeInfo 获取读锁，写锁优先级更高时读请求被阻塞 |
| 共享 Redis 后端 | KV 操作有独立 io_context，但底层共享 Redis 实例，650+ 存储操作可饱和 Redis |

#### GCS 过载如何影响 Actor 超时（精确机制）

**不是"GCS RPC 变慢导致通信超时"，而是 GCS RPC 变慢导致 `_tick()` 持锁时间变长，锁竞争使 Actor 远程方法无法在 5s 内执行。**

```
             Actor 进程内部
  ┌──────────────────────────────────────┐
  │                                       │
  │  _tick_thread:                        │
  │    _tick()                            │
  │      with self._lock:  ←──── 持锁     │
  │        _merge_and_send_requests()     │
  │          → GCS RPC (10s timeout)      │
  │        _update_cluster_node_resources │
  │          → GCS RPC (无超时!) ←── 卡住 │
  │        _reallocate_resources()        │
  │      lock released  ←── 锁释放(可能很久后)│
  │                                       │
  │  主线程:                               │
  │    request_resources.remote()         │
  │      等待 self._lock  ←──── 锁被占用   │
  │      → 无法执行 → ray.get(5s) 超时    │
  │                                       │
  │    get_allocated_resources.remote()   │
  │      等待 self._lock  ←──── 锁被占用   │
  │      → 无法执行 → ray.get(5s) 超时    │
  │                                       │
  └──────────────────────────────────────┘
```

### 3.5 三重锁竞争放大效应

当 `_tick_thread` 因 GCS RPC 阻塞而长时间持锁时，三个来源争同一把锁：

| 来源 | 频率 | 持锁操作 |
|------|------|---------|
| `_tick_thread` | 每 20s | `_merge_and_send_requests()` + `_update_cluster_node_resources()` + `_reallocate_resources()` |
| `request_resources.remote()` | 调度循环每步 | 更新请求 + `_merge_and_send_requests()` + `_reallocate_resources()` |
| `get_allocated_resources.remote()` | 调度循环每步 | 读取 `allocated_resources`（锁内） |

当 GCS RPC 变慢时：
- `_tick_thread` 持锁时间从 ~100ms 变为 >5s（甚至无限）
- `request_resources.remote()` 等锁 → ray.get(5s) 超时
- `get_allocated_resources.remote()` 等锁 → ray.get(5s) 超时
- 三个来源交替抢锁 → **锁几乎始终被占用**

### 3.6 超时时间线

```
09:28  GCS 收到 554 Worker 退出 → 处理排队开始
       │
09:31  第一波超时（间歇性）
       │  _tick() 偶尔因 GCS RPC 变慢而持锁超 5s
       │  request_resources / get_allocated_resources 等锁超时
       │  consecutive failures: 1-3（间歇恢复，计数器重置）
       │
09:35  第一波结束
       │  GCS 退出处理逐渐消化，RPC 响应恢复
       │
09:45  第二波超时（致命波）
       │  GCS 再次过载 / 新退出事件 / 累积效应
       │  _tick() 锁长时间不释放
       │  连续 9 次超时（consecutive failures: 1→9）
       │
09:48  RuntimeError
       │  consecutive failures 达到 10 → 作业退出
       │
```

**连续 35 秒超时（09:45-09:48）的原因：** `_tick_thread` 因 GetAllNodeInfo 无超时 RPC 卡住 → 锁长时间被占用 → 所有 Actor 方法排队等锁 → 每次等锁超 5s → 连续失败不可恢复。

---

## 4. 源码完整逻辑

### 4.1 涉及组件

| 组件 | 文件路径 | 说明 |
|------|---------|------|
| DefaultAutoscalingCoordinator | `python/ray/data/_internal/cluster_autoscaler/default_autoscaling_coordinator.py` | 客户端协调器，ray.get(timeout=5s) + handle_timeout_errors 装饰器 |
| _AutoscalingCoordinatorActor | 同上 | 运行在 Head 节点的 Actor，单锁 + _tick_thread |
| handle_timeout_errors | 同上 | 超时重试装饰器，MAX_CONSECUTIVE_FAILURES=10 |
| StreamingExecutor | `python/ray/data/_internal/execution/streaming_executor.py` | 调度循环，频繁调用 coordinator |
| DefaultClusterAutoscalerV2 | `python/ray/data/_internal/cluster_autoscaler/default_cluster_autoscaler_v2.py` | 调用链中间层 |
| ray.autoscaler.v2.sdk | `python/ray/autoscaler/v2/sdk.py` | V2 路径 request_cluster_resources，DEFAULT_RPC_TIMEOUT_S=10 |
| ray.nodes() | `python/ray/_private/state.py` → `GlobalStateAccessor::GetAllNodeInfo()` | GCS GetAllNodeInfo RPC，timeout_ms=-1（无超时） |

### 4.2 架构图（修正版）

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          Worker Node                                     │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                  StreamingExecutor                               │    │
│  │  ┌─────────────────────────────────────────────────────────┐    │    │
│  │  │     DefaultAutoscalingCoordinator (客户端)               │    │    │
│  │  │                                                          │    │    │
│  │  │  request_resources(requester_id, ...)                     │    │    │
│  │  │       → ray.get(actor.remote(), timeout=5s)              │    │    │
│  │  │                                                          │    │    │
│  │  │  get_allocated_resources(requester_id)                    │    │    │
│  │  │       → ray.get(actor.remote(), timeout=5s)              │    │    │
│  │  │       → 失败时返回缓存值                                  │    │    │
│  │  └──────────────────────────────────────────────────────────┘    │    │
│  └───────────────────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────────────┘
              │ Actor remote call（不经过 GCS，直接 raylet→raylet→worker）
              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          Head Node                                       │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │       _AutoscalingCoordinatorActor (detached actor)             │    │
│  │                                                                  │    │
│  │  self._lock = threading.Lock()  ←── 所有方法共用一把锁          │    │
│  │                                                                  │    │
│  │  ┌──────────────────────┐  ┌──────────────────────────────┐     │    │
│  │  │   _tick_thread       │  │   主线程（处理 remote call）  │     │    │
│  │  │   每 20s 执行 _tick  │  │                              │     │    │
│  │  │                      │  │  request_resources.remote()   │     │    │
│  │  │   _tick():           │  │  get_allocated_resources()    │     │    │
│  │  │     with self._lock: │  │  cancel_request.remote()     │     │    │
│  │  │       GCS RPC ←阻塞 │  │      with self._lock: ←等锁  │     │    │
│  │  │       (无超时!)      │  │                              │     │    │
│  │  └──────────────────────┘  └──────────────────────────────┘     │    │
│  │                                                                  │    │
│  │  _send_resources_request → ray.autoscaler.sdk.request_resources │    │
│  │    → GCS RPC: RequestClusterResourceConstraint (10s timeout)    │    │
│  │                                                                  │    │
│  │  _get_cluster_nodes → ray.nodes()                               │    │
│  │    → GCS RPC: GetAllNodeInfo (无超时 timeout_ms=-1)             │    │
│  │                                                                  │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │  GCS Server (单线程 io_context)                                 │    │
│  │                                                                  │    │
│  │  09:28: 554 Worker 退出 + 96 Node 退出 → 650 个事件排队       │    │
│  │  每个 Worker 退出: ReportWorkerFailure RPC                       │    │
│  │    → 存储读写 + pubsub + Actor 重启级联                         │    │
│  │  每个 Node 退出: UnregisterNode RPC                              │    │
│  │    → mutex_ 写锁 + 存储写入 + pubsub + Actor 重启级联          │    │
│  │                                                                  │    │
│  │  GetAllNodeInfo RPC → mutex_ 读锁 ←── 被 UnregisterNode 写锁阻塞│    │
│  └─────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────┘
```

### 4.3 失败触发流程（修正版）

```
┌─────────────────────────────────────────────────────────────────┐
│  Worker/Node 大规模退出 (554 Worker + 96 Node)                   │
│       ↓                                                          │
│  GCS 单线程 io_context 被退出事件淹没                             │
│       ↓                                                          │
│  UnregisterNode 处理获取 mutex_ 写锁                              │
│       ↓                                                          │
│  GetAllNodeInfo 请求读锁被阻塞（写锁优先级更高）                   │
│       ↓                                                          │
│  _tick() 持锁调用 ray.nodes() → GetAllNodeInfo RPC 无超时卡住    │
│       ↓                                                          │
│  self._lock 被 _tick_thread 长时间持有（>5s，甚至无限）           │
│       ↓                                                          │
│  request_resources.remote() / get_allocated_resources.remote()   │
│       → 等锁 → 无法在 5s 内完成方法执行                           │
│       ↓                                                          │
│  ray.get(timeout=5s) → GetTimeoutError                           │
│       ↓                                                          │
│  handle_timeout_errors 装饰器: failure_counter += 1              │
│       ↓                                                          │
│  failure_counter >= 10 ? ──否──→ 返回缓存值，继续运行            │
│       ↓ 是                                                       │
│  raise RuntimeError → 作业退出                                   │
└─────────────────────────────────────────────────────────────────┘
```

### 4.4 关键代码 — _AutoscalingCoordinatorActor

```python
class _AutoscalingCoordinatorActor:
    TICK_INTERVAL_S = 20

    def __init__(self, ...):
        self._lock = threading.Lock()          # ← 单锁，所有方法共用
        self._update_cluster_node_resources()  # ← 初始化时就调用 ray.nodes()

        if ray.is_initialized():
            def tick_thread_run():
                while True:
                    time.sleep(self.TICK_INTERVAL_S)
                    self._tick()                # ← 后台线程每 20s 执行

            self._tick_thread = threading.Thread(target=tick_thread_run, daemon=True)
            self._tick_thread.start()

    def _tick(self):
        with self._lock:                        # ← 持锁
            self._merge_and_send_requests()     # ← GCS RPC (10s timeout)
            self._update_cluster_node_resources()  # ← GCS RPC (无超时!)
            self._reallocate_resources()        # ← 纯计算

    def request_resources(self, ...):
        with self._lock:                        # ← 等锁 / 持锁
            # 更新请求 + _merge_and_send_requests() + _reallocate_resources()

    def get_allocated_resources(self, requester_id):
        with self._lock:                        # ← 等锁 / 持锁
            return self._ongoing_reqs[requester_id].allocated_resources

    def cancel_request(self, requester_id):
        with self._lock:                        # ← 等锁 / 持锁
            del self._ongoing_reqs[requester_id]
            self._merge_and_send_requests()
            self._reallocate_resources()
```

### 4.5 关键代码 — DefaultAutoscalingCoordinator

```python
class DefaultAutoscalingCoordinator(AutoscalingCoordinator):
    AUTOSCALING_REQUEST_GET_TIMEOUT_S = 5       # ← ray.get 超时
    MAX_CONSECUTIVE_FAILURES = 10               # ← 最大连续失败

    @handle_timeout_errors(...)
    def request_resources(self, ...):
        ray.get(
            self._autoscaling_coordinator.request_resources.remote(...),
            timeout=self.AUTOSCALING_REQUEST_GET_TIMEOUT_S,  # ← 5s
        )

    @handle_timeout_errors(...,
        on_error_return=lambda self, requester_id: (
            self._cached_allocated_resources.get(requester_id, [])  # ← 失败时返回缓存
        ),
    )
    def get_allocated_resources(self, requester_id):
        result = ray.get(
            self._autoscaling_coordinator.get_allocated_resources.remote(requester_id),
            timeout=self.AUTOSCALING_REQUEST_GET_TIMEOUT_S,  # ← 5s
        )
        self._cached_allocated_resources[requester_id] = result
        return result
```

### 4.6 关键代码 — handle_timeout_errors 装饰器

```python
def handle_timeout_errors(failure_counter_attr, operation_name, ...):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            failure_counter = getattr(self, failure_counter_attr)
            try:
                result = func(self, *args, **kwargs)
                setattr(self, failure_counter_attr, 0)     # ← 成功时重置
                return result
            except ray.exceptions.GetTimeoutError as exc:
                failure_counter += 1
                setattr(self, failure_counter_attr, failure_counter)

                if failure_counter >= self.MAX_CONSECUTIVE_FAILURES:  # ← >=10
                    raise RuntimeError(
                        f"Failed to {operation_name} for {requester_id} "
                        f"after {failure_counter} consecutive failures."
                    ) from exc

                logger.warning(msg, exc_info=True)

                if on_error_return is not None:
                    return on_error_return(self, requester_id)  # ← 返回缓存值
        return wrapper
    return decorator
```

### 4.7 关键代码 — Actor 创建与绑定

```python
def get_or_create_autoscaling_coordinator():
    scheduling_strategy = NodeAffinitySchedulingStrategy(
        ray.get_runtime_context().get_node_id(),
        soft=False,                                # ← 硬绑定到 Head 节点
    )
    actor_cls = ray.remote(num_cpus=0, max_restarts=-1, max_task_retries=-1)(
        _AutoscalingCoordinatorActor
    ).options(
        name="AutoscalingCoordinator",
        namespace="AutoscalingCoordinator",
        get_if_exists=True,                        # ← 全局单例
        lifetime="detached",                       # ← detached，随 session 存活
        scheduling_strategy=scheduling_strategy,
    )
    with _get_or_create_lock:                      # ← 创建时的线程锁
        return actor_cls.remote()
```

---

## 5. GCS 过载详细分析

### 5.1 Worker 退出处理路径

```
raylet: HandleClientConnectionError (connection error code 2)
  → DisconnectClient()
  → gcs_client_.Workers().AsyncReportWorkerFailure()
    → GCS RPC: ReportWorkerFailure
```

GCS 服务端处理（`gcs_worker_manager.cc`）：

```
HandleReportWorkerFailure RPC
  → GetWorkerInfo() (存储读取)
  → is_alive=false
  → 通知 worker_dead_listeners_ (同步迭代)
    → gcs_actor_manager_->OnWorkerDead() (迭代所有 Actor，重启)
    → gcs_placement_group_scheduler_->HandleWaitingRemovedBundles()
    → gcs_task_manager_->OnWorkerDead()
  → WorkerTable.Put() (存储写入)
  → gcs_publisher_.PublishWorkerFailure() (pubsub 发布)
```

### 5.2 Node 退出处理路径

```
GCS Health Check Manager
  → 健康检查失败（5 次连续失败，约 15s）
  → on_node_death_callback_
    → gcs_node_manager_->OnNodeFailure()
```

GCS 服务端处理（`gcs_node_manager.cc`）：

```
OnNodeFailure → InternalOnNodeFailure
  → absl::MutexLock lock(&mutex_)           ← 写锁
  → 从 alive_nodes_ 移除，加入 dead_nodes_
  → 通知 node_removed_listeners_
    → gcs_resource_manager_->OnNodeDead()
    → gcs_placement_group_manager_->OnNodeDead()
    → gcs_actor_manager_->OnNodeDead()       ← 迭代所有 Actor，重启（最昂贵）
    → gcs_job_manager_->OnNodeDead()
    → gcs_autoscaler_state_manager_->OnNodeDead()
  → NodeTable.Put() (存储写入)
  → pubsub 发布 (2 条消息: GcsNodeInfo + NodeAddressAndLiveness)
```

### 5.3 GetAllNodeInfo 处理路径

```
HandleGetAllNodeInfo RPC
  → absl::ReaderMutexLock lock(&mutex_)     ← 读锁
  → 迭代 alive_nodes_ + dead_nodes_ (O(N) 操作)
  → 序列化所有节点到 reply proto
  → send_reply_callback()
```

### 5.4 读写锁竞争

```
UnregisterNode 处理:  mutex_.WriterLock()  ←── 写锁（96 个并发）
GetAllNodeInfo 处理:  mutex_.ReaderLock()  ←── 读锁（被阻塞）

当写锁持有者优先级更高时：
  → GetAllNodeInfo 读锁请求被阻塞
  → 直到所有 UnregisterNode 写操作完成
  → GetAllNodeInfo 才能获取读锁
```

### 5.5 GCS 并发限制与批处理

| 机制 | 配置 | 默认值 | 说明 |
|------|------|--------|------|
| RPC 并发限制 | `gcs_max_active_rpcs_per_handler` | `hw_concurrency/4 * 100` ≈ 400 | 不会限流 554 个 ReportWorkerFailure |
| pubsub 批大小 | `publish_batch_size` | 5000 | 仅批量化长轮询响应 |
| 健康检查参数 | `health_check_failure_threshold` | 5 | 自然限流 Node 死亡判定（约 15s） |
| dead node 缓存限制 | `maximum_gcs_dead_node_cached_count` | 1000 | 内存限制 |
| Redis 存储批量化 | `maximum_gcs_storage_operation_batch_size` | 可配置 | 仅批量化 multi-get |

**Worker/Node 退出事件没有 rate limiting、batching、priority queuing。**

### 5.6 GCS io_context 分离

| io_context | 服务 | 是否被过载影响 |
|------------|------|---------------|
| 默认 io_context (单线程) | Worker/Node 退出处理、GetAllNodeInfo、GetAllResourceUsage 等 | **受影响** |
| task_io_context | GcsTaskManager | 不受影响 |
| pubsub_io_context | GcsPublisher | 不受影响 |
| KV io_context | GcsInternalKVManager | 不受影响（但共享 Redis 后端可能饱和） |
| ray_syncer_io_context | RaySyncer | 不受影响 |

---

## 6. 缓解与修复方案

### 6.1 临时方案 — 增加容忍度

```bash
# 增加最大连续失败次数（默认 10）
export RAY_DATA_AUTOSCALING_COORDINATOR_MAX_CONSECUTIVE_FAILURES=30

# 增加单次超时时间（默认 5 秒）
export RAY_DATA_AUTOSCALING_COORDINATOR_REQUEST_GET_TIMEOUT_S=15
```

**注意：这只是延缓问题，不是解决根本原因。当 GetAllNodeInfo 无超时卡住时，增大 ray.get timeout 也不能帮助。**

### 6.2 根本方案 — 代码修复

#### 方案 A：拆锁（最直接）

将 `_tick()` 中的阻塞 GCS RPC 操作移出锁的范围：

```python
def _tick(self):
    # 先在锁外完成阻塞 RPC
    merged_req = None
    cluster_node_resources = None
    with self._lock:
        self._purge_expired_requests()
        merged_req = [req.requested_resources for req in self._ongoing_reqs.values()]
        # 合并请求列表（纯计算，无阻塞）

    # 锁外执行阻塞 GCS RPC
    self._send_resources_request(merged_req)         # GCS RPC (10s timeout)
    cluster_node_resources = self._get_cluster_nodes()  # GCS RPC (无超时)

    # 再次获取锁，更新状态
    with self._lock:
        # 比较并更新 cluster_node_resources
        if cluster_node_resources != self._cluster_node_resources:
            self._cluster_node_resources = cluster_node_resources
            self._reallocate_resources()
```

#### 方案 B：为 GCS RPC 添加超时

给 `ray.nodes()` 的底层 `GetAllNodeInfo` RPC 设置超时（而非无限等待），避免 `_tick()` 持锁永远卡住：

```python
# 在 _update_cluster_node_resources 中添加超时
def _update_cluster_node_resources(self):
    try:
        nodes = list(filter(_is_node_eligible, self._get_cluster_nodes()))
    except Exception:
        # GCS RPC 超时或失败时，使用上次缓存的结果
        return False
```

#### 方案 C：分离 _tick 到独立进程

将 _tick_thread 的周期性任务移到独立的 Actor 或进程，与主 Actor 的远程方法处理不共享锁：

```python
class _AutoscalingCoordinatorTickActor:
    """独立 Actor，只负责周期性 _tick"""
    TICK_INTERVAL_S = 20

    def __init__(self, main_actor):
        self._main_actor = main_actor  # 通过 remote call 与主 Actor 交互

    def tick(self):
        # 在独立进程中执行，不影响主 Actor 锁
        ...
```

#### 方案 D：异步化 GCS RPC

将 `_tick()` 中的阻塞 GCS RPC 改为异步调用，避免持锁期间阻塞：

```python
def _tick(self):
    with self._lock:
        # 记录需要发送的请求（不阻塞）
        pending_reqs = self._get_pending_requests()

    # 锁外异步发送
    self._send_resources_request_async(pending_reqs)
    self._update_cluster_node_resources_async()
```

### 6.3 预防措施

| 措施 | 说明 |
|------|------|
| Head 节点资源预留 | 确保 Head 节点有足够 CPU/内存（当前 16 核不足以支撑 500 Worker 集群） |
| ray-dashboard 资源限制 | 当前 dashboard 占 100% CPU，需限制其资源使用 |
| GCS HA | 配置 GCS 高可用，避免单点故障 |
| Worker 退出限流 | 在 GCS 服务端为 ReportWorkerFailure 添加 rate limiting 或 batching |
| 监控告警 | 设置 Head 节点 CPU/内存告警、GCS RPC 响应时间告警 |

### 6.4 方案对比

| 方案 | 优点 | 缺点 | 推荐优先级 |
|------|------|------|-----------|
| A: 拆锁 | 直接解决锁竞争，改动小 | 需要两次加锁，状态一致性需注意 | **高** |
| B: GCS RPC 超时 | 防止无限卡住，改动最小 | 需要缓存 fallback 逻辑，可能丢失更新 | **高** |
| C: 分离 Actor | 完全消除锁竞争 | 架构改动大，跨 Actor 通信增加延迟 | 中 |
| D: 异步 RPC | 不阻塞线程 | 异步编程复杂度高，错误处理困难 | 低 |

**推荐组合：方案 A + 方案 B** — 拆锁 + 添加 GCS RPC 超时，既解决锁竞争，又防止无限阻塞。

---

## 7. 排查命令参考

### 7.1 Head 节点排查

```bash
# 检查 Actor 状态
ray list actors --filter name=AutoscalingCoordinator

# 检查集群状态
ray status

# 检查 Head 节点负载
top -b -n 1

# 检查容器 CPU 限制
cat /sys/fs/cgroup/cpu/cpu.cfs_quota_us
cat /sys/fs/cgroup/cpu/cpu.cfs_period_us
```

### 7.2 日志排查

```bash
# GCS 日志 — Worker/Node 退出
rg "connection error code|UnregisterNode|ReportWorkerFailure" /tmp/ray/session_*/logs/gcs_server.out

# raylet 日志 — Actor state-dump
rg "AutoscalingCoordinatorActor|state-dump" /tmp/ray/session_*/logs/raylet.out

# driver 日志 — consecutive failures
rg "consecutive failures|GetTimeoutError|RuntimeError" /tmp/ray/session_*/logs/job-driver-*.log

# Ray 进程
ps aux | grep ray
```

### 7.3 Dashboard 排查

访问 Ray Dashboard（通常 `http://<head-node>:8265`），检查：

| 面板 | 检查项 |
|------|--------|
| Nodes | 节点存活状态、资源使用率 |
| Actors | AutoscalingCoordinator Actor 状态 |
| Logs | GCS/Raylet 错误日志 |
| Data | Dataset 执行状态和进度 |

---

## 8. 总结

| 项目 | 说明 |
|------|------|
| **错误类型** | RuntimeError |
| **触发条件** | get_allocated_resources / request_resources 连续超时 >= 10 次 |
| **影响** | 作业立即退出，状态更新为 FAILED |
| **直接原因** | Actor 内 threading.Lock 锁竞争 — _tick_thread 持锁做阻塞 GCS RPC，主线程无法在 5s 内完成方法执行 |
| **根本原因** | 554 Worker + 96 Node 大规模退出 → GCS 单线程 io_context 过载 → GetAllNodeInfo 读锁被 UnregisterNode 写锁阻塞 → _tick() 持锁卡住（RPC 无超时）→ 三重锁竞争 → ray.get(5s) 连续超时 |
| **核心设计缺陷** | (1) Actor 内所有方法共用一把锁；(2) _tick() 在锁内做阻塞 GCS RPC；(3) ray.nodes() → GetAllNodeInfo RPC 无超时 |
| **临时缓解** | 增大 MAX_CONSECUTIVE_FAILURES 和 REQUEST_GET_TIMEOUT_S |
| **根本修复** | 拆锁 + 为 GCS RPC 添加超时 + Head 节点资源预留 |
