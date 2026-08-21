# Ray PENDING_NODE_ASSIGNMENT 问题排查报告

## 问题描述

作业 48000000 中有3个 task 长期处于 `PENDING_NODE_ASSIGNMENT` 状态，无法调度执行。

## 环境

- Ray 版本: `2.54.0+kuaishou.a760958466`（未包含修复 commit `82d05363`）
- 集群 Head 节点: `10.137.44.181`
- 集群资源: 14190 CPU, 10.07 TiB memory
- 作业入口: `face_decup_ann_score.py`
- 作业 Driver PID: 3165752

## 现象

### 1. 3个 Pending Task 详情

| Task ID (前缀) | Name | Required Resources | 进入 Pending 时间 |
|---|---|---|---|
| `1958a016...` | StreamingRepartition[_map_task] | CPU: 1 | 2026-08-19 17:32:59 |
| `297598f9...` | StreamingRepartition[_map_task] | CPU: 1, memory: 10.85GiB | 2026-08-19 20:30:09 |
| `e6ffb9a6...` | StreamingRepartition[_map_task] | CPU: 1 | 2026-08-19 17:32:26 |

### 2. 关键特征

- `node_id: null` — 调度器尚未为这些 task 分配到任何节点
- 集群资源充足: CPU 使用率 0/14190，memory 使用率 0/10.07TiB
- Head 节点 raylet 持续出现 `process_failed_runtime_env_setup_failed: 3`
- 作业从 8/19 20:49 开始卡住，持续近18小时无进展

## 日志现场证据

### Driver Core Worker gRPC Event Stats（直接证据）

日志路径: `/tmp/ray/session_latest/logs/python-core-driver-48000000ffffffffffffffffffffffffffffffffffffffffffffffff_3165752.log`

查询命令:
```bash
grep "RequestWorkerLease - " /tmp/ray/session_latest/logs/python-core-driver-48000000ffffffffffffffffffffffffffffffffffffffffffffffff_3165752.log | grep -v OnReply | tail -3
grep "RequestWorkerLease.OnReplyReceived" /tmp/ray/session_latest/logs/python-core-driver-48000000ffffffffffffffffffffffffffffffffffffffffffffffff_3165752.log | tail -3
```

日志内容:
```
# 发出的 lease 请求 — total=80966, active=3（3个请求卡住无回复）
NodeManagerService.grpc_client.RequestWorkerLease - 80966 total (3 active), Execution time: mean = 255.84ms, total = 20714450.63ms, Queueing time: mean = 0.00ms

# 收到的 lease 回复 — total=80963（差3个，从未收到回复）
NodeManagerService.grpc_client.RequestWorkerLease.OnReplyReceived - 80963 total (0 active), Execution time: mean = 0.36ms, total = 29540.63ms
```

**80966 - 80963 = 3**，3个 RequestWorkerLease 请求发出后 callback 永远没有触发。

### 时间线

| 时间 | RequestWorkerLease total | active | OnReplyReceived total | 差值 |
|------|--------------------------|--------|----------------------|------|
| 20:45:51 | 80930 | 3 | 80927 | 3 |
| 20:48:51 | 80964 | 3 | 80961 | 3 |
| **20:49:51** | **80966** | **3** | **80963** | **3** ← 从此冻结 |
| 20:50:51 ~ 14:22 (次日) | 80966 | 3 | 80963 | 3 |

从 `2026-08-19 20:49:51` 开始，3个 RequestWorkerLease RPC 发出后从未收到回复，持续近18小时。

### Head 节点 Raylet 日志

路径: `/tmp/ray/session_latest/logs/raylet.out`

- 无 job 48000000 的 lease 记录
- 无 redirect/spillback 记录
- 仅有 `process_failed_runtime_env_setup_failed: 3`（属于其他 job 03000000）

### GCS Server 日志

路径: `/tmp/ray/session_latest/logs/gcs_server.out`

- 无这3个 task 的调度记录
- 仅记录了 runtime env 下载（working_dir `_ray_pkg_940b36bbcb571117.zip`）

### 为什么其他日志没有记录

spillback 决策是 driver 自己做的（基于数据本地性的 LocalityAwareLeasePolicy），没有经过本地 raylet，所以 head raylet 日志无记录。远端目标节点已死，请求卡在 gRPC 层，还没到达远端 raylet，远端也不可能有记录。

## 根因分析

### 缺少修复 Commit: `82d0536332`

分支: `T11613716-rpc-timeout`

```
[T11613716] add timeout for RequestWorkerLease RPC to prevent permanent scheduling deadlock

When a node dies after being selected as a spillback target, the lease
request to that dead node enters an infinite gRPC retry loop because
method_timeout_ms is set to -1 (infinite). The RetryableGrpcClient
"swallows" the UNAVAILABLE error and never calls back to
NormalTaskSubmitter, causing the task to be permanently stuck.
```

### 完整死锁链路

```
1. Driver 的 LocalityAwareLeasePolicy 根据数据本地性直接选了远端节点
   → Driver 直接向远端节点发 RequestWorkerLease (method_timeout_ms=-1)
   → 不经过本地 raylet（所以 head raylet 日志无记录）

2. RetryableGrpcClient 把请求排入 pending_requests_
   → method_timeout_ms=-1 → timeout=absl::InfiniteFuture()
   → pending_requests_.emplace(InfiniteFuture, request)

3. 远端节点死亡，gRPC channel 进入 TRANSIENT_FAILURE ↔ CONNECTING 循环
   → gRPC 尝试连接 → 收到 UNAVAILABLE

4. RetryableGrpcClient 捕获 UNAVAILABLE，不调用上层 callback
   → 而是调用 Retry() 重新入队（IsGrpcRetryableStatus(UNAVAILABLE)==true）
   → 上层 NormalTaskSubmitter 的 callback 永远不触发

5. CheckChannelStatus() 定时器每秒检查:
   ├─ TRANSIENT_FAILURE: 只是等，调 server_unavailable_timeout_callback
   │   → 查 GCS 节点状态 → 可能因延迟查到 ALIVE，不做任何事
   │   → 即使查到 DEAD，Disconnect() 只从 pool 移除引用
   │   → 不 fail 掉 pending 的 RPC 请求
   ├─ READY/IDLE: 重发所有 pending_requests_ → 又 UNAVAILABLE → 又 Retry()
   └─ 超时扫描: timeout=InfiniteFuture 永远 > now，永远跳过

6. NormalTaskSubmitter 的 callback 永远不触发
   → 无法回退到本地调度
   → task 永久卡在 PENDING_NODE_ASSIGNMENT
```

### method_timeout_ms 的两层含义

#### 第一层: gRPC 级别的 deadline

```cpp
// client_call.h:82-84
if (timeout_ms != -1) {
    auto deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(timeout_ms);
    context_.set_deadline(deadline);  // 设在 gRPC ClientContext 上
}
```

- `-1` 时不设 deadline → gRPC 请求永不超时
- 对端死了 gRPC 不会主动返回 DEADLINE_EXCEEDED

#### 第二层: RetryableGrpcClient 级别的队列超时

```cpp
// retryable_grpc_client.cc:194-196
const auto timeout = request->GetTimeoutMs() == -1
                         ? absl::InfiniteFuture()      // ← -1 时永不超时
                         : now + absl::Milliseconds(request->GetTimeoutMs());
pending_requests_.emplace(timeout, std::move(request));  // 按超时时间排序
```

超时扫描逻辑:

```cpp
// retryable_grpc_client.cc:53-61
while (!pending_requests_.empty()) {
    auto iter = pending_requests_.begin();
    if (iter->first > now) {       // ← -1 时 iter->first = InfiniteFuture
        break;                      // ← 永远 > now，永远 break
    }
    // 超时 Fail() 逻辑，永远走不到
}
```

**`method_timeout_ms` 的含义: 这个 RPC 请求从发出开始，允许在 RetryableGrpcClient 的重试队列里待多长时间。超过这个时间还没成功，就 Fail 掉并回调上层。**

- `-1`: 允许在重试队列里无限等待（gRPC 也不设 deadline）
- `600000`: 最多等10分钟，超时后 `Fail(TimedOut)`，callback 触发，上层可以重新调度

### 为什么只有 RequestWorkerLease 会卡死

其他 retryable RPC 不会卡死，不是因为 RPC 本身不同，而是因为调用方语义不同:

| RPC | 发送方向 | 卡死了有影响吗 |
|-----|---------|---------------|
| `ReturnWorkerLease` | worker → raylet | 无影响，只是清理 |
| `CancelWorkerLease` | worker → raylet | 无影响，取消操作是 best-effort |
| `PinObjectIDs` | worker → raylet | 无影响，引用计数不影响调度 |
| `ReleaseUnusedBundles` | autoscaler → raylet | 无影响，清理操作 |
| `ShutdownRaylet` / `DrainRaylet` | GCS → raylet | 节点死了不需要 |
| **`RequestWorkerLease`** | **core worker → raylet** | **卡死 = task 永久 PENDING！** |

**核心区别: 只有 RequestWorkerLease 的 callback 驱动调度循环**

RequestWorkerLease 的 callback 做了什么 (normal_task_submitter.cc:337-470):
- `status.ok() && reply.canceled()` → 处理取消，可能 fail task
- `reply.rejected()` → 重试本地调度 `RequestNewWorkerIfNeeded()`
- `worker granted` → 分配 task 给 worker
- `redirect` → 转发到新 raylet
- `!status.ok() && remote` → 回退本地调度 `RequestNewWorkerIfNeeded()`

这个 callback 是整个调度循环的"引擎"。其他 retryable RPC 的 callback 都不驱动关键循环——它们是"一次性"操作，卡住只是延迟清理动作。

另外，`RequestWorkerLease` 是唯一一个会由 NormalTaskSubmitter 主动发向远端 raylet 的 RPC。其他 retryable RPC 的目标都是本节点或已知存活的 raylet。

### gRPC 节点死亡后的行为

**gRPC client channel 永远不会因为对端死亡而进入 SHUTDOWN 状态。** 只有显式调用 `channel->Shutdown()` 才会进入。节点死亡后，channel 在 `TRANSIENT_FAILURE ↔ CONNECTING` 之间无限循环。

### server_unavailable_timeout_callback 的回收机制及其局限

```cpp
// raylet_client_pool.cc:24-65
// 触发后:
1. 检查 gcs_client->Nodes() 缓存
2. 缓存显示 DEAD → Disconnect() ✅
3. 缓存显示 ALIVE → 不做任何事 ❌ （GCS 延迟可能查到过时状态）
4. 缓存没有 → 异步查 GCS
   → DEAD → Disconnect() ✅
   → ALIVE → 不做任何事 ❌
```

**问题1: GCS 节点状态有延迟**，可能查到 ALIVE

**问题2: `Disconnect()` 只从 pool 中移除引用，不会 fail 掉 pending 的 RPC**

```cpp
void RayletClientPool::Disconnect(ray::NodeID id) {
    client_map_.erase(it);  // 只是移除 shared_ptr
}
// 如果有 pending RPC 正在持有 RayletClient 的 shared_ptr，
// RetryableGrpcClient 继续运行，pending_requests_ 里的请求继续等待
```

## Spillback vs Locality-Aware Scheduling

### Spillback（真正的 spillback）

1. Driver 向本地 raylet 发 RequestWorkerLease（`grant_or_reject=false`）
2. 本地 raylet 资源不足，返回 `retry_at_raylet_address`（redirect）
3. Driver 向远端 raylet 发 RequestWorkerLease（`grant_or_reject=true`, `is_spillback=true`）
4. 远端 raylet 必须 grant 或 reject，不能再 redirect

### Locality-Aware Scheduling（本案例）

1. Driver 的 `LocalityAwareLeasePolicy.GetBestNodeForLease()` 根据数据本地性直接选远端节点
2. Driver 直接向远端节点发 RequestWorkerLease（`grant_or_reject=false`, `is_spillback=false`）
3. 远端 raylet 还可以再 redirect 到其他节点

```cpp
// normal_task_submitter.cc:313-326
const bool is_spillback = (raylet_address != nullptr);  // 在 if 之前计算
// ...
if (raylet_address == nullptr) {
    // LeasePolicy 直接选了远端节点
    std::tie(best_node_address, is_selected_based_on_locatity) =
        lease_policy_->GetBestNodeForLease(lease_spec);
    raylet_address = &best_node_address;
}
// is_spillback 仍然是 false（因为原始 raylet_address == nullptr）
```

**Locality-Aware 直接选远端节点不算 spillback，但两种路径的死锁机制完全相同** — 都是 `method_timeout_ms=-1`，RetryableGrpcClient 吞掉 UNAVAILABLE，callback 永远不触发。

## 修复 Commit 内容

### Commit: `82d05363327ebea8a9839e38ea7c03da29899de9`

### 修复1: 给 RequestWorkerLease 加超时

```cpp
// raylet_client.cc — 修复前
INVOKE_RETRYABLE_RPC_CALL(..., /*method_timeout_ms*/ -1);  // 永不超时

// raylet_client.cc — 修复后
INVOKE_RETRYABLE_RPC_CALL(...,
    grant_or_reject
        ? RayConfig::instance().worker_lease_timeout_ms()  // 600000ms (10分钟)
        : -1);
```

超时后 callback 触发，`NormalTaskSubmitter` 收到 `status.IsTimedOut()`，回退到本地调度。

### 修复2: grpc_max_ready_idle_resend_count

```cpp
// retryable_grpc_client.cc — 新增
// READY/IDLE 但连续10次 resend 都 UNAVAILABLE → Fail 所有 pending requests
// 处理 "IP 被复用" 场景：TCP 连得通（READY），但 gRPC 请求失败
const auto max_resend_count = RayConfig::instance().grpc_max_ready_idle_resend_count();
if (max_resend_count > 0 &&
    consecutive_ready_idle_resend_count_ >= max_resend_count) {
    // Fail all pending requests
}
```

### 修复3: 增加日志

修复前 RetryableGrpcClient 在重试路径上几乎没有日志（只有 DEBUG 级别），无法排查。修复后新增:

```cpp
+ RAY_LOG(WARNING) << server_name_ << " pending request timed out in retry queue "
+ RAY_LOG(INFO) << server_name_ << " request entered retry queue, starting check timer"
+ RAY_LOG(DEBUG) << server_name_ << " channel is READY/IDLE, resending N pending requests"
```

## 代码关键文件

| 文件 | 路径 | 关键内容 |
|------|------|----------|
| WorkerPool | `src/ray/raylet/worker_pool.cc` | StartNewWorker callback, RuntimeEnvCreationFailed |
| LocalLeaseManager | `src/ray/raylet/scheduling/local_lease_manager.cc` | PoppedWorkerHandler, CancelLeases on RuntimeEnvCreationFailed |
| NormalTaskSubmitter | `src/ray/core_worker/task_submission/normal_task_submitter.cc` | RequestNewWorkerIfNeeded, spillback callback |
| TaskManager | `src/ray/core_worker/task_manager.cc` | FailPendingTask |
| RetryableGrpcClient | `src/ray/rpc/retryable_grpc_client.cc` | CheckChannelStatus, Retry, timeout 逻辑 |
| RetryableGrpcClient Header | `src/ray/rpc/retryable_grpc_client.h` | INVOKE_RETRYABLE_RPC_CALL 宏, RetryableGrpcRequest |
| RayletClient | `src/ray/raylet_rpc_client/raylet_client.cc` | RequestWorkerLease, method_timeout_ms |
| ClientCall | `src/ray/rpc/client_call.h` | gRPC deadline 设置 |
| RayletClientPool | `src/ray/raylet_rpc_client/raylet_client_pool.cc` | GetDefaultUnavailableTimeoutCallback, Disconnect |
| LeasePolicy | `src/ray/core_worker/lease_policy.cc` | LocalityAwareLeasePolicy, GetBestNodeForLease |

## 结论

根因是运行集群的 Ray 版本（`2.54.0+kuaishou.a760958466`）未包含修复 commit `82d05363`。当 LocalityAwareLeasePolicy 选中了一个已死亡的远端节点作为 RequestWorkerLease 目标时，由于 `method_timeout_ms=-1`，RetryableGrpcClient 将 UNAVAILABLE 错误吞掉并无限重试，callback 永远不触发，task 永久卡在 PENDING_NODE_ASSIGNMENT。

修复方案: 将 `82d05363`（分支 `T11613716-rpc-timeout`）合入运行集群的 Ray 版本并重新部署。
