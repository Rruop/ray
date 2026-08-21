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

调度决策是 driver 自己做的（基于数据本地性的 LocalityAwareLeasePolicy），没有经过本地 raylet，所以 head raylet 日志无记录。远端目标节点已死，请求卡在 gRPC 层，还没到达远端 raylet，远端也不可能有记录。

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

### RequestWorkerLease 请求的完整代码路径

#### Step 1: Task 提交 — NormalTaskSubmitter.RequestNewWorkerIfNeeded()

```cpp
// normal_task_submitter.cc:313-335
const bool is_spillback = (raylet_address != nullptr);
bool is_selected_based_on_locality = false;
if (raylet_address == nullptr) {
    // LeasePolicy 选择目标节点
    std::tie(best_node_address, is_selected_based_on_locality) =
        lease_policy_->GetBestNodeForLease(lease_spec);
    raylet_address = &best_node_address;
}

auto raylet_client = raylet_client_pool_->GetOrConnectByAddress(*raylet_address);

raylet_client->RequestWorkerLease(
    lease_spec.GetMessage(),
    /*grant_or_reject=*/is_spillback,
    [this, scheduling_key, lease_id, ...](const Status &status,
                                          const rpc::RequestWorkerLeaseReply &reply) {
        // 这个 callback 是调度循环的"引擎"
        // 处理 grant/reject/redirect/failure
        // ...
    },
    ...,
    /*method_timeout_ms*/ -1);  // ← 永不超时！
```

#### Step 2: INVOKE_RETRYABLE_RPC_CALL 宏展开

```cpp
// retryable_grpc_client.h:37-50
#define INVOKE_RETRYABLE_RPC_CALL(retryable_rpc_client, SERVICE, METHOD, \
                                  request, callback, rpc_client, method_timeout_ms) \
  (retryable_rpc_client->CallMethod<SERVICE, METHOD##Request, METHOD##Reply>( \
      &SERVICE::Stub::PrepareAsync##METHOD, \
      rpc_client, \
      #SERVICE ".grpc_client." #METHOD, \
      std::move(request), \
      callback, \
      method_timeout_ms))
```

#### Step 3: CallMethod 创建 RetryableGrpcRequest

```cpp
// retryable_grpc_client.h:247-255
template <typename Service, typename Request, typename Reply>
void RetryableGrpcClient::CallMethod(
    PrepareAsyncFunction<Service, Request, Reply> prepare_async_function,
    std::shared_ptr<GrpcClient<Service>> grpc_client,
    std::string call_name,
    Request request,
    ClientCallback<Reply> callback,
    int64_t timeout_ms) {
  num_active_requests_++;
  RetryableGrpcRequest::Create(weak_from_this(), ..., callback, timeout_ms)
      ->CallMethod();  // 立即发送
}
```

#### Step 4: RetryableGrpcRequest 内部的 executor — 首次发送和重试逻辑

```cpp
// retryable_grpc_client.h:260-297
auto executor = [weak_retryable_grpc_client, prepare_async_function,
                 grpc_client, call_name, request, callback](
                    std::shared_ptr<RetryableGrpcRequest> retryable_grpc_request) {
  // 通过底层 GrpcClient 发送 gRPC 请求
  grpc_client->template CallMethod<Request, Reply>(
      prepare_async_function,
      request,
      // gRPC 回调
      [weak_retryable_grpc_client, retryable_grpc_request, callback](
          const ray::Status &status, Reply &&reply) {
        auto retryable_grpc_client = weak_retryable_grpc_client.lock();
        // 关键判断：成功 或 不可重试的错误 → 调用上层 callback
        if (status.ok() || !IsGrpcRetryableStatus(status) || !retryable_grpc_client) {
          callback(status, std::move(reply));
          // ...
          return;
        }
        // 可重试错误（UNAVAILABLE）→ 不调 callback，重新入队！
        retryable_grpc_client->Retry(retryable_grpc_request);
      },
      call_name,
      retryable_grpc_request->GetTimeoutMs());  // timeout_ms 传给底层 gRPC
};
```

#### Step 5: gRPC 底层 — deadline 设置

```cpp
// client_call.h:76-84
ClientCallImpl(const ClientCallback<Reply> &callback,
               const ClusterID &cluster_id,
               std::shared_ptr<StatsHandle> stats_handle,
               bool record_stats,
               int64_t timeout_ms = -1)
    : callback_(...), ... {
  if (timeout_ms != -1) {
    auto deadline =
        std::chrono::system_clock::now() + std::chrono::milliseconds(timeout_ms);
    context_.set_deadline(deadline);  // 设在 gRPC ClientContext 上
  }
  // -1 时不设 deadline → gRPC 请求永不超时
}
```

#### Step 6: Retry() — 请求重新入队

```cpp
// retryable_grpc_client.cc:170-200
void RetryableGrpcClient::Retry(std::shared_ptr<RetryableGrpcRequest> request) {
  const auto now = absl::Now();
  const auto request_bytes = request->GetRequestBytes();
  // ...

  // 关键：计算超时时间
  const auto timeout = request->GetTimeoutMs() == -1
                           ? absl::InfiniteFuture()      // ← -1 时永不超时
                           : now + absl::Milliseconds(request->GetTimeoutMs());
  pending_requests_.emplace(timeout, std::move(request));

  if (!server_unavailable_timeout_time_.has_value()) {
    server_unavailable_timeout_time_ =
        now + absl::Seconds(server_reconnect_timeout_base_seconds_);
    SetupCheckTimer();  // 启动定时器
  }
}
```

#### Step 7: CheckChannelStatus() — 定时检查（每秒）

```cpp
// retryable_grpc_client.cc — 修复前版本
void RetryableGrpcClient::CheckChannelStatus(bool reset_timer) {
  // 超时扫描
  const auto now = absl::Now();
  while (!pending_requests_.empty()) {
    auto iter = pending_requests_.begin();
    if (iter->first > now) {
      break;                  // ← timeout=InfiniteFuture 时，永远 > now，永远 break
    }
    // 超时 Fail — 永远走不到
    iter->second->Fail(ray::Status::TimedOut(...));
    pending_requests_.erase(iter);
  }

  if (pending_requests_.empty()) {
    server_unavailable_timeout_time_ = std::nullopt;
    return;
  }

  auto status = channel_->GetState(false);

  switch (status) {
  case GRPC_CHANNEL_TRANSIENT_FAILURE:
  case GRPC_CHANNEL_CONNECTING: {
    // 只是等，触发 server_unavailable_timeout_callback
    if (server_unavailable_timeout_time_ < now) {
      RAY_LOG(WARNING) << server_name_ << " has been unavailable for more than ...";
      server_unavailable_timeout_callback_();
      attempt_number_++;
      // 重新设更长的超时
    }
    SetupCheckTimer();  // 继续等
    break;
  }
  case GRPC_CHANNEL_SHUTDOWN: {
    RAY_LOG(FATAL) << "Channel should never go to this status.";
    break;
  }
  case GRPC_CHANNEL_READY:
  case GRPC_CHANNEL_IDLE: {
    server_unavailable_timeout_time_ = std::nullopt;
    // 重发所有 pending_requests_ — 但如果远端是死节点，又会 UNAVAILABLE
    while (!pending_requests_.empty()) {
      pending_requests_.begin()->second->CallMethod();  // 静默重发，无日志
      pending_requests_.erase(pending_requests_.begin());
    }
    break;
  }
  default:
    break;
  }
}
```

#### Step 8: gRPC 回调处理 — "吞掉" UNAVAILABLE

当 gRPC 请求因远端节点死亡返回 `UNAVAILABLE` 时：

```cpp
// retryable_grpc_client.h:285-295 — executor 内的回调
[weak_retryable_grpc_client, retryable_grpc_request, callback](
    const ray::Status &status, Reply &&reply) {
  auto retryable_grpc_client = weak_retryable_grpc_client.lock();
  // status.ok() = false, IsGrpcRetryableStatus(UNAVAILABLE) = true
  if (status.ok() || !IsGrpcRetryableStatus(status) || !retryable_grpc_client) {
    callback(status, std::move(reply));  // ← 不会走到这里
    return;
  }
  // 走到这里：不调 callback，重新入队！
  retryable_grpc_client->Retry(retryable_grpc_request);
}
```

**这就是死锁的核心：UNAVAILABLE 被 RetryableGrpcClient 吞掉，上层 callback 永远不被调用。**

#### Step 9: NormalTaskSubmitter 的 callback 处理（永远走不到）

如果 callback 能触发，其逻辑如下：

```cpp
// normal_task_submitter.cc:337-470
if (status.ok()) {
    if (reply.canceled()) {
        // 处理取消（RuntimeEnvCreationFailed 等）
    } else if (reply.rejected()) {
        RAY_CHECK(is_spillback);
        RequestNewWorkerIfNeeded(scheduling_key);  // 重试本地
    } else if (!reply.worker_address().node_id().empty()) {
        // Lease granted，分配 worker
    } else {
        // Redirect 到其他 raylet
        RAY_CHECK(!is_spillback);
        RequestNewWorkerIfNeeded(scheduling_key, &reply.retry_at_raylet_address());
    }
} else if (NodeID::FromBinary(raylet_address.node_id()) != local_node_id_) {
    // 远端 raylet 失败 → 回退本地调度
    RAY_LOG_EVERY_MS(INFO, 30 * 1000) << "Retrying attempt to schedule lease at remote node";
    RequestNewWorkerIfNeeded(scheduling_key);  // 重试！
} else {
    // 本地 raylet 失败 → 进程退出
    QuickExit();
}
```

**如果 callback 能触发（如加了 timeout），会走到 `!status.ok() && remote` 分支，回退本地调度。**

### method_timeout_ms 的两层含义

#### 第一层: gRPC 级别的 deadline

```cpp
// client_call.h:82-84
if (timeout_ms != -1) {
    auto deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(timeout_ms);
    context_.set_deadline(deadline);  // 设在 gRPC ClientContext 上
}
```

这是 gRPC 原生的请求超时：如果服务端在 deadline 之前没有返回回复，gRPC 返回 `DEADLINE_EXCEEDED`。
- `-1` 时不设 deadline → gRPC 请求永不超时 → 对端死了 gRPC 也不会主动返回 DEADLINE_EXCEEDED

#### 第二层: RetryableGrpcClient 级别的队列超时

```cpp
// retryable_grpc_client.cc:194-196
const auto timeout = request->GetTimeoutMs() == -1
                         ? absl::InfiniteFuture()      // ← -1 时永不超时
                         : now + absl::Milliseconds(request->GetTimeoutMs());
pending_requests_.emplace(timeout, std::move(request));  // 按超时时间排序
```

请求被 `Retry()` 重新入队后，`CheckChannelStatus()` 定时扫描时检查：

```cpp
// retryable_grpc_client.cc:53-61
while (!pending_requests_.empty()) {
    auto iter = pending_requests_.begin();
    if (iter->first > now) {    // iter->first 就是上面存的 timeout
        break;                  // 没超时，跳过
    }
    iter->second->Fail(Status::TimedOut(...));  // 超时了，fail 掉
}
```

**`-1` → `InfiniteFuture` → 永远 `> now` → 永远不会被超时 fail 掉。**

#### 两层的关系

```
请求发出（CallMethod）
  │
  ├─ 网络通了 → 服务端收到请求
  │    └─ 服务端处理中...
  │         ├─ 在 deadline 内返回 → callback(status.ok(), reply) ✅
  │         └─ 超过 deadline → gRPC 返回 DEADLINE_EXCEEDED → callback(RpcError)
  │
  └─ 网络不通 → gRPC 返回 UNAVAILABLE
       └─ IsGrpcRetryableStatus(UNAVAILABLE) == true
            └─ 不调 callback，而是 Retry() 重新入队
                 └─ 在队列里等待 channel 恢复
                      ├─ method_timeout_ms != -1:
                      │    在 CheckChannelStatus 扫描时超时 → Fail(TimedOut)
                      │    → callback(status=TimedOut) 被调用 ✅
                      │
                      └─ method_timeout_ms == -1:
                           timeout = InfiniteFuture
                           永远不会被超时扫描命中
                           callback 永远不会被调用 ❌
```

**`method_timeout_ms` 的含义：这个 RPC 请求从发出开始，允许在 RetryableGrpcClient 的重试队列里待多长时间。超过这个时间还没成功，就 Fail 掉并回调上层。**

- `-1`: 允许在重试队列里无限等待（gRPC 也不设 deadline）
- `600000`: 最多等10分钟，超时后 `Fail(TimedOut)`，callback 触发，上层可以重新调度

### 为什么只有 RequestWorkerLease 会卡死

其他 retryable RPC 不会卡死，不是因为 RPC 本身不同，而是因为调用方语义不同：

| RPC | 发送方向 | 卡死了有影响吗 |
|-----|---------|---------------|
| `ReturnWorkerLease` | worker → raylet | 无影响，只是清理 |
| `CancelWorkerLease` | worker → raylet | 无影响，取消操作是 best-effort |
| `PinObjectIDs` | worker → raylet | 无影响，引用计数不影响调度 |
| `ReleaseUnusedBundles` | autoscaler → raylet | 无影响，清理操作 |
| `ShutdownRaylet` / `DrainRaylet` | GCS → raylet | 节点死了不需要 |
| **`RequestWorkerLease`** | **core worker → raylet** | **卡死 = task 永久 PENDING！** |

**核心区别：只有 RequestWorkerLease 的 callback 驱动调度循环**

RequestWorkerLease 的 callback 做了什么 (normal_task_submitter.cc:337-470)：
- `status.ok() && reply.canceled()` → 处理取消，可能 fail task
- `reply.rejected()` → 重试本地调度 `RequestNewWorkerIfNeeded()`
- `worker granted` → 分配 task 给 worker
- `redirect` → 转发到新 raylet
- `!status.ok() && remote` → 回退本地调度 `RequestNewWorkerIfNeeded()`

这个 callback 是整个调度循环的"引擎"。其他 retryable RPC 的 callback 都不驱动关键循环——它们是"一次性"操作，卡住只是延迟清理动作。

另外，`RequestWorkerLease` 是唯一一个会由 NormalTaskSubmitter 主动发向远端 raylet 的 RPC。其他 retryable RPC 的目标都是本节点或已知存活的 raylet。

### gRPC 节点死亡后的行为

**gRPC client channel 永远不会因为对端死亡而进入 SHUTDOWN 状态。** 只有显式调用 `channel->Shutdown()` 才会进入。节点死亡后，channel 在 `TRANSIENT_FAILURE ↔ CONNECTING` 之间无限循环：

```
gRPC Channel 状态转换规则：
- SHUTDOWN：只有显式 channel->Shutdown() 才会进入，对端死亡不会触发
- TRANSIENT_FAILURE：连接失败，gRPC 认为是瞬态的，会自动重连
- CONNECTING：正在尝试连接
- READY：连接成功
- IDLE：空闲（无活动）

节点死亡后：TRANSIENT_FAILURE ↔ CONNECTING 无限循环
```

所以对端死了 gRPC 确实会"失败"（返回 UNAVAILABLE），但 `RetryableGrpcClient` 把这个失败吞掉了——不传给上层 callback，而是自动 Retry()。

### server_unavailable_timeout_callback 的回收机制及其局限

```cpp
// raylet_client_pool.cc:24-65 — GetDefaultUnavailableTimeoutCallback
return [addr, gcs_client, raylet_client_pool]() {
    const NodeID node_id = NodeID::FromBinary(addr.node_id());

    // 1. 检查 GCS 缓存
    if (gcs_client->Nodes().IsSubscribedToNodeChange()) {
        auto node_info = gcs_client->Nodes().GetNodeAddressAndLiveness(node_id, false);
        if (!node_info) {
            // 缓存没有，异步查 GCS
            gcs_check_node_alive();
            return;
        }
        if (node_info->state() == rpc::GcsNodeInfo::DEAD) {
            // 查到 DEAD → Disconnect()
            raylet_client_pool->Disconnect(node_id);
            return;
        }
        // 查到 ALIVE → 不做任何事 ❌
        return;
    }
    // 异步查 GCS
    gcs_check_node_alive();
};
```

**问题1: GCS 节点状态有延迟**，从节点死亡到 GCS 标记为 DEAD 有时间差，callback 可能查到 ALIVE，不做任何事。

**问题2: `Disconnect()` 只从 pool 中移除引用，不会 fail 掉 pending 的 RPC**

```cpp
// raylet_client_pool.cc
void RayletClientPool::Disconnect(ray::NodeID id) {
    absl::MutexLock lock(&mu_);
    auto it = client_map_.find(id);
    if (it == client_map_.end()) return;
    client_map_.erase(it);  // 只是移除 shared_ptr
}
// 如果 NormalTaskSubmitter 的 callback 闭包持有 RayletClient 的 shared_ptr，
// RayletClient 对象不会被销毁，RetryableGrpcClient 继续运行！
// pending_requests_ 里的请求继续等待，但 method_timeout_ms=-1 永不超时
```

## Spillback vs Locality-Aware Scheduling

### Spillback（真正的 spillback）

1. Driver 向本地 raylet 发 RequestWorkerLease（`grant_or_reject=false`）
2. 本地 raylet 资源不足，返回 `retry_at_raylet_address`（redirect）
3. Driver 收到 redirect 后，调用 `RequestNewWorkerIfNeeded(scheduling_key, &reply.retry_at_raylet_address())`
4. 此时 `raylet_address != nullptr` → `is_spillback = true`
5. Driver 向远端 raylet 发 RequestWorkerLease（`grant_or_reject=true`）
6. 远端 raylet 必须 grant 或 reject，不能再 redirect

```cpp
// normal_task_submitter.cc:430-435 — 处理 redirect
} else {
    // The raylet redirected us to a different raylet to retry at.
    RAY_CHECK(!is_spillback);
    RAY_LOG(DEBUG) << "Redirect lease " << lease_id << " from raylet "
                   << NodeID::FromBinary(raylet_address.node_id())
                   << " to raylet "
                   << NodeID::FromBinary(reply.retry_at_raylet_address().node_id());
    RequestNewWorkerIfNeeded(scheduling_key, &reply.retry_at_raylet_address());
}
```

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
    raylet_address = &best_node_address;  // raylet_address 被赋值
}
// 但 is_spillback 仍然是 false（因为计算时原始 raylet_address == nullptr）
```

LocalityAwareLeasePolicy 的选择逻辑：

```cpp
// lease_policy.cc:24-52
std::pair<rpc::Address, bool> LocalityAwareLeasePolicy::GetBestNodeForLease(
    const LeaseSpecification &spec) {
  // Spread 策略优先
  if (spec.GetMessage().scheduling_strategy().scheduling_strategy_case() ==
      rpc::SchedulingStrategy::kSpreadSchedulingStrategy) {
    return std::make_pair(fallback_rpc_address_, false);  // 本地节点
  }
  // Node Affinity 优先
  if (auto node_id_values = GetHardNodeAffinityValues(spec.GetLabelSelector())) {
    // ...
  }
  // 数据本地性选择
  if (auto node_id = GetBestNodeIdForLease(spec)) {
    if (auto addr = node_addr_factory_(node_id.value())) {
      return std::make_pair(addr.value(), true);  // ← 返回远端节点！
    }
  }
  return std::make_pair(fallback_rpc_address_, false);  // fallback 到本地
}

// GetBestNodeIdForLease：选数据对象本地字节数最多的节点
std::optional<NodeID> LocalityAwareLeasePolicy::GetBestNodeIdForLease(
    const LeaseSpecification &spec) {
  const auto object_ids = spec.GetDependencyIds();
  absl::flat_hash_map<NodeID, uint64_t> bytes_local_table;
  uint64_t max_bytes = 0;
  std::optional<NodeID> max_bytes_node;
  for (const ObjectID &object_id : object_ids) {
    if (auto locality_data = locality_data_provider_.GetLocalityData(object_id)) {
      for (const NodeID &node_id : locality_data->nodes_containing_object) {
        auto &bytes = bytes_local_table[node_id];
        bytes += locality_data->object_size;
        if (bytes > max_bytes) {
          max_bytes = bytes;
          max_bytes_node = node_id;  // 选中本地性最好的节点
        }
      }
    }
  }
  return max_bytes_node;
}
```

**Locality-Aware 直接选远端节点不算 spillback，但两种路径的死锁机制完全相同** — 都是 `method_timeout_ms=-1`，RetryableGrpcClient 吞掉 UNAVAILABLE，callback 永远不触发。

| 路径 | is_spillback | grant_or_reject | 远端 raylet 行为 |
|------|-------------|-----------------|-----------------|
| LeasePolicy 选远端节点 | false | false | 可以再 redirect |
| 本地 raylet redirect | true | true | 必须 grant 或 reject |

区别在于远端 raylet 收到请求后的行为，但**远端节点死了两种路径都会卡死**。

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

注意：修复只在 `grant_or_reject=true`（真正的 spillback）时加超时，`grant_or_reject=false`（locality-aware）时仍然是 -1。这意味着 locality-aware 路径的死锁可能仍未完全修复。

超时后 callback 触发，`NormalTaskSubmitter` 收到 `status.IsTimedOut()`，走到 `!status.ok() && remote` 分支，回退到本地调度。

### 修复2: grpc_max_ready_idle_resend_count

```cpp
// retryable_grpc_client.cc — 修复后新增
case GRPC_CHANNEL_READY:
case GRPC_CHANNEL_IDLE: {
    server_unavailable_timeout_time_ = std::nullopt;

    const auto max_resend_count =
        RayConfig::instance().grpc_max_ready_idle_resend_count();
    if (max_resend_count > 0 &&
        consecutive_ready_idle_resend_count_ >= max_resend_count) {
      RAY_LOG(WARNING) << server_name_ << " channel is "
                       << (status == GRPC_CHANNEL_READY ? "READY" : "IDLE")
                       << " but " << consecutive_ready_idle_resend_count_
                       << " consecutive resends all failed (UNAVAILABLE). "
                       << "Failing all " << pending_requests_.size()
                       << " pending requests. "
                       << "This indicates the target IP may have been reused by a "
                       << "different pod that cannot serve gRPC requests.";
      while (!pending_requests_.empty()) {
        auto iter = pending_requests_.begin();
        iter->second->Fail(ray::Status::TimedOut(absl::StrFormat(
            "Channel READY/IDLE but %d consecutive resends failed for %s. "
            "Target node is likely dead with IP reused.",
            consecutive_ready_idle_resend_count_,
            server_name_)));
        pending_requests_.erase(iter);
      }
      pending_requests_bytes_ = 0;
      consecutive_ready_idle_resend_count_ = 0;
      break;
    }

    consecutive_ready_idle_resend_count_++;
    // 正常重发逻辑...
}
```

处理 "IP 被复用" 场景：TCP 连得通（channel READY），但 gRPC 请求失败（UNAVAILABLE）。连续10次后直接 Fail 所有 pending requests。

### 修复3: 增加超时扫描日志

```cpp
// retryable_grpc_client.cc — 修复后新增
while (!pending_requests_.empty()) {
    auto iter = pending_requests_.begin();
    if (iter->first > now) {
      break;
    }
+   RAY_LOG(WARNING) << server_name_
+                    << " pending request timed out in retry queue "
+                    << "(channel_state=" << channel_->GetState(false)
+                    << ", pending_requests=" << pending_requests_.size()
+                    << ", resend_count=" << consecutive_ready_idle_resend_count_
+                    << ", attempt_number=" << attempt_number_ << ")";
    iter->second->Fail(ray::Status::TimedOut(...));
    // ...
}
```

### 修复4: TRANSIENT_FAILURE 分支增加日志

```cpp
case GRPC_CHANNEL_TRANSIENT_FAILURE:
case GRPC_CHANNEL_CONNECTING: {
+   consecutive_ready_idle_resend_count_ = 0;
    if (server_unavailable_timeout_time_ < now) {
      RAY_LOG(WARNING) << server_name_ << " has been unavailable for more than "
+                      << " seconds"
+                      << " (channel_state=TRANSIENT_FAILURE, pending_requests="
+                      << pending_requests_.size() << ")";
      // ...
    }
}
```

### 修复5: Reset consecutive count

```cpp
if (pending_requests_.empty()) {
    server_unavailable_timeout_time_ = std::nullopt;
+   consecutive_ready_idle_resend_count_ = 0;
    return;
}
```

## 日志级别配置

### 正确的环境变量

Ray 日志级别由 **`RAY_BACKEND_LOG_LEVEL`** 环境变量控制，不是 `RAY_LOG_LEVEL`。

`RAY_LOG_LEVEL` 在 Ray 代码中不存在，设置后不会有任何效果。

```cpp
// src/ray/util/logging.cc:283-306
void RayLog::InitSeverityThreshold(RayLogLevel severity_threshold) {
  const char *var_value = std::getenv("RAY_BACKEND_LOG_LEVEL");
  if (var_value != nullptr) {
    std::string data = var_value;
    std::transform(data.begin(), data.end(), data.begin(), ::tolower);
    if (data == "trace") {
      severity_threshold = RayLogLevel::TRACE;
    } else if (data == "debug") {
      severity_threshold = RayLogLevel::DEBUG;
    } else if (data == "info") {
      severity_threshold = RayLogLevel::INFO;
    } else if (data == "warning") {
      severity_threshold = RayLogLevel::WARNING;
    } else if (data == "error") {
      severity_threshold = RayLogLevel::ERROR;
    } else if (data == "fatal") {
      severity_threshold = RayLogLevel::FATAL;
    }
    RAY_LOG(INFO) << "Set ray log level from environment variable RAY_BACKEND_LOG_LEVEL"
                  << " to " << static_cast<int>(severity_threshold);
  }
  severity_threshold_ = severity_threshold;
}
```

| 环境变量 | 控制范围 | 有效值 |
|---------|---------|-------|
| `RAY_BACKEND_LOG_LEVEL` | C++ 层（raylet、core worker、GCS） | trace/debug/info/warning/error/fatal（不区分大小写） |
| `RAY_DEDUP_LOGS` | Python 层日志去重 | 0/1 |

### 生效时机

`getenv` 读的是进程启动时的环境变量，只在 `InitSeverityThreshold()` 时调用一次，之后不会重新读取。

```bash
# 正确用法：提交 job 时设置
RAY_BACKEND_LOG_LEVEL=debug ray job submit ...
```

**生效范围：**

| 组件 | 是否生效 | 原因 |
|------|---------|------|
| 新 job 的 driver 进程 | ✅ | 新进程，启动时继承环境变量 |
| 新 job 的 worker 进程 | ❌ | worker 由 raylet 拉起，继承 raylet 的环境，不是 job submit 的环境 |
| raylet | ❌ | 已启动，不会重新读环境变量 |
| GCS | ❌ | 已启动 |

对于排查本问题，**driver 的 DEBUG 日志就够了**——关键日志在 `NormalTaskSubmitter`（Requesting lease、spillback 目标地址、callback 触发）和 `RetryableGrpcClient`（channel 状态、重试详情），这些都运行在 driver 的 core worker 线程里。

如果要改 raylet 的日志级别，需要重启集群时设置，无法对运行中的集群动态修改。

## 代码关键文件

| 文件 | 路径 | 关键内容 |
|------|------|----------|
| NormalTaskSubmitter | `src/ray/core_worker/task_submission/normal_task_submitter.cc` | RequestNewWorkerIfNeeded, is_spillback, spillback callback |
| TaskManager | `src/ray/core_worker/task_manager.cc` | FailPendingTask |
| LeasePolicy | `src/ray/core_worker/lease_policy.cc` | LocalityAwareLeasePolicy::GetBestNodeForLease, GetBestNodeIdForLease |
| RetryableGrpcClient | `src/ray/rpc/retryable_grpc_client.cc` | CheckChannelStatus, Retry, 超时扫描, channel 状态处理 |
| RetryableGrpcClient Header | `src/ray/rpc/retryable_grpc_client.h` | INVOKE_RETRYABLE_RPC_CALL 宏, RetryableGrpcRequest::Create, executor/回调 |
| RayletClient | `src/ray/raylet_rpc_client/raylet_client.cc` | RequestWorkerLease, method_timeout_ms |
| ClientCall | `src/ray/rpc/client_call.h` | ClientCallImpl 构造函数, gRPC deadline 设置 |
| RayLog | `src/ray/util/logging.cc` | InitSeverityThreshold, RAY_BACKEND_LOG_LEVEL |
| RayletClientPool | `src/ray/raylet_rpc_client/raylet_client_pool.cc` | GetDefaultUnavailableTimeoutCallback, Disconnect |
| CoreWorkerProcess | `src/ray/core_worker/core_worker_process.cc` | RayletClientPool 创建, callback 绑定 |

## 结论

根因是运行集群的 Ray 版本（`2.54.0+kuaishou.a760958466`）未包含修复 commit `82d05363`。当 LocalityAwareLeasePolicy 选中了一个已死亡的远端节点作为 RequestWorkerLease 目标时，由于 `method_timeout_ms=-1`，RetryableGrpcClient 将 UNAVAILABLE 错误吞掉并无限重试，callback 永远不触发，task 永久卡在 PENDING_NODE_ASSIGNMENT。

修复方案: 将 `82d05363`（分支 `T11613716-rpc-timeout`）合入运行集群的 Ray 版本并重新部署。

注意：修复 commit 只在 `grant_or_reject=true`（真正 spillback）时加了超时，`grant_or_reject=false`（locality-aware 路径）时 `method_timeout_ms` 仍然是 -1，该路径的死锁可能仍未完全修复。建议同时给 `grant_or_reject=false` 的路径也加上超时。
