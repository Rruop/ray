# RequestWorkerLease gRPC 卡死深入分析

> 本文档是对 `ray-task-event-eviction-and-lease-stuck-analysis.md` 中"问题二：RequestWorkerLease gRPC 卡死"的深入代码级分析，详细追踪了从 spillback 到死节点直到永久卡死的完整调用链、`Disconnect` 不 fail pending requests 的根因、以及具体的确认和修复方法。

---

## 一、核心问题：为什么 gRPC 失败后"重试"反而导致卡死？

gRPC 失败后确实会重试。**问题的本质是重试层"吞掉"了错误，导致上层（NormalTaskSubmitter）永远不知道请求失败了。**

正常设计意图：
- `RetryableGrpcClient` 用于处理"网络短暂断开后自动恢复"的场景
- 可重试错误（UNAVAILABLE/UNKNOWN）不立即通知调用方，而是在队列中等待 channel 恢复后重发

但在"目标节点永久死亡"场景下：
- 请求永远不可能成功
- 但重试机制没有终止条件（`method_timeout_ms = -1` → `InfiniteFuture`）
- NormalTaskSubmitter 的 callback 永远不被调用
- Task 永远无法被重新调度

**核心矛盾**：`RetryableGrpcClient` 把"该不该重试"的决定权从 NormalTaskSubmitter 手中夺走了——上层有能力处理失败（重试本地调度），但重试层不给它这个机会。

---

## 二、完整调用链（带代码注释）

### 第一步：NormalTaskSubmitter 发起 spillback lease 请求

**文件**: `src/ray/core_worker/task_submission/normal_task_submitter.cc:274-328`

```cpp
void NormalTaskSubmitter::RequestNewWorkerIfNeeded(
    const SchedulingKey &scheduling_key,
    const rpc::Address *raylet_address) {  // ← spillback 时传入死节点地址

  auto &scheduling_key_entry = scheduling_key_entries_[scheduling_key];

  // ... 前置检查（pending lease 数量限制、idle worker 检查等）...

  const bool is_spillback = (raylet_address != nullptr);  // ← true

  // 通过 pool 获取到死节点的 RayletClient
  // 如果 pool 中没有会新建一个 gRPC 连接
  auto raylet_client = raylet_client_pool_->GetOrConnectByAddress(*raylet_address);
  //                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  // 返回 shared_ptr<RayletClient>, 但这是局部变量，函数结束后释放

  // 发起 lease 请求
  raylet_client->RequestWorkerLease(
      lease_spec.GetMessage(),
      /*grant_or_reject=*/is_spillback,  // spillback 时为 true
      // ===== 这个 callback 就是"解卡的关键" =====
      // 只有它被调用，task 才能被重新调度
      [this, scheduling_key, lease_id, function_or_actor_name,
       is_spillback, raylet_address = *raylet_address](
          const Status &status, const rpc::RequestWorkerLeaseReply &reply) {
        // ...
        if (status.ok()) {
          // 各种成功处理...
        } else if (NodeID::FromBinary(raylet_address.node_id()) != local_node_id_) {
          // ===== 远程 lease 失败 → 重试本地调度 =====
          // 如果这个 callback 能被调用到，task 就能恢复!
          // TODO(swang): Fail after some number of retries?
          RequestNewWorkerIfNeeded(scheduling_key);  // 重新本地调度
        }
      },
      ...);

  // 将 lease_id 加入 pending_lease_requests
  // 在 callback 被调用之前不会被移除!
  scheduling_key_entry.pending_lease_requests.emplace(lease_id, *raylet_address);
}
```

**关键点**：`pending_lease_requests[lease_id]` 被插入后，只有 callback 被调用后才会 `erase`（第 348 行）。如果 callback 永远不被调用，这个 lease 就永远占着坑位，task 永远无法被调度。

### 第二步：RayletClient 转发到 RetryableGrpcClient

**文件**: `src/ray/raylet_rpc_client/raylet_client.cc:53-71`

```cpp
void RayletClient::RequestWorkerLease(
    const rpc::LeaseSpec &lease_spec,
    bool grant_or_reject,
    const rpc::ClientCallback<rpc::RequestWorkerLeaseReply> &callback, ...) {
  rpc::RequestWorkerLeaseRequest request;
  // ... 填充 request ...

  INVOKE_RETRYABLE_RPC_CALL(retryable_grpc_client_,
                            NodeManagerService,
                            RequestWorkerLease,
                            request,
                            callback,      // ← NormalTaskSubmitter 的 callback
                            grpc_client_,
                            /*method_timeout_ms*/ -1);  // ← 关键: -1 = 永不超时!
}
```

`INVOKE_RETRYABLE_RPC_CALL` 宏展开（`retryable_grpc_client.h:37-50`）：
```cpp
retryable_grpc_client_->CallMethod<NodeManagerService,
                                    RequestWorkerLeaseRequest,
                                    RequestWorkerLeaseReply>(
    &NodeManagerService::Stub::PrepareAsyncRequestWorkerLease,
    grpc_client_,
    "NodeManagerService.grpc_client.RequestWorkerLease",
    std::move(request),
    callback,
    /*timeout_ms=*/ -1);   // ← 这个 -1 决定了重试时的超时行为
```

### 第三步：CallMethod 创建 RetryableGrpcRequest 并发送

**文件**: `src/ray/rpc/retryable_grpc_client.h:240-257`

```cpp
template <typename Service, typename Request, typename Reply>
void RetryableGrpcClient::CallMethod(..., int64_t timeout_ms) {
  num_active_requests_++;

  // 创建一个 RetryableGrpcRequest 对象，包含:
  //   executor_: 实际发送 gRPC 的函数
  //   failure_callback_: 请求被 Fail() 时调用上层 callback
  //   timeout_ms_: -1 (传进来的)
  RetryableGrpcRequest::Create(weak_from_this(), ..., callback, timeout_ms)
      ->CallMethod();  // ← 立即调用 executor_ 发送第一次 gRPC
}
```

### 第四步：executor_ 内部 — gRPC 结果的分支决策

**文件**: `src/ray/rpc/retryable_grpc_client.h:274-298`

```cpp
auto executor = [weak_retryable_grpc_client,
                 prepare_async_function,
                 grpc_client,           // ← shared_ptr<GrpcClient>, 强引用
                 call_name, request,
                 callback]              // ← NormalTaskSubmitter 的 callback
    (std::shared_ptr<RetryableGrpcRequest> retryable_grpc_request) {

  // 真正通过 gRPC 发送请求
  grpc_client->template CallMethod<Request, Reply>(
      prepare_async_function,
      request,
      // ===== gRPC 返回后的回调（在 io_context 线程中执行）=====
      [weak_retryable_grpc_client, retryable_grpc_request, callback](
          const ray::Status &status, Reply &&reply) {

        auto retryable_grpc_client = weak_retryable_grpc_client.lock();

        if (status.ok() || !IsGrpcRetryableStatus(status) || !retryable_grpc_client) {
          // 情况1: 成功 → 调用 NormalTaskSubmitter callback
          // 情况2: 不可重试错误 → 也调用 callback
          // 情况3: RetryableGrpcClient 已析构 → 也调用 callback
          callback(status, std::move(reply));
          return;
        }

        // ===== 情况4: 可重试错误 (UNAVAILABLE 或 UNKNOWN) =====
        // ===== 不调用 callback! 放入重试队列 =====
        retryable_grpc_client->Retry(retryable_grpc_request);
        // ↑ NormalTaskSubmitter 的 callback 此时没有被调用!
        //   它完全不知道请求失败了
      },
      call_name,
      retryable_grpc_request->GetTimeoutMs());
};
```

**分支决策表**：

| gRPC 返回状态 | `IsGrpcRetryableStatus` | 行为 | NormalTaskSubmitter callback |
|---|---|---|---|
| OK | - | 直接调用 callback | **被调用** ✓ |
| PERMISSION_DENIED 等 | false | 直接调用 callback | **被调用** ✓ |
| **UNAVAILABLE** | **true** | **进入 Retry 队列** | **不被调用** ✗ |
| **UNKNOWN** | **true** | **进入 Retry 队列** | **不被调用** ✗ |

`IsGrpcRetryableStatus` 定义（`src/ray/common/grpc_util.h:130-133`）：
```cpp
inline bool IsGrpcRetryableStatus(Status status) {
  return status.IsRpcError() && (status.rpc_code() == grpc::StatusCode::UNAVAILABLE ||
                                 status.rpc_code() == grpc::StatusCode::UNKNOWN);
}
```

连接死节点时，gRPC 返回 `UNAVAILABLE`，所以走 `Retry` 路径 — **callback 不被调用**。

### 第五步：Retry 将请求放入永不超时的等待队列

**文件**: `src/ray/rpc/retryable_grpc_client.cc:131-173`

```cpp
void RetryableGrpcClient::Retry(std::shared_ptr<RetryableGrpcRequest> request) {
  const auto now = absl::Now();
  const auto request_bytes = request->GetRequestBytes();
  auto self = shared_from_this();

  // 背压分支（pending 超限时阻塞线程，此场景不走）
  if (pending_requests_bytes_ + request_bytes > max_pending_requests_bytes_) { ... }

  // 计算超时时间
  pending_requests_bytes_ += request_bytes;
  const auto timeout = request->GetTimeoutMs() == -1
                           ? absl::InfiniteFuture()    // ← timeout_ms=-1 → 永不超时!
                           : now + absl::Milliseconds(request->GetTimeoutMs());

  // 放入 pending_requests_ (按 timeout 排序的 multimap)
  pending_requests_.emplace(timeout, std::move(request));
  //                        ^^^^^^^^
  // key = InfiniteFuture, 在 CheckChannelStatus 的超时清理中:
  //   iter->first (InfiniteFuture) > now → 永远成立 → 永远不会被超时淘汰

  if (!server_unavailable_timeout_time_.has_value()) {
    // 第一次重试，启动定时器
    server_unavailable_timeout_time_ =
        now + absl::Seconds(server_reconnect_timeout_base_seconds_);
    SetupCheckTimer();  // ← 开始周期性执行 CheckChannelStatus
  }
}
```

### 第六步：CheckChannelStatus — 两种循环路径都无法终止

**文件**: `src/ray/rpc/retryable_grpc_client.cc:52-129`

```cpp
void RetryableGrpcClient::CheckChannelStatus(bool reset_timer) {
  const auto now = absl::Now();

  // ========== 超时清理 ==========
  while (!pending_requests_.empty()) {
    auto iter = pending_requests_.begin();
    if (iter->first > now) {
      break;  // ← InfiniteFuture > now 永远成立! 永远不会在这里被清理
    }
    iter->second->Fail(ray::Status::TimedOut(...));  // ← 永远执行不到
  }

  if (pending_requests_.empty()) {
    server_unavailable_timeout_time_ = std::nullopt;
    return;  // 队列空则返回，不重启 timer
  }

  // ========== 检查 gRPC channel 状态 ==========
  auto status = channel_->GetState(false);

  switch (status) {
  // ===== 路径 A: TRANSIENT_FAILURE/CONNECTING =====
  case GRPC_CHANNEL_TRANSIENT_FAILURE:
  case GRPC_CHANNEL_CONNECTING: {
    if (server_unavailable_timeout_time_ < now) {
      // 超过等待阈值 → 触发 unavailable callback
      server_unavailable_timeout_callback_();
      // ↑ 这会查 GCS、确认节点死亡后调 Disconnect()
      // ↑ 但 Disconnect 只从 pool 移除引用，不 fail pending!（见后文详细分析）

      attempt_number_++;
      server_unavailable_timeout_time_ = now + absl::Seconds(exponential_backoff);
    }
    if (reset_timer) {
      SetupCheckTimer();  // ← 继续定时检查，无限循环
    }
    break;
  }

  // ===== 路径 B: READY/IDLE（最危险的卡死路径）=====
  case GRPC_CHANNEL_READY:
  case GRPC_CHANNEL_IDLE: {
    server_unavailable_timeout_time_ = std::nullopt;  // ← 清除!
    // 重发所有 pending 请求
    while (!pending_requests_.empty()) {
      pending_requests_.begin()->second->CallMethod();  // ← 重发
      pending_requests_.erase(pending_requests_.begin());
    }
    pending_requests_bytes_ = 0;
    attempt_number_ = 0;  // ← 重置重试计数!
    break;
    // ← 没有调用 SetupCheckTimer()! Timer 暂停
    // ← 没有触发 server_unavailable_timeout_callback_()!
    //    所以 Disconnect() 永远不会在这个分支被调用
  }
  }
}
```

---

## 三、两种卡死路径的详细分析

### 路径 A：channel = TRANSIENT_FAILURE

```
Timer → CheckChannelStatus → TRANSIENT_FAILURE
  → 等到 timeout → 触发 callback → GCS 查询节点
    → 如果 GCS 确认死亡 → Disconnect(node_id)
      → client_map_.erase (见后文为什么这不能解卡)
    → 如果 GCS 查询失败 → "Failed to get node info" → 什么都不做
  → SetupCheckTimer → 下次继续
```

这条路径**有可能恢复**（如果 Disconnect 后对象最终析构），但不可靠（见第四节）。

### 路径 B：channel = READY/IDLE（主要卡死路径）

```
Timer → CheckChannelStatus → READY/IDLE
  → 重发请求 → gRPC 调用发出
  → 目标节点已死 → UNAVAILABLE
  → IsGrpcRetryableStatus = true → Retry()
  → Retry 中: server_unavailable_timeout_time_ == nullopt
    → 设置新的 timeout, SetupCheckTimer() → Timer 重启
  → 下次 Timer → CheckChannelStatus → channel 仍是 READY/IDLE
  → 又重发 → 又失败 → 又 Retry → 无限循环!
```

**为什么这条路径比路径 A 更危险**:
1. `server_unavailable_timeout_callback_()` **永远不会被触发** → `Disconnect` 永远不会被调用
2. `attempt_number_` **每次被重置为 0** → 无法通过阈值触发任何保护机制
3. 循环频率很高（timer interval 毫秒级 + gRPC 往返秒级）

**为什么死节点的 channel 可能显示 READY/IDLE?**

gRPC channel 状态基于 **TCP 传输层** 而非 **应用层 RPC 成功与否**:

| K8s 场景 | channel 状态 | RPC 结果 |
|----------|-------------|---------|
| 死节点 IP 被新 Pod 接管 | READY | UNAVAILABLE (新 Pod 不是 Raylet) |
| kube-proxy 能接受 TCP 但后端已无 | READY | UNAVAILABLE |
| DNS 解析成功但 TCP 未建立 | IDLE | - |
| TCP 连接被 RST | TRANSIENT_FAILURE | UNAVAILABLE |

在 K8s 环境中（本案例的 KML 平台），前两种情况非常常见。

### 卡死循环全景图

```
┌────────────────────────────────────────────────────────────────────┐
│                                                                     │
│                              ┌──────────────────────────────┐      │
│                              │                              │      │
│                              ▼                              │      │
│  ┌─ CheckChannelStatus ────────────────────────────────┐   │      │
│  │                                                      │   │      │
│  │  超时清理: InfiniteFuture > now → 永远不清理         │   │      │
│  │                                                      │   │      │
│  │  channel == READY/IDLE:                              │   │      │
│  │    - 不触发 server_unavailable_timeout_callback_()   │   │      │
│  │    - Disconnect() 永远不会被调用                     │   │      │
│  │    - attempt_number_ = 0 (重置)                      │   │      │
│  │    - 重发: CallMethod() ──────────────────────────────┼───┼──┐  │
│  │    - 不调用 SetupCheckTimer()                        │   │  │  │
│  │                                                      │   │  │  │
│  └──────────────────────────────────────────────────────┘   │  │  │
│                                                              │  │  │
│       ┌──────────────────────────────────────────────────────┘  │  │
│       │ (如果 TRANSIENT_FAILURE → SetupCheckTimer 继续)         │  │
│       └──────────────────────────────────────────────────────────┘  │
│                                                                     │
│  ┌──────────────────────────────────────────────────────────────┘  │
│  │ gRPC 请求发出, 目标节点已死                                     │
│  ▼                                                                  │
│  ┌─ gRPC CompletionQueue ──────────────────────────┐               │
│  │  返回 UNAVAILABLE                                │               │
│  └───────────────────┬─────────────────────────────┘               │
│                      │                                              │
│                      ▼                                              │
│  ┌─ executor_ 回调 ─────────────────────────────────┐              │
│  │  IsGrpcRetryableStatus(UNAVAILABLE) = true        │              │
│  │  → 不调用 NormalTaskSubmitter callback!           │              │
│  │  → retryable_grpc_client->Retry(request)          │              │
│  └───────────────────┬──────────────────────────────┘              │
│                      │                                              │
│                      ▼                                              │
│  ┌─ Retry() ────────────────────────────────────────┐              │
│  │  timeout = InfiniteFuture (method_timeout=-1)     │              │
│  │  pending_requests_.emplace(∞, request)            │              │
│  │                                                   │              │
│  │  server_unavailable_timeout_time_ == nullopt?     │              │
│  │  YES (被 READY/IDLE 分支清过)                     │              │
│  │    → SetupCheckTimer() ────────────────────────────┼──────────────┘
│  │                                                   │
│  └───────────────────────────────────────────────────┘
│
│  整个循环中:
│  - NormalTaskSubmitter 的 callback 从未被调用
│  - pending_lease_requests[lease_id] 永远不被 erase
│  - scheduling_key_entry.task_queue 中的 task 永远不被调度
│  - Ray Data 永远显示 "Tasks: 1"
└─────────────────────────────────────────────────────────────────────┘
```

---

## 四、`Disconnect` 为什么不 fail 已有 request — 详细分析

### 4.1 Disconnect 的代码

```cpp
// src/ray/raylet_rpc_client/raylet_client_pool.cc:100-107
void RayletClientPool::Disconnect(ray::NodeID id) {
  absl::MutexLock lock(&mu_);
  auto it = client_map_.find(id);
  if (it == client_map_.end()) return;
  client_map_.erase(it);   // 就这一行。结束了。
  // 没有调用 it->second->Shutdown()
  // 没有调用 retryable_grpc_client_->FailAllPending()
  // 没有任何通知内部清理的逻辑
}
```

头文件注释明确说明了设计意图（`raylet_client_pool.h:49-51`）：
```cpp
/// Removes a connection to the worker from the pool, if one exists. Since the
/// shared pointer will no longer be retained in the pool, the connection will
/// be open until it's no longer used, at which time it will disconnect.
```

设计思路是：**"我只管从池子里拿走引用，对象自己引用计数归零时自然清理"**。

### 4.2 对象所有权和引用计数

```
client_map_[dead_node_id] → shared_ptr<RayletClient>   ← erase 删的是这个
                                    │
                                    ├── grpc_client_: shared_ptr<GrpcClient>
                                    │       ↑
                                    │       │ (RetryableGrpcRequest 的 executor_ 也持有一份)
                                    │
                                    └── retryable_grpc_client_: shared_ptr<RetryableGrpcClient>
                                                │
                                                ├── pending_requests_: 卡死的请求在这里
                                                └── timer_: 回调用 weak_ptr (不阻止析构)
```

### 4.3 erase 后对象能否析构？

**在 TRANSIENT_FAILURE 场景下**：`Disconnect` 是在 `CheckChannelStatus` 的 timer 回调中被调用的。此时调用栈上有 `self = weak_self.lock()` 持有一份 `shared_ptr<RetryableGrpcClient>`。

时序分析：
```
T1: Timer 回调: auto self = weak_self.lock() → 成功
    → 引用计数: client_map_(1) + timer回调的self(1) = 2

T2: CheckChannelStatus → server_unavailable_timeout_callback_()
    → Disconnect → client_map_.erase
    → 引用计数: timer回调的self(1) = 1

T3: SetupCheckTimer() ← 注册新 timer，用 weak_ptr，不增加引用计数

T4: Timer 回调返回 → self 析构
    → 引用计数: 0
    → RayletClient 析构 → retryable_grpc_client_ 释放

T5: RetryableGrpcClient 析构:
    ~RetryableGrpcClient() {
      timer_.cancel();  // 取消 T3 注册的新 timer
      while (!pending_requests_.empty()) {
        request->Fail(Status::Disconnected("GRPC client is shut down."));
        // ↑ 调用 failure_callback_ → callback(Disconnected, {})
        // ↑ NormalTaskSubmitter 的 callback 终于被调用!
      }
    }
```

**所以在 TRANSIENT_FAILURE 场景下**，如果 GCS 查询成功且确认节点死亡 → `Disconnect` → 对象最终析构 → callback 被调用 → task 能恢复。

**但有两个"如果"使得这条恢复路径不可靠**：
1. GCS 查询可能失败 → `Disconnect` 不被调用 → 下次重试
2. 更重要的是：**如果 channel 不是 TRANSIENT_FAILURE 而是 READY/IDLE，`server_unavailable_timeout_callback_` 永远不会被触发，`Disconnect` 永远不会被调用!**

### 4.4 为什么 READY/IDLE 场景下永远不析构

在 READY/IDLE 分支中：
1. `server_unavailable_timeout_callback_()` 不会被调用
2. `Disconnect()` 不会被调用
3. `client_map_` 中的 `shared_ptr<RayletClient>` 永远存在
4. `RayletClient` 永远不析构
5. `RetryableGrpcClient` 永远不析构
6. `pending_requests_` 中的请求永远不被 fail
7. **NormalTaskSubmitter 的 callback 永远不被调用**

**这就是 20+ 小时卡死的根因。**

### 4.5 设计缺陷总结

| 缺陷 | 表现 |
|------|------|
| `Disconnect` 只删引用不 fail pending | 依赖引用计数归零触发析构，但析构本身在 READY/IDLE 场景不会发生 |
| READY/IDLE 不触发 unavailable callback | 死节点的 channel 在 K8s 中可能维持 READY/IDLE |
| `attempt_number_` 在 READY/IDLE 中被重置 | 无法通过阈值触发保护机制 |
| `method_timeout_ms = -1` | 请求永远不会超时，是最根本的兜底缺失 |

---

## 五、如何确认是否因这个问题导致的卡死

### 5.1 快速确认步骤

```bash
# 步骤 1: 确认有 lease 卡死
grep "RequestWorkerLease.*active" /tmp/ray/session_latest/logs/raylet.out | tail -5
# 预期: "RequestWorkerLease - XXXXX total (1 active)"
# 如果 active > 0 且持续不变 → 确认存在卡死的 lease

# 步骤 2: 确认有死节点
grep -i "node.*dead\|died\|disconnect" /tmp/ray/session_latest/logs/raylet.out | tail -10
# 或
ray list nodes --filter "state=DEAD"

# 步骤 3: 确认 spillback 到死节点
grep "Redirect lease\|Spilling lease" /tmp/ray/session_latest/logs/raylet.out | tail -10
# 看是否有 spillback 到后来死掉的节点

# 步骤 4: 关键确认 — callback 是否被调用
grep "Retrying attempt to schedule lease" /tmp/ray/session_latest/logs/worker-*.out
# 如果看不到这条日志 → callback 从未被调用 → 确认是这个 bug!
# 如果能看到 → callback 被调用了，可能是其他问题

# 步骤 5: 判断卡死路径（TRANSIENT_FAILURE vs READY/IDLE）
grep "has been unavailable" /tmp/ray/session_latest/logs/worker-*.out
# 如果看不到 → channel 没进入 TRANSIENT_FAILURE → READY/IDLE 循环 → 最危险的场景
# 如果能看到 → 走的是 TRANSIENT_FAILURE 路径，Disconnect 可能不生效
```

### 5.2 关键日志模式匹配

以下是定位此问题需要关注的**日志关键词及其含义**：

| 日志关键词 | 来源文件 | 含义 | 判断 |
|---|---|---|---|
| `RequestWorkerLease - N total (M active)` | raylet.out | 当前 lease 请求统计 | M > 0 且持续不变 = 卡死 |
| `Redirect lease <id> from raylet <A> to raylet <B>` | worker-*.out (normal_task_submitter.cc:428) | 本地 Raylet 发起 spillback | 记录 B 节点，后续检查是否死亡 |
| `Retrying attempt to schedule lease` | worker-*.out (normal_task_submitter.cc:441) | 远程 lease 失败后回调被触发 | **出现 = 非此 bug**（callback 被调用了） |
| `has been unavailable for more than N seconds` | worker-*.out (retryable_grpc_client.cc:85) | channel 在 TRANSIENT_FAILURE | 出现 = 路径 A |
| `Failed to get node info from GCS` | worker-*.out (raylet_client_pool.cc:37) | GCS 查询失败，Disconnect 未执行 | 出现 = Disconnect 恢复路径被阻断 |
| `Disconnecting raylet client because its node is dead` | worker-*.out (raylet_client_pool.cc:49) | GCS 确认节点死亡，执行 Disconnect | 出现 = 路径 A 的 Disconnect 被触发 |
| `Dropping task status events for task` | worker-*.out | Worker buffer 满 | task event 淘汰（可观测性问题） |
| `Max number of tasks event` | gcs_server.out | GCS 存储满 | task 在 dashboard 不可见 |

### 5.3 综合诊断脚本

```bash
#!/bin/bash
# ray_lease_stuck_diagnosis.sh
# 用法: bash ray_lease_stuck_diagnosis.sh [ray_session_dir]

RAY_DIR="${1:-/tmp/ray/session_latest}"
echo "========================================"
echo " RequestWorkerLease 卡死问题诊断"
echo " Ray session: $RAY_DIR"
echo "========================================"

echo ""
echo "=== 1. Lease 请求状态（是否有 active 卡死）==="
ACTIVE_LINE=$(grep "RequestWorkerLease.*active" "$RAY_DIR/logs/raylet.out" 2>/dev/null | tail -1)
if [ -n "$ACTIVE_LINE" ]; then
    echo "  最新状态: $ACTIVE_LINE"
    ACTIVE_COUNT=$(echo "$ACTIVE_LINE" | grep -oP '\d+(?= active)')
    if [ "$ACTIVE_COUNT" -gt 0 ]; then
        echo "  *** 警告: 有 $ACTIVE_COUNT 个 active lease 请求 ***"
        echo "  检查是否持续不变（间隔 30s 再执行一次对比）"
    fi
else
    echo "  未找到 RequestWorkerLease 日志"
fi

echo ""
echo "=== 2. 节点死亡事件 ==="
DEAD_NODES=$(grep -i "node.*dead\|Disconnecting raylet client" "$RAY_DIR/logs/"*.out 2>/dev/null | tail -5)
if [ -n "$DEAD_NODES" ]; then
    echo "$DEAD_NODES"
else
    echo "  未发现节点死亡事件"
fi

echo ""
echo "=== 3. Spillback/Redirect 事件 ==="
REDIRECTS=$(grep "Redirect lease\|Spilling lease\|retry_at_raylet" "$RAY_DIR/logs/"*.out 2>/dev/null | tail -5)
if [ -n "$REDIRECTS" ]; then
    echo "$REDIRECTS"
else
    echo "  未发现 spillback 事件"
fi

echo ""
echo "=== 4. 关键判断: NormalTaskSubmitter callback 是否被调用 ==="
RETRY_LOG=$(grep -c "Retrying attempt to schedule lease" "$RAY_DIR/logs/"worker-*.out 2>/dev/null)
if [ "$RETRY_LOG" -gt 0 ]; then
    echo "  callback 被调用了 ${RETRY_LOG} 次 → 可能不是 gRPC 卡死 bug"
    echo "  最近的重试日志:"
    grep "Retrying attempt to schedule lease" "$RAY_DIR/logs/"worker-*.out 2>/dev/null | tail -3
else
    echo "  *** callback 从未被调用 → 高度疑似 gRPC 卡死 bug ***"
fi

echo ""
echo "=== 5. Channel 状态判断 ==="
UNAVAILABLE_LOG=$(grep -c "has been unavailable" "$RAY_DIR/logs/"worker-*.out 2>/dev/null)
if [ "$UNAVAILABLE_LOG" -gt 0 ]; then
    echo "  channel 进入过 TRANSIENT_FAILURE 状态 ${UNAVAILABLE_LOG} 次"
    echo "  → 走的是路径 A（可能通过 Disconnect 析构恢复，也可能 GCS 查询失败）"
    echo "  检查 GCS 查询是否失败:"
    grep -c "Failed to get node info from GCS" "$RAY_DIR/logs/"worker-*.out 2>/dev/null
else
    echo "  *** channel 未进入 TRANSIENT_FAILURE → 走的是 READY/IDLE 循环 ***"
    echo "  *** 这是最危险的卡死路径：Disconnect 永远不会被触发 ***"
fi

echo ""
echo "=== 6. Disconnect 是否生效 ==="
DISCONNECT_LOG=$(grep -c "Disconnecting raylet client because its node is dead" "$RAY_DIR/logs/"worker-*.out 2>/dev/null)
echo "  Disconnect 被触发次数: $DISCONNECT_LOG"

echo ""
echo "=== 7. gRPC 重试频率（判断 READY/IDLE 循环）==="
# 如果有 debug 日志开启，可以看到频繁的 CallMethod
echo "  (需要开启 RAY_BACKEND_LOG_LEVEL=debug 才能看到重试频率)"
echo "  替代方法: 观察 CPU 使用率是否异常偏高（频繁重试消耗 CPU）"

echo ""
echo "========================================"
echo " 诊断结论"
echo "========================================"
if [ "$ACTIVE_COUNT" -gt 0 ] && [ "$RETRY_LOG" -eq 0 ]; then
    echo "  *** 高度确认: RequestWorkerLease gRPC 卡死 ***"
    echo "  active lease: $ACTIVE_COUNT"
    echo "  callback 调用次数: 0"
    if [ "$UNAVAILABLE_LOG" -eq 0 ]; then
        echo "  卡死路径: READY/IDLE 循环（最严重）"
    else
        echo "  卡死路径: TRANSIENT_FAILURE + Disconnect 不生效"
    fi
    echo ""
    echo "  修复方案:"
    echo "    1. 临时: 重启 driver 进程"
    echo "    2. 根本: 设置 RAY_worker_lease_timeout_ms=300000"
    echo "       或升级到包含 timeout 修复的版本"
else
    echo "  暂未确认为此 bug，请进一步排查"
fi
echo "========================================"
```

### 5.4 Python 运行时诊断

```python
#!/usr/bin/env python3
"""
ray_lease_stuck_check.py - 运行时检测 RequestWorkerLease 卡死

用法: python ray_lease_stuck_check.py
要求: 在 Ray 集群可达的环境中执行
"""
import ray
from ray.util.state import list_tasks, list_nodes
import time

def check_lease_stuck():
    ray.init(ignore_reinit_error=True)

    print("=" * 60)
    print("RequestWorkerLease 卡死运行时诊断")
    print("=" * 60)

    # 1. 获取节点状态
    nodes = ray.nodes()
    alive_node_ids = {n["NodeID"] for n in nodes if n["Alive"]}
    dead_node_ids = {n["NodeID"] for n in nodes if not n["Alive"]}
    print(f"\n节点状态: {len(alive_node_ids)} alive, {len(dead_node_ids)} dead")

    if not dead_node_ids:
        print("  没有死节点，不太可能是 spillback 到死节点的问题")
        return

    print(f"  死节点 IDs: {dead_node_ids}")

    # 2. 查看 PENDING_NODE_ASSIGNMENT 的 task
    print("\n检查 PENDING_NODE_ASSIGNMENT tasks...")
    try:
        pending = list_tasks(
            filters=[("state", "=", "PENDING_NODE_ASSIGNMENT")],
            limit=100
        )
    except Exception as e:
        print(f"  警告: list_tasks 失败 ({e})")
        print("  可能是 task event 被淘汰导致无法查询")
        pending = []

    if not pending:
        print("  没有找到 PENDING_NODE_ASSIGNMENT 的 task")
        print("  注意: 如果 task event 已被淘汰，这里可能查不到卡死的 task")
    else:
        print(f"  找到 {len(pending)} 个 PENDING_NODE_ASSIGNMENT tasks")
        bug_confirmed = False
        for t in pending:
            node_id = getattr(t, 'node_id', None) or getattr(t, 'scheduling_node_id', None)
            if node_id and node_id in dead_node_ids:
                print(f"\n  *** BUG CONFIRMED ***")
                print(f"  Task: {t.task_id}")
                print(f"  Name: {getattr(t, 'name', 'unknown')}")
                print(f"  Scheduled to DEAD node: {node_id}")
                bug_confirmed = True

        if not bug_confirmed:
            print("  所有 pending tasks 的 scheduling node 都是存活的")

    # 3. 持续监控 active lease（需要 raylet 级别的监控）
    print("\n" + "=" * 60)
    print("建议: 使用 shell 脚本检查 raylet.out 中的 active lease 数量")
    print("  grep 'RequestWorkerLease.*active' /tmp/ray/session_latest/logs/raylet.out | tail -1")
    print("=" * 60)

if __name__ == "__main__":
    check_lease_stuck()
```

### 5.5 诊断决策树

```
                    lease active > 0 且持续不变?
                    │
          ┌─────── YES ───────┐                    NO → 不是 lease 卡死问题
          │                   │
          ▼                   │
    有死节点?                   │
    │                         │
┌── YES ──┐     NO → 可能是资源不足或其他调度问题
│         │
▼         │
有 spillback 到该死节点?
│
┌── YES ──┐     NO → 可能是其他调度问题
│         │
▼         │
"Retrying attempt to schedule lease" 出现?
│
┌── YES ──────────────────────────────┐
│                                      │
│  不是这个 bug                        │
│  (callback 被调用了，但重试也        │
│   不成功 → 查 infeasible 等问题)     │
│                                      │
└── NO ────────────────────────────────┘
     │
     ▼
*** 确认是这个 bug: gRPC 卡死 ***
     │
     ├── "has been unavailable" 出现?
     │    │
     │    ├── YES → 路径 A: TRANSIENT_FAILURE
     │    │         检查 "Disconnecting raylet client" 是否出现
     │    │         └── YES → Disconnect 触发了但可能析构竞态
     │    │         └── NO → GCS 查询失败阻止了 Disconnect
     │    │
     │    └── NO → *** 路径 B: READY/IDLE 循环 (最严重) ***
     │              Disconnect 永远不会被触发
     │              channel 看起来正常但请求持续失败
     │
     └── 修复: 设置 RAY_worker_lease_timeout_ms=300000
```

### 5.6 日志时间线还原方法

当确认是此 bug 后，按以下顺序还原事件时间线：

```bash
# 1. 找到节点死亡时间
grep -i "node.*dead\|Worker.*died\|lost plasma" $RAY_DIR/logs/raylet.out | \
  awk '{print $1, $2}' | sort | head -5
# 输出类似: [2025-05-14 16:03:18]

# 2. 找到 spillback 到死节点的时间（应在节点死亡前后）
grep "Redirect lease" $RAY_DIR/logs/worker-*.out | \
  grep "<dead_node_id的前几位>" | head -5

# 3. 确认卡死开始时间（最后一次 active 变化）
grep "RequestWorkerLease.*active" $RAY_DIR/logs/raylet.out | \
  awk -F'[\\[\\]]' '/1 active/{print $2}' | tail -1

# 4. 确认当前持续时间
echo "当前时间: $(date)"
echo "卡死开始: <步骤3的输出>"
```

---

## 六、修复方案

### 6.1 方案 A（P0 推荐）：RequestWorkerLease 添加超时

**改动最小，效果最好的兜底修复。** 无论 channel 是什么状态，超时后请求一定会被 fail，callback 一定会被调用。

**文件 1**: `src/ray/common/ray_config_def.h`

添加配置项：
```cpp
/// Timeout in milliseconds for RequestWorkerLease RPC to a remote raylet.
/// When a spillback lease request to a dead node times out, the task will be
/// rescheduled locally via the NormalTaskSubmitter retry path.
/// This prevents permanent scheduling deadlocks caused by infinite gRPC retries
/// to dead nodes. Set to -1 to disable (infinite timeout, original behavior).
RAY_CONFIG(int64_t, worker_lease_timeout_ms, 300000)  // 默认 5 分钟
```

**文件 2**: `src/ray/raylet_rpc_client/raylet_client.cc:70`

```cpp
// 修改前:
/*method_timeout_ms*/ -1

// 修改后:
/*method_timeout_ms*/ RayConfig::instance().worker_lease_timeout_ms()
```

**超时后的恢复路径**：
```
CheckChannelStatus():
  while (!pending_requests_.empty()) {
    auto iter = pending_requests_.begin();
    if (iter->first > now) break;
    // ↑ 300s 后: iter->first (now + 300s) 不再 > now → 不 break!
    iter->second->Fail(Status::TimedOut(...));  // ← 终于被执行!
  }

Fail(TimedOut) → failure_callback_(Status::TimedOut)
  → callback(Status::TimedOut, Reply{})  // NormalTaskSubmitter callback 被调用!

NormalTaskSubmitter callback:
  status.ok() = false
  NodeID::FromBinary(raylet_address.node_id()) != local_node_id_ → true (远程节点)
  → "Retrying attempt to schedule lease..."
  → RequestNewWorkerIfNeeded(scheduling_key)  // 重新本地调度 → task 恢复!
```

**环境变量方式部署（不改代码）**：
```bash
export RAY_worker_lease_timeout_ms=300000
```

### 6.2 方案 B（P1）：Disconnect 时主动 fail pending requests

**文件 1**: `src/ray/rpc/retryable_grpc_client.h`

添加 `Shutdown` 方法声明：
```cpp
class RetryableGrpcClient : public std::enable_shared_from_this<RetryableGrpcClient> {
 public:
  // ...existing...

  /// Fail all pending requests and cancel the timer.
  /// Called when the remote node is confirmed dead.
  void Shutdown();
};
```

**文件 2**: `src/ray/rpc/retryable_grpc_client.cc`

添加实现：
```cpp
void RetryableGrpcClient::Shutdown() {
  timer_.cancel();
  while (!pending_requests_.empty()) {
    auto iter = pending_requests_.begin();
    io_context_.post(
        [request = std::move(iter->second)]() {
          request->Fail(Status::Disconnected("Remote node is dead."));
        },
        "RetryableGrpcClient::Shutdown");
    pending_requests_bytes_ -= iter->second->GetRequestBytes();
    pending_requests_.erase(iter);
  }
  pending_requests_bytes_ = 0;
}
```

**文件 3**: `src/ray/raylet_rpc_client/raylet_client_interface.h`

```cpp
class RayletClientInterface {
 public:
  virtual ~RayletClientInterface() = default;
  virtual void Shutdown() {}  // 新增
};
```

**文件 4**: `src/ray/raylet_rpc_client/raylet_client.h`

```cpp
class RayletClient : public RayletClientInterface {
 public:
  void Shutdown() override {
    retryable_grpc_client_->Shutdown();
  }
};
```

**文件 5**: `src/ray/raylet_rpc_client/raylet_client_pool.cc`

```cpp
void RayletClientPool::Disconnect(ray::NodeID id) {
  std::shared_ptr<ray::RayletClientInterface> client;
  {
    absl::MutexLock lock(&mu_);
    auto it = client_map_.find(id);
    if (it == client_map_.end()) return;
    client = std::move(it->second);
    client_map_.erase(it);
  }
  // 解锁后 shutdown，避免死锁
  client->Shutdown();
}
```

**局限**：只在 TRANSIENT_FAILURE 路径生效（因为只有这个路径触发 callback → `Disconnect`）。READY/IDLE 循环场景仍需方案 A 兜底。

### 6.3 方案 C（P1）：READY/IDLE 分支增加重试上限

**文件**: `src/ray/rpc/retryable_grpc_client.h`

添加成员变量：
```cpp
// Number of consecutive times requests were resent in READY/IDLE state but failed.
uint32_t consecutive_ready_idle_retry_count_ = 0;
```

**文件**: `src/ray/rpc/retryable_grpc_client.cc`

修改 CheckChannelStatus：
```cpp
case GRPC_CHANNEL_READY:
case GRPC_CHANNEL_IDLE: {
  server_unavailable_timeout_time_ = std::nullopt;

  consecutive_ready_idle_retry_count_++;
  if (consecutive_ready_idle_retry_count_ >
      RayConfig::instance().max_grpc_ready_idle_retries()) {
    RAY_LOG(WARNING) << server_name_ << " channel READY/IDLE but "
                     << consecutive_ready_idle_retry_count_
                     << " retries failed. Failing all pending.";
    while (!pending_requests_.empty()) {
      auto iter = pending_requests_.begin();
      iter->second->Fail(Status::TimedOut(
          "Max READY/IDLE retries exceeded for " + server_name_));
      pending_requests_bytes_ -= iter->second->GetRequestBytes();
      pending_requests_.erase(iter);
    }
    pending_requests_bytes_ = 0;
    consecutive_ready_idle_retry_count_ = 0;
    break;
  }

  // 原有逻辑
  while (!pending_requests_.empty()) {
    pending_requests_.begin()->second->CallMethod();
    pending_requests_.erase(pending_requests_.begin());
  }
  pending_requests_bytes_ = 0;
  attempt_number_ = 0;
  break;
}

case GRPC_CHANNEL_TRANSIENT_FAILURE:
case GRPC_CHANNEL_CONNECTING: {
  consecutive_ready_idle_retry_count_ = 0;  // 不是连续 READY/IDLE 就重置
  // ... 原有代码 ...
}
```

在 `Retry()` 被调用时（请求从 READY/IDLE 重发后又失败回来）不重置此计数器，因为它只在 `CheckChannelStatus` 的分支判断中递增和重置。

### 6.4 推荐组合

| 优先级 | 方案 | 覆盖场景 | 改动量 |
|--------|------|----------|--------|
| **P0** | A: 加 timeout | **所有场景（万能兜底）** | 2 行代码 + 1 个配置 |
| P1 | B: Disconnect fail pending | TRANSIENT_FAILURE 场景快速恢复 | 新增接口+方法 |
| P1 | C: READY/IDLE 重试上限 | READY/IDLE 循环快速终止 | 1 个计数器 + 判断 |

**最小可行修复**：只做方案 A，改 2 行代码，覆盖所有场景。
**完整修复**：A + B + C 组合，多层防护。

---

## 七、验证修复生效

### 7.1 部署后验证

```bash
# 等待 5 分钟后检查（timeout 默认 300s）

# 1. 确认超时机制生效 — 看到这条日志说明超时触发了
grep "Timed out while waiting for" /tmp/ray/session_latest/logs/worker-*.out
# 预期输出: "Timed out while waiting for Raylet <IP> to become available."

# 2. 确认 task 被重新调度 — 看到这条日志说明 callback 被调用了
grep "Retrying attempt to schedule lease" /tmp/ray/session_latest/logs/worker-*.out
# 预期: 能看到重试日志

# 3. 确认不再有持续卡死的 active lease
watch -n 10 'grep "RequestWorkerLease.*active" /tmp/ray/session_latest/logs/raylet.out | tail -1'
# 预期: active 数在超时后降为 0

# 4. Ray Data 作业进度恢复
grep "Running:" /tmp/ray/session_latest/logs/job-driver-*.log | tail -3
# 预期: 进度不再卡住
```

### 7.2 验证 timeout 配置生效

```bash
# 确认环境变量被正确读取
grep "worker_lease_timeout_ms" /tmp/ray/session_latest/logs/raylet.out
# 或通过 Ray 内部 API:
python -c "import ray; ray.init(); print(ray._private.ray_config.instance().worker_lease_timeout_ms())"
```

### 7.3 回归测试要点

修复后需要确认不会影响正常场景：
1. **正常 spillback 不受影响**：spillback 到活着的节点，lease 应在毫秒-秒级完成，远小于 5 分钟超时
2. **短暂网络抖动不误判**：网络抖动通常几秒恢复，5 分钟超时足够容忍
3. **大规模集群调度延迟**：即使调度很慢，5 分钟内完成 lease 是合理预期

---

## 八、与原文档的勘误

原文档 `ray-task-event-eviction-and-lease-stuck-analysis.md` 中对问题二的分析**整体正确**，以下是需要修正的细节：

| 原文描述 | 修正 |
|----------|------|
| "闭包持有 shared_ptr 阻止析构" | 不够准确。Timer 回调用 `weak_ptr` 不阻止析构。`executor_` 中捕获的也是 `weak_ptr<RetryableGrpcClient>`。真正阻止析构的是 `client_map_` 中的 `shared_ptr`（READY/IDLE 场景下 `Disconnect` 不会被调用，所以 map 中的引用永远存在）。 |
| 场景 B "READY/IDLE → 无限重发 → attempt_number_ 被重置，callback 永远不触发" | 正确，但需补充：READY/IDLE 分支不调用 `SetupCheckTimer()`，timer 暂停。是后续 `Retry()` 检测到 `server_unavailable_timeout_time_ == nullopt` 后重新启动 timer 的。 |
| 场景 C "callback 成功 Disconnect → 只移除 pool 引用，不 fail pending requests" | 需要区分：如果 Disconnect 后 `RayletClient` 引用计数归零能析构，析构函数**会** fail pending。但在 READY/IDLE 场景下 Disconnect 根本不会被调用，所以这个析构清理机制也是无效的。 |

---

## 九、实际实现：Timeout 仅对 Remote 请求生效 + 诊断日志增强

> 本章记录最终落地的修复实现，对应 commit: `fix: apply RequestWorkerLease timeout only to spillback requests and enhance diagnostics`

### 9.1 设计决策：为什么 timeout 只对 remote（spillback）请求生效

原始方案 A 对**所有** `RequestWorkerLease` 请求统一加 timeout。但分析发现这会引入误杀问题：

| 请求类型 | 目标 | 超时后果 | 是否应加 timeout |
|---|---|---|---|
| Local lease | 本地 raylet | 进入 `else` 分支 → Worker `QuickExit()` 或 Driver fail 所有任务 | **不应该** |
| Remote lease (spillback) | 远程 raylet | 进入 remote failed 分支 → 回退本地重新调度 | **应该** |

Local raylet 可能因为过载导致处理慢（但没死），统一加 timeout 会把"慢"误判为"死"，导致 worker 被误杀。而 local raylet 真死的情况下，gRPC channel 会走 `UNAVAILABLE` → `RetryableGrpcClient` 的 `server_unavailable_timeout_callback` 独立处理，不需要依赖 method timeout。

**实现**：利用已有的 `grant_or_reject` 参数（由 `NormalTaskSubmitter` 设置为 `is_spillback`，`normal_task_submitter.cc:330`），在 `RayletClient::RequestWorkerLease` 中条件化 timeout：

```cpp
// src/ray/raylet_rpc_client/raylet_client.cc:70-73
INVOKE_RETRYABLE_RPC_CALL(retryable_grpc_client_,
                          NodeManagerService,
                          RequestWorkerLease,
                          request, callback, grpc_client_,
                          /*method_timeout_ms*/
                          grant_or_reject
                              ? RayConfig::instance().worker_lease_timeout_ms()
                              : -1);
```

- `grant_or_reject == true`（spillback）→ `worker_lease_timeout_ms`（默认 600s）
- `grant_or_reject == false`（local）→ `-1`（无超时）

### 9.2 `method_timeout_ms` 的两层含义

`method_timeout_ms` 在 RPC 调用链中同时控制两个超时层：

#### 层级 1: gRPC Deadline（`client_call.h:81-84`）

```cpp
if (timeout_ms != -1) {
    auto deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(timeout_ms);
    context_.set_deadline(deadline);
}
```

单次 gRPC 调用的超时。如果 server 在 deadline 之前没回复，gRPC 返回 `DEADLINE_EXCEEDED`。

#### 层级 2: RetryableGrpcClient pending queue timeout（`retryable_grpc_client.cc:163-165`）

```cpp
const auto timeout = request->GetTimeoutMs() == -1
                         ? absl::InfiniteFuture()
                         : now + absl::Milliseconds(request->GetTimeoutMs());
pending_requests_.emplace(timeout, std::move(request));
```

当 gRPC 因网络瞬时错误（`UNAVAILABLE`/`UNKNOWN`）失败后，请求进入 pending 重试队列。`timeout_ms` 决定排队等待的最大时间。`CheckChannelStatus` 定期检查，超时的请求被 Fail：

```cpp
iter->second->Fail(ray::Status::TimedOut(
    "Timed out while waiting for <server_name> to become available."));
```

#### 两层的关系

`worker_lease_timeout_ms` 同时作用于两层：
- gRPC deadline: 请求发出后 600s 内 server 必须回复
- Pending queue: 进入重试队列后 600s 内必须恢复连接并成功发送

**无论哪层触发超时，最终效果相同**：`NormalTaskSubmitter` 的 callback 被调用（带 error status），task 回退到本地重新调度。

### 9.3 超时后的重试机制详解

#### gRPC DEADLINE_EXCEEDED 不会被 RetryableGrpcClient 内部重试

```cpp
// retryable_grpc_client.h:287
if (status.ok() || !IsGrpcRetryableStatus(status) || !retryable_grpc_client) {
    callback(status, std::move(reply));  // 直接回调调用者
    return;
}
retryable_grpc_client->Retry(retryable_grpc_request);  // 只有 UNAVAILABLE/UNKNOWN 才重试
```

`IsGrpcRetryableStatus` 只匹配 `UNAVAILABLE` 和 `UNKNOWN`。`DEADLINE_EXCEEDED` 不在其中，所以 deadline 超时后 callback **直接被调用**（不进入重试队列）。

#### Pending queue 超时直接 Fail

排队超时是 `Retry` 内部的机制本身——请求已在队列中，超时后 `Fail` 被调用，callback 以 `TimedOut` 状态回调。

#### NormalTaskSubmitter 层面的重试

无论哪种超时触发 callback 失败，`normal_task_submitter.cc:446-472`：

```cpp
} else if (NodeID::FromBinary(raylet_address.node_id()) != local_node_id_) {
    // remote lease failed → retry locally
    sched_entry.spillback_retry_count++;
    // ... logging ...
    RequestNewWorkerIfNeeded(scheduling_key);  // 回退到 local raylet
}
```

**完整恢复流程**：
```
RequestWorkerLease(remote, timeout=600s)
  ├─ gRPC 成功 → callback(ok) → 正常处理
  ├─ UNAVAILABLE/UNKNOWN → RetryableGrpcClient.Retry()
  │   ├─ 排队等待网络恢复，channel READY 时重发
  │   └─ 排队超过 600s → Fail(TimedOut "waiting for...") → callback
  └─ DEADLINE_EXCEEDED → 直接 callback(DeadlineExceeded)
       ↓
NormalTaskSubmitter: status 不是 ok 且是 remote 节点
  → spillback_retry_count++
  → RequestNewWorkerIfNeeded(local) → 回退到本地重新调度
  → local raylet 可能又 spillback → 再超时 → 再回来 → 循环
```

**当前实现仍然是无限重试**（`spillback_retry_count` 仅诊断用途，不参与决策）。原代码的 TODO 已保留：
```cpp
// TODO(swang): Fail after some number of retries?
```

### 9.4 Callback 状态分类完整描述

`RequestWorkerLease` 的 callback 基于 `status.ok()` 顶层分支，共有 6 种处理路径：

```
                        callback(status, reply)
                               │
                    ┌──── status.ok? ────┐
                    │YES                 │NO
                    ▼                    ▼
            ┌─ reply 分支 ─┐      ┌─ 目标节点? ─┐
            │              │      │             │
     canceled  rejected  granted  redirect   remote    local
       (1)      (2)       (3)      (4)        (5)       (6)
        │         │        │        │          │         │
     fail/     retry     终态✓   spillback   retry    exit/fail
     retry     locally            to remote  locally   终态✗
```

#### status.ok() = true（RPC 成功，解析 reply）

| # | 条件 | 语义 | 处理 | spillback_retry_count |
|---|---|---|---|---|
| 1 | `reply.canceled()` | 调度被取消 | 见 9.5 详解 | 不变 |
| 2 | `reply.rejected()` | Remote raylet 资源不足拒绝 | `RequestNewWorkerIfNeeded(local)` | **++** |
| 3 | `!reply.worker_address().node_id().empty()` | Grant 成功 | `AddWorkerLeaseClient` + `OnWorkerIdle` | **= 0** |
| 4 | else | Redirect (spillback 指令) | `RequestNewWorkerIfNeeded(remote)` | 不变 |

#### status.ok() = false（RPC 失败）

| # | 条件 | 语义 | 处理 | spillback_retry_count |
|---|---|---|---|---|
| 5 | 目标是 remote 节点 | Remote lease failed | `RequestNewWorkerIfNeeded(local)` | **++** |
| 6 | 目标是 local 节点 | Local raylet 挂了 | Worker: `QuickExit()` / Driver: fail all tasks | 不变 |

#### rejected 也递增 spillback_retry_count 的原因

`rejected`（路径 2）和 `remote RPC failed`（路径 5）对 caller 的效果完全相同：spillback 出去 → 没拿到 worker → 回退到 local。区别仅在于失败层次：

| | rejected | RPC failed |
|---|---|---|
| 请求到达 server？ | ✅ 到达并处理 | ❌ 或超时 |
| 失败原因 | local raylet 资源视图过期，remote 无资源 | 网络/节点死亡/超时 |
| 后续动作 | 相同：回退 local 重新调度 | 相同 |

计入同一计数器的理由：
1. **诊断一致性**：`spillback_retry_count` 语义是"spillback 未成功的总次数"，无论原因
2. **识别抖动**：资源视图持续过期会导致反复 spillback → rejected 循环
3. **未来限流基础**：若加 `if (retry_count > max) { fail }` 逻辑，rejected 也应计入

### 9.5 `reply.canceled()` 详解：产生条件与终态原因

#### 产生条件

`canceled` 由 **raylet 端** 设置。发生在 lease 请求已在 raylet 调度队列中排队等待，但 worker 分配之前被主动取消：

| failure_type | 触发条件 | Server 端来源 |
|---|---|---|
| `SCHEDULING_CANCELLED_PLACEMENT_GROUP_REMOVED` | PG 被用户删除（`remove_placement_group()`） | `ClusterLeaseManager::CancelLeases` |
| `SCHEDULING_CANCELLED_RUNTIME_ENV_SETUP_FAILED` | Worker 启动时 runtime_env 安装失败 | `LocalLeaseManager` worker pop 时 |
| `SCHEDULING_CANCELLED_UNSCHEDULABLE` | 资源需求在集群中永久不可满足，或 lease granting 时变为 infeasible | `ClusterLeaseManager` + `LocalLeaseManager:424` |
| `SCHEDULING_CANCELLED_INTENDED` | 调用者已死（caller worker 不存在） | `node_manager.cc:1809` |

#### 产生流程

```
Worker/Driver                     Raylet
    │                               │
    ├── RequestWorkerLease ────────►│
    │                               ├── 放入调度队列 (leases_to_schedule_ / waiting_lease_queue_)
    │                               │      ...排队等待...
    │                               │
    │   (外部事件触发: PG 删除 / env 失败 / 资源不可调度 / caller 死亡)
    │                               │
    │                               ├── CancelLeases(predicate, failure_type, message)
    │                               │   ├── reply->set_canceled(true)
    │                               │   ├── reply->set_failure_type(...)
    │                               │   └── send_reply_callback(Status::OK, ...)
    │◄── reply(canceled=true) ──────│
```

#### 为什么是终态（三个 failure_type）

`RUNTIME_ENV_SETUP_FAILED`、`PLACEMENT_GROUP_REMOVED`、`UNSCHEDULABLE` 被当作终态的原因是**重试没有意义，条件不会自行恢复**：

| failure_type | 为什么不可恢复 |
|---|---|
| `PLACEMENT_GROUP_REMOVED` | PG 已被用户显式删除，其调度约束永远无法满足 |
| `RUNTIME_ENV_SETUP_FAILED` | 依赖安装失败（如 pip 版本冲突），相同 env spec 重试大概率还是失败。代码注释："makes an implicit assumption that runtime_env failures are not transient" |
| `UNSCHEDULABLE` | 任务请求的资源形状在整个集群中不存在（如请求 8 GPU 但所有节点最多 4 GPU），结构性不可调度 |

终态处理方式：
```cpp
tasks_to_fail = std::move(sched_entry.task_queue);  // 取出所有排队任务
sched_entry.task_queue.clear();
// 后续对每个 task 标记失败 → 用户代码收到 RayTaskError
```

#### 非终态的 cancel

`SCHEDULING_CANCELLED_INTENDED`（caller 死亡）和 `SCHEDULING_FAILED`（调度失败但可重试）走 `else` 分支 → `RequestNewWorkerIfNeeded`，允许重试。

### 9.6 配置说明

`worker_lease_timeout_ms` 支持三种设置方式：

```bash
# 方式 1: 环境变量（推荐生产使用，每个进程启动时读取一次）
export RAY_worker_lease_timeout_ms=600000

# 方式 2: ray.init() system_config（通过 GCS 广播到集群所有节点）
ray.init(_system_config={"worker_lease_timeout_ms": 600000})

# 方式 3: Ray cluster yaml
ray start --head --system-config='{"worker_lease_timeout_ms":600000}'
```

| 值 | 效果 |
|---|---|
| `600000`（默认） | 10 分钟超时，平衡检测速度和误判率 |
| `300000` | 5 分钟，更快检测 dead node，但对慢节点容忍度低 |
| `-1` | 禁用超时（恢复原始行为，仅用于排障） |

**注意**：改动后此配置仅影响 remote（spillback）请求。Local lease 请求硬编码为 -1（无超时），不受此配置影响。

### 9.7 修改文件清单

| 文件 | 改动 | 关键行 |
|------|------|--------|
| `src/ray/raylet_rpc_client/raylet_client.cc` | timeout 条件化：spillback → 600s，local → -1 | 70-73 |
| `src/ray/common/ray_config_def.h` | 默认值 600000，注释明确仅对 spillback 生效 | 1031-1038 |
| `src/ray/core_worker/task_submission/normal_task_submitter.h` | 添加 `spillback_retry_count` 字段 | 322-325 |
| `src/ray/core_worker/task_submission/normal_task_submitter.cc` | 增强日志、区分 failure_reason、递增/重置 retry count | 398-479 |

### 9.8 诊断日志示例

#### Remote lease 超时（pending queue timeout）
```
[WARNING] Remote lease failed (id: abc123 name: task_func) target_node_id: def456
 target_ip: 10.0.1.5. Retrying on local node. reason: connection_failed
 spillback_retry_count: 3 timeout_config_ms: 600000
 error: Timed out while waiting for Raylet 10.0.1.5 to become available.
```

#### Remote lease 超时（gRPC deadline）
```
[WARNING] Remote lease failed (id: abc123 name: task_func) target_node_id: def456
 target_ip: 10.0.1.5. Retrying on local node. reason: grpc_deadline_exceeded
 spillback_retry_count: 5 timeout_config_ms: 600000
 error: RPC Error: Deadline Exceeded
```

#### Remote lease rejected
```
[INFO] Lease rejected (id: abc123 name: task_func) target_node_id: def456
 target_ip: 10.0.1.5. spillback_retry_count: 2. Retrying on local node.
```

### 9.9 与方案 A 的区别

原方案 A（第六章 6.1 节）对所有请求统一加 timeout。实际实现的改进：

| | 方案 A（原设计） | 实际实现 |
|---|---|---|
| Local lease timeout | 300s（可能误杀 worker） | -1（永不超时，安全） |
| Remote lease timeout | 300s | 600s（更保守，减少误判） |
| 诊断能力 | 无 | `spillback_retry_count` + failure_reason 分类 |
| rejected 处理 | 无日志 | INFO 级别限流日志 + retry count |

### 9.10 调度协议不变量：两跳模型与 RAY_CHECK 断言

#### 两跳调度模型

Ray 的 `RequestWorkerLease` 调度严格遵循**两跳模型**：

```
Worker ──(1)──► Local Raylet ──(redirect)──► Remote Raylet
                     ▲                            │
                     └────────(rejected/fail)──────┘
```

协议规则：
- **Local raylet**（`is_spillback = false`）可回复：cancel、grant、**redirect**（spillback 到 remote）
- **Remote raylet**（`is_spillback = true`）可回复：cancel、grant、**reject**（回退 local）
- **Remote raylet 不允许再 redirect 到另一个 remote raylet**

#### 为什么禁止级联 redirect

如果允许 remote raylet 再 redirect（A → B → C → D...），会导致：

1. **无限循环风险**：A redirect → B redirect → A redirect → B...
2. **故障恢复困难**：B redirect 到 C，C 挂了，无人负责重试（Worker 已丢失 local raylet 上下文）
3. **资源视图发散**：每一跳使用不同节点的资源视图，越远越过期，调度质量急剧下降

#### 两个互为镜像的 RAY_CHECK 断言

```cpp
// normal_task_submitter.cc — redirect 分支
} else {
  // The raylet redirected us to a different raylet to retry at.
  RAY_CHECK(!is_spillback);  // 断言：redirect 一定来自 local raylet
  RequestNewWorkerIfNeeded(scheduling_key, &reply.retry_at_raylet_address());
}

// normal_task_submitter.cc — rejected 分支
} else if (reply.rejected()) {
  RAY_CHECK(is_spillback);  // 断言：reject 一定来自 remote raylet
  RequestNewWorkerIfNeeded(scheduling_key);
}
```

| 断言 | 保证 | 违反含义 |
|---|---|---|
| `RAY_CHECK(!is_spillback)` in redirect | Redirect 只能从 local raylet 发出 | Remote raylet 发了 redirect → 协议 bug，crash 暴露 |
| `RAY_CHECK(is_spillback)` in rejected | Reject 只能从 remote raylet 发出 | Local raylet 发了 reject → 协议 bug，crash 暴露 |

#### 设计意图

- Local raylet 是调度决策者：它有全局资源视图（通过 GCS 广播），决定任务该去哪个节点
- Remote raylet 是执行者：要么接受（grant），要么拒绝（reject）回退给决策者
- 决策权不下放：remote 不做二次路由决策，避免分布式路由的复杂性

这确保了任何 lease 请求最多经过 **local → remote → 回退 local** 一个完整循环，不会在多个 remote 之间漂移，使得失败恢复路径清晰可控。

### 9.11 `grant_or_reject` 参数详解

#### Proto 定义

```protobuf
// node_manager.proto:48-51
// If it's true, either grant the lease if the task is
// locally schedulable or reject the request.
// Else, the raylet may return another raylet at which to retry the request.
bool grant_or_reject = 3;
```

#### 语义

`grant_or_reject` 告诉目标 raylet：**你只能二选一——要么 grant（分配 worker），要么 reject（拒绝）。不允许再 redirect 到其他节点。**

| grant_or_reject | 目标 raylet 可回复 | 不可回复 | 设置场景 |
|---|---|---|---|
| `false` | grant、redirect（spillback 到另一个节点） | reject | Worker → **local** raylet |
| `true` | grant、**reject** | redirect | Worker → **remote** raylet（spillback 目标） |

#### Server 端行为

当 raylet 尝试将 lease spillback 到其他节点时，会检查 `grant_or_reject_`：

```cpp
// cluster_lease_manager.cc:429-434
if (work->grant_or_reject_) {
    // grant_or_reject=true → 不允许再 spillback，直接 reject
    reply->set_rejected(true);
    send_reply_callback_(Status::OK(), ...);
    return;
}
// grant_or_reject=false → 执行正常 spillback（redirect 到另一个节点）

// local_lease_manager.cc:676-681（同样逻辑）
if (work->grant_or_reject_) {
    reply->set_rejected(true);
    send_reply_callback_(Status::OK(), ...);
    return;
}
```

#### Client 端设置

```cpp
// normal_task_submitter.cc:330
raylet_client->RequestWorkerLease(
    lease_spec,
    /*grant_or_reject=*/is_spillback,  // spillback 请求设为 true
    ...);
```

`is_spillback = true` → `grant_or_reject = true`，含义链：
1. "你是我 spillback 到的远程节点"
2. "你要么给我 worker（grant），要么直接说不行（reject）"
3. "不要再把我踢到第三个节点去（不允许 redirect）"

#### 与两跳模型的关系

`grant_or_reject` 是两跳模型的**协议级实现机制**：

```
                    grant_or_reject=false          grant_or_reject=true
Worker ───────────────────► Local Raylet ──────────────► Remote Raylet
                            可以: grant/redirect         可以: grant/reject
                            不能: reject                 不能: redirect
```

- Client 端（`NormalTaskSubmitter`）通过设置 `grant_or_reject=is_spillback` 发起约束
- Server 端（`ClusterLeaseManager`/`LocalLeaseManager`）执行约束：检查 `grant_or_reject_` 决定是否允许 spillback
- 断言端（`NormalTaskSubmitter` callback）通过 `RAY_CHECK` 验证协议未被违反

三者配合，从发起、执行、验证三个环节确保两跳模型的不变量。

---

## 十、附录：相关代码文件索引

| 功能 | 文件 | 关键行 |
|------|------|--------|
| Lease 请求入口 | `src/ray/core_worker/task_submission/normal_task_submitter.cc` | 274-470 |
| Lease RPC 发送（条件化超时） | `src/ray/raylet_rpc_client/raylet_client.cc` | 53-73 |
| INVOKE_RETRYABLE_RPC_CALL 宏 | `src/ray/rpc/retryable_grpc_client.h` | 37-50 |
| CallMethod（创建 RetryableGrpcRequest） | `src/ray/rpc/retryable_grpc_client.h` | 240-257 |
| RetryableGrpcRequest::Create（executor + failure_callback） | `src/ray/rpc/retryable_grpc_client.h` | 259-314 |
| IsGrpcRetryableStatus | `src/ray/common/grpc_util.h` | 130-133 |
| Retry（InfiniteFuture 入队） | `src/ray/rpc/retryable_grpc_client.cc` | 131-173 |
| CheckChannelStatus（超时清理 + channel 状态判断） | `src/ray/rpc/retryable_grpc_client.cc` | 52-129 |
| SetupCheckTimer（weak_ptr） | `src/ray/rpc/retryable_grpc_client.cc` | 40-50 |
| ~RetryableGrpcClient（析构时 fail pending） | `src/ray/rpc/retryable_grpc_client.cc` | 23-38 |
| GetDefaultUnavailableTimeoutCallback（GCS 查询+Disconnect） | `src/ray/raylet_rpc_client/raylet_client_pool.cc` | 24-79 |
| Disconnect（只 erase 不 fail） | `src/ray/raylet_rpc_client/raylet_client_pool.cc` | 100-107 |
| RayletClient 构造（创建 RetryableGrpcClient） | `src/ray/raylet_rpc_client/raylet_client.cc` | 32-51 |
| Spillback 决策（不验证存活） | `src/ray/raylet/scheduling/cluster_lease_manager.cc` | 422-461 |
| worker_lease_timeout_ms 配置（仅 spillback，默认 600s） | `src/ray/common/ray_config_def.h` | 1031-1038 |
| spillback_retry_count 诊断计数器 | `src/ray/core_worker/task_submission/normal_task_submitter.h` | 322-325 |
| grpc_max_ready_idle_resend_count 配置 | `src/ray/common/ray_config_def.h` | 1039-1045 |
| consecutive_ready_idle_resend_count_ 成员 | `src/ray/rpc/retryable_grpc_client.h` | 235-238 |
| READY/IDLE 重试上限逻辑 | `src/ray/rpc/retryable_grpc_client.cc` | 123-170 |

---

## 十一、gRPC Status Code 深入分析：DEADLINE_EXCEEDED / UNAUTHENTICATED / ABORTED

### 11.1 `GrpcStatusToRayStatus` 的三种非通用分支

`src/ray/common/grpc_util.h:106-125`：

```cpp
inline Status GrpcStatusToRayStatus(const grpc::Status &s) {
  if (s.ok()) return Status::OK();

  if (s.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED) {
    return {StatusCode::TimedOut, GrpcStatusToRayStatusMessage(s)};
  }
  if (s.error_code() == grpc::StatusCode::UNAUTHENTICATED) {
    return Status::Unauthenticated(GrpcStatusToRayStatusMessage(s));
  }
  if (s.error_code() == grpc::StatusCode::ABORTED) {
    return {Status::StringToCode(s.error_message()), s.error_details()};
  }
  return Status::RpcError(GrpcStatusToRayStatusMessage(s), s.error_code());
}
```

### 11.2 三种 Code 的本质区别

| gRPC Code | 产生者 | 在 Ray 中的语义 | 转换后的 Ray Status |
|---|---|---|---|
| `DEADLINE_EXCEEDED` | **gRPC 库自身**（客户端侧 timer） | 请求超时（设了 deadline 但 server 没在时限内回复） | `StatusCode::TimedOut` |
| `UNAUTHENTICATED` | **Ray server 端**显式返回 | 认证失败（cluster ID 不匹配） | `Status::Unauthenticated(...)` |
| `ABORTED` | **Ray server 端**显式返回 | Ray 内部业务错误的通用传输通道 | 从 `error_message` 解析出原始 Ray StatusCode |

#### DEADLINE_EXCEEDED

- **产生者**：gRPC 库。当 `context.set_deadline()` 被设置后，server 在 deadline 之前没回复，gRPC 框架在客户端侧生成此错误
- **不是 server 端产出**的，是客户端本地 timer 超时
- **前提条件**：必须设了 deadline（`timeout_ms != -1`），否则永远不会产生

#### UNAUTHENTICATED

- **产生者**：Ray server 端代码显式返回（见 `RayStatusToGrpcStatus`）
- **场景**：cluster ID 校验失败，说明连错集群了
- **是应用层逻辑错误**，不是网络问题

#### ABORTED

- **产生者**：Ray server 端代码显式返回
- **设计意图**：注释 "Unlike `UNKNOWN`, `ABORTED` is never generated by the library, so using it means more robust"
- **是 Ray 的通用业务错误传输通道**：Ray 把自己的 StatusCode 序列化到 `error_message`，原始 message 放到 `error_details`

### 11.3 节点断开时的实际 gRPC 返回

**节点断开时 gRPC 返回的是 `UNAVAILABLE`，不是 `DEADLINE_EXCEEDED`。**

| 场景 | gRPC 返回 | 原因 |
|---|---|---|
| TCP 连接被 RST / 拒绝 | `UNAVAILABLE` | gRPC 检测到传输层失败 |
| DNS 解析失败 | `UNAVAILABLE` | 无法建立连接 |
| Server 进程崩溃 | `UNAVAILABLE` | 连接中断 |
| 设了 deadline 但 server 没回复 | `DEADLINE_EXCEEDED` | 客户端本地超时 |
| 没设 deadline（`timeout_ms = -1`） | **永远不会** `DEADLINE_EXCEEDED` | 没有超时机制 |

### 11.4 `IsGrpcRetryableStatus` 决定的后续处理差异

```cpp
inline bool IsGrpcRetryableStatus(Status status) {
    return status.IsRpcError() && (status.rpc_code() == grpc::StatusCode::UNAVAILABLE ||
                                   status.rpc_code() == grpc::StatusCode::UNKNOWN);
}
```

| gRPC Code | 被 RetryableGrpcClient 内部重试？ | Callback 被调用？ | 后续 |
|---|---|---|---|
| `UNAVAILABLE` | **是**（进 Retry 队列） | **否**（被"吞掉"） | 等待恢复或超时 |
| `UNKNOWN` | **是**（进 Retry 队列） | **否** | 同上 |
| `DEADLINE_EXCEEDED` | **否**（TimedOut 非 RpcError） | **是**（直接回调） | NormalTaskSubmitter 收到 → 重调度 |
| `UNAUTHENTICATED` | **否** | **是** | 上层处理认证错误 |
| `ABORTED` | **否** | **是** | 解码为原始 Ray error |

**关键结论**：`DEADLINE_EXCEEDED` 绕过了 RetryableGrpcClient 的重试机制，让 callback 立即被调用。这是 `worker_lease_timeout_ms` 设 gRPC deadline 的价值之一。

---

## 十二、两条超时路径的深入分析

### 12.1 `worker_lease_timeout_ms` 同时作用于两层

`method_timeout_ms` 在 RPC 调用链中同时控制两个独立的超时层：

#### 层级 1: gRPC Deadline（单次 RPC 超时）

```cpp
// client_call.h:81-84
if (timeout_ms != -1) {
    auto deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(timeout_ms);
    context_.set_deadline(deadline);
}
```

- 每次 `CallMethod()` 发出的 gRPC 请求都带这个 deadline
- Server 在 deadline 前没回复 → gRPC 库返回 `DEADLINE_EXCEEDED`
- 转换为 `StatusCode::TimedOut` → 不走 Retry → callback 直接被调用

#### 层级 2: Pending Queue Timeout（排队等待超时）

```cpp
// retryable_grpc_client.cc Retry():
const auto method_timeout_ms = request->GetTimeoutMs();
const auto timeout = method_timeout_ms == -1
                         ? absl::InfiniteFuture()
                         : now + absl::Milliseconds(method_timeout_ms);
pending_requests_.emplace(timeout, std::move(request));
```

- 当 RPC 因 UNAVAILABLE/UNKNOWN 失败后进入重试队列
- `timeout` 是队列中的过期时间
- `CheckChannelStatus` 每秒扫描，超时的被 Fail

### 12.2 两条路径的触发条件

```
CallMethod() 发出 gRPC 请求（deadline = now + 600s）
     │
     ├─── 路径 1: gRPC Deadline ─────────────────────────────────┐
     │  Server 接受了连接但一直不回复（TCP 活着，应用层卡住）      │
     │  600s 后 gRPC 库在客户端侧触发                            │
     │  → DEADLINE_EXCEEDED                                      │
     │  → IsGrpcRetryableStatus = false                          │
     │  → callback 直接被调用 ✓                                  │
     │                                                            │
     ├─── 路径 2: 快速 UNAVAILABLE → 排队超时 ───────────────────┤
     │  Server 连接被拒/RST（毫秒级返回 UNAVAILABLE）             │
     │  → IsGrpcRetryableStatus = true                           │
     │  → Retry() → 进入 pending_requests_ 队列                  │
     │     timeout = now + 600s                                   │
     │  → CheckChannelStatus 每秒扫描                             │
     │  → 600s 后请求过期 → Fail(TimedOut) → callback ✓          │
     │                                                            │
     └─── 路径 3: READY/IDLE 循环（timeout 被反复重置）───────────┘
        Channel READY → 取出重发 → UNAVAILABLE → 再入队(timeout重置)
        → 排队超时永远不触发 → 需要 resend count limit 兜底
```

### 12.3 READY/IDLE 循环中 timeout 被重置的机制

```
T=0s:    第一次 RPC → UNAVAILABLE → Retry()
         timeout = 0 + 600s = 600s

T=1s:    CheckChannelStatus → channel READY → 取出重发

T=1.01s: UNAVAILABLE → Retry()
         timeout = 1.01 + 600s = 601.01s  ← 重新计算!

T=2s:    CheckChannelStatus → channel READY → 取出重发

T=2.01s: UNAVAILABLE → Retry()
         timeout = 2.01 + 600s = 602.01s  ← 又重置!

... timeout 永远追不上 now ...
```

**每次从队列取出重发、再入队列，`timeout` 按 `now + 600s` 重新计算。** READY/IDLE 循环中排队超时永远不会触发。

### 12.4 三层防护的覆盖矩阵

| 场景 | gRPC Deadline (路径1) | Pending Queue 超时 (路径2) | READY/IDLE Resend Limit (路径3) |
|---|---|---|---|
| Server 卡死不回复 (TCP 活) | **生效** (600s) | 不进队列 | 不适用 |
| TRANSIENT_FAILURE (TCP 断开) | 不适用(快速返回) | **生效** (600s) | 不适用 |
| READY/IDLE + 快速 UNAVAILABLE | 不生效(快速返回) | 不生效(被重置) | **生效** (~10s) |

---

## 十三、根因重新分析：为什么 `timeout_ms = -1` 不是真正的卡死原因

### 13.1 TRANSIENT_FAILURE 路径能自愈

即使 `timeout_ms = -1`（排队超时不存在），TRANSIENT_FAILURE 路径仍有独立的恢复机制：

```
Timer (每 1s) → CheckChannelStatus → TRANSIENT_FAILURE
  → server_unavailable_timeout_time_ 到达
  → server_unavailable_timeout_callback_()
  → GCS 查询节点状态
  → 确认死亡 → Disconnect(node_id)
  → client_map_.erase → 引用计数归零 → 析构
  → ~RetryableGrpcClient → Fail(Disconnected) → callback 被调用 → task 恢复
```

**这条路径与 `timeout_ms` 完全无关**——它通过 GCS 确认 + 对象析构来恢复。

### 13.2 真正导致永久卡死的根因

**READY/IDLE 分支的三个设计缺陷**：

```cpp
case GRPC_CHANNEL_READY:
case GRPC_CHANNEL_IDLE: {
    server_unavailable_timeout_time_ = std::nullopt;  // ← 缺陷1: callback 永远不被触发
    // 重发...
    attempt_number_ = 0;                              // ← 缺陷2: 退避进度被重置
    break;
    // ← 缺陷3: 不调 SetupCheckTimer, 但 Retry 会重启
}
```

| 缺陷 | 后果 |
|---|---|
| 清空 `server_unavailable_timeout_time_` | `server_unavailable_timeout_callback_` 永远不被触发 → GCS 永远不被查询 → Disconnect 永远不被调用 |
| 重置 `attempt_number_` | 即使偶尔闪到 TRANSIENT_FAILURE 也从头退避 |
| 循环重发 + timeout 重置 | 排队超时永远不触发（即使设了 600s） |

### 13.3 `worker_lease_timeout_ms` 实际解决了什么

| 场景 | 是否被 worker_lease_timeout_ms 解决 |
|---|---|
| TRANSIENT_FAILURE + GCS 正常 | 不需要它（GCS→Disconnect 自愈） |
| TRANSIENT_FAILURE + GCS 持续失败 | ✓ 解决（排队 600s 后超时兜底） |
| **READY/IDLE 循环** | ✗ **不能解决**（timeout 被重置） |

### 13.4 修复优先级矫正

| 方案 | 覆盖场景 | 实际重要性 |
|---|---|---|
| **C: READY/IDLE 重试上限** (`grpc_max_ready_idle_resend_count`) | READY/IDLE 循环（主要卡死路径） | **核心修复** |
| A: `worker_lease_timeout_ms` | TRANSIENT_FAILURE + GCS 不可用 | 防御纵深兜底 |

---

## 十四、CheckChannelStatus 完整监测与退避机制

### 14.1 Timer 驱动架构

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     Timer 驱动的周期性监测系统                             │
│                                                                          │
│  入口: Retry() 首次被调用时 SetupCheckTimer()                            │
│  周期: check_channel_status_interval_milliseconds = 1000ms (1s)         │
│  退出: pending_requests_ 为空时停止 timer                                │
└─────────────────────────────────────────────────────────────────────────┘
```

### 14.2 SetupCheckTimer 与 CheckChannelStatus 的调用链

```
Retry(request)
  │
  ├─ 入队 pending_requests_
  │
  └─ if (!server_unavailable_timeout_time_.has_value())
       │
       ├── 设置 server_unavailable_timeout_time_
       └── SetupCheckTimer()
                │
                ▼
            timer_.expires_from_now(1000ms)
            timer_.async_wait(callback)
                │
                ▼ (1 秒后，io_context 线程)
            CheckChannelStatus(true)
                │
                ├── Phase 1: 超时清理
                ├── Phase 2: channel 状态分支
                └── Phase 3: timer 续期
```

**`!server_unavailable_timeout_time_.has_value()` 的作用**：防重复启动 timer 的 guard。
- Timer 已经在跑（`has_value() == true`）→ 不重复启动
- READY/IDLE 分支清空后（`nullopt`）→ 下次 Retry 会重新启动

### 14.3 Timer 续期决策

| 场景 | 谁调 SetupCheckTimer | Timer 状态 |
|---|---|---|
| TRANSIENT_FAILURE 分支 | CheckChannelStatus 自己 | **持续运行** |
| READY/IDLE 正常重发 | 不调用 | **暂停** → 等 Retry() 重启 |
| READY/IDLE 超限 fail | 不调用 | **暂停** → 等 Retry() 重启 |
| 超时清理后队列空 | 不调用（return） | **停止** → 等 Retry() 重启 |

Timer 不会永久停止：只要有请求失败进入 Retry()，它就会被重新启动。

### 14.4 Phase 1: 超时清理（逐请求过期检查）

```cpp
const auto now = absl::Now();
while (!pending_requests_.empty()) {
    auto iter = pending_requests_.begin();  // btree_multimap 最小 key
    if (iter->first > now) break;           // 最早过期的都没到 → 后面更不会
    iter->second->Fail(TimedOut(...));
    pending_requests_.erase(iter);
}
```

- `pending_requests_` 是 `btree_multimap<absl::Time, Request>`，按过期时间升序
- `timeout_ms = -1` → key 是 `InfiniteFuture` → 永远不被清理
- `timeout_ms = 600000` → key 是 `入队时间 + 600s` → 600s 后被清理
- 每秒扫描一次

### 14.5 Phase 2A: TRANSIENT_FAILURE 的指数退避

```cpp
case GRPC_CHANNEL_TRANSIENT_FAILURE:
case GRPC_CHANNEL_CONNECTING: {
    consecutive_ready_idle_resend_count_ = 0;

    if (server_unavailable_timeout_time_ < now) {
        // 退避周期到了 → 触发 callback
        server_unavailable_timeout_callback_();

        attempt_number_++;
        server_unavailable_timeout_time_ =
            now + absl::Seconds(ExponentialBackoff::GetBackoffMs(
                attempt_number_, base_ms=1000, max_ms=60000) / 1000);
    }
    SetupCheckTimer();  // 1s 后再来
    break;
}
```

**退避公式**：`GetBackoffMs(attempt, base, max) = min(base × 2^attempt, max)`

| attempt | 下次 callback 触发间隔 | 累计时间 |
|---|---|---|
| 0 | 1s | 1s |
| 1 | 2s | 3s |
| 2 | 4s | 7s |
| 3 | 8s | 15s |
| 4 | 16s | 31s |
| 5 | 32s | 63s |
| 6+ | 60s（封顶） | 123s, 183s, ... |

**注意**：Timer 每 1s 触发一次 CheckChannelStatus，但 callback 只在退避周期到达时才触发。大部分 CheckChannelStatus 调用只是检查 `server_unavailable_timeout_time_ < now` 然后直接 `SetupCheckTimer()` 继续等。

**callback 的行为**（`raylet_client_pool.cc:28-78`）：

```
server_unavailable_timeout_callback_()
    │
    ├── 本地 GCS 缓存查节点
    │     ├── DEAD → Disconnect(node_id)
    │     ├── ALIVE → return（节点活着）
    │     └── 不在缓存 → GCS RPC 查询
    │
    └── GCS RPC 异步查询
          ├── RPC 失败 → return（下次再试）
          ├── DEAD / 不存在 → Disconnect(node_id)
          └── ALIVE → return
```

### 14.6 Phase 2B: READY/IDLE 的重试计数

```cpp
case GRPC_CHANNEL_READY:
case GRPC_CHANNEL_IDLE: {
    server_unavailable_timeout_time_ = std::nullopt;

    if (consecutive_ready_idle_resend_count_ >= max_resend_count) {
        // 超过上限 → fail 所有 pending → 终止循环
        Fail(TimedOut("Channel READY/IDLE but N consecutive resends failed..."));
        consecutive_ready_idle_resend_count_ = 0;
        break;
    }

    consecutive_ready_idle_resend_count_++;
    // 重发所有队列中的请求
    while (!pending_requests_.empty()) {
        pending_requests_.begin()->second->CallMethod();
        pending_requests_.erase(pending_requests_.begin());
    }
    attempt_number_ = 0;
    break;
    // 不调用 SetupCheckTimer() → Timer 暂停
    // 重发后如果失败 → Retry() → SetupCheckTimer() 重启
}
```

READY/IDLE 没有退避——每次 Timer 触发就立即重发。循环时间 ≈ 1s（Timer interval）。

### 14.7 gRPC Channel 状态的获取方式

```cpp
auto status = channel_->GetState(false);
//                              ^^^^^ try_to_connect = false
```

- `channel_` 是 `std::shared_ptr<grpc::Channel>`，在 `RayletClient` 构建时创建
- `GetState(false)` 读取 gRPC 内部维护的状态缓存，**不主动触发新连接**
- gRPC 库内部通过 TCP keepalive、HTTP/2 PING、连接失败等事件自动维护 channel 状态
- 状态转换由 gRPC 库异步完成，Ray 只是每秒读取一次

gRPC 内部 channel 状态机：
```
IDLE ──(RPC调用触发连接)──► CONNECTING ──(TCP成功+HTTP2 SETTINGS)──► READY
  ▲                              │                                     │
  │                         (TCP失败)                           (连接断开/RST)
  │                              ▼                                     ▼
  └──(idle timeout)────── TRANSIENT_FAILURE ◄───────────────────────────┘
                                │
                       (gRPC 内部 backoff 重试)
                                ▼
                           CONNECTING → (成功 → READY, 失败 → TRANSIENT_FAILURE)
```

gRPC 内部的 reconnection backoff（独立于 Ray 的退避）：
- 初始 backoff: 1s
- 最大 backoff: 120s
- Multiplier: 1.6
- Jitter: ±20%

这只影响 TCP 连接重建尝试的频率，不影响 Ray 的请求重试逻辑。

### 14.8 两种退避对比

| | TRANSIENT_FAILURE 退避 | READY/IDLE 计数 |
|---|---|---|
| **机制** | 指数退避触发 callback | 固定频率重发 + 计数上限 |
| **目的** | 给 GCS 查询/网络恢复留时间 | 快速检测 IP 复用 |
| **callback/fail 频率** | 1s→2s→4s→...→60s | 固定 ~1s/次 |
| **请求状态** | 安静排队等待，不重发 | 每次取出重发 |
| **终止方式** | Disconnect→析构→fail 或排队超时 | 计数器到 10 → fail |
| **计数器** | `attempt_number_`（退避级别） | `consecutive_ready_idle_resend_count_`（重发次数） |
| **重置条件** | channel 变 READY → 归零 | channel 变 TRANSIENT_FAILURE → 归零 |

---

## 十五、两层重试的关系：传输层 vs 应用层

### 15.1 `consecutive_ready_idle_resend_count_` vs `spillback_retry_count`

```
┌─────────────────────────────────────────────────────────────────────┐
│  NormalTaskSubmitter (应用层)                                         │
│  spillback_retry_count: 跨多次完整 RPC 的重试次数（仅诊断）          │
│                                                                      │
│  每次 callback 被调用（无论什么原因失败）→ count++                    │
│  → RequestNewWorkerIfNeeded(local) → local raylet 可能又 spillback   │
│                                                                      │
├──────────────────────────────────────────────────────────────────────┤
│  RetryableGrpcClient (传输层)                                        │
│  consecutive_ready_idle_resend_count_: 单次 RPC 内的重发次数         │
│                                                                      │
│  对上层不可见，是"一次 RPC 尝试"内部的实现细节                        │
│  触发上限 → Fail → callback 被调用 → 上层才知道失败了                │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

### 15.2 一次完整循环时序

```
NormalTaskSubmitter: spillback 到 dead_node
    │
    ▼
RetryableGrpcClient: 发送 RPC → UNAVAILABLE → 内部重试
    resend_count: 1, 2, 3, ..., 10 (约 10s)
    │
    ▼ resend_count >= 10
Fail("Channel READY/IDLE but 10 consecutive resends failed")
    │
    ▼
NormalTaskSubmitter: callback(TimedOut)
    spillback_retry_count++ (0→1)
    failure_reason = "ready_idle_resend_limit"
    RequestNewWorkerIfNeeded(local)
    │
    ▼
Local Raylet: 资源视图是否已更新?
    │
    ├── 已更新（知道节点死了）→ spillback 到活节点 → grant ✓
    │   spillback_retry_count = 0
    │
    └── 未更新 → 又 spillback 到同一死节点 → 又一轮 10s → count=2
        → 继续循环... 直到资源视图更新
```

### 15.3 为什么 `spillback_retry_count` 不需要上限

当前 `spillback_retry_count` 仅用于诊断日志，没有行为决策。原因：

1. **循环能自行收敛**：资源视图通过 GCS 广播更新
2. **收敛时间可预期**：`ray_syncer_message_refresh_interval_ms` 决定上界
3. **加上限风险高**：正常的资源抖动（节点临时负载高→reject→重试）也会被误杀

例如 `ray_syncer_message_refresh_interval_ms = 60000`（60s）：
- 每轮传输层检测 ~10s
- 最多 6 轮 × 10s = 60s 后资源视图更新 → 收敛

### 15.4 配置协同关系

```
grpc_max_ready_idle_resend_count = 10        (~10s 检测一个死目标)
                    ×
ray_syncer_message_refresh_interval_ms       (资源视图更新延迟)
                    =
最差恢复时间 ≈ ceil(sync_interval / 10s) × 10s

例: sync_interval = 60s → 最差 60s
例: sync_interval = 10s → 最差 10s
```

---

## 十六、READY/IDLE 重试上限修复实现

### 16.1 修复概述

commit: `[T11613716] add READY/IDLE resend limit to prevent infinite gRPC retry loops`

| 文件 | 改动 |
|---|---|
| `src/ray/common/ray_config_def.h` | 新增 `grpc_max_ready_idle_resend_count` 配置（默认 10） |
| `src/ray/rpc/retryable_grpc_client.h` | 新增 `consecutive_ready_idle_resend_count_` 成员变量 |
| `src/ray/rpc/retryable_grpc_client.cc` | READY/IDLE 分支增加计数+上限检查逻辑；增强诊断日志 |
| `src/ray/core_worker/task_submission/normal_task_submitter.cc` | 增加 `ready_idle_resend_limit` failure_reason；日志限流 |

### 16.2 计数器行为

| 事件 | 计数器操作 |
|---|---|
| CheckChannelStatus 进入 READY/IDLE 且重发 | `++` |
| CheckChannelStatus 进入 TRANSIENT_FAILURE/CONNECTING | `= 0` |
| `pending_requests_` 变空（所有请求成功或超时） | `= 0` |
| 超限触发 fail all | `= 0` |

### 16.3 为什么不在 RPC 成功时重置

如果 READY/IDLE 重发后 RPC **成功**了（目标确实是有效 Raylet），请求不会回到 Retry 队列 → 队列自然清空 → `pending_requests_.empty()` → 计数器在下次 CheckChannelStatus 开头被清零。

不需要额外的"成功时重置"逻辑——队列清空本身就是重置信号。

### 16.4 `grpc_max_ready_idle_resend_count` 取值建议

| count 值 | 检测时间 | 误杀风险 | 适用场景 |
|---|---|---|---|
| 3 | ~3s | Raylet 重启(2-5s)可能误触发 | 不推荐 |
| 5 | ~5s | 低风险 | 资源广播很快(≤10s)的环境 |
| **10** | **~10s** | **安全** | **通用默认值** |
| 20 | ~20s | 无风险但检测慢 | 极保守环境 |

**设置原则**：检测时间应远小于 `ray_syncer_message_refresh_interval_ms`，否则检测不是瓶颈。

### 16.5 部署方式

```bash
# 环境变量
export RAY_grpc_max_ready_idle_resend_count=10

# ray.init()
ray.init(_system_config={"grpc_max_ready_idle_resend_count": 10})

# 禁用（恢复原始行为）
export RAY_grpc_max_ready_idle_resend_count=0
```

---

## 十七、完整诊断日志体系

### 17.1 日志关键词与含义

| 日志关键词 | 来源 | 含义 | 对应场景 |
|---|---|---|---|
| `request entered retry queue, starting check timer` | retryable_grpc_client.cc Retry() | 请求首次进入重试队列 | 问题开始时间点 |
| `channel is READY/IDLE, resending N pending requests (resend_count=X/10)` | retryable_grpc_client.cc (DEBUG) | READY/IDLE 循环中，正在重发 | READY/IDLE 循环进行中 |
| `N consecutive resends all failed (UNAVAILABLE). Failing all M pending requests` | retryable_grpc_client.cc | READY/IDLE 重试上限触发 | **READY/IDLE 循环终止** |
| `has been unavailable for more than N seconds (channel_state=TRANSIENT_FAILURE, pending_requests=M)` | retryable_grpc_client.cc | TRANSIENT_FAILURE 退避 callback 触发 | TRANSIENT_FAILURE 检测中 |
| `pending request timed out in retry queue (channel_state=X, ...)` | retryable_grpc_client.cc | 排队超时被清理（worker_lease_timeout_ms 生效） | 排队超时兜底 |
| `Disconnecting raylet client because its node is dead` | raylet_client_pool.cc | GCS 确认节点死亡，执行 Disconnect | TRANSIENT_FAILURE 恢复路径 |
| `Failed to get node info from GCS` | raylet_client_pool.cc | GCS 查询失败 | Disconnect 恢复被阻断 |
| `Remote lease failed ... reason: X spillback_retry_count: N` | normal_task_submitter.cc | 传输层失败回调到应用层 | 应用层重试 |
| `Lease rejected ... spillback_retry_count: N` | normal_task_submitter.cc | 远程 Raylet 资源不足拒绝 | 资源视图过期 |

### 17.2 failure_reason 分类

| failure_reason | 触发层 | 单次耗时 | 含义 |
|---|---|---|---|
| `ready_idle_resend_limit` | RetryableGrpcClient READY/IDLE 上限 | **~10s** | 节点死了但 IP 被复用 |
| `connection_failed` | RetryableGrpcClient 排队超时 | ~600s | 节点死了，TCP 连不上 |
| `grpc_deadline_exceeded` | gRPC Deadline | ~600s | 对端不回复（卡住） |
| `rpc_error` | 非 TimedOut 的错误 | 即时 | UNAUTHENTICATED 等不可重试错误 |

### 17.3 生产环境排查命令

```bash
RAY_DIR="/tmp/ray/session_latest"

# 1. 确认是否触发了 READY/IDLE 上限（核心修复生效的标志）
grep "consecutive resends all failed" "$RAY_DIR/logs/"worker-*.out

# 2. 确认是否触发了排队超时
grep "pending request timed out in retry queue" "$RAY_DIR/logs/"worker-*.out

# 3. 确认 TRANSIENT_FAILURE 路径
grep "has been unavailable.*TRANSIENT_FAILURE" "$RAY_DIR/logs/"worker-*.out

# 4. 查看应用层重试次数和 failure_reason 分布
grep "Remote lease failed" "$RAY_DIR/logs/"worker-*.out | \
  grep -oP 'reason: \w+' | sort | uniq -c | sort -rn

# 5. 查看 spillback_retry_count 是否收敛
grep "spillback_retry_count" "$RAY_DIR/logs/"worker-*.out | \
  grep -oP 'spillback_retry_count: \d+' | sort -t: -k2 -n | tail -5

# 6. 首次进入重试的时间（问题开始点）
grep "request entered retry queue" "$RAY_DIR/logs/"worker-*.out | head -3

# 7. 配置确认
grep -E "grpc_max_ready_idle_resend_count|worker_lease_timeout_ms" "$RAY_DIR/logs/"raylet.out
```

### 17.4 诊断决策树（更新版）

```
                    lease active > 0 且持续不变?
                    │
          ┌─────── YES ───────┐                    NO → 不是 lease 卡死问题
          │                   │
          ▼                   │
    "Remote lease failed" 出现?
    │
┌── YES ────────────────────────────────────────┐
│                                                │
│  修复已生效，正在重试中                        │
│  检查 failure_reason:                         │
│  ├── ready_idle_resend_limit → IP 复用场景    │
│  ├── connection_failed → TCP 断开 + 排队超时  │
│  └── grpc_deadline_exceeded → Server 卡死     │
│                                                │
│  检查 spillback_retry_count:                  │
│  ├── 持续增长 → 资源视图更新慢，等待收敛      │
│  └── 最终归零 → 已恢复 ✓                      │
│                                                │
└── NO ─────────────────────────────────────────┘
     │
     ▼
*** callback 从未被调用 → 修复未生效 ***
     │
     ├── 检查配置:
     │    RAY_grpc_max_ready_idle_resend_count 是否为 0 (禁用)?
     │    RAY_worker_lease_timeout_ms 是否为 -1 (禁用)?
     │
     └── 检查是否是修复前的版本（无 resend limit 逻辑）
```

### 17.5 日志时间线还原方法

```bash
# 完整事件时间线
echo "=== 1. 问题开始（首次进入重试队列）==="
grep "request entered retry queue" $RAY_DIR/logs/worker-*.out | head -3

echo "=== 2. READY/IDLE 循环（DEBUG 级别，需开启）==="
grep "resending.*pending requests" $RAY_DIR/logs/worker-*.out | tail -5

echo "=== 3. 传输层终止（resend limit 或 timeout）==="
grep -E "consecutive resends all failed|pending request timed out" $RAY_DIR/logs/worker-*.out | head -5

echo "=== 4. 应用层回调 ==="
grep "Remote lease failed" $RAY_DIR/logs/worker-*.out | head -5

echo "=== 5. 恢复（如果有）==="
grep "Lease granted" $RAY_DIR/logs/worker-*.out | tail -3
```

---

## 十八、配置参数完整索引

| 参数 | 默认值 | 作用 | 环境变量 |
|---|---|---|---|
| `worker_lease_timeout_ms` | 600000 (10min) | spillback RPC 的排队超时 + gRPC deadline | `RAY_worker_lease_timeout_ms` |
| `grpc_max_ready_idle_resend_count` | 10 | READY/IDLE 最大连续重发次数 | `RAY_grpc_max_ready_idle_resend_count` |
| `grpc_client_check_connection_status_interval_milliseconds` | 1000 | CheckChannelStatus 调用周期 | `RAY_grpc_client_check_connection_status_interval_milliseconds` |
| `raylet_rpc_server_reconnect_timeout_base_s` | 1 | TRANSIENT_FAILURE 退避基准 | `RAY_raylet_rpc_server_reconnect_timeout_base_s` |
| `raylet_rpc_server_reconnect_timeout_max_s` | 60 | TRANSIENT_FAILURE 退避上限 | `RAY_raylet_rpc_server_reconnect_timeout_max_s` |
| `raylet_report_resources_period_milliseconds` | 100 | Raylet 上报资源周期 | `RAY_raylet_report_resources_period_milliseconds` |
| `ray_syncer_message_refresh_interval_ms` | 20000 | Ray Syncer 消息刷新间隔 | `RAY_ray_syncer_message_refresh_interval_ms` |

---

## 十九、Timeout 重置机制深入分析

### 19.1 问题：为什么 600s 超时永远不会过期？

在 READY/IDLE 循环场景中，即使设置了 `worker_lease_timeout_ms=600000`（600s），请求也永远不会因超时被清理。原因是 **timeout deadline 不是从首次发送计算，而是从每次重新入队计算**。

### 19.2 Timeout 计算的代码路径

请求首次失败后进入 `Retry()`（`retryable_grpc_client.cc:178`）：

```cpp
const auto now = absl::Now();  // ← 取当前时间
const auto method_timeout_ms = request->GetTimeoutMs();
const auto timeout = method_timeout_ms == -1
                         ? absl::InfiniteFuture()
                         : now + absl::Milliseconds(method_timeout_ms);
pending_requests_.emplace(timeout, std::move(request));
```

关键：deadline = `now + timeout_ms`，而 `now` 是每次入队时的当前时间。

### 19.3 READY/IDLE 循环中的 Timeout 重置时间线

```
T=0:       首次 RPC 发出, deadline 设为 T+600s
T=0.01:    UNAVAILABLE 快速返回
           → Retry() → pending_requests_[T+600.01] = request
           → SetupCheckTimer()

T=1:       CheckChannelStatus() → channel READY/IDLE
           → 超时扫描: pending_requests_ 首项 T+600.01 > T+1 → 不过期
           → CallMethod() 重发, 从 pending_requests_ 移除

T=1.01:    UNAVAILABLE 快速返回
           → Retry() → pending_requests_[T+601.01] = request   ← deadline 向后推了！
           → server_unavailable_timeout_time_ 被清空（READY/IDLE 分支设了 nullopt）
           → 重新 SetupCheckTimer()

T=2:       CheckChannelStatus() → channel READY/IDLE
           → 超时扫描: T+601.01 > T+2 → 不过期
           → CallMethod() 重发

T=2.01:    UNAVAILABLE 快速返回
           → Retry() → pending_requests_[T+602.01] = request   ← 再次向后推！

... 无限循环 ...
```

每次经过 READY/IDLE → CallMethod() → UNAVAILABLE → Retry() 循环，deadline 都会从**当前时间**重新计算。600s 的窗口永远不会关闭。

### 19.4 对比：TRANSIENT_FAILURE 路径不存在此问题

在 TRANSIENT_FAILURE 分支中，请求**不会被移出队列**：

```cpp
case GRPC_CHANNEL_TRANSIENT_FAILURE:
case GRPC_CHANNEL_CONNECTING: {
    // 请求留在 pending_requests_ 中，deadline 不变
    // 只是等待下一次 timer 触发
    if (reset_timer) SetupCheckTimer();
    break;
}
```

请求的 deadline 保持为首次入队时计算的值，不会被重置。所以 600s 后确实会过期：

```
T=0.01:    pending_requests_[T+600.01] = request

T=1:       CheckChannelStatus() → TRANSIENT_FAILURE → 不动
T=2:       CheckChannelStatus() → TRANSIENT_FAILURE → 不动
...
T=600.02:  CheckChannelStatus()
           → 超时扫描: T+600.01 < T+600.02 → 过期！
           → Fail(Status::TimedOut(...))
```

### 19.5 两条路径的 Timeout 行为总结

| 场景 | 请求是否移出队列 | Deadline 是否重置 | 600s 能否生效 |
|------|-----------------|------------------|--------------|
| TRANSIENT_FAILURE | 否，留在队列 | 否，保持首次值 | ✓ 能生效 |
| READY/IDLE | 是，CallMethod 后移出 | 是，重新入队时重算 | ✗ 永远不过期 |
| timeout_ms = -1 | N/A | N/A | ✗ InfiniteFuture |

### 19.6 修复如何解决此问题

`grpc_max_ready_idle_resend_count` 引入了基于**次数**而非时间的终止条件：

```cpp
if (max_resend_count > 0 &&
    consecutive_ready_idle_resend_count_ >= max_resend_count) {
    // 不依赖 timeout，直接按次数终止
    Fail(...);
}
```

次数计数器 `consecutive_ready_idle_resend_count_` 在每次 READY/IDLE 重发时递增，不受 timeout 重置影响。

---

## 二十、gRPC Channel 状态获取机制

### 20.1 状态获取 API

在 `CheckChannelStatus()` 中（`retryable_grpc_client.cc:81`）：

```cpp
auto status = channel_->GetState(false);
```

- `channel_` 类型：`std::shared_ptr<grpc::Channel>`
- `GetState(bool try_to_connect)` 是 gRPC C++ 核心 API
- 参数 `false`：不触发连接尝试（如果是 IDLE 状态不会主动去连）
- 参数 `true`：如果当前是 IDLE，会触发连接尝试转入 CONNECTING

### 20.2 Channel 状态机

gRPC 内部维护的状态机（定义于 `grpc/impl/codegen/connectivity_state.h`）：

```
                        ┌─────────────────────────────────┐
                        │                                 │
                        ▼                                 │
                ┌──────────────┐                          │
   创建 Channel │    IDLE      │ ←── keepalive 超时 ──────┤
                └──────┬───────┘                          │
                       │                                  │
          首次 RPC 或 GetState(true)                      │
                       │                                  │
                       ▼                                  │
                ┌──────────────┐                          │
                │  CONNECTING  │ ←── 内部 backoff 重试 ───┤
                └──────┬───────┘                          │
                       │                                  │
           ┌───────────┴───────────┐                      │
           │                       │                      │
     TCP 握手成功            TCP 握手失败                  │
           │                       │                      │
           ▼                       ▼                      │
    ┌──────────────┐     ┌───────────────────┐            │
    │    READY     │     │ TRANSIENT_FAILURE  │ ───────────┘
    └──────┬───────┘     └───────────────────┘
           │                    (内部 backoff: 1s→2s→4s...→120s)
           │
     连接断开 / RST / FIN
           │
           ▼
    IDLE 或 TRANSIENT_FAILURE (取决于断开方式)
```

### 20.3 各状态的含义与产生条件

| 状态 | 含义 | 产生条件 | Ray 的处理 |
|------|------|---------|-----------|
| `GRPC_CHANNEL_IDLE` | Channel 创建后未使用，或连接被优雅关闭 | 新建 channel / keepalive 超时 / 长时间无 RPC | 重发 pending 请求 |
| `GRPC_CHANNEL_CONNECTING` | 正在进行 TCP 三次握手 | 从 IDLE 或 TRANSIENT_FAILURE 触发连接 | 等同 TRANSIENT_FAILURE 处理 |
| `GRPC_CHANNEL_READY` | TCP 连接正常，HTTP/2 会话建立 | TCP + TLS + HTTP/2 协商完成 | 重发 pending 请求 |
| `GRPC_CHANNEL_TRANSIENT_FAILURE` | TCP 连接失败 | connect() 返回错误 / 连接被 RST | 保留队列，等待重连 |
| `GRPC_CHANNEL_SHUTDOWN` | Channel 被显式关闭 | 调用 channel->Shutdown() | RAY_LOG(FATAL) |

### 20.4 状态检测是被动读取而非主动监控

Ray **不订阅**状态变化通知（虽然 gRPC 提供了 `NotifyOnStateChange` API）。而是通过 **定时器轮询**：

```
SetupCheckTimer() → timer 1s → CheckChannelStatus() → channel_->GetState(false) → 读取当前状态
```

这意味着状态变化不会立即被感知，最多延迟一个 timer 周期（默认 1s）。

### 20.5 gRPC 内部重连 Backoff（独立于 Ray）

gRPC 库内部有自己的重连策略，与 Ray 的 `ExponentialBackoff` 完全独立：

| 属性 | gRPC 内部 | Ray RetryableGrpcClient |
|------|-----------|------------------------|
| 初始退避 | 1s | `server_reconnect_timeout_base_seconds_` (默认1s) |
| 最大退避 | 120s | `server_reconnect_timeout_max_seconds_` (默认60s) |
| 作用 | 控制 TCP 重连尝试间隔 | 控制 `server_unavailable_timeout_callback_` 调用间隔 |
| 目标 | TRANSIENT_FAILURE → CONNECTING 的触发频率 | GCS 查询节点存活的频率 |

### 20.6 K8s IP 复用场景下的状态表现

```
原始 Pod (10.0.0.5:9999):
  Channel 状态: READY
  RPC 结果: 正常

原始 Pod 被杀:
  Channel 状态: READY → (发现断开) → IDLE 或 TRANSIENT_FAILURE
  取决于: 是优雅关闭(FIN) 还是强杀(RST/无响应)

新 Pod 获得 10.0.0.5:
  ┌─ 新 Pod 不监听 9999 端口:
  │   Channel 状态: TRANSIENT_FAILURE (connect refused)
  │   Ray 处理: 保留队列等待, 最终 GCS callback 触发 Disconnect → ✓ 正确处理
  │
  └─ 新 Pod 监听 9999 端口 (如 envoy sidecar, 其他服务):
      Channel 状态: READY (TCP + HTTP/2 握手成功)
      RPC 结果: UNAVAILABLE (gRPC 服务不认识这个 method)
      Ray 处理: READY/IDLE 分支 → 重发 → 又失败 → 无限循环
                修复后: consecutive_ready_idle_resend_count >= 10 → 终止
```

---

## 二十一、完整 Stuck 场景时序图

整合所有分析，展示从正常到卡死到修复后恢复的完整时序：

```
═══════════════════════ 正常阶段 ═══════════════════════

Worker               Raylet-A (10.0.0.5)     GCS
  │                       │                    │
  │── RequestWorkerLease ─→│                    │
  │←── Reply (granted) ───│                    │
  │                       │                    │

═══════════════════ Pod 被杀 (T=0) ══════════════════════

Worker               Raylet-A (dead)         GCS         新 Pod (10.0.0.5)
  │                       ✗                    │               │
  │── RequestWorkerLease ─→✗                   │               │
  │   (TCP connects to new pod at 10.0.0.5)   │               │
  │←── UNAVAILABLE ────────────────────────────┼───────────────│
  │                                            │               │
  │   [进入 Retry()]                           │               │
  │   pending_requests_[T+600] = request       │               │
  │   SetupCheckTimer()                        │               │
  │                                            │               │

═══════════ 修复前：无限循环 (T=1, 2, 3, ...) ═══════════

  │   [T=1: CheckChannelStatus]                │               │
  │   channel_->GetState(false) = READY        │               │
  │   server_unavailable_timeout_time_ = nullopt               │
  │   CallMethod() 重发                        │               │
  │── RequestWorkerLease ──────────────────────┼──────────────→│
  │←── UNAVAILABLE ────────────────────────────┼───────────────│
  │   [Retry() 重新入队]                       │               │
  │   pending_requests_[T+601] = request       │  ← deadline 被重置!
  │                                            │               │
  │   ... 重复 20+ 小时 ...                    │               │

═══════════ 修复后：10 次后终止 (T=1~10) ═══════════════

  │   [T=1~9: 同上，但计数器递增]              │               │
  │   consecutive_ready_idle_resend_count = 1,2,...,9           │
  │                                            │               │
  │   [T=10: CheckChannelStatus]               │               │
  │   consecutive_ready_idle_resend_count = 10 >= 10           │
  │   RAY_LOG(WARNING) << "consecutive resends all failed"     │
  │   Fail(TimedOut("Channel READY/IDLE but 10 consecutive.."))│
  │                                            │               │
  │   [回调到 normal_task_submitter.cc]        │               │
  │   failure_reason = "ready_idle_resend_limit"               │
  │   RAY_LOG_EVERY_MS(WARNING) << "Remote lease failed"       │
  │   spillback_retry_count++                  │               │
  │   RequestWorkerLease → local raylet (重新调度)             │
  │                                            │               │

═══════════════════ 恢复阶段 ═══════════════════════════

  │   [资源广播最终收敛, 移除 dead node]       │               │
  │── RequestWorkerLease → Raylet-B ──────────→│               │
  │←── Reply (granted) ───────────────────────│               │
  │   ✓ 恢复正常                              │               │
```
