# Ray Streaming Generator Task 提交、执行、回调与对象恢复机制详解

## 目录

- [1. Normal Task 完整提交流程](#1-normal-task-完整提交流程)
- [2. PushNormalTask 回调详解](#2-pushnormaltask-回调详解)
- [3. 提交方与执行方的通信架构](#3-提交方与执行方的通信架构)
- [4. addr 参数的作用](#4-addr-参数的作用)
- [5. 回调中的四种结果处理](#5-回调中的四种结果处理)
- [6. resubmit_generator：延迟重建机制](#6-resubmit_generator延迟重建机制)
- [7. MarkTaskReturnObjectsFailed 与 Streaming Generator](#7-marktaskreturnobjectsfailed-与-streaming-generator)
- [8. reconstructable_return_ids_ 生命周期](#8-reconstructable_return_ids_-生命周期)
- [9. 重建时跳过已有 Object 的机制](#9-重建时跳过已有-object-的机制)
- [10. 重试 vs 重建：两种恢复机制对比](#10-重试-vs-重建两种恢复机制对比)
- [11. ray.wait 与 ray.get 在不同场景下的行为](#11-raywait-与-rayget-在不同场景下的行为)

---

## 1. Normal Task 完整提交流程

### 阶段 1：提交端 — SubmitTask + 依赖解析

```
用户代码 ray.remote(f).remote()
       │
       ▼
NormalTaskSubmitter::SubmitTask(task_spec)
       │
       ▼
resolver_.ResolveDependencies(task_spec, callback)
  - 解析 task 参数引用的 Object 依赖
  - 如果依赖不可用，等依赖就绪后才触发回调
  - 失败 → task_manager_.FailOrRetryPendingTask(DEPENDENCY_RESOLUTION_FAILED)
  - 成功 → 进入下一步
```

### 阶段 2：提交端 — 入队 + 请求 Worker Lease

```
依赖解析完成回调中：
  │
  ├─ 检查是否已取消 (cancelled_tasks_)
  │
  ├─ task 入队: scheduling_key_entry.task_queue.push_back(task_spec)
  │
  ├─ 如果有空闲 worker → 直接调用 OnWorkerIdle()
  │
  └─ RequestNewWorkerIfNeeded(scheduling_key)
       │
       ▼
  raylet_client->RequestWorkerLease(lease_spec, callback)
  - 向 raylet 请求一个 worker 的租约
  - raylet 分配一个 worker，返回 worker_address
  - 回调中调用 AddWorkerLeaseClient() + OnWorkerIdle()
```

### 阶段 3：提交端 — OnWorkerIdle → PushNormalTask

```
OnWorkerIdle(addr, scheduling_key, was_error=false, ...)
  │
  ├─ was_error / lease过期 / 队列为空 → ReturnWorkerLease (归还 worker)
  │
  └─ 队列非空 && worker空闲 →
       │
       ├─ 从队列取出 task_spec
       ├─ lease_entry.is_busy = true
       ├─ executing_tasks_.emplace(task_id, addr)
       │
       └─ PushNormalTask(addr, client, scheduling_key, task_spec, resources)
            │
            ├─ 构造 PushTaskRequest (task_spec + resource_mapping + intended_worker_id)
            ├─ task_manager_.MarkTaskWaitingForExecution(task_id, node_id, worker_id)
            │
            └─ client->PushNormalTask(request, callback)  ← gRPC 调用
```

### 阶段 4：RPC 传输层

```
CoreWorkerClient::PushNormalTask(request, callback)
  │
  ├─ request->set_sequence_number(-1)      // normal task 无序
  ├─ request->set_client_processed_up_to(-1)
  │
  └─ INVOKE_RPC_CALL(CoreWorkerService, PushTask, *request, callback)
       │
       ▼  ━━━ gRPC 网络传输 ━━━▶  远程 CoreWorker
```

### 阶段 5：执行端 — HandlePushTask

```
CoreWorker::HandlePushTask(request, reply, send_reply_callback)
  │
  ├─ 验证 intended_worker_id 是否匹配
  ├─ 对 actor creation task 做特殊处理
  │
  └─ Normal Task 路径:
       │
       ├─ task_receiver_->QueueTaskForExecution(request, reply, send_reply_callback)
       │    │
       │    ├─ 创建 execute_callback = [task_handler_(task_spec, ...)]
       │    ├─ 创建 cancel_callback
       │    └─ normal_task_execution_queue_->EnqueueTask(TaskToExecute)
       │
       └─ task_execution_service_.post(ExecuteQueuedNormalTasks)
            │
            └─ normal_task_execution_queue_->ExecuteQueuedTasks()
                 - 从队列取出 task
                 - 调用 execute_callback → task_handler_()
```

### 阶段 6：执行端 — ExecuteTask（真正执行用户代码）

```
CoreWorker::ExecuteTask(task_spec, resource_ids, return_objects, ...)
  │
  ├─ GetAndPinArgsForExecutor()
  │    - 从 plasma/in-memory store 获取并 pin 参数 Object
  │
  ├─ 设置 worker 上下文（current task, running_tasks_）
  │
  ├─ 准备 return_objects 槽位 (每个返回值一个)
  │
  └─ options_.task_execution_callback(...)  ← 真正调用用户函数！
       │
       │  (这是 Python/C++/Java 语言运行时的回调)
       │  执行用户的 @ray.remote 函数
       │  返回值写入 return_objects
       │
       ▼
  return_objects 被填充 → 返回 Status
```

### 阶段 7：执行端 → 返回结果

```
TaskReceiver::HandleTaskExecutionResult(status, task_spec, result, send_reply_callback, reply)
  │
  ├─ 将 return_objects 填入 reply
  ├─ 设置 is_retryable_error, was_cancelled_before_running 等标志
  │
  └─ send_reply_callback(Status::OK())
       │
       ▼  ━━━ gRPC 网络传输 ━━━▶  提交端回调被触发
```

### 阶段 8：提交端 — 回调处理

```
client->PushNormalTask 的回调被触发 (status, reply)
  │
  ├─────────────────────── 锁内（mu_） ───────────────────────┤
  │                                                           │
  │  executing_tasks_.erase(task_id)                          │
  │  resubmit_generator = generators_to_resubmit_.erase()     │
  │  lease_entry.is_busy = false  ← 标记 worker 空闲          │
  │  scheduling_key_entry.num_busy_workers--                  │
  │                                                           │
  │  ┌─ status.ok() (gRPC 成功)                              │
  │  │  直接调用 OnWorkerIdle() 让 worker 继续执行下一个 task │
  │  │                                                       │
  │  └─ !status.ok() (gRPC 失败 = worker 崩溃/死亡)          │
  │     │                                                     │
  │     ├─ failed_tasks_pending_failure_cause_.insert(task_id)│
  │     └─ raylet_client->GetWorkerFailureCause(lease_id,     │
  │           nested_callback)                               │
  │            │                                             │
  │            └─ nested_callback:                           │
  │               HandleGetWorkerFailureCause()               │
  │                 → FailOrRetryPendingTask(task_id, ...)    │
  │                   ├─ 有重试次数 → RetryTaskIfPossible    │
  │                   │   (重新提交 task 到队列)              │
  │                   └─ 无重试次数 → FailPendingTask         │
  │                       (标记所有返回 Object 为失败)        │
  │                                                           │
  │  OnWorkerIdle(was_error=!status.ok(), ...)               │
  │    - error 时 → ReturnWorkerLease (归还 worker)          │
  │    - 正常时 → 尝试派发下一个 task                        │
  │                                                           │
  ├─────────────────────── 锁外 ─────────────────────────────┤
  │                                                           │
  │  ┌─ status.ok() 时:                                      │
  │  │                                                       │
  │  ├─ reply.was_cancelled_before_running() == true         │
  │  │  → task_manager_.FailPendingTask(TASK_CANCELLED)      │
  │  │                                                       │
  │  ├─ resubmit_generator == true                           │
  │  │  → task_manager_.MarkGeneratorFailedAndResubmit()     │
  │  │                                                       │
  │  ├─ retry_exceptions + is_retryable_error + 可重试       │
  │  │  → task_manager_.RetryTaskIfPossible(task_id, error)  │
  │  │    ├─ 重试次数 > 0 → 重新提交 task                    │
  │  │    └─ 重试次数 = 0 → FailPendingTask                  │
  │  │                                                       │
  │  └─ 其他（正常完成）                                     │
  │     → task_manager_.CompletePendingTask(task_id, reply)  │
  │        ├─ 处理 return_objects (写入 plasma/in-memory)    │
  │        ├─ 更新引用计数                                   │
  │        └─ 从 pending 集合移除 task                       │
  │                                                           │
  └───────────────────────────────────────────────────────────┘
```

---

## 2. PushNormalTask 回调详解

回调**不是在本地执行 task**，而是在远程 worker 执行完 task 并发回 `PushTaskReply` 后，处理 gRPC 响应。

### 回调内部逻辑

**阶段一：状态清理 + 错误处理**（在 mutex 锁内）

- 从 `executing_tasks_` 移除
- 检查 `generators_to_resubmit_` 是否需要重新提交 generator
- 标记 worker 空闲 (`lease_entry.is_busy = false`)
- 减少 busy 计数 (`num_busy_workers--`)

**若 `!status.ok()`（gRPC 失败，worker 崩溃/死亡）**：

- 向 raylet 查询失败原因 `GetWorkerFailureCause()`
- 在嵌套回调中调用 `HandleGetWorkerFailureCause()` → `FailOrRetryPendingTask()`
- 失败原因可能是：`WORKER_DIED`、`NODE_DIED` 等

**调用 `OnWorkerIdle()`**：检查队列中是否有更多 task 可以派发给此 worker

**阶段二：任务结果处理**（锁外，仅在 `status.ok()` 时）

| 场景 | 行为 |
|------|------|
| gRPC 成功 + 正常完成 | `CompletePendingTask` |
| gRPC 成功 + 被取消 | `FailPendingTask(TASK_CANCELLED)` |
| gRPC 成功 + 可重试异常 | `RetryTaskIfPossible` |
| gRPC 失败（worker 死亡） | 查询原因后 `FailOrRetryPendingTask` |
| gRPC 失败（节点死亡） | `FailOrRetryPendingTask(NODE_DIED)` |

---

## 3. 提交方与执行方的通信架构

### 核心机制：提交方 Worker 与执行方 Worker 之间是**直接 gRPC 通信**

```
┌─────────────┐    ①RequestWorkerLease     ┌─────────────┐
│  Submitter  │ ──────────────────────────▶ │   Raylet    │
│   Worker    │ ◀──────────────────────── │  (调度分配)  │
└──────┬──────┘    ②reply.worker_address    └──────┬──────┘
       │                                            │
       │         ③GetOrConnect(addr)                │ 启动/分配
       │         建立直连 gRPC channel              │
       │                                            ▼
       │                                   ┌──────────────┐
       ├──── ③PushNormalTask (gRPC) ──────▶│  Executor     │
       │                                   │   Worker      │
       │◀─── ④PushTaskReply (gRPC) ───────│  (执行task)   │
       │                                   └──────────────┘
       │
       │  ⑤ReturnWorkerLease (归还worker)
       └────────────────────────────────────▶ Raylet
```

### Raylet 的角色

仅负责调度（选节点、分配资源、启动 worker），**不中转 task 数据**。

### 获取执行 Worker 的地址

提交方不直接"选择"worker，而是向 raylet **申请租约**：

```cpp
raylet_client->RequestWorkerLease(lease_spec.GetMessage(), ...,
  [](const Status &status, const rpc::RequestWorkerLeaseReply &reply) {
    if (!reply.worker_address().node_id().empty()) {
      AddWorkerLeaseClient(reply.worker_address(), ...);
      OnWorkerIdle(reply.worker_address(), ...);
    }
  });
```

### 建立直连

```cpp
void NormalTaskSubmitter::AddWorkerLeaseClient(
    const rpc::Address &worker_address, ...) {
  core_worker_client_pool_->GetOrConnect(worker_address);  // 直连
}
```

### 租约生命周期

Submitter 持有 worker 租约 → 派发 task → task 完成 → 继续派发或 `ReturnWorkerLease` 归还

---

## 4. addr 参数的作用

`PushNormalTask` 的 `addr` 参数是**执行方 Worker 的地址**（executor），不是 owner 的地址。

### addr 的来源

```
RequestNewWorkerIfNeeded()
  → raylet 回调返回 reply.worker_address()  ← 这就是 addr
  → AddWorkerLeaseClient(worker_address, ...)
  → OnWorkerIdle(reply.worker_address(), ...)
  → PushNormalTask(addr, client, ...)
```

### addr 在发送阶段（仅2处）

```cpp
// 1. 告诉执行方"你是预期接收者"（防错机制，不是路由）
request->set_intended_worker_id(addr.worker_id());

// 2. 记录 task 在哪个 node/worker 执行（追踪用）
task_manager_.MarkTaskWaitingForExecution(
    task_id,
    NodeID::FromBinary(addr.node_id()),
    WorkerID::FromBinary(addr.worker_id()));
```

### addr 在回调阶段（主要消费方）

```
1. worker_to_lease_entry_[addr]       → 查找 lease 状态
2. raylet_client_pool_->GetOrConnectByAddress(cur_lease_entry.addr) → 找 raylet
3. OnWorkerIdle(addr, ...)             → 继续调度
4. CompletePendingTask(task_id, reply, addr, ...) → 传递给 task_manager
```

**`client` 是"怎么发"，`addr` 是"发给谁、发给谁之后怎么管理"**。`addr` 在回调中被用作 map 的 key 来查找 lease 状态、归还 worker、定位 raylet 等。

---

## 5. 回调中的四种结果处理

```cpp
if (status.ok()) {
  if (reply.was_cancelled_before_running()) {
    // 情况1：task 在开始执行前就被取消了
    task_manager_.FailPendingTask(task_id, rpc::ErrorType::TASK_CANCELLED);

  } else if (resubmit_generator) {
    // 情况2：generator task 需要重新提交（对象恢复）
    task_manager_.MarkGeneratorFailedAndResubmit(task_id);

  } else if (!task_spec.GetMessage().retry_exceptions() ||    // ①用户未开启重试
             !reply.is_retryable_error() ||                   // ②错误不可重试
             !task_manager_.RetryTaskIfPossible(              // ③重试次数用完
                 task_id,
                 gcs::GetRayErrorInfo(rpc::ErrorType::TASK_EXECUTION_EXCEPTION,
                                       reply.task_execution_error()))) {
    // 情况3：不需要重试 或 不能重试 → 标记完成
    task_manager_.CompletePendingTask(task_id, reply, addr, reply.is_application_error());
  }
  // 隐含 else：RetryTaskIfPossible 返回 true → task 已被重新提交
}
```

### 情况1：`was_cancelled_before_running`

执行方 worker 收到 PushTaskRequest 后检查发现 task 已被取消（CancelTask RPC 先到了），没有执行用户代码，设置 `reply.was_cancelled_before_running = true`。

### 情况2：`resubmit_generator`

Generator task 的返回值 Object 丢失时，需要重新执行 generator 来重建数据。详见 [第6节](#6-resubmit_generator延迟重建机制)。

### 情况3：异常处理的三重判断

| 条件 | 含义 | 示例 |
|------|------|------|
| `!retry_exceptions` | 用户代码未启用 `@ray.remote(retry_exceptions=True)` | 默认情况 |
| `!is_retryable_error` | 执行方判断该异常不可重试 | `RayTaskError`（用户主动 raise 的异常） |
| `!RetryTaskIfPossible()` | 开启了重试但重试次数已用完 | `max_retries=3` 已重试3次 |

只有三个条件全为 false（开启了重试 + 异常可重试 + 还有重试次数），task 才会被重新提交到队列，跳过 `CompletePendingTask`。

---

## 6. resubmit_generator：延迟重建机制

这是 Ray 的**对象恢复（Object Reconstruction）**机制中的特殊路径，只出现在 **Streaming Generator Task** 上。

### 触发场景

```
1. Generator Task 正在远程 Worker 上运行 (状态: SUBMITTED_TO_WORKER)
2. 它之前产出的某个 Object 在 Plasma 中丢失了
3. 引用计数器检测到 Object 丢失，触发 TaskManager::ResubmitTask()
4. ResubmitTask() 发现：
   - task 是 IsStreamingGenerator()
   - task 状态是 SUBMITTED_TO_WORKER（还在运行！）
   - 还有重试次数 (num_retries_left_ > 0)
5. 不能立即重新提交！因为 task 还在运行
   → 设置 should_queue_generator_resubmit = true
   → 调用 queue_generator_resubmit_(spec)
   → 即 NormalTaskSubmitter::QueueGeneratorForResubmit()
6. QueueGeneratorForResubmit 将 task_id 加入 generators_to_resubmit_ 集合
7. 等 task 执行完毕，PushNormalTask 回调触发时：
   resubmit_generator = generators_to_resubmit_.erase(task_id) > 0
8. 调用 MarkGeneratorFailedAndResubmit(task_id)
   - 标记当前 attempt 为 FAILED (GENERATOR_TASK_FAILED_FOR_OBJECT_RECONSTRUCTION)
   - SetupTaskEntryForResubmit() → 增加 attempt number，减少 num_retries_left
   - async_retry_task_callback_(spec, 0) → 立即重新提交 task
```

### 为什么不能立即重提交？

```cpp
if (task_entry.spec_.IsStreamingGenerator() &&
    task_entry.GetStatus() == rpc::TaskStatus::SUBMITTED_TO_WORKER) {
  if (task_entry.num_retries_left_ == 0) {
    return rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED;
  }
  should_queue_generator_resubmit = true;
}
```

Streaming generator 可能还在运行中，产出的 Object 是动态的——可能 Object 生成后被 Plasma 淘汰了，但 generator 还在继续产出新的。此时不能打断正在运行的 generator，只能等它**执行完毕后**再重新提交来重建丢失的 Object。

### 完整时序图

```
时间 ──────────────────────────────────────────────────────────────▶

Generator Task 在 Worker 上运行中
  │ 产出 Object1, Object2, Object3 ...
  │
  │         Object2 被 Plasma 淘汰
  │              │
  │              ▼
  │         ResubmitTask() 被触发
  │              │
  │              ├─ task 还在运行 → 不能打断
  │              ├─ QueueGeneratorForResubmit()
  │              │   generators_to_resubmit_.insert(task_id)
  │              │
  │              │     ... generator 继续运行 ...
  │              │
  │              ▼
  │         Task 执行完毕 → PushTaskReply 返回
  │              │
  │              ├─ resubmit_generator == true
  │              ├─ MarkGeneratorFailedAndResubmit()
  │              │   ├─ 标记 FAILED
  │              │   └─ async_retry_task_callback_() → 重新提交 task
  │              │
  │              ▼
  │         新的 Generator Task 开始执行
  │              └─ 重新产出 Object1, Object2, Object3 ...（恢复丢失的 Object）
  ▼
```

---

## 7. MarkTaskReturnObjectsFailed 与 Streaming Generator

位于 `task_manager.cc:1597-1629`，属于 `MarkTaskReturnObjectsFailed` 方法，在 task 最终失败时被调用。

### 第一步：标记流结束

```cpp
const auto generator_id = spec.ReturnId(0);
MarkEndOfStream(generator_id, /*item_index*/ -1);
```

- `generator_id = spec.ReturnId(0)`：streaming generator 的第 0 个返回值是 generator 的身份标识
- `MarkEndOfStream(generator_id, -1)`：告诉消费者"这个流不会再产出新数据了"
- 传 `-1 表示"从当前最大索引之后结束"

`MarkEndOfStream` 内部：
1. 找到 generator_id 对应的 ObjectRefStream
2. 在流的末尾放置一个 END_OF_STREAMING_GENERATOR Error Object（哨兵值）
3. 消费者读到它就知道流已结束

### 第二步：标记所有 streaming generator 返回值为失败

```cpp
auto num_streaming_generator_returns = spec.NumStreamingGeneratorReturns();
for (size_t i = 0; i < num_streaming_generator_returns; i++) {
    const auto generator_return_id = spec.StreamingGeneratorReturnId(i);
    if (store_in_plasma_ids.contains(generator_return_id)) {
        put_in_local_plasma_callback_(error, generator_return_id);  // 写 Plasma
    } else {
        in_memory_store_.Put(error, generator_return_id, ...);     // 写内存
    }
}
```

### 这段代码在 Owner 端执行

完全在**Owner（提交方）端**执行，与 Executor 无关。Executor 此刻已经不可用或执行失败了。

### 为什么需要这第三段处理？

**正常情况**：generator 执行成功时，返回值已经写入了 Object Store。此时再写 Error 会被 Ray 忽略（不允许覆盖已有值），所以是 **no-op**。

**Lineage reconstruction 失败的情况**：

1. Generator 执行成功 → 返回值写入 Plasma
2. 节点故障 → Plasma 中的返回值丢失
3. 触发 lineage reconstruction → 重新执行 generator 恢复数据
4. 重试也失败了（达到 max_retries）→ 调用 FailPendingTask → MarkTaskReturnObjectsFailed
5. 此时 Plasma 中这些 Object 已经不存在了 → Error Object 可以覆盖写入
6. 下游消费者 ray.get() 读到的是 Error 而非永远阻塞

### 写入目标的选择

| 条件 | 写入位置 | 原因 |
|------|---------|------|
| `store_in_plasma_ids` 包含该 ID | Plasma Store | 该 Object 之前存放在 Plasma 中（大对象） |
| 不包含 | In-Memory Store | 该 Object 是小对象，存在内存中即可 |

---

## 8. reconstructable_return_ids_ 生命周期

### 阶段一：创建初始化（AddPendingTask）

```cpp
// TaskEntry 构造函数
TaskEntry(TaskSpecification spec, ...)
    : spec_(std::move(spec)), ... {
    reconstructable_return_ids_.reserve(num_returns);
    for (size_t i = 0; i < num_returns; i++) {
        reconstructable_return_ids_.insert(spec_.ReturnId(i));  // 初始包含所有普通返回值
    }
}
```

**此时集合内容**：`{ReturnId(0), ReturnId(1), ..., ReturnId(N-1)}`

### 阶段二：首次执行成功（CompletePendingTask）

```cpp
if (first_execution) {
    // 1. 动态返回值加入集合
    for (const auto &dynamic_return_id : dynamic_returns_in_plasma) {
        it->second.reconstructable_return_ids_.insert(dynamic_return_id);
    }
    // 2. Streaming generator 的 Plasma 返回值加入集合
    if (spec.IsStreamingGenerator()) {
        for (const auto &return_id_info : reply.streaming_generator_return_ids()) {
            if (return_id_info.is_plasma_object()) {
                it->second.reconstructable_return_ids_.insert(
                    ObjectID::FromBinary(return_id_info.object_id()));
            }
        }
    }
}

// 3. Direct return（非 Plasma）从集合中移除
for (const auto &direct_return_id : direct_return_ids) {
    it->second.reconstructable_return_ids_.erase(direct_return_id);
}
```

**此时集合内容**：只包含**实际写入了 Plasma 的返回值**

### 阶段三：判断是否可重试

```cpp
bool task_retryable = it->second.num_retries_left_ != 0 &&
                      !it->second.reconstructable_return_ids_.empty();
if (task_retryable) {
    release_lineage = false;  // 保留 task spec（lineage），不清理
} else {
    submissible_tasks_.erase(it);  // 不需要重试 → 直接清理
}
```

### 阶段四：Object 引用释放（RemoveLineageReference）

```cpp
it->second.reconstructable_return_ids_.erase(object_id);

// 集合空了 + task 不再 pending → 完全清理
if (it->second.reconstructable_return_ids_.empty() && !it->second.IsPending()) {
    // 释放 task 的所有参数引用
    submissible_tasks_.erase(it);  // 彻底删除 task entry
}
```

### 完整生命周期图

```
AddPendingTask()
  │
  ▼
reconstructable_return_ids_ = {ReturnId(0), ReturnId(1), ReturnId(2)}
  │  初始：包含所有声明返回值
  │
  ▼
首次执行成功 CompletePendingTask(first_execution=true)
  │
  ├─ direct return → erase
  │  ReturnId(0) 是小对象走 in_memory_store → erase
  │
  ├─ plasma return → 保留
  │  ReturnId(1), ReturnId(2) 走 Plasma → 保留
  │
  ├─ dynamic plasma return → insert
  │  dynamic_3 走 Plasma → insert
  │
  └─ streaming generator plasma return → insert
     streaming_4 走 Plasma → insert
  │
  ▼
reconstructable_return_ids_ = {ReturnId(1), ReturnId(2), dynamic_3, streaming_4}
  │
  │  判断可重试: num_retries_left_ > 0 && 非空 → 保留 task entry
  │
  ▼
消费者逐步 ray.get() + 释放引用
  │
  ├─ ReturnId(1) 引用释放 → RemoveLineageReference → erase
  ├─ ReturnId(2) 引用释放 → erase
  ├─ dynamic_3 引用释放 → erase
  └─ streaming_4 引用释放 → erase
     {} ← 空了！
  │
  ▼
reconstructable_return_ids_.empty() && !IsPending() → true
  → submissible_tasks_.erase(it)  → task entry 彻底删除
```

### 各阶段操作总结

| 阶段 | 操作 | 集合变化 |
|------|------|---------|
| 创建 | `TaskEntry` 构造 | 加入所有 `ReturnId(i)` |
| 首次执行成功 | `CompletePendingTask` | 移除 direct return，加入 dynamic/streaming Plasma return |
| 引用释放 | `RemoveLineageReference` | 逐个 erase |
| 集合清空 | 自动触发 | task entry 从 `submissible_tasks_` 彻底删除 |
| 重建重试 | `GetTaskReturnObjectsToStoreInPlasma` | 作为"需要恢复的 Object 清单"返回 |
| 最终失败 | `MarkTaskReturnObjectsFailed` | 遍历集合尝试写入 Error |

---

## 9. 重建时跳过已有 Object 的机制

核心机制是 **`reconstructable_return_ids_` + `store_in_plasma_ids` + `in_memory_store_.Put` 的不可覆盖语义**，三层保护确保已存在的 Object 不会被重复写入或被覆盖。

### 第一层：`reconstructable_return_ids_` 区分哪些 Object 需要恢复

首次执行成功时，Owner 记录下哪些返回值存在 Plasma 中。被消费者读取释放的 direct return 从集合移除，仍被引用的 Plasma return 保留在集合中。

### 第二层：`GetTaskReturnObjectsToStoreInPlasma` 决定重试时写哪里

```cpp
absl::flat_hash_set<ObjectID> TaskManager::GetTaskReturnObjectsToStoreInPlasma(
    const TaskID &task_id, bool *first_execution_out) const {
  bool first_execution = it->second.num_successful_executions_ == 0;
  if (!first_execution) {
      // 重试时：只恢复仍然在 reconstructable_return_ids_ 中的 Object
      store_in_plasma_ids = it->second.reconstructable_return_ids_;
  }
  return store_in_plasma_ids;
}
```

### 第三层：`in_memory_store_.Put` 的不可覆盖语义

`in_memory_store_.Put` **不允许覆盖已有值的 Object**：

- 如果 Object 已经在 Plasma 中存在 → `in_memory_store` 中有 `OBJECT_IN_PLASMA` 哨兵 → Put 被忽略
- 如果 Object 已丢失 → `in_memory_store` 中没有值 → Put 成功写入

### Streaming Generator 的额外机制

对于 Streaming Generator 的每个上报 Object，还有以下去重机制：

#### `InsertToStream` — 流索引级别去重

```cpp
bool ObjectRefStream::InsertToStream(const ObjectID &object_id, int64_t item_index) {
  // 已超过流末尾 → 忽略
  if (end_of_stream_index_ != -1 && item_index >= end_of_stream_index_)
    return false;
  // 索引已被消费过 → 跳过
  if (item_index < next_index_)
    return false;
  // 该 Object 已经写入过流 → 跳过
  auto [_, inserted] = refs_written_to_stream_.emplace(object_id);
  if (!inserted)
    return false;
  return true;
}
```

#### `HandleReportGeneratorItemReturns` — attempt number 去重

```cpp
if (it->second.spec_.AttemptNumber() > attempt_number) {
  // 旧 attempt 的上报，直接忽略
  execution_signal_callback(
      Status::NotFound("Stale object reports from the previous attempt."), -1);
  return false;
}

// 已消费的 index 也直接返回
if (stream_it->second.IsObjectConsumed(item_index)) {
  execution_signal_callback(Status::OK(), total_consumed);
  return false;
}
```

#### `HandleTaskReturn` + `in_memory_store_.Put` — 值级别去重

即使通过了前两层检查，`in_memory_store_.Put` 的不可覆盖语义保证已有值的 Object 不会被覆盖。

### 总结

| 机制 | 保护什么 |
|------|---------|
| `InsertToStream(item_index < next_index_)` | 流索引去重：已消费的 index 不再重复写入流 |
| `AttemptNumber > attempt_number` | 旧 attempt 去重：崩溃的旧 Executor 的延迟上报被丢弃 |
| `in_memory_store_.Put 不可覆盖` | 值去重：已有值的 Object 不会被覆盖 |
| `store_in_plasma_ids = reconstructable_return_ids_` | 只恢复仍需 Plasma 存储的 Object |

核心思路：**重建是整个 generator 重新执行**，不是只恢复单个丢失的 Object。Ray 通过多层去重确保重新执行产出的"已存在的 Object"被自然跳过，只补充真正丢失的值。

---

## 10. 重试 vs 重建：两种恢复机制对比

### 区别

| | 重试 (Retry) | 重建 (Reconstruction) |
|---|---|---|
| **触发者** | gRPC 失败 / task 执行异常 | Plasma Object 丢失被 ReferenceCounter 检测到 |
| **触发时机** | PushNormalTask 回调中 `!status.ok()` | task 已完成但返回值 Object 从 Plasma 丢失 |
| **task 状态** | task 正在执行时 worker 出问题 | task **已完成**（FINISHED）但 Object 丢失 |
| **入口函数** | `FailOrRetryPendingTask` → `RetryTaskIfPossible` | `ResubmitTask` → `SetupTaskEntryForResubmit` |
| **重试预算** | 消耗 `num_retries_left_` | 消耗同一个 `num_retries_left_` |
| **是否重新执行整个 task** | 是 | 是 |

### 三种场景

#### 场景A：Generator 运行中 + 节点宕机 → 重试

```
Generator 在 Node A 运行中
  ▼ Node A 宕机
  │
  ├─ gRPC 断开 → 回调 !status.ok()
  │   → GetWorkerFailureCause → NODE_DIED
  │   → FailOrRetryPendingTask → RetryTaskIfPossible
  │   → task 重新提交到新 worker ← "重试"
  │
  └─ Plasma Object 丢失
      → ResubmitTask → task 状态 != FINISHED/FAILED → return nullopt
      → 什么都不做（重试已在进行中）
```

**节点宕机时，"重试"先触发，"重建"发现 task 已在重试中就跳过了。**

#### 场景B：Generator 已完成 + 之后 Object 丢失 → 重建

```
Generator 在 Node A 上执行完毕 → FINISHED
  ...
  ▼ Node A 宕机 / Plasma eviction
  │
  ├─ 没有 gRPC 回调（task 早已结束）
  │
  └─ Plasma Object 丢失 → ReferenceCounter 检测到
      → ResubmitTask()
      → task 状态 == FINISHED → SetupTaskEntryForResubmit
      → async_retry_task_callback_ → 重新提交 task ← "重建"
```

#### 场景C：Generator 运行中 + Object 被 Plasma 淘汰 → 延迟重建

```
Generator 在 Node A 运行中，产出了 obj_0, obj_1
  ▼ obj_0 被 Plasma LRU 淘汰（Node A 还活着！）
  │
  ├─ 没有 gRPC 回调（generator 还在运行，连接正常）
  │
  └─ ReferenceCounter 检测到 obj_0 丢失
      → ResubmitTask()
      → IsStreamingGenerator() && status == SUBMITTED_TO_WORKER
      → QueueGeneratorForResubmit（打标记）
      → 不能立即重提交！generator 还在运行
      ...
      Generator 执行完毕 → PushTaskReply 回调
      → resubmit_generator == true
      → MarkGeneratorFailedAndResubmit → 重新提交 generator
```

### 对比总结

| 场景 | 实际触发 | 另一个机制的行为 |
|------|---------|----------------|
| Generator 运行中节点宕机 | **重试**（gRPC 失败触发） | 重建 → no-op（task 已在重试中） |
| Generator 已完成，Object 后丢失 | **重建**（ResubmitTask 触发） | 没有重试（gRPC 早已完成） |
| Generator 运行中，Object 被淘汰 | **延迟重建**（QueueGeneratorForResubmit） | 没有重试（gRPC 连接正常） |

**本质区别**：
- **重试**：响应 task **执行过程**中的失败（worker 死了、异常了）
- **重建**：响应 task **执行结果**的丢失（Object 数据丢了，需要重跑 task 恢复）
- 两者共享同一个 `num_retries_left_` 预算

---

## 11. ray.wait 与 ray.get 在不同场景下的行为

### `_next_sync` 实现分析

```python
def _next_sync(self, timeout_s=None):
    core_worker = self.worker.core_worker
    expected_ref, is_ready = core_worker.peek_object_ref_stream(self._generator_ref)

    if not is_ready:
        _, unready = ray.wait([expected_ref], timeout=timeout_s, fetch_local=False)
        if len(unready) > 0:
            return ray.ObjectRef.nil()  # ← 返回 nil，不阻塞

    ref = core_worker.try_read_next_object_ref_stream(self._generator_ref)
    ...
    return ref
```

### `on_data_ready` 中的调用

```python
# block ref 用 timeout_s=0，不会阻塞
self._pending_block_ref = self._streaming_gen._next_sync(timeout_s=0)

# metadata ref 用 timeout_s=0.1
self._pending_meta_ref = self._streaming_gen._next_sync(
    timeout_s=METADATA_WAIT_TIMEOUT_S  # 0.1秒
)

# metadata 值获取用 1秒超时
meta_with_schema_bytes = ray.get(
    self._pending_meta_ref, timeout=METADATA_GET_TIMEOUT_S  # 1.0秒
)
except ray.exceptions.GetTimeoutError:
    logger.warning("Timed out waiting for metadata...")
    break
```

### `ray.wait` 对 `ObjectRefGenerator` 的 ready 语义

`ray.wait` 对 `ObjectRefGenerator` 的判断是：**只要 generator 产出了至少一个新的 ObjectRef（即流中有新数据可读），就视为 ready。**

| 状态 | `ray.wait` 是否 ready |
|------|----------------------|
| Generator 正在执行，刚产出第 N 个 block | **ready**（流中有新 item） |
| Generator 正在执行，还没产出新 block | **不 ready**（等待中） |
| Generator 执行完毕（正常结束） | **ready**（流结束，EOF 信号） |
| Generator 执行失败 | **ready**（generator_ref 带异常） |
| Worker 宕机，Object 丢失正在重建 | **不 ready**（等重建完成） |
| 重建也失败 | **ready**（Error Object 写入，可读） |

### StreamingExecutor 中的 ray.wait

```python
# streaming_executor_state.py process_completed_tasks
ready, _ = ray.wait(
    list(active_tasks.keys()),   # 包含 ObjectRefGenerator
    num_returns=len(active_tasks),
    fetch_local=False,
    timeout=0.1,
)
```

### 三种场景下 ray.wait / ray.get 的行为

#### 场景A：Generator 运行中 + 节点宕机 → 重试

**`ray.wait` on `ObjectRefGenerator`：**

```
宕机瞬间 → 不 ready（gRPC 断开，没有新 item）
重试 task 被调度到新 worker → 执行中 → 不 ready
重试开始产出新 block → ready（流中有新 item）
如果重试也失败 → ready（generator_ref 带异常，流结束）
```

**已拿到的 `block_0`, `block_1` 的 `ray.get`：**

| Object 位置 | 状态 | `ray.get` 行为 |
|-------------|------|---------------|
| in-memory（小对象）| Owner 内存中仍有值 | 立即返回 |
| Plasma 在 Node A | 丢失 | **阻塞**，等重试重新产出同 ID 的 Object |

#### 场景B：Generator 已完成 + 之后 Object 丢失 → 重建

**`ray.wait` on `ObjectRefGenerator`：**

Generator 已经不在 active_tasks 中了，StreamingExecutor 已经不再 wait 它。

**丢失的 `block_ref` 的 `ray.get`：**

```
Object 丢失 → ray.get 阻塞
ReferenceCounter 检测到 → ResubmitTask()
task 状态 == FINISHED → SetupTaskEntryForResubmit → 重新提交
新 worker 重新执行 generator → 产出同 ID 的 Object
重建的 Object 写入 Plasma → ray.get 解除阻塞 → 返回值
```

**如果重建也失败**：

```
num_retries_left_ == 0 → FailPendingTask
→ MarkTaskReturnObjectsFailed → 写入 Error Object
→ ray.get 解除阻塞 → 抛异常
```

#### 场景C：Generator 运行中 + Object 被 Plasma 淘汰 → 延迟重建

**`ray.wait` on `ObjectRefGenerator`：**

可能 ready！因为 generator 还在运行，可能产出了新的 block_2 → `ray.wait` 返回 ready → `on_data_ready` 拿到新 block（正常）

**但 block_0 的 `ray.get`：**

```
block_0 的 Plasma 值丢失
ResubmitTask() → QueueGeneratorForResubmit（打标记）
ray.get(block_0) 阻塞 → 等待重建
Generator 继续运行...产出 block_2, block_3...（StreamingExecutor 正常消费新 block）
Generator 执行完毕 → PushTaskReply 回调
→ resubmit_generator == true
→ MarkGeneratorFailedAndResubmit → 重新提交 generator
新的 generator 执行 → 重新产出 block_0, block_1, block_2...
已存在的 Object 写入被跳过（no-op）
block_0 丢失 → 写入成功 → ray.get(block_0) 解除阻塞
```

**Python 层的超时保护**：

如果丢失的 block_0 刚好是 `_pending_block_ref` 或 `_pending_meta_ref`：
- `ray.get` 带 1 秒超时 → 超时 → `GetTimeoutError` → break → 下轮重试
- **不会永久 hang**

### 三种场景对比

| | 场景A：运行中宕机→重试 | 场景B：已完成→重建 | 场景C：运行中淘汰→延迟重建 |
|---|---|---|---|
| **谁触发** | gRPC 失败 | ReferenceCounter | ReferenceCounter |
| **ray.wait(Gen)** | 宕机时 not ready → 重试产出后 ready | 不再 wait（task 已完成） | 可能 ready（新 item 仍在产出） |
| **已拿到的 block ref** | 内存版立即返回；Plasma 版阻塞等重试 | 丢失的阻塞等重建 | 丢失的阻塞等延迟重建 |
| **能读新 block 吗** | 不能（重试中） | 不需要（已完成） | **能**（generator 还在跑） |
| **ray.get 会 hang 吗** | 短暂阻塞等重试完成 | 阻塞等重建完成 | 阻塞等 generator 结束+重建 |
| **最终失败呢** | Error Object → 抛异常 | Error Object → 抛异常 | Error Object → 抛异常 |
| **Python 层超时保护** | `ray.get(meta, timeout=1s)` | N/A | `ray.get(meta, timeout=1s)` |

---

## 附录：关键源码文件索引

| 文件 | 内容 |
|------|------|
| `src/ray/core_worker/task_submission/normal_task_submitter.cc` | Normal task 提交、PushNormalTask 回调 |
| `src/ray/core_worker/task_submission/normal_task_submitter.h` | NormalTaskSubmitter 类定义 |
| `src/ray/core_worker/task_manager.cc` | TaskManager 核心逻辑（CompletePendingTask, FailOrRetryPendingTask, ResubmitTask 等） |
| `src/ray/core_worker/task_manager.h` | TaskEntry, ObjectRefStream 定义 |
| `src/ray/core_worker/core_worker.cc` | ExecuteTask, HandlePushTask |
| `src/ray/core_worker/task_execution/task_receiver.cc` | TaskReceiver, QueueTaskForExecution |
| `src/ray/protobuf/core_worker.proto` | PushTask RPC 定义 |
| `src/ray/core_worker_rpc_client/core_worker_client.cc` | CoreWorkerClient::PushNormalTask |
| `python/ray/_private/object_ref_generator.py` | ObjectRefGenerator, `_next_sync` |
| `python/ray/data/_internal/execution/interfaces/physical_operator.py` | DataOpTask, on_data_ready |
| `python/ray/data/_internal/execution/streaming_executor_state.py` | process_completed_tasks, ray.wait 调用 |
