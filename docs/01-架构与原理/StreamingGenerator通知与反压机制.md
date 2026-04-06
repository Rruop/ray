# Ray Streaming Generator 通知机制与背压设计深度解析

## 概述

本文档详细解析 Ray Streaming Generator 的完整通知机制：从 Executor yield 对象，到 Owner 端处理，再到 ray.wait / ray.get 如何被唤醒，以及背压（backpressure）机制的设计原理。重点剖析 `execution_signal_callback` 的生命周期和"存入-取出"设计的必然性。

---

## 1. 整体架构

### 1.1 三方参与者

```
┌──────────────────┐     RPC: ReportGeneratorItemReturns     ┌──────────────────────┐
│  Executor Worker │ ──────────────────────────────────────► │  Owner Worker         │
│  (执行 generator │                                        │  (提交 generator task │
│   task 的进程)   │                                        │   的进程)             │
│                  │                                        │                       │
│  yield obj_0     │  report(index=0, object_id=X0)         │  ObjectRefStream      │
│  yield obj_1     │  report(index=1, object_id=X1)         │  ┌─ next_index_=0     │
│  ...             │  ...                                   │  ├─ refs_written_      │
│                  │                                        │  └─ end_of_stream_=-1 │
│                  │                                        │                       │
│                  │                                        │  MemoryStore          │
│                  │                                        │  ┌─ X0 → RayObject    │
│                  │                                        │  └─ X1 → RayObject    │
└──────────────────┘                                        └───────┬──────────────┘
                                                                    │
                                              ray.wait / ray.get ◄──┘
```

- **Executor Worker**：执行 generator task 的进程，yield 时通过 RPC 上报
- **Owner Worker**：提交 generator task 的进程，管理 ObjectRefStream 和 MemoryStore
- **用户代码**：通过 ray.wait / ray.get 消费 generator 产出的对象

### 1.2 ObjectID 的确定性生成——关键前提

Generator 的每个输出 ObjectID 是**确定性的**，在 Task 提交时就能算出来：

```cpp
// task_manager.h:147
ObjectID GetObjectRefAtIndex(int64_t generator_index) const;
```

给定 `generator_id` 和 `index`，就能算出对应的 `ObjectID`。这意味着：**在 Executor 产出任何值之前，Owner 已经知道未来的每个 ObjectID 是什么。**

---

## 2. Executor 端：yield 时上报

当 Executor 执行 `yield value` 时，它通过 `CoreWorker::ReportGeneratorItemReturns` 向 Owner 发送 RPC。

### 2.1 发送端代码

位于 `src/ray/core_worker/core_worker.cc:3396-3458`：

```cpp
Status CoreWorker::ReportGeneratorItemReturns(
    const std::pair<ObjectID, std::shared_ptr<RayObject>> &dynamic_return_object,
    const ObjectID &generator_id,
    const rpc::Address &caller_address,          // ← Owner 的 gRPC 地址
    int64_t item_index,
    uint64_t attempt_number,
    const std::shared_ptr<GeneratorBackpressureWaiter> &waiter) {
  rpc::ReportGeneratorItemReturnsRequest request;
  request.mutable_worker_addr()->CopyFrom(rpc_address_);
  request.set_item_index(item_index);
  request.set_generator_id(generator_id.Binary());
  request.set_attempt_number(attempt_number);

  // 通过 caller_address 获取 Owner 的 gRPC 客户端
  auto client = core_worker_client_pool_->GetOrConnect(caller_address);

  if (!dynamic_return_object.first.IsNil()) {
    SerializeReturnObject(dynamic_return_object.first,
                          dynamic_return_object.second,
                          request.mutable_returned_object());
    // 清理借用引用
    ReferenceCounterInterface::ReferenceTableProto borrowed_refs;
    reference_counter_->PopAndClearLocalBorrowers(
        {dynamic_return_object.first}, &borrowed_refs, &deleted);
    memory_store_->Delete(deleted);
  }

  waiter->IncrementObjectGenerated();

  // 发送 RPC，callback 在 Owner 回复时执行
  client->ReportGeneratorItemReturns(
      std::move(request),
      [waiter, generator_id, return_id, item_index](
          const Status &status, const rpc::ReportGeneratorItemReturnsReply &reply) {
        int64_t num_objects_consumed = 0;
        if (status.ok()) {
          num_objects_consumed = reply.total_num_object_consumed();
        } else {
          // RPC 失败，不做背压，让 Executor 继续执行
          num_objects_consumed = waiter->TotalObjectGenerated();
        }
        waiter->HandleObjectReported(num_objects_consumed);
      });

  // 阻塞等待，直到 Owner 回复 RPC 且消费数允许继续
  return waiter->WaitUntilObjectConsumed();
}
```

**关键点：**
- `caller_address` 是 Owner 的 gRPC 地址，由 Task 提交时确定
- RPC 回复 callback 通知 `waiter` 消费数变化
- `waiter->WaitUntilObjectConsumed()` 会阻塞 Executor，直到 Owner 回复且背压条件满足

### 2.2 Executor 的阻塞等待

Executor 端的阻塞逻辑在 `GeneratorBackpressureWaiter` 中。当 Owner 不回复 RPC 时，`client->ReportGeneratorItemReturns` 的回调不会执行，`waiter->HandleObjectReported` 不会被调用，`WaitUntilObjectConsumed()` 就一直阻塞。

**挂起的 RPC 连接本身就是通知通道**——不需要额外的推送机制。

---

## 3. Owner 端：三层调用链

```
gRPC 框架路由
  │  收到 ReportGeneratorItemReturns RPC
  ▼
CoreWorker::HandleReportGeneratorItemReturns()     ← 第 1 层：RPC 入口（core_worker.cc:3460）
  │  构造 execution_signal_callback lambda
  │  委托给 TaskManager
  ▼
TaskManager::HandleReportGeneratorItemReturns()    ← 第 2 层：业务逻辑（task_manager.cc:780）
  │  写入 ObjectRefStream
  │  注册引用计数
  │  放入 MemoryStore
  │  背压判断：存入或立即执行 callback
  ▼
ObjectRefStream::InsertToStream()                  ← 第 3 层：数据结构操作
HandleTaskReturn() → MemoryStore::Put()            ← 唤醒 ray.wait / ray.get
```

### 3.1 第 1 层：CoreWorker RPC 入口

位于 `src/ray/core_worker/core_worker.cc:3460-3485`：

```cpp
void CoreWorker::HandleReportGeneratorItemReturns(
    rpc::ReportGeneratorItemReturnsRequest request,
    rpc::ReportGeneratorItemReturnsReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  auto generator_id = ObjectID::FromBinary(request.generator_id());
  auto worker_id = WorkerID::FromBinary(request.worker_addr().worker_id());

  task_manager_->HandleReportGeneratorItemReturns(
      request,
      /*execution_signal_callback=*/
      // ★ 这个 lambda 就是"回复 RPC"的句柄
      [reply,
       worker_id = std::move(worker_id),
       generator_id = std::move(generator_id),
       send_reply_callback = std::move(send_reply_callback)](
          const Status &status, int64_t total_num_object_consumed) {
        if (!status.ok()) {
          RAY_CHECK_EQ(total_num_object_consumed, -1);
        }
        reply->set_total_num_object_consumed(total_num_object_consumed);
        send_reply_callback(status, nullptr, nullptr);  // ← 回复 RPC！
      });
}
```

**关键点：** `execution_signal_callback` 是一个 lambda，它捕获了 gRPC 的 `send_reply_callback`。调用它 = 回复 RPC = 通知 Executor 继续。

`send_reply_callback` 是 gRPC 框架提供的一次性回复句柄，只能调用一次。

### 3.2 第 2 层：TaskManager 业务逻辑

位于 `src/ray/core_worker/task_manager.cc:780-879`：

```cpp
bool TaskManager::HandleReportGeneratorItemReturns(
    const rpc::ReportGeneratorItemReturnsRequest &request,
    const ExecutionSignalCallback &execution_signal_callback) {
  const auto &generator_id = ObjectID::FromBinary(request.generator_id());
  const auto &task_id = generator_id.TaskId();
  int64_t item_index = request.item_index();
  int64_t attempt_number = request.attempt_number();
  auto backpressure_threshold = -1;

  // ─── 步骤 1：获取背压阈值 ───
  {
    absl::MutexLock lock(&mu_);
    auto it = submissible_tasks_.find(task_id);
    if (it != submissible_tasks_.end()) {
      backpressure_threshold = it->second.spec_.GeneratorBackpressureNumObjects();
      if (it->second.spec_.AttemptNumber() > attempt_number) {
        // 过期 attempt 的报告，直接丢弃
        execution_signal_callback(
            Status::NotFound("Stale object reports from the previous attempt."), -1);
        return false;
      }
    }
  }

  const auto store_in_plasma_ids = GetTaskReturnObjectsToStoreInPlasma(task_id);

  absl::MutexLock lock(&object_ref_stream_ops_mu_);
  auto stream_it = object_ref_streams_.find(generator_id);
  if (stream_it == object_ref_streams_.end()) {
    execution_signal_callback(Status::NotFound("Stream is already deleted"), -1);
    return false;
  }

  // ─── 步骤 2：写入 ObjectRefStream ───
  if (request.has_returned_object()) {
    const rpc::ReturnObject &returned_object = request.returned_object();
    const auto object_id = ObjectID::FromBinary(returned_object.object_id());

    auto index_not_used_yet = stream_it->second.InsertToStream(object_id, item_index);

    // ─── 步骤 3：注册引用计数 ───
    if (index_not_used_yet) {
      reference_counter_.OwnDynamicStreamingTaskReturnRef(object_id, generator_id);
      num_objects_written += 1;
    }
    reference_counter_.UpdateObjectPendingCreation(object_id, false);

    // ─── 步骤 4：放入 MemoryStore → 唤醒 ray.wait / ray.get ───
    StatusOr<bool> put_res =
        HandleTaskReturn(object_id,
                         returned_object,
                         NodeID::FromBinary(request.worker_addr().node_id()),
                         /*store_in_plasma=*/store_in_plasma_ids.contains(object_id));
  }

  // ─── 步骤 5：背压判断 ───
  auto total_generated = stream_it->second.TotalNumObjectWritten();
  auto total_consumed = stream_it->second.TotalNumObjectConsumed();

  if (stream_it->second.IsObjectConsumed(item_index)) {
    // 对象已被消费，立即通知
    execution_signal_callback(Status::OK(), total_consumed);
    return false;
  }

  if (backpressure_threshold != -1 &&
      (item_index - stream_it->second.LastConsumedIndex()) >= backpressure_threshold) {
    // ★ 背压！存入 callback，不回复 RPC
    auto signal_it = ref_stream_execution_signal_callbacks_.find(generator_id);
    if (signal_it == ref_stream_execution_signal_callbacks_.end()) {
      execution_signal_callback(Status::NotFound("Stream is deleted."), -1);
    } else {
      signal_it->second.push_back(execution_signal_callback);  // ← 存入队列
    }
  } else {
    // 不背压，立即回复 RPC
    execution_signal_callback(Status::OK(), total_consumed);   // ← 立即执行
  }
  return num_objects_written != 0;
}
```

五个步骤的职责总结：

| 步骤 | 操作 | 目的 |
|------|------|------|
| 1 | 获取 `backpressure_threshold` | 判断是否需要背压 |
| 2 | `ObjectRefStream.InsertToStream()` | 标记"index N 的 ObjectID 已写入" |
| 3 | `reference_counter.OwnDynamicStreamingTaskReturnRef()` | 注册引用计数，防止 GC |
| 4 | `HandleTaskReturn()` → `MemoryStore.Put()` | 放入 MemoryStore，唤醒等待者 |
| 5 | 背压判断：存入或立即执行 callback | 控制 Executor 产出速率 |

### 3.3 步骤 4 的展开：MemoryStore.Put() 如何唤醒等待者

`HandleTaskReturn` 的完整逻辑（`task_manager.cc:551-621`）：

```cpp
StatusOr<bool> TaskManager::HandleTaskReturn(const ObjectID &object_id,
                                             const rpc::ReturnObject &return_object,
                                             const NodeID &worker_node_id,
                                             bool store_in_plasma) {
  if (return_object.in_plasma()) {
    // 大对象：在 Plasma 中，MemoryStore 放入 OBJECT_IN_PLASMA 占位符
    reference_counter_.UpdateObjectPinnedAtRaylet(object_id, worker_node_id);
    in_memory_store_.Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
                         object_id,
                         reference_counter_.HasReference(object_id));
  } else {
    // 小对象：直接放入 MemoryStore
    RayObject object(data_buffer, metadata_buffer, nested_refs, ...);
    if (store_in_plasma) {
      put_in_local_plasma_callback_(object, object_id);
    } else {
      in_memory_store_.Put(object, object_id, reference_counter_.HasReference(object_id));
    }
  }
  // ... nested refs 处理
  return direct_return;
}
```

当 `MemoryStore.Put()` 被调用时，内部的唤醒逻辑：

```cpp
// memory_store.cc:172-243（简化）
void CoreWorkerMemoryStore::Put(const RayObject &object,
                                const ObjectID &object_id,
                                const bool has_reference) {
  absl::MutexLock lock(&mu_);

  // 检查是否有同步 GetRequest 等待者
  auto object_request_iter = object_get_requests_.find(object_id);
  if (object_request_iter != object_get_requests_.end()) {
    for (auto &get_request : object_request_iter->second) {
      get_request->Set(object_id, object_entry);  // ← 投递给等待者
    }
  }

  // 检查是否有异步 GetAsync 等待者
  auto async_callback_it = object_async_get_requests_.find(object_id);
  if (async_callback_it != object_async_get_requests_.end()) {
    async_callbacks = std::move(callbacks);
    // 在锁外执行回调
    io_context_.post([...]() { cb(object_entry); });
  }

  // 存入 objects_
  EmplaceObjectAndUpdateStats(object_id, object_entry);
}
```

`GetRequest::Set()` 的唤醒：

```cpp
// memory_store.cc:101-113
void GetRequest::Set(const ObjectID &object_id, std::shared_ptr<RayObject> object) {
  std::scoped_lock<std::mutex> lock(mutex_);
  objects_.emplace(object_id, object);
  if (objects_.size() == num_objects_ ||
      (abort_if_any_object_is_exception_ && object->IsException() &&
       !object->IsInPlasmaError())) {
    is_ready_ = true;
    cv_.notify_all();  // ← 唤醒 ray.wait / ray.get 的阻塞等待
  }
}
```

**这就是"产出 → 通知"的核心环节：对象通过 Put 写入 MemoryStore，GetRequest 被唤醒，ray.wait / ray.get 得到通知。**

---

## 4. Python 端的通知流程

### 4.1 ray.wait 对 ObjectRefGenerator 的处理

位于 `python/ray/_raylet.pyx:3252-3293`：

```python
def wait(self, object_refs_or_generators, int num_returns,
         int64_t timeout_ms, c_bool fetch_local):
    object_refs = []
    for ref_or_generator in object_refs_or_generators:
        if isinstance(ref_or_generator, ObjectRefGenerator):
            # ★ 在调用 wait 之前，先获取 generator 的下一个预期 ObjectID
            object_refs.append(ref_or_generator._get_next_ref())
        else:
            object_refs.append(ref_or_generator)

    wait_ids = ObjectRefsToVector(object_refs)
    with nogil:
        op_status = CCoreWorkerProcess.GetCoreWorker().Wait(
            wait_ids, num_returns, timeout_ms, &results, fetch_local)

    ready, not_ready = [], []
    for i, object_ref_or_generator in enumerate(object_refs_or_generators):
        if results[i]:
            ready.append(object_ref_or_generator)
        else:
            not_ready.append(object_ref_or_generator)
    return ready, not_ready
```

**流程：**
1. 对每个 `ObjectRefGenerator`，调用 `_get_next_ref()` 获取下一个预期的 `ObjectRef`
2. 把这些 `ObjectRef` 传给 C++ `CoreWorker::Wait()`
3. `CoreWorker::Wait()` → `MemoryStore::Wait()` 等待对象就绪
4. 当 Executor yield 时，`MemoryStore.Put()` 唤醒等待

`_get_next_ref()` 的实现（`object_ref_generator.py:178-186`）：

```python
def _get_next_ref(self) -> "ray.ObjectRef":
    """Return the next reference from a generator.

    Note that the ObjectID generated from a generator
    is always deterministic.
    """
    self.worker.check_connected()
    core_worker = self.worker.core_worker
    return core_worker.peek_object_ref_stream(self._generator_ref)[0]
```

### 4.2 ray.get 对 ObjectRefGenerator 的处理

`ray.get()` 不允许直接传入 `ObjectRefGenerator`（`worker.py:2950`）：

```python
if isinstance(object_refs, ObjectRefGenerator):
    return object_refs  # 直接返回，不做 get
```

用户通过迭代 generator 逐个 `ray.get()`：

```python
gen = streaming_task.remote()   # 返回 ObjectRefGenerator
for ref in gen:                  # __next__ 调用 _next_sync()
    value = ray.get(ref)         # 对单个 ObjectRef 调用 ray.get()
```

### 4.3 `_next_sync()` 的完整流程

位于 `python/ray/_private/object_ref_generator.py:188-242`：

```python
def _next_sync(self, timeout_s=None) -> "ray.ObjectRef":
    core_worker = self.worker.core_worker

    # 1. 获取下一个预期的 ObjectRef
    expected_ref, is_ready = core_worker.peek_object_ref_stream(self._generator_ref)

    # 2. 如果对象还没就绪，通过 ray.wait 等待
    if not is_ready:
        _, unready = ray.wait([expected_ref], timeout=timeout_s, fetch_local=False)
        if len(unready) > 0:
            return ray.ObjectRef.nil()  # 超时，返回 nil

    # 3. 对象就绪，消费 ObjectRefStream 的下一个 index
    try:
        ref = core_worker.try_read_next_object_ref_stream(self._generator_ref)
        assert not ref.is_nil()
    except ObjectRefStreamEndOfStreamError:
        # Stream 结束处理
        if self._generator_task_raised:
            raise StopIteration from None
        try:
            ray.get(self._generator_ref)  # 检查 generator task 是否有异常
        except Exception:
            self._generator_task_raised = True
            return self._generator_ref
        else:
            raise StopIteration from None
    return ref
```

---

## 5. CoreWorker 与 TaskManager 的分层关系

### 5.1 TryReadObjectRefStream 的三层调用链

```
Python (Cython)
  │  core_worker.try_read_next_object_ref_stream(generator_id)
  │     ↓ Cython 绑定
  ▼
CoreWorker::TryReadObjectRefStream(generator_id, &c_object_ref)    ← 适配层
  │  ObjectID → rpc::ObjectReference 类型转换
  │     ↓
  ▼
TaskManager::TryReadObjectRefStream(generator_id, &object_id)     ← 业务逻辑层
  │  操作 ObjectRefStream + 背压信号触发
  │     ↓
  ▼
ObjectRefStream::TryReadNextItem(&object_id)                      ← 数据结构层
     移动 next_index_，返回 ObjectID
```

### 5.2 CoreWorker 版本的代码

位于 `src/ray/core_worker/core_worker.cc:3302-3310`：

```cpp
Status CoreWorker::TryReadObjectRefStream(const ObjectID &generator_id,
                                          rpc::ObjectReference *object_ref_out) {
  ObjectID object_id;
  // 委托给 TaskManager，返回 ObjectID
  const auto &status = task_manager_->TryReadObjectRefStream(generator_id, &object_id);

  // ★ 类型转换：ObjectID → rpc::ObjectReference
  RAY_CHECK(object_ref_out != nullptr);
  object_ref_out->set_object_id(object_id.Binary());                  // 二进制 ID
  object_ref_out->mutable_owner_address()->CopyFrom(rpc_address_);    // Owner 地址
  return status;
}
```

**转换只发生在 CoreWorker 这一层。** `rpc::ObjectReference` 是 protobuf 消息，除了 `object_id`，还需要 `owner_address`（Owner 的 gRPC 地址）。`ObjectID` 只是 28 字节的标识符，不含地址信息。CoreWorker 持有 `rpc_address_`，所以只有它能补全 owner_address。

### 5.3 TaskManager 版本的代码

位于 `src/ray/core_worker/task_manager.cc:637-679`：

```cpp
Status TaskManager::TryReadObjectRefStream(const ObjectID &generator_id,
                                           ObjectID *object_id_out) {
  // 1. 获取背压阈值
  auto backpressure_threshold = 0;
  {
    absl::MutexLock lock(&mu_);
    auto it = submissible_tasks_.find(generator_id.TaskId());
    if (it != submissible_tasks_.end()) {
      backpressure_threshold = it->second.spec_.GeneratorBackpressureNumObjects();
    }
  }

  absl::MutexLock lock(&object_ref_stream_ops_mu_);

  // 2. 消费 stream 的下一个 index
  auto stream_it = object_ref_streams_.find(generator_id);
  auto status = stream_it->second.TryReadNextItem(object_id_out);

  // 3. ★ 消费成功后，检查是否需要解除背压
  if (status.ok()) {
    auto total_generated = stream_it->second.TotalNumObjectWritten();
    auto total_consumed = stream_it->second.TotalNumObjectConsumed();
    auto total_unconsumed = total_generated - total_consumed;
    if (backpressure_threshold != -1 && total_unconsumed < backpressure_threshold) {
      auto it = ref_stream_execution_signal_callbacks_.find(generator_id);
      if (it != ref_stream_execution_signal_callbacks_.end()) {
        for (const auto &execution_signal : it->second) {
          // ★ 触发被挂起的 callback → 回复 RPC → 唤醒 Executor
          execution_signal(Status::OK(), total_consumed);
        }
        it->second.clear();
      }
    }
  }
  return status;
}
```

### 5.4 ObjectRefStream 版本的代码

位于 `src/ray/core_worker/task_manager.cc:129-155`：

```cpp
Status ObjectRefStream::TryReadNextItem(ObjectID *object_id_out) {
  *object_id_out = GetObjectRefAtIndex(next_index_);
  if (IsFinished()) {
    // next_index_ == end_of_stream_index_ → EOF
    return Status::ObjectRefEndOfStream("");
  }

  auto it = refs_written_to_stream_.find(*object_id_out);
  if (it != refs_written_to_stream_.end()) {
    // index 已写入 → 消费
    total_num_object_consumed_ += 1;
    next_index_ += 1;
  } else {
    // index 未写入 → 返回 Nil，调用者应重试
    *object_id_out = ObjectID::Nil();
  }
  return Status::OK();
}
```

### 5.5 三层对比

| | CoreWorker 版本 | TaskManager 版本 | ObjectRefStream 版本 |
|---|---|---|---|
| **文件** | `core_worker.cc:3302` | `task_manager.cc:637` | `task_manager.cc:129` |
| **输入** | `ObjectID generator_id` | `ObjectID generator_id` | 无（成员方法） |
| **输出** | `rpc::ObjectReference*` (protobuf) | `ObjectID*` | `ObjectID*` |
| **做什么** | 类型转换 + 委托 | 业务逻辑 + 背压触发 | 纯数据结构操作 |
| **线程安全** | 无锁（委托给 TM） | 加锁 `object_ref_stream_ops_mu_` | 无锁（由 TM 的锁保护） |

### 5.6 同样的分层模式也体现在其他方法

```cpp
// StreamingGeneratorIsFinished：1:1 委托，无类型转换
bool CoreWorker::StreamingGeneratorIsFinished(const ObjectID &generator_id) const {
  return task_manager_->StreamingGeneratorIsFinished(generator_id);
}

// PeekObjectRefStream：委托 + ObjectID → rpc::ObjectReference 转换
std::pair<rpc::ObjectReference, bool> CoreWorker::PeekObjectRefStream(
    const ObjectID &generator_id) {
  auto [object_id, ready] = task_manager_->PeekObjectRefStream(generator_id);
  rpc::ObjectReference object_ref;
  object_ref.set_object_id(object_id.Binary());
  object_ref.mutable_owner_address()->CopyFrom(rpc_address_);
  return {object_ref, ready};
}
```

### 5.7 为什么要有 CoreWorker 这一层

Ray 的所有对外 API 统一挂在 `CoreWorker` 上，`CoreWorker` 是 Python/Cython 与 C++ 内部实现的**唯一入口**。好处：

1. **类型隔离**：内部用 `ObjectID`，外部用 `rpc::ObjectReference`（protobuf），转换逻辑集中在一处
2. **权限/校验集中**：CoreWorker 可以统一做参数校验、日志、权限检查
3. **调用者无需知道内部结构**：Cython 层只依赖 `CoreWorker` 的公共接口，不需要直接持有 `TaskManager` 的引用

---

## 6. execution_signal_callback 的完整生命周期

### 6.1 类型定义

```cpp
// task_manager.h:58
using ExecutionSignalCallback = std::function<void(Status, int64_t)>;
```

### 6.2 存储

```cpp
// task_manager.h:749
// Map from generator_id → vector of pending callbacks
absl::flat_hash_map<ObjectID, std::vector<ExecutionSignalCallback>>
    ref_stream_execution_signal_callbacks_ ABSL_GUARDED_BY(object_ref_stream_ops_mu_);
```

### 6.3 生命周期：创建 → 存储 → 触发

#### 阶段 A：callback 的创建（CoreWorker 层）

```
core_worker.cc:3460-3485

CoreWorker::HandleReportGeneratorItemReturns(request, reply, send_reply_callback)
  │
  └─ 构造 lambda 作为 execution_signal_callback：
     │
     │  [reply, send_reply_callback](Status status, int64_t total_consumed) {
     │      reply->set_total_num_object_consumed(total_consumed);
     │      send_reply_callback(status, nullptr, nullptr);  // ← 回复 gRPC！
     │  }
     │
     └─ 传给 TaskManager::HandleReportGeneratorItemReturns(request, callback)
```

**这个 lambda 的作用就是"回复 RPC"。** 调用了它，Executor 那边才会收到 RPC 回复，才能继续 yield。

#### 阶段 B：callback 的存储（TaskManager::HandleReportGeneratorItemReturns）

```
task_manager.cc:863-874

if (需要背压：item_index - LastConsumedIndex() >= backpressure_threshold) {
    // ★ 不立即回复 RPC → Executor 被阻塞
    signal_it->second.push_back(execution_signal_callback);   // ← 存入队列
} else {
    // 不需要背压 → 立即回复 RPC → Executor 继续
    execution_signal_callback(Status::OK(), total_consumed);  // ← 立即执行
}
```

#### 阶段 C：callback 的触发（TaskManager::TryReadObjectRefStream）

```
task_manager.cc:659-675

if (消费成功 && 未消费数 < 背压阈值) {
    auto it = ref_stream_execution_signal_callbacks_.find(generator_id);
    if (it != ref_stream_execution_signal_callbacks_.end()) {
        for (const auto &execution_signal : it->second) {
            execution_signal(Status::OK(), total_consumed);  // ← 执行被挂起的 callback
        }
        it->second.clear();                                  // ← 清空队列
    }
}
```

**这里取出的是阶段 B 存入的同一个 callback 对象。** 调用它的效果就是回复之前挂起的 RPC，让 Executor 恢复执行。

### 6.4 两处方法的对比

| | HandleReportGeneratorItemReturns | TryReadObjectRefStream |
|---|---|---|
| **角色** | 生产端（callback 的写入者） | 消费端（callback 的触发者） |
| **对 callback 做什么** | 创建并存入 `ref_stream_execution_signal_callbacks_` 队列 | 从队列取出并执行 |
| **callback 本身是什么** | CoreWorker 构造的 lambda，功能是 `send_reply_callback(status)` = 回复 RPC | 同一个 lambda 对象 |
| **执行时机** | 不背压时立即执行；背压时存起来 | 用户消费后，未消费数低于阈值时取出执行 |
| **效果** | 回复 RPC → Executor 继续/恢复 | 同上 |

---

## 7. 为什么必须存入 callback，不能消费时直接通知

### 7.1 本质原因：RPC 回复是唯一的通知通道

callback 里包装的是 gRPC 的 `send_reply_callback`：

```cpp
// core_worker.cc:3469-3484
[reply, send_reply_callback](Status status, int64_t total_consumed) {
    reply->set_total_num_object_consumed(total_consumed);
    send_reply_callback(status, nullptr, nullptr);  // ← 这就是 gRPC 的 reply
}
```

`send_reply_callback` 是 gRPC 框架给的**一次性回复句柄**。调用它 = 回复 RPC。Executor 那边在等这个回复：

```cpp
// core_worker.cc:3431-3453  Executor 端
client->ReportGeneratorItemReturns(
    std::move(request),
    [waiter, ...](const Status &status, const rpc::ReportGeneratorItemReturnsReply &reply) {
        // ← 这个 lambda 在 RPC 回复到达后才执行
        waiter->HandleObjectReported(num_objects_consumed);
    });

// 阻塞等待，直到 Owner 回复 RPC 且消费数允许继续
return waiter->WaitUntilObjectConsumed();
```

### 7.2 Ray Worker 之间只有请求-响应模式

```
Executor ──── RPC Request ──────────► Owner
         ◄─── RPC Response ──────────  ← 唯一能"通知"Executor 的方式
```

Ray 的 Worker 之间使用**请求-响应**模式的 gRPC，没有反向推送通道。如果 Owner 在收到 RPC 时不回复（背压），那这条 RPC 连接就挂着。以后想通知 Executor 继续，**唯一的方式就是回复这条挂起的 RPC**。

而回复 RPC 的手段就是那个 `send_reply_callback`。如果不存下来，消费时就没有东西可以回复了。

### 7.3 对比两种方案

**方案 A（当前设计）：存 callback，消费时取出执行**

```
Executor                         Owner
   │                                │
   │──RPC(index=N)──────────────►  │  背压，不回复
   │   等待...                      │  callback 存入队列
   │   等待...                      │
   │   等待...                      │  用户消费 → 取出 callback
   │◄──RPC Reply ◄────────────────│  执行 callback = 回复 RPC
   │  恢复执行                      │
```

**方案 B（假设的"直接通知"）：消费时发新 RPC 通知 Executor**

```
Executor                         Owner
   │                                │
   │──RPC(index=N)──────────────►  │  背压，先回复一个"请等待"
   │◄──RPC Reply("wait")──────────│
   │  进入等待循环                   │
   │  ...                           │
   │  ...                           │  用户消费
   │◄──新 RPC("resume")───────────│  Owner 主动推送恢复信号
   │  恢复执行                      │
```

方案 B 的问题：

1. **需要新通道**：Owner 要能主动向 Executor 发 RPC，需要额外的 gRPC 端点、连接管理
2. **Executor 端变复杂**：不能简单阻塞在 `WaitUntilObjectConsumed()` 上，需要轮询或维护额外的事件循环
3. **竞态条件**："wait" 回复和 "resume" 通知之间可能有时序问题——如果 Owner 刚回复"wait"但消费紧接着发生了，通知和等待逻辑容易出错
4. **多一次网络往返**：先回复"wait"，再发"resume"，多了两次网络通信

### 7.4 当前设计的优雅之处

当前设计把**挂起的 RPC 连接**本身当作通知管道，零额外成本：

- **存 callback = 保持 RPC 连接挂着**（TCP 连接还活着，只是没回复）
- **执行 callback = 回复 RPC**（利用已有的 TCP 连接，零额外开销）
- **不需要新通道**、**不需要新协议**、**不需要 Executor 端额外逻辑**

本质上这就是 **long polling** 思想——客户端发请求，服务端延迟回复，回复本身就承载了通知。

### 7.5 一个类比

想象你打电话给客服（Executor → Owner RPC）：

- **不背压**：客服说"好的，请继续" → 立即回复 RPC → 电话挂断
- **背压（当前设计）**：客服说"请别挂，我处理完叫你" → 不挂电话，callback 存着 → 处理完后在同一次通话里说"好了继续" → 回复 RPC
- **背压（方案 B）**：客服说"我稍后回你" → 先挂电话 → 处理完后再打回去 → 需要"回拨"能力（新通道）

当前设计选择了"别挂电话"，因为"回拨"需要额外基础设施且更复杂。

---

## 8. 完整时序图

```
Executor                   Owner CoreWorker              Owner TaskManager
   │                            │                            │
   │ yield obj_0                │                            │
   │──ReportRPC(index=0)──────►│                            │
   │                            │──HandleReportGenerator───►│
   │                            │  ItemReturns(request,      │
   │                            │  callback_A = [reply RPC]) │
   │                            │                            │
   │                            │                    InsertToStream(X0, 0)
   │                            │                    OwnDynamicReturnRef(X0)
   │                            │                    HandleTaskReturn(X0)
   │                            │                    → MemoryStore.Put(X0)
   │                            │                       → GetRequest::Set()
   │                            │                       → cv_.notify_all()  ◄── 唤醒 ray.wait/get
   │                            │                            │
   │                            │                    未消费数=0 < 阈值
   │                            │                    callback_A(OK, 0)       ◄── 立即回复
   │◄──RPC Reply(OK, 0)────────│◄──────────────────────┘    │
   │ 继续 yield                 │                            │
   │                            │                            │
   │ yield obj_1                │                            │
   │──ReportRPC(index=1)──────►│                            │
   │                            │──HandleReportGenerator───►│
   │                            │  ItemReturns(request,      │
   │                            │  callback_B = [reply RPC]) │
   │                            │                            │
   │                            │                    InsertToStream(X1, 1)
   │                            │                    HandleTaskReturn(X1)
   │                            │                    → MemoryStore.Put(X1)
   │                            │                            │
   │                            │                    未消费数=1 < 阈值
   │                            │                    callback_B(OK, 1)
   │◄──RPC Reply(OK, 1)────────│◄──────────────────────┘    │
   │ 继续 yield                 │                            │
   │ ...                        │                            │
   │                            │                            │
   │ yield obj_N (N >= 阈值)    │                            │
   │──ReportRPC(index=N)──────►│                            │
   │                            │──HandleReportGenerator───►│
   │                            │  ItemReturns(request,      │
   │                            │  callback_C = [reply RPC]) │
   │                            │                            │
   │                            │                    InsertToStream(XN, N)
   │                            │                    HandleTaskReturn(XN)
   │                            │                    → MemoryStore.Put(XN)
   │                            │                            │
   │                            │                    未消费数 >= 阈值 ★ 背压！
   │                            │                    push_back(callback_C)
   │   ← RPC 未回复，           │                    到 ref_stream_execution_
   │     Executor 阻塞等待      │                    signal_callbacks_[gen_id]
   │     ...                    │                            │
   │                            │                            │
   │                            │     用户调用 TryReadObjectRefStream
   │                            │                            │
   │                            │──TryReadObjectRefStream──►│
   │                            │                            │
   │                            │                    TryReadNextItem()
   │                            │                    → next_index_ += 1
   │                            │                    → total_consumed += 1
   │                            │                            │
   │                            │                    未消费数 < 阈值 ★ 解除背压！
   │                            │                    遍历 callbacks:
   │                            │                    callback_C(OK, consumed)
   │                            │                       │    │ ← 延迟回复！
   │◄──RPC Reply(OK, consumed)─│◄──────────────────────┘    │
   │ 恢复执行                   │                            │
```

---

## 9. 总结

| 问题 | 答案 |
|------|------|
| 新对象 ObjectID 如何提前知道？ | **确定性生成**：`generator_id + index` 唯一确定 ObjectID，在 yield 之前就已计算好 |
| ray.wait 怎么等待未产出的对象？ | 通过 `_get_next_ref()` → `PeekObjectRefStream()` 获取下一个 ObjectID，然后对该 ObjectID 调用 `MemoryStore::Wait()` |
| ray.get 怎么等待？ | 不直接对 Generator 调用。迭代 Generator 时 `_next_sync()` 先 `ray.wait([expected_ref])` 等待就绪，再 `try_read_next_object_ref_stream` 消费 |
| 产出到通知的关键环节？ | `MemoryStore::Put()` → 检查 `object_get_requests_` → `GetRequest::Set()` → `cv_.notify_all()` |
| 背压如何工作？ | `execution_signal_callback` 挂起/触发，控制 Executor 的产出速率 |
| Memory Store 的角色？ | **通知中枢**：不管对象来自普通 Task 还是 Streaming Generator，都通过 `Put()` → `GetRequest`/`async_callback` 统一唤醒等待者 |
| CoreWorker 和 TaskManager 的关系？ | CoreWorker 是适配层（类型转换 + 委托），TaskManager 是业务逻辑层 |
| ObjectID → rpc::ObjectReference 在哪转换？ | 只在 CoreWorker 层，补全 `owner_address` |
| 为什么必须存 callback？ | 挂起的 RPC 连接是唯一通知通道，`send_reply_callback` 是回复 RPC 的唯一手段，不存下来消费时无法通知 |
