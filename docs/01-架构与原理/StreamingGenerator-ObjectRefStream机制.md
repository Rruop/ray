# Ray Streaming Generator ObjectRefStream 机制详解

## 1. 整体架构概览

Streaming Generator 是 Ray 中支持任务逐步流式返回结果的机制。核心数据结构是 `ObjectRefStream`，由 `TaskManager` 管理，存在于**任务 Owner（调用方）** 的 CoreWorker 中。

### 角色分工

| 角色 | 说明 |
|------|------|
| **Owner（调用方）** | 提交 generator task，持有 `generator_id`（即 `ReturnId(0)`），拥有返回对象的引用计数，维护 `object_ref_streams_`，接收 `ReportGeneratorItemReturns` gRPC |
| **Executor（执行方）** | 被调度运行 generator task，yield 值时调用 `ReportGeneratorItemReturns` 主动发 gRPC 给 Owner |
| **Consumer** | 通过 `ObjectRefGenerator` 迭代消费流式结果，调用 `peek`/`try_read` 等 API |

### 数据流简图

```
Executor                    Owner/Caller              Consumer
  |                             |                        |
  |--- yield item_i ----------->|  InsertToStream        |
  |   (gRPC, 不回复则阻塞)       |  HandleTaskReturn      |
  |                             |                        |
  |                             |<--- next() -----------|
  |                             |  TryReadNextItem       |
  |                             |  释放 queued callbacks |
  |<--- gRPC reply -------------|                        |
  |  waiter 唤醒，继续 yield     |                        |
```

---

## 2. 创建阶段：AddPendingTask

**文件**: `src/ray/core_worker/task_manager.cc:237`

提交任务时，用同一个 `generator_id = spec.ReturnId(0)` 做两件事：

1. 生成 `returned_refs[0]` — 返回给调用方的外部句柄
2. 创建 `object_ref_streams_[generator_id]` — 内部的流式缓冲区

```cpp
// task_manager.cc:267-316
size_t num_returns = spec.NumReturns();  // streaming generator 为 1
std::vector<rpc::ObjectReference> returned_refs;
returned_refs.reserve(num_returns);
for (size_t i = 0; i < num_returns; i++) {
    auto return_id = spec.ReturnId(i);
    // ... AddOwnedObject, 设置 ref 字段 ...
    returned_refs.push_back(std::move(ref));
}

// task_manager.cc:321-332
if (spec.IsStreamingGenerator()) {
    const auto generator_id = spec.ReturnId(0);  // 与 returned_refs[0] 中的 ID 相同
    absl::MutexLock lock(&object_ref_stream_ops_mu_);
    auto inserted =
        object_ref_streams_.emplace(generator_id, ObjectRefStream(generator_id));
    ref_stream_execution_signal_callbacks_.emplace(
        generator_id, std::vector<ExecutionSignalCallback>());
    RAY_CHECK(inserted.second);
}

return returned_refs;  // 返回给调用方，调用方用 generator_id 作"钥匙"
```

**关键关系**：`returned_refs[0]` 是流的入口 ticket，`object_ref_streams_[generator_id]` 是实际的管道。调用方拿到的 ref 包含 `generator_id`，后续所有操作（`PeekObjectRefStream`、`TryReadObjectRefStream`、`DeleteObjectRefStream` 等）都传这个 ref 里的 `generator_id` 回来查找对应的 `ObjectRefStream`。

---

## 3. ObjectRefStream 类定义

**文件**: `src/ray/core_worker/task_manager.h:70-199`

```cpp
using ExecutionSignalCallback = std::function<void(Status, int64_t)>;

class ObjectRefStream {
 public:
  explicit ObjectRefStream(ObjectID generator_id)
      : generator_task_id_(generator_id.TaskId()),
        generator_id_(std::move(generator_id)) {}

  Status TryReadNextItem(ObjectID *object_id_out);
  bool IsFinished() const;
  std::pair<ObjectID, bool> PeekNextItem();
  bool IsObjectConsumed(int64_t item_index) const;
  bool InsertToStream(const ObjectID &object_id, int64_t item_index);
  bool TemporarilyInsertToStreamIfNeeded(const ObjectID &object_id);
  void MarkEndOfStream(int64_t item_index, ObjectID *object_id_in_last_index);
  absl::flat_hash_set<ObjectID> GetItemsUnconsumed() const;
  std::vector<ObjectID> PopUnconsumedItems();

  int64_t LastConsumedIndex() const { return next_index_ - 1; }
  int64_t EofIndex() const { return end_of_stream_index_; }
  int64_t TotalNumObjectWritten() const { return total_num_object_written_; }
  int64_t TotalNumObjectConsumed() const { return total_num_object_consumed_; }

 private:
  ObjectID GetObjectRefAtIndex(int64_t generator_index) const;

  TaskID generator_task_id_;
  ObjectID generator_id_;

  // Refs that are temporarily owned (index not known yet).
  absl::flat_hash_set<ObjectID> temporarily_owned_refs_;
  // Refs already written to stream.
  absl::flat_hash_set<ObjectID> refs_written_to_stream_;
  // The last index of the stream. -1 means EOF not reached.
  int64_t end_of_stream_index_ = -1;
  // The next index to read. If next_index_ == end_of_stream_index_, EOF.
  int64_t next_index_ = 0;
  // The maximum index seen from the executor.
  int64_t max_index_seen_ = -1;
  int64_t total_num_object_written_{};
  int64_t total_num_object_consumed_{};
};
```

### 确定性 ObjectID 计算

**文件**: `src/ray/core_worker/task_manager.cc:231-235`

```cpp
ObjectID ObjectRefStream::GetObjectRefAtIndex(int64_t generator_index) const {
  RAY_CHECK_LT(generator_index, RayConfig::instance().max_num_generator_returns());
  // Index 1 is reserved for the first task return from a generator task itself.
  return ObjectID::FromIndex(generator_task_id_, 2 + generator_index);
}
```

**关键设计**：Object ID 由 `task_id + index` 确定性推导，双方独立算出同一个值。Executor 只负责报告"第 i 个值的内容就绪"，Owner 端据此标记 ready 并存储实际数据。Consumer 甚至可以在 Executor 还没写进来之前就通过 `PeekNextItem()` 预算出下一个要读的 ObjectID。

这与 `TaskSpecification::StreamingGeneratorReturnId` 一致：

```cpp
// task_spec.cc:223-229
ObjectID TaskSpecification::StreamingGeneratorReturnId(size_t generator_index) const {
  RAY_CHECK_EQ(NumReturns(), 1UL);
  RAY_CHECK_LT(generator_index, RayConfig::instance().max_num_generator_returns());
  return ObjectID::FromIndex(TaskId(), 2 + generator_index);
}
```

---

## 4. 写入阶段：Executor → Owner

### 4a. Executor 发送 gRPC

**文件**: `src/ray/core_worker/core_worker.cc:3156-3217`

当 Executor yield 一个值时，调用 `ReportGeneratorItemReturns`：

```cpp
Status CoreWorker::ReportGeneratorItemReturns(
    const std::pair<ObjectID, std::shared_ptr<RayObject>> &dynamic_return_object,
    const ObjectID &generator_id,
    const rpc::Address &owner_address,
    int64_t item_index,
    uint64_t attempt_number,
    const std::shared_ptr<GeneratorBackpressureWaiter> &waiter) {
  rpc::ReportGeneratorItemReturnsRequest request;
  request.mutable_worker_addr()->CopyFrom(rpc_address_);
  request.set_item_index(item_index);
  request.set_generator_id(generator_id.Binary());
  request.set_attempt_number(attempt_number);
  auto client = core_worker_client_pool_->GetOrConnect(owner_address);

  if (!dynamic_return_object.first.IsNil()) {
    SerializeReturnObject(dynamic_return_object.first,
                          dynamic_return_object.second,
                          request.mutable_returned_object());
    // 清理 executor 端的 borrower 和 memory store
    std::vector<ObjectID> deleted;
    ReferenceCounterInterface::ReferenceTableProto borrowed_refs;
    reference_counter_->PopAndClearLocalBorrowers(
        {dynamic_return_object.first}, &borrowed_refs, &deleted);
    memory_store_->Delete(deleted);
  }
  const auto return_id = dynamic_return_object.first;

  waiter->IncrementObjectGenerated();  // 递增 generated 计数

  // 发送 gRPC，回调中处理 ack
  client->ReportGeneratorItemReturns(
      std::move(request),
      [waiter, generator_id, return_id, item_index](
          const Status &status, const rpc::ReportGeneratorItemReturnsReply &reply) {
        int64_t num_objects_consumed = 0;
        if (status.ok()) {
          num_objects_consumed = reply.total_num_object_consumed();
        } else {
          // gRPC 失败则假设全部已消费，解除背压
          num_objects_consumed = waiter->TotalObjectGenerated();
        }
        waiter->HandleObjectReported(num_objects_consumed);
      });

  // 背压阻塞，等消费者消费后才能继续 yield
  return waiter->WaitUntilObjectConsumed();
}
```

### 4b. Owner 接收 gRPC

**文件**: `src/ray/core_worker/core_worker.cc:3220-3250`

`execution_signal_callback` 就是 `send_reply_callback` 的包装。调用它 = 发送 gRPC reply 给 executor。

```cpp
void CoreWorker::HandleReportGeneratorItemReturns(
    rpc::ReportGeneratorItemReturnsRequest request,
    rpc::ReportGeneratorItemReturnsReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  auto generator_id = ObjectID::FromBinary(request.generator_id());
  auto worker_id = WorkerID::FromBinary(request.worker_addr().worker_id());
  task_manager_->HandleReportGeneratorItemReturns(
      request,
      [reply, worker_id, generator_id, send_reply_callback](
          const Status &status, int64_t total_num_object_consumed) {
        if (!status.ok()) {
          RAY_CHECK_EQ(total_num_object_consumed, -1);
        }
        reply->set_total_num_object_consumed(total_num_object_consumed);
        send_reply_callback(status, nullptr, nullptr);
      });
}
```

**注意**：这里的 Owner 是 object 的 owner，即提交任务（调用 `.remote()`）的那个 core worker，不是执行任务的 worker。

### 4c. TaskManager 核心写入逻辑

**文件**: `src/ray/core_worker/task_manager.cc:790-890`

```cpp
bool TaskManager::HandleReportGeneratorItemReturns(
    const rpc::ReportGeneratorItemReturnsRequest &request,
    const ExecutionSignalCallback &execution_signal_callback) {
  const auto &generator_id = ObjectID::FromBinary(request.generator_id());
  const auto &task_id = generator_id.TaskId();
  int64_t item_index = request.item_index();
  int64_t attempt_number = request.attempt_number();
  auto backpressure_threshold = -1;

  {
    absl::MutexLock lock(&mu_);
    auto it = submissible_tasks_.find(task_id);
    if (it != submissible_tasks_.end()) {
      backpressure_threshold = it->second.spec_.GeneratorBackpressureNumObjects();
      if (it->second.spec_.AttemptNumber() > attempt_number) {
        // 过时 attempt 的报告，忽略
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

  size_t num_objects_written = 0;
  if (request.has_returned_object()) {
    const rpc::ReturnObject &returned_object = request.returned_object();
    const auto object_id = ObjectID::FromBinary(returned_object.object_id());

    // ★ 核心：写入 stream
    auto index_not_used_yet = stream_it->second.InsertToStream(object_id, item_index);

    if (index_not_used_yet) {
      reference_counter_.OwnDynamicStreamingTaskReturnRef(object_id, generator_id);
      num_objects_written += 1;
    }
    // 对象已就绪，可被 fetch
    reference_counter_.UpdateObjectPendingCreation(object_id, false);
    // ★ 存储实际值到 memory store 或 plasma
    StatusOr<bool> put_res =
        HandleTaskReturn(object_id, returned_object,
                         NodeID::FromBinary(request.worker_addr().node_id()),
                         store_in_plasma_ids.contains(object_id));
  }

  // ★★★ 背压逻辑 ★★★
  auto total_generated = stream_it->second.TotalNumObjectWritten();
  auto total_consumed = stream_it->second.TotalNumObjectConsumed();

  if (stream_it->second.IsObjectConsumed(item_index)) {
    // 已被消费 → 立即回复
    execution_signal_callback(Status::OK(), total_consumed);
    return false;
  }

  if (backpressure_threshold != -1 &&
      (item_index - stream_it->second.LastConsumedIndex()) >= backpressure_threshold) {
    // 背压：延迟回复（executor 会阻塞在 WaitUntilObjectConsumed）
    auto signal_it = ref_stream_execution_signal_callbacks_.find(generator_id);
    if (signal_it == ref_stream_execution_signal_callbacks_.end()) {
      execution_signal_callback(Status::NotFound("Stream is deleted."), -1);
    } else {
      signal_it->second.push_back(execution_signal_callback);  // 排队
    }
  } else {
    // 未达阈值 → 立即回复
    execution_signal_callback(Status::OK(), total_consumed);
  }
  return num_objects_written != 0;
}
```

### 4d. InsertToStream — 实际写入

**文件**: `src/ray/core_worker/task_manager.cc:180-215`

```cpp
bool ObjectRefStream::InsertToStream(const ObjectID &object_id, int64_t item_index) {
  // ★ 断言：executor 发来的 object_id 必须等于确定性计算值
  RAY_CHECK_EQ(object_id, GetObjectRefAtIndex(item_index));
  if (end_of_stream_index_ != -1 && item_index >= end_of_stream_index_) {
    return false;  // EOF 之后忽略
  }
  if (item_index < next_index_) {
    return false;  // 已被消费，忽略
  }
  if (temporarily_owned_refs_.find(object_id) != temporarily_owned_refs_.end()) {
    temporarily_owned_refs_.erase(object_id);
  }
  auto [_, inserted] = refs_written_to_stream_.emplace(object_id);
  if (!inserted) {
    return false;  // 已写入过
  }
  max_index_seen_ = std::max(max_index_seen_, item_index);
  total_num_object_written_ += 1;
  return true;
}
```

### 4e. 序列化决定 in_plasma

**文件**: `src/ray/core_worker/common.cc:57-87`

`SerializeReturnObject` 在 Executor 端决定数据是 direct 还是 plasma：

```cpp
void SerializeReturnObject(const ObjectID &object_id,
                           const std::shared_ptr<RayObject> &return_object,
                           rpc::ReturnObject *return_object_proto) {
  return_object_proto->set_object_id(object_id.Binary());

  if (return_object->GetData() != nullptr && return_object->GetData()->IsPlasmaBuffer()) {
    // ★ 数据在 plasma 中 → 只设标记，不传数据
    return_object_proto->set_in_plasma(true);
  } else {
    // ★ 数据在内存中 → 把 data 和 metadata 拷进 gRPC
    if (return_object->GetData() != nullptr) {
      return_object_proto->set_data(return_object->GetData()->Data(),
                                    return_object->GetData()->Size());
    }
    if (return_object->GetMetadata() != nullptr) {
      return_object_proto->set_metadata(return_object->GetMetadata()->Data(),
                                        return_object->GetMetadata()->Size());
    }
  }
  // nested refs 和 direct_transport_metadata ...
}
```

**关键**：`IsPlasmaBuffer()` 判断数据是否写入到了 plasma store。如果 Executor 把 yield 的值 put 到了本地 plasma store，则 `in_plasma=true`，gRPC 只传标记不传数据；否则把数据直接序列化进 gRPC。

### 4f. HandleTaskReturn — Owner 端存储实际数据

**文件**: `src/ray/core_worker/task_manager.cc:550-610`

```cpp
StatusOr<bool> TaskManager::HandleTaskReturn(const ObjectID &object_id,
                                             const rpc::ReturnObject &return_object,
                                             const NodeID &worker_node_id,
                                             bool store_in_plasma) {
  bool direct_return = false;
  reference_counter_.UpdateObjectSize(object_id, return_object.size());
  const auto nested_refs =
      VectorFromProtobuf<rpc::ObjectReference>(return_object.nested_inlined_refs());

  if (return_object.in_plasma()) {
    // ★ Plasma 对象：数据在 Executor 的 plasma store 中
    // Owner 只记录位置，放一个哨兵
    reference_counter_.UpdateObjectPinnedAtRaylet(object_id, worker_node_id);
    in_memory_store_.Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
                         object_id,
                         reference_counter_.HasReference(object_id));
  } else {
    // ★ Direct 对象：数据在 gRPC 请求中
    // Owner 从 gRPC 反序列化，存到自己的内存或本地 plasma
    std::shared_ptr<LocalMemoryBuffer> data_buffer;
    if (!return_object.data().empty()) {
      data_buffer = std::make_shared<LocalMemoryBuffer>(
          const_cast<uint8_t *>(
              reinterpret_cast<const uint8_t *>(return_object.data().data())),
          return_object.data().size());
    }
    std::shared_ptr<LocalMemoryBuffer> metadata_buffer;
    if (!return_object.metadata().empty()) {
      metadata_buffer = std::make_shared<LocalMemoryBuffer>(
          const_cast<uint8_t *>(
              reinterpret_cast<const uint8_t *>(return_object.metadata().data())),
          return_object.metadata().size());
    }

    RayObject object(data_buffer, metadata_buffer, nested_refs,
                     /*copy_data=*/false,
                     reference_counter_.GetTensorTransport(object_id));
    if (store_in_plasma) {
      // Owner 的本地 plasma store
      Status s = put_in_local_plasma_callback_(object, object_id);
      if (!s.ok()) {
        return s;
      }
    } else {
      // Owner 的内存 store
      in_memory_store_.Put(object, object_id,
                           reference_counter_.HasReference(object_id));
      direct_return = true;
    }
  }
  // ...
  return direct_return;
}
```

### 4g. 数据存储位置总结

| 场景 | 数据存在哪 | gRPC 传什么 | Owner 端做什么 |
|------|-----------|-----------|--------------|
| **Direct return**（小对象） | Executor 内存 | `data` + `metadata` 完整传 | Owner 从 gRPC 提取数据，存入 `in_memory_store_` 或本地 plasma |
| **Plasma return**（大对象） | **Executor 的 plasma store** | 只传 `in_plasma=true`，无数据 | Owner 记录 `worker_node_id` 位置，放 `OBJECT_IN_PLASMA` 哨兵到 `in_memory_store_`，后续 `ray.get()` 时从 plasma 拉取 |

对于 streaming generator，和普通 task 的机制完全一样——写入走 `HandleReportGeneratorItemReturns` → `HandleTaskReturn` 同一条路径，只是数据是逐步流式到达而非一次性返回。

---

## 5. 读取阶段：Consumer → Owner

### 5a. PeekNextItem — 预看

**文件**: `src/ray/core_worker/task_manager.cc:161-168`

```cpp
std::pair<ObjectID, bool> ObjectRefStream::PeekNextItem() {
  const auto &object_id = GetObjectRefAtIndex(next_index_);  // 确定性计算
  if (refs_written_to_stream_.find(object_id) == refs_written_to_stream_.end()) {
    return {object_id, false};  // 还没写入，但 ID 已知
  } else {
    return {object_id, true};   // 已写入，ready
  }
}
```

**注意**：即使 object 还没写入，`PeekNextItem` 也能通过确定性计算返回预测的 `object_id`。`ready` 只是判断该 ID 是否已在 `refs_written_to_stream_` 中。

**TaskManager::PeekObjectRefStream** (`task_manager.cc:730-744`)：

```cpp
std::pair<ObjectID, bool> TaskManager::PeekObjectRefStream(const ObjectID &generator_id) {
  absl::MutexLock lock(&object_ref_stream_ops_mu_);
  auto stream_it = object_ref_streams_.find(generator_id);
  RAY_CHECK(stream_it != object_ref_streams_.end());
  const auto &result = stream_it->second.PeekNextItem();
  // 临时拥有这个 ref（可能 executor 还没报告）
  TemporarilyOwnGeneratorReturnRefIfNeededInternal(result.first, generator_id);
  return result;
}
```

**CoreWorker::PeekObjectRefStream** (`core_worker.cc`)：

```cpp
std::pair<rpc::ObjectReference, bool> CoreWorker::PeekObjectRefStream(
    const ObjectID &generator_id) {
  auto [object_id, ready] = task_manager_->PeekObjectRefStream(generator_id);
  rpc::ObjectReference object_ref;
  object_ref.set_object_id(object_id.Binary());
  object_ref.mutable_owner_address()->CopyFrom(rpc_address_);
  return {object_ref, ready};
}
```

这里的 `generator_id` 就是 `AddPendingTask` 中 `spec.ReturnId(0)` 创建的那个 ID，与 `returned_refs[0]` 中的 ID 相同。返回的 `object_ref` 中的 `object_id` 是从 `ObjectRefStream` 中确定性计算出来的，不是 Executor 指定的。

### 5b. TryReadNextItem — 实际读取

**文件**: `src/ray/core_worker/task_manager.cc:128-154`

```cpp
Status ObjectRefStream::TryReadNextItem(ObjectID *object_id_out) {
  *object_id_out = GetObjectRefAtIndex(next_index_);  // 确定性计算下一个 ID
  if (IsFinished()) {
    RAY_CHECK(next_index_ == end_of_stream_index_);
    return Status::ObjectRefEndOfStream("");  // 流结束
  }
  auto it = refs_written_to_stream_.find(*object_id_out);
  if (it != refs_written_to_stream_.end()) {
    total_num_object_consumed_ += 1;
    next_index_ += 1;  // 推进读指针
  } else {
    // 还没写入，返回 Nil
    *object_id_out = ObjectID::Nil();
  }
  return Status::OK();
}
```

### 5c. TaskManager::TryReadObjectRefStream — 读取 + 释放背压

**文件**: `src/ray/core_worker/task_manager.cc:636-674`

```cpp
Status TaskManager::TryReadObjectRefStream(const ObjectID &generator_id,
                                           ObjectID *object_id_out) {
  auto backpressure_threshold = 0;
  {
    absl::MutexLock lock(&mu_);
    auto it = submissible_tasks_.find(generator_id.TaskId());
    if (it != submissible_tasks_.end()) {
      backpressure_threshold = it->second.spec_.GeneratorBackpressureNumObjects();
    }
  }

  absl::MutexLock lock(&object_ref_stream_ops_mu_);
  auto stream_it = object_ref_streams_.find(generator_id);
  RAY_CHECK(stream_it != object_ref_streams_.end());
  auto status = stream_it->second.TryReadNextItem(object_id_out);

  // ★ 读取成功后，检查是否需要释放背压
  if (status.ok()) {
    auto total_generated = stream_it->second.TotalNumObjectWritten();
    auto total_consumed = stream_it->second.TotalNumObjectConsumed();
    auto total_unconsumed = total_generated - total_consumed;
    if (backpressure_threshold != -1 && total_unconsumed < backpressure_threshold) {
      auto it = ref_stream_execution_signal_callbacks_.find(generator_id);
      if (it != ref_stream_execution_signal_callbacks_.end()) {
        // ★ 触发所有排队的回调 → 发送 gRPC reply → executor 解除阻塞
        for (const auto &execution_signal : it->second) {
          execution_signal(Status::OK(), total_consumed);
        }
        it->second.clear();
      }
    }
  }
  return status;
}
```

### 5d. CoreWorker C++ API

**文件**: `src/ray/core_worker/core_worker.cc`

```cpp
Status CoreWorker::TryReadObjectRefStream(const ObjectID &generator_id,
                                          rpc::ObjectReference *object_ref_out) {
  ObjectID object_id;
  const auto &status = task_manager_->TryReadObjectRefStream(generator_id, &object_id);
  RAY_CHECK(object_ref_out != nullptr);
  object_ref_out->set_object_id(object_id.Binary());
  object_ref_out->mutable_owner_address()->CopyFrom(rpc_address_);
  return status;
}

std::pair<rpc::ObjectReference, bool> CoreWorker::PeekObjectRefStream(
    const ObjectID &generator_id) {
  auto [object_id, ready] = task_manager_->PeekObjectRefStream(generator_id);
  rpc::ObjectReference object_ref;
  object_ref.set_object_id(object_id.Binary());
  object_ref.mutable_owner_address()->CopyFrom(rpc_address_);
  return {object_ref, ready};
}
```

---

## 6. 背压机制

### 6a. 数据结构

**文件**: `src/ray/core_worker/task_manager.h`

```cpp
// The consumer side of object ref stream should signal the executor
// to resume execution via signal callbacks (i.e., RPC reply).
absl::flat_hash_map<ObjectID, std::vector<ExecutionSignalCallback>>
    ref_stream_execution_signal_callbacks_ ABSL_GUARDED_BY(object_ref_stream_ops_mu_);
```

其中 `ExecutionSignalCallback = std::function<void(Status, int64_t)>`：
- `Status`: `OK` 表示 object 将被消费/已消费；`NotFound` 表示流已删除或过期
- `int64_t`: `total_num_object_consumed`。如果 status 非 OK，值为 -1

### 6b. 背压协议文档

来自 `task_manager.h` 中的注释：

```
Backpressure Impl
-----------------
Streaming generator optionally supports backpressure when
`generator_backpressure_num_objects` is included in a task spec.

Executor Side:
- When a new object is yielded, executor sends a gRPC request that
  contains an object size and records total_object_generated.
- If a total_object_generated - total_object_consumed > threshold,
  it blocks a thread and pauses execution. The consumer communicates
  `object_consumed` (via gRPC reply) when objects are consumed from it,
  and the execution resumes.
- If a gRPC request fails, the executor assumes all the objects are
  consumed and resume execution.

Client Side:
- If object_generated - object_consumed < threshold, it sends a reply that
  contains `object_consumed` to an executor immediately.
- If object_generated - object_consumed > threshold, it doesn't reply
  until objects are consumed via TryReadObjectRefStream.
- If objects are not going to be consumed (e.g., generator is deleted
  or objects are already consumed), it replies immediately.
```

### 6c. GeneratorBackpressureWaiter — Executor 端阻塞

**文件**: `src/ray/core_worker/generator_waiter.h`

```cpp
class GeneratorBackpressureWaiter {
 public:
  GeneratorBackpressureWaiter(int64_t generator_backpressure_num_objects,
                              std::function<Status()> check_signals);
  Status WaitUntilObjectConsumed();
  Status WaitAllObjectsReported();
  void IncrementObjectGenerated();
  void HandleObjectReported(int64_t total_objects_consumed);
  int64_t TotalObjectConsumed() const;
  int64_t TotalObjectGenerated() const;

 private:
  mutable absl::Mutex mutex_;
  absl::CondVar backpressure_cond_var_;
  absl::CondVar all_objects_reported_cond_var_;
  const int64_t backpressure_threshold_;
  const std::function<Status()> check_signals_;
  int64_t total_objects_generated_ = 0;
  int64_t num_object_reports_in_flight_ = 0;
  int64_t total_objects_consumed_ = 0;
};
```

**文件**: `src/ray/core_worker/generator_waiter.cc`

```cpp
GeneratorBackpressureWaiter::GeneratorBackpressureWaiter(
    int64_t generator_backpressure_num_objects, std::function<Status()> check_signals)
    : backpressure_threshold_(generator_backpressure_num_objects),
      check_signals_(std::move(check_signals)) {
  RAY_CHECK_NE(generator_backpressure_num_objects, 0);
  RAY_CHECK(check_signals_ != nullptr);
}

Status GeneratorBackpressureWaiter::WaitUntilObjectConsumed() {
  if (backpressure_threshold_ < 0) {
    RAY_CHECK_EQ(backpressure_threshold_, -1);
    // Backpressure disabled if backpressure_threshold_ == -1.
    return Status::OK();
  }

  absl::MutexLock lock(&mutex_);
  auto return_status = Status::OK();
  auto total_object_unconsumed = total_objects_generated_ - total_objects_consumed_;
  if (total_object_unconsumed >= backpressure_threshold_) {
    // ★ 阻塞，等待条件变量唤醒
    while (total_object_unconsumed >= backpressure_threshold_) {
      backpressure_cond_var_.WaitWithTimeout(&mutex_, absl::Seconds(1));
      total_object_unconsumed = total_objects_generated_ - total_objects_consumed_;
      return_status = check_signals_();  // 检查 Python 信号（如 SIGINT）
      if (!return_status.ok()) {
        break;
      }
    }
  }
  return return_status;
}

void GeneratorBackpressureWaiter::HandleObjectReported(int64_t total_objects_consumed) {
  absl::MutexLock lock(&mutex_);
  num_object_reports_in_flight_--;
  if (num_object_reports_in_flight_ <= 0) {
    all_objects_reported_cond_var_.SignalAll();
  }
  total_objects_consumed_ = std::max(total_objects_consumed, total_objects_consumed_);
  auto total_object_unconsumed = total_objects_generated_ - total_objects_consumed_;
  if (total_object_unconsumed < backpressure_threshold_) {
    backpressure_cond_var_.SignalAll();  // ★ 唤醒阻塞的 executor
  }
}

void GeneratorBackpressureWaiter::IncrementObjectGenerated() {
  absl::MutexLock lock(&mutex_);
  total_objects_generated_ += 1;
  num_object_reports_in_flight_++;
}
```

### 6d. 背压释放流程

execution signal callback 本质就是 `ReportGeneratorItemReturns` gRPC 的 `send_reply_callback`。

1. 当 Owner 背压时（未消费数 >= threshold），把 `send_reply_callback` 存入 `ref_stream_execution_signal_callbacks_` 队列，**不回复 gRPC**
2. Executor 在 `WaitUntilObjectConsumed()` 上阻塞
3. Consumer 调用 `TryReadObjectRefStream` 消费一个 item 后，如果未消费数 < threshold，Owner 触发所有排队的 callback（发送 gRPC reply）
4. Executor 收到 reply，`HandleObjectReported` 更新 `total_objects_consumed_`，`backpressure_cond_var_.SignalAll()` 唤醒，继续 yield

### 6e. 流删除触发所有 pending callback

**文件**: `src/ray/core_worker/task_manager.cc`

```cpp
bool TaskManager::TryDelObjectRefStreamInternal(const ObjectID &generator_id) {
  auto signal_it = ref_stream_execution_signal_callbacks_.find(generator_id);
  if (signal_it != ref_stream_execution_signal_callbacks_.end()) {
    for (const auto &execution_signal : signal_it->second) {
      execution_signal(Status::NotFound("Stream is deleted."), -1);
    }
    ref_stream_execution_signal_callbacks_.erase(signal_it);
  }
  // ... pop unconsumed items, release refs ...
}
```

---

## 7. 流结束标记

### 7a. MarkEndOfStream

**文件**: `src/ray/core_worker/task_manager.cc:218-229`

```cpp
void ObjectRefStream::MarkEndOfStream(int64_t item_index,
                                      ObjectID *object_id_in_last_index) {
  if (end_of_stream_index_ != -1) return;
  // NOTE: If the task returns a nondeterministic number of values, the second
  // try may return fewer values than the first try. If the first try fails
  // mid-execution, then on a successful second try, when we mark the end of
  // the stream here, any extra unconsumed returns from the first try will be
  // dropped.
  end_of_stream_index_ = std::max(next_index_, item_index);
  auto end_of_stream_id = GetObjectRefAtIndex(end_of_stream_index_);
  *object_id_in_last_index = end_of_stream_id;
}
```

**文件**: `src/ray/core_worker/task_manager.cc:760-780`

```cpp
void TaskManager::MarkEndOfStream(const ObjectID &generator_id,
                                  int64_t end_of_stream_index) {
  absl::MutexLock lock(&object_ref_stream_ops_mu_);
  ObjectID last_object_id;
  auto stream_it = object_ref_streams_.find(generator_id);
  if (stream_it == object_ref_streams_.end()) return;
  stream_it->second.MarkEndOfStream(end_of_stream_index, &last_object_id);
  if (!last_object_id.IsNil()) {
    reference_counter_.OwnDynamicStreamingTaskReturnRef(last_object_id, generator_id);
    // 在末尾放一个哨兵对象
    RayObject error(rpc::ErrorType::END_OF_STREAMING_GENERATOR);
    in_memory_store_.Put(error, last_object_id,
                         reference_counter_.HasReference(last_object_id));
  }
}
```

### 7b. CompletePendingTask 中触发

当 generator task 完成时（`CompletePendingTask`），Owner 标记流结束：

**文件**: `src/ray/core_worker/task_manager.cc:1078-1086`

```cpp
if (spec.IsStreamingGenerator()) {
    const auto generator_id = ObjectID::FromBinary(reply.return_objects(0).object_id());
    if (first_execution) {
      ObjectID last_ref_in_stream;
      MarkEndOfStream(generator_id, reply.streaming_generator_return_ids_size());
    }
    // ...
}
```

---

## 8. Python 端消费

**文件**: `python/ray/_private/object_ref_generator.py`

### 8a. ObjectRefGenerator 类

```python
@PublicAPI
class ObjectRefGenerator:
    def __init__(self, generator_ref: "ray.ObjectRef", worker: "Worker"):
        self._generator_ref = generator_ref  # 即 generator_id
        self._generator_task_raised = False
        self.worker = worker
```

### 8b. _next_sync — 同步读取

```python
def _next_sync(self, timeout_s=None) -> "ray.ObjectRef":
    core_worker = self.worker.core_worker

    # 1. Peek — 预算出下一个 ObjectRef（即使还未写入）
    expected_ref, is_ready = core_worker.peek_object_ref_stream(self._generator_ref)

    if not is_ready:
        # 2. 等待对象就绪
        _, unready = ray.wait([expected_ref], timeout=timeout_s, fetch_local=False)
        if len(unready) > 0:
            return ray.ObjectRef.nil()  # 超时返回 nil

    try:
        # 3. 读取 — 消费一个 item，触发背压释放
        ref = core_worker.try_read_next_object_ref_stream(self._generator_ref)
        assert not ref.is_nil()
    except ObjectRefStreamEndOfStreamError:
        # 流结束处理
        if self._generator_task_raised:
            raise StopIteration from None
        try:
            ray.get(self._generator_ref)
        except Exception:
            self._generator_task_raised = True
            return self._generator_ref
        else:
            raise StopIteration from None
    return ref
```

### 8c. _next_async — 异步读取

```python
async def _next_async(self, timeout_s=None):
    core_worker = self.worker.core_worker
    ref, is_ready = core_worker.peek_object_ref_stream(self._generator_ref)

    if not is_ready:
        _, unready = await asyncio.wait(
            [asyncio.create_task(self._suppress_exceptions(ref))], timeout=timeout_s)
        if len(unready) > 0:
            return ray.ObjectRef.nil()

    try:
        ref = core_worker.try_read_next_object_ref_stream(self._generator_ref)
        assert not ref.is_nil()
    except ObjectRefStreamEndOfStreamError:
        if self._generator_task_raised:
            raise StopAsyncIteration from None
        try:
            await self._generator_ref
        except Exception:
            self._generator_task_raised = True
            return self._generator_ref
        else:
            raise StopAsyncIteration from None
    return ref
```

### 8d. 其他 API

```python
def completed(self) -> "ray.ObjectRef":
    """返回一个当 generator task 完成时 ready 的 ref"""
    return self._generator_ref

def next_ready(self) -> bool:
    """判断 next(gen) 的结果是否就绪"""
    if self.is_finished():
        return False
    expected_ref, is_ready = core_worker.peek_object_ref_stream(self._generator_ref)
    if is_ready:
        return True
    ready, _ = ray.wait([expected_ref], timeout=0, fetch_local=False)
    return len(ready) > 0

def is_finished(self) -> bool:
    """判断 generator 是否结束"""
    finished = core_worker.is_object_ref_stream_finished(self._generator_ref)
    if finished:
        if self._generator_task_raised:
            return True
        else:
            try:
                ray.get(self._generator_ref)
            except Exception:
                return False
            else:
                return True
    else:
        return False

def __del__(self):
    if hasattr(self.worker, "core_worker"):
        self.worker.core_worker.async_delete_object_ref_stream(self._generator_ref)
```

### 8e. Cython 绑定

**文件**: `python/ray/_raylet.pyx`

```cython
def try_read_next_object_ref_stream(self, ObjectRef generator_id):
    cdef:
        CObjectID c_generator_id = generator_id.native()
        CObjectReference c_object_ref
    with nogil:
        check_status(
            CCoreWorkerProcess.GetCoreWorker().TryReadObjectRefStream(
                c_generator_id, &c_object_ref))
    return ObjectRef(
        c_object_ref.object_id(),
        c_object_ref.owner_address().SerializeAsString(),
        "",
        skip_adding_local_ref=True)

def peek_object_ref_stream(self, ObjectRef generator_id):
    cdef:
        CObjectID c_generator_id = generator_id.native()
        pair[CObjectReference, c_bool] c_object_ref_and_is_ready_pair
    with nogil:
        c_object_ref_and_is_ready_pair = (
                CCoreWorkerProcess.GetCoreWorker().PeekObjectRefStream(
                    c_generator_id))
    return (ObjectRef(
                c_object_ref_and_is_ready_pair.first.object_id(),
                c_object_ref_and_is_ready_pair.first.owner_address().SerializeAsString()),
            c_object_ref_and_is_ready_pair.second)

def is_object_ref_stream_finished(self, ObjectRef generator_id):
    cdef:
        CObjectID c_generator_id = generator_id.native()
        c_bool finished
    with nogil:
        finished = CCoreWorkerProcess.GetCoreWorker().StreamingGeneratorIsFinished(
            c_generator_id)
    return finished

def async_delete_object_ref_stream(self, ObjectRef generator_id):
    cdef:
        CObjectID c_generator_id = generator_id.native()
    with nogil:
        CCoreWorkerProcess.GetCoreWorker().AsyncDelObjectRefStream(c_generator_id)
```

---

## 9. RPC 协议

**文件**: `src/ray/protobuf/core_worker.proto`

```protobuf
message ReportGeneratorItemReturnsRequest {
  ReturnObject returned_object = 1;
  Address worker_addr = 2;
  int64 item_index = 3;
  bytes generator_id = 5;
  uint64 attempt_number = 6;
}

message ReportGeneratorItemReturnsReply {
  // The total number objects consumed from the generator.
  // -1 means it is not known.
  int64 total_num_object_consumed = 1;
}
```

---

## 10. 完整数据流总结

```
EXECUTOR (producer)                          OWNER/CALLER (consumer)
=====================                        =======================

yield value
    |
    v
CoreWorker::ReportGeneratorItemReturns()
    |
    v  (gRPC: ReportGeneratorItemReturnsRequest)
    |       item_index, generator_id, returned_object
    v
CoreWorker::HandleReportGeneratorItemReturns()
    |
    v
TaskManager::HandleReportGeneratorItemReturns()
    |
    +---> stream.InsertToStream(object_id, item_index)
    |     [GetObjectRefAtIndex 验证确定性]
    +---> reference_counter_.OwnDynamicStreamingTaskReturnRef(...)
    +---> HandleTaskReturn(...)
    |       if in_plasma: 记录位置 + 放哨兵
    |       else: 从 gRPC 提取数据 → in_memory_store_ 或本地 plasma
    |
    +---> Backpressure check:
    |     if unconsumed >= threshold:
    |       QUEUE execution_signal_callback (defer gRPC reply)
    |     else:
    |       execution_signal_callback(OK, total_consumed)
    |       (sends gRPC reply immediately)
    |
    |                                           Consumer calls:
    |                                           next(ObjectRefGenerator)
    |                                               |
    |                                               v
    |                                           peek_object_ref_stream()
    |                                           [确定性计算下一个 ObjectID]
    |                                           [可能 ray.wait() 等待就绪]
    |                                               |
    |                                               v
    |                                           try_read_next_object_ref_stream()
    |                                               |
    |                                               v
    |                                           TaskManager::TryReadObjectRefStream()
    |                                               |
    |                                               +---> stream.TryReadNextItem()
    |                                               |     (increments next_index_,
    |                                               |      total_num_object_consumed_++)
    |                                               |
    |                                               +---> if total_unconsumed < threshold:
    |                                                     trigger all queued
    |                                                     execution_signal_callbacks
    |                                                     (sends deferred gRPC replies)
    |
    |  <--- gRPC reply arrives --->
    v
waiter.HandleObjectReported(total_consumed)
    |
    v
backpressure_cond_var_.SignalAll()
(executor unblocks and yields next value)
```

---

## 11. 关键源码文件索引

| 文件 | 关键内容 |
|------|---------|
| `src/ray/core_worker/task_manager.h` | `ObjectRefStream` 类定义、`ExecutionSignalCallback`、`ref_stream_execution_signal_callbacks_` |
| `src/ray/core_worker/task_manager.cc` | `AddPendingTask`、`InsertToStream`、`TryReadNextItem`、`PeekNextItem`、`HandleReportGeneratorItemReturns`、`TryReadObjectRefStream`、`PeekObjectRefStream`、`MarkEndOfStream`、`GetObjectRefAtIndex`、`HandleTaskReturn` |
| `src/ray/core_worker/core_worker.cc` | `ReportGeneratorItemReturns`、`HandleReportGeneratorItemReturns`、`TryReadObjectRefStream`、`PeekObjectRefStream` |
| `src/ray/core_worker/generator_waiter.h` | `GeneratorBackpressureWaiter` 类定义 |
| `src/ray/core_worker/generator_waiter.cc` | `WaitUntilObjectConsumed`、`HandleObjectReported`、`IncrementObjectGenerated` |
| `src/ray/core_worker/common.cc` | `SerializeReturnObject`（决定 in_plasma） |
| `src/ray/common/task/task_spec.cc` | `NumReturns`、`ReturnId`、`StreamingGeneratorReturnId`、`IsStreamingGenerator`、`SetNumStreamingGeneratorReturns` |
| `python/ray/_private/object_ref_generator.py` | `ObjectRefGenerator` Python API |
| `python/ray/_raylet.pyx` | Cython 绑定 |
| `src/ray/protobuf/core_worker.proto` | `ReportGeneratorItemReturnsRequest/Reply` |
