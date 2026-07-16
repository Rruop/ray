# Ray Streaming Generator 与任务完成/失败机制详解

本文档详细分析 Ray 中 Streaming Generator 的完整生命周期，包括任务执行、yield 上报、消费、完成/失败/重试处理、EOF 哨兵、异常传播等核心机制。

---

## 目录

1. [Streaming Generator 执行架构总览](#1-streaming-generator-执行架构总览)
2. [三个写入通道的分工](#2-三个写入通道的分工)
3. [Worker 侧执行流程](#3-worker-侧执行流程)
4. [Driver 侧接收流程](#4-driver-侧接收流程)
5. [Python 消费者读取流程](#5-python-消费者读取流程)
6. [gRPC status 与 PushTaskReply 语义区分](#6-grpc-status-与-pushtaskreply-语义区分)
7. [CompletePendingTask 完整流程](#7-completependingtask-完整流程)
8. [FailPendingTask 完整流程](#8-failpendingtask-完整流程)
9. [FailOrRetryPendingTask 与重试机制](#9-failorretrypendingtask-与重试机制)
10. [Memory Store 不可覆写机制](#10-memory-store-不可覆写机制)
11. [MarkEndOfStream 与 EOF 哨兵](#11-markendofstream-与-eof-哨兵)
12. [retryable vs non-retryable 异常处理](#12-retryable-vs-non-retryable-异常处理)
13. [六种场景完整对比](#13-六种场景完整对比)
14. [generator_ref (ReturnId(0)) 的完整处理](#14-generator_ref-returnid0-的完整处理)
15. [ObjectRefStream TryReadNextItem 与 PeekNextItem](#15-objectrefstream-tryreadnextitem-与-peeknextitem)
16. [血缘恢复与 Plasma Store 重建](#16-血缘恢复与-plasma-store-重建)
17. [Ray Data 消费者的失败处理](#17-ray-data-消费者的失败处理)
18. [关键代码文件索引](#18-关键代码文件索引)

---

## 1. Streaming Generator 执行架构总览

### 1.1 三方参与者

```
Worker（执行方）                    Driver（Owner/调用方）              Python消费者
───────────────                    ──────────────────              ────────────
① 执行 generator 函数               ④ 接收 Report RPC               ⑥ _next_sync 读取
② 每次 yield: Report RPC           ⑤ CompletePendingTask           ⑦ ray.get 获取值
③ 任务结束: PushTaskReply           MarkEndOfStream
```

### 1.2 ObjectID 空间

```cpp
// task_spec.cc
ObjectID TaskSpecification::ReturnId(size_t return_index) const {
    return ObjectID::FromIndex(TaskId(), return_index + 1);
}

ObjectID TaskSpecification::StreamingGeneratorReturnId(size_t generator_index) const {
    RAY_CHECK_EQ(NumReturns(), 1UL);
    return ObjectID::FromIndex(TaskId(), 2 + generator_index);
}
```

```
FromIndex(task_id, 1)    = generator_ref  ← 任务本身 (ReturnId(0))
FromIndex(task_id, 2)    = yield 0        ← 第 1 次 yield (StreamingGeneratorReturnId(0))
FromIndex(task_id, 3)    = yield 1        ← 第 2 次 yield
FromIndex(task_id, 4)    = yield 2        ← 第 3 次 yield
...
FromIndex(task_id, 2+N)  = EOF 位置       ← 由 MarkEndOfStream 写入
```

### 1.3 关键数据结构

**PushTaskReply**（`core_worker.proto:123-175`）：

```protobuf
message PushTaskReply {
  repeated ReturnObject return_objects = 1;               // generator_ref 的值
  repeated ReturnObject dynamic_return_objects = 2;      // 动态返回值
  bool worker_exiting = 3;
  repeated ObjectReferenceCount borrowed_refs = 4;
  bool is_retryable_error = 5;
  bool is_application_error = 6;
  bool was_cancelled_before_running = 7;
  optional string actor_repr_name = 8;
  string task_execution_error = 9;
  repeated StreamingGeneratorReturnIdInfo streaming_generator_return_ids = 10;
}
```

**StreamingGeneratorReturnIdInfo**：

```protobuf
message StreamingGeneratorReturnIdInfo {
    bytes object_id = 1;
    bool is_plasma_object = 2;
}
```

**ObjectRefStream**（`task_manager.h:107-147`）：

```cpp
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
  void MarkEndOfStream(int64_t item_index, ObjectID *object_id_in_last_index);
  int64_t LastConsumedIndex() const { return next_index_ - 1; }
  int64_t EofIndex() const { return end_of_stream_index_; }

 private:
  ObjectID GetObjectRefAtIndex(int64_t generator_index) const {
      return ObjectID::FromIndex(generator_task_id_, 2 + generator_index);
  }

  TaskID generator_task_id_;
  ObjectID generator_id_;
  absl::flat_hash_set<ObjectID> temporarily_owned_refs_;
  absl::flat_hash_set<ObjectID> refs_written_to_stream_;
  int64_t end_of_stream_index_ = -1;
  int64_t next_index_ = 0;
  int64_t max_index_seen_ = -1;
  int64_t total_num_object_written_{};
  int64_t total_num_object_consumed_{};
};
```

---

## 2. 三个写入通道的分工

Streaming Generator 的数据通过三个独立通道写入 Driver 侧 memory store：

| 通道 | 何时写入 | 写哪个 ObjectID | 写什么内容 | 传输方式 |
|------|---------|----------------|-----------|---------|
| ① ReportGeneratorItemReturns RPC | 每次 yield 时 | `FromIndex(task_id, 2+N)` 各 yield 位置 | 正常值 / RayTaskError | 独立 gRPC |
| ② PushTaskReply.return_objects | 任务结束时 | `FromIndex(task_id, 1)` generator_ref | None / RayTaskError | PushTaskReply |
| ③ MarkEndOfStream | 任务结束时 | `FromIndex(task_id, 2+EOF_index)` | END_OF_STREAMING_GENERATOR 哨兵 | 函数调用 |

**三者写不同 ObjectID，不会冲突**。

### 分工原因

- **yield 位置的数据必须实时传递**：消费者可能在任务结束前就开始消费，不能等 PushTaskReply
- **generator_ref 的值在任务结束时才确定**：正常完成是 None，异常是 RayTaskError
- **EOF 哨兵必须在任务结束时写入**：告诉消费者 stream 终止

---

## 3. Worker 侧执行流程

### 3.1 execute_streaming_generator_sync

**文件**: `_raylet.pyx:1301-1348`

```python
cdef execute_streaming_generator_sync(StreamingGeneratorExecutionContext context):
    cdef int64_t gen_index = 0

    assert context.return_size == 1  # 只有 generator_ref

    gen = context.generator
    try:
        stats = None
        while True:
            try:
                output = gen.send(stats)           # ① 驱动 generator 到下一个 yield
                stats = report_streaming_generator_output(context, output, gen_index, None)
                                                  # ② 上报这一条 yield
                gen_index += 1
            except StopIteration:
                break                              # ③ generator 正常结束
    except Exception as e:
        report_streaming_generator_exception(context, e, gen_index, None)
                                                  # ④ 上报异常（仅 non-retryable 走这里）

    # ⑤ 等待所有 in-flight Report RPC 完成
    with nogil:
        return_status = context.waiter.get().WaitAllObjectsReported()
    check_status(return_status)
```

**关键**：`gen.send(stats)` 是驱动器——每次调用推进 generator 执行到下一个 `yield`，返回 yield 的值。Generator 协议中，`StopIteration` = 正常结束，其他 `Exception` = 用户代码异常。

### 3.2 report_streaming_generator_output

**文件**: `_raylet.pyx:1170-1220`

```python
cdef report_streaming_generator_output(context, output, generator_index, ...):
    cdef c_pair[CObjectID, shared_ptr[CRayObject]] return_obj

    # ① 创建返回对象（序列化 + 分配 ObjectID）
    create_generator_return_obj(output, context.generator_id, worker,
                                context.caller_address, context.task_id,
                                context.return_size, generator_index,
                                context.is_async, &return_obj)

    # ② 记录到 streaming_generator_returns（用于 PushTaskReply）
    context.streaming_generator_returns[0].push_back(
        c_pair[CObjectID, c_bool](return_obj.first, is_plasma_object(return_obj.second)))

    # ③ 通过 ReportGeneratorItemReturns RPC 发给 Driver
    with nogil:
        check_status(CCoreWorkerProcess.GetCoreWorker().ReportGeneratorItemReturns(
            return_obj, context.generator_id, context.caller_address,
            generator_index, context.attempt_number, context.waiter))
```

### 3.3 report_streaming_generator_exception

**文件**: `_raylet.pyx:1236-1290`

```python
cdef report_streaming_generator_exception(context, e, generator_index, ...):
    cdef c_pair[CObjectID, shared_ptr[CRayObject]] return_obj

    create_generator_error_object(e, worker, context.task_type,
                                  context.caller_address, context.task_id,
                                  ..., &return_obj,
                                  context.is_retryable_error,
                                  context.application_error)

    del e  # 尽快释放异常对象的内存

    # 记录到 streaming_generator_returns
    context.streaming_generator_returns[0].push_back(
        c_pair[CObjectID, c_bool](return_obj.first, is_plasma_object(return_obj.second)))

    # 通过 ReportGeneratorItemReturns RPC 发给 Driver
    with nogil:
        check_status(CCoreWorkerProcess.GetCoreWorker().ReportGeneratorItemReturns(
            return_obj, context.generator_id, context.caller_address,
            generator_index, context.attempt_number, context.waiter))
```

### 3.4 create_generator_error_object — retryable vs non-retryable 分流

**文件**: `_raylet.pyx:1508-1610`

```python
cdef create_generator_error_object(e, worker, ..., error_object, is_retryable_error, application_error):
    is_retryable_error[0] = determine_if_retryable(
        should_retry_exceptions, e, serialized_retry_exception_allowlist,
        function_descriptor)

    if is_retryable_error[0]:
        # ★ retryable 异常 → 直接抛出！不发 Report RPC
        # 因为任务会被重试，无需在 yield 位置写错误
        raise e

    # non-retryable 异常 → 创建错误对象 + 发 Report RPC
    error_id = core_worker.allocate_dynamic_return_id_for_generator(
        caller_address, task_id.native(), return_size, generator_index, is_async)
    intermediate_result.push_back(
        c_pair[CObjectID, shared_ptr[CRayObject]](error_id, shared_ptr[CRayObject]()))
    store_task_errors(worker, e, True, ..., &intermediate_result, application_error, ...)
    error_object[0] = intermediate_result.back()
```

**关键分流**：

| 异常类型 | `create_generator_error_object` 行为 | 是否发 Report RPC | 是否 push_back 到 streaming_generator_returns |
|---------|---|---|---|
| non-retryable | 创建 RayTaskError，写入 `intermediate_result` | ✅ 是 | ✅ 是 |
| retryable | `raise e`，直接抛出 | ❌ 否 | ❌ 否 |

**注意**：`determine_if_retryable` 只判断异常**类型**是否可重试，不检查重试次数（Worker 不知道 `num_retries_left`）。

### 3.5 execute_streaming_generator_sync 返回后的处理

```python
# _raylet.pyx 外层:
if is_streaming_generator:
    execute_streaming_generator_sync(context)
    outputs = None   # ← Streaming generator 输出已通过 Report RPC 发送

# 然后进入:
if (returns[0].size() == 1
        and not inspect.isgenerator(outputs)      # None 不是 generator
        and not inspect.isasyncgen(outputs)):     # None 不是 async generator
    outputs = (None,)   # ★ 单返回值包装为元组

# 再进入:
core_worker.store_task_outputs(worker, (None,), caller_address, returns, None, ...)
# → returns[0][0].second 被填充为序列化 None 的 RayObject
```

**retryable 异常时**：`create_generator_error_object` 抛出 `raise e` → 异常向上传播 → 外层 `except BaseException as e` 捕获 → `store_task_errors` 写入 → `returns[0][0].second = RayTaskError`。

### 3.6 store_task_errors — 为所有返回槽写同一个 RayTaskError

**文件**: `_raylet.pyx:966-1025`

```python
cdef store_task_errors(worker, exc, task_exception, actor, actor_id, function_name,
                       CTaskType task_type, ..., returns, application_error, ...):
    failure_object = RayTaskError(function_name, backtrace, exc, ...)

    if application_error != NULL:
        application_error[0] = str(failure_object)[-MAX_APPLICATION_ERROR_LENGTH:]

    errors = []
    for _ in range(returns[0].size()):       # ★ 所有返回槽写同一个错误
        errors.append(failure_object)

    num_errors_stored = core_worker.store_task_outputs(
        worker, errors, caller_address, returns, None, ...)
```

---

## 4. Driver 侧接收流程

### 4.1 HandleReportGeneratorItemReturns — 实时接收 yield

**文件**: `task_manager.cc:833-920`

```cpp
bool TaskManager::HandleReportGeneratorItemReturns(
    const rpc::ReportGeneratorItemReturnsRequest &request,
    const ExecutionSignalCallback &execution_signal_callback) {
  const auto &generator_id = ObjectID::FromBinary(request.generator_id());
  int64_t item_index = request.item_index();
  int64_t attempt_number = request.attempt_number();

  // ★ 检查 attempt_number：旧 attempt 的报告直接拒绝
  {
    absl::MutexLock lock(&mu_);
    auto it = submissible_tasks_.find(task_id);
    if (it != submissible_tasks_.end()) {
      if (it->second.spec_.AttemptNumber() > attempt_number) {
        execution_signal_callback(Status::NotFound("Stale..."), -1);
        return false;
      }
    }
  }

  const auto store_in_plasma_ids = GetTaskReturnObjectsToStoreInPlasma(task_id);

  if (request.has_returned_object()) {
    const auto object_id = ObjectID::FromBinary(returned_object.object_id());

    // ① 写入 stream
    auto index_not_used_yet = stream_it->second.InsertToStream(object_id, item_index);
    if (index_not_used_yet) {
      reference_counter_.OwnDynamicStreamingTaskReturnRef(object_id, generator_id);
    }
    reference_counter_.UpdateObjectPendingCreation(object_id, false);

    // ② 写入 memory store
    HandleTaskReturn(object_id, returned_object, ...);
  }

  // ③ 背压判断
  if (stream_it->second.IsObjectConsumed(item_index)) {
    execution_signal_callback(Status::OK(), total_consumed);
    return false;
  }
  if (backpressure_threshold != -1 &&
      (item_index - stream_it->second.LastConsumedIndex()) >= backpressure_threshold) {
    signal_it->second.push_back(execution_signal_callback);  // 暂存，稍后回复
  } else {
    execution_signal_callback(Status::OK(), total_consumed);  // 立即回复
  }
}
```

### 4.2 CompletePendingTask — 任务结束时处理

**文件**: `task_manager.cc:912-1078`

`CompletePendingTask` 处理 `PushTaskReply` 中的三类数据：

```
① reply.return_objects       → HandleTaskReturn 写 generator_ref
② reply.dynamic_return_objects → HandleTaskReturn 写 dynamic 返回
③ reply.streaming_generator_return_ids → 记录元信息 + 计算 EOF 位置
```

详见第7节。

---

## 5. Python 消费者读取流程

### 5.1 _next_sync — 三步交互

**文件**: `object_ref_generator.py:224-270`

```python
def _next_sync(self, timeout_s=None):
    core_worker = self.worker.core_worker

    # ① Peek：预览下一个位置
    expected_ref, is_ready = core_worker.peek_object_ref_stream(self._generator_ref)
    #  ↑ C++: stream.PeekNextItem() → (GetObjectRefAtIndex(next_index_), is_ready)

    # ② Wait：如果未就绪，等待
    if not is_ready:
        _, unready = ray.wait([expected_ref], timeout=timeout_s, fetch_local=False)
        if len(unready) > 0:
            return ray.ObjectRef.nil()  # 超时返回 nil

    # ③ Read：消费并推进 index
    try:
        ref = core_worker.try_read_next_object_ref_stream(self._generator_ref)
        #  ↑ C++: stream.TryReadNextItem(&next_id) → next_index_++
        assert not ref.is_nil()
    except ObjectRefStreamEndOfStreamError:
        if self._generator_task_raised:
            raise StopIteration from None

        try:
            ray.get(self._generator_ref)          # ④ 检查 generator_ref 是否有异常
        except Exception:
            self._generator_task_raised = True
            return self._generator_ref             # 返回含异常的 ref
        else:
            raise StopIteration from None           # 正常结束

    return ref
```

### 5.2 _next_sync vs _next_async

| 特性 | `_next_sync` | `_next_async` |
|------|-------------|---------------|
| 等待方式 | `ray.wait([ref], timeout=timeout_s)` | `await asyncio.wait([ref], timeout=timeout_s)` |
| 获取值 | `ray.get(generator_ref)` | `await generator_ref` |
| 适用场景 | 同步代码 | asyncio 事件循环 |
| EOF 处理 | 相同逻辑 | 相同逻辑 |

---

## 6. gRPC status 与 PushTaskReply 语义区分

### 6.1 两层语义

| 层次 | 代表 | 含义 |
|------|------|------|
| **gRPC `status`** | 传输层状态 | Worker 进程是否可达 |
| **`PushTaskReply`** | 任务执行层状态 | 任务是否被取消、是否可重试异常、返回值内容 |

**`status.ok()` 不代表任务成功！** 只代表 Worker 进程响应了 RPC。

### 6.2 各种场景下 status 和 reply 的值

| 场景 | `status.ok()` | `reply` 内容 |
|------|-------------|-------------|
| 任务正常完成 | `true` | `return_objects` 含正常返回值 |
| 任务抛出应用异常 | `true` | `return_objects` 含异常对象，`is_application_error=true` |
| 任务被取消 | `true` | `was_cancelled_before_running=true` |
| Worker 进程崩溃 | `false` | 无 reply（超时或连接错误） |
| Worker 主动退出 | `true` | `worker_exiting=true` |

### 6.3 NormalTaskSubmitter 中的处理

**文件**: `normal_task_submitter.cc:328-415`

```cpp
client->PushNormalTask(std::move(request),
    [this, ...](Status status, const rpc::PushTaskReply &reply) {
      if (!status.ok()) {
        // 传输失败 → 查询 Worker 失败原因
        failed_tasks_pending_failure_cause_.insert(task_id);
        raylet_client->GetWorkerFailureCause(lease_id, callback);
      }
      if (status.ok()) {
        if (reply.was_cancelled_before_running()) {
          task_manager_.FailPendingTask(task_id, rpc::ErrorType::TASK_CANCELLED);
        } else if (!task_spec.GetMessage().retry_exceptions()
                   || !reply.is_retryable_error()
                   || !task_manager_.RetryTaskIfPossible(...)) {
          task_manager_.CompletePendingTask(task_id, reply, addr,
                                            reply.is_application_error());
        }
      }
    });
```

### 6.4 ActorTaskSubmitter 中的处理

**文件**: `actor_task_submitter.cc:445-570`

```cpp
void ActorTaskSubmitter::HandlePushTaskReply(
    const Status &status, const rpc::PushTaskReply &reply, ...) {
  if (resubmit_generator) {
    task_manager_.MarkGeneratorFailedAndResubmit(task_id);
    return;
  }
  if ((status.ok() && reply.was_cancelled_before_running()) ||
      status.IsSchedulingCancelled()) {
    HandleTaskCancelledBeforeExecution(status, reply, task_spec);
  } else if (status.ok() && !is_retryable_exception) {
    task_manager_.CompletePendingTask(task_id, reply, addr, reply.is_application_error());
  } else {
    will_retry = task_manager_.FailOrRetryPendingTask(...);
    if (!is_actor_dead && !will_retry) {
      if (status.ok()) {
        task_manager_.CompletePendingTask(task_id, reply, addr, true);
      } else if (timeout != 0) {
        queue.wait_for_death_info_tasks_.push_back(...);
      } else {
        task_manager_.FailPendingTask(...);
      }
    }
  }
}
```

---

## 7. CompletePendingTask 完整流程

**文件**: `task_manager.cc:912-1078`

```
CompletePendingTask(task_id, reply, worker_addr, is_application_error)
  │
  ├─ 1. GetTaskReturnObjectsToStoreInPlasma(task_id, &first_execution)
  │     → first_execution = (num_successful_executions_ == 0)
  │
  ├─ 2. 处理 dynamic_return_objects（如果有）
  │     ├─ RAY_CHECK(reply.return_objects_size() == 1)
  │     ├─ generator_id = FromBinary(reply.return_objects(0).object_id())
  │     ├─ 每个 dynamic_return:
  │     │   ├─ first_execution → AddDynamicReturn(object_id, generator_id)
  │     │   └─ HandleTaskReturn(object_id, return_object)
  │     └─ 如果 HandleTaskReturn 失败 → FailOrRetryPendingTask(fail_immediately=true)
  │
  ├─ 3. 处理 return_objects
  │     ├─ 每个 return_object:
  │     │   ├─ HandleTaskReturn(object_id, return_object) → 写 generator_ref
  │     │   └─ 如果失败 → FailOrRetryPendingTask(fail_immediately=true)
  │     └─ 记录 direct_return_ids
  │
  ├─ 4. 在 mu_ 锁内更新状态（★ 关键：streaming_generator_return_ids 处理）
  │     ├─ first_execution 时:
  │     │   ├─ SetNumStreamingGeneratorReturns(reply.streaming_generator_return_ids_size())
  │     │   ├─ 记录 is_plasma_object 的 IDs 到 reconstructable_return_ids_
  │     │   └─ AddDynamicReturnId(dynamic_return_id) 到 spec
  │     ├─ num_successful_executions_++
  │     ├─ is_application_error → FAILED else → FINISHED
  │     └─ 决定 release_lineage
  │
  ├─ 5. Streaming Generator 特殊处理（锁外）
  │     ├─ first_execution:
  │     │   └─ MarkEndOfStream(generator_id, streaming_generator_return_ids_size())
  │     └─ !first_execution && is_application_error:
  │         └─ 对每个 NumStreamingGeneratorReturns():
  │             HandleTaskReturn(StreamingGeneratorReturnId(i), reply.return_objects(0))
  │             → 写 RayTaskError 到所有已知 yield 位置
  │
  └─ 6. RemoveFinishedTaskReferences + ShutdownIfNeeded
```

### streaming_generator_return_ids 在 CompletePendingTask 中的处理

**阶段1（锁内）**：只记录元信息，不写数据

```cpp
if (spec.IsStreamingGenerator()) {
    // ① 记录 yield 总数
    auto num_streaming_generator_returns =
        reply.streaming_generator_return_ids_size();
    spec.SetNumStreamingGeneratorReturns(num_streaming_generator_returns);

    // ② 记录哪些是 plasma 对象（用于血缘恢复）
    for (const auto &return_id_info : reply.streaming_generator_return_ids()) {
        if (return_id_info.is_plasma_object()) {
            it->second.reconstructable_return_ids_.insert(
                ObjectID::FromBinary(return_id_info.object_id()));
        }
    }
}
```

**阶段2（锁外）**：用 size 计算 EOF 位置

```cpp
if (first_execution) {
    MarkEndOfStream(generator_id, reply.streaming_generator_return_ids_size());
} else if (is_application_error) {
    for (size_t i = 0; i < spec.NumStreamingGeneratorReturns(); i++) {
        HandleTaskReturn(spec.StreamingGeneratorReturnId(i), reply.return_objects(0), ...);
    }
}
```

| 用途 | 使用了什么 | 阶段 |
|------|----------|------|
| 计算 EOF 位置 | `.size()` | 阶段2 |
| 记录 yield 总数 | `.size()` → `SetNumStreamingGeneratorReturns` | 阶段1 |
| 标记 plasma 对象 | `.is_plasma_object()` → `reconstructable_return_ids_` | 阶段1 |
| 重执行失败遍历 | `NumStreamingGeneratorReturns()` → `StreamingGeneratorReturnId(i)` | 阶段2 |

---

## 8. FailPendingTask 完整流程

**文件**: `task_manager.cc:1210-1282`

```
FailPendingTask(task_id, error_type, status, ray_error_info)
  │
  ├─ 1. GetTaskReturnObjectsToStoreInPlasma(task_id)
  │
  ├─ 2. mu_ 锁内：
  │     ├─ is_canceled_ && error_type != TASK_CANCELLED → 覆盖为 TASK_CANCELLED
  │     ├─ 设置 TaskStatus: FAILED 或 FINISHED（IntentionalSystemExit）
  │     └─ submissible_tasks_.erase(it)
  │
  ├─ 3. RemoveFinishedTaskReferences
  │
  ├─ 4. ★ MarkTaskReturnObjectsFailed(spec, error_type, ray_error_info, store_in_plasma_ids)
  │     ├─ 为每个 NumReturns() 写入错误
  │     │   generator_ref = FromIndex(task_id, 1)
  │     │   → plasma 或 memory store
  │     ├─ 为每个 DynamicReturnId 写入错误
  │     └─ IsStreamingGenerator() 特殊处理:
  │         ├─ MarkEndOfStream(generator_id, -1)  ← 在当前位置标记 EOF
  │         └─ 为每个 NumStreamingGeneratorReturns() 写入错误
  │
  └─ 5. ShutdownIfNeeded
```

### MarkTaskReturnObjectsFailed — Driver 自己构造 RayTaskError

**文件**: `task_manager.cc:1580-1660`

```cpp
void TaskManager::MarkTaskReturnObjectsFailed(
    const TaskSpecification &spec, rpc::ErrorType error_type, ...) {
  const TaskID task_id = spec.TaskId();
  // ★ 自己构造错误对象（没有 reply）
  RayObject error(error_type, ray_error_info);

  // ① 写所有静态返回位置（i=0 → generator_ref）
  for (int i = 0; i < num_returns; i++) {
    const auto object_id = ObjectID::FromIndex(task_id, i + 1);
    if (store_in_plasma_ids.contains(object_id)) {
      Status s = put_in_local_plasma_callback_(error, object_id);
      if (!s.ok()) {
        in_memory_store_.Put(error, object_id, ...);
      }
    } else {
      in_memory_store_.Put(error, object_id, ...);
    }
  }

  // ② 写所有 dynamic 返回位置
  if (spec.ReturnsDynamic()) { /* 同上 */ }

  // ③ Streaming Generator 特殊处理
  if (spec.IsStreamingGenerator()) {
    MarkEndOfStream(generator_id, -1);  // ← 在当前位置标记 EOF

    for (size_t i = 0; i < num_streaming_generator_returns; i++) {
      const auto generator_return_id = spec.StreamingGeneratorReturnId(i);
      // plasma 或 memory store（同上）
    }
  }
}
```

### CompletePendingTask 写 RayTaskError vs FailPendingTask 写 RayTaskError 的区别

| | CompletePendingTask（`!first_execution && is_application_error`） | FailPendingTask → `MarkTaskReturnObjectsFailed` |
|---|---|---|
| RayTaskError 来源 | `reply.return_objects(0)` — Worker 发来的 | Driver 自己构造 `RayObject(error_type)` |
| EOF index | `streaming_generator_return_ids_size()` | `-1`（当前位置） |
| generator_ref | 通过 `HandleTaskReturn` 写入 reply 的值 | 通过 `FromIndex(task_id,1)` 写入 Driver 构造的错误 |
| 已有 yield 值 | no-op 保留 | no-op 保留 |
| 无值 yield | 写入 reply 中的 RayTaskError | 写入 Driver 构造的 RayTaskError |
| plasma 对象 | 不特殊处理 | `put_in_local_plasma_callback_` 尝试写 plasma |

---

## 9. FailOrRetryPendingTask 与重试机制

### 9.1 FailOrRetryPendingTask

**文件**: `task_manager.cc:1284-1318`

```cpp
bool TaskManager::FailOrRetryPendingTask(const TaskID &task_id,
                                         rpc::ErrorType error_type, ...) {
  bool will_retry = false;
  if (!fail_immediately) {
    will_retry = RetryTaskIfPossible(task_id, ...);
  }
  if (!will_retry && mark_task_object_failed) {
    FailPendingTask(task_id, error_type, status, ray_error_info);
  }
  return will_retry;
}
```

### 9.2 RetryTaskIfPossible

**文件**: `task_manager.cc:1143-1210`

```cpp
bool TaskManager::RetryTaskIfPossible(const TaskID &task_id,
                                      const rpc::RayErrorInfo &error_info) {
  bool will_retry = false;
  {
    absl::MutexLock lock(&mu_);
    auto &num_retries_left = task_entry.num_retries_left_;

    if (num_retries_left > 0) {
      will_retry = true;
      num_retries_left--;
    } else if (num_retries_left == -1) {  // 无限重试
      will_retry = true;
    } else {
      RAY_CHECK(num_retries_left == 0);     // 不重试
    }

    if (will_retry) {
      SetTaskStatus(task_entry, rpc::TaskStatus::FAILED, ...);
      task_entry.MarkRetry();  // ★ 只设置 is_retry_=true，不重置 num_successful_executions_
    }
  }

  if (will_retry) {
    uint32_t delay_ms = GetTaskRetryDelayMs(spec.AttemptNumber(), error_type);
    async_retry_task_callback_(spec, delay_ms);  // 异步重新提交
    return true;
  }
  return false;
}
```

### 9.3 MarkRetry 不重置 num_successful_executions_

```cpp
// task_manager.h:570
void MarkRetry() { is_retry_ = true; }
// ★ 注意：只设置 is_retry_，不重置 num_successful_executions_
// num_successful_executions_ 仍为 0 → 重试后 CompletePendingTask 中 first_execution=true
```

### 9.4 决策流程图

```
RPC 回调收到 (status, reply)
  │
  ├─ !status.ok() → 传输层失败
  │   ├─ Normal Task → GetWorkerFailureCause → FailOrRetryPendingTask
  │   └─ Actor Task → 检查 Actor 状态
  │       ├─ ALIVE/RESTARTING → FailOrRetryPendingTask(ACTOR_UNAVAILABLE)
  │       └─ DEAD → FailPendingTask
  │
  └─ status.ok() → Worker 答复了
      ├─ was_cancelled_before_running → FailPendingTask(TASK_CANCELLED)
      ├─ is_retryable_error → RetryTaskIfPossible
      │   ├─ 有重试次数 → 重试（★ 不调 CompletePendingTask）
      │   └─ 无重试次数 → CompletePendingTask(is_application_error=true)
      └─ 其他 → CompletePendingTask
```

### 9.5 任务重试时 Worker 侧的行为

**重试时整个 generator 函数从头执行**。之前 Worker 上的任务已经自然结束（异常或正常退出），不存在"停止旧 task"的动作。

重试时 Worker 会重新产生所有 yield，通过 Report RPC 发给 Driver：

```
首次执行: yield0 → Report(index=0) ✅
重试执行: yield0 → Report(index=0, attempt=新值)
  → Driver: InsertToStream → no-op（已有）
  → Driver: HandleTaskReturn → Put no-op（已有）

首次执行: yield2 → retryable异常 → ★ 没发 Report RPC
重试执行: yield2 → Report(index=2) → Driver 写入新值 ✅
```

---

## 10. Memory Store 不可覆写机制

### 10.1 Put 方法

**文件**: `memory_store.cc:110-170`

```cpp
void CoreWorkerMemoryStore::Put(const RayObject &object,
                                const ObjectID &object_id,
                                const bool has_reference) {
  std::shared_ptr<RayObject> object_entry = std::make_shared<RayObject>(...);

  {
    absl::MutexLock lock(&mu_);

    auto iter = objects_.find(object_id);
    if (iter != objects_.end()) {
      return;  // ★ 已存在，直接返回，不覆写
    }

    // 唤醒异步/同步等待者
    // ...

    if (!reference_counting_enabled_ || has_reference) {
      EmplaceObjectAndUpdateStats(object_id, object_entry);
    } else {
      OnDelete(object_entry);  // 无引用，立即标记已删除
    }
  }
}
```

### 10.2 对各场景的影响

| 场景 | 谁先写 | 谁后写（no-op） | 结果 |
|------|--------|----------------|------|
| 正常完成 | `HandleReportGeneratorItemReturns` 写 yield 值 | `CompletePendingTask` 的 `HandleTaskReturn` | 后写被忽略 |
| 重试时已有值 | 首次 Report RPC 写入 | 重试 Report RPC 再次写入 | no-op，首次值保留 |
| `MarkTaskReturnObjectsFailed` | Report RPC 已写 yield 值 | `MarkTaskReturnObjectsFailed` 写错误 | 错误被忽略，原始值保留 |
| 血缘恢复成功 | `OBJECT_IN_PLASMA` 哨兵已写 | 重建后的新值写 plasma | memory store 哨兵不变，plasma 中实际数据被重建 |

### 10.3 Plasma Store 可以覆盖

Memory store 中 `OBJECT_IN_PLASMA` 哨兵不覆盖，但 plasma store 内的实际数据可以通过 `put_in_local_plasma_callback_` 重建到同一个 ObjectID（重建通常在不同 Node 的 plasma store 上）。

---

## 11. MarkEndOfStream 与 EOF 哨兵

### 11.1 ObjectRefStream::MarkEndOfStream

**文件**: `task_manager.cc:209-230`

```cpp
void ObjectRefStream::MarkEndOfStream(int64_t item_index,
                                      ObjectID *object_id_in_last_index) {
  if (end_of_stream_index_ != -1) {
    return;  // 已标记过
  }
  // ★ 关键公式：end_of_stream_index_ = max(next_index_, item_index)
  end_of_stream_index_ = std::max(next_index_, item_index);
  auto end_of_stream_id = GetObjectRefAtIndex(end_of_stream_index_);
  *object_id_in_last_index = end_of_stream_id;
}
```

### 11.2 TaskManager::MarkEndOfStream — 写入哨兵

**文件**: `task_manager.cc:610-636`

```cpp
void TaskManager::MarkEndOfStream(const ObjectID &generator_id,
                                  int64_t end_of_stream_index) {
  absl::MutexLock lock(&object_ref_stream_ops_mu_);
  ObjectID last_object_id;

  auto stream_it = object_ref_streams_.find(generator_id);
  if (stream_it == object_ref_streams_.end()) { return; }

  stream_it->second.MarkEndOfStream(end_of_stream_index, &last_object_id);
  if (!last_object_id.IsNil()) {
    reference_counter_.OwnDynamicStreamingTaskReturnRef(last_object_id, generator_id);
    RayObject error(rpc::ErrorType::END_OF_STREAMING_GENERATOR);
    // ★ 写入 EOF 哨兵到 memory store
    in_memory_store_.Put(error, last_object_id,
                         reference_counter_.HasReference(last_object_id));
  }
}
```

### 11.3 两种 end_of_stream_index 的含义

| 调用位置 | `item_index` | 语义 |
|---------|-------------|------|
| `CompletePendingTask`（正常完成） | `streaming_generator_return_ids_size()` | generator 产出了 N 个元素，EOF 在第 N 个位置 |
| `CompletePendingTask`（retryable 耗尽） | `streaming_generator_return_ids_size()` | 只有成功 yield 的数量，EOF 跳过未写入的位置 |
| `MarkTaskReturnObjectsFailed` / `MarkTaskNoRetryInternal` | `-1` | 任务失败/取消，EOF 在消费者当前读取位置 |

### 11.4 `-1` 的含义 — "当前位置"

```cpp
end_of_stream_index_ = std::max(next_index_, -1) = next_index_
```

`next_index_` 是消费者当前读到的位置。传 `-1` 的原因（注释原文）：

> "Pass -1 because the task has been canceled, so we should just end the stream at the caller's current index. This is needed because we may receive generator reports out of order. If the task reports a later index then exits because it was canceled, we will hang waiting for the intermediate indices."

### 11.5 EOF 哨兵写不进去时

**EOF 哨兵的写入可能因为 `Put` 不可覆写而失败**（该位置已有值）。但消费者仍然能正确看到 EOF：

```cpp
Status ObjectRefStream::TryReadNextItem(ObjectID *object_id_out) {
    *object_id_out = GetObjectRefAtIndex(next_index_);
    if (IsFinished()) {  // ★ 先检查 IsFinished，不依赖哨兵对象
        RAY_CHECK(next_index_ == end_of_stream_index_);
        return Status::ObjectRefEndOfStream("");
    }
    // ...
}
```

`IsFinished()` 只看 `end_of_stream_index_` 和 `next_index_` 的关系，**不依赖 memory store 中是否有哨兵对象**。

---

## 12. retryable vs non-retryable 异常处理

### 12.1 Worker 侧的分流

```
gen.send() 抛出异常
  → except Exception as e:
  → report_streaming_generator_exception
    → create_generator_error_object:
      ├─ determine_if_retryable() → True (retryable):
      │   → raise e                    ★ 异常重新抛出
      │   → 不发 Report RPC
      │   → 不 push_back 到 streaming_generator_returns
      │   → 外层 store_task_errors → generator_ref 写 RayTaskError
      │
      └─ determine_if_retryable() → False (non-retryable):
          → 创建 RayTaskError + Report RPC 发走
          → push_back 到 streaming_generator_returns
          → execute_streaming_generator_sync 正常返回
          → generator_ref 写 None
```

### 12.2 Driver 侧的处理

```
PushTaskReply 到达:
  ├─ is_retryable_error=true:
  │   ├─ RetryTaskIfPossible → 有重试 → 重试
  │   └─ 无重试 → CompletePendingTask(is_application_error=true)
  │
  └─ is_application_error=true (non-retryable):
      → CompletePendingTask(is_application_error=true)
```

### 12.3 对 yield 位置的影响

| 异常类型 | yield 异常位置 | 之前的 yield | generator_ref | streaming_generator_return_ids |
|---------|-------------|------------|--------------|---------------------------|
| non-retryable | 写 RayTaskError | 保留正常值 | None | 包含异常 ID |
| retryable（有重试） | 不写（等重试） | 保留正常值 | —（不调 CompletePendingTask） | 不含异常 ID |
| retryable（耗尽） | 不写 | 保留正常值 | RayTaskError | 不含异常 ID |

### 12.4 异常传播路径

```
★ 路径 A: non-retryable 异常 → yield 位置直接抛异常
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Worker: report_streaming_generator_exception → Report RPC(error_object)
Driver: HandleReportGeneratorItemReturns → HandleTaskReturn → Put(RayTaskError, yield_id)
消费者: ray.get(yield_ref) → 反序列化 RayTaskError → 抛异常

★ 路径 B: retryable 异常（耗尽）→ generator_ref 传播异常
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Worker: create_generator_error_object → raise e → store_task_errors → PushTaskReply
Driver: CompletePendingTask → HandleTaskReturn(generator_ref, RayTaskError)
消费者: TryReadNextItem → EndOfStreamError → ray.get(generator_ref) → 抛异常

★ 路径 C: Worker 崩溃 → generator_ref 传播异常
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Driver: FailPendingTask → MarkTaskReturnObjectsFailed
消费者: TryReadNextItem → EndOfStreamError → ray.get(generator_ref) → 抛异常
```

---

## 13. 六种场景完整对比

### 场景 1：正常完成（3 次 yield）

```
Worker:
  yield0 → ①Report(正常值, index=0)
  yield1 → ①Report(正常值, index=1)
  yield2 → ①Report(正常值, index=2)
  StopIteration → outputs=None → (None,)
  → ②PushTaskReply.return_objects(0) = None
  → streaming_generator_return_ids = [yield0, yield1, yield2]

Driver:
  ① HandleReportGeneratorItemReturns ×3 → 写入 yield0/1/2
  ② CompletePendingTask:
     HandleTaskReturn(generator_ref, None) ✅
     MarkEndOfStream(N=3) ✅

Memory Store:
  generator_ref = None
  yield0/1/2 = 正常值
  EOF = END_OF_STREAMING_GENERATOR

消费者: yield0✅ → yield1✅ → yield2✅ → EOF → StopIteration
```

### 场景 2：non-retryable 异常（yield2 时异常）

```
Worker:
  yield0 → ①Report(正常值, index=0)
  yield1 → ①Report(正常值, index=1)
  yield2 → ①Report(RayTaskError, index=2)  ← create_generator_error_object 创建
  → execute_streaming_generator_sync 正常返回
  → outputs=None → (None,)
  → ②PushTaskReply.return_objects(0) = None
  → streaming_generator_return_ids = [yield0, yield1, yield2_error]

Driver:
  ① HandleReportGeneratorItemReturns ×3 → 写入 yield0/1 + yield2_error
  ② CompletePendingTask:
     HandleTaskReturn(generator_ref, None) ✅
     MarkEndOfStream(N=3) ✅

Memory Store:
  generator_ref = None
  yield0/1 = 正常值  yield2 = RayTaskError ★
  EOF = END_OF_STREAMING_GENERATOR

消费者: yield0✅ → yield1✅ → yield2 ★ray.get 抛异常★ → 中断
★ yield2 异常后 generator 已终止，不会再有后续 yield
```

### 场景 3：retryable 异常（有重试次数）

```
Worker:
  yield0 → ①Report(正常值, index=0)
  yield1 → ①Report(正常值, index=1)
  yield2 → retryable → create_generator_error_object → raise e
    → ★ 不发 Report RPC
    → ★ 不 push_back 到 streaming_generator_returns
  → 异常传播 → store_task_errors → return_objects(0) = RayTaskError
  → ②PushTaskReply.return_objects(0) = RayTaskError
  → streaming_generator_return_ids = [yield0, yield1]  ← 只有 2 个
  → is_retryable_error = true

Driver:
  ① HandleReportGeneratorItemReturns ×2 → 写入 yield0/1
  ② 收到 reply → RetryTaskIfPossible → 有重试 → 重试
     ★ 不调 CompletePendingTask

Memory Store（此刻）:
  generator_ref = 无值
  yield0/1 = 正常值  yield2 = 无值
  EOF = 无值

消费者: yield0✅ → yield1✅ → _next_sync → peek(index=2) not ready → 阻塞等待

重试 Worker:
  yield0 → ①Report(正常值, index=0) → no-op
  yield1 → ①Report(正常值, index=1) → no-op
  yield2 → ①Report(正常值, index=2) → ✅ 写入
  yield3 → ①Report(正常值, index=3) → ✅ 写入
  → 正常结束 → ②PushTaskReply.return_objects(0) = None
  → streaming_generator_return_ids = [0,1,2,3]

Driver:
  ② CompletePendingTask(first_execution=true):
     HandleTaskReturn(generator_ref, None) ✅
     MarkEndOfStream(N=4) ✅

消费者: 从阻塞恢复 → yield2✅ → yield3✅ → EOF → StopIteration
```

### 场景 4：retryable 异常（耗尽无重试次数）

```
Worker: 同场景 3 的首次执行

Driver:
  ② RetryTaskIfPossible → 无重试次数 → return false
     → CompletePendingTask(is_application_error=true)

  CompletePendingTask:
    HandleTaskReturn(generator_ref, RayTaskError) ✅
    first_execution=true → MarkEndOfStream(N=2) ✅
      → end_of_stream_index_ = max(2, 2) = 2
      → EOF 哨兵写入 FromIndex(task_id, 4)

Memory Store:
  generator_ref = RayTaskError ★
  yield0/1 = 正常值  yield2 = 无值（EOF 在此位置）
  EOF = END_OF_STREAMING_GENERATOR

消费者:
  yield0✅ → yield1✅ → peek(index=2)
  → FromIndex(task_id,4), is_ready=true(EOF哨兵)
  → TryReadNextItem → IsFinished()=true → EndOfStreamError
  → ray.get(generator_ref) → RayTaskError → 返回 generator_ref
```

### 场景 5：Worker 崩溃（有重试次数）

```
Worker: 进程死亡

Driver:
  !status.ok() → GetWorkerFailureCause → FailOrRetryPendingTask(NODE_DIED)
  → RetryTaskIfPossible → 有重试 → 重试

消费者: 阻塞等待（和场景 3 类似），重试成功后恢复
```

### 场景 6：Worker 崩溃（无重试次数）

```
Driver:
  FailOrRetryPendingTask → 无重试 → FailPendingTask
  → MarkTaskReturnObjectsFailed:
      generator_ref = RayTaskError ✅（新建写入）
      yield0 = RayTaskError → Put no-op（已有正常值保留）
      yield1 = RayTaskError → Put no-op（已有正常值保留）
      MarkEndOfStream(generator_id, -1) → EOF 在当前位置

Memory Store:
  generator_ref = RayTaskError
  yield0/1 = 正常值（no-op 保留）
  EOF = END_OF_STREAMING_GENERATOR

消费者: peek(index=2) → EOF哨兵 → EndOfStreamError
       → ray.get(generator_ref) → RayTaskError
```

---

## 14. generator_ref (ReturnId(0)) 的完整处理

### 14.1 generator_ref 的 ID

```cpp
generator_id = ReturnId(0) = FromIndex(task_id, 1)
```

### 14.2 Worker 侧填充内容

| 场景 | return_objects[0].second 内容 | 原因 |
|------|-------------------------------|------|
| 正常完成 | 序列化 `None` | `outputs=None → (None,) → store_task_outputs` |
| non-retryable 异常 | 序列化 `None` | `execute_streaming_generator_sync` 内部已捕获并通过 Report RPC 发走异常，函数正常返回 |
| retryable 异常 | `RayTaskError` | `create_generator_error_object` 检测到 retryable → `raise e` → 外层 `store_task_errors` |
| 函数直接抛异常（非 generator） | `RayTaskError` | 未进入 `is_streaming_generator` 分支 |

### 14.3 Python 消费者的两步读取

消费者先逐个消费 yield 位置，遇到 EOF 后才查 generator_ref：

```python
def _next_sync(self):
    try:
        ref = try_read_next_object_ref_stream()  # 第1步：消费 yield ref
    except ObjectRefStreamEndOfStreamError:
        try:
            ray.get(self._generator_ref)          # 第2步：查 generator_ref
        except Exception:
            return self._generator_ref             # 任务级失败
        else:
            raise StopIteration                    # 正常结束
    return ref
```

| generator_ref 内容 | `ray.get` 结果 | 消费者行为 |
|-------------------|--------------|----------|
| None | 返回 None | `StopIteration` |
| RayTaskError | 抛异常 | 返回 `generator_ref` |
| 无值 | 阻塞 | 等待（不应该发生） |

---

## 15. ObjectRefStream TryReadNextItem 与 PeekNextItem

### 15.1 TryReadNextItem — 消费

**文件**: `task_manager.cc:74-100`

```cpp
Status ObjectRefStream::TryReadNextItem(ObjectID *object_id_out) {
  *object_id_out = GetObjectRefAtIndex(next_index_);
  // GetObjectRefAtIndex(next_index_) = FromIndex(task_id, 2 + next_index_)

  if (IsFinished()) {
    RAY_CHECK(next_index_ == end_of_stream_index_);
    return Status::ObjectRefEndOfStream("");  // ★ EOF
  }

  auto it = refs_written_to_stream_.find(*object_id_out);
  if (it != refs_written_to_stream_.end()) {
    total_num_object_consumed_ += 1;
    next_index_ += 1;  // ★ 推进消费指针
    return Status::OK();
  } else {
    *object_id_out = ObjectID::Nil();  // 未写入
    return Status::OK();
  }
}
```

**关键**：Read 不删除 `refs_written_to_stream_` 中的元素，只推进 `next_index_`。

### 15.2 PeekNextItem — 预览

```cpp
std::pair<ObjectID, bool> ObjectRefStream::PeekNextItem() {
  const auto &object_id = GetObjectRefAtIndex(next_index_);
  bool is_ready = refs_written_to_stream_.find(object_id)
                  != refs_written_to_stream_.end();
  return {object_id, is_ready};
}
```

**关键**：Peek 不推进 `next_index_`，只查看下一个位置的 ObjectID 和就绪状态。

### 15.3 IsFinished — 判断 EOF

```cpp
bool ObjectRefStream::IsFinished() const {
  return end_of_stream_index_ != -1 && next_index_ >= end_of_stream_index_;
}
```

**只看 `end_of_stream_index_` 和 `next_index_`**，不依赖 memory store 中的哨兵对象。

### 15.4 InsertToStream — 去重

```cpp
bool ObjectRefStream::InsertToStream(const ObjectID &object_id, int64_t item_index) {
  if (end_of_stream_index_ != -1 && item_index >= end_of_stream_index_) { return false; }
  if (item_index < next_index_) { return false; }  // 已消费过
  auto [_, inserted] = refs_written_to_stream_.emplace(object_id);
  if (!inserted) { return false; }  // ★ 已在 set 中 → no-op
  total_num_object_written_ += 1;
  return true;
}
```

### 15.5 Read vs Peek 对比

| 操作 | `next_index_` 变化 | `refs_written_to_stream_` 变化 | 用途 |
|------|-------------------|-------------------------------|------|
| `PeekNextItem` | 不变 | 不变 | 检查下一个元素是否就绪 |
| `TryReadNextItem` | `+=1` | 不变 | 消费下一个元素 |
| `InsertToStream` | 不变 | `emplace` | 写入新元素 |

### 15.6 消费触发背压释放

```cpp
// TaskManager::TryReadObjectRefStream 中:
if (status.ok()) {
    auto total_unconsumed = total_generated - total_consumed;
    if (backpressure_threshold != -1 && total_unconsumed < backpressure_threshold) {
        // ★ 通知执行方可以继续 yield
        for (const auto &execution_signal : it->second) {
            execution_signal(Status::OK(), total_consumed);
        }
        it->second.clear();
    }
}
```

---

## 16. 血缘恢复与 Plasma Store 重建

### 16.1 哪些对象需要重建

```cpp
// CompletePendingTask 首次执行时记录:
if (spec.IsStreamingGenerator()) {
    for (const auto &return_id_info : reply.streaming_generator_return_ids()) {
        if (return_id_info.is_plasma_object()) {
            // ★ 只有 plasma 对象才需要重建
            it->second.reconstructable_return_ids_.insert(
                ObjectID::FromBinary(return_id_info.object_id()));
        }
    }
}
```

直接返回（in-memory）不参与重建——它们在 Driver 本地 memory store 中，不随 Node 丢失。

### 16.2 重建流程

```
1. Node A 死亡 → plasma 对象全部丢失
2. reference_counter_ 检测到不可达 → 触发 lineage reconstruction
3. 重新提交任务到新 Worker
4. Worker 从头执行 generator → 每个 yield 重新通过 Report RPC 上报

Driver HandleReportGeneratorItemReturns:
  → InsertToStream → 重试时 no-op（已有值）
  → HandleTaskReturn → memory store Put no-op
  → 但 plasma 对象: put_in_local_plasma_callback_ → ★ 重建成功

5. 如果重建也失败:
   → MarkTaskReturnObjectsFailed:
     → put_in_local_plasma_callback_(error, object_id)
     → ★ 写入错误对象到本地 plasma store
```

### 16.3 Memory Store vs Plasma Store 覆盖行为

```
Memory Store（不可覆写）:
  Put(OBJECT_IN_PLASMA哨兵, yield_id) → 已存在 → no-op
  哨兵不变

Plasma Store（可以覆盖）:
  Node A 存活: Create(yield_id, data_A) + Seal → data_A
  Node A 死亡: data_A 丢失
  重建时: Create(yield_id, data_A') + Seal → data_A' ★ 同 ObjectID，不同物理数据
  重建失败: Create(yield_id, error) + Seal → 错误对象 ★ 覆盖成功
```

---

## 17. Ray Data 消费者的失败处理

### 17.1 DataOpTask.on_data_ready

**文件**: `physical_operator.py:174-290`

```python
def on_data_ready(self, max_bytes_to_read):
    bytes_read = 0
    while max_bytes_to_read is None or bytes_read < max_bytes_to_read:
        # ① 读 block_ref（timeout=0）
        if self._pending_block_ref.is_nil():
            try:
                self._pending_block_ref = self._streaming_gen._next_sync(timeout_s=0)
            except StopIteration:
                self._task_done_callback(None, ...)  # 正常完成
                self._has_finished = True
                break
            if self._pending_block_ref.is_nil():
                break  # 没有新数据，下次轮询

        # ② 读 metadata_ref
        if self._pending_meta_ref.is_nil():
            try:
                self._pending_meta_ref = self._streaming_gen._next_sync(timeout_s=0.1)
            except StopIteration:
                # ★ metadata 缺失 → block_ref 包含异常
                try:
                    ray.get(self._pending_block_ref)
                    assert False, "Above ray.get should raise"
                except Exception as ex:
                    self._task_done_callback(ex, ...)  # 通知失败
                    raise ex from None

        # ③ ray.get(metadata) + 输出
        meta_with_schema_bytes = ray.get(self._pending_meta_ref, timeout=1.0)
        # ...
```

### 17.2 三种失败在 Ray Data 层的处理

| 异常类型 | `_next_sync` 返回 | Ray Data 行为 | 处理方式 |
|---------|------------------|-------------|---------|
| retryable（有重试） | `nil`（等待中） | 无感知 | 下次轮询继续 |
| non-retryable | 异常 yield 的 ref | 读 metadata 时 StopIteration | `ray.get(block_ref)` 抛异常 → `task_done_callback(ex)` |
| Worker 崩溃（无重试） | `generator_ref`（含异常） | 读 metadata 时 StopIteration | `ray.get(generator_ref)` 抛异常 → `task_done_callback(ex)` |

---

## 18. 关键代码文件索引

| 文件 | 关键内容 |
|------|---------|
| `src/ray/core_worker/task_manager.h` | ObjectRefStream 类定义, TaskManager 接口, MarkRetry, num_successful_executions_ |
| `src/ray/core_worker/task_manager.cc` | CompletePendingTask, FailPendingTask, FailOrRetryPendingTask, RetryTaskIfPossible, MarkEndOfStream, MarkTaskReturnObjectsFailed, MarkTaskNoRetryInternal, HandleReportGeneratorItemReturns, ObjectRefStream 方法 |
| `src/ray/core_worker/store_provider/memory_store/memory_store.cc` | Put 方法（不可覆写机制） |
| `src/ray/core_worker/common.cc` | SerializeReturnObject（nullptr → QuickExit） |
| `src/ray/core_worker/core_worker.cc` | ExecuteTask, HandlePushTask, ReportGeneratorItemReturns, HandleReportGeneratorItemReturns |
| `src/ray/core_worker/task_execution/task_receiver.cc` | HandleTaskExecutionResult, QueueTaskForExecution, execute_callback |
| `src/ray/core_worker/task_submission/normal_task_submitter.cc` | PushNormalTask RPC 回调 |
| `src/ray/core_worker/task_submission/actor_task_submitter.cc` | HandlePushTaskReply |
| `src/ray/core_worker_rpc_client/core_worker_client.cc` | PushNormalTask / PushActorTask gRPC 发送 |
| `src/ray/protobuf/core_worker.proto` | PushTaskReply, StreamingGeneratorReturnIdInfo |
| `python/ray/_raylet.pyx` | store_task_outputs, store_task_errors, execute_streaming_generator_sync/async, report_streaming_generator_output/exception, create_generator_error_object |
| `python/ray/_private/object_ref_generator.py` | ObjectRefGenerator, _next_sync, _next_async |
| `python/ray/data/_internal/execution/interfaces/physical_operator.py` | DataOpTask.on_data_ready |
| `python/ray/data/_internal/execution/operators/task_pool_map_operator.py` | _try_schedule_task (streaming + backpressure) |
