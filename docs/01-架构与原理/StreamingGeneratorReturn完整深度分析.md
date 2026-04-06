# STREAMING_GENERATOR_RETURN 完整工作机制详解

## 一、概述

`STREAMING_GENERATOR_RETURN` 是 Ray 中流式生成器（Streaming Generator）的核心标记常量，定义在 `src/ray/common/constants.h:27`：

```cpp
constexpr int kStreamingGeneratorReturn = -2;
```

当用户使用 `num_returns="streaming"` 时，Ray 将 `num_returns` 设置为 `-2`，触发流式生成器模式。与普通远程调用（调用端等待所有返回值一次性返回）不同，流式生成器允许执行端每 yield 一个值就立即报告给调用端，实现管道化的流式传输。

---

## 二、完整数据流

```
┌─────────────────────────────────────────────────────────────────────┐
│                        调用端 (Caller/Consumer)                      │
│                                                                     │
│  1. 提交任务(num_returns=kStreamingGeneratorReturn=-2)               │
│  2. 获得单个 generator_ref → 包装为 ObjectRefGenerator              │
│  3. 迭代 ObjectRefGenerator:                                        │
│     ├─ try_read_next_object_ref_stream()                            │
│     │  → 读取 ObjectRefStream 中的下一个 ObjectRef                  │
│     │  → 通知 TaskManager 消费数+1 (触发反压释放)                    │
│     └─ 直到收到 END_OF_STREAMING_GENERATOR 信号 → StopIteration     │
└────────────────────────┬────────────────────────────────────────────┘
                         │  gRPC: PushTask
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       执行端 (Executor/Producer)                     │
│                                                                     │
│  1. 识别 is_streaming_generator = true                              │
│  2. 创建 GeneratorBackpressureWaiter                                │
│  3. 循环执行 generator:                                             │
│     ├─ yield value                                                  │
│     │  → report_streaming_generator_output()                        │
│     │     → ReportGeneratorItemReturns gRPC → 发送给调用端           │
│     │     → waiter->IncrementObjectGenerated()                      │
│     │     → waiter->WaitUntilObjectConsumed()  ← 反压阻塞点!        │
│     └─ StopIteration → break                                        │
│  4. waiter->WaitAllObjectsReported()  ← 等待所有 in-flight 报告完成  │
│  5. 返回 PushTaskReply (含 streaming_generator_return_ids)          │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 三、各阶段详细分析

### 阶段 1：任务提交（调用端）

**代码位置：** `python/ray/remote_function.py:410-413`

```python
if num_returns == "streaming":
    num_returns = ray._raylet.STREAMING_GENERATOR_RETURN  # = -2
```

**C++ 端处理：** `src/ray/core_worker/core_worker.cc:2107-2113`

```cpp
bool is_streaming_generator = num_returns == kStreamingGeneratorReturn;  // -2
if (is_streaming_generator) {
    num_returns = 1;           // 实际只有1个主返回值（generator_ref）
    returns_dynamic = true;    // 标记为动态返回
}
```

**关键点：** `num_returns` 从 `-2` 被修正为 `1`，但 `is_streaming_generator=true` 和 `generator_backpressure_num_objects` 被写入 TaskSpec 的 protobuf 中。

**返回给调用端：** 只返回 1 个 `generator_ref`（ObjectRef），不是多个。Python 端将其包装为 `ObjectRefGenerator`：

```python
# python/ray/actor.py:2187-2192
if num_returns == STREAMING_GENERATOR_RETURN:
    assert len(object_refs) == 1
    generator_ref = object_refs[0]
    return ObjectRefGenerator(generator_ref, worker)
```

### 阶段 2：任务执行（执行端）

执行端在 `_raylet.pyx` 中通过 `execute_streaming_generator_sync` / `execute_streaming_generator_async` 执行：

```python
# _raylet.pyx:1291-1301 (简化)
while True:
    try:
        output = gen.send(stats)      # yield 下一个值
        stats = report_streaming_generator_output(context, output, gen_index, None)
        gen_index += 1
    except StopIteration:
        break
```

### 阶段 3：单个值的报告流程（核心机制）

每次 yield 时，`report_streaming_generator_output` 被调用（`_raylet.pyx:1131`）：

**Step 3.1：** 将输出序列化为 RayObject

```python
create_generator_return_obj(output, context.generator_id, ..., generator_index, ...)
```

**Step 3.2：** 通过 C++ gRPC 发送 `ReportGeneratorItemReturns` 请求给调用端

```cpp
// core_worker.cc:3431-3453
waiter->IncrementObjectGenerated();   // 生成计数+1

client->ReportGeneratorItemReturns(
    std::move(request),
    [waiter, ...](const Status &status, const Reply &reply) {
        int64_t num_objects_consumed = reply.total_num_object_consumed();
        waiter->HandleObjectReported(num_objects_consumed);  // 更新消费计数
    });
```

**Step 3.3：** 反压等待 — 这是关键！

```cpp
// core_worker.cc:3457
return waiter->WaitUntilObjectConsumed();
```

### 阶段 4：调用端接收报告

调用端的 `HandleReportGeneratorItemReturns`（`core_worker.cc:3460`）被触发：

```cpp
void CoreWorker::HandleReportGeneratorItemReturns(request, reply, send_reply_callback) {
    task_manager_->HandleReportGeneratorItemReturns(
        request,
        /*execution_signal_callback=*/
        [reply, send_reply_callback](const Status &status, int64_t total_num_object_consumed) {
            reply->set_total_num_object_consumed(total_num_object_consumed);
            send_reply_callback(status, nullptr, nullptr);  // 回复执行端
        });
}
```

在 `TaskManager::HandleReportGeneratorItemReturns`（`task_manager.cc:780`）中：

1. 写入 ObjectRefStream：将新的 ObjectID 插入流中
2. 处理返回对象：通过 `HandleTaskReturn` 存储对象值
3. 反压判断：

```cpp
// task_manager.cc:863-878
if (backpressure_threshold != -1 &&
    (item_index - stream_it->second.LastConsumedIndex()) >= backpressure_threshold) {
    // 未消费数 >= 阈值 → 不回复执行端（反压！）
    // 将回调挂到 ref_stream_execution_signal_callbacks_ 等待消费触发
    signal_it->second.push_back(execution_signal_callback);
} else {
    // 未消费数 < 阈值 → 立即回复，让执行端继续
    execution_signal_callback(Status::OK(), total_consumed);
}
```

### 阶段 5：调用端消费 → 释放反压

当调用端调用 `next(gen)` 或 `gen.__anext__()` 时，`ObjectRefGenerator._next_sync()` 被触发：

```python
# object_ref_generator.py:224
ref = core_worker.try_read_next_object_ref_stream(self._generator_ref)
```

这调用 C++ 的 `TaskManager::TryReadObjectRefStream`（`task_manager.cc:637`）：

```cpp
Status TaskManager::TryReadObjectRefStream(const ObjectID &generator_id, ...) {
    auto status = stream_it->second.TryReadNextItem(object_id_out);  // 读取下一个

    if (status.ok()) {
        auto total_consumed = stream_it->second.TotalNumObjectConsumed();
        auto total_unconsumed = total_generated - total_consumed;
        if (backpressure_threshold != -1 && total_unconsumed < backpressure_threshold) {
            // 消费后低于阈值 → 触发信号回调，回复执行端
            for (const auto &signal : ref_stream_execution_signal_callbacks_[generator_id]) {
                signal(Status::OK(), total_consumed);  // 发送 gRPC 回复！
            }
            callbacks.clear();
        }
    }
}
```

### 阶段 6：流结束

当 generator 执行完毕（StopIteration）后：

1. **执行端：** 调用 `waiter->WaitAllObjectsReported()` — 确保所有 in-flight 的报告都已收到回复
2. **执行端：** 返回 PushTaskReply，其中包含 `streaming_generator_return_ids`（所有已生成 ObjectRef 的 ID 和是否存储在 Plasma）
3. **调用端：** `CompletePendingTask` 处理 reply，调用 `MarkEndOfStream`
4. **MarkEndOfStream：** 在流的末尾写入一个 `END_OF_STREAMING_GENERATOR` 错误对象作为哨兵

```cpp
// task_manager.cc:771-777
RayObject error(rpc::ErrorType::END_OF_STREAMING_GENERATOR);
in_memory_store_.Put(error, last_object_id, ...);
```

5. **调用端：** 当 `try_read_next_object_ref_stream` 读取到这个哨兵时，抛出 `ObjectRefStreamEndOfStreamError`，Python 端捕获后转为 `StopIteration`

---

## 四、反压机制详细分析

`STREAMING_GENERATOR_RETURN` 有反压机制！它由 `generator_backpressure_num_objects` 参数控制。

### 反压参数

- 定义在 proto：`int64 generator_backpressure_num_objects = 38;`
- 默认值 `-1`：禁用反压（生成器不限速）
- `> 0`：当未消费对象数达到此阈值时暂停生成器
- 不允许 `= 0`：会触发 `RAY_CHECK_NE(result, 0)`

### 反压工作原理（双向通信协议）

```
执行端 (Producer)                          调用端 (Consumer)
    │                                            │
    │  ── ReportGeneratorItemReturns gRPC ──►     │
    │     (item_index, object_data)               │
    │                                             │
    │     waiter->IncrementObjectGenerated()      │
    │     waiter->WaitUntilObjectConsumed()  ◀──  │  判断: 未消费数 >= 阈值?
    │         │                                   │  ├── YES: 不回复, 挂起callback
    │         │  阻塞等待...                       │  └── NO:  立即回复 total_consumed
    │         │                                   │
    │     ◀── gRPC Reply ────────────────────     │
    │     (total_num_object_consumed)             │
    │                                             │
    │     waiter->HandleObjectReported(           │  当消费者调用 next(gen):
    │         total_consumed)                     │  TryReadObjectRefStream()
    │     → 如果未消费 < 阈值, SignalAll()        │  → 检查未消费 < 阈值
    │     → WaitUntilObjectConsumed 解除阻塞      │  → 触发 signal callback
    │                                             │  → 回复执行端
```

### GeneratorBackpressureWaiter 核心逻辑

**执行端阻塞（`generator_waiter.cc:32-57`）：**

```cpp
Status GeneratorBackpressureWaiter::WaitUntilObjectConsumed() {
    if (backpressure_threshold_ < 0) return Status::OK();  // -1 = 禁用
    
    auto total_object_unconsumed = total_objects_generated_ - total_objects_consumed_;
    if (total_object_unconsumed >= backpressure_threshold_) { 
        // 阻塞! 等待条件变量
        while (total_object_unconsumed >= backpressure_threshold_) {
            backpressure_cond_var_.WaitWithTimeout(&mutex_, absl::Seconds(1));
            total_object_unconsumed = total_objects_generated_ - total_objects_consumed_;
        }   
    }   
}   
```

**消费端释放（`generator_waiter.cc:78-96`）：**

```cpp
void GeneratorBackpressureWaiter::HandleObjectReported(int64_t total_objects_consumed) {
    num_object_reports_in_flight_--;
    total_objects_consumed_ = std::max(total_objects_consumed, total_objects_consumed_);
    auto total_object_unconsumed = total_objects_generated_ - total_objects_consumed_;
    if (total_object_unconsumed < backpressure_threshold_) {
        backpressure_cond_var_.SignalAll();  // 唤醒阻塞的生产者!
    }   
}
```

### 反压的双重保障

反压实际上在两个层面同时生效：

1. **调用端 TaskManager 层面：** 通过延迟发送 gRPC reply 实现反压
   - 当未消费数 ≥ 阈值时，不发送 gRPC 回复，将 callback 挂起
   - 当消费后未消费数 < 阈值时，发送回复

2. **执行端 GeneratorBackpressureWaiter 层面：** 通过条件变量阻塞
   - `WaitUntilObjectConsumed()` 在 yield 后调用
   - 当 `total_generated - total_consumed >= threshold` 时阻塞
   - 当收到 gRPC reply 中的 `total_consumed` 更新后，如果低于阈值则唤醒

### 反压的边界情况处理

1. **gRPC 失败：** 如果报告失败，执行端假设所有对象都已消费，解除阻塞
```cpp
// core_worker.cc:3444-3446
if (!status.ok()) {
    num_objects_consumed = waiter->TotalObjectGenerated();  // 视为全部消费
}
```

2. **生成器被删除：** 如果消费端删除了 ObjectRefGenerator，需要通知执行端停止阻塞
```cpp
// task_manager.cc:698-699
for (const auto &signal : signal_it->second) {
    signal(Status::NotFound("Stream is deleted."), -1);  // 错误状态解除阻塞
}
```

3. **任务重试：** 使用 `attempt_number` 过滤过期报告，避免旧尝试的报告干扰新的执行

4. **所有对象报告完毕后才结束任务：**
```python
# _raylet.pyx:1312
return_status = context.waiter.get().WaitAllObjectsReported()
```
这确保所有 in-flight 的 gRPC 报告都收到回复后，才返回 PushTaskReply，避免调用端永远收不到某些 ObjectRef 的值。

---

## 五、ObjectID 的确定性生成

流式生成器的 ObjectID 是确定性的，这对反压和重试机制至关重要：

```cpp
// task_manager.cc:232-236
ObjectID ObjectRefStream::GetObjectRefAtIndex(int64_t generator_index) const {
    // Index 1 is reserved for the first task return (generator_ref itself).
    return ObjectID::FromIndex(generator_task_id_, 2 + generator_index);
}
```

- Index 0：不使用
- Index 1：`generator_ref`（任务主返回值）
- Index 2, 3, 4, ...：每个 yield 的 ObjectRef

这意味着调用端可以在值实际产生之前就预知下一个 ObjectRef 的 ID（通过 `peek_object_ref_stream`），这对于 `ray.wait()` 等操作很关键。

---

## 六、完整代码逻辑逐行解析

### 全局架构图

```
┌───────────────────────────────────────────────────────────────────────────────┐
│                          调用端 (Consumer)                                    │
│                                                                               │
│  remote_function.py / actor.py                                                │
│     │  num_returns == "streaming" → kStreamingGeneratorReturn(-2)             │
│     ▼                                                                         │
│  _raylet.pyx: submit_task()                                                   │
│     │  → C++ CoreWorker::SubmitTask()                                         │
│     │     → BuildCommonTaskSpec(builder, ..., num_returns=-2, ...)            │
│     │        → num_returns=1, is_streaming_generator=true                     │
│     │        → protobuf: streaming_generator=true                             │
│     │           generator_backpressure_num_objects=N                          │
│     │     → TaskManager::AddPendingTask(spec)                                 │
│     │        → 创建 ObjectRefStream(generator_id)                             │
│     │        → 返回 1 个 generator_ref                                        │
│     ▼                                                                         │
│  ObjectRefGenerator(generator_ref, worker)                                    │
│     │  next(gen) → _next_sync()                                               │
│     │    → peek_object_ref_stream() → PeekObjectRefStream()                   │
│     │    → ray.wait() 等待 ObjectRef 就绪                                     │
│     │    → try_read_next_object_ref_stream() → TryReadObjectRefStream()       │
│     │       → ObjectRefStream.TryReadNextItem()                               │
│     │       → 触发反压释放信号                                                 │
│     └── → END_OF_STREAMING_GENERATOR → StopIteration                         │
└───────────────────────────────────────────────────────────────────────────────┘

┌───────────────────────────────────────────────────────────────────────────────┐
│                          执行端 (Producer)                                    │
│                                                                               │
│  CoreWorker::ExecuteTask(spec)                                                │
│     │  spec.IsStreamingGenerator() == true                                    │
│     │  → task_execution_callback(is_streaming_generator=true,                 │
│     │       generator_backpressure_num_objects=N)                             │
│     ▼                                                                         │
│  _raylet.pyx: execute_task()                                                  │
│     │  is_streaming_generator=true                                            │
│     │  → StreamingGeneratorExecutionContext.make(                             │
│     │       generator_id, ..., generator_backpressure_num_objects)            │
│     │  → context.initialize(generator)                                        │
│     │  → execute_streaming_generator_sync(context) 或                         │
│     │    execute_streaming_generator_async(context)                           │
│     ▼                                                                         │
│  循环:                                                                        │
│     │  gen.send(stats) → yield output                                         │
│     │  → report_streaming_generator_output(context, output, gen_index)        │
│     │     → create_generator_return_obj() → 序列化为 RayObject                │
│     │     → C++ ReportGeneratorItemReturns(gRPC) → 发送给调用端               │
│     │     → waiter->WaitUntilObjectConsumed() ← 反压阻塞点!                   │
│     │  gen_index += 1                                                          │
│     └── StopIteration → break                                                 │
│                                                                               │
│  waiter->WaitAllObjectsReported()  ← 等所有 in-flight 报告完成                 │
│  返回 PushTaskReply (含 streaming_generator_return_ids)                       │
└───────────────────────────────────────────────────────────────────────────────┘
```

---

### 第一步：Python 入口 — 定义远程生成器函数

**用户代码：**

```python
@ray.remote(num_returns="streaming")
def gen():
    for i in range(10):
        yield i

generator = gen.remote()  # 返回 ObjectRefGenerator
for ref in generator:
    print(ray.get(ref))
```

#### 1.1 常量传递链

```
C++:     src/ray/common/constants.h:27
           constexpr int kStreamingGeneratorReturn = -2;
                │
Cython:  python/ray/includes/common.pxd:861
           cdef int kStreamingGeneratorReturn    (extern from "ray/common/constants.h")
                │
         python/ray/includes/common.pxi:149
           STREAMING_GENERATOR_RETURN = kStreamingGeneratorReturn
                │
Python:  python/ray/remote_function.py:21
           from ... import STREAMING_GENERATOR_RETURN
         python/ray/actor.py:44
           from ... import STREAMING_GENERATOR_RETURN
```

#### 1.2 远程函数提交（`remote_function.py:400-418`）

```python
num_returns = task_options["num_returns"]
if num_returns is None:
    if self._is_generator:
        num_returns = "streaming"       # ← 生成器函数默认用 streaming
    else:
        num_returns = 1

if num_returns == "streaming":
    num_returns = ray._raylet.STREAMING_GENERATOR_RETURN   # = -2

generator_backpressure_num_objects = task_options["_generator_backpressure_num_objects"]
if generator_backpressure_num_objects is None:
    generator_backpressure_num_objects = -1  # ← 默认禁用反压
```

#### 1.3 Cython → C++ 提交（`_raylet.pyx:3471-3563`）

```python
def submit_task(self, ..., int num_returns, int64_t generator_backpressure_num_objects, ...):
    task_options = CTaskOptions(
        name, num_returns, c_resources, b"",
        generator_backpressure_num_objects,
        ...)

    with nogil:
        return_refs = CCoreWorkerProcess.GetCoreWorker().SubmitTask(
            ray_function, args_vector, task_options, ...)
```

---

### 第二步：C++ 调用端 — 任务构建与提交

#### 2.1 CoreWorker::SubmitTask（`core_worker.cc:2168-2250`）

```cpp
std::vector<rpc::ObjectReference> CoreWorker::SubmitTask(...) {
    TaskSpecBuilder builder;
    BuildCommonTaskSpec(builder, ...,
        task_options.num_returns,              // = -2
        ...,
        task_options.generator_backpressure_num_objects,  // = -1 或用户设定值
        ...);
    TaskSpecification task_spec = std::move(builder).ConsumeAndBuild();
    returned_refs = task_manager_->AddPendingTask(..., task_spec, ...);
    return returned_refs;
}
```

#### 2.2 BuildCommonTaskSpec — 核心转换逻辑（`core_worker.cc:2089-2143`）

```cpp
void CoreWorker::BuildCommonTaskSpec(..., int num_returns, ..., 
    int64_t generator_backpressure_num_objects, ...) {

    bool returns_dynamic = num_returns == -1;
    if (returns_dynamic) {
        num_returns = 1;
    }

    // ★★★ 核心分支：识别 streaming generator ★★★
    bool is_streaming_generator = num_returns == kStreamingGeneratorReturn;  // -2
    if (is_streaming_generator) {
        num_returns = 1;           // 实际只有 1 个主返回值
        returns_dynamic = true;    // 也标记为动态返回
    }
    RAY_CHECK(num_returns >= 0);   // num_returns 现在是 1

    builder.SetCommonTaskSpec(...,
        num_returns, returns_dynamic, is_streaming_generator,
        generator_backpressure_num_objects, ...);
}
```

#### 2.3 TaskSpecBuilder::SetCommonTaskSpec — 写入 protobuf（`task_util.h:132-194`）

```cpp
TaskSpecBuilder &SetCommonTaskSpec(...,
    uint64_t num_returns,           // = 1
    bool returns_dynamic,           // = true
    bool is_streaming_generator,    // = true
    int64_t generator_backpressure_num_objects, ...) {

    message_->set_num_returns(num_returns);                     // 1
    message_->set_returns_dynamic(returns_dynamic);             // true
    message_->set_streaming_generator(is_streaming_generator);  // true
    message_->set_generator_backpressure_num_objects(
        generator_backpressure_num_objects);                     // -1 或用户设定
}
```

**写入 protobuf 后，TaskSpec 的关键字段：**

| 字段 | 值 |
|------|----|
| `num_returns` | 1 |
| `returns_dynamic` | true |
| `streaming_generator` | true |
| `generator_backpressure_num_objects` | -1（或 N） |

#### 2.4 TaskManager::AddPendingTask — 创建流和引用（`task_manager.cc:238-352`）

```cpp
std::vector<rpc::ObjectReference> TaskManager::AddPendingTask(...) {
    size_t num_returns = spec.NumReturns();  // = 1
    for (size_t i = 0; i < num_returns; i++) {
        auto return_id = spec.ReturnId(i);
        reference_counter_.AddOwnedObject(return_id, ..., /*add_local_ref=*/true, ...);
        rpc::ObjectReference ref;
        ref.set_object_id(return_id.Binary());
        ref.mutable_owner_address()->CopyFrom(caller_address);
        returned_refs.push_back(std::move(ref));
    }

    // ★★★ 创建 ObjectRefStream ★★★
    if (spec.IsStreamingGenerator()) {
        const auto generator_id = spec.ReturnId(0);
        absl::MutexLock lock(&object_ref_stream_ops_mu_);
        auto inserted = object_ref_streams_.emplace(
            generator_id, ObjectRefStream(generator_id));
        ref_stream_execution_signal_callbacks_.emplace(
            generator_id, std::vector<ExecutionSignalCallback>());
        RAY_CHECK(inserted.second);
    }

    submissible_tasks_.try_emplace(spec.TaskId(), spec, max_retries, ...);
    return returned_refs;  // 返回 1 个 generator_ref
}
```

#### 2.5 Python 包装返回值（`actor.py:2187-2192`）

```python
if num_returns == STREAMING_GENERATOR_RETURN:  # -2
    assert len(object_refs) == 1               # 只有1个 ref
    generator_ref = object_refs[0]
    return ObjectRefGenerator(generator_ref, worker)
```

---

### 第三步：执行端 — 任务调度与识别

#### 3.1 任务到达执行端

```cpp
Status CoreWorker::ExecuteTask(const TaskSpecification &task_spec, ...) {
    Status status = options_.task_execution_callback(
        task_spec.CallerAddress(),
        task_type,
        // ...
        streaming_generator_returns,     // ← 输出参数
        /*is_streaming_generator=*/task_spec.IsStreamingGenerator(),    // true
        /*generator_backpressure_num_objects=*/task_spec.GeneratorBackpressureNumObjects(),
        ...);
}
```

#### 3.2 Python 任务执行回调（`_raylet.pyx:1820-1872`）

```python
if is_streaming_generator:
    assert returns[0].size() == 1

    is_async_gen = inspect.isasyncgen(outputs)
    is_sync_gen = inspect.isgenerator(outputs)

    if (not is_sync_gen and not is_async_gen):
        raise ValueError("Functions with @ray.remote(num_returns=\"streaming\" "
                         "must return a generator")

    # ★★★ 创建执行上下文 ★★★
    context = StreamingGeneratorExecutionContext.make(
        returns[0][0].first,
        task_type,
        caller_address,
        task_id,
        ...,
        generator_backpressure_num_objects)

    context.initialize(outputs)

    if is_async_gen:
        if generator_backpressure_num_objects != -1:
            raise ValueError("_generator_backpressure_num_objects is "
                             "not supported for an async actor.")
        core_worker.run_async_func_or_coro_in_event_loop(
            execute_streaming_generator_async(context), ...)
    else:
        # ★★★ 同步执行 ★★★
        execute_streaming_generator_sync(context)

    outputs = None  # generator 输出已被流式报告，不需要返回
```

#### 3.3 StreamingGeneratorExecutionContext.make（`_raylet.pyx:1076-1123`）

```python
@staticmethod
cdef make(const CObjectID &generator_id, ...,
          int64_t generator_backpressure_num_objects):
    cdef StreamingGeneratorExecutionContext self = StreamingGeneratorExecutionContext()
    self.generator_id = generator_id
    self.caller_address = caller_address
    self.streaming_generator_returns = streaming_generator_returns

    # ★★★ 创建反压等待器 ★★★
    self.waiter = make_shared[CGeneratorBackpressureWaiter](
        generator_backpressure_num_objects,
        check_signals)
    return self
```

`GeneratorBackpressureWaiter` 构造函数（`generator_waiter.cc:23-29`）：

```cpp
GeneratorBackpressureWaiter::GeneratorBackpressureWaiter(
    int64_t generator_backpressure_num_objects,
    std::function<Status()> check_signals)
    : backpressure_threshold_(generator_backpressure_num_objects),
      check_signals_(std::move(check_signals)) {
    RAY_CHECK_NE(generator_backpressure_num_objects, 0);  // 不允许 0
}
```

---

### 第四步：执行端 — 生成器主循环

#### 4.1 同步执行（`_raylet.pyx:1262-1313`）

```python
cdef execute_streaming_generator_sync(StreamingGeneratorExecutionContext context):
    cdef:
        int64_t gen_index = 0
        CRayStatus return_status

    gen = context.generator

    try:
        stats = None
        while True:
            try:
                output = gen.send(stats)
                stats = report_streaming_generator_output(
                    context, output, gen_index, None)
                gen_index += 1
            except StopIteration:
                break
    except Exception as e:
        report_streaming_generator_exception(context, e, gen_index, None)

    # ★★★ 等待所有 in-flight 报告完成 ★★★
    with nogil:
        return_status = context.waiter.get().WaitAllObjectsReported()
    check_status(return_status)
```

#### 4.2 异步执行（`_raylet.pyx:1316-1388`）

```python
async def execute_streaming_generator_async(context):
    gen = context.generator
    loop = asyncio.get_running_loop()
    executor = worker.core_worker.get_event_loop_executor()

    try:
        stats = None
        while True:
            try:
                output = await gen.asend(stats)
                stats = await loop.run_in_executor(
                    executor,
                    report_streaming_generator_output,
                    context, output, cur_generator_index, interrupt_signal_event)
                cur_generator_index += 1
            except StopAsyncIteration:
                break
    except Exception as e:
        report_streaming_generator_exception(context, e, cur_generator_index, None)

    return_status = context.waiter.get().WaitAllObjectsReported()
```

---

### 第五步：每次 yield — 报告单个结果

#### 5.1 `report_streaming_generator_output`（`_raylet.pyx:1131-1194`）

```python
cdef report_streaming_generator_output(
    StreamingGeneratorExecutionContext context,
    output,          # yield 出来的值
    generator_index, # 当前是第几个 yield
    interrupt_signal_event):

    # ★ Step 5.2: 将 Python 对象序列化为 RayObject ★
    create_generator_return_obj(
        output,
        context.generator_id,
        worker,
        context.caller_address,
        context.task_id,
        context.return_size,        # = 1
        generator_index,
        context.is_async,
        &return_obj)

    del output  # 立即释放 Python 对象引用

    # 记录到 streaming_generator_returns 输出列表
    context.streaming_generator_returns[0].push_back(
        c_pair[CObjectID, c_bool](
            return_obj.first,                    # ObjectID
            is_plasma_object(return_obj.second))) # 是否存在 Plasma 中

    # ★ Step 5.3: 通过 gRPC 报告给调用端 ★
    with nogil:
        check_status(CCoreWorkerProcess.GetCoreWorker().ReportGeneratorItemReturns(
            return_obj,
            context.generator_id,
            context.caller_address,
            generator_index,
            context.attempt_number,
            context.waiter))    # ← 传入反压等待器
```

#### 5.2 `create_generator_return_obj` — 序列化（`_raylet.pyx:1420-1466`）

```python
cdef create_generator_return_obj(
    output, const CObjectID &generator_id, worker,
    const CAddress &caller_address, TaskID task_id,
    return_size, generator_index, is_async,
    c_pair[CObjectID, shared_ptr[CRayObject]] *return_object):

    # ★★★ 分配确定性的动态返回 ID ★★★
    return_id = core_worker.allocate_dynamic_return_id_for_generator(
        caller_address,
        task_id.native(),
        return_size,          # = 1
        generator_index,      # 0, 1, 2, ...
        is_async,
    )
    # allocate_dynamic_return_id_for_generator 内部:
    #   ObjectID::FromIndex(task_id, 1 + return_size + generator_index)
    #   = ObjectID::FromIndex(task_id, 2 + generator_index)
    # 
    # 所以: yield 0 → Index 2, yield 1 → Index 3, ...
    # Index 1 被保留给 generator_ref（任务主返回值）

    intermediate_result.push_back(
        c_pair[CObjectID, shared_ptr[CRayObject]](
            return_id, shared_ptr[CRayObject]()))

    core_worker.store_task_outputs(
        worker, [output], caller_address,
        &intermediate_result, generator_id.Binary())

    return_object[0] = intermediate_result.back()
```

#### 5.3 `CoreWorker::ReportGeneratorItemReturns`（`core_worker.cc:3396-3458`）

```cpp
Status CoreWorker::ReportGeneratorItemReturns(
    const std::pair<ObjectID, std::shared_ptr<RayObject>> &dynamic_return_object,
    const ObjectID &generator_id,
    const rpc::Address &caller_address,
    int64_t item_index,
    uint64_t attempt_number,
    const std::shared_ptr<GeneratorBackpressureWaiter> &waiter) {

    // ★ Step A: 构建 gRPC 请求 ★
    rpc::ReportGeneratorItemReturnsRequest request;
    request.mutable_worker_addr()->CopyFrom(rpc_address_);
    request.set_item_index(item_index);
    request.set_generator_id(generator_id.Binary());
    request.set_attempt_number(attempt_number);

    auto client = core_worker_client_pool_->GetOrConnect(caller_address);

    // 序列化返回对象
    SerializeReturnObject(dynamic_return_object.first,
                          dynamic_return_object.second,
                          request.mutable_returned_object());

    // ★ Step B: 递增生成计数 ★
    waiter->IncrementObjectGenerated();

    // ★ Step C: 异步发送 gRPC，注册回调 ★
    client->ReportGeneratorItemReturns(
        std::move(request),
        [waiter, generator_id, return_id, item_index](
            const Status &status, const rpc::ReportGeneratorItemReturnsReply &reply) {
            int64_t num_objects_consumed = 0;
            if (status.ok()) {
                num_objects_consumed = reply.total_num_object_consumed();
            } else {
                num_objects_consumed = waiter->TotalObjectGenerated();
            }
            waiter->HandleObjectReported(num_objects_consumed);
        });

    // ★ Step D: 反压等待 ★
    return waiter->WaitUntilObjectConsumed();
}
```

#### 5.4 `GeneratorBackpressureWaiter::WaitUntilObjectConsumed`（`generator_waiter.cc:32-57`）

```cpp
Status GeneratorBackpressureWaiter::WaitUntilObjectConsumed() {
    if (backpressure_threshold_ < 0) {
        RAY_CHECK_EQ(backpressure_threshold_, -1);
        return Status::OK();  // -1 表示反压禁用
    }

    absl::MutexLock lock(&mutex_);
    auto return_status = Status::OK();
    auto total_object_unconsumed = total_objects_generated_ - total_objects_consumed_;

    if (total_object_unconsumed >= backpressure_threshold_) {
        // ★★★ 超过阈值，阻塞! ★★★
        while (total_object_unconsumed >= backpressure_threshold_) {
            backpressure_cond_var_.WaitWithTimeout(&mutex_, absl::Seconds(1));
            total_object_unconsumed = total_objects_generated_ - total_objects_consumed_;
            return_status = check_signals_();  // 检查是否应该中断
            if (!return_status.ok()) break;
        }
    }
    return return_status;
}
```

#### 5.5 `GeneratorBackpressureWaiter::HandleObjectReported`（`generator_waiter.cc:78-96`）

```cpp
void GeneratorBackpressureWaiter::HandleObjectReported(int64_t total_objects_consumed) {
    absl::MutexLock lock(&mutex_);

    num_object_reports_in_flight_--;

    if (num_object_reports_in_flight_ <= 0) {
        all_objects_reported_cond_var_.SignalAll();  // 唤醒 WaitAllObjectsReported
    }

    total_objects_consumed_ = std::max(total_objects_consumed, total_objects_consumed_);

    // ★★★ 检查是否低于阈值，唤醒阻塞的生产者 ★★★
    auto total_object_unconsumed = total_objects_generated_ - total_objects_consumed_;
    if (total_object_unconsumed < backpressure_threshold_) {
        backpressure_cond_var_.SignalAll();
    }
}
```

---

### 第六步：调用端 — 接收报告

#### 6.1 `CoreWorker::HandleReportGeneratorItemReturns`（`core_worker.cc:3460-3485`）

```cpp
void CoreWorker::HandleReportGeneratorItemReturns(
    rpc::ReportGeneratorItemReturnsRequest request,
    rpc::ReportGeneratorItemReturnsReply *reply,
    rpc::SendReplyCallback send_reply_callback) {

    auto generator_id = ObjectID::FromBinary(request.generator_id());

    task_manager_->HandleReportGeneratorItemReturns(
        request,
        [reply, send_reply_callback = std::move(send_reply_callback)](
            const Status &status, int64_t total_num_object_consumed) {
            reply->set_total_num_object_consumed(total_num_object_consumed);
            send_reply_callback(status, nullptr, nullptr);
        });
}
```

#### 6.2 `TaskManager::HandleReportGeneratorItemReturns`（`task_manager.cc:780-880`）

这是反压的核心决策点：

```cpp
bool TaskManager::HandleReportGeneratorItemReturns(
    const rpc::ReportGeneratorItemReturnsRequest &request,
    const ExecutionSignalCallback &execution_signal_callback) {

    const auto &generator_id = ObjectID::FromBinary(request.generator_id());
    int64_t item_index = request.item_index();
    int64_t attempt_number = request.attempt_number();
    auto backpressure_threshold = -1;

    {
        absl::MutexLock lock(&mu_);
        auto it = submissible_tasks_.find(task_id);
        if (it != submissible_tasks_.end()) {
            backpressure_threshold = it->second.spec_.GeneratorBackpressureNumObjects();
            // 过滤过期尝试的报告
            if (it->second.spec_.AttemptNumber() > attempt_number) {
                execution_signal_callback(
                    Status::NotFound("Stale object reports from the previous attempt."), -1);
                return false;
            }
        }
    }

    absl::MutexLock lock(&object_ref_stream_ops_mu_);
    auto stream_it = object_ref_streams_.find(generator_id);

    // ★ Step A: 将返回对象写入流 ★
    if (request.has_returned_object()) {
        const auto object_id = ObjectID::FromBinary(returned_object.object_id());
        auto index_not_used_yet = stream_it->second.InsertToStream(object_id, item_index);

        if (index_not_used_yet) {
            reference_counter_.OwnDynamicStreamingTaskReturnRef(object_id, generator_id);
        }
        reference_counter_.UpdateObjectPendingCreation(object_id, false);
        HandleTaskReturn(object_id, returned_object, ...);
    }

    // ★ Step B: 反压决策 ★
    auto total_generated = stream_it->second.TotalNumObjectWritten();
    auto total_consumed = stream_it->second.TotalNumObjectConsumed();

    if (stream_it->second.IsObjectConsumed(item_index)) {
        execution_signal_callback(Status::OK(), total_consumed);
        return false;
    }

    // ★★★ 核心反压逻辑 ★★★
    if (backpressure_threshold != -1 &&
        (item_index - stream_it->second.LastConsumedIndex()) >= backpressure_threshold) {
        // 未消费数 >= 阈值 → 不回复! 将回调挂起
        auto signal_it = ref_stream_execution_signal_callbacks_.find(generator_id);
        if (signal_it == ref_stream_execution_signal_callbacks_.end()) {
            execution_signal_callback(Status::NotFound("Stream is deleted."), -1);
        } else {
            signal_it->second.push_back(execution_signal_callback);
        }
    } else {
        // 未消费数 < 阈值 → 立即回复，让执行端继续
        execution_signal_callback(Status::OK(), total_consumed);
    }
}
```

#### 6.3 `ObjectRefStream::InsertToStream`（`task_manager.cc:181-210`）

```cpp
bool ObjectRefStream::InsertToStream(const ObjectID &object_id, int64_t item_index) {
    RAY_CHECK_EQ(object_id, GetObjectRefAtIndex(item_index));  // 验证确定性 ID

    if (end_of_stream_index_ != -1 && item_index >= end_of_stream_index_) {
        return false;  // 流已结束
    }
    if (item_index < next_index_) {
        return false;  // 已被消费
    }

    auto [_, inserted] = refs_written_to_stream_.emplace(object_id);
    if (!inserted) {
        return false;  // 已存在
    }

    max_index_seen_ = std::max(max_index_seen_, item_index);
    total_num_object_written_ += 1;
    return true;
}
```

---

### 第七步：调用端 — 消费 ObjectRef

#### 7.1 `ObjectRefGenerator._next_sync`（`object_ref_generator.py:188-242`）

```python
def _next_sync(self, timeout_s=None):
    core_worker = self.worker.core_worker

    # ★ Step A: 预览下一个 ObjectRef ★
    expected_ref, is_ready = core_worker.peek_object_ref_stream(self._generator_ref)

    if not is_ready:
        _, unready = ray.wait([expected_ref], timeout=timeout_s, fetch_local=False)
        if len(unready) > 0:
            return ray.ObjectRef.nil()  # 超时返回空

    # ★ Step B: 从流中读取下一个 ObjectRef ★
    try:
        ref = core_worker.try_read_next_object_ref_stream(self._generator_ref)
        assert not ref.is_nil()
    except ObjectRefStreamEndOfStreamError:
        # ★ 到达流末尾 ★
        if self._generator_task_raised:
            raise StopIteration from None
        try:
            ray.get(self._generator_ref)  # 检查 generator task 是否异常
        except Exception:
            self._generator_task_raised = True
            return self._generator_ref
        else:
            raise StopIteration from None
    return ref
```

#### 7.2 C++ `TryReadObjectRefStream` — 触发反压释放（`task_manager.cc:637-679`）

```cpp
Status TaskManager::TryReadObjectRefStream(
    const ObjectID &generator_id, ObjectID *object_id_out) {

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

    // ★ Step A: 读取下一个 item ★
    auto status = stream_it->second.TryReadNextItem(object_id_out);

    // ★ Step B: 如果读取成功，检查是否需要释放反压 ★
    if (status.ok()) {
        auto total_generated = stream_it->second.TotalNumObjectWritten();
        auto total_consumed = stream_it->second.TotalNumObjectConsumed();
        auto total_unconsumed = total_generated - total_consumed;

        if (backpressure_threshold != -1 && total_unconsumed < backpressure_threshold) {
            // ★★★ 消费后低于阈值，触发挂起的回调! ★★★
            auto it = ref_stream_execution_signal_callbacks_.find(generator_id);
            if (it != ref_stream_execution_signal_callbacks_.end()) {
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

#### 7.3 `ObjectRefStream::TryReadNextItem`（`task_manager.cc:129-155`）

```cpp
Status ObjectRefStream::TryReadNextItem(ObjectID *object_id_out) {
    *object_id_out = GetObjectRefAtIndex(next_index_);  // 确定性的 ID

    if (IsFinished()) {
        return Status::ObjectRefEndOfStream("");  // 流结束
    }

    auto it = refs_written_to_stream_.find(*object_id_out);
    if (it != refs_written_to_stream_.end()) {
        // ★ 对象已写入，可以消费 ★
        total_num_object_consumed_ += 1;
        next_index_ += 1;
        return Status::OK();
    } else {
        *object_id_out = ObjectID::Nil();
        return Status::OK();  // 对象还没写入
    }
}
```

---

### 第八步：生成器结束 — 流终止

#### 8.1 执行端等待所有报告完成

```python
# _raylet.pyx:1311-1313
with nogil:
    return_status = context.waiter.get().WaitAllObjectsReported()
check_status(return_status)
```

```cpp
// generator_waiter.cc:59-70
Status GeneratorBackpressureWaiter::WaitAllObjectsReported() {
    absl::MutexLock lock(&mutex_);
    while (num_object_reports_in_flight_ > 0) {
        all_objects_reported_cond_var_.WaitWithTimeout(&mutex_, absl::Seconds(1));
        auto return_status = check_signals_();
        if (!return_status.ok()) break;
    }
    return return_status;
}
```

#### 8.2 执行端写入 `streaming_generator_returns` 到 PushTaskReply

```cpp
// task_receiver.cc:54-60
for (const auto &it : result.streaming_generator_returns) {
    const auto &object_id = it.first;
    bool is_plasma_object = it.second;
    auto return_id_proto = reply->add_streaming_generator_return_ids();
    return_id_proto->set_object_id(object_id.Binary());
    return_id_proto->set_is_plasma_object(is_plasma_object);
}
```

#### 8.3 调用端 `CompletePendingTask` 处理流结束（`task_manager.cc:1007-1086`）

```cpp
if (spec.IsStreamingGenerator()) {
    auto num_streaming_generator_returns = reply.streaming_generator_return_ids_size();
    if (num_streaming_generator_returns > 0) {
        spec.SetNumStreamingGeneratorReturns(num_streaming_generator_returns);
        for (const auto &return_id_info : reply.streaming_generator_return_ids()) {
            if (return_id_info.is_plasma_object()) {
                it->second.reconstructable_return_ids_.insert(
                    ObjectID::FromBinary(return_id_info.object_id()));
            }
        }
    }
}

if (spec.IsStreamingGenerator()) {
    const auto generator_id = ObjectID::FromBinary(reply.return_objects(0).object_id());
    if (first_execution) {
        MarkEndOfStream(generator_id, reply.streaming_generator_return_ids_size());
    }
}
```

#### 8.4 `TaskManager::MarkEndOfStream`（`task_manager.cc:753-778`）

```cpp
void TaskManager::MarkEndOfStream(const ObjectID &generator_id, 
                                   int64_t end_of_stream_index) {
    absl::MutexLock lock(&object_ref_stream_ops_mu_);
    ObjectID last_object_id;

    auto stream_it = object_ref_streams_.find(generator_id);
    if (stream_it == object_ref_streams_.end()) {
        return;
    }

    // ★★★ 在流的末尾标记 EOF ★★★
    stream_it->second.MarkEndOfStream(end_of_stream_index, &last_object_id);

    if (!last_object_id.IsNil()) {
        reference_counter_.OwnDynamicStreamingTaskReturnRef(last_object_id, generator_id);

        // ★★★ 在最后一个位置放入哨兵错误对象 ★★★
        RayObject error(rpc::ErrorType::END_OF_STREAMING_GENERATOR);
        in_memory_store_.Put(error, last_object_id,
                             reference_counter_.HasReference(last_object_id));
    }
}
```

#### 8.5 `ObjectRefStream::MarkEndOfStream`（`task_manager.cc:212-230`）

```cpp
void ObjectRefStream::MarkEndOfStream(int64_t item_index, 
                                       ObjectID *object_id_in_last_index) {
    if (end_of_stream_index_ != -1) {
        return;  // 已经标记过了
    }
    end_of_stream_index_ = std::max(next_index_, item_index);

    auto end_of_stream_id = GetObjectRefAtIndex(end_of_stream_index_);
    *object_id_in_last_index = end_of_stream_id;
}
```

#### 8.6 消费端感知流结束

当 `TryReadNextItem` 被调用时，如果 `next_index_ == end_of_stream_index_`：

```cpp
bool ObjectRefStream::IsFinished() const {
    return end_of_stream_index_ != -1 && next_index_ >= end_of_stream_index_;
}

Status ObjectRefStream::TryReadNextItem(ObjectID *object_id_out) {
    *object_id_out = GetObjectRefAtIndex(next_index_);
    if (IsFinished()) {
        return Status::ObjectRefEndOfStream("");
    }
    // ...
}
```

Cython 层将 `ObjectRefEndOfStream` 转为 Python 的 `ObjectRefStreamEndOfStreamError`，然后在 `ObjectRefGenerator._next_sync` 中捕获并转为 `StopIteration`。

---

### 第九步：异常与容错

#### 9.1 生成器抛出异常（`_raylet.pyx:1197-1248`）

```python
cdef report_streaming_generator_exception(context, e, generator_index, interrupt_signal_event):
    create_generator_error_object(e, ..., &return_obj, ...)
    del e

    context.streaming_generator_returns[0].push_back(
        c_pair[CObjectID, c_bool](return_obj.first, is_plasma_object(return_obj.second)))

    with nogil:
        check_status(CCoreWorkerProcess.GetCoreWorker().ReportGeneratorItemReturns(
            return_obj, context.generator_id, ...))
```

#### 9.2 任务失败时的流终止（`task_manager.cc:1601-1630`）

```cpp
if (spec.IsStreamingGenerator()) {
    const auto generator_id = spec.ReturnId(0);
    MarkEndOfStream(generator_id, /*item_index=*/-1);

    auto num_streaming_generator_returns = spec.NumStreamingGeneratorReturns();
    for (size_t i = 0; i < num_streaming_generator_returns; i++) {
        const auto generator_return_id = spec.StreamingGeneratorReturnId(i);
        in_memory_store_.Put(error, generator_return_id, ...);
    }
}
```

#### 9.3 生成器被删除（`object_ref_generator.py:289-295`）

```python
def __del__(self):
    if hasattr(self.worker, 'core_worker'):
        self.worker.core_worker.async_delete_object_ref_stream(self._generator_ref)
```

`AsyncDelObjectRefStream` 会通知所有挂起的回调：

```cpp
// task_manager.cc:694-704
auto signal_it = ref_stream_execution_signal_callbacks_.find(generator_id);
if (signal_it != ref_stream_execution_signal_callbacks_.end()) {
    for (const auto &execution_signal : signal_it->second) {
        execution_signal(Status::NotFound("Stream is deleted."), -1);
    }
    ref_stream_execution_signal_callbacks_.erase(signal_it);
}
```

---

### 第十步：反压协议完整时序图

以 `generator_backpressure_num_objects = 3` 为例：

```
时间 →
执行端                                    调用端
  │                                         │
  │ yield 0 → ReportItemReturns(idx=0) ──►  │ InsertToStream(id_0, 0)
  │   waiter.IncrementObjectGenerated()     │ 0 - 0 = 0 < 3 → 立即回复
  │   WaitUntilObjectConsumed()             │ ◄── reply(total_consumed=0)
  │   0-0=0 < 3 → 不阻塞，继续              │   waiter.HandleObjectReported(0)
  │                                         │
  │ yield 1 → ReportItemReturns(idx=1) ──►  │ InsertToStream(id_1, 1)
  │   waiter.IncrementObjectGenerated()     │ 1-0=1 < 3 → 立即回复
  │   WaitUntilObjectConsumed()             │ ◄── reply(total_consumed=0)
  │   1-0=1 < 3 → 不阻塞                    │   waiter.HandleObjectReported(0)
  │                                         │
  │ yield 2 → ReportItemReturns(idx=2) ──►  │ InsertToStream(id_2, 2)
  │   waiter.IncrementObjectGenerated()     │ 2-0=2 < 3 → 立即回复
  │   WaitUntilObjectConsumed()             │ ◄── reply(total_consumed=0)
  │   2-0=2 < 3 → 不阻塞                    │   waiter.HandleObjectReported(0)
  │                                         │
  │ yield 3 → ReportItemReturns(idx=3) ──►  │ InsertToStream(id_3, 3)
  │   waiter.IncrementObjectGenerated()     │ ★ 3-0=3 >= 3 → 反压!
  │   WaitUntilObjectConsumed()             │ → 挂起 callback，不回复!
  │   ★ 3-0=3 >= 3 → 阻塞! ★               │
  │   (等待条件变量...)                       │
  │                                         │ next(gen) → TryReadNextItem()
  │                                         │ → next_index_: 0→1, consumed: 0→1
  │                                         │ 3-1=2 < 3 → 触发挂起的 callback!
  │                                         │ ◄── reply(total_consumed=1)
  │   waiter.HandleObjectReported(1)        │
  │   3-1=2 < 3 → SignalAll()              │
  │   WaitUntilObjectConsumed 解除阻塞       │
  │                                         │
  │ yield 4 → ReportItemReturns(idx=4) ──►  │ InsertToStream(id_4, 4)
  │   ...                                   │ 4-1=3 >= 3 → 反压!
  │   阻塞...                                │
  │                                         │ next(gen) → consumed: 1→2
  │                                         │ 4-2=2 < 3 → 释放
  │   解除阻塞...                            │
```

---

## 七、ObjectRefStream 的阻塞/通知机制详解

### 问题一：ObjectRefStream 会去阻塞 get 吗？

**不会。** `ObjectRefStream` 本身不负责阻塞，它只是一个有索引的 ObjectID 队列，读写操作都是非阻塞的。真正的阻塞发生在更底层的 `CoreWorkerMemoryStore` 中。

**完整调用链：**

```
next(gen)                                              # 用户代码
  └→ ObjectRefGenerator._next_sync()                   # object_ref_generator.py:188
       │
       ├→ core_worker.peek_object_ref_stream()          # 预览下一个 ObjectRef
       │    └→ TaskManager::PeekObjectRefStream()       # task_manager.cc:731
       │         └→ ObjectRefStream::PeekNextItem()     # task_manager.cc:162
       │              │                                  # ★ 非阻塞! 只检查是否已写入
       │              ├→ 已写入: return (object_id, true)
       │              └→ 未写入: return (object_id, false)
       │
       ├→ ray.wait([expected_ref], timeout, fetch_local=False)  # ★ 阻塞在这里!
       │    └→ CoreWorker::Wait()                       # core_worker.cc:1678
       │         └→ CoreWorkerMemoryStore::Wait()       # memory_store.cc:412
       │              └→ GetImpl()                      # memory_store.cc:259
       │                   │
       │                   ├→ 先检查 objects_ map，对象已有则立即返回
       │                   └→ 对象没有 → 创建 GetRequest
       │                        └→ get_request->Wait(iteration_timeout)
       │                             │                    # ★ 条件变量阻塞!
       │                             └→ cv_.wait_for(lock, timeout, []{return is_ready_;})
       │
       └→ core_worker.try_read_next_object_ref_stream() # 读取并消费
            └→ TaskManager::TryReadObjectRefStream()    # task_manager.cc:637
                 └→ ObjectRefStream::TryReadNextItem()  # task_manager.cc:129
                      │                                  # ★ 非阻塞! 只是移动游标
                      ├→ 对象已写入: next_index_++, consumed++ → Status::OK()
                      ├→ 流结束:  return Status::ObjectRefEndOfStream
                      └→ 未写入:   return ObjectID::Nil()
```

**关键区别：**

| 层 | 操作 | 是否阻塞 | 机制 |
|----|------|----------|------|
| `ObjectRefStream` | `PeekNextItem` / `TryReadNextItem` | 不阻塞 | 纯数据结构，读写游标 |
| `CoreWorkerMemoryStore` | `Get` / `Wait` / `GetAsync` | 阻塞 | `GetRequest` + 条件变量 `cv_` |
| `GeneratorBackpressureWaiter` | `WaitUntilObjectConsumed` | 阻塞 | 条件变量 `backpressure_cond_var_` |

`ObjectRefStream` 只负责记录哪些 ObjectID 已产生、哪些已被消费，它是一个有序的索引表。真正的"等待数据到达"逻辑在 `MemoryStore` 层。

### 问题二：新数据生成后如何通知调用方？

通知分为两个独立的信号通路：

#### 信号通路 1：通知"数据值已到达"（MemoryStore 层）

```
执行端                                                 调用端
  │                                                      │
  │ yield output                                         │ ray.wait([expected_ref]) 阻塞中...
  │   → ReportGeneratorItemReturns gRPC ──────────────►  │
  │                                                      │ HandleReportGeneratorItemReturns()
  │                                                      │   → InsertToStream(object_id, index)
  │                                                      │   → HandleTaskReturn(object_id, ...)
  │                                                      │       → in_memory_store_.Put(object, object_id, ...)
  │                                                      │         │
  │                                                      │         ▼
  │                                                      │   CoreWorkerMemoryStore::Put()
  │                                                      │   (memory_store.cc:172-243)
```

`CoreWorkerMemoryStore::Put` 的关键逻辑：

```cpp
void CoreWorkerMemoryStore::Put(const RayObject &object,
                                 const ObjectID &object_id,
                                 const bool has_reference) {
    std::vector<std::function<void(std::shared_ptr<RayObject>)>> async_callbacks;

    {
        absl::MutexLock lock(&mu_);

        auto iter = objects_.find(object_id);
        if (iter != objects_.end()) return;

        // ★★★ 检查是否有异步 Get 回调在等待 ★★★
        auto async_callback_it = object_async_get_requests_.find(object_id);
        if (async_callback_it != object_async_get_requests_.end()) {
            async_callbacks = std::move(async_callback_it->second);
            object_async_get_requests_.erase(async_callback_it);
        }

        // ★★★ 检查是否有同步 GetRequest 在等待 ★★★
        auto object_request_iter = object_get_requests_.find(object_id);
        if (object_request_iter != object_get_requests_.end()) {
            auto &get_requests = object_request_iter->second;
            for (auto &get_request : get_requests) {
                // → GetRequest::Set()
                //   → is_ready_ = true
                //   → cv_.notify_all()   ← ★ 唤醒阻塞的 ray.wait / ray.get!
                get_request->Set(object_id, object_entry);
            }
        }

        EmplaceObjectAndUpdateStats(object_id, object_entry);
    }

    if (!async_callbacks.empty()) {
        io_context_.post([async_callbacks, object_entry]() {
            for (const auto &cb : async_callbacks) { cb(object_entry); }
        }, ...);
    }
}
```

#### 信号通路 2：通知"ObjectRef 可消费"（ObjectRefStream 层 + 反压释放）

```
调用端
  │ try_read_next_object_ref_stream(generator_ref)
  │  → TryReadObjectRefStream()
  │    → TryReadNextItem()
  │      → next_index_++, total_num_object_consumed_++     # 消费了一个
  │    → 检查: total_unconsumed = generated - consumed
  │    → 如果 total_unconsumed < backpressure_threshold:
  │       → 遍历 ref_stream_execution_signal_callbacks_[generator_id]
  │       → 每个回调: execution_signal_callback(Status::OK(), total_consumed)
  │         │
  │         └→ 这个回调就是延迟的 gRPC send_reply_callback!
  │            → reply->set_total_num_object_consumed(total_consumed)
  │            → send_reply_callback(status, ...)
  │
  │                                                        执行端
  │                                                 ◄──── gRPC reply 到达
  │                                                   waiter->HandleObjectReported(consumed)
  │                                                     → total_consumed_ = max(consumed, ...)
  │                                                     → 如果 unconsumed < threshold:
  │                                                        backpressure_cond_var_.SignalAll()
  │                                                   WaitUntilObjectConsumed() 解除阻塞!
  │                                                   generator 继续执行下一个 yield
```

#### 两个通路的关系对比

| | 通路 1: 数据到达通知 | 通路 2: 反压释放通知 |
|--|----------------------|----------------------|
| **解决什么问题** | 调用端如何知道对象值已 ready | 执行端如何知道可以继续产出 |
| **阻塞方** | 调用端 (`ray.wait` / `ray.get`) | 执行端 (`WaitUntilObjectConsumed`) |
| **通知机制** | `GetRequest::Set()` → `cv_.notify_all()` | `execution_signal_callback()` → gRPC reply → `backpressure_cond_var_.SignalAll()` |
| **触发时机** | `MemoryStore::Put()` 被调用时 | `TryReadObjectRefStream()` 消费后 |
| **传输通道** | 进程内（条件变量） | 跨进程（gRPC reply） |
| **数据流方向** | 执行端 → 调用端（对象值） | 调用端 → 执行端（消费计数） |

### `ray.get` 在 streaming generator 中的角色

`ray.get()` 在 `_next_sync` 中不会阻塞等待流中的每个 yield 值。它只在流结束时被调用：

```python
try:
    ref = core_worker.try_read_next_object_ref_stream(self._generator_ref)
except ObjectRefStreamEndOfStreamError:
    try:
        ray.get(self._generator_ref)  # ← 只检查 generator task 是否异常
    except Exception:
        self._generator_task_raised = True
        return self._generator_ref
    else:
        raise StopIteration
```

`ray.get(self._generator_ref)` 获取的是任务主返回值（Index 1），不是每个 yield 的值。它只在流结束时被调用来检查 generator task 本身是否失败。真正的每个 yield 值的等待是通过 `ray.wait([expected_ref])` 完成的。

### GetAsync 通知路径（用于 `_next_async`）

异步路径 (`_next_async`) 不使用 `ray.wait`，而是使用 `await ref`，这走的是 `GetAsync` 路径：

```cpp
void CoreWorkerMemoryStore::GetAsync(const ObjectID &object_id, callback) {
    absl::MutexLock lock(&mu_);
    auto iter = objects_.find(object_id);
    if (iter != objects_.end()) {
        io_context_.post([callback, obj = iter->second]() { callback(obj); });
    } else {
        object_async_get_requests_[object_id].push_back(callback);
    }
}
```

当 `Put()` 被调用时，会取出并执行这些 async 回调。

---

## 八、总结

| 特性 | 说明 |
|------|------|
| 标记值 | `kStreamingGeneratorReturn = -2` |
| 返回给调用端 | 1 个 `generator_ref`，包装为 `ObjectRefGenerator` |
| 值传递方式 | 每次 yield → gRPC `ReportGeneratorItemReturns` → 立即可用 |
| 反压机制 | 有，通过 `generator_backpressure_num_objects` 控制 |
| 反压默认 | `-1`（禁用），需显式设置 |
| 反压实现 | 双层：调用端延迟 gRPC 回复 + 执行端条件变量阻塞 |
| 流结束信号 | `END_OF_STREAMING_GENERATOR` 哨兵对象 → `StopIteration` |
| ObjectID生成 | 确定性，`ObjectID::FromIndex(task_id, 2 + generator_index)` |
| 容错 | gRPC失败→解除反压；生成器删除→通知执行端；任务重试→`attempt_number`过滤 |

### 关键数据结构

| 数据结构 | 位置 | 作用 |
|----------|------|------|
| `ObjectRefStream` | 调用端 TaskManager | 有序的 ObjectID 流，跟踪读写位置和 EOF |
| `GeneratorBackpressureWaiter` | 执行端 | 条件变量阻塞/唤醒，跟踪生成/消费计数 |
| `ref_stream_execution_signal_callbacks_` | 调用端 TaskManager | 挂起的 gRPC reply 回调，消费后触发 |
| `StreamingGeneratorExecutionContext` | 执行端 Python | 持有 generator、waiter、gRPC 地址 |
| `ObjectRefGenerator` | 调用端 Python | 用户可迭代的生成器接口 |

**反压双层机制：**

1. 调用端 TaskManager：通过延迟发送 gRPC reply 实现反压（不回复 = 执行端不知道消费数 = 无法继续）
2. 执行端 GeneratorBackpressureWaiter：通过条件变量在 `WaitUntilObjectConsumed()` 中阻塞

两层协同工作：调用端决定何时释放反压（消费数低于阈值时），执行端决定如何响应（阻塞当前线程直到收到释放信号）。
