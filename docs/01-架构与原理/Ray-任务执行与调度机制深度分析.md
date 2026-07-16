# Ray 任务执行与调度机制详解

本文档详细分析 Ray 中任务执行五层架构和任务队列调度机制。

---

## 目录

1. [任务执行五层架构](#1-任务执行五层架构)
2. [任务队列调度机制](#2-任务队列调度机制)

---

## 1. 任务执行五层架构

### 1.1 层次关系总览

```
层①: options_.task_execution_callback  ← Python/Java/C++ 前端回调（运行用户代码）
     │
     │  被 CoreWorker::ExecuteTask 调用
     ▼
层②: CoreWorker::ExecuteTask           ← 参数准备+引用管理+调用层①+清理
     │
     │  被 std::bind 绑定为 task_handler_
     ▼
层③: TaskReceiver::task_handler_        ← 函数包装（std::function，解耦接口）
     │
     │  在 QueueTaskForExecution 中被 execute_callback 捕获
     ▼
层④: execute_callback (lambda)          ← 调用层③ + HandleTaskExecutionResult + send_reply_callback
     │
     │  包装为 TaskToExecute
     ▼
层⑤: TaskToExecute::execute_callback_  ← 入队对象携带的闭包
     │
     │  被任务队列调度时调用
     ▼
层⑥: TaskToExecute::Execute()          ← 最外层触发点（被队列调度器调用）
```

### 1.2 每层的具体组装代码

#### 层① → 层②：语言前端注册到 CoreWorkerOptions

`TaskExecutionCallback` 类型定义（`core_worker_options.h:43-66`）：

```cpp
using TaskExecutionCallback = std::function<Status(
    const rpc::Address &caller_address,
    TaskType task_type,
    const std::string task_name,
    const RayFunction &ray_function,
    const std::unordered_map<std::string, double> &required_resources,
    const std::vector<std::shared_ptr<RayObject>> &args,
    const std::vector<rpc::ObjectReference> &arg_refs,
    const std::string &debugger_breakpoint,
    const std::string &serialized_retry_exception_allowlist,
    std::vector<std::pair<ObjectID, std::shared_ptr<RayObject>>> *returns,
    std::vector<std::pair<ObjectID, std::shared_ptr<RayObject>>> *dynamic_returns,
    std::vector<std::pair<ObjectID, bool>> *streaming_generator_returns,
    std::shared_ptr<LocalMemoryBuffer> &creation_task_exception_pb_bytes,
    bool *is_retryable_error,
    std::string *actor_repr_name,
    std::string *application_error,
    const std::vector<ConcurrencyGroup> &defined_concurrency_groups,
    const std::string name_of_concurrency_group_to_execute,
    bool is_reattempt,
    bool is_streaming_generator,
    bool retry_exception,
    int64_t generator_backpressure_num_objects,
    const std::optional<std::string> &tensor_transport)>;
```

**`task_execution_callback` 是语言无关的 C++ 函数接口**，每种语言前端各自实现并注入：

| 语言 | 设置位置 | 实现方式 |
|---|---|---|
| Python | `_raylet.pyx:2819` | Cython C 函数 `task_execution_handler`，获取 GIL 后调用 Python 的 `execute_task_with_cancellation_handler` |
| Java | `io_ray_runtime_RayNativeRuntime.cc:123` | C++ lambda，通过 JNI 获取 `JNIEnv*`，调用 Java 的 `java_task_executor.execute()` |
| C++ | 用户自行实现 | 直接设置 `options.task_execution_callback` 为自己的函数 |

Python 侧的 `task_execution_handler`（`_raylet.pyx:2274-2317`）：

```python
cdef CRayStatus task_execution_handler(
        const CAddress &caller_address,
        CTaskType task_type,
        const c_string task_name,
        const CRayFunction &ray_function,
        const unordered_map[c_string, double] &c_resources,
        const c_vector[shared_ptr[CRayObject]] &c_args,
        const c_vector[CObjectReference] &c_arg_refs,
        ...,
        c_bool is_streaming_generator,
        int64_t generator_backpressure_num_objects,
        optional[c_string] c_tensor_transport) nogil:
    with gil, disable_client_hook():
        try:
            execute_task_with_cancellation_handler(...)
        except SystemExit as e:
            return CRayStatus.IntentionalSystemExit()
    return CRayStatus.OK()
```

Java 侧忽略 Python-only 参数（如 `tensor_transport`、`defined_concurrency_groups`）使用 `RAY_UNUSED`。

#### 层② → 层③：CoreWorker 构造时将 ExecuteTask 绑定为 task_handler_

**文件**: `core_worker.cc:376-395`

```cpp
auto execute_task = std::bind(&CoreWorker::ExecuteTask,
                              this,                          // 绑定 this 指针
                              std::placeholders::_1,        // task_spec
                              std::placeholders::_2,         // resource_ids
                              std::placeholders::_3,         // return_objects
                              std::placeholders::_4,         // dynamic_return_objects
                              std::placeholders::_5,         // streaming_generator_returns
                              std::placeholders::_6,         // borrowed_refs
                              std::placeholders::_7,         // is_retryable_error
                              std::placeholders::_8,         // actor_repr_name
                              std::placeholders::_9);        // application_error

actor_task_execution_arg_waiter_ = std::make_unique<ActorTaskExecutionArgWaiter>(
    [this](const std::vector<rpc::ObjectReference> &args, int64_t tag) {
      RAY_CHECK_OK(raylet_ipc_client_->WaitForActorCallArgs(args, tag))
          << "WaitForActorCallArgs IPC failed unexpectedly";
    });

task_receiver_ = std::make_unique<TaskReceiver>(
    task_execution_service_,
    *task_event_buffer_,
    execute_task,                    // ← 作为 task_handler_ 存入 TaskReceiver
    *actor_task_execution_arg_waiter_,
    options_.initialize_thread_callback);
```

`TaskHandler` 类型定义（`task_receiver.h:32-42`）：

```cpp
using TaskHandler = std::function<Status(
    const TaskSpecification &task_spec,
    std::optional<ResourceMappingType> resource_ids,
    std::vector<std::pair<ObjectID, std::shared_ptr<RayObject>>> *return_objects,
    std::vector<std::pair<ObjectID, std::shared_ptr<RayObject>>> *dynamic_return_objects,
    std::vector<std::pair<ObjectID, bool>> *streaming_generator_returns,
    RepeatedObjectRefCount *borrower_refs,
    bool *is_retryable_error,
    std::string *actor_repr_name,
    std::string *application_error)>;
```

**关键**：`std::bind` 将成员函数 `ExecuteTask` 的 `this` 指针固定，剩余参数以占位符保留。`task_handler_` 类型是 `std::function<Status(...)>`——**不再依赖 CoreWorker 类型**，实现依赖倒置。

#### 层③ → 层④：QueueTaskForExecution 将 task_handler_ 包装为 execute_callback

**文件**: `task_receiver.cc:131-146`

```cpp
auto execute_callback =
    [this, reply, send_reply_callback, resource_ids = std::move(resource_ids)](
        const TaskSpecification &t) mutable {
      TaskExecutionResult result;
      auto status = task_handler_(t,
                                  std::move(resource_ids),
                                  &result.return_objects,
                                  &result.dynamic_return_objects,
                                  &result.streaming_generator_returns,
                                  reply->mutable_borrowed_refs(),
                                  &result.is_retryable_error,
                                  &result.actor_repr_name,
                                  &result.application_error);

      HandleTaskExecutionResult(status, t, result, send_reply_callback, reply);
    };
```

关键变换：
- `task_handler_` 签名是 9 个参数 → `execute_callback` 签名是 `(const TaskSpecification&)` 一个参数
- lambda 捕获了 `reply`、`send_reply_callback`、`resource_ids`，将它们从外部"注入"
- lambda 额外执行 `HandleTaskExecutionResult`——将"执行"和"回复"绑定为原子操作

#### 层④ → 层⑤：包装为 TaskToExecute 入队

**文件**: `task_receiver.cc:164-189`

```cpp
if (task_spec.IsActorCreationTask()) {
    SetupActor(task_spec.IsAsyncioActor(),
               task_spec.MaxActorConcurrency(),
               task_spec.AllowOutOfOrderExecution());
    normal_task_execution_queue_->EnqueueTask(
        TaskToExecute(execute_callback, cancel_callback, std::move(task_spec)));
} else if (task_spec.IsActorTask()) {
    auto it = actor_task_execution_queues_.find(task_spec.CallerWorkerId());
    if (it == actor_task_execution_queues_.end()) {
        it = actor_task_execution_queues_
                 .emplace(task_spec.CallerWorkerId(),
                     allow_out_of_order_execution_
                         ? std::make_unique<UnorderedActorTaskExecutionQueue>(...)
                         : std::make_unique<OrderedActorTaskExecutionQueue>(...))
                 .first;
    }
    it->second->EnqueueTask(
        request.sequence_number(),
        request.client_processed_up_to(),
        TaskToExecute(execute_callback, cancel_callback, std::move(task_spec)));
} else {
    normal_task_execution_queue_->EnqueueTask(
        TaskToExecute(execute_callback, cancel_callback, std::move(task_spec)));
}
```

#### 层⑤ TaskToExecute 类定义

**文件**: `common.h:29-54` + `common.cc:26-35`

```cpp
class TaskToExecute {
 public:
  TaskToExecute(
      std::function<void(const TaskSpecification &)> execute_callback,
      std::function<void(const TaskSpecification &, const Status &)> cancel_callback,
      TaskSpecification task_spec);

  void Execute();
  void Cancel(const Status &status);
  bool DependenciesResolved() const;
  void MarkDependenciesResolved();
  const std::vector<rpc::ObjectReference> &PendingDependencies() const;
  const TaskSpecification &TaskSpec() const;

 private:
  std::function<void(const TaskSpecification &)> execute_callback_;
  std::function<void(const TaskSpecification &, const Status &)> cancel_callback_;
  TaskSpecification task_spec_;
  std::vector<rpc::ObjectReference> pending_dependencies_;  // = task_spec_.GetDependencies()
};

TaskToExecute::TaskToExecute(...)
    : execute_callback_(std::move(execute_callback)),
      cancel_callback_(std::move(cancel_callback)),
      task_spec_(std::move(task_spec)),
      pending_dependencies_(task_spec_.GetDependencies()) {}

void TaskToExecute::Execute() { execute_callback_(task_spec_); }
void TaskToExecute::Cancel(const Status &status) { cancel_callback_(task_spec_, status); }
```

**TaskExecutionResult**（`common.h:56-77`）：

```cpp
struct TaskExecutionResult {
  std::vector<std::pair<ObjectID, shared_ptr<RayObject>>> return_objects;
  std::vector<std::pair<ObjectID, shared_ptr<RayObject>>> dynamic_return_objects;
  std::vector<std::pair<ObjectID, bool>> streaming_generator_returns;
  bool is_retryable_error = false;
  std::string actor_repr_name;
  std::string application_error;
};
```

### 1.3 每层的"增值"——为什么不能合并

| 层 | 输入 | 输出 | 增值（如果去掉会丢失什么） |
|---|---|---|---|
| ⑥ TaskToExecute::Execute() | TaskSpec | 无 | **调度控制**——决定何时调用 vs 取消，检查依赖就绪状态 |
| ⑤ execute_callback lambda | TaskSpec + 捕获的 reply/send_reply_callback | 无 | **执行与回复绑定**——将"运行任务"和"发 gRPC reply"绑成原子操作 |
| ④ CoreWorker::ExecuteTask | TaskSpec + ResourceIds | Status + return_objects + borrowed_refs | **C++ 基础设施**——参数获取/Pin、引用计数管理、worker 上下文、Actor 注册、错误处理 |
| ③ task_execution_handler | C++ args/RayObject | C++ returns/RayObject | **语言边界跨越**——GIL/JNI 管理、序列化/反序列化、异常转换 |
| ② 用户代码 | 语言原生对象 | 语言原生返回值 | **业务逻辑**——用户函数执行 |

**核心思想**：每一层只做一件事，通过**函数参数（输入/输出）** 和 **lambda 捕获（闭包注入）** 将上下文逐层传递和丰富。层与层之间通过 `std::function` 接口解耦——上层不需要知道下层的具体类型，只需要符合签名即可调用。

### 1.4 数据在各层间的流转

```
层⑥ TaskToExecute::Execute()
  传入: task_spec_ (构造时由 QueueTaskForExecution 传入)
  调用: execute_callback_(task_spec_)
     │
     ▼
层⑤ execute_callback lambda
  传入: task_spec (来自层⑥)
  注入: reply, send_reply_callback, resource_ids (lambda 捕获)
  调用: task_handler_(t, resource_ids, &result.return_objects, ..., reply->mutable_borrowed_refs())
     │                                                    ↑ 输出参数              ↑ 输出参数
     ▼
层④ CoreWorker::ExecuteTask
  传入: task_spec, resource_ids (来自层⑤)
  输出: return_objects, dynamic_return_objects, borrowed_refs, status (通过输出参数)
  内部: GetAndPinArgsForExecutor → args
        options_.task_execution_callback(args, ..., return_objects, ...)
     │                                    ↑输入              ↑输出
     ▼
层③ task_execution_handler (Python/Java/C++)
  传入: args, arg_refs, ray_function, ...
  输出: returns (填充 return_objects)
  内部: 反序列化 → 调用用户函数 → 序列化返回值
     │
     ▼
层② 用户代码
  输入: 反序列化后的 Python/Java 对象
  输出: 函数返回值 → 序列化为 RayObject
```

### 1.5 CoreWorker::ExecuteTask 关键流程

**文件**: `core_worker.cc:2757-2932`

```cpp
Status CoreWorker::ExecuteTask(
    const TaskSpecification &task_spec,
    std::optional<ResourceMappingType> resource_ids,
    std::vector<std::pair<ObjectID, std::shared_ptr<RayObject>>> *return_objects,
    std::vector<std::pair<ObjectID, std::shared_ptr<RayObject>>> *dynamic_return_objects,
    std::vector<std::pair<ObjectID, bool>> *streaming_generator_returns,
    ReferenceCounterInterface::ReferenceTableProto *borrowed_refs,
    bool *is_retryable_error,
    std::string *actor_repr_name,
    std::string *application_error) {

  // 1. 获取参数
  GetAndPinArgsForExecutor(task_spec, &args, &arg_refs, &borrowed_ids);

  // 2. 设置 worker 上下文
  worker_context_->SetCurrent(task_spec);

  // 3. Actor 创建任务特殊处理
  if (task_spec.IsActorCreationTask()) { ... }

  // 4. 调用语言前端回调（层②）
  Status status = options_.task_execution_callback(
      task_spec.CallerAddress(),
      task_type,
      task_spec.GetName(),
      func,
      task_spec.GetRequiredResources().GetResourceUnorderedMap(),
      args,
      arg_refs,
      task_spec.GetDebuggerBreakpoint(),
      task_spec.GetSerializedRetryExceptionAllowlist(),
      return_objects,
      dynamic_return_objects,
      streaming_generator_returns,
      creation_task_exception_pb_bytes,
      is_retryable_error,
      actor_repr_name,
      application_error,
      defined_concurrency_groups,
      name_of_concurrency_group_to_execute,
      /*is_reattempt=*/task_spec.AttemptNumber() > 0,
      /*is_streaming_generator=*/task_spec.IsStreamingGenerator(),
      /*retry_exception=*/task_spec.ShouldRetryExceptions(),
      /*generator_backpressure_num_objects=*/
      task_spec.GeneratorBackpressureNumObjects(),
      /*tensor_transport=*/task_spec.TensorTransport());

  // 5. 清理借用引用
  if (!borrowed_ids.empty()) {
    reference_counter_->PopAndClearLocalBorrowers(borrowed_ids, borrowed_refs, &deleted);
  }

  return status;
}
```

---

## 2. 任务队列调度机制

### 2.1 HandlePushTask 分派入队

**文件**: `core_worker.cc:3340-3399`

```cpp
void CoreWorker::HandlePushTask(rpc::PushTaskRequest request,
                                rpc::PushTaskReply *reply,
                                rpc::SendReplyCallback send_reply_callback) {
  // ...
  if (request.task_spec().type() == TaskType::ACTOR_TASK) {
    // Actor 任务：整体 post 到 task_execution_service_ 事件循环
    task_execution_service_.post(
        [this, request = std::move(request), reply,
         send_reply_callback = std::move(send_reply_callback), func_name]() mutable {
          if (IsExiting()) { return; }
          task_receiver_->QueueTaskForExecution(
              std::move(request), reply, send_reply_callback);
        },
        "CoreWorker.HandlePushTaskActor");
  } else {
    // 普通任务：同步入队 + post 执行触发
    task_receiver_->QueueTaskForExecution(
        std::move(request), reply, send_reply_callback);
    task_execution_service_.post(
        [this, func_name] {
          if (IsExiting()) { return; }
          task_receiver_->ExecuteQueuedNormalTasks();
        },
        "CoreWorker.HandlePushTask");
  }
}
```

### 2.2 为什么 Actor 任务整体 post 到 io_thread

**根本原因**：Actor 任务有**顺序性和并发组**约束，其入队逻辑必须与执行调度在**同一线程**上运行。

**普通任务**没有顺序约束，入队操作极其简单——就是 `push_back` 到一个 deque，用 MutexLock 保护：

```cpp
// normal_task_execution_queue.cc
void NormalTaskExecutionQueue::EnqueueTask(TaskToExecute task) {
    absl::MutexLock lock(&mu_);
    pending_normal_tasks_.push_back(std::move(task));
}
```

**Actor 任务**的入队不是简单 push，而是要参与**复杂的顺序调度**：

1. 检查 `client_processed_up_to` 跳过过期请求
2. 按 `seq_no` 插入 btree_map（有序）
3. 启动 AsyncWait 等参数就绪
4. 回调中执行 ExecuteQueuedTasks()——按序出队
5. 还要处理超时定时器、retry 任务等

这些操作访问 `group_states_`、`pending_task_id_to_is_canceled` 等共享状态，如果入队在 gRPC 线程而调度在 io_thread，会出现竞态条件和顺序破坏。代码中有硬断言：

```cpp
// ordered_actor_task_execution_queue.cc:59
RAY_CHECK(std::this_thread::get_id() == main_thread_id_);
```

**所有对 Actor 队列的操作都必须在 io_thread 上**。

### 2.3 三条执行路径

#### 路径 A：普通任务（同步逐个执行）

**文件**: `normal_task_execution_queue.cc:73-79`

```cpp
void NormalTaskExecutionQueue::ExecuteQueuedTasks() {
  while (auto task = TryPopQueuedTask()) {
      task->Execute();   // 同步阻塞，执行完一个才 pop 下一个
  }
}
```

#### 路径 B：有序 Actor 任务（按 seq_no 顺序+依赖等待）

**文件**: `ordered_actor_task_execution_queue.cc`

**EnqueueTask**（55-133）：

```cpp
void OrderedActorTaskExecutionQueue::EnqueueTask(int64_t seq_no,
                                                 int64_t client_processed_up_to,
                                                 TaskToExecute task) {
  RAY_CHECK(seq_no != -1);
  RAY_CHECK(std::this_thread::get_id() == main_thread_id_);

  TaskSpecification task_spec = task.TaskSpec();
  const std::string &group = task_spec.ConcurrencyGroupName();
  auto [iter, _] = group_states_.try_emplace(
      group, ConcurrencyGroupOrderingState(task_execution_service_));
  auto &group_state = iter->second;

  if (client_processed_up_to >= group_state.next_seq_no) {
    group_state.next_seq_no = client_processed_up_to + 1;
  }

  if (is_retry) {
    retry_task = &group_state.pending_retry_tasks.emplace_back(std::move(task));
  } else {
    RAY_CHECK(group_state.pending_tasks.emplace(seq_no, std::move(task)).second);
  }

  if (!dependencies.empty()) {
    waiter_.AsyncWait(dependencies, [..., ExecuteQueuedTasks()]);
  }
  ExecuteQueuedTasks();
}
```

**ExecuteQueuedTasks**（146-252）：

```cpp
void OrderedActorTaskExecutionQueue::ExecuteQueuedTasks() {
  for (auto &[group_name, group_state] : group_states_) {
    // 1. 取消过期请求 (seq_no < next_seq_no)
    while (!group_state.pending_tasks.empty() &&
           group_state.pending_tasks.begin()->first < group_state.next_seq_no) {
      // Cancel and erase
    }

    // 2. 处理 retry 请求（无需排序）
    while (retry_iter != group_state.pending_retry_tasks.end()) {
      if (!request.DependenciesResolved()) { retry_iter++; continue; }
      ExecuteRequest(std::move(request));
      group_state.pending_retry_tasks.erase(retry_iter++);
    }

    // 3. 处理有序请求 (seq_no == next_seq_no)
    while (!group_state.pending_tasks.empty()) {
      auto &[seq_no, request] = *begin_it;
      if (seq_no == group_state.next_seq_no) {
        if (request.DependenciesResolved()) {
          ExecuteRequest(std::move(request));
          group_state.next_seq_no++;
        } else { break; }
      } else if (group_state.seq_no_to_skip.erase(group_state.next_seq_no) > 0) {
        group_state.next_seq_no++;
      } else { break; }
    }

    // 4. 如果头任务在等前序，启动重排序超时定时器
    if (head_of_line_waiting) {
      group_state.wait_timer_.async_wait([cancel all tasks on timeout]);
    }
  }
}
```

**ExecuteRequest**（254-265）：

```cpp
void OrderedActorTaskExecutionQueue::ExecuteRequest(TaskToExecute &&request) {
  auto pool = pool_manager_->GetExecutor(request.ConcurrencyGroupName(),
                                         request.FunctionDescriptor());
  if (pool == nullptr) {
    AcceptRequestOrRejectIfCanceled(task_id, request);
  } else {
    pool->Post([this, request = std::move(request), task_id]() mutable {
      AcceptRequestOrRejectIfCanceled(task_id, request);
    });
  }
}
```

**AcceptRequestOrRejectIfCanceled**（267-284）：

```cpp
void OrderedActorTaskExecutionQueue::AcceptRequestOrRejectIfCanceled(
    TaskID task_id, TaskToExecute &request) {
  bool is_canceled = false;
  {
    absl::MutexLock lock(&mu_);
    auto it = pending_task_id_to_is_canceled.find(task_id);
    if (it != pending_task_id_to_is_canceled.end()) {
      is_canceled = it->second;
    }
  }
  if (is_canceled) {
    request.Cancel(Status::SchedulingCancelled(...));
  } else {
    request.Execute();
  }
  absl::MutexLock lock(&mu_);
  pending_task_id_to_is_canceled.erase(task_id);
}
```

#### 路径 C：无序 Actor 任务（asyncio / max_concurrency > 1）

**文件**: `unordered_actor_task_execution_queue.cc`

**EnqueueTask**（65-104）：

```cpp
void UnorderedActorTaskExecutionQueue::EnqueueTask(int64_t seq_no,
                                                   int64_t client_processed_up_to,
                                                   TaskToExecute task) {
  RAY_CHECK(std::this_thread::get_id() == main_thread_id_);
  TaskID task_id = task.TaskID();
  bool run_task = true;
  std::optional<TaskToExecute> task_to_cancel;
  {
    absl::MutexLock lock(&mu_);
    if (pending_task_id_to_is_canceled.contains(task_id)) {
      // 同 task_id 有先前 attempt 正在运行 → 入 queued_actor_tasks_ 等待
      run_task = false;
      auto it = queued_actor_tasks_.find(task_id);
      if (it != queued_actor_tasks_.end()) {
        // 保留更大 attempt_number 的
        if (it->second.AttemptNumber() > task.AttemptNumber()) {
          task_to_cancel = std::move(task);
        } else {
          task_to_cancel = std::move(it->second);
          queued_actor_tasks_.insert_or_assign(task_id, std::move(task));
        }
      } else {
        queued_actor_tasks_.emplace(task_id, std::move(task));
      }
    } else {
      pending_task_id_to_is_canceled.emplace(task_id, false);
      run_task = true;
    }
  }
  if (run_task) { RunRequest(std::move(task)); }
  if (task_to_cancel.has_value()) { task_to_cancel->Cancel(...); }
}
```

**RunRequest**（141-153）：

```cpp
void UnorderedActorTaskExecutionQueue::RunRequest(TaskToExecute request) {
  const TaskSpecification &task_spec = request.TaskSpec();
  if (!request.PendingDependencies().empty()) {
    // 等待依赖就绪
    waiter_.AsyncWait(dependencies, [...callback...] {
      request.MarkDependenciesResolved();
      RunRequestWithResolvedDependencies(std::move(request));
    });
  } else {
    request.MarkDependenciesResolved();
    RunRequestWithResolvedDependencies(std::move(request));
  }
}
```

**RunRequestWithResolvedDependencies**（121-139）：

```cpp
void UnorderedActorTaskExecutionQueue::RunRequestWithResolvedDependencies(
    TaskToExecute request) {
  const auto task_id = request.TaskID();
  if (is_asyncio_) {
    auto fiber = fiber_state_manager_->GetExecutor(...);
    fiber->EnqueueFiber([this, request = std::move(request), task_id]() mutable {
      AcceptRequestOrRejectIfCanceled(task_id, request);
    });
  } else {
    auto pool = pool_manager_->GetExecutor(...);
    if (pool == nullptr) {
      AcceptRequestOrRejectIfCanceled(task_id, request);
    } else {
      pool->Post([this, request = std::move(request), task_id]() mutable {
        AcceptRequestOrRejectIfCanceled(task_id, request);
      });
    }
  }
}
```

**AcceptRequestOrRejectIfCanceled**（185-215）——执行后检查排队 attempt：

```cpp
void UnorderedActorTaskExecutionQueue::AcceptRequestOrRejectIfCanceled(
    TaskID task_id, TaskToExecute &request) {
  bool is_canceled = false;
  { /* check pending_task_id_to_is_canceled */ }
  if (is_canceled) {
    request.Cancel(...);
  } else {
    request.Execute();
  }

  // 执行完毕后检查同 task_id 是否有排队的下一个 attempt
  std::optional<TaskToExecute> request_to_run;
  {
    absl::MutexLock lock(&mu_);
    auto it = queued_actor_tasks_.find(task_id);
    if (it != queued_actor_tasks_.end()) {
      request_to_run = std::move(it->second);
      queued_actor_tasks_.erase(it);
    } else {
      pending_task_id_to_is_canceled.erase(task_id);
    }
  }
  if (request_to_run.has_value()) {
    task_execution_service_.post([RunRequest(std::move(*request_to_run))]);
  }
}
```

### 2.4 QueueTaskForExecution 不回复，执行完后才回复

`QueueTaskForExecution` 只创建 `execute_callback`（捕获了 `reply` 和 `send_reply_callback`），入队后立即返回。`send_reply_callback` 在 `HandleTaskExecutionResult` 中才被调用，此时 reply 已包含所有返回值。`reply` 和 `send_reply_callback` 就像"填好再寄的回信信封"。

### 2.5 GetAndPinArgsForExecutor 详细流程

**文件**: `core_worker.cc:3247-3339`

在 `CoreWorker::ExecuteTask` 内部调用（`core_worker.cc:2801`），是**执行前的参数准备**：

**第 1 步：遍历参数，区分 ByRef 和 ByValue**

```cpp
Status CoreWorker::GetAndPinArgsForExecutor(
    const TaskSpecification &task,
    std::vector<std::shared_ptr<RayObject>> *args,
    std::vector<rpc::ObjectReference> *arg_refs,
    std::vector<ObjectID> *borrowed_ids) {
  absl::flat_hash_set<ObjectID> by_ref_ids;
  absl::flat_hash_map<ObjectID, std::vector<size_t>> by_ref_indices;

  for (size_t i = 0; i < task.NumArgs(); ++i) {
    if (task.ArgByRef(i)) {
      // ★ 大值 by-ref 路径 ★
      const auto &arg_ref = task.ArgRef(i);
      const auto arg_id = ObjectID::FromBinary(arg_ref.object_id());
      by_ref_ids.insert(arg_id);
      by_ref_indices[arg_id].push_back(i);
      arg_refs->push_back(arg_ref);
      args->emplace_back();
      // Pin: 增加引用计数，防止 GC
      reference_counter_->AddLocalReference(arg_id, task.CallSiteString());
      reference_counter_->AddBorrowedObject(arg_id, ObjectID::Nil(),
                                             task.ArgRef(i).owner_address());
      borrowed_ids->push_back(arg_id);
      // 写入占位符
      memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
                         task.ArgObjectId(i),
                         reference_counter_->HasReference(task.ArgObjectId(i)));
    } else {
      // ★ 小值内联路径 ★
      auto data = std::make_shared<LocalMemoryBuffer>(
          task.ArgData(i), task.ArgDataSize(i));
      auto metadata = std::make_shared<LocalMemoryBuffer>(
          task.ArgMetadata(i), task.ArgMetadataSize(i));
      args->push_back(std::make_shared<RayObject>(data, metadata, ...));
      // 不调用 AddBorrowedObject，不写入 in_memory_store_
      // 对嵌套引用 ArgInlinedRefs(i): AddLocalReference(inlined_id)
    }
  }
```

**第 2 步：从 Plasma Store 获取 ByRef 参数的实际值**

```cpp
  if (!by_ref_ids.empty()) {
    plasma_store_provider_->Get(by_ref_ids, owner_addresses,
                                -1/*timeout=无限等待*/, &result_map);
  }
```

- timeout = -1 表示**无限等待**，会阻塞直到所有 ByRef 参数都从 Plasma Store 取回
- 若本地 Plasma 无 → raylet 从远端拉取，Get() 阻塞等待

### 2.6 Actor Task 参数依赖等待

Actor Task 通过 `ActorTaskExecutionArgWaiter` 进行额外的依赖等待：

```cpp
// CoreWorker 初始化时 (core_worker.cc:387-391):
actor_task_execution_arg_waiter_ = std::make_unique<ActorTaskExecutionArgWaiter>(
    [this](const std::vector<rpc::ObjectReference> &args, int64_t tag) {
      RAY_CHECK_OK(raylet_ipc_client_->WaitForActorCallArgs(args, tag))
          << "WaitForActorCallArgs IPC failed unexpectedly";
    });
```

raylet 确认参数就绪后调用 `MarkReady(tag)` → 触发回调 → `MarkDependenciesResolved()` → `ExecuteQueuedTasks()`。

### 2.7 等待机制对比

| 环节 | 位置 | 等待方式 | 等待内容 |
|---|---|---|---|
| `ResolveDependencies` | **调用方** | **异步** | 本地 `in_memory_store_` 中已有的小对象 → 立即返回；不存在 → 等 `Put` 触发回调；大对象已 in Plasma → 不拉取，保持 ByRef |
| `AsyncWait` | **执行方** Actor 队列 | **异步** | 等待 raylet 确认远程参数已到达本地节点 |
| `GetAndPinArgsForExecutor` | **执行方** ExecuteTask 内 | **同步阻塞** | 从本地 Plasma Store 读取所有 ByRef 参数（`timeout=-1`），若本地无则 raylet 从远端拉取 |
| `task_execution_callback` | **执行方** ExecuteTask 内 | **同步阻塞** | 用户代码实际执行 |
