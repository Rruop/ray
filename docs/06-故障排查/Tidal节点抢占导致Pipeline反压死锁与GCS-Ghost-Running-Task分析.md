# Tidal 节点抢占导致 Pipeline 反压死锁与 GCS Ghost RUNNING Task 分析

**时间**: 2026-08-25
**作业**: kml-task-...-100660116 (job_id=05000000)
**现象**: 作业完全卡死，QGPreprocessMapper 7998800 不变，QGInferMapper 7992301 不变

---

## 一、作业配置与拓扑

### 集群拓扑

| 节点类型 | 说明 | 资源 |
|---------|------|------|
| head | Driver 运行 | - |
| worker-1 (tidal) | GPU 节点，可被 K8s 抢占 | 大量 GPU+CPU |
| worker-2 | CPU only | 5 ALIVE |

### 作业配置

```
enable_object_replication = false
enable_pin_transfer = false
limit = 8000000
default_scheduling_label = worker-1
```

### Pipeline

```
ReadArrowJSON(580113088行)
  → LimitOperator[limit=8000000]
  → StreamingRepartition
  → QGPreprocessMapper(1500 actors, 1 CPU each)
  → QGInferMapper(1000 actors, 3 CPU+1 GPU each)
  → Filter
  → Write(TaskPoolMapOperator, num_cpus=1)
```

### Tidal 节点持续抢占

- **1949+** 个 worker-1 节点死亡 (SIGTERM)
- **86182** 次 NODE_OUT_OF_MEMORY
- 15:50 后 recovery 停止

---

## 二、QGPreprocessMapper 和 QGInferMapper 卡住的原因

### 核心结论：卡住的原因不是 recovery object 问题，而是 pipeline 反压死锁

之前的分析（见"多轮分析修正记录"）已经确认：**error object 不会导致 actor task 无限重试，作业会失败而非 hang**。因此需要重新定位 hang 的根因。

### 2.1 QGInferMapper 卡在 7992301 的原因

QGInferMapper 有 1000 个 actor（3 CPU + 1 GPU），全部调度到 worker-1（tidal 节点）。tidal 节点被 K8s 持续抢占导致 actor 反复 RESTARTING → GCS 中 1406 个 FAILED (ACTOR_UNAVAILABLE)。

**关键代码路径：actor 不可用时 task 被阻塞**

当 actor 处于 RESTARTING 状态时，`SendPendingTasks` 不会发送任何 task：

```cpp
// src/ray/core_worker/task_submission/actor_task_submitter.cc:535-565
void ActorTaskSubmitter::SendPendingTasks(const ActorID &actor_id) {
  auto it = client_queues_.find(actor_id);
  RAY_CHECK(it != client_queues_.end());
  auto &client_queue = it->second;
  auto &actor_submit_queue = client_queue.actor_submit_queue_;
  if (client_queue.pending_out_of_scope_death_) {
    // Wait until the actor is dead and then decide
    return;
  }
  if (!client_queue.client_address_.has_value()) {
    // actor 地址为空（RESTARTING 状态）→ 不发送任何 task
    if (client_queue.state_ == rpc::ActorTableData::RESTARTING &&
        client_queue.fail_if_actor_unreachable_) {
      // fail_if_actor_unreachable=true 时立即失败
      while (true) {
        auto task = actor_submit_queue->PopNextTaskToSend();
        if (!task.has_value()) break;
        io_service_.post(/* HandlePushTaskReply with IOError */);
      }
    }
    // fail_if_actor_unreachable=false 时 → task 静默等待，不发也不失败
    return;
  }
  // 只有 actor 地址存在时才发送 task
  while (true) {
    auto task = actor_submit_queue->PopNextTaskToSend();
    if (!task.has_value()) break;
    PushActorTask(client_queue, task->first, task->second);
  }
}
```

**结果**：大量 task 长时间停留在 `PENDING_NODE_ASSIGNMENT` 或 `SUBMITTED_TO_WORKER`（等 actor 地址），既不执行也不失败。

**部分 actor 恢复后执行 task 但遇到 error object**：

actor 恢复后接收 task，但输入 object 可能已丢失。此时的完整路径：

1. Actor 端 `GetAndPinArgsForExecutor` → 从 plasma 拿到 error RayObject → `IsException()=true`
2. Python 层 `raise_if_dependency_failed` → raise `ObjectReconstructionFailedError`
3. 被外层 `except BaseException`(line 2054) 捕获 → `store_task_errors` → 正常返回 application error
4. Driver 端 `CompletePendingTask(is_application_error=true)` → task FAILED
5. `task_retryable = num_retries_left_!=0 && !reconstructable_return_ids_.empty()`
6. streaming generator 首次执行失败 → `reconstructable_return_ids_` 为空 → **不重试**
7. `MarkEndOfStream(generator_id, 0)` → streaming generator EOF

**此路径不会导致 hang**，但会导致部分 task 以 application error 终止。

### 2.2 QGPreprocessMapper 卡在 7998800 的原因

QGPreprocessMapper 有 1500 个 actor（1 CPU each），也全部调度到 worker-1。

**pipeline 反压死锁链**：

```
QGInferMapper 卡住
  → 不消费 QGPreprocessMapper 的输出
  → QGPreprocessMapper 输出队列（output_queue）堆积
  → Ray Data backpressure 策略触发
  → QGPreprocessMapper 被限制提交新 task（task-submission backpressure）
  → QGPreprocessMapper 停止处理新输入
  → 上游 StreamingRepartition 和 ReadArrowJSON 也被反压
  → 整个 pipeline 停滞
```

**Ray Data backpressure 机制详解**：

Ray Data 的 streaming executor 在每个调度循环中通过 backpressure policy 决定是否允许 operator 提交新 task：

```python
# python/ray/data/_internal/execution/streaming_executor_state.py:615-700
def get_eligible_operators(
    topology, backpressure_policies, *, ensure_liveness
):
    for op, state in topology.items():
        # 检查所有 backpressure policy
        triggered_policy = None
        for p in backpressure_policies:
            if not p.can_add_input(op):
                triggered_policy = p.name
                break
        in_backpressure = triggered_policy is not None

        # 如果被 backpressure，op 不可提交新 task
        op_runnable = not is_completed and can_add and has_bundles

        if in_backpressure:
            reasons.append(f"backpressure({triggered_policy})")
```

**关键 policy：`ConcurrencyCapBackpressurePolicy`**

```python
# python/ray/data/_internal/execution/backpressure_policy/concurrency_cap_backpressure_policy.py
class ConcurrencyCapBackpressurePolicy(BackpressurePolicy):
    """基于 output queue 增长率动态限制每个 operator 的并发度

    - 维护 output queue 大小的 EWMA 作为典型水位: level
    - 维护偏离水平的绝对残差的 EWMA 作为尺度代理: dev
    - 定义死区: deadband [lower, upper] = [level - K_DEV*dev, level + K_DEV*dev]
    - q > upper → target_cap = running - BACKOFF_FACTOR (回退)
    - q < lower → target_cap = running + RAMPUP_FACTOR (爬升)
    - else → target_cap = running (保持)
    """
```

**还有 `downstream_capacity_backpressure_policy`**：

```python
# 当下游 operator 的 object store 预算利用率过高时
# max_task_output_bytes_to_read 返回 0 → 阻止当前 op 产生更多输出
# process_completed_tasks 中检查:
for policy in backpressure_policies:
    policy_limit = policy.max_task_output_bytes_to_read(op)
    if policy_limit == 0:
        limiting_policy = policy.name  # 完全反压

op.notify_in_task_output_backpressure(max_bytes_to_read == 0, limiting_policy)
```

**反压传播链**：

当 QGInferMapper 的 actor 大量不可用时：
1. InferMapper 无法消费 → PreprocessMapper 的 `output_queue_bytes` 持续增长
2. backpressure policy 检测到 output queue 过大 → `can_add_input()` 返回 False
3. PreprocessMapper 被 task-submission backpressure → 不提交新 task
4. PreprocessMapper 的 `output_queue` 停止增长（不再产生新输出）
5. 上游 StreamingRepartition 的 output queue 也堆积 → 也被 backpressure
6. 整个 pipeline 从下游到上游逐级阻塞 → **完全死锁**

7998800 和 7992301 之间的差值 6499 行是 PreprocessMapper 已产生但 InferMapper 还没消费的输出。

### 2.3 为什么作业 hang 而非失败

关键问题：**为什么 error object 路径不会导致 hang，但作业仍然 hang？**

答案在于：error object 只影响少数 task（recovery 确实失败的），而 **大部分 task 卡在调度/反压层面**：

1. **大量 actor 处于 RESTARTING** → task 静默等待在 `SendPendingTasks` 中
2. **恢复的 actor 执行部分 task** → 遇到 error → task 正常 FAILED → streaming generator EOF
3. **剩余 actor 仍不可用** → output queue 堆积 → backpressure → 上游全部阻塞
4. **backpressure 不会产生异常** → 只是静默等待 → 没有超时机制 → **hang**

Ray Data 的 streaming executor 有一个 `ensure_liveness` 机制：

```python
# streaming_executor_state.py
def get_eligible_operators(topology, backpressure_policies, *, ensure_liveness):
    if ensure_liveness:
        # 当所有 operator 都被 backpressure 或无输入时
        # 仍然选择一个最优先的 operator 执行
        # 但如果被选的 operator 也没有可执行的 task，就卡住了
```

`ensure_liveness` 无法打破 **actor 不可用导致的 task 阻塞**——因为问题不在 operator 层面，而在 Ray Core 的 actor task 提交层面。

---

## 三、GCS 中 Ghost RUNNING Task 的产生原因

### 3.1 现象

GCS 中有 207 个 task 显示为 RUNNING 状态，但实际上对应的 worker 已死亡。

### 3.2 代码根因

GCS 的 `MarkTasksFailedOnWorkerDead` 只按 `worker_id` 索引查找需要标记 FAILED 的 task：

```cpp
// src/ray/gcs/gcs_task_manager.cc:107-128
void GcsTaskManager::GcsTaskManagerStorage::MarkTasksFailedOnWorkerDead(
    const WorkerID &worker_id, const rpc::WorkerTableData &worker_failure_data) {
  auto task_attempts_itr = worker_index_.find(worker_id);
  if (task_attempts_itr == worker_index_.end()) {
    // No tasks by the worker. → 找不到则不做任何处理
    return;
  }
  // 找到了才标记 FAILED
  for (const auto &task_locator : task_attempts_itr->second) {
    MarkTaskAttemptFailedIfNeeded(task_locator, ...);
  }
}
```

`worker_index_` 在 `UpdateIndex` 中建立：

```cpp
// src/ray/gcs/gcs_task_manager.cc:238-251
void GcsTaskManager::GcsTaskManagerStorage::UpdateIndex(
    const std::shared_ptr<TaskEventLocator> &loc) {
  const auto &task_events = loc->GetTaskEventsMutable();
  const auto worker_id = GetWorkerID(task_events);

  primary_index_.insert({task_attempt, loc});
  task_index_[task_id].insert(loc);
  job_index_[job_id].insert(loc);
  if (!worker_id.IsNil()) {
    worker_index_[worker_id].insert(loc);  // 只有 worker_id 非空才加入索引
  }
}
```

`GetWorkerID` 从 task event 的 `state_updates.worker_id` 获取：

```cpp
// src/ray/common/protobuf_utils.cc:307-312
WorkerID GetWorkerID(const rpc::TaskEvents &task_event) {
  if (task_event.has_state_updates() && task_event.state_updates().has_worker_id()) {
    return WorkerID::FromBinary(task_event.state_updates().worker_id());
  }
  return WorkerID::Nil();  // worker_id 未设置 → 返回 Nil
}
```

**`worker_id` 在 `MarkTaskWaitingForExecution` 中设置**（即 task 到达 `SUBMITTED_TO_WORKER` 状态时）。如果 task 在到达此状态之前 worker 就死了，`worker_id` 为空，task 不会被加入 `worker_index_`。

### 3.3 Ghost 产生的时序分析

```
1. Driver 提交 task → PENDING_ARGS_AVAIL → PENDING_NODE_ASSIGNMENT (无 worker_id)
2. Raylet 分配 worker → driver 收到回复 → MarkTaskWaitingForExecution
   → SUBMITTED_TO_WORKER (设置 worker_id + node_id，上报到 GCS)
3. Worker 端 ExecuteTask → RUNNING (上报 RUNNING 事件，但 state_update 只含 pid，
   不含 worker_id)
4. Worker 被 K8s 抢占 (SIGTERM) → worker 进程死亡
5. GCS 收到 worker dead 通知 → MarkTasksFailedOnWorkerDead
```

**情况 A**：step 2 的 `SUBMITTED_TO_WORKER` 事件已上报到 GCS → `worker_id` 非空 → `worker_index_` 中有该 task → `MarkTasksFailedOnWorkerDead` 能找到并标记 FAILED。**不产生 ghost。**

**情况 B**：worker 被抢占时，worker 端的 task_event_buffer 中有 RUNNING 事件但来不及 flush（进程被 SIGTERM kill），而 driver 端的 `SUBMITTED_TO_WORKER` 事件虽然上报了但 GCS 在合并（`MergeFrom`）后，最终 task event 显示的状态可能来自多条事件的合并。

**情况 C（最可能产生 ghost 的场景）**：

GCS 的 `UpdateExistingTaskAttempt` 使用 `MergeFrom` 合并事件：

```cpp
// src/ray/gcs/gcs_task_manager.cc:175-180
void UpdateExistingTaskAttempt(...) {
  auto &existing_task = loc->GetTaskEventsMutable();
  existing_task.MergeFrom(task_events);  // 后到的事件覆盖先到的
  UpdateIndex(loc);
}
```

当 GCS 收到来自 worker 端的 RUNNING 事件（`state_updates` 包含 `pid` 但**不含** `worker_id`）和来自 driver 端的 SUBMITTED_TO_WORKER 事件（`state_updates` 包含 `worker_id`）时，`MergeFrom` 的行为取决于 protobuf 合并规则：
- `state_updates` 中的 `worker_id` 不会被 RUNNING 事件覆盖（因为 RUNNING 事件不含 `worker_id` 字段）
- `state_updates` 中的 `state_ts_ns` 会被 RUNNING 事件的新状态条目追加

**但如果时序是**：RUNNING 事件先到达 GCS（worker 本地 flush），而 SUBMITTED_TO_WORKER 事件后到达（driver 经过 raylet 中转），则 GCS 先用 RUNNING 事件创建 task event（此时 `worker_id` 为 Nil），后续 SUBMITTED_TO_WORKER 事件通过 `MergeFrom` 合并时**应该**补上 `worker_id` 并触发 `UpdateIndex`。

**但如果 SUBMITTED_TO_WORKER 事件因为某种原因没有到达 GCS**（例如 driver 端的 task_event_buffer 也没来得及 flush，或 GCS 事件被 GC 淘汰），则 task 在 GCS 中永远是 RUNNING + `worker_id` = Nil → `MarkTasksFailedOnWorkerDead` 找不到 → **ghost RUNNING**。

### 3.4 缺少 `MarkTasksFailedOnNodeDead` 机制

`MarkTasksFailedOnWorkerDead` 只按 `worker_id` 查找。但如果 task 的 `worker_id` 为空（还没到达 `SUBMITTED_TO_WORKER`），就无法被标记 FAILED。

**需要有一个按 `node_id` 查找的机制**——当节点死亡时，标记所有该节点上的 task 为 FAILED。但目前代码中没有 `MarkTasksFailedOnNodeDead`。

虽然 GCS 有 `node_index_`（从 `task_events.state_updates.node_id` 建立），但它只用于 GC 查找，没有用于 worker dead 场景的 mark failed。

### 3.5 GCS Mark Task Failed 的延迟机制

```cpp
// src/ray/gcs/gcs_task_manager.cc:733-750
void GcsTaskManager::OnWorkerDead(
    const WorkerID &worker_id, const std::shared_ptr<rpc::WorkerTableData> &worker_data) {
  auto timer = std::make_shared<boost::asio::deadline_timer>(
      io_service_,
      boost::posix_time::milliseconds(
          RayConfig::instance().gcs_mark_task_failed_on_worker_dead_delay_ms()));
  timer->async_wait([...] {
    // 延迟后才标记
    task_event_storage_->MarkTasksFailedOnWorkerDead(worker_id, *worker_data);
  });
}
```

**延迟由 `gcs_mark_task_failed_on_worker_dead_delay_ms` 控制**。在此延迟期间，task 仍然显示为 RUNNING。但如果 `worker_index_` 中没有该 task（`worker_id` 为空），延迟结束后也不会标记 FAILED。

---

## 四、Recovery 失败后的完整代码路径

### 4.1 Recovery 失败 → Error Object 写入 Plasma

```cpp
// src/ray/core_worker/object_recovery_manager.cc:141-149
void ObjectRecoveryManager::ReconstructObject(const ObjectID &object_id) {
  LineageReconstructionEligibility eligibility =
      reference_counter_.GetLineageReconstructionEligibility(object_id);

  if (eligibility != LineageReconstructionEligibility::ELIGIBLE) {
    auto error_type_opt = ToErrorType(eligibility);
    rpc::ErrorType error_type = error_type_opt.value_or(rpc::ErrorType::OBJECT_LOST);
    recovery_failure_callback_(object_id, error_type, /*pin_object=*/true);
    return;
  }
  // ...尝试重试 producer task...
}
```

`recovery_failure_callback_` 在 `core_worker_process.cc` 中绑定：

```cpp
// src/ray/core_worker/core_worker_process.cc:653-665
auto object_recovery_manager = std::make_unique<ObjectRecoveryManager>(
    rpc_address, raylet_client_pool, std::move(object_lookup),
    *task_manager, *reference_counter, *memory_store,
    [this](const ObjectID &object_id, rpc::ErrorType reason, bool pin_object) {
      auto core_worker = GetCoreWorker();
      RAY_UNUSED(core_worker->Put(RayObject(reason),  // error RayObject
                                  /*contained_object_ids=*/{},
                                  object_id,
                                  /*pin_object=*/pin_object));
    });
```

`CoreWorker::Put` 调用 `PutInLocalPlasmaStore`：

```cpp
// src/ray/core_worker/core_worker.cc:992-1025
Status CoreWorker::PutInLocalPlasmaStore(const RayObject &object,
                                         const ObjectID &object_id,
                                         bool pin_object) {
  bool object_exists = false;
  RAY_RETURN_NOT_OK(plasma_store_provider_->Put(
      object, object_id, /*owner_address=*/rpc_address_, &object_exists));
  if (!object_exists) {
    if (pin_object) {
      local_raylet_rpc_client_->PinObjectIDs(rpc_address_, {object_id}, ...);
    } else {
      RAY_RETURN_NOT_OK(plasma_store_provider_->Release(object_id));
    }
  }
  // 关键：写入 memory store 的标记是 OBJECT_IN_PLASMA（而非 error 本身）
  memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
                     object_id,
                     reference_counter_->HasReference(object_id));
  return Status::OK();
}
```

**关键结论**：error object 写入 plasma store，memory store 只写入 `OBJECT_IN_PLASMA` 标记。这意味着后续的 `GetAsync` 会拿到 `OBJECT_IN_PLASMA` 而非 error 本身。

### 4.2 Driver 端 ResolveDependencies 处理

```cpp
// src/ray/core_worker/task_submission/dependency_resolver.cc
// ResolveDependencies → GetAsync → 拿到 OBJECT_IN_PLASMA → IsInPlasmaError()=true
// → 不 inline → status=OK → task 发送到 actor
```

由于 memory store 中是 `OBJECT_IN_PLASMA`，driver 端认为 object 在 plasma 中可用，不会阻止 task 的提交。

### 4.3 Actor 端获取 Error Object

```cpp
// src/ray/core_worker/store_provider/plasma_store_provider.cc:283-292
// GetAndPinArgsForExecutor → ArgByRef=true → plasma_store_provider_->Get()
// → 从 plasma 拿到 error RayObject → IsException()=true → got_exception=true
// → 返回 OK（不报错，因为拿到了"东西"）
```

### 4.4 Python 层异常传播

```python
# python/ray/_raylet.pyx:893-902
def raise_if_dependency_failed(args):
    for arg in args:
        if isinstance(arg, RayError):
            raise arg  # ObjectReconstructionFailedError → raise
```

被外层 `except BaseException` 捕获：

```python
# python/ray/_raylet.pyx:2054
except BaseException as e:
    store_task_errors(e, ...)  # 包装为 RayTaskError → application_error=True
```

### 4.5 Driver 端处理 Actor Task Reply

```cpp
// src/ray/core_worker/task_submission/actor_task_submitter.cc:668-697
const bool is_retryable_exception = status.ok() && reply.is_retryable_error();
// is_retryable_exception = false（因为是 application error，不是 system error）

if (status.ok() && !is_retryable_exception) {
    // CompletePendingTask 路径
    task_manager_.CompletePendingTask(
        task_id, reply, addr, reply.is_application_error());
}
```

### 4.6 CompletePendingTask 中的重试判断

```cpp
// src/ray/core_worker/task_manager.cc:1065-1073
bool task_retryable = it->second.num_retries_left_ != 0 &&
                      !it->second.reconstructable_return_ids_.empty();
// streaming generator 首次执行失败 → reconstructable_return_ids_ 为空
// → task_retryable = false → 不重试

if (task_retryable) {
    release_lineage = false;
    // ...保留 task spec 用于重试...
} else {
    submissible_tasks_.erase(it);  // 从可提交列表中移除
}
```

### 4.7 Streaming Generator EOF

```cpp
// src/ray/core_worker/task_manager.cc:1077-1100
if (spec.IsStreamingGenerator()) {
    const auto generator_id = ObjectID::FromBinary(reply.return_objects(0).object_id());
    if (first_execution) {
        MarkEndOfStream(generator_id, reply.streaming_generator_return_ids_size());
    } else {
        if (is_application_error) {
            // 非首次执行失败 → 失败剩余的 generator returns
            for (size_t i = 0; i < spec.NumStreamingGeneratorReturns(); i++) {
                HandleTaskReturn(generator_return_id, return_object, ...);
            }
        }
    }
}
```

### 4.8 Ray Data 最终处理

```python
# python/ray/data/_internal/execution/interfaces/physical_operator.py
# _next_sync → ray.get(pending_block_ref) → 抛 RayTaskError
# → _task_done_callback(ex) → raise → _ClosingIterator → shutdown → 作业失败
```

**此路径不会导致 hang**——它会导致作业以异常终止。

---

## 五、Actor Task 被 Actor 不可用阻塞的路径

### 5.1 FailOrRetryPendingTask 路径

当 actor 不可用（网络错误或 actor 已死）时：

```cpp
// src/ray/core_worker/task_submission/actor_task_submitter.cc:717-796
if (!status.ok()) {
    // push task failed due to network error
    bool is_actor_dead = false;
    auto &queue = queue_pair->second;
    is_actor_dead = queue.state_ == rpc::ActorTableData::DEAD;

    if (is_actor_dead) {
        // actor 已死 → 用 actor death cause 标记 error
        error_info = gcs::GetErrorInfoFromActorDeathCause(death_cause);
    } else {
        // actor 仍然"活着"但不可达 → ACTOR_UNAVAILABLE
        error_info.set_error_type(rpc::ErrorType::ACTOR_UNAVAILABLE);
    }

    will_retry = task_manager_.FailOrRetryPendingTask(
        task_id, error_info.error_type(), &status, &error_info,
        /*mark_task_object_failed*/ is_actor_dead, fail_immediately);
}
```

### 5.2 FailOrRetryPendingTask 中的重试判断

```cpp
// src/ray/core_worker/task_manager.cc:1100-1260
bool TaskManager::FailOrRetryPendingTask(...) {
    int32_t num_retries_left = task_entry.num_retries_left_;
    bool will_retry = num_retries_left != 0;

    if (will_retry) {
        // 重试：重新提交 task
        SetTaskStatus(task_entry, rpc::TaskStatus::PENDING_ARGS_AVAIL, ...);
        async_retry_task_callback_(spec, delay_ms);
        return true;
    } else {
        // 不重试：标记 FAILED
        return false;
    }
}
```

**对于 Ray Data 的 actor task**，`max_retries=-1`（无限重试），所以 `num_retries_left != 0` 始终为 true → **ACTOR_UNAVAILABLE 时 task 会被无限重试**。

**但这里有个关键区别**：
- `ACTOR_UNAVAILABLE` + `max_retries=-1` → **无限重试**（task 在 driver 端的 task_manager 中不断重试）
- `ObjectReconstructionFailedError` → `CompletePendingTask` + `application_error=true` → **不重试**（因为 `reconstructable_return_ids_` 为空）

### 5.3 ACTOR_UNAVAILABLE 无限重试与 hang 的关系

ACTOR_UNAVAILABLE 的 task 被无限重试，但 **每次重试都需要重新发送到 actor**。如果 actor 一直不可用（RESTARTING），重试的 task 会回到 `SendPendingTasks` 的等待队列中，不断重试但永远无法执行。

**这不会产生新的输出，也不会触发异常**——只是静默地不断重试，直到 actor 恢复。但如果 actor 始终无法恢复（tidal 节点持续抢占），这些 task 会永远等待。

---

## 六、完整死锁链总结

```
1. Tidal 节点被 K8s 持续抢占
   → worker-1 上大量 actor 死亡 → actor 进入 RESTARTING

2. Actor 不可用 → 新提交的 actor task 两条路径：
   a. SendPendingTasks: actor 地址为空 → task 静默等待
   b. PushActorTask 网络失败 → ACTOR_UNAVAILABLE → FailOrRetryPendingTask
      → max_retries=-1 → 无限重试 → 但 actor 不恢复 → 再次 ACTOR_UNAVAILABLE
      → 循环（不产生输出，不触发异常）

3. QGInferMapper 无法消费 → QGPreprocessMapper 的 output_queue 堆积

4. Ray Data backpressure 触发：
   - ConcurrencyCapBackpressurePolicy: output queue 过大 → can_add_input=False
   - downstream_capacity_backpressure: 下游容量不足 → max_task_output_bytes_to_read=0

5. QGPreprocessMapper 被 backpressure → 不提交新 task
   → 上游 StreamingRepartition 也被 backpressure
   → 上游 ReadArrowJSON 也被反压
   → 整个 pipeline 停滞

6. GCS ghost RUNNING task：
   - worker 死亡但 worker_id 为空的 task 不被 MarkTasksFailedOnWorkerDead 标记
   - 这些 task 在 GCS 中永远显示为 RUNNING（实际已死）
   - 缺少 MarkTasksFailedOnNodeDead 机制

7. 少数恢复的 actor 执行 task 时遇到 error object → application error
   → CompletePendingTask + MarkEndOfStream → streaming generator EOF
   → Ray Data 检测到异常 → 但此路径只影响个别 task，不影响整体死锁

最终结果：整个 pipeline 在 backpressure 层面死锁，没有超时机制，
作业 hang 而非 fail。
```

---

## 七、社区相关 Issue

| Issue | 状态 | 描述 | 与本问题关系 |
|-------|------|------|-------------|
| [#53727](https://github.com/ray-project/ray/issues/53727) | OPEN | plasma object 丢失 + max_restarts=-1 导致 actor 重启后无限重试 | **最相似**：同样涉及 actor 不稳定 + object 丢失，但 creation arg 不是 method arg |
| [#62535](https://github.com/ray-project/ray/issues/62535) | CLOSED | streaming generator tasks 消失但 stream 未结束 | 不同：worker 启动失败 vs worker 被抢占 |
| [#62537](https://github.com/ray-project/ray/issues/62537) | CLOSED | MarkEndOfStream 缺失导致 stream 不结束 | 不同：本案例 MarkEndOfStream 正确调用 |
| [#60150](https://github.com/ray-project/ray/issues/60150) | CLOSED | actor alive 但内部 broken 导致无限重试 | 不同：actor alive vs actor RESTARTING |
| [#39800](https://github.com/ray-project/ray/issues/39800) | OPEN | 重试不足导致 object 永久丢失 | 方向相反：本案例是重试过多 |
| [#41477](https://github.com/ray-project/ray/issues/41477) | CLOSED | recovery resubmit 不受 max_retries 控制 | 不同层面 |

**GCS ghost RUNNING task 的问题尚未被社区报告**——搜索了以下关键词均无匹配：
- `"ghost RUNNING task GCS"`
- `"MarkTasksFailedOnWorkerDead node_id nil"`
- `"task stuck RUNNING worker preempted node died"`
- `"task event worker_id not set RUNNING"`
- `"GCS task manager mark failed node dead worker_id"`

---

## 八、多轮分析修正记录

### 错误1（已修正）：error object 被 inline 到 task spec

**错误说法**：recovery 失败后 error object 被 inline 到 task spec，导致 driver 端立即检测到异常。

**实际代码**：`recovery_failure_callback_` 调 `PutInLocalPlasmaStore` → error object 写入 plasma，memory store 写入 `RayObject(OBJECT_IN_PLASMA)` 标记 → `GetAsync` 拿到 `OBJECT_IN_PLASMA` → `IsInPlasmaError()=true` → **不 inline**，保留 ObjectRef。

### 错误2（已修正）：异常传播导致 worker 崩溃

**错误说法**：`ObjectReconstructionFailedError` 传播为 `UnexpectedSystemExit` 导致 worker 崩溃。

**实际代码**：`raise_if_dependency_failed` 在外层 `try`(line 1808)中抛出，被外层 `except BaseException`(line 2054)捕获 → `store_task_errors` 处理 → 正常返回，worker 不崩溃。

### 错误3（已修正）：max_retries=-1 导致无限重试

**错误说法**：`max_retries=-1` 导致 error object 路径的 task 无限重试。

**实际代码**：streaming generator 从未启动 → `reconstructable_return_ids_` 为空 → `task_retryable=false` → task 从 `submissible_tasks_` 中移除 → **不重试**。

### 正确结论

object 无法重建时，actor task **不会无限重试**，worker **不会崩溃**，task 正常返回 application error → Ray Data 检测到错误 → **作业失败而非 hang**。

但 ACTOR_UNAVAILABLE 路径的 task **会无限重试**（因为 `FailOrRetryPendingTask` 只看 `num_retries_left`，不看 `reconstructable_return_ids_`），这是另一条独立路径。

---

## 九、总结

| 问题 | 原因 | 是 recovery 问题? | 社区 issue? |
|------|------|------------------|-------------|
| PreprocessMapper/InferMapper 卡住 | pipeline 反压死锁（actor 不稳定 + 资源不足） | **否**，是调度/反压问题 | 无直接对应 |
| GCS ghost RUNNING task | `MarkTasksFailedOnWorkerDead` 只按 `worker_id` 查找，`worker_id` 为空时无法标记 FAILED；缺少 `MarkTasksFailedOnNodeDead` | **否**，是 GCS task event 管理缺陷 | **无**（未被社区报告） |
| 少数 task 遇到 error object | recovery 失败后 error 被正确处理为 application error，task 不无限重试 | **是**，但不导致 hang | 无直接对应 |
| ACTOR_UNAVAILABLE 无限重试 | `FailOrRetryPendingTask` + `max_retries=-1` → 无限重试但 actor 不恢复 | **否**，是 actor 调度问题 | 类似 #53727 |

**根本解决方案**：
1. 避免将关键 actor 调度到 tidal/可抢占节点
2. 启用 `enable_object_replication=true` 和 `enable_pin_transfer=true`
3. 为 pipeline 增加 watchdog 超时机制（检测长时间无进展 → 自动 fail）
4. GCS 增加 `MarkTasksFailedOnNodeDead` 机制（按 node_id 索引）
