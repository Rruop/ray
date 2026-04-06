# Ray Task Event 数据链路、淘汰机制与可见性分析

> 集群规模：26 节点 | Ray 版本：2.52.1 | 作业总 task 数：5200 万+
> 问题日期：2025-05-14
> 关联文档：[Task Event 淘汰 + Lease 卡死分析](./ray-task-event-eviction-and-lease-stuck-analysis.md)

---

## 一、Dashboard Task 数据获取链路

### 1.1 完整调用链

Dashboard 展示的 task 数据，从前端到 GCS 内存的完整路径：

```
Dashboard 前端
  → GET /api/v0/tasks/summarize?filter_keys=job_id&filter_predicates=%3D&filter_values=04000000
    → state_head.py:300  StateHead.summarize_tasks()
      → state_api_utils.py:111  handle_summary_api(self._state_api.summarize_tasks, req)
        → state_api_utils.py:104  summary_options_from_req(req) 解析请求参数
        → state_aggregator.py:569  StateAPIManager.summarize_tasks(option)
          → 内部调用 self.list_tasks(limit=RAY_MAX_LIMIT_FROM_API_SERVER, filters=...)
            → state_aggregator.py:298  self._client.get_all_task_info(filters=...)
              → state_manager.py:232  gRPC GetTaskEvents(request) → GCS
                → gcs_task_manager.cc:426  HandleGetTaskEvents()
                  → 从 GCS 内存存储 (task_events_list_[0..2]) 中查询
                    → 返回当前存储在 GCS 中的 task events
          → 对返回结果按 func_or_class_name 分组聚合为 state_counts
            → 返回 {summary: {"func_name": {state_counts: {"RUNNING": 5, "FINISHED": 100}}, ...}}
```

### 1.2 /api/v0/tasks/summarize 路由定义

**文件**: `python/ray/dashboard/modules/state/state_head.py:300-304`

```python
@routes.get("/api/v0/tasks/summarize")
@RateLimitedModule.enforce_max_concurrent_calls
async def summarize_tasks(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
    record_extra_usage_tag(TagKey.CORE_STATE_API_SUMMARIZE_TASKS, "1")
    return await handle_summary_api(self._state_api.summarize_tasks, req)
```

### 1.3 请求参数解析

**文件**: `python/ray/dashboard/state_api_utils.py:104-108`

```python
def summary_options_from_req(req: aiohttp.web.Request) -> SummaryApiOptions:
    timeout = int(req.query.get("timeout", DEFAULT_RPC_TIMEOUT))
    filters = _get_filters_from_req(req)
    summary_by = req.query.get("summary_by", None)
    return SummaryApiOptions(timeout=timeout, filters=filters, summary_by=summary_by)
```

**文件**: `python/ray/dashboard/state_api_utils.py:63-73`

```python
def _get_filters_from_req(req):
    filter_keys = req.query.getall("filter_keys", [])
    filter_predicates = req.query.getall("filter_predicates", [])
    filter_values = req.query.getall("filter_values", [])
    assert len(filter_keys) == len(filter_values)
    filters = []
    for key, predicate, val in zip(filter_keys, filter_predicates, filter_values):
        filters.append((key, predicate, val))
    return filters
```

支持的参数：
- `timeout` — 请求超时(秒)
- `filter_keys` / `filter_predicates` / `filter_values` — 支持 `job_id`, `actor_id`, `task_id`, `name`, `state`
- `summary_by` — 聚合方式：`"func_name"` (默认) 或 `"lineage"`

### 1.4 summarize_tasks 核心逻辑

**文件**: `python/ray/dashboard/state_aggregator.py:569-618`

```python
async def summarize_tasks(self, option: SummaryApiOptions) -> SummaryApiResponse:
    summary_by = option.summary_by or "func_name"

    # summarize 内部调用的就是 list_tasks，尝试获取尽可能多的条目
    result = await self.list_tasks(
        option=ListApiOptions(
            timeout=option.timeout,
            limit=RAY_MAX_LIMIT_FROM_API_SERVER,  # 尽量多拿
            filters=option.filters,
            detail=summary_by == "lineage",
        )
    )

    # 客户端侧聚合：按 func_or_class_name 分组统计各状态计数
    if summary_by == "func_name":
        summary_results = TaskSummaries.to_summary_by_func_name(tasks=result.result)
    else:
        actors = await self.list_actors(...)
        summary_results = TaskSummaries.to_summary_by_lineage(
            tasks=result.result, actors=actors.result
        )

    # 数据丢失检测
    if (summary_results.total_actor_scheduled + summary_results.total_actor_tasks
        + summary_results.total_tasks < result.num_filtered):
        warnings.append(
            "There is missing data in this aggregation. "
            "Possibly due to task data being evicted to preserve memory."
        )
```

### 1.5 list_tasks → GCS GetTaskEvents RPC

**文件**: `python/ray/dashboard/state_aggregator.py:298-356`

```python
async def list_tasks(self, *, option: ListApiOptions) -> ListApiResponse:
    reply = await self._client.get_all_task_info(
        timeout=option.timeout,
        filters=option.filters,
        exclude_driver=option.exclude_driver,
    )
    # reply.events_by_task 包含 GCS 当前存储的 task events
    result = [protobuf_to_task_state_dict(message) for message in reply.events_by_task]
    num_total = len(result) + reply.num_status_task_events_dropped
    # ...
```

**文件**: `python/ray/util/state/state_manager.py:232-294`

```python
async def get_all_task_info(self, timeout, limit, filters, exclude_driver):
    req_filters = GetTaskEventsRequest.Filters()
    for filter in filters:
        key, predicate, value = filter
        # 构建 protobuf filter（支持 actor_id, job_id, task_id, name, state）
    request = GetTaskEventsRequest(limit=limit, filters=req_filters)
    reply = await self._gcs_task_info_stub.GetTaskEvents(request, timeout=timeout)
    return reply
```

### 1.6 关键结论

| 问题 | 答案 |
|------|------|
| summarize 和 list_tasks 的数据源 | **完全相同**，都是 GCS GetTaskEvents RPC |
| summarize 的区别 | 拿到数据后在 Dashboard 侧按 func_name/lineage 聚合 |
| GCS 返回什么 | 当前**内存中**的 task event（最多 100K 条） |
| 被淘汰的 task | dashboard 彻底看不到，无论用哪个接口 |

---

## 二、Worker Buffer 存储与合并机制

### 2.1 Buffer 中每个状态转换是独立条目

Worker 的 `TaskEventBufferImpl` 使用 `boost::circular_buffer` 存储 task status event。**每一次状态转换都是 buffer 中的一个独立条目，不做合并**。

**文件**: `src/ray/core_worker/task_event_buffer.h:577`

```cpp
/// Circular buffered task status events.
boost::circular_buffer<std::shared_ptr<TaskEvent>> status_events_
    ABSL_GUARDED_BY(mutex_);
```

**文件**: `src/ray/core_worker/task_event_buffer.cc:1009-1054`

```cpp
void TaskEventBufferImpl::AddTaskStatusEvent(std::unique_ptr<TaskEvent> status_event) {
  absl::MutexLock lock(&mutex_);
  if (!enabled_) {
    return;
  }
  std::shared_ptr<TaskEvent> status_event_shared_ptr = std::move(status_event);

  // 检查该 task attempt 是否已被标记为 dropped
  if (dropped_task_attempts_unreported_.count(
          status_event_shared_ptr->GetTaskAttempt()) != 0u) {
    // 之前已被淘汰，后续所有事件直接丢弃
    stats_counter_.Increment(
        TaskEventBufferCounter::kNumTaskStatusEventDroppedSinceLastFlush);
    return;
  }

  if (status_events_.full()) {
    // Buffer 满了，FIFO 淘汰最老事件
    const auto &to_evict = status_events_.front();
    // 标记被淘汰事件的 task attempt 为 dropped
    auto inserted = dropped_task_attempts_unreported_.insert(to_evict->GetTaskAttempt());
    stats_counter_.Increment(
        TaskEventBufferCounter::kNumTaskStatusEventDroppedSinceLastFlush);
    // ... 日志告警
  } else {
    stats_counter_.Increment(TaskEventBufferCounter::kNumTaskStatusEventsStored);
  }
  // 入 buffer（如果 buffer 满了会自动覆盖 front）
  status_events_.push_back(status_event_shared_ptr);
}
```

Buffer 中的实际存储示例：

```
index  内容
[0]    TaskStatusEvent(task_A, PENDING_ARGS_AVAIL,    t=100)
[1]    TaskStatusEvent(task_A, PENDING_NODE_ASSIGNMENT, t=101)
[2]    TaskStatusEvent(task_B, PENDING_ARGS_AVAIL,    t=102)
[3]    TaskStatusEvent(task_A, SUBMITTED_TO_WORKER,   t=200)
[4]    TaskStatusEvent(task_B, PENDING_NODE_ASSIGNMENT, t=201)
[5]    TaskStatusEvent(task_C, PENDING_ARGS_AVAIL,    t=202)
...
```

**对容量的影响**：一个 task 完整生命周期有 4-5 个状态转换（PENDING_ARGS → PENDING_NODE → SUBMITTED → RUNNING/FINISHED），100K 容量的 buffer 实际只能承载约 **25K 个 task 的完整生命周期事件**。

### 2.2 单个 TaskStatusEvent 的数据结构

**文件**: `src/ray/core_worker/task_event_buffer.h:119-220`

```cpp
/// TaskStatusEvent is generated when a task changes its status.
class TaskStatusEvent : public TaskEvent {
private:
  /// 一个状态值（如 RUNNING、FINISHED 等）
  rpc::TaskStatus task_status_ = rpc::TaskStatus::NIL;

  /// 该状态发生的时间戳
  int64_t timestamp_ = -1;

  /// Task 规格（仅在 include_task_info=true 的首次事件中设置）
  std::shared_ptr<const TaskSpecification> task_spec_ = nullptr;

  /// 可选的状态更新（node_id, worker_id, error_info 等）
  std::optional<const TaskStateUpdate> state_update_ = std::nullopt;
};
```

每个 `TaskStatusEvent` 只代表**一次状态转换**，不是完整的状态历史。

### 2.3 Flush 时的合并：CreateDataToSend

合并**只发生在 flush 时**。定时 flush 从 buffer 取出最多 `task_events_send_batch_size` (默认 10,000) 条 event，按 `TaskAttempt` 分组聚合为 protobuf。

**文件**: `src/ray/core_worker/task_event_buffer.cc:729-764`

```cpp
TaskEventBuffer::TaskEventDataToSend TaskEventBufferImpl::CreateDataToSend(
    const std::vector<std::shared_ptr<TaskEvent>> &status_events_to_send,
    const std::vector<std::shared_ptr<TaskEvent>> &profile_events_to_send,
    const absl::flat_hash_set<TaskAttempt> &dropped_task_attempts_to_send) {

  // 按 TaskAttempt (task_id + attempt_number) 聚合
  absl::flat_hash_map<TaskAttempt, rpc::TaskEvents> agg_task_events;

  auto to_rpc_event_fn = [&](const std::shared_ptr<TaskEvent> &event) {
        if (dropped_task_attempts_to_send.contains(event->GetTaskAttempt())) {
          return;  // 被 drop 的 task attempt，不发送
        }
        // try_emplace：同一 task attempt 复用同一个 rpc::TaskEvents 对象
        auto [itr_task_events, _] =
            agg_task_events.try_emplace(event->GetTaskAttempt());
        // 将当前事件的状态+时间戳合并到同一个 protobuf 中
        event->ToRpcTaskEvents(&(itr_task_events->second));
      };

  std::for_each(status_events_to_send.begin(), status_events_to_send.end(), to_rpc_event_fn);
  std::for_each(profile_events_to_send.begin(), profile_events_to_send.end(), to_rpc_event_fn);
  // ...
}
```

### 2.4 ToRpcTaskEvents：状态合并到 state_ts_ns map

**文件**: `src/ray/common/protobuf_utils.cc:351-359`

```cpp
void FillTaskStatusUpdateTime(const ray::rpc::TaskStatus &task_status,
                              int64_t timestamp,
                              ray::rpc::TaskStateUpdate *state_updates) {
  if (task_status == rpc::TaskStatus::NIL) {
    return;
  }
  // 向 state_ts_ns map 中添加一个条目
  (*state_updates->mutable_state_ts_ns())[task_status] = timestamp;
}
```

**protobuf 定义**: `src/ray/protobuf/gcs.proto:197-215`

```protobuf
message TaskStateUpdate {
  optional bytes node_id = 1;
  optional bytes worker_id = 8;
  optional RayErrorInfo error_info = 9;
  // Key 是 TaskStatus 枚举的整数值，Value 是该状态发生时的时间戳
  map<int32, int64> state_ts_ns = 14;
}

message TaskEvents {
  bytes task_id = 1;
  int32 attempt_number = 2;
  optional TaskInfoEntry task_info = 3;
  optional TaskStateUpdate state_updates = 4;
  optional ProfileEvents profile_events = 5;
  bytes job_id = 6;
}
```

### 2.5 合并后发给 GCS 的数据结构

一次 flush 后，同一 task attempt 的多个状态事件被合并为一个 `rpc::TaskEvents`：

```
rpc::TaskEvents {
  task_id: "task_A"
  attempt_number: 0
  task_info: { name: "read_parquet", type: NORMAL_TASK, ... }
  state_updates: {
    state_ts_ns: {
      PENDING_ARGS_AVAIL:    1620000000000,
      PENDING_NODE_ASSIGNMENT: 1620000001000,
      SUBMITTED_TO_WORKER:   1620000002000,
    }
    node_id: <assigned_node>
    worker_id: <assigned_worker>
  }
}
```

### 2.6 GCS 侧接收：MergeFrom

GCS 收到后，如果该 task attempt 已存在，使用 protobuf 的 `MergeFrom` 合并：

**文件**: `src/ray/gcs/gcs_task_manager.cc:167-207`

```cpp
void GcsTaskManager::GcsTaskManagerStorage::UpdateExistingTaskAttempt(
    const std::shared_ptr<TaskEventLocator> &loc,
    const rpc::TaskEvents &task_events) {
  auto &existing_task = loc->GetTaskEventsMutable();

  // protobuf MergeFrom：map 字段会追加新 key，覆盖已有 key
  existing_task.MergeFrom(task_events);

  // 更新 GC 优先级列表位置（如 PENDING → FINISHED 后优先级从 2 变为 0）
  auto target_list_index = gc_policy_->GetTaskListPriority(existing_task);
  auto cur_list_index = loc->GetCurrentListIndex();
  if (target_list_index != cur_list_index) {
    task_events_list_[target_list_index].push_front(std::move(existing_task));
    task_events_list_[cur_list_index].erase(loc->GetCurrentListIterator());
    loc->SetCurrentList(target_list_index, task_events_list_[target_list_index].begin());
  }
}
```

这意味着跨多次 flush 的状态也会被合并。例如：
- 第 1 次 flush 上报 `{PENDING_ARGS: t1, PENDING_NODE: t2}`
- 第 2 次 flush 上报 `{SUBMITTED: t3, FINISHED: t4}` （从 executor 来的 `RUNNING: t3.5` 也会 merge 进来）
- GCS 最终存储 `{PENDING_ARGS: t1, PENDING_NODE: t2, SUBMITTED: t3, RUNNING: t3.5, FINISHED: t4}`

---

## 三、Owner Buffer vs Executor Buffer

### 3.1 两个 Buffer 的职责分工

一个 task 的状态事件由**两方**报告，各自使用自己的 `TaskEventBuffer`：

```
Owner (Driver) 的 TaskEventBuffer           Executor (远端 Worker) 的 TaskEventBuffer
┌──────────────────────────────────┐       ┌──────────────────────────────┐
│ PENDING_ARGS_AVAIL     (提交时)   │       │ RUNNING            (开始执行) │
│ PENDING_NODE_ASSIGNMENT (等调度)  │       │                              │
│ SUBMITTED_TO_WORKER    (已派发)   │       │                              │
│ FINISHED / FAILED      (完成时)   │       │                              │
└──────────────────────────────────┘       └──────────────────────────────┘
         ↓ flush (每 report_interval_ms)           ↓ flush
        GCS                                       GCS
         ↓ MergeFrom 合并同一 task attempt 的所有状态
   state_ts_ns = {PENDING_ARGS:t1, PENDING_NODE:t2, SUBMITTED:t3, RUNNING:t4, FINISHED:t5}
```

### 3.2 Owner 侧报告的状态（调用方 = Driver）

**文件**: `src/ray/core_worker/task_manager.cc`

```cpp
// 1. 提交时 — PENDING_ARGS_AVAIL
// task_manager.cc:343-349
void TaskManager::AddPendingTask(...) {
  SetTaskStatus(task_entry, rpc::TaskStatus::PENDING_ARGS_AVAIL);
}

// 2. 参数就绪，请求 lease — PENDING_NODE_ASSIGNMENT
// task_manager.cc:1672-1683
void TaskManager::SetTaskPendingNodeAssignment(...) {
  SetTaskStatus(task_entry, rpc::TaskStatus::PENDING_NODE_ASSIGNMENT);
}

// 3. Worker 被分配 — SUBMITTED_TO_WORKER
// (在 NormalTaskSubmitter 回调中)

// 4. 执行完毕收到回复 — FINISHED
// task_manager.cc:1053
void TaskManager::CompletePendingTask(...) {
  SetTaskStatus(it->second, rpc::TaskStatus::FINISHED);
}
```

### 3.3 Executor 侧报告的状态（执行方 = 远端 Worker）

**文件**: `src/ray/core_worker/core_worker.cc:2855-2874`

```cpp
// 开始执行 task 时报告 RUNNING
if (!options_.is_local_mode) {
    worker::TaskStatusEvent::TaskStateUpdate update;
    {
      absl::MutexLock lock(&mutex_);
      update = (task_spec.IsActorTask() && !actor_repr_name_.empty())
                   ? worker::TaskStatusEvent::TaskStateUpdate(actor_repr_name_, pid_)
                   : worker::TaskStatusEvent::TaskStateUpdate(pid_);
    }

    RAY_UNUSED(
        task_event_buffer_->RecordTaskStatusEventIfNeeded(
            task_spec.TaskId(),
            task_spec.JobId(),
            task_spec.AttemptNumber(),
            task_spec,
            rpc::TaskStatus::RUNNING,        // ← 只报告 RUNNING
            /*include_task_info=*/false,      // ← 不含 task_info（由 Owner 首次上报）
            update));
}
```

### 3.4 压力分布

| 角色 | Buffer | 承载的事件 | 压力 |
|------|--------|-----------|------|
| Owner (Driver) | 1 个 buffer | 所有 task 的 PENDING_ARGS + PENDING_NODE + SUBMITTED + FINISHED | **极高** — 5200万 task × 4 状态 = 2亿+ 事件 |
| Executor (各 Worker) | 每节点 1 个 buffer | 仅该 Worker 执行的 task 的 RUNNING 事件 | 分散 — 每 Worker 只处理分配给它的 task |

**这就是为什么 PENDING_NODE_ASSIGNMENT 被淘汰的 buffer 一定是 Owner (Driver) 侧的 buffer** — 因为 PENDING_NODE_ASSIGNMENT 只有 Owner 会报告。

---

## 四、两层淘汰机制的关系

### 4.1 串联关系

事件必须经过两道关卡才能在 dashboard 上可见：

```
Task 状态变化
  → [第一层] Worker 侧 TaskEventBufferImpl (circular_buffer, 容量 100K)
      │  ← 存在 buffer 中等待定时 flush
      │  ← 如果 buffer 满了：FIFO 淘汰最老事件，标记该 task attempt 为 dropped
      │  ← 一旦标记 dropped，该 task attempt 的所有后续事件直接丢弃
      │
      │  定时 flush (每 report_interval_ms 触发一次)
      │  ← 如果上一次 gRPC 还在 in_flight → 跳过本次 flush（背压）
      │  ← 每次最多取 task_events_send_batch_size (10000) 条发送
      │  ← CreateDataToSend 按 TaskAttempt 聚合后发送
      ↓
  → [第二层] GCS GcsTaskManagerStorage (内存，容量 100K)
      │  ← 收到事件后 AddOrReplaceTaskEvent → 存储或合并
      │  ← 如果存储满了：按优先级淘汰
      │     Priority 0 (先淘汰): FINISHED task
      │     Priority 1 (次之):   Actor task (RUNNING 状态的 ACTOR_TASK)
      │     Priority 2 (最后):   其他 (含 PENDING_NODE_ASSIGNMENT 的 normal task)
      │  ← 同优先级内：FIFO（最老的先被淘汰）
      ↓
  → Dashboard 查询 GCS 内存获取结果
```

### 4.2 两层对比

| 层面 | Worker 侧 Buffer | GCS 侧 Storage |
|------|------------------|----------------|
| 容量 | 100,000 个 event 条目 | 100,000 个 task attempt |
| 淘汰策略 | 纯 FIFO（不区分 task 状态） | 优先级（先完成的→Actor→其他） |
| 数据粒度 | 单条 status event | 整个 task attempt（含所有状态的合并 event） |
| 触发条件 | 新事件入 buffer 时 buffer 已满 | 新 task attempt 存入时存储已满 |
| dropped 标记效果 | 后续所有 event 直接丢弃 | 后续上报的 event 也丢弃 |

### 4.3 两层之间的交互

#### Worker drop → 通知 GCS

Worker flush 时会把 `dropped_task_attempts_unreported_` 集合发给 GCS：

**文件**: `src/ray/core_worker/task_event_buffer.cc:616-630`

```cpp
// 从 dropped 集合中取出 task attempt 发送给 GCS
while (!dropped_task_attempts_unreported_.empty()) {
    auto itr = dropped_task_attempts_unreported_.begin();
    dropped_task_attempts_to_send->insert(*itr);
    dropped_task_attempts_unreported_.erase(itr);
    num_dropped_task_attempts_to_send++;
}
```

GCS 收到后的处理：

**文件**: `src/ray/gcs/gcs_task_manager.cc:623-647`

```cpp
void GcsTaskManager::GcsTaskManagerStorage::RecordDataLossFromWorker(
    const rpc::TaskEventData &data) {
  for (const auto &dropped_attempt : data.dropped_task_attempts()) {
    const auto task_id = TaskID::FromBinary(dropped_attempt.task_id());
    auto attempt_number = dropped_attempt.attempt_number();
    auto job_id = task_id.JobId();
    // 记录到 job 级别的 drop 统计
    job_task_summary_[job_id].RecordTaskAttemptDropped(
        std::make_pair(task_id, attempt_number));

    // 如果 GCS 中已有该 task attempt 的数据，主动删除
    const auto &loc_iter = primary_index_.find(std::make_pair(task_id, attempt_number));
    if (loc_iter != primary_index_.end()) {
      RemoveTaskAttempt(loc_iter->second);
    }
  }
}
```

#### GCS evict → Worker 无感知

GCS 淘汰的 task attempt **不会通知 Worker**。Worker 可能还在上报这个 task 的新事件，但 GCS 的 `ShouldDropTaskAttempt()` 检查会直接丢弃：

**文件**: `src/ray/gcs/gcs_task_manager.cc:365-372`

```cpp
// 检查 GCS 是否已经标记该 task attempt 为 dropped
if (job_task_summary_[job_id].ShouldDropTaskAttempt(GetTaskAttempt(events_by_task))) {
    RAY_LOG(DEBUG) << "already dropping task " << ...;
    return;  // 直接丢弃，不存储
}
```

#### 双重淘汰的叠加效应

一个 task 可能：
1. 在 Worker buffer 中被淘汰（第一层丢失）→ 后续所有状态事件全部丢弃
2. 即使没在 Worker 被淘汰，也可能在 GCS 被淘汰（第二层丢失）
3. 两层都淘汰 = dashboard 上**完全不可见**

---

## 五、GCS 的角色：纯可观测性，不参与 Task 完成感知

### 5.1 Task 完成通知路径（不经过 GCS）

```
Task 执行完成:
  Executor Worker
    → 直接 RPC 回复 PushTaskReply → Owner (Driver) 的 TaskManager
      → task_manager.cc:1053  SetTaskStatus(FINISHED)
      → 通过 ObjectRef 机制通知 ray.wait() / ray.get()
```

具体流程：
1. 执行 task 的 Worker 执行完毕，通过**直接 RPC** 回复 Owner（不经过 GCS）
2. Owner 的 `TaskManager::CompletePendingTask()` 收到回复，标记 task 为 FINISHED
3. Owner 把返回值写入 object store 或直接返回
4. `ray.wait()` / `ray.get()` 通过 object ready 事件感知到完成

**文件**: `src/ray/core_worker/task_manager.cc:1046-1055`

```cpp
// Owner 收到 task 执行结果的回调
if (is_application_error) {
    SetTaskStatus(
        it->second,
        rpc::TaskStatus::FAILED,
        worker::TaskStatusEvent::TaskStateUpdate(gcs::GetRayErrorInfo(
            rpc::ErrorType::TASK_EXECUTION_EXCEPTION, reply.task_execution_error())));
} else {
    SetTaskStatus(it->second, rpc::TaskStatus::FINISHED);
}
num_pending_tasks_--;
```

### 5.2 GCS 在 Task 生命周期中的角色

| 功能 | 是否依赖 GCS task event |
|------|----------------------|
| Task 调度 (RequestWorkerLease) | ❌ 不依赖 — 通过 RaySyncer 资源视图 |
| Task 完成通知 | ❌ 不依赖 — Owner 和 Executor 直接 RPC |
| Task 重试决策 | ❌ 不依赖 — Owner 自己的 TaskManager 维护 |
| Dashboard task 状态展示 | ✅ 依赖 — **唯一数据源** |
| `ray list tasks` CLI | ✅ 依赖 |
| Task 失败标记 (Worker 死亡时) | ✅ 依赖 — GCS 收到 Worker 死亡后标记未完成 task 为 FAILED |

**GCS 对 task event 的角色纯粹是可观测性存储**。即使 GCS 完全不可用，task 执行和完成通知也不受影响（只是 dashboard 看不到）。

### 5.3 GCS 主动标记 task 失败的场景

GCS 在以下情况会主动修改 task 状态（但这是可观测性层面的修正，不影响实际调度）：

**Worker 死亡时**:

**文件**: `src/ray/gcs/gcs_task_manager.cc:729-748`

```cpp
void GcsTaskManager::OnWorkerDead(
    const WorkerID &worker_id,
    const std::shared_ptr<rpc::WorkerTableData> &worker_data) {
  // 延迟执行（等待 worker 上报最终状态的机会）
  timer->async_wait([this, worker_id, worker_data](...) {
    // 将该 worker 上所有未终止的 task 标记为 FAILED
    task_event_storage_->MarkTasksFailedOnWorkerDead(worker_id, *worker_data);
  });
}
```

**Job 结束时**:

**文件**: `src/ray/gcs/gcs_task_manager.cc:750-771`

```cpp
void GcsTaskManager::OnJobFinished(const JobID &job_id, int64_t job_finish_time_ms) {
  timer->async_wait([this, job_id, job_finish_time_ms](...) {
    // 将该 job 所有未终止的 task 标记为 FAILED
    task_event_storage_->MarkTasksFailedOnJobEnds(job_id, job_finish_time_ms * 1000 * 1000);
  });
}
```

---

## 六、Ray Data 作业的调用方与 Raylet 节点关系

### 6.1 结论：所有 task 由 Driver 直接提交

对于使用 `TaskPoolMapOperator` 的 Ray Data 作业，**所有 `.remote()` 调用都在 Driver 进程的 `StreamingExecutor` 线程中发起**，不经过中间 Actor。

### 6.2 代码证据

#### StreamingExecutor 是 Driver 进程中的线程

**文件**: `python/ray/data/_internal/execution/streaming_executor.py:75, 134-135`

```python
class StreamingExecutor(Executor, threading.Thread):
    """A streaming Dataset executor."""
    Executor.__init__(self, self._data_context.execution_options)
    thread_name = f"StreamingExecutor-{self._dataset_id}"
    threading.Thread.__init__(self, daemon=True, name=thread_name)
```

#### 调度循环在 Driver 线程中执行

**文件**: `python/ray/data/_internal/execution/streaming_executor.py:621`

```python
# _scheduling_loop_step 中
topology[op].dispatch_next_task()
```

#### TaskPoolMapOperator 直接 .remote()

**文件**: `python/ray/data/_internal/execution/operators/task_pool_map_operator.py:108-143`

```python
def _try_schedule_task(self, bundle: RefBundle, strict: bool):
    gen = self._map_task.options(**dynamic_ray_remote_args).remote(
        self._map_transformer_ref,
        data_context,
        ctx,
        *bundle.block_refs,
        slices=bundle.slices,
        **self.get_map_task_kwargs(),
    )
    self._submit_data_task(gen, bundle)
```

#### ActorPoolMapOperator 也从 Driver 提交

**文件**: `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py:381-390`

```python
def _try_schedule_tasks_internal(self, ...):
    gen = actor.submit.options(
        num_returns="streaming",
        **self._ray_actor_task_remote_args,
    ).remote(
        self.data_context, ctx, *input_blocks,
        slices=bundle.slices, **self.get_map_task_kwargs(),
    )
```

#### PhysicalOperator 明确说明只在 Driver 侧

**文件**: `python/ray/data/_internal/execution/interfaces/physical_operator.py:399-400`

```python
# Physical operators are stateful and non-serializable;
# they live on the driver side of the Dataset only.
```

### 6.3 架构示意图

```
Driver 进程 (单节点):
┌───────────────────────────────────────────────────────┐
│  StreamingExecutor 线程                                │
│    → 每秒提交几千个 .remote() 调用                     │
│                                                       │
│  TaskManager:                                         │
│    → 5200 万 task 的 pending/complete 追踪             │
│                                                       │
│  TaskEventBuffer (circular_buffer 100K):              │
│    → 所有 task 的 4 个状态转换都进这一个 buffer         │
│    → 5200万 × 4 = 2亿+ 条 event 要经过这个 buffer     │
│    → 但 buffer 只能存 100K 条                          │
│    → flush 速率只有 1K/s (report_interval=10s 配置)    │
│                                                       │
│  NormalTaskSubmitter:                                  │
│    → 所有 RequestWorkerLease 从这里发出                │
└───────────────────────────────────────────────────────┘
           │
           ▼
Driver 所在节点的 Raylet:
┌───────────────────────────────────────────────────────┐
│  ClusterLeaseManager:                                  │
│    → 接收所有来自 Driver 的 lease 请求                 │
│    → GetBestSchedulableNode → 选目标节点               │
│    → ScheduleOnNode → spillback 到远端 Raylet          │
│    → 那 2,095,790 个 lease 请求全从这里发出            │
└───────────────────────────────────────────────────────┘
           │
           ▼ (spillback)
远端 Raylet → 分配 Worker → Worker 执行 task
           │
           ▼
远端 Worker 的 TaskEventBuffer:
  → 只报告自己执行的 task 的 RUNNING 状态
  → 压力分散在多个节点
```

### 6.4 单点瓶颈效应

| 组件 | 位置 | 瓶颈原因 |
|------|------|---------|
| `StreamingExecutor` | Driver 线程 | 单线程调度所有 task |
| `TaskEventBuffer` | Driver CoreWorker | 一个 100K buffer 承载 2亿+ event |
| `TaskManager` | Driver CoreWorker | 维护所有 pending task 状态 |
| `RequestWorkerLease` | Driver Raylet | 所有 lease 请求的唯一入口 |
| `ray.wait()` | Driver 线程 | 轮询所有 task 完成状态 |

---

## 七、`task_events_report_interval_ms=10000` 的影响分析

### 7.1 与默认值对比

默认值是 1000ms (1s)，配置为 10000ms (10s)，**差 10 倍**。

| 指标 | 默认 (1000ms) | 配置 (10000ms) |
|------|--------------|----------------|
| flush 频率 | 每秒 1 次 | 每 10 秒 1 次 |
| 每次 flush 最大批量 | 10,000 events | 10,000 events |
| 最大 drain 速率 | 10,000 events/s | 1,000 events/s |
| buffer 可积累时间 | 1s | 10s |

### 7.2 Flush 触发逻辑

**文件**: `src/ray/core_worker/task_event_buffer.cc:477-521`

```cpp
void TaskEventBufferImpl::Start(bool send_task_events_to_gcs) {
  // ...
  auto report_interval_ms = RayConfig::instance().task_events_report_interval_ms();
  // 定时触发 flush
  periodical_runner_->RunFnPeriodically(
      [this] { FlushEvents(/*forced=*/false); },
      report_interval_ms,
      "CoreWorker.deadline_timer.flush_task_events");
}
```

### 7.3 背压机制

**文件**: `src/ray/core_worker/task_event_buffer.cc:912-938`

```cpp
void TaskEventBufferImpl::FlushEvents(bool forced) {
  if (!enabled_ && !stopping_) {
    return;
  }

  // 背压：上一次 gRPC 还在 in_flight → 跳过本次 flush
  if ((gcs_grpc_in_progress_.load() > 0 ||
       event_aggregator_grpc_in_progress_.load() > 0) &&
      !forced) {
    RAY_LOG_EVERY_N_OR_DEBUG(WARNING, 100)
        << "GCS or the event aggregator hasn't replied to the previous flush events "
           "call (likely overloaded). Skipping reporting task state events and retry later."
        << "[gcs_grpc_in_progress=" << gcs_grpc_in_progress_.load() << "]"
        << "[cur_status_events_size="
        << stats_counter_.Get(TaskEventBufferCounter::kNumTaskStatusEventsStored)
        << "]";
    return;
  }
  // ...
}
```

### 7.4 Buffer 溢出计算

假设作业每秒产生 2,000 个 task event（不算多，如果一个 task 有 4-5 个状态转换）：

**默认 1s flush**：
```
T=0s: buffer 积累 2,000 → flush 2,000 → buffer = 0
T=1s: buffer 积累 2,000 → flush 2,000 → buffer = 0
永远不会溢出
```

**10s flush**：
```
T=0-10s: buffer 积累 20,000 → flush 10,000 (batch limit) → buffer 剩 10,000
T=10-20s: buffer 积累 20,000 → 总 30,000 → flush 10,000 → buffer 剩 20,000
T=20-30s: buffer 积累 20,000 → 总 40,000 → flush 10,000 → buffer 剩 30,000
...
T=40-50s: buffer 总量接近 100K → 开始 FIFO 淘汰！
```

**加上背压**：
```
T=0s:  flush 10K events, gRPC in_flight
T=10s: gRPC 还没返回 → 跳过 (buffer 积累 20K)
T=20s: gRPC 还没返回 → 跳过 (buffer 积累 40K)
T=30s: gRPC 还没返回 → 跳过 (buffer 积累 60K)
T=40s: gRPC 还没返回 → 跳过 (buffer 积累 80K)
T=50s: gRPC 还没返回 → 跳过 (buffer 积累 100K → 溢出！)

一次 GCS 慢响应可能导致 50s 内 buffer 打满
```

### 7.5 对可见性的影响

- 一个 task 从 RUNNING 到 FINISHED 的状态变化，最多要等 **10 秒**才被 flush 到 GCS
- 如果 task 执行时间 < 10s，task 从提交到完成可能在**一次 flush 间隔内完成所有状态转换**
- Dashboard 在这 10 秒窗口内**看不到 RUNNING 状态**

### 7.6 建议

降低到默认的 1000ms，或至少 3000ms。10000ms 在大量 task 场景下会严重影响可见性和 buffer 稳定性。

---

## 八、Actor Task 不可见原因分析

### 8.1 GCS 优先淘汰 Actor Task

**文件**: `src/ray/gcs/gcs_task_manager.h:71-86`

```cpp
class FinishedTaskActorTaskGcPolicy : public TaskEventsGcPolicyInterface {
 public:
  size_t MaxPriority() const override { return 3; }
  size_t GetTaskListPriority(const rpc::TaskEvents &task_events) const override {
    if (IsTaskFinished(task_events)) return 0;  // 已完成 → 最先淘汰
    if (IsActorTask(task_events))    return 1;  // Actor task → 次之
    return 2;                                    // 其他(含 PENDING normal task) → 最后淘汰
  }
};
```

**文件**: `src/ray/common/protobuf_utils.cc:332-339`

```cpp
bool IsActorTask(const rpc::TaskEvents &task_event) {
  if (!task_event.has_task_info()) {
    return false;
  }
  const auto &task_info = task_event.task_info();
  return task_info.type() == rpc::TaskType::ACTOR_TASK ||
         task_info.type() == rpc::TaskType::ACTOR_CREATION_TASK;
}
```

**GCS 淘汰顺序与 Actor Task 关系**：

| 优先级 | 状态 | 淘汰顺序 | Actor task 影响 |
|--------|------|----------|----------------|
| 0 | FINISHED（任何类型） | 最先被淘汰 | Actor task FINISHED 后最先被清除 |
| 1 | Actor task（RUNNING 状态） | 其次 | RUNNING 的 actor task 比 normal task 更早被淘汰 |
| 2 | 其他（含 PENDING 的 normal task） | 最后 | normal task 得到最多保护 |

Actor task 无论 RUNNING 还是 FINISHED，都比 normal task 更容易被从 GCS 清除。

### 8.2 report_interval=10s 导致短生命周期 task 从未可见

如果 actor method 执行时间 < 10s：

```
T=0s:   Actor 提交 task → PENDING_ARGS_AVAIL (进入 Owner buffer)
T=0.1s: task 开始执行   → RUNNING (进入 Executor buffer)
T=2s:   task 完成       → FINISHED (进入 Owner buffer)
T=10s:  Owner flush     → PENDING_ARGS + FINISHED 一起发到 GCS
T=10s:  Executor flush  → RUNNING 发到 GCS
T=10s+: GCS 收到 → MergeFrom 合并 → task 状态直接是 FINISHED
        → 作为 Priority 0 (FINISHED)，马上成为淘汰候选
```

### 8.3 Worker Buffer 溢出丢失

如果 actor 持续高频提交 task，Owner (Driver) 的 buffer 积累速度远超 drain 速率 (1K/s)：
- Buffer 满后 FIFO 淘汰最老事件
- 早期提交的 task 的 PENDING_ARGS_AVAIL 事件被淘汰
- 该 task attempt 被标记为 dropped
- 后续的 RUNNING、FINISHED 事件**全部被丢弃**
- 结果：这些 task 永远不会出现在 GCS 和 dashboard 上

### 8.4 Task Event 报告的开关检查

**文件**: `src/ray/core_worker/task_event_buffer.cc:420-449`

```cpp
bool TaskEventBufferImpl::RecordTaskStatusEventIfNeeded(
    const TaskID &task_id,
    const JobID &job_id,
    int32_t attempt_number,
    const TaskSpecification &spec,
    rpc::TaskStatus status,
    bool include_task_info,
    std::optional<const TaskStatusEvent::TaskStateUpdate> state_update) {
  if (!Enabled()) {          // (1) TaskEventBuffer 必须已启动
    return false;
  }
  if (!spec.EnableTaskEvents()) {   // (2) 每个 task 的 enable_task_events 标志
    return false;
  }
  // ... 创建并添加事件
}
```

`enable_task_events` 默认为 `true`：

**文件**: `src/ray/common/constants.h:20`

```cpp
constexpr bool kDefaultTaskEventEnabled = true;
```

**文件**: `python/ray/_common/ray_option_utils.py:159`

```python
"enable_task_events": Option(bool, default_value=True),
```

但 **Ray Serve 默认禁用**：

**文件**: `python/ray/serve/_private/constants.py:466`

```python
RAY_SERVE_ENABLE_TASK_EVENTS = get_env_bool("RAY_SERVE_ENABLE_TASK_EVENTS", "0")
```

### 8.5 Actor Task 不可见原因总结

| 原因 | 影响 | 可能性 |
|------|------|--------|
| GCS 优先淘汰 Actor Task (Priority 1) | RUNNING actor task 比 normal task 更早被淘汰 | **高** |
| Actor task FINISHED 后进入 Priority 0 | 最先被淘汰，短暂可见后消失 | **高** |
| report_interval=10s + 短生命周期 task | 事件到达 GCS 时已是 FINISHED，直接进淘汰队列 | **高** |
| Driver Buffer FIFO 溢出 | task attempt 被 drop，后续所有事件丢弃 | **高**（大规模作业） |
| `enable_task_events=False` | 事件完全不产生 | 低（需检查配置） |

---

## 九、关键配置参数

| 参数 | 默认值 | 含义 | 建议值（大规模作业） |
|------|--------|------|-------------------|
| `task_events_report_interval_ms` | 1,000 | Worker flush 间隔(ms) | 1,000 (保持默认) |
| `task_events_max_num_task_in_gcs` | 100,000 | GCS 最多存储的 task event 数量 | 1,000,000 |
| `task_events_max_num_status_events_buffer_on_worker` | 100,000 | Worker 侧 buffer 容量 | 500,000 |
| `task_events_send_batch_size` | 10,000 | 每次 flush 最大事件数 | 保持默认 |
| `task_events_max_dropped_task_attempts_tracked_per_job_in_gcs` | 1,000,000 | GCS 记录的已淘汰 task attempt 上限 | 5,000,000 |

---

## 十、核心文件索引

| 功能 | 文件 | 关键行 |
|------|------|--------|
| Dashboard summarize 路由 | `python/ray/dashboard/modules/state/state_head.py` | 300-304 |
| 请求参数解析 | `python/ray/dashboard/state_api_utils.py` | 63-73, 104-108 |
| summarize 聚合逻辑 | `python/ray/dashboard/state_aggregator.py` | 569-618 |
| list_tasks → GCS RPC | `python/ray/dashboard/state_aggregator.py` | 298-356 |
| gRPC GetTaskEvents 调用 | `python/ray/util/state/state_manager.py` | 232-294 |
| GCS HandleGetTaskEvents | `src/ray/gcs/gcs_task_manager.cc` | 426-621 |
| Worker Buffer 存储 | `src/ray/core_worker/task_event_buffer.cc` | 1009-1054 |
| Worker Buffer flush | `src/ray/core_worker/task_event_buffer.cc` | 912-979 |
| 背压检查 | `src/ray/core_worker/task_event_buffer.cc` | 919-938 |
| 从 buffer 取事件 | `src/ray/core_worker/task_event_buffer.cc` | 589-646 |
| flush 时按 TaskAttempt 聚合 | `src/ray/core_worker/task_event_buffer.cc` | 729-784 |
| 状态时间戳写入 state_ts_ns | `src/ray/common/protobuf_utils.cc` | 351-359 |
| GCS 接收并合并 | `src/ray/gcs/gcs_task_manager.cc` | 167-207 |
| GCS 存储与淘汰 | `src/ray/gcs/gcs_task_manager.cc` | 332-389 |
| GCS 淘汰优先级策略 | `src/ray/gcs/gcs_task_manager.h` | 71-86 |
| GCS 接收 Worker drop 通知 | `src/ray/gcs/gcs_task_manager.cc` | 623-647 |
| Owner 报告 PENDING_ARGS | `src/ray/core_worker/task_manager.cc` | 343-349 |
| Owner 报告 PENDING_NODE | `src/ray/core_worker/task_manager.cc` | 1672-1683 |
| Owner 报告 FINISHED | `src/ray/core_worker/task_manager.cc` | 1053 |
| Executor 报告 RUNNING | `src/ray/core_worker/core_worker.cc` | 2855-2874 |
| Task event 开关检查 | `src/ray/core_worker/task_event_buffer.cc` | 420-449 |
| IsActorTask 判断 | `src/ray/common/protobuf_utils.cc` | 332-339 |
| GCS 标记 Worker 死亡 task | `src/ray/gcs/gcs_task_manager.cc` | 729-748 |
| GCS 标记 Job 结束 task | `src/ray/gcs/gcs_task_manager.cc` | 750-771 |
| dropped_task_attempts GC | `src/ray/gcs/gcs_task_manager.cc` | 773-809 |
| Ray Data StreamingExecutor | `python/ray/data/_internal/execution/streaming_executor.py` | 75, 134, 569-621 |
| TaskPoolMapOperator .remote() | `python/ray/data/_internal/execution/operators/task_pool_map_operator.py` | 108-143 |
| ActorPoolMapOperator .remote() | `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py` | 381-390 |
| PhysicalOperator 说明 | `python/ray/data/_internal/execution/interfaces/physical_operator.py` | 399-400 |

---

## 十一、Worker 上报周期与 API 查询数据一致性分析

### 11.1 上报机制概述

Worker 进程通过 `TaskEventBuffer` 缓冲 task 状态事件，定时 flush 到 GCS。Dashboard 的 `/api/v0/tasks` 和 `/api/v0/tasks/summarize` 始终从 GCS 读取数据，**不直接查询 Worker**。

### 11.2 上报周期与配置

**文件**: `src/ray/common/ray_config_def.h:456-462`

```cpp
/// The interval duration for which task state events will be reported to GCS.
/// The reported data should only be used for observability.
/// Setting the value to 0 disables the task event recording and reporting.
RAY_CONFIG(int64_t, task_events_report_interval_ms, 1000)
```

默认 **1000ms（1秒）**。每个 Worker 进程独立运行一个 `PeriodicalRunner`，互不同步。

**定时 flush 注册** — `src/ray/core_worker/task_event_buffer.cc:488-519`：

```cpp
void TaskEventBufferImpl::Start(bool send_task_events_to_gcs, bool auto_flush) {
  // ...
  auto report_interval_ms = RayConfig::instance().task_events_report_interval_ms();
  RAY_CHECK(report_interval_ms > 0)
      << "RAY_task_events_report_interval_ms should be > 0 to use TaskEventBuffer.";

  // 设置 circular buffer 容量
  status_events_.set_capacity(
      RayConfig::instance().task_events_max_num_status_events_buffer_on_worker());

  // ...

  RAY_LOG(INFO) << "Reporting task events to GCS every " << report_interval_ms << "ms.";
  periodical_runner_->RunFnPeriodically(
      [this] { FlushEvents(/*forced= */ false); },
      report_interval_ms,
      "CoreWorker.deadline_timer.flush_task_events");
}
```

### 11.3 完整配置参数表

**文件**: `src/ray/common/ray_config_def.h:456-489`

| 配置参数 | 默认值 | 含义 |
|---------|--------|------|
| `task_events_report_interval_ms` | 1,000 | Worker flush 间隔(ms)，设为 0 禁用 |
| `task_events_max_num_status_events_buffer_on_worker` | 100,000 | Worker 侧 circular buffer 容量，FIFO 淘汰 |
| `task_events_send_batch_size` | 10,000 | 每次 flush 最大发送事件数 |
| `task_events_max_num_task_in_gcs` | 100,000 | GCS 最多存储的 task event 数量，超出时 GC 淘汰 |
| `task_events_max_dropped_task_attempts_tracked_per_job_in_gcs` | 1,000,000 | GCS 记录的已 drop task attempt 上限 |
| `enable_core_worker_task_event_to_gcs` | true | 是否启用 Worker→GCS 上报 |
| `enable_core_worker_ray_event_to_aggregator` | false | 是否启用 Worker→Event Aggregator 路径（默认关闭） |

### 11.4 完整数据链路

```
┌─────────────────────────────────────────────────────────────────────┐
│ Worker 进程 (CoreWorker)                                             │
│                                                                      │
│  TaskManager / TaskExecutionQueue                                    │
│    → RecordTaskStatusEventIfNeeded()                                  │
│    → 写入 TaskEventBuffer.status_events_ (circular_buffer)           │
│                                                                      │
│  PeriodicalRunner (每 1000ms)                                        │
│    → FlushEvents(forced=false)                                       │
│      ├─ 检查背压：gcs_grpc_in_progress_ > 0 → 跳过                   │
│      ├─ 从 buffer 取事件 (batch_size=10,000)                        │
│      ├─ 按 TaskAttempt 聚合 (protobuf MergeFrom)                    │
│      └─ SendTaskEventsToGCS() → gRPC AddTaskEventData                │
│                                                                      │
└──────────────────────────────────┬──────────────────────────────────┘
                                   │ gRPC
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│ GCS (GcsTaskManager)                                                 │
│                                                                      │
│  HandleAddTaskEventData()                                            │
│    → RecordTaskEventData()                                           │
│    → GcsTaskManagerStorage.AddOrReplaceTaskEvent()                  │
│    → 存入内存 task_events_list_ (上限 100,000)                      │
│                                                                      │
│  HandleGetTaskEvents()  ← Dashboard 查询入口                         │
│    → 遍历 task_events_list_                                          │
│    → 统计 total_state_counts                                         │
│    → 设置 num_status_task_events_dropped                            │
│    → 返回 events_by_task + total_state_counts                        │
│                                                                      │
└──────────────────────────────────┬──────────────────────────────────┘
                                   │ gRPC
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Dashboard Server (Python)                                            │
│                                                                      │
│  /api/v0/tasks          → list_tasks()   → GetTaskEvents → GCS     │
│  /api/v0/tasks/summarize → summarize_tasks() → list_tasks() → GCS  │
│                                                                      │
│  state_aggregator.py:list_tasks()                                   │
│    → reply = self._client.get_all_task_info(filters=...)             │
│    → result = [protobuf_to_task_state_dict(msg) for msg in           │
│               reply.events_by_task]                                  │
│    → num_total = len(result) + reply.num_status_task_events_dropped  │
│    → 返回 ListApiResponse(result, total, total_state_counts, ...)    │
│                                                                      │
│  state_aggregator.py:summarize_tasks()                              │
│    → 调用 list_tasks(limit=RAY_MAX_LIMIT_FROM_API_SERVER)            │
│    → 按 func_name 或 lineage 聚合                                    │
│    → 检查数据丢失，附加 warning                                       │
│    → 返回 SummaryApiResponse                                         │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### 11.5 偏差来源分析

#### 11.5.1 时间窗口偏差（最多 1 秒 + 网络延迟）

每个 Worker 每 1 秒 flush 一次，各 Worker **独立计时、互不同步**。查询时可能有些 Worker 刚 flush 完、有些还没到下一次 flush。状态变更最多有 1 秒延迟才到达 GCS。

```
t=0.0s: Worker A 状态 RUNNING → FINISHED（写入本地 buffer）
t=0.3s: 查询 /api/v0/tasks → GCS 中还是 RUNNING（Worker A 还没 flush）
t=1.0s: Worker A flush → GCS 更新为 FINISHED
t=1.2s: 再次查询 → GCS 中是 FINISHED
```

#### 11.5.2 背压导致的更长延迟

**文件**: `src/ray/core_worker/task_event_buffer.cc:912-938`

```cpp
void TaskEventBufferImpl::FlushEvents(bool forced) {
  if (!enabled_ && !stopping_) {
    return;
  }

  // 背压检查：上一次 gRPC 还没返回 → 跳过本次 flush
  if ((gcs_grpc_in_progress_.load() > 0 ||
       event_aggregator_grpc_in_progress_.load() > 0) &&
      !forced) {
    RAY_LOG_EVERY_N_OR_DEBUG(WARNING, 100)
        << "GCS or the event aggregator hasn't replied to the previous flush events "
           "call (likely overloaded). "
           "Skipping reporting task state events and retry later."
        << "[gcs_grpc_in_progress=" << gcs_grpc_in_progress_.load() << "]"
        << "[event_aggregator_grpc_in_progress="
        << event_aggregator_grpc_in_progress_.load() << "]"
        << "[cur_status_events_size="
        << stats_counter_.Get(TaskEventBufferCounter::kNumTaskStatusEventsStored)
        << "][cur_profile_events_size="
        << stats_counter_.Get(TaskEventBufferCounter::kNumTaskProfileEventsStored) << "]";
    return;  // ← 直接返回，不发送
  }

  // 从 buffer 取事件
  std::vector<std::shared_ptr<TaskEvent>> status_events_to_send;
  // ...
  GetTaskStatusEventsToSend(&status_events_to_send, ...);

  // 聚合并发送
  TaskEventBuffer::TaskEventDataToSend data = CreateDataToSend(...);
  if (send_task_events_to_gcs_enabled_ && has_gcs_payload) {
    SendTaskEventsToGCS(std::move(data.task_event_data));
  }
}
```

GCS 过载时 Worker 跳过 flush，延迟可能远超 1 秒。`forced=true` 时跳过背压检查（仅用于 shutdown 时的最终 flush）。

#### 11.5.3 Worker 缓冲区溢出（FIFO 丢弃）

**文件**: `src/ray/core_worker/task_event_buffer.h`

Worker 侧 `status_events_` 是 `boost::circular_buffer`，容量默认 100,000。溢出时最旧事件被 FIFO 淘汰，对应 task attempt 加入 `dropped_task_attempts_unreported_`，下次 flush 时上报给 GCS。

#### 11.5.4 GCS 端淘汰

**文件**: `src/ray/gcs/gcs_task_manager.cc:332-389`

GCS 存储上限默认 100,000 个 task。超出时按优先级淘汰：
- Priority 0: 已 FINISHED 的 task（最先淘汰）
- Priority 1: Actor task（次之）
- Priority 2: 其他（含 PENDING normal task，最后淘汰）

被淘汰的 task 从 GCS 存储中消失，Dashboard 查询不到。

#### 11.5.5 两个 API 独立查询导致不一致

`/api/v0/tasks` 和 `/api/v0/tasks/summarize` 是两次独立的 HTTP 请求，各自触发一次 GCS 查询。两次查询之间可能有 Worker 完成了新的 flush：

```
t=0.0s: Worker A 状态 RUNNING → FINISHED
t=0.3s: /api/v0/tasks 查询 → GCS 中还是 RUNNING
t=0.5s: Worker A flush → GCS 更新为 FINISHED
t=0.7s: /api/v0/tasks/summarize 查询 → GCS 中是 FINISHED

→ Task Table 显示 RUNNING，Progress Bar 显示 FINISHED，产生不一致
```

### 11.6 数据丢失追踪机制

系统追踪并暴露数据丢失信息，但**不保证数据完整**。

#### 11.6.1 Worker 侧 drop 上报

Worker 缓冲区溢出时，将 dropped task attempt 上报给 GCS：

**文件**: `src/ray/gcs/gcs_task_manager.cc:623-647`

```cpp
void GcsTaskManager::GcsTaskManagerStorage::RecordDataLossFromWorker(
    const rpc::TaskEventData &data) {
  for (const auto &dropped_attempt : data.dropped_task_attempts()) {
    const auto task_id = TaskID::FromBinary(dropped_attempt.task_id());
    auto attempt_number = dropped_attempt.attempt_number();
    auto job_id = task_id.JobId();
    job_task_summary_[job_id].RecordTaskAttemptDropped(
        std::make_pair(task_id, attempt_number));
    stats_counter_.Increment(kTotalNumTaskAttemptsDropped);

    // 移除该 task attempt 的所有已有事件
    // （数据丢失以 task attempt 粒度为单位，保证不出现部分数据）
    const auto &loc_iter = primary_index_.find(std::make_pair(task_id, attempt_number));
    if (loc_iter != primary_index_.end()) {
      RemoveTaskAttempt(loc_iter->second);
    }
  }
  // ...
}
```

#### 11.6.2 GCS 查询响应中的 drop 信息

**文件**: `src/ray/gcs/gcs_task_manager.cc:444-620`

GCS 在 `HandleGetTaskEvents` 响应中设置 `num_status_task_events_dropped`：

```cpp
void GcsTaskManager::HandleGetTaskEvents(rpc::GetTaskEventsRequest request,
                                         rpc::GetTaskEventsReply *reply,
                                         rpc::SendReplyCallback send_reply_callback) {
  // ...
  if (job_ids.size() == 1) {
    const JobID &job_id = *job_ids.begin();
    task_events = task_event_storage_->GetTaskEvents(job_id);

    // 填入 per-job 数据丢失信息
    if (task_event_storage_->HasJob(job_id)) {
      const auto &job_summary = task_event_storage_->GetJobTaskSummary(job_id);
      reply->set_num_profile_task_events_dropped(job_summary.NumProfileEventsDropped());
      reply->set_num_status_task_events_dropped(job_summary.NumTaskAttemptsDropped());
    }
  } else {
    // 全局数据丢失信息
    reply->set_num_profile_task_events_dropped(
        task_event_storage_->NumProfileEventsDropped());
    reply->set_num_status_task_events_dropped(
        task_event_storage_->NumTaskAttemptsDropped());
  }

  // 遍历所有 task events，统计 total_state_counts
  for (auto &task_event : *task_events | boost::adaptors::reversed) {
    if (task_event.has_state_updates()) {
      auto latest_state = GetLatestTaskStatus(task_event);
      total_state_counts[ray::rpc::TaskStatus_Name(latest_state)]++;
    }
    // ... 过滤、截断 ...
  }

  reply->set_num_total_stored(task_events->size());
  reply->set_num_truncated(num_limit_truncated);
  reply->set_num_filtered_on_gcs(num_filtered);
  for (const auto &[state_name, state_count] : total_state_counts) {
    (*reply->mutable_total_state_counts())[state_name] = state_count;
  }
}
```

#### 11.6.3 Dashboard 侧 drop 信息展示

**文件**: `python/ray/dashboard/state_aggregator.py:298-356`

`list_tasks()` 将 drop 数量计入 `total`：

```python
async def list_tasks(self, *, option: ListApiOptions) -> ListApiResponse:
    reply = await self._client.get_all_task_info(
        timeout=option.timeout,
        filters=option.filters,
        exclude_driver=option.exclude_driver,
    )

    def transform(reply) -> ListApiResponse:
        result = [
            protobuf_to_task_state_dict(message) for message in reply.events_by_task
        ]
        num_after_truncation = len(result)
        # 关键：total = 返回的 task 数 + 被 drop 的 task 数
        num_total = len(result) + reply.num_status_task_events_dropped

        result = do_filter(result, option.filters, TaskState, option.detail)
        num_filtered = len(result)
        result.sort(key=lambda entry: entry["task_id"])
        result = list(islice(result, option.limit))

        return ListApiResponse(
            result=result,
            total=num_total,
            num_after_truncation=num_after_truncation,
            num_filtered=num_filtered,
            total_state_counts=dict(reply.total_state_counts)
            if reply.total_state_counts
            else None,
            num_total_stored=reply.num_total_stored,
            num_filtered_on_gcs=reply.num_filtered_on_gcs,
        )
```

**文件**: `python/ray/dashboard/state_aggregator.py:569-618`

`summarize_tasks()` 检查数据丢失并附加 warning：

```python
async def summarize_tasks(self, option: SummaryApiOptions) -> SummaryApiResponse:
    summary_by = option.summary_by or "func_name"

    # summarize 使用最大 limit 尽量减少数据丢失
    result = await self.list_tasks(
        option=ListApiOptions(
            timeout=option.timeout,
            limit=RAY_MAX_LIMIT_FROM_API_SERVER,
            filters=option.filters,
            detail=summary_by == "lineage",
        )
    )

    if summary_by == "func_name":
        summary_results = TaskSummaries.to_summary_by_func_name(tasks=result.result)
    else:
        actors = await self.list_actors(...)
        summary_results = TaskSummaries.to_summary_by_lineage(
            tasks=result.result, actors=actors.result
        )
    summary = StateSummary(node_id_to_summary={"cluster": summary_results})
    warnings = result.warnings

    # 检查数据丢失
    if (
        summary_results.total_actor_scheduled
        + summary_results.total_actor_tasks
        + summary_results.total_tasks
        < result.num_filtered
    ):
        warnings = warnings or []
        warnings.append(
            "There is missing data in this aggregation. "
            "Possibly due to task data being evicted to preserve memory."
        )

    return SummaryApiResponse(
        total=result.total,
        result=summary,
        warnings=warnings,
        total_state_counts=result.total_state_counts,
        num_total_stored=result.num_total_stored,
        num_filtered_on_gcs=result.num_filtered_on_gcs,
    )
```

#### 11.6.4 `total_state_counts` 的时序特性

`total_state_counts` 是 GCS 在**同一次查询**中遍历全部存储条目统计的，统计的是 GCS 当前存储中的数据。`total_state_counts` 和 `events_by_task` 来自同一次 GCS 遍历，两者之间不会有时序不一致。但这些数据本身可能有最多 1 秒（或更长，如果有背压）的上报延迟。

### 11.7 官方声明：仅用于可观测性

**文件**: `src/ray/common/ray_config_def.h:456-458`

```cpp
/// The interval duration for which task state events will be reported to GCS.
/// The reported data should only be used for observability.
/// Setting the value to 0 disables the task event recording and reporting.
RAY_CONFIG(int64_t, task_events_report_interval_ms, 1000)
```

**文件**: `src/ray/common/ray_config_def.h:463-468`

```cpp
/// The number of task attempts being dropped per job tracked at GCS. When GCS is forced
/// to stop tracking some task attempts that are lost, this will incur potential partial
/// data loss for a single task attempt (e.g. some task events were dropped, but some were
/// tracked). When this happens, users should be cautious of inconsistency in the task
/// events data.
RAY_CONFIG(int64_t,
           task_events_max_dropped_task_attempts_tracked_per_job_in_gcs,
           1 * 1000 * 1000)
```

**文件**: `src/ray/core_worker/task_event_buffer.h`

> Task events will be lost in the below cases:
> 1. If any of the gRPC call failed, the task events will be dropped and warnings logged.
> 2. More than `RAY_task_events_max_num_status_events_buffer_on_worker` (default: 100,000) tasks have been stored in the buffer, any new task events will be dropped.

> No overloading of GCS: If GCS failed to respond quickly enough to the previous report, reporting of events to GCS will be delayed until GCS replies the gRPC in future intervals.

### 11.8 偏差总结

| 偏差来源 | 延迟/影响 | 代码位置 |
|---------|----------|---------|
| 上报周期延迟 | 最多 ~1s + gRPC 延迟 | `task_event_buffer.cc:515` `RunFnPeriodically` |
| GCS 背压跳过 | GCS 未响应时跳过本次 flush，延迟可能远超 1s | `task_event_buffer.cc:919-938` |
| Worker 缓冲区溢出 | 超过 100,000 事件时 FIFO 丢弃最旧事件 | `task_event_buffer.h` `status_events_` |
| GCS 端淘汰 | 超过 100,000 task 时 GC 淘汰旧事件 | `gcs_task_manager.cc:332-389` |
| 部分数据丢失 | 某 task attempt 事件部分被丢、部分保留 | `gcs_task_manager.cc:623-647` |
| 两次 API 查询不同步 | `/api/v0/tasks` 和 `/api/v0/tasks/summarize` 独立查询，可能返回不同结果 | `state_aggregator.py:298,569` |

### 11.9 缓解方法

| 方法 | 效果 | 代价 |
|------|------|------|
| 减小 `RAY_task_events_report_interval_ms`（如 500ms） | 减少时间窗口偏差 | 增加 gRPC 带宽和 GCS CPU |
| 增大 `RAY_task_events_max_num_task_in_gcs` | 减少 GCS 淘汰导致的偏差 | 增加 GCS 内存 |
| 增大 `RAY_task_events_max_num_status_events_buffer_on_worker` | 减少 Worker 端 FIFO 丢弃 | 增加 Worker 内存 |
| 同一请求中同时返回 `events_by_task` 和 `total_state_counts` | 消除两个 API 间的时序不一致 | 已实现（GCS 单次遍历同时返回） |
| 对同一 API 多次查询取最新 | 减少单次查询的时间偏差 | 增加查询负载 |

---

## 十二、相关文档

- [Task Event 淘汰 + RequestWorkerLease gRPC 卡死分析](./ray-task-event-eviction-and-lease-stuck-analysis.md) — Worker buffer FIFO 淘汰、GCS 优先级淘汰、gRPC 永久卡死的根因分析
- [Ray GCS FD 耗尽排查](./ray-gcs-fd-exhaustion-troubleshooting.md) — 另一个导致调度阻塞的问题
