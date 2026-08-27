# GCS Ghost RUNNING Task 根因分析与修复方案

**时间**: 2026-08-25
**问题**: Worker 死亡后，GCS/Dashboard 中部分 task 持续显示为 RUNNING（ghost RUNNING），持续 10+ 小时不恢复
**关键特征**: Ghost task **没有 node_id 和 worker_id**

---

## 一、问题现象

- Worker 死亡（被 K8s SIGTERM 抢占）后，GCS 中部分 task 永久显示为 RUNNING
- 这些 ghost task 的 `state_updates` 中 **缺少 `worker_id` 和 `node_id`**
- 节点本身没有丢失（worker 死亡但节点仍存活）
- Ghost 持续 10+ 小时，不会自行恢复

---

## 二、根因分析

### 2.1 背景：Task 状态事件的两个上报源

Ray 的 task 状态由两个独立的进程上报到 GCS：

| 上报源 | 上报的状态 | 包含的字段 |
|--------|-----------|------------|
| **Driver 端** | PENDING_ARGS_AVAIL → PENDING_NODE_ASSIGNMENT → **SUBMITTED_TO_WORKER** → FAILED/FINISHED | `worker_id`, `node_id`, `error_info` |
| **Worker 端** | **RUNNING** | `pid`（和可选的 `actor_repr_name`） |

**关键点**：`worker_id` 和 `node_id` **只在 driver 端的 `SUBMITTED_TO_WORKER` 事件中设置**。Worker 端上报的 RUNNING 事件 **不包含** `worker_id` 和 `node_id`。

### 2.2 GCS 的 MarkTasksFailedOnWorkerDead 机制

当 worker 死亡时，GCS 通过 `MarkTasksFailedOnWorkerDead` 标记该 worker 上的 task 为 FAILED：

```cpp
// src/ray/gcs/gcs_task_manager.cc:107-128
void MarkTasksFailedOnWorkerDead(const WorkerID &worker_id, ...) {
    auto task_attempts_itr = worker_index_.find(worker_id);
    if (task_attempts_itr == worker_index_.end()) {
        return;  // 找不到 → 不标记 FAILED
    }
    for (const auto &task_locator : task_attempts_itr->second) {
        MarkTaskAttemptFailedIfNeeded(task_locator, ...);
    }
}
```

`worker_index_` 在 `UpdateIndex` 中建立：

```cpp
// src/ray/gcs/gcs_task_manager.cc:249
if (!worker_id.IsNil()) {
    worker_index_[worker_id].insert(loc);
}
```

`GetWorkerID` 从 task event 的 `state_updates.worker_id` 获取：

```cpp
// src/ray/common/protobuf_utils.cc:307-312
WorkerID GetWorkerID(const rpc::TaskEvents &task_event) {
    if (task_event.has_state_updates() && task_event.state_updates().has_worker_id()) {
        return WorkerID::FromBinary(task_event.state_updates().worker_id());
    }
    return WorkerID::Nil();  // 未设置 → Nil → 不加入 worker_index_
}
```

### 2.3 Ghost 产生的完整时序

```
1. Driver 提交 task → AddPendingTask → PENDING_ARGS_AVAIL (上报 GCS)

2. Driver 端解析依赖 → MarkDependenciesResolved → PENDING_NODE_ASSIGNMENT (上报 GCS)

3. Driver 端发送 task 到 worker：
   - Actor: PushActorTask → MarkTaskWaitingForExecution
     → SUBMITTED_TO_WORKER (含 worker_id, node_id) → 加入 driver 的 event buffer
   - Normal: PushNormalTask → MarkTaskWaitingForExecution
     → SUBMITTED_TO_WORKER (含 worker_id, node_id) → 加入 driver 的 event buffer

4. Worker 端收到 task 并执行：
   - ExecuteTask → RecordTaskStatusEventIfNeeded(RUNNING)
   → RUNNING 事件 (含 pid，不含 worker_id/node_id) → 加入 worker 的 event buffer

5. ⚠️ Driver 端 event buffer 溢出（大量 task 并发 + tidal 抢占导致重试风暴）
   → circular buffer 淘汰旧事件
   → SUBMITTED_TO_WORKER 事件被淘汰
   → task attempt 加入 dropped_task_attempts_unreported_
   → 后续该 attempt 的所有事件也被丢弃（包括 FAILED）

6. Worker 端 event buffer 没有溢出（或先 flush）
   → RUNNING 事件成功上报到 GCS

7. GCS 收到 RUNNING 事件：
   → primary_index_ 中无此 (task_id, attempt_number)（因为 SUBMITTED_TO_WORKER 丢失）
   → AddNewTaskEvent → 创建新 TaskEventLocator
   → state_updates 只有 pid，没有 worker_id/node_id
   → GetWorkerID 返回 Nil → 不加入 worker_index_
   → GetNodeID 返回 Nil → 不加入 node_index_（也不存在）

8. Worker 被 kill (SIGTERM)

9. GCS 收到 worker dead 通知 → OnWorkerDead → MarkTasksFailedOnWorkerDead
   → worker_index_.find(worker_id) → 找不到！（因为 GCS 从未收到 SUBMITTED_TO_WORKER）
   → 不标记 FAILED → task 在 GCS 中保持 RUNNING

10. Driver 端 flush → 报告 dropped_task_attempts
    → GCS 收到 → RecordDataLossFromWorker → RemoveTaskAttempt
    → 从 GCS 中删除该 task attempt
    → 但如果 driver 端 flush 延迟、或 gRPC 发送失败
    → GCS 中 task 永久保持 RUNNING → ghost RUNNING
```

### 2.4 Driver 端 Event Buffer 溢出机制

```cpp
// src/ray/core_worker/task_event_buffer.cc:1081-1091
if (status_events_.full()) {
    const auto &to_evict = status_events_.front();
    auto inserted = dropped_task_attempts_unreported_.insert(to_evict->GetTaskAttempt());
    // ...
}
status_events_.push_back(status_event_shared_ptr);
```

当 `status_events_`（circular buffer）满时，最早的事件被淘汰。该 task attempt 加入 `dropped_task_attempts_unreported_` 后，**后续所有该 attempt 的事件也被丢弃**：

```cpp
// src/ray/core_worker/task_event_buffer.cc:1071-1074
if (dropped_task_attempts_unreported_.count(
        status_event_shared_ptr->GetTaskAttempt()) != 0u) {
    // This task attempt has been dropped before, so we drop this event.
    return;
}
```

这意味着：如果 `SUBMITTED_TO_WORKER` 被淘汰，后续的 `FAILED` 事件也会被丢弃。Driver 端通过 `dropped_task_attempts` 字段通知 GCS 哪些 task attempt 的事件丢失了，但 **通知是延迟的**（只在下次 flush 时发送）。

### 2.5 GCS 收到 dropped_task_attempts 后的处理

```cpp
// src/ray/gcs/gcs_task_manager.cc:625-636
void RecordDataLossFromWorker(const rpc::TaskEventData &data) {
    for (const auto &dropped_attempt : data.dropped_task_attempts()) {
        const auto &loc_iter = primary_index_.find(std::make_pair(task_id, attempt_number));
        if (loc_iter != primary_index_.end()) {
            RemoveTaskAttempt(loc_iter->second);
            // 从所有索引中移除，包括 worker_index_
        }
    }
}
```

GCS 收到后删除该 task attempt。**如果删除时 task 正在 RUNNING 状态，删除后 GCS 不再有该 task → Dashboard 不显示 → 不产生 ghost。**

**但如果 driver 端 flush 延迟**（大量 worker dead 处理导致 driver 端 CPU 竞争），`dropped_task_attempts` 可能迟迟不到 GCS → GCS 中 task 保持 RUNNING。

### 2.6 RUNNING 事件不含 worker_id 的原因

```cpp
// src/ray/core_worker/core_worker.cc:2819-2824
worker::TaskStatusEvent::TaskStateUpdate update;
{
    absl::MutexLock lock(&mutex_);
    update = (task_spec.IsActorTask() && !actor_repr_name_.empty())
                 ? worker::TaskStatusEvent::TaskStateUpdate(actor_repr_name_, pid_)
                 : worker::TaskStatusEvent::TaskStateUpdate(pid_);
}
```

`TaskStateUpdate` 的构造函数（`task_event_buffer.h:134,137`）：
```cpp
TaskStateUpdate(std::string actor_repr_name, uint32_t pid)
    : actor_repr_name_(std::move(actor_repr_name)), pid_(pid) {}
explicit TaskStateUpdate(uint32_t pid) : pid_(pid) {}
```

RUNNING 事件只包含 `pid`，不含 `worker_id`/`node_id`。

`ToRpcTaskEvents` 中有 CHECK 限制：
```cpp
// src/ray/core_worker/task_event_buffer.cc:129-133
if (state_update_->worker_id_.has_value()) {
    RAY_CHECK(task_status_ == rpc::TaskStatus::SUBMITTED_TO_WORKER)
        << "When task status changes to SUBMITTED_TO_WORKER, Worker ID should be "
           "included in the status update";
    dst_state_update->set_worker_id(state_update_->worker_id_->Binary());
}
```

**RUNNING 状态不允许设置 worker_id**——这是一个设计限制。

### 2.7 Protobuf MergeFrom 行为与事件到达顺序

GCS 使用 `MergeFrom` 合并同一 task attempt 的多个事件：

```cpp
// src/ray/gcs/gcs_task_manager.cc:180
existing_task.MergeFrom(task_events);
```

Protobuf3 MergeFrom 语义：
- 如果 source 有字段设置，覆盖 destination
- 如果 source 没有字段设置，destination 保留原值
- Map 字段：同 key 覆盖，不同 key 合并

**事件到达顺序无关——只要两方事件最终都到达 GCS，合并结果一致**：

**如果 SUBMITTED_TO_WORKER（含 worker_id）先到 GCS，RUNNING（含 pid）后到**：
- SUBMITTED_TO_WORKER 先到 → `AddNewTaskEvent` → 创建 TaskEventLocator（含 worker_id）→ `UpdateIndex` → `worker_index_[worker_id].insert(loc)`
- RUNNING 后到 → `UpdateExistingTaskAttempt` → `MergeFrom` → `worker_id` 保留（RUNNING 不含 worker_id → 不覆盖）→ `worker_index_` 已有此 task → 无问题

**如果 RUNNING 先到 GCS，SUBMITTED_TO_WORKER 后到**：
- RUNNING 先到 → `AddNewTaskEvent` → 创建 TaskEventLocator（无 worker_id）→ `GetWorkerID` 返回 Nil → **不加入 `worker_index_`**
- SUBMITTED_TO_WORKER 后到 → `UpdateExistingTaskAttempt` → `MergeFrom` → `worker_id` 被加入 → `UpdateIndex` 重新计算 → **加入 `worker_index_`** → 无问题
- 关键：`UpdateIndex` 在每次 MergeFrom 后都会被调用，会重新计算 `GetWorkerID`，如果此时 `worker_id` 非空则加入索引

**但如果 SUBMITTED_TO_WORKER 从未到达 GCS**：
- 只有 RUNNING 事件 → 无 `worker_id` → `worker_index_` 无此 task → ghost RUNNING
- **到不到是问题，先到后到不是问题**

### 2.7.1 MarkTasksFailedOnWorkerDead 是纯 GCS 侧逻辑

整个 `MarkTasksFailedOnWorkerDead` 机制不涉及 Driver 或 Worker 端的交互：

```
GCS 收到 worker dead 通知
  → gcs_server.cc: OnWorkerDead 回调
  → gcs_task_manager_->OnWorkerDead(worker_id, worker_data)
  → 延迟 gcs_mark_task_failed_on_worker_dead_delay_ms 后
  → MarkTasksFailedOnWorkerDead(worker_id)
  → worker_index_.find(worker_id) → 查找该 worker 上所有 task
  → 逐个 MarkTaskAttemptFailedIfNeeded
```

### 2.7.2 worker_id 复用：一个 worker 执行多个 task

一个 worker 进程在其生命周期内可以执行多个 task，因此 `worker_index_` 中一个 `worker_id` 对应一个 **set**（而非单个 task）：

- **Normal task worker**：一个 worker 进程顺序执行多个 task（执行完一个接下一个）
- **Actor worker**：一个 actor 进程处理多个 actor task（按消息顺序执行）

```cpp
// worker_index_ 的类型
std::unordered_map<WorkerID, std::unordered_set<std::shared_ptr<TaskEventLocator>>> worker_index_;
```

`MarkTasksFailedOnWorkerDead` 遍历 `worker_index_[worker_id]` 这个 set，将该 worker 上**所有未结束的 task** 全部标记 FAILED。

### 2.7.3 FAILED 和 RUNNING 分别在不同进程的 event buffer 中

FAILED 事件是在 **Driver 端** 的 event buffer 中上报的，RUNNING 事件是在 **Worker 端** 的 event buffer 中上报的：

| 事件 | 上报端 | 代码路径 |
|------|--------|---------|
| RUNNING | Worker 端 | `ExecuteTask` → `RecordTaskStatusEventIfNeeded(RUNNING)` → Worker 的 `task_event_buffer_` |
| SUBMITTED_TO_WORKER | Driver 端 | `MarkTaskWaitingForExecution` → `SetTaskStatus(SUBMITTED_TO_WORKER)` → Driver 的 `task_event_buffer_` |
| FAILED | Driver 端 | `CompletePendingTask` / `FailOrRetryPendingTask` → `SetTaskStatus(FAILED)` → `RecordTaskStatusEventIfNeeded(FAILED)` → Driver 的 `task_event_buffer_` |
| FINISHED | Driver 端 | `CompletePendingTask` → `SetTaskStatus(FINISHED)` → `RecordTaskStatusEventIfNeeded(FINISHED)` → Driver 的 `task_event_buffer_` |

**关键**：FAILED 和 RUNNING 不在同一个 buffer 中，它们只在 GCS 端通过 `MergeFrom` 合并。

### 2.7.4 淘汰后的 task attempt 永远不会变为 FAILED

当 Driver 端 event buffer 溢出，某个 task attempt 的 SUBMITTED_TO_WORKER 被淘汰后：

1. 该 attempt 被加入 `dropped_task_attempts_unreported_`
2. **该 attempt 后续所有事件也被丢弃**（包括 FAILED）
3. GCS 中只有 Worker 端上报的 RUNNING 事件（无 worker_id）
4. **没有任何后续状态更新能到达 GCS 中的这个 entry**
5. Worker dead → `MarkTasksFailedOnWorkerDead` 按 worker_id 查 → 找不到 → 不标记 FAILED
6. **唯一的清理路径**：Driver 的 `dropped_task_attempts` 通知到达 GCS → `RecordDataLossFromWorker` → `RemoveTaskAttempt` → 删除整个 entry

**所以 ghost RUNNING 不会"变为 FAILED"——它永远停在 RUNNING，直到被 dropped 通知删除。** 如果 dropped 通知也丢失（gRPC 失败不重试），就是永久 ghost

### 2.8 根因总结

| 层级 | 问题 | 影响 |
|------|------|------|
| **Worker 端** | RUNNING 事件不包含 worker_id/node_id | GCS 无法建立 worker_index_ |
| **Driver 端** | Event buffer 溢出导致 SUBMITTED_TO_WORKER 丢失 | GCS 永远收不到 worker_id |
| **GCS 端** | MarkTasksFailedOnWorkerDead 只按 worker_id 查找 | worker_index_ 缺失时无法标记 FAILED |
| **GCS 端** | MarkTaskAttemptFailedIfNeeded 不更新 GC priority | 标记 FAILED 后 task 仍在错误 priority 列表 |

**核心因果链**：Worker 端 RUNNING 事件不含 worker_id + Driver 端 SUBMITTED_TO_WORKER 可能丢失 → GCS 中 task 无 worker_id → worker dead 时无法标记 FAILED → ghost RUNNING

### 2.9 Driver 端 gRPC 发送失败不重试

```cpp
// src/ray/core_worker/task_event_buffer.cc - SendTaskEventsToGCS
auto on_complete = [...] (const Status &status) {
    if (!status.ok()) {
        RAY_LOG(WARNING) << "Failed to push task events...";
        // ⚠️ 没有重试！没有放回 buffer！数据直接丢失！
    }
    gcs_grpc_in_progress_.fetch_sub(1);
};
```

而且 flush 时如果上一次 gRPC 还没回来，直接跳过：

```cpp
if ((gcs_grpc_in_progress_.load() > 0 || ...) && !forced) {
    // 跳过本次 flush
    return;
}
```

**影响**：Driver 发送 `dropped_task_attempts` 通知到 GCS 时，如果 gRPC 失败（网络抖动、GCS 过载），数据**直接丢失，不会重试**。`dropped_task_attempts_unreported_` 中已取出的 attempt 不会放回——下次 flush 时从 set 中取出并删除了，即使发送失败，这些 attempt 也从 `unreported` 集合中移除了。

**后果**：如果 gRPC 发送失败（哪怕一次），dropped 通知就永久丢失 → GCS 中不完整的 RUNNING 事件永久保留 → ghost RUNNING。

### 2.10 dropped_task_attempts_unreported_ vs status_events_ 容量对比

| 数据结构 | 单条目内存 | 100,000 条总内存 | 有无上限 |
|---------|-----------|-----------------|---------|
| `status_events_`（circular buffer） | 含 task spec + state_updates，**~数 KB** | 数百 MB | 有上限（100,000） |
| `dropped_task_attempts_unreported_`（flat_hash_set） | `std::pair<TaskID, int32_t>`，**~24 bytes** | ~2.4 MB | **无上限** |

**为什么 `dropped_task_attempts_unreported_` 无上限**：单个 dropped entry 只占 24 bytes，远小于 status_events_ 中的完整事件（数 KB），差约两个数量级。所以在设计时认为不需要限制。

**但无上限本身也是风险**：在 tidal 抢占风暴场景下，大量 task attempt 被淘汰 → `dropped_task_attempts_unreported_` 可涨到几十万个 entry，累积几十 MB。每次 flush 最多发 10,000 个 dropped，积压时需要多次 flush 才能清完。

**但即使给 event buffer 无限容量也不能解决根本问题**：

1. 无上限 buffer → OOM 风险（driver 进程内存被吃光）
2. 即使 buffer 够大不溢出，Worker 端和 Driver 端是**独立 flush** 的，仍可能出现 RUNNING 先到 GCS、SUBMITTED_TO_WORKER 后到的窗口期（但 MergeFrom 会补上，见 2.7）
3. 真正的根因是 **RUNNING 事件不带 worker_id 这个设计缺陷**——无论 buffer 多大，如果 Driver 端事件因任何原因不到 GCS（不只是 buffer 溢出，还有 gRPC 失败等），GCS 中的 task 就永远无 worker_id

所以增大 buffer 只是降低概率，不消除根因。核心修复（RUNNING 带 worker_id）是从语义上彻底解决问题。

### 2.11 根因完整因果链

```
Worker 端 RUNNING 事件不含 worker_id（设计缺陷）
  +
Driver 端 SUBMITTED_TO_WORKER 可能因以下原因丢失不到达 GCS：
  - Event buffer 溢出淘汰（FIFO，最早事件先淘汰）
  - gRPC 发送失败不重试
  - flush 被跳过（上一次 gRPC 未完成）
  - Driver 端 CPU 竞争导致 flush 延迟
  ↓
GCS 中 task 只有 RUNNING 事件，无 worker_id
  ↓
worker_index_ 中无此 task（UpdateIndex 只在 worker_id 非空时 insert）
  ↓
Worker dead → MarkTasksFailedOnWorkerDead(worker_id) → worker_index_.find 找不到
  ↓
Task 不被标记 FAILED，永远保持 RUNNING
  ↓
唯一清理路径：dropped_task_attempts 通知 → 但也可能因 gRPC 失败而丢失
  ↓
永久 ghost RUNNING
```

---

## 三、修复方案

### 3.1 核心修复：Worker 端 RUNNING 事件携带 worker_id 和 node_id

**目的**：从根源解决——确保 GCS 收到 RUNNING 事件时就能建立 worker_id 索引，即使 driver 端的 SUBMITTED_TO_WORKER 事件丢失。

#### 3.1.1 `src/ray/core_worker/task_event_buffer.h`

`TaskStateUpdate` 新增构造函数：

```cpp
// 新增：RUNNING 事件携带 worker_id
TaskStateUpdate(const WorkerID &worker_id, uint32_t pid)
    : worker_id_(worker_id), pid_(pid) {}

// 新增：Actor task RUNNING 事件携带 worker_id + actor_repr_name
TaskStateUpdate(const WorkerID &worker_id, std::string actor_repr_name, uint32_t pid)
    : worker_id_(worker_id),
      actor_repr_name_(std::move(actor_repr_name)),
      pid_(pid) {}
```

#### 3.1.2 `src/ray/core_worker/task_event_buffer.cc`

`ToRpcTaskEvents` 放宽 CHECK，允许任何状态携带 worker_id：

```cpp
// 当前代码：
if (state_update_->worker_id_.has_value()) {
    RAY_CHECK(task_status_ == rpc::TaskStatus::SUBMITTED_TO_WORKER)
        << "...";
    dst_state_update->set_worker_id(state_update_->worker_id_->Binary());
}

// 修改为：
if (state_update_->worker_id_.has_value()) {
    dst_state_update->set_worker_id(state_update_->worker_id_->Binary());
}
```

同理放宽 node_id 的处理（`ToRpcTaskEvents` 和 `ToRpcTaskExportEvents` 中都有类似的 CHECK）。

#### 3.1.3 `src/ray/core_worker/core_worker.cc`

`ExecuteTask` 中上报 RUNNING 时使用新的 TaskStateUpdate：

```cpp
// 当前代码 (line 2819-2824)：
worker::TaskStatusEvent::TaskStateUpdate update;
{
    absl::MutexLock lock(&mutex_);
    update = (task_spec.IsActorTask() && !actor_repr_name_.empty())
                 ? worker::TaskStatusEvent::TaskStateUpdate(actor_repr_name_, pid_)
                 : worker::TaskStatusEvent::TaskStateUpdate(pid_);
}

// 修改为：
worker::TaskStatusEvent::TaskStateUpdate update;
{
    absl::MutexLock lock(&mutex_);
    if (task_spec.IsActorTask() && !actor_repr_name_.empty()) {
        update = worker::TaskStatusEvent::TaskStateUpdate(
            worker_context_->GetCurrentWorkerID(),
            actor_repr_name_,
            pid_);
    } else {
        update = worker::TaskStatusEvent::TaskStateUpdate(
            worker_context_->GetCurrentWorkerID(),
            pid_);
    }
}
```

**注意**：`node_id` 已在 `TaskStatusEvent` 构造函数中通过 `node_id_` 字段传入（当前 `ExecuteTask` 调用 `RecordTaskStatusEventIfNeeded` 时使用 `core_worker.node_id_`），所以 `node_id` 不需要额外处理。

**但是** `ToRpcTaskEvents` 中设置 `node_id` 也有 CHECK：

```cpp
if (state_update_->node_id_.has_value()) {
    RAY_CHECK(task_status_ == rpc::TaskStatus::SUBMITTED_TO_WORKER)
        << "When task status changes to SUBMITTED_TO_WORKER, the Node ID should be "
           "included in the status update";
    dst_state_update->set_node_id(state_update_->node_id_->Binary());
}
```

需要同样放宽此 CHECK。

### 3.2 辅助修复：GCS MarkTaskAttemptFailedIfNeeded 后更新 GC priority

**文件**：`src/ray/gcs/gcs_task_manager.cc`

当前 `MarkTaskAttemptFailedIfNeeded` 只修改 `state_ts_ns`，不移动 task event 到正确的 GC priority 列表。标记 FAILED 后，task 应从 priority 1/2 移到 priority 0（finished）。

修改 `MarkTaskAttemptFailedIfNeeded`，在设置 FAILED 后重新计算 GC priority：

```cpp
void MarkTaskAttemptFailedIfNeeded(...) {
    auto &task_events = locator->GetTaskEventsMutable();
    if (IsTaskTerminated(task_events)) {
        return;
    }
    auto state_updates = task_events.mutable_state_updates();
    (*state_updates->mutable_state_ts_ns())[ray::rpc::TaskStatus::FAILED] = failed_ts_ns;
    state_updates->mutable_error_info()->CopyFrom(error_info);

    // 新增：更新 GC priority 列表
    auto target_list_index = gc_policy_->GetTaskListPriority(task_events);
    auto cur_list_index = locator->GetCurrentListIndex();
    if (target_list_index != cur_list_index) {
        task_events_list_[target_list_index].push_front(std::move(task_events));
        task_events_list_[cur_list_index].erase(locator->GetCurrentListIterator());
        locator->SetCurrentList(target_list_index,
                                task_events_list_[target_list_index].begin());
    }
}
```

### 3.3 可选修复：GCS 添加 node_index_ + MarkTasksFailedOnNodeDead

作为额外防护层，在 worker_id 查找失败时提供按 node_id 的备选路径。

**文件**：
- `src/ray/gcs/gcs_task_manager.h` — 新增 `node_index_`、`MarkTasksFailedOnNodeDead`、`OnNodeDead`
- `src/ray/gcs/gcs_task_manager.cc` — 实现新方法，修改 `UpdateIndex`/`RemoveFromIndex`
- `src/ray/common/protobuf_utils.cc/h` — 新增 `GetNodeID`
- `src/ray/gcs/gcs_server.cc` — 节点死亡回调中调用 `OnNodeDead`

---

## 四、修复优先级

| 优先级 | 修改 | 覆盖场景 | 风险 |
|--------|------|---------|------|
| **P0** | 3.1 Worker RUNNING 携带 worker_id | 所有 ghost 场景（actor + normal task） | 低（只增加字段，不改逻辑） |
| **P1** | 3.2 GC priority 更新 | MarkTasksFailedOnWorkerDead 后 GC 正确淘汰 | 低（修复遗漏的列表移动） |
| **P2** | 3.3 node_index_ + MarkTasksFailedOnNodeDead | 节点死亡时的额外防护 | 中（新增索引结构） |

---

## 五、涉及文件

| 文件 | 修改 |
|------|------|
| `src/ray/core_worker/task_event_buffer.h` | TaskStateUpdate 新增含 worker_id 的构造函数 |
| `src/ray/core_worker/task_event_buffer.cc` | ToRpcTaskEvents 放宽 CHECK；ToRpcTaskExportEvents 同步修改 |
| `src/ray/core_worker/core_worker.cc` | ExecuteTask 中 RUNNING 使用新 TaskStateUpdate |
| `src/ray/gcs/gcs_task_manager.cc` | MarkTaskAttemptFailedIfNeeded 后更新 GC priority |
| `src/ray/gcs/gcs_task_manager.h` | (P2) node_index_ + MarkTasksFailedOnNodeDead |
| `src/ray/common/protobuf_utils.cc/h` | (P2) 新增 GetNodeID |
| `src/ray/gcs/gcs_server.cc` | (P2) 节点死亡回调 |

---

## 六、验证

### 6.1 单元测试

1. **TestRunningEventContainsWorkerId**：Worker 执行 task → 上报 RUNNING → 验证 protobuf 包含 `state_updates.worker_id`
2. **TestMarkTasksFailedOnWorkerDeadWithWorkerIdFromRunning**：GCS 只收到 RUNNING（含 worker_id）→ worker dead → 通过 worker_id 找到 → 标记 FAILED
3. **TestMarkTaskAttemptFailedIfNeededUpdatesGcPriority**：标记 FAILED 后 task 移到 priority=0
4. **TestDroppedTaskAttemptStillMarkableOnWorkerDead**：Driver 端 SUBMITTED_TO_WORKER 丢失 → Worker 端 RUNNING 含 worker_id → worker dead → 正确标记 FAILED

### 6.2 集成测试

模拟 tidal 节点抢占场景：
1. 大量 task 提交 + driver 端 event buffer 接近满
2. Worker 被 SIGTERM kill
3. 验证 Dashboard 中无 ghost RUNNING task
4. 验证 MarkTasksFailedOnWorkerDead 正确标记 FAILED

### 6.3 回归测试

- `src/ray/core_worker/tests/task_event_buffer_test.cc`
- `src/ray/core_worker/tests/task_event_buffer_export_event_test.cc`
- `src/ray/gcs/tests/gcs_task_manager_test.cc`
- 现有 `actor_task_submitter_test` + `normal_task_submitter_test`

---

## 七、验证 ghost 根因的方法（下次复现时）

1. **Driver 端日志**：搜索 `"Dropping task status events for task"` — 确认 event buffer 是否溢出
2. **GCS 日志**：搜索 `MarkTasksFailedOnWorkerDead` — 确认是否因 worker_id 缺失而找不到 task
3. **Ghost task 状态**：检查 `state_updates` 是否只有 `worker_pid` 而无 `worker_id`/`node_id`
4. **Driver 端统计**：搜索 `kNumTaskStatusEventDroppedSinceLastFlush` 计数器 — 确认是否有大量事件丢弃
