# Ray OOM Kill → Error Object → UNRECONSTRUCTABLE 完整代码链路深度分析

本文档详细分析 Ray 集群中 OOM Kill Worker 后，导致下游 Task 输入 ObjectRef 被标记为 OUT_OF_MEMORY error object，最终触发 OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED 的完整 C++/Python 代码链路。

---

## 目录

1. [问题现象](#1-问题现象)
2. [Raylet OOM Kill Worker 的完整代码链路](#2-raylet-oom-kill-worker-的完整代码链路)
3. [should_retry 的判断逻辑：GroupByOwnerIdWorkerKillingPolicy](#3-should_retry-的判断逻辑)
4. [Driver CoreWorker 如何获取 OOM 失败原因](#4-driver-coreworker-如何获取-oom-失败原因)
5. [FailOrRetryPendingTask：retry 还是 fail 的决策](#5-failorretrypendingtaskretry-还是-fail-的决策)
6. [MarkTaskReturnObjectsFailed：Error Object 的创建](#6-marktaskreturnobjectsfailederror-object-的创建)
7. [下游级联传播：raise_if_dependency_failed](#7-下游级联传播raise_if_dependency_failed)
8. [OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED 的三种触发路径](#8-object_unreconstructable_max_attempts_exceeded-的三种触发路径)
9. [完整异常链路时序图](#9-完整异常链路时序图)
10. [根因总结与修复建议](#10-根因总结与修复建议)
11. [Error Block 不会跨算子直接传播](#11-error-block-不会跨算子直接传播)
12. [级联恢复的完整代码链路](#12-级联恢复的完整代码链路)
13. [定位排查步骤和日志查看方法](#13-定位排查步骤和日志查看方法)

---

## 1. 问题现象

### 典型报错（三层嵌套异常）

```
ray.exceptions.RayTaskError(OutOfMemoryError): ray::MapWorker(MapBatches(QGPreprocessMapper)).submit()
  At least one of the input arguments for this task could not be computed:
ray.exceptions.RayTaskError: ray::StreamingRepartition[num_rows_per_block=512,strict=False]()
  At least one of the input arguments for this task could not be computed:
ray.exceptions.OutOfMemoryError: 1 worker(s) were killed due to the node running low on memory.
  Memory on the node (IP: 10.80.244.19) was 957.08GB / 1007.20GB (0.950240)
  ...
  Ray killed 1 worker(s) based on the killing policy:
  [(Task: ... task name=ReadArrowJSON->SplitBlocks(7), pid=478, actual memory used=12.17GB, ...)]
```

### 算子执行链路

```
ReadArrowJSON->SplitBlocks(7)   ← 被 OOM kill 的 worker
        ↓ (输出 ObjectRef)
StreamingRepartition             ← 输入依赖失败
        ↓ (输出 ObjectRef)
MapBatches(QGPreprocessMapper)  ← 输入依赖失败
        ↓
Driver (streaming_executor)     ← ray.get() 抛出异常
```

### 关键配置

| 配置项 | 值 | 说明 |
|--------|-----|------|
| `max_retries` | -1 | 无限重试 |
| `max_errored_blocks` | -1 | unlimited，异常不中断 |
| `object_reconstruction_enabled` | False | 未启用 object reconstruction |
| `num_oom_retries_left_` | 3 (默认) | OOM 重试上限，独立于 num_retries_left_ |
| `max_lineage_bytes_` | 1GB (默认) | 超过后触发 EvictLineage |
| `RAY_memory_usage_threshold` | 0.95 (默认) | 95% 内存阈值 |

---

## 2. Raylet OOM Kill Worker 的完整代码链路

### 2.1 内存监控入口

`src/ray/raylet/node_manager.cc` 中的 `MemoryMonitor` 定期检测节点内存使用率：

```cpp
// node_manager.cc - 内存监控回调
// 当 memory_monitor 检测到内存使用率超过阈值时触发
```

### 2.2 选择要 Kill 的 Worker

```cpp
// node_manager.cc:3060-3063
std::vector<std::pair<std::shared_ptr<WorkerInterface>, bool>>
    workers_to_kill_and_should_retry =
        worker_killing_policy_->SelectWorkersToKill(
            workers, process_memory_snapshot, system_memory);
```

返回值为 `vector<pair<WorkerInterface, bool>>`，其中 `bool` 即 **`should_retry`**。

### 2.3 设置 Worker 失败原因 + Kill Worker

```cpp
// node_manager.cc:3104-3124
for (const auto &[worker_to_kill, should_retry] :
     workers_to_kill_and_should_retry) {
    // 构造 OUT_OF_MEMORY 错误信息
    rpc::RayErrorInfo worker_failure_reason;
    worker_failure_reason.set_error_message(worker_exit_message);
    worker_failure_reason.set_error_type(rpc::ErrorType::OUT_OF_MEMORY);

    // 将失败原因和 should_retry 标记存入 worker_failure_reasons_ map
    if (!worker_to_kill->GetGrantedLeaseId().IsNil()) {
        SetWorkerFailureReason(worker_to_kill->GetGrantedLeaseId(),
                               worker_failure_reason,
                               should_retry);  // ← should_retry 传递到这里
    }

    // SIGKILL worker
    DestroyWorker(worker_to_kill,
                  rpc::WorkerExitType::NODE_OUT_OF_MEMORY,
                  worker_exit_message,
                  true /* force */);
}
```

### 2.4 SetWorkerFailureReason 的存储逻辑

```cpp
// node_manager.cc:3281-3292
void NodeManager::SetWorkerFailureReason(const LeaseID &lease_id,
                                         const rpc::RayErrorInfo &failure_reason,
                                         bool should_retry) {
    ray::TaskFailureEntry entry(failure_reason, should_retry);
    auto result = worker_failure_reasons_.emplace(lease_id, std::move(entry));
    // 存入 map：lease_id → (failure_reason, should_retry)
}
```

`TaskFailureEntry` 结构：
```cpp
struct TaskFailureEntry {
    rpc::RayErrorInfo ray_error_info_;    // 错误详情
    bool should_retry_;                   // 是否应该重试
    std::chrono::steady_clock::time_point creation_time_;  // 创建时间（用于 GC）
};
```

---

## 3. should_retry 的判断逻辑

### 3.1 GroupByOwnerIdWorkerKillingPolicy（默认策略）

```cpp
// worker_killing_policy_group_by_owner.cc:100-108
// 先检查是否有无需 lease 的 idle worker 可以 kill（内存超过阈值的）
std::shared_ptr<WorkerInterface> idle_worker_to_kill = nullptr;
for (const auto &worker : workers) {
    if (worker->GetGrantedLeaseId().IsNil()) {
        // idle worker 没有 lease
        if (used_memory > idle_worker_killing_memory_threshold_bytes_ && ...) {
            idle_worker_to_kill = worker;
        }
    }
}
```

如果有 idle worker 可 kill：
```cpp
// worker_killing_policy_group_by_owner.cc:90-91
return {{idle_worker_to_kill, /*should_retry=*/false}};  // idle worker 不重试
```

### 3.2 分组 key 是 ParentTaskId()，而非 owner 地址

分组的 key 是 **`LeaseSpecification::ParentTaskId()`**，即提交该 task 的父 task 的 TaskID（通常为 Driver 的 TaskID），而不是 worker 的 owner 地址。

```cpp
// worker_killing_policy_group_by_owner.cc:104-131
/// Group workers by owner id
TaskID non_retriable_owner_id = TaskID::Nil();    // ← 所有 non-retriable 归入同一 Nil 组
std::unordered_map<TaskID, Group> group_map;

for (std::shared_ptr<WorkerInterface> worker : workers) {
    // 跳过没有 granted lease 的 worker（idle worker 不参与分组）
    if (worker->GetGrantedLeaseId().IsNil()) {
      continue;
    }

    // 获取当前 lease 的 IsRetriable() 状态
    bool retriable = worker->GetGrantedLease().GetLeaseSpecification().IsRetriable();

    // ★ 分组 key 选择：
    //   retriable → 按 ParentTaskId() 分组（通常都是 Driver 的 TaskID）
    //   non-retriable → 全部归入 TaskID::Nil() 组
    TaskID owner_id =
        retriable ? worker->GetGrantedLease().GetLeaseSpecification().ParentTaskId()
                  : non_retriable_owner_id;   // TaskID::Nil()

    // 插入对应 group
    std::unordered_map<TaskID, Group>::iterator it = group_map.find(owner_id);
    if (it == group_map.end()) {
        Group group(owner_id, retriable);
        group.AddToGroup(worker);
        group_map.emplace(owner_id, std::move(group));
    } else {
        Group &group = it->second;
        group.AddToGroup(worker);
    }
}
```

**分组规则汇总**：

| Worker 当前 lease 类型 | IsRetriable() | 分组 key | 说明 |
|------------------------|---------------|----------|------|
| Normal task (max_retries>0) | true | ParentTaskId() | 通常都映射到 Driver TaskID → 同组 |
| Actor creation task (max_restarts>0) | true | ParentTaskId() | 也映射到 Driver TaskID → 与 normal task 同组 |
| Actor creation task (max_restarts=0) | **false** | TaskID::Nil() | 所有 non-retriable 归入 Nil 组 |
| Normal task (max_retries=0) | **false** | TaskID::Nil() | 所有 non-retriable 归入 Nil 组 |
| Idle worker（无 lease） | N/A | **跳过，不参与分组** | `GetGrantedLeaseId().IsNil()` → continue |

**关键细节**：

1. **`ParentTaskId()` 的实现**（`lease_spec.cc:172-178`）：
```cpp
TaskID LeaseSpecification::ParentTaskId() const {
    // Set to Nil for driver tasks.
    if (message_->parent_task_id().empty()) {
        return TaskID::Nil();
    }
    return TaskID::FromBinary(message_->parent_task_id());
}
```
所有由 Driver 直接提交的 task（包括 normal task 和 actor creation task），其 `parent_task_id` 都是 Driver 的 TaskID，因此 retriable 的 worker 会归入同一个 group。

2. **`non_retriable_owner_id = TaskID::Nil()`**：所有 non-retriable 的 worker（无论来自哪个 owner）都归入同一个 Nil 组，这意味着一个 Nil 组可能包含多个不同 owner 的 non-retriable worker。

3. **idle worker 不参与分组**：没有 granted lease 的 worker（`GetGrantedLeaseId().IsNil()`）直接跳过，不计入任何 group 的 `GetAllWorkers().size()`。

4. **同组 retriable 一致性检查**（`worker_killing_policy_group_by_owner.cc:233-240`）：
```cpp
void Group::AddToGroup(std::shared_ptr<WorkerInterface> worker) {
    // ...
    bool retriable = worker->GetGrantedLease().GetLeaseSpecification().IsRetriable();
    RAY_CHECK_EQ(retriable_, retriable);  // ★ 同组必须 retriable 一致
    workers_.push_back(worker);
}
```
由于 ParentTaskId 相同的 retriable worker 和 ParentTaskId 为 Nil 的 non-retriable worker 的 key 不同，它们天然不会进入同一 group。RAY_CHECK 是额外的防御性断言。

### 3.3 IsRetriable 的判断（★ LeaseSpecification 与 TaskSpecification 差异）

#### 3.3.1 LeaseSpecification::IsRetriable() — Killing Policy 使用的版本

Killing Policy 中 `worker->GetGrantedLease().GetLeaseSpecification().IsRetriable()` 调用的是 `LeaseSpecification::IsRetriable()`：

```cpp
// lease_spec.cc:149-157
bool LeaseSpecification::IsRetriable() const {
    if (IsActorCreationTask() && MaxActorRestarts() == 0) {
        return false;   // actor 创建任务 max_restarts=0 → 不可重试
    }
    if (IsNormalTask() && MaxRetries() == 0) {
        return false;   // 普通 task max_retries=0 → 不可重试
    }
    return true;        // 其他情况均可重试
}
```

**关键**：`LeaseSpecification` 构造时有 RAY_CHECK 约束：

```cpp
// lease_spec.cc:26-27
RAY_CHECK(task_spec.type() == rpc::TaskType::NORMAL_TASK ||
          task_spec.type() == rpc::TaskType::ACTOR_CREATION_TASK);
```

`LeaseSpecification` **只处理两种类型**：`NORMAL_TASK` 和 `ACTOR_CREATION_TASK`。**Actor method task (`ACTOR_TASK`) 不会产生独立的 lease** — actor method task 在已创建的 actor worker 上执行，该 worker 持有的 lease 始终是 actor creation task 的 lease。

因此 `LeaseSpecification::IsRetriable()` 的判断逻辑简化为：

| Worker 当前持有的 lease 类型 | IsRetriable() 条件 | 结果 |
|---|---|---|
| NORMAL_TASK | max_retries > 0 | **true** |
| NORMAL_TASK | max_retries == 0 | **false** |
| ACTOR_CREATION_TASK | max_actor_restarts > 0 | **true** |
| ACTOR_CREATION_TASK | max_actor_restarts == 0 | **false** |
| ACTOR_TASK | **不会出现**（无独立 lease） | N/A |

#### 3.3.2 TaskSpecification::IsRetriable() — Task Manager 使用的版本（对比）

注意 `TaskSpecification::IsRetriable()` 与 `LeaseSpecification::IsRetriable()` **不同**：

```cpp
// task_spec.cc:602-613
bool TaskSpecification::IsRetriable() const {
    if (IsActorTask()) {                    // ★ actor method task 始终不可重试
        return false;
    }
    if (IsActorCreationTask() && MaxActorRestarts() == 0) {
        return false;
    }
    if (IsNormalTask() && MaxRetries() == 0) {
        return false;
    }
    return true;
}
```

差异：`TaskSpecification` 多了 `IsActorTask()` 的判断（行 603-604），actor method task 直接返回 `false`。但这对 Killing Policy 无影响，因为 Killing Policy 使用的是 `LeaseSpecification`，而 actor worker 持有的是 actor creation task 的 lease。

#### 3.3.3 Actor Worker 的 Lease 生命周期

理解 actor worker 在 Killing Policy 分组中的归属，需要明确 actor worker 的 lease 生命周期：

```
1. 用户创建 actor (Python 端)
   → Core Worker 提交 actor creation task (TaskType::ACTOR_CREATION_TASK)
   → rpc::TaskSpec 包含 actor_creation_task_spec().max_actor_restarts()

2. GCS Actor Manager 注册/创建 actor
   → GcsActor 构造: lease_spec_ = std::make_unique<LeaseSpecification>(*task_spec_)
   → LeaseSpecification(task_spec) 构造:
     - message_->set_type(task_spec.type())           // ACTOR_CREATION_TASK
     - message_->set_max_actor_restarts(
           task_spec.actor_creation_task_spec().max_actor_restarts())

3. GcsActorScheduler 请求 worker lease
   → raylet_client->RequestWorkerLease(
         actor->GetLeaseSpecification().GetMessage(), ...)

4. Raylet NodeManager 收到 RequestWorkerLease
   → 创建 RayLease{LeaseSpec} 对象
   → Worker 被分配 lease 后: worker->GetGrantedLease().GetLeaseSpecification()

5. Actor worker 整个生命周期中：
   → 初始化时持有 ACTOR_CREATION_TASK 的 lease
   → 初始化完成后执行 actor method task，但 lease 不变（仍是 creation lease）
   → 只有当 actor 被销毁或重启时，lease 才会变化
```

**核心结论**：actor worker 在执行 method task 期间，仍然持有 actor creation task 的 lease。因此 Killing Policy 检查 `IsRetriable()` 时，看的是 `ACTOR_CREATION_TASK` + `max_actor_restarts` 的组合，而非 method task 本身。

**★ 修正：Actor Worker 始终持有 lease，不会归还**

之前的分析错误地认为 actor worker 在 method task 执行完成后会归还 lease（`GetGrantedLeaseId().IsNil()=true`）。
实际上，actor creation task 完成后，`CleanupLease` 调用 `ConvertWorkerToActor`，**不清除 `GrantedLeaseId`**：

```cpp
// node_manager.cc:2357-2382
bool NodeManager::CleanupLease(const std::shared_ptr<WorkerInterface> &worker) {
  ...
  if (lease_spec.IsActorCreationTask()) {
    ConvertWorkerToActor(worker, lease);  // 转为 actor，不归还 lease
  }
  if (!lease_spec.IsActorCreationTask()) {
    worker->GrantLeaseId(LeaseID::Nil()); // 只有非 actor 才清除 lease
  }
  return !lease_spec.IsActorCreationTask(); // actor: return false → worker_idle=false
}
```

actor worker **整个生命周期中始终持有** actor creation task 的 lease（`GetGrantedLeaseId().IsNil()=false`），
不会因为执行 method task 完成或等待下一个 method task 而归还 lease。

| Actor Worker 状态 | GetGrantedLeaseId() | Lease 类型 | IsRetriable() | 分组归属 |
|---|---|---|---|---|
| 刚创建，执行 `__init__` | **非 Nil** | ACTOR_CREATION_TASK | 取决于 max_actor_restarts | retriable → ParentTaskId() 组; non-retriable → Nil 组 |
| `__init__` 完成，等待 method task | **非 Nil**（lease 不归还） | ACTOR_CREATION_TASK | 取决于 max_actor_restarts | **同上，始终参与分组** |
| 执行 actor method task | **非 Nil** | 仍为 ACTOR_CREATION_TASK lease | 取决于 max_actor_restarts | **同上** |
| Idle（method 执行完，等待下一个） | **非 Nil**（lease 不归还） | 仍为 ACTOR_CREATION_TASK lease | 取决于 max_actor_restarts | **同上，始终参与分组** |

**★ Ray Data ActorPool 的 max_restarts 默认值**

之前的分析错误地认为 Ray Data ActorPool 中 `max_actor_restarts` 默认为 `0`。
实际上，`ActorPoolMapOperator._apply_default_remote_args` 将默认值设为 **`-1`（无限重启）**：

```python
# actor_pool_map_operator.py:595-604
if "max_restarts" not in ray_remote_args:
    ray_remote_args["max_restarts"] = -1        # ← 无限重启，不是 0
if ("max_task_retries" not in ray_remote_args
    and ray_remote_args.get("max_restarts") != 0):
    ray_remote_args["max_task_retries"] = -1    # ← 无限 method task 重试
```

因此 Ray Data ActorPool 场景下，`max_actor_restarts=-1 > 0`，`IsRetriable()=true`，
actor worker 归入 `ParentTaskId()` 组（Driver group），与 ReadArrowJSON **同组**。

只有用户**显式设置 `max_restarts=0`**（如 `map_batches(..., max_restarts=0)`）时，
`IsRetriable()` 才为 `false`，actor worker 才归入 Nil 组。

### 3.4 排序策略：优先选择 retriable 的最大 group

```cpp
// worker_killing_policy_group_by_owner.cc:146-158
std::sort(sorted.begin(), sorted.end(),
    [](const Group &left, const Group &right) -> bool {
        int left_retriable = left.IsRetriable() ? 0 : 1;   // retriable 排前面
        int right_retriable = right.IsRetriable() ? 0 : 1;
        if (left_retriable == right_retriable) {
            if (left.GetAllWorkers().size() == right.GetAllWorkers().size()) {
                return left.GetGrantedLeaseTime() > right.GetGrantedLeaseTime();
            }
            return left.GetAllWorkers().size() > right.GetAllWorkers().size();  // 大组优先
        }
        return left_retriable < right_retriable;
    });
```

排序优先级：**retriable > non-retriable > 最大组 > 最新组**

### 3.5 should_retry 的核心判断（★ 关键代码）

```cpp
// worker_killing_policy_group_by_owner.cc:160-162
Group selected_group = sorted.front();
bool should_retry =
    selected_group.GetAllWorkers().size() > 1 && selected_group.IsRetriable();
//                              ^^^^^^^^^^^^^^^^^^^^^^^^
//                              核心条件：同一 owner group 下必须有 >1 个 worker
```

**这就是 `should_retry` 判断的核心逻辑。** 在 Ray Data ActorPool 默认配置下
（`max_restarts=-1`），actor worker 与 ReadArrowJSON 同属 Driver group，group size ≥ 2，
`should_retry=true`。只有当用户显式设置 `max_restarts=0` 或 Driver group 中仅有 1 个 worker
时，`should_retry` 才为 `false`。

### 3.6 在你的场景中为什么 should_retry=false

#### 3.6.1 场景中的 Worker 列表

以 10.80.244.19 节点为例（Top 10 memory users）：

```
PID    MEM(GB)    COMMAND
478    12.17      ray::ReadArrowJSON->SplitBlocks(7)   ← 唯一一个 ReadArrowJSON worker
57     5.43       raylet
476    1.52       ray::MapWorker(MapBatches(QGInferMapper))  ← Actor Worker
126    0.17       ray::DashboardAgent
...
```

**关键前提**：`MapBatches(QGInferMapper)` 使用 `ActorPool` compute 策略，MapWorker 是 **actor**，不是 normal task。

#### 3.6.2 完整分组推理链（★ 关键分析）

之前的分析存在错误：简单地认为"ReadArrowJSON 的 owner 是 Driver，该 owner group 下只有 1 个 ReadArrowJSON worker"，而忽略了 **MapWorker 作为 actor 如何参与分组** 的问题。

实际上，分组结果取决于 **OOM Kill 那一刻每个 worker 的 lease 状态**：

**Step 1: ReadArrowJSON->SplitBlocks(7) 的分组归属**

```
ReadArrowJSON->SplitBlocks(7):
  → Normal task, max_retries=-1 (无限)
  → IsRetriable() = true (max_retries > 0)
  → 分组 key = ParentTaskId() = Driver TaskID
  → 归入 Driver group
```

**Step 2: MapWorker(MapBatches(QGInferMapper)) 的分组归属**

MapWorker 是 actor，**始终持有** actor creation task 的 lease（不会归还，见 §3.3.3 修正），
其分组归属取决于 `max_actor_restarts`：

```
情况 A: MapWorker（Ray Data ActorPool 默认配置 max_restarts=-1）
  → GetGrantedLeaseId().IsNil() = false（始终持有 lease）
  → Lease 类型: ACTOR_CREATION_TASK (actor worker 持有的是 creation lease)
  → max_actor_restarts = -1 > 0:
     IsRetriable() = true
     分组 key = ParentTaskId() = Driver TaskID
     → 与 ReadArrowJSON 同组 (Driver group)
     → group size ≥ 2
     → should_retry = true

情况 B: MapWorker（用户显式设置 max_restarts=0）
  → GetGrantedLeaseId().IsNil() = false（始终持有 lease）
  → Lease 类型: ACTOR_CREATION_TASK
  → max_actor_restarts == 0:
     IsRetriable() = false
     分组 key = TaskID::Nil()
     → 与 ReadArrowJSON 不同组
     → ReadArrowJSON 独占 Driver group, size = 1
     → should_retry = false
```

#### 3.6.3 本场景的实际分组结果（★ 修正后）

**修正前（错误）**：认为 MapWorker 在 idle 时归还 lease，不参与分组。
**修正后**：MapWorker 始终持有 lease，始终参与分组。

**Ray Data ActorPool 默认配置下（`max_restarts=-1`）**：

```
Killing Policy 遍历 workers:
  PID 478 (ReadArrowJSON):
    → GetGrantedLeaseId().IsNil() = false (正在执行)
    → IsRetriable() = true (normal task, max_retries=-1)
    → 分组 key = Driver TaskID
    → 加入 Driver group

  PID 476 (MapWorker):
    → GetGrantedLeaseId().IsNil() = false (★ 始终持有 lease，不归还)
    → Lease 类型: ACTOR_CREATION_TASK
    → max_actor_restarts = -1 > 0 → IsRetriable() = true
    → 分组 key = ParentTaskId() = Driver TaskID
    → 加入 Driver group

分组结果:
  Driver group: [ReadArrowJSON->SplitBlocks(7), MapWorker(MapBatches)]   size = 2
  Nil group: (无 non-retriable worker)

排序后:
  selected_group = Driver group (retriable 优先)
  selected_group.GetAllWorkers().size() == 2
  should_retry = 2 > 1 && true = true
```

**★ 这意味着在 Ray Data ActorPool 默认配置下，`should_retry=true`，而非之前分析的 `false`。**

ReadArrowJSON 被 OOM kill 后，会进入 `RetryTaskIfPossible()` 流程，拥有 OOM 重试机会
（`num_oom_retries_left_` 默认 3 次）。

#### 3.6.4 should_retry=true 时的完整路径

当 `should_retry=true` 时，被 kill 的 ReadArrowJSON worker 的 task 会经历 OOM retry：

```
1. Raylet OOM Kill → should_retry=true (group size=2)
2. SetWorkerFailureReason(lease_id, OUT_OF_MEMORY, should_retry=true)
3. Driver HandleGetWorkerFailureCause → fail_immediately=false
4. FailOrRetryPendingTask(fail_immediately=false)
   → 调用 RetryTaskIfPossible()
   → 检查 OOM retry: num_oom_retries_left_ (默认 3)
   → num_oom_retries_left_ > 0 → will_retry=true, num_oom_retries_left_--
   → 重新提交 task
5. 如果 retry 后在同节点再次 OOM:
   → 同一流程再次触发
   → num_oom_retries_left_ 继续递减
6. 重试 3 次后 num_oom_retries_left_ == 0:
   → will_retry = false
   → 调用 FailPendingTask()
   → task spec 从 submissible_tasks_ 中删除
   → MarkTaskReturnObjectsFailed() → OUT_OF_MEMORY error object
7. 后续 lineage reconstruction 尝试:
   → ResubmitTask → submissible_tasks_ 中找不到
   → OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED
```

**但注意**：retry 期间，Ray 会将 task 重新调度到**其他可用节点**
（因为被 kill 的节点仍然内存紧张），如果其他节点有足够内存，task 可以成功执行。

#### 3.6.5 用户显式设置 max_restarts=0 时的情况

如果用户在 `map_batches` 中设置了 `max_restarts=0`（如
`map_batches(..., max_restarts=0)`），此时 actor worker 的 `IsRetriable()=false`：

```
Killing Policy 遍历 workers:
  PID 478 (ReadArrowJSON):
    → IsRetriable() = true → 分组 key = Driver TaskID → Driver group

  PID 476 (MapWorker, max_restarts=0):
    → GetGrantedLeaseId().IsNil() = false (始终持有 lease)
    → Lease 类型: ACTOR_CREATION_TASK
    → max_actor_restarts == 0 → IsRetriable() = false
    → 分组 key = TaskID::Nil()
    → 归入 Nil group（不与 ReadArrowJSON 同组）

分组结果:
  Driver group: [ReadArrowJSON->SplitBlocks(7)]   size = 1
  Nil group: [MapWorker(MapBatches)]              size = 1

排序后:
  selected_group = Driver group (retriable 优先)
  should_retry = 1 > 1 && true = false
```

此时 `should_retry=false`，与原文档分析一致。

#### 3.6.6 小结（★ 修正后）

**Ray Data ActorPool 默认配置下（`max_restarts=-1`）**：
- MapWorker 始终持有 lease，`IsRetriable()=true`，归入 Driver group
- 与 ReadArrowJSON 同组 → group size ≥ 2 → **`should_retry=true`**
- OOM kill 后会进入 retry 流程（默认 3 次 OOM retry），有恢复机会

**用户显式设置 `max_restarts=0` 时**：
- MapWorker `IsRetriable()=false`，归入 Nil group
- ReadArrowJSON 独占 Driver group → group size = 1 → **`should_retry=false`**
- OOM kill 后直接 FailPendingTask，无 retry 机会

**之前分析的错误**：
1. 错误地认为 actor worker idle 时归还 lease → 实际上 actor 始终持有 creation lease
2. 错误地认为 Ray Data ActorPool 的 `max_restarts` 默认为 0 → 实际默认为 -1（无限）

### 3.7 should_retry 设计意图

Ray 的设计逻辑是：

| group size | IsRetriable | should_retry | 原因 |
|------------|-------------|--------------|------|
| >1 | true | **true** | 同 group 有其他 worker 可以接替工作，retry 有意义 |
| =1 | true | **false** | kill 后无替代 worker，retry 大概率在同样条件下再次 OOM |
| 任意 | false | **false** | task 本身不可重试 |
| idle worker（非 actor，无 lease） | N/A | **false** | idle worker 没有 task 需要重试 |

**★ 注意**：actor worker 不会归还 lease，因此不会出现 "idle actor worker 无 lease" 的情况。
只有非 actor 的 idle worker（如普通 task worker 完成 task 后等待新 task）才可能
`GetGrantedLeaseId().IsNil()=true`。

**Actor Worker 对 should_retry 判断的影响**（★ 修正后）：

| MapWorker 状态 | 是否参与分组 | 对 Driver group size 的影响 | should_retry |
|---|---|---|---|
| 任意状态（Ray Data 默认 max_restarts=-1） | **始终参与**（持有 lease） | 贡献+1 → size≥2 | **true** |
| 任意状态（用户设 max_restarts=0） | 归入 Nil 组 | 无贡献 → size=1 | **false** |
| 任意状态（用户设 max_restarts=N>0） | 归入 Driver 组 | 贡献+1 → size≥2 | **true** |

### 3.8 实际场景中 should_retry=false 的触发条件

#### 3.8.1 关键前提：Killing Policy 按节点独立执行

`GroupByOwnerIdWorkerKillingPolicy` 的 `SelectWorkersToKill` 由每个节点的 raylet 独立调用，
**只看自己节点上的 worker**。一个节点上的分组结果不受其他节点上 worker 的影响。

因此，即使集群整体有大量 actor worker（`max_restarts=-1`），如果 OOM 发生的节点上
**没有 actor worker**，该节点的 Driver group 可能只有 1 个 retriable worker。

#### 3.8.2 两个框架的默认配置分析

##### framework runner（qg_v4）

`pipeline/framework/processors/qg_v4.py` 中的 `map_batches` **没有**显式传 `max_restarts`：

```python
# qg_v4.py:61-79
cpu_kw = dict(
    fn_constructor_kwargs={...},
    batch_format="pandas",
    num_cpus=int(p.get("cpu_num_cpus", 1)),
    num_gpus=0,
    compute=cpu_compute,
    batch_size=int(p.get("cpu_batch_size", 8)),
)
# ↑ 没有 max_restarts → 走 ActorPool 默认 -1（无限重启）
```

GPU actor 同理（`gpu_kw` 通过 `ctx.helpers.gpu_map_kwargs` 构建，也没有 `max_restarts`）。

**结论**：两套框架默认配置下，`max_restarts=-1`，actor worker 的 `IsRetriable()=true`，
只要与 ReadArrowJSON 在同一节点，就会归入 Driver group，group size ≥ 2，`should_retry=true`。

##### fs_ray pipeline

`ops/core/feature_service/fs_ray_pipeline.py` 中的 `map_batches(FsRayActor, ...)` 同样
没有显式传 `max_restarts`，走默认 `-1`。

#### 3.8.3 should_retry=false 的三种触发场景

**场景 1：ReadArrowJSON 独占节点（★ 最可能）**

ReadArrowJSON 通常在数据所在节点执行（locality 亲和），而 CPU/GPU actor 受
`scheduler_avoid_gpu_nodes` 和 `scheduling_strategy` 调度到不同节点。

```
节点 A（数据本地节点）：
  ReadArrowJSON worker → 12.17GB（内存最大，被 kill）
  （没有 CPU/GPU actor 在此节点）

Killing Policy 分组（只看节点 A 的 worker）：
  Driver group = [ReadArrowJSON]  → size = 1
  should_retry = 1 > 1 && true = false
  → 直接 FailPendingTask → UNRECONSTRUCTABLE
```

即使集群其他节点上有大量 `max_restarts=-1` 的 actor worker，它们不参与节点 A 的分组。

这是**实际部署中最常见的触发场景**，尤其是：
- 数据源节点（存储亲和）与计算节点分离
- ReadArrowJSON 是节点上唯一的 Ray task worker
- actor 全部被调度到 GPU 节点或其他 CPU 节点

**场景 2：用户显式设置 `max_restarts=0`**

如果用户代码写了 `map_batches(..., max_restarts=0)`，即使 actor 与 ReadArrowJSON
在同一节点：

```
节点 A：
  ReadArrowJSON worker → IsRetriable() = true → Driver group
  MapWorker (max_restarts=0) → IsRetriable() = false → Nil group

  Driver group = [ReadArrowJSON]  → size = 1
  should_retry = false
```

**从两套框架的代码看**：当前都没有显式设 `max_restarts=0`。早期 qg_v4 迁移报告
提到过 `max_restarts=3`（有限值，仍 retriable），后被移除走默认 `-1`。

**场景 3：idle worker 优先被 kill（跳过分组逻辑）**

Killing Policy 的第一阶段（`worker_killing_policy_group_by_owner.cc:78-101`）：
**优先 kill 内存超过阈值且无 lease 的 idle worker**，直接返回 `should_retry=false`。

```cpp
// worker_killing_policy_group_by_owner.cc:78-101
// 遍历所有 worker，找 GetGrantedLeaseId().IsNil() 的 idle worker
if (worker->GetGrantedLeaseId().IsNil()) {
    if (used_memory > idle_worker_killing_memory_threshold_bytes_ && ...) {
        idle_worker_to_kill = worker;
    }
}
if (idle_worker_to_kill) {
    return {{idle_worker_to_kill, /*should_retry=*/false}};  // ← 跳过分组
}
```

此场景 kill 的是 idle worker 本身（无 lease），不影响正在执行的 retriable task 的
`should_retry` 判断。但如果后续 OOM 持续触发，进入分组逻辑后仍可能命中场景 1。

#### 3.8.4 should_retry=true 但仍最终失败的场景

Ray Data ActorPool 默认 `max_restarts=-1`，如果 actor 与 ReadArrowJSON 在同节点，
`should_retry=true`，进入 OOM retry 流程（默认 3 次）：

```
1. OOM kill → should_retry=true → RetryTaskIfPossible()
2. num_oom_retries_left_ = 3 → retry
3. 如果 retry 被调度到同一节点（Ray 默认在同一 raylet 调度）
   → 再次 OOM → retry → 再次 OOM
4. 3 次 retry 耗尽 → num_oom_retries_left_ == 0
   → will_retry = false
   → FailPendingTask()
   → task spec 删除 → UNRECONSTRUCTABLE
```

**与 `should_retry=false` 的区别**：
- `should_retry=true`：有 3 次 OOM retry 机会，如果调度到其他节点可能成功恢复
- `should_retry=false`：直接 fail，无任何 retry 机会

**两者最终结果相同**（如果 retry 全部在同节点 OOM），
但 `should_retry=true` 给了跨节点恢复的可能性。

#### 3.8.5 跨节点 retry 的关键问题

`RetryTaskIfPossible` 重新提交 task 时，Ray 的调度器会根据资源可用性选择节点：

```cpp
// task_manager.cc:1137-1215 (RetryTaskIfPossible)
// OOM retry 时：
if (num_oom_retries_left_ != 0) {
    will_retry = true;
    if (num_oom_retries_left_ > 0) {
        num_oom_retries_left_--;
    }
    // 设置 task 为 FAILED → MarkRetry → 重新提交
    // async_retry_task_callback_ 重新提交到调度队列
    // 调度器会根据资源可用性选择节点（不一定在 OOM 节点）
}
```

如果 OOM 节点仍然内存紧张，调度器会将 task 调度到其他有资源的节点，task 有机会成功执行。
但如果所有节点都内存紧张，retry 仍会失败。

#### 3.8.6 小结

| 场景 | should_retry | OOM retry 机会 | 最终结果 | 实际概率 |
|---|---|---|---|---|
| ReadArrowJSON 独占 OOM 节点 | **false** | 无 | 直接 UNRECONSTRUCTABLE | **高**（数据亲和调度） |
| 用户设 max_restarts=0 | **false** | 无 | 直接 UNRECONSTRUCTABLE | 低（默认不设） |
| idle worker 被 kill | **false** | 无 | idle worker 无需 retry | 仅影响 idle worker |
| actor 与 ReadArrowJSON 同节点（默认配置） | **true** | 3 次 | retry 成功恢复 / retry 耗尽后 UNRECONSTRUCTABLE | 中（取决于跨节点调度） |

---

## 4. Driver CoreWorker 如何获取 OOM 失败原因

### 4.1 PushTaskReply 失败 → 查询 GetWorkerFailureCause

当 Raylet kill worker 后，Driver 的 CoreWorker 正在等待 `PushTaskReply`：

```cpp
// normal_task_submitter.cc:571-595
if (!status.ok()) {
    // PushTask 回复失败
    failed_tasks_pending_failure_cause_.insert(task_id);
    RAY_LOG(DEBUG) << "Getting error from raylet for task " << task_id;
    // 向 Raylet 发送 GetWorkerFailureCause RPC
    const auto callback = [this, status, task_id, addr](
                              const Status &get_task_failure_cause_reply_status,
                              const rpc::GetWorkerFailureCauseReply &reply) {
        bool will_retry = HandleGetWorkerFailureCause(
            status, task_id, addr,
            get_task_failure_cause_reply_status, reply);
        // ...
    };
    raylet_client->GetWorkerFailureCause(cur_lease_entry.lease_id, callback);
}
```

### 4.2 Raylet 返回失败原因

```cpp
// node_manager.cc:717-728
void NodeManager::HandleGetWorkerFailureCause(...) {
    auto it = worker_failure_reasons_.find(lease_id);
    if (it != worker_failure_reasons_.end()) {
        reply->mutable_failure_cause()->CopyFrom(it->second.ray_error_info_);
        //                                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
        //                                OUT_OF_MEMORY 错误信息
        reply->set_fail_task_immediately(!it->second.should_retry_);
        //                           ^^^^^^^^^^^^^^^^^^^^^^^^^^^
        //                           should_retry=false → fail_task_immediately=true
    }
}
```

**关键转换**：`should_retry` 被取反为 `fail_task_immediately`：
- `should_retry=true` → `fail_task_immediately=false` → 可以重试
- `should_retry=false` → `fail_task_immediately=true` → 立即失败

### 4.3 HandleGetWorkerFailureCause 处理回复

```cpp
// normal_task_submitter.cc:626-704
bool NormalTaskSubmitter::HandleGetWorkerFailureCause(
    const Status &task_execution_status,
    const TaskID &task_id,
    const rpc::Address &addr,
    const Status &get_worker_failure_cause_reply_status,
    const rpc::GetWorkerFailureCauseReply &reply) {
    rpc::ErrorType task_error_type = rpc::ErrorType::WORKER_DIED;
    std::unique_ptr<rpc::RayErrorInfo> error_info;
    bool fail_immediately = false;

    if (get_worker_failure_cause_reply_status.ok()) {
        if (reply.has_failure_cause()) {
            task_error_type = reply.failure_cause().error_type();
            // → rpc::ErrorType::OUT_OF_MEMORY
            error_info = std::make_unique<rpc::RayErrorInfo>(reply.failure_cause());
        }
        fail_immediately = reply.fail_task_immediately();
        // → true (因为 should_retry=false)
    }

    // 最终调用 FailOrRetryPendingTask
    return task_manager_.FailOrRetryPendingTask(
        task_id,
        task_error_type,          // OUT_OF_MEMORY
        &task_execution_status,
        error_info.get(),
        /*mark_task_object_failed=*/true,
        fail_immediately);        // ← true
}
```

---

## 5. FailOrRetryPendingTask：retry 还是 fail 的决策

### 5.1 核心决策逻辑

```cpp
// task_manager.cc:1348-1374
bool TaskManager::FailOrRetryPendingTask(const TaskID &task_id,
                                         rpc::ErrorType error_type,
                                         const Status *status,
                                         const rpc::RayErrorInfo *ray_error_info,
                                         bool mark_task_object_failed,
                                         bool fail_immediately) {
    bool will_retry = false;

    if (!fail_immediately) {
        // fail_immediately=false 时才尝试重试
        will_retry = RetryTaskIfPossible(task_id, ...);
    }
    // ↑ 你的场景中 fail_immediately=true，所以这整个 if 被跳过

    if (!will_retry && mark_task_object_failed) {
        // will_retry=false → 进入此分支
        FailPendingTask(task_id, error_type, status, ray_error_info);
        // → 标记所有输出为 OUT_OF_MEMORY error object
    }

    return will_retry;  // false
}
```

### 5.2 你的场景中的决策路径

```
fail_immediately = true  (因为 should_retry=false)
    ↓
跳过 RetryTaskIfPossible()
    ↓
will_retry = false
    ↓
调用 FailPendingTask()
    ↓
1. submissible_tasks_.erase(it)  — 从 task map 中移除 task spec
2. MarkTaskReturnObjectsFailed()  — 所有输出标记为 OUT_OF_MEMORY error object
```

### 5.3 对比：如果 should_retry=true 会怎样

```
fail_immediately = false
    ↓
调用 RetryTaskIfPossible()
    ↓
如果是 OOM 错误：检查 num_oom_retries_left_ (默认 3)
    - num_oom_retries_left_ > 0 → will_retry=true, 递减
    - num_oom_retries_left_ == -1 → will_retry=true (无限)
    - num_oom_retries_left_ == 0 → will_retry=false
如果不是 OOM 错误：检查 num_retries_left_
    ↓
will_retry=true 时：
    - SetTaskStatus(FAILED)
    - MarkRetry() — attempt_number++
    - 重新提交 task
    - 不会调用 FailPendingTask()
    - task spec 保留在 submissible_tasks_ 中
```

---

## 6. MarkTaskReturnObjectsFailed：Error Object 的创建

### 6.1 FailPendingTask 的执行

```cpp
// task_manager.cc:1257-1330
void TaskManager::FailPendingTask(const TaskID &task_id,
                                  rpc::ErrorType error_type,
                                  const Status *status,
                                  const rpc::RayErrorInfo *ray_error_info) {
    // ...
    {
        absl::MutexLock lock(&mu_);
        auto it = submissible_tasks_.find(task_id);
        // 确认 task 仍在 submissible_tasks_ 中且是 pending 状态
        spec = it->second.spec_;

        // 设置 task 状态为 FAILED
        SetTaskStatus(it->second, rpc::TaskStatus::FAILED, ...);

        // ★ 从 submissible_tasks_ 中删除 task spec
        submissible_tasks_.erase(it);
        num_pending_tasks_--;
    }

    // 释放 task 的所有引用
    RemoveFinishedTaskReferences(spec, /*release_lineage=*/true, ...);

    // ★ 标记所有返回对象为 error object
    MarkTaskReturnObjectsFailed(spec, error_type, ray_error_info, store_in_plasma_ids);
}
```

### 6.2 MarkTaskReturnObjectsFailed 的完整逻辑

```cpp
// task_manager.cc:1555-1624
void TaskManager::MarkTaskReturnObjectsFailed(
    const TaskSpecification &spec,
    rpc::ErrorType error_type,
    const rpc::RayErrorInfo *ray_error_info,
    const absl::flat_hash_set<ObjectID> &store_in_plasma_ids) {

    const TaskID task_id = spec.TaskId();
    // 创建 error RayObject
    RayObject error(error_type, ray_error_info);

    // 1. 处理固定数量的返回对象
    int64_t num_returns = spec.NumReturns();
    for (int i = 0; i < num_returns; i++) {
        const auto object_id = ObjectID::FromIndex(task_id, i + 1);
        if (store_in_plasma_ids.contains(object_id)) {
            put_in_local_plasma_callback_(error, object_id);
            // ★ 将 OUT_OF_MEMORY error object 写入 plasma store
        } else {
            in_memory_store_.Put(error, object_id, ...);
            // ★ 或写入内存 store
        }
    }

    // 2. 处理动态返回对象（ReturnsDynamic）
    if (spec.ReturnsDynamic()) {
        for (const auto &dynamic_return_id : spec.DynamicReturnIds()) {
            // 同样标记为 error object
        }
    }

    // 3. ★ streaming generator 的特殊处理
    if (spec.IsStreamingGenerator()) {
        const auto generator_id = spec.ReturnId(0);
        MarkEndOfStream(generator_id, /*item_index=*/-1);

        // 遍历 streaming generator 的所有返回 object
        auto num_streaming_generator_returns = spec.NumStreamingGeneratorReturns();
        for (size_t i = 0; i < num_streaming_generator_returns; i++) {
            const auto generator_return_id = spec.StreamingGeneratorReturnId(i);
            if (store_in_plasma_ids.contains(generator_return_id)) {
                put_in_local_plasma_callback_(error, generator_return_id);
                // ★ 覆盖 plasma 中的对象为 error object
            } else {
                in_memory_store_.Put(error, generator_return_id, ...);
            }
        }
    }
}
```

**streaming generator 的注释说明**（`task_manager.cc:1597-1601`）：
```cpp
// If it was a streaming generator, try failing all the return object refs.
// In a normal time, it is no-op because the object ref values are already
// written, and Ray doesn't allow to overwrite values for the object ref.
// It is only useful when lineage reconstruction retry is failed. In this
// case, all these objects are lost from the plasma store, so we
// can overwrite them.
```

这段注释说明：
- **正常情况**：写入 error object 是 no-op，因为 plasma 中已有值且不可覆盖
- **lineage reconstruction 失败后**：plasma 中的对象已丢失，此时可以覆盖为 error object

### 6.3 Error Object 的数据结构（C++ 侧序列化）

`RayObject` 的 error 构造函数 (`ray_object.cc:113-122`)：

```cpp
RayObject::RayObject(rpc::ErrorType error_type, const rpc::RayErrorInfo *ray_error_info) {
  if (ray_error_info == nullptr) {
    // 无详细错误信息：metadata = error_type 数字，data = nullptr
    Init(nullptr, MakeErrorMetadataBuffer(error_type), {});
    return;
  }
  // 有详细错误信息：
  // data = msgpack 序列化的 RayErrorInfo protobuf
  // metadata = error_type 数字
  const auto error_buffer = MakeSerializedErrorBuffer<rpc::RayErrorInfo>(*ray_error_info);
  Init(std::move(error_buffer), MakeErrorMetadataBuffer(error_type), {});
  return;
}
```

**Error Object 在 plasma/memory store 中的存储格式**：

| 字段 | 内容 | 说明 |
|------|------|------|
| `metadata_` | `MakeErrorMetadataBuffer(error_type)` → 例如 `"11"` (OUT_OF_MEMORY 的 protobuf enum 值) | 1-2 字节的 ASCII 数字字符串 |
| `data_` | `MakeSerializedErrorBuffer(ray_error_info)` → msgpack 序列化的 RayErrorInfo protobuf | 包含完整错误消息、task 名、pid 等；若 `ray_error_info==nullptr` 则 `data_=nullptr` |

**`MakeErrorMetadataBuffer` 的实现** (`ray_object.cc:36-39`)：
```cpp
std::shared_ptr<ray::LocalMemoryBuffer> MakeErrorMetadataBuffer(
    ray::rpc::ErrorType error_type) {
  std::string meta = std::to_string(static_cast<int>(error_type));
  // 例如 OUT_OF_MEMORY = 11 → metadata = "11"
  return MakeBufferFromString(meta);
}
```

**`MakeSerializedErrorBuffer` 的实现** (`ray_object.cc:55-92`)：

将 `rpc::RayErrorInfo` protobuf 序列化为 msgpack 格式：
```
RayErrorInfo (protobuf)
  → SerializeToString() → pb_serialized_exception (bytes)
  → msgpack.pack_bin(pb_serialized_exception) → msgpack_serialized_exception
  → [offset(9 bytes)] [msgpack_serialized_exception] → final_buffer
```

最终 data 的结构为：`[msgpack_length][msgpack_serialized_RayErrorInfo]`

### 6.4 Error Object 的反序列化（Python 侧）

当 Python 端通过 `ray.get()` 或 task 执行前读取到 error object 时，完整的反序列化链路如下：

#### Step 1: `worker.get_objects()` 调用 `core_worker.get_objects()`

```python
# worker.py:993
serialized_objects = self.core_worker.get_objects(object_refs, timeout_ms)
```

返回的 `serialized_objects` 是 `List[Tuple[data, metadata, tensor_transport]]`。

#### Step 2: `worker.deserialize_objects()` → `context.deserialize_objects()`

```python
# worker.py:944-946
context = self.get_serialization_context()
return context.deserialize_objects(serialized_objects, object_refs, rdt_objects)
```

#### Step 3: `serialization.deserialize_objects()` 遍历每个 object

```python
# serialization.py:559-589
for object_ref, (data, metadata, _transport) in zip(object_refs, serialized_ray_objects):
    obj = self._deserialize_object(data, metadata, object_ref, object_tensors)
    results.append(obj)
```

#### Step 4: `_deserialize_object()` 检测 metadata 中的 error type

```python
# serialization.py:429-447
metadata_fields = metadata.split(b",")
# metadata = b"11" (OUT_OF_MEMORY 的 enum 值)
# metadata_fields[0] = b"11"

try:
    error_type = int(metadata_fields[0])  # → 11 = OUT_OF_MEMORY
except Exception:
    raise Exception(...)
```

**`IsException` 的 C++ 侧检测逻辑** (`ray_object.cc:124-148`)：
```cpp
bool RayObject::IsException(rpc::ErrorType *error_type) const {
  // 性能优化：metadata >2 字符的不是 error（如 "PYTHON"）
  static_assert(ray::rpc::ErrorType_MAX < 100);
  if (metadata_ == nullptr || metadata_->Size() > 2) {
    return false;
  }
  // 检查 metadata 是否匹配某个 ErrorType 的数字字符串
  const auto error_type_descriptor = ray::rpc::ErrorType_descriptor();
  for (int i = 0; i < error_type_descriptor->value_count(); i++) {
    const auto error_type_number = error_type_descriptor->value(i)->number();
    if (metadata == std::to_string(error_type_number)) {
      if (error_type) *error_type = rpc::ErrorType(error_type_number);
      return true;
    }
  }
  return false;
}
```

#### Step 5: 根据 error_type 反序列化为对应 Python 异常

```python
# serialization.py:456-541
if error_type == ErrorType.Value("TASK_EXECUTION_EXCEPTION"):
    # Task 执行异常 → 反序列化为 RayTaskError
    obj = self._deserialize_msgpack_data(data, metadata_fields)
    return RayError.from_bytes(obj)

elif error_type == ErrorType.Value("OUT_OF_MEMORY"):
    # ★ OOM 错误 → 反序列化为 OutOfMemoryError
    error_info = self._deserialize_error_info(data, metadata_fields)
    return OutOfMemoryError(error_info.error_message)

elif ErrorType.Name(error_type).startswith("OBJECT_UNRECONSTRUCTABLE_"):
    # 血缘重建失败 → ObjectReconstructionFailedError
    return ObjectReconstructionFailedError(...)
```

**`_deserialize_error_info` 的实现**：
```python
# serialization.py:396-402
def _deserialize_error_info(self, data, metadata_fields):
    assert data
    # 从 data 中 msgpack 反序列化出 protobuf bytes
    pb_bytes = self._deserialize_msgpack_data(data, metadata_fields)
    assert pb_bytes
    # 从 protobuf bytes 解析出 RayErrorInfo
    ray_error_info = RayErrorInfo()
    ray_error_info.ParseFromString(pb_bytes)
    return ray_error_info
```

#### Step 6: `worker.get_objects()` 对 RayError 类型的处理

```python
# worker.py:1015-1023
for value in values:
    if isinstance(value, RayError):
        if isinstance(value, RayTaskError):
            raise value.as_instanceof_cause()  # ★ RayTaskError 的特殊处理
        else:
            raise value  # OutOfMemoryError 等直接抛出
```

#### Step 7: `RayTaskError.as_instanceof_cause()` 创建双异常类

当下游 task 的输出被标记为 `TASK_EXECUTION_EXCEPTION` 类型（因为 `raise_if_dependency_failed` 捕获上游 error 后重新抛出，被序列化为 `RayTaskError`），Python 端反序列化后会创建**双异常类**：

```python
# exceptions.py:245-275
def as_instanceof_cause(self):
    """Returns an exception that's an instance of the cause's class."""
    cause_cls = self.cause.__class__  # 如 OutOfMemoryError
    if issubclass(RayTaskError, cause_cls):
        return self
    try:
        return self.make_dual_exception_instance()
    except TypeError:
        return self
```

**`make_dual_exception_instance` 的核心逻辑**：

```python
# exceptions.py:164-207
def _make_normal_dual_exception_instance(self):
    cause_cls = self.cause.__class__  # 如 OutOfMemoryError
    error_msg = str(self)  # RayTaskError 的完整错误信息

    class cls(RayTaskError, cause_cls):
        # ★ 同时继承 RayTaskError 和 cause 类（如 OutOfMemoryError）
        def __init__(self, cause):
            self.cause = cause

        def __getattr__(self, name):
            return getattr(self.cause, name)  # 代理所有属性到 cause

        def __str__(self):
            return error_msg

    name = f"RayTaskError({cause_cls.__name__})"
    # 例如 "RayTaskError(OutOfMemoryError)"
    cls.__name__ = name
    cls.__qualname__ = name

    return cls(self.cause)
```

**双异常类的作用**：
- `isinstance(obj, RayTaskError)` → True（Ray 框架层面识别）
- `isinstance(obj, OutOfMemoryError)` → True（业务层面识别）
- 可以 `catch OutOfMemoryError` 或 `catch RayTaskError` 都能捕获

### 6.5 下游 task 的 error 传播链路的序列化/反序列化细节

**上游 ReadArrowJSON OOM kill**：
- C++ `MarkTaskReturnObjectsFailed` → `RayObject(OUT_OF_MEMORY, ray_error_info)`
- metadata = `"11"` (OUT_OF_MEMORY enum value)
- data = msgpack(RayErrorInfo{error_message="1 worker(s) were killed..."})

**下游 StreamingRepartition 执行前**：
- `_raylet.pyx:1835-1840`：反序列化参数 → 检测到输入是 OutOfMemoryError
- `raise_if_dependency_failed(arg)` → 直接 `raise arg`（OutOfMemoryError）
- 异常被 Ray worker 捕获 → 序列化为 `RayTaskError(cause=OutOfMemoryError)`
- metadata = `"5"` (TASK_EXECUTION_EXCEPTION enum value)
- data = pickle5(RayTaskError{cause=OutOfMemoryError, traceback, function_name})

**再下游 MapBatches(QGPreprocessMapper)**：
- 反序列化参数 → 输入是 RayTaskError(cause=OutOfMemoryError)
- `raise_if_dependency_failed(arg)` → `raise arg`（RayTaskError）
- 异常被捕获 → 序列化为新的 `RayTaskError(cause=RayTaskError(cause=OutOfMemoryError))`
- 形成三层嵌套

**Driver 端 `ray.get()` 反序列化**：
1. 反序列化最外层 → `RayTaskError(cause=RayTaskError(cause=OutOfMemoryError))`
2. `as_instanceof_cause()` → 创建 `RayTaskError(RayTaskError)` 双异常类
3. `raise value.as_instanceof_cause()`
4. 用户看到的 traceback 展示三层嵌套错误信息

### 6.6 Plasma Store 对已存在 Object 的写保护

**关键结论**：`put_in_local_plasma_callback_` **不能覆盖** plasma store 中已 sealed 的 object。

**代码链路**：

```cpp
// obj_lifecycle_mgr.cc:47-48 — plasma store 端拒绝重复创建
if (object_store_->GetObject(object_info.object_id) != nullptr) {
    return {nullptr, PlasmaError::ObjectExists};  // ← 拒绝
}
```

```cpp
// plasma_store_provider.cc:163-166 — client 端将 ObjectExists 转为 OK
else if (status.IsObjectExists()) {
    RAY_LOG_EVERY_MS(WARNING, 5000)
        << "Trying to put an object that already existed in plasma: " << object_id;
    status = Status::OK();  // 不报错，但也没有覆盖
}
```

```cpp
// plasma_store_provider.cc:106-112 — Put 方法跳过写入
// data == nullptr（因为 Create 时 ObjectExists，没有分配新 buffer）
if (data != nullptr) {
    memcpy + Seal  // 正常写入路径
} else if (object_exists) {
    *object_exists = true;  // 什么都没写，直接返回
}
```

**`MarkTaskReturnObjectsFailed` 的注释确认** (`task_manager.cc:1597-1601`)：
> In a normal time, it is no-op because the object ref values are already written, and Ray doesn't allow to overwrite values for the object ref. It is only useful when lineage reconstruction retry is failed. In this case, all these objects are lost from the plasma store, so we can overwrite them.

| 场景 | plasma 中状态 | `MarkTaskReturnObjectsFailed` 效果 |
|------|---------------|-------------------------------------|
| 正常完成后的 FailPendingTask | object 已 sealed 在 plasma 中 | **no-op**：`Create` 返回 ObjectExists → 跳过写入 |
| Lineage reconstruction 失败后 | object 已从 plasma evicted | **有效**：`Create` 成功 → 写入 error object |

---

## 7. 下游级联传播：raise_if_dependency_failed

### 7.1 下游 Task 检测输入依赖失败

当 ReadArrowJSON 的输出被标记为 OUT_OF_MEMORY error object 后，下游 task（如 StreamingRepartition）在执行前会检查输入依赖：

```python
# ray/_raylet.pyx - raise_if_dependency_failed()
# 在 task 执行前检查输入 ObjectRef 是否为 error object
def raise_if_dependency_failed(input_object_refs):
    for object_ref in input_object_refs:
        if is_error_object(object_ref):
            # 不执行业务逻辑
            # 将自身输出也标记为 error object
            raise RayTaskError("At least one of the input arguments could not be computed")
```

### 7.2 级联传播机制

```
ReadArrowJSON->SplitBlocks(7) 的输出 → OUT_OF_MEMORY error object
                ↓
StreamingRepartition 执行前检测输入 → raise_if_dependency_failed
                ↓
不执行业务逻辑，自身输出也标记为 error object：
    RayTaskError: "At least one of the input arguments for this task could not be computed:
                   ray.exceptions.OutOfMemoryError: ..."
                ↓
MapBatches(QGPreprocessMapper) 执行前检测输入 → raise_if_dependency_failed
                ↓
不执行业务逻辑，自身输出也标记为 error object：
    RayTaskError(OutOfMemoryError): "At least one of the input arguments could not be computed:
                                      ray.exceptions.RayTaskError: ..."
```

### 7.3 Driver 端 on_data_ready 的处理

```python
# physical_operator.py:232-242
def on_data_ready(self):
    try:
        ray.get(self._pending_block_ref)
        # ↑ 读取 task 返回的 block ObjectRef
        # ↑ 如果是 error object，抛出 RayTaskError
    except Exception as ex:
        raise ex from None
```

```python
# streaming_executor_state.py:471
bytes_read = task.on_data_ready()  # → 抛出异常
```

```python
# streaming_executor_state.py:501
# max_errored_blocks=unlimited 时，异常被忽略
RAY_LOG(ERROR) << "An exception was raised from a task of operator ..."
# 继续处理下一个 task
```

---

## 8. OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED 的三种触发路径

### 8.1 错误来源

```cpp
// task_manager.cc:353-365
std::optional<rpc::ErrorType> TaskManager::ResubmitTask(
    const TaskID &task_id, std::vector<ObjectID> *task_deps) {
    {
        absl::MutexLock lock(&mu_);
        auto it = submissible_tasks_.find(task_id);
        if (it == submissible_tasks_.end()) {
            // ★ task 已从 submissible_tasks_ 中被移除
            return rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED;
        }
        // ...
    }
}
```

`ResubmitTask` 被 `ObjectRecoveryManager::ReconstructObject` 调用，用于 lineage reconstruction。

### 8.2 路径 A：FailPendingTask 直接删除（你的案例中的主要路径）

**完整链路**：

```
1. Raylet OOM Kill → should_retry=false (group size=1)
2. SetWorkerFailureReason(lease_id, OUT_OF_MEMORY, should_retry=false)
3. Driver HandleGetWorkerFailureCause → fail_immediately=true
4. FailOrRetryPendingTask(fail_immediately=true)
   → 跳过 RetryTaskIfPossible
   → will_retry=false
   → FailPendingTask()
5. FailPendingTask 中:
   submissible_tasks_.erase(it)   ← task spec 被移除
   MarkTaskReturnObjectsFailed()  ← 所有输出标记为 error object
6. 后续 lineage reconstruction 尝试 ResubmitTask
   → submissible_tasks_.find(task_id) == end()
   → 返回 OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED
```

### 8.3 路径 B：reconstructable_return_ids_ 为空导致删除

task 正常完成后，如果 `reconstructable_return_ids_` 为空，task 会被从 `submissible_tasks_` 中移除：

```cpp
// task_manager.cc:1059-1066 (CompletePendingTask 中)
bool task_retryable = it->second.num_retries_left_ != 0 &&
                      !it->second.reconstructable_return_ids_.empty();
if (task_retryable) {
    // 保留 task spec 供 lineage reconstruction 使用
    release_lineage = false;
    it->second.lineage_footprint_bytes_ = it->second.spec_.GetMessage().ByteSizeLong();
    total_lineage_footprint_bytes_ += it->second.lineage_footprint_bytes_;
} else {
    submissible_tasks_.erase(it);  // ← 直接删除
}
```

**`reconstructable_return_ids_` 的生命周期**：

```cpp
// task_manager.cc:1443-1490 (RemoveLineageReference)
int64_t TaskManager::RemoveLineageReference(const ObjectID &object_id,
                                             std::vector<ObjectID> *released_objects) {
    // 当某个 plasma return object 出作用域时
    it->second.reconstructable_return_ids_.erase(object_id);

    if (it->second.reconstructable_return_ids_.empty() && !it->second.IsPending()) {
        // 所有 plasma return 都出作用域，且 task 不再 pending
        // → 释放 task 的参数引用
        // → submissible_tasks_.erase(it)   ← task spec 被移除
    }
}
```

**对于 streaming generator**：已 yield 的 block 被下游消费完后，`reconstructable_return_ids_` 逐渐清空。当最后一个 return id 出作用域时，task spec 被删除。

### 8.4 路径 C：EvictLineage 导致删除

当 `total_lineage_footprint_bytes_ > max_lineage_bytes_`（默认 1GB）时触发：

```cpp
// task_manager.cc:1067-1073 (CompletePendingTask 中)
if (total_lineage_footprint_bytes_ > max_lineage_bytes_) {
    RAY_LOG(INFO) << "Total lineage size is " << total_lineage_footprint_bytes_ / 1e6
                  << "MB, which exceeds the limit of " << max_lineage_bytes_ / 1e6
                  << "MB";
    min_lineage_bytes_to_evict =
        total_lineage_footprint_bytes_ - (max_lineage_bytes_ / 2);
}
```

EvictLineage 的执行：

```cpp
// reference_counter.cc:823-836
int64_t ReferenceCounter::EvictLineage(int64_t min_bytes_to_evict) {
    while (!reconstructable_owned_objects_.empty() &&
           lineage_bytes_evicted < min_bytes_to_evict) {
        ObjectID object_id = std::move(reconstructable_owned_objects_.front());
        reconstructable_owned_objects_.pop_front();

        auto it = object_id_refs_.find(object_id);
        lineage_bytes_evicted += ReleaseLineageReferences(it);
    }
    return lineage_bytes_evicted;
}
```

```cpp
// reference_counter.cc:569-612 (ReleaseLineageReferences)
int64_t ReferenceCounter::ReleaseLineageReferences(ReferenceTable::iterator ref) {
    // 调用 on_lineage_released_ 回调
    lineage_bytes_evicted += on_lineage_released_(ref->first, &argument_ids);
    //                    ↑ 这是 TaskManager::RemoveLineageReference

    // 标记 lineage_eligibility 为 INELIGIBLE_LINEAGE_EVICTED
    if (!ref->second.OutOfScope(lineage_pinning_enabled_) &&
        ref->second.lineage_eligibility_ == ELIGIBLE) {
        ref->second.lineage_eligibility_ =
            LineageReconstructionEligibility::INELIGIBLE_LINEAGE_EVICTED;
    }
}
```

EvictLineage 后的级联效果：
1. `RemoveLineageReference` 从 `reconstructable_return_ids_` 中移除 object
2. 如果 `reconstructable_return_ids_` 变空 → `submissible_tasks_.erase(it)`
3. 后续 reconstruction 尝试 → `OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED`

### 8.5 INELIGIBLE_LINEAGE_EVICTED 与 OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED 的关系

两者是 **不同的 ErrorType**，但都通过 `recovery_failure_callback_` 最终写入 error object，导致下游抛出异常。

#### 8.5.1 LineageReconstructionEligibility 枚举映射

```cpp
// reference_counter_interface.h
std::optional<rpc::ErrorType> ToErrorType(LineageReconstructionEligibility eligibility) {
    switch (eligibility) {
    case INELIGIBLE_LINEAGE_EVICTED:
        return rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE_LINEAGE_EVICTED;
    case INELIGIBLE_MAX_ATTEMPTS_EXCEEDED:
        return rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED;
    case INELIGIBLE_PUT:       // Put() 创建的 object 不可重建
    case INELIGIBLE_BORROWED:  // 借用的 object 不能被 non-owner 重建
    default:
        return std::nullopt;
    }
}
```

**区别**：
- `OBJECT_UNRECONSTRUCTABLE_LINEAGE_EVICTED`：task spec 因 EvictLineage 被强制淘汰，reconstruction 不可能
- `OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED`：task spec 因 FailPendingTask/reconstructable_return_ids_ 为空被删除，ResubmitTask 在 submissible_tasks_ 中找不到

**共同点**：两者都表示 **reconstruction 永久不可行**，因为 task spec 已不存在。

#### 8.5.2 recovery_failure_callback_ 的完整代码路径

**Step 1: 注册回调**（core_worker_process.cc:653-668）

```cpp
auto object_recovery_manager = std::make_unique<ObjectRecoveryManager>(
    rpc_address,
    raylet_client_pool,
    std::move(object_lookup),
    *task_manager,
    *reference_counter,
    *memory_store,
    // ★ recovery_failure_callback_ 的实现
    [this](const ObjectID &object_id, rpc::ErrorType reason, bool pin_object) {
        RAY_LOG(DEBUG).WithField(object_id)
            << "Failed to recover object due to " << rpc::ErrorType_Name(reason);
        auto core_worker = GetCoreWorker();
        // 将 error object 写入 plasma/in_memory_store
        RAY_UNUSED(core_worker->Put(RayObject(reason),
                                    /*contained_object_ids=*/{},
                                    object_id,
                                    /*pin_object=*/pin_object));
    });
```

**Step 2: 触发回调的三处位置**（object_recovery_manager.cc）

**位置 A**：lineage eligibility 不满足时（第 146-150 行）

```cpp
void ObjectRecoveryManager::ReconstructObject(const ObjectID &object_id) {
    LineageReconstructionEligibility eligibility =
        reference_counter_.GetLineageReconstructionEligibility(object_id);

    if (eligibility != LineageReconstructionEligibility::ELIGIBLE) {
        auto error_type_opt = ToErrorType(eligibility);
        rpc::ErrorType error_type = error_type_opt.value_or(rpc::ErrorType::OBJECT_LOST);
        // ★ INELIGIBLE_LINEAGE_EVICTED → OBJECT_UNRECONSTRUCTABLE_LINEAGE_EVICTED
        // ★ INELIGIBLE_MAX_ATTEMPTS_EXCEEDED → 本分支不会触发（此 eligibility 不在此处返回）
        recovery_failure_callback_(object_id, error_type, /*pin_object=*/true);
        return;
    }
    // ... ResubmitTask ...
}
```

**位置 B**：依赖 object 恢复失败时（第 170-178 行）

```cpp
// ResubmitTask 成功，但 task 的依赖 object 恢复失败
for (const auto &dep : task_deps) {
    auto error = RecoverObject(dep);
    if (error.has_value()) {
        // ★ 依赖恢复失败，传入 dep 的 error type + pin_object=false
        //   （因为 dep 可能不属于当前 owner，不能 pin）
        recovery_failure_callback_(dep, *error, /*pin_object=*/false);
    }
}
```

**位置 C**：ResubmitTask 失败时（第 180-187 行）

```cpp
// ResubmitTask 返回了 error type
// ★ 典型情况：submissible_tasks_ 中找不到 task → OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED
} else {
    RAY_LOG(INFO).WithField(object_id)
        << "Cannot recover object: " << rpc::ErrorType_Name(*error_type_optional);
    reference_counter_.UpdateObjectPendingCreation(object_id, false);
    recovery_failure_callback_(object_id,
                               *error_type_optional,
                               /*pin_object=*/true);
}
```

**Step 3: CoreWorker::Put 写入 error object**（core_worker.cc:1030-1036）

```cpp
Status CoreWorker::Put(const RayObject &object,
                       const std::vector<ObjectID> &contained_object_ids,
                       const ObjectID &object_id,
                       bool pin_object) {
    RAY_RETURN_NOT_OK(WaitForActorRegistered(contained_object_ids));
    return PutInLocalPlasmaStore(object, object_id, pin_object);
}
```

```cpp
Status CoreWorker::PutInLocalPlasmaStore(const RayObject &object,
                                         const ObjectID &object_id,
                                         bool pin_object) {
    bool object_exists = false;
    RAY_RETURN_NOT_OK(plasma_store_provider_->Put(
        object, object_id, /*owner_address=*/rpc_address_, &object_exists));
    if (!object_exists) {
        // pin 处理...
    }
    memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
                       object_id,
                       reference_counter_->HasReference(object_id));
    return Status::OK();
}
```

**关键**：`plasma_store_provider_->Put()` 内部：
- 如果 plasma 中该 object 已存在且 sealed → 返回 `ObjectExists` → `object_exists=true` → **不覆盖**
- 如果 plasma 中该 object 已丢失（如 worker 被 kill 后 plasma object 被 evict）→ Create 成功 → error object 写入 plasma

**Step 4: Python 端读取 error object 抛出异常**

当 Python 通过 `ray.get()` 读取该 error object 时：
- `deserialize_objects` 检测 metadata 中的 error type
- 根据 error type 创建对应异常类（如 `ObjectReconstructionFailedError`、`OutOfMemoryError` 等）
- 通过 `as_instanceof_cause()` 创建双异常类并 raise

#### 8.5.3 完整触发时序

```
场景 A：EvictLineage 导致 lineage 不可恢复

1. total_lineage_footprint_bytes_ > 1GB
2. EvictLineage() → ReleaseLineageReferences → RemoveLineageReference
   → submissible_tasks_.erase(task_id)
   → lineage_eligibility_ = INELIGIBLE_LINEAGE_EVICTED
3. 下游发现 object 丢失 → RecoverObject → ReconstructObject
4. GetLineageReconstructionEligibility → INELIGIBLE_LINEAGE_EVICTED
5. ToErrorType → OBJECT_UNRECONSTRUCTABLE_LINEAGE_EVICTED
6. recovery_failure_callback_(object_id, OBJECT_UNRECONSTRUCTABLE_LINEAGE_EVICTED, true)
7. CoreWorker::Put(RayObject(OBJECT_UNRECONSTRUCTABLE_LINEAGE_EVICTED), {}, object_id)
8. error object 写入 plasma / in_memory_store
9. 下游 ray.get() → 抛出 ObjectReconstructionFailedMaximumAttemptsExceededError


场景 B：FailPendingTask 导致 task spec 删除（本案例主路径）

1. OOM kill → should_retry=false → FailPendingTask
2. submissible_tasks_.erase(task_id)
3. MarkTaskReturnObjectsFailed() → OUT_OF_MEMORY error object 写入
4. 下游 raise_if_dependency_failed → 级联 error 传播
5. 后续尝试 RecoverObject → ReconstructObject → ResubmitTask
6. submissible_tasks_.find(task_id) == end()
7. ResubmitTask 返回 OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED
8. recovery_failure_callback_(object_id, OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED, true)
9. CoreWorker::Put → 写入 OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED error object
   （但 plasma 中已有 OUT_OF_MEMORY error object → ObjectExists → 不覆盖）
10. 最终 Python 端读到的仍然是第一次写入的 OUT_OF_MEMORY error
```

**注意**：场景 B 中步骤 9 试图覆盖，但 plasma store 的写保护使得 **先写入的 OUT_OF_MEMORY error 不会被后写入的 OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED 覆盖**。Python 端 `ray.get()` 抛出的是原始的 `OutOfMemoryError`，而非 `ObjectReconstructionFailedMaximumAttemptsExceededError`。

---

## 9. 完整异常链路时序图

```
┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│   Raylet     │    │   Driver     │    │  Streaming   │    │  MapBatches  │
│ (10.80.244.19│    │  CoreWorker  │    │  Repartition │    │  QGPreprocess│
│              │    │              │    │              │    │    Mapper     │
└──────┬───────┘    └──────┬───────┘    └──────┬───────┘    └──────┬───────┘
       │                   │                   │                   │
  ① memory_monitor         │                   │                   │
  检测 957GB/1007GB         │                   │                   │
  = 95.06% ≥ 95%           │                   │                   │
       │                   │                   │                   │
  ② SelectWorkersToKill    │                   │                   │
  → group size=1           │                   │                   │
  → should_retry=false     │                   │                   │
       │                   │                   │                   │
  ③ SetWorkerFailureReason │                   │                   │
  (lease_id, OOM, false)  │                   │                   │
       │                   │                   │                   │
  ④ DestroyWorker(SIGKILL) │                   │                   │
  PID=478 killed           │                   │                   │
       │                   │                   │                   │
       │  ⑤ PushTaskReply │                   │                   │
       │  status != ok     │                   │                   │
       │──────────────────→│                   │                   │
       │                   │                   │                   │
       │  ⑥ GetWorkerFailureCause RPC          │                   │
       │←─────────────────│                   │                   │
       │                   │                   │                   │
       │  ⑦ Reply:         │                   │                   │
       │  error_type=OOM   │                   │                   │
       │  fail_immediately=true│                │                   │
       │──────────────────→│                   │                   │
       │                   │                   │                   │
       │         ⑧ FailOrRetryPendingTask      │                   │
       │         fail_immediately=true          │                   │
       │         → 跳过 RetryTaskIfPossible    │                   │
       │         → will_retry=false             │                   │
       │                   │                   │                   │
       │         ⑨ FailPendingTask()           │                   │
       │         → submissible_tasks_.erase()   │                   │
       │         → MarkTaskReturnObjectsFailed()│                   │
       │         → 所有输出标记为 OOM error obj │                   │
       │                   │                   │                   │
       │                   │  ⑩ raise_if_dependency_failed        │
       │                   │──────────────────→│                   │
       │                   │  输入为 error object                  │
       │                   │  输出也标记为 error │                   │
       │                   │                   │                   │
       │                   │                   │  ⑪ raise_if_dependency_failed
       │                   │                   │──────────────────→│
       │                   │                   │  输入为 error object
       │                   │                   │  输出也标记为 error
       │                   │                   │                   │
       │                   │  ⑫ on_data_ready()│                   │
       │                   │  ray.get(block_ref)│                   │
       │                   │  → RayTaskError    │                   │
       │                   │  (OOM)             │                   │
       │                   │  → streaming_executor 忽略 (unlimited)│
       │                   │                   │                   │
       │                   │  ⑬ 后续 lineage reconstruction 尝试   │
       │                   │  ResubmitTask(task_id)                │
       │                   │  → submissible_tasks_ 中找不到       │
       │                   │  → OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED
```

---

## 10. 根因总结与修复建议

### 10.1 根因（★ 修正后）

**直接原因**：`GroupByOwnerIdWorkerKillingPolicy` 中 `should_retry` 的判断条件为
`group.GetAllWorkers().size() > 1 && group.IsRetriable()`。

**Ray Data ActorPool 默认配置下（`max_restarts=-1`）**：

actor worker 与 ReadArrowJSON 同属 Driver group → group size ≥ 2 → `should_retry=true`。
此时 OOM kill 后会进入 retry 流程（3 次 OOM retry），如果 retry 期间调度到其他内存充足的
节点，task 可以成功恢复。**但如果 3 次 OOM retry 都失败**（如同节点反复 OOM），
最终仍会 `FailPendingTask`，结果与 `should_retry=false` 相同。

**用户显式设置 `max_restarts=0` 时**：

actor worker 归入 Nil group → ReadArrowJSON 独占 Driver group → group size = 1 →
`should_retry=false` → 直接 `FailPendingTask`，无 retry 机会。

**根本原因**：
1. 节点内存压力达到 95% → OOM Killer 触发
2. killing policy 选择 ReadArrowJSON worker（内存最大的 retriable task worker）
3. 根据分组结果决定 `should_retry`：
   - 默认配置（`max_restarts=-1`）：`should_retry=true`，有 3 次 OOM retry 机会
   - 显式 `max_restarts=0`：`should_retry=false`，直接 fail
4. 如果 retry 全部失败或直接 fail：`FailPendingTask` → 从 `submissible_tasks_` 中删除 task
5. 所有输出 ObjectRef 被标记为 OUT_OF_MEMORY error object
6. 下游 StreamingRepartition、MapBatches 级联标记 error
7. 后续 lineage reconstruction 尝试恢复 → `submissible_tasks_` 中已无 task →
   `OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED`

### 10.2 为什么 spill 机制未生效

从 OOM 错误信息中可以看到：
```
Eviction Stats:
(global lru) capacity: 200000000000     ← 200GB
(global lru) used: 0.128939%            ← 仅使用 0.13%
(global lru) num objects: 631
(global lru) num evictions: 0           ← 零次驱逐
(global lru) bytes evicted: 0           ← 零字节驱逐
```

虽然 `bytes spillable: 594223183`（~594MB）是可 spill 的，但 LRU 的驱逐次数为 0。这说明 **spill 机制并未触发**。原因可能是：
- object store 的 eviction policy 只在 `used > capacity` 时触发
- 但 200GB capacity 远未达到
- 真正的内存压力来自 **worker 进程的 heap 内存**，而非 object store 内存
- object store spill 无法释放 worker heap 内存

### 10.3 修复建议

| 方案 | 具体操作 | 效果 |
|------|----------|------|
| 增加每 task CPU 请求数 | `.repartition(num_blocks).map_batches(fn, num_cpus=2)` | 降低同节点并行 task 数，减少内存压力 |
| 减小 SplitBlocks 因子 | `additional_split_factor=3` 而非 7 | 减少每个 ReadArrowJSON task 产出的 block 数和内存占用 |
| 增加节点内存 | 配置更大内存的节点 | 直接解决内存不足 |
| 调整 OOM 阈值 | `RAY_memory_usage_threshold=0.97` | 延迟 OOM kill 触发 |
| 确保 group size > 1 | 保持 Ray Data 默认 `max_restarts=-1`（不要设 0） | 让 `should_retry=true`，允许 OOM retry |
| 增加 OOM retry 次数 | `ray.init(_system_config={"num_oom_retries_default": 5})` | 更多 OOM retry 机会，提高在其他节点恢复的概率 |
| 启用 object reconstruction | `object_reconstruction_enabled=True` | 在 worker 被杀后通过 lineage reconstruction 恢复丢失的 plasma object |
| 显式设置 max_restarts | `map_batches(..., max_restarts=-1)`（默认值） | 确保 actor 可重启，归入 Driver group 参与 should_retry 判断 |

### 10.4 关键代码文件索引

| 文件 | 关键函数/逻辑 | 行号 |
|------|---------------|------|
| `src/ray/raylet/worker_killing_policy_group_by_owner.cc` | `SelectWorkersToKill`, 分组逻辑, `should_retry` 判断 | 33-172 |
| `src/ray/raylet/worker_killing_policy_group_by_owner.cc` | `Group::AddToGroup` (retriable 一致性检查) | 233-240 |
| `src/ray/raylet/worker_killing_policy_group_by_owner.cc` | `Group::GetAllWorkers`, `Group::IsRetriable` | 227, 256-258 |
| `src/ray/raylet/node_manager.cc` | `SetWorkerFailureReason`, `DestroyWorker`, `HandleGetWorkerFailureCause` | 3096-3130, 3281-3292, 717-728 |
| `src/ray/common/lease/lease_spec.cc` | `LeaseSpecification` 构造 (RAY_CHECK 类型约束) | 24-66 |
| `src/ray/common/lease/lease_spec.cc` | `IsRetriable()` | 149-157 |
| `src/ray/common/lease/lease_spec.cc` | `IsActorCreationTask()`, `IsNormalTask()` | 80-86 |
| `src/ray/common/lease/lease_spec.cc` | `MaxActorRestarts()`, `MaxRetries()` | 139-147 |
| `src/ray/common/lease/lease_spec.cc` | `ParentTaskId()` | 172-178 |
| `src/ray/common/task/task_spec.cc` | `TaskSpecification::IsRetriable()` (含 IsActorTask 判断) | 602-613 |
| `src/ray/gcs/actor/gcs_actor.h` | `GcsActor` 构造 (LeaseSpecification 创建) | 76-179 |
| `src/ray/gcs/actor/gcs_actor_scheduler.cc` | `RequestWorkerLease` (发送 LeaseSpec 到 raylet) | 261-268 |
| `src/ray/core_worker/task_submission/normal_task_submitter.cc` | `HandleGetWorkerFailureCause` | 626-704 |
| `src/ray/core_worker/task_manager.cc` | `FailOrRetryPendingTask` | 1348-1374 |
| `src/ray/core_worker/task_manager.cc` | `RetryTaskIfPossible` (OOM retry) | 1137-1215 |
| `src/ray/core_worker/task_manager.cc` | `FailPendingTask` | 1257-1330 |
| `src/ray/core_worker/task_manager.cc` | `MarkTaskReturnObjectsFailed` | 1555-1624 |
| `src/ray/core_worker/task_manager.cc` | `ResubmitTask` (UNRECONSTRUCTABLE) | 353-395 |
| `src/ray/core_worker/task_manager.cc` | `RemoveLineageReference` (reconstructable_return_ids_ 清空) | 1443-1490 |
| `src/ray/core_worker/task_manager.cc` | `CompletePendingTask` (task_retryable 判断) | 1059-1066 |
| `src/ray/core_worker/reference_counter.cc` | `EvictLineage` | 823-836 |
| `src/ray/core_worker/reference_counter.cc` | `ReleaseLineageReferences` | 569-612 |
| `src/ray/core_worker/object_recovery_manager.cc` | `ReconstructObject`, `GetLineageReconstructionEligibility`, `recovery_failure_callback_` 触发点 | 全文 |
| `src/ray/core_worker/object_recovery_manager.h` | `ObjectRecoveryFailureCallback` 类型定义 | 38-39 |
| `src/ray/core_worker/core_worker_process.cc` | `recovery_failure_callback_` 实现（Put error object） | 653-668 |
| `src/ray/core_worker/core_worker.cc` | `PutInLocalPlasmaStore`, `Put(RayObject, ..., object_id)` | 992-1036 |
| `src/ray/core_worker/reference_counter_interface.h` | `ToErrorType(LineageReconstructionEligibility)` 枚举映射 | - |
| `ray/data/_internal/execution/interfaces/physical_operator.py` | `on_data_ready`, `ray.get(_pending_block_ref)` | 232-242 |
| `ray/data/_internal/execution/streaming_executor_state.py` | `process_completed_tasks`, max_errored_blocks | 471-501 |
| `ray/_raylet.pyx` | `raise_if_dependency_failed` | - |
| `ray/exceptions.py` | `RayTaskError`, `as_instanceof_cause` | - |

---

## 11. Error Block 不会跨算子直接传播

### 11.1 之前分析的修正

之前 §7 分析认为 OOM kill 后 error object 通过 `raise_if_dependency_failed` 从 ReadArrowJSON 级联传播到 StreamingRepartition 再到 QGPreprocessMapper。**这个结论是错误的**。

**正确结论**：Ray Data 中 error block 不会从上游算子传递到下游算子。QGPreprocessMapper 的报错来自 **lineage reconstruction 阶段的级联恢复失败**，而非 task 执行阶段的直接级联传播。

### 11.2 Ray Data 的 block 流转机制

Ray Data 中每个算子的输出通过 `ObjectRefGenerator`（streaming generator）逐个 yield block + metadata。Driver 的 `streaming_executor` 通过 `ray.wait()` 检测 task 完成后，调用 `on_data_ready()` 读取输出：

```python
# physical_operator.py:174-210
def on_data_ready(self, max_bytes_to_read):
    while max_bytes_to_read is None or bytes_read < max_bytes_to_read:
        if self._pending_block_ref.is_nil():
            # 从 streaming generator 取下一个 block_ref
            self._pending_block_ref = self._streaming_gen._next_sync(timeout_s=0)
            # 正常: 返回 block ObjectRef
            # 异常: StopIteration (generator 已终止)

        if self._pending_meta_ref.is_nil():
            # 从 streaming generator 取下一个 meta_ref
            self._pending_meta_ref = self._streaming_gen._next_sync(
                timeout_s=METADATA_WAIT_TIMEOUT_S
            )
            # 正常: 返回 metadata ObjectRef
            # 异常: StopIteration → 说明 task 失败了

            # ★ 关键：StopIteration 时，block_ref 实际是 error object
            except StopIteration:
                try:
                    ray.get(self._pending_block_ref)  # ← 这里就抛异常了！
                except Exception as ex:
                    self._has_finished = True
                    raise ex from None  # ← 异常抛出 on_data_ready()

        # 正常流程：读取 metadata，产出 RefBundle
        meta = ray.get(self._pending_meta_ref, timeout=METADATA_GET_TIMEOUT_S)
        self._output_ready_callback(
            RefBundle([(self._pending_block_ref, meta)], owns_blocks=True)
        )  # ← 只有正常 block 才会放入 output_queue
```

### 11.3 streaming_executor 对 error 的处理

```python
# streaming_executor_state.py:512-551
for task in ready_tasks:
    if isinstance(task, DataOpTask):
        try:
            bytes_read = task.on_data_ready(...)  # ← 抛异常
        except Exception as e:
            errored_blocks_per_op[state] += 1
            num_errored_blocks += 1
            should_ignore = (
                max_errored_blocks < 0
                or max_errored_blocks >= num_errored_blocks
            )
            if should_ignore:
                # max_errored_blocks=-1 → 忽略，打日志继续
                logger.error(error_message, exc_info=e)
            else:
                raise e from None  # 终止执行
```

**关键**：`on_data_ready()` 抛异常后，**没有 RefBundle 被放入 output_queue**。下游算子的 `_add_input_inner()` 不会被调用，根本看不到这个 error block。

### 11.4 上游算子 error 不会传递到下游的完整证明

```
ReadArrowJSON OOM kill → streaming generator 提前终止
→ on_data_ready() → _next_sync() → StopIteration
→ ray.get(error_block_ref) → 抛 OutOfMemoryError
→ streaming_executor_state.py:521 except 捕获
→ max_errored_blocks=-1 → 忽略，打日志
→ 该 task 结束，output_queue 中没有任何 RefBundle
→ 下游 StreamingRepartition 的 add_input() 不会收到这个 block
→ error 就到此为止，不传播
```

**所以 QGPreprocessMapper 的报错不可能来自"error block 直接级联传播"，只能来自 lineage reconstruction 阶段。**

---

## 12. 级联恢复的完整代码链路

### 12.1 触发起点：节点下线检测

```cpp
// core_worker.cc:747-760
auto on_node_change = [...](const NodeID &node_id,
                             const rpc::GcsNodeAddressAndLiveness &data) {
    if (data.state() == rpc::GcsNodeInfo::DEAD) {
        RAY_LOG(INFO).WithField(node_id)
            << "Node failure. All objects pinned on that node will be lost "
               "if object reconstruction is not enabled.";
        reference_counter->ResetObjectsOnRemovedNode(node_id);
    }
};
```

```cpp
// reference_counter.cc:893-908
void ReferenceCounter::ResetObjectsOnRemovedNode(const NodeID &node_id) {
    for (auto it = object_id_refs_.begin(); it != object_id_refs_.end(); it++) {
        const auto &object_id = it->first;
        if (it->second.pinned_at_node_id_.value_or(NodeID::Nil()) == node_id ||
            it->second.spilled_node_id == node_id) {
            UnsetObjectPrimaryCopy(it);  // 清除 pinned 位置
            if (!it->second.OutOfScope(lineage_pinning_enabled_)) {
                objects_to_recover_.push_back(object_id);  // ★ 加入恢复队列
            }
        }
        RemoveObjectLocationInternal(it, node_id);
    }
}
```

### 12.2 周期性恢复驱动（每 100ms）

```cpp
// core_worker.cc:470-494
periodical_runner_->RunFnPeriodically([this] {
    const auto lost_objects = reference_counter_->FlushObjectsToRecover();
    if (!lost_objects.empty()) {
        RAY_LOG(ERROR) << ":info_message: Attempting to recover "
                       << lost_objects.size()
                       << " lost objects by resubmitting their tasks or setting "
                          "a new primary location from existing copies.";

        memory_store_->Delete(lost_objects);  // 清除 in-memory store 中的旧值

        for (const auto &object_id : lost_objects) {
            RAY_UNUSED(object_recovery_manager_->RecoverObject(object_id));
        }
    }
}, 100, "CoreWorker.RecoverObjects");
```

```cpp
// reference_counter.cc:910-915
std::vector<ObjectID> ReferenceCounter::FlushObjectsToRecover() {
    absl::MutexLock lock(&mutex_);
    std::vector<ObjectID> objects_to_recover = std::move(objects_to_recover_);
    objects_to_recover_.clear();
    return objects_to_recover;
}
```

### 12.3 RecoverObject 的入口逻辑

```cpp
// object_recovery_manager.cc:17-90
std::optional<rpc::ErrorType> ObjectRecoveryManager::RecoverObject(
    const ObjectID &object_id) {
    // 检查引用是否存在
    bool ref_exists = reference_counter_.IsPlasmaObjectPinnedOrSpilled(
        object_id, &owned_by_us, &pinned_at, &spilled);
    if (!ref_exists) {
        return rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE_REF_NOT_FOUND;
    }
    if (!owned_by_us) {
        return rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE_BORROWED;
    }

    bool requires_recovery = pinned_at.IsNil() && !spilled;
    if (requires_recovery) {
        // 注册为待恢复，防止重复恢复
        objects_pending_recovery_.insert(object_id);
    }

    if (!already_pending_recovery) {
        // 先尝试 in_memory_store_.GetAsync — 如果已有值则无需恢复
        // 否则调用 object_lookup_ 查找其他节点上的副本
        object_lookup_(object_id,
            [this](const ObjectID &id, std::vector<rpc::Address> locations) {
                PinOrReconstructObject(id, std::move(locations));
            });
    }
}
```

### 12.4 PinOrReconstructObject：尝试 pin 副本，失败则重建

```cpp
// object_recovery_manager.cc:93-100
void PinOrReconstructObject(const ObjectID &object_id,
                             std::vector<rpc::Address> locations) {
    if (!locations.empty()) {
        // ★ 路径 1：有其他节点上的副本 → pin 它
        PinExistingObjectCopy(object_id, locations.back(), ...);
    } else {
        // ★ 路径 2：没有副本 → 必须通过 lineage reconstruction 重建
        ReconstructObject(object_id);
    }
}
```

### 12.5 ReconstructObject：级联恢复的核心

```cpp
// object_recovery_manager.cc:136-187
void ReconstructObject(const ObjectID &object_id) {
    // ① 检查 lineage reconstruction 是否允许
    LineageReconstructionEligibility eligibility =
        reference_counter_.GetLineageReconstructionEligibility(object_id);

    if (eligibility != ELIGIBLE) {
        // 不允许重建 → 直接失败
        // INELIGIBLE_LINEAGE_EVICTED → OBJECT_UNRECONSTRUCTABLE_LINEAGE_EVICTED
        // INELIGIBLE_MAX_ATTEMPTS_EXCEEDED → OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED
        auto error_type = ToErrorType(eligibility);
        recovery_failure_callback_(object_id, error_type, /*pin_object=*/true);
        return;
    }

    // ② ResubmitTask：重新提交产生此 object 的 task
    const auto task_id = object_id.TaskId();
    std::vector<ObjectID> task_deps;  // ← 用于收集该 task 的所有输入依赖
    reference_counter_.UpdateObjectPendingCreation(object_id, true);
    auto error_type_optional = task_manager_.ResubmitTask(task_id, &task_deps);

    if (!error_type_optional.has_value()) {
        // ③ ★★★ 级联恢复：ResubmitTask 成功，但需要恢复该 task 的所有输入依赖
        for (const auto &dep : task_deps) {
            auto error = RecoverObject(dep);  // ← 递归！对每个依赖也执行恢复
            if (error.has_value()) {
                // 依赖也无法恢复
                // 注意 pin_object=false，因为 dep 可能不属于当前 owner
                recovery_failure_callback_(dep, *error, /*pin_object=*/false);
            }
        }
    } else {
        // task spec 不存在（已从 submissible_tasks_ 中移除）
        // 典型：FailPendingTask 已删除 / lineage 已被 EvictLineage 淘汰
        reference_counter_.UpdateObjectPendingCreation(object_id, false);
        recovery_failure_callback_(object_id, *error_type_optional, /*pin_object=*/true);
    }
}
```

### 12.6 ResubmitTask：如何收集 task 的输入依赖

```cpp
// task_manager.cc:353-411
std::optional<rpc::ErrorType> TaskManager::ResubmitTask(
    const TaskID &task_id, std::vector<ObjectID> *task_deps) {

    auto it = submissible_tasks_.find(task_id);
    if (it == submissible_tasks_.end()) {
        // ★ task 不存在 → 无法重建
        return rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED;
    }

    // ... 检查 streaming generator 状态、task 是否正在运行 ...

    // 设置 task 为重试状态
    SetupTaskEntryForResubmit(task_entry);

    // ★ 收集该 task 的所有输入依赖 ObjectID
    UpdateReferencesForResubmit(spec, task_deps);

    RAY_LOG(INFO) << "Resubmitting task that produced lost plasma object, attempt #"
                  << spec.AttemptNumber() << ": " << spec.DebugString();

    // 异步重新提交 task
    async_retry_task_callback_(spec, /*delay_ms=*/0);

    return std::nullopt;  // 成功
}
```

```cpp
// task_manager.cc:438-449
void TaskManager::UpdateReferencesForResubmit(const TaskSpecification &spec,
                                               std::vector<ObjectID> *task_deps) {
    task_deps->reserve(spec.NumArgs());
    for (size_t i = 0; i < spec.NumArgs(); i++) {
        if (spec.ArgByRef(i)) {
            task_deps->emplace_back(spec.ArgObjectId(i));  // ★ 每个 arg 的 ObjectID
        } else {
            const auto &inlined_refs = spec.ArgInlinedRefs(i);
            for (const auto &inlined_ref : inlined_refs) {
                task_deps->emplace_back(ObjectID::FromBinary(inlined_ref.object_id()));
            }
        }
    }
}
```

### 12.7 recovery_failure_callback_ 的实现

```cpp
// core_worker_process.cc:653-668
auto object_recovery_manager = std::make_unique<ObjectRecoveryManager>(
    ...,
    [this](const ObjectID &object_id, rpc::ErrorType reason, bool pin_object) {
        RAY_LOG(DEBUG).WithField(object_id)
            << "Failed to recover object due to " << rpc::ErrorType_Name(reason);
        auto core_worker = GetCoreWorker();
        // ★ 将 error object 写入 plasma / in_memory_store
        RAY_UNUSED(core_worker->Put(RayObject(reason),
                                    /*contained_object_ids=*/{},
                                    object_id,
                                    /*pin_object=*/pin_object));
    });
```

`CoreWorker::Put` → `PutInLocalPlasmaStore` → `plasma_store_provider_->Put()`:
- 如果 plasma 中该 object 已存在且 sealed → `ObjectExists` → **no-op**
- 如果 plasma 中该 object 已丢失 → Create 成功 → error object 写入

### 12.8 完整级联恢复时序图（本场景）

```
节点 10.80.244.19 下线
│
├─ ① GCS 通知 Driver CoreWorker
│   core_worker.cc:755: "Node failure. All objects pinned on that node will be lost"
│
├─ ② ResetObjectsOnRemovedNode
│   遍历所有 pinned_at == node_id 的 object
│   → StreamingRepartition 的某个输出 block_X (pinned 在该节点)
│   → ReadArrowJSON 的某个输出 block_Y (pinned 在该节点)
│   → objects_to_recover_ 加入 block_X, block_Y, ...
│
├─ ③ 100ms 后，FlushObjectsToRecover
│   memory_store_->Delete(lost_objects)
│   RecoverObject(block_X)  ← 恢复 QGPreprocessMapper 的输入
│
├─ ④ ReconstructObject(block_X)
│   → GetLineageReconstructionEligibility(block_X) → ELIGIBLE
│   → ResubmitTask(StreamingRepartition 的 task_id, &task_deps)
│      → task_deps = [block_Y_1, block_Y_2, ...]  ★ ReadArrowJSON 的输出
│      → "Resubmitting task that produced lost plasma object, attempt #N"
│      → async_retry_task_callback_ → 重新提交 StreamingRepartition task
│      → 返回 std::nullopt (成功)
│
├─ ⑤ ★ 级联：for dep in task_deps
│   RecoverObject(block_Y_1)  ← 递归恢复 ReadArrowJSON 的输出
│   │
│   ├─ ReconstructObject(block_Y_1)
│   │   → ResubmitTask(ReadArrowJSON 的 task_id, &task_deps2)
│   │   → submissible_tasks_.find(task_id) == end()  ★ 找不到！
│   │      （FailPendingTask 已删除了 task spec，因 should_retry=false）
│   │   → 返回 OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED
│   │
│   ├─ recovery_failure_callback_(block_Y_1, MAX_ATTEMPTS_EXCEEDED, false)
│   │   → CoreWorker::Put(RayObject(MAX_ATTEMPTS_EXCEEDED), {}, block_Y_1, false)
│   │   → 写入 error object 到 plasma/in_memory_store
│   │
│   RecoverObject(block_Y_2) ... 同理
│
├─ ⑥ StreamingRepartition task 被重新提交后执行
│   → 执行时反序列化输入参数
│   → ray.get(block_Y_1) → 拿到的是 error object
│   → raise_if_dependency_failed → 输出也变成 RayTaskError error
│   → QGPreprocessMapper 的输入 block_X 仍然是 error
│
└─ ⑦ QGPreprocessMapper 的 on_data_ready()
    → ray.get(block_X) → 拿到 RayTaskError(OutOfMemoryError) error
    → 抛出你看到的报错
    → streaming_executor max_errored_blocks=-1 → 忽略
```

### 12.9 级联恢复的日志证据

你之前在 Driver 日志中看到的完全符合上述级联恢复流程：

```
Resubmitting task that produced lost plasma object, attempt #4:
  task_name=ReadArrowJSON->SplitBlocks(7), attempt_number=4

Resubmitting task that produced lost plasma object, attempt #3:
  task_name=StreamingRepartition, attempt_number=3
```

- StreamingRepartition 的 attempt #3：在恢复 QGPreprocessMapper 输入时触发
- ReadArrowJSON 的 attempt #4：在恢复 StreamingRepartition 输入时**递归**触发

attempt 从 #0 到 #8 递增说明反复重试，每次因 OOM 又被杀，形成循环。

---

## 13. 定位排查步骤和日志查看方法

### 13.1 Driver 日志（最关键）

```bash
# 日志路径
ls /tmp/ray/session_latest/logs/python-core-driver-*.log

# 搜索 OOM kill 相关
grep "OUT_OF_MEMORY\|Worker failure cause\|Fail immediately\|Task failed" \
  /tmp/ray/session_latest/logs/python-core-driver-*.log

# 搜索 lineage reconstruction 相关
grep "Resubmitting task that produced lost plasma object" \
  /tmp/ray/session_latest/logs/python-core-driver-*.log

# 搜索节点下线
grep "Node failure.*All objects pinned" \
  /tmp/ray/session_latest/logs/python-core-driver-*.log

# 搜索 recovery 失败
grep "Cannot recover object\|OBJECT_UNRECONSTRUCTABLE" \
  /tmp/ray/session_latest/logs/python-core-driver-*.log

# 搜索 recovery 尝试（每 100ms 触发）
grep "Attempting to recover.*lost objects" \
  /tmp/ray/session_latest/logs/python-core-driver-*.log

# 统计 attempt 分布（判断是否反复重试）
grep "attempt_number" /tmp/ray/session_latest/logs/python-core-driver-*.log | \
  grep -oP "attempt_number=\d+" | sort | uniq -c | sort -rn
```

### 13.2 Worker 节点 raylet 日志（OOM kill 决策过程）

```bash
# 登录 OOM 发生的 worker 节点（IP 从错误信息中获取）
ls /tmp/ray/session_latest/logs/raylet.out

# 搜索 OOM kill 决策
grep "Killing\|memory_monitor\|should_retry\|OOM\|out of memory" \
  /tmp/ray/session_latest/logs/raylet.out

# 搜索 killing policy 选择过程
grep "SelectWorkersToKill\|GroupByOwnerId\|worker.*killed" \
  /tmp/ray/session_latest/logs/raylet.out
```

### 13.3 Python 应用日志（streaming_executor 的 ERROR 输出）

```bash
# 应用进程的 stdout/stderr
grep "streaming_executor_state.py.*An exception was raised" \
  /tmp/ray/session_latest/logs/worker-*.out
```

### 13.4 快速判断异常来源的决策树

```
QGPreprocessMapper 报 OutOfMemoryError
│
├─ Driver 日志有 "Resubmitting task" + "Node failure"？
│   ├─ YES → lineage reconstruction 阶段，节点下线导致 object 丢失
│   │       → 排查：哪个节点下线了？为什么？
│   │       → 进一步：级联恢复链路上哪个 task spec 找不到？
│   │         grep "OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED" driver.log
│   │
│   └─ NO → 其他原因
│
├─ Driver 日志有 "Attempting to recover N lost objects"？
│   ├─ YES → 确认有 object 丢失触发了恢复流程
│   │       检查恢复是否成功：
│   │       grep "Cannot recover\|Recovery complete" driver.log
│   └─ NO → 可能是首次执行阶段直接失败
│
├─ Driver 日志有 "Fail immediately? true" + "OUT_OF_MEMORY"？
│   ├─ YES → should_retry=false（group size=1），task 直接失败
│   │       → error 在上游算子的 on_data_ready 被捕获，不传到下游
│   │       → 但会导致 FailPendingTask 删除 task spec
│   │       → 后续级联恢复时 ResubmitTask 找不到 task → UNRECONSTRUCTABLE
│   └─ NO → should_retry=true，task 尝试重试
│
└─ Driver 日志有 "Cannot recover object: OBJECT_UNRECONSTRUCTABLE"？
    └─ YES → reconstruction 彻底失败，这是下游算子看到 error 的直接原因
```

### 13.5 你场景的完整排查路径

```
1. 确认节点下线：grep "Node failure" driver.log
   → 日志显示某节点 DEAD

2. 确认级联恢复触发：grep "Resubmitting task" driver.log
   → ReadArrowJSON attempt #0-#8
   → StreamingRepartition attempt #0-#8

3. 确认级联恢复失败：grep "Cannot recover\|UNRECONSTRUCTABLE" driver.log
   → ReadArrowJSON task spec 已从 submissible_tasks_ 中移除
   → 原因：FailPendingTask 直接删除（should_retry=false, group size=1）

4. 确认 should_retry=false：grep "Fail immediately.*true" driver.log
   → OUT_OF_MEMORY + fail_immediately=true

5. 确认 error object 写入：grep "Failed to recover object due to" driver.log
   → recovery_failure_callback_ 写入 error

6. 确认下游报错：grep "An exception was raised.*QGPreprocessMapper" driver.log
   → 最终在 Python 端看到的异常
```

### 13.6 关键代码文件索引（补充）

| 文件 | 关键函数/逻辑 | 行号 |
|------|---------------|------|
| `src/ray/core_worker/core_worker.cc` | 周期性恢复驱动 `RecoverObjects` (100ms) | 470-494 |
| `src/ray/core_worker/core_worker.cc` | 节点下线回调 `on_node_change` | 747-760 |
| `src/ray/core_worker/reference_counter.cc` | `ResetObjectsOnRemovedNode` (objects_to_recover_) | 893-908 |
| `src/ray/core_worker/reference_counter.cc` | `FlushObjectsToRecover` | 910-915 |
| `src/ray/core_worker/object_recovery_manager.cc` | `RecoverObject` 入口 | 17-90 |
| `src/ray/core_worker/object_recovery_manager.cc` | `PinOrReconstructObject` (pin 副本 or 重建) | 93-100 |
| `src/ray/core_worker/object_recovery_manager.cc` | `ReconstructObject` (级联恢复核心) | 136-187 |
| `src/ray/core_worker/task_manager.cc` | `ResubmitTask` + `UpdateReferencesForResubmit` | 353-449 |
| `src/ray/core_worker/task_manager.cc` | `SetupTaskEntryForResubmit` | 420-436 |
| `ray/data/_internal/execution/interfaces/physical_operator.py` | `on_data_ready` (error 检测点) | 174-280 |
| `ray/data/_internal/execution/streaming_executor_state.py` | `process_completed_tasks` (error 捕获) | 512-551 |
| `ray/data/_internal/execution/operators/map_operator.py` | `_add_input_inner` (下游接收输入) | 496-514 |
