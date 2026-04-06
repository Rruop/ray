# 腾讯 Ray 内部版本优化方案详细设计

> 基于知乎文章《腾讯 Ray Data 大模型推理优化实践》及 Ray 源码 v2.52.1 分析
>
> 场景: 2000+ 节点集群，4w+ actors，12w+ blocks，低优/抢占式资源

---

## 目录

- [一、Task/Actor 调度超时机制](#一tasktor-调度超时机制)
- [二、Actor 黑名单机制](#二actor-黑名单机制)
- [三、Autoscaling 重构（利用率算法优化）](#三autoscaling-重构利用率算法优化)
- [四、指数扩容机制](#四指数扩容机制)
- [五、序列化合并优化](#五序列化合并优化)
- [六、不健康算子实例自动检测](#六不健康算子实例自动检测)
- [七、算子实例 Timeout 机制](#七算子实例-timeout-机制)
- [八、集群热更新能力](#八集群热更新能力)
- [九、Head 节点与 GCS 优化](#九head-节点与-gcs-优化)
- [十、Driver 进程优化](#十driver-进程优化)

---

## 一、Task/Actor 调度超时机制

### 1.1 问题分析

在社区版中，Task/Actor 调度存在以下现有超时机制：

| 配置项 | 默认值 | 作用域 | 代码位置 |
|--------|--------|--------|----------|
| `timeout_ms_task_wait_for_death_info` | 1000ms | actor task 等待 death info | `ray_config_def.h:720` |
| `worker_lease_timeout_milliseconds` | 500ms | worker 持有其他 worker lease 的最大时间 | `ray_config_def.h:237` |
| `worker_lease_timeout_ms` | 600000ms | spillback 远程 lease 请求的 gRPC deadline | `ray_config_def.h:1039` |
| `grpc_max_ready_idle_resend_count` | 10 | READY/IDLE 状态下最大重试次数 | `ray_config_def.h:1048` |

**核心缺失**: 没有对 Task/Actor 调度阶段（PENDING_NODE_ASSIGNMENT）设置**总超时**。当 Task 处于 PENDING_NODE_ASSIGNMENT 状态时，如果集群资源不足或节点异常，Task 会永远 pending，导致整个作业卡死。

#### 现有超时机制详解（为何不能解决问题）

**1. `CheckTimeoutTasks` (`actor_task_submitter.cc:503`)**

这个函数**不是**调度超时，而是 actor task 等待"死亡信息"的超时。

触发场景：当一个 actor task 提交后，如果该 actor 已经 DEAD，Ray 会尝试获取 actor 的死亡原因（death info）来生成有意义的错误信息。`CheckTimeoutTasks` 管理的是这个"等待 death info"的超时队列。

```cpp
// actor_task_submitter.cc:503
void ActorTaskSubmitter::CheckTimeoutTasks() {
  auto now = current_time_ms();
  // 遍历 wait_for_death_info_tasks_ 队列
  // 这个队列存放的是: actor 已知 DEAD, 但还在等 GCS 返回详细死因的 task
  while (!wait_for_death_info_tasks_.empty()) {
    auto &front = wait_for_death_info_tasks_.front();
    if (now < front.deadline) break;  // 还没超时

    // 超时了: 不再等 death info，直接用通用错误标记 task 失败
    auto task_spec = std::move(front.task_spec);
    wait_for_death_info_tasks_.pop_front();
    // → 标记 task 失败，错误类型: ACTOR_DIED
    task_manager_.FailPendingTask(task_spec.TaskId(), ...);
  }
}
```

**适用阶段**: task 已经提交给 actor → actor 死亡 → 等 GCS 返回 death info
**超时时间**: `timeout_ms_task_wait_for_death_info` = 1000ms
**作用**: 防止永远等不到 death info（如 GCS 不可达时）
**不能解决的问题**: task 处于 PENDING_NODE_ASSIGNMENT 阶段，还没有 actor 接收

**2. `lease_timeout_ms_` (`normal_task_submitter.h:269`)**

这是远程 worker lease 请求的 **gRPC deadline**，不是调度总超时。

```cpp
// normal_task_submitter.h:269
int64_t lease_timeout_ms_;  // = RayConfig::worker_lease_timeout_ms() = 600000 (10分钟)
```

工作流程：
```
Driver/Worker                    本地 Raylet              远端 Raylet
    |                               |                       |
    |-- RequestWorkerLease -------->|                       |
    |   (本地请求, 无 gRPC deadline) |                       |
    |                               |-- 资源不足 ----------->|
    |                               |   spillback           |
    |<-- reply: spillback_addr -----|                       |
    |                               |                       |
    |-- RequestWorkerLease (remote, deadline=10min) ------->|
    |   ↑ 这就是 lease_timeout_ms_                          |
    |                                                       |
    |   情况A: 10分钟内远端分配了 worker → 成功              |
    |   情况B: 10分钟后 gRPC 超时 → 调用失败处理:           |
    |                                                       |
    |   失败处理 (normal_task_submitter.cc:446-483):         |
    |   spillback_retry_count_++                            |
    |   RequestNewWorkerIfNeeded(scheduling_key)  ← 重新来! |
    |   没有总超时! 永远重试!                                |
```

**关键问题**: `lease_timeout_ms_` 只控制单次 gRPC 请求的 deadline。超时后只是重试，没有总超时计数。如果集群长期资源不足：
- 每 10 分钟超时一次
- 超时后 `spillback_retry_count++` 并重新请求
- 永远循环下去

**3. `worker_lease_timeout_milliseconds` (`ray_config_def.h:237`)**

这是另一个不同的概念：当 worker A 获得了一个 lease（可以执行 task），但 worker A 想把这个 lease 转交给 worker B 时，B 必须在 500ms 内使用该 lease，否则 lease 被回收。

```cpp
// ray_config_def.h:237
RAY_CONFIG(int64_t, worker_lease_timeout_milliseconds, 500)
```

**作用**: 防止 worker 之间转交 lease 时永远不执行。纯粹的 worker-to-worker 协调超时。
**与调度无关**: 这发生在 lease 已经分配成功之后，不涉及等待资源的问题。

**4. `grpc_max_ready_idle_resend_count` (`ray_config_def.h:1048`)**

当 worker 发送 READY/IDLE 状态通知给 Raylet 但对端已 dead 时的重试保护：

```cpp
// ray_config_def.h:1048
RAY_CONFIG(int32_t, grpc_max_ready_idle_resend_count, 10)
```

**作用**: 防止 worker 向已 dead 的 Raylet 无限重发 READY 状态。10 次失败后停止重试。
**与调度超时无关**: 这是心跳/状态上报层面的保护。

#### 现有机制的覆盖范围总结

```
Task 生命周期:
                                          ← 这段没有任何超时保护! →
┌─────────┐    ┌──────────────────────┐    ┌───────────┐    ┌──────┐
│ PENDING │    │ PENDING_NODE_        │    │ SUBMITTED │    │ DONE │
│ ARGS    │───>│ ASSIGNMENT           │───>│ TO_WORKER │───>│      │
│ AVAIL   │    │ (等待资源/spillback)  │    │           │    │      │
└─────────┘    └──────────────────────┘    └───────────┘    └──────┘
                     ↑                           ↑
                     │                           │
          lease_timeout_ms: 单次                 CheckTimeoutTasks:
          gRPC 超时后重试,                        actor DEAD 后等 death info
          无总超时                                超时(1s), 标记失败
```

**现有调度流程分析**:

```
NormalTaskSubmitter                          Raylet                        GCS
      |                                       |                            |
      |-- RequestWorkerLease (local, ∞) ----->|                            |
      |                                       |-- 资源不足, spillback ------>|
      |<-- spillback_address (远端) ----------|                            |
      |                                       |                            |
      |-- RequestWorkerLease (remote, 10min)->|                            |
      |         (worker_lease_timeout_ms)     |                            |
      |                                       |                            |
      |   若远端超时/失败: 本地重试            |                            |
      |   (spillback_retry_count++)           |                            |
      |   但没有总超时! Task永远重试!          |                            |
```

关键代码路径 (`normal_task_submitter.cc:446-483`):
- 远端 lease 失败时，只会无限重试 `RequestNewWorkerIfNeeded(scheduling_key)` 而不会标记 task 失败

GCS Actor 调度同理 (`gcs_actor_scheduler.cc:519-599`):
- `HandleWorkerLeaseReply` 在失败时调用 `RetryLeasingWorkerFromNode`，无总超时限制
- `node_to_actors_when_leasing_` (`gcs_actor_scheduler.h:338`) 只在节点被检测为 DEAD 时通过 `CancelOnNode` 清理

### 1.2 设计方案

**修改层次**: Ray Core (C++) + Python 配置

**核心思路**: 在 TaskManager 和 GcsActorScheduler 中增加调度阶段的超时检测。

#### 1.2.1 Task 调度超时

**修改文件**:
- `src/ray/common/ray_config_def.h` — 新增配置项
- `src/ray/core_worker/task_manager.h` — TaskEntry 增加调度时间戳
- `src/ray/core_worker/task_submission/normal_task_submitter.h/cc` — 超时检查

**新增配置** (`ray_config_def.h`):

```cpp
/// Task 调度阶段最大超时时间（毫秒），超时后触发 spill back 或失败
/// 覆盖 PENDING_ARGS_AVAIL -> PENDING_NODE_ASSIGNMENT 全阶段
/// -1 表示禁用（兼容社区版行为）
RAY_CONFIG(int64_t, task_scheduling_timeout_ms, -1)

/// Actor 创建调度阶段最大超时时间（毫秒）
/// -1 表示禁用
RAY_CONFIG(int64_t, actor_scheduling_timeout_ms, -1)
```

**TaskEntry 增加时间戳** (`task_manager.h:518` 的 TaskEntry 结构体):

```cpp
struct TaskEntry {
  // ... 现有字段 (spec_, num_retries_left_, counter_, etc.) ...

  // 新增: 进入 PENDING_NODE_ASSIGNMENT 状态的时间戳
  int64_t scheduling_start_timestamp_ms_ = 0;

  void SetStatus(rpc::TaskStatus new_status) {
    // 现有逻辑...
    // 新增: 记录进入调度状态的时间
    if (new_status == rpc::TaskStatus::PENDING_NODE_ASSIGNMENT) {
      scheduling_start_timestamp_ms_ = current_time_ms();
    }
  }
};
```

**NormalTaskSubmitter 超时检查** (`normal_task_submitter.cc`):

```cpp
void NormalTaskSubmitter::CheckSchedulingTimeouts() {
  auto now = current_time_ms();
  auto timeout_ms = RayConfig::instance().task_scheduling_timeout_ms();
  if (timeout_ms <= 0) return;

  for (auto &[scheduling_key, sched_entry] : scheduling_key_entries_) {
    auto it = sched_entry.task_queue.begin();
    while (it != sched_entry.task_queue.end()) {
      auto &spec = *it;
      // 使用 TaskEntry 中记录的调度开始时间
      auto task_entry = task_manager_.GetTaskEntry(spec.TaskId());
      if (!task_entry) { ++it; continue; }

      auto elapsed = now - task_entry->scheduling_start_timestamp_ms_;
      if (task_entry->scheduling_start_timestamp_ms_ > 0 && elapsed > timeout_ms) {
        RAY_LOG(WARNING) << "Task " << spec.TaskId()
                         << " scheduling timeout after " << elapsed << "ms"
                         << " (scheduling_key=" << scheduling_key.DebugString() << ")"
                         << ", failing task";
        // 标记Task失败，返回 TASK_SCHEDULING_TIMEOUT 错误
        // 上层 Ray Data 框架可根据此错误做 block 级重试
        task_manager_.FailPendingTask(spec.TaskId(),
                                      rpc::ErrorType::TASK_SCHEDULING_TIMEOUT);
        it = sched_entry.task_queue.erase(it);
      } else {
        ++it;
      }
    }
  }
}
```

**驱动超时检查的定时器** (在 NormalTaskSubmitter 构造函数中注册):

```cpp
// normal_task_submitter.cc 构造函数中:
if (RayConfig::instance().task_scheduling_timeout_ms() > 0) {
  periodical_runner_.RunFnPeriodically(
      [this] { CheckSchedulingTimeouts(); },
      // 检查间隔为超时时间的 1/4，兼顾精度和开销
      std::max(1000L, RayConfig::instance().task_scheduling_timeout_ms() / 4),
      "NormalTaskSubmitter.CheckSchedulingTimeouts");
}
```

#### 1.2.2 Actor 调度超时

**修改文件**:
- `src/ray/gcs/gcs_server/gcs_actor_scheduler.h/cc`
- `src/ray/gcs/gcs_server/gcs_actor_manager.h/cc`

**在 GcsActor 中记录调度开始时间**:

```cpp
// gcs_actor.h:
class GcsActor {
  // 新增:
  int64_t scheduling_start_timestamp_ms_ = 0;
  void SetSchedulingStartTimestamp(int64_t ts) { scheduling_start_timestamp_ms_ = ts; }
  int64_t GetSchedulingStartTimestamp() const { return scheduling_start_timestamp_ms_; }
};
```

**GcsActorScheduler 超时检查** (`gcs_actor_scheduler.cc`):

```cpp
void GcsActorScheduler::CheckSchedulingTimeouts() {
  auto timeout_ms = RayConfig::instance().actor_scheduling_timeout_ms();
  if (timeout_ms <= 0) return;

  auto now = current_time_ms();
  std::vector<std::pair<NodeID, ActorID>> timed_out_entries;

  for (auto &[node_id, actor_ids] : node_to_actors_when_leasing_) {
    for (auto &actor_id : actor_ids) {
      auto actor = gcs_actor_manager_.GetRegisteredActor(actor_id);
      if (!actor) continue;

      auto start_ts = actor->GetSchedulingStartTimestamp();
      if (start_ts > 0 && now - start_ts > timeout_ms) {
        timed_out_entries.emplace_back(node_id, actor_id);
      }
    }
  }

  for (auto &[node_id, actor_id] : timed_out_entries) {
    auto actor = gcs_actor_manager_.GetRegisteredActor(actor_id);
    if (!actor) continue;

    RAY_LOG(WARNING) << "Actor " << actor_id
                     << " scheduling timeout on node " << node_id
                     << " after " << (now - actor->GetSchedulingStartTimestamp()) << "ms"
                     << ", attempting reschedule on different node";

    // 从当前节点的 leasing map 中移除
    auto iter = node_to_actors_when_leasing_.find(node_id);
    if (iter != node_to_actors_when_leasing_.end()) {
      iter->second.erase(actor_id);
      if (iter->second.empty()) {
        node_to_actors_when_leasing_.erase(iter);
      }
    }

    // 触发失败处理: 重新调度到其他节点
    schedule_failure_handler_(std::move(actor),
                              rpc::RequestWorkerLeaseReply::SCHEDULING_TIMEOUT,
                              "Actor scheduling timeout");
  }
}
```

**在 `Schedule` 方法中记录时间戳** (`gcs_actor_scheduler.cc:49`):

```cpp
void GcsActorScheduler::Schedule(std::shared_ptr<GcsActor> actor) {
  // ... 现有节点选择逻辑 ...
  actor->SetSchedulingStartTimestamp(current_time_ms());  // 新增
  RAY_CHECK(node_to_actors_when_leasing_[actor->GetNodeID()]
                .emplace(actor->GetActorID())
                .second);
  LeaseWorkerFromNode(actor, node.value());
}
```

### 1.3 Object 血缘回溯超时

**问题**: `ObjectRecoveryManager::RecoverObject()` (`object_recovery_manager.h:41`) 通过递归调用 `ReconstructObject` → `RecoverObject(dep)` 回溯依赖链，没有总超时机制。深层 DAG（21个算子链）中，递归回溯可能无限等待。

从代码看 (`object_recovery_manager.cc:140-188`)，`ReconstructObject` 会递归恢复所有 `task_deps`，每个 dep 又可能触发其自身的依赖恢复。

**修改文件**:
- `src/ray/core_worker/object_recovery_manager.h/cc`
- `src/ray/common/ray_config_def.h`

**设计**:

```cpp
// ray_config_def.h:
RAY_CONFIG(int64_t, object_lineage_reconstruction_timeout_ms, 300000)  // 5分钟

// object_recovery_manager.h 新增:
class ObjectRecoveryManager {
  // 新增: 记录每个 object 开始 recovery 的时间
  absl::flat_hash_map<ObjectID, int64_t> recovery_start_times_
      ABSL_GUARDED_BY(objects_pending_recovery_mu_);

  // 新增: 周期性检查 recovery 超时
  void CheckRecoveryTimeouts();
};

// object_recovery_manager.cc:
void ObjectRecoveryManager::CheckRecoveryTimeouts() {
  absl::MutexLock lock(&objects_pending_recovery_mu_);
  auto now = current_time_ms();
  auto timeout = RayConfig::instance().object_lineage_reconstruction_timeout_ms();
  if (timeout <= 0) return;

  std::vector<ObjectID> timed_out;
  for (auto &[obj_id, start_time] : recovery_start_times_) {
    if (now - start_time > timeout) {
      timed_out.push_back(obj_id);
    }
  }

  for (auto &obj_id : timed_out) {
    RAY_LOG(WARNING) << "Object " << obj_id
                     << " lineage reconstruction timeout after " << timeout << "ms";
    objects_pending_recovery_.erase(obj_id);
    recovery_start_times_.erase(obj_id);
    // 触发上层兜底: 回调通知 Ray Data 框架层进行 block 级重试
    recovery_failure_callback_(obj_id,
                                rpc::ErrorType::OBJECT_RECONSTRUCTION_TIMEOUT,
                                /*pin_object=*/false);
  }
}
```

在 `RecoverObject` 中记录开始时间 (`object_recovery_manager.cc:24-91`):

```cpp
std::optional<rpc::ErrorType> ObjectRecoveryManager::RecoverObject(
    const ObjectID &object_id) {
  // ... 现有检查 ...
  if (!already_pending_recovery) {
    // 新增: 记录恢复开始时间
    recovery_start_times_[object_id] = current_time_ms();
    // ... 现有恢复逻辑 ...
  }
  return std::nullopt;
}
```

---

## 二、Actor 黑名单机制

### 2.1 问题分析

当前 Ray Data 的 `ActorPoolMapOperator` 中:
- `_start_actor()` (`actor_pool_map_operator.py:298`) 使用 `max_restarts=-1`（无限重启）
- `_task_done_callback` (`actor_pool_map_operator.py:325`) 只执行 `pending_to_running` 状态转换
- `_try_schedule_tasks_internal` (`actor_pool_map_operator.py:361`) 中 task 完成回调仅调用 `on_task_completed`

如果某个 actor 反复失败（如特定节点 GPU 故障），actor 会被无限重启，持续接收和丢失 task 数据。`refresh_actor_state()` (`actor_pool_map_operator.py:1207`) 只检测 ALIVE/RESTARTING/DEAD 状态，不追踪失败模式。

### 2.2 设计方案

**修改文件**:
- `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py`

**核心设计**: 在 `_ActorPool` 中新增黑名单管理:

```python
# actor_pool_map_operator.py 中 _ActorPool 类新增 (基于 line 892 的类定义):

class _ActorPool(AutoscalingActorPool):
    # 新增配置常量
    _ACTOR_BLACKLIST_THRESHOLD = 3      # 连续失败N次加入黑名单
    _ACTOR_BLACKLIST_COOLDOWN_S = 300   # 黑名单冷却时间(秒)

    def __init__(self, ...):
        # ... 现有初始化 (lines 904-971) ...

        # 新增: 黑名单相关状态
        self._actor_failure_counts: Dict[ray.actor.ActorHandle, int] = {}
        self._blacklisted_actors: Dict[ray.actor.ActorHandle, float] = {}
        # actor -> 加入黑名单时间

    def on_task_failed(self, actor: ray.actor.ActorHandle, error: Exception):
        """当 actor 上的 task 失败时调用"""
        count = self._actor_failure_counts.get(actor, 0) + 1
        self._actor_failure_counts[actor] = count

        if count >= self._ACTOR_BLACKLIST_THRESHOLD:
            logical_id = self._actor_to_logical_id.get(actor, "unknown")
            logger.warning(
                f"Actor {logical_id} has failed {count} consecutive tasks, "
                f"adding to blacklist for {self._ACTOR_BLACKLIST_COOLDOWN_S}s"
            )
            self._blacklisted_actors[actor] = time.time()
            # 不再向该 actor 分配新 task
            # 触发扩容补偿: 创建替代 actor
            self.scale(ActorPoolScalingRequest.upscale(
                delta=1,
                reason=f"replacing blacklisted actor {logical_id}"
            ))

    def on_task_succeeded(self, actor: ray.actor.ActorHandle):
        """当 actor 上的 task 成功时，重置失败计数"""
        self._actor_failure_counts[actor] = 0

    def _is_blacklisted(self, actor: ray.actor.ActorHandle) -> bool:
        """检查 actor 是否在黑名单中"""
        if actor not in self._blacklisted_actors:
            return False
        # 检查冷却期
        elapsed = time.time() - self._blacklisted_actors[actor]
        if elapsed > self._ACTOR_BLACKLIST_COOLDOWN_S:
            # 冷却期结束，移出黑名单
            del self._blacklisted_actors[actor]
            self._actor_failure_counts[actor] = 0
            logger.info(
                f"Actor {self._actor_to_logical_id.get(actor, 'unknown')} "
                f"removed from blacklist after cooldown"
            )
            return False
        return True
```

**修改 `_ActorTaskSelector` 的 actor 选择逻辑**:

当前 `select_actors()` 方法通过 `_actor_pool` 获取可用 actor 列表。需要在此处过滤黑名单 actor:

```python
# 在 _ActorPool 中重写获取可调度 actor 的方法:

def get_schedulable_actors(self) -> List[ray.actor.ActorHandle]:
    """返回可以接收新 task 的 actor 列表（排除黑名单）"""
    schedulable = []
    for actor, state in self._running_actors.items():
        if (state.num_tasks_in_flight < self._max_tasks_in_flight
            and not state.is_restarting
            and not self._is_blacklisted(actor)):
            schedulable.append(actor)
    return schedulable
```

**在 `_try_schedule_tasks_internal` 中集成回调** (`actor_pool_map_operator.py:361`):

```python
def _try_schedule_tasks_internal(self, strict: bool) -> int:
    num_submitted_tasks = 0
    for bundle, actor in self._actor_task_selector.select_actors(...):
        # ... 现有提交逻辑 ...

        def _task_done_callback(actor_to_return, success: bool = True,
                                error: Optional[Exception] = None):
            self._actor_pool.on_task_completed(actor_to_return)
            if success:
                self._actor_pool.on_task_succeeded(actor_to_return)
            else:
                self._actor_pool.on_task_failed(actor_to_return, error)

        # 修改 _submit_data_task 以传递成功/失败信息
        self._submit_data_task(
            gen, bundle,
            partial(_task_done_callback, actor_to_return=actor)
        )
        num_submitted_tasks += 1
    return num_submitted_tasks
```

---

## 三、Autoscaling 重构（利用率算法优化）

### 3.1 问题分析

社区版 `DefaultResizingPolicy` (`actor_pool_resizing_policy.py:46`) 的核心问题：

| 问题 | 代码位置 | 影响 |
|------|----------|------|
| 利用率计算忽略 pending actors | `autoscaling_actor_pool.py` 的 `get_pool_util()` 只用 `num_running_actors()` 作分母 | 新 actor 启动期间(模型加载可达数分钟)持续过度扩容 |
| 缩容只能每次减1 | `actor_pool_resizing_policy.py:84` 的 `compute_downscale_delta` 固定返回 1 | 大规模场景缩容极慢，数百 actor 需要数百轮调度 |
| upscaling 阈值固定 1.75 | `context.py:271` 的 `DEFAULT_ACTOR_POOL_UTIL_UPSCALING_THRESHOLD = 1.75` | 对某些场景过于保守 |
| max_upscaling_delta 默认 1 | `context.py:281` 的 `DEFAULT_ACTOR_POOL_MAX_UPSCALING_DELTA = 1` | 每轮调度最多新增1个actor，冷启动极慢 |

**现有利用率计算** (`autoscaling_actor_pool.py` 和 `actor_pool_map_operator.py:1516`):

```python
def get_pool_util(self) -> float:
    if self.num_running_actors() == 0:
        return float("inf")  # 或 0.0 (取决于子类)
    return self.num_tasks_in_flight() / (
        self._max_actor_concurrency * self.num_running_actors()
    )
    # 注: num_pending_actors() 完全未被使用!
```

**现有扩容决策** (`default_actor_autoscaler.py:98-210` 的 `_derive_target_scaling_config`):
- 当 `util >= upscaling_threshold` 且 pool 未达 max_size 时触发扩容
- 已有对 pending actors 的检查: 如果 `num_pending_actors() > 0`，不做额外扩容
  - 但这个检查过于粗糙: 即使有1个 pending actor 也完全阻止扩容

### 3.2 设计方案

**修改文件**:
- `python/ray/data/_internal/actor_autoscaler/autoscaling_actor_pool.py`
- `python/ray/data/_internal/actor_autoscaler/actor_pool_resizing_policy.py`
- `python/ray/data/_internal/actor_autoscaler/default_actor_autoscaler.py`

#### 3.2.1 利用率计算优化

```python
# autoscaling_actor_pool.py 修改 get_pool_util:

class AutoscalingActorPool(ABC):
    def get_pool_util(self) -> float:
        """计算利用率，将 pending actors 纳入分母（加权）

        设计考量:
        - pending actors 尚不能真正处理 task，不能等价于 running actors
        - 但它们即将 ready，应该抑制进一步扩容
        - 使用 0.5 权重: pending actor 贡献 50% 的有效容量
        """
        running = self.num_running_actors()
        pending = self.num_pending_actors()

        if running == 0 and pending == 0:
            return float("inf")

        # effective_capacity: running 全权重 + pending 半权重
        effective_capacity = running + pending * 0.5
        if effective_capacity == 0:
            return float("inf")

        return self.num_tasks_in_flight() / (
            self.max_actor_concurrency() * effective_capacity
        )
```

#### 3.2.2 更激进的扩容策略

```python
# actor_pool_resizing_policy.py 新增:

class AggressiveResizingPolicy(ActorPoolResizingPolicy):
    """更激进的扩缩容策略，适用于大规模场景

    改进点:
    1. 扩容时考虑 pending actors 的衰减效应
    2. 缩容支持批量（不再固定为1）
    3. 利用率驱动 + pending感知 的综合决策
    """

    def __init__(self, upscaling_threshold: float, max_upscaling_delta: int,
                 downscale_batch_size: int = 1):
        self._upscaling_threshold = upscaling_threshold
        self._max_upscaling_delta = max_upscaling_delta
        self._downscale_batch_size = downscale_batch_size

    def compute_upscale_delta(self, actor_pool, util) -> int:
        current = actor_pool.current_size()
        pending = actor_pool.num_pending_actors()

        # 如果有 pending actors，按比例衰减扩容需求
        # 避免 pending 阶段的过度扩容
        if pending > 0:
            decay_factor = max(0.3, 1.0 - pending / max(current, 1))
        else:
            decay_factor = 1.0

        plan_delta = math.ceil(
            current * (util / self._upscaling_threshold - 1) * decay_factor
        )

        limits = [
            self._max_upscaling_delta,
            actor_pool.max_size() - current,
        ]
        delta = min(plan_delta, *limits)
        return max(1, delta)

    def compute_downscale_delta(self, actor_pool) -> int:
        """批量缩容: 基于空闲 actor 数量"""
        # num_free_task_slots / max_tasks_per_actor = 空闲 actor 等价数
        idle_equiv = actor_pool.num_free_task_slots() // max(
            actor_pool.max_tasks_in_flight_per_actor(), 1)
        return min(self._downscale_batch_size, max(1, idle_equiv))
```

#### 3.2.3 缩容优化: 解决 actor 因血缘/GC 长时间无法销毁

```python
# actor_pool_map_operator.py 中 _ActorPool 增加强制缩容:

class _ActorPool(AutoscalingActorPool):
    _ACTOR_SCALE_DOWN_FORCE_TIMEOUT_S = 60  # 缩容超时强制释放

    def __init__(self, ...):
        # ... 现有初始化 ...
        self._scale_down_pending_since: Optional[float] = None

    def scale(self, req: ActorPoolScalingRequest) -> Optional[int]:
        # ... 现有逻辑 (lines 1050-1117) ...

        if req.delta < 0:
            # 原逻辑: _remove_inactive_actor
            num_released = ...

            if self._pending_scale_down_count > 0:
                if self._scale_down_pending_since is None:
                    self._scale_down_pending_since = time.time()
                elif (time.time() - self._scale_down_pending_since >
                      self._ACTOR_SCALE_DOWN_FORCE_TIMEOUT_S):
                    # 强制释放: kill actor 即使还有 in-flight tasks
                    logger.warning(
                        f"Force releasing {self._pending_scale_down_count} actors "
                        f"after {self._ACTOR_SCALE_DOWN_FORCE_TIMEOUT_S}s pending"
                    )
                    self._force_release_pending_scale_down()
                    self._scale_down_pending_since = None
            else:
                self._scale_down_pending_since = None

    def _force_release_pending_scale_down(self):
        """强制释放无法正常缩容的 actor"""
        count = self._pending_scale_down_count
        for _ in range(count):
            # 选择 in-flight tasks 最少的 actor 强制 kill
            actor = min(
                self._running_actors,
                key=lambda a: self._running_actors[a].num_tasks_in_flight
            )
            ray.kill(actor, no_restart=True)
            self._remove_actor(actor)
        self._pending_scale_down_count = 0
```

---

## 四、指数扩容机制

### 4.1 问题分析

社区版 `DefaultResizingPolicy.compute_upscale_delta` 的 `max_upscaling_delta` 默认为 1 (`context.py:281`)，即每轮调度循环最多新增 1 个 actor。

调度循环频率（`streaming_executor.py:455` 的 while 循环）约每 0.1-1s 一轮。启动 100 个 actor 需要 100 轮循环 = 100+ 秒，这还不包括 actor 实际启动时间。

在大规模推理场景（如需要启动数百个 GPU 推理 actor），这个线性冷启动行为严重制约吞吐。

### 4.2 设计方案

**修改文件**:
- `python/ray/data/_internal/actor_autoscaler/actor_pool_resizing_policy.py`
- `python/ray/data/context.py`

**新增策略类**:

```python
# actor_pool_resizing_policy.py 新增:

class ExponentialResizingPolicy(ActorPoolResizingPolicy):
    """指数级扩缩容策略

    扩容时采用指数增长: 1, 2, 4, 8, 16, ...
    直到达到 max_size 或资源预算上限。
    缩容时按比例批量缩减。

    适用场景:
    - 大规模推理 actor pool（100+ actors）
    - GPU 密集型算子需要快速填满资源
    - 数据量已知较大，不需要保守试探
    """

    def __init__(self, upscaling_threshold: float,
                 initial_scale_delta: int = 1,
                 exponential_base: float = 2.0,
                 max_upscaling_delta: int = 64):
        self._upscaling_threshold = upscaling_threshold
        self._initial_scale_delta = initial_scale_delta
        self._exponential_base = exponential_base
        self._max_upscaling_delta = max_upscaling_delta
        self._consecutive_upscale_count = 0

    def compute_upscale_delta(self, actor_pool, util) -> int:
        """指数增长的扩容量

        连续扩容的次数越多，每次扩容的量指数增长:
        第1次: initial_scale_delta (默认1)
        第2次: initial_scale_delta * base (默认2)
        第3次: initial_scale_delta * base^2 (默认4)
        ...直到 max_upscaling_delta

        设计考量:
        - 首次扩容保守（探测资源可用性）
        - 连续需要扩容说明资源缺口大，加速填充
        - 一旦利用率回归正常，重置指数计数
        """
        self._consecutive_upscale_count += 1

        # 指数计算
        exp_delta = math.ceil(
            self._initial_scale_delta *
            (self._exponential_base ** (self._consecutive_upscale_count - 1))
        )

        # 同时考虑利用率驱动的 delta
        util_driven_delta = math.ceil(
            actor_pool.current_size() * (util / self._upscaling_threshold - 1)
        )

        # 取两者的较大值: 指数增长 OR 利用率需求
        plan_delta = max(exp_delta, util_driven_delta)

        # 应用上限
        remaining_to_max = actor_pool.max_size() - actor_pool.current_size()
        delta = min(plan_delta, self._max_upscaling_delta, remaining_to_max)

        return max(1, delta)

    def compute_downscale_delta(self, actor_pool) -> int:
        # 缩容时重置连续扩容计数
        self._consecutive_upscale_count = 0
        # 按当前规模的 10% 缩容，至少1个
        return max(1, actor_pool.current_size() // 10)

    def on_no_scaling_needed(self):
        """当利用率在正常范围内，不需要扩缩容时调用

        逐步衰减连续扩容计数，但不完全重置
        这样如果短暂稳定后又需要扩容，不需要从1重新开始
        """
        self._consecutive_upscale_count = max(
            0, self._consecutive_upscale_count - 1
        )
```

**在 AutoscalingConfig 中新增配置** (`context.py`):

```python
# context.py 新增:

DEFAULT_ACTOR_POOL_RESIZING_POLICY: str = env_string(
    "RAY_DATA_ACTOR_POOL_RESIZING_POLICY", "default"
)  # "default" | "exponential" | "aggressive"

DEFAULT_ACTOR_POOL_EXPONENTIAL_BASE: float = env_float(
    "RAY_DATA_ACTOR_POOL_EXPONENTIAL_BASE", 2.0
)

@dataclass
class AutoscalingConfig:
    # ... 现有字段 (lines 346-383) ...
    resizing_policy: str = DEFAULT_ACTOR_POOL_RESIZING_POLICY
    exponential_base: float = DEFAULT_ACTOR_POOL_EXPONENTIAL_BASE
```

**在 DefaultActorAutoscaler 中根据配置选择策略**:

```python
# default_actor_autoscaler.py __init__ 中:
def __init__(self, ...):
    # ...
    if config.resizing_policy == "exponential":
        self._actor_pool_resizing_policy = ExponentialResizingPolicy(
            upscaling_threshold=config.actor_pool_util_upscaling_threshold,
            exponential_base=config.exponential_base,
            max_upscaling_delta=config.actor_pool_max_upscaling_delta,
        )
    elif config.resizing_policy == "aggressive":
        self._actor_pool_resizing_policy = AggressiveResizingPolicy(
            upscaling_threshold=config.actor_pool_util_upscaling_threshold,
            max_upscaling_delta=config.actor_pool_max_upscaling_delta,
            downscale_batch_size=max(1, config.actor_pool_max_upscaling_delta // 4),
        )
    else:
        self._actor_pool_resizing_policy = DefaultResizingPolicy(
            upscaling_threshold=config.actor_pool_util_upscaling_threshold,
            max_upscaling_delta=config.actor_pool_max_upscaling_delta,
        )
```

**扩容行为对比**:

| 轮次 | Default (delta=1) | Exponential (base=2) | Aggressive (max=8) |
|------|-------------------|----------------------|--------------------|
| 1    | +1                | +1                   | +8 (利用率驱动)     |
| 2    | +1                | +2                   | +8                  |
| 3    | +1                | +4                   | +8                  |
| 4    | +1                | +8                   | +8                  |
| 5    | +1                | +16                  | +8                  |
| **累计** | **5** actors  | **31** actors        | **40** actors      |

---

## 五、序列化合并优化

### 5.1 问题分析

在 `ActorPoolMapOperator.start()` (`actor_pool_map_operator.py:248`) 中，调用 `scale(initial_size)` 触发批量 actor 创建。每个 actor 通过 `_start_actor()` (`actor_pool_map_operator.py:298`) 创建时:

```python
# 当前代码 (line 314-322):
actor = self._actor_cls.options(...).remote(
    ctx=ctx,                              # DataContext 对象
    logical_actor_id=logical_actor_id,
    src_fn_name=self.name,
    map_transformer=self._map_transformer,  # UDF 对象 (可能很大)
    actor_location_tracker=get_or_create_actor_location_tracker(),
)
```

Ray 的 `.remote()` 调用会对每个参数进行 pickle 序列化。当 `initial_size=100` 时，`self._map_transformer`（包含用户 UDF、模型加载逻辑等）被序列化 100 次。

**社区版的部分优化** (line 253-256):
```python
# Trigger the large UDF warning check by accessing the property.
_ = self._map_transformer_ref
```

这只是触发了大 UDF 的告警检查（`_map_transformer_ref` property），但没有实际将序列化结果复用。

### 5.2 设计方案

**修改文件**:
- `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py`

**核心思路**: 利用 `ray.put()` 预先将大对象放入 Object Store（仅序列化一次），后续 actor 创建时传递 `ObjectRef`。

#### 5.2.1 ObjectRef 参数传递机制详解

当 `.remote()` 被调用时，Ray 对参数的处理路径取决于参数类型（`python/ray/_raylet.pyx:751` 的 `prepare_args_internal`）：

```
.remote(arg1=普通对象, arg2=ObjectRef)
   │
   ├── arg1 是普通对象 (DataContext, MapTransformer 等)
   │   └── serialize(arg1)          ← 每次调用都执行! (pickle + plasma 写入)
   │       → 小对象 (<100KB): inline 到 TaskSpec 中 (CTaskArgByValue)
   │       → 大对象 (>=100KB): 自动 ray.put() → CTaskArgByReference
   │       ※ 即使多个 .remote() 传同一个对象，每次都重新序列化!
   │
   └── arg2 是 ObjectRef
       └── CTaskArgByReference(arg2.id())  ← 仅传递 ID, 零序列化开销!
           Worker 端通过 LocalDependencyResolver 自动 resolve
```

**底层实现** (`_raylet.pyx:751`):

```python
# 简化的 prepare_args_internal 逻辑:
def prepare_args_internal(args):
    for arg in args:
        if isinstance(arg, ObjectRef):
            # 直接用引用, 不序列化
            task_args.append(CTaskArgByReference(arg.binary_id()))
        else:
            # 需要序列化!
            serialized = worker.get_serialization_context().serialize(arg)
            if serialized.total_bytes < MAX_INLINE_SIZE:
                task_args.append(CTaskArgByValue(serialized))
            else:
                # 大对象: 隐式 ray.put (仍然是序列化!)
                ref = worker.put_serialized(serialized)
                task_args.append(CTaskArgByReference(ref.binary_id()))
```

**Worker 端的自动 resolve**:

当 worker 收到 task 参数中的 `CTaskArgByReference` 时，`LocalDependencyResolver` 自动从 Object Store 获取数据：

```
Worker 接收 task 参数:
├── CTaskArgByValue (inline)  → 直接反序列化
└── CTaskArgByReference (ref) → Object Store get → 反序列化
    │
    ├── 同节点: plasma mmap 零拷贝访问 (只需反序列化, 不需网络传输)
    └── 跨节点: Object Store pull → 本地 plasma → 反序列化
```

**关键结论**: Ray 的 `.remote()` 调用会**自动将 ObjectRef 参数 resolve 为实际对象传给函数**。即用户函数收到的是真实对象（DataContext / MapTransformer），不是 ObjectRef。

**因此 `_MapWorker.__init__` 中的 `isinstance(ctx, ray.ObjectRef)` 检查实际上不需要**——Ray 框架已经自动 resolve 了。但为了防御性编程和向后兼容，保留该检查无害。

#### 5.2.2 为什么 `ray.put()` + ObjectRef 能避免重复序列化

```
场景: 启动 100 个 actor, map_transformer = 50MB 的 UDF 对象

社区版 (直接传对象):
┌─────────────────────────────────────────────────────────┐
│ Driver 进程                                              │
│                                                         │
│ for i in range(100):                                    │
│   actor.remote(map_transformer=self._map_transformer)   │
│                                                         │
│   每次 .remote() 调用:                                   │
│   ├── serialize(map_transformer)   ← 50MB pickle, ~200ms │
│   ├── plasma_put(serialized_bytes) ← 50MB 写入          │
│   └── 生成新的 ObjectRef                                  │
│                                                         │
│ 总计: 100 次序列化 × 200ms = 20秒 Driver CPU 被占用!     │
│ Object Store: 100 份 50MB 副本 = 5GB                    │
└─────────────────────────────────────────────────────────┘

优化后 (ray.put + 传 ObjectRef):
┌─────────────────────────────────────────────────────────┐
│ Driver 进程                                              │
│                                                         │
│ # 只序列化一次                                           │
│ ref = ray.put(self._map_transformer)   ← 50MB, 一次性    │
│                                                         │
│ for i in range(100):                                    │
│   actor.remote(map_transformer=ref)    ← 传 ObjectRef   │
│                                                         │
│   每次 .remote() 调用:                                   │
│   └── CTaskArgByReference(ref.id)  ← 仅32字节ID, ~0ms   │
│                                                         │
│ 总计: 1 次序列化 × 200ms = 200ms                         │
│ Object Store: 1 份 50MB + 引用计数 = 50MB               │
└─────────────────────────────────────────────────────────┘
```

**节省的不只是 CPU 时间，还有 Object Store 空间**: 社区版中，即使 Ray 对大对象做隐式 `ray.put()`，每次 `.remote()` 都会生成**不同的** ObjectRef（因为每次序列化结果是独立的 plasma 对象）。而显式 `ray.put()` 一次，100 个 actor 共享**同一个** ObjectRef。

#### 5.2.3 同节点零拷贝机制

当多个 actor 调度到同一节点时，它们 `ray.get(同一个 ObjectRef)` 的行为：

```
Node A 的 Object Store (Plasma, 共享内存):
┌──────────────────────────────────┐
│ ObjectRef_123 → [50MB 序列化数据] │  ← 只有一份!
└──────────────────────────────────┘
         ↑ mmap   ↑ mmap   ↑ mmap
     Actor_1   Actor_2   Actor_3
     (进程1)   (进程2)   (进程3)

每个 Actor 的 ray.get(ObjectRef_123):
├── 检查本地 Plasma → 找到!
├── mmap 映射到进程地址空间 ← 零拷贝! 无内存复制!
└── deserialize(mmap_ptr)  ← 反序列化仍需执行(CPU开销)
    但不需要网络传输, 不需要内存拷贝
```

**注意**: 反序列化仍然每个 actor 执行一次（因为 Python 对象需要在各自进程空间构造）。优化的是 Driver 端的序列化和 Object Store 空间，不是 Worker 端的反序列化。

```python
# actor_pool_map_operator.py:

class ActorPoolMapOperator(MapOperator):
    def start(self, options: ExecutionOptions):
        self._actor_locality_enabled = options.actor_locality_enabled
        super().start(options)

        self._actor_cls = ray.remote(**self._ray_remote_args)(self._map_worker_cls)
        _ = self._map_transformer_ref

        # === 新增: 预序列化合并 ===
        # 将 map_transformer 和 data_context 只序列化一次放入 Object Store
        # 后续所有 actor 创建共享同一份序列化数据
        self._map_transformer_obj_ref = ray.put(self._map_transformer)
        self._data_context_obj_ref = ray.put(self.data_context)
        # ========================

        self._actor_pool.scale(
            ActorPoolScalingRequest(
                delta=self._actor_pool.initial_size(),
                reason="scaling to initial size"
            )
        )
        # ... 后续 wait_for_min_actors_s 逻辑不变 ...

    def _start_actor(self, labels: Dict[str, str], logical_actor_id: str
    ) -> Tuple[ActorHandle, ObjectRef]:
        assert self._actor_cls is not None
        if self._ray_remote_args_fn:
            self._refresh_actor_cls()

        actor = self._actor_cls.options(
            _labels={self._OPERATOR_ID_LABEL_KEY: self.id, **labels}
        ).remote(
            # === 修改: 传递 ObjectRef 而非原始对象 ===
            ctx=self._data_context_obj_ref,            # ObjectRef (≈ 零拷贝)
            logical_actor_id=logical_actor_id,
            src_fn_name=self.name,
            map_transformer=self._map_transformer_obj_ref,  # ObjectRef
            actor_location_tracker=get_or_create_actor_location_tracker(),
        )
        res_ref = actor.get_location.remote()
        # ... 后续 callback 不变 ...
        return actor, res_ref
```

**修改 `_MapWorker.__init__`** (lines 643-674) — 无需修改:

```python
class _MapWorker:
    def __init__(
        self,
        ctx: DataContext,          # Ray 自动 resolve ObjectRef → DataContext
        src_fn_name: str,
        map_transformer: MapTransformer,  # Ray 自动 resolve ObjectRef → MapTransformer
        logical_actor_id: str,
        actor_location_tracker: ray.actor.ActorHandle,
    ):
        # 无需 isinstance(ctx, ray.ObjectRef) 检查!
        # Ray 的 LocalDependencyResolver 在调用 __init__ 之前
        # 已经自动将 CTaskArgByReference resolve 为实际对象
        #
        # 即: 传入 ObjectRef 时, Worker 收到的已经是 ray.get(ref) 的结果
        # 函数签名中的类型标注保持不变, 行为完全兼容
        self.src_fn_name = src_fn_name
        self._map_transformer = map_transformer
        DataContext._set_current(ctx)
        self._init_udf_with_retries(ctx)
        # ... 其余不变 ...
```

**重要**: 由于 Ray 框架层已经处理了 ObjectRef → 实际对象的转换，`_MapWorker.__init__` **完全不需要修改**。这是此优化方案的一个优势——只需修改 Driver 端的 `start()` 和 `_start_actor()`, Worker 端零改动。

**性能收益分析**:

| 场景 | 社区版 | 优化后 |
|------|--------|--------|
| 100 actors, 50MB UDF | 100次序列化 = 5GB 序列化工作 | 1次序列化 = 50MB |
| Driver CPU 时间 | O(N * serialize_time) | O(serialize_time) |
| Object Store 占用 | N 份独立副本 | 1 份 + 引用计数 |
| 同节点 actor | 反序列化 N 次 | 反序列化 N 次 (不变) |

注: Ray 的 Object Store 使用共享内存，同节点的多个 actor `ray.get` 同一个 ObjectRef 时实际是零拷贝 mmap 访问。

---

## 六、不健康算子实例自动检测

### 6.1 问题分析

当前 Ray Data 没有自动检测"慢算子"的机制:
- `refresh_actor_state()` (`actor_pool_map_operator.py:1207`) 只检查 GCS 中的状态（ALIVE/RESTARTING/DEAD）
- 无法检测到"活着但极慢"的 actor（如 GPU 被其他进程抢占、内存 swap 导致计算极慢）
- 在低优/抢占式资源场景下，部分 actor 可能因资源压制导致推理速度慢 10-100x

### 6.2 设计方案

**新增文件**:
- `python/ray/data/_internal/execution/issue_detector.py`

**修改文件**:
- `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py`

#### 6.2.1 性能检测器

```python
# python/ray/data/_internal/execution/issue_detector.py

import time
import math
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from ray.actor import ActorHandle

logger = logging.getLogger(__name__)


@dataclass
class ActorPerformanceStats:
    """跟踪单个 actor 的性能统计"""
    total_tasks_completed: int = 0
    total_execution_time_s: float = 0.0
    last_task_start_time: Optional[float] = None
    consecutive_slow_count: int = 0

    @property
    def avg_task_duration_s(self) -> float:
        if self.total_tasks_completed == 0:
            return 0.0
        return self.total_execution_time_s / self.total_tasks_completed


class UnhealthyActorDetector:
    """基于运行时统计（均值 + 标准差）自动检测不健康的算子实例

    检测策略:
    1. 收集每个 actor 的 task 执行时间统计
    2. 计算全局均值和标准差
    3. 如果某个 actor 的平均执行时间 > 均值 + N*标准差，标记为 slow
    4. 如果某个 actor 的当前 task 执行时间 > 均值 + M*标准差，标记为 stuck

    设计考量:
    - 需要足够样本（min_samples）才开始检测，避免冷启动误报
    - 使用相对阈值（sigma倍数）而非绝对阈值，适配不同 UDF 执行时间
    - stuck 检测有最低60s阈值，避免正常波动误报
    """

    def __init__(self,
                 slow_threshold_sigma: float = 2.0,
                 stuck_threshold_sigma: float = 3.0,
                 min_samples_for_detection: int = 5,
                 max_consecutive_slow: int = 3):
        self._slow_threshold_sigma = slow_threshold_sigma
        self._stuck_threshold_sigma = stuck_threshold_sigma
        self._min_samples = min_samples_for_detection
        self._max_consecutive_slow = max_consecutive_slow
        self._actor_stats: Dict[ActorHandle, ActorPerformanceStats] = {}

    def record_task_start(self, actor: ActorHandle):
        if actor not in self._actor_stats:
            self._actor_stats[actor] = ActorPerformanceStats()
        self._actor_stats[actor].last_task_start_time = time.time()

    def record_task_completion(self, actor: ActorHandle, duration_s: float):
        if actor not in self._actor_stats:
            self._actor_stats[actor] = ActorPerformanceStats()
        stats = self._actor_stats[actor]
        stats.total_tasks_completed += 1
        stats.total_execution_time_s += duration_s
        stats.last_task_start_time = None

        # 检测当前 task 是否 slow
        mean, std = self._compute_global_stats()
        if mean > 0 and std > 0:
            if duration_s > mean + self._slow_threshold_sigma * std:
                stats.consecutive_slow_count += 1
            else:
                stats.consecutive_slow_count = 0

    def _compute_global_stats(self) -> Tuple[float, float]:
        """计算所有 actor 的全局 task 时间均值和标准差"""
        durations = [
            s.avg_task_duration_s for s in self._actor_stats.values()
            if s.total_tasks_completed >= self._min_samples
        ]
        if len(durations) < 2:
            return 0.0, 0.0

        mean = sum(durations) / len(durations)
        variance = sum((d - mean) ** 2 for d in durations) / len(durations)
        std = math.sqrt(variance)
        return mean, std

    def detect_unhealthy_actors(self) -> List[Tuple[ActorHandle, str]]:
        """检测并返回不健康的 actor 列表

        Returns:
            List of (actor, reason) tuples
        """
        mean, std = self._compute_global_stats()
        if mean == 0 or std == 0:
            return []

        unhealthy = []
        now = time.time()

        for actor, stats in self._actor_stats.items():
            # 检测1: 持续慢（平均执行时间远高于正常水平）
            if (stats.total_tasks_completed >= self._min_samples and
                stats.avg_task_duration_s > mean + self._slow_threshold_sigma * std):
                unhealthy.append((actor,
                    f"slow: avg {stats.avg_task_duration_s:.1f}s vs "
                    f"global avg {mean:.1f}s "
                    f"(threshold: {mean + self._slow_threshold_sigma * std:.1f}s)"))

            # 检测2: 当前 task 卡住（执行时间远超正常）
            if stats.last_task_start_time is not None:
                current_duration = now - stats.last_task_start_time
                stuck_threshold = mean + self._stuck_threshold_sigma * std
                if current_duration > max(stuck_threshold, 60):  # 至少60秒
                    unhealthy.append((actor,
                        f"stuck: current task running {current_duration:.0f}s vs "
                        f"threshold {stuck_threshold:.1f}s"))

            # 检测3: 连续慢（即使平均还行，但最近连续几次都慢）
            if stats.consecutive_slow_count >= self._max_consecutive_slow:
                unhealthy.append((actor,
                    f"consecutive_slow: {stats.consecutive_slow_count} "
                    f"consecutive slow tasks"))

        return unhealthy

    def remove_actor(self, actor: ActorHandle):
        self._actor_stats.pop(actor, None)
```

#### 6.2.2 在 ActorPoolMapOperator 中集成

```python
# actor_pool_map_operator.py:

from ray.data._internal.execution.issue_detector import UnhealthyActorDetector

class ActorPoolMapOperator(MapOperator):
    def __init__(self, ...):
        # ... 现有初始化 ...
        self._unhealthy_detector = UnhealthyActorDetector()
        self._last_health_check_time = 0.0
        self._HEALTH_CHECK_INTERVAL_S = 10.0  # 每10秒检查一次

    def _try_schedule_tasks_internal(self, strict: bool) -> int:
        for bundle, actor in self._actor_task_selector.select_actors(...):
            # 记录 task 开始
            task_start_time = time.time()
            self._unhealthy_detector.record_task_start(actor)

            # ... 现有提交逻辑 ...

            def _task_done_callback(actor_to_return, start_time):
                duration = time.time() - start_time
                self._unhealthy_detector.record_task_completion(
                    actor_to_return, duration)
                self._actor_pool.on_task_completed(actor_to_return)

            self._submit_data_task(
                gen, bundle,
                partial(_task_done_callback,
                        actor_to_return=actor, start_time=task_start_time)
            )

    def _periodic_health_check(self):
        """在调度循环中定期调用"""
        now = time.time()
        if now - self._last_health_check_time < self._HEALTH_CHECK_INTERVAL_S:
            return
        self._last_health_check_time = now

        unhealthy = self._unhealthy_detector.detect_unhealthy_actors()
        for actor, reason in unhealthy:
            logical_id = self._actor_pool._actor_to_logical_id.get(actor, "unknown")
            logger.warning(
                f"Unhealthy actor detected ({logical_id}): {reason}")
            # 将不健康 actor 加入黑名单
            self._actor_pool.on_task_failed(
                actor, RuntimeError(f"Unhealthy: {reason}"))
```

---

## 七、算子实例 Timeout 机制

### 7.1 问题分析

用户自定义的算子（UDF）可能因为实现问题导致长时间无法返回（如死循环、网络阻塞、GPU hang 等），没有现成的超时中断机制。

当前 actor task 提交 (`actor_pool_map_operator.py:382`) 使用 streaming generator:
```python
gen = actor.submit.options(num_returns="streaming", ...).remote(...)
```

Ray 的 streaming generator 没有内置超时机制。一旦 UDF 卡死，该 actor 的所有 task slot 被永久占用。

### 7.2 设计方案

**修改文件**:
- `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py`
- `python/ray/data/_internal/compute.py` (ActorPoolStrategy 增加 timeout 参数)
- `python/ray/data/_internal/execution/streaming_executor.py`

#### 7.2.1 用户接口

```python
# compute.py 中 ActorPoolStrategy 增加:

@PublicAPI
class ActorPoolStrategy(ComputeStrategy):
    def __init__(
        self,
        *,
        size: Optional[int] = None,
        min_size: Optional[int] = None,
        max_size: Optional[int] = None,
        initial_size: Optional[int] = None,
        max_tasks_in_flight_per_actor: Optional[int] = None,
        enable_true_multi_threading: bool = False,
        # 新增:
        task_timeout_s: Optional[float] = None,  # 单个 task 的超时时间(秒)
    ):
        # ... 现有初始化 ...
        self.task_timeout_s = task_timeout_s
```

用户使用方式:
```python
ds.map_batches(
    my_udf,
    compute=ray.data.ActorPoolStrategy(
        min_size=10,
        max_size=100,
        task_timeout_s=300,  # 5分钟超时
    ),
)
```

#### 7.2.2 Timeout 检查实现

```python
# actor_pool_map_operator.py 中:

class ActorPoolMapOperator(MapOperator):
    def __init__(self, ...):
        # ... 现有初始化 ...
        self._task_timeout_s = getattr(compute_strategy, 'task_timeout_s', None)
        # 记录活跃 task: {gen_ref: (actor, start_time, bundle)}
        self._active_tasks: Dict[ObjectRef, Tuple[ActorHandle, float, RefBundle]] = {}

    def _try_schedule_tasks_internal(self, strict: bool) -> int:
        for bundle, actor in self._actor_task_selector.select_actors(...):
            # ... 现有逻辑 ...
            gen = actor.submit.options(
                num_returns="streaming",
                **self._ray_actor_task_remote_args,
            ).remote(...)

            # 记录 task 的开始时间
            if self._task_timeout_s is not None:
                self._active_tasks[gen._generator_ref] = (
                    actor, time.time(), bundle)

            def _task_done_callback(actor_to_return, gen_ref):
                self._actor_pool.on_task_completed(actor_to_return)
                self._active_tasks.pop(gen_ref, None)

            self._submit_data_task(
                gen, bundle,
                partial(_task_done_callback,
                        actor_to_return=actor,
                        gen_ref=gen._generator_ref)
            )
            num_submitted_tasks += 1
        return num_submitted_tasks

    def check_task_timeouts(self):
        """由 StreamingExecutor 的主循环定期调用"""
        if self._task_timeout_s is None:
            return

        now = time.time()
        timed_out = []

        for gen_ref, (actor, start_time, bundle) in list(
            self._active_tasks.items()
        ):
            elapsed = now - start_time
            if elapsed > self._task_timeout_s:
                timed_out.append((gen_ref, actor, elapsed, bundle))

        for gen_ref, actor, elapsed, bundle in timed_out:
            logical_id = self._actor_pool._actor_to_logical_id.get(
                actor, "unknown")
            logger.warning(
                f"Task on actor {logical_id} timed out after {elapsed:.0f}s "
                f"(limit: {self._task_timeout_s}s), cancelling"
            )
            # 取消超时的 task
            ray.cancel(gen_ref, force=True)
            del self._active_tasks[gen_ref]

            # 通知 actor pool 做相应处理
            self._actor_pool.on_task_completed(actor)
            # 将超时 actor 标记为可能不健康
            self._actor_pool.on_task_failed(
                actor, TimeoutError(f"Task timeout after {elapsed:.0f}s"))

            # 将 bundle 重新入队用于重试
            self._bundle_queue.append(bundle)
```

#### 7.2.3 在 StreamingExecutor 主循环中集成

```python
# streaming_executor.py 的 _scheduling_loop_step 中 (line 569):

def _scheduling_loop_step(self, topology):
    # ... 现有调度逻辑 (process_completed_tasks, select_operator_to_run, etc.) ...

    # 新增: 检查算子超时 (每轮调度循环都检查)
    for op, state in topology.items():
        if hasattr(op, 'check_task_timeouts'):
            op.check_task_timeouts()

    # ... 后续 autoscaling 等逻辑 ...
```

---

## 八、集群热更新能力

### 8.1 问题分析

当前 Ray 的配置（`RayConfig`）在进程启动时初始化一次，不支持运行时修改。对于超大规模集群（2000 节点），重启代价极大：
- GCS 重启导致所有 actor 信息丢失
- 所有 worker 进程需要重新连接
- Object Store 中的数据可能丢失

需要热更新的场景:
1. 动态调整 actor pool 的 min/max size
2. 动态修改 worker group 的副本数
3. 运行时调整 autoscaling 参数

### 8.2 设计方案

#### 8.2.1 KubeRay 层: Worker Group 热更新

**修改文件**:
- `python/ray/autoscaler/_private/kuberay/autoscaling_config.py`

当前 `AutoscalingConfigProducer.__call__()` 已经每次从 K8S API 读取最新 CR 配置 (`autoscaling_config.py:42-87`)。KubeRay operator 本身支持通过修改 RayCluster CR 的 `replicas` 字段来动态调整 worker 数量。

**增强**: 支持通过 CR annotation 传递更细粒度的动态配置:

```python
# autoscaling_config.py 修改 _derive_autoscaling_config_from_ray_cr:

def _derive_autoscaling_config_from_ray_cr(ray_cr):
    # ... 现有逻辑 (lines 90-142) ...

    # 新增: 解析 CR 中的动态配置 annotation
    annotations = ray_cr.get("metadata", {}).get("annotations", {})

    # 支持通过 annotation 覆盖 autoscaling 参数
    for key, value in annotations.items():
        if key == "ray.io/dynamic-idle-timeout-seconds":
            autoscaling_config["idle_timeout_minutes"] = float(value) / 60
        elif key == "ray.io/dynamic-upscaling-speed":
            autoscaling_config["upscaling_speed"] = float(value)

    # 支持通过 annotation 修改 worker group 的 min/max
    for group_name, node_type in available_node_types.items():
        prefix = f"ray.io/dynamic-{group_name}-"
        for key, value in annotations.items():
            if key == f"{prefix}min-workers":
                node_type["min_workers"] = int(value)
            elif key == f"{prefix}max-workers":
                node_type["max_workers"] = int(value)

    return autoscaling_config
```

**用户操作方式** (无需重启集群):

```bash
# 动态修改 worker group 的最大副本数
kubectl annotate raycluster my-cluster \
  ray.io/dynamic-worker-max-workers=1000 --overwrite

# 动态修改空闲超时
kubectl annotate raycluster my-cluster \
  ray.io/dynamic-idle-timeout-seconds=120 --overwrite
```

#### 8.2.2 Ray Data 层: Actor Pool 配置热更新

**修改文件**:
- `python/ray/data/_internal/execution/streaming_executor.py`
- 新增: `python/ray/data/_internal/execution/operator_config_controller.py`

`_ActorPool` 已有 `update_config()` 方法支持修改 min/max/target size。需要补充的是将外部配置变更传导到 actor pool 的通道。

**设计**: 通过 GCS Internal KV 作为配置传递通道:

```python
# 新增 operator_config_controller.py:

import json
import time
import logging
from typing import Dict, Optional
import ray
from ray.data._internal.execution.operators.actor_pool_map_operator import (
    ActorPoolMapOperator,
)

logger = logging.getLogger(__name__)


class OperatorConfigController:
    """通过 GCS Internal KV 传递算子运行时配置变更

    配置 Key 格式: ray_data_op_config/{job_id}/{operator_id}
    配置 Value 格式: JSON {"min_size": N, "max_size": M, "target_size": K}

    用户通过 ray.get_runtime_context().get_job_id() + operator name
    写入 GCS Internal KV 来触发配置变更。
    """

    POLL_INTERVAL_S = 10
    CONFIG_KV_NAMESPACE = b"ray_data_op_config"

    def __init__(self, topology, data_context):
        self._topology = topology
        self._last_poll = 0
        self._gcs_client = ray._private.gcs_utils.GcsClient(
            address=ray.get_runtime_context().gcs_address)
        self._job_id = ray.get_runtime_context().get_job_id()

    def check_updates(self) -> Dict:
        """轮询 GCS KV 检查配置变更"""
        now = time.time()
        if now - self._last_poll < self.POLL_INTERVAL_S:
            return {}
        self._last_poll = now

        updates = {}
        for op, state in self._topology.items():
            if not isinstance(op, ActorPoolMapOperator):
                continue

            config_key = f"{self._job_id}/{op.id}/parallelism".encode()
            try:
                value = self._gcs_client.internal_kv_get(
                    config_key, self.CONFIG_KV_NAMESPACE)
                if value:
                    config_dict = json.loads(value)
                    updates[op] = config_dict
                    logger.info(
                        f"Received config update for operator {op.name}: "
                        f"{config_dict}")
            except Exception as e:
                logger.debug(f"Failed to check config for {op.name}: {e}")

        return updates


# 用户端 API (可以通过 Ray client 调用):
def update_operator_config(job_id: str, operator_id: str,
                           min_size: int = None, max_size: int = None,
                           target_size: int = None):
    """热更新运行中作业的算子配置"""
    gcs_client = ray._private.gcs_utils.GcsClient(
        address=ray.get_runtime_context().gcs_address)

    config = {}
    if min_size is not None:
        config["min_size"] = min_size
    if max_size is not None:
        config["max_size"] = max_size
    if target_size is not None:
        config["target_size"] = target_size

    config_key = f"{job_id}/{operator_id}/parallelism".encode()
    gcs_client.internal_kv_put(
        config_key, json.dumps(config).encode(),
        overwrite=True, namespace=b"ray_data_op_config")
```

**在 StreamingExecutor 中集成**:

```python
# streaming_executor.py:

class StreamingExecutor(Executor, threading.Thread):
    def _initialize_operators(self, ...):
        # ... 现有初始化 ...

        # 新增: 初始化配置控制器
        self._config_controller = None
        if self._data_context.enable_dynamic_config:
            self._config_controller = OperatorConfigController(
                topology=self._topology,
                data_context=self._data_context,
            )

    def _scheduling_loop_step(self, topology):
        # ... 现有调度逻辑 ...

        # 新增: 同步动态配置
        if self._config_controller:
            config_updates = self._config_controller.check_updates()
            for op, new_config in config_updates.items():
                actor_pools = op.get_autoscaling_actor_pools()
                for pool in actor_pools:
                    pool.update_config(
                        min_size=new_config.get("min_size"),
                        max_size=new_config.get("max_size"),
                    )
                    if "target_size" in new_config:
                        target = new_config["target_size"]
                        current = pool.current_size()
                        if target > current:
                            pool.scale(ActorPoolScalingRequest.upscale(
                                delta=target - current,
                                reason="dynamic config update"))
```

---

## 九、Head 节点与 GCS 优化

### 9.1 问题分析

GCS 是 Ray 集群的全局单点组件 (`gcs_server.cc`)。从 `DoStart` 方法 (lines 267-322) 可以看到 GCS 承载的组件:

```
GcsServer 组件列表:
├── GcsNodeManager        (节点管理, 2000节点)
├── GcsResourceManager    (资源状态同步, 每100ms上报)
├── GcsActorManager       (Actor注册/管理, 4w actors)
├── GcsTaskManager        (Task event 缓存, 配置 task_events_max_num_task_in_gcs)
├── GcsJobManager         (Job 管理)
├── PubSubHandler         (8个 channel 的发布/订阅)
├── GcsHealthCheckManager (节点健康检查)
├── RaySyncer            (状态同步)
├── KVService            (Internal KV 存储)
├── FunctionManager      (UDF 序列化存储)
├── RuntimeEnvManager    (运行时环境)
├── GcsAutoscalerStateManager (Autoscaler 状态)
└── GcsPlacementGroupManager (Placement Group)
```

在大规模场景下面临:
1. **CPU 负载**: 大量 actor 注册/管理、pub/sub 消息、资源状态同步
2. **内存占用**: Task event 累积 (`GcsTaskManagerStorage`)、actor 元数据
3. **pub/sub 数据丢失**: 高负载下消息可能丢失 (subscriber 回调处理 `actor_manager.cc:295`)
4. **RPC 超时**: 高负载下 gRPC 请求排队

### 9.2 设计方案

#### 9.2.1 参数化调优（不修改代码）

通过环境变量调优 `ray_config_def.h` 中的配置项:

```bash
# === GCS 负载降低 ===

# Raylet 资源上报频率 (默认100ms → 500ms, 降低 GCS 接收压力)
export RAY_raylet_report_resources_period_milliseconds=500

# GCS pull 资源负载频率 (默认1000ms → 5000ms)
export RAY_gcs_pull_resource_loads_period_milliseconds=5000

# Debug dump 频率 (默认10000ms → 60000ms, 降低日志开销)
export RAY_debug_dump_period_milliseconds=60000

# === Object Fetch 容忍 ===

# fetch 告警超时 (默认60s → 120s)
export RAY_fetch_warn_timeout_milliseconds=120000

# fetch 失败超时 (默认600s → 1200s)
export RAY_fetch_fail_timeout_milliseconds=1200000

# === Task Event 内存控制 ===

# GCS 中缓存的最大 task event 数 (根据实际需求调整)
export RAY_task_events_max_num_task_in_gcs=100000

# === Worker Lease 超时 ===

# 远程 lease 超时 (默认600s, 大规模场景可适当增加)
export RAY_worker_lease_timeout_ms=900000
```

#### 9.2.2 pub/sub 订阅失败重试（代码修改）

**修改文件**: `src/ray/core_worker/actor_management/actor_manager.cc`

当前 `SubscribeActorState` (lines 295-335) 的 `on_done` callback 只在成功时缓存 actor name，不处理失败情况:

```cpp
// 当前代码 (actor_manager.cc:316-328):
gcs_client_->Actors().AsyncSubscribe(
    actor_id,
    actor_notification_callback,
    [this, actor_id, cached_actor_name](Status status) {
      // on_done: 只处理成功
      if (status.ok() && !cached_actor_name.empty()) {
        absl::MutexLock lock(&cache_mutex_);
        auto iter = subscribed_actors_.find(actor_id);
        if (iter != subscribed_actors_.end() && iter->second) {
          cached_actor_name_to_ids_.emplace(cached_actor_name, actor_id);
        }
      }
      // 失败时无处理 → 可能丢失 actor 状态通知!
    });
```

**优化**: 添加订阅失败的延迟重试:

```cpp
void ActorManager::SubscribeActorState(const ActorID &actor_id) {
  // ... 现有检查逻辑 ...

  auto subscribe_done = [this, actor_id, cached_actor_name](Status status) {
    if (status.ok()) {
      // 现有成功处理...
      if (!cached_actor_name.empty()) {
        absl::MutexLock lock(&cache_mutex_);
        auto iter = subscribed_actors_.find(actor_id);
        if (iter != subscribed_actors_.end() && iter->second) {
          cached_actor_name_to_ids_.emplace(cached_actor_name, actor_id);
        }
      }
    } else {
      // 新增: 订阅失败时延迟重试
      RAY_LOG(WARNING) << "Failed to subscribe actor " << actor_id
                       << " state from GCS: " << status.ToString()
                       << ". Will retry in 5s.";
      // 标记订阅无效以允许重新订阅
      {
        absl::MutexLock lock(&cache_mutex_);
        subscribed_actors_.erase(actor_id);
      }
      // 5秒后重试订阅
      io_service_.post([this, actor_id]() {
        std::this_thread::sleep_for(std::chrono::seconds(5));
        SubscribeActorState(actor_id);
      });
    }
  };

  gcs_client_->Actors().AsyncSubscribe(
      actor_id, actor_notification_callback, subscribe_done);
}
```

#### 9.2.3 GCS 内存优化: Task Event 淘汰策略增强

当前 `GcsTaskManagerStorage` 已有容量限制和 GC 策略 (`gcs_task_manager.h:175-487`)：
- 最大容量: `RAY_task_events_max_num_task_in_gcs`
- 淘汰策略: `FinishedTaskActorTaskGcPolicy` 按优先级淘汰 (finished → actor_task → other)

**优化**: 在大规模场景下，补充基于 Job 粒度的限额:

```cpp
// gcs_task_manager.h 新增:
RAY_CONFIG(int64_t, max_task_events_per_job, 10000)

// gcs_task_manager.cc AddOrReplaceTaskEvent 中:
void GcsTaskManagerStorage::AddOrReplaceTaskEvent(rpc::TaskEvents &&events) {
  auto job_id = JobID::FromBinary(events.job_id());

  // 新增: per-job 限额检查
  auto &job_summary = job_task_summary_[job_id];
  if (job_summary.total_events() >
      RayConfig::instance().max_task_events_per_job()) {
    // 该 job 已超限额，丢弃非关键 event
    if (!events.has_state_updates() ||
        events.state_updates().state_ts_ns().empty()) {
      stats_counter_.Increment(kTotalNumDroppedTaskEventsOverJobLimit);
      return;
    }
  }

  // ... 现有添加逻辑 ...
}
```

#### 9.2.4 fetch function RPC 超时容忍

```cpp
// core_worker.cc 中对 fetch 失败做兜底:
// (在 object_recovery_manager 触发前增加重试层)

// ray_config_def.h:
RAY_CONFIG(int32_t, max_object_fetch_retries, 3)

// core_worker 中维护重试计数:
void CoreWorker::HandleFetchObjectTimeout(const ObjectID &object_id) {
  auto &retry_count = fetch_retry_counts_[object_id];
  retry_count++;

  if (retry_count < RayConfig::instance().max_object_fetch_retries()) {
    RAY_LOG(WARNING) << "Object " << object_id << " fetch timeout (attempt "
                     << retry_count << "/"
                     << RayConfig::instance().max_object_fetch_retries()
                     << "), retrying...";
    // 重新发起 fetch 请求
    object_recovery_manager_->RecoverObject(object_id);
  } else {
    RAY_LOG(ERROR) << "Object " << object_id << " fetch failed after "
                   << retry_count << " retries, triggering lineage reconstruction";
    fetch_retry_counts_.erase(object_id);
    // 进入正常失败处理流程
    MarkObjectFailed(object_id, rpc::ErrorType::OBJECT_FETCH_TIMED_OUT);
  }
}
```

---

## 十、Driver 进程优化

### 10.1 问题分析

Driver 进程中的 `StreamingExecutor` (`streaming_executor.py`) 在主循环中处理所有算子的调度。在大规模场景（4w actors, 12w blocks）下:

#### 10.1.1 整体线程架构

```
┌───────────────────────────────────────────────────────────────────┐
│ 用户主线程                                                         │
│                                                                   │
│  for batch in ds.iter_batches():                                  │
│    → _ClosingIterator.get_next()    # streaming_executor.py:1098  │
│      → OpState.get_output_blocking()                              │
│        → while True:                                              │
│            ref = output_queue.pop()                                │
│            if ref: return ref                                      │
│            time.sleep(0.01)          # 每10ms轮询一次              │
│                                                                   │
│  (阻塞等待 output queue 有数据)                                     │
└───────────────────────────────────────────────────────────────────┘
        ↕ 通过 OpState.output_queue 通信 (线程安全队列)
┌───────────────────────────────────────────────────────────────────┐
│ 调度 daemon 线程 (StreamingExecutor.run)                            │
│                                                                   │
│  while True:  # streaming_executor.py:455-487, 无sleep/yield      │
│    _scheduling_loop_step(topology)                                │
│    update_metrics()                                               │
│    if not continue_sched or shutdown: break                       │
└───────────────────────────────────────────────────────────────────┘
```

**关键事实**:
- 调度线程 `run()` 的外层循环**没有 sleep/yield**，唯一的"暂停"来自 `process_completed_tasks` 内部的 `ray.wait(timeout=0.1)`
- 用户线程和调度线程通过 `OpState.output_queue` 解耦，用户线程以 10ms 间隔轮询

#### 10.1.2 `_scheduling_loop_step` 详细结构

基于 `streaming_executor.py:569-665` 的实际代码：

```
_scheduling_loop_step (每轮):
│
├─[1] resource_manager.update_usages()
│
├─[2] process_completed_tasks()              ← 唯一阻塞点
│   ├── ray.wait(active_tasks, timeout=0.1)  ← 最多阻塞100ms
│   │   (streaming_executor_state.py:537-544)
│   │   注: num_returns=len(active_tasks), fetch_local=False
│   │   如果有 ready 的立即返回，否则等100ms
│   ├── Phase 2: 对每个 ready task 调用 prepare_metadata()
│   ├── ray.wait(meta_refs, timeout=0.0)     ← 完全非阻塞!
│   │   (METADATA_WAIT_TIMEOUT_S = 0.0)
│   └── Phase 4-5: complete_with_metadata + output 分发
│
├─[3] Dispatch while True 循环              ← 核心调度
│   ├── select_operator_to_run()
│   │   ├── get_eligible_operators()         O(num_ops × num_policies)
│   │   │   (streaming_executor_state.py:790-901)
│   │   │   检查每个 op: has_completed, can_add_input, has_pending_bundles
│   │   │   对每个 op 检查所有 backpressure_policies.can_add_input(op)
│   │   └── ranker.rank_operators()          O(num_eligible_ops)
│   │       (ranker.py:89-124)
│   │       对每个 op 算 (throttling_disabled, obj_store_mem) rank
│   ├── topology[op].dispatch_next_task()    从 inqueue 移到 op
│   ├── resource_manager.update_usages()     每次 dispatch 后都更新
│   └── (循环直到 select_operator_to_run 返回 None)
│
├─[4] cluster_autoscaler.try_trigger_scaling()
├─[5] actor_autoscaler.try_trigger_scaling()
│   └── refresh_actor_state() for each pool
├─[6] config_controller.try_apply_config()
├─[7] update_operator_states(topology)
├─[8] refresh_progress_manager
├─[9] update_stats_metrics + debug_dump (周期性)
└─[10] return: not all(op.has_completed())
```

**关键瓶颈分析**:

| 瓶颈 | 代码位置 | 耗时 | 影响 |
|------|----------|------|------|
| `ray.wait(timeout=0.1)` 固定等待 | `streaming_executor_state.py:537-544` | 0-100ms (固定上限) | 即使有 pending dispatch work 也要等 |
| `select_operator_to_run` **每次 dispatch 都调** | `streaming_executor_state.py:904-939` | 0.1-1ms × N次 | dispatch N个task需要调N+1次 |
| `resource_manager.update_usages` **每次 dispatch 都调** | 每次 dispatch 后 | 0.05-0.5ms × N次 | 同上 |
| `refresh_actor_state` 逐个查 GCS | `actor_pool_map_operator.py:1207-1222` | 4w actors 时可达数十ms | 阻塞调度循环 |
| `get_eligible_operators` 遍历全部 op | `streaming_executor_state.py:790-901` | O(ops × policies) | 深pipeline(20+算子)时开销大 |

**耗时估算** (大规模: 20算子, 4w actors, 12w blocks):
```
场景A: 有大量pending task + 大量completion
├── ray.wait(0.1s): 立即返回(0ms) 因为有 ready tasks
├── process completions: 10-50ms (反序列化 metadata)
├── dispatch 100 tasks: 100 × (select_op 1ms + dispatch 0.5ms + update 0.2ms) ≈ 170ms
├── autoscaling: 10-50ms (refresh_actor_state)
└── 其他: 5-10ms
总计: ~200-280ms

场景B: 无pending task, 等待completion
├── ray.wait(0.1s): 100ms (超时)
├── process completions: 0ms (无ready)
├── dispatch: 0ms (无eligible op)
├── autoscaling: 10-50ms
└── 其他: 5-10ms
总计: ~115-160ms
```

**场景A的核心问题**: dispatch 100 个 task 时，每个 task 都要重新执行一次完整的 `select_operator_to_run`（遍历所有算子 + backpressure 检查 + 排序），开销线性增长。

### 10.2 设计方案

**修改文件**:
- `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py`
- `python/ray/data/_internal/execution/streaming_executor.py`
- `python/ray/data/_internal/execution/streaming_executor_state.py`

#### 10.2.1 Actor 状态刷新优化

当前 `refresh_actor_state()` 每个调度循环都会调用，遍历所有 running actors:

```python
# 当前代码 (actor_pool_map_operator.py:1207-1222):
def refresh_actor_state(self):
    for actor in self.get_running_actor_refs():
        actor_state = actor._get_local_state()  # GCS查询
        # ... 状态更新 ...
```

**优化: 降低刷新频率 + 增量检查**:

```python
class _ActorPool(AutoscalingActorPool):
    _REFRESH_STATE_INTERVAL_S = 2.0  # 从每轮循环改为每2秒

    def __init__(self, ...):
        # ... 现有初始化 ...
        self._last_state_refresh = 0.0
        self._actors_needing_check: set = set()  # 增量: 只检查有变化的

    def refresh_actor_state(self):
        """优化: 降低刷新频率，增量检查"""
        now = time.time()
        if now - self._last_state_refresh < self._REFRESH_STATE_INTERVAL_S:
            return
        self._last_state_refresh = now

        # 只检查: (1) 新加入的 actors (2) 上次是 RESTARTING 的
        actors_to_check = (
            self._actors_needing_check or set(self.get_running_actor_refs())
        )
        self._actors_needing_check.clear()

        for actor in actors_to_check:
            if actor not in self._running_actors:
                continue
            actor_state = actor._get_local_state()
            if actor_state in (None, gcs_pb2.ActorTableData.ActorState.DEAD):
                continue
            elif actor_state == gcs_pb2.ActorTableData.ActorState.RESTARTING:
                self._update_running_actor_state(actor, True)
                # 下次继续检查此 actor
                self._actors_needing_check.add(actor)
            else:
                self._update_running_actor_state(actor, False)

    def on_actor_state_changed(self, actor):
        """收到 actor 状态变更通知时调用（通过 pub/sub）"""
        self._actors_needing_check.add(actor)
```

#### 10.2.2 调度循环优化: 活跃 operator 快速路径

当前 `select_operator_to_run` (`streaming_executor_state.py:904-939`) 通过 `get_eligible_operators` 遍历所有 operator 并进行 backpressure 检查:

```python
# 优化 streaming_executor_state.py:

def select_operator_to_run(topology, resource_manager,
                           backpressure_policies, ensure_liveness, ranker):
    # 快速路径: 如果只有少量 operator 有 pending work，只检查它们
    # 避免对所有 operator 执行完整的 eligibility 检查

    # Phase 1: 快速筛选有工作的 operator
    ops_with_input = []
    for op, state in topology.items():
        if state.total_enqueued_input_blocks() > 0 and op.can_add_input():
            ops_with_input.append(op)

    if not ops_with_input:
        return None

    # Phase 2: 只对有工作的 operator 做 backpressure 检查
    eligible_ops = []
    for op in ops_with_input:
        if all(policy.can_add_input(op) for policy in backpressure_policies):
            eligible_ops.append(op)

    if not eligible_ops:
        if ensure_liveness:
            # liveness 保证: 死锁检测逻辑
            return _select_for_liveness(topology, ops_with_input)
        return None

    # Phase 3: 排序（只对 eligible 子集）
    if len(eligible_ops) == 1:
        return eligible_ops[0]  # 跳过排序

    ranks = ranker.rank_operators(eligible_ops, topology, resource_manager)
    next_op, _ = min(zip(eligible_ops, ranks), key=lambda t: t[1])
    return next_op
```

#### 10.2.3 Blocks metadata 内存优化

```python
# streaming_executor_state.py 中 OpState 新增:

class OpState:
    _MAX_CACHED_BLOCK_METADATA = 1000  # 最多缓存详细 metadata

    def __init__(self, op, inqueues):
        # ... 现有初始化 ...
        # 新增: 聚合统计 (替代存储所有历史 metadata)
        self._completed_stats = {
            "total_blocks": 0,
            "total_bytes": 0,
            "total_rows": 0,
        }

    def add_output(self, bundle: RefBundle):
        # ... 现有逻辑 ...
        self.num_completed_tasks += 1

        # 新增: 统计聚合 + metadata 淘汰
        for block_ref, meta in bundle.blocks:
            self._completed_stats["total_blocks"] += 1
            self._completed_stats["total_bytes"] += meta.size_bytes or 0
            self._completed_stats["total_rows"] += meta.num_rows or 0

    def _compact_output_queue(self):
        """当 output queue 过大时进行压缩

        对已经被下游消费的 block metadata 进行聚合后释放
        只保留最近 N 条详细记录用于调试
        """
        if self.output_queue.num_blocks() > self._MAX_CACHED_BLOCK_METADATA * 2:
            # 下游已消费的 metadata 不需要保留详细信息
            # 保留引用但释放详细元数据
            pass  # 实际实现依赖 RefBundle 的 metadata 结构
```

#### 10.2.4 调度循环瓶颈分析与优化

**澄清**: 社区版的 dispatch 阶段本身就是"有可用 task 就一直调度"（unbounded while True），直到 `select_operator_to_run` 返回 None。这个设计是合理的，dispatch 本身不是瓶颈。

**真正的瓶颈**在于每轮循环中其他阶段的固定开销：

| 阶段 | 耗时来源 | 优化方案 |
|------|----------|----------|
| `process_completed_tasks` | `ray.wait(timeout=0.1s)` 固定等待 | 动态 timeout: 如果有 pending dispatch 则 timeout=0 |
| `select_operator_to_run` | 每次 dispatch 都遍历全部 op + backpressure 检查 | 上面 10.2.2 的快速路径优化 |
| `refresh_actor_state` | 逐 actor 查 GCS | 上面 10.2.1 的增量检查 + 降频 |

**优化: 动态调整 ray.wait timeout**:

```python
# streaming_executor.py 中:

def _scheduling_loop_step(self, topology):
    # 判断是否有 pending work 需要立即处理
    has_pending_work = any(
        state.total_enqueued_input_blocks() > 0 and op.can_add_input()
        for op, state in topology.items()
    )

    # Phase 1: 处理完成的 task
    # 如果有 pending dispatch work，不等待 — timeout=0 立即返回
    # 如果没有 pending work，使用标准 timeout 等待新的 completions
    wait_timeout = 0 if has_pending_work else 0.1

    errored_blocks_per_op, _ = process_completed_tasks(
        topology,
        self._backpressure_policies,
        self._max_errored_blocks,
        timeout=wait_timeout,  # 新增: 动态 timeout
    )

    # Phase 2: Dispatch (保持原有 unbounded while True — 有就调)
    while True:
        op = select_operator_to_run(topology, ...)
        if op is None:
            break
        topology[op].dispatch_next_task()

    # Phase 3: Autoscaling (不变)
    self._actor_autoscaler.try_trigger_scaling()
```

**收益**: 当大量 task 等待 dispatch 时，不会浪费 100ms 在 `ray.wait` 上，调度循环频率从 ~10Hz 提升到 ~100Hz+。当没有 pending work 时，回退到标准等待，避免 CPU 空转。

**补充优化: `process_completed_tasks` 批量处理上限**:

```python
# streaming_executor_state.py 中:

def process_completed_tasks(topology, ..., max_completions: int = 200):
    """处理已完成的 task，带上限防止单轮处理耗时过长

    设计考量:
    - 不限制 dispatch（有 task 就调），但限制 completion 处理
    - completion 处理涉及反序列化 metadata、更新 output queue 等较重操作
    - 在 12w blocks 场景下，一次 burst 完成大量 task 时，
      反序列化所有 metadata 可能耗时数秒，阻塞后续 dispatch
    """
    completed_count = 0
    # ... ray.wait 获取 ready refs ...
    for ref in ready_refs:
        if completed_count >= max_completions:
            break
        # 处理 completion...
        completed_count += 1
```

#### 10.2.5 一次性调度 vs 分批调度: 深入分析

##### 社区版实际行为

社区版 `_scheduling_loop_step` 的 dispatch 循环（`streaming_executor.py:607-627`）：

```python
# 社区版: unbounded while True, 有可用task就一直调度
i = 0
while True:
    op = select_operator_to_run(
        topology, self._resource_manager, self._backpressure_policies,
        ensure_liveness=self._consumer_idling(), ranker=self._ranker,
    )
    if op is None:
        break
    topology[op].dispatch_next_task()
    self._resource_manager.update_usages()
    i += 1
    if i % self._progress_manager.TOTAL_PROGRESS_REFRESH_EVERY_N_STEPS == 0:
        self._refresh_progress_manager(topology)
```

**行为**: 有可用 task 就一直调度，直到所有算子都满（actor slot 用完 / backpressure / 无 pending bundles）。

##### 方案对比

| 方案 | 描述 | 优点 | 缺点 |
|------|------|------|------|
| **A. 一次性全调 (社区版)** | unbounded while True | 最大化并行度, 简单 | dispatch 期间无法处理 completion; 大规模时 dispatch 阶段耗时长(上百ms) |
| **B. 固定分批 (MAX=100)** | 每轮最多调100个 | 保证 completion 能被及时处理 | 多绕循环每圈多100ms ray.wait; 批大小难以选择 |
| **C. 动态timeout + 不限dispatch** | 有pending时timeout=0 | 不限制dispatch吞吐, 消除无效等待 | dispatch 阶段本身的开销仍在 |
| **D. 批dispatch + 优先队列 (推荐)** | 对同一op批量dispatch, 减少select开销 | 大幅减少select_operator_to_run调用次数 | 需修改dispatch循环结构 |

##### 为什么不应该简单限制 dispatch 数量

限制 dispatch 数量（方案B）的问题在于: **调度循环外层没有 sleep**，下一轮循环会立即执行 `process_completed_tasks` 中的 `ray.wait(timeout=0.1s)`，这会白白等 100ms。所以如果限制每轮 dispatch=100 但有 200 个 task 等调度，那第二批 100 个 task 会被延迟 100ms 才能调度。

但如果配合方案C（动态timeout=0），分批就不再有额外等待代价，每轮循环会立即开始。此时的问题变为: 即使 timeout=0，每轮循环仍有 `select_operator_to_run` 的 N+1 次调用开销。

##### 结论: 最优方案是减少 `select_operator_to_run` 的冗余调用

**核心洞察**: 当同一个 operator 连续有多个 task 要 dispatch 时，不需要每个 task 都重新遍历全部 operator 做选择。

#### 10.2.6 批 Dispatch 优化 (Batch Dispatch per Operator)

**问题**: 在 dispatch while True 循环中，每 dispatch 一个 task 都调用一次 `select_operator_to_run`（成本: O(num_ops × num_policies)）。如果某个 operator 有 50 个 pending bundles 且 actor pool 有 50 个空闲 slot，需要调用 51 次 `select_operator_to_run`。

**优化思路**: 选中一个 operator 后，一次性 dispatch 它的多个 task（直到该 op 不再 eligible），然后再选下一个 operator。

```python
# streaming_executor.py 优化后的 dispatch 循环:

def _scheduling_loop_step(self, topology):
    # ... process_completed_tasks ...

    # === 优化: 批 dispatch 循环 ===
    dispatched_this_step = 0
    while True:
        op = select_operator_to_run(
            topology, self._resource_manager, self._backpressure_policies,
            ensure_liveness=self._consumer_idling(), ranker=self._ranker,
        )
        if op is None:
            break

        # 选中 op 后，批量 dispatch 直到该 op 不再可调度
        batch_count = 0
        op_state = topology[op]
        while op_state.has_pending_bundles() and op.can_add_input():
            op_state.dispatch_next_task()
            batch_count += 1
            dispatched_this_step += 1

            # 每 batch_size 个 task 检查一次是否需要更新资源视图
            # (避免过度调度超出内存预算)
            if batch_count % _BATCH_RESOURCE_CHECK_INTERVAL == 0:
                self._resource_manager.update_usages()
                # 重新检查 backpressure
                if not all(p.can_add_input(op) for p in self._backpressure_policies):
                    break

        # batch 结束后统一更新资源
        self._resource_manager.update_usages()

    # ... autoscaling, update_states ...

# 推荐值: 每16个task检查一次资源/backpressure
_BATCH_RESOURCE_CHECK_INTERVAL = 16
```

**性能对比** (dispatch 100 个 task 到同一个 operator):

| 指标 | 社区版 (逐个select) | 批dispatch |
|------|---------------------|-----------|
| `select_operator_to_run` 调用 | 101次 | 1-2次 |
| `resource_manager.update_usages` 调用 | 100次 | 6-7次 (每16个一次) |
| 预估耗时 | 100 × 1.7ms ≈ 170ms | 100 × 0.5ms + 2 × 1ms ≈ 52ms |
| 总 dispatch 吞吐 | ~600 tasks/s | ~2000 tasks/s |

#### 10.2.7 优先队列优化 (Priority Queue for Operator Selection)

当 pipeline 有 20+ 个算子时，`get_eligible_operators` 遍历全部算子的开销不可忽略。可以用优先队列维护 operator 的调度优先级，避免每次全量遍历。

##### 当前 `select_operator_to_run` 的复杂度

```python
# 当前 (streaming_executor_state.py:904-939):
def select_operator_to_run(...):
    # Step 1: 遍历所有 op 检查 eligible — O(num_ops × num_policies)
    eligible_ops = get_eligible_operators(topology, backpressure_policies, ...)

    # Step 2: 对 eligible ops 排序 — O(num_eligible × log(num_eligible))
    ranks = ranker.rank_operators(eligible_ops, topology, resource_manager)
    next_op, _ = min(zip(eligible_ops, ranks), key=lambda t: t[1])
    return next_op
```

每次调用: O(total_ops × policies + eligible_ops × log(eligible_ops))

##### 优先队列方案

```python
# streaming_executor.py 新增:
import heapq
from dataclasses import dataclass, field

@dataclass(order=True)
class _OpScheduleEntry:
    """优先队列中的 operator 调度条目

    排序规则 (priority 越小优先级越高):
    1. throttling_disabled 的 op 优先 (priority[0] = 0)
    2. object_store_memory 使用量低的 op 优先 (priority[1])
    """
    priority: tuple = field(compare=True)
    op: object = field(compare=False)  # PhysicalOperator, 不参与比较


class _OperatorPriorityScheduler:
    """基于优先队列的 operator 调度器

    设计要点:
    1. 维护一个 min-heap，按 (throttling_disabled, obj_store_mem) 排序
    2. operator 状态变化时（有新 input / completion / backpressure 变化），
       标记为 dirty 需要重新计算 priority
    3. select 时从 heap 顶取，只验证顶部 op 的 eligibility，
       不需要遍历全部 op

    适用条件:
    - pipeline 有 10+ 个算子
    - 每轮循环需要 dispatch 多个 task

    不适用条件:
    - pipeline 只有 2-3 个算子（遍历开销可忽略）
    - 全部 op 几乎同时 eligible（退化为全量排序）
    """

    _FULL_REBUILD_INTERVAL = 10  # 每10轮循环做一次全量 rebuild

    def __init__(self, topology, backpressure_policies, resource_manager, ranker):
        self._topology = topology
        self._backpressure_policies = backpressure_policies
        self._resource_manager = resource_manager
        self._ranker = ranker
        self._heap: List[_OpScheduleEntry] = []
        self._iteration_count = 0
        self._dirty_ops: set = set()  # 需要重新计算 priority 的 op
        self._rebuild()

    def _rebuild(self):
        """全量重建优先队列"""
        self._heap.clear()
        for op, state in self._topology.items():
            if op.has_completed():
                continue
            priority = self._compute_priority(op)
            heapq.heappush(self._heap, _OpScheduleEntry(priority=priority, op=op))
        self._dirty_ops.clear()

    def _compute_priority(self, op) -> tuple:
        """计算 operator 的调度优先级"""
        throttling_disabled = 0 if op.throttling_disabled() else 1
        obj_store_mem = self._resource_manager.get_op_usage(op).object_store_memory
        return (throttling_disabled, obj_store_mem)

    def mark_dirty(self, op):
        """当 operator 状态变化时标记需要重计算"""
        self._dirty_ops.add(op)

    def select_next(self, ensure_liveness: bool) -> Optional[object]:
        """从优先队列选择下一个可调度的 operator

        与原版 select_operator_to_run 的区别:
        - 原版: 遍历全部 op → 过滤 eligible → 排序 → 取最优
        - 优化: 从 heap 顶依次取，验证 eligibility，第一个通过的即为结果

        最好情况: O(1) — heap 顶即为 eligible
        最坏情况: O(num_ops) — 所有 op 都不 eligible (等价于原版)
        平均情况: O(k) — k 为 heap 中首个 eligible op 之前的非 eligible 数量
        """
        self._iteration_count += 1

        # 周期性全量 rebuild (处理 priority 漂移)
        if (self._iteration_count % self._FULL_REBUILD_INTERVAL == 0
                or len(self._dirty_ops) > len(self._heap) // 2):
            self._rebuild()

        # 从 heap 顶开始找第一个 eligible 的 op
        skipped = []
        result = None

        while self._heap:
            entry = heapq.heappop(self._heap)
            op = entry.op
            state = self._topology.get(op)

            if state is None or op.has_completed():
                continue  # 已完成，丢弃

            # 检查是否 eligible
            if (state.has_pending_bundles()
                    and op.can_add_input()
                    and all(p.can_add_input(op) for p in self._backpressure_policies)):
                result = op
                # 放回 heap (可能 priority 已变)
                if op in self._dirty_ops:
                    entry = _OpScheduleEntry(
                        priority=self._compute_priority(op), op=op)
                    self._dirty_ops.discard(op)
                heapq.heappush(self._heap, entry)
                break
            else:
                skipped.append(entry)

        # 将跳过的放回 heap
        for entry in skipped:
            heapq.heappush(self._heap, entry)

        if result is None and ensure_liveness:
            # Fallback: 完整的 liveness 检查
            return self._select_for_liveness()

        return result

    def _select_for_liveness(self):
        """Fallback: 等价于原版的 ensure_liveness 逻辑"""
        # 当所有 op 都被 backpressure 阻止时，选择一个
        # 能打破死锁的 op (详见 streaming_executor_state.py 中的 liveness 逻辑)
        pass
```

**在 StreamingExecutor 中集成**:

```python
# streaming_executor.py:

class StreamingExecutor(Executor, threading.Thread):
    def _initialize_operators(self, ...):
        # ... 现有初始化 ...

        # 新增: 当算子数量较多时使用优先队列优化
        self._use_priority_scheduler = len(self._topology) >= 8
        if self._use_priority_scheduler:
            self._priority_scheduler = _OperatorPriorityScheduler(
                self._topology,
                self._backpressure_policies,
                self._resource_manager,
                self._ranker,
            )

    def _scheduling_loop_step(self, topology):
        # ... process_completed_tasks ...

        # Dispatch 循环
        while True:
            if self._use_priority_scheduler:
                op = self._priority_scheduler.select_next(
                    ensure_liveness=self._consumer_idling())
            else:
                op = select_operator_to_run(
                    topology, self._resource_manager, self._backpressure_policies,
                    ensure_liveness=self._consumer_idling(), ranker=self._ranker,
                )

            if op is None:
                break
            topology[op].dispatch_next_task()

            # 通知优先队列该 op 状态变化
            if self._use_priority_scheduler:
                self._priority_scheduler.mark_dirty(op)

            self._resource_manager.update_usages()
```

##### 优先队列的适用性分析

| 场景 | 算子数 | select调用次数/轮 | 社区版耗时 | 优先队列耗时 | 收益 |
|------|--------|-------------------|-----------|-------------|------|
| 简单ETL | 3-5 | 5-20 | 5-20ms | 5-20ms | 无(开销可忽略) |
| 中等pipeline | 8-15 | 20-50 | 20-50ms | 5-15ms | 2-3x |
| 复杂推理链 | 20+ | 50-200 | 50-200ms | 10-30ms | 5-10x |

**推荐**: 只在 `len(topology) >= 8` 时启用优先队列。少量算子场景遍历开销极小，优先队列的 heap 维护反而是额外开销。

#### 10.2.8 综合优化: 批Dispatch + 动态Timeout + 优先队列

将三个优化组合的完整方案:

```python
# streaming_executor.py 优化后的 _scheduling_loop_step:

_BATCH_RESOURCE_CHECK_INTERVAL = 16

def _scheduling_loop_step(self, topology):
    """优化后的调度循环

    三重优化:
    1. 动态 ray.wait timeout: 有 pending work 时 timeout=0
    2. 批 dispatch: 选中 op 后批量 dispatch, 减少 select 调用
    3. 优先队列: 大 pipeline 时 O(1) 选择 op (而非 O(N))
    """

    # === Phase 0: 判断是否有 pending work ===
    has_pending_work = any(
        state.has_pending_bundles() and op.can_add_input()
        for op, state in topology.items()
    )

    # === Phase 1: 处理完成的 task (动态 timeout) ===
    self._resource_manager.update_usages()
    wait_timeout = 0.0 if has_pending_work else 0.1

    errored_blocks_per_op, _ = process_completed_tasks(
        topology, self._backpressure_policies, self._max_errored_blocks,
        timeout=wait_timeout,
    )

    # === Phase 2: 批 Dispatch 循环 ===
    self._resource_manager.update_usages()

    while True:
        # 选择下一个要调度的 operator
        if self._use_priority_scheduler:
            op = self._priority_scheduler.select_next(
                ensure_liveness=self._consumer_idling())
        else:
            op = select_operator_to_run(
                topology, self._resource_manager, self._backpressure_policies,
                ensure_liveness=self._consumer_idling(), ranker=self._ranker,
            )

        if op is None:
            break

        # 批量 dispatch 同一个 op 的多个 task
        op_state = topology[op]
        batch_count = 0
        while op_state.has_pending_bundles() and op.can_add_input():
            op_state.dispatch_next_task()
            batch_count += 1

            # 每 N 个 task 检查一次资源/backpressure
            if batch_count % _BATCH_RESOURCE_CHECK_INTERVAL == 0:
                self._resource_manager.update_usages()
                if not all(p.can_add_input(op) for p in self._backpressure_policies):
                    break

        # 批结束，更新资源 + 通知优先队列
        self._resource_manager.update_usages()
        if self._use_priority_scheduler:
            self._priority_scheduler.mark_dirty(op)

    # === Phase 3: Autoscaling + 状态更新 (不变) ===
    self._cluster_autoscaler.try_trigger_scaling()
    self._actor_autoscaler.try_trigger_scaling()
    # ...
```

**综合性能对比** (20算子, dispatch 100 tasks 到 5 个不同 op):

| 指标 | 社区版 | 仅动态timeout | +批dispatch | +优先队列 |
|------|--------|--------------|-------------|----------|
| `select_operator_to_run` 调用 | 101次 | 101次 | 5-6次 | 5-6次 (O(1) each) |
| `resource_manager.update` | 100次 | 100次 | 11次 | 11次 |
| `ray.wait` 无效等待 | 0-100ms | 0ms | 0ms | 0ms |
| select单次耗时(20 ops) | ~2ms | ~2ms | ~2ms | ~0.1ms |
| dispatch阶段总耗时 | ~200ms | ~200ms | ~55ms | ~52ms |
| 整轮循环耗时 | ~260ms | ~160ms | ~70ms | ~65ms |



---

## 总结: 修改文件索引

| 优化项 | 主要修改文件 | 层级 | 优先级 |
|--------|-------------|------|--------|
| Task/Actor 调度超时 | `ray_config_def.h`, `task_manager.h`, `normal_task_submitter.cc`, `gcs_actor_scheduler.cc` | Ray Core (C++) | P0 |
| Object 血缘回溯超时 | `object_recovery_manager.h/cc`, `ray_config_def.h` | Ray Core (C++) | P1 |
| Actor 黑名单 | `actor_pool_map_operator.py` (_ActorPool) | Ray Data (Python) | P0 |
| Autoscaling 重构 | `actor_pool_resizing_policy.py`, `autoscaling_actor_pool.py`, `default_actor_autoscaler.py` | Ray Data (Python) | P0 |
| 指数扩容 | `actor_pool_resizing_policy.py`, `context.py` | Ray Data (Python) | P1 |
| 序列化合并 | `actor_pool_map_operator.py` (start, _start_actor) | Ray Data (Python) | P1 |
| 不健康实例检测 | 新增 `issue_detector.py`, 修改 `actor_pool_map_operator.py` | Ray Data (Python) | P1 |
| 算子 Timeout | `actor_pool_map_operator.py`, `compute.py`, `streaming_executor.py` | Ray Data (Python) | P0 |
| 集群热更新 | `autoscaling_config.py`, `streaming_executor.py`, 新增 `operator_config_controller.py` | KubeRay + Ray Data | P2 |
| GCS 优化 | `ray_config_def.h`, `actor_manager.cc`, `gcs_task_manager.h/cc` | Ray Core (C++) | P1 |
| Driver 优化: 动态timeout | `streaming_executor.py`, `streaming_executor_state.py` | Ray Data (Python) | P1 |
| Driver 优化: 批dispatch | `streaming_executor.py` | Ray Data (Python) | P1 |
| Driver 优化: 优先队列 | `streaming_executor.py`, `streaming_executor_state.py` | Ray Data (Python) | P2 |
| Driver 优化: Actor状态刷新降频 | `actor_pool_map_operator.py` | Ray Data (Python) | P1 |

---

## 架构全景图

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                              用户应用层                                        │
│  ds.map_batches(udf, compute=ActorPoolStrategy(task_timeout_s=300))          │
├──────────────────────────────────────────────────────────────────────────────┤
│                         Ray Data 框架层 (Python)                              │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐     │
│  │ StreamingExecutor (主调度循环)                                       │     │
│  │  ├── OperatorConfigController [热更新]                              │     │
│  │  ├── process_completed_tasks [调度防超负荷]                          │     │
│  │  ├── select_operator_to_run [活跃op快速路径]                         │     │
│  │  └── check_task_timeouts [算子Timeout]                              │     │
│  └─────────────────────────────────────────────────────────────────────┘     │
│                                                                              │
│  ┌──────────────────────────────────────┐  ┌───────────────────────────┐    │
│  │ ActorPoolMapOperator                 │  │ DefaultActorAutoscaler    │    │
│  │  ├── _ActorPool                      │  │  ├── ExponentialPolicy    │    │
│  │  │   ├── 黑名单 (on_task_failed)     │  │  ├── AggressivePolicy     │    │
│  │  │   ├── 强制缩容                    │  │  └── DefaultPolicy        │    │
│  │  │   └── 增量状态刷新               │  └───────────────────────────┘    │
│  │  ├── UnhealthyActorDetector         │                                    │
│  │  ├── 序列化合并 (ray.put)            │                                    │
│  │  └── TaskTimeout 检查               │                                    │
│  └──────────────────────────────────────┘                                    │
├──────────────────────────────────────────────────────────────────────────────┤
│                         Ray Core 层 (C++)                                     │
│                                                                              │
│  ┌──────────────────────┐  ┌──────────────────┐  ┌─────────────────────┐   │
│  │ NormalTaskSubmitter   │  │ GcsActorScheduler │  │ ObjectRecoveryMgr   │   │
│  │  └── 调度超时检查     │  │  └── 调度超时检查  │  │  └── 血缘回溯超时   │   │
│  │     (周期性定时器)    │  │    (周期性定时器)  │  │    (周期性定时器)    │   │
│  └──────────────────────┘  └──────────────────┘  └─────────────────────┘   │
│                                                                              │
│  ┌──────────────────────┐  ┌──────────────────┐  ┌─────────────────────┐   │
│  │ GcsServer             │  │ ActorManager     │  │ CoreWorker          │   │
│  │  ├── TaskManager      │  │  └── 订阅重试    │  │  └── fetch重试      │   │
│  │  │   └── per-job限额  │  └──────────────────┘  └─────────────────────┘   │
│  │  └── 参数化调优       │                                                   │
│  └──────────────────────┘                                                    │
├──────────────────────────────────────────────────────────────────────────────┤
│                         KubeRay 集群管理层                                     │
│  ┌──────────────────────────────────────────────┐                            │
│  │ AutoscalingConfigProducer                    │                            │
│  │  └── CR annotation 动态配置 (热更新)          │                            │
│  └──────────────────────────────────────────────┘                            │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 实施路线图

### Phase 1 (P0 - 生产必备)

1. **Actor 黑名单** - 防止反复失败的 actor 持续消耗数据
2. **算子 Timeout** - 防止 UDF 死循环/网络阻塞导致作业卡死
3. **Task 调度超时** - 防止资源不足时作业永远 pending
4. **Autoscaling 重构** - 利用率计算纳入 pending actors，避免过度扩容

### Phase 2 (P1 - 性能优化)

5. **指数扩容** - 大规模场景冷启动加速
6. **序列化合并** - 降低 Driver CPU 开销
7. **不健康实例检测** - 自动识别并隔离慢 actor
8. **GCS 优化** - 降低集群管理面负载
9. **Object 血缘回溯超时** - 深层 DAG 的兜底
10. **Driver 优化** - 调度循环性能提升

### Phase 3 (P2 - 运维增强)

11. **集群热更新** - 支持不重启集群调整配置

---

## 配置汇总

### 新增 Ray Core 配置 (ray_config_def.h)

```cpp
RAY_CONFIG(int64_t, task_scheduling_timeout_ms, -1)
RAY_CONFIG(int64_t, actor_scheduling_timeout_ms, -1)
RAY_CONFIG(int64_t, object_lineage_reconstruction_timeout_ms, 300000)
RAY_CONFIG(int64_t, max_task_events_per_job, 10000)
RAY_CONFIG(int32_t, max_object_fetch_retries, 3)
```

### 新增 Ray Data 配置 (context.py / 环境变量)

```python
# Autoscaling 策略选择
RAY_DATA_ACTOR_POOL_RESIZING_POLICY = "default"  # "exponential" | "aggressive"
RAY_DATA_ACTOR_POOL_EXPONENTIAL_BASE = 2.0

# 动态配置开关
RAY_DATA_ENABLE_DYNAMIC_CONFIG = False
```

### 新增 ActorPoolStrategy 参数

```python
ActorPoolStrategy(
    task_timeout_s=300,  # 新增: 单个 task 超时
)
```

### GCS 调优环境变量 (推荐值)

```bash
RAY_raylet_report_resources_period_milliseconds=500
RAY_gcs_pull_resource_loads_period_milliseconds=5000
RAY_debug_dump_period_milliseconds=60000
RAY_fetch_warn_timeout_milliseconds=120000
RAY_fetch_fail_timeout_milliseconds=1200000
RAY_task_events_max_num_task_in_gcs=100000
```
