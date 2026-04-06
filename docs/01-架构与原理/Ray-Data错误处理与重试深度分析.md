# Ray Data 错误处理与重试机制深度分析

> 本文档整理自对 Ray Data 任务执行过程中 OOM、节点失败、错误块计数、重试机制等问题的深入分析讨论。

## 目录

1. [背景问题](#1-背景问题)
2. [Ray Core 重试机制](#2-ray-core-重试机制)
3. [Ray Data 错误块计数逻辑](#3-ray-data-错误块计数逻辑)
4. [NodeTerminated 事件分析](#4-nodeterminated-事件分析)
5. [Object Reconstruction vs 依赖重建](#5-object-reconstruction-vs-依赖重建)
6. [RaySystemError 异常体系](#6-raysystemerror-异常体系)
7. [ObjectLostError 处理流程](#7-objectlosterror-处理流程)
8. [上游 Task 重试与死循环问题](#8-上游-task-重试与死循环问题)
9. [Node Failure 日志解读与定位](#9-node-failure-日志解读与定位)
10. [Node 自动重启机制](#10-node-自动重启机制)
11. [关键代码位置索引](#11-关键代码位置索引)
12. [总结与最佳实践](#12-总结与最佳实践)

---

## 1. 背景问题

### 1.1 观察到的现象

在分析 Ray Dashboard 上的 Job（ID: 12000000）时，观察到以下现象：

- Dashboard 显示 **Errored Blocks = 7**
- 日志中出现 NodeTerminated 事件
- 出现以下日志信息：
  ```
  [2026-04-18 22:48:53,034 I 773649 773703] core_worker.cc:751: Node failure.
  All objects pinned on that node will be lost if object reconstruction is not enabled.
  node_id=1b06dc4266575649b946ee78113a87b96f17899665bcbfa32d7a96bb
  ```

### 1.2 核心疑问

1. OOM 导致 task 被 kill 和 Node 回收不是会无限重试吗？为什么会有 errored blocks？
2. 重试期间为什么会去获取结果？
3. 依赖重建和 Object Reconstruction 是同一个机制吗？
4. ObjectLostError 之后如何触发上游重算？会不会死循环？

---

## 2. Ray Core 重试机制

### 2.1 重试配置

Ray Data 默认使用无限重试：

```python
# python/ray/data/_internal/remote_fn.py:31-38
default_ray_remote_args = {
    "scheduling_strategy": "DEFAULT",
    "max_retries": -1,  # 无限重试
}
```

### 2.2 重试决策逻辑

重试决策在 `task_manager.cc` 的 `RetryOrFailTask` 方法中实现：

```cpp
// src/ray/core_worker/task_manager.cc:1138-1250

bool TaskManager::RetryOrFailTask(
    const TaskID &task_id,
    rpc::ErrorType error_type,
    const Status *status,
    const rpc::RayErrorInfo *ray_error_info,
    bool should_mark_task_failed) {

    // 检查重试次数
    if (task_entry.num_retries_left_ == 0) {
        // 没有重试次数了，标记失败
        FailPendingTask(task_id, error_type, ...);
        return false;
    }

    // 特殊情况：被抢占的节点不消耗重试次数
    bool was_node_preempted =
        ray_error_info &&
        ray_error_info->has_actor_died_error() &&
        ray_error_info->actor_died_error().preempted();

    // OOM 有单独的重试计数器
    if (error_type == rpc::ErrorType::OUT_OF_MEMORY) {
        if (task_entry.num_oom_retries_left_ == 0) {
            FailPendingTask(task_id, error_type, ...);
            return false;
        }
        if (task_entry.num_oom_retries_left_ > 0) {
            task_entry.num_oom_retries_left_--;
        }
    }

    // 执行重试
    ScheduleRetry(task_entry);
    return true;
}
```

### 2.3 重试类型

| 错误类型 | 重试行为 | 是否消耗重试次数 |
|----------|----------|------------------|
| NODE_DIED | 自动重试 | 是（除非被抢占） |
| OUT_OF_MEMORY | 自动重试 | 消耗 OOM 专用计数器 |
| TASK_CANCELLED | 不重试 | - |
| ACTOR_DIED (preempted) | 自动重试 | **否** |
| RaySystemError | 自动重试 | 是 |

### 2.4 关键点：重试是异步的

```
Task 执行
    │
    ▼
Task 失败 (NODE_DIED/OOM 等)
    │
    ├──► Ray Core 检测到失败
    │        │
    │        ▼
    │    RetryOrFailTask()
    │        │
    │        ├── num_retries_left > 0 ?
    │        │       │
    │        │       ├─ Yes ─► ScheduleRetry() ─► 重新执行
    │        │       │
    │        │       └─ No ──► FailPendingTask() ─► 产生 Error Object
    │        │
    │        └── 同时，原始 ObjectRef 的 ray.get() 可能在其他地方被调用
    │
    └──► Ray Data Executor 调用 ray.get() 获取结果
             │
             ├─ 如果 Task 正在重试中 ─► 等待
             │
             └─ 如果 Task 已失败 ─► 抛出异常 ─► errored_blocks++
```

---

## 3. Ray Data 错误块计数逻辑

### 3.1 计数位置

错误块计数在 `streaming_executor_state.py` 中的 `process_completed_tasks` 函数：

```python
# python/ray/data/_internal/execution/streaming_executor_state.py:606-636

try:
    # 尝试获取 task 的 metadata
    task.prepare_metadata()
except Exception as e:
    # 异常发生时计数
    errored_blocks_per_op[state] += 1
    num_errored_blocks += 1

    should_ignore = (
        max_errored_blocks < 0  # -1 表示无限容错
        or max_errored_blocks >= num_errored_blocks
    )

    if should_ignore:
        # 记录错误但继续执行
        logger.error(error_message, exc_info=e)
    else:
        # 超过阈值，终止 pipeline
        raise e from None
```

### 3.2 另一个计数点：获取 metadata 时

```python
# python/ray/data/_internal/execution/streaming_executor_state.py:695-725

try:
    # Metadata is ready locally, ray.get won't block
    meta_with_schema = ray.get(meta_ref, timeout=0)
    bytes_read = task.complete_with_metadata(meta_with_schema)
except Exception as e:
    errored_blocks_per_op[state] += 1
    num_errored_blocks += 1
    # ... 同样的处理逻辑
```

### 3.3 重试与错误块计数的关系

```
┌─────────────────────────────────────────────────────────────────┐
│                    两个独立的并行机制                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Ray Core 层面 (C++)           │    Ray Data 层面 (Python)       │
│  ─────────────────────         │    ──────────────────────       │
│                                │                                  │
│  Task 执行失败                  │    Executor 调度循环             │
│      │                         │        │                        │
│      ▼                         │        ▼                        │
│  RetryOrFailTask()             │    process_completed_tasks()    │
│      │                         │        │                        │
│      ▼                         │        ▼                        │
│  max_retries > 0 ?             │    ray.get(meta_ref) 成功？     │
│      │                         │        │                        │
│   ┌──┴──┐                      │     ┌──┴──┐                     │
│   │     │                      │     │     │                     │
│  Yes   No                      │   Yes    No                     │
│   │     │                      │     │     │                     │
│   ▼     ▼                      │     ▼     ▼                     │
│ 重试  FailTask                 │   继续  errored_blocks++        │
│                                │                                  │
│  这两个机制是并行独立运行的！                                      │
│  Ray Core 在重试的同时，Ray Data 可能已经尝试获取结果并失败       │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 3.4 为什么无限重试还有 errored blocks？

**根本原因**：Ray Data Executor 的调度循环和 Ray Core 的重试机制是**异步并行**的。

```
时间线示例：

T0: Task 开始执行
T1: Node 挂掉，Task 失败
T2: Ray Core 开始准备重试（ScheduleRetry）
T3: Ray Data Executor 调用 ray.get() 尝试获取结果
    └─► 此时 Task 还未重试完成，ObjectRef 对应的是 Error
    └─► 异常被捕获，errored_blocks++
T4: Ray Core 完成重试调度，新 Task 开始执行
T5: 新 Task 成功完成
T6: 但 errored_blocks 已经 +1 了
```

---

## 4. NodeTerminated 事件分析

### 4.1 节点终止原因

节点终止有三种主要原因：

| 原因 | 说明 | 对 Task 的影响 |
|------|------|----------------|
| `AUTOSCALER_DRAIN_PREEMPTED` | 节点被抢占（如 Spot 实例） | Task 重试，不消耗重试次数 |
| `AUTOSCALER_DRAIN_IDLE` | 节点空闲被缩容 | Task 重试，消耗重试次数 |
| `UNEXPECTED_TERMINATION` | 节点意外崩溃 | Task 重试，消耗重试次数 |

### 4.2 节点失败处理流程

```cpp
// src/ray/core_worker/core_worker.cc:737-763

// 订阅 GCS 节点状态变化
gcs_client_->Nodes().SubscribeAllOrDie(
    [this, &reference_counter](const rpc::GcsNodeInfo &data) {
        const auto node_id = NodeID::FromBinary(data.node_id());

        if (data.state() == rpc::GcsNodeInfo::DEAD) {
            // 节点死亡
            RAY_LOG(INFO).WithField(node_id)
                << "Node failure. All objects pinned on that node will be lost "
                << "if object reconstruction is not enabled.";

            // 重置该节点上的所有对象
            reference_counter->ResetObjectsOnRemovedNode(node_id);
        }
    });
```

### 4.3 ResetObjectsOnRemovedNode 处理

```cpp
// src/ray/core_worker/reference_counter.cc:893-908

void ReferenceCounter::ResetObjectsOnRemovedNode(const NodeID &node_id) {
    absl::MutexLock lock(&mutex_);

    for (auto it = object_id_refs_.begin(); it != object_id_refs_.end(); it++) {
        const auto &object_id = it->first;

        // 检查对象是否在该节点上
        if (it->second.pinned_at_node_id_ == node_id ||
            it->second.spilled_node_id == node_id) {

            // 清除对象的主副本位置
            UnsetObjectPrimaryCopy(it);

            // 如果对象还在作用域内，加入恢复队列
            if (!it->second.OutOfScope(lineage_pinning_enabled_)) {
                objects_to_recover_.push_back(object_id);
            }
        }
    }
}
```

---

## 5. Object Reconstruction vs 依赖重建

### 5.1 核心结论

**它们是同一机制的两个触发路径**，但有不同的触发条件和行为。

### 5.2 机制对比

```
┌─────────────────────────────────────────────────────────────────┐
│                    Object 丢失时的恢复路径                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  路径 A: Task Retry (依赖重建)                                    │
│  ─────────────────────────────                                   │
│  触发条件: Task 执行失败 (NODE_DIED/OOM等)                        │
│  入口: task_manager.cc:1170+ RetryOrFailTask()                   │
│                                                                  │
│      TaskEntry.num_retries_left_ > 0 ?                          │
│              │                                                   │
│              ▼                                                   │
│      async_retry_task_callback_(spec, delay_ms)                 │
│              │                                                   │
│              ▼                                                   │
│      Core Worker 重新提交 Task                                   │
│              │                                                   │
│              ▼                                                   │
│      输入依赖自动重新 ray.get() → 触发依赖的恢复                   │
│                                                                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  路径 B: Object Reconstruction (lineage 重建)                    │
│  ─────────────────────────────────────────                       │
│  触发条件: ray.get(object_ref) 时 object 丢失                    │
│  前提: enable_object_reconstruction=True (默认 False)           │
│  入口: object_recovery_manager.cc:24 RecoverObject()             │
│                                                                  │
│      1. 检查是否有 secondary copy (其他节点)                      │
│              │                                                   │
│              ▼ 没有 copy                                         │
│      2. ReconstructObject() → ResubmitTask()                    │
│              │                                                   │
│              ▼                                                   │
│      3. 递归恢复 task 的依赖: RecoverObject(dep)                 │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 5.3 关键代码：Object Reconstruction

```cpp
// src/ray/core_worker/object_recovery_manager.cc:140-188

void ObjectRecoveryManager::ReconstructObject(const ObjectID &object_id) {
    // 检查 lineage 是否可重建
    LineageReconstructionEligibility eligibility =
        reference_counter_.GetLineageReconstructionEligibility(object_id);

    if (eligibility != LineageReconstructionEligibility::ELIGIBLE) {
        // 不可重建，返回 OBJECT_LOST 错误
        auto error_type = ToErrorType(eligibility).value_or(rpc::ErrorType::OBJECT_LOST);
        recovery_failure_callback_(object_id, error_type, /*pin_object=*/true);
        return;
    }

    // 关键: 调用 TaskManager.ResubmitTask() 重新执行生成该 object 的 task
    const auto task_id = object_id.TaskId();
    std::vector<ObjectID> task_deps;
    auto error_type_optional = task_manager_.ResubmitTask(task_id, &task_deps);

    if (!error_type_optional.has_value()) {
        // 递归恢复 task 的依赖
        for (const auto &dep : task_deps) {
            auto error = RecoverObject(dep);  // 递归调用
            if (error.has_value()) {
                recovery_failure_callback_(dep, *error, /*pin_object=*/false);
            }
        }
    }
}
```

### 5.4 lineage_pinning_enabled 检查

```cpp
// src/ray/core_worker/reference_counter.cc:1650-1654

LineageReconstructionEligibility ReferenceCounter::GetLineageReconstructionEligibility(
    const ObjectID &object_id) const {

    if (!lineage_pinning_enabled_) {
        // 默认返回 INELIGIBLE，Object Reconstruction 不可用
        return LineageReconstructionEligibility::INELIGIBLE_LINEAGE_DISABLED;
    }
    // ...
}
```

### 5.5 两者的本质区别

| 维度 | Task Retry (依赖重建) | Object Reconstruction |
|------|---------------------|----------------------|
| **触发时机** | Task 执行失败时 | `ray.get()` 发现 object 丢失时 |
| **默认状态** | 始终启用 (`max_retries=-1`) | **默认关闭** |
| **启用方式** | `@ray.remote(max_retries=N)` | `ray.init(enable_object_reconstruction=True)` |
| **依赖处理** | 隐式 - 重新执行时自动 `ray.get()` 输入 | 显式 - 递归调用 `RecoverObject(dep)` |
| **lineage 保留** | 不需要保留完整 lineage | 需要 `lineage_pinning_enabled_=True` |
| **内存开销** | 低 - 只保留当前 task spec | 高 - 保留所有 task 的 spec 直到 object 不再引用 |

### 5.6 为什么 Ray Data 只用 Task Retry？

1. **流式执行**：上游 Stage 的 task 也在执行，丢失的 block 会被重新生成
2. **Block 生命周期短**：一旦下游消费完，block 就被释放，不需要长期保留 lineage
3. **避免内存膨胀**：Object Reconstruction 需要保留所有 task spec（lineage），对大规模数据处理不友好

---

## 6. RaySystemError 异常体系

### 6.1 异常层次结构

```python
# python/ray/exceptions.py

RayError (基类)
    │
    ├── RaySystemError  # 系统级错误，可重试
    │       │
    │       ├── RayChannelError         # Channel 相关错误
    │       │       │
    │       │       └── RayChannelTimeoutError  # Channel 超时
    │       │
    │       ├── RayCgraphCapacityExceeded  # Compiled Graph 容量超限
    │       │
    │       └── RayDirectTransportError    # 直接传输错误
    │
    ├── ObjectLostError      # 对象丢失 (注意：不是 RaySystemError 的子类!)
    │
    ├── NodeDiedError        # 节点死亡 (注意：不是 RaySystemError 的子类!)
    │
    └── ObjectReconstructionFailedError  # 对象重建失败
```

### 6.2 RaySystemError 定义

```python
# python/ray/exceptions.py:495-509

@PublicAPI
class RaySystemError(RayError):
    """Indicates that Ray encountered a system error.
    This exception can be thrown when the raylet is killed.
    """
    def __init__(self, client_exc, traceback_str=None):
        self.client_exc = client_exc
        self.traceback_str = traceback_str
```

### 6.3 Ray Data 的 retry_exceptions 处理

```python
# python/ray/data/_internal/remote_fn.py:59-80

def _add_system_error_to_retry_exceptions(ray_remote_args) -> None:
    """Modify the remote args so that Ray retries `RaySystemError`s."""
    retry_exceptions = ray_remote_args.get("retry_exceptions", False)

    if isinstance(retry_exceptions, list) and RaySystemError not in retry_exceptions:
        retry_exceptions.append(ray.exceptions.RaySystemError)
    elif not retry_exceptions:
        retry_exceptions = [ray.exceptions.RaySystemError]

    ray_remote_args["retry_exceptions"] = retry_exceptions
```

**重要**：`ObjectLostError` 和 `NodeDiedError` **不是** `RaySystemError` 的子类，它们直接继承自 `RayError`。

---

## 7. ObjectLostError 处理流程

### 7.1 完整流程图

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  默认配置: enable_object_reconstruction=False (lineage_pinning_enabled=false)│
└─────────────────────────────────────────────────────────────────────────────┘

Node 挂掉
    │
    ▼
ResetObjectsOnRemovedNode() [reference_counter.cc:893]
    │  将丢失的 objects 加入 objects_to_recover_ 队列
    ▼
FlushObjectsToRecover() [每 100ms 周期执行, core_worker.cc:469]
    │
    ▼
ObjectRecoveryManager::RecoverObject(object_id) [object_recovery_manager.cc:24]
    │
    ├─► 首先检查是否有 secondary copy (其他节点有副本)
    │       │
    │       ├─► 如果有: PinExistingObjectCopy() → 成功恢复，无需重算
    │       │
    │       └─► 如果没有: ReconstructObject()
    │                │
    │                ▼
    │       GetLineageReconstructionEligibility() [reference_counter.cc:1650]
    │                │
    │       ┌────────┴────────┐
    │       │                 │
    │       ▼                 ▼
    │  lineage_pinning_     lineage_pinning_
    │  enabled_=TRUE        enabled_=FALSE (默认)
    │       │                 │
    │       ▼                 ▼
    │  返回 ELIGIBLE      返回 INELIGIBLE_LINEAGE_DISABLED
    │       │                 │
    │       ▼                 ▼
    │  ResubmitTask()     recovery_failure_callback_()
    │  (重新执行生成         [core_worker_process.cc:660]
    │   该 object 的              │
    │   task + 递归                ▼
    │   恢复依赖)            Put(RayObject(ErrorType::OBJECT_LOST))
    │                             │
    │                             ▼
    │                      ray.get() 抛出 ObjectLostError
    │                             │
    │                             ▼
    │                      Task 失败，触发 Task Retry
    │                      (因为 max_retries=-1)
    │                             │
    │                             ▼
    │                      重新执行 Task → 重新 ray.get(input_refs)
    │                             │
    │                             ▼
    │                      如果上游 Task 也重试了，input 最终可用
    │                      如果上游没重试，继续抛 ObjectLostError
    │                      → 无限重试直到成功
    │
    └──────────────────────────────────────────────────────────────────────────►
```

### 7.2 recovery_failure_callback_ 实现

```cpp
// src/ray/core_worker/core_worker_process.cc:660-668

[this](const ObjectID &object_id, rpc::ErrorType reason, bool pin_object) {
    RAY_LOG(DEBUG).WithField(object_id)
        << "Failed to recover object due to " << rpc::ErrorType_Name(reason);
    auto core_worker = GetCoreWorker();

    // 将 OBJECT_LOST error 写入 object store
    RAY_UNUSED(core_worker->Put(RayObject(reason),  // ErrorType::OBJECT_LOST
                                /*contained_object_ids=*/{},
                                object_id,
                                /*pin_object=*/pin_object));
}
```

---

## 8. 上游 Task 重试与死循环问题

### 8.1 核心问题

**上游 task 会被主动触发重试吗？**

**答案**：在默认配置下（`enable_object_reconstruction=False`），**不会**。上游 task 只有在它**自己执行失败**时才会重试。

### 8.2 场景分析

#### 场景 A: 下游在数据丢失前已获取数据

```
T0: Task 1.1 完成 → Block A 在 Node X
T1: Task 2.1 开始，dependency resolver 拿到 Block A (内联到 task spec)
T2: Node X 挂掉
T3: Task 2.1 正常执行（数据已在本地），输出 Block B
    ✅ 没有问题
```

#### 场景 B: 下游在数据丢失时还未获取数据

```
T0: Task 1.1 完成 → Block A 在 Node X (plasma)
T1: Task 2.1 提交，开始 dependency resolution (GetAsync 等待)
T2: Node X 挂掉
    │
    ├─► lineage_pinning_enabled=False (默认)
    │       │
    │       ▼
    │   recovery_failure_callback_(Block A, OBJECT_LOST)
    │       │
    │       ▼
    │   Put(RayObject(OBJECT_LOST), Block A)
    │       │
    │       ▼
    │   GetAsync callback 被触发，收到 Error object
    │       │
    │       ▼
    │   Task 2.1 被调度执行
    │       │
    │       ▼
    │   Worker 执行 task，发现参数是 Error object
    │       │
    │       ▼
    │   抛出 ObjectLostError 到 Python
    │       │
    │       ▼
    │   Task 失败，触发 retry
    │       │
    │       ▼
    │   重复尝试...
    │
    └─► lineage_pinning_enabled=True (手动开启)
            │
            ▼
        ResubmitTask(Task 1.1) ← 主动重建上游！
            │
            ▼
        Task 1.1 重新执行，生成新的 Block A
            │
            ▼
        Task 2.1 的 GetAsync 收到新数据
```

### 8.3 会不会死循环？

**可能会！** 如果满足以下条件：

1. 上游 block 永久丢失
2. `enable_object_reconstruction=False`（默认）
3. 上游 task 已经完成，lineage 被 GC
4. `max_errored_blocks=-1`（默认无限容错）

```python
# 死循环场景：

Stage 1 Task 1.1 完成 → 输出 Block A
Stage 1 Task 1.1 的 lineage 被 GC
Node X 挂掉 → Block A 丢失
Stage 2 Task 2.1 重试 → ray.get(Block A) → OBJECT_LOST
Stage 2 Task 2.1 再重试 → ray.get(Block A) → OBJECT_LOST
Stage 2 Task 2.1 再重试 → ray.get(Block A) → OBJECT_LOST
... 无限循环！
```

### 8.4 为什么实际生产中通常不会卡死？

1. **`max_errored_blocks` 兜底**：
   ```python
   should_ignore = (
       max_errored_blocks < 0  # 默认 -1 = 无限容错
       or max_errored_blocks >= num_errored_blocks
   )
   ```
   如果设置了 `max_errored_blocks > 0`，超过阈值会终止 pipeline。

2. **Ray Data 的设计假设**：
   - 流式执行中，上游 Stage 的 task 完成后，下游应该**立即消费**
   - 如果下游及时消费，数据在 plasma 中的存活时间很短
   - 只有在下游严重 lag 或节点频繁挂掉时，才会出现死循环

3. **实际解决方案**：
   - 开启 `enable_object_reconstruction=True`（代价是内存占用增加）
   - 使用 **checkpoint**（定期保存中间结果到持久存储）

### 8.5 总结表格

| 问题 | 答案 |
|------|------|
| 上游 task 会被主动触发重试吗？ | **不会**（除非开启 `enable_object_reconstruction`） |
| 下游拿不到数据会怎样？ | 下游 task 失败并重试，再次尝试 `ray.get` |
| 会死循环吗？ | **可能会**，如果上游 block 永久丢失且 `max_errored_blocks=-1` |
| 为什么 job 只有 7 个 errored blocks？ | 大部分情况下数据能恢复（其他副本/上游重新生成） |

---

## 9. Node Failure 日志解读与定位

### 9.1 日志解释

```
[2026-04-18 22:48:53,034 I 773649 773703] core_worker.cc:751: Node failure.
All objects pinned on that node will be lost if object reconstruction is not enabled.
node_id=1b06dc4266575649b946ee78113a87b96f17899665bcbfa32d7a96bb
```

**日志字段含义**：

| 字段 | 含义 |
|------|------|
| `I` | INFO 级别日志 |
| `773649` | 进程 ID |
| `773703` | 线程 ID |
| `core_worker.cc:751` | 代码位置 |
| `node_id=1b06dc...` | 失败的节点 ID（28 字节 hex = 56 字符） |

**触发位置** (`core_worker.cc:737-763`)：

```cpp
// 订阅 GCS 节点状态变化
gcs_client_->Nodes().SubscribeAllOrDie(
    [this, &reference_counter](const rpc::GcsNodeInfo &data) {
        const auto node_id = NodeID::FromBinary(data.node_id());

        if (data.state() == rpc::GcsNodeInfo::DEAD) {
            // 打印这条日志
            RAY_LOG(INFO).WithField(node_id)
                << "Node failure. All objects pinned on that node will be lost "
                << "if object reconstruction is not enabled.";

            // 重置该节点上的所有对象
            reference_counter->ResetObjectsOnRemovedNode(node_id);
        }
    });
```

**这条日志意味着**：
1. GCS 检测到某个节点死亡（`state == DEAD`）
2. GCS 通知了所有订阅者（包括这个 core worker）
3. 该 core worker 正在处理节点失败事件

### 9.2 NodeDeathInfo 结构

节点死亡原因记录在 `NodeDeathInfo` 中：

```protobuf
// src/ray/protobuf/common.proto:347-359
message NodeDeathInfo {
  enum Reason {
    UNSPECIFIED = 0;
    EXPECTED_TERMINATION = 1;      // 预期的终止（正常关闭）
    UNEXPECTED_TERMINATION = 2;    // 意外终止（心跳丢失）
    AUTOSCALER_DRAIN_PREEMPTED = 3; // 被 Autoscaler 抢占
    AUTOSCALER_DRAIN_IDLE = 4;     // 空闲缩容
  }
  Reason reason = 1;
  string reason_message = 2;  // 详细描述
}
```

### 9.3 GCS 推断死亡原因逻辑

GCS 在 `gcs_node_manager.cc:539-564` 推断死亡原因：

```cpp
rpc::NodeDeathInfo GcsNodeManager::InferDeathInfo(const NodeID &node_id) {
    auto iter = draining_nodes_.find(node_id);
    rpc::NodeDeathInfo death_info;

    // 检查是否在 draining 列表中
    bool expect_force_termination;
    if (iter == draining_nodes_.end()) {
        expect_force_termination = false;
    } else if (iter->second->deadline_timestamp_ms() == 0) {
        expect_force_termination = false;
    } else {
        // 检查是否超过 deadline 且是抢占类型
        expect_force_termination =
            (current_sys_time_ms() > iter->second->deadline_timestamp_ms()) &&
            (iter->second->reason() == DRAIN_NODE_REASON_PREEMPTION);
    }

    if (expect_force_termination) {
        // 被抢占（如 Spot 实例回收）
        death_info.set_reason(rpc::NodeDeathInfo::AUTOSCALER_DRAIN_PREEMPTED);
        death_info.set_reason_message(iter->second->reason_message());
    } else {
        // 意外终止（心跳丢失）
        death_info.set_reason(rpc::NodeDeathInfo::UNEXPECTED_TERMINATION);
        death_info.set_reason_message(
            "health check failed due to missing too many heartbeats");
    }
    return death_info;
}
```

### 9.4 如何定位 Node Failure 原因

#### 方法 1: 查看 GCS 日志

```bash
# 在 head 节点查看 GCS 日志
grep "death reason" /tmp/ray/session_latest/logs/gcs_server.out

# 输出示例：
# death reason = UNEXPECTED_TERMINATION, death message = health check failed due to missing too many heartbeats
# death reason = AUTOSCALER_DRAIN_PREEMPTED, death message = Spot instance preemption
```

#### 方法 2: 使用 Ray Dashboard

Dashboard 的 Cluster 页面会显示节点状态和死亡原因。

#### 方法 3: 使用 State API

```python
from ray.util.state import list_nodes

nodes = list_nodes(filters=[("state", "=", "DEAD")])
for node in nodes:
    print(f"Node: {node['node_id']}")
    print(f"  Death reason: {node.get('death_info', {}).get('reason')}")
    print(f"  Death message: {node.get('death_info', {}).get('reason_message')}")
```

#### 方法 4: 查看 Raylet 日志（在失败节点上，如果还能访问）

```bash
# 查看 raylet 日志
grep -E "OOM|SIGTERM|SIGKILL|error|fatal" /tmp/ray/session_latest/logs/raylet.out

# 常见原因:
# - OOM Killer 杀掉了 raylet
# - raylet 进程崩溃
# - 网络问题导致心跳超时
```

### 9.5 常见原因对照表

| 原因 | 日志特征 | 根因 |
|------|----------|------|
| `UNEXPECTED_TERMINATION` + "missing heartbeats" | 心跳超时 | OOM/网络问题/进程崩溃 |
| `AUTOSCALER_DRAIN_PREEMPTED` | 明确标记抢占 | Spot 实例被云厂商回收 |
| `AUTOSCALER_DRAIN_IDLE` | 明确标记空闲 | 集群缩容，节点被正常下线 |
| `EXPECTED_TERMINATION` | 明确标记预期 | `ray stop` 或正常关闭 |

### 9.6 进一步排查 UNEXPECTED_TERMINATION

如果是 `UNEXPECTED_TERMINATION`，需要进一步排查：

```bash
# 1. 检查系统日志（OOM killer）
dmesg | grep -i "killed process"
journalctl -k | grep -i "out of memory"

# 2. 检查 raylet 进程是否被杀
cat /tmp/ray/session_latest/logs/raylet.err

# 3. 检查网络连通性
# 心跳超时可能是网络问题
ping <gcs_address>
```

---

## 10. Node 自动重启机制

### 10.1 Node 会自动重启吗？

**答案：取决于部署环境和配置。**

| 部署环境 | 自动重启？ | 机制 |
|----------|-----------|------|
| **KubeRay (K8s)** | ✅ 是 | K8s Pod 自动重启 |
| **Ray Autoscaler (云)** | ✅ 是 | Autoscaler 检测到心跳丢失后尝试恢复 |
| **手动部署 (裸机)** | ❌ 否 | 需要外部机制（systemd 等） |
| **ray start 本地** | ❌ 否 | 进程死亡后不会自动重启 |

### 10.2 KubeRay 环境（KML 平台）

在 KubeRay/KML 环境中，**Node 会自动重启**：

```
Worker Pod 崩溃 (OOM/进程挂掉)
       │
       ▼
K8s 检测到 Pod 状态异常
       │
       ▼
K8s 根据 RestartPolicy 重启 Pod
       │
       ▼
新的 raylet 进程启动
       │
       ▼
生成新的 Node ID，向 GCS 注册
       │
       ▼
集群恢复
```

**关键配置**（在 RayCluster CR 中）：
```yaml
spec:
  workerGroupSpecs:
    - replicas: 10
      restartPolicy: Always  # Pod 崩溃后自动重启
```

### 10.3 Ray Autoscaler 环境（AWS/GCP/Azure）

Ray Autoscaler 有**节点恢复**机制：

```python
# python/ray/autoscaler/_private/autoscaler.py:1274-1320

def attempt_to_recover_unhealthy_nodes(self, now):
    """尝试恢复不健康的节点"""
    for node_id in self.non_terminated_nodes.worker_ids:
        self.recover_if_needed(node_id, now)

def recover_if_needed(self, node_id, now):
    if not self.can_update(node_id):
        return
    if self.heartbeat_on_time(node_id, now):
        return  # 心跳正常，不需要恢复

    # 心跳丢失，尝试恢复
    logger.warning(
        "StandardAutoscaler: "
        f"{node_id}: No recent heartbeat, "
        "restarting Ray to recover..."
    )

    # 启动 NodeUpdater 来重启 Ray
    updater = NodeUpdaterThread(
        node_id=node_id,
        # ...
        ray_start_commands=self.config["worker_start_ray_commands"],
        for_recovery=True,  # 标记为恢复操作
    )
    updater.start()
```

**恢复流程**：

```
Autoscaler 检测到心跳丢失
       │
       ▼
调用 recover_if_needed()
       │
       ├─► 如果 VM 还在运行：
       │       │
       │       ▼
       │   SSH 到节点执行 ray start 命令
       │       │
       │       ▼
       │   raylet 重启（新的 Node ID）
       │
       └─► 如果 VM 已终止：
               │
               ▼
           Autoscaler 根据资源需求决定是否启动新 VM
               │
               ▼
           如果需要，调用云 API 创建新实例
```

### 10.4 手动部署 / 本地运行

**不会自动重启**，需要外部机制：

```bash
# 方法 1: 使用 systemd（Linux）
[Unit]
Description=Ray Worker

[Service]
ExecStart=/usr/bin/ray start --address=<head_ip>:6379 --block
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

```bash
# 方法 2: 使用 supervisor
[program:ray-worker]
command=ray start --address=<head_ip>:6379 --block
autorestart=true
```

### 10.5 Node ID 生成逻辑

**重启后是新的 Node ID**，每次启动都会生成随机 ID：

```python
# python/ray/_private/node.py:241-244
if os.environ.get("RAY_OVERRIDE_NODE_ID_FOR_TESTING"):
    node_id = os.environ["RAY_OVERRIDE_NODE_ID_FOR_TESTING"]
else:
    node_id = ray.NodeID.from_random().hex()  # 随机生成！
```

然后传递给 raylet：

```cpp
// src/ray/raylet/main.cc:453
ray::NodeID raylet_node_id = ray::NodeID::FromHex(node_id);

// src/ray/raylet/main.cc:1072
self_node_info.set_node_id(raylet_node_id.Binary());
```

### 10.6 重新注册流程

```
┌─────────────────────────────────────────────────────────────────┐
│                    Node 失败与重启流程                           │
└─────────────────────────────────────────────────────────────────┘

原始状态:
  Node A (ID: abc123...) 在集群中运行
      │
      ▼
Node A 失败（OOM/网络/崩溃）
      │
      ├─► GCS 检测到心跳丢失
      │       │
      │       ▼
      │   InferDeathInfo() → UNEXPECTED_TERMINATION
      │       │
      │       ▼
      │   RemoveNode(abc123..., death_info, DEAD)
      │       │
      │       ▼
      │   通知所有订阅者（包括 core_worker）
      │       │
      │       ▼
      │   打印日志: "Node failure. All objects pinned on that node..."
      │
      └─► Node A 进程结束

如果 Autoscaler/K8s 决定重启:
      │
      ▼
新的 raylet 进程启动
      │
      ▼
node_id = ray.NodeID.from_random().hex()  # 生成新的 ID: xyz789...
      │
      ▼
raylet 向 GCS 注册: RegisterNode(xyz789...)
      │
      ▼
GCS 将其视为【新节点】（与 abc123 无关）
```

### 10.7 为什么设计成新的 Node ID？

1. **简化状态管理**：
   - 不需要处理"同一节点重启"的复杂状态
   - GCS 将其视为全新节点，清晰明了

2. **避免脏状态**：
   - 旧节点上的 actor、object、task 状态都已失效
   - 用新 ID 可以确保不会误用旧状态

3. **云环境兼容**：
   - 在 K8s/云环境中，Pod 重启后 IP 可能变化
   - 使用新 ID 避免混淆

### 10.8 重启后的影响

| 方面 | 影响 |
|------|------|
| **对象** | 旧节点上的对象丢失，需要重建或恢复 |
| **Actor** | 在该节点上的 actor 死亡，需要重新调度 |
| **Task** | 运行中的 task 失败，触发重试 |
| **资源** | 旧节点的资源释放，新节点重新汇报资源 |

### 10.9 如何追踪同一物理节点的多次注册

```python
from ray.util.state import list_nodes

# 列出所有节点（包括死亡的）
nodes = list_nodes(filters=[])
for node in nodes:
    print(f"Node ID: {node['node_id'][:16]}...")
    print(f"  State: {node['state']}")
    print(f"  IP: {node['node_manager_address']}")
    print(f"  Start Time: {node.get('start_time_ms')}")
    print(f"  End Time: {node.get('end_time_ms')}")
    print()

# 你可能会看到：
# Node ID: 1b06dc4266575649... (旧的，DEAD)
# Node ID: xyz789abcdef1234... (新的，ALIVE，相同 IP)
```

或者通过 `instance_id`（云环境）：

```python
# 云环境中，同一实例的多次注册会有相同的 instance_id
print(f"  Instance ID: {node.get('instance_id')}")
```

### 10.10 Node 重启总结

| 问题 | 答案 |
|------|------|
| KML/KubeRay 中 Node 会自动重启吗？ | **会**，K8s 会重启 Pod |
| Autoscaler 云环境中会自动重启吗？ | **会**，Autoscaler 会尝试恢复或启动新实例 |
| 手动部署会自动重启吗？ | **不会**，需要 systemd/supervisor 等外部机制 |
| 重启后是同一个 Node ID 吗？ | **不是**，每次启动都是新的随机 ID |
| 重启后上面的 Task/Object 怎么办？ | Task 会重试（`max_retries=-1`），Object 可能丢失 |

---

## 11. 关键代码位置索引

### 11.1 Ray Core (C++)

| 文件 | 行号 | 功能 |
|------|------|------|
| `src/ray/core_worker/task_manager.cc` | 1138-1250 | RetryOrFailTask - 重试决策 |
| `src/ray/core_worker/task_manager.cc` | 400-412 | ResubmitTask - 任务重提交 |
| `src/ray/core_worker/core_worker.cc` | 737-763 | 节点失败处理 |
| `src/ray/core_worker/core_worker.cc` | 467-491 | RecoverObjects 周期任务 |
| `src/ray/core_worker/reference_counter.cc` | 893-908 | ResetObjectsOnRemovedNode |
| `src/ray/core_worker/reference_counter.cc` | 1650-1654 | GetLineageReconstructionEligibility |
| `src/ray/core_worker/object_recovery_manager.cc` | 24-91 | RecoverObject |
| `src/ray/core_worker/object_recovery_manager.cc` | 140-188 | ReconstructObject |
| `src/ray/core_worker/core_worker_process.cc` | 653-668 | ObjectRecoveryManager 创建 + 失败回调 |
| `src/ray/core_worker/task_submission/dependency_resolver.cc` | 97-203 | ResolveDependencies |

### 9.2 Ray Data (Python)

| 文件 | 行号 | 功能 |
|------|------|------|
| `python/ray/data/_internal/remote_fn.py` | 31-44 | cached_remote_fn - 默认 max_retries=-1 |
| `python/ray/data/_internal/remote_fn.py` | 59-80 | _add_system_error_to_retry_exceptions |
| `python/ray/data/_internal/execution/streaming_executor_state.py` | 606-636 | 错误块计数 (prepare_metadata) |
| `python/ray/data/_internal/execution/streaming_executor_state.py` | 695-725 | 错误块计数 (ray.get metadata) |
| `python/ray/data/_internal/execution/operators/task_pool_map_operator.py` | 108-141 | _try_schedule_task |
| `python/ray/data/_internal/execution/operators/map_operator.py` | 595-648 | _submit_data_task |
| `python/ray/data/_internal/execution/operators/map_operator.py` | 749-810 | _map_task |

### 9.3 异常定义 (Python)

| 文件 | 行号 | 功能 |
|------|------|------|
| `python/ray/exceptions.py` | 495-509 | RaySystemError |
| `python/ray/exceptions.py` | 631-668 | ObjectLostError |
| `python/ray/exceptions.py` | 783-862 | ObjectReconstructionFailedError |
| `python/ray/exceptions.py` | 981-1007 | RayChannelError 系列 |

---

## 10. 总结与最佳实践

### 10.1 核心理解

1. **Ray Core 重试和 Ray Data 错误计数是并行独立的**
   - Ray Core 在重试 task 的同时，Ray Data 可能已经尝试获取结果并计入 errored_blocks

2. **Object Reconstruction 和 Task Retry 是不同的恢复路径**
   - Task Retry：task 执行失败时触发，自动重新执行
   - Object Reconstruction：ray.get() 发现 object 丢失时触发，需要手动开启

3. **默认配置下，上游 task 不会因下游需要数据而被主动触发重试**
   - 需要开启 `enable_object_reconstruction=True` 才能主动重建

4. **可能出现死循环的场景**
   - 上游 block 永久丢失 + lineage 被 GC + 无限容错

### 10.2 最佳实践

1. **设置合理的 max_errored_blocks**
   ```python
   ctx = ray.data.DataContext.get_current()
   ctx.max_errored_blocks = 100  # 而不是默认的 -1
   ```

2. **对关键数据启用 Object Reconstruction**
   ```python
   ray.init(enable_object_reconstruction=True)
   ```

3. **使用 Checkpoint 保存中间结果**
   ```python
   ds = ray.data.read_parquet("...")
   ds = ds.map(fn1)
   ds.write_parquet("/checkpoint/stage1")  # 保存中间结果
   ds = ray.data.read_parquet("/checkpoint/stage1")
   ds = ds.map(fn2)
   ```

4. **监控 errored_blocks 指标**
   - Dashboard 上观察 Errored Blocks 数量
   - 如果持续增长，检查节点稳定性

5. **理解日志含义**
   ```
   Node failure. All objects pinned on that node will be lost
   if object reconstruction is not enabled.
   ```
   这是信息日志，说明节点失败，但**不一定**会导致数据丢失（可能有副本或下游已消费）

### 10.3 调试技巧

1. **查看 Task 重试日志**
   ```bash
   grep "Resubmitting task" driver.log
   ```

2. **查看 Object Recovery 日志**
   ```bash
   grep "Attempting to recover" driver.log
   grep "Cannot recover object" driver.log
   ```

3. **查看 errored_blocks 来源**
   ```bash
   grep "An exception was raised from a task of operator" driver.log
   ```

---

## 附录：术语表

| 术语 | 含义 |
|------|------|
| `errored_blocks` | Ray Data 中执行失败的数据块计数 |
| `max_retries` | Ray Core 任务最大重试次数，-1 表示无限 |
| `lineage` | 对象的创建血缘，即生成该对象的 task spec |
| `Object Reconstruction` | 基于 lineage 重建丢失对象的机制 |
| `Task Retry` | 任务执行失败后自动重试的机制 |
| `plasma` | Ray 的分布式对象存储 |
| `NodeTerminated` | 节点被终止的事件 |
| `RaySystemError` | Ray 系统级错误，会被自动重试 |
| `ObjectLostError` | 对象丢失错误 |

---

*文档生成时间: 2026-04-19*
*基于 Ray 2.52.1 代码分析*
