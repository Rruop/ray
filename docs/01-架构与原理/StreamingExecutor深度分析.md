# Ray Data Streaming Executor 深度解析

本文档详细分析 Ray Data 的 Streaming Executor 实现，包括任务处理、错误处理、通知机制等核心内容。

---

## 目录

1. [process_completed_tasks 错误处理分析](#1-process_completed_tasks-错误处理分析)
2. [Task Ready 判断机制](#2-task-ready-判断机制)
3. [底层通知机制详解](#3-底层通知机制详解)
4. [DataOpTask、MetadataOpTask 与 ObjectRefGenerator 的关系](#4-dataoptask-metadataoptask-与-objectrefgenerator-的关系)
5. [concurrency 参数与 Actor Pool](#5-concurrency-参数与-actor-pool)
6. [Actor 空闲超时释放机制](#6-actor-空闲超时释放机制)
7. [任务异常时的标记与通知流程](#7-任务异常时的标记与通知流程)
8. [Raylet/Node/Actor 异常处理机制](#8-rayletnodeactor-异常处理机制)
9. [节点故障时 Driver 更新 ObjectRef 状态机制](#9-节点故障时-driver-更新-objectref-状态机制)

---

## 1. process_completed_tasks 错误处理分析

### 1.1 函数概述

`process_completed_tasks` 位于 `python/ray/data/_internal/execution/streaming_executor_state.py`，负责处理所有已完成的任务，更新 operator 状态。

### 1.2 错误处理位置

代码中有两个主要的错误处理位置：

#### 位置 1: Phase 2 - prepare_metadata() 阶段 (行 583-636)

```python
try:
    prepared = task.prepare_metadata()
    ...
except Exception as e:
    errored_blocks_per_op[state] += 1
    num_errored_blocks += 1
    should_ignore = (
        max_errored_blocks < 0
        or max_errored_blocks >= num_errored_blocks
    )
    if should_ignore:
        logger.error(error_message, exc_info=e)  # 记录但继续
    else:
        raise e from None  # 终止执行
```

**触发场景**：
- Task 内部异常：当 streaming generator 只 yield 了 block_ref 但没有 yield meta_ref 就抛出 `StopIteration` 时
- 此时 `block_ref` 实际上是一个异常对象

**代码路径** (`physical_operator.py:293-308`):
```python
except StopIteration:
    # generator 应该每次 yield 2个值 (block 和 metadata)
    # 如果这里收到 StopIteration，说明任务内部出错了
    # 此时 block_ref 实际上是异常对象
    try:
        ray.get(self._pending_block_ref)  # 这会抛出实际异常
    except Exception as ex:
        raise ex from None
```

#### 位置 2: Phase 4 - complete_with_metadata() 阶段 (行 687-725)

```python
try:
    meta_with_schema = ray.get(meta_ref, timeout=0)
    bytes_read = task.complete_with_metadata(meta_with_schema)
except Exception as e:
    errored_blocks_per_op[state] += 1
    num_errored_blocks += 1
    # ... 同样的错误处理逻辑
```

**触发场景**：
- `ray.get()` 失败：对象所在的 worker/node 挂掉，对象丢失或重建失败
- `complete_with_metadata()` 内部异常

### 1.3 错误容忍机制

通过 `max_errored_blocks` 参数控制：
- `max_errored_blocks < 0`：无限容忍，忽略所有错误继续执行
- `max_errored_blocks >= 0`：容忍指定数量的错误 block，超过后 abort

### 1.4 关键结论

**"task ready" ≠ "无错误"**。`ray.wait()` 只是告诉你有结果了，但结果可能是异常对象。

| 阶段 | Ready 状态 | 可能的错误来源 |
|------|-----------|---------------|
| `prepare_metadata()` | Task 的 ObjectRefGenerator ready | Task UDF 执行失败、Worker 崩溃 |
| `complete_with_metadata()` | meta_ref 在 `ray.wait()` 后 ready | 对象丢失/重建失败、数据损坏 |

---

## 2. Task Ready 判断机制

### 2.1 两种 Waitable 类型

```python
# physical_operator.py:53
Waitable = Union[ray.ObjectRef, ObjectRefGenerator]
```

- **DataOpTask** 返回 `ObjectRefGenerator`
- **MetadataOpTask** 返回普通 `ray.ObjectRef`

### 2.2 ObjectRef（普通任务）的 Ready 机制

```
┌────────────────────────────────────────────────────────────┐
│                    Object Store                             │
├────────────────────────────────────────────────────────────┤
│  ObjectRef ──► [数据/异常] 写入完成 ──► ready              │
└────────────────────────────────────────────────────────────┘
```

**Ready 条件**：对象在 Object Store 中被创建（成功写入数据，或写入异常对象）

### 2.3 ObjectRefGenerator（Streaming Generator）的 Ready 机制

```
┌─────────────────────────────────────────────────────────────┐
│                  Streaming Generator                         │
├─────────────────────────────────────────────────────────────┤
│  Generator ──► yield block_1 ──► yield meta_1 ──► ...       │
│                     │                                        │
│                     ▼                                        │
│              有新输出产生 ──► ready                          │
└─────────────────────────────────────────────────────────────┘
```

**Ready 条件**：Generator 产生了**至少一个新的输出**（不是整个 generator 完成）

### 2.4 关键区别

| 特性 | ObjectRef | ObjectRefGenerator |
|------|-----------|-------------------|
| Ready 含义 | 整个任务完成 | 有新输出可读 |
| ray.wait 返回后 | 可以 ray.get 获取最终结果 | 需要循环 `_next_sync()` 读取多个输出 |
| 一次 wait 后 | 任务状态完全确定 | 可能还有更多输出，需要再次 wait |

### 2.5 Worker 崩溃或任务失败时

即使 worker 崩溃或执行失败，`ray.wait()` 仍然会返回 ready：

```python
# Worker 崩溃
ray.wait([obj_ref])  # 返回 ready（Ray 检测到 worker 死亡）
ray.get(obj_ref)     # 抛出 RayWorkerError 或 RayActorError

# 任务内部异常
@ray.remote
def my_task():
    raise ValueError("something wrong")

ref = my_task.remote()
ray.wait([ref])      # 返回 ready（任务已完成，但带异常）
ray.get(ref)         # 抛出 RayTaskError，包装了原始 ValueError
```

---

## 3. 底层通知机制详解

### 3.1 整体架构

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          Task 执行端 (Worker)                            │
├─────────────────────────────────────────────────────────────────────────┤
│  Task 执行完成 → 调用 Put() 将结果写入 MemoryStore/PlasmaStore          │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │
                               ▼ 对象写入
┌─────────────────────────────────────────────────────────────────────────┐
│                      CoreWorkerMemoryStore::Put()                        │
├─────────────────────────────────────────────────────────────────────────┤
│  1. 将对象写入 objects_ map                                              │
│  2. 查找是否有等待该对象的 GetRequest                                    │
│  3. 如果有 → 调用 GetRequest::Set() 唤醒等待者                          │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │
                               ▼ 唤醒
┌─────────────────────────────────────────────────────────────────────────┐
│                      GetRequest::Set()                                   │
├─────────────────────────────────────────────────────────────────────────┤
│  objects_.emplace(object_id, object);                                    │
│  if (objects_.size() == num_objects_ || 是异常对象) {                    │
│      is_ready_ = true;                                                   │
│      cv_.notify_all();  ← 条件变量唤醒                                   │
│  }                                                                       │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │
                               ▼ 唤醒
┌─────────────────────────────────────────────────────────────────────────┐
│                      ray.wait() 返回                                     │
└─────────────────────────────────────────────────────────────────────────┘
```

### 3.2 核心机制：条件变量 (Condition Variable)

```cpp
// memory_store.cc 中的 GetRequest 类
class GetRequest {
    std::condition_variable cv_;  // 条件变量
    std::mutex mutex_;
    bool is_ready_ = false;

    // 等待对象 ready
    bool Wait(int64_t timeout_ms) {
        std::unique_lock<std::mutex> lock(mutex_);
        // 阻塞等待，直到 is_ready_ = true 或超时
        return cv_.wait_for(lock, timeout_ms, [this] { return is_ready_; });
    }

    // 当对象被 Put 时调用
    void Set(const ObjectID &object_id, std::shared_ptr<RayObject> object) {
        std::scoped_lock<std::mutex> lock(mutex_);
        objects_.emplace(object_id, object);
        if (objects_.size() == num_objects_ || 是异常) {
            is_ready_ = true;
            cv_.notify_all();  // 唤醒所有等待者
        }
    }
};
```

### 3.3 对于 ObjectRef（普通任务）

```
Driver/调用端                              Worker/执行端
     │                                          │
     │  ray.wait([obj_ref])                     │
     │     │                                    │
     │     ▼                                    │
     │  CoreWorker::Wait()                      │
     │     │                                    │
     │     ▼                                    │
     │  MemoryStore::Wait()                     │
     │     │                                    │
     │     ▼                                    │
     │  GetRequest::Wait()  ◄─────────────────  │ 任务完成
     │     │ (cv.wait_for)                      │    │
     │     │                                    │    ▼
     │     │                          MemoryStore::Put(result)
     │     │                                    │
     │     │◄───────────────────────────────────│ GetRequest::Set()
     │     │  cv.notify_all()                   │    cv.notify_all()
     │     ▼                                    │
     │  返回 ready                              │
```

### 3.4 对于 ObjectRefGenerator（Streaming Generator）

```
Driver/调用端                              Worker/执行端
     │                                          │
     │  ray.wait([generator])                   │  streaming task
     │     │                                    │     │
     │     ▼                                    │     ▼
     │  等待 generator 的                       │  yield block_1
     │  下一个输出 ready                        │     │
     │     │                                    │     ▼
     │     │◄───────────────────────────────────│  Put(block_1_ref)
     │     │                                    │     │
     │  返回 ready                              │     ▼
     │     │                                    │  yield meta_1
     │  _next_sync() 获取 block_1               │     │
     │  _next_sync() 获取 meta_1                │     ▼
     │     │                                    │  yield block_2
     │     │                                    │  ...
```

### 3.5 总结

| 层次 | 机制 | 说明 |
|------|------|------|
| **C++ 底层** | `std::condition_variable` | 线程同步原语，`wait_for` + `notify_all` |
| **MemoryStore** | `GetRequest` 对象 | 维护等待对象集合，追踪 ready 状态 |
| **通知触发点** | `Put()` 调用 | 任务完成/失败时，将结果 Put 到 store |
| **Ready 含义** | 对象状态确定 | 数据 ready 或 异常 ready，都算 ready |

---

## 4. DataOpTask、MetadataOpTask 与 ObjectRefGenerator 的关系

### 4.1 类层次结构

```
OpTask (抽象基类)
   │
   ├── DataOpTask
   │       └── 包装 ObjectRefGenerator（streaming generator）
   │       └── 处理实际的数据 Block
   │
   └── MetadataOpTask
           └── 包装普通 ObjectRef
           └── 处理元数据/状态信息（不涉及数据 Block）
```

### 4.2 核心区别

| 特性 | DataOpTask | MetadataOpTask |
|------|------------|----------------|
| **包装的对象** | `ObjectRefGenerator` | 普通 `ObjectRef` |
| **输出特点** | 多个输出（streaming yield） | 单个输出（一次性返回） |
| **处理内容** | 实际数据 Block + Metadata | 只有元数据/状态信息 |
| **完成通知** | 需要多次 `_next_sync()` 读取 | `ray.wait()` ready 后直接完成 |
| **用途** | map/filter 等数据处理任务 | Actor 就绪检查、shuffle 元数据等 |

### 4.3 代码定义对比

```python
# physical_operator.py

class DataOpTask(OpTask):
    """处理 Block 数据的任务"""
    def __init__(
        self,
        task_index: int,
        streaming_gen: ObjectRefGenerator,  # ← Streaming Generator
        output_ready_callback: Callable[[RefBundle], None],  # 每个 block ready 时调用
        task_done_callback: Callable[[Optional[Exception]], None],
        ...
    ):
        self._streaming_gen = streaming_gen
        # 可能产生多个 block

    def get_waitable(self) -> ObjectRefGenerator:
        return self._streaming_gen


class MetadataOpTask(OpTask):
    """只处理元数据的任务"""
    def __init__(
        self,
        task_index: int,
        object_ref: ray.ObjectRef,  # ← 普通 ObjectRef
        task_done_callback: Callable[[], None],  # 完成时调用（无输出）
        ...
    ):
        self._object_ref = object_ref
        # 只有一个结果，不涉及数据

    def get_waitable(self) -> ray.ObjectRef:
        return self._object_ref

    def on_task_finished(self):
        """任务完成回调"""
        self._task_done_callback()
```

### 4.4 MetadataOpTask 的使用场景

#### 场景 1: Actor Pool 的 Actor 就绪检查

```python
# actor_pool_map_operator.py

def _start_actor(self, labels, logical_actor_id):
    # 启动 Actor
    actor = self._actor_cls.options(...).remote(...)

    # 调用 actor.get_location.remote() 获取位置信息
    # 这是一个普通的 ObjectRef，不是 streaming generator
    res_ref = actor.get_location.remote()

    def _task_done_callback(res_ref):
        # Actor ready 后，将其从 pending 移到 running pool
        self._actor_pool.pending_to_running(res_ref)

    # 使用 MetadataOpTask 追踪 Actor 就绪状态
    self._submit_metadata_task(
        res_ref,  # ← 普通 ObjectRef
        lambda: _task_done_callback(res_ref),
    )
```

**工作流程**：
```
Actor 启动 (pending)
    │
    ▼
get_location.remote() 返回 ObjectRef
    │
    ▼
创建 MetadataOpTask 追踪该 ref
    │
    ▼
ray.wait() 检测到 ref ready
    │
    ▼
on_task_finished() → 将 Actor 移入 running pool
    │
    ▼
Actor 可以开始处理数据任务
```

#### 场景 2: Hash Shuffle 的分区元数据

```python
# hash_shuffle.py

def _on_input_block_available(self, input_index, block_ref, block_metadata):
    # Shuffle 任务：将 block 分区
    input_block_partition_shards_metadata_tuple_ref = hash_partition.remote(...)

    def _on_partitioning_done(task_idx):
        # 分区完成后处理元数据
        out_bundle = ...
        self._output_shuffled_bundle(task_idx, out_bundle)

    # 使用 MetadataOpTask 追踪分区任务
    task = MetadataOpTask(
        task_index=cur_shuffle_task_idx,
        object_ref=input_block_partition_shards_metadata_tuple_ref,
        task_done_callback=functools.partial(_on_partitioning_done, cur_shuffle_task_idx),
    )
```

### 4.5 在 process_completed_tasks 中的处理差异

```python
# streaming_executor_state.py

for state, ready_tasks in ready_tasks_by_op.items():
    for task in ready_tasks:
        if isinstance(task, DataOpTask):
            # DataOpTask: 需要复杂的 prepare_metadata + batch wait + complete
            try:
                prepared = task.prepare_metadata()  # 获取 block_ref 和 meta_ref
                if prepared:
                    pending_meta_tasks.append((state, task, meta_ref))
            except Exception as e:
                # 处理错误
                ...
        else:
            # MetadataOpTask: 简单地标记完成
            assert isinstance(task, MetadataOpTask)
            non_data_tasks.append((state, task))

# MetadataOpTask 的处理非常简单
for state, task in non_data_tasks:
    task.on_task_finished()  # 直接调用回调，不涉及数据处理
```

### 4.6 总结图示

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           MapOperator                                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  _data_tasks: Dict[int, DataOpTask]                                         │
│      │                                                                       │
│      └── DataOpTask ──► ObjectRefGenerator                                  │
│              │                                                               │
│              ├── yield block_1, meta_1                                      │
│              ├── yield block_2, meta_2                                      │
│              └── ...                                                        │
│                                                                              │
│  _metadata_tasks: Dict[int, MetadataOpTask]  (仅 ActorPoolMapOperator 有)   │
│      │                                                                       │
│      └── MetadataOpTask ──► ObjectRef (actor.get_location.remote())         │
│              │                                                               │
│              └── 返回 actor 位置，Actor 就绪                                 │
│                                                                              │
│  get_active_tasks():                                                        │
│      return list(_metadata_tasks.values()) + list(_data_tasks.values())     │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 5. concurrency 参数与 Actor Pool

### 5.1 关键判断逻辑

决定使用 **TaskPool** 还是 **ActorPool** 的核心逻辑在 `_internal/util.py:586-673`：

```python
# 判断 fn 是否是 callable class
if isinstance(fn, CallableClass):
    is_callable_class = True
else:
    is_callable_class = False

# 根据 concurrency 参数决定策略
if concurrency is not None:
    if isinstance(concurrency, int):
        if is_callable_class:
            return ActorPoolStrategy(size=concurrency)  # ← Actor Pool
        else:
            return TaskPoolStrategy(size=concurrency)   # ← Task Pool
else:
    if is_callable_class:
        return ActorPoolStrategy(min_size=1, max_size=None)  # ← Actor Pool (自动伸缩)
    else:
        return TaskPoolStrategy()  # ← Task Pool
```

### 5.2 示例代码分析

```python
# 示例 Pipeline
ds = ds.map(
    VideoPreprocessMapper,  # ← 这是一个类 (callable class)
    fn_constructor_kwargs={...},
    concurrency=preprocess_concurrency,  # ← int 类型
    num_cpus=1,
)
```

**结论**：因为 `VideoPreprocessMapper` 是一个类，所以会使用 **ActorPoolStrategy(size=concurrency)**，即固定大小的 Actor Pool。

### 5.3 两种模式对比

| 代码 | fn 类型 | concurrency | 实际使用 |
|------|--------|-------------|---------|
| `map(VideoClipProcessMapper(...))` | 实例（callable） | cpu_concurrency | **ActorPool** |
| `map(VideoPreprocessMapper, ...)` | 类 | preprocess_concurrency | **ActorPool** |
| `map(VideoInferenceMapper, ...)` | 类 | gpu_concurrency | **ActorPool** |
| `filter(is_multi_shot_detect_enabled, ...)` | 函数 | cpu_concurrency | **TaskPool** |

### 5.4 ComputeStrategy 类型

```python
# compute.py

class TaskPoolStrategy(ComputeStrategy):
    """使用 Ray Tasks 执行，无状态"""
    def __init__(self, size: Optional[int] = None):
        self.size = size  # 最大并发 task 数

class ActorPoolStrategy(ComputeStrategy):
    """使用 Actor Pool 执行，有状态"""
    def __init__(
        self,
        size: Optional[int] = None,  # 固定大小
        min_size: Optional[int] = None,  # 最小大小（自动伸缩）
        max_size: Optional[int] = None,  # 最大大小（自动伸缩）
        initial_size: Optional[int] = None,  # 初始大小
        max_tasks_in_flight_per_actor: Optional[int] = None,  # 每个 actor 最大并发任务
    ):
        ...
```

---

## 6. Actor 空闲超时释放机制

### 6.1 简短答案

**Ray Data 没有基于时间的空闲超时释放机制**。Actor Pool 缩容是基于 **利用率 (utilization)** 而不是 **空闲时间**。

### 6.2 Actor Pool 缩容触发条件

```python
# default_actor_autoscaler.py

# 缩容阈值（默认 0.5，即 50%）
self._actor_pool_scaling_down_threshold = config.actor_pool_util_downscaling_threshold

# 利用率计算
util = num_tasks_in_flight / (max_concurrency * num_running_actors)

# 缩容判断
if util <= self._actor_pool_scaling_down_threshold:  # 利用率 <= 50%
    # 触发缩容
    return ActorPoolScalingRequest.downscale(...)
```

### 6.3 缩容触发的场景

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     Actor Pool 缩容条件                                  │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  1. 所有输入已消费完毕 (inputs_complete && enqueued_blocks == 0)          │
│     → 强制释放所有 Actor                                                 │
│                                                                          │
│  2. 池大小超过 max_size                                                  │
│     → 缩容到 max_size                                                    │
│                                                                          │
│  3. 利用率 <= 50% (默认阈值)                                             │
│     → 缩容，但不低于 min_size                                            │
│                                                                          │
│  4. Operator shutdown (Pipeline 结束)                                    │
│     → 释放所有 Actor                                                     │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### 6.4 固定大小 Actor Pool 的情况

当使用 `concurrency=10` 时：

```python
ActorPoolStrategy(size=10)  # 等价于 min_size=10, max_size=10
```

**这意味着**：
- Actor Pool 大小固定为 10
- 即使利用率很低，也**不会缩容**（因为 current_size == min_size）
- Actor 会一直存活直到 Pipeline 结束

### 6.5 缩容时 Actor 的释放流程

```python
# actor_pool_map_operator.py

def _release_running_actor(self, actor):
    # 默认：通过引用计数 GC 释放，而不是 ray.kill
    # 这样可以保留 lineage 用于对象重建
    del self._running_actors[actor]

def _release_running_actors(self, force: bool):
    # 先调用 on_exit 回调
    on_exit_refs = [actor.on_exit.remote() for actor in running]

    # 等待优雅关闭（默认超时 30s）
    ray.wait(on_exit_refs, timeout=self._ACTOR_POOL_GRACEFUL_SHUTDOWN_TIMEOUT_S)

    # 如果是强制释放，才调用 ray.kill
    if force:
        for actor in running:
            ray.kill(actor)
```

### 6.6 如果想要空闲超时释放

Ray Data 原生**不支持**空闲超时释放。如果需要，可以：

1. **使用自动伸缩 Actor Pool**：
   ```python
   ds.map(
       MyMapper,
       compute=ActorPoolStrategy(min_size=1, max_size=10),  # 允许缩容到 1
   )
   ```

2. **调整缩容阈值**（更激进的缩容）：
   ```python
   from ray.data import DataContext
   ctx = DataContext.get_current()
   ctx.actor_pool_util_downscaling_threshold = 0.3  # 30% 就开始缩容
   ```

3. **自定义 Autoscaler**（高级）：继承 `DefaultActorPoolAutoscaler` 添加时间维度判断

### 6.7 总结

| 问题 | 答案 |
|------|------|
| `concurrency=10` + 类 会用 Actor Pool 吗？ | **是**，使用固定大小 10 的 ActorPool |
| Actor 空闲会超时释放吗？ | **否**，基于利用率缩容，不是时间 |
| 固定大小 Pool 会缩容吗？ | **否**，min_size == max_size == size |
| Actor 什么时候释放？ | Pipeline 结束 或 手动 shutdown |

---

## 7. 任务异常时的标记与通知流程

### 7.1 完整架构概览

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                            Worker 端 (执行任务)                                  │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  @ray.remote(num_returns="streaming")                                           │
│  def streaming_task():                                                          │
│      yield block_1     ──► ReportGeneratorItemReturns (RPC) ───┐                │
│      yield metadata_1  ──► ReportGeneratorItemReturns (RPC) ───┤                │
│      yield block_2     ──► ...                                 │                │
│      ...                                                       │                │
│      raise Exception() ──► Task fails                          │                │
│                                                                │                │
└────────────────────────────────────────────────────────────────┼────────────────┘
                                                                 │
                                                                 ▼ gRPC
┌─────────────────────────────────────────────────────────────────────────────────┐
│                            Caller 端 (Driver/调度器)                             │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  HandleReportGeneratorItemReturns()                                             │
│      │                                                                          │
│      ├── InsertToStream(object_id, index)  ◄── 将 ref 加入 stream               │
│      │                                                                          │
│      └── in_memory_store_.Put(object, object_id)  ◄── 写入 MemoryStore          │
│              │                                                                   │
│              └── GetRequest::Set() ──► cv_.notify_all()  ◄── 唤醒等待者          │
│                                                                                  │
│  FailPendingTask() / MarkTaskReturnObjectsFailed()  ◄── 任务失败时               │
│      │                                                                          │
│      ├── MarkEndOfStream()  ◄── 标记流结束                                       │
│      │                                                                          │
│      └── in_memory_store_.Put(error_object)  ◄── 写入异常对象                    │
│              │                                                                   │
│              └── cv_.notify_all()  ◄── 唤醒等待者                                │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 7.2 正常写入时的通知流程

#### 步骤 1: Worker 端执行 yield

```python
# Python 端的 streaming task
@ray.remote(num_returns="streaming")
def my_task():
    for i in range(10):
        yield process(i)  # 每次 yield 触发一次 ReportGeneratorItemReturns
```

#### 步骤 2: Worker 调用 ReportGeneratorItemReturns (C++)

```cpp
// core_worker.cc:3199
Status CoreWorker::ReportGeneratorItemReturns(
    const std::pair<ObjectID, std::shared_ptr<RayObject>> &dynamic_return_object,
    const ObjectID &generator_id,
    const rpc::Address &caller_address,
    int64_t item_index,
    ...) {

  // 构建 RPC 请求
  rpc::ReportGeneratorItemReturnsRequest request;
  request.set_item_index(item_index);
  request.set_generator_id(generator_id.Binary());

  // 序列化返回对象（block 或 metadata）
  if (!dynamic_return_object.first.IsNil()) {
    SerializeReturnObject(dynamic_return_object.first,
                          dynamic_return_object.second,
                          request.mutable_returned_object());
  }

  // 通过 gRPC 发送到 Caller
  client->ReportGeneratorItemReturns(std::move(request), callback);
}
```

#### 步骤 3: Caller 端处理 ReportGeneratorItemReturns

```cpp
// task_manager.cc:780
bool TaskManager::HandleReportGeneratorItemReturns(
    const rpc::ReportGeneratorItemReturnsRequest &request,
    const ExecutionSignalCallback &execution_signal_callback) {

  const auto &generator_id = ObjectID::FromBinary(request.generator_id());
  int64_t item_index = request.item_index();

  // 1. 将 ObjectID 插入到 stream 中
  auto index_not_used_yet = stream_it->second.InsertToStream(object_id, item_index);

  // 2. 更新引用计数
  if (index_not_used_yet) {
    reference_counter_.OwnDynamicStreamingTaskReturnRef(object_id, generator_id);
  }

  // 3. 标记对象不再是 pending creation 状态
  reference_counter_.UpdateObjectPendingCreation(object_id, false);

  // 4. 将对象写入 MemoryStore（关键！触发通知）
  StatusOr<bool> put_res = HandleTaskReturn(object_id, ...);
}
```

#### 步骤 4: HandleTaskReturn 写入 MemoryStore

```cpp
// task_manager.cc (HandleTaskReturn 内部)
StatusOr<bool> TaskManager::HandleTaskReturn(const ObjectID &object_id, ...) {
  // ... 处理对象数据 ...

  // 写入 in-memory store
  in_memory_store_.Put(object, object_id, has_reference);
}
```

#### 步骤 5: MemoryStore::Put 触发通知

```cpp
// memory_store.cc:172
void CoreWorkerMemoryStore::Put(const RayObject &object,
                                const ObjectID &object_id,
                                const bool has_reference) {
  std::vector<std::function<void(std::shared_ptr<RayObject>)>> async_callbacks;

  {
    absl::MutexLock lock(&mu_);

    // 1. 查找是否有等待该对象的 GetRequest
    auto object_request_iter = object_get_requests_.find(object_id);
    if (object_request_iter != object_get_requests_.end()) {
      auto &get_requests = object_request_iter->second;

      // 2. 对每个等待的 GetRequest 调用 Set()
      for (auto &get_request : get_requests) {
        get_request->Set(object_id, object_entry);  // ← 这里触发通知！
      }
    }

    // 3. 将对象存入 objects_ map
    EmplaceObjectAndUpdateStats(object_id, object_entry);
  }
}
```

#### 步骤 6: GetRequest::Set 唤醒等待者

```cpp
// memory_store.cc:101
void GetRequest::Set(const ObjectID &object_id, std::shared_ptr<RayObject> object) {
  std::scoped_lock<std::mutex> lock(mutex_);

  if (is_ready_) {
    return;  // 已经有足够的对象了
  }

  object->SetAccessed();
  objects_.emplace(object_id, object);

  // 检查是否已经收集到足够的对象，或者对象是异常
  if (objects_.size() == num_objects_ ||
      (abort_if_any_object_is_exception_ && object->IsException() &&
       !object->IsInPlasmaError())) {
    is_ready_ = true;
    cv_.notify_all();  // ← 唤醒所有等待的线程！
  }
}
```

### 7.3 任务异常时的通知流程

#### 步骤 1: 检测到任务失败

任务失败可能由多种原因触发：Worker 崩溃、任务抛出异常、资源不足、超时等

```cpp
// 例如：Worker 崩溃时，Raylet 会通知 TaskManager
void TaskManager::FailOrRetryPendingTask(const TaskID &task_id,
                                         rpc::ErrorType error_type,
                                         const Status *status,
                                         const rpc::RayErrorInfo *ray_error_info,
                                         bool mark_task_object_failed,
                                         bool fail_immediately) {

  // 1. 尝试重试
  bool will_retry = RetryTaskIfPossible(task_id, ray_error_info);

  // 2. 如果不重试，标记任务失败
  if (!will_retry && mark_task_object_failed) {
    FailPendingTask(task_id, error_type, status, ray_error_info);
  }
}
```

#### 步骤 2: FailPendingTask 处理

```cpp
// task_manager.cc:1261
void TaskManager::FailPendingTask(const TaskID &task_id,
                                  rpc::ErrorType error_type,
                                  const Status *status,
                                  const rpc::RayErrorInfo *ray_error_info) {

  // 1. 更新任务状态
  SetTaskStatus(it->second, rpc::TaskStatus::FAILED, ...);

  // 2. 清理任务引用
  RemoveFinishedTaskReferences(spec, ...);

  // 3. 标记所有返回对象为失败（关键！）
  MarkTaskReturnObjectsFailed(spec, error_type, ray_error_info, store_in_plasma_ids);
}
```

#### 步骤 3: MarkTaskReturnObjectsFailed - 写入错误对象

```cpp
// task_manager.cc:1559
void TaskManager::MarkTaskReturnObjectsFailed(
    const TaskSpecification &spec,
    rpc::ErrorType error_type,
    const rpc::RayErrorInfo *ray_error_info,
    const absl::flat_hash_set<ObjectID> &store_in_plasma_ids) {

  const TaskID task_id = spec.TaskId();

  // 创建错误对象
  RayObject error(error_type, ray_error_info);

  // 对普通返回值，写入错误对象
  int64_t num_returns = spec.NumReturns();
  for (int i = 0; i < num_returns; i++) {
    const auto object_id = ObjectID::FromIndex(task_id, i + 1);
    // 写入 MemoryStore（或 Plasma）
    in_memory_store_.Put(error, object_id, ...);  // ← 触发通知！
  }

  // 对于 Streaming Generator，特殊处理
  if (spec.IsStreamingGenerator()) {
    const auto generator_id = spec.ReturnId(0);

    // 1. 标记流结束
    MarkEndOfStream(generator_id, /*item_index*/ -1);

    // 2. 对所有已知的 generator 返回值写入错误
    auto num_streaming_generator_returns = spec.NumStreamingGeneratorReturns();
    for (size_t i = 0; i < num_streaming_generator_returns; i++) {
      const auto generator_return_id = spec.StreamingGeneratorReturnId(i);
      in_memory_store_.Put(error, generator_return_id, ...);  // ← 触发通知！
    }
  }
}
```

#### 步骤 4: MarkEndOfStream - 标记流结束

```cpp
// task_manager.cc:753
void TaskManager::MarkEndOfStream(const ObjectID &generator_id,
                                  int64_t end_of_stream_index) {
  ObjectID last_object_id;

  // 1. 标记流的结束索引
  stream_it->second.MarkEndOfStream(end_of_stream_index, &last_object_id);

  if (!last_object_id.IsNil()) {
    // 2. 在结束位置写入一个特殊的 END_OF_STREAMING_GENERATOR 错误对象
    reference_counter_.OwnDynamicStreamingTaskReturnRef(last_object_id, generator_id);

    RayObject error(rpc::ErrorType::END_OF_STREAMING_GENERATOR);
    in_memory_store_.Put(error, last_object_id, ...);  // ← 这也会触发通知！
  }
}
```

### 7.4 Python 端的读取流程

当 `ray.wait()` 返回 ready 后，Python 端如何读取：

```python
# object_ref_generator.py:188
def _next_sync(self, timeout_s: Optional[int | float] = None) -> "ray.ObjectRef":
    core_worker = self.worker.core_worker

    # 1. Peek 下一个 ref 是否 ready
    expected_ref, is_ready = core_worker.peek_object_ref_stream(self._generator_ref)

    if not is_ready:
        # 2. 等待 ref ready
        _, unready = ray.wait([expected_ref], timeout=timeout_s, fetch_local=False)
        if len(unready) > 0:
            return ray.ObjectRef.nil()  # 超时

    try:
        # 3. 从 stream 中读取 ref
        ref = core_worker.try_read_next_object_ref_stream(self._generator_ref)
        assert not ref.is_nil()
    except ObjectRefStreamEndOfStreamError:
        # 4. 流结束了
        if self._generator_task_raised:
            raise StopIteration from None

        try:
            # 5. 检查 generator_ref 是否包含异常
            ray.get(self._generator_ref)
        except Exception:
            # 6. 有异常！返回 generator_ref（它包含异常）
            self._generator_task_raised = True
            return self._generator_ref  # ← 返回包含异常的 ref
        else:
            # 7. 正常结束
            raise StopIteration from None

    return ref
```

### 7.5 完整时序图

```
                Worker                          Caller (Driver)
                  │                                   │
    正常 yield    │                                   │
    ─────────────────────────────────────────────────────────────────
                  │                                   │
  yield block_1   │ ─── ReportGeneratorItemReturns ──►│
                  │                                   │ HandleReportGeneratorItemReturns
                  │                                   │   └─► InsertToStream(block_1_id)
                  │                                   │   └─► in_memory_store_.Put(block_1)
                  │                                   │         └─► GetRequest::Set()
                  │                                   │               └─► cv_.notify_all() ◄── 唤醒！
                  │                                   │
  yield meta_1    │ ─── ReportGeneratorItemReturns ──►│
                  │                                   │   └─► 同上流程
                  │                                   │
    任务失败      │                                   │
    ─────────────────────────────────────────────────────────────────
                  │                                   │
  Worker 崩溃     │ ────── Raylet 通知 ──────────────►│
       或         │                                   │ FailOrRetryPendingTask()
  raise Exception │                                   │   └─► FailPendingTask()
                  │                                   │         └─► MarkTaskReturnObjectsFailed()
                  │                                   │               │
                  │                                   │               ├─► MarkEndOfStream()
                  │                                   │               │     └─► Put(END_OF_STREAM)
                  │                                   │               │           └─► cv_.notify_all() ◄── 唤醒！
                  │                                   │               │
                  │                                   │               └─► Put(error_object) for each return
                  │                                   │                     └─► cv_.notify_all() ◄── 唤醒！
                  │                                   │
```

### 7.6 关键点总结

| 场景 | 触发点 | 写入内容 | 通知机制 |
|------|--------|----------|----------|
| **正常 yield** | `ReportGeneratorItemReturns` | 实际数据对象 | `Put()` → `cv_.notify_all()` |
| **Worker 崩溃** | `FailPendingTask` | `RayObject(error_type)` | `Put()` → `cv_.notify_all()` |
| **任务异常** | `FailPendingTask` | `RayObject(TASK_EXECUTION_EXCEPTION)` | `Put()` → `cv_.notify_all()` |
| **流结束** | `MarkEndOfStream` | `RayObject(END_OF_STREAMING_GENERATOR)` | `Put()` → `cv_.notify_all()` |

**核心机制**：无论正常还是异常，最终都是通过 `CoreWorkerMemoryStore::Put()` 写入对象，然后触发 `condition_variable::notify_all()` 唤醒所有等待该对象的线程。

---

## 8. Raylet/Node/Actor 异常处理机制

本章详细分析当 Raylet、Node 或 Actor 发生异常时，Ray 如何检测、传播错误并通知上游调用者。

### 8.1 异常类型概览

Ray 定义了多种错误类型来区分不同的故障场景：

| 错误类型 | 触发场景 | 可重试 |
|----------|----------|--------|
| `WORKER_DIED` | Worker 进程崩溃 | 是 |
| `NODE_DIED` | 节点失联/Raylet 崩溃 | 是（取决于配置） |
| `ACTOR_DIED` | Actor 进程死亡 | 取决于 Actor 配置 |
| `ACTOR_UNAVAILABLE` | Actor 暂时不可用 | 是 |
| `ACTOR_CREATION_FAILED` | Actor 创建失败 | 否 |
| `TASK_CANCELLED` | 任务被取消 | 否 |
| `RUNTIME_ENV_SETUP_FAILED` | 运行环境配置失败 | 否 |
| `TASK_PLACEMENT_GROUP_REMOVED` | Placement Group 被移除 | 否 |

### 8.2 整体故障检测与处理架构

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              GCS (Global Control Service)                        │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐             │
│  │ GcsNodeManager  │    │ GcsActorManager │    │ GcsJobManager   │             │
│  │                 │    │                 │    │                 │             │
│  │ - 心跳检测      │    │ - Actor 状态    │    │ - Job 状态      │             │
│  │ - 节点注册/注销 │    │ - Actor 重启    │    │ - Driver 监控   │             │
│  └────────┬────────┘    └────────┬────────┘    └────────┬────────┘             │
│           │                      │                      │                       │
│           └──────────────────────┼──────────────────────┘                       │
│                                  │                                              │
│                                  ▼                                              │
│                    PublishNodeInfo / PublishActorInfo                           │
│                                  │                                              │
└──────────────────────────────────┼──────────────────────────────────────────────┘
                                   │ Pub/Sub 通知
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              Raylet (每个节点)                                   │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐             │
│  │ Worker 监控     │    │ 任务调度        │    │ 对象管理        │             │
│  │                 │    │                 │    │                 │             │
│  │ - 进程存活检测  │    │ - 任务分发      │    │ - 对象存储      │             │
│  │ - 心跳超时      │    │ - 失败通知      │    │ - 对象重建      │             │
│  └────────┬────────┘    └────────┬────────┘    └────────┬────────┘             │
│           │                      │                      │                       │
└───────────┼──────────────────────┼──────────────────────┼───────────────────────┘
            │                      │                      │
            ▼                      ▼                      ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              CoreWorker (Driver/Worker)                          │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐             │
│  │ TaskManager     │    │ ActorTaskSubmitter│  │ NormalTaskSubmitter│           │
│  │                 │    │                 │    │                 │             │
│  │ - FailPendingTask│   │ - DisconnectActor│   │ - HandleWorkerFailure│        │
│  │ - RetryTask     │    │ - FailInflightTasks│ │ - RetryTask    │             │
│  └─────────────────┘    └─────────────────┘    └─────────────────┘     │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 8.3 Node（节点）故障处理

#### 8.3.1 节点故障检测

```cpp
// gcs_node_manager.cc:680
void GcsNodeManager::OnNodeFailure(
    const NodeID &node_id, const std::function<void()> &node_table_updated_callback) {
  absl::MutexLock lock(&mutex_);
  InternalOnNodeFailure(node_id, node_table_updated_callback);
}

void GcsNodeManager::InternalOnNodeFailure(
    const NodeID &node_id, const std::function<void()> &node_table_updated_callback) {
  auto maybe_node = GetAliveNodeFromCache(node_id);
  if (maybe_node.has_value()) {
    // 推断死亡原因
    rpc::NodeDeathInfo death_info = InferDeathInfo(node_id);

    // 从活跃节点列表中移除
    auto node = RemoveNodeFromCache(
        node_id, death_info, rpc::GcsNodeInfo::DEAD, current_sys_time_ms());

    // 添加到死亡节点缓存
    AddDeadNodeToCache(node);

    // 更新节点表并发布通知
    gcs_table_storage_->NodeTable().Put(node_id, *node, on_done);
  }
}
```

#### 8.3.2 节点死亡通知传播

```cpp
// gcs_node_manager.cc:673
// 通知所有监听者
for (auto &listener : node_removed_listeners_) {
  listener.Post("NodeManager.RemoveNodeCallback", removed_node);
}

// 死亡原因分类
enum NodeDeathInfo_Reason {
  UNSPECIFIED = 0,
  EXPECTED_TERMINATION = 1,      // 正常关闭
  UNEXPECTED_TERMINATION = 2,    // 意外终止（心跳丢失）
  AUTOSCALER_DRAIN_PREEMPTED = 3 // 自动伸缩抢占
};
```

#### 8.3.3 节点故障对任务的影响

```cpp
// normal_task_submitter.cc:637
// 当无法获取 worker 失败原因时，认为是节点死亡
task_error_type = rpc::ErrorType::NODE_DIED;

std::string error_message = absl::StrFormat(
    "Task failed because the node it was running on is dead or unavailable. "
    "Node IP: %s, node ID: %s. This can happen when a "
    "(1) raylet crashes unexpectedly (OOM, etc.) "
    "(2) raylet has lagging heartbeats due to slow network or busy workload.",
    addr.ip_address(),
    node_id.Hex());

error_info->set_error_type(rpc::ErrorType::NODE_DIED);

// 尝试重试或失败
return task_manager_.FailOrRetryPendingTask(task_id,
                                            task_error_type,
                                            &task_execution_status,
                                            error_info.get(),
                                            /*mark_task_object_failed*/ true,
                                            fail_immediately);
```

### 8.4 Worker（工作进程）故障处理

#### 8.4.1 Worker 死亡检测

Worker 死亡通常由 Raylet 检测（进程监控），然后通知 GCS 和相关的 CoreWorker。

```cpp
// normal_task_submitter.cc:341
// Worker 死亡的默认错误类型
rpc::ErrorType error_type = rpc::ErrorType::WORKER_DIED;

// 如果能获取到具体的失败原因
if (get_worker_failure_cause_reply.has_failure_cause()) {
  task_error_type = get_worker_failure_cause_reply.failure_cause().error_type();
  error_info = std::make_unique<rpc::RayErrorInfo>(
      get_worker_failure_cause_reply.failure_cause());
}
```

#### 8.4.2 Worker 故障重试逻辑

```cpp
// task_manager.cc:1165
// 检查是否是节点抢占导致的死亡
if (error_info.error_type() == rpc::ErrorType::NODE_DIED) {
  const auto node_info = gcs_client_->Nodes().GetNodeAddressAndLiveness(
      task_entry.GetNodeId(), /*filter_dead_nodes=*/false);
  is_preempted = node_info && node_info->has_death_info() &&
                 node_info->death_info().reason() ==
                     rpc::NodeDeathInfo::AUTOSCALER_DRAIN_PREEMPTED;
}

// 节点抢占不计入重试次数
if (num_retries_left > 0 || (is_preempted && task_entry.spec_.IsRetriable())) {
  will_retry = true;
  if (is_preempted) {
    RAY_LOG(INFO) << "Task " << task_id << " failed due to node preemption on node "
                  << task_entry.GetNodeId() << ", not counting against retries";
  } else {
    num_retries_left--;
  }
} else if (num_retries_left == -1) {
  // 无限重试
  will_retry = true;
}
```

### 8.5 Actor 故障处理

#### 8.5.1 Actor 状态机

```
                 ┌───────────────┐
                 │   PENDING     │  Actor 创建中
                 └───────┬───────┘
                         │ 创建成功
                         ▼
                 ┌───────────────┐
          ┌──────│    ALIVE      │◄─────┐
          │      └───────┬───────┘      │
          │              │ Actor 失败   │ 重启成功
          │              ▼              │
          │      ┌───────────────┐      │
          │      │  RESTARTING   │──────┘
          │      └───────┬───────┘
          │              │ 重启失败/达到最大重试次数
          │              ▼
          │      ┌───────────────┐
          └──────►    DEAD       │
                 └───────────────┘
```

#### 8.5.2 Actor 死亡处理 - DisconnectActor

```cpp
// actor_task_submitter.cc:401
// Actor 失败，断开客户端连接
DisconnectRpcClient(queue->second);
inflight_task_callbacks = std::move(queue->second.inflight_task_callbacks_);
queue->second.inflight_task_callbacks_.clear();

if (dead) {
  queue->second.state_ = rpc::ActorTableData::DEAD;
  queue->second.death_cause_ = death_cause;

  if (queue->second.is_restartable_ && queue->second.owned_) {
    // 如果 Actor 可重启且有待处理的任务，尝试重启
    if (!queue->second.actor_submit_queue_->Empty()) {
      RestartActorForLineageReconstruction(actor_id);
    }
  } else {
    // Actor 已死亡，失败所有待处理的任务
    RAY_LOG(INFO).WithField(actor_id)
        << "Failing pending tasks for actor because the actor is already dead.";
    task_ids_to_fail = queue->second.actor_submit_queue_->ClearAllTasks();
  }
} else if (queue->second.state_ != rpc::ActorTableData::DEAD) {
  // Actor 正在重启
  queue->second.state_ = rpc::ActorTableData::RESTARTING;
  queue->second.num_restarts_ = num_restarts;
}
```

#### 8.5.3 Actor 任务失败处理

```cpp
// actor_task_submitter.cc:455
// 失败所有待处理的任务
for (const auto &task_id : task_ids_to_fail) {
  auto status = Status::IOError("cancelling task of dead actor");
  bool fail_immediatedly =
      error_info.has_actor_died_error() &&
      error_info.actor_died_error().has_oom_context() &&
      error_info.actor_died_error().oom_context().fail_immediately();

  task_manager_.FailOrRetryPendingTask(task_id,
                                       error_type,
                                       &status,
                                       &error_info,
                                       /*mark_task_object_failed*/ true,
                                       fail_immediatedly);
}
```

#### 8.5.4 Actor 不可用 vs Actor 死亡

```cpp
// actor_task_submitter.cc:711
is_actor_dead = queue.state_ == rpc::ActorTableData::DEAD;

if (is_actor_dead) {
  // Actor 确认死亡，使用 ACTOR_DIED 错误
  const auto &death_cause = queue.death_cause_;
  error_info = gcs::GetErrorInfoFromActorDeathCause(death_cause);
} else {
  // Actor 可能暂时不可用，使用 ACTOR_UNAVAILABLE 错误
  // 可以重试
  error_info.set_error_message("The actor is temporarily unavailable: " +
                               status.ToString());
  error_info.set_error_type(rpc::ErrorType::ACTOR_UNAVAILABLE);
  error_info.mutable_actor_unavailable_error()->set_actor_id(actor_id.Binary());
}
```

### 8.6 死亡信息等待机制

当任务失败但 Actor 状态不明确时，Ray 会等待一段时间获取更详细的死亡信息：

```cpp
// actor_task_submitter.cc:755
if (RayConfig::instance().timeout_ms_task_wait_for_death_info() != 0) {
  // 等待一个宽限期获取死亡信息
  int64_t death_info_grace_period_ms =
      current_time_ms() +
      RayConfig::instance().timeout_ms_task_wait_for_death_info();

  absl::MutexLock lock(&mu_);
  auto &queue = queue_pair->second;
  queue.wait_for_death_info_tasks_.push_back(
      std::make_shared<PendingTaskWaitingForDeathInfo>(
          death_info_grace_period_ms, task_spec, status, error_info));
}

// 超时检查
// actor_task_submitter.cc:503
void ActorTaskSubmitter::CheckTimeoutTasks() {
  int64_t now = current_time_ms();
  for (auto &[actor_id, client_queue] : client_queues_) {
    auto &deque = client_queue.wait_for_death_info_tasks_;
    auto deque_itr = deque.begin();
    while (deque_itr != deque.end() && (*deque_itr)->deadline_ms_ < now) {
      // 超时，使用当前已知的错误信息失败任务
      (*deque_itr)->actor_preempted_ = client_queue.preempted_;
      timeout_tasks.push_back(*deque_itr);
      deque_itr = deque.erase(deque_itr);
    }
  }

  for (auto &task : timeout_tasks) {
    FailTaskWithError(*task);
  }
}
```

### 8.7 Raylet 故障处理

#### 8.7.1 Raylet 心跳检测

GCS 通过心跳机制检测 Raylet 存活状态：

```cpp
// gcs_node_manager.cc:648
if (node_death_info.reason() == rpc::NodeDeathInfo::UNEXPECTED_TERMINATION) {
  // 广播警告到所有 Driver
  std::string type = "node_removed";
  std::ostringstream error_message;
  error_message << "The node with node id: " << node_id
                << " and address: " << removed_node->node_manager_address()
                << " has been marked dead because the detector"
                << " has missed too many heartbeats from it. "
                << "This can happen when a "
                << "(1) raylet crashes unexpectedly (OOM, etc.) "
                << "(2) raylet has lagging heartbeats due to slow network";

  RAY_LOG(WARNING) << error_message.str();
  gcs_publisher_->PublishError(node_id.Hex(), std::move(error_data));
}
```

#### 8.7.2 Raylet 关闭（Drain）

```cpp
// gcs_node_manager.cc:211
void GcsNodeManager::DrainNode(const NodeID &node_id) {
  RAY_LOG(INFO).WithField(node_id) << "DrainNode() for node";
  auto maybe_node = GetAliveNode(node_id);
  if (!maybe_node.has_value()) {
    RAY_LOG(WARNING).WithField(node_id) << "Skip draining node which is already removed";
    return;
  }

  auto raylet_client = raylet_client_pool_->GetOrConnectByAddress(remote_address);
  // 通知 Raylet 优雅关闭
  raylet_client->ShutdownRaylet(
      node_id,
      /*graceful*/ true,
      [node_id](const Status &status, const rpc::ShutdownRayletReply &reply) {
        RAY_LOG(INFO).WithField(node_id) << "Raylet is drained. Status " << status;
      });
}
```

### 8.8 故障恢复流程图

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                            故障检测与处理流程                                    │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  ┌─────────────┐                                                                │
│  │ 故障发生     │                                                                │
│  └──────┬──────┘                                                                │
│         │                                                                        │
│         ▼                                                                        │
│  ┌─────────────────────────────────────────────────┐                            │
│  │ 故障类型判断                                     │                            │
│  │                                                  │                            │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────────────┐  │                            │
│  │  │ Worker  │  │  Node   │  │     Actor       │  │                            │
│  │  │ 进程死亡│  │ 节点死亡│  │ Actor 进程死亡  │  │                            │
│  │  └────┬────┘  └────┬────┘  └────────┬────────┘  │                            │
│  └───────┼────────────┼─────────────────┼──────────┘                            │
│          │            │                 │                                        │
│          ▼            ▼                 ▼                                        │
│  ┌─────────────────────────────────────────────────┐                            │
│  │ 错误信息收集                                     │                            │
│  │                                                  │                            │
│  │  - GetWorkerFailureCause (Worker)               │                            │
│  │  - InferDeathInfo (Node)                        │                            │
│  │  - GetErrorInfoFromActorDeathCause (Actor)      │                            │
│  └────────────────────┬────────────────────────────┘                            │
│                       │                                                          │
│                       ▼                                                          │
│  ┌─────────────────────────────────────────────────┐                            │
│  │ 重试决策                                         │                            │
│  │                                                  │                            │
│  │  RetryTaskIfPossible()                          │                            │
│  │    ├─ num_retries_left > 0 ? → 重试              │                            │
│  │    ├─ is_preempted ? → 不计入重试次数            │                            │
│  │    ├─ num_retries_left == -1 ? → 无限重试        │                            │
│  │    └─ fail_immediately ? → 立即失败              │                            │
│  └────────────────────┬────────────────────────────┘                            │
│                       │                                                          │
│          ┌────────────┴────────────┐                                            │
│          ▼                         ▼                                            │
│  ┌─────────────┐          ┌─────────────────┐                                   │
│  │   重试任务   │          │    任务失败     │                                   │
│  │             │          │                 │                                   │
│  │ 重新调度到  │          │ FailPendingTask │                                   │
│  │ 其他 Worker │          │      │          │                                   │
│  └─────────────┘          │      ▼          │                                   │
│                           │ MarkTaskReturn  │                                   │
│                           │ ObjectsFailed   │                                   │
│                           │      │          │                                   │
│                           │      ▼          │                                   │
│                           │ Put(error_obj)  │                                   │
│                           │      │          │                                   │
│                           │      ▼          │                                   │
│                           │ cv.notify_all() │                                   │
│                           │      │          │                                   │
│                           │      ▼          │                                   │
│                           │ ray.get() 抛出  │                                   │
│                           │ RayTaskError    │                                   │
│                           └─────────────────┘                                   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 8.9 Ray Data 中的故障处理

在 Ray Data 中，当 Raylet/Node/Actor 故障时，错误会传播到 `process_completed_tasks`：

```python
# streaming_executor_state.py

# 1. ray.wait() 返回 ready（包含错误对象）
ready, _ = ray.wait(list(active_tasks.keys()), ...)

# 2. 在 prepare_metadata() 或 ray.get() 时抛出异常
try:
    prepared = task.prepare_metadata()
    # 或
    meta_with_schema = ray.get(meta_ref, timeout=0)
except Exception as e:
    # 这里会捕获 RayActorError, RayTaskError, RayWorkerError 等
    errored_blocks_per_op[state] += 1

    # 根据 max_errored_blocks 配置决定是否继续
    should_ignore = (
        max_errored_blocks < 0
        or max_errored_blocks >= num_errored_blocks
    )
    if should_ignore:
        logger.error(error_message, exc_info=e)
    else:
        raise e from None  # 终止 Pipeline
```

### 8.10 关键配置参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `timeout_ms_task_wait_for_death_info` | 1000 | 等待死亡信息的超时时间（毫秒） |
| `task_retry_attempts` | 3 | 任务默认重试次数 |
| `actor_max_restarts` | 0 | Actor 最大重启次数（0 表示不重启） |
| `max_errored_blocks` | 0 | Ray Data 允许的最大错误 block 数 |
| `raylet_heartbeat_period_milliseconds` | 1000 | Raylet 心跳周期 |
| `num_heartbeats_timeout` | 30 | 心跳超时判定次数 |

### 8.11 Python 层的异常类型

```python
# Python 中捕获到的异常类型
from ray.exceptions import (
    RayTaskError,           # 任务执行异常
    RayActorError,          # Actor 相关异常
    RayWorkerError,         # Worker 进程死亡
    ObjectStoreFullError,   # Object Store 满
    GetTimeoutError,        # ray.get 超时
    TaskCancelledError,     # 任务被取消
    RuntimeEnvSetupError,   # 运行环境配置失败
)

# 在 Ray Data 中
try:
    result = ray.get(ref)
except RayActorError as e:
    # Actor 死亡
    # e.actor_id - 死亡的 Actor ID
    # e.cause - 死亡原因
except RayTaskError as e:
    # 任务失败
    # e.cause - 原始异常
except RayWorkerError as e:
    # Worker 进程死亡
    # e.node_ip - Worker 所在节点 IP
```

### 8.12 总结

| 故障类型 | 检测方 | 通知路径 | 重试策略 |
|----------|--------|----------|----------|
| **Worker 死亡** | Raylet | Raylet → CoreWorker → TaskManager | 根据 `max_retries` 重试 |
| **Node 死亡** | GCS (心跳) | GCS → 订阅者 → TaskManager | 抢占不计入重试次数 |
| **Actor 死亡** | GCS | GCS → ActorTaskSubmitter → TaskManager | 根据 Actor 配置重启 |
| **Raylet 崩溃** | GCS (心跳) | GCS → 所有受影响任务 | 按 Worker 死亡处理 |

**核心设计原则**：
1. **统一的错误传播机制**：所有故障最终都会通过 `FailPendingTask` → `MarkTaskReturnObjectsFailed` → `Put(error_object)` → `cv.notify_all()` 传播
2. **可配置的重试策略**：根据故障类型和配置决定是否重试
3. **死亡信息等待**：为了提供更准确的错误信息，会等待一个宽限期获取详细的死亡原因
4. **抢占特殊处理**：自动伸缩导致的节点抢占不计入重试次数

---

## 9. 节点故障时 Driver 更新 ObjectRef 状态机制

在 Ray Data 场景下，当节点发生故障时，**Driver 作为任务的 Owner 负责更新 ObjectRef 和 StreamingRef 的状态**。本章详细分析这个机制。

### 9.1 核心问题

> 在 Ray Data 场景下，如果有 Node 失败，那么 Node 节点上的 ObjectRef 和 StreamingRef 是 Driver 这边去更改状态的吗？

**答案：是的**。Driver 作为 Task 的 Owner，在本地更新 ObjectRef/StreamingRef 的状态。

### 9.2 完整流程图

```
Node 故障
    │
    ▼
┌─────────────────────────────────────────────────────┐
│ GCS (Global Control Service)                        │
│ 1. 检测心跳超时 → OnNodeFailure()                    │
│ 2. 发布 RAY_NODE_DEATH 通知给所有订阅者              │
└─────────────────────────────────────────────────────┘
    │
    ▼ Pub/Sub 通知
┌─────────────────────────────────────────────────────┐
│ Driver (作为 Task Owner)                            │
│ CoreWorker 收到节点死亡通知                          │
│   │                                                 │
│   ├─→ 对于该节点上正在执行的任务：                    │
│   │   TaskManager::FailPendingTask()                │
│   │     → MarkTaskReturnObjectsFailed()             │
│   │     → MemoryStore::Put(error_object)            │
│   │     → cv_.notify_all()  # ObjectRef 变 ready    │
│   │                                                 │
│   └─→ 对于 StreamingGenerator：                     │
│       TaskManager::MarkEndOfStream()                │
│         → 写入 END_OF_STREAMING_GENERATOR 标记       │
│         → 后续的 ObjectRef 也变 ready (含错误)       │
└─────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────┐
│ Ray Data Streaming Executor                         │
│ ray.wait() 返回 ready                               │
│   → process_completed_tasks() 处理                  │
│   → 检测到错误，触发重试或失败                        │
└─────────────────────────────────────────────────────┘
```

### 9.3 Owner 模式详解

Ray 使用 **Owner 模式** 管理 ObjectRef 的生命周期和状态：

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              Owner 模式                                          │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│   谁是 Owner？                                                                   │
│   ┌─────────────────────────────────────────────────────────┐                   │
│   │ • 对于 ray.remote() 调用的任务：调用方是 Owner          │                   │
│   │ • 在 Ray Data 中：Driver 是几乎所有任务的 Owner         │                   │
│   │ • Actor 方法调用：调用方（通常是 Driver）是 Owner       │                   │
│   └─────────────────────────────────────────────────────────┘                   │
│                                                                                  │
│   Owner 的职责：                                                                 │
│   ┌─────────────────────────────────────────────────────────┐                   │
│   │ • 追踪任务状态（pending, running, completed, failed）   │                   │
│   │ • 管理 ObjectRef 的引用计数                             │                   │
│   │ • 处理任务失败和重试                                    │                   │
│   │ • 写入错误对象到本地 MemoryStore                        │                   │
│   │ • 触发 cv_.notify_all() 唤醒等待者                      │                   │
│   └─────────────────────────────────────────────────────────┘                   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 9.4 关键点：错误对象写入 Driver 本地

当节点故障时，错误对象是写入 **Driver 本地的 MemoryStore**，而不是失败节点的存储：

```cpp
// src/ray/core_worker/task_manager.cc

void TaskManager::MarkTaskReturnObjectsFailed(...) {
  // 创建错误对象
  RayObject error(error_type, ray_error_info);

  // 写入 Driver 本地的 MemoryStore（不是远程节点）
  in_memory_store_.Put(error, object_id, ...);  // ← 本地操作！
}
```

这意味着：
1. **不依赖网络通信**：失败节点已不可达，不需要与其通信
2. **本地触发通知**：`cv_.notify_all()` 在 Driver 本地执行
3. **即时响应**：不需要等待远程确认

### 9.5 两类对象的处理

#### 9.5.1 正在执行的任务（ObjectRef/StreamingRef）

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│ 正在执行的任务                                                                   │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  状态：Worker 正在执行任务，尚未返回结果                                         │
│                                                                                  │
│  故障处理：                                                                      │
│  1. GCS 检测到节点死亡                                                           │
│  2. 通知 Driver（Owner）                                                        │
│  3. Driver 的 TaskManager 调用 FailPendingTask()                                │
│  4. 对于普通 ObjectRef：写入错误对象                                            │
│  5. 对于 StreamingGenerator：                                                   │
│     - 调用 MarkEndOfStream() 标记流结束                                         │
│     - 对所有未完成的 ObjectRef 写入错误对象                                      │
│  6. cv_.notify_all() 唤醒 ray.wait()                                            │
│                                                                                  │
│  代码路径：                                                                      │
│  ┌──────────────────────────────────────────────────────────────────┐           │
│  │ FailPendingTask()                                                │           │
│  │   └─► MarkTaskReturnObjectsFailed()                              │           │
│  │         ├─► in_memory_store_.Put(error, object_id)  // 普通 ref  │           │
│  │         └─► if (IsStreamingGenerator)                            │           │
│  │               ├─► MarkEndOfStream(generator_id)                  │           │
│  │               └─► in_memory_store_.Put(error, generator_return_id)│          │
│  └──────────────────────────────────────────────────────────────────┘           │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

#### 9.5.2 已完成但存储在失败节点的对象

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│ 已完成存储在失败节点的对象                                                       │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  状态：任务已完成，结果存储在 Plasma Store（失败节点上）                          │
│                                                                                  │
│  这种情况与"正在执行"不同：                                                      │
│  - ObjectRef 已经是 ready 状态                                                   │
│  - 但 ray.get() 时发现对象不可达                                                │
│                                                                                  │
│  处理方式：                                                                      │
│  1. Lineage Reconstruction（血统重建）                                          │
│     - 根据 DAG 重新执行产生该对象的任务                                         │
│     - 需要开启 enable_object_reconstruction                                      │
│                                                                                  │
│  2. 抛出异常                                                                     │
│     - 如果无法重建，ray.get() 抛出 ObjectLostError                              │
│                                                                                  │
│  代码示例：                                                                      │
│  ┌──────────────────────────────────────────────────────────────────┐           │
│  │ # 对象已 ready，但 ray.get() 失败                                │           │
│  │ ready, _ = ray.wait([ref])  # 返回 ready                         │           │
│  │ result = ray.get(ref)  # ObjectLostError 或 触发重建             │           │
│  └──────────────────────────────────────────────────────────────────┘           │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 9.6 StreamingGenerator 的特殊处理

StreamingGenerator 需要额外处理流的结束标记：

```cpp
// src/ray/core_worker/task_manager.cc:753
void TaskManager::MarkEndOfStream(const ObjectID &generator_id,
                                  int64_t end_of_stream_index) {
  ObjectID last_object_id;

  // 1. 标记流的结束索引
  stream_it->second.MarkEndOfStream(end_of_stream_index, &last_object_id);

  if (!last_object_id.IsNil()) {
    // 2. 在结束位置写入 END_OF_STREAMING_GENERATOR 标记
    reference_counter_.OwnDynamicStreamingTaskReturnRef(last_object_id, generator_id);

    RayObject error(rpc::ErrorType::END_OF_STREAMING_GENERATOR);
    in_memory_store_.Put(error, last_object_id, ...);  // ← 触发通知
  }
}
```

在 Python 端，`_next_sync()` 会检测到流结束：

```python
# object_ref_generator.py:188
def _next_sync(self, timeout_s):
    try:
        ref = core_worker.try_read_next_object_ref_stream(self._generator_ref)
    except ObjectRefStreamEndOfStreamError:
        # 流结束了
        try:
            ray.get(self._generator_ref)  # 检查是否有异常
        except Exception:
            self._generator_task_raised = True
            return self._generator_ref  # 返回包含异常的 ref
        raise StopIteration
```

### 9.7 代码证据

以下是证明 Driver（Owner）负责更新状态的关键代码：

#### 9.7.1 TaskManager 在 Driver 进程中运行

```cpp
// src/ray/core_worker/core_worker.cc
CoreWorker::CoreWorker(...) {
  // TaskManager 是 CoreWorker 的成员
  task_manager_ = std::make_unique<TaskManager>(
      memory_store_.get(),        // ← Driver 本地的 MemoryStore
      reference_counter_.get(),
      ...);
}
```

#### 9.7.2 FailPendingTask 写入本地 MemoryStore

```cpp
// src/ray/core_worker/task_manager.cc:1559
void TaskManager::MarkTaskReturnObjectsFailed(...) {
  RayObject error(error_type, ray_error_info);

  for (int i = 0; i < num_returns; i++) {
    const auto object_id = ObjectID::FromIndex(task_id, i + 1);

    // in_memory_store_ 是 Driver 本地的存储
    in_memory_store_.Put(error, object_id, ...);
  }
}
```

#### 9.7.3 Put 触发本地 notify_all

```cpp
// src/ray/core_worker/store_provider/memory_store/memory_store.cc:172
void CoreWorkerMemoryStore::Put(...) {
  // 这是 Driver 本地的 MemoryStore
  for (auto &get_request : get_requests) {
    get_request->Set(object_id, object_entry);  // ← 本地唤醒
  }
}

void GetRequest::Set(...) {
  cv_.notify_all();  // ← 本地条件变量
}
```

### 9.8 总结

| 问题 | 答案 |
|------|------|
| 谁更新 ObjectRef 状态？ | **Driver（作为 Owner）** |
| 错误对象写在哪里？ | **Driver 本地的 MemoryStore** |
| 如何触发通知？ | **本地 cv_.notify_all()** |
| GCS 的角色？ | **检测故障并通知订阅者**，不直接更新对象状态 |
| StreamingRef 如何处理？ | **MarkEndOfStream + 写入错误对象** |

**核心结论**：

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                                                                                  │
│   GCS 只负责检测故障并通知                                                       │
│           │                                                                      │
│           ▼                                                                      │
│   Driver（作为 Owner）收到通知后：                                               │
│   1. 在本地 MemoryStore 写入错误对象                                            │
│   2. 触发本地 cv_.notify_all()                                                  │
│   3. ray.wait() 返回 ready                                                      │
│   4. ray.get() 抛出异常                                                         │
│                                                                                  │
│   → 整个状态更新过程在 Driver 本地完成，不依赖失败节点                            │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 附录：关键源码文件

| 文件路径 | 主要内容 |
|----------|----------|
| `python/ray/data/_internal/execution/streaming_executor_state.py` | process_completed_tasks, OpState, Topology |
| `python/ray/data/_internal/execution/interfaces/physical_operator.py` | DataOpTask, MetadataOpTask, PhysicalOperator |
| `python/ray/data/_internal/compute.py` | TaskPoolStrategy, ActorPoolStrategy |
| `python/ray/data/_internal/util.py` | compute strategy 选择逻辑 |
| `python/ray/data/_internal/execution/operators/map_operator.py` | MapOperator 实现 |
| `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py` | ActorPoolMapOperator 实现 |
| `python/ray/data/_internal/actor_autoscaler/default_actor_autoscaler.py` | Actor Pool 自动伸缩逻辑 |
| `python/ray/_private/object_ref_generator.py` | ObjectRefGenerator Python 实现 |
| `src/ray/core_worker/core_worker.cc` | CoreWorker C++ 实现 |
| `src/ray/core_worker/task_manager.cc` | TaskManager 实现 |
| `src/ray/core_worker/store_provider/memory_store/memory_store.cc` | MemoryStore 实现 |
| `src/ray/core_worker/task_submission/actor_task_submitter.cc` | Actor 任务提交与故障处理 |
| `src/ray/core_worker/task_submission/normal_task_submitter.cc` | 普通任务提交与故障处理 |
| `src/ray/gcs/gcs_node_manager.cc` | GCS 节点管理与故障检测 |

---

*文档生成时间：2026-04-23*
