# HashShuffleAggregator 初始化失败链路与跨节点重试分析

> 本文档整理自对 HashShuffleAggregator `__init__` 异常传播、作业失败链路、以及坏节点场景下重试机制的深入分析讨论。

## 目录

1. [问题背景](#1-问题背景)
2. [HashShuffleAggregator 与 _MapWorker 的防御对比](#2-hashshuffleaggregator-与-_mapworker-的防御对比)
3. [`__init__` 异常 → 作业失败的完整链路](#3-__init__-异常--作业失败的完整链路)
4. [`process_completed_tasks` 的异常捕获差异](#4-process_completed_tasks-的异常捕获差异)
5. [`max_task_retries=-1` 为什么无法挽救](#5-max_task_retries-1-为什么无法挽救)
6. [坏节点场景：进程内重试的死循环](#6-坏节点场景进程内重试的死循环)
7. [实现方案：支持 CUDA 异常等跨节点重试](#7-实现方案支持-cuda-异常等跨节点重试)
8. [关键代码位置索引](#8-关键代码位置索引)

---

## 1. 问题背景

HashShuffleAggregator 是 Ray Data hash shuffle 算子的核心 actor，负责聚合分区数据。其创建代码如下：

```python
# hash_shuffle.py:1399-1418
aggregator = HashShuffleAggregator.options(
    **self._aggregator_ray_remote_args
).remote(
    aggregator_id,
    self._num_input_seqs,
    target_partition_ids,
    self._aggregation_factory_ref,
    self._target_max_block_size,
    self._min_max_shards_compaction_thresholds,
)
```

核心问题：如果 `__init__` 中的 `agg_factory()` 抛出异常（例如 CUDA OOM），这个作业会重试还是失败？

**结论：会直接失败，没有任何重试机制。**

---

## 2. HashShuffleAggregator 与 _MapWorker 的防御对比

### _MapWorker — 两层防御

| 层级 | 机制 | 配置 |
|------|------|------|
| 第一层：进程内重试 | `_init_udf_with_retries` | `actor_init_retry_on_errors` / `actor_init_max_retries` |
| 第二层：Ray Core 重建 | `max_restarts=-1` | actor 死后由 GCS 在不同节点重建 |

### HashShuffleAggregator — 无防御

| 层级 | 机制 | 现状 |
|------|------|------|
| 第一层：进程内重试 | **不存在** | `agg_factory()` 直接调用，无 retry |
| 第二层：Ray Core 重建 | `max_restarts=0`（默认） | actor 死后不重建 |

### Ray remote 装饰器对比

```python
# _MapWorker 默认配置 (actor_pool_map_operator.py:562-576)
@ray.remote  # 运行时注入：
#   max_restarts = -1      (无限重建)
#   max_task_retries = -1  (无限重试)

# HashShuffleAggregator (hash_shuffle.py:1609-1612)
@ray.remote(
    max_task_retries=-1  # 仅此一项
    # max_restarts 未设置 → 默认 0 → 不重建
)
class HashShuffleAggregator:
```

### DataContext 配置

```python
# context.py 默认值
DEFAULT_ACTOR_TASK_RETRY_ON_ERRORS = False
DEFAULT_ACTOR_INIT_RETRY_ON_ERRORS = False
DEFAULT_ACTOR_INIT_MAX_RETRIES = 3
```

`actor_init_retry_on_errors` 默认 `False`，即使 `_MapWorker` 也不会在进程内重试 init 错误，除非显式开启。

---

## 3. `__init__` 异常 → 作业失败的完整链路

### 3.1 完整传播链路

```
HashShuffleAggregator.__init__() 抛异常 (如 agg_factory() 失败)
  │
  ▼
[1] Ray Worker 进程捕获 (_raylet.pyx:2289-2297)
  → 设置 is_creation_task_error = True
  → SystemExit 导致 worker 进程退出
  → 退出类型: WorkerExitType::USER_ERROR
  │
  ▼
[2] Raylet 通知 GCS (gcs_actor_manager.cc:1272-1278)
  → 设置 death_cause = creation_task_failure_context
  → 调用 RestartActor(actor_id, need_reschedule, death_cause)
  │
  ▼
[3] GCS RestartActor 检查 max_restarts (gcs_actor_manager.cc:1474-1520)
  → max_restarts=0 → remaining_restarts=0
  → need_reconstruct = false (因为 USER_ERROR)
  → actor 状态 = DEAD (永久死亡)
  │
  ▼
[4] GCS 通知 Driver 端 DisconnectActor (actor_task_submitter.cc:408-466)
  → dead=true, is_restartable=false
  → 对所有 pending tasks: MarkTaskNoRetry(task_id)
  → 将 num_retries_left_ 强制置为 0
  → FailOrRetryPendingTask → RetryTaskIfPossible 返回 false
  │
  ▼
[5] Python 调用方收到 ActorDiedError
  → ActorDiedError(cause=RayTaskError, actor_init_failed=True)
  → "The actor died because of an error raised in its creation task"
```

### 3.2 Shuffle 阶段：submit.remote() 失败路径

```
Aggregator actor DEAD
  │
  ▼
_shuffle_block 内 submit.remote() 返回错误 ObjectRef
  → ray.wait() 检测到 ready
  → _shuffle_block task 本身失败 (无 try/except)
  │
  ▼
MetadataOpTask 完成 → _on_partitioning_done 回调
  → ray.get(task.get_waitable(), timeout=60)  ← 无 try/except
  → 抛出 ActorDiedError
  │
  ▼
异常穿透:
  process_completed_tasks() → _scheduling_loop_step() → run()
  │
  ▼
run() 捕获 (streaming_executor.py:262-290):
  exc = e
  state.mark_finished(exc)
  │
  ▼
get_output_blocking() (streaming_executor_state.py:321-333):
  raise self._exception
  │
  ▼
_ClosingIterator.get_next() (streaming_executor_state.py:1102-1123):
  self._executor.shutdown(force=False, exception=e)
  raise
  │
  ▼
JOB FAILS
```

关键代码 — `_on_partitioning_done` 完全裸露：

```python
# hash_shuffle.py:731-757
def _on_partitioning_done(cur_shuffle_task_idx: int):
    task = self._shuffling_tasks[input_index].pop(cur_shuffle_task_idx)
    # ← 无 try/except
    input_block_metadata, partition_shards_stats = ray.get(
        task.get_waitable(), timeout=60
    )
    # ... 后续处理
```

### 3.3 Finalize 阶段：finalize.remote() 失败路径

```
Aggregator actor DEAD
  │
  ▼
finalize.remote() 返回错误 ObjectRefGenerator
  → DataOpTask streaming generator 遇到 ActorDiedError
  │
  ▼
process_completed_tasks 中 prepare_metadata() 捕获异常
  → 检查 max_errored_blocks (默认=0)
  → 阈值立即超过 → raise e from None
  │
  ▼
同上穿透至 JOB FAILS
```

---

## 4. `process_completed_tasks` 的异常捕获差异

`process_completed_tasks` 对两种任务类型的异常处理**完全不同**：

### DataOpTask（finalize 阶段）— 有 try/except

```python
# streaming_executor_state.py:590
try:
    prepared = task.prepare_metadata()
except Exception as e:
    errored_blocks_per_op[state] += 1
    num_errored_blocks += 1
    should_ignore = (
        max_errored_blocks < 0
        or max_errored_blocks >= num_errored_blocks
    )
    if should_ignore:
        logger.error(error_message, exc_info=e)  # 忽略，继续
    else:
        raise e from None  # abort

# 同样在 complete_with_metadata (streaming_executor_state.py:695-725)
try:
    meta_with_schema = ray.get(meta_ref, timeout=0)
    bytes_read = task.complete_with_metadata(meta_with_schema)
except Exception as e:
    # 同样的 max_errored_blocks 检查逻辑
```

### MetadataOpTask（shuffle 阶段）— 无 try/except

```python
# streaming_executor_state.py:641-643
for state, task in non_data_tasks:
    task.on_task_finished()   # ← 仅调用回调，无任何异常处理
```

`on_task_finished()` 直接调用 `_on_partitioning_done`，其中 `ray.get()` 无 try/except，异常直接穿透。

### 差异总结

| 任务类型 | try/except | 异常控制 | 默认行为 |
|----------|-----------|---------|---------|
| DataOpTask（finalize） | **有** | `max_errored_blocks` 控制 | 默认=0，第一个错误 abort |
| MetadataOpTask（shuffle） | **无** | 无任何控制 | **无条件 abort** |

这意味着 **shuffle 阶段的 aggregator 死亡是最脆弱的路径** — 完全没有容错机制。

---

## 5. `max_task_retries=-1` 为什么无法挽救

### 核心原因：`MarkTaskNoRetry` 在 `FailOrRetryPendingTask` 之前被调用

当 actor 永久死亡时，Ray Core 的 C++ 代码调用链：

```cpp
// actor_task_submitter.cc:442-466 (DisconnectActor, dead=true)
for (auto &task_id : task_ids_to_fail) {
    task_manager_.MarkTaskNoRetry(task_id);       // ← 先强制置0
    task_manager_.FailOrRetryPendingTask(task_id, error_type, ...);  // ← 再尝试重试
}
```

`MarkTaskNoRetry` 的实现：

```cpp
// task_manager.cc:1498-1526
void TaskManager::MarkTaskNoRetryInternal(const TaskID &task_id, bool canceled) {
    auto it = submissible_tasks_.find(task_id);
    if (it != submissible_tasks_.end()) {
        it->second.num_retries_left_ = 0;       // 强制置0
        it->second.num_oom_retries_left_ = 0;
    }
}
```

`RetryTaskIfPossible` 的判断：

```cpp
// task_manager.cc:1138-1252
if (num_retries_left > 0) {
    will_retry = true;
    num_retries_left--;
} else if (num_retries_left == -1) {
    will_retry = true;  // 无限重试
} else {
    RAY_CHECK(num_retries_left == 0);  // NO RETRY
}
```

由于 `MarkTaskNoRetry` 已经将 `num_retries_left_` 置为 0，即使原本是 -1（无限），重试也会被拒绝。

### 不同场景对比

| 场景 | Error Type | `MarkTaskNoRetry` | `max_task_retries` 生效 | 结果 |
|------|-----------|-------------------|----------------------|------|
| actor 永久死亡 (`max_restarts=0`) | `ACTOR_DIED` | **YES** → 置0 | **NO** | 立即失败 |
| actor 正在重启 (`max_restarts>0`) | `ACTOR_UNAVAILABLE` | NO | **YES** | 按配置重试 |
| task 执行异常 (retryable) | `TASK_EXECUTION_EXCEPTION` | NO | **YES** | 按配置重试 |
| 新 task 提交到 DEAD actor | `ACTOR_DIED` | **YES** → 置0 | **NO** | 立即失败 |

---

## 6. 坏节点场景：进程内重试的死循环

### 6.1 进程内重试无法换节点

`_init_udf_with_retries` 是在同一个 actor worker 进程内的 `while` 循环：

```python
# actor_pool_map_operator.py:666-688
def _init_udf_with_retries(self, ctx: DataContext) -> None:
    max_retries = (
        ctx.actor_init_max_retries if ctx.actor_init_retry_on_errors else 0
    )
    last_exception = None
    attempt = 0
    while max_retries < 0 or attempt <= max_retries:
        try:
            self._map_transformer.init()  # ← 同进程同节点
            return
        except Exception as e:
            last_exception = e
            attempt += 1
    raise last_exception
```

每次重试都在**同一个进程、同一个节点**上调用，完全没有换节点的能力。如果节点 GPU 硬件故障，进程内重试**会一直失败**。

### 6.2 `max_restarts` 理论上可以换节点，但 init 失败被阻断

Ray Core 在 actor 重启时，会通过 `SelectRandomAliveNode` 随机选择节点，理论上可以换到好节点：

```cpp
// gcs_actor_scheduler.cc:49-81
void GcsActorScheduler::Schedule(std::shared_ptr<GcsActor> actor) {
    auto node_id = SelectForwardingNode(actor);  // 随机选节点
    LeaseWorkerFromNode(actor, node.value());
}
```

但 init 异常的退出类型是 `USER_ERROR`：

```cpp
// core_worker.cc:3228-3235
if (status.IsCreationTaskError()) {
    Exit(rpc::WorkerExitType::USER_ERROR, ...);
}

// gcs_actor_manager.cc:1216-1217
bool need_reconstruct = disconnect_type != rpc::WorkerExitType::INTENDED_USER_EXIT &&
                        disconnect_type != rpc::WorkerExitType::USER_ERROR;
// USER_ERROR → need_reconstruct = false → actor 永久 DEAD
```

**关键矛盾**：
- `SYSTEM_ERROR`（节点宕机）→ `need_reconstruct=true` → GCS 跨节点重建
- `USER_ERROR`（init 异常）→ `need_reconstruct=false` → actor 直接 DEAD

即使设置 `max_restarts=-1`，init 失败也不会触发重建，因为 GCS 认为这是"代码 bug"而非"环境问题"。

### 6.3 `IsActorRestartable` 的额外阻断

```cpp
// protobuf_utils.cc:164-174
bool IsActorRestartable(const rpc::ActorTableData &actor) {
    return actor.death_cause().context_case() == ContextCase::kActorDiedErrorContext &&
           actor.death_cause().actor_died_error_context().reason() ==
               rpc::ActorDiedErrorContext::OUT_OF_SCOPE &&
           ...;
}
```

init 失败的 `death_cause` 是 `kCreationTaskFailureContext`，不满足 `kActorDiedErrorContext` 条件，即使想通过 lineage reconstruction 恢复也不行。

### 6.4 没有节点级黑名单

| 机制 | 存在 | 作用 |
|------|------|------|
| `failed_nodes_cache_` | 存在 | 仅记录已确认死亡的节点，不是"坏节点"黑名单 |
| `avoid_local_node` | 存在 | 避免本地节点调度（用于 spillback），不是避坏节点 |
| `avoid_gpu_nodes` | 存在 | 非 GPU task 避开 GPU 节点，不是避坏节点 |
| 节点级黑名单 | **不存在** | Ray Core / Ray Data 均无此机制 |

腾讯 Ray 优化方案（`docs/07-设计方案/腾讯Ray优化方案.md`）提出了 actor 级黑名单（非节点级），但**未实现**。

---

## 7. 实现方案：支持 CUDA 异常等跨节点重试

### 方案 A：为 HashShuffleAggregator 添加进程内重试（推荐，改动最小）

参照 `_MapWorker._init_udf_with_retries` 模式：

```python
# hash_shuffle.py - HashShuffleAggregator

def __init__(self, aggregator_id, num_input_seqs, target_partition_ids,
             agg_factory, target_max_block_size,
             min_max_shards_compaction_thresholds=None):
    self._aggregator_id = aggregator_id
    self._target_max_block_size = target_max_block_size
    self._max_num_blocks_compaction_threshold = (
        min_max_shards_compaction_thresholds[1]
        if min_max_shards_compaction_thresholds is not None else None
    )

    # 替换原来的 agg_factory() 直接调用
    self._aggregation = self._init_aggregation_with_retries(agg_factory)

    min_num_blocks_compaction_threshold = (
        min_max_shards_compaction_thresholds[0]
        if min_max_shards_compaction_thresholds is not None else None
    )

    self._input_seq_partition_buckets = self._allocate_partition_buckets(
        num_input_seqs, target_partition_ids,
        min_num_blocks_compaction_threshold,
    )

    self._bg_thread = threading.Thread(
        target=self._debug_dump,
        name="hash_shuffle_aggregator_debug_dump",
        daemon=True,
    )
    self._bg_thread.start()

def _init_aggregation_with_retries(self, agg_factory):
    ctx = DataContext.get_current()
    max_retries = (
        ctx.actor_init_max_retries if ctx.actor_init_retry_on_errors else 0
    )
    last_exception = None
    attempt = 0
    while max_retries < 0 or attempt <= max_retries:
        try:
            return agg_factory()
        except Exception as e:
            last_exception = e
            logger.debug(
                f"Failed to init aggregation on attempt {attempt + 1} "
                f"(max_retries={'infinite' if max_retries < 0 else max_retries}): {e}",
                exc_info=True,
            )
            attempt += 1
    raise last_exception
```

**优点**：
- 复用已有 `DataContext.actor_init_retry_on_errors` / `actor_init_max_retries`
- 改动最小，只改 `HashShuffleAggregator` 一个类
- 与 `_MapWorker` 行为一致

**局限**：
- 同一节点重试，坏节点场景无效

**用户使用**：
```python
ctx = DataContext.get_current()
ctx.actor_init_retry_on_errors = True
ctx.actor_init_max_retries = -1  # 无限重试，适合 CUDA OOM 等瞬态错误
```

### 方案 B：模拟 SYSTEM_ERROR 实现跨节点重建（解决坏节点问题）

在进程内重试耗尽后，主动 `os._exit(1)` 让 worker 以非 USER_ERROR 方式退出，触发 GCS 跨节点重建：

```python
def _init_aggregation_with_retries(self, agg_factory):
    ctx = DataContext.get_current()
    max_retries = (
        ctx.actor_init_max_retries if ctx.actor_init_retry_on_errors else 0
    )
    last_exception = None
    attempt = 0
    while max_retries < 0 or attempt <= max_retries:
        try:
            return agg_factory()
        except Exception as e:
            last_exception = e
            attempt += 1
    # 重试耗尽，主动退出进程让 GCS 重建
    # os._exit(1) → WorkerExitType=SYSTEM_ERROR → need_reconstruct=true
    # 配合 max_restarts=-1，actor 会在不同节点重建
    logger.error(f"Init failed after {attempt} retries: {last_exception}")
    os._exit(1)
```

同时需要在 `_derive_final_shuffle_aggregator_ray_remote_args` 中添加 `max_restarts`：

```python
finalized_remote_args = {
    "max_concurrency": max_concurrency,
    "max_restarts": -1,  # 允许无限重建
    **aggregator_ray_remote_args,
}
```

**链路变化**：
```
进程内重试失败 → os._exit(1) → worker 进程异常退出
  → Raylet 检测到 → 通知 GCS (非 USER_ERROR)
  → need_reconstruct = true
  → GCS 触发重建 → SelectRandomAliveNode → 可能换到好节点
  → 新节点上再次进程内重试 → 成功则继续，失败则再次 exit+重建
```

**风险**：
- `os._exit(1)` 不是 Ray 的标准行为，可能影响监控和日志
- 如果大量节点都有问题，可能产生频繁重建循环
- 需要增加重建次数上限（如 `max_restarts=10`），防止无限循环

### 方案 C：修改 Ray Core 让 init 异常可重建（最规范，改动最大）

在 GCS 中增加规则，对特定异常类型（如 CUDA Error）即使来自 init 也允许重建：

```cpp
// gcs_actor_manager.cc
// 修改 need_reconstruct 的判断逻辑
bool need_reconstruct = false;
if (disconnect_type == rpc::WorkerExitType::USER_ERROR) {
    // 检查 creation_task_exception 是否为可重试的环境异常
    if (IsRetryableEnvironmentError(creation_task_exception)) {
        need_reconstruct = true;  // CUDA OOM 等视为环境问题
    }
}
```

这是最规范的方案，但需要修改 Ray Core，改动范围大。

### 推荐组合

**方案 A + 方案 B** 是不修改 Ray Core 的前提下最实用的组合：

1. 第一层：进程内重试（方案 A），处理瞬态错误（如临时 CUDA OOM 后释放可用）
2. 第二层：跨节点重建（方案 B），处理坏节点场景（如硬件故障）
3. 加上重建次数上限（如 `max_restarts=10`），防止无限循环

---

## 8. 关键代码位置索引

### HashShuffleAggregator 相关

| 位置 | 文件:行号 | 说明 |
|------|----------|------|
| Actor 定义 | `hash_shuffle.py:1609-1612` | `@ray.remote(max_task_retries=-1)` |
| `__init__` | `hash_shuffle.py:1632-1664` | `agg_factory()` 直接调用，无重试 |
| AggregatorPool.start | `hash_shuffle.py:1390-1419` | 创建 aggregator，无 try/except |
| _on_partitioning_done | `hash_shuffle.py:731-757` | `ray.get()` 无 try/except |
| _on_aggregation_done | `hash_shuffle.py:878-892` | 仅 log，不 re-raise |
| 默认 remote args | `hash_shuffle.py:1142-1157` | 无 max_restarts |
| derive_final_args | `hash_shuffle.py:1513-1541` | 无 max_restarts |

### _MapWorker 对比

| 位置 | 文件:行号 | 说明 |
|------|----------|------|
| 默认 remote args | `actor_pool_map_operator.py:562-576` | `max_restarts=-1`, `max_task_retries=-1` |
| `_init_udf_with_retries` | `actor_pool_map_operator.py:666-688` | 进程内 UDF init 重试 |

### Ray Core 异常处理

| 位置 | 文件:行号 | 说明 |
|------|----------|------|
| Worker 捕获 init 异常 | `_raylet.pyx:2289-2297` | `is_creation_task_error=True` |
| WorkerExitType=USER_ERROR | `core_worker.cc:3228-3235` | init 异常 → USER_ERROR |
| GCS need_reconstruct 判断 | `gcs_actor_manager.cc:1216-1217` | USER_ERROR → false |
| RestartActor | `gcs_actor_manager.cc:1474-1520` | max_restarts=0 → DEAD |
| MarkTaskNoRetry | `actor_task_submitter.cc:242,452` | 置 num_retries_left_=0 |
| FailOrRetryPendingTask | `actor_task_submitter.cc:168-262` | Dead actor 不重试 |
| IsActorRestartable | `protobuf_utils.cc:164-174` | creation_task_failure 不可重启 |
| ActorDiedError 定义 | `exceptions.py:260-320` | actor_init_failed=True |
| SelectForwardingNode | `gcs_actor_scheduler.cc:49-81` | 随机选节点（理论上可换节点） |

### StreamingExecutor 错误传播

| 位置 | 文件:行号 | 说明 |
|------|----------|------|
| DataOpTask try/except | `streaming_executor_state.py:590-640` | max_errored_blocks 控制 |
| MetadataOpTask 无 try/except | `streaming_executor_state.py:641-643` | 直接调用回调 |
| run() 捕获异常 | `streaming_executor.py:262-290` | 设置 state.mark_finished(exc) |
| get_output_blocking | `streaming_executor_state.py:321-333` | raise self._exception |
| _ClosingIterator.get_next | `streaming_executor_state.py:1102-1123` | shutdown + re-raise |

### DataContext 配置

| 位置 | 文件:行号 | 说明 |
|------|----------|------|
| 默认值 | `context.py:221-225` | `DEFAULT_ACTOR_INIT_RETRY_ON_ERRORS=False` |
| DataContext 字段 | `context.py:~760-770` | `actor_init_retry_on_errors`, `actor_init_max_retries` |
