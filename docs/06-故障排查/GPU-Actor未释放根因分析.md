# Ray Data GPU Actor 未释放根因分析

## 1. 问题现象

### 1.1 环境信息

| 项目 | 值 |
|------|-----|
| Ray 分支 | release-2.54.2 (自定义修改版) |
| 未释放集群 Job ID | `1c000000` |
| 已释放集群 Job ID | `06000000` (602 GPU 集群) |
| 问题 Operator | `MapBatches(DistributedQwenVLVideoProcessMapper)` |
| Pipeline 配置 | `processing_mode="qwenvl"`, `streaming_gpu_concurrency=2` |

### 1.2 症状描述

- **未释放集群**：QwenVL operator 的 Ray Data 进度条显示 `Actors: 0`（表明 Ray Data 内部 actor pool 已清空），但 Ray Dashboard 上仍有 **747 个 MapWorker actor** 和 **9059 个 QwenVLCPUPreprocessActor** 处于 ALIVE 状态，持续占用 GPU 资源。
- **已释放集群**：相同代码运行，actor 在 operator 处理完毕后**自然释放**（用户确认：手动停止 job 之前 actor 已经释放了）。
- 两个集群运行**相同代码**，行为不同。

### 1.3 核心疑问

1. 为什么 Ray Data 显示 `Actors: 0` 但 Ray Core 层面 actor 仍然存活？
2. 为什么两个相同代码的 job 表现不同——一个自然释放，一个不释放？

---

## 2. 排查方式与工具

### 2.1 排查路径

```
问题现象
  ├── 1. Actor 生命周期分析（创建 → 使用 → 释放）
  │     ├── ActorPoolMapOperator._release_running_actor()
  │     ├── _ActorPool.shutdown()
  │     └── StreamingExecutor.shutdown()
  ├── 2. Actor 引用计数分析（Python GC + Ray 分布式引用计数）
  │     ├── ActorHandle.__del__() → RemoveActorHandleReference()
  │     ├── DataOpTask 持有的引用链
  │     └── ObjectRefGenerator 引用分析
  ├── 3. Autoscaler 缩容逻辑分析
  │     ├── DefaultActorAutoscaler._derive_target_scaling_config()
  │     ├── _inputs_complete 状态传播
  │     └── compute_downscale_delta() 每次只缩 1 个
  └── 4. 多阶段 Pipeline 状态传播分析
        ├── update_operator_states() → all_inputs_done()
        └── has_completed() 依赖链
```

### 2.2 涉及的关键源码文件

| 文件 | 关注点 |
|------|--------|
| `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py` | Actor pool 管理、actor 释放逻辑 |
| `python/ray/data/_internal/execution/streaming_executor.py` | 调度主循环、shutdown 逻辑 |
| `python/ray/data/_internal/execution/streaming_executor_state.py` | Operator 状态管理、完成状态传播 |
| `python/ray/data/_internal/actor_autoscaler/default_actor_autoscaler.py` | Actor 自动缩容逻辑 |
| `python/ray/data/_internal/actor_autoscaler/actor_pool_resizing_policy.py` | 缩容策略（每次缩 1 个） |
| `python/ray/data/_internal/execution/interfaces/physical_operator.py` | DataOpTask、has_completed()、has_execution_finished() |
| `python/ray/data/_internal/execution/operators/map_operator.py` | _submit_data_task、_data_tasks 管理 |
| `python/ray/actor.py` | ActorHandle GC 机制 |
| `python/ray/data/context.py` | `_enable_actor_pool_on_exit_hook` 默认值 |
| `pipeline/multi_video_classifier_merge/pipeline_builder.py` | Pipeline 构建逻辑 |

---

## 3. 详细排查过程

### 3.1 阶段一：Actor 释放路径分析

#### 3.1.1 Actor 释放只依赖 Python GC，不调用 ray.kill()

追踪 `StreamingExecutor.shutdown()` 的完整调用链：

```
StreamingExecutor.__del__()
  → self.shutdown(force=False)           # force 永远是 False (line 283)
    → op.shutdown(timer, force=force)    # force=False (line 347)
      → ActorPoolMapOperator._do_shutdown(force=False)
        → self._actor_pool.shutdown(force=False)
          → self._release_running_actors(force=False)
            → for actor in running:
                self._release_running_actor(actor)   # 只做 del，不 ray.kill
              # if force:  ← 此处 force=False，ray.kill 永远不执行
              #     ray.kill(actor)
```

**关键代码** (`actor_pool_map_operator.py:1434-1472`)：

```python
def _release_running_actor(self, actor):
    if actor not in self._running_actors:
        return None
    # 更新统计信息
    actor_state = self._running_actors[actor]
    self._total_num_tasks_in_flight -= actor_state.num_tasks_in_flight
    if actor_state.num_tasks_in_flight > 0:
        self._num_active_actors -= 1
    if actor_state.is_restarting:
        self._num_restarting_actors -= 1
    # on_exit hook 默认不执行
    if self._enable_actor_pool_on_exit_hook:  # 默认 False
        ref = actor.on_exit.remote()
    else:
        ref = None
    # 仅从字典中删除引用，依赖 Python GC 释放
    del self._running_actors[actor]
    del self._actor_to_logical_id[actor]
    return ref
```

**结论**：Actor 释放**完全依赖 Python 引用计数/GC**。`_release_running_actor` 只是从内部字典中删除 actor handle 引用，期望 Python GC 回收后触发 `ActorHandle.__del__()` → `RemoveActorHandleReference()` → 分布式引用计数归零 → actor 进程被终止。

#### 3.1.2 `_enable_actor_pool_on_exit_hook` 默认为 False

在 `context.py:711`：
```python
_enable_actor_pool_on_exit_hook: bool = False
```

这意味着 `_MapWorker.on_exit()` 永远不会被显式调用，UDF 的 `__del__` 清理（包括 `DistributedQwenVLVideoProcessMapper.__del__` 中的 `ray.kill(actor)` 清理子 actor 的逻辑）只有在 MapWorker actor 进程被终止时才会触发。

#### 3.1.3 max_restarts=-1 的影响

`ActorPoolMapOperator._apply_default_remote_args()` (`actor_pool_map_operator.py:559-585`) 默认设置：
```python
if "max_restarts" not in ray_remote_args:
    ray_remote_args["max_restarts"] = -1  # 无限重启
```

Actor 生命周期与 `max_restarts=-1` 的交互：
- 当所有 ActorHandle Python 引用被 GC 回收后，actor 经历两阶段终止：
  1. **OUT_OF_SCOPE**：actor 进程被杀死（但 `max_restarts=-1` 可能让其重启）
  2. **REF_DELETED**：C++ `WaitForActorRefDeleted` 最终确认所有引用已删除，永久终止 actor
- 经研究确认：即使 `max_restarts=-1`，当所有 handle 引用被正确删除后，actor **最终会被永久终止**。

### 3.2 阶段二：Actor Handle 引用链分析

#### 3.2.1 ActorHandle 不存在循环引用

分析 `python/ray/actor.py` 中 `ActorHandle` 的实现：

| 组件 | 是否持有 ActorHandle 引用 | 说明 |
|------|---------------------------|------|
| `_ActorMethodMetadata` (永久存储) | **否** | 设计上刻意不引用 ActorHandle 以避免循环引用 |
| `ActorMethod` (临时对象) | 是 (`self._actor`) | 仅在 `actor.method_name` 属性访问时创建，立即使用后释放 |
| `ObjectRef` | **否** | 只存储 ObjectID 和 owner 地址，不引用 ActorHandle |
| `ObjectRefGenerator` | **否** | 只存储 `ObjectRef` 和 `worker`，不引用 ActorHandle |

**结论**：ActorHandle 本身不存在循环引用问题，Python 的引用计数可以正确处理。

#### 3.2.2 DataOpTask 中的引用保持

每个提交到 actor 的 task 都创建一个 `DataOpTask` 对象，存储在 `MapOperator._data_tasks` 字典中。

**引用链**：
```
MapOperator._data_tasks[task_index]
  → DataOpTask
    → _streaming_gen (ObjectRefGenerator，来自 actor.submit.remote())
    → _task_done_callback (functools.partial)
      → 内层 _task_done_callback (closure，capture 了 task_done_callback)
        → partial(_task_done_callback, actor_to_return=actor)  ← 持有 ActorHandle!
```

**关键发现**：`DataOpTask._task_done_callback` 通过 `functools.partial` 闭包持有 `ActorHandle` 引用。即使 task 已完成（`_has_finished=True`），`DataOpTask` 的属性**不会被清空**（没有 `self._task_done_callback = None` 这样的清理逻辑）。

**Task 完成后的清理**：
1. `DataOpTask.on_data_ready()` 捕获 `StopIteration` → 调用 `_task_done_callback(None)`
2. `MapOperator._task_done_callback` → `self._data_tasks.pop(task_index)` 从字典中移除
3. `task_done_callback()` → `self._actor_pool.on_task_completed(actor)` 返还 actor 到 pool

`_data_tasks.pop()` 后，`DataOpTask` 只在执行器调度循环的局部变量 `active_tasks` 中被引用。一旦当前调度循环迭代结束，局部变量超出作用域，`DataOpTask` 变为 GC 候选。

**风险点**：如果 `DataOpTask` 因为任何原因被意外保留（如存在异常 traceback 引用、debugger 保持等），ActorHandle 引用将无法释放。

#### 3.2.3 子 Actor（QwenVLCPUPreprocessActor）的生命周期

`DistributedQwenVLVideoProcessMapper` 在 UDF 内部创建子 actor：
```python
# mappers/distributed_qwen_vl_video_process_mapper.py
def __del__(self):
    self._shutdown_llm()
    for actor in self._actors:
        try:
            ray.kill(actor)
        except Exception:
            pass
```

子 actor 的清理依赖 UDF 的 `__del__`，而 UDF 的 `__del__` 只有在 MapWorker actor 进程被终止时才会触发（因为 `_enable_actor_pool_on_exit_hook=False`，不会显式调用 `on_exit()`）。

**Actor 所有权链**：
```
Driver → MapWorker actor → QwenVLCPUPreprocessActor (子 actor)
```

根据 Ray Core 的 actor 所有权机制，当 MapWorker actor 进程终止时，子 actor 也会通过 `OnWorkerDead → GenOwnerDiedCause` 被永久终止。**但如果 MapWorker 不终止，子 actor 也不会终止。**

### 3.3 阶段三：Autoscaler 缩容逻辑分析

#### 3.3.1 缩容触发条件

`DefaultActorAutoscaler._derive_target_scaling_config()` (`default_actor_autoscaler.py:98-202`) 有两条缩容路径：

**路径 A：强制缩容（"consumed all inputs"）**
```python
if op.has_completed() or (
    op._inputs_complete and op_state.total_enqueued_input_blocks() == 0
):
    num_to_scale_down = self._compute_downscale_delta(actor_pool)  # 返回 1
    return ActorPoolScalingRequest.downscale(
        delta=-num_to_scale_down, force=True, reason="consumed all inputs"
    )
```
- `force=True`：绕过 10 秒 debounce 冷却期
- **不检查 min_size**：理论上可以缩到 0
- 每次只缩 1 个（`DefaultResizingPolicy.compute_downscale_delta` 固定返回 1）

**路径 B：基于利用率的缩容**
```python
elif util <= self._actor_pool_scaling_down_threshold:
    if actor_pool.current_size() <= actor_pool.min_size():
        return ActorPoolScalingRequest.no_op(reason="reached min size")
    max_can_release = actor_pool.current_size() - actor_pool.min_size()
    num_to_scale_down = min(compute_downscale_delta(), max_can_release)
    return ActorPoolScalingRequest.downscale(delta=-num_to_scale_down, ...)
```
- **受 min_size 约束**：不会缩到 min_size 以下
- **受 10 秒 debounce 约束**（非 force）

#### 3.3.2 `_inputs_complete` 状态传播链

`_inputs_complete` 在 operator 上的设置依赖上游 operator 的完成状态传播：

```
update_operator_states() (streaming_executor_state.py:519-558)
  → 遍历所有 operator
    → 检查每个上游依赖是否 has_completed() == True 且 output_queue 为空
      → 如果所有上游都完成 → 调用 op.all_inputs_done()
        → 设置 op._inputs_complete = True
```

**关键：时序问题**

在 `streaming_executor.py` 的调度循环中（`_scheduling_loop_step`）：
```python
# Phase 5: Housekeeping
self._cluster_autoscaler.try_trigger_scaling()    # Line 823
self._actor_autoscaler.try_trigger_scaling()      # Line 824 ← autoscaler 先运行
...
update_operator_states(topology)                   # Line 832 ← 状态更新后运行
```

**Autoscaler 在 `update_operator_states()` 之前运行**，意味着状态更新有**一个迭代的延迟**。这在正常情况下不是问题（仅延迟约 50-100ms），但在边界条件下可能导致 autoscaler 看到的状态不是最新的。

#### 3.3.3 `has_completed()` 依赖链

```
has_completed()
  ├── has_execution_finished()
  │     ├── _is_execution_marked_finished, OR
  │     ├── _inputs_complete == True
  │     │     AND num_active_tasks() == 0     ← len(self._data_tasks)
  │     │     AND internal_input_queue_num_blocks() == 0
  │     │
  │     └── (MapOperator.num_active_tasks() 只计 _data_tasks，不计 metadata tasks)
  │
  ├── internal_output_queue_num_blocks == 0
  └── not has_next()
```

**关键**：`num_active_tasks()` 对 MapOperator 只返回 `len(self._data_tasks)`，排除了 metadata tasks（如 actor 启动任务）。这是正确的设计——pending actor 不应阻止 operator 完成。

#### 3.3.4 每次只缩 1 个的时序影响

`DefaultResizingPolicy.compute_downscale_delta()` 固定返回 `1`：
```python
def compute_downscale_delta(self, actor_pool: "AutoscalingActorPool") -> int:
    return 1
```

假设有 N 个 actor 需要缩容：
- 每次调度循环迭代（约 50-100ms）只能缩 1 个
- 缩完 N 个 actor 需要 N 次迭代 ≈ N × 100ms
- 对于 747 个 MapWorker，理论需要约 75 秒

但如果 actor 都处于忙碌状态（有 in-flight tasks），则使用**延迟缩容**机制：
1. `_remove_inactive_actor()` 找不到空闲 actor
2. 设置 `_pending_scale_down_count = 1`
3. `get_available_actors()` 排除被标记缩容的 actor（不再分配新任务）
4. actor 上的 task 完成后，`on_task_completed()` 触发 `_release_running_actor()`

### 3.4 阶段四：潜在死锁场景分析

#### 3.4.1 Task 卡住导致 Actor 无法释放

**最关键的潜在死锁场景**：

```
1. operator 的 _inputs_complete = True
2. 所有 bundle 已从 _bundle_queue 分发为 task
3. 但某些 task 的 streaming generator 永远不结束（UDF 内部死锁/hang）
4. → num_active_tasks() > 0（_data_tasks 中仍有 DataOpTask）
5. → has_execution_finished() 返回 False
6. → has_completed() 返回 False
7. → 上游 operator 的完成状态无法传播到下游
8. → Autoscaler 的强制缩容路径条件满足（_inputs_complete=True 且 enqueued=0）
9. → 每次循环请求缩 1 个，但 actor 都是 busy 的
10. → _pending_scale_down_count = 1，actor 被排除出可用列表
11. → 但 task 永远不完成 → actor 永远不释放
```

**在这种情况下**：
- `Actors: 0` 显示是因为 `_running_actors` 字典被清空（autoscaler 或 shutdown 路径执行了 `del`）
- 但 Python GC 未能回收 ActorHandle（因为 `DataOpTask` 中的 callback partial 仍持有引用）
- 或者 GC 已回收 Python 对象，但 C++ 层的分布式引用计数未归零

#### 3.4.2 Actor Task 无法强制取消

```python
# physical_operator.py:807-825
def _cancel_active_tasks(self, force: bool):
    for task in tasks:
        task._cancel(force=force)

# DataOpTask._cancel():
def _cancel(self, force: bool):
    is_actor_task = not self.get_task_id().actor_id().is_nil()
    ray.cancel(
        self.get_waitable(),
        recursive=True,
        force=force and not is_actor_task,  # Actor task 的 force 永远为 False!
    )
```

**Actor task 不能被强制取消**。`ray.cancel` 对 actor task 只进行**协作式取消**，如果 UDF 不检查取消标志，task 将永远不会被取消。

#### 3.4.3 `_data_tasks` 的清理时机

| 时机 | 触发方式 | 是否清理 callback 引用 |
|------|----------|------------------------|
| 单个 task 完成 | `_task_done_callback` → `_data_tasks.pop(task_index)` | DataOpTask 被移出字典，但内部属性不清空 |
| Operator shutdown | `MapOperator._do_shutdown()` → `self._data_tasks.clear()` | 所有 DataOpTask 被释放 |
| 正常运行中 | - | DataOpTask 的 `_task_done_callback`, `_streaming_gen` 等属性**永远不会被主动清空** |

### 3.5 阶段五：两个 Job 行为差异的可能原因

已排除的假设：
- ~~已释放 Job 是因为 driver 退出导致 OWNER_DIED~~ → 用户确认 actor 在手动停止 job 之前已自然释放

**可能的差异原因**：

#### 原因 1：Task 卡住的非确定性

`DistributedQwenVLVideoProcessMapper` 内部逻辑复杂（vLLM 推理 + CPU actor 预处理 + 远程文件下载），存在多个可能 hang 的点：
- vLLM 引擎推理超时/死锁
- CPU actor 预处理 hang
- 视频下载/解码 hang
- OOM 导致进程状态异常

如果已释放 Job 的所有 task 都正常完成，autoscaler 的 "consumed all inputs" 强制缩容路径正常工作，每次缩 1 个，逐步释放所有 actor。

如果未释放 Job 有 task 卡住，则：
- `num_active_tasks() > 0` 阻止 `has_execution_finished()`
- 但 `_inputs_complete=True` 且 `total_enqueued_input_blocks()==0`，autoscaler 仍然会请求缩容
- Actor 由于有 in-flight task 无法立即释放（`on_task_completed` 不会被调用）
- `_pending_scale_down_count` 每次迭代被重置为 1（不累积），但由于 task 永不完成，actor 也永不变为 idle

#### 原因 2：Python GC 的非确定性

即使 `_release_running_actor()` 从字典中删除了 actor handle，如果存在其他意外引用（如：
- `DataOpTask` 中 callback partial 持有的引用
- 异常 traceback 中的引用
- 某些 debug/logging 框架保留的引用
- CPython GC 的循环收集器未及时触发

这些都可能导致 ActorHandle 的 `__del__` 延迟或无法执行，进而导致 C++ 层 `RemoveActorHandleReference()` 不被调用，分布式引用计数不归零，actor 进程不被终止。

#### 原因 3：集群资源差异导致的行为差异

602 GPU 集群与未释放集群可能在以下方面不同：
- 内存压力导致 Python GC 行为不同
- 调度延迟导致 task 完成顺序不同
- 网络/存储条件导致 UDF 执行时间不同

---

## 4. 排查结论

### 4.1 根因总结

**核心问题：Ray Data 的 actor 释放机制存在脆弱性，完全依赖 Python GC 的引用计数来终止 actor，缺少 `ray.kill()` 作为兜底手段。**

具体而言：

1. **`shutdown(force=False)` 永远不调用 `ray.kill()`**：正常关闭路径只从内部字典中删除 actor handle 引用，依赖 Python GC 回收后通过分布式引用计数机制终止 actor。如果任何地方意外持有引用，actor 将永远存活。

2. **`_enable_actor_pool_on_exit_hook=False`**：默认不调用 `on_exit()` 来显式触发 UDF 清理。这意味着子 actor（如 `QwenVLCPUPreprocessActor`）只能在 MapWorker 进程终止后通过 owner-died 机制被清理。

3. **DataOpTask 不主动清理引用**：task 完成后 `_has_finished=True`，但 `_task_done_callback`（持有 ActorHandle partial）和 `_streaming_gen` 不会被清空。这为引用泄漏创造了窗口。

4. **Actor task 无法强制取消**：如果 UDF 内部 hang，actor task 无法被强制终止，相关的 DataOpTask 和 ActorHandle 引用将永远存在。

5. **缩容策略保守**：每次只缩 1 个 actor，对于大规模 actor pool（747 个），需要大量迭代才能完成缩容。

### 4.2 `Actors: 0` 但 actor 仍存活的解释

| Ray Data 层面 | Ray Core 层面 |
|---------------|---------------|
| `_running_actors` 字典已清空 | ActorHandle 的 Python 引用可能仍被 DataOpTask callback 等持有 |
| `_pending_actors` 字典已清空 | 即使 Python 引用清空，`RemoveActorHandleReference` 可能未被调用（GC 延迟） |
| 进度条显示 `Actors: 0` | 分布式引用计数未归零，actor 进程仍在运行 |
| 内部状态认为 actor 已释放 | Ray Core 认为 actor 仍在使用中 |

---

## 5. 解决办法

### 5.1 短期修复（推荐）

#### 方案 A：在 `_release_running_actor` 中添加显式 `ray.kill()`

修改 `actor_pool_map_operator.py` 中的 `_release_running_actor` 方法，在删除引用后显式调用 `ray.kill()`：

```python
def _release_running_actor(self, actor):
    if actor not in self._running_actors:
        return None
    # ... 现有的统计更新逻辑 ...
    if self._enable_actor_pool_on_exit_hook:
        ref = actor.on_exit.remote()
    else:
        ref = None
    del self._running_actors[actor]
    del self._actor_to_logical_id[actor]

    # 新增：显式终止 actor，不依赖 GC
    try:
        ray.kill(actor, no_restart=True)
    except Exception:
        pass

    return ref
```

**优点**：简单直接，确保 actor 被终止
**缺点**：`ray.kill` 会阻止 lineage 重建；但在 release 场景下 lineage 已不需要

#### 方案 B：启用 `_enable_actor_pool_on_exit_hook`

在 pipeline 代码中启用 actor 退出钩子：

```python
import ray
ctx = ray.data.DataContext.get_current()
ctx._enable_actor_pool_on_exit_hook = True
```

**优点**：触发 UDF 的 `__del__` 清理子 actor
**缺点**：不能解决 MapWorker 自身不释放的问题

#### 方案 C：在 `_do_shutdown` 中使用 `force=True`

修改 `StreamingExecutor.shutdown()` 或 `ActorPoolMapOperator._do_shutdown()`，在 operator 完成后使用 `force=True` 关闭：

```python
def _do_shutdown(self, force: bool = False):
    # 当 operator 已完成所有输入处理时，强制关闭
    if self._inputs_complete:
        force = True
    self._actor_pool.shutdown(force=force)
    super()._do_shutdown(force)
```

### 5.2 中期改进

#### 方案 D：DataOpTask 完成后清理引用

在 `DataOpTask` 完成时主动清理内部引用：

```python
# physical_operator.py DataOpTask.on_data_ready()
except StopIteration:
    self._task_done_callback(None)
    self._has_finished = True
    # 新增：清理引用，帮助 GC
    self._task_done_callback = None
    self._output_ready_callback = None
    self._streaming_gen = None
    break
```

#### 方案 E：增加 stuck task 检测和超时机制

为 DataOpTask 添加超时检测：

```python
class DataOpTask:
    def __init__(self, ...):
        ...
        self._last_output_time = time.monotonic()
        self._task_timeout_s = 3600  # 1 小时超时

    def on_data_ready(self, max_bytes_to_read):
        self._last_output_time = time.monotonic()
        ...

    def is_stuck(self) -> bool:
        return (time.monotonic() - self._last_output_time) > self._task_timeout_s
```

#### 方案 F：批量缩容策略

修改 `DefaultResizingPolicy.compute_downscale_delta` 在 "consumed all inputs" 场景下返回全部需要释放的 actor 数量：

```python
def compute_downscale_delta(self, actor_pool: "AutoscalingActorPool") -> int:
    # 当所有输入已消费完毕时，一次性释放所有空闲 actor
    idle = actor_pool.num_idle_actors()
    if idle > 0:
        return idle
    return 1
```

### 5.3 长期架构改进

1. **Actor 释放不应完全依赖 Python GC**：应在 `_release_running_actor` 中使用 `ray.kill(actor, no_restart=True)` 作为确定性释放手段。
2. **添加 actor 释放确认机制**：在 `_release_running_actor` 后验证 actor 确实被终止。
3. **统一 shutdown 策略**：当 operator 完成执行后（`has_execution_finished()`），应自动使用 `force=True` 关闭 actor pool。
4. **添加 actor 生命周期监控指标**：暴露 "Ray Data 认为已释放但 Ray Core 仍存活" 的 actor 数量。

---

## 6. 参考信息

### 6.1 关键常量

| 常量 | 值 | 文件 | 说明 |
|------|-----|------|------|
| `_enable_actor_pool_on_exit_hook` | `False` | `context.py:711` | 不调用 UDF 退出钩子 |
| `max_restarts` | `-1` | `actor_pool_map_operator.py:571` | Actor 无限重启 |
| `max_task_retries` | `-1` | `actor_pool_map_operator.py:576` | Task 无限重试 |
| `_ACTOR_POOL_SCALE_DOWN_DEBOUNCE_PERIOD_S` | `10` | `actor_pool_map_operator.py:892` | 缩容冷却期 |
| `compute_downscale_delta()` | `1` | `actor_pool_resizing_policy.py:84-85` | 每次只缩 1 个 |
| `_ACTOR_POOL_GRACEFUL_SHUTDOWN_TIMEOUT_S` | `30` | `actor_pool_map_operator.py:893` | 优雅关闭超时 |

### 6.2 Actor 状态判定对照表

| Ray Data 状态 | 含义 | 对应代码 |
|---------------|------|----------|
| `Actors: N` (进度条) | `running + pending + restarting` | `_ActorPoolInfo` |
| `num_active_tasks()` | `len(self._data_tasks)` | `map_operator.py:725-734` |
| `has_execution_finished()` | `_inputs_complete && active_tasks==0 && internal_queue==0` | `physical_operator.py:413-434` |
| `has_completed()` | `has_execution_finished() && output_queue==0 && !has_next()` | `physical_operator.py:436-458` |

### 6.3 Actor 释放路径对照表

| 触发方式 | 是否调用 ray.kill() | 是否调用 on_exit() | 是否受 min_size 约束 |
|----------|---------------------|--------------------|-----------------------|
| Autoscaler 缩容 (利用率低) | 否 | 否 | 是 |
| Autoscaler 缩容 (consumed all inputs) | 否 | 否 | 否 |
| Operator shutdown (force=False) | 否 | 取决于 `_enable_actor_pool_on_exit_hook` | 不适用 |
| Operator shutdown (force=True) | 是 | 取决于 `_enable_actor_pool_on_exit_hook` | 不适用 |
| Driver 退出 | 自动 (OWNER_DIED) | 否 | 不适用 |
