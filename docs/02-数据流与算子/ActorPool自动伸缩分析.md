# Ray Data Actor Pool 自动伸缩与 Actor 退出机制分析

## 1. 问题背景

### 1.1 现象描述

在使用 Ray Data 的 `ActorPoolStrategy` 时，配置了 4 个 actor，但观察到：
- 只有 1 个 actor 在处理数据
- 其他 3 个 actor 在启动后约 3 秒就退出了

### 1.2 相关日志

```
UserWarning: The minimum number of concurrent actors for 'Map(DistributedStreamingVideoProcessMapper)'
is set to 4, but the operator only received 1 input(s). This means that the operator can launch at
most 1 task(s), and won't fully utilize the available concurrency.

[2026-04-16 14:53:19,511] actor_task_submitter.cc:75: Set actor max pending calls to -1 actor_id=e492d3aea0e55b976bdb7cb70a000000
[2026-04-16 14:53:19,511] core_worker.cc:2929: Creating actor actor_id=e492d3aea0e55b976bdb7cb70a000000
[2026-04-16 14:53:22,154] task_receiver.cc:117: Actor creation task finished, actor_id: e492d3aea0e55b976bdb7cb70a000000
[2026-04-16 14:53:25,935] core_worker_shutdown_executor.cc:123: Executing worker exit: INTENDED_SYSTEM_EXIT
    - Worker exits because the actor is killed. The actor is dead because all references to the actor were removed.
```

**关键时间线**：
- `14:53:19` - 创建 actor
- `14:53:22` - actor 创建完成
- `14:53:25` - actor 退出（仅 3 秒后）

## 2. Actor 退出原因分析

### 2.1 核心结论

Actor 退出是**预期行为**，原因是 **Ray Data 的 Autoscaler 检测到输入已经消费完毕，主动缩容回收空闲 actor**。

### 2.2 关键日志解读

| 日志信息 | 含义 |
|---------|------|
| `min_size=4, but only received 1 input(s)` | 配置了 4 个 actor，但实际只有 1 个输入 block |
| `all references to the actor were removed` | actor 引用被删除触发 GC 回收（而非 `ray.kill()`） |
| `INTENDED_SYSTEM_EXIT` | 系统主动退出，非异常 |

## 3. 自动伸缩机制详解

### 3.1 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│                   Streaming Executor                         │
│                                                              │
│  ┌──────────────────┐    ┌──────────────────────────────┐  │
│  │ Control Loop     │───▶│ _actor_autoscaler            │  │
│  │ (每次迭代)        │    │   .try_trigger_scaling()     │  │
│  └──────────────────┘    └──────────────────────────────┘  │
│                                    │                         │
│                                    ▼                         │
│                          ┌──────────────────┐               │
│                          │ _derive_target_  │               │
│                          │ scaling_config() │               │
│                          └────────┬─────────┘               │
│                                   │                         │
│            ┌──────────────────────┼──────────────────────┐  │
│            ▼                      ▼                      ▼  │
│     ┌──────────┐          ┌──────────┐           ┌──────────┐
│     │ Scale Up │          │  No Op   │           │Scale Down│
│     └──────────┘          └──────────┘           └──────────┘
└─────────────────────────────────────────────────────────────┘
```

### 3.2 触发伸缩的入口

**文件**: `python/ray/data/_internal/execution/streaming_executor.py:629-631`

```python
# Trigger autoscaling
self._cluster_autoscaler.try_trigger_scaling()
self._actor_autoscaler.try_trigger_scaling()
```

Streaming Executor 的控制循环在每次迭代时都会调用 autoscaler 的 `try_trigger_scaling()` 方法。

### 3.3 缩容判断逻辑

**文件**: `python/ray/data/_internal/actor_autoscaler/default_actor_autoscaler.py:98-111`

```python
def _derive_target_scaling_config(
    self,
    actor_pool: AutoscalingActorPool,
    op: "PhysicalOperator",
    op_state: "OpState",
) -> ActorPoolScalingRequest:
    # 关键判断：如果输入已经消费完毕，强制缩容
    if op.has_completed() or (
        op._inputs_complete and op_state.total_enqueued_input_blocks() == 0
    ):
        num_to_scale_down = self._compute_downscale_delta(actor_pool)
        return ActorPoolScalingRequest.downscale(
            delta=-num_to_scale_down, force=True, reason="consumed all inputs"
        )
```

**缩容触发条件**：
1. `op._inputs_complete = True` - 上游已经没有更多输入
2. `op_state.total_enqueued_input_blocks() == 0` - 输入队列为空

当这两个条件同时满足时，表示**没有更多的工作需要做了**，autoscaler 会强制缩容所有空闲 actor。

### 3.4 其他伸缩场景

| 场景 | 条件 | 动作 |
|------|------|------|
| Pool 低于最小值 | `current_size < min_size` | 扩容到 min_size |
| Pool 超过最大值 | `current_size > max_size` | 缩容到 max_size |
| 利用率过高 | `util >= upscaling_threshold` (默认 0.5) | 扩容 |
| 利用率过低 | `util <= downscaling_threshold` (默认 0.3) | 缩容 |
| 输入消费完毕 | `inputs_complete && enqueued_blocks == 0` | **强制缩容** |

## 4. Actor Pool 回收实现

### 4.1 回收方式：引用计数 GC

**文件**: `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py:1442-1480`

```python
def _release_running_actor(
    self, actor: ray.actor.ActorHandle
) -> Optional[ObjectRef]:
    """Remove the given actor from the pool and trigger its `on_exit` callback."""

    # NOTE: By default, we remove references to the actor and let ref counting
    # garbage collect the actor, instead of using ray.kill.
    #
    # Otherwise, actor cannot be reconstructed for the purposes of produced
    # object's lineage reconstruction.

    if actor not in self._running_actors:
        return None

    # ... 更新统计信息 ...

    # 关键：删除引用，让 Ray GC 回收 actor
    del self._running_actors[actor]
    del self._actor_to_logical_id[actor]

    return ref
```

**重要**：Ray Data 不使用 `ray.kill()` 强制杀死 actor，而是通过**删除 actor 引用**让 Ray 的引用计数 GC 机制自动回收。这就是为什么日志显示 `all references to the actor were removed`。

### 4.2 回收触发的三种场景

#### 场景 A：任务完成后的空闲回收

**文件**: `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py:1289-1304`

```python
def on_task_completed(self, actor: ray.actor.ActorHandle):
    """Called when a task completes. Returns the provided actor to the pool."""
    self._running_actors[actor].num_tasks_in_flight -= 1
    self._total_num_tasks_in_flight -= 1

    if not self._running_actors[actor].num_tasks_in_flight:
        self._num_active_actors -= 1
        # 如果有待处理的缩容请求，立即释放这个刚变空闲的 actor
        if self._pending_scale_down_count > 0:
            self._release_running_actor(actor)
            self._pending_scale_down_count -= 1
```

#### 场景 B：主动缩容

**文件**: `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py:1089-1101`

```python
def scale(self, req: ActorPoolScalingRequest) -> Optional[int]:
    # ...
    elif req.delta < 0:
        num_to_remove = abs(req.delta)
        num_released = 0

        # 尝试立即移除空闲的 actor
        for _ in range(num_to_remove):
            if self._remove_inactive_actor():
                num_released += 1

        # 如果还有忙碌的 actor 需要缩容，记录待处理数量
        num_deferred = num_to_remove - num_released
        self._pending_scale_down_count = num_deferred
```

#### 场景 C：Operator 完成后 shutdown

**文件**: `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py:1403-1409`

```python
def shutdown(self, force: bool = False):
    """Kills all actors, including running/active actors.
    This is called once the operator is shutting down.
    """
    self._release_pending_actors(force=force)
    self._release_running_actors(force=force)
```

### 4.3 移除空闲 Actor 的实现

**文件**: `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py:1370-1401`

```python
def _remove_inactive_actor(self) -> bool:
    """Kills a single pending or idle actor, if any actors are pending/idle."""
    # 优先移除 pending（还没创建完成的）actor，减少启动开销
    released = self._try_remove_pending_actor()
    if not released:
        # 如果没有 pending actor，则移除空闲的 running actor
        released = self._try_remove_idle_actor()
    return released

def _try_remove_idle_actor(self) -> bool:
    for actor, state in self._running_actors.items():
        if state.num_tasks_in_flight == 0:  # 空闲判断
            # NOTE: This is a fire-and-forget op
            self._release_running_actor(actor)
            return True
    return False
```

## 5. 问题场景的完整流程

### 5.1 时间线重建

```
时间                事件                                           说明
────────────────────────────────────────────────────────────────────────────
14:53:19           启动 4 个 actor                                因为 min_size=4
    │
14:53:22           所有 actor 创建完成                            进入 ALIVE 状态
    │
14:53:22+          只有 1 个输入 block                            只有 1 个 actor 被分配任务
    │                                                            其他 3 个空闲
    │
14:53:22+          Streaming Executor 调用                       检测伸缩需求
                   _actor_autoscaler.try_trigger_scaling()
    │
    │              autoscaler 检测到:
    │              - op._inputs_complete = True
    │              - total_enqueued_input_blocks() = 0
    │
    ▼              触发: ActorPoolScalingRequest.downscale(
                       force=True,
                       reason="consumed all inputs"
                   )
    │
    ▼              actor_pool.scale() 执行缩容
                   → _remove_inactive_actor() × 3
                   → 3 个空闲 actor 的引用被删除
    │
14:53:25           Ray GC 检测到引用计数为 0                      回收这 3 个 actor
                   日志: "all references to the actor were removed"
────────────────────────────────────────────────────────────────────────────
```

### 5.2 流程图

```
┌──────────────────┐
│ 用户配置         │
│ min_size=4       │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ 启动 4 个 Actor  │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐     ┌─────────────────────────┐
│ 只有 1 个输入    │────▶│ 只有 1 个 Actor 工作    │
│ block            │     │ 3 个 Actor 空闲         │
└────────┬─────────┘     └─────────────────────────┘
         │
         ▼
┌──────────────────────────────────────┐
│ Autoscaler 检测到:                   │
│ - inputs_complete = True             │
│ - enqueued_blocks = 0                │
└────────┬─────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────┐
│ 触发强制缩容                         │
│ reason="consumed all inputs"         │
└────────┬─────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────┐
│ 删除 3 个空闲 Actor 的引用           │
│ del self._running_actors[actor]      │
└────────┬─────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────┐
│ Ray GC 回收 Actor                    │
│ "all references were removed"        │
└──────────────────────────────────────┘
```

## 6. 如何确认这个原因

### 6.1 方法一：开启 Debug 日志（最直接）

```python
import logging
logging.getLogger("ray.data").setLevel(logging.DEBUG)
```

或设置环境变量：
```bash
export RAY_DATA_LOG_LEVEL=DEBUG
```

**预期看到的日志**：

```
Scaled down actor pool by 3 (reason=consumed all inputs; running=1, restarting=0, pending=0)
```

`reason=consumed all inputs` 就是确认缩容是因为输入已经消费完毕。

### 6.2 方法二：查看 Ray Dashboard 指标

在 Ray Dashboard 的 Data 页面查看：

| 指标 | 说明 | 预期值 |
|------|------|--------|
| Queued blocks | 队列中等待处理的 block | **0**（缩容时） |
| ActorPool current_size | 当前 actor 数量 | **1**（缩容后） |
| num_inputs_received | 收到的输入 block 数 | **1** |

### 6.3 方法三：程序化验证

```python
import ray
from ray.data import ActorPoolStrategy

# 在执行前后检查 metrics
ds = ray.data.read_xxx(...)
ds = ds.map_batches(fn, compute=ActorPoolStrategy(min_size=4))

# 迭代消费数据
for batch in ds.iter_batches():
    pass

# 查看 stats
print(ds.stats())  # 会显示 num_inputs_received 等指标
```

### 6.4 方法四：从现有日志确认

你的日志中已经有确认信息：

| 日志信息 | 说明 |
|---------|------|
| `min_size=4, but only received 1 input(s)` | 只有 1 个输入 block |
| `all references to the actor were removed` | actor 是通过引用删除方式回收的 |
| actor 在创建后 ~3s 退出 | 符合"处理完输入后立即缩容"的时间线 |

## 7. 解决方案

### 7.1 如果希望保留所有 4 个 Actor

#### 方案 A：增加输入 block 数量

```python
# 在上游增加 block 数量，让每个 actor 都有工作
ds = ds.repartition(4)

# 或使用 override_num_blocks
ds = ray.data.read_parquet(path, override_num_blocks=4)
```

#### 方案 B：使用固定大小的 Actor Pool

```python
# 使用 size 参数，而不是 min_size/max_size
ds.map_batches(fn, compute=ActorPoolStrategy(size=4))
```

**注意**：`ActorPoolStrategy(min_size=4, max_size=4)` 虽然看起来是固定大小，但仍会触发 `consumed all inputs` 的强制缩容逻辑。

### 7.2 如果数据量确实很小

如果实际只有 1 个输入 block，那么 3 个 actor 快速退出是**正常且预期的行为**，是 Ray Data 的资源优化策略：

- 只需要 1 个 actor 处理 1 个 block
- 其他 3 个 actor 属于"过度配置"
- 快速回收可以释放资源给其他任务使用

### 7.3 检查数据源

建议检查数据源为什么只产生了 1 个输入 block：

```python
# 查看数据源的 block 数量
ds = ray.data.read_xxx(...)
print(f"Number of blocks: {ds.num_blocks()}")
```

## 9. Actor 扩容后资源增加 memory 的根因分析

### 9.1 现象描述

用户在 `map_batches` 中只指定了 `num_cpus` 和 `num_gpus`，未指定 `memory`：

```python
ds.map_batches(
    fn,
    compute=ActorPoolStrategy(min_size=1, max_size=4),
    num_cpus=1,
    num_gpus=1,
    # 未指定 memory
)
```

观察到：
- **初始创建的 actor** 只有 cpu 和 gpu 资源，没有 memory
- **扩容后创建的新 actor** 多了 memory 资源

### 9.2 根因：`ConfigureMapTaskMemoryUsingOutputSize` 优化规则

memory 不是扩容逻辑添加的，而是 Ray Data 在**物理计划优化阶段**通过 `ConfigureMapTaskMemoryUsingOutputSize` 规则根据输出大小自动估算并注入的。

#### 9.2.1 优化规则注册

**文件**: `python/ray/data/_internal/logical/optimizers.py:38-44`

```python
_PHYSICAL_RULESET = Ruleset([
    InheritTargetMaxBlockSizeRule,
    SetReadParallelismRule,
    FuseOperators,
    ConfigureMapTaskMemoryUsingOutputSize,  # ← 物理优化阶段注入 memory
])
```

#### 9.2.2 规则实现

**文件**: `python/ray/data/_internal/logical/rules/configure_map_task_memory.py:16-57`

```python
class ConfigureMapTaskMemoryRule(Rule, abc.ABC):
    def apply(self, plan: PhysicalPlan) -> PhysicalPlan:
        for op in plan.dag.post_order_iter():
            if not isinstance(op, MapOperator):
                continue

            # 包装一个新的 ray_remote_args_fn
            def ray_remote_args_fn(
                op: MapOperator = op,
                original_ray_remote_args_fn=op._ray_remote_args_fn  # 捕获原始的(通常为None)
            ) -> Dict[str, Any]:
                assert isinstance(op, MapOperator), op

                static_ray_remote_args = copy.deepcopy(op._ray_remote_args)

                dynamic_ray_remote_args = {}
                if original_ray_remote_args_fn is not None:
                    dynamic_ray_remote_args = original_ray_remote_args_fn()

                # 条件：用户未指定 memory + 未使用 PlacementGroup 调度
                if (
                    "memory" not in static_ray_remote_args
                    and "memory" not in dynamic_ray_remote_args
                    and not any(
                        isinstance(
                            scheduling_strategy, PlacementGroupSchedulingStrategy
                        )
                        for scheduling_strategy in (
                            static_ray_remote_args.get("scheduling_strategy"),
                            dynamic_ray_remote_args.get("scheduling_strategy"),
                            op.data_context.scheduling_strategy,
                            op.data_context.scheduling_strategy_large_args,
                        )
                    )
                ):
                    # 估算 memory
                    memory = self.estimate_per_task_memory_requirement(op)
                    if memory is not None:           # ← 关键判断：None 时不注入
                        dynamic_ray_remote_args["memory"] = memory

                return dynamic_ray_remote_args

            # 覆盖 op 的 ray_remote_args_fn
            op._ray_remote_args_fn = ray_remote_args_fn   # ← 设置！

        return plan
```

#### 9.2.3 memory 估算逻辑

**文件**: `python/ray/data/_internal/logical/rules/configure_map_task_memory.py:70-87`

```python
class ConfigureMapTaskMemoryUsingOutputSize(ConfigureMapTaskMemoryRule):
    def estimate_per_task_memory_requirement(self, op: MapOperator) -> Optional[int]:
        # Typically, this configuration won't make a difference because
        # `average_bytes_per_output` is usually ~128 MiB and each core usually has
        # 4 GiB of memory. However, if `num_cpus` is small (e.g., 0.01) or
        # `target_max_block_size` is large (e.g., 1GB), then tasks can OOM even
        # if it just uses enough memory to produce an output block. By setting
        # `memory` to the average output size, we can mitigate this case.
        #
        # We set it to 1 target block size out of assumption that *at least* 1 copy
        # of data (to process heap) will be made during processing.
        #
        # Note that, unless object store memory is manually specified, by default Ray's
        # "memory" resource is exclusive of the Object Store memory allocated on the
        # node (i.e., its total allocatable value is Total memory - Object Store
        # memory).
        return op.metrics.average_bytes_per_output
```

#### 9.2.4 `average_bytes_per_output` 的运行时依赖

**文件**: `python/ray/data/_internal/execution/interfaces/op_runtime_metrics.py:703-708`

```python
@metric_property(
    description="Average size of task output in bytes.",
    metrics_group=MetricsGroup.OUTPUTS,
)
def average_bytes_per_output(self) -> Optional[float]:
    """Average size in bytes of output blocks."""
    if self.num_task_outputs_generated == 0:
        return None          # ← 初始时返回 None
    else:
        return self.bytes_task_outputs_generated / self.num_task_outputs_generated
```

**关键**：`average_bytes_per_output` 是运行时指标，在尚未执行任何 task 时返回 `None`，有了输出统计后才返回有效值。

### 9.3 `ray_remote_args_fn` 的完整设置链路

#### 第1步：用户 API 调用

**文件**: `python/ray/data/dataset.py:557` — 用户调用 `ds.map_batches(fn, num_cpus=1, num_gpus=1, ...)`

```python
# dataset.py:881-893
ray_remote_args["num_cpus"] = num_cpus      # 1
ray_remote_args["num_gpus"] = num_gpus      # 1
# 用户没传 memory，所以 ray_remote_args 中没有 "memory" 键

map_batches_op = MapBatches(
    self._logical_plan.dag,
    fn,
    ...,
    ray_remote_args_fn=ray_remote_args_fn,  # 用户没传 → None
    ray_remote_args=ray_remote_args,        # {num_cpus:1, num_gpus:1}
    ...,
)
```

此时 logical operator `MapBatches` 持有：
- `self.ray_remote_args_fn = None`（用户未指定）
- `self.ray_remote_args = {"num_cpus": 1, "num_gpus": 1}`

**文件**: `python/ray/data/_internal/logical/operators/map_operator.py:47-83`

```python
class AbstractMap(AbstractOneToOne):
    def __init__(
        self,
        ...,
        ray_remote_args: Optional[Dict[str, Any]] = None,
        ray_remote_args_fn: Optional[Callable[[], Dict[str, Any]]] = None,
        ...,
    ):
        ...
        self.ray_remote_args = ray_remote_args or {}
        self.ray_remote_args_fn = ray_remote_args_fn   # ← 存储用户的 fn（通常 None）
        ...
```

#### 第2步：生成执行计划（3个子步骤）

**文件**: `python/ray/data/_internal/logical/optimizers.py:76-96`

```python
def get_execution_plan(logical_plan: LogicalPlan) -> Tuple[PhysicalPlan, ...]:
    # (1) Logical 优化
    optimized_logical_plan = LogicalOptimizer().optimize(logical_plan)

    # (2) Logical → Physical（planning）
    physical_plan, callbacks = create_planner().plan(optimized_logical_plan)

    # (3) Physical 优化 ← 关键：这里注入 ray_remote_args_fn
    return PhysicalOptimizer().optimize(physical_plan), callbacks
```

##### 第2.1步：Logical → Physical 转换

**文件**: `python/ray/data/_internal/planner/plan_udf_map_op.py:178`

将 logical `MapBatches` 转为 physical `ActorPoolMapOperator`，传递 `ray_remote_args_fn`（此时仍为 `None`）：

```python
return MapOperator.create(
    map_transformer,
    input_physical_dag,
    data_context,
    name=op.name,
    compute_strategy=compute,
    ray_remote_args=op.ray_remote_args,         # {num_cpus:1, num_gpus:1}
    ray_remote_args_fn=op.ray_remote_args_fn,   # None
)
```

**文件**: `python/ray/data/_internal/execution/operators/map_operator.py:337-447`

`MapOperator.create()` 根据 compute strategy 分发：

```python
@classmethod
def create(cls, ..., ray_remote_args_fn=None, ray_remote_args=None, ...):
    ...
    if isinstance(compute_strategy, ActorPoolStrategy):
        return ActorPoolMapOperator(
            map_transformer,
            input_op,
            data_context,
            ...,
            ray_remote_args_fn=ray_remote_args_fn,  # None
            ray_remote_args=ray_remote_args,        # {num_cpus:1, num_gpus:1}
            ...,
        )
```

**文件**: `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py:165`

`ActorPoolMapOperator.__init__` 存储到实例变量：

```python
self._ray_remote_args_fn = ray_remote_args_fn  # None
self._ray_remote_args = self._apply_default_remote_args(
    self._ray_remote_args, self.data_context
)
```

##### 第2.2步：Physical 优化 — 注入 `ray_remote_args_fn`

`PhysicalOptimizer().optimize(physical_plan)` 执行 `ConfigureMapTaskMemoryUsingOutputSize` 规则（见 9.2.2），将 `op._ray_remote_args_fn` 从 `None` **覆盖为包装函数**。

包装函数内部逻辑：
1. 复制静态 `ray_remote_args`（`{num_cpus:1, num_gpus:1}`）
2. 调用原始 `ray_remote_args_fn`（如果有的话）
3. 检查条件：用户未指定 memory + 未使用 PlacementGroup
4. 调用 `estimate_per_task_memory_requirement(op)` → `op.metrics.average_bytes_per_output`
5. 如果返回值非 `None`，则注入 `dynamic_ray_remote_args["memory"] = memory`

**至此 `op._ray_remote_args_fn` 从 `None` 变成了一个包装函数。**

#### 第3步：执行 — Actor 创建

##### 初始启动

**文件**: `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py:290-296`

```python
def start(self, options: ExecutionOptions):
    ...
    # 初始 actor_cls 使用静态 ray_remote_args 创建（无 memory）
    self._actor_cls = ray.remote(**self._ray_remote_args)(self._map_worker_cls)
    # self._ray_remote_args = {num_cpus:1, num_gpus:1, max_restarts:-1, ...} ← 无 memory

    self._actor_pool.scale(ActorPoolScalingRequest(
        delta=self._actor_pool.initial_size(), reason="scaling to initial size"
    ))
```

##### `_start_actor` — 每次 actor 创建时

**文件**: `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py:316-340`

```python
def _start_actor(self, labels, logical_actor_id):
    assert self._actor_cls is not None
    if self._ray_remote_args_fn:          # ← 此时已非None（被优化器设置了）
        self._refresh_actor_cls()         # ← 调用动态函数获取memory
    actor = self._actor_cls.options(
        _labels={self._OPERATOR_ID_LABEL_KEY: self.id, **labels}
    ).remote(...)
    ...
```

##### `_refresh_actor_cls` — 动态合并参数

**文件**: `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py:342-356`

```python
def _refresh_actor_cls(self):
    """当 ray_remote_args_fn 被指定时，在初始化新 worker 之前调用此方法，
    获取新的 remote args 传给 worker。用新的 remote args 更新 self._actor_cls。"""
    assert self._ray_remote_args_fn, "_ray_remote_args_fn must be provided"
    remote_args = self._ray_remote_args.copy()      # {num_cpus:1, num_gpus:1, ...}
    new_remote_args = self._ray_remote_args_fn()     # ← 调用优化器包装的函数

    # 用动态返回的 args 覆盖静态 args
    for k, v in new_remote_args.items():
        remote_args[k] = v                            # 合并：memory 被注入
    self._actor_cls = ray.remote(**remote_args)(self._map_worker_cls)
```

**这里调用的就是第2.2步注入的那个 `ray_remote_args_fn` 函数。**

##### `ray_remote_args_fn()` 执行时

```python
def ray_remote_args_fn(op=op, original_ray_remote_args_fn=None):
    # ...条件检查通过...
    memory = self.estimate_per_task_memory_requirement(op)
    # = op.metrics.average_bytes_per_output
    if memory is not None:
        dynamic_ray_remote_args["memory"] = memory
    return dynamic_ray_remote_args
```

### 9.4 完整时序图

```
用户调用 map_batches(num_cpus=1, num_gpus=1)
  │  ray_remote_args_fn = None
  │  ray_remote_args = {cpu:1, gpu:1}
  ▼
get_execution_plan()
  │
  ├─ (1) Logical 优化
  │
  ├─ (2) Logical → Physical (planning)
  │     └─ ActorPoolMapOperator 创建
  │        _ray_remote_args_fn = None
  │        _ray_remote_args = {cpu:1, gpu:1, max_restarts:-1, ...}
  │
  └─ (3) Physical 优化 ← ConfigureMapTaskMemoryUsingOutputSize 规则
        └─ op._ray_remote_args_fn = 包装函数（内含 memory 估算逻辑）
           包装函数逻辑:
             - 检查 "memory" not in static_args → True
             - 调用 estimate_per_task_memory_requirement(op)
             - = op.metrics.average_bytes_per_output
             - 初始时 num_task_outputs_generated == 0 → 返回 None
             - memory is None → 不注入
  ▼
start() 初始 actor 创建
  │  _refresh_actor_cls() → ray_remote_args_fn()
  │  → average_bytes_per_output → None (num_task_outputs_generated=0)
  │  → 不注入 memory
  │  → actor_cls = ray.remote(cpu:1, gpu:1)(MapWorker)
  │  → 初始 actor 只有 {cpu:1, gpu:1}  ✅ 无 memory
  ▼
Task 执行，产生输出
  │  num_task_outputs_generated > 0
  │  average_bytes_per_output → 有效值 (如 128MB)
  ▼
Autoscaler 触发扩容 → _start_actor()
  │  _refresh_actor_cls() → ray_remote_args_fn()
  │  → average_bytes_per_output → 有效值 (如 128MB)
  │  → dynamic_ray_remote_args["memory"] = 128MB
  │  → remote_args = {cpu:1, gpu:1, memory:128MB, ...}
  │  → actor_cls = ray.remote(cpu:1, gpu:1, memory:128MB)(MapWorker)
  │  → 新 actor 带 memory 资源  ✅ 有 memory
```

### 9.5 关键代码文件索引

| 文件路径 | 关键内容 |
|---------|---------|
| `python/ray/data/dataset.py:557` | 用户 API `map_batches()`，接收 `ray_remote_args_fn` 参数 |
| `python/ray/data/_internal/logical/operators/map_operator.py:47` | `AbstractMap.__init__` 存储 `ray_remote_args_fn` |
| `python/ray/data/_internal/planner/plan_udf_map_op.py:178` | Logical → Physical 转换，传递 `ray_remote_args_fn` |
| `python/ray/data/_internal/execution/operators/map_operator.py:337` | `MapOperator.create()` 工厂方法，分发到 ActorPool/TaskPool |
| `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py:165` | `ActorPoolMapOperator.__init__` 存储 `_ray_remote_args_fn` |
| `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py:290` | `start()` 初始 actor 创建 |
| `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py:316` | `_start_actor()` 每次创建 actor 的入口 |
| `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py:342` | `_refresh_actor_cls()` 动态合并 remote args |
| `python/ray/data/_internal/logical/optimizers.py:38` | Physical 规则集注册 `ConfigureMapTaskMemoryUsingOutputSize` |
| `python/ray/data/_internal/logical/rules/configure_map_task_memory.py:16` | 优化规则实现，包装 `ray_remote_args_fn` |
| `python/ray/data/_internal/execution/interfaces/op_runtime_metrics.py:703` | `average_bytes_per_output` 运行时指标 |

### 9.6 关键方法索引

| 方法 | 文件:行号 | 作用 |
|------|----------|------|
| `map_batches()` | `dataset.py:557` | 用户 API，接收 `ray_remote_args_fn` |
| `get_execution_plan()` | `optimizers.py:76` | 执行计划生成入口（3步：逻辑优化→规划→物理优化） |
| `ConfigureMapTaskMemoryRule.apply()` | `configure_map_task_memory.py:16` | 物理优化规则，注入 `ray_remote_args_fn` |
| `estimate_per_task_memory_requirement()` | `configure_map_task_memory.py:86` | 估算 memory（返回 `average_bytes_per_output`） |
| `average_bytes_per_output` | `op_runtime_metrics.py:703` | 运行时指标，无输出时返回 None |
| `start()` | `actor_pool_map_operator.py:290` | 初始 actor 创建 |
| `_start_actor()` | `actor_pool_map_operator.py:316` | 每次创建 actor 的入口，检查 `_ray_remote_args_fn` |
| `_refresh_actor_cls()` | `actor_pool_map_operator.py:342` | 调用 `ray_remote_args_fn()` 动态合并参数 |

### 9.7 总结

1. **memory 来源**：`ConfigureMapTaskMemoryUsingOutputSize` 物理优化规则在执行计划生成阶段自动注入，而非用户手动指定或扩容逻辑添加
2. **注入机制**：规则包装 `op._ray_remote_args_fn`，在每次创建 actor 时动态调用，根据运行时指标 `average_bytes_per_output` 估算 memory
3. **初始 actor 无 memory 的原因**：`average_bytes_per_output` 是运行时指标，初始时 `num_task_outputs_generated == 0` 返回 `None`，规则跳过注入
4. **扩容 actor 有 memory 的原因**：task 执行产生输出后，`average_bytes_per_output` 变为有效值，扩容时 `_refresh_actor_cls()` 调用 `ray_remote_args_fn()` 拿到有效 memory 并注入
5. **设计意图**：防止低 `num_cpus`（如 0.01）或大 `target_max_block_size`（如 1GB）场景下 task OOM，按输出块大小估算 memory 需求

## 10. 相关代码文件索引

| 文件路径 | 关键内容 |
|---------|---------|
| `python/ray/data/_internal/actor_autoscaler/default_actor_autoscaler.py` | Autoscaler 伸缩决策逻辑 |
| `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py` | Actor Pool 管理、缩容执行 |
| `python/ray/data/_internal/execution/streaming_executor.py` | Streaming Executor 控制循环 |
| `python/ray/data/_internal/execution/streaming_executor_state.py` | OpState、输入队列状态 |

### 10.1 关键方法索引

| 方法 | 文件:行号 | 作用 |
|------|----------|------|
| `try_trigger_scaling()` | `streaming_executor.py:631` | 触发伸缩检查 |
| `_derive_target_scaling_config()` | `default_actor_autoscaler.py:98` | 计算伸缩决策 |
| `scale()` | `actor_pool_map_operator.py:1050` | 执行伸缩操作 |
| `_remove_inactive_actor()` | `actor_pool_map_operator.py:1370` | 移除空闲 actor |
| `_release_running_actor()` | `actor_pool_map_operator.py:1442` | 释放 actor 引用 |
| `on_task_completed()` | `actor_pool_map_operator.py:1289` | 任务完成回调 |

## 11. 总结

1. **这不是 Bug**：Actor 快速退出是 Ray Data Autoscaler 的预期行为
2. **触发条件**：输入消费完毕（`inputs_complete=True` 且 `enqueued_blocks=0`）
3. **回收方式**：通过删除引用触发 Ray GC，而非 `ray.kill()`
4. **确认方法**：开启 DEBUG 日志查看 `reason=consumed all inputs`
5. **解决方案**：增加输入 block 数量，或使用 `ActorPoolStrategy(size=N)`
6. **memory 自动注入**：扩容后 actor 多出 memory 资源是 `ConfigureMapTaskMemoryUsingOutputSize` 优化规则根据运行时输出统计自动估算注入的，初始 actor 因无输出数据而不带 memory

## 12. Actor 多 Task 并发处理机制：max_tasks_in_flight_per_actor / max_concurrency / enable_true_multi_threading

### 12.1 问题背景

用户希望让一个 actor 同时处理两个 task。Ray Data 提供了三个相关参数来控制 actor 的并发能力：

| 参数 | 作用层 | 定义位置 |
|------|--------|----------|
| `max_tasks_in_flight_per_actor` | Ray Data 调度层 | `ActorPoolStrategy.__init__` (`compute.py:116`) |
| `max_concurrency` | Ray Core actor 层 | `ray.remote(max_concurrency=N)` |
| `enable_true_multi_threading` | UDF 包装层 | `ActorPoolStrategy.__init__` (`compute.py:117`) |

三者作用在不同层级，可以组合使用。

### 12.2 max_tasks_in_flight_per_actor：调度层流水线与数据预取延迟隐藏

#### 12.2.1 参数初始化与默认值

**文件**: `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py:168-186`

```python
max_actor_concurrency = self._ray_remote_args.get("max_concurrency", 1)

self._actor_pool = _ActorPool(
    ...,
    max_actor_concurrency=max_actor_concurrency,
    max_tasks_in_flight_per_actor=(
        compute_strategy.max_tasks_in_flight_per_actor
        or data_context.max_tasks_in_flight_per_actor
        or max_actor_concurrency
        * DEFAULT_ACTOR_MAX_TASKS_IN_FLIGHT_TO_MAX_CONCURRENCY_FACTOR
    ),
    ...,
)
```

**默认值链**：用户未指定 → `data_context` 未指定 → `max_concurrency * 2`

`DEFAULT_ACTOR_MAX_TASKS_IN_FLIGHT_TO_MAX_CONCURRENCY_FACTOR` 定义在 `python/ray/data/context.py:253`，默认为 2：

```python
DEFAULT_ACTOR_MAX_TASKS_IN_FLIGHT_TO_MAX_CONCURRENCY_FACTOR = env_integer(
    "RAY_DATA_ACTOR_DEFAULT_MAX_TASKS_IN_FLIGHT_TO_MAX_CONCURRENCY_FACTOR", 2
)
```

即默认 `max_tasks_in_flight = 2 * max_concurrency`。当 `max_concurrency=1` 时，默认 `max_tasks_in_flight=2`。

#### 12.2.2 可调度 Actor 判断

**文件**: `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py:1196-1201`

```python
def schedulable_actors(self) -> List[ray.actor.ActorHandle]:
    available_actors = self.get_available_actors()
    return [
        actor
        for actor, state in available_actors.items()
        if state.num_tasks_in_flight < self.max_tasks_in_flight_per_actor()
        and not state.is_restarting
    ]
```

只要 actor 的 `num_tasks_in_flight < max_tasks_in_flight_per_actor`，就还能接收新 task。这是控制并发的核心门控。

#### 12.2.3 任务提交流程

**文件**: `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py:370-396`

```python
def _try_schedule_tasks_internal(self, strict: bool) -> int:
    num_submitted_tasks = 0
    for bundle, actor in self._actor_task_selector.select_actors(
        self._bundle_queue, self._actor_locality_enabled, strict=strict,
    ):
        self._metrics.on_input_dequeued(bundle)
        input_blocks = [block for block, _ in bundle.blocks]
        self._actor_pool.on_task_submitted(actor)  # num_tasks_in_flight += 1

        ctx = TaskContext(task_idx=self._next_data_task_idx, ...)
        gen = actor.submit.options(
            num_returns="streaming",
            **self._ray_actor_task_remote_args,
        ).remote(
            self.data_context, ctx, *input_blocks,
            slices=bundle.slices, **self.get_map_task_kwargs(),
        )

        def _task_done_callback(actor_to_return):
            self._actor_pool.on_task_completed(actor_to_return)

        self._submit_data_task(gen, bundle, partial(_task_done_callback, actor_to_return=actor))
        num_submitted_tasks += 1
    return num_submitted_tasks
```

**关键点**：
- `actor.submit.options(num_returns="streaming").remote(...)` 是异步调用，提交后立即返回，不等执行完成
- `on_task_submitted(actor)` 立即将 `num_tasks_in_flight += 1`（`actor_pool_map_operator.py:1204-1205`）
- 回调 `_task_done_callback` 在 task 完成时调用 `on_task_completed` 将计数减回

#### 12.2.4 任务完成回调

**文件**: `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py:1293-1312`

```python
def on_task_completed(self, actor: ray.actor.ActorHandle):
    if actor not in self._running_actors:
        return
    assert self._running_actors[actor].num_tasks_in_flight > 0
    self._running_actors[actor].num_tasks_in_flight -= 1
    self._total_num_tasks_in_flight -= 1
    if not self._running_actors[actor].num_tasks_in_flight:
        self._num_active_actors -= 1
        if self._pending_scale_down_count > 0:
            self._release_running_actor(actor)
            self._pending_scale_down_count -= 1
```

#### 12.2.5 "数据预取延迟隐藏"原理

当一个 actor 正在执行 task A 的 UDF 时，如果 `max_tasks_in_flight_per_actor=1`，必须等 task A 完全完成才能提交 task B。task B 的输入数据（block）只能在 task A 完成后才开始从 Object Store 传输到 actor 进程，存在一个"串行等待"周期：

```
无流水线 (max_tasks_in_flight=1):
  |--fetch A--||--UDF A--|          |--fetch B--||--UDF B--|
  ↑ A完成才能开始fetch B              ↑ B的数据传输必须等A完全结束

有流水线 (max_tasks_in_flight=2):
  |--fetch A--||--UDF A--|
               |--fetch B--||--UDF B--|
               ↑ A执行UDF时，B已经开始fetch数据
```

**Ray Core 的 actor task 调度机制**：当 `actor.submit.remote()` 被调用后，Ray 运行时会在 actor 所在节点上异步地将输入 ObjectRef 解引用（从 Object Store 拉取 block 到 actor 进程内存）。这个拉取过程与 actor 正在执行的其他 task 是并行的。

因此，`max_tasks_in_flight_per_actor=2` 允许 actor 在执行 task A 的同时，提前提交 task B，让 Ray 运行时开始为 task B 预取数据。当 task A 完成时，task B 的数据可能已经到位，无需等待。

**这是典型的流水线优化**：将"数据传输"与"计算执行"重叠，消除数据传输的等待空闲期。

### 12.3 max_concurrency：从 Python API 到 C++ 线程池的完整链路

`max_concurrency` 不是 Ray Data 的参数，而是 Ray Core 的 actor 级参数。它通过 `**ray_remote_args` 传入 `map_batches`，最终传到 `ray.remote(max_concurrency=N)(cls)`，控制 actor 进程内部的线程池大小。

#### 12.3.1 参数传递链：Python API → ray.remote

**用户 API 入口** (`python/ray/data/dataset.py:770`)

```python
def _map_batches_impl(
    ...,
    num_cpus: Optional[float],
    num_gpus: Optional[float],
    **ray_remote_args,   # ← max_concurrency 通过 **kwargs 传入
):
    compute = get_compute_strategy(fn, compute=compute, concurrency=concurrency)
    if num_cpus is not None:
        ray_remote_args["num_cpus"] = num_cpus
    if num_gpus is not None:
        ray_remote_args["num_gpus"] = num_gpus
    # max_concurrency 留在 ray_remote_args 字典中

    map_batches_op = MapBatches(
        ...,
        ray_remote_args=ray_remote_args,  # ← 传给 logical operator
    )
```

**ActorPoolMapOperator 初始化** (`python/ray/data/_internal/execution/operators/actor_pool_map_operator.py:155-186`)

```python
# _ray_remote_args 包含 max_concurrency
self._ray_remote_args = self._apply_default_remote_args(
    self._ray_remote_args, self.data_context
)

max_actor_concurrency = self._ray_remote_args.get("max_concurrency", 1)

self._actor_pool = _ActorPool(
    ...,
    max_actor_concurrency=max_actor_concurrency,    # ← 用于资源计算
    max_tasks_in_flight_per_actor=(
        compute_strategy.max_tasks_in_flight_per_actor
        or data_context.max_tasks_in_flight_per_actor
        or max_actor_concurrency
        * DEFAULT_ACTOR_MAX_TASKS_IN_FLIGHT_TO_MAX_CONCURRENCY_FACTOR
    ),
    ...,
)
```

**Actor 创建** (`python/ray/data/_internal/execution/operators/actor_pool_map_operator.py:253`)

```python
def start(self, options: ExecutionOptions):
    ...
    # max_concurrency 在 self._ray_remote_args 中，直接传给 ray.remote()
    self._actor_cls = ray.remote(**self._ray_remote_args)(self._map_worker_cls)
    #                       ↑ 展开为 ray.remote(num_cpus=1, max_concurrency=2, ...)(_MapWorker)
```

#### 12.3.2 Ray Core Python 层：默认值与校验

**默认值** (`python/ray/_private/ray_constants.py:501`)

```python
# Default max_concurrency option in @ray.remote for threaded actors.
DEFAULT_MAX_CONCURRENCY_THREADED = 1
# asyncio actor 默认是 1000
DEFAULT_MAX_CONCURRENCY_ASYNC = 1000
```

**默认值应用** (`python/ray/actor.py:1556-1562`)

```python
is_asyncio = has_async_methods(meta.modified_class)

if actor_options.get("max_concurrency") is None:
    actor_options["max_concurrency"] = (
        DEFAULT_MAX_CONCURRENCY_ASYNC
        if is_asyncio
        else ray_constants.DEFAULT_MAX_CONCURRENCY_THREADED
    )
```

**乱序执行校验** (`python/ray/actor.py:1790-1808`)

```python
allow_out_of_order_execution = actor_options.get("allow_out_of_order_execution")

# max_concurrency > 1 时强制启用乱序执行
if allow_out_of_order_execution is None:
    allow_out_of_order_execution = is_asyncio or max_concurrency > 1

if max_concurrency > 1 and not allow_out_of_order_execution:
    raise ValueError(
        "If you're using multi-threaded actors, Ray can't execute actor tasks "
        "in order. Set `allow_out_of_order_execution=True` to allow "
        "out-of-order execution."
    )
```

**关键**：`max_concurrency > 1` 会自动启用 `allow_out_of_order_execution=True`，后续影响 task 队列类型选择。

#### 12.3.3 Cython 层：传递给 C++ CoreWorker

**Cython 绑定** (`python/ray/_raylet.pyx:3565-3645`)

```python
def create_actor(self, ..., int32_t max_concurrency, ...):
    ...
    with nogil:
        status = CCoreWorkerProcess.GetCoreWorker().CreateActor(
            ...,
            CActorCreationOptions(
                ..., max_concurrency,    # ← 传入 C++ 结构体
                ..., is_asyncio,
                ..., allow_out_of_order_execution,
                ...
            ))
```

**C++ 结构体** (`src/ray/core_worker/common.h:119-180`)

```cpp
struct ActorCreationOptions {
    ActorCreationOptions(int64_t max_restarts_p,
                         int64_t max_task_retries_p,
                         int max_concurrency_p, ...)
        : ...,
          max_concurrency(max_concurrency_p), ... {}

    /// The max number of concurrent tasks to run on this direct call actor.
    const int max_concurrency = 1;
    ...
};
```

**序列化到 protobuf** (`src/ray/common/task/task_util.h:248-295`)

```cpp
TaskSpecBuilder &SetActorCreationTaskSpec(..., int max_concurrency = 1, ...) {
    ...
    actor_creation_spec->set_max_concurrency(max_concurrency);
    ...
}
```

#### 12.3.4 C++ CoreWorker：Actor 创建时创建线程池

**Actor worker 接收创建任务** (`src/ray/core_worker/task_execution/task_receiver.cc:97-106`)

```cpp
// 当 actor creation task 到达 worker 进程时
if (task_spec.IsActorCreationTask()) {
    concurrency_groups_ = task_spec.ConcurrencyGroups();
    if (is_asyncio_) {
        // asyncio actor: 使用 FiberState (boost fibers, 协程而非 OS 线程)
        fiber_state_manager_ = std::make_shared<ConcurrencyGroupManager<FiberState>>(
            concurrency_groups_, fiber_max_concurrency_, initialize_thread_callback_);
    } else {
        // 普通线程 actor: 使用 BoundedExecutor (OS 线程池)
        const int default_max_concurrency = task_spec.MaxActorConcurrency();
        pool_manager_ = std::make_shared<ConcurrencyGroupManager<BoundedExecutor>>(
            concurrency_groups_, default_max_concurrency, initialize_thread_callback_);
    }
}
```

**SetupActor 存储 max_concurrency** (`src/ray/core_worker/task_execution/task_receiver.cc:208-287`)

```cpp
void TaskReceiver::SetupActor(bool is_asyncio,
                              int fiber_max_concurrency,
                              bool allow_out_of_order_execution) {
    is_asyncio_ = is_asyncio;
    fiber_max_concurrency_ = fiber_max_concurrency;   // ← 存储
    allow_out_of_order_execution_ = allow_out_of_order_execution;
}
```

#### 12.3.5 BoundedExecutor：实际的 OS 线程池

**线程池创建** (`src/ray/core_worker/task_execution/thread_pool.cc:26-67`)

```cpp
BoundedExecutor::BoundedExecutor(
    int max_concurrency,
    std::function<std::function<void()>()> initialize_thread_callback,
    boost::chrono::milliseconds timeout_ms)
    : work_guard_(boost::asio::make_work_guard(io_context_)),
      initialize_thread_callback_(initialize_thread_callback) {

    RAY_CHECK(max_concurrency > 0) << "max_concurrency must be greater than 0";

    boost::latch init_latch(max_concurrency);

    threads_.reserve(max_concurrency);
    for (int i = 0; i < max_concurrency; i++) {
        // 创建 max_concurrency 个 OS 线程
        threads_.emplace_back([this, &init_latch]() {
            // 每个线程启动时获取 Python GIL 状态
            std::function<void()> releaser = InitializeThread();
            init_latch.count_down();
            // 阻塞在这里，等待并处理 io_context 中的任务
            io_context_.run();
            if (releaser) { releaser(); }
        });
    }
    auto status = init_latch.wait_for(timeout_ms);
    ...
}

// 提交任务到线程池
void BoundedExecutor::Post(std::function<void()> fn) {
    boost::asio::post(io_context_, std::move(fn));
}
```

**关键设计**：
- 创建 `max_concurrency` 个 OS 线程
- 使用 `boost::asio::io_context` 作为任务队列
- 每个线程调用 `io_context_.run()` 阻塞等待任务
- `Post(fn)` 提交任务到队列，任意空闲线程取出执行

**GIL 初始化回调** (`python/ray/_raylet.pyx:2216-2232`)

```python
cdef function[void()] initialize_pygilstate_for_thread() nogil:
    """Initializes a C++ thread to make it be considered as a Python thread."""
    cdef function[void()] callback
    with gil:
        gstate = PyGILState_Ensure()       # ← 获取 Python GIL 状态
        callback = bind(pygilstate_release, ref(gstate))
    return callback
```

每个线程池中的线程在启动时调用 `PyGILState_Ensure()` 注册自己为 Python 线程，这样才能在执行 actor method 时获取 GIL 运行 Python 代码。

#### 12.3.6 是否创建线程池的决策

**NeedDefaultExecutor** (`src/ray/core_worker/task_execution/thread_pool.h:31-37`)

```cpp
// BoundedExecutor (线程 actor) 的 NeedDefaultExecutor
static bool NeedDefaultExecutor(int32_t max_concurrency_in_default_group,
                                bool has_other_concurrency_groups) {
    if (max_concurrency_in_default_group == 0) {
        return false;
    }
    // max_concurrency > 1 或有 concurrency groups 时才创建线程池
    return max_concurrency_in_default_group > 1 || has_other_concurrency_groups;
}
```

**关键决策**：
- `max_concurrency == 1`：**不创建线程池**，`default_executor_ = nullptr`，task 在主线程执行
- `max_concurrency > 1`：创建 `BoundedExecutor` 线程池，task 通过 `Post()` 分发到线程

对比 asyncio actor 的 `NeedDefaultExecutor` (`src/ray/core_worker/task_execution/fiber.h:114-120`)：

```cpp
// FiberState (asyncio actor) 的 NeedDefaultExecutor
static bool NeedDefaultExecutor(int32_t max_concurrency_in_default_group,
                                bool has_other_concurrency_groups) {
    // asyncio 模式总是需要 default executor
    return true;
}
```

#### 12.3.7 Task 到达时的分发

**队列类型选择** (`src/ray/core_worker/task_execution/task_receiver.cc:214-238`)

```cpp
} else if (task_spec.IsActorTask()) {
    auto it = actor_task_execution_queues_.find(task_spec.CallerWorkerId());
    if (it == actor_task_execution_queues_.end()) {
        it = actor_task_execution_queues_.emplace(
            task_spec.CallerWorkerId(),
            allow_out_of_order_execution_
                ? std::make_unique<UnorderedActorTaskExecutionQueue>(   // ← max_concurrency>1
                      ..., pool_manager_, ...)
                : std::make_unique<OrderedActorTaskExecutionQueue>(    // ← max_concurrency=1
                      ..., pool_manager_, ...)
        ).first;
    }
}
```

- `max_concurrency > 1` → `allow_out_of_order_execution=True` → `UnorderedActorTaskExecutionQueue`
- `max_concurrency == 1` → `OrderedActorTaskExecutionQueue`（但 pool 为空，主线程执行）

**分发到线程池** (`src/ray/core_worker/task_execution/unordered_actor_task_execution_queue.cc:144-168`)

```cpp
void UnorderedActorTaskExecutionQueue::RunRequestWithResolvedDependencies(
    TaskToExecute request) {
    const auto task_id = request.TaskID();
    if (is_asyncio_) {
        // asyncio actor: 分发到 fiber
        auto fiber = fiber_state_manager_->GetExecutor(...);
        fiber->EnqueueFiber([this, request = std::move(request), task_id]() mutable {
            AcceptRequestOrRejectIfCanceled(task_id, request);
        });
    } else {
        // 线程 actor: 分发到线程池
        RAY_CHECK(pool_manager_ != nullptr);
        auto pool = pool_manager_->GetExecutor(...);
        if (pool == nullptr) {
            // max_concurrency == 1: 主线程直接执行
            AcceptRequestOrRejectIfCanceled(task_id, request);
        } else {
            // max_concurrency > 1: Post 到 BoundedExecutor 线程池
            pool->Post([this, request = std::move(request), task_id]() mutable {
                AcceptRequestOrRejectIfCanceled(task_id, request);
            });
        }
    }
}
```

**`AcceptRequestOrRejectIfCanceled`** (`src/ray/core_worker/task_execution/unordered_actor_task_execution_queue.cc:185-200`)

```cpp
void UnorderedActorTaskExecutionQueue::AcceptRequestOrRejectIfCanceled(
    TaskID task_id, TaskToExecute &request) {
    bool is_canceled = false;
    { /* check if canceled */ }

    if (is_canceled) {
        request.Cancel(Status::SchedulingCancelled("Task is canceled."));
    } else {
        request.Execute();    // ← 实际执行 actor method (调用 Python UDF)
    }
}
```

`request.Execute()` 调用注册的 `task_execution_handler`（Cython 函数），反序列化参数并调用 Python actor method。这一步运行在 **线程池的某个工作线程** 上。

#### 12.3.8 完整链路时序图

```
Python API: map_batches(fn, max_concurrency=2, num_cpus=1)
  │
  ▼ ray_remote_args = {max_concurrency: 2, num_cpus: 1}
  │
ActorPoolMapOperator.__init__()
  │  max_actor_concurrency = _ray_remote_args.get("max_concurrency", 1) = 2
  │  max_tasks_in_flight_per_actor = 2 * 2 = 4 (默认因子)
  │
  ▼ start()
  │
  │  ray.remote(max_concurrency=2, num_cpus=1)(_MapWorker)
  │    │
  │    ▼ actor._remote()  [python/ray/actor.py:1556]
  │    │   max_concurrency 默认值处理
  │    │   allow_out_of_order_execution = True (因为 max_concurrency > 1)
  │    │
  │    ▼ worker.core_worker.create_actor(max_concurrency=2)
  │    │   [python/ray/_raylet.pyx:3565]
  │    │
  │    ▼ CCoreWorker.CreateActor()  [src/ray/core_worker/core_worker.cc:2353]
  │    │   → 序列化到 protobuf: actor_creation_spec.set_max_concurrency(2)
  │    │
  │    ▼ Actor worker 进程接收创建任务
  │    │   [src/ray/core_worker/task_execution/task_receiver.cc:97]
  │    │
  │    │   SetupActor(is_asyncio=false, fiber_max_concurrency=2,
  │    │              allow_out_of_order_execution=true)
  │    │
  │    │   pool_manager_ = ConcurrencyGroupManager<BoundedExecutor>(
  │    │       concurrency_groups={}, max_concurrency=2, ...)
  │    │
  │    │   NeedDefaultExecutor(2, false) → true
  │    │   → default_executor_ = BoundedExecutor(2, ...)
  │    │   → 创建 2 个 OS 线程，每个线程 io_context_.run() 等待
  │    │
  │    ▼ Actor task 到达
  │    │   [task_receiver.cc:214]
  │    │   allow_out_of_order_execution_ = true
  │    │   → 创建 UnorderedActorTaskExecutionQueue
  │    │
  │    │   RunRequestWithResolvedDependencies(request)
  │    │   → pool->Post(lambda { request.Execute() })
  │    │   → 某个工作线程从 io_context_ 取出 lambda 执行
  │    │   → request.Execute() → 调用 Python actor method → _MapWorker.submit()
  │    │   → _map_task() → map_transformer.apply_transform() → UDF.__call__()
```

### 12.4 enable_true_multi_threading：UDF 并发控制

#### 12.4.1 作用机制

`enable_true_multi_threading` 是 Ray Data 层（非 Ray Core 层）的参数，控制 UDF 是否允许在多线程中并发执行。

**判断逻辑** (`python/ray/data/_internal/planner/plan_udf_map_op.py:369-381`)

```python
if (
    not is_async_udf
    and isinstance(compute, ActorPoolStrategy)
    and not compute.enable_true_multi_threading
):
    # NOTE: By default Actor-based UDFs are restricted to run within a
    # single-thread (when enable_true_multi_threading=False).
    #
    # Historically, this has been done to allow block-fetching, batching, etc to
    # be overlapped with the actual UDF invocation, while avoiding the
    # pitfalls of concurrent GPU access (like OOMs, etc) when specifying
    # max_concurrency > 1.
    udf = make_callable_class_single_threaded(udf)
```

#### 12.4.2 _SingleThreadedWrapper 实现

**文件**: `python/ray/data/_internal/execution/util.py:58-83`

```python
def make_callable_class_single_threaded(callable_cls: CallableClass) -> CallableClass:
    """Returns a thread-safe CallableClass with the same logic as the provided
    `callable_cls`.

    This function allows the usage of concurrent actors by safeguarding user logic
    behind a separate thread.

    This allows batch slicing and formatting to occur concurrently, to overlap with the
    user provided UDF.
    """

    class _SingleThreadedWrapper(callable_cls):
        def __init__(self, *args, **kwargs):
            self.thread_pool_executor = ThreadPoolExecutor(max_workers=1)
            super().__init__(*args, **kwargs)

        def __repr__(self):
            return super().__repr__()

        def __call__(self, *args, **kwargs):
            # ThreadPoolExecutor will reuse the same thread for every submit call.
            future = self.thread_pool_executor.submit(super().__call__, *args, **kwargs)
            return future.result()

    return _SingleThreadedWrapper
```

**关键设计**：
- 用 `ThreadPoolExecutor(max_workers=1)` 包装 UDF 的 `__call__`
- 即使 Ray Core 分配了多个线程（`max_concurrency>1`），所有线程调用 UDF 时都会被序列化到同一个单线程池
- `future.result()` 阻塞等待 UDF 执行完成，调用方线程在此期间释放
- 但 block fetching / batching（step 1 和 3）仍然可以在 Ray Core 的多线程中并行执行

#### 12.4.3 三步流水线模型与参数作用域

每个 actor task 的执行分为三个步骤，这三个步骤发生在 `_map_task` (`map_operator.py:743`) 和 `MapTransformer.apply_transform` (`map_transformer.py:210`) 中：

```
Step 1: Batching Inputs
  ← Ray Core 线程：从 Object Store 拉取 block、反序列化、组装 batch
  ← map_transformer 的非 UDF transform_fn 执行（如 BlockBatching）

Step 2: Running actor UDF
  ← 调用用户的 __call__ (即 callable_class.__call__)
  ← 如果 enable_true_multi_threading=False，被 _SingleThreadedWrapper 序列化

Step 3: Batching Outputs
  ← Ray Core 线程：序列化输出 block、yield 回 _map_task、写回 Object Store
  ← map_transformer 的非 UDF transform_fn 执行（如 block sizing）
```

```python
# map_transformer.py:210-230
def apply_transform(self, input_blocks: Iterable[Block], ctx: TaskContext) -> Iterable[Block]:
    iter = input_blocks
    for transform_fn in self._transform_fns:
        iter = transform_fn(iter, ctx)          # ← Step 1/3 的 transform 函数
        if transform_fn._is_udf:
            iter = self._udf_timed_iter(iter)   # ← Step 2 的 UDF 调用
    return iter
```

```python
# map_operator.py:743-790 — _map_task 的核心循环
def _map_task(map_transformer, data_context, ctx, *blocks, ...):
    block_iter = iter(blocks)                    # ← Step 1: 输入 blocks
    for block in map_transformer.apply_transform(block_iter, ctx):
        # ← Step 2 在 apply_transform 内部完成 UDF 调用
        block_meta = BlockAccessor.for_block(block).get_metadata()
        yield block                              # ← Step 3: 输出 block 写回 Object Store
        yield BlockMetadataWithSchema(metadata=..., schema=block_schema)
```

- `max_concurrency` 控制的是 Ray Core actor 线程池大小，影响 **Step 1 和 Step 3** 可以并行
- `enable_true_multi_threading` 控制的是 **Step 2 (UDF)** 是否也被并行化

#### 12.4.4 四种组合的语义矩阵

| `enable_true_multi_threading` | `max_concurrency` | 线程池 | Step 1/3 并行 | Step 2 (UDF) 并行 | 实际效果 |
|---|---|---|---|---|---|
| False/True | 1 | 不创建 | 否 | 否 | 完全串行，主线程一次处理一个 task 的全部步骤 |
| False | >1 | N 线程 | 是 | 否 | N 个线程并行处理 step 1/3，但 UDF 被 `SingleThreadedWrapper` 的 `ThreadPoolExecutor(1)` 序列化 |
| True | >1 | N 线程 | 是 | 是 | N 个线程全并行，UDF 也并发执行（受 GIL 限制） |

#### 12.4.5 _SingleThreadedWrapper 如何实现 UDF 串行化（max_concurrency>1 + enable_true_multi_threading=False）

当 `max_concurrency=2` 且 `enable_true_multi_threading=False` 时：

```
Ray Core 线程池 (BoundedExecutor, 2 线程):
  线程1: |--fetch A--||--batch A--||--UDF A--||--batch out A--|
                                  ↑               |--fetch B--||--batch B--|
  线程2:                          ↑               UDF B 等 UDF A 完成后才能执行
                                  ↑
  _SingleThreadedWrapper 的 ThreadPoolExecutor(1):
    |--UDF A--|  |--UDF B--|
    ↑ UDF 被序列化到这个单线程池
```

1. 线程1 完成 fetch A + batch A，调用 `_SingleThreadedWrapper.__call__`
2. `_SingleThreadedWrapper` 将 UDF 提交到 `ThreadPoolExecutor(max_workers=1)`，然后 `future.result()` **阻塞等待**
3. 线程1 阻塞期间，线程2 可以执行 fetch B + batch B（step 1 并行）
4. 线程2 也调用 `_SingleThreadedWrapper.__call__`，同样提交到同一个 `ThreadPoolExecutor(1)`
5. 但 `ThreadPoolExecutor(1)` 只有一个工作线程，UDF B 必须等 UDF A 完成后才能开始
6. UDF A 完成后，线程1 解除阻塞，执行 batch out A（step 3）
7. UDF B 开始执行，完成后线程2 解除阻塞，执行 batch out B

**效果**：fetch/batch 并行加速，但 GPU 推理或计算密集型 UDF 一次只跑一个，避免并发 GPU 访问导致 OOM。

#### 12.4.6 enable_true_multi_threading=True 时的真并发执行链路

当 `max_concurrency=2` 且 `enable_true_multi_threading=True` 时，UDF **不被** `_SingleThreadedWrapper` 包装，两个 Ray Core 线程可以同时调用 UDF 的 `__call__`。

##### 层级 1：Ray Core 线程池 — 两个 task 同时进入 actor

`BoundedExecutor(2)` 有 2 个 OS 线程，2 个 task 通过 `pool->Post()` 同时分发到 2 个线程执行。每个线程已通过 `PyGILState_Ensure()` 注册为 Python 线程，可以执行 Python 代码。

```cpp
// unordered_actor_task_execution_queue.cc:144-168
void UnorderedActorTaskExecutionQueue::RunRequestWithResolvedDependencies(
    TaskToExecute request) {
    // ...
    // 线程 actor: 分发到线程池
    auto pool = pool_manager_->GetExecutor(...);
    if (pool == nullptr) {
        // max_concurrency == 1: 主线程直接执行
        AcceptRequestOrRejectIfCanceled(task_id, request);
    } else {
        // max_concurrency > 1: Post 到 BoundedExecutor 线程池
        pool->Post([this, request = std::move(request), task_id]() mutable {
            AcceptRequestOrRejectIfCanceled(task_id, request);
        });
    }
}
```

两个 task 被分别 `Post` 到 `io_context_`，两个线程各自从中取出一个 task 执行 `request.Execute()`，调用 Python actor method。

##### 层级 2：UDF 无包装 — 两个线程同时调用 `__call__`

`enable_true_multi_threading=True` → `plan_udf_map_op.py:369` 的 if 条件不满足 → **不调用** `make_callable_class_single_threaded`，UDF 保持原始类：

```python
# plan_udf_map_op.py:369-381
if (
    not is_async_udf
    and isinstance(compute, ActorPoolStrategy)
    and not compute.enable_true_multi_threading   # ← True 时此条件为 False
):
    # 这段代码不会执行，UDF 不被包装
    udf = make_callable_class_single_threaded(udf)
# UDF 保持原始类，__call__ 可被多个线程同时调用
```

两个 Ray Core 线程**各自独立**走完 step 1 → 2 → 3 的全流程，调用同一个 UDF 实例的 `__call__`，**没有任何串行化瓶颈**。

##### 层级 3：三步全部并行

```
max_concurrency=2, enable_true_multi_threading=True:

Ray Core 线程池 (2 threads, 无 SingleThreadedWrapper):
  线程1: |--fetch A--||--batch A--||--UDF A--||--batch out A--|
  线程2:                |--fetch B--||--batch B--||--UDF B--||--batch out B--|
                                      ↑
                                两个 UDF 同时运行在不同 OS 线程上
```

**关键重叠**：
- fetch A 和 fetch B **并行**（I/O 重叠）
- batch A 和 batch B **并行**（数据准备重叠）
- UDF A 和 UDF B **同时调用**（无 SingleThreadedWrapper 阻塞）
- batch out A 和 batch out B **并行**（输出序列化重叠）

##### 层级 4：Python GIL 限制 — "同时"的实际效果

两个 OS 线程同时调用 UDF 的 `__call__`，但 Python 的 GIL（全局解释器锁）保证同一时刻只有一个线程执行 Python 字节码。所以"同时"的实际效果取决于 UDF 性质：

```python
# 线程1 调用 UDF.__call__(batch_A)
# 线程2 调用 UDF.__call__(batch_B)
# 两个线程都在 Python 层面请求执行，但 GIL 只允许一个持有
```

| UDF 类型 | 是否真正并行 | 原因 |
|----------|------------|------|
| **纯 Python CPU 计算** | 不会真并行 | GIL 限制，两个线程交替获取 GIL 执行，本质是并发而非并行 |
| **C 扩展 / 释放 GIL 的库** | 真并行 | 如 NumPy、PyTorch 的 C 底层在计算时释放 GIL，两个线程真正同时跑 |
| **I/O 操作（网络/磁盘）** | 真并行 | I/O 等待时释放 GIL，如 HTTP 请求、文件读写 |
| **GPU 推理** | 真并行（但有 OOM 风险） | CUDA 操作释放 GIL，但两个 batch 同时上 GPU 可能超显存 |

##### 完整执行时序（enable_true_multi_threading=True）

```
max_concurrency=2, enable_true_multi_threading=True:

Ray Core 线程池 (2 threads, 无 SingleThreadedWrapper):

  线程1: |--fetch A--||--batch A--||--UDF A (持有GIL)--||--batch out A--|
  线程2:                |--fetch B--||--batch B--||--UDF B (等GIL)--||--batch out B--|
                                                   ↑
                                  UDF A 释放 GIL 时 UDF B 才能获取 GIL 执行
                                  (但 NumPy/Torch 等 C 操作期间会释放 GIL)

  _SingleThreadedWrapper: 无 (enable_true_multi_threading=True 时不创建)
  GIL 状态: 两个线程竞争 GIL，纯 Python 代码串行，C 扩展/I/O 可真正并行
```

**逐步说明**：

1. 两个 task 几乎同时到达，`Post()` 到线程池
2. 线程1 和线程2 **并行**执行 fetch A 和 fetch B（I/O 操作释放 GIL，真并行）
3. 线程1 完成 fetch A + batch A，调用 UDF 的 `__call__`（原始类，无包装）
4. 线程1 **获取 GIL**，开始执行 UDF A 的 Python 代码
5. 线程2 完成 fetch B + batch B，也调用 UDF 的 `__call__`
6. 线程2 **等待 GIL**（因为线程1 持有）
7. 当 UDF A 执行到 C 扩展代码（如 `numpy.array()`、`torch.matmul()`）时，**释放 GIL**
8. 线程2 获取 GIL，开始执行 UDF B 的 Python 代码
9. 两个 UDF 在 C 扩展层面**真正并行**，在纯 Python 代码层面**交替执行**

##### enable_true_multi_threading 两种模式完整对比

```
enable_true_multi_threading=False (默认):
  Ray Core 2 线程:  fetch A/B 并行 ✓   batch A/B 并行 ✓
  SingleThreadedWrapper(1):  UDF A → UDF B 串行 ✗
  → step 1/3 并行, step 2 串行 (显式串行化，无 GIL 争抢)

enable_true_multi_threading=True:
  Ray Core 2 线程:  fetch A/B 并行 ✓   batch A/B 并行 ✓
  无包装:            UDF A 和 UDF B 同时调用 ✓
  GIL:               纯 Python 代码交替执行, C 扩展/I/O 真并行
  → step 1/2/3 全部可并行 (但受 GIL 限制)
```

| 对比维度 | `enable_true_multi_threading=False` | `enable_true_multi_threading=True` |
|----------|--------------------------------------|-------------------------------------|
| 串行化机制 | `ThreadPoolExecutor(1)` 显式序列化 | 无串行化，依赖 GIL 隐式控制 |
| UDF 执行方式 | 严格串行：A 完成后 B 才开始 | 并发调用：A 和 B 同时请求执行 |
| 纯 Python UDF | 串行（无 GIL 争抢，效率更高） | 串行（GIL 争抢，效率可能更低） |
| C 扩展 / NumPy / Torch UDF | 串行（C 操作也无法并行） | 真并行（C 操作释放 GIL） |
| I/O 密集型 UDF | 串行（I/O 等待无法并行） | 真并行（I/O 等待释放 GIL） |
| GPU 安全性 | 安全（一次只跑一个 batch） | 有 OOM 风险（两个 batch 同时上 GPU） |
| 线程争抢 | 无（单线程池无竞争） | 有（多线程竞争 GIL） |

### 12.5 资源分配视角

**文件**: `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py:594-598`

```python
def per_task_resource_allocation(self: "PhysicalOperator") -> ExecutionResources:
    max_concurrency = self._actor_pool.max_actor_concurrency()
    per_actor_resource_usage = self._actor_pool.per_actor_resource_usage()
    return per_actor_resource_usage.scale(1 / max_concurrency)
```

每个 task 实际分到的资源 = actor 总资源 / max_concurrency。例如 actor 配了 4 CPU + `max_concurrency=2`，则每个 task 分到 2 CPU。

### 12.6 Autoscaler 利用率计算与配置校验

**文件**: `python/ray/data/_internal/actor_autoscaler/autoscaling_actor_pool.py:115-122`

```python
def get_pool_util(self) -> float:
    if self.num_running_actors() == 0:
        return float("inf")
    else:
        return self.num_tasks_in_flight() / (
            self.max_actor_concurrency() * self.num_running_actors()
        )
```

利用率 = 总 in-flight task 数 / (max_concurrency × actor 数)。

**文件**: `python/ray/data/_internal/actor_autoscaler/default_actor_autoscaler.py:299-311`

```python
max_tasks_in_flight_per_actor = actor_pool.max_tasks_in_flight_per_actor()
max_concurrency = actor_pool.max_actor_concurrency()

if (
    max_tasks_in_flight_per_actor / max_concurrency
    < self._actor_pool_scaling_up_threshold
):
    logger.warning(
        f"Actor Pool configuration of the {op} will not allow it to scale up: "
        f"configured utilization threshold ({self._actor_pool_scaling_up_threshold * 100}%) "
        f"couldn't be reached with configured max_concurrency={max_concurrency} "
        f"and max_tasks_in_flight_per_actor={max_tasks_in_flight_per_actor} "
        f"(max utilization will be max_tasks_in_flight_per_actor / max_concurrency = "
        f"{(max_tasks_in_flight_per_actor / max_concurrency) * 100:g}%)"
    )
```

如果 `max_tasks_in_flight_per_actor / max_concurrency < upscaling_threshold`（默认 0.5），会警告 autoscaler 永远无法触发扩容。例如 `max_tasks_in_flight=1, max_concurrency=2` 时，最大利用率只有 50%，达不到默认的 0.5 扩容阈值。

### 12.7 完整数据流时序图

#### 12.7.1 max_tasks_in_flight=2, max_concurrency=1（默认）

```
时间 ──────────────────────────────────────────────────────────►

Actor Pool (调度层):
  Task A submit ──────────────────────────────────────► on_task_completed(A)
  Task B submit (A还在跑) ──────────────────────► on_task_completed(B)
       ↑ num_tasks_in_flight=2                     ↑ num_tasks_in_flight=0

Actor 内部 (Ray Core, max_concurrency=1, 单线程):
  |--fetch A--||--batch A--||--UDF A (SingleThreadedWrapper)--||--batch out A--|
                            ↑ 此时线程被UDF A占用
                               |--fetch B (排队等待线程)--||--batch B--||--UDF B--||--batch out B--|
```

Task B 在 Actor Pool 层面已经提交（`num_tasks_in_flight=2`），Ray 运行时开始为 B 预取数据。但 actor 单线程，B 的 UDF 必须等 A 的 UDF 完成后才能执行。**预取隐藏了 fetch 延迟。**

#### 12.7.2 max_tasks_in_flight=2, max_concurrency=2, enable_true_multi_threading=True

```
时间 ──────────────────────────────────────────────────────────►

Actor Pool (调度层):
  Task A submit ──────────────────────────────────────────► on_task_completed(A)
  Task B submit ──────────────────────────────────────────► on_task_completed(B)

Actor 内部 (Ray Core, max_concurrency=2, 双线程池, 无 SingleThreadedWrapper):
  线程1: |--fetch A--||--batch A--||--UDF A (持有GIL)--||--batch out A--|
  线程2:            |--fetch B--||--batch B--||--UDF B (等GIL/或并行)--||--batch out B--|
                                                ↑
                                      UDF A 释放 GIL 时 UDF B 才能获取
                                      (NumPy/Torch 等 C 操作期间释放 GIL, 可真并行)
```

两个线程全流程并行。UDF 同时调用，纯 Python 代码受 GIL 限制交替执行，C 扩展/I/O 操作可真正并行。

#### 12.7.3 max_tasks_in_flight=2, max_concurrency=2, enable_true_multi_threading=False

```
时间 ──────────────────────────────────────────────────────────►

Actor Pool (调度层):
  Task A submit ──────────────────────────────────────────► on_task_completed(A)
  Task B submit ──────────────────────────────────────────► on_task_completed(B)

Actor 内部 (Ray Core, max_concurrency=2, 双线程池, UDF 被 SingleThreadedWrapper 包装):
  线程1: |--fetch A--||--batch A--||--UDF A (占用单线程池)--||--batch out A--|
  线程2:                |--fetch B--||--batch B--||--UDF B (等A完成)--||--batch out B--|
                                                       ↑
                                             _SingleThreadedWrapper 的 ThreadPoolExecutor(1):
                                               |--UDF A--||--UDF B--|
                                               ↑ 显式串行化, 无 GIL 争抢
```

fetch/batch 并行，但 UDF 串行。关键重叠：
- fetch A 和 fetch B **并行**（I/O 重叠）
- batch out A 和 UDF B **并行**（输出序列化与计算重叠）
- UDF A 和 UDF B **串行**（被 `ThreadPoolExecutor(1)` 序列化）

适合 GPU 推理场景：数据准备并行加速，但 GPU 推理一次只跑一个，避免 OOM。

### 12.8 参数选择建议

| 场景 | 推荐配置 | 原因 |
|------|----------|------|
| 纯 Python CPU 密集型 UDF | `max_tasks_in_flight_per_actor=2`（默认），`max_concurrency=1` | 预取流水线足够；GIL 限制下多线程无益，反而增加争抢开销 |
| GPU 推理（大 batch，防 OOM） | `max_concurrency=2` + `enable_true_multi_threading=False` | 数据准备（fetch/batch）并行加速；推理串行避免并发 GPU 访问导致 OOM |
| NumPy/PyTorch 计算密集型 UDF | `max_concurrency=2` + `enable_true_multi_threading=True` | C 扩展释放 GIL，两个 UDF 的底层计算可真正并行 |
| I/O 密集型 UDF（网络请求等） | `max_concurrency=2` + `enable_true_multi_threading=True` | I/O 等待释放 GIL，UDF 真并发，充分利用等待时间 |
| 低延迟要求 | `max_tasks_in_flight_per_actor=1` | 减少排队延迟（牺牲预取流水线） |

**选择决策树**：

```
UDF 是否释放 GIL？
├── 否（纯 Python 计算）
│   └── max_concurrency=1, max_tasks_in_flight_per_actor=2 (默认)
│       → 预取流水线即可，不需要多线程
│
└── 是（C 扩展 / I/O / GPU）
    └── 是否需要保护共享资源（如 GPU 显存）？
        ├── 是（GPU 大 batch，防 OOM）
        │   └── max_concurrency=2, enable_true_multi_threading=False
        │       → I/O 并行, UDF 串行
        │
        └── 否（I/O 请求, 小 GPU batch, CPU C 扩展）
            └── max_concurrency=2, enable_true_multi_threading=True
                → 全并行, UDF 也并发
```

### 12.9 关键文件索引

#### Ray Data 层

| 文件 | 关键代码 |
|------|----------|
| `python/ray/data/dataset.py:770` | `map_batches` 用户 API，`**ray_remote_args` 接收 `max_concurrency` |
| `python/ray/data/_internal/compute.py:116` | `ActorPoolStrategy.__init__` 参数定义 |
| `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py:155-186` | 参数初始化和默认值计算 |
| `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py:253` | `start()` 中 `ray.remote(**ray_remote_args)` 创建 actor |
| `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py:370-396` | `_try_schedule_tasks_internal` 任务提交 |
| `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py:1196-1201` | `schedulable_actors` 可调度判断 |
| `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py:1204-1205` | `on_task_submitted` 计数递增 |
| `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py:1293-1312` | `on_task_completed` 任务完成回调 |
| `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py:594-598` | `per_task_resource_allocation` 资源分配 |
| `python/ray/data/_internal/execution/operators/map_operator.py:743` | `_map_task` — task 执行核心函数 |
| `python/ray/data/_internal/execution/operators/map_transformer.py:210` | `apply_transform` — transform 函数链式调用 |
| `python/ray/data/_internal/execution/operators/map_transformer.py:198` | `_udf_timed_iter` — UDF 调用计时包装 |
| `python/ray/data/_internal/planner/plan_udf_map_op.py:369-381` | `enable_true_multi_threading` 判断与包装 |
| `python/ray/data/_internal/execution/util.py:58-83` | `make_callable_class_single_threaded` UDF 串行化包装 |
| `python/ray/data/_internal/actor_autoscaler/autoscaling_actor_pool.py:115-122` | `get_pool_util` 利用率计算 |
| `python/ray/data/_internal/actor_autoscaler/default_actor_autoscaler.py:299-311` | autoscaler 配置校验警告 |
| `python/ray/data/context.py:253` | `DEFAULT_ACTOR_MAX_TASKS_IN_FLIGHT_TO_MAX_CONCURRENCY_FACTOR = 2` |

#### Ray Core 层

| 文件 | 关键代码 |
|------|----------|
| `python/ray/_private/ray_constants.py:501` | `DEFAULT_MAX_CONCURRENCY_THREADED = 1` |
| `python/ray/actor.py:1556-1562` | `_remote()` 中 max_concurrency 默认值处理 |
| `python/ray/actor.py:1790-1808` | `max_concurrency > 1` 时强制 `allow_out_of_order_execution=True` |
| `python/ray/_raylet.pyx:3565-3645` | Cython 绑定，`max_concurrency` 传入 `CActorCreationOptions` |
| `python/ray/_raylet.pyx:2216-2232` | `initialize_pygilstate_for_thread` — 线程 GIL 初始化回调 |
| `src/ray/core_worker/common.h:119-180` | `ActorCreationOptions` 结构体存储 `max_concurrency` |
| `src/ray/common/task/task_util.h:248-295` | protobuf 序列化 `set_max_concurrency()` |
| `src/ray/common/task/task_spec.cc:488-491` | `MaxActorConcurrency()` 读取 |
| `src/ray/core_worker/task_execution/task_receiver.cc:97-106` | actor 创建时线程池类型选择 |
| `src/ray/core_worker/task_execution/task_receiver.cc:208-287` | `SetupActor` 存储 max_concurrency |
| `src/ray/core_worker/task_execution/concurrency_group_manager.cc:28-54` | `ConcurrencyGroupManager` 线程池管理 |
| `src/ray/core_worker/task_execution/thread_pool.h:31-37` | `NeedDefaultExecutor` — 是否创建线程池 |
| `src/ray/core_worker/task_execution/thread_pool.cc:26-67` | `BoundedExecutor` — OS 线程池实现 |
| `src/ray/core_worker/task_execution/unordered_actor_task_execution_queue.cc:144-168` | task 分发到线程池 |
| `src/ray/core_worker/task_execution/unordered_actor_task_execution_queue.cc:185-200` | `AcceptRequestOrRejectIfCanceled` → `request.Execute()` |
