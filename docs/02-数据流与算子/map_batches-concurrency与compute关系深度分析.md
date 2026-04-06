# Ray Data map_batches: concurrency 与 compute 关系深度分析

## 1. 问题背景

在使用 Ray Data 的 `map_batches` 时，常常同时指定 `concurrency` 和 `compute` 参数：

```python
result_ds = ds.map_batches(
    actor_cls,
    fn_constructor_kwargs=dict(config=config),
    batch_size=config.batch_size,
    concurrency=config.concurrency,
    compute=config.get_actor_pool_strategy(),
    num_cpus=config.num_cpus,
    num_gpus=config.num_gpus,
)
```

这两个参数都涉及并发控制，但它们之间的关系和优先级容易混淆。本文档基于源码深入分析二者的关系。

---

## 2. 核心结论

**`concurrency` 是 `compute` 的旧版别名，已被废弃。当 `compute` 和 `concurrency` 同时指定时，`compute` 优先，`concurrency` 被完全忽略。**

---

## 3. 源码分析

### 3.1 map_batches 函数签名

**文件:** `python/ray/data/dataset.py:487`

```python
def map_batches(
    self,
    fn: UserDefinedFunction[DataBatch, DataBatch],
    *,
    batch_size: Union[int, None, Literal["default"]] = None,
    compute: Optional[ComputeStrategy] = None,
    batch_format: Optional[str] = "default",
    zero_copy_batch: bool = True,
    fn_args: Optional[Iterable[Any]] = None,
    fn_kwargs: Optional[Dict[str, Any]] = None,
    fn_constructor_args: Optional[Iterable[Any]] = None,
    fn_constructor_kwargs: Optional[Dict[str, Any]] = None,
    num_cpus: Optional[float] = None,
    num_gpus: Optional[float] = None,
    memory: Optional[float] = None,
    concurrency: Optional[Union[int, Tuple[int, int], Tuple[int, int, int]]] = None,
    udf_modifying_row_count: bool = True,
    ...
)
```

`concurrency` 参数的 docstring（第 678 行）明确标注：

> `concurrency: This argument is deprecated. Use ``compute`` argument.`

### 3.2 统一解析函数: get_compute_strategy()

**文件:** `python/ray/data/_internal/util.py:566-700`

这是 `concurrency` 与 `compute` 关系的核心解析函数。在 `_map_batches_without_batch_size_validation`（第 808 行）中被调用：

```python
compute = get_compute_strategy(
    fn,
    fn_constructor_args=fn_constructor_args,
    compute=compute,
    concurrency=concurrency,
)
```

### 3.3 完整解析逻辑

```python
def get_compute_strategy(
    fn: "UserDefinedFunction",
    fn_constructor_args: Optional[Iterable[Any]] = None,
    compute: Optional[Union[str, "ComputeStrategy"]] = None,
    concurrency: Optional[Union[int, Tuple[int, int], Tuple[int, int, int]]] = None,
) -> "ComputeStrategy":
    # 1. 判断 fn 是否为 Callable Class
    if isinstance(fn, CallableClass):
        is_callable_class = True
    else:
        is_callable_class = False
        if fn_constructor_args is not None:
            raise ValueError(...)

    # 2. 如果 compute 已显式提供，验证并直接返回（concurrency 被忽略）
    if compute is not None:
        if is_callable_class and (compute == "tasks" or isinstance(compute, TaskPoolStrategy)):
            raise ValueError("can't schedule callable classes with task pool strategy")
        elif not is_callable_class and (compute == "actors" or isinstance(compute, ActorPoolStrategy)):
            raise ValueError("can't schedule regular functions with actor pool strategy")
        return compute

    # 3. 如果 compute 未设置但 concurrency 已设置（废弃路径）
    elif concurrency is not None:
        logger.warning("``concurrency`` is deprecated in Ray 2.51...")
        if isinstance(concurrency, tuple):
            # 2 元组: (min_size, max_size) -> ActorPoolStrategy
            if len(concurrency) == 2:
                return ActorPoolStrategy(min_size=concurrency[0], max_size=concurrency[1])
            # 3 元组: (min_size, max_size, initial_size) -> ActorPoolStrategy
            else:
                return ActorPoolStrategy(
                    min_size=concurrency[0],
                    max_size=concurrency[1],
                    initial_size=concurrency[2],
                )
        elif isinstance(concurrency, int):
            if is_callable_class:
                return ActorPoolStrategy(size=concurrency)   # int + class = ActorPool
            else:
                return TaskPoolStrategy(size=concurrency)    # int + function = TaskPool
        else:
            raise ValueError(...)

    # 4. 都未设置，根据 fn 类型使用默认策略
    else:
        if is_callable_class:
            return ActorPoolStrategy(min_size=1, max_size=None)  # 自动伸缩 1..inf
        else:
            return TaskPoolStrategy()  # 无限 tasks
```

---

## 4. concurrency 到 compute 的完整转换规则

| `fn` 类型 | `compute` | `concurrency` 值 | 转换结果 |
|---|---|---|---|
| Callable Class | 已设置 | 任意 | **使用 compute，忽略 concurrency** |
| 普通 Function | 已设置 | 任意 | **使用 compute，忽略 concurrency** |
| Callable Class | 未设置 | `int` | `ActorPoolStrategy(size=int)` |
| 普通 Function | 未设置 | `int` | `TaskPoolStrategy(size=int)` |
| Callable Class | 未设置 | `(min, max)` | `ActorPoolStrategy(min_size=min, max_size=max)` |
| Callable Class | 未设置 | `(min, max, init)` | `ActorPoolStrategy(min_size=min, max_size=max, initial_size=init)` |
| 普通 Function | 未设置 | 无 | `TaskPoolStrategy()` (无限并发) |
| Callable Class | 未设置 | 无 | `ActorPoolStrategy(min_size=1, max_size=inf)` |

---

## 5. 两种 ComputeStrategy 详解

### 5.1 TaskPoolStrategy

**文件:** `python/ray/data/_internal/compute.py:27-64`

```python
class TaskPoolStrategy(ComputeStrategy):
    def __init__(self, *, size: Optional[int] = None):
        self.size = size
```

- 每次调用创建一个独立的 Ray Task
- `size` 限制最大同时运行的 Task 数量
- 适用于**无状态**的普通函数
- Task 执行完毕即销毁，无启动开销复用

### 5.2 ActorPoolStrategy

**文件:** `python/ray/data/_internal/compute.py:65-219`

```python
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
    ):
```

关键属性初始化后：
- `self.min_size = min_size or 1`
- `self.max_size = max_size or float("inf")`
- `self.initial_size = initial_size or self.min_size`
- `self.max_tasks_in_flight_per_actor` — 控制每个 Actor 同时在执行的任务数（流水线并发）
- `self.enable_true_multi_threading` — 控制是否在单个 Actor 内多线程执行 UDF

多线程行为矩阵（来自 docstring）：

| `enable_true_multi_threading` | `max_concurrency` | 行为 |
|---|---|---|
| False/True | 1 | 每个 Actor 同时只执行 1 个 task |
| False | >1 | 多个 task 并发调度 I/O，UDF 串行执行 |
| True | >1 | 多个 task 并发调度，UDF 也并发执行 |

---

## 6. ComputeStrategy 到物理算子的映射

### 6.1 MapOperator.create() 工厂方法

**文件:** `python/ray/data/_internal/execution/operators/map_operator.py:390-435`

```python
@classmethod
def create(
    cls,
    map_transformer, input_op, data_context, ...,
    compute_strategy: Optional[ComputeStrategy] = None, ...
):
    if compute_strategy is None:
        compute_strategy = TaskPoolStrategy()

    if isinstance(compute_strategy, TaskPoolStrategy):
        return TaskPoolMapOperator(
            ...,
            max_concurrency=compute_strategy.size,   # TaskPoolStrategy.size -> max_concurrency
            ...
        )
    elif isinstance(compute_strategy, ActorPoolStrategy):
        return ActorPoolMapOperator(
            ...,
            compute_strategy=compute_strategy,       # 整个 strategy 传给 ActorPoolMapOperator
            ...
        )
```

### 6.2 TaskPoolMapOperator: max_concurrency 限制

**文件:** `python/ray/data/_internal/execution/operators/task_pool_map_operator.py:39-95`

`TaskPoolStrategy.size` 直接映射为 `TaskPoolMapOperator.max_concurrency`，通过反压策略强制限制：

**文件:** `python/ray/data/_internal/execution/backpressure_policy/concurrency_cap_backpressure_policy.py:147`

```python
def can_add_input(self, op: "PhysicalOperator") -> bool:
    num_tasks_running = op.metrics.num_tasks_running
    ...
    return num_tasks_running < self._concurrency_caps[op]
```

### 6.3 ActorPoolMapOperator: Actor 池自动伸缩

**文件:** `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py:71-170`

构造函数中，`ActorPoolStrategy` 的属性流入 `_ActorPool`：

```python
self._actor_pool = _ActorPool(
    self._start_actor,
    per_actor_resource_usage,
    min_size=compute_strategy.min_size,         # 来自 ActorPoolStrategy
    max_size=compute_strategy.max_size,         # 来自 ActorPoolStrategy
    initial_size=compute_strategy.initial_size,  # 来自 ActorPoolStrategy
    max_actor_concurrency=max_actor_concurrency, # 来自 ray_remote_args.get("max_concurrency", 1)
    max_tasks_in_flight_per_actor=(
        compute_strategy.max_tasks_in_flight_per_actor
        or data_context.max_tasks_in_flight_per_actor
        or max_actor_concurrency * DEFAULT_ACTOR_MAX_TASKS_IN_FLIGHT_TO_MAX_CONCURRENCY_FACTOR
    ),
)
```

`can_add_input` 方法（第 285 行）委托给 task selector：

```python
def can_add_input(self) -> bool:
    return self._actor_task_selector.can_schedule_task()
```

task selector（第 558 行）检查是否有可用 Actor：

```python
def can_schedule_task(self) -> bool:
    available_actors = self._actor_pool.schedulable_actors()
    return len(available_actors) > 0
```

`schedulable_actors()`（第 924 行）按 `max_tasks_in_flight` 过滤：

```python
def schedulable_actors(self) -> List[ray.actor.ActorHandle]:
    available_actors = self.get_available_actors()
    return [
        actor for actor, state in available_actors.items()
        if state.num_tasks_in_flight < self.max_tasks_in_flight_per_actor()
        and not state.is_restarting
    ]
```

---

## 7. 完整调用链

```
Dataset.map_batches(fn, compute=..., concurrency=...)
  │
  ▼
Dataset._map_batches_without_batch_size_validation(...)
  │
  ▼
get_compute_strategy(fn, compute, concurrency)     # 统一解析 concurrency → compute
  │  返回: TaskPoolStrategy 或 ActorPoolStrategy
  ▼
MapBatches(logical_op, fn, compute=..., ...)        # 逻辑算子，存储 compute strategy
  │
  ▼
MapOperator.create(compute_strategy=...)
  │
  ├── TaskPoolStrategy ──▶ TaskPoolMapOperator(max_concurrency=compute_strategy.size)
  │                            │
  │                            ▼
  │                          ConcurrencyCapBackpressurePolicy 限制并发 Task 数
  │
  └── ActorPoolStrategy ──▶ ActorPoolMapOperator(compute_strategy=compute_strategy)
                               │
                               ▼
                             _ActorPool(min_size, max_size, initial_size, max_tasks_in_flight_per_actor, ...)
                               │
                               ▼
                             Actor 自动伸缩 + 按 num_tasks_in_flight 分发任务
```

---

## 8. 实际场景分析

回到本文开头的问题代码：

```python
result_ds = ds.map_batches(
    actor_cls,          # Callable Class
    concurrency=config.concurrency,
    compute=config.get_actor_pool_strategy(),  # 显式提供 ActorPoolStrategy
)
```

由于 `compute` 和 `concurrency` 同时指定：

1. **`concurrency` 参数完全无效** — `get_compute_strategy()` 中 `compute is not None` 分支直接返回 `compute`，不处理 `concurrency`
2. **最终行为完全由 `get_actor_pool_strategy()` 返回的 `ActorPoolStrategy` 决定**
3. `ActorPoolStrategy` 控制的并发维度比 `concurrency`（单一 int）丰富得多：
   - **`min_size` / `max_size`**: Actor 池的自动伸缩范围
   - **`initial_size`**: 初始 Actor 数量
   - **`max_tasks_in_flight_per_actor`**: 每个 Actor 同时在执行的任务数（流水线并发）
   - **`enable_true_multi_threading`**: 是否在单个 Actor 内多线程执行 UDF

### 8.1 concurrency 的局限性

| 维度 | `concurrency` (int) | `ActorPoolStrategy` |
|---|---|---|
| Actor 数量 | 固定值 | min/max 自动伸缩 |
| 初始 Actor 数 | 不支持 | `initial_size` |
| 每 Actor 并发任务数 | 不支持 | `max_tasks_in_flight_per_actor` |
| 多线程 UDF | 不支持 | `enable_true_multi_threading` |
| TaskPool 支持 | int → TaskPoolStrategy(size) | 需显式指定 TaskPoolStrategy |

### 8.2 建议

既然已经使用了 `compute=get_actor_pool_strategy()`，**`concurrency` 参数可以安全删除**：

```python
result_ds = ds.map_batches(
    actor_cls,
    fn_constructor_kwargs=dict(config=config),
    batch_size=config.batch_size,
    compute=config.get_actor_pool_strategy(),  # 仅用 compute
    num_cpus=config.num_cpus,
    num_gpus=config.num_gpus,
)
```

所有并发控制逻辑统一在 `get_actor_pool_strategy()` 中配置，更清晰也更灵活。

---

## 9. 关键源码索引

| 文件 | 行号 | 内容 |
|---|---|---|
| `python/ray/data/dataset.py` | 487-786 | `map_batches()` 定义 |
| `python/ray/data/dataset.py` | 788-830 | `_map_batches_without_batch_size_validation()` 调用 `get_compute_strategy()` |
| `python/ray/data/_internal/util.py` | 566-700 | `get_compute_strategy()` 核心解析函数 |
| `python/ray/data/_internal/compute.py` | 27-64 | `TaskPoolStrategy` 类定义 |
| `python/ray/data/_internal/compute.py` | 65-219 | `ActorPoolStrategy` 类定义 |
| `python/ray/data/_internal/execution/operators/map_operator.py` | 390-435 | `MapOperator.create()` 工厂方法 |
| `python/ray/data/_internal/execution/operators/task_pool_map_operator.py` | 32-95 | `TaskPoolMapOperator.__init__()` |
| `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py` | 71-170 | `ActorPoolMapOperator.__init__()` |
| `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py` | 285-290 | `ActorPoolMapOperator.can_add_input()` |
| `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py` | 558-561 | `_ActorTaskSelectorImpl.can_schedule_task()` |
| `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py` | 924-932 | `_ActorPool.schedulable_actors()` |
| `python/ray/data/_internal/execution/backpressure_policy/concurrency_cap_backpressure_policy.py` | 147-170 | `ConcurrencyCapBackpressurePolicy` 并发上限强制 |
| `python/ray/data/_internal/logical/operators/map_operator.py` | 186-240 | `MapBatches` 逻辑算子类 |
