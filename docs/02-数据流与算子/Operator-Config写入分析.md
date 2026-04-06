# Ray Data Operator Config 写入机制分析

## 背景

在使用 `ds.map()` 时，发现以下两种写法在配置写入行为上不同：

**写法 A（配置会写入）：**
```python
ds = ds.map(
    StreamingVideoProcessMapper,
    fn_constructor_kwargs={"config": streaming_config},
    num_gpus=streaming_num_gpus,
    num_cpus=streaming_num_cpus,
    compute=ray.data.ActorPoolStrategy(
        min_size=streaming_gpu_concurrency,
        max_size=streaming_gpu_concurrency,
    ),
)
```

**写法 B（配置未写入）：**
```python
ds = ds.map(
    StreamingVideoProcessMapper,
    fn_constructor_kwargs={"config": streaming_config},
    num_gpus=streaming_num_gpus,
    num_cpus=streaming_num_cpus,
)
```

本文档分析配置写入的完整代码路径、各参数组合下的行为差异、以及不会写入的所有情况。

---

## 一、配置写入的入口：`_initialize_operator_config_sync`

**文件：** `python/ray/data/_internal/execution/streaming_executor.py:380-408`

```python
def _initialize_operator_config_sync(self) -> None:
    """Initialize operator configuration synchronization if enabled."""
    # 门槛 1：功能开关
    if not self._data_context.enable_dynamic_execution_config_sync:
        return

    from ray.data._internal.execution.config import (
        ConfigController,
        create_execution_config_store,
    )

    # 门槛 2：job_id 获取
    job_id = self._get_job_submission_id_or_job_id()
    if not job_id:
        logger.debug("No job_id or job_submission_id available")
        return

    try:
        # 门槛 3：store 创建
        store = create_execution_config_store(
            data_context=self._data_context,
            job_id=job_id,
        )
        if store is None:
            return

        # 实际写入配置
        store.init(self._generate_initial_operator_config(job_id))
        self._config_controller = ConfigController(self._topology, store)
        logger.info("Operator configuration synchronization initialized")
    # 门槛 4：异常捕获
    except Exception as e:
        logger.warning(f"Failed to initialize operator config sync: {e}")
```

该方法在 `execute()` 中被调用（第 257 行），是配置写入的唯一入口。

### 外层 4 道门槛

| # | 条件 | 代码位置 | 说明 |
|---|------|---------|------|
| 1 | `enable_dynamic_execution_config_sync == False` | 第 382 行 | 功能总开关，为 False 时直接 return |
| 2 | `job_id` 为 None | 第 391-394 行 | `RAY_JOB_CONFIG_JSON_ENV_VAR` 未设置且 `get_job_id()` 返回空 |
| 3 | `create_execution_config_store()` 返回 None | 第 401-402 行 | store 类型为 kconf 但缺少依赖包或 key/token |
| 4 | try 块内任意异常 | 第 407-408 行 | 被 catch 后仅 warning，静默跳过 |

---

## 二、配置写入的核心逻辑：`_generate_initial_operator_config`

**文件：** `python/ray/data/_internal/execution/streaming_executor.py:410-450`

```python
def _generate_initial_operator_config(self, job_id: str):
    from ray.data._internal.execution.config import (
        ExecutionConfig,
        TaskPoolOperatorConfig,
        ActorPoolOperatorConfig,
    )

    config = ExecutionConfig(job_id=job_id)

    for op in self._topology.keys():
        if isinstance(op, TaskPoolMapOperator):
            config.add_operator(
                TaskPoolOperatorConfig(
                    id=op.id,
                    name=op.name,
                    max_concurrency=op.get_max_concurrency_limit(),
                )
            )
        elif isinstance(op, ActorPoolMapOperator):
            actor_pools = op.get_autoscaling_actor_pools()
            for actor_pool in actor_pools:
                config.add_operator(
                    ActorPoolOperatorConfig(
                        id=op.id,
                        name=op.name,
                        min_size=actor_pool.min_size(),
                        max_size=actor_pool.max_size(),
                        size=actor_pool.current_size(),
                    )
                )

    return config
```

该方法遍历 topology 中的所有 operator，**只处理两种类型**：
- `TaskPoolMapOperator` → 写入 `TaskPoolOperatorConfig`
- `ActorPoolMapOperator` → 写入 `ActorPoolOperatorConfig`

其他所有 operator 类型被跳过。

---

## 三、`.map()` 参数到 Operator 类型的完整推导

### 3.1 决策核心：`get_compute_strategy()`

**文件：** `python/ray/data/_internal/util.py:566-673`

```python
def get_compute_strategy(fn, fn_constructor_args=None, compute=None, concurrency=None):
    from ray.data._internal.compute import ActorPoolStrategy, TaskPoolStrategy
    from ray.data.block import CallableClass

    if isinstance(fn, CallableClass):
        is_callable_class = True
    else:
        is_callable_class = False

    # 分支 1：显式指定 compute
    if compute is not None:
        # 校验 callable class 不能用 TaskPoolStrategy
        # 校验普通函数不能用 ActorPoolStrategy
        return compute

    # 分支 2：指定 concurrency（已废弃，legacy 路径）
    elif concurrency is not None:
        if isinstance(concurrency, tuple):
            if not is_callable_class:
                raise ValueError(...)
            if len(concurrency) == 2:
                return ActorPoolStrategy(min_size=concurrency[0], max_size=concurrency[1])
            else:  # len == 3
                return ActorPoolStrategy(min_size=..., max_size=..., initial_size=...)
        elif isinstance(concurrency, int):
            if is_callable_class:
                return ActorPoolStrategy(size=concurrency)
            else:
                return TaskPoolStrategy(size=concurrency)

    # 分支 3：都不指定，使用默认值
    else:
        if is_callable_class:
            return ActorPoolStrategy(min_size=1, max_size=None)
        else:
            return TaskPoolStrategy()
```

### 3.2 Strategy → Operator 映射

**文件：** `python/ray/data/_internal/execution/operators/map_operator.py:395-447`

```python
@classmethod
def create(cls, ..., compute_strategy=None, ...):
    if compute_strategy is None:
        compute_strategy = TaskPoolStrategy()

    if isinstance(compute_strategy, TaskPoolStrategy):
        return TaskPoolMapOperator(...)

    elif isinstance(compute_strategy, ActorPoolStrategy):
        return ActorPoolMapOperator(...)
```

### 3.3 所有参数组合的结果汇总

#### 3.3.1 `fn` 是 callable class（如 `StreamingVideoProcessMapper`）

| 参数配置 | 返回的 Strategy | 生成的 Operator | 是否写入配置 |
|---------|----------------|-----------------|-------------|
| 无 `compute`，无 `concurrency` | `ActorPoolStrategy(min_size=1, max_size=None)` | `ActorPoolMapOperator` | 是 (`ActorPoolOperatorConfig`) |
| `compute=ActorPoolStrategy(min_size=N, max_size=N)` | `ActorPoolStrategy(min_size=N, max_size=N)` | `ActorPoolMapOperator` | 是 (`ActorPoolOperatorConfig`) |
| `compute=TaskPoolStrategy()` | **raise ValueError** | - | - |
| `concurrency=4`（int） | `ActorPoolStrategy(size=4)` | `ActorPoolMapOperator` | 是 (`ActorPoolOperatorConfig`) |
| `concurrency=(2, 8)`（tuple） | `ActorPoolStrategy(min_size=2, max_size=8)` | `ActorPoolMapOperator` | 是 (`ActorPoolOperatorConfig`) |
| `concurrency=(2, 8, 4)`（3-tuple） | `ActorPoolStrategy(min_size=2, max_size=8, initial_size=4)` | `ActorPoolMapOperator` | 是 (`ActorPoolOperatorConfig`) |

#### 3.3.2 `fn` 是普通函数

| 参数配置 | 返回的 Strategy | 生成的 Operator | 是否写入配置 |
|---------|----------------|-----------------|-------------|
| 无 `compute`，无 `concurrency` | `TaskPoolStrategy()` | `TaskPoolMapOperator` | 是 (`TaskPoolOperatorConfig`) |
| `compute=TaskPoolStrategy(size=N)` | `TaskPoolStrategy(size=N)` | `TaskPoolMapOperator` | 是 (`TaskPoolOperatorConfig`) |
| `compute=ActorPoolStrategy(...)` | **raise ValueError** | - | - |
| `concurrency=4`（int） | `TaskPoolStrategy(size=4)` | `TaskPoolMapOperator` | 是 (`TaskPoolOperatorConfig`) |
| `concurrency=(2, 8)`（tuple） | **raise ValueError** | - | - |

### 3.4 完整代码推导链路示例

**示例 A：callable class + 显式 ActorPoolStrategy**
```
ds.map(StreamingVideoProcessMapper, compute=ActorPoolStrategy(min_size=N, max_size=N))
  → get_compute_strategy(): compute is not None → return compute      [util.py:621]
  → MapRows(compute=ActorPoolStrategy(min_size=N, max_size=N))
  → Planner → plan_udf_map_op()
  → MapOperator.create(compute_strategy=ActorPoolStrategy(...))
  → isinstance(_, ActorPoolStrategy) is True
  → ActorPoolMapOperator                                               [map_operator.py:425-445]
  → _generate_initial_operator_config()
  → isinstance(op, ActorPoolMapOperator) is True                       [streaming_executor.py:437]
  → config.add_operator(ActorPoolOperatorConfig(...))                  ✅ 写入
```

**示例 B：callable class + 不指定任何参数**
```
ds.map(StreamingVideoProcessMapper)
  → get_compute_strategy(): compute=None, concurrency=None
  → is_callable_class=True
  → return ActorPoolStrategy(min_size=1, max_size=None)                [util.py:671]
  → MapRows(compute=ActorPoolStrategy(min_size=1, max_size=None))
  → Planner → plan_udf_map_op()
  → MapOperator.create(compute_strategy=ActorPoolStrategy(...))
  → isinstance(_, ActorPoolStrategy) is True
  → ActorPoolMapOperator                                               [map_operator.py:425-445]
  → _generate_initial_operator_config()
  → isinstance(op, ActorPoolMapOperator) is True                       [streaming_executor.py:437]
  → config.add_operator(ActorPoolOperatorConfig(...))                  ✅ 写入
```

**示例 C：callable class + concurrency=int**
```
ds.map(StreamingVideoProcessMapper, concurrency=4)
  → get_compute_strategy(): compute=None, concurrency=4
  → is_callable_class=True, isinstance(concurrency, int)=True
  → return ActorPoolStrategy(size=4)                                   [util.py:661]
  → ActorPoolMapOperator
  → config.add_operator(ActorPoolOperatorConfig(...))                  ✅ 写入
```

**示例 D：普通函数 + concurrency=int**
```
ds.map(my_func, concurrency=4)
  → get_compute_strategy(): compute=None, concurrency=4
  → is_callable_class=False, isinstance(concurrency, int)=True
  → return TaskPoolStrategy(size=4)                                    [util.py:663]
  → TaskPoolMapOperator(max_concurrency=4)
  → config.add_operator(TaskPoolOperatorConfig(...))                   ✅ 写入
```

---

## 四、不会写入配置的所有情况

### 4.1 外层网关拦截（整个初始化不执行）

| 原因 | 检查方式 |
|------|---------|
| `enable_dynamic_execution_config_sync` 为 False | 检查 DataContext 配置，这是最常见的原因 |
| `job_id` 获取失败 | 日志中出现 `"No job_id or job_submission_id available"` |
| `create_execution_config_store()` 返回 None | store 为 kconf 类型但缺 `infra-framework` 包或缺 key/token |
| 初始化过程中抛异常 | 日志中出现 `"Failed to initialize operator config sync: ..."` |

**验证方式：** 检查日志中是否存在 `"Operator configuration synchronization initialized"` 这条 info 级别日志。如果没有，说明在外层网关就被拦截了。

### 4.2 内层逻辑跳过（operator 类型不匹配）

以下 operator 类型出现在 topology 中时不会被写入配置：

| Operator 类型 | 对应的 Ray Data 操作 | 继承关系 |
|--------------|---------------------|---------|
| `InputDataBuffer` | `ray.data.read_*()` 数据源 | `PhysicalOperator` |
| `AllToAllOperator` | `.repartition()` / `.sort()` / `.random_shuffle()` | `PhysicalOperator` |
| `LimitOperator` | `.limit()` | `OneToOneOperator → PhysicalOperator` |
| `OutputSplitter` | `.streaming_split()` | `PhysicalOperator` |
| `UnionOperator` | `.union()` | `NAryOperator → PhysicalOperator` |
| `ZipOperator` | `.zip()` | `NAryOperator → PhysicalOperator` |
| `AggregateNumRows` | 内部行数聚合 | `PhysicalOperator` |
| `HashAggregateOperator` | `.groupby().count()` 等聚合操作 | `AllToAllOperator → PhysicalOperator` |

**注意：** 对于 `.map()` 操作本身，无论怎么配置参数，最终的 operator 一定是 `TaskPoolMapOperator` 或 `ActorPoolMapOperator` 之一，内层逻辑一定会写入。

### 4.3 `.map()` 参数导致报错（无法执行到写入）

| 参数组合 | 结果 |
|---------|------|
| callable class + `compute=TaskPoolStrategy()` | raise ValueError |
| 普通函数 + `compute=ActorPoolStrategy(...)` | raise ValueError |
| 普通函数 + `concurrency=(2, 8)`（tuple） | raise ValueError |

---

## 五、Config Store 创建逻辑

**文件：** `python/ray/data/_internal/execution/config/store.py:56-117`

```python
def create_execution_config_store(data_context, job_id=None):
    store_type = data_context.execution_config_store_type or "gcs"

    if store_type == "gcs":
        return GcsExecutionConfigStore(gcs_client=..., job_id=job_id)

    if store_type == "memory":
        return MemoryExecutionConfigStore()

    if store_type == "kconf":
        # 可能返回 None 的两种情况：
        # 1. 缺少 infra-framework 包 → ImportError → return None
        # 2. 缺少 kconf_key 或 kconf_token → return None
        ...

    # 未知类型：fallback 到 memory
    return MemoryExecutionConfigStore()
```

支持 3 种 store 类型：
- **gcs**（默认）：使用 Ray GCS 存储
- **memory**：内存存储
- **kconf**：使用 kconf 配置中心，需要额外依赖和配置

---

## 六、Operator 类型继承关系

```
PhysicalOperator
├── InputDataBuffer
├── AggregateNumRows
├── OutputSplitter (+ InternalQueueOperatorMixin)
├── OneToOneOperator
│   ├── LimitOperator
│   └── MapOperator (+ InternalQueueOperatorMixin, ABC)
│       ├── TaskPoolMapOperator     ← 会写入 TaskPoolOperatorConfig
│       └── ActorPoolMapOperator    ← 会写入 ActorPoolOperatorConfig
├── AllToAllOperator (+ InternalQueueOperatorMixin + SubProgressBarMixin)
│   ├── HashShuffleOperator
│   └── HashAggregateOperator
└── NAryOperator
    ├── UnionOperator (+ InternalQueueOperatorMixin)
    └── ZipOperator (+ InternalQueueOperatorMixin)
```

---

## 七、`ActorPoolStrategy` 参数归一化

**文件：** `python/ray/data/_internal/compute.py:109-181`

```python
class ActorPoolStrategy:
    def __init__(self, *, size=None, min_size=None, max_size=None, initial_size=None):
        self.min_size = min_size or 1
        self.max_size = max_size or float("inf")
        self.initial_size = initial_size or self.min_size
```

| 调用方式 | 归一化后的值 | 行为特征 |
|---------|------------|---------|
| `ActorPoolStrategy(size=4)` | min=4, max=4, initial=4 | 固定大小池，不自动扩缩 |
| `ActorPoolStrategy(min_size=2, max_size=8)` | min=2, max=8, initial=2 | 自动扩缩，范围 [2, 8] |
| `ActorPoolStrategy(min_size=1, max_size=None)` | min=1, max=inf, initial=1 | 自动扩缩，无上限 |

固定大小池（`min_size == max_size`）的 autoscaler 会跳过扩缩容决策（`default_actor_autoscaler.py:233-236`）。

---

## 八、结论

### `.map()` 本身总会写入

对于 `.map()` 操作，无论 `compute`、`concurrency` 如何配置（只要不报错），最终 operator 一定是 `TaskPoolMapOperator` 或 `ActorPoolMapOperator`，都在 `_generate_initial_operator_config` 的处理范围内。

### 如果观察到没有写入，问题在外层网关

最大的可能性是 **`enable_dynamic_execution_config_sync` 为 False**，这是第一道门槛，直接 return，后续逻辑全部跳过。

**排查步骤：**
1. 检查日志中是否有 `"Operator configuration synchronization initialized"` → 有则外层通过
2. 检查是否有 `"No job_id or job_submission_id available"` → job_id 获取失败
3. 检查是否有 `"Failed to initialize operator config sync: ..."` → 初始化异常
4. 以上都没有 → 说明 `enable_dynamic_execution_config_sync` 为 False，直接被第一道门槛拦截
