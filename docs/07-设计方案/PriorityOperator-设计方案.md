# PriorityOperator 设计方案（基于 Ray 2.56.1）

## 1. 背景与动机

### 1.1 问题

Ray Data 2.56.1 引入了 `MixOperator`（commit `a68fc8d691` + `a1d1bdedb0`），使用 Deficit-Adjusted Weighted Round-Robin 算法，保证多个输入的**行比例**收敛到目标权重。但该算法有一个根本限制：**当 deficit 最大的输入 buffer 为空时，会阻塞等待，不从其他输入取数据**（`mix_operator.py` `_try_output` 中 `if not self._input_buffers[best_index].has_next(): return`）。

这导致：
- 无法实现"优先消费"语义——高优先级数据可用时永远从高优先级取，高优先级暂无数据时立即 fallback 到次优先级
- 低优先级输入的延迟会拖累整体吞吐（阻塞等待）
- 无法有效支持无界流场景（某个低优先级流可能长时间无数据）

### 1.2 需求

上游有多个 dataset（可能包含无界流），需要：

1. **严格优先级**：高优先级数据可用时，永远优先输出高优先级
2. **非阻塞 fallback**：高优先级暂无数据时，立即从次优先级取数据，不等待
3. **流式处理**：逐 block 增量输出，不依赖任何输入完成
4. **支持无界流**：所有输入均可为无界流，某个输入永不结束时算子仍可持续输出

### 1.3 MixOperator vs PriorityOperator 对比

| 维度 | MixOperator (2.56.1) | PriorityOperator |
|------|---------------------|-----------------|
| 选择算法 | Deficit-Adjusted Weighted Round-Robin | 严格优先级扫描 + 非阻塞 fallback |
| 阻塞行为 | 选中的输入 buffer 空时**阻塞等待** | 选中的输入 buffer 空时**跳过，尝试次优先级** |
| 语义 | 保证行比例收敛到权重 | 保证优先级顺序，高优优先 |
| 确定性 | 是（相同输入到达顺序 → 相同输出顺序） | 条件确定性（见 §3.6） |
| 状态追踪 | `_rows_seen[i]` 累计行数 | 无（不需要 deficit 计算） |
| 适用场景 | 训练数据按比例混合 | 多源优先消费、实时/离线混合 |

---

## 2. 逻辑算子设计：`Priority`

### 2.1 设计原则

完全遵循 2.56.1 的 `Mix` 逻辑算子模式：
- `@dataclass(frozen=True, repr=False, eq=False, init=False)` frozen dataclass
- 继承 `NAry` + `LogicalOperatorUnifiesInputSchemas`（与 `Mix`、`Union` 一致）
- 使用 `object.__setattr__` 设置字段（frozen dataclass 的约束）
- 实现 `_with_new_input_dependencies`（优化器 rewrite 需要）

### 2.2 PriorityStoppingCondition 枚举

文件：`python/ray/data/_internal/logical/operators/n_ary_operator.py`

```python
@PublicAPI(stability="alpha")
class PriorityStoppingCondition(enum.Enum):
    """Controls when a priority pipeline terminates.

    DRAIN_ALL: Pipeline runs until ALL inputs are exhausted.
        Lower-priority inputs are only consumed when higher-priority
        inputs have no data available.
    STOP_ON_HIGHEST: Pipeline ends when the highest-priority input
        is exhausted, regardless of remaining data in lower-priority inputs.
    """

    DRAIN_ALL = "drain_all"
    STOP_ON_HIGHEST = "stop_on_highest"
```

**设计考量**：
- `DRAIN_ALL`（默认）：所有输入的数据都会被消费，只是按优先级顺序输出。适合"高优先级优先，但低优先级也要处理完"的场景
- `STOP_ON_HIGHEST`：最高优先级输入耗尽即停，低优先级数据被丢弃。适合"高优先级是核心业务数据，低优先级是补充/兜底数据"的场景

### 2.3 Priority 逻辑算子

```python
@dataclass(frozen=True, repr=False, eq=False, init=False)
class Priority(NAry, LogicalOperatorUnifiesInputSchemas):
    """Logical operator for priority-based dataset mixing.

    Inputs are ordered by priority: input_dependencies[0] has the highest
    priority, input_dependencies[-1] has the lowest.
    """

    _name: str = field(init=False, repr=False)
    _input_dependencies: List[LogicalOperator] = field(init=False, repr=False)
    _num_outputs: Optional[int] = field(init=False, default=None, repr=False)
    stopping_condition: PriorityStoppingCondition = field(init=False, repr=False)

    def __init__(
        self,
        *input_ops: LogicalOperator,
        stopping_condition: PriorityStoppingCondition = PriorityStoppingCondition.DRAIN_ALL,
    ):
        for input_op in input_ops:
            assert isinstance(input_op, LogicalOperator), input_op
        object.__setattr__(self, "_name", self.__class__.__name__)
        object.__setattr__(self, "_input_dependencies", list(input_ops))
        object.__setattr__(self, "_num_outputs", None)
        object.__setattr__(self, "stopping_condition", stopping_condition)

    def estimated_num_outputs(self) -> Optional[int]:
        if self.stopping_condition == PriorityStoppingCondition.STOP_ON_HIGHEST:
            return self.input_dependencies[0].estimated_num_outputs()

        # DRAIN_ALL: sum of all inputs' estimated outputs
        total = 0
        for dep in self.input_dependencies:
            n = dep.estimated_num_outputs()
            if n is None:
                return None
            total += n
        return total

    def _with_new_input_dependencies(
        self, input_dependencies: List[LogicalOperator]
    ) -> LogicalOperator:
        return self.__class__(
            *input_dependencies,
            stopping_condition=self.stopping_condition,
        )
```

**与 2.56.1 `Mix` 的对比**：

| 维度 | Mix | Priority |
|------|-----|----------|
| 继承 | `NAry, LogicalOperatorUnifiesInputSchemas` | 相同 |
| 额外字段 | `weights: List[float]`, `stopping_condition` | 仅 `stopping_condition` |
| `estimated_num_outputs` | STOP_ON_SHORTEST → None；STOP_ON_LONGEST_DROP → sum | STOP_ON_HIGHEST → deps[0]；DRAIN_ALL → sum |
| `_with_new_input_dependencies` | 传递 weights + stopping_condition | 仅传递 stopping_condition |

**为什么继承 `LogicalOperatorUnifiesInputSchemas`**：与 Mix/Union 一致，Priority 的输出 schema 是所有输入 schema 的统一（unify）。该 mixin 提供默认的 `infer_schema()` 实现，通过 `unify_schemas_with_validation` 合并各输入 schema。

---

## 3. 物理算子设计：`PriorityOperator`

### 3.1 类定义

文件：`python/ray/data/_internal/execution/operators/priority_operator.py`（新增）

```python
class PriorityOperator(InternalQueueOperatorMixin, NAryOperator):
    """An operator that merges blocks from multiple input operators into
    a single output stream using strict priority ordering.

    When multiple inputs have data available, the highest-priority input
    is always selected. When the highest-priority input's buffer is empty,
    the operator immediately falls back to the next-highest-priority input
    with available data — it never blocks waiting for a specific input.

    Inputs are ordered by priority: input_dependencies[0] has the highest
    priority, input_dependencies[-1] has the lowest.
    """
```

### 3.2 继承体系

```
PhysicalOperator
├── InternalQueueOperatorMixin  (内部队列接口: _input_queues, _output_queues)
│   ├── AllToAllOperator
│   ├── OutputSplitter
│   ├── MixOperator            (2.56.1)
│   └── PriorityOperator       ← NEW
└── NAryOperator               (多输入依赖)
    ├── UnionOperator
    ├── ZipOperator
    ├── MixOperator            (2.56.1)
    └── PriorityOperator       ← NEW (多重继承: InternalQueueOperatorMixin + NAryOperator)
```

与 MixOperator 完全一致的继承结构，复用 executor 的所有基础设施（队列指标、backpressure、完成判定等）。

### 3.3 构造函数

```python
def __init__(
    self,
    data_context: DataContext,
    *input_ops: PhysicalOperator,
    stopping_condition: PriorityStoppingCondition = PriorityStoppingCondition.DRAIN_ALL,
):
    assert len(input_ops) >= 1

    self._stopping_condition = stopping_condition

    self._input_buffers: List[BaseBundleQueue] = [
        FIFOBundleQueue() for _ in range(len(input_ops))
    ]
    self._output_buffer: BaseBundleQueue = FIFOBundleQueue()

    self._input_done_flags: List[bool] = [False] * len(input_ops)
    self._stopped: bool = False

    self._stats: StatsDict = {"Priority": []}

    input_names = ", ".join([op._name for op in input_ops])
    name = f"Priority({input_names})"
    super().__init__(data_context, *input_ops, name=name)
```

**关键设计决策**：
- **不需要 weights**：优先级完全由输入顺序决定（index 0 = 最高优先级），无需额外的权重参数
- **不需要 `_rows_seen`**：不追踪行数，不做 deficit 计算，简化状态（对比 MixOperator 的 `self._rows_seen: List[int]`）

### 3.4 核心算法：`_select_highest_priority_available`

```python
def _select_highest_priority_available(self) -> int:
    """Select the highest-priority input that has data available.

    Scans inputs in priority order (index 0 = highest).
    Returns -1 if all inputs are exhausted.
    Returns -2 if no exhausted input has data (should wait for more).
    """
    all_exhausted = True
    for i in range(len(self._input_buffers)):
        if self._is_input_exhausted(i):
            continue
        all_exhausted = False
        if self._input_buffers[i].has_next():
            return i
    if all_exhausted:
        return -1
    return -2
```

**算法说明**：
1. 从 index 0（最高优先级）开始扫描
2. 跳过已耗尽的输入（done + buffer 空）
3. 第一个**未耗尽且有数据**的输入即为选中输入
4. 如果所有输入都耗尽 → 返回 -1（算子结束）
5. 如果存在未耗尽输入但 buffer 都空 → 返回 -2（等待更多数据到达，**不阻塞**）

**与 MixOperator `_select_most_behind_input` 的关键区别**：

| MixOperator | PriorityOperator |
|-------------|-----------------|
| 计算 `gap = weight[i] * total - rows_seen[i]` | 无 deficit，按 index 顺序扫描 |
| 返回 deficit 最大的输入 index | 返回优先级最高且有数据的 index |
| 选中的输入 buffer 空时**等待**（`return`） | 选中的输入 buffer 空时**跳过**（继续扫描） |

### 3.5 `_try_output` 实现

```python
def _try_output(self) -> None:
    """Move blocks from input buffers to the output buffer.

    On each iteration, selects the highest-priority input with data
    available. If no input has data but some are not yet exhausted,
    we wait (return) rather than blocking — the next call to
    _add_input_inner or input_done will trigger another attempt.
    """
    if self._stopped:
        return

    while True:
        if self._stopping_condition == PriorityStoppingCondition.STOP_ON_HIGHEST:
            if self._is_input_exhausted(0):
                self._stopped = True
                self.mark_execution_finished()
                return

        selected = self._select_highest_priority_available()
        if selected == -1:
            # All inputs exhausted
            if not self._stopped:
                self._stopped = True
                self.mark_execution_finished()
            return
        if selected == -2:
            # Some inputs not exhausted but no data available yet
            return

        bundle = self._input_buffers[selected].get_next()
        self._metrics.on_input_dequeued(bundle, input_index=selected)
        self._output_buffer.add(bundle)
        self._metrics.on_output_queued(bundle)
```

**核心语义**：
- 每次 `_try_output` 循环中，从优先级最高且有数据的输入取一个 RefBundle 放入 output_buffer
- 当所有未耗尽输入的 buffer 都空时，`return` 退出（等下一次 `_add_input_inner` 触发新的 `_try_output`）
- **绝不阻塞等待**某个特定输入

**对比 MixOperator `_try_output`**：MixOperator 在选中的输入 buffer 空时 `return`（等待该输入）；PriorityOperator 在高优 buffer 空时**继续扫描次优**，只有所有未耗尽输入都空时才 `return`。

### 3.6 确定性分析

PriorityOperator 的输出顺序**不是无条件确定性的**——它取决于数据到达时机。

**条件确定性**：
- 如果高优先级输入的数据始终先于低优先级到达，则输出是完全确定性的（永远从高优先级取）
- 如果数据到达顺序不确定，则输出顺序也不确定，但**优先级语义始终成立**：只要有高优先级数据可用，就永远不输出低优先级数据

**这是设计意图**：优先消费的语义本身就意味着"谁先来谁优先（在优先级约束下）"，而非 MixOperator 的"严格按比例分配"。

### 3.7 完整接口实现

接口实现完全遵循 2.56.1 MixOperator 的模式，包括 `# ---` section 注释分隔：

```python
# ------------------------------------------------------------------
# InternalQueueOperatorMixin interface
# ------------------------------------------------------------------

@property
@override
def _input_queues(self) -> List[BaseBundleQueue]:
    return self._input_buffers

@property
@override
def _output_queues(self) -> List[BaseBundleQueue]:
    return [self._output_buffer]

# ------------------------------------------------------------------
# PhysicalOperator interface
# ------------------------------------------------------------------

@override
def mark_execution_finished(self) -> None:
    # Override InternalQueueOperatorMixin's version to preserve the
    # output buffer for draining. Only clear input queues.
    PhysicalOperator.mark_execution_finished(self)
    self.clear_internal_input_queue()

@override
def _add_input_inner(self, refs: RefBundle, input_index: int) -> None:
    assert not self.has_completed()
    assert 0 <= input_index < len(self._input_dependencies), input_index
    if self._stopped:
        return
    self._input_buffers[input_index].add(refs)
    self._metrics.on_input_queued(refs, input_index=input_index)
    self._try_output()

@override
def input_done(self, input_index: int) -> None:
    self._input_done_flags[input_index] = True
    self._try_output()

@override
def all_inputs_done(self) -> None:
    super().all_inputs_done()
    self._try_output()

@override
def has_next(self) -> bool:
    return len(self._output_buffer) > 0

@override
def _get_next_inner(self) -> RefBundle:
    refs = self._output_buffer.get_next()
    self._metrics.on_output_dequeued(refs)
    return refs

@override
def num_outputs_total(self) -> Optional[int]:
    if self._stopping_condition == PriorityStoppingCondition.STOP_ON_HIGHEST:
        return self.input_dependencies[0].num_outputs_total()
    total = 0
    for dep in self.input_dependencies:
        n = dep.num_outputs_total()
        if n is None:
            return None
        total += n
    return total

@override
def num_output_rows_total(self) -> Optional[int]:
    if self._stopping_condition == PriorityStoppingCondition.STOP_ON_HIGHEST:
        return self.input_dependencies[0].num_output_rows_total()
    total = 0
    for dep in self.input_dependencies:
        n = dep.num_output_rows_total()
        if n is None:
            return None
        total += n
    return total

@override
def get_stats(self) -> StatsDict:
    return self._stats

@override
def throttling_disabled(self) -> bool:
    # TODO: Disable throttling along with Union once NAry operator resource accounting is fixed.
    return False

# ------------------------------------------------------------------
# Output selection
# ------------------------------------------------------------------

def _is_input_exhausted(self, index: int) -> bool:
    """An input is exhausted when it's done and its buffer is empty."""
    return (
        self._input_done_flags[index]
        and not self._input_buffers[index].has_next()
    )
```

**与 MixOperator 的 `mark_execution_finished` 一致**：Override `InternalQueueOperatorMixin` 的默认实现（会同时清空 input 和 output queue），仅清空 input queue，保留 output buffer 供下游 draining。

---

## 4. 公共 API 设计

### 4.1 `Dataset.priority_mix()`

文件：`python/ray/data/dataset.py`

遵循 2.56.1 `mix()` 的 API 风格（`@PublicAPI(stability="alpha", api_group=SMJ_API_GROUP)`、docstring 格式、Dataset 构造方式）：

```python
@PublicAPI(stability="alpha", api_group=SMJ_API_GROUP)
def priority_mix(
    self,
    *other: "Dataset",
    stopping_condition: PriorityStoppingCondition = PriorityStoppingCondition.DRAIN_ALL,
) -> "Dataset":
    """Mix this dataset with others using strict priority ordering.

    This is a streaming operator that merges blocks from multiple input
    datasets into a single output stream, respecting strict priority
    ordering. This dataset has the highest priority; subsequent datasets
    have decreasing priority. When the highest-priority dataset has data
    available, it's always consumed first. When it's temporarily empty,
    lower-priority datasets are consumed as fallback — the operator
    never blocks waiting for a specific input.

    Unlike :meth:`~ray.data.Dataset.mix`, which guarantees proportional
    output ratios via weighted round-robin, ``priority_mix()`` guarantees
    priority ordering — high-priority data is always output before
    low-priority data when available.

    .. caution::
        Priority-mixed datasets aren't lineage-serializable. As a result,
        they can't be used as a tunable hyperparameter in Ray Tune.

    Examples:

        >>> import ray
        >>> ds_high = ray.data.from_items([{"x": 1}, {"x": 2}])
        >>> ds_low = ray.data.from_items([{"x": 3}, {"x": 4}])
        >>> ds = ds_high.priority_mix(ds_low)
        >>> list(ds.iter_batches(batch_size=2))  # doctest: +SKIP
        [{'x': [1, 2]}]

    Args:
        *other: Lower-priority datasets, in decreasing priority order.
            All datasets must produce the same schema.
        stopping_condition: Controls when the pipeline terminates.
            See :class:`~ray.data.PriorityStoppingCondition` for options.
            Defaults to ``DRAIN_ALL``.

    Returns:
        A new dataset whose rows are merged from the input datasets
        according to strict priority ordering.
    """
    datasets = [self] + list(other)

    start_time = time.perf_counter()

    logical_plans = [ds._logical_plan for ds in datasets]
    op = PriorityLogicalOperator(
        *[plan.dag for plan in logical_plans],
        stopping_condition=stopping_condition,
    )
    logical_plan = LogicalPlan(op, self.context)

    stats = DatasetStats(
        metadata={"Priority": []},
        parent=[d._raw_stats() for d in datasets],
    )
    stats.time_total_s = time.perf_counter() - start_time
    return Dataset(
        logical_plan,
        self.context.copy(),
        stats,
    )
```

### 4.2 import 语句

文件：`python/ray/data/dataset.py` 的 import 块（与 2.56.1 `Mix` 的 import 模式一致）：

```python
from ray.data._internal.logical.operators import (
    Count,
    Filter,
    FlatMap,
    InputData,
    Join,
    Limit,
    MapBatches,
    MapRows,
    Priority as PriorityLogicalOperator,
    PriorityStoppingCondition,
    Project,
    RandomizeBlocks,
    RandomShuffle,
    Repartition,
    Sort,
    StreamingRepartition,
    StreamingSplit,
    Union as UnionLogicalOperator,
    Write,
    Zip,
)
```

### 4.3 导出 PriorityStoppingCondition

文件：`python/ray/data/__init__.py`（与 2.56.1 `MixStoppingCondition` 的导出模式一致）：

```python
from ray.data._internal.logical.operators.n_ary_operator import (
    PriorityStoppingCondition,
)

__all__ = [
    ...
    "PriorityStoppingCondition",
    ...
]
```

---

## 5. Planner 注册

文件：`python/ray/data/_internal/planner/planner.py`

### 5.1 import

```python
from ray.data._internal.execution.operators.priority_operator import PriorityOperator
from ray.data._internal.logical.operators import (
    ...
    Priority,
    ...
)
```

### 5.2 plan 函数

```python
def plan_priority_op(logical_op, physical_children, data_context):
    assert len(physical_children) >= 1
    return PriorityOperator(
        data_context,
        *physical_children,
        stopping_condition=logical_op.stopping_condition,
    )
```

### 5.3 注册

```python
_DEFAULT_PLAN_FNS = {
    ...
    Mix: plan_mix_op,
    Priority: plan_priority_op,   # ← NEW
    Union: plan_union_op,
    ...
}
```

---

## 6. logical operators `__init__.py` 导出

文件：`python/ray/data/_internal/logical/operators/__init__.py`

```python
from ray.data._internal.logical.operators.n_ary_operator import (
    Mix,
    MixStoppingCondition,
    NAry,
    Priority,                # ← NEW
    PriorityStoppingCondition,  # ← NEW
    Union,
    Zip,
)

__all__ = [
    ...
    "Mix",
    "MixStoppingCondition",
    "NAry",
    "Priority",              # ← NEW
    "PriorityStoppingCondition",  # ← NEW
    ...
]
```

---

## 7. 与优化规则的兼容性

### 7.1 Operator Fusion

2.56.1 的 `operator_fusion.py` **不支持 NAry 算子的融合**（只融合 OneToOneOperator 链）。MixOperator 和 UnionOperator 都不参与融合。PriorityOperator 同理，**不受融合规则影响**。

### 7.2 Predicate Push-Down

`Priority` 继承 `LogicalOperatorUnifiesInputSchemas`，但**不**继承 `LogicalOperatorSupportsPredicatePassThrough`。这意味着 filter 不会被推入 Priority 的各输入分支——这是正确的，因为推入 filter 会改变各输入的可用性时序，进而影响优先级选择的正确性。

对比：`Union` 继承 `LogicalOperatorSupportsPredicatePassThrough` 并返回 `PUSH_INTO_BRANCHES`，因为 Union 的输出顺序与各分支的数据到达时序无关。Priority 则相反，优先级语义依赖数据到达时序，**不能推入 filter**。

### 7.3 Backpressure

PriorityOperator 的 `throttling_disabled()` 返回 `False`，与 MixOperator 一致，参与 executor 的标准 backpressure 机制。

### 7.4 Resource Accounting

`InternalQueueOperatorMixin` 提供的队列大小指标（`internal_input_queue_num_blocks/bytes`, `internal_output_queue_num_blocks/bytes`）自动被 executor 的 `OpState` 使用，无需额外处理。

### 7.5 Schema Unification

继承 `LogicalOperatorUnifiesInputSchemas` 后，`infer_schema()` 自动通过 `unify_schemas_with_validation` 合并各输入 schema。与 Mix/Union 行为一致——所有输入必须产生兼容的 schema。

---

## 8. 无界流支持

### 8.1 所有输入均可为无界流

PriorityOperator 的核心算法是"按优先级顺序扫描，第一个有数据的输入就取"。这个机制对流类型没有依赖：

| 场景 | 行为 | 是否正确 |
|------|------|---------|
| 全部无界流 | 高优无界流永远被优先，低优仅在高优 buffer 暂空时被消费 | 正确 |
| 全部有界流 | 高优优先消费，耗尽后 fallback 到次优，最终全部耗尽停止 | 正确 |
| 高优有界 + 低优无界 | 高优耗尽后 fallback 到无界低优流，持续输出 | 正确 |
| 高优无界 + 低优有界 | 高优永远优先，低优仅在高优 buffer 暂空时被消费 | 正确 |

**关键点**：
- **不依赖任何输入"结束"**：算法仅检查 buffer 是否有数据 + 输入是否 done
- **无界流永不触发 `input_done`**：所以该输入永远不会被标记为 exhausted，算法只是在其 buffer 空时跳过，有数据时继续取

### 8.2 DRAIN_ALL + 无界流

当某个高优先级输入是无界流时：
- 算子永远优先输出该高优先级数据
- 低优先级输入仅在高优先级 buffer 暂空时被消费（可能永远不被消费）
- 算子**不会停止**（高优先级输入永不耗尽）

**这是正确行为**：优先消费语义下，如果高优先级源源不断，低优先级自然排不上。

### 8.3 STOP_ON_HIGHEST + 无界流

如果高优先级输入是无界流，`STOP_ON_HIGHEST` 意味着算子永不停止——除非用户外部中断。这是无界流的正常行为。

### 8.4 与 Mix 的无界流对比

| Mix 停止条件 | 无界流支持 |
|-------------|-----------|
| STOP_ON_SHORTEST | 支持（最短输入耗竭即停） |
| STOP_ON_LONGEST_DROP | **不支持**（最长输入永不耗尽，其他输入 drop out 后仍不停） |

| Priority 停止条件 | 无界流支持 |
|------------------|-----------|
| DRAIN_ALL | 支持（高优无界时低优可能不被消费，但不报错；高优有界则正常 drain 所有） |
| STOP_ON_HIGHEST | 高优无界时不停止（正常行为）；高优有界时停于高优耗尽 |

---

## 9. 文件变更清单

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `python/ray/data/_internal/logical/operators/n_ary_operator.py` | 修改 | 添加 `PriorityStoppingCondition` 枚举和 `Priority` 逻辑算子 |
| `python/ray/data/_internal/logical/operators/__init__.py` | 修改 | 导出 `Priority`, `PriorityStoppingCondition` |
| `python/ray/data/_internal/execution/operators/priority_operator.py` | **新增** | `PriorityOperator` 物理算子 |
| `python/ray/data/_internal/planner/planner.py` | 修改 | 添加 `plan_priority_op` 和注册 |
| `python/ray/data/dataset.py` | 修改 | 添加 `priority_mix()` 方法 + import |
| `python/ray/data/__init__.py` | 修改 | 导出 `PriorityStoppingCondition` |
| `python/ray/data/tests/test_priority_operator.py` | **新增** | 单元测试 |

---

## 10. 测试策略

### 10.1 单元测试（`test_priority_operator.py`）

```python
class TestPriorityOperator:

    def test_priority_ordering(self):
        """高优有数据时永远从高优取"""
        # 3 个输入，index 0 最高优先级
        # 所有输入都有数据 → 输出全部来自 index 0，然后 index 1，最后 index 2

    def test_fallback_on_empty_high_priority(self):
        """高优 buffer 空时立即 fallback"""
        # 输入 0 空，输入 1 有数据 → 输出来自输入 1
        # 输入 0 后续到达数据 → 恢复从输入 0 取

    def test_no_blocking_all_low_when_high_empty(self):
        """不阻塞等待空 buffer"""
        # 输入 0 暂无数据但未 done → 跳过，从输入 1 取
        # 对比 MixOperator 会阻塞等待输入 0

    def test_drain_all(self):
        """DRAIN_ALL 模式消费所有输入"""
        # 所有输入数据最终都被输出

    def test_stop_on_highest(self):
        """STOP_ON_HIGHEST 模式在高优耗尽后停止"""
        # 输入 0 耗尽 → 算子停止，即使输入 1 仍有数据

    def test_stop_on_highest_ignores_low_data(self):
        """STOP_ON_HIGHEST 模式下低优数据被丢弃"""
        # 先到达低优数据，再到达高优数据
        # 高优耗尽后，即使低优仍有数据也不输出

    def test_exhausted_input_skipped(self):
        """已耗尽的输入不再被选择"""
        # 输入 0 done + buffer 空 → 自动跳过

    def test_single_input(self):
        """单输入退化为直接传递"""
        # 行为等价于直接传递

    def test_multiple_inputs_interleaved_arrival(self):
        """多个输入交错到达时的优先级行为"""
        # 模拟流式到达：高优 block 1 → 低优 block 1 → 高优 block 2 → ...
        # 验证每次都从有数据的最高优先级输入取

    def test_three_inputs_priority_order(self):
        """3 个输入的优先级顺序"""

    def test_num_outputs_total_drain_all(self):
        """DRAIN_ALL 模式下 num_outputs_total = sum"""

    def test_num_outputs_total_stop_on_highest(self):
        """STOP_ON_HIGHEST 模式下 num_outputs_total = deps[0]"""

    def test_stats_key(self):
        """stats dict 包含 "Priority" key"""

    def test_name_contains_inputs(self):
        """算子名称包含 "Priority" 和输入名称"""

    def test_add_input_after_stopped(self):
        """算子停止后 add_input 不报错（静默忽略）"""

    def test_internal_queue_metrics(self):
        """internal queue 指标正常工作"""
```

### 10.2 核心不变量

```python
def test_priority_invariant(bundle_sequence):
    """核心不变量：输出时，如果高优先级输入 buffer 非空，
    则输出必须来自高优先级输入"""
    # 对比 MixOperator 的不变量：输出行比例收敛到权重
```

### 10.3 集成测试

```python
def test_priority_mix_end_to_end(ray_start_regular_shared):
    """端到端测试：通过 Dataset.priority_mix() API 运行完整 pipeline"""
    ds_high = ray.data.from_items([{"x": i} for i in range(10)])
    ds_low = ray.data.from_items([{"x": i} for i in range(20, 25)])
    result = ds_high.priority_mix(ds_low)
    output = result.take_all()
    # 高优数据全部在前
    assert [r["x"] for r in output[:10]] == list(range(10))
```

---

## 11. 数据流示意

```
Dataset A (高优先级, 无界流) ──┐
                                │  PriorityOperator
Dataset B (中优先级, 有界流) ──┤  严格优先级扫描
                                │  高优可用 → 取高优
Dataset C (低优先级, 有界流) ──┘  高优暂无 → fallback 次优
                                ↓
输出: [A block] [A block] [A block] [B block] [A block] [A block] ...
       ↑ A 有数据时全从 A 取     ↑ A 暂无数据时 fallback 到 B

当 A 耗尽（DRAIN_ALL）:
输出: ... [A block] [B block] [B block] [C block] [C block] [C block]
                      ↑ A 耗尽后从 B 取     ↑ B 耗尽后从 C 取
```

---

## 12. 扩展考虑

### 12.1 动态优先级调整（Future）

当前设计优先级在构建时固定。未来可扩展为支持动态优先级回调：

```python
def priority_mix(
    self,
    *other: "Dataset",
    priority_fn: Optional[Callable[[List[int]], int]] = None,
    ...
)
```

`priority_fn` 接收各输入的 buffer 状态，返回应选择的输入 index。默认实现即当前严格优先级。

### 12.2 优先级 + 权重混合（Future）

某些场景可能需要"优先级组内按权重混合"：

```python
# 同一优先级组内按权重混合，组间按优先级
ds.priority_mix(
    ds_a, ds_b,                    # 优先级 1 组
    ds_c,                          # 优先级 2
    group_weights=[[0.7, 0.3]],   # 组内权重
)
```

当前方案不包含此特性，留作后续迭代。

---

## 13. 实现步骤

| 步骤 | 文件 | 工作量 |
|------|------|--------|
| 1. 添加 `PriorityStoppingCondition` 枚举 | `n_ary_operator.py` | 0.5h |
| 2. 添加 `Priority` 逻辑算子（frozen dataclass + `LogicalOperatorUnifiesInputSchemas`） | `n_ary_operator.py` | 1h |
| 3. 导出 `Priority`, `PriorityStoppingCondition` | `operators/__init__.py` | 0.5h |
| 4. 实现 `PriorityOperator` 物理算子 | `priority_operator.py`（新文件） | 2h |
| 5. 注册 `plan_priority_op` | `planner.py` | 0.5h |
| 6. 添加 `priority_mix()` API | `dataset.py` | 1h |
| 7. 导出 `PriorityStoppingCondition` | `__init__.py` | 0.5h |
| 8. 单元测试 | `test_priority_operator.py`（新文件） | 3h |
| 9. 集成测试 + 文档 | - | 2h |
| **合计** | | **~11h** |
