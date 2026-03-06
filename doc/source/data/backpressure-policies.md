# Ray Data 反压策略详细文档

## 概述

Ray Data 使用流式执行器（StreamingExecutor）来执行数据处理 DAG。为了防止内存溢出和优化吞吐量，Ray Data 实现了多种反压策略（Backpressure Policy）来控制任务调度和数据流动。

反压策略的核心作用是在数据处理管道中，当下游处理速度跟不上上游生产速度时，限制上游算子的任务调度，从而避免内存积压。

## 反压策略架构

### 基类：BackpressurePolicy

所有反压策略都继承自 `BackpressurePolicy` 基类（位于 `python/ray/data/_internal/execution/backpressure_policy/backpressure_policy.py`），该基类定义了两个核心接口：

```python
class BackpressurePolicy(ABC):
    """Interface for back pressure policies."""

    def __init__(
        self,
        data_context: DataContext,
        topology: "Topology",
        resource_manager: "ResourceManager",
    ):
        self._data_context = data_context
        self._topology = topology
        self._resource_manager = resource_manager

    @property
    def name(self) -> str:
        """Human-readable name for UX/progress bar display."""
        return type(self).__name__

    def can_add_input(self, op: "PhysicalOperator") -> bool:
        """判断是否可以向算子添加新输入。

        如果返回 False，算子将被反压，无法运行新任务。
        用于 `streaming_executor_state.py::select_operator_to_run()` 中。

        注意：如果多个反压策略同时启用，只要任意一个策略返回 False，
        算子就会被反压。
        """
        return True

    def max_task_output_bytes_to_read(self, op: "PhysicalOperator") -> Optional[int]:
        """返回给定算子可以从运行中任务读取的最大输出字节数。

        None 表示无限制。用于输出反压，限制算子从运行中的任务读取数据的数量。

        注意：如果多个反压策略对同一算子返回非 None 值，
        将使用这些值中的最小值作为限制。
        """
        return None
```

### 默认启用的策略

Ray Data 默认启用以下三种反压策略（定义在 `backpressure_policy/__init__.py`）：

```python
ENABLED_BACKPRESSURE_POLICIES = [
    ConcurrencyCapBackpressurePolicy,
    ResourceBudgetBackpressurePolicy,
    DownstreamCapacityBackpressurePolicy,
]
ENABLED_BACKPRESSURE_POLICIES_CONFIG_KEY = "backpressure_policies.enabled"
```

**重要**：当多个反压策略同时启用时，只要任意一个策略返回 `False`，算子就会被反压。

---

## 策略详解

### 1. ConcurrencyCapBackpressurePolicy（并发上限反压策略）

**位置**：`python/ray/data/_internal/execution/backpressure_policy/concurrency_cap_backpressure_policy.py`

**显示名称**：`ConcurrencyCap`

#### 核心思想
动态限制每个算子的并发任务数，基于输出队列的增长率进行调整。通过监控队列大小的变化趋势，自适应地调整并发上限。


#### 完整计算逻辑

使用 **非对称指数移动平均 (Asymmetric EWMA)** 算法：

**1. EWMA 更新公式**（`_update_ewma_asymmetric` 方法）：

```python
def _update_ewma_asymmetric(self, prev_value: float, sample: float) -> float:
    if prev_value <= 0:
        return sample
    # 快速上升，缓慢下降
    alpha = EWMA_ALPHA_UP if sample > prev_value else EWMA_ALPHA
    return (1 - alpha) * prev_value + alpha * sample
```

其中：
- `EWMA_ALPHA = 0.1`（慢速下降因子）
- `EWMA_ALPHA_UP = 1.0 - (1.0 - EWMA_ALPHA)^2 = 0.19`（快速上升因子）

**2. Level 和 Dev 更新**（`_update_level_and_dev` 方法）：

```python
def _update_level_and_dev(self, op, q_bytes):
    q = float(q_bytes)
    level_prev = self._q_level_nbytes[op]
    dev_prev = self._q_level_dev[op]

    # 偏差样本：相对于前一个 level 的绝对残差
    dev_sample = abs(q - level_prev) if level_prev > 0 else 0.0
    dev = self._update_ewma_asymmetric(dev_prev, dev_sample)

    # 更新 level
    level = self._update_ewma_asymmetric(level_prev, q)

    self._q_level_nbytes[op] = level
    self._q_level_dev[op] = dev
```

**3. 死区 (Deadband) 计算**：

```
下限 (lower) = level - K_DEV × dev
上限 (upper) = level + K_DEV × dev
```

**4. 有效并发上限计算**（`_effective_cap` 方法）：

```python
def _effective_cap(self, op, num_tasks_running, current_queue_size_bytes):
    cap_cfg = self._concurrency_caps[op]  # 配置的最大并发

    level = float(self._q_level_nbytes[op])
    dev = max(1.0, float(self._q_level_dev[op]))  # 确保 dev >= 1
    upper = level + K_DEV * dev
    lower = level - K_DEV * dev

    if current_queue_size_bytes > upper:
        # 回退：减少并发
        target = num_tasks_running - BACKOFF_FACTOR
    elif current_queue_size_bytes < lower:
        # 增加：提高并发
        target = num_tasks_running + RAMPUP_FACTOR
    else:
        # 保持：维持当前并发
        target = num_tasks_running

    # 限制在 [1, configured_cap] 范围内
    target = max(1, target)
    if not math.isinf(cap_cfg):
        target = min(target, int(cap_cfg))
    return int(target)
```

#### 配置参数

| 参数 | 环境变量 | 默认值 | 说明 |
|------|---------|--------|------|
| EWMA_ALPHA | `RAY_DATA_CONCURRENCY_CAP_EWMA_ALPHA` | 0.1 | EWMA 平滑因子（慢降） |
| EWMA_ALPHA_UP | 自动计算 | 0.19 | EWMA 快速上升因子，`1-(1-EWMA_ALPHA)^2` |
| K_DEV | `RAY_DATA_CONCURRENCY_CAP_K_DEV` | 1.0 | 死区宽度系数 |
| BACKOFF_FACTOR | `RAY_DATA_CONCURRENCY_CAP_BACKOFF_FACTOR` | 1 | 回退因子 |
| RAMPUP_FACTOR | `RAY_DATA_CONCURRENCY_CAP_RAMPUP_FACTOR` | 1 | 增加因子 |
| AVAILABLE_OBJECT_STORE_BUDGET_THRESHOLD | `RAY_DATA_CONCURRENCY_CAP_AVAILABLE_OBJECT_STORE_BUDGET_THRESHOLD` | 0.1 | 跳过动态反压的 Object Store 预算阈值 |

#### 反压判断流程 (`can_add_input` 方法)

```python
def can_add_input(self, op):
    num_tasks_running = op.metrics.num_tasks_running

    # 跳过条件检查
    if (
        not isinstance(op, MapOperator)
        or not self._resource_manager.is_op_eligible(op)
        or not self.enable_dynamic_output_queue_size_backpressure
        or self._resource_manager._is_blocking_materializing_op(op)
    ):
        return num_tasks_running < self._concurrency_caps[op]

    # 检查 Object Store 可用预算比例
    available_budget_fraction = get_available_object_store_budget_fraction(
        self._resource_manager, op, consider_downstream_ineligible_ops=True
    )
    if (
        available_budget_fraction is not None
        and available_budget_fraction > AVAILABLE_OBJECT_STORE_BUDGET_THRESHOLD
    ):
        # 预算充足，跳过动态反压
        return num_tasks_running < self._concurrency_caps[op]

    # 计算当前队列大小
    current_queue_size_bytes = (
        self._resource_manager.get_mem_op_internal(op)
        + self._resource_manager.get_mem_op_outputs(op, include_ineligible_downstream=True)
    )

    # 更新 EWMA 状态并计算有效并发上限
    self._update_level_and_dev(op, current_queue_size_bytes)
    effective_cap = self._effective_cap(op, num_tasks_running, current_queue_size_bytes)

    return num_tasks_running < effective_cap
```

#### 跳过反压的条件

1. 算子不是 `MapOperator`
2. 算子不符合反压资格（`is_op_eligible` 返回 False）
3. 动态输出队列大小反压被禁用（`enable_dynamic_output_queue_size_backpressure = False`）
4. 下游是需要完全物化的算子（如 `AllToAllOperator`）
5. Object Store 可用预算比例 > 阈值（默认 0.1）

#### 注意事项

- **仅支持 `TaskPoolMapOperator`**：只有该类型的算子会应用动态并发上限
- **非对称 EWMA**：队列大小上升时快速响应，下降时缓慢调整，避免过度波动
- **最小并发为 1**：即使需要回退，也保证至少有一个任务可以运行

---

### 2. ResourceBudgetBackpressurePolicy（资源预算反压策略）

**位置**：`python/ray/data/_internal/execution/backpressure_policy/resource_budget_backpressure_policy.py`

**显示名称**：`ResourceBudget`

#### 核心思想

基于 ResourceManager 中的资源预算进行反压控制。这是最基础的反压策略，直接委托给资源管理器进行判断。

#### 完整实现

```python
class ResourceBudgetBackpressurePolicy(BackpressurePolicy):

    @property
    def name(self) -> str:
        return "ResourceBudget"

    def can_add_input(self, op: "PhysicalOperator") -> bool:
        """委托给 ResourceManager 的资源分配器判断"""
        if self._resource_manager._op_resource_allocator is not None:
            return self._resource_manager._op_resource_allocator.can_submit_new_task(op)
        return True

    def max_task_output_bytes_to_read(self, op: "PhysicalOperator") -> Optional[int]:
        """委托给 ResourceManager 判断可读取的最大字节数"""
        return self._resource_manager.max_task_output_bytes_to_read(op)
```

#### 反压逻辑

- **输入反压**：通过 `OpResourceAllocator.can_submit_new_task()` 判断是否可以提交新任务
- **输出反压**：通过 `ResourceManager.max_task_output_bytes_to_read()` 限制可读取的输出字节数
- 基于 CPU、GPU、内存、Object Store 内存等资源的预算和使用情况进行判断

#### 使用场景

- **适用于**：需要严格控制资源使用的场景
- **典型场景**：集群资源有限，需要精确控制每个算子的资源消耗

---

### 3. DownstreamCapacityBackpressurePolicy（下游容量反压策略）

**位置**：`python/ray/data/_internal/execution/backpressure_policy/downstream_capacity_backpressure_policy.py`

**显示名称**：`DownstreamCapacity`

#### 核心思想

基于下游处理容量进行反压。通过计算队列积压量与下游处理容量的比值，决定是否需要反压上游算子。

#### 辅助函数

**1. 获取可用 Object Store 预算比例**：

```python
def get_available_object_store_budget_fraction(
    resource_manager, op, consider_downstream_ineligible_ops
) -> Optional[float]:
    op_usage = resource_manager.get_op_usage(
        op, include_ineligible_downstream=consider_downstream_ineligible_ops
    )
    op_budget = resource_manager.get_budget(op)
    if op_usage is None or op_budget is None:
        return None

    total_usage = op_usage.object_store_memory
    total_budget = op_budget.object_store_memory
    total_mem = total_usage + total_budget

    if total_mem == 0:
        return None

    return total_budget / total_mem  # 可用比例
```

**2. 获取已用 Object Store 预算比例**：

```python
def get_utilized_object_store_budget_fraction(
    resource_manager, op, consider_downstream_ineligible_ops
) -> Optional[float]:
    available_fraction = get_available_object_store_budget_fraction(...)
    if available_fraction is None:
        return None
    return 1 - available_fraction  # 已用比例
```

#### 完整计算逻辑

**1. 获取队列大小**（`_get_queue_size_bytes` 方法）：

```python
def _get_queue_size_bytes(self, op):
    # 当前算子的输出队列大小
    op_outputs_usage = self._topology[op].output_queue_bytes()

    # 加上下游不合格算子的内存使用
    op_outputs_usage += sum(
        self._resource_manager.get_op_usage(next_op).object_store_memory
        for next_op in self._resource_manager._get_downstream_ineligible_ops(op)
    )
    return op_outputs_usage
```

**2. 获取下游容量**（`_get_downstream_capacity_size_bytes` 方法）：

```python
def _get_downstream_capacity_size_bytes(self, op):
    if not op.output_dependencies:
        # 无下游依赖，返回外部消费者字节数
        return self._resource_manager.get_external_consumer_bytes()

    total_capacity_size_bytes = 0
    for output_dependency in op.output_dependencies:
        if self._resource_manager.is_op_eligible(output_dependency):
            # 合格的下游：累加其待处理任务输入
            total_capacity_size_bytes += (
                output_dependency.metrics.obj_store_mem_pending_task_inputs or 0
            )
        else:
            # 不合格的下游：递归查找更下游的合格算子
            total_capacity_size_bytes += self._get_downstream_capacity_size_bytes(
                output_dependency
            )
    return total_capacity_size_bytes
```

**3. 计算队列/容量比**（`_get_queue_ratio` 方法）：

```python
def _get_queue_ratio(self, op):
    queue_size_bytes = self._get_queue_size_bytes(op)
    downstream_capacity_size_bytes = self._get_downstream_capacity_size_bytes(op)

    if downstream_capacity_size_bytes == 0:
        # 无下游容量，不进行反压
        return 0

    return queue_size_bytes / downstream_capacity_size_bytes
```

**4. 反压判断**（`_should_apply_backpressure` 方法）：

```python
def _should_apply_backpressure(self, op):
    # 检查跳过条件
    if self._should_skip_backpressure(op):
        return False

    # 检查已用预算比例
    utilized_budget_fraction = get_utilized_object_store_budget_fraction(
        self._resource_manager, op, consider_downstream_ineligible_ops=True
    )
    if (
        utilized_budget_fraction is not None
        and utilized_budget_fraction <= OBJECT_STORE_BUDGET_UTIL_THRESHOLD
    ):
        # 已用预算比例低于阈值，跳过反压
        return False

    # 检查队列比
    queue_ratio = self._get_queue_ratio(op)
    return queue_ratio > self._backpressure_capacity_ratio
```

#### 配置参数

| 参数 | 环境变量/配置 | 默认值 | 说明 |
|------|--------------|--------|------|
| OBJECT_STORE_BUDGET_UTIL_THRESHOLD | `RAY_DATA_DOWNSTREAM_CAPACITY_OBJECT_STORE_BUDGET_UTIL_THRESHOLD` | 0.9 | 启用反压的 Object Store 预算利用率阈值 |
| backpressure_capacity_ratio | `RAY_DATA_DOWNSTREAM_CAPACITY_BACKPRESSURE_RATIO` 或 `DataContext.downstream_capacity_backpressure_ratio` | 10.0 | 反压容量比阈值，设为 None 可禁用 |

#### 输出读取限制

```python
def max_task_output_bytes_to_read(self, op):
    if self._should_apply_backpressure(op):
        return 0  # 完全阻止从运行中的任务读取输出
    return None  # 无限制
```

#### 跳过反压的条件

1. `downstream_capacity_backpressure_ratio` 设置为 None（显式禁用）
2. 算子不符合反压资格（`is_op_eligible` 返回 False）
3. 算子是物化算子（`_is_blocking_materializing_op` 返回 True）

---

## 策略组合与协同工作

### 反压判断流程

在 `streaming_executor_state.py::get_eligible_operators()` 中：

```python
for op, state in topology.items():
    # 检查所有反压策略
    triggered_policy = None
    for p in backpressure_policies:
        if not p.can_add_input(op):
            triggered_policy = p.name  # 记录第一个触发反压的策略
            break

    in_backpressure = triggered_policy is not None

    # 只有未被反压的算子才能加入 eligible_ops
    if not is_completed and can_add and has_bundles:
        if not in_backpressure:
            eligible_ops.append(op)
```

### 输出读取限制计算

在 `streaming_executor_state.py::process_completed_tasks()` 中：

```python
for op, state in topology.items():
    max_bytes_to_read = None
    limiting_policy = None

    for policy in backpressure_policies:
        policy_limit = policy.max_task_output_bytes_to_read(op)
        if policy_limit is not None:
            if policy_limit == 0 and limiting_policy is None:
                limiting_policy = policy.name
            if max_bytes_to_read is None:
                max_bytes_to_read = policy_limit
            else:
                # 取所有策略中的最小值（最严格的限制）
                max_bytes_to_read = min(max_bytes_to_read, policy_limit)

    if max_bytes_to_read is not None:
        max_bytes_to_read_per_op[state] = max_bytes_to_read
```

### 活跃性保证

为了避免死锁，当没有合格算子且所有算子都空闲时，返回可调度的算子：

```python
if (
    not eligible_ops
    and ensure_liveness
    and all(op.num_active_tasks() == 0 for op in topology)
):
    return dispatchable_ops  # 返回可调度但被反压的算子
```

---

## 如何选择合适的反压策略

### 场景一：通用数据处理

**推荐配置**：使用默认的三种策略组合

```python
# 默认配置，无需修改
```

### 场景二：资源受限环境

**推荐配置**：强调 ResourceBudgetBackpressurePolicy

```python
# 确保资源预算策略生效
data_context = ray.data.DataContext.get_current()
# 使用默认配置即可
```

### 场景三：管道处理速度不均衡

**推荐配置**：调整下游容量反压比（默认已启用，值为 10.0）

```python
data_context = ray.data.DataContext.get_current()
# 降低下游容量反压比，例如 2.0 表示队列大小不超过下游容量的 2 倍
# 默认值为 10.0，降低该值可以更早触发反压
data_context.downstream_capacity_backpressure_ratio = 2.0

# 如果需要禁用此策略，设为 None
# data_context.downstream_capacity_backpressure_ratio = None
```

### 场景四：需要动态并发调整

**推荐配置**：启用动态输出队列大小反压

```python
data_context = ray.data.DataContext.get_current()
data_context.enable_dynamic_output_queue_size_backpressure = True
```

### 场景五：自定义策略组合

```python
from ray.data._internal.execution.backpressure_policy import (
    ENABLED_BACKPRESSURE_POLICIES_CONFIG_KEY,
    ConcurrencyCapBackpressurePolicy,
    ResourceBudgetBackpressurePolicy,
)

data_context = ray.data.DataContext.get_current()
# 只启用特定策略
data_context.set_config(
    ENABLED_BACKPRESSURE_POLICIES_CONFIG_KEY,
    [ConcurrencyCapBackpressurePolicy, ResourceBudgetBackpressurePolicy]
)
```

---

## 调优建议

### 1. 监控指标

通过日志和指标监控反压效果：
- `task_submission_backpressure_time` - 任务提交反压时间
- `task_output_backpressure_time` - 任务输出反压时间
- `obj_store_mem_*` - Object Store 内存使用指标

### 2. 常见问题诊断

| 问题 | 可能原因 | 调优方向 |
|------|---------|---------|
| 吞吐量低 | 反压过于激进 | 提高阈值或减少反压策略 |
| 内存溢出 | 反压不足 | 降低阈值或启用更多策略 |
| 处理速度波动大 | EWMA 参数不合适 | 调整 EWMA_ALPHA 和 K_DEV |

### 3. 环境变量调优

```bash
# 调整并发上限策略的 EWMA 平滑因子
export RAY_DATA_CONCURRENCY_CAP_EWMA_ALPHA=0.2

# 调整死区宽度系数
export RAY_DATA_CONCURRENCY_CAP_K_DEV=1.5

# 调整下游容量策略的 Object Store 预算利用率阈值
export RAY_DATA_DOWNSTREAM_CAPACITY_OBJECT_STORE_BUDGET_UTIL_THRESHOLD=0.85
```

---

## ResourceManager 资源使用与分配策略

ResourceManager 是反压策略的核心依赖，负责跟踪和分配执行资源。本节详细说明其内部数据结构和计算逻辑。

**位置**：`python/ray/data/_internal/execution/resource_manager.py`

### 核心数据结构

```python
class ResourceManager:
    def __init__(self, topology, options, get_total_resources, data_context):
        # 全局资源跟踪
        self._global_limits = ExecutionResources.zero()       # 全局资源限制
        self._global_usage = ExecutionResources.zero()        # 全局资源使用量
        self._global_running_usage = ExecutionResources.zero()  # 运行中的使用量
        self._global_pending_usage = ExecutionResources.zero()  # 待定的使用量

        # 每个算子的资源使用跟踪
        self._op_usages: Dict[PhysicalOperator, ExecutionResources] = {}
        self._op_running_usages: Dict[PhysicalOperator, ExecutionResources] = {}
        self._op_pending_usages: Dict[PhysicalOperator, ExecutionResources] = {}

        # Object Store 内存跟踪（按算子）
        self._mem_op_internal: Dict[PhysicalOperator, int] = defaultdict(int)
        self._mem_op_outputs: Dict[PhysicalOperator, int] = defaultdict(int)

        # 外部消费者缓冲字节数
        self._external_consumer_bytes: int = 0

        # 资源分配器（ReservationOpResourceAllocator）
        self._op_resource_allocator = create_resource_allocator(self, data_context)
```

### 内存使用量计算

#### `_mem_op_internal` - 算子内部内存使用

```python
def _estimate_object_store_memory_usage(self, op, state) -> int:
    # 不计算 InputDataBuffer 的内存使用（它们是预先创建的）
    if isinstance(op, InputDataBuffer):
        return 0

    # 算子内部的 Object Store 使用量：正在运行的任务的待输出块
    mem_op_internal = op.metrics.obj_store_mem_pending_task_outputs or 0

    self._mem_op_internal[op] = mem_op_internal
    return mem_op_internal
```

**计算公式**：
```
mem_op_internal = obj_store_mem_pending_task_outputs
                = 正在运行任务尚未完成的输出块大小
```

#### `_mem_op_outputs` - 算子输出内存使用

```python
# 算子输出的 Object Store 使用量
op_outputs_bytes = (
    # 算子内部输出队列
    op.metrics.obj_store_mem_internal_outqueue
    +
    # 算子外部输出队列（OpState 中）
    state.output_queue_bytes()
)

# 下游使用的此算子输出
used_op_outputs_bytes = sum([
    (
        # 下游内部输入队列中的块
        downstream_op.metrics.obj_store_mem_internal_inqueue
        +
        # 下游活动任务使用的输入块
        downstream_op.metrics.obj_store_mem_pending_task_inputs
    )
    for downstream_op in op.output_dependencies
])

self._mem_op_outputs[op] = op_outputs_bytes + used_op_outputs_bytes
```

**计算公式**：
```
mem_op_outputs = internal_outqueue + external_outqueue
               + Σ(downstream_inqueue + downstream_pending_inputs)
```

**内存流向示意图**：
```
┌─────────────────────────────────────────────────────────────────┐
│                         Op (算子)                                │
│  ┌─────────────────────┐    ┌─────────────────────────────────┐ │
│  │  Running Tasks      │    │  Output Queues                  │ │
│  │  ┌───────────────┐  │    │  ┌───────────────────────────┐  │ │
│  │  │pending_outputs│──┼────┼─▶│internal_outqueue          │  │ │
│  │  │(mem_op_internal)│ │    │  └───────────────────────────┘  │ │
│  │  └───────────────┘  │    │  ┌───────────────────────────┐  │ │
│  └─────────────────────┘    │  │external_outqueue (OpState)│  │ │
│                              │  └───────────────────────────┘  │ │
│                              └────────────┬───────────────────┘ │
└───────────────────────────────────────────┼─────────────────────┘
                                            │
                     (mem_op_outputs 包含以下所有) ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Downstream Op (下游算子)                      │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │  Input Queues                                               ││
│  │  ┌─────────────────────┐  ┌─────────────────────┐          ││
│  │  │internal_inqueue     │  │pending_task_inputs  │          ││
│  │  │(等待处理的输入块)     │  │(正在处理的输入块)     │          ││
│  │  └─────────────────────┘  └─────────────────────┘          ││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
```

### 队列回调机制与指标更新

Ray Data 使用回调机制在数据流动时更新队列指标。以下是完整的回调函数和指标更新逻辑。

#### 核心回调函数（`OpRuntimeMetrics` 类）

**位置**：`python/ray/data/_internal/execution/interfaces/op_runtime_metrics.py`

```python
class OpRuntimeMetrics:
    def __init__(self, op: "PhysicalOperator"):
        # 内部队列（使用 BundleQueue 跟踪大小）
        self._internal_inqueue = create_bundle_queue()   # 内部输入队列
        self._internal_outqueue = create_bundle_queue()  # 内部输出队列
        self._pending_task_inputs = create_bundle_queue()  # 待处理任务输入

        # 队列块计数
        self.obj_store_mem_internal_inqueue_blocks = 0   # 内部输入队列块数
        self.obj_store_mem_internal_outqueue_blocks = 0  # 内部输出队列块数
```

#### 1. `on_input_queued` - 输入入队回调

当输入块被添加到算子的内部输入队列时调用：

```python
def on_input_queued(self, input: RefBundle):
    """当算子将输入入队时的回调。"""
    self.obj_store_mem_internal_inqueue_blocks += len(input.blocks)
    self._internal_inqueue.add(input)
```

**调用时机**：
- `MapOperator._add_input_inner()` - 当输入被添加到 block_ref_bundler 时
- `AllToAllOperator._add_input_inner()` - 当输入被添加到 _input_buffer 时

#### 2. `on_input_dequeued` - 输入出队回调

当输入块从算子的内部输入队列移除时调用：

```python
def on_input_dequeued(self, input: RefBundle):
    """当算子将输入出队时的回调。"""
    self.obj_store_mem_internal_inqueue_blocks -= len(input.blocks)
    self._internal_inqueue.remove(input)
```

**调用时机**：
- `MapOperator._add_input_inner()` - 当 bundler 合并输入块后
- `MapOperator.clear_internal_input_queue()` - 清空内部输入队列时
- `AllToAllOperator.all_inputs_done()` - 处理完所有输入后

#### 3. `on_output_queued` - 输出入队回调

当输出块被添加到算子的内部输出队列时调用：

```python
def on_output_queued(self, output: RefBundle):
    """当算子将输出入队时的回调。"""
    self.obj_store_mem_internal_outqueue_blocks += len(output.blocks)
    self._internal_outqueue.add(output)
```

**调用时机**：
- `MapOperator` 的任务输出回调（`_output_ready_callback`）中：
```python
def _output_ready_callback(task_index: int, output: RefBundle):
    self._metrics.on_task_output_generated(task_index, output)
    self._output_queue.add(output, key=task_index)
    self._metrics.on_output_queued(output)  # <-- 在这里调用
```

#### 4. `on_output_dequeued` - 输出出队回调

当输出块从算子的内部输出队列移除时调用：

```python
def on_output_dequeued(self, output: RefBundle):
    """当算子将输出出队时的回调。"""
    self.obj_store_mem_internal_outqueue_blocks -= len(output.blocks)
    self._internal_outqueue.remove(output)
```

**调用时机**：
- `MapOperator._get_next_inner()` - 当下游算子获取输出时：
```python
def _get_next_inner(self) -> RefBundle:
    bundle = self._output_queue.get_next()
    self._metrics.on_output_dequeued(bundle)  # <-- 在这里调用
    return bundle
```
- `MapOperator.clear_internal_output_queue()` - 清空内部输出队列时

#### 5. `on_task_submitted` - 任务提交回调

当算子提交新任务时调用，将输入添加到 pending_task_inputs：

```python
def on_task_submitted(self, task_index: int, inputs: RefBundle, task_id=None):
    """当算子提交任务时的回调。"""
    self.num_tasks_submitted += 1
    self.num_tasks_running += 1
    self.bytes_inputs_of_submitted_tasks += inputs.size_bytes()
    self._pending_task_inputs.add(inputs)  # 将输入添加到待处理队列
```

#### 6. `on_task_finished` - 任务完成回调

当任务完成时调用，从 pending_task_inputs 移除输入：

```python
def on_task_finished(self, task_index: int, exception: Optional[Exception]):
    """当任务完成时的回调。"""
    self.num_tasks_running -= 1
    self.num_tasks_finished += 1

    inputs = self._running_tasks[task_index].inputs
    self._pending_task_inputs.remove(inputs)  # 从待处理队列移除
    inputs.destroy_if_owned()  # 释放输入数据
```

### 队列指标的计算属性

```python
@property
def obj_store_mem_internal_inqueue(self) -> int:
    """内部输入队列的字节大小。"""
    return self._internal_inqueue.estimate_size_bytes()

@property
def obj_store_mem_internal_outqueue(self) -> int:
    """内部输出队列的字节大小。"""
    return self._internal_outqueue.estimate_size_bytes()

@property
def obj_store_mem_pending_task_inputs(self) -> int:
    """待处理任务输入的字节大小。"""
    return self._pending_task_inputs.estimate_size_bytes()

@property
def obj_store_mem_pending_task_outputs(self) -> Optional[float]:
    """运行中任务的待输出块的估计字节大小。"""
    per_task_output = self.obj_store_mem_max_pending_output_per_task
    if per_task_output is None:
        return None
    return self.num_tasks_running * per_task_output
```

### 上下游队列的数据流动

#### 完整数据流示意图

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              上游算子 (Producer Op)                          │
│                                                                             │
│  1. 任务生成输出                                                             │
│     └─► on_task_output_generated() 更新任务输出统计                          │
│                                                                             │
│  2. 输出入队到内部输出队列                                                    │
│     └─► _output_queue.add(output)                                           │
│     └─► on_output_queued(output)                                            │
│         └─► obj_store_mem_internal_outqueue_blocks += len(output.blocks)    │
│         └─► _internal_outqueue.add(output)                                  │
│                                                                             │
│  3. 输出从内部队列移动到外部队列 (OpState.output_queue)                        │
│     └─► op.has_next() = True                                                │
│     └─► op_state.add_output(op.get_next())                                  │
│         └─► _get_next_inner() 调用 on_output_dequeued()                     │
│         └─► output_queue.append(ref)                                        │
│         └─► 更新 num_external_outqueue_blocks/bytes                         │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
              ┌───────────────────────────────────────────┐
              │     OpState.output_queue (外部输出队列)    │
              │  = 下游 OpState.input_queues[i] (外部输入)  │
              │                                           │
              │  注意：这是同一个队列对象的两个引用          │
              │  (在 setup_state 时通过引用传递连接)        │
              └───────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐

---

## Op 内部队列与 OpState 外部队列的关系

### 为什么需要两层队列？

Ray Data 使用**两层队列架构**：算子内部队列（Op 内部）和外部队列（OpState 管理）。这种设计有以下原因：

#### 1. 职责分离

| 队列类型 | 管理者 | 职责 |
|---------|--------|------|
| **内部队列** (`_output_queue`) | 算子 (Operator) | 处理算子特定的语义（如排序、合并） |
| **外部队列** (`output_queue`) | OpState | 管理算子间的数据传递和调度 |

#### 2. 支持 `preserve_order` 语义

**内部队列的关键作用**是支持 `preserve_order=True` 时的输出排序。

```python
# MapOperator.start() 中
def start(self, options: "ExecutionOptions"):
    if options.preserve_order:
        self._output_queue = ReorderingBundleQueue()  # 支持按 task_index 排序
    else:
        self._output_queue = FIFOBundleQueue()        # 简单 FIFO
```

**问题背景**：
- 任务是并行执行的，完成顺序不确定
- 例如：Task 0, 1, 2 并行执行，可能 Task 2 先完成，然后 Task 0, 最后 Task 1
- 如果 `preserve_order=True`，输出必须按 Task 0 → Task 1 → Task 2 的顺序

**ReorderingBundleQueue 如何工作**：

```python
class ReorderingBundleQueue(BaseBundleQueue):
    """按照 key (task_index) 顺序而非插入顺序迭代的队列"""

    def __init__(self):
        self._inner: Dict[int, Deque[RefBundle]] = defaultdict(deque)
        self._current_key: int = 0           # 当前期望的 task_index
        self._finalized_keys: Set[int] = set()  # 已完成的 task_index

    def add(self, bundle: RefBundle, key: int):
        """按 task_index 存储输出"""
        self._inner[key].append(bundle)

    def has_next(self) -> bool:
        """只有当前 task_index 的输出可用时才返回 True"""
        # 跳过已完成但输出为空的 task
        while (
            self._current_key in self._finalized_keys
            and len(self._inner[self._current_key]) == 0
        ):
            self._current_key += 1

        return len(self._inner[self._current_key]) > 0

    def get_next(self) -> RefBundle:
        """始终返回当前 task_index 的输出"""
        return self._inner[self._current_key].popleft()

    def finalize(self, key: int):
        """标记某个 task 已完成所有输出"""
        self._finalized_keys.add(key)
```

**示例**：
```
任务完成顺序：Task 2 → Task 0 → Task 1
输出到达顺序：[Output_2a, Output_2b] → [Output_0] → [Output_1a, Output_1b]

ReorderingBundleQueue 内部状态：
_inner = {
    0: [Output_0],
    1: [Output_1a, Output_1b],
    2: [Output_2a, Output_2b]
}
_current_key = 0
_finalized_keys = {0, 1, 2}

has_next() 检查 _inner[0]，返回 True
get_next() 返回 Output_0（不是 Output_2a）

输出顺序（对下游可见）：Output_0 → Output_1a → Output_1b → Output_2a → Output_2b
```

#### 3. 外部队列无法实现排序

**外部队列 (`OpBufferQueue`)** 是线程安全的 FIFO 队列：

```python
class OpBufferQueue:
    """上下游算子之间缓冲 RefBundle 的 FIFO 队列（线程安全）"""

    def __init__(self):
        self._queue = create_bundle_queue()  # 简单 FIFO
        self._lock = threading.Lock()        # 线程安全

    def append(self, ref: RefBundle):
        """追加到队列末尾（无排序能力）"""
        with self._lock:
            self._queue.add(ref)

    def pop(self) -> Optional[RefBundle]:
        """从队列头部取出"""
        with self._lock:
            return self._queue.get_next()
```

外部队列的设计目标是：
- **线程安全**：支持消费者线程并发访问
- **简单高效**：只做数据传递，不做业务逻辑
- **支持 output_split**：用于数据并行消费

如果把排序逻辑放在外部队列：
- 需要知道 task_index 和完成状态 → 破坏了职责分离
- 每个算子可能有不同的排序需求 → 无法统一实现

#### 4. 解耦调度与执行

```
┌─────────────────────────────────────────────────────────────────┐
│                     流式执行器 (StreamingExecutor)               │
│                                                                 │
│  调度循环：                                                      │
│  1. select_operator_to_run() - 选择下一个要执行的算子            │
│  2. dispatch_next_task() - 从外部输入队列取数据给算子            │
│  3. process_completed_tasks() - 处理完成的任务                   │
│     └─► 从算子内部队列拉取数据到外部队列                          │
│         while op.has_next():                                    │
│             op_state.add_output(op.get_next())                  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
         │                                    ▲
         │ dispatch_next_task()               │ add_output()
         ▼                                    │
┌─────────────────────────────────────────────────────────────────┐
│                          算子 (Operator)                         │
│                                                                 │
│  内部处理：                                                      │
│  1. add_input() - 接收输入                                       │
│  2. 执行任务 - 生成输出                                          │
│  3. 输出存入内部队列 - _output_queue.add()                       │
│  4. has_next()/get_next() - 暴露给调度器                         │
│                                                                 │
│  【算子控制何时、以何顺序暴露输出】                               │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**关键点**：调度器只通过 `has_next()`/`get_next()` 接口与算子交互，不关心算子内部如何管理输出。

#### 5. 队列对比总结

| 特性 | 内部队列 (`_output_queue`) | 外部队列 (`output_queue`) |
|------|---------------------------|--------------------------|
| **位置** | 算子内部 | OpState |
| **类型** | `ReorderingBundleQueue` 或 `FIFOBundleQueue` | `OpBufferQueue` |
| **线程安全** | 否（单线程访问） | 是（消费者线程可能并发访问） |
| **排序能力** | 是（支持按 task_index 排序） | 否（FIFO） |
| **业务逻辑** | 包含（排序、合并等） | 不包含（纯数据传递） |
| **访问者** | 算子自己 | 调度器、下游算子 |
| **指标跟踪** | `obj_store_mem_internal_outqueue` | `output_queue_bytes()` |

#### 6. 两层队列的数据流

```
任务完成                      内部队列                    外部队列
   │                            │                          │
   │  on_task_output_generated  │                          │
   ▼                            │                          │
   ●─────────────────────────►  │                          │
   │                            │                          │
   │  _output_queue.add()       │                          │
   │  on_output_queued()        │                          │
   ▼                            ▼                          │
                          ┌─────────┐                      │
                          │ 内部队列 │                      │
                          │ (可能   │                      │
                          │  重排序) │                      │
                          └────┬────┘                      │
                               │                           │
                               │  op.has_next() = True     │
                               │  op.get_next()            │
                               │  on_output_dequeued()     │
                               ▼                           │
                               ●───────────────────────────►
                               │                           │
                               │  op_state.add_output()    │
                               │  output_queue.append()    │
                               │                           │
                                                           ▼
                                                     ┌───────────┐
                                                     │  外部队列  │
                                                     │  (FIFO)   │
                                                     └─────┬─────┘
                                                           │
                                                           ▼
                                                    下游算子输入
```

### 不使用单一队列的原因

如果只使用一个队列，会面临以下问题：

1. **无法支持 `preserve_order`**：外部队列是 FIFO，无法实现按 task_index 排序
2. **职责混乱**：队列需要同时处理算子内部逻辑和跨算子通信
3. **线程安全开销**：内部队列不需要线程安全，如果合并会增加不必要的锁开销
4. **扩展性差**：不同算子可能需要不同的内部队列语义（如 AllToAllOperator 需要完全物化）

---

## preserve_order 详解

### 定义与作用

`preserve_order` 是 `ExecutionOptions` 的一个配置选项，用于控制数据块在流式执行过程中是否保持原始顺序。

**位置**：`python/ray/data/_internal/execution/interfaces/execution_options.py`

```python
class ExecutionOptions:
    """Options for execution of a Dataset.

    Args:
        preserve_order: Set this to preserve the ordering between blocks processed by
            operators. Off by default.
    """

    def __init__(
        self,
        ...
        preserve_order: bool = False,  # 默认关闭
        ...
    ):
        self.preserve_order = preserve_order
```

### 为什么需要 preserve_order？

**问题背景**：Ray Data 使用并行任务执行数据处理，任务完成顺序是不确定的。

```
原始数据块顺序：  Block 0 → Block 1 → Block 2 → Block 3
任务提交顺序：    Task 0   → Task 1   → Task 2   → Task 3
任务完成顺序：    Task 2   → Task 0   → Task 3   → Task 1  (不确定)

如果 preserve_order=False：
输出顺序：        Block 2' → Block 0' → Block 3' → Block 1' (按完成顺序)

如果 preserve_order=True：
输出顺序：        Block 0' → Block 1' → Block 2' → Block 3' (保持原始顺序)
```

### 使用场景

#### 1. 确定性数据分割

```python
# 训练/测试集分割需要确定性
ctx = ray.data.DataContext.get_current()
ctx.execution_options.preserve_order = True

ds = ray.data.range(1000)
train, test = ds.streaming_train_test_split(test_size=0.2, seed=42)

# 多次执行会得到相同的分割结果
```

#### 2. 有序数据处理

```python
# 时间序列数据需要保持顺序
ctx.execution_options.preserve_order = True

ds = ray.data.read_parquet("time_series/*.parquet")
ds = ds.map_batches(process_time_series)  # 保持时间顺序
```

#### 3. 可重现的结果

```python
# 调试和测试需要可重现的输出
ctx.execution_options.preserve_order = True

ds = ray.data.range(100)
result1 = ds.map(lambda x: x * 2).take_all()
result2 = ds.map(lambda x: x * 2).take_all()
assert result1 == result2  # 相同顺序
```

### 实现机制

#### MapOperator 中的实现

```python
# map_operator.py
def start(self, options: "ExecutionOptions"):
    if options.preserve_order:
        # 使用重排序队列，按 task_index 顺序输出
        self._output_queue = ReorderingBundleQueue()
    else:
        # 使用 FIFO 队列，按完成顺序输出
        self._output_queue = FIFOBundleQueue()
```

#### ReorderingBundleQueue 工作原理

```python
class ReorderingBundleQueue:
    def __init__(self):
        self._inner: Dict[int, Deque[RefBundle]] = defaultdict(deque)
        self._current_key: int = 0  # 当前期望的 task_index
        self._finalized_keys: Set[int] = set()

    def add(self, bundle: RefBundle, key: int):
        # 按 task_index 存储，不是按到达顺序
        self._inner[key].append(bundle)

    def has_next(self) -> bool:
        # 只有当前 task_index 有输出时才返回 True
        while (
            self._current_key in self._finalized_keys
            and len(self._inner[self._current_key]) == 0
        ):
            self._current_key += 1
        return len(self._inner[self._current_key]) > 0

    def get_next(self) -> RefBundle:
        # 始终返回当前 task_index 的输出
        return self._inner[self._current_key].popleft()

    def finalize(self, key: int):
        # 标记某个 task 已完成所有输出
        self._finalized_keys.add(key)
```

### 性能影响

| 方面 | preserve_order=False | preserve_order=True |
|------|---------------------|---------------------|
| **吞吐量** | 更高（无等待） | 可能降低（等待慢任务） |
| **延迟** | 低（立即输出） | 可能增加（等待前序任务） |
| **内存** | 低 | 可能更高（缓冲快任务的输出） |
| **确定性** | 否 | 是 |

**权衡**：如果 Task 0 很慢，即使 Task 1-10 都完成了，它们的输出也必须等待 Task 0 完成后才能被下游消费。

---

## _get_queue_size_bytes 计算详解

### 定义与位置

`_get_queue_size_bytes` 是 `DownstreamCapacityBackpressurePolicy` 中用于计算队列积压大小的方法。

**位置**：`python/ray/data/_internal/execution/backpressure_policy/downstream_capacity_backpressure_policy.py`

```python
def _get_queue_size_bytes(self, op: "PhysicalOperator") -> int:
    """Get the output current queue size
    (this operator + ineligible downstream operators) in bytes for the given operator.
    """
    # 当前算子的外部输出队列大小
    op_outputs_usage = self._topology[op].output_queue_bytes()

    # 加上下游不合格算子的内存使用
    op_outputs_usage += sum(
        self._resource_manager.get_op_usage(next_op).object_store_memory
        for next_op in self._resource_manager._get_downstream_ineligible_ops(op)
    )
    return op_outputs_usage
```

### 计算含义

**队列大小 = 外部输出队列 + 下游不合格算子的内存使用**

#### 为什么包含下游不合格算子？

**不合格算子（Ineligible Operators）** 是指：
- `throttling_disabled() = True` 的算子（如 `LimitOperator`）
- 已完成执行的算子（`has_execution_finished() = True`）

这些算子**不参与资源预留和反压控制**，它们的内存使用被视为上游合格算子的"延伸"。

```
┌─────────────────────────────────────────────────────────────────┐
│  管道示例：map1 (eligible) → limit1 (ineligible) → map2 (eligible)│
│                                                                 │
│  对于 map1 的 _get_queue_size_bytes 计算：                       │
│                                                                 │
│  queue_size = map1.output_queue_bytes()    ← map1 的外部输出队列  │
│             + limit1.object_store_memory   ← limit1 的全部内存   │
│                                                                 │
│  原因：limit1 不参与反压，它的积压应该反映在 map1 的队列大小中     │
└─────────────────────────────────────────────────────────────────┘
```

#### 与 _get_downstream_capacity_size_bytes 的对比

```python
def _get_downstream_capacity_size_bytes(self, op: "PhysicalOperator") -> int:
    """下游容量 = 下游合格算子的待处理任务输入之和"""
    if not op.output_dependencies:
        return self._resource_manager.get_external_consumer_bytes()

    total_capacity_size_bytes = 0
    for output_dependency in op.output_dependencies:
        if self._resource_manager.is_op_eligible(output_dependency):
            # 合格的下游：使用其 pending_task_inputs
            total_capacity_size_bytes += (
                output_dependency.metrics.obj_store_mem_pending_task_inputs or 0
            )
        else:
            # 不合格的下游：递归查找更下游的合格算子
            total_capacity_size_bytes += self._get_downstream_capacity_size_bytes(
                output_dependency
            )
    return total_capacity_size_bytes
```

**对比总结**：

| 方法 | 计算内容 | 处理不合格算子 |
|------|---------|---------------|
| `_get_queue_size_bytes` | 队列积压 | 包含不合格算子的内存使用 |
| `_get_downstream_capacity_size_bytes` | 下游处理能力 | 跳过不合格算子，递归找合格算子 |

### 为什么只使用 output_queue_bytes？

`_get_queue_size_bytes` 只使用 `output_queue_bytes()`（外部输出队列），**不包含内部输出队列**。

原因：
1. **内部输出队列受算子自己控制**：如 `ReorderingBundleQueue` 可能为了 `preserve_order` 而缓冲数据
2. **外部输出队列反映真正的积压**：数据已经准备好传递给下游，但下游还没处理
3. **避免误判**：内部队列可能因为排序需要而暂时增大，不代表真正的背压需求

---

## _mem_op_internal 为什么只考虑 running task？

### 问题分析

`_mem_op_internal` 的计算：

```python
# resource_manager.py
mem_op_internal = op.metrics.obj_store_mem_pending_task_outputs or 0
```

而 `obj_store_mem_pending_task_outputs` 的计算：

```python
# op_runtime_metrics.py
@property
def obj_store_mem_pending_task_outputs(self) -> Optional[float]:
    """运行中任务的待输出块的估计字节大小"""
    per_task_output = self.obj_store_mem_max_pending_output_per_task
    if per_task_output is None:
        return None
    return self.num_tasks_running * per_task_output
```

### 为什么只考虑 running task？

**原因：这是对 Ray 流式生成器缓冲区的估算**

```
┌─────────────────────────────────────────────────────────────────┐
│                      任务执行生命周期                            │
│                                                                 │
│  pending_task_inputs    →    running task    →    输出队列       │
│  (等待处理的输入)             (正在执行)          (已完成的输出)   │
│                                │                                │
│                                ▼                                │
│                    ┌─────────────────────┐                      │
│                    │ Ray Generator Buffer │                     │
│                    │ (流式生成器缓冲区)    │                     │
│                    │                      │                     │
│                    │ 已生成但未 yield 的块 │ ← mem_op_internal   │
│                    └─────────────────────┘                      │
│                                │                                │
│                        yield 后                                 │
│                                ▼                                │
│                    ┌─────────────────────┐                      │
│                    │ 内部输出队列        │ ← mem_op_outputs      │
│                    │ (已 yield 的输出)   │                      │
│                    └─────────────────────┘                      │
│                                │                                │
│                                ▼                                │
│                    ┌─────────────────────┐                      │
│                    │ 外部输出队列        │ ← mem_op_outputs      │
│                    │ (OpState 管理)      │                      │
│                    └─────────────────────┘                      │
└─────────────────────────────────────────────────────────────────┘
```

**关键理解**：

1. **只有 running task 才有 generator buffer**：
   - 待调度的任务（pending）还没开始执行，没有输出
   - 已完成的任务（finished）的输出已经移动到输出队列

2. **这是估算值**：
   - 无法精确知道每个任务的 generator buffer 中有多少数据
   - 使用 `num_tasks_running × max_pending_output_per_task` 估算

3. **数据只存在一个地方**：
   - 数据块要么在 generator buffer（`mem_op_internal`）
   - 要么在输出队列（`mem_op_outputs`）
   - **不会同时存在于两处**

### 计算公式

```python
obj_store_mem_pending_task_outputs = num_tasks_running × per_task_output

per_task_output = bytes_per_output × num_pending_outputs
                = average_bytes_per_output × min(
                    _max_num_blocks_in_streaming_gen_buffer,
                    average_num_outputs_per_task
                  )
```

---

## used_op_outputs_bytes 与 obj_store_mem_pending_task_inputs 是否重复计算？

### 问题分析

```python
# ResourceManager._estimate_object_store_memory_usage
used_op_outputs_bytes = sum([
    (
        downstream_op.metrics.obj_store_mem_internal_inqueue
        + downstream_op.metrics.obj_store_mem_pending_task_inputs  # ← 这里
    )
    for downstream_op in op.output_dependencies
])
```

问题：`obj_store_mem_pending_task_inputs` 是下游算子的指标，那么它会不会被算在下游算子的 `mem_op_internal` 里导致重复？

### 答案：不会重复

**关键理解**：`_mem_op_internal` 和 `pending_task_inputs` 跟踪的是**不同的数据**。

```
┌─────────────────────────────────────────────────────────────────┐
│                       数据块 B 的生命周期                        │
│                                                                 │
│  上游算子 (OpA)                                                  │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │ 1. 任务执行中，B 在 generator buffer                        ││
│  │    → 计入 OpA.mem_op_internal                               ││
│  │                                                             ││
│  │ 2. 任务 yield B，B 进入 OpA 内部输出队列                     ││
│  │    → 计入 OpA.mem_op_outputs (obj_store_mem_internal_outqueue)│
│  │                                                             ││
│  │ 3. B 移动到 OpA 外部输出队列                                 ││
│  │    → 计入 OpA.mem_op_outputs (output_queue_bytes)           ││
│  └─────────────────────────────────────────────────────────────┘│
│                              │                                  │
│                              ▼                                  │
│  下游算子 (OpB)                                                  │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │ 4. B 进入 OpB 内部输入队列                                   ││
│  │    → 计入 OpA.used_op_outputs_bytes (obj_store_mem_internal_inqueue)│
│  │                                                             ││
│  │ 5. B 被 OpB 任务使用                                        ││
│  │    → 计入 OpA.used_op_outputs_bytes (obj_store_mem_pending_task_inputs)│
│  │                                                             ││
│  │ 6. OpB 任务完成，B 被释放                                    ││
│  │    → 从所有计数中移除                                        ││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
```

### 不同指标跟踪不同的数据

| 指标 | 跟踪的数据 | 所属算子 | 计入 |
|------|-----------|---------|------|
| `obj_store_mem_pending_task_outputs` | 上游任务的 generator buffer 中的输出 | 上游 | 上游.mem_op_internal |
| `obj_store_mem_internal_outqueue` | 上游内部输出队列中的块 | 上游 | 上游.mem_op_outputs |
| `output_queue_bytes()` | 上游外部输出队列中的块 | 上游 | 上游.mem_op_outputs |
| `obj_store_mem_internal_inqueue` | 下游内部输入队列中的块 | 下游 | **上游**.used_op_outputs_bytes |
| `obj_store_mem_pending_task_inputs` | 下游任务正在处理的输入 | 下游 | **上游**.used_op_outputs_bytes |

**关键点**：
1. 下游的 `obj_store_mem_internal_inqueue` 和 `obj_store_mem_pending_task_inputs` **计入上游的内存使用**
2. 下游自己的 `mem_op_internal` 是下游任务的 **输出**（generator buffer），不是输入
3. 每个数据块在任意时刻**只会被计入一个指标**

### 验证：下游的 mem_op_internal 包含什么？

```python
# 下游算子的 mem_op_internal
downstream.mem_op_internal = downstream.obj_store_mem_pending_task_outputs
                           = 下游任务正在生成的输出（在 generator buffer 中）
                           ≠ 下游任务正在处理的输入（obj_store_mem_pending_task_inputs）
```

**示意图**：

```
上游任务 (OpA)                    下游任务 (OpB)
     │                                │
     │ 产生输出 B                      │ 产生输出 C
     ▼                                ▼
┌─────────────┐                 ┌─────────────┐
│ OpA 的      │                 │ OpB 的      │
│ generator   │                 │ generator   │
│ buffer (B)  │ ← OpA.internal  │ buffer (C)  │ ← OpB.internal
└─────────────┘                 └─────────────┘
     │                                ▲
     │                                │ 处理 B 生成 C
     ▼                                │
┌─────────────┐                 ┌─────────────┐
│ OpA 输出队列 │ ─────────────►   │ OpB 输入    │
│    (B)      │                 │ /任务 (B)   │
└─────────────┘                 └─────────────┘
      │                               │
      └───── OpA.used_op_outputs_bytes ────┘
              (B 计入这里)
```

### 完整内存计算公式

```
OpA.mem_op_internal = OpA 运行中任务的 generator buffer 大小

OpA.mem_op_outputs = OpA.obj_store_mem_internal_outqueue  (OpA 内部输出队列)
                   + OpA.output_queue_bytes()              (OpA 外部输出队列)
                   + Σ OpB.obj_store_mem_internal_inqueue  (下游 OpB 内部输入队列)
                   + Σ OpB.obj_store_mem_pending_task_inputs (下游 OpB 任务输入)

OpA.total_object_store_usage = OpA.mem_op_internal + OpA.mem_op_outputs
```

**不重复的原因**：每个数据块 B 在任意时刻只会处于上述位置之一。

---

## used_op_outputs_bytes 指标计算详解

### 定义

`used_op_outputs_bytes` 表示**当前算子的输出被下游算子使用的字节数**。这是计算 `mem_op_outputs` 的重要组成部分。

### 计算位置

**位置**：`python/ray/data/_internal/execution/resource_manager.py` 的 `_estimate_object_store_memory_usage` 方法

```python
def _estimate_object_store_memory_usage(self, op, state) -> int:
    # ...

    # 下游使用的此算子输出
    used_op_outputs_bytes = sum(
        [
            (
                # 下游内部输入队列中的块
                downstream_op.metrics.obj_store_mem_internal_inqueue
        +
                # 下游活动任务使用的输入块
                downstream_op.metrics.obj_store_mem_pending_task_inputs
            )
            for downstream_op in op.output_dependencies
        ]
    )

    self._mem_op_outputs[op] = op_outputs_bytes + used_op_outputs_bytes
    return self._mem_op_outputs[op] + self._mem_op_internal[op]
```

### 计算公式

```
used_op_outputs_bytes = Σ (
    downstream_op.obj_store_mem_internal_inqueue
    + downstream_op.obj_store_mem_pending_task_inputs
) for each downstream_op in op.output_dependencies
```

### 为什么计入上游算子？

**设计原则**：数据块的内存使用**计入生产者**，而不是消费者。

```
┌─────────────────────────────────────────────────────────────────┐
│                      上游算子 (Producer)                         │
│                                                                 │
│  mem_op_outputs = op_outputs_bytes + used_op_outputs_bytes      │
│                                                                 │
│  op_outputs_bytes:                                              │
│    - obj_store_mem_internal_outqueue (内部输出队列)              │
│    - output_queue_bytes() (外部输出队列)                         │
│                                                                 │
│  used_op_outputs_bytes:  <-- 虽然在下游，但计入上游              │
│    - downstream.obj_store_mem_internal_inqueue                  │
│    - downstream.obj_store_mem_pending_task_inputs               │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      下游算子 (Consumer)                         │
│                                                                 │
│  注意：下游的 internal_inqueue 和 pending_task_inputs           │
│  存储的是上游产生的数据块，因此内存使用应计入上游                 │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**原因**：
1. **数据来源跟踪**：数据块由上游生产，其内存使用应反映在上游的资源消耗中
2. **反压控制**：当上游的 `mem_op_outputs` 过高时，应该限制上游的生产速度
3. **公平资源分配**：每个算子只对自己产生的数据负责

### 数据流示意

```
上游算子产生数据块 B
        │
        ▼
┌───────────────────┐
│ 上游 internal_out │  ← 计入上游 op_outputs_bytes
└───────────────────┘
        │
        ▼
┌───────────────────┐
│ 上游 external_out │  ← 计入上游 op_outputs_bytes
│ = 下游 external_in│     (同一队列)
└───────────────────┘
        │
        ▼
┌───────────────────┐
│ 下游 internal_in  │  ← 计入上游 used_op_outputs_bytes
└───────────────────┘
        │
        ▼
┌───────────────────┐
│ 下游 pending_task │  ← 计入上游 used_op_outputs_bytes
│     _inputs       │
└───────────────────┘
        │
        ▼
  数据块 B 被处理并释放
```

---

## obj_store_mem_internal_inqueue 详解

### 定义

`obj_store_mem_internal_inqueue` 表示**算子内部输入队列中所有数据块的字节大小**。

### 计算方式

**位置**：`python/ray/data/_internal/execution/interfaces/op_runtime_metrics.py`

```python
@metric_property(
    description="Byte size of input blocks in the operator's internal input queue.",
    metrics_group=MetricsGroup.OBJECT_STORE_MEMORY,
)
def obj_store_mem_internal_inqueue(self) -> int:
    return self._internal_inqueue.estimate_size_bytes()
```

### 更新时机

#### 1. 入队 - `on_input_queued`

当输入被添加到算子的内部输入队列时调用：

```python
def on_input_queued(self, input: RefBundle):
    """当算子将输入入队时的回调。"""
    self.obj_store_mem_internal_inqueue_blocks += len(input.blocks)
    self._internal_inqueue.add(input)  # 更新字节大小
```

**调用位置**：
- `MapOperator._add_input_inner()` - 输入添加到 block_ref_bundler 时
- `AllToAllOperator._add_input_inner()` - 输入添加到 _input_buffer 时

```python
# MapOperator._add_input_inner()
def _add_input_inner(self, refs: RefBundle, input_index: int):
    self._block_ref_bundler.add_bundle(refs)
    self._metrics.on_input_queued(refs)  # <-- 这里调用
```

#### 2. 出队 - `on_input_dequeued`

当输入从内部队列移除时调用：

```python
def on_input_dequeued(self, input: RefBundle):
    """当算子将输入出队时的回调。"""
    self.obj_store_mem_internal_inqueue_blocks -= len(input.blocks)
    self._internal_inqueue.remove(input)  # 更新字节大小
```

**调用位置**：
- `MapOperator._add_input_inner()` - bundler 合并输入后
- `MapOperator.clear_internal_input_queue()` - 清空队列时

```python
# MapOperator._add_input_inner()
def _add_input_inner(self, refs: RefBundle, input_index: int):
    self._block_ref_bundler.add_bundle(refs)
    self._metrics.on_input_queued(refs)

    if self._block_ref_bundler.has_bundle():
        (input_refs, bundled_input) = self._block_ref_bundler.get_next_bundle()
        for bundle in input_refs:
            self._metrics.on_input_dequeued(bundle)  # <-- 每个原始输入都出队
```

### 在资源计算中的使用

```python
# resource_manager.py
used_op_outputs_bytes = sum([
    (
        downstream_op.metrics.obj_store_mem_internal_inqueue  # <-- 使用这个指标
        + downstream_op.metrics.obj_store_mem_pending_task_inputs
    )
    for downstream_op in op.output_dependencies
])
```

---

## obj_store_mem_pending_task_inputs 详解

### 定义

`obj_store_mem_pending_task_inputs` 表示**正在被运行中任务处理的输入数据块的字节大小**。

### 计算方式

```python
@metric_property(
    description="Byte size of input blocks used by pending tasks.",
    metrics_group=MetricsGroup.OBJECT_STORE_MEMORY,
)
def obj_store_mem_pending_task_inputs(self) -> int:
    return self._pending_task_inputs.estimate_size_bytes()
```

### 生命周期

```
输入数据块的生命周期：

external_inqueue → internal_inqueue → pending_task_inputs → 释放
     │                   │                   │
     │                   │                   └── on_task_finished() 时移除
     │                   │                       inputs.destroy_if_owned() 释放
     │                   │
     │                   └── on_input_dequeued() 时移除
     │                       on_task_submitted() 时添加到 pending
     │
     └── dispatch_next_task() 时移除
         on_input_queued() 时添加到 internal
```

### 更新时机

#### 1. 添加 - `on_task_submitted`

当任务被提交时，输入从内部队列移动到 pending_task_inputs：

```python
def on_task_submitted(
    self,
    task_index: int,
    inputs: RefBundle,
    task_id: Optional[ray.TaskID] = None,
):
    """当算子提交任务时的回调。"""
    self.num_tasks_submitted += 1
    self.num_tasks_running += 1
    self.bytes_inputs_of_submitted_tasks += inputs.size_bytes()
    self.rows_inputs_of_submitted_tasks += inputs.num_rows() or 0
    self._pending_task_inputs.add(inputs)  # <-- 添加到 pending
    self._running_tasks[task_index] = RunningTaskInfo(
        inputs=inputs,
        ...
    )
```

#### 2. 移除 - `on_task_finished`

当任务完成时，输入从 pending_task_inputs 移除并释放：

```python
def on_task_finished(self, task_index: int, exception: Optional[Exception]):
    """当任务完成时的回调。"""
    self.num_tasks_running -= 1
    self.num_tasks_finished += 1

    # ...

    inputs = self._running_tasks[task_index].inputs
    self.num_task_inputs_processed += len(inputs)
    total_input_size = inputs.size_bytes()
    self.bytes_task_inputs_processed += total_input_size

    self._pending_task_inputs.remove(inputs)  # <-- 从 pending 移除

    # 释放输入数据的内存
    inputs.destroy_if_owned()

    del self._running_tasks[task_index]
```

### 与 internal_inqueue 的区别

| 指标 | 含义 | 数据位置 |
|------|------|---------|
| `obj_store_mem_internal_inqueue` | 等待被处理的输入 | 内部输入队列（等待调度） |
| `obj_store_mem_pending_task_inputs` | 正在被处理的输入 | 任务执行中（已调度） |

### 数据流转示意

```
                         obj_store_mem_internal_inqueue
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────┐
│                    internal_inqueue (等待中)                     │
│  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐                               │
│  │ B4  │ │ B3  │ │ B2  │ │ B1  │  ← 等待被调度的输入块          │
│  └─────┘ └─────┘ └─────┘ └─────┘                               │
└─────────────────────────────────────────────────────────────────┘
                                    │
                    on_input_dequeued() + on_task_submitted()
                                    │
                                    ▼
                      obj_store_mem_pending_task_inputs
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────┐
│                 pending_task_inputs (处理中)                     │
│  ┌─────────────────┐  ┌─────────────────┐                      │
│  │ Task 0: [B0]    │  │ Task 1: [B1]    │  ← 正在执行任务的输入  │
│  └─────────────────┘  └─────────────────┘                      │
└─────────────────────────────────────────────────────────────────┘
                                    │
                        on_task_finished()
                        inputs.destroy_if_owned()
                                    │
                                    ▼
                               内存释放
```

---

## 下游算子数据处理流程

以下是下游算子接收和处理数据的完整流程：

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              下游算子 (Consumer Op)                          │
│                                                                             │
│  1. 调度器从外部输入队列取出数据                                              │
│     └─► dispatch_next_task()                                                │
│         └─► ref = inqueue.pop()                                             │
│         └─► 更新 num_external_inqueue_blocks/bytes (减少)                   │
│         └─► op.add_input(ref, input_index)                                  │
│             └─► on_input_received(refs)                                     │
│                                                                             │
│  2. 输入入队到内部输入队列                                                    │
│     └─► _add_input_inner(refs, input_index)                                 │
│         └─► on_input_queued(refs)                                           │
│             └─► obj_store_mem_internal_inqueue_blocks += len(refs.blocks)   │
│             └─► _internal_inqueue.add(refs)                                 │
│                                                                             │
│  3. 输入从内部队列移动到任务 (MapOperator)                                    │
│     └─► bundler.get_next_bundle() 合并输入                                  │
│         └─► on_input_dequeued(bundle) 对每个原始输入                         │
│     └─► _submit_data_task(bundled_input)                                    │
│         └─► on_task_submitted(task_index, inputs)                           │
│             └─► _pending_task_inputs.add(inputs)                            │
│                                                                             │
│  4. 任务完成后                                                               │
│     └─► on_task_finished(task_index, exception)                             │
│         └─► _pending_task_inputs.remove(inputs)                             │
│         └─► inputs.destroy_if_owned() 释放内存                              │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### 外部队列连接机制

在 `streaming_executor_state.py` 的 `build_streaming_topology()` 中：

```python
def setup_state(op: PhysicalOperator) -> OpState:
    inqueues = []
    for parent in op.input_dependencies:
        parent_state = setup_state(parent)
        # 关键：父算子的 output_queue 直接作为子算子的 input_queue
        inqueues.append(parent_state.output_queue)

    op_state = OpState(op, inqueues)
    topology[op] = op_state
    return op_state
```

这意味着：
- `上游算子.OpState.output_queue` ≡ `下游算子.OpState.input_queues[i]`
- 它们是**同一个队列对象**，不是数据复制

#### 指标计算位置

| 指标 | 所属算子 | 计算公式 |
|------|---------|---------|
| `obj_store_mem_internal_outqueue` | 当前算子 | `_internal_outqueue.estimate_size_bytes()` |
| `output_queue_bytes()` | 当前算子 | `OpState.output_queue.memory_usage` |
| `obj_store_mem_internal_inqueue` | 下游算子 | `_internal_inqueue.estimate_size_bytes()` |
| `obj_store_mem_pending_task_inputs` | 下游算子 | `_pending_task_inputs.estimate_size_bytes()` |

#### ResourceManager 中的聚合计算

```python
def _estimate_object_store_memory_usage(self, op, state) -> int:
    # 算子内部使用（运行中任务的待输出）
    mem_op_internal = op.metrics.obj_store_mem_pending_task_outputs or 0

    # 算子输出使用
    op_outputs_bytes = (
        op.metrics.obj_store_mem_internal_outqueue  # 内部输出队列
        + state.output_queue_bytes()                 # 外部输出队列
    )

    # 下游使用的此算子输出（计入当前算子的 mem_op_outputs）
    used_op_outputs_bytes = sum([
        (
            downstream_op.metrics.obj_store_mem_internal_inqueue
            + downstream_op.metrics.obj_store_mem_pending_task_inputs
        )
        for downstream_op in op.output_dependencies
    ])

    self._mem_op_internal[op] = mem_op_internal
    self._mem_op_outputs[op] = op_outputs_bytes + used_op_outputs_bytes

    return mem_op_internal + op_outputs_bytes + used_op_outputs_bytes
```

### op_usage - 算子资源使用量

```python
def update_usages(self):
    for op, state in reversed(self._topology.items()):
        op.update_resource_usage()

        # 获取 CPU/GPU 使用（不含 object_store_memory）
        op_usage = op.current_processor_usage()

        # 估算 object store 内存使用
        used_object_store = self._estimate_object_store_memory_usage(op, state)

        # 合并 object_store_memory 到 usage
        op_usage = op_usage.copy(object_store_memory=used_object_store)

        # 如果算子有额外资源使用，添加进去
        if isinstance(op, ReportsExtraResourceUsage):
            op_usage.add(op.extra_resource_usage())

        self._op_usages[op] = op_usage
```

**计算公式**：
```
op_usage = ExecutionResources(
    cpu = current_processor_usage().cpu,
    gpu = current_processor_usage().gpu,
    object_store_memory = mem_op_internal + mem_op_outputs
)
```

---

## ReservationOpResourceAllocator 资源分配器

ReservationOpResourceAllocator 实现了基于预留的资源分配策略，是 ResourceBudgetBackpressurePolicy 的核心依赖。

### 核心数据结构

```python
class ReservationOpResourceAllocator(OpResourceAllocator):
    def __init__(self, resource_manager, reservation_ratio):
        self._reservation_ratio = reservation_ratio  # 默认 50%

        # 每个算子的预留资源（不包括 _reserved_for_op_outputs）
        self._op_reserved: Dict[PhysicalOperator, ExecutionResources] = {}

        # 专门为算子输出预留的内存
        self._reserved_for_op_outputs: Dict[PhysicalOperator, float] = {}

        # 总共享资源（所有算子间共享）
        self._total_shared = ExecutionResources.zero()

        # 每个算子的资源预算
        self._op_budgets: Dict[PhysicalOperator, ExecutionResources] = {}

        # 每个算子生成新任务输出的剩余内存预算
        self._output_budgets: Dict[PhysicalOperator, float] = {}

        # 是否预留了运行至少一个任务的最小资源
        self._reserved_min_resources: Dict[PhysicalOperator, bool] = {}
```

### op_reserved - 算子预留资源

**计算过程**（`_update_reservation` 方法）：

```python
def _update_reservation(self, limits: ExecutionResources):
    eligible_ops = self._resource_manager.get_eligible_ops()
    remaining = limits.copy()

    # 每个算子的默认预留 = limits * reservation_ratio / num_ops
    default_reserved = limits.scale(self._reservation_ratio / len(eligible_ops))

    for op in eligible_ops:
        # 输出预留：至少是默认预留的一半
        reserved_for_outputs = ExecutionResources(
            0, 0, max(default_reserved.object_store_memory / 2, 1)
        )

        # 任务预留 = 默认预留 - 输出预留
        reserved_for_tasks = default_reserved.subtract(reserved_for_outputs)

        # 应用最小/最大资源约束
        min_resource, max_resource = op.min_max_resource_requirements()
        if min_resource is not None:
            reserved_for_tasks = reserved_for_tasks.max(min_resource)
        if max_resource is not None:
            reserved_for_tasks = reserved_for_tasks.min(max_resource)

        self._op_reserved[op] = reserved_for_tasks
        self._reserved_for_op_outputs[op] = reserved_for_outputs.object_store_memory

        remaining = remaining.subtract(reserved_for_tasks.add(reserved_for_outputs))

    self._total_shared = remaining.max(ExecutionResources.zero())
```

**计算公式**：
```
default_reserved = limits × reservation_ratio / num_eligible_ops

reserved_for_op_outputs = max(default_reserved.object_store_memory / 2, 1)

op_reserved = clamp(
    default_reserved - reserved_for_outputs,
    min_resource_requirements,
    max_resource_requirements
)

total_shared = limits - Σ(op_reserved + reserved_for_op_outputs)
```

**预留分配示意图**：
```
┌─────────────────────────────────────────────────────────────────┐
│                     Global Limits (全局限制)                     │
├───────────────────────────────────────────────┬─────────────────┤
│           Reserved (预留部分)                  │  Shared (共享)   │
│  ┌─────────────────────────────────────────┐  │                 │
│  │  Op1_reserved │ Op1_output_reserved    │  │                 │
│  ├─────────────────────────────────────────┤  │                 │
│  │  Op2_reserved │ Op2_output_reserved    │  │  total_shared   │
│  ├─────────────────────────────────────────┤  │                 │
│  │  Op3_reserved │ Op3_output_reserved    │  │                 │
│  └─────────────────────────────────────────┘  │                 │
└───────────────────────────────────────────────┴─────────────────┘
        50% (reservation_ratio)                      50%
```

### _op_budgets - 算子资源预算

**计算过程**（`update_budgets` 方法）：

```python
def update_budgets(self, *, limits: ExecutionResources):
    self._update_reservation(limits)
    self._op_budgets.clear()

    eligible_ops = self._resource_manager.get_eligible_ops()
    remaining_shared = self._total_shared

    for op in eligible_ops:
        # 计算算子的内存使用量
        op_mem_usage = self._resource_manager.get_mem_op_internal(op)

        # 添加超出 reserved_for_op_outputs 的输出使用量
        op_outputs_usage = self._resource_manager.get_mem_op_outputs(
            op, include_ineligible_downstream=True
        )
        op_mem_usage += max(op_outputs_usage - self._reserved_for_op_outputs[op], 0)

        op_usage = self._resource_manager.get_op_usage(op).copy(
            object_store_memory=op_mem_usage
        )

        # 预留资源的剩余部分
        op_reserved_remaining = self._op_reserved[op].subtract(op_usage).max(
            ExecutionResources.zero()
        )

        self._op_budgets[op] = op_reserved_remaining

        # 超出预留的部分从共享资源中扣除
        op_reserved_exceeded = op_usage.subtract(self._op_reserved[op]).max(
            ExecutionResources.zero()
        )
        remaining_shared = remaining_shared.subtract(op_reserved_exceeded)

    remaining_shared = remaining_shared.max(ExecutionResources.zero())

    # 分配剩余共享资源（从下游到上游）
    for i, op in enumerate(reversed(eligible_ops)):
        op_shared = remaining_shared.scale(1.0 / (len(eligible_ops) - i))

        # 如果预算不足以调度最小资源，允许从上游借用
        to_borrow = (
            op.min_scheduling_resources()
            .subtract(self._op_budgets[op].add(op_shared))
            .max(ExecutionResources.zero())
        )
        if not to_borrow.is_zero():
            if op_shared.add(to_borrow).satisfies_limit(remaining_shared):
                op_shared = op_shared.add(to_borrow)

        remaining_shared = remaining_shared.subtract(op_shared)
        self._op_budgets[op] = self._op_budgets[op].add(op_shared)

    # 阻塞式物化算子禁用 object store 内存限制
    for op in eligible_ops:
        if self._resource_manager._is_blocking_materializing_op(op):
            self._op_budgets[op] = self._op_budgets[op].copy(
                object_store_memory=float("inf")
            )
```

**计算公式**：
```
op_mem_usage = mem_op_internal + max(mem_op_outputs - reserved_for_op_outputs, 0)

op_reserved_remaining = max(op_reserved - op_usage, 0)

op_reserved_exceeded = max(op_usage - op_reserved, 0)

remaining_shared = total_shared - Σ(op_reserved_exceeded)

op_shared = remaining_shared / remaining_ops_count

op_budget = op_reserved_remaining + op_shared
```

**Budget 计算示意图**：
```
┌─────────────────────────────────────────────────────────────────┐
│                        Op Budget 计算                           │
│                                                                 │
│   op_reserved ─────┐                                           │
│                    │                                            │
│                    ▼                                            │
│   ┌────────────────────────────────┐                           │
│   │      op_usage (当前使用)        │                           │
│   │  ┌──────────────────────────┐  │                           │
│   │  │ Used                     │  │                           │
│   │  └──────────────────────────┘  │                           │
│   │  ┌──────────────────────────┐  │   ┌─────────────────────┐ │
│   │  │ op_reserved_remaining   │◀─┼───│ 预留剩余部分         │ │
│   │  └──────────────────────────┘  │   └─────────────────────┘ │
│   └────────────────────────────────┘                           │
│                                                                 │
│   op_shared ───────────────────────────┐                       │
│                                        │                        │
│                                        ▼                        │
│   ┌────────────────────────────────────────────────────────┐   │
│   │              op_budget = reserved_remaining + shared    │   │
│   └────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### _output_budgets - 输出预算

**计算过程**（`max_task_output_bytes_to_read` 方法）：

```python
def max_task_output_bytes_to_read(self, op: PhysicalOperator) -> Optional[int]:
    if op not in self._op_budgets:
        return None

    # 从 op_budget 获取 object_store_memory 部分
    res = self._op_budgets[op].object_store_memory

    # 添加 reserved_for_op_outputs 的剩余部分
    op_outputs_usage = self._resource_manager.get_mem_op_outputs(
        op, include_ineligible_downstream=True
    )
    res += max(self._reserved_for_op_outputs[op] - op_outputs_usage, 0)

    if math.isinf(res):
        self._output_budgets[op] = res
        return None

    res = int(res)

    # 如果结果为 0 但需要解除背压，返回 1
    if res == 0 and self._should_unblock_streaming_output_backpressure(op):
        res = 1

    self._output_budgets[op] = res
    return res
```

**计算公式**：
```
output_budget = op_budget.object_store_memory
              + max(reserved_for_op_outputs - op_outputs_usage, 0)
```

### can_submit_new_task - 任务提交判断

```python
def can_submit_new_task(self, op: PhysicalOperator) -> bool:
    budget = self.get_budget(op)

    if budget is None:
        return True

    return (
        # 增量资源使用是否满足预算限制
        op.incremental_resource_usage().satisfies_limit(budget)
        and
        # 避免在没有 Object Store 预算时调度（用于任务输出）
        budget.object_store_memory >= (
            op.metrics.obj_store_mem_max_pending_output_per_task or 0
        )
    )
```

### 关键计算公式汇总

| 变量 | 计算公式 |
|------|----------|
| `mem_op_internal` | `obj_store_mem_pending_task_outputs` |
| `mem_op_outputs` | `internal_outqueue + external_outqueue + Σ(downstream_inqueue + downstream_pending_inputs)` |
| `op_usage.object_store_memory` | `mem_op_internal + mem_op_outputs` |
| `default_reserved` | `limits × reservation_ratio / num_eligible_ops` |
| `reserved_for_op_outputs` | `max(default_reserved.object_store_memory / 2, 1)` |
| `op_reserved` | `default_reserved - reserved_for_outputs`（受 min/max 约束） |
| `total_shared` | `limits - Σ(op_reserved + reserved_for_op_outputs)` |
| `op_budget` | `max(op_reserved - op_usage, 0) + op_shared` |
| `output_budget` | `op_budget.object_store_memory + max(reserved_for_op_outputs - op_outputs_usage, 0)` |
| `allocation` | `budget + op_usage` |

### 配置参数

| 参数 | 环境变量 | 默认值 | 说明 |
|------|---------|--------|------|
| reservation_ratio | `DataContext.op_resource_reservation_ratio` | 0.5 | 资源预留比例 |
| op_resource_reservation_enabled | `DataContext.op_resource_reservation_enabled` | True | 是否启用资源预留 |
| OBJECT_STORE_MEMORY_LIMIT_FRACTION | `RAY_DATA_OBJECT_STORE_MEMORY_LIMIT_FRACTION` | 0.5 | Object Store 内存限制比例 |
| GLOBAL_LIMITS_UPDATE_INTERVAL_S | - | 1 | 全局资源限制刷新间隔（秒） |

---

## 源代码位置

| 组件 | 文件位置 |
|------|---------|
| BackpressurePolicy 基类 | `python/ray/data/_internal/execution/backpressure_policy/backpressure_policy.py` |
| ConcurrencyCapBackpressurePolicy | `python/ray/data/_internal/execution/backpressure_policy/concurrency_cap_backpressure_policy.py` |
| ResourceBudgetBackpressurePolicy | `python/ray/data/_internal/execution/backpressure_policy/resource_budget_backpressure_policy.py` |
| DownstreamCapacityBackpressurePolicy | `python/ray/data/_internal/execution/backpressure_policy/downstream_capacity_backpressure_policy.py` |
| 策略注册与获取 | `python/ray/data/_internal/execution/backpressure_policy/__init__.py` |
| 反压应用逻辑 | `python/ray/data/_internal/execution/streaming_executor_state.py` |
| ResourceManager | `python/ray/data/_internal/execution/resource_manager.py` |
| ReservationOpResourceAllocator | `python/ray/data/_internal/execution/resource_manager.py` |
| ExecutionResources | `python/ray/data/_internal/execution/interfaces/execution_options.py` |
| OpRuntimeMetrics | `python/ray/data/_internal/execution/interfaces/op_runtime_metrics.py` |
| MapOperator | `python/ray/data/_internal/execution/operators/map_operator.py` |
| AllToAllOperator | `python/ray/data/_internal/execution/operators/base_physical_operator.py` |
| PhysicalOperator | `python/ray/data/_internal/execution/interfaces/physical_operator.py` |

---

## 总结

Ray Data 的反压机制通过三种策略的组合，实现了对数据流的精细控制：

1. **ConcurrencyCapBackpressurePolicy**：动态调整并发，使用非对称 EWMA 算法自适应处理速度变化
2. **ResourceBudgetBackpressurePolicy**：基于资源预算，委托给 ResourceManager 精确控制资源使用
3. **DownstreamCapacityBackpressurePolicy**：基于下游容量，通过队列/容量比平衡管道各阶段

选择合适的策略组合和参数配置，可以在吞吐量和内存使用之间取得最佳平衡。
