# Ray Data 资源分配内部机制详解

## 概述

本文档详细描述 Ray Data 中 `ReservationOpResourceAllocator` 的资源分配机制，包括内存使用跟踪、预算计算、以及反压控制的内部实现。

---

## 一、核心数据结构

### 1.1 ResourceManager 中的内存跟踪

```python
class ResourceManager:
    # 每个算子的内部内存使用（Generator buffer）
    self._mem_op_internal: Dict[PhysicalOperator, int] = defaultdict(int)

    # 每个算子的输出内存使用（输出队列 + 下游使用）
    self._mem_op_outputs: Dict[PhysicalOperator, int] = defaultdict(int)

    # 每个算子的总资源使用（CPU + GPU + Object Store）
    self._op_usages: Dict[PhysicalOperator, ExecutionResources] = {}
```

### 1.2 ReservationOpResourceAllocator 中的预算管理

```python
class ReservationOpResourceAllocator:
    # 预留比例（默认 50%）
    self._reservation_ratio: float

    # 每个算子的预留资源（不包括输出预留）
    self._op_reserved: Dict[PhysicalOperator, ExecutionResources] = {}

    # 每个算子专门为输出预留的内存
    self._reserved_for_op_outputs: Dict[PhysicalOperator, float] = {}

    # 所有算子预留后剩余的共享资源
    self._total_shared: ExecutionResources

    # 每个算子的实际可用预算（不包括输出预留）
    self._op_budgets: Dict[PhysicalOperator, ExecutionResources] = {}

    # 每个算子用于输出的剩余预算
    self._output_budgets: Dict[PhysicalOperator, float] = {}
```

---

## 二、`_mem_op_internal` vs `_mem_op_outputs`

### 2.1 数据生命周期视角

```
任务执行过程中的数据流：

   Generator Buffer              Op 输出队列              下游输入队列
   (pending_task_outputs)        (internal_outqueue)       (internal_inqueue)
   ┌─────────────────────┐       ┌─────────────────┐       ┌─────────────────┐
   │  _mem_op_internal   │ ──→   │  _mem_op_outputs │ ──→  │  _mem_op_outputs │
   │  (算子内部)          │       │  (算子输出)       │       │  (算子输出)       │
   └─────────────────────┘       └─────────────────┘       └─────────────────┘
         ↑                             ↑                         ↑
    任务正在生成                    已完成输出                  已发送到下游
    但尚未 yield                   可被外界获取                作为下游输入
```

### 2.2 关键区别

| 指标 | 数据状态 | 可访问性 | 归类 |
|------|---------|---------|------|
| `obj_store_mem_pending_task_outputs` | 正在 Generator 缓冲区中 | ❌ 不可被外界获取 | `_mem_op_internal` |
| `obj_store_mem_internal_outqueue` | 在算子输出队列中 | ✅ 可被 OpState 拉取 | `_mem_op_outputs` |
| `obj_store_mem_internal_inqueue` | 在下游输入队列中 | ✅ 可被下游任务使用 | `_mem_op_outputs` |

### 2.3 计算方式

```python
def _estimate_object_store_memory_usage(self, op, state):
    # 算子内部使用：正在运行任务的待输出块
    mem_op_internal = op.metrics.obj_store_mem_pending_task_outputs or 0

    # 算子输出使用：内部输出队列 + 外部输出队列
    op_outputs_bytes = (
        op.metrics.obj_store_mem_internal_outqueue  # 内部输出队列
        + state.output_queue_bytes()                 # 外部输出队列（OpState）
    )

    # 下游使用的此算子输出
    used_op_outputs_bytes = sum([
        (
            downstream_op.metrics.obj_store_mem_internal_inqueue      # 下游内部输入队列
            + downstream_op.metrics.obj_store_mem_pending_task_inputs # 下游任务输入
        )
        for downstream_op in op.output_dependencies
    ])

    self._mem_op_internal[op] = mem_op_internal
    self._mem_op_outputs[op] = op_outputs_bytes + used_op_outputs_bytes
```

### 2.4 为什么 `pending_task_outputs` 是"内部"使用

`pending_task_outputs` 代表的是**尚在任务 generator 内部、还没 yield 出来的数据**：

1. **任务还在运行**：数据还在 Ray 任务的 generator buffer 里
2. **无法被外部访问**：无法被任何外部逻辑获取
3. **生命周期绑定**：与运行中的任务绑定，任务完成这部分内存就释放

---

## 三、两个指标的后续作用

### 3.1 预算分配中的不同用途

在 `update_budgets()` 中：

```python
for op in eligible_ops:
    op_mem_usage = 0

    # 1. 内部使用量：全额计入 op_mem_usage
    op_mem_usage += self._resource_manager.get_mem_op_internal(op)

    # 2. 输出使用量：只计入超出预留部分
    op_outputs_usage = self._resource_manager.get_mem_op_outputs(
        op, include_ineligible_downstream=True
    )
    op_mem_usage += max(op_outputs_usage - self._reserved_for_op_outputs[op], 0)
```

**关键区别**：

| 指标 | 预算计算方式 | 原因 |
|------|-------------|------|
| `_mem_op_internal` | 全额计入 `op_mem_usage` | Generator buffer 是任务执行的必要开销，必须从任务预算中扣除 |
| `_mem_op_outputs` | 只计入超出 `_reserved_for_op_outputs` 的部分 | 输出有专门预留的配额，只有超额部分才挤占任务预算 |

### 3.2 输出读取限制

在 `max_task_output_bytes_to_read()` 中：

```python
def max_task_output_bytes_to_read(self, op):
    # 基础预算
    res = self._op_budgets[op].object_store_memory

    # 加上输出预留中剩余的部分
    op_outputs_usage = self._resource_manager.get_mem_op_outputs(
        op, include_ineligible_downstream=True
    )
    res += max(self._reserved_for_op_outputs[op] - op_outputs_usage, 0)
```

这里**只使用 `_mem_op_outputs`**，因为输出读取限制控制的是"从任务中拉取多少输出"，直接影响输出队列。

### 3.3 并发上限策略

在 `ConcurrencyCapBackpressurePolicy.can_add_input()` 中：

```python
# 计算总队列大小用于 EWMA 调整
current_queue_size_bytes = (
    self._resource_manager.get_mem_op_internal(op)
    + self._resource_manager.get_mem_op_outputs(op, include_ineligible_downstream=True)
)
```

这里**两者都使用**，因为并发上限策略需要知道算子的**总内存压力**。

### 3.4 总结图示

```
                          ┌─────────────────────────────────────────┐
                          │           算子内存使用总览                │
                          └─────────────────────────────────────────┘
                                          │
              ┌───────────────────────────┴───────────────────────────┐
              ↓                                                       ↓
    ┌─────────────────────┐                              ┌─────────────────────┐
    │   _mem_op_internal  │                              │   _mem_op_outputs   │
    │                     │                              │                     │
    │ pending_task_outputs│                              │ internal_outqueue + │
    │ (Generator buffer)  │                              │ downstream usage    │
    └─────────────────────┘                              └─────────────────────┘
              │                                                       │
              │ 用途:                                                  │ 用途:
              │ 1. 全额计入 op_mem_usage                               │ 1. 有专门预留配额
              │ 2. 控制任务调度预算                                     │ 2. 只计超额部分
              │ 3. 并发上限计算                                        │ 3. 输出读取限制
              │                                                       │ 4. 并发上限计算
              ↓                                                       ↓
    ┌─────────────────────┐                              ┌─────────────────────┐
    │  can_submit_new_task│                              │max_task_output_bytes│
    │    预算是否足够       │                              │    _to_read         │
    └─────────────────────┘                              └─────────────────────┘
```

---

## 四、`update_budgets` 详细逻辑

### 4.1 整体流程

`update_budgets` 分为两个阶段：

1. **`_update_reservation`**：计算每个算子的预留资源
2. **预算分配**：根据预留和使用量计算实际可用预算

### 4.2 阶段一：`_update_reservation` - 预留计算

#### 预留计算公式

```
总资源限制 (limits)
    │
    ├─── 预留比例 (reservation_ratio, 默认 50%)
    │        │
    │        └─── 每个算子的默认预留 = limits × reservation_ratio / num_eligible_ops
    │                  │
    │                  ├─── reserved_for_outputs = 默认预留的 50%（至少 1 byte）
    │                  │         └─── 只有 object_store_memory，CPU/GPU 为 0
    │                  │
    │                  └─── reserved_for_tasks = 默认预留 - reserved_for_outputs
    │                            └─── 包含 CPU/GPU/object_store_memory
    │
    └─── 共享资源 = limits - 所有算子预留之和
```

#### 详细步骤

```python
def _update_reservation(self, limits: ExecutionResources):
    eligible_ops = self._resource_manager.get_eligible_ops()

    # 步骤 1: 计算每个算子的默认预留份额
    # 例如: limits=1000MB, reservation_ratio=0.5, 2个算子
    #       default_reserved = 1000 × 0.5 / 2 = 250MB
    default_reserved = limits.scale(self._reservation_ratio / len(eligible_ops))

    remaining = limits.copy()

    for index, op in enumerate(eligible_ops):
        # 步骤 2: 输出预留 = 默认预留的一半（仅内存）
        # reserved_for_outputs = (0 CPU, 0 GPU, 125MB)
        reserved_for_outputs = ExecutionResources(
            0, 0, max(default_reserved.object_store_memory / 2, 1)
        )

        # 步骤 3: 任务预留 = 默认预留 - 输出预留
        # reserved_for_tasks = (CPU, GPU, 125MB)
        reserved_for_tasks = default_reserved.subtract(reserved_for_outputs)

        # 步骤 4: 约束调整（根据算子的最小/最大资源需求）
        min_resource_usage, max_resource_usage = op.min_max_resource_requirements()
        if min_resource_usage is not None:
            reserved_for_tasks = reserved_for_tasks.max(min_resource_usage)
        if max_resource_usage is not None:
            reserved_for_tasks = reserved_for_tasks.min(max_resource_usage)

        # 步骤 5: 检查剩余资源是否足够（只检查 CPU/GPU，不检查内存）
        if reserved_for_tasks.add(reserved_for_outputs).satisfies_limit(
            remaining, ignore_object_store_memory=True
        ):
            self._reserved_min_resources[op] = True
        else:
            # 资源不足时，只预留最小内存，放弃 CPU/GPU
            self._reserved_min_resources[op] = False
            reserved_for_tasks = ExecutionResources(0, 0, min_resource_usage.object_store_memory)

        # 步骤 6: 保存预留值
        self._op_reserved[op] = reserved_for_tasks
        self._reserved_for_op_outputs[op] = reserved_for_outputs.object_store_memory

        # 步骤 7: 从剩余资源中扣除
        remaining = remaining.subtract(reserved_for_tasks.add(reserved_for_outputs))
        remaining = remaining.max(ExecutionResources.zero())

    # 步骤 8: 剩余资源成为共享池
    self._total_shared = remaining
```

#### 预留分配图示

```
假设：limits = (4 CPU, 0 GPU, 1000MB), reservation_ratio = 0.5, 2个算子

┌─────────────────────────────────────────────────────────────┐
│                    总资源限制: 1000MB                        │
└─────────────────────────────────────────────────────────────┘
                              │
          ┌───────────────────┴───────────────────┐
          │                                       │
          ↓                                       ↓
┌─────────────────────┐             ┌─────────────────────────┐
│   预留资源 (50%)     │             │     共享资源 (50%)       │
│      500MB          │             │        500MB             │
└─────────────────────┘             └─────────────────────────┘
          │
    ┌─────┴─────┐
    ↓           ↓
┌───────┐   ┌───────┐
│ Op1   │   │ Op2   │
│ 250MB │   │ 250MB │
└───────┘   └───────┘
    │           │
 ┌──┴──┐     ┌──┴──┐
 ↓     ↓     ↓     ↓
Tasks Outputs Tasks Outputs
125MB 125MB  125MB 125MB
```

### 4.3 阶段二：预算分配

```python
def update_budgets(self, *, limits: ExecutionResources):
    # 阶段 1: 更新预留
    self._update_reservation(limits)

    self._op_budgets.clear()
    eligible_ops = self._resource_manager.get_eligible_ops()
    if len(eligible_ops) == 0:
        return

    remaining_shared = self._total_shared

    # ============================================
    # 阶段 2: 计算每个算子的预算
    # ============================================
    for op in eligible_ops:
        # 2.1 计算 op_mem_usage（实际内存使用量）
        op_mem_usage = 0

        # (a) 内部使用量：全额计入
        op_mem_usage += self._resource_manager.get_mem_op_internal(op)

        # (b) 输出使用量：只计入超出预留的部分
        op_outputs_usage = self._resource_manager.get_mem_op_outputs(
            op, include_ineligible_downstream=True
        )
        op_mem_usage += max(op_outputs_usage - self._reserved_for_op_outputs[op], 0)

        # 2.2 计算 op_reserved_remaining（预留剩余）
        op_usage = self._resource_manager.get_op_usage(op).copy(
            object_store_memory=op_mem_usage
        )

        op_reserved = self._op_reserved[op]

        # 预留剩余 = 预留 - 使用量（不能为负）
        op_reserved_remaining = op_reserved.subtract(op_usage).max(
            ExecutionResources.zero()
        )

        # 初始预算 = 预留剩余
        self._op_budgets[op] = op_reserved_remaining

        # 2.3 超额部分从共享资源中扣除
        op_reserved_exceeded = op_usage.subtract(op_reserved).max(
            ExecutionResources.zero()
        )
        remaining_shared = remaining_shared.subtract(op_reserved_exceeded)

    remaining_shared = remaining_shared.max(ExecutionResources.zero())

    # ============================================
    # 阶段 3: 分配共享资源（从下游到上游）
    # ============================================
    for i, op in enumerate(reversed(eligible_ops)):
        # 默认平均分配
        op_shared = remaining_shared.scale(1.0 / (len(eligible_ops) - i))

        # 如果预算不足最小调度资源，尝试借用
        to_borrow = op.min_scheduling_resources().subtract(
            self._op_budgets[op].add(op_shared)
        ).max(ExecutionResources.zero())

        if not to_borrow.is_zero() and op_shared.add(to_borrow).satisfies_limit(remaining_shared):
            op_shared = op_shared.add(to_borrow)

        # 确保不超过算子的最大资源需求
        _, max_resource_usage = op.min_max_resource_requirements()
        if max_resource_usage != ExecutionResources.inf():
            total_reserved = self._get_total_reserved(op)
            op_usage = self._resource_manager.get_op_usage(op)
            current_allocation = total_reserved.max(op_usage)
            max_shared = max_resource_usage.subtract(current_allocation).max(
                ExecutionResources.zero()
            )
            op_shared = op_shared.min(max_shared)

        remaining_shared = remaining_shared.subtract(op_shared)
        self._op_budgets[op] = self._op_budgets[op].add(op_shared)

    # ============================================
    # 阶段 4: 特殊处理物化算子（取消内存限制）
    # ============================================
    for op in eligible_ops:
        if self._resource_manager._is_blocking_materializing_op(op):
            self._op_budgets[op] = self._op_budgets[op].copy(
                object_store_memory=float("inf")
            )
```

---

## 五、`_reserved_for_op_outputs` 的含义与必要性

### 5.1 代码注释原文

```python
# Note, if we don't reserve memory for op outputs, all the budget may be used by
# the pending task outputs, and/or op's internal output buffers (the latter can
# happen when `preserve_order=True`).
# Then we'll have no budget to pull blocks from the op.
```

### 5.2 关键概念：`max_task_output_bytes_to_read`

要理解这个注释，首先需要理解 `max_task_output_bytes_to_read` 的作用。

#### 函数定义

```python
def max_task_output_bytes_to_read(self, op: PhysicalOperator) -> Optional[int]:
    # 基础：任务预算中的剩余内存
    res = self._op_budgets[op].object_store_memory

    # 加上：输出预留中剩余的部分
    op_outputs_usage = self._resource_manager.get_mem_op_outputs(op, ...)
    res += max(self._reserved_for_op_outputs[op] - op_outputs_usage, 0)

    return res  # 这个值决定能从 Generator 拉取多少字节
```

#### 作用

这个函数控制的是**从正在运行的任务的 Generator 中拉取多少数据到算子的输出队列**。

```python
# 在 on_data_ready 中使用：
def on_data_ready(self, max_bytes_to_read: Optional[int]) -> int:
    bytes_read = 0
    while max_bytes_to_read is None or bytes_read < max_bytes_to_read:
        # 如果 max_bytes_to_read = 0，这个循环立即退出
        # 不会从 Generator 中拉取任何数据！
        ...
```

#### 数据流图

```
┌────────────────────────────────────────────────────────────────────┐
│                           算子内部                                  │
├────────────────────────────────────────────────────────────────────┤
│                                                                    │
│  ┌─────────────────┐     max_task_output_    ┌─────────────────┐  │
│  │ Generator Buffer│     bytes_to_read       │ Internal Output │  │
│  │ (pending_task_  │ ─────────────────────→  │     Queue       │  │
│  │    outputs)     │     控制这个拉取操作     │ (internal_out)  │  │
│  │                 │                         │                 │  │
│  │ _mem_op_internal│                         │ _mem_op_outputs │  │
│  └─────────────────┘                         └─────────────────┘  │
│                                                      │             │
│                                                      ↓             │
│                                              ┌─────────────────┐  │
│                                              │ External Output │  │
│                                              │     Queue       │  │
│                                              │ (OpState 中)    │  │
│                                              └─────────────────┘  │
└────────────────────────────────────────────────────────────────────┘
                                                       │
                                                       ↓
                                                   下游算子
```

**关键理解**：`max_task_output_bytes_to_read` 控制的是从 **Generator Buffer** 到 **Internal Output Queue** 这一步的数据拉取。

### 5.3 问题场景分析：没有 `_reserved_for_op_outputs` 时

#### 场景设置

假设：
- 算子总预留 = 100MB
- 全部作为 `_op_reserved`（任务执行预算），没有输出专用预留
- 正在运行 5 个任务

#### 状态分析

```
时刻 T1：
┌─────────────────────────────────────────────────────────────────┐
│                       算子状态                                   │
├─────────────────────────────────────────────────────────────────┤
│  正在运行 5 个任务                                               │
│  pending_task_outputs (Generator buffer) = 80MB                 │
│  internal_outqueue (算子内部输出队列) = 15MB                     │
│  external_outqueue + downstream = 5MB                           │
├─────────────────────────────────────────────────────────────────┤
│  _mem_op_internal = 80MB                                        │
│  _mem_op_outputs = 15MB + 5MB = 20MB                            │
│  总使用 = 100MB                                                  │
└─────────────────────────────────────────────────────────────────┘
```

#### 预算计算（没有输出预留）

```python
op_mem_usage = 80MB (internal) + 20MB (outputs) = 100MB
op_reserved = 100MB
op_reserved_remaining = 100 - 100 = 0MB

_op_budgets[op] = 0MB
```

#### `max_task_output_bytes_to_read` 计算

```python
res = _op_budgets[op].object_store_memory = 0
res += max(_reserved_for_op_outputs[op] - op_outputs_usage, 0)
    = max(0 - 20, 0) = 0  # 因为没有输出预留！

结果：max_task_output_bytes_to_read = 0
```

#### 后果

**`max_task_output_bytes_to_read = 0` 意味着**：

1. **Generator buffer 中的数据无法被拉取出来**
2. 数据卡在 Generator 里
3. 任务无法完成（因为 generator 满了，无法继续 yield）
4. 算子的输出队列无法增长
5. 下游无法获得输入
6. **整个管道停滞！**

### 5.4 有 `_reserved_for_op_outputs` 时

#### 预留分配

```
_op_reserved = 50MB（任务执行）
_reserved_for_op_outputs = 50MB（输出缓冲）
```

#### 预算计算

```python
op_outputs_usage = 20MB
op_mem_usage = 80MB (internal) + max(20 - 50, 0) = 80MB  # 输出在配额内，不计入
op_reserved = 50MB
op_reserved_remaining = max(50 - 80, 0) = 0MB

_op_budgets[op] = 0MB（任务预算耗尽）
```

#### `max_task_output_bytes_to_read` 计算

```python
res = _op_budgets[op].object_store_memory = 0
res += max(_reserved_for_op_outputs[op] - op_outputs_usage, 0)
    = max(50 - 20, 0) = 30MB  # ← 关键差异！

结果：max_task_output_bytes_to_read = 30MB
```

#### 效果

**现在可以从 Generator 拉取 30MB 数据**：
- Generator buffer 中的数据可以被拉取到输出队列
- 任务可以继续执行并最终完成
- 下游可以获得输入
- 管道继续流动

### 5.5 `preserve_order=True` 时的特殊问题

`preserve_order=True` 会导致更严重的积压：

```
假设 4 个任务，按顺序完成应该是 Task1 → Task2 → Task3 → Task4
但实际完成顺序是 Task3 → Task4 → Task1 → Task2

preserve_order=True 时：
┌─────────────────────────────────────────────────────────────┐
│                    Op 内部输出队列                           │
│                                                             │
│  Task3 输出 ──┐                                             │
│  Task4 输出 ──┼── 必须等待 Task1, Task2 完成才能输出到外部   │
│               │   全部积压在内部队列（internal_outqueue）！   │
│               ↓                                             │
│         ┌─────────────┐                                     │
│         │ 积压 40MB   │  ← 这些数据无法输出到 OpState        │
│         │ 等待排序    │     因为顺序不对                     │
│         └─────────────┘                                     │
└─────────────────────────────────────────────────────────────┘
```

这会导致 `_mem_op_outputs`（包含 internal_outqueue）快速增长，进一步挤占预算。

### 5.6 注释的正确解读

```
"all the budget may be used by the pending task outputs"
→ 所有预算被 pending_task_outputs (_mem_op_internal) 占用

"and/or op's internal output buffers (the latter can happen when preserve_order=True)"
→ 和/或被算子内部输出队列占用（preserve_order=True 时因排序等待会积压）

"Then we'll have no budget to pull blocks from the op"
→ 然后就没有预算从算子中拉取数据块了
→ 指的是 max_task_output_bytes_to_read = 0
→ 无法从 Generator 中拉取数据到输出队列
→ 管道停滞！
```

### 5.7 `_reserved_for_op_outputs` 的作用总结

**核心作用**：确保从 Generator Buffer 到 Internal Output Queue 这一步有足够的预算，即使任务执行预算已经耗尽。

```
┌─────────────────────────────────────────────────────────────────┐
│  没有 _reserved_for_op_outputs:                                 │
│                                                                 │
│  max_task_output_bytes_to_read = _op_budgets[op] + 0            │
│                                = 0（当预算耗尽时）               │
│                                                                 │
│  → 无法从 Generator 拉取数据 → 管道停滞                         │
├─────────────────────────────────────────────────────────────────┤
│  有 _reserved_for_op_outputs:                                   │
│                                                                 │
│  max_task_output_bytes_to_read = _op_budgets[op]                │
│                                + max(reserved - usage, 0)       │
│                                = 0 + 30MB = 30MB                │
│                                                                 │
│  → 仍可从 Generator 拉取数据 → 管道继续流动                     │
└─────────────────────────────────────────────────────────────────┘
```

### 5.8 场景总结

| 场景 | 没有输出专用预留 | 有输出专用预留 |
|------|-----------------|---------------|
| **预算耗尽时** | `max_task_output_bytes_to_read = 0`，无法拉取 | 仍有预算可拉取 |
| **`preserve_order=True`** | 内部队列积压加剧问题 | 积压在独立配额内 |
| **下游处理慢** | 上游频繁阻塞/恢复 | 上游可持续生产到配额上限 |

---

## 六、`op_reserved_remaining` 的含义

### 6.1 计算逻辑

```python
# op_reserved: 算子的预留资源（不含输出预留）
op_reserved = self._op_reserved[op]

# op_usage: 算子的实际使用量
op_usage = self._resource_manager.get_op_usage(op).copy(
    object_store_memory=op_mem_usage
)

# op_reserved_remaining: 预留中剩余可用的资源
op_reserved_remaining = op_reserved.subtract(op_usage).max(ExecutionResources.zero())
```

### 6.2 含义图示

```
op_reserved_remaining = 预留资源 - 已使用资源

场景1：预留 > 使用（正常情况）
┌─────────────┐
│  预留 100MB  │
│  ┌───────┐  │
│  │使用60 │  │  → op_reserved_remaining = 40MB
│  └───────┘  │
└─────────────┘

场景2：使用 > 预留（超额使用）
┌─────────────┐
│  预留 100MB  │
│  ┌─────────────┐
│  │  使用 150MB │  → op_reserved_remaining = 0
│  └─────────────┘    op_reserved_exceeded = 50MB（从共享资源扣除）
└─────────────┘
```

---

## 七、`remaining_shared` 为负的问题分析

### 7.1 问题描述

```python
# 第 975 行：可能变负
remaining_shared = remaining_shared.subtract(op_reserved_exceeded)

# 第 977 行：强制设为 0
remaining_shared = remaining_shared.max(ExecutionResources.zero())
```

当 `remaining_shared` 变负时，意味着**所有算子的超额使用总和超过了共享资源池**。

### 7.2 为什么不从 budget 中扣除超限部分

这是一个**有意的设计决策**：

```
场景：remaining_shared = -50MB（超限 50MB）

方案1（当前实现）：remaining_shared = 0，不从 budget 扣除
    → 已运行的任务继续运行，不会被杀死
    → 新任务调度被阻止（因为 budget 已经很小或为 0）
    → 系统逐渐恢复平衡

方案2（从 budget 中扣除）：
    → budget 可能变负
    → 已运行的任务无法被回收（Ray 不支持杀死正在运行的任务）
    → 增加了复杂性但没有实际收益
```

**关键洞察**：`_op_budgets` 控制的是**新任务的调度**，而不是已经运行的任务。

### 7.3 实际的超限控制机制

```
超限发生时的控制流：

┌─────────────────────────────────────────────────────────────┐
│  1. 算子使用超过预留 → op_reserved_exceeded > 0             │
│                                                             │
│  2. remaining_shared 被扣减 → 可能变负 → 被设为 0           │
│                                                             │
│  3. 共享资源分配阶段：                                       │
│     - remaining_shared = 0，没有共享资源可分配              │
│     - 每个算子的 _op_budgets 只包含 op_reserved_remaining   │
│                                                             │
│  4. 超额算子的 op_reserved_remaining = 0（因为使用 > 预留）  │
│     → budget = 0 → can_submit_new_task() = False            │
│     → 无法调度新任务                                        │
│                                                             │
│  5. 等待已运行任务完成 → 使用量下降 → 逐渐恢复              │
└─────────────────────────────────────────────────────────────┘
```

### 7.4 数值示例

```
假设：
- limits = 1000MB
- reservation_ratio = 0.5 → 预留 500MB，共享 500MB
- 2 个算子，每个预留 250MB

极端情况（严重超限）：
- Op1 使用 600MB（超出 350MB）
- Op2 使用 500MB（超出 250MB）

计算过程：
  remaining_shared = 500MB

  Op1: op_reserved_exceeded = 350MB
       remaining_shared = 500 - 350 = 150MB
       op_reserved_remaining = max(250 - 600, 0) = 0

  Op2: op_reserved_exceeded = 250MB
       remaining_shared = 150 - 250 = -100MB → 设为 0
       op_reserved_remaining = max(250 - 500, 0) = 0

结果：
  - _op_budgets[Op1] = 0（无法调度新任务）
  - _op_budgets[Op2] = 0（无法调度新任务）
  - 系统等待任务完成，使用量下降后恢复
```

---

## 八、`_op_usages` 和 `get_global_limits` 的逻辑

### 8.1 `_op_usages` 更新流程

```python
def update_usages(self):
    for op, state in reversed(self._topology.items()):
        # 1. 获取 CPU/GPU 使用量（来自算子本身）
        op_usage = op.current_processor_usage()
        # 此时 op_usage.object_store_memory = 0

        # 2. 计算 Object Store 内存使用量
        used_object_store = self._estimate_object_store_memory_usage(op, state)
        # used_object_store = _mem_op_internal + _mem_op_outputs

        # 3. 合并
        op_usage = op_usage.copy(object_store_memory=used_object_store)

        # 4. 保存
        self._op_usages[op] = op_usage
```

### 8.2 `_op_usages` 包含的内容

```
_op_usages[op] = ExecutionResources(
    cpu = 正在运行任务使用的 CPU 数
    gpu = 正在运行任务使用的 GPU 数
    object_store_memory = _mem_op_internal + _mem_op_outputs
                        = pending_task_outputs + internal_outqueue
                          + external_outqueue + downstream_inqueue
                          + downstream_pending_inputs
)
```

### 8.3 `_op_usages` vs `update_budgets` 中的 `op_usage`

**注意区别**：

```python
# _op_usages 中的 object_store_memory（完整使用量，用于监控）
self._op_usages[op].object_store_memory = _mem_op_internal + _mem_op_outputs

# update_budgets 中的 op_mem_usage（用于预算计算）
op_mem_usage = _mem_op_internal + max(_mem_op_outputs - _reserved_for_op_outputs, 0)
```

### 8.4 `get_global_limits` 计算公式

```python
def get_global_limits(self) -> ExecutionResources:
    # 1. 获取用户配置的资源限制
    default_limits = self._options.resource_limits

    # 2. 获取排除的资源（预留给其他用途）
    exclude = self._options.exclude_resources

    # 3. 获取集群总资源
    total_resources = self._get_total_resources()

    # 4. 应用 Object Store 内存比例限制
    #    默认 50%（启用 OpResourceAllocator 时）
    #    默认 25%（未启用时）
    total_resources = total_resources.copy(
        object_store_memory=total_resources.object_store_memory
                           * default_mem_fraction
    )

    # 5. 计算最终限制
    #    = min(用户配置, 集群可用) - 排除资源
    self._global_limits = default_limits.min(total_resources).subtract(exclude)
```

### 8.5 `get_global_limits` 图示

```
┌────────────────────────────────────────────────────────┐
│                    集群总资源                           │
│            total_resources (100%)                       │
└────────────────────────────────────────────────────────┘
                         │
                         ↓
┌────────────────────────────────────────────────────────┐
│           Object Store 内存比例限制                     │
│    object_store_memory × 50% (DEFAULT_FRACTION)        │
└────────────────────────────────────────────────────────┘
                         │
                         ↓
┌────────────────────────────────────────────────────────┐
│              与用户配置取最小值                         │
│          default_limits.min(total_resources)           │
└────────────────────────────────────────────────────────┘
                         │
                         ↓
┌────────────────────────────────────────────────────────┐
│                减去排除资源                             │
│               .subtract(exclude)                       │
└────────────────────────────────────────────────────────┘
                         │
                         ↓
              ┌─────────────────┐
              │  global_limits  │
              └─────────────────┘
```

### 8.6 传递给 `update_budgets` 的 `limits`

```python
def _update_allocated_budgets(self):
    # 1. 获取已完成算子的使用量（它们的输出还在队列中）
    completed_ops_usage = self._get_completed_ops_usage()

    # 2. 可用限制 = 全局限制 - 已完成算子使用量
    available_limits = (
        self.get_global_limits()
        .subtract(completed_ops_usage)
        .max(ExecutionResources.zero())
    )

    # 3. 传递给 update_budgets
    self._op_resource_allocator.update_budgets(limits=available_limits)
```

---

## 九、完整的资源管理流程图

```
┌─────────────────────────────────────────────────────────────────────┐
│                        执行循环每次迭代                               │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  1. ResourceManager.update_usages()                                 │
│     ├── 遍历所有算子                                                 │
│     ├── 计算 _mem_op_internal, _mem_op_outputs                      │
│     ├── 更新 _op_usages (CPU + GPU + Object Store)                  │
│     └── 调用 _update_allocated_budgets()                            │
│                                                                     │
│  2. _update_allocated_budgets()                                     │
│     ├── available_limits = global_limits - completed_ops_usage      │
│     └── update_budgets(limits=available_limits)                     │
│                                                                     │
│  3. update_budgets(limits)                                          │
│     ├── _update_reservation(limits)                                 │
│     │   ├── 计算 _op_reserved (任务执行预留)                         │
│     │   ├── 计算 _reserved_for_op_outputs (输出预留)                 │
│     │   └── 计算 _total_shared (共享池)                              │
│     │                                                               │
│     ├── 计算 _op_budgets                                            │
│     │   ├── op_mem_usage = internal + max(outputs - output_reserve, 0)│
│     │   ├── op_reserved_remaining = reserved - usage                │
│     │   └── 从 remaining_shared 扣除超额                             │
│     │                                                               │
│     └── 分配 remaining_shared 到各算子                               │
│                                                                     │
│  4. select_operator_to_run()                                        │
│     └── can_submit_new_task(op)                                     │
│         └── 检查 _op_budgets[op] 是否足够                            │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 十、关键设计思想总结

| 设计 | 目的 |
|------|------|
| 输出预留与任务预留分离 | 防止任务执行占满预算导致输出无法拉取，避免 `preserve_order` 死锁 |
| 超额使用从共享资源扣除 | 允许突发使用，但全局资源仍受控 |
| 从下游到上游分配共享资源 | 优先保证下游有资源消费，避免上游积压 |
| 物化算子取消内存限制 | 避免 AllToAll 等算子因反压导致死锁 |
| `remaining_shared` 为负时设为 0 | 已运行任务无法回收，只需阻止新任务调度 |

---

## 十一、完整数值示例

```
假设：
- limits = (4 CPU, 0 GPU, 1000MB)
- reservation_ratio = 0.5
- 2个合格算子: Op1, Op2
- Op1 当前使用: 2 CPU, 80MB internal, 60MB outputs
- Op2 当前使用: 1 CPU, 30MB internal, 20MB outputs

═══════════════════════════════════════════════════════════════
阶段1: _update_reservation
═══════════════════════════════════════════════════════════════

每个算子默认预留 = 1000 × 0.5 / 2 = 250MB

Op1:
  _reserved_for_op_outputs[Op1] = 250 / 2 = 125MB
  _op_reserved[Op1] = (2CPU, 0GPU, 125MB)

Op2:
  _reserved_for_op_outputs[Op2] = 125MB
  _op_reserved[Op2] = (2CPU, 0GPU, 125MB)

_total_shared = 1000 - 250 - 250 = 500MB

═══════════════════════════════════════════════════════════════
阶段2: 计算预算
═══════════════════════════════════════════════════════════════

Op1:
  op_mem_usage = 80 (internal) + max(60 - 125, 0) = 80MB
  op_usage = (2CPU, 0GPU, 80MB)
  op_reserved = (2CPU, 0GPU, 125MB)
  op_reserved_remaining = (2-2, 0-0, 125-80) = (0CPU, 0GPU, 45MB)
  op_reserved_exceeded = (0, 0, 0)  ← 未超额

Op2:
  op_mem_usage = 30 (internal) + max(20 - 125, 0) = 30MB
  op_usage = (1CPU, 0GPU, 30MB)
  op_reserved = (2CPU, 0GPU, 125MB)
  op_reserved_remaining = (2-1, 0-0, 125-30) = (1CPU, 0GPU, 95MB)
  op_reserved_exceeded = (0, 0, 0)

remaining_shared = 500MB（无超额扣除）

═══════════════════════════════════════════════════════════════
阶段3: 分配共享资源（从下游到上游）
═══════════════════════════════════════════════════════════════

Op2 (downstream):
  op_shared = 500 / 2 = 250MB
  _op_budgets[Op2] = (1CPU, 0GPU, 95MB) + (0, 0, 250MB) = (1CPU, 0GPU, 345MB)
  remaining_shared = 500 - 250 = 250MB

Op1 (upstream):
  op_shared = 250 / 1 = 250MB
  _op_budgets[Op1] = (0CPU, 0GPU, 45MB) + (0, 0, 250MB) = (0CPU, 0GPU, 295MB)

═══════════════════════════════════════════════════════════════
最终预算
═══════════════════════════════════════════════════════════════

Op1: _op_budgets = (0CPU, 0GPU, 295MB)
     _output_budgets = 295 + max(125-60, 0) = 295 + 65 = 360MB

Op2: _op_budgets = (1CPU, 0GPU, 345MB)
     _output_budgets = 345 + max(125-20, 0) = 345 + 105 = 450MB
```
