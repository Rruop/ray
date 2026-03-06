# ReservationOpResourceAllocator 资源分配机制详解

本文档详细分析 Ray Data 中 `ReservationOpResourceAllocator` 的资源分配机制，包括 `update_budgets` 的计算逻辑、算子节流禁用原因，以及 `update_usages` 的遍历顺序分析。

**源文件位置**: `python/ray/data/_internal/execution/resource_manager.py`

---

## 目录

1. [ReservationOpResourceAllocator 概述](#1-reservationopresourceallocator-概述)
2. [update_budgets 详细计算逻辑](#2-update_budgets-详细计算逻辑)
3. [为什么某些算子禁用节流](#3-为什么某些算子禁用节流)
4. [update_usages 反向遍历分析](#4-update_usages-反向遍历分析)

---

## 1. ReservationOpResourceAllocator 概述

### 1.1 类定义

`ReservationOpResourceAllocator` 位于 `resource_manager.py:745-800`，是一种基于预留的资源分配策略实现。

### 1.2 核心设计思想

```
┌─────────────────────────────────────────────────────────────────┐
│                      全局资源 (Global Limits)                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              预留资源 (Reserved Resources)               │   │
│  │           reservation_ratio × global_limits              │   │
│  │                                                         │   │
│  │   ┌─────────┐   ┌─────────┐   ┌─────────┐              │   │
│  │   │  Op1    │   │  Op2    │   │  Op3    │   ...        │   │
│  │   │ 预留    │   │ 预留    │   │ 预留    │              │   │
│  │   └─────────┘   └─────────┘   └─────────┘              │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              共享资源 (Shared Resources)                 │   │
│  │         (1 - reservation_ratio) × global_limits          │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 1.3 关键数据结构

| 数据结构 | 类型 | 含义 |
|---------|------|------|
| `_op_reserved` | `Dict[PhysicalOperator, ExecutionResources]` | 每个算子的任务预留资源（不含输出） |
| `_reserved_for_op_outputs` | `Dict[PhysicalOperator, float]` | 每个算子输出的专用预留内存 |
| `_total_shared` | `ExecutionResources` | 共享资源池总量 |
| `_op_budgets` | `Dict[PhysicalOperator, ExecutionResources]` | 每个算子的最终可用预算 |
| `_reserved_min_resources` | `Dict[PhysicalOperator, bool]` | 算子是否已预留最小资源 |

### 1.4 Eligible vs Ineligible 算子

```python
# resource_manager.py:421-428
def is_op_eligible(self, op: PhysicalOperator) -> bool:
    """Whether the op is eligible for memory reservation."""
    return (
        not op.throttling_disabled()           # 未禁用节流
        and not op.has_execution_finished()    # 未完成执行
    )
```

**分组机制**：Ineligible 算子的资源使用会被归属到其上游 eligible 算子。

示例管道：
```
map1 -> limit -> map2 -> streaming_split
  │       │         │           │
  └───────┘         └───────────┘
  (组1: eligible    (组2: eligible
   + ineligible)     + ineligible)
```

---

## 2. update_budgets 详细计算逻辑

### 2.1 整体流程图

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           update_budgets 完整流程                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ Phase 1: 更新预留资源 (_update_reservation)                          │   │
│  │   • 为每个 eligible 算子计算预留资源                                  │   │
│  │   • 返回剩余的共享资源池 _total_shared                               │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    ↓                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ Phase 2: 计算每个算子的实际使用量和预留剩余                           │   │
│  │   • 遍历 eligible_ops（正序，上游→下游）                              │   │
│  │   • 计算 op_mem_usage、op_reserved_remaining、op_reserved_exceeded   │   │
│  │   • 初始化 _op_budgets[op] = op_reserved_remaining                   │   │
│  │   • remaining_shared -= op_reserved_exceeded                         │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    ↓                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ Phase 3: 分配共享资源                                                 │   │
│  │   • 遍历 eligible_ops（逆序，下游→上游）                              │   │
│  │   • 平均分配 + 借用机制 + 上限限制                                    │   │
│  │   • _op_budgets[op] += op_shared                                     │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    ↓                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ Phase 4: 处理剩余资源和特殊情况                                       │   │
│  │   • 剩余资源给最下游无上限的算子                                      │   │
│  │   • 物化算子禁用内存反压                                              │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Phase 1: 更新预留资源 (`_update_reservation`)

**位置**: 第 802-871 行

#### 计算公式

```python
# 每个算子的默认预留资源
default_reserved = limits × (reservation_ratio / num_eligible_ops)

# 例如：总资源 100GB，reservation_ratio=0.5，3个算子
# default_reserved = 100GB × (0.5 / 3) = 16.67GB per op
```

#### 预留资源的拆分

```
default_reserved (16.67GB)
        │
        ├──→ reserved_for_outputs (8.33GB) : 专门用于算子输出
        │
        └──→ reserved_for_tasks (8.33GB)   : 用于运行任务
```

**代码实现**:
```python
reserved_for_outputs = max(default_reserved.object_store_memory / 2, 1)
reserved_for_tasks = default_reserved - reserved_for_outputs
```

#### 资源约束处理

```python
# 确保 reserved_for_tasks 满足算子的最小/最大资源需求
reserved_for_tasks = reserved_for_tasks.max(min_resource_usage)
reserved_for_tasks = reserved_for_tasks.min(max_resource_usage)
```

#### 资源不足时的降级处理

如果剩余资源不足以满足算子的最小预留：
```python
if not enough_remaining:
    # 只预留对象存储内存（可超额），放弃 CPU/GPU 预留
    reserved_for_tasks = ExecutionResources(0, 0, min_object_store_memory)
    self._reserved_min_resources[op] = False  # 标记未满足最小预留
```

#### 输出

```python
self._op_reserved[op] = reserved_for_tasks           # 任务预留
self._reserved_for_op_outputs[op] = reserved_for_outputs  # 输出预留
self._total_shared = remaining                       # 剩余共享资源
```

### 2.3 Phase 2: 计算实际使用量和预留剩余

**位置**: 第 946-976 行

**遍历顺序**: 正序（上游 → 下游）

#### Step 2.1: 计算算子内存使用量

```python
op_mem_usage = 0

# 1. 算子内部内存使用（pending task outputs + internal buffers）
op_mem_usage += get_mem_op_internal(op)

# 2. 输出使用量超出预留部分
op_outputs_usage = get_mem_op_outputs(op, include_ineligible_downstream=True)
op_mem_usage += max(op_outputs_usage - reserved_for_op_outputs[op], 0)
```

**图解**:
```
                    reserved_for_op_outputs = 8GB
                              │
op_outputs_usage = 12GB       │
┌─────────────────────────────┼───────────┐
│  在预留范围内 (8GB)          │ 超出 (4GB) │
│  (不计入 op_mem_usage)       │ (计入)     │
└─────────────────────────────┴───────────┘
```

#### Step 2.2: 构建完整的资源使用量对象

```python
op_usage = get_op_usage(op).copy(object_store_memory=op_mem_usage)
# op_usage 现在包含：CPU、GPU、调整后的内存
```

#### Step 2.3: 计算预留剩余和超额

```python
op_reserved = self._op_reserved[op]

# 预留剩余 = max(预留 - 使用, 0)
op_reserved_remaining = max(op_reserved - op_usage, 0)

# 预留超额 = max(使用 - 预留, 0)
op_reserved_exceeded = max(op_usage - op_reserved, 0)
```

**图解**:
```
场景A: 使用 < 预留           场景B: 使用 > 预留
┌─────────────┐              ┌─────────────┬─────┐
│   使用量    │  剩余        │   预留量    │超额 │
│             │◄────►        │             │◄───►│
└─────────────┴─────┘        └─────────────┴─────┘
     预留量                        使用量
```

#### Step 2.4: 更新预算和共享池

```python
# 初始预算 = 预留剩余
self._op_budgets[op] = op_reserved_remaining

# 从共享资源中扣除超额使用
remaining_shared = remaining_shared - op_reserved_exceeded
```

### 2.4 Phase 3: 分配共享资源

**位置**: 第 980-1018 行

**遍历顺序**: 逆序（下游 → 上游）

> 为什么逆序？下游算子更接近数据消费者，优先保证下游有资源可以处理数据，避免管道阻塞。

#### Step 3.1: 平均分配共享资源

```python
for i, op in enumerate(reversed(eligible_ops)):
    # 剩余算子数 = len(eligible_ops) - i
    op_shared = remaining_shared × (1 / 剩余算子数)
```

**示例** (3个算子，remaining_shared = 30GB):
```
迭代1 (op3): op_shared = 30 × (1/3) = 10GB, remaining = 20GB
迭代2 (op2): op_shared = 20 × (1/2) = 10GB, remaining = 10GB
迭代3 (op1): op_shared = 10 × (1/1) = 10GB, remaining = 0GB
```

#### Step 3.2: 借用机制

如果算子的预算不足以运行最小任务，允许向上游"借用"：

```python
# 计算需要借用的资源
to_borrow = max(min_scheduling_resources - (current_budget + op_shared), 0)

# 如果借用后仍在共享池范围内，允许借用
if to_borrow > 0 and (op_shared + to_borrow) ≤ remaining_shared:
    op_shared = op_shared + to_borrow
```

**图解**:
```
min_scheduling_resources = 15GB
current_budget = 2GB
op_shared (平均分配) = 8GB
─────────────────────────────────
current_budget + op_shared = 10GB < 15GB
to_borrow = 15 - 10 = 5GB

借用后: op_shared = 8 + 5 = 13GB
```

#### Step 3.3: 上限约束

如果算子有最大资源使用限制：

```python
if max_resource_usage != inf:
    total_reserved = _get_total_reserved(op)  # 预留总量
    current_allocation = max(total_reserved, op_usage)

    # 最多还能分配的共享资源
    max_shared = max(max_resource_usage - current_allocation, 0)

    # 限制 op_shared 不超过上限
    op_shared = min(op_shared, max_shared)
```

#### Step 3.4: 更新预算和共享池

```python
remaining_shared = remaining_shared - op_shared
self._op_budgets[op] = self._op_budgets[op] + op_shared
```

### 2.5 Phase 4: 处理剩余资源和特殊情况

**位置**: 第 1020-1037 行

#### Step 4.1: 剩余资源分配

由于上限约束，可能有剩余的共享资源没分配完：

```python
if remaining_shared > 0:
    # 找最下游的无上限算子，把剩余资源给它
    for op in reversed(eligible_ops):
        if max_resource_usage == inf:
            self._op_budgets[op] += remaining_shared
            break
```

#### Step 4.2: 物化算子特殊处理

`AllToAllOperator` 等物化算子需要等待所有上游输出才能处理：

```python
for op in eligible_ops:
    if _is_blocking_materializing_op(op):
        # 禁用对象存储内存反压，避免死锁
        self._op_budgets[op].object_store_memory = float("inf")
```

**为什么需要这个处理？**
```
map → sort (物化算子)

map 必须输出所有数据 → sort 才能开始排序
如果 map 因内存反压被阻塞 → sort 永远等不到完整输入 → 死锁
```

### 2.6 数据结构更新总结

| 数据结构 | 更新时机 | 含义 |
|---------|---------|------|
| `_op_reserved[op]` | Phase 1 | 算子的任务预留资源（不含输出） |
| `_reserved_for_op_outputs[op]` | Phase 1 | 算子输出的专用预留内存 |
| `_total_shared` | Phase 1 | 共享资源池总量 |
| `_op_budgets[op]` | Phase 2 初始化，Phase 3 累加 | 算子的最终可用预算 |

### 2.7 最终预算计算公式

```
_op_budgets[op] = max(reserved - usage, 0)           # Phase 2: 预留剩余
                + allocated_shared                    # Phase 3: 分配的共享
                + extra_remaining (如果是最下游无上限) # Phase 4.1

如果是物化算子的上游:
    _op_budgets[op].object_store_memory = ∞          # Phase 4.2
```

### 2.8 为什么 `include_ineligible_downstream=True`

在计算 `op_outputs_usage` 时：

```python
op_outputs_usage = self._resource_manager.get_mem_op_outputs(
    op, include_ineligible_downstream=True
)
```

**原因**：

1. **内存归属**：Ineligible 算子（如 `limit`）不参与资源预留，但它们的输出队列会占用内存
2. **准确计算**：这部分内存必须计入上游 eligible 算子的账户
3. **防止过度分配**：如果不包含，系统会认为还有更多可用资源，可能导致 OOM

```python
# get_mem_op_outputs 实现 (resource_manager.py:341-357)
def get_mem_op_outputs(self, op, include_ineligible_downstream=False):
    op_outputs_usage = self._mem_op_outputs[op]

    if not include_ineligible_downstream:
        return op_outputs_usage

    # 加上下游所有 ineligible 算子的资源使用
    return (
        op_outputs_usage
        + self._get_downstream_ineligible_ops_usage(op).object_store_memory
    )
```

---

## 3. 为什么某些算子禁用节流

### 3.1 节流 (Throttling) 的作用

在 Ray Data 中，**节流**是一种反压机制：
- 当算子使用的资源（CPU、内存）超过预算时，调度器会**暂停向该算子发送新任务**
- 目的是防止内存溢出（OOM）和资源争抢

```
┌─────────────────────────────────────────────────────────────────┐
│  正常算子的节流机制                                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  上游 ──[数据]──► 当前算子 ──[数据]──► 下游                       │
│                     │                                           │
│                     ▼                                           │
│              如果资源使用 > 预算                                  │
│                     │                                           │
│                     ▼                                           │
│              暂停接收上游数据（反压）                              │
│                                                                 │
└────────────────────────────────────────────────────────────┘
```

### 3.2 LimitOperator - 因为它是"数据终结者"

**位置**: `limit_operator.py:132-133`

```
def throttling_disabled(self) -> bool:
    return True
```

**原因分析**:

```
read_parquet() ──► map() ──► limit(100) ──► 输出
     │               │           │
     │               │           └── 只需要前100行就停止
     │               │
     │               └── 处理数据
     │
     └── 可能有百万行数据
```

1. **`limit` 的职责是截断数据流**：一旦达到限制就立即停止
2. **如果对 `limit` 进行节流**：
   - 上游会被反压，减慢数据产生速度
   - 但 `limit` 本身几乎不消耗资源（只是转发数据）
   - 反而会延长整个管道的执行时间
3. **`limit` 的内存占用很小**：它不运行任务，只是检查行数并转发数据块

### 3.3 OutputSplitter (streaming_split) - 因为它只操作元数据

**位置**: `output_splitter.py:111-118`

```python
def throttling_disabled(self) -> bool:
    """Disables resource-based throttling.

    It doesn't make sense to throttle the inputs to this operator, since all that
    would do is lower the buffer size and prevent us from emitting outputs /
    reduce the locality hit rate.
    """
    return True
```

**原因分析**:

```
map() ──► OutputSplitter(n=3) ──► split_0 ──► consumer_0
                │                 split_1 ──► consumer_1
                │                 split_2 ──► consumer_2
                │
                └── 只是给数据块打上 split_idx 标签
```

1. **`OutputSplitter` 只操作元数据**：不会产生新数据，只是给 `RefBundle` 设置 `output_split_idx`
2. **如果进行节流**：
   - 缓冲区变小 → 无法有效进行**局部性优化**（将数据发送到靠近数据的节点）
   - 下游消费者得不到数据 → 整个管道阻塞
3. **它是管道的"出口"**：节流出口只会造成整体吞吐下降

### 3.4 AllToAllOperator (sort, shuffle) - 因为节流会导致死锁

**位置**: `base_physical_operator.py:238-240`

```python
def throttling_disabled(self) -> bool:
    # Disable resource allocation and throttling for the operator
    return True
```

**原因分析**:

`AllToAllOperator` 是一种**阻塞式物化算子**：它必须收集**所有**输入数据后才能开始处理。

```
┌─────────────────────────────────────────────────────────────────┐
│  AllToAllOperator 的工作方式 (如 sort)                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  map() ──[所有数据]──► sort() ──[排序后数据]──► 下游              │
│                         │                                       │
│                         ▼                                       │
│                   all_inputs_done()                             │
│                         │                                       │
│                         ▼                                       │
│                   执行排序逻辑                                   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**如果对 `AllToAllOperator` 进行节流**：

```
死锁场景:

1. sort() 预算 = 10GB
2. 上游 map() 产生 15GB 数据
3. sort() 的 _input_buffer 达到 10GB
4. 节流触发 → 反压上游 map()
5. map() 停止产生数据
6. sort() 永远等不到 all_inputs_done()
7. 💀 死锁！

             ┌──────────────────────┐
             │   sort() 在等待      │
             │   所有输入完成        │
             └──────────┬───────────┘
                        │ 但是
             ┌──────────▼───────────┐
             │   map() 被反压        │
             │   无法继续产生数据     │
             └──────────────────────┘
```

这就是为什么在 `update_budgets` 中有特殊处理（第 1029-1037 行）：

```python
# A materializing operator like `AllToAllOperator` waits for all its input
# operator's outputs before processing data. This often forces the input
# operator to exceed its object store memory budget. To prevent deadlock, we
# disable object store memory backpressure for the input operator.
for op in eligible_ops:
    if self._resource_manager._is_blocking_materializing_op(op):
        self._op_budgets[op] = self._op_budgets[op].copy(
            object_store_memory=float("inf")
        )
```

### 3.5 其他禁用节流的算子

| 算子 | 文件位置 | 原因 |
|------|---------|------|
| `ZipOperator` | `zip_operator.py:150` | 需要同时从多个输入获取数据，节流会导致不平衡 |
| `AggregateNumRows` | `aggregate_num_rows.py:60` | 只统计行数，不产生实际数据 |

### 3.6 禁用节流的算子如何被资源管理器处理

这些算子变成 **ineligible**：
1. **不参与资源预留**：不会为它们分配专用预算
2. **不会被节流**：调度器不会因为它们超过预算而阻止上游
3. **资源使用被归属到上游**：它们的内存占用计入上游 eligible 算子的账户

### 3.7 总结

| 算子类型 | 禁用节流原因 | 风险控制 |
|---------|-------------|---------|
| **LimitOperator** | 不消耗资源，节流只会拖慢执行 | 内存占用归属上游 |
| **OutputSplitter** | 只操作元数据，是管道出口 | 内存占用归属上游 |
| **AllToAllOperator** | 阻塞式，节流会导致死锁 | 上游内存预算设为无限 |
| **ZipOperator** | 需要同步多个输入 | 内存占用归属上游 |

**核心设计原则**：
> 对于不消耗计算资源或需要完整输入才能工作的算子，节流是**有害**的。它们的资源使用被"转嫁"给上游 eligible 算子来统一管理。

---

## 4. update_usages 反向遍历分析

### 4.1 代码位置

**`update_usages` 方法**: `resource_manager.py:213-267`

```python
def update_usages(self):
    """Recalculate resource usages."""
    # ...
    # Iterate from last to first operator.
    for op, state in reversed(self._topology.items()):  # ← 反向遍历
        # ...
        used_object_store = self._estimate_object_store_memory_usage(op, state)
        # ...
```

### 4.2 `_estimate_object_store_memory_usage` 的依赖关系

```python
# resource_manager.py:163-211
def _estimate_object_store_memory_usage(self, op, state) -> int:
    # ...
    # Outputs of this operator used downstream
    used_op_outputs_bytes = sum(
        [
            (
                # Blocks pending in the downstream (internal) input queue
                downstream_op.metrics.obj_store_mem_internal_inqueue
                +
                # Blocks used as inputs of downstream's active tasks
                downstream_op.metrics.obj_store_mem_pending_task_inputs
            )
            for downstream_op in op.output_dependencies  # ← 需要访问下游算子的 metrics
        ]
    )
    # ...
```

### 4.3 反向遍历的原始意图

如果下游 metrics 是在 `update_usages` 中更新的，那么必须反向遍历以确保访问到最新值：

```
如果正向遍历 (op1 → op4):
  处理 op1 时需要 op2.metrics ← 但 op2 还没更新！

如果反向遍历 (op4 → op1):
  处理 op1 时需要 op2.metrics ← op2 已经更新过了 ✓
```

### 4.4 实际情况：metrics 是实时更新的

查看 `obj_store_mem_internal_inqueue` 的实现：

```python
# op_runtime_metrics.py:643-644
@metric_property
def obj_store_mem_internal_inqueue(self) -> int:
    return self._internal_inqueue.estimate_size_bytes()
```

这是一个 **`@metric_property`（只读属性）**，它直接从 `_internal_inqueue` 队列计算大小。

而 `_internal_inqueue` 的更新是通过回调函数**实时进行**的：

```python
# op_runtime_metrics.py:799-802
def on_input_queued(self, input: RefBundle):
    """Callback when the operator queues an input."""
    self._internal_inqueue.add(input)

# op_runtime_metrics.py:804-808
def on_input_dequeued(self, input: RefBundle):
    """Callback when the operator dequeues an input."""
    self._internal_inqueue.remove(input)
```

### 4.5 结论：反向遍历可能不是必需的

| metrics 字段 | 更新时机 | 是否需要反向遍历 |
|-------------|---------|----------------|
| `obj_store_mem_internal_inqueue` | **实时**（`on_input_queued/dequeued` 回调） | ❌ 不需要 |
| `obj_store_mem_pending_task_inputs` | **实时**（任务调度时更新） | ❌ 不需要 |
| `obj_store_mem_internal_outqueue` | **实时**（`on_output_queued/dequeued` 回调） | ❌ 不需要 |

因为这些 metrics 都是**在数据流动时实时更新**的，而不是在 `update_usages()` 中更新，所以：

```python
# 在 _estimate_object_store_memory_usage 中访问下游 metrics
used_op_outputs_bytes = sum([
    downstream_op.metrics.obj_store_mem_internal_inqueue  # ← 已经是最新值
    + downstream_op.metrics.obj_store_mem_pending_task_inputs  # ← 已经是最新值
    for downstream_op in op.output_dependencies
])
```

**无论正向还是反向遍历，访问到的下游 metrics 都是最新的。**

### 4.6 为什么代码仍然使用反向遍历？

可能的原因：

1. **历史遗留**：早期版本可能有在 `update_usages` 中更新 metrics 的逻辑，后来重构了但没改遍历顺序

2. **防御性编程**：即使现在不需要，保持反向遍历也不会出错，而且如果未来有人添加了依赖下游的计算逻辑，反向遍历仍然是正确的

3. **与 `update_budgets` 保持一致**：`update_budgets` 中的共享资源分配确实需要逆序（优先下游），可能为了代码风格一致

### 4.7 总结

| 遍历方式 | 当前代码是否需要 | 说明 |
|---------|----------------|------|
| **反向遍历** | ❌ 不是必需的 | 因为 metrics 是实时更新的，不依赖遍历顺序 |
| **正向遍历** | ✅ 同样可行 | 功能上等价 |

**就当前的实现而言，`update_usages` 中的反向遍历不是必需的，但保留它也不会造成问题。**

---

## 附录：关键文件路径

| 文件 | 路径 |
|------|------|
| ResourceManager | `python/ray/data/_internal/execution/resource_manager.py` |
| OpRuntimeMetrics | `python/ray/data/_internal/execution/interfaces/op_runtime_metrics.py` |
| LimitOperator | `python/ray/data/_internal/execution/operators/limit_operator.py` |
| OutputSplitter | `python/ray/data/_internal/execution/operators/output_splitter.py` |
| AllToAllOperator | `python/ray/data/_internal/execution/operators/base_physical_operator.py` |
| PhysicalOperator | `python/ray/data/_internal/execution/interfaces/physical_operator.py` |
