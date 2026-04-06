# Ray Data Operator 完成状态判定与 Blocks Outputted 机制分析

## 概述

本文档分析 Ray Data 中 operator 完成状态的判定机制，以及 `Blocks Outputted` 指标的分子/分母含义、更新时机，并解释为什么有时会出现 `6659/6691` 这样分子分母不一致的现象。

---

## 1. Operator 完成状态判定

### 1.1 两层完成状态

Ray Data 中每个 operator 有两层完成状态：

#### 第一层：执行完成 (`has_execution_finished()`)

文件：`python/ray/data/_internal/execution/interfaces/physical_operator.py:514-535`

```python
def has_execution_finished(self) -> bool:
    return self._is_execution_marked_finished or (
        self._inputs_complete
        and self.num_active_tasks() == 0
        and internal_input_queue_num_blocks == 0
    )
```

触发条件有两条路径：

**路径 1 -- 自动完成（自底向上）**：三个条件必须同时满足：
- `self._inputs_complete == True`：所有上游依赖已完成且输出队列为空时，通过 `all_inputs_done()` 设置
- `self.num_active_tasks() == 0`：无正在运行的 task
- `internal_input_queue_num_blocks == 0`：operator 内部输入缓冲区已排空

**路径 2 -- 显式标记（自顶向下）**：直接调用 `mark_execution_finished()`，发生在：
- `LimitOperator` 达到行数限制时
- `InputDataBuffer` 启动时（无 task，仅传递预物化 block）
- 反向传播：所有下游 operator 已完成时，上游也被标记完成

#### 第二层：完全完成 (`has_completed()`)

文件：`physical_operator.py:537-559`

```python
def has_completed(self) -> bool:
    return (
        self.has_execution_finished()
        and internal_output_queue_num_blocks == 0
        and not self.has_next()
    )
```

比执行完成更严格——不仅要求执行结束，还要求所有输出已被下游完全消费。

#### 第三层：OpState `_finished`（消费者线程信号）

文件：`streaming_executor_state.py:283, 434-439`

```python
def mark_finished(self, exception: Optional[Exception] = None):
    """Marks this operator as finished. Used for exiting get_output_blocking."""
    if exception is None:
        self._finished = True
    else:
        self._exception = exception
```

仅设置在最终输出 operator 上，用于向消费者线程发出 `StopIteration` 信号。

---

### 1.2 `_inputs_complete` 的设置时机

文件：`streaming_executor_state.py:748-768`

```python
def update_operator_states(topology: Topology) -> None:
    for op, op_state in topology.items():
        if op_state.inputs_done_called:
            continue
        all_inputs_done = True
        for idx, dep in enumerate(op.input_dependencies):
            if dep.has_completed() and not topology[dep].output_queue:
                if not op_state.input_done_called[idx]:
                    op.input_done(idx)
                    op_state.input_done_called[idx] = True
            else:
                all_inputs_done = False

        if all_inputs_done:
            op.all_inputs_done()       # 设置 _inputs_complete = True
            op_state.inputs_done_called = True
```

当上游所有依赖 operator 的 `has_completed()` 为 True 且它们的 output_queue 为空时，当前 operator 的 `all_inputs_done()` 被调用。

---

## 2. Blocks Outputted 分子/分母详解

### 2.1 日志格式

文件：`streaming_executor.py:906`

```python
f"Blocks Outputted: {state.num_completed_tasks}/{op.num_outputs_total()}"
```

---

### 2.2 分子：`state.num_completed_tasks`

#### 含义

Operator 已经产出并被 pull 到 `OpState.output_queue` 中的 **RefBundle 数量**。

每成功从 operator 内部队列 pull 出一个 RefBundle，分子就 +1。

#### 初始化

文件：`streaming_executor_state.py:278`

```python
self.num_completed_tasks = 0
```

#### 更新时机

在调度循环的 **Phase 5: Pull outputs** 阶段更新。

文件：`streaming_executor_state.py:730-735`

```python
# ===== Phase 5: Pull outputs =====
# Pull any operator outputs into the streaming op state.
for op, op_state in topology.items():
    while op.has_next():
        op_state.add_output(op.get_next())
```

`add_output` 内部逻辑（`streaming_executor_state.py:352-366`）：

```python
def add_output(self, ref: RefBundle) -> None:
    """Move a bundle produced by the operator to its outqueue."""
    ref, diverged = dedupe_schemas_with_validation(...)
    self._schema = ref.schema
    self._warned_on_schema_divergence |= diverged
    self.output_queue.append(ref)
    self.num_completed_tasks += 1  # 分子 +1
```

#### 关键约束

分子能否增长取决于 `op.has_next()` 的返回值：

对于 `MapOperator`（`map_operator.py:679-681`）：

```python
def has_next(self) -> bool:
    assert self._started
    return self._output_queue.has_next()
```

`self._output_queue` 是一个 `BundleQueue`，有两种实现：

- **FifoBundleQueue**：先进先出，只要队列非空就返回 True
- **ReorderingBundleQueue**（`preserve_order=True` 时使用）：必须按 task 提交顺序释放 block

`ReorderingBundleQueue` 的 `has_next()` 逻辑（`bundle_queue/reordering.py:50-57`）：

```python
def has_next(self) -> bool:
    while (
        self._current_key in self._finalized_keys
        and len(self._inner[self._current_key]) == 0
    ):
        self._move_to_next_key()
    return len(self._inner[self._current_key]) > 0
```

只有当前 key（按顺序的 task_index）对应的 deque 中有 bundle 时才返回 True。后续 task 产出的 block 即使已在队列中，也不会被释放。

---

### 2.3 分母：`op.num_outputs_total()`

#### 含义

Operator 总共应该产出的 block 数量。执行完成后为实际产出数，执行中为估算值与实际值的较大者。

#### 计算逻辑

文件：`physical_operator.py:626-651`

```python
def num_outputs_total(self) -> Optional[int]:
    actual = self._metrics.num_task_outputs_generated  # 来源 A：实际计数
    estimated = self._estimated_num_output_bundles     # 来源 B：估算值

    # 执行完成后，返回确定值
    if self.has_execution_finished():
        if actual > 0:
            return actual
        return estimated if estimated is not None else 0

    # 执行中，取估算和实际的较大者
    if estimated is not None:
        return max(estimated, actual)
    return actual if actual > 0 else None
```

#### 来源 A：`num_task_outputs_generated`（实际计数）

**定义**（`op_runtime_metrics.py:280-283`）：

```python
num_task_outputs_generated: int = metric_field(
    default=0,
    description="Number of output blocks generated by tasks.",
)
```

**更新时机**：在 task 的 output ready callback 中更新，即 Ray 远程 task 每产出一批 block 时立即触发。

调用链：

```
远程 task 产出一个 block
  → DataOpTask 收到 output
    → _output_ready_callback(task_index, output)   [map_operator.py:598-605]
      → self._metrics.on_task_output_generated(task_index, output)
      → self._output_queue.add(output, key=task_index)
```

具体逻辑（`map_operator.py:598-606`）：

```python
def _output_ready_callback(task_index, output: RefBundle):
    # Since output is streamed, it should only contain one block.
    assert len(output) == 1
    self._metrics.on_task_output_generated(task_index, output)  # 更新分母
    self._output_queue.add(output, key=task_index)  # 放入内部 BundleQueue
    self._metrics.on_output_queued(output)
```

`on_task_output_generated` 内部（`op_runtime_metrics.py:882-888`）：

```python
def on_task_output_generated(self, task_index: int, output: RefBundle):
    """Callback when a new task generates an output."""
    num_outputs = len(output)  # RefBundle 中的 block 数量（streaming 模式下通常为 1）
    output_bytes = output.size_bytes()
    num_rows_produced = output.num_rows()

    self.num_task_outputs_generated += num_outputs  # 分母的实际计数 +1
    self.bytes_task_outputs_generated += output_bytes
    self.rows_task_outputs_generated += num_rows_produced
```

#### 来源 B：`_estimated_num_output_bundles`（估算值）

在执行过程中，每个 task 完成时会重新估算总 block 数量。估算公式：

```
estimated = upstream_op_num_outputs / average_num_inputs_per_task * average_num_outputs_per_task
```

当执行尚未完成且 `estimated is not None` 时，分母 = `max(estimated, actual)`。

---

## 3. 分子分母不一致的原因分析

### 3.1 完整数据流路径

```
Task 产出 block（远程执行）
  ↓
_output_ready_callback()
  → metrics.num_task_outputs_generated += 1     ← 【分母更新】
  → self._output_queue.add(output, key=task_index)  ← 进入内部 BundleQueue
  ↓
...... 等待下一次调度循环迭代 ......
  ↓
Phase 5: Pull outputs
  → op.has_next()    ← BundleQueue 判断是否有可取的 bundle
  → op.get_next()    ← 从 BundleQueue 取出
  → op_state.add_output()
    → num_completed_tasks += 1   ← 【分子更新】
```

### 3.2 不一致场景

| 场景 | 详细说明 |
|------|----------|
| **排序等待（最常见）** | `preserve_order=True` 时，task N 的 block 已生成（分母 +1），但 task N-1 还没 finalize，`ReorderingBundleQueue.has_next()` 返回 False，分子无法更新 |
| **调度循环延迟** | 分母在 output callback 中立即更新（异步），分子要等到下一轮调度循环的 Phase 5 才更新 |
| **执行结束瞬间** | operator 已标记 finished（无活跃 task + inputs_complete + 内部输入队列空），但内部 output BundleQueue 中还有未被 pull 的 block |
| **背压** | 下游消费不够快时，pull 操作可能被延迟 |

### 3.3 示例：6659/6691 的含义

- **6691**（分母）：task 们实际已经生产出了 6691 个 block，都已经通过 `_output_ready_callback` 进入了 operator 内部的 BundleQueue
- **6659**（分子）：只有 6659 个 block 被成功从内部 BundleQueue pull 到了 `OpState.output_queue`
- **差值 32**：有 32 个 block 卡在 `ReorderingBundleQueue` 中，等待前序 task finalize 后按序释放

### 3.4 最终是否会对齐

**会**。当所有 task 完成并 finalize 后，`ReorderingBundleQueue` 中的所有 block 都会被释放，Phase 5 会逐步把它们 pull 出来，最终分子 = 分母。

如果在 operator 标记为 finished 时观察到不一致，说明观察时机处于"执行已完成但输出尚未完全排空"的中间状态，属于正常现象。

---

## 4. 整个 Job 的完成判定

### 4.1 调度循环退出条件

文件：`streaming_executor.py:665`

```python
def _scheduling_loop_step(self, topology: Topology) -> bool:
    ...
    # Keep going until all operators run to completion.
    return not all(op.has_completed() for op in topology)
```

当拓扑中 **所有** operator 的 `has_completed()` 都为 True 时，调度循环退出。

### 4.2 级联完成流程

```
1. InputDataBuffer 最先完成（无 task，仅传递预物化 block）
       ↓
2. 上游 operator 完成 + output_queue 清空
       ↓ 触发 all_inputs_done()
3. 下游 operator 满足自动完成条件
       ↓ 逐层传播
4. 最终 output operator 完成
       ↓
5. _scheduling_loop_step() 返回 False，循环退出
       ↓
6. state.mark_finished() 通知消费者线程
       ↓
7. 设置 DatasetState.FINISHED，调用 op.shutdown() 清理
```

### 4.3 DatasetState 枚举

文件：`python/ray/data/_internal/execution/dataset_state.py`

```python
class DatasetState(enum.IntEnum):
    UNKNOWN = 0
    RUNNING = 1
    FINISHED = 2
    FAILED = 3
    PENDING = 4
```

---

## 5. 关键文件索引

| 文件 | 关键行号 | 内容 |
|------|----------|------|
| `python/ray/data/_internal/execution/interfaces/physical_operator.py` | 514-559 | `has_execution_finished()`, `has_completed()` |
| 同上 | 626-651 | `num_outputs_total()` 分母计算逻辑 |
| `python/ray/data/_internal/execution/streaming_executor_state.py` | 278, 352-366 | `num_completed_tasks` 定义和 `add_output()` 更新 |
| 同上 | 730-735 | Phase 5: Pull outputs 调度逻辑 |
| 同上 | 748-768 | `update_operator_states()` inputs_done 传播 |
| `python/ray/data/_internal/execution/streaming_executor.py` | 460-487 | 主调度循环 |
| 同上 | 665 | `_scheduling_loop_step()` 退出条件 |
| 同上 | 906 | `Blocks Outputted` 日志格式 |
| `python/ray/data/_internal/execution/interfaces/op_runtime_metrics.py` | 280-283 | `num_task_outputs_generated` 定义 |
| 同上 | 882-888 | `on_task_output_generated()` 分母更新逻辑 |
| `python/ray/data/_internal/execution/operators/map_operator.py` | 598-606 | `_output_ready_callback` 数据流入口 |
| 同上 | 679-681 | `has_next()` 委托给 BundleQueue |
| `python/ray/data/_internal/execution/bundle_queue/reordering.py` | 14-80 | `ReorderingBundleQueue` 排序逻辑 |
| `python/ray/data/_internal/execution/bundle_queue/fifo.py` | - | `FifoBundleQueue` FIFO 逻辑 |
| `python/ray/data/_internal/execution/dataset_state.py` | - | `DatasetState` 枚举定义 |
