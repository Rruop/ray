# Write 算子 output_rows Metric 修复

## 问题描述

### 问题 1：Write 算子的 output_rows 显示错误
- **现象**：Write 算子的 `output_rows` 指标显示的是 task 数量（每个 task 显示 1），而不是实际写入的行数
- **根因**：Write 算子在 `generate_collect_write_stats_fn()` 中输出的是 1 行的 stats DataFrame（包含 `num_rows`, `size_bytes` 等统计信息），而 `BlockAccessor.get_metadata()` 返回的 `num_rows` 是这个 DataFrame 的行数（=1），而非实际写入的数据行数

### 问题 2：Blocks Outputted 显示 X/Y 时 X > Y
- **现象**：进度条显示 `Blocks Outputted: 5/4` 这样的情况
- **根因**：`num_outputs_total()` 返回的估算值基于平均值计算，在某些情况下可能小于实际已输出的数量

## 解决方案

### 核心思路
在源头（`_map_task`）修复 metadata，而不是在下游（`on_output_taken`）做特殊处理。

### 修改文件

#### 1. `python/ray/data/_internal/planner/plan_write_op.py`

在 `generate_collect_write_stats_fn()` 中，将实际写入的行数和字节数存储到 `TaskContext.kwargs`：

```python
def fn(blocks: Iterator[Block], ctx: TaskContext) -> Iterator[Block]:
    """Handles stats collection for block writes."""
    block_accessors = [BlockAccessor.for_block(block) for block in blocks]
    total_num_rows = sum(ba.num_rows() for ba in block_accessors)
    total_size_bytes = sum(ba.size_bytes() for ba in block_accessors)

    # Store actual write stats in context for _map_task to use when
    # constructing BlockMetadata. This ensures the output block's metadata
    # reflects the actual rows/bytes written, not the stats DataFrame's 1 row.
    ctx.kwargs["_write_stats_num_rows"] = total_num_rows
    ctx.kwargs["_write_stats_size_bytes"] = total_size_bytes

    # ... rest of function
```

#### 2. `python/ray/data/_internal/execution/operators/map_operator.py`

在 `_map_task()` 中检测并使用 `ctx.kwargs` 中的实际值来覆盖 metadata：

```python
with MemoryProfiler(data_context.memory_usage_poll_interval_s) as profiler:
    for block in map_transformer.apply_transform(block_iter, ctx):
        block_meta = BlockAccessor.for_block(block).get_metadata()
        block_schema = BlockAccessor.for_block(block).schema()

        # For Write operators, use actual written rows/bytes from context
        # instead of the stats DataFrame's 1 row.
        # NOTE: Write operators always produce exactly one output block per task,
        # so we can safely pop these values (they won't be needed again).
        if "_write_stats_num_rows" in ctx.kwargs:
            block_meta = replace(
                block_meta,
                num_rows=ctx.kwargs.pop("_write_stats_num_rows"),
                size_bytes=ctx.kwargs.pop("_write_stats_size_bytes"),
            )

        # ... rest of function
```

#### 3. `python/ray/data/_internal/execution/interfaces/physical_operator.py`

修改 `num_outputs_total()` 方法：
- 当 operator 已完成执行时，返回实际的 `num_task_outputs_generated`
- 执行中时，确保返回值 >= 实际已生成的输出数量

```python
def num_outputs_total(self) -> Optional[int]:
    """Returns the total number of output bundles of this operator."""
    # When execution is finished, return the actual count instead of estimate
    if self.has_execution_finish:
        return self._metrics.num_task_outputs_generated if self._metrics else 0

    estimated = self._estimated_num_output_bundles
    # Ensure estimate is at least as large as actual outputs generated
    actual = self._metrics.num_task_outputs_generated if self._metrics else 0
    if estimated is not None:
        return max(estimated, actual)
    return actual if actual > 0 else None
```

## 关键设计决策

### 为什么选择在源头修复而不是在 metrics 层特殊处理？

1. **无 `ray.get()` 调用**：避免在 `on_output_taken()` 热路径中调用 `ray.get()` 获取 stats DataFrame 内容
2. **Metadata 从源头正确**：所有下游的 metrics 追踪代码都能统一工作
3. **遵循现有模式**：使用 `ctx.kwargs` 传递信息是已有的模式（如 `_datasink_write_return`）
4. **线程安全**：每个 task 有独立的 `TaskContext`，无并发问题

### 为什么 `num_outputs_total()` 需要在完成后返回实际值？

- `_estimated_num_output_bundles` 是基于平均值估算的：`estimated_num_tasks * average_num_outputs_per_task`
- 即使所有 tasks 完成，由于舍入误差，估算值可能和实际值略有不同
- 在作业完成后返回实际值可以确保 `Blocks Outputted: X/Y` 中 X 和 Y 最终相等

## 关于 `_estimated_num_output_bundles` 和 `num_completed_tasks` 的关系

### 指标含义

| 指标 | 含义 | 更新时机 |
|------|------|----------|
| `_estimated_num_output_bundles` | 估算的 output bundles 总数 | 每个 task 完成时更新 |
| `state.num_completed_tasks` | 实际输出的 bundles 数量 | 每次 `add_output()` 调用时 +1 |
| `num_task_outputs_generated` | 实际生成的 blocks 数量 | 每次 `on_task_output_generated()` 时累加 |

### 关键发现

1. **`num_completed_tasks` 命名有误导性**：它实际计数的是输出的 **bundles** 数量，而不是完成的 tasks 数量

2. **在 streaming output 场景下**，每个 `RefBundle` 恰好包含 1 个 block（见 `map_operator.py` 第 602-603 行的注释和断言）：
   ```python
   # Since output is streamed, it should only contain one block.
   assert len(output) == 1
   ```

3. **因此以下三个值最终应该相等**：
   - `num_task_outputs_generated`（blocks 数量）
   - `state.num_completed_tasks`（bundles 数量）
   - `_estimated_num_output_bundles`（估算的 bundles 数量）

### 估算逻辑

`_estimated_num_output_bundles` 在每个 task 完成时通过 `estimate_total_num_of_blocks()` 更新：

```python
estimated_num_output_bundles = round(
    estimated_num_tasks * metrics.average_num_outputs_per_task
)
```

其中：
- `estimated_num_tasks = upstream_op_num_outputs / metrics.average_num_inputs_per_task`
- `average_num_outputs_per_task = num_outputs_of_finished_tasks / num_tasks_finished`

### 为什么可能出现 X > Y？

在作业**进行中**，由于估算基于平均值，可能出现短暂的不一致：

1. **早期 tasks 产出较少**：如果早期完成的 tasks 产出的 outputs 数量少于平均值，估算值会偏低
2. **舍入误差**：`round()` 可能导致估算值略有偏差
3. **估算更新滞后**：估算在 task 完成时更新，但 outputs 是 streaming 产出的

### 解决方案

修改 `num_outputs_total()` 确保：
1. **执行中**：返回 `max(estimated, actual)`，保证不小于实际值
2. **完成后**：优先返回 `actual`（如果 > 0），否则返回 `estimated`
   - 对于运行 tasks 的算子（如 MapOperator），`actual` 会有正确的值
   - 对于不运行 tasks 的算子（如 InputDataBuffer），`actual` 为 0，使用 `estimated`

```python
def num_outputs_total(self) -> Optional[int]:
    actual = self._metrics.num_task_outputs_generated if self._metrics else 0
    estimated = self._estimated_num_output_bundles

    # When execution is finished, return the best known value
    if self.has_execution_finished():
        # For operators that don't run tasks (e.g., InputDataBuffer),
        # actual will be 0, so prefer estimated if available
        if actual > 0:
            return actual
        return estimated if estimated is not None else 0

    # During execution, ensure estimate is at least as large as actual
    if estimated is not None:
        return max(estimated, actual)
    return actual if actual > 0 else None
```

### 特殊情况：InputDataBuffer

`InputDataBuffer` 是一个不运行 tasks 的算子：
- 它在初始化时就设置 `_estimated_num_output_bundles = len(self._input_data)`
- 它不更新 `num_task_outputs_generated`（始终为 0）
- 因此在完成后需要使用 `estimated` 而非 `actual`

## 测试

### 新增测试文件
- `python/ray/data/tests/test_write_operator_metrics.py`：测试 Write 算子的 metrics 正确性

### 测试类

#### `TestWriteOperatorE2E`
- `test_basic_write`：基本写入测试
- `test_multiple_tasks`：多 task 写入测试

#### `TestWriteOperatorRowsMetric`（验证 Rows Outputted 指标）
- `test_write_rows_metric_basic`：验证 `rows_task_outputs_generated` 等于实际写入行数（而非 stats DataFrame 的 1 行）
- `test_write_rows_metric_multiple_blocks`：验证多 blocks 场景下 `rows_task_outputs_generated` 正确

#### `TestWriteStatsMetadata`
- `test_multiple_blocks`：测试多 blocks 的 stats 收集
- `test_single_block`：测试单 block 的 stats 收集

### 在 `test_map_operator.py` 中新增
- `test_num_outputs_total_returns_actual_when_finished`：测试完成后返回实际值
- `test_num_outputs_total_at_least_actual_during_execution`：测试执行中估算值 >= 实际值

## 总修改量

| 文件 | 新增行数 | 修改内容 |
|------|----------|----------|
| `plan_write_op.py` | +6 | 存储实际 stats 到 context |
| `map_operator.py` | +11 | 检测并覆盖 Write 算子的 metadata |
| `physical_operator.py` | +10 | 完善 `num_outputs_total()` 逻辑 |
| `test_map_operator.py` | +75 | 新增 2 个测试用例 |
| `test_write_operator_metrics.py` | +163 | 新增测试文件（含 Rows Outputted 指标测试） |

**核心代码修改**：约 27 行
**测试代码**：约 238 行
