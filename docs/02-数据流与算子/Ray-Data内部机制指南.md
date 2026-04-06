# Ray Data 内部机制指南

本文档整理了 Ray Data 执行引擎的内部机制，包括 Block 处理、反压机制、日志调试等内容。

---

## 目录

1. [启用 DEBUG 日志](#1-启用-debug-日志)
2. [Progress Manager 刷新机制](#2-progress-manager-刷新机制)
3. [Map 算子中 Block 和行数的关系](#3-map-算子中-block-和行数的关系)
4. [控制 Block 行数](#4-控制-block-行数)
5. [Block 输出组装机制](#5-block-输出组装机制)
6. [OutputBlockSizeOption 设置位置](#6-outputblocksizeoption-设置位置)
7. [反压机制分析](#7-反压机制分析)
8. [常见日志问题](#8-常见日志问题)

---

## 1. 启用 DEBUG 日志

### 方法 1：使用 runtime_env 配置文件

创建 `debug_logging.yaml`：

```yaml
version: 1
disable_existing_loggers: false
handlers:
  console:
    class: logging.StreamHandler
    level: DEBUG
    stream: ext://sys.stderr
loggers:
  ray.data:
    level: DEBUG
    handlers: [console]
    propagate: false
```

提交作业：

```bash
ray job submit \
  --working-dir . \
  --runtime-env-json='{"env_vars": {"RAY_DATA_LOGGING_CONFIG": "./debug_logging.yaml"}}' \
  -- python your_script.py
```

### 方法 2：在脚本中配置

```python
import logging
import ray

ray.init()

logger = logging.getLogger("ray.data")
logger.setLevel(logging.DEBUG)
for handler in logger.handlers:
    handler.setLevel(logging.DEBUG)

if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
```

### 查看日志

```bash
ray job logs <job_id>
```

---

## 2. Progress Manager 刷新机制

### `_refresh_progress_manager` 方法

位置：`python/ray/data/_internal/execution/streaming_executor.py:667-675`

```python
def _refresh_progress_manager(self, topology: Topology):
    if self._progress_manager:
        for op_state in topology.values():
            if not isinstance(op_state.op, InputDataBuffer):
                self._progress_manager.update_operator_progress(
                    op_state, self._resource_manager
                )
        self._progress_manager.refresh()
```

### 调用周期

| 调用时机 | 周期 |
|---------|------|
| dispatch 循环中 | 每 **50 个** dispatch 操作（`TOTAL_PROGRESS_REFRESH_EVERY_N_STEPS`） |
| 调度循环结束 | 每次 `_scheduling_loop_step` 完成 |

### Progress Manager 类型

| 环境 | Progress Manager 类型 |
|------|----------------------|
| 非交互式终端 (ray job submit) | `LoggingExecutionProgressManager` |
| 交互式终端 + rich 启用 | `RichExecutionProgressManager` |
| 交互式终端 + tqdm | `TqdmExecutionProgressManager` |
| 禁用进度条 | `NoopExecutionProgressManager` |

---

## 3. Map 算子中 Block 和行数的关系

### 核心概念

```
Block (块) > Batch (批) > Row (行)
```

- **Block**：Ray Data 中的基本数据单元（PyArrow Table 或 Pandas DataFrame）
- **Batch**：固定大小的数据批次
- **Row**：单行数据

### Transform 类型

| 类型 | 输入/输出粒度 | 预处理逻辑 |
|------|-------------|-----------|
| `BlockMapTransformFn` | Block → Block | 直接传入整个 block |
| `BatchMapTransformFn` | Batch → Batch | 将 block 拆分为固定大小的 batch |
| `RowMapTransformFn` | Row → Row | 将 block 逐行迭代 |

### 按行执行的逻辑 (`RowMapTransformFn`)

```python
# map_transformer.py:305-309
def _pre_process(self, blocks: Iterable[Block]) -> Iterable[MapTransformFnData]:
    for block in blocks:
        block = BlockAccessor.for_block(block)
        for row in block.iter_rows(public_row_format=True):  # 逐行迭代
            yield row
```

### Block 与 Task 的关系

- **多个 Block** 可以被 bundled 到 **一个 Task**
- **一个 Block** 不会被拆分给 **多个 Task**
- Map task 使用 `num_returns="streaming"`，每处理完一个 output block 就立即 yield 给下游

---

## 4. 控制 Block 行数

### 4.1 控制上游 Block 行数（Map 输入）

#### 方法 A：读取时指定 block 数量

```python
ds = ray.data.read_parquet("data.parquet", override_num_blocks=100)
```

#### 方法 B：使用 repartition（推荐）

```python
ds = ray.data.read_parquet("data.parquet")
ds = ds.repartition(target_num_rows_per_block=1000)  # 每个 block 最多 1000 行
ds = ds.map(my_fn)
```

### 4.2 控制下游 Block 行数（Map 输出）

#### 方法 A：设置 target_max_block_size（按字节控制）

```python
from ray.data import DataContext

ctx = DataContext.get_current()
ctx.target_max_block_size = 10 * 1024 * 1024  # 10MB
```

#### 方法 B：Map 后使用 repartition

```python
ds = ds.map(my_fn)
ds = ds.repartition(target_num_rows_per_block=500)
```

### 4.3 参数对照表

| 目标 | 参数/方法 | 控制粒度 |
|------|----------|----------|
| **上游输入** | | |
| 读取时指定 block 数 | `read_xxx(override_num_blocks=N)` | block 数量 |
| 重新分区 (按行数) | `repartition(target_num_rows_per_block=N)` | 每 block 行数 |
| 重新分区 (按数量) | `repartition(num_blocks=N)` | block 数量 |
| **下游输出** | | |
| 全局 block 大小 | `DataContext.target_max_block_size` | 字节大小 |
| Map 后重新分区 | `repartition(target_num_rows_per_block=N)` | 每 block 行数 |
| 批处理大小 | `map_batches(batch_size=N)` | 批行数 |

### 4.4 数据流图

```
读取数据
    ↓
[override_num_blocks] ← 控制读取时的 block 数
    ↓
[repartition(target_num_rows_per_block=X)] ← 精确控制上游每个 block 行数
    ↓
Map 算子
    ↓
[target_max_block_size] ← 控制输出 block 字节大小
    ↓
[repartition(target_num_rows_per_block=Y)] ← 精确控制下游每个 block 行数
    ↓
下游消费
```

---

## 5. Block 输出组装机制

### `_shape_blocks` 方法

位置：`python/ray/data/_internal/execution/operators/map_transformer.py:62-93`

```python
def _shape_blocks(self, results: Iterable[MapTransformFnData]) -> Iterable[Block]:
    buffer = BlockOutputBuffer(self._output_block_size_option)

    # 对于 Row 类型，使用 buffer.add 逐行添加
    if self._input_type == MapTransformFnDataType.Row:
        append = buffer.add

    # 遍历所有结果（每一行）
    for result in results:
        append(result)           # 1. 把一行数据添加到 buffer
        while buffer.has_next(): # 2. 检查 buffer 是否达到阈值
            yield buffer.next()  # 3. 如果达到阈值，输出一个 block

    # 处理完所有行后，输出剩余数据
    buffer.finalize()
    while buffer.has_next():
        yield buffer.next()
```

### 关键点

- `_shape_blocks` **只调用一次**，但它是一个生成器
- 内部 **每行都会调用** `buffer.add(row)`
- **只有达到阈值时** 才会 `yield` 一个 block 到下游

### `has_next()` 判断逻辑

| 配置 | 每行检查 | 输出条件 |
|------|---------|---------|
| 默认 (`target_max_block_size=128MB`) | ✅ 是 | buffer 字节数 > 128MB |
| 设置 `target_num_rows_per_block=100` | ✅ 是 | buffer 行数 > 100 |
| 两者都设置 | ✅ 是 | **任一条件**满足就输出 |
| 都不设置 (`OutputBlockSizeOption=None`) | ❌ 否 | 等所有行处理完才输出 |

---

## 6. OutputBlockSizeOption 设置位置

### 主要设置点

#### 1. Map/MapBatches/Filter 算子

位置：`python/ray/data/_internal/planner/plan_udf_map_op.py:287-289`

```python
output_block_size_option = OutputBlockSizeOption.of(
    target_max_block_size=data_context.target_max_block_size,  # 默认 128MB
)
```

#### 2. Read 算子

位置：`python/ray/data/_internal/planner/plan_read_op.py:116`

```python
output_block_size_option=OutputBlockSizeOption.of(
    target_max_block_size=data_context.target_max_block_size,
)
```

#### 3. Streaming Repartition 算子

位置：`python/ray/data/_internal/planner/plan_udf_map_op.py:192-194`

```python
output_block_size_option=OutputBlockSizeOption.of(
    target_num_rows_per_block=op.target_num_rows_per_block,
)
```

### 数据流

```
DataContext.target_max_block_size (默认 128MB)
        ↓
plan_udf_map_op.py
        ↓
OutputBlockSizeOption.of(target_max_block_size=128MB)
        ↓
RowMapTransformFn(output_block_size_option=...)
        ↓
_shape_blocks() 使用 BlockOutputBuffer(output_block_size_option)
        ↓
buffer.has_next() 检查是否达到阈值
```

### 默认值

```python
# context.py:55
DEFAULT_TARGET_MAX_BLOCK_SIZE = 128 * 1024 * 1024  # 128MB
```

---

## 7. 反压机制分析

### 反压日志示例

```
Map(VideoPreprocessMapper): 185/1
  Tasks: 30 [backpressured:tasks(ResourceBudget)]
  Queued blocks: 4 (176.4MiB)
  Resources: 9.0 CPU, 13.1GiB object store
    (in=10.1GiB, out=3.0GiB)
    alloc=(cpu=12.0, gpu=0.0, obj_store=13.3GiB)
    budget=(cpu=3.0, gpu=0.0, obj_store=169.1MiB, out=215.9MiB)
```

### 反压判断逻辑

位置：`python/ray/data/_internal/execution/resource_manager.py:860-874`

```python
def can_submit_new_task(self, op: PhysicalOperator) -> bool:
    budget = self.get_budget(op)
    return (
        op.incremental_resource_usage().satisfies_limit(budget)
        and
        budget.object_store_memory >= (op.metrics.obj_store_mem_max_pending_output_per_task or 0)
    )
```

### 反压原因

| 资源 | 已分配 (alloc) | 剩余预算 (budget) | 状态 |
|------|--------------|-----------------|------|
| CPU | 12.0 | 3.0 | ⚠️ 预算紧张 |
| Object Store | 13.3GiB | 169.1MiB | ❌ **预算严重不足** |
| Output | - | 215.9MiB | ⚠️ 输出预算有限 |

### 根本原因

**Object Store 内存预算耗尽**

- 输入数据积压: `in=10.1GiB`
- 输出数据积压: `out=3.0GiB`
- 剩余预算: 只有 `169.1MiB`

### 解决方法

#### 1. 增加 Object Store 内存

```bash
ray start --head --object-store-memory=20000000000  # 20GB
```

#### 2. 减少并发任务数

```python
ds = ds.map(fn, compute=ray.data.TaskPoolStrategy(size=10))
```

#### 3. 减小 block 大小

```python
from ray.data import DataContext
ctx = DataContext.get_current()
ctx.target_max_block_size = 64 * 1024 * 1024  # 64MB
```

#### 4. 减小上游 block 数量/大小

```python
ds = ds.repartition(target_num_rows_per_block=50)
```

---

## 8. 常见日志问题

### 8.1 API Server 响应慢

日志示例：
```
Waiting for the response from the API server address http://10.137.75.19:8265/api/v0/tasks
```

#### 原因

- Task 数量太多，API server 查询慢
- Dashboard 负载高
- GCS 压力大
- 网络延迟

#### 解决方法

```bash
# 禁用 Dashboard
ray start --head --include-dashboard=false

# 或增加超时时间
export RAY_STATE_SERVER_REQUEST_TIMEOUT_S=60
```

### 8.2 DEBUG 日志周期

| 日志类型 | 周期 |
|---------|------|
| 执行进度日志 | 每 300 秒（`DEBUG_LOG_INTERVAL_SECONDS`） |
| 算子指标日志 | 每 300 秒 |
| 进度条刷新 | 每 50 个 dispatch 或每次调度循环 |

---

## 附录：关键文件位置

| 文件 | 功能 |
|------|------|
| `streaming_executor.py` | 流式执行器主逻辑 |
| `map_operator.py` | Map 算子实现 |
| `map_transformer.py` | 数据转换逻辑（`_shape_blocks`） |
| `output_buffer.py` | Block 输出缓冲区 |
| `resource_manager.py` | 资源管理和反压判断 |
| `backpressure_policy/` | 反压策略实现 |
| `plan_udf_map_op.py` | Map 算子物理计划生成 |
| `context.py` | DataContext 配置 |
