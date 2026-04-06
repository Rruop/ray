# Checkpoint Recovery Empty Block Bug: Blocks Input=20 但 Rows Input=0

## 问题描述

在本地测试中，作业 `http://127.0.0.1:8265/#/jobs/02000000` 的 TaskPoolOperator 算子显示：
- **Blocks Input = 20**
- **Rows Input = 0**

实际场景：作业经过 checkpoint 恢复后，数据全被过滤，TaskPoolOperator 是自定义的 `slow_fn`（`map_batches`），没有实际数据输入。

## 测试代码

```python
# python/ray/data/tests/test/test_dataset_kconf_configuration_controller.py

def slow_fn(batch):
    time.sleep(30)  # 模拟慢操作
    return batch

ds = ray.data.range(400, override_num_blocks=20)

ds = ds.map_batches(
    slow_fn,
    batch_size=5,
    custom_id="task_pool_op_1",
    custom_name="TaskPoolOperator",
    compute=ray.data.TaskPoolStrategy(size=2),
)
```

## DAG 结构

```
InputDataBuffer → Read(TaskPoolMapOperator) → TaskPoolOperator(map_batches slow_fn)
```

Read operator 内部包含两个 transform：
1. `BlockMapTransformFn(do_read)` — 读取数据
2. `BlockMapTransformFn(filter_checkpointed_rows_for_blocks)` — 过滤已 checkpoint 的行

---

## 完整数据流分析

### 1. Dashboard "Blocks Input" 和 "Rows Input" 的数据来源

前端组件 `DataOverviewTable.tsx` 中定义了列：

```tsx
// python/ray/dashboard/client/src/components/DataOverviewTable.tsx:43-48
{
    label: "Rows Input",
    helpInfo: <Typography>Rows inputted by input operator.</Typography>,
},
{
    label: "Blocks Input",
    helpInfo: <Typography>Total blocks inputted by operator.</Typography>,
},
```

前端渲染直接使用 `data.input_rows` 和 `data.input_blocks`。

后端 API 数据来自 `streaming_executor.py`：

```python
# python/ray/data/_internal/execution/streaming_executor.py:781-782
"input_rows": op.metrics.num_row_inputs_received,
"input_blocks": op.metrics.num_block_inputs_received,
```

### 2. OpRuntimeMetrics 中指标的记录

```python
# python/ray/data/_internal/execution/interfaces/op_runtime_metrics.py:867-871
def on_input_received(self, input: RefBundle):
    """Callback when the operator receives a new input."""
    self.num_inputs_received += 1
    self.num_block_inputs_received += len(input.blocks)    # block 数量
    self.num_row_inputs_received += input.num_rows() or 0  # 总行数
    self.bytes_inputs_received += input.size_bytes()
```

关键区别：
- `num_block_inputs_received` 统计的是 **block 的数量**（`len(input.blocks)`）
- `num_row_inputs_received` 统计的是 **block 的行数总和**（`input.num_rows()`）

### 3. RefBundle.num_rows() 的计算

```python
# python/ray/data/_internal/execution/interfaces/ref_bundle.py:136-154
def num_rows(self) -> Optional[int]:
    total = 0
    for metadata, block_slice in zip(self.metadata, self.slices):
        if block_slice is None:
            if metadata.num_rows is None:
                return None
            total += metadata.num_rows
        else:
            total += block_slice.num_rows
    return total
```

遍历 bundle 中每个 block 的 metadata，累加 `metadata.num_rows`。如果某个 block 的 metadata 中 `num_rows=0`，则 `total += 0`。

### 4. on_input_received 的调用时机

```python
# python/ray/data/_internal/execution/interfaces/physical_operator.py:754-755
def add_input(self, refs: RefBundle, input_index: int):
    self._metrics.on_input_received(refs)        # 统计输入
    self._add_input_inner(refs, input_index)     # 实际处理
```

`refs` 是上游 operator 传过来的 RefBundle。其 metadata 来自上游 operator 的输出。

### 5. 上游 Read Operator 的输出如何构建

Read operator 是 `TaskPoolMapOperator`，其 `_map_task` 产出的 block 和 metadata 通过 streaming generator 传递：

```python
# python/ray/data/_internal/execution/operators/map_operator.py:775-820
for block in map_transformer.apply_transform(blocks_iter, ctx):
    block_meta = BlockAccessor.for_block(block).get_metadata()
    block_schema = BlockAccessor.for_block(block).schema()
    ...
    gen_stats: StreamingGeneratorStats = yield block
    ...
    bm = BlockMetadataWithSchema.from_metadata(
        replace(block_meta, exec_stats=exec_stats, ...),
        schema=block_schema if not yielded_schema else None,
    )
    yield pickle.dumps(bm)
```

在 driver 端，`DataOpTask.on_data_ready()` 接收：

```python
# python/ray/data/_internal/execution/interfaces/physical_operator.py:272-282
meta_with_schema: "BlockMetadataWithSchema" = pickle.loads(meta_with_schema_bytes)
meta = meta_with_schema.metadata
self._output_ready_callback(
    RefBundle(
        [(self._pending_block_ref, meta)],
        owns_blocks=True,
        schema=meta_with_schema.schema,
    ),
)
```

### 6. Checkpoint Filter 的过滤逻辑

```python
# python/ray/data/checkpoint/util.py:61-106
def filter_checkpointed_rows_for_blocks(
    blocks: Iterable[Block],
    task_context: TaskContext,
    checkpoint_config: CheckpointConfig,
) -> Iterable[Block]:
    ...
    for block in blocks:
        filtered_block = filter_fn(block)
        ba = BlockAccessor.for_block(filtered_block)
        if ba.num_rows() > 0:
            yield filtered_block
```

**此函数正确地跳过了空 block**（`if ba.num_rows() > 0`），当所有行被过滤后不 yield 任何结果。

### 7. Checkpoint Filter 的具体过滤实现

`BatchBasedCheckpointFilter` 提供了多种过滤方式：

#### 7.1 普通二分搜索过滤 (`filter_rows_for_block`)

```python
# python/ray/data/checkpoint/checkpoint_filter.py:540-607
def filter_rows_for_block(self, block: Block, checkpointed_ids: Block) -> Block:
    if len(checkpointed_ids) == 0 or len(block) == 0:
        return block
    ...
    # 使用二分搜索 + 多线程过滤
    with concurrent.futures.ThreadPoolExecutor(...) as executor:
        masks = list(executor.map(filter_with_ckpt_chunk, ckpt_chunks))
    final_mask = numpy.logical_and.reduce(masks)
    mask_array = pyarrow.array(final_mask)
    filtered_block = block.filter(mask_array)
    return filtered_block
```

**注意**：当 `checkpointed_ids` 为空或 `block` 为空时，直接返回原 block（不做过滤）。这在恢复场景下是安全的——如果 checkpoint 数据为空，说明没有已处理的数据。

#### 7.2 Roaring Bitmap 过滤 (`filter_rows_for_block_with_raoring_bitmap`)

```python
# python/ray/data/checkpoint/checkpoint_filter.py:610-642
def filter_rows_for_block_with_raoring_bitmap(self, block, checkpointed_ids) -> Block:
    if len(checkpointed_ids) == 0 or len(block) == 0:
        return block
    bitmap = _get_or_build_roaring_bitmap(...)
    block_ids = block[self.id_column].to_numpy()
    mask = np.fromiter((x in bitmap for x in block_ids), dtype=bool, count=len(block_ids))
    keep_mask = ~mask
    return block.filter(pa.array(keep_mask))
```

#### 7.3 Bloom Filter 过滤 (`filter_rows_for_block_with_bloom_filter`)

```python
# python/ray/data/checkpoint/checkpoint_filter.py:646-675
def filter_rows_for_block_with_bloom_filter(self, block, bloom) -> Block:
    if len(block) == 0 or len(bloom) == 0:
        return block
    block_uint64_ids = _hash_id_column_to_uint64(block, self.id_column)
    in_checkpoint = bloom.contains_many(block_uint64_ids)
    keep_mask = ~in_checkpoint
    return block.filter(pa.array(keep_mask))
```

#### 7.4 Redis 过滤 (`filter_block_by_redis_ckpt`)

```python
# python/ray/data/checkpoint/checkpoint_filter.py:678-714
def filter_block_by_redis_ckpt(self, block: Block) -> Block:
    redis_client = redis.Redis(...)
    is_exists = _is_redis_checkpoint_key_exists(...)
    if len(block) == 0 or is_exists == '0':
        return block
    ...
    keep_mask = ~numpy.array(exists_flags, dtype=bool)
    return block.filter(pyarrow.array(keep_mask))
```

### 8. _BlockShapingIterator 的行为

当 checkpoint filter 不 yield 任何结果时，`_BlockShapingIterator` 的行为：

```python
# python/ray/data/_internal/execution/operators/map_transformer.py:442-458
class _BlockShapingIterator(Iterator[Block]):
    def __next__(self) -> Block:
        while True:
            if self._buffer.has_next():
                return self._buffer.next()
            elif self._finalized:
                raise StopIteration
            try:
                result = next(self._results_iter)  # filter 不 yield → StopIteration
                self._append_buffer(result)
            except StopIteration:
                self._buffer.finalize()
                self._finalized = True
```

当 filter 不 yield 任何结果时：
1. `next(self._results_iter)` 抛出 `StopIteration`
2. `self._buffer.finalize()` 被调用
3. `self._finalized = True`
4. 循环回到 `has_next()` 检查

### 9. BlockOutputBuffer.has_next() 的 Bug

**这是根因所在！**

```python
# python/ray/data/_internal/output_buffer.py:144-160
def has_next(self) -> bool:
    """Returns true when a complete output block is produced."""

    # TODO remove emitting empty blocks
    if self._finalized:
        return not self._has_yielded_blocks or self._buffer.num_rows() > 0
        #     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
        #     当从未 yield 过 block 时，这个值为 True！
        #     所以即使 buffer 完全为空，也会返回 True！
    elif self._output_block_size_option is None:
        return False
    elif self._output_block_size_option.disable_block_shaping:
        return self._buffer.num_rows() > 0

    return self._exceeded_buffer_row_limit() or self._exceeded_buffer_size_limit()
```

当所有行被 checkpoint filter 过滤后：
- `self._finalized = True`
- `self._has_yielded_blocks = False`（从未 yield 过 block）
- `self._buffer.num_rows() = 0`（buffer 中没有任何数据）

计算：`not False or 0 > 0` = `True or False` = **`True`**

**所以 `has_next()` 返回 `True`，即使 buffer 完全为空！**

代码中甚至有一行 `TODO` 注释：`# TODO remove emitting empty blocks`，说明开发者已经意识到了这个问题但尚未修复。

### 10. BlockOutputBuffer.next() 产出空 block

```python
# python/ray/data/_internal/output_buffer.py:181-212
def next(self) -> Block:
    assert self.has_next()
    block = self._buffer.build()  # 构建空 block
    ...
    return block
```

`DelegatingBlockBuilder.build()`：

```python
# python/ray/data/_internal/delegating_block_builder.py:61-68
def build(self) -> Block:
    if self._builder is None:
        if self._empty_block is not None:
            self._builder = BlockAccessor.for_block(self._empty_block).builder()
            self._builder.add_block(self._empty_block)
        else:
            self._builder = ArrowBlockBuilder()  # 创建空的 Arrow builder
    return self._builder.build()
```

`ArrowBlockBuilder._empty_table()`：

```python
# python/ray/data/_internal/arrow_block.py:186-187
@staticmethod
def _empty_table() -> "pyarrow.Table":
    return pyarrow_table_from_pydict({})  # 0 列 0 行的空 Arrow table
```

`TableBlockBuilder.build()`：

```python
# python/ray/data/_internal/table_block.py:130-141
def build(self) -> Block:
    if self._columns:
        tables = [self._table_from_pydict(self._columns)]
    else:
        tables = []
    tables.extend(self._tables)
    if len(tables) == 0:
        return self._empty_table()  # 返回空 table！
    else:
        return self._combine_tables(tables)
```

### 11. 空 block 的 metadata

`ArrowBlockAccessor.num_rows()`：

```python
# python/ray/data/_internal/arrow_block.py:324-327
def num_rows(self) -> int:
    # Arrow may represent an empty table via an N > 0 row, 0-column table,
    # e.g. when slicing an empty table, so we return 0 if num_columns == 0 else 0.
    return self._table.num_rows if self._table.num_columns > 0 else 0
```

空 Arrow table（0 列 0 行）的 `num_rows()` 返回 **0**。

`BlockAccessor.get_metadata()`：

```python
# python/ray/data/block.py:519-532
def get_metadata(self, input_files=None, block_exec_stats=None, task_exec_stats=None) -> BlockMetadata:
    return BlockMetadata(
        num_rows=self.num_rows(),     # 0
        size_bytes=self.size_bytes(),  # 0
        input_files=tuple(input_files) if input_files is not None else None,
        exec_stats=block_exec_stats,
        task_exec_stats=task_exec_stats,
    )
```

### 12. 完整的因果链

```
1. Checkpoint 恢复后，所有数据已被 checkpoint
   ↓
2. filter_checkpointed_rows_for_blocks 过滤掉所有行
   （正确地不 yield 任何 block）
   ↓
3. _BlockShapingIterator 收到 StopIteration
   调用 BlockOutputBuffer.finalize()
   ↓
4. BlockOutputBuffer.has_next() 返回 True
   因为 `not self._has_yielded_blocks` = True
   ↓
5. BlockOutputBuffer.next() 调用 DelegatingBlockBuilder.build()
   返回一个 0 列 0 行的空 Arrow table
   ↓
6. _map_task yield 这个空 block
   BlockAccessor.for_block(block).get_metadata() → num_rows=0, size_bytes=0
   ↓
7. DataOpTask.on_data_ready() 构建空 RefBundle
   RefBundle([(empty_block_ref, BlockMetadata(num_rows=0))])
   ↓
8. 空 RefBundle 传递给下游 TaskPoolOperator
   ↓
9. TaskPoolOperator.on_input_received() 记录：
   num_block_inputs_received += 1  (block 数+1)
   num_row_inputs_received += 0    (行数+0)
   ↓
10. 20 个 read task 各自产出 1 个空 block
   → Dashboard 显示：Blocks Input=20, Rows Input=0
```

---

## 涉及的关键文件

| 文件 | 作用 |
|------|------|
| `python/ray/data/_internal/output_buffer.py` | **Bug 所在** — `BlockOutputBuffer.has_next()` 在 finalized 后即使 buffer 为空也返回 True |
| `python/ray/data/_internal/execution/operators/map_transformer.py` | `_BlockShapingIterator` 调用 `BlockOutputBuffer` |
| `python/ray/data/_internal/execution/operators/map_operator.py` | `_map_task` 产出 block 和 metadata |
| `python/ray/data/_internal/execution/interfaces/physical_operator.py` | `DataOpTask.on_data_ready()` 构建输出 RefBundle；`add_input()` 调用 `on_input_received` |
| `python/ray/data/_internal/execution/interfaces/op_runtime_metrics.py` | `on_input_received()` 记录 `num_block_inputs_received` 和 `num_row_inputs_received` |
| `python/ray/data/_internal/execution/interfaces/ref_bundle.py` | `RefBundle.num_rows()` 从 metadata 计算 |
| `python/ray/data/_internal/delegating_block_builder.py` | `build()` 对空 buffer 返回空 Arrow table |
| `python/ray/data/_internal/arrow_block.py` | `ArrowBlockBuilder._empty_table()` 返回空 table；`ArrowBlockAccessor.num_rows()` 对空 table 返回 0 |
| `python/ray/data/_internal/table_block.py` | `TableBlockBuilder.build()` 对空 tables 返回 `_empty_table()` |
| `python/ray/data/block.py` | `BlockMetadata`、`BlockAccessor.get_metadata()` |
| `python/ray/data/checkpoint/util.py` | `filter_checkpointed_rows_for_blocks()` — 过滤已 checkpoint 的行 |
| `python/ray/data/checkpoint/checkpoint_filter.py` | `BatchBasedCheckpointFilter` — 各种过滤实现 |
| `python/ray/data/_internal/planner/checkpoint/plan_read_op.py` | `plan_read_op_with_checkpoint_filter()` — 将 checkpoint filter 添加到 Read operator |
| `python/ray/data/_internal/execution/streaming_executor.py` | 将 operator metrics 传给 dashboard API |
| `python/ray/dashboard/client/src/components/DataOverviewTable.tsx` | 前端展示 "Blocks Input" / "Rows Input" |
| `python/ray/data/_internal/stats.py` | `_StatsActor` 存储 dataset 和 operator 的状态信息 |

---

## 修复方案

### 方案 A（推荐）：修复 BlockOutputBuffer.has_next()

**文件**：`python/ray/data/_internal/output_buffer.py`

```python
def has_next(self) -> bool:
    """Returns true when a complete output block is produced."""

    if self._finalized:
        # 修复：如果从未 yield 过 block 且 buffer 为空，不产空 block
        if not self._has_yielded_blocks and self._buffer.num_rows() == 0:
            return False
        return not self._has_yielded_blocks or self._buffer.num_rows() > 0
    elif self._output_block_size_option is None:
        return False
    elif self._output_block_size_option.disable_block_shaping:
        return self._buffer.num_rows() > 0

    return self._exceeded_buffer_row_limit() or self._exceeded_buffer_size_limit()
```

**优点**：从根本上解决问题，空 block 不会被产出。
**风险**：需要确认 "每个 task 至少产出 1 个 output block" 的语义在其他场景下是否被依赖。代码中的 TODO 注释 `# TODO remove emitting empty blocks` 说明开发者已经计划移除这个行为。

### 方案 B（防御性）：在 _map_task 中跳过空 block

**文件**：`python/ray/data/_internal/execution/operators/map_operator.py`

```python
for block in map_transformer.apply_transform(blocks_iter, ctx):
    accessor = BlockAccessor.for_block(block)
    if accessor.num_rows() == 0:
        continue  # 跳过空 block
    block_meta = accessor.get_metadata()
    block_schema = accessor.schema()
    ...
```

**优点**：防御性修复，不影响 `BlockOutputBuffer` 的语义。
**缺点**：不是根本修复，如果其他地方也有产出空 block 的路径仍会出问题。

### 方案 C（最安全）：同时采用方案 A 和方案 B

在 `BlockOutputBuffer` 层面修复根本问题，同时在 `_map_task` 中添加防御性检查，确保空 block 不会传递到下游。

---

## 补充：Read Operator 输入数据的 Metadata 分析

### InputDataBuffer 中的 ReadTask Metadata

`plan_read_op` 中创建 ReadTask 的 RefBundle 时：

```python
# python/ray/data/_internal/planner/plan_read_op.py:30-53
def _derive_metadata(read_task: ReadTask, read_task_ref: ObjectRef) -> BlockMetadata:
    locations = get_local_object_locations([read_task_ref])
    task_size = locations[read_task_ref]["object_size"]

    return BlockMetadata(
        num_rows=1,          # 注意：这里是 1，不是实际行数
        size_bytes=task_size,  # 是 ReadTask 的序列化大小，不是数据大小
        exec_stats=None,
        input_files=None,
    )
```

这个 metadata 是 ReadTask 对象的元数据（不是最终数据 block 的），`num_rows=1` 和 `size_bytes=task_size` 只是占位值。最终的 block metadata 来自 `_map_task` 中 `BlockAccessor.for_block(block).get_metadata()`。

### RangeDatasource 的 ReadTask Metadata

```python
# python/ray/data/_internal/datasource/range_datasource.py:99-118
while i < n:
    count = min(block_size, n - i)
    meta = BlockMetadata(
        num_rows=count,               # 正确的行数
        size_bytes=8 * count * element_size,  # 正确的字节数
        input_files=None,
        exec_stats=None,
    )
    read_tasks.append(ReadTask(..., meta, ...))
```

RangeDatasource 的 ReadTask metadata 有正确的 `num_rows`，但这个 metadata 只用于 Read operator 的输入统计（在 `InputDataBuffer` 中被 `_derive_metadata` 覆盖），不会传递到下游。

---

## 补充：Dashboard 数据流完整路径

```
Operator 内部
    ↓
OpRuntimeMetrics.on_input_received()
    num_block_inputs_received += len(input.blocks)
    num_row_inputs_received += input.num_rows() or 0
    ↓
StreamingExecutor._update_stats_metrics()
    op_info = {
        "input_rows": op.metrics.num_row_inputs_received,
        "input_blocks": op.metrics.num_block_inputs_received,
        ...
    }
    ↓
_StatsActor.update_dataset()
    self.datasets[dataset_tag].update(state)
    更新 Prometheus 指标
    ↓
Dashboard 前端 DataOverviewTable.tsx
    <TableCell>{data.input_rows}</TableCell>
    <TableCell>{data.input_blocks}</TableCell>
```

---

## 补充：BlockOutputBuffer.has_next() 逻辑的原始意图分析

`has_next()` 中 `not self._has_yielded_blocks` 条件的原始意图：

> 保证每个 task 至少产出 1 个 output block，即使 task 处理完后 buffer 为空。

这在以下场景是有意义的：
- 一个 map task 收到输入但 UDF 没有产出任何输出行（例如 `flat_map` 过滤掉所有行）
- 系统仍然需要产出一个 block 来表示 "这个 task 已完成处理"

但在 checkpoint 恢复场景下，这个语义导致了问题：空 block 被 dashboard 统计为 "有 block 输入但无行数据"，造成误导。

**合理的语义应该是**：如果一个 task 没有产出任何有效数据，就不应该产出空 block。空 block 在下游只会增加不必要的开销（占用 object store 空间、增加调度开销），并且误导 dashboard 的指标展示。

---

## 补充：DelegatingBlockBuilder 对空 block 的处理

```python
# python/ray/data/_internal/delegating_block_builder.py:38-54
def add_block(self, block: Block):
    accessor = BlockAccessor.for_block(block)
    if accessor.num_rows() == 0:
        # Don't infer types of empty lists. Store the block and use it if no
        # other data is added. https://github.com/ray-project/ray/issues/20290
        self._empty_block = block
        return
    if self._builder is None:
        self._builder = accessor.builder()
    else:
        ...
    self._builder.add_block(accessor.to_block())
```

`DelegatingBlockBuilder` 在 `add_block` 时**会跳过空 block**（`if accessor.num_rows() == 0: return`），但 `build()` 在没有任何数据时仍然会构建一个空 block：

```python
def build(self) -> Block:
    if self._builder is None:
        if self._empty_block is not None:
            self._builder = BlockAccessor.for_block(self._empty_block).builder()
            self._builder.add_block(self._empty_block)
        else:
            self._builder = ArrowBlockBuilder()  # 无任何数据 → 空 builder
    return self._builder.build()
```

这说明 Ray Data 的设计在多处都假设 "至少有一个 output block"，这是一个需要修正的假设。
