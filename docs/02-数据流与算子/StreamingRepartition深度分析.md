# Ray Data StreamingRepartition 深度分析

## 概述

`StreamingRepartition` 是 Ray Data 中基于流式（streaming）方式实现的 repartition 操作。与传统的 `Repartition`（基于 AllToAll shuffle）不同，`StreamingRepartition` 继承自 `AbstractMap`，是一个流式 1:1 map 算子，不需要物化全部输入数据。

其核心目标是：**将任意大小的输入 block 流式地重组为目标行数（`target_num_rows_per_block`）的输出 block**。

---

## 关键文件

| 文件 | 职责 |
|------|------|
| `python/ray/data/dataset.py:1677-1799` | 用户入口 `repartition()` API，路由到 `StreamingRepartition` 或 `Repartition` |
| `python/ray/data/_internal/logical/operators/map_operator.py:448-465` | `StreamingRepartition` 逻辑算子定义 |
| `python/ray/data/_internal/streaming_repartition.py` | `StreamingRepartitionRefBundler`：Driver 端攒批 + 切割逻辑 |
| `python/ray/data/_internal/planner/plan_udf_map_op.py:182-210` | 物理算子规划 `plan_streaming_repartition_op` |
| `python/ray/data/_internal/execution/operators/map_operator.py:514-534` | `MapOperator._add_input_inner`：Task 提交调度 |
| `python/ray/data/_internal/execution/operators/map_transformer.py:62-93` | `_shape_blocks`：Task 内部输出 block 切分 |
| `python/ray/data/_internal/output_buffer.py` | `BlockOutputBuffer`：Task 内部输出 block 切分的核心缓冲区 |
| `python/ray/data/_internal/execution/interfaces/ref_bundle.py` | `RefBundle`、`BlockSlice`：逻辑切分和 merge 的数据结构 |
| `python/ray/data/_internal/planner/exchange/split_repartition_task_scheduler.py` | 非 streaming 的 SplitRepartition（对比参考） |

---

## 用户入口：`dataset.repartition()`

```python
# python/ray/data/dataset.py:1677-1799
def repartition(
    self,
    num_blocks: Optional[int] = None,
    target_num_rows_per_block: Optional[int] = None,
    *,
    shuffle: bool = False,
    keys: Optional[List[str]] = None,
    sort: bool = False,
) -> "Dataset":
```

路由逻辑（`dataset.py:1784-1796`）：

```python
if target_num_rows_per_block is not None:
    # 流式 repartition，不需要物化全部数据
    op = StreamingRepartition(
        self._logical_plan.dag,
        target_num_rows_per_block=target_num_rows_per_block,
    )
else:
    # 传统 repartition，AllToAll 操作
    op = Repartition(
        self._logical_plan.dag,
        num_outputs=num_blocks,
        shuffle=shuffle,
        keys=keys,
        sort=sort,
    )
```

三种 repartition 模式：

| 模式 | 参数 | 特点 |
|------|------|------|
| Shuffle Repartition | `num_blocks` + `shuffle=True` | 全量 shuffle，精确 `num_blocks` 个输出 block |
| Split Repartition | `num_blocks` + `shuffle=False` | 按索引切分+合并，精确 `num_blocks` 个输出 block |
| **Streaming Repartition** | `target_num_rows_per_block` | 流式切分，每个输出 block 约 `target` 行 |

---

## 逻辑算子定义

```python
# python/ray/data/_internal/logical/operators/map_operator.py:448-465
class StreamingRepartition(AbstractMap):
    """Logical operator for streaming repartition operation.

    Args:
        target_num_rows_per_block: The target number of rows per block granularity
            for streaming repartition.
    """

    def __init__(
        self,
        input_op: LogicalOperator,
        target_num_rows_per_block: int,
    ):
        super().__init__(
            f"StreamingRepartition[num_rows_per_block={target_num_rows_per_block}]",
            input_op,
            can_modify_num_rows=False,
        )
        self.target_num_rows_per_block = target_num_rows_per_block
```

关键点：
- 继承 `AbstractMap` 而非 `AbstractAllToAll`，是流式 1:1 map 算子
- 只有一个核心参数：`target_num_rows_per_block`
- `can_modify_num_rows=False`：不改变总行数

---

## 两层切分架构

StreamingRepartition 采用 **两层切分** 设计：

```
┌─────────────────────────────────────────────────────────────────┐
│                    Driver 端（第一层）                           │
│  StreamingRepartitionRefBundler                                 │
│  职责：攒批 + 逻辑切割，决定何时提交 task                        │
│  操作：RefBundle.merge_ref_bundles() + RefBundle.slice()        │
│  特点：零拷贝，通过 BlockSlice 偏移量复用 ObjectRef              │
├─────────────────────────────────────────────────────────────────┤
│                    Task 端（第二层）                             │
│  BlockOutputBuffer + _shape_blocks                              │
│  职责：将 N×target 行的输入切成 N 个 target 行的输出 block       │
│  操作：DelegatingBlockBuilder.build() + BlockAccessor.slice()   │
│  特点：物理切分，产生新 block 对象                               │
└─────────────────────────────────────────────────────────────────┘
```

---

## 第一层：Driver 端攒批与逻辑切割

### 核心类：`StreamingRepartitionRefBundler`

```python
# python/ray/data/_internal/streaming_repartition.py
class StreamingRepartitionRefBundler(BaseRefBundler):
    """Incrementally builds task inputs to produce multiples of target-sized outputs."""

    def __init__(self, target_num_rows_per_block: int):
        assert target_num_rows_per_block > 0
        self._target_num_rows = target_num_rows_per_block
        self._pending_bundles: Deque[RefBundle] = deque()    # 等待中的 bundle
        self._ready_bundles: Deque[RefBundle] = deque()      # 已攒够的 bundle
        self._consumed_input_bundles: List[RefBundle] = []   # 已消费的输入 bundle
        self._total_pending_rows = 0                         # 当前累积行数
```

### 各方法职责

| 方法 | 做什么 | 攒批？ | 切割？ |
|------|--------|:---:|:---:|
| `add_bundle(ref_bundle)` | 将新 bundle 放入 pending 队列，然后调用 `_try_build_ready_bundle()` | ✅ | ✅ |
| `_try_build_ready_bundle()` | 判断行数够不够，够了就 merge + slice 构建 ready bundle | ✅ | ✅ |
| `has_bundle()` | 只检查 `_ready_bundles` 队列是否非空 | ❌ | ❌ |
| `get_next_bundle()` | 只从 `_ready_bundles` 队列 popleft 取出一个 | ❌ | ❌ |
| `done_adding_bundles()` | 上游结束时 flush 剩余 pending bundles | ✅ | ❌ |

> **重要**：攒批和切割都发生在 `add_bundle()` 内部（通过 `_try_build_ready_bundle()`），`get_next_bundle()` 只是把已经攒好切好的 ready bundle 取出来交给调度器提交 task。

### `add_bundle()` 方法

```python
# streaming_repartition.py:75-79
def add_bundle(self, ref_bundle: RefBundle):
    self._total_pending_rows += ref_bundle.num_rows()
    self._pending_bundles.append(ref_bundle)
    self._try_build_ready_bundle()                       # ← 攒批+切割在这里触发
    self._consumed_input_bundles.append(ref_bundle)
```

### `_try_build_ready_bundle()` 方法（核心逻辑）

```python
# streaming_repartition.py:44-73
def _try_build_ready_bundle(self, flush_remaining: bool = False):
    if self._total_pending_rows >= self._target_num_rows:
        # 计算需要从最后一个 bundle 切多少行
        # 例如：total=350, target=100 → 350%100=50
        #       last_bundle=200行 → rows_needed = 200 - 50 = 150
        rows_needed_from_last_bundle = (
            self._pending_bundles[-1].num_rows()
            - self._total_pending_rows % self._target_num_rows
        )
        assert rows_needed_from_last_bundle >= 0

        pending_bundles = list(self._pending_bundles)
        remaining_bundle = None

        if (
            rows_needed_from_last_bundle > 0
            and rows_needed_from_last_bundle < pending_bundles[-1].num_rows()
        ):
            # 对最后一个 bundle 做逻辑切分
            last_bundle = pending_bundles.pop()
            sliced_bundle, remaining_bundle = last_bundle.slice(
                rows_needed_from_last_bundle
            )
            pending_bundles.append(sliced_bundle)

        # 将所有 pending bundles merge 成一个 ready bundle
        self._ready_bundles.append(RefBundle.merge_ref_bundles(pending_bundles))
        self._pending_bundles.clear()
        self._total_pending_rows = 0

        # 剩余部分留给下一轮
        if remaining_bundle and remaining_bundle.num_rows() > 0:
            self._pending_bundles.append(remaining_bundle)
            self._total_pending_rows += remaining_bundle.num_rows()

    # 上游结束时 flush 剩余
    if flush_remaining and len(self._pending_bundles) > 0:
        self._ready_bundles.append(
            RefBundle.merge_ref_bundles(self._pending_bundles)
        )
        self._pending_bundles.clear()
        self._total_pending_rows = 0
```

### Ready Bundle 包含的行数

`_try_build_ready_bundle` **只调用一次 merge**，所以一个 ready bundle 包含的行数是：

```
ready_bundle_rows = total_pending_rows - (total_pending_rows % target_num_rows)
```

即 `N × target_num_rows`（N ≥ 1）。例如：
- 累积了 350 行，target=100 → ready bundle = 300 行，remainder = 50 行
- 累积了 100 行，target=100 → ready bundle = 100 行（`100%100==0`，不做 slice，整体 merge）
- 一个大 block 500 行到达 → ready bundle = 500 行（`500%100==0`）

### Task 数量公式

```
num_tasks ≈ ceil(total_input_rows / target_num_rows_per_block)
```

---

## 跨 Block 组合、复用和切分

### `RefBundle.slice()` — 逻辑切分（零拷贝）

```python
# python/ray/data/_internal/execution/interfaces/ref_bundle.py:223-303
def slice(self, needed_rows: int) -> Tuple["RefBundle", "RefBundle"]:
    """Slice a RefBundle into two bundles at the given row boundary."""
    block_slices = []
    for metadata, block_slice in zip(self.metadata, self.slices):
        if block_slice is None:
            # None 代表完整 block，转为显式 BlockSlice
            block_slices.append(
                BlockSlice(start_offset=0, end_offset=metadata.num_rows)
            )
        else:
            block_slices.append(block_slice)

    consumed_blocks, consumed_slices = [], []
    remaining_blocks, remaining_slices = [], []
    rows_to_take = needed_rows

    for (block_ref, metadata), block_slice in zip(self.blocks, block_slices):
        block_rows = block_slice.num_rows
        if rows_to_take >= block_rows:
            # 整个 block 都归 consumed
            consumed_blocks.append((block_ref, metadata))
            consumed_slices.append(block_slice)
            rows_to_take -= block_rows
        else:
            if rows_to_take == 0:
                # 不需要更多行，剩余 block 归 remaining
                remaining_blocks.append((block_ref, metadata))
                remaining_slices.append(block_slice)
                continue
            # 在 block 内部切一刀：同一个 ObjectRef 被两个 slice 引用
            consume_slice = BlockSlice(
                start_offset=block_slice.start_offset,
                end_offset=block_slice.start_offset + rows_to_take,
            )
            consumed_blocks.append((block_ref, metadata))
            consumed_slices.append(consume_slice)

            leftover_rows = block_rows - rows_to_take
            if leftover_rows > 0:
                remainder_slice = BlockSlice(
                    start_offset=consume_slice.end_offset,
                    end_offset=block_slice.end_offset,
                )
                remaining_blocks.append((block_ref, metadata))
                remaining_slices.append(remainder_slice)
            rows_to_take = 0

    sliced_bundle = RefBundle(
        blocks=tuple(consumed_blocks), schema=self.schema,
        owns_blocks=False, slices=consumed_slices,
    )
    remaining_bundle = RefBundle(
        blocks=tuple(remaining_blocks), schema=self.schema,
        owns_blocks=False, slices=remaining_slices,
    )
    return sliced_bundle, remaining_bundle
```

**关键特性**：
- 通过 `BlockSlice(start_offset, end_offset)` 记录偏移量
- **零拷贝**：同一个 `block_ref`（ObjectRef）可以出现在两个不同的 RefBundle 中，各自持有不同的 slice 范围
- 不实际拷贝数据，只做逻辑层面的切分

### `RefBundle.merge_ref_bundles()` — 逻辑合并

```python
# ref_bundle.py:306-317
@classmethod
def merge_ref_bundles(cls, bundles: List["RefBundle"]) -> "RefBundle":
    assert bundles, "Cannot merge an empty list of RefBundles."
    merged_blocks = list(itertools.chain(*[bundle.blocks for bundle in bundles]))
    merged_slices = list(itertools.chain(*[bundle.slices for bundle in bundles]))
    return cls(
        blocks=tuple(merged_blocks),
        schema=bundles[0].schema,
        owns_blocks=bundles[0].owns_blocks,
        slices=merged_slices,
    )
```

**关键特性**：
- 不拷贝数据，只是将多个 bundle 的 blocks 列表和 slices 列表拼接
- 合并后的 bundle 仍然引用原始的 ObjectRef

### `BlockSlice` 数据结构

```python
# ref_bundle.py:15-26
@dataclass
class BlockSlice:
    """A slice of a block."""
    start_offset: int   # 起始行偏移（包含）
    end_offset: int      # 结束行偏移（不包含）

    @property
    def num_rows(self) -> int:
        return self.end_offset - self.start_offset
```

### 跨 Block 组合复用示例

假设上游产生 3 个 block：`B1(30行)`, `B2(50行)`, `B3(40行)`，`target=100`。

**Step 1 — 累积阶段**：

```
B1 到达 → pending=[B1], total=30
B2 到达 → pending=[B1, B2], total=80
B3 到达 → pending=[B1, B2, B3], total=120 ≥ 100 ✓
```

**Step 2 — 构建 ready bundle**：

```
rows_needed_from_last_bundle = B3.num_rows() - (120 % 100)
                             = 40 - 20 = 20

对 B3 做 slice(20):
  → B3_front: BlockSlice(start=0, end=20) — 20行
  → B3_remainder: BlockSlice(start=20, end=40) — 20行

merge [B1, B2, B3_front] → ready bundle (100行，跨 3 个原始 block)
```

**Step 3 — 剩余部分**：

```
B3_remainder(20行) 留在 pending，等下一批 block 继续组合
同一个 B3 的 ObjectRef 被两个 bundle 共享引用（不同 slice 范围）
```

---

## Driver 端元数据切分 vs Task 端物理切分

### 核心问题：Driver 端切了一个 block 的一部分，传给 Task 的是什么？

**整个 ObjectRef 传过去**，Task 端根据 `BlockSlice` 元数据做真正的切片。

#### Driver 端：只传元数据，不传切片

`task_pool_map_operator.py:137-142`：

```python
gen = self._map_task.options(**dynamic_ray_remote_args).remote(
    ...
    *bundle.block_refs,    # ① 整个 ObjectRef（完整 block 数据）
    slices=bundle.slices,  # ② BlockSlice(start, end) 元数据
    ...
)
```

即使一个 block 被切成了 `[0, 324)` 和 `[324, 400)` 两部分分给不同 task，**两个 task 收到的是同一个 ObjectRef**，只是各自的 `slices` 不同：

```
Task 1 收到: block_ref=X, slice=BlockSlice(0, 324)
Task 2 收到: block_ref=X, slice=BlockSlice(324, 400)
                                  ↑ 同一个 ObjectRef X
```

#### Task 端：根据元数据真正切片

```python
# python/ray/data/_internal/execution/interfaces/ref_bundle.py:445-464
def _iter_sliced_blocks(blocks, slices):
    blocks_list = list(blocks)
    for block, block_slice in zip(blocks_list, slices):
        if block_slice is None:
            yield block                    # 完整 block 直接用
        else:
            accessor = BlockAccessor.for_block(block)
            start = block_slice.start_offset
            end = block_slice.end_offset
            yield accessor.slice(start, end, copy=False)  # 真正切片，零拷贝视图
```

Ray 自动把 `ObjectRef` 解引用为实际 Block 数据（Arrow Table 等），然后 `accessor.slice(start, end, copy=False)` 取出 `[start, end)` 行的**视图**，不拷贝数据。

#### 完整流程图

```
Driver 端:
  block X (400行) 的 ObjectRef
     │
     ├─ RefBundle.slice(324) →
     │    bundle_A: block_ref=X, slice=BlockSlice(0, 324)      [前324行]
     │    bundle_B: block_ref=X, slice=BlockSlice(324, 400)   [后76行]
     │
     │  提交 Task 1: (*bundle_A.block_refs, slices=bundle_A.slices)
     │              = (X, BlockSlice(0, 324))
     │  提交 Task 2: (*bundle_B.block_refs, slices=bundle_B.slices)
     │              = (X, BlockSlice(324, 400))
     │                ↑ 注意：两个 task 收到同一个 ObjectRef X

远端 Task 1:
  ray.get(X) → 完整 Arrow Table (400行)
  accessor.slice(0, 324, copy=False) → 前324行的零拷贝视图

远端 Task 2:
  ray.get(X) → 完整 Arrow Table (400行)   ← 同一个对象
  accessor.slice(324, 400, copy=False) → 后76行的零拷贝视图
```

#### 两层切分的本质区别

| 维度 | Driver 端 | Task 端 |
|------|----------|---------|
| **操作对象** | `RefBundle`（引用 + 元数据） | `Block`（实际 Arrow Table） |
| **切分方式** | 生成 `BlockSlice(start, end)` 偏移量 | `accessor.slice(start, end)` 物理读取 |
| **是否拷贝数据** | 否（零拷贝，只记录偏移） | `copy=False`，零拷贝视图 |
| **同一个 ObjectRef** | 可被多个 bundle 共享引用 | 可被多个 task 读取不同行范围 |
| **何时读取数据** | 不读取 | `_iter_sliced_blocks` 时按偏移量读取 |

#### 参数传递表

| 参数 | 类型 | 传递方式 | 说明 |
|------|------|---------|------|
| `*bundle.block_refs` | `List[ObjectRef]` | 直接传递引用 | Ray 自动从 ObjectRef 解引用为实际 Block 数据 |
| `slices=bundle.slices` | `Tuple[BlockSlice]` | 直接传递 Python 对象 | 切片元数据，描述每个 block 中属于本 task 的行范围 |
| `_map_transformer_ref` | `ObjectRef` | `ray.put()` 序列化到 Object Store | 远端 Worker 自动 `ray.get()` 反序列化 |

---

## Task 提交调度

### `MapOperator._add_input_inner()` — 提交入口

```python
# python/ray/data/_internal/execution/operators/map_operator.py:514-534
def _add_input_inner(self, refs: RefBundle, input_index: int):
    assert input_index == 0, input_index

    # 1. 喂入 bundler（攒批+切割在这里发生）
    self._block_ref_bundler.add_bundle(refs)
    self._metrics.on_input_queued(refs)

    # 2. 检查 bundler 是否有 ready bundle
    if self._block_ref_bundler.has_bundle():
        # 3. 取出 ready bundle（只是 popleft，不做数据操作）
        (input_refs, bundled_input) = self._block_ref_bundler.get_next_bundle()
        for bundle in input_refs:
            self._metrics.on_input_dequeued(bundle)

        # 4. 提交 Ray remote task
        self._try_schedule_task(bundled_input, strict=True)
```

完整调用链：

```
上游 operator 输出 RefBundle
  → MapOperator._add_input_inner(refs)
    → _block_ref_bundler.add_bundle(refs)           ← 攒批 + 切割
      → _try_build_ready_bundle()
        → RefBundle.slice()                          ← 逻辑切分最后一个 bundle
        → RefBundle.merge_ref_bundles()              ← 合并成 ready bundle
    → _block_ref_bundler.has_bundle()                ← 只查询
    → _block_ref_bundler.get_next_bundle()           ← 只取出
    → _try_schedule_task(bundled_input, strict=True)  ← 提交 Ray remote task
```

---

## 物理算子规划

```python
# python/ray/data/_internal/planner/plan_udf_map_op.py:182-210
def plan_streaming_repartition_op(
    op: StreamingRepartition,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> MapOperator:
    assert len(physical_children) == 1
    input_physical_dag = physical_children[0]
    compute = get_compute(op.compute)

    # 恒等变换：不做任何数据转换
    # 输出切分通过 OutputBlockSizeOption 控制
    transform_fn = BlockMapTransformFn(
        lambda blocks, ctx: blocks,  # ← 恒等变换
        output_block_size_option=OutputBlockSizeOption.of(
            target_num_rows_per_block=op.target_num_rows_per_block,
            # To split n*target_max_block_size row into n blocks
        ),
    )
    map_transformer = MapTransformer([transform_fn])

    operator = MapOperator.create(
        map_transformer,
        input_physical_dag,
        data_context,
        name=op.name,
        compute_strategy=compute,
        ref_bundler=StreamingRepartitionRefBundler(op.target_num_rows_per_block),
        ray_remote_args=op.ray_remote_args,
        ray_remote_args_fn=op.ray_remote_args_fn,
    )
    return operator
```

关键点：
- `transform_fn` 是恒等变换（`lambda blocks, ctx: blocks`），**不做任何数据转换**
- `OutputBlockSizeOption.target_num_rows_per_block` 控制 task 内部输出 block 切分
- `ref_bundler=StreamingRepartitionRefBundler(...)` 控制 Driver 端攒批

---

## 第二层：Task 内部输出 Block 切分

### 调用链

```
MapTransformFn.__call__()                        # map_transformer.py:95-102
  → _pre_process(blocks)                          # 直接返回 blocks
  → _apply_transform(ctx, blocks)                 # 恒等变换: lambda blocks, ctx: blocks
  → _post_process(results)                        # map_transformer.py:411-417
    → _shape_blocks(results)                      # map_transformer.py:62-93
      → BlockOutputBuffer(output_block_size_option)
      → 循环: buffer.add_block(block) + while buffer.has_next(): yield buffer.next()
```

### `_shape_blocks()` — 输出 Block 切分入口

```python
# python/ray/data/_internal/execution/operators/map_transformer.py:62-93
def _shape_blocks(self, results: Iterable[MapTransformFnData]) -> Iterable[Block]:
    buffer = BlockOutputBuffer(self._output_block_size_option)

    # 选择 append 方法
    if self._input_type == MapTransformFnDataType.Block:
        append = buffer.add_block
    elif self._input_type == MapTransformFnDataType.Batch:
        append = buffer.add_batch
    else:
        append = buffer.add

    # 流式处理：输入一个 block，可能输出多个 block
    for result in results:
        append(result)
        while buffer.has_next():       # ← 只要超过 target 就持续输出
            yield buffer.next()

    # 最终 flush
    buffer.finalize()
    while buffer.has_next():
        yield buffer.next()
```

### `BlockOutputBuffer` — 核心缓冲区

```python
# python/ray/data/_internal/output_buffer.py
class BlockOutputBuffer:
    def __init__(self, output_block_size_option: Optional[OutputBlockSizeOption]):
        self._output_block_size_option = output_block_size_option
        self._buffer = DelegatingBlockBuilder()   # 内部统一缓冲区
        self._finalized = False
        self._has_yielded_blocks = False
```

### `has_next()` — 判断是否该输出

```python
# output_buffer.py:144-160
def has_next(self) -> bool:
    if self._finalized:
        # finalize 后：只要 buffer 还有数据就输出（flush 尾巴）
        return not self._has_yielded_blocks or self._buffer.num_rows() > 0
    elif self._output_block_size_option is None:
        return False
    elif self._output_block_size_option.disable_block_shaping:
        return self._buffer.num_rows() > 0
    # 正常模式：行数严格大于 target 才触发
    return self._exceeded_buffer_row_limit() or self._exceeded_buffer_size_limit()

def _exceeded_buffer_row_limit(self) -> bool:
    return (
        self._max_num_rows_per_block() is not None
        and self._buffer.num_rows() > self._max_num_rows_per_block()
        #                           ↑ 注意是严格大于，不是 >=
    )
```

> **关键细节**：判断条件是 `>` 而非 `>=`，所以 buffer 内恰好 100 行时 `has_next()` 返回 `False`，buffer 内 101 行时才返回 `True`。

### `next()` — 物理切分输出

```python
# output_buffer.py:181-212
def next(self) -> Block:
    assert self.has_next()

    block = self._buffer.build()              # 把 buffer 数据构建成一个 block

    accessor = BlockAccessor.for_block(block)
    block_remainder = None
    target_num_rows = None

    # 判断是否需要切分
    if self._exceeded_block_row_slice_limit(accessor):
        # 行数超限
        target_num_rows = self._max_num_rows_per_block()
    elif self._exceeded_block_size_slice_limit(accessor):
        # 字节数超限，估算行数
        num_bytes_per_row = accessor.size_bytes() / accessor.num_rows()
        target_num_rows = max(
            1, math.ceil(self._max_bytes_per_block() / num_bytes_per_row)
        )

    # 精确切分
    if target_num_rows is not None and target_num_rows < accessor.num_rows():
        block = accessor.slice(0, target_num_rows, copy=False)          # 取前 N 行
        block_remainder = accessor.slice(target_num_rows, accessor.num_rows(), copy=False)  # 剩余

    # 重建 buffer，remainder 放回
    self._buffer = DelegatingBlockBuilder()
    if block_remainder is not None:
        self._buffer.add_block(block_remainder)    # 剩余数据放回 buffer，下次继续切

    self._has_yielded_blocks = True
    return block   # 返回恰好 target_num_rows 行的 block
```

### `_exceeded_block_row_slice_limit()` — 切分阈值判断

```python
# output_buffer.py:172-179
def _exceeded_block_row_slice_limit(self, block: BlockAccessor) -> bool:
    # 注意这里也是严格大于（>），不是 >=
    return (
        self._max_num_rows_per_block() is not None
        and block.num_rows() > self._max_num_rows_per_block()
    )
```

### 不记录原始 Block 边界

`BlockOutputBuffer` **不维护任何 block 边界信息**。工作方式是：

```
输入 blocks → 全部喂进 DelegatingBlockBuilder（统一 buffer）
            → build() 产出一整块
            → slice 切分
```

所有输入 block 被 `add_block()` 追加到同一个 `DelegatingBlockBuilder` 后，原始 block 边界信息就**丢失了**。切分纯粹基于累积行数做 `slice(0, target)` / `slice(target, total)`，不关心数据来自哪个原始 block。

这与 Driver 端的 `RefBundle.slice()` 不同——Driver 端是逻辑切分（通过 `BlockSlice` 记录偏移量，零拷贝复用 ObjectRef），而 task 内部的 `BlockOutputBuffer` 是**物理 build + 物理 slice**，产生全新的 block 对象。

---

## 完整流程示例

### 场景：输入 350 行，target_num_rows_per_block=100

#### Driver 端

假设上游产生 [blockA(150行), blockB(200行)]：

```
Step 1: add_bundle(bundleA_150行)
  → pending=[bundleA], total=150, 150 ≥ 100
  → rows_needed_from_last = 150 - 150%100 = 150 - 50 = 100
  → 100 > 0 且 100 < 150 → 对 bundleA 做 slice(100)
    → bundleA_front(100行) + bundleA_remainder(50行)
  → merge [bundleA_front] → ready_bundle_1 (100行)
  → pending=[bundleA_remainder(50行)], total=50

Step 2: add_bundle(bundleB_200行)
  → pending=[bundleA_remainder, bundleB], total=250, 250 ≥ 100
  → rows_needed_from_last = 200 - 250%100 = 200 - 50 = 150
  → 150 > 0 且 150 < 200 → 对 bundleB 做 slice(150)
    → bundleB_front(150行) + bundleB_remainder(50行)
  → merge [bundleA_remainder(50行), bundleB_front(150行)] → ready_bundle_2 (200行)
  → pending=[bundleB_remainder(50行)], total=50

Step 3: done_adding_bundles()
  → flush_remaining=True
  → merge [bundleB_remainder(50行)] → ready_bundle_3 (50行)
```

Driver 端共产生 **3 个 task**：
- Task 1：100 行
- Task 2：200 行
- Task 3：50 行（尾巴）

#### Task 1 内部（100 行）

```
add_block(100行) → buffer=100行
has_next()? → False (100 不> 100)
finalize()
has_next()? → True (finalized && 100 > 0)
next() → build(100行) → 不超限 → 直接返回
  → 输出: block(100行)
```

#### Task 2 内部（200 行，可能来自多个原始 block）

```
Step 1: add_block(block_1) → buffer累积
Step 2: add_block(block_2) → buffer累积至200行
        has_next()? → True (200 > 100)
        next() → build(200行) → slice(0,100) + slice(100,200)
          → 输出: block(100行), remainder(100行)放回buffer
Step 3: has_next()? → False (100 不> 100)
Step 4: finalize()
        has_next()? → True (100 > 0)
        next() → build(100行) → 不超限 → 直接返回
          → 输出: block(100行)
```

#### Task 3 内部（50 行，尾巴）

```
add_block(50行) → buffer=50行
has_next()? → False (50 不> 100)
finalize()
has_next()? → True (finalized && 50 > 0)
next() → build(50行) → 不超限 → 直接返回
  → 输出: block(50行)
```

#### 最终输出

```
[100行, 100行, 100行, 50行] — 共 4 个 block
```

---

## 中间输出 Block 行数保证

| 场景 | 输出 block 行数 | 原因 |
|------|:-:|------|
| 中间 task（ready bundle = N×target 行） | 全部 = target 行 | `has_next()` 严格 `>` 保证 |
| 最后一个 task（flush remaining） | ≤ target 行 | `finalize()` 后按 `num_rows > 0` flush |
| 最后一个 task 内部的最后一个 block | `total_rows % target` | `finalize()` flush 剩余 |

**在正常切分中，中间 block 不会出现不满足行数的情况**。每次 `BlockOutputBuffer.next()` 只有在 `buffer.num_rows() > target` 时才触发，切出的都是恰好 `target` 行。唯一产生不足 target 行 block 的情况是数据集末尾的尾巴。

---

## 对比：非 Streaming 的 SplitRepartition

### `SplitRepartitionTaskScheduler`

```python
# python/ray/data/_internal/planner/exchange/split_repartition_task_scheduler.py
class SplitRepartitionTaskScheduler(ExchangeTaskScheduler):
    def execute(self, refs, output_num_blocks, ctx, ...):
        # 1. 计算全部输入行数
        input_num_rows = sum(ref_bundle.num_rows() for ref_bundle in refs)

        # 2. 计算全局切分索引（等分）
        indices = []
        cur_idx = 0
        for _ in range(output_num_blocks - 1):
            cur_idx += input_num_rows / output_num_blocks
            indices.append(int(cur_idx))

        # 3. 调用 _split_at_indices 做物理切分
        split_return = _split_at_indices(blocks_with_metadata, indices, ...)

        # 4. 提交 reduce task 合并切片
        reduce_return = [
            reduce_task.remote(*split_block_refs[j])
            for j in range(output_num_blocks)
            if len(split_block_refs[j]) > 0
        ]
```

### `_split_at_indices()` — 三阶段切分

```python
# python/ray/data/_internal/split.py:246-291
def _split_at_indices(blocks_with_metadata, indices, owned_by_consumer, block_rows=None):
    # Phase 1: 计算每个 block 的切分点
    block_rows = _calculate_blocks_rows(blocks_with_metadata)
    valid_indices = _generate_valid_indices(block_rows, indices)
    per_block_split_indices = _generate_per_block_split_indices(block_rows, valid_indices)

    # Phase 2: 对每个 block 进行物理切分（远程 task）
    all_blocks_split_results = _split_all_blocks(
        blocks_with_metadata, per_block_split_indices, owned_by_consumer
    )

    # Phase 3: 组装最终结果
    split_sizes = [helper[i] - helper[i-1] for i in range(1, len(helper))]
    return _generate_global_split_results(all_blocks_split_results, split_sizes)
```

### `_generate_per_block_split_indices()` — 全局索引映射到各 block

```python
# split.py:47-88
def _generate_per_block_split_indices(num_rows_per_block, split_indices):
    """将全局切分索引映射到每个 block 内部的局部索引。"""
    per_block_split_indices = []
    current_input_block_id = 0
    current_block_split_indices = []
    current_block_global_offset = 0
    current_index_id = 0

    while current_index_id < len(split_indices):
        split_index = split_indices[current_index_id]
        current_block_row = num_rows_per_block[current_input_block_id]
        if split_index - current_block_global_offset <= current_block_row:
            # 此切分点落在当前 block 内
            current_block_split_indices.append(
                split_index - current_block_global_offset
            )
            current_index_id += 1
            continue
        # 切分点不在当前 block，移动到下一个 block
        per_block_split_indices.append(current_block_split_indices)
        current_block_split_indices = []
        current_block_global_offset += num_rows_per_block[current_input_block_id]
        current_input_block_id += 1

    # 补齐剩余 block
    while len(per_block_split_indices) < len(num_rows_per_block):
        per_block_split_indices.append(current_block_split_indices)
        current_block_split_indices = []
    return per_block_split_indices
```

### `_split_single_block()` — 对单个 block 做物理切分

```python
# split.py:91-134
def _split_single_block(block_id, block, meta, split_indices):
    """在指定索引处切分单个 block。"""
    split_meta = []
    split_blocks = []
    block_accessor = BlockAccessor.for_block(block)
    prev_index = 0
    split_indices.append(meta.num_rows)  # 追加结尾索引
    for index in split_indices:
        split_block = block_accessor.slice(prev_index, index)
        accessor = BlockAccessor.for_block(split_block)
        _meta = BlockMetadata(
            num_rows=accessor.num_rows(),
            size_bytes=accessor.size_bytes(),
            input_files=meta.input_files,
            exec_stats=stats.build(),
        )
        split_meta.append(_meta)
        split_blocks.append(split_block)
        prev_index = index
    return tuple([(block_id, split_meta)] + split_blocks)
```

### 对比总结

| 维度 | StreamingRepartition | SplitRepartition |
|------|------|------|
| **算子类型** | `AbstractMap`（流式 1:1） | `AbstractAllToAll`（全量） |
| **何时提交 task** | 流式：攒够 target 行就提交 | 全量：先等所有输入就绪 |
| **task 数量** | `ceil(total_rows / target)` | `output_num_blocks`（用户指定） |
| **split 方式** | Driver 端逻辑切（BlockSlice）+ task 内物理切 | 全局算 indices → 远程 `_split_single_block` 物理切 → reduce 合并 |
| **跨 block 切分** | ✅ 通过 BlockSlice 偏移量 | ✅ 通过 `_generate_per_block_split_indices` 映射全局索引到各 block |
| **记录切分位置** | Driver 端：`BlockSlice(start, end)` 记录偏移；Task 内：不记录，物理 slice | `per_block_split_indices` 记录每个 block 需要在哪些行切 |
| **中间 block 行数保证** | ✅ 中间 block 都是 target 行 | ✅ 约等于 `total_rows / output_num_blocks` |
| **数据物化** | 不需要物化全部输入 | 需要物化全部输入 |

---

## 总结

### StreamingRepartition 核心设计

1. **两层切分架构**：Driver 端负责攒批和逻辑切割（`StreamingRepartitionRefBundler`），Task 端负责物理切分（`BlockOutputBuffer`），两层都由同一个 `target_num_rows_per_block` 参数驱动。

2. **攒批与切割在 `add_bundle()` 中完成**：`_try_build_ready_bundle()` 在 `add_bundle()` 内部被调用，负责判断行数是否足够、做逻辑切割和 merge。`get_next_bundle()` 只是从就绪队列取出结果。

3. **跨 Block 组合复用切分**：
   - **组合**：通过 `RefBundle.merge_ref_bundles()` 将多个小 block 合并
   - **切分**：通过 `RefBundle.slice()` 精确切割，使用 `BlockSlice` 偏移量实现零拷贝
   - **复用**：同一个物理 block 的 ObjectRef 可被两个 task 共享引用

4. **Task 数量**：`ceil(total_input_rows / target_num_rows_per_block)`

5. **输出 Block 行数保证**：中间 block 都是恰好 target 行，只有最后一个 block 可能不足 target 行（`total_rows % target`）。

6. **Task 内部不记录原始 block 边界**：`BlockOutputBuffer` 将所有输入 block 喂进统一的 `DelegatingBlockBuilder`，原始边界信息丢失，切分纯粹基于累积行数。

---

## `_map_task` 与 `MapTransformer` 的关系

### 执行壳 vs 变换逻辑

`_map_task` 是**远程执行壳**，负责参数解包、切片读取和 streaming yield；`MapTransformer` 是**可序列化的变换逻辑**，负责实际的数据变换和 block shaping。两者是"执行环境"和"变换逻辑"的分离。

```
┌───────────────────────────────────────────────────┐
│  Driver 端                                         │
│                                                    │
│  MapTransformer 创建                               │
│    ├── transform_fns: [BlockMapTransformFn, ...]   │
│    ├── init_fn                                     │
│    └── output_block_size_option                    │
│         │                                          │
│    ray.put() ──→ _map_transformer_ref (ObjectRef)  │
│                                                    │
│  _try_schedule_task():                             │
│    _map_task.remote(                               │
│        _map_transformer_ref,  ← 变换逻辑的引用       │
│        data_context,                               │
│        ctx,                                        │
│        *bundle.block_refs,   ← 数据的引用            │
│        slices=bundle.slices,  ← 切片元数据           │
│    )                                               │
└────────────────────────────────────────────────────┘
                    │
                    │  Ray 调度到远端 Worker
                    ▼
┌───────────────────────────────────────────────────┐
│  远端 Worker                                       │
│                                                    │
│  _map_task(map_transformer, data_context, ctx,    │
│           *blocks, slices):                         │
│                                                    │
│    # 1. map_transformer 是从 ObjectRef 反序列化     │
│    #    得到的，和 Driver 端创建的是同一个对象        │
│                                                    │
│    # 2. 根据 slices 切片读取 blocks                  │
│    blocks_iter = _iter_sliced_blocks(blocks, slices)│
│                                                    │
│    # 3. 调用 map_transformer 执行变换链               │
│    for block in map_transformer.apply_transform(   │
│        blocks_iter, ctx                            │
│    ):                                              │
│        yield block       ← streaming generator      │
│        yield metadata    ← 每个 block 后跟元数据     │
└───────────────────────────────────────────────────┘
```

### `MapTransformer` 的序列化与远端反序列化

#### Driver 端保存引用

```python
# python/ray/data/_internal/execution/operators/map_operator.py:198-244
class MapOperator(OneToOneOperator, ABC):
    def __init__(self, map_transformer, ...):
        self._map_transformer = map_transformer  # 保存变换逻辑
        ...
        # _map_transformer_ref 延迟初始化为 None
        self.__map_transformer_ref = None
```

#### 惰性序列化到 Ray Object Store

```python
# map_operator.py:247-257
@property
def _map_transformer_ref(self):
    """延迟序列化：首次访问时才 ray.put()
    延迟原因：on_start 回调可能在首个 bundle 到达时修改 transformer 状态，
    必须在修改完成后再序列化。
    """
    if self.__map_transformer_ref is None:
        self.__map_transformer_ref = ray.put(self._map_transformer)
        self._warn_large_udf()
    return self.__map_transformer_ref
```

**延迟序列化的原因**：`on_start` 回调（如 Iceberg 写入的 schema 演化）可能在第一个 bundle 到达时修改 transformer 的状态，必须在修改完成后再序列化。

#### `start()` 可能追加额外变换

```python
# map_operator.py:483-498
def start(self, options):
    ...
    map_transformer = self._map_transformer
    # 如果需要额外的 block split
    if self.get_additional_split_factor() > 1:
        split_transformer = MapTransformer([...])
        # fuse 会把两个 transformer 的 transform_fns 拼接
        map_transformer = map_transformer.fuse(split_transformer)

    self._map_transformer = map_transformer  # 更新引用
```

### Task 提交时传递给远端的参数

```python
# python/ray/data/_internal/execution/operators/task_pool_map_operator.py:115-153
def _try_schedule_task(self, bundle: RefBundle, strict: bool):
    self._notify_first_input(bundle)  # 触发 on_start（如果首次）

    ctx = TaskContext(
        task_idx=self._next_data_task_idx,
        op_name=self.name,
        target_max_block_size_override=self.target_max_block_size_override,
    )

    dynamic_ray_remote_args = self._get_dynamic_ray_remote_args(input_bundle=bundle)
    ...

    # _map_task 已被 cached_remote_fn(_map_task, num_returns="streaming")
    # 包装为 Ray remote function
    gen = self._map_task.options(**dynamic_ray_remote_args).remote(
        self._map_transformer_ref,   # ① 变换逻辑（ObjectRef，远端 ray.get() 反序列化）
        data_context,                 # ② DataContext
        ctx,                          # ③ TaskContext
        *bundle.block_refs,           # ④ 所有 block 的 ObjectRef（数据本身不经过 Driver）
        slices=bundle.slices,         # ⑤ 每个 block 的 BlockSlice 元数据
        **self.get_map_task_kwargs(), # ⑥ 额外 kwargs
    )

    self._submit_data_task(gen, bundle)
```

**参数解析**：

| 参数 | 类型 | 传递方式 | 说明 |
|------|------|---------|------|
| `_map_transformer_ref` | ObjectRef | `ray.put()` 序列化到 Object Store | 远端 Worker 自动 `ray.get()` 反序列化 |
| `bundle.block_refs` | List[ObjectRef] | 直接传递引用 | Ray 自动从 ObjectRef 解引用为实际 Block 数据 |
| `bundle.slices` | Tuple[BlockSlice] | 直接传递 Python 对象 | 切片元数据，描述每个 block 中属于本 task 的行范围 |
| `ctx` | TaskContext | 直接传递 | Task 上下文信息 |

### 远端 Worker：`_map_task` 执行

```python
# python/ray/data/_internal/execution/operators/map_operator.py:728-820
def _map_task(
    map_transformer: MapTransformer,    # ← 从 ObjectRef 反序列化得到
    data_context: DataContext,           # ← DataContext
    ctx: TaskContext,                    # ← TaskContext
    *blocks: Block,                     # ← Ray 自动从 ObjectRef 解引用为实际 Block 数据
    slices: Optional[List[BlockSlice]] = None,  # ← 切片元数据
    **kwargs,
) -> Iterator[Union[Block, "BlockMetadataWithSchema"]]:
    task_start_s = time.perf_counter()

    ctx.kwargs.update(kwargs)

    with DataContext.current(data_context), TaskContext.current(ctx):
        # 动态 override（如果有）
        map_transformer.override_target_max_block_size(
            ctx.target_max_block_size_override
        )

        # ====== 步骤 1：根据 slices 切片读取 ======
        blocks_iter = _iter_sliced_blocks(blocks, slices) if slices else iter(blocks)

        # ====== 步骤 2：调用 map_transformer 执行变换链 ======
        yielded_schema = False
        for block in map_transformer.apply_transform(blocks_iter, ctx):
            # ====== 步骤 3：yield 输出 block ======
            block_meta = BlockAccessor.for_block(block).get_metadata()
            ...
            yield block               # streaming generator yield 数据
            yield pickle.dumps(bm)     # yield 元数据
```

**`_map_task` 本身不做任何数据变换**，它只是：
1. 解包参数
2. 根据 slice 元数据切片
3. 调用 `map_transformer.apply_transform()`
4. 将输出 block 以 streaming generator 形式 yield 出去

---

## 远端 Worker 执行链详解

### `_iter_sliced_blocks` — 根据 slice 元数据真正切片读取

```python
# python/ray/data/_internal/execution/interfaces/ref_bundle.py:445-465
def _iter_sliced_blocks(
    blocks: Iterable[Block],
    slices: List[Optional[BlockSlice]],
) -> Iterator[Block]:
    blocks_list = list(blocks)
    for block, block_slice in zip(blocks_list, slices):
        if block_slice is None:
            yield block  # 完整 block，直接 yield
        else:
            accessor = BlockAccessor.for_block(block)
            start = block_slice.start_offset
            end = block_slice.end_offset
            # 真正的切片操作！根据 slice 元数据读取 [start, end) 行
            yield accessor.slice(start, end, copy=False)
```

**`copy=False`**：零拷贝切片，不实际复制数据，返回原 block 的视图。

### `MapTransformer.apply_transform` — 链式惰性迭代器

```python
# python/ray/data/_internal/execution/operators/map_transformer.py:218-245
def apply_transform(self, input_blocks, ctx):
    # 最后一个 transform 负责输出 block 的大小控制
    last_transform = self._transform_fns[-1]
    if self.target_max_block_size_override is not None:
        last_transform.override_target_max_block_size(...)

    iter = input_blocks
    # 顺序执行所有 transform_fn，形成链式惰性迭代器
    for transform_fn in self._transform_fns:
        iter = transform_fn(iter, ctx)  # 调用 __call__
        if transform_fn._is_udf:
            iter = self._udf_timed_iter(iter)  # 计时 UDF

    return iter
```

**关键**：这是一个**惰性链式迭代器**，每个 `transform_fn(iter, ctx)` 返回的是一个新的 Iterable，不会立即执行。只有当 `_map_task` 中 `for block in ...` 消费时才真正触发计算。

对 StreamingRepartition 来说，`_transform_fns` 只有 **1 个** `BlockMapTransformFn`。

### `BlockMapTransformFn.__call__` — 三阶段调用链

```python
# python/ray/data/_internal/execution/operators/map_transformer.py:108-116
class MapTransformFn(ABC):
    def __call__(self, blocks, ctx):
        batches = self._pre_process(blocks)      # 阶段1：预处理
        results = self._apply_transform(ctx, batches)  # 阶段2：执行变换
        yield from self._post_process(results)   # 阶段3：后处理
```

对 StreamingRepartition 的 `BlockMapTransformFn`：

```python
# 阶段1：_pre_process（默认实现，不做事）
def _pre_process(self, blocks):
    return blocks  # 直接返回

# 阶段2：_apply_transform
def _apply_transform(self, ctx, blocks):
    return self._block_fn(blocks, ctx)
    # self._block_fn = lambda blocks, ctx: blocks  (identity)

# 阶段3：_post_process → _shape_blocks
def _post_process(self, results):
    if self._disable_block_shaping:
        return results  # 不切分
    return self._shape_blocks(results)  # 用 BlockOutputBuffer 切分
```

### `_BlockShapingIterator` — 逐 block 喂入 buffer 并按 target 切分输出

```python
# python/ray/data/_internal/execution/operators/map_transformer.py:419-460
class _BlockShapingIterator(Iterator[Block]):
    def __init__(self, results, input_type, output_block_size_option):
        self._results_iter = iter(results)
        self._buffer = BlockOutputBuffer(output_block_size_option)
        self._finalized = False

        if input_type == MapTransformFnDataType.Block:
            self._append_buffer = self._buffer.add_block
        elif input_type == MapTransformFnDataType.Batch:
            self._append_buffer = self._buffer.add_batch
        else:
            assert input_type == MapTransformFnDataType.Row
            self._append_buffer = self._buffer.add

    def __next__(self) -> Block:
        while True:
            # 优先输出：buffer 中已有完整 block
            if self._buffer.has_next():
                return self._buffer.next()  # 返回精确 target 行的 block

            # 已结束，无更多数据
            elif self._finalized:
                raise StopIteration

            try:
                # 从 identity transform 取下一个 block
                result = next(self._results_iter)
                self._append_buffer(result)  # 加入 buffer
            except StopIteration:
                # 所有 block 都处理完，finalize
                self._buffer.finalize()
                self._finalized = True
                # 下一轮循环会检查 has_next()，输出剩余数据
```

### 远端执行完整调用链（StreamingRepartition 场景）

```
_map_task (远端 Worker)
  │
  ├─ _iter_sliced_blocks(blocks, slices)
  │    for each block + slice:
  │      accessor.slice(start, end, copy=False)  # 按元数据真正切片
  │    yield sliced_block
  │
  │  → blocks_iter = [只属于本 task 的行的迭代器]
  │
  ├─ map_transformer.apply_transform(blocks_iter, ctx)
  │    │
  │    │  for transform_fn in [BlockMapTransformFn]:
  │    │    iter = transform_fn(iter, ctx)
  │    │
  │    │  → BlockMapTransformFn.__call__(iter, ctx):
  │    │       │
  │    │       ├─ _pre_process(iter) → iter  (无操作)
  │    │       │
  │    │       ├─ _apply_transform(ctx, iter)
  │    │       │    → lambda blocks, ctx: blocks  (identity)
  │    │       │    → 返回同样的 block 迭代器
  │    │       │
  │    │       └─ _post_process(results)
  │    │            → _shape_blocks(results)
  │    │            → _BlockShapingIterator:
  │    │                 buffer = BlockOutputBuffer(target_num_rows=1024)
  │    │                 │
  │    │                 while True:
  │    │                   if buffer.has_next():        # buffer.num_rows() > target?
  │    │                     return buffer.next()        # 切出精确 target 行
  │    │                   elif finalized:
  │    │                     raise StopIteration
  │    │                   result = next(results_iter)  # 取更多 block
  │    │                   buffer.add_block(result)      # 喂入 buffer
  │    │
  │    │  → 输出: [1024, 1024, ..., <1024 的尾 block]
  │    │
  └─ for block in map_transformer.apply_transform(...):
       yield block              # streaming generator 输出
       yield pickle.dumps(bm)   # 输出元数据

Ray streaming generator 将 block 逐个传回 Driver
```

### 职责分离总结

| 组件 | 运行位置 | 职责 |
|------|---------|------|
| **`StreamingRepartitionRefBundler`** | Driver | 累积行数，按 target 整数倍切分 ready bundle（**输入侧行数对齐**） |
| **`_map_task`** | 远端 Worker | 执行壳：解包参数、切片读取、调用 transformer、streaming yield 输出 |
| **`MapTransformer`** | Driver 创建 → 序列化 → 远端反序列化执行 | 变换链：顺序执行 transform_fns（StreamingRepartition 只有 identity + block shaping） |
| **`BlockMapTransformFn._apply_transform`** | 远端 Worker | 实际变换函数（identity：原样返回 block） |
| **`BlockMapTransformFn._post_process` → `_BlockShapingIterator`** | 远端 Worker | 输出侧 block 塑形：用 `BlockOutputBuffer` 将 n×target 行切分为 n 个 target 行的 block |
| **`BlockOutputBuffer`** | 远端 Worker | 累积→切分循环：buffer 行数 > target 时切出精确 target 行，余数放回 buffer |

---

## Task 提交粒度详解

### 不是"每满 1 个 target 提交 1 个 task"

每次 `_add_input_inner` 调用中，`_try_build_ready_bundle` 只执行**一次** `if` 判断（不是 `while`），因此一个 `add_bundle` 调用最多产生 1 个 ready bundle → 1 个 task。

合并后剩余行 = `total_pending_rows % target`，一定 < target，所以**不会二次触发**。

### 一个 task 可能处理 n×target 行

当一个大 bundle 到来导致累积行数远超 target 时，ready bundle 的行数 = `⌊total/target⌋ × target`，可能是 1×、2×、3×... target。这个 task 内部由 `BlockOutputBuffer` 切成 n 个 target 行的 block。

### 数值示例（target=1024）

| 到来的 bundle 行数 | 累积行数 | ready bundle 行数 | 提交几个 task |
|-------------------|---------|-----------------|-------------|
| 700 | 700 | 0 | 0 |
| 400 (续) | 1100 | **1024**（余76） | 1 |
| 3000 | 3076 | **3072 = 3×1024**（余4） | **1**（不是3） |
| 100+100 | 204 | 0 | 0 |

**场景 3000 行最关键**：来了一个 3000 行的 bundle，加上之前的 4 行剩余，总共 3076 行，超过 3 个 target。但 **只创建 1 个 task**，这个 task 收到 3072 行，task 内部由 `BlockOutputBuffer` 切成 3 个 1024 行的 block。

### `all_inputs_done` — 尾部 flush

```python
# python/ray/data/_internal/execution/operators/map_operator.py:648-666
def all_inputs_done(self):
    # 通知 bundler：不再有新输入
    self._block_ref_bundler.done_adding_bundles()

    # 处理 bundler 中可能剩余的 ready bundle
    while self._block_ref_bundler.has_bundle():
        (input_refs, bundled_input) = self._block_ref_bundler.get_next_bundle()
        for bundle in input_refs:
            self._metrics.on_input_dequeued(bundle, input_index=0)

        # strict=False：不强制启动 task（actors 可能都忙）
        self._try_schedule_task(bundled_input, strict=False)

    super().all_inputs_done()
```

`done_adding_bundles` 调用 `_try_build_ready_bundle(flush_remaining=True)`，把 < target 的剩余行打包为尾 bundle。

---

## `strict` 参数的影响

### `plan_streaming_repartition_op` 完整代码

```python
# python/ray/data/_internal/planner/plan_udf_map_op.py:182-233
def plan_streaming_repartition_op(
    op: StreamingRepartition,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> MapOperator:
    assert len(physical_children) == 1
    input_physical_dag = physical_children[0]
    compute = get_compute(op.compute)

    # 恒等变换 + 输出 block 切分配置
    transform_fn = BlockMapTransformFn(
        lambda blocks, ctx: blocks,
        output_block_size_option=OutputBlockSizeOption.of(
            target_num_rows_per_block=op.target_num_rows_per_block,
        ),
    )
    map_transformer = MapTransformer([transform_fn])

    # strict=True → 使用 StreamingRepartitionRefBundler（精确行数对齐）
    # strict=False → ref_bundler=None，使用默认 BlockRefBundler（best-effort 攒批）
    if op.strict:
        ref_bundler = StreamingRepartitionRefBundler(op.target_num_rows_per_block)
    else:
        ref_bundler = None

    operator = MapOperator.create(
        map_transformer,
        input_physical_dag,
        data_context,
        name=op.name,
        compute_strategy=compute,
        ref_bundler=ref_bundler,
        ray_remote_args=op.ray_remote_args,
        ray_remote_args_fn=op.ray_remote_args_fn,
    )

    return operator
```

### `strict=True` vs `strict=False` 的差异

| 维度 | `strict=True` | `strict=False` |
|------|--------------|---------------|
| **Ref Bundler** | `StreamingRepartitionRefBundler` | `None`（使用默认 `BlockRefBundler`） |
| **Task 输入行数** | **精确** n×target 行 | best-effort，不保证对齐 |
| **Driver 端切割** | `RefBundle.slice()` 精确切割 | 不切割，整 bundle 合并 |
| **Task 内 block shaping** | `BlockOutputBuffer(target_num_rows)` | `BlockOutputBuffer(target_num_rows)` |
| **输出 block 行数保证** | 中间 block 精确 target 行 | best-effort，不保证 |

**注意**：`strict` 默认为 `False`。不开启 `strict=True` 时，Driver 端不保证输入行数对齐，但 Task 内部的 `BlockOutputBuffer` 仍然会按 `target_num_rows_per_block` 切分——只是因为输入不是 target 的整数倍，某些中间 block 可能不足 target 行。

### 逻辑算子定义

```python
# python/ray/data/_internal/logical/operators/map_operator.py:515-569
class StreamingRepartition(AbstractMap):
    def __init__(
        self,
        input_op: LogicalOperator,
        target_num_rows_per_block: int,
        strict: bool = True,
    ):
        super().__init__(
            f"StreamingRepartition[num_rows_per_block={target_num_rows_per_block}]",
            input_op,
            can_modify_num_rows=False,
        )
        self.target_num_rows_per_block = target_num_rows_per_block
        self.strict = strict
```

### 用户 API

```python
# python/ray/data/dataset.py:1843-1847
op = StreamingRepartition(
    self._logical_plan.dag,
    target_num_rows_per_block=target_num_rows_per_block,
    strict=strict,  # 用户可控制
)
```

---

## 完整端到端流程示例（含远端执行）

### 场景：输入 4 个 bundle，行数分别为 700, 400, 600, 800，target=1024

#### Driver 端：攒批与切割

**Step 1：add_bundle(700)**

```
pending = [700], total = 700
700 < 1024 → 不触发构建
```

**Step 2：add_bundle(400)**

```
pending = [700, 400], total = 1100
1100 >= 1024 → 触发构建
  rows_needed = 400 - (1100 % 1024) = 400 - 76 = 324
  0 < 324 < 400 → 切片！

  last_bundle.slice(324):
    block_rows=400, rows_to_take=324
    → consume_slice = BlockSlice(0, 324)     [前324行]
    → remainder_slice = BlockSlice(324, 400) [后76行]

  pending_bundles = [700, sliced(324)]
  merge → ready bundle: 700+324 = 1024行 ✅
  remaining = [76行] 放回 pending

ready_bundles = [1024行 bundle]
→ has_bundle()=True → _try_schedule_task(strict=True)
→ 提交 Task 1
```

**Step 3：add_bundle(600)**

```
pending = [76, 600], total = 676
676 < 1024 → 不触发
```

**Step 4：add_bundle(800)**

```
pending = [76, 600, 800], total = 1476
1476 >= 1024 → 触发构建
  rows_needed = 800 - (1476 % 1024) = 800 - 452 = 348
  0 < 348 < 800 → 切片！

  last_bundle.slice(348):
    block_rows=800, rows_to_take=348
    → consume_slice = BlockSlice(0, 348)
    → remainder_slice = BlockSlice(348, 800)  [452行]

  pending_bundles = [76, 600, sliced(348)]
  merge → ready bundle: 76+600+348 = 1024行 ✅
  remaining = [452行] 放回 pending

ready_bundles = [1024行 bundle]
→ 提交 Task 2
```

**Step 5：all_inputs_done → flush_remaining=True**

```
pending = [452], total = 452
452 < 1024 → 不进入 >= 分支
flush_remaining=True → 直接合并 [452] 为尾 bundle

ready_bundles = [452行 bundle]
→ 提交 Task 3（strict=False）
```

#### 远端 Worker 执行

**Task 1（1024行）**：

```
_iter_sliced_blocks(blocks, slices):
  block_0: accessor.slice(0, 700, copy=False) → 700行
  block_1: accessor.slice(0, 324, copy=False) → 324行

map_transformer.apply_transform(blocks_iter, ctx):
  BlockMapTransformFn.__call__():
    _pre_process: 无操作 → [700行block, 324行block]
    _apply_transform: identity → [700行block, 324行block]
    _post_process → _BlockShapingIterator:
      buffer = BlockOutputBuffer(target_num_rows=1024)

      add_block(700行) → buffer=700 → has_next: 700 > 1024? No
      add_block(324行) → buffer=1024 → has_next: 1024 > 1024? No
      finalize → has_next: True (finalized && 1024 > 0)
      next() → build(1024行) → _exceeded_block_row_slice_limit: 1024 > 1024? No
        → 不切分，直接返回 1024 行 block

  yield block(1024行)
  yield metadata
```

**Task 2（1024行）**：同 Task 1，输出 1 个 1024 行 block。

**Task 3（452行，尾部）**：

```
_iter_sliced_blocks → 读取 452 行
identity transform → 原样返回
_BlockShapingIterator:
  add_block(452行) → buffer=452 → has_next: 452 > 1024? No
  finalize → has_next: True
  next() → build(452行) → 不切分，直接返回

yield block(452行)
yield metadata
```

#### 最终输出

```
[1024行, 1024行, 452行] — 共 3 个 block
```

---

## 两层保证机制总结

| 层级 | 位置 | 机制 | 保证 |
|------|------|------|------|
| **Driver 端** | `StreamingRepartitionRefBundler._try_build_ready_bundle` | `rows_needed = L - (P%T)` 公式 + `RefBundle.slice()` | 每个 ready bundle = **n × T** 行（T 的精确整数倍） |
| **Remote Task 端** | `BlockOutputBuffer.next()` | `accessor.slice(0, target_num_rows)` 切分 | 每个 output block = **T** 行（最后一个可能 < T） |

**切片不拷贝数据**：无论是 Driver 端的 `RefBundle.slice()`（只生成 `BlockSlice` 元数据），还是 Remote Task 端的 `accessor.slice(copy=False)`，都是零拷贝操作，同一个 `ObjectRef` 可被多个 task 共享读取不同行范围。
