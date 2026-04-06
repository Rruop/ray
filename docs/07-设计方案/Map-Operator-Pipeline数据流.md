# Ray Data Map Operator 数据处理管线详解

本文档详细分析 Ray Data 中 `map_batches` 等 map 操作的完整数据流、Task 调度粒度、Batch 处理粒度、Block 输出重组逻辑及其相互关系。

---

## 目录

1. [整体架构概览](#1-整体架构概览)
2. [输入端：Block 到 Batch 的切分与攒行](#2-输入端block-到-batch-的切分与攒行)
3. [UDF 处理阶段](#3-udf-处理阶段)
4. [输出端：BlockOutputBuffer 重组逻辑](#4-输出端blockoutputbuffer-重组逻辑)
5. [Task 调度粒度与 Batch 粒度的关系](#5-task-调度粒度与-batch-粒度的关系)
6. [output_block_size_option 的来源与配置](#6-output_block_size_option-的来源与配置)
7. [Worker 端执行保证：每次 Task 必定 Flush 输出](#7-worker-端执行保证每次-task-必定-flush-输出)
8. [完整数据流举例](#8-完整数据流举例)
9. [min_rows_per_bundle 完整设置链路（逐行追踪）](#9-min_rows_per_bundle-完整设置链路逐行追踪)
10. [BlockRefBundler 合并 RefBundle 的详细逻辑](#10-blockrefbundler-合并-refbundle-的详细逻辑)
11. [map vs map_batches(batch_size=None) 对比分析](#11-map-vs-map_batchesbatch_size_none-对比分析)
12. [输入 block 不足 128MB 时"一进一出"的完整推导](#12-输入-block-不足-128mb-时一进一出的完整推导)
13. [_map_task 在 Worker 端执行的完整链路](#13-_map_task-在-worker-端执行的完整链路)

---

## 1. 整体架构概览

```
               调度层（Driver 端）                        执行层（Worker 端，单次远程调用内部）
         ┌──────────────────────────┐              ┌──────────────────────────────────────┐
         │                          │  1次远程调用  │                                      │
Block A ─┤                          ├─────────────→│  block_iter = [A, B, C]              │
Block B ─┤  BlockRefBundler         │              │       │                              │
Block C ─┤  攒够 min_rows_per_bundle │              │       ▼                              │
         │  打成一个 RefBundle       │              │  Batcher 按 batch_size 切分           │
         │                          │              │  → batch_1 (batch_size 行)            │
         │                          │              │  → batch_2 (batch_size 行)            │
         │                          │              │  → batch_3 (尾部不足 batch_size)       │
         │                          │              │       │                              │
         │                          │              │       ▼ 每个 batch 调一次 UDF         │
         │ 消费 streaming           │  流式返回    │  UDF(batch_1) → results               │
         │ generator 的输出         │←────────────┤  UDF(batch_2) → results               │
         │  · 收到 output block     │              │  UDF(batch_3) → results               │
         │  · 更新元数据            │              │       │                              │
         │                          │              │       ▼                              │
         │                          │              │  BlockOutputBuffer 按 128MiB 重组输出  │
         │                          │              │  yield output blocks (streaming)      │
         └──────────────────────────┘              └──────────────────────────────────────┘
```

**核心要点**：
- Driver 端只做调度（决定把哪些 block 打包发给哪个 Worker）和消费结果
- 所有数据处理（Batcher 切分、UDF 执行、BlockOutputBuffer 攒批重组）都在 **Worker 端** 完成

---

## 2. 输入端：Block 到 Batch 的切分与攒行

### 2.1 核心代码路径

`BatchMapTransformFn._pre_process` 调用 `batch_blocks` → `blocks_to_batches` → `Batcher`

**文件**: `python/ray/data/_internal/execution/operators/map_transformer.py:349-359`

```python
class BatchMapTransformFn(MapTransformFn):
    def _pre_process(self, blocks: Iterable[Block]) -> Iterable[MapTransformFnData]:
        ensure_copy = not self._zero_copy_batch and self._batch_size is not None
        return batch_blocks(
            blocks=iter(blocks),
            stats=None,
            batch_size=self._batch_size,
            batch_format=self._batch_format,
            ensure_copy=ensure_copy,
        )
```

**文件**: `python/ray/data/_internal/block_batching/util.py:75-143`

```python
def blocks_to_batches(block_iter, *, batch_size, ...):
    batcher = Batcher(batch_size=batch_size, ensure_copy=ensure_copy)

    for block in block_iter:
        batcher.add(block)                  # 不断添加 block 到 buffer
        while batcher.has_batch():          # 攒够 batch_size 才切出
            batch = batcher.next_batch()
            yield Batch(...)

    batcher.done_adding()
    # 尾部不够 batch_size 的也输出
    if not drop_last and batcher.has_any():
        batch = batcher.next_batch()
        yield batch
```

### 2.2 Batcher 的攒行逻辑

**文件**: `python/ray/data/_internal/batcher.py:50-157`

```python
class Batcher(BatcherInterface):
    def __init__(self, batch_size: Optional[int], ensure_copy: bool = False):
        self._batch_size = batch_size
        self._buffer = []
        self._buffer_size = 0

    def add(self, block: Block):
        """添加 block 到 buffer，空 block 会被忽略"""
        if BlockAccessor.for_block(block).num_rows() > 0:
            self._buffer.append(block)
            self._buffer_size += BlockAccessor.for_block(block).num_rows()

    def has_batch(self) -> bool:
        """buffer 中行数 >= batch_size 时返回 True"""
        return self.has_any() and (
            self._batch_size is None or self._buffer_size >= self._batch_size
        )

    def next_batch(self) -> Block:
        # batch_size=None 时，直接返回整个 block
        if self._batch_size is None:
            block = self._buffer[0]
            self._buffer = []
            self._buffer_size = 0
            return block

        # 从 buffer 中精确切出 batch_size 行
        output = DelegatingBlockBuilder()
        leftover = []
        needed = self._batch_size
        for block in self._buffer:
            accessor = BlockAccessor.for_block(block)
            if needed <= 0:
                leftover.append(block)
            elif accessor.num_rows() <= needed:
                output.add_block(accessor.to_block())
                needed -= accessor.num_rows()
            else:
                # block 比需要的多，切一部分出来，剩余留在 buffer
                output.add_block(accessor.slice(0, needed, copy=False))
                leftover.append(accessor.slice(needed, accessor.num_rows(), copy=False))
                needed = 0

        self._buffer = leftover
        self._buffer_size -= self._batch_size
        return output.build()
```

### 2.3 batch_size 的两种情况

| `batch_size` 值 | 行为 |
|---|---|
| `None` | 每个输入 block 原样作为一个 batch 传给 UDF（一进一出） |
| 指定了具体值 | **跨 block 攒行**。不断 `add(block)` 累积，直到 `buffer_size >= batch_size` 才切出一个 batch。如果一个 block 行数 > `batch_size`，则 slice 出 `batch_size` 行，剩余留在 buffer |

**关键结论**：输入端的 `batch_size` 会**完全打破 block 边界**，跨 block 攒行或将大 block 切分成多个 batch。

---

## 3. UDF 处理阶段

**文件**: `python/ray/data/_internal/execution/operators/map_transformer.py:361-364`

```python
class BatchMapTransformFn(MapTransformFn):
    def _apply_transform(self, ctx, batches):
        yield from self._batch_fn(batches, ctx)
```

UDF (`_batch_fn`) 接收一个 batch 迭代器，逐个 batch 处理。输出行数完全由 UDF 决定：
- 可以 1:1（如 transform）
- 可以 1:N（如 flatmap/explode）
- 可以 N:0（如 filter 全过滤）

---

## 4. 输出端：BlockOutputBuffer 重组逻辑

### 4.1 _post_process 和 _shape_blocks

**文件**: `python/ray/data/_internal/execution/operators/map_transformer.py:62-102`

```python
class BatchMapTransformFn(MapTransformFn):
    def _post_process(self, results):
        return self._shape_blocks(results)

class MapTransformFn(ABC):
    def _shape_blocks(self, results):
        buffer = BlockOutputBuffer(self._output_block_size_option)

        if self._input_type == MapTransformFnDataType.Batch:
            append = buffer.add_batch
        # ...

        for result in results:
            append(result)
            while buffer.has_next():       # 增量输出：超过阈值就 yield
                yield buffer.next()

        buffer.finalize()                  # 全部处理完后 finalize
        while buffer.has_next():
            yield buffer.next()            # flush 剩余数据
```

**关键点**：所有 batch 的 UDF 输出流入**同一个 `BlockOutputBuffer`**，不是每个 batch 独立一个 buffer。

### 4.2 BlockOutputBuffer 的增量输出判断

**文件**: `python/ray/data/_internal/output_buffer.py:82-212`

```python
class BlockOutputBuffer:
    def __init__(self, output_block_size_option):
        self._output_block_size_option = output_block_size_option
        self._buffer = DelegatingBlockBuilder()
        self._finalized = False
        self._has_yielded_blocks = False

    def has_next(self) -> bool:
        # finalize 后的保底机制
        if self._finalized:
            return not self._has_yielded_blocks or self._buffer.num_rows() > 0

        # 未设置 output_block_size_option 时，不增量输出
        elif self._output_block_size_option is None:
            return False

        # 超过行数限制或字节限制时输出
        return self._exceeded_buffer_row_limit() or self._exceeded_buffer_size_limit()

    def _exceeded_buffer_size_limit(self) -> bool:
        return (
            self._max_bytes_per_block() is not None
            and self._buffer.get_estimated_memory_usage() > self._max_bytes_per_block()
        )
```

### 4.3 输出端的两种模式

| `OutputBlockSizeOption` | 行为 |
|---|---|
| `None`（未指定） | **不增量输出**，把所有 UDF 结果攒成一个大 block 输出（`has_next` 在 finalize 前始终返回 `False`） |
| 指定了 `target_max_block_size`（默认 128MiB）或 `target_num_rows_per_block` | **增量输出**。每次 `add_batch` 后检查是否超过阈值，超过就 yield 一个 block |

### 4.4 输出保底机制

`finalize()` 后的 `has_next()` 逻辑：

```python
return not self._has_yielded_blocks or self._buffer.num_rows() > 0
```

- 如果 buffer 中还有数据 → 输出最后一个 block
- 如果 UDF 没产出任何数据（`_has_yielded_blocks == False`）→ **输出一个空 block**

**结论**：每次 task 调用至少输出一个 block，即使 UDF 过滤掉了所有数据。

---

## 5. Task 调度粒度与 Batch 粒度的关系

### 5.1 Task 粒度：BlockRefBundler

**文件**: `python/ray/data/_internal/execution/operators/map_operator.py:514-534`

```python
class MapOperator:
    def _add_input_inner(self, refs: RefBundle, input_index: int):
        self._block_ref_bundler.add_bundle(refs)
        if self._block_ref_bundler.has_bundle():
            (input_refs, bundled_input) = self._block_ref_bundler.get_next_bundle()
            self._try_schedule_task(bundled_input)   # 一次远程调用
```

**文件**: `python/ray/data/_internal/execution/operators/map_operator.py:813-911`（`BlockRefBundler`）

```python
class BlockRefBundler:
    def has_bundle(self) -> bool:
        return self._bundle_buffer and (
            self._min_rows_per_bundle is None
            or self._bundle_buffer_size >= self._min_rows_per_bundle
            or (self._finalized and self._bundle_buffer_size >= 0)
        )
```

- `min_rows_per_bundle = None`：不攒，每个 RefBundle 直接提交
- `min_rows_per_bundle = N`：攒够 N 行才打包提交为一个 task
- finalize 时：不够也强制提交

### 5.2 远程 Task 执行入口

**文件**: `python/ray/data/_internal/execution/operators/task_pool_map_operator.py:108-143`

```python
class TaskPoolMapOperator:
    def _try_schedule_task(self, bundle: RefBundle, strict: bool):
        ctx = TaskContext(task_idx=self._next_data_task_idx, ...)
        gen = self._map_task.options(...).remote(
            self._map_transformer_ref,
            data_context,
            ctx,
            *bundle.block_refs,       # 多个 block 作为 *args 传入
            slices=bundle.slices,
        )
```

**文件**: `python/ray/data/_internal/execution/operators/map_operator.py:737-810`

```python
def _map_task(map_transformer, data_context, ctx, *blocks, slices=None, **kwargs):
    if slices:
        block_iter = _iter_sliced_blocks(blocks, slices)
    else:
        block_iter = iter(blocks)

    # 对所有 blocks 作为一个整体迭代器，走完整的 transform 链路
    for block in map_transformer.apply_transform(block_iter, ctx):
        yield block
        yield BlockMetadataWithSchema(...)
```

### 5.2.1 ObjectRef 传递与切片机制

Driver 端提交 task 时，**整个 ObjectRef 传过去**，而非只传切片部分的数据。Task 端根据 `slices` 元数据做真正的切片读取。

```python
# task_pool_map_operator.py:137-142
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

Task 端的 `_iter_sliced_blocks` 根据 `BlockSlice` 元数据做真正的切片：

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
            yield accessor.slice(start, end, copy=False)  # 零拷贝视图
```

Ray 自动把 `ObjectRef` 解引用为实际 Block 数据（Arrow Table 等），然后 `accessor.slice(start, end, copy=False)` 取出 `[start, end)` 行的**视图**，不拷贝数据。

**完整流程图**：

```
Driver 端:
  block X (400行) 的 ObjectRef
     │
     ├─ RefBundle.slice(324) →
     │    bundle_A: block_ref=X, slice=BlockSlice(0, 324)      [前324行]
     │    bundle_B: block_ref=X, slice=BlockSlice(324, 400)   [后76行]
     │
     │  提交 Task 1: (X, BlockSlice(0, 324))
     │  提交 Task 2: (X, BlockSlice(324, 400))
     │                ↑ 两个 task 收到同一个 ObjectRef X

远端 Task 1:
  ray.get(X) → 完整 Arrow Table (400行)
  accessor.slice(0, 324, copy=False) → 前324行的零拷贝视图

远端 Task 2:
  ray.get(X) → 完整 Arrow Table (400行)   ← 同一个对象
  accessor.slice(324, 400, copy=False) → 后76行的零拷贝视图
```

**Driver 端元数据切分 vs Task 端物理切分**：

| 维度 | Driver 端 | Task 端 |
|------|----------|---------|
| **操作对象** | `RefBundle`（引用 + 元数据） | `Block`（实际 Arrow Table） |
| **切分方式** | 生成 `BlockSlice(start, end)` 偏移量 | `accessor.slice(start, end)` 物理读取 |
| **是否拷贝数据** | 否（零拷贝，只记录偏移） | `copy=False`，零拷贝视图 |
| **同一个 ObjectRef** | 可被多个 bundle 共享引用 | 可被多个 task 读取不同行范围 |
| **何时读取数据** | 不读取 | `_iter_sliced_blocks` 时按偏移量读取 |

### 5.3 min_rows_per_bundled_input 的来源

**文件**: `python/ray/data/_internal/planner/plan_udf_map_op.py:334`

```python
return MapOperator.create(
    map_transformer,
    ...
    min_rows_per_bundle=op.min_rows_per_bundled_input,  # 来自 batch_size
    ...
)
```

**文件**: `python/ray/data/dataset.py:818-835`

```python
map_batches_op = MapBatches(
    ...
    min_rows_per_bundled_input=batch_size,  # 直接取 batch_size
    ...
)
```

### 5.4 两层攒行对比

| 层级 | 位置 | 控制参数 | 作用 |
|------|------|---------|------|
| **Task 粒度** | Driver 端 `BlockRefBundler` | `min_rows_per_bundle` = `batch_size` | 决定多少 block 打包成一次远程调用 |
| **Batch 粒度** | Worker 端 `Batcher` | `batch_size` | 决定 UDF 每次接收多少行 |
| **输出 Block 粒度** | Worker 端 `BlockOutputBuffer` | `target_max_block_size` (128MiB) | 决定输出 block 大小 |

**核心结论**：`batch_size` 同时影响两件事——Task 的最小输入行数和 UDF 每次处理的行数。但**一个 Task 内部通常包含多个 batch 调用**。Batch 不是远程调用的粒度，而是 UDF 调用的粒度。

---

## 6. output_block_size_option 的来源与配置

### 6.1 默认值

**文件**: `python/ray/data/context.py:55,586`

```python
DEFAULT_TARGET_MAX_BLOCK_SIZE = 128 * 1024 * 1024  # 128 MiB

class DataContext:
    target_max_block_size: Optional[int] = DEFAULT_TARGET_MAX_BLOCK_SIZE
```

### 6.2 物理计划阶段设置

**文件**: `python/ray/data/_internal/planner/plan_udf_map_op.py:287-310`

```python
def plan_udf_map_op(op, data_context, ...):
    output_block_size_option = OutputBlockSizeOption.of(
        target_max_block_size=data_context.target_max_block_size,  # 128 MiB
    )

    transform_fn = BatchMapTransformFn(
        _generate_transform_fn_for_map_batches(fn),
        batch_size=op.batch_size,
        batch_format=op.batch_format,
        zero_copy_batch=op.zero_copy_batch,
        is_udf=True,
        output_block_size_option=output_block_size_option,  # 传入此处
    )
```

### 6.3 用户如何修改

`map_batches()` API **没有暴露** `output_block_size_option` 参数，只能全局修改：

```python
ctx = ray.data.DataContext.get_current()
ctx.target_max_block_size = 256 * 1024 * 1024  # 改为 256 MiB
```

### 6.4 各操作的典型配置

| 操作 | 文件 | 值 |
|---|---|---|
| `map_batches` / `map` / `flat_map` | `plan_udf_map_op.py:287-309` | `data_context.target_max_block_size` (默认 128 MiB) |
| `filter` | `plan_udf_map_op.py:221-258` | `data_context.target_max_block_size` (默认 128 MiB) |
| `read` | `plan_read_op.py:116-118` | `data_context.target_max_block_size` (默认 128 MiB) |
| 内部 project 操作 | `plan_udf_map_op.py:164` | **None**（无 block shaping，1-in/1-out） |
| hash shuffle | `hash_shuffle.py:1804-1806` | `DEFAULT_SHUFFLE_TARGET_MAX_BLOCK_SIZE` (1 GiB) |

### 6.5 Override 机制

**文件**: `python/ray/data/_internal/execution/operators/map_transformer.py:210-224`

```python
class MapTransformer:
    def apply_transform(self, input_blocks, ctx):
        last_transform = self._transform_fns[-1]
        if self.target_max_block_size_override is not None:
            last_transform.override_target_max_block_size(
                self.target_max_block_size_override
            )
        # ...
```

`MapOperator.create()` 可以传入 `target_max_block_size_override`，它会在运行时覆盖最后一个 transform 的 `output_block_size_option`。

---

## 7. Worker 端执行保证：每次 Task 必定 Flush 输出

### 7.1 BlockOutputBuffer 是 Task 级别的局部变量

`BlockOutputBuffer` 在 `_shape_blocks` 内部**局部创建**，生命周期和单次 task 调用绑定：

```python
def _shape_blocks(self, results):
    buffer = BlockOutputBuffer(self._output_block_size_option)  # 每次 task 新建
    # ... 处理 ...
    buffer.finalize()              # task 结束时 finalize
    while buffer.has_next():
        yield buffer.next()        # 把剩余数据全部 flush 出去
```

### 7.2 不存在跨 Task 攒数据

- `BlockOutputBuffer` 没有任何跨 task 的状态持久化机制
- 它是函数内的局部变量，task 执行完就销毁
- `finalize()` 保证所有数据输出

### 7.3 输出保证

| 场景 | 行为 |
|------|------|
| UDF 产出了数据 | 一定输出一个或多个 block |
| UDF 产出空结果（0 行） | 输出一个**空 block**（保底机制，代码注释 `# TODO remove emitting empty blocks` 表示将来可能移除） |

---

## 8. 完整数据流举例

### 场景设定

```python
ds.map_batches(udf_fn, batch_size=1024)
# DataContext.target_max_block_size = 128 MiB (默认)
# 输入：5 个 block，每个 500 行，每行数据很小
```

### Driver 端：BlockRefBundler 攒 block

```
min_rows_per_bundle = 1024 (来自 batch_size)

add(block_1: 500行) → buffer_size=500 < 1024, 不提交
add(block_2: 500行) → buffer_size=1000 < 1024, 不提交
add(block_3: 500行) → buffer_size=1500 >= 1024
  → 提交 Task 1, RefBundle = [block_1, block_2, block_3]

add(block_4: 500行) → buffer_size=500 < 1024, 不提交
add(block_5: 500行) → buffer_size=1000 < 1024, 不提交
finalize()          → 强制提交
  → 提交 Task 2, RefBundle = [block_4, block_5]
```

### Worker 端 Task 1：处理 1500 行

```
收到 blocks = [block_1(500行), block_2(500行), block_3(500行)]

─── _pre_process (Batcher, batch_size=1024) ───
  add(block_1: 500行) → buffer=500, 不够
  add(block_2: 500行) → buffer=1000, 不够
  add(block_3: 500行) → buffer=1500 >= 1024
    → 切出 batch_1: 1024 行 (来自 block_1 全部 + block_2 全部 + block_3 前 24 行)
    → buffer 剩余: block_3 后 476 行
  done_adding()
    → 输出 batch_2: 476 行 (尾部)

─── _apply_transform (调 UDF) ───
  UDF(batch_1: 1024行) → 产出 result_1
  UDF(batch_2: 476行)  → 产出 result_2

─── _post_process (_shape_blocks, target=128MiB) ───
  BlockOutputBuffer 创建
  add_batch(result_1) → 数据很小 << 128MiB, has_next()=False
  add_batch(result_2) → 数据仍很小, has_next()=False
  finalize()          → has_next()=True (有数据未输出)
    → yield 一个 output block (包含 result_1 + result_2 合并)
```

### Worker 端 Task 2：处理 1000 行

```
收到 blocks = [block_4(500行), block_5(500行)]

─── _pre_process (Batcher, batch_size=1024) ───
  add(block_4: 500行) → buffer=500, 不够
  add(block_5: 500行) → buffer=1000, 不够
  done_adding()
    → 输出 batch_1: 1000 行 (不够 batch_size 但已结束)

─── _apply_transform ───
  UDF(batch_1: 1000行) → 产出 result_1

─── _post_process ───
  add_batch(result_1)
  finalize()
    → yield 一个 output block
```

### 最终结果

```
输入: 5 个 block, 共 2500 行
Task: 2 个远程调用
UDF 调用: 3 次 (Task1 中 2 次, Task2 中 1 次)
输出: 2 个 block (每个 task 输出 1 个，因为数据量远小于 128MiB)
```

---

## 附录：关键文件索引

| 文件 | 关键内容 |
|------|---------|
| `python/ray/data/_internal/execution/operators/map_transformer.py` | `MapTransformFn`、`BatchMapTransformFn`、`_shape_blocks`、`MapTransformer` |
| `python/ray/data/_internal/output_buffer.py` | `BlockOutputBuffer`、`OutputBlockSizeOption` |
| `python/ray/data/_internal/block_batching/block_batching.py` | `batch_blocks` 入口 |
| `python/ray/data/_internal/block_batching/util.py` | `blocks_to_batches`、`Batcher` 调用 |
| `python/ray/data/_internal/batcher.py` | `Batcher` 核心攒行逻辑 |
| `python/ray/data/_internal/execution/operators/map_operator.py` | `MapOperator`、`_map_task`、`BlockRefBundler` |
| `python/ray/data/_internal/execution/operators/task_pool_map_operator.py` | `TaskPoolMapOperator` 远程 task 提交 |
| `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py` | `ActorPoolMapOperator` actor 调用 |
| `python/ray/data/_internal/planner/plan_udf_map_op.py` | 物理计划：构建 `BatchMapTransformFn` 和 `MapOperator` |
| `python/ray/data/context.py` | `DEFAULT_TARGET_MAX_BLOCK_SIZE` (128 MiB) |
| `python/ray/data/dataset.py` | `map_batches()` 用户 API 入口 |

---

## 附录 B：`_map_task` 与 `MapTransformer` 的关系

### 执行壳 vs 变换逻辑

`_map_task` 是**远程执行壳**，负责参数解包、切片读取和 streaming yield；`MapTransformer` 是**可序列化的变换逻辑**，负责实际的数据变换和 block shaping。

```
┌───────────────────────────────────────────────────┐
│  Driver 端                                         │
│                                                    │
│  MapTransformer 创建                               │
│    ├── transform_fns: [BatchMapTransformFn, ...]   │
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
│    # 1. map_transformer 从 ObjectRef 反序列化       │
│    #    和 Driver 端创建的是同一个 Python 对象       │
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

### `MapTransformer` 的序列化机制

```python
# python/ray/data/_internal/execution/operators/map_operator.py:244-257
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

`start()` 中还可能通过 `fuse()` 追加额外变换（如 additional_split_factor），之后才序列化。

### `_map_task` 远程执行代码

```python
# python/ray/data/_internal/execution/operators/map_operator.py:728-820
def _map_task(
    map_transformer: MapTransformer,    # ← 从 ObjectRef 反序列化得到
    data_context: DataContext,
    ctx: TaskContext,
    *blocks: Block,                     # ← Ray 自动从 ObjectRef 解引用
    slices: Optional[List[BlockSlice]] = None,
    **kwargs,
) -> Iterator[Union[Block, "BlockMetadataWithSchema"]]:
    with DataContext.current(data_context), TaskContext.current(ctx):
        map_transformer.override_target_max_block_size(
            ctx.target_max_block_size_override
        )

        # 步骤1：根据 slices 切片读取
        blocks_iter = _iter_sliced_blocks(blocks, slices) if slices else iter(blocks)

        # 步骤2：调用 map_transformer 执行变换链
        for block in map_transformer.apply_transform(blocks_iter, ctx):
            # 步骤3：yield 输出 block
            yield block
            yield pickle.dumps(BlockMetadataWithSchema(...))
```

**`_map_task` 本身不做任何数据变换**，它只是：
1. 解包参数
2. 根据 slice 元数据切片
3. 调用 `map_transformer.apply_transform()`
4. 将输出 block 以 streaming generator 形式 yield 出去

### `apply_transform` 的链式惰性迭代器

```python
# python/ray/data/_internal/execution/operators/map_transformer.py:218-245
def apply_transform(self, input_blocks, ctx):
    last_transform = self._transform_fns[-1]
    if self.target_max_block_size_override is not None:
        last_transform.override_target_max_block_size(...)

    iter = input_blocks
    for transform_fn in self._transform_fns:
        iter = transform_fn(iter, ctx)  # 惰性链式，不会立即执行
        if transform_fn._is_udf:
            iter = self._udf_timed_iter(iter)  # 计时 UDF
    return iter
```

**关键**：每个 `transform_fn(iter, ctx)` 返回的是一个新的 Iterable，不会立即执行。只有当 `_map_task` 中 `for block in ...` 消费时才真正触发计算。

### `_iter_sliced_blocks` — 根据 slice 元数据真正切片

```python
# python/ray/data/_internal/execution/interfaces/ref_bundle.py:445-465
def _iter_sliced_blocks(blocks, slices):
    blocks_list = list(blocks)
    for block, block_slice in zip(blocks_list, slices):
        if block_slice is None:
            yield block
        else:
            accessor = BlockAccessor.for_block(block)
            yield accessor.slice(block_slice.start_offset, block_slice.end_offset, copy=False)
```

`copy=False`：零拷贝切片，不实际复制数据。

### Task 提交参数解析

| 参数 | 类型 | 传递方式 | 说明 |
|------|------|---------|------|
| `_map_transformer_ref` | ObjectRef | `ray.put()` 序列化到 Object Store | 远端 Worker 自动 `ray.get()` 反序列化 |
| `bundle.block_refs` | List[ObjectRef] | 直接传递引用 | Ray 自动从 ObjectRef 解引用为实际 Block 数据 |
| `bundle.slices` | Tuple[BlockSlice] | 直接传递 Python 对象 | 切片元数据（StreamingRepartition 场景） |
| `ctx` | TaskContext | 直接传递 | Task 上下文 |

### 职责分离

| 组件 | 运行位置 | 职责 |
|------|---------|------|
| **`_map_task`** | 远端 Worker | 执行壳：解包参数、切片读取、调用 transformer、streaming yield 输出 |
| **`MapTransformer`** | Driver 创建 → 序列化 → 远端反序列化执行 | 变换链：顺序执行 transform_fns（预处理 → UDF → block shaping） |
| **`BlockRefBundler` / `StreamingRepartitionRefBundler`** | Driver | 攒批逻辑：决定哪些 block 打包成一个 task |
| **`BlockOutputBuffer`** | 远端 Worker | 输出 block 塑形：按阈值切分输出 |

---

## 9. `min_rows_per_bundle` 完整设置链路（逐行追踪）

本节从用户 API 调用开始，逐层追踪 `min_rows_per_bundle` 如何从 `batch_size` 演变到 `BlockRefBundler` 的目标行数。

### 9.1 逻辑层：设置 `min_rows_per_bundled_input`

**文件**: `python/ray/data/dataset.py:818-835`

```python
# 用户调用 ds.map_batches(fn, batch_size=N)
map_batches_op = MapBatches(
    self._logical_plan.dag,
    fn,
    batch_size=batch_size,               # ← 用户传入的 batch_size
    ...
    min_rows_per_bundled_input=batch_size,  # ← 同一个值，复用为调度层目标行数
    ...
)
```

**关键**：`batch_size` 被同时赋给两个用途：
- `op.batch_size = N` → 传给 Worker 端 `Batcher`，控制 UDF 每次处理的行数
- `op.min_rows_per_bundled_input = N` → 传给 Driver 端 `BlockRefBundler`，控制每个 map task 至少凑多少行输入

**文件**: `python/ray/data/_internal/logical/operators/map_operator.py:60`

```python
class AbstractMap(AbstractOneToOne):
    def __init__(
        self,
        name: str,
        ...
        min_rows_per_bundled_input: Optional[int] = None,  # 默认 None
        ...
    ):
        """
        Args:
            min_rows_per_bundled_input: Minimum number of rows a single bundle of
                blocks passed on to the task must possess.
        """
        self.min_rows_per_bundled_input = min_rows_per_bundled_input
```

### 9.2 规划层：逻辑算子 → 物理算子

**文件**: `python/ray/data/_internal/planner/plan_udf_map_op.py:326-340`

```python
def plan_udf_map_op(op: AbstractUDFMap, physical_children, data_context):
    ...
    return MapOperator.create(
        map_transformer,
        input_physical_dag,
        data_context,
        name=op.name,
        compute_strategy=compute,
        min_rows_per_bundle=op.min_rows_per_bundled_input,  # ← 传递到物理层
        ray_remote_args_fn=op.ray_remote_args_fn,
        ray_remote_args=op.ray_remote_args,
        per_block_limit=op.per_block_limit,
        id=op._id,
    )
```

### 9.3 物理算子构造：`BlockRefBundler` 持有该值

**文件**: `python/ray/data/_internal/execution/operators/map_operator.py:189-213`

```python
class MapOperator:
    def __init__(
        self,
        map_transformer: MapTransformer,
        input_op: PhysicalOperator,
        data_context: DataContext,
        name: str,
        target_max_block_size_override: Optional[int],
        min_rows_per_bundle: Optional[int],          # ← 接收
        ref_bundler: Optional[BaseRefBundler],
        ...
    ):
        ...
        # Bundles block references up to the min_rows_per_bundle target.
        self._block_ref_bundler = ref_bundler or BlockRefBundler(min_rows_per_bundle)
```

`BlockRefBundler` 构造函数（map_operator.py:822-836）：

```python
class BlockRefBundler(BaseRefBundler):
    """Rebundles RefBundles to get them close to a particular number of rows."""

    def __init__(self, min_rows_per_bundle: Optional[int]):
        assert (
            min_rows_per_bundle is None or min_rows_per_bundle >= 0
        ), "Min rows per bundle has to be non-negative"
        self._min_rows_per_bundle = min_rows_per_bundle
        self._bundle_buffer: List[RefBundle] = []
        self._bundle_buffer_size = 0
        self._bundle_buffer_size_bytes = 0
        self._finalized = False
```

### 9.4 `map`（map_rows）与 `map_batches` 的 `min_rows_per_bundle` 对比

**文件**: `python/ray/data/dataset.py:440-455`

```python
# ds.map(fn) — 注意：没有传 min_rows_per_bundled_input
map_op = MapRows(
    self._logical_plan.dag,
    fn,
    fn_args=fn_args,
    fn_kwargs=fn_kwargs,
    fn_constructor_args=fn_constructor_args,
    fn_constructor_kwargs=fn_constructor_kwargs,
    compute=compute,
    ray_remote_args_fn=ray_remote_args_fn,
    ray_remote_args=ray_remote_args,
    custom_id=custom_id,
    custom_name=custom_name,
)
# → MapRows 构造时 min_rows_per_bundled_input 默认 None
```

| 操作 | `min_rows_per_bundled_input` | `BlockRefBundler` 行为 |
|------|---|---|
| `map_batches(batch_size=N)` | `N` | 攒够 N 行才提交一个 map task |
| `map_batches(batch_size=None)` | `None` | 不合并，每个 block 一个 map task |
| `map(fn)` | `None`（默认） | 不合并，每个 block 一个 map task |
| `flat_map(fn)` | `None`（默认） | 不合并，每个 block 一个 map task |

---

## 10. `BlockRefBundler` 合并 RefBundle 的详细逻辑

### 10.1 `add_bundle` — 累加到缓冲区

**文件**: `python/ray/data/_internal/execution/operators/map_operator.py:847-851`

```python
def add_bundle(self, bundle: RefBundle):
    """Add a bundle to the bundler."""
    self._bundle_buffer.append(bundle)
    self._bundle_buffer_size += self._get_bundle_size(bundle)  # 累加行数
    self._bundle_buffer_size_bytes += bundle.size_bytes()
```

只做缓冲，**不实际合并**。每个 bundle 原样存入 list。

### 10.2 `has_bundle` — 是否凑够

**文件**: `map_operator.py:851-857`

```python
def has_bundle(self) -> bool:
    return self._bundle_buffer and (
        self._min_rows_per_bundle is None
        or self._bundle_buffer_size >= self._min_rows_per_bundle
        or (self._finalized and self._bundle_buffer_size >= 0)
    )
```

三种返回 True 的情况：
1. `min_rows_per_bundle is None`：buffer 非空即 True（每个 bundle 单独成一个 task）
2. `buffer_size >= min_rows_per_bundle`：攒够了目标行数
3. `finalized and buffer_size >= 0`：上游输入结束，剩余全部强制提交

### 10.3 `get_next_bundle` — 真正合并

**文件**: `map_operator.py:863-908`

```python
def get_next_bundle(self):
    assert self.has_bundle()

    # min_rows_per_bundle is None 分支：短路，不合并
    if self._min_rows_per_bundle is None:
        assert len(self._bundle_buffer) == 1
        bundle = self._bundle_buffer[0]
        self._bundle_buffer = []
        self._bundle_buffer_size = 0
        self._bundle_buffer_size_bytes = 0
        return [bundle], bundle

    # 否则：按行数累加多个 bundle 到 output_buffer
    remainder = []
    output_buffer = []
    output_buffer_size = 0

    for idx, bundle in enumerate(self._bundle_buffer):
        bundle_size = self._get_bundle_size(bundle)

        # Add bundle to the output buffer so long as either
        #   - Output buffer size is still 0
        #   - Output buffer doesn't exceed the `_min_rows_per_bundle` threshold
        if (
            output_buffer_size < self._min_rows_per_bundle
            or output_buffer_size == 0
        ):
            output_buffer.append(bundle)
            output_buffer_size += bundle_size
        else:
            remainder = self._bundle_buffer[idx:]
            break

    self._bundle_buffer = remainder
    self._bundle_buffer_size = sum(
        self._get_bundle_size(bundle) for bundle in remainder
    )
    self._bundle_buffer_size_bytes = sum(
        bundle.size_bytes() for bundle in remainder
    )

    return list(output_buffer), _merge_ref_bundles(*output_buffer)
```

**合并算法**：遍历 buffer 中的 bundle，累加行数。只要累加值 < `min_rows_per_bundle`，就继续加入 `output_buffer`。一旦超过阈值，剩余 bundle 留在 buffer 中等下一轮。`output_buffer_size == 0` 的兜底条件确保即使某个 bundle 行数已超阈值，也至少打包一个 bundle（避免空提交）。

### 10.4 `_merge_ref_bundles` — 不做物理拼接

**文件**: `map_operator.py:919-929`

```python
def _merge_ref_bundles(*bundles: RefBundle) -> RefBundle:
    """Merge N ref bundles into a single bundle of multiple blocks."""
    bundles = [bundle for bundle in bundles if bundle is not None]
    assert len(bundles) > 0
    blocks = list(
        itertools.chain(block for bundle in bundles for block in bundle.blocks)
    )
    owns_blocks = all(bundle.owns_blocks for bundle in bundles)
    schema = _take_first_non_empty_schema(bundle.schema for bundle in bundles)
    return RefBundle(blocks, owns_blocks=owns_blocks, schema=schema)
```

**关键澄清**：这里**没有物理拼接数据**。`itertools.chain` 只是把多个 bundle 的 block 列表串接成一个 list。新 `RefBundle` 的 `blocks` 字段是一个包含原来分散在各 bundle 里所有 block 的 list。block 本身还是各自独立的对象（Arrow Table / pandas DataFrame），没有被 concat。

物理拼接发生在两个地方：
- **输入端**：Worker 端 `Batcher.next_batch()` 中的 `DelegatingBlockBuilder.add_block()` + `build()`
- **输出端**：Worker 端 `BlockOutputBuffer.next()` 中的 `DelegatingBlockBuilder.build()`

### 10.5 `_add_input_inner` — 输入到达时的调用

**文件**: `map_operator.py:516-528`

```python
def _add_input_inner(self, refs: RefBundle, input_index: int):
    assert input_index == 0, input_index

    # Add RefBundle to the bundler.
    self._block_ref_bundler.add_bundle(refs)       # 累加 block 到 buffer
    self._metrics.on_input_queued(refs)

    if self._block_ref_bundler.has_bundle():        # 凑够了 min_rows_per_bundle 行?
        # The ref bundler combines one or more `RefBundle`s into a new
        # `RefBundle`. To update metrics appropriately, we need to deque
        # original input bundles.
        (input_refs, bundled_input) = self._block_ref_bundler.get_next_bundle()
        for bundle in input_refs:
            self._metrics.on_input_dequeued(bundle)

        # If the bundler has a full bundle, add it to the operator's task submission
        # queue
        #
        # NOTE: This is a strict path, hence operator is *required* to launch
        #       at least 1 task
        self._try_schedule_task(bundled_input, strict=True)  # 提交一个 map task
```

`strict=True` 表示一旦 `has_bundle()` 为 True，**必须提交至少一个 task**。

---

## 11. `map` vs `map_batches(batch_size=None)` 对比分析

### 11.1 `map`（map_rows）的 transform 链

**文件**: `python/ray/data/_internal/execution/operators/map_transformer.py:283-317`

```python
class RowMapTransformFn(MapTransformFn):
    """A rows-to-rows MapTransformFn."""

    def _pre_process(self, blocks: Iterable[Block]) -> Iterable[MapTransformFnData]:
        # 逐行迭代 block，不涉及 batch_size
        for block in blocks:
            block = BlockAccessor.for_block(block)
            for row in block.iter_rows(public_row_format=True):
                yield row

    def _apply_transform(self, ctx, inputs):
        yield from self._row_fn(inputs, ctx)  # fn 对每行调用一次

    def _post_process(self, results):
        return self._shape_blocks(results)  # 同样走 BlockOutputBuffer
```

### 11.2 `map_batches(batch_size=None)` 的 transform 链

**文件**: `python/ray/data/_internal/execution/operators/map_transformer.py:331-367`

```python
class BatchMapTransformFn(MapTransformFn):
    def _pre_process(self, blocks):
        return batch_blocks(
            blocks=iter(blocks),
            stats=None,
            batch_size=self._batch_size,  # None
            ...
        )
```

当 `batch_size=None` 时，`Batcher` (batcher.py:110) 短路：

```python
def next_batch(self) -> Block:
    if self._batch_size is None:
        assert len(self._buffer) == 1
        block = self._buffer[0]
        ...
        return block  # 整个 block 当一个 batch
```

### 11.3 三种场景完整对比

| 维度 | `map(fn)` | `map_batches(fn, batch_size=None)` | `map_batches(fn, batch_size=N)` |
|------|-----------|-------------------------------------|--------------------------------|
| **调度层** `min_rows_per_bundle` | `None` | `None` | `N` |
| **调度层行为** | 不合并，每个 block 一个 task | 不合并，每个 block 一个 task | 攒够 N 行才打包成一个 task |
| **Worker 端预处理** | 逐行迭代 block | `Batcher(None)` 短路，每个 block 一个 batch | `Batcher(N)` 跨 block 拼接切分 |
| **UDF 调用粒度** | 每行一次 | 每个 block 一次 | 每 N 行一次 |
| **输出端** | `BlockOutputBuffer`（128MB 阈值） | `BlockOutputBuffer`（128MB 阈值） | `BlockOutputBuffer`（128MB 阈值） |
| **输出 block 数** | 1+（取决于数据量 vs 128MB） | 1+ | 1+ |

### 11.4 "一个 block 一个 map task" 的精确含义

当 `min_rows_per_bundle=None` 时（`map` 或 `map_batches(batch_size=None)`）：

1. `BlockRefBundler.has_bundle()` → `min_rows_per_bundle is None` 分支 → buffer 非空即 True
2. `get_next_bundle()` → 短路返回 buffer 中的唯一一个 bundle（`assert len(self._bundle_buffer) == 1`）
3. `_try_schedule_task(bundled_input)` → 提交一个 map task，输入只含一个 block
4. Worker 端 `iter(blocks)` → 只有一个 block 的迭代器

所以 **"一个 block 一个 map task"** 是准确的。但 **"输出一个 block"** 不一定——取决于输出数据量是否超过 `target_max_block_size`。

---

## 12. 输入 block 不足 128MB 时"一进一出"的完整推导

### 12.1 场景

```python
ds.map_batches(fn, batch_size=None)
# 或 ds.map(fn)
# DataContext.target_max_block_size = 128 MiB (默认)
# 输入 block 大小：50 MiB（不足 128 MiB）
# fn 不显著膨胀数据（如 1:1 transform）
```

### 12.2 调度层

```
BlockRefBundler(min_rows_per_bundle=None)
  add(block_1: 50MiB) → has_bundle()=True (None 分支)
  get_next_bundle() → [block_1], block_1  (不合并)
  _try_schedule_task(block_1) → 提交 Task 1
```

### 12.3 Worker 端 — 输入处理

```
_map_task 收到 blocks = (block_1,)
block_iter = iter(blocks)  →  只有一个 block

BatchMapTransformFn._pre_process (batch_blocks):
  Batcher(batch_size=None):
    add(block_1) → buffer=[block_1], buffer_size=...
    has_batch() → True (batch_size=None 短路)
    next_batch() → 返回 block_1 (不切分)

fn(block_1) → result_batch  (fn 调用一次)
```

### 12.4 Worker 端 — 输出处理

```
_shape_blocks:
  buffer = BlockOutputBuffer(output_block_size_option)
    # output_block_size_option 非 None (target_max_block_size=128MiB)

  append = buffer.add_batch

  for result in [result_batch]:
      buffer.add_batch(result_batch)
      # DelegatingBlockBuilder.add_batch → batch_to_block → add_block → concat
      # buffer 累积后约 50 MiB < 128 MiB

      while buffer.has_next():
          # has_next() 在未 finalize 时:
          #   output_block_size_option 非 None → 检查 _exceeded_buffer_size_limit()
          #   50 MiB < 128 MiB → 返回 False
          # → 不产出 block

  buffer.finalize()
  while buffer.has_next():
      # has_next() 在 finalized 后:
      #   return not self._has_yielded_blocks or self._buffer.num_rows() > 0
      #   _has_yielded_blocks=False → not False = True
      #   → 返回 True (保底输出)
      yield buffer.next()
      # buffer.build() → 产出唯一一个 block (包含 result_batch 的全部数据)
      # _has_yielded_blocks = True
```

### 12.5 结论

**输入 block 不足 128MB + fn 不膨胀数据 → 一个输入 block 进、一个输出 block 出（一进一出）。**

只有以下情况会打破"一进一出"：

| 打破条件 | 原因 | 输出 block 数 |
|----------|------|---------------|
| fn 膨胀数据（输出 > 128MB） | `BlockOutputBuffer` 增量切分 | 多个 |
| `output_block_size_option = None` | 不增量输出但也不做 block shaping，`finalize()` 产出唯一一个 | 1 个 |
| `disable_block_shaping = True` | 每个 batch 直接作为一个 block | 等于 fn 调用次数 |
| `target_num_rows_per_block` 设定 | 按行数而非字节限制切分 | 1+ |

### 12.6 `BlockOutputBuffer.has_next()` 三种模式完整说明

**文件**: `python/ray/data/_internal/output_buffer.py:140-152`

```python
def has_next(self) -> bool:
    if self._finalized:
        # finalize 后：有数据就输出；没输出过也强制输出一个空 block
        return not self._has_yielded_blocks or self._buffer.num_rows() > 0
    elif self._output_block_size_option is None:
        # 不分块：等 finalize 一次性吐出（1-in/1-out 语义）
        return False
    elif self._output_block_size_option.disable_block_shaping:
        # 禁用 shaping：每个 batch 直接当 block
        return self._buffer.num_rows() > 0
    # 正常模式：超过字节/行数限制才产出
    return self._exceeded_buffer_row_limit() or self._exceeded_buffer_size_limit()
```

| `output_block_size_option` | 未 finalize 时 `has_next()` | finalize 后 `has_next()` |
|---|---|---|
| `None` | 始终 `False` | `True`（保底输出 1 个 block） |
| 有 `target_max_block_size`（默认 128MB） | 超过阈值才 `True` | `True`（剩余数据或空 block 保底） |
| 有 `target_num_rows_per_block` | 超过行数才 `True` | `True` |
| `disable_block_shaping=True` | 有数据就 `True` | `True` |

### 12.7 `BlockOutputBuffer.next()` — 物理构建 block

**文件**: `output_buffer.py:162-212`

```python
def next(self) -> Block:
    assert self.has_next()

    block = self._buffer.build()  # DelegatingBlockBuilder.build() → 物理拼接

    accessor = BlockAccessor.for_block(block)
    block_remainder = None
    target_num_rows = None

    # 如果构建出的 block 超过阈值 1.5 倍，切片
    if self._exceeded_block_row_slice_limit(accessor):
        target_num_rows = self._max_num_rows_per_block()
    elif self._exceeded_block_size_slice_limit(accessor):
        assert accessor.num_rows() > 0, "Block may not be empty"
        num_bytes_per_row = accessor.size_bytes() / accessor.num_rows()
        target_num_rows = max(
            1, math.ceil(self._max_bytes_per_block() / num_bytes_per_row)
        )

    if target_num_rows is not None and target_num_rows < accessor.num_rows():
        block = accessor.slice(0, target_num_rows, copy=False)
        block_remainder = accessor.slice(
            target_num_rows, accessor.num_rows(), copy=False
        )

    self._buffer = DelegatingBlockBuilder()  # 清空 buffer
    if block_remainder is not None:
        self._buffer.add_block(block_remainder)  # 剩余放回新 buffer

    self._has_yielded_blocks = True
    return block
```

`build()` 调用 `DelegatingBlockBuilder.build()` → `ArrowBlockBuilder.build()` → 产出物理合并后的 Arrow Table。这才是输出端的**真正物理拼接**。

切片机制（`MAX_SAFE_BLOCK_SIZE_FACTOR` = 1.5）：如果 build 出的 block 超过 `target_max_block_size` 的 1.5 倍（即 192MB），才切片，确保最后一个 block 至少是目标大小的一半。

---

## 13. `_map_task` 在 Worker 端执行的完整链路

### 13.1 `_map_task` 是 Ray remote function

**文件**: `python/ray/data/_internal/execution/operators/task_pool_map_operator.py:107`

```python
# 构造时：用 cached_remote_fn 把 _map_task 包装成 Ray remote function
self._map_task = cached_remote_fn(_map_task, **ray_remote_static_args)
```

`cached_remote_fn` 本质是对 `ray.remote(_map_task)` 的封装，返回一个可 `.remote()` 调用的 remote function handle。

### 13.2 Driver 端提交 task

**文件**: `task_pool_map_operator.py:109-153`

```python
def _try_schedule_task(self, bundle: RefBundle, strict: bool):
    # Notify first input for deferred initialization (e.g., Iceberg schema evolution).
    self._notify_first_input(bundle)
    # Submit the task as a normal Ray task.
    ctx = TaskContext(
        task_idx=self._next_data_task_idx,
        op_name=self.name,
        target_max_block_size_override=self.target_max_block_size_override,
    )

    dynamic_ray_remote_args = self._get_dynamic_ray_remote_args(input_bundle=bundle)
    dynamic_ray_remote_args["name"] = self.name
    logical_usage = ExecutionResources.from_resource_dict(dynamic_ray_remote_args)

    if (
        "_generator_backpressure_num_objects" not in dynamic_ray_remote_args
        and self.data_context._max_num_blocks_in_streaming_gen_buffer is not None
    ):
        # The `_generator_backpressure_num_objects` parameter should be
        # `2 * _max_num_blocks_in_streaming_gen_buffer` because we yield
        # 2 objects for each block: the block and the block metadata.
        dynamic_ray_remote_args["_generator_backpressure_num_objects"] = (
            2 * self.data_context._max_num_blocks_in_streaming_gen_buffer
        )

    gen = self._map_task.options(**dynamic_ray_remote_args).remote(
        self._map_transformer_ref,        # ① 逻辑：MapTransformer 的 ObjectRef
        self._data_context_ref,          # ② DataContext 的 ObjectRef
        ctx,                             # ③ TaskContext（Python 对象）
        *bundle.block_refs,              # ④ 数据：各 block 的 ObjectRef（展开为 *args）
        slices=bundle.slices,            # ⑤ 切片元数据
        **self.get_map_task_kwargs(),    # ⑥ 额外 kwargs
    )

    self._current_logical_usage = self._current_logical_usage.add(logical_usage)

    def task_done_callback():
        self._current_logical_usage = self._current_logical_usage.subtract(logical_usage)

    self._submit_data_task(gen, bundle, task_done_callback=task_done_callback)
```

**关键点**：
- `.remote(...)` 把参数发给 Ray 调度器，Ray 在远端 Worker 上执行 `_map_task` 函数体
- `num_returns="streaming"` 表示返回一个 streaming generator，Worker 可以增量产出的 output block，Driver 端增量消费
- `_generator_backpressure_num_objects` 控制 streaming generator 缓冲区中未消费对象的上限，实现反压

### 13.3 Worker 端执行 `_map_task`

**文件**: `python/ray/data/_internal/execution/operators/map_operator.py:743-810`

```python
def _map_task(
    map_transformer: MapTransformer,    # ← Ray 从 ObjectRef 解引用得到
    data_context: DataContext,          # ← Ray 从 ObjectRef 解引用得到
    ctx: TaskContext,                   # ← 直接传递的 Python 对象
    *blocks: Block,                     # ← Ray 自动从各 ObjectRef 解引用为实际 Block
    slices: Optional[List[BlockSlice]] = None,
    **kwargs,
) -> Iterator[Union[Block, "BlockMetadataWithSchema"]]:
    logger.debug(
        "Executing map task of operator %s with task index %d",
        ctx.op_name,
        ctx.task_idx,
    )
    DataContext._set_current(data_context)
    ctx.kwargs.update(kwargs)

    TaskContext.set_current(ctx)

    stats = BlockExecStats.builder()
    map_transformer.override_target_max_block_size(ctx.target_max_block_size_override)

    # 步骤1：根据 slices 切片读取 blocks
    block_iter: Iterable[Block]
    if slices:
        block_iter = _iter_sliced_blocks(blocks, slices)
    else:
        block_iter = iter(blocks)

    # 步骤2：调用 map_transformer 执行变换链（预处理 → UDF → block shaping）
    with MemoryProfiler(data_context.memory_usage_poll_interval_s) as profiler:
        for block in map_transformer.apply_transform(block_iter, ctx):
            block_meta = BlockAccessor.for_block(block).get_metadata()
            block_schema = BlockAccessor.for_block(block).schema()

            # For Write operators, use actual written rows/bytes from context
            if "_write_stats_num_rows" in ctx.kwargs:
                block_meta = replace(
                    block_meta,
                    num_rows=ctx.kwargs.pop("_write_stats_num_rows"),
                    size_bytes=ctx.kwargs.pop("_write_stats_size_bytes"),
                )

            # 步骤3：yield 输出 block + metadata
            exec_stats = stats.build()
            stats: StreamingGeneratorStats = yield block          # ← yield 数据
            if stats:
                exec_stats.block_ser_time_s = stats.object_creation_dur_s

            exec_stats.udf_time_s = map_transformer.udf_time_s(reset=True)
            exec_stats.task_idx = ctx.task_idx
            exec_stats.max_uss_bytes = profiler.estimate_max_uss()

            yield BlockMetadataWithSchema(                         # ← yield 元数据
                metadata=replace(block_meta, exec_stats=exec_stats),
                schema=block_schema,
            )

    TaskContext.reset_current()
```

**`_map_task` 在 Worker 端的执行流程**：

1. **反序列化**：Ray 自动从 Object Store 解引用 `map_transformer_ref`、`data_context_ref`、各 `block_ref`，得到 Python 对象
2. **设置上下文**：`DataContext._set_current(data_context)` 和 `TaskContext.set_current(ctx)` 为 Worker 端设置全局上下文
3. **切片读取**：`_iter_sliced_blocks(blocks, slices)` 根据 `BlockSlice` 元数据对 block 做零拷贝切片（`copy=False`）
4. **执行变换链**：`map_transformer.apply_transform(block_iter, ctx)` 依次执行 `BatchMapTransformFn.__call__` → `_pre_process`（Batcher 切分）→ `_apply_transform`（调 UDF）→ `_post_process`（BlockOutputBuffer 重组）
5. **Streaming yield**：每产出一个 output block 就 `yield` 一次，紧跟一个 `BlockMetadataWithSchema`。Driver 端增量消费，不需要等整个 task 完成

**`_map_task` 本身不做任何数据变换**，它只是：
1. 解包参数
2. 设置上下文
3. 根据 slice 元数据切片
4. 调用 `map_transformer.apply_transform()`
5. 将输出 block 以 streaming generator 形式 yield 出去

### 13.4 Driver 端与 Worker 端的职责分离

| 组件 | 运行位置 | 职责 |
|------|---------|------|
| `BlockRefBundler` | Driver | 攒批：决定哪些 block 打包成一个 task |
| `_try_schedule_task` | Driver | 提交 Ray task：构造参数、设置资源、调用 `.remote()` |
| `_submit_data_task` | Driver | 管理 streaming generator：消费 output block、更新 metrics |
| `_map_task` | Worker | 执行壳：解包参数、设置上下文、切片、调用 transformer、yield 输出 |
| `MapTransformer` | Worker | 变换链：`_pre_process` → `_apply_transform` → `_post_process` |
| `Batcher` | Worker | 输入端：按 `batch_size` 跨 block 切分 batch |
| UDF (`fn`) | Worker | 用户函数：对每个 batch 执行实际计算 |
| `BlockOutputBuffer` | Worker | 输出端：按 `target_max_block_size` 重组 output block |
| `DelegatingBlockBuilder` | Worker | 物理拼接：`add_block`/`add_batch` + `build()` 合并数据 |

### 13.5 完整链路时序

```
┌─ Driver 端 ─────────────────────────────────────────────────────────────────┐
│                                                                             │
│  ① 上游 operator 产出 RefBundle (含 block ObjectRefs)                        │
│     ↓                                                                       │
│  ② _add_input_inner(refs)                                                   │
│     ↓                                                                       │
│  ③ BlockRefBundler.add_bundle(refs)                                         │
│     ↓                                                                       │
│  ④ BlockRefBundler.has_bundle()? → False: 等待; True: 继续                    │
│     ↓                                                                       │
│  ⑤ BlockRefBundler.get_next_bundle() → (input_refs, bundled_input)           │
│     ↓                                                                       │
│  ⑥ _try_schedule_task(bundled_input)                                        │
│     ├─ 构造 TaskContext                                                      │
│     ├─ _get_dynamic_ray_remote_args() → 资源/调度参数                          │
│     ├─ _map_task.options(...).remote(                                         │
│     │     map_transformer_ref,   ← ray.put(MapTransformer) 的 ObjectRef      │
│     │     data_context_ref,       ← ray.put(DataContext) 的 ObjectRef         │
│     │     ctx,                    ← TaskContext Python 对象                     │
│     │     *bundle.block_refs,     ← 数据 block 的 ObjectRefs                   │
│     │     slices=bundle.slices,   ← 切片元数据                                  │
│     │  )                                                                     │
│     ↓                                                                       │
│  ⑦ _submit_data_task(gen, bundle) → 管理 streaming generator                 │
│     ├─ gen 是 ObjectRefGenerator (num_returns="streaming")                   │
│     ├─ 增量消费 gen 的输出 block                                              │
│     └─ 每个 block 后跟一个 BlockMetadataWithSchema                             │
│                                                                             │
└──────────────────────────┬──────────────────────────────────────────────────┘
                           │ Ray 调度到远端 Worker
                           ▼
┌─ Worker 端 ──────────────────────────────────────────────────────────────────┐
│                                                                             │
│  ① Ray 从 Object Store 解引用所有参数：                                        │
│     ray.get(map_transformer_ref) → MapTransformer 对象                        │
│     ray.get(data_context_ref)   → DataContext 对象                           │
│     ray.get(block_ref_1)        → Block (Arrow Table)                         │
│     ray.get(block_ref_2)        → Block (Arrow Table)                         │
│     ...                                                                      │
│                                                                             │
│  ② DataContext._set_current(data_context)                                    │
│     TaskContext.set_current(ctx)                                             │
│                                                                             │
│  ③ block_iter = iter(blocks) 或 _iter_sliced_blocks(blocks, slices)          │
│     ↓                                                                       │
│  ④ map_transformer.apply_transform(block_iter, ctx):                         │
│     │                                                                       │
│     ├─ _pre_process(block_iter)  → batch_blocks → Batcher                    │
│     │  Batcher(batch_size=N):                                                │
│     │    add(block_1) → add(block_2) → ... → 凑够 N 行                       │
│     │    next_batch() → batch_1 (N 行)                                       │
│     │    next_batch() → batch_2 (N 行)                                       │
│     │    ...                                                                │
│     │                                                                       │
│     ├─ _apply_transform(ctx, batches) → fn(batch_1) → result_1               │
│     │                                    → fn(batch_2) → result_2            │
│     │                                    → ...                              │
│     │                                                                       │
│     └─ _post_process(results) → _shape_blocks → BlockOutputBuffer           │
│        BlockOutputBuffer(target_max_block_size=128MiB):                      │
│          add_batch(result_1) → 累积                                           │
│          add_batch(result_2) → 累积                                           │
│          ... → 超过 128MiB? yield block_1, 清空 buffer                        │
│          finalize() → yield block_last (剩余数据)                              │
│                                                                             │
│  ⑤ for block in apply_transform(...):                                       │
│       yield block                    ← streaming yield 数据                   │
│       yield BlockMetadataWithSchema  ← streaming yield 元数据                 │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 13.6 `_map_task` 与 `MapTransformer` 的关系

`_map_task` 是**远程执行壳**，负责参数解包、切片读取和 streaming yield；`MapTransformer` 是**可序列化的变换逻辑**，负责实际的数据变换和 block shaping。

```
┌───────────────────────────────────────────────────┐
│  Driver 端                                         │
│                                                    │
│  MapTransformer 创建                               │
│    ├── transform_fns: [BatchMapTransformFn, ...]   │
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
│    # 1. map_transformer 从 ObjectRef 反序列化       │
│    #    和 Driver 端创建的是同一个 Python 对象       │
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

### 13.7 `_map_transformer_ref` 的延迟序列化机制

**文件**: `python/ray/data/_internal/execution/operators/map_operator.py:244-257`

```python
@property
def _map_transformer_ref(self):
    """Lazily serialize _map_transformer to object store on first access.

    Deferred until first task submission so that on_start callbacks
    (e.g., on_write_start for Iceberg) can modify the transformer state
    before serialization.
    """
    if self.__map_transformer_ref is None:
        self.__map_transformer_ref = ray.put(self._map_transformer)
        self._warn_large_udf()
    return self.__map_transformer_ref
```

**延迟序列化的原因**：

1. **`on_start` 回调**：某些算子（如 Iceberg Write）在首个 input bundle 到达时需要根据 schema 修改 `MapTransformer` 的状态（如 schema 演化）。如果提前 `ray.put`，后续修改不会被序列化
2. **`fuse()` 追加变换**：`start()` 方法中可能通过 `map_transformer.fuse(split_transformer)` 追加额外变换（如 `additional_split_factor`），必须在修改完成后再序列化
3. **避免无用序列化**：如果算子从未被调度执行（如被 limit 截断），延迟序列化避免了无谓的 `ray.put` 开销

### 13.8 `_map_transformer_ref` vs `bundle.block_refs` — 逻辑 vs 数据

| 维度 | `_map_transformer_ref` | `bundle.block_refs` |
|------|------------------------|---------------------|
| **类型** | `ObjectRef` (单个) | `List[ObjectRef]` (多个) |
| **内容** | `MapTransformer` 对象（UDF + transform 链 + 配置） | 实际数据 block（Arrow Table / pandas DataFrame） |
| **创建方式** | `ray.put(self._map_transformer)` (Driver 端手动 put) | 上游算子产出 block 时自动放入 Object Store |
| **共享性** | 同一算子的所有 task 共享同一个 `ObjectRef` | 每个 task 拿到不同的 `block_refs` |
| **Worker 端** | `ray.get()` 一次得到完整 `MapTransformer` | `ray.get()` 得到实际 Block 数据 |
| **修改** | `on_start` / `fuse()` 可在序列化前修改 | 不可修改（只读数据） |

**本质区别**：`map_transformer_ref` 传递的是**逻辑**（如何处理数据），`block_refs` 传递的是**数据**（要处理什么）。所有 Worker 执行相同的变换逻辑，但处理不同的数据分片。

### 13.9 Task 提交参数完整解析

| 参数 | 类型 | 传递方式 | Worker 端处理 | 说明 |
|------|------|---------|-------------|------|
| `map_transformer_ref` | `ObjectRef` | `ray.put()` 序列化到 Object Store | Ray 自动 `ray.get()` 反序列化 | 变换逻辑（UDF + transform 链） |
| `data_context_ref` | `ObjectRef` | `ray.put()` 序列化到 Object Store | Ray 自动 `ray.get()` 反序列化 | DataContext 配置 |
| `ctx` | `TaskContext` | 直接传递 Python 对象 | 直接使用 | Task 上下文（task_idx、op_name 等） |
| `*block_refs` | `List[ObjectRef]` | 展开为 `*args` | Ray 自动从各 ObjectRef 解引用 | 数据 block |
| `slices` | `List[BlockSlice]` | kwargs 传递 | `_iter_sliced_blocks` 切片 | 切片元数据（StreamingRepartition 场景） |
| `**map_task_kwargs` | `Dict[str, Any]` | kwargs 传递 | `ctx.kwargs.update(kwargs)` | 额外参数 |

**注意**：`ctx`（TaskContext）是 Python 对象直接传递，不是通过 Object Store。Ray 会自动序列化/反序列化普通 Python 对象参数。但对于大对象（如 MapTransformer），使用 `ray.put()` 放入 Object Store 可以避免每次 `.remote()` 时重复序列化（尤其是同一个 transformer 被多个 task 共享时，只需 put 一次）。
