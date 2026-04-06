# PR #60294 分析：加速 Checkpoint Filter 并减少内存使用

> PR: [https://github.com/ray-project/ray/pull/60294](https://github.com/ray-project/ray/pull/60294)
> 标题: [Data] Speed up checkpoint filter and reduce memory usage
> 作者: wxwmd

---

## 一、Issue 核心问题

PR 解决了两个关键性能问题：

### 问题 1：Arrow→Numpy 转换重复执行

每个 ReadTask 都会将 Arrow 类型的 checkpoint_id 数组拷贝并转换为 Numpy 数组。这个转换是最耗时的操作，却在每个 ReadTask 中重复执行。

### 问题 2：内存冗余

每个 ReadTask 都持有一份 checkpoint_id 数组的副本，导致集群内存使用极高。

测试数据：1亿个 string ID，Arrow 存储 ~2GB，但转成 Numpy 后膨胀到 ~10GB。如果有 1000 个节点，仅 checkpoint 就消耗 ~10TB 内存，导致 OOM。

### 性能测试结果

- 测试设置：10,000 个 parquet 文件，每个 100,000 个 ID（共 100M），checkpoint 有 80M IDs，过滤出 20M 剩余
- 节点：16 核，64GB 内存

| 版本 | 耗时 |
|------|------|
| 原始 ray 2.54.0 | 680s |
| 优化版本 | 190s |
| 加速比 | **3.6x** |

内存方面：原始版本 OOM，优化版本通过。

---

## 二、Arrow → Numpy 转换发生在哪里

在当前代码库中，Arrow→Numpy 转换发生在以下位置：

### 核心位置：`filter_rows_for_block()`（二分搜索路径）

文件：`python/ray/data/checkpoint/checkpoint_filter.py`，约第 475-510 行

```python
# 输入 block 的 ID 列转 numpy
block_ids = block[self.id_column].to_numpy()

# checkpoint 的每个 chunk 转 numpy
def filter_with_ckpt_chunk(ckpt_chunk: pyarrow.ChunkedArray) -> numpy.ndarray:
    try:
        ckpt_ids = transform_pyarrow.to_numpy(ckpt_chunk, zero_copy_only=True)
    except (pa.ArrowInvalid, ValueError):
        ckpt_ids = transform_pyarrow.to_numpy(ckpt_chunk, zero_copy_only=False)
```

### 其他转换位置

- **Roaring Bitmap 路径**：`_build_bitmap_from_checkpoint_table()` 中 `chunk.to_numpy(zero_copy_only=True)`，以及 `filter_rows_for_block_with_raoring_bitmap()` 中 `block[self.id_column].to_numpy()`
- **Redis 路径**：`filter_block_by_redis_ckpt()` 中 `block[self.id_column].to_numpy().tolist()`
- **Bloom Filter 路径**：`bloom_filter.py` 的 `_hash_id_column_to_uint64()` 中 `chunk.to_numpy(zero_copy_only=False)`

### 关键点

在原始设计中，checkpoint 的 Arrow Table 作为 `ObjectRef` 传递给每个 ReadTask，然后每个 ReadTask 在过滤时调用 `to_numpy()` 进行转换。这意味着如果有 N 个 ReadTask，Arrow→Numpy 转换就执行 N 次，且每个 task 都持有一份 Numpy 副本。

PR 的解决方案是引入一个专门的 **ActorPool**，在其中只做一次 Arrow→Numpy 转换，然后由这些 actor 负责所有 block 的过滤操作。

---

## 三、`load_checkpoint` 中的关键代码逻辑

```python
ref_bundles: List[RefBundle] = list(checkpoint_ds.iter_internal_ref_bundles())
assert len(ref_bundles) == 1
ref_bundle: RefBundle = ref_bundles[0]
schema: Schema = ref_bundle.schema
assert len(ref_bundle.blocks) == 1
block_ref: ObjectRef[Block] = ref_bundle.blocks[0][0]
metadata: BlockMetadata = ref_bundle.blocks[0][1]

# Post-process the block
checkpoint_block_ref: ObjectRef[Block] = self._postprocess_block(block_ref)
```

这段代码位于 `CheckpointLoader.load_checkpoint()` 方法中（`checkpoint_filter.py`，约第 310-325 行），作用是**从已 repartition 为 1 块的 checkpoint dataset 中提取出最终的 block 引用，并进行后处理**。逐步解析：

1. **`checkpoint_ds.iter_internal_ref_bundles()`**：执行 checkpoint 数据集的整个 pipeline（读取 parquet → 预处理排序 → repartition），返回 `RefBundle` 迭代器。`list()` 将其物化为列表。
2. **`assert len(ref_bundles) == 1`**：验证确实只有 1 个 RefBundle（因为前面调用了 `repartition(num_blocks=1)`）。
3. **`ref_bundle = ref_bundles[0]`**：取出这唯一的 RefBundle。`RefBundle` 是 Ray Data 的核心数据结构，包含一组 block 引用及其元数据。
4. **`schema = ref_bundle.schema`**：获取 checkpoint 数据的 schema（列名和类型信息），后续用于验证。
5. **`assert len(ref_bundle.blocks) == 1`**：验证 RefBundle 中只有 1 个 block（repartition 应该保证了这一点）。
6. **`block_ref = ref_bundle.blocks[0][0]`**：取出 block 的 `ObjectRef`。`blocks` 是 `Tuple[Tuple[ObjectRef[Block], BlockMetadata], ...]`，所以 `[0][0]` 是 ObjectRef，`[0][1]` 是 metadata。
7. **`metadata = ref_bundle.blocks[0][1]`**：取出 block 的元数据（大小、行数等），用于日志和验证。
8. **`self._postprocess_block(block_ref)`**：这是最关键的一步。调用 `_combine_chunks.remote(block_ref)`，即提交一个 Ray remote task。

---

## 四、`_postprocess_block` 和 `_combine_chunks` 的作用

`_postprocess_block` 委托给 `_combine_chunks`：

```python
def _postprocess_block(self, block_ref: ObjectRef[Block]) -> ObjectRef[Block]:
    """Combine the block so it has fewer chunks."""
    return _combine_chunks.remote(block_ref)

@ray.remote(max_retries=-1)
def _combine_chunks(ckpt_block: pyarrow.Table) -> pyarrow.Table:
    """Combine chunks for the checkpoint block."""
    from ray.data._internal.arrow_ops.transform_pyarrow import combine_chunks
    combined_ckpt_block = combine_chunks(ckpt_block)
    return combined_ckpt_block
```

### `combine_chunks` 的实现

文件：`python/ray/data/_internal/arrow_ops/transform_pyarrow.py`，第 1124 行

```python
def combine_chunks(table: "pyarrow.Table", copy: bool = False) -> "pyarrow.Table":
    new_column_values_arrays = []
    for col in table.columns:
        new_column_values_arrays.append(combine_chunked_array(col, copy))
    return pyarrow.Table.from_arrays(new_column_values_arrays, schema=table.schema)
```

对每一列的 `ChunkedArray`，将多个不连续的 chunk 合并为一个（或少数几个）连续的 `Array`。

### `combine_chunked_array` 的四个分支

```python
def combine_chunked_array(array, ensure_copy=False):
    if _is_pa_extension_type(array.type):
        return _concatenate_extension_column(array, ensure_copy)  # 扩展类型
    elif len(array.chunks) == 0:
        return pa.array([], type=array.type)                       # 空数组
    elif len(array.chunks) == 1 and not ensure_copy:
        return array                                               # 单chunk直接返回
    else:
        return _try_combine_chunks_safe(array)                     # 多chunk安全合并
```

### `_try_combine_chunks_safe` 的安全处理

处理超过 2GB 的数组时，Arrow 的 int32 偏移量会溢出。此方法将 chunk 分组为每组 < 2GB，避免溢出：

```python
def _try_combine_chunks_safe(array):
    # 安全合并的情况：
    # - 非变宽类型 (非 string/binary/list)
    # - large 变宽类型 (使用 int64 偏移)
    # - 总大小 < INT32_MAX (2GB)
    if (not any(p(array.type) for p in _VARIABLE_WIDTH_INT32_OFFSET_PA_TYPE_PREDICATES)
        or any(p(array.type) for p in _VARIABLE_WIDTH_INT64_OFFSET_PA_TYPE_PREDICATES)
        or array.nbytes < INT32_MAX):
        return array.combine_chunks()  # 直接合并为单个 Array

    # 不安全的情况: 变宽类型 + int32偏移 + >2GB
    # 分组合并，每组 < 2GB
    new_chunks = []
    cur_chunk_group = []
    cur_chunk_group_size = 0
    for chunk in array.chunks:
        chunk_size = chunk.nbytes
        if cur_chunk_group_size + chunk_size > INT32_MAX:
            if cur_chunk_group:
                new_chunks.append(pa.concat_arrays(cur_chunk_group))
            cur_chunk_group = []
            cur_chunk_group_size = 0
        cur_chunk_group.append(chunk)
        cur_chunk_group_size += chunk_size
    if cur_chunk_group:
        new_chunks.append(pa.concat_arrays(cur_chunk_group))
    return pa.chunked_array(new_chunks)  # 返回 ChunkedArray (chunk数减少)
```

---

## 五、Chunk 与 Block 的关系

### Block（Ray Data 概念）

Block 是 Ray Data 的**数据传输和执行单元**。一个 `Block` 在这里就是一个 `pyarrow.Table`，包含若干行数据。

在 checkpoint 加载流程中：
- checkpoint 由多个 parquet 文件组成
- 读取后每个 parquet 文件可能产生一个或多个 block
- `repartition(num_blocks=1)` 将所有 block 合并为 1 个 block
- 这个 block 作为 `ObjectRef[Block]` 在 Ray object store 中传递

### Chunk（PyArrow 概念）

Chunk 是 PyArrow `ChunkedArray` 内部的**内存分段**。一个 `pyarrow.Table` 的每一列都是 `ChunkedArray`，由多个 `pa.Array` chunk 组成。

```
pyarrow.Table (1个Block)
├── column "id": ChunkedArray
│   ├── chunk[0]: pa.Array  ← 10万行 (来自parquet文件1)
│   ├── chunk[1]: pa.Array  ← 10万行 (来自parquet文件2)
│   ├── chunk[2]: pa.Array  ← 10万行 (来自parquet文件3)
│   └── ...
└── column "value": ChunkedArray
    ├── chunk[0]: pa.Array
    ├── chunk[1]: pa.Array
    └── ...
```

**chunk 是怎么产生的？** 当 Ray Data 从多个 parquet 文件读取数据时，每个 parquet 文件被解析为一个 Arrow chunk，然后这些 chunk 被拼接到同一个 `ChunkedArray` 中，不做数据拷贝——只是引用的拼接。所以读取 1000 个 parquet 文件后，每一列都是一个包含 1000 个 chunk 的 `ChunkedArray`。

### `combine_chunks` 的作用

```
合并前: ChunkedArray [chunk0, chunk1, chunk2, ..., chunk999]  ← 内存不连续
                            ↓ combine_chunks
合并后: Array (单块连续内存)                                     ← 内存连续
```

---

## 六、单个 Chunk 内部的内存布局

**单个 `pa.Array` chunk 内部是连续的**。但多个 chunk 之间不连续。

`pa.Array` 的内存布局由若干个连续的 buffer 组成：

### int64 类型的单个 chunk

```
┌─────────────────────────────────┐
│ validity bitmap buffer (连续)    │  ← 每个值 1 bit，标记是否为 null
├─────────────────────────────────┤
│ data buffer (连续)              │  ← N × 8 bytes，int64 值紧密排列
└─────────────────────────────────┘
```

### string 类型的单个 chunk

```
┌─────────────────────────────────┐
│ validity bitmap buffer (连续)    │
├─────────────────────────────────┤
│ offsets buffer (连续)            │  ← (N+1) × 4 bytes (int32)，记录每个字符串的起止位置
├─────────────────────────────────┤
│ values buffer (连续)             │  ← 所有字符串的实际字节内容，紧密拼接
└─────────────────────────────────┘
```

### ChunkedArray 是多个 chunk 的引用拼接

```
ChunkedArray (3个chunk):
chunk[0] → [独立内存块A] (连续)
chunk[1] → [独立内存块B] (连续，但与A不相邻)
chunk[2] → [独立内存块C] (连续，但与A/B都不相邻)
```

`ChunkedArray` 不做数据拷贝，只是把多个 chunk 的指针组合在一起。

---

## 七、Arrow → Numpy 为什么耗时且内存膨胀

### 7.1 为什么耗时

核心原因是 **ChunkedArray 无法实现零拷贝转换**。

Numpy `ndarray` 要求**内存连续（contiguous）**。当 Arrow 数据有多个 chunk 时，chunk 之间不连续，必须：
1. **分配一块新的连续内存**（大小 = 整列数据总大小）
2. **逐 chunk 拷贝数据**到新内存中

如果字符串类型，还需要额外为每个字符串创建 Python 对象。

在原始设计中，这个转换在每个 ReadTask 中都执行一次。如果有 1000 个 ReadTask，就执行 1000 次，每次都要分配连续内存 + 逐 chunk 拷贝。

### 7.2 为什么字符串内存膨胀 ~5 倍

Arrow 的 string 类型采用**紧凑的列式存储**：
- 一个 `int32` offset 数组，记录每个字符串的起止位置
- 一个连续的 `bytes` buffer，存储所有字符串的实际内容
- 没有额外的 Python 对象开销

而 Numpy 对字符串只能用 `object` dtype，每个元素都是一个**独立的 Python `str` 对象**，每个对象有 CPython 的对象头（~49 bytes overhead）+ 字符串内容。

以 1 亿个平均 10 字节的字符串为例：

| | 计算方式 | 大小 |
|---|---|---|
| **Arrow** | offsets: N×4=400MB + values: N×10=1000MB | **~1.4 GB** |
| **Numpy** | array pointers: N×8=800MB + N个Python对象: N×(49+10)=5.9GB | **~6.7 GB** |

差异来源：

```
Arrow string 单个元素开销:
  offsets中占4字节 + values中占实际长度 ≈ 4 + 10 = 14 bytes

Numpy object单个元素开销:
  array中指针8字节 + Python对象头49字节 + 字符串内容10字节 ≈ 67 bytes

膨胀比: 67 / 14 ≈ 4.8x
```

PR 作者的 demo：

```python
import sys
import numpy as np
N = 10_000_000
arr = np.array([f"text_{i}" for i in range(N)])
mem = arr.nbytes + sum(sys.getsizeof(s) for s in arr)
print(f"the 1kw arr costs {mem / 1024**3}GB, array of size 10000_0000 will costs {10 * mem / 1024**3}GB")
# 1千万个字符串 → 约 1GB
# 1亿个字符串 → 约 10GB
```

---

## 八、零拷贝 vs 拷贝的判断标准

零拷贝的本质是：**Numpy ndarray 能否直接引用 Arrow 的同一块内存，不需要任何格式转换**。

### 零拷贝的条件（必须同时满足）

1. **内存连续** — 单 chunk（或 combine_chunks 后）
2. **无 null** — 不需要处理 validity bitmap
3. **格式相同** — Arrow 的 data buffer 布局 == Numpy 的 data buffer 布局

### 完整判断表

| 场景 | `zero_copy_only=True` | `zero_copy_only=False` |
|------|----------------------|----------------------|
| **单 chunk + 定宽类型(int64等) + 无null** | ✅ 零拷贝 | ✅ 零拷贝 |
| **单 chunk + 定宽类型 + 有null** | ❌ 抛异常 | 拷贝（需要合并 mask） |
| **单 chunk + string类型** | ❌ 抛异常 | 拷贝（格式不同） |
| **多 chunk + 任何类型** | ❌ 抛异常 | 拷贝（chunk间不连续） |

### 为什么违反任何一个条件就必须拷贝

#### 条件 1：内存连续

```
Arrow 多 chunk:
  chunk[0]: [v0, v1, v2]  地址 0x1000
  chunk[1]: [v3, v4, v5]  地址 0x5000   ← 中间有 gap
  chunk[2]: [v6, v7, v8]  地址 0x9000   ← 中间有 gap

Numpy ndarray 要求: 一个指向连续内存起始地址的指针 + shape/stride
  data ptr → [v0,v1,v2,v3,v4,v5,v6,v7,v8]  必须一块连续内存
```

Numpy 底层就是一个 `void*` 指针指向一块连续内存，没有能力跳到多个不连续地址去取数据。所以多 chunk 必须分配新内存、逐 chunk 逐字节拷贝过去。

#### 条件 2：无 null

```
Arrow int64 with null:
  bitmap:   [1, 0, 1, 1, 0]        ← bit 0 = null
  data:     [5, ??, 7, 8, ??]      ← null位置的data buffer是垃圾值

Numpy int64:
  data:     [5, 0,  7, 8, 0 ]      ← null位置必须填入有效值
```

Arrow 的 null 信息存在 bitmap 里，data buffer 里对应位置是**垃圾值**。Numpy 没有独立 bitmap，每个位置必须有有效值。两者内存不一致，无法共享。

#### 条件 3：格式相同（最关键的区别）

**int64 — 格式完全一致，可以共享内存：**
```
Arrow int64 data buffer:   00 05 00 00 00 00 00 00 | 0A 00 00 00 00 00 00 00 | ...
                           ← v0=5 (小端int64)    →   ← v1=10                  →

Numpy int64 ndarray:       00 05 00 00 00 00 00 00 | 0A 00 00 00 00 00 00 00 | ...
                           ← 完全相同的字节流 →

→ 逐字节一致，Numpy 直接指向 Arrow 的 data buffer 即可，零拷贝
```

**string — 格式完全不同，不可能共享内存：**
```
Arrow string (3个值: "hi", "world", "ok"):
  offsets:  [0, 2, 7, 9]                    ← 4个int32 = 16 bytes
  values:   [h, i, w, o, r, l, d, o, k]     ← 9 bytes连续字节流
  bitmap:   [1, 1, 1]                        ← 1 byte
  总内存: 16 + 9 + 1 = 26 bytes

Numpy object array (3个值: "hi", "world", "ok"):
  data:     [ptr0, ptr1, ptr2]              ← 3个指针 = 24 bytes
               ↓        ↓        ↓
            PyObject  PyObject  PyObject     ← 每个对象: 49字节头 + 字符串内容
            "hi"(51B) "world"(54B) "ok"(51B)
  总内存: 24 + 51 + 54 + 51 = 180 bytes
```

两种格式在**字节层面完全不同**：

| | Arrow string | Numpy object |
|---|---|---|
| 值的定位方式 | offset 数组定位 + values 连续字节 | 指针数组 → 每个指向独立 Python 对象 |
| 每个字符串额外开销 | offsets 中 4 bytes | Python 对象头 49 bytes |
| null 表示 | bitmap 中 1 bit | 需要额外 mask 或填 None 对象 |
| 内存结构 | 3 段连续 buffer | 1 段指针 + N 个散落的堆对象 |

没有任何一段 Arrow buffer 可以被 Numpy 直接复用，所以**必须分配全新内存 + 逐元素创建 Python 对象 + 拷贝字符串内容**。

---

## 九、int/float 定宽类型的详细分析

### 内存布局对比（以 1 亿个 int64 为例，无 null）

```
Arrow int64 单chunk:
  validity bitmap: [11111111...11111111]  N/8 bytes = 12.5MB  (全1, 无null)
  data buffer:     [v0|v1|v2|...|vN]      N×8 bytes = 800MB    (连续int64)
  总计: ~812.5MB

Numpy int64 ndarray:
  data:            [v0|v1|v2|...|vN]       N×8 bytes = 800MB    (连续int64)
  总计: 800MB
```

两者的 data buffer 内存布局**完全一致**，都是 N×8 字节连续排列的小端序整数。所以单 chunk + 无 null 时，`to_numpy(zero_copy_only=True)` 可以直接返回指向同一块内存的 ndarray，**零拷贝、零膨胀**。

### 有 null 的情况

Arrow 用 **validity bitmap** 表示 null（每值 1 bit），null 位置的 data buffer 值是未定义的垃圾值。Numpy 没有 null 的概念，`int64` ndarray 中每个位置都必须有有效值。

```
Arrow:
  bitmap:   [1, 0, 1, 1, 0, ...]     ← 0表示null
  data:     [5, ?, 7, 8, ?, ...]     ← ?是垃圾值

Numpy需要:
  data:     [5, 0, 7, 8, 0, ...]      ← null位置需要填充某个值(如0或NaN)
```

Arrow 的 null 信息存在 bitmap 里，data buffer 里对应位置是**垃圾值**。Numpy 没有独立 bitmap，每个位置必须有有效值。两者内存不一致，无法共享。

### 多 chunk 的情况

```
ChunkedArray (3个chunk, int64):
  chunk[0] → [v0, v1, v2]    内存地址 0x1000
  chunk[1] → [v3, v4, v5]    内存地址 0x5000  (不连续!)
  chunk[2] → [v6, v7, v8]    内存地址 0x9000  (不连续!)

to_numpy() 需要:
  ndarray →  [v0,v1,v2,v3,v4,v5,v6,v7,v8]  一块连续内存
                    ↑ 必须分配新内存 + 逐chunk拷贝
```

**这就是 `combine_chunks` 对定宽类型的关键价值**：合并后变成单 chunk 连续内存，就有可能零拷贝了。

### combine_chunks 对定宽类型的效果

```
合并前 (多chunk):
  ChunkedArray → 3块不连续内存
  to_numpy(zero_copy_only=True) → ❌ 抛异常
  to_numpy(zero_copy_only=False) → 拷贝 (分配800MB新内存 + 逐chunk拷贝)

合并后 (combine_chunks):
  Array → 1块连续内存
  to_numpy(zero_copy_only=True) → ✅ 零拷贝 (无null时)
  to_numpy(zero_copy_only=False) → ✅ 零拷贝 (无null时)
```

### 完整对比表

| 条件 | int64/float64 | string |
|------|--------------|--------|
| 多 chunk, 无 null | 拷贝，无膨胀 | 拷贝，膨胀~5x |
| 单 chunk, 无 null | **零拷贝** | 拷贝，膨胀~5x |
| 单 chunk, 有 null | 拷贝，无膨胀 | 拷贝，膨胀~5x |
| 每个 ReadTask 重复执行 | N次拷贝 | N次拷贝 + N份膨胀内存 |

对于 **int64 类型的 ID 列**：combine_chunks 之后实际上可以做到零拷贝。PR 中描述的严重性能问题主要发生在 **string 类型 ID** 的场景下，因为 string 即使合并 chunk 后也必须拷贝且膨胀 ~5 倍。

---

## 十、`to_numpy` 函数实现

文件：`python/ray/data/_internal/arrow_ops/transform_pyarrow.py`，约第 549-590 行

```python
def to_numpy(
    array: Union["pyarrow.Array", "pyarrow.ChunkedArray"],
    *,
    zero_copy_only: bool = True,
) -> np.ndarray:
    """Wrapper for `Array`s and `ChunkedArray`s `to_numpy` API,
    handling API divergence b/w Arrow versions"""

    import pyarrow as pa
    from ray.data._internal.utils.transform_pyarrow import _is_native_tensor_type

    if isinstance(array, pa.Array):
        if pa.types.is_null(array.type):
            return np.full(len(array), np.nan, dtype=np.float32)
        if _is_native_tensor_type(array.type):
            return array.to_numpy_ndarray()  # 零拷贝
        return array.to_numpy(zero_copy_only=zero_copy_only)
    elif isinstance(array, pa.ChunkedArray):
        if pa.types.is_null(array.type):
            return np.full(array.length(), np.nan, dtype=np.float32)
        if _is_native_tensor_type(array.type):
            numpy_chunks = [chunk.to_numpy_ndarray() for chunk in array.chunks]
            if len(numpy_chunks) == 0:
                return np.empty((0,) + tuple(array.type.shape))
            return np.vstack(numpy_chunks)
        if PYARROW_VERSION >= MIN_PYARROW_VERSION_CHUNKED_ARRAY_TO_NUMPY_ZEROCOPY_ONLY:
            return array.to_numpy(zero_copy_only=zero_copy_only)
        else:
            return array.to_numpy()
    else:
        raise ValueError(
            f"Either of `Array` or `ChunkedArray` was expected, got {type(array)}"
        )
```

### 分支说明

**`pa.Array` 分支：**
1. Null 类型：返回 `np.full(len(array), np.nan, dtype=np.float32)`
2. Native tensor 类型：调用 `array.to_numpy_ndarray()`，零拷贝
3. 其他类型：委托 `array.to_numpy(zero_copy_only=zero_copy_only)`

**`pa.ChunkedArray` 分支：**
1. Null 类型：返回 `np.full(array.length(), np.nan, dtype=np.float32)`
2. Native Tensor 类型：逐 chunk 转 numpy，然后 `np.vstack` 拼接
3. 其他类型（新版 pyarrow）：`array.to_numpy(zero_copy_only=zero_copy_only)`
4. 其他类型（旧版 pyarrow）：`array.to_numpy()`（不支持 `zero_copy_only` 参数）

---

## 十一、PR 评论中的关键讨论

### 评论 1 (daiping8, 2026-01-20)

> 两个问题：
> 1. 单个全局 actor 可能成为瓶颈。所有 Read 相关的过滤请求都经过同一个 `BatchBasedCheckpointFilter` actor，可能导致请求排队。
> 2. `checkpointed_ids` 被完全物化为 numpy 数组并常驻内存。如果 checkpoint 很大，actor 进程必须有足够的连续内存来容纳整个 ID 列。
>
> 建议：分片设计（多个 actor，按 `hash(id)` 或 ID 范围分区），支持 "部分加载 + 部分过滤" 模式。

### 评论 2 (wxwmd, 2026-01-21)

> 在我的测试中，过滤请求处理非常快。如果 checkpoint 有 1.15 亿行，每个 block 有 10k+ 行，每个过滤请求可以在 0.2s 内处理完。

### 评论 5 (wxwmd, 2026-02-10)

> 当我第一次解决这个问题时，我和你有同样的想法：只做一次 `Arrow->NumPy` 转换，然后广播那个 NumPy 数组。
>
> 但我实现并测试后发现一个问题：NumPy 数组太大了。
> 例如，我有 1 亿个字符串 ID，Arrow 存储 2GB，但 NumPy 大约需要 ~10GB。
> 让每个 worker 在内存中保持一个 ~10GB 的对象是不可接受的。我们的集群有大约 1000 个节点，这意味着大约 10,000 GB 的内存仅用于 checkpoint。
> 这也是我要解决的第二个问题：冗余内存使用。

### 评论 6 (owenowenisme, 2026-02-10)

> 我认为这是合理的，一个问题是我们应该避免这个 actor 成为瓶颈。你有什么计划来避免吗？另外这可能影响我们的反压？

---

## 十二、总结

### 零拷贝的判断本质

```
Arrow buffer 的字节流 == Numpy data 的字节流 ?
      ↓
  ┌─────────────┬──────────────┐
  │  是         │  否          │
  ↓             ↓              │
  零拷贝       拷贝(格式转换)   │
                               │
  什么情况一致:                 │  什么情况不一致:
  - int64 单chunk 无null       │  - 多chunk (不连续)
  - float64 单chunk 无null     │  - 有null (bitmap vs 无)
  - 任何定宽类型同上            │  - string (offset+values vs 对象指针)
                               │  - 任何变长类型
```

**int64 可以零拷贝不是因为它是"简单类型"，而是因为 Arrow 和 Numpy 在内存中的字节排列恰好完全一致**。string 不能零拷贝不是因为"复杂"，而是因为 Arrow 的列式紧凑存储和 Numpy 的 Python 对象指针数组在**字节层面完全不同**，不存在共享内存的可能。

### combine_chunks 的作用

```
combine_chunks 的作用:
  多个不连续 chunk → 单个连续 pa.Array
       ↓
  对定宽类型: 使 to_numpy() 有可能零拷贝（省去一次内存分配+拷贝）
  对string类型: 合并chunk后仍需拷贝，但至少只做一次而非每task重复做
```

### PR #60294 的核心优化

```
原始: 每个ReadTask都做 to_numpy() → N次拷贝 + N份膨胀后的内存副本
优化: 在专门actor中做1次 to_numpy() → 1次拷贝 + 1份内存副本
     3.6x加速 + 解决OOM
```

### 整体流程图

```
checkpoint_path/
├── file_0.parquet ─→ 读入后产生 Arrow chunk[0]
├── file_1.parquet ─→ 读入后产生 Arrow chunk[1]  
├── file_2.parquet ─→ 读入后产生 Arrow chunk[2]
└── ...
         ↓
  pyarrow.Table (1个 Block)
    column "id": ChunkedArray = [chunk0, chunk1, chunk2, ...]  ← N个chunk
         ↓
  repartition(1) → 仍为1个Block, 但ChunkedArray可能有多个chunk
         ↓
  combine_chunks() → 将N个chunk合并为1个连续Array
         ↓
  to_numpy() → 转换为连续ndarray (此处可能拷贝, 字符串类型必然拷贝且膨胀)
```
