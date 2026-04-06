# Ray Data read_parquet Block 计算逻辑详解

本文档详细介绍 `ray.data.read_parquet()` 中 Block 数量和行数的计算逻辑。

## 目录

- [Block 数量计算](#block-数量计算)
- [Block 行数计算](#block-行数计算)
- [控制 Block 最大行数的方法](#控制-block-最大行数的方法)
- [BlockOutputBuffer 切分机制](#blockoutputbuffer-切分机制)
- [Row Group 与 Block 的关系](#row-group-与-block-的关系)
- [完整流程图](#完整流程图)
- [配置参数汇总表](#配置参数汇总表)

---

## Block 数量计算

### 自动 parallelism 公式

当 `parallelism=-1`（默认值）时，Ray Data 使用 `_autodetect_parallelism()` 函数自动计算：

```python
parallelism = max(
    min(read_op_min_num_blocks, max_reasonable_parallelism),  # 默认 200
    min_safe_parallelism,                                      # 基于最大 block 大小
    avail_cpus * 2,                                            # 可用 CPU 的 2 倍
)
```

**相关参数：**

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `read_op_min_num_blocks` | 200 | 最小读取 block 数量 |
| `target_max_block_size` | 128 MiB | 目标最大 block 大小 |
| `target_min_block_size` | 1 MiB | 目标最小 block 大小 |

**计算逻辑：**

```python
# 基于最大 block 大小的安全并行度
min_safe_parallelism = max(1, int(mem_size / target_max_block_size))

# 基于最小 block 大小的合理并行度
max_reasonable_parallelism = max(1, int(mem_size / target_min_block_size))
```

---

## Block 行数计算

### 核心公式

```
每 Block 行数 = target_max_block_size / avg_row_in_mem_bytes
```

其中：
- `target_max_block_size`：目标最大 block 大小（默认 128 MiB）
- `avg_row_in_mem_bytes`：每行数据在内存中的平均大小

### 采样过程

Ray Data 通过采样来估算 `avg_row_in_mem_bytes`：

```python
# 1. 确定采样文件数量
target_num_samples = ceil(文件总数 * 0.01)  # 采样 1% 的文件

# 2. 限制采样数量在合理范围
target_num_samples = max(min(target_num_samples, 10), 2)  # 最少 2 个，最多 10 个

# 3. 均匀分布采样（避免数据倾斜影响）
pivots = np.linspace(0, len(fragments) - 1, target_num_samples).astype(int)
sampled_fragments = [fragments[idx] for idx in pivots]

# 4. 从每个采样文件读取第一个 Row Group 的前 N 行
batch_size = max(min(SAMPLE_NUM_ROWS, 第一个 Row Group 行数), 1)

# 5. 计算平均每行内存大小
avg_row_in_mem_bytes = sample_batch.nbytes / sample_batch.num_rows
```

### 计算示例

**场景：** 100 个 Parquet 文件，每行平均 1 KB

```
target_max_block_size = 128 MiB = 134,217,728 bytes
avg_row_in_mem_bytes = 1024 bytes

每 Block 行数 = 134,217,728 / 1024 = 131,072 行
```

---

## 控制 Block 最大行数的方法

### 方法 1：使用 override_num_blocks（推荐）

```python
import ray

ds = ray.data.read_parquet(
    "s3://bucket/path/",
    override_num_blocks=1000  # 指定 block 数量
)
```

**效果：** 数据被均匀分成 1000 个 block，每个 block 的行数 ≈ 总行数 / 1000

### 方法 2：调整 target_max_block_size

```python
import ray
from ray.data import DataContext

ctx = DataContext.get_current()
ctx.target_max_block_size = 64 * 1024 * 1024  # 64 MiB

ds = ray.data.read_parquet("s3://bucket/path/")
```

**效果：** 每个 block 的目标大小变为 64 MiB，行数相应减少一半

### 方法 3：使用 repartition

```python
ds = ray.data.read_parquet("s3://bucket/path/")
ds = ds.repartition(500)  # 重新分区为 500 个 block
```

**注意：** 会触发 shuffle 操作，有额外开销

### 方法 4：后处理 - 使用 split_at_indices

```python
ds = ray.data.read_parquet("s3://bucket/path/")

# 假设总共 1M 行，想分成 100 个 block（每个 10000 行）
indices = list(range(10000, 1000000, 10000))
blocks = ds.split_at_indices(indices)
```

---

## BlockOutputBuffer 切分机制

### 1.5 倍阈值逻辑

`BlockOutputBuffer` 使用 1.5 倍阈值来决定是否切分 block，确保最后一个 block 至少有目标大小的一半：

```python
# 源码位置: python/ray/data/_internal/output_buffer.py

MAX_SAFE_BLOCK_SIZE_FACTOR = 1.5

def _exceeded_block_size_slice_limit(self, block: BlockAccessor) -> bool:
    # 只有当 block 超过目标大小的 1.5 倍时才切分
    return (
        self._max_bytes_per_block() is not None
        and block.size_bytes() >= MAX_SAFE_BLOCK_SIZE_FACTOR * self._max_bytes_per_block()
    )
```

### 切分逻辑

当 block 超过 1.5 倍阈值时：

```python
def next(self) -> Block:
    block = self._buffer.build()
    accessor = BlockAccessor.for_block(block)

    if self._exceeded_block_size_slice_limit(accessor):
        # 计算目标行数
        num_bytes_per_row = accessor.size_bytes() / accessor.num_rows()
        target_num_rows = max(1, math.ceil(self._max_bytes_per_block() / num_bytes_per_row))

        # 切分 block
        block = accessor.slice(0, target_num_rows, copy=False)
        block_remainder = accessor.slice(target_num_rows, accessor.num_rows(), copy=False)
```

### 示例

```
target_max_block_size = 128 MiB
实际阈值 = 128 * 1.5 = 192 MiB

场景 1: block = 150 MiB → 不切分（< 192 MiB）
场景 2: block = 200 MiB → 切分为 ~128 MiB + ~72 MiB
```

---

## Row Group 与 Block 的关系

### 概念区分

| 概念 | 层级 | 说明 |
|------|------|------|
| **Row Group** | Parquet 文件内部 | Parquet 的物理存储单位，每个文件包含多个 Row Group |
| **Block** | Ray Data 逻辑 | Ray Data 的处理单位，可能跨多个文件或 Row Group |

### 映射关系

```
┌─────────────────────────────────────────────────────────────┐
│                      Parquet 文件结构                        │
├─────────────────────────────────────────────────────────────┤
│  File 1                    File 2                           │
│  ┌─────────────────────┐   ┌─────────────────────┐          │
│  │ Row Group 0         │   │ Row Group 0         │          │
│  │ Row Group 1         │   │ Row Group 1         │          │
│  │ Row Group 2         │   │ Row Group 2         │          │
│  └─────────────────────┘   └─────────────────────┘          │
└─────────────────────────────────────────────────────────────┘
                              ↓
                        Ray Data 读取
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                      Ray Data Blocks                         │
├─────────────────────────────────────────────────────────────┤
│  Block 0: File1.RG0 + File1.RG1 部分                        │
│  Block 1: File1.RG1 剩余 + File1.RG2                        │
│  Block 2: File2.RG0 + File2.RG1 部分                        │
│  Block 3: File2.RG1 剩余 + File2.RG2                        │
└─────────────────────────────────────────────────────────────┘
```

**关键点：**
- Row Group 是 Parquet 的读取单位，但 Block 按内存大小切分
- 一个 Block 可能包含多个 Row Group 的数据
- 一个 Row Group 也可能被切分到多个 Block

---

## 完整流程图

```
┌────────────────────────────────────────────────────────────────────┐
│                    read_parquet() 调用                              │
└────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────┐
│  1. 文件发现阶段                                                    │
│     - 扫描路径，获取所有 Parquet 文件列表                            │
│     - 解析每个文件的元数据（Row Group 信息）                         │
└────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────┐
│  2. 采样估算阶段                                                    │
│     - 采样 1% 文件（最少 2 个，最多 10 个）                          │
│     - 读取每个采样文件的第一个 Row Group 前 N 行                     │
│     - 计算 avg_row_in_mem_bytes（每行平均内存大小）                  │
│     - 计算 encoding_ratio（编码压缩比）                              │
└────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────┐
│  3. Parallelism 计算阶段                                            │
│     - 估算总内存大小: mem_size = 文件大小 * encoding_ratio          │
│     - 计算 min_safe_parallelism = mem_size / target_max_block_size │
│     - 计算最终 parallelism = max(default, safe, cpus * 2)          │
└────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────┐
│  4. 读取任务生成阶段                                                 │
│     - 将文件分配给 parallelism 个读取任务                            │
│     - 每个任务负责读取一组文件/Row Group                             │
└────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────┐
│  5. Block 输出阶段                                                   │
│     - BlockOutputBuffer 按 target_max_block_size 切分数据           │
│     - 使用 1.5x 阈值决定是否切分                                     │
│     - 生成最终的 Block 序列                                          │
└────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────┐
│  6. 后续处理                                                         │
│     - map_batches / filter / 其他转换操作                            │
│     - 每个 Block 独立并行处理                                        │
└────────────────────────────────────────────────────────────────────┘
```

---

## 配置参数汇总表

| 参数 | 默认值 | 位置 | 说明 |
|------|--------|------|------|
| `target_max_block_size` | 128 MiB | DataContext | 目标最大 block 大小 |
| `target_min_block_size` | 1 MiB | DataContext | 目标最小 block 大小 |
| `read_op_min_num_blocks` | 200 | DataContext | 读取操作最小 block 数 |
| `override_num_blocks` | None | read_parquet() | 覆盖自动计算的 block 数 |
| `MAX_SAFE_BLOCK_SIZE_FACTOR` | 1.5 | context.py | block 切分阈值因子 |
| `PARQUET_ENCODING_RATIO_ESTIMATE_SAMPLING_RATIO` | 0.01 | parquet_datasource.py | 采样文件比例 |
| `PARQUET_ENCODING_RATIO_ESTIMATE_MIN_NUM_SAMPLES` | 2 | parquet_datasource.py | 最小采样文件数 |
| `PARQUET_ENCODING_RATIO_ESTIMATE_MAX_NUM_SAMPLES` | 10 | parquet_datasource.py | 最大采样文件数 |

### 配置示例

```python
import ray
from ray.data import DataContext

# 获取当前上下文
ctx = DataContext.get_current()

# 查看当前配置
print(f"target_max_block_size: {ctx.target_max_block_size / 1024 / 1024} MiB")
print(f"target_min_block_size: {ctx.target_min_block_size / 1024 / 1024} MiB")
print(f"read_op_min_num_blocks: {ctx.read_op_min_num_blocks}")

# 修改配置
ctx.target_max_block_size = 64 * 1024 * 1024  # 64 MiB
ctx.read_op_min_num_blocks = 100

# 读取数据
ds = ray.data.read_parquet("path/to/data/")
```

---

## 参考源码位置

- Block 数量计算: `python/ray/data/_internal/util.py` - `_autodetect_parallelism()`
- Block 切分逻辑: `python/ray/data/_internal/output_buffer.py` - `BlockOutputBuffer`
- Parquet 采样: `python/ray/data/_internal/datasource/parquet_datasource.py` - `_sample_fragments()`
- Parallelism 规则: `python/ray/data/_internal/logical/rules/set_read_parallelism.py`
- 默认配置: `python/ray/data/context.py` - `DataContext`
