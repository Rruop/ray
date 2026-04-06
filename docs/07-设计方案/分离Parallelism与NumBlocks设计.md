# 设计方案：分离 parallelism（ReadTask 数）与 override_num_blocks（输出 Block 数）

> 基于 Ray 2.52.1 源码分析
> 日期：2026-06-02

---

## 目录

1. [背景与问题](#1-背景与问题)
2. [当前实现的问题详析](#2-当前实现的问题详析)
3. [设计目标](#3-设计目标)
4. [改造方案](#4-改造方案)
5. [API 设计](#5-api-设计)
6. [核心代码改动](#6-核心代码改动)
7. [边界场景处理](#7-边界场景处理)
8. [向后兼容性](#8-向后兼容性)
9. [测试计划](#9-测试计划)
10. [风险评估](#10-风险评估)
11. [替代方案](#11-替代方案)

---

## 1. 背景与问题

### 1.1 当前参数现状

Ray Data 的读取 API（如 `read_parquet`, `read_csv`, `read_json` 等）提供两个参数控制并行度：

| 参数 | 状态 | 文档描述 | 实际效果 |
|------|------|----------|----------|
| `parallelism` | 已废弃（Ray 2.10） | 控制并行度 | 同时影响 ReadTask 数量和目标 block 数量 |
| `override_num_blocks` | 当前推荐 | "Override the number of output blocks from all read tasks" | 实际上与 `parallelism` 完全等价 |

两者的关系（`read_api.py:4522-4533`）：

```python
def _get_num_output_blocks(parallelism=-1, override_num_blocks=None):
    if parallelism != -1:
        warn("deprecated")
    elif override_num_blocks is not None:
        parallelism = override_num_blocks   # 直接赋值，完全等价
    return parallelism
```

### 1.2 核心矛盾

`parallelism` / `override_num_blocks` 这**一个参数**同时控制了两个语义不同的维度：

```
                      ┌─────────────────────────────────────────┐
                      │        当前：一个参数控制两个维度         │
                      │                                         │
  override_num_blocks │─────────────────────────────────────────│
          = N         │  1. ReadTask 数量 = min(N, num_files)   │
                      │  2. 目标 Block 数量 ≈ N                 │
                      │                                         │
                      │  用户无法独立控制这两者！                 │
                      └─────────────────────────────────────────┘
```

**用户期望**：

```
  parallelism = T       →  T 个 ReadTask 并行读取（控制 I/O 并行度）
  override_num_blocks = B  →  最终产出 B 个 Block（控制下游并行度）
```

---

## 2. 当前实现的问题详析

### 2.1 问题一：无法实现"少量 Task、大量 Block"

**场景**：100 个 Parquet 文件，用户想要 10 个 ReadTask 并行读，但最终输出 100 个 Block 给下游。

**当前行为**：

```python
ds = ray.data.read_parquet("s3://...", override_num_blocks=10)

# 结果：
# - 10 个 ReadTask（正确）
# - compute_additional_split_factor 检测到：
#   detected_parallelism = 10（用户指定）
#   num_read_tasks = 10
#   estimated_num_blocks = 10 × size_based_splits
#   如果 size_based_splits = 1 → estimated_num_blocks = 10
#   10 >= 10，不需要额外 split → 最终只有约 10 个 Block（太少！）
```

**问题**：下游 100 个 CPU 只能拿到 10 个 Block，并行度严重不足。

### 2.2 问题二：无法实现"大量 Task、少量 Block"

**场景**：100 个小文件，用户想要 100 个 ReadTask 充分利用集群 I/O 带宽，但最终只需要 10 个 Block（数据量小，Block 太多反而有调度开销）。

**当前行为**：

```python
ds = ray.data.read_parquet("s3://...", override_num_blocks=10)

# 结果：
# - 只有 10 个 ReadTask（错误！用户想要 100 个 Task 利用 I/O）
# - 最终约 10 个 Block
```

### 2.3 问题三：override_num_blocks 的文档与实现不一致

文档说 "Override the number of output blocks"，但实际效果是：
- `get_read_tasks(override_num_blocks)` → 控制 **ReadTask 数量**
- `compute_additional_split_factor` 中 → 作为 **目标 Block 数量** 的参考值
- 当 ReadTask 不足时 → 通过 `_split_blocks` 拆分补偿

用户很难预测最终实际产出多少个 Block。

### 2.4 问题四：compute_additional_split_factor 中语义模糊

```python
# set_read_parallelism.py:23-87
def compute_additional_split_factor(
    datasource_or_legacy_reader,
    parallelism: int,       # ← 这是 "task 数" 还是 "block 数"？
    mem_size: int,
    target_max_block_size,
    ...
):
    detected_parallelism, reason, _ = _autodetect_parallelism(
        parallelism, ...    # ← 这里 parallelism 被当作 "目标并行度"
    )
    num_read_tasks = len(
        datasource_or_legacy_reader.get_read_tasks(detected_parallelism)  # ← 这里被当作 "task 数"
    )
    # ...
    if estimated_num_blocks < detected_parallelism:  # ← 这里又当作 "block 数"
        k = ceil(detected_parallelism / estimated_num_blocks)
```

同一个参数 `parallelism`，在不同上下文中语义不同，逻辑混杂。

---

## 3. 设计目标

1. **语义清晰**：`parallelism` 只控制 ReadTask 数量，`override_num_blocks` 只控制最终 Block 数量
2. **独立可控**：两个参数可以独立设置，互不干扰
3. **向后兼容**：只设置其中一个参数时，行为与现有逻辑一致
4. **自动模式保留**：两个参数都不设置时（`-1`/`None`），自动检测逻辑保持不变
5. **最小改动**：尽量复用现有的 `_split_blocks` 和 `_additional_split_factor` 机制

---

## 4. 改造方案

### 4.1 核心思路

```
  改造前：
    parallelism/override_num_blocks → 同一个值 → 同时影响 Task 数和 Block 数

  改造后：
    parallelism        → 只影响 ReadTask 数量（get_read_tasks 的参数）
    override_num_blocks → 只影响最终 Block 数量（split factor 计算的目标值）
    split_factor = ceil(override_num_blocks / num_read_tasks)
```

### 4.2 参数语义重定义

| 参数 | 新语义 | 默认值 | 说明 |
|------|--------|--------|------|
| `parallelism` | ReadTask 并行数量 | `-1`（自动） | 传递给 `get_read_tasks()`，决定文件分组 |
| `override_num_blocks` | 最终输出 Block 数量 | `None`（自动） | 传递给 `compute_additional_split_factor()`，决定 split factor |

### 4.3 参数组合矩阵

| parallelism | override_num_blocks | 行为 |
|:-----------:|:-------------------:|------|
| -1 | None | 全自动：自动检测 Task 数，自动检测 Block 数（当前默认行为，不变） |
| N | None | 自动 Block 数：N 个 ReadTask，Block 数按 size 自动计算 |
| -1 | B | 自动 Task 数：自动检测 ReadTask 数，目标 B 个 Block |
| N | B | 全手动：N 个 ReadTask，目标 B 个 Block |

---

## 5. API 设计

### 5.1 用户 API 变更

```python
# read_api.py

def read_parquet(
    paths: Union[str, List[str]],
    *,
    parallelism: int = -1,                      # 语义变更：控制 ReadTask 数量
    override_num_blocks: Optional[int] = None,  # 语义明确：控制最终 Block 数量
    concurrency: Optional[int] = None,
    **arrow_parquet_args,
) -> Dataset:
    """
    Args:
        parallelism: The number of read tasks to create. This controls the
            I/O parallelism during the read phase. Set to -1 (default) for
            auto-detection based on cluster resources and data size.

            Note: The actual number of read tasks may be less than this value
            if there are fewer files than the requested parallelism.

        override_num_blocks: Override the number of output blocks from all
            read tasks. This controls the parallelism available to downstream
            operators. If not set, the number of blocks is automatically
            determined based on data size and target_max_block_size.

            When both parallelism and override_num_blocks are set, parallelism
            controls the number of read tasks, and override_num_blocks controls
            the final number of output blocks. The system will split each read
            task's output into ceil(override_num_blocks / parallelism) blocks.

    """
```

同样的文档变更适用于 `read_csv`, `read_json`, `read_datasource`, `from_items` 等。

### 5.2 废弃警告调整

```python
def _get_num_output_blocks(
    parallelism: int = -1,
    override_num_blocks: Optional[int] = None,
) -> Tuple[int, Optional[int]]:
    """Returns (parallelism, override_num_blocks) separately.

    Before: both were merged into a single parallelism value.
    After: they are kept separate for independent control.
    """
    if parallelism != -1 and override_num_blocks is not None:
        logger.warning(
            "Both parallelism and override_num_blocks are set. "
            "parallelism controls the number of read tasks, "
            "override_num_blocks controls the number of output blocks."
        )
    return parallelism, override_num_blocks
```

---

## 6. 核心代码改动

### 6.1 `_get_num_output_blocks` — 参数不再合并

**文件**: `python/ray/data/read_api.py`

```python
# 改造前
def _get_num_output_blocks(parallelism=-1, override_num_blocks=None):
    if parallelism != -1:
        warn("deprecated")
    elif override_num_blocks is not None:
        parallelism = override_num_blocks
    return parallelism

# 改造后
def _get_num_output_blocks(parallelism=-1, override_num_blocks=None):
    """Returns (parallelism, override_num_blocks) separately."""
    if parallelism != -1:
        logger.info(
            "parallelism controls the number of read tasks. "
            "Use override_num_blocks to control output block count."
        )
    return parallelism, override_num_blocks
```

### 6.2 `read_datasource` — 分别传递两个参数

**文件**: `python/ray/data/read_api.py:383-482`

```python
def read_datasource(
    datasource, *,
    parallelism=-1,
    override_num_blocks=None,
    concurrency=None,
    **read_args,
):
    # 改造前
    # parallelism = _get_num_output_blocks(parallelism, override_num_blocks)

    # 改造后
    parallelism, num_blocks = _get_num_output_blocks(parallelism, override_num_blocks)

    ctx = DataContext.get_current()
    datasource_or_legacy_reader = _get_datasource_or_legacy_reader(datasource, ctx, read_args)

    # ReadTask 数量使用 parallelism
    requested_parallelism, _, _ = _autodetect_parallelism(
        parallelism, ctx.target_max_block_size, ctx,
        datasource_or_legacy_reader,
        placement_group=ray.util.get_current_placement_group(),
    )

    read_tasks = datasource_or_legacy_reader.get_read_tasks(requested_parallelism)

    read_op = Read(
        datasource,
        datasource_or_legacy_reader,
        parallelism=parallelism,           # 控制 ReadTask 数量
        override_num_blocks=num_blocks,    # 控制最终 Block 数量（新增）
        num_outputs=len(read_tasks) if read_tasks else 0,
        ray_remote_args=ray_remote_args,
        compute=TaskPoolStrategy(concurrency),
    )
    # ...
```

### 6.3 `Read` 逻辑算子 — 新增 `override_num_blocks` 字段

**文件**: `python/ray/data/_internal/logical/operators/read_operator.py`

```python
class Read(AbstractMap, SourceOperator, ...):
    def __init__(
        self,
        datasource: Datasource,
        datasource_or_legacy_reader: Union[Datasource, Reader],
        parallelism: int,
        override_num_blocks: Optional[int] = None,  # 新增
        num_outputs: Optional[int] = None,
        ray_remote_args=None,
        compute=None,
    ):
        super().__init__(...)
        self.datasource = datasource
        self.datasource_or_legacy_reader = datasource_or_legacy_reader
        self.parallelism = parallelism              # 只控制 ReadTask 数
        self.override_num_blocks = override_num_blocks  # 新增：只控制 Block 数
        self.detected_parallelism = None
```

### 6.4 `compute_additional_split_factor` — 核心逻辑改造

**文件**: `python/ray/data/_internal/logical/rules/set_read_parallelism.py`

```python
def compute_additional_split_factor(
    datasource_or_legacy_reader: Union[Datasource, Reader],
    parallelism: int,                              # ReadTask 数量（语义明确）
    override_num_blocks: Optional[int],            # 目标 Block 数量（新增参数）
    mem_size: int,
    target_max_block_size: Optional[int],
    cur_additional_split_factor: Optional[int] = None,
) -> Tuple[int, str, int, Optional[int]]:
    """Returns (detected_parallelism, reason, estimated_num_blocks, k)."""

    ctx = DataContext.get_current()

    # Step 1: 检测 ReadTask 数量
    # parallelism 控制的是 ReadTask 数，不再混淆为 "block 数"
    if parallelism == -1:
        # 自动检测 ReadTask 数量
        detected_parallelism, reason, _ = _autodetect_parallelism(
            -1, target_max_block_size, ctx,
            datasource_or_legacy_reader, mem_size
        )
    else:
        detected_parallelism = parallelism
        reason = "user-specified"

    # Step 2: 获取实际 ReadTask 数量
    num_read_tasks = len(
        datasource_or_legacy_reader.get_read_tasks(detected_parallelism)
    )

    # Step 3: 计算基于大小的 split
    size_based_splits = 1
    if mem_size and target_max_block_size is not None:
        expected_block_size = mem_size / num_read_tasks
        size_based_splits = round(
            max(1, expected_block_size / target_max_block_size)
        )
        if cur_additional_split_factor:
            size_based_splits *= cur_additional_split_factor

    estimated_num_blocks = num_read_tasks * size_based_splits

    # Step 4: 确定目标 Block 数量
    if override_num_blocks is not None:
        # 用户显式指定了目标 Block 数量
        target_num_blocks = override_num_blocks
    else:
        # 自动：使用 detected_parallelism 作为目标（保持向后兼容）
        target_num_blocks = detected_parallelism

    # Step 5: 确保目标 Block 数不低于 size-based 计算（防 OOM）
    target_num_blocks = max(target_num_blocks, estimated_num_blocks)

    # Step 6: 计算额外 split factor
    if num_read_tasks > 0 and target_num_blocks > estimated_num_blocks:
        k = math.ceil(target_num_blocks / num_read_tasks)
        # k 应该至少覆盖 size_based_splits
        k = max(k, size_based_splits)
        estimated_num_blocks = num_read_tasks * k
        return detected_parallelism, reason, estimated_num_blocks, k

    return detected_parallelism, reason, estimated_num_blocks, None
```

### 6.5 `SetReadParallelismRule._apply` — 传递新参数

```python
class SetReadParallelismRule(Rule):
    def _apply(self, op: PhysicalOperator, logical_op: Read):
        estimated_in_mem_bytes = logical_op.infer_metadata().size_bytes

        (
            detected_parallelism,
            reason,
            estimated_num_blocks,
            k,
        ) = compute_additional_split_factor(
            logical_op.datasource_or_legacy_reader,
            logical_op.parallelism,                  # 只控制 Task 数
            logical_op.override_num_blocks,          # 只控制 Block 数（新增）
            estimated_in_mem_bytes,
            op.target_max_block_size_override or op.data_context.target_max_block_size,
            op._additional_split_factor,
        )

        logical_op.set_detected_parallelism(detected_parallelism)

        if k is not None:
            op.set_additional_split_factor(k)
```

### 6.6 `plan_read_op` — 使用 `detected_parallelism` 获取 ReadTask

**文件**: `python/ray/data/_internal/planner/plan_read_op.py`

此文件无需改动。`get_input_data` 已经使用 `op.get_detected_parallelism()`，
而 `detected_parallelism` 的语义（ReadTask 数量）在改造后更加明确。

### 6.7 改动文件汇总

| 文件 | 改动类型 | 改动说明 |
|------|----------|----------|
| `python/ray/data/read_api.py` | 修改 | `_get_num_output_blocks` 返回两个值；`read_datasource` 分别传递；各 `read_*` 函数文档更新 |
| `python/ray/data/_internal/logical/operators/read_operator.py` | 修改 | `Read.__init__` 增加 `override_num_blocks` 字段 |
| `python/ray/data/_internal/logical/rules/set_read_parallelism.py` | 修改 | `compute_additional_split_factor` 增加 `override_num_blocks` 参数，分离 task/block 计算 |
| `python/ray/data/datasource/datasource.py` | 无改动 | `Datasource.get_read_tasks` 接口不变 |
| `python/ray/data/datasource/file_based_datasource.py` | 无改动 | `get_read_tasks` 实现不变 |
| `python/ray/data/_internal/planner/plan_read_op.py` | 无改动 | 已使用 `detected_parallelism` |
| `python/ray/data/_internal/execution/operators/map_operator.py` | 无改动 | `_split_blocks` 机制不变 |

---

## 7. 边界场景处理

### 7.1 文件数少于 parallelism

```python
ds = ray.data.read_parquet("s3://...", parallelism=100, override_num_blocks=200)

# 只有 50 个文件
# detected_parallelism = 100
# num_read_tasks = min(100, 50) = 50
# target_num_blocks = max(200, 50 * size_based_splits)
# k = ceil(200 / 50) = 4
# 最终: 50 个 ReadTask，每个拆成 4 份 → 200 个 Block
```

### 7.2 override_num_blocks 少于 ReadTask 数

```python
ds = ray.data.read_parquet("s3://...", parallelism=100, override_num_blocks=10)

# 100 个文件
# num_read_tasks = 100
# target_num_blocks = 10
# 但 estimated_num_blocks = 100 * size_based_splits >= 100
# target_num_blocks = max(10, 100) = 100（防 OOM：不能少于 size 估算）
# 不需要额外 split，最终 100 个 Block
#
# 注意：这种情况下 override_num_blocks=10 被忽略，
# 因为 size-based 安全下限更高。应打印 warning。
```

### 7.3 override_num_blocks = -1（自动模式）

```python
ds = ray.data.read_parquet("s3://...", parallelism=10, override_num_blocks=-1)

# 10 个 ReadTask
# target_num_blocks = detected_parallelism = 10（使用 parallelism 作为 fallback）
# 行为与当前 override_num_blocks=None 完全一致
```

### 7.4 parallelism = -1（自动 Task 数）+ override_num_blocks = B

```python
ds = ray.data.read_parquet("s3://...", override_num_blocks=100)

# detected_parallelism = _autodetect_parallelism(-1, ...) → 自动计算
# num_read_tasks = min(detected_parallelism, num_files)
# target_num_blocks = 100
# k = ceil(100 / num_read_tasks) if needed
# 最终: 自动 Task 数，100 个 Block
```

### 7.5 from_items 特殊路径

`from_items` 不使用 `Datasource.get_read_tasks`，而是直接在 API 层分块。
改造时需要在 `from_items` 中也分别处理两个参数：

```python
# read_api.py: from_items
parallelism, override_num_blocks = _get_num_output_blocks(parallelism, override_num_blocks)

# Task 数使用 parallelism
detected_parallelism, _, _ = _autodetect_parallelism(parallelism, ...)
detected_parallelism = min(len(items), detected_parallelism)

# 分块时使用 override_num_blocks（如果指定）
if override_num_blocks is not None and override_num_blocks > 0:
    final_num_blocks = max(override_num_blocks, detected_parallelism)
else:
    final_num_blocks = detected_parallelism
```

---

## 8. 向后兼容性

### 8.1 只设置 parallelism（旧用法）

```python
ds = ray.data.read_parquet("s3://...", parallelism=10)

# 改造后：
#   parallelism = 10 → 10 个 ReadTask
#   override_num_blocks = None → 自动计算 Block 数
# 行为与改造前一致（parallelism 同时作为 task 数和 block 数目标）
```

### 8.2 只设置 override_num_blocks（当前推荐用法）

```python
ds = ray.data.read_parquet("s3://...", override_num_blocks=10)

# 改造前：
#   parallelism = 10 → 10 个 ReadTask，目标 10 个 Block
#
# 改造后（关键差异！）：
#   parallelism = -1 → 自动检测 ReadTask 数
#   override_num_blocks = 10 → 目标 10 个 Block
#
# ⚠️ 行为变更！
# 改造前 override_num_blocks 同时控制了 task 数和 block 数
# 改造后只控制 block 数，task 数回到自动检测
```

**这是一个 Breaking Change！** 需要提供过渡期。

### 8.3 过渡方案

**Option A：渐进式迁移（推荐）**

1. **Ray 2.XX**：新增 `num_read_tasks` 参数，`parallelism` 和 `override_num_blocks` 行为不变
2. **Ray 2.XX+1**：`override_num_blocks` 只控制 Block 数，打印 migration warning
3. **Ray 2.XX+2**：移除 `parallelism`，最终 API 为 `num_read_tasks` + `override_num_blocks`

**Option B：直接引入新参数名**

1. 新增 `num_read_tasks` 参数，语义明确控制 ReadTask 数量
2. `override_num_blocks` 语义变更为只控制 Block 数量
3. `parallelism` 保持废弃但行为不变（向后兼容）
4. 当 `parallelism` 和 `num_read_tasks` 同时设置时报错

```python
def read_parquet(
    paths,
    *,
    num_read_tasks: int = -1,                     # 新增：控制 ReadTask 数量
    override_num_blocks: Optional[int] = None,    # 语义变更：只控制 Block 数量
    parallelism: int = -1,                        # 废弃：兼容旧代码
    ...
):
    if parallelism != -1 and num_read_tasks != -1:
        raise ValueError("Cannot set both parallelism and num_read_tasks")
    if parallelism != -1:
        warn("parallelism is deprecated, use num_read_tasks")
        num_read_tasks = parallelism
    ...
```

### 8.4 推荐方案：Option B

理由：
- 新参数名 `num_read_tasks` 语义清晰，不会与旧参数混淆
- `override_num_blocks` 的语义变更是自然的（文档本来就说是 "output blocks"）
- `parallelism` 的废弃路径已经铺好，无需额外的中间版本
- 避免了 Option A 中 `override_num_blocks` 行为突然变更的 Breaking Change

---

## 9. 测试计划

### 9.1 单元测试

| 测试场景 | 输入 | 预期 |
|----------|------|------|
| 自动模式 | `num_read_tasks=-1, override_num_blocks=None` | 行为与当前默认一致 |
| 只设 Task 数 | `num_read_tasks=10` | 10 个 ReadTask，Block 数自动 |
| 只设 Block 数 | `override_num_blocks=100` | 自动 Task 数，100 个 Block |
| 同时设置 | `num_read_tasks=10, override_num_blocks=100` | 10 个 ReadTask，100 个 Block |
| Task 数 > 文件数 | `num_read_tasks=100`，50 个文件 | 50 个 ReadTask，可能 split |
| Block 数 < size 下限 | `override_num_blocks=5`，大数据 | Block 数取 size 下限 |
| 旧 parallelism | `parallelism=10` | 等价于 `num_read_tasks=10` |

### 9.2 集成测试

- 端到端验证 `ds.take()` 和 `ds.count()` 结果正确
- 验证下游 map_batches 的并行度受 Block 数量影响
- 验证 `concurrency` 与 `num_read_tasks` 独立生效
- 验证 AutoScaler 场景下动态调整行为

### 9.3 性能测试

- 对比改造前后相同参数的执行时间（确保无性能退化）
- 测试 `num_read_tasks=10, override_num_blocks=1000` 场景下的内存使用

---

## 10. 风险评估

### 10.1 Breaking Change 风险

| 场景 | 风险 | 缓解措施 |
|------|------|----------|
| 使用 `override_num_blocks` 的现有代码 | Task 数从手动变为自动 | Option B 引入 `num_read_tasks`，`override_num_blocks` 行为不变直到用户迁移 |
| 使用 `parallelism` 的现有代码 | 无风险 | `parallelism` 行为不变，仅打印废弃警告 |
| 自定义 Datasource 实现 | 低风险 | `get_read_tasks(parallelism)` 接口不变 |

### 10.2 Split Factor 计算精度

- `_split_blocks` 是按行数拆分，如果单个 ReadTask 产出的 block 行数少于 `k`，可能无法拆成 `k` 份
- 需要确保 `k` 是上界，实际 Block 数可能略多于目标

### 10.3 Size-Based 安全下限

- `target_num_blocks = max(override_num_blocks, estimated_num_blocks)` 确保 Block 数不会少于内存安全下限
- 当 `override_num_blocks` 远小于 size 下限时，应打印 warning 提醒用户

---

## 11. 替代方案

### 11.1 方案二：仅文档修正

不改代码，只修正 `override_num_blocks` 的文档描述，将其语义从 "output blocks" 改为 "read tasks"。

**优点**：零代码改动，无 Breaking Change
**缺点**：参数名 `override_num_blocks` 与实际效果（控制 Task 数）仍然不一致，用户无法独立控制

### 11.2 方案三：新增 DataContext 配置

在 `DataContext` 中新增 `read_task_parallelism_factor`，通过全局配置控制 ReadTask 与 Block 的比例。

**优点**：不改变现有 API
**缺点**：全局配置不够灵活，无法针对不同 read 操作设置不同值

### 11.3 方案四：Post-Read Repartition

用户先读取数据，然后通过 `ds.repartition(B)` 控制最终 Block 数。

```python
ds = ray.data.read_parquet("s3://...", override_num_blocks=10)
ds = ds.repartition(100)  # 10 个 ReadTask → 100 个 Block
```

**优点**：不改变 Read 层逻辑
**缺点**：
- `repartition` 需要 shuffle，开销远大于 `_split_blocks`（零拷贝切片）
- 语义上 `repartition` 不是 Read 阶段的配置，逻辑不连贯
- 用户体验差

---

## 附录：与现有代码的关系

### A.1 现有 split 机制复用

本方案**完全复用**现有的 `_additional_split_factor` 和 `_split_blocks` 机制，无需修改执行层代码。改造仅限于：

1. 参数传递路径（`_get_num_output_blocks`, `read_datasource`, `Read`）
2. Split factor 计算逻辑（`compute_additional_split_factor`）

### A.2 compute_additional_split_factor 改造前后对比

```
改造前:
  输入: parallelism（同时作为 task 数和 block 数）
  逻辑:
    detected_parallelism = _autodetect_parallelism(parallelism, ...)
    num_read_tasks = get_read_tasks(detected_parallelism)
    estimated_num_blocks = num_read_tasks * size_based_splits
    if estimated_num_blocks < detected_parallelism:
      k = ceil(detected_parallelism / estimated_num_blocks)

改造后:
  输入: parallelism（task 数）, override_num_blocks（block 数）
  逻辑:
    detected_parallelism = _autodetect_parallelism(parallelism, ...)
    num_read_tasks = get_read_tasks(detected_parallelism)
    target_blocks = override_num_blocks or detected_parallelism
    target_blocks = max(target_blocks, num_read_tasks * size_based_splits)  # 防 OOM
    if target_blocks > num_read_tasks:
      k = ceil(target_blocks / num_read_tasks)
```

核心差异：**目标 Block 数的来源从 `detected_parallelism`（混淆）变为 `override_num_blocks`（明确）**。

### A.3 相关 Issue 参考

- [ray-project/ray#36836](https://github.com/ray-project/ray/issues/36836) — `override_num_blocks` 语义混乱的讨论
- [ray-project/ray#41971](https://github.com/ray-project/ray/issues/41971) — 用户反馈 override_num_blocks 行为与文档不符
