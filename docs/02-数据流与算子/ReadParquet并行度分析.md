# Ray Data read_parquet 并行度参数深度分析

本文档详细分析 `ray.data.read_parquet` 中 `concurrency`、`override_num_blocks` 两个参数的作用机制，
以及自动并行度计算逻辑 `_autodetect_parallelism`、read task 内部 block 产出机制的完整代码路径。

> 基于 Ray 2.52.1 源码分析

---

## 目录

1. [核心结论](#1-核心结论)
2. [参数入口：read_parquet 函数签名](#2-参数入口read_parquet-函数签名)
3. [override_num_blocks 完整代码路径](#3-override_num_blocks-完整代码路径)
4. [concurrency 完整代码路径](#4-concurrency-完整代码路径)
5. [_autodetect_parallelism 自动并行度计算](#5-_autodetect_parallelism-自动并行度计算)
6. [compute_additional_split_factor 额外分裂因子](#6-compute_additional_split_factor-额外分裂因子)
7. [Read Task 内部 Block 产出机制](#7-read-task-内部-block-产出机制)
8. [举例说明](#8-举例说明)
9. [完整流程图](#9-完整流程图)
10. [compute_additional_split_factor 与 get_read_tasks 完整逻辑链](#10-compute_additional_split_factor-与-get_read_tasks-完整逻辑链)

---

## 1. 核心结论

| 参数 | 作用 | 控制 task 总数？ | 一个 task 读多文件？ | 控制输出 block 数？ |
|------|------|:---:|:---:|:---:|
| `concurrency` | 限制同时运行的 task 数（运行时限流） | 否 | 否 | 否 |
| `override_num_blocks` | 控制 read task 总数 | **是** | **是** | 间接（影响 task 数，但 block 数由数据量决定） |

- **`override_num_blocks`** 决定了文件如何被分组到 read task 中。设置为 N 表示将所有文件分成 N 组，每组一个 read task。
- **`concurrency`** 只是一个运行时并发度上限，不影响 task 总数和 block 总数。
- **一个 read task 读多个文件时，每个文件独立产出 block，不会合并。每个文件通常产出多个 block。**

---

## 2. 参数入口：read_parquet 函数签名

**文件**: `python/ray/data/read_api.py:890-1077`

```python
def read_parquet(
    paths: Union[str, List[str]],
    *,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    columns: Optional[List[str]] = None,
    parallelism: int = -1,                      # 已废弃，使用 override_num_blocks 替代
    # ...
    concurrency: Optional[int] = None,          # 运行时并发度限制
    override_num_blocks: Optional[int] = None,  # 控制 read task 总数
    **arrow_parquet_args,
) -> Dataset:
```

参数文档说明：

- **`concurrency`**（`read_api.py:1018-1021`）：
  > "The maximum number of Ray tasks to run concurrently. Set this to control
  > number of tasks to run concurrently. **This doesn't change the total number
  > of tasks run or the total number of output blocks.** By default, concurrency
  > is dynamically decided based on the available resources."

- **`override_num_blocks`**（`read_api.py:1022-1025`）：
  > "Override the number of output blocks from all read tasks. By default, the
  > number of output blocks is dynamically decided based on input data size
  > and available resources."

---

## 3. override_num_blocks 完整代码路径

### 3.1 参数转换：`_get_num_output_blocks`

**文件**: `python/ray/data/read_api.py:4522-4533`

```python
def _get_num_output_blocks(
    parallelism: int = -1,
    override_num_blocks: Optional[int] = None,
) -> int:
    if parallelism != -1:
        logger.warning(
            "The argument ``parallelism`` is deprecated in Ray 2.10. Please specify "
            "argument ``override_num_blocks`` instead."
        )
    elif override_num_blocks is not None:
        parallelism = override_num_blocks    # <-- override_num_blocks 赋值给 parallelism
    return parallelism
```

`override_num_blocks` 会被转换为内部的 `parallelism` 参数。如果用户同时传了旧的 `parallelism` 参数，则优先使用旧参数并打印废弃警告。

### 3.2 传入 read_datasource

**文件**: `python/ray/data/read_api.py:1068-1077`

```python
return read_datasource(
    datasource,
    num_cpus=num_cpus,
    num_gpus=num_gpus,
    memory=memory,
    parallelism=parallelism,       # <-- 已被 override_num_blocks 替换
    ray_remote_args=ray_remote_args,
    concurrency=concurrency,
    override_num_blocks=override_num_blocks,
)
```

### 3.3 read_datasource 中的处理

**文件**: `python/ray/data/read_api.py:420-482`

```python
def read_datasource(datasource, ..., parallelism=-1, concurrency=None, override_num_blocks=None):
    # Step 1: 转换参数
    parallelism = _get_num_output_blocks(parallelism, override_num_blocks)

    # Step 2: 自动检测并行度（如果 parallelism > 0，直接返回该值）
    requested_parallelism, _, _ = _autodetect_parallelism(
        parallelism,
        ctx.target_max_block_size,
        DataContext.get_current(),
        datasource_or_legacy_reader,
        placement_group=cur_pg,
    )

    # Step 3: 获取初始的 read tasks（此处主要用于元数据统计）
    read_tasks = datasource_or_legacy_reader.get_read_tasks(requested_parallelism)

    # Step 4: 创建 Read 逻辑算子
    read_op = Read(
        datasource,
        datasource_or_legacy_reader,
        parallelism=parallelism,                   # <-- 存储到逻辑算子中
        num_outputs=len(read_tasks) if read_tasks else 0,
        ray_remote_args=ray_remote_args,
        compute=TaskPoolStrategy(concurrency),     # <-- concurrency 包装为执行策略
    )
```

### 3.4 文件分组：`get_read_tasks` 中的 `np.array_split`

**文件**: `python/ray/data/_internal/datasource/parquet_datasource.py:515-572`

这是 `override_num_blocks` 生效的**核心位置**——文件被分组到 read task 中：

```python
def get_read_tasks(self, parallelism, ...):
    # ...
    for fragments, paths in zip(
        np.array_split(pq_fragments, parallelism),   # <-- 将 N 个文件分成 parallelism 组
        np.array_split(pq_paths, parallelism),
    ):
        if len(fragments) <= 0:
            continue

        meta = BlockMetadata(
            num_rows=None,
            size_bytes=self._estimate_in_mem_size(fragments),
            input_files=paths,
            exec_stats=None,
        )

        read_tasks.append(
            ReadTask(
                lambda f=fragments: read_fragments(
                    block_udf,
                    to_batches_kwargs,
                    default_read_batch_size_rows,
                    data_columns,
                    data_columns_rename_map,
                    partition_columns,
                    read_schema,
                    f,              # <-- 每个 task 拿到一组 fragments（多个文件）
                    include_paths,
                    partitioning,
                    filter_expr,
                ),
                meta,
                schema=target_schema,
                per_task_row_limit=per_task_row_limit,
            )
        )

    return read_tasks
```

**关键**：`np.array_split(pq_fragments, parallelism)` 将所有文件均匀分成 `parallelism` 组，每组对应一个 `ReadTask`。

### 3.5 执行阶段：`plan_read_op`

**文件**: `python/ray/data/_internal/planner/plan_read_op.py:56-131`

```python
def plan_read_op(op, physical_children, data_context):
    def get_input_data(target_max_block_size) -> List[RefBundle]:
        parallelism = op.get_detected_parallelism()   # <-- 从优化器获取最终并行度
        read_tasks = op.datasource_or_legacy_reader.get_read_tasks(
            parallelism,
            per_task_row_limit=op.per_block_limit,
            data_context=data_context,
        )
        # 每个 read_task 被 ray.put 并包装成 RefBundle
        for read_task in read_tasks:
            read_task_ref = ray.put(read_task)
            ref_bundle = RefBundle(((read_task_ref, _derive_metadata(read_task, read_task_ref)),), ...)
            ret.append(ref_bundle)
        return ret

    inputs = InputDataBuffer(data_context, input_data_factory=get_input_data)

    def do_read(blocks: Iterable[ReadTask], _: TaskContext) -> Iterable[Block]:
        for read_task in blocks:
            yield from read_task()    # <-- 执行 read_task，yield 所有 block

    return MapOperator.create(
        map_transformer,
        inputs,
        data_context,
        name=op.name,
        compute_strategy=op.compute,      # <-- TaskPoolStrategy(concurrency)
        ray_remote_args=op.ray_remote_args,
    )
```

---

## 4. concurrency 完整代码路径

### 4.1 包装为 TaskPoolStrategy

**文件**: `python/ray/data/read_api.py:466-472`

```python
read_op = Read(
    datasource,
    datasource_or_legacy_reader,
    parallelism=parallelism,
    num_outputs=len(read_tasks) if read_tasks else 0,
    ray_remote_args=ray_remote_args,
    compute=TaskPoolStrategy(concurrency),   # <-- concurrency → TaskPoolStrategy.size
)
```

### 4.2 传递到 TaskPoolMapOperator

**文件**: `python/ray/data/_internal/planner/plan_read_op.py:123-130`

```python
return MapOperator.create(
    map_transformer,
    inputs,
    data_context,
    name=op.name,
    compute_strategy=op.compute,    # <-- TaskPoolStrategy(concurrency)
    ray_remote_args=op.ray_remote_args,
)
```

`MapOperator.create` 内部创建 `TaskPoolMapOperator(max_concurrency=compute_strategy.size)`。

### 4.3 运行时限流：ConcurrencyCapBackpressurePolicy

`ConcurrencyCapBackpressurePolicy` 在调度时检查：

```python
def can_add_input(self, op: "PhysicalOperator") -> bool:
    num_tasks_running = op.metrics.num_tasks_running
    return num_tasks_running < self._concurrency_caps[op]
```

**类比**：
- `override_num_blocks = 20` → 总共 20 个工作包
- `concurrency = 4` → 同时最多 4 个工人在干活

**典型用途**：
1. **内存控制** — 避免同时跑太多 task 导致 OOM
2. **IO 限流** — 避免对 S3 等远程存储发起过多并发请求
3. **资源竞争** — 集群资源有限时避免抢占太多 worker

---

## 5. _autodetect_parallelism 自动并行度计算

当用户不设置 `override_num_blocks`（即 `parallelism == -1`）时，系统自动计算并行度。

**文件**: `python/ray/data/_internal/util.py:134-250`

### 5.1 默认配置值

**文件**: `python/ray/data/context.py:51-81`

| 常量 | 默认值 | 含义 |
|------|--------|------|
| `DEFAULT_TARGET_MAX_BLOCK_SIZE` | `128 * 1024 * 1024` = **128 MiB** | 单个 block 最大尺寸，防止 OOM |
| `DEFAULT_TARGET_MIN_BLOCK_SIZE` | `1 * 1024 * 1024` = **1 MiB** | 单个 block 最小尺寸，避免过小 block 开销 |
| `DEFAULT_READ_OP_MIN_NUM_BLOCKS` | **200** | 默认起始并行度 |

```python
# context.py:51-55
# We chose 128MiB for default: With streaming execution and num_cpus many concurrent
# tasks, the memory footprint will be about 2 * num_cpus * target_max_block_size ~= RAM
# * DEFAULT_OBJECT_STORE_MEMORY_LIMIT_FRACTION * 0.3
DEFAULT_TARGET_MAX_BLOCK_SIZE = 128 * 1024 * 1024

# context.py:69
DEFAULT_TARGET_MIN_BLOCK_SIZE = 1 * 1024 * 1024

# context.py:81
DEFAULT_READ_OP_MIN_NUM_BLOCKS = 200
```

### 5.2 完整源码

```python
# python/ray/data/_internal/util.py:134-250
def _autodetect_parallelism(
    parallelism: int,
    target_max_block_size: Optional[int],
    ctx: DataContext,
    datasource_or_legacy_reader=None,
    mem_size: Optional[int] = None,
    placement_group=None,
    avail_cpus: Optional[int] = None,
) -> Tuple[int, str, Optional[int]]:

    min_safe_parallelism = 1
    max_reasonable_parallelism = sys.maxsize

    # Step 1: 估算数据总大小
    if mem_size is None and datasource_or_legacy_reader:
        mem_size = datasource_or_legacy_reader.estimate_inmemory_data_size()

    # Step 2: 根据数据大小计算上下界
    if mem_size is not None and not np.isnan(mem_size) and target_max_block_size is not None:
        min_safe_parallelism = max(1, int(mem_size / target_max_block_size))
        max_reasonable_parallelism = max(1, int(mem_size / ctx.target_min_block_size))

    # Step 3: 如果 parallelism == -1（用户未指定），自动计算
    if parallelism < 0:
        avail_cpus = avail_cpus or _estimate_avail_cpus(placement_group)

        # 三者取最大值
        parallelism = max(
            min(ctx.read_op_min_num_blocks, max_reasonable_parallelism),  # 默认值（但不产生过小 block）
            min_safe_parallelism,                                         # 安全值（防止 block 过大 OOM）
            avail_cpus * 2,                                               # 至少 2 倍 CPU
        )

    return parallelism, reason, mem_size
```

### 5.3 计算逻辑详解

三个候选值取 `max`：

```
parallelism = max(A, B, C)

A = min(read_op_min_num_blocks, max_reasonable_parallelism)
  = min(200, mem_size / 1MiB)
  含义：默认起始值 200，但如果数据太小会被 max_reasonable 截断，避免产生 < 1MiB 的 block

B = min_safe_parallelism
  = mem_size / 128MiB
  含义：保证每个 block ≤ 128 MiB，防止 OOM

C = avail_cpus * 2
  含义：充分利用集群 CPU 资源
```

### 5.4 辅助函数：`_estimate_avail_cpus`

**文件**: `python/ray/data/_internal/util.py:253-284`

```python
def _estimate_avail_cpus(cur_pg: Optional["PlacementGroup"]) -> int:
    cluster_cpus = int(ray.cluster_resources().get("CPU", 1))
    cluster_gpus = int(ray.cluster_resources().get("GPU", 0))

    if cur_pg:
        # 在 placement group 内，按比例估算可用 CPU
        pg_cpus = 0
        for bundle in cur_pg.bundle_specs:
            cpu_fraction = bundle.get("CPU", 0) / max(1, cluster_cpus)
            gpu_fraction = bundle.get("GPU", 0) / max(1, cluster_gpus)
            max_fraction = max(cpu_fraction, gpu_fraction)
            pg_cpus += 2 * int(max_fraction * cluster_cpus)
        return min(cluster_cpus, pg_cpus)

    return cluster_cpus
```

### 5.5 举例说明

#### 例 1：小数据集（1 GiB，集群 8 CPU）

```
mem_size = 1 GiB = 1024 MiB

A = min(200, 1024/1) = min(200, 1024) = 200
B = 1024/128 = 8
C = 8 * 2 = 16

parallelism = max(200, 8, 16) = 200
每个 block 约 1024/200 ≈ 5 MiB
```

**结论**：受 `read_op_min_num_blocks`（默认 200）主导。

#### 例 2：大数据集（100 GiB，集群 8 CPU）

```
mem_size = 100 GiB = 102400 MiB

A = min(200, 102400/1) = 200
B = 102400/128 = 800
C = 8 * 2 = 16

parallelism = max(200, 800, 16) = 800
每个 block 约 102400/800 = 128 MiB
```

**结论**：受 `min_safe_parallelism`（安全下限）主导，保证每个 block 不超过 128 MiB。

#### 例 3：小数据集 + 大集群（1 GiB，集群 200 CPU）

```
mem_size = 1024 MiB

A = min(200, 1024) = 200
B = 1024/128 = 8
C = 200 * 2 = 400

parallelism = max(200, 8, 400) = 400
每个 block 约 1024/400 ≈ 2.5 MiB
```

**结论**：受 CPU 数主导，充分利用集群资源。

#### 例 4：极小数据集（100 MiB，集群 8 CPU）

```
mem_size = 100 MiB

A = min(200, 100/1) = min(200, 100) = 100   ← max_reasonable 截断
B = 100/128 ≈ 1
C = 8 * 2 = 16

parallelism = max(100, 1, 16) = 100
每个 block 约 100/100 = 1 MiB
```

**结论**：`max_reasonable_parallelism` 截断了默认 200，避免产生 < 1 MiB 的过小 block。

#### 例 5：超大数据集 + 大集群（1 TiB，集群 500 CPU）

```
mem_size = 1 TiB = 1048576 MiB

A = min(200, 1048576) = 200
B = 1048576/128 = 8192
C = 500 * 2 = 1000

parallelism = max(200, 8192, 1000) = 8192
每个 block 约 128 MiB
```

**结论**：数据量极大时，安全下限 `min_safe_parallelism` 远超其他候选值。

---

## 6. compute_additional_split_factor 额外分裂因子

当文件数少于 detected_parallelism 时，系统无法创建足够多的 read task。此时通过 split factor 将每个 task 的输出拆分成更多 block，保证下游算子获得足够的并行度。

**文件**: `python/ray/data/_internal/logical/rules/set_read_parallelism.py:23-87`

```python
def compute_additional_split_factor(
    datasource_or_legacy_reader,
    parallelism: int,
    mem_size: int,
    target_max_block_size: Optional[int],
    cur_additional_split_factor: Optional[int] = None,
) -> Tuple[int, str, int, Optional[int]]:

    ctx = DataContext.get_current()

    # Step 1: 计算 detected_parallelism
    detected_parallelism, reason, _ = _autodetect_parallelism(
        parallelism, target_max_block_size, ctx, datasource_or_legacy_reader, mem_size
    )

    # Step 2: 获取实际的 read task 数量（受文件数限制）
    num_read_tasks = len(
        datasource_or_legacy_reader.get_read_tasks(detected_parallelism)
    )

    # Step 3: 计算基于大小的分裂因子
    expected_block_size = None
    if mem_size:
        expected_block_size = mem_size / num_read_tasks
        if target_max_block_size is None:
            size_based_splits = 1
        else:
            size_based_splits = round(
                max(1, expected_block_size / target_max_block_size)
            )
    else:
        size_based_splits = 1

    if cur_additional_split_factor:
        size_based_splits *= cur_additional_split_factor

    estimated_num_blocks = num_read_tasks * size_based_splits

    # Step 4: 如果 block 数仍不够，额外增加分裂因子 k
    if estimated_num_blocks < detected_parallelism and estimated_num_blocks > 0:
        k = math.ceil(detected_parallelism / estimated_num_blocks)
        estimated_num_blocks = estimated_num_blocks * k
        return detected_parallelism, reason, estimated_num_blocks, k

    return detected_parallelism, reason, estimated_num_blocks, None
```

### 6.1 应用分裂因子：SetReadParallelismRule

**文件**: `python/ray/data/_internal/logical/rules/set_read_parallelism.py:90-148`

```python
class SetReadParallelismRule(Rule):
    def _apply(self, op: PhysicalOperator, logical_op: Read):
        estimated_in_mem_bytes = logical_op.infer_metadata().size_bytes

        (detected_parallelism, reason, estimated_num_blocks, k) = compute_additional_split_factor(
            logical_op.datasource_or_legacy_reader,
            logical_op.parallelism,                   # <-- 来自 override_num_blocks
            estimated_in_mem_bytes,
            op.target_max_block_size_override or op.data_context.target_max_block_size,
            op._additional_split_factor,
        )

        logical_op.set_detected_parallelism(detected_parallelism)

        if k is not None:
            op.set_additional_split_factor(k)    # <-- 每个 task 输出拆分成 k 个 block
```

### 6.2 举例

```
场景：detected_parallelism=800，但只有 100 个文件

num_read_tasks = 100（文件数 < parallelism，最多 100 个 task）
expected_block_size = mem_size / 100

假设 size_based_splits = 1（数据不大）
estimated_num_blocks = 100 * 1 = 100

100 < 800，所以：
k = ceil(800 / 100) = 8
每个 read task 的输出被拆分成 8 个 block
最终 estimated_num_blocks = 100 * 8 = 800
```

---

## 7. Read Task 内部 Block 产出机制

### 7.1 核心问题

当 `override_num_blocks=10` 且有 100 个文件时，每个 task 分配到 ~10 个文件。
每个 task 产出多少个 block？

**答案**：每个文件独立产出 block，且通常一个文件产出多个 block。文件之间**不会**合并。

### 7.2 代码分析：read_fragments

**文件**: `python/ray/data/_internal/datasource/parquet_datasource.py:806-851`

```python
def read_fragments(
    block_udf,
    to_batches_kwargs,
    default_read_batch_size_rows,    # <-- batch_size，控制每批行数
    data_columns,
    data_columns_rename_map,
    partition_columns,
    schema,
    fragments: List[_ParquetFragment],  # <-- 该 task 分配到的文件列表
    include_paths,
    partitioning,
    filter_expr=None,
) -> Iterator["pyarrow.Table"]:

    assert len(fragments) > 0

    logger.debug(f"Reading {len(fragments)} parquet fragments")
    for fragment in fragments:                        # ← 外层循环：逐文件遍历
        ctx = ray.data.DataContext.get_current()
        for table in iterate_with_retry(
            lambda: _read_batches_from(
                fragment.original,
                schema=schema,
                data_columns=data_columns,
                data_columns_rename_map=data_columns_rename_map,
                partition_columns=partition_columns,
                partitioning=partitioning,
                include_path=include_paths,
                filter_expr=filter_expr,
                batch_size=default_read_batch_size_rows,  # ← 按行数分批
                to_batches_kwargs=to_batches_kwargs,
            ),
            "reading batches",
            match=ctx.retried_io_errors,
        ):
            if table.num_rows > 0:
                if block_udf is not None:
                    yield block_udf(table)
                else:
                    yield table                       # ← 每个 batch 产生一个 block
```

### 7.3 代码分析：_read_batches_from

**文件**: `python/ray/data/_internal/datasource/parquet_datasource.py:854-955`

```python
def _read_batches_from(
    fragment: "ParquetFileFragment",
    *,
    schema,
    data_columns,
    data_columns_rename_map,
    partition_columns,
    partitioning,
    filter_expr=None,
    batch_size=None,           # <-- 每批读取的行数
    include_path=False,
    use_threads=False,
    to_batches_kwargs=None,
) -> Iterable["pyarrow.Table"]:

    to_batches_kwargs = dict(to_batches_kwargs or {})
    if batch_size is not None:
        to_batches_kwargs.setdefault("batch_size", batch_size)

    def _generate_tables() -> "pa.Table":
        for batch in fragment.to_batches(        # ← PyArrow 按 batch_size 分批读取
            columns=data_columns,
            filter=filter_expr,
            schema=schema,
            use_threads=use_threads,
            **to_batches_kwargs,
        ):
            table = pa.Table.from_batches([batch])   # 每个 batch → 一个 Table

            if partition_col_values:
                table = _add_partitions_to_table(partition_col_values, table)

            if include_path:
                table = ArrowBlockAccessor.for_block(table).fill_column(
                    "path", fragment.path
                )

            yield table                          # ← 每个 batch 产出一个 block
```

### 7.4 batch_size 的估算

**文件**: `python/ray/data/_internal/datasource/parquet_datasource.py:1118-1141`

```python
def _estimate_reader_batch_size(
    file_infos: List[Optional[_ParquetFileInfo]],
    target_block_size: Optional[int],            # <-- 默认 128 MiB
) -> Optional[int]:
    if target_block_size is None:
        return None

    avg_num_rows_per_block = [
        target_block_size / fi.avg_row_in_mem_bytes
        for fi in file_infos
        if fi is not None and fi.avg_row_in_mem_bytes is not None and fi.avg_row_in_mem_bytes > 0
    ]

    if not avg_num_rows_per_block:
        return DEFAULT_PARQUET_READER_ROW_BATCH_SIZE   # 默认 10,000 行

    estimated_batch_size = max(math.ceil(np.mean(avg_num_rows_per_block)), 1)
    return estimated_batch_size
```

**目标**：每个 batch 的内存大小 ≈ `target_max_block_size`（128 MiB）。

计算公式：`batch_size = target_max_block_size / avg_row_in_mem_bytes`

### 7.5 Block 产出总结

对于一个 read task 包含 F 个文件，每个文件有 R_i 行，batch_size 为 B：

```
task 产出的 block 数 = Σ ceil(R_i / B)    （i = 1 到 F）
```

| 因素 | 值 |
|------|-----|
| 每个 task 的文件数 | F = 总文件数 / override_num_blocks |
| 每个文件的 block 数 | ceil(文件行数 / batch_size) |
| batch_size | target_max_block_size / avg_row_in_mem_bytes |
| **task 总 block 数** | **所有文件的 block 数之和** |

**关键点**：
1. 每个文件**至少**产出 1 个 block（只要文件非空）
2. 文件之间**不会合并**成一个 block，是独立遍历的
3. 每个文件通常产出**多个** block（取决于文件大小和 batch_size）
4. `override_num_blocks` 控制的是 **task 数量**，不是 **block 数量**
5. block 数量由**数据量 / target_max_block_size** 决定

---

## 8. 举例说明

### 场景：100 个 Parquet 文件，每个文件 50,000 行，每行约 2 KiB

**不设任何参数**（集群 8 CPU）：

```
mem_size ≈ 100 * 50000 * 2KiB ≈ 10 GiB = 10240 MiB

_autodetect_parallelism:
  A = min(200, 10240/1) = 200
  B = 10240/128 = 80
  C = 8 * 2 = 16
  parallelism = max(200, 80, 16) = 200

但文件只有 100 个，所以：
  num_read_tasks = 100（不能超过文件数）
  expected_block_size = 10240/100 = 102.4 MiB < 128 MiB
  size_based_splits = round(max(1, 102.4/128)) = 1
  estimated_num_blocks = 100

  100 < 200，所以：
  k = ceil(200/100) = 2
  每个 task 输出拆分成 2 个 block
  最终 estimated_num_blocks = 200

结果：
  - 100 个 read task
  - 每个 task 读 1 个文件
  - 每个 task 内部：batch_size ≈ 128MiB/2KiB = 65536 行
    因为文件只有 50000 行 < 65536，所以每个文件产出 1 个 block
  - 再通过 split factor k=2 拆分，每个 task 最终产出 2 个 block
  - 总计 200 个 block
```

**设置 `override_num_blocks=10`**：

```
parallelism = 10（用户显式指定，跳过自动计算）

num_read_tasks = 10
每个 task 读 10 个文件

每个 task 内部（10 个文件）：
  batch_size ≈ 65536 行
  每个文件 50000 行 < 65536 → 每个文件产出 1 个 block
  每个 task 产出 10 个 block

总计：10 * 10 = 100 个 block

compute_additional_split_factor:
  expected_block_size = 10240/10 = 1024 MiB
  size_based_splits = round(1024/128) = 8
  estimated_num_blocks = 10 * 8 = 80

  注意：parallelism=10（用户显式设置），detected_parallelism=10
  80 > 10，不需要额外 k
```

**设置 `override_num_blocks=10, concurrency=4`**：

```
与上面相同的 task 分配：10 个 task，每个读 10 个文件
但同一时刻最多只有 4 个 task 在并行执行
执行完一个才会调度下一个
```

---

## 9. 完整流程图

```
                          用户调用
                             │
                             ▼
          read_parquet(paths, override_num_blocks=N, concurrency=C)
                             │
                             ▼
          _get_num_output_blocks(override_num_blocks=N)
                │
                ▼
          parallelism = N  (如果 N 不为 None)
          parallelism = -1 (如果 N 为 None，走自动计算)
                │
                ▼
          read_datasource(datasource, parallelism, concurrency=C)
                │
                ├──→ _autodetect_parallelism(parallelism, ...)
                │       │
                │       ├── parallelism > 0: 直接返回（用户显式设置）
                │       └── parallelism == -1: 自动计算
                │             parallelism = max(
                │               min(200, mem_size/1MiB),
                │               mem_size/128MiB,
                │               avail_cpus*2
                │             )
                │
                ├──→ datasource.get_read_tasks(parallelism)
                │       │
                │       └── np.array_split(files, parallelism)
                │             → 每组一个 ReadTask
                │
                └──→ Read(parallelism=parallelism, compute=TaskPoolStrategy(C))
                             │
                             ▼
               ┌─────────────────────────────┐
               │    优化阶段                   │
               │  SetReadParallelismRule       │
               │                               │
               │  compute_additional_split_factor()
               │    → detected_parallelism     │
               │    → split_factor k           │
               │                               │
               │  如果 num_tasks < parallelism: │
               │    k = ceil(parallelism/tasks) │
               │    每个 task 输出拆成 k 个 block │
               └──────────────┬────────────────┘
                              │
                              ▼
               ┌─────────────────────────────┐
               │    执行阶段                   │
               │  plan_read_op()              │
               │                               │
               │  get_read_tasks(parallelism)  │
               │    → N 个 ReadTask            │
               │                               │
               │  MapOperator.create(          │
               │    strategy=TaskPoolStrategy(C)│
               │  )                            │
               │    → TaskPoolMapOperator       │
               │      max_concurrency=C        │
               └──────────────┬────────────────┘
                              │
                              ▼
               ┌─────────────────────────────┐
               │    ReadTask 执行              │
               │                               │
               │  for fragment in fragments:   │  ← 遍历每个文件
               │    for batch in to_batches(): │  ← 按 batch_size 分批
               │      yield table              │  ← 每个 batch = 一个 block
               │                               │
               │  block 总数 =                 │
               │    Σ ceil(file_rows/batch_size)│
               └──────────────┬────────────────┘
                              │
                              ▼
               ┌─────────────────────────────┐
               │    运行时调度                  │
               │  ConcurrencyCapBackpressure  │
               │                               │
               │  同时运行的 task 数 ≤ C       │
               │  （如果 C 未设置则不限制）     │
               └──────────────────────────────┘
```

---

## 10. compute_additional_split_factor 与 get_read_tasks 完整逻辑链

本节深入分析从 `compute_additional_split_factor` 到最终 block 产出的完整执行链路，
揭示 ReadTask 数量与最终 block 数量之间的精确关系。

### 10.1 两次 get_read_tasks 调用

在整个读取流程中，`get_read_tasks` 被调用**两次**，分别在不同阶段：

| 调用位置 | 阶段 | parallelism 参数 | 目的 |
|----------|------|-------------------|------|
| `read_datasource`（read_api.py:460） | API 层 | `requested_parallelism`（自动检测或用户指定） | 获取初始 read task 数量，用于元数据和统计 |
| `compute_additional_split_factor`（set_read_parallelism.py:37） | 优化器 | `detected_parallelism`（再次自动检测） | 确定实际 read task 数量，计算 split factor |
| `plan_read_op → get_input_data`（plan_read_op.py:75） | 执行层 | `detected_parallelism`（优化器设定） | 实际生成并提交 read task |

**注意**：`read_datasource` 中的调用和 `compute_additional_split_factor` 中的调用都
使用 `_autodetect_parallelism` 的结果，但传入的参数略有差异——后者额外考虑了
`mem_size`（从 `infer_metadata()` 获取的估算内存大小）和 `cur_additional_split_factor`。

### 10.2 compute_additional_split_factor 逐步拆解

**文件**: `python/ray/data/_internal/logical/rules/set_read_parallelism.py:23-87`

```python
def compute_additional_split_factor(
    datasource_or_legacy_reader: Union[Datasource, Reader],
    parallelism: int,                                  # 用户指定的 parallelism
    mem_size: int,                                     # 估算的内存总大小
    target_max_block_size: Optional[int],              # 目标最大 block 大小
    cur_additional_split_factor: Optional[int] = None, # 已有的 split factor
) -> Tuple[int, str, int, Optional[int]]:
    # 返回: (detected_parallelism, reason, estimated_num_blocks, k)
```

**完整步骤**：

```
Step A: 自动检测 parallelism
  detected_parallelism = _autodetect_parallelism(parallelism, target_max_block_size, ctx, ...)

  如果 parallelism == -1: 自动计算（见第5节）
  如果 parallelism > 0:  直接使用用户指定值

Step B: 获取实际 read task 数量
  num_read_tasks = len(datasource_or_legacy_reader.get_read_tasks(detected_parallelism))

  注意：num_read_tasks <= detected_parallelism
  原因：min(parallelism, len(paths)) 限制了 task 数不能超过文件数

Step C: 基于内存大小计算 size_based_splits
  expected_block_size = mem_size / num_read_tasks

  如果 mem_size == 0 或 target_max_block_size == None:
    size_based_splits = 1
  否则:
    size_based_splits = round(max(1, expected_block_size / target_max_block_size))

  含义：每个 task 产出的数据如果超过 target_max_block_size，
        就需要在 size 维度拆分成更多 block

Step D: 叠加已有的 split factor
  if cur_additional_split_factor:
    size_based_splits *= cur_additional_split_factor

  含义：如果优化器之前已经设置过 split factor，在原有基础上乘法叠加

Step E: 计算 estimated_num_blocks
  estimated_num_blocks = num_read_tasks * size_based_splits

Step F: 检查是否仍不够 parallelism，计算额外 split factor k
  if estimated_num_blocks < detected_parallelism and estimated_num_blocks > 0:
    k = ceil(detected_parallelism / estimated_num_blocks)
    estimated_num_blocks = estimated_num_blocks * k
    return (detected_parallelism, reason, estimated_num_blocks, k)
  else:
    return (detected_parallelism, reason, estimated_num_blocks, None)
```

### 10.3 Split Factor 的传递与应用

`compute_additional_split_factor` 的返回值 `k` 被传递到 `MapOperator`，
在执行阶段通过 `_split_blocks()` 函数将每个 ReadTask 的输出拆分。

**传递路径**：

```
compute_additional_split_factor() 返回 k
  → SetReadParallelismRule._apply() 调用 op.set_additional_split_factor(k)
  → MapOperator._additional_split_factor = k
  → MapOperator._init_ray_remote_args() 中检查:
      if self.get_additional_split_factor() > 1:
          split_transformer = MapTransformer([BlockMapTransformFn(
              lambda blocks, ctx: _split_blocks(blocks, split_factor),
              disable_block_shaping=True,
          )])
          map_transformer = map_transformer.fuse(split_transformer)
  → ReadTask 执行后，输出 block 被按行数均分为 k 个子 block
```

### 10.4 _split_blocks 执行逻辑

**文件**: `python/ray/data/_internal/execution/operators/map_operator.py:975-984`

```python
def _split_blocks(blocks: Iterable[Block], split_factor: float) -> Iterable[Block]:
    for block in blocks:
        block = BlockAccessor.for_block(block)
        offset = 0
        split_sizes = _splitrange(block.num_rows(), split_factor)
        for size in split_sizes:
            if size <= 0:
                continue
            yield block.slice(offset, offset + size, copy=False)
            offset += size
```

**`_splitrange`** 将 `num_rows` 按 `split_factor` 拆分成近似等大的子集。
例如 `num_rows=100, split_factor=3` → `[33, 33, 34]`。

**关键特性**：
- 拆分是**按行**进行的，不涉及序列化/反序列化
- 使用 `copy=False`（零拷贝 slice），开销极低
- 拆分发生在 ReadTask **执行之后**，作为 MapTransformer 的一个 transform step

### 10.5 最终 Block 数量的精确公式

```
最终 block 数 = num_read_tasks × size_based_splits × k
```

各变量的含义和来源：

| 变量 | 来源 | 含义 |
|------|------|------|
| `num_read_tasks` | `len(get_read_tasks(detected_parallelism))` | 实际 ReadTask 数量，受文件数限制 |
| `size_based_splits` | `round(max(1, expected_block_size / target_max_block_size))` | 基于内存大小的拆分倍数 |
| `k` | `ceil(detected_parallelism / (num_read_tasks × size_based_splits))` | 补偿拆分因子，仅在不足时计算 |

**但实际运行时的 block 数还受 ReadTask 内部机制影响**：
- 每个 ReadTask 可能产出多个 block（每个文件的每个 batch 一个 block）
- `per_task_row_limit` 可以进一步拆分 ReadTask 的输出
- `_split_blocks` 作用在 ReadTask 产出的**每个** block 上

因此更精确的公式为：

```
最终 block 数 = Σ(i=1..num_read_tasks) [ Σ(j=1..blocks_from_task_i) split_size(blocks_j, k) ]
```

其中 `blocks_from_task_i` 是第 i 个 ReadTask 自然产出的 block 数量（由文件数和 batch_size 决定），
`split_size(block, k)` 是单个 block 按 k 拆分后的子 block 数量。

### 10.6 完整参数流转图

```
用户 API
  │
  ├── parallelism=-1, override_num_blocks=None
  │     │
  │     ▼ _get_num_output_blocks()
  │   parallelism = -1
  │     │
  │     ▼ _autodetect_parallelism()
  │   detected_parallelism = max(min(200, mem/1MiB), mem/128MiB, cpus*2)
  │     │
  │     ▼ get_read_tasks(detected_parallelism)
  │   num_read_tasks = min(detected_parallelism, num_files)
  │     │
  │     ▼ compute_additional_split_factor()
  │   size_based_splits = round(max(1, (mem/num_read_tasks) / 128MiB))
  │   estimated_num_blocks = num_read_tasks × size_based_splits
  │   if estimated_num_blocks < detected_parallelism:
  │     k = ceil(detected_parallelism / estimated_num_blocks)
  │   最终: num_read_tasks × size_based_splits × k 个 block
  │
  ├── parallelism=-1, override_num_blocks=N
  │     │
  │     ▼ _get_num_output_blocks()
  │   parallelism = N
  │     │
  │     ▼ _autodetect_parallelism(parallelism=N)
  │   detected_parallelism = N  （用户指定，直接返回）
  │     │
  │     ▼ get_read_tasks(N)
  │   num_read_tasks = min(N, num_files)
  │     │
  │     ▼ compute_additional_split_factor()
  │   size_based_splits = round(max(1, (mem/num_read_tasks) / 128MiB))
  │   estimated_num_blocks = num_read_tasks × size_based_splits
  │   if estimated_num_blocks < N:
  │     k = ceil(N / estimated_num_blocks)
  │   最终: num_read_tasks × size_based_splits × k 个 block
  │
  └── parallelism=N (deprecated), override_num_blocks=None
        │
        ▼ _get_num_output_blocks()
      parallelism = N （同 override_num_blocks=N 的路径）
```

### 10.7 关键问题：parallelism 和 override_num_blocks 语义混淆

当前实现中，`parallelism`（旧名）和 `override_num_blocks`（新名）是**完全相同**的参数：

```python
# read_api.py:4522-4533
def _get_num_output_blocks(parallelism=-1, override_num_blocks=None):
    if parallelism != -1:
        warn("deprecated")
    elif override_num_blocks is not None:
        parallelism = override_num_blocks   # 直接赋值
    return parallelism
```

**这个值同时影响了两个维度**：

1. **ReadTask 数量**：通过 `get_read_tasks(parallelism)` → 文件分组
2. **最终 block 数量**：通过 `compute_additional_split_factor()` 中的 split factor 计算

**带来的问题**：
- 用户无法独立控制"多少个并行读取任务"和"最终输出多少个 block"
- 设 `override_num_blocks=10` 意味着只有 10 个 ReadTask（并行度低），但同时系统认为目标 block 数也只有 10
- 如果想要"10 个并行任务但 100 个 block"，当前无法实现
- `override_num_blocks` 的文档说"Override the number of output blocks"，但实际效果更像是"override the number of read tasks"

**参见**：`docs/design/ray-data-decouple-parallelism-and-num-blocks.md` 中的改造设计方案。

---

## 附录：关键文件索引

| 文件 | 关键内容 |
|------|----------|
| `python/ray/data/read_api.py:890-1077` | `read_parquet` 函数入口 |
| `python/ray/data/read_api.py:420-482` | `read_datasource` 核心逻辑 |
| `python/ray/data/read_api.py:4522-4533` | `_get_num_output_blocks` 参数转换 |
| `python/ray/data/_internal/util.py:134-250` | `_autodetect_parallelism` 自动并行度计算 |
| `python/ray/data/_internal/util.py:253-284` | `_estimate_avail_cpus` CPU 估算 |
| `python/ray/data/context.py:51-81` | 默认配置常量 |
| `python/ray/data/_internal/logical/rules/set_read_parallelism.py:23-87` | `compute_additional_split_factor` |
| `python/ray/data/_internal/logical/rules/set_read_parallelism.py:90-148` | `SetReadParallelismRule` 优化器规则 |
| `python/ray/data/datasource/datasource.py:268-285` | `Datasource.get_read_tasks` 接口定义 |
| `python/ray/data/datasource/file_based_datasource.py:249-355` | `FileBasedDatasource.get_read_tasks` 实现 |
| `python/ray/data/_internal/logical/operators/read_operator.py:26-203` | `Read` 逻辑算子定义 |
| `python/ray/data/_internal/execution/operators/map_operator.py:225-232` | `_additional_split_factor` 字段定义 |
| `python/ray/data/_internal/execution/operators/map_operator.py:470-498` | split factor 应用逻辑 |
| `python/ray/data/_internal/execution/operators/map_operator.py:975-984` | `_split_blocks` 执行拆分 |
| `python/ray/data/_internal/planner/plan_read_op.py:56-131` | `plan_read_op` 执行计划 |
