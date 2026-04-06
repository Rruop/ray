# Ray Data Schema 推断与传递机制完整分析

> 基于 commit 07baebd0、a1b9dc62、3c60006e 的代码分析

---

## 目录

1. [背景：三个 Commit 的变更链](#1-背景三个-commit-的变更链)
2. [TableBlockBuilder.add() 完整逻辑](#2-tableblockbuilderadd-完整逻辑)
3. [DelegatingBlockBuilder 代理逻辑](#3-delegatingblockbuilder-代理逻辑)
4. [ArrowBlockBuilder 与 Arrow concat 的 schema unify](#4-arrowblockbuilder-与-arrow-concat-的-schema-unify)
5. [BlockOutputBuffer 的 block 塑形与切割](#5-blockoutputbuffer-的-block-塑形与切割)
6. [MapTransformer 的三种 UDF 模式与 schema 推断](#6-maptransformer-的三种-udf-模式与-schema-推断)
7. [_map_task 的 schema 传递逻辑](#7-_map_task-的-schema-传递逻辑)
8. [OpState.add_output 的 schema 去重与校验](#8-opstateadd_output-的-schema-去重与校验)
9. [BlockMetadataWithSchema 的序列化与 LRU Cache](#9-blockmetadatawithschema-的序列化与-lru-cache)
10. [Schema 推断全链路总结与潜在问题](#10-schema-推断全链路总结与潜在问题)

---

## 1. 背景：三个 Commit 的变更链

### 1.1 a1b9dc62 — Remove Schema From BlockMetadata (#53454)

**核心变更：** 将 schema 从 `BlockMetadata` 移到 operator 级别。

- 之前：每个 `BlockMetadata` 都携带 schema，一个 `RefBundle` 中有 N 个 block 就有 N 份重复 schema
- 之后：schema 移到 `RefBundle.schema` 和 `BlockMetadataWithSchema`，每个 bundle 只携带一份 schema
- 新增 `BlockMetadataWithSchema(metadata=..., schema=...)` 命名元组
- `_map_task` yield 的类型从 `Block, BlockMetadata` 变为 `Block, BlockMetadataWithSchema`
- `OpState` 新增 `_schema` 和 `_warned_on_schema_divergence` 状态
- 新增 `dedupe_schemas_with_validation()` 函数

**关键代码变更：**

```python
# block.py — 新增 BlockMetadataWithSchema
@DeveloperAPI(stability="alpha")
@dataclass
class BlockMetadataWithSchema(BlockMetadata):
    schema: Optional[Schema] = None

    def __init__(self, metadata: BlockMetadata, schema: Optional["Schema"] = None):
        super().__init__(
            input_files=metadata.input_files,
            size_bytes=metadata.size_bytes,
            num_rows=metadata.num_rows,
            exec_stats=metadata.exec_stats,
        )
        self.schema = schema

    @property
    def metadata(self) -> BlockMetadata:
        return BlockMetadata(
            num_rows=self.num_rows,
            size_bytes=self.size_bytes,
            exec_stats=self.exec_stats,
            input_files=metadata.input_files,
        )
```

```python
# physical_operator.py — DataOpTask 消费端变更
meta_with_schema: "BlockMetadataWithSchema" = ray.get(next(self._streaming_gen))
meta = meta_with_schema.metadata
self._output_ready_callback(
    RefBundle(
        [(block_ref, meta)],
        owns_blocks=True,
        schema=meta_with_schema.schema,
    ),
)
```

### 1.2 07baebd0 — Yield only first schema in _map_task (#62720)

**核心变更：** 在 `_map_task` 中只 yield 第一个 block 的 schema，后续 block 的 schema 设为 None，减少反序列化开销约 25%。

```python
# map_operator.py
yielded_schema: bool = False

for block in map_transformer.apply_transform(blocks_iter, ctx):
    block_schema = BlockAccessor.for_block(block).schema()
    # ...
    yield BlockMetadataWithSchema.from_metadata(
        ...,
        schema=block_schema if not yielded_schema else None,
    )
    yielded_schema = True
```

同时在 `OpState.add_output` 中增加 `if ref.schema is not None` 判断，跳过 schema=None 的 bundle 的 dedupe 逻辑：

```python
def add_output(self, ref: RefBundle) -> None:
    if ref.schema is not None:
        out_ref, diverged = dedupe_schemas_with_validation(...)
        # ... dedupe 逻辑
        ref = out_ref
        self._schema = ref.schema
    self.output_queue.append(ref)
```

### 1.3 3c60006e — Cache deserialized Arrow schemas in BlockMetadataWithSchema (#63462)

**核心变更：** 对 `BlockMetadataWithSchema.__setstate__` 中 `pa.ipc.read_schema` 加了 `@functools.lru_cache`，消除 scheduler 线程上重复的 schema 反序列化开销。

```python
@functools.lru_cache(maxsize=128)
def _read_arrow_schema_cached(schema_bytes: bytes) -> "pa.Schema":
    return pa.ipc.read_schema(pa.BufferReader(schema_bytes))

class BlockMetadataWithSchema(BlockMetadata):
    def __setstate__(self, state: Dict[str, Any]):
        schema_val: bytes | bytearray | Schema | None = state["schema"]
        if isinstance(schema_val, (bytes, bytearray)):
            if isinstance(schema_val, bytearray):
                schema_val = bytes(schema_val)  # bytearray 不可哈希，转 bytes
            state["schema"] = _read_arrow_schema_cached(schema_val)
        self.__dict__.update(state)
```

---

## 2. TableBlockBuilder.add() 完整逻辑

**文件：** `python/ray/data/_internal/table_block.py`

### 2.1 数据结构

```python
class TableBlockBuilder(BlockBuilder):
    def __init__(self, block_type):
        # 未压缩的行数据：列名 → 值列表
        # 使用 defaultdict(list) 方便追加
        self._columns = collections.defaultdict(list)

        # 已压缩的 table 列表
        # 当未压缩数据超过阈值时，会被压缩成一个 table 追加到这里
        self._tables: List[Any] = []

        # 已压缩 tables 的大小统计游标（延迟计算）
        self._tables_size_cursor = 0
        self._tables_size_bytes = 0

        # 未压缩数据的大小估算器
        self._uncompacted_size = SizeEstimator()

        self._num_rows = 0              # 总行数
        self._num_uncompacted_rows = 0  # 未压缩行数（_columns 中的行数）
        self._num_compactions = 0
        self._block_type = block_type   # (pyarrow.Table, bytes) 或 (pandas.DataFrame,)
```

### 2.2 add(item) — 逐行添加

```python
def add(self, item: Union[dict, Mapping]) -> None:
    # Step 1: 支持 as_pydict() 协议
    # ArrowRow 等 object 可以通过 as_pydict() 转为 dict
    if hasattr(item, "as_pydict"):
        item = item.as_pydict()

    # Step 2: 类型校验，必须是 dict/Mapping
    if not isinstance(item, collections.abc.Mapping):
        raise ValueError(
            "Returned elements of an TableBlock must be of type `dict`, "
            "got {} (type {}).".format(item, type(item))
        )

    # Step 3: 【Schema Union — 正向】
    # 新行有而 builder 没有的列，在 _columns 中创建该列，
    # 并用 None 填充之前所有行的该列值
    for column_name in item:
        if column_name not in self._columns:
            self._columns[column_name] = [None] * self._num_uncompacted_rows

    # Step 4: 【Schema Union — 反向】
    # builder 有而新行没有的列，用 None 填充当前行
    # item.get(column_name) 对缺失的 key 返回 None
    for column_name in self._columns:
        value = item.get(column_name)
        self._columns[column_name].append(value)

    # Step 5: 更新行数
    self._num_rows += 1
    self._num_uncompacted_rows += 1

    # Step 6: 检查是否需要压缩
    self._compact_if_needed()

    # Step 7: 更新大小估算
    self._uncompacted_size.add(item)
```

**具体例子：**

```python
builder = ArrowBlockBuilder()

# 第 1 行：有 a, b 两列
builder.add({"a": 1, "b": 2})
# _columns = {"a": [1], "b": [2]}
# _num_uncompacted_rows = 1

# 第 2 行：只有 a 列
builder.add({"a": 3})
# Step 3: "a" 已在 _columns 中，跳过
# Step 4: _columns["a"].append(3), _columns["b"].append(None)
# _columns = {"a": [1, 3], "b": [2, None]}
# _num_uncompacted_rows = 2

# 第 3 行：有 c 列（新列）
builder.add({"c": 4})
# Step 3: "c" 不在 _columns 中 → _columns["c"] = [None, None]
# Step 4: _columns["a"].append(None), _columns["b"].append(None), _columns["c"].append(4)
# _columns = {"a": [1, 3, None], "b": [2, None, None], "c": [None, None, 4]}
# _num_uncompacted_rows = 3

# 最终 schema = {"a": int64, "b": int64, "c": int64}（None 值会被 Arrow 处理为 null）
```

**关键特性：** `add()` 每次添加一行都会做双向 schema union：
- 正向：新列出现时，历史行补 None
- 反向：旧行存在时，新行缺失列补 None

这保证了 `_columns` 中所有列的列表长度一致，最终 build 出的 block 拥有统一的 schema。

### 2.3 add_block(block) — 整块添加

```python
def add_block(self, block: Any) -> None:
    # Step 1: 类型检查（Arrow vs Pandas），不检查 schema 是否一致！
    if not isinstance(block, self._block_type):
        raise TypeError(
            f"Got a block of type {type(block)}, expected {self._block_type}."
            "If you are mapping a function, ensure it returns an "
            "object with the expected type. Block:\n"
            f"{block}"
        )

    # Step 2: 直接追加到 _tables 列表，不做 schema 校验
    accessor = BlockAccessor.for_block(block)
    self._tables.append(block)
    self._num_rows += accessor.num_rows()
```

**注意：** `add_block` **不做任何 schema 校验**，只是类型检查（Arrow/Pandas）和直接追加。Schema 的统一推迟到 `build()` 时处理。

### 2.4 _compact_if_needed() — 压缩

```python
def _compact_if_needed(self) -> None:
    assert self._columns
    # 未压缩数据大小 < 阈值（默认 DEFAULT_TARGET_MAX_BLOCK_SIZE），不压缩
    if self._uncompacted_size.size_bytes() < MAX_UNCOMPACTED_SIZE_BYTES:
        return

    # 压缩：将 _columns 转为一个 table
    block = self._table_from_pydict(self._columns)
    self.add_block(block)

    # 清空未压缩状态
    self._uncompacted_size = SizeEstimator()
    self._columns.clear()
    self._num_compactions += 1
    self._num_uncompacted_rows = 0
```

**触发条件：** 当未压缩的 Python 数据（`_columns` 中的 list）大小超过 `MAX_UNCOMPACTED_SIZE_BYTES`（默认 128MB）。

**压缩过程：** 调用 `_table_from_pydict(self._columns)` 将 dict-of-lists 转为 Arrow Table（或 Pandas DataFrame），然后通过 `add_block()` 追加到 `_tables`。

**注意：** 压缩后 `_columns` 被清空，新的行会重新开始构建 `_columns`。这意味着在压缩前后，`_columns` 可能代表不同的列集合——但最终 `build()` 时会通过 `_combine_tables` 统一。

### 2.5 build() — 最终构建

```python
def build(self) -> Block:
    # Step 1: 如果有未压缩数据，先转成 table
    if self._columns:
        tables = [self._table_from_pydict(self._columns)]
    else:
        tables = []

    # Step 2: 追加已压缩的 tables
    tables.extend(self._tables)

    # Step 3: 空则返回空表
    if len(tables) == 0:
        return self._empty_table()

    # Step 4: 合并所有 tables（这里处理 schema 不一致的情况）
    else:
        return self._combine_tables(tables)
```

**对 Arrow：** `_combine_tables` → `transform_pyarrow.concat(tables, promote_types=True)`

**对 Pandas：** `_combine_tables` → `pd.concat(tables, ignore_index=True)`

---

## 3. DelegatingBlockBuilder 代理逻辑

**文件：** `python/ray/data/_internal/delegating_block_builder.py`

`DelegatingBlockBuilder` 是一个代理层，延迟决定底层使用 `ArrowBlockBuilder` 还是 `PandasBlockBuilder`。

```python
class DelegatingBlockBuilder(BlockBuilder):
    def __init__(self):
        self._builder = None        # 延迟初始化，第一个 add 时决定
        self._empty_block = None    # 存储空 block（用于只有空数据的场景）

    @property
    def _inferred_block_type(self) -> Optional[BlockType]:
        """从第一个添加的 item 推断 block 类型"""
        if self._builder is not None:
            return self._builder.block_type()
        return None
```

### 3.1 add(item) — 逐行添加

```python
def add(self, item: Mapping[str, Any]) -> None:
    assert isinstance(item, collections.abc.Mapping), item

    # 默认使用 ArrowBlockBuilder（不根据 item 内容判断）
    if self._builder is None:
        self._builder = ArrowBlockBuilder()

    self._builder.add(item)
```

**注意：** `DelegatingBlockBuilder.add()` **始终默认使用 ArrowBlockBuilder**，不管 item 的实际内容是什么。只有通过 `add_block` 或 `add_batch` 添加第一个非空数据时，才会根据数据类型推断 builder。

### 3.2 add_batch(batch) — 批量添加

```python
def add_batch(self, batch: DataBatch):
    """将用户批次数据转为内部 block 再添加"""
    block = BlockAccessor.batch_to_block(batch, self._inferred_block_type)
    return self.add_block(block)
```

### 3.3 add_block(block) — 整块添加

```python
def add_block(self, block: Block):
    accessor = BlockAccessor.for_block(block)

    # 空 block：单独存储，不影响 builder 类型推断
    if accessor.num_rows() == 0:
        self._empty_block = block
        return

    # 第一个非空 block：根据其类型创建 builder
    if self._builder is None:
        self._builder = accessor.builder()
    else:
        # 后续 block：类型必须一致
        block_type = accessor.block_type()
        assert block_type == self._inferred_block_type, (
            block_type,
            self._inferred_block_type,
        )

    # 追加到 builder（不做 schema 校验！）
    self._builder.add_block(accessor.to_block())
```

**关键点：**
- 第一个非空 block 决定 builder 类型（Arrow 或 Pandas）
- 后续 block 类型必须一致（assert 检查），但 **schema 不检查**
- `add_block` 调用 `self._builder.add_block()`，最终到 `TableBlockBuilder.add_block()`，不做 schema 校验

### 3.4 build() — 构建

```python
def build(self) -> Block:
    if self._builder is None:
        if self._empty_block is not None:
            # 只有空 block 的情况
            self._builder = BlockAccessor.for_block(self._empty_block).builder()
            self._builder.add_block(self._empty_block)
        else:
            # 完全没有数据，默认 Arrow
            self._builder = ArrowBlockBuilder()
    return self._builder.build()
```

---

## 4. ArrowBlockBuilder 与 Arrow concat 的 schema unify

**文件：** `python/ray/data/_internal/arrow_block.py`

### 4.1 ArrowBlockBuilder

```python
class ArrowBlockBuilder(TableBlockBuilder):
    def __init__(self):
        super().__init__((pyarrow.Table, bytes))  # block_type 允许 Table 或 bytes

    @staticmethod
    def _table_from_pydict(columns: Dict[str, List[Any]]) -> Block:
        """将 dict-of-lists 转为 Arrow Table"""
        return pyarrow_table_from_pydict(
            {
                column_name: convert_to_pyarrow_array(column_values, column_name)
                for column_name, column_values in columns.items()
            }
        )

    @staticmethod
    def _combine_tables(tables: List[Block]) -> Block:
        """合并多个 Arrow Table"""
        if len(tables) > 1:
            return transform_pyarrow.concat(tables, promote_types=True)
        else:
            return tables[0]

    @staticmethod
    def _empty_table() -> "pyarrow.Table":
        return pyarrow_table_from_pydict({})
```

### 4.2 transform_pyarrow.concat — schema unify 的核心

**文件：** `python/ray/data/_internal/arrow_ops/transform_pyarrow.py`

```python
def concat(
    blocks: List["pyarrow.Table"],
    *,
    promote_types: bool = False,
    preserve_order: Optional[bool] = None,
) -> "pyarrow.Table":
    if not blocks:
        return pa.table([])
    if len(blocks) == 1:
        return blocks[0]

    # Step 1: 收集所有 table 的 schema，尝试 unify
    schemas_to_unify = [b.schema for b in blocks]
    try:
        schema = unify_schemas(schemas_to_unify, promote_types=promote_types)
    except Exception as e:
        raise ArrowConversionError(
            f"Failed to unify schemas: {str(e)}\n"
            f"{'-' * 16}\n"
            f"Schemas:\n"
            f"{'-' * 16}\n"
            f"{schemas_to_unify}"
        ) from e

    # Step 2: 将 blocks 分为 schema 匹配和不匹配两组
    matched_blocks: List[pa.Table] = []
    mismatched_blocks: List[pa.Table] = []
    for block in blocks:
        if block.schema == schema:
            matched_blocks.append(block)
        else:
            mismatched_blocks.append(block)

    # Step 3: 快路径 — 所有 block schema 都一致
    if len(matched_blocks) == len(blocks):
        return pa.concat_tables(blocks)

    # Step 4: 慢路径 — 有 schema 不一致的 block
    if preserve_order is None:
        preserve_order = DataContext.get_current().execution_options.preserve_order

    if preserve_order or len(matched_blocks) <= 1:
        return _concat_mismatched_blocks(
            blocks, schema=schema, tensor_types=tensor_types,
            promote_types=promote_types,
        )

    # 非保序时：匹配的先 concat，再和 mismatched 合并
    single_matched_block = pa.concat_tables(matched_blocks)
    return _concat_mismatched_blocks(
        mismatched_blocks + [single_matched_block],
        schema=schema, ...
    )
```

### 4.3 unify_schemas — schema 统一算法

```python
def unify_schemas(
    schemas: List["pyarrow.Schema"], *, promote_types: bool = False
) -> "pyarrow.Schema":
    if not schemas:
        raise ValueError("No schemas provided for unify_schemas")

    # Step 1: 去重 — 相同 schema 只保留一份（100x 加速）
    schema_to_compare = schemas[0].remove_metadata()
    schemas_to_unify = [schemas[0]]
    for schema in schemas[1:]:
        if not schema.remove_metadata().equals(schema_to_compare):
            schemas_to_unify.append(schema)

    # Step 2: 只有一个 schema，直接返回
    if len(schemas_to_unify) == 1:
        return schemas_to_unify[0]

    # Step 3: 尝试 PyArrow 原生 unify
    try:
        return _unify_schemas_pyarrow(schemas_to_unify, promote_types)
    except (pyarrow.lib.ArrowTypeError, pyarrow.lib.ArrowInvalid) as e:
        pyarrow_exception = e

    # Step 4: PyArrow unify 失败，尝试调和（reconcile）分歧字段
    overrides = _reconcile_diverging_fields(schemas_to_unify, promote_types)
    if not overrides:
        raise pyarrow_exception  # 无法调和，抛出原始异常

    # Step 5: 应用 overrides 后重试 unify
    updated_schemas = []
    for schema in schemas_to_unify:
        for name, new_type in overrides.items():
            try:
                idx = schema.get_field_index(name)
                field = schema.field(name).with_type(new_type)
                schema = schema.set(idx, field)
            except KeyError:
                pass
        updated_schemas.append(schema)

    return _unify_schemas_pyarrow(updated_schemas, promote_types)
```

**unify 算法总结：**
1. 去重相同 schema（性能优化）
2. 尝试 `pa.unify_schemas()`（PyArrow 原生，支持类型提升）
3. 失败则尝试 `_reconcile_diverging_fields` 调和（主要处理 tensor/extension type）
4. 仍然失败则抛出 `ArrowTypeError`

**schema unify 的语义：**
- 列名取所有 schema 的并集
- 相同列名但类型不同时，尝试找到公共类型（type promotion）
- 缺失的列在对应 block 中补 null

---

## 5. BlockOutputBuffer 的 block 塑形与切割

**文件：** `python/ray/data/_internal/output_buffer.py`

### 5.1 核心结构

```python
class BlockOutputBuffer:
    def __init__(self, output_block_size_option: Optional[OutputBlockSizeOption]):
        self._output_block_size_option = output_block_size_option
        self._buffer = DelegatingBlockBuilder()  # 内部 builder
        self._finalized = False
        self._has_yielded_blocks = False
```

### 5.2 添加数据的三种方式

```python
def add(self, item: Any) -> None:
    """添加单行（dict）"""
    self._buffer.add(item)         # → DelegatingBlockBuilder.add() → ArrowBlockBuilder.add()

def add_batch(self, batch: DataBatch) -> None:
    """添加一个批次"""
    self._buffer.add_batch(batch)   # → 转为 block 后 add_block()

def add_block(self, block: Block) -> None:
    """添加一个完整 block"""
    self._buffer.add_block(block)   # → DelegatingBlockBuilder.add_block()
```

### 5.3 has_next() — 判断是否有完整 block 可输出

```python
def has_next(self) -> bool:
    if self._finalized:
        # 已 finalize：返回剩余数据
        return not self._has_yielded_blocks or self._buffer.num_rows() > 0
    elif self._output_block_size_option is None:
        # block sizing 被禁用：不增量输出，等所有数据吃完
        return False
    elif self._output_block_size_option.disable_block_shaping:
        # block shaping 禁用：有数据就输出
        return self._buffer.num_rows() > 0

    # 正常模式：超过大小或行数限制
    return self._exceeded_buffer_row_limit() or self._exceeded_buffer_size_limit()
```

### 5.4 next() — 获取下一个输出 block（含切割逻辑）

```python
def next(self) -> Block:
    # Step 1: build 当前 buffer 中的所有数据为一个 block
    block = self._buffer.build()
    accessor = BlockAccessor.for_block(block)

    block_remainder = None
    target_num_rows = None

    # Step 2: 检查是否需要按行数切割
    if self._exceeded_block_row_slice_limit(accessor):
        target_num_rows = self._max_num_rows_per_block()

    # Step 3: 检查是否需要按字节大小切割
    # 使用 1.5x 阈值（MAX_SAFE_BLOCK_SIZE_FACTOR），
    # 确保最后一个 block 至少是 target 的一半
    elif self._exceeded_block_size_slice_limit(accessor):
        assert accessor.num_rows() > 0, "Block may not be empty"
        num_bytes_per_row = accessor.size_bytes() / accessor.num_rows()
        target_num_rows = max(
            1, math.ceil(self._max_bytes_per_block() / num_bytes_per_row)
        )

    # Step 4: 执行切割
    if target_num_rows is not None and target_num_rows < accessor.num_rows():
        block = accessor.slice(0, target_num_rows, copy=False)
        block_remainder = accessor.slice(
            target_num_rows, accessor.num_rows(), copy=False
        )

    # Step 5: 重置 buffer，将切割余量放回
    self._buffer = DelegatingBlockBuilder()
    if block_remainder is not None:
        self._buffer.add_block(block_remainder)

    self._has_yielded_blocks = True
    return block
```

**切割逻辑总结：**
- 按 `target_num_rows_per_block` 切：超过即切
- 按 `target_max_block_size` 切：超过 1.5x 才切（`MAX_SAFE_BLOCK_SIZE_FACTOR = 1.5`）
- 切割使用 `accessor.slice(start, end, copy=False)`，**零拷贝，保留原 schema**

**关键结论：** 切割操作不会改变 schema。被切割出来的每个子 block 都与原 block 拥有相同的 schema。

---

## 6. MapTransformer 的三种 UDF 模式与 schema 推断

**文件：** `python/ray/data/_internal/execution/operators/map_transformer.py`

### 6.1 MapTransformer 的链式调用

```python
class MapTransformer:
    def apply_transform(
        self,
        input_blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> Iterable[Block]:
        last_transform = self._transform_fns[-1]
        if self.target_max_block_size_override is not None:
            last_transform.override_target_max_block_size(...)

        iter = input_blocks
        # 依次应用每个 transform_fn
        for transform_fn in self._transform_fns:
            iter = transform_fn(iter, ctx)
            if transform_fn._is_udf:
                iter = self._UDFTimingIterator(iter, self)
        return iter
```

每个 `MapTransformFn.__call__` 的处理流程：

```
blocks → _pre_process() → _apply_transform() → _post_process() → blocks
              ↓                  ↓                   ↓
         转换输入格式      执行 UDF / 变换函数      整形为 block
```

### 6.2 RowMapTransformFn — 逐行模式

```python
class RowMapTransformFn(MapTransformFn):
    def __init__(self, row_fn, ...):
        super().__init__(input_type=MapTransformFnDataType.Row, is_udf=is_udf, ...)
        self._row_fn = row_fn

    def _pre_process(self, blocks: Iterable[Block]) -> Iterable[MapTransformFnData]:
        return _RowBasedIterator(blocks)  # 将 block 迭代器转为行迭代器

    def _apply_transform(self, ctx, inputs):
        return self._row_fn(inputs, ctx)  # UDF 接收行迭代器，yield 行

    def _post_process(self, results):
        return self._shape_blocks(results)  # 通过 _BlockShapingIterator 整形
```

**Schema 推断路径：**
1. UDF yield 的每一行是 `dict`
2. `_BlockShapingIterator` 调用 `buffer.add(row)`
3. `buffer.add(row)` → `DelegatingBlockBuilder.add()` → `ArrowBlockBuilder.add()`
4. `ArrowBlockBuilder.add()` 做 schema union（缺失列补 None）
5. 同一个 buffer build 出的所有 block 拥有 **统一 schema**
6. 如果 buffer 输出多个 block（因切割），所有切割后的子 block schema 相同

**结论：Row 模式下，同一个 `_map_task` 中所有输出 block 的 schema 一致。**

### 6.3 BatchMapTransformFn — 批次模式

```python
class BatchMapTransformFn(MapTransformFn):
    def __init__(self, batch_fn, ...):
        super().__init__(input_type=MapTransformFnDataType.Batch, is_udf=is_udf, ...)
        self._batch_fn = batch_fn

    def _pre_process(self, blocks):
        # 将 blocks 转为 batches
        return batch_blocks(
            blocks=iter(blocks),
            batch_size=self._batch_size,
            batch_format=self._batch_format,
            ...
        )

    def _apply_transform(self, ctx, batches):
        return self._batch_fn(batches, ctx)  # UDF yield batch

    def _post_process(self, results):
        return self._shape_blocks(results)
```

**Schema 推断路径：**
1. UDF yield 的每个 batch 通过 `buffer.add_batch(batch)` 添加
2. `add_batch` → `BlockAccessor.batch_to_block(batch)` 转为 block
3. 然后 `add_block(block)` 追加到 builder
4. `add_block` 不做 schema 校验，但最终 `build()` 时 `_combine_tables` 会做 schema unify
5. 如果 unify 失败，会抛出 `ArrowConversionError`

**结论：Batch 模式下，同一个 `_map_task` 中所有输出 block 的 schema 会通过 unify_schemas 统一，如果无法统一则报错。**

### 6.4 BlockMapTransformFn — 块模式

```python
class BlockMapTransformFn(MapTransformFn):
    def __init__(self, block_fn, ..., disable_block_shaping=False, ...):
        super().__init__(
            input_type=MapTransformFnDataType.Block,
            is_udf=is_udf,
            output_block_size_option=output_block_size_option,
        )
        self._block_fn = block_fn
        self._disable_block_shaping = disable_block_shaping

    def _apply_transform(self, ctx, blocks):
        return self._block_fn(blocks, ctx)  # UDF yield block (pa.Table / pd.DataFrame)

    def _post_process(self, results):
        if self._disable_block_shaping:
            return results  # 【关键】直接返回，不经过 buffer！
        return self._shape_blocks(results)
```

**Schema 推断路径（两种情况）：**

**情况 A：`disable_block_shaping=False`（默认）**
1. UDF yield 的每个 block 通过 `buffer.add_block(block)` 添加
2. 经过 buffer 的整形和 `_combine_tables` 的 schema unify
3. 输出 block 的 schema 统一

**情况 B：`disable_block_shaping=True`**
1. UDF yield 的 block **直接返回**，不经过 buffer
2. **没有 schema unify**
3. 不同 yield 的 block 可能有不同 schema
4. 在 `_map_task` 中，每个 block 的 schema 通过 `BlockAccessor.for_block(block).schema()` 单独获取
5. 但 07baebd0 只传第一个 block 的 schema，后续都是 None

**结论：Block 模式 + `disable_block_shaping=True` 时，不同输出 block 可能有不同 schema，而 07baebd0 只传第一个 schema——后续 block 的 schema 丢失，这是一个潜在问题。**

### 6.5 _BlockShapingIterator — block 塑形迭代器

```python
class _BlockShapingIterator(Iterator[Block]):
    def __init__(self, results, input_type, output_block_size_option):
        self._results_iter = iter(results)
        self._buffer = BlockOutputBuffer(output_block_size_option)
        self._finalized = False

        # 根据输入类型选择 buffer 的 append 方法
        if input_type == MapTransformFnDataType.Block:
            self._append_buffer = self._buffer.add_block
        elif input_type == MapTransformFnDataType.Batch:
            self._append_buffer = self._buffer.add_batch
        else:
            self._append_buffer = self._buffer.add

    def __next__(self) -> Block:
        while True:
            # 有完整 block 可输出
            if self._buffer.has_next():
                return self._buffer.next()

            # 已结束
            elif self._finalized:
                raise StopIteration

            try:
                # 取下一个 UDF 输出，放入 buffer
                result = next(self._results_iter)
                self._append_buffer(result)
            except StopIteration:
                self._buffer.finalize()
                self._finalized = True
```

---

## 7. _map_task 的 schema 传递逻辑

**文件：** `python/ray/data/_internal/execution/operators/map_operator.py`

### 7.1 完整代码（07baebd0 之后）

```python
def _map_task(
    map_transformer: MapTransformer,
    data_context: DataContext,
    ctx: TaskContext,
    *blocks: Block,
    slices: Optional[List[BlockSlice]] = None,
    **kwargs: Dict[str, Any],
) -> Iterator[Union[Block, "BlockMetadataWithSchema"]]:
    task_start_s = time.perf_counter()
    blk_exec_stats_builder = BlockExecStats.builder()

    ctx.kwargs.update(kwargs)

    with DataContext.current(data_context), TaskContext.current(ctx):
        map_transformer.override_target_max_block_size(
            ctx.target_max_block_size_override
        )

        blocks_iter = _iter_sliced_blocks(blocks, slices) if slices else iter(blocks)

        # 只传第一个 block 的 schema，后续 block schema 设为 None
        yielded_schema: bool = False

        with MemoryProfiler(data_context.memory_usage_poll_interval_s) as profiler:
            for block in map_transformer.apply_transform(blocks_iter, ctx):
                # 每个 block 单独获取 metadata 和 schema
                block_meta = BlockAccessor.for_block(block).get_metadata()
                block_schema = BlockAccessor.for_block(block).schema()

                # Write operator 的特殊处理
                if "_write_stats_num_rows" in ctx.kwargs:
                    block_meta = replace(
                        block_meta,
                        num_rows=ctx.kwargs.pop("_write_stats_num_rows"),
                        size_bytes=ctx.kwargs.pop("_write_stats_size_bytes"),
                    )

                blk_exec_stats_builder.finish()

                # yield block（数据）
                gen_stats: StreamingGeneratorStats = yield block

                exec_stats = blk_exec_stats_builder.build(
                    block_ser_time_s=(
                        gen_stats.object_creation_dur_s if gen_stats else None
                    ),
                    udf_time_s=map_transformer.udf_time_s(reset=True),
                    task_idx=ctx.task_idx,
                    max_uss_bytes=profiler.estimate_max_uss(),
                )

                task_dur_s = time.perf_counter() - task_start_s

                # yield metadata+schema（元数据）
                bm = BlockMetadataWithSchema.from_metadata(
                    replace(
                        block_meta,
                        exec_stats=exec_stats,
                        task_exec_stats=TaskExecWorkerStats(
                            task_wall_time_s=task_dur_s
                        ),
                    ),
                    schema=block_schema if not yielded_schema else None,
                )
                yielded_schema = True

                # Reset trackers
                blk_exec_stats_builder = BlockExecStats.builder()
                profiler.reset()
```

### 7.2 yield 协议

`_map_task` 是一个 generator，交替 yield 两种对象：
1. `yield block` — 数据 block（`pyarrow.Table` 或 `pd.DataFrame`）
2. `yield BlockMetadataWithSchema` — 元数据 + schema

消费端 `DataOpTask._read_next()` 的逻辑：

```python
# physical_operator.py
class DataOpTask(OpTask):
    def _read_next(self):
        # 1. 获取 block 的 ObjectRef
        block_ref = next(self._streaming_gen)
        # 2. 获取对应的 BlockMetadataWithSchema
        meta_with_schema = ray.get(next(self._streaming_gen))
        # 3. 拆分 metadata 和 schema
        meta = meta_with_schema.metadata
        self._output_ready_callback(
            RefBundle(
                [(block_ref, meta)],
                owns_blocks=True,
                schema=meta_with_schema.schema,
            ),
        )
```

### 7.3 schema 传递流

```
_map_task 内:
  block_1 → schema=S1 → RefBundle(blocks=[(ref1, meta1)], schema=S1)
  block_2 → schema=None → RefBundle(blocks=[(ref2, meta2)], schema=None)
  block_3 → schema=None → RefBundle(blocks=[(ref3, meta3)], schema=None)

DataOpTask._read_next():
  每个 yield 对 → 构建 RefBundle，schema 来自 BlockMetadataWithSchema.schema

MapOperator._output_ready_callback():
  output = RefBundle([(block_ref, meta)], owns_blocks=True, schema=...)
  self._output_queue.add(output, key=task_index)
```

### 7.4 假设与风险

**07baebd0 的假设：**

> *each yielded block should have the same schema, since each one is a slice of the UDF's single output block, and we know that slicing preserves the schema*

**成立条件：**
- Row/Batch 模式：`_BlockShapingIterator` 通过 buffer 整形，所有 block 来自同一个 builder 或是同一个 block 的 slice → schema 一致
- Block 模式 + block shaping：经过 buffer，schema unify → 一致

**不成立条件：**
- Block 模式 + `disable_block_shaping=True`：UDF 直接 yield 不同 schema 的 block → **schema 不一致，但只传第一个**

---

## 8. OpState.add_output 的 schema 去重与校验

**文件：** `python/ray/data/_internal/execution/streaming_executor_state.py`

### 8.1 OpState 的 schema 状态

```python
class OpState:
    def __init__(self, op, inqueues):
        # ...
        self._schema: Optional["Schema"] = None           # 该 operator 迄今为止的 schema
        self._warned_on_schema_divergence: bool = False   # 是否已 warning 过 schema 不一致
```

### 8.2 add_output 完整逻辑（07baebd0 之后）

```python
def add_output(self, ref: RefBundle) -> None:
    """将 operator 产出的 bundle 移到其 output queue"""

    # 只有 schema 非 None 时才做 dedupe
    if ref.schema is not None:
        out_ref, diverged = dedupe_schemas_with_validation(
            self._schema,          # 之前的 schema
            ref,                   # 新的 RefBundle
            enforce_schemas=self.op.data_context.enforce_schemas,
        )

        if (
            diverged
            and not self._warned_on_schema_divergence
            and self.op.data_context.enforce_schemas
        ):
            warning_message = _build_schemas_mismatch_warning(
                self._schema, ref.schema
            )
            logger.warning(warning_message)

        ref = out_ref
        self._schema = ref.schema
        self._warned_on_schema_divergence |= diverged

    self.output_queue.append(ref)
    self.num_completed_tasks += 1
```

**注意：** `ref.schema is not None` 的判断意味着 07baebd0 中 schema=None 的 bundle（同一 task 的第 2+ 个 block）会直接跳过 dedupe，直接进入 output queue。这些 bundle 的 schema 字段为 None。

### 8.3 dedupe_schemas_with_validation 完整逻辑

```python
def dedupe_schemas_with_validation(
    old_schema: Optional["Schema"],
    bundle: "RefBundle",
    enforce_schemas: bool = False,
) -> Tuple["RefBundle", bool]:
    """Unify/Dedupe 两个 schema

    Args:
        old_schema: 之前的 schema。可以是 None（首次）。
        bundle: 新的 RefBundle。
        enforce_schemas: True → 允许 schema 不一致，返回 unified schema。
                         False → 不允许，保留旧 schema。

    Returns:
        (RefBundle, diverged): 带有去重后 schema 的 bundle + 是否发生分歧
    """
    diverged = False

    from ray.data.block import _is_empty_schema

    # 旧 schema 为空 → 直接返回新 bundle（首次）
    if _is_empty_schema(old_schema):
        return bundle, diverged

    # schema 相等 → 直接返回（快路径）
    if old_schema == bundle.schema:
        return bundle, diverged

    # schema 不一致
    diverged = True

    if enforce_schemas:
        # unify 两个 schema（允许 type promotion）
        old_schema = unify_schemas_with_validation([old_schema, bundle.schema])

    # enforce_schemas=False(默认): 用旧 schema 替换新 schema
    return (
        RefBundle(
            bundle.blocks,
            schema=old_schema,       # 保留旧 schema
            owns_blocks=bundle.owns_blocks,
            output_split_idx=bundle.output_split_idx,
            _cached_object_meta=bundle._cached_object_meta,
            _cached_preferred_locations=bundle._cached_preferred_locations,
        ),
        diverged,
    )
```

### 8.4 不同 enforce_schemas 下的行为

| 场景 | `enforce_schemas=False`（默认） | `enforce_schemas=True` |
|---|---|---|
| schema 一致 | 直接通过 | 直接通过 |
| schema 不一致 | **新 schema 被旧 schema 覆盖** | 调用 `unify_schemas_with_validation` 合并 |
| warning | **不打印** | 打印 warning（仅第一次） |
| 结果 | `RefBundle.schema = 第一个 task 的 schema` | `RefBundle.schema = unified schema` |

**`enforce_schemas` 默认值：**

```python
# context.py
DEFAULT_ENFORCE_SCHEMAS = env_bool("RAY_DATA_ENFORCE_SCHEMAS", False)

class DataContext:
    enforce_schemas: bool = DEFAULT_ENFORCE_SCHEMAS
```

### 8.5 潜在问题

1. **默认模式下 schema 不一致被静默掩盖**：`diverged=True` 但 warning 只在 `enforce_schemas=True` 时打印
2. **下游拿到的 schema 与实际数据不匹配**：
   - Task 2 有 Task 1 没有的列 → 下游 schema 里没有这些列
   - Task 2 缺少某些列 → 下游以为有这些列但实际数据里缺失
3. **`dataset.schema()` 返回的是第一个 task 的 schema** — 可能不代表所有数据

---

## 9. BlockMetadataWithSchema 的序列化与 LRU Cache

**文件：** `python/ray/data/block.py`

### 9.1 序列化（`__getstate__`）— 发送端（worker）

```python
def __getstate__(self) -> Dict[str, Any]:
    # 获取所有字段
    state = {f.name: getattr(self, f.name) for f in fields(BlockMetadataWithSchema)}

    # pa.Schema 序列化为 Arrow IPC bytes（跨进程传输）
    if isinstance(self.schema, pa.Schema):
        state["schema"] = self.schema.serialize().to_pybytes()
    else:
        # PandasBlockSchema 或 None 保持原样
        state["schema"] = self.schema

    return state
```

**为什么序列化为 bytes：** `pa.Schema` 对象不能直接 pickle，需要通过 `schema.serialize().to_pybytes()` 转为 Arrow IPC 格式的 bytes 进行跨进程传输。

### 9.2 反序列化（`__setstate__`）— 接收端（scheduler）

```python
def __setstate__(self, state: Dict[str, Any]):
    schema_val: bytes | bytearray | Schema | None = state["schema"]
    if isinstance(schema_val, (bytes, bytearray)):
        # bytearray 不可哈希，转为 bytes 后才能做 LRU cache key
        if isinstance(schema_val, bytearray):
            schema_val = bytes(schema_val)
        # 通过 LRU cached 函数反序列化
        state["schema"] = _read_arrow_schema_cached(schema_val)
    # PandasBlockSchema 或 None 不需要反序列化
    self.__dict__.update(state)
```

### 9.3 LRU Cache 实现

```python
@functools.lru_cache(maxsize=128)
def _read_arrow_schema_cached(schema_bytes: bytes) -> "pa.Schema":
    return pa.ipc.read_schema(pa.BufferReader(schema_bytes))
```

**工作原理：**

1. Python 标准库 `@functools.lru_cache`，基于 dict 实现
2. **cache key** = `schema_bytes`（`bytes` 类型，可哈希）
3. **cache value** = `pa.Schema` 对象
4. **maxsize=128**：最多缓存 128 个不同的 schema

**命中条件：**
- 同一个 operator 的所有 task 产出的 schema bytes 完全相同
- 第一次调用：miss → 执行 `pa.ipc.read_schema` 反序列化
- 后续相同 bytes：hit → 直接返回缓存的 `pa.Schema` 对象

**性能收益（3c60006e 的 benchmark 数据）：**

| 指标 | Before | After |
|---|---|---|
| `__setstate__` → `pa.ipc.read_schema` 占 scheduler 线程 | **60.2%** | 接近 0% |
| 1000 actor release test (task time) | 6.35s | 2.80s |
| 5000 actor release test (task time) | 19.56s | 10.68s |

### 9.4 为什么用 `bytes` 做 key 而非 `pa.Schema`

1. **`pa.Schema` 不是确定性的可哈希对象**：虽然 `pa.Schema` 实现了 `__hash__`，但不同 schema 对象的 hash 冲突可能导致错误缓存
2. **序列化后的 bytes 是确定性表示**：相同的 schema 一定产生相同的 bytes（IPC 格式），做 key 最可靠
3. **bytes 是不可变且可哈希的**：满足 `lru_cache` 的要求

### 9.5 bytearray 的处理

```python
if isinstance(schema_val, bytearray):
    schema_val = bytes(schema_val)  # bytearray 不可哈希，必须转 bytes
```

**为什么可能出现 bytearray：** 某些替代序列化路径（如第三方 pickle 协议）可能使用 `bytearray` 而非 `bytes`。转换后确保能用 `lru_cache`。

### 9.6 缓存生命周期

- **无主动清理**：进程生命周期内有效
- **`maxsize=128`**：超过 128 个不同 schema 时，LRU 淘汰最久未使用的
- **正常场景**：一个 topology 中 operator 数量远小于 128，每个 operator 只有 1 种 schema → 永远不会触发淘汰

---

## 10. Schema 推断全链路总结与潜在问题

### 10.1 全链路 Schema 流转图

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Worker (_map_task)                            │
│                                                                     │
│  UDF yield ──→ MapTransformFn._post_process() ──→ Block[]          │
│                    │                                                │
│                    ├─ Row/Batch 模式: _BlockShapingIterator         │
│                    │   ├─ buffer.add(row/batch)                    │
│                    │   │   └─ TableBlockBuilder.add()              │
│                    │   │       └─ Schema Union (缺列补 None)       │
│                    │   └─ buffer.next() → slice (保 schema)         │
│                    │                                                │
│                    └─ Block 模式 + disable_block_shaping:           │
│                        └─ 直接返回，无 schema unify ⚠️              │
│                                                                     │
│  for block in apply_transform():                                    │
│      block_schema = BlockAccessor.for_block(block).schema()         │
│      yield block                                                    │
│      yield BlockMetadataWithSchema(                                 │
│          schema=block_schema if not yielded_schema else None  ────→ │
│      )                                    只传第一个 block 的 schema│
│      yielded_schema = True                                          │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ pickle 传输
                                    │ __getstate__: pa.Schema → bytes
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     Scheduler (StreamingExecutor)                    │
│                                                                     │
│  DataOpTask._read_next():                                           │
│      meta_with_schema = ray.get(next(self._streaming_gen))          │
│      └─ __setstate__: bytes → _read_arrow_schema_cached() (LRU)    │
│                                                                     │
│      RefBundle(                                                     │
│          blocks=[(ref, meta)],                                      │
│          schema=meta_with_schema.schema  ← 第一个=S1, 后续=None     │
│      )                                                              │
│                                                                     │
│  MapOperator._output_ready_callback():                              │
│      self._output_queue.add(output)  → 加入 operator 输出队列       │
│                                                                     │
│  OpState.add_output(ref):                                           │
│      if ref.schema is not None:    ← 跳过 schema=None 的 bundle     │
│          dedupe_schemas_with_validation(self._schema, ref, ...)      │
│          ├─ 首次: self._schema = ref.schema (S1)                    │
│          ├─ 一致: 直接通过                                          │
│          └─ 不一致 + enforce=False: 用旧 schema 覆盖 ⚠️            │
│          └─ 不一致 + enforce=True:  unify + warning                 │
│                                                                     │
│      self.output_queue.append(ref) → 传递给下游 operator             │
└─────────────────────────────────────────────────────────────────────┘
```

### 10.2 各层级的 Schema 推断策略

| 层级 | 机制 | 策略 |
|---|---|---|
| `TableBlockBuilder.add()` | Schema Union | 新列补 None、缺列补 None，取并集 |
| `TableBlockBuilder.build()` → `_combine_tables` | Schema Unify | `unify_schemas()` → `pa.unify_schemas()` |
| `BlockOutputBuffer.next()` → `slice` | 保留原 Schema | 切割不改变 schema |
| `_map_task` → `yielded_schema` | 取第一个 | 只传第一个 block 的 schema，后续 None |
| `_merge_ref_bundles` | `_take_first_non_empty_schema` | 取第一个非空 schema |
| `OpState.add_output` → `dedupe_schemas_with_validation` | 默认保留旧 | `enforce=False`: 旧覆盖新；`enforce=True`: unify |
| `dataset.schema()` | 取 operator schema | 最终返回第一个 task 的 schema |

### 10.3 潜在问题汇总

#### 问题 1：Block 模式 + `disable_block_shaping=True` 时 schema 丢失

**位置：** `_map_task` + `BlockMapTransformFn`

**场景：** UDF 直接 yield 多个不同 schema 的 `pa.Table`，且 `disable_block_shaping=True`

**问题：** 07baebd0 只传第一个 block 的 schema，后续 block 的 `schema=None`，在 `OpState.add_output` 中被跳过

**影响：** 下游 operator 看到的 schema 只是第一个 block 的，无法感知后续 block 的 schema 变化

**严重度：** 中。目前 Ray Data 内建的 BlockMapTransformFn 使用较少，且大多数 UDF 不会产出不同 schema 的 block

#### 问题 2：不同 task 间 schema 不一致被静默掩盖

**位置：** `OpState.add_output` → `dedupe_schemas_with_validation`

**场景：** Task 1 产出 schema S1，Task 2 产出 schema S2 ≠ S1

**问题：** 默认 `enforce_schemas=False` 时：
- S2 被静默替换为 S1
- 不打印 warning
- `diverged=True` 但无法观察到

**影响：**
- `dataset.schema()` 返回 S1，不代表所有数据
- 下游 operator 认为数据是 S1，但 Task 2 的实际数据可能是 S2
- 缺失列在 `TableBlockBuilder.add()` 时会补 None，所以"通常不会 crash"，但数据可能丢失

**严重度：** 中高。对于 schema 确实会变化的场景（如动态列），这是一个 silent data corruption

#### 问题 3：`_merge_ref_bundles` 不校验 schema 一致性

**位置：** `map_operator.py` → `_merge_ref_bundles`

**场景：** 多个 RefBundle 被 merge 时

**问题：** 使用 `_take_first_non_empty_schema`，只取第一个非空 schema，不做校验

```python
# 代码中已有 TODO 标注
# TODO: Reconcile the schemas rather than taking the first non-empty schema.
```

**严重度：** 低。merge 通常发生在 operator 内部的 rebundle 逻辑中，且被 merge 的 bundle 通常来自同一个 task

#### 问题 4：LRU cache 在极端场景下的正确性

**位置：** `_read_arrow_schema_cached`

**场景：** 同一个 schema 被序列化为不同的 bytes（理论上不应该发生，但某些 Arrow 版本可能有非确定性序列化）

**问题：** 如果同一 schema 产生不同 bytes，LRU cache 无法命中，退化到每次都反序列化

**严重度：** 低。Arrow IPC 序列化是确定性的，此问题在实践中不会发生

### 10.4 建议

1. **对于问题 1**：在 `BlockMapTransformFn(disable_block_shaping=True)` 场景下，应该对每个 yield 的 block 都传 schema，而非只传第一个。或者至少在发现 schema 不一致时 warning

2. **对于问题 2**：
   - 建议在 `dedupe_schemas_with_validation` 中，即使 `enforce_schemas=False`，也应该在 `diverged=True` 时打印 warning（降低级别为 debug 或 info）
   - 或者考虑改变默认行为：`enforce_schemas=True` 作为默认值

3. **对于问题 3**：将 `_take_first_non_empty_schema` 改为校验所有非空 schema 是否一致

4. **对于 LRU cache**：当前实现合理，无需修改
