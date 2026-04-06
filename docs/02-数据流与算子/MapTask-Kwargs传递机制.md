# Map Task Kwargs 参数传递机制

## 概述

本文档描述 `add_map_task_kwargs_fn` 注册的参数如何传递到 `TaskContext.kwargs` 的完整流程。

## 使用场景

以 checkpoint 功能为例，在 `plan_read_op_with_checkpoint_filter` 中注册额外的 kwargs：

```python
# python/ray/data/_internal/planner/checkpoint/plan_read_op.py:43-45
physical_op.add_map_task_kwargs_fn(
    lambda: {CHECKPOINTED_IDS_KWARG_NAME: load_checkpoint()}
)
```

## 参数传递流程

```
┌─────────────────────────────────────────────────────────────────────────┐
│  1. 注册阶段                                                              │
│  add_map_task_kwargs_fn(fn) → _map_task_kwargs_fns.append(fn)           │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  2. 收集阶段                                                              │
│  get_map_task_kwargs() → 遍历 _map_task_kwargs_fns 并合并结果              │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  3. 调用阶段                                                              │
│  _map_task(..., **kwargs) → kwargs 作为函数参数传入                        │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  4. 注入阶段                                                              │
│  ctx.kwargs.update(kwargs) → 合并到 TaskContext.kwargs                   │
└─────────────────────────────────────────────────────────────────────────┘
```

## 详细代码分析

### 1. 注册回调函数

**文件**: `python/ray/data/_internal/execution/operators/map_operator.py:259-263`

```python
def add_map_task_kwargs_fn(self, map_task_kwargs_fn: Callable[[], Dict[str, Any]]):
    """Add a callback function that generates additional kwargs for the map tasks.
    In the map tasks, the kwargs can be accessible via `TaskContext.kwargs`.
    """
    self._map_task_kwargs_fns.append(map_task_kwargs_fn)
```

- 回调函数签名: `Callable[[], Dict[str, Any]]`
- 存储位置: `self._map_task_kwargs_fns` 列表

### 2. 收集所有 kwargs

**文件**: `python/ray/data/_internal/execution/operators/map_operator.py:279-287`

```python
def get_map_task_kwargs(self) -> Dict[str, Any]:
    """Get the kwargs for the map task.
    Subclasses should pass the returned kwargs to the map tasks.
    In the map tasks, the kwargs can be accessible via `TaskContext.kwargs`.
    """
    kwargs = self._map_task_kwargs.copy()      # 复制静态 kwargs
    for fn in self._map_task_kwargs_fns:       # 遍历所有注册的回调
        kwargs.update(fn())                     # 调用回调并合并结果
    return kwargs
```

### 3. 传递给 _map_task 函数

**文件**: `python/ray/data/_internal/execution/operators/map_operator.py:737-744`

```python
def _map_task(
    map_transformer: MapTransformer,
    data_context: DataContext,
    ctx: TaskContext,
    *blocks: Block,
    slices: Optional[List[BlockSlice]] = None,
    **kwargs: Dict[str, Any],   # ← kwargs 在这里接收
) -> Iterator[Union[Block, "BlockMetadataWithSchema"]]:
```

### 4. 注入到 TaskContext

**文件**: `python/ray/data/_internal/execution/operators/map_operator.py:763`

```python
ctx.kwargs.update(kwargs)  # 将 kwargs 合并到 TaskContext.kwargs
```

## 在 Transform 函数中访问

在 map transformer 的处理函数中，可以通过 `ctx.kwargs` 访问这些参数：

```python
def filter_checkpointed_rows_for_blocks(
    blocks: Iterable[Block],
    ctx: TaskContext,
    checkpoint_config: CheckpointConfig,
) -> Iterable[Block]:
    # 通过 ctx.kwargs 访问 checkpointed_ids
    checkpointed_ids_ref = ctx.kwargs.get(CHECKPOINTED_IDS_KWARG_NAME)
    if checkpointed_ids_ref is not None:
        checkpointed_ids = ray.get(checkpointed_ids_ref)
        # ... 使用 checkpointed_ids 过滤数据
```

## 关键类关系

```
MapOperator
├── _map_task_kwargs: Dict[str, Any]        # 静态 kwargs (构造时传入)
├── _map_task_kwargs_fns: List[Callable]    # 动态 kwargs 回调函数列表
├── add_map_task_kwargs_fn(fn)              # 注册回调
└── get_map_task_kwargs() -> Dict           # 收集所有 kwargs

TaskContext
└── kwargs: Dict[str, Any]                  # 存储合并后的 kwargs
```

## 总结

| 步骤 | 方法/位置 | 说明 |
|------|-----------|------|
| 1 | `add_map_task_kwargs_fn()` | 注册返回 Dict 的回调函数 |
| 2 | `get_map_task_kwargs()` | 调用所有回调，合并结果 |
| 3 | `_map_task(**kwargs)` | 作为函数参数传递 |
| 4 | `ctx.kwargs.update(kwargs)` | 注入到 TaskContext |
| 5 | `ctx.kwargs[key]` | 在 transform 函数中访问 |


def on_task_output_generated(self, task_index: int, output: RefBundle) 在这里统计输出的指标