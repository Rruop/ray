# Ray Data Schema 优化与 Actor 调度优化 Commit 分析

> 本文档详细分析了 Ray Data 中 schema 优化链（4 个 commit）和 actor 调度优化链（2 个 commit）及代码重构 commit 的工作逻辑和关联关系，以及 `ObjectReconstructionFailedError` 错误的根因分析。

---

## 一、Schema 优化链（4 个递进 Commit）

### 1.1 `a1b9dc62` — Remove Schema From BlockMetadata (#53454)

**作者**: iamjustinhsu
**日期**: 2025-06-12
**影响范围**: 84 files, +870 / -431

#### 目标

将 schema 从 `BlockMetadata` 中移除，提升到更高层级管理。

#### 问题背景

之前每个 `BlockMetadata` 都携带一个 `schema` 字段。当 `RefBundle` 包含多个 block 时，相同的 schema 被重复存储多次，造成内存冗余和序列化/反序列化开销成倍放大。

#### 核心改动

**新建 `BlockMetadataWithSchema` 类**（`block.py:227`）：

```python
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
            input_files=self.input_files,
        )
```

**`RefBundle` 新增 `schema` 字段**（`ref_bundle.py:34`）：

```python
# The schema of the blocks in this bundle. This is optional, and may be None
# if blocks are empty.
schema: Optional["Schema"]
```

**`DataOpTask` 适配**（`physical_operator.py:138`）：

```python
# 改动前
meta = ray.get(next(self._streaming_gen))
RefBundle([(block_ref, meta)], owns_blocks=True)

# 改动后
meta_with_schema = ray.get(next(self._streaming_gen))
meta = meta_with_schema.metadata
RefBundle([(block_ref, meta)], owns_blocks=True, schema=meta_with_schema.schema)
```

**`OpState` 新增 schema 去重**（`streaming_executor_state.py:200`）：

新增 `_schema` 状态和 `dedupe_schemas_with_validation()` 函数。当新的 `RefBundle` 进入 output queue 时，检查 schema 是否与已有的一致，一致则复用旧 schema（去重），不一致则发出警告或进行 unify。

#### 关键事实：没有减少传输量

每个 block 仍然 yield 一个 `BlockMetadataWithSchema`，仍然通过 `ray.get()` 传输 schema。每个 task yield 几个 block，就传几个 schema。**网络传输量完全没变。**

#### 实际收益

1. **架构清理**：schema 概念上属于 dataset/operator 级别，不属于单个 block
2. **为后续优化铺路**：只有先让 schema 从 `BlockMetadata` 中独立出来，后续 commit 才能做到：
   - `ab9a7d7d`：只在第一个 block yield schema
   - `81bda34d`：自定义 `__getstate__`/`__setstate__`
   - `6ecde5ed`：LRU cache 缓存反序列化
3. **`BlockMetadata` 变轻**：在不需要 schema 的场景中传递的元数据更小

---

### 1.2 `ab9a7d7d` — Yield only first schema in _map_task (#62720)

**作者**: iamjustinhsu
**日期**: 2026-04-22
**影响范围**: 2 files, +29 / -20

#### 目标

在 map task 中只 yield 第一个 block 的 schema，后续 block 不再携带 schema。

#### 问题背景

同一个 task 产出的所有 block 都是同一 UDF 输出 block 的切片，切片操作保持 schema 不变，所以后续 block 的 schema 完全冗余。

#### 核心改动

**`map_operator.py`**（`_map_task` 函数）：

```python
# 新增标志
yielded_schema: bool = False

# 第一个 block yield 时 schema=block_schema，之后 schema=None
schema=block_schema if not yielded_schema else None,
yielded_schema = True
```

**`streaming_executor_state.py`**（`OpState.add_output`）：

```python
if ref.schema is not None:
    out_ref, diverged = dedupe_schemas_with_validation(...)
    ...
    ref = out_ref
    self._schema = ref.schema
```

在处理 schema 去重之前加入 `if ref.schema is not None` 判断。如果 schema 为 None，跳过去重逻辑，直接入队。

#### 性能效果

100 节点集群上约 **25%** 的反序列化时间降低。

---

### 1.3 `81bda34d` — Regular pickle before task return to decrease deserialization on driver (#62726)

**作者**: iamjustinhsu
**日期**: 2026-04-29
**影响范围**: 6 files, +35 / -6

#### 目标

避免 Ray 的 cloudpickle 反序列化 `BlockMetadataWithSchema`，改用标准 `pickle`。

#### 问题背景

map task 通过 `yield` 返回 `BlockMetadataWithSchema` 对象，Ray 内部使用 `cloudpickle` 来序列化/反序列化。`cloudpickle` 比 `pickle` 慢得多（它需要处理嵌套函数、闭包等），而 `BlockMetadataWithSchema` 是一个简单的 dataclass，完全不需要 cloudpickle 的能力。

#### 核心改动

**`block.py` — 新增 `__getstate__`/`__setstate__`**：

```python
def __getstate__(self) -> Dict[str, Any]:
    state = {f.name: getattr(self, f.name) for f in fields(BlockMetadataWithSchema)}
    if isinstance(self.schema, pa.Schema):
        state["schema"] = self.schema.serialize().to_pybytes()
    else:
        state["schema"] = self.schema
    return state

def __setstate__(self, state: Dict[str, Any]):
    schema_val: bytes | Schema | None = state["schema"]
    if isinstance(schema_val, (bytes, bytearray)):
        state["schema"] = pa.ipc.read_schema(pa.BufferReader(schema_val))
    self.__dict__.update(state)
```

**所有 yield 点**（`map_operator.py`、`hash_shuffle.py`、`gpu_shuffle/hash_shuffle.py`）：

```python
# 改动前
yield BlockMetadataWithSchema(...)

# 改动后
bm = BlockMetadataWithSchema(...)
yield pickle.dumps(bm)
```

**`physical_operator.py`（driver 侧）**：

```python
# 改动前
meta_with_schema: "BlockMetadataWithSchema" = ray.get(...)

# 改动后
meta_with_schema_bytes: bytes = ray.get(...)
meta_with_schema: "BlockMetadataWithSchema" = pickle.loads(meta_with_schema_bytes)
```

#### 关键洞察

Ray 在 worker → driver 传输时对 ObjectRef 使用 cloudpickle，通过提前 `pickle.dumps()` 将 dataclass 转为 bytes，driver 侧只需 `pickle.loads()` 这个 bytes，完全绕过了 cloudpickle。

#### 性能效果

driver 侧 `pickle.loads` 非常快（仅约 200ms），对比 cloudpickle 大幅降低。

---

### 1.4 `6ecde5ed` — Cache deserialized Arrow schemas in BlockMetadataWithSchema (#63462)

**作者**: Xinyuan
**日期**: 2026-05-19
**影响范围**: 3 files, +182 / -2

#### 目标

缓存 Arrow schema 的反序列化结果，避免重复解析相同的 IPC bytes。

#### 问题背景

`81bda34d` 引入的 `__setstate__` 中，每次 `pickle.loads` 都会调用 `pa.ipc.read_schema()` 重新解析 IPC bytes。在调度器线程的热路径上，每个完成的 task 都会触发一次。对于宽 schema（数百列，尤其是带有 `ArrowTensorType` 等 extension type），这成为调度器线程的瓶颈：

- `BlockMetadataWithSchema.__setstate__` → `pa.ipc.read_schema` 占调度器线程 **60.2%** 的时间
- 同一 operator 的所有 task 携带完全相同的 schema bytes，却被重复解析数千次

#### 核心改动

**新增 `_read_arrow_schema_cached` 函数**（`block.py:313`）：

```python
@functools.lru_cache(maxsize=128)
def _read_arrow_schema_cached(schema_bytes: bytes) -> "pa.Schema":
    return pa.ipc.read_schema(pa.BufferReader(schema_bytes))
```

**修改 `__setstate__`**：

```python
def __setstate__(self, state: Dict[str, Any]):
    schema_val: bytes | bytearray | Schema | None = state["schema"]
    if isinstance(schema_val, (bytes, bytearray)):
        if isinstance(schema_val, bytearray):
            schema_val = bytes(schema_val)
        state["schema"] = _read_arrow_schema_cached(schema_val)
    self.__dict__.update(state)
```

#### 性能效果（1000 actor，600 列 schema 测试）

| | 500 actors | 1000 | 2000 | 5000 |
|---|---|---|---|---|
| Before (actor) | 5.84s | 12.28s | 25.24s | 67.98s |
| After (actor) | 2.42s | 5.07s | 10.86s | 26.93s |
| **提速** | **2.4x** | **2.4x** | **2.3x** | **2.5x** |

---

### 1.5 四个 Commit 的关联总结

```
a1b9dc62 (架构重构)
    ↓  将 schema 从 BlockMetadata 移出，建立 BlockMetadataWithSchema + RefBundle.schema 体系
ab9a7d7d (减少产出)
    ↓  同一 task 只 yield 第一个 schema，后续为 None → 减少 schema 传输量
81bda34d (序列化优化)
    ↓  自定义 __getstate__/__setstate__ + pickle.dumps/loads 绕过 cloudpickle
6ecde5ed (反序列化缓存)
    ↓  LRU cache 缓存 pa.Schema 反序列化，消除重复 IPC 解析
```

| | a1b9dc62 | ab9a7d7d | 81bda34d | 6ecde5ed |
|---|---|---|---|---|
| **减少传输量？** | 否 | 是（N个block只传1个schema） | 否（等量，但改用pickle） | 否（等量，但缓存反序列化） |
| **实际作用** | 架构重构，解耦schema | 减少schema传输次数 | 绕过cloudpickle | 避免重复IPC解析 |

它们遵循一个**递进优化**模式：
1. **结构层面**：把 schema 从每 block 提升到每 bundle，消除冗余存储
2. **传输层面**：同一 task 内只传一次 schema，减少网络/序列化量
3. **协议层面**：用标准 pickle 替代 cloudpickle，自定义 schema 的二进制序列化
4. **计算层面**：缓存反序列化结果，同一 schema bytes 只解析一次

最终效果：从每个 block 都带 schema + cloudpickle 序列化 + 重复 IPC 解析，优化到了**每个 operator 只传一次 schema + 标准 pickle + LRU 缓存复用**。在大规模集群和宽 schema 场景下，调度器线程开销降低了数倍。

---

## 二、Actor 调度优化链（2 个递进 Commit）

### 2.1 `59c4372a` — Heap based actor ranking (#62114)

**作者**: iamjustinhsu
**日期**: 2026-03-26
**影响范围**: 3 files, +305 / -64

#### 背景：Actor Pool 的调度问题

Ray Data 的 `ActorPoolMapOperator` 使用 actor pool 来执行 map task。当有新 bundle 需要调度时，需要从 pool 中选出一个最合适的 actor。选择标准有两个维度：

1. **Locality（数据局部性）**：优先选择 bundle 数据所在节点上的 actor，减少网络传输
2. **Busyness（忙碌度）**：优先选择 `num_tasks_in_flight` 最少的 actor

#### 改动前的原始代码

```python
def select_actors(self, bundle, actor_locality_enabled):
    available_actors = self._schedulable_actors()  # O(M) 过滤
    if not available_actors:
        return None
    ranks = self._rank_actors(available_actors, bundle)  # O(M)
    target_actor_idx = min(range(len(available_actors)), key=...)
    return available_actors[target_actor_idx]

def _schedulable_actors(self) -> List[ActorHandle]:
    return [
        actor for actor, state in self._running_actors.items()
        if state.num_tasks_in_flight < self.max_tasks_in_flight_per_actor()
        and not state.is_restarting
    ]  # O(M)

def _rank_actors(self, actors, bundle):
    # 对每个 actor 计算 (locality_rank, num_tasks_in_flight)
    # locality_rank = -total_bytes（节点上数据越多，rank 越小，越优先）
    # 没有数据在本节点的 actor，locality_rank = INT32_MAX
    return [(locs_priorities.get(state.actor_location, INT32_MAX),
             state.num_tasks_in_flight) for ...]
```

**问题**：每次选 actor 都要遍历全部 actor（O(M)），计算 rank，再取最小值。对于 N 个 bundle、M 个 actor，总复杂度 **O(N × M)**。

#### 核心改动

**1. 新增 `heapdict` 数据结构**

Vendored 了第三方库 `heapdict`（`python/ray/data/_internal/utils/heapdict.py`），这是一个支持 **O(log M) 更新** 的最小堆字典：
- `__setitem__`：插入或更新 key 的优先级，O(log M)
- `peekitem()`：获取堆顶（最小值），O(1)
- 支持通过 key 直接修改优先级后自动 re-heapify

**2. 新增两个索引结构**

```python
# 全局 heap：actor → num_tasks_in_flight
# 堆顶就是最空闲的 actor
self._alive_actors_to_in_flight_tasks_heap: heapdict[ActorHandle, _ActorRank]

# 节点 → 该节点上所有 alive actor 的集合
self._alive_node_to_actor_map: DefaultDict[NodeIdStr, Set[ActorHandle]]
```

关键维护逻辑在 `_update_rank` 中：
- actor alive → 加入全局 heap 和 node map
- actor restarting/dead → 从全局 heap 删除（node map 在 `refresh_actor_state` 开头整体 clear）

**3. 改写 `select_actors`**

```python
def select_actors(self, bundle, actor_locality_enabled):
    # O(1) 检查是否有空闲 actor
    _, least_busy_rank = self._alive_actors_to_in_flight_tasks_heap.peekitem()
    if least_busy_rank >= self.max_tasks_in_flight_per_actor():
        return None

    target_actor = None
    if bundle is not None and actor_locality_enabled:
        target_actor = self._find_actor_with_locality(bundle)  # 走 locality 路径

    if target_actor is None:
        target_actor, _ = self._alive_actors_to_in_flight_tasks_heap.peekitem()  # O(1) fallback

    return target_actor
```

**4. `_find_actor_with_locality`（locality 路径）**

```python
def _find_actor_with_locality(self, bundle):
    preferred_locs = bundle.get_preferred_object_locations()
    actor_ranks = []
    # 遍历每个偏好节点 → 该节点上每个 alive actor
    for node_id, total_bytes in preferred_locs.items():
        for actor in self._alive_node_to_actor_map[node_id]:
            num_tasks_in_flight = self._running_actors[actor].num_tasks_in_flight
            if num_tasks_in_flight >= max_tasks:
                continue
            actor_ranks.append((actor, -total_bytes, num_tasks_in_flight))
    # 全局最优：locality 最高 + 最空闲
    return min(actor_ranks, key=lambda x: (x[1], x[2]))[0]
```

用 `_alive_node_to_actor_map` 快速定位节点上的 actor，但仍需遍历节点上的**所有 actor** 来找最空闲的。

代码中留了 TODO：
> `TODO(Justin): This can optimized further to node_id -> heap[actor, num_tasks_in_flight]`

这正是下一个 commit 做的事。

#### 复杂度分析

| | 改动前 | 59c4372a |
|---|---|---|
| **无 locality** | O(M) 扫描 | O(1) peek |
| **有 locality** | O(M) 全局扫描 | O(N_nodes × M_per_node) |
| **rank 更新** | 无（每次重新算） | O(log M) |

---

### 2.2 `1de3b19e` — Rank actors per node in a heap (#62309)

**作者**: iamjustinhsu
**日期**: 2026-04-06
**影响范围**: 1 file, +43 / -49

#### 核心改动

将每节点上的 actor 集合从 `Set[ActorHandle]` 升级为 `heapdict[ActorHandle, _ActorRank]`，实现 O(1) 找到每节点最空闲的 actor。

**1. 数据结构替换**

```python
# 之前
self._alive_node_to_actor_map: DefaultDict[NodeIdStr, Set[ActorHandle]]

# 之后
self._alive_node_to_actor_heap: DefaultDict[NodeIdStr, heapdict[ActorHandle, _ActorRank]]
```

**2. `_find_actor_with_locality` 重写**

```python
def _find_actor_with_locality(self, bundle):
    max_tasks = self.max_tasks_in_flight_per_actor()
    # 按节点上数据量降序排列偏好节点
    for node_id, _total_bytes in sorted(
        preferred_locs.items(), key=lambda item: (-item[1], item[0])
    ):
        node_heap = self._alive_node_to_actor_heap.get(node_id)
        if not node_heap:
            continue
        actor, rank = node_heap.peekitem()  # O(1) 拿到该节点最空闲的 actor
        if rank < max_tasks:
            return actor
    return None
```

不再收集所有候选 actor 再全局排序，而是按 locality 优先级遍历节点，对每个节点 O(1) peek 最空闲 actor，找到第一个有容量的就返回，**提前终止**。

**3. rank 更新同步**

每次 actor 的 `num_tasks_in_flight` 变化时，需要同时更新两个 heap：

```python
# 全局 heap
self._alive_actors_to_in_flight_tasks_heap[actor] = rank
# 对应节点的 heap
node_heap = self._alive_node_to_actor_heap.get(state.actor_location)
if node_heap is not None and actor in node_heap:
    node_heap[actor] = rank  # O(log M_per_node)
```

#### 两个 Commit 的关联总结

```
改动前: O(N × M) — 每次 bundle 调度，遍历全部 actor 计算 rank

59c4372a: 引入全局 heapdict
  ├── 无 locality 路径: O(1) peek 全局最空闲 actor
  ├── 有 locality 路径: O(N_nodes × M_per_node)  ← 瓶颈残留
  └── TODO 注释: "node_id -> heap[actor, ...]"

1de3b19e: 引入每节点 heapdict
  ├── 无 locality 路径: O(1)（不变）
  ├── 有 locality 路径: O(N_nodes) ← 每节点 O(1) peek + 提前终止
  └── rank 更新: O(log M)（同时更新全局 heap 和节点 heap）
```

| | 改动前 | 59c4372a | 1de3b19e |
|---|---|---|---|
| **无 locality** | O(M) 扫描 | O(1) peek | O(1) peek |
| **有 locality** | O(M) 全局扫描 | O(N_nodes × M_per_node) | O(N_nodes) |
| **rank 更新** | 无（每次重新算） | O(log M) | O(log M) × 2 |
| **数据结构** | 无索引 | 全局 heap + 节点 Set | 全局 heap + 每节点 heap |

`1de3b19e` 正是 `59c4372a` 中那个 TODO 的实现，两步走完才达到了完整的优化目标。

---

## 三、代码重构 Commit

### 3.1 `2eb5ec05` — Final bundle clean up (#62891)

**作者**: iamjustinhsu
**日期**: 2026-04-27
**影响范围**: 8 files, +106 / -176（净减 70 行）

#### 目标

消除 `InternalQueueOperatorMixin` 中 6 个抽象方法的重复实现，用声明式的 `_input_queues` / `_output_queues` 属性替代。

#### 改动前的问题

`InternalQueueOperatorMixin` 定义了 6 个抽象方法：

```python
class InternalQueueOperatorMixin(PhysicalOperator, abc.ABC):
    @abc.abstractmethod
    def internal_input_queue_num_blocks(self) -> int: ...
    @abc.abstractmethod
    def internal_input_queue_num_bytes(self) -> int: ...
    @abc.abstractmethod
    def internal_output_queue_num_blocks(self) -> int: ...
    @abc.abstractmethod
    def internal_output_queue_num_bytes(self) -> int: ...
    @abc.abstractmethod
    def clear_internal_input_queue(self) -> None: ...
    @abc.abstractmethod
    def clear_internal_output_queue(self) -> None: ...
```

每个子类都要各自实现这 6 个方法，但逻辑高度雷同。以 `UnionOperator` 和 `ZipOperator` 为例，实现几乎一字不差。有 5 个子类各自写了一遍：`AllToAllOperator`、`MapOperator`、`ActorPoolMapOperator`、`OutputSplitter`、`UnionOperator`、`ZipOperator`。

#### 改动后的设计

核心思路：**每个子类只需声明"我的内部队列有哪些"，公共逻辑由基类统一实现。**

**基类：从 6 个抽象方法 → 2 个抽象属性 + 4 个通用实现**

```python
class InternalQueueOperatorMixin(PhysicalOperator, abc.ABC):
    @property
    @abc.abstractmethod
    def _input_queues(self) -> List["BaseBundleQueue"]:
        """Return all the internal input buffer queues for this operator."""

    @property
    @abc.abstractmethod
    def _output_queues(self) -> List["BaseBundleQueue"]:
        """Return all the internal output buffer queues for this operator."""

    # 以下全部有了通用实现，不再是抽象方法
    def internal_input_queue_num_blocks(self) -> int:
        return sum(q.num_blocks() for q in self._input_queues)

    def internal_input_queue_num_bytes(self) -> int:
        return sum(q.estimate_size_bytes() for q in self._input_queues)

    def internal_output_queue_num_blocks(self) -> int:
        return sum(q.num_blocks() for q in self._output_queues)

    def internal_output_queue_num_bytes(self) -> int:
        return sum(q.estimate_size_bytes() for q in self._output_queues)

    def clear_internal_input_queue(self) -> None:
        for input_buffer in self._input_queues:
            input_buffer.clear()

    def clear_internal_output_queue(self) -> None:
        for output_buffer in self._output_queues:
            output_buffer.clear()
```

**子类：只需声明队列列表**

| 子类 | `_input_queues` | `_output_queues` |
|---|---|---|
| `AllToAllOperator` | `[self._input_buffer]` | `[self._output_buffer]` |
| `TaskPoolMapOperator` | `[self._block_ref_bundler]` | `[self._output_queue]` |
| `ActorPoolMapOperator` | `[self._bundle_queue, self._block_ref_bundler]` | `[self._output_queue]` |
| `OutputSplitter` | `[self._buffer]` | `[self._output_queue]` |
| `UnionOperator` | `self._input_buffers`（多个输入） | `[self._output_buffer]` |
| `ZipOperator` | `self._input_buffers`（多个输入） | `[self._output_buffer]` |

注意 `ActorPoolMapOperator` 有**两个**输入队列，之前需要专门 override 来累加两个队列，现在只需在列表中多返回一个元素。

#### 行为变化

之前的 clear 是逐个 dequeue 并调用 `self._metrics.on_input_dequeued()`，现在直接调用 queue 的 `clear()` 方法。`clear()` 内部会正确更新统计计数，但**不再触发 operator 级别的 `on_input_dequeued` metrics 回调**。这在执行结束时是合理的，因为这些 metrics 已经没有消费方了。

#### 总结

典型的"声明式配置替代命令式实现"重构模式。前提是所有 operator 的内部缓冲区已经统一为 `BaseBundleQueue` 的子类，只有统一了接口，才能在基类中用通用逻辑操作它们。

---

## 四、ObjectReconstructionFailedError 错误分析

### 4.1 报错信息

```
ray.exceptions.RayTaskError(ObjectReconstructionFailedError):
  ray::StreamingRepartition[num_rows_per_block=40]() (pid=4709, ip=10.48.35.39)
  At least one of the input arguments for this task could not be computed:
ray.exceptions.ObjectReconstructionFailedError: Failed to retrieve object bf2dc223c5fcc270b518845222473111eca2f6371a00000002000000.
```

### 4.2 错误层次

1. **外层 `RayTaskError`**：`StreamingRepartition` 这个 task 执行失败了
2. **内层 `ObjectReconstructionFailedError`**：原因是该 task 的某个**输入参数**（ObjectRef）无法获取

关键信息是 **"At least one of the input arguments for this task could not be computed"**——这不是 task 本身执行出错，而是 task 的输入数据丢失了。

### 4.3 异常类层次

```python
class RayError(Exception)                          # 所有 Ray 异常的基类
  └── ObjectLostError(RayError)                    # 对象丢失
        └── ObjectReconstructionFailedError(ObjectLostError)  # 对象丢失且无法重建
```

`ObjectReconstructionFailedError` 继承自 `ObjectLostError`，不是 `RayTaskError` 的子类。但在本错误中，它被包装在 `RayTaskError` 的 `cause` 字段中传播。

### 4.4 根因分析

Ray 的对象存储模型中，每个 task 的输入是 ObjectRef。当 worker 节点故障、object 被溢出后无法恢复、或 lineage 被驱逐时，这个 ObjectRef 指向的对象就会丢失。Ray 会尝试通过 lineage reconstruction（重放产生该对象的 task 链）来恢复，但如果恢复失败，就抛出 `ObjectReconstructionFailedError`。

从代码（`exceptions.py:786-825`）看，失败原因有以下几种：

| ErrorType | 含义 |
|---|---|
| `OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED` | 重试次数超过上限 |
| `OBJECT_UNRECONSTRUCTABLE_LINEAGE_EVICTED` | lineage 被驱逐（内存压力） |
| `OBJECT_UNRECONSTRUCTABLE_PUT` | 对象由 `ray.put()` 创建，没有 task lineage |
| `OBJECT_UNRECONSTRUCTABLE_RETRIES_DISABLED` | task 设置了 `max_retries=0` |
| `OBJECT_UNRECONSTRUCTABLE_BORROWED` | 对象跨 ownership 边界借用 |
| `OBJECT_UNRECONSTRUCTABLE_TASK_CANCELLED` | 产生该对象的 task 已被取消 |
| `OBJECT_UNRECONSTRUCTABLE_LINEAGE_DISABLED` | 全局禁用了 lineage reconstruction |
| `OBJECT_UNRECONSTRUCTABLE_REF_NOT_FOUND` | 引用计数器中找不到引用（通常是 bug） |

### 4.5 数据流路径

```
上游 task → ObjectRef (block) → StreamingRepartition task
                                 ↑
                           这里 ray.get() 输入参数时
                           发现 ObjectRef 指向的对象丢失且无法重建
```

具体来说，Ray Data 的流式执行中：
1. 上游 operator 的 task 产出的 block 作为 ObjectRef 存入对象存储
2. `StreamingRepartition` task 提交时，Ray Core 将这些 ObjectRef 作为参数传入
3. Ray Core 在 worker 上执行 task 前，需要先获取这些输入参数
4. 如果某个 ObjectRef 的对象已丢失且 lineage reconstruction 失败，整个 task 就会因为输入不可用而失败

### 4.6 会导致作业失败吗？

**会。** 从 `streaming_executor_state.py:674-702` 的逻辑看：

```python
try:
    bytes_read = task.on_data_ready(...)
except Exception as e:
    num_errored_blocks += 1
    should_ignore = max_errored_blocks < 0 or max_errored_blocks >= num_errored_blocks
    if should_ignore:
        logger.error(error_message, exc_info=e)  # 只记录，继续
    else:
        raise e from None  # 直接抛出，终止执行
```

- `max_errored_blocks` 默认值为 0（通过 `DataContext` 配置）
- 所以**第一次遇到这个错误就会抛出异常，终止整个 Dataset 执行**

即使 `max_errored_blocks` 设大了可以容忍，但 `ObjectReconstructionFailedError` 意味着数据已经丢失，后续依赖该数据的所有计算都无法进行，跳过也没有意义。

### 4.7 配置了无限重试会怎样？

需要区分两个层面的"重试"：

#### Ray Core 层面的 `max_retries`（`@ray.remote(max_retries=N)`）

这是 Ray Core 对**task 执行失败**的重试。但对 `ObjectReconstructionFailedError` **无效**：

- 它不是 task 执行时抛出的用户代码异常
- 它是 Ray Core 在**解析 task 输入参数时**发现 ObjectRef 不可用而产生的
- 如果产生该 ObjectRef 的上游 task 的 `max_retries` 已耗尽，那下游 task 设置无限重试也没有意义——上游数据已经无法恢复了
- 如果原因是 lineage 被驱逐，那即使下游 task 无限重试，每次重试都会遇到同样的错误

#### Ray Data 层面的 `iterate_with_retry`（UDF 级重试）

从 `map_operator.py:708-732`：

```python
retry_on = data_context.retried_map_errors
if retry_on:
    block_iter = iterate_with_retry(
        transform_iter_factory,
        match=None if retry_on is True else retry_on,
        max_attempts=data_context.max_map_retries + 1,
    )
```

这个重试只针对 **UDF 执行阶段**的异常。而 `ObjectReconstructionFailedError` 发生在 UDF 执行**之前**（输入参数解析阶段），所以这个重试机制也不会生效。

#### Ray Data 的 `max_errored_blocks`

这是唯一能"容忍"这类错误的机制，但只是跳过而非重试。且数据已经丢失，跳过只是掩盖问题。

### 4.8 结论

| 问题 | 答案 |
|---|---|
| 根因 | 上游 task 产出的 block ObjectRef 对象丢失，且 lineage reconstruction 失败 |
| 会导致作业失败吗？ | 会，`max_errored_blocks` 默认为 0，首次即终止 |
| 无限重试能解决吗？ | 不能。Ray Core 重试和 Ray Data UDF 重试都不覆盖此场景 |
| 真正的解决方案 | 找到根因——为什么对象丢失？ |

### 4.9 排查方向

1. **检查节点是否有过故障/重启**：Dashboard → Nodes 页面，看 ip=10.48.35.39 的节点是否曾有异常
2. **检查 Object Store 内存压力**：`RAY_max_lineage_bytes` 默认 1GB，如果 lineage 被驱逐，会报 `OBJECT_UNRECONSTRUCTABLE_LINEAGE_EVICTED`
3. **设置 `RAY_record_ref_creation_sites=1`**：如报错信息提示，开启后可以看到 ObjectRef 是在哪里创建的，帮助定位是哪个上游 task 的输出丢失
4. **如果是 `ray.put()` 创建的对象**：`OBJECT_UNRECONSTRUCTABLE_PUT`，需要改为从 task 返回值
5. **检查 `max_retries` 配置**：上游 task 的 `max_retries` 是否足够（默认值通常较小）
