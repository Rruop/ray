# Ray Data Streaming Generator 机制详解

本文档详细解析 Ray Data 中 streaming generator 的工作机制，包括 `ray.wait()`、`prepare_metadata()`、`_next_sync()` 的实现原理，以及 Driver 与 Worker 之间的通信机制。

---

## 目录

1. [ray.wait() 参数详解](#1-raywait-参数详解)
2. [prepare_metadata() 方法逻辑](#2-prepare_metadata-方法逻辑)
3. [_next_sync() 实现原理](#3-_next_sync-实现原理)
4. [Block 获取机制与大小影响](#4-block-获取机制与大小影响)
5. [Driver 获取 streaming_gen 的机制](#5-driver-获取-streaming_gen-的机制)
6. [Object Ref Stream 底层机制](#6-object-ref-stream-底层机制)
7. [完整数据流总结](#7-完整数据流总结)

---

## 1. ray.wait() 参数详解

### 1.1 基本用法

```python
ready, _ = ray.wait(
    list(active_tasks.keys()),
    num_returns=len(active_tasks),
    fetch_local=False,
    timeout=0.1,
)
```

### 1.2 参数含义

| 参数 | 含义 |
|------|------|
| `num_returns` | 指定**最多**等待多少个任务完成。当有这么多任务就绪时，函数会立即返回 |
| `timeout` | 最大等待时间（秒）。超过此时间，无论完成多少任务都会返回 |
| `fetch_local` | 是否将对象拉取到本地。`False` 表示只检查状态，不传输数据 |

### 1.3 是否必须等到 timeout？

**不是**。这是一个 **OR** 关系，满足任一条件就返回：

1. 有 `num_returns` 个任务完成 → 立即返回
2. 达到 `timeout` 时间 → 立即返回（即使完成数少于 `num_returns`）

### 1.4 行为示例

```python
ready, _ = ray.wait(
    list(active_tasks.keys()),
    num_returns=len(active_tasks),  # 等待所有任务
    fetch_local=False,
    timeout=0.1,  # 最多等 0.1 秒
)
```

行为是：
- 如果 0.1 秒内所有任务都完成 → 立即返回
- 如果 0.1 秒后只有部分完成 → 返回已完成的部分
- 如果 0.1 秒内没有任务完成 → 返回空列表

这是一种**非阻塞轮询**模式，常用于在循环中检查任务进度而不长时间阻塞。

---

## 2. prepare_metadata() 方法逻辑

### 2.1 方法定义

**位置**: `python/ray/data/_internal/execution/interfaces/physical_operator.py:254-317`

```python
def prepare_metadata(self) -> bool:
    """Prepare metadata ref without blocking.

    This method prepares the block_ref and meta_ref for batch processing.
    It does not block waiting for metadata to be ready.

    Returns:
        True if metadata ref is ready for batch waiting, False otherwise.
    """
```

### 2.2 方法目的

**非阻塞地**准备 `block_ref` 和 `meta_ref`，用于后续批量处理。这是一个"准备阶段"，不会阻塞等待数据就绪。

### 2.3 执行流程图

```
┌─────────────────────────────────────────────────────────┐
│ Step 1: 获取 block_ref (如果还没有)                      │
│   _streaming_gen._next_sync(timeout_s=0)                │
│   └─> 返回 nil → 数据未就绪，返回 False                  │
│   └─> StopIteration → 任务完成，返回 False               │
│   └─> 有效 ref → 继续                                    │
├─────────────────────────────────────────────────────────┤
│ Step 2: 获取 meta_ref (如果还没有)                       │
│   _streaming_gen._next_sync(timeout_s=0)                │
│   └─> 返回 nil → metadata 未就绪，返回 False             │
│   └─> StopIteration → 任务出错，抛异常                   │
│   └─> 有效 ref → 返回 True                               │
└─────────────────────────────────────────────────────────┘
```

### 2.4 源码解析

```python
def prepare_metadata(self) -> bool:
    if self._has_finished:
        return False

    # Step 1: 获取 block_ref
    if self._pending_block_ref.is_nil():
        assert self._pending_meta_ref.is_nil()

        try:
            # 非阻塞获取 block 引用
            self._pending_block_ref = self._streaming_gen._next_sync(timeout_s=0)
        except StopIteration:
            self._task_done_callback(None)
            self._has_finished = True
            return False

        if self._pending_block_ref.is_nil():
            # generator 当前没有新输出
            return False

        self._block_ready_callback(self._pending_block_ref)

    # Step 2: 获取 meta_ref
    if self._pending_meta_ref.is_nil():
        try:
            self._pending_meta_ref = self._streaming_gen._ne_sync(
                timeout_s=METADATA_WAIT_TIMEOUT_S  # 0.0
            )
        except StopIteration:
            # 如果这里抛出 StopIteration，说明任务出错
            try:
                ray.get(self._pending_block_ref)
            except Exception as ex:
                self._task_done_callback(ex)
                self._has_finished = True
                raise ex from None

        if self._pending_meta_ref.is_nil():
            # Metadata ref 还未就绪
            return False

        self._metadata_ready_callback(self._pending_meta_ref)

    # 两者都就绪
    return True
```

### 2.5 关键配置

```python
# physical_operator.py:42-50
METADATA_GET_TIMEOUT_S = 1.0      # ray.get 获取 metadata 的超时时间
METADATA_WAIT_TIMEOUT_S = 0.0     # _next_sync 的超时时间，设为 0 实现非阻塞
```

注释说明了设计意图：

> 设为 0 实现非阻塞行为。当 meta_ref 未立即可用时，任务会在下一次调度循环中重试。
> 这避免了处理多任务时的串行阻塞，当 block 较小时显著提升调度循环性能。

---

## 3. _next_sync() 实现原理

### 3.1 方法定义

**位置**: `python/ray/_private/object_ref_generator.py:188-242`

```python
def _next_sync(self, timeout_s: Optional[int | float] = None) -> "ray.ObjectRef":
    """Waits for timeout_s and returns the object ref if available.

    If an object is not available within the given timeout, it
    returns a nil object reference.

    If -1 timeout is provided, it means it waits infinitely.
    """
```

### 3.2 核心流程

```python
def _next_sync(self, timeout_s=None):
    core_worker = self.worker.core_worker

    # 1. 先 peek（窥视）下一个 ObjectRef
    expected_ref, is_ready = core_worker.peek_object_ref_stream(self._generator_ref)

    # 2. 如果未就绪，等待 timeout_s
    if not is_ready:
        _, unready = ray.wait([expected_ref], timeout=timeout_s, fetch_local=False)
        if len(unready) > 0:
            return ray.ObjectRef.nil()  # 超时，返回空引用

    # 3. 从流中读取
    try:
        ref = core_worker.try_read_next_object_ref_stream(self._generator_ref)
        assert not ref.is_nil()
    except ObjectRefStreamEndOfStreamError:
        # 处理流结束或异常情况
        ...

    return ref
```

### 3.3 timeout_s=0 时的行为

当 `timeout_s=0` 时：
- `ray.wait` 立即返回
- 如果数据未就绪，直接返回 `ray.ObjectRef.nil()`
- **完全非阻塞**

### 3.4 关键操作说明

| 操作 | 说明 | 是否阻塞 |
|------|------|---------|
| `peek_object_ref_stream` | 查看 stream 中下一个 ref（不消费） | 否，本地操作 |
| `ray.wait(timeout=0)` | 检查 ref 状态 | 否，立即返回 |
| `try_read_next_object_ref_stream` | 从 stream 中消费 ref | 否，本地操作 |

---

## 4. Block 获取机制与大小影响

### 4.1 每个 task 只会获取一个 block 吗？

**`prepare_metadata()` 每次调用最多获取一对 (block_ref, meta_ref)**。

但要注意：
1. 一个 Ray task 可以是 **streaming generator**，产出**多个** block
2. `prepare_metadata()` 会被**多次调用**，每次处理一个 block
3. 外层循环会不断调用，直到 generator 的所有 block 都被处理

### 4.2 on_data_ready() 的循环处理

从 `on_data_ready()` 方法可以看到循环获取多个 block：

```python
def on_data_ready(self, max_bytes_to_read: Optional[int]) -> int:
    bytes_read = 0
    while max_bytes_to_read is None or bytes_read < max_bytes_to_read:
        # ... 循环获取多个 block
        if self._pending_block_ref.is_nil():
            self._pending_block_ref = self._streaming_gen._next_sync(timeout_s=0)
            # ...

        if self._pending_meta_ref.is_nil():
            self._pending_meta_ref = self._streaming_gen._next_sync(timeout_s=0)
            # ...

        # 处理完成后累加
        bytes_read += meta.size_bytes

    return bytes_read
```

### 4.3 Block 大小对时长的影响

#### 4.3.1 数据传输时间

```python
meta_with_schema = ray.get(self._pending_meta_ref, timeout=METADATA_GET_TIMEOUT_S)
```

- `METADATA_GET_TIMEOUT_S = 1.0` 秒
- metadata 对象需要从 worker 传输到 driver
- Block 越大 → metadata 计算越慢 → 传输可能更慢

#### 4.3.2 调度循环延迟

由于 `prepare_metadata()` 使用 `timeout_s=0`（非阻塞）：
- 如果 block 还在计算中 → 返回 False → 下次调度循环再试
- **Block 计算时间越长，需要的调度循环次数越多**

#### 4.3.3 调度循环频率影响

```python
# 在调度器中 (streaming_executor.py)
ready, _ = ray.wait(active_tasks, timeout=0.1, ...)
```

- 每 0.1 秒一次调度循环
- 小 block 更快完成 → 更频繁地有数据产出
- 大 block → 可能需要多个 0.1 秒周期才能完成

#### 4.3.4 吞吐量 vs 延迟权衡

| Block 大小 | 优点 | 缺点 |
|-----------|------|------|
| **小 block** | 低延迟，流式输出更快 | 更多调度开销，更多 ObjectRef |
| **大 block** | 批处理效率高，开销少 | 首次输出延迟高，内存占用大 |

---

## 5. Driver 获取 streaming_gen 的机制

### 5.1 完整数据流图

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                                DRIVER 端                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  1. 提交任务时：                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ gen = self._map_task.options(...).remote(...)                       │    │
│  │       ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^                       │    │
│  │       调用 .remote() 返回的是 ObjectRefGenerator                    │    │
│  │       (不是普通 ObjectRef，因为任务返回类型是 Iterator)              │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                              │                                              │
│                              ▼                                              │
│  2. 保存到 DataOpTask：                                                      │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ data_task = DataOpTask(task_index, gen, ...)                        │    │
│  │ self._data_tasks[task_index] = data_task                            │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                              │
                              │ Ray Core 底层机制
                              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                               WORKER 端                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  def _map_task(...) -> Iterator[Union[Block, BlockMetadata]]:              │
│      for block in map_transformer.apply_transform(...):                    │
│          yield block                    # ① 产出 block                     │
│          yield BlockMetadataWithSchema(...)  # ② 产出 metadata             │
│                                                                             │
│  每次 yield 会：                                                             │
│  1. 将对象存入本地 Object Store                                              │
│  2. 生成 ObjectRef 并发送到 Object Ref Stream                               │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 5.2 关键机制：num_returns="streaming"

当一个 Ray task 声明为返回 `Iterator` 或使用 `num_returns="streaming"` 时：

```python
# task_pool_map_operator.py:106
self._map_task = cached_remote_fn(_map_task, **ray_remote_static_args)
# ray_remote_static_args 包含 num_returns="streaming"
```

调用 `.remote()` 时：
- **不会**返回普通的 `ObjectRef`
- **而是**返回 `ObjectRefGenerator` 对象

### 5.3 ObjectRefGenerator 的内部结构

```python
# object_ref_generator.py:57-65
class ObjectRefGenerator:
    def __init__(self, generator_ref: "ray.ObjectRef", worker: "Worker"):
        self._generator_ref = generator_ref  # 指向 generator task 的引用
        self.worker = worker                  # driver 的 worker 实例
```

**关键点**：
- `_generator_ref` 是一个**特殊的 ObjectRef**，代表整个 streaming task
- 它不指向具体数据，而是指向一个 **Object Ref Stream**

### 5.4 任务提交到执行的完整链路

```python
# task_pool_map_operator.py:134-143
gen = self._map_task.options(**dynamic_ray_remote_args).remote(
    self._map_transformer_ref,
    data_context,
    ctx,
    *bundle.block_refs,
    slices=bundle.slices,
    **self.get_map_task_kwargs(),
)

self._submit_data_task(gen, bundle)
```

```python
# map_operator.py:627-636
data_task = DataOpTask(
    task_index,
    gen,  # ObjectRefGenerator
    lambda output: _output_ready_callback(task_index, output),
    functools.partial(_task_done_callback, task_index),
)
self._data_tasks[task_index] = data_task
```

### 5.5 数据 vs 引用

| 操作 | 位置 | 返回内容 | 是否传输数据 |
|------|------|---------|-------------|
| `_map_task.remote()` | Driver | `ObjectRefGenerator` | ❌ 只是控制结构 |
| `_next_sync()` | Driver | `ObjectRef` (block/meta 的引用) | ❌ 只是引用 |
| `ray.get(meta_ref)` | Driver | 实际的 metadata 对象 | ✅ 从 Object Store 传输 |
| `ray.get(block_ref)` | 下游 Worker | 实际的 block 数据 | ✅ 从 Object Store 传输 |

---

## 6. Object Ref Stream 底层机制

### 6.1 核心问题

`_next_sync(0)` 的时候，`ObjectRefGenerator` 是怎么能得到 stream 的状态的？任务明明在远端执行。

### 6.2 核心机制：RPC 通知 + 本地 Stream 缓存

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              DRIVER 端                                       │
│                                                                             │
│  TaskManager                                                                │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │ object_ref_streams_[generator_id] = ObjectRefStream                   │  │
│  │                                                                       │  │
│  │   Stream 内容 (本地缓存):                                              │  │
│  │   ┌─────────────────────────────────────────────────────────────────┐ │  │
│  │   │ [obj_ref_1, obj_ref_2, obj_ref_3, ...]  ← 由 Worker RPC 写入    │ │  │
│  │   └─────────────────────────────────────────────────────────────────┘ │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                              ▲                                              │
│                              │ HandleReportGeneratorItemReturns()           │
│                              │ (写入 stream)                                │
│                              │                                              │
│  CoreWorker gRPC Server ◄────┘                                              │
│         ▲                                                                   │
│         │ RPC: ReportGeneratorItemReturns                                   │
│         │                                                                   │
└─────────│───────────────────────────────────────────────────────────────────┘
          │
          │ 网络
          │
┌─────────│───────────────────────────────────────────────────────────────────┐
│         ▼                                                                   │
│  CoreWorker (执行任务)                                                       │
│                                                                             │
│  def _map_task():                                                           │
│      for block in transform():                                              │
│          yield block  ───────► 触发 ReportGeneratorItemReturns RPC          │
│          yield meta   ───────► 触发 ReportGeneratorItemReturns RPC          │
│                                                                             │
│                              WORKER 端                                       │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 6.3 详细流程

#### Step 1: 任务提交时（Driver 端）

```cpp
// task_manager.cc:324-332
if (spec.IsStreamingGenerator()) {
    const auto generator_id = spec.ReturnId(0);
    absl::MutexLock lock(&object_ref_stream_ops_mu_);
    // 在 Driver 的 TaskManager 中创建一个空的 ObjectRefStream
    object_ref_streams_.emplace(generator_id, ObjectRefStream(generator_id));
}
```

此时 **stream 是空的**，只是预先创建了容器。

#### Step 2: Worker 执行 yield 时

当 Python generator `yield` 一个对象：

```cpp
// core_worker.cc:3206-3235 (Worker 端)
rpc::ReportGeneratorItemReturnsRequest request;
request.set_item_index(item_index);
request.set_generator_id(generator_id.Binary());
// ... 序列化对象引用信息 ...

// 发送 RPC 到 Driver
client->ReportGeneratorItemReturns(std::move(request), callback);
```

#### Step 3: Driver 接收 RPC 并写入本地 Stream

```cpp
// task_manager.cc:780-828 (Driver 端)
bool TaskManager::HandleReportGeneratorItemReturns(request, callback) {
    const auto &generator_id = ObjectID::FromBinary(request.generator_id());

    absl::MutexLock lock(&object_ref_stream_ops_mu_);
    auto stream_it = object_ref_streams_.find(generator_id);

    // 关键：将 object_id 写入 Driver 本地的 stream
    stream_it->second.InsertToStream(object_id, item_index);
}
```

#### Step 4: PeekObjectRefStream 读取本地 Stream

```cpp
// task_manager.cc:731-745 (Driver 端)
std::pair<ObjectID, bool> TaskManager::PeekObjectRefStream(const ObjectID &generator_id) {
    absl::MutexLock lock(&object_ref_stream_ops_mu_);
    auto stream_it = object_ref_streams_.find(generator_id);

    // 从本地 stream 中 peek
    const auto &result = stream_it->second.PeekNextItem();
    return result;  // (object_id, is_ready)
}
```

### 6.4 Python 到 C++ 的调用链

```python
# Python 层
def _next_sync(self, timeout_s=0):
    core_worker = self.worker.core_worker
    expected_ref, is_ready = core_worker.peek_object_ref_stream(self._generator_ref)
```

```cython
# Cython 层 (_raylet.pyx:4767-4780)
def peek_object_ref_stream(self, ObjectRef generator_id):
    cdef:
        CObjectID c_generator_id = generator_id.native()
        pair[CObjectReference, c_bool] c_object_ref_and_is_ready_pair

    with nogil:
        c_object_ref_and_is_ready_pair = (
                CCoreWorkerProcess.GetCoreWorker().PeekObjectRefStream(
                    c_generator_id))

    return (ObjectRef(...), c_object_ref_and_is_ready_pair.second)
```

```cpp
// C++ 层 (core_worker.cc:3119-3126)
std::pair<rpc::ObjectReference, bool> CoreWorker::PeekObjectRefStream(
    const ObjectID &generator_id) {
  auto [object_id, ready] = task_manager_->PeekObjectRefStream(generator_id);
  // ...
  return {object_ref, ready};
}
```

### 6.5 时序图

```
Driver                                          Worker
  │                                                │
  │  1. task.remote() 提交任务                     │
  │  ──────────────────────────────────────────►   │
  │  [创建空的 object_ref_stream]                  │
  │                                                │
  │                                                │  2. 执行 _map_task()
  │                                                │
  │                                                │  3. yield block_1
  │   ◄──────────────────────────────────────────  │  [RPC: ReportGeneratorItemReturns]
  │  [写入 stream: obj_ref_1]                      │
  │                                                │
  │  4. _next_sync(0) 调用                         │
  │  [peek stream → obj_ref_1, ready=true]         │
  │                                                │
  │                                                │  5. yield metadata_1
  │   ◄──────────────────────────────────────────  │  [RPC: ReportGeneratorItemReturns]
  │  [写入 stream: meta_ref_1]                     │
  │                                                │
  │  6. _next_sync(0) 调用                         │
  │  [peek stream → meta_ref_1, ready=true]        │
  │                                                │
```

### 6.6 关键点总结

| 问题 | 答案 |
|------|------|
| Stream 在哪里？ | **Driver 端**的 `TaskManager::object_ref_streams_` |
| 谁写入 stream？ | Worker 通过 **RPC** 通知 Driver 写入 |
| peek 是本地还是远程？ | **纯本地操作**，读取 Driver 内存中的 stream |
| Worker yield 如何触发？ | yield → C++ CoreWorker → RPC 到 Driver |
| 为什么 `_next_sync(0)` 可以立即返回？ | 因为它只是读取 Driver 本地的 stream 缓存 |

### 6.7 设计优势

```
传统方式 (每次 poll 远端):
  Driver ──poll──► Worker   # 高延迟，网络开销大

Ray 的方式 (Worker 推送):
  Worker ──push──► Driver   # 低延迟，Driver 本地读取
```

**优势**：
1. `peek` 和 `read` 是纯本地操作，无网络延迟
2. Worker 主动推送，避免 Driver 轮询
3. Stream 作为缓冲，解耦生产和消费速度

---

## 7. 完整数据流总结

### 7.1 整体架构图

```
┌─────────────────────────────────────────────────────────────────────────┐
│ DRIVER                                                                  │
│                                                                         │
│  ① task.remote() → ObjectRefGenerator (立即返回，任务异步执行)          │
│                              │                                          │
│  ② _next_sync(0) ───────────┼──► peek + wait + read stream             │
│         │                    │    (查询 stream 状态，获取 ObjectRef)    │
│         ▼                    │                                          │
│    ObjectRef ◄───────────────┘                                          │
│    (block_ref 或 meta_ref)                                              │
│         │                                                               │
│  ③ ray.get(meta_ref) ─────────► 从 Object Store 拉取 metadata          │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
                                      ▲
                                      │ Object Store 网络传输
                                      │
┌─────────────────────────────────────│───────────────────────────────────┐
│ WORKER                              │                                   │
│                                     │                                   │
│  _map_task() 执行：                  │                                   │
│    for block in transform():        │                                   │
│        yield block     ──────► Object Store ──► Stream (ref)            │
│        yield metadata  ──────► Object Store ──► Stream (ref)            │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 7.2 关键源码位置

| 组件 | 文件位置 |
|------|---------|
| DataOpTask | `python/ray/data/_internal/execution/interfaces/physical_operator.py` |
| ObjectRefGenerator | `python/ray/_private/object_ref_generator.py` |
| _map_task | `python/ray/data/_internal/execution/operators/map_operator.py:737` |
| TaskPoolMapOperator | `python/ray/data/_internal/execution/operators/task_pool_map_operator.py` |
| CoreWorker (C++) | `src/ray/core_worker/core_worker.cc` |
| TaskManager (C++) | `src/ray/core_worker/task_manager.cc` |
| Cython bindings | `python/ray/_raylet.pyx` |

### 7.3 核心要点

1. **Driver 拿到的 `ObjectRefGenerator`** 是 Ray 调度系统创建的控制对象，不包含实际数据
2. **`_next_sync()` 只是从本地 Object Ref Stream 中获取 `ObjectRef`**（引用），不触发网络传输
3. **实际数据仍在 Worker 的 Object Store 中**
4. **只有 `ray.get()` 才会触发数据从 Object Store 传输到请求方**
5. **Worker 通过 RPC 主动推送** ref 到 Driver 的 stream，而不是 Driver 轮询 Worker

---

## 附录：相关配置参数

```python
# physical_operator.py
METADATA_GET_TIMEOUT_S = 1.0      # ray.get 获取 metadata 的超时
METADATA_WAIT_TIMEOUT_S = 0.0     # _next_sync 的超时（非阻塞）

# streaming_executor.py (调度循环)
timeout = 0.1                      # ray.wait 的超时时间
```
