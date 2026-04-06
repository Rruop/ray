# Ray Data 中 ObjectRef 传递与生命周期分析

## 分析目标

用户希望了解 Ray Data 中传递 ObjectRef 和 List[ObjectRef] 时的处理方式：
1. 引用能否直接传递
2. 是否有泄漏风险
3. 是否有提前释放的风险

---

## 核心发现

### 1. ObjectRef 能否直接传递？

**✅ 可以直接传递**，Ray Data 提供了专门的 API：

| API | 输入类型 | 是否 ray.put() |
|-----|---------|---------------|
| `from_arrow_refs()` | `List[ObjectRef]` | ❌ 直接使用 |
| `from_numpy_refs()` | `List[ObjectRef]` | ❌ 直接使用 |
| `from_blocks()` | `List[Block]` | ✅ 内部调用 ray.put() |
| `from_items()` | Python 对象 | ✅ 内部调用 ray.put() |

**关键代码路径**:
```
python/ray/data/read_api.py
  ├── from_arrow_refs() → 直接接受 ObjectRef
  ├── from_numpy_refs() → 直接接受 ObjectRef
  └── from_blocks() → 调用 ray.put() 转换为 ObjectRef
       ↓
python/ray/data/_internal/logical/operators/from_operators.py
  └── FromArrow/FromBlocks → 创建 RefBundle(owns_blocks=False)
```

### 2. 引用管理机制

#### 2.1 核心数据结构: RefBundle

```python
@dataclass
class RefBundle:
    blocks: Tuple[Tuple[ObjectRef[Block], BlockMetadata], ...]
    owns_blocks: bool  # 关键所有权标记
```

**所有权语义**:
- `owns_blocks=False`: 共享所有权，不会被 eager_free 释放
- `owns_blocks=True`: 独占所有权，使用完毕后可释放

#### 2.2 引用计数层次

```
┌─────────────────────────────────────────────────┐
│              Python 层 (object_ref.pxi)          │
│  __init__: add_object_ref_reference()           │
│  __dealloc__: remove_object_ref_reference()     │
├─────────────────────────────────────────────────┤
│              Ray Data 层 (ref_bundle.py)         │
│  owns_blocks + eager_free 控制是否主动释放       │
│  destroy_if_owned() 执行实际释放                 │
├─────────────────────────────────────────────────┤
│              C++ 层 (reference_counter.cc)       │
│  local_ref_count + submitted_task_ref_count     │
│  + contained_in_owned.size() = 总引用数         │
└─────────────────────────────────────────────────┘
```

### 3. 泄漏风险分析

| 风险场景 | 描述 | 风险等级 |
|---------|------|---------|
| 全局缓存未清理 | `_CACHE_STORAGE` 存储 RefBundle，需手动 clear_cache() | 🟡 中 |
| Out-of-band 序列化 | 使用 cloudpickle 直接序列化 ObjectRef 会永久固定 | 🔴 高 |
| trace_allocations 调试模式 | `_MemActor` 保存所有分配引用 | 🟢 低(仅调试) |
| FromOperators 持有引用 | LogicalPlan 存在期间不释放 input_data | 🟢 低(预期行为) |

### 4. 提前释放风险分析

| 风险场景 | 描述 | 风险等级 |
|---------|------|---------|
| eager_free 误用 | owns_blocks=True 且 eager_free=True 时立即释放 | 🟡 中 |
| iter_batches 消费后释放 | yield 后立即释放，用户不应保存 block_ref | 🟡 中 |
| 嵌套引用反序列化顺序 | 外部对象先释放可能导致内部对象丢失 | 🟡 中 |
| Borrower worker 崩溃 | 借用者崩溃可能丢失传递链信息 | 🔴 高(分布式场景) |

### 5. 安全机制

1. **默认 owns_blocks=False**: From* 操作符创建的 RefBundle 默认不拥有
2. **RefBundle 不可变**: owns_blocks 字段设置后不可修改
3. **Python GC 协作**: CoreWorker 触发 Python GC 清理无用引用
4. **借用协议**: Owner 追踪所有 borrower，等待全部释放

---

## 关键文件清单

| 文件 | 作用 |
|-----|------|
| `python/ray/data/_internal/execution/interfaces/ref_bundle.py` | RefBundle 定义，所有权管理 |
| `python/ray/data/_internal/memory_tracing.py` | trace_allocation/deallocation |
| `python/ray/data/_internal/logical/operators/from_operators.py` | from_* 输入算子 |
| `python/ray/data/read_api.py` | 公开 API |
| `python/ray/includes/object_ref.pxi` | ObjectRef Python 层实现 |
| `python/ray/_private/serialization.py` | 序列化/反序列化逻辑 |
| `src/ray/core_worker/reference_counter.cc` | C++ 引用计数器 |

---

## 最佳实践建议

### ✅ 推荐做法

1. **使用 `from_arrow_refs()` / `from_numpy_refs()`** 直接传递已有 ObjectRef
2. **避免持有 iter_batches 返回的 block_ref**，消费后立即处理
3. **使用完 Dataset 后调用 `clear_cache()`** 清理缓存

### ❌ 避免做法

1. **不要直接用 cloudpickle 序列化 ObjectRef**，会导致泄漏
2. **不要在 eager_free=True 时假设 ObjectRef 一直有效**
3. **不要手动设置 owns_blocks=True** 除非完全理解生命周期

---

## 详细源码分析

### from_arrow_refs() 实现

```python
# python/ray/data/read_api.py
def from_arrow_refs(
    tables: Union[
        ObjectRef["pyarrow.Table"],
        List[ObjectRef["pyarrow.Table"]],
    ],
    *,
    override_num_blocks: Optional[int] = None,
    concurrency: Optional[int] = None,
) -> MaterializedDataset:
    """从 Arrow table ObjectRef 创建 Dataset。

    关键点：
    - 直接接受 ObjectRef，不会重新 ray.put()
    - 创建 FromArrow 算子，设置 owns_blocks=False
    """
    if isinstance(tables, ray.ObjectRef):
        tables = [tables]

    # 直接传递 ObjectRef 列表
    return MaterializedDataset(
        ExecutionPlan(
            FromArrow(tables, override_num_blocks),
            ...
        ),
        ...
    )
```

### RefBundle 所有权管理

```python
# python/ray/data/_internal/execution/interfaces/ref_bundle.py
@dataclass
class RefBundle:
    """管理 Block ObjectRef 的包装器。

    关键属性:
    - blocks: (ObjectRef, BlockMetadata) 元组列表
    - owns_blocks: 是否拥有这些 block 的所有权
    """
    blocks: Tuple[Tuple[ObjectRef[Block], BlockMetadata], ...]
    owns_blocks: bool

    def destroy_if_owned(self) -> int:
        """如果拥有所有权，释放所有 block。

        只有 owns_blocks=True 时才会实际释放。
        这是防止提前释放的关键机制。
        """
        if not self.owns_blocks:
            return 0

        for block_ref, _ in self.blocks:
            # 通过删除引用来释放
            del block_ref
        return len(self.blocks)
```

### C++ 层引用计数

```cpp
// src/ray/core_worker/reference_counter.cc
bool ReferenceCounter::HasReference(const ObjectID &object_id) const {
    auto it = object_id_refs_.find(object_id);
    if (it == object_id_refs_.end()) {
        return false;
    }

    // 总引用数 = 本地引用 + 提交任务引用 + 包含在其他对象中的引用
    return it->second.local_ref_count > 0 ||
           it->second.submitted_task_ref_count > 0 ||
           !it->second.contained_in_owned.empty();
}

void ReferenceCounter::AddLocalReference(
    const ObjectID &object_id,
    const std::string &call_site) {
    // Python 层每创建一个 ObjectRef 就会调用此方法
    auto &ref = object_id_refs_[object_id];
    ref.local_ref_count++;
}

void ReferenceCounter::RemoveLocalReference(
    const ObjectID &object_id,
    std::vector<ObjectID> *deleted) {
    // Python 层 ObjectRef.__dealloc__ 时调用
    auto it = object_id_refs_.find(object_id);
    if (it == object_id_refs_.end()) {
        return;
    }

    it->second.local_ref_count--;

    // 只有所有引用都为 0 时才删除
    if (ShouldDelete(it->second)) {
        deleted->push_back(object_id);
        object_id_refs_.erase(it);
    }
}
```

---

## 典型使用场景

### 场景 1: 安全传递已有 ObjectRef

```python
import ray
from ray.data import from_arrow_refs
import pyarrow as pa

# 假设已有一些 Arrow table 的 ObjectRef
@ray.remote
def create_table():
    return pa.table({"col": [1, 2, 3]})

# 创建 ObjectRef
refs = [create_table.remote() for _ in range(10)]

# 安全传递给 Ray Data - 不会复制数据
ds = from_arrow_refs(refs)

# 处理数据
result = ds.map(lambda x: x).take_all()

# 注意: refs 仍然有效，因为 from_arrow_refs 不拥有这些引用
# Ray Data 使用 owns_blocks=False
```

### 场景 2: 避免泄漏的缓存使用

```python
import ray.data

# 使用缓存
ds = ray.data.read_parquet("s3://bucket/data")
ds = ds.materialize()  # 缓存到内存

# ... 使用 ds ...

# 完成后清理缓存，避免泄漏
ray.data.clear_cache()
```

### 场景 3: iter_batches 的正确使用

```python
ds = ray.data.read_parquet("s3://bucket/data")

# ✅ 正确: 立即处理 batch
for batch in ds.iter_batches():
    process(batch)
    # batch 在下次迭代时可能被释放

# ❌ 错误: 保存 batch 引用
batches = list(ds.iter_batches())  # 可能导致问题
```

---

## map/map_batches 返回 ObjectRef 的处理流程

### 核心问题

如果在 `map` 或 `map_batches` 的 UDF 中返回 `ObjectRef` 或 `List[ObjectRef]`，会发生什么？

### 处理流程图

```
┌─────────────────────────────────────────────────────────────────────┐
│                        UDF 返回 ObjectRef                            │
└─────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│  MapTransformFn._post_process() → BlockOutputBuffer                 │
│  将 UDF 输出转换为 Block                                             │
└─────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│  DelegatingBlockBuilder.add_batch()                                  │
│  调用 BlockAccessor.batch_to_block()                                 │
└─────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│  convert_to_pyarrow_array()  (tensor_extensions/arrow.py)           │
│  尝试转换为 Arrow 原生类型                                           │
│  ❌ ObjectRef 不是 Arrow 原生类型，转换失败                           │
└─────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│  ArrowPythonObjectArray.from_objects()  (object_extensions/arrow.py)│
│  使用 pickle_dumps() 序列化 ObjectRef                                │
│  ⚠️ 这是 out-of-band 序列化！                                        │
└─────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│  ray._private.serialization.add_contained_object_ref()              │
│  检测到 out-of-band 序列化：                                         │
│  - 如果 RAY_allow_out_of_band_object_ref_serialization=0 → 抛异常   │
│  - 如果 =1 (默认) → 调用 add_object_ref_reference() 永久固定引用    │
└─────────────────────────────────────────────────────────────────────┘
```

### 关键代码路径

**1. UDF 输出转换为 Block**

```python
# python/ray/data/_internal/tensor_extensions/arrow.py:219-301
def convert_to_pyarrow_array(column_values, column_name):
    try:
        # 尝试转换为 tensor array 或原生 Arrow 类型
        if _should_convert_to_tensor(column_values, column_name):
            return ArrowTensorArray.from_numpy(...)
        else:
            return _convert_to_pyarrow_native_array(column_values, column_name)
    except ArrowConversionError:
        # ObjectRef 无法转换为 Arrow 原生类型，走这个分支
        # 使用 pickle 序列化
        return ArrowPythonObjectArray.from_objects(column_values)
```

**2. ObjectRef 被 pickle 序列化**

```python
# python/ray/data/_internal/object_extensions/arrow.py:106-119
class ArrowPythonObjectArray(pa.ExtensionArray):
    def from_objects(objects):
        type_ = ArrowPythonObjectType()
        all_dumped_bytes = []
        for obj in objects:
            # ⚠️ 关键：ObjectRef 在这里被 pickle 序列化
            dumped_bytes = pickle_dumps(
                obj, "Error pickling object to convert to Arrow"
            )
            all_dumped_bytes.append(dumped_bytes)
        arr = pa.array(all_dumped_bytes, type=type_.storage_type)
        return type_.wrap_array(arr)
```

**3. Out-of-band 序列化处理**

```python
# python/ray/_private/serialization.py:302-335
def add_contained_object_ref(
    self,
    object_ref: "ray.ObjectRef",
    *,
    allow_out_of_band_serialization: bool,
    call_site: Optional[str] = None,
):
    if self.is_in_band_serialization():
        # 正常路径：记录嵌套引用
        self._thread_local.object_refs.add(object_ref)
    else:
        # Out-of-band 序列化（map/map_batches 返回 ObjectRef 走这里）
        if not allow_out_of_band_serialization:
            raise ray.exceptions.OufOfBandObjectRefSerializationException(...)
        else:
            # ⚠️ 关键：永久固定引用，防止被 GC
            ray._private.worker.global_worker.core_worker.add_object_ref_reference(
                object_ref
            )
```

### 风险分析

| 风险类型 | 描述 | 风险等级 |
|---------|------|---------|
| **内存泄漏** | ObjectRef 被永久固定 (`add_object_ref_reference`)，即使 Dataset 被销毁也不会释放 | 🔴 **高** |
| **不会自动 ray.get()** | ObjectRef 不会被自动解引用，后续处理需要手动调用 `ray.get()` | 🟡 中 |
| **提前释放** | 由于永久固定，**不存在**提前释放风险 | 🟢 低 |

### 为什么会泄漏？

```
正常 ObjectRef 生命周期:
┌───────────┐     ┌───────────┐     ┌───────────┐
│ ray.put() │────▶│ 引用持有  │────▶│ 引用销毁  │────▶ 对象被 GC
└───────────┘     └───────────┘     └───────────┘

map/map_batches 返回 ObjectRef:
┌───────────┐     ┌───────────────────────────────────────────┐
│ ray.put() │────▶│ add_object_ref_reference() 永久固定       │
└───────────┘     │ (没有对应的 remove_object_ref_reference)  │
                  │ 对象永远不会被 GC，直到 worker 进程退出     │
                  └───────────────────────────────────────────┘
```

### 检测方法

设置环境变量禁止 out-of-band 序列化，运行时会抛出异常：

```bash
export RAY_allow_out_of_band_object_ref_serialization=0
```

```python
# 运行会抛出 OufOfBandObjectRefSerializationException
ds = ray.data.range(10)
ds = ds.map(lambda x: ray.put(x))  # 抛异常
```

---

## Ray Core 自定义 Task/Actor 中 ObjectRef 传递

### 核心问题

如果不使用 Ray Data，而是自定义 Ray task 或 actor，传递 ObjectRef 是否有问题？

### 关键区别: In-band vs Out-of-band 序列化

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        In-band 序列化 (安全)                                 │
│  通过 Ray 的 task/actor 参数和返回值传递 ObjectRef                           │
│  → Ray 自动追踪嵌套引用，正确管理生命周期                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                       Out-of-band 序列化 (危险)                              │
│  通过 cloudpickle/pickle 直接序列化 ObjectRef                                │
│  → Ray 无法追踪引用，需永久固定防止提前释放                                   │
└─────────────────────────────────────────────────────────────────────────────┘
```

### In-band 序列化详细流程 (Ray Core Task/Actor)

**场景: Task 返回包含 ObjectRef 的对象**

```python
import ray

@ray.remote
def create_nested_ref():
    inner_ref = ray.put({"data": "inner"})
    return {"outer": 123, "inner_ref": inner_ref}  # 返回包含 ObjectRef 的字典

result_ref = create_nested_ref.remote()
result = ray.get(result_ref)  # {"outer": 123, "inner_ref": ObjectRef(...)}
inner_data = ray.get(result["inner_ref"])  # {"data": "inner"}
```

**代码流程:**

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ 1. Task 执行完成，准备序列化返回值                                           │
│    python/ray/_private/serialization.py:606-621                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ 2. 调用 _serialize_to_pickle5()                                              │
│    → set_in_band_serialization()  # 标记为 in-band 模式                      │
│    → pickle.dumps(value)          # 序列化返回值                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ 3. pickle 遇到 ObjectRef 时，调用 object_ref_reducer()                       │
│    python/ray/_private/serialization.py:206-252                             │
│    → add_contained_object_ref(obj, allow_out_of_band_serialization=True)    │
│    → 因为 is_in_band_serialization() == True:                               │
│      self._thread_local.object_refs.add(object_ref)  # 收集嵌套引用          │
└─────────────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ 4. 序列化完成后，调用 get_and_clear_contained_object_refs()                  │
│    → 返回所有收集的嵌套 ObjectRef                                            │
│    → 创建 Pickle5SerializedObject(metadata, inband, writer, contained_refs) │
└─────────────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ 5. 写入 Object Store 时，嵌套引用被记录                                      │
│    python/ray/_raylet.pyx:3200-3246                                         │
│    → contained_object_ids = ObjectRefsToVector(contained_object_refs)       │
│    → SealOwned(..., contained_object_ids)                                   │
└─────────────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ 6. C++ 层记录嵌套引用关系                                                    │
│    src/ray/core_worker/reference_counter.cc                                 │
│    → AddNestedObjectIds(outer_id, contained_ids)                            │
│    → 建立 outer → inner 的依赖关系                                          │
└─────────────────────────────────────────────────────────────────────────────┘
```

**关键代码: _serialize_to_pickle5 (In-band 序列化)**

```python
# python/ray/_private/serialization.py:606-621
def _serialize_to_pickle5(self, metadata, value):
    writer = Pickle5Writer()
    try:
        # ⭐ 关键: 设置 in-band 模式
        self.set_in_band_serialization()

        # pickle 序列化时，遇到 ObjectRef 会调用 object_ref_reducer
        # object_ref_reducer 会调用 add_contained_object_ref
        # 因为是 in-band 模式，ObjectRef 会被收集到 _thread_local.object_refs
        inband = pickle.dumps(
            value, protocol=5, buffer_callback=writer.buffer_callback
        )
    except Exception as e:
        self.get_and_clear_contained_object_refs()
        raise e
    finally:
        # 序列化完成后恢复 out-of-band 模式
        self.set_out_of_band_serialization()

    # ⭐ 关键: 获取收集的嵌套引用，传递给 Pickle5SerializedObject
    return Pickle5SerializedObject(
        metadata, inband, writer, self.get_and_clear_contained_object_refs()
    )
```

**关键代码: add_contained_object_ref (In-band 分支)**

```python
# python/ray/_private/serialization.py:302-335
def add_contained_object_ref(
    self,
    object_ref: "ray.ObjectRef",
    *,
    allow_out_of_band_serialization: bool,
    call_site: Optional[str] = None,
):
    if self.is_in_band_serialization():
        # ⭐ In-band 模式: 收集嵌套引用，Ray 会追踪依赖关系
        # 这确保了 inner ObjectRef 的生命周期与 outer 对象绑定
        if not hasattr(self._thread_local, "object_refs"):
            self._thread_local.object_refs = set()
        self._thread_local.object_refs.add(object_ref)
    else:
        # Out-of-band 模式: 永久固定引用，防止提前释放
        if not allow_out_of_band_serialization:
            raise OufOfBandObjectRefSerializationException(...)
        else:
            # ⚠️ 永久固定引用，会导致内存泄漏
            ray._private.worker.global_worker.core_worker.add_object_ref_reference(
                object_ref
            )
```

**关键代码: C++ 层反序列化和注册借用关系**

```cpp
// src/ray/core_worker/core_worker.cc:947-953
void CoreWorker::DeserializeAndRegisterObjectRef(
    const ObjectID &object_id,
    const ObjectID &outer_object_id,  // 外层对象 ID
    const rpc::Address &owner_address,
    const std::string &serialized_object_status) {
  // 将 inner 对象注册为 outer 对象的借用
  // 这建立了依赖关系: inner 的生命周期与 outer 绑定
  reference_counter_->AddBorrowedObject(object_id, outer_object_id, owner_address);
  // ...
}
```

### In-band 序列化的生命周期保证

```
Owner Worker (创建 inner_ref 和 outer_ref)
┌────────────────────────────────────────────────────────────────┐
│ inner_ref = ray.put(data)           # Owner 追踪 inner_ref     │
│ outer_ref = ray.put({"ref": inner_ref})  # Owner 追踪 outer_ref│
│                                                                 │
│ ReferenceCounter 状态:                                          │
│   inner_ref: local_ref_count=1, contained_in_owned=[outer_ref] │
│   outer_ref: local_ref_count=1, nested_ids=[inner_ref]         │
└────────────────────────────────────────────────────────────────┘

Borrower Worker (接收 outer_ref)
┌────────────────────────────────────────────────────────────────┐
│ ray.get(outer_ref) → 反序列化                                   │
│   → DeserializeAndRegisterObjectRef(inner_ref, outer_ref, ...) │
│   → AddBorrowedObject(inner_ref, outer_ref, owner_address)     │
│                                                                 │
│ ReferenceCounter 状态:                                          │
│   inner_ref: borrowed, outer_id=outer_ref, owner=Owner         │
│   outer_ref: borrowed, owner=Owner                             │
│                                                                 │
│ 生命周期保证:                                                    │
│   - 只要 borrower 持有 outer_ref，inner_ref 也会被保持          │
│   - borrower 释放 outer_ref 时，会通知 owner                    │
│   - owner 收到通知后才可能释放 inner_ref                         │
└────────────────────────────────────────────────────────────────┘
```

### Out-of-band 序列化的问题 (Ray Data map/map_batches)

**为什么 Ray Data 的 map 是 out-of-band 序列化？**

```python
# Ray Data map 的 UDF 返回值处理
def map_udf(batch):
    ref = ray.put(data)
    return {"ref": ref}  # 这个 ref 如何被序列化？

# Ray Data 内部:
# 1. UDF 返回值 → BlockOutputBuffer
# 2. 转换为 Arrow Block
# 3. Arrow 不认识 ObjectRef → 走 ArrowPythonObjectArray
# 4. ArrowPythonObjectArray 使用 pickle_dumps() 序列化
# 5. pickle_dumps() 不是通过 Ray 的 _serialize_to_pickle5 调用的
# 6. 所以 is_in_band_serialization() == False
# 7. ObjectRef 被永久固定
```

**关键代码: ArrowPythonObjectArray.from_objects (Out-of-band)**

```python
# python/ray/data/_internal/object_extensions/arrow.py:106-119
class ArrowPythonObjectArray(pa.ExtensionArray):
    def from_objects(objects):
        type_ = ArrowPythonObjectType()
        all_dumped_bytes = []
        for obj in objects:
            # ⚠️ 直接调用 pickle_dumps，不是通过 Ray 的序列化上下文
            # 此时 is_in_band_serialization() == False
            dumped_bytes = pickle_dumps(
                obj, "Error pickling object to convert to Arrow"
            )
            all_dumped_bytes.append(dumped_bytes)
        arr = pa.array(all_dumped_bytes, type=type_.storage_type)
        return type_.wrap_array(arr)
```

### 对比总结

| 场景 | 序列化方式 | 引用追踪 | 泄漏风险 | 提前释放风险 |
|------|-----------|---------|---------|-------------|
| **Ray Core task 返回 ObjectRef** | In-band | ✅ 自动追踪 | 🟢 无 | 🟢 无 |
| **Ray Core task 返回 List[ObjectRef]** | In-band | ✅ 自动追踪 | 🟢 无 | 🟢 无 |
| **Ray Core task 参数传递 ObjectRef** | In-band | ✅ 自动追踪 | 🟢 无 | 🟢 无 |
| **Ray Core actor 方法返回 ObjectRef** | In-band | ✅ 自动追踪 | 🟢 无 | 🟢 无 |
| **ray.put() 包含 ObjectRef** | In-band | ✅ 自动追踪 | 🟢 无 | 🟢 无 |
| **Ray Data map 返回 ObjectRef** | Out-of-band | ❌ 不追踪 | 🔴 高 | 🟢 无(因为被永久固定) |
| **直接 cloudpickle.dumps(ObjectRef)** | Out-of-band | ❌ 不追踪 | 🔴 高 | 🟢 无(因为被永久固定) |

### 为什么 Ray Core 返回 List[ObjectRef] 也安全？

`object_ref_reducer` 被注册到 cloudpickle 的 dispatch 中，**每个** ObjectRef 被序列化时都会调用它：

```python
# python/ray/_private/serialization.py:206-254
def object_ref_reducer(obj):
    # 每个 ObjectRef 都会调用 add_contained_object_ref
    self.add_contained_object_ref(obj, ...)
    # ...

# 注册到 cloudpickle
self._register_cloudpickle_reducer(ray.ObjectRef, object_ref_reducer)
```

当序列化 `List[ObjectRef]` 时：
1. pickle 遍历 list 中的每个元素
2. 遇到 ObjectRef 时调用 `object_ref_reducer`
3. 每个 ObjectRef 都被收集到 `_thread_local.object_refs`
4. 最终所有嵌套引用都被正确追踪

```python
# ✅ 完全安全
@ray.remote
def return_list_of_refs():
    refs = [ray.put(i) for i in range(10)]
    return refs  # 每个 ref 都会被追踪

refs = ray.get(return_list_of_refs.remote())
for ref in refs:
    print(ray.get(ref))  # 正常工作，不会泄漏，不会提前释放
```

### 为什么 Ray Data map/map_batches 使用 Out-of-band 序列化？

**根本原因：Ray Data 的架构设计导致 UDF 返回值必须先转换为 Arrow Block**

```
Ray Data map UDF 处理链路:

UDF 返回值 {"ref": ObjectRef(...)}
        │
        ▼
BlockOutputBuffer.add(item)          # output_buffer.py:88
        │
        ▼
DelegatingBlockBuilder.add(item)     # delegating_block_builder.py:21
        │
        ▼
ArrowBlockBuilder.add(item)          # 继承自 TableBlockBuilder
        │
        ▼
ArrowBlockBuilder._table_from_pydict()  # arrow_block.py:166
        │
        ▼
convert_to_pyarrow_array(column_values, column_name)  # tensor_extensions/arrow.py:219
        │
        ├─ 尝试转换为 Arrow 原生类型 → 失败 (ObjectRef 不是 Arrow 类型)
        │
        ▼
ArrowPythonObjectArray.from_objects(column_values)  # object_extensions/arrow.py:106
        │
        ▼
pickle_dumps(obj, ...)               # ⚠️ 这是普通的 pickle，不是 Ray 的序列化！
        │
        ▼
object_ref_reducer() 被调用
        │
        ├─ is_in_band_serialization() == False  (因为不是通过 _serialize_to_pickle5)
        │
        ▼
add_object_ref_reference() 永久固定   # ⚠️ 泄漏！
```

**关键代码证据:**

```python
# python/ray/data/_internal/object_extensions/arrow.py:106-119
class ArrowPythonObjectArray(pa.ExtensionArray):
    def from_objects(objects):
        for obj in objects:
            # ⚠️ 直接调用 pickle_dumps
            # 此时 SerializationContext.is_in_band_serialization() == False
            # 因为这不是 Ray Core 的序列化路径
            dumped_bytes = pickle_dumps(obj, ...)
        # ...
```

```python
# python/ray/_private/serialization.py:302-335
def add_contained_object_ref(self, object_ref, ...):
    if self.is_in_band_serialization():  # ← Ray Data 场景这里是 False
        # In-band: 收集嵌套引用
        self._thread_local.object_refs.add(object_ref)
    else:
        # Out-of-band: 永久固定引用 (Ray Data map 走这里)
        ray._private.worker.global_worker.core_worker.add_object_ref_reference(
            object_ref
        )
```

**对比 Ray Core task 返回值:**

```python
# python/ray/_private/serialization.py:606-621
def _serialize_to_pickle5(self, metadata, value):
    try:
        self.set_in_band_serialization()  # ← 设置 in_band=True
        inband = pickle.dumps(value, ...)
    finally:
        self.set_out_of_band_serialization()
    return Pickle5SerializedObject(..., self.get_and_clear_contained_object_refs())
```

### 为什么 Ray Data 不能简单使用 In-band 序列化？

**问题的根源：`pickle_dumps()` 只是调用 `pickle.dumps()`，没有设置 in_band 模式**

```python
# ray/_common/serialization.py:21-26
def pickle_dumps(obj: Any, error_msg: str):
    try:
        return pickle.dumps(obj)  # ← 只是普通的 pickle，没有设置 in_band
    except ...
```

**理论上可以修复，但需要改动 Ray Data 的架构：**

```python
# 假设的修复方案 (目前代码没有这样做)
def from_objects(objects):
    context = ray._private.worker.global_worker.get_serialization_context()
    try:
        context.set_in_band_serialization()  # ← 需要添加
        for obj in objects:
            dumped_bytes = pickle.dumps(obj)  # 现在会收集 contained_object_refs
            all_dumped_bytes.append(dumped_bytes)
        contained_refs = context.get_and_clear_contained_object_refs()  # ← 需要添加
        # 但是问题来了：这些 contained_refs 怎么传递给 Block？
    finally:
        context.set_out_of_band_serialization()
```

**核心难点：即使收集了 `contained_object_refs`，还需要解决：**

1. **Arrow Block 不知道嵌套引用**
   - Arrow 只是存储 pickle 字节
   - Arrow 没有 `contained_object_refs` 的概念

2. **Block 的序列化不经过 Ray 的序列化上下文**
   - Block 是 Arrow Table，直接用 Arrow IPC 序列化
   - 不是用 `_serialize_to_pickle5()` 序列化

3. **需要额外的元数据追踪**
   - 需要在 Block 之外维护嵌套引用信息
   - 需要修改 RefBundle 来存储这些信息

```
当前架构:

Block (Arrow Table)  ─────────────────────────────▶  ray.put(block)
     │                                                    │
     │ 内部有 pickle 序列化的 ObjectRef                    │
     │ 但 Arrow 不知道                                    │
     ▼                                                    ▼
ObjectRef 被永久固定                               Block 的 ObjectRef
(因为是 out-of-band)                              (正确管理)

需要的架构:

Block + contained_refs ──────────────────────────▶  ray.put(block, nested_ids=contained_refs)
     │                                                    │
     │                                                    │
     ▼                                                    ▼
contained_refs 被正确追踪                          Block + nested_refs
                                                   都正确管理
```

**对比 Ray Core：** Ray Core 的 `ray.put()` 可以正确处理嵌套引用，因为它使用 `_serialize_to_pickle5()` 并传递 `contained_object_refs` 给 C++ 层。但 Ray Data 的 Block 序列化是通过 Arrow IPC，不经过这个流程。

### Ray Core Task/Actor 安全传递 ObjectRef 的完整示例

```python
import ray

ray.init()

# ✅ 安全: Task 返回嵌套 ObjectRef
@ray.remote
def create_nested():
    inner = ray.put({"inner": "data"})
    return {"outer": 123, "ref": inner}

result_ref = create_nested.remote()
result = ray.get(result_ref)
print(ray.get(result["ref"]))  # {"inner": "data"}

# ✅ 安全: Task 参数传递 ObjectRef
@ray.remote
def process_ref(obj_ref):
    data = ray.get(obj_ref)
    return data["inner"] + "_processed"

inner_ref = ray.put({"inner": "data"})
processed = ray.get(process_ref.remote(inner_ref))
print(processed)  # "data_processed"

# ✅ 安全: Task 返回 List[ObjectRef]
@ray.remote
def create_refs():
    refs = [ray.put(i) for i in range(3)]
    return refs  # 返回 ObjectRef 列表

refs = ray.get(create_refs.remote())
print(ray.get(refs))  # [0, 1, 2]

# ✅ 安全: Actor 方法返回 ObjectRef
@ray.remote
class DataStore:
    def __init__(self):
        self.data = {}

    def put(self, key, value):
        ref = ray.put(value)
        self.data[key] = ref
        return ref

    def get_ref(self, key):
        return self.data[key]

store = DataStore.remote()
ref = ray.get(store.put.remote("key1", {"value": 100}))
print(ray.get(ref))  # {"value": 100}

# 即使通过 get_ref 获取，也是安全的
ref2 = ray.get(store.get_ref.remote("key1"))
print(ray.get(ref2))  # {"value": 100}
```

### 正确做法 vs 错误做法

```python
import ray

# ❌ 错误：返回 ObjectRef，会导致泄漏
def bad_udf(batch):
    result = some_computation(batch)
    return {"ref": ray.put(result)}  # ObjectRef 被永久固定！

ds.map_batches(bad_udf)

# ❌ 错误：返回 ObjectRef 列表
def bad_udf2(batch):
    refs = [ray.put(x) for x in batch]
    return {"refs": refs}  # 所有 ObjectRef 被永久固定！

ds.map_batches(bad_udf2)

# ✅ 正确：返回实际数据
def good_udf(batch):
    result = some_computation(batch)
    return {"data": result}  # 直接返回数据

ds.map_batches(good_udf)

# ✅ 正确：如果需要传递 ObjectRef，在 UDF 内部解引用
def good_udf2(batch):
    refs = [ray.put(x) for x in batch]
    results = ray.get(refs)  # 在 UDF 内部解引用
    return {"data": results}

ds.map_batches(good_udf2)
```

### 数据传输流程

```
Worker A (执行 map UDF)          Worker B (下一个操作符)
┌────────────────────┐           ┌────────────────────┐
│ 1. 执行 UDF        │           │                    │
│ 2. 返回 ObjectRef  │           │                    │
│ 3. pickle 序列化   │           │                    │
│ 4. 存入 Block      │           │                    │
│ 5. ray.put(Block)  │──────────▶│ 6. ray.get(Block)  │
│    → ObjectRef[B]  │           │ 7. 反序列化 Block  │
└────────────────────┘           │ 8. 取出 ObjectRef  │
                                 │ 9. 使用需 ray.get()│
                                 └────────────────────┘

注意：
- Block 本身正常通过 ObjectRef 传输
- Block 内包含的 ObjectRef 被 pickle 序列化
- 内部的 ObjectRef 被永久固定在 Worker A
```

### 总结

| 问题 | 答案 |
|------|------|
| UDF 返回 ObjectRef 会自动 ray.get()? | **否**，会被 pickle 序列化存储 |
| 是否有泄漏风险? | **是**，ObjectRef 被永久固定直到 worker 退出 |
| 是否有提前释放风险? | **否**，因为被永久固定了 |
| 推荐做法 | 在 UDF 内部调用 `ray.get()` 获取实际数据再返回 |

---

## Ray Data 处理 ObjectRef 的 Workaround 方案

由于 Ray Data 的架构限制，如果需要在 map/map_batches 中处理 ObjectRef，可以使用以下 workaround 方案：

### 方案 1: 在 UDF 内部解引用 (推荐)

```python
# ❌ 错误：返回 ObjectRef
ds.map(lambda x: {"ref": ray.put(compute(x))})

# ✅ 正确：在 UDF 内部 ray.get()
ds.map(lambda x: {"data": compute(x)})  # 直接返回数据，不用 ray.put

# 如果确实需要中间 ray.put（比如大对象复用）
def udf(x):
    ref = ray.put(large_compute(x))
    result = ray.get(ref)  # 立即解引用
    return {"data": result}

ds.map(udf)
```

### 方案 2: 使用 from_arrow_refs / from_numpy_refs 输入

如果你已经有 `List[ObjectRef]`，用专门的 API 输入：

```python
# 假设你有一批 ObjectRef
refs = [ray.put(data) for data in large_dataset]

# ✅ 正确：使用专门的 API 输入（owns_blocks=False，不会泄漏）
ds = ray.data.from_arrow_refs(refs)

# 然后正常处理
ds = ds.map(lambda x: transform(x))
```

### 方案 3: 两阶段处理

如果必须在 map 中产生 ObjectRef 并在后续使用：

```python
# 阶段 1: 用 Ray Core task 生成 ObjectRef 列表
@ray.remote
def batch_process(items):
    return [ray.put(compute(x)) for x in items]

# 收集所有 ObjectRef
all_refs = []
for batch in ds.iter_batches(batch_size=1000):
    refs = ray.get(batch_process.remote(batch))
    all_refs.extend(refs)

# 阶段 2: 用 from_arrow_refs 输入
ds2 = ray.data.from_arrow_refs(all_refs)
```

### 方案 4: 使用 map_batches 返回完整数据

```python
# 如果需要并行计算，在 map_batches 内部完成所有工作
def batch_udf(batch):
    # 所有计算在这里完成
    results = []
    for item in batch:
        result = heavy_compute(item)  # 计算
        results.append(result)
    return {"result": results}  # 返回实际数据，不是 ObjectRef

ds.map_batches(batch_udf)
```

### 方案 5: 使用 Actor 缓存中间结果

如果需要跨 batch 共享大对象：

```python
@ray.remote
class DataCache:
    def __init__(self):
        self.cache = {}

    def put(self, key, value):
        self.cache[key] = value
        return key

    def get(self, key):
        return self.cache[key]

cache = DataCache.remote()

def udf_with_cache(batch):
    results = []
    for x in batch:
        # 计算并缓存
        key = ray.get(cache.put.remote(x["id"], compute(x)))
        # 返回 key 而不是 ObjectRef
        results.append({"cache_key": key})
    return results

# map 返回 key，后续通过 cache actor 获取
ds.map_batches(udf_with_cache)
```

### Workaround 方案对比

| 方案 | 适用场景 | 复杂度 | 推荐度 |
|------|---------|-------|-------|
| **方案 1: UDF 内解引用** | 大多数场景 | 低 | ⭐⭐⭐⭐⭐ |
| **方案 2: from_*_refs 输入** | 已有 ObjectRef 列表 | 低 | ⭐⭐⭐⭐⭐ |
| **方案 3: 两阶段处理** | 需要生成 ObjectRef | 中 | ⭐⭐⭐ |
| **方案 4: 返回完整数据** | 数据量不大 | 低 | ⭐⭐⭐⭐ |
| **方案 5: Actor 缓存** | 跨 batch 共享大对象 | 高 | ⭐⭐ |

**核心原则：不要让 ObjectRef 出现在 map/map_batches 的返回值中。**

---

## 总结

### Ray Core vs Ray Data 的 ObjectRef 处理

| 使用方式 | 是否安全 | 原因 |
|---------|---------|------|
| **Ray Core task 返回 ObjectRef** | ✅ 安全 | In-band 序列化，自动追踪嵌套引用 |
| **Ray Core task 返回 List[ObjectRef]** | ✅ 安全 | 每个 ObjectRef 都被 `object_ref_reducer` 处理 |
| **Ray Core actor 方法返回 ObjectRef** | ✅ 安全 | In-band 序列化，自动追踪嵌套引用 |
| **ray.put() 包含 ObjectRef** | ✅ 安全 | In-band 序列化，自动追踪嵌套引用 |
| **Ray Data map/map_batches 返回 ObjectRef** | ❌ 泄漏 | Out-of-band 序列化，永久固定 |
| **直接 cloudpickle.dumps(ObjectRef)** | ❌ 泄漏 | Out-of-band 序列化，永久固定 |

### 关键结论

1. **自己调用 `.remote()` 完全安全**
   - 无论返回单个 ObjectRef 还是 List[ObjectRef]
   - Ray Core 使用 in-band 序列化，自动追踪所有嵌套引用
   - 不会泄漏，不会提前释放

2. **Ray Data map/map_batches 返回 ObjectRef 会泄漏**
   - 因为架构限制，UDF 返回值必须转换为 Arrow Block
   - ObjectRef 不是 Arrow 类型，通过 `ArrowPythonObjectArray` pickle 存储
   - 这个 pickle 路径不经过 Ray 的 in-band 序列化
   - 导致 ObjectRef 被永久固定

3. **Ray Data 的 Workaround**
   - 推荐：在 UDF 内部解引用
   - 推荐：使用 `from_arrow_refs()` / `from_numpy_refs()` 输入已有 ObjectRef
   - 核心原则：**不要让 ObjectRef 出现在 map/map_batches 的返回值中**

### Ray Data 的 ObjectRef 管理机制

1. **传递安全**: `from_*_refs()` API 允许直接传递 ObjectRef，无需额外 `ray.put()`
2. **所有权清晰**: `owns_blocks` 标记明确区分共享和独占所有权
3. **多层保护**: Python GC + Ray Data eager_free + C++ 引用计数三层保护

### 主要风险来源

- **Ray Data map 返回 ObjectRef**: 会被永久固定，导致内存泄漏
- **直接用 cloudpickle 序列化 ObjectRef**: 同上
- **未清理全局缓存**: 使用完 Dataset 后应调用 `clear_cache()`
- **在 eager_free 模式下错误地持有引用**: 可能导致访问已释放的对象
