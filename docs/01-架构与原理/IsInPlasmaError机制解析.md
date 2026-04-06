# Ray Memory Store 中 IsInPlasmaError 机制深度解析

## 概述

在 Ray 的 `CoreWorkerMemoryStore` 中，`IsInPlasmaError()` 是连接内存存储层与 Plasma 共享内存存储层的关键机制。本文档详细解析其在 `memory_store.cc` 中的所有使用场景及底层实现原理。

---

## 1. IsInPlasmaError 的底层实现

### 1.1 方法定义

位于 `src/ray/common/ray_object.h:118`：

```cpp
/// Whether the object has been promoted to plasma (i.e., since it was too
/// large to return directly as part of a gRPC response).
bool IsInPlasmaError() const;
```

### 1.2 方法实现

位于 `src/ray/common/ray_object.cc:136-143`：

```cpp
bool RayObject::IsInPlasmaError() const {
  if (metadata_ == nullptr) {
    return false;
  }
  const std::string_view metadata(reinterpret_cast<const char *>(metadata_->Data()),
                                  metadata_->Size());
  return metadata == kObjectInPlasmaStr;
}
```

其中 `kObjectInPlasmaStr` 定义在 `src/ray/common/ray_object.cc:21-22`：

```cpp
static const std::string kObjectInPlasmaStr =
    std::to_string(ray::rpc::ErrorType::OBJECT_IN_PLASMA);
```

**核心逻辑：** 判断一个 `RayObject` 的 metadata 是否等于 `OBJECT_IN_PLASMA` 错误类型的字符串表示。如果相等，说明这个对象不在内存 store 中，而是存放在 Plasma（共享内存对象存储）中。

### 1.3 与 IsException 的关系

`IsException()` 的实现（`src/ray/common/ray_object.cc:113-134`）中，`OBJECT_IN_PLASMA` 同样会使其返回 `true`：

```cpp
bool RayObject::IsException(rpc::ErrorType *error_type) const {
  // ...省略部分代码...
  const std::string_view metadata(reinterpret_cast<const char *>(metadata_->Data()),
                                  metadata_->Size());
  if (metadata == kObjectInPlasmaStr) {
    if (error_type) {
      *error_type = rpc::ErrorType::OBJECT_IN_PLASMA;
    }
    return true;  // OBJECT_IN_PLASMA 也会让 IsException 返回 true
  }
  // ...后续检查其他 ErrorType...
}
```

**关键点：** `IsInPlasmaError()` 是 `IsException()` 的子集。`IsException()` 对 `OBJECT_IN_PLASMA` 也返回 `true`，因此在业务逻辑中需要用 `IsException() && !IsInPlasmaError()` 来区分"真正的异常"和"Plasma 占位符"。

---

## 2. 为什么叫 "Error" 却不是真正的错误？

这是 Ray 中的一个巧妙设计。`OBJECT_IN_PLASMA` 虽然属于 `rpc::ErrorType` 枚举，但它**不是真正的异常/错误**，而是一个**位置标记（location marker）**：

- Ray 的 Memory Store 只存放**小型对象**（直接在进程内存中）
- **大型对象**会被溢出到 Plasma 共享内存存储
- 当对象被提升到 Plasma 时，Memory Store 中会放入一个 `OBJECT_IN_PLASMA` 占位符，告诉后续的 `Get`/`Wait` 操作："这个对象不在我这里，你得去 Plasma 里找"

所以 `IsInPlasmaError()` 实际意思是：**"这是一个指向 Plasma 的占位符，不是对象的真实值"**。

---

## 3. OBJECT_IN_PLASMA 的写入来源

谁把 `OBJECT_IN_PLASMA` 写入 Memory Store？主要有 4 个入口：

### 3.1 CoreWorker::Get() 从 Plasma 读取后标记

位于 `src/ray/core_worker/core_worker.cc:1020`：

```cpp
memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
                   object_id,
                   reference_counter_->HasReference(object_id));
```

场景：当 `ray.get()` 从 Plasma 读取对象后，将 Memory Store 中的条目标记为 IN_PLASMA。

### 3.2 ObjectRecoveryManager 对象恢复时写入

位于 `src/ray/core_worker/object_recovery_manager.cc:86`：

```cpp
// 如果对象有 pinned 位置，写入 IN_PLASMA 占位符
in_memory_store_.Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
                     object_id,
                     reference_counter_.HasReference(object_id));
```

以及 `object_recovery_manager.cc:127`（Pin 已有副本成功时）：

```cpp
if (status.ok() && reply.successes(0)) {
  in_memory_store_.Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
                       object_id,
                       reference_counter_.HasReference(object_id));
  reference_counter_.UpdateObjectPinnedAtRaylet(object_id, node_id);
}
```

场景：对象恢复时，如果对象有 pinned 位置或成功 pin 了已有副本，写入 IN_PLASMA 占位符。

### 3.3 FutureResolver 解析 Future 时写入

位于 `src/ray/core_worker/future_resolver.h:49`，注释明确说明：

```cpp
/// Resolve the value for a future. This will periodically contact the given
/// owner until the owner dies or the owner has finished creating the object.
/// In either case, this will put an OBJECT_IN_PLASMA error as the future's
/// value.
```

场景：解析 future 时，写入 IN_PLASMA 占位符。

### 3.4 TaskManager 任务完成时写入

位于 `src/ray/core_worker/task_manager.cc:568`：

```cpp
in_memory_store_.Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA), ...);
```

场景：任务完成后，如果对象太大无法直接返回，写入 IN_PLASMA 占位符。

### 写入来源汇总

| 来源 | 文件 | 场景 |
|------|------|------|
| `CoreWorker::Get()` | `core_worker.cc:1020` | 从 Plasma 读取对象后标记 |
| `ObjectRecoveryManager` | `object_recovery_manager.cc:86,127` | 对象恢复时有 pinned 位置 |
| `FutureResolver` | `future_resolver.h:49` | 解析 future 时写入占位符 |
| `TaskManager` | `task_manager.cc:568` | 任务完成后对象太大无法直接返回 |

---

## 4. memory_store.cc 中的具体使用场景

### 4.1 GetRequest::Set() — Get 请求的提前终止判断

位于 `memory_store.cc:101-113`：

```cpp
void GetRequest::Set(const ObjectID &object_id, std::shared_ptr<RayObject> object) {
  std::scoped_lock<std::mutex> lock(mutex_);
  if (is_ready_) {
    return;  // We have already hit the number of objects to return limit.
  }
  object->SetAccessed();
  objects_.emplace(object_id, object);
  if (objects_.size() == num_objects_ ||
      (abort_if_any_object_is_exception_ && object->IsException() &&
       !object->IsInPlasmaError())) {   // ← 关键判断
    is_ready_ = true;
    cv_.notify_all();
  }
}
```

**逻辑解析：**

当 `ray.get()` 等待多个对象时，`GetRequest::Set()` 在每个对象到达时被调用。`is_ready_` 变为 `true` 有两个条件（满足其一即可）：

1. `objects_.size() == num_objects_`：所有请求的对象都已到达
2. `abort_if_any_object_is_exception_ && object->IsException() && !object->IsInPlasmaError()`：启用了异常终止且当前对象是真正的异常

**`!IsInPlasmaError()` 的作用：** 如果某个对象只是 `OBJECT_IN_PLASMA` 占位符，说明数据在 Plasma 里，调用者还需要去 Plasma 读取真实值，不能就此终止等待。只有非 Plasma 的真正异常（如 task 抛出异常、worker crash）才会触发提前终止。

**例子：** 假设 `ray.get([obj1, obj2])`，其中 `obj1` 返回了 `OBJECT_IN_PLASMA`，`obj2` 还未就绪：
- `obj1->IsException()` 返回 `true`（因为 OBJECT_IN_PLASMA 也是 ErrorType）
- `obj1->IsInPlasmaError()` 返回 `true`
- 因此 `!obj1->IsInPlasmaError()` 返回 `false`
- 整个条件为 `false`，不会提前终止，继续等待 `obj2`

而如果 `obj1` 返回了 `TASK_EXECUTION_EXCEPTION`：
- `obj1->IsException()` 返回 `true`
- `obj1->IsInPlasmaError()` 返回 `false`
- 因此 `!obj1->IsInPlasmaError()` 返回 `true`
- 整个条件为 `true`，提前终止等待并返回错误

### 4.2 GetImpl() — 扫描已有对象时的相同逻辑

位于 `memory_store.cc:276-302`：

```cpp
// Check for existing objects and see if this get request can be fullfilled.
for (size_t i = 0; i < object_ids.size(); i++) {
  const auto &object_id = object_ids[i];
  auto iter = objects_.find(object_id);
  if (iter != objects_.end()) {
    iter->second->SetAccessed();
    (*results)[i] = iter->second;
    num_found += 1;
    if (abort_if_any_object_is_exception && iter->second->IsException() &&
        !iter->second->IsInPlasmaError()) {   // ← 同样的过滤逻辑
      existing_objects_has_exception = true;
    }
  } else {
    remaining_ids.insert(object_id);
  }
  // Only wait sets at_most_num_objects to false.
  if (num_found >= num_objects && at_most_num_objects) {
    break;
  }
}

// Return if all the objects are obtained, or any existing objects are known to have
// exception.
if (remaining_ids.empty() || num_found >= num_objects ||
    existing_objects_has_exception) {
  return Status::OK();
}
```

**逻辑解析：**

在创建 `GetRequest` 之前，先扫描 Memory Store 中已有的对象。只有**真正的异常**才设置 `existing_objects_has_exception = true`，从而跳过等待直接返回。`OBJECT_IN_PLASMA` 不算异常，因为对象的值可以在 Plasma 中找到。

**执行流程：**
1. 遍历所有请求的 object_id
2. 如果在 Memory Store 中找到，记录到 results 并增加 `num_found`
3. 如果对象是真正异常（排除 IN_PLASMA），标记 `existing_objects_has_exception = true`
4. 如果所有对象已找到，或达到所需数量，或存在真正异常 → 直接返回 OK
5. 否则，创建 `GetRequest` 等待剩余对象

### 4.3 Get() 重载版本 — 异常标记

位于 `memory_store.cc:388-410`：

```cpp
Status CoreWorkerMemoryStore::Get(
    const absl::flat_hash_set<ObjectID> &object_ids,
    int64_t timeout_ms,
    const WorkerContext &ctx,
    absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> *results,
    bool *got_exception) {
  const std::vector<ObjectID> id_vector(object_ids.begin(), object_ids.end());
  std::vector<std::shared_ptr<RayObject>> result_objects;
  RAY_RETURN_NOT_OK(Get(id_vector, id_vector.size(), timeout_ms, ctx, &result_objects));

  for (size_t i = 0; i < id_vector.size(); i++) {
    if (result_objects[i] != nullptr) {
      (*results)[id_vector[i]] = result_objects[i];
      if (result_objects[i]->IsException() && !result_objects[i]->IsInPlasmaError()) {
        // Can return early if an object value contains an exception.
        // InPlasmaError does not count as an exception because then the object
        // value should then be found in plasma.
        *got_exception = true;
      }
    }
  }
  return Status::OK();
}
```

**逻辑解析：**

这是 `Get` 的另一个重载版本，额外返回 `got_exception` 标志。注释直接说明：**InPlasmaError 不算异常，因为对象的值可以在 Plasma 中找到。**

**`got_exception` 的用途：** 调用者（如 `core_worker.cc`）根据此标志决定是否需要继续去 Plasma 读取。如果 `got_exception = true`，说明有真正的异常，可以直接返回错误；如果 `got_exception = false` 但某些对象是 `OBJECT_IN_PLASMA`，则需要去 Plasma 读取。

### 4.4 Wait() — 区分 ready 和 plasma 对象

位于 `memory_store.cc:412-442`：

```cpp
Status CoreWorkerMemoryStore::Wait(const absl::flat_hash_set<ObjectID> &object_ids,
                                   int num_objects,
                                   int64_t timeout_ms,
                                   const WorkerContext &ctx,
                                   absl::flat_hash_set<ObjectID> *ready,
                                   absl::flat_hash_set<ObjectID> *plasma_object_ids) {
  std::vector<ObjectID> id_vector(object_ids.begin(), object_ids.end());
  std::vector<std::shared_ptr<RayObject>> result_objects;
  auto status = GetImpl(id_vector,
                        num_objects,
                        timeout_ms,
                        ctx,
                        &result_objects,
                        /*abort_if_any_object_is_exception=*/false,  // Wait 不因异常提前终止
                        /*at_most_num_objects=*/false);
  // Ignore TimedOut statuses since we return ready objects explicitly.
  if (!status.IsTimedOut()) {
    RAY_RETURN_NOT_OK(status);
  }
  for (size_t i = 0; i < id_vector.size(); i++) {
    if (result_objects[i] != nullptr) {
      if (result_objects[i]->IsInPlasmaError()) {
        plasma_object_ids->insert(id_vector[i]);   // ← Plasma 对象单独归类
      } else if (ready->size() < static_cast<size_t>(num_objects)) {
        ready->insert(id_vector[i]);               // ← 非 Plasma 对象归入 ready
      }
    }
  }
  return Status::OK();
}
```

**逻辑解析：**

`ray.wait()` 需要将对象分为"已就绪"和"未就绪"两类。这里的处理与 `Get` 不同：

- `IsInPlasmaError() == true` → 归入 `plasma_object_ids` 集合
- 其他非空对象 → 归入 `ready` 集合（受 `num_objects` 限制）

注意 `GetImpl` 调用时 `abort_if_any_object_is_exception=false`，这意味着 `Wait` 不会因为异常而提前终止——它只是分类，不决定是否继续等待。

**Plasma 对象为什么不算 ready？** 因为 `ray.wait()` 的语义是判断对象是否**立即可用**。Plasma 中的对象需要额外的 I/O 操作才能读取，不算"立即可用"。

### 4.5 Delete() — 分流删除处理

位于 `memory_store.cc:444-459`：

```cpp
void CoreWorkerMemoryStore::Delete(const absl::flat_hash_set<ObjectID> &object_ids,
                                   absl::flat_hash_set<ObjectID> *plasma_ids_to_delete) {
  absl::MutexLock lock(&mu_);
  for (const auto &object_id : object_ids) {
    auto it = objects_.find(object_id);
    if (it != objects_.end()) {
      if (it->second->IsInPlasmaError()) {
        plasma_ids_to_delete->insert(object_id);  // ← 收集 Plasma ID，交给 Plasma 层删除
      } else {
        OnDelete(it->second);                     // ← 直接删除内存中的对象
        EraseObjectAndUpdateStats(object_id);
      }
    }
  }
}
```

**逻辑解析：**

删除操作根据 `IsInPlasmaError()` 分流：

- **Plasma 占位符**：只收集 object_id 到 `plasma_ids_to_delete`，由调用者（如 `core_worker.cc`）负责向 Plasma 发起真正的删除操作。Memory Store 中不执行 `OnDelete` 和统计更新，因为占位符本身不持有真实数据。
- **本地内存对象**：执行 `OnDelete()`（检查未处理异常通知）和 `EraseObjectAndUpdateStats()`（更新统计计数）。

### 4.6 Contains() — 标记对象位置

位于 `memory_store.cc:473-483`：

```cpp
bool CoreWorkerMemoryStore::Contains(const ObjectID &object_id, bool *in_plasma) {
  absl::MutexLock lock(&mu_);
  auto it = objects_.find(object_id);
  if (it != objects_.end()) {
    if (it->second->IsInPlasmaError()) {
      *in_plasma = true;  // ← 标记对象在 Plasma 中
    }
    return true;
  }
  return false;
}
```

**逻辑解析：**

`Contains` 返回对象是否在 Memory Store 中（包括占位符）。通过 `in_plasma` 输出参数告诉调用者：对象虽然在 Memory Store 中有条目，但真实数据在 Plasma 里。

### 4.7 EraseObjectAndUpdateStats() — 统计分流

位于 `memory_store.cc:518-533`：

```cpp
inline void CoreWorkerMemoryStore::EraseObjectAndUpdateStats(const ObjectID &object_id) {
  auto it = objects_.find(object_id);
  if (it == objects_.end()) {
    return;
  }

  if (it->second->IsInPlasmaError()) {
    num_in_plasma_ -= 1;       // ← Plasma 占位符只减 in_plasma 计数
  } else {
    num_local_objects_ -= 1;   // ← 本地对象减 local_objects 计数和字节数
    num_local_objects_bytes_ -= it->second->GetSize();
  }
  RAY_CHECK(num_in_plasma_ >= 0 && num_local_objects_ >= 0 &&
            num_local_objects_bytes_ >= 0);
  objects_.erase(it);
}
```

### 4.8 EmplaceObjectAndUpdateStats() — 插入时统计分流

位于 `memory_store.cc:535-548`：

```cpp
inline void CoreWorkerMemoryStore::EmplaceObjectAndUpdateStats(
    const ObjectID &object_id, std::shared_ptr<RayObject> &object_entry) {
  auto inserted = objects_.emplace(object_id, object_entry).second;
  if (inserted) {
    if (object_entry->IsInPlasmaError()) {
      num_in_plasma_ += 1;       // ← Plasma 占位符只增 in_plasma 计数
    } else {
      num_local_objects_ += 1;   // ← 本地对象增 local_objects 计数和字节数
      num_local_objects_bytes_ += object_entry->GetSize();
    }
  }
  RAY_CHECK(num_in_plasma_ >= 0 && num_local_objects_ >= 0 &&
            num_local_objects_bytes_ >= 0);
}
```

**统计分流总结：** Memory Store 维护两组统计：
- `num_in_plasma_`：Plasma 占位符数量（不计大小，因为占位符没有实际数据）
- `num_local_objects_` / `num_local_objects_bytes_`：本地内存对象的数量和字节大小

---

## 5. 整体流程图

```
Task 执行完成，返回大对象
         │
         ▼
对象被 Put 到 Plasma 共享内存
         │
         ▼
Memory Store 中写入 OBJECT_IN_PLASMA 占位符
（通过 TaskManager / ObjectRecoveryManager / FutureResolver 等写入）
         │
         ▼
ray.get() 访问 Memory Store
         │
         ├─ 发现 IsInPlasmaError() == true
         │   → 不视为异常，继续去 Plasma 读取真实数据
         │
         ├─ 发现 IsException() && !IsInPlasmaError()
         │   → 真正的异常，立即终止等待并返回错误
         │
         └─ 发现正常对象
             → 直接返回数据

ray.wait() 访问 Memory Store
         │
         ├─ IsInPlasmaError() == true
         │   → 归入 plasma_object_ids（不算 ready）
         │
         └─ 非 Plasma 对象
             → 归入 ready 集合

Delete 操作
         │
         ├─ IsInPlasmaError() == true
         │   → 只收集 ID，交给 Plasma 层删除
         │
         └─ 本地对象
             → 直接删除 + 更新统计
```

---

## 6. 总结

`IsInPlasmaError()` 是 Ray 两层对象存储（Memory Store + Plasma）之间的桥梁，核心作用是：

1. **位置标记**：标记"数据不在这里，去 Plasma 找"
2. **异常过滤**：在所有异常判断中排除 `OBJECT_IN_PLASMA`，避免将位置标记误判为真正的执行错误
3. **操作分流**：在 Get/Wait/Delete/Contains/统计等操作中，根据是否为 Plasma 占位符走不同处理路径
4. **统计隔离**：Plasma 占位符和本地对象使用不同的计数器，避免大小统计混淆

**一句话：`IsInPlasmaError()` 在 Memory Store 中的作用是"我知道这个对象在哪——它在 Plasma 里，别把它当成错误"。**
