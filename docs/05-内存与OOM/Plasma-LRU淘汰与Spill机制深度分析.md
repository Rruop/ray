# Plasma Store LRU 淘汰、Spill 与 OOM 处理完整分析

## 目录

1. [整体架构](#1-整体架构)
2. [Plasma 容量与内存分配](#2-plasma-容量与内存分配)
3. [LRU 数据结构与核心操作](#3-lru-数据结构与核心操作)
4. [对象生命周期与 ref_count](#4-对象生命周期与-ref_count)
5. [第1阶段：CreateObjectInternal — LRU 淘汰](#5-第1阶段createobjectinternal--lru-淘汰)
6. [第2阶段：CreateRequestQueue — GC/Spill/Grace Period/Fallback](#6-第2阶段createrequestqueue--gcspillgrace-periodfallback)
7. [Spill 机制详解](#7-spill-机制详解)
8. [淘汰 vs Spill 的关系](#8-淘汰-vs-spill-的关系)
9. [RequireSpace 核心公式与场景推演](#9-requirespace-核心公式与场景推演)
10. ["LRU 淘汰不了那么多"的完整代码路径](#10-lru-淘汰不了那么多的完整代码路径)
11. ["只需少量空间，要求淘汰 20%"的代码路径](#11-只需少量空间要求淘汰-20-的代码路径)
12. [Unpin 不主动释放内存](#12-unpin-不主动释放内存)
13. [Dashboard Object Store Memory 指标追踪](#13-dashboard-object-store-memory-指标追踪)
14. [关键数值汇总](#14-关键数值汇总)
15. [源码索引](#15-源码索引)
16. [Primary Object Spill 后的 Delete 流程](#16-primary-object-spill-后的-delete-流程)
17. [Replicated Object Spill 后的 Delete 流程](#17-replicated-object-spill-后的-delete-流程)
18. [HandleObjectMissing vs HandleObjectFreed](#18-handleobjectmissing-vs-handleobjectfreed)
19. [Pull 依赖对象的完整生命周期](#19-pull-依赖对象的完整生命周期)
20. [CancelPull 的必要性](#20-cancelpull-的必要性)
21. [Fused Spill 文件与指标语义](#21-fused-spill-文件与指标语义)
22. [Spill 指标体系](#22-spill-指标体系)
23. [代码修改记录](#23-代码修改记录)

---

## 1. 整体架构

Plasma Store 的 OOM 处理分为两个阶段，由不同组件负责：

```
新对象 Create 请求
        │
        ▼
┌─────────────────────────────────────────────────────────────┐
│ 第1阶段: CreateObjectInternal (obj_lifecycle_mgr.cc:177)    │
│                                                             │
│  在 Plasma Store 线程内同步执行                              │
│  循环最多 11 次 (num_tries=0..10):                          │
│    1. CreateObject(fallback=false) → 尝试分配内存            │
│    2. RequireSpace → LRU 选择淘汰对象                        │
│    3. EvictObjects → 物理删除 LRU 中的对象                   │
│    4. 重试 CreateObject                                      │
│                                                             │
│  如果 11 轮后仍 OOM:                                        │
│    allow_fallback=false → 返回 nullptr                       │
│    allow_fallback=true  → 磁盘 mmap 分配                     │
└──────────────────────┬──────────────────────────────────────┘
                       │ 返回 nullptr (OOM)
                       ▼
┌─────────────────────────────────────────────────────────────┐
│ 第2阶段: CreateRequestQueue::ProcessRequests (queue.cc:84)   │
│                                                             │
│  在 Plasma Store 线程内执行，通过回调触发 raylet 线程的操作   │
│  Step 1: trigger_global_gc_() → 全局 GC                     │
│  Step 2: spill_objects_callback_() → Spill                 │
│  Step 3: grace period 等待                                  │
│  Step 4: ProcessRequest(fallback=true) → 磁盘分配            │
└─────────────────────────────────────────────────────────────┘
```

**关键设计原则**：LRU 淘汰是同步的、即时生效的；Spill 是异步的、需要等待完成的。

---

## 2. Plasma 容量与内存分配

### 2.1 FootprintLimit

```cpp
// plasma_allocator.h:96
const int64_t kFootprintLimit;  // 常量，初始化后不可变
```

Plasma Store 的容量上限 `kFootprintLimit` 在 `PlasmaAllocator` 构造时确定，之后不变。

**来源**（raylet/main.cc 中初始化）：
```cpp
// 由系统内存或 /dev/shm 大小计算得出
// 通常为系统可用共享内存的 90%
```

### 2.2 Allocated 追踪

```cpp
// plasma_allocator.h:99
int64_t allocated_;  // 当前已分配的总字节数（含 fallback 分配）

// plasma_allocator.cc:102-109
std::optional<Allocation> PlasmaAllocator::Allocate(size_t bytes) {
  void *mem = dlmemalign(kAlignment, bytes);
  if (!mem) {
    return absl::nullopt;  // 分配失败
  }
  allocated_ += bytes;     // ★ 分配成功后才计数
  return BuildAllocation(mem, bytes, false);
}

// plasma_allocator.cc:138-143
void PlasmaAllocator::Free(Allocation allocation) {
  dlfree(allocation.address_);
  allocated_ -= allocation.size_;  // ★ free 时减少计数
  if (internal::IsOutsideInitialAllocation(allocation.address_)) {
    fallback_allocated_ -= allocation.size_;
  }
}
```

**注意**：`Allocated()` 反映的是通过 dlmalloc 实际分配出去的内存总和，包括正常分配和 fallback 分配。

### 2.3 dlmalloc 与碎片问题

```cpp
// plasma_allocator.h:58-64 注释
/// NOTE: due to fragmentation, there is a possibility that the
/// allocator has the capacity but fails to fulfill the allocation
/// request.
```

dlmalloc 是一个通用内存分配器，可能产生内存碎片。**即使 `Allocated() < FootprintLimit`（账面有空间），`dlmemalign` 也可能返回 `nullptr`（碎片导致无法找到足够大的连续空闲块）**。

这是第1阶段需要循环重试的根本原因——free 内存后 dlmalloc 可能合并空闲块，使下一次分配成功。

### 2.4 内存分配与释放的完整路径

```
Create 请求 → PlasmaStore::CreateObject → ObjectLifecycleManager::CreateObject
  → CreateObjectInternal → ObjectStore::CreateObject → PlasmaAllocator::Allocate
    → dlmemalign(64, object_size) → 成功: allocated_ += bytes, 返回 entry
                                   → 失败: 返回 nullptr

Evict → ObjectLifecycleManager::EvictObjects → DeleteObjectInternal
  → ObjectStore::DeleteObject → PlasmaAllocator::Free
    → dlfree(ptr) → allocated_ -= bytes
```

---

## 3. LRU 数据结构与核心操作

### 3.1 LRUCache 数据结构

```cpp
// eviction_policy.h:89-114
class LRUCache {
 private:
  /// 双向链表，存储 (ObjectID, size) 对，按 LRU 顺序排列
  /// 链表头部 = 最近使用，链表尾部 = 最久未使用
  typedef std::list<std::pair<ObjectID, int64_t>> ItemList;
  ItemList item_list_;

  /// 哈希表，ObjectID → 链表迭代器，O(1) 查找
  absl::flat_hash_map<ObjectID, ItemList::iterator> item_map_;

  const int64_t original_capacity_;  // 初始容量 = FootprintLimit
  int64_t capacity_;                 // 当前容量（可调整）
  int64_t used_capacity_;           // LRU 中对象占用的总大小
};
```

**重要**：LRU 的 `used_capacity_` 和 allocator 的 `Allocated()` 是不同的：
- `Allocated()` = dlmalloc 实际分配的总内存（包括被 pin 的和 LRU 中的）
- `LRUCache::used_capacity_` = 仅 LRU 中的对象大小（即 ref_count==0 的 sealed 对象）

关系：`Allocated() = pinned_memory_bytes_ + LRU::used_capacity_ + unsealed_memory`

### 3.2 Add — 对象创建时加入 LRU

```cpp
// eviction_policy.cc:27-32
void LRUCache::Add(const ObjectID &key, int64_t size) {
  RAY_CHECK(item_map_.find(key) == item_map_.end());  // 不允许重复
  item_list_.emplace_front(key, size);   // 插入链表头部（最近使用）
  item_map_.emplace(key, item_list_.begin());
  used_capacity_ += size;
}
```

调用时机：`ObjectLifecycleManager::CreateObject` 成功后：

```cpp
// obj_lifecycle_mgr.cc:35-37
auto entry = CreateObjectInternal(object_info, source, fallback_allocator);
if (entry == nullptr) {
  return {nullptr, PlasmaError::OutOfMemory};
}
eviction_policy_->ObjectCreated(object_info.object_id);  // ★ 加入 LRU
```

### 3.3 Remove — 从 LRU 中移除

```cpp
// eviction_policy.cc:34-43
int64_t LRUCache::Remove(const ObjectID &key) {
  auto it = item_map_.find(key);
  if (it == item_map_.end()) {
    return -1;  // 不在 LRU 中
  }
  int64_t size = it->second->second;
  used_capacity_ -= size;
  item_list_.erase(it->second);
  item_map_.erase(it);
  return size;
}
```

调用场景：
1. `BeginObjectAccess` — 对象被 Get 使用，ref_count 从 0→1，从 LRU 移除
2. `RemoveObject` — 对象被物理删除（淘汰或 abort），从 LRU 移除
3. `ChooseObjectsToEvict` — 淘汰后从 LRU 移除

### 3.4 BeginObjectAccess / EndObjectAccess — Pin/Unpin 与 LRU 联动

```cpp
// eviction_policy.cc:146-157
void EvictionPolicy::BeginObjectAccess(const ObjectID &object_id) {
  // 对象开始被使用 → 从 LRU 移除（不可被淘汰）
  cache_.Remove(object_id);
  pinned_memory_bytes_ += GetObjectSize(object_id);
}

void EvictionPolicy::EndObjectAccess(const ObjectID &object_id) {
  auto size = GetObjectSize(object_id);
  // 对象不再被使用 → 加入 LRU（可被淘汰）
  cache_.Add(object_id, size);
  pinned_memory_bytes_ -= size;
}
```

**状态转换图**：

```
                    AddReference (ref_count: 0→1)
                    BeginObjectAccess: 从 LRU 移除
                 ┌──────────────────────────────┐
                 │                              │
                 ▼                              │
  ┌──────────┐    ┌──────────────┐    ┌─────────┴───────┐
  │ 在 LRU 中 │    │  被创建(unsealed) │    │  被 Pin (ref≥1)  │
  │ ref=0    │◄───│  ref=1 (creator)│───►│  不可被淘汰      │
  │ 可被淘汰  │    └──────────────┘    └─────────────────┘
  └──────────┘                              │
       │                                    │
       │ RemoveReference (ref_count: 1→0)   │
       │ EndObjectAccess: 加入 LRU          │
       └────────────────────────────────────┘
```

### 3.5 ChooseObjectsToEvict — 选择淘汰对象

```cpp
// eviction_policy.cc:76-87
int64_t LRUCache::ChooseObjectsToEvict(int64_t num_bytes_required,
                                       std::vector<ObjectID> &objects_to_evict) {
  int64_t bytes_evicted = 0;
  auto it = item_list_.end();  // 从链表尾部（最久未使用）开始
  while (bytes_evicted < num_bytes_required && it != item_list_.begin()) {
    it--;
    objects_to_evict.push_back(it->first);  // 加入淘汰列表
    bytes_evicted += it->second;             // 累加淘汰大小
    bytes_evicted_total_ += it->second;
    num_evictions_total_ += 1;
  }
  return bytes_evicted;
}
```

**关键行为**：
- 从链表尾部（最久未使用）向前遍历
- 直到累计淘汰大小 ≥ `num_bytes_required` **或** 遍历完所有 LRU 对象
- **LRU 中对象不够时**：遍历完所有对象后返回，`bytes_evicted` < `num_bytes_required`
- **LRU 为空时**：不进入 while 循环，返回 `bytes_evicted = 0`

`EvictionPolicy::ChooseObjectsToEvict` 包装层：

```cpp
// eviction_policy.cc:98-107
int64_t EvictionPolicy::ChooseObjectsToEvict(int64_t num_bytes_required,
                                             std::vector<ObjectID> &objects_to_evict) {
  int64_t bytes_evicted =
      cache_.ChooseObjectsToEvict(num_bytes_required, objects_to_evict);
  // 从 LRU 中移除被选中的对象
  for (auto &object_id : objects_to_evict) {
    cache_.Remove(object_id);
  }
  return bytes_evicted;
}
```

### 3.6 RequireSpace — 淘汰的入口

```cpp
// eviction_policy.cc:117-131
int64_t EvictionPolicy::RequireSpace(int64_t size,
                                     std::vector<ObjectID> &objects_to_evict) {
  // 1. 计算账面缺口
  int64_t required_space = allocator_.Allocated() + size - allocator_.GetFootprintLimit();

  // 2. 淘汰目标 = max(账面缺口, 总容量的 20%)
  int64_t space_to_free = std::max(required_space, allocator_.GetFootprintLimit() / 5);

  // 3. 从 LRU 尾部选择对象淘汰
  int64_t num_bytes_evicted = ChooseObjectsToEvict(space_to_free, objects_to_evict);

  // 4. 返回 still_needed = required_space - num_bytes_evicted
  //    注意：用的是 required_space 而非 space_to_free！
  return required_space - num_bytes_evicted;
}
```

**核心公式详解**：

```
required_space  = Allocated + new_object_size - FootprintLimit
space_to_free   = max(required_space, FootprintLimit / 5)
num_bytes_evicted = LRU 实际选出的淘汰大小
返回值 = required_space - num_bytes_evicted
```

| 参数 | 含义 |
|------|------|
| `Allocated` | dlmalloc 当前已分配的总字节数 |
| `size` | 新对象需要的字节数 |
| `FootprintLimit` | Plasma Store 容量上限 |
| `required_space` | 账面缺口（可能为负数，表示账面有空间） |
| `space_to_free` | 实际告诉 LRU 要淘汰多少 |
| `num_bytes_evicted` | LRU 实际能淘汰多少 |

---

## 4. 对象生命周期与 ref_count

### 4.1 AddReference — ref_count 增加

```cpp
// obj_lifecycle_mgr.cc:105-119
bool ObjectLifecycleManager::AddReference(const ObjectID &object_id) {
  auto entry = object_store_->GetObject(object_id);
  if (!entry) {
    return false;
  }
  // ★ ref_count 从 0→1 时，从 LRU 中移除（不可再被淘汰）
  if (entry->ref_count_ == 0) {
    eviction_policy_->BeginObjectAccess(object_id);
  }
  entry->ref_count_++;
  return true;
}
```

**调用时机**：
- Plasma Get 请求 → `PlasmaStore::AddToClientObjectIds` → `AddReference`
- Pull 管理器 Pin 对象 → `TryPinObject` → `AddReference`
- Raylet Pin 主副本 → `PinObjectsAndWaitForFree` → `AddReference`

### 4.2 RemoveReference — ref_count 减少

```cpp
// obj_lifecycle_mgr.cc:121-148
bool ObjectLifecycleManager::RemoveReference(const ObjectID &object_id) {
  auto entry = object_store_->GetObject(object_id);
  entry->ref_count_--;

  if (entry->ref_count_ > 0) {
    return true;  // 还有引用，不做特殊处理
  }

  // ★ ref_count 降到 0 → 加入 LRU（可被淘汰）
  eviction_policy_->EndObjectAccess(object_id);

  // 如果对象在 earger_deletion_objects_ 中，立即删除
  if (earger_deletion_objects_.count(object_id) > 0) {
    DeleteObjectInternal(object_id);
  }
  return true;
}
```

**调用时机**：
- Plasma Release 请求 → `PlasmaStore::RemoveFromClientObjectIds` → `RemoveReference`
- Pull 管理器 Unpin → `UnpinObject` → `RemoveReference`
- Raylet 释放主副本 → `ReleaseFreedObject` → `RemoveReference`

### 4.3 ref_count 与 LRU 的状态对应

```
ref_count 状态        LRU 状态           可淘汰性
─────────────────────────────────────────────────
0                    在 LRU 中            ✓ 可淘汰
1 (被 creator 持有)   不在 LRU 中           ✗ 不可淘汰
>1 (多引用)          不在 LRU 中           ✗ 不可淘汰
```

### 4.4 EvictObjects 的前置条件

```cpp
// obj_lifecycle_mgr.cc:254-267
void ObjectLifecycleManager::EvictObjects(const std::vector<ObjectID> &object_ids) {
  for (const auto &object_id : object_ids) {
    auto entry = object_store_->GetObject(object_id);
    RAY_CHECK(entry != nullptr)              // 对象必须存在
        << "To evict an object it must be in the object table.";
    RAY_CHECK(entry->state_ == ObjectState::PLASMA_SEALED)  // 必须 sealed
        << "To evict an object it must have been sealed.";
    RAY_CHECK(entry->ref_count_ == 0)        // 必须 ref_count==0
        << "To evict an object, there must be no clients currently using it.";

    DeleteObjectInternal(object_id);  // 物理删除
  }
}
```

### 4.5 DeleteObjectInternal — 物理删除

```cpp
// obj_lifecycle_mgr.cc:269-282
void ObjectLifecycleManager::DeleteObjectInternal(const ObjectID &object_id) {
  auto entry = object_store_->GetObject(object_id);
  bool aborted = entry->state_ == ObjectState::PLASMA_CREATED;

  stats_collector_->OnObjectDeleting(*entry);
  earger_deletion_objects_.erase(object_id);
  eviction_policy_->RemoveObject(object_id);  // 从 LRU 移除（如果还在）
  object_store_->DeleteObject(object_id);      // dlfree + 从 object_table_ 删除

  if (!aborted) {
    delete_object_callback_(object_id);  // 通知 raylet 对象已删除
  }
}
```

`object_store_->DeleteObject` 的实现：

```cpp
// object_store.cc:68-74
bool ObjectStore::DeleteObject(const ObjectID &object_id) {
  auto entry = GetMutableObject(object_id);
  allocator_.Free(std::move(entry->allocation_));  // ★ dlfree + allocated_ -= size
  object_table_.erase(object_id);                    // 从对象表中删除
  return true;
}
```

---

## 5. 第1阶段：CreateObjectInternal — LRU 淘汰

### 5.1 完整代码

```cpp
// obj_lifecycle_mgr.cc:177-204
const LocalObject *ObjectLifecycleManager::CreateObjectInternal(
    const ray::ObjectInfo &object_info,
    plasma::flatbuf::ObjectSource source,
    bool allow_fallback_allocation) {
  // Try to evict objects until there is enough space.
  // NOTE(ekl) if we can't achieve this after a number of retries, it's
  // because memory fragmentation in dlmalloc prevents us from allocating
  // even if our footprint tracker here still says we have free space.
  for (int num_tries = 0; num_tries <= 10; num_tries++) {
    auto result =
        object_store_->CreateObject(object_info, source, /*fallback_allocate*/ false);
    if (result != nullptr) {
      return result;  // ★ 分配成功，直接返回
    }
    // Tell the eviction policy how much space we need to create this object.
    std::vector<ObjectID> objects_to_evict;
    int64_t space_needed =
        eviction_policy_->RequireSpace(object_info.GetObjectSize(), objects_to_evict);
    EvictObjects(objects_to_evict);
    // More space is still needed.
    if (space_needed > 0) {
      RAY_LOG(DEBUG) << "attempt to allocate " << object_info.GetObjectSize()
                     << " failed, need " << space_needed;
      break;  // ★ LRU 不够，退出循环
    }
    // space_needed <= 0 → 继续循环，重试 CreateObject
  }

  if (!allow_fallback_allocation) {
    return nullptr;  // 不允许 fallback → 返回 OOM
  }

  // 磁盘 mmap 分配
  auto result =
      object_store_->CreateObject(object_info, source, /*fallback_allocate*/ true);
  if (result == nullptr) {
    RAY_LOG(ERROR) << "Plasma fallback allocator failed, likely out of disk space.";
  }
  return result;
}
```

### 5.2 逐轮执行流程

```
for (num_tries = 0; num_tries <= 10; num_tries++) {
    │
    ├─ ① CreateObject(fallback=false)
    │    │  → allocator_.Allocate(object_size)
    │    │  → dlmemalign(64, object_size)
    │    │  ├─ 成功 → 返回 entry → 结束
    │    │  └─ 失败 → 继续 ②
    │    │
    │    ▼
    ├─ ② RequireSpace(size, objects_to_evict)
    │    │  required_space = Allocated + size - FootprintLimit
    │    │  space_to_free = max(required_space, FootprintLimit/5)
    │    │  num_bytes_evicted = ChooseObjectsToEvict(space_to_free, ...)
    │    │  返回 space_needed = required_space - num_bytes_evicted
    │    │
    │    ▼
    ├─ ③ EvictObjects(objects_to_evict)
    │    │  对每个选中的对象:
    │    │    RAY_CHECK(sealed && ref_count==0)
    │    │    DeleteObjectInternal → dlfree + allocated_ -= size
    │    │
    │    ▼
    ├─ ④ 判断 space_needed
    │    │  space_needed > 0 → break 退出循环
    │    │  space_needed ≤ 0 → 继续循环
    │    │
    │    ▼
    └─ 回到 ① 重试 CreateObject
}
```

### 5.3 循环退出的三种情况

| 条件 | 行为 | 原因 |
|------|------|------|
| `CreateObject` 成功 | 立即返回 | 分配成功 |
| `space_needed > 0` | break 退出 | LRU 淘汰不够，再循环也没用 |
| 11 轮跑完 | 退出循环 | 碎片场景，LRU 可能已空但仍分配失败 |

### 5.4 fallback 分配

```cpp
// obj_lifecycle_mgr.cc:199-204
auto result =
    object_store_->CreateObject(object_info, source, /*fallback_allocate*/ true);
```

fallback 分配时，`ObjectStore::CreateObject` 调用 `FallbackAllocate`：

```cpp
// plasma_allocator.cc:114-133
std::optional<Allocation> PlasmaAllocator::FallbackAllocate(size_t bytes) {
  // 强制 dlmalloc 使用独立的 mmap 文件
  RAY_CHECK(dlmallopt(M_MMAP_THRESHOLD, 0));
  void *mem = dlmemalign(kAlignment, bytes);
  RAY_CHECK(dlmallopt(M_MMAP_THRESHOLD, MAX_SIZE_T));  // 恢复默认

  if (!mem) { return absl::nullopt; }

  allocated_ += bytes;  // 仍然计入 Allocated
  // 判断是否在初始分配区域外
  if (internal::IsOutsideInitialAllocation(mem)) {
    is_fallback_allocated = true;
    fallback_allocated_ += bytes;
  }
  return BuildAllocation(mem, bytes, is_fallback_allocated);
}
```

**fallback 分配不受 `FootprintLimit` 限制**——它从磁盘 mmap 新文件，只受磁盘空间限制。但 `allocated_` 仍然计数，所以 `Allocated()` 可能超过 `FootprintLimit`。

---

## 6. 第2阶段：CreateRequestQueue — GC/Spill/Grace Period/Fallback

### 6.1 CreateRequestQueue 与 PlasmaStore 的关系

当 `CreateObjectInternal` 返回 `nullptr`（OOM），`PlasmaStore::CreateObject` 返回 `PlasmaError::OutOfMemory`。这个错误通过 `create_callback_` 传回 `CreateRequestQueue`。

```cpp
// store.cc:143-151
PlasmaError PlasmaStore::HandleCreateObjectRequest(
    const std::shared_ptr<Client> &client,
    const std::vector<uint8_t> &message,
    bool fallback_allocator,
    PlasmaObject *object) {
  auto error = CreateObject(object_info, source, client, fallback_allocator, object);
  if (error == PlasmaError::OutOfMemory) {
    RAY_LOG(DEBUG) << "Not enough memory to create the object...";
  }
  return error;  // ★ OOM 传回 CreateRequestQueue
}
```

### 6.2 ProcessRequests 完整代码与逻辑

```cpp
// create_request_queue.cc:84-137
Status CreateRequestQueue::ProcessRequests() {
  bool logged_oom = false;
  while (!queue_.empty()) {
    auto request_it = queue_.begin();

    // Step 1: 尝试正常分配
    auto status = ProcessRequest(/*fallback_allocator=*/false, *request_it);

    // Step 1a: 磁盘满检查
    if ((*request_it)->error_ == PlasmaError::OutOfMemory &&
        fs_monitor_.OverCapacity()) {
      (*request_it)->error_ = PlasmaError::OutOfDisk;
      FinishRequest(request_it);
      return Status::OutOfDisk("System running out of disk.");
    }

    auto now = get_time_();
    if (status.ok()) {
      // 分配成功 → 处理下一个请求
      FinishRequest(request_it);
      oom_start_time_ns_ = -1;
    } else {
      // ★ 分配失败 (OOM) → 进入 OOM 处理链路

      // Step 2: 触发全局 GC
      if (trigger_global_gc_) {
        trigger_global_gc_();
      }

      // 记录 OOM 开始时间
      if (oom_start_time_ns_ == -1) {
        oom_start_time_ns_ = now;
      }

      // Step 3: 触发 Spill
      auto spill_pending = spill_objects_callback_();
      if (spill_pending) {
        // 有 spill 正在进行 → 等待 spill 完成
        oom_start_time_ns_ = -1;  // 重置 grace period
        return Status::TransientObjectStoreFull("Waiting for objects to spill.");
      }

      // Step 4: Grace period 等待
      if (now - oom_start_time_ns_ < oom_grace_period_ns_) {
        return Status::ObjectStoreFull("Waiting for grace period.");
      }

      // Step 5: Fallback 分配
      status = ProcessRequest(/*fallback_allocator=*/true, *request_it);
      if (!status.ok()) {
        // 磁盘也满 → OutOfDisk
        (*request_it)->error_ = PlasmaError::OutOfDisk;
      }
      FinishRequest(request_it);
    }
  }
  return Status::OK();
}
```

### 6.3 OOM 处理的五个步骤

```
ProcessRequest(fallback=false) → OOM
        │
        ▼
Step 1: 磁盘满检查
  fs_monitor_.OverCapacity() → OutOfDisk (直接失败)
        │ 磁盘不满
        ▼
Step 2: trigger_global_gc_()
  → NodeManager::SetShouldGlobalGC()
  → 通知所有 worker 释放 out-of-scope 对象的 plasma 引用
  → 这些对象 unpin → ref_count 归零 → 加入 LRU
  → 但 GC 是异步的，不会立即生效
        │
        ▼
Step 3: spill_objects_callback_()
  → LocalObjectManager::SpillObjectUptoMaxThroughput()
  → 选择 pinned_objects_ 中 spillable 的对象溢写到磁盘
  → 溢写完成后 unpin → ref_count 归零 → 加入 LRU
  ├─ 有 spill 正在进行 → 返回 TransientObjectStoreFull (等待)
  └─ 没有 spillable 对象 → 继续
        │
        ▼
Step 4: grace period 等待
  if (now - oom_start_time < grace_period_ns)
  → 返回 ObjectStoreFull (等待)
  → 等待 GC 和 spill 生效
        │ grace period 过期
        ▼
Step 5: ProcessRequest(fallback=true)
  → CreateObjectInternal(allow_fallback=true)
  → 从磁盘 mmap 分配
  ├─ 成功 → 完成
  └─ 失败 → OutOfDisk (最终失败)
```

### 6.4 回调注册（raylet/main.cc）

spill 和 GC 的回调在 raylet 初始化时注册：

```cpp
// raylet/main.cc:782-797
auto object_store_runner = std::make_unique<ray::ObjectStoreRunner>(
    object_manager_config,
    /*spill_objects_callback=*/
    [&]() {
      // 从 plasma store 线程 post 到 raylet 线程
      main_service.post(
          [&]() { local_object_manager->SpillObjectUptoMaxThroughput(); },
          "NodeManager.SpillObjects");
      return local_object_manager->IsSpillingInProgress();
    },
    /*object_store_full_callback=*/
    [&]() {
      main_service.post([&]() { node_manager->SetShouldGlobalGC(); },
                        "NodeManager.SetShouldGlobalGC");
    },
    ...
);
```

**注意线程安全**：spill 和 GC 回调是从 Plasma Store 线程调用的，通过 `main_service.post` 转移到 raylet 的主事件循环线程执行。

---

## 7. Spill 机制详解

### 7.1 Spill 的触发时机

**Spill 不是主动触发的，没有水位线机制。** 只在 Create OOM 失败后，作为 OOM 处理链路的一环被动触发：

```
Create 分配内存失败
  → LRU 淘汰后仍不够 (或 LRU 为空无对象可淘汰)
  → CreateObjectInternal 返回 nullptr (OOM)
  → ProcessRequests 收到 OOM
  → trigger_global_gc_()
  → spill_objects_callback_()  ← ★ 只有这里才触发 spill
```

### 7.2 SpillObjectUptoMaxThroughput

```cpp
// local_object_manager.cc:169-182
void LocalObjectManager::SpillObjectUptoMaxThroughput() {
  if (RayConfig::instance().object_spilling_config().empty()) {
    return;  // 未配置 spill → 直接返回
  }
  bool can_spill_more = true;
  while (can_spill_more) {
    if (!TryToSpillObjects()) {
      break;  // 没有更多对象可 spill
    }
    can_spill_more = num_active_workers_ < max_active_workers_;
  }
}
```

### 7.3 TryToSpillObjects — 选择对象溢写

```cpp
// local_object_manager.cc:186-227
bool LocalObjectManager::TryToSpillObjects() {
  if (RayConfig::instance().object_spilling_config().empty()) {
    return false;
  }

  int64_t bytes_to_spill = 0;
  std::vector<ObjectID> objects_to_spill;
  int64_t num_to_spill = 0;
  size_t idx = 0;

  // 遍历所有被 pin 的对象
  for (const auto &[object_id, ray_object] : pinned_objects_) {
    if (is_plasma_object_spillable_(object_id)) {
      const int64_t object_size = ray_object->GetSize();

      // 最大文件大小限制
      if (max_spilling_file_size_bytes_ > 0 && !objects_to_spill.empty() &&
          bytes_to_spill + object_size > max_spilling_file_size_bytes_) {
        break;
      }

      bytes_to_spill += object_size;
      objects_to_spill.push_back(object_id);
      ++num_to_spill;

      // 最大融合对象数限制
      if (num_to_spill == max_fused_object_count_) {
        break;
      }
    }
    ++idx;
  }

  if (objects_to_spill.empty()) {
    return false;  // 没有 spillable 对象
  }

  // 最小溢写大小检查
  if (idx == objects_pending_spill_.size() &&
      bytes_to_spill < min_spilling_size_ &&
      !objects_pending_spill_.empty()) {
    // 对象总大小 < min_spilling_size_ 且有其他对象正在 spill
    // 等待当前 spill 完成再决定
    return false;
  }

  // 开始溢写
  SpillObjectsInternal(objects_to_spill, callback);
  return true;
}
```

### 7.4 Spillable 判断

`is_plasma_object_spillable_` 是在 raylet 初始化时传入的回调：

```cpp
// 判断条件：
// 1. 对象在 Plasma Store 中是 sealed 状态
// 2. 对象的 ref_count 在 Plasma Store 中 == 1（只有 raylet 持有）
// 3. 对象不在 objects_pending_deletion_ 中
// 4. 对象不在 objects_pending_spill_ 中
```

**Spill 的前提**：对象被 LocalObjectManager Pin（ref_count==1），sealded，不在 pending deletion/spill 中。

### 7.5 SpillObjectsInternal — 溢写执行

```cpp
// local_object_manager.cc:282-390
void LocalObjectManager::SpillObjectsInternal(...) {
  std::vector<ObjectID> objects_to_spill;

  for (const auto &id : object_ids) {
    // 过滤不可 spill 的对象
    if (pinned_objects_.count(id) == 0 && objects_pending_spill_.count(id) == 0) {
      callback(Status::Invalid("...not marked as the primary copy."));
      return;
    }

    auto it = pinned_objects_.find(id);
    if (it != pinned_objects_.end()) {
      objects_to_spill.push_back(id);

      // ★ 从 pinned_objects_ 移到 objects_pending_spill_
      auto object_size = it->second->GetSize();
      num_bytes_pending_spill_ += object_size;
      objects_pending_spill_[id] = std::move(it->second);
      pinned_objects_size_ -= object_size;
      pinned_objects_.erase(it);
    }
  }

  // 异步发送给 IO worker 执行 spill
  num_active_workers_ += 1;
  io_worker_pool_.PopSpillWorker([this, objects_to_spill, callback](...) {
    // ... RPC 调用 io_worker 执行 spill ...
    // 完成后: num_active_workers_ -= 1
  });
}
```

### 7.6 Spill 完成后的 Unpin

Spill 完成后，`OnObjectSpilled` 被调用：

```cpp
// local_object_manager.cc:447-494
void LocalObjectManager::OnObjectSpilled(const std::vector<ObjectID> &object_ids,
                                         const rpc::SpillObjectsReply &worker_reply) {
  for (size_t i = 0; i < worker_reply.spilled_objects_url_size(); ++i) {
    const ObjectID &object_id = object_ids[i];
    const std::string &object_url = worker_reply.spilled_objects_url(i);

    spilled_objects_url_.emplace(object_id, object_url);

    // ★ 从 objects_pending_spill_ 中移除
    auto it = objects_pending_spill_.find(object_id);
    const auto object_size = it->second->GetSize();
    num_bytes_pending_spill_ -= object_size;
    objects_pending_spill_.erase(it);

    // ★ 但是 RayObject 被销毁后，Plasma Store 中的 ref_count 减少
    // （通过 ReleaseFreedObject → on_objects_freed_ → PlasmaStore::DeleteObject）
  }
}
```

### 7.7 Spill 与 Plasma ref_count 的关系

Spill 完成后，对象的 Plasma Store ref_count 变化：

```
Spill 前:  pinned_objects_ 持有 RayObject → Plasma Store ref_count = 1
Spill 中:  objects_pending_spill_ 持有 → ref_count 仍为 1
Spill 后:  objects_pending_spill_ 释放 RayObject → Plasma Store RemoveReference → ref_count = 0
           → EndObjectAccess → 对象加入 LRU → 可被淘汰
```

**但更常见的路径是**：当对象 out-of-scope 后，owner 发布 eviction 消息，raylet 调用 `ReleaseFreedObject`：

```cpp
// local_object_manager.cc:106-131
void LocalObjectManager::ReleaseFreedObject(const ObjectID &object_id) {
  auto it = local_objects_.find(object_id);
  if (it == local_objects_.end() || it->second.is_freed_) {
    return;
  }
  it->second.is_freed_ = true;

  // 从 pinned_objects_ 中移除 → RayObject 被销毁
  // → Plasma Store 的 ref_count 归零
  // → 对象加入 LRU
  auto pinned_objects_it = pinned_objects_.find(object_id);
  if (pinned_objects_it != pinned_objects_.end()) {
    pinned_objects_size_ -= pinned_objects_it->second->GetSize();
    pinned_objects_.erase(pinned_objects_it);
    local_objects_.erase(it);
  } else {
    // 如果正在 spill，延迟删除
    spilled_object_pending_delete_.push(object_id);
  }

  // 通知集群其他节点删除该对象
  objects_pending_deletion_.emplace(object_id);
  FlushFreeObjects();
}
```

### 7.8 IsSpillingInProgress — 返回给 Plasma Store

```cpp
// local_object_manager.cc:184
bool LocalObjectManager::IsSpillingInProgress() { return num_active_workers_ > 0; }
```

当 `spill_objects_callback_` 返回 `true`（有 spill 正在进行），`ProcessRequests` 会返回 `TransientObjectStoreFull`，等待稍后重试。

---

## 8. 淘汰 vs Spill 的关系

### 8.1 核心区别

| 机制 | 作用对象 | 条件 | 效果 | 释放内存 |
|------|---------|------|------|---------|
| **LRU 淘汰** | ref_count==0 且 sealed 的对象 | 在 LRU 缓存中 | 物理删除，立即释放 | ✓ 立即 |
| **Spill** | ref_count==1 且 spillable 的对象 | 被 LocalObjectManager pin | 溢写到磁盘，unpin 后进入 LRU | ✗ 不直接释放 |

### 8.2 协作关系

**Spill 本身不释放内存——它只是把数据备份到磁盘，然后 unpin（ref_count 归零），让对象进入 LRU，需要下一轮 LRU 淘汰才会真正释放。**

```
Spill:  ref_count=1 的对象 → 溢写到磁盘 → unpin → ref_count=0 → 加入 LRU
                                                                     │
                                                              下一轮 CreateObject
                                                              RequireSpace 时
                                                                     │
                                                                     ▼
Evict: LRU 中的对象 → ChooseObjectsToEvict → EvictObjects → 物理删除 → 释放内存
```

### 8.3 对象的 ref_count 与处理方式对应

```
                    ref_count 状态
                    ┌──────────────┐
                    │  =0 (无 pin)  │ → 在 LRU 中 → 可直接淘汰（第1阶段）
                    │  =1 (有 pin)  │ → 不在 LRU → 需要先 Spill → unpin → ref_count=0 → 加入 LRU → 再淘汰
                    │  >1 (多引用)  │ → 不可淘汰也不可 Spill → 需要 GC 减少引用
                    └──────────────┘
```

---

## 9. RequireSpace 核心公式与场景推演

### 9.1 公式

```
required_space = Allocated + new_object_size - FootprintLimit
space_to_free  = max(required_space, FootprintLimit / 5)
返回值          = required_space - num_bytes_evicted
```

### 9.2 场景 A：账面有空间但碎片导致 OOM

```
FootprintLimit = 1024MB
Allocated = 500MB
新对象 size = 50MB

required_space = 500 + 50 - 1024 = -474MB (负数!)
space_to_free = max(-474, 204) = 204MB  ← 仍然淘汰 204MB
```

**原因**：账面有 474MB 空间但 `dlmemalign` 失败，是碎片问题。强制淘汰 204MB，free 内存让 dlmalloc 合并空闲块。

### 9.3 场景 B：内存使用刚好超一点

```
FootprintLimit = 1024MB
Allocated = 1000MB
新对象 size = 50MB

required_space = 1000 + 50 - 1024 = 26MB
space_to_free = max(26, 204) = 204MB  ← 仍然淘汰 204MB
```

虽然只需要 26MB，但淘汰 204MB，为后续请求预留空间。

### 9.4 场景 C：需求超过 20%

```
FootprintLimit = 1024MB
Allocated = 1020MB
新对象 size = 500MB

required_space = 1020 + 500 - 1024 = 496MB
space_to_free = max(496, 204) = 496MB  ← 淘汰 496MB
```

### 9.5 场景 D：只需 20MB 但要求淘汰 200MB，LRU 只能淘汰 20MB

```
FootprintLimit = 1024MB
Allocated = 1000MB
新对象 size = 44MB

required_space = 1000 + 44 - 1024 = 20MB
space_to_free = max(20, 204) = 204MB
LRU 中只有 20MB 的对象

第1轮:
  CreateObject → OOM
  RequireSpace:
    space_to_free = 204MB
    ChooseObjectsToEvict(204MB) → 只选出 20MB 的对象
    num_bytes_evicted = 20MB
    返回 space_needed = 20 - 20 = 0  ← 不是正数!
  EvictObjects: 淘汰 20MB

第2轮:
  CreateObject → 成功! (有了 20MB 空间)
  return result  ← ★ 直接返回，不再进入 RequireSpace
```

**关键**：`space_to_free = 204MB` 只是告诉 LRU "尽量选 204MB 的对象出来"，但 LRU 只选了 20MB。`space_needed = required_space - 20 = 0`，不 break，下一轮 CreateObject 成功就直接结束了。

**`space_to_free` 是选择目标而非强制淘汰量——LRU 尽力选，选多少淘汰多少，够用就行。**

---

## 10. "LRU 淘汰不了那么多"的完整代码路径

### 10.1 场景分类

| LRU 淘汰情况 | required_space | space_needed | 行为 | 后果 |
|---|---|---|---|---|
| LRU 够（≥ space_to_free） | 任意 | ≤ 0 | 继续循环 | 通常下轮 CreateObject 成功 |
| LRU 不够 | ≤ 0 | ≤ 0 | 跑满 10 轮 | 10 轮后 OOM |
| LRU 不够 | > 0 | > 0 | 立即 break | 直接 OOM |
| LRU 空 | ≤ 0 | ≤ 0 | 跑满 10 轮 | 10 轮后 OOM |
| LRU 空 | > 0 | > 0 | 立即 break | 直接 OOM |

### 10.2 场景：LRU 不够且 required_space ≤ 0

```
FootprintLimit = 1024MB
Allocated = 900MB
新对象 size = 100MB

required_space = 900 + 100 - 1024 = -24MB (负数!)
space_to_free = max(-24, 204) = 204MB
LRU 中只有 150MB → ChooseObjectsToEvict 选出 150MB

第1轮:
  CreateObject → OOM (碎片)
  RequireSpace:
    num_bytes_evicted = 150MB
    space_needed = -24 - 150 = -174MB  ← 负数
  EvictObjects: 淘汰 150MB
  space_needed = -174 ≤ 0 → 继续循环

第2轮: Allocated = 750MB
  CreateObject → OOM (碎片仍未解决)
  RequireSpace:
    required_space = 750 + 100 - 1024 = -174MB
    space_to_free = 204MB
    LRU 中只有 50MB → 选出 50MB
    num_bytes_evicted = 50MB
    space_needed = -174 - 50 = -224MB  ← 仍为负数
  EvictObjects: 淘汰 50MB
  space_needed ≤ 0 → 继续循环

第3轮: Allocated = 700MB
  CreateObject → OOM
  RequireSpace:
    LRU 为空 → num_bytes_evicted = 0
    space_needed = -224 - 0 = -224MB  ← 仍为负数
  EvictObjects: 空列表
  space_needed ≤ 0 → 继续循环

第4轮~第11轮: 同第3轮
  LRU 一直为空，每轮都是: CreateObject → OOM → RequireSpace → 0淘汰 → 继续

10轮全部跑完后退出循环:
  allow_fallback=false → 返回 nullptr (OOM) → 进入第2阶段
  allow_fallback=true  → 磁盘分配
```

**★ 关键发现**：当 `required_space ≤ 0` 且 LRU 为空时，`space_needed` 仍为负数，循环会跑满 10 轮。每轮都尝试 `CreateObject` 但因为碎片失败，LRU 无对象可淘汰，白白循环 10 次。

### 10.3 场景：LRU 不够且 required_space > 0

```
FootprintLimit = 1024MB
Allocated = 1020MB
新对象 size = 200MB

required_space = 1020 + 200 - 1024 = 196MB
space_to_free = max(196, 204) = 204MB
LRU 中只有 100MB → ChooseObjectsToEvict 选出 100MB

第1轮:
  CreateObject → OOM
  RequireSpace:
    num_bytes_evicted = 100MB
    space_needed = 196 - 100 = 96MB  ← 正数!
  EvictObjects: 淘汰 100MB
  space_needed = 96 > 0 → ★ 立即 break，不继续循环

退出循环:
  allow_fallback=false → 返回 nullptr (OOM) → 进入第2阶段
  allow_fallback=true  → 磁盘分配
```

**`required_space > 0` 且 LRU 淘汰不够时，第一轮就 break 退出。** 因为即使继续循环，LRU 中已经没有更多对象可淘汰了，再循环也分配不了。

### 10.4 第2阶段的完整处理路径

当 `CreateObjectInternal` 返回 nullptr（OOM）后，`create_callback_` 返回 `PlasmaError::OutOfMemory`，进入 `ProcessRequests`：

```
ProcessRequests:
  ProcessRequest(fallback=false) → OOM

  ① 磁盘满检查:
    if (OOM && fs_monitor_.OverCapacity())
      → OutOfDisk，直接失败

  ② trigger_global_gc_()  → 全局 GC
     → NodeManager::SetShouldGlobalGC()
     → 通知所有 worker 释放 out-of-scope 对象
     → 这些对象 unpin → ref_count 归零 → 加入 LRU
     → 但 GC 是异步的，不会立即生效

  ③ spill_objects_callback_()  → Spill
     → SpillObjectUptoMaxThroughput()
     → 选择 pinned_objects_ 中 spillable 的对象溢写到磁盘

     两种结果:
     ├─ 有 spill 正在进行 (num_active_workers_ > 0)
     │  → 返回 true (spill_pending)
     │  → oom_start_time_ns_ = -1 (重置 grace period)
     │  → 返回 TransientObjectStoreFull (等待 spill 完成)
     │  → ★ ProcessRequests 退出，稍后重试
     │
     └─ 没有 spillable 对象可 spill
        → 返回 false (spill_pending = false)
        → 继续 ④

  ④ grace period:
    if (now - oom_start_time_ns_ < grace_period_ns)
      → 返回 ObjectStoreFull (等待)
      → ★ ProcessRequests 退出，稍后重试

  ⑤ fallback_allocator=true:
    → ProcessRequest(fallback=true)
    → CreateObjectInternal(allow_fallback=true)
    → 第1阶段仍可能 OOM，但 allow_fallback=true
    → 10轮循环后走磁盘 mmap 分配
    → 如果磁盘也满 → OutOfDisk
```

---

## 11. "只需少量空间，要求淘汰 20%"的代码路径

### 11.1 核心问题

当 `required_space` 很小（甚至为负）但 `space_to_free = FootprintLimit/5` 很大时，LRU 不需要真的淘汰 20%。`space_to_free` 只是 LRU 选择对象的目标量，实际淘汰多少取决于 LRU 中有多少对象。

### 11.2 详细代码追踪

```cpp
// eviction_policy.cc:117-131
int64_t EvictionPolicy::RequireSpace(int64_t size,
                                     std::vector<ObjectID> &objects_to_evict) {
  int64_t required_space = allocator_.Allocated() + size - allocator_.GetFootprintLimit();
  int64_t space_to_free = std::max(required_space, allocator_.GetFootprintLimit() / 5);
  int64_t num_bytes_evicted = ChooseObjectsToEvict(space_to_free, objects_to_evict);
  return required_space - num_bytes_evicted;  // ★ 注意：用的是 required_space
}
```

**返回值的计算用的是 `required_space`，不是 `space_to_free`！**

这意味着即使 `space_to_free = 204MB`，只要 LRU 实际淘汰了 `required_space` 大小的对象，返回值就 ≤ 0，循环继续。

### 11.3 具体场景

```
FootprintLimit = 1024MB, space_to_free = 204MB (固定)

场景: 只需要 20MB 空间
  required_space = 20MB
  space_to_free = max(20, 204) = 204MB

  LRU 有 200MB 对象 → ChooseObjectsToEvict(204MB) → 选出 200MB
  num_bytes_evicted = 200MB
  返回值 = 20 - 200 = -180MB  ← 负数
  → space_needed ≤ 0 → 不 break → 下一轮 CreateObject 成功

  但如果 LRU 只有 20MB 对象 → ChooseObjectsToEvict(204MB) → 选出 20MB
  num_bytes_evicted = 20MB
  返回值 = 20 - 20 = 0  ← 不是正数!
  → space_needed ≤ 0 → 不 break → 下一轮 CreateObject 成功

  ★ 在后一种情况，虽然要求淘汰 204MB，但只淘汰了 20MB，
    只要满足 required_space (20MB) 就够了，CreateObject 就能成功。
```

### 11.4 为什么用 `required_space` 而不是 `space_to_free` 计算返回值

```
如果用 space_to_free 计算: 返回值 = space_to_free - num_bytes_evicted
  = 204 - 20 = 184MB > 0 → break!
  → 即使已经淘汰了足够满足 required_space 的量，也会 break
  → 这会导致不必要的 OOM

用 required_space 计算: 返回值 = required_space - num_bytes_evicted
  = 20 - 20 = 0 → 不 break!
  → 只有当真正不满足 required_space 时才 break
  → 这是正确的行为
```

**设计意图**：`space_to_free` 是"理想淘汰量"（多淘汰一些减少未来 OOM），`required_space` 是"最低淘汰量"（必须满足才能分配）。返回值只看最低需求是否满足，理想量不满足不影响分配。

### 11.5 完整流程总结

```
CreateObject → OOM
  RequireSpace:
    space_to_free = max(required_space, 20%容量)  ← 选择目标
    LRU 尽力选 → 选出 num_bytes_evicted 字节
    返回 required_space - num_bytes_evicted       ← 判断依据

  如果 required_space ≤ num_bytes_evicted:
    → 返回值 ≤ 0 → 不 break → 下一轮 CreateObject 成功

  如果 required_space > num_bytes_evicted:
    → 返回值 > 0 → break → OOM → 进入第2阶段
```

**额外淘汰（超过 required_space 的部分）是"白送的"——它多释放的空间让后续 Create 更容易成功，但不影响 break 判断。**

---

## 12. Unpin 不主动释放内存

### 12.1 核心结论

**Unpin（ref_count 降到 0，对象加回 LRU）不会主动释放内存。** 必须等新 Create 请求 OOM 后，`RequireSpace` → `ChooseObjectsToEvict` 从 LRU 中选择对象淘汰，才会真正释放。

### 12.2 Unpin 的完整代码路径

```
RemoveReference (ref_count: 1→0)
  → eviction_policy_->EndObjectAccess(object_id)
     → cache_.Add(object_id, size)  // 加入 LRU 链表头部
        → item_list_.emplace_front(key, size)
        → used_capacity_ += size
  → 对象仍在 object_table_ 中
  → 内存仍在（dlmalloc 未 free）
  → ★ 没有任何主动删除/释放的动作
```

**Unpin 之后对象的状态**：
- 仍在 `ObjectStore::object_table_` 中
- `PlasmaAllocator::Allocated()` 仍包含它的内存
- 在 LRU 中，可被未来淘汰选中
- **没有任何定时清理、水位线触发、或主动回收机制**

### 12.3 内存真正释放的唯一路径

```
新 Create 请求 → dlmemalign OOM
  → RequireSpace → ChooseObjectsToEvict 从 LRU 尾部选
  → EvictObjects → DeleteObjectInternal
    → object_store_->DeleteObject()
      → allocator_.Free()  → dlfree()  → allocated_ -= size  // ★ 真正释放
    → delete_object_callback_(object_id)                      // ★ 通知 raylet
```

### 12.4 为什么不主动释放

设计上的考虑：
1. **避免频繁分配/释放**：如果刚 unpin 就 free，下一秒又要 Get 同一个对象，需要重新分配内存、重新传输数据
2. **LRU 缓存语义**：LRU 本身就是缓存——最近使用的对象保留在内存中，直到空间不足时才淘汰
3. **被动淘汰更高效**：只在真正需要空间时才淘汰，避免不必要的 I/O

### 12.5 内存回收时间线

```
时间 ─────────────────────────────────────────────────────────►

对象 A 被 Unpin          新 Create OOM            对象 A 被淘汰
  │                        │                        │
  │  加入 LRU，内存仍在    │  RequireSpace          │  dlfree
  │  Allocated 不变       │  ChooseObjectsToEvict  │  Allocated 减少
  │  used_memory_ 不变    │  选中对象 A             │  used_memory_ 减少
  │                        │  EvictObjects          │
  │                        │                        │
  ├──────── 可被 Get 命中 ────────┤                  │
  │    (ref_count 0→1,       │                  │
  │     从 LRU 移出)         │                  │
```

---

## 13. Dashboard Object Store Memory 指标追踪

### 13.1 used_memory_ 的两个变更途径

`used_memory_` 只通过两个回调变更，这两个回调在 raylet/main.cc 中注册：

**增加 — `add_object_callback_`（Seal 时触发）**：

```cpp
// raylet/main.cc:799-806
/*add_object_callback=*/
[&](const ray::ObjectInfo &object_info) {
  main_service.post(
      [&object_manager, &node_manager, object_info]() {
        object_manager->HandleObjectAdded(object_info);  // ★ used_memory_ += size
        node_manager->HandleObjectLocal(object_info);
      }, "ObjectManager.ObjectAdded");
},
```

`add_object_callback_` 的唯一触发点：

```cpp
// store.cc:275-282
void PlasmaStore::SealObjects(const std::vector<ObjectID> &object_ids) {
  for (size_t i = 0; i < object_ids.size(); ++i) {
    auto entry = object_lifecycle_mgr_.SealObject(object_ids[i]);
    add_object_callback_(entry->GetObjectInfo());  // ★ 只在 Seal 时调用
  }
}
```

→ **`used_memory_` 增加的唯一途径：对象被 Seal 时。**

**减少 — `delete_object_callback_`（DeleteObjectInternal 且非 Abort 时触发）**：

```cpp
// raylet/main.cc:808-816
/*delete_object_callback=*/
[&](const ray::ObjectID &object_id) {
  main_service.post(
      [&object_manager, &node_manager, object_id]() {
        object_manager->HandleObjectDeleted(object_id);  // ★ used_memory_ -= size
        node_manager->HandleObjectMissing(object_id);
      }, "ObjectManager.ObjectDeleted");
},
```

`delete_object_callback_` 的唯一调用点：

```cpp
// obj_lifecycle_mgr.cc:243-256
void ObjectLifecycleManager::DeleteObjectInternal(const ObjectID &object_id) {
  auto entry = object_store_->GetObject(object_id);
  bool aborted = entry->state_ == ObjectState::PLASMA_CREATED;

  stats_collector_->OnObjectDeleting(*entry);
  earger_deletion_objects_.erase(object_id);
  eviction_policy_->RemoveObject(object_id);
  object_store_->DeleteObject(object_id);  // dlfree

  if (!aborted) {
    // only send notification if it's not aborted.
    delete_object_callback_(object_id);  // ★ 只在非 abort 时调用
  }
}
```

→ **`used_memory_` 减少的唯一途径：`DeleteObjectInternal` 被调用且对象不是 aborted（即已被 Seal）。**

### 13.2 DeleteObjectInternal 的所有调用路径

`DeleteObjectInternal` 有 **4 个调用点**：

```
路径1: LRU 淘汰 (EvictObjects)
  CreateObjectInternal → RequireSpace → ChooseObjectsToEvict
  → EvictObjects → DeleteObjectInternal
  条件: sealed && ref_count==0
  aborted: false → ★ delete_object_callback_ 被调用 → used_memory_ 减少

路径2: 显式删除 (DeleteObject)
  PlasmaStore 收到 PlasmaDeleteRequest
  → ObjectLifecycleManager::DeleteObject
  → if (sealed && ref_count==0) → DeleteObjectInternal
  条件: sealed && ref_count==0
  aborted: false → ★ delete_object_callback_ 被调用 → used_memory_ 减少

路径3: Eager deletion (RemoveReference)
  RemoveReference → ref_count 降到 0
  → if (earger_deletion_objects_.count > 0) → DeleteObjectInternal
  条件: sealed (RAY_CHECK(entry->Sealed()))
  aborted: false → ★ delete_object_callback_ 被调用 → used_memory_ 减少

路径4: Abort (AbortObject)
  PlasmaStore 收到 PlasmaAbortRequest
  → ObjectLifecycleManager::AbortObject → DeleteObjectInternal
  条件: unsealed (state_ != PLASMA_SEALED)
  aborted: true → ★ delete_object_callback_ 不被调用 → used_memory_ 不变
  (因为未 Seal 的对象从未触发 add_object_callback_，used_memory_ 从未增加)
```

### 13.3 回调与 used_memory_ 的完整对应

| 路径 | 触发条件 | aborted | add_callback | delete_callback | used_memory_ |
|------|---------|---------|-------------|----------------|-------------|
| Seal | 对象写入完成封印 | - | ✓ 调用 | - | += size |
| LRU 淘汰 | sealed, ref==0 | false | - | ✓ 调用 | -= size |
| 显式删除 | sealed, ref==0 | false | - | ✓ 调用 | -= size |
| Eager deletion | ref→0 且在 earger 集合 | false | - | ✓ 调用 | -= size |
| Abort | unsealed | true | - | ✗ 不调用 | 不变 |
| **Unpin (ref→0)** | sealed, ref==0, 不在 earger 集合 | - | - | ✗ 不调用 | **不变** |
| **加入 LRU** | EndObjectAccess | - | - | ✗ 不调用 | **不变** |

### 13.4 Eager Deletion 详解

#### 13.4.1 含义

Eager Deletion（急切删除）是指：**有人想删除这个对象但当前删不了（ref_count > 0 或未 sealed），先标记为"待急切删除"，等条件满足时立即删除，绕过 LRU。**

与普通 Unpin 的核心区别：

```
普通 Unpin:  ref→0 → 加入 LRU → 等 OOM 时被动淘汰 → 才真正释放
Eager:       ref→0 → 立即 DeleteObjectInternal → 立即释放
```

#### 13.4.2 标记时机 — DeleteObject 被调用但无法立即删除

当 `DeleteObject` 被调用但对象无法立即删除时（ref_count > 0 或未 sealed），对象 ID 被加入 `earger_deletion_objects_`：

```cpp
// obj_lifecycle_mgr.cc:99-116
PlasmaError ObjectLifecycleManager::DeleteObject(const ObjectID &object_id) {
  auto entry = object_store_->GetObject(object_id);
  if (entry == nullptr) {
    return PlasmaError::ObjectNonexistent;
  }

  // 情况1: 对象还未 sealed → 不能删除（可能有内存损坏风险）
  if (entry->state_ != ObjectState::PLASMA_SEALED) {
    // Put it into deletion cache, it will be deleted later.
    earger_deletion_objects_.emplace(object_id);  // ★ 标记为待急切删除
    return PlasmaError::ObjectNotSealed;
  }

  // 情况2: 对象还在被使用 → 不能删除
  if (entry->ref_count_ != 0) {
    // To delete an object, there must be no clients currently using it.
    // Put it into deletion cache, it will be deleted later.
    earger_deletion_objects_.emplace(object_id);  // ★ 标记为待急切删除
    return PlasmaError::ObjectInUse;
  }

  // 情况3: sealed && ref_count==0 → 可以立即删除
  DeleteObjectInternal(object_id);
  return PlasmaError::OK;
}
```

#### 13.4.3 触发时机 — RemoveReference 使 ref_count 降到 0

当 `RemoveReference` 使 ref_count 降到 0 时，检查 `earger_deletion_objects_`：

```cpp
// obj_lifecycle_mgr.cc:121-167
bool ObjectLifecycleManager::RemoveReference(const ObjectID &object_id) {
  auto entry = object_store_->GetObject(object_id);
  if (!entry || entry->ref_count_ == 0) {
    return false;
  }

  entry->ref_count_--;

  if (entry->ref_count_ > 0) {
    return true;  // 还有引用，不做特殊处理
  }

  // ref_count 降到 0

  // ★ Step 1: 先加入 LRU（EndObjectAccess 会把对象加入 LRU 链表）
  eviction_policy_->EndObjectAccess(object_id);

  // ★ Step 2: 检查是否在 earger_deletion_objects_ 中
  RAY_CHECK(entry->Sealed()) << object_id << " is not sealed while ref count becomes 0.";
  if (earger_deletion_objects_.count(object_id) > 0) {
    // ★ 在 earger 集合中 → 立即删除，绕过 LRU
    DeleteObjectInternal(object_id);
    // DeleteObjectInternal 内部会:
    //   1. eviction_policy_->RemoveObject(object_id)  // 从 LRU 移除（刚加入又被移除）
    //   2. object_store_->DeleteObject(object_id)      // dlfree + allocated_ -= size
    //   3. delete_object_callback_(object_id)          // 通知 raylet → used_memory_ -= size
  }
  return true;
}
```

**注意**：即使走了 earger deletion 路径，`EndObjectAccess` 仍然会先被调用（加入 LRU），但紧接着 `DeleteObjectInternal` 中的 `eviction_policy_->RemoveObject` 会把它从 LRU 移除。所以最终效果是：对象不留在 LRU 中，直接被物理删除。

#### 13.4.4 DeleteObject 的上游调用链

`DeleteObject` 被 `PlasmaStore` 在收到 `PlasmaDeleteRequest` 时调用：

```cpp
// store.cc:453-461
case fb::MessageType::PlasmaDeleteRequest: {
  std::vector<ObjectID> object_ids;
  std::vector<PlasmaError> error_codes;
  ReadDeleteRequest(input, input_size, &object_ids);
  error_codes.reserve(object_ids.size());
  for (auto &object_id : object_ids) {
    error_codes.push_back(object_lifecycle_mgr_.DeleteObject(object_id));  // ★
  }
  RAY_RETURN_NOT_OK(SendDeleteReply(client, object_ids, error_codes));
} break;
```

`PlasmaDeleteRequest` 的来源是 `ObjectManager::FreeObjects`：

```
LocalObjectManager::FlushFreeObjects()
  → on_objects_freed_(objects_to_delete)
  → ObjectManager::FreeObjects(object_ids, local_only=false)
    → buffer_pool_.FreeObjects(object_ids)
      → PlasmaClient::Delete(object_ids)           // 发送 PlasmaDeleteRequest
        → PlasmaStore 收到 PlasmaDeleteRequest
        → ObjectLifecycleManager::DeleteObject(object_id)
          → if (sealed && ref_count==0) → DeleteObjectInternal (立即删除)
          → if (ref_count > 0) → earger_deletion_objects_.emplace (标记待删)
          → if (!sealed) → earger_deletion_objects_.emplace (标记待删)
```

#### 13.4.5 FlushFreeObjects 的触发时机

`FlushFreeObjects` 有两种触发方式：

**方式1：ReleaseFreedObject 时批量触发**

当 owner 发布 eviction 消息，raylet 调用 `ReleaseFreedObject` 释放主副本 Pin：

```cpp
// local_object_manager.cc:106-148
void LocalObjectManager::ReleaseFreedObject(const ObjectID &object_id) {
  // ...
  // 从 pinned_objects_ 中移除 → RayObject 被销毁
  // → Plasma Store 的 ref_count 归零（通过 RemoveReference）
  auto pinned_objects_it = pinned_objects_.find(object_id);
  if (pinned_objects_it != pinned_objects_.end()) {
    pinned_objects_size_ -= pinned_objects_it->second->GetSize();
    pinned_objects_.erase(pinned_objects_it);  // ★ RayObject 销毁 → ref_count 归零
    local_objects_.erase(it);
  }

  // 加入待删除队列
  if (free_objects_period_ms_ >= 0) {
    objects_pending_deletion_.emplace(object_id);
  }

  // ★ 批量到达阈值或 period==0 时立即 flush
  if (objects_pending_deletion_.size() == free_objects_batch_size_ ||
      free_objects_period_ms_ == 0) {
    FlushFreeObjects();
  }
}
```

**方式2：定时器周期性触发**

```cpp
// node_manager.cc:418-421
if (RayConfig::instance().free_objects_period_milliseconds() > 0) {
  periodical_runner_->RunFnPeriodically(
      [this] { local_object_manager_.FlushFreeObjects(); },
      RayConfig::instance().free_objects_period_milliseconds(),
      "NodeManager.deadline_timer.flush_free_objects");
}
```

#### 13.4.6 Eager Deletion 的完整时序

```
阶段1: Owner 对象 OutOfScope
  → Owner 的 reference_counter 判断对象可以释放
  → 发布 WorkerObjectEviction 消息到 eviction channel

阶段2: Raylet 收到 eviction 消息
  → subscription_callback → ReleaseFreedObject(object_id)
  → pinned_objects_.erase → RayObject 销毁
  → Plasma Store: RemoveReference → ref_count 降到 0
  → EndObjectAccess → 对象加入 LRU (可被被动淘汰)
  → objects_pending_deletion_.emplace(object_id)

阶段3: FlushFreeObjects 触发（批量阈值或定时器）
  → on_objects_freed_(objects_to_delete)
  → ObjectManager::FreeObjects(object_ids)
  → PlasmaClient::Delete(object_ids)
  → PlasmaStore 收到 PlasmaDeleteRequest
  → ObjectLifecycleManager::DeleteObject(object_id)

阶段4: DeleteObject 的三种分支
  ├─ 对象在 LRU 中 (ref_count==0, sealed):
  │  → 立即 DeleteObjectInternal → dlfree + delete_callback
  │  → used_memory_ 减少
  │
  ├─ 对象被其他人 Get 了 (ref_count > 0):
  │  → earger_deletion_objects_.emplace (标记待急切删除)
  │  → 返回 ObjectInUse
  │  → 等其他人 Release → ref_count 降到 0
  │  → RemoveReference 检查 earger 集合 → 有! → 立即 DeleteObjectInternal
  │  → used_memory_ 减少
  │
  └─ 对象还未 Seal:
     → earger_deletion_objects_.emplace (标记待急切删除)
     → 返回 ObjectNotSealed
     → 等对象 Seal 后...
     → 但 Seal 不会检查 earger 集合！
     → 需要等后续 ref_count 变化时才会检查
```

#### 13.4.7 Eager Deletion vs 普通 LRU 淘汰 vs 普通 Unpin

```
┌─────────────────────────────────────────────────────────────────────┐
│                     对象释放的三种路径                                │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│ 路径A: 普通 Unpin (最常见的路径)                                     │
│   ref_count 1→0 → EndObjectAccess → 加入 LRU                        │
│   → 内存不释放，used_memory_ 不变                                     │
│   → 等 OOM 时被动淘汰才释放                                          │
│                                                                     │
│ 路径B: Eager Deletion (有人明确要删除但当时删不了)                    │
│   DeleteObject → ref>0 → 标记 earger                                │
│   → ref 降到 0 → 立即 DeleteObjectInternal                           │
│   → 内存立即释放，used_memory_ 立即减少                               │
│   → 绕过 LRU，不等 OOM                                               │
│                                                                     │
│ 路径C: 直接删除 (DeleteObject 时 ref==0 且 sealed)                    │
│   DeleteObject → ref==0 && sealed → 立即 DeleteObjectInternal        │
│   → 内存立即释放，used_memory_ 立即减少                               │
│   → 绕过 LRU，不等 OOM                                               │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

**为什么需要 Eager Deletion**：当 owner 释放对象（OutOfScope）时，raylet 会 `FlushFreeObjects` → `FreeObjects` → `PlasmaDeleteRequest` 主动要求 Plasma Store 删除对象。但如果此时对象刚好被其他人 Get 了（ref_count > 0），Plasma Store 无法立即删除。Eager Deletion 机制保证：一旦引用释放，立即删除，而不是让对象留在 LRU 中等待被动淘汰。

### 13.5 /cluster 页面显示的 "Object Store Memory"

Dashboard 的 `/cluster` 页面中，每个 Node 的 "Object Store Memory" 显示格式为：

```
X MB / Y MB (Z%)
```

其中：
- **分子 (X)** = `objectStoreUsedMemory` = 已使用的内存
- **分母 (Y)** = `objectStoreAvailableMemory + objectStoreUsedMemory` = 总容量
- **百分比 (Z)** = X / Y

### 13.6 指标的完整数据流

```
┌─────────────────────────────────────────────────────────────────┐
│ Plasma Store 线程                                                │
│                                                                  │
│  SealObjects() → add_object_callback_(object_info)              │
│    → [post 到 raylet 线程]                                      │
│    → ObjectManager::HandleObjectAdded()                          │
│         used_memory_ += data_size + metadata_size  ← ★ 增加     │
│                                                                  │
│  DeleteObjectInternal() → delete_object_callback_(object_id)    │
│    → [post 到 raylet 线程]                                      │
│    → ObjectManager::HandleObjectDeleted()                        │
│         used_memory_ -= data_size + metadata_size  ← ★ 减少     │
└──────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────┐
│ Raylet gRPC Handler                                              │
│                                                                  │
│  NodeManager::HandleGetNodeStats()                               │
│    → local_object_manager_.FillObjectStoreStats(reply)           │
│    → object_manager_.FillObjectStoreStats(reply)                 │
│         stats->set_object_store_bytes_used(used_memory_)  ← ★   │
│         stats->set_object_store_bytes_avail(config_.object_store_memory) │
└──────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────┐
│ Dashboard Head (Python)                                          │
│                                                                  │
│  node_head.py: _update_node_stats()                             │
│    → stub.GetNodeStats() → DataSource.node_stats[node_id]       │
│                                                                  │
│  datacenter.py: DataOrganizer.get_node_info()                    │
│    → store_stats = node_stats.get("storeStats", {})              │
│    → used = int(store_stats.get("objectStoreBytesUsed", 0))      │
│    → total = int(store_stats.get("objectStoreBytesAvail", 0))    │
│    → ray_stats = {                                               │
│         "object_store_used_memory": used,                        │
│         "object_store_available_memory": total - used,           │
│       }                                                          │
│    → node_info["raylet"].update(ray_stats)                      │
└──────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────┐
│ Dashboard Frontend (React/TypeScript)                            │
│                                                                  │
│  NodeRow.tsx / index.tsx:                                        │
│    const objectStoreTotalMemory =                                │
│        raylet.objectStoreAvailableMemory + raylet.objectStoreUsedMemory │
│    PercentageBar:                                                │
│      num = raylet.objectStoreUsedMemory                          │
│      total = objectStoreTotalMemory                              │
└──────────────────────────────────────────────────────────────────┘
```

### 13.7 used_memory_ 的来源 — 不是 Allocated()

**Dashboard 显示的 "Object Store Memory" 是 `ObjectManager::used_memory_`，不是 `PlasmaAllocator::Allocated()`。**

```cpp
// object_manager.h:501
int64_t used_memory_ = 0;

// object_manager.cc:177
void ObjectManager::HandleObjectAdded(const ObjectInfo &object_info) {
  local_objects_[object_id].object_info = object_info;
  used_memory_ += object_info.data_size + object_info.metadata_size;  // ★ Seal 时增加
}

// object_manager.cc:200-205
void ObjectManager::HandleObjectDeleted(const ObjectID &object_id) {
  auto object_info = it->second.object_info;
  local_objects_.erase(it);
  used_memory_ -= object_info.data_size + object_info.metadata_size;  // ★ 删除时减少
}
```

### 13.8 三个不同层次的内存指标

| 指标 | 位置 | 含义 | Dashboard 显示 |
|------|------|------|----------------|
| `PlasmaAllocator::Allocated()` | plasma_allocator.cc:134 | dlmalloc 实际分配的总字节（含 fallback） | ✗ 不显示 |
| `ObjectStatsCollector::num_bytes_in_use_` | stats_collector.h:83 | ref_count > 0 的对象占用字节 | ✗ 不显示 |
| `ObjectManager::used_memory_` | object_manager.h:501 | raylet 视角所有已知对象的 data_size+metadata_size 之和 | ✓ **这就是 Dashboard 的值** |

### 13.9 Unpin 时 used_memory_ 不会减少

**关键**：`used_memory_` 只在 `HandleObjectDeleted` 时减少，而 `HandleObjectDeleted` 只由 `delete_object_callback_` 触发。

```
Unpin (ref_count: 1→0):
  → RemoveReference → EndObjectAccess → 加入 LRU
  → ★ delete_object_callback_ 未被调用
  → ★ HandleObjectDeleted 未被调用
  → ★ used_memory_ 不减少
  → ★ Dashboard "Object Store Memory" 不变

Eviction (真正淘汰):
  → EvictObjects → DeleteObjectInternal
  → delete_object_callback_(object_id)  ← ★ 被调用
  → HandleObjectDeleted → used_memory_ -= size  ← ★ 减少
  → Dashboard "Object Store Memory" 减少
```

### 13.10 各操作对内存指标的影响

| 操作 | Allocated() | used_memory_ | Dashboard 显示 |
|------|-------------|-------------|----------------|
| Create (dlmemalign 成功) | += size | 不变 (还未 Seal) | 不变 |
| Seal | 不变 | += data_size + metadata_size | 增加 |
| Get (ref_count 0→1) | 不变 | 不变 | 不变 |
| Release/Unpin (ref_count 1→0) | 不变 | 不变 | **不变** |
| 加入 LRU (EndObjectAccess) | 不变 | 不变 | 不变 |
| LRU 淘汰 (EvictObjects) | -= size (dlfree) | -= data+metadata_size | **减少** |
| Spill 完成后 Unpin | 不变 (直到被淘汰) | 不变 | 不变 |
| Spill 后被 LRU 淘汰 | -= size | -= data+metadata_size | **减少** |
| Abort (unsealed 对象) | -= size | 不变 (未 Seal 过) | 不变 |
| Fallback Allocate | += size | 后续 Seal 时增加 | 增加 |

### 13.11 Unpin 后内存指标的"假象"

```
实际场景:
  1. 1000 个对象被 Seal → used_memory_ = 1GB, Allocated = 1GB
  2. 所有对象被 Unpin → ref_count = 0 → 全部在 LRU 中
  3. 此时:
     Allocated = 1GB        (dlmalloc 未 free，内存仍占用)
     used_memory_ = 1GB     (HandleObjectDeleted 未调用)
     Dashboard = 1GB / 1GB (100%)  ← ★ 显示 100% 使用！

  4. 新 Create 请求 OOM → LRU 淘汰 200MB
  5. 此时:
     Allocated = 800MB     (dlfree 了 200MB)
     used_memory_ = 800MB  (HandleObjectDeleted 减少了 200MB)
     Dashboard = 800MB / 1GB (80%)
```

**所以 Dashboard 显示的"使用量"在 Unpin 后不会下降，只有真正淘汰后才会下降。即使所有对象都已在 LRU 中（可被淘汰），Dashboard 仍显示 100% 使用。**

---

## 14. 关键数值汇总

| 指标 | 值 | 来源 |
|------|------|------|
| Plasma 容量上限 | 系统内存或 /dev/shm 的 90% | `plasma_allocator.cc:GetFootprintLimit()` |
| 每次淘汰最小目标 | 总容量的 20% | `eviction_policy.cc:126: GetFootprintLimit()/5` |
| CreateObjectInternal 重试次数 | 11 次 (num_tries=0..10) | `obj_lifecycle_mgr.cc:179` |
| OOM grace period | `oom_grace_period_s` 配置（默认 5s） | `RayConfig::instance().oom_grace_period_s()` |
| 淘汰前提 | sealed 且 ref_count==0 | `obj_lifecycle_mgr.cc:262-267` |
| Spill 前提 | sealed 且 ref_count==1 且 spillable | `local_object_manager.cc:is_plasma_object_spillable_()` |
| Spill 触发时机 | 仅在 OOM 后被 `spill_objects_callback_()` 调用 | `create_request_queue.cc:117` |
| Spill 最小大小 | `min_spilling_size` 配置 | `RayConfig::instance().min_spilling_size()` |
| Spill 最大融合数 | `max_fused_object_count` | `local_object_manager.h` 构造参数 |
| Spill 最大文件大小 | `max_spilling_file_size_bytes` | `RayConfig::instance().max_spilling_file_size_bytes()` |
| 分配对齐 | 64 字节 | `plasma_allocator.cc:57: kAllocationAlignment` |
| dlmalloc 内部保留 | 256 * sizeof(size_t) 字节 | `plasma_allocator.cc:63: kDlMallocReserved` |

---

## 15. 源码索引

| 文件 | 关键函数/类 | 说明 |
|------|------------|------|
| `eviction_policy.h` | `LRUCache`, `EvictionPolicy` | LRU 数据结构和淘汰策略接口 |
| `eviction_policy.cc` | `RequireSpace`, `ChooseObjectsToEvict`, `BeginObjectAccess`, `EndObjectAccess` | LRU 淘汰核心逻辑 |
| `obj_lifecycle_mgr.h` | `ObjectLifecycleManager` | 对象生命周期管理器 |
| `obj_lifecycle_mgr.cc` | `CreateObjectInternal`, `EvictObjects`, `DeleteObjectInternal`, `AddReference`, `RemoveReference` | 对象创建+淘汰+引用计数 |
| `create_request_queue.h` | `CreateRequestQueue` | Create 请求队列 |
| `create_request_queue.cc` | `ProcessRequests`, `ProcessRequest`, `TryRequestImmediately` | OOM 处理链路 |
| `plasma_allocator.h` | `PlasmaAllocator` | 内存分配器 |
| `plasma_allocator.cc` | `Allocate`, `FallbackAllocate`, `Free`, `GetFootprintLimit` | dlmalloc 内存分配/释放 |
| `object_store.cc` | `CreateObject`, `DeleteObject`, `SealObject` | 对象存储层 |
| `store.cc` | `PlasmaStore`, `HandleCreateObjectRequest`, `SealObjects` | Plasma Store 主类 |
| `local_object_manager.h` | `LocalObjectManager` | 本地对象管理器（Pin/Spill/Restore） |
| `local_object_manager.cc` | `SpillObjectUptoMaxThroughput`, `TryToSpillObjects`, `ReleaseFreedObject`, `OnObjectSpilled` | Spill 核心逻辑 |
| `object_manager.h` | `used_memory_` | Dashboard "Object Store Memory" 的数据源 |
| `object_manager.cc` | `HandleObjectAdded`, `HandleObjectDeleted`, `FillObjectStoreStats`, `FreeObjects` | used_memory_ 增减与 gRPC 上报 |
| `object_buffer_pool.cc` | `FreeObjects` | 转发 Delete 请求到 Plasma Store |
| `plasma/client.cc` | `PlasmaClient::Delete` | 发送 PlasmaDeleteRequest |
| `raylet/main.cc` | spill_objects_callback, object_store_full_callback, add/delete_object_callback, on_objects_freed | 回调注册 |
| `node_manager.cc` | `HandleGetNodeStats`, FlushFreeObjects 定时器 | gRPC GetNodeStats 处理 + 定期 flush |
| `datacenter.py` | `DataOrganizer.get_node_info` | Dashboard 后端数据转换 |
| `NodeRow.tsx` / `index.tsx` | PercentageBar | Dashboard 前端显示 |

---

## 16. Primary Object Spill 后的 Delete 流程

1. **Owner GC 触发** → `ReleaseFreedObject(object_id)`
   - 如果还在 `pinned_objects_`（未 spill）：直接释放内存，无需删磁盘
   - 如果已 spill（不在 `pinned_objects_`）：标记 `is_freed_=true`，push 进 `spilled_object_pending_delete_` 队列

2. **ProcessSpilledObjectsDeleteQueue** 处理队列：
   - 在 `spilled_objects_url_` 中找到 URL → 递减 `url_ref_count_`，ref=0 时收集 URL 删磁盘
   - 递减 `spilled_bytes_current_`（primary spill 当前值）
   - 删除 `local_objects_entry`

3. **DeleteSpilledObjects** 发送 RPC 给 IO worker 删磁盘文件（失败重试最多 3 次）

---

## 17. Replicated Object Spill 后的 Delete 流程

1. **Owner GC 触发** → `ReplicatedObjectManager::HandleObjectFreed(object_id)`
   - 如果还在 `replicated_objects_`（未 spill）：释放 pinned 内存
   - 如果已 spill（在 `spilled_object_ids_`）：
     - 递减 `spilled_replicated_bytes_current_`
     - erase `spilled_object_ids_`
     - 调用 `on_spilled_replicated_delete_` → `EnqueueSpilledObjectForDelete` → push 进 `spilled_object_pending_delete_` 队列
   - 如果正在 spill（在 `pending_free_object_ids_`）：推迟到 `OnSpillComplete`

2. **ProcessSpilledObjectsDeleteQueue** 处理队列：
   - `spilled_objects_url_` 中有 replicated 对象的 entry（`OnObjectSpilled` 对所有对象都 emplace 了）
   - 可以找到 URL → 递减 `url_ref_count_`，ref=0 时删磁盘
   - `is_replicated = !local_objects_.contains(object_id)` 为 true，不会递减 `spilled_bytes_current_`（正确）

磁盘文件删除流程完整：replicated 对象的 `url_ref_count_` 和磁盘删除都能正确执行，无泄漏。

---

## 18. HandleObjectMissing vs HandleObjectFreed

| | HandleObjectMissing | HandleObjectFreed |
|---|---|---|
| **触发原因** | plasma LRU 淘汰，object 从内存中消失 | owner worker 通知 object out of scope（GC 释放） |
| **语义** | "内存中没了，但磁盘副本仍有效" | "对象生命周期结束，磁盘副本也应该删除" |
| **对 spilled entry** | 保留（磁盘副本还在，可 restore） | 删除（owner 不要了，磁盘也要清掉） |
| **对 spilled_replicated_bytes_current_** | 不减（磁盘占用还在） | 减（磁盘占用释放） |
| **对 subscription** | spilled entry 保留订阅不变 | 取消订阅 |
| **是否触发磁盘删除** | 否 | 是（`on_spilled_replicated_delete_` → 入 delete 队列） |

---

## 19. Pull 依赖对象的完整生命周期

```
① RequestLeaseDependencies
│  ├─ 记录依赖关系到 required_objects_
│  └─ lease_entry->pull_request_id_ = object_manager_.Pull(required_objects, TASK_ARGS)
│     → PullManager 开始从远端 fetch objects → PinNewObjectIfNeeded（plasma 级 pin）

② Pull 完成 → 对象到达本地 plasma
│  ├─ HandleObjectLocal → HandleObjectLocal → DecrementMissingDependencies
│  │  → 所有依赖就绪 → lease 进入调度队列
│  └─ PullManager.PinNewObjectIfNeeded → TryPinObject（plasma 客户端引用级 pin）

③ Lease 调度 — PinLeaseArgs 接管保护
│  ├─ PinLeaseArgsIfMemoryAvailable → get_lease_arguments_ → PinLeaseArgs
│  │  → pinned_lease_arguments_[dep] = (RayObject, refcount++)  // 防止 LRU 淘汰
│  │
│  ├─ RemoveLeaseDependencies → CancelPull(pull_request_id_)
│  │  → PullManager.DeactivateBundlePullRequest → UnpinObject（释放 PullManager pin）
│  │  // 此时 PinLeaseArgs 已接管保护，对象不会被 LRU
│  │
│  └─ PopWorker → GrantLease → worker 执行任务

④ Lease 完成
   └─ CleanupLease → ReleaseLeaseArgs
      → pinned_lease_arguments_[dep].refcount--
      → refcount == 0 → 释放 RayObject unique_ptr
         → 如果 PinObjectsAndWaitForFree 没有长期 pin → 对象可被 plasma LRU 淘汰
```

---

## 20. CancelPull 的必要性

`CancelPull` 不仅是"停止拉取"，更重要的是清理 PullManager 中的残留状态和资源：

1. **pinned_objects_** — 持有 plasma 客户端引用（占用内存配额）
2. **object_pull_requests_** — 维护 pull 状态和重试计时器
3. **object directory 位置订阅** — 订阅对象位置变化（浪费网络开销）
4. **active_object_pull_requests_** — 占用 admission control 配额

Pull 完成后如果不 CancelPull，这些资源永远不会被清理。

---

## 21. Fused Spill 文件与指标语义

Ray 的 fused spill 机制将多个 object 合并写入同一个文件，`url_ref_count_` 记录每个 base URL 的引用数。

`spilled_bytes_current_`（Primary）和 `spilled_replicated_bytes_current_`（Replicated）的含义：

- **递增时机**：`OnObjectSpilled` / `OnSpillComplete` 时，对象成功 spill 到磁盘
- **递减时机**：
  - Primary：`ProcessSpilledObjectsDeleteQueue` 中，从 `spilled_objects_url_` 删 entry 时
  - Replicated：`HandleObjectFreed` / `ReleasePins` 中，owner GC 时
- **语义**："仍有效的 spill 字节数"（对象逻辑上未 free），不等于实际磁盘占用

由于 fused spill 文件机制：
- 递减 `spilled_bytes_current_` 时只是 `url_ref_count_--`，磁盘文件可能还在（ref_count > 0）
- 只有 `url_ref_count_ == 0` 时才真正删磁盘文件
- RPC 删除可能失败（最多重试 3 次）

---

## 22. Spill 指标体系

| 指标 | 类型 | tag | 含义 |
|------|------|-----|------|
| `spill_manager_objects_bytes{Spilled,Primary}` | Gauge | State=Spilled, Source=Primary | Primary spill 仍有效字节数 |
| `spill_manager_objects_bytes{Spilled,Replicated}` | Gauge | State=Spilled, Source=Replicated | Replicated spill 仍有效字节数 |
| `spill_manager_objects{OnDisk}` | Gauge | State=OnDisk | 磁盘上残留的 fused spill 文件数量（`url_ref_count_.size()`） |
| `spill_manager_objects{PendingDelete}` | Gauge | State=PendingDelete | delete 队列深度 |
| `spill_manager_request_total{FailedDeletion}` | Counter | Type=FailedDeletion | 删除 RPC 失败次数 |

泄漏检测方法：
- OnDisk 文件数量持续增长但 Spilled 字节数平稳 → fused file 未及时删除
- FailedDeletion 增长 → 删除 RPC 失败
- `spilled_bytes_current_` + `spilled_replicated_bytes_current_` 对比 OnDisk，差值反映"fused file 中对象已 free 但文件未删"的部分

三者的关系：OnDisk(文件数) >= 0，Spilled(字节数)反映逻辑状态，两者独立。如果 OnDisk 持续增长而 Spilled 下降，说明 fused file 泄漏。

---

## 23. 代码修改记录

| 修改 | 文件 | 说明 |
|------|------|------|
| RAY_CHECK 下溢保护 | `replicated_object_manager.cc` | `HandleObjectFreed` / `ReleasePins` / `RestoreSpilledReplicatedObject` 中 `spilled_replicated_bytes_current_` 减操作前加 RAY_CHECK |
| OnSpillComplete 优化 | `replicated_object_manager.cc` | 提前检查 `pending_free_object_ids_`，避免不必要的 emplace+erase |
| 删除无用累计指标 | `node_manager.proto`, `local_object_manager.h/cc` | 删除 `spilled_bytes_primary` / `spilled_bytes_replicated` / `spilled_objects_primary` / `spilled_objects_replicated` 及相关代码 |
| OnDisk gauge | `node_manager.proto`, `local_object_manager.cc` | 新增 `spilled_files_on_disk` proto 字段和 `spill_manager_objects{OnDisk}` gauge |
| 命名一致性 | `local_object_manager.cc` | Restored → RestoredTotal，与 SpilledTotal 一致 |
| 代码清理 | `local_object_manager.cc` | 合并 `OnObjectSpilled` 中两个连续的 `if (is_replicated)` 块，DebugString 补充 OnDisk 信息 |
