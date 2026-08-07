# Plasma Store 对象生命周期与 Pull/Push/Create 全链路分析

本文档详细分析 Ray Plasma Store 中 `add_object_callback_`、`delete_object_callback_`、`ObjectLifecycleManager` 三者之间的关系，以及对象从创建、拉取、接收、淘汰的完整生命周期。

---

## 目录

- [1. 核心组件关系总览](#1-核心组件关系总览)
- [2. PlasmaStore 与两个 Callback](#2-plasmastore-与两个-callback)
- [3. ObjectLifecycleManager 详解](#3-objectlifecyclemanager-详解)
- [4. 对象状态与生命周期](#4-对象状态与生命周期)
- [5. ObjectBufferPool — chunk 级创建与 seal](#5-objectbufferpool--chunk-级创建与-seal)
- [6. PullManager — 对象拉取调度](#6-pullmanager--对象拉取调度)
- [7. PushManager — 对象推送管理](#7-pushmanager--对象推送管理)
- [8. 完整链路 1：Worker 本地创建对象](#8-完整链路-1worker-本地创建对象)
- [9. 完整链路 2：Pull 远程对象](#9-完整链路-2pull-远程对象)
- [10. 完整链路 3：Replication Push 接收对象](#10-完整链路-3replication-push-接收对象)
- [11. 完整链路 4：对象淘汰/删除](#11-完整链路-4对象淘汰删除)
- [12. Callback 触发汇总表](#12-callback-触发汇总表)
- [13. 关键代码索引](#13-关键代码索引)

---

## 1. 核心组件关系总览

```
┌─────────────────────────────────────────────────────────────────────┐
│                          PlasmaStore (store.cc)                      │
│                                                                       │
│  add_object_callback_ ◄──── SealObjects()                            │
│  delete_object_callback_ ──┐                                         │
│                             │                                         │
│  object_lifecycle_mgr_ ◄───┘ (同时持有 delete_object_callback_)       │
│    ├── object_store_        (IObjectStore — 存储 LocalObject)          │
│    ├── eviction_policy_     (IEvictionPolicy — LRU 淘汰策略)          │
│    ├── earger_deletion_objects_ (待删除集合)                           │
│    └── stats_collector_     (统计)                                     │
│                                                                       │
│  create_request_queue_     (创建请求队列)                              │
│  get_request_queue_        (Get 请求队列)                              │
└────────────────────┬────────────────────────────────────────────────┘
                     │
                     │ add_object_callback_ 触发后
                     ▼
┌─────────────────────────────────────────────────────┐
│  raylet/main.cc (add_object_callback 绑定)           │
│    ├── object_manager->HandleObjectAdded()           │
│    └── node_manager->HandleObjectLocal()              │
│          ├── lease_dependency_manager_                │
│          ├── wait_manager_                           │
│          ├── async_plasma_objects_notification_       │
│          └── MaybeReplicateObject()                   │
└─────────────────────────────────────────────────────┘
                     │
                     │ delete_object_callback_ 触发后
                     ▼
┌─────────────────────────────────────────────────────┐
│  raylet/main.cc (delete_object_callback 绑定)        │
│    ├── object_manager->HandleObjectDeleted()         │
│    └── node_manager->HandleObjectMissing()           │
└─────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────┐
│  ObjectBufferPool (object_buffer_pool.cc)            │
│    ├── create_buffer_state_  (chunk 状态追踪)        │
│    ├── store_client_         (plasma client)         │
│    │     ├── CreateAndSpillIfNeeded()                 │
│    │     ├── Seal() → SealObjects → add_object_cb     │
│    │     └── Abort() → AbortObject                    │
│    ├── CreateChunk() → EnsureBufferExists             │
│    ├── WriteChunk() → memcpy + seal on last chunk     │
│    └── AbortCreate() → Release + Abort                │
└─────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────┐
│  PullManager (pull_manager.cc)                       │
│    ├── get_request_bundles_   (GET_REQUEST 最高优先) │
│    ├── wait_request_bundles_  (WAIT_REQUEST 中优先)  │
│    ├── task_argument_bundles_ (TASK_ARGS 最低优先)   │
│    ├── active_object_pull_requests_ (活跃拉取集合)   │
│    ├── object_pull_requests_  (每个 object 的拉取状态)│
│    └── UpdatePullsBasedOnAvailableMemory()            │
└─────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────┐
│  PushManager (push_manager.cc)                       │
│    ├── push_state_map_       (节点→对象→PushState)   │
│    ├── chunks_in_flight_      (在途 chunk 数)         │
│    ├── max_chunks_in_flight_  (最大在途 chunk 限流)   │
│    └── ScheduleRemainingPushes() (轮询调度)           │
└─────────────────────────────────────────────────────┘
```

---

## 2. PlasmaStore 与两个 Callback

### 定义

```cpp
// src/ray/object_manager/plasma/common.h
using AddObjectCallback = std::function<void(const ObjectInfo &, plasma::flatbuf::ObjectSource)>;
using DeleteObjectCallback = std::function<void(const ObjectID &)>;
```

### 构造函数中赋值

```cpp
// src/ray/object_manager/plasma/store.cc:69-124
PlasmaStore::PlasmaStore(
    instrumented_io_context &main_service,
    IAllocator &allocator,
    ray::FileSystemMonitor &fs_monitor,
    const std::string &socket_name,
    uint32_t delay_on_oom_ms,
    ray::SpillObjectsCallback spill_objects_callback,
    std::function<void()> object_store_full_callback,
    ray::AddObjectCallback add_object_callback,
    ray::DeleteObjectCallback delete_object_callback)
    : add_object_callback_(add_object_callback),          // 保存 add callback
      delete_object_callback_(delete_object_callback),   // 保存 delete callback
      object_lifecycle_mgr_(allocator_, delete_object_callback_),  // 同时传给 lifecycle mgr
      ...
```

**关键点**：`delete_object_callback_` 被**两处**持有：
1. `PlasmaStore::delete_object_callback_` — 但 PlasmaStore 本身**从不直接调用**它
2. `ObjectLifecycleManager::delete_object_callback_` — **实际调用者**

`add_object_callback_` 只由 `PlasmaStore` 持有并在 `SealObjects` 中调用。

### 成员变量

```cpp
// src/ray/object_manager/plasma/store.h:250-258
const ray::AddObjectCallback add_object_callback_;
const ray::DeleteObjectCallback delete_object_callback_;
ObjectLifecycleManager object_lifecycle_mgr_ ABSL_GUARDED_BY(mutex_);
```

### add_object_callback_ 的唯一触发点

```cpp
// src/ray/object_manager/plasma/store.cc:274-283
void PlasmaStore::SealObjects(const std::vector<ObjectID> &object_ids) {
  for (size_t i = 0; i < object_ids.size(); ++i) {
    RAY_LOG(DEBUG) << "sealing object " << object_ids[i];
    auto entry = object_lifecycle_mgr_.SealObject(object_ids[i]);
    RAY_CHECK(entry) << object_ids[i] << " is missing or not sealed.";
    add_object_callback_(entry->GetObjectInfo(), entry->GetSource());  // 唯一触发点
  }
  for (size_t i = 0; i < object_ids.size(); ++i) {
    get_request_queue_.MarkObjectSealed(object_ids[i]);
  }
}
```

### delete_object_callback_ 的唯一触发点

```cpp
// src/ray/object_manager/plasma/obj_lifecycle_mgr.cc:198-214
void ObjectLifecycleManager::DeleteObjectInternal(const ObjectID &object_id) {
  auto entry = object_store_->GetObject(object_id);
  RAY_CHECK(entry != nullptr);

  bool aborted = entry->state_ == ObjectState::PLASMA_CREATED;  // 是否是未 seal 的对象

  stats_collector_->OnObjectDeleting(*entry);
  earger_deletion_objects_.erase(object_id);
  eviction_policy_->RemoveObject(object_id);
  object_store_->DeleteObject(object_id);

  if (!aborted) {
    // 只对已 seal 的对象触发 delete callback
    // 未 seal 的对象被 abort 时不触发
    delete_object_callback_(object_id);
  }
}
```

### Callback 的绑定（raylet/main.cc）

```cpp
// src/ray/raylet/main.cc:800-816
/*add_object_callback=*/
[&](const ray::ObjectInfo &object_info,
    plasma::flatbuf::ObjectSource source) {
  main_service.post(
      [&object_manager, &node_manager, object_info, source]() {
        object_manager->HandleObjectAdded(object_info);
        node_manager->HandleObjectLocal(object_info, source);
      }, "ObjectManager.ObjectAdded");
},

/*delete_object_callback=*/
[&](const ray::ObjectID &object_id) {
  main_service.post(
      [&object_manager, &node_manager, object_id]() {
        object_manager->HandleObjectDeleted(object_id);
        node_manager->HandleObjectMissing(object_id);
      }, "ObjectManager.ObjectMissing");
},
```

**线程安全**：callback 通过 `main_service.post()` 投递到 raylet 主线程异步执行，避免在 plasma store 的锁内直接调用 raylet 代码。

---

## 3. ObjectLifecycleManager 详解

### 类定义

```cpp
// src/ray/object_manager/plasma/obj_lifecycle_mgr.h:105-188
class ObjectLifecycleManager : public IObjectLifecycleManager {
 public:
  ObjectLifecycleManager(IAllocator &allocator,
                         ray::DeleteObjectCallback delete_object_callback);

  std::pair<const LocalObject *, flatbuf::PlasmaError> CreateObject(
      const ray::ObjectInfo &object_info,
      plasma::flatbuf::ObjectSource source,
      bool fallback_allocator) override;

  const LocalObject *GetObject(const ObjectID &object_id) const override;
  const LocalObject *SealObject(const ObjectID &object_id) override;
  flatbuf::PlasmaError AbortObject(const ObjectID &object_id) override;
  flatbuf::PlasmaError DeleteObject(const ObjectID &object_id) override;
  bool AddReference(const ObjectID &object_id) override;
  bool RemoveReference(const ObjectID &object_id) override;

 private:
  const LocalObject *CreateObjectInternal(
      const ray::ObjectInfo &object_info,
      plasma::flatbuf::ObjectSource source,
      bool allow_fallback_allocation);
  void EvictObjects(const std::vector<ObjectID> &object_ids);
  void DeleteObjectInternal(const ObjectID &object_id);

  std::unique_ptr<IObjectStore> object_store_;        // 持有所有 LocalObject
  std::unique_ptr<IEvictionPolicy> eviction_policy_;  // LRU 淘汰策略
  const ray::DeleteObjectCallback delete_object_callback_;
  absl::flat_hash_set<ObjectID> earger_deletion_objects_;  // 待删除集合
  std::unique_ptr<ObjectStatsCollector> stats_collector_;
};
```

### 核心方法

#### CreateObject

```cpp
// obj_lifecycle_mgr.cc:39-54
std::pair<const LocalObject *, PlasmaError>
ObjectLifecycleManager::CreateObject(const ObjectInfo &object_info,
                                      ObjectSource source,
                                      bool fallback_allocator) {
  // 检查是否已存在
  if (object_store_->GetObject(object_info.object_id) != nullptr) {
    return {nullptr, PlasmaError::ObjectExists};
  }
  auto entry = CreateObjectInternal(object_info, source, fallback_allocator);
  if (entry == nullptr) {
    return {nullptr, PlasmaError::OutOfMemory};
  }
  // 通知淘汰策略和统计
  eviction_policy_->ObjectCreated(object_info.object_id, entry, fallback_allocator);
  stats_collector_->OnObjectCreating(*entry);
  return {entry, PlasmaError::OK};
}
```

#### CreateObjectInternal — 分配内存，必要时淘汰

```cpp
// obj_lifecycle_mgr.cc:170-205
const LocalObject *ObjectLifecycleManager::CreateObjectInternal(...) {
  // 最多尝试 10 次分配
  for (int num_tries = 0; num_tries <= 10; num_tries++) {
    auto result = object_store_->CreateObject(object_info, source, false);
    if (result != nullptr) return result;

    // 空间不够，请求淘汰策略选出可淘汰对象
    std::vector<ObjectID> objects_to_evict;
    int64_t space_needed =
        eviction_policy_->RequireSpace(object_info.GetObjectSize(), objects_to_evict);
    EvictObjects(objects_to_evict);

    if (space_needed > 0) break;  // 仍然不够，跳出
  }

  // 主分配器失败，尝试 fallback（文件系统分配）
  if (!allow_fallback_allocation) return nullptr;
  auto result = object_store_->CreateObject(object_info, source, true);
  return result;
}
```

#### SealObject

```cpp
// obj_lifecycle_mgr.cc:56-63
const LocalObject *ObjectLifecycleManager::SealObject(const ObjectID &object_id) {
  auto entry = object_store_->SealObject(object_id);
  if (entry != nullptr) {
    stats_collector_->OnObjectSealed(*entry);
  }
  return entry;
}
```

`object_store_->SealObject()` 将对象状态从 `PLASMA_CREATED` 改为 `PLASMA_SEALED`。

#### AbortObject

```cpp
// obj_lifecycle_mgr.cc:65-82
PlasmaError ObjectLifecycleManager::AbortObject(const ObjectID &object_id) {
  auto entry = object_store_->GetObject(object_id);
  if (entry == nullptr) return PlasmaError::ObjectNonexistent;
  if (entry->state_ != ObjectState::PLASMA_CREATED) return PlasmaError::ObjectSealed;

  // 不管 ref_count，强制删除
  DeleteObjectInternal(object_id);
  return PlasmaError::OK;
}
```

**注意**：`AbortObject` 调用 `DeleteObjectInternal`，但因为对象状态是 `PLASMA_CREATED`，`DeleteObjectInternal` 中 `aborted=true`，**不会触发 `delete_object_callback_`**。

#### DeleteObject

```cpp
// obj_lifecycle_mgr.cc:84-105
PlasmaError ObjectLifecycleManager::DeleteObject(const ObjectID &object_id) {
  auto entry = object_store_->GetObject(object_id);
  if (entry == nullptr) return PlasmaError::ObjectNonexistent;

  if (entry->state_ != ObjectState::PLASMA_SEALED) {
    // 未 seal 的对象，加入待删除集合，等 seal 后删除
    earger_deletion_objects_.insert(object_id);
    return PlasmaError::ObjectNotSealed;
  }
  if (entry->ref_count_ > 0) {
    // 有引用，加入待删除集合，等 ref_count 降为 0 后删除
    earger_deletion_objects_.insert(object_id);
    return PlasmaError::ObjectInUse;
  }
  // sealed + ref_count==0 → 立即删除
  DeleteObjectInternal(object_id);
  return PlasmaError::OK;
}
```

#### AddReference / RemoveReference

```cpp
// obj_lifecycle_mgr.cc:131-174
bool ObjectLifecycleManager::AddReference(const ObjectID &object_id) {
  auto entry = object_store_->GetObject(object_id);
  if (entry->ref_count_ == 0) {
    // 从 0 变为 1 → 通知淘汰策略：对象正在被访问，不可淘汰
    eviction_policy_->BeginObjectAccess(object_id);
  }
  entry->ref_count_++;
  return true;
}

bool ObjectLifecycleManager::RemoveReference(const ObjectID &object_id) {
  auto entry = object_store_->GetObject(object_id);
  entry->ref_count_--;
  if (entry->ref_count_ > 0) return true;

  // ref_count 降为 0 → 通知淘汰策略：对象可被淘汰
  eviction_policy_->EndObjectAccess(object_id);

  // 如果在待删除集合中，立即删除
  if (earger_deletion_objects_.count(object_id) > 0) {
    DeleteObjectInternal(object_id);
  }
  return true;
}
```

#### EvictObjects

```cpp
// obj_lifecycle_mgr.cc:222-239
void ObjectLifecycleManager::EvictObjects(const std::vector<ObjectID> &object_ids) {
  for (const auto &object_id : object_ids) {
    auto entry = object_store_->GetObject(object_id);
    RAY_CHECK(entry != nullptr);
    RAY_CHECK(entry->state_ == ObjectState::PLASMA_SEALED)
        << "To evict an object it must have been sealed.";
    RAY_CHECK(entry->ref_count_ == 0)
        << "To evict an object, there must be no clients currently using it.";
    DeleteObjectInternal(object_id);
  }
}
```

---

## 4. 对象状态与生命周期

### 对象状态枚举

```cpp
// src/ray/object_manager/plasma/common.h:41-45
enum class ObjectState : int {
  PLASMA_CREATED = 1,  // 已创建但未 seal
  PLASMA_SEALED = 2,   // 已 seal，不可变，可通过 Get 访问
};
```

### LocalObject

```cpp
// src/ray/object_manager/plasma/common.h:102-188
class LocalObject {
 public:
  ObjectState state_ = ObjectState::PLASMA_CREATED;
  mutable int32_t ref_count_ = 0;
  ray::ObjectInfo object_info_;
  Allocation allocation_;
  plasma::flatbuf::ObjectSource source_;  // 对象来源
};
```

### 状态流转图

```
CreateObject
    │
    ▼
PLASMA_CREATED (ref_count ≥ 1，创建者持有引用)
    │
    ├── SealObject ──────────> PLASMA_SEALED → add_object_callback_ 触发
    │                                              │
    │                                              ├── 引用正常管理 (AddRef / RemoveRef)
    │                                              │
    │                                              ├── ref_count == 0 + 淘汰 → DeleteObjectInternal
    │                                              │                                → delete_object_callback_
    │                                              │
    │                                              └── ref_count > 0 + Delete → earger_deletion_objects_
    │                                                        │
    │                                                        └── RemoveRef → ref_count==0 → DeleteObjectInternal
    │                                                                                      → delete_object_callback_
    │
    └── AbortObject ─────────> DeleteObjectInternal
                                → aborted=true → 不触发 delete_object_callback_
```

### earger_deletion_objects_（待删除集合）

当 `DeleteObject` 被调用但对象**暂时无法删除**时（未 seal 或有引用），加入此集合。后续在 `SealObject` 或 `RemoveReference` 检测到条件满足时，自动触发 `DeleteObjectInternal`。

---

## 5. ObjectBufferPool — chunk 级创建与 seal

### 数据结构

```cpp
// src/ray/object_manager/object_buffer_pool.h:167-188
enum class CreateChunkState : uint8_t {
  AVAILABLE = 0,   // chunk 未被写入
  REFERENCED,      // CreateChunk 成功，正在被写入
  SEALED,          // chunk 已写入完成
};

struct CreateBufferState {
  uint64_t metadata_size_;
  uint64_t data_size_;
  std::vector<ChunkInfo> chunk_info_;
  std::vector<CreateChunkState> chunk_state_;
  uint64_t num_seals_remaining_;      // 还剩几个 chunk 未写入
  uint64_t num_inflight_copies_ = 0;  // 正在 memcpy 的线程数（防止 abort 时竞争）
};

absl::flat_hash_map<ObjectID, CreateBufferState> create_buffer_state_;
```

### CreateChunk

```cpp
// src/ray/object_manager/object_buffer_pool.cc:110-128
Status ObjectBufferPool::CreateChunk(const ObjectID &object_id,
                                     const rpc::Address &owner_address,
                                     uint64_t data_size, uint64_t metadata_size,
                                     uint64_t chunk_index,
                                     plasma::flatbuf::ObjectSource source) {
  absl::MutexLock lock(&pool_mutex_);
  // 确保对象 buffer 已在 plasma 中创建
  RAY_RETURN_NOT_OK(EnsureBufferExists(
      object_id, owner_address, data_size, metadata_size, chunk_index, source));
  auto &state = create_buffer_state_.at(object_id);
  // 检查 chunk 状态
  if (state.chunk_state_[chunk_index] != CreateChunkState::AVAILABLE) {
    return Status::IOError("Chunk already received by a different thread.");
  }
  state.chunk_state_[chunk_index] = CreateChunkState::REFERENCED;
  return Status::OK();
}
```

### EnsureBufferExists — 首次 chunk 触发 plasma Create

```cpp
// object_buffer_pool.cc:247-324
Status ObjectBufferPool::EnsureBufferExists(...) {
  if (create_buffer_state_.contains(object_id)) return Status::OK();  // 已存在

  // 等待其他线程的 inflight create 完成
  // ...

  // 阻塞调用 plasma client 创建对象
  RAY_RETURN_NOT_OK(store_client_->CreateAndSpillIfNeeded(
      object_id, owner_address, data_size + metadata_size,
      &create_buffer, ...));

  // 构建 chunk 划分
  auto chunks = BuildChunks(object_id, create_buffer.data, data_size, ...);

  // 记录状态
  create_buffer_state_[object_id] = {
      metadata_size, data_size, chunks,
      chunk_states(全部 AVAILABLE),
      num_seals_remaining_ = chunks.size()
  };
}
```

### WriteChunk — 写入数据，最后一个 chunk 触发 Seal

```cpp
// object_buffer_pool.cc:130-182
void ObjectBufferPool::WriteChunk(const ObjectID &object_id,
                                  uint64_t data_size, uint64_t metadata_size,
                                  const uint64_t chunk_index,
                                  const std::string &data) {
  // Phase 1 (locked): 获取 chunk 信息，标记 REFERENCED→SEALED，inflight_copies++
  {
    absl::MutexLock lock(&pool_mutex_);
    auto &it = create_buffer_state_.find(object_id);
    // ...
    it->second.chunk_state_.at(chunk_index) = CreateChunkState::SEALED;
    it->second.num_inflight_copies_++;
  }

  // Phase 2 (unlocked): memcpy 数据到共享内存
  std::memcpy(chunk_info->data_, data.data(), chunk_info->buffer_length_);

  // Phase 3 (locked): inflight_copies--，seals_remaining--
  {
    absl::MutexLock lock(&pool_mutex_);
    auto it = create_buffer_state_.find(object_id);
    it->second.num_inflight_copies_--;
    it->second.num_seals_remaining_--;

    if (it->second.num_seals_remaining_ == 0) {
      // 所有 chunk 写完 → Seal + Release
      RAY_CHECK_OK(store_client_->Seal(object_id));
      RAY_CHECK_OK(store_client_->Release(object_id));
      create_buffer_state_.erase(it);
    }
  }
}
```

**关键**：`store_client_->Seal()` 触发 `PlasmaStore::SealObjects()` → `add_object_callback_()`。

### AbortCreate — 放弃创建

```cpp
// object_buffer_pool.cc:185-210
void ObjectBufferPool::AbortCreateInternal(const ObjectID &object_id) {
  // 等待所有 inflight memcpy 完成
  pool_mutex_.Await(absl::Condition(&no_copy_inflight));

  auto it = create_buffer_state_.find(object_id);
  if (it != create_buffer_state_.end()) {
    RAY_CHECK_OK(store_client_->Release(object_id));
    RAY_CHECK_OK(store_client_->Abort(object_id));
    create_buffer_state_.erase(object_id);
  }
}
```

`store_client_->Abort()` 触发 `PlasmaStore::AbortObject()` → `ObjectLifecycleManager::AbortObject()` → `DeleteObjectInternal()` → **不触发** `delete_object_callback_`（因为 `aborted=true`）。

---

## 6. PullManager — 对象拉取调度

### Bundle 优先级

```cpp
// src/ray/object_manager/pull_manager.h:39-43
enum BundlePriority : uint8_t {
  GET_REQUEST,    // 最高优先 — ray.get() 触发
  WAIT_REQUEST,   // 中优先 — ray.wait() 触发
  TASK_ARGS,      // 最低优先 — 任务参数依赖
};
```

### 关键数据结构

```cpp
// pull_manager.h
struct ObjectPullRequest {
  std::vector<NodeID> client_locations;    // 哪些远程节点有该 object
  std::string spilled_url;                 // 外部存储 URL
  NodeID spilled_node_id;                  // 哪个节点 spill 了该 object
  bool pending_object_creation;            // 是否正在重建
  double next_pull_time;                   // 下次 pull 重试时间
  double expiration_time_seconds;           // 超时时间
  uint8_t num_retries;                     // 重试次数
  bool object_size_set;                    // 对象大小是否已知
  size_t object_size;
  absl::flat_hash_set<uint64_t> bundle_request_ids;  // 哪些 bundle 需要该 object
};

absl::flat_hash_map<ObjectID, ObjectPullRequest> object_pull_requests_;

absl::flat_hash_map<ObjectID, absl::flat_hash_set<uint64_t>>
    active_object_pull_requests_;  // 活跃的 pull 请求

// 三个优先级队列
BundlePullRequestQueue get_request_bundles_;
BundlePullRequestQueue wait_request_bundles_;
BundlePullRequestQueue task_argument_bundles_;
```

### Pull 发起流程

```cpp
// pull_manager.cc:74-133
uint64_t PullManager::Pull(
    const std::vector<rpc::ObjectReference> &object_ref_bundle,
    BundlePriority prio, const TaskMetricsKey &task_key,
    std::vector<rpc::ObjectReference> *objects_to_locate) {
  const uint64_t req_id = next_req_id_++;

  // 1. 创建 BundlePullRequest
  // 2. 对每个新 object，创建 ObjectPullRequest，加入 objects_to_locate
  // 3. 对已知位置的 object，标记 pullable
  // 4. 按优先级加入对应队列
  // 5. 根据可用内存激活 pull
  UpdatePullsBasedOnAvailableMemory(num_bytes_available_);
  return req_id;
}
```

### 对象定位

```cpp
// object_manager.cc:225-252
uint64_t ObjectManager::Pull(...) {
  std::vector<rpc::ObjectReference> objects_to_locate;
  auto request_id = pull_manager_->Pull(object_refs, prio, task_key, &objects_to_locate);

  // 订阅 ObjectDirectory 获取位置变化
  const auto &callback = [this](const ObjectID &object_id,
      const std::unordered_set<NodeID> &client_ids, ...) {
    pull_manager_->OnLocationChange(object_id, client_ids, ...);
  };

  for (const auto &ref : objects_to_locate) {
    object_directory_->SubscribeObjectLocations(
        object_directory_pull_callback_id_, object_id, ref.owner_address(), callback);
  }
  return request_id;
}
```

`OnLocationChange` 更新 `ObjectPullRequest` 的 `client_locations`、`spilled_url` 等，如果可拉取性变化则更新 bundle 状态。

### Pull 请求调度

```cpp
// pull_manager.cc:348-397
void PullManager::TryToMakeObjectLocal(const ObjectID &object_id) {
  // 1. 已在本地？ → 跳过
  // 2. 不再活跃？ → 跳过
  // 3. 重试时间未到？ → 跳过
  // 4. 从已知位置随机选一个节点，发 Pull 请求
  PullFromRandomLocation(object_id);
  // 5. 无远程节点？ → 尝试从外部存储恢复
  // 6. 完全无位置？ → 设置超时定时器 (OBJECT_FETCH_TIMED_OUT)
}
```

### 内存优先级调度

```cpp
// pull_manager.cc:199-258
void PullManager::UpdatePullsBasedOnAvailableMemory(int64_t num_bytes_available) {
  // 激活优先级从高到低：
  // 1. GET_REQUEST — 无条件激活（即使超配额）
  // 2. WAIT_REQUEST — 遵守配额
  // 3. TASK_ARGS — 遵守配额
  // 如果超出内存配额，从低优先级队列尾部取消激活
}
```

### IsObjectActive

```cpp
bool PullManager::IsObjectActive(const ObjectID &object_id) {
  absl::MutexLock lock(&active_objects_mu_);
  return active_object_pull_requests_.contains(object_id);
}
```

此方法被 `ReceiveReplicationPushChunk` 和 `ReceivePullChunk` 调用来判断 pull 是否活跃。

---

## 7. PushManager — 对象推送管理

### 数据结构

```cpp
// src/ray/object_manager/push_manager.h:61-86
struct PushState {
  NodeID node_id_;
  ObjectID object_id_;
  int64_t num_chunks_;
  std::function<void(int64_t)> chunk_send_fn_;
  int64_t next_chunk_id_ = 0;
  int64_t num_chunks_to_send_;
};

// 节点 → 对象 → PushState 迭代器
absl::flat_hash_map<NodeID, flat_hash_map<ObjectID, list<PushState>::iterator>>
    push_state_map_;

int64_t chunks_in_flight_ = 0;     // 当前在途 chunk 数
int64_t max_chunks_in_flight_;     // 最大在途 chunk 限流
int64_t chunks_remaining_ = 0;     // 总剩余 chunk 数
```

### StartPush

```cpp
// push_manager.cc:19-42
void PushManager::StartPush(const NodeID &dest_id, const ObjectID &obj_id,
                            int64_t num_chunks,
                            std::function<void(int64_t)> send_chunk_fn) {
  auto &dest_map = push_state_map_[dest_id];
  auto it = dest_map.find(obj_id);
  if (it == dest_map.end()) {
    // 新 push
    dest_map[obj_id] = push_requests_with_chunks_to_send_.emplace(...);
  } else {
    // 重复 push（比如之前的 push 超时重试）：重新发送所有 chunk
    chunks_remaining_ += it->second->ResendAllChunks(std::move(send_chunk_fn));
  }
  ScheduleRemainingPushes();
}
```

### ScheduleRemainingPushes — 轮询调度

```cpp
// push_manager.cc:50-80
void PushManager::ScheduleRemainingPushes() {
  // 轮询遍历所有 push 请求，每次发一个 chunk
  // 遵守 max_chunks_in_flight_ 限流
  while (chunks_in_flight_ < max_chunks_in_flight_ && chunks_remaining_ > 0) {
    auto &push_state = push_requests_with_chunks_to_send_.front();
    push_state.chunk_send_fn_(push_state.next_chunk_id_++);
    chunks_in_flight_++;
    push_state.num_chunks_to_send_--;
    chunks_remaining_--;

    if (push_state.num_chunks_to_send_ == 0) {
      // 该 push 完成
      push_requests_with_chunks_to_send_.pop_front();
    } else {
      // 轮询：移到队尾
      push_requests_with_chunks_to_send_.splice(
          push_requests_with_chunks_to_send_.end(),
          push_requests_with_chunks_to_send_,
          push_requests_with_chunks_to_send_.begin());
    }
  }
}
```

### OnChunkComplete

```cpp
// push_manager.cc:44-48
void PushManager::OnChunkComplete(int64_t chunk_size) {
  chunks_in_flight_--;
  chunks_remaining_--;
  ScheduleRemainingPushes();
}
```

---

## 8. 完整链路 1：Worker 本地创建对象

```
Worker                           PlasmaStore                    ObjectLifecycleManager
  │                                   │                                    │
  │── PlasmaCreateRequest ──────────>│                                    │
  │                                   │── CreateObject() ────────────────>│
  │                                   │                                   │── CreateObjectInternal()
  │                                   │                                   │   (分配内存，必要时淘汰)
  │                                   │                                   │<── LocalObject*
  │                                   │<── PlasmaObject ──────────────────│
  │<── CreateReply ──────────────────│                                    │
  │                                   │                                    │
  [Worker 通过 mmap 直接写入共享内存]  │                                    │
  │                                   │                                    │
  │── PlasmaSealRequest ────────────>│                                    │
  │                                   │── SealObject() ─────────────────>│
  │                                   │                                   │── SealObject()
  │                                   │                                   │   (PLASMA_CREATED→SEALED)
  │                                   │<── entry* ───────────────────────│
  │                                   │── add_object_callback_() ────────>│ [posted to main_service]
  │<── SealReply ────────────────────│                                    │
                                       │                                    │
                                       v                                    v
                                ObjectManager                      NodeManager
                                HandleObjectAdded()                 HandleObjectLocal()
                                - local_objects_[id] = info        - lease_dependency_manager_
                                - used_memory_ += size             - wait_manager_
                                - ReportObjectAdded (GCS)          - 通知等待 worker
                                - PinNewObjectIfNeeded             - MaybeReplicateObject
                                - unfulfilled_push_requests_       - SpillIfOverThreshold
```

### HandleObjectAdded 详解

```cpp
// src/ray/object_manager/object_manager.cc:181-209
void ObjectManager::HandleObjectAdded(const ObjectInfo &object_info) {
  const ObjectID &object_id = object_info.object_id;
  local_objects_[object_id].object_info = object_info;
  used_memory_ += object_info.data_size + object_info.metadata_size;

  // 通知 ObjectDirectory（其他节点可查询到该对象在本节点）
  object_directory_->ReportObjectAdded(object_id, self_node_id_, object_info);

  // 如果有活跃 pull 正在拉该对象，pin 住防止被淘汰
  pull_manager_->PinNewObjectIfNeeded(object_id);

  // 处理未满足的 push 请求（之前 push 时对象不在本地，现在可以推送了）
  auto iter = unfulfilled_push_requests_.find(object_id);
  if (iter != unfulfilled_push_requests_.end()) {
    for (auto &pair : iter->second) {
      auto &node_id = pair.first;
      main_service_->post([this, object_id, node_id]() {
        Push(object_id, node_id);
      }, "ObjectManager.ObjectAddedPush");
    }
    unfulfilled_push_requests_.erase(iter);
  }
}
```

### HandleObjectLocal 详解

```cpp
// src/ray/raylet/node_manager.cc:2418-2458
void NodeManager::HandleObjectLocal(const ObjectInfo &object_info,
                                    plasma::flatbuf::ObjectSource source) {
  const ObjectID &object_id = object_info.object_id;

  // 1. 通知 lease dependency manager → 可能唤醒等待的 lease
  const auto ready_lease_ids = lease_dependency_manager_.HandleObjectLocal(object_id);
  local_lease_manager_.LeasesUnblocked(ready_lease_ids);

  // 2. 通知 wait manager
  wait_manager_.HandleObjectLocal(object_id);

  // 3. 通知等待该对象的 worker
  auto waiting_workers = async_plasma_objects_notification_.extract(object_id);
  for (const auto &worker : waiting_workers) {
    worker->rpc_client()->PlasmaObjectReady(request, ...);
  }

  // 4. 判断是否是 replication push → 统计
  if (source == plasma::flatbuf::ObjectSource::ReceivedByPush &&
      object_manager_.ConsumeReplicationPushReceived(object_id)) {
    object_replication_succeeded_.Record(1);
  }

  // 5. 触发可能的对象复制
  MaybeReplicateObject(object_info, source);

  // 6. 检查是否需要 spill
  SpillIfOverPrimaryObjectsThreshold();
}
```

---

## 9. 完整链路 2：Pull 远程对象

```
LeaseDependencyManager          PullManager           ObjectDirectory         Remote Node
       │                              │                       │                    │
  RequestLeaseDependencies()         │                       │                    │
       │── object_manager_.Pull() ──>│                       │                    │
       │                              │── SubscribeObjLoc ──>│                    │
       │                              │                       │── 查询 GCS ────────│
       │                              │<── OnLocationChange ──│                    │
       │                              │   (client_locations)  │                    │
       │                              │                       │                    │
       │                              │── SendPullRequest ─────────────────────────>│
       │                              │                       │                    │
       │                              │                       │     HandlePull()  │
       │                              │                       │     └── Push()    │
       │                              │                       │     └── PushLocalObject()
       │                              │                       │     └── PushObjectInternal()
       │                              │                       │     └── push_manager_->StartPush()
       │                              │                       │                    │
       │                              │<──── RPC PushRequest (chunk by chunk) ─────│
       │                              │                       │                    │
       │                      HandlePush()                     │                    │
       │                      ReceivePullChunk()              │                    │
       │                       ├── IsObjectActive?            │                    │
       │                       ├── buffer_pool_.CreateChunk() │                    │
       │                       │   └── EnsureBufferExists()   │                    │
       │                       │       └── store_client_->CreateAndSpillIfNeeded()
       │                       │           └── PlasmaStore::CreateObject()         │
       │                       │               └── object_lifecycle_mgr_.CreateObject()
       │                       │                   └── 分配内存(PLASMA_CREATED)      │
       │                       ├── buffer_pool_.WriteChunk() │                    │
       │                       │   └── memcpy + Seal on last chunk                 │
       │                       │       └── store_client_->Seal()                   │
       │                       │           └── PlasmaStore::SealObjects()           │
       │                       │               └── add_object_callback_() ──────>  │
       │                              │                       │                    │
       │                       ┌──────┴──────┐               │                    │
       │                       ▼             ▼               │                    │
       │                HandleObjectAdded  HandleObjectLocal  │                    │
       │                              │   lease_dep_mgr_.HandleObjectLocal()       │
       │                              │   └── LeasesUnblocked()                     │
       │                              │       └── lease 进入 grant 队列               │
```

---

## 10. 完整链路 3：Replication Push 接收对象

```
NodeManager                     Remote Node
HandleObjectLocal()                    │
  └── MaybeReplicateObject()           │
       └── PushForReplication() ──────>│
            (is_replication=true)       │
                                        │── PushLocalObject(is_replication=true)
                                        │── PushObjectInternal()
                                        │── push_manager_->StartPush()
                                        │
Local Node                              │
  HandlePush()                          │
  ├── is_replication_push=true          │
  └── ReceiveReplicationPushChunk() <───│ (chunk by chunk)
       │
       ├── 检查 replication_push_terminated_objects_ (已终止？)
       ├── 检查 has_active_pull (pull 是否活跃？)
       ├── 检查 object_already_local (对象已在本地？)
       │
       ├── [rejected] → 标记 terminated，后续 chunk 全部拒绝
       │
       └── [accepted]
            ├── buffer_pool_.CreateChunk(source=ReceivedByPush)
            │   └── EnsureBufferExists()
            │       └── store_client_->CreateAndSpillIfNeeded()
            │           └── PlasmaStore::CreateObject()
            │               └── PLASMA_CREATED
            │
            ├── buffer_pool_.WriteChunk() → memcpy
            │
            └── last chunk: num_seals_remaining_==0
                ├── store_client_->Seal() → PlasmaStore::SealObjects()
                │   └── add_object_callback_()
                │       ├── HandleObjectAdded()
                │       └── HandleObjectLocal()
                │           ├── source == ReceivedByPush
                │           ├── ConsumeReplicationPushReceived() → 统计
                │           ├── lease_dependency_manager_.HandleObjectLocal()
                │           └── MaybeReplicateObject()
                │
                └── store_client_->Release()
```

### ReceiveReplicationPushChunk 竞争保护

```cpp
// object_manager.cc:696-784
bool ObjectManager::ReceiveReplicationPushChunk(...) {
  const bool has_active_pull = pull_manager_->IsObjectActive(object_id);
  const bool object_already_local = local_objects_.count(object_id) > 0;

  {
    absl::MutexLock lock(&replication_push_state_mu_);
    // 已终止 → 拒绝后续 chunk
    if (replication_push_terminated_objects_.contains(object_id)) {
      return false;
    }
    // pull 活跃或对象已存在 → 拒绝 push
    if (has_active_pull || object_already_local) {
      replication_push_terminated_objects_.emplace(object_id, now_ms);
      replication_push_objects_.erase(object_id);
      return false;
    }
  }

  // 尝试创建 chunk
  auto chunk_status = buffer_pool_.CreateChunk(
      object_id, owner_address, data_size, metadata_size, chunk_index,
      plasma::flatbuf::ObjectSource::ReceivedByPush);

  if (chunk_status.ok()) {
    buffer_pool_.WriteChunk(...);
    {
      absl::MutexLock lock(&replication_push_state_mu_);
      replication_push_objects_[object_id] = now_ms;  // 标记为 push 接收中
    }
    return true;
  } else {
    // CreateChunk 失败 → 再次检查 pull 是否在此期间变活跃
    const bool has_active_pull_after_create = pull_manager_->IsObjectActive(object_id);
    {
      absl::MutexLock lock(&replication_push_state_mu_);
      replication_push_terminated_objects_.emplace(object_id, now_ms);
      if (has_active_pull_after_create) {
        replication_push_objects_.erase(object_id);
      }
    }
    if (!has_active_pull_after_create) {
      buffer_pool_.AbortCreate(object_id);
    }
    return false;
  }
}
```

### TTL 清理

```cpp
// object_manager.cc:1001-1046
void ObjectManager::CleanupExpiredReplicationPushState() {
  const int64_t ttl_ms = RayConfig::instance().object_replication_state_ttl_ms();
  // 清理过期的 replication_push_objects_ 和 replication_push_terminated_objects_
  // 对过期的 push_objects_（未 seal 的对象），调用 buffer_pool_.AbortCreate() 防止泄漏
}
```

---

## 11. 完整链路 4：对象淘汰/删除

### 淘汰触发点

1. **创建新对象时空间不足** — `CreateObjectInternal` → `eviction_policy_->RequireSpace()` → `EvictObjects()`
2. **显式删除** — `PlasmaStore::ProcessClientMessage(PlasmaDeleteRequest)` → `object_lifecycle_mgr_.DeleteObject()`
3. **引用归零后的延迟删除** — `RemoveReference()` → ref_count==0 且在 `earger_deletion_objects_` 中 → `DeleteObjectInternal()`

### 淘汰前提条件

- 对象必须是 `PLASMA_SEALED` 状态
- 对象的 `ref_count_` 必须为 0（无客户端在使用）

### 淘汰流程

```
CreateObjectInternal (空间不足)
    │
    ├── eviction_policy_->RequireSpace(size)
    │   └── 返回需要淘汰的 object_ids 列表
    │
    └── EvictObjects(object_ids)
        │
        └── 对每个 object_id:
            ├── 检查 PLASMA_SEALED + ref_count_==0
            └── DeleteObjectInternal(object_id)
                ├── stats_collector_->OnObjectDeleting()
                ├── earger_deletion_objects_.erase()
                ├── eviction_policy_->RemoveObject()
                ├── object_store_->DeleteObject()  (释放内存)
                └── delete_object_callback_(object_id)  ← 触发！
                    │
                    └── main_service.post →
                        ├── object_manager->HandleObjectDeleted()
                        │   ├── local_objects_.erase()
                        │   ├── used_memory_ -= size
                        │   ├── object_directory_->ReportObjectRemoved()
                        │   ├── replication_push_objects_.erase()
                        │   ├── replication_push_terminated_objects_.erase()
                        │   └── pull_manager_->ResetRetryTimer()
                        │
                        └── node_manager->HandleObjectMissing()
                            └── (通知依赖管理器等)
```

### Abort 不触发 delete callback

```
AbortObject (未 seal 的对象)
    │
    └── DeleteObjectInternal()
        ├── aborted = (state_ == PLASMA_CREATED)  → true
        ├── stats_collector_->OnObjectDeleting()
        ├── earger_deletion_objects_.erase()
        ├── eviction_policy_->RemoveObject()
        ├── object_store_->DeleteObject()
        └── if (!aborted) { ... }  → 不执行！
            → delete_object_callback_ 不触发
```

这是合理的：未 seal 的对象从未被上层系统感知（`add_object_callback_` 未触发过），因此删除它时也不需要通知上层。

---

## 12. Callback 触发汇总表

### add_object_callback_

| 触发场景 | 调用位置 | 文件:行 |
|----------|----------|---------|
| Worker 本地创建 + Seal | `SealObjects()` | `store.cc:275` |
| Pull 远程对象最后一个 chunk 写完 | `WriteChunk()` → `Seal()` → `SealObjects()` | `buffer_pool.cc:176` → `store.cc:275` |
| Replication Push 最后一个 chunk 写完 | `WriteChunk()` → `Seal()` → `SealObjects()` | `buffer_pool.cc:176` → `store.cc:275` |

**共同点**：不管对象来源如何，只要 `SealObjects()` 被调用，就触发 `add_object_callback_`。

### delete_object_callback_

| 触发场景 | 调用链 | `aborted` |
|----------|--------|-----------|
| LRU 淘汰（sealed + ref_count==0） | `EvictObjects` → `DeleteObjectInternal` | `false` ✅ 触发 |
| 显式删除（sealed + ref_count==0） | `DeleteObject` → `DeleteObjectInternal` | `false` ✅ 触发 |
| 引用归零延迟删除 | `RemoveReference` → `DeleteObjectInternal` | `false` ✅ 触发 |
| Abort（未 seal） | `AbortObject` → `DeleteObjectInternal` | `true` ❌ 不触发 |

---

## 13. 关键代码索引

| 组件 | 文件 |
|------|------|
| PlasmaStore | `src/ray/object_manager/plasma/store.cc` / `.h` |
| ObjectLifecycleManager | `src/ray/object_manager/plasma/obj_lifecycle_mgr.cc` / `.h` |
| ObjectStore (IObjectStore) | `src/ray/object_manager/plasma/object_store.cc` / `.h` |
| EvictionPolicy | `src/ray/object_manager/plasma/eviction_policy.cc` / `.h` |
| LocalObject | `src/ray/object_manager/plasma/common.h` |
| ObjectBufferPool | `src/ray/object_manager/object_buffer_pool.cc` / `.h` |
| PullManager | `src/ray/object_manager/pull_manager.cc` / `.h` |
| PushManager | `src/ray/object_manager/push_manager.cc` / `.h` |
| ObjectManager | `src/ray/object_manager/object_manager.cc` / `.h` |
| Callback 绑定 | `src/ray/raylet/main.cc:800-816` |
| HandleObjectLocal | `src/ray/raylet/node_manager.cc:2418` |
| HandleObjectAdded | `src/ray/object_manager/object_manager.cc:181` |
| HandleObjectDeleted | `src/ray/object_manager/object_manager.cc:206` |
| HandleObjectMissing | `src/ray/raylet/node_manager.cc:2462` |

### 关键方法快速定位

| 方法 | 文件 | 大致行号 |
|------|------|----------|
| `PlasmaStore::CreateObject` | `store.cc` | ~181 |
| `PlasmaStore::SealObjects` | `store.cc` | ~274 |
| `PlasmaStore::AbortObject` | `store.cc` | ~285 |
| `ObjectLifecycleManager::CreateObject` | `obj_lifecycle_mgr.cc` | ~39 |
| `ObjectLifecycleManager::CreateObjectInternal` | `obj_lifecycle_mgr.cc` | ~170 |
| `ObjectLifecycleManager::SealObject` | `obj_lifecycle_mgr.cc` | ~56 |
| `ObjectLifecycleManager::AbortObject` | `obj_lifecycle_mgr.cc` | ~65 |
| `ObjectLifecycleManager::DeleteObject` | `obj_lifecycle_mgr.cc` | ~84 |
| `ObjectLifecycleManager::DeleteObjectInternal` | `obj_lifecycle_mgr.cc` | ~198 |
| `ObjectLifecycleManager::EvictObjects` | `obj_lifecycle_mgr.cc` | ~222 |
| `ObjectLifecycleManager::AddReference` | `obj_lifecycle_mgr.cc` | ~131 |
| `ObjectLifecycleManager::RemoveReference` | `obj_lifecycle_mgr.cc` | ~149 |
| `ObjectBufferPool::CreateChunk` | `object_buffer_pool.cc` | ~110 |
| `ObjectBufferPool::EnsureBufferExists` | `object_buffer_pool.cc` | ~247 |
| `ObjectBufferPool::WriteChunk` | `object_buffer_pool.cc` | ~130 |
| `ObjectBufferPool::AbortCreate` | `object_buffer_pool.cc` | ~185 |
| `PullManager::Pull` | `pull_manager.cc` | ~74 |
| `PullManager::OnLocationChange` | `pull_manager.cc` | ~282 |
| `PullManager::TryToMakeObjectLocal` | `pull_manager.cc` | ~348 |
| `PullManager::UpdatePullsBasedOnAvailableMemory` | `pull_manager.cc` | ~199 |
| `PushManager::StartPush` | `push_manager.cc` | ~19 |
| `PushManager::ScheduleRemainingPushes` | `push_manager.cc` | ~50 |
| `PushManager::OnChunkComplete` | `push_manager.cc` | ~44 |
| `ObjectManager::Pull` | `object_manager.cc` | ~225 |
| `ObjectManager::Push` | `object_manager.cc` | ~349 |
| `ObjectManager::PushForReplication` | `object_manager.cc` | ~397 |
| `ObjectManager::HandlePush` | `object_manager.cc` | ~612 |
| `ObjectManager::ReceivePullChunk` | `object_manager.cc` | ~640 |
| `ObjectManager::ReceiveReplicationPushChunk` | `object_manager.cc` | ~696 |
| `ObjectManager::HandleObjectAdded` | `object_manager.cc` | ~181 |
| `ObjectManager::HandleObjectDeleted` | `object_manager.cc` | ~206 |
| `ObjectManager::CleanupExpiredReplicationPushState` | `object_manager.cc` | ~1001 |
