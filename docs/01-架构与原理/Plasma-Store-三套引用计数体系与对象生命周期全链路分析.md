# Plasma Store 三套引用计数体系与对象生命周期全链路分析

本文档详细分析 Ray Plasma Store 中三套引用计数（owner ref、client count、server ref_count）的完整工作机制，以及对象在 Create、Put、Push、Pull、ReceivedByPush 等路径下的 ref 变化全链路，含相关代码位置。

---

## 目录

- [1. 三套计数体系总览](#1-三套计数体系总览)
- [2. Client Count 详解](#2-client-count-详解)
- [3. Server ref_count 详解](#3-server-ref_count-详解)
- [4. Owner Ref 详解](#4-owner-ref-详解)
- [5. 三套计数的联动关系](#5-三套计数的联动关系)
- [6. 完整链路 1：Put（owner 本地小对象）](#6-完整链路-1putowner-本地小对象)
- [7. 完整链路 2：SealExisting（task 执行端写返回值）](#7-完整链路-2sealexistingtask-执行端写返回值)
- [8. 完整链路 3：PinExistingReturnObject（plasma 中已有对象）](#8-完整链路-3pinexistingreturnobjectplasma-中已有对象)
- [9. 完整链路 4：Pull（worker 主动拉取远程对象）](#9-完整链路-4pullworker-主动拉取远程对象)
- [10. 完整链路 5：ReceivedByPush（replication push 接收端）—— 问题所在](#10-完整链路-5receivedbypushreplication-push-接收端--问题所在)
- [11. 完整链路 6：Owner Unpin（owner out of scope）](#11-完整链路-6owner-unpinowner-out-of-scope)
- [12. Pull Pin vs Primary Pin 生命周期对比](#12-pull-pin-vs-primary-pin-生命周期对比)
- [13. 汇总对比表](#13-汇总对比表)
- [14. 关键代码索引](#14-关键代码索引)
- [15. Object Location 上报机制](#15-object-location-上报机制)
- [16. WorkerObjectEviction Pub/Sub 完整机制](#16-workerobjecteviction-pubsub-完整机制)
- [17. WorkerObjectLocations Pub/Sub 机制](#17-workerobjectlocations-pubsub-机制)
- [18. Spill 流程详解](#18-spill-流程详解)
- [19. Spill 恢复流程详解](#19-spill-恢复流程详解)
- [20. 关键代码索引（补充）](#20-关键代码索引补充)

---

## 1. 三套计数体系总览

| 计数 | 存储位置 | 含义 | 决定什么 |
|------|---------|------|---------|
| **owner ref** | `reference_counter.cc` → `object_id_refs_[id]` | 对象语义生命周期（谁 owns、是否 out of scope、pin 在哪个节点） | 何时发布 WorkerObjectEviction |
| **client count** | `client.cc` → `objects_in_use_[id].count` | "这个 client 进程还要不要持有 mmap 映射" | 何时发 ReleaseRequest 给 server（count=0 时） |
| **server ref_count** | `common.h:181` → `LocalObject.ref_count_` | "有多少个 client 在用此对象" | 是否可被 LRU 淘汰（=0 时加入 LRU） |

**核心规则**：
- server ref_count > 0 → `BeginObjectAccess`（从 LRU 移除）→ 不可淘汰
- server ref_count == 0 → `EndObjectAccess`（加入 LRU）→ 可淘汰
- LRU 淘汰只看 server ref_count，不看 client count 也不看 owner ref

---

## 2. Client Count 详解

### 2.1 存储结构

```cpp
// client.h:339
struct ObjectInUseEntry {
    int count = 0;       // Create/Get 次数 - Release 次数
    PlasmaObject object; // 客户端持有的 buffer 引用
    bool is_sealed;      // 是否已 seal
};

// client.h:323
absl::flat_hash_map<ObjectID, std::unique_ptr<ObjectInUseEntry>> objects_in_use_;
```

### 2.2 Client count 变更操作

#### InsertObjectInUse（count 0→1）

```cpp
// client.cc:110
void PlasmaClient::InsertObjectInUse(const ObjectID &object_id,
                                     std::unique_ptr<PlasmaObject> object,
                                     bool is_sealed) {
  auto inserted =
      objects_in_use_.insert({object_id, std::make_unique<ObjectInUseEntry>()});
  RAY_CHECK(inserted.second) << "Object already in use";
  auto it = inserted.first;
  it->second->object = std::move(*object);
  it->second->count = 1;  // ← count 初始为 1
  it->second->is_sealed = is_sealed;
}
```

**唯一调用者**：`client.cc:188`（Create 路径中 `HandleCreateReply` 后）

#### IncrementObjectCount（count +1）

```cpp
// client.cc:126
void PlasmaClient::IncrementObjectCount(const ObjectID &object_id) {
  auto object_entry = objects_in_use_.find(object_id);
  RAY_CHECK(object_entry != objects_in_use_.end());
  object_entry->second->count += 1;
}
```

**唯一调用者**：`client.cc:193`（Create 路径，Seal 前保护，count 1→2）

#### Release（count -1，count==0 时通知 server）

```cpp
// client.cc:490
Status PlasmaClient::Release(const ObjectID &object_id) {
  std::lock_guard<std::recursive_mutex> guard(client_mutex_);
  const auto object_entry = objects_in_use_.find(object_id);
  RAY_CHECK(object_entry != objects_in_use_.end());

  object_entry->second->count -= 1;  // ← count 减 1
  RAY_CHECK_GE(object_entry->second->count, 0);

  if (object_entry->second->count == 0) {  // ← 只有 count==0 才通知 server
    // MarkObjectUnused: 从 objects_in_use_ 移除，释放 mmap 映射
    RAY_RETURN_NOT_OK(MarkObjectUnused(object_id));
    // SendReleaseRequest: 通知 plasma server ref_count-1
    RAY_RETURN_NOT_OK(SendReleaseRequest(store_conn_, object_id, may_unmap));
    // ... (处理 may_unmap 和 deletion_cache)
  }
  return Status::OK();
}
```

**关键**：count > 0 的 Release 是**静默的**，不发 ReleaseRequest，server 不知道。只有 count==0 时才 `MarkObjectUnused`（释放 mmap）+ `SendReleaseRequest`（通知 server）。

#### MarkObjectUnused（从 objects_in_use_ 移除）

```cpp
// client.cc:480
Status PlasmaClient::MarkObjectUnused(const ObjectID &object_id) {
  auto object_entry = objects_in_use_.find(object_id);
  RAY_CHECK(object_entry != objects_in_use_.end());
  RAY_CHECK_EQ(object_entry->second->count, 0);
  objects_in_use_.erase(object_id);  // ← 只是移除条目，不释放 plasma 对象
  return Status::OK();
}
```

**注意**：`MarkObjectUnused` 只是客户端清理自己的 `objects_in_use_` 表，对象在 plasma store 服务端还活着（ref_count 可能 > 0）。真正释放对象是服务端 ref_count=0 → LRU 淘汰时 `DeleteObject`。

### 2.3 Client count 变更与 server ref 的对应关系

| client 操作 | client count 变化 | 是否通知 server | server ref 变化 |
|------------|-------------------|----------------|----------------|
| Create → 加入 | 0→1+1=2 | 是（CreateRequest → AddToClientObjectIds） | +1 |
| Seal 内部 Release | 2→1 | 否（count>0） | 不变 |
| 回调 Release | 1→0 → 退出 | 是（ReleaseRequest → RemoveFromClientObjectIds） | -1 |
| Get → 加入 | 0→1 | 是（GetRequest → AddToClientObjectIds） | +1 |
| Release | 1→0 → 退出 | 是（ReleaseRequest → RemoveFromClientObjectIds） | -1 |

**规律**：client **首次加入**（Create/Get 时出现在 server 的 client 列表中）→ server ref +1；client **退出**（Release 后 count=0，从 server 的 client 列表移除）→ server ref -1。client 内部 count 变化不影响 server ref。

### 2.4 多 Client 并存

每个和 plasma store 建立连接的进程都是一个独立的 client，各自有独立的 client count：

| client | 谁创建的 | 典型场景 |
|--------|---------|---------|
| worker 的 PlasmaClient | core_worker 进程 | Create/Get/Seal/Release |
| raylet 的 PlasmaClient | raylet 进程 | GetObjectsFromPlasma（pin 时） |
| buffer_pool 的 PlasmaClient | object_manager 线程 | CreateChunk/Seal/Release（Push/Pull 传输） |

**server ref_count = 所有 client 引用数之和**。每个 client 最多贡献 1 个 ref（加入时 +1，退出时 -1）。

---

## 3. Server ref_count 详解

### 3.1 存储结构

```cpp
// common.h:114
class LocalObject {
 public:
  explicit LocalObject(Allocation allocation)
      : allocation_(std::move(allocation)), ref_count_(0) {}

  int32_t GetRefCount() const { return ref_count_; }

 private:
  mutable int32_t ref_count_;  // 有多少个 client 在使用此对象
};
```

### 3.2 AddReference（server ref +1）

```cpp
// obj_lifecycle_mgr.cc:128
bool ObjectLifecycleManager::AddReference(const ObjectID &object_id) {
  auto entry = object_store_->GetObject(object_id);
  if (!entry) { return false; }
  if (entry->ref_count_ == 0) {
    // ref_count 从 0→1: 告诉淘汰策略"对象正在使用"
    eviction_policy_->BeginObjectAccess(object_id);  // 从 LRU 移除，不可淘汰
  }
  entry->ref_count_++;
  stats_collector_->OnObjectRefIncreased(*entry);
  return true;
}
```

**唯一调用者**：`store.cc:144 AddToClientObjectIds → AddReference`

触发场景：
1. **Create 请求**：`store.cc:149 HandleCreateObjectRequest` → `store.cc:176 CreateObject` → `store.cc:191 AddToClientObjectIds` → `AddReference`
2. **Get 请求**：`store.cc:428 PlasmaGetRequest` → `ProcessGetRequest` → Get 完成回调 `store.cc:108 AddToClientObjectIds` → `AddReference`
3. **Get 队列中对象 sealed**：`store.cc:284 MarkObjectSealed` → 遍历等待此对象的 GetRequest → `ReturnFromGet` → 为每个 client `AddToClientObjectIds` → `AddReference`

### 3.3 RemoveReference（server ref -1）

```cpp
// obj_lifecycle_mgr.cc:148
bool ObjectLifecycleManager::RemoveReference(const ObjectID &object_id) {
  auto entry = object_store_->GetObject(object_id);
  if (!entry || entry->ref_count_ == 0) { return false; }

  entry->ref_count_--;
  stats_collector_->OnObjectRefDecreased(*entry);

  if (entry->ref_count_ > 0) {
    return true;  // 还有其他 client 在用
  }

  // ref_count == 0: 告诉淘汰策略"对象不再使用"
  eviction_policy_->EndObjectAccess(object_id);  // 加入 LRU，可被淘汰

  RAY_CHECK(entry->Sealed());
  if (earger_deletion_objects_.count(object_id) > 0) {
    DeleteObjectInternal(object_id);  // 立即删除（而非等 LRU）
  }
  return true;
}
```

**唯一调用者**：`store.cc:256 RemoveFromClientObjectIds → RemoveReference`

触发场景：
1. **Release 请求**：`store.cc:434 PlasmaReleaseRequest` → `store.cc:440 ReleaseObject` → `store.cc:265 RemoveFromClientObjectIds` → `RemoveReference`
2. **Client 断连**：`store.cc:478 PlasmaDisconnectClient` → `store.cc:485 DisconnectClient` → 遍历 client 所有 object_ids → 逐个 `RemoveFromClientObjectIds` → `RemoveReference`

### 3.4 LRU 淘汰策略

```cpp
// eviction_policy.cc:105
int64_t EvictionPolicy::ChooseObjectsToEvict(int64_t num_bytes_required,
                                             std::vector<ObjectID> &objects_to_evict) {
  // 只从 LRU cache 中选对象淘汰
  int64_t bytes_evicted = cache_.ChooseObjectsToEvict(num_bytes_required, objects_to_evict);
  for (auto &object_id : objects_to_evict) {
    cache_.Remove(object_id);
  }
  return bytes_evicted;
}

// eviction_policy.cc:136
void EvictionPolicy::BeginObjectAccess(const ObjectID &object_id) {
  cache_.Remove(object_id);             // 从 LRU 移除 → 不可淘汰
  pinned_memory_bytes_ += GetObjectSize(object_id);
}

void EvictionPolicy::EndObjectAccess(const ObjectID &object_id) {
  auto size = GetObjectSize(object_id);
  cache_.Add(object_id, size);          // 加入 LRU → 可被淘汰
  pinned_memory_bytes_ -= size;
}
```

**关键**：`BeginObjectAccess` 在 ref_count 从 0→1 时调用（从 LRU 移除），`EndObjectAccess` 在 ref_count 从 1→0 时调用（加入 LRU）。LRU 只淘汰 ref_count=0 的对象。

---

## 4. Owner Ref 详解

Owner ref 不是数值计数，而是一组语义状态：

```cpp
// reference_counter.h 中 ObjectReference 结构
struct ObjectReference {
  bool owned_by_us_;                         // 是否是 owner
  std::optional<NodeID> pinned_at_node_id_;   // primary pin 在哪个节点
  NodeID spilled_node_id;                    // spill 在哪个节点
  std::optional<NodeID> pin_transferred_from_; // pin 转移来源
  absl::flat_hash_set<NodeID> locations;      // 对象副本在哪些节点
  // OutOfScope() 判断: ref_count==0 且无 borrowed 且无 nested
};
```

### 4.1 设置 pinned_at_node_id_

```cpp
// reference_counter.cc:953
void ReferenceCounter::UpdateObjectPinnedAtRaylet(const ObjectID &object_id,
                                                  const NodeID &node_id,
                                                  bool is_pin_transfer) {
  absl::MutexLock lock(&mutex_);
  auto it = object_id_refs_.find(object_id);
  if (it != object_id_refs_.end()) {
    if (freed_objects_.contains(object_id)) { return; }  // 已释放

    if (!is_node_dead_(node_id)) {
      auto previous_node_id = it->second.pinned_at_node_id_;
      it->second.pinned_at_node_id_ = node_id;  // ← 记录 pin 节点
      if (is_pin_transfer && previous_node_id.has_value()) {
        it->second.pin_transferred_from_ = previous_node_id;
      }
    } else {
      UnsetObjectPrimaryCopy(it);
      objects_to_recover_.push_back(object_id);
    }
    UpdateOwnedObjectCounters(object_id, it->second, /*decrement=*/false);
  }
}
```

**调用者**：
1. `task_manager.cc:572`（owner 收到 task 返回，`in_plasma==true`）
2. `core_worker.cc:1001`（owner 本地 `PutInLocalPlasmaStore`）
3. Pin 转移场景

**注意**：`UpdateObjectPinnedAtRaylet` 只是元数据记录，不涉及 plasma refcount 操作。真正的保护是 raylet 的 `PinObjectsAndWaitForFree` 维持的 server ref_count=1。

### 4.2 节点死亡时的处理

```cpp
// reference_counter.cc:895
void ReferenceCounter::ResetObjectsOnRemovedNode(const NodeID &node_id) {
  for (auto it = object_id_refs_.begin(); it != object_id_refs_.end(); it++) {
    const auto &object_id = it->first;
    auto &ref = it->second;
    const bool has_copy_on_dead_node = ref.locations.contains(node_id);
    const bool primary_on_dead_node =
        ref.pinned_at_node_id_.value_or(NodeID::Nil()) == node_id;
    const bool spilled_on_dead_node = ref.spilled_node_id == node_id;
    const bool protected_by_pin_transfer =
        ref.pin_transferred_from_.has_value() && *ref.pin_transferred_from_ == node_id;

    if (primary_on_dead_node || spilled_on_dead_node) {
      // 需要恢复
      const size_t surviving_locations =
          ref.locations.size() - (has_copy_on_dead_node ? 1 : 0);
      if (surviving_locations > 0) {
        object_on_dead_node_.Record(1, {{"Status", "needs_recovery_has_replica"}});
      } else {
        object_on_dead_node_.Record(1, {{"Status", "needs_recovery_no_replica"}});
      }
      UnsetObjectPrimaryCopy(it);
      if (!ref.OutOfScope(lineage_pinning_enabled_)) {
        objects_to_recover_.push_back(object_id);
      }
    } else if (protected_by_pin_transfer) {
      object_on_dead_node_.Record(1, {{"Status", "protected_by_pin_transfer"}});
      ref.pin_transferred_from_.reset();
    } else if (has_copy_on_dead_node) {
      // 只是副本丢失，不需要恢复
      object_on_dead_node_.Record(1, {{"Status", "secondary_copy_lost"}});
    }
    RemoveObjectLocationInternal(it, node_id);
  }
}
```

### 4.3 OutOfScope → 发布 eviction → 触发 unpin

```
owner reference_counter 判定对象 OutOfScope
  → 发布 WorkerObjectEviction (pub/sub 消息)
  → raylet 收到订阅回调
  → ReleaseFreedObject → 释放 pinned_objects_
  → RayObject 析构 → PlasmaClient::Release
  → [IPC: ReleaseRequest] → server ref-1
```

---

## 5. 三套计数的联动关系

### 5.1 联动总览

```
owner ref (reference_counter)
  │ OutOfScope 时发布 WorkerObjectEviction (pub/sub)
  ↓
raylet 收到订阅回调
  │ ReleaseFreedObject → 释放 RayObject buffer
  │ RayObject 析构 → PlasmaClient::Release (raylet 的 client count)
  ↓
raylet client count 1→0
  │ SendReleaseRequest (IPC)
  ↓
plasma store 收到 ReleaseRequest
  │ RemoveFromClientObjectIds → RemoveReference
  ↓
server ref_count 1→0
  │ EndObjectAccess → 加入 LRU → 可淘汰
```

**方向是单向的**：owner → raylet → server。owner 决定"什么时候允许释放"，raylet 决定"我还要不要继续持有着"，server 决定"实际能不能淘汰"。

**反过来不联动**：server ref=0（对象被 LRU evict）不会通知 owner。owner 的 `reference_counter` 不知道对象已不在 plasma 中，直到需要用时 Get 不到 → 触发 reconstruction。

### 5.2 Owner 不直接操作 plasma store

**Owner 从不经过 PlasmaClient**。owner 不需要 plasma client 连接。owner 只管语义引用计数，发布 "这个对象可以释放了" 的消息。真正持有 plasma client 的是 **raylet**（pin 时 Get 进来的）和 **worker**（Create/Get 进来的）。

---

## 6. 完整链路 1：Put（owner 本地小对象）

**调用链**：`CoreWorker::Put` → `PutInLocalPlasmaStore` → `plasma_store_provider_->Put` → `Create` + `Seal` → `PinObjectIDs`

```
步骤  代码位置                                        client count   server ref   说明
──────────────────────────────────────────────────────────────────────────────────────────
1     client.cc:188 InsertObjectInUse(count=1)         0→1           -           "我在用"
2     client.cc:193 IncrementObjectCount               1→2           -           "Seal前保护"
3     store.cc:176 CreateObject
      → store.cc:191 AddToClientObjectIds
        → obj_lifecycle_mgr.cc:141 AddReference         2             0→1         server ref+1
        → BeginObjectAccess(从LRU移除)

4     plasma_store_provider.cc:118 Seal()
      → client.cc:584 is_sealed = true
      → client.cc:587 SendSealRequest
        → [IPC: SealRequest]
        → store.cc:275 SealObjects → add_object_callback_
      → client.cc:598 Release(count 2→1)                 2→1           1           不发ReleaseRequest

5     core_worker.cc:1008 PinObjectIDs(异步RPC)
      → [RPC: PinObjectIDsRequest 给 raylet]
      → node_manager.cc:2605 HandlePinObjectIDs
        → node_manager.cc:2578 GetObjectsFromPlasma
          → [IPC: GetRequest]
          → store.cc:108 AddToClientObjectIds(raylet作为新client)
            → AddReference                               1             1→2         raylet加入
        → node_manager.cc:2646 local_object_manager_
           .PinObjectsAndWaitForFree
          → local_object_manager.cc:47 local_objects_.emplace
          → local_object_manager.cc:51 pinned_objects_.emplace
          → local_object_manager.cc:68 Subscribe(WorkerObjectEviction)

6     core_worker.cc:1016 回调 Release()
      → client.cc:500 count-=1 (1→0)
      → client.cc:512 count==0:
        → client.cc:514 MarkObjectUnused (从objects_in_use_移除)
        → client.cc:515 SendReleaseRequest
          → [IPC: ReleaseRequest]
          → store.cc:256 RemoveFromClientObjectIds
            → obj_lifecycle_mgr.cc:157 RemoveReference   已移除          2→1
            → ref_count>0, 不做其他事

      ──── 最终稳态 ────
      client: worker已退出, raylet持有一份(count=1, 在raylet的PlasmaClient中)
      server ref: 1 (raylet pin持有) → spillable, 不可LRU淘汰
      owner ref: pinned_at_node_id_=本节点 (后面UpdateObjectPinnedAtRaylet设置)
```

**Owner 侧**（task_manager 处理返回值时）：

```cpp
// task_manager.cc:556
StatusOr<bool> TaskManager::HandleTaskReturn(const ObjectID &object_id,
                                             const rpc::ReturnObject &return_object,
                                             const NodeID &worker_node_id,
                                             bool store_in_plasma) {
  if (return_object.in_plasma()) {
    // 对象已在远端 plasma 中 → 只记录元数据
    reference_counter_.UpdateObjectPinnedAtRaylet(
        object_id, worker_node_id, /*is_pin_transfer=*/false);
    in_memory_store_.Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA), ...);
  } else if (store_in_plasma) {
    // 需要存到本地 plasma → PutInLocalPlasmaStore
    Status s = put_in_local_plasma_callback_(object, object_id);
  } else {
    // 直接内存对象
    in_memory_store_.Put(object, ...);
    direct_return = true;
  }
}
```

---

## 7. 完整链路 2：SealExisting（task 执行端写返回值）

### 7.1 Python 层序列化与调用

```python
# _raylet.pyx:4207 store_task_outputs
# 遍历 task 的每个返回值 output，序列化后调用 store_task_output

# _raylet.pyx:4175 store_task_output
def store_task_output(self, serialized_object, return_id, ...):
    # 步骤A: 尝试在plasma中分配buffer
    AllocateReturnObject(return_id, data_size, metadata, contained_id,
                         caller_address, &task_output_inlined_bytes, return_ptr)

    if return_ptr != NULL:    # 对象不存在于plasma，新分配了buffer
        # 步骤B: 把序列化数据写入plasma buffer
        serialized_object.write_to(Buffer.make(return_ptr.get().GetData()))
        # 步骤C: Seal + Pin
        SealReturnObject(return_id, return_ptr, generator_id, caller_address)
    else:                     # 对象已存在于plasma
        # 步骤D: Pin已有的对象
        PinExistingReturnObject(return_id, return_ptr, generator_id, caller_address)
```

### 7.2 C++ AllocateReturnObject 的大小判断

```cpp
// core_worker.cc:2917
Status CoreWorker::AllocateReturnObject(const ObjectID &object_id, ...) {
  bool object_already_exists = false;
  std::shared_ptr<Buffer> data_buffer;
  if (data_size > 0) {
    if (data_size < max_direct_call_object_size_ &&
        task_output_inlined_bytes + data_size <= task_rpc_inlined_bytes_limit) {
      // 小对象: LocalMemoryBuffer, 不进plasma, 直接通过RPC传回owner
      data_buffer = std::make_shared<LocalMemoryBuffer>(data_size);
    } else {
      // 大对象: CreateExisting → plasma Create
      RAY_RETURN_NOT_OK(CreateExisting(metadata, data_size, object_id,
                                       owner_address, &data_buffer,
                                       /*created_by_worker=*/true));
      object_already_exists = data_buffer == nullptr;  // plasma中已有对象
    }
  }
  if (!object_already_exists) {
    *return_object = std::make_shared<RayObject>(data_buffer, metadata, ...);
  }
  // object_already_exists时 return_ptr = NULL
}
```

### 7.3 SealReturnObject

```cpp
// core_worker.cc:3220
Status CoreWorker::SealReturnObject(const ObjectID &return_id,
                                    const std::shared_ptr<RayObject> &return_object,
                                    const ObjectID &generator_id,
                                    const rpc::Address &owner_address) {
  if (return_object->GetData() != nullptr &&
      return_object->GetData()->IsPlasmaBuffer()) {
    // 大对象在plasma中 → SealExisting
    status = SealExisting(return_id, true, generator_id, owner_address_ptr);
  }
  // 小对象在LocalMemoryBuffer中 → IsPlasmaBuffer()==false → 不做任何plasma操作
  return status;
}
```

### 7.4 SealExisting

```cpp
// core_worker.cc:1198
Status CoreWorker::SealExisting(const ObjectID &object_id,
                                bool pin_object, ...) {
  RAY_RETURN_NOT_OK(plasma_store_provider_->Seal(object_id));
  if (pin_object) {
    local_raylet_rpc_client_->PinObjectIDs(
        owner_address, {object_id}, generator_id,
        [this, object_id](const Status &status, const rpc::PinObjectIDsReply &reply) {
          if (!status.ok()) { return; }
          // 回调Release: 保护转移到raylet后，worker释放自己的引用
          if (!plasma_store_provider_->Release(object_id).ok()) { ... }
        });
  } else {
    RAY_RETURN_NOT_OK(plasma_store_provider_->Release(object_id));
    reference_counter_->FreePlasmaObjects({object_id});
  }
}
```

### 7.5 ref 变化时序

```
步骤  代码位置                                        client count   server ref
────────────────────────────────────────────────────────────────────────────────
1     AllocateReturnObject→Create                       2             1
2     Python write_to (memcpy到plasma buffer)           2             1
3     SealReturnObject→SealExisting
      → Seal: client.cc:598 Release(count 2→1)          1             1
      → PinObjectIDs(异步): raylet Get → AddRef          1             2
      → 回调Release: count 1→0 → SendReleaseRequest      已移除          1

      ──── 最终稳态 ────
      server ref: 1 (raylet pin持有) → spillable
```

---

## 8. 完整链路 3：PinExistingReturnObject（plasma 中已有对象）

**场景**：reconstruction 重执行时，返回值已在 plasma 中存在（raylet 之前 pin 的 ref=1）

```cpp
// core_worker.cc:3294
bool CoreWorker::PinExistingReturnObject(const ObjectID &return_id, ...) {
  // 1. 临时建立引用关系，否则Get不知道owner地址
  reference_counter_->AddLocalReference(return_id, "<temporary>");
  reference_counter_->AddBorrowedObject(return_id, ObjectID::Nil(), owner_address);
  auto owner_addresses = reference_counter_->GetOwnerAddresses({return_id});

  // 2. 非阻塞Get: 从plasma取对象
  Status status = plasma_store_provider_->Get({return_id}, owner_addresses, 0, &result_map);
  // Get → PlasmaClient::Get → InsertObjectInUse(count=1)
  //     → [IPC: GetRequest] → AddToClientObjectIds → AddReference (server ref +1)

  // 3. 释放临时引用（不影响plasma）
  RemoveLocalReference(return_id);

  if (result_map.contains(return_id)) {
    *return_object = std::move(result_map[return_id]);

    // 4. 异步Pin: 让raylet也持有一份
    local_raylet_rpc_client_->PinObjectIDs(
        owner_address, {return_id}, generator_id,
        [return_id, pinned_return_object=*return_object](...) {
          // 回调: pinned_return_object 释放 → worker的RayObject析构
          // → PlasmaClient::Release → count 1→0 → SendReleaseRequest
          // → server ref -1
        });
    return true;
  }
  return false;
}
```

### ref 变化时序

```
步骤  代码位置                                        client count   server ref
────────────────────────────────────────────────────────────────────────────────
1     Get(return_id, timeout=0)                        worker:0→1     原值+1
      → InsertObjectInUse(count=1)
      → [IPC: GetRequest] → AddToClientObjectIds → AddReference

2     PinObjectIDs(异步RPC)                            worker:1       原值+2
      → raylet GetObjectsFromPlasma
        → AddToClientObjectIds → AddReference (raylet加入)

3     回调 → pinned_return_object 释放                  worker:1→0     原值+1
      → RayObject析构 → PlasmaClient::Release
      → count 1→0 → MarkObjectUnused + SendReleaseRequest
      → [IPC: ReleaseRequest] → RemoveReference

      ──── 最终稳态 ────
      server ref = 原值 + 1 (raylet pin新增的)
      如果原值=1(reconstruction前raylet pin的), 最终=1
      (PinObjectsAndWaitForFree发现local_objects_已有, 不重复pin)
```

---

## 9. 完整链路 4：Pull（worker 主动拉取远程对象）

### 9.1 数据接收（Remote Push → 本地 Create + Write + Seal + Release）

```
远端Push数据:
  object_manager.cc:610 HandlePush → ReceivePullChunk
  → buffer_pool.cc:100 CreateChunk
    → buffer_pool.cc:225 EnsureBufferExists
      → store_client_->CreateAndSpillIfNeeded
        → client.cc:188 InsertObjectInUse(count=1)        buffer_pool:2  server:0→1
        → client.cc:193 IncrementObjectCount(count=2)      buffer_pool:2  server:1
        → [IPC: CreateRequest, source=ReceivedByPull]
        → store.cc:191 AddToClientObjectIds → AddReference

  buffer_pool.cc:124 WriteChunk (逐chunk写入)               buffer_pool:2  server:1

  最后一个chunk写完:
  buffer_pool.cc:172 Seal → store_client_->Seal()
    → client.cc:587 SendSealRequest → [IPC: SealRequest]
    → client.cc:598 Release(count 2→1)                      buffer_pool:1  server:1

  buffer_pool.cc:173 Release → store_client_->Release()
    → client.cc:500 count-=1 (1→0)
    → client.cc:512 count==0:
      → MarkObjectUnused + SendReleaseRequest
      → [IPC: ReleaseRequest]
      → store.cc:256 RemoveFromClientObjectIds
        → RemoveReference (1→0)                              buffer_pool:移除  server:0
        → EndObjectAccess → 加入LRU! evictable!
```

### 9.2 add_object_callback_ 触发 PinNewObjectIfNeeded（恢复 server ref）

```
plasma store线程: Seal → add_object_callback_(post到raylet main_service)

main.cc:806-812:
  [&](const ray::ObjectInfo &object_info, plasma::flatbuf::ObjectSource source) {
    main_service.post([&]() {
      object_manager->HandleObjectAdded(object_info);
      node_manager->HandleObjectLocal(object_info, source);
    }, "ObjectManager.ObjectAdded");
  }

HandleObjectAdded:
  → object_manager.cc:149 ReportObjectAdded (通知object_directory)
  → object_manager.cc:154 pull_manager_->PinNewObjectIfNeeded(object_id)

PinNewObjectIfNeeded:
  // pull_manager.cc:586
  void PullManager::PinNewObjectIfNeeded(const ObjectID &object_id) {
    absl::MutexLock lock(&active_objects_mu_);
    bool active = active_object_pull_requests_.count(object_id) > 0;
    if (active) {
      TryPinObject(object_id);
    }
  }

TryPinObject:
  // pull_manager.cc:598
  bool PullManager::TryPinObject(const ObjectID &object_id) {
    if (pinned_objects_.count(object_id) > 0) { return true; }
    auto ref = pin_object_(object_id);  // main.cc:843 回调
    if (ref != nullptr) {
      pinned_objects_[object_id] = std::move(ref);
      return true;
    }
    return false;
  }

pin_object_ 回调:
  // main.cc:843
  [&](const ray::ObjectID &object_id) {
    std::vector<ray::ObjectID> object_ids = {object_id};
    std::vector<std::unique_ptr<ray::RayObject>> results;
    if (node_manager->GetObjectsFromPlasma(object_ids, &results) &&
        results.size() > 0) {
      result = std::move(results[0]);
    }
    return result;
  }

  → GetObjectsFromPlasma → [IPC: GetRequest]
    → store.cc:108 AddToClientObjectIds(raylet作为新client)
      → AddReference                                  server ref: 0→1
  → 返回 RayObject → pinned_objects_.emplace (pull_manager持有)
```

### 9.3 Worker Get（使用对象）

```
HandleObjectLocal:
  → node_manager.cc:2448 通知等待的worker
  → worker收到 PlasmaObjectReady RPC
  → worker 调 GetObjectsFromPlasmaStore
    → [IPC: GetRequest] → AddToClientObjectIds → AddReference  server ref: 1→2

  worker 用完后 Release:
    → [IPC: ReleaseRequest] → RemoveFromClientObjectIds → RemoveReference  server ref: 2→1
```

### 9.4 Pull request 取消 → Unpin

```
Pull request 取消的三个触发场景:

1. Worker 的 Get 请求完成（依赖满足）
   → lease_dependency_manager.cc:156
     → object_manager_.CancelPull(pull_request_id)

2. Worker 断连
   → lease_dependency_manager.cc:194
     → object_manager_.CancelPull(pull_request_id)

3. Lease 的依赖不再需要（Task被调度或取消）
   → lease_dependency_manager.cc:263
     → object_manager_.CancelPull(pull_request_id)

CancelPull 链路:
  → pull_manager.cc:317 CancelPull
    → pull_manager.cc:325 if(bundles.active_requests.count(request_id) > 0):
      → DeactivateBundlePullRequest
        → pull_manager.cc:185 遍历request.objects_
        → pull_manager.cc:188 if(it->second.empty()):
          → pull_manager.cc:197 UnpinObject(obj_id)

UnpinObject:
  // pull_manager.cc:625
  void PullManager::UnpinObject(const ObjectID &object_id) {
    auto it = pinned_objects_.find(object_id);
    if (it != pinned_objects_.end()) {
      pinned_objects_size_ -= it->second->GetSize();
      pinned_objects_.erase(it);  // RayObject析构 → PlasmaClient::Release
    }
  }

  → RayObject 析构 → PlasmaClient::Release
    → [IPC: ReleaseRequest] → server ref 1→0 → 加入LRU → 可淘汰
```

### 9.5 Pull 路径完整时序

```
时间    pull_manager (raylet client)     worker (worker client)     server ref
──────────────────────────────────────────────────────────────────────────────
T1      PinNewObjectIfNeeded → Get       -                           1
T2      持有 pin                          Get → 加入                  2
T3      CancelPull → UnpinObject          持有 RayObject               1
        → RayObject析构
        → raylet ReleaseRequest
T4      -                                  用完 → RayObject析构          0
                                           → worker ReleaseRequest
                                           → 加入LRU → 可淘汰
```

**Pull pin 的生命周期 = pull 请求的生命周期**，和 owner 无关。对象到了、worker 用完了、或者 worker/task 不需要了 → cancel pull → unpin → server ref=0。

---

## 10. 完整链路 5：ReceivedByPush（replication push 接收端）—— 问题所在

### 10.1 数据接收

```
远端raylet MaybeReplicateObject → Push:
  object_manager.cc:610 HandlePush
  → is_replication_push == true:
    → ReceiveReplicationPushChunk(node_id, object_id, owner_address, ...)

ReceiveReplicationPushChunk:
  // object_manager.cc:696
  → 检查: has_active_pull || object_already_local
    → 如果有active pull或对象已存在: 拒绝push (skipped)
    → 如果都没有: 继续

  → buffer_pool.cc:100 CreateChunk
    → EnsureBufferExists → store_client_->CreateAndSpillIfNeeded
      → InsertObjectInUse(count=1) + IncrementObjectCount(count=2)    2  1
      → [IPC: CreateRequest, source=ReceivedByPush]

  → WriteChunk (逐chunk写入)

  最后一个chunk写完:
  → buffer_pool.cc:172 Seal → store_client_->Seal()                  1  1
  → buffer_pool.cc:173 Release → store_client_->Release()
    → count 1→0 → MarkObjectUnused + SendReleaseRequest
    → [IPC: ReleaseRequest] → RemoveFromClientObjectIds → RemoveReference
    → server ref 1→0 → EndObjectAccess → 加入LRU → evictable!
```

### 10.2 add_object_callback_ 触发但无人 Pin

```
add_object_callback_ 触发:
  → HandleObjectAdded → PinNewObjectIfNeeded
    → pull_manager.cc:588:
      bool active = active_object_pull_requests_.count(object_id) > 0;
      // active == false! 没人pull这个对象
      // 不调用 TryPinObject → server ref 保持 0

  → HandleObjectLocal:
    → MaybeReplicateObject: source==ReceivedByPush, 跳过
    → SpillIfOverPrimaryObjectsThreshold:
      → pinned_used 不含 ReceivedByPush → 不触发spill
    → 没有任何后续Get/Pin操作
```

### 10.3 最终状态

```
      ──── 最终稳态 ────
      client: 全部退出 (buffer_pool Release后无后续client)
      server ref: 0 → LRU随时淘汰!
      owner ref: locations 中可能包含此节点 (ReportObjectAdded时添加)
                但无 pinned_at_node_id_ (不是primary)
```

**对比所有路径**：

| 路径 | Seal+Release后 server ref | 谁在后续 Get/Pin | 最终 server ref | 状态 |
|------|--------------------------|----------------|----------------|------|
| Put | 0 | raylet PinObjectIDs | 1 | spillable |
| SealExisting | 0 | raylet PinObjectIDs | 1 | spillable |
| PinExistingReturnObject | 0→1(Get)→2(Pin)→1(Release) | Get + raylet Pin | 1 | spillable |
| Pull | 0 | pull_manager TryPinObject | 1 | spillable |
| **ReceivedByPush** | **0** | **无** | **0** | **evictable!** |

---

## 11. 完整链路 6：Owner Unpin（owner out of scope）

```
步骤  代码位置                                        说明
────────────────────────────────────────────────────────────────────────────────
1     reference_counter.cc 判定对象OutOfScope()
      → 发布 WorkerObjectEviction (pub/sub)
        → core_worker_subscriber_->Publish(...)

2     raylet 收到订阅回调:
      local_object_manager.cc:82 subscription_callback
        → local_object_manager.cc:86 ReleaseFreedObject(obj_id)

3     local_object_manager.cc:111 ReleaseFreedObject
      → local_object_manager.cc:117 is_freed_ = true
      → local_object_manager.cc:123 pinned_objects_it != end:
        → local_object_manager.cc:129 pinned_objects_size_ -= size
        → local_object_manager.cc:130 pinned_objects_.erase(it)
        → local_object_manager.cc:131 local_objects_.erase(it)

4     RayObject 析构 → 释放持有的 plasma buffer
      → raylet的 PlasmaClient::Release
        → client.cc:500 count-=1 (1→0)
        → client.cc:512 count==0:
          → MarkObjectUnused + SendReleaseRequest
          → [IPC: ReleaseRequest]

5     store.cc:434 处理 ReleaseRequest:
      → store.cc:440 ReleaseObject
        → store.cc:265 RemoveFromClientObjectIds
          → store.cc:256 RemoveReference
            → obj_lifecycle_mgr.cc:157 entry->ref_count_--
            → obj_lifecycle_mgr.cc:161 if(ref_count_ == 0):
              → obj_lifecycle_mgr.cc:164 EndObjectAccess
                → eviction_policy.cc:145:
                  cache_.Add(object_id, size)      // 加入LRU → 可淘汰
                  pinned_memory_bytes_ -= size

6     如有spilled文件:
      → local_object_manager.cc:135 spilled_object_pending_delete_.push
      → 后续 FlushFreeObjects → FreeObjects
        → DeleteSpilledObjects RPC 给 DELETE_WORKER
```

---

## 12. Pull Pin vs Primary Pin 生命周期对比

| | Primary pin (`PinObjectsAndWaitForFree`) | Pull pin (`PullManager::TryPinObject`) |
|---|---|---|
| **触发** | `PinObjectIDs` RPC（Create/Put/SealExisting 后） | `PinNewObjectIfNeeded`（Pull 完成后） |
| **unpin 触发** | owner 发布 `WorkerObjectEviction` → raylet 收到订阅回调 → `ReleaseFreedObject` | **最后一个 pull request 取消** → `UnpinObject` |
| **生命周期** | 和 owner 的语义引用绑定，owner out of scope 才释放 | 和 pull request 绑定，pull 请求没了就释放 |
| **owner 关系** | 订阅 owner 的 `WorkerObjectEviction` 通道 | 不订阅 owner |
| **server ref** | owner 活着期间一直=1 | pull 请求取消后立刻=0 |
| **spillable** | 是（ref=1 时可 spill 不可 evict） | 是（pull 期间可 spill 不可 evict） |
| **代码** | `local_object_manager.cc:31` | `pull_manager.cc:598` |

---

## 13. 汇总对比表

### 13.1 各路径最终 server ref 状态

| 路径 | 写完后 server ref | 谁持 pin | unpin 触发 | 最终生命周期 |
|------|-------------------|---------|-----------|------------|
| Put | 1 | raylet (PinObjectsAndWaitForFree) | owner out of scope → eviction | owner 活着期间 |
| SealExisting | 1 | raylet (PinObjectsAndWaitForFree) | owner out of scope → eviction | owner 活着期间 |
| PinExistingReturnObject | 1 | raylet (PinObjectsAndWaitForFree) | owner out of scope → eviction | owner 活着期间 |
| Pull | 1 | pull_manager (TryPinObject) | pull request 取消 | pull 请求期间 |
| **ReceivedByPush** | **0** | **无** | **不适用** | **立即可被 LRU evict** |

### 13.2 Client count 与 server ref 对应关系

| client 操作 | client count | server ref | IPC |
|------------|-------------|-----------|-----|
| Create → 加入 | 0→2 | 0→1 (+1) | CreateRequest → AddToClientObjectIds |
| Seal 内部 Release | 2→1 | 不变 | 无（count>0） |
| 回调 Release → 退出 | 1→0 | -1 | ReleaseRequest → RemoveFromClientObjectIds |
| Get → 加入 | 0→1 | +1 | GetRequest → AddToClientObjectIds |
| Release → 退出 | 1→0 | -1 | ReleaseRequest → RemoveFromClientObjectIds |

---

## 14. 关键代码索引

### Client Count

| 文件 | 行号 | 函数 | 说明 |
|------|------|------|------|
| `plasma/client.h` | 339 | `ObjectInUseEntry` | client count 存储结构 |
| `plasma/client.cc` | 110 | `InsertObjectInUse` | count 0→1 |
| `plasma/client.cc` | 126 | `IncrementObjectCount` | count +1 |
| `plasma/client.cc` | 490 | `Release` | count -1，count=0 时通知 server |
| `plasma/client.cc` | 480 | `MarkObjectUnused` | 从 objects_in_use_ 移除 |

### Server ref_count

| 文件 | 行号 | 函数 | 说明 |
|------|------|------|------|
| `plasma/common.h` | 181 | `LocalObject::ref_count_` | server ref 存储 |
| `plasma/obj_lifecycle_mgr.cc` | 128 | `AddReference` | ref +1，0→1 时 BeginObjectAccess |
| `plasma/obj_lifecycle_mgr.cc` | 148 | `RemoveReference` | ref -1，→0 时 EndObjectAccess |
| `plasma/store.cc` | 134 | `AddToClientObjectIds` | client 加入 → AddReference |
| `plasma/store.cc` | 247 | `RemoveFromClientObjectIds` | client 退出 → RemoveReference |
| `plasma/eviction_policy.cc` | 105 | `ChooseObjectsToEvict` | LRU 淘汰选择 |
| `plasma/eviction_policy.cc` | 136 | `BeginObjectAccess` | 从 LRU 移除，不可淘汰 |
| `plasma/eviction_policy.cc` | 144 | `EndObjectAccess` | 加入 LRU，可淘汰 |

### Owner Ref

| 文件 | 行号 | 函数 | 说明 |
|------|------|------|------|
| `core_worker/reference_counter.cc` | 953 | `UpdateObjectPinnedAtRaylet` | 设置 pinned_at_node_id_ |
| `core_worker/reference_counter.cc` | 895 | `ResetObjectsOnRemovedNode` | 节点死亡处理 |
| `core_worker/reference_counter.cc` | 919 | `needs_recovery_has_replica` | 有存活副本 |
| `core_worker/reference_counter.cc` | 921 | `needs_recovery_no_replica` | 无存活副本 |
| `core_worker/reference_counter.cc` | 940 | `secondary_copy_lost` | 非主副本丢失 |

### Raylet Pin

| 文件 | 行号 | 函数 | 说明 |
|------|------|------|------|
| `raylet/local_object_manager.cc` | 31 | `PinObjectsAndWaitForFree` | primary pin + 订阅 owner eviction |
| `raylet/local_object_manager.cc` | 111 | `ReleaseFreedObject` | owner eviction → unpin |
| `raylet/local_object_manager.cc` | 625 | `PullManager::UnpinObject` | pull pin 释放 |
| `raylet/node_manager.cc` | 2605 | `HandlePinObjectIDs` | 处理 PinObjectIDs RPC |
| `raylet/node_manager.cc` | 2419 | `HandleObjectLocal` | 对象本地化回调 |

### Core Worker 写入路径

| 文件 | 行号 | 函数 | 说明 |
|------|------|------|------|
| `core_worker/core_worker.cc` | 2917 | `AllocateReturnObject` | 大小判断 + Create |
| `core_worker/core_worker.cc` | 3220 | `SealReturnObject` | SealExisting 入口 |
| `core_worker/core_worker.cc` | 1198 | `SealExisting` | Seal + Pin + Release |
| `core_worker/core_worker.cc` | 3294 | `PinExistingReturnObject` | 已有对象 + Get + Pin |
| `core_worker/core_worker.cc` | 1001 | `PutInLocalPlasmaStore` | Put + Pin |
| `core_worker/store_provider/plasma_store_provider.cc` | 98 | `Put` | Create + Seal 同步 |
| `core_worker/store_provider/plasma_store_provider.cc` | 256 | `Get` | Get 路径 |

### Object Manager Push/Pull

| 文件 | 行号 | 函数 | 说明 |
|------|------|------|------|
| `object_manager/object_manager.cc` | 610 | `HandlePush` | 区分 replication/pull push |
| `object_manager/object_manager.cc` | 643 | `ReceivePullChunk` | Pull chunk 接收 |
| `object_manager/object_manager.cc` | 696 | `ReceiveReplicationPushChunk` | Replication push 接收 |
| `object_manager/object_buffer_pool.cc` | 100 | `CreateChunk` | chunk 级 Create |
| `object_manager/object_buffer_pool.cc` | 225 | `EnsureBufferExists` | 确保buffer存在 |
| `object_manager/object_buffer_pool.cc` | 171-174 | Seal + Release | 所有 chunk 写完 |
| `object_manager/pull_manager.cc` | 586 | `PinNewObjectIfNeeded` | Pull 完成后 pin |
| `object_manager/pull_manager.cc` | 598 | `TryPinObject` | 尝试 pin |
| `object_manager/pull_manager.cc` | 625 | `UnpinObject` | Pull pin 释放 |
| `object_manager/pull_manager.cc` | 317 | `CancelPull` | 取消 pull |

### Lease Dependency Manager（pull request 管理）

| 文件 | 行号 | 函数 | 说明 |
|------|------|------|------|
| `raylet/lease_dependency_manager.cc` | 53 | `CancelPull` (wait) | Wait 请求完成 → cancel pull |
| `raylet/lease_dependency_manager.cc` | 156 | `CancelPull` (get) | Get 请求完成 → cancel pull |
| `raylet/lease_dependency_manager.cc` | 263 | `CancelPull` (lease) | Lease 不再需要 → cancel pull |

---

## 15. Object Location 上报机制

Ray 集群中每个节点需要将对象的位置信息（内存副本、spill 位置）上报给 Owner Worker，Owner 维护全局 location 集合并通过 pub/sub 推送给订阅者。

### 15.1 上报通道

所有 location 上报通过 `OwnershipBasedObjectDirectory` → gRPC `UpdateObjectLocationBatch` 发送给 Owner Worker，**三种更新类型复用同一通道**：

| 事件类型 | 触发点 | RPC 字段 | 含义 |
|---------|--------|---------|------|
| 内存副本增加 | `ObjectManager::HandleObjectAdded` (object_manager.cc:178) | `plasma_location_update=ADDED` | 对象进入本节点 plasma |
| 内存副本移除 | `ObjectManager::HandleObjectDeleted` (object_manager.cc:207) | `plasma_location_update=REMOVED` | 对象从本节点 plasma 移除 |
| Spill 完成 | `LocalObjectManager::OnObjectSpilled` (local_object_manager.cc:440) | `spilled_location_update{spilled_url, spilled_to_local_storage}` | 对象被 spill 到外部存储 |

### 15.2 上报流程详解

```
节点 B (Raylet)                              节点 A (Owner Worker)
────────────────                             ────────────────────

1. 对象进入/离开 plasma / spill 完成

2. OwnershipBasedObjectDirectory:
   ReportObjectAdded / ReportObjectRemoved / ReportObjectSpilled
   (ownership_object_directory.cc:121 / :144 / :167)

3. 构造 ObjectLocationUpdate，缓存到:
   location_buffers_[owner_worker_id].second[object_id]
   location_buffers_[owner_worker_id].first.emplace_back(object_id)

4. SendObjectLocationUpdateBatchIfNeeded()
   → batch 最多 kMaxObjectReportBatchSize 个 update
   → gRPC UpdateObjectLocationBatch ─────────→

5. Owner Worker 收到:                        CoreWorker::HandleUpdateObjectLocationBatch
                                              (core_worker.cc:3705)

                                              遍历 object_location_updates:
                                              ├ has_plasma_location_update?
                                              │  ├ ADDED → AddObjectLocationOwner
                                              │  │   → reference_counter_->AddObjectLocation
                                              │  │     → it->second.locations.emplace(node_id)
                                              │  │     → PushToLocationSubscribers(it)
                                              │  └ REMOVED → RemoveObjectLocationOwner
                                              │      → reference_counter_->RemoveObjectLocation
                                              │        → it->second.locations.erase(node_id)
                                              │        → PushToLocationSubscribers(it)
                                              │
                                              └ has_spilled_location_update?
                                                → AddSpilledObjectLocationOwner
                                                  → reference_counter_->HandleObjectSpilled
                                                    → it->second.spilled = true
                                                    → it->second.spilled_url = url
                                                    → it->second.spilled_node_id = node_id
                                                    → PushToLocationSubscribers(it)
```

### 15.3 关键点：Pull 对象也会上报

**Pull 路径创建的对象同样走 `HandleObjectAdded` → `ReportObjectAdded`**。无论对象来自 Push 还是 Pull，只要被 Seal 到 plasma store 且有 `object_info`（含 owner 信息），都会上报到 Owner。

**但 ReceivedByPush 对象的问题是**：Seal+Release 后 ref=0 → LRU 立即淘汰 → `HandleObjectDeleted` → `ReportObjectRemoved` → Owner 把节点从 locations 移除。**加进去又被移除了**，所以 Owner 认为该节点没有副本。

### 15.4 Owner 端 location 数据结构

```cpp
// reference_counter.cc — Reference 结构中的关键字段
struct Reference {
    absl::flat_hash_set<NodeID> locations;   // 有内存副本的节点集合
    std::string spilled_url;                 // spill URL (本地路径或 S3 URL)
    NodeID spilled_node_id;                  // spill 所在节点 (IsNil 表示外部存储)
    bool spilled;                            // 是否已被 spill
    bool did_spill;                          // 是否执行过 spill
    std::optional<NodeID> pinned_at_node_id_; // primary copy 所在节点
};
```

Owner 的 `PushToLocationSubscribers` 发布 `WorkerObjectLocationsPubMessage`，包含：
- `node_ids`：所有有内存副本的节点
- `spilled_url`：spill URL
- `spilled_node_id`：spill 所在节点
- `pending_creation`：是否正在创建中
- `object_size`：对象大小

---

## 16. WorkerObjectEviction Pub/Sub 完整机制

Owner Worker 上持有对象语义生命周期，当对象 OutOfScope 时需要通知所有 pin 了该对象的 raylet 释放。这套通知通过 **WorkerObjectEviction** pub/sub channel 实现。

### 16.1 架构：每个 Worker 进程既是 Publisher 又是 Subscriber

```
节点 A (Owner Worker)                     节点 B (Raylet)
┌──────────────────────┐                  ┌──────────────────────┐
│ CoreWorker 进程       │                  │ Raylet 进程           │
│                      │                  │                      │
│ Publisher            │                  │ core_worker_subscriber_│
│  (object_info_publisher_)               │   (Subscriber)       │
│  ├ 注册 channel:      │                  │   ├ 注册 channel:     │
│  │  WORKER_OBJECT_   │                  │   │  WORKER_OBJECT_   │
│  │  EVICTION         │                  │   │  EVICTION         │
│  │                   │                  │   │                   │
│  ├ subscribers_:      │   Long Polling   │   │                   │
│  │  subscriber_id→   │◄───gRPC──────────│   │                   │
│  │  SubscriberState  │                  │   │                   │
│  │   ├ mailbox_      │                  │   │                   │
│  │   └ long_polling_ │                  │   │                   │
│  │     connection    │                  │   │                   │
│  │                   │    消息发布       │   │                   │
│  │ Publish() ────────│───gRPC──────────→│   HandleLongPolling   │
│  │                   │   (Long Polling   │   Response           │
│  │                   │    Reply)         │   → subscription_callback│
└──────────────────────┘                  └──────────────────────┘
```

### 16.2 步骤 1：订阅（Raylet → Owner Worker）

```
1. LocalObjectManager::PinObjectsAndWaitForFree
   (local_object_manager.cc:57)
   构造 WorkerObjectEvictionSubMessage:
     {
       object_id,
       intended_worker_id = owner_address.worker_id(),
       subscriber_address = {self_node_id, ip, port}  // raylet 地址
     }

2. core_worker_subscriber_->Subscribe(
     sub_message,
     ChannelType::WORKER_OBJECT_EVICTION,
     owner_address,       // Owner 的 RPC 地址（可能跨节点）
     object_id.Binary(),
     subscription_callback,  // Owner 发布 eviction 时触发
     owner_dead_callback)    // Owner 死亡时触发

3. Subscriber::Subscribe (subscriber.cc:261)
   → 构造 CommandItem (subscribe 命令)
   → commands_[publisher_id].emplace(command)
   → SendCommandBatchIfPossible()
     → gRPC PubsubCommandBatchRequest 发给 Owner Worker
   → Channel(channel_type)->Subscribe() → 注册本地回调
   → MakeLongPollingConnectionIfNotConnected()
     → 建立长轮询连接

4. Owner Worker 收到 PubsubCommandBatchRequest
   → CoreWorker::HandlePubsubCommandBatch
   → ProcessSubscribeMessage
     → ProcessSubscribeForObjectEviction (core_worker.cc:3562)
       → 构造 unpin_object lambda:
           [this](const ObjectID &object_id) {
             PubMessage pub_message;
             pub_message.set_channel_type(WORKER_OBJECT_EVICTION);
             pub_message.mutable_worker_object_eviction_message()
               ->set_object_id(object_id.Binary());
             object_info_publisher_->Publish(std::move(pub_message));
           }
       → reference_counter_->AddObjectOutOfScopeOrFreedCallback(
             object_id, unpin_object)
         → 存入 it->second.on_object_out_of_scope_or_freed_callbacks

5. Owner Worker 的 Publisher 注册订阅:
   → Publisher::RegisterSubscription(channel_type, subscriber_id, key_id)
   → subscription_index_map_[channel].AddEntry(key_id, subscriber)
```

### 16.3 步骤 2：Long Polling（Raylet 持续等待消息）

```
6. Subscriber::MakeLongPollingPubsubConnection (subscriber.cc:296)
   → 构造 PubsubLongPollingRequest {subscriber_id, publisher_id, max_processed_sequence_id}
   → subscriber_client->PubsubLongPolling(request, callback)
     → [gRPC] 发给 Owner Worker

7. Owner Worker 收到 PubsubLongPollingRequest
   → Publisher::ConnectToSubscriber (publisher.cc:364)
   → 找到/创建 SubscriberState
   → subscriber->ConnectToSubscriber(request, ...)
     → long_polling_connection_ = make_unique<LongPollConnection>(reply_callback)
     → PublishIfPossible(force_noop=false)
       → mailbox_ 为空? → 不回复，保持长轮询挂起
       → mailbox_ 有消息? → 立刻回复

   (长轮询挂起: Owner Worker 持有 reply_callback, 不立刻回复，
    等 Publish 有消息时才回复)
```

### 16.4 步骤 3：Owner OutOfScope → 发布 eviction 消息

```
8. Owner 的 ReferenceCounter 判定对象 OutOfScope
   → OnObjectOutOfScopeOrFreed (reference_counter.cc:839)
     → 遍历 on_object_out_of_scope_or_freed_callbacks:
       → callback(object_id)

9. unpin_object lambda (core_worker.cc:3566):
   → 构造 PubMessage {channel=WORKER_OBJECT_EVICTION, object_id}
   → object_info_publisher_->Publish(std::move(pub_message))

10. Publisher::Publish (publisher.cc:421)
    → pub_message.set_sequence_id(++next_sequence_id_)
    → subscription_index.Publish(pub_message)
      → EntityState::Publish(msg)
        → mailbox_.push_back(msg)
        → PublishIfPossible(force_noop=false)
          → long_polling_connection_ 存在?
            → 从 mailbox_ 取出消息
            → pub_messages->Add(msg)
            → long_polling_connection_->send_reply_callback(...)
              → [gRPC reply] 发给 Raylet 的 Subscriber
```

### 16.5 步骤 4：Raylet 收到消息 → unpin

```
11. Subscriber::HandleLongPollingResponse (subscriber.cc:321)
    → 收到 PubsubLongPollingReply {pub_messages}
    → 遍历 pub_messages:
      → Channel(channel_type)->HandlePublishedMessage(publisher_address, msg)
        → 查找注册的 subscription_callback → 调用

12. subscription_callback (local_object_manager.cc:68):
    → ReleaseFreedObject(obj_id)
      → pinned_objects_.erase(it)
      → RayObject 析构 → PlasmaClient A Release → [IPC] server ref-1

13. 重新建立长轮询 (subscriber.cc:399):
    → if (SubscriptionExists(publisher_id)):
        MakeLongPollingPubsubConnection(publisher_address)
        → 重新发起 Long Polling，等待下一个消息
```

### 16.6 步骤 5：Owner 死亡的处理

```
Owner Worker 进程崩溃 → gRPC 连接断开
  → Subscriber::HandleLongPollingResponse 收到错误 status
    → subscriber.cc:328: status.ok() == false
    → 遍历 channels_ → HandlePublisherFailure(publisher_address, status)
      → 调用 owner_dead_callback (local_object_manager.cc:76):
        → ReleaseFreedObject(obj_id)
        → pinned_objects_.erase → RayObject 析构 → server ref-1
```

### 16.7 时序图

```
节点 B Raylet                      节点 A Owner Worker
────────────────                   ────────────────────
1. Subscribe(owner_address)
   → PubsubCommandBatch ──gRPC──→  注册 subscription
   → Long Polling ──gRPC────────→  挂起 reply_callback
                                    (等待消息)

     ... 对象 pin 期间, Long Polling 持续挂起 ...

                                    7. OutOfScope
                                    → callback → Publish()
                                    → mailbox_ 有消息
5. ←── Long Polling Reply ──gRPC──  回复 (包含 eviction 消息)

8. subscription_callback
   → ReleaseFreedObject
   → server ref-1
   → 重新 Long Polling ──gRPC──→   挂起新的 reply_callback
                                    (无更多消息)
```

### 16.8 连接粒度和资源消耗

**不是每个 object 一条连接**，是 **每个 owner worker 一条长轮询 gRPC 连接**，多个 object 共享。

Subscriber 内部数据结构：
```cpp
// subscriber.cc
publishers_connected_: Map<publisher_id, bool>
  → 每个 publisher_id (owner worker_id) 只建立一条 Long Polling gRPC 连接

commands_[publisher_id]: Queue<CommandItem>
  → 同一个 owner 的多个 subscribe 命令会 batch 到一个 PubsubCommandBatchRequest 里发送
```

关键代码 `MakeLongPollingConnectionIfNotConnected` (subscriber.cc:308):
```cpp
auto publishers_connected_it = publishers_connected_.find(publisher_id);
if (publishers_connected_it == publishers_connected_.end()) {
    publishers_connected_.emplace(publisher_id);
    MakeLongPollingPubsubConnection(publisher_address);  // 只在首次创建
}
```

资源消耗模型：

| 维度 | 粒度 | 数量级 |
|------|------|--------|
| gRPC 长轮询连接 | per owner worker | = 活跃 worker 数 (百~千级) |
| subscribe 命令 | per object | 一次性，batch 发送 |
| channel 内订阅条目 | per (publisher, key_id) | = 被 pin 的 object 数 |
| 消息投递 | per eviction event | 按需，仅 OutOfScope 时 |

- **连接数**：100 个 worker → 100 条长轮询连接（而非几万条）
- **内存**：每条连接维护一个 `mailbox_` 和 `SubscriberState`
- **CPU**：长轮询无消息时完全挂起，零开销；有消息时一次 reply 可批量携带多个 `pub_message`
- 每个 object 在 Owner 的 `ReferenceCounter` 里注册一个回调（`on_object_out_of_scope_or_freed_callbacks`）—— 纯内存，O(1) per object
- 每个 object 在 Publisher 的 `SubscriptionIndex` 里占一个 entry —— 纯内存

---

## 17. WorkerObjectLocations Pub/Sub 机制

除了 eviction 通道，还有 **WorkerObjectLocations** 通道，用于对象位置变更通知。PullManager 等组件通过订阅此通道获取对象的实时位置（内存副本节点、spill URL）。

### 17.1 订阅流程

```
PullManager 需要拉取对象 → ObjectManager::Pull
  → object_directory_->SubscribeObjectLocations(callback_id, object_id, owner_address, callback)
    → OwnershipBasedObjectDirectory::SubscribeObjectLocations (ownership_object_directory.cc:320)
      → 构造 WorkerObjectLocationsSubMessage {intended_worker_id, object_id}
      → object_location_subscriber_->Subscribe(
          sub_message,
          ChannelType::WORKER_OBJECT_LOCATIONS_CHANNEL,
          owner_address,
          object_id.Binary(),
          subscribe_done_callback,
          msg_published_callback,    // 位置变更时触发
          failure_callback)         // Owner 死亡或 ref 已删除时触发
      → 创建 LocationListenerState:
        {
          owner_address,
          current_object_locations,   // 当前已知内存副本节点
          spilled_url,                // spill URL
          spilled_node_id,            // spill 所在节点
          pending_creation,           // 是否正在创建
          object_size,                // 对象大小
          callbacks                   // 回调集合
        }
```

### 17.2 消息发布

当 Owner 端的对象位置发生变化时（`PushToLocationSubscribers`，reference_counter.cc:1678）：
```cpp
void ReferenceCounter::PushToLocationSubscribers(ReferenceTable::iterator it) {
    rpc::PubMessage pub_message;
    pub_message.set_key_id(object_id.Binary());
    pub_message.set_channel_type(WORKER_OBJECT_LOCATIONS_CHANNEL);
    auto object_locations_msg = pub_message.mutable_worker_object_locations_message();
    FillObjectInformationInternal(it, object_locations_msg);
    object_info_publisher_->Publish(std::move(pub_message));
}
```

发布内容 (`FillObjectInformationInternal`，reference_counter.cc:1713):
```cpp
for (const auto &node_id : it->second.locations) {
    object_info->add_node_ids(node_id.Binary());    // 内存副本节点
}
object_info->set_object_size(it->second.object_size_);
object_info->set_spilled_url(it->second.spilled_url);     // spill URL
object_info->set_spilled_node_id(it->second.spilled_node_id.Binary());  // spill 节点
object_info->set_pending_creation(it->second.pending_creation_);
object_info->set_did_spill(it->second.did_spill);
```

### 17.3 订阅者收到更新

```
Subscriber 收到 Long Polling Reply
  → ObjectLocationSubscriptionCallback (ownership_object_directory.cc:264)
    → UpdateObjectLocations(location_info, ..., &current_object_locations, &spilled_url, &spilled_node_id, ...)
    → 位置有变化? → 回调 PullManager 的 callback:
      callback(locations, spilled_url, spilled_node_id, pending_creation, object_size)
      → PullManager 更新 request.client_locations, request.spilled_url 等
```

### 17.4 触发 PushToLocationSubscribers 的场景

| 场景 | 触发代码 | 位置变更 |
|------|---------|---------|
| 内存副本增加 | `AddObjectLocationInternal` (reference_counter.cc:1468) | `locations +{node_id}` |
| 内存副本移除 | `RemoveObjectLocationInternal` | `locations -{node_id}` |
| Spill 完成 | `HandleObjectSpilled` (reference_counter.cc:1551) | `spilled_url`, `spilled_node_id` |
| 对象大小更新 | `UpdateObjectSize` (reference_counter.cc:414) | `object_size` |
| Pending creation 变更 | `UpdateObjectPendingCreationInternal` (reference_counter.cc:1494) | `pending_creation` |
| 首次订阅 | `PublishObjectLocationSnapshot` (reference_counter.cc:1731) | 全量快照 |

### 17.5 与 Eviction 通道的对比

| 维度 | WORKER_OBJECT_EVICTION | WORKER_OBJECT_LOCATIONS |
|------|----------------------|------------------------|
| 订阅者 | pin 了 primary copy 的 raylet | 需要拉取对象的 raylet (PullManager) |
| 触发条件 | Owner OutOfScope | 任何 location 变更 |
| 消息内容 | 仅 object_id | locations + spilled_url + object_size + ... |
| 生命周期 | primary copy pin 期间 | pull request 存续期间 |
| 连接粒度 | per owner worker | per owner worker |

两个通道共享同一套 pub/sub 基础设施（Long Polling），但独立运作，互不干扰。

---

## 18. Spill 流程详解

当 Plasma Store 内存压力过大时，raylet 会将 primary copy 对象 spill 到外部存储（本地磁盘或 S3）。

### 18.1 Spill 触发条件

```
NodeManager::SpillIfOverPrimaryObjectsThreshold()
  → LocalObjectManager::SpillObjectUptoMaxThroughput() (local_object_manager.cc:162)
    → 循环调用 TryToSpillObjects() 直到没有更多可 spill 或 worker 用满
```

### 18.2 Spillable 判定

```cpp
// store.cc:562
bool PlasmaStore::IsObjectSpillable(const ObjectID &object_id) {
    absl::MutexLock lock(&mutex_);
    auto entry = object_lifecycle_mgr_.GetObject(object_id);
    if (!entry) return false;
    return entry->Sealed() && entry->GetRefCount() == 1;
    // 只有 ref_count==1 (只有一个 client 持有) 才可 spill
}
```

**只有 primary pin 的对象可 spill**：
- Primary pin：raylet PlasmaClient A Create → Seal → Release → ref_count=1 → 满足条件
- Pull pin：同样 ref_count=1，但 `LocalObjectManager::TryToSpillObjects` 只遍历 `pinned_objects_`（primary pin 集合），不包含 PullManager 的 `pinned_objects_`
- **ReceivedByPush**：ref_count=0，不可 spill（也无需 spill，已经被 LRU 淘汰了）

### 18.3 Spill 执行流程

```
1. LocalObjectManager::TryToSpillObjects() (local_object_manager.cc:186)
   → 遍历 pinned_objects_，找 is_plasma_object_spillable_==true 的对象
   → 最多合并 max_fused_object_count_ 个对象到一个文件
   → 构造 objects_to_spill 列表

2. 对象从 pinned_objects_ 移到 objects_pending_spill_:
   → pinned_objects_.erase(it)
   → objects_pending_spill_[id] = std::move(it->second)
   → num_bytes_pending_spill_ += object_size

3. SpillObjectsInternal (local_object_manager.cc:234)
   → io_worker_pool_.PopSpillWorker()  // 获取一个 I/O worker
   → gRPC SpillObjects → CoreWorker::HandleSpillObjects (core_worker.cc:4214)
     → options_.spill_objects(object_refs)  // Python 层写外部存储
     → 返回 spilled_objects_url[]

4. Spill 成功 → OnObjectSpilled (local_object_manager.cc:417)
   → spilled_objects_url_.emplace(object_id, object_url)
   → 从 objects_pending_spill_ 移除
   → 更新 spilled_bytes_total_, spilled_objects_total_

5. 上报 Owner:
   → object_directory_->ReportObjectSpilled(
       object_id, self_node_id_, owner_address, object_url, generator_id, is_local_fs)
     → gRPC UpdateObjectLocationBatch → Owner Worker
     → Owner 更新 spilled_url, spilled_node_id
     → PushToLocationSubscribers → 通知所有订阅者
```

### 18.4 Spill 后对象在 plasma 中的状态

Spill 完成后，对象**仍然在 plasma store 中**（primary pin 持有），但 owner 已知 spill URL。当 Owner OutOfScope 时：
```
Owner → eviction pub/sub → raylet ReleaseFreedObject
  → pinned_objects_.erase → RayObject 析构 → PlasmaClient Release
  → server ref_count 0 → LRU 可淘汰
  → 如果还没被淘汰，plasma 自动清理
  → 如果对象已被 LRU 淘汰，那 spill URL 就是唯一的恢复途径
```

### 18.5 Pull pin 对象的 spill 行为

- Pull pin 的对象**不会被主动 spill**（不在 `LocalObjectManager::pinned_objects_` 中）
- Pull 请求结束后，`PullManager::UnpinObject` → RayObject 析构 → ref_count=0 → **被 LRU 淘汰**（evict，非 spill）
- Evict 和 Spill 的区别：evict 不保存到外部存储，数据直接丢失；spill 保存后可恢复

---

## 19. Spill 恢复流程详解

当 PullManager 需要拉取一个对象，但该对象没有内存副本时，可以从 spill 存储中恢复。

### 19.1 TryToMakeObjectLocal 完整优先级

```cpp
// pull_manager.cc:447
void PullManager::TryToMakeObjectLocal(const ObjectID &object_id) {
    // 优先级1: 从有内存副本的节点 pull
    bool did_pull = PullFromRandomLocation(object_id);
    if (did_pull) return;

    // 优先级2: 本地 spill 文件直接 restore
    std::string direct_restore_url = get_locally_spilled_object_url_(object_id);
    // → LocalObjectManager::GetLocalSpilledObjectURL
    //   → is_external_storage_type_fs_==true 才有值
    //   → 返回 spilled_objects_url_[object_id]

    // 优先级3: S3 等外部存储 URL restore
    if (direct_restore_url.empty()) {
        if (!request.spilled_url.empty() && request.spilled_node_id.IsNil()) {
            direct_restore_url = request.spilled_url;
        }
    }

    if (!direct_restore_url.empty()) {
        restore_spilled_object_(object_id, object_size, url, callback);
    }

    // 优先级4: 都没有 → 等待 reconstruction 或超时
}
```

### 19.2 PullFromRandomLocation 内部优先级

```cpp
// pull_manager.cc:511
bool PullManager::PullFromRandomLocation(const ObjectID &object_id) {
    auto &node_vector = request.client_locations;  // 有内存副本的节点
    auto &spilled_node_id = request.spilled_node_id;

    if (!node_vector.empty()) {
        // 子优先级 A: 从随机一个内存节点 pull
        int node_index = random(0, node_vector.size()-1);
        send_pull_request_(object_id, node_vector[node_index]);
        return true;
    }

    if (!spilled_node_id.IsNil() && spilled_node_id != self_node_id_) {
        // 子优先级 B: 向 spill 所在节点发 pull request
        // 远端会自动从磁盘恢复并推送
        send_pull_request_(object_id, spilled_node_id);
        return true;
    }

    // spilled_node_id == self_node_id_ (本地有 spill)
    // → 返回 false，让后面走本地 restore 路径
    return false;
}
```

**关键**：如果 `spilled_node_id == self_node_id_`（spill 在本节点），不会向自己发 pull request，避免 gRPC 自己跟自己通信。返回 false 后走本地 restore 路径。

### 19.3 完整优先级排序

| 优先级 | 路径 | 延迟组成 | 条件 |
|--------|------|-----------|------|
| 1 | 远端内存节点 pull | 网络传输 | owner locations 中有其他节点有内存副本 |
| 2 | 本地 spill restore | 本地磁盘读 + plasma 写入 | 本地有 spill 文件（fs 类型存储） |
| 3 | 远端 spill 节点 pull | 远端磁盘读 + 网络传输 | `spilled_node_id` 非空且非本节点 |
| 4 | S3 外部存储 restore | 网络下载 + 本地写入 | `spilled_node_id.IsNil()`，有 URL |
| 5 | 等待 reconstruction | 重新计算 | 都没有 |

### 19.4 远端 spill 节点的自动恢复

当 PullManager 向 spill 所在节点发 pull request 时：

```
节点 A (PullManager)                节点 B (spill 所在节点)
────────────────                    ────────────────────
send_pull_request_(object_id, B)
  → gRPC PullRequest ───────────→  ObjectManager::HandlePull (object_manager.cc:616)
                                     → Push(object_id, node_id=A)

                                   Push() 逻辑 (object_manager.cc:321):
                                   ├ local_objects_ 有? → PushLocalObject (内存推送)
                                   ├ 本地有 spill 文件?
                                   │  → PushFromFilesystem (从磁盘读→推送)
                                   │    → SpilledObjectReader::CreateSpilledObjectReader(url)
                                   │    → chunk_object_reader
                                   │    → PushObjectInternal(from_disk=true)
                                   └ 都没有? → 加入 unfulfilled_push_requests_
                                       → 等对象恢复后再推送
```

`PushFromFilesystem` (object_manager.cc:411) 将磁盘读取调度到 RPC 线程（off-main-thread），不阻塞主事件循环。

### 19.5 本地 restore 流程

```
1. get_locally_spilled_object_url_(object_id) 返回非空 URL

2. restore_spilled_object_(object_id, object_size, url, callback)
   → ObjectManager → LocalObjectManager::AsyncRestoreSpilledObject
     (local_object_manager.cc:470)
     → io_worker_pool_.PopRestoreWorker()  // 获取 restore I/O worker
     → gRPC RestoreSpilledObjects → CoreWorker::HandleRestoreSpilledObjects
       → options_.restore_spilled_objects(object_refs, spilled_urls)
         → Python 层读取外部存储 → 写回 plasma store

3. restore 成功 → 对象重新进入 plasma → HandleObjectAdded
   → ReportObjectAdded → Owner 更新 locations
```

### 19.6 S3 外部存储 restore

当 `spilled_node_id.IsNil()` 时，说明对象 spill 到了 S3 等外部存储，没有具体节点可以请求。这时直接从 URL 下载：

```
condition: !request.spilled_url.empty() && request.spilled_node_id.IsNil()
  → direct_restore_url = request.spilled_url  // S3 URL
  → restore_spilled_object_(...)  // 同本地 restore 路径
    → Python 层从 S3 下载 → 写入 plasma
```

---

## 20. 关键代码索引（补充）

### Pub/Sub 基础设施

| 文件 | 行号 | 函数 | 说明 |
|------|------|------|------|
| `pubsub/subscriber_interface.h` | 40 | `SubscriberInterface` | 订阅者接口 |
| `pubsub/subscriber.cc` | 261 | `Subscriber::Subscribe` | 注册订阅 + 建立长轮询 |
| `pubsub/subscriber.cc` | 296 | `MakeLongPollingPubsubConnection` | 建立/复用 gRPC 长轮询 |
| `pubsub/subscriber.cc` | 321 | `HandleLongPollingResponse` | 处理长轮询回复 |
| `pubsub/subscriber.cc` | 308 | `MakeLongPollingConnectionIfNotConnected` | 每个 publisher 只建一条连接 |
| `pubsub/publisher.cc` | 364 | `Publisher::ConnectToSubscriber` | 注册长轮询连接 |
| `pubsub/publisher.cc` | 421 | `Publisher::Publish` | 发布消息到 mailbox + 触发长轮询回复 |
| `pubsub/publisher.cc` | 28 | `EntityState::Publish` | 消息入 mailbox + PublishIfPossible |

### Object Location 上报

| 文件 | 行号 | 函数 | 说明 |
|------|------|------|------|
| `object_manager/ownership_object_directory.cc` | 121 | `ReportObjectAdded` | 上报内存副本增加 (ADDED) |
| `object_manager/ownership_object_directory.cc` | 144 | `ReportObjectRemoved` | 上报内存副本移除 (REMOVED) |
| `object_manager/ownership_object_directory.cc` | 167 | `ReportObjectSpilled` | 上报 spill 完成 (spilled_url) |
| `object_manager/ownership_object_directory.cc` | 228 | `SendObjectLocationUpdateBatchIfNeeded` | 批量发送 location 更新 |
| `core_worker/core_worker.cc` | 3705 | `HandleUpdateObjectLocationBatch` | Owner 接收 location 更新 |
| `core_worker/core_worker.cc` | 3774 | `AddObjectLocationOwner` | 添加内存副本节点 |
| `core_worker/core_worker.cc` | 3805 | `RemoveObjectLocationOwner` | 移除内存副本节点 |
| `core_worker/reference_counter.cc` | 1443 | `AddObjectLocation` | locations.emplace(node_id) |
| `core_worker/reference_counter.cc` | 1470 | `RemoveObjectLocation` | locations.erase(node_id) |
| `core_worker/reference_counter.cc` | 1520 | `HandleObjectSpilled` | 记录 spilled_url, spilled_node_id |

### WorkerObjectEviction Pub/Sub

| 文件 | 行号 | 函数 | 说明 |
|------|------|------|------|
| `raylet/local_object_manager.cc` | 57 | `PinObjectsAndWaitForFree` | 订阅 owner eviction |
| `raylet/local_object_manager.cc` | 68 | `subscription_callback` | 收到 eviction → ReleaseFreedObject |
| `raylet/local_object_manager.cc` | 76 | `owner_dead_callback` | Owner 死亡 → ReleaseFreedObject |
| `core_worker/core_worker.cc` | 3562 | `ProcessSubscribeForObjectEviction` | Owner 注册 eviction 回调 |
| `core_worker/reference_counter.cc` | 839 | `OnObjectOutOfScopeOrFreed` | 遍历 callbacks → 触发 eviction |
| `core_worker/reference_counter.cc` | 889 | `AddObjectOutOfScopeOrFreedCallback` | 注册 eviction 回调 |

### WorkerObjectLocations Pub/Sub

| 文件 | 行号 | 函数 | 说明 |
|------|------|------|------|
| `object_manager/ownership_object_directory.cc` | 320 | `SubscribeObjectLocations` | 订阅对象位置变更 |
| `object_manager/ownership_object_directory.cc` | 264 | `ObjectLocationSubscriptionCallback` | 收到位置更新 |
| `core_worker/reference_counter.cc` | 1678 | `PushToLocationSubscribers` | 发布位置快照 |
| `core_worker/reference_counter.cc` | 1713 | `FillObjectInformationInternal` | 填充 locations/spilled_url 等 |
| `core_worker/reference_counter.cc` | 1731 | `PublishObjectLocationSnapshot` | 首次订阅时发布全量快照 |
| `core_worker/core_worker.cc` | 3642 | `ProcessSubscribeObjectLocations` | Owner 处理位置订阅 |

### Spill 相关

| 文件 | 行号 | 函数 | 说明 |
|------|------|------|------|
| `object_manager/plasma/store.cc` | 560 | `IsObjectSpillable` | 判定 ref_count==1 且 sealed |
| `raylet/local_object_manager.cc` | 186 | `TryToSpillObjects` | 遍历 pinned_objects_ 找可 spill 对象 |
| `raylet/local_object_manager.cc` | 234 | `SpillObjectsInternal` | 调度 I/O worker 执行 spill |
| `raylet/local_object_manager.cc` | 417 | `OnObjectSpilled` | spill 完成回调 |
| `raylet/local_object_manager.cc` | 440 | `ReportObjectSpilled` | 上报 spill URL 给 Owner |
| `core_worker/core_worker.cc` | 4214 | `HandleSpillObjects` | I/O Worker 执行实际 spill |

### Spill 恢复相关

| 文件 | 行号 | 函数 | 说明 |
|------|------|------|------|
| `object_manager/pull_manager.cc` | 447 | `TryToMakeObjectLocal` | 恢复优先级入口 |
| `object_manager/pull_manager.cc` | 511 | `PullFromRandomLocation` | 内存节点 / 远端 spill 节点 |
| `object_manager/pull_manager.cc` | 473 | `get_locally_spilled_object_url_` | 本地 spill URL 查询 |
| `object_manager/pull_manager.cc` | 485 | `restore_spilled_object_` | 触发 restore |
| `raylet/local_object_manager.cc` | 470 | `AsyncRestoreSpilledObject` | 本地 restore 调度 |
| `raylet/local_object_manager.cc` | 449 | `GetLocalSpilledObjectURL` | 本地 spill URL 获取 |
| `core_worker/core_worker.cc` | 4230 | `HandleRestoreSpilledObjects` | I/O Worker 执行实际 restore |
| `object_manager/object_manager.cc` | 321 | `Push` | 远端 Push 入口（内存 / 磁盘 / 等待） |
| `object_manager/object_manager.cc` | 355 | `PushLocalObject` | 从内存推送 |
| `object_manager/object_manager.cc` | 411 | `PushFromFilesystem` | 从 spill 文件推送 |
| `object_manager/spilled_object_reader.cc` | 29 | `CreateSpilledObjectReader` | 从 URL 创建磁盘读取器 |
