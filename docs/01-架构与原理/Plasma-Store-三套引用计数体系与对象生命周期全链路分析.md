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
