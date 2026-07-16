# Ray 对象管理与引用计数机制详解

本文档详细分析 Ray 中 ObjectManager、ReferenceCounter、LocalObjectManager、PlasmaStoreProvider、MemoryStore 之间的协作关系，以及对象所有权、位置追踪、引用计数、跨节点获取的完整机制。

---

## 目录

1. [ObjectManager 架构与职责分工](#1-objectmanager-架构与职责分工)
2. [ReferenceCounter 完整机制：Owner vs Borrower](#2-referencecounter-完整机制owner-vs-borrower)
3. [HandleUpdateObjectLocationBatch 机制](#3-handleupdateobjectlocationbatch-机制)
4. [ReportObjectAdded / LocalObjectManager / ReferenceCounter 协作](#4-reportobjectadded--localobjectmanager--referencecounter-协作)
5. [ray.put() 完整流程](#5-rayput-完整流程)
6. [跨 owner 的 ray.put() 流程](#6-跨-owner-的-rayput-流程)
7. [任务返回对象完整流程](#7-任务返回对象完整流程)
8. [任务参数 ObjectRef 在执行方的注册机制](#8-任务参数-objectref-在执行方的注册机制)
9. [SubmitTask 参数注册 vs 执行方参数注册的区别](#9-submittask-参数注册-vs-执行方参数注册的区别)
10. [四大组件关系：ReferenceCounter / PlasmaStoreProvider / MemoryStore / ObjectManager](#10-四大组件关系referencecounter--plasmastoreprovider--memorystore--objectmanager)
11. [对象获取判断机制：逐层回退判断链](#11-对象获取判断机制逐层回退判断链)
12. [关键源文件索引](#12-关键源文件索引)

---

## 1. ObjectManager 架构与职责分工

### 1.1 三个核心组件的职责划分

每个 Raylet 节点上有三个组件共同管理对象，职责各不相同：

| 组件 | 职责 | 存什么 |
|---|---|---|
| **ObjectManager** | 对象传输层：Push/Pull 对象到其他节点 | 传输中的 chunk 状态、push/pull 请求队列 |
| **LocalObjectManager** | 本地对象生命周期：pin、spill、restore、free | `pinned_objects_`、`local_objects_`（pin 元信息） |
| **Plasma Store** | 共享内存存储 | 对象实际数据 |

```
┌──────────────────────────────────────────────────────────┐
│                      Raylet 进程                          │
│                                                          │
│  ┌────────────────┐  ┌──────────────────┐  ┌──────────┐ │
│  │  ObjectManager  │  │ LocalObjectManager│  │  Plasma  │ │
│  │  (传输引擎)      │  │ (pin/释放管理)    │  │  Store   │ │
│  │                │  │                  │  │ (共享内存) │ │
│  │ • Push/Pull    │  │ • Pin + 订阅     │  │ • LRU    │ │
│  │ • PullManager  │  │   eviction       │  │   驱逐    │ │
│  │ • PushManager  │  │ • Spill/Restore  │  │ • ref_   │ │
│  │ • ObjectBuffer │  │ • ReleaseFreed   │  │   count   │ │
│  │   Pool         │  │   Object         │  │          │ │
│  └────────────────┘  └──────────────────┘  └──────────┘ │
└──────────────────────────────────────────────────────────┘
```

### 1.2 ObjectManager 自身的子组件

```cpp
// src/ray/object_manager/object_manager.h
class ObjectManager : public ObjectManagerInterface {
  PullManager pull_manager_;           // 拉取策略：选哪个节点、何时重试
  PushManager push_manager_;           // 推送限流：去重 + chunk 调度
  ObjectBufferPool buffer_pool_;       // Plasma 读写缓冲池
  IObjectDirectory *object_directory_; // 对象位置目录（OBOD）
};
```

| 子组件 | 职责 |
|---|---|
| **PullManager** | 决定从哪个节点拉取、内存预算控制、优先级（ray.get > ray.wait > task args）、重试 |
| **PushManager** | 限流去重、跟踪在途 chunk、chunk 完成后调度剩余推送 |
| **ObjectBufferPool** | 传输过程中创建/读取/写入 Plasma 缓冲区 |
| **OwnershipBasedObjectDirectory** | 对象位置发现：订阅 owner 位置更新、汇报本地位置变化 |

### 1.3 ObjectManager 不关心所有权

ObjectManager 在 owner 节点和其他节点上运行的是**同一套代码**。它是一个"搬运工"，不关心谁是 owner。Owner 的区分发生在 CoreWorker 的 ReferenceCounter 层面。

```cpp
// src/ray/object_manager/object_manager.cc:171
void ObjectManager::HandleObjectAdded(const ObjectInfo &object_info) {
  local_objects_[object_id].object_info = object_info;
  used_memory_ += object_info.data_size + object_info.metadata_size;
  // 通知 owner 位置（不管 owner 是谁，都通过 OBOD 汇报）
  object_directory_->ReportObjectAdded(object_id, self_node_id_, object_info);
  pull_manager_->PinNewObjectIfNeeded(object_id);
  // ... 处理 unfulfilled_push_requests_ ...
}
```

---

## 2. ReferenceCounter 完整机制：Owner vs Borrower

### 2.1 ReferenceCounter 在每个 Worker 都存在

ReferenceCounter 属于 CoreWorker，**每个 Worker 进程一个**，Raylet 没有 ReferenceCounter。

同一个 `object_id_refs_` map 同时存储 owned 和 borrowed 对象，通过 `owned_by_us_` 区分：

```cpp
// src/ray/core_worker/reference_counter.h
class ReferenceCounter {
  // 所有追踪的对象引用，不区分 owned/borrowed
  ReferenceTable object_id_refs_;  // absl::flat_hash_map<ObjectID, Reference>
};
```

### 2.2 Reference 结构关键字段

```cpp
// src/ray/core_worker/reference_counter.h:307-519
struct Reference {
  // === 基础信息 ===
  std::optional<rpc::Address> owner_address_;  // Owner 的 RPC 地址
  bool owned_by_us_ = false;                    // 是否是 owner
  int64_t local_ref_count = 0;                  // Python 侧引用计数
  int64_t submitted_task_ref_count = 0;         // 正在运行的任务引用
  int64_t lineage_ref_count = 0;                // Lineage 引用

  // === Owner 独有信息 ===
  absl::flat_hash_set<NodeID> locations;        // 对象在哪些节点存在
  std::optional<NodeID> pinned_at_node_id_;     // 主副本 pin 在哪个 raylet
  std::string spilled_url;                      // spill 到外部存储的 URL
  NodeID spilled_node_id;                       // 执行 spill 的节点
  bool pending_creation_ = false;               // 对象是否还在创建中

  // === Borrower 追踪（Owner 侧）===
  BorrowInfo borrow_info;                       // 所有已知 borrower

  // === Borrower 侧信息 ===
  bool publish_ref_removed = false;             // Owner 订阅后设为 true
  bool foreign_owner_already_monitoring = false; // _owner 参数场景

  // === 回调 ===
  std::vector<std::function<void(const ObjectID &)>>
      on_object_out_of_scope_or_freed_callbacks;  // 对象释放回调
};
```

### 2.3 Owner vs Borrower 的 ReferenceCounter 对比

| | Owner Worker | Borrower Worker |
|---|---|---|
| `owned_by_us_` | `true` | `false` |
| 判断引用是否用完 | **是**，决定是否触发 eviction | **是**，决定是否通知 owner |
| 追踪 `locations` / `pinned_at_node_id_` | 是 | 否 |
| 追踪 `borrowers` | 所有已知 borrower + `WaitForRefRemoved` | 只追踪自己进一步分发的 |
| 触发 eviction（发布 WORKER_OBJECT_EVICTION） | **是** | 否 |
| 发布 RefRemoved | 否（消费方） | **是**（当 ref count=0） |
| 判断对象物理在不在 | 通过 `locations` + `pinned_at_node_id_` | **不关心**，只关心自己的引用计数 |

### 2.4 Borrower 的引用生命周期

```
Borrower Python 引用释放
  → RemoveLocalReference()
    → RefCount() == 0
      → 如果 publish_ref_removed_=true（owner 订阅过）
        → PublishRefRemovedInternal()
          → 发布 WORKER_REF_REMOVED 消息给 owner
      → 如果还有未汇报的 borrower 信息
        → 不能删除，等汇报完
      → 否则
        → DeleteReferenceInternal() → 从 object_id_refs_ 中移除
```

### 2.5 Owner 的引用生命周期

```
Owner 的 ref count=0 且所有 borrower 都释放
  → OnObjectOutOfScopeOrFreed()
    → 触发 on_object_out_of_scope_or_freed_callbacks
      → 发布 WORKER_OBJECT_EVICTION（通知 raylet 释放 pin）
    → UnsetObjectPrimaryCopy()（清除 pinned_at_node_id_）
  → Reference 条目可删除
```

### 2.6 两条 pub-sub 通道

**通道 1：WORKER_OBJECT_EVICTION（Owner → Raylet: "可以释放了"）**

```cpp
// Raylet 侧订阅（PinObjectsAndWaitForFree 中）:
core_worker_subscriber_->Subscribe(
    ..., rpc::ChannelType::WORKER_OBJECT_EVICTION,
    owner_address, object_id.Binary(),
    subscription_callback = [](msg) { ReleaseFreedObject(obj_id); },
    owner_dead_callback = [](id) { ReleaseFreedObject(obj_id); });

// Owner 侧处理订阅请求:
// src/ray/core_worker/core_worker.cc:3731
void CoreWorker::ProcessSubscribeForObjectEviction(...) {
  auto unpin_object = [this](const ObjectID &object_id) {
    // 发布 eviction 消息
    object_info_publisher_->Publish(WORKER_OBJECT_EVICTION, ...);
  };
  // 注册到 on_object_out_of_scope_or_freed_callbacks
  reference_counter_->AddObjectOutOfScopeOrFreedCallback(object_id, unpin_object);
}
```

**通道 2：WORKER_REF_REMOVED_CHANNEL（Borrower → Owner: "我不用了"）**

```cpp
// Owner 侧订阅 borrower:
// src/ray/core_worker/reference_counter.cc:1242
void ReferenceCounter::WaitForRefRemoved(...) {
  // 订阅 borrower 的 WORKER_REF_REMOVED_CHANNEL
}

// Borrower 侧发布:
// src/ray/core_worker/reference_counter.cc:1366
void ReferenceCounter::PublishRefRemovedInternal(...) {
  // 发布 ref_removed 消息，包含嵌套 borrower 信息
  object_info_publisher_->Publish(WORKER_REF_REMOVED_CHANNEL, ...);
}

// Owner 收到后:
// src/ray/core_worker/reference_counter.cc:1227
void ReferenceCounter::CleanupBorrowersOnRefRemoved(...) {
  // 合并传递 borrower、删除当前 borrower
  // 如果所有 borrower 都释放且 ref count=0
  //   → OnObjectOutOfScopeOrFreed()
}
```

### 2.7 同一个 ObjectID 在不同 Worker 的 ReferenceCounter 中

```
ObjectRef X (owner=Worker A)
  │
  ├─ Worker A (Owner): owned_by_us_=true, local_ref_count=1,
  │                    locations={Node1, Node2}, pinned_at_node_id_=Node1,
  │                    borrowers={B,C}, on_out_of_scope_callbacks=[unpin]
  │
  ├─ Worker B (执行方): owned_by_us_=false, local_ref_count=1,
  │                    owner_address_=A, publish_ref_removed=true
  │
  └─ Worker C (中间传递): owned_by_us_=false, local_ref_count=1,
                          owner_address_=A
```

三个 Worker 各自的 ReferenceCounter 都有条目，但字段完全不同。Owner 持有完整的控制信息，Borrower 只持有引用计数和 owner 地址。

---

## 3. HandleUpdateObjectLocationBatch 机制

### 3.1 调用方和触发时机

**调用方**：产生对象节点上的 `OwnershipBasedObjectDirectory`（OBOD），在以下事件发生时向 **owner CoreWorker** 发 RPC：

- `ReportObjectAdded()` — 对象加入本地 plasma
- `ReportObjectRemoved()` — 对象从本地 plasma 移除
- `ReportObjectSpilled()` — 对象 spill 到外部存储

**注意**：`ReportObjectAdded()` 是在**产生 object 的节点**上调用的，不一定是 owner 节点。OBOD 再通过 RPC 把位置更新发送给 owner CoreWorker。

### 3.2 批量 + 背压机制

```cpp
// src/ray/object_manager/ownership_object_directory.h:128-134
// 每个 owner 维护一个 FIFO 缓冲区
absl::flat_hash_map<WorkerID,
    std::pair<std::deque<ObjectID>,
              absl::flat_hash_map<ObjectID, rpc::ObjectLocationUpdate>>>
    location_buffers_;

// 在途请求集合（背压）
absl::flat_hash_set<WorkerID> in_flight_requests_;
```

```cpp
// src/ray/object_manager/ownership_object_directory.cc:173-258
void OwnershipBasedObjectDirectory::SendObjectLocationUpdateBatchIfNeeded(
    const WorkerID &worker_id, const NodeID &node_id, ...) {
  if (in_flight_requests_.contains(worker_id)) return;  // 背压：在途则等
  if (location_buffers_[worker_id].first.empty()) return;  // 无更新则跳过

  // 构造批量请求
  rpc::UpdateObjectLocationBatchRequest request;
  request.set_intended_worker_id(worker_id.Binary());
  request.set_node_id(node_id.Binary());
  // 最多发 kMaxObjectReportBatchSize 条
  // 同一对象多次更新只保留最新（map 覆盖），FIFO 保序防饿死

  in_flight_requests_.emplace(worker_id);
  owner_client->UpdateObjectLocationBatch(
      std::move(request),
      [this, worker_id, ...](const Status &status, ...) {
        in_flight_requests_.erase(worker_id);
        if (!status.ok()) {
          location_buffers_.erase(worker_id);  // owner 死了，清空
          return;
        }
        SendObjectLocationUpdateBatchIfNeeded(worker_id, ...);  // 递归发送剩余
      });
}
```

### 3.3 Owner CoreWorker 的处理

```cpp
// src/ray/core_worker/core_worker.cc:3865
void CoreWorker::HandleUpdateObjectLocationBatch(
    rpc::UpdateObjectLocationBatchRequest request,
    rpc::UpdateObjectLocationBatchReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  const auto &worker_id = request.intended_worker_id();
  if (HandleWrongRecipient(WorkerID::FromBinary(worker_id), send_reply_callback)) {
    return;  // 目标不是自己，忽略
  }

  const auto &node_id = NodeID::FromBinary(request.node_id());
  for (const auto &update : request.object_location_updates()) {
    const auto &object_id = ObjectID::FromBinary(update.object_id());

    if (update.has_spilled_location_update()) {
      // 记录 spill 信息
      AddSpilledObjectLocationOwner(object_id, ...);
      // → reference_counter_->HandleObjectSpilled()
    }

    if (update.has_plasma_location_update()) {
      if (update.plasma_location_update() == rpc::ObjectPlasmaLocationUpdate::ADDED) {
        AddObjectLocationOwner(object_id, node_id);
        // → reference_counter_->AddObjectLocation()
        // → locations.emplace(node_id)
        // → PushToLocationSubscribers() 通知订阅者
      } else if (update.plasma_location_update() == rpc::ObjectPlasmaLocationUpdate::REMOVED) {
        RemoveObjectLocationOwner(object_id, node_id);
        // → reference_counter_->RemoveObjectLocation()
        // → locations.erase(node_id)
        // → PushToLocationSubscribers()
      }
    }
  }
  send_reply_callback(Status::OK(), ...);
}
```

### 3.4 AddObjectLocationOwner 详细逻辑

```cpp
// src/ray/core_worker/core_worker.cc:3943
void CoreWorker::AddObjectLocationOwner(const ObjectID &object_id,
                                         const NodeID &node_id) {
  if (gcs_client_->Nodes().IsNodeDead(node_id)) return;  // 过滤死节点

  auto reference_exists = reference_counter_->AddObjectLocation(object_id, node_id);
  // 如果是 generator 任务，动态添加内部 ObjectID
  if (!maybe_generator_id.IsNil()) {
    if (task_manager_->ObjectRefStreamExists(maybe_generator_id)) {
      task_manager_->TemporarilyOwnGeneratorReturnRefIfNeeded(object_id, maybe_generator_id);
    } else {
      reference_counter_->AddDynamicReturn(object_id, maybe_generator_id);
    }
    RAY_UNUSED(reference_counter_->AddObjectLocation(object_id, node_id));
  }
  if (reference_exists) {
    MaybeTriggerPinTransfer(object_id, node_id);  // preemptible→stable 迁移
  }
}
```

### 3.5 完整数据流

```
Plasma Store 事件 (对象 added/removed/spilled)
  │
  ▼
ObjectManager::HandleObjectAdded / HandleObjectDeleted
  │
  ▼
OwnershipBasedObjectDirectory::ReportObjectAdded/Removed/Spilled
  │
  ├─ 缓冲到 location_buffers_[owner_worker_id]
  │
  ▼
SendObjectLocationUpdateBatchIfNeeded()
  │
  ├─ RPC: UpdateObjectLocationBatch → Owner CoreWorker
  │
  ▼
CoreWorker::HandleUpdateObjectLocationBatch()
  │
  ├─ AddObjectLocationOwner → AddObjectLocation → locations.emplace(node_id)
  │                                          → PushToLocationSubscribers()
  │
  └─ RemoveObjectLocationOwner → RemoveObjectLocation → locations.erase(node_id)
                                              → PushToLocationSubscribers()
```

---

## 4. ReportObjectAdded / LocalObjectManager / ReferenceCounter 协作

### 4.1 两条独立的路径

**路径1：位置通知（被动）**— ReportObjectAdded

```
Plasma seal → HandleObjectAdded → OBOD::ReportObjectAdded
  → 批量缓冲 → RPC UpdateObjectLocationBatch → Owner CoreWorker
  → AddObjectLocationOwner() → reference_counter_->AddObjectLocation()
  → 写入 Reference.locations 集合 → PushToLocationSubscribers()
  （发布给订阅 WORKER_OBJECT_LOCATIONS_CHANNEL 的 raylet/worker）
```

**路径2：Pin 订阅（主动）**— LocalObjectManager

```
Owner CoreWorker 发 PinObjectIDs RPC → Raylet HandlePinObjectIDs
  → LocalObjectManager::PinObjectsAndWaitForFree()
    → pinned_objects_ 持有 RayObject（防止 plasma 驱逐）
    → 订阅 Owner 的 WORKER_OBJECT_EVICTION 通道
    → Owner 的 ProcessSubscribeForObjectEviction()
      → 注册 unpin_object callback 到 on_object_out_of_scope_or_freed_callbacks
```

### 4.2 locations vs pinned_at_node_id_ 的区别

| | `locations` | `pinned_at_node_id_` |
|---|---|---|
| 含义 | 对象在**哪些节点**的 plasma 中存在 | 对象的**主副本**被哪个 raylet pin 了 |
| 更新方式 | OBOD 被动通知（ReportObjectAdded/Removed） | CoreWorker 主动 PinObjectIDs 后设置 |
| 用途 | 供 Pull 查找位置、locality 调度 | 控制 eviction/recovery 逻辑 |
| 释放时 | REMOVED 通知后逐个移除 | `OnObjectOutOfScopeOrFreed` 时 reset |

两者可能暂时不一致（`locations` 批量延迟，`pinned_at_node_id_` 立即更新）。

### 4.3 释放的完整闭环

```
Owner ref count=0 且所有 borrower 都释放
  → OnObjectOutOfScopeOrFreed()
    → 触发 unpin_object callback → 发布 WORKER_OBJECT_EVICTION
    → Raylet 收到 → ReleaseFreedObject()
      → 从 pinned_objects_ 移除（plasma 可驱逐）
      → 加入 objects_pending_deletion_ → FlushFreeObjects() → FreeObjects()
    → UnsetObjectPrimaryCopy() → pinned_at_node_id_.reset()

  （同时）Plasma 驱逐后 → HandleObjectDeleted → ReportObjectRemoved
    → Owner 收到 REMOVED → RemoveObjectLocation → 从 locations 移除
```

### 4.4 产生者 Raylet 的角色

对象在节点 A 产生，owner 在节点 B：

```
执行 Worker (节点A)                     Owner Worker (节点B)
    │                                       │
    ├─ 写入 plasma + Seal                    │
    ├─ PinObjectIDs(owner=B) → 本地 raylet   │
    │                                       │
    └─ Raylet (节点A) 收到 PinObjectIDs:     │
       ├─ pinned_objects_[id] = RayObject    │  ← 只管 pin 住
       ├─ local_objects_[id] = {             │  ← 只记 owner 地址
       │     owner_address_ = B的地址          │
       │   }                                  │
       ├─ 订阅 B 的 WORKER_OBJECT_EVICTION    │  ← 等 B 说"放了吧"
       │                                       │
       │                                       ├─ ReferenceCounter 记录所有元信息：
       │                                       │   locations={A}, pinned_at=A
       │                                       │   borrowers, ref_count...
       │                                       │
       │  ←── WORKER_OBJECT_EVICTION ──────── │  ← B 决定释放
       ├─ ReleaseFreedObject()                 │
       │   → 从 pinned_objects_ 移除           │
       │   → FreeObjects() → plasma 驱逐       │
```

### 4.5 LocalObjectManager 管的最小元信息

```cpp
// src/ray/raylet/local_object_manager.h:214
struct LocalObjectInfo {
  rpc::Address owner_address_;           // 谁是 owner（用于订阅 eviction）
  bool is_freed_ = false;               // owner 是否已通知释放
  std::optional<ObjectID> generator_id_; // 动态返回的 generator
  size_t object_size_;                  // 大小（统计/spill 决策）
};
```

LocalObjectManager 只管 pin 相关的最小元信息，不管引用计数、borrower、位置信息。

---

## 5. ray.put() 完整流程

### 5.1 Worker 侧完整代码路径

```cpp
// src/ray/core_worker/core_worker.cc:971
Status CoreWorker::Put(const RayObject &object,
                       const std::vector<ObjectID> &contained_object_ids,
                       ObjectID *object_id) {
  // 1. 生成 ObjectID
  *object_id = ObjectID::FromIndex(worker_context_->GetCurrentInternalTaskId(),
                                   worker_context_->GetNextPutIndex());

  // 2. 注册到 ReferenceCounter（owner = 自己）
  reference_counter_->AddOwnedObject(*object_id,
                                     contained_object_ids,
                                     rpc_address_,          // owner = self
                                     CurrentCallSite(),
                                     object.GetSize(),
                                     LineageReconstructionEligibility::INELIGIBLE_PUT,
                                     /*add_local_ref=*/true,
                                     NodeID::FromBinary(rpc_address_.node_id()));

  // 3. 写入 Plasma 并 Pin
  auto status = Put(object, contained_object_ids, *object_id, /*pin_object=*/true);
  if (!status.ok()) {
    RemoveLocalReference(*object_id);
  }
  return status;
}
```

### 5.2 AddOwnedObjectInternal 详细逻辑

```cpp
// src/ray/core_worker/reference_counter.cc:346
bool ReferenceCounter::AddOwnedObjectInternal(
    const ObjectID &object_id,
    const std::vector<ObjectID> &inner_ids,
    const rpc::Address &owner_address,
    const std::string &call_site,
    const int64_t object_size,
    LineageReconstructionEligibility lineage_eligibility,
    bool add_local_ref,
    const std::optional<NodeID> &pinned_at_node_id, ...) {

  // 1. 去重检查
  if (object_id_refs_.contains(object_id)) return false;

  // 2. 增加计数器
  if (ObjectID::IsActorID(object_id)) num_actors_owned_by_us_++;
  else num_objects_owned_by_us_++;

  // 3. 构造并插入 Reference
  auto it = object_id_refs_
                .emplace(object_id,
                         Reference(owner_address, call_site, object_size,
                                   lineage_eligibility, pinned_at_node_id, ...))
                .first;
  // 设置关键字段:
  //   owned_by_us_ = true
  //   owner_address_ = rpc_address_ (self)
  //   pinned_at_node_id_ = 本节点
  //   pending_creation_ = false (因为 pinned_at_node_id 有值)

  // 4. 注册嵌套对象
  if (!inner_ids.empty()) {
    AddNestedObjectIdsInternal(object_id, inner_ids, rpc_address_);
  }

  // 5. 立即将 pin 位置加入 locations
  if (pinned_at_node_id.has_value()) {
    AddObjectLocationInternal(it, pinned_at_node_id.value());
  }

  // 6. 加入可重建列表
  reconstructable_owned_objects_.emplace_back(object_id);

  // 7. 增加 Python 本地引用
  if (add_local_ref) {
    it->second.local_ref_count++;
  }

  // 8. 更新计数器
  UpdateOwnedObjectCounters(object_id, it->second, /*decrement=*/false);
  return true;
}
```

### 5.3 PutInLocalPlasmaStore 详细逻辑

```cpp
// src/ray/core_worker/core_worker.cc:992
Status CoreWorker::PutInLocalPlasmaStore(const RayObject &object,
                                          const ObjectID &object_id,
                                          bool pin_object) {
  // 1. 写入 Plasma Store
  RAY_RETURN_NOT_OK(plasma_store_provider_->Put(object, object_id, ...));

  if (pin_object) {
    // 2. 异步 Pin（发 RPC 给本地 raylet）
    local_raylet_rpc_client_->PinObjectIDs(
        rpc_address_, {object_id}, /*generator_id=*/ObjectID::Nil(),
        [this, object_id](const Status &status, ...) {
          if (!status.ok()) { return; }
          // 3. Pin 确认后才 Release（防竞态）
          if (!plasma_store_provider_->Release(object_id).ok()) {
            RAY_LOG(ERROR) << "Failed to release object";
          }
        });
  }

  // 4. Memory Store 放 OBJECT_IN_PLASMA 哨兵
  memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
                     object_id,
                     reference_counter_->HasReference(object_id));
  return Status::OK();
}
```

### 5.4 PlasmaStoreProvider::Put

```cpp
// src/ray/core_worker/store_provider/plasma_store_provider.cc:98
Status CoreWorkerPlasmaStoreProvider::Put(const RayObject &object,
                                          const ObjectID &object_id,
                                          const rpc::Address &owner_address, ...) {
  std::shared_ptr<Buffer> data;
  // 1. Create：分配共享内存
  RAY_RETURN_NOT_OK(Create(object.GetMetadata(),
                           object.HasData() ? object.GetData()->Size() : 0,
                           object_id, owner_address, &data, /*created_by_worker=*/true));
  // 2. 写入数据
  if (data != nullptr && object.HasData()) {
    memcpy(data->Data(), object.GetData()->Data(), object.GetData()->Size());
  }
  // 3. Seal：使对象可读
  RAY_RETURN_NOT_OK(Seal(object_id));
  return Status::OK();
}
```

### 5.5 完整流程图

```
Python: ray.put(value)
  │
  ▼
CoreWorker::Put()
  ├─ 1. ObjectID::FromIndex(task_id, put_index)
  ├─ 2. AddOwnedObject() → ReferenceCounter
  │     └─ AddOwnedObjectInternal()
  │         ├─ object_id_refs_.emplace(id, Reference(...))
  │         │   owned_by_us_=true, owner_address_=self
  │         │   pinned_at_node_id_=本节点, local_ref_count++
  │         │   locations.insert(本节点)
  │         └─ PushToLocationSubscribers()
  │
  ├─ 3. PlasmaStoreProvider::Put()
  │     ├─ Create() → 分配 plasma 内存
  │     ├─ memcpy() → 写入数据
  │     └─ Seal() → 对象可读，触发 add_object_callback_
  │
  ├─ 4. PinObjectIDs RPC → 本地 raylet
  │     └─ Raylet::HandlePinObjectIDs
  │         ├─ GetObjectsFromPlasma()
  │         └─ PinObjectsAndWaitForFree()
  │             ├─ pinned_objects_[id] = RayObject
  │             ├─ local_objects_[id] = LocalObjectInfo{owner_address_}
  │             └─ 订阅 Owner 的 WORKER_OBJECT_EVICTION 通道
  │                 → ProcessSubscribeForObjectEviction()
  │                 → AddObjectOutOfScopeOrFreedCallback(unpin_object)
  │
  ├─ 5. [Pin 回复后] Release() → 释放 worker 的 plasma 引用
  │
  └─ 6. MemoryStore::Put(OBJECT_IN_PLASMA) → 标记对象在 plasma 中
```

---

## 6. 跨 owner 的 ray.put() 流程

当使用 `ray.put(value, _owner=xxx)` 时，putting worker 不是 owner：

```
Putting Worker (Borrower)              Designated Owner Worker
    │                                      │
    ├─ AddLocalReference(object_id)        │
    ├─ AddBorrowedObject(id, Nil,          │
    │     owner_address,                   │
    │     foreign_owner_already_monitoring=true)
    │                                      │
    ├─ AssignObjectOwner RPC ─────────────>│
    │  (object_id, borrower_addr,          │
    │   contained_ids, object_size)         │
    │                                      ├─ AddOwnedObject(id, ...,
    │                                      │     add_local_ref=false,
    │                                      │     pinned_at_node_id=borrower节点)
    │                                      │   → owned_by_us_=true
    │                                      │   → locations.insert(borrower节点)
    │                                      │
    │                                      ├─ AddBorrowerAddress(id, borrower_addr)
    │                                      │   → borrow_info.borrowers.insert(borrower)
    │                                      │   → WaitForRefRemoved()
    │                                      │     订阅 borrower 的 REF_REMOVED 通道
    │                                      │
    │ <──── reply OK ──────────────────────│
    │                                      │
    ├─ Plasma Create(owner_addr=real_owner) │
    ├─ PinObjectIDs(owner_addr=real_owner)  │
    │   → Raylet 订阅 real owner 的 EVICTION│
```

### 6.1 HandleAssignObjectOwner

```cpp
// src/ray/core_worker/core_worker.cc:4505
void CoreWorker::HandleAssignObjectOwner(
    rpc::AssignObjectOwnerRequest request, ...) {
  const auto object_id = ObjectID::FromBinary(request.object_id());
  const auto owner_address = request.owner_address();

  reference_counter_->AddOwnedObject(
      object_id, contained_ids,
      rpc_address_,               // self = owner
      call_site, object_size,
      INELIGIBLE_PUT,
      /*add_local_ref=*/false,    // ← 关键区别：owner 不持 Python 引用
      pinned_at_node_id = borrower的节点);

  reference_counter_->AddBorrowerAddress(object_id, borrower_address);
  // → 添加 borrower → WaitForRefRemoved() 订阅 borrower 的 REF_REMOVED

  memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA), object_id, ...);
}
```

---

## 7. 任务返回对象完整流程

### 7.1 Phase 1：Owner 提交任务（先注册返回 ID）

```cpp
// src/ray/core_worker/task_manager.cc:237
void TaskManager::AddPendingTask(...) {
  // 对每个返回 ID 注册 owned ref（任务执行前！）
  for (size_t i = 0; i < spec.NumReturns(); i++) {
    reference_counter_.AddOwnedObject(
        spec.ReturnId(i), {}, caller_address,
        call_site, /*object_size=*/-1,
        lineage_eligibility,
        /*add_local_ref=*/true,    // Python 引用
        /*pinned_at_node_id=*/{}); // 未知，pending
  }

  // 追踪任务依赖
  reference_counter_.UpdateSubmittedTaskReferences(return_ids, task_deps);
  // → return_id: pending_creation_ = true
  // → arg_id: submitted_task_ref_count++, lineage_ref_count++
}
```

### 7.2 Phase 2：执行 Worker 写入 plasma + pin

```cpp
// src/ray/core_worker/core_worker.cc:1190
Status CoreWorker::SealExisting(const ObjectID &object_id,
                                bool pin_object,
                                const ObjectID &generator_id,
                                const std::unique_ptr<rpc::Address> &owner_address) {
  // 1. Seal：使 plasma 对象可读
  RAY_RETURN_NOT_OK(plasma_store_provider_->Seal(object_id));

  if (pin_object) {
    // 2. Pin：异步发 RPC（owner_address 是任务提交方，不是执行方）
    local_raylet_rpc_client_->PinObjectIDs(
        owner_address != nullptr ? *owner_address : rpc_address_,
        {object_id}, generator_id,
        [this, object_id](const Status &status, ...) {
          if (!status.ok()) { return; }
          // 3. Pin 确认后才 Release
          plasma_store_provider_->Release(object_id);
        });
  }

  // 4. Memory Store 哨兵
  memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA), object_id, ...);
  return Status::OK();
}
```

### 7.3 Phase 3：Owner 从 PushTaskReply 获知位置

```cpp
// src/ray/core_worker/task_manager.cc:550
void TaskManager::HandleTaskReturn(const ObjectID &object_id,
                                   const rpc::ReturnObject &return_object,
                                   const NodeID &worker_node_id, ...) {
  reference_counter_.UpdateObjectSize(object_id, return_object.size());

  if (return_object.in_plasma()) {
    // ★ Owner 在这里获知对象物理位置
    reference_counter_.UpdateObjectPinnedAtRaylet(object_id, worker_node_id);
    // → Reference.pinned_at_node_id_ = 执行节点 NodeID
    // → Reference.locations 中也会包含此节点（后续 ReportObjectAdded 补充）

    in_memory_store_.Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA), object_id, ...);
  } else {
    // 内联返回
    if (store_in_plasma) {
      put_in_local_plasma_callback_(...);  // 提升到 owner 本地 plasma
    } else {
      in_memory_store_.Put(object, object_id, ...);  // 直接存 memory store
    }
  }
}
```

### 7.4 UpdateObjectPinnedAtRaylet

```cpp
// src/ray/core_worker/reference_counter.cc:917
void ReferenceCounter::UpdateObjectPinnedAtRaylet(const ObjectID &object_id,
                                                   const NodeID &node_id) {
  absl::MutexLock lock(&mutex_);
  auto it = object_id_refs_.find(object_id);
  RAY_CHECK(it->second.owned_by_us_);  // 只有 owner 追踪此信息
  if (!it->second.OutOfScope(...)) {
    it->second.pinned_at_node_id_ = node_id;  // 设置主副本位置
  }
}
```

### 7.5 与 ray.put() 的关键区别

| | `ray.put()` | 任务返回 |
|---|---|---|
| 注册时机 | 调用时立即注册 | **提交任务时**就注册（执行前） |
| `pinned_at_node_id_` | 注册时就设置（本节点） | 注册时为空，**完成时才设置**（执行节点） |
| `pending_creation_` | `false` | **`true`**（提交时标记，完成时清除） |
| 谁调 PinObjectIDs | Owner 自己调本地 raylet | **执行 Worker** 调本地 raylet（传 owner_address） |
| 对象物理位置 | Owner 的本地 plasma | **执行节点的 plasma**（除非小对象被提升） |
| Owner 怎么知道位置 | 主动设置的 | **PushTaskReply 的 `in_plasma=true`** → `UpdateObjectPinnedAtRaylet()` |

### 7.6 动态 Generator 返回的区别

| | 普通返回 | Generator 返回 |
|---|---|---|
| 返回 ID 何时确定 | 提交时已知 | **执行时才分配** |
| Owner 注册 | `AddPendingTask` 中 `AddOwnedObject` | 执行 Worker 通过 RPC 报告后，Owner 调 `AddDynamicReturn` |
| 执行 Worker 侧引用 | 无特殊引用 | `AddBorrowedObject`（视为借用） |
| 报告机制 | `PushTaskReply` 一次性 | Streaming：`ReportGeneratorItemReturns` RPC 逐条 |

---

## 8. 任务参数 ObjectRef 在执行方的注册机制

### 8.1 提交方：参数 ref 的序列化

```python
# python/ray/_raylet.pyx:821
if isinstance(arg, ObjectRef):
    c_arg = (<ObjectRef>arg).native()
    op_status = CCoreWorkerProcess.GetCoreWorker().GetOwnerAddress(
            c_arg, &c_owner_address)
    args_vector.push_back(
        unique_ptr[CTaskArg](new CTaskArgByReference(
            c_arg,                               # ObjectID
            c_owner_address,                     # Owner 的 RPC 地址
            arg.call_site(),
            move(c_tensor_transport))))
```

### 8.2 提交方 AddPendingTask 中的参数追踪

```cpp
// src/ray/core_worker/task_manager.cc:244-334
void TaskManager::AddPendingTask(...) {
  for (size_t i = 0; i < spec.NumArgs(); i++) {
    if (spec.ArgByRef(i)) {
      task_deps.push_back(spec.ArgObjectId(i));    // by-ref 参数
    } else {
      for (const auto &inlined_ref : spec.ArgInlinedRefs(i)) {
        task_deps.push_back(ObjectID::FromBinary(inlined_ref.object_id())); // 嵌套 ref
      }
    }
  }
  reference_counter_.UpdateSubmittedTaskReferences(return_ids, task_deps);
  // → 对每个 arg_id: submitted_task_ref_count++（防 GC）
  //                 lineage_ref_count++
}
```

**注意**：提交方的参数 ref 条目**早就存在**于 ReferenceCounter（Python ObjectRef 持有时就注册了），SubmitTask 只是多加 `submitted_task_ref_count`。

### 8.3 执行方：GetAndPinArgsForExecutor — 核心注册点

```cpp
// src/ray/core_worker/core_worker.cc:3247-3340
Status CoreWorker::GetAndPinArgsForExecutor(
    const TaskSpecification &task,
    std::vector<std::shared_ptr<RayObject>> *args,
    std::vector<rpc::ObjectReference> *arg_refs,
    std::vector<ObjectID> *borrowed_ids) {

  for (size_t i = 0; i < task.NumArgs(); i++) {
    if (task.ArgByRef(i)) {
      // === by-reference 参数 ===
      const auto &arg_ref = task.ArgRef(i);
      const auto arg_id = ObjectID::FromBinary(arg_ref.object_id());

      // ★ 步骤1：AddLocalReference — 新建条目，local_ref_count=1
      reference_counter_->AddLocalReference(arg_id, task.CallSiteString());

      // ★ 步骤2：AddBorrowedObject — 记录 owner，标记为 borrowed
      reference_counter_->AddBorrowedObject(
          arg_id, ObjectID::Nil(), task.ArgRef(i).owner_address());

      borrowed_ids->push_back(arg_id);

      // ★ 步骤3：Memory Store 放 OBJECT_IN_PLASMA 哨兵
      memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
                         task.ArgObjectId(i), ...);
    } else {
      // === by-value 参数中的嵌套 ref ===
      args->push_back(std::make_shared<RayObject>(..., task.ArgInlinedRefs(i), ...));
      for (const auto &inlined_ref : task.ArgInlinedRefs(i)) {
        const auto inlined_id = ObjectID::FromBinary(inlined_ref.object_id());
        // ★ AddLocalReference（嵌套 ref）
        reference_counter_->AddLocalReference(inlined_id, task.CallSiteString());
        borrowed_ids->push_back(inlined_id);
        // 注意："ownership info must be added later via AddBorrowedObject"
        // 在 Python 反序列化时补充 AddBorrowedObject
      }
    }
  }

  // 从 plasma 取实际值
  auto owner_addresses = reference_counter_->GetOwnerAddresses(object_ids);
  RAY_RETURN_NOT_OK(
      plasma_store_provider_->Get(object_ids, owner_addresses, -1, &result_map));
}
```

### 8.4 AddLocalReference 代码

```cpp
// src/ray/core_worker/reference_counter.cc:418
void ReferenceCounter::AddLocalReference(const ObjectID &object_id,
                                         const string &call_site) {
  auto it = object_id_refs_.find(object_id);
  if (it == object_id_refs_.end()) {
    // NOTE: ownership info must be added later via AddBorrowedObject
    it = object_id_refs_.emplace(object_id, Reference(call_site, -1)).first;
  }
  bool was_in_use = it->second.RefCount() > 0;
  it->second.local_ref_count++;      // local_ref_count++
  if (!was_in_use && it->second.RefCount() > 0) {
    SetNestedRefInUseRecursive(it);  // 通知 owner 嵌套 ref 在使用中
  }
}
```

### 8.5 AddBorrowedObjectInternal 代码

```cpp
// src/ray/core_worker/reference_counter.cc:124
bool ReferenceCounter::AddBorrowedObjectInternal(
    const ObjectID &object_id,
    const ObjectID &outer_id,
    const rpc::Address &owner_address, ...) {
  auto it = object_id_refs_.find(object_id);
  if (it == object_id_refs_.end()) {
    it = object_id_refs_.emplace(object_id, Reference()).first;
  }
  it->second.owner_address_ = owner_address;    // 记录 owner
  it->second.foreign_owner_already_monitoring |= foreign_owner_already_monitoring;

  if (!outer_id.IsNil()) {
    // 设置嵌套关系
    it->second.mutable_nested()->contained_in_borrowed_ids.insert(outer_id);
    outer_it->second.mutable_nested()->contains.insert(object_id);
    if (it->second.RefCount() > 0) {
      SetNestedRefInUseRecursive(it);
    }
  }

  if (it->second.RefCount() == 0) {
    DeleteReferenceInternal(it, nullptr);  // 无引用则清理
  }
  return true;
}
```

### 8.6 Python 层嵌套 ref 的补充注册

```python
# python/ray/includes/object_ref.pxi:~67
class ObjectRef:
    def __init__(self, id, owner_addr="", ..., skip_adding_local_ref=False, ...):
        if hasattr(worker, "core_worker"):
            if not skip_adding_local_ref:
                worker.core_worker.add_object_ref_reference(self)
                # → C++ AddLocalReference（第二次递增 local_ref_count）
            self.in_core_worker = True
```

嵌套 ObjectRef 反序列化时：

```python
# python/ray/_private/serialization.py:67
def _object_ref_deserializer(binary, call_site, owner_address, ...):
    obj_ref = ray.ObjectRef(binary, owner_address, call_site, ...)
    if owner_address:
        outer_id = context.get_outer_object_ref()
        worker.core_worker.deserialize_and_register_object_ref(
            obj_ref.binary(), outer_id, owner_address, ...)
        # → C++ RegisterOwnershipInfoAndResolveFuture
        # → AddBorrowedObject(object_id, outer_id, owner_address)
```

```cpp
// src/ray/core_worker/core_worker.cc:949
void CoreWorker::RegisterOwnershipInfoAndResolveFuture(
    const ObjectID &object_id,
    const ObjectID &outer_object_id,
    const rpc::Address &owner_address, ...) {
  reference_counter_->AddBorrowedObject(object_id, outer_object_id, owner_address);
  future_resolver_->ResolveFutureAsync(object_id, owner_address);
}
```

### 8.7 任务完成后清理 borrowed ref

```cpp
// src/ray/core_worker/core_worker.cc:2876
if (!borrowed_ids.empty()) {
  reference_counter_->PopAndClearLocalBorrowers(borrowed_ids, borrowed_refs, &deleted);
}

// src/ray/core_worker/reference_counter.cc:1028
void ReferenceCounter::PopAndClearLocalBorrowers(
    const vector<ObjectID> &borrowed_ids, ReferenceTableProto *proto, ...) {
  for (const auto &borrowed_id : borrowed_ids) {
    // 清理 borrower 信息，打包回传给 owner
    GetAndClearLocalBorrowersInternal(borrowed_id, ..., /*deduct_local_ref=*/true, proto, ...);
  }
  for (const auto &borrowed_id : borrowed_ids) {
    // 减去 GetAndPinArgsForExecutor 中加的 local_ref_count
    auto it = object_id_refs_.find(borrowed_id);
    it->second.local_ref_count--;
    if (it->second.RefCount() == 0) {
      DeleteReferenceInternal(it, deleted);
      // → 如果 publish_ref_removed_=true → PublishRefRemovedInternal → 通知 owner
    }
  }
}
```

### 8.8 执行方 ray.get() 的路径

```
执行 Worker ray.get(arg_ref)
  → 自己的 ReferenceCounter::HasOwner() → 有（刚注册的 borrowed ref）
  → 自己的 ReferenceCounter::GetOwnerAddresses() → 拿到 owner 地址
  → 自己的 MemoryStore → OBJECT_IN_PLASMA 哨兵
  → 自己的 PlasmaStoreProvider → 查本地 plasma
    → 本地有（参数已被 raylet 拉到本地）→ 直接返回
    → 本地没有 → 自己的 raylet Pull → 问 owner 位置 → 远端 Push 过来
```

---

## 9. SubmitTask 参数注册 vs 执行方参数注册的区别

### 9.1 核心区别

| | 提交方（SubmitTask 时） | 执行方（GetAndPinArgsForExecutor） |
|---|---|---|
| 条目是否已存在 | **已存在**（Python ObjectRef 持有时就注册了） | **不存在**，需新建 |
| 增加什么计数 | `submitted_task_ref_count++` | `local_ref_count++`（新建时） |
| 是否记录 owner | 不需要（本来就是自己的或早就 borrowed） | 需要 `AddBorrowedObject` 记录 owner |
| 计数用途 | 防止参数在任务运行期间被 GC | 让执行方能 `ray.get()`，能被 owner 追踪 |
| 任务完成后 | `submitted_task_ref_count--`（`RemoveSubmittedTaskReferences`） | `local_ref_count--`（`PopAndClearLocalBorrowers`） |

### 9.2 为什么不一样

提交方能拿到 ObjectRef 传给 SubmitTask，说明 Python 侧已经持有这个 ref，**ReferenceCounter 早就注册过了**。SubmitTask 只是多加一个"锁"防止参数被 GC。

执行方是**第一次见到**这个 ObjectID，之前完全没有条目，必须从零注册。

### 9.3 同一参数 ref 在三方 ReferenceCounter 中的状态

```
ObjectRef X 作为任务参数
  │
  ├─ 提交方 Worker (Owner 或 Borrower):
  │   条目早就存在，SubmitTask 时:
  │     submitted_task_ref_count++ (任务运行期间)
  │     lineage_ref_count++       (lineage 引用)
  │
  ├─ 执行方 Worker (Borrower):
  │   条目在 GetAndPinArgsForExecutor 时新建:
  │     AddLocalReference → local_ref_count=1
  │     AddBorrowedObject → owner_address_=提交方/真正owner
  │   任务完成后:
  │     PopAndClearLocalBorrowers → local_ref_count--
  │     如果 RefCount()==0 → publish_ref_removed → 通知 owner
  │
  └─ Owner Worker:
      如果 owner ≠ 提交方:
        owners 的 ReferenceCounter 中有完整信息
        borrowers 集合包含提交方和执行方
```

---

## 10. 四大组件关系：ReferenceCounter / PlasmaStoreProvider / MemoryStore / ObjectManager

### 10.1 各自职责

| 组件 | 所属 | 职责 | 存什么 |
|---|---|---|---|
| **ReferenceCounter** | CoreWorker | 生命周期控制平面：引用计数、所有权、位置追踪 | 元数据（`locations`, `pinned_at_node_id_`, `borrowers`, `ref_count`） |
| **PlasmaStoreProvider** | CoreWorker | Plasma 客户端：Create/Seal/Get/Release | 无（是 plasma store 的客户端接口） |
| **MemoryStore** | CoreWorker | 进程内小对象存储 + `OBJECT_IN_PLASMA` 标记 | 小对象内联数据 或 哨兵值 |
| **ObjectManager** | Raylet | 跨节点传输引擎：Push/Pull | 传输中的 chunk 状态、push/pull 请求队列 |

### 10.2 读取路径（ray.get()）

```
CoreWorker::Get()
  │
  ├─ 1. 查 MemoryStore
  │     ├─ 命中内联数据 → 直接返回（小对象）
  │     └─ 命中 OBJECT_IN_PLASMA → 转 PlasmaStoreProvider
  │
  ├─ 2. PlasmaStoreProvider::Get()
  │     └─ 从 plasma 共享内存读取 → 返回
  │
  └─ 3. 都没命中 → 触发拉取
        └─ ReferenceCounter 查 owner → 告知哪个节点有
           → ObjectManager::Pull() → 跨节点传输
             → 写入本地 plasma → PlasmaStoreProvider 可读
```

### 10.3 写入路径（ray.put()）

```
CoreWorker::Put()
  ├─ PlasmaStoreProvider::Create() + 写入 + Seal()  → 数据在 plasma
  ├─ MemoryStore::Put(OBJECT_IN_PLASMA)             → 哨兵标记
  ├─ ReferenceCounter::AddOwnedObject()              → 记录元数据
  └─ PinObjectIDs → Raylet pin + 订阅 eviction
```

### 10.4 释放路径

```
Python 引用释放
  → ReferenceCounter::RemoveLocalReference()
    → RefCount()==0 且无 borrower
      → OnObjectOutOfScopeOrFreed()
        → 触发 eviction callback
          → 发布 WORKER_OBJECT_EVICTION
            → Raylet::ReleaseFreedObject()
              → ObjectManager::FreeObjects()
                → Plasma 驱逐
              → MemoryStore 哨兵自然过期
```

### 10.5 一句话关系

- **ReferenceCounter**：决定"**该不该留**"（控制平面，只有元数据）
- **PlasmaStoreProvider**：决定"**怎么读写大对象**"（CoreWorker 到 Plasma 的客户端接口）
- **MemoryStore**：决定"**小对象放哪**" + "**大对象在 plasma 的索引**"（哨兵值）
- **ObjectManager**：决定"**怎么搬**"（跨节点传输引擎，不关心生命周期）

PlasmaStoreProvider 和 MemoryStore 是**存储层**（存数据本身），ReferenceCounter 是**控制层**（存元数据），ObjectManager 是**传输层**（搬数据）。三者通过 CoreWorker 串联。

---

## 11. 对象获取判断机制：逐层回退判断链

### 11.1 完整判断链

```
ray.get(obj_id)
│
├─ 1. CoreWorker 层
│   ├─ ReferenceCounter::HasOwner() → 连 owner 都没有？报错 ObjectUnknownOwner
│   ├─ MemoryStore::Get() → 有内联数据？直接返回
│   │                    → 是 OBJECT_IN_PLASMA 哨兵？转 plasma
│   │                    → 啥都没有？阻塞等
│   └─ PlasmaStoreProvider::Get()
│       ├─ 本地 plasma 里有没有？→ store_client_->Get() 直接查
│       └─ 没有 → IPC 通知 raylet 去拉
│
├─ 2. Raylet 层
│   └─ LeaseDependencyManager::StartGetRequest()
│       └─ ObjectManager::Pull()
│           └─ PullManager::Pull()
│               ├─ 本地已有？→ 标记 local，完成
│               └─ 没有 → 需要知道在哪 → 订阅 owner 的位置
│
├─ 3. 位置发现层
│   └─ OwnershipBasedObjectDirectory::SubscribeObjectLocations()
│       └─ 问 Owner 的 WORKER_OBJECT_LOCATIONS_CHANNEL
│           Owner 回复：{node_ids[], spilled_url, pending_creation, object_size}
│
└─ 4. PullManager 拿到位置后的决策
    ├─ pending_creation=true → 对象还在创建中，等
    ├─ node_ids 非空 → 从随机节点 Pull
    ├─ node_ids 空，有 spilled_node_id → 向 spilled 节点 Pull（它先 restore）
    ├─ node_ids 空，有 spilled_url（外部存储）→ 本地 restore from URL
    ├─ node_ids 空，无 spill，pending=false → 设超时，超时后报 OBJECT_FETCH_TIMED_OUT
    └─ ref_removed=true → 对象已被删除，报 OBJECT_DELETED
```

### 11.2 CoreWorker::GetObjects 代码

```cpp
// src/ray/core_worker/core_worker.cc:1516
Status CoreWorker::GetObjects(const std::vector<ObjectID> &ids, ...) {
  // Step 1: 检查 owner 是否存在
  StatusSet<StatusT::NotFound> objects_have_owners = reference_counter_->HasOwner(ids);
  // 如果没有 owner → ObjectUnknownOwner

  // Step 2: 查 Memory Store
  memory_store_->Get(memory_object_ids, timeout_ms, *worker_context_, &result_map, ...);
  // 命中内联数据 → 直接返回
  // 命中 OBJECT_IN_PLASMA → 转步骤 3

  // Step 3: 查 Plasma Store
  plasma_store_provider_->Get(plasma_object_ids, owner_addresses, local_timeout_ms, &result_map);
}
```

### 11.3 PlasmaStoreProvider::Get 代码

```cpp
// src/ray/core_worker/store_provider/plasma_store_provider.cc:~200
Status CoreWorkerPlasmaStoreProvider::Get(...) {
  // 1. 发 IPC 通知 raylet 开始拉取
  raylet_ipc_client_->AsyncGetObjects(batch_ids, batch_owner_addresses, ...);
  // 2. 立即查本地 plasma（timeout=0）
  GetObjectsFromPlasmaStore(..., /*timeout=*/0, ...);
  // 3. 没拿到 → 进入轮询循环
  while (!all_found && !timeout_expired) {
    GetObjectsFromPlasmaStore(..., remaining_timeout, ...);
  }
}
```

### 11.4 PullManager::TryToMakeObjectLocal 决策树

```cpp
// src/ray/object_manager/pull_manager.cc:446-503
bool PullManager::TryToMakeObjectLocal(const ObjectID &object_id) {
  // 1. 已在本地？
  if (object_is_local_(object_id)) return true;

  // 2. 不再需要？
  if (!active_object_pull_requests_.count(object_id)) return false;

  // 3. 重试定时器未到？
  if (next_pull_time > now) return false;

  // 4. 从随机位置拉取
  if (PullFromRandomLocation(object_id)) return true;
  //   → client_locations 非空 → 随机选节点 send_pull_request_
  //   → client_locations 空 + spilled_node_id → 向 spilled 节点 send_pull_request_

  // 5. 本地 restore（从外部存储）
  auto locally_spilled_url = get_locally_spilled_object_url_(object_id);
  if (!locally_spilled_url.empty()) {
    restore_spilled_object_(object_id, size, url, callback);
    return true;
  }
  if (!spilled_url.empty() && spilled_node_id.IsNil()) {
    // 外部存储（如 S3），不绑定特定节点
    restore_spilled_object_(object_id, size, spilled_url, callback);
    return true;
  }

  // 6. 都没有 → 设超时定时器
  RAY_CHECK(!request.pending_object_creation);
  if (expiration_time_seconds == 0) {
    expiration_time_seconds = now + fetch_fail_timeout_milliseconds;
  }
  if (expired) {
    fail_pull_request_(object_id, OBJECT_FETCH_TIMED_OUT);
  }
  return false;
}
```

### 11.5 Owner 的 ReferenceCounter 对象状态分类

```cpp
// src/ray/core_worker/reference_counter.cc:72-96
// Owned object 互斥状态：
// 1. pending_creation:  pending_creation_ == true
// 2. in_memory:         !pending && !pinned_at_node_id_ && !spilled
// 3. in_plasma:         !pending && pinned_at_node_id_ && !spilled
// 4. spilled_and_pinned: spilled && pinned_at_node_id_
// 5. spilled_only:      spilled && !pinned_at_node_id_
```

### 11.6 各层"在不在"判断依据

| 层 | 怎么判断在不在 | 数据源 |
|---|---|---|
| **MemoryStore** | 查 `objects_` map 有没有 | 本地 heap 存储 |
| **PlasmaStoreProvider** | `store_client_->Get()` | 本地共享内存 |
| **PullManager** | `object_is_local_()` 回调 | Raylet 的 local_objects 集合 |
| **Owner ReferenceCounter** | `locations` + `pinned_at_node_id_` + `spilled_url` | 所有位置信息权威 |
| **OwnershipBasedObjectDirectory** | 订阅 owner 的 pubsub 更新 | Owner 推送 |

判断"在不在"的权威是 **Owner 的 ReferenceCounter**。Borrower 和 Raylet 自己都不维护位置信息，它们通过订阅 Owner 的 `WORKER_OBJECT_LOCATIONS_CHANNEL` 被动接收更新。

### 11.7 为什么 ray.get() 时 ReferenceCounter 一定有条目

Python 的 ObjectRef 持有一个 local_ref_count，ObjectRef 存在 ⟺ ReferenceCounter 有条目。ObjectRef 的产生只有三条路径：

| 路径 | 何时注册到 ReferenceCounter |
|---|---|
| `ray.put()` 创建 | `AddOwnedObject()` — 创建时立即注册 |
| `SubmitTask()` 提交任务 | `AddOwnedObject(return_id)` — 提交时就注册了返回 ID |
| 收到别人传来的 ObjectRef（作为参数/返回值） | `AddLocalReference()` + `AddBorrowedObject()` — 反序列化 ObjectRef 时自动注册 |

核心逻辑：Python 还没拿到 ObjectRef 就不会调 `ray.get()`；拿到了就必然已经注册过。

### 11.8 对象不在 owner 节点时的控制权

**Borrower 的 CoreWorker 不控制对象物理在哪，只负责拿。**

- 对象在哪个节点 → **Owner** 的 `ReferenceCounter.locations`
- 主副本 pin 在哪 → **Owner** 的 `ReferenceCounter.pinned_at_node_id_`
- 何时可以释放 → **Owner** 决定（ref=0 且 borrower 都释放）
- 通知 raylet 释放 → **Owner** 发布 `WORKER_OBJECT_EVICTION`
- 物理存储/驱逐 → **Raylet** 的 LocalObjectManager 执行
- 跨节点搬运 → **Raylet** 的 ObjectManager 执行
- **我要拿对象** → **Borrower** 发请求，不关心也不控制物理位置

Borrower 的角色：**我要用 → 去拿 → 用完 → 告诉 owner**。物理位置的控制权完全在 owner + raylet 侧。

---

## 12. 关键源文件索引

| 文件 | 核心功能 |
|---|---|
| `src/ray/core_worker/reference_counter.h` | Reference 结构、BorrowInfo、NestedReferenceCount |
| `src/ray/core_worker/reference_counter.cc` | AddOwnedObject, AddBorrowedObject, AddLocalReference, AddObjectLocation, RemoveObjectLocation, UpdateObjectPinnedAtRaylet, OnObjectOutOfScopeOrFreed, PublishRefRemovedInternal, CleanupBorrowersOnRefRemoved, WaitForRefRemoved, PopAndClearLocalBorrowers |
| `src/ray/core_worker/core_worker.cc` | Put(), PutInLocalPlasmaStore(), SealExisting(), ExecuteTask(), GetAndPinArgsForExecutor(), HandleUpdateObjectLocationBatch(), AddObjectLocationOwner(), RemoveObjectLocationOwner(), ProcessSubscribeForObjectEviction(), HandleAssignObjectOwner(), RegisterOwnershipInfoAndResolveFuture(), Get(), GetObjects() |
| `src/ray/core_worker/task_manager.cc` | AddPendingTask(), HandleTaskReturn(), CompletePendingTask(), UpdateSubmittedTaskReferences(), OnTaskDependenciesInlined() |
| `src/ray/core_worker/store_provider/memory_store/memory_store.cc` | Put (不可覆写), Get |
| `src/ray/core_worker/store_provider/plasma_store_provider.cc` | Put, Create, Seal, Release, Get |
| `src/ray/object_manager/object_manager.cc` | HandleObjectAdded, HandleObjectDeleted, Push, Pull |
| `src/ray/object_manager/pull_manager.cc` | Pull(), OnLocationChange(), TryToMakeObjectLocal(), PinNewObjectIfNeeded() |
| `src/ray/object_manager/push_manager.cc` | 推送限流和 chunk 调度 |
| `src/ray/object_manager/ownership_object_directory.cc` | ReportObjectAdded, ReportObjectRemoved, ReportObjectSpilled, SubscribeObjectLocations, SendObjectLocationUpdateBatchIfNeeded |
| `src/ray/object_manager/ownership_object_directory.h` | location_buffers_, in_flight_requests_ 批量缓冲结构 |
| `src/ray/raylet/node_manager.cc` | HandlePinObjectIDs, HandleObjectLocal, HandleObjectMissing, HandleAsyncGetObjectsRequest, GetObjectsFromPlasma |
| `src/ray/raylet/local_object_manager.h` | LocalObjectInfo 结构 |
| `src/ray/raylet/local_object_manager.cc` | PinObjectsAndWaitForFree, ReleaseFreedObject, FlushFreeObjects |
| `src/ray/protobuf/core_worker.proto` | UpdateObjectLocationBatch RPC, ObjectLocationUpdate 消息 |
| `src/ray/protobuf/pubsub.proto` | WORKER_OBJECT_EVICTION=0, WORKER_REF_REMOVED_CHANNEL=1 |
| `python/ray/_raylet.pyx` | 参数序列化 (CTaskArgByReference), VectorToObjectRefs |
| `python/ray/_private/serialization.py` | _object_ref_deserializer 嵌套 ref 反序列化 |
| `python/ray/includes/object_ref.pxi` | ObjectRef.__init__ → add_object_ref_reference → AddLocalReference |
| `src/ray/core_worker/task_submission/dependency_resolver.cc` | LocalDependencyResolver, 依赖内联 |
| `src/ray/raylet/lease_dependency_manager.cc` | StartGetRequest, HandleObjectLocal, HandleObjectMissing |
