# Ray 对象生命周期机制详解

本文档详细分析 Ray 中任务返回值从写入到可用的完整生命周期，涵盖 SealReturnObject / SealExisting / Seal / Pin / HandleObjectAdded 五大核心环节，以及 Plasma Store 与 Memory Store 的互补协作关系。

---

## 目录

1. [总体架构概览](#1-总体架构概览)
2. [SealReturnObject：入口分流](#2-sealreturnobject入口分流)
3. [SealExisting：大值完整路径](#3-sealexisting大值完整路径)
4. [PlasmaClient::Seal → PlasmaStore::SealObjects：跨进程通信](#4-plasmaclientseal--plasmastoresealobjects跨进程通信)
5. [add_object_callback_ → HandleObjectAdded：回调触发链路](#5-add_object_callback_--handleobjectadded回调触发链路)
6. [HandleObjectLocal：依赖解阻塞](#6-handleobjectlocal依赖解阻塞)
7. [Pin 机制：防止 Plasma 对象被驱逐](#7-pin-机制防止-plasma-对象被驱逐)
8. [Memory Store 与 Plasma Store 互补关系](#8-memory-store-与-plasma-store-互补关系)
9. [小值路径 vs 大值路径完整对比](#9-小值路径-vs-大值路径完整对比)
10. [Push 与 unfulfilled_push_requests_ 机制](#10-push-与-unfulfilled_push_requests_-机制)
11. [MarkObjectSealed：唤醒等待中的 Get 请求](#11-markobjectsealed唤醒等待中的-get-请求)
12. [三种前端调用 SealReturnObject 的差异](#12-三种前端调用-sealreturnobject-的差异)
13. [对象释放与 Unpin 机制](#13-对象释放与-unpin-机制)
14. [关键源文件索引](#14-关键源文件索引)

---

## 1. 总体架构概览

Ray 的任务返回值根据大小走两条完全不同的路径：

```
任务执行完毕，产生返回值
       │
       ├─ 小值 (≤100KB) ──→ LocalMemoryBuffer ──→ 直接写入 Memory Store
       │                                         无 Seal / 无 Pin / 无 HandleObjectAdded
       │
       └─ 大值 (>100KB) ──→ PlasmaBuffer ──→ SealExisting 路径
                                               │
                                               ├─ ① plasma_store_provider_->Seal()
                                               │     → PlasmaClient::Seal() → Unix Socket
                                               │     → PlasmaStore::SealObjects()
                                               │       ├─ SealObject() 标记 sealed
                                               │       ├─ add_object_callback_() ★
                                               │       └─ MarkObjectSealed() 唤醒 Get
                                               │
                                               ├─ ② PinObjectIDs RPC → Raylet Pin
                                               │
                                               ├─ ③ plasma_store_provider_->Release()
                                               │     （Pin 确认后才 Release）
                                               │
                                               └─ ④ memory_store_->Put(OBJECT_IN_PLASMA)
                                                   在 Memory Store 放占位符
```

---

## 2. SealReturnObject：入口分流

### 2.1 SealReturnObject 代码

```cpp
// src/ray/core_worker/core_worker.cc:3011
Status CoreWorker::SealReturnObject(const ObjectID &return_id,
                                    const std::shared_ptr<RayObject> &return_object,
                                    const ObjectID &generator_id,
                                    const rpc::Address &owner_address) {
  RAY_CHECK(return_object);
  auto owner_address_ptr = std::make_unique<rpc::Address>(owner_address);

  if (return_object->GetData() != nullptr && return_object->GetData()->IsPlasmaBuffer()) {
    // 大值路径：PlasmaBuffer → SealExisting
    status = SealExisting(return_id, true, generator_id, owner_address_ptr);
    if (!status.ok()) {
      RAY_LOG(FATAL) << "Failed to seal object in store: " << status.message();
    }
  }
  // 小值路径：什么都不做，直接返回 OK
  return status;
}
```

### 2.2 分流判断：IsPlasmaBuffer()

关键在于 `return_object->GetData()->IsPlasmaBuffer()`：

| 返回值类型 | Buffer 类型 | IsPlasmaBuffer() | 走哪条路径 |
|---|---|---|---|
| 小值 (≤100KB) | `LocalMemoryBuffer` | `false` | 不调用 Seal，直接返回 OK |
| 大值 (>100KB) | `PlasmaBuffer` | `true` | 调用 `SealExisting` |

### 2.3 return_ptr 为 NULL 的场景

在 `_raylet.pyx` 的 `store_task_output` 中：

```
AllocateReturnObject(return_id, ...)
  │
  ├─ 小值 → 返回 LocalMemoryBuffer → return_ptr[0] != NULL
  │
  ├─ 大值，首次创建 → 返回 PlasmaBuffer → return_ptr[0] != NULL
  │
  └─ 大值，对象已存在（任务重试/推测执行）→ 返回 nullptr → return_ptr[0] == NULL
```

当 `return_ptr[0] == NULL` 时，走 `PinExistingReturnObject` 路径（直接 Pin 已有对象，不再 Seal）。

---

## 3. SealExisting：大值完整路径

### 3.1 完整代码

```cpp
// src/ray/core_worker/core_worker.cc:1191
Status CoreWorker::SealExisting(const ObjectID &object_id,
                                bool pin_object,
                                const ObjectID &generator_id,
                                const std::unique_ptr<rpc::Address> &owner_address) {
  // ① Seal：通知 Plasma Store 对象写入完毕，变可读
  RAY_RETURN_NOT_OK(plasma_store_provider_->Seal(object_id));

  if (pin_object) {
    // ② Pin：异步发 RPC 让 Raylet 持有 plasma buffer，防驱逐
    RAY_LOG(DEBUG) << "Pinning sealed object";
    local_raylet_rpc_client_->PinObjectIDs(
        owner_address != nullptr ? *owner_address : rpc_address_,
        {object_id},
        generator_id,
        [this, object_id](const Status &status, const rpc::PinObjectIDsReply &reply) {
          if (!status.ok()) {
            RAY_LOG(ERROR) << "Request to local raylet to pin object failed";
            return;
          }
          // ③ Release：Pin 确认后才 Release，避免 Pin 前被驱逐的竞态
          if (!plasma_store_provider_->Release(object_id).ok()) {
            RAY_LOG(ERROR) << "Failed to release object, might cause a leak in plasma.";
          }
        });
  } else {
    // 不 Pin：直接 Release，并释放引用计数
    RAY_RETURN_NOT_OK(plasma_store_provider_->Release(object_id));
    reference_counter_->FreePlasmaObjects({object_id});
  }

  // ④ Memory Store 占位符：让 Get() 能发现此对象
  memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
                     object_id,
                     reference_counter_->HasReference(object_id));
  return Status::OK();
}
```

### 3.2 四步操作的时序关系

```
Seal ──→ PinObjectIDs RPC (异步) ──→ [Raylet 回复] ──→ Release
 │                                                              │
 └─ 对象变 sealed，可被 Get                               └─ Worker 释放 plasma buffer 引用
 │                                                              │
 └─ 触发 add_object_callback_ ──→ HandleObjectAdded
                                   └─ Raylet 侧也开始 Pin

 Memory Store Put(OBJECT_IN_PLASMA) ── 在 Seal 之后、Release 之前
```

**关键时序**：Pin RPC 是异步的，但 Release 必须等 Pin 确认后。这是为了防止以下竞态：

```
若 Release 先于 Pin 执行：
  Worker Release → plasma ref_count=0 → LRU 可驱逐 → Raylet Pin 时对象已不在
```

### 3.3 SealOwned：另一种入口

```cpp
// src/ray/core_worker/core_worker.cc:1171
Status CoreWorker::SealOwned(const ObjectID &object_id,
                             bool pin_object,
                             const std::unique_ptr<rpc::Address> &owner_address) {
  auto status = SealExisting(object_id, pin_object, ObjectID::Nil(), owner_address);
  if (status.ok()) return status;
  // 失败时清理引用
  RemoveLocalReference(object_id);
  return status;
}
```

`SealOwned` 用于 `ray.put()` 场景，`generator_id` 固定为 `ObjectID::Nil()`。

---

## 4. PlasmaClient::Seal → PlasmaStore::SealObjects：跨进程通信

### 4.1 PlasmaClient::Seal

```cpp
// src/ray/object_manager/plasma/client.cc:569
Status PlasmaClient::Seal(const ObjectID &object_id) {
  std::lock_guard<std::recursive_mutex> guard(client_mutex_);

  // 检查客户端是否持有此对象的引用
  auto object_entry = objects_in_use_.find(object_id);
  if (object_entry == objects_in_use_.end()) {
    return Status::ObjectNotFound("Seal() called on an object without a reference to it");
  }
  if (object_entry->second->is_sealed) {
    return Status::ObjectAlreadySealed("Seal() called on an already sealed object");
  }

  object_entry->second->is_sealed = true;

  // 通过 Unix Socket 发送 PlasmaSealRequest
  RAY_RETURN_NOT_OK(SendSealRequest(store_conn_, object_id));

  // 等待 PlasmaSealReply（同步阻塞）
  std::vector<uint8_t> buffer;
  RAY_RETURN_NOT_OK(PlasmaReceive(store_conn_, MessageType::PlasmaSealReply, &buffer));
  ObjectID sealed_id;
  RAY_RETURN_NOT_OK(ReadSealReply(buffer.data(), buffer.size(), &sealed_id));
  RAY_CHECK_EQ(sealed_id, object_id);

  // Seal 成功后自动 Release（递减客户端侧的 ref_count）
  RAY_RETURN_NOT_OK(Release(object_id));
  return Status::OK();
}
```

注意：`PlasmaClient::Seal` 内部会自动调用 `Release`，这是客户端侧的 ref_count 管理。而 `SealExisting` 中的 `plasma_store_provider_->Release()` 是**服务端侧**的 Release，两者互不冲突。

### 4.2 PlasmaStore::SealObjects

```cpp
// src/ray/object_manager/plasma/store.cc:475 → 275
// ProcessClientMessage 中收到 PlasmaSealRequest 后：
case fb::MessageType::PlasmaSealRequest: {
  ObjectID object_id;
  ReadSealRequest(input, input_size, &object_id);
  SealObjects({object_id});                                    // ★ 入口
  RAY_RETURN_NOT_OK(SendSealReply(client, object_id, PlasmaError::OK));
  break;
}

void PlasmaStore::SealObjects(const std::vector<ObjectID> &object_ids) {
  for (size_t i = 0; i < object_ids.size(); ++i) {
    RAY_LOG(DEBUG) << "sealing object " << object_ids[i];
    // ② 标记对象为 sealed 状态
    auto entry = object_lifecycle_mgr_.SealObject(object_ids[i]);
    RAY_CHECK(entry) << object_ids[i] << " is missing or not sealed.";

    // ③ 触发回调 → 通知 Raylet
    add_object_callback_(entry->GetObjectInfo());
  }

  for (size_t i = 0; i < object_ids.size(); ++i) {
    // ④ 唤醒等待此对象的 Get 请求
    get_request_queue_.MarkObjectSealed(object_ids[i]);
  }
}
```

### 4.3 ObjectLifecycleManager::SealObject → ObjectStore::SealObject

```cpp
// src/ray/object_manager/plasma/obj_lifecycle_mgr.cc:64
const LocalObject *ObjectLifecycleManager::SealObject(const ObjectID &object_id) {
  auto entry = object_store_->SealObject(object_id);
  if (entry != nullptr) {
    stats_collector_->OnObjectSealed(*entry);  // 统计
  }
  return entry;
}

// src/ray/object_manager/plasma/object_store.cc:71
const LocalObject *ObjectStore::SealObject(const ObjectID &object_id) {
  auto entry = GetMutableObject(object_id);
  if (entry == nullptr || entry->state_ == ObjectState::PLASMA_SEALED) {
    return nullptr;  // 对象不存在或已 sealed
  }
  entry->state_ = ObjectState::PLASMA_SEALED;         // ★ 状态转换
  entry->construct_duration_ = std::time(nullptr) - entry->create_time_;
  return entry;
}
```

对象状态转换：`PLASMA_CREATED` → `PLASMA_SEALED`

Seal 之前：对象数据已写入但不可被 Get 读取（`Get` 会阻塞等待）
Seal 之后：对象变可读，`Get` 可以返回，LRU 也可驱逐

---

## 5. add_object_callback_ → HandleObjectAdded：回调触发链路

### 5.1 回调注册

回调在 `raylet/main.cc` 中注册，通过 `PlasmaStoreRunner::Start` 传给 `PlasmaStore` 构造函数：

```cpp
// src/ray/raylet/main.cc:799
PlasmaStoreRunner::Start(
    /*spill_objects_callback=*/...,
    /*object_store_full_callback=*/...,
    /*add_object_callback=*/
    [&](const ray::ObjectInfo &object_info) {
      // ★ 通过 main_service_.post 投递到 Raylet 事件循环
      main_service.post(
          [&object_manager, &node_manager, object_info]() {
            object_manager->HandleObjectAdded(object_info);  // 步骤 A
            node_manager->HandleObjectLocal(object_info);    // 步骤 B
          },
          "ObjectManager.ObjectAdded");
    },
    /*delete_object_callback=*/...);
```

**跨线程投递**：`add_object_callback_` 在 Plasma Store 的 I/O 线程中被调用，但 `main_service_.post()` 将处理投递到 Raylet 主线程的事件循环中。这保证了线程安全。

### 5.2 回调类型定义

```cpp
// src/ray/object_manager/common.h:243
using AddObjectCallback = std::function<void(const ObjectInfo &)>;
```

### 5.3 传递链路

```
raylet/main.cc 构造 lambda
  → ObjectStoreRunner(add_object_callback)
    → PlasmaStoreRunner::Start(add_object_callback)
      → PlasmaStore(add_object_callback)
        → PlasmaStore::add_object_callback_ 成员变量
          → SealObjects() 中调用 add_object_callback_(entry->GetObjectInfo())
```

### 5.4 HandleObjectAdded 完整代码

```cpp
// src/ray/object_manager/object_manager.cc:171
void ObjectManager::HandleObjectAdded(const ObjectInfo &object_info) {
  const ObjectID &object_id = object_info.object_id;
  RAY_LOG(DEBUG) << "Object added " << object_id;

  // 1. 记录本地对象元信息
  RAY_CHECK(local_objects_.count(object_id) == 0);  // 不应重复
  local_objects_[object_id].object_info = object_info;
  used_memory_ += object_info.data_size + object_info.metadata_size;

  // 2. 通知对象目录（GCS / owner）本节点有此对象
  object_directory_->ReportObjectAdded(object_id, self_node_id_, object_info);

  // 3. 如果此对象正被 PullManager 活跃拉取，立即 Pin 住防驱逐
  pull_manager_->PinNewObjectIfNeeded(object_id);

  // 4. 处理因对象未 Seal 而暂存的 Push 请求
  auto iter = unfulfilled_push_requests_.find(object_id);
  if (iter != unfulfilled_push_requests_.end()) {
    for (auto &pair : iter->second) {
      auto &node_id = pair.first;
      main_service_->post([this, object_id, node_id]() { Push(object_id, node_id); },
                          "ObjectManager.ObjectAddedPush");
      // 取消超时定时器
      if (pair.second != nullptr) {
        pair.second->cancel();
      }
    }
    unfulfilled_push_requests_.erase(iter);
  }
}
```

### 5.5 HandleObjectAdded 四大功能详解

#### 功能 1：记录 local_objects_ 和统计 used_memory_

```cpp
local_objects_[object_id].object_info = object_info;
used_memory_ += object_info.data_size + object_info.metadata_size;
```

`local_objects_` 是 ObjectManager 维护的本地对象元信息表，后续 Push / Pull 都依赖此表判断对象是否在本节点。

#### 功能 2：ReportObjectAdded → 通知 owner

```cpp
// src/ray/object_manager/ownership_object_directory.cc:121
void OwnershipBasedObjectDirectory::ReportObjectAdded(
    const ObjectID &object_id, const NodeID &node_id, const ObjectInfo &object_info) {
  const WorkerID &worker_id = object_info.owner_worker_id;
  const auto owner_address = GetOwnerAddressFromObjectInfo(object_info);
  auto owner_client = GetClient(owner_address);
  if (owner_client == nullptr) {
    // 对象没有 owner（Plasma Store 预热对象），no-op
    return;
  }

  // 向 owner 发送 ObjectLocationUpdate (ADDED)
  rpc::ObjectLocationUpdate &update = location_buffers_[worker_id].second[object_id];
  update.set_object_id(object_id.Binary());
  update.set_plasma_location_update(rpc::ObjectPlasmaLocationUpdate::ADDED);
  location_buffers_[worker_id].first.emplace_back(object_id);
  SendObjectLocationUpdateBatchIfNeeded(worker_id, node_id, owner_address);
}
```

`SendObjectLocationUpdateBatchIfNeeded` 会批量向 owner worker 发送 `UpdateObjectLocationBatch` RPC，通知 owner 此对象在本节点可用。owner 收到后，若有正在等待此对象的 `ray.get()` 调用，可以发起 Pull。

#### 功能 3：PinNewObjectIfNeeded

```cpp
// src/ray/object_manager/pull_manager.cc:586
void PullManager::PinNewObjectIfNeeded(const ObjectID &object_id) {
  absl::MutexLock lock(&active_objects_mu_);
  bool active = active_object_pull_requests_.count(object_id) > 0;
  if (active) {
    if (TryPinObject(object_id)) {
      RAY_LOG(DEBUG) << "Pinned newly created object " << object_id;
    } else {
      RAY_LOG(DEBUG) << "Failed to pin newly created object " << object_id;
    }
  }
}

bool PullManager::TryPinObject(const ObjectID &object_id) {
  if (pinned_objects_.count(object_id) > 0) {
    return true;  // 已经 Pin 过
  }
  auto ref = pin_object_(object_id);  // RPC 到 Raylet 的 PinObjectIDs
  if (ref != nullptr) {
    pinned_objects_size_ += ref->GetSize();
    pinned_objects_[object_id] = std::move(ref);
    // 记录从 Pull 请求开始到 Pin 成功的耗时
    pull_manager_object_request_time_ms_histogram_.Record(...);
    return true;
  }
  num_failed_pins_total_++;
  return false;
}
```

**用途**：当本节点正在 Pull 某对象时，该对象突然在本地被创建（Seal），PullManager 应立即 Pin 住它，防止刚 Seal 就被 LRU 驱逐。

#### 功能 4：处理 unfulfilled_push_requests_

详见 [第 10 章](#10-push-与-unfulfilled_push_requests_-机制)。

---

## 6. HandleObjectLocal：依赖解阻塞

`HandleObjectAdded` 处理 ObjectManager 层面的逻辑，而 `HandleObjectLocal` 处理 Raylet 调度层面的逻辑。两者在同一个 `main_service_.post` 中被串行调用：

```cpp
// src/ray/raylet/node_manager.cc:2417
void NodeManager::HandleObjectLocal(const ObjectInfo &object_info) {
  const ObjectID &object_id = object_info.object_id;

  // 1. 通知 LeaseDependencyManager → 解除等待此对象的 task lease 阻塞
  const auto ready_lease_ids = lease_dependency_manager_.HandleObjectLocal(object_id);
  local_lease_manager_.LeasesUnblocked(ready_lease_ids);

  // 2. 通知 WaitManager → ray.wait() 中等待此对象的可以返回
  wait_manager_.HandleObjectLocal(object_id);

  // 3. 通知等待此 Plasma 对象的 Worker（通过 PlasmaObjectReady RPC）
  auto waiting_workers = absl::flat_hash_set<std::shared_ptr<WorkerInterface>>();
  {
    absl::MutexLock guard(&plasma_object_notification_lock_);
    auto waiting = async_plasma_objects_notification_.extract(object_id);
    if (!waiting.empty()) {
      waiting_workers.swap(waiting.mapped());
    }
  }
  rpc::PlasmaObjectReadyRequest request;
  request.set_object_id(object_id.Binary());
  for (const auto &worker : waiting_workers) {
    worker->rpc_client()->PlasmaObjectReady(request, ...);
  }

  // 4. 检查是否超过 spill 阈值
  SpillIfOverPrimaryObjectsThreshold();
}
```

### 6.1 Worker 订阅 Plasma 对象就绪通知

当 Worker 调用 `ray.get()` 等待一个不在本地的大值对象时，流程为：

```
Worker: plasma_store_provider_->Get(object_id, timeout=-1)
  → 若对象不在本地 → 阻塞等待
  → 同时发 SubscribePlasmaReady 给 Raylet
```

```cpp
// src/ray/raylet/node_manager.cc:2490
void NodeManager::ProcessSubscribePlasmaReady(...) {
  if (lease_dependency_manager_.CheckObjectLocal(id)) {
    // 对象已在本地，直接通知 Worker
    worker->rpc_client()->PlasmaObjectReady(request, ...);
  } else {
    // 对象不在本地，发起 Pull，并注册到 async_plasma_objects_notification_
    lease_dependency_manager_.StartOrUpdateWaitRequest(worker_id, refs);
    {
      absl::MutexLock guard(&plasma_object_notification_lock_);
      async_plasma_objects_notification_[id].insert(associated_worker);
    }
  }
}
```

当对象 Seal 后，`HandleObjectLocal` 会从 `async_plasma_objects_notification_` 中取出所有等待的 Worker，通过 `PlasmaObjectReady` RPC 通知它们。

### 6.2 LeaseDependencyManager 依赖解阻塞

```
对象 A 在节点 N 上 Seal
  → HandleObjectLocal(A)
    → lease_dependency_manager_.HandleObjectLocal(A)
      → 查找哪些 task lease 等待对象 A
      → 返回 ready_lease_ids
    → local_lease_manager_.LeasesUnblocked(ready_lease_ids)
      → 重新调度这些 lease，让对应的 Worker 执行任务
```

---

## 7. Pin 机制：防止 Plasma 对象被驱逐

### 7.1 两种 Pin 路径

| 场景 | Pin 路径 | 触发时机 |
|---|---|---|
| 任务返回值（正常路径） | `SealExisting` → `PinObjectIDs` RPC | Seal 之后立即 Pin |
| 任务返回值（重试/推测） | `PinExistingReturnObject` → `PinObjectIDs` RPC | 对象已 Seal，直接 Pin |
| 远程 Pull 到本节点 | `PullManager::PinNewObjectIfNeeded` | `HandleObjectAdded` 时检查 |
| 主动 Pin（如 `ray.put`） | `SealExisting` → `PinObjectIDs` RPC | 同正常路径 |

### 7.2 PinObjectIDs RPC 处理

```cpp
// src/ray/raylet/node_manager.cc:2588
void NodeManager::HandlePinObjectIDs(rpc::PinObjectIDsRequest request,
                                     rpc::PinObjectIDsReply *reply,
                                     rpc::SendReplyCallback send_reply_callback) {
  std::vector<ObjectID> object_ids;
  for (const auto &object_id_binary : request.object_ids()) {
    object_ids.push_back(ObjectID::FromBinary(object_id_binary));
  }

  std::vector<std::unique_ptr<RayObject>> results;
  if (!GetObjectsFromPlasma(object_ids, &results)) {
    // 从 Plasma Store 读取失败
    for (size_t i = 0; i < object_ids.size(); ++i) {
      reply->add_successes(false);
    }
  } else {
    // 逐个检查对象是否可用
    for (...) {
      if (*result_it == nullptr || local_object_manager_.ObjectPendingDeletion(*object_id_it)) {
        reply->add_successes(false);  // 对象已被驱逐或待删除
      } else {
        reply->add_successes(true);
      }
    }
    // Pin 成功的对象交给 LocalObjectManager 管理
    local_object_manager_.PinObjectsAndWaitForFree(
        object_ids, std::move(results), request.owner_address(), generator_id);
  }
  send_reply_callback(Status::OK(), nullptr, nullptr);
}
```

### 7.3 GetObjectsFromPlasma：Raylet 侧读取 Plasma 对象

```cpp
// src/ray/raylet/node_manager.cc:2561
bool NodeManager::GetObjectsFromPlasma(const std::vector<ObjectID> &object_ids,
                                       std::vector<std::unique_ptr<RayObject>> *results) {
  // 通过 Plasma Store Client Get（timeout=0，不阻塞）
  std::vector<plasma::ObjectBuffer> plasma_results;
  if (!store_client_->Get(object_ids, /*timeout_ms=*/0, &plasma_results).ok()) {
    return false;
  }
  for (const auto &plasma_result : plasma_results) {
    if (plasma_result.data == nullptr) {
      results->push_back(nullptr);
    } else {
      results->emplace_back(std::unique_ptr<RayObject>(
          new RayObject(plasma_result.data, plasma_result.metadata, {})));
    }
  }
  return true;
}
```

**关键**：Raylet 通过 `store_client_->Get` 获取 Plasma 对象的 buffer 引用（`shared_ptr<Buffer>`），只要 Raylet 持有此引用，Plasma Store 就不会释放此对象的内存（ref_count > 0）。

### 7.4 LocalObjectManager::PinObjectsAndWaitForFree

```cpp
// src/ray/raylet/local_object_manager.cc:31
void LocalObjectManager::PinObjectsAndWaitForFree(
    const std::vector<ObjectID> &object_ids,
    std::vector<std::unique_ptr<RayObject>> &&objects,
    const rpc::Address &owner_address,
    const ObjectID &generator_id) {
  for (size_t i = 0; i < object_ids.size(); i++) {
    const auto &object_id = object_ids[i];
    auto &object = objects[i];
    if (object == nullptr) {
      RAY_LOG(ERROR) << "Plasma object " << object_id
                     << " was evicted before the raylet could pin it.";
      continue;
    }

    // 记录到 local_objects_（LocalObjectManager 自己的表）
    const auto inserted = local_objects_.emplace(
        object_id, LocalObjectInfo(owner_address, generator_id, object->GetSize()));
    if (inserted.second) {
      // 首次 Pin
      pinned_objects_size_ += object->GetSize();
      pinned_objects_.emplace(object_id, std::move(object));  // 持有 RayObject 引用
    }

    // 订阅 owner 的 ObjectEviction 通知
    // 当 owner 释放引用后，会发布 eviction 消息，Raylet 收到后释放 Pin
    auto subscription_callback = [this, owner_address](const rpc::PubMessage &msg) {
      const auto obj_id = ObjectID::FromBinary(msg.worker_object_eviction_message().object_id());
      ReleaseFreedObject(obj_id);
    };
    auto owner_dead_callback = [this, owner_address](const std::string &object_id_binary, ...) {
      const auto obj_id = ObjectID::FromBinary(object_id_binary);
      ReleaseFreedObject(obj_id);
    };

    core_worker_subscriber_->Subscribe(
        ..., rpc::ChannelType::WORKER_OBJECT_EVICTION,
        owner_address, object_id.Binary(), ...,
        subscription_callback, owner_dead_callback);
  }
}
```

**Pin 的本质**：Raylet 的 `LocalObjectManager` 持有 `RayObject`（包含 Plasma Buffer 的 `shared_ptr`），只要这个 `shared_ptr` 不释放，Plasma Store 的 ref_count 就不为 0，对象就不会被 LRU 驱逐。

### 7.5 Pin 到 Unpin 的完整生命周期

```
Pin:
  Worker → PinObjectIDs RPC → Raylet HandlePinObjectIDs
    → GetObjectsFromPlasma → 获取 RayObject (含 plasma buffer)
    → LocalObjectManager::PinObjectsAndWaitForFree
      → pinned_objects_[id] = RayObject  → 持有引用
      → 订阅 owner 的 WORKER_OBJECT_EVICTION 频道

Unpin:
  Owner Worker 引用计数归零 → 发布 ObjectEviction 消息
    → Raylet 收到订阅回调 → ReleaseFreedObject(object_id)
      → pinned_objects_.erase(id)  → 释放 RayObject → plasma buffer ref_count--
      → 如果对象还在 pinned 状态：从 pinned_objects_ 中移除
      → 如果对象正在 spilled：等 spill 完成后再清理
      → objects_pending_deletion_ → FlushFreeObjects → 从 Plasma Store 删除

Owner 死亡:
  → owner_dead_callback → ReleaseFreedObject
```

---

## 8. Memory Store 与 Plasma Store 互补关系

### 8.1 双 Store 架构

```
┌─────────────────────────────────────────────────────┐
│                   CoreWorker                         │
│                                                      │
│  ┌──────────────┐         ┌───────────────────────┐ │
│  │ Memory Store │         │ Plasma Store Provider  │ │
│  │ (进程内内存)  │         │ (跨进程 Unix Socket)   │ │
│  │              │         │                        │ │
│  │ • 小值真实数据 │         │ • 大值真实数据          │ │
│  │ • OBJECT_IN_  │         │ • 支持 LRU 驱逐        │ │
│  │   PLASMA 占位 │         │ • 支持 spill 到磁盘    │ │
│  │ • Put 不可覆写│         │ • 支持 ref_count       │ │
│  └──────────────┘         └───────────────────────┘ │
└─────────────────────────────────────────────────────┘
         │                            │
         │ (进程内直接访问)            │ (Unix Socket)
         ▼                            ▼
    直接返回数据              Plasma Store 进程
    (零拷贝)                 (独立共享内存管理)
```

### 8.2 两种 Store 各存什么

| 特性 | Memory Store | Plasma Store |
|---|---|---|
| 存储位置 | CoreWorker 进程内 | 独立进程（共享内存 `/dev/shm`） |
| 小值 (≤100KB) | **存真实数据** | 不涉及 |
| 大值 (>100KB) | **存 `OBJECT_IN_PLASMA` 占位符** | **存真实数据** |
| 覆写 | Put 不可覆写（已存在则忽略） | Seal 后不可覆写，但可重建新实例 |
| 驱逐 | 不会驱逐（随引用计数释放） | LRU 驱逐 / spill 到磁盘 |
| Get 超时 | 不阻塞，立即返回 | 可阻塞等待 Seal |

### 8.3 OBJECT_IN_PLASMA 占位符的作用

当大值对象被 Seal 后，`SealExisting` 会在 Memory Store 中放入 `OBJECT_IN_PLASMA` 占位符：

```cpp
memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA), object_id, ...);
```

这个占位符的意义：

1. **让 `memory_store_->Get()` 能立即返回**：Worker 调用 `ray.get()` 时，CoreWorker 先查 Memory Store，发现 `OBJECT_IN_PLASMA`，知道数据在 Plasma 中
2. **触发 Plasma 读取**：CoreWorker 内部根据 `OBJECT_IN_PLASMA` 标记，转向 `plasma_store_provider_->Get()` 读取
3. **防止重复 Put**：Memory Store 的 `Put` 不可覆写，确保占位符和真实数据不会冲突

### 8.4 Memory Store Put 不可覆写

```cpp
// src/ray/core_worker/store_provider/memory_store/memory_store.cc
bool MemoryStore::Put(const RayObject &object, const ObjectID &id, bool pinning) {
  absl::MutexLock lock(&mutex_);
  auto it = objects_.find(id);
  if (it != objects_.end()) {
    // 对象已存在，不可覆写
    // 但如果有等待者，需要通知
    if (!it->second.empty()) {
      // 通知等待的 Get 请求
    }
    return false;  // 不覆写
  }
  objects_[id] = ...;
  // 通知等待的 Get 请求
  return true;
}
```

---

## 9. 小值路径 vs 大值路径完整对比

### 9.1 小值路径（LocalMemoryBuffer）

```
任务执行完毕，返回小值
  │
  ├─ AllocateReturnObject → 创建 LocalMemoryBuffer → return_ptr[0] != NULL
  │
  ├─ task_execution_callback 将结果写入 return_ptr[0]
  │
  ├─ SealReturnObject → IsPlasmaBuffer() == false → 直接返回 OK
  │   （不调用 Seal / 不调用 Pin / 不触发 HandleObjectAdded）
  │
  └─ 结果已由前端直接 Put 到 Memory Store（在 SealReturnObject 之前）
      memory_store_->Put(真实数据, object_id)
```

**小值的关键特点**：
- **无 Seal**：不需要通知 Plasma Store
- **无 Pin**：不通过 `PinObjectIDs` RPC
- **无 HandleObjectAdded**：不触发 ObjectManager 的回调链
- **无 OBJECT_IN_PLASMA**：Memory Store 直接存真实数据
- **无跨进程通信**：纯进程内操作

### 9.2 大值路径（PlasmaBuffer）

```
任务执行完毕，返回大值
  │
  ├─ AllocateReturnObject → 创建 PlasmaBuffer → return_ptr[0] != NULL
  │     （Plasma Store 已 Allocate 共享内存）
  │
  ├─ task_execution_callback 将结果写入 Plasma 共享内存
  │
  ├─ SealReturnObject → IsPlasmaBuffer() == true
  │   └─ SealExisting(return_id, true, generator_id, owner_address)
  │       │
  │       ├─ ① plasma_store_provider_->Seal(object_id)
  │       │     → PlasmaClient::Seal → Unix Socket → PlasmaStore::SealObjects
  │       │       ├─ SealObject(): PLASMA_CREATED → PLASMA_SEALED
  │       │       ├─ add_object_callback_() → main_service_.post → HandleObjectAdded
  │       │       │                                          → HandleObjectLocal
  │       │       └─ MarkObjectSealed() → 唤醒等待 Get 请求
  │       │
  │       ├─ ② PinObjectIDs RPC (异步)
  │       │     → NodeManager::HandlePinObjectIDs
  │       │       → GetObjectsFromPlasma → 获取 RayObject
  │       │       → LocalObjectManager::PinObjectsAndWaitForFree
  │       │         → pinned_objects_[id] = RayObject (持有 plasma buffer)
  │       │         → 订阅 WORKER_OBJECT_EVICTION
  │       │
  │       ├─ ③ [Pin 回调后] plasma_store_provider_->Release(object_id)
  │       │
  │       └─ ④ memory_store_->Put(OBJECT_IN_PLASMA, object_id)
  │
  └─ 对象现在可被其他 Worker 通过 ray.get() 读取
```

### 9.3 重试路径（return_ptr == NULL）

```
任务重试/推测执行，大值已存在
  │
  ├─ AllocateReturnObject → 对象已 Seal → 返回 nullptr → return_ptr[0] == NULL
  │
  └─ PinExistingReturnObject
      │
      ├─ reference_counter_->AddLocalReference / AddBorrowedObject
      ├─ plasma_store_provider_->Get({return_id}, timeout=0) → 尝试读取
      │     │
      │     ├─ 成功 → 获取到 RayObject → PinObjectIDs RPC
      │     │   （不再 Seal，因为已 Seal）
      │   └─ 失败（已被驱逐）→ 返回 false → 上层报错
      │
      └─ 通知 Memory Store
```

---

## 10. Push 与 unfulfilled_push_requests_ 机制

### 10.1 Push vs Pull 命名约定

| 操作 | 含义 | 方向 | 发起方 |
|---|---|---|---|
| **Pull** | 调度意图："我需要这个对象" | 请求方 → 远端 | 需要对象的节点 |
| **Push** | 实际数据传输："持有方推送给请求方" | 持有方 → 请求方 | 拥有对象的节点 |

Pull 是"调度意图"，Push 是"实际传输"。在 Ray 中，Push 是由 Pull 触发的：请求方 Pull → 远端收到 Pull 请求 → 远端 Push 数据过来。

但还有一种场景：**主动 Push**。当节点 A 发现对象在节点 B 被需要时，A 可以主动 Push 给 B。

### 10.2 Push 请求遇到未 Seal 对象

```cpp
// src/ray/object_manager/object_manager.cc:321
void ObjectManager::Push(const ObjectID &object_id, const NodeID &node_id) {
  // 情况 1：对象在本地 local_objects_ 中（已 Seal）→ 直接 Push
  if (local_objects_.count(object_id) != 0) {
    return PushLocalObject(object_id, node_id);
  }

  // 情况 2：对象在本地磁盘中（spilled）→ 从文件系统 Push
  auto object_url = get_spilled_object_url_(object_id);
  if (!object_url.empty() && is_external_storage_type_fs()) {
    return PushFromFilesystem(object_id, node_id, object_url);
  }

  // 情况 3：对象不在本地 → 暂存到 unfulfilled_push_requests_，等 Seal 后再 Push
  auto &nodes = unfulfilled_push_requests_[object_id];
  if (nodes.count(node_id) == 0) {
    std::unique_ptr<boost::asio::deadline_timer> timer;
    if (config_.push_timeout_ms > 0) {
      // 设置超时定时器
      timer.reset(new boost::asio::deadline_timer(*main_service_));
      timer->expires_from_now(boost::posix_time::milliseconds(config_.push_timeout_ms));
      timer->async_wait([this, object_id, node_id](const boost::system::error_code &error) {
        if (!error) {
          HandlePushTaskTimeout(object_id, node_id);  // 超时后移除此 Push 请求
        }
      });
    }
    if (config_.push_timeout_ms != 0) {
      nodes.emplace(node_id, std::move(timer));
    }
  }
}
```

### 10.3 unfulfilled_push_requests_ 的含义

`unfulfilled_push_requests_` 是 `unordered_map<ObjectID, map<NodeID, timer>>`，存储"本应 Push 但因对象未 Seal 而暂存"的请求。

典型场景：

```
节点 B 向节点 A 发 Pull 请求（我需要对象 X）
节点 A 的 PullManager 收到请求 → 调用 Push(X, B)
  → 但 X 还在写入中（未 Seal）→ local_objects_ 中没有 X
  → 暂存到 unfulfilled_push_requests_[X] = {B: timer}

... 对象 X 写入完毕，Seal ...

HandleObjectAdded(X)
  → 检查 unfulfilled_push_requests_[X]
    → 找到 {B: timer}
      → main_service_.post(Push(X, B))  ← 现在可以 Push 了
      → cancel timer
    → erase unfulfilled_push_requests_[X]
```

### 10.4 HandlePushTaskTimeout

```cpp
// src/ray/object_manager/object_manager.cc:283
void ObjectManager::HandlePushTaskTimeout(const ObjectID &object_id,
                                          const NodeID &node_id) {
  RAY_LOG(WARNING) << "Invalid Push request ObjectID: " << object_id
                   << " after waiting for " << config_.push_timeout_ms << " ms.";
  auto iter = unfulfilled_push_requests_.find(object_id);
  if (iter == unfulfilled_push_requests_.end()) {
    return;  // 可能已被 HandleObjectAdded 处理并 cancel 了定时器
  }
  size_t num_erased = iter->second.erase(node_id);
  RAY_CHECK(num_erased == 1);
  if (iter->second.size() == 0) {
    unfulfilled_push_requests_.erase(iter);
  }
}
```

超时后，Push 请求被丢弃。这意味着如果对象 Seal 太慢，Push 请求可能超时丢失。

### 10.5 PushLocalObject：实际数据传输

```cpp
// src/ray/object_manager/object_manager.cc:365
void ObjectManager::PushLocalObject(const ObjectID &object_id, const NodeID &node_id) {
  const ObjectInfo &object_info = local_objects_[object_id].object_info;

  // 创建 ObjectReader（从 Plasma Store 读取共享内存）
  auto [object_reader, status] = buffer_pool_.CreateObjectReader(object_id, owner_address);

  // 分 chunk 传输
  PushObjectInternal(object_id, node_id,
      std::make_shared<ChunkObjectReader>(std::move(object_reader), config_.object_chunk_size),
      /*from_disk=*/false);
}
```

大对象被分成多个 chunk 传输，每个 chunk 通过 `PushRequest` RPC 发送到目标节点的 `HandlePush`。

---

## 11. MarkObjectSealed：唤醒等待中的 Get 请求

### 11.1 GetRequestQueue 机制

当 Worker 通过 `PlasmaClient::Get` 请求一个未 Seal 的对象时，Get 请求被放入 `GetRequestQueue` 中等待：

```cpp
// src/ray/object_manager/plasma/get_request_queue.cc
void GetRequestQueue::AddRequest(const std::shared_ptr<ClientInterface> &client,
                                 const std::vector<ObjectID> &object_ids,
                                 int64_t timeout_ms) {
  auto get_request = std::make_shared<GetRequest>(io_context_, client, object_ids, ...);

  for (const auto &object_id : unique_ids) {
    auto entry = object_lifecycle_mgr_.GetObject(object_id);
    if (entry != nullptr && entry->Sealed()) {
      // 对象已 Seal，直接标记为 satisfied
      get_request->num_unique_objects_satisfied_ += 1;
      object_satisfied_callback_(object_id, ...);
    } else {
      // 对象未 Seal，加入等待队列
      object_get_requests_[object_id].push_back(get_request);
    }
  }

  // 全部满足或超时 → 完成
  if (all_satisfied || timeout_ms == 0) {
    OnGetRequestCompleted(get_request);
  } else if (timeout_ms != -1) {
    // 设置超时定时器
    get_request->AsyncWait(timeout_ms, ...);
  }
}
```

### 11.2 MarkObjectSealed 唤醒

```cpp
// src/ray/object_manager/plasma/get_request_queue.cc
void GetRequestQueue::MarkObjectSealed(const ObjectID &object_id) {
  auto it = object_get_requests_.find(object_id);
  if (it == object_get_requests_.end()) {
    return;  // 没有等待此对象的 Get 请求
  }

  auto &get_requests = it->second;
  for (size_t i = 0; i < num_requests; ++i) {
    auto get_request = get_requests[index];

    // 获取对象信息，填充到 get_request 中
    auto entry = object_lifecycle_mgr_.GetObject(object_id);
    auto *plasma_object = &get_request->objects_[object_id];
    entry->ToPlasmaObject(plasma_object, /*check_sealed=*/true);
    get_request->num_unique_objects_satisfied_ += 1;

    // 通知客户端此对象已可用
    object_satisfied_callback_(object_id, fallback_allocated_fd, get_request);

    // 如果此 GetRequest 的所有对象都已满足 → 完成
    if (get_request->num_unique_objects_satisfied_ ==
        get_request->num_unique_objects_to_wait_for_) {
      OnGetRequestCompleted(get_request);
    }
  }

  // 清理：此对象不再有等待的 Get 请求
  object_get_requests_.erase(object_id);
}
```

**时序**：`MarkObjectSealed` 在 `SealObjects` 中位于 `add_object_callback_` 之后调用。但 `add_object_callback_` 是 `post` 异步投递的，所以 `MarkObjectSealed` 实际上先于 `HandleObjectAdded` 执行（在同一线程中顺序执行，但 `post` 投递的任务在下一个事件循环周期）。

---

## 12. 三种前端调用 SealReturnObject 的差异

### 12.1 Python 前端 (_raylet.pyx)

```python
# python/ray/_raylet.pyx
def store_task_output(...):
    # 1. AllocateReturnObject → 返回 return_ptr
    with nogil:
        check_status(
            CCoreWorkerProcess.GetCoreWorker().AllocateReturnObject(
                return_id, ... &return_ptr[0], ...))

    # 2. 写入数据到 return_ptr
    if return_ptr[0] != NULL:
        write_data_to_ptr(return_ptr[0], result)
        # 3. SealReturnObject
        with nogil:
            check_status(
                CCoreWorkerProcess.GetCoreWorker().SealReturnObject(
                    return_id, return_ptr[0], generator_id, caller_address))
    else:
        # return_ptr == NULL → PinExistingReturnObject
        success = CCoreWorkerProcess.GetCoreWorker().PinExistingReturnObject(
            return_id, &return_ptr[0], generator_id, caller_address)

# Streaming Generator 场景：
def create_generator_return_obj(...):
    # generator_id 传入实际值（非 Nil）
    check_status(
        CCoreWorkerProcess.GetCoreWorker().SealReturnObject(
            object_id, return_ptr[0], generator_id, caller_address))
```

**Python 特点**：
- Streaming Generator 传入**实际 generator_id**（非 Nil）
- `return_ptr == NULL` 时走 `PinExistingReturnObject`
- 支持小值直接写入 Memory Store（在 SealReturnObject 之前已完成）

### 12.2 Java 前端 (io_ray_runtime_RayNativeRuntime.cc)

```cpp
// src/ray/core_worker/lib/java/io_ray_runtime_RayNativeRuntime.cc
JNIEXPORT void JNICALL Java_io_ray_runtime_RayNativeRuntime_nativeSealReturnObject(
    JNIEnv *env, jobject, jbyteArray returnId, jlong returnObj, jbyteArray callerAddress) {
  auto return_object = reinterpret_cast<std::shared_ptr<RayObject> *>(returnObj);
  auto return_id = JavaByteArrayToId<ObjectID>(env, returnId);
  auto caller_address = JavaByteArrayToAddress(env, callerAddress);

  RAY_CHECK_OK(CoreWorkerProcess::GetCoreWorker().SealReturnObject(
      return_id, *return_object, /*generator_id=*/ObjectID::Nil(), caller_address));
}
```

**Java 特点**：
- `generator_id` 固定为 `ObjectID::Nil()`（Java 不支持 Streaming Generator）
- SealReturnObject 由 Java 层显式调用

### 12.3 C++ 前端 (task_executor.cc)

```cpp
// cpp/src/ray/runtime/task/task_executor.cc
Status TaskExecutor::ExecuteTask(...) {
  // 执行用户代码，写入 result
  RAY_CHECK_OK(task_caller_->RunTaskForTaskExecution(..., &result));

  // SealReturnObject
  RAY_CHECK_OK(CoreWorkerProcess::GetCoreWorker().SealReturnObject(
      result_id, result,
      /*generator_id=*/ObjectID::Nil(),  // C++ 不支持 Streaming Generator
      caller_address));
}
```

**C++ 特点**：
- `generator_id` 固定为 `ObjectID::Nil()`
- ExecuteTask 内部直接调用 SealReturnObject（不需要外部显式调用）

### 12.4 三种前端对比

| 特性 | Python | Java | C++ |
|---|---|---|---|
| generator_id | 实际值 (Streaming Generator) 或 Nil | Nil | Nil |
| return_ptr==NULL 处理 | `PinExistingReturnObject` | Java 层处理 | 不处理（假设不重试） |
| SealReturnObject 调用方 | `_raylet.pyx` (store_task_output) | Java native method | `TaskExecutor::ExecuteTask` |
| 小值写入 | 前端直接 Put Memory Store | 前端直接 Put | 前端直接 Put |

---

## 13. 对象释放与 Unpin 机制

### 13.1 释放触发条件

对象被 Unpin 释放有三种触发路径：

```
路径 1：Owner 引用计数归零
  Worker 的 reference_counter_ 检测到对象无引用
    → 发布 ObjectEviction 消息（WORKER_OBJECT_EVICTION 频道）
    → Raylet 订阅回调 → ReleaseFreedObject

路径 2：Owner Worker 死亡
  → Raylet 检测到 Worker 断连
    → owner_dead_callback → ReleaseFreedObject

路径 3：对象被 Plasma Store LRU 驱逐
  → delete_object_callback_ → HandleObjectDeleted → HandleObjectMissing
    → PullManager::ResetRetryTimer（若仍在 Pull 则重新拉取）
```

### 13.2 ReleaseFreedObject 完整流程

```cpp
// src/ray/raylet/local_object_manager.cc:115
void LocalObjectManager::ReleaseFreedObject(const ObjectID &object_id) {
  auto it = local_objects_.find(object_id);
  if (it == local_objects_.end() || it->second.is_freed_) {
    return;  // 已释放或不存在
  }
  it->second.is_freed_ = true;

  auto pinned_objects_it = pinned_objects_.find(object_id);
  if (pinned_objects_it != pinned_objects_.end()) {
    // 对象还在 Plasma 中（未 spilled）
    pinned_objects_size_ -= pinned_objects_it->second->GetSize();
    pinned_objects_.erase(pinned_objects_it);  // 释放 RayObject → plasma ref_count--
    local_objects_.erase(it);
  } else {
    // 对象正在 spill 或已 spilled
    spilled_object_pending_delete_.push(object_id);
  }

  // 尝试从集群中删除所有副本
  if (free_objects_period_ms_ >= 0) {
    objects_pending_deletion_.emplace(object_id);
  }
  if (objects_pending_deletion_.size() == free_objects_batch_size_ ||
      free_objects_period_ms_ == 0) {
    FlushFreeObjects();
  }
}
```

### 13.3 HandleObjectDeleted（Plasma Store 驱逐回调）

```cpp
// src/ray/object_manager/object_manager.cc:191
void ObjectManager::HandleObjectDeleted(const ObjectID &object_id) {
  auto it = local_objects_.find(object_id);
  auto object_info = it->second.object_info;
  local_objects_.erase(it);
  used_memory_ -= object_info.data_size + object_info.metadata_size;

  // 通知 GCS / owner 此节点不再有此对象
  object_directory_->ReportObjectRemoved(object_id, self_node_id_, object_info);

  // 如果此对象正在被 Pull，重试拉取
  pull_manager_->ResetRetryTimer(object_id);
}
```

`delete_object_callback_` 同样在 `raylet/main.cc` 中注册：

```cpp
/*delete_object_callback=*/
[&](const ray::ObjectID &object_id) {
  main_service.post(
      [&object_manager, &node_manager, object_id]() {
        object_manager->HandleObjectDeleted(object_id);
        node_manager->HandleObjectMissing(object_id);
      },
      "ObjectManager.ObjectDeleted");
}
```

### 13.4 HandleObjectMissing（Raylet 调度层面）

```cpp
// src/ray/raylet/node_manager.cc:2440
void NodeManager::HandleObjectMissing(const ObjectID &object_id) {
  // 通知 LeaseDependencyManager 对象不再本地
  const auto waiting_lease_ids = lease_dependency_manager_.HandleObjectMissing(object_id);
  // 需要此对象的任务 lease 被重新阻塞
}
```

---

## 14. 关键源文件索引

| 文件 | 核心功能 |
|---|---|
| `src/ray/core_worker/core_worker.cc` | SealReturnObject, SealExisting, SealOwned, PinExistingReturnObject |
| `src/ray/core_worker/store_provider/plasma_store_provider.cc` | PlasmaStoreProvider::Seal, Create, Release |
| `src/ray/object_manager/plasma/client.cc` | PlasmaClient::Seal (Unix Socket RPC) |
| `src/ray/object_manager/plasma/store.cc` | PlasmaStore::SealObjects, ProcessClientMessage |
| `src/ray/object_manager/plasma/object_store.cc` | ObjectStore::SealObject (状态转换) |
| `src/ray/object_manager/plasma/obj_lifecycle_mgr.cc` | ObjectLifecycleManager::SealObject (代理层) |
| `src/ray/object_manager/plasma/get_request_queue.cc` | MarkObjectSealed (唤醒 Get) |
| `src/ray/object_manager/plasma/store_runner.cc` | PlasmaStoreRunner::Start (回调注册) |
| `src/ray/raylet/main.cc` | add_object_callback / delete_object_callback 注册 |
| `src/ray/object_manager/object_manager.cc` | HandleObjectAdded, Push, HandlePush, HandlePushTaskTimeout |
| `src/ray/object_manager/pull_manager.cc` | PinNewObjectIfNeeded, TryPinObject |
| `src/ray/object_manager/ownership_object_directory.cc` | ReportObjectAdded, SendObjectLocationUpdateBatchIfNeeded |
| `src/ray/raylet/node_manager.cc` | HandlePinObjectIDs, HandleObjectLocal, HandleObjectMissing, ProcessSubscribePlasmaReady, GetObjectsFromPlasma |
| `src/ray/raylet/local_object_manager.cc` | PinObjectsAndWaitForFree, ReleaseFreedObject, FlushFreeObjects |
| `src/ray/core_worker/store_provider/memory_store/memory_store.cc` | Put (不可覆写), Get |
| `python/ray/_raylet.pyx` | store_task_output, AllocateReturnObject + SealReturnObject (Python 前端) |
| `src/ray/core_worker/lib/java/io_ray_runtime_RayNativeRuntime.cc` | nativeSealReturnObject (Java 前端) |
| `cpp/src/ray/runtime/task/task_executor.cc` | ExecuteTask + SealReturnObject (C++ 前端) |
