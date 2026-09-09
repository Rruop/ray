# Streaming Generator Object Recovery 完整分析

## 目录

1. [对象创建与 Seal 机制](#1-对象创建与-seal-机制)
2. [HandlePush / HandlePull / CreateOwnedAndIncrementLocalRef 到 HandleObjectAdded 的路径](#2-handlepush--handlepull--createownedandincrementlocalref-到-handleobjectadded-的路径)
3. [Recovery 触发与重提交](#3-recovery-触发与重提交)
4. [Recovery 时是否会重复创建对象](#4-recovery-时是否会重复创建对象)
5. [新 Worker 上的对象产生与 Seal](#5-新-worker-上的对象产生与-seal)
6. [多余对象的清除机制](#6-多余对象的清除机制)
7. [Lineage Ref 与 OutOfScope 判断](#7-lineage-ref-与-outofscope-判断)
   - 7a. [为什么 OutOfScope 不等于可以从 ref table 中删除](#为什么-outofscope-不等于可以从-ref-table-中删除)
   - 7b. [DeleteReferenceInternal 的完整两步流程](#deletereferenceinternal-的完整两步流程)
   - 7c. [OnObjectOutOfScopeOrFreed 与 EraseReference 的区别](#onobjectoutofscopeorfreed-与-erasereference-的区别)
   - 7d. [完整状态转移图](#完整状态转移图)
   - 7e. [ReleaseLineageReferences 的递归清理](#releaselineagereferences-的递归清理)
8. [AddObjectOutOfScopeOrFreedCallback 详细分支逻辑](#8-addobjectoutofscopeorfreedcallback-详细分支逻辑)
9. [ProcessSubscribeForObjectEviction 与 Eviction 订阅机制](#9-processsubscribeforobjecteviction-与-eviction-订阅机制)
10. [完整 Recovery 交互时序](#10-完整-recovery-交互时序)
11. [LineageReconstructionEligibility 详解](#11-lineagereconstructioneligibility-详解)
12. [object_id_refs_ 和 freed_objects_ 更新时机](#12-object_id_refs_-和-freed_objects_-更新时机)
    - 12a. [为什么不能 OutOfScope 就从 ref table 中删除 — 总结](#为什么不能-outofscope-就从-ref-table-中删除--总结)

---

## 1. 对象创建与 Seal 机制

### Seal 的含义

**Seal = "封印/密封"**，在 Plasma Store 中表示对象从"可写入"状态变为"只读完成"状态。

```
Create (创建)  →  对象处于 "unsealed" 状态
                   → 分配了内存 buffer
                   → 调用方可以写入数据

Write (写入)   →  向 buffer 中 memcpy 数据

Seal (密封)    →  对象从 "unsealed" 变为 "sealed" 状态
                   → 对象变为只读，不再允许写入
                   → 触发 add_object_callback_ → HandleObjectAdded
                   → 对象对其他进程可见（可以被 Get）
```

| 状态 | 含义 | 其他进程能否 Get |
|------|------|----------------|
| Created (unsealed) | 正在写入数据，未完成 | 不能 — `Get` 会等待或超时 |
| Sealed | 写入完成，只读 | 可以 — 对象可用 |
| Aborted | 写入失败，取消 | 不能 — buffer 被回收 |

### Plasma Store 内部实现

`src/ray/object_manager/plasma/object_store.cc:71`:

```cpp
const LocalObject *ObjectStore::SealObject(const ObjectID &object_id) {
  auto it = objects_.find(object_id);
  if (it == objects_.end() || it->second->state_ == ObjectState::PLASMA_SEALED) {
    return nullptr;  // 不存在或已 sealed → 返回 null
  }
  it->second->state_ = ObjectState::PLASMA_SEALED;  // 状态变更
  return it->second.get();
}
```

### Seal 触发 HandleObjectAdded 的链路

`src/ray/object_manager/plasma/store.cc:275`:

```cpp
void PlasmaStore::SealObjects(const std::vector<ObjectID> &object_ids) {
  for (size_t i = 0; i < object_ids.size(); ++i) {
    auto entry = object_lifecycle_mgr_.SealObject(object_ids[i]);
    RAY_CHECK(entry) << object_ids[i] << " is missing or not sealed.";
    add_object_callback_(entry->GetObjectInfo(), entry->GetSource());
  }
  for (size_t i = 0; i < object_ids.size(); ++i) {
    get_request_queue_.MarkObjectSealed(object_ids[i]);
  }
}
```

`add_object_callback_` 在 raylet 启动时注册（`src/ray/raylet/main.cc:800`）:

```cpp
/*add_object_callback=*/
[&](const ray::ObjectInfo &object_info,
    plasma::flatbuf::ObjectSource source) {
  main_service.post(
      [&object_manager, &node_manager, object_info, source]() {
        object_manager->HandleObjectAdded(object_info);
        node_manager->HandleObjectLocal(object_info, source);
      },
      "ObjectManager.ObjectAdded");
},
```

`HandleObjectAdded`（`src/ray/object_manager/object_manager.cc:181`）:

```cpp
void ObjectManager::HandleObjectAdded(const ObjectInfo &object_info) {
  const ObjectID &object_id = object_info.object_id;
  RAY_CHECK(local_objects_.count(object_id) == 0);
  local_objects_[object_id].object_info = object_info;
  used_memory_ += object_info.data_size + object_info.metadata_size;
  object_directory_->ReportObjectAdded(object_id, self_node_id_, object_info);
  pull_manager_->PinNewObjectIfNeeded(object_id);

  // 处理之前因为对象不在本地而未完成的 push 请求
  auto iter = unfulfilled_push_requests_.find(object_id);
  if (iter != unfulfilled_push_requests_.end()) {
    for (auto &pair : iter->second) {
      main_service_->post([this, object_id, node_id]() { Push(object_id, node_id); }, ...);
    }
    unfulfilled_push_requests_.erase(iter);
  }
}
```

---

## 2. HandlePush / HandlePull / CreateOwnedAndIncrementLocalRef 到 HandleObjectAdded 的路径

三条路径最终都汇聚到同一个点：**Plasma Store 的 `SealObjects()` → `add_object_callback_()` → `HandleObjectAdded()`**。

### 核心原则

**谁调了 `Seal`，谁就触发了 `HandleObjectAdded`。** 仅 Create 不会触发，必须 Seal。

### 路径1: HandlePush → HandleObjectAdded

```
HandlePush (object_manager.cc:610)
  → ReceivePullChunk or ReceiveReplicationPushChunk
    → buffer_pool_.CreateChunk() (object_buffer_pool.cc:97)
    → buffer_pool_.WriteChunk() (object_buffer_pool.cc:130)
      → [当所有 chunk 写完, num_seals_remaining_==0] store_client_->Seal()
        → PlasmaClient::Seal() (client.cc:569)
          → [发送 PlasmaSealRequest over socket]
            → PlasmaStore::SealObjects() (store.cc:275)
              → add_object_callback_() (store.cc:280)
                → [posted to main_service] (main.cc:801)
                  → ObjectManager::HandleObjectAdded() (object_manager.cc:181)
```

**HandlePush 入口** (`object_manager.cc:610`):

```cpp
void ObjectManager::HandlePush(rpc::PushRequest request, ...) {
  ObjectID object_id = ObjectID::FromBinary(request.object_id());
  bool is_replication_push = request.is_replication_push();

  bool success = is_replication_push
                     ? ReceiveReplicationPushChunk(...)
                     : ReceivePullChunk(...);
}
```

**ReceivePullChunk** (`object_manager.cc:634`):

```cpp
bool ObjectManager::ReceivePullChunk(...) {
  if (!pull_manager_->IsObjectActive(object_id)) {
    return false;
  }
  auto chunk_status = buffer_pool_.CreateChunk(
      object_id, owner_address, data_size, metadata_size, chunk_index,
      plasma::flatbuf::ObjectSource::ReceivedByPull);
  if (chunk_status.ok()) {
    buffer_pool_.WriteChunk(object_id, data_size, metadata_size, chunk_index, data);
    return true;
  }
}
```

**WriteChunk 中的 Seal 触发** (`object_buffer_pool.cc:130`):

```cpp
void ObjectBufferPool::WriteChunk(...) {
  // ... memcpy 数据 ...
  {
    absl::MutexLock lock(&pool_mutex_);
    auto it = create_buffer_state_.find(object_id);
    it->second.num_inflight_copies_--;
    it->second.num_seals_remaining_--;
    if (it->second.num_seals_remaining_ == 0) {
      RAY_CHECK_OK(store_client_->Seal(object_id));     // ← 触发 Seal
      RAY_CHECK_OK(store_client_->Release(object_id));
      create_buffer_state_.erase(it);
    }
  }
}
```

### 路径2: HandlePull → (远端)HandlePush → HandleObjectAdded

**HandlePull 本身不触发本节点的 HandleObjectAdded**，它只是向远端 Push：

```cpp
void ObjectManager::HandlePull(rpc::PullRequest request, ...) {
  ObjectID object_id = ObjectID::FromBinary(request.object_id());
  NodeID node_id = NodeID::FromBinary(request.node_id());
  main_service_->post([this, object_id, node_id]() { Push(object_id, node_id); },
                      "ObjectManager.HandlePull");
  send_reply_callback(Status::OK(), nullptr, nullptr);
}
```

远端节点收到 Push 后走路径1。

### 路径3: CreateOwnedAndIncrementLocalRef → SealOwned → HandleObjectAdded

```
CreateOwnedAndIncrementLocalRef (core_worker.cc:1046)
  → plasma_store_provider_->Create() (core_worker.cc:1116)
    → store_client_->CreateAndSpillIfNeeded() (plasma_store_provider.cc:137)
      → PlasmaClient::CreateAndSpillIfNeeded() (client.cc:215)
        → [发送 PlasmaCreateRequest — buffer 创建但未 sealed]

[调用方写入数据后调用 SealOwned()]

SealOwned (core_worker.cc:1184)
  → SealExisting (core_worker.cc:1194)
    → plasma_store_provider_->Seal() (core_worker.cc:1201)
      → store_client_->Seal()
        → PlasmaClient::Seal() (client.cc:569)
          → [发送 PlasmaSealRequest over socket]
            → PlasmaStore::SealObjects() (store.cc:275)
              → add_object_callback_()
                → HandleObjectAdded() (object_manager.cc:181)
```

**CreateOwnedAndIncrementLocalRef** (`core_worker.cc:1046`):

```cpp
Status CoreWorker::CreateOwnedAndIncrementLocalRef(...) {
  *object_id = ObjectID::FromIndex(worker_context_->GetCurrentInternalTaskId(),
                                   worker_context_->GetNextPutIndex());
  if (owned_by_us) {
    reference_counter_->AddOwnedObject(*object_id, ...,
        LineageReconstructionEligibility::INELIGIBLE_PUT, ...);
  }
  // ★ 在 Plasma 中创建 buffer（不触发 HandleObjectAdded，只是 Create）
  status = plasma_store_provider_->Create(metadata, data_size, *object_id,
      /*owner_address=*/real_owner_address, data,
      /*created_by_worker=*/true, is_experimental_mutable_object,
      worker_context_->IsReconstruction());
  // 返回 buffer 给调用方
}
```

**SealOwned** (`core_worker.cc:1184`):

```cpp
Status CoreWorker::SealOwned(const ObjectID &object_id, bool pin_object, ...) {
  auto status = SealExisting(object_id, pin_object, ObjectID::Nil(), ...);
  ...
}
```

**SealExisting** (`core_worker.cc:1194`):

```cpp
Status CoreWorker::SealExisting(const ObjectID &object_id, bool pin_object,
                                const ObjectID &generator_id, ...) {
  RAY_RETURN_NOT_OK(plasma_store_provider_->Seal(object_id));  // ★ Seal
  if (pin_object) {
    local_raylet_rpc_client_->PinObjectIDs(
        owner_address, {object_id}, generator_id, ...);        // ★ Pin
  }
  memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
                     object_id, reference_counter_->HasReference(object_id));
  return Status::OK();
}
```

---

## 3. Recovery 触发与重提交

### Recovery 触发链路

**定期检测丢失对象** (`core_worker.cc:477`):

```cpp
periodical_runner_->RunFnPeriodically(
    [this] {
      const auto lost_objects = reference_counter_->FlushObjectsToRecover();
      if (!lost_objects.empty()) {
        RAY_LOG(ERROR) << "Attempting to recover " << lost_objects.size()
                       << " lost objects by resubmitting their tasks or setting a new "
                       << "primary location from existing copies.";
        memory_store_->Delete(lost_objects);
        for (const auto &object_id : lost_objects) {
          RAY_UNUSED(object_recovery_manager_->RecoverObject(object_id));
        }
      }
    }, 100, "CoreWorker.RecoverObjects");
```

**RecoverObject** (`object_recovery_manager.cc:30`):

```cpp
std::optional<rpc::ErrorType> ObjectRecoveryManager::RecoverObject(
    const ObjectID &object_id) {
  // 检查对象是否还有 pin/spilled 位置
  bool ref_exists = reference_counter_.IsPlasmaObjectPinnedOrSpilled(
      object_id, &owned_by_us, &pinned_at, &spilled);

  bool requires_recovery = pinned_at.IsNil() && !spilled;
  if (requires_recovery) {
    // ★ 防重复：objects_pending_recovery_ 集合
    absl::MutexLock lock(&objects_pending_recovery_mu_);
    already_pending_recovery = !objects_pending_recovery_.insert(object_id).second;
  }

  if (!already_pending_recovery) {
    // 注册异步回调
    in_memory_store_.GetAsync(object_id, [...](...) {
      objects_pending_recovery_.erase(object_id);
    });
    // 查找对象位置
    object_lookup_(object_id, [...](...) {
      PinOrReconstructObject(object_id, locations);
    });
  }
}
```

**PinOrReconstructObject** (`object_recovery_manager.cc:96`):

```cpp
void PinOrReconstructObject(const ObjectID &object_id,
                            std::vector<rpc::Address> locations) {
  if (!locations.empty()) {
    // 路径A：还有副本 → Pin 已有副本（不重新创建）
    PinExistingObjectCopy(object_id, location, ...);
  } else {
    // 路径B：没有副本 → 线性重执行（重新创建）
    ReconstructObject(object_id);
  }
}
```

**ReconstructObject** (`object_recovery_manager.cc:127`):

```cpp
void ReconstructObject(const ObjectID &object_id) {
  LineageReconstructionEligibility eligibility =
      reference_counter_.GetLineageReconstructionEligibility(object_id);
  if (eligibility != LineageReconstructionEligibility::ELIGIBLE) {
    // 不可恢复 → 报错
    recovery_failure_callback_(object_id, error_type, true);
    return;
  }
  // 可恢复 → 重提交 task
  task_manager_.ResubmitTask(task_id, &task_deps);
}
```

### Task 重提交

**ResubmitTask** (`task_manager.cc:353`):

```cpp
std::optional<rpc::ErrorType> TaskManager::ResubmitTask(
    const TaskID &task_id, std::vector<ObjectID> *task_deps) {
  {
    absl::MutexLock lock(&mu_);
    auto &task_entry = it->second;

    if (task_entry.spec_.IsStreamingGenerator() &&
        task_entry.GetStatus() == rpc::TaskStatus::SUBMITTED_TO_WORKER) {
      // streaming generator 正在运行中 → 标记需要重提交
      should_queue_generator_resubmit = true;
    } else if (task_entry.GetStatus() != FINISHED &&
               task_entry.GetStatus() != FAILED) {
      return std::nullopt;  // 已经在重试中
    } else {
      SetupTaskEntryForResubmit(task_entry);
    }
    spec = task_entry.spec_;
  }

  spec.SetIsReconstruction(true);  // ★ 标记为重构造

  if (should_queue_generator_resubmit) {
    return queue_generator_resubmit_(spec);  // 延迟重提交
  }

  UpdateReferencesForResubmit(spec, task_deps);  // ★ 更新依赖引用
  async_retry_task_callback_(spec, 0);           // ★ 重提交
}
```

**UpdateReferencesForResubmit** (`task_manager.cc:440`):

```cpp
void TaskManager::UpdateReferencesForResubmit(const TaskSpecification &spec,
                                              std::vector<ObjectID> *task_deps) {
  // 收集所有依赖
  for (size_t i = 0; i < spec.NumArgs(); i++) {
    if (spec.ArgByRef(i)) {
      task_deps->emplace_back(spec.ArgObjectId(i));
    }
  }

  reference_counter_.UpdateResubmittedTaskReferences(*task_deps);
  // ★ UpdateResubmittedTaskReferences 只增加 submitted_task_ref_count
  //   不增加 lineage_ref_count（首次提交时已计数过）

  for (const auto &task_dep : *task_deps) {
    bool was_freed = reference_counter_.TryMarkFreedObjectInUseAgain(task_dep);
    if (was_freed) {
      // 之前被 free 的依赖对象，现在 recovery 需要重新使用
      in_memory_store_.Delete({task_dep});
    }
  }
}
```

**UpdateResubmittedTaskReferences** (`reference_counter.cc:529`):

```cpp
void UpdateResubmittedTaskReferences(const std::vector<ObjectID> &argument_ids) {
  for (const ObjectID &argument_id : argument_ids) {
    auto it = object_id_refs_.find(argument_id);
    RAY_CHECK(it != object_id_refs_.end());
    it->second.submitted_task_ref_count++;
    // ★ 注意：lineage_ref_count 不增加！
    // 因为 lineage 已经在首次提交时计数过了
  }
}
```

### Streaming Generator 的特殊处理

对于 streaming generator，如果 task 正在运行中（`SUBMITTED_TO_WORKER`），`ResubmitTask` 不会立即重提交，而是通过 `queue_generator_resubmit_` 延迟。当当前执行完成时，通过 `MarkGeneratorFailedAndResubmit` 触发重提交。

`MarkGeneratorFailedAndResubmit` (`task_manager.cc:475`):

```cpp
void TaskManager::MarkGeneratorFailedAndResubmit(const TaskID &task_id) {
  TaskSpecification spec;
  {
    absl::MutexLock lock(&mu_);
    auto &task_entry = it->second;
    SetTaskStatus(task_entry, rpc::TaskStatus::FAILED, ...);
    SetupTaskEntryForResubmit(task_entry);
    spec = task_entry.spec_;
  }
  spec.SetIsReconstruction(true);
  // Note: Don't need to call UpdateReferencesForResubmit because
  // CompletePendingTask or FailPendingTask are not called when this is.
  async_retry_task_callback_(spec, 0);
}
```

---

## 4. Recovery 时是否会重复创建对象

### 核心结论

**是的。** Streaming generator 的 recovery 是"全量重执行"而非"增量恢复"。Task 不知道哪些对象还活着、哪些丢了，它只是重新跑一遍。

### GetTaskReturnObjectsToStoreInPlasma 决定哪些对象写入 Plasma

`task_manager.cc:1538`:

```cpp
absl::flat_hash_set<ObjectID> TaskManager::GetTaskReturnObjectsToStoreInPlasma(
    const TaskID &task_id, bool *first_execution_out) const {
  bool first_execution = false;
  absl::flat_hash_set<ObjectID> store_in_plasma_ids = {};
  absl::MutexLock lock(&mu_);
  auto it = submissible_tasks_.find(task_id);
  first_execution = it->second.num_successful_executions_ == 0;
  if (!first_execution) {
    // ★ 非首次执行 → 返回 reconstructable_return_ids_
    //   即之前所有在 plasma 中存过的 return object IDs
    store_in_plasma_ids = it->second.reconstructable_return_ids_;
  }
  if (first_execution_out != nullptr) {
    *first_execution_out = first_execution;
  }
  return store_in_plasma_ids;
}
```

**首次执行**：`store_in_plasma_ids` 为空，executor 决定什么放 Plasma。
**重执行**：`store_in_plasma_ids` = `reconstructable_return_ids_`（所有 plasma-eligible 的 return IDs）。

### reconstructable_return_ids_ 的管理

**添加** — `CompletePendingTask` (`task_manager.cc:1007`):

```cpp
if (first_execution) {
  for (const auto &dynamic_return_id : dynamic_returns_in_plasma) {
    it->second.reconstructable_return_ids_.insert(dynamic_return_id);
  }
  if (spec.IsStreamingGenerator()) {
    for (const auto &return_id_info : reply.streaming_generator_return_ids()) {
      if (return_id_info.is_plasma_object()) {
        it->second.reconstructable_return_ids_.insert(
            ObjectID::FromBinary(return_id_info.object_id()));
      }
    }
  }
}
```

**移除** — `RemoveLineageReference` (`task_manager.cc:1449`):

```cpp
int64_t TaskManager::RemoveLineageReference(const ObjectID &object_id, ...) {
  auto it = submissible_tasks_.find(task_id);
  // ★ 当 plasma 对象 out of scope 时，从 reconstructable_return_ids_ 中删除
  it->second.reconstructable_return_ids_.erase(object_id);

  // 如果所有 plasma returns 都 out of scope → 释放 lineage
  if (it->second.reconstructable_return_ids_.empty() && !it->second.IsPending()) {
    // 释放 task 的所有参数引用
  }
}
```

### 防止重复的机制

虽然 task 会全量重执行，但有多层过滤防止重复创建：

| 层次 | 机制 | 位置 |
|------|------|------|
| Plasma Store | `CreateObject` 检查 `ObjectExists` | `obj_lifecycle_mgr.cc:46` |
| Plasma Client | `Seal` 检查 `is_sealed` | `client.cc:580` |
| PlasmaStoreProvider | `Put` 中 `data==nullptr` 跳过 Seal | `plasma_store_provider.cc:104` |
| ObjectRefStream | `InsertToStream` 去重（`item_index < next_index_`） | `task_manager.cc:180` |
| ReferenceCounter | `AddOwnedObjectInternal` 检查已存在 | `reference_counter.cc:356` |
| Attempt 过滤 | 旧 attempt 的上报被忽略 | `task_manager.cc:798` |

---

## 5. 新 Worker 上的对象产生与 Seal

### 关键：Executor 端在产出对象时就已经 Create + Seal 了，不需要等 HandleTaskReturn

#### AllocateReturnObject (`core_worker.cc:2916`)

```cpp
Status CoreWorker::AllocateReturnObject(const ObjectID &object_id,
                                        const size_t &data_size, ...) {
  if (data_size > 0) {
    if (static_cast<int64_t>(data_size) < max_direct_call_object_size_ &&
        (*task_output_inlined_bytes + data_size <=
         RayConfig::instance().task_rpc_inlined_bytes_limit())) {
      // 小对象 → 内存 buffer
      data_buffer = std::make_shared<LocalMemoryBuffer>(data_size);
    } else {
      // 大对象 → 在 executor 的本地 Plasma 中 Create
      RAY_RETURN_NOT_OK(CreateExisting(metadata, data_size, object_id,
                                       owner_address, &data_buffer, true));
      object_already_exists = data_buffer == nullptr;
    }
  }
  *return_object = std::make_shared<RayObject>(data_buffer, metadata, ...);
}
```

#### SealReturnObject (`core_worker.cc:3220`)

```cpp
Status CoreWorker::SealReturnObject(const ObjectID &return_id,
                                    const std::shared_ptr<RayObject> &return_object,
                                    const ObjectID &generator_id,
                                    const rpc::Address &owner_address) {
  if (return_object->GetData() != nullptr && return_object->GetData()->IsPlasmaBuffer()) {
    // ★ 是 Plasma buffer → 立即 Seal！
    status = SealExisting(return_id, true, generator_id, owner_address_ptr);
  }
  // 如果是内存 buffer → 不需要 Seal
  return status;
}
```

#### SealExisting 中的 Pin (`core_worker.cc:1194`)

```cpp
Status CoreWorker::SealExisting(const ObjectID &object_id, bool pin_object,
                                const ObjectID &generator_id, ...) {
  RAY_RETURN_NOT_OK(plasma_store_provider_->Seal(object_id));  // 1. Seal
  if (pin_object) {
    // 2. ★ 主动请求本地 raylet pin，传入 owner_address 和 generator_id
    local_raylet_rpc_client_->PinObjectIDs(
        owner_address != nullptr ? *owner_address : rpc_address_,
        {object_id},
        generator_id,
        [this, object_id](const Status &status, const rpc::PinObjectIDsReply &reply) {
          if (!status.ok()) {
            RAY_LOG(ERROR) << "Request to local raylet to pin object failed: " << status;
            return;
          }
          // 3. raylet 回复后 Release plasma client 的引用
          if (!plasma_store_provider_->Release(object_id).ok()) {
            RAY_LOG(ERROR) << "Failed to release object, might cause a leak in plasma.";
          }
        });
  }
  memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
                     object_id, reference_counter_->HasReference(object_id));
  return Status::OK();
}
```

**关键：`PinObjectIDs` 传入了 `owner_address`（owner worker 的地址），不是 executor 的地址。** raylet 用这个地址来订阅 owner 的 eviction 消息。

#### ReportGeneratorItemReturns — Seal 之后才上报 (`core_worker.cc:3365`)

```cpp
Status CoreWorker::ReportGeneratorItemReturns(...) {
  if (!dynamic_return_object.first.IsNil()) {
    // ★ SerializeReturnObject 中：
    //   如果 IsPlasmaBuffer() → in_plasma = true（只传元数据，不发数据）
    //   如果是内存 buffer → in_plasma = false（发数据内容）
    SerializeReturnObject(dynamic_return_object.first,
                          dynamic_return_object.second,
                          request.mutable_returned_object());
  }
  client->ReportGeneratorItemReturns(std::move(request), ...);
}
```

### 总结：对象在哪里 Seal

| 对象类型 | Create + Seal 位置 | 是否触发 HandleObjectAdded |
|----------|-------------------|---------------------------|
| 大对象（Plasma buffer） | **Executor (新 worker)** | **是** — 在 executor 的 NodeB raylet 上立即触发 |
| 小对象（inline） + store_in_plasma=true | **Owner 本地 Plasma**（通过 `put_in_local_plasma_callback_`） | **是** — 在 owner 节点的 raylet 上触发 |
| 小对象（inline） + store_in_plasma=false | 不写 Plasma | 否 — 只写入内存存储 |

---

## 6. 多余对象的清除机制

### 核心机制：Eviction Subscription（发布-订阅）

Ray 使用基于 gRPC long-polling 的发布-订阅模式。当 raylet pin 了一个对象后，会订阅 owner 的 `WORKER_OBJECT_EVICTION` 频道。当 owner 的引用计数认为该对象可以释放时，owner 发布 eviction 消息，raylet 收到后释放本地对象。

### Raylet 端：Pin + 订阅

`local_object_manager.cc:31`:

```cpp
void LocalObjectManager::PinObjectsAndWaitForFree(
    const std::vector<ObjectID> &object_ids, ...,
    const rpc::Address &owner_address,
    const ObjectID &generator_id) {

  // 1. Pin 对象到本地
  const auto inserted = local_objects_.emplace(
      object_id, LocalObjectInfo(owner_address, generator_id, object->GetSize()));
  if (inserted.second) {
    pinned_objects_size_ += object->GetSize();
    pinned_objects_.emplace(object_id, std::move(object));
  } else {
    // 已 pin 过
    auto original_worker_id = WorkerID::FromBinary(inserted.first->second.owner_address_.worker_id());
    auto new_worker_id = WorkerID::FromBinary(owner_address.worker_id());
    if (original_worker_id != new_worker_id) {
      // TODO(swang): Handle this case. We should use the new owner address
      // and object copy.
      RAY_LOG(WARNING)
          << "Received PinObjects request from a different owner " << new_worker_id
          << " from the original " << original_worker_id << ". Object " << object_id
          << " may get freed while the new owner still has the object in scope.";
    }
    continue;
  }

  // 2. ★ 订阅 owner 的 WORKER_OBJECT_EVICTION 频道
  auto subscription_callback = [this, owner_address](const rpc::PubMessage &msg) {
    const auto &object_eviction_msg = msg.worker_object_eviction_message();
    const auto obj_id = ObjectID::FromBinary(object_eviction_msg.object_id());
    ReleaseFreedObject(obj_id);  // 收到 eviction 消息 → 释放
    core_worker_subscriber_->Unsubscribe(
        rpc::ChannelType::WORKER_OBJECT_EVICTION, owner_address, obj_id.Binary());
  };

  auto owner_dead_callback = [this, owner_address](const std::string &object_id_binary, ...) {
    const auto obj_id = ObjectID::FromBinary(object_id_binary);
    ReleaseFreedObject(obj_id);  // owner 死了 → 释放
  };

  core_worker_subscriber_->Subscribe(
      std::move(sub_message),
      rpc::ChannelType::WORKER_OBJECT_EVICTION,
      owner_address,           // ★ 订阅目标：owner worker
      object_id.Binary(),      // 订阅 key：object_id
      subscription_callback,
      owner_dead_callback);
}
```

### ReleaseFreedObject → Plasma 删除

`local_object_manager.cc:111`:

```cpp
void LocalObjectManager::ReleaseFreedObject(const ObjectID &object_id) {
  auto it = local_objects_.find(object_id);
  if (it == local_objects_.end() || it->second.is_freed_) {
    return;
  }
  it->second.is_freed_ = true;

  auto pinned_objects_it = pinned_objects_.find(object_id);
  if (pinned_objects_it != pinned_objects_.end()) {
    pinned_objects_size_ -= pinned_objects_it->second->GetSize();
    pinned_objects_.erase(pinned_objects_it);
    local_objects_.erase(it);
  }

  // 加入待删除队列
  objects_pending_deletion_.emplace(object_id);
  FlushFreeObjects();
}
```

`FlushFreeObjects` (`local_object_manager.cc:150`):

```cpp
void LocalObjectManager::FlushFreeObjects() {
  if (!objects_pending_deletion_.empty()) {
    std::vector<ObjectID> objects_to_delete(objects_pending_deletion_.begin(),
                                            objects_pending_deletion_.end());
    on_objects_freed_(objects_to_delete);  // → ObjectManager::FreeObjects
    objects_pending_deletion_.clear();
  }
}
```

`main.cc:886`:

```cpp
/*on_objects_freed=*/
[&](const std::vector<ray::ObjectID> &object_ids) {
  object_manager->FreeObjects(object_ids, /*local_only=*/false);
},
```

---

## 7. Lineage Ref 与 OutOfScope 判断

### Reference 中的各种 ref count

`reference_counter.h:348`:

```cpp
/// The reference count. This number includes:
/// - Python references to the ObjectID.
/// - Pending submitted tasks that depend on the object.
/// - ObjectIDs containing this ObjectID that we own and that are still in scope.
size_t RefCount() const {
  return local_ref_count + submitted_task_ref_count +
         nested().contained_in_owned.size();
}
// ★ RefCount() 不包含 lineage_ref_count！

size_t lineage_ref_count = 0;          // 依赖此对象的可重试任务数
size_t local_ref_count = 0;           // Python 层引用数
size_t submitted_task_ref_count = 0;   // 已提交但未完成的任务引用数
```

### OutOfScope 判断

`reference_counter.h:359`:

```cpp
bool OutOfScope(bool lineage_pinning_enabled) const {
  bool in_scope = RefCount() > 0;
  bool is_nested = !nested().contained_in_borrowed_ids.empty();
  bool has_borrowers = !borrow().borrowers.empty();
  bool was_stored_in_objects = !borrow().stored_in_objects.empty();

  bool has_lineage_references = false;
  if (lineage_pinning_enabled && owned_by_us_ &&
      lineage_eligibility_ != LineageReconstructionEligibility::ELIGIBLE) {
    // ★ 只有非 ELIGIBLE 时 lineage_ref_count 才影响 OutOfScope
    has_lineage_references = lineage_ref_count > 0;
  }
  // ELIGIBLE 时 has_lineage_references 永远是 false

  return !(in_scope || is_nested || has_nested_refs_to_report || has_borrowers ||
           was_stored_in_objects || has_lineage_references);
}
```

### ShouldDelete 判断

`reference_counter.h:380`:

```cpp
bool ShouldDelete(bool lineage_pinning_enabled) const {
  if (lineage_pinning_enabled) {
    // ★ OutOfScope 且 lineage_ref_count == 0 才能删除
    return OutOfScope(lineage_pinning_enabled) && (lineage_ref_count == 0);
  } else {
    return OutOfScope(lineage_pinning_enabled);
  }
}
```

### lineage_ref_count 的增减

**增加：任务提交时** (`reference_counter.cc:497`):

```cpp
void UpdateSubmittedTaskReferences(return_ids, argument_ids_to_add, ...) {
  for (const ObjectID &argument_id : argument_ids_to_add) {
    it->second.submitted_task_ref_count++;
    // ★ 每个依赖此对象的新任务提交时，lineage_ref_count +1
    it->second.lineage_ref_count++;
  }
}
```

**减少：任务完成且不可重试时** (`reference_counter.cc:609`):

```cpp
void RemoveSubmittedTaskReferences(argument_ids, bool release_lineage, ...) {
  for (const ObjectID &argument_id : argument_ids) {
    it->second.submitted_task_ref_count--;
    if (release_lineage) {
      // ★ release_lineage=true → 任务完成且不可再重试 → lineage_ref_count--
      if (it->second.lineage_ref_count > 0) {
        it->second.lineage_ref_count--;
      }
    }
    if (it->second.RefCount() == 0) {
      DeleteReferenceInternal(it, deleted);
    }
  }
}
```

**减少：lineage 被 evict 时** (`reference_counter.cc:569`):

```cpp
int64_t ReleaseLineageReferences(ReferenceTable::iterator ref) {
  for (const ObjectID &argument_id : argument_ids) {
    arg_it->second.lineage_ref_count--;
    if (arg_it->second.OutOfScope(lineage_pinning_enabled_)) {
      OnObjectOutOfScopeOrFreed(arg_it);  // ★ 触发 eviction
    }
    if (arg_it->second.ShouldDelete(lineage_pinning_enabled_)) {
      EraseReference(arg_it);  // 从 object_id_refs_ 中删除
    }
  }
}
```

**重提交时：只增加 submitted_task_ref_count，不增加 lineage_ref_count** (`reference_counter.cc:529`):

```cpp
void UpdateResubmittedTaskReferences(const std::vector<ObjectID> &argument_ids) {
  for (const ObjectID &argument_id : argument_ids) {
    it->second.submitted_task_ref_count++;
    // ★ lineage_ref_count 不增加！因为 lineage 已经在首次提交时计数过了
  }
}
```

### ELIGIBLE vs 非 ELIGIBLE 对 OutOfScope 的影响

| lineage_eligibility_ | RefCount=0, lineage_ref>0 | OutOfScope | Plasma 副本 | 含义 |
|---|---|---|---|---|
| **ELIGIBLE** | has_lineage_references = false | **true** | **释放** | 可重建，不需要保留副本 |
| **INELIGIBLE_PUT** | has_lineage_references = true | **false** | **保留** | 不可重建，必须保留副本 |

**ELIGIBLE** = 对象可以通过重新执行产生它的 task 来恢复。所以即使 `RefCount=0`（应用层不再使用），只要 `lineage_ref_count > 0`（还有任务可能重试需要它），系统**不需要保留 Plasma 副本**——丢了可以重建。

**非 ELIGIBLE** = 对象**不能通过重执行恢复**。如果 Plasma 副本丢了就永久丢失。所以 `lineage_ref_count > 0` 时**必须保留 Plasma 副本**。

### 为什么 OutOfScope 不等于可以从 ref table 中删除

`DeleteReferenceInternal`（`reference_counter.cc:740`）是 ref count 归零时的核心处理函数，它分两步执行：

```cpp
void DeleteReferenceInternal(ReferenceTable::iterator it, ...) {
  // ─── 第一步：OutOfScope 处理 ───
  if (it->second.OutOfScope(lineage_pinning_enabled_)) {
    // 递归处理嵌套引用
    for (const auto &inner_id : it->second.nested().contains) {
      // ... 递归 DeleteReferenceInternal(inner_it) ...
    }
    // ★ 触发 eviction callbacks，释放 Plasma 副本
    OnObjectOutOfScopeOrFreed(it);
    // 从 reconstructable_owned_objects_ 中移除
  }

  // ─── 第二步：ShouldDelete 判断 ───
  if (it->second.ShouldDelete(lineage_pinning_enabled_)) {
    // ★ 只有 ShouldDelete=true 才从 object_id_refs_ 中删除
    ReleaseLineageReferences(it);  // 递归减少依赖对象的 lineage_ref_count
    EraseReference(it);            // 从 object_id_refs_ 中删除
  }
}
```

**OutOfScope 和 ShouldDelete 的职责分离**：

| 函数 | 职责 | 效果 |
|------|------|------|
| `OutOfScope == true` | 触发 `OnObjectOutOfScopeOrFreed` | 释放 Plasma 副本（eviction）、清空 callbacks、unset pinned_at |
| `ShouldDelete == true` | 触发 `EraseReference` | 从 `object_id_refs_` 中删除、从 `freed_objects_` 中删除、调用 ref deleted callbacks |

**为什么不能 OutOfScope 就直接从 ref table 中删除？**

因为 `lineage_ref_count > 0` 时，对象虽然已经 OutOfScope（Plasma 副本可以释放），但还有任务可能在未来重试时需要此对象的 lineage 信息。此时：

1. **需要保留 ref 记录**：ref table 中保存了 `lineage_ref_count`、`lineage_eligibility_`、owner 信息等。如果删除了，后续 lineage 释放时无法正确处理。
2. **需要保留 ownership 信息**：其他 worker 可能需要查询此对象的 owner，如果从 ref table 中删除，查询会失败。
3. **需要保留 `object_ref_deleted_callbacks`**：某些组件可能注册了"ref 完全删除"的回调（如 generator stream 的清理），只有 `ShouldDelete=true` 时才应该触发。

**`ShouldDelete` 要求 `lineage_ref_count == 0` 的根本原因**：

`lineage_ref_count` 表示"还有多少个可重试的任务依赖此对象"。只要它 > 0，就意味着某个任务可能在未来重试，需要通过 lineage 链找到此对象。如果此时从 ref table 中删除，lineage 链就断了，重试时无法正确恢复依赖关系。

只有当 `lineage_ref_count == 0`（所有依赖此对象的任务都不可能再重试了），才能安全删除 ref 记录。

### DeleteReferenceInternal 的完整两步流程

```
DeleteReferenceInternal(it):
│
├── RefCount() == 0 && publish_ref_removed → PublishRefRemovedInternal
│   (通知 borrower：我们不再借用此对象)
│
├── 第一步: OutOfScope 检查
│   if (OutOfScope):
│   ├── 递归处理嵌套引用 (nested().contains)
│   │   └── 对每个 inner_id: DeleteReferenceInternal(inner_it)
│   ├── OnObjectOutOfScopeOrFreed(it):
│   │   ├── 调用 on_object_out_of_scope_or_freed_callbacks (如 unpin_object)
│   │   │   → 发布 WORKER_OBJECT_EVICTION → raylet 释放 Plasma 副本
│   │   ├── 清空 callbacks
│   │   ├── UnsetObjectPrimaryCopy:
│   │   │   ├── pinned_at_node_id_.reset()
│   │   │   └── 清除 spilled 信息
│   │   └── 更新 owned object 计数器
│   └── 从 reconstructable_owned_objects_ 中移除
│
└── 第二步: ShouldDelete 检查
    if (ShouldDelete):  // OutOfScope && lineage_ref_count == 0
    ├── ReleaseLineageReferences(it):
    │   ├── 调用 on_lineage_released_ callback (从 task_manager 获取依赖链)
    │   │   └── 如果对象仍 in scope 且 ELIGIBLE → 标记为 INELIGIBLE_LINEAGE_EVICTED
    │   └── 对每个 argument_id:
    │       ├── lineage_ref_count--
    │       ├── if OutOfScope → OnObjectOutOfScopeOrFreed (递归触发 eviction)
    │       └── if ShouldDelete → ReleaseLineageReferences + EraseReference (递归删除)
    └── EraseReference(it):
        ├── PublishFailure (WORKER_OBJECT_LOCATIONS_CHANNEL)
        ├── freed_objects_.erase
        ├── 更新计数器 (num_objects_owned_by_us_--)
        ├── 调用 object_ref_deleted_callbacks
        └── object_id_refs_.erase  ★ 最终从 ref table 中删除
```

### OnObjectOutOfScopeOrFreed 与 EraseReference 的区别

| 操作 | `OnObjectOutOfScopeOrFreed` | `EraseReference` |
|------|---------------------------|-----------------|
| **触发条件** | `OutOfScope == true` | `ShouldDelete == true` (即 OutOfScope && lineage_ref==0) |
| **效果** | 释放 Plasma 副本、清空 callbacks、unset pinned_at | 从 ref table 中删除、从 freed_objects_ 中删除、调用 ref deleted callbacks |
| **对象是否还在 ref table** | **在** (除非 ShouldDelete 也为 true) | **不在** |
| **lineage_ref_count 要求** | 无要求 | 必须为 0 |
| **可逆性** | 不可逆（callbacks 清空） | 不可逆（ref 记录删除） |

### 完整状态转移图

```
                         ┌────────────────────────────────────────────────────────────────┐
                         │                    对象在 object_id_refs_ 中                      │
                         │                                                                │
                    ┌────┴────┐                                                      ┌────┴────┐
                    │ 创建对象  │                                                      │ 被 freed │
                    └────┬────┘                                                      └────┬────┘
                         │                                                                │
                    AddOwnedObject                                                  FreePlasmaObjects
                    AddOwnedObjectInternal                                          freed_objects_.insert
                    object_id_refs_.insert                                          OnObjectOutOfScopeOrFreed
                                                                                         │
                         │                                                                │
                    ┌────┴──────────────────────────┐                                 ┌────┴────┐
                    │ RefCount > 0                  │                                 │被 freed  │
                    │ (应用层在使用)                 │                                 │(Plasma已释放)│
                    │ OutOfScope = false            │                                 │ref还在    │
                    │ ShouldDelete = false          │                                 └────┬────┘
                    │ Plasma 副本保留               │                                      │
                    │ freed_objects_: 无            │                                      │
                    └────┬──────────────────────────┘                                      │
                         │                                                                 │
                    RemoveLocalReference /                                              AddObjectOutOfScopeOrFreedCallback
                    RemoveSubmittedTaskReferences                                       → 分支3: return false → 立即 unpin
                         │
                    RefCount() == 0
                         │
                    ┌────┴──────────────────────────────────────────────────────────┐
                    │                   RefCount == 0                               │
                    │                                                              │
                    │    DeleteReferenceInternal 被调用                              │
                    └────┬─────────────────────────────────────────┬────────────────┘
                         │                                         │
                    ┌────┴──────────────────┐               ┌──────┴──────────────────┐
                    │ ELIGIBLE               │               │ 非 ELIGIBLE (如 PUT)     │
                    │                        │               │                          │
                    │ OutOfScope = true      │               │ lineage_ref > 0:         │
                    │ (不看 lineage_ref)     │               │   has_lineage_refs = true│
                    │                        │               │   OutOfScope = false     │
                    │ → OnObjectOutOfScope   │               │   → 不触发 eviction     │
                    │   OrFreed              │               │   → Plasma 副本保留      │
                    │ → Plasma 副本释放      │               │                          │
                    │ → callbacks 清空       │               │ lineage_ref == 0:       │
                    │ → pinned_at reset      │               │   has_lineage_refs = false│
                    │                        │               │   OutOfScope = true      │
                    │ ShouldDelete?          │               │   → 同 ELIGIBLE 路径     │
                    │ lineage_ref == 0?      │               └──────────────────────────┘
                    └────┬───────────┬───────┘
                         │           │
                    ┌────┴────┐ ┌────┴──────────────────┐
                    │yes      │ │no (lineage_ref > 0)   │
                    │         │ │                       │
                    │EraseRef │ │保留在 object_id_refs_  │
                    │         │ │callbacks 已清空        │
                    │object_id│ │pinned_at 已 reset     │
                    │_refs_   │ │Plasma 副本已释放       │
                    │.erase() │ │                       │
                    │freed_   │ │后续 AddOutOfScope...  │
                    │objects_ │ │Callback:              │
                    │.erase() │ │→ 分支2: return false  │
                    │         │ │  (OutOfScope &&       │
                    │ref      │ │   !ShouldDelete)      │
                    │deleted  │ │  → 立即 unpin_object  │
                    │callbacks│ │                       │
                    │调用     │ │等 lineage 释放:        │
                    └────┬────┘ │lineage_ref_count-- → 0 │
                         │      │→ ShouldDelete = true   │
                    对象完全    │→ EraseReference        │
                    从 ref     │→ object_id_refs_.erase │
                    table 中   └────┬───────────────────┘
                    删除             │
                               最终删除 (同左边路径)
```

### `ReleaseLineageReferences` 的递归清理

当 `ShouldDelete=true` 触发 `EraseReference` 前，会先调用 `ReleaseLineageReferences`，它会**递归减少依赖对象的 lineage_ref_count**：

```cpp
// reference_counter.cc:569
int64_t ReleaseLineageReferences(ReferenceTable::iterator ref) {
  // 1. 调用 on_lineage_released_ callback → 从 task_manager 获取此对象的依赖链
  lineage_bytes_evicted += on_lineage_released_(ref->first, &argument_ids);

  // 2. 如果对象仍 in scope 且 ELIGIBLE → 标记为 INELIGIBLE_LINEAGE_EVICTED
  //    (lineage 已 evict，不能再通过重执行恢复)
  if (!ref->second.OutOfScope(lineage_pinning_enabled_) &&
      ref->second.lineage_eligibility_ == LineageReconstructionEligibility::ELIGIBLE) {
    ref->second.lineage_eligibility_ = INELIGIBLE_LINEAGE_EVICTED;
  }

  // 3. 对每个依赖对象，递归减少 lineage_ref_count
  for (const ObjectID &argument_id : argument_ids) {
    arg_it->second.lineage_ref_count--;

    // 4. 如果依赖对象因此变为 OutOfScope → 触发 eviction
    if (arg_it->second.OutOfScope(lineage_pinning_enabled_)) {
      OnObjectOutOfScopeOrFreed(arg_it);
    }

    // 5. 如果依赖对象因此可以 ShouldDelete → 递归删除
    if (arg_it->second.ShouldDelete(lineage_pinning_enabled_)) {
      ReleaseLineageReferences(arg_it);  // ★ 递归
      EraseReference(arg_it);
    }
  }
}
```

**这就是为什么不能 OutOfScope 就删除 ref 记录的原因**：ref 记录中保存了 lineage 依赖关系，`ReleaseLineageReferences` 需要通过 ref 记录找到依赖链中的上游对象，递归减少它们的 `lineage_ref_count`。如果提前删除了 ref 记录，这个递归清理就无法进行，会导致上游对象的 `lineage_ref_count` 永远无法归零，造成 ref table 中的内存泄漏。

---

## 8. AddObjectOutOfScopeOrFreedCallback 详细分支逻辑

`reference_counter.cc:873`:

```cpp
bool ReferenceCounter::AddObjectOutOfScopeOrFreedCallback(
    const ObjectID &object_id, const std::function<void(const ObjectID &)> callback) {
  absl::MutexLock lock(&mutex_);
  auto it = object_id_refs_.find(object_id);

  // ─── 分支1: 对象不在 ref table 中（已完全 GC）───
  if (it == object_id_refs_.end()) {
    return false;  // → 立即 unpin
  }

  // ─── 分支2: 对象已 OutOfScope 但不能 Delete（lineage_ref > 0）───
  else if (it->second.OutOfScope(lineage_pinning_enabled_) &&
           !it->second.ShouldDelete(lineage_pinning_enabled_)) {
    // OutOfScope=true → OnObjectOutOfScopeOrFreed 已执行 → callbacks 已清空
    // ShouldDelete=false → 对象未被 EraseReference → 还在 ref table 中
    // ★ 注册新 callback 也没用，因为 OnObjectOutOfScopeOrFreed 不会再次执行
    return false;  // → 立即 unpin
  }

  // ─── 分支3: 对象已被 freed（应用层主动 free）───
  else if (freed_objects_.contains(object_id)) {
    return false;  // → 立即 unpin
  }

  // ─── 分支4: 对象还在 scope → 注册回调 ───
  it->second.on_object_out_of_scope_or_freed_callbacks.emplace_back(callback);
  return true;  // → 等 OutOfScope 时触发
}
```

### 为什么分支2需要 `!ShouldDelete`

`DeleteReferenceInternal` (`reference_counter.cc:740`) 在同一个 mutex 临界区内顺序执行：

```cpp
void DeleteReferenceInternal(ReferenceTable::iterator it, ...) {
  if (it->second.OutOfScope(...)) {
    OnObjectOutOfScopeOrFreed(it);  // ① 触发并清空 callbacks
  }
  if (it->second.ShouldDelete(...)) {
    EraseReference(it);             // ② 从 ref table 中删除
  }
}
```

由于 mutex 互斥，外部不可能在 ① 和 ② 之间插入 `AddObjectOutOfScopeOrFreedCallback`。

`OutOfScope=true` 后两种结果：

| lineage_ref_count | ShouldDelete | ① 执行 | ② 执行 | 对象在 ref table？ | callbacks 状态 |
|:-:|:-:|:-:|:-:|:-:|:-:|
| > 0 | false | ✓ 清空 | ✗ | **在** | **已清空** |
| == 0 | true | ✓ 清空 | ✓ erase | **不在** | 已清空 |

**lineage_ref > 0 时**：对象还在 ref table，过了分支1，到分支2：`OutOfScope && !ShouldDelete` → `true && true` = `true` → 返回 false → 立即 unpin。

**lineage_ref == 0 时**：对象已被 EraseReference，在分支1 `find() == end` → 直接返回 false。

**两种情况都返回 false，都立即 unpin，只是走不同分支。**

`!ShouldDelete` 的作用是精确匹配分支2要处理的情况："对象还在 ref table 中（过了分支1），但 callbacks 已经被清空了不能再注册（OutOfScope 已触发但对象未被 erase，即 lineage_ref > 0）"。

`!ShouldDelete` 就是 `lineage_ref_count > 0` 的等价表达——它精确描述了"callbacks 已清空但对象还在 ref table"的唯一状态。

### 调用处

`core_worker.cc:3814`:

```cpp
// Owner 收到 raylet 的订阅请求
if (!reference_counter_->AddObjectOutOfScopeOrFreedCallback(object_id, unpin_object)) {
  // ★ 返回 false → 立即 unpin
  unpin_object(object_id);
  RAY_LOG(DEBUG) << "Reference for object has already been freed.";
}
// 返回 true → 回调已注册，等 OutOfScope 时自动触发
```

---

## 9. ProcessSubscribeForObjectEviction 与 Eviction 订阅机制

### 基于 gRPC Long-Polling 的发布-订阅

Ray 使用 long-polling 模式实现发布-订阅，不依赖外部消息中间件。

- **Publisher**（Owner worker）：持有 `object_info_publisher_`
- **Subscriber**（NodeB raylet）：持有 `core_worker_subscriber_`
- 两个 gRPC 接口：
  - `PubsubCommandBatch`：发送订阅/取消订阅命令
  - `PubsubLongPolling`：接收发布消息

### 完整触发链路

```
┌─────────────────┐                          ┌─────────────────┐
│  NodeB Raylet    │                          │  Owner Worker    │
│  (Subscriber)   │                          │  (Publisher)    │
└────────┬────────┘                          └────────┬────────┘
         │                                            │
    1. PinObjectsAndWaitForFree                       │
       → core_worker_subscriber_->Subscribe(...)      │
         │                                            │
    2. Subscribe() 内部做三件事:                        │
       ├─ 命令入队: commands_[publisher_id].emplace()  │
       ├─ 发送命令: SendCommandBatchIfPossible()       │─── gRPC PubsubCommandBatch ───→
       │                                            3. HandlePubsubCommandBatch
       │                                               → ProcessSubscribeMessage
       │                                               → ProcessSubscribeForObjectEviction
       │                                               → AddObjectOutOfScopeOrFreedCallback
       │                                               ←── gRPC reply ───┤
       │                                            │
       ├─ 建立长轮询: MakeLongPollingConnection()      │
       │   → gRPC PubsubLongPolling ─────────────────────→ HandlePubsubLongPolling
       │                                            │   → ConnectToSubscriber (挂起)
       │                                            │
       └─ 注册本地回调: Channel->Subscribe(...)        │
         │                                            │
         │            ... 时间流逝 ...                  │
         │                                            │
         │                                            6. 对象 OutOfScope 时:
         │                                               OnObjectOutOfScopeOrFreed
         │                                               → unpin_object callback
         │                                               → object_info_publisher_->Publish()
         │                                               ←── long polling 回复 eviction ───┤
         │                                            │
    7. 收到 long polling 回复                           │
       → Channel->HandlePublishedMessage               │
       → subscription_callback(msg)                    │
       → ReleaseFreedObject → FlushFreeObjects         │
       → FreeObjects → Plasma 删除                     │
```

### Subscriber::Subscribe 内部

`subscriber.cc:257`:

```cpp
void Subscriber::Subscribe(std::unique_ptr<rpc::SubMessage> sub_message,
                           rpc::ChannelType channel_type,
                           const rpc::Address &publisher_address, ...) {
  // ① 将订阅命令入队
  auto command = std::make_unique<CommandItem>();
  command->cmd.set_channel_type(channel_type);
  command->cmd.set_key_id(*key_id);
  command->cmd.mutable_subscribe_message()->Swap(sub_message.get());
  commands_[publisher_id].emplace(std::move(command));

  // ② 通过 gRPC PubsubCommandBatch 发送给 publisher
  SendCommandBatchIfPossible(publisher_address);

  // ③ 建立 long polling 连接（如果还没有）
  MakeLongPollingConnectionIfNotConnected(publisher_address);

  // ④ 在本地 channel 中注册 subscription_callback 和 owner_dead_callback
  this->Channel(channel_type)->Subscribe(
      publisher_address, key_id,
      std::move(subscription_callback),
      std::move(subscription_failure_callback));
}
```

### ProcessSubscribeForObjectEviction 核心逻辑

`core_worker.cc:3765`:

```cpp
void CoreWorker::ProcessSubscribeForObjectEviction(
    const rpc::WorkerObjectEvictionSubMessage &message) {

  // 定义 unpin 回调
  auto unpin_object = [this](const ObjectID &object_id) {
    // 发布 eviction 消息给所有订阅者
    rpc::PubMessage pub_message;
    pub_message.set_key_id(object_id.Binary());
    pub_message.set_channel_type(rpc::ChannelType::WORKER_OBJECT_EVICTION);
    pub_message.mutable_worker_object_eviction_message()->set_object_id(object_id.Binary());
    object_info_publisher_->Publish(std::move(pub_message));
  };

  const auto object_id = ObjectID::FromBinary(message.object_id());
  const auto intended_worker_id = WorkerID::FromBinary(message.intended_worker_id());

  // ★ 检查1: 如果订阅不是给当前 worker 的 → 立即 unpin
  if (intended_worker_id != worker_context_->GetWorkerID()) {
    unpin_object(object_id);
    return;
  }

  // ★ 检查2: 如果有 generator_id → 先确保 owner 知道这个对象存在
  if (message.has_generator_id()) {
    const auto generator_id = ObjectID::FromBinary(message.generator_id());
    if (task_manager_->ObjectRefStreamExists(generator_id)) {
      // streaming generator → 临时拥有这个 ref
      task_manager_->TemporarilyOwnGeneratorReturnRefIfNeeded(object_id, generator_id);
    } else {
      // dynamic generator → 添加动态返回
      reference_counter_->AddDynamicReturn(object_id, generator_id);
    }
  }

  // ★ 检查3: 尝试注册 OutOfScope 回调
  if (!reference_counter_->AddObjectOutOfScopeOrFreedCallback(object_id, unpin_object)) {
    // 返回 false → 对象已 OutOfScope / freed / 不存在
    // ★ 立即发布 eviction 消息 → raylet 收到后释放对象
    unpin_object(object_id);
  }
  // 返回 true → 回调已注册，等对象 OutOfScope 时自动触发
}
```

### HandlePubsubCommandBatch 入口

`core_worker.cc:3858`:

```cpp
void CoreWorker::HandlePubsubCommandBatch(rpc::PubsubCommandBatchRequest request, ...) {
  const auto subscriber_id = NodeID::FromBinary(request.subscriber_id());
  for (const auto &command : request.commands()) {
    if (command.has_unsubscribe_message()) {
      object_info_publisher_->UnregisterSubscription(...);
    } else {
      ProcessSubscribeMessage(
          command.subscribe_message(),
          command.channel_type(),
          command.key_id(),
          subscriber_id);
    }
  }
  send_reply_callback(Status::OK(), ...);
}
```

### ProcessSubscribeMessage 分发

`core_worker.cc:3822`:

```cpp
StatusSet<StatusT::InvalidArgument> ProcessSubscribeMessage(...) {
  object_info_publisher_->RegisterSubscription(channel_type, subscriber_id, key_id);

  if (sub_message.has_worker_object_eviction_message()) {
    ProcessSubscribeForObjectEviction(sub_message.worker_object_eviction_message());
  } else if (sub_message.has_worker_ref_removed_message()) {
    ProcessSubscribeForRefRemoved(sub_message.worker_ref_removed_message());
  } else {
    ProcessSubscribeObjectLocations(sub_message.worker_object_locations_message());
  }
}
```

---

## 10. 完整 Recovery 交互时序

### 场景设定

- Streaming generator task 首次在 NodeA 执行，产出 `obj1, obj2, obj3`
- 应用层已消费完 `obj1`（Python ref 释放）
- `obj2` 还在被使用
- **NodeA 被 kill**
- Recovery 重新执行到 NodeB

### 阶段一：首次执行（NodeA）

```
NodeA Executor → 产出 obj1, obj2, obj3

每个对象：
  AllocateReturnObject → CreateExisting (Plasma Create on NodeA)
  SealReturnObject → SealExisting → Plasma Seal → HandleObjectAdded (NodeA raylet)
    → PinObjectIDs(owner_address, generator_id)
    → NodeA raylet: Pin + 订阅 owner 的 WORKER_OBJECT_EVICTION

  ReportGeneratorItemReturns → Owner
    → InsertToStream(obj, index) → refs_written_to_stream_
    → OwnDynamicStreamingTaskReturnRef → AddOwnedObjectInternal
    → HandleTaskReturn(in_plasma=true)
      → UpdateObjectPinnedAtRaylet(obj, NodeA)
      → in_memory_store_.Put(OBJECT_IN_PLASMA)

Owner 状态:
  object_id_refs_: {obj1, obj2, obj3}  (全部 owned, ELIGIBLE)
  pinned_at_node_id_: {obj1: NodeA, obj2: NodeA, obj3: NodeA}
  reconstructable_return_ids_: {obj1, obj2, obj3}
  object_ref_streams_: {generator_id → stream with obj1, obj2, obj3}
  next_index_: 消费进度（obj1 已消费 → next_index >= 1）
```

### 阶段二：应用层消费 obj1

```
应用层 del obj1 → RemoveLocalReference(obj1)
  → RefCount() == 0
  → DeleteReferenceInternal
    → OutOfScope?
      → RefCount == 0 ✓
      → ELIGIBLE → has_lineage_references = false (不看 lineage_ref_count)
      → OutOfScope = true ★
    → OnObjectOutOfScopeOrFreed
      → 调用 on_object_out_of_scope_or_freed_callbacks (即 unpin_object)
      → ★ 清空 callbacks
      → Publish WORKER_OBJECT_EVICTION
      → NodeA raylet 收到 → ReleaseFreedObject → Plasma 删除 obj1
      → UnsetObjectPrimaryCopy → pinned_at_node_id_.reset()
    → ShouldDelete?
      → OutOfScope(true) && lineage_ref_count==0 → false (lineage_ref>0)
      → ★ 不删除 ref，保留在 object_id_refs_ 中

  → RemoveLineageReference(obj1)  (从 task_manager 触发)
    → reconstructable_return_ids_.erase(obj1)

Owner 状态:
  object_id_refs_: {obj1, obj2, obj3}  (obj1 仍在，但 callbacks 已清空)
  obj1: OutOfScope=true, ShouldDelete=false, callbacks=空, pinned_at=nil
  obj2/obj3: 正常
  reconstructable_return_ids_: {obj2, obj3}
```

### 阶段三：NodeA 被 Kill

```
Owner 检测到 NodeA 死亡 → ResetObjectsOnRemovedNode(NodeA)

  遍历 object_id_refs_:
    obj1: pinned_at 已 reset → 不处理
    obj2: pinned_at == NodeA → UnsetObjectPrimaryCopy → objects_to_recover_.push_back(obj2)
    obj3: pinned_at == NodeA → UnsetObjectPrimaryCopy → objects_to_recover_.push_back(obj3)

Owner 状态:
  objects_to_recover_: [obj2, obj3]
  ★ obj1 不在 objects_to_recover_ 中（pinned_at 已 reset，且 lineage 仍在）
```

### 阶段四：Recovery 触发

```
Owner 定期心跳 (每 100ms):
  FlushObjectsToRecover() → [obj2, obj3]
  memory_store_->Delete([obj2, obj3])

  for obj2: RecoverObject(obj2)
    → needs recovery (pinned_at=Nil, spilled=false)
    → objects_pending_recovery_.insert(obj2) → 成功
    → in_memory_store_.GetAsync(obj2, recovery_complete_callback)
    → object_lookup_(obj2, lookup_callback)

  → object_lookup 回调: NodeA 已死，没有 locations
  → PinOrReconstructObject: locations 为空
  → ReconstructObject(obj2)
    → GetLineageReconstructionEligibility → ELIGIBLE
    → ResubmitTask(task_id)
```

### 阶段五：Task 重提交到 NodeB

```
ResubmitTask → spec.SetIsReconstruction(true)
  → UpdateReferencesForResubmit:
    → UpdateResubmittedTaskReferences(task_deps)
      ★ 只增加 submitted_task_ref_count，不增加 lineage_ref_count
    → TryMarkFreedObjectInUseAgain: 如果依赖对象之前被 freed → 重新标记为 in-use
  → async_retry_task_callback_(spec, 0)
    → 正常调度路径 → 调度到 NodeB
```

### 阶段六：NodeB 重新执行 Task

```
NodeB Executor 执行 task → 从头产出 obj1, obj2, obj3

对每个对象（以 obj1 为例）:

6a. AllocateReturnObject
    → 大对象: CreateExisting → Plasma Create on NodeB (is_reconstruction=true)
    → 小对象: LocalMemoryBuffer (inline)

6b. SealReturnObject
    → if IsPlasmaBuffer():
      → SealExisting(obj1, pin=true, generator_id, owner_address)
        → plasma_store_provider_->Seal(obj1) → Plasma Seal on NodeB
        → ★ HandleObjectAdded on NodeB raylet
        → PinObjectIDs(owner_address, {obj1}, generator_id)
          → NodeB raylet: Pin obj1 + 订阅 owner 的 WORKER_OBJECT_EVICTION

6c. ReportGeneratorItemReturns → Owner (RPC)
    → SerializeReturnObject: in_plasma=true (只传元数据)
```

### 阶段七：Owner 处理 ReportGeneratorItemReturns

```
HandleReportGeneratorItemReturns(request)

  attempt_number 检查: 当前 AttemptNumber > request.attempt_number → 跳过（旧 attempt）

  store_in_plasma_ids = GetTaskReturnObjectsToStoreInPlasma(task_id)
    → first_execution = false
    → store_in_plasma_ids = reconstructable_return_ids_ = {obj2, obj3}
    ★ obj1 不在其中（已被移除）

对 obj1 (item_index=0):
  InsertToStream(obj1, 0)
    → item_index(0) < next_index_(>=1)? → 是 → return false (已消费)
    → index_not_used_yet = false

  OwnDynamicStreamingTaskReturnRef → 不调用 (index_not_used_yet == false)

  ★ HandleTaskReturn(obj1, returned_object, NodeB, store_in_plasma=false)
    → return_object.in_plasma() == true (obj1 在 NodeB 的 Plasma 中)
    → UpdateObjectPinnedAtRaylet(obj1, NodeB)
      → object_id_refs_.find(obj1) → 找到（obj1 仍在 ref table 中）
      → freed_objects_.contains(obj1)? → 否
      → OutOfScope?
        → RefCount==0, ELIGIBLE → has_lineage_references=false → OutOfScope=true
      → ★ OutOfScope → 跳过更新（不进入 if !OutOfScope 分支）
      → pinned_at_node_id_ 不更新
    → in_memory_store_.Put(OBJECT_IN_PLASMA, obj1, HasReference(obj1))
      → HasReference 返回 true（obj1 还在 object_id_refs_ 中）

对 obj2 (item_index=1):
  InsertToStream(obj2, 1) → true (未消费)
  OwnDynamicStreamingTaskReturnRef(obj2, generator_id)
    → AddOwnedObjectInternal(obj2) → 已存在 → return false (不重复添加)
  HandleTaskReturn(obj2, returned_object, NodeB, store_in_plasma=true)
    → in_plasma == true
    → UpdateObjectPinnedAtRaylet(obj2, NodeB)
      → obj2 在 object_id_refs_ 中
      → 未 OutOfScope → ★ 更新 pinned_at_node_id_ = NodeB
    → in_memory_store_.Put(OBJECT_IN_PLASMA, obj2, HasReference=true)

对 obj3: 同 obj2
```

### 阶段八：NodeB Raylet 的 Eviction 订阅处理

```
NodeB raylet 收到 PinObjectIDs → PinObjectsAndWaitForFree
  → Pin obj1, obj2, obj3
  → 对每个对象向 Owner 发送 WORKER_OBJECT_EVICTION 订阅
    → Subscriber::Subscribe → SendCommandBatchIfPossible → gRPC PubsubCommandBatch

Owner 收到订阅请求 → HandlePubsubCommandBatch → ProcessSubscribeMessage
  → ProcessSubscribeForObjectEviction:

对 obj1:
  → 检查 generator_id → ObjectRefStreamExists → TemporarilyOwnGeneratorReturnRefIfNeeded
    → InsertToStream: item_index(0) < next_index_ → 已消费 → return false
    → 不临时拥有
  → AddObjectOutOfScopeOrFreedCallback(obj1, unpin_object):
    → 分支1: obj1 在 object_id_refs_ 中 → 不匹配
    → 分支2: OutOfScope(true) && !ShouldDelete(true, lineage_ref>0) → ★ 匹配 → return false
    → ★ 立即 unpin_object(obj1)
      → Publish WORKER_OBJECT_EVICTION
      → NodeB raylet 收到（通过 long polling reply）
      → ReleaseFreedObject(obj1) → Plasma 删除 obj1 ★★

对 obj2:
  → AddObjectOutOfScopeOrFreedCallback(obj2, unpin_object):
    → 分支1: obj2 在 object_id_refs_ 中 → 不匹配
    → 分支2: OutOfScope(false) → 不匹配
    → 分支3: 不在 freed_objects_ → 不匹配
    → 分支4: return true → 注册回调
    → 等 obj2 ref 归零 → OnObjectOutOfScopeOrFreed → unpin_object → NodeB 释放

对 obj3: 同 obj2
```

### 阶段九：Recovery 完成

```
Owner 的 in_memory_store_.Put(OBJECT_IN_PLASMA, obj2) 触发之前注册的 GetAsync 回调:

  recovery_complete_callback(obj2):
    → objects_pending_reconstruction_.erase(obj2) → 记录成功指标
    → objects_pending_recovery_.erase(obj2)

  recovery_complete_callback(obj3): 同上

  ★ obj1 不在 objects_pending_recovery_ 中 → 没有 recovery callback
```

### 阶段十：最终清理

```
当应用层消费完 obj2, obj3:

  RemoveLocalReference(obj2) → RefCount == 0
    → DeleteReferenceInternal
      → OutOfScope → OnObjectOutOfScopeOrFreed
        → 调用 unpin_object 回调 → Publish WORKER_OBJECT_EVICTION
        → NodeB raylet 收到 → ReleaseFreedObject(obj2) → Plasma 删除
      → ShouldDelete (lineage_ref==0?) → EraseReference → 从 object_id_refs_ 中删除

  RemoveLineageReference(obj2)
    → reconstructable_return_ids_.erase(obj2)

  obj3 同理

当 obj1 的 lineage 最终释放:
  任务不可再重试 → release_lineage=true → lineage_ref_count--
  → lineage_ref_count == 0 → ShouldDelete = true
  → ReleaseLineageReferences → EraseReference → 从 object_id_refs_ 中删除
```

### 完整交互图

```
         NodeA (Kill)                    Owner                      NodeB (Recovery)
         ┌──────────┐                ┌──────────┐                ┌──────────┐
         │ Executor │                │  Worker  │                │ Executor │
         │ (Dead)   │                │ (Owner)  │                │          │
         └────┬─────┘                └────┬─────┘                └────┬─────┘
     ┌────────┴────────┐                  │                  ┌────────┴────────┐
     │ NodeA Raylet    │                  │                  │ NodeB Raylet    │
     │ (Dead)          │                  │                  │ (New)           │
     └─────────────────┘                  │                  └─────────────────┘
                                           │
    ┌──────────────────────────────────────┼──────────────────────────────────────┐
    │              阶段三: NodeA Kill       │                                      │
    │ ResetObjectsOnRemovedNode ──────────→│ objects_to_recover = [obj2, obj3]    │
    │                                      │ obj1 已 OutOfScope (ELIGIBLE)         │
    │              阶段四: Recovery 触发    │                                      │
    │                                      │ RecoverObject(obj2/obj3)              │
    │                                      │ → ResubmitTask                        │
    │              阶段五: Task 重提交      │                                      │
    │                                      │ async_retry_task_callback_ ───────────→ SubmitTask
    │                                      │                                      │
    │              阶段六: NodeB 执行       │                           ┌──────────┤
    │                                      │                           │ Create+Seal obj1
    │                                      │                           │ → HandleObjectAdded (NodeB)
    │                                      │                           │ → PinObjectIDs(owner)
    │                                      │                           │ Create+Seal obj2 → 同上
    │                                      │                           │ Create+Seal obj3 → 同上
    │                                      │                           │
    │              阶段七: 上报到 Owner     │ ←── ReportGeneratorItemReturns ──── │
    │                                      │                           │
    │ obj1: InsertToStream→false(已消费)   │                           │
    │   HandleTaskReturn→UpdatePinned→跳过 │                           │
    │   (OutOfScope=true, ELIGIBLE)        │                           │
    │ obj2: InsertToStream→true            │                           │
    │   HandleTaskReturn→UpdatePinned→NodeB│                           │
    │ obj3: 同 obj2                        │                           │
    │                                      │                           │
    │              阶段八: 订阅处理          │ ←── PubsubCommandBatch(obj1) ────── │
    │                                      │ ←── PubsubCommandBatch(obj2) ────── │
    │                                      │ ←── PubsubCommandBatch(obj3) ────── │
    │                                      │                           │
    │ obj1: AddOutOfScopeCallback→false    │                           │
    │   (OutOfScope && !ShouldDelete)       │                           │
    │   → 立即 unpin_object ──────────────────── LongPolling reply ──→ │
    │                                      │                           │ → ReleaseFreedObject(obj1)
    │                                      │                           │ → Plasma 删除 obj1 ★
    │ obj2: AddOutOfScopeCallback→true     │                           │
    │   → 注册回调（等 ref 归零）            │                           │
    │ obj3: 同 obj2                        │                           │
    │                                      │                           │
    │              阶段九: Recovery 完成     │                           │
    │ in_memory_store Put(obj2) → GetAsync │                           │
    │   → recovery_complete_callback       │                           │
    │                                      │                           │
    │              阶段十: 最终清理          │                           │
    │ 应用层 del obj2 → RemoveLocalRef      │                           │
    │   → OutOfScope → unpin_object ─────────── LongPolling reply ──→ │
    │   → ShouldDelete → EraseReference    │                           │ → ReleaseFreedObject(obj2)
    │   → RemoveLineageReference           │                           │ → Plasma 删除 obj2 ★
    │ obj3 同理                             │                           │
    │                                      │                           │
    │ obj1 lineage 最终释放:               │                           │
    │   lineage_ref_count-- → 0            │                           │
    │   → ShouldDelete → EraseReference    │                           │
    └──────────────────────────────────────┴────────────────────────── ┘
```

### 关键结论

| 对象 | NodeB 上是否创建 | Owner 是否记录位置 | 何时从 NodeB 清除 | 是否泄漏 |
|------|:---:|:---:|---|:---:|
| **obj1**（已消费） | 是（Create+Seal 不可逆） | 否（OutOfScope 跳过更新） | 订阅到达 owner 时 `AddObjectOutOfScopeOrFreedCallback` 分支2 返回 false → 立即 eviction | **否** |
| **obj2**（需要恢复） | 是 | 是（pinned_at=NodeB） | 应用层释放 ref → OutOfScope → eviction 消息 | **否** |
| **obj3**（需要恢复） | 是 | 是（pinned_at=NodeB） | 同 obj2 | **否** |

---

## 11. LineageReconstructionEligibility 详解

### 定义

`reference_counter_interface.h:31`:

```cpp
enum class LineageReconstructionEligibility {
  /// Eligible - lineage is available for reconstruction attempt.
  ELIGIBLE,
  /// Created by ray.put(), no task lineage to replay.
  INELIGIBLE_PUT,
  /// Task created with max_retries=0.
  INELIGIBLE_NO_RETRIES,
  /// Lineage evicted due to memory pressure.
  INELIGIBLE_LINEAGE_EVICTED,
  /// Lineage pinning is disabled system-wide, reconstruction not supported.
  INELIGIBLE_LINEAGE_DISABLED,
  /// Object reference not found in table.
  INELIGIBLE_REF_NOT_FOUND,
};
```

### 各类型的设置时机

| 类型 | 设置位置 | 含义 |
|------|---------|------|
| `ELIGIBLE` | `task_manager.cc:272` (max_retries > 0) | 可通过重执行 task 恢复 |
| `INELIGIBLE_PUT` | `core_worker.cc:1072` (ray.put) | ray.put() 创建，没有 task lineage |
| `INELIGIBLE_NO_RETRIES` | `task_manager.cc:270` (max_retries=0) | 任务不允许重试 |
| `INELIGIBLE_LINEAGE_EVICTED` | `reference_counter.cc:580` | lineage 因内存压力被 evict |
| `INELIGIBLE_LINEAGE_DISABLED` | `reference_counter.cc:1694` | 系统禁用了 lineage pinning |
| `INELIGIBLE_REF_NOT_FOUND` | `reference_counter.cc:1698` | ref table 中找不到 |

### Streaming Generator 的 eligibility

Generator task 的 return_id（generator_id）的 eligibility 取决于 `max_retries`：

```cpp
// task_manager.cc:270
if (max_retries == 0) {
  lineage_eligibility = LineageReconstructionEligibility::INELIGIBLE_NO_RETRIES;
} else {
  lineage_eligibility = LineageReconstructionEligibility::ELIGIBLE;  // ★ 通常情况
}
```

Streaming generator 的动态 return objects 通过 `OwnDynamicStreamingTaskReturnRef` **继承** generator 的 `lineage_eligibility_`：

```cpp
// reference_counter.cc:297
AddOwnedObjectInternal(object_id, ...,
    outer_it->second.lineage_eligibility_,  // ★ 继承 generator 的 eligibility
    /*add_local_ref=*/true, ...);
```

### 为什么非 ELIGIBLE 要看 lineage_ref_count

| eligibility | 对象丢失后能否恢复 | lineage_ref>0 时 Plasma 副本 | 原因 |
|---|---|---|---|
| **ELIGIBLE** | 可以（重执行 task） | **释放** | 丢了可以重建，不需要保留副本 |
| **非 ELIGIBLE** | 不可以 | **保留** | 丢了永久丢失，必须保留副本 |

在 `OutOfScope` 中：

```cpp
bool has_lineage_references = false;
if (lineage_pinning_enabled && owned_by_us_ &&
    lineage_eligibility_ != LineageReconstructionEligibility::ELIGIBLE) {
  // ★ 只有非 ELIGIBLE 时 lineage_ref_count 才影响 OutOfScope
  has_lineage_references = lineage_ref_count > 0;
}
// ELIGIBLE 时 has_lineage_references 永远是 false
```

---

## 12. object_id_refs_ 和 freed_objects_ 更新时机

### object_id_refs_ 的更新

**增加**：
- `AddOwnedObject` / `AddOwnedObjectInternal` → 对象首次创建时插入
- `OwnDynamicStreamingTaskReturnRef` → streaming generator return 首次上报时
- `AddBorrowedObject` → 借用对象时
- `AddLocalReference` → 如果不存在则 `emplace` 创建

**删除**（`EraseReference`）：

`DeleteReferenceInternal` (`reference_counter.cc:740`):

```cpp
void DeleteReferenceInternal(ReferenceTable::iterator it, ...) {
  if (it->second.OutOfScope(lineage_pinning_enabled_)) {
    OnObjectOutOfScopeOrFreed(it);  // 触发 eviction callbacks
  }
  if (it->second.ShouldDelete(lineage_pinning_enabled_)) {
    // ★ 只有 OutOfScope && lineage_ref_count==0 时才删除
    ReleaseLineageReferences(it);  // 释放 lineage → 递归减少依赖对象的 lineage_ref_count
    EraseReference(it);            // 从 object_id_refs_ 中删除
  }
}
```

`EraseReference` (`reference_counter.cc:790`):

```cpp
void EraseReference(ReferenceTable::iterator it) {
  object_info_publisher_->PublishFailure(...);
  RAY_CHECK(it->second.ShouldDelete(lineage_pinning_enabled_));
  freed_objects_.erase(it->first);  // ★ 同时从 freed_objects_ 中移除
  // ... 更新计数器 ...
  // ... 调用 object_ref_deleted_callbacks ...
  object_id_refs_.erase(it);  // ★ 从 ref table 中删除
}
```

**何时从 object_id_refs_ 中删除的完整条件**：
```
RefCount() == 0  (local_ref + submitted_task_ref + contained_in_owned 全部为0)
&&
OutOfScope == true  (无 borrowers, 无 nested borrowed, 无 lineage ref[仅非ELIGIBLE])
&&
ShouldDelete == true  (OutOfScope && lineage_ref_count == 0)
```

触发 `DeleteReferenceInternal` 的路径：
- `RemoveLocalReference` → `local_ref_count--` → `RefCount()==0`
- `RemoveSubmittedTaskReferences` → `submitted_task_ref_count--` → `RefCount()==0`
- `CleanupBorrowersOnRefRemoved` → borrower 回复后减少引用

### freed_objects_ 的更新

**增加**（`freed_objects_.insert`）：

`FreePlasmaObjects` (`reference_counter.cc:716`):

```cpp
void FreePlasmaObjects(const std::vector<ObjectID> &object_ids) {
  for (const ObjectID &object_id : object_ids) {
    auto it = object_id_refs_.find(object_id);
    if (it == object_id_refs_.end()) continue;

    freed_objects_.insert(object_id);  // ★ 标记为 freed
    // ★ 不删除 ref，保留 ownership 信息
    OnObjectOutOfScopeOrFreed(it);     // 立即触发 eviction callbacks
  }
}
```

触发 `FreePlasmaObjects` 的路径：
- `ray.internal.free(object_id)` → `DeleteImpl` → `FreePlasmaObjects`（应用层主动 free）
- `SealExisting(pin_object=false)` → `FreePlasmaObjects`（不 pin 的对象直接释放）

**移除**（`freed_objects_.erase`）：

1. `EraseReference` 中：对象完全删除时清理
```cpp
freed_objects_.erase(it->first);  // 从 freed_objects_ 中移除
```

2. `TryMarkFreedObjectInUseAgain`：recovery 时对象需要重新使用
```cpp
bool TryMarkFreedObjectInUseAgain(const ObjectID &object_id) {
  if (!object_id_refs_.contains(object_id)) return false;
  return freed_objects_.erase(object_id) != 0u;  // ★ 从 freed_objects_ 中移除
}
```

`TryMarkFreedObjectInUseAgain` 在 `UpdateReferencesForResubmit` 中被调用：

```cpp
// task_manager.cc:460
for (const auto &task_dep : *task_deps) {
  bool was_freed = reference_counter_.TryMarkFreedObjectInUseAgain(task_dep);
  if (was_freed) {
    // 之前被 free 的依赖对象，现在 recovery 需要重新使用
    in_memory_store_.Delete({task_dep});
  }
}
```

### 完整生命周期图

```
═══════════════════════════════════════════════════════════════════════
                    object_id_refs_ 中对象完整生命周期
═══════════════════════════════════════════════════════════════════════

【阶段1: 创建】
  AddOwnedObject / AddOwnedObjectInternal / OwnDynamicStreamingTaskReturnRef
  → object_id_refs_.insert(obj)
  → freed_objects_: 无
  → pinned_at_node_id_: 可能设置
  → local_ref_count > 0
  → lineage_ref_count: 0 (初始)
  → on_object_out_of_scope_or_freed_callbacks: 空

───────────────────────────────────────────────────────────────────────

【阶段2: 任务提交，依赖此对象】
  UpdateSubmittedTaskReferences
  → submitted_task_ref_count++
  → lineage_ref_count++  ★ lineage 计数增加
  → RefCount() > 0
  → OutOfScope = false
  → Plasma 副本保留

───────────────────────────────────────────────────────────────────────

【阶段3: 应用层使用中】
  RefCount() > 0 (local_ref > 0 或 submitted_task > 0 或 contained_in_owned > 0)
  → OutOfScope = false
  → ShouldDelete = false
  → Plasma 副本保留
  → object_id_refs_ 中有
  → freed_objects_: 无
  → callbacks: 可能已注册 (如 raylet 的 unpin_object 回调)

───────────────────────────────────────────────────────────────────────

【阶段4: 应用层释放 (del obj)】
  RemoveLocalReference → local_ref_count--
  → RefCount() == 0
  → DeleteReferenceInternal 被调用:

  ┌─ ELIGIBLE 对象 (如 streaming generator return, max_retries > 0):
  │
  │   OutOfScope:
  │     in_scope = false (RefCount==0)
  │     has_lineage_references = false (ELIGIBLE → 不看 lineage_ref_count)
  │     → OutOfScope = true ★
  │
  │   DeleteReferenceInternal:
  │     → OutOfScope=true → OnObjectOutOfScopeOrFreed
  │       ├── 调用 on_object_out_of_scope_or_freed_callbacks (如 unpin_object)
  │       │   → 发布 WORKER_OBJECT_EVICTION → raylet 释放 Plasma 副本 ★
  │       ├── ★ 清空 callbacks
  │       └── UnsetObjectPrimaryCopy → pinned_at_node_id_.reset()
  │
  │     → ShouldDelete?
  │       ├── lineage_ref_count == 0:
  │       │   → ShouldDelete = true
  │       │   → ReleaseLineageReferences
  │       │   │   → 递归减少依赖对象的 lineage_ref_count
  │       │   │   → 可能触发依赖对象的 OutOfScope → 递归 OnObjectOutOfScopeOrFreed
  │       │   │   → 可能触发依赖对象的 ShouldDelete → 递归 EraseReference
  │       │   → EraseReference
  │       │     ├── freed_objects_.erase(obj)  ★ 从 freed 集合中移除
  │       │     ├── 调用 object_ref_deleted_callbacks
  │       │     └── object_id_refs_.erase(obj) ★ 从 ref table 中删除
  │       │   ★★ 对象完全从 ref table 中删除 ★★
  │       │
  │       └── lineage_ref_count > 0:
  │           → ShouldDelete = false
  │           → ★ 不 EraseReference
  │           → 对象保留在 object_id_refs_ 中
  │           → 但: callbacks 已清空, pinned_at 已 reset, Plasma 副本已释放
  │           → 等待 lineage 释放
  │
  └─ 非 ELIGIBLE 对象 (如 ray.put 创建, INELIGIBLE_PUT):
  │
  │   OutOfScope:
  │     in_scope = false (RefCount==0)
  │     has_lineage_references = (lineage_ref_count > 0) (非 ELIGIBLE → 看 lineage)
  │     ├── lineage_ref_count > 0:
  │     │   → OutOfScope = false ★
  │     │   → DeleteReferenceInternal 中 OutOfScope=false → 不触发 OnObjectOutOfScopeOrFreed
  │     │   → ShouldDelete = false (OutOfScope=false)
  │     │   → ★ 不释放 Plasma 副本, 不清空 callbacks, 不 erase
  │     │   → 对象完整保留在 ref table 中
  │     │   → 等待 lineage_ref_count 归零
  │     │
  │     └── lineage_ref_count == 0:
  │         → OutOfScope = true
  │         → 同 ELIGIBLE 路径 → OnObjectOutOfScopeOrFreed → ShouldDelete=true → EraseReference

───────────────────────────────────────────────────────────────────────

【阶段5: 应用层主动 free (ray.internal.free)】
  FreePlasmaObjects:
  → freed_objects_.insert(obj)  ★ 标记为 freed
  → OnObjectOutOfScopeOrFreed
    → 调用 callbacks → 发布 eviction → raylet 释放 Plasma 副本
    → 清空 callbacks
    → UnsetObjectPrimaryCopy
  → ★ ref 保留在 object_id_refs_ 中 (保留 ownership 信息)
  → 后续 AddObjectOutOfScopeOrFreedCallback:
    → 分支3: freed_objects_.contains(obj) → return false → 立即 unpin_object

───────────────────────────────────────────────────────────────────────

【阶段6: Lineage 最终释放】
  任务不可再重试 → RemoveSubmittedTaskReferences(release_lineage=true)
  → submitted_task_ref_count--
  → lineage_ref_count--

  当 lineage_ref_count == 0:
  → OutOfScope = true (所有对象, 因为 has_lineage_references = false)
  → ShouldDelete = true (OutOfScope && lineage_ref_count == 0)
  → DeleteReferenceInternal:
    → OnObjectOutOfScopeOrFreed (如果之前没执行过)
    → EraseReference:
      ├── freed_objects_.erase(obj)  ★ 从 freed 集合中移除
      ├── 调用 object_ref_deleted_callbacks
      └── object_id_refs_.erase(obj) ★ 从 ref table 中删除
  ★★ 对象完全从 ref table 中删除 ★★

  对于 ELIGIBLE + lineage_ref > 0 的对象 (阶段4中保留的):
  → ReleaseLineageReferences 中:
    → 如果对象仍 in scope 且 ELIGIBLE → 标记为 INELIGIBLE_LINEAGE_EVICTED
    → 递归减少依赖对象的 lineage_ref_count
    → 可能触发依赖对象的 OutOfScope → 递归 OnObjectOutOfScopeOrFreed
    → 可能触发依赖对象的 ShouldDelete → 递归 EraseReference

───────────────────────────────────────────────────────────────────────

【阶段7: Recovery 重提交时对象被重新使用】
  UpdateReferencesForResubmit → TryMarkFreedObjectInUseAgain(obj):
  → freed_objects_.erase(obj)  ★ 从 freed 集合中移除
  → 对象重新可用，可被新 task 使用
  → in_memory_store_.Delete({obj}) → 清理 freed 标记
```

### 为什么不能 OutOfScope 就从 ref table 中删除 — 总结

**核心原因：`lineage_ref_count > 0` 时 ref 记录还需要保留**，原因有三：

1. **lineage 递归清理依赖 ref 记录**：`ReleaseLineageReferences` 需要通过 ref 记录找到依赖链中的上游对象，递归减少它们的 `lineage_ref_count`。如果提前删除了 ref 记录，递归清理无法进行，上游对象的 `lineage_ref_count` 永远无法归零，造成 ref table 内存泄漏。

2. **保留 ownership 信息**：ref table 中保存了 `owner_address_`、`lineage_eligibility_` 等信息。其他 worker 可能需要查询此对象的 owner。`FreePlasmaObjects` 的注释明确说："Free only the plasma value. We must keep the reference around so that we have the ownership information."

3. **保留 `object_ref_deleted_callbacks`**：某些组件注册了"ref 完全删除"的回调（如 generator stream 的清理），这些回调应该在对象完全不可恢复时才触发，而非 Plasma 副本释放时就触发。

**OutOfScope 的效果是"释放 Plasma 副本"，ShouldDelete 的效果是"从 ref table 中删除"**。两者分离设计使得系统可以在保留元数据的同时释放实际的 Plasma 内存。

---

## 关键文件索引

| 文件 | 关键函数 | 行号 |
|------|---------|------|
| `src/ray/object_manager/plasma/store.cc` | `SealObjects`, `add_object_callback_` | 275 |
| `src/ray/raylet/main.cc` | `add_object_callback` 注册 | 800 |
| `src/ray/object_manager/object_manager.cc` | `HandlePush`, `HandlePull`, `HandleObjectAdded` | 610, 801, 181 |
| `src/ray/object_manager/object_buffer_pool.cc` | `CreateChunk`, `WriteChunk` | 97, 130 |
| `src/ray/object_manager/plasma/client.cc` | `Seal`, `CreateAndSpillIfNeeded` | 569, 215 |
| `src/ray/core_worker/core_worker.cc` | `CreateOwnedAndIncrementLocalRef`, `SealExisting`, `SealReturnObject`, `ReportGeneratorItemReturns`, `ProcessSubscribeForObjectEviction`, `PutInLocalPlasmaStore` | 1046, 1194, 3220, 3365, 3765, 1000 |
| `src/ray/core_worker/task_manager.cc` | `ResubmitTask`, `HandleReportGeneratorItemReturns`, `HandleTaskReturn`, `CompletePendingTask`, `GetTaskReturnObjectsToStoreInPlasma`, `RemoveLineageReference`, `MarkGeneratorFailedAndResubmit`, `InsertToStream` | 353, 784, 555, 913, 1538, 1449, 475, 180 |
| `src/ray/core_worker/reference_counter.cc` | `UpdateObjectPinnedAtRaylet`, `AddObjectOutOfScopeOrFreedCallback`, `DeleteReferenceInternal`, `OnObjectOutOfScopeOrFreed`, `FreePlasmaObjects`, `OwnDynamicStreamingTaskReturnRef`, `AddOwnedObjectInternal`, `UpdateSubmittedTaskReferences`, `UpdateResubmittedTaskReferences`, `RemoveSubmittedTaskReferences`, `ReleaseLineageReferences`, `EraseReference`, `TryMarkFreedObjectInUseAgain` | 953, 873, 740, 839, 716, 273, 346, 497, 529, 609, 569, 790, 708 |
| `src/ray/core_worker/reference_counter.h` | `Reference::RefCount`, `OutOfScope`, `ShouldDelete` | 348, 359, 380 |
| `src/ray/core_worker/reference_counter_interface.h` | `LineageReconstructionEligibility` enum | 31 |
| `src/ray/core_worker/object_recovery_manager.cc` | `RecoverObject`, `PinOrReconstructObject`, `ReconstructObject` | 30, 96, 127 |
| `src/ray/core_worker/store_provider/plasma_store_provider.cc` | `Put`, `Create`, `Seal` | 100, 126, 175 |
| `src/ray/raylet/local_object_manager.cc` | `PinObjectsAndWaitForFree`, `ReleaseFreedObject`, `FlushFreeObjects` | 31, 111, 150 |
| `src/ray/raylet/node_manager.cc` | `HandlePinObjectIDs` | 2630 |
| `src/ray/pubsub/subscriber.cc` | `Subscribe`, `SendCommandBatchIfPossible`, `MakeLongPollingPubsubConnection` | 257, 389, 295 |
| `src/ray/core_worker/common.cc` | `SerializeReturnObject` | 57 |
