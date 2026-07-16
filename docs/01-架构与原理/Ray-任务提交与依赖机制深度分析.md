# Ray 任务提交与依赖机制详解

本文档详细分析 Ray 中任务提交流程、ObjectRef 生命周期、依赖解析与内联、跨进程 ObjectRef 传递、Worker 租赁、TaskSpec 构造等核心机制。

---

## 目录

1. [NormalTaskSubmitter::SubmitTask 流程](#1-normaltasksubmittersubmittask-流程)
2. [ObjectRef 生命周期](#2-objectref-生命周期)
3. [依赖解析与内联机制](#3-依赖解析与内联机制)
4. [tensor_transport 特殊处理](#4-tensor_transport-特殊处理)
5. [跨进程 ObjectRef 传递](#5-跨进程-objectref-传递)
6. [Worker 租赁机制](#6-worker-租赁机制)
7. [TaskSpec 构造流程](#7-taskspec-构造流程)

---

## 1. NormalTaskSubmitter::SubmitTask 流程

### 1.1 完整提交流程

**文件**: `src/ray/core_worker/task_submission/normal_task_submitter.cc:35-97`

```cpp
void NormalTaskSubmitter::SubmitTask(TaskSpecification task_spec) {
  RAY_CHECK(task_spec.IsNormalTask());
  RAY_LOG(DEBUG) << "Submit task " << task_spec.TaskId();

  resolver_.ResolveDependencies(task_spec, [this, task_spec](Status status) mutable {
    task_manager_.MarkDependenciesResolved(task_spec.TaskId());
    if (!status.ok()) {
      task_manager_.MarkTaskCanceled(task_spec.TaskId());
      task_manager_.MarkDependenciesCanceled(task_spec.TaskId());
      return;
    }

    task_spec.GetMutableMessage().set_dependency_resolution_timestamp_ms(
        current_sys_time_ms());

    const SchedulingKey scheduling_key(task_spec.GetSchedulingClass(),
                                       task_spec.GetDependencyIds(),
                                       task_spec.GetRuntimeEnvHash());
    auto &scheduling_key_entry = scheduling_key_entries_[scheduling_key];
    scheduling_key_entry.lease_spec = LeaseSpecification(task_spec.GetMessage());
    scheduling_key_entry.task_queue.push_back(std::move(task_spec));

    if (!scheduling_key_entry.AllWorkersBusy()) {
      for (const auto &active_worker_addr : scheduling_key_entry.active_workers) {
        auto iter = worker_to_lease_entry_.find(active_worker_addr);
        RAY_CHECK(iter != worker_to_lease_entry_.end());
        auto &lease_entry = iter->second;
        if (!lease_entry.is_busy) {
          OnWorkerIdle(active_worker_addr, scheduling_key,
                       /*was_error=*/false, /*error_detail=*/"",
                       /*worker_exiting=*/false, lease_entry.assigned_resources);
          break;
        }
      }
    }
    RequestNewWorkerIfNeeded(scheduling_key);
  });
}
```

### 1.2 流程步骤

```
SubmitTask(task_spec)
  │
  ├─ 1. resolver_.ResolveDependencies() 异步解析依赖
  │      ├─ 扫描 ArgByRef 参数 → local_dependency_ids
  │      ├─ GetAsync 等待所有依赖可用
  │      └─ InlineDependencies 改写 task_spec（小值内联）
  │
  ├─ 2. 计算SchedulingKey = (SchedulingClass, DependencyIds, RuntimeEnvHash)
  │
  ├─ 3. 任务入队 scheduling_key_entry.task_queue
  │
  ├─ 4. 检查 AllWorkersBusy()
  │      ├─ 有空闲 worker → 立即 OnWorkerIdle 派发任务
  │      └─ 全忙 → 跳过
  │
  └─ 5. RequestNewWorkerIfNeeded() 请求租赁新 worker
```

### 1.3 SchedulingKey 组成

**文件**: `src/ray/core_worker/task_submission/normal_task_submitter.h:57-58`

```cpp
using SchedulingKey = std::tuple<SchedulingClass, std::vector<ObjectID>, RuntimeEnvHash>;
```

| 组件 | 含义 |
|------|------|
| SchedulingClass | 资源形状+函数描述符的哈希 ID |
| DependencyIds | plasma 依赖的 ObjectID 列表（做数据局部性优化） |
| RuntimeEnvHash | 运行环境哈希（worker 必须匹配才能执行） |

### 1.4 AllWorkersBusy 判断

**文件**: `src/ray/core_worker/task_submission/normal_task_submitter.h:296-299`

```cpp
bool AllWorkersBusy() const {
  RAY_CHECK_LE(num_busy_workers, active_workers.size());
  return num_busy_workers == active_workers.size();
}
```

**作用域**: 运作在**同一个提交任务的 CoreWorker 上**。`active_workers` 是该 CoreWorker 从 raylet 租赁来的远端 worker 的 `rpc::Address` 集合，`num_busy_workers` 跟踪其中有任务在执行的 worker 数量。

### 1.5 OnWorkerIdle 调用时机

OnWorkerIdle 是**统一的 worker 空闲处理入口**，在两个时机被调用：

| 调用位置 | 时机 | 目的 |
|---------|------|------|
| `SubmitTask` 中 | 新任务入队时，发现有空闲 worker | 立即派发新任务给空闲 worker |
| `PushNormalTask` 回调中 | 远端 worker 执行完任务后 | 处理刚变空闲的 worker |

OnWorkerIdle 逻辑：
```
OnWorkerIdle(addr, scheduling_key, was_error, ...)
  │
  ├─ 有错误 / worker 退出 / 租赁超时 / 队列空?
  │   → ReturnWorkerLease()   // 归还 worker 给 raylet
  │
  └─ 队列中还有任务?
      → 取出下一个 task dispatch
      → lease_entry.is_busy = true
      → num_busy_workers++
      → PushNormalTask(...)    // 继续派发下一个任务
```

---

## 2. ObjectRef 生命周期

### 2.1 Reference 核心数据结构

**文件**: `src/ray/core_worker/reference_counter.h:445-640`

每个跟踪的 ObjectID 在 `object_id_refs_` 中有一个 `Reference` 结构体：

```cpp
struct Reference {
  size_t local_ref_count = 0;                // Python ObjectRef 句柄数
  size_t submitted_task_ref_count = 0;        // 依赖此对象的已提交任务数
  size_t lineage_ref_count = 0;              // 可能重试的任务依赖数
  bool owned_by_us_ = false;                  // 是否是本 worker 拥有
  rpc::Address owner_address_;               // 对象 owner 地址
  std::optional<NodeID> pinned_at_node_id_;  // 对象 pin 在哪个节点
  std::optional<std::string> tensor_transport_;  // 传输方式
  absl::flat_hash_set<rpc::Address> borrowers;    // 远端借用者集合
  bool publish_ref_removed = false;          // 是否需要在 ref 清除时通知 owner
  // ... nested info, borrow info, callbacks ...
};

size_t RefCount() const {
  return local_ref_count + submitted_task_ref_count +
         nested().contained_in_owned.size();
}
```

### 2.2 创建阶段

#### ray.put() 路径

**文件**: `src/ray/core_worker/core_worker.cc:1039-1116`

```cpp
Status CoreWorker::CreateOwnedAndIncrementLocalRef(...) {
  *object_id = ObjectID::FromIndex(worker_context_->GetCurrentInternalTaskId(),
                                   worker_context_->GetNextPutIndex());

  bool owned_by_us = real_owner_address.worker_id() == rpc_address_.worker_id();
  if (owned_by_us) {
    reference_counter_->AddOwnedObject(*object_id,
                                       contained_object_ids,
                                       rpc_address_,
                                       CurrentCallSite(),
                                       data_size + metadata->Size(),
                                       LineageReconstructionEligibility::INELIGIBLE_PUT,
                                       /*add_local_ref=*/true,
                                       NodeID::FromBinary(rpc_address_.node_id()),
                                       tensor_transport);
  } else {
    // Foreign owner case (ray.put with _owner=...)
    AddLocalReference(*object_id);
    reference_counter_->AddBorrowedObject(*object_id, ObjectID::Nil(),
                                          real_owner_address,
                                          /*foreign_owner_already_monitoring=*/true);
    // Send AssignObjectOwner RPC to the remote owner
  }

  status = plasma_store_provider_->Create(metadata, data_size, *object_id, ...);
}
```

**关键**:
- `add_local_ref=true` → `local_ref_count=1`
- `pinned_at_node_id` 已设置 → `pending_creation_=false`
- `owned_by_us=true` → 本 worker 是 owner

#### Task 返回值路径

**文件**: `src/ray/core_worker/task_manager.cc:273-321`

```cpp
std::vector<rpc::ObjectReference> TaskManager::AddPendingTask(...) {
  for (size_t i = 0; i < num_returns; i++) {
    auto return_id = spec.ReturnId(i);
    reference_counter_.AddOwnedObject(return_id,
                                      /*contained_ids=*/{},
                                      caller_address,     // ★ 谁提交，谁是 owner
                                      call_site,
                                      -1,
                                      lineage_eligibility,
                                      /*add_local_ref=*/true,
                                      /*pinned_at_node_id=*/std::optional<NodeID>(),
                                      tensor_transport);
  }
  reference_counter_.UpdateSubmittedTaskReferences(return_ids, task_deps);
}
```

**关键**:
- `add_local_ref=true` → `local_ref_count=1`
- `pinned_at_node_id=nullopt` → `pending_creation_=true`（值尚未生成）
- `caller_address` 是提交任务的 CoreWorker → **owner 是提交方**

#### 关于 Owner 的纠正

| 场景 | owner | 代码机制 |
|------|-------|---------|
| `ref = actor.create.remote()` | **提交方 Driver** | `AddPendingTask(caller_address=Driver)` |
| Actor 内部 `ray.put("data")` | **Actor 自己** | `CreateOwnedAndIncrementLocalRef(owner=Actor自己)` |
| `ray.put("data", _owner=actor)` | **Actor**（显式指定） | `AddBorrowedObject(owner=Actor) + AssignObjectOwner RPC` |

### 2.3 借用（Borrowing）阶段

当 ObjectRef 跨进程传递时（作为任务参数或嵌套在返回值中）：

```
Owner 提交任务时:
  → UpdateSubmittedTaskReferences(): argument_id 的 submitted_task_ref_count++ 和 lineage_ref_count++

任务在远端 Worker 执行完毕:
  → UpdateFinishedTaskReferences(): MergeRemoteBorrowers() 合并借用者信息
  → submitted_task_ref_count-- (如果借用者仍在使用，则借用者地址加入 borrowers 集合)
  → Owner 对每个新 borrower 调用 WaitForRefRemoved() (pub/sub 订阅)

Borrower 端:
  → AddBorrowedObject() 或 AddLocalReference(): owned_by_us=false
  → SubscribeRefRemoved(): 设置 publish_ref_removed=true
```

### 2.4 跨进程追踪协议（WaitForRefRemoved / PublishRefRemoved）

基于 **pub/sub** 机制：

#### Owner 端：WaitForRefRemoved

**文件**: `src/ray/core_worker/reference_counter.cc:926-973`

```cpp
void ReferenceCounter::WaitForRefRemoved(const ReferenceTable::iterator &ref_it,
                                         const rpc::Address &addr,
                                         const ObjectID &contained_in_id) {
  RAY_CHECK(ref_it->second.owned_by_us_);  // 只有 owner 才能发送

  auto *request = sub_message->mutable_worker_ref_removed_message();
  request->mutable_reference()->set_object_id(object_id.Binary());
  request->mutable_reference()->mutable_owner_address()->CopyFrom(*ref_it->second.owner_address_);
  request->set_contained_in_id(contained_in_id.Binary());
  request->set_intended_worker_id(addr.worker_id());

  const auto message_published_callback = [this, addr, object_id](const rpc::PubMessage &msg) {
    const ReferenceTable new_borrower_refs =
        ReferenceTableFromProto(msg.worker_ref_removed_message().borrowed_refs());
    CleanupBorrowersOnRefRemoved(new_borrower_refs, object_id, addr);
    object_info_subscriber_->Unsubscribe(...);
  };

  object_info_subscriber_->Subscribe(
      std::move(sub_message),
      rpc::ChannelType::WORKER_REF_REMOVED_CHANNEL,
      addr, object_id.Binary(), ...);
}
```

#### Borrower 端：SubscribeRefRemoved

**文件**: `src/ray/core_worker/reference_counter.cc:1391-1429`

```cpp
void ReferenceCounter::SubscribeRefRemoved(const ObjectID &object_id,
                                           const ObjectID &contained_in_id,
                                           const rpc::Address &owner_address) {
  auto it = object_id_refs_.find(object_id);
  if (it == object_id_refs_.end()) {
    it = object_id_refs_.emplace(object_id, Reference()).first;
  }

  if (reference.RefCount() == 0) {
    // 已经不再使用！立即回复
    PublishRefRemovedInternal(object_id);
    DeleteReferenceInternal(it, nullptr);
  } else {
    // 仍在使用，设标记以便 RefCount 归零时通知
    reference.publish_ref_removed = true;
  }
}
```

#### MergeRemoteBorrowers

**文件**: `src/ray/core_worker/reference_counter.cc:833-893`

```cpp
void ReferenceCounter::MergeRemoteBorrowers(const ObjectID &object_id,
                                            const rpc::Address &worker_addr,
                                            const ReferenceTable &borrowed_refs) {
  if (borrower_ref.RefCount() > 0) {
    auto inserted = it->second.mutable_borrow()->borrowers.insert(worker_addr).second;
    if (inserted) {
      new_borrowers.push_back(worker_addr);
    }
  }
  // 递归合并下游 borrower
  for (const auto &nested_borrower : borrower_ref.borrow().borrowers) {
    auto inserted = it->second.mutable_borrow()->borrowers.insert(nested_borrower).second;
    if (inserted) {
      new_borrowers.push_back(nested_borrower);
    }
  }
  // 对每个新 borrower 发送 WaitForRefRemoved
  if (it->second.owned_by_us_) {
    for (const auto &addr : new_borrowers) {
      WaitForRefRemoved(it, addr);
    }
  }
  // 递归处理嵌套 ref
  for (const auto &inner_id : borrower_ref.nested().contains) {
    MergeRemoteBorrowers(inner_id, worker_addr, borrowed_refs);
  }
}
```

### 2.5 嵌套 Ref 发现与追踪

**文件**: `src/ray/core_worker/reference_counter.cc:1073-1132`

```cpp
void ReferenceCounter::AddNestedObjectIdsInternal(
    const ObjectID &object_id,
    const std::vector<ObjectID> &inner_ids,
    const rpc::Address &owner_address) {
  if (owner_address.worker_id() == rpc_address_.worker_id()) {
    // 我们拥有外层对象
    for (const auto &inner_id : inner_ids) {
      it->second.mutable_nested()->contains.insert(inner_id);
      inner_it->second.mutable_nested()->contained_in_owned.insert(object_id);
      // inner_id 的 RefCount 被 contained_in_owned 贡献
    }
  } else {
    // 我们不拥有外层对象（task caller 在远端）
    for (const auto &inner_id : inner_ids) {
      if (inner_it->second.owned_by_us_) {
        // 我们拥有 inner_id → caller 成为 borrower
        inner_it->second.mutable_borrow()->borrowers.insert(owner_address);
        WaitForRefRemoved(inner_it, owner_address, object_id);
      } else {
        // 我们也不拥有 inner_id → 记录 stored_in_objects
        inner_it->second.mutable_borrow()->stored_in_objects.emplace(object_id, owner_address);
      }
    }
  }
}
```

### 2.6 销毁阶段

```
Python ObjectRef.__dealloc__()
    → RemoveLocalReference(): local_ref_count--
    → RefCount() == 0?
        │
        ├─ 借用对象 & publish_ref_removed=true:
        │   → PublishRefRemovedInternal() → 通过 WORKER_REF_REMOVED_CHANNEL 通知 Owner
        │
        ├─ 自有对象 & 无 borrowers:
        │   → DeleteReferenceInternal()
        │       → OutOfScope() 检查条件:
        │           - RefCount() == 0
        │           - 无 borrowers
        │           - 无 stored_in_objects
        │           - 无 contained_in_borrowed_ids
        │           - 无 lineage 引用
        │       → OnObjectOutOfScopeOrFreed(): 触发回调
        │           → plasma_store_provider_->Release(): 解除 plasma pin
        │           → UnsetObjectPrimaryCopy(): 清除 pinned_at_node_id
        │       → ShouldDelete()? → ReleaseLineageReferences() → EraseReference()
        │
        └─ 自有对象 & 有 borrowers:
            → 等待所有 borrower 通知
            → CleanupBorrowersOnRefRemoved(): 从 borrowers 集合移除
            → 全部 borrower 清完 → 继续上述删除流程
```

### 2.7 生命周期全景图

```
1. CREATION
   ray.put() / task return
       │
       ▼
   AddOwnedObject()  -- owned_by_us=true, local_ref_count=1
       │
       ▼
   Reference exists in object_id_refs_ with owner_address = self

2. BORROWING (ObjectRef 跨进程传递)
   Task arg / 反序列化嵌套 ref
       │
       ▼
   AddBorrowedObject() / AddLocalReference() -- owned_by_us=false
       │
       ▼
   Owner sends WaitForRefRemoved (pub/sub subscribe to WORKER_REF_REMOVED_CHANNEL)
   Borrower sets publish_ref_removed = true

3. IN SCOPE (ref count tracking)
   - local_ref_count: Python ObjectRef 句柄
   - submitted_task_ref_count: 待处理任务依赖
   - contained_in_owned: 嵌套在自有对象中
   - lineage_ref_count: 可能重试的任务
   - borrowers: 远端借用者集合

4. DESTRUCTION
   Python ObjectRef.__dealloc__()
       │
       ▼
   RemoveLocalReference() → local_ref_count--
       │
       ▼
   RefCount() == 0?
       ├─ Borrowed → PublishRefRemoved → Owner 收到通知
       ├─ Owned, no borrowers → DeleteReference → Release plasma → EraseReference
       └─ Owned, has borrowers → 等待所有 borrower RefRemoved
```

---

## 3. 依赖解析与内联机制

### 3.1 ResolveDependencies 完整流程

**文件**: `src/ray/core_worker/task_submission/dependency_resolver.cc:63-160`

```cpp
void LocalDependencyResolver::ResolveDependencies(
    TaskSpecification &task, std::function<void(Status)> on_dependencies_resolved) {
  absl::flat_hash_set<ObjectID> local_dependency_ids;
  absl::flat_hash_set<ActorID> actor_dependency_ids;

  for (size_t i = 0; i < task.NumArgs(); i++) {
    if (task.ArgByRef(i)) {
      local_dependency_ids.insert(task.ArgObjectId(i));
    }
    for (const auto &inlined_ref : task.ArgInlinedRefs(i)) {
      const auto object_id = ObjectID::FromBinary(inlined_ref.object_id());
      if (ObjectID::IsActorID(object_id)) {
        const auto actor_id = ObjectID::ToActorID(object_id);
        if (actor_creator_.IsActorInRegistering(actor_id)) {
          actor_dependency_ids.insert(actor_id);
        }
      }
    }
  }

  if (local_dependency_ids.empty() && actor_dependency_ids.empty()) {
    on_dependencies_resolved(Status::OK());  // 快速路径
    return;
  }

  // Phase 3: 注册 pending state
  auto inserted = pending_tasks_.emplace(
      task_id,
      std::make_unique<TaskState>(task, local_dependency_ids, actor_dependency_ids,
                                  std::move(on_dependencies_resolved)));

  // Phase 4: 异步获取对象依赖
  for (const auto &obj_id : local_dependency_ids) {
    in_memory_store_.GetAsync(obj_id, [this, task_id, obj_id](std::shared_ptr<RayObject> obj) {
      absl::MutexLock lock(&mu_);
      auto &state = it->second;
      state->local_dependencies[obj_id] = std::move(obj);
      if (--state->obj_dependencies_remaining == 0) {
        InlineDependencies(state->local_dependencies, state->task, ...);
        if (state->actor_dependencies_remaining == 0) {
          resolved_task_state = std::move(state);
          pending_tasks_.erase(it);
        }
      }
      // 锁外调用回调
      if (resolved_task_state) {
        resolved_task_state->on_dependencies_resolved_(resolved_task_state->status);
      }
    });
  }

  // Phase 5: 异步等待 actor 依赖
  for (const auto &actor_id : actor_dependency_ids) {
    actor_creator_.AsyncWaitForActorRegisterFinish(actor_id, [this, task_id](const Status &status) {
      if (--state->actor_dependencies_remaining == 0 &&
          state->obj_dependencies_remaining == 0) {
        resolved_task_state = std::move(state);
        pending_tasks_.erase(it);
      }
    });
  }
}
```

### 3.2 "Ready" 判断机制

**依赖被视为"ready"当 `GetAsync` 的回调被触发。** 两个倒计时器：

| 计数器 | 初始值 | 递减时机 | 归零动作 |
|--------|-------|---------|---------|
| `obj_dependencies_remaining` | by-ref 参数个数 | 每个 GetAsync 回调 | 调用 InlineDependencies |
| `actor_dependencies_remaining` | 正在注册的 actor 数 | 每个 AsyncWaitForActorRegisterFinish 回调 | 检查是否两个都归零 |

**最终 `on_dependencies_resolved` 回调仅在两个计数器都归零时触发。**

### 3.3 GetAsync 实现

**文件**: `src/ray/core_worker/store_provider/memory_store/memory_store.cc:138-151`

```cpp
void CoreWorkerMemoryStore::GetAsync(
    const ObjectID &object_id, std::function<void(std::shared_ptr<RayObject>)> callback) {
  absl::MutexLock lock(&mu_);
  auto iter = objects_.find(object_id);
  if (iter == objects_.end()) {
    // 对象不在 store → 注册回调等待
    object_async_get_requests_[object_id].push_back(std::move(callback));
    return;
  }
  // 对象已在 store → 异步投递到事件循环（非同步调用！）
  auto &object_ptr = iter->second;
  object_ptr->SetAccessed();
  io_context_.post(
      [callback = std::move(callback), object_ptr]() { callback(object_ptr); },
      "CoreWorkerMemoryStore.GetAsync.Callback");
}
```

**关键**: 即使对象已存在，也是异步 post 到事件循环，不会同步调用，避免死锁。

### 3.4 Put 触发已注册回调

**文件**: `src/ray/core_worker/store_provider/memory_store/memory_store.cc:172-227`

```cpp
void CoreWorkerMemoryStore::Put(const RayObject &object,
                                const ObjectID &object_id,
                                const bool has_reference) {
  std::vector<std::function<void(std::shared_ptr<RayObject>)>> async_callbacks;
  {
    absl::MutexLock lock(&mu_);
    if (objects_.find(object_id) != objects_.end()) {
      return;  // 已存在，no-op
    }
    // 取出所有等待此 ID 的异步回调
    auto async_callback_it = object_async_get_requests_.find(object_id);
    if (async_callback_it != object_async_get_requests_.end()) {
      async_callbacks = std::move(async_callback_it->second);
      object_async_get_requests_.erase(async_callback_it);
    }
    // 存入 store
    EmplaceObjectAndUpdateStats(object_id, object_entry);
  }
  // 锁外投递回调
  io_context_.post(
      [async_callbacks = std::move(async_callbacks), object_entry]() {
        for (const auto &cb : async_callbacks) { cb(object_entry); }
      }, "CoreWorkerMemoryStore.Put.get_async_callbacks");
}
```

### 3.5 什么会把对象放入 in-memory store

| 来源 | Put 的内容 | 场景 |
|------|-----------|------|
| `TaskManager::HandleTaskReturn` | 实际值 | 小任务返回值直接返回（in_plasma=false） |
| `TaskManager::HandleTaskReturn` | `OBJECT_IN_PLASMA` 占位符 | 大任务返回值入 plasma（in_plasma=true） |
| `CoreWorker::GetAndPinArgsForExecutor` | `OBJECT_IN_PLASMA` 占位符 | by-ref 参数在 plasma 中 |
| `CoreWorker::SealExisting` | `OBJECT_IN_PLASMA` 占位符 | 对象 seal 后入 plasma |
| `FutureResolver::ProcessResolvedObject` | 实际值 | 从远端 owner 获取内联值 |

### 3.6 IsInPlasmaError 判断

**文件**: `src/ray/common/ray_object.cc:136-144`

```cpp
bool RayObject::IsInPlasmaError() const {
  if (metadata_ == nullptr) { return false; }
  const std::string_view metadata(reinterpret_cast<const char *>(metadata_->Data()),
                                  metadata_->Size());
  return metadata == kObjectInPlasmaStr;  // 等于 OBJECT_IN_PLASMA 枚举的字符串
}
```

当 `RayObject(rpc::ErrorType::OBJECT_IN_PLASMA)` 被构造时，metadata 中包含该错误类型的数字字符串。`IsInPlasmaError()` 检查 metadata 是否等于此字符串。

### 3.7 InlineDependencies 完整实现

**文件**: `src/ray/core_worker/task_submission/dependency_resolver.cc:27-62`

```cpp
void InlineDependencies(
    const absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> &dependencies,
    TaskSpecification &task,
    std::vector<ObjectID> *inlined_dependency_ids,
    std::vector<ObjectID> *contained_ids,
    const TensorTransportGetter &tensor_transport_getter) {
  auto &msg = task.GetMutableMessage();
  for (size_t i = 0; i < task.NumArgs(); i++) {
    if (task.ArgByRef(i)) {
      const auto &id = task.ArgObjectId(i);
      const auto &it = dependencies.find(id);
      if (it != dependencies.end()) {
        auto *mutable_arg = msg.mutable_args(i);
        if (!it->second->IsInPlasmaError()) {
          // 值在本地内存，未入 plasma → 内联
          if (auto tensor_transport = tensor_transport_getter(id)) {
            // GPU/NCCL: 保留 object_ref，设置 tensor_transport
            mutable_arg->set_tensor_transport(std::move(*tensor_transport));
          } else {
            // 默认 OBJECT_STORE: 清除 object_ref
            mutable_arg->clear_object_ref();
            inlined_dependency_ids->push_back(id);
          }
          mutable_arg->set_is_inlined(true);  // ★ 唯一设置 is_inlined=true 的地方
          mutable_arg->set_data(it->second->GetData()->Data(), it->second->GetData()->Size());
          mutable_arg->set_metadata(it->second->GetMetadata()->Data(), it->second->GetMetadata()->Size());
          for (const auto &nested_ref : it->second->GetNestedRefs()) {
            mutable_arg->add_nested_inlined_refs()->CopyFrom(nested_ref);
          }
        } else {
          // 值在 plasma 中 → 保留 ObjectRef
          auto tensor_transport = mutable_arg->object_ref().tensor_transport();
          mutable_arg->set_tensor_transport(tensor_transport);
        }
      }
    }
  }
}
```

### 3.8 is_inlined 的完整状态矩阵

`is_inlined` 仅在 `InlineDependencies` 中被设为 `true`，初始构造时永远为 `false`。

| `has_object_ref` | `is_inlined` | 含义 | 何时出现 |
|---|---|---|---|
| true | false | **常规 by-ref**，需要从 store 获取 | 初始构造 TaskArgByReference |
| true | true | **GPU/NCCL 内联**，值已内联但保留 ref 以便 receiver 用 ID 做 key 查找 GPU tensor | InlineDependencies + 有 tensor_transport |
| false | true | **完全内联**，值已内联且 ref 已清除 | InlineDependencies + 无 tensor_transport |
| false | false | **纯值参数**，从来不是 ObjectRef | 初始构造 TaskArgByValue |

`ArgByRef()` 判断逻辑（`task_spec.cc:265`）：
```cpp
return has_object_ref() && !is_inlined();
```

### 3.9 内联示例：从 Task 返回值到新 Task 参数

```python
@ray.remote
def foo():
    return "small_data"    # 小于 100KB，会被内联

@ray.remote
def bar(x):
    return x

ref = foo.remote()
result = bar.remote(ref)
```

完整流程：
```
foo 执行端 Worker:
  │
  ├─ AllocateReturnObject: data_size < 100KB → LocalMemoryBuffer
  ├─ SerializeReturnObject: in_plasma=false, data="small_data"
  └── PushTaskReply ──→ Driver 收到

Driver (Owner):
  │
  ├─ HandleTaskReturn: in_plasma=false
  │   → in_memory_store_.Put(RayObject("hello"), ref)  // ★ 真实数据
  │
  └─ bar.remote(ref):
      │
      ├─ ResolveDependencies:
      │   ├─ ArgByRef(0)=true → GetAsync(ref)
      │   ├─ 在 in_memory_store_ 找到真实数据
      │   └─ InlineDependencies:
      │       ├─ !IsInPlasmaError() → 小值路径
      │       ├─ clear_object_ref()
      │       ├─ set_is_inlined(true)
      │       └─ set_data("small_data")  // ★ 值写入 task spec
      │
      └─ 发往 Worker B 的 task spec:
          args[0] = { data: "small_data", is_inlined: true }
          // Worker B 直接从 task spec 读值，无需成为 borrower
```

### 3.10 无竞态条件

`GetAsync` 和 `Put` 都在 `mu_` 保护下操作：
- Put 先 → 对象在 `objects_`，GetAsync 直接 post 回调
- GetAsync 先 → 回调在 `object_async_get_requests_`，Put 时触发
- 对象已存在时 Put 是 no-op

---

## 4. tensor_transport 特殊处理

### 4.1 Protobuf 中的位置

**文件**: `src/ray/protobuf/common.proto`

| 位置 | 字段 | 含义 |
|------|------|------|
| `ObjectReference.tensor_transport` (line 725) | 创建 ObjectRef 时设置 | 单个引用的传输方式 |
| `TaskArg.tensor_transport` (line 768) | 依赖解析时设置 | 任务参数级别 |
| `TaskSpec.tensor_transport` (line 622) | 构建任务时设置 | 整个任务的返回值传输方式 |

### 4.2 有效传输类型

**文件**: `python/ray/experimental/rdt/util.py:93-94`

```python
DEFAULT_TRANSPORTS = ["NIXL", "GLOO", "NCCL", "CUDA_IPC"]
```

### 4.3 TensorTransportGetter 回调

**文件**: `src/ray/core_worker/task_submission/dependency_resolver.h:37-38`

```cpp
using TensorTransportGetter =
    std::function<std::optional<std::string>(const ObjectID &object_id)>;
```

- **ActorTaskSubmitter**（`core_worker_process.cc:525-528`）: 查询 `reference_counter_->GetTensorTransport(object_id)`
- **NormalTaskSubmitter**（`core_worker_process.cc:570-573`）: **始终返回 `std::nullopt`**（RDT 当前仅支持 actor task）

### 4.4 InlineDependencies 中的三种路径

```
对每个 ArgByRef(i) == true 且依赖已找到的参数:
  │
  ├─ !IsInPlasmaError() && 有 tensor_transport (NCCL等):
  │   → set_tensor_transport(...)
  │   → 保留 object_ref (has_object_ref=true)
  │   → set_is_inlined(true)
  │   → 不加入 inlined_dependency_ids（不减引用计数）
  │   原因: 接收方需用 object_id 从 actor 内部 GPU store 取数据
  │
  ├─ !IsInPlasmaError() && 无 tensor_transport (默认 OBJECT_STORE):
  │   → clear_object_ref() (has_object_ref=false)
  │   → set_is_inlined(true)
  │   → 加入 inlined_dependency_ids（减引用计数）
  │   原因: 数据已内联，可直接执行
  │
  └─ IsInPlasmaError() (值在 plasma 中):
      → 保留 object_ref
      → 从原 object_ref 复制 tensor_transport 到 TaskArg 级
      → 不设置 is_inlined
      原因: 数据在 plasma，执行方需从 plasma 获取
```

### 4.5 tensor_transport 注册到引用计数器

**文件**: `src/ray/core_worker/reference_counter.cc:373-376`

```cpp
auto it = object_id_refs_
              .emplace(object_id,
                       Reference(owner_address, call_site, object_size,
                                 lineage_eligibility, pinned_at_node_id,
                                 tensor_transport))  // ★ 存入 Reference
              .first;
```

查询方法（`reference_counter.cc:1829-1837`）：
```cpp
std::optional<std::string> ReferenceCounter::GetTensorTransport(
    const ObjectID &object_id) const {
  absl::MutexLock lock(&mutex_);
  auto it = object_id_refs_.find(object_id);
  if (it == object_id_refs_.end()) { return std::nullopt; }
  return it->second.tensor_transport_;
}
```

---

## 5. 跨进程 ObjectRef 传递

### 5.1 小值 vs 大值在执行方 Worker 的区别

| 场景 | task spec 中 arg | Worker B 的 ArgByRef | Worker B 角色 | in_memory_store_ | 数据来源 |
|------|-----------------|---------------------|--------------|-----------------|---------|
| 小值内联 | `{data, is_inlined=true}` | **false** | **非 borrower** | 无 | 直接从 task spec 读 |
| 大值 by-ref | `{object_ref, is_inlined=false}` | **true** | **borrower** | OBJECT_IN_PLASMA 占位符 | 从 plasma 阻塞获取 |

### 5.2 执行方 GetAndPinArgsForExecutor

**文件**: `src/ray/core_worker/core_worker.cc:3247-3347`

```cpp
Status CoreWorker::GetAndPinArgsForExecutor(
    const TaskSpecification &task, ...) {
  for (size_t i = 0; i < task.NumArgs(); ++i) {
    if (task.ArgByRef(i)) {
      // ★ 大值 by-ref 路径 ★
      const auto arg_id = ObjectID::FromBinary(arg_ref.object_id());
      reference_counter_->AddLocalReference(arg_id, task.CallSiteString());
      reference_counter_->AddBorrowedObject(arg_id, ObjectID::Nil(),
                                             task.ArgRef(i).owner_address());
      borrowed_ids->push_back(arg_id);
      memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA), arg_id, ...);
      // 阻塞等待从 plasma 获取
      plasma_store_provider_->Get(object_ids, owner_addresses, -1, &result_map);
    } else {
      // ★ 小值内联路径 ★
      auto data = std::make_shared<LocalMemoryBuffer>(
          task.ArgData(i), task.ArgDataSize(i));
      auto metadata = std::make_shared<LocalMemoryBuffer>(
          task.ArgMetadata(i), task.ArgMetadataSize(i));
      args->push_back(std::make_shared<RayObject>(data, metadata, ...));
      // 不调用 AddBorrowedObject，不写入 in_memory_store_
    }
  }
}
```

**关键**: `ArgByRef()=false` 时，Worker B 完全不知道它曾是个 ObjectRef，直接从 task spec 读值，不成为 borrower。

### 5.3 Borrower 形成：PushTaskReply.borrowed_refs

**执行方任务完成后**（`core_worker.cc:2935-2955`）：

```cpp
if (!borrowed_ids.empty()) {
  reference_counter_->PopAndClearLocalBorrowers(borrowed_ids, borrowed_refs, &deleted);
}
```

`PopAndClearLocalBorrowers` 将借用状态序列化到 `borrowed_refs` proto，然后扣除人工 local ref。

**Owner 收到回复后**（`reference_counter.cc:543`）：

```cpp
void ReferenceCounter::UpdateFinishedTaskReferences(...) {
  // 先合并 borrower 信息（在减引用计数之前）
  for (const ObjectID &argument_id : argument_ids) {
    MergeRemoteBorrowers(argument_id, worker_addr, refs);
  }
  // 然后释放 submitted task ref
  RemoveSubmittedTaskReferences(argument_ids, release_lineage, deleted);
}
```

### 5.4 完整跨进程传递示例

```python
@ray.remote
def create_data():
    return "hello"

@ray.remote
def process_data(data_ref):
    return ray.get(data_ref)

ref = create_data.remote()
result_ref = process_data.remote(ref)
```

#### 大值路径完整时序：

```
Driver (Owner)                          Worker B (Borrower)
─────────────                           ──────────────────
AddPendingTask: submitted_task_ref_count++
│
├── PushTask(task_spec with ObjectRef) ──→
│                                        GetAndPinArgsForExecutor:
│                                          AddLocalReference(ref_id)
│                                          AddBorrowedObject(ref_id, Driver_addr)
│                                          ★ Worker B 成为 borrower ★
│                                          Put(OBJECT_IN_PLASMA, ref_id)
│                                          plasma_store_provider_->Get({ref_id}, -1)
│                                            → 阻塞等待从 plasma 获取
│
│                                        执行 process_data:
│                                          ray.get(data_ref) → 从 plasma 读取
│
│                                        PopAndClearLocalBorrowers:
│                                          序列化借用状态 → borrowed_refs
│                                          local_ref_count-- (移除人工 pin)
│
├── PushTaskReply(borrowed_refs) ←───────
│
UpdateFinishedTaskReferences:
  MergeRemoteBorrowers(ref_id, WorkerB_addr, borrowed_refs):
    如果 Worker B 仍持有 ref → 加入 borrowers
    → WaitForRefRemoved → 订阅 Worker B 的 ref_removed channel
  RemoveSubmittedTaskReferences:
    submitted_task_ref_count--
    lineage_ref_count--
│
│                                    Worker B: ObjectRef.__dealloc__
│                                      → RemoveLocalReference
│                                      → RefCount()==0
│                                      → publish_ref_removed==true
│                                      → PublishRefRemovedInternal
│                                        ★ 通知 Owner ★
│
CleanupBorrowersOnRefRemoved:
  从 borrowers 移除 Worker B
  如果 RefCount()==0 → 释放 plasma 对象
```

### 5.5 嵌套 Ref 场景（返回值中包含 ObjectRef）

```python
@ray.remote
def pass_through(data_ref):
    return data_ref  # 返回 ObjectRef 本身
```

序列化时 `object_ref_reducer` 捕获 `data_ref` 作为 `contained_object_ref`，传给 `AllocateReturnObject` → `AddNestedObjectIds`。

反序列化时 `_object_ref_deserializer` 调用 `RegisterOwnershipInfoAndResolveFuture`：
- `AddBorrowedObject(data_ref, outer=result_ref, owner_addr)` → 创建 `contained_in_borrowed_ids` 关系
- `future_resolver_->ResolveFutureAsync(data_ref, owner_addr)` → 向 Owner 查询 GetObjectStatus

### 5.6 小值 ref 在 Borrower 端的 ray.get

当 ref 通过非任务参数途径（如嵌套在返回值中反序列化）到达 borrower 时：

```
FutureResolver 向 Owner 查询 GetObjectStatus:
  ├─ Owner 的 in_memory_store_ 中有真实数据 "hello"
  ├─ PopulateObjectStatus: 小值 → 把 data 内联到 reply 中
  └─ 回复: { status=CREATED, data="hello", metadata=... }

Borrower 的 ProcessResolvedObject:
  ├─ 收到真实数据
  ├─ in_memory_store_.Put(RayObject("hello"), ref)  // ★ 真实数据，非占位符 ★
  └─ ray.get(ref) → memory_store_->Get → 直接返回 "hello"
```

### 5.7 Borrower 持有 ref 时 in_memory_store_ 内容总结

| Worker B 如何得到 ref | 值大小 | in_memory_store_ 内容 | ray.get() 路径 |
|---|---|---|---|
| 任务参数（内联小值） | 小 | **无**（直接从 task spec 读值） | 不适用，收到的是值不是 ref |
| 任务参数（by-ref 大值） | 大 | OBJECT_IN_PLASMA 占位符 | plasma |
| 嵌套在返回值中反序列化 | 小 | **真实数据**（通过 GetObjectStatus 从 owner 获取） | 内存直接返回 |
| 嵌套在返回值中反序列化 | 大 | OBJECT_IN_PLASMA 占位符 | plasma |
| 自己提交任务获得 | 小 | **真实数据**（HandleTaskReturn Put） | 内存直接返回 |
| 自己提交任务获得 | 大 | OBJECT_IN_PLASMA 占位符 | plasma |

### 5.8 Borrower 提交使用 ref

**任何持有 ref 的 worker 都可以提交使用它**，不限于 owner：

```python
@ray.remote
def middle(data_ref):
    # Worker B 是 data_ref 的 borrower
    # in_memory_store_ 中只有 OBJECT_IN_PLASMA 占位符
    result = bar.remote(data_ref)  # ★ Worker B 也能提交使用 ref ★
    return result
```

此时 Worker B 的依赖解析：
```
GetAsync(data_ref) → 返回 RayObject(OBJECT_IN_PLASMA)  // 只有占位符
InlineDependencies:
  IsInPlasmaError() == true → 不内联，保留 ObjectRef
  → 执行方 Worker C 也成为 borrower
  → 借用链: Owner → Worker B → Worker C
```

---

## 6. Worker 租赁机制

### 6.1 租赁触发条件

**文件**: `src/ray/core_worker/task_submission/normal_task_submitter.cc:275-516`

`RequestNewWorkerIfNeeded` 中四个条件**全部满足**才触发：

| 条件 | 含义 |
|------|------|
| `pending_lease_requests.size() < max_pending` | 速率限制允许 |
| `AllWorkersBusy() == true` | 所有已租 worker 都在忙 |
| `task_queue` 非空 | 有任务要执行 |
| `task_queue.size() > pending_lease_requests.size()` | 不是每个任务都已有待处理租赁 |

### 6.2 租赁请求全流程

```
Submitter                                    Raylet
   │                                            │
   ├── RequestWorkerLease(lease_spec, backlog) ──→│
   │                                            │ 1. PrestartWorkers(backlog)
   │                                            │ 2. ClusterLeaseManager::QueueAndScheduleLease()
   │                                            │    → GetBestSchedulableNode()
   │                                            │
   │<── reply: worker_address (本地有资源) ───────│    本地节点: LocalLeaseManager
   │                                            │      → WaitForLeaseArgsRequests
   │                                            │      → GrantScheduledLeasesToWorkers
   │                                            │      → worker_pool_.PopWorker() → Grant()
   │                                            │
   │<── reply: retry_at_raylet_address ────────│    远程节点: spillback
   │                                            │
   │<── reply: canceled=true ──────────────────│    不可行: infeasible_leases_
   │                                            │
   │<── (无回复，排队等待) ───────────────────────│    排队等资源释放
```

### 6.3 Spillback 二级调度

```
1. Submitter → 本地 Raylet (grant_or_reject=false)
2. 本地 Raylet 发现远程节点更优 → 回复 retry_at_raylet_address=远程Raylet
3. Submitter → 远程 Raylet (grant_or_reject=true)
4a. 远程 Raylet 有资源 → 回复 worker_address (成功)
4b. 远程 Raylet 无资源 → 回复 rejected=true (不再重定向)
5. Submitter 收到 rejected → 回到原始 Raylet 重试
```

### 6.4 LeaseSpec Protobuf

**文件**: `src/ray/protobuf/common.proto:478-500`

```protobuf
message LeaseSpec {
  bytes lease_id = 1;
  bytes job_id = 2;
  Address caller_address = 3;
  TaskType type = 4;
  map<string, double> required_resources = 9;
  map<string, double> required_placement_resources = 10;
  SchedulingStrategy scheduling_strategy = 11;
  LabelSelector label_selector = 12;
  int64 depth = 13;
  RuntimeEnvInfo runtime_env_info = 14;
  repeated ObjectReference dependencies = 15;
  FunctionDescriptor function_descriptor = 18;
  FallbackStrategy fallback_strategy = 23;
}
```

### 6.5 active_workers 生命周期

**增加**: `AddWorkerLeaseClient`（`normal_task_submitter.cc:98-112`）

```cpp
void NormalTaskSubmitter::AddWorkerLeaseClient(
    const rpc::Address &worker_address,
    const rpc::Address &raylet_address,
    ..., const SchedulingKey &scheduling_key, const LeaseID &lease_id) {
  core_worker_client_pool_->GetOrConnect(worker_address);
  int64_t expiration = current_time_ms() + lease_timeout_ms_;
  worker_to_lease_entry_.emplace(worker_address,
      LeaseEntry{raylet_address, expiration, assigned_resources, scheduling_key, lease_id});
  auto &scheduling_key_entry = scheduling_key_entries_[scheduling_key];
  RAY_CHECK(scheduling_key_entry.active_workers.emplace(worker_address).second);  // ★ 加入
}
```

调用链：
```
RequestNewWorkerIfNeeded
  → RequestWorkerLease RPC → Raylet 回复 worker_address
  → AddWorkerLeaseClient(worker_address, raylet_address, ...)
  → OnWorkerIdle(worker_address, ...)  // 立即派发任务
```

**移除**: `ReturnWorkerLease`（`normal_task_submitter.cc:115-133`）

```cpp
void NormalTaskSubmitter::ReturnWorkerLease(const rpc::Address &addr, ...) {
  RAY_CHECK(!lease_entry.is_busy);
  scheduling_key_entry.active_workers.erase(addr);  // ★ 移除
  auto raylet_client = raylet_client_pool_->GetOrConnectByAddress(lease_entry.addr);
  raylet_client->ReturnWorkerLease(addr.port(), lease_entry.lease_id, was_error, ...);
  worker_to_lease_entry_.erase(addr);
}
```

**移除触发条件**（在 `OnWorkerIdle` 中检查）：

| 条件 | 说明 |
|------|------|
| `was_error == true` | 上一个任务执行出错 |
| `worker_exiting == true` | worker 正在退出 |
| `current_time_ms() > lease_expiration_time` | 租赁超时 |
| `task_queue.empty()` | 无更多任务可派发 |

### 6.6 任务完成后的调度循环

```
PushTask RPC 回调 (远端 worker 任务完成)
    → is_busy = false, num_busy_workers--
    → OnWorkerIdle()
        ├─ task_queue 非空 → 派发下一个任务到同一 worker
        ├─ task_queue 空 / 超时 / 出错 → ReturnWorkerLease()
        └─ CancelWorkerLeaseIfNeeded() (队列空时取消 pending lease)
        → RequestNewWorkerIfNeeded() (检查是否需要更多 worker)
```

### 6.7 Backlog 上报

```cpp
int64_t BacklogSize() const {
  if (task_queue.size() < pending_lease_requests.size()) { return 0; }
  return task_queue.size() - pending_lease_requests.size();
}
```

- **即时上报**: `RequestNewWorkerIfNeeded()` 末尾
- **定期上报**: `CoreWorker::PeriodicalHook()` 调用 `ReportWorkerBacklog()`
- 上报给本地 Raylet，Raylet 据此 `PrestartWorkers(backlog_size)` 预热 worker

### 6.8 Lease 超时配置

**文件**: `src/ray/common/ray_config_def.h`

```cpp
RAY_CONFIG(int64_t, worker_lease_timeout_milliseconds, ...)
```

过期时间在 `AddWorkerLeaseClient` 中计算：
```cpp
int64_t expiration = current_time_ms() + lease_timeout_ms_;
```

检查在 `OnWorkerIdle` 中：
```cpp
if (current_time_ms() > lease_entry.lease_expiration_time) {
  ReturnWorkerLease(addr, ...);
}
```

---

## 7. TaskSpec 构造流程

### 7.1 Python 层参数准备

**文件**: `python/ray/_raylet.pyx:766-895`

`prepare_args_internal` 三个分支：

```
遍历每个参数 arg:
│
├─ isinstance(arg, ObjectRef)?  ─── YES ──→
│   │
│   │  c_arg = arg.native()           // ObjectID
│   │  owner_addr = GetOwnerAddress(c_arg)  // 从 reference_counter_ 查 owner
│   │  tensor_transport = arg.c_tensor_transport()
│   │
│   └─→ new CTaskArgByReference(object_id, owner_addr, call_site, tensor_transport)
│
├─ else + size <= 100KB + 累计 <= 10MB?  ─── YES ──→
│   │
│   │  serialize(arg) → data, metadata
│   │  contained_refs = 序列化中发现的内嵌 ObjectRef
│   │
│   └─→ new CTaskArgByValue(CRayObject(data, metadata, inlined_refs))
│
└─ else + 太大 ──→
    │
    │  serialize(arg) → put_serialized_object_and_increment_local_ref()
    │  → 数据写入 plasma，返回 put_id
    │  → owner = 当前 CoreWorker
    │
    └─→ new CTaskArgByReference(put_id, self_rpc_address, call_site, tensor_transport)
```

### 7.2 C++ TaskArg 类

**文件**: `src/ray/common/task/task_util.h`

#### TaskArgByReference (lines 42-70)

```cpp
class TaskArgByReference : public TaskArg {
  void ToProto(rpc::TaskArg *arg_proto) const override {
    auto ref = arg_proto->mutable_object_ref();
    ref->set_object_id(id_.Binary());
    ref->mutable_owner_address()->CopyFrom(owner_address_);
    ref->set_call_site(call_site_);
    if (tensor_transport_.has_value()) {
      ref->set_tensor_transport(*tensor_transport_);
    }
    // 不设置 data/metadata/is_inlined
  }
};
```

#### TaskArgByValue (lines 72-97)

```cpp
class TaskArgByValue : public TaskArg {
  void ToProto(rpc::TaskArg *arg_proto) const {
    if (value_->HasData()) {
      arg_proto->set_data(value_->GetData()->Data(), value_->GetData()->Size());
    }
    if (value_->HasMetadata()) {
      arg_proto->set_metadata(value_->GetMetadata()->Data(), value_->GetMetadata()->Size());
    }
    for (const auto &nested_ref : value_->GetNestedRefs()) {
      arg_proto->add_nested_inlined_refs()->CopyFrom(nested_ref);
    }
    // 不设置 object_ref/is_inlined
  }
};
```

### 7.3 Protobuf 定义

**文件**: `src/ray/protobuf/common.proto`

```protobuf
message TaskArg {
  ObjectReference object_ref = 1;            // by-ref 时设置
  bytes data = 2;                             // by-value 时设置
  bytes metadata = 3;                         // by-value 时设置
  repeated ObjectReference nested_inlined_refs = 4;  // 值中内嵌的 ref
  bool is_inlined = 5;                        // ★ 初始构造时永远为 false ★
  optional string tensor_transport = 6;       // 依赖解析时可能设置
}

message ObjectReference {
  bytes object_id = 1;
  Address owner_address = 2;
  string call_site = 3;
  optional string tensor_transport = 4;
}

message TaskSpec {
  TaskType type = 1;
  string name = 2;
  FunctionDescriptor function_descriptor = 4;
  bytes task_id = 6;
  Address caller_address = 10;
  repeated TaskArg args = 11;                 // ★ 参数列表 ★
  uint64 num_returns = 12;
  // ... 其他字段
}
```

### 7.4 TaskSpecBuilder 组装

**文件**: `src/ray/common/task/task_util.h:99-330`

```cpp
class TaskSpecBuilder {
  TaskSpecBuilder() : message_(std::make_shared<rpc::TaskSpec>()) {}

  TaskSpecBuilder &SetCommonTaskSpec(...) {
    message_->set_type(TaskType::NORMAL_TASK);
    message_->set_task_id(task_id.Binary());
    message_->set_caller_address(...);
    // ... 所有公共字段
    return *this;
  }

  TaskSpecBuilder &AddArg(const TaskArg &arg) {
    auto ref = message_->add_args();  // 追加 TaskArg
    arg.ToProto(ref);                 // 填充该 TaskArg
    return *this;
  }

  TaskSpecBuilder &SetNormalTaskSpec(...) {
    message_->set_max_retries(max_retries);
    message_->mutable_scheduling_strategy()->CopyFrom(scheduling_strategy);
    return *this;
  }

  TaskSpecification ConsumeAndBuild() && {
    return TaskSpecification(std::move(message_));
  }
};
```

### 7.5 CoreWorker::SubmitTask 入口

**文件**: `src/ray/core_worker/core_worker.cc:1958-2013`

```cpp
std::vector<rpc::ObjectReference> CoreWorker::SubmitTask(
    const RayFunction &function,
    const std::vector<std::unique_ptr<TaskArg>> &args,
    const TaskOptions &task_options, ...) {
  TaskSpecBuilder builder;
  BuildCommonTaskSpec(builder, ...);
  for (const auto &arg : args) {
    builder.AddArg(*arg);  // ★ 每个 arg.ToProto() 填充 TaskArg ★
  }
  builder.SetNormalTaskSpec(...);
  TaskSpecification task_spec = std::move(builder).ConsumeAndBuild();

  returned_refs = task_manager_->AddPendingTask(...);
  io_service_.post(
      [this, task_spec = std::move(task_spec)]() mutable {
        normal_task_submitter_->SubmitTask(std::move(task_spec));
      }, "CoreWorker.SubmitTask");
  return returned_refs;
}
```

### 7.6 初始构造后的 args 状态

| 参数类型 | object_ref | data/metadata | nested_inlined_refs | is_inlined |
|---------|-----------|---------------|--------------------|----|
| ObjectRef 参数 | {object_id, owner_addr, call_site, tensor_transport} | 空 | 空 | **false** |
| 小值参数 | 空 | 序列化字节 | 内嵌的 ObjectRef | **false** |
| 大值 put 后 by-ref | {put_id, caller_addr, call_site} | 空 | 空 | **false** |

### 7.7 message_ 与 ref 的关系

```
Python ObjectRef "ref"
    │
    ├─ native() → ObjectID (128 bit 唯一标识)
    ├─ GetOwnerAddress() → owner 的 rpc::Address
    └─ c_tensor_transport() → optional<string>
         │
         ▼
CTaskArgByReference(ObjectID, owner_addr, call_site, tensor_transport)
         │
         ▼ ToProto()
TaskArg { object_ref: {object_id, owner_addr, call_site, tensor_transport},
          is_inlined: false (默认) }
         │
         ▼ AddArg()
message_->args[i]  (shared_ptr<rpc::TaskSpec> 内部)
         │
         ▼ ResolveDependencies → InlineDependencies
         │
         ├── 小值: clear_object_ref, set_data, set_is_inlined(true)
         └── 大值: 保留 object_ref, is_inlined 保持 false
```

### 7.8 大小阈值配置

**文件**: `src/ray/common/ray_config_def.h`

| 配置 | 默认值 | 含义 |
|------|-------|------|
| `max_direct_call_object_size` | 100 KB (`100 * 1024`) | 单个返回值内联上限 |
| `task_rpc_inlined_bytes_limit` | 10 MB (`10 * 1024 * 1024`) | 任务返回值累计内联上限 |

### 7.9 任务返回值的大小判断

**文件**: `src/ray/core_worker/core_worker.cc:2708-2751`

```cpp
Status CoreWorker::AllocateReturnObject(...) {
  if (static_cast<int64_t>(data_size) < max_direct_call_object_size_ &&
      (*task_output_inlined_bytes + static_cast<int64_t>(data_size) <=
       RayConfig::instance().task_rpc_inlined_bytes_limit())) {
    // 内联: LocalMemoryBuffer
    data_buffer = std::make_shared<LocalMemoryBuffer>(data_size);
    *task_output_inlined_bytes += data_size;
  } else {
    // Plasma: PlasmaBuffer
    RAY_RETURN_NOT_OK(CreateExisting(metadata, data_size, object_id, ...));
  }
}
```

序列化（`common.cc:57-89`）：
```cpp
void SerializeReturnObject(...) {
  if (return_object->GetData()->IsPlasmaBuffer()) {
    return_object_proto->set_in_plasma(true);  // data/metadata 为空
  } else {
    return_object_proto->set_data(return_object->GetData()->Data(), ...);
    return_object_proto->set_metadata(return_object->GetMetadata()->Data(), ...);
    // in_plasma 保持 false
  }
}
```

---

## 关键代码文件索引

| 文件 | 关键内容 |
|------|---------|
| `src/ray/core_worker/task_submission/dependency_resolver.cc` | ResolveDependencies, InlineDependencies |
| `src/ray/core_worker/store_provider/memory_store/memory_store.cc` | GetAsync, Put |
| `src/ray/core_worker/reference_counter.cc/h` | AddOwnedObject, AddBorrowedObject, RemoveLocalReference, DeleteReferenceInternal, MergeRemoteBorrowers, WaitForRefRemoved, PublishRefRemoved |
| `src/ray/core_worker/task_manager.cc` | AddPendingTask, HandleTaskReturn, CompletePendingTask |
| `src/ray/core_worker/core_worker.cc` | SubmitTask, GetAndPinArgsForExecutor, AllocateReturnObject, CreateOwnedAndIncrementLocalRef |
| `src/ray/core_worker/task_submission/normal_task_submitter.cc/h` | SubmitTask, OnWorkerIdle, RequestNewWorkerIfNeeded, PushNormalTask, AddWorkerLeaseClient, ReturnWorkerLease |
| `src/ray/common/task/task_util.h` | TaskArgByReference, TaskArgByValue, TaskSpecBuilder |
| `src/ray/common/task/task_spec.cc/h` | TaskSpecification, ArgByRef, ArgObjectId, ArgData |
| `python/ray/_raylet.pyx` | prepare_args_internal, submit_task |
| `src/ray/protobuf/common.proto` | TaskArg, ObjectReference, TaskSpec, ReturnObject |
| `src/ray/common/ray_config_def.h` | max_direct_call_object_size, task_rpc_inlined_bytes_limit |
| `src/ray/core_worker/task_execution/task_receiver.cc` | PushTaskReply, borrowed_refs |
| `src/ray/raylet/node_manager.cc` | HandleRequestWorkerLease |
| `src/ray/raylet/scheduling/cluster_lease_manager.cc` | ScheduleAndGrantLeases, spillback |
| `src/ray/raylet/scheduling/local_lease_manager.cc` | Grant, WaitForLeaseArgsRequests |
