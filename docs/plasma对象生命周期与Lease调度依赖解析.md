# Plasma 对象生命周期与 Lease 调度依赖解析

## 概述

本文档详细梳理 Ray 集群中 Plasma 对象从创建到删除的完整生命周期代码链路，以及 Lease（任务调度单元）的依赖解析、调度授予、执行清理的完整流程。涵盖以下核心主题：

1. Plasma 对象三层引用机制与 Pin/Unpin 流程
2. Raylet 主动 Spill 与 Plasma LRU 被动淘汰的区别
3. Primary 和 Replicated 对象的 Spill/Evict/Delete 差异
4. Replication Push 与 Pull 的数据到达路径与冲突处理
5. Lease 依赖解析的两层机制（Submitter 侧 + Raylet 侧）
6. ScheduleAndGrantLeases 的四重门控
7. SchedulingClass 与 Lease/Task 的关系
8. Worker 执行完成后的 Lease 清理链路

---

## 一、Plasma 对象三层引用机制

### 概念

Plasma 对象的"是否可被淘汰"由三层机制串联控制，三者通过析构函数副作用单向串联：

| 层级 | 机制 | 控制什么 |
|------|------|----------|
| A | `shared_ptr<RayObject>` 引用计数 | 控制 `RayObject`（持有 `PlasmaBuffer`）何时析构 |
| B | `~PlasmaBuffer()` 析构函数副作用 | 析构时自动调用 `client_->Release(object_id_)` |
| C | Plasma Store 的 `ref_count_` | 控制对象是否可被 LRU 淘汰 |

**单向串联**：A → B → C。A 释放导致 B 触发，B 触发导致 C 减 1。不存在反向影响。

### 代码链路

```
shared_ptr<RayObject> 最后一个引用释放
  → ~RayObject()
    → ~PlasmaBuffer()                              // client.cc:49
      → client_->Release(object_id_)               // 自动调用
        → PlasmaClient::Release                     // client.cc:490
          → count--
          → MarkObjectUnused
          → SendReleaseRequest [IPC]
            → Plasma Server: RemoveFromClientObjectIds  // store.cc:247
              → RemoveReference                       // obj_lifecycle_mgr.cc:148
                → ref_count_--
                → if ref_count_ == 0:
                    EndObjectAccess                  // eviction_policy.cc:142
                      → 加入 LRU 队列（可被淘汰）
```

### 关键点

- `PlasmaBuffer` 析构函数自动调用 `Release`，raylet / worker 不需要显式调用 Release
- 对象 `ref_count_ > 0` 时不会被 LRU 淘汰
- `ref_count_ == 0` 后进入 LRU 队列，等待被 `RequireSpace()` 或 `EvictObjects()` 回收

---

## 二、Raylet 主动 Spill 与 Plasma LRU 被动淘汰

### 两种回收路径

| 路径 | 触发方 | 时机 | 效果 |
|------|--------|------|------|
| **主动 Spill** | Raylet | `SpillObjectUptoMaxThroughput()`，内存压力超过阈值时 | 写磁盘 → 释放 plasma 引用 → 后续 LRU 回收内存 |
| **被动 LRU Evict** | Plasma Store | `RequireSpace()`，创建新对象空间不足时 | 直接释放内存 |

两者不冲突，是先后关系：Raylet 先主动 Spill 减引用，Plasma 后续 LRU 回收内存。

### 主动 Spill 的入口

```
SpillIfOverPrimaryObjectsThreshold()            // node_manager.cc:2407
  │ allocated_percentage = GetPrimaryBytes() / GetMemoryCapacity()
  │ if allocated_percentage >= object_spilling_threshold:
  └─ local_object_manager_.SpillObjectUptoMaxThroughput()  // local_object_manager.cc:169
       │
       │ ★ Replicated 对象优先 Spill（先选）
       │ 1. replicated_object_manager_->GetSpillableObjects(max_fused_object_count_)
       │    → 从 ReplicatedObjectManager::replicated_objects_ 选出可 spill 的
       │    → move RayObject → objects_pending_spill_ + pending_spill_is_replicated_
       │
       │ ★ Primary 对象其次（后选）
       │ 2. 遍历 pinned_objects_，调用 is_plasma_object_spillable_(object_id)
       │    → move RayObject → objects_pending_spill_
       │
       └─ SpillObjectsInternal(objects_to_spill)
            → IO Worker 序列化写磁盘
            → OnObjectSpilled 回调
```

### Get 期间不能 Spill/Evict

Worker 持有 `RayObject` → `PlasmaBuffer` → store `ref_count_ ≥ 1` → 不会 LRU 淘汰。

Raylet 只对自己 `pinned_objects_` 中的对象主动 Spill。PullManager pin 的对象在 `PullManager::pinned_objects_` 中，不在 `LocalObjectManager::pinned_objects_` 中，因此 Raylet 不会主动 Spill Pull 的对象。

---

## 三、Primary 和 Replicated 对象的 Spill/Evict/Delete 差异

### OnObjectSpilled — 记录 Spill 指标

```cpp
// local_object_manager.cc:469
void LocalObjectManager::OnObjectSpilled(...) {
  spilled_bytes_total_ += object_info.data_size;  // 混合累加（需拆分）
  // is_replicated 变量已可用，可区分 Primary vs Replicated
  if (pending_spill_is_replicated_.count(object_id)) {
    replicated_spilled_bytes_total_ += object_info.data_size;
  } else {
    primary_spilled_bytes_total_ += object_info.data_size;
  }
}
```

### OnObjectRestored — 记录 Restore 指标

```cpp
// local_object_manager.cc:548
void LocalObjectManager::OnObjectRestored(...) {
  restored_bytes_total_ += object_size;  // 同理需拆分
}
```

### HandleObjectMissing — Evict 路径的分叉

```cpp
// node_manager.cc:2506
void NodeManager::HandleObjectMissing(const ObjectID &object_id) {
  // Primary vs Replicated 分叉
  if (local_object_manager_.IsPrimary(object_id)) {
    // Primary 被删除 → needs_recovery
  } else {
    // Replicated 副本被删除 → secondary_copy_lost
  }
}
```

### 删除链路

Primary 删除时其所有副本也被删除（连锁删除），Replicated delete_bytes >> Primary delete_bytes 是正常的，因为 replication 只复制大对象。

---

## 四、Replication Push 与 Pull 的数据到达路径

### Pull 路径

```
Worker A: ray.get(object_id) → 本地没有
  → PullManager::Pull()                        // pull_manager.cc:48
    │ 创建 ObjectPullRequest + BundlePullRequest
    └─ UpdatePullsBasedOnAvailableMemory → ActivateNextBundlePullRequest
         │
         ├─ ① TryPinObject(obj_id)             // pull_manager.cc:602
         │    └─ pin_object_(object_id)
         │         └─ NodeManager::GetObjectsFromPlasma  // node_manager.cc:2646
         │              └─ store_client_->Get()           // Raylet PlasmaClient
         │                   → AddReference (server ref +1)
         │              ← unique_ptr<RayObject>(PlasmaBuffer)
         │         → pinned_objects_[id] = std::move(ref)  // PullManager 持有
         │
         └─ ② objects_to_pull.push_back(obj_id)
              → 后续 TryToMakeObjectLocal(obj_id)      // pull_manager.cc:446
                → PullFromRandomLocation(obj_id)        // pull_manager.cc:511
                  → send_pull_request_(obj_id, node_id) // 发 PullRequest 到远端

远端响应 → ReceivePullChunk → CreateChunk(source=ReceivedByPull) → Seal
  → add_object_callback_ → HandleObjectAdded
    → source == ReceivedByPull → 不走 ReplicatedObjectManager
    → PullManager::PinNewObjectIfNeeded(object_id)   // pull_manager.cc:586
      │ bool active = active_object_pull_requests_.count(object_id) > 0
      │ if active:
      └─ TryPinObject(object_id)   // 复用同一函数
    → HandleObjectLocal → LeaseDependencyManager::HandleObjectLocal
```

### Replication Push 路径

```
源节点 → PushForReplication → ReceiveReplicationPushChunk
  → CreateChunk(source=ReceivedByPush) → Seal
  → add_object_callback_ → HandleObjectAdded
    → source == ReceivedByPush
    → ReplicatedObjectManager::PinReplicatedObject
      → replicated_objects_[id] = RayObject (持有 PlasmaBuffer 引用)
      → ref_count ≥ 1 → 可 Spill
```

### TryPinObject vs PinNewObjectIfNeeded

| | `TryPinObject` | `PinNewObjectIfNeeded` |
|---|---|---|
| **前置条件** | 无，直接调 `pin_object_()` | 先检查 `active_object_pull_requests_` 中是否存在 |
| **调用场景** | `ActivateNextBundlePullRequest` 中 activate 时 | `HandleObjectAdded` 回调中，对象刚被 Seal 到 plasma 时 |
| **核心逻辑** | 相同：`pin_object_()` → `pinned_objects_[id] = ref` | 调用 `TryPinObject`，完全复用 |
| **重复 pin 保护** | `pinned_objects_.count(id) > 0` → 直接 return true | 同（由 TryPinObject 内部保证） |

**`PinNewObjectIfNeeded` 的 `active` 检查意义**：`HandleObjectAdded` 对所有新到对象都触发，但只有正在 pull 的对象才需要 PullManager pin。如果是 replication push 创建的对象，`active_object_pull_requests_` 中没有该 object_id，直接跳过。

### Replication 和 Pull 冲突

```cpp
// object_manager.cc:739
if (has_active_pull || object_already_local) {
    std::string reason = object_already_local ? "object_already_exists" : "already_pulling";
    object_replication_skipped_received_.Record(1, {{"Reason", reason}});
    replication_push_objects_.erase(object_id);
    return false;
}
```

| 场景 | 行为 | 原因 |
|------|------|------|
| Pull 正在进行 + Replication push 到 | push 被拒绝，reason=`already_pulling` | Pull 已在创建 plasma buffer |
| 对象已在本地 + Replication push 到 | push 被拒绝，reason=`object_already_exists` | 对象已存在，无需重复 |
| Push 先到 + Pull 请求来 | pull 发现对象已存在，直接完成 | Plasma 已有该对象 |

**冲突时 Pull 获胜。** 如果 Pull 先到 → push 被跳过 → 该对象不被视为 Replicated。如果 Push 先到 → pull 直接完成 → 该对象被 ReplicatedObjectManager 管理。

---

## 五、PullManager UnpinObject 的触发场景

`UnpinObject` 只在 `DeactivateBundlePullRequest`（`pull_manager.cc:201`）中调用：

| 调用链 | 场景 |
|--------|------|
| `CancelPull → DeactivateBundlePullRequest → UnpinObject` | Lease 完成/取消 → `RemoveLeaseDependencies` → `object_manager_.CancelPull` |
| `DeactivateUntilMarginAvailable → DeactivateBundlePullRequest → UnpinObject` | 内存不足时，停掉低优先级 pull bundle，腾出配额给高优先级 |
| Worker 断连 → `CancelGetRequest` → `CancelPull` → ... | Worker 死亡/断连时清理 |

### PullManager pin 和 PinLeaseArgs pin 是同一个 PlasmaClient

**是的。** `pin_object_` 回调和 `get_lease_arguments_` 回调都调用 `NodeManager::GetObjectsFromPlasma`，使用 `NodeManager::store_client_`（`node_manager.cc:208`）——Raylet 启动时创建的**唯一一个** PlasmaClient。

```cpp
// main.cc:860 — pin_object_ 回调
[&](const ObjectID &object_id) {
  return node_manager->GetObjectsFromPlasma({object_id}, &results);
}

// main.cc:1079 — get_lease_arguments_ 回调
[&](const vector<ObjectID> &object_ids, vector<unique_ptr<RayObject>> *results) {
  return node_manager->GetObjectsFromPlasma(object_ids, results);
}
```

PullManager pin 一次 + PinLeaseArgs pin 一次 = 同一 PlasmaClient 两次 Get → `ref_count_` 累加为 2。需要两次 Release 才归零。

### Pull 的对象不能被 Raylet 主动 Spill

Pull pin 的对象在 `PullManager::pinned_objects_` 中，不在 `LocalObjectManager::pinned_objects_` 中。Pull unpin 后 `ref_count_=0`，只能等 Plasma LRU 被动淘汰。

Replicated push 先到的对象在 `ReplicatedObjectManager::replicated_objects_` 中，Raylet 主动 Spill 时**优先**选它（`GetSpillableObjects` 先于 `pinned_objects_` 遍历）。

---

## 六、Lease 依赖解析的两层机制

### 第一层：Submitter 侧 — LocalDependencyResolver

**判断"对象在 Submitter 本地 MemoryStore 是否可用"，目的是内联小对象。**

```
NormalTaskSubmitter::SubmitTask(task_spec)       // normal_task_submitter.cc:39
  → resolver_.ResolveDependencies(task_spec, callback)  // dependency_resolver.cc:97
    │
    │ 对每个 ArgByRef 的 obj_id:
    │   in_memory_store_.GetAsync(obj_id, callback)      // memory_store.cc:142
    │     │
    │     ├─ 对象已在 MemoryStore → 立即回调
    │     │    → 不是 IsInPlasmaError:
    │     │       → 内联: set_data/set_metadata 直接写进 task message
    │     │       → clear_object_ref (不需要 plasma 传输)
    │     │    → 是 IsInPlasmaError:
    │     │       → 保留 object_ref (需要到 raylet 侧拿)
    │     │
    │     └─ 对象不在 MemoryStore → 注册到等待队列
    │          object_async_get_requests_[object_id].push_back(callback)
    │          ★ 不回调，等上游产出
    │
    │ 所有 local_dependencies 到齐 → on_dependencies_resolved(Status::OK())
    └─ callback → SubmitTask RPC → 发到 raylet
```

**GetAsync 的等待机制**：

```cpp
// memory_store.cc:142
void CoreWorkerMemoryStore::GetAsync(object_id, callback) {
  auto iter = objects_.find(object_id);
  if (iter == objects_.end()) {
    object_async_get_requests_[object_id].push_back(callback);
    return;  // 等待，不回调
  }
  callback(iter->second);  // 立即回调
}

// memory_store.cc:172 — 上游写回结果时触发
void CoreWorkerMemoryStore::Put(object, object_id) {
  objects_[object_id] = object_entry;
  auto callbacks = std::move(object_async_get_requests_[object_id]);
  for (auto &cb : callbacks) {
    cb(object_entry);  // 触发等待的 callback
  }
}
```

**Submitter 侧不判断 ref 是否产出**，`GetAsync` 只是查本地 MemoryStore。如果对象还没产出（本地和远端都没有），callback 就一直挂着，直到上游 task 完成写回结果 → `MemoryStore::Put` 触发 callback。

### 第二层：Raylet 侧 — LeaseDependencyManager

**判断"对象在 Raylet 本地 Plasma Store 是否可用"，目的是拉取大对象到本地。**

只有 Submitter 侧未内联的 object_ref（大对象）才会到达 Raylet 侧。

---

## 七、Lease 生命周期完整流程

### 阶段一：Lease 提交 → 等待依赖

```
LocalLeaseManager::WaitForLeaseArgsRequests(work)           // local_lease_manager.cc:96
  │
  ├─ 有依赖：lease_dependency_manager_.RequestLeaseDependencies(lease_id, deps, task_key)
  │           // lease_dependency_manager.cc:237
  │    │
  │    │  1. queued_lease_requests_[lease_id] = LeaseDependencies(deps)
  │    │  2. 对每个 obj_id in deps:
  │    │       if local_objects_.contains(obj_id):
  │    │         DecrementMissingDependencies()  // 已在本地
  │    │       else:
  │    │         required_objects_[obj_id].dependent_leases.insert(lease_id)
  │    │  3. Pull 请求：
  │    │     lease_entry->pull_request_id_ = object_manager_.Pull(deps, TASK_ARGS)
  │    │  4. return num_missing_dependencies_ == 0  // ★ 就绪判断
  │    │
  │    ├─ 返回 true（所有依赖已在本地）：
  │    │    → leases_to_grant_[scheduling_key].push_back(work)
  │    │
  │    └─ 返回 false（有缺失依赖）：
  │         → waiting_lease_queue_.push_back(work)
  │         → MaybeMarkFootprintAsBusy(PULLING_TASK_ARGUMENTS)
  │
  └─ 无依赖：直接进 leases_to_grant_
```

### "依赖就绪"的判断标准

**`LeaseDependencyManager` 维护 `num_missing_dependencies_` 计数器，减到 0 就解阻塞。** 判断标准是"对象在本地 Plasma Store 中是否存在"（`local_objects_` 集合），不区分对象来源。

### 阶段二：依赖到达 → Lease 解阻塞

```
Plasma 对象 Seal 完成（Pull/Replication Push/Worker 创建 均可）
  → add_object_callback_                    // main.cc:801 绑定
    → ObjectManager::HandleObjectAdded
    → NodeManager::HandleObjectLocal(object_info)      // node_manager.cc:2421
      │
      ├─ 1. lease_dependency_manager_.HandleObjectLocal(object_id)  // lease_dependency_manager.cc:310
      │     │  a. local_objects_.insert(object_id)     // 标记为"本地可用"
      │     │  b. 查找 required_objects_[obj_id].dependent_leases
      │     │  c. 对每个依赖此对象的 lease:
      │     │       lease_entry->DecrementMissingDependencies()
      │     │       if num_missing_dependencies_ == 0:     // ★ 判断条件
      │     │         ready_lease_ids.push_back(lease_id)
      │     │  d. 返回 ready_lease_ids
      │     │
      │     └─ 返回 ready_lease_ids
      │
      ├─ 2. local_lease_manager_.LeasesUnblocked(ready_lease_ids)  // local_lease_manager.cc:733
      │     │  对每个 ready lease_id:
      │     │    从 waiting_lease_queue_ 移到 leases_to_grant_[scheduling_key]
      │     │
      │     └─ ScheduleAndGrantLeases()  // 触发调度
      │
      └─ 3. 通知等待此对象的 Worker: PlasmaObjectReady RPC
```

### 对象被 Evict → Lease 重新阻塞

```
Plasma 对象被 LRU 淘汰 / Spill 后删除
  → delete_object_callback_              // main.cc:826 绑定
    → ObjectManager::HandleObjectDeleted
    → NodeManager::HandleObjectMissing(object_id)  // node_manager.cc:2461
      └─ lease_dependency_manager_.HandleObjectMissing(object_id)  // lease_dependency_manager.cc:283
           │  1. local_objects_.erase(object_id)
           │  2. 对每个依赖此对象的 lease:
           │       lease_entry->IncrementMissingDependencies()  // +1
           │       if 原来是 0（已就绪）:
           │         waiting_lease_ids.push_back(lease_id)  // 重新阻塞
           └─ 返回 waiting_lease_ids
```

### `num_missing_dependencies_` 计数器变化总结

| 事件 | 操作 | 代码位置 |
|------|------|----------|
| `RequestLeaseDependencies` | 初始值 = 依赖总数 - 已在本地数 | `lease_dependency_manager.cc:249-251` |
| `HandleObjectLocal` | `DecrementMissingDependencies()` → -1 | `lease_dependency_manager.cc:325` |
| `HandleObjectMissing` | `IncrementMissingDependencies()` → +1 | `lease_dependency_manager.cc:291` |

---

## 八、ScheduleAndGrantLeases — 四重门控

### 调用时机（事件驱动，非周期性）

| 触发场景 | 代码位置 |
|----------|----------|
| 新 Lease 提交 | `WaitForLeaseArgsRequests` → 末尾 (`local_lease_manager.cc:96`) |
| 依赖到达 → Lease 解阻塞 | `LeasesUnblocked(ready_ids)` → 末尾 (`local_lease_manager.cc:755`) |
| ClusterLeaseManager 分配 Lease 到本节点 | (`cluster_lease_manager.cc:67`) |
| Worker 返回/Lease 完成 | `HandleWorkerAvailable` → `cluster_lease_manager_.ScheduleAndGrantLeases()` (`node_manager.cc:1377`) |

### 遍历范围

**遍历所有 `leases_to_grant_` 中的 lease，不只是触发时参数对应的那个。** `leases_to_grant_` 是 `unordered_map<SchedulingClass, deque<Work>>`，包含所有依赖已就绪、等待授予的 lease。

```cpp
// local_lease_manager.cc:131
void LocalLeaseManager::GrantScheduledLeasesToWorkers() {
  for (auto shapes_it = leases_to_grant_.begin(); 
       shapes_it != leases_to_grant_.end();) {
    auto &leases_to_grant_queue = shapes_it->second;  // ★ 局部变量引用 = 当前 class 的队列
    for (auto work_it = leases_to_grant_queue.begin(); ...) {
      // 逐个检查四重门控
    }
  }
}
```

`leases_to_grant_` 和 `leases_to_grant_queue` 不是两个数据结构，是同一棵树的根和叶。`leases_to_grant_` 是整个 map，`leases_to_grant_queue` 是遍历时对当前 SchedulingClass 的 deque 引用。

### 四重门控详解

```
ScheduleAndGrantLeases()                                  // local_lease_manager.cc:126
  │
  ├─ GrantScheduledLeasesToWorkers()                     // local_lease_manager.cc:131
  │   │
  │   │  遍历 leases_to_grant_ 队列，对每个 work：
  │   │
  │   │  ┌─ 门控1：SchedulingClass 容量上限 ─────────────────────┐
  │   │  │  sched_cls_cap_enabled_ && granted >= capacity?       │
  │   │  │    → TrySpillback（转给其他节点）                      │
  │   │  │    或 break（等冷却时间）                              │
  │   │  └──────────────────────────────────────────────────────┘
  │   │                    ↓ 通过
  │   │  ┌─ 门控2：PinLeaseArgsIfMemoryAvailable ────────────────┐
  │   │  │  get_lease_arguments_(deps, &args)                    │
  │   │  │    → NodeManager::GetObjectsFromPlasma                │
  │   │  │    → store_client_->Get() → plasma AddReference      │
  │   │  │  ← vector<unique_ptr<RayObject>>                     │
  │   │  │                                                       │
  │   │  │  检查每个 arg != nullptr:                             │
  │   │  │    → 有 nullptr → args_missing=true                   │
  │   │  │       → 退回 waiting_lease_queue_（依赖被 evict 了）  │
  │   │  │       → 需要重新 pull                                │
  │   │  │                                                       │
  │   │  │  检查 pinned_lease_arguments_bytes_ 上限:             │
  │   │  │    → 超限 → ReleaseLeaseArgs + return false           │
  │   │  │       → work 设为 WAITING_FOR_AVAILABLE_PLASMA_MEMORY│
  │   │  │       → 等其他 lease 释放 arg 后重试                 │
  │   │  │                                                       │
  │   │  │  通过 → PinLeaseArgs(lease_spec, args)               │
  │   │  │    → pinned_lease_arguments_[obj_id] = (RayObject, refcount++) │
  │   │  │    → pinned_lease_arguments_bytes_ += size           │
  │   │  └──────────────────────────────────────────────────────┘
  │   │                    ↓ 通过
  │   │  ┌─ 门控3：本地资源分配 ─────────────────────────────────┐
  │   │  │  AllocateLocalTaskResources(required_resources)      │
  │   │  │    → 失败 → ReleaseLeaseArgs + TrySpillback         │
  │   │  │    → 成功 → allocated_instances 记录                │
  │   │  └──────────────────────────────────────────────────────┘
  │   │                    ↓ 通过
  │   │  ┌─ 门控4：PopWorker ───────────────────────────────────┐
  │   │  │  worker_pool_.PopWorker(spec, callback)               │
  │   │  │    → work->SetStateWaitingForWorker()                │
  │   │  │                                                       │
  │   │  │  callback(PoppedWorkerHandler):                      │
  │   │  │    ├─ 有 worker → GrantLease → 发给 worker 执行      │
  │   │  │    │   （lease 变为 GRANTED 状态）                     │
  │   │  │    │                                                   │
  │   │  │    ├─ canceled → RemoveFromGrantedLeasesIfExists     │
  │   │  │    │                                                   │
  │   │  │    ├─ 无 worker（各种失败）:                          │
  │   │  │    │   → ReleaseLeaseArgs(lease_id)  // 释放 pin     │
  │   │  │    │   → ReleaseWorkerResources                      │
  │   │  │    │   → 设为 WAITING 重试 / CancelLease             │
  │   │  └──────────────────────────────────────────────────────┘
  │   │
  │   └─ SpillWaitingLeases()                              // local_lease_manager.cc:444
  │      遍历 waiting_lease_queue_ 尾部：
  │        → GetBestSchedulableNode（找其他节点）
  │        → Spillback（转发 lease 到远程节点）
  │        → RemoveLeaseDependencies（清理本地 pull）
  │
  └─ done
```

### PinLeaseArgs 的引用计数机制

```cpp
// local_lease_manager.cc:840
void LocalLeaseManager::PinLeaseArgs(const LeaseSpecification &lease_spec,
                                     vector<unique_ptr<RayObject>> args) {
  auto [it, pinned_lease_inserted] =
      pinned_lease_arguments_.emplace(deps[i], make_pair(move(args[i]), 0));
  if (pinned_lease_inserted) {
    pinned_lease_arguments_bytes_ += it->second.first->GetSize();
  }
  it->second.second++;  // 引用计数 +1（多个 lease 可能依赖同一个 arg）
}
```

### ReleaseLeaseArgs 的清理

```cpp
// local_lease_manager.cc:864
void LocalLeaseManager::ReleaseLeaseArgs(const LeaseID &lease_id) {
  for (auto &arg : it->second) {
    auto arg_it = pinned_lease_arguments_.find(arg);
    arg_it->second.second--;  // 引用计数 -1
    if (arg_it->second.second == 0) {
      // 最后一个依赖此 arg 的 lease 也释放了
      pinned_lease_arguments_bytes_ -= arg_it->second.first->GetSize();
      pinned_lease_arguments_.erase(arg_it);
      // → ~RayObject → ~PlasmaBuffer → Release → ref_count--
    }
  }
  granted_lease_args_.erase(it);
}
```

---

## 九、SchedulingClass 与 Lease/Task 的关系

### SchedulingClass 是什么

**SchedulingClass 是一个整数 ID，是 `SchedulingClassDescriptor` 的去重索引。** 多个 Lease/Task 如果"调度特征相同"就映射到同一个 SchedulingClass。

### SchedulingClassDescriptor 的组成

```cpp
// scheduling_class_util.h
struct SchedulingClassDescriptor {
  ResourceSet resource_set;              // CPU/GPU/内存等资源需求
  LabelSelector label_selector;          // 节点标签选择器
  FunctionDescriptor function_descriptor; // 函数描述（模块名+函数名）
  int64_t depth;                         // 调用深度（嵌套层数）
  rpc::SchedulingStrategy scheduling_strategy; // 调度策略
  vector<FallbackOption> fallback_strategy;    // 降级策略
};
```

**不包含参数（dependencies）**。参数是 Lease 执行时的数据依赖，由 `LeaseDependencyManager` 管理，和调度分类无关。相同函数 + 相同资源需求 + 相同策略的 task，不管参数是什么，都属于同一个 SchedulingClass。

### 映射机制

```cpp
// lease_spec.cc:331-338 — LeaseSpecification 构造时
auto sched_cls_desc = SchedulingClassDescriptor(
    resource_set, label_selector, function_descriptor,
    depth, GetSchedulingStrategy(), fallback_strategy);
sched_cls_id_ = SchedulingClassToIds::GetSchedulingClass(sched_cls_desc);

// scheduling_class_util.cc — 全局双向映射
SchedulingClass SchedulingClassToIds::GetSchedulingClass(const SchedulingClassDescriptor &desc) {
  auto it = sched_cls_to_id_.find(desc);
  if (it == sched_cls_to_id_.end()) {
    sched_cls_id = ++next_sched_id_;  // 分配新 ID
    sched_cls_to_id_[desc] = sched_cls_id;
    sched_id_to_cls_.emplace(sched_cls_id, desc);
  } else {
    sched_cls_id = it->second;  // 复用已有 ID
  }
  return sched_cls_id;
}
```

### 举例

```
Task A: func=preprocess, resources={CPU:1, GPU:0}, depth=1, strategy=default
Task B: func=preprocess, resources={CPU:1, GPU:0}, depth=1, strategy=default
Task C: func=preprocess, resources={CPU:2, GPU:1}, depth=1, strategy=default
Task D: func=train,      resources={CPU:1, GPU:0}, depth=1, strategy=default

Descriptor(A) == Descriptor(B) → SchedulingClass = 7  (相同)
Descriptor(C) != Descriptor(A) → SchedulingClass = 8  (资源不同)
Descriptor(D) != Descriptor(A) → SchedulingClass = 9  (函数不同)

leases_to_grant_[7] = [Lease_A, Lease_B, ...]   // A 和 B 在同一队列
leases_to_grant_[8] = [Lease_C, ...]
leases_to_grant_[9] = [Lease_D, ...]
```

### 为什么要按 SchedulingClass 分组

1. **公平调度**：按 SchedulingClass 做 fair share，同一 class 的 granted lease 数量不能超过总 CPU / class 数量
2. **容量上限**（`sched_cls_cap_enabled_`）：限制同一 SchedulingClass 同时授予的 lease 数，防止嵌套 task 导致 worker 爆炸
3. **Spillback**：容量超限时可以按 class 整批 spillback 到其他节点

---

## 十、Lease 完成 → 清理链路

### "Lease 完成"的含义

**Lease 完成 = Worker 执行完 task/actor 创建 并归还。** "granted lease"就是"正在执行（或刚执行完）的那个 task"，不是"等待授予"的 lease。

### 完整触发链路

```
Worker 执行完毕 → 向 raylet 报告
  → NodeManager::HandleWorkerAvailable(worker)            // node_manager.cc:1366
    │
    ├─ if worker 有 granted lease (GetGrantedLeaseId 非空):
    │    CleanupLease(worker)                              // node_manager.cc:2357
    │      │
    │      ├─ local_lease_manager_.CleanupLease(worker, &lease)  // local_lease_manager.cc:769
    │      │    │  1. *lease = worker->GetGrantedLease()
    │      │    │  2. RemoveFromGrantedLeasesIfExists(*lease)
    │      │    │  3. ReleaseLeaseArgs(lease_id)              // 释放 PinLeaseArgs 的 plasma 引用
    │      │    │  4. ReleaseWorkerResources(worker)          // 释放 CPU/GPU 资源
    │      │    └─
    │      │
    │      ├─ if 非 Actor 创建 task:
    │      │    CancelWaitRequest(worker->WorkerId())
    │      │    CancelGetRequest(worker->WorkerId())
    │      │    // CancelGetRequest → 间接触发:
    │      │    //   CancelPull(pull_request_id)
    │      │    //     → DeactivateBundlePullRequest
    │      │    //       → UnpinObject(obj_id)
    │      │    //         → ~RayObject → ~PlasmaBuffer → Release
    │      │    //         → ref_count-- → 可能归零 → 可 LRU
    │      │
    │      └─ if Actor 创建 task:
    │           → ConvertWorkerToActor（不清理 lease，actor 继续）
    │
    ├─ if worker_idle (非 Actor):
    │    worker_pool_.PushWorker(worker)  // 归还空闲 worker
    │
    └─ cluster_lease_manager_.ScheduleAndGrantLeases()  // 触发新一轮调度
```

### 两层 Plasma 引用的释放

Raylet 对同一个对象可能持有**两层 Plasma 引用**：

| 引用层 | 由谁 pin | 何时释放 |
|--------|---------|---------|
| PullManager pin | `ActivateNextBundlePullRequest → TryPinObject` | `CancelPull → DeactivateBundlePullRequest → UnpinObject` |
| PinLeaseArgs pin | `PinLeaseArgsIfMemoryAvailable → GetObjectsFromPlasma` | `ReleaseLeaseArgs → ~RayObject → ~PlasmaBuffer → Release` |

两层都释放后对象 `ref_count_` 才归零，才能被 LRU 淘汰。

---

## 十一、PullManager 的 Bundle 优先级与内存配额

### 三种 Bundle 优先级

| 优先级 | Bundle | 来源 |
|--------|--------|------|
| 最高 | `get_request_bundles_` | `ray.get()` / `AsyncGet` |
| 中等 | `wait_request_bundles_` | `ray.wait()` |
| 最低 | `task_argument_bundles_` | Lease 依赖 |

### UpdatePullsBasedOnAvailableMemory 的调度逻辑

```cpp
// pull_manager.cc:232
void PullManager::UpdatePullsBasedOnAvailableMemory(int64_t num_bytes_available) {
  // 1. 激活 get 请求（最高优先级，无条件激活，可能需要挤占低优先级）
  while (get_requests_remaining) {
    DeactivateUntilMarginAvailable(task_argument_bundles_, ...);  // 挤占 task
    DeactivateUntilMarginAvailable(wait_request_bundles_, ...);   // 挤占 wait
    ActivateNextBundlePullRequest(get_request_bundles_, /*respect_quota=*/false, ...);
  }

  // 2. 激活 wait 请求（中等优先级，可能挤占 task）
  while (wait_requests_remaining) {
    DeactivateUntilMarginAvailable(task_argument_bundles_, ...);  // 挤占 task
    ActivateNextBundlePullRequest(wait_request_bundles_, /*respect_quota=*/true, ...);
  }

  // 3. 激活 task 请求（最低优先级，受配额限制）
  while (ActivateNextBundlePullRequest(task_argument_bundles_, /*respect_quota=*/true, ...)) {
  }

  // 4. 如果仍然超容，从尾部 deactive
  DeactivateUntilMarginAvailable(task_argument_bundles_, /*retain_min=*/1, ...);
  DeactivateUntilMarginAvailable(wait_request_bundles_, /*retain_min=*/1, ...);
}
```

### ActivateNextBundlePullRequest 内部流程

```cpp
// pull_manager.cc:113
bool PullManager::ActivateNextBundlePullRequest(bundles, respect_quota, objects_to_pull) {
  // 配额检查
  if (respect_quota && bytes_to_pull > RemainingQuota()) return false;

  for (const auto &obj_id : next_request.objects_) {
    if (needs_pull) {
      active_object_pull_requests_[obj_id].insert(next_request_id);
      TryPinObject(obj_id);           // ① 尝试 pin（对象可能已在本地）
      objects_to_pull->push_back(obj_id);
      ResetRetryTimer(obj_id);
    }
  }

  bundles.ActivateBundlePullRequest(next_request_id);
  return true;
}

// 后续在 UpdatePullsBasedOnAvailableMemory 末尾:
for (const auto &obj_id : objects_to_pull) {
  TryToMakeObjectLocal(obj_id);        // ② 发 PullRequest 到远端
}
```

**TryPinObject 和 SendPullRequest 是两个独立步骤**：
- `TryPinObject`：如果对象恰好在 pull 激活时已在本地（比如刚被 replication push 创建），直接占住引用防止被 LRU 淘汰
- `SendPullRequest`：如果对象不在本地，从远端拉取

对象已在本地 → TryPinObject 成功 → TryToMakeObjectLocal 发现 `object_is_local_` 返回 true 直接 return，不会发 PullRequest。

---

## 十二、关键数据结构总览

### PullManager

| 数据结构 | 类型 | 含义 |
|----------|------|------|
| `pinned_objects_` | `map<ObjectID, unique_ptr<RayObject>>` | PullManager pin 的对象（持有 PlasmaBuffer 引用） |
| `active_object_pull_requests_` | `map<ObjectID, set<request_id>>` | 当前正在 pull 的对象 |
| `object_pull_requests_` | `map<ObjectID, ObjectPullRequest>` | 所有 pull 请求（含远端位置信息） |
| `get_request_bundles_` | `BundlePullRequestQueue` | ray.get 的 bundle 队列（最高优先级） |
| `wait_request_bundles_` | `BundlePullRequestQueue` | ray.wait 的 bundle 队列（中等优先级） |
| `task_argument_bundles_` | `BundlePullRequestQueue` | Lease 依赖的 bundle 队列（最低优先级） |

### LeaseDependencyManager

| 数据结构 | 类型 | 含义 |
|----------|------|------|
| `local_objects_` | `flat_hash_set<ObjectID>` | 本地可用的对象集合（就绪判断依据） |
| `required_objects_` | `map<ObjectID, ObjectDependencies>` | 对象 → 依赖此对象的 lease/worker 列表 |
| `queued_lease_requests_` | `map<LeaseID, unique_ptr<LeaseDependencies>>` | Lease → 依赖信息 + missing 计数器 |
| `get_requests_` | `map<pair<WorkerID, req_id>, pair<vector<ObjectID>, PullRequestId>>` | ray.get 请求 |
| `wait_requests_` | `map<WorkerID, set<ObjectID>>` | ray.wait 请求 |

### LocalLeaseManager

| 数据结构 | 类型 | 含义 |
|----------|------|------|
| `leases_to_grant_` | `map<SchedulingClass, deque<Work>>` | 依赖已就绪、等待授予的 lease |
| `waiting_lease_queue_` | `deque<Work>` | 等待依赖的 lease |
| `waiting_leases_index_` | `map<LeaseID, iterator>` | Lease → 在等待队列中的位置 |
| `pinned_lease_arguments_` | `map<ObjectID, pair<RayObject, int>>` | PinLeaseArgs pin 的对象 + 引用计数 |
| `granted_lease_args_` | `map<LeaseID, vector<ObjectID>>` | Lease → 其依赖的 arg 列表 |
| `info_by_sched_cls_` | `map<SchedulingClass, SchedulingClassInfo>` | SchedulingClass → 容量/已授予信息 |

### CoreWorkerMemoryStore

| 数据结构 | 类型 | 含义 |
|----------|------|------|
| `objects_` | `map<ObjectID, shared_ptr<RayObject>>` | 内存中的对象 |
| `object_async_get_requests_` | `map<ObjectID, vector<callback>>` | GetAsync 等待队列 |
