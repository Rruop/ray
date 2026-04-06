# Ray 对象生命周期、Recovery 与 Pin 机制深度分析

## 文档信息

| 项目 | 内容 |
|------|------|
| 标题 | Ray Object 生命周期、Recovery 机制与 Pin 原理深度分析 |
| 基于 Ray 版本 | 2.52.1 |
| 关联文档 | phase1-object-replication-implementation-plan.md, phase2-pin-transfer-implementation-plan.md |

---

## 目录

1. [血缘重建触发机制](#1-血缘重建触发机制)
2. [ray.get 完整执行流程](#2-rayget-完整执行流程)
3. [ray.get 超时行为](#3-rayget-超时行为)
4. [Recovery 触发路径与 ray.get 的关系](#4-recovery-触发路径与-rayget-的关系)
5. [非 Owner 节点的 ray.get](#5-非-owner-节点的-rayget)
6. [ObjectRef 内容与 Ownership-Based Object Directory](#6-objectref-内容与-ownership-based-object-directory)
7. [两层引用计数机制](#7-两层引用计数机制)
8. [PinObjectIDs 机制详解](#8-pinobjectids-机制详解)
9. [Release 语义](#9-release-语义)
10. [ReportObjectAdded 流程](#10-reportobjectadded-流程)
11. [NotifyWorkerBlocked 资源释放机制](#11-notifyworkerblocked-资源释放机制)
12. [Phase 1 对象副本推送同步/异步分析](#12-phase-1-对象副本推送同步异步分析)
13. [Owner 的 pinned_at_node_id_ 修改时机](#13-owner-的-pinned_at_node_id_-修改时机)
14. [Pin 转移 vs Recovery 的 memory_store_.Put 差异](#14-pin-转移-vs-recovery-的-memory_storeput-差异)
15. [PinObjectIDs 回调的执行位置](#15-pinobjectids-回调的执行位置)
16. [OBJECT_IN_PLASMA 哨兵模式完整生命周期](#16-object_in_plasma-哨兵模式完整生命周期)
17. [HandlePinObjectIDs 的执行者：Raylet 而非 Owner/Worker](#17-handlepinobjectids-的执行者raylet-而非-ownerworker)
18. [对象从创建到回收的完整生命周期](#18-对象从创建到回收的完整生命周期)
19. [主本与副本的生命周期管理差异](#19-主本与副本的生命周期管理差异)
20. [两个 pinned_objects_ 的区别：PullManager vs LocalObjectManager](#20-两个-pinned_objects_-的区别pullmanager-vs-localobjectmanager)
21. [ray.get 期间 PullManager 的临时 Pin 机制](#21-rayget-期间-pullmanager-的临时-pin-机制)
22. [Phase 2 Pin 转移后旧 Pin 的释放机制](#22-phase-2-pin-转移后旧-pin-的释放机制)

---

## 1. 血缘重建触发机制

### 1.1 触发条件

血缘重建（Lineage Reconstruction）**仅在 `pinned_at_node_id_` 指向的节点被判定 Dead 时触发**，不是任意副本丢失都会触发。

### 1.2 触发链路

```
GCS 检测到节点 Dead
  → Owner 收到通知
    → ReferenceCounter::ResetObjectsOnRemovedNode(dead_node_id)
      → 遍历所有 owned objects
        → if pinned_at_node_id_ == dead_node_id:
             UnsetObjectPrimaryCopy()
             objects_to_recover_.push_back(object_id)
      → ObjectRecoveryManager::RecoverObject()
        → 先尝试 PinExistingObjectCopy（找其他副本）
        → 找不到 → ReconstructObject（血缘重算）
```

**关键代码**：`reference_counter.cc:893`

```cpp
void ReferenceCounter::ResetObjectsOnRemovedNode(const NodeID &node_id) {
    absl::MutexLock lock(&mutex_);
    for (auto it = object_id_refs_.begin(); it != object_id_refs_.end(); it++) {
        if (it->second.pinned_at_node_id_.value_or(NodeID::Nil()) == node_id ||
            it->second.spilled_node_id == node_id) {
            UnsetObjectPrimaryCopy(it);
            if (!it->second.OutOfScope(lineage_pinning_enabled_)) {
                objects_to_recover_.push_back(object_id);
            }
        }
        RemoveObjectLocationInternal(it, node_id);
    }
}
```

### 1.3 Phase 2 Pin 转移的意义

将 `pinned_at_node_id_` 从可抢占节点 A 转移到稳定节点 B 后，A 挂了不再触发 Recovery（因为 `pinned_at` 不再指向 A）。

---

## 2. ray.get 完整执行流程

### 2.1 两层等待架构

`ray.get` 采用两层阻塞机制：**内存层**（条件变量）和 **Plasma 层**（轮询）。

### 2.2 第一层：内存等待（条件变量阻塞）

`CoreWorker::Get` → `CoreWorker::GetObjects` → `memory_store_->Get` → `CoreWorkerMemoryStore::GetImpl`

`GetImpl` 的核心是一个基于 `std::condition_variable` 的 `GetRequest` 对象：

```
memory_store.cc:

class GetRequest {
    std::mutex mutex_;
    std::condition_variable cv_;     // ← 线程阻塞在这里
    bool is_ready_ = false;
};

GetRequest::Wait(timeout_ms):
    if timeout == -1:
        cv_.wait(lock, [this]{ return is_ready_; })   // 无限等
    else:
        cv_.wait_for(lock, timeout_ms, ...)            // 限时等
```

执行流程：
1. 先查 `objects_` map，已有的立刻返回
2. 缺失的对象创建 `GetRequest`，注册到 `object_get_requests_[object_id]`
3. 通知 Raylet：`NotifyWorkerBlocked()`（让 Raylet 释放 worker 资源给别的 task）
4. **线程阻塞**在 `cv_.wait` / `cv_.wait_for`
5. 被唤醒后通知 Raylet：`NotifyWorkerUnblocked()`

**唤醒机制**：任何地方调用 `CoreWorkerMemoryStore::Put(object_id, value)` 时，会找到注册的 `GetRequest`，调用 `GetRequest::Set()` → `cv_.notify_all()` → 睡眠线程被唤醒。

### 2.3 第二层：Plasma 拉取（轮询阻塞）

如果内存层返回的是 `IsInPlasmaError` 哨兵（说明对象太大，在 Plasma 共享内存中），进入第二层：

```
CoreWorkerPlasmaStoreProvider::Get():
    1. raylet_ipc_client_->AsyncGetObjects(ids)   // 通知 Raylet 发起 Pull
           ↓ Raylet 侧
         NodeManager::AsyncGet()
           → object_manager_.Pull()               // 从远程节点拉数据

    2. 轮询循环:
       while (!all_ready && !timed_out) {
           store_client_->Get(ids, batch_timeout)  // 阻塞在 Plasma IPC
           // Plasma Store 收到数据后返回
           check_signals()
       }
```

这层是**轮询**而非条件变量，每次调 Plasma Store 的 `Get` 阻塞 `batch_timeout` 毫秒，等 Plasma Store 收到数据后返回。

### 2.4 完整调用链

```
Python: ray.get(ref)
  │
  ▼
CoreWorker::Get()                              [core_worker.cc:1495]
  │
  ▼
CoreWorker::GetObjects()                        [core_worker.cc:1548]
  │
  ├─ reference_counter_->HasOwner(ids)          # 验证 ownership
  │
  ├─ memory_store_->Get()                       # 第一层：内存
  │     │
  │     ▼
  │   CoreWorkerMemoryStore::GetImpl()          [memory_store.cc:259]
  │     ├─ 检查 objects_ map 是否有现成值
  │     ├─ 创建 GetRequest（基于 condition_variable）
  │     ├─ raylet_ipc_client_->NotifyWorkerBlocked()
  │     ├─ 阻塞: GetRequest::Wait(iteration_timeout)  ◄── 线程在这里睡眠
  │     │    被唤醒条件: CoreWorkerMemoryStore::Put() → GetRequest::Set() → cv_.notify_all()
  │     ├─ 迭代间检查 Python signals
  │     └─ raylet_ipc_client_->NotifyWorkerUnblocked()
  │
  ├─ 过滤出 IsInPlasmaError 的对象 → plasma_object_ids
  │
  └─ plasma_store_provider_->Get()              # 第二层：Plasma/远程
        │
        ▼
      CoreWorkerPlasmaStoreProvider::Get()      [plasma_store_provider.cc:253]
        ├─ raylet_ipc_client_->AsyncGetObjects()   # 通知 Raylet 发起 Pull
        │     │
        │     ▼ (Raylet 侧)
        │   NodeManager::AsyncGet()             [node_manager.cc:2318]
        │     → LeaseDependencyManager::StartGetRequest()
        │       → object_manager_.Pull()        # 从远程节点拉数据
        │
        ├─ 轮询循环:
        │   ├─ store_client_->Get(ids, batch_timeout)  ◄── 阻塞在 Plasma IPC
        │   │     PlasmaClient::GetBuffers()    [client.cc:291]
        │   │       → SendGetRequest → PlasmaReceive（阻塞直到数据到达或超时）
        │   ├─ 检查 signals
        │   ├─ WarnIfFetchHanging()
        │   └─ yield plasma lock
        │
        └─ ScopedResponse 析构 → CancelGetRequest（取消 Pull）
```

---

## 3. ray.get 超时行为

### 3.1 timeout 在两层之间传递并递减

```
CoreWorker::GetObjects(timeout_ms=5000):
    │
    ├─ start = now()
    ├─ memory_store_->Get(timeout_ms=5000)
    │    └─ 假设花了 2000ms 拿到部分结果
    │
    ├─ remaining = 5000 - (now() - start) = 3000ms
    │
    └─ plasma_store_provider_->Get(timeout_ms=3000)
         └─ 轮询 batch_timeout = min(remaining, signal_check_interval)
              超过 3000ms → Status::TimedOut
```

### 3.2 超时后的行为

- `GetImpl` 清理注册的 `GetRequest`，返回 `Status::TimedOut`
- `GetObjects` 中拿不到的对象 `results[i] = nullptr`
- 传回 Python 层，抛出 **`GetTimeoutError`**
- **不影响对象本身**——对象后续仍可能通过 Recovery 恢复，下次 `ray.get` 可以拿到

---

## 4. Recovery 触发路径与 ray.get 的关系

### 4.1 核心结论

**`ray.get` 本身不触发 Recovery。** Recovery 完全是被动的，由**节点死亡通知**驱动。两条路径独立并行：

```
路径 A: ray.get（被动等待）          路径 B: Recovery（节点死亡驱动）
================================    =====================================
ray.get(ref)                        GCS 检测到节点 Dead
  │                                   │
  ▼                                   ▼
memory_store_.Get()                 on_node_change 回调 [core_worker.cc:751]
  │                                   │
  ▼                                   ▼
cv_.wait()  ◄── 线程睡在这里         ReferenceCounter::ResetObjectsOnRemovedNode()
  │                                   │ 遍历所有 owned objects
  │                                   │ if pinned_at == dead_node:
  │                                   │   objects_to_recover_.push_back(obj)
  │                                   │
  │                                   ▼ (每 100ms 定时任务)
  │                                 FlushObjectsToRecover()
  │                                   │
  │                                   ├─ memory_store_.Delete(lost_objects)  ①
  │                                   │
  │                                   ▼
  │                                 RecoverObject(object_id)
  │                                   │
  │                                   ├─ 有其他副本?
  │                                   │   ├─ Yes → PinExistingObjectCopy()
  │                                   │   │         成功后:
  │              ┌─────────────────────│───│── memory_store_.Put(OBJECT_IN_PLASMA)  ②
  │              │                     │   │
  │              │                     │   └─ No → ReconstructObject()
  │              │                     │             task_manager_.ResubmitTask()
  │              │                     │             Task 重新执行...
  │              │                     │             SealExisting → PinObjectIDs
  │              │                     │             memory_store_.Put(OBJECT_IN_PLASMA)  ③
  │              ▼                     │
cv_.notify_all() ←─────────────────────┘
  │
  ▼
ray.get 返回结果
```

### 4.2 Recovery 的两种情况

**情况 1：有其他副本（Phase 1 推送到稳定节点 B 的副本还在）**

```
RecoverObject()
  → IsPlasmaObjectPinnedOrSpilled: pinned_at=Nil, spilled=false
  → 需要恢复
  → object_lookup_ 查全局目录，发现 B 有副本
  → PinExistingObjectCopy(B):
      raylet_client_pool_->GetOrConnectByAddress(B)->PinObjectIDs(...)
      成功 → memory_store_.Put(OBJECT_IN_PLASMA)    ← 唤醒 ray.get
           → UpdateObjectPinnedAtRaylet(obj, B)
```

**情况 2：无副本，需要血缘重算**

```
RecoverObject()
  → object_lookup_ 查全局目录，无任何副本
  → ReconstructObject():
      → task_manager_.ResubmitTask(task_id)
      → Task 被重新调度到某个 Worker 执行
      → Worker 执行完毕 → SealExisting → PinObjectIDs
      → Owner 收到 Task Reply → memory_store_.Put(OBJECT_IN_PLASMA)  ← 唤醒 ray.get
```

### 4.3 关键连接点

**`memory_store_.Put()`** 是 Recovery 和 `ray.get` 之间唯一的通信桥梁。Recovery 完成后调用 Put 放入一个 `OBJECT_IN_PLASMA` 哨兵，这个 Put 会触发 `cv_.notify_all()` 唤醒正在 `ray.get` 中睡眠的线程。

### 4.4 时序关系

```
时间  ─────────────────────────────────────────────────────────→

t0: ray.get(ref) 发起, 线程阻塞在 cv_.wait()
t1: 节点 A 挂了
t2: GCS 检测到 Dead, 通知 Owner
t3: ResetObjectsOnRemovedNode: objects_to_recover_ += obj
t4: 定时任务(100ms): FlushObjectsToRecover → RecoverObject
t5: Recovery 完成 → memory_store_.Put() → cv_.notify_all()
t6: ray.get 线程被唤醒, 从 Plasma Pull 数据, 返回

如果有 timeout 且 timeout < (t5 - t0):
    → ray.get 在 t_timeout 超时返回 GetTimeoutError
    → Recovery 仍在后台继续（不受影响）
    → 用户可以再次 ray.get(ref) 拿到结果
```

---

## 5. 非 Owner 节点的 ray.get

### 5.1 ray.get 可以在任何节点执行

`ray.get(ref)` 可以在 Driver、任意 Worker 上调用，不限于 Owner。

```python
# Driver (节点 D) 是 Owner
ref = task.remote()          # Owner = D

# 场景 1: Driver 自己 get（Owner == Getter）
ray.get(ref)

# 场景 2: 把 ref 传给另一个 Task
@ray.remote
def consume(ref):
    return ray.get(ref)      # 节点 C 执行 get（Non-owner）

consume.remote(ref)
```

每个 `ObjectRef` 内部携带了 **Owner 的地址**（`owner_address`），这是非 Owner 节点定位对象的起点。

### 5.2 非 Owner 节点的对象定位：基于 Owner 的 Pub/Sub

```
Non-owner (节点 C)                          Owner (节点 D)
===================                         ===============

ray.get(ref)
  │
  ▼
plasma_store_provider_->Get()
  │
  ▼
raylet_ipc_client_->AsyncGetObjects(ids, owner_addresses)
  │
  ▼ (本地 Raylet)
NodeManager::AsyncGet()
  → object_manager_.Pull(object_refs)
     │
     ▼
  PullManager::Pull()
     │ 返回 objects_to_locate (首次见到的 object)
     ▼
  OwnershipBasedObjectDirectory
    ::SubscribeObjectLocations(object_id, owner_address)
     │
     │  订阅 RPC ──────────────────→  CoreWorker::ProcessSubscribeObjectLocations()
     │                                  │
     │                                  ▼
     │                                ReferenceCounter::PublishObjectLocationSnapshot()
     │                                  │
     │                                  ▼
     │                                PushToLocationSubscribers()
     │                                  发布: {node_ids=[A], size=1MB, ...}
     │                                  │
     │  ←── 位置更新 ─────────────────  │
     ▼
  PullManager::OnLocationChange(object_id, locations=[A])
     │
     ▼
  TryToMakeObjectLocal()
     → send_pull_request_(object_id, node_A)  # 向节点 A 发 Pull 请求
     → 远程节点 Push 数据到本地 Plasma
     → store_client_->Get() 轮询返回数据
     → ray.get 返回
```

### 5.3 非 Owner 节点的 OBJECT_IN_PLASMA 哨兵来源

ObjectRef 到达非 Owner 节点时，必然经过反序列化，反序列化过程会通过 `FutureResolver` 往 memory store 放值。

```
Python 反序列化 ObjectRef
  │
  ▼
CoreWorker::RegisterOwnershipInfoAndResolveFuture()  [core_worker.cc:944]
  │
  ├─ 情况 A（快速路径）: 发送端序列化时已内联了对象状态
  │    → ProcessResolvedObject() 立即执行
  │      → memory_store_->Put(OBJECT_IN_PLASMA 或 实际数据)     ← 同步放入
  │
  └─ 情况 B（慢速路径）: 对象在发送时还没 ready
       → ResolveFutureAsync()
         → 发送 GetObjectStatus RPC 到 Owner
           → Owner 的 memory_store_.GetAsync() 等对象 ready
           → 回复
         → 回调: ProcessResolvedObject()
           → memory_store_->Put(OBJECT_IN_PLASMA 或 实际数据)   ← 异步放入
```

### 5.4 ray.get 和反序列化的时序

两种情况都能正确工作：

```
时序 1: 先 Put 后 Get（常见）
─────────────────────────────────
t0: 反序列化 ObjectRef → ProcessResolvedObject → memory_store_.Put(哨兵)
t1: ray.get(ref) → memory_store_.Get() → 立即找到哨兵 → 走 Plasma 拉取路径

时序 2: 先 Get 后 Put（对象还没 ready）
─────────────────────────────────
t0: ray.get(ref) → memory_store_.Get()
      → objects_ 中找不到 → 创建 GetRequest → cv_.wait() 阻塞
t1: FutureResolver RPC 回调 → memory_store_.Put(哨兵)
      → 查找 object_get_requests_ → get_request->Set() → cv_.notify_all()
t2: ray.get 线程被唤醒 → 拿到 OBJECT_IN_PLASMA → 走 Plasma 拉取路径
```

**关键机制**：`memory_store_.Put()` 会查找所有注册的 `GetRequest` 并通过 `cv_.notify_all()` 唤醒。所以无论 Get 和 Put 哪个先到，都能正确汇合。

### 5.5 Owner 死亡的处理

```
FutureResolver::ResolveFutureAsync()
  → GetObjectStatus RPC 发到 Owner
    → Owner 已死，RPC 失败
      → memory_store_->Put(RayObject(OWNER_DIED_ERROR))   ← 放入错误对象
        → cv_.notify_all() 唤醒 ray.get
          → Python 层抛出 OwnerDiedError
```

### 5.6 Recovery 成功后如何通知非 Owner 节点

Recovery 完成 → 通知非 Owner → 解除阻塞，涉及 4 个阶段：

```
Owner (节点 D)                  非 Owner (节点 C)              新节点 (B)
=============                   ==================             ==========

                                ray.get(ref)
                                  │
                                  ▼
                                Plasma 轮询  ◄── 阻塞中
                                  │ (已向 Owner 订阅了位置)

┌─ GCS: 节点 A dead ──→         │
│                                │
│ ResetObjectsOnRemovedNode(A)   │
│   ├─ 移除 A 的位置              │
│   ├─ PushToLocationSubscribers │
│   │    发布: {node_ids=[], pending_creation=false}
│   │         ───────────────→   │
│   │                            │ PullManager::OnLocationChange(locations=[])
│   │                            │   → 无可用位置，等待下次更新
│   │                            │
│   └─ objects_to_recover_ += obj│
│                                │
│ (100ms 定时任务)               │
│ RecoverObject(obj)             │
│   │                            │
│   ├─ PinExistingObjectCopy(B)  │
│   │  → PinObjectIDs RPC ──────────────────────→ Pin 成功
│   │  ← OK                     │
│   │  UpdateObjectPinnedAtRaylet(obj, B)
│   │    → PushToLocationSubscribers
│   │       发布: {node_ids=[B]} │
│   │            ─────────────→  │
│   │                            │ PullManager::OnLocationChange(locations=[B])  ← ★
│   │                            │   → TryToMakeObjectLocal()
│   │                            │     → send_pull_request_(obj, B)
│   │                            │            ─────────────────→ Push(obj, C)
│   │                            │            ←── 数据到达 ────
│   │                            │ 数据写入本地 Plasma
│   │                            │ store_client_->Get() 返回    ← ★ 解除阻塞
│   │                            ▼
│   │                            ray.get 返回结果 ✅
```

### 5.7 Owner 节点 vs 非 Owner 节点 ray.get 的唤醒差异

| 场景 | 唤醒机制 | 代码位置 |
|------|---------|---------|
| **Owner 节点自己 ray.get** | Recovery 完成后 `memory_store_.Put(OBJECT_IN_PLASMA)` → `cv_.notify_all()` 直接唤醒线程 | `object_recovery_manager.cc:127` |
| **非 Owner 节点 ray.get** | Owner 通过 Pub/Sub 发布新位置 → PullManager Pull → 数据到达本地 Plasma → 轮询循环退出 | `ownership_object_directory.cc:320` → `pull_manager.cc:362` |

---

## 6. ObjectRef 内容与 Ownership-Based Object Directory

### 6.1 ObjectRef 只存 Owner 地址

```protobuf
// common.proto:713
message ObjectReference {
    bytes object_id = 1;              // 对象 ID
    Address owner_address = 2;        // Owner 的地址（唯一的位置信息）
    string call_site = 3;             // 调试用的调用点
    optional string tensor_transport = 4;
}
```

**不包含**：
- 对象数据存储在哪个节点 ❌
- 副本在哪些节点 ❌
- 对象大小 ❌
- 引用计数 ❌

### 6.2 位置信息由 Owner 的 ReferenceCounter 集中管理

```cpp
// reference_counter.h
struct Reference {
    // 所有已知副本的位置集合（用于 Pull 定位）
    absl::flat_hash_set<NodeID> locations;

    // 主 Pin 副本所在节点（有且仅有一个，这个节点挂了就触发 Recovery）
    std::optional<NodeID> pinned_at_node_id_;

    // Spill 相关
    std::string spilled_url;
    NodeID spilled_node_id;
    bool spilled = false;
};
```

### 6.3 locations vs pinned_at_node_id_

| 字段 | 含义 | 数量 | 谁设置的 |
|------|------|------|---------|
| `pinned_at_node_id_` | 主 Pin 所在节点 | 0 或 1 | `PinObjectIDs` 回调 / `UpdateObjectPinnedAtRaylet` |
| `locations` | 所有已知副本节点 | 0..N | `ReportObjectAdded` → `AddObjectLocation` |

两者的关系：

```
locations = {A, B, C}           ← 三个节点都有副本
pinned_at_node_id_ = A          ← 只有 A 的副本有 Pin 保护

A 挂了:
  → pinned_at_node_id_ 清除 → 触发 Recovery
  → locations 移除 A → {B, C}
  → Recovery 尝试 PinExistingObjectCopy(B 或 C)
  → 成功后 pinned_at_node_id_ = B
```

### 6.4 为什么这样设计（Ownership-Based vs 传统 DHT）

```
方案 A（传统分布式哈希表）:
  ObjectRef 存: {object_id, location_node_1, location_node_2, ...}
  问题:
    1. ObjectRef 在传递过程中，位置可能已变（对象被驱逐/迁移）
    2. 多个持有者各自缓存位置 → 一致性问题
    3. 更新位置需要通知所有持有者 → 广播风暴

方案 B（Ray 的 Ownership 方案）:        ← 当前设计
  ObjectRef 存: {object_id, owner_address}
  Owner 集中维护: {locations, pinned_at, spilled_url, ...}

  优势:
    1. 单一数据源 → 无一致性问题
    2. 位置变更只需更新 Owner → 无广播
    3. ObjectRef 轻量（固定大小），可以高效序列化传递
    4. Owner 挂了 → 所有该 Owner 的对象都变成 OWNER_DIED 错误
       （这是一个可接受的故障语义）
```

### 6.5 具体运作方式

```
节点 C 调用 ray.get(ref):
  │
  │ ref 只知道 Owner 地址，不知道对象在哪
  │
  ▼
本地 Raylet 的 PullManager:
  │
  │ 需要知道对象在哪 → 通过 OwnershipBasedObjectDirectory
  │
  ▼
OwnershipBasedObjectDirectory::SubscribeObjectLocations(object_id, owner_address)
  │
  │ 向 Owner 订阅位置
  │
  ▼
Owner 的 ReferenceCounter:
  │ 查 Reference.locations = {A, B}
  │
  ▼
PushToLocationSubscribers():
  │ 发布: {node_ids=[A, B], size=1MB}
  │
  ▼
节点 C 的 PullManager::OnLocationChange(locations=[A, B]):
  │
  │ 从 A 或 B 中随机选一个发 Pull 请求
  │
  ▼
  数据拉取完成 → ray.get 返回
```

**总结**：ObjectRef 是一个"指向 Owner 的指针"，Owner 是"指向数据的指针"。这种两级间接寻址的设计牺牲了一次 RPC 往返（订阅 Owner），换来了全局一致性和简洁性。

---

## 7. 两层引用计数机制

### 7.1 两层独立的引用计数

Ray 中有**两层独立的引用计数**：

```
层次 1: ReferenceCounter（Owner 维护，分布式语义）
  - local_ref_count: Python 侧还有多少 ObjectRef 指向这个对象
  - 降到 0 → 对象可以被回收

层次 2: Plasma Client objects_in_use_（每个进程本地，共享内存语义）
  - count: 这个进程有多少次 Get/Create 还没 Release
  - 降到 0 → Plasma Server 知道这个进程不再读写这块共享内存
  - 对象进入 LRU，但不一定被驱逐（其他进程可能还在用）
```

### 7.2 不是同时为 0 才回收，而是层次 1 驱动层次 2

完整的回收链路：

```
Python: del ref / ref 离开作用域
  │
  ▼
Owner 的 ReferenceCounter:
  local_ref_count -= 1
  if RefCount() == 0:
    DeleteReferenceInternal()
      → OutOfScope() == true
        → OnObjectOutOfScopeOrFreed()
          → UnsetObjectPrimaryCopy()          // pinned_at_node_id_ 清除
          → 触发 on_object_out_of_scope_or_freed_callbacks
            │
            ▼
          unpin_object(object_id)             // 发布 WORKER_OBJECT_EVICTION 消息
            → object_info_publisher_->Publish(eviction_msg)
              │
              │  PubSub 通知到 Raylet
              ▼
          Raylet 的 LocalObjectManager:
            subscription_callback 被触发
              → ReleaseFreedObject(object_id)
                → is_freed_ = true
                → pinned_objects_.erase(object_id)    // 删除持有的 RayObject
                  → RayObject 析构
                    → SharedMemoryBuffer 析构
                      → PlasmaClient::Release()
                        → objects_in_use_ count: 1 → 0
                          → MarkObjectUnused()
                            → Plasma Store 知道没人用了
                              → 对象进入 LRU，可被驱逐
```

### 7.3 回收阶段表

| 阶段 | 发生什么 | 触发条件 |
|------|---------|---------|
| 1 | Owner `RefCount() → 0` | Python 侧所有 ObjectRef 被 del |
| 2 | Owner 发布 `WORKER_OBJECT_EVICTION` | RefCount 归零触发 callback |
| 3 | Raylet 收到通知，删除 `pinned_objects_[obj]` | PubSub 回调 |
| 4 | `RayObject` 析构 → `SharedMemoryBuffer` 析构 → `PlasmaClient::Release()` | C++ RAII 析构链 |
| 5 | Plasma `objects_in_use_` count → 0 | Release 调用 |
| 6 | 对象进入 LRU，内存紧张时被驱逐 | Plasma Store 的 eviction 策略 |

注意第 6 步：即使 `objects_in_use_` 为 0，对象也**不一定立刻被删除**。它只是从"被保护"变成"可驱逐"，进入 LRU 队列。Plasma Store 在需要空间时才会真正驱逐。

---

## 8. PinObjectIDs 机制详解

### 8.1 Pin 的物理本质

**Pin 的本质：Raylet 用自己的 PlasmaClient 做 `Get`，拿到 `RayObject`（内含 `shared_ptr<SharedMemoryBuffer>`），然后一直持有不释放。**

```
Plasma Store (共享内存)
┌──────────────────────────────────┐
│  Object X: [100MB data]          │
│  objects_in_use_[X].count = 1    │  ← Raylet 的 PlasmaClient 持有
│                                  │     (通过 pinned_objects_ 中的
│                                  │      RayObject → SharedMemoryBuffer)
│                                  │
│  只要 count > 0，LRU 不会驱逐    │
└──────────────────────────────────┘
```

### 8.2 HandlePinObjectIDs 完整流程

```cpp
// node_manager.cc:2590
void NodeManager::HandlePinObjectIDs(request, reply, callback) {
    // Step 1: 从本地 Plasma Store Get 对象
    GetObjectsFromPlasma(object_ids, &results);
    //   → PlasmaClient::Get()
    //   → objects_in_use_[obj].count += 1   // Raylet 的 plasma client 持有引用
    //   → 返回 unique_ptr<RayObject>（包含 SharedMemoryBuffer）

    // Step 2: 交给 LocalObjectManager 长期保管
    local_object_manager_.PinObjectsAndWaitForFree(
        object_ids, std::move(results), owner_address, generator_id);
}
```

### 8.3 PinObjectsAndWaitForFree 做了什么

```cpp
// local_object_manager.cc:31
void LocalObjectManager::PinObjectsAndWaitForFree(...) {
    for (each object) {
        // 1. 将 RayObject 存入 pinned_objects_ map —— 这就是"Pin"
        pinned_objects_.emplace(object_id, std::move(object));
        //   ↑ RayObject 被 move 到 map 中，只要 map entry 存在，
        //     SharedMemoryBuffer 的引用计数就 > 0，
        //     Plasma Store 就不能驱逐这块内存

        // 2. 订阅 Owner 的 WORKER_OBJECT_EVICTION 频道
        core_worker_subscriber_->Subscribe(
            WORKER_OBJECT_EVICTION, owner_address, object_id,
            subscription_callback,    // Owner 说"可以释放了"
            owner_dead_callback       // Owner 挂了也释放
        );
    }
}
```

### 8.4 Unpin 机制（Owner 通知 Raylet 释放）

```
Owner: RefCount() → 0
  │
  ▼
发布 WORKER_OBJECT_EVICTION 消息
  │
  ▼ (PubSub)
Raylet 的 subscription_callback:
  ReleaseFreedObject(object_id)
    → pinned_objects_.erase(object_id)        // 从 map 中删除
      → unique_ptr<RayObject> 被销毁          // RAII 析构
        → SharedMemoryBuffer 引用计数 -1
          → PlasmaClient::Release()
            → objects_in_use_[X].count: 1 → 0
              → MarkObjectUnused()
              → 对象进入 LRU ← 此时才可被驱逐
```

如果 Owner 挂了（`owner_dead_callback`），也走同样的 `ReleaseFreedObject` 路径。

### 8.5 三种 PinObjectIDs 场景对比

#### 场景 1：对象创建（Worker 发起，Worker ≠ Owner）

```
Worker（可抢占节点 A，执行 Task）          Owner（稳定节点 D，提交 Task 的 Driver）
    │
    │ SealExisting(object_id)
    │
    │ local_raylet_rpc_client_->PinObjectIDs(
    │     owner_addr,          ← Owner D 的地址（不是自己）
    │     {object_id}, ...)
    │         │
    │         ▼
    │   本地 Raylet A: HandlePinObjectIDs
    │     → GetObjectsFromPlasma → 拿到 RayObject
    │     → pinned_objects_[obj] = RayObject       ← Pin 住
    │     → Subscribe(WORKER_OBJECT_EVICTION,
    │                 owner_addr=D)                 ← 订阅的是 Owner D
    │     → reply(success=true)
    │
    │ ← OK
```

```cpp
// core_worker.cc:1209-1212
const auto &owner_addr =
    owner_address != nullptr ? *owner_address : rpc_address_;
local_raylet_rpc_client_->PinObjectIDs(owner_addr, {object_id}, ...);
//                                     ^^^^^^^^^ Owner 的地址
```

关键点：
- **调用方**：执行 Task 的 **Worker**（不是 Owner）
- **接收方**：Worker 的**本地 Raylet**（同一节点）
- **`owner_addr` 参数**：传的是 Owner 的地址，不是 Worker 自己
- Raylet 订阅的是 **Owner** 的 eviction 通知，不是 Worker 的

#### 场景 2：Recovery（Owner 发起）

```
Owner（节点 D）                              远程 Raylet（节点 B，有副本）
    │
    │ PinExistingObjectCopy(object_id, B_addr)
    │
    │ raylet_client_pool_
    │   ->GetOrConnectByAddress(B_addr)      ← 连接到远程 Raylet B
    │   ->PinObjectIDs(
    │       rpc_address_,                     ← 传的是自己（Owner）的地址
    │       {object_id}, ...)
    │         │
    │         ▼
    │   远程 Raylet B: HandlePinObjectIDs
    │     → GetObjectsFromPlasma → 拿到 RayObject
    │     → pinned_objects_[obj] = RayObject
    │     → Subscribe(WORKER_OBJECT_EVICTION,
    │                 owner_addr=D)
    │     → reply(success=true)
    │
    │ ← OK
    │ UpdateObjectPinnedAtRaylet(obj, B)
```

关键点：
- **调用方**：**Owner** 自己
- **接收方**：**远程 Raylet**（有副本的节点 B）

#### 场景 3：Phase 2 Pin 转移（Owner 发起）

和 Recovery 完全一样的模式：Owner 发起，连接远程 Raylet，传自己的地址。

### 8.6 场景汇总表

```
                  场景 1:                场景 2:                 场景 3:
                  对象创建               Recovery                Pin 转移
─────────────────────────────────────────────────────────────────────────
谁调用?           Worker（执行Task的）    Owner                   Owner

调用哪个 Raylet?  本地 Raylet            远程 Raylet（有副本的）   远程 Raylet（稳定节点）
                  (local_raylet_         (raylet_client_pool_    (raylet_client_pool_
                   rpc_client_)           ->GetOrConnect...)      ->GetOrConnect...)

owner_addr        传 Owner 的地址        传自己的地址             传自己的地址
参数              (owner_address)        (rpc_address_)          (rpc_address_)

Raylet 订阅谁     Owner                  Owner                   Owner
的 eviction?

回调中做什么？     Release + Replicate    UpdateObjectPinnedAt     UpdateObjectPinnedAt
                  (不更新 pinned_at,     + memory_store_.Put      (不需要 Put,
                   由 TaskReply 更新)                              对象不在 Owner 节点)
```

### 8.7 PinObjectIDs 与 Owner 的 ReferenceCounter 的关系

**`PinObjectIDs` RPC 本身不修改 Owner 的 `ReferenceCounter`。修改发生在调用方收到回复后。**

| 场景 | 谁修改 `pinned_at_node_id_` | 在哪里修改 |
|------|----------------------------|-----------|
| 对象创建 | Owner 收到 Task Reply 时 | `TaskManager::CompletePendingTask` → `UpdateObjectPinnedAtRaylet` |
| Recovery | Owner 的 PinObjectIDs 回调中 | `ObjectRecoveryManager::PinExistingObjectCopy` 回调 |
| Phase 2 Pin 转移 | Owner 的 PinObjectIDs 回调中 | `CoreWorker::DoPinTransfer` 回调 |

可以把整个 Pin 体系理解为两层：

| 层次 | 在哪里 | 做什么 | 谁管理 |
|------|-------|--------|-------|
| **物理 Pin** | Raylet 的 `pinned_objects_` | 持有 `RayObject` 引用，防止 Plasma 驱逐 | Raylet (LocalObjectManager) |
| **逻辑 Pin** | Owner 的 `pinned_at_node_id_` | 记录主 Pin 在哪个节点，节点挂了触发 Recovery | Owner (ReferenceCounter) |

两层配合工作，但**由不同的通信路径分别设置**。

---

## 9. Release 语义

### 9.1 Release 的含义

`Release` 是释放 Worker 的 Plasma 客户端对对象的本地引用：

```cpp
// plasma/client.cc
Status PlasmaClient::Release(const ObjectID &object_id) {
    auto entry = objects_in_use_.find(object_id);
    entry->second->count -= 1;          // 递减本客户端的使用计数
    if (entry->second->count == 0) {
        MarkObjectUnused(object_id);     // 从 objects_in_use_ 移除
        SendReleaseRequest(...);         // 通知 Plasma Server
    }
}
```

### 9.2 Release 操作的是层次 2（本地共享内存引用），不是层次 1（Owner 分布式引用）

### 9.3 SealExisting 中的 Release 时序

```
CoreWorker::SealExisting(object_id):
  │
  │ // 此时 Worker 的 plasma client 持有引用（Create 时获得的）
  │ // objects_in_use_[object_id].count = 1
  │
  ├─ Seal(object_id)        // 对象变为不可变
  │
  ├─ PinObjectIDs(object_id) ──→ Raylet
  │    │                          │
  │    │                        Raylet 用自己的 PlasmaClient 做 Get
  │    │                        → Raylet 的 objects_in_use_ count = 1
  │    │                        → 存到 local_object_manager_ 长期持有
  │    │
  │    ← OK（Raylet 已 Pin 住）
  │
  ├─ MaybeReplicateObject()  // 异步发起副本推送
  │
  └─ Release(object_id)      // Worker 放手
       → Worker 的 objects_in_use_ count: 1 → 0
       → 但 Raylet 的 count 仍然是 1，对象不会被驱逐
```

**为什么必须等 PinObjectIDs 回复后才 Release？**

```
危险时序（如果先 Release）:
  t0: Worker Release → objects_in_use_ 所有客户端 count = 0
  t1: Plasma Store 发现无人引用 → 对象进入 LRU
  t2: 内存紧张 → Plasma 驱逐这个对象
  t3: Raylet 的 PinObjectIDs 到达 → Get 失败 → 对象丢失！
```

Worker 和 Raylet 各自有独立的 `PlasmaClient` 实例，通过不同的连接访问同一个 Plasma Store。对象安全的前提是**至少有一个客户端持有引用**。

---

## 10. ReportObjectAdded 流程

### 10.1 完整链路

```
对象通过 Push 到达稳定节点 B
  │
  ▼
ObjectBufferPool::WriteChunk() — 最后一个 chunk 到达
  ├─ store_client_->Seal(object_id)     // 使对象不可变/可读
  └─ store_client_->Release(object_id)  // 释放 BufferPool 自己的 plasma 引用
                                         // → objects_in_use_ count = 0
                                         // → MarkObjectUnused → 对象进入 LRU
  │
  ▼ (Seal 触发回调)
ObjectManager::HandleObjectAdded()
  ├─ ReportObjectAdded(object_id, self_node_id)
  │    → 发 RPC 给 Owner: "节点 B 有这个对象了"
  │    → Owner 收到后: AddObjectLocation(obj, B) — 加入 locations 集合
  │    → 不改变 pinned_at_node_id_（仍指向可抢占节点 A）
  │
  └─ PullManager::PinNewObjectIfNeeded(object_id)
       → if active_object_pull_requests_.count(object_id) > 0:
            TryPinObject()   // 仅当有活跃的 Pull 请求时才 Pin
         else:
            什么都不做        // ← Push 到达的对象走这条路径
```

### 10.2 Push 到达的副本不会自动 Pin

| 到达方式 | 是否自动 Pin | 原因 |
|---------|-------------|------|
| `ray.get` 触发的 Pull | **是** | PullManager 有 active pull request，对象到达后调 `TryPinObject` 持有引用 |
| Phase 1 主动 Push（复制） | **否** | 没有 active pull request，`PinNewObjectIfNeeded` 直接跳过 |

所以 Phase 1 Push 到稳定节点的副本：
- plasma `objects_in_use_` count = 0（无人持有引用）
- 在 LRU 中，内存紧张时随时被驱逐
- **这就是 Phase 2 存在的意义** — 通过 `PinObjectIDs` RPC 主动 Pin 住这个副本

---

## 11. NotifyWorkerBlocked 资源释放机制

### 11.1 谁会触发这个机制

```cpp
// context.cc:385
bool WorkerContext::ShouldReleaseResourcesOnBlockingCalls() const {
  return worker_type_ != WorkerType::DRIVER    // 不是 Driver
      && !CurrentActorIsDirectCall()             // 不是 Actor
      && CurrentThreadIsMain();                  // 在主线程
}
```

| 调用者 | 触发 Block/Unblock？ | 原因 |
|--------|---------------------|------|
| Driver | **否** | Driver 不持有调度资源 |
| Actor Worker | **否** | Actor 持有生命周期资源，不能中途释放 |
| 普通 Task Worker | **是** | 仅此一种情况会触发 |

### 11.2 具体做了什么

```
ray.get 阻塞前:
  NotifyWorkerBlocked() → Raylet 侧:
    worker 持有的 CPU 资源 AddResourceInstances() → 加回可用池
    worker->MarkBlocked()
    ScheduleAndGrantLeases() → 立即尝试调度新 Task 到这个 CPU 上

ray.get 返回后:
  NotifyWorkerUnblocked() → Raylet 侧:
    SubtractResourceInstances(allow_going_negative=true) → 从可用池减回来
    worker->MarkUnblocked()
```

**只释放/回收 CPU 资源，不涉及 GPU、内存等。**

### 11.3 防止"来回释放"的保护机制

**保护 1：幂等守卫**

```cpp
// node_manager.cc:2298
void NodeManager::HandleNotifyWorkerBlocked(worker) {
  if (worker->IsBlocked()) return;   // 已经 blocked 了，不重复释放
}
```

**保护 2：`allow_going_negative` 防止无限制"借"CPU**

```
场景: Worker X 持有 1 CPU，频繁 block/unblock

t0: X 持有 1 CPU，可用 CPU = 0
t1: X block → 释放 1 CPU → 可用 CPU = 1
t2: 新 Task Y 被调度到这 1 CPU → 可用 CPU = 0
t3: X unblock → SubtractResourceInstances(allow_going_negative=true)
    → 可用 CPU = -1   ← 允许负数！
t4: 此时无法再调度新 Task（CPU < 0）
t5: Y 执行完毕 → 归还 1 CPU → 可用 CPU = 0
    此时才能调度下一个 Task
```

如果不允许负数，每次 block 都"凭空创造"1 CPU，反复 block/unblock 就能无限多开 Task。允许负数后，最多**只能多借 1 个 CPU slot**。

**保护 3：Task 结束时的清理**

```cpp
// local_lease_manager.cc:1058
ReleaseWorkerResources(worker):
  if (worker->IsBlocked()) {
    // CPU 已经释放过了，从 allocated_instances 中清掉，避免双重释放
    allocated_instances->Remove(cpu_resource_ids);
  }
```

### 11.4 存在的问题：资源广播开销

每次 block/unblock 都会触发 `version_++`，通过 ray_syncer 广播到集群所有节点。如果大量 Task Worker 频繁调 `ray.get`，会产生显著的资源广播流量。但 Actor Worker 和 Driver 不受影响（不触发这个机制）。

### 11.5 设计目的

当 Worker 阻塞在 `ray.get` 等待远程对象时，它不做有用工作。释放 CPU 允许其他 Task 利用这个 CPU slot，避免资源浪费和潜在的死锁（所有 Worker 都阻塞在 `ray.get`，而被等待的 Task 需要 Worker 才能执行）。

---

## 12. Phase 1 对象副本推送同步/异步分析

### 12.1 全链路异步，fire-and-forget

```
CoreWorker::SealExisting()
  → PinObjectIDs 回调中:
      MaybeReplicateObject(object_id)        ← 异步 RPC，不等结果
      Release(object_id)                      ← 立即执行，不等复制完成
```

```cpp
// core_worker.cc:1243
void CoreWorker::MaybeReplicateObject(const ObjectID &object_id) {
  if (!is_preemptible_node_) return;
  // ...
  local_raylet_rpc_client_->ReplicateObject(   // ← 异步 RPC
      object_id,
      [object_id](const Status &status, const rpc::ReplicateObjectReply &reply) {
        // 只记日志，不做任何阻塞操作
      });
  // ← 立即返回，不等 RPC 回复
}
```

Raylet 侧也是异步：

```cpp
// node_manager.cc:3637
void NodeManager::HandleReplicateObject(...) {
  // ... 各种检查 ...
  object_manager_.Push(object_id, target_node);   // ← 异步 fire-and-forget
  replications_in_flight_.fetch_sub(1);            // ← 立即递减，不等 Push 完成
  reply->set_accepted(true);
  send_reply_callback(Status::OK(), ...);          // ← 立即回复 Worker
}
```

### 12.2 完整时序

```
Worker (可抢占节点 A)          Raylet A              ObjectManager           Raylet B
       │                        │                       │                      │
  MaybeReplicateObject()        │                       │                      │
  ──ReplicateObject RPC──→      │                       │                      │
       │                    检查 strategy/size/并发       │                      │
       │                    SelectStableNode → B         │                      │
       │                    Push(obj, B) ──────────→     │                      │
  ←── accepted=true ────        │                    异步发送 chunks ──────→     │
       │                        │                       │                 接收 + Seal
  Release(object_id)            │                       │                      │
       │                        │                       │                      │
  ★ Worker 继续执行             │                       │                      │
  ★ 不等 Push 是否完成          │                       │                      │
       │                        │                       │                 ReportObjectAdded
       │                        │                       │                   → 通知 Owner
```

### 12.3 设计意图

Phase 1 是 best-effort 的。Worker 的关键路径（SealExisting → Release → 继续执行下一个 Task）不应被网络传输阻塞。如果复制失败，原始 Pin 仍在可抢占节点，Recovery 机制兜底。

---

## 13. Owner 的 pinned_at_node_id_ 修改时机

### 13.1 核心问题

Owner 的 `ReferenceCounter::Reference` 中的 `pinned_at_node_id_` 在三个场景下会被修改，但**修改的触发时机和路径各不相同**。

### 13.2 三种场景的修改路径

#### 场景 A：对象创建（Task 执行完成）

```
Worker（可抢占节点 A）                    Owner（稳定节点 D）
    │                                       │
    │ SealExisting → PinObjectIDs → 本地 Raylet
    │   → Pin 成功（Raylet 持有 RayObject）
    │                                       │
    │ Task 执行完毕                          │
    │ 返回 Task Reply ──────────────────→    │
    │   reply 中包含:                        │
    │     object_id                          │
    │     pinned_at_node_id = A              │
    │                                       │
    │                                   TaskManager::CompletePendingTask()
    │                                     → reference_counter_->UpdateObjectPinnedAtRaylet(
    │                                           object_id, node_A)
    │                                       │
    │                                   ★ pinned_at_node_id_ = A
```

**关键点**：`pinned_at_node_id_` 的设置**不在 `PinObjectIDs` 的回调中**，而是在**完全独立的 Task Reply 路径**中。这是因为：
- Worker 调 `PinObjectIDs` 到本地 Raylet，回调在 **Worker** 上（不是 Owner）
- Owner 通过 Task Reply 才知道 Pin 在哪个节点
- 两者是不同的通信路径

**代码位置**：`task_manager.cc:566`

```cpp
void TaskManager::CompletePendingTask(const TaskID &task_id, ...) {
    // ...
    for (const auto &return_object : reply.return_objects()) {
        if (return_object.in_plasma()) {
            reference_counter_->UpdateObjectPinnedAtRaylet(
                ObjectID::FromBinary(return_object.object_id()),
                NodeID::FromBinary(return_object.pinned_at_raylet_id()));
        }
    }
}
```

#### 场景 B：Recovery（Owner 发起的 PinExistingObjectCopy）

```
Owner（节点 D）                          远程 Raylet（节点 B）
    │                                       │
    │ RecoverObject(object_id)              │
    │   → 查到 B 有副本                     │
    │   → PinExistingObjectCopy(obj, B)     │
    │                                       │
    │ PinObjectIDs RPC ─────────────────→   │
    │                                   HandlePinObjectIDs
    │                                     → Pin 成功
    │ ←── reply(success=true) ──────────    │
    │                                       │
    │ 回调中（在 Owner 上执行）:              │
    │   reference_counter_->UpdateObjectPinnedAtRaylet(object_id, B)
    │   memory_store_.Put(OBJECT_IN_PLASMA)  // 唤醒 ray.get
    │                                       │
    │ ★ pinned_at_node_id_ = B              │
```

**关键点**：Owner 自己发起 RPC → 回调在 Owner 上 → **直接在回调中修改** `pinned_at_node_id_`。

**代码位置**：`object_recovery_manager.cc:118-135`

```cpp
auto callback = [this, object_id, new_loc_id, ...](const Status &status,
                                                     const rpc::PinObjectIDsReply &reply) {
    if (status.ok() && reply.successes_size() > 0 && reply.successes(0)) {
        reference_counter_->UpdateObjectPinnedAtRaylet(object_id, new_loc_id);
        in_memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA), object_id);
    } else {
        // Recovery 失败，重试或重算
    }
};
```

#### 场景 C：Phase 2 Pin 转移（Owner 发起的 DoPinTransfer）

```
Owner（节点 D）                          稳定节点 Raylet（节点 B）
    │                                       │
    │ AddObjectLocationOwner(obj, B)        │
    │   → MaybeTriggerPinTransfer(obj, B)   │
    │     → DoPinTransfer(obj, B, addr_B)   │
    │                                       │
    │ PinObjectIDs RPC ─────────────────→   │
    │                                   HandlePinObjectIDs
    │                                     → Pin 成功
    │ ←── reply(success=true) ──────────    │
    │                                       │
    │ 回调中（在 Owner 上执行）:              │
    │   reference_counter_->UpdateObjectPinnedAtRaylet(object_id, B)
    │   // ★ 不需要 memory_store_.Put（见第 14 章）
    │                                       │
    │ ★ pinned_at_node_id_ = B（从 A 转移到 B）
```

**代码位置**：`core_worker.cc` 的 `DoPinTransfer` 回调

### 13.3 三种场景汇总

| 场景 | 谁触发修改 | 通过什么路径 | 修改时机 |
|------|-----------|-------------|---------|
| **对象创建** | Owner 的 TaskManager | Task Reply（Worker → Owner） | Task 完成时 |
| **Recovery** | Owner 的 ObjectRecoveryManager | PinObjectIDs 回调 | RPC 成功回复时 |
| **Pin 转移** | Owner 的 CoreWorker | PinObjectIDs 回调 | RPC 成功回复时 |

### 13.4 UpdateObjectPinnedAtRaylet 内部逻辑

```cpp
// reference_counter.cc:920
bool ReferenceCounter::UpdateObjectPinnedAtRaylet(
    const ObjectID &object_id, const NodeID &raylet_id) {

    absl::MutexLock lock(&mutex_);
    auto it = object_id_refs_.find(object_id);
    if (it == object_id_refs_.end()) return false;

    // 检查是否已被 freed（用户调了 ray.cancel 或 del）
    if (freed_objects_.count(object_id)) {
        // 对象已被释放，不更新 Pin
        return false;
    }

    auto &ref = it->second;
    if (ref.pinned_at_node_id_.has_value()) {
        // 已经有 Pin 了（对象创建时总是走这里，因为 Task Reply 前
        // Worker 已经发了 PinObjectIDs 并通过 Task Reply 设置了）
        // Recovery 和 Pin Transfer 走这条分支时，是覆盖旧的 pinned_at
        RAY_LOG(DEBUG) << "Object " << object_id
                       << " already pinned at " << *ref.pinned_at_node_id_
                       << ", updating to " << raylet_id
                       << ". This can happen during reconstruction or "
                       << "pin transfer from preemptible node";
    }

    ref.pinned_at_node_id_ = raylet_id;     // ← 核心：更新 pinned 节点
    ref.object_size = ...;
    PushToLocationSubscribers(it);           // 通知订阅者位置变更
    return true;
}
```

---

## 14. Pin 转移 vs Recovery 的 memory_store_.Put 差异

### 14.1 核心区别

| 操作 | 是否需要 `memory_store_.Put` | 原因 |
|------|---------------------------|------|
| **Recovery** | **需要** | 因为之前 `memory_store_.Delete` 删除了 OBJECT_IN_PLASMA 哨兵 |
| **Pin 转移** | **不需要** | OBJECT_IN_PLASMA 哨兵从未被删除，对象始终"可用" |

### 14.2 Recovery 为什么需要 memory_store_.Put

Recovery 的完整流程中有一个关键步骤：**在发起恢复前先删除 memory store 中的哨兵**。

```
节点 A 死亡
  │
  ▼
ResetObjectsOnRemovedNode(A)
  → objects_to_recover_.push_back(object_id)
  │
  ▼ (100ms 定时任务)
FlushObjectsToRecover()
  │
  ├── memory_store_.Delete(lost_objects)     ← ★ 删除了 OBJECT_IN_PLASMA 哨兵
  │     此时如果有 ray.get 在等待:
  │       - 已拿到哨兵的: 正在 Plasma 层轮询，但 Plasma 中对象已不可用
  │       - 还没拿到哨兵的: 在 memory store 层阻塞，没有东西可以唤醒
  │
  ▼
RecoverObject(object_id)
  → PinExistingObjectCopy() 或 ReconstructObject()
    → 成功后:
        memory_store_.Put(OBJECT_IN_PLASMA)  ← ★ 重新放回哨兵，唤醒 ray.get
          → cv_.notify_all()
            → ray.get 线程被唤醒
              → 进入 Plasma 层拉取数据
```

**为什么要先 Delete？** 因为旧的哨兵指向的对象已经不存在了（节点死了），如果不删除：
- memory store 层返回哨兵
- Plasma 层尝试 Get → 对象不存在 → 一直轮询
- 需要通过 Delete + 重新 Put 来**重启整个获取流程**

### 14.3 Pin 转移为什么不需要 memory_store_.Put

Pin 转移发生时，可抢占节点**还活着**，对象数据**完好无损**：

```
Pin 转移全过程中的对象可用性:

t0: 对象在可抢占节点 A（Plasma Store 中）
    memory_store_: 有 OBJECT_IN_PLASMA 哨兵 ✅
    Plasma Store A: 对象数据完整 ✅
    pinned_at_node_id_ = A

t1: Phase 1 Push 完成，稳定节点 B 也有副本
    memory_store_: 哨兵仍在 ✅
    Plasma Store A: 数据完整 ✅
    Plasma Store B: 数据完整 ✅（但 LRU 中）
    pinned_at_node_id_ = A

t2: Pin 转移开始（MaybeTriggerPinTransfer）
    memory_store_: 哨兵仍在 ✅       ← 从未被任何人 Delete
    所有数据仍然完好
    pinned_at_node_id_ = A

t3: PinObjectIDs RPC 发到稳定节点 B
    memory_store_: 哨兵仍在 ✅
    pinned_at_node_id_ = A

t4: Pin 转移成功，回调执行
    pinned_at_node_id_ = B            ← 只改了这个
    memory_store_: 哨兵仍在 ✅        ← 不需要动

★ 整个过程中对象始终可用，ray.get 随时都能成功
★ 没有人调过 memory_store_.Delete，所以不需要 Put
```

### 14.4 对比总结

```
Recovery 路径:
  Delete(哨兵) → [对象不可用的窗口] → RecoverObject → Put(哨兵) → 对象重新可用
                   ↑                                    ↑
                   必须清除旧的                          必须放回新的

Pin 转移路径:
  [对象始终可用] → DoPinTransfer → UpdateObjectPinnedAtRaylet → [对象仍然可用]
   ↑                                                             ↑
   哨兵从未被删除                                                  不需要做任何事
```

Pin 转移的唯一作用是**改变 `pinned_at_node_id_` 的指向**——从可抢占节点 A 指向稳定节点 B。这样当 A 被回收时，Owner 不会认为 Pin 副本丢失（因为 `pinned_at_node_id_` 指向的是还活着的 B），不触发 Recovery。

---

## 15. PinObjectIDs 回调的执行位置

### 15.1 核心原则

**谁发起 RPC，回调就在谁的进程中执行。** 这是 gRPC 异步调用的基本机制——回调注册在客户端侧的事件循环中。

### 15.2 三种场景的回调执行位置

#### 场景 1：对象创建（Worker 发起）

```
Worker 进程（节点 A）                     Raylet 进程（节点 A）
    │                                       │
    │ local_raylet_rpc_client_              │
    │   ->PinObjectIDs(..., callback)       │
    │         │                             │
    │         │── RPC ──────────────→        │
    │         │                         HandlePinObjectIDs
    │         │                           → Pin 成功
    │         │←── reply ──────────         │
    │         │                             │
    │   callback 在 Worker 进程中执行       │
    │     → Release(object_id)              │
    │     → MaybeReplicateObject()          │
    │                                       │
    │   ★ 不修改 pinned_at_node_id_         │
    │     （由 Task Reply 另一条路径处理）     │
```

回调内容（`core_worker.cc` SealExisting）：

```cpp
auto callback = [this, object_id, ...](const Status &status) {
    // 在 Worker 进程中执行
    if (status.ok()) {
        MaybeReplicateObject(object_id);  // 异步发起副本推送
    }
    Release(object_id);  // 释放 Worker 的 Plasma 引用
};
local_raylet_rpc_client_->PinObjectIDs(owner_addr, {object_id}, ..., callback);
```

#### 场景 2：Recovery（Owner 发起）

```
Owner 进程（节点 D）                     远程 Raylet（节点 B）
    │                                       │
    │ raylet_client_pool_                   │
    │   ->GetOrConnectByAddress(B)          │
    │   ->PinObjectIDs(..., callback)       │
    │         │                             │
    │         │── RPC ──────────────→        │
    │         │                         HandlePinObjectIDs
    │         │                           → Pin 成功
    │         │←── reply ──────────         │
    │         │                             │
    │   callback 在 Owner 进程中执行        │
    │     → UpdateObjectPinnedAtRaylet()    │  ← 直接修改 pinned_at
    │     → memory_store_.Put(PLASMA)       │  ← 唤醒 ray.get
```

#### 场景 3：Pin 转移（Owner 发起）

```
Owner 进程（节点 D）                     远程 Raylet（节点 B）
    │                                       │
    │ raylet_client_pool_                   │
    │   ->GetOrConnectByAddress(B)          │
    │   ->PinObjectIDs(..., callback)       │
    │         │                             │
    │         │── RPC ──────────────→        │
    │         │                         HandlePinObjectIDs
    │         │                           → Pin 成功
    │         │←── reply ──────────         │
    │         │                             │
    │   callback 在 Owner 进程中执行        │
    │     → UpdateObjectPinnedAtRaylet()    │  ← 直接修改 pinned_at
    │     → pin_transfers_in_flight_.erase  │  ← 清除去重标记
    │     ★ 不需要 memory_store_.Put        │
```

### 15.3 回调位置汇总

| 场景 | RPC 发起方 | 回调执行进程 | 回调做什么 |
|------|-----------|-------------|-----------|
| **对象创建** | Worker | **Worker 进程** | Release + MaybeReplicateObject（不修改 pinned_at） |
| **Recovery** | Owner | **Owner 进程** | UpdateObjectPinnedAtRaylet + memory_store_.Put |
| **Pin 转移** | Owner | **Owner 进程** | UpdateObjectPinnedAtRaylet（不需要 Put） |

### 15.4 为什么对象创建的回调不修改 pinned_at_node_id_

对象创建时，回调在 **Worker** 上执行，而 `pinned_at_node_id_` 在 **Owner 的 ReferenceCounter** 中。Worker 进程没有 Owner 的 ReferenceCounter 实例，不可能直接修改。

因此 Ray 设计了另一条通信路径：

```
Worker                                    Owner
  │                                         │
  │ PinObjectIDs 成功（回调在 Worker 上）      │
  │                                         │
  │ Task 执行完毕                            │
  │ 发送 Task Reply ─────────────────→       │
  │   reply.return_objects[i]:               │
  │     object_id = X                        │
  │     in_plasma = true                     │
  │     pinned_at_raylet_id = A              │  ← 告诉 Owner Pin 在节点 A
  │                                         │
  │                                     TaskManager::CompletePendingTask()
  │                                       → reference_counter_
  │                                           ->UpdateObjectPinnedAtRaylet(X, A)
  │                                         │
  │                                     ★ pinned_at_node_id_ = A
```

这个两步设计是因为 Worker 和 Owner 通常不在同一个进程（Worker 执行 Task，Owner 是提交 Task 的 Driver 或 Actor），必须通过网络通信传递 Pin 信息。

---

## 16. OBJECT_IN_PLASMA 哨兵模式完整生命周期

### 16.1 哨兵是什么

`OBJECT_IN_PLASMA` 是一个特殊的 `RayObject`，不含实际数据，仅在 metadata 中存储错误类型标记。它存放在 `CoreWorkerMemoryStore`（进程内 map）中，作为**"对象数据在 Plasma 共享内存中"的指示器**。

```cpp
// 创建哨兵对象
RayObject(rpc::ErrorType::OBJECT_IN_PLASMA)
//   → data_ = nullptr
//   → metadata_ = "14"  (OBJECT_IN_PLASMA 的枚举值编码为字符串)
```

### 16.2 哨兵的作用：桥接两层 Get

`ray.get` 的两层架构依赖哨兵来决定是否进入 Plasma 层：

```
CoreWorker::GetObjects()
  │
  ├─ memory_store_->Get(object_ids)
  │     返回 results map
  │
  ├─ 检查返回的 RayObject:
  │     for (result : results) {
  │       if (result->IsInPlasmaError()) {    ← ★ 检测到哨兵
  │         plasma_object_ids.insert(id);      // 需要从 Plasma 获取
  │         result_map.erase(id);              // 不是最终结果
  │       }
  │     }
  │
  └─ plasma_store_provider_->Get(plasma_object_ids)   ← 第二层
```

**没有哨兵 → memory store 层永远阻塞**（没有东西可以唤醒 `cv_.wait()`）

**有哨兵但不是 OBJECT_IN_PLASMA → 当作最终结果返回**（如 `OWNER_DIED` 错误）

### 16.3 哨兵的设置时机：完整枚举

哨兵在**6 个位置**被设置，覆盖了对象可用性的所有入口：

#### 位置 1：Worker 侧 SealExisting（对象创建者设置）

**角色**：执行 Task 的 Worker（对象数据的创建者）

```cpp
// core_worker.cc:1020
// SealExisting 末尾，对象已 Seal 进 Plasma
memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
                   object_id,
                   reference_counter_->HasReference(object_id));
```

**时机**：对象写入 Plasma 并 Seal 之后，无论 PinObjectIDs 是否成功
**作用**：让 Worker 自身的 `ray.get`（如果有）能找到哨兵并去 Plasma 取数据

#### 位置 2：Worker 侧 SealExisting（borrowed 对象版本）

**角色**：执行 Task 的 Worker（创建 borrowed 对象时）

```cpp
// core_worker.cc:1237
// SealExisting 的 borrowed 对象路径
memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
                   object_id,
                   reference_counter_->HasReference(object_id));
```

#### 位置 3：Owner 侧 CompletePendingTask（通过 Task Reply）

**角色**：Owner（收到 Worker 的 Task 完成通知）

```cpp
// task_manager.cc:568
// Worker 通过 Task Reply 告知 Owner 对象在 Plasma 中
if (return_object.in_plasma()) {
    reference_counter_.UpdateObjectPinnedAtRaylet(object_id, worker_node_id);
    // Mark it as in plasma with a dummy object.
    in_memory_store_.Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
                         object_id,
                         reference_counter_.HasReference(object_id));
}
```

**时机**：Owner 收到 Task Reply 时
**作用**：让 Owner 进程中的 `ray.get` 能感知对象在 Plasma 中（如果 Owner 是 Driver 且调了 `ray.get`）

#### 位置 4：非 Owner 节点 FutureResolver（ObjectRef 反序列化）

**角色**：任意非 Owner 节点（收到 ObjectRef 时）

```cpp
// future_resolver.cc:112
// ProcessResolvedObject: Owner 回复对象状态后
in_memory_store_->Put(RayObject(data_buffer, metadata_buffer, inlined_refs),
                       object_id,
                       reference_counter_->HasReference(object_id));
// 如果对象在 Plasma 中（data 为空），RayObject 的 metadata 包含 OBJECT_IN_PLASMA
// 如果是小对象直接内联，则 data_buffer 有实际数据
```

**两条子路径**：

```
情况 A（快速路径）: 发送端序列化时已内联了对象状态
  → RegisterOwnershipInfoAndResolveFuture()
    → ProcessResolvedObject() 同步执行
      → memory_store_->Put(...)     ← 同步放入哨兵

情况 B（慢速路径）: 对象在发送时还没 ready
  → ResolveFutureAsync()
    → GetObjectStatus RPC → Owner
      → Owner 回复
    → ProcessResolvedObject()
      → memory_store_->Put(...)     ← 异步放入哨兵
```

#### 位置 5：Recovery 成功后（PinExistingObjectCopy 回调）

**角色**：Owner（Recovery 完成后重建可用性）

```cpp
// object_recovery_manager.cc:86, 127
// 两个位置都会放哨兵:
// 1. ReconstructObject 成功后 (line 86)
// 2. PinExistingObjectCopy 成功后 (line 127)
in_memory_store_.Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
                     object_id,
                     reference_counter_.HasReference(object_id));
```

**时机**：Recovery 完成后
**作用**：**重新**放回被 `memory_store_.Delete` 删除的哨兵，唤醒正在 `ray.get` 中等待的线程

#### 位置 6：Worker 收到 Task 参数（Plasma 对象引用）

**角色**：执行 Task 的 Worker（接收上游 Plasma 对象参数）

```cpp
// core_worker.cc:3560-3567
// Worker 准备执行 Task，处理 Plasma 对象类型的参数
// We need to put an OBJECT_IN_PLASMA error here so the subsequent call to Get()
// properly redirects to the plasma store.
if (!options_.is_local_mode) {
    memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
                       task.ArgObjectId(i),
                       reference_counter_->HasReference(task.ArgObjectId(i)));
}
```

**时机**：Worker 执行 Task 前处理参数时
**作用**：Task 参数是 Plasma 对象引用，Worker 需要 `ray.get` 这些参数，放哨兵让 Get 能正确导向 Plasma

### 16.4 哨兵的删除时机

哨兵在**3 种场景**下被删除：

#### 场景 1：Recovery 前的清理

```cpp
// core_worker.cc:481
// FlushObjectsToRecover 定时任务中，RecoverObject 之前
memory_store_->Delete(lost_objects);
// 目的: 清除旧的哨兵，因为指向的对象已不可用
// 后续: RecoverObject 成功后重新 Put
```

#### 场景 2：用户显式删除对象

```cpp
// core_worker.cc:4493
// DeleteImpl: 用户调 ray.cancel 或 del
memory_store_->Delete(object_ids);
// 紧接着放入 OBJECT_FREED 错误:
memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_FREED), object_id, ...);
// 目的: ray.get 返回 ObjectFreedError 而不是永远阻塞
```

#### 场景 3：Task 执行完毕后清理借用引用

```cpp
// core_worker.cc:3183
// Worker 执行完 Task 后，清理不再需要的借用引用
reference_counter_->PopAndClearLocalBorrowers(borrowed_ids, borrowed_refs, &deleted);
memory_store_->Delete(deleted);
// 目的: 清理 Worker 进程中的临时哨兵
```

### 16.5 哨兵生命周期全景图

```
                      ┌──────────────────────────────────────────┐
                      │        OBJECT_IN_PLASMA 哨兵生命周期       │
                      └──────────────────────────────────────────┘

╔═══════════════════════════════════════════════════════════════════════╗
║ 正常对象创建路径                                                       ║
╠═══════════════════════════════════════════════════════════════════════╣
║                                                                       ║
║  Worker A:                    Owner D:                                ║
║  SealExisting()               CompletePendingTask()                   ║
║    → Put(哨兵) ①               → Put(哨兵) ③                          ║
║    (Worker 本地用)              (Owner 本地用)                          ║
║                                                                       ║
║  Non-Owner C:                                                         ║
║  FutureResolver                                                       ║
║    → ProcessResolvedObject                                            ║
║      → Put(哨兵) ④                                                    ║
║      (C 节点本地用)                                                    ║
║                                                                       ║
║  ★ 各节点各自放哨兵，用于自己进程的 ray.get                               ║
║                                                                       ║
╠═══════════════════════════════════════════════════════════════════════╣
║ 正常回收路径                                                           ║
╠═══════════════════════════════════════════════════════════════════════╣
║                                                                       ║
║  Python: del ref → RefCount → 0                                       ║
║    → 发布 WORKER_OBJECT_EVICTION                                      ║
║    → Raylet unpin → Plasma LRU 驱逐                                   ║
║    → memory_store_ 中的哨兵随 Reference 清理自然消失                     ║
║      (引用计数归零后 DeleteReferenceInternal 清理)                       ║
║                                                                       ║
╠═══════════════════════════════════════════════════════════════════════╣
║ Recovery 路径（哨兵经历 删除 → 重建）                                    ║
╠═══════════════════════════════════════════════════════════════════════╣
║                                                                       ║
║  节点 A 死亡                                                          ║
║    → ResetObjectsOnRemovedNode()                                      ║
║      → objects_to_recover_ += object_id                               ║
║    → FlushObjectsToRecover() (100ms 定时)                             ║
║      → memory_store_.Delete(lost_objects)   ← ★ 删除哨兵              ║
║        ┌──────────────────────────────────┐                           ║
║        │  此时 ray.get 无法被唤醒           │                           ║
║        │  memory_store_ 中没有任何值        │                           ║
║        │  → cv_.wait() 持续阻塞            │                           ║
║        └──────────────────────────────────┘                           ║
║      → RecoverObject()                                                ║
║        → PinExistingObjectCopy(B) 成功                                ║
║          → memory_store_.Put(哨兵) ⑤       ← ★ 重建哨兵               ║
║            → cv_.notify_all()              ← 唤醒 ray.get             ║
║                                                                       ║
╠═══════════════════════════════════════════════════════════════════════╣
║ Pin 转移路径（哨兵不受影响）                                             ║
╠═══════════════════════════════════════════════════════════════════════╣
║                                                                       ║
║  MaybeTriggerPinTransfer()                                            ║
║    → DoPinTransfer() → PinObjectIDs RPC                               ║
║      → 成功: UpdateObjectPinnedAtRaylet()                             ║
║    ★ memory_store_ 中的哨兵从未被动过                                   ║
║    ★ 对象始终可用                                                      ║
║                                                                       ║
╚═══════════════════════════════════════════════════════════════════════╝
```

### 16.6 哨兵的 Put 是幂等的

`memory_store_->Put()` 内部检查：如果 `objects_` map 中已有该 ID 的值，Put 是 **no-op**（不覆盖）。所以多个路径同时放哨兵不会冲突。

```
// 例: Owner 同时收到 Task Reply 和 FutureResolver 回调
// 两者都调 Put(OBJECT_IN_PLASMA, object_id)
// 第二次 Put 是 no-op，不会出错
```

### 16.7 哨兵 vs 实际数据的区分

| memory_store_ 中的值 | IsInPlasmaError() | 含义 | ray.get 行为 |
|---------------------|-------------------|------|-------------|
| `RayObject(OBJECT_IN_PLASMA)` | true | 对象在 Plasma 共享内存中 | 进入第二层 Plasma Get |
| `RayObject(data, metadata)` | false | 小对象，数据直接在内存中 | 直接返回数据 |
| `RayObject(OWNER_DIED)` | false (IsException=true) | Owner 已死 | 抛出 OwnerDiedError |
| `RayObject(OBJECT_FREED)` | false (IsException=true) | 对象被显式删除 | 抛出 ObjectFreedError |
| `RayObject(OBJECT_DELETED)` | false (IsException=true) | 对象已出作用域 | 抛出 ObjectDeletedError |
| 不存在 | N/A | 对象尚未 ready 或已被 Delete | cv_.wait() 阻塞等待 |

---

## 17. HandlePinObjectIDs 的执行者：Raylet 而非 Owner/Worker

### 17.1 常见误解

容易混淆 PinObjectIDs 的三个参与者：
- **调用方**（发起 RPC 的进程）：Worker 或 Owner
- **执行方**（处理 RPC 的进程）：**Raylet**（始终是 Raylet）
- **订阅的 Owner**（Raylet 通过 PubSub 订阅的对象）：Owner 进程

### 17.2 HandlePinObjectIDs 运行在 Raylet 进程

`HandlePinObjectIDs` 是 `NodeManager`（Raylet 进程）的方法，不在 Worker 或 Owner 进程中执行：

```
任何调用方                              Raylet 进程
(Worker 或 Owner)                       (NodeManager)
    │                                       │
    │ PinObjectIDs RPC ─────────────→       │
    │                                   HandlePinObjectIDs()     ← Raylet 执行
    │                                     │
    │                                     ├─ GetObjectsFromPlasma(object_ids)
    │                                     │    → PlasmaClient::Get()
    │                                     │    → 拿到 unique_ptr<RayObject>
    │                                     │       (包含 SharedMemoryBuffer)
    │                                     │
    │                                     ├─ local_object_manager_
    │                                     │    .PinObjectsAndWaitForFree(
    │                                     │        object_ids,
    │                                     │        std::move(results),    ← RayObject 转移给 pinned_objects_
    │                                     │        owner_address,         ← 用于订阅 eviction
    │                                     │        generator_id)
    │                                     │
    │                                     └─ reply(success=true/false)
    │                                       │
    │ ←── reply ────────────────────────    │
```

### 17.3 Raylet 不关心是谁调用的

`HandlePinObjectIDs` 的逻辑完全不区分调用方是 Worker 还是 Owner：

```cpp
// node_manager.cc:2590
void NodeManager::HandlePinObjectIDs(rpc::PinObjectIDsRequest request,
                                     rpc::PinObjectIDsReply *reply,
                                     rpc::SendReplyCallback send_reply_callback) {
    // 从请求中获取 owner_address —— 无论谁发起调用，owner_address 始终指向 Owner
    auto owner_address = request.owner_address();

    // 从本地 Plasma 获取对象
    GetObjectsFromPlasma(object_ids, &results);

    // 长期持有 + 订阅 Owner 的 eviction 通知
    local_object_manager_.PinObjectsAndWaitForFree(
        object_ids, std::move(results), owner_address, generator_id);
    //                                  ^^^^^^^^^^^^^
    //                                  始终是 Owner 地址，不是调用方地址
}
```

**关键区别**：
- **请求中的 `owner_address`**：始终是 Owner 的地址（无论是 Worker 还是 Owner 发起调用）
- Raylet 订阅的是 **Owner** 的 `WORKER_OBJECT_EVICTION` 频道
- Owner 说"对象可以释放了" → Raylet 才会 unpin

### 17.4 三种场景中 Raylet 行为完全相同

| 场景 | 调用方 | Raylet 行为 |
|------|-------|------------|
| 对象创建 | Worker 调本地 Raylet | Get → Pin → 订阅 Owner 的 eviction |
| Recovery | Owner 调远程 Raylet | Get → Pin → 订阅 Owner 的 eviction |
| Pin 转移 | Owner 调远程 Raylet | Get → Pin → 订阅 Owner 的 eviction |

Raylet 做的事情完全一样——它不知道也不关心这个 Pin 请求来自什么场景。差异全在调用方的回调中。

---

## 18. 对象从创建到回收的完整生命周期

### 18.1 完整时序（含所有关键状态变化）

以下展示一个 Plasma 对象从创建到被回收的完整生命周期，标注每一步涉及的哨兵、Pin、引用计数变化：

```
阶段 1: 对象创建
═══════════════════════════════════════════════════════════════

Worker A (执行 Task)                    Raylet A              Owner D
    │                                     │                      │
    │ CreateOwnedAndIncrementLocalRef()   │                      │
    │   → Plasma Store: 分配共享内存       │                      │
    │   → Worker plasma client:           │                      │
    │     objects_in_use_[X].count = 1    │                      │
    │                                     │                      │
    │ 写入数据到共享内存缓冲区              │                      │
    │                                     │                      │
    │ SealExisting(X)                     │                      │
    │   → Plasma Store: Seal（不可变）     │                      │
    │   → PinObjectIDs(owner_addr=D, X)  │                      │
    │     ──RPC──────────────────→        │                      │
    │                                HandlePinObjectIDs         │
    │                                  Raylet plasma client:     │
    │                                  objects_in_use_[X].count=1│
    │                                  pinned_objects_[X] = obj  │
    │                                  Subscribe(EVICTION, D)    │
    │     ←──reply(success)────          │                      │
    │                                     │                      │
    │   callback:                         │                      │
    │     MaybeReplicateObject(X)         │  (Phase 1 异步推送)   │
    │     Release(X)                      │                      │
    │       → Worker plasma client:       │                      │
    │         objects_in_use_[X].count=0   │                      │
    │         MarkObjectUnused(X)          │                      │
    │       → 但 Raylet 仍持有引用         │                      │
    │         所以 Plasma 不驱逐           │                      │
    │                                     │                      │
    │   memory_store_->Put(PLASMA哨兵, X) │                      │
    │     → Worker 进程: 哨兵就位 ✅       │                      │
    │                                     │                      │
    │ Task 完成，发送 Task Reply ──────────────────────────────→  │
    │   reply: {X, in_plasma=true,        │                      │
    │           pinned_at_raylet_id=A}    │                      │
    │                                     │              CompletePendingTask()
    │                                     │                UpdateObjectPinnedAtRaylet(X,A)
    │                                     │                  → pinned_at_node_id_ = A ✅
    │                                     │                in_memory_store_.Put(PLASMA哨兵,X)
    │                                     │                  → Owner 进程: 哨兵就位 ✅


阶段 2: 对象使用（ray.get）
═══════════════════════════════════════════════════════════════

Driver D (Owner):                       Raylet D              Raylet A
    │                                     │                      │
    │ ray.get(ref_X)                     │                      │
    │   → memory_store_->Get(X)          │                      │
    │     → 找到 PLASMA 哨兵              │                      │
    │     → IsInPlasmaError() = true     │                      │
    │                                     │                      │
    │   → plasma_store_provider_->Get(X)  │                      │
    │     → AsyncGetObjects(X) ─────→    │                      │
    │                              object_manager_.Pull(X)       │
    │                                SubscribeObjectLocations    │
    │                                  → 向 Owner 订阅位置       │
    │                                  → Owner 回复: 在节点 A    │
    │                                send_pull_request(X, A) ──→│
    │                                     │                 Push(X, D)
    │                                     │ ←───── 数据到达 ─── │
    │                                  数据写入本地 Plasma        │
    │     ← store_client_->Get(X) 返回   │                      │
    │                                     │                      │
    │   → 返回对象数据 ✅                  │                      │


阶段 3: 对象回收
═══════════════════════════════════════════════════════════════

Python Driver D:                        Raylet A              Plasma Store A
    │                                     │                      │
    │ del ref_X / ref_X 离开作用域         │                      │
    │   → local_ref_count -= 1            │                      │
    │   → RefCount() == 0                 │                      │
    │     → DeleteReferenceInternal()     │                      │
    │       → OutOfScope() == true        │                      │
    │         → 发布 WORKER_OBJECT_EVICTION│                      │
    │           ──PubSub────────────→     │                      │
    │                              subscription_callback:        │
    │                                ReleaseFreedObject(X)       │
    │                                  pinned_objects_.erase(X)  │
    │                                    → RayObject 析构        │
    │                                      → SharedMemoryBuffer  │
    │                                        析构                 │
    │                                        → PlasmaClient      │
    │                                          ::Release(X)      │
    │                                          objects_in_use_   │
    │                                          [X].count: 1→0    │
    │                                          MarkObjectUnused   │
    │                                            ──────────→     │
    │                                     │                 对象进入 LRU
    │                                     │                 内存紧张时驱逐
    │                                     │                      │
    │ ★ 此时 memory_store_ 中的           │                      │
    │   哨兵也随 Reference 清理而消失      │                      │
```

### 18.2 各状态节点的完整快照

| 时刻 | Worker plasma `objects_in_use_[X]` | Raylet `pinned_objects_[X]` | Raylet plasma `objects_in_use_[X]` | Owner `pinned_at_node_id_` | memory_store_ 哨兵 |
|------|-----------------------------------|---------------------------|-----------------------------------|--------------------------|------------------|
| Seal 后、Pin 前 | count=1 | 不存在 | 不存在 | Nil | 不存在 |
| Pin 成功、Release 前 | count=1 | 存在 | count=1 | Nil (Task Reply 未到) | Worker 侧: 存在 |
| Release 后、Task Reply 前 | 已删除 | 存在 | count=1 | Nil | Worker 侧: 存在 |
| Task Reply 后 | 已删除 | 存在 | count=1 | A | 两侧都存在 |
| Phase 1 Push 后 | 已删除 | A: 存在, B: 不存在 | A: count=1, B: count=0 | A | 两侧都存在 |
| Phase 2 Pin 转移后 | 已删除 | A: 存在, B: 存在 | A: count=1, B: count=1 | **B** | 两侧都存在 |
| Python del 后 | 已删除 | 释放中... | 释放中... | 清除 | 清除 |

### 18.3 关键设计洞察

1. **哨兵是 per-process 的**：每个可能调 `ray.get` 的进程都需要自己的哨兵实例。Worker、Owner、非 Owner 节点各自独立设置。

2. **哨兵和 Pin 是解耦的**：哨兵决定 "ray.get 能否被唤醒并去 Plasma 取数据"，Pin 决定 "Plasma 中的数据是否受保护不被驱逐"。两者由不同机制管理。

3. **Recovery 是唯一需要"重建哨兵"的场景**：因为 `memory_store_.Delete` 清除了旧哨兵，必须 Put 新的。Pin 转移不涉及哨兵操作。

4. **层次 1 引用计数（Owner ReferenceCounter）驱动一切回收**：Python `del ref` → RefCount 归零 → 发布 eviction → Raylet unpin → Plasma 引用归零 → LRU 可驱逐。层次 2（Plasma `objects_in_use_`）只是被动跟随。

---

## 19. 主本与副本的生命周期管理差异

### 19.1 主本（Pinned Copy）—— Owner 主动管理

主本是 `pinned_at_node_id_` 指向的节点上由 Raylet `LocalObjectManager::pinned_objects_` 持有的那份副本。

**生命周期**：

```
创建: Worker SealExisting → PinObjectIDs → Raylet 的 pinned_objects_[X] = RayObject
      → Plasma 引用计数 > 0，EvictionPolicy 标记为 "正在访问"，LRU 不驱逐

存续: Raylet 持有 RayObject（shared_ptr<SharedMemoryBuffer>）
      → 只要 pinned_objects_ 中有这个 entry，Plasma Store 无法驱逐

删除: Owner RefCount → 0
      → 发布 WORKER_OBJECT_EVICTION 消息（PubSub）
      → Raylet 收到 subscription_callback
        → ReleaseFreedObject(object_id)
          → pinned_objects_.erase(object_id)     // 删除 RayObject
            → SharedMemoryBuffer 析构
              → PlasmaClient::Release()
                → Plasma ref_count: 1 → 0
                  → EndObjectAccess() → 进入 LRU
```

**关键点：主本的删除完全由 Owner 的 PubSub 通知驱动**，Raylet 自己不会主动释放。Owner 死了也会释放（`owner_dead_callback`）。

### 19.2 副本（Unpinned Copy / LRU Copy）—— 被动管理

副本指的是通过 Push（Phase 1 复制）、Pull（`ray.get` 拉取）等方式到达其他节点的对象副本。Phase 1 Push 到达的副本**没有 Pin 保护**（`PinNewObjectIfNeeded` 因无 active pull request 而跳过），`ref_count=0`，在 Plasma 的 LRU 队列中。

**生命周期**：

```
创建: ObjectManager::Push 数据到达远程节点
      → ObjectBufferPool::WriteChunk → Seal → Release
      → Plasma ref_count = 0，立即进入 LRU
      → ReportObjectAdded → Owner 的 locations 集合加入该节点

删除方式 1: LRU 驱逐（Plasma 内存不足）
      → EvictionPolicy::ChooseObjectsToEvict
        → 选中 ref_count=0 的对象
          → DeleteObjectInternal
            → delete_object_callback → ObjectManager::HandleObjectDeleted
              → ReportObjectRemoved → 通知 Owner
                → Owner: RemoveObjectLocation(object_id, node_id)
                  → locations.erase(node_id)

删除方式 2: Owner 主动广播删除（RefCount 归零后）
      → 主本节点 Raylet 收到 WORKER_OBJECT_EVICTION
        → ReleaseFreedObject → objects_pending_deletion_ += object_id
        → FlushFreeObjects → on_objects_freed_(objects_to_delete)
          → ObjectManager::FreeObjects(object_ids, local_only=false)
            → 本地: buffer_pool_.FreeObjects → PlasmaClient::Delete
            → 远程: 广播 FreeObjectsRequest RPC 到集群所有节点
              → 每个远程节点 ObjectManager::HandleFreeObjects
                → FreeObjects(object_ids, local_only=true)
                  → buffer_pool_.FreeObjects → PlasmaClient::Delete
                    → Plasma Store: DeleteObject → 物理删除共享内存
```

### 19.3 副本被 Worker 使用时的保护：Plasma ref_count

副本没有 Raylet 级别的 Pin（`LocalObjectManager::pinned_objects_`），但在 worker 使用期间有 **Plasma 层的 ref_count 保护**：

```
Worker（副本所在节点 B）                      Plasma Store B
    │                                            │
    │ ray.get(ref)                               │
    │   → PlasmaClient::Get(object_id)           │
    │     → SendGetRequest ──────────────────→   │
    │                                        ProcessGetRequest
    │                                          → AddToClientObjectIds
    │                                            → AddReference(object_id)
    │                                              → ref_count: 0 → 1       ← ★
    │                                              → BeginObjectAccess()
    │                                                → LRU cache 移除此对象
    │                                                → ★ LRU 驱逐选不到它
    │                                            → 返回 PlasmaObject 数据
    │     ← 收到数据                              │
    │     → 构建 SharedMemoryBuffer（mmap 指针）    │
    │                                            │
    │ ← ray.get 返回，Python 拿到数据             │
    │   result = ray.get(ref)                     │
    │   → Python 持有 numpy array 等               │
    │   → SharedMemoryBuffer 仍在 → ref_count = 1 │
    │   → ★ 使用期间不可被驱逐                     │
    │                                            │
    │ del result / result 离开 Python 作用域       │
    │   → Python GC → SharedMemoryBuffer 析构      │
    │     → PlasmaBuffer 析构                      │
    │       → PlasmaClient::Release(object_id)    │
    │         → SendReleaseRequest ──────────→    │
    │                                        ReleaseObject
    │                                          → RemoveFromClientObjectIds
    │                                            → RemoveReference(object_id)
    │                                              → ref_count: 1 → 0
    │                                              → EndObjectAccess()
    │                                                → LRU cache 重新加入
    │                                              → ★ 重新可被驱逐
```

### 19.4 主本与副本的删除流程对比

```
                    主本 (Pinned Copy)              副本 (Unpinned Copy)
════════════════════════════════════════════════════════════════════════
保护机制          Raylet pinned_objects_          无（ref_count=0，LRU 中）
                  持有 RayObject 引用             使用期间有 Plasma ref_count

能被 LRU 驱逐？   不能                            能（无人使用时随时驱逐）

Owner 感知？       pinned_at_node_id_             locations 集合中的一个元素

删除触发者         Owner (WORKER_OBJECT_EVICTION    两种：
                   PubSub 通知)                    1. Plasma LRU 自动驱逐
                                                   2. Owner 广播 FreeObjects

删除后通知 Owner？ 不需要（Owner 自己发起的）       LRU 驱逐时: ReportObjectRemoved
                                                     → RemoveObjectLocation

节点死亡影响       如果是 pinned_at 节点            不触发 Recovery
                   → 触发 Recovery                 （只从 locations 中移除）
```

### 19.5 所有删除最终都和 Owner 有关

1. **主本删除**：直接由 Owner 的 RefCount 归零触发。Owner 发布 `WORKER_OBJECT_EVICTION` → Raylet unpin → Plasma 可驱逐。

2. **副本被 LRU 驱逐**：Plasma Store 自主决定驱逐，但**会通知 Owner**（通过 `ReportObjectRemoved` → `RemoveObjectLocation`），Owner 从 `locations` 集合中移除该节点。

3. **副本被 Owner 主动删除**：主本 Raylet 收到 Owner 的 eviction 通知后，除了 unpin 主本，还会调用 `FlushFreeObjects` → `ObjectManager::FreeObjects(local_only=false)` **广播 FreeObjects RPC 到集群所有节点**，主动删除所有远程副本的 Plasma 数据。

```
Owner RefCount → 0
  │
  ├─→ PubSub: WORKER_OBJECT_EVICTION → 主本 Raylet
  │     → ReleaseFreedObject → pinned_objects_.erase     ← 主本 unpin
  │     → objects_pending_deletion_ += object_id
  │     → FlushFreeObjects → on_objects_freed_
  │       → ObjectManager::FreeObjects(local_only=false)
  │         ├─ 本地 Plasma: Delete                        ← 删除主本数据
  │         └─ 广播到所有远程节点: FreeObjectsRequest      ← 删除所有副本数据
  │              → 每个节点: PlasmaClient::Delete
  │                → Plasma Store: DeleteObject            ← 物理释放共享内存
  │
  └─→ 如果副本已被 LRU 驱逐？
       → FreeObjects 到达时对象已不存在 → PlasmaError::ObjectNonexistent → 忽略
```

完整的删除链路是：**Owner 的引用计数归零 → 通知主本 Raylet unpin → 主本 Raylet 广播删除到全集群 → 所有节点清理本地 Plasma 数据**。副本的 LRU 驱逐只是一个"提前清理"——即使副本没被 LRU 驱逐，最终也会被 Owner 触发的全集群广播删除掉。

---

## 20. 两个 pinned_objects_ 的区别：PullManager vs LocalObjectManager

### 20.1 代码中存在两个同名但完全不同的 pinned_objects_

```cpp
// 长期 Pin：LocalObjectManager（Raylet 进程内）
class LocalObjectManager {
    absl::flat_hash_map<ObjectID, std::unique_ptr<RayObject>> pinned_objects_;
    // 由 PinObjectIDs RPC → HandlePinObjectIDs → PinObjectsAndWaitForFree 创建
    // 订阅 Owner 的 WORKER_OBJECT_EVICTION
    // 生命周期跟随对象（直到 Owner 说释放或 Owner 死亡）
};

// 短期 Pin：PullManager（同在 Raylet 进程内）
class PullManager {
    absl::flat_hash_map<ObjectID, std::unique_ptr<RayObject>> pinned_objects_;
    // 由 pin_object_ 回调（GetObjectsFromPlasma）创建
    // 不订阅 Owner
    // 生命周期仅在 ray.get 期间（CancelPull 时释放）
};
```

### 20.2 详细对比

| | `LocalObjectManager::pinned_objects_` | `PullManager::pinned_objects_` |
|---|---|---|
| **所属类** | LocalObjectManager | PullManager |
| **创建方式** | `PinObjectIDs` RPC → `HandlePinObjectIDs` → `PinObjectsAndWaitForFree` | `pin_object_` 回调 → `GetObjectsFromPlasma` |
| **订阅 Owner** | **是**（WORKER_OBJECT_EVICTION） | **否** |
| **释放时机** | Owner RefCount → 0（eviction）或 Owner 死亡 | `ray.get` 完成 → `CancelPull` → `UnpinObject` |
| **生命周期** | 长期（秒 ~ 小时） | 短期（毫秒 ~ 秒） |
| **用途** | 保护主本不被 LRU 驱逐 | 保护 Pull 到达的数据在 worker Get 前不被驱逐 |
| **存在节点** | 对象数据物理存储的节点 | 发起 `ray.get` 的节点 |

### 20.3 两个 pinned_objects_ 可能在不同节点

```
节点 A (Worker 执行 Task)         节点 D (Owner/Driver)       节点 C (ray.get 调用方)
═════════════════════            ═══════════════════         ═══════════════════

Raylet A:                        CoreWorker D:               Raylet C:
  LocalObjectManager              ReferenceCounter             PullManager
    ::pinned_objects_[X]             ::pinned_at_node_id_=A      ::pinned_objects_[X]
    = RayObject(数据引用)            (逻辑记录，不持有数据)         = RayObject(数据引用)
    [长期 Pin，等 Owner 释放]        [知道 A 挂了要 Recovery]      [临时 Pin，ray.get 完就释放]

Plasma Store A:                                               Plasma Store C:
  对象数据 [被 Raylet A Pin 住]                                   对象数据 [Pull 来的副本]
  ref_count ≥ 1                                                 ref_count ≥ 1 (临时)
```

### 20.4 LocalObjectManager::pinned_objects_ 不在 Owner 节点上

**常见误解**：`LocalObjectManager::pinned_objects_` 在 Owner 节点上。

**实际情况**：它在**对象数据物理存储的那个节点**的 Raylet 上，通常**不是** Owner 节点：

```
典型场景：
  节点 A: Worker 执行 Task，创建对象 → 数据在 A 的 Plasma
  节点 D: Driver (Owner)

  → LocalObjectManager::pinned_objects_ 在节点 A（数据节点）
  → Owner(D) 上没有这个 map entry
  → Owner(D) 只有 ReferenceCounter 中的 pinned_at_node_id_ = A
```

Owner 的 `ReferenceCounter::pinned_at_node_id_` 只是一个**逻辑记录**（"主 Pin 在哪个节点"），不持有任何数据引用。实际的物理引用持有在远程 Raylet 的 `LocalObjectManager` 中。

---

## 21. ray.get 期间 PullManager 的临时 Pin 机制

### 21.1 ray.get 始终触发 PullManager Pull

无论对象是否已在本地 Plasma，`ray.get` 都会通过 `AsyncGetObjects` 通知 Raylet 发起 Pull：

```cpp
// plasma_store_provider.cc:253
Status CoreWorkerPlasmaStoreProvider::Get(...) {
    // 第一步：异步通知 Raylet（不管本地有没有，都发）
    auto status_or_cleanup = raylet_ipc_client_->AsyncGetObjects(...);
    // → Raylet → LeaseDependencyManager::StartGetRequest → PullManager::Pull
    //   → 激活时 → TryPinObject(object_id)

    // 第二步：直接尝试从本地 Plasma 取（timeout=0）
    GetObjectsFromPlasmaStore(..., /*timeout_ms=*/0, ...);

    // 第三步：如果第二步没取到，继续轮询等待...
}
```

`AsyncGetObjects` 是异步 IPC 消息，Worker 发完后不等 Raylet 处理就继续执行第二步。

### 21.2 对象已在本地时的完整时序

```
Worker 进程                              Raylet 进程（事件循环）
═════════════                            ═══════════════════════
ray.get(ref)
  │
  ├─ AsyncGetObjects(IPC 消息) ──→       (消息入队，等待处理)
  │
  ├─ store_client_->Get(timeout=0)
  │    → 对象在本地 → 立即返回数据 ✅
  │
  ├─ ray.get 返回
  │
  └─ ScopedResponse 析构
       → CancelGetRequest(IPC 消息) ──→  (消息入队)
                                         │
                                         ▼ (按顺序处理)
                                     ① 处理 AsyncGetObjects
                                        → Pull → TryPinObject
                                          → pin_object_(object_id)
                                            → GetObjectsFromPlasma → 本地有
                                              → Pin 成功 (PullManager::pinned_objects_)
                                     ② 处理 CancelGetRequest
                                        → CancelPull → DeactivateBundlePullRequest
                                          → UnpinObject
                                            → pinned_objects_.erase → Pin 释放

                                     ★ Pin 存在时间: 仅 ①②之间（微秒级）
```

### 21.3 对象不在本地时的完整时序

```
Worker 进程                              Raylet 进程
═════════════                            ═══════════════════════
ray.get(ref)
  │
  ├─ AsyncGetObjects(IPC 消息) ──→       处理 AsyncGetObjects
  │                                        → Pull → TryPinObject
  │                                          → pin_object_(object_id)
  │                                            → GetObjectsFromPlasma → 本地没有
  │                                              → 返回 nullptr → Pin 失败
  │                                        → 向 Owner 订阅位置
  │                                        → 发送 Pull 请求到远程节点
  │
  ├─ store_client_->Get(timeout)
  │    → 本地没有 → 阻塞轮询...          远程节点 Push 数据到达
  │                                        → Seal → HandleObjectAdded
  │                                          → PinNewObjectIfNeeded
  │                                            → active_object_pull_requests_ 中有
  │                                              → TryPinObject → Pin 成功 ✅
  │    → 数据出现在本地 Plasma → 返回
  │
  ├─ ray.get 返回
  │
  └─ ScopedResponse 析构
       → CancelGetRequest ──────→        CancelPull → UnpinObject → Pin 释放
```

### 21.4 两个时间点的区别

之前讨论中容易混淆的关键区别：

```
时间点 1: 对象到达本地 Plasma（HandleObjectAdded 触发时）
════════════════════════════════════════════════════════
  PinNewObjectIfNeeded 检查 active_object_pull_requests_:
    ├─ 有人在等（正在 ray.get）→ TryPinObject → Pin ✅
    └─ 没人在等（Phase 1 Push 自动到达）→ 跳过 ❌

时间点 2: 有人来 ray.get（AsyncGetObjects 到 Raylet）
════════════════════════════════════════════════════════
  PullManager::Pull → 激活 → TryPinObject:
    ├─ 本地已有 → Pin 立即成功 ✅（但 ray.get 完很快释放）
    └─ 本地没有 → Pin 失败，等数据到达后由 PinNewObjectIfNeeded Pin

★ 两个时间点之间存在无保护窗口:
  Phase 1 Push 到达 → [无 Pin，LRU 中，可被驱逐] → 某 Worker ray.get
  这个窗口可能很长（秒、分钟、甚至更久），这就是 Phase 2 要解决的问题。
```

### 21.5 PullManager Pin 的生命周期汇总

```
ray.get 发起
  │
  ├─ AsyncGetObjects → Raylet
  │    → PullManager::Pull → 激活
  │      → TryPinObject(object_id)
  │        → pin_object_ → GetObjectsFromPlasma
  │          ├─ 本地有 → RayObject → pinned_objects_[X] = RayObject  ← Pin 创建
  │          └─ 本地没有 → nullptr → 等数据到达
  │                                   → HandleObjectAdded
  │                                     → PinNewObjectIfNeeded
  │                                       → TryPinObject → Pin 创建
  │
  ├─ Worker 的 PlasmaClient::Get 拿到数据
  │
  ├─ ray.get 返回
  │
  └─ ScopedResponse 析构
       → CancelGetRequest → Raylet
         → LeaseDependencyManager::CancelGetRequest
           → PullManager::CancelPull(pull_request_id)
             → DeactivateBundlePullRequest
               → UnpinObject(object_id)
                 → pinned_objects_.erase(object_id)              ← Pin 释放
                   → RayObject 析构
                     → Plasma ref_count -= 1
```

### 21.6 PullManager Pin vs 主本 Pin 的本质区别

| | PullManager Pin (临时) | 主本 Pin (长期) |
|---|---|---|
| 创建 | `GetObjectsFromPlasma`（直接 Get） | `PinObjectIDs` RPC → `HandlePinObjectIDs` |
| 订阅 Owner | **否** | **是**（WORKER_OBJECT_EVICTION） |
| 释放触发 | `ray.get` 完成 → `CancelPull` | Owner RefCount → 0 → eviction 通知 |
| 持续时间 | 毫秒 ~ 秒 | 秒 ~ 小时 |
| 目的 | 防止 Pull 到达后、worker Get 前被 LRU 驱逐 | 保证对象不丢失（丢了要 Recovery） |
| Owner 感知 | 不感知（Owner 不知道这个 Pin 的存在） | 感知（`pinned_at_node_id_` 记录） |

---

## 22. Phase 2 Pin 转移后旧 Pin 的释放机制

### 22.1 Phase 2 完成后的状态：同一对象有两个 Pin

```
可抢占节点 A (Raylet)                Owner (节点 D)              稳定节点 B (Raylet)
═══════════════════                 ════════════                ═══════════════════

LocalObjectManager:                 ReferenceCounter:           LocalObjectManager:
  pinned_objects_[X] = RayObject      pinned_at_node_id_ = B     pinned_objects_[X] = RayObject
  订阅 Owner(D) eviction ✅           locations = {A, B}         订阅 Owner(D) eviction ✅
  ↑                                   ↑                          ↑
  旧 Pin（仍然存在）                   已改指向 B                  新 Pin（Phase 2 创建）
```

Phase 2 的 `DoPinTransfer` 只做了两件事：
1. 在稳定节点 B 创建新 Pin（通过 `PinObjectIDs` RPC）
2. 更新 Owner 的 `pinned_at_node_id_` 从 A 改为 B

**没有做第三件事**——没有通知旧节点 A 释放 Pin。这是设计决策，不是遗漏。

### 22.2 旧 Pin 的三种释放方式

#### 方式 1：Owner RefCount 归零（正常生命周期结束）

```
Python: del ref → RefCount → 0
  │
  └─ Owner 发布 WORKER_OBJECT_EVICTION
       │
       ├─ PubSub 通知 → Raylet A (旧 Pin)
       │    → subscription_callback
       │      → ReleaseFreedObject(X)
       │        → pinned_objects_.erase(X)    ← 旧 Pin 释放
       │
       └─ PubSub 通知 → Raylet B (新 Pin)
            → subscription_callback
              → ReleaseFreedObject(X)
                → pinned_objects_.erase(X)    ← 新 Pin 释放

★ 两个 Pin 同时被释放（Owner 发一条消息，两个订阅者都收到）
```

#### 方式 2：可抢占节点 A 被回收（Phase 2 的主要目标场景）

```
节点 A 死亡:
  │
  ├─ Raylet A 整个进程没了
  │    → 旧 Pin 随进程消亡（不需要显式释放）
  │
  └─ Owner 收到节点死亡通知
       → ResetObjectsOnRemovedNode(A)
         → pinned_at_node_id_ == B（不是 A）
           → ★ 不触发 Recovery（这就是 Phase 2 的意义）
         → locations.erase(A)
         → PushToLocationSubscribers: locations = {B}

  稳定节点 B 的 Pin 不受影响，对象安全 ✅
```

#### 方式 3：Owner 死亡

```
Owner(D) 死亡:
  │
  ├─ Raylet A 的 owner_dead_callback 触发
  │    → ReleaseFreedObject(X) → 旧 Pin 释放
  │
  └─ Raylet B 的 owner_dead_callback 触发
       → ReleaseFreedObject(X) → 新 Pin 释放

★ Owner 死了，对象也没意义了，两个 Pin 都释放
```

### 22.3 为什么不主动释放旧 Pin

Phase 2 设计文档中的决策：

```
| 是否释放旧 Pin？ | 不主动释放 | 最简单、最安全 |
```

**原因 1：简单性**

主动释放旧 Pin 需要额外的 RPC 和错误处理：
```
假设主动释放:
  DoPinTransfer 成功后 → 需要向旧节点 A 发送 "UnpinObject" RPC
    → 如果 A 已经挂了？→ 需要处理超时/失败
    → 如果 RPC 和节点死亡通知交叉？→ 需要处理竞态
    → 不如不做——旧 Pin 最终会自然消失
```

**原因 2：安全性**

旧 Pin 提供额外保护——万一新 Pin 因某种原因丢失：
```
极端场景:
  t0: Pin 转移完成，pinned_at = B
  t1: 稳定节点 B 意外挂了（虽然叫"稳定"，但硬件也可能故障）
  t2: ResetObjectsOnRemovedNode(B)
      → pinned_at_node_id_ 被清除 → 需要 Recovery
      → 查 locations = {A}（旧节点还活着）
      → PinExistingObjectCopy(A) → 在 A 上重新 Pin
      → ★ 旧 Pin 还在，对象数据完好，Recovery 成功

  如果旧 Pin 被主动释放了:
      → 旧节点 A 上对象可能已被 LRU 驱逐
      → PinExistingObjectCopy(A) 失败
      → 需要血缘重算 → 代价更高
```

**原因 3：代价极小**

旧 Pin 的开销：
- 一个 map entry（几十字节）
- 一个 `shared_ptr<SharedMemoryBuffer>`（指针，不是数据拷贝）
- 一个 PubSub 订阅（轻量级长连接）
- 对象数据在 Plasma 共享内存中只有一份，Pin 只是引用

**原因 4：必然会自然释放**

无论哪种情况，旧 Pin 最终都会消失：
- Owner 说对象不需要了 → eviction 通知 → 释放
- 可抢占节点被回收 → 进程消亡 → 释放
- Owner 死了 → owner_dead_callback → 释放

不需要专门的主动释放逻辑。

### 22.4 Phase 2 Pin 转移的完整时序

```
t0: 初始状态
    节点 A: pinned_objects_[X] = RayObject, 订阅 Owner(D)
    Owner:  pinned_at_node_id_ = A, locations = {A}

t1: Phase 1 Push 完成
    节点 B: Plasma 中有副本，ref_count=0，LRU 中
    Owner:  pinned_at_node_id_ = A, locations = {A, B}

t2: Owner 收到 ReportObjectAdded(B)
    → AddObjectLocationOwner(X, B)
    → MaybeTriggerPinTransfer(X, B)
      → A 是可抢占，B 是稳定 → DoPinTransfer(X, B)

t3: DoPinTransfer 发送 PinObjectIDs RPC 到 Raylet B
    Raylet B: HandlePinObjectIDs
      → GetObjectsFromPlasma → 拿到 RayObject
      → PinObjectsAndWaitForFree → pinned_objects_[X] = RayObject
      → 订阅 Owner(D) 的 WORKER_OBJECT_EVICTION
      → reply(success=true)

t4: Owner 回调
    → UpdateObjectPinnedAtRaylet(X, B)
      → pinned_at_node_id_ = B
    → pin_transfers_in_flight_.erase(X)

    ★ 此时两个 Pin 并存:
      节点 A: LocalObjectManager::pinned_objects_[X] ← 旧 Pin，仍在
      节点 B: LocalObjectManager::pinned_objects_[X] ← 新 Pin
      Owner:  pinned_at_node_id_ = B ← 已指向稳定节点

t5: (将来) 节点 A 被回收
    → Raylet A 进程消亡 → 旧 Pin 自然消失
    → Owner: pinned_at_node_id_ = B ≠ A → 不触发 Recovery ✅
    → 对象安全地保存在稳定节点 B ✅
```

---

## 关键文件索引

| 组件 | 文件 |
|------|------|
| CoreWorker Get/GetObjects | `src/ray/core_worker/core_worker.cc` (line 1495, 1548) |
| Memory Store GetImpl | `src/ray/core_worker/store_provider/memory_store/memory_store.cc` (line 259) |
| Plasma Store Provider Get | `src/ray/core_worker/store_provider/plasma_store_provider.cc` (line 253) |
| FutureResolver | `src/ray/core_worker/future_resolver.cc` (line 23, 43) |
| ReferenceCounter | `src/ray/core_worker/reference_counter.cc` |
| Object Recovery Manager | `src/ray/core_worker/object_recovery_manager.cc` (line 24) |
| NodeManager HandlePinObjectIDs | `src/ray/raylet/node_manager.cc` (line 2590) |
| LocalObjectManager PinObjectsAndWaitForFree | `src/ray/raylet/local_object_manager.cc` (line 31) |
| ObjectManager HandleObjectAdded | `src/ray/object_manager/object_manager.cc` (line 171) |
| OwnershipBasedObjectDirectory | `src/ray/object_manager/ownership_object_directory.cc` (line 320) |
| PullManager | `src/ray/object_manager/pull_manager.cc` (line 52, 362) |
| LocalLeaseManager Block/Unblock | `src/ray/raylet/scheduling/local_lease_manager.cc` (line 1090, 1119) |
| PlasmaClient Release | `src/ray/object_manager/plasma/client.cc` (line 490) |
| TaskManager CompletePendingTask | `src/ray/core_worker/task_manager.cc` (line 566) |
| UpdateObjectPinnedAtRaylet | `src/ray/core_worker/reference_counter.cc` (line 920) |
| DoPinTransfer | `src/ray/core_worker/core_worker.cc` (MaybeReplicateObject 之后) |
| PullManager TryPinObject/UnpinObject | `src/ray/object_manager/pull_manager.cc` (line 598, 625) |
| PullManager PinNewObjectIfNeeded | `src/ray/object_manager/pull_manager.cc` (line 586) |
| Plasma ObjectLifecycleManager | `src/ray/object_manager/plasma/obj_lifecycle_mgr.cc` (line 94, 128) |
| Plasma EvictionPolicy | `src/ray/object_manager/plasma/eviction_policy.cc` (line 136, 142) |
| ObjectManager FreeObjects | `src/ray/object_manager/object_manager.cc` (line 648) |
| ObjectManager HandleObjectDeleted | `src/ray/object_manager/object_manager.cc` (line 200) |
| LeaseDependencyManager CancelGetRequest | `src/ray/raylet/lease_dependency_manager.cc` (line 143) |
| LocalObjectManager ReleaseFreedObject | `src/ray/raylet/local_object_manager.cc` (line 111) |
| pin_object_ 回调定义 | `src/ray/raylet/main.cc` (line 814) |
