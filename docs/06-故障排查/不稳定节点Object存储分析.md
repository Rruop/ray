# Ray Object 存储机制深度分析：不稳定节点场景下的对象远程存储方案

## 目录

1. [问题背景](#1-问题背景)
2. [Object 存储代码逻辑深度分析](#2-object-存储代码逻辑深度分析)
   - 2.1 [两条独立的对象创建路径：Put vs SealExisting](#21-两条独立的对象创建路径put-vs-sealexisting)
   - 2.2 [Owner 语义：谁调用谁是 Owner](#22-owner-语义谁调用谁是-owner)
   - 2.3 [数据存放位置：调用方 vs 生产方](#23-数据存放位置调用方-vs-生产方)
   - 2.4 [Seal 的含义：两阶段提交](#24-seal-的含义两阶段提交)
   - 2.5 [Pin 机制详解](#25-pin-机制详解)
3. [血缘重建（Lineage Reconstruction）机制](#3-血缘重建lineage-reconstruction机制)
   - 3.1 [触发条件](#31-触发条件)
   - 3.2 [ray.put() 为何不支持血缘重建](#32-rayput-为何不支持血缘重建)
   - 3.3 [重建资格枚举](#33-重建资格枚举)
4. [对象远程存储可行性分析](#4-对象远程存储可行性分析)
   - 4.1 [方案 A：仅迁移数据，不迁移 Owner](#41-方案-a仅迁移数据不迁移-owner)
   - 4.2 [方案 B：同时迁移数据和 Owner](#42-方案-b同时迁移数据和-owner)
   - 4.3 [方案 C：利用 Object Spilling 到分布式存储](#43-方案-c利用-object-spilling-到分布式存储)
   - 4.4 [方案 D：调度层约束](#44-方案-d调度层约束)
5. [Ray Data 场景下的方案 B 简化分析](#5-ray-data-场景下的方案-b-简化分析)
   - 5.1 [Ray Data 执行模型分析](#51-ray-data-执行模型分析)
   - 5.2 [简化后的 Pin 转移方案](#52-简化后的-pin-转移方案)
6. [将结果存储到其他节点的三条路径](#6-将结果存储到其他节点的三条路径)
   - 6.1 [路径 1：强制 Inline 返回](#61-路径-1强制-inline-返回)
   - 6.2 [路径 2：Worker Seal 后 Push 到稳定节点（推荐）](#62-路径-2worker-seal-后-push-到稳定节点推荐)
   - 6.3 [路径 3：Driver 主动 Pull](#63-路径-3driver-主动-pull)
7. [路径 2 风险评估](#7-路径-2-风险评估)
8. [不稳定节点调用 task.remote() 的影响分析](#8-不稳定节点调用-taskremote-的影响分析)
   - 8.1 [问题本质](#81-问题本质)
   - 8.2 [子方案 A：转移血缘](#82-子方案-a转移血缘)
   - 8.3 [子方案 B：_owner + Push（推荐）](#83-子方案-b_owner--push推荐)
   - 8.4 [子方案 C：约束不稳定节点只执行不提交](#84-子方案-c约束不稳定节点只执行不提交)
9. [总结与建议](#9-总结与建议)
10. [关键代码文件索引](#10-关键代码文件索引)
11. [ObjectRef 引用传递机制（Borrower 协议）](#11-objectref-引用传递机制borrower-协议)
    - 11.1 [ray.put() 的引用如何传递给其他节点](#111-rayput-的引用如何传递给其他节点)
    - 11.2 [Borrower 协议的完整链路](#112-borrower-协议的完整链路)
12. [Task 内 ray.put() + return ref 的双 Ref 问题](#12-task-内-rayput--return-ref-的双-ref-问题)
    - 12.1 [两条链路的代码级完整对比](#121-两条链路的代码级完整对比)
    - 12.2 [Python 序列化层如何记录嵌套引用](#122-python-序列化层如何记录嵌套引用)
    - 12.3 [Driver 端的差异](#123-driver-端的差异)
13. [两个 Ref 之间的嵌套关系与 GC 机制](#13-两个-ref-之间的嵌套关系与-gc-机制)
    - 13.1 [NestedReferenceCount 数据结构](#131-nestedreferencecount-数据结构)
    - 13.2 [GC 级联删除](#132-gc-级联删除)
    - 13.3 [Ref 丢失时的恢复决策树](#133-ref-丢失时的恢复决策树)
    - 13.4 [链路 A vs 链路 B 丢失恢复对比](#134-链路-a-vs-链路-b-丢失恢复对比)
14. [潮汐节点是否需要考虑 Owner 迁移](#14-潮汐节点是否需要考虑-owner-迁移)
15. [潮汐节点嵌套 Task 调用的容错分析](#15-潮汐节点嵌套-task-调用的容错分析)
    - 15.1 [场景一：task2 挂了，task1 还活着](#151-场景一task2-挂了task1-还活着)
    - 15.2 [场景二：task1 也挂了](#152-场景二task1-也挂了)
    - 15.3 [场景三：task1 挂了，task2 还在跑](#153-场景三task1-挂了task2-还在跑)
16. [return ref 的 ObjectID 变化导致语义断裂问题](#16-return-ref-的-objectid-变化导致语义断裂问题)
    - 16.1 [TaskID 生成规则](#161-taskid-生成规则)
    - 16.2 [重建时 ObjectID 如何变化](#162-重建时-objectid-如何变化)
    - 16.3 [为什么 return data 没有此问题](#163-为什么-return-data-没有此问题)
17. [最终结论](#17-最终结论)
18. [Pin 发起者分析：Worker 代替 Owner 发起 PinObjectIDs](#18-pin-发起者分析worker-代替-owner-发起-pinobjectids)
    - 18.1 [SealExisting 中的 PinObjectIDs 调用](#181-sealexisting-中的-pinobjectids-调用)
    - 18.2 [Raylet 端 HandlePinObjectIDs 处理](#182-raylet-端-handlepinobjectids-处理)
    - 18.3 [Owner 在 Pin 中的角色](#183-owner-在-pin-中的角色)
19. [Plasma LRU 驱逐机制详解](#19-plasma-lru-驱逐机制详解)
    - 19.1 [ref_count 与 LRU 的联动](#191-ref_count-与-lru-的联动)
    - 19.2 [LRU 驱逐时机与选择算法](#192-lru-驱逐时机与选择算法)
    - 19.3 [被 Pin 的对象为什么不会被 LRU 驱逐](#193-被-pin-的对象为什么不会被-lru-驱逐)
20. [Object GC 完整链路与 Pin 自动回收](#20-object-gc-完整链路与-pin-自动回收)
    - 20.1 [Owner 端引用计数归零触发 GC](#201-owner-端引用计数归零触发-gc)
    - 20.2 [Raylet 端 Pin 释放（subscription_callback）](#202-raylet-端-pin-释放subscription_callback)
    - 20.3 [Owner 死亡时的回收](#203-owner-死亡时的回收)
    - 20.4 [对潮汐节点方案的影响](#204-对潮汐节点方案的影响)
21. [副本删除机制与稳定节点 Pin 建立](#21-副本删除机制与稳定节点-pin-建立)
    - 21.1 [副本数据的删除路径 A：Owner 驱动的广播删除](#211-副本数据的删除路径-aowner-驱动的广播删除)
    - 21.2 [副本数据的删除路径 B：LRU 被动驱逐](#212-副本数据的删除路径-blru-被动驱逐)
    - 21.3 [Plasma 层的删除安全保护](#213-plasma-层的删除安全保护)
    - 21.4 [Push 接收端副本的 LRU 暴露窗口](#214-push-接收端副本的-lru-暴露窗口)
    - 21.5 [稳定节点上建立主 Pin 的完整流程](#215-稳定节点上建立主-pin-的完整流程)
    - 21.6 [设计文档同步更新](#216-设计文档同步更新)

---

## 1. 问题背景

我们的集群中存在**不稳定节点资源**（可能随时被回收或故障），这些节点上执行的 Task 产生的对象（Object）存储在本地 Plasma Store 中。当不稳定节点下线时，这些对象会丢失，导致依赖这些对象的下游计算失败。

**核心诉求**：让不稳定节点将产生的对象存储到稳定节点上，从而在不稳定节点下线后，对象数据依然可用。

**关键问题**：
- Object 是否可以写入到其他节点？
- 对应的 Owner 是否也需要变更？
- 在 Ray Data 场景下，方案如何简化？

---

## 2. Object 存储代码逻辑深度分析

### 2.1 两条独立的对象创建路径：Put vs SealExisting

Ray 中有两条完全不同的对象创建路径：

#### 路径一：`ray.put()` —— 一步式创建

```
ray.put(data)
  → CoreWorker::Put()                    # core_worker.cc:966
    → 生成 ObjectID (由 put_index 决定)
    → reference_counter_->AddOwnedObject(owner=self)  # 注册自身为 Owner
    → PutInLocalPlasmaStore()            # core_worker.cc:987
      → plasma_store_provider_->Put()    # 一步完成 Create + memcpy + Seal
      → local_raylet_rpc_client_->PinObjectIDs()  # 请求本地 Raylet Pin
```

**特点**：
- Owner 一定是调用 `ray.put()` 的 Worker/Driver 本身
- 数据一定写入调用方的本地 Plasma Store
- 血缘重建资格：`INELIGIBLE_PUT`（永远不可重建）

**核心代码** (`core_worker.cc:966-985`)：
```cpp
Status CoreWorker::Put(const RayObject &object,
                       const std::vector<ObjectID> &contained_ids,
                       ObjectID *object_id) {
  *object_id = ObjectID::FromIndex(worker_context_.GetCurrentTaskID(),
                                   worker_context_.GetNextPutIndex());
  // ★ 注册自身为 Owner，eligibility = INELIGIBLE_PUT
  reference_counter_->AddOwnedObject(
      *object_id, contained_ids, rpc_address_, CurrentCallSite(),
      object.GetSize(),
      LineageReconstructionEligibility::INELIGIBLE_PUT);
  return PutInLocalPlasmaStore(object, *object_id, /*pin=*/true);
}
```

#### 路径二：Task 返回值 —— 两阶段创建

```
task.remote(args)  → Worker 执行 Task
  → AllocateReturnObject()               # core_worker.cc:2746
    → if data_size < 100KB:
        → LocalMemoryBuffer (inline 返回，数据随 gRPC Reply 走)
    → else:
        → CreateExisting()               # 在 Worker 本地 Plasma 分配 buffer
        → 状态: PLASMA_CREATED (可写，对其他人不可见)
  → 执行用户函数，将结果写入 buffer
  → SealReturnObject()                   # core_worker.cc:3055
    → SealExisting()                     # core_worker.cc:1200
      → plasma_store_provider_->Seal()   # 标记为 PLASMA_SEALED (不可变，可见)
      → local_raylet_rpc_client_->PinObjectIDs(owner_address)  # Pin 对象
```

**特点**：
- Owner 是**调用方**（提交 task.remote() 的 Worker/Driver），不是执行方
- 数据存储在**执行方**（Worker）的本地 Plasma Store
- 血缘重建资格：由 `max_retries` 决定（> 0 为 `ELIGIBLE`）

**核心代码** (`core_worker.cc:1200-1234`)：
```cpp
Status CoreWorker::SealExisting(const ObjectID &object_id,
                                bool pin_object,
                                const ObjectID &generator_id,
                                const rpc::Address &owner_address) {
  // ★ 仅执行 Seal（对象已由 CreateExisting 分配并填充）
  RAY_RETURN_NOT_OK(plasma_store_provider_->Seal(object_id));
  if (pin_object) {
    // ★ Pin 请求发往本地 Raylet，但 owner_address 指向调用方
    local_raylet_rpc_client_->PinObjectIDs(
        owner_address, {object_id},
        generator_id,
        [this, object_id](const Status &status, ...) {
          // Pin 失败时释放引用
        });
  }
  return Status::OK();
}
```

#### 两条路径的关键差异总结

| 维度 | `ray.put()` | Task 返回值 |
|------|------------|------------|
| Owner | 调用方自身 | 调用方（提交 task 的一方） |
| 数据存放 | 调用方本地 Plasma | 执行方（Worker）本地 Plasma |
| 创建方式 | Put = Create + memcpy + Seal 一步 | CreateExisting → 填充 → SealExisting 两步 |
| 血缘重建 | INELIGIBLE_PUT（不可） | ELIGIBLE（max_retries > 0 时可） |
| ObjectID 生成 | put_index | return_index |

### 2.2 Owner 语义：谁调用谁是 Owner

**核心规则：提交 Task / 调用 ray.put() 的一方是 Owner。**

Owner 的职责：
1. **位置追踪**：在 `ReferenceCounter` 中维护对象的所有已知 Plasma 位置（`locations` 集合）
2. **Pin 管理**：确保至少有一个 Raylet 持有对象的 Pin，防止被 LRU 驱逐
3. **引用计数**：跟踪本地和远程引用，当引用归零时释放对象
4. **血缘重建**：当 Pin 所在节点下线时，发起对象恢复（重新执行 Task 或 Re-pin 其他副本）
5. **命运绑定**：Owner 死亡 → 对象变为不可发现，即使数据仍在其他节点的 Plasma 中

**Owner 注册过程**：

对于 task 返回值 (`task_manager.cc:238-351`)：
```cpp
void TaskManager::AddPendingTask(const TaskSpecification &spec, ...) {
  // ★ 根据 max_retries 决定重建资格
  auto lineage_eligibility =
      (max_retries == 0)
          ? LineageReconstructionEligibility::INELIGIBLE_NO_RETRIES
          : LineageReconstructionEligibility::ELIGIBLE;

  // ★ caller_address = 提交 task 的一方 = Owner
  reference_counter_->AddOwnedObject(
      return_id, {}, caller_address, call_site,
      /*object_size=*/-1, lineage_eligibility);
}
```

### 2.3 数据存放位置：调用方 vs 生产方

数据的实际存放位置取决于**对象大小**和**创建路径**：

#### Inline 返回（< 100KB）
- 数据随 gRPC Reply 直接返回给调用方
- 存储在调用方的 in-process memory 或本地 Plasma
- 不稳定节点下线后不影响（数据已在调用方）

**判断逻辑** (`core_worker.cc:2746-2796`)：
```cpp
Status CoreWorker::AllocateReturnObject(
    const ObjectID &object_id, const size_t &data_size, ...) {
  // ★ 小于 100KB 走 inline 返回
  bool return_object_directly =
      data_size < max_direct_call_object_size;  // 默认 100KB

  if (return_object_directly) {
    // LocalMemoryBuffer → 数据会通过 gRPC 返回给调用方
    *return_object = std::make_shared<RayObject>(
        std::make_shared<LocalMemoryBuffer>(data_size), ...);
  } else {
    // ★ 大于等于 100KB → 在本地 Plasma 分配
    // 数据留在 Worker 本地，只返回引用
    CreateExisting(metadata, data_size, object_id, owner_address, ...);
  }
}
```

#### Plasma 存储（>= 100KB）
- 数据写入**执行方**的本地 Plasma Store
- 调用方只收到 `in_plasma=true` 标记和 ObjectRef
- 后续 `ray.get()` 需要通过 ObjectManager Pull 数据

**序列化逻辑** (`common.cc:57-89`)：
```cpp
void SerializeReturnObject(const ObjectID &object_id,
                           const std::shared_ptr<RayObject> &return_object,
                           rpc::ReturnObject *return_object_proto) {
  if (return_object->IsPlasmaBuffer()) {
    // ★ Plasma 对象：只标记 in_plasma，不发送数据
    return_object_proto->set_in_plasma(true);
  } else {
    // ★ Inline 对象：将完整数据放入 protobuf
    return_object_proto->set_data(data_ptr, data_size);
  }
}
```

### 2.4 Seal 的含义：两阶段提交

Seal 是 Plasma Store 的**两阶段提交协议**中的"提交"操作：

**阶段 1：Create（分配）**
- 通过 `mmap` 分配共享内存 buffer
- 对象状态为 `PLASMA_CREATED`（可写，对其他人不可见）
- 返回可写指针给 Worker

**阶段 2：Seal（提交）**
- 将状态改为 `PLASMA_SEALED`（不可变，对所有人可见）
- 触发 `add_object_callback_`（向 Owner 报告位置）
- 唤醒所有 `ray.get()` 等待者

**Create 代码** (`object_store.cc:27-60`)：
```cpp
std::pair<const ObjectInfo *, flatbuf::PlasmaError>
ObjectStore::CreateObject(const ray::ObjectID &object_id,
                         const rpc::Address &owner_address,
                         int64_t data_size, int64_t metadata_size, ...) {
  // ★ 分配共享内存
  auto allocation = allocator_.Allocate(data_size + metadata_size, ...);
  // ★ 创建 ObjectInfo，状态 = PLASMA_CREATED
  auto result = object_table_.emplace(object_id, std::move(object_info));
  // 此时对象不可被 ray.get() 发现
  return {&result.first->second, flatbuf::PlasmaError::OK};
}
```

**Seal 代码** (`object_store.cc:71-79`)：
```cpp
const ObjectInfo *ObjectStore::SealObject(const ObjectID &object_id) {
  auto it = object_table_.find(object_id);
  // ★ 状态从 CREATED → SEALED（不可变）
  it->second.state = ObjectState::PLASMA_SEALED;
  return &it->second;
}
```

**Seal 触发的后续动作** (`store.cc:275-286`)：
```cpp
void PlasmaStore::SealObjects(const std::vector<ObjectID> &object_ids) {
  for (const auto &object_id : object_ids) {
    const auto entry = object_store_.SealObject(object_id);
    // ★ 触发位置报告回调 → 向 Owner 报告 "对象在这个节点上"
    add_object_callback_(ObjectInfo(*entry));
    // ★ 唤醒所有等待这个 ObjectID 的 ray.get() 请求
    get_request_queue_.MarkObjectSealed(object_id);
  }
}
```

### 2.5 Pin 机制详解

Pin 的核心作用：**防止 Plasma Store 的 LRU 驱逐策略回收对象。**

#### Pin 的工作原理

Raylet 在 `pinned_objects_` map 中持有 `shared_ptr<RayObject>` 引用。只要引用存在，Plasma 的 `EvictObjects()` 就不会回收该对象（因为 `ref_count > 0`）。

**Pin 请求处理** (`local_object_manager.cc`)：
```cpp
void LocalObjectManager::PinObjectsAndWaitForFree(
    const std::vector<ObjectID> &object_ids,
    const rpc::Address &owner_address) {
  for (const auto &object_id : object_ids) {
    auto object = plasma_store_->GetObject(object_id);
    // ★ 持有 shared_ptr → 防止 LRU 驱逐
    pinned_objects_.emplace(object_id, std::move(object));
    // ★ 订阅 Owner 的释放通知
    // Owner 发布 WORKER_OBJECT_EVICTION 或 Owner 死亡时，释放 Pin
  }
}
```

#### Pin 的生命周期

```
Seal 完成
  → Worker 发送 PinObjectIDs RPC 给本地 Raylet
  → Raylet 持有 shared_ptr (Pin 生效)
  → Raylet 向 Owner 报告 pinned_at_node_id
  → ...
  → Owner 决定释放（引用归零 / 手动 del）
  → Owner 发布 WORKER_OBJECT_EVICTION
  → Raylet 释放 shared_ptr (Pin 解除)
  → Plasma LRU 可回收该对象
```

#### 节点下线时的 Pin 处理

当节点下线时，Owner 的 `ResetObjectsOnRemovedNode()` 被调用：

```cpp
// reference_counter.cc:893-908
void ReferenceCounter::ResetObjectsOnRemovedNode(const NodeID &node_id) {
  for (auto &[obj_id, ref] : object_id_refs_) {
    if (ref.pinned_at_node_id_ == node_id) {
      ref.pinned_at_node_id_ = NodeID::Nil();
      // ★ 将对象加入待恢复列表
      objects_to_recover_.push_back(obj_id);
    }
    // 清除该节点上的所有 location 记录
    ref.locations.erase(node_id);
  }
}
```

---

## 3. 血缘重建（Lineage Reconstruction）机制

### 3.1 触发条件

血缘重建**仅在以下条件全部满足时**才会触发：

1. **Owner 存活**：Owner 必须在线，因为血缘信息存储在 Owner 的内存中
2. **对象是 Task 返回值**：只有 task 返回值才有关联的 TaskSpec
3. **max_retries > 0**：Task 创建时指定了重试次数
4. **血缘未被驱逐**：内存压力下 `ReleaseLineageReferences()` 不会将资格标记为 `INELIGIBLE_LINEAGE_EVICTED`
5. **Pin 所在节点下线**：触发 `ResetObjectsOnRemovedNode()` → 加入 `objects_to_recover_`

**触发流程**：
```
节点下线
  → GCS 广播 NodeDeath
  → Owner 的 ResetObjectsOnRemovedNode()
    → 对象加入 objects_to_recover_
  → 定时器(100ms) FlushObjectsToRecover()
    → object_recovery_manager_->RecoverObject()
      → 检查 owned_by_us (否 → UNRECONSTRUCTABLE_BORROWED)
      → 检查 locations 是否有其他副本
        → 有 → PinExistingObjectCopy() (Re-pin)
        → 无 → ReconstructObject()
          → 检查 eligibility
            → ELIGIBLE → task_manager_.ResubmitTask() (重新执行)
            → 其他 → UNRECONSTRUCTABLE
```

### 3.2 ray.put() 为何不支持血缘重建

`ray.put()` 产生的对象在注册时被硬编码为 `INELIGIBLE_PUT`：

```cpp
// core_worker.cc:975
reference_counter_->AddOwnedObject(
    *object_id, contained_ids, rpc_address_, CurrentCallSite(),
    object.GetSize(),
    LineageReconstructionEligibility::INELIGIBLE_PUT);  // ★ 硬编码
```

原因：
- `ray.put(data)` 的 `data` 是用户在代码中临时构造的值
- 没有关联的 TaskSpec，无法通过重新执行 Task 来重建
- 即使知道是哪行代码调用了 `ray.put()`，也无法自动重新执行那段用户代码

### 3.3 重建资格枚举

```cpp
// reference_counter_interface.h:36-51
enum class LineageReconstructionEligibility {
  ELIGIBLE,                    // Task 返回值，max_retries > 0
  INELIGIBLE_PUT,             // ray.put() 产生，永远不可重建
  INELIGIBLE_NO_RETRIES,      // max_retries = 0
  INELIGIBLE_LINEAGE_EVICTED, // 内存压力下血缘被驱逐（不可逆）
  // ...
};
```

**外部 Owner（_owner 参数）的特殊情况**：

```cpp
// core_worker.cc:4439-4447 - HandleAssignObjectOwner
reference_counter_->AddOwnedObject(
    object_id, contained_object_ids, rpc_address_, call_site,
    request.object_size(),
    LineageReconstructionEligibility::INELIGIBLE_PUT,  // ★ 硬编码！
    /*add_local_ref=*/false,
    /*pinned_at_node_id=*/NodeID::FromBinary(borrower_address.node_id()));
```

即使通过 `_owner` 参数将 ownership 转移给外部 Worker，接收方也会将其标记为 `INELIGIBLE_PUT`，导致血缘重建不可能。这是方案 B 的一个重要限制。

---

## 4. 对象远程存储可行性分析

### 4.1 方案 A：仅迁移数据，不迁移 Owner

**思路**：对象仍由不稳定节点的 Worker 持有 ownership，但数据 Push 到稳定节点的 Plasma。

**优点**：
- 不涉及 Owner 变更，最小改动
- 可复用现有 ObjectManager Push 机制

**缺点**：
- **命运绑定问题未解决**：Owner（不稳定节点上的 Worker）死亡 → 所有由它 Own 的对象变为不可发现
- 即使数据在稳定节点上，没有 Owner 就无法被 `ray.get()` 定位
- 仅延缓数据丢失，不解决根本问题

**结论**：不适用于 Owner 也在不稳定节点的场景。

### 4.2 方案 B：同时迁移数据和 Owner

**思路**：将 ownership 和 数据同时迁移到稳定节点。

**通用场景下的问题**：

1. **HandleAssignObjectOwner 硬编码 INELIGIBLE_PUT**
   - 外部 Owner 接管后，血缘重建被永久禁用
   - 即使 Task 的 `max_retries > 0`，也变成不可重建

2. **血缘信息丢失**
   - TaskSpec 存储在原 Owner（调用方）的 `TaskManager.submissible_tasks_` 中
   - 转移 ownership 后，新 Owner 没有 TaskSpec，无法发起 `ResubmitTask()`

3. **没有远程 Plasma 写入通道**
   - `CreateExisting()` 只能在本地 Plasma 分配 buffer
   - 不存在 "在远程节点 Plasma 上 Create" 的 API

4. **Pin 需要远程 Raylet 配合**
   - `PinObjectIDs` 发送给本地 Raylet
   - 需要新的 RPC 或复用 ObjectManager Push 后再 Pin

5. **引用计数复杂性**
   - Owner 维护所有引用计数
   - 转移 Owner 需要原子性地迁移所有 ref count 状态

**结论**：通用场景下实现极为复杂。但在特定场景（Ray Data）下可大幅简化。

### 4.3 方案 C：利用 Object Spilling 到分布式存储

**思路**：配置 Object Spilling 到 S3/HDFS 等分布式存储系统。

**优点**：
- 完全不改 Ray 代码
- Spill/Restore 机制已经成熟

**缺点**：
- Spilling 是 LRU 驱逐触发的，不是主动的
- 恢复需要从远程存储读取，延迟高
- 不适合热数据（频繁 get 的对象）

**结论**：适合冷数据容灾，不适合低延迟需求。

### 4.4 方案 D：调度层约束

**思路**：通过调度策略避免在不稳定节点上产生需要持久化的对象。

**方式**：
- 将计算密集型 Task 调度到稳定节点
- 不稳定节点仅运行无状态的、可快速重试的 Task
- 使用 `placement_group` 或 `scheduling_strategy` 控制

**缺点**：
- 浪费不稳定节点的资源（不能存储中间结果）
- 不适合需要充分利用所有节点的场景

---

## 5. Ray Data 场景下的方案 B 简化分析

### 5.1 Ray Data 执行模型分析

在 Ray Data 的标准 map/filter/write 管道中：

#### TaskPoolMapOperator（无状态 Task）

```python
# task_pool_map_operator.py:108-143
class TaskPoolMapOperator(MapOperator):
    def _submit_data_task(self, task: MapTransformFnData):
        # ★ Driver 调用 _map_task.remote()
        # ★ Owner = Driver（稳定节点）
        ref = _map_task.options(**ray_remote_args).remote(
            task.fn, task.input
        )
```

#### ActorPoolMapOperator（Actor 方法调用）

```python
# actor_pool_map_operator.py:361-413
class ActorPoolMapOperator(MapOperator):
    def _submit_data_task(self, task: MapTransformFnData):
        # ★ Driver 调用 actor.submit.remote()
        # ★ Owner = Driver（稳定节点）
        ref = actor.submit(
            ray.remote(task.fn).options(**ray_remote_args),
            task.input
        )
```

**关键结论**：

| 角色 | 节点 | 说明 |
|------|------|------|
| Driver | 稳定节点 | 提交所有 task，是所有中间对象的 Owner |
| Worker | 不稳定节点 | 执行 task，产生的数据存在本地 Plasma |

由于 **Driver 始终在稳定节点**上，**Owner 天然是稳定的**，不需要迁移 Owner！

#### 例外情况：Hash Shuffle

```python
# hash_shuffle.py:327
# ★ Worker task 内部调用 ray.put()
# ★ Owner = Worker（不稳定节点）
partition_ref = ray.put(partition_shard)
```

Hash Shuffle 中 worker 直接调用 `ray.put()`，此时 Owner 是不稳定节点上的 Worker。这是标准 Ray Data 管道中的一个例外。

### 5.2 简化后的 Pin 转移方案

在 Ray Data 标准场景（非 Hash Shuffle）下，方案 B 简化为**仅需迁移数据**：

```
Timeline (简化的 Pin 转移):

不稳定节点 Worker              稳定节点 Raylet              Driver (Owner)
     │                              │                          │
     │ ① Seal(obj) 完成              │                          │
     │ ② ObjectManager.Push(obj) ──→│                          │
     │                              │ ③ Plasma.Create + Write   │
     │                              │ ④ Seal + Pin             │
     │                              │ ⑤ ReportObjectAdded ───→│
     │                              │                          │ ⑥ AddObjectLocation(stable_node)
     │ ⑦ Release local Pin          │                          │
     │ ⑧ 可安全下线                  │                          │ ⑦ UpdateObjectPinnedAtRaylet(stable_node)
```

**需要修改的代码**：

1. **`SealExisting()` 或 `SealReturnObject()` 后增加 Push 逻辑**：
   - 在 Seal 完成后，检查当前节点是否为不稳定节点
   - 如果是，调用 `ObjectManager::Push()` 将对象推送到目标稳定节点

2. **Owner（Driver）端 location 更新**：
   - `ReportObjectAdded` 已有现成机制
   - 新副本上线后，Owner 的 `locations` 集合会自动更新

3. **Pin 转移**：
   - 已有 `PinExistingObjectCopy()` (`object_recovery_manager.cc:109-138`) 可复用
   - 当前它用于节点下线后的 Re-pin，但逻辑完全相同

**`PinExistingObjectCopy` 已有实现**：
```cpp
// object_recovery_manager.cc:109-138
void ObjectRecoveryManager::PinExistingObjectCopy(
    const ObjectID &object_id, rpc::Address *owner_addr) {
  // ★ 从 Owner 的 locations 中随机选一个节点
  auto node_id = reference_counter_->GetRandomLocation(object_id);
  // ★ 向该节点的 Raylet 发送 PinObjectIDs RPC
  auto raylet_client = raylet_client_factory_(node_id_to_address[node_id]);
  raylet_client->PinObjectIDs(
      *owner_addr, {object_id}, ...,
      [this, object_id, node_id](const Status &status, ...) {
        if (status.ok()) {
          // ★ 更新 Owner 的 pinned_at 记录
          reference_counter_->UpdateObjectPinnedAtRaylet(object_id, node_id);
        }
      });
}
```

---

## 6. 将结果存储到其他节点的三条路径

### 6.1 路径 1：强制 Inline 返回

**思路**：增大 `max_direct_call_object_size` 阈值，让更多对象走 inline 返回。

**配置**：
```python
ray.init(_system_config={
    "max_direct_call_object_size": 10 * 1024 * 1024,  # 10MB
    "task_rpc_inlined_bytes_limit": 100 * 1024 * 1024,  # 100MB
})
```

**优点**：零代码改动，配置即生效

**缺点**：
- gRPC 内存压力大（每个 Reply 携带大量数据）
- 大对象（几十 MB 以上）不适合 inline
- `task_rpc_inlined_bytes_limit` 限制每次调用的总 inline 字节数

**适用场景**：对象普遍较小（< 几 MB）的 workload。

### 6.2 路径 2：Worker Seal 后 Push 到稳定节点（推荐）

**思路**：Worker 在 Seal 完成后，主动将对象 Push 到指定的稳定节点，然后由 Owner 将 Pin 转移到稳定节点。

**实现步骤**：

1. **Worker 端修改**（`SealExisting` 后增加逻辑）：
```cpp
// 伪代码
Status CoreWorker::SealExisting(...) {
  RAY_RETURN_NOT_OK(plasma_store_provider_->Seal(object_id));

  if (pin_object) {
    local_raylet_rpc_client_->PinObjectIDs(owner_address, {object_id}, ...);
  }

  // ★ 新增：如果当前节点是不稳定节点，Push 到稳定节点
  if (IsUnstableNode()) {
    auto target_node = SelectStableNode();
    object_manager_->Push(object_id, target_node);
  }

  return Status::OK();
}
```

2. **Push 机制已有完整实现**：
   - ObjectManager 支持分块传输（Push/Pull 协议）
   - 接收端自动 Create + Write + Seal
   - Seal 后自动触发 `ReportObjectAdded` → Owner 更新 location

3. **Owner 端 Pin 转移**：
   - Owner 收到稳定节点的 location 报告后
   - 向稳定节点 Raylet 发送 PinObjectIDs
   - 释放不稳定节点的 Pin

**优点**：
- 复用现有 ObjectManager Push 基础设施
- 改动范围可控
- 不影响 Owner / 引用计数逻辑

### 6.3 路径 3：Driver 主动 Pull

**思路**：在 Python 层面，Driver 在收到 ObjectRef 后主动拉取数据到本地。

```python
# Python 层实现
ref = task.remote(data)
# ★ 强制拉取到 Driver 本地 Plasma
local_data = ray.get(ref)
new_ref = ray.put(local_data)
# 使用 new_ref 代替 ref
```

**优点**：纯 Python 层，零 C++ 改动

**缺点**：
- 两次内存拷贝（远程 Plasma → Driver 内存 → 本地 Plasma）
- 新产生的 `new_ref` ObjectID 变了，需要修改所有下游引用
- Ray Data 的 RefBundle 管理需要适配
- `new_ref` 的 Owner 是 Driver（好），但 `INELIGIBLE_PUT`（无法重建）

---

## 7. 路径 2 风险评估

路径 2（Worker Push 后 Pin 转移）的风险评估：

### 风险等级：低 ✅

| 风险维度 | 评估 | 说明 |
|---------|------|------|
| Owner 变更 | 无 | Owner 仍是 Driver，不需要迁移 |
| 血缘重建 | 无影响 | eligibility 由 AddPendingTask 决定，不受影响 |
| ObjectManager Push | 成熟 | 已有完整的分块传输、失败重试机制 |
| Pin 转移 | 有现成实现 | `PinExistingObjectCopy` 已实现该逻辑 |
| 引用计数 | 无影响 | Push 不改变引用关系 |
| 下游 ray.get() | 无影响 | ObjectID 不变，Get 时通过 Owner 查 location |

### 需要关注的点

1. **Push 失败处理**：如果 Push 到稳定节点失败（网络/磁盘满），需要保留原始 Pin
2. **Race Condition**：Push 完成前不稳定节点下线 → 需要确保至少有一个副本 Pin 住
   - 解决：先确认 Push + Pin 成功，再释放本地 Pin
3. **目标节点选择**：需要一个策略选择哪个稳定节点（最近的 / 负载最低的）
4. **带宽消耗**：大量 Push 会增加网络负载，需要限流

### 推荐的安全实现策略

```
Worker Seal 完成
  → Push 到稳定节点 (async)
  → 本地 Pin 保持不变
  → Push 成功后，Owner 收到新 location
  → Owner 向稳定节点发送 PinObjectIDs
  → Pin 成功确认后，Owner 释放不稳定节点的 Pin
  → 不稳定节点可安全下线
```

关键：**先建新 Pin，后拆旧 Pin**，保证任何时刻至少有一个 Pin 存在。

---

## 8. 不稳定节点调用 task.remote() 的影响分析

### 8.1 问题本质

如果不稳定节点不仅执行 Task，还**提交 Task**（调用 `task.remote()`），那么：

```python
# 在不稳定节点的 Worker 上执行的 Task 内部
@ray.remote
def process(data):
    # ★ 这里的 task.remote() 调用方 = 当前 Worker（不稳定节点）
    # ★ sub_result 的 Owner = 当前 Worker
    sub_result = sub_task.remote(data)
    return ray.get(sub_result)
```

- `sub_result` 的 Owner = 不稳定节点上的 Worker
- 不稳定节点下线 → Owner 死亡 → `sub_result` 变为不可发现
- 即使 `sub_result` 的数据在稳定节点上（通过 Push），Owner 死亡后引用计数、血缘信息全部丢失

**路径 2 只解决了"数据存放"问题，未解决"Owner 在不稳定节点"问题。**

### 8.2 子方案 A：转移血缘（LineageTransfer）

**思路**：将 TaskSpec 和血缘信息从不稳定节点的 Worker 转移到稳定节点上的某个 Owner 代理。

**需要做的事**：
- 新增 `TransferOwnership` RPC
- 迁移 `Reference` 结构体（owner_address, locations, ref_count, lineage_eligibility, spilled_url, ...）
- 迁移 `TaskSpec`（包含所有参数、依赖关系）
- 通知所有 borrower（持有该 ObjectRef 的 Worker）新的 Owner 地址
- 原子性地完成以上操作

**风险**：极高
- 引用计数的分布式状态迁移需要全局一致性协议
- 任何 borrower 在迁移过程中发送的 RPC（如 WaitForRefRemoved）可能发到旧 Owner
- 涉及 `core_worker.cc`、`reference_counter.cc`、`task_manager.cc` 的大量核心代码修改

**结论**：实现复杂度太高，不推荐。

### 8.3 子方案 B：_owner + Push（推荐）

**思路**：在不稳定节点提交 Task 时，通过 `_owner` 参数将 ownership 预先指定给稳定节点上的一个 Owner 代理（如 Driver 或专用 Actor），同时数据通过 Push 到稳定节点。

**实现方式**：

```python
# 在不稳定节点执行的 Task 中
@ray.remote
def process(data, stable_owner):
    # ★ 使用 _owner 参数，将 ownership 指定给 stable_owner
    sub_result = sub_task.options(
        _owner=stable_owner
    ).remote(data)
    return ray.get(sub_result)
```

**现有 `_owner` 机制的流程**：
```
不稳定 Worker                   稳定 Owner                  执行 Worker
     │                              │                          │
     │ ① task.options(_owner=X)     │                          │
     │   .remote(data)              │                          │
     │ ② AssignObjectOwner RPC ──→│                          │
     │                              │ ③ AddOwnedObject         │
     │                              │   (INELIGIBLE_PUT! ★)    │
     │ ④ 提交 Task ───────────────────────────────────────→│
     │                              │                          │ ⑤ 执行 Task
     │                              │                          │ ⑥ Seal + Pin
     │                              │                ←─ ReportObjectAdded
     │                              │ ⑦ 更新 location          │
```

**问题**：`HandleAssignObjectOwner` 硬编码 `INELIGIBLE_PUT`，需要修改：

```cpp
// 需要修改 core_worker.cc:4439-4447
// 将 INELIGIBLE_PUT 改为从请求中读取
reference_counter_->AddOwnedObject(
    object_id, contained_object_ids, rpc_address_, call_site,
    request.object_size(),
    // ★ 改为：从请求中传递正确的 eligibility
    static_cast<LineageReconstructionEligibility>(request.lineage_eligibility()),
    /*add_local_ref=*/false,
    /*pinned_at_node_id=*/NodeID::FromBinary(borrower_address.node_id()));
```

**同时需要传递 TaskSpec**：
- 修改 `AssignObjectOwner` 请求协议，增加 TaskSpec 字段
- 接收方在 `HandleAssignObjectOwner` 中存储 TaskSpec 到 `TaskManager`

**优点**：
- `_owner` 机制已有基础框架
- 相比子方案 A，不需要运行时状态迁移
- 在提交时就确定了正确的 Owner

**缺点**：
- 需要修改 proto 协议
- 需要传递 TaskSpec（增加 RPC payload）
- 每次提交 Task 都需要额外的 `AssignObjectOwner` RPC

### 8.4 子方案 C：约束不稳定节点只执行不提交

**思路**：通过应用层约束，确保不稳定节点上的 Worker 只执行 Task，不提交新的 Task。

**实现方式**：
- Task 函数内不调用 `task.remote()` 或 `ray.put()`
- 所有 Task 提交由 Driver（稳定节点）完成
- 将需要嵌套调用的逻辑拆解为 Driver 编排的多步 pipeline

```python
# ❌ 不稳定节点上的嵌套调用
@ray.remote
def process(data):
    result = sub_task.remote(data)  # Owner = 不稳定 Worker！
    return ray.get(result)

# ✅ Driver 编排的多步 pipeline
ref1 = step1_task.remote(data)     # Owner = Driver
ref2 = step2_task.remote(ref1)     # Owner = Driver
result = ray.get(ref2)             # 安全
```

**优点**：
- 零代码改动
- 与路径 2 完美配合（Owner 始终是 Driver）

**缺点**：
- 约束了编程模型（不能嵌套调用）
- 需要重构现有使用嵌套 remote 的代码

**结论**：如果能接受编程模型约束，这是最简单安全的方案。

---

## 9. 总结与建议

### 场景决策树

```
你的场景是什么？
│
├─ Ray Data 标准 pipeline（Driver 提交所有 Task）
│  │
│  ├─ 是否有 Hash Shuffle？
│  │  ├─ 否 → ★ 路径 2（Worker Push）即可，风险低
│  │  └─ 是 → 需要额外处理 Hash Shuffle 中的 ray.put()
│  │         方案：修改 Hash Shuffle 代码，用 _owner=driver
│  │
│  └─ 对象大小？
│     ├─ 普遍 < 几 MB → 考虑路径 1（增大 inline 阈值）
│     └─ 有大对象 → 路径 2（Worker Push）
│
├─ 通用 Ray 应用（Worker 可能嵌套调用 task.remote()）
│  │
│  ├─ 可以重构为 Driver 编排？
│  │  ├─ 是 → 子方案 C + 路径 2
│  │  └─ 否 → 子方案 B（_owner + Push），需修改核心代码
│  │
│  └─ 对重建能力的需求？
│     ├─ 不需要血缘重建 → 仅用 _owner + Push 即可
│     └─ 需要血缘重建 → 需修改 HandleAssignObjectOwner + 传递 TaskSpec
│
└─ 对冷数据的容灾需求
   └─ 方案 C（Object Spilling 到分布式存储）
```

### 推荐方案优先级

1. **首选**：路径 2 + 子方案 C（约束不稳定节点只执行）
   - 适用于 Ray Data 场景
   - 风险最低，改动最小
   - Owner 天然在稳定节点上

2. **次选**：路径 2 + 子方案 B（_owner 参数 + Push）
   - 适用于需要嵌套调用的通用场景
   - 需要修改 HandleAssignObjectOwner 和 proto 协议
   - 风险中等

3. **辅助**：路径 1（增大 inline 阈值）
   - 作为路径 2 的补充，处理小对象
   - 零代码改动，配置即生效

---

## 10. 关键代码文件索引

| 文件 | 关键函数/结构 | 说明 |
|------|-------------|------|
| `src/ray/core_worker/core_worker.cc` | `Put()`, `PutInLocalPlasmaStore()`, `CreateOwnedAndIncrementLocalRef()`, `SealExisting()`, `SealReturnObject()`, `AllocateReturnObject()`, `HandleAssignObjectOwner()` | 对象创建和 Seal 的核心入口 |
| `src/ray/core_worker/reference_counter.cc` | `AddOwnedObject()`, `AddObjectLocation()`, `UpdateObjectPinnedAtRaylet()`, `ResetObjectsOnRemovedNode()`, `FlushObjectsToRecover()`, `ReleaseLineageReferences()` | 引用计数和位置追踪 |
| `src/ray/core_worker/reference_counter.h` | `Reference` struct | Owner 维护的每个对象的元数据 |
| `src/ray/core_worker/reference_counter_interface.h` | `LineageReconstructionEligibility` enum | 血缘重建资格枚举 |
| `src/ray/core_worker/object_recovery_manager.cc` | `RecoverObject()`, `PinExistingObjectCopy()`, `ReconstructObject()` | 对象恢复和 Pin 转移 |
| `src/ray/core_worker/task_manager.cc` | `AddPendingTask()`, `HandleTaskReturn()`, `ResubmitTask()` | Task 管理和血缘重建 |
| `src/ray/core_worker/common.cc` | `SerializeReturnObject()` | Inline vs Plasma 返回决策 |
| `src/ray/object_manager/ownership_object_directory.cc` | `ReportObjectAdded()`, `ReportObjectRemoved()` | 位置报告 |
| `src/ray/object_manager/plasma/store.cc` | `SealObjects()` | Seal 触发的回调链 |
| `src/ray/object_manager/plasma/object_store.cc` | `CreateObject()`, `SealObject()` | Plasma 两阶段提交 |
| `src/ray/common/ray_config_def.h` | `max_direct_call_object_size`, `task_rpc_inlined_bytes_limit` | Inline/Plasma 阈值配置 |
| `src/ray/raylet/local_object_manager.cc` | `PinObjectsAndWaitForFree()` | Pin 实现 |
| `python/ray/data/` | `task_pool_map_operator.py`, `actor_pool_map_operator.py`, `hash_shuffle.py` | Ray Data 的 Task 提交模式 |

---

---

## 11. ObjectRef 引用传递机制（Borrower 协议）

### 11.1 ray.put() 的引用如何传递给其他节点

`ray.put()` 返回的 `ObjectRef` **不是只能自己用**，可以作为参数传递给任意 Task/Actor。

```python
ref = ray.put(data)           # Owner = Driver
result = task.remote(ref)     # ★ 将 ref 传给远程 Task
```

### 11.2 Borrower 协议的完整链路

当 ObjectRef 作为 Task 参数传递时，经过以下步骤：

#### 步骤 1：Owner 端记录 Borrower

Owner 在提交 Task 时，调用 `AddBorrowerAddress()` (`reference_counter.cc:1630`) 将执行 Task 的 Worker 注册为 borrower：

```cpp
void ReferenceCounter::AddBorrowerAddress(const ObjectID &object_id,
                                          const rpc::Address &borrower_address) {
  // ★ 只有 Owner 才能添加 borrower
  RAY_CHECK(it->second.owned_by_us_);
  // 将 Worker 加入 borrowers 集合
  it->second.mutable_borrow()->borrowers.insert(borrower_address);
  // ★ 订阅 borrower 的引用释放通知
  WaitForRefRemoved(it, borrower_address);
}
```

#### 步骤 2：执行方（Worker）注册 borrowed reference

Worker 收到 Task 后，对参数中的每个 ObjectRef 调用 `AddBorrowedObject()` (`reference_counter.cc:115`)：

```cpp
bool ReferenceCounter::AddBorrowedObjectInternal(const ObjectID &object_id,
                                                  ...,
                                                  const rpc::Address &owner_address) {
  // ★ 记录 Owner 地址（不是自己）
  it->second.owner_address_ = owner_address;
  // Worker 知道去找谁查询对象位置
}
```

#### 步骤 3：Worker 通过 Owner 定位数据执行 `ray.get()`

#### 步骤 4：Task 执行完毕后，Worker 回报 borrow 信息

Task 结束时调用 `PopAndClearLocalBorrowers()` (`reference_counter.cc:1028`)，将借用情况通过 gRPC Reply 返回给调用方（Owner）。

#### 步骤 5：Borrower 也可以继续传递引用

Worker A 拿到 ref 后可以传给 Worker B，Task 结束时通过 `PopAndClearLocalBorrowers` 将这个信息逐层回报给 Owner。

#### 完整链路图

```
Driver (Owner)                Worker A                    Worker B
    │                            │                           │
    │ ref = ray.put(data)        │                           │
    │ AddOwnedObject(ref)        │                           │
    │                            │                           │
    │ task1.remote(ref) ───→    │                           │
    │ AddBorrowerAddress(A)      │                           │
    │                            │ AddBorrowedObject(ref)    │
    │                            │ ray.get(ref) → Pull data  │
    │                            │                           │
    │                            │ task2.remote(ref) ──→    │
    │                            │ (A 也可以继续传递 ref!)    │
    │                            │                           │ AddBorrowedObject(ref)
    │                            │                           │ ray.get(ref) → Pull data
    │                            │                           │
    │                            │ Task 完毕                  │
    │                ←─ PopAndClearLocalBorrowers             │
    │ (Owner 知道 B 也是 borrower)│                           │
    │ WaitForRefRemoved(B) ─────────────────────────────→   │
    │                            │                           │ Task 完毕
    │              ←────────────────── RefRemoved 通知        │
    │ 引用计数归零 → 可释放       │                           │
```

#### 与不稳定节点的关系

如果 `ray.put()` 的 Owner 在不稳定节点上，Owner 死亡后所有 borrower 都无法再定位该对象。关键问题不是"引用能不能给别人"（可以），而是"Owner 死了引用就失效了"。

---

## 12. Task 内 ray.put() + return ref 的双 Ref 问题

### 12.1 两条链路的代码级完整对比

```python
# 链路 A：直接返回数据
@ray.remote
def task1():
    data = compute()
    return data              # data 本身

# 链路 B：先 put 再返回 ref
@ray.remote
def task1():
    data = compute()
    ref = ray.put(data)      # 先 put
    return ref               # 返回 ObjectRef 对象
```

链路 B 会产生**两个完全不同的 ObjectRef**：

```
┌─────────────────────────────────────────────────────────────┐
│  outer_ref (return_id)                                      │
│  ├─ ObjectID: 由 task 的 return_index 生成                    │
│  ├─ Owner: Driver（调用 task1.remote() 的一方）               │
│  ├─ 内容: inner_ref 的序列化字节（很小，几十字节）              │
│  └─ 存放: inline 返回，在 Driver 内存中                       │
│                                                              │
│  inner_ref (put_id)                                          │
│  ├─ ObjectID: 由 Worker 的 put_index 生成                     │
│  ├─ Owner: Worker（task1 的执行节点）  ← ★ 关键区别            │
│  ├─ 内容: 实际计算数据                                        │
│  └─ 存放: Worker 本地 Plasma                                  │
└─────────────────────────────────────────────────────────────┘
```

#### 链路 A：`return data`（只有一个 ref）

```
Worker 执行 task1
  │
  ① Python 序列化 data
  │   serialized_object = serialize(data)
  │   contained_object_refs = []          # ★ data 是普通数据，不含 ObjectRef
  │
  ② AllocateReturnObject(return_id, data_size=大)    # core_worker.cc:2746
  │   if data_size < 100KB:
  │     → LocalMemoryBuffer (inline)
  │   else:
  │     → CreateExisting() → 在 Worker 本地 Plasma 分配 buffer
  │
  ③ write_to(buffer)                     # _raylet.pyx:4148
  │
  ④ SealReturnObject(return_id)          # core_worker.cc:3055
  │   → SealExisting() → Seal + Pin
  │
  ⑤ SerializeReturnObject()              # common.cc:57
  │   → in_plasma = true (大对象) 或 data=bytes (小对象)
  │   → nested_inlined_refs = []          # ★ 空！没有嵌套引用
  │
  ⑥ gRPC Reply → Driver
  │   HandleTaskReturn() → UpdateObjectPinnedAtRaylet

结果：1 个 ref (return_id)
  Owner = Driver ✅
  数据在 Worker Plasma（大对象）或 Driver 内存（小对象）
```

#### 链路 B：`ray.put(data)` + `return ref`（两个 ref）

```
Worker 执行 task1
  │
  ══════ 第一步：ray.put(data) ══════
  │
  ① CoreWorker::Put()                    # core_worker.cc:966
  │   inner_id = ObjectID::FromIndex(task_id, put_index)
  │   AddOwnedObject(inner_id, owner=self(Worker))  # ★ Owner = Worker
  │     eligibility = INELIGIBLE_PUT                  # ★ 不可重建
  │
  ② PutInLocalPlasmaStore(data, inner_id)  # core_worker.cc:987
  │   → Plasma Create + memcpy + Seal 一步完成
  │   → PinObjectIDs(inner_id) → Worker 本地 Raylet 持有 Pin
  │
  ══════ 第二步：return ref ══════
  │
  ③ Python 序列化 inner_ref
  │   pickle.dumps(inner_ref)
  │     → 触发 object_ref_reducer()       # serialization.py:206
  │       → add_contained_object_ref(inner_ref)  # ★ 记录嵌套引用
  │       → serialize_object_ref(inner_ref)
  │   serialized_object.contained_object_refs = [inner_ref]  # ★ 非空！
  │
  ④ contained_id = ObjectRefsToVector(contained_object_refs)  # _raylet.pyx:4286
  │   → [inner_id]
  │
  ⑤ AllocateReturnObject(return_id, data_size=很小)   # core_worker.cc:2746
  │   → 序列化后的 ObjectRef 只有几十字节 → LocalMemoryBuffer (inline)
  │   → AddNestedObjectIds(return_id, [inner_id])     # core_worker.cc:2766
  │
  ⑥ SerializeReturnObject()              # common.cc:57
  │   → data = 序列化的 ObjectRef 字节
  │   → nested_inlined_refs = [inner_ref]  # ★ 带着嵌套引用信息！
  │
  ⑦ gRPC Reply → Driver
  │   HandleTaskReturn() → AddNestedObjectIds(outer_id, [inner_id])

结果：2 个 ref
  outer_ref (return_id): Owner = Driver ✅, 内容=序列化的引用
  inner_ref (put_id):    Owner = Worker ⚠️, 内容=实际数据
```

### 12.2 Python 序列化层如何记录嵌套引用

当 Python 序列化一个包含 ObjectRef 的对象时，自定义 reducer 会被触发：

```python
# serialization.py:206-216
def object_ref_reducer(obj):
    # ★ 将 ObjectRef 记录到 contained_object_refs
    self.add_contained_object_ref(
        obj,
        allow_out_of_band_serialization=...,
        call_site=obj.call_site(),
    )
    obj, owner_address, object_status = worker.core_worker.serialize_object_ref(obj)
```

`add_contained_object_ref` 将 ObjectRef 添加到线程局部的集合中：

```python
# serialization.py:302-315
def add_contained_object_ref(self, object_ref, ...):
    if self.is_in_band_serialization():
        # ★ 记录：这个序列化的对象内部包含了一个 ObjectRef
        self._thread_local.object_refs.add(object_ref)
```

序列化完成后，`contained_object_refs` 被传给 C++ 层的 `AllocateReturnObject`，进而触发 `AddNestedObjectIds`。

### 12.3 Driver 端的差异

```python
# 链路 A
result_ref = task1.remote()
data = ray.get(result_ref)      # 一次 get 就拿到数据

# 链路 B
result_ref = task1.remote()
inner_ref = ray.get(result_ref) # 第一次 get：拿到的是 ObjectRef，不是数据！
data = ray.get(inner_ref)       # 第二次 get：才拿到实际数据
```

#### 关键差异总结

| 维度 | 链路 A: `return data` | 链路 B: `ray.put() + return ref` |
|------|----------------------|--------------------------------|
| `contained_object_refs` | `[]` 空 | `[inner_ref]` 非空 |
| `AllocateReturnObject` 的 data_size | 实际数据大小 | 几十字节（序列化的 ObjectRef） |
| `AddNestedObjectIds` 调用 | 不调用 | 调用，建立嵌套关系 |
| `nested_inlined_refs` | 空 | 包含 inner_ref 的 owner 信息 |
| Owner 关系 | 1 个 Owner (Driver) | 2 个 Owner (Driver + Worker) |
| 数据存放 | Worker Plasma 或 inline | **一定**在 Worker Plasma（put 的） |
| 血缘重建 | ELIGIBLE (max_retries>0) | outer=ELIGIBLE, inner=**INELIGIBLE_PUT** |
| `ray.get` 次数 | 1 次 | 2 次 |

---

## 13. 两个 Ref 之间的嵌套关系与 GC 机制

### 13.1 NestedReferenceCount 数据结构

两个 ref 在 `ReferenceCounter` 中通过 `NestedReferenceCount` 结构互相关联 (`reference_counter.h:261-282`)：

```cpp
struct NestedReferenceCount {
    // ★ 我被哪些 owned 对象包含
    absl::flat_hash_set<ObjectID> contained_in_owned;
    // ★ 我被哪些 borrowed 对象包含
    absl::flat_hash_set<ObjectID> contained_in_borrowed_ids;
    // ★ 我包含了哪些对象（反向指针）
    absl::flat_hash_set<ObjectID> contains;
};
```

在上述场景中的数据结构关系：

```
Driver 端（Owner of outer_ref）:

outer_ref (return_id):
  owned_by_us_ = true
  nested.contains = {inner_id}           ★ "我包含了 inner"

inner_ref (put_id):
  owned_by_us_ = false                   ★ Driver 不是 Owner
  owner_address_ = Worker 地址
  nested.contained_in_owned = {outer_id} ★ "我被 outer 包含"
  borrow.stored_in_objects = {outer_id → Driver地址}

Worker 端（Owner of inner_ref）:

inner_ref (put_id):
  owned_by_us_ = true                    ★ Worker 是 Owner
  lineage_eligibility_ = INELIGIBLE_PUT
  pinned_at_node_id_ = Worker 自身
  borrow.borrowers = {Driver}            ★ "Driver 在借用我"
```

关键：`contained_in_owned` 使得 outer_ref 在 scope 内时，inner_ref 的 RefCount 永远 > 0：

```cpp
// reference_counter.h:346-348
size_t RefCount() const {
  return local_ref_count + submitted_task_ref_count +
         nested().contained_in_owned.size();  // ★ 被外层 owned 对象包含也算引用
}
```

### 13.2 GC 级联删除

当 outer_ref 不再被使用时，`DeleteReferenceInternal` 会级联处理 inner_ref (`reference_counter.cc:753-772`)：

```cpp
void ReferenceCounter::DeleteReferenceInternal(ReferenceTable::iterator it, ...) {
  if (it->second.OutOfScope(lineage_pinning_enabled_)) {
    // ★ 遍历 outer 包含的所有 inner
    for (const auto &inner_id : it->second.nested().contains) {
        auto inner_it = object_id_refs_.find(inner_id);
        if (inner_it != object_id_refs_.end()) {
            if (it->second.owned_by_us_) {
                // ★ 从 inner 的 contained_in_owned 中移除 outer
                inner_it->second.mutable_nested()->contained_in_owned.erase(id);
            }
            // ★ 递归尝试删除 inner（如果 RefCount 变为 0）
            DeleteReferenceInternal(inner_it, deleted);
        }
    }
    OnObjectOutOfScopeOrFreed(it);
  }
}
```

级联关系：
```
del outer_ref (Driver 端 Python 变量出 scope)
  → outer_ref.local_ref_count = 0
  → outer_ref.RefCount() == 0
  → DeleteReferenceInternal(outer_ref)
    → inner_ref.contained_in_owned.erase(outer_id)   # inner 引用计数 -1
    → DeleteReferenceInternal(inner_ref)               # 递归检查 inner
      → 如果 inner_ref.RefCount() == 0:
        → PublishRefRemoved → 通知 Worker(Owner)
        → Worker 收到后释放 Pin 和内存
```

### 13.3 Ref 丢失时的恢复决策树

"丢失"的含义：Pin 所在节点下线，数据不在了。

```
RecoverObject(object_id)                # object_recovery_manager.cc:24-91
  │
  ├─ owned_by_us == false ?
  │    → OBJECT_UNRECONSTRUCTABLE_BORROWED     # ★ 只有 Owner 才能恢复
  │
  ├─ pinned_at 非空 或 spilled ?
  │    → 跳过（对象没丢）
  │
  └─ 确实丢了
       │
       ├─ locations 非空 ?（有其他副本）
       │    → PinExistingObjectCopy()           # Re-pin 已有副本
       │
       └─ locations 全空 ?
            → ReconstructObject()               # 重建
              │
              ├─ ELIGIBLE → ResubmitTask() 重新执行 Task
              ├─ INELIGIBLE_PUT → OBJECT_LOST ❌
              └─ INELIGIBLE_NO_RETRIES → OBJECT_UNRECONSTRUCTABLE
```

### 13.4 链路 A vs 链路 B 丢失恢复对比

#### 链路 A：`return data`（Worker 下线）

```
只有 1 个 ref：outer_ref, Owner = Driver

Worker 下线
  → Driver.ResetObjectsOnRemovedNode(worker_node)
  → RecoverObject(outer_ref)
    → owned_by_us = true ✅
    → eligibility = ELIGIBLE ✅
    → ResubmitTask(task1) → 重新执行 task1
    → 恢复成功 ✅
```

#### 链路 B：`ray.put() + return ref`（Worker 下线）

```
outer_ref: Owner = Driver   → 数据在 Driver 内存（inline），没丢
inner_ref: Owner = Worker   → Owner 已死 ❌

Driver 做 ray.get(inner_ref)
  → 向 Worker(Owner) 查 location
  → Worker 已死
  → OBJECT_UNRECONSTRUCTABLE_BORROWED ❌
  → 无法恢复
```

链路 B 是**双重不可恢复**：Owner 死了（无人发起恢复）+ 即使 Owner 活着也无法重建（INELIGIBLE_PUT）。

---

## 14. 潮汐节点是否需要考虑 Owner 迁移

### 分析

对于 `ray.put()` 产生的对象：
- 即使 Owner 在稳定节点，数据丢失后也**无法重建**（INELIGIBLE_PUT）
- Owner 在稳定节点的唯一好处是对象的**基本功能**（`ray.get`、location 查询）不会因 Owner 死亡而失效

```
           Owner 在稳定节点              Owner 在潮汐节点
           ┌─────────────┐              ┌─────────────┐
数据还在    │ ray.get ✅    │              │ ray.get ✅    │
           └─────────────┘              └─────────────┘
数据丢了    │ OBJECT_LOST ❌│              │ OBJECT_LOST ❌│
           └─────────────┘              └─────────────┘
Owner 死了  │ 不会发生      │              │ 连 ray.get   │
           │ （稳定节点）   │              │ 都做不了 ❌❌  │
           └─────────────┘              └─────────────┘
```

### 结论

既然 `ray.put()` 对象**怎样都不可重建**，花精力把 Owner 迁移到稳定节点意义不大。正确做法是**从根本上避免这条链路**：

```python
# ❌ 不要这样：产生不可恢复的 inner_ref
@ray.remote
def task1():
    result = compute()
    ref = ray.put(result)
    return ref

# ✅ 直接 return：走 SealExisting 链路
@ray.remote
def task1():
    result = compute()
    return result           # Owner = Driver, ELIGIBLE
```

真正需要关注的场景：

| 场景 | Owner | 可重建 | 策略 |
|------|-------|--------|------|
| `return data`（task 返回值） | Driver | ELIGIBLE ✅ | **核心关注**：用路径 2 Push 数据到稳定节点 |
| `ray.put()` 在 task 内部 | Worker | INELIGIBLE ❌ | **避免使用**：改为 return data |
| `ray.put()` 在 Driver 上 | Driver | INELIGIBLE ❌ | 不需关心：Owner 和数据都在稳定节点 |

---

## 15. 潮汐节点嵌套 Task 调用的容错分析

### 场景描述

```python
ref1 = task1.remote(data)        # Driver 提交，Owner(ref1) = Driver

@ray.remote(max_retries=3)
def task1(data):
    # task1 在潮汐节点 A 上执行
    ref2 = task2.remote(data)    # Owner(ref2) = task1 Worker
    result = ray.get(ref2)
    return result
```

Owner 关系链：
```
Driver ──owns──→ ref1 (task1 的返回值)
task1 的 Worker ──owns──→ ref2 (task2 的返回值)
```

### 15.1 场景一：task2 挂了，task1 还活着

```
Driver (稳定)     task1 Worker (潮汐A)     task2 Worker (潮汐B)
    │                   │                       │
    │ task1.remote() →  │ task2.remote() ──→   │ 执行中...
    │                   │                       │ ★ 潮汐B 下线！
    │                   │                       │
    │                   │ ResetObjectsOnRemovedNode(B)
    │                   │ RecoverObject(ref2)
    │                   │   owned_by_us = true ✅
    │                   │   eligibility = ELIGIBLE ✅
    │                   │   ResubmitTask(task2) → 重新调度
    │                   │              ←─────── │ task2 重新执行成功
    │            ←─────│ return result          │
    │ 正常完成 ✅        │                       │
```

**可以恢复。** task1 的 Worker 是 ref2 的 Owner，持有 task2 的 TaskSpec，可以 ResubmitTask。

### 15.2 场景二：task1 也挂了

分两种情况：

#### return ray.get(ref2)（返回数据本身）

```
Driver (稳定)       task1 Worker (潮汐A)     task2 Worker (潮汐B)
    │                     │                       │
    │                     │ ★ 潮汐A 也下线！       │
    │                     │                       │
    │ RecoverObject(ref1)                         │
    │   owned_by_us = true ✅                     │
    │   eligibility = ELIGIBLE ✅                  │
    │   ResubmitTask(task1)                       │
    │     → task1 重新调度到新节点                   │
    │                     │                       │
    │     新 task1 从头执行 │                       │
    │                     │ task2.remote() → 新 task2
    │                     │              ←─────── │ 执行成功
    │            ←───────│ return result          │
    │ 恢复成功 ✅          │                       │
```

**可以恢复。** Driver 把 task1 当作**原子单元**整体重建，task1 内部的调用链随之重新执行。

#### return ref2（返回引用）—— 有问题

```
Driver (稳定)       task1 Worker (潮汐A)
    │                     │
    │ task1 第一次执行成功   │
    │ ref1 内容 = 序列化的 ref2
    │ ref2 Owner = task1 Worker ⚠️
    │                     │
    │                     │ ★ 潮汐A 下线！
    │                     │
    │ ray.get(ref1) → 拿到 ref2 (ObjectRef)
    │ ray.get(ref2) → Owner 已死 ❌
    │ OBJECT_UNRECONSTRUCTABLE_BORROWED
```

### 15.3 场景三：task1 挂了，task2 还在跑

```
Driver (稳定)       task1 Worker (潮汐A)      task2 Worker (节点B)
    │                     │                        │
    │                     │ task2.remote() ──→     │
    │                     │ ray.get(ref2) 阻塞      │ 执行中...
    │                     │                        │
    │                     │ ★ 潮汐A 下线！           │
    │                     │                        │ (老 task2 还在跑)
    │                     │                        │
    │ RecoverObject(ref1)                          │
    │ ResubmitTask(task1) → 新节点                  │
    │                     │                        │
    │         新 task1 从头执行                       │
    │                     │ ref2' = task2.remote()  │ ← 全新的 task2
    │                     │ ray.get(ref2')          │
    │                     │              ←─────── │ 新 task2 返回
    │            ←───────│ return result           │
    │ 恢复成功 ✅          │                        │
```

老 task2 变成孤儿（Owner 已死，Pin 会释放，结果最终被 Plasma 回收），有一次计算浪费但不影响正确性。

### 总结

```
task1 在潮汐节点，嵌套调用 task2：

                        task1 还活着          task1 也挂了
                     ┌──────────────┐    ┌──────────────────┐
return ray.get(ref2) │ ✅ task1 重建   │    │ ✅ Driver 重建 task1 │
(返回数据)           │    task2       │    │    整条链路重跑     │
                     └──────────────┘    └──────────────────┘
return ref2          │ ✅ task1 重建   │    │ ❌ inner_ref Owner │
(返回引用)           │    task2       │    │    已死，不可恢复   │
                     └──────────────┘    └──────────────────┘
```

---

## 16. return ref 的 ObjectID 变化导致语义断裂问题

### 16.1 TaskID 生成规则

task2 的 TaskID 由 task1 的 `internal_task_id` 和 `task_index` 决定：

```cpp
// core_worker.cc:1990-1992
task2_id = TaskID::ForNormalTask(
    job_id,
    worker_context_->GetCurrentInternalTaskId(),  // ★ parent 的 internal task id
    next_task_index
);
```

而 `internal_task_id` 在 task 重试时会变化：

```cpp
// context.cc:70
// attempt=0: internal_id = ForExecutionAttempt(task1_id, 0) → "AAA"
// attempt=1: internal_id = ForExecutionAttempt(task1_id, 1) → "CCC"  ← 不同！
current_internal_task_id_ = TaskID::ForExecutionAttempt(task_id, attempt_number);
```

### 16.2 重建时 ObjectID 如何变化

```
═══ 第一次执行 (attempt=0) ═══

task1 internal_id = ForExecutionAttempt(task1_id, 0) = "AAA"
task2_id = ForNormalTask(job, "AAA", 1) = "BBB"
ref2 = ObjectID::FromIndex("BBB", 0) = "ref2-BBB"   ← Driver 拿到的

═══ task1 重建 (attempt=1) ═══

task1 internal_id = ForExecutionAttempt(task1_id, 1) = "CCC"  ← 变了！
task2_id = ForNormalTask(job, "CCC", 1) = "DDD"               ← 变了！
ref2' = ObjectID::FromIndex("DDD", 0) = "ref2-DDD"            ← 新的 ObjectID
```

**ResubmitTask 复用同一个 TaskSpec**（`task_manager.cc:391`），task1 本身的 TaskID 不变，只增加 AttemptNumber。但 task1 重新执行时，内部调用 `task2.remote()` 产生的子 TaskID 会因为 `ForExecutionAttempt` 的变化而不同。

### 问题的本质

```python
# Driver 端代码
ref1 = task1.remote(data)
ref2 = ray.get(ref1)          # 第一次执行成功，拿到 ref2 = "ref2-BBB"

# ... task1 节点挂了，触发重建 ...
# 重建后 ref1 的新内容 = 序列化的 ref2' = "ref2-DDD"

# 但 Python 变量 ref2 仍然是 "ref2-BBB"
ray.get(ref2)                 # ref2 = "ref2-BBB"，Owner 已死 ❌
                              # 系统不知道 "ref2-BBB" 应该被替换为 "ref2-DDD"
```

**Driver 的 Python 变量持有旧的 ObjectID，系统没有 `old_id → new_id` 的映射机制，无法自动更新。**

### 16.3 为什么 return data 没有此问题

task1 自身的 return ID 由原始 TaskID 生成（不受 AttemptNumber 影响）：

```
═══ 第一次执行 ═══
ref1 = ObjectID::FromIndex(task1_id, 0) = "ref1-XXX"

═══ task1 重建 (attempt=1) ═══
ref1 = ObjectID::FromIndex(task1_id, 0) = "ref1-XXX"  ← 同一个 ID！
```

Driver 持有的 `ref1` 始终有效，重建后新数据直接覆盖同一个 ObjectID 的内容。`ray.get(ref1)` 直接拿到重建后的数据，无需任何 ID 映射。

对比总结：

```
                return data                    return ref2
              ┌──────────────────┐          ┌──────────────────┐
Driver 持有    │ ref1 ("ref1-XXX") │          │ ref2 ("ref2-BBB") │
重建后 ID      │ 不变              │          │ 新 ref2' = "DDD"  │
ray.get       │ ray.get(ref1) ✅  │          │ ray.get(ref2) ❌  │
              │ 直接拿到新数据     │          │ 旧 ID，Owner 已死  │
              └──────────────────┘          └──────────────────┘
```

---

## 17. 最终结论

### 核心原则

在潮汐（不稳定）节点场景下，确保容错性的关键规则：

1. **Task 内始终用 `return data`，不要 `return ref`**
   - `return data` 只有一个 ref，Owner = Driver（稳定），可重建
   - `return ref` 产生嵌套双 ref，inner ref 的 Owner 在潮汐节点，不可恢复
   - 重建时 ObjectID 会变化，导致应用层引用断裂

2. **Task 内避免 `ray.put()`**
   - `ray.put()` 产生的对象 INELIGIBLE_PUT，永远不可重建
   - Owner 在潮汐节点上迁移意义不大

3. **嵌套 `task.remote()` 在 `return data` 模式下是安全的**
   - task1 调用 task2.remote()，task2 挂了 → task1(Owner) 可重建 task2
   - task1 也挂了 → Driver 重建 task1 → 整条链路从头跑

4. **数据迁移用路径 2（Worker Push）**
   - Owner 天然在稳定节点（Driver），只需解决数据存放问题
   - 复用现有 ObjectManager Push + PinExistingObjectCopy 机制
   - 先建新 Pin，后拆旧 Pin

### 场景适用性

| 场景 | 策略 | 需要代码改动 |
|------|------|------------|
| Ray Data 标准 pipeline | 路径 2（Push） | Worker 端 Seal 后增加 Push 逻辑 |
| Ray Data Hash Shuffle | 修改为 `return data` 或 `_owner=driver` | Python 层修改 |
| 通用 Ray（无嵌套调用） | 路径 2 + return data | 同上 |
| 通用 Ray（有嵌套调用） | return data + 路径 2 | 确保不 return ref |
| 通用 Ray（必须 return ref） | 需要修改核心代码 | HandleAssignObjectOwner + TaskSpec 传递 |

---

*本文档基于 Ray 2.52.1 源码分析，重点关注不稳定节点场景下的对象远程存储方案设计。*

---

## 18. Pin 发起者分析：Worker 代替 Owner 发起 PinObjectIDs

### 18.1 SealExisting 中的 PinObjectIDs 调用

一个常见的误解是 Owner 自己发起 Pin。实际上，**Pin 是由 Worker（生产者进程）发起的**，Owner 并不直接参与 Pin 的建立过程。

关键代码在 `core_worker.cc:1200-1225` 的 `SealExisting()` 中：

```cpp
// core_worker.cc:1200-1225
Status CoreWorker::SealExisting(const ObjectID &object_id,
                                bool pin_object,
                                const ObjectID &generator_id,
                                const std::unique_ptr<rpc::Address> &owner_address) {
  RAY_RETURN_NOT_OK(plasma_store_provider_->Seal(object_id));
  if (pin_object) {
    RAY_LOG(DEBUG).WithField(object_id) << "Pinning sealed object";
    // ★ Worker 调用 PinObjectIDs，但传入的是 owner_address
    local_raylet_rpc_client_->PinObjectIDs(
        owner_address != nullptr ? *owner_address : rpc_address_,
        {object_id},
        generator_id,
        [this, object_id](const Status &status, const rpc::PinObjectIDsReply &reply) {
          if (!status.ok()) {
            RAY_LOG(ERROR) << "Request to local raylet to pin object failed: "
                           << status.ToString();
            return;
          }
          // ★ Pin 确认后才 Release，避免 Pin 之前被 LRU 驱逐
          if (!plasma_store_provider_->Release(object_id).ok()) {
            RAY_LOG(ERROR).WithField(object_id)
                << "Failed to release object, might cause a leak in plasma.";
          }
        });
  } else {
    RAY_RETURN_NOT_OK(plasma_store_provider_->Release(object_id));
    reference_counter_->FreePlasmaObjects({object_id});
  }
  // ...
}
```

调用链路：

```
Worker 执行 Task → 产出 Object → Seal 写入 Plasma
  → Worker 自己调用 PinObjectIDs RPC 到本地 Raylet
     RPC 参数中携带 owner_address（Owner 的地址）
```

**关键细节**：`PinObjectIDs` 的**第一个参数**是 `owner_address`（Owner 的地址），不是 Worker 自己的地址。Raylet 需要 Owner 地址来订阅 eviction channel。

### 18.2 Raylet 端 HandlePinObjectIDs 处理

Raylet 收到 `PinObjectIDs` 后的处理（`node_manager.cc:2588-2634`）：

```
HandlePinObjectIDs(request)
  → 从 Plasma 获取 object → Get → shared_ptr<RayObject>
  → AddReference → ref_count 0→1
    → BeginObjectAccess → 从 LRU 移除 ★
  → PinObjectsAndWaitForFree(object_ids, objects, owner_address, generator_id)
```

`PinObjectsAndWaitForFree` (`local_object_manager.cc:31-109`) 做三件事：

1. **持有 shared_ptr**：`pinned_objects_[object_id] = std::move(object)` → 保持 ref_count ≥ 1 → 从 LRU 移除，不可被驱逐
2. **订阅 Owner 的 eviction channel**：用 `owner_address` 订阅 `WORKER_OBJECT_EVICTION` 频道，Owner GC 时通知释放
3. **订阅 Owner 死亡回调**：`owner_dead_callback` → Owner 挂掉也释放 Pin

```cpp
// local_object_manager.cc:67-107
// 1. 创建 eviction 订阅消息
rpc::WorkerObjectEvictionSubMessage wait_request;
wait_request.set_object_id(object_id.Binary());
wait_request.set_intended_worker_id(owner_address.worker_id());

// 2. 注册订阅回调：Owner 发布 eviction 时释放
auto subscription_callback = [this, owner_address](const rpc::PubMessage &msg) {
    const auto obj_id = ObjectID::FromBinary(
        msg.worker_object_eviction_message().object_id());
    ReleaseFreedObject(obj_id);  // ★ 释放 Pin
    core_worker_subscriber_->Unsubscribe(
        rpc::ChannelType::WORKER_OBJECT_EVICTION, owner_address, obj_id.Binary());
};

// 3. 注册 Owner 死亡回调
auto owner_dead_callback = [this, owner_address](
    const std::string &object_id_binary, const Status &) {
    const auto obj_id = ObjectID::FromBinary(object_id_binary);
    ReleaseFreedObject(obj_id);  // ★ Owner 死了也释放 Pin
};

// 4. 发起订阅
core_worker_subscriber_->Subscribe(std::move(sub_message),
                                    rpc::ChannelType::WORKER_OBJECT_EVICTION,
                                    owner_address,  // ★ 用 Owner 地址订阅
                                    object_id.Binary(),
                                    /*subscribe_done_callback=*/nullptr,
                                    subscription_callback,
                                    owner_dead_callback);
```

### 18.3 Owner 在 Pin 中的角色

Owner 自身**从不主动发起 Pin**，它只负责两件事：

1. **被动记录 Pin 位置**：通过 `HandleTaskReturn` → `UpdateObjectPinnedAtRaylet(object_id, worker_node_id)` 记录 `pinned_at_node_id_`
2. **GC 时发布 eviction 通知**：当 RefCount == 0 时，`ProcessSubscribeForObjectEviction` 注册的 callback 会 `Publish(WORKER_OBJECT_EVICTION)`，通知所有订阅了该对象的 Raylet 释放 Pin

完整的角色分工：

```
角色       │ 职责
──────────┼──────────────────────────────────────────────
Worker     │ 生产 Object → Seal → 发起 PinObjectIDs RPC
           │ (携带 owner_address 参数)
──────────┼──────────────────────────────────────────────
Raylet     │ 收到 Pin RPC → 持有 shared_ptr（防 LRU 驱逐）
           │ 用 owner_address 订阅 Owner 的 eviction channel
           │ 收到 eviction 通知 → ReleaseFreedObject
──────────┼──────────────────────────────────────────────
Owner      │ 记录 pinned_at_node_id_（被动）
(Driver)   │ RefCount == 0 时发布 eviction 通知（主动）
           │ Owner 进程死亡时 → Raylet 的 owner_dead_callback 释放
```

---

## 19. Plasma LRU 驱逐机制详解

### 19.1 ref_count 与 LRU 的联动

Plasma 中每个对象有一个 `ref_count`，控制对象是否在 LRU 链表中：

```cpp
// obj_lifecycle_mgr.cc:128-146
bool ObjectLifecycleManager::AddReference(const ObjectID &object_id) {
  auto entry = object_store_->GetObject(object_id);
  if (!entry) return false;
  // ★ 第一次被引用时（0→1），从 LRU 移除
  if (entry->ref_count_ == 0) {
    eviction_policy_->BeginObjectAccess(object_id);
  }
  entry->ref_count_++;
  return true;
}

bool ObjectLifecycleManager::RemoveReference(const ObjectID &object_id) {
  auto entry = object_store_->GetObject(object_id);
  if (!entry || entry->ref_count_ == 0) return false;
  entry->ref_count_--;
  // ★ 最后一个引用释放时（1→0），放回 LRU
  if (entry->ref_count_ == 0) {
    eviction_policy_->EndObjectAccess(object_id);
  }
  return true;
}
```

`BeginObjectAccess` 和 `EndObjectAccess` 的实现（`eviction_policy.cc:136-147`）：

```cpp
void EvictionPolicy::BeginObjectAccess(const ObjectID &object_id) {
  cache_.Remove(object_id);           // ★ 从 LRU 链表移除
  pinned_memory_bytes_ += GetObjectSize(object_id);
}

void EvictionPolicy::EndObjectAccess(const ObjectID &object_id) {
  auto size = GetObjectSize(object_id);
  cache_.Add(object_id, size);        // ★ 加回 LRU 链表
  pinned_memory_bytes_ -= size;
}
```

状态转换图：

```
ref_count == 0          ref_count > 0
 ┌──────────┐          ┌──────────┐
 │ 在 LRU 中 │ ←──────→│ 不在 LRU  │
 │ 可被驱逐  │  Add/    │ 不可驱逐  │
 │          │  Remove  │          │
 │          │  Ref     │          │
 └──────────┘          └──────────┘
```

### 19.2 LRU 驱逐时机与选择算法

**驱逐只在 `CreateObjectInternal` 分配新对象空间时触发**（`obj_lifecycle_mgr.cc:181-206`）：

```cpp
// obj_lifecycle_mgr.cc:189-206
const LocalObject *ObjectLifecycleManager::CreateObjectInternal(...) {
  for (int num_tries = 0; num_tries <= 10; num_tries++) {
    auto result = object_store_->CreateObject(object_info, source, false);
    if (result != nullptr) return result;  // 空间充足，直接分配

    // ★ 空间不足，触发驱逐
    std::vector<ObjectID> objects_to_evict;
    int64_t space_needed = eviction_policy_->RequireSpace(
        object_info.GetObjectSize(), objects_to_evict);
    EvictObjects(objects_to_evict);

    if (space_needed > 0) break;  // 驱逐后仍不够
  }
  // 如果允许 fallback，使用文件系统分配
}
```

`RequireSpace` 的驱逐策略（`eviction_policy.cc:120-134`）：

```cpp
int64_t EvictionPolicy::RequireSpace(int64_t size,
                                      std::vector<ObjectID> &objects_to_evict) {
  int64_t required_space = allocator_.Allocated() + size - allocator_.GetFootprintLimit();
  // ★ 尝试多释放 20% 余量
  int64_t space_to_free = std::max(required_space, allocator_.GetFootprintLimit() / 5);
  int64_t num_bytes_evicted = ChooseObjectsToEvict(space_to_free, objects_to_evict);
  return required_space - num_bytes_evicted;
}
```

`ChooseObjectsToEvict` 的选择算法（`eviction_policy.cc:82-94`）：

```cpp
// LRUCache::ChooseObjectsToEvict
int64_t LRUCache::ChooseObjectsToEvict(int64_t num_bytes_required,
                                        std::vector<ObjectID> &objects_to_evict) {
  int64_t bytes_evicted = 0;
  auto it = item_list_.end();  // ★ 从链表尾部开始（最旧）
  while (bytes_evicted < num_bytes_required && it != item_list_.begin()) {
    it--;
    objects_to_evict.push_back(it->first);
    bytes_evicted += it->second;
  }
  return bytes_evicted;
}
```

LRU 是一个双向链表（`std::list`）+ HashMap（`item_map_`）：
- **新对象加入时放链表头部**（`emplace_front`）
- **驱逐时从尾部选**（最旧优先）
- **被访问时移除再重新插入头部**（通过 Remove + Add）

驱逐后的硬安全检查（`obj_lifecycle_mgr.cc:232-241`）：

```cpp
void ObjectLifecycleManager::EvictObjects(const std::vector<ObjectID> &objects_to_evict) {
  for (const auto &object_id : objects_to_evict) {
    auto entry = object_store_->GetObject(object_id);
    RAY_CHECK(entry != nullptr);
    RAY_CHECK(entry->state_ == ObjectState::PLASMA_SEALED);
    RAY_CHECK(entry->ref_count_ == 0)  // ★ 绝对不会驱逐有引用的对象
        << "To evict an object, there must be no clients currently using it.";
    DeleteObjectInternal(object_id);
  }
}
```

### 19.3 被 Pin 的对象为什么不会被 LRU 驱逐

Raylet 通过 `PinObjectsAndWaitForFree` 持有 `shared_ptr<RayObject>`（`local_object_manager.cc:51`），这个 `shared_ptr` 对应 Plasma Store 中的一次 `AddReference`：

```
PinObjectsAndWaitForFree
  → pinned_objects_[object_id] = std::move(object)  // 持有 shared_ptr
  → Plasma ref_count >= 1
  → BeginObjectAccess → 从 LRU 链表移除
  → 驱逐算法看不到这个对象
  → EvictObjects 中 CHECK(ref_count == 0) 也能保证
```

对比不同场景下对象的 LRU 状态：

| 场景 | ref_count | 在 LRU 中？ | 可被驱逐？ |
|------|-----------|-----------|----------|
| 被 Raylet Pin 保护 | ≥ 1 | 否 | 否 |
| Worker 正在 ray.get() | ≥ 1 | 否 | 否 |
| PullManager 临时 Pin | ≥ 1 | 否 | 否（Pull 完成后会 UnpinObject 释放） |
| Push 接收端 Seal+Release 后无人引用 | 0 | **是** | **是** |
| 对象被 del 但 spilling 中 | ≥ 1 | 否 | 否（spilling 完成后释放） |

---

## 20. Object GC 完整链路与 Pin 自动回收

### 20.1 Owner 端引用计数归零触发 GC

当 Python 层对象引用超出作用域或显式 `del`：

```
Python: del ref / ref 超出作用域
  → C++ RefCount 递减
  → RefCount == 0
  → OnObjectOutOfScopeOrFreedCallback 被触发
```

Owner 通过 `ProcessSubscribeForObjectEviction`（`core_worker.cc:3656-3708`）注册了回调：

```cpp
// core_worker.cc:3659-3668
auto unpin_object = [this](const ObjectID &object_id) {
    RAY_LOG(DEBUG).WithField(object_id) << "Object is deleted. Unpinning the object.";
    rpc::PubMessage pub_message;
    pub_message.set_key_id(object_id.Binary());
    pub_message.set_channel_type(rpc::ChannelType::WORKER_OBJECT_EVICTION);
    pub_message.mutable_worker_object_eviction_message()->set_object_id(
        object_id.Binary());
    // ★ 广播发布到所有订阅了该对象 eviction channel 的 Raylet
    object_info_publisher_->Publish(std::move(pub_message));
};

// 注册回调到 reference_counter
if (!reference_counter_->AddObjectOutOfScopeOrFreedCallback(object_id, unpin_object)) {
    // 对象已经被释放，立即 unpin
    unpin_object(object_id);
}
```

### 20.2 Raylet 端 Pin 释放（subscription_callback）

Raylet 收到 Owner 的 eviction 通知后（`local_object_manager.cc:82-88`）：

```cpp
auto subscription_callback = [this, owner_address](const rpc::PubMessage &msg) {
    const auto obj_id = ObjectID::FromBinary(
        msg.worker_object_eviction_message().object_id());
    ReleaseFreedObject(obj_id);  // ★ 释放 Pin
    core_worker_subscriber_->Unsubscribe(
        rpc::ChannelType::WORKER_OBJECT_EVICTION, owner_address, obj_id.Binary());
};
```

`ReleaseFreedObject` (`local_object_manager.cc:111-148`) 的处理：

```cpp
void LocalObjectManager::ReleaseFreedObject(const ObjectID &object_id) {
    auto it = local_objects_.find(object_id);
    if (it == local_objects_.end() || it->second.is_freed_) return;

    it->second.is_freed_ = true;  // 标记为已释放

    auto pinned_objects_it = pinned_objects_.find(object_id);
    if (pinned_objects_it != pinned_objects_.end()) {
        // ★ 释放 shared_ptr → Plasma ref_count-- → 可能触发 EndObjectAccess → 放回 LRU
        pinned_objects_size_ -= pinned_objects_it->second->GetSize();
        pinned_objects_.erase(pinned_objects_it);
        local_objects_.erase(it);
    } else {
        // 对象正在 spilling 或已 spill，延迟处理
        spilled_object_pending_delete_.push(object_id);
    }

    // ★ 加入待删除队列
    if (free_objects_period_ms_ >= 0) {
        objects_pending_deletion_.emplace(object_id);
    }
    // 批量或立即 Flush
    if (objects_pending_deletion_.size() == free_objects_batch_size_ ||
        free_objects_period_ms_ == 0) {
        FlushFreeObjects();
    }
}
```

`FlushFreeObjects` (`local_object_manager.cc:150-163`) 调用 `on_objects_freed_` 回调：

```cpp
void LocalObjectManager::FlushFreeObjects() {
    if (!objects_pending_deletion_.empty()) {
        std::vector<ObjectID> objects_to_delete(
            objects_pending_deletion_.begin(), objects_pending_deletion_.end());
        on_objects_freed_(objects_to_delete);  // ★ 调用注册的回调
        objects_pending_deletion_.clear();
    }
}
```

在 `main.cc:854-857` 中注册的回调：

```cpp
/*on_objects_freed*/
[&](const std::vector<ray::ObjectID> &object_ids) {
    object_manager->FreeObjects(object_ids, /*local_only=*/false);  // ★ local_only=false
},
```

`local_only=false` 意味着**不仅删除本地对象，还会广播到所有其他节点删除副本**。

### 20.3 Owner 死亡时的回收

`PinObjectsAndWaitForFree` 注册了 `owner_dead_callback`（`local_object_manager.cc:92-96`）：

```cpp
auto owner_dead_callback = [this, owner_address](
    const std::string &object_id_binary, const Status &) {
    const auto obj_id = ObjectID::FromBinary(object_id_binary);
    ReleaseFreedObject(obj_id);  // ★ Owner 死亡也触发释放
};
```

这保证了即使 Owner 进程崩溃，Raylet 也会最终释放 Pin，不会发生内存泄漏。

### 20.4 对潮汐节点方案的影响

GC 机制**完全兼容**潮汐节点副本迁移方案：

1. **稳定节点上的副本**：如果通过 `PinObjectIDs` 建立了主 Pin，Raylet 会订阅 Owner 的 eviction channel → Owner GC 时通知释放 → 正确回收
2. **潮汐节点上的残留副本**：`FreeObjects(local_only=false)` 的广播会到达所有节点 → 潮汐节点也会收到删除通知
3. **Owner 死亡**：`owner_dead_callback` 确保不泄漏
4. **不需要修改 GC 代码**：eviction 通知是广播给**所有订阅者**，不是只发给 `pinned_at_node_id_`

---

## 21. 副本删除机制与稳定节点 Pin 建立

### 21.1 副本数据的删除路径 A：Owner 驱动的广播删除

这是正常 GC 流程的延续。主 Pin 节点释放后，`FlushFreeObjects` → `on_objects_freed_` → `FreeObjects(local_only=false)`：

```
ObjectManager::FreeObjects(object_ids, local_only=false)
  ├── 本地: buffer_pool_.FreeObjects(object_ids)
  │     → PlasmaClient::Delete(object_ids)
  │       → 遍历 object_ids:
  │           if objects_in_use_.count(id) == 0:
  │             not_in_use_ids.push_back(id)  // 可以删
  │           else:
  │             deletion_cache_.emplace(id)    // 正在用，延迟删
  │       → SendDeleteRequest(store_conn_, not_in_use_ids)
  │       → Plasma Store: ObjectLifecycleManager::DeleteObject(id)
  │           → ref_count != 0 ? earger_deletion_objects_ : DeleteObjectInternal
  │
  └── 远程: SpreadFreeObjectsRequest(object_ids, rpc_clients)
        → 遍历 GetAllNodeAddressAndLiveness() 中所有远程节点
        → 向每个节点发送 FreeObjectsRequest gRPC
            ↓
        远程节点: HandleFreeObjects()
          → FreeObjects(object_ids, local_only=true)  // ★ 不再扩散
            → buffer_pool_.FreeObjects() → PlasmaClient::Delete()
              → Plasma Store 删除副本 ★
```

`SpreadFreeObjectsRequest` 的详细实现（`object_manager.cc:666-688`）：

```cpp
void ObjectManager::SpreadFreeObjectsRequest(
    const std::vector<ObjectID> &object_ids,
    const std::vector<std::pair<NodeID, std::shared_ptr<rpc::ObjectManagerClientInterface>>>
        &rpc_clients) {
  rpc::FreeObjectsRequest free_objects_request;
  for (const auto &e : object_ids) {
    free_objects_request.add_object_ids(e.Binary());
  }
  for (const auto &entry : rpc_clients) {
    entry.second->FreeObjects(
        free_objects_request,
        [this, node_id = entry.first, free_objects_request](
            const Status &status, const rpc::FreeObjectsReply &reply) {
          if (!status.ok()) {
            // ★ 失败时指数退避重试
            RetryFreeObjects(node_id, 0, free_objects_request);
          }
        });
  }
}
```

**注意**：`SpreadFreeObjectsRequest` 向**所有远程节点**广播（不仅是已知 location 的节点），并且失败时会 `RetryFreeObjects` 指数退避重试，确保副本最终被清理。

### 21.2 副本数据的删除路径 B：LRU 被动驱逐

如果副本没有被 Pin（ref_count == 0，在 LRU 中），在 Plasma 需要空间创建新对象时会被 LRU 驱逐：

```
CreateObjectInternal 需要空间
  → EvictionPolicy::RequireSpace(size, objects_to_evict)
    → ChooseObjectsToEvict(space_to_free, objects_to_evict)
      → 从 LRU 链表尾部（最旧）开始选
      → 选中副本对象
  → EvictObjects(objects_to_evict)
    → CHECK(ref_count == 0)  // 只驱逐无引用的对象
    → DeleteObjectInternal(object_id)
      → eviction_policy_->RemoveObject(object_id)  // 从 LRU 移除
      → object_store_->DeleteObject(object_id)      // 物理删除
      → delete_object_callback_(object_id)          // 通知上层
  ↓
ObjectManager::HandleObjectDeleted(object_id)
  → ReportObjectRemoved(object_id, self_node_id_)  // ★ 通知 Owner 移除 location
```

LRU 驱逐后，`ReportObjectRemoved` 会通知 Owner 更新 `locations` 集合，移除被驱逐副本所在节点的 location 记录。

### 21.3 Plasma 层的删除安全保护

Plasma 有多层保护确保对象不会在使用中被意外删除：

| 保护层 | 保护机制 | 代码位置 |
|--------|---------|---------|
| **PlasmaClient** | 如果对象正在被 Worker Get 使用（`objects_in_use_`），放入 `deletion_cache_` 延迟删除 | `client.cc:636-641` |
| **PlasmaStore (DeleteObject)** | 如果 `ref_count != 0`，放入 `earger_deletion_objects_` 等 ref_count 归零后再删 | `obj_lifecycle_mgr.cc:109-113` |
| **PlasmaStore (DeleteObject)** | 如果对象未 Sealed，放入 `earger_deletion_objects_` | `obj_lifecycle_mgr.cc:101-106` |
| **LRU 驱逐** | `ref_count > 0` 的对象被 `BeginObjectAccess` 从 LRU 移除，驱逐算法看不到 | `eviction_policy.cc:136-140` |
| **LRU 驱逐硬检查** | `EvictObjects` 中 `CHECK(ref_count == 0)`，绝不驱逐有引用对象 | `obj_lifecycle_mgr.cc:236` |

`earger_deletion_objects_` 的延迟删除时机：在 `RemoveReference` 中 ref_count 归零后检查并执行（`obj_lifecycle_mgr.cc:171-173`）：

```cpp
if (entry->ref_count_ == 0) {
    eviction_policy_->EndObjectAccess(object_id);  // 放回 LRU
    if (earger_deletion_objects_.count(object_id) > 0) {
        DeleteObjectInternal(object_id);  // ★ 此前因 ref_count>0 被延迟的删除现在执行
    }
}
```

### 21.4 Push 接收端副本的 LRU 暴露窗口

当潮汐节点通过 Push 将对象发送到稳定节点时，接收端的处理链路：

```
ObjectBufferPool::WriteChunk (所有 chunk 写完)
  → store_client_->Seal(object_id)      // 对象变为 SEALED
  → store_client_->Release(object_id)   // ★ ref_count-- (Create 时加的引用被释放)
  → ref_count 归零 → EndObjectAccess → 对象放回 LRU
  ↓
ObjectManager::HandleObjectAdded()
  → ReportObjectAdded(object_id, self_node_id_)  // 上报 location 到 Owner
  → PullManager::PinNewObjectIfNeeded()            // 仅在 Pull 请求场景下临时 Pin
  ↓
Owner 收到 location → AddObjectLocation
  → 触发 TriggerPinTransfer → PinObjectIDs RPC 到稳定节点
  → Raylet Pin 对象 → 从 LRU 移除
```

**LRU 暴露窗口**：从 `Release` 到 `PinObjectIDs` 完成之间，对象在 LRU 中，可能被驱逐。

窗口大小 ≈ `Release → HandleObjectAdded → ReportObjectAdded → RPC 到 Owner → Owner AddObjectLocation → TriggerPinTransfer → PinObjectIDs RPC 到稳定节点 → HandlePinObjectIDs`

估计时间：2-3 个 RPC 往返，约数毫秒到数十毫秒。

**风险评估**：

| 稳定节点 Plasma 空间 | 风险 | 是否需要处理 |
|---------------------|------|------------|
| 充足（< 80% 使用率） | 极低 | 否 |
| 紧张（> 80%） | 中等 | 建议增大 Plasma 或使用更优策略 |
| 已满 | 高 | Push 写入本身就可能失败 |

**可选优化方案**：如果需要彻底消除暴露窗口，可以让稳定节点 Raylet 在收到 Push 数据时**同时建立 Pin**，而不是等 Owner 发起。这需要修改 `HandlePushObjectToStableNode` 的接收端逻辑：

```cpp
// 方案：接收端在 Seal 后立即 Pin，不等 Owner
// 在 ObjectManager::HandleObjectAdded() 中增加逻辑：
// 如果该对象来自潮汐节点的 Push（可通过 ObjectSource 区分），
// 则立即调用 PinObjectsAndWaitForFree()，等 Owner 的 PinObjectIDs 到达后
// 由 PinObjectsAndWaitForFree 的去重逻辑（local_objects_.emplace 返回 false）处理。
```

### 21.5 稳定节点上建立主 Pin 的完整流程

Owner（Driver）检测到新 location 在稳定节点后，主动发起 Pin 转移：

```
Owner (Driver)
  │
  │ AddObjectLocationInternal(object_id, stable_node_id)
  │   → locations.emplace(stable_node_id)
  │   → 检查: pinned_at_node_id_ 是否指向潮汐节点?
  │   → 是 → TriggerPinTransfer(object_id, stable_node_id)
  │
  │ ① PinObjectIDs RPC ──────────────────→ 稳定节点 Raylet
  │                                           │
  │                                     HandlePinObjectIDs:
  │                                     ② Plasma Get(object_id)
  │                                        → 得到 shared_ptr<RayObject>
  │                                     ③ AddReference
  │                                        → ref_count 0→1
  │                                        → BeginObjectAccess → 从 LRU 移除 ★
  │                                     ④ PinObjectsAndWaitForFree
  │                                        → pinned_objects_[object_id] = shared_ptr
  │                                        → 订阅 Owner WORKER_OBJECT_EVICTION
  │                                        → 订阅 owner_dead_callback
  │                                           │
  │  ←──────────────────── Pin OK ──────────  │
  │
  │ ⑤ UpdateObjectPinnedAtRaylet(object_id, stable_node_id)
  │    → pinned_at_node_id_ = stable_node_id
  │    → 主 Pin 正式转移到稳定节点 ★
  │
  │ ⑥ 通知旧潮汐节点释放旧 Pin
  │    → FreeObjects({object_id}, local_only=true)
  │    → 旧 Raylet: ReleaseFreedObject
  │      → 释放 shared_ptr → Plasma ref_count--
  │      → EndObjectAccess → 对象放回 LRU（可被驱逐/后续删除）
```

**Pin 建立后的对象状态**：

| 属性 | 值 | 说明 |
|------|-----|------|
| `ref_count` | ≥ 1 | `LocalObjectManager` 持有 `shared_ptr` |
| LRU 位置 | 不在 LRU 中 | `BeginObjectAccess` 已移除 |
| 是否可被驱逐 | 否 | 不在 LRU 链表中，驱逐算法看不到 |
| eviction 订阅 | 已订阅 Owner | Owner GC 时通知释放 |
| owner_dead 订阅 | 已订阅 | Owner 挂掉也释放 |
| 广播删除 | 可达 | `FreeObjects(local_only=false)` 广播到所有节点 |

**先建后拆的安全性保证**：

```
时间线：
  t0: 对象在潮汐节点有主 Pin
  t1: Owner 发起 PinObjectIDs 到稳定节点
  t2: 稳定节点 Pin 成功 → pinned_at 更新为稳定节点
  t3: 通知潮汐节点释放旧 Pin

  [t0, t2): 至少潮汐节点有 Pin → 安全
  [t2, t3): 两个节点都有 Pin → 安全
  [t3, ∞):  只有稳定节点有 Pin → 安全

  → 任何时刻至少有一个 Pin ✅
```

### 21.6 设计文档同步更新

基于以上分析，设计文档 `ray-tidal-node-object-replication-design.md` 已同步更新以下内容：

1. **5.4 节**：补充副本在 Plasma 中的状态（ref_count=0, 在 LRU 中），新增 5.4.1 小节说明 LRU 暴露窗口
2. **5.5 节**：新增 5.5.1 小节详述稳定节点 Pin 建立的完整流程（`HandlePinObjectIDs → Get → AddReference → BeginObjectAccess → PinObjectsAndWaitForFree`）
3. **5.5.5 节**：新增副本删除机制的完整说明，包括路径 A（广播删除）、路径 B（LRU 被动驱逐）、Owner 死亡回收、安全保护机制
4. **5.7 节**：稳定节点选择策略编号调整（原 5.6 → 5.7）

---

*本文档基于 Ray 2.52.1 源码分析，重点关注不稳定节点场景下的对象远程存储方案设计。*
