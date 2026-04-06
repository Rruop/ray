# Phase 2 实现计划：Pin 转移（可抢占节点 → 稳定节点）

## 概述

Phase 1 将对象数据 Push 到稳定节点，但 **Pin 仍在可抢占节点**——稳定节点上的副本 `ref_count=0`，处于 LRU 中，随时可能被驱逐。Phase 2 的目标：Owner 得知稳定节点有副本后，主动将 Pin 转移到稳定节点，保护副本不被驱逐。

## 核心设计决策

| 问题 | 决策 | 理由 |
|------|------|------|
| 在哪触发 Pin 转移？ | `CoreWorker::AddObjectLocationOwner` 中，`AddObjectLocation` 之后 | ReferenceCounter 是纯状态跟踪器，不应发起 RPC；CoreWorker 有 `gcs_client_`、`raylet_client_pool_` |
| Owner 如何判断节点是否可抢占？ | `gcs_client_->Nodes().AsyncGetAll()` 查 `GcsNodeInfo.labels`，结果缓存到 `preemptible_node_cache_` | `GcsNodeAddressAndLiveness` 不含 labels；节点标签启动后不变，缓存安全 |
| 如何执行远程 PinObjectIDs？ | 复用 `ObjectRecoveryManager::PinExistingObjectCopy` 的模式：`raylet_client_pool_->GetOrConnectByAddress(addr)->PinObjectIDs(...)` | 已有成熟模式 |
| 是否释放旧 Pin？ | **不主动释放**。旧 Pin 通过现有 `WORKER_OBJECT_EVICTION` 机制随对象生命周期自然释放 | 最简单、最安全；旧 Pin 提供额外保护；`pinned_at_node_id_` 更新后，旧节点回收不触发 Recovery |
| 去重机制 | `pin_transfers_in_flight_` (set\<ObjectID\>) | 防止同一对象并发多次 Pin 转移 |

## 数据流

```
Owner (CoreWorker, 稳定节点)                    稳定节点 B Raylet
    │                                              │
    │ ← ReportObjectAdded (B 有对象副本)           │
    │                                              │
    │ AddObjectLocationOwner(object_id, B)         │
    │   └→ reference_counter_->AddObjectLocation   │
    │   └→ MaybeTriggerPinTransfer(object_id, B)   │
    │       ├─ 检查 strategy == "push_to_stable_node"
    │       ├─ 查 pinned_at_node_id_ → 可抢占节点 A
    │       ├─ 查 preemptible_node_cache_ (或 AsyncGetAll 回填)
    │       ├─ 确认 A 是可抢占、B 是稳定
    │       └→ DoPinTransfer(object_id, B, addr_B)
    │           ├─ pin_transfers_in_flight_.insert(object_id)
    │           └→ PinObjectIDs(object_id) ──────→│
    │                                         Pin 成功
    │           ←── reply(success=true) ──────────│
    │           ├─ UpdateObjectPinnedAtRaylet(object_id, B)
    │           └─ pin_transfers_in_flight_.erase(object_id)
    │                                              │
    │ ★ pinned_at_node_id_ = B (稳定节点)          │
    │ ★ 可抢占节点 A 回收 → 不触发 Recovery        │
```

## 实现步骤

### Step 1: CoreWorker 新增成员和方法声明

**文件**: `src/ray/core_worker/core_worker.h`

新增 private 成员:
```cpp
/// Cache: NodeID → is_preemptible. Lazily populated via GCS AsyncGetAll.
absl::flat_hash_map<NodeID, bool> preemptible_node_cache_
    ABSL_GUARDED_BY(preemptible_cache_mutex_);
mutable absl::Mutex preemptible_cache_mutex_;

/// Objects currently undergoing pin transfer. Prevents duplicate transfers.
absl::flat_hash_set<ObjectID> pin_transfers_in_flight_
    ABSL_GUARDED_BY(pin_transfer_mutex_);
mutable absl::Mutex pin_transfer_mutex_;
```

新增 private 方法:
```cpp
void MaybeTriggerPinTransfer(const ObjectID &object_id,
                             const NodeID &new_location_node_id);
void DoPinTransfer(const ObjectID &object_id,
                   const NodeID &stable_node_id,
                   const rpc::Address &stable_node_address);
std::optional<bool> IsNodePreemptibleCached(const NodeID &node_id) const;
void CacheNodePreemptible(const NodeID &node_id, bool is_preemptible);
```

### Step 2: 实现缓存辅助方法

**文件**: `src/ray/core_worker/core_worker.cc`

```cpp
std::optional<bool> CoreWorker::IsNodePreemptibleCached(const NodeID &node_id) const {
  absl::MutexLock lock(&preemptible_cache_mutex_);
  auto it = preemptible_node_cache_.find(node_id);
  if (it != preemptible_node_cache_.end()) return it->second;
  return std::nullopt;
}

void CoreWorker::CacheNodePreemptible(const NodeID &node_id, bool is_preemptible) {
  absl::MutexLock lock(&preemptible_cache_mutex_);
  preemptible_node_cache_[node_id] = is_preemptible;
}
```

静态辅助函数:
```cpp
static bool IsNodePreemptibleFromLabels(const rpc::GcsNodeInfo &node_info) {
  auto it = node_info.labels().find("ray.io/node-market-type");
  return it != node_info.labels().end() &&
         it->second == RayConfig::instance().preemptible_node_market_type();
}
```

### Step 3: 实现 MaybeTriggerPinTransfer

**文件**: `src/ray/core_worker/core_worker.cc`

逻辑:
1. 检查 `object_replication_strategy != "push_to_stable_node"` → return
2. 调用 `reference_counter_->IsPlasmaObjectPinnedOrSpilled` 获取 `pinned_at`
3. 检查 `owned_by_us && !pinned_at.IsNil() && !spilled && pinned_at != new_location_node_id`
4. 检查 `pin_transfers_in_flight_` 不含此 object → 否则 return（best-effort early exit）
5. 查缓存获取两个节点的 preemptible 状态
6. 如果都已缓存 → 直接判断；否则调用 `gcs_client_->Nodes().AsyncGetAll(callback, -1, {uncached_nodes})` 查询后回调继续
7. 如果 `pinned_at 是可抢占 && new_location 是稳定` → 构建 `rpc::Address`，调用 `DoPinTransfer`

### Step 4: 实现 DoPinTransfer

**文件**: `src/ray/core_worker/core_worker.cc`

逻辑（复用 `ObjectRecoveryManager::PinExistingObjectCopy` 模式）:
1. `pin_transfers_in_flight_.insert(object_id)` → 如果已存在则 return（权威去重点）
2. 调用 `raylet_client_pool_->GetOrConnectByAddress(stable_addr)->PinObjectIDs(rpc_address_, {object_id}, ObjectID::Nil(), callback)`
3. 回调中:
   - 总是 `pin_transfers_in_flight_.erase(object_id)`
   - 成功 (`status.ok() && reply.successes_size() > 0 && reply.successes(0)`): 调用 `reference_counter_->UpdateObjectPinnedAtRaylet(object_id, stable_node_id)`
   - 失败: DEBUG 日志，原 Pin 不变

### Step 5: 在 AddObjectLocationOwner 中调用

**文件**: `src/ray/core_worker/core_worker.cc`

在 `AddObjectLocationOwner` 末尾、generator 处理之后添加:
```cpp
if (reference_exists) {
  MaybeTriggerPinTransfer(object_id, node_id);
}
```

### Step 6: 更新 UpdateObjectPinnedAtRaylet 日志

**文件**: `src/ray/core_worker/reference_counter.cc`

将 `"This should only happen during reconstruction"` 改为 `"This can happen during reconstruction or pin transfer from preemptible node"`。

## 文件改动清单

| # | 文件路径 | 改动类型 | 说明 |
|---|---------|---------|------|
| 1 | `src/ray/core_worker/core_worker.h` | 新增 | 4 个成员 + 4 个方法声明 |
| 2 | `src/ray/core_worker/core_worker.cc` | 新增 | 5 个方法实现 + 修改 AddObjectLocationOwner |
| 3 | `src/ray/core_worker/reference_counter.cc` | 修改 | 更新一条日志信息 |

## 边界情况分析

| 场景 | 行为 | 正确性 |
|------|------|--------|
| Pin 转移完成前可抢占节点挂了 | `pinned_at_node_id_` 仍指向可抢占节点 → `ResetObjectsOnRemovedNode` 触发 Recovery → `PinExistingObjectCopy` 在稳定节点上 Pin | ✅ |
| Pin 转移完成后可抢占节点挂了 | `pinned_at_node_id_` 已指向稳定节点 → 不触发 Recovery；旧 Pin 的 `owner_dead_callback` 不适用（owner 不在可抢占节点上） | ✅ |
| 稳定节点在 Pin 前 LRU 驱逐了副本 | `PinObjectIDs` 返回 `success=false` → 不更新 `pinned_at` → 原 Pin 不变 | ✅ |
| 对象在 Pin 转移中间被 del | `UpdateObjectPinnedAtRaylet` 检查 `freed_objects_` → 跳过 | ✅ |
| 同一对象并发触发两次 | `pin_transfers_in_flight_` set 去重 → 第二次 return | ✅ |
| strategy=none 时 | `MaybeTriggerPinTransfer` 第一行 early return | ✅ 零开销 |

## 线程安全

- `preemptible_node_cache_`: 由 `preemptible_cache_mutex_` 保护
- `pin_transfers_in_flight_`: 由 `pin_transfer_mutex_` 保护
- `reference_counter_->IsPlasmaObjectPinnedOrSpilled` / `UpdateObjectPinnedAtRaylet`: 内部有 `mutex_`
- `raylet_client_pool_->GetOrConnectByAddress`: 线程安全
- `gcs_client_->Nodes().AsyncGetAll`: 线程安全

## 验证

1. **编译**: `bazel build //src/ray/core_worker:core_worker_lib`
2. **日志验证**: 启用 `object_replication_strategy=push_to_stable_node`，观察 INFO 日志:
   - "Initiating pin transfer to stable node"
   - "Pin transfer succeeded, updating pinned location"
3. **功能验证**: 可抢占节点 Task 返回大对象 → 节点下线 → `ray.get` 能从稳定节点获取（不触发重算）
4. **单元测试**: 测试 `MaybeTriggerPinTransfer` 的各种 early-return 路径

## 实现状态

✅ 已实现（当前分支 `release-syp-analyze`）
