# 对象副本推送与 Pin Transfer 实现方案

## 概述

当可抢占（Spot）节点上的 Worker 执行 Task 产生大对象时，对象数据仅存储在该节点的本地 Plasma Store 中。节点被回收后，数据丢失需要血缘重算。本方案分两个阶段解决这个问题：

- **Phase 1（数据复制）**：在可抢占节点上创建对象后，主动将数据 Push 到一个稳定节点，提供数据冗余。
- **Phase 2（Pin Transfer）**：当 Owner CoreWorker 得知稳定节点上出现副本后，将 Pin 从可抢占节点转移到稳定节点，使稳定节点上的副本从 LRU 可驱逐变为 Pin 保护。

## 核心设计决策

### Phase 1：数据复制

| 问题 | 决策 | 理由 |
|------|------|------|
| 何时触发复制？ | `NodeManager::HandleObjectLocal` 中调用 `MaybeReplicateObject` | 对象刚 Seal，数据已可读且 Worker 尚未 Release，Push 可立即开始 |
| 由谁触发复制？ | Raylet 进程内的 NodeManager 直接调用 `object_manager_.Push()` | NodeManager 拥有集群资源视图和 ObjectManager 引用，无需跨进程 IPC |
| 如何判断本节点可抢占？ | 启动时从 `initial_config_.labels` 中读取 `ray.io/node-market-type`，缓存为 `is_preemptible_node_cached_` | 一次性计算，避免重复解析 |
| 复制由谁执行？ | `ObjectManager::Push(object_id, target_node)` — Raylet 进程内直接调用 | 消除 Worker→Raylet IPC 开销，减少一次序列化和调度延迟 |
| 如何选择稳定节点？ | `SelectStableNode()` 从 `cluster_resource_scheduler_` 的资源视图中随机选一个非自身、非可抢占的节点 | 简单有效，避免热点 |
| 对象大小门槛？ | `object_replication_min_size`（默认 100KB） | < 100KB 的对象通过 gRPC Inline 返回，不进 Plasma |
| 并发控制？ | `object_replication_max_concurrent`（默认 10），per-node 原子计数器 | 防止突发大量小对象压垮网络 |
| Push 失败怎么办？ | 不重试，Best-effort | 原始 Pin 仍在，后续 Recovery 机制兜底 |

### Phase 2：Pin Transfer

| 问题 | 决策 | 理由 |
|------|------|------|
| 何时触发 Pin Transfer？ | Owner CoreWorker 的 `AddObjectLocationOwner` 被调用时，检查是否需要转移 | AddObjectLocationOwner 在新副本上报时触发，是感知新位置的最早时机 |
| 由谁执行？ | Object 的 Owner CoreWorker（非 Task 执行 Worker） | Pin 信息由 Owner 的 ReferenceCounter 管理，只有 Owner 能更新 `pinned_at_node_id_` |
| 如何判断节点是否可抢占？ | `preemptible_node_cache_`（GCS 多节点缓存） | Owner 需要判断任意节点的 preemptible 属性，不能只看自己 |
| 缓存未命中怎么办？ | `AsyncGetAll` 查询 GCS 获取全部节点标签，回填缓存后重新判断 | 首次查询稍慢，后续全走缓存 |
| 转移机制？ | 向稳定节点 Raylet 发送 `PinObjectIDs` RPC，成功后更新 ReferenceCounter 的 `pinned_at` | 复用现有 RPC，无需新增接口 |
| 转移失败？ | 保持原 Pin 不变，稳定节点的 LRU 副本可能被驱逐 | 原始 Pin 仍在可抢占节点，数据仍受保护 |

## 架构对比：重构前 vs 重构后

### 重构前（原始方案）

```
CoreWorker (可抢占节点 A)          Raylet (可抢占节点 A)           Raylet (稳定节点 B)
    │                                │                              │
    │ SealExisting → PinObjectIDs ─→ │                              │
    │                              Pin 成功                          │
    │ MaybeReplicateObject ──────→   │                              │
    │ ReplicateObject RPC ────────→  │                              │
    │                              检查 strategy/size/并发           │
    │                              SelectStableNode → B             │
    │                              Push(obj, B) ──────────────→     │
    │                                │                         Receive + Seal
```

**问题**：CoreWorker → Raylet 之间多一次 IPC（`ReplicateObject` RPC），需要新增完整的 RPC 链路（proto 定义、client 接口、server handler），而 NodeManager 本身就拥有判断条件和执行能力。

### 重构后（当前实现）

```
Raylet (可抢占节点 A)                                             Raylet (稳定节点 B)
    │                                                              │
    │ Plasma Store Seal Callback                                    │
    │ → HandleObjectLocal(object_info)                             │
    │   → MaybeReplicateObject(object_info)                        │
    │     检查 strategy / is_preemptible / size / 并发               │
    │     SelectStableNode → B                                      │
    │     object_manager_.Push(obj, B) ──────────────────────→     │
    │                                                              │
    │                                                         Receive + Seal
    │                                                              │
    │                                                         ReportObjectAdded
    │                                                           → RPC to Owner
    │                                                              │
    ★ Owner 得知 B 有副本（AddObjectLocationOwner）                    │
    ★ MaybeTriggerPinTransfer → DoPinTransfer → PinObjectIDs RPC ─→ Pin 成功
    ★ UpdateObjectPinnedAtRaylet(A→B)                              │
```

**优势**：
1. **零 IPC 开销**：Seal 回调在同一进程内，`HandleObjectLocal` → `MaybeReplicateObject` → `object_manager_.Push()` 全程 Raylet 进程内完成
2. **更早触发**：Seal 即触发，不等待 Worker Pin 成功后回调
3. **更简单的代码**：删除了 `ReplicateObject` RPC 整条链路（proto、flatbuffer、IPC client/server、handler），减少约 200 行代码
4. **时间安全**：Push 在 Seal 时启动（Worker 尚未 Release，对象不会被驱逐），Pin 稍后完成并接管保护，两者无冲突

## 完整数据流

```
Phase 1: 数据复制
─────────────────────────────────────────────────────────────────

Plasma Store (可抢占节点 A)    Raylet (可抢占节点 A)           Raylet (稳定节点 B)
    │                           │                              │
    │ Seal callback ──────────→ │                              │
    │                       HandleObjectLocal                  │
    │                         MaybeReplicateObject             │
    │                           │                              │
    │                           ├─ strategy=none? → skip       │
    │                           ├─ !is_preemptible? → skip     │
    │                           ├─ size < min? → skip         │
    │                           ├─ concurrent >= max? → skip   │
    │                           ├─ SelectStableNode → B        │
    │                           └─ Push(obj, B) ──────────→    │
    │                           │                         Receive + Seal
    │                           │                              │
    │                           │                         ReportObjectAdded
    │                           │                           → RPC to Owner

Phase 2: Pin Transfer
─────────────────────────────────────────────────────────────────

Owner CoreWorker                    Raylet (稳定节点 B)
    │                                │
    │ AddObjectLocationOwner(B)      │
    │ MaybeTriggerPinTransfer        │
    │   ├─ !owned_by_us? → skip     │
    │   ├─ pinned_at==B? → skip     │
    │   ├─ already_in_flight? → skip│
    │   ├─ IsNodePreemptibleCached(A)=true?
    │   │  IsNodePreemptibleCached(B)=false?
    │   └─ DoPinTransfer            │
    │      PinObjectIDs RPC ─────────→ Pin 成功
    │      UpdateObjectPinnedAtRaylet│
    │      (A → B)                   │
    │                                │
    ★ 对象现在 Pin 在稳定节点 B，不再依赖可抢占节点 A
```

## 实现步骤

### Step 1: 新增配置项

**文件**: `src/ray/common/ray_config_def.h`

```cpp
RAY_CONFIG(std::string, object_replication_strategy, "none")
RAY_CONFIG(std::string, preemptible_node_market_type, "spot")
RAY_CONFIG(int64_t, object_replication_min_size, 100 * 1024)
RAY_CONFIG(int64_t, object_replication_max_concurrent, 10)
```

### Step 2: NodeManager 新增 MaybeReplicateObject

**文件**: `src/ray/raylet/node_manager.h`

```cpp
void MaybeReplicateObject(const ObjectInfo &object_info);
bool IsPreemptibleNode() const;
NodeID SelectStableNode() const;

bool is_preemptible_node_cached_ = false;
mutable std::mt19937 rng_{std::random_device{}()};
std::atomic<int64_t> replications_in_flight_{0};
```

**文件**: `src/ray/raylet/node_manager.cc`

`IsPreemptibleNode`：从 `initial_config_.labels` 读取 `ray.io/node-market-type`，缓存到 `is_preemptible_node_cached_`。

`SelectStableNode`：遍历 `cluster_resource_scheduler_` 的资源视图，排除自身和 `ray.io/node-market-type == preemptible_node_market_type` 的节点，随机选一个。

`MaybeReplicateObject` 逻辑：
1. `object_replication_strategy != "push_to_stable_node"` → skip + metric
2. `!is_preemptible_node_cached_` → skip + metric
3. `object_info.data_size < object_replication_min_size` → skip + metric
4. `replications_in_flight_ >= max_concurrent` → skip + metric
5. `SelectStableNode()` 返回 Nil → skip + metric
6. `object_manager_.Push(object_id, target_node)`
7. Record success metric

**调用点**：`HandleObjectLocal` 末尾，`SpillIfOverPrimaryObjectsThreshold()` 之前调用 `MaybeReplicateObject(object_info)`。

### Step 3: CoreWorker 新增 Pin Transfer

**文件**: `src/ray/core_worker/core_worker.h`

```cpp
absl::flat_hash_map<NodeID, bool> preemptible_node_cache_
    ABSL_GUARDED_BY(preemptible_cache_mutex_);
mutable absl::Mutex preemptible_cache_mutex_;

absl::flat_hash_set<ObjectID> pin_transfers_in_flight_
    ABSL_GUARDED_BY(pin_transfer_mutex_);
mutable absl::Mutex pin_transfer_mutex_;

void MaybeTriggerPinTransfer(const ObjectID &object_id,
                             const NodeID &new_location_node_id);
void DoPinTransfer(const ObjectID &object_id,
                   const NodeID &stable_node_id,
                   const rpc::Address &stable_node_address);
std::optional<bool> IsNodePreemptibleCached(const NodeID &node_id) const;
void CacheNodePreemptible(const NodeID &node_id, bool is_preemptible);
```

**文件**: `src/ray/core_worker/core_worker.cc`

`IsNodePreemptibleFromLabels`：从 `GcsNodeInfo.labels` 判断节点是否可抢占。

`IsNodePreemptibleCached` / `CacheNodePreemptible`：带 mutex 保护的缓存读写。

`MaybeTriggerPinTransfer` 逻辑：
1. `object_replication_strategy != "push_to_stable_node"` → return
2. `reference_counter_->IsPlasmaObjectPinnedOrSpilled` → 不 owned / 不 pinned / 已 spilled → return
3. `pinned_at == new_location` → return
4. `pin_transfers_in_flight_` 已有此 object → return
5. 查缓存 `IsNodePreemptibleCached(pinned_at)` 和 `IsNodePreemptibleCached(new_location)`
6. **Fast path**（两者都命中）：`pinned_at` 可抢占 且 `new_location` 不可抢占 → DoPinTransfer
7. **Slow path**（有未命中）：`AsyncGetAll` 查 GCS → 回填缓存 → 重新判断 → DoPinTransfer

`DoPinTransfer` 逻辑：
1. 插入 `pin_transfers_in_flight_`
2. `PinObjectIDs` RPC 到稳定节点 Raylet
3. 成功：`reference_counter_->UpdateObjectPinnedAtRaylet(object_id, stable_node_id)`
4. 失败：日志记录，原 Pin 位置不变

**调用点**：`AddObjectLocationOwner` 末尾，添加新位置后调用 `MaybeTriggerPinTransfer(object_id, node_id)`。

### Step 4: Metrics

**文件**: `src/ray/raylet/metrics.h`

```cpp
ray::stats::Sum object_replication_succeeded_;
ray::stats::Sum object_replication_skipped_;
```

Skipped 按原因分类 Tag：`disabled`、`not_preemptible`、`too_small`、`concurrency_limit`、`no_stable_node`。

## 文件改动清单

### Phase 1：数据复制（commit `8a8e407e87`）

| # | 文件路径 | 改动类型 | 说明 |
|---|---------|---------|------|
| 1 | `src/ray/common/ray_config_def.h` | 新增 | 4 个配置项 |
| 2 | `src/ray/raylet/node_manager.h` | 新增 | `MaybeReplicateObject` / `IsPreemptibleNode` / `SelectStableNode` 声明 + 成员 |
| 3 | `src/ray/raylet/node_manager.cc` | 新增 | 三个方法实现 + `HandleObjectLocal` 调用点 + `is_preemptible_node_cached_` 构造时缓存 |
| 4 | `src/ray/raylet/metrics.h` | 新增 | replication metrics |
| 5 | `src/mock/ray/object_manager/object_manager.h` | 修改 | Mock Push 方法签名（增加 NodeID 参数） |

### Phase 2：Pin Transfer（commit `4cf54313f7`）

| # | 文件路径 | 改动类型 | 说明 |
|---|---------|---------|------|
| 1 | `src/ray/core_worker/core_worker.h` | 新增 | `preemptible_node_cache_` / `pin_transfers_in_flight_` / 4 个方法声明 |
| 2 | `src/ray/core_worker/core_worker.cc` | 新增 | `IsNodePreemptibleFromLabels` / `IsNodePreemptibleCached` / `CacheNodePreemptible` / `MaybeTriggerPinTransfer` / `DoPinTransfer` 实现 |
| 3 | `src/ray/core_worker/reference_counter.cc` | 修改 | `AddObjectLocationOwner` 末尾调用 `MaybeTriggerPinTransfer` |

### 已删除（重构移除）

| # | 文件路径 | 删除内容 | 说明 |
|---|---------|---------|------|
| 1 | `src/ray/raylet/node_manager.h` | `HandleReplicateObject` | 不再需要 RPC handler |
| 2 | `src/ray/raylet/node_manager.cc` | `HandleReplicateObject` 实现 | 改为 `MaybeReplicateObject` 进程内调用 |
| 3 | `src/ray/raylet/node_manager_server.h` | `HandleReplicateObject` 虚方法 | RPC 链路移除 |
| 4 | `src/ray/raylet/raylet.fbs` | `ReplicateObject` 消息定义 | FlatBuffer RPC 定义移除 |
| 5 | `src/ray/raylet_rpc_client/` | `ReplicateObject` 接口和实现 | IPC 客户端链路移除 |
| 6 | `src/ray/core_worker/core_worker.h` | `is_preemptible_node_` / `MaybeReplicateObject` | Worker 不再参与复制决策 |
| 7 | `src/ray/core_worker/core_worker.cc` | `MaybeReplicateObject` 实现 + 调用点 | 逻辑移至 NodeManager |
| 8 | `src/ray/core_worker/core_worker_options.h` | `is_preemptible_node` 选项 | 不再需要 Worker 感知自身节点类型 |
| 9 | `src/ray/flatbuffers/node_manager.fbs` | `is_preemptible_node` 字段 | 注册回复不再传递此标志 |

## 边界情况分析

| 场景 | 行为 | 正确性 |
|------|------|--------|
| strategy=none | `MaybeReplicateObject` 第一行 early return | ✅ 零开销 |
| 非可抢占节点 | `is_preemptible_node_cached_=false` → 不触发 | ✅ |
| 对象 < 100KB | `data_size < min_size` → skip | ✅ |
| 无稳定节点 | `SelectStableNode` 返回 Nil → skip | ✅ |
| Push 失败 | fire-and-forget，原 Pin 不变，Recovery 兜底 | ✅ |
| 并发达到上限 | `replications_in_flight_ >= max` → skip | ✅ |
| 稳定节点 B 已有副本 | `ObjectManager::Push` 内部去重（`unfulfilled_push_requests_`） | ✅ |
| Seal 后 Worker 尚未 Release | Push 可立即开始，对象不会被 Plasma 驱逐 | ✅ |
| Pin Transfer 时 Owner 不持有对象 | `IsPlasmaObjectPinnedOrSpilled` 返回 false → skip | ✅ |
| Pin Transfer 目标节点也是可抢占的 | `new_loc_preemptible=true` → 不触发转移 | ✅ |
| Pin Transfer 缓存未命中 | AsyncGetAll 查 GCS，回填后重新判断 | ✅ |
| Pin Transfer RPC 失败 | 原始 Pin 位置不变，稳定节点副本可能被 LRU 驱逐 | ✅ |
| 同一对象多次触发 | `pin_transfers_in_flight_` 去重 | ✅ |
| 可抢占节点被回收前 Push 未完成 | 原始 Pin 丢失，需要血缘重算 | ⚠️ Phase 1 best-effort |

## 已知限制

1. **并发限制是近似的**：`replications_in_flight_` 在 Push 发起后立即递减，实际是 burst rate limiter 而非真正的并发限制
2. **Metric 是发起而非完成**：`object_replication_succeeded` 记录的是 Push 发起成功，非数据传输完成
3. **无重复 Push 去重**：不同时间可能选中不同稳定节点，导致多份副本（但实践中 Seal 只触发一次 HandleObjectLocal）
4. **Pin Transfer 依赖 GCS 可用**：缓存未命中时需要查询 GCS，如果 GCS 不可用则跳过转移
5. **Pin Transfer 是异步的**：`DoPinTransfer` 发出 RPC 后不阻塞，成功回调中更新 pinned location

## 验证

1. **编译**: `bazel build //src/ray/raylet:raylet_lib` + `bazel build //src/ray/core_worker:core_worker_lib`
2. **配置验证**: 启用 `object_replication_strategy=push_to_stable_node`，观察日志和 metrics
3. **功能验证（Phase 1）**: 可抢占节点 Task 返回大对象 → 稳定节点 Plasma 中出现副本 → Owner 的 `object_locations_` 包含稳定节点
4. **功能验证（Phase 2）**: 稳定节点收到副本后 → Owner 触发 Pin Transfer → `pinned_at_node_id_` 从可抢占节点更新为稳定节点 → 可抢占节点上对象变为 LRU 可驱逐

## 实现状态

- ✅ Phase 1：commit `8a8e407e87` — 数据复制（NodeManager 侧，零 CoreWorker 改动）
- ✅ Phase 2：commit `4cf54313f7` — Pin Transfer（CoreWorker 侧）
