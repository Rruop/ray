# GPU Actor 感知缩容完整设计方案

## 文档信息

| 项目 | 内容 |
|------|------|
| 标题 | GPU Actor 感知缩容 —— 从 Ray Data 到 Autoscaler 的完整链路 |
| 版本 | v1.2 |
| 基于 Ray 版本 | 2.52.1 |
| 状态 | 已实现（阶段 1-4），Layer 4 公开 API 重构已完成 |
| 前置依赖 | Phase 1 对象复制、Phase 2 Pin 转移（已合入） |

---

## 目录

1. [背景与动机](#1-背景与动机)
2. [目标与非目标](#2-目标与非目标)
3. [问题根因分析](#3-问题根因分析)
4. [方案总体设计](#4-方案总体设计)
5. [Layer 1：Raylet 对象感知的 Drain 关闭（C++）](#5-layer-1raylet-对象感知的-drain-关闭c)
6. [Layer 2：对象迁移实现（C++）](#6-layer-2对象迁移实现c)
7. [Layer 3：Actor 强制释放（Python）](#7-layer-3actor-强制释放python)
8. [Layer 4：Ray Data 主动触发（Python）](#8-layer-4ray-data-主动触发python)
9. [完整时序图](#9-完整时序图)
10. [竞态和边界情况处理](#10-竞态和边界情况处理)
11. [配置项设计](#11-配置项设计)
12. [关键文件清单](#12-关键文件清单)
13. [实施阶段](#13-实施阶段)
14. [测试方案](#14-测试方案)
15. [审查发现与修复记录](#15-审查发现与修复记录)
16. [Layer 4 架构改进：公开 API 重构（v1.2）](#16-layer-4-架构改进公开-api-重构v12)

---

## 1. 背景与动机

### 1.1 问题描述

Ray Data 的 GPU operator 使用长生命周期 actor（通过 `ActorPoolMapOperator`）处理数据。典型场景为 GPU 推理/训练流水线中的 map_batches 操作。当 operator 处理完所有数据后，GPU 节点应当被释放以节省成本。然而当前 Ray 的缩容链路存在多处阻塞，导致 GPU 节点在 operator 完成后长时间无法被回收。

### 1.2 现有行为

```
Ray Data GPU operator 完成
    │
    ▼
_release_running_actor() 只 del 引用
    │
    ▼
Actor 可能不退出（pending ObjectRef callback 持有 handle）
    │
    ▼
GPU 资源不归还 → leased_workers_ 不清空
    │
    ▼
NODE_WORKERS busy → IsLocalNodeIdle() == false → 永远不 idle
    │
    ▼
Autoscaler 无法 drain → 云实例不释放 → 持续计费
```

### 1.3 四个关键阻塞点

| 编号 | 阻塞点 | 所在层级 | 根因 |
|------|--------|---------|------|
| 1 | `_release_running_actor()` 只 `del` 引用，不调 `ray.kill()` | Python (Ray Data) | 为保留 lineage reconstruction 能力 |
| 2 | Actor worker 在 `leased_workers_` 中，drain 的 `CancelLeasesWithoutReply()` 不碰它 | C++ (Raylet) | CancelLeases 只处理排队的 lease，不处理已 grant 的 |
| 3 | `IsLocalNodeIdle()` 不感知 `pinned_objects_`，节点可能带着大量对象被关闭 | C++ (Raylet) | 当前 idle 检查只看 resources 和 work footprints |
| 4 | 缺乏从 Ray Data 到 Autoscaler 的主动缩容信号 | Python (Ray Data) | Ray Data 不参与集群缩容决策 |

---

## 2. 目标与非目标

### 2.1 目标

1. GPU operator 完成后，actor 被及时终止，GPU 资源立即归还
2. 节点 drain 时，pinned objects 被安全迁移或等待消费后再关闭
3. Ray Data 主动向 Autoscaler 发出 drain 信号，加速 GPU 节点回收
4. 整个链路端到端可控，从 operator 完成到云实例释放

### 2.2 非目标

1. 不修改 Autoscaler 核心调度逻辑
2. 不修改 GCS 的 drain 协议
3. 不处理 CPU-only operator 的缩容优化
4. 不修改对象的 Owner 或引用计数机制

---

## 3. 问题根因分析

### 3.1 Actor 不退出的根因链

```
ActorPoolMapOperator.all_inputs_done()
    │
    ▼
_ActorPool.scale(downscale request)
    │
    ▼
_ActorPool._release_running_actor(actor)
    │
    ├── del self._running_actors[actor]    ← 删除内部引用
    ├── del self._actor_to_logical_id[actor]
    └── 不调用 ray.kill()                  ← 关键：Actor handle 可能被其他地方持有
    │
    ▼
Actor 不退出的条件：
    ├── ObjectRef callback 持有 actor handle → 引用计数 > 0
    ├── 或 pending task result 引用 actor    → GC 不回收
    └── Actor worker 进程不退出 → GPU 资源不归还
```

### 3.2 节点不 idle 的根因链

```
Actor worker 进程仍在运行
    │
    ▼
Worker 仍在 leased_workers_ 中
    │
    ▼
NODE_WORKERS WorkFootprint 标记为 busy
    │
    ▼
IsLocalNodeIdle() 返回 false
    │
    ▼
Drain 请求被拒绝（IDLE_TERMINATION 要求节点 idle）
    │
    ▼
或 drain 被接受（PREEMPTION），但 shutdown 条件不满足
```

### 3.3 Pinned Objects 导致数据丢失的根因

```
节点进入 drain 状态 → IsLocalNodeIdle() == true
    │
    ▼
OnResourceOrStateChanged() 检查通过
    │
    ▼
shutdown_raylet_gracefully_() 被调用
    │
    ▼
但此时 pinned_objects_ 非空！
    ├── 下游 operator 还没消费这些对象
    ├── 节点关闭 → Plasma Store 销毁 → 对象丢失
    └── 需要通过 lineage 重算恢复（成本高、延迟大）
```

---

## 4. 方案总体设计

### 4.1 四层防御架构

```
Ray Data 检测到 GPU stage 完成
  │
  ▼
Layer 4: 主动触发 — kill actor + 通过公开 SDK API 请求 drain
  │       (Python: GPUNodeDrainManager + DefaultActorAutoscaler)
  │       Tick 1: kill actors → 记录 _pending_drain_nodes
  │       Tick 2: 通过 ray.autoscaler.sdk.request_node_drain() 发送 drain
  ▼
Layer 3: Actor 强制释放 — ray.kill(no_restart=True)
  │       (Python: _ActorPool.force_release_actors_on_node)
  │       释放 GPU 资源，清空 leased_workers_
  ▼
Layer 2: 对象感知关闭 — 30s 等自然消费，超时后主动迁移
  │       (C++: LocalObjectManager.MigrateAllPinnedObjects)
  │       复用已有 Phase 1/2 的 Push + Pin 转移机制
  ▼
Layer 1: Autoscaler 释放 — Raylet shutdown → GCS Dead → Reconciler → 云平台回收
          (C++: LocalResourceManager.OnResourceOrStateChanged)
```

### 4.2 设计原则

1. **分层解耦**：每层独立工作，上层加速、下层兜底
2. **最小侵入**：复用已有的 Push/Pin 转移/drain 机制
3. **安全第一**：宁可慢一点也不丢数据，最终兜底走 Recovery
4. **可配置**：每层都有开关和超时配置
5. **层次抽象**（v1.2）：应用层通过 `ray.autoscaler.sdk` 公开 API 与集群基础设施交互，不直接调用内部 GCS 接口

---

## 5. Layer 1：Raylet 对象感知的 Drain 关闭（C++）

### 5.1 核心问题

当前关闭条件（`local_resource_manager.cc`）：

```cpp
if (IsLocalNodeDraining() && IsLocalNodeIdle()) {
    shutdown_raylet_gracefully_(...);  // 不管 pinned_objects_
}
```

节点 idle + draining 就直接关闭，完全不检查是否还有 pinned objects。如果此时有下游还没消费的对象，关闭会导致对象丢失。

### 5.2 修改方案

#### 5.2.1 `local_resource_manager.h` — 新增回调和状态

```cpp
class LocalResourceManager {
 public:
  // 设置查询是否还有 pinned 对象的回调
  void SetHasPinnedObjects(std::function<bool(void)> has_pinned_objects);

  // 设置触发对象迁移的回调
  void SetTriggerObjectMigration(
      std::function<void(std::function<void()>)> trigger_object_migration);

  // 定时器调用的公开接口，重新检查 drain 状态
  void RecheckDrainState() { OnResourceOrStateChanged(); }

 private:
  // 查询是否还有 pinned 对象
  std::function<bool(void)> has_pinned_objects_;
  // 触发对象迁移
  std::function<void(std::function<void()>)> trigger_object_migration_;
  // drain 接受时间（用于计算超时）
  std::optional<absl::Time> drain_accepted_time_;
  // 是否已触发迁移（防重入）
  bool drain_migration_triggered_ = false;
};
```

#### 5.2.2 `local_resource_manager.cc` — 修改关闭逻辑

```cpp
void LocalResourceManager::OnResourceOrStateChanged() {
  if (IsLocalNodeDraining() && IsLocalNodeIdle()) {
    bool ready_to_shutdown = true;

    // 新增：对象感知检查
    if (RayConfig::instance().enable_object_aware_drain() &&
        has_pinned_objects_ && has_pinned_objects_()) {
      ready_to_shutdown = false;

      // 1. 记录 drain 开始时间
      if (!drain_accepted_time_.has_value()) {
        drain_accepted_time_ = now_fn_();
        RAY_LOG(INFO) << "Node is idle and draining but has pinned objects. "
                      << "Waiting for objects to be consumed or migrated.";
      }

      // 2. 检查是否超时
      auto elapsed = now_fn_() - *drain_accepted_time_;
      int64_t timeout_ms =
          RayConfig::instance().gpu_node_object_drain_timeout_ms();

      if (elapsed >= absl::Milliseconds(timeout_ms)) {
        // 3. 超时后触发主动迁移
        if (!drain_migration_triggered_ && trigger_object_migration_) {
          drain_migration_triggered_ = true;
          trigger_object_migration_([this]() {
            OnResourceOrStateChanged();  // 迁移发起后回调
          });
        }
      }

      // 4. 检查对象是否已全部清空
      if (!has_pinned_objects_()) {
        ready_to_shutdown = true;
      }
    }

    if (ready_to_shutdown) {
      // 所有条件满足，执行关闭
      rpc::NodeDeathInfo node_death_info = DeathInfoFromDrainRequest();
      shutdown_raylet_gracefully_(std::move(node_death_info));
    }
  }

  // 关键：无论是否等待 drain，都必须执行 version++ 和 subscriber 通知
  ++version_;
  if (resource_change_subscriber_ == nullptr) return;
  resource_change_subscriber_(ToNodeResources());
}
```

**重要设计约束**：不使用 early return，确保 `++version_` 和 `resource_change_subscriber_` 在所有分支下都被执行。否则在 drain 等待期间，资源变化事件不会被传播到集群调度视图。

**状态机**（使用 flag 而非 early return，保证 version/subscriber 通知始终执行）：

```
draining+idle
    │
    ├── has_pinned_objects == false → ready_to_shutdown = true → shutdown
    │
    └── has_pinned_objects == true → ready_to_shutdown = false
        │
        ├── elapsed < 30s → 等待（下次定时器触发重新检查）
        │
        ├── elapsed >= 30s + !migration_triggered
        │   → trigger_object_migration_()
        │   → 等待（下次定时器触发重新检查）
        │
        ├── migration_triggered + still has objects → 等待
        │
        └── !has_objects → ready_to_shutdown = true → shutdown
    │
    ▼ （无论哪个分支都执行）
    ++version_
    resource_change_subscriber_(ToNodeResources())
```

#### 5.2.3 `node_manager.cc` — 装配回调和定时器

**构造函数装配**：

```cpp
// Wire object-aware drain callbacks.
if (RayConfig::instance().enable_object_aware_drain()) {
  cluster_resource_scheduler_.GetLocalResourceManager().SetHasPinnedObjects(
      [this]() { return local_object_manager_.HasPinnedObjects(); });
  cluster_resource_scheduler_.GetLocalResourceManager().SetTriggerObjectMigration(
      [this](std::function<void()> on_complete) {
        MigratePinnedObjectsForDrain(std::move(on_complete));
      });
}
```

**HandleDrainRaylet 中启动定时器**：

```cpp
if (is_drain_accepted) {
  // 已有：cancel lease 逻辑
  auto cancelled_works = local_lease_manager_.CancelLeasesWithoutReply(...);
  // ...

  // 新增：启动周期性检查定时器（每 5s）
  if (RayConfig::instance().enable_object_aware_drain()) {
    periodical_runner_->RunFnPeriodically(
        [this]() {
          cluster_resource_scheduler_.GetLocalResourceManager()
              .RecheckDrainState();
        },
        /*period_ms=*/5000,
        "NodeManager.ObjectDrainCheck");
  }
}
```

**定时器作用**：drain 接受后，即使没有资源变化事件，也会每 5 秒重新检查 drain 状态，推动对象自然消费 → 超时迁移 → 最终关闭的流程。

---

## 6. Layer 2：对象迁移实现（C++）

### 6.1 接口设计

在 `LocalObjectManagerInterface` 中新增两个纯虚方法：

```cpp
class LocalObjectManagerInterface {
 public:
  /// 是否还有 pinned 对象
  virtual bool HasPinnedObjects() const = 0;

  /// 迁移所有 pinned 对象到其他节点
  virtual void MigrateAllPinnedObjects(
      std::function<NodeID()> select_target,
      std::function<void(const ObjectID&, const NodeID&)> push_object,
      std::function<void()> on_complete) = 0;
};
```

### 6.2 `LocalObjectManager::HasPinnedObjects`

```cpp
bool HasPinnedObjects() const override {
  return !pinned_objects_.empty();
}
```

直接检查 `pinned_objects_` map 是否为空。`pinned_objects_` 记录了所有通过 `PinObjectsAndWaitForFree` Pin 在本节点的对象。

### 6.3 `LocalObjectManager::MigrateAllPinnedObjects`

```cpp
void LocalObjectManager::MigrateAllPinnedObjects(
    std::function<NodeID()> select_target,
    std::function<void(const ObjectID&, const NodeID&)> push_object,
    std::function<void()> on_complete) {

  // 1. 快照当前 pinned 对象列表
  std::vector<ObjectID> objects_to_migrate;
  for (const auto &[object_id, _] : pinned_objects_) {
    objects_to_migrate.push_back(object_id);
  }

  if (objects_to_migrate.empty()) {
    on_complete();
    return;
  }

  RAY_LOG(INFO) << "Migrating " << objects_to_migrate.size()
                << " pinned objects for drain";

  // 2. 引用计数跟踪完成状态
  auto remaining = std::make_shared<std::atomic<int64_t>>(
      objects_to_migrate.size());

  // 3. 逐个推送
  for (const auto &object_id : objects_to_migrate) {
    NodeID target = select_target();
    if (target.IsNil()) {
      RAY_LOG(WARNING) << "No migration target for " << object_id;
      if (remaining->fetch_sub(1) == 1) on_complete();
      continue;
    }
    push_object(object_id, target);
    if (remaining->fetch_sub(1) == 1) on_complete();
  }
}
```

### 6.4 迁移后的 Unpin 机制

Push 本身是 fire-and-forget。对象到达目标节点后的处理链路：

```
Push(object_id, target)
    │
    ▼
目标 Raylet 接收对象 → ObjectManager::HandleObjectAdded
    │
    ▼
目标 Raylet 通知 Owner → ReportObjectAdded → Owner 更新 locations
    │
    ▼
已有的 MaybeTriggerPinTransfer 检测到稳定节点有副本
    │
    ▼
Pin 转移：Owner 向目标节点发 PinObjectIDs，向源节点发 Eviction
    │
    ▼
源节点 Eviction → ReleaseFreedObject → pinned_objects_ 移除
    │
    ▼
pinned_objects_ 减少 → 定时器重新检查 → 最终为空 → shutdown
```

### 6.5 `NodeManager::SelectMigrationTarget`

与已有的 `SelectStableNode()` 类似，但排除条件不同：

```cpp
NodeID NodeManager::SelectMigrationTarget() const {
  const auto &resource_view =
      cluster_resource_scheduler_.GetClusterResourceManager()
          .GetResourceView();
  std::vector<NodeID> candidates;
  for (const auto &[scheduling_node_id, node] : resource_view) {
    NodeID node_id = NodeID::FromBinary(scheduling_node_id.Binary());
    if (node_id == self_node_id_) continue;
    // 排除正在 drain 的节点（与 SelectStableNode 的区别）
    if (node.GetLocalView().is_draining) continue;
    candidates.push_back(node_id);
  }
  if (candidates.empty()) return NodeID::Nil();
  std::uniform_int_distribution<size_t> dist(0, candidates.size() - 1);
  return candidates[dist(rng_)];
}
```

| 方法 | 用途 | 排除条件 |
|------|------|---------|
| `SelectStableNode()` | Phase 1 对象复制 | 排除 preemptible 节点 |
| `SelectMigrationTarget()` | Drain 时对象迁移 | 排除 draining 节点 |

### 6.6 `NodeManager::MigratePinnedObjectsForDrain`

```cpp
void NodeManager::MigratePinnedObjectsForDrain(
    std::function<void()> on_complete) {
  local_object_manager_.MigrateAllPinnedObjects(
      /*select_target=*/[this]() { return SelectMigrationTarget(); },
      /*push_object=*/[this](const ObjectID &obj_id, const NodeID &target) {
        object_manager_.Push(obj_id, target);
      },
      std::move(on_complete));
}
```

---

## 7. Layer 3：Actor 强制释放（Python）

### 7.1 核心问题

`_release_running_actor()` 用 `del` 依赖 GC，不调 `ray.kill()`：

```python
def _release_running_actor(self, actor):
    # NOTE: By default, we remove references to the actor and let
    # ref counting garbage collect the actor, instead of using ray.kill.
    #
    # Otherwise, actor cannot be reconstructed for the purposes of
    # produced object's lineage reconstruction.
    del self._running_actors[actor]
    del self._actor_to_logical_id[actor]
```

**问题**：如果有 pending ObjectRef callback 持有 actor handle → actor 不退出 → GPU 资源不归还 → `leased_workers_` 不清空 → `NODE_WORKERS` busy → 永远不 idle。

### 7.2 `_ActorState` 新增 `is_draining` 字段

```python
@dataclass
class _ActorState:
    num_tasks_in_flight: int
    actor_location: str
    is_restarting: bool
    is_draining: bool = False  # 新增
```

### 7.3 `_ActorPool.force_release_actors_on_node()`

```python
def force_release_actors_on_node(
    self,
    node_id: str,
) -> List[ray.actor.ActorHandle]:
    """Force release all actors on a node for drain.

    Immediately kills actors with ray.kill(no_restart=True).
    Non-blocking, safe to call from the scheduling loop.
    """
    killed = []
    for actor, state in list(self._running_actors.items()):
        if state.actor_location == node_id:
            ray.kill(actor, no_restart=True)
            self._total_num_tasks_in_flight -= state.num_tasks_in_flight
            if state.num_tasks_in_flight > 0:
                self._num_active_actors -= 1
            if state.is_restarting:
                self._num_restarting_actors -= 1
            del self._running_actors[actor]
            del self._actor_to_logical_id[actor]
            killed.append(actor)
    return killed
```

**设计决策**：

- 使用 `ray.kill(no_restart=True)` 而不是 `del` —— 确保 actor worker 进程立即退出
- 非阻塞 —— 不使用 `time.sleep` 等待 grace period，避免阻塞 scheduling loop
- 立即更新内部统计 —— `_total_num_tasks_in_flight`、`_num_active_actors` 等
- `on_task_completed` 安全 —— 被 kill 的 actor 的 in-flight task 回调会触发 `on_task_completed`，此时 actor 已从 `_running_actors` 删除，需要 guard check：

```python
def on_task_completed(self, actor):
    # Actor may have been force-released during drain; skip if removed.
    if actor not in self._running_actors:
        return
    # ... 正常处理 ...
```

### 7.4 `get_available_actors()` 排除 draining actor

```python
def get_available_actors(self):
    if self._pending_scale_down_count <= 0:
        return {
            actor: state
            for actor, state in self._running_actors.items()
            if not state.is_draining  # 新增过滤
        }
    # ... 已有的 pending_scale_down 排除逻辑 ...
    return {
        actor: state
        for actor, state in self._running_actors.items()
        if actor not in actors_to_exclude
        and not state.is_draining  # 新增过滤
    }
```

### 7.5 关于 lineage reconstruction 的权衡

原代码注释说不用 `ray.kill()` 是为了保留 lineage reconstruction 能力。我们的方案用 `ray.kill(no_restart=True)` 会破坏这一点。

**但这是可接受的**，因为：

1. 对象已经通过 Phase 1 复制到了其他节点（如果是 preemptible 节点）
2. drain 时的 Layer 2 会主动迁移剩余对象
3. 即使迁移失败，Recovery 仍然可以通过 lineage 重算（task 会被重新调度到其他节点）
4. 这个 kill 只在 operator 完全结束后才触发，此时不再需要 actor 产出新数据

---

## 8. Layer 4：Ray Data 主动触发（Python）

> **v1.2 重构**：本层经过架构改进，原有的直接 GCS 调用已替换为通过 `ray.autoscaler.sdk.request_node_drain()` 公开 API。同时引入 tick 延迟机制解决 kill→idle 竞态。详细分析见 [Section 16](#16-layer-4-架构改进公开-api-重构v12)。

### 8.1 公开 API：`ray.autoscaler.sdk.request_node_drain()`

v1.2 新增公开 API，与已有的 `request_resources()` 形成对称设计（scale-up vs scale-down）。

**SDK 层**：`python/ray/autoscaler/sdk/sdk.py`

```python
@DeveloperAPI
def request_node_drain(
    node_id: bytes,
    reason: str = "",
    deadline_remaining_seconds: Optional[int] = None,
) -> bool:
    """Request the autoscaler to drain a node.

    Sends an IDLE_TERMINATION drain request to the GCS for the specified node.
    The request is advisory -- the Raylet will only accept the drain if the
    node is currently idle (no active workers). If rejected, the caller can
    rely on the Autoscaler's native idle detection as a fallback.

    This is the scale-down counterpart to ``request_resources()`` (scale-up).

    Args:
        node_id: The Ray node ID (bytes) to drain.
        reason: Human-readable reason for the drain request.
        deadline_remaining_seconds: Optional deadline in seconds. If None,
            no deadline is set (node drains gracefully).

    Returns:
        True if the drain request was accepted by the GCS, False if rejected
        (e.g. because the node still has active workers).
    """
    return commands.request_node_drain(node_id, reason, deadline_remaining_seconds)
```

**实现层**：`python/ray/autoscaler/_private/commands.py`

```python
def request_node_drain(
    node_id: bytes,
    reason: str = "",
    deadline_remaining_seconds: Optional[int] = None,
) -> bool:
    if not ray.is_initialized():
        raise RuntimeError("Ray is not initialized yet")
    import ray._private.worker
    import ray._raylet
    from ray.core.generated import autoscaler_pb2

    gcs_client = ray._raylet.GcsClient(
        address=ray._private.worker.global_worker.gcs_client.address
    )
    deadline_timestamp_ms = 0
    if deadline_remaining_seconds is not None:
        import time as _time
        deadline_timestamp_ms = int(
            (_time.time() + deadline_remaining_seconds) * 1000
        )
    is_accepted, rejection_msg = gcs_client.drain_node(
        node_id,
        autoscaler_pb2.DrainNodeReason.Value(
            "DRAIN_NODE_REASON_IDLE_TERMINATION"
        ),
        reason.encode() if isinstance(reason, str) else reason,
        deadline_timestamp_ms,
    )
    return is_accepted
```

**调用链路**：

```
ray.autoscaler.sdk.request_node_drain()     ← 公开 @DeveloperAPI
  → commands.request_node_drain()            ← 私有实现
    → GcsClient.drain_node(IDLE_TERMINATION) ← GCS RPC
```

与 `request_resources()` 的对称关系：

| API | 方向 | 作用 |
|-----|------|------|
| `request_resources(bundles=[{"GPU": 1}])` | Scale-up | 告诉 Autoscaler "我需要这些资源" |
| `request_node_drain(node_id, reason)` | Scale-down | 告诉 Autoscaler "这个节点我不需要了" |

### 8.2 `GPUNodeDrainManager`

文件：`python/ray/data/_internal/execution/gpu_node_drain_manager.py`

```python
class GPUNodeDrainManager:
    """Coordinates GPU node drain requests after actor release.

    After Ray Data force-kills GPU actors on a node, this manager sends
    an IDLE_TERMINATION drain request via the public autoscaler SDK.
    The drain is only accepted if the node is actually idle (no other
    workers running), providing a built-in safety net for multi-operator
    scenarios. If rejected, the Autoscaler's native idle detection
    serves as a fallback.
    """

    def __init__(self, enabled: bool = True):
        self._enabled = enabled
        self._drained_nodes: Set[str] = set()  # 已接受 drain 的节点

    def request_drain_for_node(self, node_id: str) -> bool:
        """Kill 完 actor 后，请求 drain 该节点。

        使用 IDLE_TERMINATION 模式，如果节点上还有其他 operator 的
        worker 在运行，Raylet 会拒绝请求（内建安全机制）。
        如果被拒绝，Autoscaler 的原生空闲检测会作为兜底。
        """
        if not self._enabled or node_id in self._drained_nodes:
            return False
        return self._request_drain(node_id)

    def _request_drain(self, node_id: str) -> bool:
        """通过公开 autoscaler SDK 发送 drain 请求。"""
        try:
            from ray.autoscaler.sdk import request_node_drain

            node_id_bytes = (
                node_id.encode() if isinstance(node_id, str) else node_id
            )
            is_accepted = request_node_drain(
                node_id_bytes,
                reason="Ray Data GPU actor scale-down",
            )
            if is_accepted:
                self._drained_nodes.add(node_id)
            return is_accepted
        except Exception:
            logger.warning(f"Failed to drain GPU node {node_id}", exc_info=True)
            return False
```

**v1.1 → v1.2 变更**：

| 项目 | v1.1 | v1.2 |
|------|------|------|
| GCS 调用方式 | 直接 `gcs_client.drain_node()` | 通过 `ray.autoscaler.sdk.request_node_drain()` |
| GCS 客户端管理 | 延迟初始化 `_gcs_client` | 无（委托给 SDK） |
| 返回值 | `None` | `bool`（是否被接受） |
| 依赖 | `ray._raylet`, `ray._private.worker`, `autoscaler_pb2` | 仅 `ray.autoscaler.sdk` |

### 8.3 集成到 `DefaultActorAutoscaler`（Tick 延迟 Drain）

v1.2 的核心改进：将 actor kill 和 drain 请求分离到不同的调度 tick，利用 tick 间隔作为天然延迟窗口，解决 kill→idle 竞态。

```python
class DefaultActorAutoscaler(ActorAutoscaler):
    def __init__(self, ..., gpu_drain_manager=None):
        ...
        self._gpu_drain_manager = gpu_drain_manager
        self._drained_ops: set = set()  # 幂等性保护
        # 待 drain 的节点 — kill 时填入，下一个 tick 处理。
        # 利用调度 tick 间隔作为天然延迟窗口，无需 sleep 或线程。
        self._pending_drain_nodes: set = set()

    def try_trigger_scaling(self):
        for op, state in self._topology.items():
            for pool in op.get_autoscaling_actor_pools():
                # 关键：force_release 必须在 scale() 之前执行
                # 避免 scale 的 del-based release 与 ray.kill 竞争
                if (
                    self._gpu_drain_manager is not None
                    and op.has_execution_finished()
                    and id(op) not in self._drained_ops
                ):
                    self._force_release_actors(op, pool)
                    self._drained_ops.add(id(op))
                else:
                    pool.scale(self._derive_target_scaling_config(pool, op, state))

        # 在 tick 末尾处理 drain 请求。
        # 此时本 tick 或之前 tick kill 的 worker 大概率已退出，
        # 节点已 idle，drain 请求更容易被接受。
        if self._pending_drain_nodes and self._gpu_drain_manager is not None:
            for node_id in list(self._pending_drain_nodes):
                self._gpu_drain_manager.request_drain_for_node(node_id)
                self._pending_drain_nodes.discard(node_id)
```

**Tick 延迟机制**：

```
Tick N:
  ├── _force_release_actors(op, pool)
  │   ├── ray.kill(actor_1)  → Worker 开始退出（异步）
  │   ├── ray.kill(actor_2)  → Worker 开始退出（异步）
  │   └── _pending_drain_nodes.add(node_A)
  │
  └── 处理 pending drain → 无（本 tick 刚加入的节点也在处理范围内，
      但此时位于 tick 末尾，kill 到此已有微秒~毫秒级延迟）

Tick N+1（如果 Tick N 末尾 drain 被拒绝或 pending 跨 tick）:
  └── 处理 pending drain
      └── request_node_drain(node_A) → Worker 已退出 → 接受
```

**v1.1 → v1.2 变更**：

| 项目 | v1.1 | v1.2 |
|------|------|------|
| 方法名 | `_force_release_and_drain()` | `_force_release_actors()` |
| Drain 时机 | kill 后立即同步调用 | kill 时记录，tick 末尾处理 |
| 竞态处理 | 无（kill 后立即 drain 可能被拒绝） | tick 间隔提供天然延迟窗口 |
| 新增状态 | 无 | `_pending_drain_nodes: set` |

### 8.4 `_force_release_actors` 实现

```python
def _force_release_actors(self, op, actor_pool):
    """Force release GPU actors and record nodes for drain on next tick."""
    from ...operators.actor_pool_map_operator import _ActorPool

    if not isinstance(actor_pool, _ActorPool):
        return

    # 只对 GPU operator 触发 drain
    if actor_pool.per_actor_resource_usage().gpu <= 0:
        return

    # 按节点聚合 actor
    actors_by_node = {}
    for actor, state in list(actor_pool.running_actors().items()):
        actors_by_node.setdefault(state.actor_location, []).append(actor)

    for node_id in actors_by_node:
        actor_pool.force_release_actors_on_node(node_id)
        # 记录待 drain 节点，在 tick 末尾处理（延迟让 worker 退出）
        self._pending_drain_nodes.add(node_id)
```

**执行顺序约束**（v1.1 保留）：`_force_release_actors` 与 `scale()` 互斥执行：
- 如果要 force release，跳过 `scale()` —— 避免 `scale` 的 `_release_running_actor`（del 引用）与 `force_release`（ray.kill + del）竞争同一个 actor
- `_drained_ops` set 确保每个 operator 只触发一次

**GPU 资源检查**（v1.1 保留）：只有 `per_actor_resource_usage().gpu > 0` 的 operator 才触发 drain，避免 CPU-only actor pool 节点被不必要地 drain。

### 8.5 集成到 `StreamingExecutor`

```python
class StreamingExecutor:
    def _get_gpu_drain_manager(self):
        """按配置决定是否创建 GPUNodeDrainManager。"""
        if not self._data_context.gpu_node_proactive_drain_enabled:
            return None
        from ...gpu_node_drain_manager import GPUNodeDrainManager
        return GPUNodeDrainManager(enabled=True)

    def execute(self, dag, ...):
        ...
        self._actor_autoscaler = create_actor_autoscaler(
            self._topology,
            self._resource_manager,
            config=self._data_context.autoscaling_config,
            gpu_drain_manager=self._get_gpu_drain_manager(),  # 新增
        )
```

### 8.6 `create_actor_autoscaler` 工厂函数更新

```python
def create_actor_autoscaler(
    topology, resource_manager, config,
    gpu_drain_manager=None,  # 新增参数
) -> ActorAutoscaler:
    if config.autoscaler_type == ActorAutoscalerType.DISABLED:
        return NoOpActorAutoscaler(topology, resource_manager)
    else:
        return DefaultActorAutoscaler(
            topology, resource_manager,
            config=config,
            gpu_drain_manager=gpu_drain_manager,  # 透传
        )
```

### 8.7 `DataContext` 配置

```python
@dataclass
class DataContext:
    # GPU 节点主动 drain
    gpu_node_proactive_drain_enabled: bool = True
```

---

## 9. 完整时序图

### 9.1 快速路径（Ray Data 主动触发 — v1.2 Tick 延迟）

```
t0: GPU operator all_inputs_done()
     │
t1:  DefaultActorAutoscaler.try_trigger_scaling()  [Tick N]
     ├── has_execution_finished() == True
     └── _force_release_actors(op, pool)
         ├── force_release_actors_on_node(node_A)
         │   └── ray.kill(actor, no_restart=True)
         │       → Worker 开始退出（异步）
         └── _pending_drain_nodes.add(node_A)
     │
t2:  try_trigger_scaling() tick 末尾  [同 Tick N]
     └── 处理 _pending_drain_nodes
         └── gpu_drain_manager.request_drain_for_node(node_A)
             └── ray.autoscaler.sdk.request_node_drain()  ← 公开 API
                 └── commands.request_node_drain()
                     └── GcsClient.drain_node(IDLE_TERMINATION)
     │
t3:  Raylet HandleDrainRaylet
     ├── IsLocalNodeIdle() == True（Worker 已退出）→ 接受
     ├── SetLocalNodeDraining()
     ├── CancelLeasesWithoutReply()
     └── 启动 5s 定时器 (ObjectDrainCheck)
     │
t4:  OnResourceOrStateChanged()
     ├── draining=true, idle=true
     ├── HasPinnedObjects() == true → 不关闭
     │   → drain_accepted_time_ = now
     │
t4~t34: 定时器每 5s 重新检查
     ├── 下游消费了一些对象 → pinned_objects_ 减少
     │   （每次检查 elapsed < 30s → 继续等待）
     │
t34: 30s 超时到达，还有 pinned 对象
     └── MigratePinnedObjectsForDrain()
         ├── SelectMigrationTarget() → 选非 draining 节点
         ├── Push 对象到目标节点
         ├── 目标节点 ReportObjectAdded → Owner 更新 locations
         ├── MaybeTriggerPinTransfer → Pin 转移
         └── Owner eviction → 本节点 unpin → pinned_objects_ 减少
     │
t35+: pinned_objects_ 清空
     └── shutdown_raylet_gracefully_()
         → UnregisterSelf() → GCS 标记 Dead
         → Reconciler → RAY_STOPPED → TERMINATING → 云平台回收实例
```

### 9.2 慢速路径（Autoscaler 空闲检测兜底 — 方案 A fallback）

```
[GPU actor 被 kill 后，drain 请求被拒绝（kill→idle 竞态）]
     │
     ▼
不重试 — 最坏情况退化为 Autoscaler 原生空闲检测：
     │
     ▼
idle_timeout_s 后 Autoscaler Scheduler._enforce_idle_termination()
  → RayStopper._drain_ray_node(IDLE_TERMINATION)
  → 进入与快速路径相同的 Raylet drain 流程
```

### 9.3 对象迁移详细流程

```
MigratePinnedObjectsForDrain(on_complete)
    │
    ▼
LocalObjectManager.MigrateAllPinnedObjects(
    select_target,   ← NodeManager::SelectMigrationTarget
    push_object,     ← ObjectManager::Push
    on_complete)
    │
    ├── 遍历 pinned_objects_ 快照
    │   │
    │   ├── select_target() → NodeID (非 draining 节点)
    │   │   └── 如果 Nil → 跳过，记 warning
    │   │
    │   └── push_object(obj_id, target)
    │       └── ObjectManager::Push → 异步 fire-and-forget
    │
    └── 所有 push 发起后 → on_complete()
        └── OnResourceOrStateChanged() → 重新检查

                        ┌───────────────────┐
Push 到达目标节点 ───→ │ ObjectManager      │
                        │ HandleObjectAdded  │
                        └────────┬──────────┘
                                 │
                                 ▼
                        ┌───────────────────┐
                        │ Owner (Driver)     │
                        │ ReportObjectAdded  │
                        │ → 更新 locations   │
                        └────────┬──────────┘
                                 │
                    ┌────────────┴────────────┐
                    ▼                         ▼
            ┌──────────────┐        ┌────────────────┐
            │ 目标 Raylet   │        │ 源 Raylet       │
            │ PinObjectIDs │        │ Eviction        │
            │ (新 pin)     │        │ → ReleaseFreed  │
            └──────────────┘        │ → unpin         │
                                    │ → pinned_objects_│
                                    │   减少           │
                                    └────────────────┘
```

---

## 10. 竞态和边界情况处理

| 场景 | 处理方式 |
|------|---------|
| Actor kill 后 drain 被拒绝（Worker 未退出） | v1.2：tick 延迟让 worker 退出后再 drain。如果仍被拒绝，不重试，依赖 Autoscaler 原生空闲检测兜底（方案 A fallback） |
| 多个 operator 共享同一 GPU 节点 | IDLE_TERMINATION 的 Raylet 端拒绝逻辑保障安全：如果节点上还有其他 operator 的 worker，drain 被拒绝 |
| 对象迁移目标节点也在 drain | `SelectMigrationTarget()` 排除 `is_draining` 的节点 |
| 迁移完成前 Owner 已经 evict 了对象 | Push 的目标 Plasma 上对象到达后会 Seal，即使原节点 unpin 了也不影响。竞态情况下 Push 可能失败，忽略即可 |
| 所有候选节点都在 drain，无迁移目标 | `SelectMigrationTarget()` 返回 Nil，跳过该对象。最终依赖 deadline 强制 shutdown + Recovery |
| 对象很大，迁移超时 | 如果有 drain deadline（`deadline_timestamp_ms`），到期后无论是否迁移完毕都 shutdown，丢失对象走 Recovery |
| `_force_release_actors` 被重复调用 | `_drained_ops` set 防止重入，每个 operator 只处理一次 |
| `force_release_actors_on_node` 对已释放的 actor | 迭代 `list(self._running_actors.items())` 快照，只处理当前存在的 actor |
| `try_trigger_scaling` 先 scale down 再 force release | v1.1 修复：force release 与 scale 互斥执行，避免竞争 |
| 被 kill 的 actor 的 in-flight task 回调 | `on_task_completed` 添加 guard：`if actor not in self._running_actors: return` |
| drain 被拒绝后其他 operator 完成 | `_drained_nodes` 只记录被接受的 drain，后续 operator 可重新请求 |
| CPU-only operator 触发不必要的 drain | v1.1 修复：检查 `per_actor_resource_usage().gpu > 0` |
| 定时器启动后 drain 被取消 | 当前 drain 不可取消（一旦 SetLocalNodeDraining 就不回退）。定时器是轻量检查，如果 `!IsLocalNodeDraining()` 则不执行任何逻辑 |
| `_pending_drain_nodes` 跨 tick 残留 | 正常行为：如果某 tick 末尾 drain 被拒绝，节点从 pending 集合移除（不重试），依赖 Autoscaler 兜底 |

---

## 11. 配置项设计

### 11.1 C++ 配置（ray_config_def.h）

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `enable_object_aware_drain` | bool | `true` | 是否启用对象感知 drain。开启后 drain 时会等 pinned objects 被消费或迁移 |
| `gpu_node_object_drain_timeout_ms` | int64_t | `30000` | 等待 pinned objects 自然消费的超时时间（ms）。超时后触发主动迁移 |

### 11.2 Python 配置（DataContext）

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `gpu_node_proactive_drain_enabled` | bool | `True` | 是否启用 Ray Data 主动 drain GPU 节点 |

### 11.3 配置交互关系

```
gpu_node_proactive_drain_enabled = True
    → 创建 GPUNodeDrainManager
    → DefaultActorAutoscaler 在 operator 完成后主动 kill actor
    → tick 延迟后通过 ray.autoscaler.sdk.request_node_drain() 请求 drain
    → 如果被拒绝，不重试，依赖 Autoscaler 原生空闲检测兜底

enable_object_aware_drain = True
    → Raylet drain 时检查 pinned objects
    → 等 30s (gpu_node_object_drain_timeout_ms) 让对象被消费
    → 超时后触发 MigrateAllPinnedObjects

两者独立工作：
- 只开 Python 端：actor 被 kill，drain 请求通过公开 API 发出，但 Raylet 可能带着 objects 直接关闭
- 只开 C++ 端：Raylet 会等 objects 迁移，但 actor 不会被主动 kill，要等 Autoscaler idle 检测
- 两者都开：完整链路，最快回收
```

---

## 12. 关键文件清单

### 12.1 C++ 层

| 文件 | 改动内容 | 新增行数 |
|------|---------|---------|
| `src/ray/common/ray_config_def.h` | 新增 `enable_object_aware_drain`, `gpu_node_object_drain_timeout_ms` | +9 |
| `src/ray/raylet/scheduling/local_resource_manager.h` | 新增 callback 成员、setter 方法、`RecheckDrainState()` | +31 |
| `src/ray/raylet/scheduling/local_resource_manager.cc` | 修改 `OnResourceOrStateChanged()` 加入对象感知逻辑 | +35 |
| `src/ray/raylet/local_object_manager_interface.h` | 接口新增 `HasPinnedObjects()`, `MigrateAllPinnedObjects()` | +9 |
| `src/ray/raylet/local_object_manager.h` | 实现接口方法声明 | +12 |
| `src/ray/raylet/local_object_manager.cc` | 实现 `MigrateAllPinnedObjects()` | +38 |
| `src/ray/raylet/node_manager.h` | 新增 `SelectMigrationTarget()`, `MigratePinnedObjectsForDrain()` | +8 |
| `src/ray/raylet/node_manager.cc` | 实现方法、装配回调、启动定时器 | +55 |
| `src/ray/raylet/tests/node_manager_test.cc` | 更新 `FakeLocalObjectManager` 添加 stub | +9 |

### 12.2 Python 层

| 文件 | 改动内容 | 新增行数 |
|------|---------|---------|
| `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py` | `_ActorState.is_draining`、`force_release_actors_on_node()`、`get_available_actors()` 过滤 | +41 |
| `python/ray/data/_internal/execution/gpu_node_drain_manager.py` | **新文件** —— `GPUNodeDrainManager` 类，通过公开 SDK API 发送 drain | +65 |
| `python/ray/data/_internal/actor_autoscaler/default_actor_autoscaler.py` | `_force_release_actors()` + tick 延迟 drain + `_pending_drain_nodes` | +47 |
| `python/ray/data/_internal/actor_autoscaler/__init__.py` | 工厂函数新增 `gpu_drain_manager` 参数 | +6 |
| `python/ray/data/_internal/execution/streaming_executor.py` | 新增 `_get_gpu_drain_manager()`，传给 autoscaler | +10 |
| `python/ray/data/context.py` | 新增 `gpu_node_proactive_drain_enabled` | +5 |
| `python/ray/autoscaler/sdk/sdk.py` | **新增** `request_node_drain()` 公开 API（`@DeveloperAPI`） | +28 |
| `python/ray/autoscaler/sdk/__init__.py` | 导出 `request_node_drain` | +2 |
| `python/ray/autoscaler/_private/commands.py` | **新增** `request_node_drain()` 底层实现 | +42 |

**总计**：17 个文件修改 + 1 个新文件，+370 行

---

## 13. 实施阶段

### 阶段 1：Raylet 对象感知 drain（C++）✅

- [x] `ray_config_def.h` 新增配置项
- [x] `local_resource_manager.h/cc` 修改关闭条件
- [x] `HasPinnedObjects()` 查询
- [x] drain 定时器
- [x] 更新 test fake 实现

### 阶段 2：对象迁移（C++）✅

- [x] `MigrateAllPinnedObjects()` 接口和实现
- [x] `SelectMigrationTarget()` 实现
- [x] `MigratePinnedObjectsForDrain()` 装配到 `NodeManager`
- [x] 构造函数 callback 装配

### 阶段 3：Actor 强制释放（Python）✅

- [x] `_ActorState.is_draining` 字段
- [x] `force_release_actors_on_node()` 方法
- [x] `get_available_actors()` 过滤 draining actor

### 阶段 4：Ray Data 主动触发（Python）✅

- [x] `GPUNodeDrainManager` 新文件
- [x] 集成到 `DefaultActorAutoscaler`（带幂等性保护）
- [x] 集成到 `StreamingExecutor`
- [x] `DataContext` 配置项
- [x] `create_actor_autoscaler` 工厂更新

### 阶段 4.1：Layer 4 公开 API 重构（v1.2）✅

- [x] `ray.autoscaler.sdk.request_node_drain()` 公开 API
- [x] `commands.request_node_drain()` 底层实现
- [x] `GPUNodeDrainManager` 重写：通过公开 SDK API 调用，移除直接 GCS 依赖
- [x] `DefaultActorAutoscaler` tick 延迟 drain：`_pending_drain_nodes` 机制
- [x] `_force_release_and_drain()` 重命名为 `_force_release_actors()`，分离 kill 和 drain

### 阶段 5：端到端验证（待执行）

- [ ] 测试完整链路：operator 完成 → actor kill → 对象迁移 → 节点关闭 → 云实例释放
- [ ] 测试 fallback 路径：autoscaler idle 检测
- [ ] 性能基准：测量 drain 延迟
- [ ] 单元测试补充

---

## 14. 测试方案

### 14.1 单元测试

#### C++ 层

| 测试场景 | 验证内容 |
|---------|---------|
| `OnResourceOrStateChanged` 带 pinned objects | 验证不会立即 shutdown |
| 超时后触发迁移 | 验证 `trigger_object_migration_` 被调用 |
| 迁移完成后 shutdown | 验证 `has_pinned_objects_` 返回 false 后正常 shutdown |
| `SelectMigrationTarget` 排除 draining 节点 | 验证 draining 节点不在候选列表 |
| `MigrateAllPinnedObjects` 空列表 | 验证立即调用 `on_complete` |
| `MigrateAllPinnedObjects` 无目标节点 | 验证跳过对象并最终调用 `on_complete` |

#### Python 层

| 测试场景 | 验证内容 |
|---------|---------|
| `force_release_actors_on_node` | 验证 `ray.kill` 被调用，内部状态正确更新 |
| `get_available_actors` 过滤 draining | 验证 `is_draining=True` 的 actor 被排除 |
| `GPUNodeDrainManager.request_drain_for_node` | 验证通过 SDK API 发送 drain，`_drained_nodes` 正确更新 |
| `_force_release_actors` 幂等性 | 验证同一 op 只处理一次 |
| `DefaultActorAutoscaler` 无 drain manager | 验证不影响正常 autoscaling |
| `_pending_drain_nodes` tick 延迟 | 验证 kill 和 drain 在不同 tick 步骤执行 |
| `request_node_drain` SDK API | 验证参数正确传递到 GCS drain RPC |

### 14.2 集成测试

| 测试场景 | 验证内容 |
|---------|---------|
| GPU map_batches 完成后节点回收 | 端到端验证：actor kill → drain → shutdown |
| 多 operator 共享节点 | 验证 IDLE_TERMINATION 在其他 worker 运行时拒绝 drain |
| drain 被拒绝后兜底 | 验证 Autoscaler 原生空闲检测最终回收节点 |
| 大对象迁移 | 验证 Push 成功且目标节点能 pin |

### 14.3 性能测试

| 指标 | 基线 | 目标 |
|------|------|------|
| operator 完成到 actor kill | N/A（之前不 kill） | < 1 个 autoscaler tick（~1s） |
| actor kill 到 drain 请求 | N/A（之前不 drain） | 同一 tick 末尾（tick 延迟机制） |
| drain 到 pinned objects 清空 | N/A | < 30s（自然消费）或 < 60s（含迁移） |
| 全链路：operator 完成到实例释放 | > 10min（依赖 idle timeout） | < 2min |

---

## 15. 审查发现与修复记录

v1.1 版本针对代码审查发现的问题进行了修复。以下是完整的审查记录。

### 15.1 [P0] `on_task_completed` assert 崩溃

**问题**：`force_release_actors_on_node` 调用 `ray.kill(actor)` 后，actor 的 in-flight task 的 streaming generator 会抛异常，触发 `_task_done_callback` → `on_task_completed(actor)`。此时 actor 已从 `_running_actors` 删除，`assert actor in self._running_actors` 直接崩溃。

```python
# 崩溃路径：
force_release_actors_on_node(node_id)
    └── ray.kill(actor)           # actor 有 2 个 in-flight tasks
    └── del _running_actors[actor] # 立即删除
    ...
    # 稍后，task 1 的回调触发
    _task_done_callback(actor)
        └── on_task_completed(actor)
            └── assert actor in self._running_actors  # ← CRASH
```

**修复**：将 assert 改为 guard check + early return。

```python
def on_task_completed(self, actor):
    if actor not in self._running_actors:
        return  # Actor was force-released during drain
    assert self._running_actors[actor].num_tasks_in_flight > 0
    ...
```

**文件**：`actor_pool_map_operator.py:1292-1294`

### 15.2 [P0] `register_actor` 从未被调用 — drain 请求永远不发出

**问题**：`GPUNodeDrainManager` 设计了 register/unregister/tick 三步流程，但 `register_actor()` 在整个代码中没有调用点。结果是 `_tracked_gpu_nodes` 永远为空，`tick()` 无事可做，drain 请求永远不发出。

**根因分析**：原设计意图是在 actor 创建时 register、释放时 unregister、定时 tick 检查。但 actor 创建分散在 `_ActorPool._on_actor_ready` 等多个内部方法中，集成点不明确。

**修复**：简化架构，去掉 register/unregister/tick 模式，改为 `request_drain_for_node(node_id)` 直接请求：

```python
class GPUNodeDrainManager:
    def __init__(self, enabled=True):
        self._drained_nodes: Set[str] = set()

    def request_drain_for_node(self, node_id: str):
        """Kill 完 actor 后直接请求 drain。"""
        if not self._enabled or node_id in self._drained_nodes:
            return
        self._request_drain(node_id)
```

多 operator 安全性由 IDLE_TERMINATION 的 Raylet 端拒绝逻辑保障：如果节点上还有其他 operator 的 worker，drain 会被拒绝。`_drained_nodes` 只记录被接受的 drain，被拒绝的可以被后续 operator 重新请求。

**文件**：`gpu_node_drain_manager.py`（重写）、`default_actor_autoscaler.py`（调用方更新）

### 15.3 [P1] `version++` 和 subscriber 通知在 early return 时被跳过

**问题**：`OnResourceOrStateChanged` 中的 object-aware drain 逻辑使用了多个 `return` 分支（等待消费、等待迁移），这些 return 跳过了方法末尾的 `++version_` 和 `resource_change_subscriber_` 通知。

```cpp
// 原代码（有 bug）：
if (elapsed < timeout) {
    return;  // ← 跳过 version++ 和 subscriber！
}
```

如果在 drain 等待期间有资源变化（worker 退出释放资源），集群调度视图不会得到更新。

**修复**：用 `bool ready_to_shutdown` flag 替代 early return，确保 `++version_` 和 `resource_change_subscriber_` 在所有分支下都被执行。

**文件**：`local_resource_manager.cc:447-494`

### 15.4 [P1] `scale()` 的 del-based release 与 `force_release` 的 `ray.kill` 竞争

**问题**：原代码执行顺序：

```
1. scale(_derive_target_scaling_config(...))
   └── "consumed all inputs" → _release_running_actor(actor) → del 引用
2. _force_release_and_drain(op, pool)
   └── force_release_actors_on_node(node_id) → ray.kill + del
```

第 1 步的 `del` 从 `_running_actors` 删除了 actor 引用但不保证 actor 进程退出（原始问题的根因）。第 2 步迭代 `_running_actors` 时找不到该 actor，跳过 kill。结果：actor 可能仍在运行但无人 kill。

**修复**：`force_release` 和 `scale()` 互斥执行。如果要 force release，跳过 `scale()`：

```python
if should_force_drain:
    self._force_release_actors(op, actor_pool)
else:
    actor_pool.scale(self._derive_target_scaling_config(...))
```

**文件**：`default_actor_autoscaler.py:61-88`

### 15.5 [P2] 不区分 GPU 和 CPU operator

**问题**：`_force_release_and_drain` 对所有 `_ActorPool` 类型的 operator 都触发 force release + drain，包括 CPU-only 的 actor pool operator。可能导致 CPU 节点被不必要地 drain。

**修复**：添加 GPU 资源检查：

```python
if actor_pool.per_actor_resource_usage().gpu <= 0:
    return
```

**文件**：`default_actor_autoscaler.py:_force_release_actors`

### 15.6 [P2] `get_available_actors` 性能回退

**问题**：原代码在 `_pending_scale_down_count <= 0` 时直接返回 `self._running_actors`（O(1)），修改后每次创建新 dict comprehension 过滤 `is_draining`（O(n)）。由于 `force_release` 立即删除 actor，`is_draining` 实际上不会在 `_running_actors` 中出现 True。

**修复**：恢复快速路径，直接返回 `self._running_actors`。`is_draining` 字段保留用于 defense-in-depth，仅在慢速路径（有 pending scale down 时）过滤。

**文件**：`actor_pool_map_operator.py:get_available_actors`

### 15.7 [P2] `MigrateAllPinnedObjects` 的 `on_complete` 语义

**已知限制**（不修复）：`on_complete` 在所有 Push 被**发起**后立即调用，而不是在 Push **完成**后。因为 `ObjectManager::Push` 是异步 fire-and-forget。

这不影响正确性：回调触发 `OnResourceOrStateChanged()` → `has_pinned_objects_()` 仍然为 true（Push 还没完成）→ 不会 shutdown。真正的 unpin 依赖后续的 Owner eviction 流程 + 定时器重新检查。

### 15.8 [P2] 定时器没有停止机制

**已知限制**（不修复）：`ObjectDrainCheck` 定时器在 shutdown 后不会自动停止。但 `shutdown_raylet_gracefully_` 后进程很快退出，窗口期极短。在该窗口期内，定时器回调调用 `RecheckDrainState()` 不会造成问题（`IsLocalNodeDraining()` 仍然为 true，但已过 shutdown 点不会再次 shutdown）。

---

### 审查修复总结

| 编号 | 优先级 | 问题 | 修复状态 |
|------|--------|------|---------|
| 15.1 | P0 | `on_task_completed` assert 崩溃 | ✅ 已修复 |
| 15.2 | P0 | `register_actor` 从未调用 | ✅ 重写为 `request_drain_for_node`（v1.2 进一步重构为公开 API） |
| 15.3 | P1 | version/subscriber 通知被跳过 | ✅ 用 flag 替代 early return |
| 15.4 | P1 | scale 和 force_release 竞争 | ✅ 互斥执行 |
| 15.5 | P2 | 不区分 GPU/CPU operator | ✅ 添加 GPU 资源检查 |
| 15.6 | P2 | get_available_actors 性能回退 | ✅ 恢复快速路径 |
| 15.7 | P2 | on_complete 语义 | 📝 记录为已知限制 |
| 15.8 | P2 | 定时器无停止机制 | 📝 记录为已知限制 |

---

## 16. Layer 4 架构改进：公开 API 重构（v1.2）

### 16.1 当前方案的架构问题

经过系统性分析，当前 Layer 4 实现存在三个架构级问题：

#### 16.1.1 违反层次抽象 — Ray Data 越权操作 Autoscaler

`GPUNodeDrainManager` 直接调用 `gcs_client.drain_node()`，这是一个标记为 "only for testing" 的内部 API：

```python
# python/ray/includes/gcs_client.pxi (Cython binding)
def drain_node(self, ...):
    """Send the DrainNode request to GCS.
    This is only for testing.
    """
```

Ray 的架构分层是：

```
应用层 (Ray Data / Serve / Train)
       ↓ 只通过 ray.autoscaler.sdk 交互
调度层 (Autoscaler / GCS / Raylet)
```

Ray Data 直接调 `drain_node` 相当于应用层绕过 SDK 直接操作集群基础设施，耦合了内部实现细节。一旦 GCS drain 协议变更（参数、语义、protobuf），Ray Data 代码会直接 break。

#### 16.1.2 同步阻塞 — 卡住调度循环

`drain_node()` 是同步 RPC：

```
DefaultActorAutoscaler.try_trigger_scaling()
  → _force_release_and_drain()
    → gpu_drain_manager.request_drain_for_node()
      → gcs_client.drain_node()  ← 同步阻塞，等 GCS 响应
```

`try_trigger_scaling()` 在 `StreamingExecutor` 的主循环中被调用。同步 drain RPC 阻塞整个调度循环，影响所有 operator 的进度（包括正在运行的非 GPU operator）。

#### 16.1.3 Kill → Idle 竞态 — 无重试兜底

```
ray.kill(actor)  →  Worker 退出中（异步）
                     │
drain_node(IDLE_TERMINATION)  →  Raylet 检查 IsLocalNodeIdle()
                                  → Worker 还没退出 → 拒绝 drain
                                  → 永远不会重试
```

`IDLE_TERMINATION` 要求节点 idle，但 `ray.kill` 后 worker 退出是异步的。当前代码没有重试机制，一旦被拒绝就放弃了。

### 16.2 三种替代方案对比

#### 方案 A：依赖 Autoscaler 现有空闲检测

核心思路：Layer 3（actor kill）保留，Layer 4 大幅简化 — 不主动调 `drain_node`，让 Autoscaler 自己检测空闲。

- **优点**：零侵入，无竞态，可维护
- **缺点**：延迟增加 ~5-35s（autoscaler tick 间隔 + idle 判定），依赖用户正确配置 `idle_timeout_s`

#### 方案 B：异步 Drain + 延迟重试

核心思路：保留主动 drain，但用异步方式避免阻塞，用延迟重试解决竞态。

- **优点**：不阻塞调度循环，延迟发送解决 kill→idle 竞态
- **缺点**：仍然使用非公开 API，引入线程增加复杂度

#### 方案 C：新增公开 API（推荐）

核心思路：向 `ray.autoscaler.sdk` 新增 `request_node_drain()` 公开函数，与已有的 `request_resources()` 形成对称设计。

- **优点**：正式契约，版本兼容性有保障；其他 Ray 库（Serve、Train）也能受益；与 `request_resources` 架构对称
- **缺点**：需新增 ~40 行封装代码

#### 方案对比

| 维度 | 方案 A（纯依赖空闲检测） | 方案 B（异步调内部 API） | 方案 C（新增公开 API） |
|------|------------------------|------------------------|----------------------|
| 延迟 | +5~35s | ~0s（但有竞态失败） | ~0s（竞态通过 tick 延迟解决） |
| 维护成本 | 零 | 高（耦合内部实现） | 低（正式契约） |
| 代码量 | 删代码 | 已写完 | +40 行封装 |
| 其他库受益 | 否 | 否 | Serve/Train 可复用 |
| 框架一致性 | ✅ | ❌ 违反分层 | ✅ 与 `request_resources` 对称 |

### 16.3 推荐策略：方案 C + 方案 A 作为 fallback

```
ray.autoscaler.sdk.request_node_drain(node_id, reason="Ray Data GPU scale-down")
  │
  ├── 成功 → 节点进入 drain 流程
  │
  └── 拒绝（节点未 idle）→ 不重试，依赖 Autoscaler 空闲检测兜底
```

#### 16.3.1 好处

1. **快速路径**：actor kill 后节点立即 idle → drain 请求立即被接受 → 零延迟缩容
2. **兜底路径**：竞态导致拒绝 → Autoscaler 5s 后自然检测到空闲 → 缓慢但确定性缩容
3. **不需要重试逻辑**：避免复杂性，最坏情况退化为方案 A
4. **正式 API**：`@DeveloperAPI` 装饰器保证版本兼容性

#### 16.3.2 Tick 延迟解决竞态

利用调度 tick 间隔作为天然延迟窗口，无需 `time.sleep` 或线程：

```python
# DefaultActorAutoscaler（实际实现）
def try_trigger_scaling(self):
    for op, state in self._topology.items():
        for actor_pool in op.get_autoscaling_actor_pools():
            if (
                self._gpu_drain_manager is not None
                and op.has_execution_finished()
                and id(op) not in self._drained_ops
            ):
                # Tick 前半：kill actors，记录待 drain 的节点
                self._force_release_actors(op, actor_pool)
                self._drained_ops.add(id(op))
            else:
                actor_pool.scale(...)

    # Tick 末尾：处理 drain（此时 kill 的 worker 大概率已退出）
    if self._pending_drain_nodes and self._gpu_drain_manager is not None:
        for node_id in list(self._pending_drain_nodes):
            self._gpu_drain_manager.request_drain_for_node(node_id)
            self._pending_drain_nodes.discard(node_id)
```

调度 tick 间隔本身就提供了天然的延迟窗口，无需 `time.sleep` 或线程。

### 16.4 公开 API 设计

新增 `ray.autoscaler.sdk.request_node_drain()`，与 `request_resources()` 对称：

```python
# ray/autoscaler/sdk/sdk.py
@DeveloperAPI
def request_node_drain(
    node_id: bytes,
    reason: str = "",
    deadline_remaining_seconds: Optional[int] = None,
) -> bool:
    """Request the autoscaler to drain a node.

    Sends an IDLE_TERMINATION drain request to the GCS for the specified node.
    The request is advisory — the Raylet will only accept the drain if the
    node is currently idle (no active workers). If rejected, the caller can
    rely on the Autoscaler's native idle detection as a fallback.

    This is the scale-down counterpart to request_resources() (scale-up).

    Args:
        node_id: The Ray node ID (bytes) to drain.
        reason: Human-readable reason for the drain request.
        deadline_remaining_seconds: Optional deadline in seconds. If None,
            no deadline is set (node drains gracefully).

    Returns:
        True if the drain request was accepted, False if rejected.
    """
```

实现链路：

```
ray.autoscaler.sdk.request_node_drain()
  → commands.request_node_drain()
    → GcsClient.drain_node(IDLE_TERMINATION)
```

### 16.5 重构后的 GPUNodeDrainManager

通过公开 API 调用，去除直接 GCS 客户端依赖（与 Section 8.2 实现一致）：

```python
class GPUNodeDrainManager:
    """Coordinates GPU node drain requests after actor release.

    After Ray Data force-kills GPU actors on a node, this manager sends
    an IDLE_TERMINATION drain request via the public autoscaler SDK.
    The drain is only accepted if the node is actually idle (no other
    workers running), providing a built-in safety net for multi-operator
    scenarios. If rejected, the Autoscaler's native idle detection
    serves as a fallback.
    """

    def __init__(self, enabled: bool = True):
        self._enabled = enabled
        self._drained_nodes: Set[str] = set()

    def request_drain_for_node(self, node_id: str) -> bool:
        if not self._enabled or node_id in self._drained_nodes:
            return False
        return self._request_drain(node_id)

    def _request_drain(self, node_id: str) -> bool:
        """Send a drain request via the public autoscaler SDK."""
        try:
            from ray.autoscaler.sdk import request_node_drain

            node_id_bytes = (
                node_id.encode() if isinstance(node_id, str) else node_id
            )
            is_accepted = request_node_drain(
                node_id_bytes,
                reason="Ray Data GPU actor scale-down",
            )
            if is_accepted:
                self._drained_nodes.add(node_id)
                logger.info(f"Drain accepted for GPU node {node_id}")
            else:
                logger.debug(f"Drain rejected for GPU node {node_id}")
            return is_accepted
        except Exception:
            logger.warning(
                f"Failed to drain GPU node {node_id}", exc_info=True
            )
            return False
```

### 16.6 层次推荐决策

| 层次 | 决策 | 理由 |
|------|------|------|
| Layer 1-2（C++ 对象感知） | 保留 | 解决真实的数据丢失问题，改动自洽 |
| Layer 3（Actor kill） | 保留 | 解决核心问题 — GPU 资源不释放 |
| Layer 4（GPUNodeDrainManager） | 方案 C 重构 | 通过公开 API 调用，消除层次抽象违反；tick 延迟解决竞态 |
