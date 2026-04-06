# Phase 1 实现计划：对象副本推送（可抢占节点 → 稳定节点）

## 概述

当可抢占（Spot）节点上的 Worker 执行 Task 产生大对象时，对象数据仅存储在该节点的本地 Plasma Store 中。节点被回收后，数据丢失需要血缘重算。Phase 1 的目标：在可抢占节点上创建对象后，主动将数据 Push 到一个稳定节点，提供数据冗余。

## 核心设计决策

| 问题 | 决策 | 理由 |
|------|------|------|
| 何时触发复制？ | `SealExisting` 中 `PinObjectIDs` 回调成功后，调用 `MaybeReplicateObject` | 对象已 Seal 且已 Pin，此时数据稳定可读 |
| Worker 如何判断自己在可抢占节点？ | 启动时从 Raylet 获取 `is_preemptible_node` 标志（通过 `GcsNodeInfo.labels`） | 一次性传递，无需反复查询 |
| 复制由谁执行？ | Worker → 本地 Raylet（`ReplicateObject` RPC） → `ObjectManager::Push` | Raylet 有全局视图，能选择稳定节点、控制并发 |
| 如何选择稳定节点？ | Raylet 从 `cluster_resource_map_` 中随机选一个非自身、非可抢占的节点 | 简单有效，避免热点 |
| 对象大小门槛？ | `object_replication_min_size`（默认 100KB） | < 100KB 的对象通过 gRPC Inline 返回，不进 Plasma |
| 并发控制？ | `object_replication_max_concurrent`（默认 10），per-node 原子计数器 | 防止突发大量小对象压垮网络 |
| Push 失败怎么办？ | 不重试，Best-effort | 原始 Pin 仍在，后续 Recovery 机制兜底 |

## 数据流

```
Worker (可抢占节点 A)              Raylet (可抢占节点 A)           Raylet (稳定节点 B)
    │                                │                              │
    │ SealExisting(object_id)        │                              │
    │ → PinObjectIDs ──────────→     │                              │
    │                              Pin 成功                          │
    │ ← OK ──────────────────────    │                              │
    │                                │                              │
    │ MaybeReplicateObject(obj)      │                              │
    │ → ReplicateObject RPC ───→     │                              │
    │                              检查 strategy/size/并发           │
    │                              SelectStableNode → B             │
    │                              Push(obj, B) ──────────────→     │
    │                                │                         Receive + Seal
    │                                │                              │
    │                                │                         ReportObjectAdded
    │                                │                           → RPC to Owner
    │                                │                              │
    │ Release(object_id)             │                              │
    │                                │                              │
    ★ Owner 得知 B 有副本（AddObjectLocation）                      │
    ★ 但 Pin 仍在 A，B 上的副本是 ref_count=0 的 LRU 副本           │
```

## 实现步骤

### Step 1: 新增配置项

**文件**: `src/ray/common/ray_config_def.h`

```cpp
/// Object replication strategy: "none" (disabled), "push_to_stable_node"
RAY_CONFIG(std::string, object_replication_strategy, "none")

/// Label value that identifies preemptible nodes
RAY_CONFIG(std::string, preemptible_node_market_type, "spot")

/// Minimum object size (bytes) to trigger replication
RAY_CONFIG(int64_t, object_replication_min_size, 100 * 1024)

/// Maximum concurrent replications per node
RAY_CONFIG(int64_t, object_replication_max_concurrent, 10)
```

### Step 2: Raylet 向 Worker 传递 is_preemptible 标志

**文件**: `src/ray/flatbuffers/node_manager.fbs`
- 在 worker 注册回复中新增 `is_preemptible_node: bool`

**文件**: `src/ray/core_worker/core_worker_options.h`
- 新增 `bool is_preemptible_node = false`

**文件**: `src/ray/core_worker/core_worker_process.cc`
- 从 Raylet 注册回复中读取 `is_preemptible_node` 并赋值

### Step 3: CoreWorker 新增 MaybeReplicateObject

**文件**: `src/ray/core_worker/core_worker.h`

```cpp
/// Whether this node is a preemptible (spot) node.
bool is_preemptible_node_ = false;

/// If this node is preemptible and replication is enabled,
/// request the local raylet to replicate the object to a stable node.
void MaybeReplicateObject(const ObjectID &object_id);
```

**文件**: `src/ray/core_worker/core_worker.cc`

```cpp
void CoreWorker::MaybeReplicateObject(const ObjectID &object_id) {
  if (!is_preemptible_node_) return;
  const auto &strategy = RayConfig::instance().object_replication_strategy();
  if (strategy != "push_to_stable_node") return;

  local_raylet_rpc_client_->ReplicateObject(
      object_id,
      [object_id](const Status &status, const rpc::ReplicateObjectReply &reply) {
        if (!status.ok() || !reply.accepted()) {
          RAY_LOG(DEBUG).WithField(object_id)
              << "Object replication not accepted: "
              << (status.ok() ? reply.error_message() : status.ToString());
        }
      });
}
```

在 `SealExisting` 的 `PinObjectIDs` 成功回调中调用：
```cpp
MaybeReplicateObject(object_id);
```

### Step 4: 新增 ReplicateObject RPC

**文件**: `src/ray/protobuf/node_manager.proto`

```protobuf
message ReplicateObjectRequest {
  bytes object_id = 1;
}

message ReplicateObjectReply {
  bool accepted = 1;
  string error_message = 2;
}
```

**文件**: `src/ray/raylet_ipc_client/raylet_ipc_client.h`
- 新增 `ReplicateObject` 方法

### Step 5: NodeManager 实现 HandleReplicateObject

**文件**: `src/ray/raylet/node_manager.h`

```cpp
/// Handle object replication request from worker.
void HandleReplicateObject(rpc::ReplicateObjectRequest request,
                           rpc::ReplicateObjectReply *reply,
                           rpc::SendReplyCallback send_reply_callback);

/// Select a random stable node for object replication.
std::optional<NodeID> SelectStableNode() const;

/// Atomic counter for in-flight replications.
std::atomic<int64_t> replications_in_flight_{0};

/// Random number generator for stable node selection.
mutable std::mt19937 rng_{std::random_device{}()};
```

**文件**: `src/ray/raylet/node_manager.cc`

`HandleReplicateObject` 逻辑：
1. 检查 `object_replication_strategy != "push_to_stable_node"` → reject
2. 检查本节点 `is_preemptible_node_` → reject if not
3. 查本地 Plasma 对象大小 < `object_replication_min_size` → reject
4. 检查 `replications_in_flight_ >= max_concurrent` → reject
5. `SelectStableNode()` → 无稳定节点 → reject
6. `replications_in_flight_++`
7. `object_manager_.Push(object_id, target_node)`
8. `replications_in_flight_--`
9. 返回 `accepted=true`

`SelectStableNode` 逻辑：
- 遍历 `cluster_resource_map_`
- 排除自身节点
- 排除 labels 中 `ray.io/node-market-type == preemptible_node_market_type` 的节点
- 从剩余节点中随机选一个

### Step 6: 新增 Metrics

**文件**: `src/ray/raylet/metrics.h`

```cpp
extern ray::stats::CounterType object_replication_succeeded_;
extern ray::stats::CounterType object_replication_skipped_;
```

Skipped 按原因分类：`strategy_disabled`、`not_preemptible`、`too_small`、`concurrency_limit`、`no_stable_node`

### Step 7: ObjectManager 暴露 Push 接口

**文件**: `src/ray/object_manager/object_manager.h`

确保 `Push(const ObjectID &, const NodeID &)` 是 public 且可从 NodeManager 调用。

## 文件改动清单

| # | 文件路径 | 改动类型 | 说明 |
|---|---------|---------|------|
| 1 | `src/ray/common/ray_config_def.h` | 新增 | 4 个配置项 |
| 2 | `src/ray/flatbuffers/node_manager.fbs` | 修改 | 新增 `is_preemptible_node` 字段 |
| 3 | `src/ray/core_worker/core_worker_options.h` | 修改 | 新增 `is_preemptible_node` 选项 |
| 4 | `src/ray/core_worker/core_worker_process.cc` | 修改 | 从 Raylet 回复中读取标志 |
| 5 | `src/ray/core_worker/core_worker.h` | 新增 | `is_preemptible_node_` + `MaybeReplicateObject` |
| 6 | `src/ray/core_worker/core_worker.cc` | 新增 | `MaybeReplicateObject` 实现 + 调用点 |
| 7 | `src/ray/protobuf/node_manager.proto` | 新增 | `ReplicateObject` RPC 定义 |
| 8 | `src/ray/raylet_ipc_client/raylet_ipc_client.h` | 修改 | 新增 RPC 客户端方法 |
| 9 | `src/ray/raylet_ipc_client/raylet_ipc_client.cc` | 修改 | RPC 客户端实现 |
| 10 | `src/ray/raylet/node_manager.h` | 新增 | Handler + SelectStableNode + 成员 |
| 11 | `src/ray/raylet/node_manager.cc` | 新增 | `HandleReplicateObject` + `SelectStableNode` 实现 |
| 12 | `src/ray/raylet/metrics.h` | 新增 | replication metrics |
| 13 | `src/ray/object_manager/object_manager.h` | 修改 | 确保 Push 接口可用 |
| 14 | `src/mock/ray/object_manager/object_manager.h` | 修改 | Mock Push 方法 |

## 边界情况分析

| 场景 | 行为 | 正确性 |
|------|------|--------|
| strategy=none | `MaybeReplicateObject` 第一行 early return | ✅ 零开销 |
| 非可抢占节点 | `is_preemptible_node_=false` → 不触发 | ✅ |
| 对象 < 100KB | Raylet 侧检查大小 → reject | ✅ |
| 无稳定节点 | `SelectStableNode` 返回 nullopt → reject | ✅ |
| Push 失败 | fire-and-forget，原 Pin 不变，Recovery 兜底 | ✅ |
| 并发达到上限 | `replications_in_flight_ >= max` → reject | ✅ |
| 稳定节点 B 已有副本 | `ObjectManager::Push` 内部去重（`unfulfilled_push_requests_`） | ✅ |

## 已知限制

1. **Pin 仍在可抢占节点**：Push 后稳定节点有数据，但 `pinned_at_node_id_` 仍指向可抢占节点。稳定节点的副本 `ref_count=0`，在 LRU 中，可能被驱逐 → Phase 2 解决
2. **并发限制是近似的**：`replications_in_flight_` 在 Push 发起后立即递减，实际是 burst rate limiter 而非真正的并发限制
3. **Metric 是发起而非完成**：`object_replication_succeeded` 记录的是 Push 发起成功，非数据传输完成
4. **无重复 Push 去重**：不同时间可能选中不同稳定节点，导致多份副本（但实践中 SealExisting 只调用一次）

## 验证

1. **编译**: `bazel build //src/ray/core_worker:core_worker_lib`
2. **配置验证**: 启用 `object_replication_strategy=push_to_stable_node`，观察日志和 metrics
3. **功能验证**: 可抢占节点 Task 返回大对象 → 稳定节点 Plasma 中出现副本 → Owner 的 `object_locations_` 包含稳定节点

## 实现状态

✅ 已合并：commit `72f01c937d`
