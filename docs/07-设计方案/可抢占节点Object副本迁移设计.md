# Ray 可抢占节点对象副本迁移设计方案

## 文档信息

| 项目 | 内容 |
|------|------|
| 标题 | 可抢占（Spot）节点场景下的 Object 副本主动迁移方案 |
| 版本 | v2.0 |
| 基于 Ray 版本 | 2.52.1 |
| 状态 | Phase 1 已实现，Phase 2/3 待实现 |

---

## 目录

1. [背景与动机](#1-背景与动机)
2. [目标与非目标](#2-目标与非目标)
3. [现有架构分析](#3-现有架构分析)
4. [方案总体设计](#4-方案总体设计)
5. [详细设计](#5-详细设计)
6. [当前实现状态](#6-当前实现状态)
7. [已知问题与限制](#7-已知问题与限制)
8. [配置项设计](#8-配置项设计)
9. [完整数据流](#9-完整数据流)
10. [异常处理](#10-异常处理)
11. [性能影响评估](#11-性能影响评估)
12. [测试方案](#12-测试方案)
13. [实施计划](#13-实施计划)
14. [后续演进](#14-后续演进)

---

## 1. 背景与动机

### 1.1 问题描述

集群中存在**可抢占节点**（Preemptible/Spot Nodes），这些节点使用竞价实例或共享资源池，可能随时被回收。当可抢占节点上的 Worker 执行 Task 产生大对象（>= 100KB）时，对象数据存储在该节点的本地 Plasma Store 中。节点被回收后，数据丢失，需要通过血缘重算恢复。

### 1.2 当前行为

```
Driver (稳定节点)                Worker (可抢占节点)
    │                               │
    │ task.remote(data) ──→        │
    │                               │ result = compute(data)
    │                               │ SealExisting → 本地 Plasma Seal
    │                               │ PinObjectIDs → 本地 Raylet Pin
    │     ←─── Reply(in_plasma=true)│
    │ UpdateObjectPinnedAtRaylet    │
    │   (pinned_at = 可抢占节点)     │
    │                               │
    │                               │ ★ 节点被回收
    │                               │
    │ ResetObjectsOnRemovedNode     │
    │ RecoverObject → ResubmitTask  │  ← 需要重新计算
```

### 1.3 为什么不需要迁移 Owner

| 对象类型 | Owner 位置 | 是否需要迁移 Owner | 原因 |
|---------|-----------|------------------|------|
| Task 返回值（`return data`） | Driver（稳定节点） | 否 | Owner 天然在稳定节点 |
| `ray.put()` 产生 | 调用方 | 否 | INELIGIBLE_PUT，迁移 Owner 也无法重建 |
| 嵌套 Task（`return data`） | 调用方（可抢占节点） | 否 | Driver 可重建整条 Task 链 |

**核心结论**：标准场景下只需迁移**数据**到稳定节点，不需要迁移 Owner。

⚠️ **但存在例外**：当 Owner 本身也在可抢占节点时（见[第 7 节](#7-已知问题与限制)）。

---

## 2. 目标与非目标

### 2.1 目标

1. Task 返回值的大对象在可抢占节点 Seal 后，异步推送一份副本到稳定节点
2. 可抢占节点回收后，对象数据仍可通过稳定节点的副本访问，无需重算
3. 对现有 Owner / 引用计数 / 血缘重建逻辑**最小侵入**
4. 策略可扩展（当前为 push_to_stable_node，未来可扩展为外部存储等）

### 2.2 非目标

1. **不迁移 Owner**：Owner 保持在原调用方
2. **不修改 `ray.put()` 路径**
3. **不处理 Inline 返回**（< 100KB，数据已随 gRPC Reply 返回）
4. **不处理 Actor 状态迁移**

---

## 3. 现有架构分析

### 3.1 进程架构

```
CoreWorker 进程 (Worker)          Raylet 进程 (NodeManager)
┌──────────────────────┐         ┌──────────────────────────┐
│ core_worker.cc        │         │ node_manager.cc           │
│                      │  IPC    │                          │
│ raylet_ipc_client_ ──────────→ │ RegisterClient            │
│                      │  gRPC   │                          │
│ local_raylet_rpc_    │         │                          │
│ client_ ─────────────────────→ │ HandleReplicateObject()  │
│                      │         │                          │
│ ★ 无 object_manager_ │         │ object_manager_ ─────────┼──→ Push/Pull
└──────────────────────┘         └──────────────────────────┘
```

**关键约束**：CoreWorker 中**没有 ObjectManager**，无法直接 Push，必须经 Raylet 中转。

### 3.2 现有 Push 机制

ObjectManager 的 Push 已非常成熟，但有一个重要限制：

| 特性 | 状态 | 说明 |
|------|------|------|
| 分块传输 | ✅ 已有 | `ChunkObjectReader` + `PushManager` |
| 限流/去重 | ✅ 已有 | `PushManager::max_chunks_in_flight_` |
| 完成回调 | ❌ 缺失 | `Push()` 返回 `void`，无 `StatusCallback` |
| 超时处理 | ✅ 已有 | `push_timeout_ms` |

### 3.3 现有 Location 上报链路

Push 到稳定节点后 Seal 自动触发 Location 上报，链路完整：

```
ObjectBufferPool::WriteChunk (所有 chunk 写完)
  → Seal → HandleObjectAdded → ReportObjectAdded → Owner
```

**无需额外代码，Owner 会自动收到新 location 通知。**

### 3.4 现有 Pin 转移机制

`PinExistingObjectCopy()` 已实现 Pin 转移逻辑，当前仅在节点下线后的恢复流程中使用。

---

## 4. 方案总体设计

### 4.1 三层架构

```
┌─────────────────────────────────────────────────┐
│  Phase 1: 数据推送层 (已实现)                      │
│  CoreWorker → ReplicateObject RPC → NodeManager  │
│  → ObjectManager.Push → 稳定节点 Plasma            │
├─────────────────────────────────────────────────┤
│  Phase 2: Pin 转移层 (待实现)                      │
│  Owner 收到新 Location → PinObjectIDs(稳定节点)    │
│  → UpdatePinnedAt → 释放旧 Pin                    │
├─────────────────────────────────────────────────┤
│  Phase 3: 应用适配层 (待实现)                      │
│  Ray Data Hash Shuffle 等 ray.put 场景改造         │
└─────────────────────────────────────────────────┘
```

### 4.2 核心设计决策

| 决策 | 选择 | 理由 |
|------|------|------|
| 备份策略 | 集群级配置 `object_replication_strategy` | 不需要节点级差异，运维简单 |
| 节点标识 | `ray.io/node-market-type` 标签 + `preemptible_node_market_type` 配置 | 复用现有标签机制 |
| Push 触发方式 | Worker 通过 ReplicateObject RPC 通知本地 Raylet | CoreWorker 无 ObjectManager |
| Push 时机 | Pin 成功后、Release 之前 | 确保 Push 发起时对象尚未被驱逐 |
| Push 语义 | 尽力而为（best-effort）| 失败退化为现有行为（血缘重算） |

---

## 5. 详细设计

### 5.1 可抢占节点标识

通过两层机制确定节点身份：

1. **节点标签**：`ray start --labels='{"ray.io/node-market-type": "spot"}'`
2. **集群配置**：`preemptible_node_market_type = "spot"` 指定哪个标签值表示可抢占

```cpp
// NodeManager 构造时缓存一次，后续直接读取
bool NodeManager::IsPreemptibleNode() const {
  auto it = initial_config_.labels.find("ray.io/node-market-type");
  return it != initial_config_.labels.end() &&
         it->second == RayConfig::instance().preemptible_node_market_type();
}
```

Worker 通过 `RegisterClientReply.is_preemptible_node` FlatBuffer 字段获取本节点身份。

### 5.2 ReplicateObject RPC

```protobuf
message ReplicateObjectRequest {
  bytes object_id = 1;
}
message ReplicateObjectReply {
  bool accepted = 1;
  string error_message = 2;
}

service NodeManagerService {
  rpc ReplicateObject(ReplicateObjectRequest) returns (ReplicateObjectReply);
}
```

### 5.3 HandleReplicateObject 处理流程

```
请求到达
  ├─ 检查 strategy == "push_to_stable_node"     → 否: 跳过
  ├─ 检查 is_preemptible_node_cached_           → 否: 跳过
  ├─ GetObjectSize(object_id)                   → < 0: 对象不存在
  ├─ object_size >= object_replication_min_size  → 否: 对象太小
  ├─ replications_in_flight_ < max_concurrent   → 否: 并发上限
  ├─ SelectStableNode()                         → Nil: 无稳定节点
  └─ object_manager_.Push(object_id, target)    → 记录 metrics
```

### 5.4 CoreWorker 触发点

```cpp
// SealExisting → PinObjectIDs callback → status.ok()
MaybeReplicateObject(object_id);    // ← 在 Release 之前调用
plasma_store_provider_->Release(object_id);
```

`MaybeReplicateObject` 做两次 early-return 检查：
1. `!is_preemptible_node_` → return（最常见路径，零开销）
2. `strategy != "push_to_stable_node"` → return

### 5.5 稳定节点选择

从 `ClusterResourceManager::GetResourceView()` 遍历所有节点，过滤条件：
- 排除自身 (`node_id == self_node_id_`)
- 排除可抢占节点 (`labels["ray.io/node-market-type"] == preemptible_type`)

从候选节点中使用 `std::uniform_int_distribution` 随机选择。

### 5.6 可观测性

三个 metrics 指标：

| Metric | 类型 | 标签 | 说明 |
|--------|------|------|------|
| `object_replication_requested` | Sum | - | 收到的副本请求总数 |
| `object_replication_succeeded` | Sum | - | 成功发起 Push 的数量 |
| `object_replication_skipped` | Sum | Reason | 跳过的数量，按原因分类 |

Reason 标签取值：`disabled`、`not_preemptible`、`not_found`、`too_small`、`concurrency_limit`、`no_stable_node`

---

## 6. 当前实现状态

### 6.1 已实现（Phase 1）

| 组件 | 文件 | 状态 |
|------|------|------|
| 配置项（4 个） | `ray_config_def.h` | ✅ |
| Proto 定义 | `node_manager.proto` | ✅ |
| RPC Handler 注册 | `node_manager_server.h` | ✅ |
| NodeManager 实现 | `node_manager.h/cc` | ✅ |
| FlatBuffers is_preemptible_node | `node_manager.fbs` | ✅ |
| IPC Client 传递 | `raylet_ipc_client*` | ✅ |
| CoreWorkerOptions 传递 | `core_worker_options.h`, `core_worker_process.cc` | ✅ |
| RayletClient RPC | `raylet_client*` | ✅ |
| CoreWorker 触发 | `core_worker.h/cc` | ✅ |
| ObjectManagerInterface::Push | `object_manager.h` | ✅ |
| ObjectManagerInterface::GetObjectSize | `object_manager.h/cc` | ✅ |
| MockObjectManager | `mock/object_manager.h` | ✅ |
| Metrics | `metrics.h`, `node_manager.cc` | ✅ |
| min_size 检查 | `node_manager.cc` | ✅ |
| max_concurrent 限流 | `node_manager.cc` | ✅ |

### 6.2 未实现

| 组件 | 说明 | Phase |
|------|------|-------|
| Pin 转移 | Owner 收到新 location 后 Pin 到稳定节点 | Phase 2 |
| 旧 Pin 释放 | Pin 转移成功后释放可抢占节点的 Pin | Phase 2 |
| Push 完成回调 | `ObjectManager::Push` 无 StatusCallback | Phase 2 |
| 重复 Push 去重 | 同一对象可能被重复请求 replication | Phase 2 |
| 应用层适配 | Ray Data Hash Shuffle 改造 | Phase 3 |

---

## 7. 已知问题与限制

### 7.1 [P0] Owner 在可抢占节点的问题

**场景**：嵌套 Task 场景中，外层 Task 在可抢占节点 A 上执行，调用内层 Task 在可抢占节点 B 上执行。此时内层 Task 返回值的 **Owner 是节点 A 上的 Worker**。

```
Driver (稳定)
  └─ outer_task.remote()  →  Worker A (可抢占, Owner)
                                └─ inner_task.remote()  →  Worker B (可抢占)
                                                             └─ return large_data
                                                                  ↓
                                                        对象 Pin 在 B, Owner 在 A
```

**问题链**：

| 步骤 | 当前行为 | 后果 |
|------|---------|------|
| 1. 对象数据 Push 到稳定节点 C | ✅ Push 成功 | 数据安全 |
| 2. 稳定节点 C 上报 location 给 Owner A | ✅ location 更新 | Owner 知道新副本 |
| 3. 节点 A 被回收 | Owner 死亡 | **所有由 A 拥有的对象失去 Owner** |
| 4. 稳定节点 C 上的副本 | 无 Pin（Phase 2 未实现）；即使有 Pin，Owner 已死，`owner_dead_callback` 触发释放 | **副本被删除** |
| 5. ray.get(ref) | 对象不可发现 | **失败** |

**当前缓解措施**：

- Ray 的 `max_retries` 机制会触发整条 Task 链重算
- Driver 的 `ResubmitTask` 可以重建 outer_task，进而重建 inner_task

**根本解决需要**：Owner 迁移或 Owner 始终在稳定节点（通过调度约束或 `_owner=driver`）。这不在当前方案范围内。

**应用层规避**：

```python
# ✅ 让 Owner 始终是 Driver
@ray.remote(max_retries=3)
def outer_task(data):
    # 不要 return inner_task.remote()
    # 而是 get 后 return 数据
    return ray.get(inner_task.remote(data))
```

### 7.2 [P1] 副本在稳定节点上无 Pin 保护

**现状**：Push 到稳定节点后，对象经过 `Seal → Release`，`ref_count == 0`，进入 LRU 链表。如果稳定节点 Plasma 空间紧张，副本可能在 Owner 完成 Pin 转移之前被 LRU 驱逐。

```
时间线：
  t0: Push 到达稳定节点 → Seal → Release → ref_count=0, 进入 LRU
  t1: ReportObjectAdded → Owner
  t2: Owner 收到新 location
  t3: (Phase 2) Owner PinObjectIDs → 稳定节点

  ★ t0 ~ t3 之间：LRU 暴露窗口，副本可能被驱逐
```

**Phase 2 实现后**，窗口缩短为 t0~t3 的 RPC 延迟（通常 ms 级）。但如果 Plasma 极度紧张（几乎满），仍有风险。

**潜在优化**：在 `HandleReplicateObject` 中向目标节点发送 PinObjectIDs 请求，而不仅仅是 Push 数据。但这增加了跨节点 RPC 复杂度。

### 7.3 [P1] Push fire-and-forget，无完成确认

`ObjectManager::Push()` 返回 `void`，没有完成回调（源码中有 `TODO: Add success/failure callbacks for push and pull`）。

**影响**：
- `HandleReplicateObject` 在 Push 发起后立即返回 `accepted=true`，但 Push 可能后续失败
- `replications_in_flight_` 计数器在 Push 发起后立即递减，无法精确限制真正在途的 Push 数量
- 无法区分 "Push 已成功送达" 和 "Push 已提交但可能失败"

**当前措施**：metrics 中 `object_replication_succeeded` 记录的是 "成功发起" 而非 "成功完成"。日志级别为 INFO，便于事后排查。

### 7.4 [P2] 并发限制是近似的

`replications_in_flight_` 在 Push 发起后立即递减，实际上限制的是 "每秒发起的 Push 速率" 而非 "同时在途的 Push 数量"。因为 `Push` 是异步的，实际在途数可能超过 `max_concurrent`。

真正的并发由 `PushManager::max_chunks_in_flight_` 控制（默认 512 chunks），但这是一个全局限制，不区分 replication 和正常 Pull 触发的 Push。

### 7.5 [P2] 无重复 Push 去重

同一对象的 `SealExisting` 理论上只调用一次，但如果 PinObjectIDs 的回调被重试或并发执行，可能导致同一对象多次触发 `MaybeReplicateObject`。`ObjectManager::Push` 内部有对 `(object_id, node_id)` 对的去重（`unfulfilled_push_requests_`），但不同的 `SelectStableNode()` 调用可能选中不同节点。

### 7.6 [P3] object_replication_min_size 未考虑 Inline 返回

对象大小检查在 `HandleReplicateObject`（Raylet 侧）进行，但实际上 < 100KB 的对象通常已经通过 gRPC Inline 返回给 Driver，根本不进入 Plasma。`MaybeReplicateObject` 只在 `pin_object=true`（即 Plasma 路径）时触发，所以这个问题在实践中不存在，但 `min_size` 的默认值（100KB）应与 `max_direct_call_object_size`（100KB）保持一致。

---

## 8. 配置项设计

### 8.1 配置项清单

```cpp
/// 备份策略: "none"（禁用）, "push_to_stable_node"（推送到稳定节点）
RAY_CONFIG(std::string, object_replication_strategy, "none")

/// 标识可抢占节点的 market type 标签值
RAY_CONFIG(std::string, preemptible_node_market_type, "spot")

/// 触发备份的最小对象大小（字节）
RAY_CONFIG(int64_t, object_replication_min_size, 100 * 1024)

/// 每节点最大并发备份数
RAY_CONFIG(int64_t, object_replication_max_concurrent, 10)
```

### 8.2 使用方式

```python
ray.init(_system_config={
    "object_replication_strategy": "push_to_stable_node",
    "preemptible_node_market_type": "spot",
    "object_replication_min_size": 102400,
    "object_replication_max_concurrent": 10,
})
```

```bash
# 可抢占节点启动
RAY_NODE_MARKET_TYPE=spot ray start --address=...
# 或
ray start --labels='{"ray.io/node-market-type": "spot"}' --address=...
```

---

## 9. 完整数据流

### 9.1 Phase 1 当前流程

```
Driver (稳定)        Worker (可抢占A)     Raylet (可抢占A)      Raylet (稳定B)
    │                     │                    │                    │
  ① task.remote() ──→   │                    │                    │
    │                   ② compute()           │                    │
    │                   ③ SealExisting         │                    │
    │                   ④ PinObjectIDs ──→    │                    │
    │                                       ⑤ Pin                  │
    │                   ←── OK ────           │                    │
    │                                          │                    │
    │                   ⑥ MaybeReplicateObject │                    │
    │                      ReplicateObject ──→│                    │
    │                                       ⑦ 检查 strategy/size/并发
    │                                       ⑧ SelectStableNode→B   │
    │                                       ⑨ Push(obj, B) ──────→│
    │                                          │               ⑩ Receive+Seal
    │                   ⑪ Release(obj)         │                    │
    │                                          │               ⑫ ReportObjectAdded
    │                                          │                  → RPC to Owner
    │  ⑬ AddObjectLocation(B)                 │                    │
    │                                          │                    │
    │  ★ 此时 Owner 知道 B 有副本              │                    │
    │  ★ 但 Pin 仍在 A, B 上的副本无 Pin 保护  │                    │
```

### 9.2 Phase 2 完成后的流程（待实现）

```
    │  ⑬ AddObjectLocation(B)                 │                    │
    │  ⑭ 检测到 Pin 在可抢占节点                │                    │
    │  ⑮ PinObjectIDs(B) ───────────────────────────────────→     │
    │                                          │               ⑯ Pin 副本
    │                            ←─────────────────── Pin OK       │
    │  ⑰ UpdatePinnedAt(B)                    │                    │
    │  ⑱ 释放 A 的旧 Pin ──────────────→      │                    │
    │                                       ⑲ Unpin                │
    │                                          │                    │
    │  ★ Pin 已转移到稳定节点 B                 │                    │
    │  ★ 可抢占节点 A 可安全下线                │                    │
```

### 9.3 节点回收后的 ray.get 路径

**Phase 1（当前）**：如果副本尚未被 LRU 驱逐：
```
Driver: ray.get(ref)
  → locations 中找到 B
  → Pull from B → 成功 ✅
```

如果副本已被 LRU 驱逐：
```
Driver: ray.get(ref)
  → RecoverObject → ResubmitTask → 重算 ❌（退化为无副本行为）
```

**Phase 2 完成后**：副本有 Pin 保护，不会被 LRU 驱逐 → 始终可用。

---

## 10. 异常处理

### 10.1 Push 失败

| 场景 | 处理 | 影响 |
|------|------|------|
| 无稳定节点 | RPC 返回 accepted=false | 退化为现有行为 |
| 对象太小 | RPC 返回 accepted=false | 正确行为，小对象已 Inline |
| 并发上限 | RPC 返回 accepted=false | 部分对象无副本 |
| Push 过程中节点下线 | 接收端丢弃不完整对象 | 重算 |
| 稳定节点 Plasma 满 | Push 写入失败 | 重算 |

**关键原则**：所有失败都是 best-effort，不阻塞 Task 返回，不影响正确性。

### 10.2 竞态条件

**Race 1：Push 完成前节点下线**
→ 接收端丢弃，Owner 触发 RecoverObject → 重算。**正确** ✅

**Race 2：Pin 转移期间对象被 del**
→ 即使 Pin 转移成功，引用计数归零后 `WORKER_OBJECT_EVICTION` 释放新 Pin。**正确** ✅

**Race 3：MaybeReplicateObject 与 Release 的顺序**
→ 当前实现中 `MaybeReplicateObject` 在 `Release` 之前调用，确保 Push 发起时对象仍在 Plasma。但 Push 是异步的，`Release` 后对象可能在 Push 完成前被驱逐。由于 Raylet 的 Pin（HandlePinObjectIDs）持有 `shared_ptr`，实际上对象在 Pin 期间不会被驱逐。**正确** ✅

---

## 11. 性能影响评估

### 11.1 延迟影响

| 操作 | 额外延迟 | 说明 |
|------|---------|------|
| Task 返回延迟 | **0** | Push 异步，不阻塞 Reply |
| MaybeReplicateObject 开销 | < 1μs（非可抢占节点） | early-return 检查 `is_preemptible_node_` |
| ray.get 延迟 | 可能降低 | 副本在稳定节点，更高可用性 |

### 11.2 网络带宽

额外带宽 = 可抢占节点上所有 Plasma 对象的总量。通过 `object_replication_max_concurrent` 限流。

### 11.3 Plasma 内存

稳定节点需要额外空间存储副本。建议为稳定节点配置更大的 `--object-store-memory`。

---

## 12. 测试方案

### 12.1 单元测试

| 测试用例 | 覆盖组件 | Phase |
|---------|---------|-------|
| `IsPreemptibleNode()` 判断正确 | NodeManager | 1 |
| `SelectStableNode()` 排除可抢占节点 | NodeManager | 1 |
| `SelectStableNode()` 无稳定节点返回 Nil | NodeManager | 1 |
| `GetObjectSize()` 本地/不存在 | ObjectManager | 1 |
| `HandleReplicateObject` strategy=none 跳过 | NodeManager | 1 |
| `HandleReplicateObject` 对象太小跳过 | NodeManager | 1 |
| `HandleReplicateObject` 并发上限跳过 | NodeManager | 1 |
| `MaybeReplicateObject` 非可抢占节点不触发 | CoreWorker | 1 |
| `MaybeReplicateObject` strategy=none 不触发 | CoreWorker | 1 |
| Pin 转移触发条件 | ReferenceCounter | 2 |
| Pin 转移的先建后拆顺序 | ReferenceCounter | 2 |

### 12.2 集成测试

| 场景 | 预期 | Phase |
|------|------|-------|
| 可抢占节点 Task 返回大对象 → 节点下线 → ray.get | Phase 1: 依赖 LRU 未驱逐; Phase 2: 稳定获取 | 1+2 |
| 可抢占节点 Task 返回小对象 | 无 Push，Inline 返回 | 1 |
| 无稳定节点 → 节点下线 | 退化为重算 | 1 |
| 高并发 Push 超过 max_concurrent | 部分跳过 | 1 |
| Owner 在可抢占节点 → Owner 节点下线 | 重算（已知限制） | - |

---

## 13. 实施计划

### Phase 1：核心数据推送 ✅ 已完成

22 个文件，+309/-11 行。

| 组件 | 文件 |
|------|------|
| 配置项 | `ray_config_def.h` |
| Proto/RPC | `node_manager.proto`, `node_manager_server.h` |
| NodeManager | `node_manager.h/cc` |
| ObjectManager | `object_manager.h/cc`, `mock/object_manager.h` |
| FlatBuffers | `node_manager.fbs` |
| IPC Client | `raylet_ipc_client*` (interface, impl, fake) |
| RayletClient | `raylet_client*` (interface, impl, fake) |
| CoreWorker | `core_worker.h/cc`, `core_worker_options.h`, `core_worker_process.cc` |
| Metrics | `metrics.h` |

### Phase 2：Pin 转移（待实现）

| 文件 | 改动 |
|------|------|
| `reference_counter.h/cc` | `AddObjectLocationInternal` 中检测 Pin 在可抢占节点，触发 `TriggerPinTransfer` |
| `reference_counter.h/cc` | 实现 `TriggerPinTransfer`：PinObjectIDs(稳定节点) → UpdatePinnedAt → 释放旧 Pin |
| `reference_counter.h/cc` | 新增 `IsPreemptibleNode(NodeID)` 查询节点标签 |

**Pin 转移的关键逻辑**：

```
AddObjectLocationInternal(node_id):
  if 新增 location 成功:
    if owned_by_us_ && pinned_at 在可抢占节点 && node_id 是稳定节点:
      TriggerPinTransfer(object_id, node_id)

TriggerPinTransfer(object_id, stable_node_id):
  raylet_client(stable_node) → PinObjectIDs(object_id)
    callback:
      if success:
        UpdateObjectPinnedAtRaylet(object_id, stable_node_id)
        // 旧 Pin 通过 WORKER_OBJECT_EVICTION 或 FreeObjects 释放
```

### Phase 3：应用适配（待实现）

| 文件 | 改动 |
|------|------|
| `python/ray/data/_internal/execution/operators/hash_shuffle.py` | `ray.put()` 改为 return data |

---

## 14. 后续演进

### 14.1 备份策略扩展

当前 `object_replication_strategy = "push_to_stable_node"` 是唯一策略。未来可扩展：

| 策略 | 说明 | 适用场景 |
|------|------|---------|
| `push_to_stable_node` | 推送到集群中的稳定节点 | 混合实例集群（当前） |
| `external_storage` | 推送到外部存储（S3/HDFS） | 无稳定节点或需要持久化 |
| `multi_replica` | 推送到多个节点 | 高可用要求 |

代码扩展点在 `HandleReplicateObject` 中添加 `else if (strategy == "...")` 分支。

### 14.2 Owner 迁移（长期）

解决 [7.1](#71-p0-owner-在可抢占节点的问题) 需要 Owner 迁移能力：

- 修改 `HandleAssignObjectOwner` 支持转移 Owner 到稳定节点
- 涉及 ReferenceCounter、GCS、所有 borrower 的通知
- 复杂度高，作为独立项目推进

### 14.3 预测性迁移

结合 Autoscaler 的 preemption 通知（`AUTOSCALER_DRAIN_PREEMPTED`），在收到回收预警时加速迁移。

### 14.4 Push 完成回调

给 `ObjectManager::Push` 添加 `StatusCallback` 参数，实现：
- 精确的并发计数
- Push 失败后重试
- metrics 区分 "发起" 和 "完成"

---

## 附录：改动文件清单（Phase 1）

| # | 文件 | 改动类型 | 说明 |
|---|------|---------|------|
| 1 | `src/ray/common/ray_config_def.h` | 新增 | 4 个配置项 |
| 2 | `src/ray/protobuf/node_manager.proto` | 新增 | ReplicateObject RPC + messages |
| 3 | `src/ray/rpc/node_manager/node_manager_server.h` | 修改 | 注册 handler |
| 4 | `src/ray/raylet/node_manager.h` | 新增 | 方法声明 + 成员 |
| 5 | `src/ray/raylet/node_manager.cc` | 新增 | 3 个方法实现 + 注册回复 |
| 6 | `src/ray/object_manager/object_manager.h` | 修改 | 接口加 Push + GetObjectSize |
| 7 | `src/ray/object_manager/object_manager.cc` | 新增 | GetObjectSize 实现 |
| 8 | `src/ray/flatbuffers/node_manager.fbs` | 修改 | is_preemptible_node 字段 |
| 9 | `src/ray/raylet_ipc_client/raylet_ipc_client_interface.h` | 修改 | RegisterClient 加出参 |
| 10 | `src/ray/raylet_ipc_client/raylet_ipc_client.h` | 修改 | 同上 |
| 11 | `src/ray/raylet_ipc_client/raylet_ipc_client.cc` | 修改 | 提取字段 |
| 12 | `src/ray/raylet_ipc_client/fake_raylet_ipc_client.h` | 修改 | fake 更新 |
| 13 | `src/ray/core_worker/core_worker_options.h` | 新增 | is_preemptible_node 字段 |
| 14 | `src/ray/core_worker/core_worker_process.cc` | 修改 | 传递 is_preemptible_node |
| 15 | `src/ray/raylet_rpc_client/raylet_client_interface.h` | 新增 | ReplicateObject 接口 |
| 16 | `src/ray/raylet_rpc_client/raylet_client.h` | 新增 | 声明 |
| 17 | `src/ray/raylet_rpc_client/raylet_client.cc` | 新增 | 实现 |
| 18 | `src/ray/raylet_rpc_client/fake_raylet_client.h` | 新增 | fake |
| 19 | `src/ray/core_worker/core_worker.h` | 新增 | 成员 + 方法声明 |
| 20 | `src/ray/core_worker/core_worker.cc` | 修改 | 核心逻辑 |
| 21 | `src/ray/raylet/metrics.h` | 新增 | 3 个 metrics |
| 22 | `src/mock/ray/object_manager/object_manager.h` | 新增 | Push + GetObjectSize mock |
