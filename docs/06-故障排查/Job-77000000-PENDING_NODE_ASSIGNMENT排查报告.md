# Job 77000000 卡在 PENDING_NODE_ASSIGNMENT 排查报告

**日期**: 2026-08-24
**集群**: kce-aip
**Job ID**: 77000000
**Ray 版本**: `2.54.0+kuaishou.a760958466`（未包含修复 commit `82d05363`）
**Head 节点**: 10.137.44.181 (CST UTC+8)
**Worker 节点**: 8ae1cc62/public-bjx-c26-kce-node272 (10.53.82.106, UTC 时区)
**作业入口**: StreamingRepartition `_map_task`

---

## 一、问题描述

作业 77000000 中 9 个 StreamingRepartition `_map_task` 长期处于 `PENDING_NODE_ASSIGNMENT` 状态，无法调度执行。集群资源充足（CPU 有大量空闲），但 task 始终无法被分配到 worker。

---

## 二、关键证据

### 2.1 9 个卡住 lease 详情

通过全量提取差集方法定位 9 个卡住的 lease：

| 目标节点 | 卡住 lease 数量 | 发出时间 (CST) |
|---|---|---|
| d041357f | 3 | 11:06~11:10 |
| b286ce11 | 2 | 11:06~11:10 |
| 8ae1cc62 | 1 | 11:06~11:10 |
| eaf028d0 | 1 | 11:06~11:10 |
| 2facb5c3 | 1 | 11:06~11:10 |
| f326 (8ae1cc62) | 1 | 11:06~11:10 |

### 2.2 8ae1cc62 节点 state-dump 数据

```
[state-dump] Number of granted lease arguments: 48
[state-dump] Total size of pinned lease arguments: 22048319656    ← 22.05 GB
[state-dump] num_waiting_for_plasma_memory: 1
[state-dump] Grant queue length: 1
```

同时 CPU 可用：620,000/700,000（62/70 核空闲）—— CPU 充足但无法调度。

### 2.3 如何从日志中获取这些指标

这些值来自 raylet 的 **state-dump**，每分钟自动输出到 `/tmp/ray/session_latest/logs/raylet.out`。对应源码在 `local_lease_manager.cc:1204-1207`：

```cpp
buffer << "Number of granted lease arguments: " << granted_lease_args_.size() << "\n";
buffer << "Number of pinned lease arguments: " << pinned_lease_arguments_.size()
buffer << "Total size of pinned lease arguments: " << pinned_lease_arguments_bytes_
```

### 2.4 查看末尾日志的方法

raylet.out 文件通常很大（千万行+），直接 grep/cat 会卡死 I/O。推荐方法：

**方法 1：tail -c 只读尾部 500KB**
```bash
tail -c 500000 /tmp/ray/session_latest/logs/raylet.out | \
  grep -E "granted lease arguments|Total size of pinned|num_waiting_for_plasma|Grant queue length" | tail -8
```

**方法 2：Python 只读文件末尾**（更安全，适用于超大文件）
```bash
python3 -c "
import os
path = '/tmp/ray/session_latest/logs/raylet.out'
size = os.path.getsize(path)
with open(path, 'rb') as f:
    f.seek(max(0, size - 500000))
    text = f.read().decode('utf-8', errors='replace')
keywords = ['granted lease arguments', 'Total size of pinned', 'num_waiting_for_plasma', 'Grant queue length']
for line in text.split(chr(10)):
    if any(k in line for k in keywords):
        print(line)
"
```

**方法 3：快捷脚本**（已在远端节点创建）
```bash
bash /tmp/check_args.sh
```

脚本内容：
```bash
#!/bin/bash
tail -c 500000 /tmp/ray/session_latest/logs/raylet.out | \
  grep -E "granted lease arguments|Total size of pinned|num_waiting_for_plasma|Grant queue length|is_idle" | tail -10
```

---

## 三、max_pinned_lease_arguments_bytes_ 计算

### 3.1 object_store_memory 确定

从 state-dump 的 `InitialConfigResources` 中：
```
object_store_memory: 3.2e+10
```

Ray 内部资源表示中 `320000000000000` = 3.2e+10 × 10000（Ray 资源单位换算），所以原始 bytes：
```
object_store_memory = 32,000,000,000 bytes = 32 GB
```

### 3.2 阈值计算

源码 `src/ray/common/ray_config_def.h:726`：
```cpp
RAY_CONFIG(float, max_task_args_memory_fraction, 0.7)
```

源码 `src/ray/raylet/main.cc:933`：
```cpp
auto max_task_args_memory =
    static_cast<int64_t>(static_cast<float>(object_manager->GetMemoryCapacity()) *
                         RayConfig::instance().max_task_args_memory_fraction());
```

因此：
```
max_pinned_lease_arguments_bytes_ = 32 GB × 0.7 = 22.4 GB
```

### 3.3 泄漏量对比

```
pinned_lease_arguments_bytes = 22,048,319,656 bytes ≈ 22.05 GB
max_pinned_lease_arguments_bytes_ = 22.4 GB

22.05 GB / 22.4 GB ≈ 98.4%  ← 已几乎打满阈值！
```

新 lease 尝试 pin args 时：22.05 GB + 新 lease args > 22.4 GB → `PinLeaseArgsIfMemoryAvailable` 返回 false → `WAITING_FOR_AVAILABLE_PLASMA_MEMORY` → 死锁。

### 3.4 关于之前错误计算的勘误

早期分析曾错误地将 `object_store_memory` 内部资源单位 `320000000000000` 误解为原始 bytes，得出 `max_pinned = 224 GB` 的错误结论。实际上 Ray 内部资源表示乘了 10000 的换算系数，真实值是 `32 GB × 0.7 = 22.4 GB`。

---

## 四、Bug 1：Raylet 端 pinned lease args 泄漏

### 4.1 原始文档的描述（不准确）

原始排查文档说 `HandleReturnWorkerLease` "❌ 完全缺少 `ReleaseLeaseArgs` 调用"——这个描述**不完全准确**。正常路径（`worker_exiting=false`）间接调用 `ReleaseLeaseArgs` 是存在的。

### 4.2 已确认的泄漏根因：HandleUnexpectedWorkerFailure / HandleNodeRemoved 直接 KillAsync

经过深入代码分析，**确认的泄漏路径是 `HandleUnexpectedWorkerFailure`（`node_manager.cc:998`）和 `HandleNodeRemoved`（`node_manager.cc:954`）中直接调 `KillAsync` 租赁 worker，不经过 `DestroyWorker`**。

#### 泄漏机制

`KillAsync` 会设置 `IsDead()=true`（`killing_=true`）。后续 `DisconnectClient` 中 CleanupLease 的条件是 `!worker->IsDead()`（`node_manager.cc:1465`），因为 `IsDead()=true` 而跳过 CleanupLease → `ReleaseLeaseArgs` 不被调用 → pinned args 永久泄漏。

而 `DestroyWorker` 的调用顺序是**先 `DisconnectClient`（此时 `IsDead()=false`）再 `KillAsync`**，保证了 CleanupLease 可被调到。所以 `DestroyWorker` 路径不会泄漏。

#### 完整泄漏调用链

```
节点 A 上 worker X 异常死亡
  → 节点 A 的 raylet DisconnectClient(X)
    → AsyncReportWorkerFailure(X) → GCS
    → GCS 广播 WorkerDeltaData 给所有 raylet
  → 节点 B 收到后 HandleUnexpectedWorkerFailure(X)
    → cluster_lease_manager_.CancelAllLeasesOwnedBy(X)
      ← 只 cancel 队列中的 lease，不管已 granted 的
    → for (leased_workers_): 找 owner=X 的租赁 worker Y
    → worker->KillAsync(Y)           ← 直接 KillAsync，不经过 DestroyWorker
    → Y.IsDead() = true
    → Y 退出后:
      → 路径1: Y 发 graceful DisconnectClient → !IsDead()=false → 跳过 CleanupLease
      → 路径2: Y 连接断开 → DestroyWorker → DisconnectClient → !IsDead()=false → 跳过 CleanupLease
    → 节点 B 上 Y 的 pinned_lease_arguments 永久泄漏
```

`HandleNodeRemoved`（owner 节点死亡，`:954`）同理：遍历 `leased_workers_`，找 `owner_node_id=死亡节点` 的租赁 worker → 直接 `KillAsync` → 同上泄漏。

### 4.3 三种 KillAsync/MarkDead 调用路径对比

| 场景 | 调用方式 | IsDead() 时序 | CleanupLease |
|---|---|---|---|
| **owner worker 死亡** (`:998`) | `KillAsync`（不经过 DestroyWorker） | KillAsync 后 = true | ❌ **泄漏** |
| **owner 节点死亡** (`:954`) | `KillAsync`（不经过 DestroyWorker） | KillAsync 后 = true | ❌ **泄漏** |
| OOM kill (`:3235`) | `DestroyWorker` → 先 DisconnectClient 再 KillAsync | DisconnectClient 时 = false | ✅ |
| 连接断开检测 (`:615`) | `DestroyWorker` | 先 DisconnectClient (false) 再 KillAsync | ✅ |
| PG bundle 释放 (`:672`) | `DestroyWorker` | 先 DisconnectClient (false) 再 KillAsync | ✅ |
| GCS 释放 actor (`:2236`) | `DestroyWorker` | 先 DisconnectClient (false) 再 KillAsync | ✅ |
| idle worker Exit 成功 (`:1274`) | `MarkDead` | worker 已 idle 无 lease | 无影响 |
| idle worker Exit 失败 (`:582`) | `KillAsync` force | worker 已 idle 无 lease | 无影响 |
| actor timeout 被 GCS kill (`:3586`) | `DestroyWorker` | 先 DisconnectClient (false) 再 KillAsync | ✅ |

### 4.4 泄漏场景详细分析

#### 场景 1：Owner worker 异常死亡 → HandleUnexpectedWorkerFailure

节点 A 上 worker X 异常死亡 → GCS 广播 → 节点 B 上 owner=X 的租赁 worker Y 被 KillAsync → 泄漏。

Worker X 异常死亡的具体触发（X 必须是某个远程租赁 worker 的 owner，在 Ray Data 场景下执行算子的 worker 提交远程 task 时自动成为 owner）：

| disconnect_type | 场景 | 频率 |
|---|---|---|
| SYSTEM_ERROR | 进程崩溃/SIGSEGV/连接断开/OOM Killer 杀进程 | 中等（8ae1cc62 有 12 次） |
| NODE_OUT_OF_MEMORY | raylet 内存监控超限主动 kill | 低（8ae1cc62 有 3 次） |
| INTENDED_SYSTEM_EXIT | PG bundle 释放/GCS 释放 actor | 低 |

注意：X 的 disconnect_type 是什么不重要，关键是 X 在节点 A 上的 `DisconnectClient` 会调 `AsyncReportWorkerFailure` → GCS 广播 → 节点 B 收到后 KillAsync Y。

#### 场景 2：Owner 节点被 GCS 标记 dead → HandleNodeRemoved

GCS 心跳超时标记节点 A dead → 广播 `NodeRemoved(A)` → 节点 B 上 owner_node=A 的租赁 worker Y 被 KillAsync → 泄漏。

触发条件：
- k8s Pod 被抢占/驱逐/重启
- 网络分区导致 GCS 误判 dead
- 节点负载过高心跳超时

前提条件：owner 必须在死亡节点上。如果 GCS 和 raylet 同 Pod（KML 典型架构），head 挂了 GCS 也挂了，不会广播 `NodeRemoved`，不会触发此路径。

#### 场景 3：Head 上 driver 异常死亡 → HandleUnexpectedWorkerFailure

Head 节点上 driver 异常退出 → `DisconnectClient(driver)` → `AsyncReportWorkerFailure` → GCS 广播 → worker 节点 `HandleUnexpectedWorkerFailure(driver)` → 遍历 `leased_workers_` 找 owner=driver 的租赁 worker → KillAsync → 泄漏。

在 Ray Data 场景下，很多 task 的 owner 是 driver 本身（driver 提交的根 task），所以 driver 死亡会导致大量远程租赁 worker 被 KillAsync → 大量泄漏。

#### 场景 4：Head raylet 挂掉重启

分两种情况：

- **GCS 和 raylet 同 Pod（KML 典型架构）**：head 挂了 GCS 也挂了，不会广播 `NodeRemoved`/`WorkerFailure`。重启后 GCS 发 `HandleNotifyGCSRestart` 重新订阅，**不触发泄漏**。
- **GCS 单独部署**：GCS 心跳超时标记 head dead → 广播 `NodeRemoved(head)` → KillAsync 租赁 worker → **泄漏**。即使 head 之后重启，泄漏已发生无法恢复。

### 4.5 worker_exiting=true 路径分析

`worker_exiting=true` 路径跳过 `HandleWorkerAvailable`，不立即调 CleanupLease。但 worker 自己退出后走 `DestroyWorker`（连接断开检测），`DestroyWorker` 先 `DisconnectClient`（此时 `IsDead()=false`）再 `KillAsync`，CleanupLease 通常可被调到。

**只有当 worker 退出前已被其他代码调了 `KillAsync`/`MarkDead`（如 owner 死亡场景中 KillAsync 的连锁效应），兜底才失败。**

所以 `worker_exiting=true` 路径**不是**主要泄漏根因，主要根因是 `HandleUnexpectedWorkerFailure`/`HandleNodeRemoved` 的直接 `KillAsync`。

### 4.6 8ae1cc62 数据对照

| disconnect_type | 次数 | 是否触发泄漏 |
|---|---|---|
| INTENDED_USER_EXIT (3) | 1718 | 否（正常退出，不是 owner） |
| INTENDED_SYSTEM_EXIT (1) | 687 | 否（idle kill 等，不是 owner） |
| SYSTEM_ERROR (0) | 12 | **可能**（如果这些 worker 是远程 lease 的 owner） |
| NODE_OUT_OF_MEMORY (4) | 3 | **可能**（同上） |

12 + 3 = 15 次异常 disconnect，如果其中一部分是其他节点租赁 worker 的 owner，每次可能泄漏若干个 pinned args。7 周内基线从 0 涨到 48，平均每周约 7 个，和 15 次异常 disconnect 的量级吻合。

### 4.7 HandleReturnWorkerLease 完整逻辑（补充参考）

```cpp
void NodeManager::HandleReturnWorkerLease(...) {
  auto lease_id = LeaseID::FromBinary(request.lease_id());
  if (!leased_workers_.contains(lease_id)) {
    send_reply_callback(Status::OK(), nullptr, nullptr);
    return;
  }
  std::shared_ptr<WorkerInterface> worker = leased_workers_[lease_id];
  ReleaseWorker(lease_id);              // 从 leased_workers_ 移除

  if (request.disconnect_worker()) {
    // 路径 A: worker 需要被销毁
    DisconnectClient(worker->Connection(), ...);
  } else {
    // 路径 B: worker 正常返回
    if (worker->IsBlocked()) {
      HandleNotifyWorkerUnblocked(worker);
    }
    local_lease_manager_.ReleaseWorkerResources(worker);  // 只释放 CPU 资源

    if (!request.worker_exiting()) {
      // 路径 B1: worker 不退出 → 加入 idle pool
      HandleWorkerAvailable(worker);
      // ↑ HandleWorkerAvailable 内部调用 CleanupLease → ReleaseLeaseArgs ✅
    }
    // 路径 B2: worker_exiting=true → 跳过 HandleWorkerAvailable ❌
    // 但兜底路径（worker 退出后 DestroyWorker）通常能释放
  }
}
```

### 4.8 CleanupLease 的代码（local_lease_manager.cc:766-776）

```cpp
void LocalLeaseManager::CleanupLease(std::shared_ptr<WorkerInterface> worker,
                                     RayLease *lease) {
  RAY_CHECK(worker != nullptr && lease != nullptr);
  *lease = worker->GetGrantedLease();
  RemoveFromGrantedLeasesIfExists(*lease);
  ReleaseLeaseArgs(lease->GetLeaseSpecification().LeaseId());  // ← 释放 pinned args
  if (worker->GetAllocatedInstances() != nullptr) {
    ReleaseWorkerResources(worker);
  }
}
```

### 4.9 77000000 的实际数据

- 77000000 的 driver 日志**没有** `worker_exiting` 事件
- 77000000 的 task 全部正常完成，走正常路径（B1），间接调用了 `ReleaseLeaseArgs`
- **48 个泄漏的 args 在 77000000 启动前就已存在**（7月31日就达到 48，77000000 8月24日才启动），来自其他历史作业

### 4.10 granted_lease_args 数据趋势分析

granted_lease_args 的值有波动（会下降），说明 `ReleaseLeaseArgs` 在正常路径中确实被调用。但**基线持续上升**：

| 时间 | granted_lease_args 峰值 | 回落后基线 | 净增 |
|---|---|---|---|
| 7月初 | 0 | 0 | 0 |
| 7月中 | 17 | ~15 | +15 |
| 7月20日 | 22 | ~18 | +3 |
| 7月25日 | 34 | ~30 | +12 |
| 7月28日 | 38 | ~35 | +5 |
| 7月31日 | 48 | ~45 | +10 |
| 8月24日 (77000000 启动) | 48 | 48 | ← 77000000 是受害者 |

每次峰值后回不到之前基线，净增几个。每次某个节点有 worker 异常死亡，其他节点上被它拥有的租赁 worker 就会泄漏 pinned args。

### 4.11 结论

- **已确认的泄漏根因**：`HandleUnexpectedWorkerFailure`/`HandleNodeRemoved` 中直接 `KillAsync` 租赁 worker（不经过 `DestroyWorker`），`IsDead()=true` 导致后续 `DisconnectClient` 跳过 `CleanupLease`
- **`worker_exiting=true` 路径**不是主要泄漏根因，因为 worker 自己退出后走 `DestroyWorker`，CleanupLease 通常可被调到
- 正常路径（`worker_exiting=false`）间接调用 `ReleaseLeaseArgs` 是存在的，原文说"完全缺少"不准确
- 48 个泄漏来自历史作业，77000000 只是受害者

### 4.9 HandleReturnWorkerLease 中 ReleaseWorker 不清除 GrantLeaseId

```cpp
// node_manager.h:363-367
void ReleaseWorker(const LeaseID &lease_id) {
    RAY_CHECK(leased_workers_.contains(lease_id));
    leased_workers_.erase(lease_id);  // 仅从 map 移除
    SetIdleIfLeaseEmpty();
}
```

`ReleaseWorker` 只从 `leased_workers_` map 中移除映射，**不清除 worker 对象上的 `GrantLeaseId`**。所以 `HandleWorkerAvailable` 中 `worker->GetGrantedLeaseId().IsNil()` 仍然为 false（因为 lease_id 还在 worker 上），会走到 `CleanupLease` → `ReleaseLeaseArgs`。

### 4.10 ReleaseWorkerResources 不清除 GrantLeaseId 和 AllocatedInstances 的关联

```cpp
// local_lease_manager.cc:1058-1088
void LocalLeaseManager::ReleaseWorkerResources(std::shared_ptr<WorkerInterface> worker) {
  auto allocated_instances = worker->GetAllocatedInstances()
                                 ? worker->GetAllocatedInstances()
                                 : worker->GetLifetimeAllocatedInstances();
  if (allocated_instances == nullptr) return;
  // ... 释放 CPU 资源 ...
  cluster_resource_scheduler_.GetLocalResourceManager().ReleaseWorkerResources(
      allocated_instances);
  worker->ClearAllocatedInstances();
  worker->ClearLifetimeAllocatedInstances();
}
```

`ReleaseWorkerResources` 只释放 CPU 资源实例并 `ClearAllocatedInstances()`，不会清除 `GrantLeaseId`。这保证了后续 `HandleWorkerAvailable → CleanupLease` 中 `LocalLeaseManager::CleanupLease` 的 `worker->GetAllocatedInstances() == nullptr` 检查不会 double-free CPU，但 `ReleaseLeaseArgs` 和 `RemoveFromGrantedLeasesIfExists` 仍然正常执行。

### 4.11 路径 B2 的 DisconnectClient 兜底分析

路径 B2（`worker_exiting=true`）跳过 `HandleWorkerAvailable` 后，期望 worker 进程自行退出后触发 `DisconnectClient`。但 `DisconnectClient` 中的 CleanupLease 调用有条件：

```cpp
// node_manager.cc:1425-1437
void NodeManager::DisconnectClient(...) {
  // ...
  if (leased_workers_.contains(worker->GetGrantedLeaseId())) {
    ReleaseWorker(worker->GetGrantedLeaseId());
  }
  // ...
}
```

而 `HandleReturnWorkerLease` 在调用 `DisconnectClient` 之前已经执行了 `ReleaseWorker(lease_id)`（从 `leased_workers_` 移除），所以如果走路径 A（`disconnect_worker=true`），`DisconnectClient` 中的 `leased_workers_.contains()` 为 false，不会 double-release。但 `DisconnectClient` 的完整逻辑中仍有其他路径触发 `CleanupLease`（如 worker 仍非 dead 状态），需结合具体代码版本分析。

### 4.12 Driver 端 Locality-Aware 选节点的完整代码逻辑

Driver 首次选节点时 `raylet_address == nullptr`，使用 `LocalityAwareLeasePolicy::GetBestNodeForLease()`（`lease_policy.cc:24-87`）：

```cpp
std::pair<rpc::Address, bool> LocalityAwareLeasePolicy::GetBestNodeForLease(
    const LeaseSpecification &spec) {
  // 优先级 1: Spread 策略
  if (spec.GetMessage().scheduling_strategy().scheduling_strategy_case() ==
      rpc::SchedulingStrategy::kSpreadSchedulingStrategy) {
    return std::make_pair(fallback_rpc_address_, false);  // 回退到 head 节点
  }

  // 优先级 2: Node Affinity（硬亲和/label selector）
  if (auto node_id_values = GetHardNodeAffinityValues(spec.GetLabelSelector())) {
    for (const auto &node_id_hex : *node_id_values) {
      if (auto addr = node_addr_factory_(NodeID::FromHex(node_id_hex))) {
        return std::make_pair(addr.value(), false);
      }
    }
    return std::make_pair(fallback_rpc_address_, false);
  }

  // 优先级 3: Node Affinity Scheduling Strategy
  if (spec.IsNodeAffinitySchedulingStrategy()) {
    if (auto addr = node_addr_factory_(spec.GetNodeAffinitySchedulingStrategyNodeId())) {
      return std::make_pair(addr.value(), false);
    }
    return std::make_pair(fallback_rpc_address_, false);
  }

  // 优先级 4: 数据本地性选择（默认路径）
  if (auto node_id = GetBestNodeIdForLease(spec)) {
    if (auto addr = node_addr_factory_(node_id.value())) {
      return std::make_pair(addr.value(), true);  // ← 第二个值=true 表示基于 locality
    }
  }
  // 无 locality 信息 → fallback
  return std::make_pair(fallback_rpc_address_, false);
}
```

**`GetBestNodeIdForLease` 的逻辑**（`lease_policy.cc:60-87`）：

1. 遍历 lease 的所有依赖对象 `spec.GetDependencyIds()`
2. 对每个对象查 `locality_data_provider_.GetLocalityData(object_id)`，获取该对象在哪些节点上有本地副本及其大小
3. 统计每个节点上的**本地对象字节总和**
4. 选字节数最大的节点返回
5. 如果没有 locality 信息（如无依赖对象），返回 `std::nullopt`，fallback 到 head 节点

**关键**：Driver 首次选节点时 `raylet_address == nullptr`，`is_spillback = false`，`grant_or_reject = false`。这决定了后续 raylet 端在无法调度时不会 reject，只会 redirect（如果走到 Spillback），但 pinned 内存超限时根本没走到 Spillback。

---

## 五、Bug 2：Driver 端 gRPC 永不超时

### 5.1 问题

`ReturnWorkerLease`（`raylet_client.cc:106-127`）和 `RequestWorkerLease` 的 `method_timeout_ms=-1`，导致 `RetryableGrpcClient` 永不超时。

```cpp
// raylet_client.cc:125
INVOKE_RETRYABLE_RPC_CALL(
    retryable_grpc_client_,
    NodeManagerService,
    ReturnWorkerLease,
    request,
    [](const Status &status, rpc::ReturnWorkerLeaseReply &&reply) {
      RAY_LOG_IF_ERROR(INFO, status) << "Error returning worker: " << status;
    },
    grpc_client_,
    /*method_timeout_ms*/ -1);  // ← 永不超时
```

### 5.2 修复 commit `82d05363` 的内容

- 给 `RequestWorkerLease` 在 `grant_or_reject=true`（spillback）时加了 `method_timeout_ms=600000`（10 分钟超时）
- 增加了 `grpc_max_ready_idle_resend_count=10`（READY/IDLE 通道连续重发 10 次后 fail 所有 pending requests）
- **只修复了 driver 端超时，未修复 raylet 端的 pinned args 泄漏**
- 修复 commit 未合入运行集群版本 `2.54.0+kuaishou.a760958466`

### 5.3 修复只覆盖了 spillback 路径

注意修复只在 `grant_or_reject=true` 时加超时，`grant_or_reject=false`（locality-aware 路径）时 `method_timeout_ms` 仍然是 -1。这意味着 locality-aware 路径的死锁可能仍未完全修复。

---

## 六、为什么不会 reject/spillback

### 6.1 grant_or_reject 对 raylet 行为的影响

`grant_or_reject` 标志决定了 raylet 端在无法调度 lease 时的行为，是理解整个死锁机制的关键。

**Driver 端设值逻辑**（`normal_task_submitter.cc:313-330`）：

```cpp
const bool is_spillback = (raylet_address != nullptr);
// ...
raylet_client->RequestWorkerLease(
    lease_spec.GetMessage(),
    /*grant_or_reject=*/is_spillback,
    ...);
```

| 场景 | raylet_address | is_spillback | grant_or_reject |
|------|---------------|-------------|-----------------|
| Driver 首次选节点（locality-aware） | nullptr → 选后赋值 | false | false |
| 本地 raylet redirect 后重试 | 非空 | true | true |

**raylet 端 ClusterLeaseManager::ScheduleOnNode 的行为**（`cluster_lease_manager.cc:422-435`）：

| grant_or_reject | raylet 无法本地调度时 | driver 收到的回复 | driver 行为 |
|-----------------|---------------------|-------------------|------------|
| true (spillback) | 立即 reject | `reply.rejected()=true` | `RequestNewWorkerIfNeeded()` 本地重试 |
| false (locality) | 设 `retry_at_raylet_address` redirect | redirect 地址 | 去新 raylet 重试 |

**LocalLeaseManager::Spillback 的行为**（`local_lease_manager.cc:674-708`）：

```cpp
void LocalLeaseManager::Spillback(const NodeID &spillback_to,
                                  const std::shared_ptr<internal::Work> &work) {
  if (work->grant_or_reject_) {
    // grant_or_reject=true → reject
    reply->set_rejected(true);
    reply_callback.send_reply_callback_(Status::OK(), nullptr, nullptr);
    return;
  }
  // grant_or_reject=false → redirect（设 retry_at_raylet_address）
  // ...
}
```

**关键**：`grant_or_reject=false` 时 raylet 不会 reject，而是 redirect。但在 pinned 内存超限场景中，raylet 既不 reject 也不 redirect（见下文分析）。

### 6.2 PinLeaseArgsIfMemoryAvailable 在资源分配之前

pinned args 超限发生在 `AllocateLocalTaskResources` **之前**：

```cpp
// local_lease_manager.cc:315-349 — GrantScheduledLeasesToWorkers 内部
bool args_missing = false;
bool success = PinLeaseArgsIfMemoryAvailable(spec, &args_missing);  // ← 先检查 pin
if (!success) {
    if (args_missing) {
        // 对象被驱逐，放回等待队列
        // ...
    } else {
        // ← 本案例走这里！pinned 内存超限
        // ❌ 没有调用 TrySpillback()！
        work->SetStateWaiting(
            internal::UnscheduledWorkCause::WAITING_FOR_AVAILABLE_PLASMA_MEMORY);
        work_it++;  // 跳过，不回复 driver，不 reject，不 redirect
    }
    continue;
}
// 下面才是资源分配
auto allocated_instances = std::make_shared<TaskResourceInstances>();
bool schedulable = AllocateLocalTaskResources(...);  // ← 资源不足时 TrySpillback
```

### 6.3 TrySpillback 只在两个地方被调用

| 调用位置 | 条件 | 场景 |
|---------|------|------|
| `local_lease_manager.cc:305` | Worker 容量超限（scheduling class cap） | 无法创建新 worker |
| `local_lease_manager.cc:364` | CPU 资源不足（`AllocateLocalTaskResources` 失败） | CPU 不够 |

**pinned 内存超限这条路径完全没有 `TrySpillback` 调用，这是设计遗漏。**

### 6.4 完整死锁机制

```
PinLeaseArgsIfMemoryAvailable 返回 false (pinned 超限)
  → lease 设为 WAITING_FOR_AVAILABLE_PLASMA_MEMORY
  → 不回复 driver（没有 send_reply_callback）
  → 不调 TrySpillback（设计遗漏）
  → 不 reject（grant_or_reject=false 时 Spillback() 才 reject，但没走到）
  → 不 redirect（同上）
  → 等待其他 lease 释放 args 后重新调度
  → 但泄漏的 args 永远不释放
  → 死锁
```

即使 `grant_or_reject=true`（spillback 场景），pinned 内存超限时也不会 reject，因为代码没有走到 `Spillback()` 函数。

### 6.5 Core Worker 端 lease 请求的完整起始点代码逻辑

#### 6.5.1 is_spillback 和 grant_or_reject 的含义

**`is_spillback`** 是 core worker 层（`NormalTaskSubmitter`）的概念，所有 core worker（包括 `WorkerType::DRIVER` 和 `WorkerType::WORKER`）提交 normal task 都走同一套逻辑：

- `is_spillback = false`：core worker 自己选节点（`lease_policy_->GetBestNodeForLease`）发起的请求，接受 redirect
- `is_spillback = true`：core worker 拿到 redirect 地址后向指定 raylet 重试，不再接受 redirect

**`grant_or_reject`** 是 raylet 端的语义，值等于 `is_spillback`：

- `false`：允许 redirect（raylet 可以把 lease 转发到别的节点）
- `true`：必须本地 grant 或 reject，不能再 redirect

设计意图：避免 lease 请求在节点间无限转发（redirect 链），spillback 请求最多走两跳。

```cpp
// normal_task_submitter.cc:323
const bool is_spillback = (raylet_address != nullptr);

// normal_task_submitter.cc:340
raylet_client->RequestWorkerLease(
    lease_spec.GetMessage(),
    /*grant_or_reject=*/is_spillback,   // 直接传 is_spillback
    ...);
```

注意：`is_spillback` 在 `GetBestNodeForLease` 之前就计算好了，即使选到远端节点，`grant_or_reject` 仍然是 `false`。

#### 6.5.2 Core Worker 首次请求流程

Core Worker 调用 `RequestNewWorkerIfNeeded` 的入口来自 `SubmitTask`（依赖解析完成后）或收到 redirect/reject 回复后。完整代码逻辑：

```cpp
// normal_task_submitter.cc:281-340
void NormalTaskSubmitter::RequestNewWorkerIfNeeded(const SchedulingKey &scheduling_key,
                                                   const rpc::Address *raylet_address) {
  auto &scheduling_key_entry = scheduling_key_entries_[scheduling_key];

  // 步骤 1: 限流检查 — pending lease 请求数不能超过上限
  if (scheduling_key_entry.pending_lease_requests.size() >=
      kMaxPendingLeaseRequestsPerSchedulingCategory) {
    return;  // 被限流
  }

  // 步骤 2: 如果有 idle worker，不需要请求新的
  if (!scheduling_key_entry.AllWorkersBusy()) {
    return;
  }

  // 步骤 3: 如果 task 队列为空，或所有 task 都已有 pending lease，不需要请求
  if (task_queue.empty() || task_queue.size() <= pending_lease_requests.size()) {
    return;
  }

  // 步骤 4: 生成唯一 lease ID
  static uint32_t lease_id_counter = 0;
  const LeaseID lease_id = LeaseID::FromWorker(worker_id_, lease_id_counter++);
  rpc::LeaseSpec lease_spec_msg = scheduling_key_entry.lease_spec.GetMessage();
  lease_spec_msg.set_lease_id(lease_id.Binary());

  // 步骤 5: 计算 is_spillback — 关键！在 GetBestNodeForLease 之前就计算
  const bool is_spillback = (raylet_address != nullptr);  // 首次调用时 nullptr → false
  bool is_selected_based_on_locality = false;

  // 步骤 6: 选择目标 raylet
  if (raylet_address == nullptr) {
    // 首次请求：由 lease policy 选最佳节点（基于数据本地性）
    std::tie(best_node_address, is_selected_based_on_locality) =
        lease_policy_->GetBestNodeForLease(lease_spec);
    raylet_address = &best_node_address;  // 赋值，但 is_spillback 已经算好了
  }
  // 否则：redirect 重试，直接用传入的 raylet_address

  // 步骤 7: 发送 gRPC 请求
  auto raylet_client = raylet_client_pool_->GetOrConnectByAddress(*raylet_address);
  raylet_client->RequestWorkerLease(
      lease_spec.GetMessage(),
      /*grant_or_reject=*/is_spillback,    // false（首次）或 true（redirect 重试）
      [this, scheduling_key, lease_id, is_spillback, raylet_address = *raylet_address](
          const Status &status, const rpc::RequestWorkerLeaseReply &reply) {
        // ... callback 逻辑（见 6.5.3）
      },
      scheduling_key_entry.BacklogSize(),
      is_selected_based_on_locality);
}
```

**RPC 层代码**（`raylet_client.cc:53-77`）：

```cpp
void RayletClient::RequestWorkerLease(
    const rpc::LeaseSpec &lease_spec,
    bool grant_or_reject,
    const rpc::ClientCallback<rpc::RequestWorkerLeaseReply> &callback,
    const int64_t backlog_size,
    const bool is_selected_based_on_locality) {
  rpc::RequestWorkerLeaseRequest request;
  request.mutable_lease_spec()->CopyFrom(lease_spec);
  request.set_grant_or_reject(grant_or_reject);
  request.set_backlog_size(backlog_size);
  request.set_is_selected_based_on_locality(is_selected_based_on_locality);
  INVOKE_RETRYABLE_RPC_CALL(retryable_grpc_client_,
                            NodeManagerService,
                            RequestWorkerLease,
                            request,
                            callback,
                            grpc_client_,
                            /*method_timeout_ms*/ -1);  // ← 永不超时！
}
```

**关键**：`is_spillback` 在 `GetBestNodeForLease` 赋值 `raylet_address` 之前就计算好了。即使选到远端节点，`grant_or_reject` 仍然是 `false`。`method_timeout_ms = -1` 意味着 gRPC 永不超时。

#### 6.5.3 Core Worker 收到回复后的 4 种分支

```cpp
// normal_task_submitter.cc:358-454
if (status.ok()) {
    if (reply.canceled()) {
        // 根据失败类型决定重试或 fail task
    } else if (reply.rejected()) {
        RAY_CHECK(is_spillback);                    // ← 只有 spillback 才会 reject
        RequestNewWorkerIfNeeded(scheduling_key);   // ← 不带 raylet_address，从头选节点
    } else if (!reply.worker_address().node_id().empty()) {
        // Lease granted
        AddWorkerLeaseClient(...) + OnWorkerIdle(...);
    } else {
        // Redirect
        RAY_CHECK(!is_spillback);                   // ← 只有首次请求才会 redirect
        RequestNewWorkerIfNeeded(scheduling_key, &reply.retry_at_raylet_address());
        // 带 raylet_address → is_spillback=true → grant_or_reject=true
    }
} else if (raylet_address.node_id() != local_node_id_) {
    // 远端 raylet gRPC 失败 → 回退本地重试
    RequestNewWorkerIfNeeded(scheduling_key);
} else {
    // 本地 raylet 挂了 → 进程退出
    QuickExit();
}
```

| 回复类型 | 条件 | `is_spillback` 断言 | Core Worker 行为 |
|---|---|---|---|
| Canceled | `reply.canceled()` | 无 | 根据失败类型重试或 fail |
| **Rejected** | `reply.rejected()` | `RAY_CHECK(is_spillback)` | `RequestNewWorkerIfNeeded()` — 从头重新选节点 |
| Granted | `reply.worker_address()` 非空 | 无 | 分配 worker 执行 task |
| **Redirected** | 上述都不满足 | `RAY_CHECK(!is_spillback)` | `RequestNewWorkerIfNeeded(key, &retry_at_raylet_address)` — 带指定地址重试 |

**关键断言**：
- Rejected 只在 `is_spillback=true` 时出现（raylet 端 `grant_or_reject=true` 时才 reject）
- Redirected 只在 `is_spillback=false` 时出现（raylet 端 `grant_or_reject=false` 时才 redirect）

### 6.6 Raylet 端 ClusterLeaseManager 调度逻辑

#### 6.6.1 为什么远端 raylet 收到请求后还要走 ClusterLeaseManager

Core Worker 通过 `GetBestNodeForLease` 选节点时使用的是 GCS 广播的资源快照，存在延迟。请求到达远端 raylet 时，该节点的 CPU 可能已被占满。所以 raylet 需要用自己的实时资源视图重新评估，必要时 redirect 到别的节点。

但问题在于：`ClusterLeaseManager::GetBestSchedulableNode` **只看 CPU/GPU 等显式资源**，完全不考虑 pinned 内存约束。

#### 6.6.2 HandleRequestWorkerLease 入口

raylet 收到 `RequestWorkerLease` gRPC 请求后进入 `HandleRequestWorkerLease`，有多个前置检查：

```cpp
// node_manager.cc:1781-1859
void NodeManager::HandleRequestWorkerLease(rpc::RequestWorkerLeaseRequest request,
                                           rpc::RequestWorkerLeaseReply *reply,
                                           rpc::SendReplyCallback send_reply_callback) {
  auto lease_id = LeaseID::FromBinary(request.lease_spec().lease_id());

  // 前置检查 1: lease 已被 granted（retry 场景）→ 直接返回已 granted 的 worker 地址
  if (leased_workers_.contains(lease_id)) {
    const auto &worker = leased_workers_[lease_id];
    reply->set_worker_pid(worker->GetProcess().GetId());
    reply->mutable_worker_address()->set_ip_address(worker->IpAddress());
    reply->mutable_worker_address()->set_port(worker->Port());
    reply->mutable_worker_address()->set_worker_id(worker->WorkerId().Binary());
    reply->mutable_worker_address()->set_node_id(self_node_id_.Binary());
    send_reply_callback(Status::OK(), nullptr, nullptr);
    return;
  }

  // 前置检查 2: 调用方（caller worker/node）已经 dead → 立即 cancel
  RayLease lease{std::move(*request.mutable_lease_spec())};
  const auto caller_worker = WorkerID::FromBinary(lease.GetLeaseSpecification().CallerAddress().worker_id());
  const auto caller_node = NodeID::FromBinary(lease.GetLeaseSpecification().CallerAddress().node_id());
  if (!lease.GetLeaseSpecification().IsDetachedActor() &&
      (failed_workers_cache_.contains(caller_worker) ||
       failed_nodes_cache_.contains(caller_node))) {
    reply->set_canceled(true);
    reply->set_failure_type(rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_INTENDED);
    reply->set_scheduling_failure_message(
        "Cancelled leasing because the caller worker is dead.");
    send_reply_callback(Status::OK(), nullptr, nullptr);
    return;
  }

  // 前置检查 3: lease 已在 ClusterLeaseManager 或 LocalLeaseManager 队列中
  // （retry 场景）→ 添加 callback 到已有的 work 对象
  if (cluster_lease_manager_.IsLeaseQueued(scheduling_class, lease_id)) {
    RAY_CHECK(cluster_lease_manager_.AddReplyCallback(scheduling_class, lease_id,
        std::move(send_reply_callback_wrapper), reply));
    return;
  }
  if (local_lease_manager_.IsLeaseQueued(scheduling_class, lease_id)) {
    RAY_CHECK(local_lease_manager_.AddReplyCallback(scheduling_class, lease_id,
        std::move(send_reply_callback_wrapper), reply));
    return;
  }

  // 所有前置检查通过 → 预启动 worker + 加入调度队列
  worker_pool_.PrestartWorkers(lease_spec, request.backlog_size());
  cluster_lease_manager_.QueueAndScheduleLease(
      std::move(lease),
      request.grant_or_reject(),              // 透传 Core Worker 的 grant_or_reject
      request.is_selected_based_on_locality(),
      {internal::ReplyCallback(std::move(send_reply_callback_wrapper), reply)});
}
```

所有 lease 请求（无论来自 Core Worker 还是 redirect）都走这个统一入口。`grant_or_reject` 和 `is_selected_based_on_locality` 直接从 gRPC 请求中透传。

#### 6.6.3 ClusterLeaseManager::QueueAndScheduleLease 核心逻辑

`QueueAndScheduleLease` 创建 `Work` 对象（封装 lease、`grant_or_reject`、`is_selected_based_on_locality`、reply callback），然后加入调度队列并立即尝试调度：

```cpp
// cluster_lease_manager.cc:47-78
void ClusterLeaseManager::QueueAndScheduleLease(
    RayLease lease, bool grant_or_reject, bool is_selected_based_on_locality,
    std::vector<internal::ReplyCallback> reply_callbacks) {
  const auto scheduling_class = lease.GetLeaseSpecification().GetSchedulingClass();

  // 创建 Work 对象 — grant_or_reject 存入 Work，后续所有决策都读这个值
  auto work = std::make_shared<internal::Work>(std::move(lease),
                                               grant_or_reject,           // ← 存入
                                               is_selected_based_on_locality,
                                               std::move(reply_callbacks));

  // 如果该 scheduling class 已标记为 infeasible，直接放 infeasible 队列
  auto infeasible_leases_iter = infeasible_leases_.find(scheduling_class);
  if (infeasible_leases_iter != infeasible_leases_.end()) {
    infeasible_leases_iter->second.emplace_back(std::move(work));
  } else {
    leases_to_schedule_[scheduling_class].emplace_back(std::move(work));
  }

  ScheduleAndGrantLeases();  // 立即尝试调度
}
```

**`ScheduleAndGrantLeases`** 的完整逻辑（`cluster_lease_manager.cc:196-295`）：

```cpp
void ClusterLeaseManager::ScheduleAndGrantLeases() {
  // 先尝试将 infeasible lease 重新调度（可能已变为 feasible）
  TryScheduleInfeasibleLease();

  for (auto shapes_it = leases_to_schedule_.begin();
       shapes_it != leases_to_schedule_.end();) {
    auto &work_queue = shapes_it->second;
    bool is_infeasible = false;

    for (auto work_it = work_queue.begin(); work_it != work_queue.end();) {
      const std::shared_ptr<internal::Work> &work = *work_it;

      // 用集群资源视图选最佳节点
      auto scheduling_node_id = cluster_resource_scheduler_.GetBestSchedulableNode(
          lease.GetLeaseSpecification(),
          /*preferred_node_id=*/work->PrioritizeLocalNode() ? self_node_id_.Binary()
                                                            : lease.GetPreferredNodeID(),
          /*exclude_local_node=*/false,   // ← 永远 false
          /*requires_object_store_memory=*/false,
          &is_infeasible);

      // 无节点可用
      if (scheduling_node_id.IsNil()) {
        // 检查是否是 NodeAffinity hard 约束导致的不可调度
        if (IsHardNodeAffinitySchedulingStrategy && target node doesn't exist) {
          ReplyCancelled(*work, SCHEDULING_CANCELLED_UNSCHEDULABLE, ...);
          work_it = work_queue.erase(work_it);
          continue;
        }
        break;  // 普通资源不足，留在队列等资源释放
      }

      // 选到了节点 → ScheduleOnNode 处理
      NodeID node_id = NodeID::FromBinary(scheduling_node_id.Binary());
      ScheduleOnNode(node_id, work);
      work_it = work_queue.erase(work_it);  // 从调度队列移除
    }

    // infeasible 的 scheduling class 移到 infeasible 队列
    if (is_infeasible) {
      infeasible_leases_[shapes_it->first] = std::move(shapes_it->second);
      leases_to_schedule_.erase(shapes_it++);
    } else if (work_queue.empty()) {
      leases_to_schedule_.erase(shapes_it++);
    } else {
      shapes_it++;
    }
  }

  // 最后调 LocalLeaseManager 的调度（处理本地 grant 队列中的 lease）
  local_lease_manager_.ScheduleAndGrantLeases();
}
```

**`PrioritizeLocalNode()`** 的定义（`internal.h:104-106`）：

```cpp
bool PrioritizeLocalNode() const {
    return grant_or_reject_ || is_selected_based_on_locality_;
}
```

- 首次请求 `grant_or_reject=false`：取决于 `is_selected_based_on_locality`（由 `GetBestNodeForLease` 返回的第二个值决定，基于 locality 选节点时为 true）
- Spillback 请求 `grant_or_reject=true`：`PrioritizeLocalNode=true` → `preferred_node_id=self_node_id_` → 强偏好本地

#### 6.6.4 GetBestSchedulableNode 的决策逻辑

这是调度器的核心函数，决定 lease 分配到哪个节点。完整代码（`cluster_resource_scheduler.cc:292-395`）：

```cpp
scheduling::NodeID ClusterResourceScheduler::GetBestSchedulableNode(
    const LeaseSpecification &lease_spec,
    const std::string &preferred_node_id,
    bool exclude_local_node,
    bool requires_object_store_memory,
    bool *is_infeasible) {
  int64_t _unused;  // violation 分数（未使用）

  // 收集所有 label selector（主 selector + fallback strategy 中的）
  std::vector<std::reference_wrapper<const LabelSelector>> label_selectors;
  label_selectors.push_back(std::cref(lease_spec.GetLabelSelector()));
  for (const auto &fallback : lease_spec.GetFallbackStrategy()) {
    label_selectors.push_back(std::cref(fallback.label_selector));
  }

  scheduling::NodeID highest_priority_unavailable_node = scheduling::NodeID::Nil();
  const LabelSelector *highest_priority_unavailable_label_selector = nullptr;
  bool any_selector_is_feasible = false;

  // 按优先级逐个尝试 label selector
  for (const auto &selector_ref : label_selectors) {
    const auto &label_selector = selector_ref.get();

    // 步骤 1: 快速路径 — preferred_node_id == 本地 + 不排除本地 + 本地可调度
    //   "If the local node is available, we should directly return it
    //    instead of going through the full hybrid policy since we don't want spillback."
    if (preferred_node_id == local_node_id_.Binary() && !exclude_local_node &&
        IsSchedulableOnNode(local_node_id_,
                            lease_spec.GetRequiredPlacementResources().GetResourceMap(),
                            label_selector,
                            requires_object_store_memory)) {
      *is_infeasible = false;
      return local_node_id_;    // ← 快速返回本地，不走完整调度策略
    }

    // 步骤 2: 走完整调度策略（Hybrid/Spread/NodeAffinity 等）
    //   遍历集群所有节点，按可用资源评分，找最佳节点
    //   exclude_local_node=false 时本地也参与评分
    bool current_selector_is_infeasible = false;
    scheduling::NodeID best_feasible_node = GetBestSchedulableNode(
        lease_spec.GetRequiredPlacementResources().GetResourceMap(),
        label_selector,
        lease_spec.GetMessage().scheduling_strategy(),
        requires_object_store_memory,
        lease_spec.IsActorCreationTask(),
        exclude_local_node,        // false — 本地始终是候选
        preferred_node_id,
        &_unused,
        &current_selector_is_infeasible);

    if (!best_feasible_node.IsNil()) {
      // 找到了 feasible 节点
      any_selector_is_feasible = true;

      if (IsSchedulableOnNode(best_feasible_node, ...)) {
        // feasible 且 available → 直接返回
        *is_infeasible = false;
        return best_feasible_node;    // 可能是本地，也可能是远端
      }

      // feasible 但 unavailable（资源暂时被占满）
      // 记录最高优先级的 unavailable 节点，继续检查 fallback
      if (highest_priority_unavailable_node.IsNil()) {
        highest_priority_unavailable_node = best_feasible_node;
        highest_priority_unavailable_label_selector = &label_selector;
      }
    }
  }

  // 步骤 3: 所有 selector 都没找到 available 节点
  if (!any_selector_is_feasible) {
    *is_infeasible = true;
    return scheduling::NodeID::Nil();    // 不可调度
  }

  // 步骤 4: 所有 best node 都 unavailable，但本地 feasible 且 preferred==本地
  //   返回本地，等资源释放
  *is_infeasible = false;
  if ((preferred_node_id == local_node_id_.Binary()) && NodeAvailable(local_node_id_)) {
    auto resource_request = ResourceMapToResourceRequest(
        lease_spec.GetRequiredPlacementResources().GetResourceMap(),
        requires_object_store_memory);
    resource_request.SetLabelSelector(*highest_priority_unavailable_label_selector);

    if (cluster_resource_manager_->HasFeasibleResources(local_node_id_, resource_request)) {
      return local_node_id_;    // 本地 feasible → 等本地资源释放
    }
  }

  // 步骤 5: 本地也不 feasible → 返回最高优先级 unavailable 节点（或 Nil）
  if (!is_local_node_with_raylet_) {
    return scheduling::NodeID::Nil();    // GCS 调度时返回 Nil
  }
  return highest_priority_unavailable_node;
}
```

**底层调度策略**（`cluster_resource_scheduler.cc:151-225`，由步骤 2 调用）：

```cpp
scheduling::NodeID ClusterResourceScheduler::GetBestSchedulableNode(
    const ResourceRequest &resource_request,
    const rpc::SchedulingStrategy &scheduling_strategy,
    bool actor_creation, bool force_spillback,
    const std::string &preferred_node_id,
    int64_t *total_violations, bool *is_infeasible) {

  // 特殊：0 CPU actor → Random 策略
  if (actor_creation && resource_request.IsEmpty() &&
      !IsHardNodeAffinitySchedulingStrategy(scheduling_strategy)) {
    return scheduling_policy_->Schedule(resource_request, SchedulingOptions::Random());
  }

  // 根据调度策略选择 SchedulingOptions:
  if (scheduling_strategy.scheduling_strategy_case() == kSpreadSchedulingStrategy) {
    return scheduling_policy_->Schedule(resource_request,
        SchedulingOptions::Spread(/*avoid_local_node=*/force_spillback, ...));
  } else if (scheduling_strategy.scheduling_strategy_case() == kNodeAffinitySchedulingStrategy) {
    return scheduling_policy_->Schedule(resource_request,
        SchedulingOptions::NodeAffinity(force_spillback, ..., node_id, soft, ...));
  } else if (has_node_label_scheduling_strategy) {
    return scheduling_policy_->Schedule(resource_request,
        SchedulingOptions::NodeLabelScheduling(scheduling_strategy));
  } else {
    // 默认：Hybrid 策略 — preferred_node_id 作为偏好，但不强制
    return scheduling_policy_->Schedule(resource_request,
        SchedulingOptions::Hybrid(/*avoid_local_node=*/force_spillback,
                                   /*require_node_available=*/force_spillback,
                                   preferred_node_id));
  }
}
```

`force_spillback` = `exclude_local_node`，在 `ClusterLeaseManager` 调用时永远是 `false`，所以：
- Hybrid 策略不强制避免本地，本地和远端平等参与评分
- 按可用资源量排序选最佳节点

**关键**：只看 CPU/GPU/自定义等显式资源和 label 约束，完全不知道 pinned 内存状态。

快速路径三个条件同时满足才直接返回本地：
1. `preferred_node_id == 本地` — 偏好是本地
2. `!exclude_local_node` — 不排除本地（ClusterLeaseManager 永远 `false`）
3. `IsSchedulableOnNode(本地)` — 本地确实有 CPU 资源可调度

跳过快速路径不意味着"本地不可调度"，只是不满足快速路径的偏好条件，需走完整策略重新评估所有节点。

#### 6.6.5 ScheduleOnNode — 本地 vs 远端的分叉

`ScheduleOnNode` 是集群调度的出口，决定 lease 走本地还是回复 Core Worker：

```cpp
// cluster_lease_manager.cc:422-460
void ClusterLeaseManager::ScheduleOnNode(const NodeID &spillback_to,
                                         const std::shared_ptr<internal::Work> &work) {
  // 选到本地 → 直接交给 LocalLeaseManager，不回复 Core Worker
  if (spillback_to == self_node_id_) {
    local_lease_manager_.QueueAndScheduleLease(work);
    return;
  }

  // 选到远端 + grant_or_reject=true → Reject
  if (work->grant_or_reject_) {
    for (const auto &reply_callback : work->reply_callbacks_) {
      reply_callback.reply_->set_rejected(true);
      reply_callback.send_reply_callback_(Status::OK(), nullptr, nullptr);
    }
    return;
  }

  // 选到远端 + grant_or_reject=false → Redirect
  internal_stats_.LeaseSpilled();

  const auto &lease = work->lease_;
  const auto &lease_spec = lease.GetLeaseSpecification();

  // 预分配远端节点资源（避免调度器视图不一致）
  if (!cluster_resource_scheduler_.AllocateRemoteTaskResources(
          scheduling::NodeID(spillback_to.Binary()),
          lease_spec.GetRequiredResources().GetResourceMap())) {
    RAY_LOG(DEBUG) << "Tried to allocate resources for request " << lease_spec.LeaseId()
                   << " on a remote node that are no longer available";
  }

  // 获取远端节点信息（地址、端口）
  auto node_info = get_node_info_(spillback_to);
  RAY_CHECK(node_info.has_value());

  // 回复 Core Worker — 设置 retry_at_raylet_address
  for (const auto &reply_callback : work->reply_callbacks_) {
    auto reply = reply_callback.reply_;
    reply->mutable_retry_at_raylet_address()->set_ip_address(
        (*node_info).node_manager_address());
    reply->mutable_retry_at_raylet_address()->set_port(
        (*node_info).node_manager_port());
    reply->mutable_retry_at_raylet_address()->set_node_id(spillback_to.Binary());
    reply_callback.send_reply_callback_(Status::OK(), nullptr, nullptr);
  }
}
```

| 选到节点 | `grant_or_reject` | 行为 | Core Worker 收到 |
|---|---|---|---|
| **本地** | 任意 | `local_lease_manager_.QueueAndScheduleLease(work)` — 不回复 Core Worker | 等待最终 grant/reject/cancel |
| **远端** | `false`（首次） | Redirect — 设置 `retry_at_raylet_address` + `AllocateRemoteTaskResources` | 带 `raylet_address` 重试（`is_spillback=true`） |
| **远端** | `true`（spillback） | Reject — 设置 `rejected=true` | 从头重新选节点（`raylet_address=nullptr`） |

### 6.7 LocalLeaseManager 中 pinned 内存超限不会 Spillback

#### 6.7.1 GrantScheduledLeasesToWorkers 完整调度循环

`LocalLeaseManager::ScheduleAndGrantLeases` 每次被调用时先执行 `GrantScheduledLeasesToWorkers`，再执行 `SpillWaitingLeases`：

```cpp
// local_lease_manager.cc:126-134
void LocalLeaseManager::ScheduleAndGrantLeases() {
  GrantScheduledLeasesToWorkers();
  SpillWaitingLeases();
}
```

`GrantScheduledLeasesToWorkers` 的完整逻辑（`local_lease_manager.cc:149-395`），核心调度循环有三个检查点：

```cpp
void LocalLeaseManager::GrantScheduledLeasesToWorkers() {
  for (auto shapes_it = leases_to_grant_.begin(); shapes_it != leases_to_grant_.end();) {
    auto &leases_to_grant_queue = shapes_it->second;
    bool is_infeasible = false;

    for (auto work_it = leases_to_grant_queue.begin();
         work_it != leases_to_grant_queue.end();) {
      auto &work = *work_it;
      const auto &spec = work->lease_.GetLeaseSpecification();
      LeaseID lease_id = spec.LeaseId();

      // 检查点 0: 已在等待 worker → 跳过
      if (work->GetState() == internal::WorkStatus::WAITING_FOR_WORKER) {
        work_it++;
        continue;
      }

      // 检查点 1: scheduling class 容量超限（worker 进程数过多）
      if (sched_cls_cap_enabled_ &&
          sched_cls_info.granted_leases.size() >= sched_cls_info.capacity &&
          work->GetState() == internal::WorkStatus::WAITING) {
        // 指数退避等待
        int64_t wait_time = sched_cls_cap_interval_ms_ * (1L << exp);

        // ← ✅ 调 TrySpillback 尝试溢出
        bool did_spill = TrySpillback(work, is_infeasible);
        if (did_spill) {
          work_it = leases_to_grant_queue.erase(work_it);
          continue;
        }
        break;  // 没溢出成功 → 等待
      }

      // 检查点 2: PinLeaseArgsIfMemoryAvailable ← 本案例死锁点
      bool args_missing = false;
      bool success = PinLeaseArgsIfMemoryAvailable(spec, &args_missing);
      if (!success) {
        if (args_missing) {
          // 参数被 evict → 放回 waiting 队列，不调 TrySpillback
          auto it = waiting_lease_queue_.insert(waiting_lease_queue_.begin(),
                                                std::move(*work_it));
          RAY_CHECK(waiting_leases_index_.emplace(lease_id, it).second);
          cluster_resource_scheduler_.GetLocalResourceManager().MaybeMarkFootprintAsBusy(
              WorkFootprint::PULLING_TASK_ARGUMENTS);
          work_it = leases_to_grant_queue.erase(work_it);
        } else {
          // ❌ pinned 内存超限 → 只是 SetStateWaiting，不调 TrySpillback
          RAY_LOG(DEBUG) << "Granting lease " << lease_id
                         << " would put this node over the max memory allowed for "
                            "arguments of granted leases ("
                         << max_pinned_lease_arguments_bytes_
                         << "). Waiting to grant lease until other leases are returned";
          RAY_CHECK(!granted_lease_args_.empty() && !pinned_lease_arguments_.empty());
          work->SetStateWaiting(
              internal::UnscheduledWorkCause::WAITING_FOR_AVAILABLE_PLASMA_MEMORY);
          work_it++;
        }
        continue;
      }

      // 检查点 3: CPU 资源分配
      auto allocated_instances = std::make_shared<TaskResourceInstances>();
      bool schedulable =
          !cluster_resource_scheduler_.GetLocalResourceManager().IsLocalNodeDraining() &&
          cluster_resource_scheduler_.GetLocalResourceManager()
              .AllocateLocalTaskResources(spec.GetRequiredResources().GetResourceMap(),
                                          allocated_instances);
      if (!schedulable) {
        ReleaseLeaseArgs(lease_id);   // 先释放 pinned args
        // ← ✅ 调 TrySpillback 尝试溢出
        bool did_spill = TrySpillback(work, is_infeasible);
        if (!did_spill) {
          work->SetStateWaiting(
              internal::UnscheduledWorkCause::WAITING_FOR_RESOURCES_AVAILABLE);
          break;
        }
        work_it = leases_to_grant_queue.erase(work_it);
      } else {
        // 所有检查通过 → PopWorker → grant lease
        work->allocated_instances_ = allocated_instances;
        work->SetStateWaitingForWorker();
        worker_pool_.PopWorker(spec, [...](worker, status, ...) -> bool {
          return PoppedWorkerHandler(worker, status, lease_id, ...);
        });
        work_it++;
      }
    }
    // ... 处理 scheduling class 移动/删除
  }
}
```

**三个 TrySpillback 调用点对比**：

| 检查点 | 条件 | 调 TrySpillback | 本案例走这里？ |
|---|---|---|---|
| 检查点 1 | scheduling class 容量超限 | ✅ 是 | 否 |
| 检查点 2 | Pin 内存失败 | ❌ **否** | **是** ← 死锁点 |
| 检查点 3 | CPU 资源不足 | ✅ 是 | 否（CPU 充足） |

#### 6.7.2 PinLeaseArgsIfMemoryAvailable 完整代码

```cpp
// local_lease_manager.cc:783-840
bool LocalLeaseManager::PinLeaseArgsIfMemoryAvailable(
    const LeaseSpecification &lease_spec, bool *args_missing) {
  std::vector<std::unique_ptr<RayObject>> args;
  const auto &deps = lease_spec.GetDependencyIds();

  // 步骤 1: 从 plasma 获取参数对象引用
  if (!deps.empty()) {
    if (!get_lease_arguments_(deps, &args)) {
      *args_missing = true;    // 获取失败（如对象被 evict）
      return false;
    }
    // 检查是否有 null 参数（被 evict）
    for (size_t i = 0; i < deps.size(); i++) {
      if (args[i] == nullptr) {
        *args_missing = true;
        return false;
      }
    }
  }

  // 步骤 2: 计算参数大小
  *args_missing = false;
  size_t lease_arg_bytes = 0;
  for (auto &arg : args) {
    lease_arg_bytes += arg->GetSize();
  }

  // 步骤 3: 先无条件 PinLeaseArgs（增加引用计数和 pinned 字节数）
  PinLeaseArgs(lease_spec, std::move(args));

  // 步骤 4: 检查是否超限
  if (max_pinned_lease_arguments_bytes_ == 0) {
    return true;    // 未设阈值 → 无限制
  }

  if (lease_arg_bytes > max_pinned_lease_arguments_bytes_) {
    // 单个 lease 的 args 就超限 → 仅 WARNING，仍然返回 true
    RAY_LOG(WARNING) << "Granted lease " << lease_spec.LeaseId()
                     << " has arguments of size " << lease_arg_bytes
                     << ", but the max memory allowed is only "
                     << max_pinned_lease_arguments_bytes_;
  } else if (pinned_lease_arguments_bytes_ > max_pinned_lease_arguments_bytes_) {
    // ← 本案例走这里！加上新 lease 后总 pinned 字节数超限
    ReleaseLeaseArgs(lease_spec.LeaseId());   // 撤销刚才的 Pin
    RAY_LOG(DEBUG) << "Cannot grant lease " << lease_spec.LeaseId()
                   << " with arguments of size " << lease_arg_bytes
                   << " current pinned bytes is " << pinned_lease_arguments_bytes_;
    return false;
  }

  return true;
}
```

**注意**：先 `PinLeaseArgs`（增加计数），再检查是否超限，超限则 `ReleaseLeaseArgs` 撤销。所以返回 `false` 时 pinned 计数已恢复原状。

#### 6.7.3 PinLeaseArgs 和 ReleaseLeaseArgs 的引用计数机制

```cpp
// local_lease_manager.cc:842-870
void LocalLeaseManager::PinLeaseArgs(const LeaseSpecification &lease_spec,
                                     std::vector<std::unique_ptr<RayObject>> args) {
  const auto &deps = lease_spec.GetDependencyIds();
  auto executed_lease_inserted =
      granted_lease_args_.emplace(lease_spec.LeaseId(), deps).second;

  if (executed_lease_inserted) {
    for (size_t i = 0; i < deps.size(); i++) {
      auto [it, pinned_lease_inserted] =
          pinned_lease_arguments_.emplace(deps[i], std::make_pair(std::move(args[i]), 0));
      if (pinned_lease_inserted) {
        // 第一个需要此参数的 lease → 增加字节数
        pinned_lease_arguments_bytes_ += it->second.first->GetSize();
      }
      it->second.second++;   // 引用计数 +1
    }
  }
}

// local_lease_manager.cc:875-891
void LocalLeaseManager::ReleaseLeaseArgs(const LeaseID &lease_id) {
  auto it = granted_lease_args_.find(lease_id);
  if (it != granted_lease_args_.end()) {
    for (auto &arg : it->second) {
      auto arg_it = pinned_lease_arguments_.find(arg);
      RAY_CHECK(arg_it != pinned_lease_arguments_.end());
      RAY_CHECK(arg_it->second.second > 0);
      arg_it->second.second--;    // 引用计数 -1
      if (arg_it->second.second == 0) {
        // 最后一个需要此参数的 lease 释放了 → 减少字节数
        pinned_lease_arguments_bytes_ -= arg_it->second.first->GetSize();
        pinned_lease_arguments_.erase(arg_it);
      }
    }
    granted_lease_args_.erase(it);
  }
}
```

泄漏的本质：`KillAsync` 跳过 `CleanupLease` → `ReleaseLeaseArgs` 不被调用 → `granted_lease_args_` 中记录永远存在 → 引用计数永远 > 0 → `pinned_lease_arguments_bytes_` 永远不下降。

### 6.8 什么时候会走 Spillback（Redirect）vs Reject

#### 6.8.1 TrySpillback 完整代码

`TrySpillback` 被 LocalLeaseManager 在两种 CPU 相关场景下调，尝试将 lease 溢出到远程节点：

```cpp
// local_lease_manager.cc:520-541
bool LocalLeaseManager::TrySpillback(const std::shared_ptr<internal::Work> &work,
                                     bool &is_infeasible) {
  const auto &spec = work->lease_.GetLeaseSpecification();

  // 用集群调度器选最佳远程节点
  // preferred_node_id=本地（偏好留在本地）
  // exclude_local_node=false（本地也参与，但如果本地没资源自然不会选到）
  auto scheduling_node_id = cluster_resource_scheduler_.GetBestSchedulableNode(
      spec,
      /*preferred_node_id=*/self_node_id_.Binary(),
      /*exclude_local_node=*/false,
      /*requires_object_store_memory=*/false,
      &is_infeasible);

  // 无可用远端节点、不可调度、或选到的还是本地 → 无法溢出
  if (is_infeasible || scheduling_node_id.IsNil() ||
      scheduling_node_id == self_scheduling_node_id_) {
    return false;
  }

  // 选到远端节点 → 调 Spillback
  NodeID node_id = NodeID::FromBinary(scheduling_node_id.Binary());
  Spillback(node_id, work);
  num_unschedulable_lease_spilled_++;

  // 清理 lease 的依赖跟踪
  if (!spec.GetDependencies().empty()) {
    lease_dependency_manager_.RemoveLeaseDependencies(spec.LeaseId());
  }
  return true;
}
```

#### 6.8.2 LocalLeaseManager::Spillback 完整代码

`Spillback` 是 redirect/reject 的执行函数，根据 `grant_or_reject` 决定走哪条路径：

```cpp
// local_lease_manager.cc:696-724
void LocalLeaseManager::Spillback(const NodeID &spillback_to,
                                  const std::shared_ptr<internal::Work> &work) {
  // grant_or_reject=true → Reject（不能再 redirect）
  if (work->grant_or_reject_) {
    for (const auto &reply_callback : work->reply_callbacks_) {
      reply_callback.reply_->set_rejected(true);
      reply_callback.send_reply_callback_(Status::OK(), nullptr, nullptr);
    }
    return;
  }

  // grant_or_reject=false → Redirect
  num_lease_spilled_++;
  const auto &lease_spec = work->lease_.GetLeaseSpecification();

  // 预分配远端节点资源
  if (!cluster_resource_scheduler_.AllocateRemoteTaskResources(
          scheduling::NodeID(spillback_to.Binary()),
          lease_spec.GetRequiredResources().GetResourceMap())) {
    RAY_LOG(DEBUG) << "Tried to allocate resources for request " << lease_spec.LeaseId()
                   << " on a remote node that are no longer available";
  }

  // 获取远端节点信息
  auto node_info_ptr = get_node_info_(spillback_to);
  RAY_CHECK(node_info_ptr)
      << "Spilling back to a node manager, but no GCS info found for node "
      << spillback_to;

  // 回复 Core Worker — 设置 retry_at_raylet_address
  for (const auto &reply_callback : work->reply_callbacks_) {
    auto reply = reply_callback.reply_;
    reply->mutable_retry_at_raylet_address()->set_ip_address(
        node_info_ptr->node_manager_address());
    reply->mutable_retry_at_raylet_address()->set_port(
        node_info_ptr->node_manager_port());
    reply->mutable_retry_at_raylet_address()->set_node_id(spillback_to.Binary());
    reply_callback.send_reply_callback_(Status::OK(), nullptr, nullptr);
  }
}
```

**注意**：`ClusterLeaseManager::ScheduleOnNode` 和 `LocalLeaseManager::Spillback` 都能产生 redirect/reject，逻辑相同，只是调用场景不同：
- `ScheduleOnNode`：集群级调度选到远端时调用
- `Spillback`：本地调度发现 CPU 不足或容量超限时通过 `TrySpillback` 调用

#### Spillback（Redirect）= `grant_or_reject=false` + 远端有资源

| 场景 | 触发位置 | 说明 |
|---|---|---|
| 集群调度选到远端 | `ClusterLeaseManager::ScheduleOnNode` | `GetBestSchedulableNode` 返回远端节点 |
| 本地 CPU 不足 | `LocalLeaseManager::GrantScheduledLeasesToWorkers:378` | `AllocateLocalTaskResources` 失败后调 `TrySpillback` |
| Worker 容量超限 | `LocalLeaseManager::GrantScheduledLeasesToWorkers:305` | scheduling class cap 超额后调 `TrySpillback` |
| Waiting 队列溢出 | `LocalLeaseManager::SpillWaitingLeases:440` | 定期检查 waiting 队列能否溢出到远端 |

Redirect 后 Core Worker 带 `raylet_address` 重试，`is_spillback=true, grant_or_reject=true`。

#### Reject = `grant_or_reject=true` + 无法在本地 grant

| 场景 | 触发位置 | 说明 |
|---|---|---|
| Spillback 请求选到远端 | `ClusterLeaseManager::ScheduleOnNode:429` | `grant_or_reject=true` 时不能 redirect，只能 reject |
| Spillback 请求本地 CPU 不足 | `LocalLeaseManager::TrySpillback → Spillback:698` | `grant_or_receed=true` → reject |

Reject 后 Core Worker 从头重新选节点（`raylet_address=nullptr`）。

#### 两跳设计

```
第 1 跳（首次请求）:
  Core Worker → 选节点 A (grant_or_reject=false)
    → A CPU 不足 → GetBestSchedulableNode 选到 B → Redirect → Core Worker 再找 B (grant_or_reject=true)
    → A CPU 不足 → 无别的节点 → WAITING (留在 A 的队列)

第 2 跳（spillback 请求）:
  Core Worker → 找 B (grant_or_reject=true)
    → B CPU 不足 → Reject → Core Worker 从头重新选
    → B CPU 不足 → 无别的节点 → WAITING (留在 B 的队列)

最多两跳，避免无限转发
```

### 6.9 本案例 77000000 的完整调度路径

```
Core Worker (head driver) 首次提交
  → SubmitTask → ResolveDependencies → RequestNewWorkerIfNeeded  (normal_task_submitter.cc:40-54)
  → is_spillback = (raylet_address==nullptr) = false              (normal_task_submitter.cc:323)
  → GetBestNodeForLease 选到 8ae1cc62（基于数据本地性）            (lease_policy.cc:60-87)
    → is_selected_based_on_locality = true
  → RequestWorkerLease(grant_or_reject=false, ...)               (raylet_client.cc:53-77)
    → method_timeout_ms=-1 → gRPC 永不超时
  → gRPC 请求发往 8ae1cc62 raylet

8ae1cc62 raylet 收到请求
  → HandleRequestWorkerLease                                      (node_manager.cc:1781)
    → 前置检查 1: lease_id 未在 leased_workers_ 中（不是 retry）
    → 前置检查 2: caller worker/node 未在 failed_cache 中
    → 前置检查 3: lease 未在 cluster/local lease manager 队列中
    → PrestartWorkers
    → ClusterLeaseManager::QueueAndScheduleLease                   (cluster_lease_manager.cc:47)
      → 创建 Work(lease, grant_or_reject=false, is_selected_based_on_locality=true)
      → 加入 leases_to_schedule_ 队列
      → ScheduleAndGrantLeases                                     (cluster_lease_manager.cc:196)

  ClusterLeaseManager::ScheduleAndGrantLeases
    → GetBestSchedulableNode                                       (cluster_resource_scheduler.cc:292)
      → PrioritizeLocalNode() = grant_or_reject(false) || is_selected_based_on_locality(true) = true
      → preferred_node_id = self_node_id_ (8ae1cc62 本地)
      → 快速路径: preferred_node_id == local_node_id_ ✅
                && !exclude_local_node ✅ (永远 false)
                && IsSchedulableOnNode(local_node_id_) ✅ (CPU 70 核空闲)
      → return local_node_id_  ← 命中快速路径
    → ScheduleOnNode(self_node_id_, work)                          (cluster_lease_manager.cc:422)
      → spillback_to == self_node_id_ → local_lease_manager_.QueueAndScheduleLease(work)
      → 不回复 Core Worker

  LocalLeaseManager::QueueAndScheduleLease → ScheduleAndGrantLeases (local_lease_manager.cc:96, 126)
    → GrantScheduledLeasesToWorkers                                (local_lease_manager.cc:149)

  GrantScheduledLeasesToWorkers
    → 检查点 0: work state != WAITING_FOR_WORKER → 继续
    → 检查点 1: scheduling class 容量未超限 → 继续
    → 检查点 2: PinLeaseArgsIfMemoryAvailable                      (local_lease_manager.cc:783)
      → get_lease_arguments_: 所有参数在 plasma 中 ✅
      → args_missing = false
      → lease_arg_bytes 计算
      → PinLeaseArgs(lease_spec, args)                             (local_lease_manager.cc:842)
        → granted_lease_args_.emplace(lease_id, deps)
        → pinned_lease_arguments_bytes_ += arg size
        → pinned_lease_arguments_bytes_ = 22.05 GB + 新 args > 22.4 GB
      → 检查超限: pinned_lease_arguments_bytes_ > max_pinned_lease_arguments_bytes_
        → ReleaseLeaseArgs(lease_id)  ← 撤销刚才的 Pin            (local_lease_manager.cc:875)
        → return false
    → ❌ 不调 TrySpillback（代码中没有此调用）
    → SetStateWaiting(WAITING_FOR_AVAILABLE_PLASMA_MEMORY)          (local_lease_manager.cc:349)
    → work_it++ → 继续处理 grant queue 中的下一个 lease

    → 检查点 3: 不会到达（已在检查点 2 continue）

  → SpillWaitingLeases                                            (local_lease_manager.cc:440)
    → 本 lease 在 leases_to_grant_ 中，不在 waiting_lease_queue_ 中
    → SpillWaitingLeases 只处理 waiting 队列 → 不影响本 lease

  → lease 卡在 leases_to_grant_ 队列中，状态 WAITING_FOR_AVAILABLE_PLASMA_MEMORY
  → 等其他 lease 释放 args → 但泄漏的永远不会释放 → 永久死锁

Core Worker 端: RetryableGrpcClient (method_timeout_ms=-1) 永不超时 → callback 永远不触发
→ 9 个 task 永久卡在 PENDING_NODE_ASSIGNMENT
```

**核心**：集群调度只看 CPU 资源选节点（`GetBestSchedulableNode` 快速路径命中 `IsSchedulableOnNode`），完全不知道本地 pinned 内存已打满。8ae1cc62 CPU 充足 → 选本地 → LocalLeaseManager `PinLeaseArgsIfMemoryAvailable` 失败 → 代码中**没有**调 `TrySpillback` → 不 Redirect → 不 Reject → 卡死。pinned 内存阈值是一种隐式资源约束，但调度器只感知 CPU/GPU 等显式资源，pin 失败后的分支也没有把 lease 转移到别的节点，形成了"CPU 够用但内存卡死"的死锁。

## 七、等待其他 lease 释放 args 的后续完整流程

当 lease 因 `pinned_lease_arguments_bytes_ > max_pinned_lease_arguments_bytes_` 进入 `WAITING_FOR_AVAILABLE_PLASMA_MEMORY` 状态后（`local_lease_manager.cc:335-346`），后续流程依赖其他 lease 释放 args 触发重新调度。

### 7.1 重新调度的触发点

`ScheduleAndGrantLeases()` 在以下场景被调用：

| 触发场景 | 代码位置 | 说明 |
|---------|---------|------|
| 新 lease 入队 | `LocalLeaseManager::QueueAndScheduleLease` (`local_lease_manager.cc:96`) | 新 lease 到达时触发 |
| 依赖对象就绪 | `LocalLeaseManager::LeasesUnblocked` (`local_lease_manager.cc:733`) | lease 的参数对象被拉到本地后触发 |
| Worker 归还 | `NodeManager::HandleWorkerAvailable` (`node_manager.cc:1371`) | worker 归还后触发 `cluster_lease_manager_.ScheduleAndGrantLeases()` |
| 资源变更 | `NodeManager::HandleRescaleLocalResources` (`node_manager.cc:2070`) | 节点资源扩缩容时触发 |

### 7.2 正常流程（无 Bug 时）

```
其他 task 完成 → Driver ReturnWorkerLease
  → HandleReturnWorkerLease (node_manager.cc:2080)
    → ReleaseWorker(lease_id)                    // 从 leased_workers_ 移除
    → local_lease_manager_.ReleaseWorkerResources(worker)  // 释放 CPU
    → HandleWorkerAvailable(worker)              // node_manager.cc:2114
      → CleanupLease(worker)                     // node_manager.cc:2362
        → local_lease_manager_.CleanupLease(worker, &lease)
          → RemoveFromGrantedLeasesIfExists()   // ✅ 从 granted 集合移除
          → ReleaseLeaseArgs()                   // ✅ 释放 pinned args
      → cluster_lease_manager_.ScheduleAndGrantLeases()  // ✅ 触发重新调度
        → local_lease_manager_.GrantScheduledLeasesToWorkers()
          → PinLeaseArgsIfMemoryAvailable()
            → pinned_lease_arguments_bytes_ 已下降 → ✅ 可以成功 pin
          → Grant 成功 → 回复 driver
```

### 7.3 SpillWaitingLeases 补救机制

每次 `ScheduleAndGrantLeases()` 执行时都会调用 `SpillWaitingLeases()`（`local_lease_manager.cc:133`），它会遍历 waiting 队列尝试将 lease 溢出到远程节点：

```cpp
// local_lease_manager.cc:440-512
void LocalLeaseManager::SpillWaitingLeases() {
  auto it = waiting_lease_queue_.end();
  while (it != waiting_lease_queue_.begin()) {
    it--;
    const auto &lease = (*it)->lease_;
    bool lease_dependencies_blocked =
        lease_dependency_manager_.LeaseDependenciesBlocked(lease_id);

    scheduling::NodeID scheduling_node_id;
    if (!lease_spec.IsSpreadSchedulingStrategy()) {
      scheduling_node_id = cluster_resource_scheduler_.GetBestSchedulableNode(
          lease_spec,
          /*preferred_node_id*/ self_node_id_.Binary(),
          /*exclude_local_node*/ lease_dependencies_blocked,  // ← 关键！
          /*requires_object_store_memory*/ true,
          &is_infeasible);
    }

    if (!scheduling_node_id.IsNil() && scheduling_node_id != self_scheduling_node_id_) {
      // 找到远程节点 → Spillback
      Spillback(node_id, *it);
      // 从 waiting 队列移除
    } else {
      // 无可用远程节点 → 保留在本地等待
      break;
    }
  }
}
```

### 7.4 SpillWaitingLeases 的局限

1. **`exclude_local_node` 由 `LeaseDependenciesBlocked` 决定**：如果 WAITING_FOR_AVAILABLE_PLASMA_MEMORY 的 lease 的依赖对象都在本地（未被驱逐、未被拉取），`LeaseDependenciesBlocked` 返回 false，`exclude_local_node=false`，调度器可能仍然选本地节点 → 无法溢出

2. **`requires_object_store_memory=true`**：虽然传了此标志，但 `GetBestSchedulableNode` 的调度策略主要看 CPU 资源，**不感知远端节点的 pinned args 内存占用**（代码注释 `TODO(swang): The policy currently does not account for the amount of object store memory availability`）

3. **从队尾开始遍历**：优先溢出队列末尾（最近加入的）lease，队头（最先等待的）可能被保留

4. **`grant_or_reject=false` 时走 redirect**：如果成功溢出，`Spillback()` 函数中 `grant_or_reject=false` 不会 reject，而是设置 `retry_at_raylet_address` 让 driver 去 redirect

### 7.5 本案例中 SpillWaitingLeases 无法生效的原因

`WAITING_FOR_AVAILABLE_PLASMA_MEMORY` 的 lease：
- 依赖对象**已经在本地**（pin 成功了只是内存不够被 release 了）→ `LeaseDependenciesBlocked` 返回 false
- `exclude_local_node=false` → 调度器不排除本地 → `GetBestSchedulableNode` 可能仍选本地
- 即使选到远程节点，远程节点也不一定有这些依赖对象的本地副本（需要重新拉取数据）

### 7.6 如果 SpillWaitingLeases 成功溢出到远程节点

当 `grant_or_reject=false`（本案例）时，走 redirect 路径：

```cpp
// local_lease_manager.cc:674-708 — Spillback 函数
if (work->grant_or_reject_) {
    reply->set_rejected(true);  // grant_or_reject=true 才 reject
    return;
}
// grant_or_reject=false → redirect
// 设置 retry_at_raylet_address → driver 收到后向新 raylet 发请求
```

Driver 收到 redirect 后（`normal_task_submitter.cc:425-435`）：
```cpp
} else {
    RAY_CHECK(!is_spillback);
    RequestNewWorkerIfNeeded(scheduling_key, &reply.retry_at_raylet_address());
}
```

此时 `raylet_address != nullptr` → `is_spillback=true` → 第二次请求 `grant_or_reject=true`，远端 raylet 必须 grant 或 reject。

### 7.7 Driver 端 callback 处理逻辑（如果正常收到回复）

```cpp
// normal_task_submitter.cc:337-470
if (status.ok()) {
    if (reply.canceled()) {
        // 处理取消（RuntimeEnvCreationFailed 等）
    } else if (reply.rejected()) {
        RAY_CHECK(is_spillback);
        RequestNewWorkerIfNeeded(scheduling_key);  // reject → 重新本地选节点
    } else if (!reply.worker_address().node_id().empty()) {
        // Lease granted，分配 worker
    } else {
        // Redirect 到其他 raylet
        RAY_CHECK(!is_spillback);
        RequestNewWorkerIfNeeded(scheduling_key, &reply.retry_at_raylet_address());
    }
} else if (NodeID::FromBinary(raylet_address.node_id()) != local_node_id_) {
    // 远端 raylet 失败 → 回退本地调度
    RequestNewWorkerIfNeeded(scheduling_key);  // 重试！
} else {
    // 本地 raylet 失败 → 进程退出
    QuickExit();
}
```

**如果 callback 能触发（如加了 timeout），会走到 `!status.ok() && remote` 分支，回退本地调度。**

---

## 八、8ae1cc62 节点 disconnect 数据分析

### 8.1 Worker 启动与断开

- 8ae1cc62 上共 2430 个 worker 启动，2422 个 disconnect
- 当前 8 个 IDLE worker（4 个普通 + 2 SpillWorker + 2 RestoreWorker）
- 77000000 的 worker disconnect 数 = 0

### 8.2 disconnect_type 分布

| disconnect_type | 值 | 数量 |
|---|---|---|
| INTENDED_USER_EXIT | 3 | 1718 |
| INTENDED_SYSTEM_EXIT | 1 | 687 |
| SYSTEM_ERROR | 0 | 12 |
| NODE_OUT_OF_MEMORY | 4 | 3 |

### 8.3 Worker exit 事件

74 个 worker exit 事件，全部 `force_exit: 1`（idle worker 被 job finish kill），没有 `force_exit: 0` 的 idle timeout exit。

### 8.4 当前状态（raylet 已重启）

- raylet 已被重启，node ID 变了（`3577354374366075196`，之前是 `8ae1cc62`）
- granted_lease_args = 0，pinned_lease_arguments_bytes = 0
- CPU 70/70 核完全空闲
- 之前的 48 个泄漏和 22GB 全部清零

---

## 九、完整因果链

```
历史作业中 owner worker/节点异常死亡
  → HandleUnexpectedWorkerFailure / HandleNodeRemoved
  → 直接 KillAsync 租赁 worker（不经过 DestroyWorker）
  → IsDead()=true → DisconnectClient 跳过 CleanupLease
  → ReleaseLeaseArgs 不被调用 → pinned args 永久泄漏
  → granted_lease_args_ 和 pinned_lease_arguments_bytes_ 基线持续增长

基线持续上升: 0→17→22→34→38→42→47→48（7月31日达到48）

77000000 启动（8月24日）
  → 8ae1cc62 上已有 48 个泄漏的 granted args，pinned = 22.05 GB
  → max_pinned = 22.4 GB (32 GB × 0.7)
  → 77000000 的 9 个 StreamingRepartition _map_task 发到各节点

f326 lease 到达 8ae1cc62
  → PinLeaseArgsIfMemoryAvailable: 22.05 GB + 新 args > 22.4 GB
  → return false → WAITING_FOR_AVAILABLE_PLASMA_MEMORY
  → 不回复 driver，不 spillback，不 reject
  → Driver 端 RetryableGrpcClient (method_timeout_ms=-1) 永不超时
  → callback 永远不触发
  → 9 个 task 永久卡在 PENDING_NODE_ASSIGNMENT
```

---

## 十、修复方案

### 10.1 Driver 端修复（commit `82d05363`，已存在但未合入）

- 给 `RequestWorkerLease` 在 spillback 场景加 `method_timeout_ms=600000`
- 增加 `grpc_max_ready_idle_resend_count=10`
- **需要合入运行集群版本**（当前仅在功能分支 `T11613716-rpc-timeout`）

### 10.2 Raylet 端核心修复：HandleUnexpectedWorkerFailure / HandleNodeRemoved 泄漏（未实现）

直接在 `KillAsync` 前加 `CleanupLease` 不够——`DisconnectClient` 除了 CleanupLease 还做了 push error 到 driver、ReleaseWorker、ScheduleAndGrantLeases 等。但直接用 `DestroyWorker` 替代 `KillAsync` 也不行——`DestroyWorker` 内部调 `DisconnectClient` → `ReleaseWorker` 会修改正在遍历的 `leased_workers_` map，导致迭代器失效。

正确修复：先收集需要 kill 的 worker 列表，遍历完后再逐个 `DestroyWorker`：

```cpp
// 修改位置: node_manager.cc:944-956 (HandleNodeRemoved) 和 :988-1001 (HandleUnexpectedWorkerFailure)

// 原代码：
// for (const auto &[_, worker] : leased_workers_) {
//     if (...) continue;
//     worker->KillAsync(io_service_);  // ❌ 直接 KillAsync 导致泄漏
// }

// 修改为：
std::vector<std::shared_ptr<WorkerInterface>> workers_to_kill;
for (const auto &[_, worker] : leased_workers_) {
    if (...) continue;
    workers_to_kill.push_back(worker);
}
for (const auto &worker : workers_to_kill) {
    DestroyWorker(worker,
                  rpc::WorkerExitType::INTENDED_SYSTEM_EXIT,
                  "Leased worker killed because its owner died");
}
```

这样 `DestroyWorker` 会完整执行 `DisconnectClient`（CleanupLease + ReleaseLeaseArgs + push error + ScheduleAndGrantLeases）再 `KillAsync`，不会遗漏任何清理步骤，也不会破坏迭代器。

### 10.3 Raylet 端修复：HandleReturnWorkerLease 中 worker_exiting=true 路径（未实现）

```cpp
// 修改位置: node_manager.cc:2101 附近
if (!request.worker_exiting()) {
    HandleWorkerAvailable(worker);
} else {
    RayLease lease;
    local_lease_manager_.CleanupLease(worker, &lease);
}
```

### 10.4 Pin 内存超限时增加 TrySpillback（未实现）

```cpp
// 修改位置: local_lease_manager.cc:340 附近
if (!success && !args_missing) {
    bool did_spill = TrySpillback(work, is_infeasible);
    if (did_spill) {
        work_it = leases_to_grant_queue.erase(work_it);
        continue;
    }
    work->SetStateWaiting(
        internal::UnscheduledWorkCause::WAITING_FOR_AVAILABLE_PLASMA_MEMORY);
    work_it++;
}
```

### 10.5 修复优先级

| 优先级 | 修复 | 覆盖场景 | 状态 |
|---|---|---|---|
| **P0** | 合入 `82d05363`（driver 端超时） | driver 永久卡死 | 代码已有，需合入 |
| **P0** | HandleUnexpectedWorkerFailure/HandleNodeRemoved 中 KillAsync 前调 CleanupLease | **raylet 端泄漏根因** | 未实现 |
| **P1** | HandleReturnWorkerLease 中 worker_exiting=true 释放 args | worker_exiting 路径补漏 | 未实现 |
| **P1** | Pin 内存超限时 TrySpillback | 避免单节点死锁 | 未实现 |

两个 P0 缺一不可：只修 driver 超时 → lease 卡 10 分钟后超时不会永久卡死；只修 raylet 泄漏 → pinned args 不再增长但已有旧场景仍需超时保护；两者都修 → 彻底解决。

---

## 十一、环境备注

### 11.1 时区差异

- Head 节点：CST (UTC+8)
- Worker 节点：UTC
- Driver 日志：CST

### 11.2 环境变量 RAY_BACKEND_LOG_LEVEL

- `RAY_BACKEND_LOG_LEVEL=debug` 只对新进程生效
- 已运行的 raylet 无法动态修改日志级别，需重启
- Driver 的 DEBUG 日志通常足够（关键日志在 `NormalTaskSubmitter` 和 `RetryableGrpcClient`）

### 11.3 raylet.out 文件大小

- 文件通常千万行+，直接 grep/cat 会卡死 I/O
- 使用 `tail -c` 或 Python 读尾部方式查看

### 11.4 WezTerm 远端连接

- pane 3 = head 节点 (10.137.44.181)
- pane 5 = worker 节点 8ae1cc62/public-bjx-c26-kce-node272 (10.53.82.106)
- pane 7 之前是 edc216c2 (10.53.149.41)，已断开

### 11.5 不要对未注册 handler 的进程发信号

排查过程中曾用 `os.kill(pid, signal.SIGUSR1)` 误杀作业 48000000（exit code 138），以后不要对未注册 signal handler 的进程发信号。

---

## 十二、相关源码索引

| 路径 | 行号 | 作用 |
|---|---|---|
| `src/ray/raylet/node_manager.cc` | 2080-2114 | `HandleReturnWorkerLease` — worker_exiting=true 时跳过 HandleWorkerAvailable |
| `src/ray/raylet/node_manager.cc` | 2098 | `ReleaseWorkerResources(worker)` 在 HandleWorkerAvailable 之前调用 |
| `src/ray/raylet/node_manager.cc` | 2101 | `if (!request.worker_exiting())` — 跳过 HandleWorkerAvailable 的条件 |
| `src/ray/raylet/node_manager.cc` | 1364-1385 | `HandleWorkerAvailable` — 包含 CleanupLease 调用 |
| `src/ray/raylet/node_manager.cc` | 2357-2380 | `NodeManager::CleanupLease` — 调 local_lease_manager_.CleanupLease |
| `src/ray/raylet/node_manager.cc` | 1425-1571 | `DisconnectClient` — 兜底路径，!IsDead() 时调 CleanupLease |
| `src/ray/raylet/node_manager.cc` | 1465 | `DisconnectClient` 中 CleanupLease 条件 `(!lease_id.IsNil() || !actor_id.IsNil()) && !worker->IsDead()` |
| `src/ray/raylet/node_manager.cc` | 979-1001 | `HandleUnexpectedWorkerFailure` — owner worker 死亡时直接 KillAsync 租赁 worker（泄漏根因） |
| `src/ray/raylet/node_manager.cc` | 954 | `HandleNodeRemoved` 中 KillAsync — owner 节点死亡时直接 KillAsync 租赁 worker（泄漏根因） |
| `src/ray/raylet/node_manager.cc` | 984 | `CancelAllLeasesOwnedBy` — 只 cancel 队列中的 lease，不管已 granted 的 |
| `src/ray/raylet/node_manager.cc` | 1448 | `DisconnectClient` 中 ReleaseWorker — leased_workers_ 可能已被 HandleReturnWorkerLease 删除 |
| `src/ray/raylet/scheduling/local_lease_manager.cc` | 766-776 | `CleanupLease` — 包含 ReleaseLeaseArgs 调用 |
| `src/ray/raylet/scheduling/local_lease_manager.cc` | 786-810 | `PinLeaseArgsIfMemoryAvailable` — 先无条件 PinLeaseArgs，再检查超限 |
| `src/ray/raylet/scheduling/local_lease_manager.cc` | 299-349 | `GrantScheduledLeasesToWorkers` — PinLeaseArgs 失败后设为 WAITING_FOR_AVAILABLE_PLASMA_MEMORY |
| `src/ray/raylet/scheduling/local_lease_manager.cc` | 818-840 | `PinLeaseArgs` — 引用计数 + pinned_lease_arguments_bytes_ 累加 |
| `src/ray/raylet/scheduling/local_lease_manager.cc` | 842-860 | `ReleaseLeaseArgs` — 引用计数 -1，归零时释放内存计数 |
| `src/ray/raylet/scheduling/local_lease_manager.cc` | 1048 | `ReleaseWorkerResources` — 只释放 CPU 资源，不清 lease_id |
| `src/ray/raylet/scheduling/local_lease_manager.cc` | 1204-1207 | `DumpState` — 输出 granted_lease_args、pinned_lease_arguments_bytes |
| `src/ray/core_worker/task_submission/normal_task_submitter.cc` | 114-165 | `ReturnWorkerLease` / `OnWorkerIdle` — disconnect_worker=was_error |
| `src/ray/core_worker/task_receiver.cc` | 128 | `worker_exiting=true` 的触发条件（IsIntentionalSystemExit 等） |
| `src/ray/raylet_rpc_client/raylet_client.cc` | 106-127 | `ReturnWorkerLease` — method_timeout_ms=-1 |
| `src/ray/common/ray_config_def.h` | 726 | `max_task_args_memory_fraction = 0.7` |
| `src/ray/raylet/main.cc` | 933 | `max_task_args_memory = capacity × fraction` |
| `src/ray/core_worker/lease_policy.cc` | 24-87 | `LocalityAwareLeasePolicy::GetBestNodeForLease` — 优先级链和 `GetBestNodeIdForLease` 本地性选择逻辑 |
| `src/ray/core_worker/lease_policy.cc` | 60-87 | `GetBestNodeIdForLease` — 遍历依赖对象，按本地字节数选节点 |
| `src/ray/raylet/scheduling/cluster_lease_manager.cc` | 422-435 | `ScheduleOnNode` — grant_or_reject 决定 reject/redirect |
| `src/ray/raylet/scheduling/local_lease_manager.cc` | 674-708 | `Spillback` — grant_or_reject=true reject, false redirect |
| `src/ray/raylet/scheduling/local_lease_manager.cc` | 516-545 | `TrySpillback` — 找远程节点溢出 |
| `src/ray/raylet/scheduling/local_lease_manager.cc` | 440-512 | `SpillWaitingLeases` — 遍历 waiting 队列尝试溢出，exclude_local_node 由 LeaseDependenciesBlocked 决定 |
| `src/ray/raylet/scheduling/local_lease_manager.cc` | 126-134 | `ScheduleAndGrantLeases` — 调用 GrantScheduledLeasesToWorkers + SpillWaitingLeases |
| `src/ray/raylet/scheduling/local_lease_manager.cc` | 711-733 | `LeasesUnblocked` — 依赖对象就绪时从 waiting 移到 grant 队列并触发 ScheduleAndGrantLeases |
| `src/ray/raylet/node_manager.cc` | 1352-1371 | `HandleWorkerAvailable` — 包含 CleanupLease 调用和 ScheduleAndGrantLeases 触发 |
| `src/ray/raylet/node_manager.h` | 363-367 | `ReleaseWorker` — 仅从 leased_workers_ map 移除，不清除 worker 上的 GrantLeaseId |
| `src/ray/core_worker/task_submission/normal_task_submitter.cc` | 323 | `is_spillback = (raylet_address != nullptr)` — spillback 标志计算 |
| `src/ray/core_worker/task_submission/normal_task_submitter.cc` | 326-332 | `GetBestNodeForLease` — 首次选节点时调用 |
| `src/ray/core_worker/task_submission/normal_task_submitter.cc` | 335-340 | `RequestWorkerLease(grant_or_reject=is_spillback)` — 发送 lease 请求 |
| `src/ray/core_worker/task_submission/normal_task_submitter.cc` | 358-454 | 回复 callback — 4 种分支（canceled/rejected/granted/redirected） |
| `src/ray/core_worker/task_submission/normal_task_submitter.cc` | 420-424 | Rejected 处理 — `RAY_CHECK(is_spillback)` + 从头重新选节点 |
| `src/ray/core_worker/task_submission/normal_task_submitter.cc` | 444-454 | Redirected 处理 — `RAY_CHECK(!is_spillback)` + 带地址重试 |
| `src/ray/raylet_rpc_client/raylet_client.cc` | 53-77 | `RequestWorkerLease` — `method_timeout_ms=-1` |
| `src/ray/raylet/node_manager.cc` | 1781-1859 | `HandleRequestWorkerLease` — raylet 入口，透传 `grant_or_reject` |
| `src/ray/raylet/scheduling/cluster_lease_manager.cc` | 47-78 | `QueueAndScheduleLease` — 创建 Work 对象，`grant_or_reject` 存入 |
| `src/ray/raylet/scheduling/cluster_lease_manager.cc` | 196-295 | `ScheduleAndGrantLeases` — 集群级调度，调 `GetBestSchedulableNode` |
| `src/ray/raylet/scheduling/cluster_lease_manager.cc` | 214-217 | `PrioritizeLocalNode()` 决定 `preferred_node_id` |
| `src/ray/raylet/scheduling/cluster_lease_manager.cc` | 422-460 | `ScheduleOnNode` — 本地→LocalLeaseManager，远端→redirect/reject |
| `src/ray/raylet/scheduling/internal.h` | 63-82 | `Work` 类 — `grant_or_reject_` 字段和 `PrioritizeLocalNode()` |
| `src/ray/raylet/scheduling/internal.h` | 104-106 | `PrioritizeLocalNode() = grant_or_reject_ || is_selected_based_on_locality_` |
| `src/ray/raylet/scheduling/cluster_resource_scheduler.cc` | 292-395 | `GetBestSchedulableNode(LeaseSpec)` — 快速路径 + 完整调度策略 |
| `src/ray/raylet/scheduling/cluster_resource_scheduler.cc` | 318-326 | 快速路径 — `preferred==本地 + IsSchedulableOnNode` 直接返回 |
| `src/ray/raylet/scheduling/cluster_resource_scheduler.cc` | 151-225 | `GetBestSchedulableNode(ResourceRequest)` — Hybrid/Spread/NodeAffinity 策略 |
| `src/ray/raylet/scheduling/local_lease_manager.cc` | 520-541 | `TrySpillback` — 找远程节点溢出 |
| `src/ray/raylet/scheduling/local_lease_manager.cc` | 696-724 | `Spillback` — `grant_or_reject=true` reject，`false` redirect |
| `src/ray/raylet/scheduling/local_lease_manager.cc` | 326-354 | Pin 失败分支 — `args_missing` vs pinned 超限，都不调 TrySpillback |
