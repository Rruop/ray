# Object Replication 优化修复方案

## 概述

本文档描述 Object Replication 功能的完整优化修复方案，涵盖 ObjectSource 枚举细化、IsReconstruction 标记传递链、Object Store Memory 按来源指标、复制目标选择 usage ratio 过滤等多个改进项。

---

## 一、ObjectSource 枚举细化

### 问题

原 `ReceivedFromRemoteRaylet` 一个枚举值覆盖了所有远程接收场景，无法区分 replication push、pull、lineage reconstruction。

### 修复

将 `plasma.fbs` 中的 `ObjectSource` 枚举拆分为：

| 枚举值 | 含义 |
|--------|------|
| `CreatedByWorker` | worker 本地创建 |
| `RestoredFromStorage` | 从外部存储恢复 |
| `ReceivedByPush` | 通过 replication push 接收 |
| `ReceivedByPull` | 通过 pull 请求接收 |
| `ReconstructedByWorker` | lineage reconstruction 重算 |
| `ErrorStoredByRaylet` | raylet 存储的错误对象 |

### 涉及文件

- `src/ray/object_manager/plasma/plasma.fbs` — 枚举定义
- `src/ray/object_manager/object_manager.cc` — `ReceivePullChunk` 传 `ReceivedByPull`，`ReceiveReplicationPushChunk` 传 `ReceivedByPush`
- `src/ray/object_manager/object_buffer_pool.cc/.h` — `CreateChunk`/`EnsureBufferExists` 新增 `source` 参数，不再硬编码 `ReceivedFromRemoteRaylet`

---

## 二、IsReconstruction 标记传递链

### 问题

lineage reconstruction 产生的对象无法被识别，所有重算对象在 stats 中都被计为 `CreatedByWorker`。

### 修复

新增 `is_reconstruction` 字段，从 task 重试入口一直传递到 plasma store 的 `ObjectSource`：

```
TaskSpec.is_reconstruction (common.proto #46)
    ↓
TaskSpecification::IsReconstruction() / SetIsReconstruction() (task_spec.h/.cc)
    ↓
WorkerContext::IsReconstruction() (context.h/.cc) — SetTaskExecutionInfo 时读取
    ↓
CoreWorker::CreateOwnedAndIncrementLocalRef / CreateExisting (core_worker.cc)
    ↓
PlasmaStoreProvider::Create(..., is_reconstruction) (plasma_store_provider.h/.cc)
    ↓
ObjectSource::ReconstructedByWorker (plasma.fbs)
```

### 涉及文件

- `src/ray/protobuf/common.proto` — 新增 `bool is_reconstruction = 46`
- `src/ray/common/task/task_spec.h/.cc` — 新增 `IsReconstruction()` / `SetIsReconstruction()`
- `src/ray/core_worker/context.h/.cc` — `WorkerThreadContext` 新增 `is_reconstruction_` 字段，在 `SetTaskExecutionInfo` 时从 `task_spec.IsReconstruction()` 读取，在 `ResetTaskStatistics` 时清零
- `src/ray/core_worker/core_worker.cc` — `CreateOwnedAndIncrementLocalRef` 和 `CreateExisting` 传入 `worker_context_->IsReconstruction()`
- `src/ray/core_worker/store_provider/plasma_store_provider.h/.cc` — `Create` 新增 `is_reconstruction` 参数，`is_reconstruction` 优先级高于 `created_by_worker`

---

## 三、TaskManager 重试路径的 is_reconstruction 设置

### 问题

1. `spec = task_entry.spec_` 是 shared_ptr 浅拷贝，`SetIsReconstruction` 修改了原始 protobuf
2. streaming generator 的 `ResubmitTask` 排队路径不需要设置 `is_reconstruction`（submitter 只用 TaskId 查表）
3. `async_retry_task_callback_` 传了旧的 `spec` 而非设了 `is_reconstruction` 的 `spec_copy`

### 三个重试路径的关系

| 路径 | 触发场景 | 是 reconstruction？ |
|------|---------|-------------------|
| `RetryTaskIfPossible` | Task 执行失败（error/retry_exceptions/OOM/node死亡等） | 不是，这是普通重试 |
| `ResubmitTask` | `ObjectRecoveryManager` 发现对象丢失，需要 lineage reconstruction | 是，这是重建 |
| `MarkGeneratorFailedAndResubmit` | Streaming generator 被 `ResubmitTask` 排队后，执行完成时回调 resubmit | 是，这也是重建链的一部分 |

`ResubmitTask` 和 `MarkGeneratorFailedAndResubmit` 不是互斥的，是上下游链式调用：

```
对象丢失 → ObjectRecoveryManager::RecoverObject()
               ↓
         ResubmitTask(task_id)
               ↓
    如果是 streaming generator 且正在运行：
         should_queue_generator_resubmit = true
         queue_generator_resubmit_(spec_copy)  ← 第1步：排队（不设 is_reconstruction）
         ↓
    Submitter 把 task_id 记入 generators_to_resubmit_
         ↓
    generator 执行完成，PushTaskReply 回来
         ↓
    Submitter 发现 generators_to_resubmit_ 有该 task
         resubmit_generator = true
         ↓
    MarkGeneratorFailedAndResubmit(task_id)  ← 第2步：实际 resubmit（设 is_reconstruction=true）
```

### 修复

```cpp
// ResubmitTask:
auto spec_copy = TaskSpecification(spec.GetMessage());  // 深拷贝

if (should_queue_generator_resubmit) {
    // 排队路径不设 is_reconstruction，submitter 只用 TaskId 查表
    return queue_generator_resubmit_(spec_copy) ? std::nullopt : ...;
}

spec_copy.SetIsReconstruction(true);  // 只在真正 resubmit 时设
UpdateReferencesForResubmit(spec_copy, ...);
async_retry_task_callback_(spec_copy, ...);  // 用 spec_copy

// MarkGeneratorFailedAndResubmit:
auto spec_copy = TaskSpecification(spec.GetMessage());  // 深拷贝
spec_copy.SetIsReconstruction(true);
async_retry_task_callback_(spec_copy, ...);
```

### 涉及文件

- `src/ray/core_worker/task_manager.cc`

---

## 四、Object Store Memory 按来源指标

### 问题

`object_store_memory` gauge 只按 Location（SHM/DISK）× State（SEALED/UNSEALED）上报，无法知道当前内存中各来源（Create/Push/Pull/Reconstruct）各占多少。

### 修复

新增 `object_store_memory_by_source` Gauge 指标：

```cpp
// metrics.h
inline ray::stats::Gauge GetObjectStoreMemoryBySourceGaugeMetric() {
  return ray::stats::Gauge{
      "object_store_memory_by_source",
      "Object store memory in bytes by object source on this node. "
      "Source can be CreatedByWorker, RestoredFromStorage, ReceivedByPush, "
      "ReceivedByPull, ReconstructedByWorker, or ErrorStoredByRaylet.",
      "bytes",
      {"Source"},
  };
}
```

在 `RecordMetrics()` 中按 6 种 Source 分别上报当前字节数，数据来源于已有的 `num_bytes_*` 计数器（`OnObjectCreated` 增 / `OnObjectDeleting` 减，对称）。

### 涉及文件

- `src/ray/common/metrics.h` — 新增 `GetObjectStoreMemoryBySourceGaugeMetric()`
- `src/ray/object_manager/plasma/stats_collector.h` — 新增 `object_store_memory_by_source_gauge_` 成员
- `src/ray/object_manager/plasma/stats_collector.cc` — `RecordMetrics()` 中上报 6 种 Source

---

## 五、Stats Collector 命名修正

### 问题

`ReceivedFromRemoteRaylet` 拆分后，`num_objects_received_` / `num_bytes_received_` 只跟踪 `ReceivedByPush`，命名歧义。

### 修复

| 原名 | 新名 |
|------|------|
| `num_objects_received_` | `num_objects_received_by_push_` |
| `num_bytes_received_` | `num_bytes_received_by_push_` |

同步更新 `GetDebugDump` 输出标签：`"objects received"` → `"objects received by push"`。

### 涉及文件

- `src/ray/object_manager/plasma/stats_collector.h` — 成员重命名
- `src/ray/object_manager/plasma/stats_collector.cc` — 所有引用 + debug dump 标签
- `src/ray/object_manager/plasma/tests/stats_collector_test.cc` — 测试中局部变量和断言

---

## 六、actual_object_store_memory_used 传递链

### 问题

复制目标选择（`SelectReplicationTarget`）只能看到 `available` 资源（可能被预留），不知道节点 object store 的实际使用量。

### 修复

新增 `actual_object_store_memory_used` 字段，从 raylet 传播到调度器：

```
raylet/main.cc: get_actual_used_object_store_memory = object_manager->GetUsedMemory()
    ↓
LocalResourceManager::PopulateResourceViewSyncMessage()
    → set_actual_object_store_memory_used(double)
    ↓
ray_syncer.proto: ResourceViewSyncMessage.actual_object_store_memory_used (#9)
    ↓
ClusterResourceManager::UpdateNode()
    → local_view.actual_object_store_memory_used = ...
    ↓
NodeResources.actual_object_store_memory_used (cluster_resource_data.h)
    ↓
NodeManager::SelectReplicationTarget() 读取使用
```

### 与 `get_used_object_store_memory` 的区别

| 回调 | scheduler_report_pinned_bytes_only 模式 | 正常模式 |
|------|----------------------------------------|---------|
| `get_used_object_store_memory` | 返回 primary bytes（用于调度决策） | 返回 `GetUsedMemory()` |
| `get_actual_used_object_store_memory` | 始终返回 `GetUsedMemory()` | 始终返回 `GetUsedMemory()` |

### 涉及文件

- `src/ray/protobuf/ray_syncer.proto` — 新增 `double actual_object_store_memory_used = 9`
- `src/ray/common/scheduling/cluster_resource_data.h` — `NodeResources` 新增 `double actual_object_store_memory_used = 0.0`
- `src/ray/raylet/main.cc` — 新增 `get_actual_used_object_store_memory` callback
- `src/ray/raylet/scheduling/local_resource_manager.h/.cc` — 构造函数新增参数，`PopulateResourceViewSyncMessage` 中设置
- `src/ray/raylet/scheduling/cluster_resource_scheduler.h/.cc` — 构造函数/Init 新增参数透传
- `src/ray/raylet/scheduling/cluster_resource_manager.h/.cc` — `UpdateNode` 中读取并设置到 `NodeResources`，新增 `SetActualObjectStoreMemoryUsed` public 方法

---

## 七、复制目标选择 usage ratio 过滤

### 问题

复制目标可能选到 object store 内存使用率已经很高的节点。

### 修复

在 `SelectReplicationTarget` 中新增 usage ratio 检查：

```cpp
double total_mem = node.GetLocalView()
    .total.Get(ResourceID::ObjectStoreMemory()).Double();
if (total_mem > 0) {
    double actual_used = node.GetLocalView().actual_object_store_memory_used;
    double usage_ratio = actual_used / total_mem;
    if (usage_ratio > RayConfig::instance()
            .object_replication_max_target_usage_ratio()) {  // default 0.85
        continue;  // 跳过高使用率节点
    }
}
```

`total_mem == 0` 时不做限制（未上报容量的节点不因除零被误过滤）。

### 涉及文件

- `src/ray/common/ray_config_def.h` — 新增 `object_replication_max_target_usage_ratio` (默认 0.85)
- `src/ray/raylet/node_manager.cc` — `SelectReplicationTarget` 新增过滤逻辑

---

## 八、MaybeReplicateObject 逻辑修正

### 问题

原逻辑 `source == ReceivedFromRemoteRaylet` 时 return，枚举拆分后需覆盖所有非 `CreatedByWorker` 的 source。

### 修复

改为 `source != CreatedByWorker` 时 return，即只有 `CreatedByWorker` 的对象才触发复制。

**关于 `ReconstructedByWorker` 不复制的原因**：对于 streaming generator，重建会产生可能无用的 object，由 `HandleReportGeneratorItemReturns` / `HandleTaskReturn` 负责过滤，因此不需要复制。

### 涉及文件

- `src/ray/raylet/node_manager.cc`

---

## 九、ClusterResourceManager 新增 public 方法

### 问题

测试需要设置 `actual_object_store_memory_used`，但 `AddOrUpdateNode` 是 private 的。

### 修复

新增 public 方法：

```cpp
bool ClusterResourceManager::SetActualObjectStoreMemoryUsed(
    scheduling::NodeID node_id, double used) {
  NodeResources local_view;
  if (!GetNodeResources(node_id, &local_view)) return false;
  local_view.actual_object_store_memory_used = used;
  AddOrUpdateNode(node_id, local_view);
  return true;
}
```

### 涉及文件

- `src/ray/raylet/scheduling/cluster_resource_manager.h/.cc`

---

## 十、local_resource_manager.h 注释修复

### 问题

新增 `get_actual_used_object_store_memory_` 成员时，`get_pull_manager_at_capacity_` 的注释被误删。

### 修复

恢复每个成员的注释：

```cpp
/// Function to get used object store memory.
std::function<int64_t(void)> get_used_object_store_memory_;
/// Function to get actual used object store memory.
std::function<int64_t(void)> get_actual_used_object_store_memory_;
/// Function to get whether the pull manager is at capacity.
std::function<bool(void)> get_pull_manager_at_capacity_;
```

### 涉及文件

- `src/ray/raylet/scheduling/local_resource_manager.h`

---

## 十一、Grafana Dashboard 面板

### 新增 2 个面板

| 面板 | 位置 | 查询 | 说明 |
|------|------|------|------|
| Object Store Memory by Source (id=1010) | x=12,y=149 | `sum(ray_object_store_memory_by_source{...}) by (Source)` | 集群汇总，按 Source 堆叠 |
| Object Store Memory by Source by Instance (id=1011) | x=0,y=157 | `sum(ray_object_store_memory_by_source{...}) by (Source, instance)` | 按节点拆分 |

### Dashboard 中 Object Store Memory 面板布局

| 面板 | 维度 | 说明 |
|------|------|------|
| Object Store Memory Usage (id=58) | by instance | 各节点总占用（SHM+DISK） |
| Object Store Memory Usage % (id=59) | by instance | 各节点占用百分比 |
| Object Store Memory Spilled to Disk (id=60) | by instance | 溢写到磁盘的 |
| Object Store Memory by Location (id=29) | by Location (SHM/DISK) | 按内存位置 |
| **Object Store Memory by Source (id=1010)** | **by Source** | **按来源（Create/Push/Pull/Reconstruct等），集群汇总** |
| **Object Store Memory by Source by Instance (id=1011)** | **by Source + instance** | **按来源+节点拆分** |

### 涉及文件

- `python/ray/dashboard/modules/metrics/dashboards/default_grafana_dashboard.json`

---

## 十二、FRIEND_TEST 更新

### 问题

测试重命名 `TestMaybeReplicateObjectReceivedFromRemote` → `TestMaybeReplicateObjectNonCreatedByWorker`，新增 `TestSelectReplicationTargetSkipsHighUsageNode`，但 `node_manager.h` 中 FRIEND_TEST 未同步。

### 修复

```cpp
// 删除
FRIEND_TEST(NodeManagerReplicationTest, TestMaybeReplicateObjectReceivedFromRemote);
// 新增
FRIEND_TEST(NodeManagerReplicationTest, TestMaybeReplicateObjectNonCreatedByWorker);
FRIEND_TEST(NodeManagerReplicationTest, TestSelectReplicationTargetSkipsHighUsageNode);
```

### 涉及文件

- `src/ray/raylet/node_manager.h`

---

## 测试覆盖

| 测试文件 | 新增/修改内容 |
|---------|-------------|
| `src/ray/common/tests/task_spec_test.cc` | `TestIsReconstruction` — 验证 set/get/clear |
| `src/ray/object_manager/plasma/tests/stats_collector_test.cc` | 6 种 source 的 CreateAndAbort / CreateAndDelete / Eviction 测试 |
| `src/ray/raylet/scheduling/tests/local_resource_manager_test.cc` | `PopulateResourceViewSyncMessageWithActualUsedMemory` / `WithoutActualUsedMemory` |
| `src/ray/raylet/scheduling/tests/cluster_resource_manager_test.cc` | `UpdateNode` 验证 `actual_object_store_memory_used` 传播 |
| `src/ray/raylet/tests/node_manager_replication_test.cc` | `TestMaybeReplicateObjectNonCreatedByWorker` / `TestSelectReplicationTargetSkipsHighUsageNode` |
| `src/ray/raylet/tests/node_manager_test.cc` | 构造函数新增 `get_actual_used_object_store_memory` 参数 |

---

## 变更文件总览（35 个文件）

| 文件 | 变更类型 |
|------|---------|
| `src/ray/object_manager/plasma/plasma.fbs` | 枚举拆分 |
| `src/ray/object_manager/object_manager.cc` | 传入新 source 枚举值 |
| `src/ray/object_manager/object_buffer_pool.cc` | 新增 source 参数 |
| `src/ray/object_manager/object_buffer_pool.h` | 新增 source 参数 |
| `src/ray/protobuf/common.proto` | 新增 is_reconstruction 字段 |
| `src/ray/protobuf/ray_syncer.proto` | 新增 actual_object_store_memory_used 字段 |
| `src/ray/common/task/task_spec.cc` | 新增 IsReconstruction / SetIsReconstruction |
| `src/ray/common/task/task_spec.h` | 新增声明 |
| `src/ray/common/metrics.h` | 新增 GetObjectStoreMemoryBySourceGaugeMetric |
| `src/ray/common/ray_config_def.h` | 新增 object_replication_max_target_usage_ratio |
| `src/ray/common/scheduling/cluster_resource_data.h` | NodeResources 新增字段 |
| `src/ray/core_worker/context.cc` | WorkerThreadContext 新增 is_reconstruction_ |
| `src/ray/core_worker/context.h` | 新增 IsReconstruction 方法 |
| `src/ray/core_worker/core_worker.cc` | 传入 IsReconstruction |
| `src/ray/core_worker/store_provider/plasma_store_provider.cc` | source 判断逻辑 |
| `src/ray/core_worker/store_provider/plasma_store_provider.h` | 新增参数 |
| `src/ray/core_worker/task_manager.cc` | deep copy + is_reconstruction 设置 |
| `src/ray/object_manager/plasma/stats_collector.cc` | 6 种 source + 新 gauge + 命名修正 |
| `src/ray/object_manager/plasma/stats_collector.h` | 成员重命名 + 新 gauge |
| `src/ray/raylet/main.cc` | 新增 callback |
| `src/ray/raylet/node_manager.cc` | source 检查 + usage ratio 过滤 |
| `src/ray/raylet/node_manager.h` | FRIEND_TEST 更新 |
| `src/ray/raylet/scheduling/cluster_resource_manager.cc` | UpdateNode + SetActualObjectStoreMemoryUsed |
| `src/ray/raylet/scheduling/cluster_resource_manager.h` | 新增方法声明 |
| `src/ray/raylet/scheduling/cluster_resource_scheduler.cc` | 参数透传 |
| `src/ray/raylet/scheduling/cluster_resource_scheduler.h` | 参数声明 |
| `src/ray/raylet/scheduling/local_resource_manager.cc` | PopulateResourceViewSyncMessage |
| `src/ray/raylet/scheduling/local_resource_manager.h` | 新增参数 + 注释修复 |
| `python/ray/dashboard/modules/metrics/dashboards/default_grafana_dashboard.json` | 新增 2 个面板 |
| `src/ray/common/tests/task_spec_test.cc` | 新增测试 |
| `src/ray/object_manager/plasma/tests/stats_collector_test.cc` | source 覆盖 + 命名修正 |
| `src/ray/raylet/scheduling/tests/local_resource_manager_test.cc` | 新增测试 + 参数适配 |
| `src/ray/raylet/scheduling/tests/cluster_resource_manager_test.cc` | 新增断言 |
| `src/ray/raylet/tests/node_manager_replication_test.cc` | 新增测试 + 参数适配 |
| `src/ray/raylet/tests/node_manager_test.cc` | 参数适配 |
