# 副本机制作业分析、Task 产出大小与 Spill 指标对账

## 概述

本文档整理 Ray 集群中 Object Replication 副本机制的作业分析结论、Task 产出大小的统计异常（为什么日志中显示的对象大小远小于 Plasma 物理大小）、以及 Spill 相关指标的完整对账体系。

---

## 一、副本机制作业情况分析

### 1.1 集群环境

| 项目 | 值 |
|------|-----|
| Head 节点 Prometheus | `http://10.50.81.40:9090` |
| Ray 指标前缀 | `ray_` |
| 当前作业 | `kml-task-100043003-record-100636054-prod-head-h-0` |
| 集群版本 | `2.55.1+kuaishou.2c9cf276ef` |
| Counter 修复 | 已包含（`06a3bbf606` 是 `2c9cf276ef` 的祖先） |

### 1.2 `object_replication_bytes` 虚高问题（已修复）

#### 问题

`object_replication_bytes` 显示 9327 GB，看似异常偏高。

#### 根因

原始代码使用 `Sum` 聚合 + `UpDownCounter` 类型 + `Delta` 时间粒度，导致 Prometheus 抓取时每次增量被重复累加。

#### 修复

改为 `Counter` 类型 + `Cumulative` 时间粒度（提交 `8b922ed`），已部署到集群且生效。

#### 验证

修复后 `object_replication_bytes` 增速 ~300 GB/10min，与 `replication_succeeded` 增速匹配，确认为 Counter 累计值，行为正常。

### 1.3 `allow_tidal_target=false` 的行为

#### 配置含义

`object_replication_allow_tidal_target` 默认为 `false`。当设为 `false` 时：

| 节点类型 | 作为 Replication Source | 作为 Replication Target |
|----------|----------------------|----------------------|
| Tidal（可抢占） | **可以** — 主动 push 出去 | **不可以** — 被排除 |
| Non-Tidal Worker | 可以 | 可以 |

#### 代码逻辑

```cpp
// node_manager.cc:3718 — SelectReplicationTarget
for (auto &node : cluster_resource_scheduler_->GetClusterResourceManager()) {
    const auto &node_id = node.first;
    bool is_tidal_node = ...;  // 判断节点是否为 tidal

    // ★ 核心过滤：tidal 节点不能作为 replication target
    if (!RayConfig::instance().object_replication_allow_tidal_target() && is_tidal_node) {
        continue;  // 跳过 tidal 节点
    }

    // 检查节点资源是否足够
    if (!node.GetLocalView().total.Has(ResourceID::ObjectStoreMemory())) {
        continue;
    }
    // ... usage ratio 检查 ...
    // 选为 target → PushForReplication
}
```

#### 影响

- Tidal 节点上的 Primary 对象会被主动 push 到 Non-Tidal Worker 节点（作为副本）
- Tidal 节点死亡时触发 `needs_recovery_*`（因为 Primary 在 Tidal 上）
- Non-Tidal Worker 节点死亡时触发 `secondary_copy_lost`（因为持有副本但不是 Primary）

### 1.4 `secondary_copy_lost` vs `needs_recovery` 的分析

#### 触发条件对比

| 指标 | 触发条件 | 含义 | 是否触发 Recovery |
|------|----------|------|------------------|
| `secondary_copy_lost` | Non-Tidal Worker 节点死亡（持有副本） | 副本丢失，但 Primary 仍存活 | **否** — Primary 还在，不需要恢复 |
| `needs_recovery_has_replica` | Tidal 节点死亡（Primary 在其上），副本存在于其他节点 | Primary 丢失，有副本可用 | **是** |
| `needs_recovery_no_replica` | 节点死亡，Primary 丢失且无任何副本 | Primary 丢失，无可用副本 | **是** |

#### 代码逻辑

```cpp
// node_manager.cc:2461 — HandleObjectMissing
void NodeManager::HandleObjectMissing(const ObjectID &object_id) {
    // 1. 通知 LeaseDependencyManager 重新阻塞依赖此对象的 lease
    const auto waiting_lease_ids = lease_dependency_manager_.HandleObjectMissing(object_id);

    // 2. 通知 PullManager 重新 pull
    // 3. 区分 Primary vs Replicated：
    if (local_object_manager_.IsPrimary(object_id)) {
        // Primary 丢失 → needs_recovery
        object_recovery_attempted_.Record(1);
        if (has_replica_on_other_node) {
            needs_recovery_has_replica_.Record(1);
        } else {
            needs_recovery_no_replica_.Record(1);
        }
    } else {
        // Replicated 副本丢失 → secondary_copy_lost
        secondary_copy_lost_.Record(1);
        // ★ 不触发 recovery — Primary 仍在其他节点
    }
}
```

#### 数值验证

```
object_recovery_attempted = 13
= needs_recovery_has_replica(12) + needs_recovery_no_replica(1)
```

完全匹配，Recovery 在正常工作。

#### Object Recovery / Objects on Dead Nodes 面板不显示的原因

面板只显示 `needs_recovery_*` 相关指标。`secondary_copy_lost` 不会触发 Recovery（Primary 仍存活），因此不在此面板中显示。这是正确行为。

### 1.5 Replicated delete_bytes >> Primary delete_bytes

#### 现象

```
Replicated delete_bytes = 7710 GB
Primary delete_bytes   = 4560 GB
```

#### 原因

**正常行为。** 连锁删除机制：Primary 被删除时，其所有副本也同时被删除。Replicated bytes 比例更高因为：

1. Replication 只复制大对象（>= `object_replication_min_size`，通常 100KB+）
2. 每个 Primary 删除时，所有副本同时被删除，Replicated 侧累加的 bytes 自然更大

### 1.6 MaybeReplicateObject 的触发条件

```cpp
// node_manager.cc — HandleObjectLocal 中
void NodeManager::MaybeReplicateObject(const ObjectInfo &object_info, ObjectSource source) {
    // 只有 CreatedByWorker 的对象才触发复制
    if (source != plasma::flatbuf::ObjectSource::CreatedByWorker) {
        return;
    }

    // 检查是否满足复制条件
    if (object_info.data_size < RayConfig::instance().object_replication_min_size()) {
        return;  // 对象太小，不需要复制
    }

    // 选择 target 节点
    auto targets = SelectReplicationTarget(object_info);
    for (auto &target : targets) {
        object_manager_.PushForReplication(object_info.object_id, target);
        object_replication_requested_.Record(1);
    }
}
```

**ReceivedByPush / ReceivedByPull / ReconstructedByWorker 等来源的对象不触发复制**，避免递归复制。

---

## 二、Task 产出大小分析——为什么日志中显示那么小

### 2.1 问题

QGPreprocessMapper 输出的对象在 Ray Data 日志 / BlockMetadata 中显示 `size_bytes` 仅为 ~20 KB/block，但同一对象在 Plasma Store 中的物理大小为 ~1 GiB。差异高达 ~50000 倍。

**核心问题：日志中显示的 `size_bytes` 是什么，它是怎么算出来的，和 Plasma 物理大小是什么关系？**

### 2.2 对象大小计算的完整代码链路

#### 链路一：Python 侧序列化 — 决定写入 Plasma 的 `data_size`

```
Worker 执行 task → return value → Python 对象序列化
  → ray._private.serialization.SerializationContext.serialize(value)
    → 根据对象类型选择序列化方式：
       ├─ 小对象（< min_inline_bytes）：直接 inline，不进 Plasma
       └─ 大对象（>= min_inline_bytes）：写入 Plasma
            → 计算 data_size = len(serialized_data)
            → PlasmaClient::Create(object_id, data_size, metadata_size)
              → IPC → Plasma Store 分配共享内存
            → memcpy(data, serialized_data, data_size)
            → PlasmaClient::Seal(object_id)
              → IPC → Plasma Store Seal
              → ★ 此时 Store 记录 object_info.data_size = 实际写入的物理大小
              → add_object_callback_ → HandleObjectLocal
```

**关键点：`data_size` 是序列化后的二进制数据大小，不是 Python 对象的 `.nbytes` 属性。** 序列化过程使用 pickle / cloudpickle / custom serializer，对 variable-shaped tensor 会完整序列化所有 ndarray 数据，因此 Plasma 中的物理大小是准确的。

#### 链路二：Ray Data BlockMetadata 中的 `size_bytes` — 这是日志显示的值

```
Ray Data MapOperator (如 QGPreprocessMapper)
  → map_batches UDF 返回 batch 数据
  → _build_block() 构建 Block
    → BlockAccessor.for_block(block).get_metadata()
      → BlockMetadata(
           schema=...,
           num_rows=...,
           size_bytes=???    ← ★ 这就是日志中显示的值
         )
```

**`size_bytes` 的计算方式取决于 Block 类型**：

```python
# ArrowBlock（pyarrow.Table）
size_bytes = table.nbytes
# → 准确！pyarrow.Table.nbytes 计算所有列的物理大小，包括 binary 数据

# TensorBlock（TensorArray）
size_bytes = tensor_array.nbytes
# → 不准确！variable-shaped tensor 时严重偏小

# SimpleBlock / 其他
size_bytes = sum(len(serialize(x)) for x in block)  # 或近似值
```

### 2.3 `TensorArray.nbytes` 的 Bug 详解

#### 代码逻辑

```python
# TensorArray 内部表示
class TensorArray:
    def __init__(self, numpy_data):
        if is_variable_shaped(numpy_data):
            # variable-shaped tensor → 使用 object-dtype ndarray
            # 每个 element 是一个 Python 对象引用（8 bytes pointer）
            self._ndarray = np.array(numpy_data, dtype=object)
        else:
            # fixed-shape tensor → 使用普通 ndarray
            self._ndarray = numpy_data

    @property
    def nbytes(self):
        return self._ndarray.nbytes
        # ★ 对 object-dtype ndarray:
        #   self._ndarray.dtype.itemsize = 8 (指针大小)
        #   self._ndarray.shape = (num_rows,)
        #   → nbytes = 8 * num_rows
        #   例如 num_rows=2500 → nbytes = 20000 字节 (~20 KB)
        #
        # 对固定形状 ndarray:
        #   nbytes = dtype.itemsize * shape[0] * ... * shape[-1]
        #   → 准确
```

#### 为什么差异这么大

| 阶段 | 大小 | 说明 |
|------|------|------|
| Python `TensorArray.nbytes` | ~20 KB | 只统计指针数组（`8 * num_rows`） |
| Python 序列化后（pickle/cloudpickle） | ~1 GiB | 完整序列化每个 ndarray 的实际数据 |
| Plasma `object_info.data_size` | ~1 GiB | = 序列化后的大小（准确） |
| Ray Data BlockMetadata `size_bytes` | ~20 KB | = `TensorArray.nbytes`（来自 Bug） |

**差异根因**：`TensorArray.nbytes` 只统计了 object-dtype ndarray 的指针数组大小，没有递归统计每个 Python 对象引用指向的实际 ndarray 数据。而 Python 序列化器（pickle）会完整序列化所有嵌套对象，所以 Plasma 中的物理大小是准确的。

#### 影响

| 使用 `size_bytes` 的场景 | 影响 |
|------------------------|------|
| Ray Data 日志中 Block 大小 | 显示 ~20 KB 而非 ~1 GiB，误导运维 |
| BlockMetadata 估算 | 严重偏低 |
| PinLeaseArgs 内存限制 | 可能低估实际占用 |
| PullManager 配额计算 | 可能低估 |
| **Replication / Spill 调度** | **不受影响** — 使用 `object_info.data_size`（Plasma 真实大小） |

### 2.4 `pyarrow.Table.nbytes` 的行为

**包含 binary/tensor payload**（之前分析有误，已纠正）。

```python
# pyarrow.Table.nbytes 计算逻辑
# 遍历所有列：
for column in table.columns:
    # 对 binary 列：计算所有 binary 值的实际长度
    # 对 int/float 列：计算 itemsize * length
    # 对 list/struct 列：递归计算子列
    size += column.nbytes

# 不包括：schema、metadata、列名等开销
# 包括：所有 binary 类型的实际数据长度
```

### 2.5 `RayObject::GetSize()` — C++ 侧的准确大小

```cpp
// ray_object.h:100
uint64_t GetSize() const {
    uint64_t size = 0;
    size += (data_ != nullptr) ? data_->Size() : 0;      // ★ PlasmaBuffer 的 Size
    size += (metadata_ != nullptr) ? metadata_->Size() : 0;
    return size;
}
```

`data_->Size()` 来自 Plasma Store 分配的共享内存 buffer 大小，等于 `object_info.data_size`，是准确的物理大小。

### 2.6 `object_info.data_size` 的来源 — Plasma Store Create 时确定

```cpp
// plasma_store_provider.cc:104 — Worker 写入 Plasma
Status CoreWorkerPlasmaStoreProvider::Put(const RayObject &object, ...) {
    // ★ data_size 来自序列化后的实际大小
    RAY_RETURN_NOT_OK(Create(object.GetMetadata(),
                             object.HasData() ? object.GetData()->Size() : 0,  // ← data_size
                             object_id, owner_address, &data,
                             /*created_by_worker=*/true));
    if (data != nullptr) {
        if (object.HasData()) {
            memcpy(data->Data(), object.GetData()->Data(), object.GetData()->Size());
        }
        RAY_RETURN_NOT_OK(Seal(object_id));  // → Store 记录 data_size
    }
}

// protocol.cc:248 — IPC 协议解析 Create 请求
object_info->data_size = message->data_size();    // ★ 客户端请求的 data_size
object_info->metadata_size = message->metadata_size();

// obj_lifecycle_mgr.cc:42 — Store 端 Create
const LocalObject *ObjectLifecycleManager::CreateObject(const ray::ObjectInfo &object_info, ...) {
    RAY_LOG(DEBUG) << "Creating object " << object_info.object_id
                   << " size " << object_info.data_size;
    // 分配 data_size + metadata_size 的共享内存
    // Seal 后 add_object_callback_ → HandleObjectLocal → MaybeReplicateObject
    // 此时 object_info.data_size 是准确的物理大小
}
```

### 2.7 Ray 中对象大小的来源总结

| 来源 | 代码位置 | 值 | 准确性 | 用途 |
|------|----------|-----|--------|------|
| `object_info.data_size` | `plasma/protocol.cc:248` | IPC Create 请求中的 data_size | **准确** | Replication/Spill/指标 |
| `RayObject::GetSize()` | `ray_object.h:100` | data->Size() + metadata->Size() | **准确** | PinLeaseArgs/PullManager 配额 |
| `TensorArray.nbytes` | Python 侧 | object-dtype ndarray 的 nbytes | **严重偏低** | Ray Data BlockMetadata |
| `pyarrow.Table.nbytes` | Python 侧 | 所有列物理大小之和 | **准确** | Ray Data BlockMetadata |
| BlockMetadata `size_bytes` | Python 侧 | 取决于 Block 类型 | **TensorBlock 时偏低** | 日志/Dashboard 显示 |

### 2.8 为什么日志显示 ~20 KB 而非 ~1 GiB——完整解释

```
QGPreprocessMapper 输出流程：

1. Worker 执行 UDF → 返回 variable-shaped tensor 数据
   → 每行是一个不同 shape 的 ndarray（例如视频特征向量）

2. Ray Data 构建 TensorBlock
   → TensorArray(numpy_data)
     → 内部存为 object-dtype ndarray（指针数组）
     → .nbytes = 8 * num_rows ≈ 20 KB

3. 序列化写入 Plasma
   → pickle/cloudpickle 序列化整个 TensorArray
   → 递归序列化每个指针指向的 ndarray
   → serialized_data_size ≈ 1 GiB
   → Plasma Create(data_size=1 GiB) → 分配共享内存 → memcpy → Seal
   → ★ object_info.data_size = 1 GiB（准确）

4. 构建 BlockMetadata
   → size_bytes = TensorArray.nbytes ≈ 20 KB  ← ★ Bug：用了 Python 侧的 nbytes
   → 而非 object_info.data_size = 1 GiB

5. 日志输出
   → "Block X: size_bytes=20KB, num_rows=2500"  ← 用户看到的
   → 但实际 Plasma 物理大小 = 1 GiB

结论：日志中的 size_bytes 来自 Python 侧 TensorArray.nbytes，
      只统计了指针数组大小（8 * num_rows），没有递归统计实际 ndarray 数据。
      Plasma 物理大小（object_info.data_size）是准确的，因为序列化器完整处理了所有嵌套对象。
```

---

## 三、Spill 相关指标对账

### 3.1 Spill 指标体系总览

#### 指标来源：`LocalObjectManager::RecordMetrics()`（`local_object_manager.cc:628`）

| Prometheus 指标名 | 类型 | Tag | 记录的值 | 含义 |
|------------------|------|-----|---------|------|
| `ray_spill_manager_objects_bytes` | Gauge | State=Pinned | `pinned_objects_size_` | 当前 Primary pinned 大小 |
| `ray_spill_manager_objects_bytes` | Gauge | State=PendingSpill | `num_bytes_pending_spill_` | 正在写磁盘的 bytes |
| `ray_spill_manager_objects_bytes` | Gauge | State=PendingRestore | `num_bytes_pending_restore_` | 正在从磁盘恢复的 bytes |
| `ray_spill_manager_objects_bytes` | Gauge | State=Spilled | `spilled_bytes_total_` | **累计** spill 总量 |
| `ray_spill_manager_objects_bytes` | Gauge | State=Restored | `restored_objects_total_` | **累计** restore 总次数 |
| `ray_spill_manager_objects` | Gauge | State=Pinned | `pinned_objects_.size()` | 当前 Primary pinned 对象数 |
| `ray_spill_manager_objects` | Gauge | State=PendingRestore | `objects_pending_restore_.size()` | 等待恢复的对象数 |
| `ray_spill_manager_objects` | Gauge | State=PendingSpill | `objects_pending_spill_.size()` | 等待 spill 的对象数 |
| `ray_spill_manager_request_total` | Gauge | Type=Spilled | `spilled_objects_total_` | **累计** spill 对象总次数 |
| `ray_spill_manager_request_total` | Gauge | Type=Restored | `restored_objects_total_` | **累计** restore 对象总次数 |
| `ray_spill_manager_throughput_mb` | Gauge | Type=Spilled | `spilled_bytes_total_ / spill_time_total_s_` | Spill 吞吐率 (MiB/s) |
| `ray_spill_manager_throughput_mb` | Gauge | Type=Restored | `restored_bytes_total_ / restore_time_total_s_` | Restore 吞吐率 (MiB/s) |

### 3.2 Spill 完整代码链路

#### Pin 入口

```cpp
// local_object_manager.cc:25 — PinObjectsAndWaitForFree
void LocalObjectManager::PinObjectsAndWaitForFree(
    const vector<ObjectID> &object_ids,
    vector<unique_ptr<RayObject>> &&objects,
    const rpc::Address &owner_address,
    const ObjectID &generator_id) {
  for (size_t i = 0; i < object_ids.size(); i++) {
    const auto &object_id = object_ids[i];
    auto &object = objects[i];
    if (object == nullptr) {
      RAY_LOG(ERROR) << "Plasma object " << object_id
                     << " was evicted before the raylet could pin it.";
      continue;
    }
    // 第一次 pin 此对象
    const auto inserted = local_objects_.emplace(
        object_id, LocalObjectInfo(owner_address, generator_id, object->GetSize()));
    if (inserted.second) {
      pinned_objects_size_ += object->GetSize();     // ★ 累加 pinned 大小
      pinned_objects_.emplace(object_id, std::move(object));  // ★ 持有 RayObject 引用
    }

    // 订阅 Owner 的 WorkerObjectEviction 消息
    // 当 Owner freed 对象时 → subscription_callback → ReleaseFreedObject
    auto subscription_callback = [this, owner_address](const rpc::PubMessage &msg) {
      const auto &object_eviction_msg = msg.worker_object_eviction_message();
      const auto obj_id = ObjectID::FromBinary(object_eviction_msg.object_id());
      ReleaseFreedObject(obj_id);
    };
    auto owner_dead_callback = [this, owner_address](const string &object_id_binary, ...) {
      const auto obj_id = ObjectID::FromBinary(object_id_binary);
      ReleaseFreedObject(obj_id);
    };
    core_worker_subscriber_->Subscribe(..., subscription_callback, owner_dead_callback);
  }
}
```

#### Spill 入口

```cpp
// local_object_manager.cc:169 — SpillObjectUptoMaxThroughput
void LocalObjectManager::SpillObjectUptoMaxThroughput() {
  if (RayConfig::instance().object_spilling_config().empty()) {
    return;  // 未配置 spill 存储，跳过
  }
  bool can_spill_more = true;
  while (can_spill_more) {
    if (!TryToSpillObjects()) {
      break;  // 没有可 spill 的对象
    }
    can_spill_more = num_active_workers_ < max_active_workers_;  // IO worker 并发限制
  }
}

// local_object_manager.cc:183 — TryToSpillObjects
bool LocalObjectManager::TryToSpillObjects() {
  int64_t bytes_to_spill = 0;
  vector<ObjectID> objects_to_spill;

  // ★ 遍历 pinned_objects_（Primary 对象），选可 spill 的
  for (const auto &[object_id, ray_object] : pinned_objects_) {
    if (is_plasma_object_spillable_(object_id)) {      // 检查 ref_count == 1（只有 raylet 引用）
      int64_t object_size = ray_object->GetSize();
      // 文件大小限制（多个对象可能 fuse 到一个文件）
      if (max_spilling_file_size_bytes_ > 0 && !objects_to_spill.empty() &&
          bytes_to_spill + object_size > max_spilling_file_size_bytes_) {
        break;
      }
      bytes_to_spill += object_size;
      objects_to_spill.push_back(object_id);
      if (objects_to_spill.size() == max_fused_object_count_) {
        break;
      }
    }
  }

  if (objects_to_spill.empty()) return false;

  // 太小且已有正在 spill 的 → 等一等
  if (bytes_to_spill < min_spilling_size_ && !objects_pending_spill_.empty()) {
    return false;
  }

  // move RayObject 到 pending_spill（释放 raylet 引用）
  for (auto &obj_id : objects_to_spill) {
    auto it = pinned_objects_.find(obj_id);
    num_bytes_pending_spill_ += it->second->GetSize();
    objects_pending_spill_.emplace(obj_id, std::move(it->second));
    pinned_objects_size_ -= it->second->GetSize();
    pinned_objects_.erase(it);
  }

  // 发给 IO Worker 序列化写磁盘
  SpillObjectsInternal(objects_to_spill, callback);
}
```

**注意：以上是 Primary 的 spill 路径。Replicated 对象的 spill 优先级更高，先从 `replicated_object_manager_->GetSpillableObjects()` 获取，再遍历 `pinned_objects_`。**

#### OnObjectSpilled — Spill 完成，更新指标

```cpp
// local_object_manager.cc:390 — OnObjectSpilled
void LocalObjectManager::OnObjectSpilled(
    const vector<ObjectID> &object_ids,
    const rpc::SpillObjectsReply &worker_reply) {
  for (size_t i = 0; i < worker_reply.spilled_objects_url_size(); ++i) {
    const ObjectID &object_id = object_ids[i];
    const string &object_url = worker_reply.spilled_objects_url(i);

    // URL 引用计数（多对象可能 fuse 到同一文件）
    auto parsed_url = ParseURL(object_url);
    const auto base_url_it = parsed_url->find("url");
    url_ref_count_[base_url_it->second] += 1;

    // 记录 spill URL
    spilled_objects_url_.emplace(object_id, object_url);

    // ★ 从 pending_spill 移除，更新指标
    auto it = objects_pending_spill_.find(object_id);
    const auto object_size = it->second->GetSize();
    num_bytes_pending_spill_ -= object_size;
    objects_pending_spill_.erase(it);

    // ★★★ 累计 spill 指标（当前缺少 Source 拆分）
    spilled_bytes_total_ += object_size;       // 累计总量
    spilled_bytes_current_ += object_size;     // 当前磁盘上的量
    spilled_objects_total_++;                  // 累计次数

    // 通知 Object Directory
    auto freed_it = local_objects_.find(object_id);
    if (freed_it == local_objects_.end() || freed_it->second.is_freed_) {
      continue;  // 对象已被 freed，跳过通知
    }
    object_directory_->ReportObjectSpilled(
        object_id, self_node_id_, freed_it->second.owner_address_,
        object_url, ..., is_external_storage_type_fs_);
  }
}
```

#### OnObjectRestored — Restore 完成，更新指标

```cpp
// local_object_manager.cc:470 — AsyncRestoreSpilledObject
void LocalObjectManager::AsyncRestoreSpilledObject(
    const ObjectID &object_id, int64_t object_size,
    const string &object_url, function<void(const Status &)> callback) {
  if (objects_pending_restore_.count(object_id) > 0) {
    return;  // dedup：同一对象只恢复一次
  }
  objects_pending_restore_.emplace(object_id);
  num_bytes_pending_restore_ += object_size;

  // 请求 IO Worker 从磁盘读取
  io_worker_pool_.PopRestoreWorker([this, ...](auto io_worker) {
    rpc::RestoreSpilledObjectsRequest request;
    request.add_spilled_objects_url(object_url);
    request.add_object_ids_to_restore(object_id.Binary());
    io_worker->rpc_client()->RestoreSpilledObjects(
        request, [this, ...](const Status &status, const RestoreSpilledObjectsReply &r) {
          io_worker_pool_.PushRestoreWorker(io_worker);
          num_bytes_pending_restore_ -= object_size;
          objects_pending_restore_.erase(object_id);

          if (!status.ok()) {
            RAY_LOG(ERROR) << "Failed to restore spilled object";
          } else {
            auto restored_bytes = r.bytes_restored_total();
            // ★★★ 累计 restore 指标（当前缺少 Source 拆分）
            restored_bytes_total_ += restored_bytes;   // 累计总量
            restored_objects_total_ += 1;               // 累计次数
            restore_time_total_s_ += elapsed_seconds;
          }
          callback(status);
        });
  });
}
```

#### RecordMetrics — 上报 Prometheus

```cpp
// local_object_manager.cc:628 — RecordMetrics
void LocalObjectManager::RecordMetrics() const {
  // 吞吐率
  if (spilled_bytes_total_ != 0 && spill_time_total_s_ != 0) {
    spill_manager_metrics_.spill_manager_throughput_mb_gauge.Record(
        spilled_bytes_total_ / 1024 / 1024 / spill_time_total_s_, {{"Type", "Spilled"}});
  }
  if (restored_bytes_total_ != 0 && restore_time_total_s_ != 0) {
    spill_manager_metrics_.spill_manager_throughput_mb_gauge.Record(
        restored_bytes_total_ / 1024 / 1024 / restore_time_total_s_, {{"Type", "Restored"}});
  }

  // 对象数量
  spill_manager_metrics_.spill_manager_objects_gauge.Record(
      pinned_objects_.size(), {{"State", "Pinned"}});
  spill_manager_metrics_.spill_manager_objects_gauge.Record(
      objects_pending_restore_.size(), {{"State", "PendingRestore"}});
  spill_manager_metrics_.spill_manager_objects_gauge.Record(
      objects_pending_spill_.size(), {{"State", "PendingSpill"}});

  // 字节数
  spill_manager_metrics_.spill_manager_objects_bytes_gauge.Record(
      pinned_objects_size_, {{"State", "Pinned"}});
  spill_manager_metrics_.spill_manager_objects_bytes_gauge.Record(
      num_bytes_pending_spill_, {{"State", "PendingSpill"}});
  spill_manager_metrics_.spill_manager_objects_bytes_gauge.Record(
      num_bytes_pending_restore_, {{"State", "PendingRestore"}});
  spill_manager_metrics_.spill_manager_objects_bytes_gauge.Record(
      spilled_bytes_total_, {{"State", "Spilled"}});     // ★ 无 Source 标签
  spill_manager_metrics_.spill_manager_objects_bytes_gauge.Record(
      restored_objects_total_, {{"State", "Restored"}});  // ★ 无 Source 标签

  // 请求总数
  spill_manager_metrics_.spill_manager_request_total_gauge.Record(
      spilled_objects_total_, {{"Type", "Spilled"}});      // ★ 无 Source 标签
  spill_manager_metrics_.spill_manager_request_total_gauge.Record(
      restored_objects_total_, {{"Type", "Restored"}});    // ★ 无 Source 标签

  // 磁盘 spill 大小
  object_store_memory_gauge_.Record(
      spilled_bytes_current_, {{stats::LocationKey, "SPILLED"}});
}
```

#### ReleaseFreedObject — Owner freed 时清理

```cpp
// local_object_manager.cc:106 — ReleaseFreedObject
void LocalObjectManager::ReleaseFreedObject(const ObjectID &object_id) {
  auto it = local_objects_.find(object_id);
  if (it == local_objects_.end() || it->second.is_freed_) {
    return;  // 已 freed 或不存在
  }
  it->second.is_freed_ = true;  // 标记为 freed

  // 对象在三种状态之一：pinned、spilling、spilled
  auto pinned_objects_it = pinned_objects_.find(object_id);
  if (pinned_objects_it != pinned_objects_.end()) {
    // ★ 状态1：仍被 pinned（未 spill）→ 直接删除
    pinned_objects_size_ -= pinned_objects_it->second->GetSize();
    pinned_objects_.erase(pinned_objects_it);
    local_objects_.erase(it);
  } else {
    // ★ 状态2/3：正在 spill 或已 spill → 加入 pending_delete 队列
    // 等 spill 完成后再删除磁盘文件
    spilled_object_pending_delete_.push(object_id);
  }
}
```

#### ProcessSpilledObjectsDeleteQueue — 清理已 freed 的 spill 文件

```cpp
// local_object_manager.cc:530 — ProcessSpilledObjectsDeleteQueue
void LocalObjectManager::ProcessSpilledObjectsDeleteQueue(int64_t delete_count) {
  vector<string> object_urls_to_delete;
  while (!spilled_object_pending_delete_.empty() && delete_count-- > 0) {
    const auto &object_id = spilled_object_pending_delete_.front();

    // 如果对象还在 pending_spill，等 spill 完成再处理
    if (objects_pending_spill_.contains(object_id)) {
      break;
    }

    // 对象已 spill → 清理磁盘
    const auto spilled_objects_url_it = spilled_objects_url_.find(object_id);
    if (spilled_objects_url_it != spilled_objects_url_.end()) {
      string &object_url = spilled_objects_url_it->second;
      auto parsed_url = ParseURL(object_url);
      const auto base_url_it = parsed_url->find("url");
      url_ref_count_it->second -= 1;  // ★ URL 引用计数 -1

      // 所有引用此文件的对象都 freed → 删除文件
      if (url_ref_count_it->second == 0) {
        url_ref_count_.erase(url_ref_count_it);
        object_urls_to_delete.emplace_back(object_url);
      }
      spilled_objects_url_.erase(spilled_objects_url_it);

      // ★ 更新当前 spill 大小
      spilled_bytes_current_ -= local_objects_.at(object_id).object_size_;
    } else {
      // 未 spill → 重新 pin（防止内存泄漏）
      pinned_objects_.erase(object_id);
    }
    local_objects_.erase(object_id);
    spilled_object_pending_delete_.pop();
  }

  if (!object_urls_to_delete.empty()) {
    DeleteSpilledObjects(std::move(object_urls_to_delete));  // 异步删除磁盘文件
  }
}
```

### 3.3 对账公式

#### 基本守恒

```
spilled_bytes_total = 累计所有被 spill 的对象大小之和（只增不减）
restored_bytes_total = 累计所有被 restore 的对象大小之和（只增不减）

spilled_bytes_current = 当前磁盘上的 spill 大小
                     = spilled_bytes_total - 已删除的 spill 大小
```

#### 对账关系

| 对账项 | 公式 | 说明 |
|--------|------|------|
| Spill 对象数 vs Spill bytes | `spilled_objects_total * avg_object_size ≈ spilled_bytes_total` | 每次 spills 一个对象，两个计数器同时增加 |
| Spilled 当前量 | `spilled_bytes_current_` 在 spill 时 +、在 DeleteQueue 时 - | 通过 `spilled_object_pending_delete_` 清理 |
| Pinned 对象数 | `pinned_objects_.size()` = `local_objects_` 中 is_freed_=false 的数量 | 被 freed 的对象从 pinned 移到 pending_delete |
| Primary pinned bytes | `pinned_objects_size_` = `pinned_objects_` 中所有对象大小之和 | 对应 `ray_spill_manager_objects_bytes{State=Pinned}` |
| Pinned + Pending + Spilled(current) | `pinned_objects_size_ + num_bytes_pending_spill_ + spilled_bytes_current_` | 三者之和为 Raylet 管理的本地 Primary 总占用 |

#### Spill -> Delete 完整生命周期

```
Worker 创建对象 → Seal → add_object_callback_
  → PinObjectsAndWaitForFree: pinned_objects_[id] = RayObject, pinned_objects_size_ += size
  → 订阅 Owner 的 eviction 通知

内存压力 → SpillObjectUptoMaxThroughput
  → TryToSpillObjects: 遍历 pinned_objects_ → move 到 objects_pending_spill_
    → pinned_objects_size_ -= size, num_bytes_pending_spill_ += size
  → SpillObjectsInternal: IO Worker 写磁盘

IO Worker 完成 → OnObjectSpilled
  → num_bytes_pending_spill_ -= size
  → spilled_bytes_total_ += size, spilled_bytes_current_ += size, spilled_objects_total_++
  → spilled_objects_url_[id] = url

Owner freed → ReleaseFreedObject: is_freed_ = true
  → if 已 spill: spilled_object_pending_delete_.push(id)
  → if 未 spill: pinned_objects_.erase → local_objects_.erase

ProcessSpilledObjectsDeleteQueue:
  → url_ref_count_-- → if == 0: 删除磁盘文件
  → spilled_bytes_current_ -= size
  → local_objects_.erase(id)
```

### 3.4 SpilledTotal / RestoredTotal 缺少 Source 标签问题

#### 问题

当前 `spilled_bytes_total_` 和 `restored_bytes_total_` **混合累加** Primary 和 Replicated 对象，无法区分来源。

#### 需要 Source 标签的原因

- Replication 对象的 spill 优先级更高（先 `GetSpillableObjects` 再遍历 `pinned_objects_`）
- Replicated 对象的恢复可能通过 re-replication 而非磁盘恢复
- 没有标签时无法在 Grafana 中区分两者的 spill/restore 量和速率

#### 需要拆分的指标

| 指标 | 当前 | 改进 |
|------|------|------|
| `spilled_bytes_total_` | 单一计数器 | 拆分为 `primary_spilled_bytes_total_` + `replicated_spilled_bytes_total_` |
| `restored_bytes_total_` | 单一计数器 | 同理拆分 |
| `spilled_objects_total_` | 单一计数器 | 同理拆分 |
| `restored_objects_total_` | 单一计数器 | 同理拆分 |

#### 可用的判别依据

```cpp
// local_object_manager.cc — SpillObjectUptoMaxThroughput 中
// Replicated 对象来源：replicated_object_manager_->GetSpillableObjects()
// Primary 对象来源：pinned_objects_ 遍历
// pending_spill_is_replicated_ map 已记录每个 spill 对象是否为 replicated

// OnObjectSpilled 中可判断：
if (pending_spill_is_replicated_.count(object_id)) {
    replicated_spilled_bytes_total_ += object_size;
} else {
    primary_spilled_bytes_total_ += object_size;
}
```

### 3.5 需要加 Source 标签的指标优先级

| 优先级 | 指标 | 说明 |
|--------|------|------|
| **P0** | SpilledTotal / RestoredTotal | 无法区分 Primary vs Replicated 的 spill/restore 量 |
| **P1** | `spill_manager_request_total` | 当前只有 Type=Spilled/Restored，缺 Source |
| **P2** | `spill_manager_objects_bytes` PendingSpill | 当前 pending 中混了 Primary 和 Replicated |
| **P3** | `object_replication_bytes` Grafana 面板 | 应改为 `rate()` 或 `increase()` 显示增速 |

---

## 四、指标对账验证方法

### 4.1 Spill 指标对账

#### 验证 1：Spill 吞吐率

```
rate(ray_spill_manager_objects_bytes{State="Spilled"}[5m])
  ≈ ray_spill_manager_throughput_mb{Type="Spilled"} * 1024 * 1024  (bytes/s)
```

#### 验证 2：Pinned 量守恒

```
ray_spill_manager_objects_bytes{State="Pinned"}
  + ray_spill_manager_objects_bytes{State="PendingSpill"}
  ≈ Raylet 管理的 Primary 总占用
```

#### 验证 3：Spill 累计量 vs 当前量

```
spilled_bytes_total（累计，只增不减）
  - spilled_bytes_current（当前磁盘上）
  = 已删除的 spill 大小
```

### 4.2 Replication 指标对账

#### 验证 1：replication_succeeded 增速与 replication_bytes 增速匹配

```
increase(ray_object_replication_succeeded[10m]) * avg_object_size
  ≈ increase(ray_object_replication_bytes[10m])
```

#### 验证 2：recovery 数值匹配

```
ray_object_recovery_attempted
  = ray_object_recovery_has_replica + ray_object_recovery_no_replica
```

#### 验证 3：replication skipped 原因分布

```
ray_object_replication_skipped_received{Reason="already_pulling"}     — Pull 冲突
ray_object_replication_skipped_received{Reason="object_already_exists"} — 对象已存在
```

### 4.3 Delete 指标对账

```
Primary delete_bytes < Replicated delete_bytes  ← 正常（连锁删除 + 大对象复制）

Primary delete 增速 ≈ owner freed / lineage GC 增速
Replicated delete 增速 ≈ Primary delete 增速 * 平均副本数
```

---

## 五、关键代码位置索引

| 功能 | 文件 | 行号 | 说明 |
|------|------|------|------|
| Pin 入口 | `local_object_manager.cc:25` | `PinObjectsAndWaitForFree` | 首次 pin + 订阅 Owner eviction |
| Spill 入口 | `local_object_manager.cc:169` | `SpillObjectUptoMaxThroughput` | 优先 Replicated，其次 Primary |
| TryToSpillObjects | `local_object_manager.cc:183` | 遍历 pinned_objects_ 选可 spill 的 | is_plasma_object_spillable_ 检查 |
| OnObjectSpilled | `local_object_manager.cc:390` | `spilled_bytes_total_ += size` | **缺少 Source 拆分** |
| AsyncRestoreSpilledObject | `local_object_manager.cc:470` | IO Worker 读取磁盘 | `restored_bytes_total_ += size` |
| RecordMetrics | `local_object_manager.cc:628` | 上报所有 spill 指标 | **SpilledTotal/RestoredTotal 无 Source** |
| FillObjectStoreStats | `local_object_manager.cc:619` | RPC 暴露统计信息 | 供 Dashboard 使用 |
| ReleaseFreedObject | `local_object_manager.cc:106` | Owner freed 时 Unpin + 记录 delete | 区分已 spill / 未 spill |
| ProcessSpilledObjectsDeleteQueue | `local_object_manager.cc:530` | 清理已 freed 的 spill 对象 | url_ref_count_ 管理 |
| Replicated 优先 Spill | `local_object_manager.cc:190` | `GetSpillableObjects` 先于 `pinned_objects_` | Replicated spill 优先 |
| Replication Push 冲突 | `object_manager.cc:739` | `already_pulling` / `object_already_exists` | Push 被拒绝，Pull 获胜 |
| MaybeReplicateObject | `node_manager.cc:2421+` | `source != CreatedByWorker → return` | 仅 CreatedByWorker 触发复制 |
| SelectReplicationTarget | `node_manager.cc:3718` | `allow_tidal=false` 排除 tidal 节点 | Tidal 不作为 target |
| HandleObjectLocal | `node_manager.cc:2417` | 对象到达本地 → lease 解阻塞 | 不区分来源 |
| HandleObjectMissing | `node_manager.cc:2454` | 对象被 evict → lease 重新阻塞 | Primary/Replicated 分叉 |
| RayObject::GetSize | `ray_object.h:100` | data->Size() + metadata->Size() | 准确的物理大小 |
| Plasma Create | `plasma_store_provider.cc:104` | `object.GetData()->Size()` 为 data_size | 序列化后大小 |
| Plasma Store Create | `obj_lifecycle_mgr.cc:42` | 分配 data_size 共享内存 | 记录 object_info.data_size |
| SerializeReturnObject | `common.cc:69` | `return_object->GetSize()` → set_size() | Task 返回大小 |
| TensorArray.nbytes bug | Python 侧 | variable-shaped tensor 只返回指针大小 | ~20KB vs ~1GiB |
| object_replication_bytes 修复 | 已提交 `8b922ed` | Counter + Cumulative | 已部署生效 |
