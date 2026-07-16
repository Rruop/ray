# Object Reconstruction 机制深度分析

## 1. 概述

当 object store 中的 object 因节点故障或内存压力被 evict 丢失时，Ray 提供 Object Reconstruction 机制，通过重新执行产生该 object 的 task（lineage re-execution）来恢复数据。本文档详细分析 reconstruction 的完整代码链路、触发条件、依赖递归重建、lineage 内存管理与淘汰机制、以及与 `_enable_object_reconstruction` 配置参数的关系。

## 2. 配置参数

### 2.1 `lineage_pinning_enabled`（C++ 层）

- **定义位置**：`src/ray/common/ray_config_def.h:162`
- **默认值**：`true`
- **作用**：控制 raylet 和 core_worker 是否保留已完成 task 的 spec（用于后续重建）和 object 的 lineage 引用关系

```cpp
RAY_CONFIG(bool, lineage_pinning_enabled, true)
```

### 2.2 `_enable_object_reconstruction`（Python 层）

- **定义位置**：`python/ray/_private/worker.py:1627`
- **默认值**：`False`
- **作用**：仅在 `ray.init()` 创建新集群时，往 `_system_config` 中写入 `lineage_pinning_enabled=True`

```python
# worker.py:1627
_enable_object_reconstruction: bool = kwargs.pop(
    "_enable_object_reconstruction", False
)
```

```python
# parameter.py:258
if enable_object_reconstruction:
    self._system_config["lineage_pinning_enabled"] = True
```

### 2.3 关键结论

**`_enable_object_reconstruction` 只是 `lineage_pinning_enabled` 的语法糖。** 由于 `lineage_pinning_enabled` 的 C++ 默认值已经是 `true`，即使不设 `_enable_object_reconstruction=True`，reconstruction 也会工作。

验证方式：在集群上查看 `ray start` 的 `system-config`，如果没有 `lineage_pinning_enabled` 字段，则使用 C++ 默认值 `true`：

```bash
cat /proc/1/cmdline | tr '\0' ' ' | grep -oE 'system-config=\{[^}]*\}'
# 如果输出中没有 lineage_pinning_enabled，说明用的是默认值 true
```

### 2.4 `max_lineage_bytes`

- **定义位置**：`src/ray/common/ray_config_def.h:170`
- **默认值**：`1GB`（`1024 * 1024 * 1024`）
- **作用**：限制保留的 lineage 总大小，超过后触发淘汰

```cpp
RAY_CONFIG(int64_t, max_lineage_bytes, 1024 * 1024 * 1024)
```

### 2.5 日志级别配置

- **环境变量**：`RAY_BACKEND_LOG_LEVEL`
- **可选值**：`trace`, `debug`, `info`, `warning`, `error`, `fatal`
- **配置方式**：仅支持环境变量，不支持 `system_config`

```cpp
// src/ray/util/logging.cc:284
void RayLog::InitSeverityThreshold(RayLogLevel severity_threshold) {
  const char *var_value = std::getenv("RAY_BACKEND_LOG_LEVEL");
  if (var_value != nullptr) {
    // ... 解析并覆盖 severity_threshold
  }
  severity_threshold_ = severity_threshold;
}
```

启动时设置：
```bash
RAY_BACKEND_LOG_LEVEL=debug ray start --head ...
```

## 3. Reconstruction 触发流程

### 3.1 Object 丢失检测

当节点死亡或 object 被 evict 时，plasma store 通知 raylet，raylet 通知 core_worker：

```
plasma store → object deleted callback → raylet::HandleObjectMissing
    → core_worker::NotifyObjectEvicted
    → reference_counter::DeleteObjectLocations
    → 触发 recovery
```

### 3.2 Recovery 入口

Recovery 由 **object owner** 的 core_worker 上的 `ObjectRecoveryManager` 发起：

```cpp
// src/ray/core_worker/object_recovery_manager.cc:54
void ObjectRecoveryManager::PinOrReconstructObject(
    const ObjectID &object_id,
    std::vector<rpc::Address> locations) {
  // 1. 先尝试 pin 现有的副本
  if (!locations.empty()) {
    const auto location = std::move(locations.back());
    locations.pop_back();
    PinExistingObjectCopy(object_id, location, std::move(locations));
  } else {
    // 2. 没有副本，走 reconstruction
    ReconstructObject(object_id);
  }
}
```

Recovery 有两条路径：
- **Pin 副本**：如果 object 有 secondary copy（通过 replication 机制创建），直接 pin 一份副本提升为 primary
- **Reconstruction**：没有副本，通过重新执行产生该 object 的 task 来恢复

### 3.3 Reconstruction 核心逻辑

```cpp
// src/ray/core_worker/object_recovery_manager.cc:108
void ObjectRecoveryManager::ReconstructObject(const ObjectID &object_id) {
  // Step 1: 检查 lineage reconstruction eligibility
  LineageReconstructionEligibility eligibility =
      reference_counter_.GetLineageReconstructionEligibility(object_id);

  if (eligibility != LineageReconstructionEligibility::ELIGIBLE) {
    auto error_type_opt = ToErrorType(eligibility);
    rpc::ErrorType error_type = error_type_opt.value_or(rpc::ErrorType::OBJECT_LOST);
    RAY_LOG(INFO).WithField(object_id)
        << "Cannot recover object: " << rpc::ErrorType_Name(error_type);
    recovery_failure_callback_(object_id, error_type, /*pin_object=*/true);
    return;
  }

  // Step 2: 重置 pending_creation 标志（必须在 ResubmitTask 之前设置）
  reference_counter_.UpdateObjectPendingCreation(object_id, true);

  // Step 3: 重新提交产生该 object 的 task
  const auto task_id = object_id.TaskId();
  std::vector<ObjectID> task_deps;
  auto error_type_optional = task_manager_.ResubmitTask(task_id, &task_deps);

  if (!error_type_optional.has_value()) {
    // Step 4: 递归恢复 task 的依赖
    for (const auto &dep : task_deps) {
      auto error = RecoverObject(dep);
      if (error.has_value()) {
        RAY_LOG(INFO).WithField(dep)
            << "Cannot recover dependency: " << rpc::ErrorType_Name(*error);
      }
    }
  }
}
```

### 3.4 Eligibility 检查

```cpp
// src/ray/core_worker/reference_counter.cc:1650
LineageReconstructionEligibility
ReferenceCounter::GetLineageReconstructionEligibility(
    const ObjectID &object_id) const {
  if (!lineage_pinning_enabled_) {
    return LineageReconstructionEligibility::INELIGIBLE_LINEAGE_DISABLED;
  }
  absl::MutexLock lock(&mutex_);
  auto it = object_id_refs_.find(object_id);
  if (it == object_id_refs_.end()) {
    return LineageReconstructionEligibility::INELIGIBLE_REF_NOT_FOUND;
  }
  return it->second.lineage_eligibility_;
}
```

`lineage_eligibility_` 的初始值在 task 创建时设置：

```cpp
// src/ray/core_worker/task_manager.cc:278
if (max_retries == 0) {
  lineage_eligibility = LineageReconstructionEligibility::INELIGIBLE_NO_RETRIES;
} else {
  lineage_eligibility = LineageReconstructionEligibility::ELIGIBLE;
}
```

- `max_retries=0` → `INELIGIBLE_NO_RETRIES`（不允许重建）
- `max_retries>0` 或 `max_retries=-1`（无限重试）→ `ELIGIBLE`

### 3.5 ResubmitTask

```cpp
// src/ray/core_worker/task_manager.cc:354
std::optional<rpc::ErrorType> TaskManager::ResubmitTask(
    const TaskID &task_id, std::vector<ObjectID> *task_deps) {
  TaskSpecification spec;
  {
    absl::MutexLock lock(&mu_);
    auto it = submissible_tasks_.find(task_id);
    // 路径 A: task spec 不存在（已被清除）
    if (it == submissible_tasks_.end()) {
      return rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED;
    }

    auto &task_entry = it->second;
    if (task_entry.is_canceled_) {
      return rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE_TASK_CANCELLED;
    }

    // 路径 B: streaming generator 且重试次数用完
    if (task_entry.spec_.IsStreamingGenerator() &&
        task_entry.GetStatus() == rpc::TaskStatus::SUBMITTED_TO_WORKER) {
      if (task_entry.num_retries_left_ == 0) {
        return rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED;
      }
    } else if (task_entry.GetStatus() != rpc::TaskStatus::FINISHED &&
               task_entry.GetStatus() != rpc::TaskStatus::FAILED) {
      // 路径 C: task 已在运行中，不需要重新提交
      return std::nullopt;
    } else {
      // 路径 D: 正常重新提交
      SetupTaskEntryForResubmit(task_entry);
    }
    spec = task_entry.spec_;
  }

  // 收集 task 依赖
  UpdateReferencesForResubmit(spec, task_deps);

  // 异步重新提交 task
  async_retry_task_callback_(spec, /*delay_ms=*/0);
  return std::nullopt;
}
```

#### `OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED` 的触发条件

| 触发路径 | 条件 | 无限重试是否触发 |
|---------|------|:---:|
| 路径 A | task spec 从 `submissible_tasks_` 中被清除 | **是**（return object 全部 out of scope 时） |
| 路径 B | streaming generator 且 `num_retries_left_ == 0` | 否（无限重试保持 -1） |
| 路径 C | task 正在运行 | 否（返回 nullopt，不报错） |
| 路径 D | 正常重提交 | 否 |

**无限重试（`max_retries=-1`）仍然可能触发 `MAX_ATTEMPTS_EXCEEDED`**——当 return object 全部 out of scope 时，task spec 被清除。

## 4. 递归重建

### 4.1 依赖链重建

当 C 依赖 B、B 依赖 A，且 C 的 object 丢失时，重建过程是递归的：

```
C object 丢失
  → ReconstructObject(C)
  → ResubmitTask(C_task)
  → 发现 C 依赖 B → RecoverObject(B)
    → B 也丢失 → ReconstructObject(B)
    → ResubmitTask(B_task)
    → 发现 B 依赖 A → RecoverObject(A)
      → A 还在 plasma → pin A → B 可以重新执行
      → A 也丢失 → ReconstructObject(A)
        → 如果 A 不可重建 → 报错 → B 失败 → C 失败
```

### 4.2 代码路径

```cpp
// Step 3 中 ResubmitTask 成功后，递归恢复依赖
for (const auto &dep : task_deps) {
  auto error = RecoverObject(dep);  // 递归调用 PinOrReconstructObject
  if (error.has_value()) {
    // 依赖无法恢复，当前 object 也会失败
  }
}
```

`task_deps` 在 `UpdateReferencesForResubmit` 中从 task spec 的参数中提取：

```cpp
// src/ray/core_worker/task_manager.cc:438
void TaskManager::UpdateReferencesForResubmit(const TaskSpecification &spec,
                                              std::vector<ObjectID> *task_deps) {
  for (size_t i = 0; i < spec.NumArgs(); i++) {
    if (spec.ArgByRef(i)) {
      task_deps->emplace_back(spec.ArgObjectId(i));
    }
  }
}
```

只有 `ArgByRef` 的参数（即存储在 plasma 中的大对象引用）才会被递归恢复。内联参数不需要恢复。

## 5. Task Spec 生命周期与 Lineage 管理

### 5.1 `submissible_tasks_`（TaskManager）

存储 task spec（任务定义：函数、参数、依赖等）。每个 task entry 包含：

```cpp
// src/ray/core_worker/task_manager.h
struct TaskEntry {
  TaskSpecification spec_;
  int32_t num_retries_left_;      // -1 表示无限重试
  size_t num_successful_executions_;
  absl::flat_hash_set<ObjectID> reconstructable_return_ids_;
  int64_t lineage_footprint_bytes_;
  // ...
};
```

#### Task 完成时的处理

```cpp
// src/ray/core_worker/task_manager.cc:1059
bool task_retryable = it->second.num_retries_left_ != 0 &&
                      !it->second.reconstructable_return_ids_.empty();
if (task_retryable) {
  // 保留 spec，计入 lineage 大小
  release_lineage = false;
  it->second.lineage_footprint_bytes_ = it->second.spec_.GetMessage().ByteSizeLong();
  total_lineage_footprint_bytes_ += it->second.lineage_footprint_bytes_;
  // 检查是否超过 max_lineage_bytes
  if (total_lineage_footprint_bytes_ > max_lineage_bytes_) {
    min_lineage_bytes_to_evict = total_lineage_footprint_bytes_ - (max_lineage_bytes_ / 2);
  }
} else {
  // 清除 spec
  submissible_tasks_.erase(it);
}
```

#### `task_retryable` 的判断条件

| 条件 | 说明 |
|------|------|
| `num_retries_left_ != 0` | 还有重试次数（无限重试为 -1，永远满足） |
| `!reconstructable_return_ids_.empty()` | 至少有一个 return object 仍在 scope 且存储在 plasma |

**当 return object 全部 out of scope 时，`reconstructable_return_ids_` 变空，task spec 被清除。** 此时即使开了无限重试，后续 reconstruction 会因找不到 spec 而报 `MAX_ATTEMPTS_EXCEEDED`。

#### `reconstructable_return_ids_` 的更新

```cpp
// task_manager.cc:1459 — object out of scope 时移除
it->second.reconstructable_return_ids_.erase(object_id);

// task_manager.cc:1464 — 全部 out of scope 且 task 不在 pending
if (it->second.reconstructable_return_ids_.empty() && !it->second.IsPending()) {
  // 递减依赖的 lineage ref count
  for (size_t i = 0; i < it->second.spec_.NumArgs(); i++) {
    if (it->second.spec_.ArgByRef(i)) {
      released_objects->push_back(it->second.spec_.ArgObjectId(i));
    }
  }
  submissible_tasks_.erase(it);  // 清除 spec
}
```

### 5.2 `reference_counter` 中的 Lineage

`reference_counter` 维护两个与 lineage 相关的数据结构：

#### `object_id_refs_`

每个 object 的引用计数和 lineage 信息：

```cpp
// src/ray/core_worker/reference_counter.h
struct Reference {
  rpc::Address owner_address_;
  std::optional<NodeID> pinned_at_node_id_;
  bool owned_by_us_;
  LineageReconstructionEligibility lineage_eligibility_;
  size_t lineage_ref_count_;  // 依赖此 object 的可重试 task 数量
  bool pending_creation_;
  // ...
};
```

#### `reconstructable_owned_objects_`

一个 FIFO 队列，记录所有可重建的 owned object，用于 `EvictLineage` 时按顺序淘汰：

```cpp
// src/ray/core_worker/reference_counter.h
std::deque<ObjectID> reconstructable_owned_objects_;
absl::flat_hash_map<ObjectID, std::deque<ObjectID>::iterator>
    reconstructable_owned_objects_index_;
```

### 5.3 `submissible_tasks_` 和 Lineage 的关系

两者是同一个 lineage 机制的两个层面：

```
submissible_tasks_ (TaskManager)          reference_counter lineage
┌─────────────────────────┐              ┌─────────────────────────┐
│ Task A spec              │◄─────────────│ Object A → [no deps]     │
│ Task B spec              │◄─────────────│ Object B → [Object A]    │
│ Task C spec              │◄─────────────│ Object C → [Object B]    │
└─────────────────────────┘              └─────────────────────────┘
      │                                          │
      │ total_lineage_footprint_bytes_           │ reconstructable_owned_objects_
      │ 累加 spec 大小                           │ 按顺序排列，可被 EvictLineage 淘汰
      ▼                                          ▼
   > 1GB 时触发 EvictLineage → 淘汰 reference_counter 中的 lineage reference
   → lineage_eligibility_ 改为 INELIGIBLE_LINEAGE_EVICTED
   → 但 submissible_tasks_ 中的 spec 不一定被清除（内存泄漏风险）
```

**关键问题**：两个模块独立淘汰，没打通：

1. **`TaskManager`** 管 task spec 的内存（`total_lineage_footprint_bytes_ > max_lineage_bytes_`），但淘汰时调的是 `reference_counter_.EvictLineage()`，淘汰 reference_counter 中的 lineage reference，**不淘汰自己的 spec**
2. **`ReferenceCounter`** 管 object 引用计数，object 全部 out of scope 时清除 reference，但**不通知 TaskManager 清除 spec**

结果是：lineage 被淘汰了，`lineage_eligibility_` 标记为 `INELIGIBLE_LINEAGE_EVICTED`，但 spec 还留在 `submissible_tasks_` 里白占内存，永远不会再被用到。

代码中也有注释提到这个 leak 风险：

```cpp
// task_manager.cc:1018
// TODO: It is possible that the dynamically returned refs
// have already been consumed by the caller and deleted. This can
// cause a memory leak of the task metadata, because we will
// never receive a callback from the ReferenceCounter to erase
// the task.
```

### 5.4 一个 Task 对应多个 Object

一个 task 可以有多个 return object，它们在 `reference_counter` 中是独立的 entry，但都指向同一个 task spec：

```
submissible_tasks_[task_id] = TaskSpec{...}

reference_counter:
  object_id_refs_[return_id_0] → Reference{ task_id, lineage_eligibility: ELIGIBLE }
  object_id_refs_[return_id_1] → Reference{ task_id, lineage_eligibility: ELIGIBLE }
  object_id_refs_[return_id_2] → Reference{ task_id, lineage_eligibility: ELIGIBLE }
```

淘汰时可能淘汰了 `return_id_0` 的 lineage reference，但 `return_id_1` 还是 ELIGIBLE。这时：
- `return_id_1` 丢失 → 重建成功（spec 还在）
- `return_id_0` 丢失 → `INELIGIBLE_LINEAGE_EVICTED` → 重建失败

但 spec 不会因为 `return_id_0` 的 lineage 被淘汰而从 `submissible_tasks_` 清除——只要 `return_id_1` 还在 scope（`reconstructable_return_ids_` 不为空），spec 就保留着。

## 6. Lineage 淘汰机制

### 6.1 触发条件

当 task 完成后 `total_lineage_footprint_bytes_` 超过 `max_lineage_bytes_`（默认 1GB）：

```cpp
// task_manager.cc:1066
if (total_lineage_footprint_bytes_ > max_lineage_bytes_) {
  RAY_LOG(INFO) << "Total lineage size is " << total_lineage_footprint_bytes_ / 1e6
                << "MB, which exceeds the limit of " << max_lineage_bytes_ / 1e6
                << "MB";
  min_lineage_bytes_to_evict = total_lineage_footprint_bytes_ - (max_lineage_bytes_ / 2);
}
```

淘汰目标是将 lineage 总大小降到 `max_lineage_bytes_ / 2`（512MB）。

### 6.2 淘汰过程

```cpp
// task_manager.cc:1128
if (min_lineage_bytes_to_evict > 0) {
  auto bytes_evicted = reference_counter_.EvictLineage(min_lineage_bytes_to_evict);
  RAY_LOG(INFO) << "Evicted " << bytes_evicted / 1e6 << "MB of task lineage.";
}
```

```cpp
// reference_counter.cc:823
int64_t ReferenceCounter::EvictLineage(int64_t min_bytes_to_evict) {
  absl::MutexLock lock(&mutex_);
  int64_t lineage_bytes_evicted = 0;
  while (!reconstructable_owned_objects_.empty() &&
         lineage_bytes_evicted < min_bytes_to_evict) {
    ObjectID object_id = std::move(reconstructable_owned_objects_.front());
    reconstructable_owned_objects_.pop_front();
    reconstructable_owned_objects_index_.erase(object_id);

    auto it = object_id_refs_.find(object_id);
    RAY_CHECK(it != object_id_refs_.end());
    lineage_bytes_evicted += ReleaseLineageReferences(it);
  }
  return lineage_bytes_evicted;
}
```

### 6.3 ReleaseLineageReferences

```cpp
// reference_counter.cc:569
int64_t ReferenceCounter::ReleaseLineageReferences(ReferenceTable::iterator ref) {
  int64_t lineage_bytes_evicted = 0;
  std::vector<ObjectID> argument_ids;
  if (on_lineage_released_ && ref->second.owned_by_us_) {
    lineage_bytes_evicted += on_lineage_released_(ref->first, &argument_ids);
    // 标记 lineage 已淘汰
    if (!ref->second.OutOfScope(lineage_pinning_enabled_) &&
        ref->second.lineage_eligibility_ == LineageReconstructionEligibility::ELIGIBLE) {
      ref->second.lineage_eligibility_ =
          LineageReconstructionEligibility::INELIGIBLE_LINEAGE_EVICTED;
    }
  }
  // 递归释放依赖的 lineage
  for (const auto &argument_id : argument_ids) {
    auto arg_it = object_id_refs_.find(argument_id);
    if (arg_it != object_id_refs_.end()) {
      arg_it->second.lineage_ref_count_--;
      if (arg_it->second.OutOfScope(lineage_pinning_enabled_)) {
        OnObjectOutOfScopeOrFreed(arg_it);
      }
      if (arg_it->second.ShouldDelete(lineage_pinning_enabled_)) {
        lineage_bytes_evicted += ReleaseLineageReferences(arg_it);
        EraseReference(arg_it);
      }
    }
  }
  return lineage_bytes_evicted;
}
```

淘汰后 `lineage_eligibility_` 从 `ELIGIBLE` 变为 `INELIGIBLE_LINEAGE_EVICTED`，后续重建时 `GetLineageReconstructionEligibility` 返回 `INELIGIBLE_LINEAGE_EVICTED`，报 `OBJECT_UNRECONSTRUCTABLE_LINEAGE_EVICTED` 错误。

## 7. 重建后的 Object 是否会走副本机制

**会。** Reconstruction 重建的 object 会重新触发副本推送。

完整链路：

```
Object 丢失 → Reconstruction → Task 重新执行 → Object 重新创建到 plasma
  → plasma add_object_callback(source=CreatedByWorker)
  → NodeManager::HandleObjectLocal(object_info, source)
  → MaybeReplicateObject(object_info, source)
  → source == CreatedByWorker → 如果是 tidal 节点 → 触发 replication
```

但 pin 副本恢复的 object 不会走 replication：

```
Object 丢失 → Pin 副本 → Object 从副本复制到 plasma
  → plasma add_object_callback(source=ReceivedFromRemoteRaylet)
  → NodeManager::HandleObjectLocal(object_info, source)
  → MaybeReplicateObject(object_info, source)
  → source == ReceivedFromRemoteRaylet → 跳过（不再重复复制）
```

```cpp
// node_manager.cc:3692
void NodeManager::MaybeReplicateObject(const ObjectInfo &object_info,
                                       plasma::flatbuf::ObjectSource source) {
  if (!RayConfig::instance().enable_object_replication()) {
    return;
  }
  // 副本来的 object 不再复制
  if (source == plasma::flatbuf::ObjectSource::ReceivedFromRemoteRaylet) {
    return;
  }
  if (!is_tidal_node_) {
    return;
  }
  if (object_info.data_size < RayConfig::instance().object_replication_min_bytes()) {
    object_replication_skipped_.Record(1, {{"Reason", "too_small"}});
    return;
  }
  // ... 触发 replication
}
```

这意味着 reconstruction 重建后不仅恢复了 object，还重新触发了副本推送，后续如果再丢失就可以直接 pin 副本，不需要再重算。

## 8. Reconstruction 失败场景总结

| 失败场景 | 错误类型 | 无限重试是否可能触发 |
|---------|---------|:---:|
| Task spec 从 `submissible_tasks_` 被清除（return object 全部 out of scope） | `OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED` | **是** |
| Task 被显式取消（`CancelTask` 设 `num_retries_left_=0`） | `OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED` | 是 |
| Lineage 内存超限被淘汰 | `OBJECT_UNRECONSTRUCTABLE_LINEAGE_EVICTED` | **是** |
| `lineage_pinning_enabled=false` | `INELIGIBLE_LINEAGE_DISABLED` | 否（C++ 默认 true） |
| `max_retries=0` | `INELIGIBLE_NO_RETRIES` | 否 |
| Object ref 不在 `object_id_refs_` 中 | `INELIGIBLE_REF_NOT_FOUND` | 是 |
| 依赖的 object 也不可重建 | 依赖的错误类型向上传播 | 是 |

### 无限重试为何仍可能失败

即使 task 开了无限重试（`max_retries=-1`），`num_retries_left_` 永远保持 `-1`，不会递减到 0。但以下情况仍会导致重建失败：

1. **Return object 全部 out of scope** → `reconstructable_return_ids_` 变空 → task spec 从 `submissible_tasks_` 清除 → `MAX_ATTEMPTS_EXCEEDED`
2. **Lineage 内存淘汰** → `lineage_eligibility_` 变为 `INELIGIBLE_LINEAGE_EVICTED` → `LINEAGE_EVICTED`
3. **依赖链中的上游 object 不可重建** → 错误向上传播

## 9. `lineage_eligibility_` 与 `submissible_tasks_` 不一致 Bug

### 9.1 问题描述

当 `CompletePendingTask` 判断 `task_retryable = false` 时，直接 `submissible_tasks_.erase(it)` 删除 task spec，但**没有**通知 `reference_counter` 把对应 object 的 `lineage_eligibility_` 降级为 `INELIGIBLE_LINEAGE_EVICTED`。导致后续 `ReconstructObject` 时 `GetLineageReconstructionEligibility` 返回 `ELIGIBLE`，但 `ResubmitTask` 找不到 task spec，返回 `MAX_ATTEMPTS_EXCEEDED`。

### 9.2 `lineage_eligibility_` 被降级的唯一路径

`lineage_eligibility_` 只在 `ReleaseLineageReferences`（`reference_counter.cc:569-582`）中被降级：

```cpp
int64_t ReferenceCounter::ReleaseLineageReferences(ReferenceTable::iterator ref) {
  if (on_lineage_released_ && ref->second.owned_by_us_) {
    lineage_bytes_evicted += on_lineage_released_(ref->first, &argument_ids);
    // 核心条件：只有 OutOfScope() == false 时才降级
    if (!ref->second.OutOfScope(lineage_pinning_enabled_) &&
        ref->second.lineage_eligibility_ == ELIGIBLE) {
      ref->second.lineage_eligibility_ = INELIGIBLE_LINEAGE_EVICTED;
    }
  }
}
```

`ReleaseLineageReferences` 有两条调用路径：

| 调用路径 | `OutOfScope()` 状态 | 是否降级 `lineage_eligibility_` |
|---------|---------------------|:---:|
| `EvictLineage` → `ReleaseLineageReferences` | 通常为 `false`（object 还 in scope） | **是** |
| `DeleteReferenceInternal` → `ShouldDelete` → `ReleaseLineageReferences` | 为 `true`（ref count 已归零） | **否**（合理，因为已没人需要） |

### 9.3 `CompletePendingTask` 删除 task spec 的路径

`task_manager.cc:1059-1070`：

```cpp
bool task_retryable = it->second.num_retries_left_ != 0 &&
                      !it->second.reconstructable_return_ids_.empty();
if (task_retryable) {
    release_lineage = false;  // 保留 spec
} else {
    submissible_tasks_.erase(it);  // 直接删除 spec，不调 ReleaseLineageReferences
}
```

**`submissible_tasks_.erase(it)` 不会触发 `ReleaseLineageReferences`**，所以 `lineage_eligibility_` 保持 `ELIGIBLE` 不变。

### 9.4 具体触发场景：Streaming Generator 重试完成

对于 streaming generator task（Ray Data 的 `cached_remote_fn` 默认 `max_retries=-1`）：

1. **第一次完成**（`first_execution = true`）→ `reconstructable_return_ids_` 填入所有 return → `task_retryable = true` → 保留 spec
2. **Consumer 逐个消费 return object 并释放 ref** → `RemoveLineageReference` → `reconstructable_return_ids_` 逐个 erase
3. **当 `reconstructable_return_ids_` 变空 且 `!IsPending()`** → `submissible_tasks_.erase(it)` → task spec 被删除
4. **但此时 `object_id_refs_` 中的 `lineage_eligibility_` 仍然是 `ELIGIBLE`**——因为 `ReleaseLineageReferences` 检查 `!OutOfScope()` 时，object 已经 out of scope（`RefCount() == 0`），所以不会降级
5. **后续如果某个 consumer（如 actor task）仍然持有该 block 的 ref**（`RefCount() > 0`），该 object 的 `lineage_eligibility_` 仍然是 `ELIGIBLE`
6. **Tidal 节点死亡 → object 在 plasma 中丢失** → `RecoverObject()` → `GetLineageReconstructionEligibility()` 返回 `ELIGIBLE` → 进入 `ReconstructObject()`
7. **`ResubmitTask()` → `submissible_tasks_` 中找不到 task** → 返回 `MAX_ATTEMPTS_EXCEEDED`

**核心矛盾**：`reference_counter` 认为 lineage 还在（`ELIGIBLE`），但 `task_manager` 已经把 task spec 删了。Ray 用 `MAX_ATTEMPTS_EXCEEDED` 作为 "task spec 找不到" 的通用错误码，对这个场景是**误导的**——实际原因是 task spec 因 return object 全部 out of scope 而被清理，而非 retries 耗尽。

### 9.5 影响范围

| 场景 | 是否触发此 Bug |
|------|:---:|
| 普通 task 完成，所有 return object 仍在 scope | 否（`task_retryable = true`，spec 保留） |
| 普通 task 完成，所有 return object out of scope | 否（此时 `reconstructable_return_ids_` 空 → spec 删，但 object ref 也删了，不会触发 recovery） |
| Streaming generator，部分 return 仍被 consumer 持有，部分已释放 | **可能触发**（`reconstructable_return_ids_` 可能因其他 return 释放而清空，但某些 return 的 ref 还在） |
| Streaming generator 重试完成（`first_execution = false`），上次的所有 return 已 out of scope | **最可能触发**（`reconstructable_return_ids_` 在 CompletePendingTask 时为空 → spec 删 → lineage_eligibility_ 仍 ELIGIBLE） |

### 9.6 修复建议

在 `CompletePendingTask` 中 `submissible_tasks_.erase(it)` 之前，应通知 `reference_counter` 把所有 return object 的 `lineage_eligibility_` 降级为 `INELIGIBLE_LINEAGE_EVICTED`（或引入新的 `INELIGIBLE_SPEC_DELETED` 枚举值），以确保 `ReconstructObject` 能返回准确的错误类型。

---

## 10. Ray Data 场景下的完整错误传播链路

### 10.1 场景描述

Ray Data Pipeline：`ReadArrowJSON → SplitBlocks → StreamingRepartition → QGPreprocessMapper(4000 CPU actors) → QGInferMapper(600 GPU actors) → Filter → Write`

当 StreamingRepartition 产出的某个 block object 丢失且无法重建时，错误会层层传播：

### 10.2 三层错误传播链路

```
┌─────────────────────────────────────────────────────────────────┐
│ 第 1 层：StreamingRepartition 的某个 output object 丢失          │
│                                                                  │
│ Tidal 节点死亡 → plasma 中的 object 消失                         │
│ → ObjectRecoveryManager::RecoverObject(object_id)               │
│ → PinOrReconstructObject()                                      │
│   → PinExistingObjectCopy() 失败（所有 location 都死了）         │
│   → ReconstructObject()                                          │
│     → GetLineageReconstructionEligibility() → ELIGIBLE          │
│     → ResubmitTask() → submissible_tasks_ 找不到 task            │
│     → 返回 MAX_ATTEMPTS_EXCEEDED                                │
│     → recovery_failure_callback_(object_id, error_type, true)  │
│       → CoreWorker::Put(RayObject(OBJECT_RECONSTRUCTION_FAILED))│
│         → 写入 plasma + memory_store_ 标记 OBJECT_IN_PLASMA     │
│                                                                  │
│ 此时：该 object_id 在 plasma 中的值 = ErrorType 对象             │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│ 第 2 层：下游 PreprocessMapper actor task 的依赖解析失败          │
│                                                                  │
│ PreprocessMapper.submit() 提交 actor task                        │
│ → DependencyResolver 解析参数                                    │
│   → ray.get(input_object_ref) 拿到的值 = RayError 对象           │
│   → _raylet.pyx:1839: raise_if_dependency_failed(arg)          │
│     → 抛出 ObjectReconstructionFailedError                      │
│                                                                  │
│ 错误堆栈格式化（exceptions.py:318-323）：                        │
│   ray._raylet.raise_if_dependency_failed                         │
│   → 替换为 "At least one of the input arguments for              │
│      this task could not be computed:"                           │
│   → 后面附上原始异常 ObjectReconstructionFailedError             │
│                                                                  │
│ ActorTaskSubmitter 检测到依赖失败：                              │
│ → FailOrRetryPendingTask(task_id, DEPENDENCY_RESOLUTION_FAILED)  │
│   → RetryTaskIfPossible()                                        │
│     → max_retries=-1: will_retry = true → num_retries_left 不变  │
│   → 但如果依赖 object 的 reconstruction 永久失败，重试也拿不到值│
│   → 最终 FailPendingTask()                                       │
│     → MarkTaskReturnObjectsFailed(spec, error_type)              │
│     → 把该 actor task 的 return objects 也标记为 ErrorType       │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│ 第 3 层：Ray Data streaming executor 看到 task 失败              │
│                                                                  │
│ DataOpTask.on_data_ready()                                       │
│   → ray.get(block_ref) 拿到 RayTaskError                         │
│   → process_completed_tasks() 捕获异常                           │
│   → num_errored_blocks++                                         │
│   → max_errored_blocks=0 → 直接 abort 整个 pipeline             │
│                                                                  │
│ 错误层层包装显示：                                                │
│   RayTaskError(ObjectReconstructionFailedError):                 │
│     MapWorker(QGPreprocessMapper).submit()                       │
│       At least one of the input arguments...                     │
│       RayTaskError: StreamingRepartition[...]                    │
│         At least one of the input arguments...                   │
│         ObjectReconstructionFailedError: Failed to retrieve...   │
│         [OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED]         │
└─────────────────────────────────────────────────────────────────┘
```

### 10.3 Ray Data 不重试 Operator Task

Ray Data 的 streaming executor 对 `ObjectReconstructionFailedError` **没有特殊重试逻辑**：

```python
# streaming_executor_state.py:481-509
except Exception as e:
    errored_blocks_per_op[state] += 1
    num_errored_blocks += 1
    should_ignore = max_errored_blocks < 0 or max_errored_blocks >= num_errored_blocks
    if should_ignore:
        logger.error(error_message, exc_info=e)
    else:
        raise e from None  # 默认 max_errored_blocks=0，直接 abort
```

`cached_remote_fn`（`remote_fn.py:37`）设置 `max_retries=-1`，但这是 **Ray Core 层的 task 重试**（worker 死亡后自动重提交），不是 **Ray Data 层的 operator task 重试**。Ray Data 不会重新提交失败的 operator task。

### 10.4 Ray Core 层的 Task 重试 vs Ray Data 层的 Operator Task

| 层次 | 机制 | 触发条件 | 重试行为 |
|------|------|---------|---------|
| Ray Core | `max_retries=-1` | Worker 进程死亡 / 节点死亡 / 系统错误 | 无限重试 task 执行 |
| Ray Core | Lineage Reconstruction | Object 丢失 | 重新执行产生该 object 的 task |
| Ray Data | `max_errored_blocks` | `ray.get()` 抛出任何异常 | 默认 0 = 直接 abort，不重试 |
| Ray Data | Actor Pool 重启 | Actor 健康检查失败 | 重建 actor，重新提交 task |

### 10.5 `raise_if_dependency_failed` 的代码路径

```python
# _raylet.pyx:891
cdef raise_if_dependency_failed(arg):
    if isinstance(arg, RayError):
        raise arg  # 直接抛出原始异常

# _raylet.pyx:1839-1840
for arg in args:
    raise_if_dependency_failed(arg)  # 在参数反序列化后逐个检查
```

`exceptions.py:310-325` 在格式化 traceback 时，将 `raise_if_dependency_failed` 替换为友好的提示信息 "At least one of the input arguments for this task could not be computed:"。

### 10.6 `recovery_failure_callback_` 的写入路径

```cpp
// core_worker_process.cc:660-665
[this](const ObjectID &object_id, rpc::ErrorType reason, bool pin_object) {
    auto core_worker = GetCoreWorker();
    RAY_UNUSED(core_worker->Put(RayObject(reason), {}, object_id, pin_object));
}
```

`CoreWorker::Put(RayObject(error_type), ...)` → `PutInLocalPlasmaStore()` → 写入 plasma + `memory_store_->Put(OBJECT_IN_PLASMA)`。后续 `ray.get()` 拿到这个 ErrorType 对象 → Python 层反序列化为对应的异常类。

### 10.7 关键代码文件

| 文件 | 说明 |
|------|------|
| `python/ray/_raylet.pyx:891` | `raise_if_dependency_failed` — 依赖失败时抛出异常 |
| `python/ray/_raylet.pyx:1839` | Worker 执行 task 时逐个检查参数是否为 RayError |
| `python/ray/exceptions.py:310-325` | Traceback 格式化：替换 `raise_if_dependency_failed` |
| `python/ray/exceptions.py:786-810` | `ObjectReconstructionFailedError.REASON_MESSAGES` |
| `python/ray/data/_internal/remote_fn.py:37` | `cached_remote_fn` 默认 `max_retries=-1` |
| `python/ray/data/_internal/execution/streaming_executor_state.py:481-509` | `process_completed_tasks` 错误处理 |
| `src/ray/core_worker/task_submission/actor_task_submitter.cc:230-238` | 依赖解析失败 → `FailOrRetryPendingTask` |
| `src/ray/core_worker/task_manager.cc:1258-1344` | `FailPendingTask` → `MarkTaskReturnObjectsFailed` |
| `src/ray/core_worker/task_manager.cc:1556-1600` | `MarkTaskReturnObjectsFailed` — 把 return objects 标记为 ErrorType |
| `src/ray/core_worker/core_worker_process.cc:660-665` | `recovery_failure_callback_` — 写入 ErrorType 到 plasma |

---

## 11. Prometheus 指标

### 9.1 Recovery 指标（core_worker 层）

| 指标名 | 说明 |
|--------|------|
| `ray_object_recovery_attempted` | object 丢失后发起 recovery 的总次数 |
| `ray_object_recovery_skipped` | recovery 被跳过的次数（by Reason） |
| `ray_object_recovery_pin_succeeded` | 通过 pin 副本成功恢复的次数 |
| `ray_object_recovery_pin_failed` | pin 副本失败的次数 |
| `ray_object_reconstruction_attempted` | 走到 lineage reconstruction 的次数 |
| `ray_object_reconstruction_succeeded` | reconstruction 成功的次数 |
| `ray_object_reconstruction_failed` | reconstruction 失败的次数（by Reason） |

### 9.2 Object 数量指标

| 指标名 | 说明 |
|--------|------|
| `ray_object_store_num_local_objects` | 当前节点 plasma 中的 object 数量（Gauge） |
| `ray_object_store_dist_count` | 累计创建的 object 数量，按 Source 分组（CreatedByWorker / ReceivedFromRemoteRaylet） |

**`CreatedByWorker`** 表示该节点上 task 执行后创建的 object，是真正"计算产生"的 object 数量。`ReceivedFromRemoteRaylet` 是通过 pull 或 replication push 接收的 object，不是新创建的。

查询总量：
```promql
sum(ray_object_store_dist_count{Source="CreatedByWorker"})
```

### 9.3 日志级别配置

Reconstruction 相关日志分布在不同级别：

| 日志 | 级别 | 位置 |
|------|------|------|
| "Attempting to recover N lost objects" | INFO | `core_worker.cc:476` |
| "Cannot recover object: ..." | INFO | `object_recovery_manager.cc` |
| "Resubmitting task that produced lost plasma object" | INFO | `task_manager.cc:400` |
| "Object local on node" | DEBUG | `node_manager.cc:2423` |
| "Attempting to reconstruct object" | DEBUG | `object_recovery_manager.cc:130` |
| "Total lineage size exceeds limit" | INFO | `task_manager.cc:1067` |
| "Evicted N MB of task lineage" | INFO | `task_manager.cc:1131` |
| "Node failure. All objects pinned on that node will be lost" | INFO | `core_worker.cc:756` |

通过 `RAY_BACKEND_LOG_LEVEL=debug` 可以看到完整的重建链路日志。

## 12. 关键代码文件索引

| 文件 | 说明 |
|------|------|
| `src/ray/core_worker/object_recovery_manager.cc` | Recovery 主逻辑：PinOrReconstructObject, ReconstructObject |
| `src/ray/core_worker/object_recovery_manager.h` | Recovery manager 声明，包含所有 recovery/reconstruction metric 成员 |
| `src/ray/core_worker/reference_counter.cc` | Lineage eligibility 检查、EvictLineage、ReleaseLineageReferences |
| `src/ray/core_worker/reference_counter.h` | Reference 结构定义，lineage_eligibility_, lineage_ref_count_ |
| `src/ray/core_worker/task_manager.cc` | ResubmitTask, CompletePendingTask, RemoveFinishedTaskReferences |
| `src/ray/core_worker/task_manager.h` | TaskEntry 定义, num_retries_left_, reconstructable_return_ids_ |
| `src/ray/raylet/node_manager.cc` | HandleObjectLocal → MaybeReplicateObject |
| `src/ray/common/ray_config_def.h` | lineage_pinning_enabled, max_lineage_bytes 配置定义 |
| `src/ray/util/logging.cc` | RAY_BACKEND_LOG_LEVEL 环境变量解析 |
| `python/ray/_private/worker.py` | `_enable_object_reconstruction` 参数处理 |
| `python/ray/_private/parameter.py` | `_enable_object_reconstruction` → `lineage_pinning_enabled` 映射 |
| `src/ray/core_worker/metrics.h` | Recovery/Reconstruction metric 定义 |

---

## 13. EvictLineage 深度分析：LINEAGE_EVICTED 与 MAX_ATTEMPTS_EXCEEDED 的分歧

### 13.1 EvictLineage 完整触发链路

当 task 完成后 `total_lineage_footprint_bytes_` 超过 `max_lineage_bytes_`（默认 1GB），触发 EvictLineage：

```cpp
// task_manager.cc:1066-1071
if (total_lineage_footprint_bytes_ > max_lineage_bytes_) {
  RAY_LOG(INFO) << "Total lineage size is " << total_lineage_footprint_bytes_ / 1e6
                << "MB, which exceeds the limit of " << max_lineage_bytes_ / 1e6
                << "MB";
  min_lineage_bytes_to_evict =
      total_lineage_footprint_bytes_ - (max_lineage_bytes_ / 2);
}
```

淘汰目标是将 lineage 总大小降到 `max_lineage_bytes_ / 2`。

```cpp
// task_manager.cc:1129-1132
if (min_lineage_bytes_to_evict > 0) {
  auto bytes_evicted = reference_counter_.EvictLineage(min_lineage_bytes_to_evict);
  RAY_LOG(INFO) << "Evicted " << bytes_evicted / 1e6 << "MB of task lineage.";
}
```

### 13.2 EvictLineage 从 FIFO 队列取队头淘汰

```cpp
// reference_counter.cc:823-839
int64_t ReferenceCounter::EvictLineage(int64_t min_bytes_to_evict) {
  absl::MutexLock lock(&mutex_);
  int64_t lineage_bytes_evicted = 0;
  while (!reconstructable_owned_objects_.empty() &&
         lineage_bytes_evicted < min_bytes_to_evict) {
    ObjectID object_id = std::move(reconstructable_owned_objects_.front());
    reconstructable_owned_objects_.pop_front();
    reconstructable_owned_objects_index_.erase(object_id);

    auto it = object_id_refs_.find(object_id);
    RAY_CHECK(it != object_id_refs_.end());
    lineage_bytes_evicted += ReleaseLineageReferences(it);
  }
  return lineage_bytes_evicted;
}
```

`reconstructable_owned_objects_` 是 FIFO 队列，在 `AddOwnedObjectInternal` 时入队：

```cpp
// reference_counter.cc:388-390
reconstructable_owned_objects_.emplace_back(object_id);
auto back_it = reconstructable_owned_objects_.end();
back_it--;
RAY_CHECK(reconstructable_owned_objects_index_.emplace(object_id, back_it).second);
```

**FIFO 顺序意味着最老的（通常是上游 read 阶段的输出）最先被淘汰。**

### 13.3 ReleaseLineageReferences 的 eligibility 标记逻辑

```cpp
// reference_counter.cc:569-607
int64_t ReferenceCounter::ReleaseLineageReferences(ReferenceTable::iterator ref) {
  int64_t lineage_bytes_evicted = 0;
  std::vector<ObjectID> argument_ids;
  if (on_lineage_released_ && ref->second.owned_by_us_) {
    RAY_LOG(DEBUG) << "Releasing lineage for object " << ref->first;
    lineage_bytes_evicted += on_lineage_released_(ref->first, &argument_ids);
    // ★ 关键标记逻辑
    if (!ref->second.OutOfScope(lineage_pinning_enabled_) &&
        ref->second.lineage_eligibility_ == LineageReconstructionEligibility::ELIGIBLE) {
      ref->second.lineage_eligibility_ =
          LineageReconstructionEligibility::INELIGIBLE_LINEAGE_EVICTED;
    }
  }

  for (const ObjectID &argument_id : argument_ids) {
    auto arg_it = object_id_refs_.find(argument_id);
    if (arg_it == object_id_refs_.end()) {
      continue;
    }
    if (arg_it->second.lineage_ref_count == 0) {
      continue;
    }
    arg_it->second.lineage_ref_count--;
    if (arg_it->second.OutOfScope(lineage_pinning_enabled_)) {
      OnObjectOutOfScopeOrFreed(arg_it);
    }
    if (arg_it->second.ShouldDelete(lineage_pinning_enabled_)) {
      RAY_CHECK(!arg_it->second.publish_ref_removed);
      lineage_bytes_evicted += ReleaseLineageReferences(arg_it);
      EraseReference(arg_it);
    }
  }
  return lineage_bytes_evicted;
}
```

### 13.4 eligibility 标记条件分析

eligibility 标记为 `INELIGIBLE_LINEAGE_EVICTED` 的条件：

```
条件 1: !OutOfScope(lineage_pinning_enabled_)
条件 2: lineage_eligibility_ == ELIGIBLE
```

**关键：`OutOfScope` 的判断逻辑**：

```cpp
// reference_counter.h:349-371
bool OutOfScope(bool lineage_pinning_enabled) const {
  bool in_scope = RefCount() > 0;
  bool is_nested = !nested().contained_in_borrowed_ids.empty();
  bool has_borrowers = !borrow().borrowers.empty();
  bool was_stored_in_objects = !borrow().stored_in_objects.empty();

  bool has_lineage_references = false;
  if (lineage_pinning_enabled && owned_by_us_ &&
      lineage_eligibility_ != LineageReconstructionEligibility::ELIGIBLE) {
    has_lineage_references = lineage_ref_count > 0;
  }

  return !(in_scope || is_nested || has_nested_refs_to_report || has_borrowers ||
           was_stored_in_objects || has_lineage_references);
}
```

```cpp
// reference_counter.h:346-349
size_t RefCount() const {
  return local_ref_count + submitted_task_ref_count +
         nested().contained_in_owned.size();
}
```

**`lineage_ref_count` 不在 `RefCount()` 中**！`RefCount()` 只包含 `local_ref_count` + `submitted_task_ref_count` + `contained_in_owned.size()`。

**`lineage_ref_count` 在 `OutOfScope` 中只在一种情况下被检查**：`lineage_eligibility_ != ELIGIBLE` 时。即**当 eligibility 仍为 ELIGIBLE 时，lineage_ref_count 被 OutOfScope 完全忽略**。

### 13.5 两种 EvictLineage 场景的分歧

| X 的状态 | RefCount() | OutOfScope | 是否标记 eligibility | 后续报错类型 |
|----------|------------|------------|---------------------|-------------|
| **X 还有 Python 引用**（local_ref_count > 0） | > 0 | **false** | ✅ 标记 INELIGIBLE | **LINEAGE_EVICTED** |
| **X 正在被某个 task 作为参数执行**（submitted_task_ref_count > 0） | > 0 | **false** | ✅ 标记 INELIGIBLE | **LINEAGE_EVICTED** |
| **X 被嵌套在另一个 owned object 中**（contained_in_owned.size() > 0） | > 0 | **false** | ✅ 标记 INELIGIBLE | **LINEAGE_EVICTED** |
| **RefBundle 已 del，下游 lineage_ref_count 还在**（全为 0） | = 0 | **true** | ❌ 不标记 | **MAX_ATTEMPTS_EXCEEDED**（bug） |

```
                        EvictLineage(X)
                             │
                             ▼
                    X.RefCount() > 0 ?
                    ┌──────┴──────┐
                    │ Yes         │ No
                    ▼             ▼
             X.OutOfScope()     X.OutOfScope()
             = false            = true
                    │                │
                    ▼                ▼
             标记 INELIGIBLE      不标记（BUG）
             LINEAGE_EVICTED     eligibility 仍 ELIGIBLE
                    │                │
                    ▼                ▼
             后续 X 丢失:          后续 X 丢失:
             eligibility检查       eligibility检查
             → LINEAGE_EVICTED     → ELIGIBLE(通过)
             ✅ 报错正确            → ResubmitTask
                                  → 找不到spec
                                  → MAX_ATTEMPTS_EXCEEDED
                                  ❌ 报错类型错误
```

### 13.6 LINEAGE_EVICTED 触发的完整代码路径

当 X 被 EvictLineage 时 `RefCount() > 0`（例如 `submitted_task_ref_count > 0`，即 B_task 正在执行）：

```
1. EvictLineage(min_bytes)
   → 取队头 X

   X.submitted_task_ref_count = 1  (B_task 还在执行)
   X.RefCount() = 1
   X.OutOfScope() = false

2. ReleaseLineageReferences(X):
   ① on_lineage_released_(X) → RemoveLineageReference(X)
     → A_task.reconstructable_return_ids_.erase(X)
     → {} 为空 → submissible_tasks_.erase(A_task) → A_task spec 删了

   ② 检查标记:
     !X.OutOfScope() = !false = true   ← 条件满足
     X.eligibility == ELIGIBLE          ← 条件满足
     → X.eligibility = INELIGIBLE_LINEAGE_EVICTED ✅

3. 后续 X 丢失:
   ReconstructObject(X)
   → GetLineageReconstructionEligibility(X)      // reference_counter.cc:1650
     → object_id_refs_.find(X) → 找到！Reference 还在
     → return X.eligibility → INELIGIBLE_LINEAGE_EVICTED
   → ToErrorType → LINEAGE_EVICTED                // object_recovery_manager.cc:143
   → recovery_failure_callback_(X, LINEAGE_EVICTED, pin=true)
     → core_worker->Put(RayObject(LINEAGE_EVICTED), {}, X)  // core_worker_process.cc:665
```

### 13.7 MAX_ATTEMPTS_EXCEEDED Bug 的完整代码路径

当 X 被 EvictLineage 时 `RefCount() == 0`（例如 driver 已 del RefBundle，B_task 也已完成）：

```
1. EvictLineage(min_bytes)
   → 取队头 X

   X.local_ref_count = 0
   X.submitted_task_ref_count = 0
   X.lineage_ref_count = 1
   X.RefCount() = 0
   X.OutOfScope() = true (ELIGIBLE 不看 lineage_ref_count)

2. ReleaseLineageReferences(X):
   ① on_lineage_released_(X) → RemoveLineageReference(X)   // task_manager.cc:1443
     → A_task.reconstructable_return_ids_.erase(X)
     → {} 为空 → submissible_tasks_.erase(A_task) → A_task spec 删了！

   ② 检查标记:
     !X.OutOfScope() = !true = false   ← 条件不满足
     → 跳过！eligibility 仍 ELIGIBLE

   ③ X.ShouldDelete()?                                  // reference_counter.h:378-384
     OutOfScope=true, lineage_ref_count=1 → false
     → X.Reference 保留

   X 最终状态:
     A_task spec: 不存在
     X.Reference: 存在
     X.eligibility: ELIGIBLE  ← BUG!
     X.pinned_at: 有值（X 还在 plasma 中）
```

**此时还没有报错**，因为 X 还在 plasma 中。报错发生在 X 丢失后：

```
3. 节点 N 故障，X 的 plasma 副本丢失:
   → ReferenceCounter::ResetObjectsOnRemovedNode(N)  // reference_counter.cc:893
   → X.pinned_at_node_id_ = Nil
   → objects_to_recover_.push_back(X)

4. 100ms 定时器:                                    // core_worker.cc:473
   → FlushObjectsToRecover() → 取出 X
   → memory_store_->Delete(X)                       // core_worker.cc:481
   → RecoverObject(X)

5. RecoverObject(X) → ReconstructObject(X):         // object_recovery_manager.cc:140
   → GetLineageReconstructionEligibility(X)          // reference_counter.cc:1650
     → object_id_refs_.find(X) → 找到！Reference 还在
     → return X.eligibility → ELIGIBLE  ← 通过了！

   → ResubmitTask(A_task_id)                        // task_manager.cc:354
     → submissible_tasks_.find(A_task_id) → end     // 已在步骤 2① 被删
     → return OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED  ← 报错！

   → recovery_failure_callback_(X, MAX_ATTEMPTS_EXCEEDED, pin=true)
     → core_worker->Put(RayObject(MAX_ATTEMPTS_EXCEEDED), {}, X)
       → 写入 in_memory_store（不是 plasma！）
```

### 13.8 recovery_failure_callback 写 in_memory_store 而非 plasma

```cpp
// core_worker_process.cc:660-668
[this](const ObjectID &object_id, rpc::ErrorType reason, bool pin_object) {
  RAY_LOG(DEBUG).WithField(object_id)
      << "Failed to recover object due to " << rpc::ErrorType_Name(reason);
  auto core_worker = GetCoreWorker();
  RAY_UNUSED(core_worker->Put(RayObject(reason),
                              /*contained_object_ids=*/{},
                              object_id,
                              /*pin_object=*/pin_object));
}
```

`CoreWorker::Put` 在 `pin_object=false` 时写入 `in_memory_store_`，远程 Worker 通过 `ray.get` 看不到这个 error marker（它只查本地 memory store 和 plasma），导致反复触发恢复。

### 13.9 FlushObjectsToRecover 先 Delete 再恢复的循环

```cpp
// core_worker.cc:473-487
periodical_runner_->RunFnPeriodically(
    [this] {
      const auto lost_objects = reference_counter_->FlushObjectsToRecover();
      if (!lost_objects.empty()) {
        RAY_LOG(ERROR) << ":info_message: Attempting to recover " << lost_objects.size()
                       << " lost objects...";
        memory_store_->Delete(lost_objects);       // ← ★ 删除上次的 error marker
        for (const auto &object_id : lost_objects) {
          RAY_UNUSED(object_recovery_manager_->RecoverObject(object_id));
        }
      }
    },
    100,
    "CoreWorker.RecoverObjects");
```

每 100ms 轮询时，先 `Delete` 上次放入的 error marker（包括 `MAX_ATTEMPTS_EXCEEDED`），然后重新尝试恢复。如果恢复再次失败，又放入新的 error marker，下次轮询又删掉，形成循环。

### 13.10 恢复反复触发的三条回路

| 回路 | 触发机制 | 代码位置 |
|------|---------|---------|
| **定时器轮询** | 每 100ms `FlushObjectsToRecover` → 先 Delete 上次 error marker → 再次 `RecoverObject` | `core_worker.cc:473` |
| **远程 Worker 请求** | 远程 Worker `ray.get` 发现 plasma 中无值 → 向 Owner 发恢复请求 → 又走 `RecoverObject` | `object_recovery_manager.cc` |
| **位置持续无效** | `pinned_at_node_id_` 为 Nil → 节点扫描时反复入队 `objects_to_recover_` | `reference_counter.cc:893` |

---

## 14. A→B→C 链中 MAX_ATTEMPTS_EXCEEDED 完整时序示例

### 14.1 场景设定

```
A_task: read 产出 X
B_task: map 依赖 X 产出 Y1, Y2, Y3 (streaming generator)
C_task_1: map 依赖 Y1 产出 Z1
C_task_2: map 依赖 Y2 产出 Z2
C_task_3: map 依赖 Y3 产出 Z3
```

### 14.2 完整时序

```
── t1: A_task 完成 ──
  → X 提交到 plasma
  → driver 收到 X 的 RefBundle
  → X.local_ref_count = 1
  → reconstructable_owned_objects_ 队列: [X]
  → X.eligibility = ELIGIBLE
  → A_task spec 保留（retryable, reconstructable_return_ids_={X}）

── t2: B_task 提交，参数 [X] ──
  → X.submitted_task_ref_count++ (0→1)
  → X.lineage_ref_count++        (0→1)

── t3: driver 调度 B_task 后 del X 的 RefBundle ──
  → X.local_ref_count-- (1→0)
  → X.RefCount() = submitted(1) = 1  ← 还不为0

── t4: B_task 完成（retryable）──
  → Y1, Y2, Y3 产出
  → RemoveFinishedTaskReferences:               // task_manager.cc:1096
    → X.submitted_task_ref_count-- (1→0)
    → release_lineage = false（B retryable）
    → X.lineage_ref_count 不变 = 1

  → X.RefCount() = 0  ← 现在归零了

  → B_task retryable → lineage_footprint 计入
  → total_lineage_footprint_bytes_ += B_task.spec.ByteSizeLong()

  此时 X 的状态:
    X.local_ref_count = 0
    X.submitted_task_ref_count = 0
    X.lineage_ref_count = 1
    X.RefCount() = 0
    X.OutOfScope() = true (ELIGIBLE 不看 lineage_ref_count)

── t5: C_task_1, C_task_2, C_task_3 依次完成 ──
  → 每个 C_task retryable
  → 每个都增加 lineage_footprint
  → total_lineage_footprint_bytes_ 持续增长

── t6: 最后一个 C_task 完成，total_lineage > max_lineage_bytes ──
  → min_lineage_bytes_to_evict = total - max/2     // task_manager.cc:1071

  → EvictLineage 开始！                           // task_manager.cc:1129

  ┌─────────────────────────────────────────────────┐
  │ 第 1 轮: 取队头 X（最老的）                        │
  │                                                   │
  │ X.RefCount() = 0                                  │
  │ X.OutOfScope() = true                             │
  │                                                   │
  │ ReleaseLineageReferences(X):                      │
  │   ① on_lineage_released_(X)                       │
  │     → RemoveLineageReference(X)  // task_manager.cc:1443 │
  │     → A_task.reconstructable_return_ids_.erase(X) │
  │     → {} 为空 → submissible_tasks_.erase(A_task)  │
  │     → A_task spec 删了！                           │
  │                                                   │
  │   ② 检查标记:                                     │
  │     !X.OutOfScope() = !true = false               │
  │     → 跳过！eligibility 仍 ELIGIBLE                │
  │                                                   │
  │   ③ X.ShouldDelete()?                             │
  │     OutOfScope=true, lineage_ref_count=1 → false  │
  │     → X.Reference 保留                            │
  │                                                   │
  │ X 最终状态:                                       │
  │   A_task spec: 不存在                             │
  │   X.Reference: 存在                               │
  │   X.eligibility: ELIGIBLE  ← BUG!                │
  │   X.pinned_at: 有值（X 还在 plasma 中）            │
  └─────────────────────────────────────────────────┘
```

**此时还没有报错**，因为 X 还在 plasma 中。报错发生在 X 丢失后：

```
── t7: 节点 N 故障，X 的 plasma 副本丢失 ──
  → ResetObjectsOnRemovedNode(N)        // reference_counter.cc:893
  → X.pinned_at_node_id_ = Nil
  → objects_to_recover_.push_back(X)

── t8: 100ms 定时器 ──                    // core_worker.cc:473
  → FlushObjectsToRecover() → 取出 X
  → memory_store_->Delete(X)            // 删除上次的 error marker
  → RecoverObject(X)

── t9: RecoverObject(X) ──
  → pinned_at=Nil, spilled=false → requires_recovery=true
  → ReconstructObject(X):                // object_recovery_manager.cc:140
    → GetLineageReconstructionEligibility(X)  // reference_counter.cc:1650
      → object_id_refs_.find(X) → 找到！Reference 还在
      → return X.eligibility → ELIGIBLE  ← 通过了！

    → ResubmitTask(A_task_id)            // task_manager.cc:354
      → submissible_tasks_.find(A_task_id) → end
      → return OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED ← 报错！

    → recovery_failure_callback_(X, MAX_ATTEMPTS_EXCEEDED, pin=true)
      → core_worker->Put(RayObject(MAX_ATTEMPTS_EXCEEDED), {}, X)
      → 写入 in_memory_store（不是 plasma）

── t10: 远程 Worker 依赖 X ──
  → 尝试从 plasma 获取 X → 失败
  → 又向 Driver 发恢复请求
  → 又走 t9 的流程 → 又报 MAX_ATTEMPTS_EXCEEDED
```

### 14.3 关键时间窗口

```
t1 ──────────── t6 ──────────── t7 ──────────── t8/t9
│               │               │               │
│  X 正常存在    │  EvictLineage  │  节点故障       │  恢复触发
│  plasma 有值   │  删 A_task spec │  plasma 丢失    │  报错
│               │  X.Reference在  │  pinned_at=Nil  │
│               │  eligibility    │               │
│               │  = ELIGIBLE     │               │
│               │  (BUG)         │               │
```

**t6 到 t7 之间是安全的**——X 的 plasma 值还在，没人触发恢复。**只有在 t7 之后（X 丢失），bug 才暴露。**

### 14.4 为什么 B 完成后 X.RefCount()=0

```
t1: A_task 完成 → driver 持有 X 的 RefBundle → local_ref_count=1
t2: B_task 提交 → X.submitted_task_ref_count=1
t3: driver del RefBundle → X.local_ref_count=0 → RefCount=1(只有submitted)
t4: B_task 完成 → X.submitted_task_ref_count=0 → RefCount=0
```

**B 完成后，没有任何 Python 变量或正在执行的 task 持有 X，所以 RefCount=0，OutOfScope=true。**

如果 B 完成时 driver 还持有 X 的 RefBundle（local_ref_count=1），则：

```
X.RefCount() = 1 → OutOfScope=false → EvictLineage 会标记 INELIGIBLE → 报 LINEAGE_EVICTED ✅
```

**但 Ray Data 的流程是**：B_task 提交后 driver 立即 del 输入 RefBundle（`on_task_finished` → `inputs.destroy_if_owned()` → `del _running_tasks[task_index]`），所以到 EvictLineage 触发时 X.RefCount() 已经是 0 了。

### 14.5 pipeline 不同阶段的报错类型

| 管道阶段 | object 年龄 | 被 Evict 时 RefCount | 报错类型 |
|---------|------------|---------------------|---------|
| **Read 的输出（X）** | 最老 | 通常 = 0（RefBundle 已 del） | **MAX_ATTEMPTS_EXCEEDED** |
| **中间 Map 的输出（Y）** | 中间 | 可能 > 0（下游 task 正在执行） | **LINEAGE_EVICTED** |
| **最后 Map 的输出（Z）** | 最新 | 通常 > 0（Python 还在消费） | **LINEAGE_EVICTED** |

**Ray Data 中，read 阶段的输出最容易被 Evict，且此时 RefBundle 已经被 del 了，所以报 MAX_ATTEMPTS_EXCEEDED 而不是 LINEAGE_EVICTED。**

### 14.6 两种报错如何同时出现

```
pipeline: Read(A) → Map(B) → Map(C) → Consumer

时刻 1: A_task 完成，X1 入队 reconstructable_owned_objects_
  → driver del X1 的 RefBundle → X1.local_ref_count = 0

时刻 2: B_task 完成，Y1 入队
  → driver del Y1 的 RefBundle → Y1.local_ref_count = 0

时刻 3: C_task 完成（retryable），total_lineage > max_lineage_bytes
  → EvictLineage 开始

  第 1 轮: 取 X1（队头，最老）
    → X1.RefCount() = 0, OutOfScope=true → 不标记 → MAX_ATTEMPTS_EXCEEDED 候选

  如果 min_bytes 还不够，继续：

  第 2 轮: 取 Y1
    → Y1.RefCount() = 0, OutOfScope=true → 不标记 → MAX_ATTEMPTS_EXCEEDED 候选

  但如果此时正好有 C_task_2 正在执行（submitted_task_ref_count > 0）：
    → Y1.RefCount() > 0, OutOfScope=false → 标记 → LINEAGE_EVICTED
```

**同一个 pipeline 中，不同 object 可能报不同类型的错误。** 上游老 object 倾向报 MAX_ATTEMPTS_EXCEEDED，下游新 object 倾向报 LINEAGE_EVICTED。

---

## 15. FIFO 队列顺序与激进淘汰目标分析

### 15.1 淘汰量计算：淘汰到 max/2 而非 max

```cpp
// task_manager.cc:1071
min_lineage_bytes_to_evict = total_lineage_footprint_bytes_ - (max_lineage_bytes_ / 2);
```

当 `total = 1.1 * max` 时，需要淘汰 `0.6 * max`，即淘汰 **55% 的总量**。不是"淘汰一半"，而是**淘汰到只剩 max/2**。

这是个**一次性批量淘汰**，没有渐进机制——触发一次就把队头一路淘汰到满足为止。

### 15.2 不同超限程度下的淘汰比例

| 场景 | total | 淘汰量 | 淘汰比例 | 上游是否被淘汰 |
|------|-------|--------|---------|-------------|
| 刚超限 | 1.01GB | 0.51GB | ~50% | 是 |
| 超 10% | 1.1GB | 0.6GB | ~55% | 是 |
| 超 50% | 1.5GB | 1.0GB | ~67% | 是 |

一次淘汰 50%+ 的 lineage，几乎必然命中上游节点。

### 15.3 FIFO 导致上游优先被淘汰

```
创建时间线：
  X1 (A1 的输出) → Y1 (B1 的输出) → Z1 (C1 的输出) → ...

队列: [X1, Y1, Z1, ...]

EvictLineage 先淘汰 X1（最老的）
```

**上游 read 阶段的输出创建最早、排在队头，但它们往往是整条 pipeline 最关键的依赖**——所有下游 task 都间接依赖它们。淘汰上游一个，等于整条链的 lineage 都断了。

### 15.4 为什么 Ray Data 中 FIFO 顺序特别有害

Ray Data 的 pipeline 执行模式：

```
Read → 产出 X_1...X_N（最早创建，排在队头）
Map_B → 产出 Y_1...Y_N
Map_C → 产出 Z_1...Z_N（最晚创建，排在队尾）
```

1. **Read 的输出最先入队**，总是队头
2. **Read 输出的 RefBundle 最先被 del**（下游 task 提交后立即 del）
3. 所以 Read 输出被 Evict 时 `RefCount()=0`，eligibility 不标记
4. 但 Read 的输出是**所有下游的根依赖**，丢失它等于丢失整条链

### 15.5 更合理的淘汰策略方向

| 方案 | 思路 | 优点 | 缺点 |
|------|------|------|------|
| **淘汰到 max 而非 max/2** | `min = total - max` | 减少淘汰量，降低上游被误杀概率 | 频繁触发 |
| **LRU 而非 FIFO** | 淘汰最近最少被依赖的 | 保护活跃上游 | 实现复杂 |
| **按依赖深度加权** | 下游（依赖少）优先淘汰 | 保护关键上游 | 需要追踪依赖深度 |
| **渐进式淘汰** | 每次淘汰少量，多次触发 | 平滑 | 实现复杂 |
| **max/2 → max*0.8** | `min = total - max*0.8` | 留更多 buffer | 治标不治本 |

**当前设计本质问题**：用 FIFO 顺序 + 大比例淘汰，但队头恰好是最关键的上游依赖。应该**至少改为 LRU 或按下游依赖数排序**，让被依赖最少的 object 先淘汰。

---

## 16. 四个设计缺陷汇总

### 缺陷 1：EvictLineage 不标记 OutOfScope 的 ELIGIBLE 对象

**位置**：`reference_counter.cc:579-582`

```cpp
if (!ref->second.OutOfScope(lineage_pinning_enabled_) &&
    ref->second.lineage_eligibility_ == LineageReconstructionEligibility::ELIGIBLE) {
  ref->second.lineage_eligibility_ =
      LineageReconstructionEligibility::INELIGIBLE_LINEAGE_EVICTED;
}
```

**问题**：当 X 的 `RefCount()=0`（OutOfScope=true）但 `lineage_ref_count>0` 时，EvictLineage 删除了 task spec 但不标记 eligibility。X 处于"ELIGIBLE + 无 task spec + Reference 保留"的矛盾态。

**修复方向**：EvictLineage 删除了 task spec 后，应该无条件标记为 `INELIGIBLE_LINEAGE_EVICTED`，不管 `OutOfScope` 是否为 true。或者检查 `ShouldDelete()`——如果 `ShouldDelete()` 返回 false（lineage_ref_count > 0），说明 object 还"活着"，必须标记。

### 缺陷 2：recovery_failure_callback 写 in_memory_store 而非 plasma

**位置**：`core_worker_process.cc:660-668`

```cpp
[this](const ObjectID &object_id, rpc::ErrorType reason, bool pin_object) {
  auto core_worker = GetCoreWorker();
  RAY_UNUSED(core_worker->Put(RayObject(reason),
                              /*contained_object_ids=*/{},
                              object_id,
                              /*pin_object=*/pin_object));
}
```

**问题**：`Put` 在 `pin_object=true` 时写入 plasma（正确路径，如 OOM Kill → `MarkTaskReturnObjectsFailed`），但 `ReconstructObject` 传的是 `pin_object=true` 而 error type 不是 `OBJECT_IN_PLASMA`，导致远程 Worker 看不到 error marker，反复触发恢复。

**修复方向**：恢复失败时应该也写 plasma（像 `MarkTaskReturnObjectsFailed` 那样），或者在 `FlushObjectsToRecover` 中不再 Delete 上次写入的 error marker。

### 缺陷 3：FlushObjectsToRecover 先 Delete 再恢复的循环

**位置**：`core_worker.cc:481`

```cpp
memory_store_->Delete(lost_objects);
for (const auto &object_id : lost_objects) {
  RAY_UNUSED(object_recovery_manager_->RecoverObject(object_id));
}
```

**问题**：每次 100ms 轮询先 Delete 上次的 error marker，然后重新尝试恢复。如果恢复持续失败，形成无限循环。

**修复方向**：记录恢复失败次数，超过阈值后不再重试，或不在 Delete 之前就检查 object 是否已有 error marker。

### 缺陷 4：恢复失败不更新 pinned_at_node_id_

**位置**：`object_recovery_manager.cc` 中 `ReconstructObject` 失败后没有更新 `pinned_at_node_id_`

**问题**：object 持续处于"无位置"状态（`pinned_at_node_id_=Nil`），节点扫描时反复入队 `objects_to_recover_`，触发循环恢复。

**修复方向**：恢复失败后应该设置 `pinned_at_node_id_` 为某个特殊值（或标记为"unrecoverable"），防止重复入队。

### 16.1 对比：OOM Kill 的正确路径

OOM Kill 走的是正确路径，能正确写入 plasma error marker：

```
OOM Kill（should_retry=false）:
  Raylet: HandleGetWorkerFailureCause
    → FailOrRetryPendingTask(task_id, error_type, ..., mark_task_object_failed=true)
      → RetryTaskIfPossible → num_retries_left==0 → will_retry=false
      → FailPendingTask                              // task_manager.cc:1275
        → submissible_tasks_.erase(it)
        → MarkTaskReturnObjectsFailed(spec, error_type, ray_error_info, store_in_plasma_ids)
          → put_in_local_plasma_callback_(error, object_id)   // ★ 写入 plasma
```

```cpp
// task_manager.cc:1555-1580
void TaskManager::MarkTaskReturnObjectsFailed(
    const TaskSpecification &spec,
    rpc::ErrorType error_type,
    const rpc::RayErrorInfo *ray_error_info,
    const absl::flat_hash_set<ObjectID> &store_in_plasma_ids) {
  RayObject error(error_type, ray_error_info);
  for (int i = 0; i < num_returns; i++) {
    const auto object_id = ObjectID::FromIndex(task_id, i + 1);
    if (store_in_plasma_ids.contains(object_id)) {
      Status s = put_in_local_plasma_callback_(error, object_id);  // ★ 写入 plasma
      if (!s.ok()) {
        in_memory_store_.Put(error, object_id, ...);              // fallback
      }
    }
  }
}
```

OOM Kill 路径中 `FailPendingTask` 直接调用 `put_in_local_plasma_callback_` 写入 plasma，远程 Worker 能看到 error marker，不会反复触发恢复。**这是 lineage reconstruction 路径应该效仿但没效仿的正确行为。**

---

## 17. lineage_ref_count 生命周期详解

### 17.1 +1 时机：只在 task 首次提交时对参数 +1

```cpp
// reference_counter.cc — UpdateSubmittedTaskReferences
// 当 task 首次提交时，对每个 plasma 参数的 lineage_ref_count +1
```

**重提交（ResubmitTask）不 +1**——`UpdateResubmittedTaskReferences` 只处理 `submitted_task_ref_count`，不增加 `lineage_ref_count`。

### 17.2 -1 时机：通过 RemoveLineageReference 触发

```cpp
// task_manager.cc:1443-1491
int64_t TaskManager::RemoveLineageReference(const ObjectID &object_id,
                                              std::vector<ObjectID> *released_objects) {
  const TaskID &task_id = object_id.TaskId();
  auto it = submissible_tasks_.find(task_id);

  it->second.reconstructable_return_ids_.erase(object_id);

  if (it->second.reconstructable_return_ids_.empty() && !it->second.IsPending()) {
    // 递减依赖的 lineage ref count
    for (size_t i = 0; i < it->second.spec_.NumArgs(); i++) {
      if (it->second.spec_.ArgByRef(i)) {
        released_objects->push_back(it->second.spec_.ArgObjectId(i));
      }
    }
    // 删 spec
    submissible_tasks_.erase(it);
  }
  // ...
}
```

`released_objects` 返回给 `ReleaseLineageReferences`，在循环中对每个参数 `lineage_ref_count--`：

```cpp
// reference_counter.cc:598-606
for (const ObjectID &argument_id : argument_ids) {
  auto arg_it = object_id_refs_.find(argument_id);
  if (arg_it != object_id_refs_.end()) {
    arg_it->second.lineage_ref_count--;
    // ...
  }
}
```

### 17.3 两阶段清理模型

```
阶段 1: RefCount() == 0 → OutOfScope() → 释放 plasma 内存
  触发: Python del ref / task 完成 → submitted_task_ref_count 归零
  效果: UnsetObjectPrimaryCopy → 发布 WORKER_OBJECT_EVICTION → Raylet unpin

阶段 2: lineage_ref_count == 0 → ShouldDelete() → 删 lineage + Reference
  触发: 所有依赖此 object 的可重试 task 都不再可重试
  效果: ReleaseLineageReferences → 删上游 lineage_ref_count → EraseReference
```

**关键问题**：`RefCount()==0 && lineage_ref_count>0` 时，阶段 1 已触发（plasma 内存释放），但阶段 2 还没到（Reference 保留）。这个中间态正是 Bug 的温床。

### 17.4 streaming generator 的 lineage 释放时序

```
B_task: streaming generator 产出 Y1, Y2, Y3

A_task 的 lineage_ref_count 对 X 的 +1 由 B_task 首次提交时增加
B_task 的 lineage_ref_count 对 X 不直接 +1（B_task 的参数是 X）

释放时序：
  1. Y1 被 C_task_1 消费完 → RemoveLineageReference(Y1)
     → B_task.reconstructable_return_ids_.erase(Y1)
     → 但 {Y2, Y3} 不为空 → B_task spec 保留 → X.lineage_ref_count 不减

  2. Y2 被 C_task_2 消费完 → RemoveLineageReference(Y2)
     → B_task.reconstructable_return_ids_.erase(Y2)
     → 但 {Y3} 不为空 → B_task spec 保留 → X.lineage_ref_count 不减

  3. Y3 被 C_task_3 消费完 → RemoveLineageReference(Y3)
     → B_task.reconstructable_return_ids_.erase(Y3)
     → {} 为空 → B_task spec 删除
     → released_objects 包含 X → X.lineage_ref_count-- (1→0)
     → X.ShouldDelete() = true → ReleaseLineageReferences(X) → 删 A_task spec
     → EraseReference(X)
```

**上游 input block 的 `lineage_ref_count` 要等直接依赖的 task 的所有输出都被消费完才归零。**

### 17.5 pipeline 终端输出 Z 的 lineage_ref_count

Z（pipeline 终端输出，如 `write()` 或 `iter_batches()` 的输出）创建时 `lineage_ref_count = 0`：
- 没有下游 Ray task 依赖 Z
- Z 不需要对任何参数 +1 lineage_ref_count
- Z 的 `ShouldDelete()` 取决于 `RefCount()` 和 `lineage_ref_count`

Z 的回收触发：
- `write()` 场景：`write()` 返回后 driver del Z 的 RefBundle → `RefCount()=0` → `ShouldDelete()=true` → 立即清理
- `iter_batches()` 场景：driver 消费完最后一个 batch 后 del → 同上

---

## 18. 附加代码索引

| 文件 | 行号 | 说明 |
|------|------|------|
| `src/ray/core_worker/reference_counter.cc` | 569-607 | `ReleaseLineageReferences` — eligibility 标记与递归释放 |
| `src/ray/core_worker/reference_counter.cc` | 823-839 | `EvictLineage` — FIFO 队列逐个淘汰 |
| `src/ray/core_worker/reference_counter.cc` | 893-904 | `ResetObjectsOnRemovedNode` — 节点故障时入队恢复 |
| `src/ray/core_worker/reference_counter.cc` | 1650-1660 | `GetLineageReconstructionEligibility` — eligibility 检查 |
| `src/ray/core_worker/reference_counter.h` | 346-349 | `RefCount()` — 不含 lineage_ref_count |
| `src/ray/core_worker/reference_counter.h` | 357-371 | `OutOfScope()` — ELIGIBLE 时不看 lineage_ref_count |
| `src/ray/core_worker/reference_counter.h` | 378-384 | `ShouldDelete()` — 需 lineage_ref_count==0 |
| `src/ray/core_worker/task_manager.cc` | 354-364 | `ResubmitTask` — spec 不存在时返回 MAX_ATTEMPTS_EXCEEDED |
| `src/ray/core_worker/task_manager.cc` | 1066-1071 | CompletePendingTask 中 lineage 超限判断与淘汰量计算 |
| `src/ray/core_worker/task_manager.cc` | 1129-1132 | 调用 `EvictLineage` |
| `src/ray/core_worker/task_manager.cc` | 1443-1491 | `RemoveLineageReference` — spec 删除与 released_objects 收集 |
| `src/ray/core_worker/task_manager.cc` | 1555-1580 | `MarkTaskReturnObjectsFailed` — OOM Kill 正确路径写入 plasma |
| `src/ray/core_worker/core_worker.cc` | 473-487 | `FlushObjectsToRecover` — 先 Delete 再恢复的循环 |
| `src/ray/core_worker/core_worker_process.cc` | 660-668 | `recovery_failure_callback` — 写 in_memory_store |
| `src/ray/core_worker/object_recovery_manager.cc` | 140-187 | `ReconstructObject` — eligibility 检查 → ResubmitTask → 递归恢复 |
| `src/ray/common/ray_config_def.h` | 170 | `max_lineage_bytes` 配置（默认 1GB） |
