# Ray Pending Demands Memory 来源与两层调度机制深度解析

**日期**: 2026-06-02
**Ray 版本**: 2.52.1
**关键词**: Pending Demands, memory 资源, LeasePolicy, Raylet 调度, spillback, prepare_args_internal
**关联文档**: [PENDING_NODE_ASSIGNMENT 堆积排查](./pending-node-assignment-by-pinned-args-memory-20260601.md) — 同一场景下 pinned lease args 内存预算耗尽的深入排查

---

## 一、Pending Demands 中 memory 的来源

### 1.1 问题描述

用户没有显式指定 task 的 memory，但 `ray status` 仍然显示不同的 memory 需求：

```
Pending Demands:
  {'CPU': 2.0, 'memory': 21578230.0}: 1+ pending tasks/actors
  {'CPU': 2.0, 'memory': 20289499.0}: 1+ pending tasks/actors
```

这些值（≈20.5MB 和 ≈19.3MB）表示不同 task/actor 的资源需求 shape 不同。

### 1.2 数据流全链路

Pending Demands 的数据从 task 提交到最终展示，经过以下完整链路：

```
Python 用户代码 (ray.remote / PG bundle)
    ↓ resources dict (含或不含 memory)
Cython (_raylet.pyx: prepare_resources)
    ↓ unordered_map<string, double>
C++ CoreWorker::SubmitTask (core_worker.cc:2168)
    ↓ required_resources → TaskSpecBuilder
NormalTaskSubmitter::SubmitTask (normal_task_submitter.cc:34)
    ↓ task_spec → scheduling_key_entries_[key].task_queue
Raylet NodeManager::HandleRequestWorkerLease (node_manager.cc:1794)
    ↓ ClusterLeaseManager::QueueAndScheduleLease
Raylet ClusterLeaseManager::ScheduleAndGrantLeases (cluster_lease_manager.cc:196)
    ↓ SchedulingClass → scheduling_class_descriptor.resource_set
SchedulerResourceReporter::FillResourceUsage (scheduler_resource_reporter.cc:59)
    ↓ demand.shape() → resource_load_by_shape.resource_demands
GCS AutoscalerStateManager::GetPendingResourceRequests (gcs_autoscaler_state_manager.cc:324)
    ↓ FillAggregateLoad → aggregate_load
Autoscaler V2 Utils::_demand_report (utils.py:635)
    ↓ format_resource_demand_summary → Pending Demands 显示
```

#### 各层详细代码追踪

**Step 1: Python 层 — `resources_from_ray_options`**

```python
# python/ray/_common/utils.py:137
def resources_from_ray_options(options_dict: Dict[str, Any]) -> Dict[str, Any]:
    resources = (options_dict.get("resources") or {}).copy()
    num_cpus = options_dict.get("num_cpus")
    num_gpus = options_dict.get("num_gpus")
    memory = options_dict.get("memory")
    # ...
    if memory is not None:
        resources["memory"] = int(memory)  # ★ 只有显式指定时才加入
    return resources
```

**结论：如果用户不指定 memory，resources dict 中就没有 `memory` 键。**

**Step 2: Cython 层 — `prepare_resources`**

```cython
# python/ray/_raylet.pyx:650
cdef int prepare_resources(
        dict resource_dict,
        unordered_map[c_string, double] *resource_map) except -1:
    for key, value in resource_dict.items():
        if value > 0:
            resource_map[0][key.encode("ascii")] = float(value)
    return 0
```

纯透传，不做任何默认值补充。

**Step 3: C++ CoreWorker — `SubmitTask`**

```cpp
// src/ray/core_worker/core_worker.cc:2188-2189
auto constrained_resources =
    AddPlacementGroupConstraint(task_options.resources, scheduling_strategy);
```

`AddPlacementGroupConstraint` 只在 task 属于 Placement Group 时给资源名加 PG 前缀（如 `CPU_group_xxx`），不增加 memory。

**Step 4: C++ Raylet — `ComputeResources`**

```cpp
// src/ray/common/task/task_spec.cc:58
void TaskSpecification::ComputeResources() {
    auto &required_resources = message_->required_resources();
    if (required_resources.empty()) {
        required_resources_ = ResourceSet::Nil();
    } else {
        required_resources_ =
            std::make_shared<ResourceSet>(MapFromProtobuf(required_resources));
    }
    // ...
    // SchedulingClassDescriptor 基于 resource_set 计算
    sched_cls_desc = SchedulingClassDescriptor(resource_set, ...);
    sched_cls_id_ = SchedulingClassToIds::GetSchedulingClass(sched_cls_desc);
}
```

C++ 层面也不做 memory 自动推断，直接从 protobuf 读取 `required_resources`。

**Step 5: Raylet — `SchedulerResourceReporter::FillResourceUsage`**

```cpp
// src/ray/raylet/scheduling/scheduler_resource_reporter.cc:100-113
const auto &resources = scheduling_class_descriptor.resource_set.GetResourceMap();
auto by_shape_entry = resource_load_by_shape->Add();
for (const auto &resource : resources) {
    (*by_shape_entry->mutable_shape())[label] = quantity;
}
```

`resource_set` 就是 task 的 `required_resources`，直接写入 `resource_demands` 的 shape。

**Step 6: GCS — `FillAggregateLoad`**

```cpp
// src/ray/gcs/state_util.cc:22
void FillAggregateLoad(
    const rpc::ResourcesData &resources_data,
    absl::flat_hash_map<ResourceDemandKey, rpc::ResourceDemand> *aggregate_load) {
    const auto &load = resources_data.resource_load_by_shape();
    for (const auto &demand : load.resource_demands()) {
        ResourceDemandKey key;
        key.shape = demand.shape();  // ★ 直接取 shape，不做修改
        // ...
        auto &aggregate_demand = (*aggregate_load)[key];
        aggregate_demand.set_num_ready_requests_queued(
            aggregate_demand.num_ready_requests_queued() +
            demand.num_ready_requests_queued());
    }
}
```

**Step 7: GCS — `GetPendingResourceRequests`**

```cpp
// src/ray/gcs/gcs_autoscaler_state_manager.cc:324
void GcsAutoscalerStateManager::GetPendingResourceRequests(
    rpc::autoscaler::ClusterResourceState *state) {
    auto aggregate_load = GetAggregatedResourceLoad();
    for (auto &[key, demand] : aggregate_load) {
        const auto &shape = key.shape;
        auto num_pending = demand.num_infeasible_requests_queued() +
                           demand.backlog_size() +
                           demand.num_ready_requests_queued();
        if (num_pending > 0) {
            auto pending_req = state->add_pending_resource_requests();
            pending_req->set_count(num_pending);
            auto req = pending_req->mutable_request();
            req->mutable_resources_bundle()->insert(shape.begin(), shape.end());
        }
    }
}
```

**Step 8: Autoscaler V2 — `_demand_report`**

```python
# python/ray/autoscaler/v2/utils.py:635
def _demand_report(data: ClusterStatus) -> str:
    resource_demands = [
        (bundle.bundle, bundle.count)
        for demand in data.resource_demands.ray_task_actor_demand
        for bundle in demand.bundles_by_count
    ]
    if resource_demands:
        demand_lines.extend(format_resource_demand_summary(resource_demands))
```

### 1.3 Memory 的实际来源

**Ray Core 本身不会在没有指定 memory 时自动推断 memory。** 你看到的 memory 值一定来自于某个上游组件。具体有以下几种可能来源：

#### 来源 1（最可能）：Placement Group Bundle 的 memory

如果使用了 Placement Group，并且 bundle 中包含了 `memory` 资源，当 `placement_group_capture_child_tasks=True` 时，child task 会被自动分配到 PG bundle 中，task 的 `required_resources` 会继承 PG bundle 中的 memory 值。

```python
# 创建 PG 时 bundle 包含了 memory
pg = ray.util.placement_group([{"CPU": 2, "memory": 21578230}])

# child task 会被分配到这个 bundle，继承其 memory
@ray.remote(num_cpus=2)
def my_func():  # 虽然没指定 memory，但 PG bundle 有
    ...
```

在 C++ 层面，`AddPlacementGroupConstraint()` (位于 `src/ray/common/bundle_spec.cc:158`) 会将 bundle 的资源映射为 PG 约束资源名（如 `CPU_group_xxx`），而 bundle 中原始的 `memory` 值就变成了 task 的 memory 需求。

#### 来源 2：Ray Data 自动注入的 memory

如果使用 Ray Data，部分 operator 会自动计算并注入 memory 需求：

- **TaskPoolMapOperator**: `memory=self._ray_remote_args.get("memory", 0)` — 默认 0
- **ActorPoolMapOperator**: `memory=self._ray_remote_args.get("memory")` — 默认 None
- **HashShuffleOperator**: 会根据集群 memory 自动计算 memory 分配

所以如果使用了 `HashShuffleOperator`（如 `sort()`、`repartition()` 等操作），Ray Data 会自动计算 memory 需求。

#### 来源 3：高层库自动计算 memory

Ray 的其他高层库也可能自动计算 memory：

- **Ray Serve** 的 replica 会根据配置分配 memory
- **Ray Train** 的 training worker 有时自动推断 memory
- **Ray LLM** 明确设置了 `memory: 100`

### 1.4 如何确认具体来源

```bash
# 1. 检查是否有 Placement Group
ray list placement-groups

# 2. 查看 task/actor 的详细资源需求
ray list tasks --format json  # 查看 required_resources 字段

# 3. 查看 actor 详情
ray list actors --format json  # 查看 required_resources 字段

# 4. 直接检查 PG bundle 中的 memory
python -c "
import ray
ray.init()
for pg in ray.util.placement_group_table().values():
    print(f'PG {pg[\"placement_group_id\"]}: bundles={pg[\"bundles\"]}')
"
```

---

## 二、Raylet Scheduler 是哪个 Raylet —— 两层调度架构

### 2.1 核心问题

Raylet Scheduler 可以是提交端的本地 Raylet，也可以是远端 Raylet，取决于调度结果。这里存在两层决策。

### 2.2 完整的两层调度架构

```
┌─────────────────────────────────────────────────────────────────────────┐
│  第 1 层决策: CoreWorker → 哪个 Raylet 负责调度？                        │
│  (LeasePolicy::GetBestNodeForLease)                                    │
│                                                                         │
│  这个决策非常"轻量"——它只考虑数据局部性(locality)：                       │
│  "task 的参数数据最多在哪个节点上？"                                     │
│                                                                         │
│  结果: 选出一个 raylet 地址, 发送 RequestWorkerLease RPC                  │
└────────────────────────────┬────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  第 2 层决策: Raylet → 哪个节点最终执行 task？                           │
│  (ClusterLeaseManager::ScheduleAndGrantLeases                          │
│   → GetBestSchedulableNode)                                             │
│                                                                         │
│  这个决策是"重量级"的——它持有整个集群的资源视图：                         │
│  - 每个节点的 CPU/GPU/memory 可用量                                     │
│  - 每个节点正在运行的 task 数量                                          │
│  - SchedulingStrategy (SPREAD/Hybrid/NodeAffinity 等)                   │
│  - 节点是否 feasible/infeasible                                         │
│                                                                         │
│  结果:                                                                   │
│  ├─ 本地有资源 → ScheduleOnNode(self_node_id_) → 本地执行               │
│  └─ 本地没资源 → ScheduleOnNode(remote_node_id) → spillback             │
└─────────────────────────────────────────────────────────────────────────┘
```

### 2.3 首次请求不一定到本地 Raylet（纠正）

**关键纠正**：首次请求不一定是到本地 Raylet。

`RequestNewWorkerIfNeeded` 有两种调用路径：

```cpp
// src/ray/core_worker/task_submission/normal_task_submitter.cc:313
const bool is_spillback = (raylet_address != nullptr);
```

| 调用场景 | `raylet_address` 参数 | `is_spillback` | 含义 |
|----------|----------------------|----------------|------|
| 首次请求 | `nullptr` | `false` | CoreWorker 主动发起，需 LeasePolicy 选择 raylet |
| Spillback 重定向 | 非 `nullptr`（远端 raylet 地址） | `true` | 由首次请求的 Raylet 重定向而来 |

首次请求的目标 Raylet 由 `LeasePolicy::GetBestNodeForLease` 决定：

```cpp
// normal_task_submitter.cc:315-319
if (raylet_address == nullptr) {
    // 首次请求：用 LeasePolicy 选择目标 raylet
    std::tie(best_node_address, is_selected_based_on_locality) =
        lease_policy_->GetBestNodeForLease(lease_spec);
    raylet_address = &best_node_address;
}
```

### 2.4 LeasePolicy 的两种实现

Ray 默认启用 `locality_aware_leasing_enabled=true`（配置于 `src/ray/common/ray_config_def.h:706`），使用 `LocalityAwareLeasePolicy`。

```cpp
// src/ray/core_worker/core_worker_process.cc:550-556
auto lease_policy =
    RayConfig::instance().locality_aware_leasing_enabled()
        ? std::unique_ptr<LeasePolicyInterface>(
              std::make_unique<LocalityAwareLeasePolicy>(
                  *reference_counter, node_addr_factory, raylet_address))
        : std::unique_ptr<LeasePolicyInterface>(
              std::make_unique<LocalLeasePolicy>(raylet_address));
```

#### 2.4.1 `LocalityAwareLeasePolicy`（默认）

```cpp
// src/ray/core_worker/lease_policy.cc:24-59
std::pair<rpc::Address, bool> GetBestNodeForLease(const LeaseSpecification &spec) {
    // 优先级 1: SPREAD 策略 → 回退到本地 raylet
    if (spread_strategy) return fallback_rpc_address_;

    // 优先级 2: NodeAffinity / Label 策略 → 指定节点
    if (node_affinity) return target_node_addr;

    // 优先级 3: 数据局部性 → 拥有最多参数数据的节点
    // ★ 可能返回远端 raylet！
    if (auto node_id = GetBestNodeIdForLease(spec)) {
        if (auto addr = node_addr_factory_(node_id.value())) {
            return std::make_pair(addr.value(), true);  // is_selected_based_on_locality=true
        }
    }

    // 优先级 4: 没有数据偏好 → 回退到本地 raylet
    return std::make_pair(fallback_rpc_address_, false);
}
```

数据局部性的核心算法：

```cpp
// src/ray/core_worker/lease_policy.cc:63-88
std::optional<NodeID> LocalityAwareLeasePolicy::GetBestNodeIdForLease(
    const LeaseSpecification &spec) {
    const auto object_ids = spec.GetDependencyIds();
    absl::flat_hash_map<NodeID, uint64_t> bytes_local_table;
    uint64_t max_bytes = 0;
    std::optional<NodeID> max_bytes_node;
    // 遍历 task 的所有参数 ObjectRef
    for (const ObjectID &object_id : object_ids) {
        if (auto locality_data = locality_data_provider_.GetLocalityData(object_id)) {
            for (const NodeID &node_id : locality_data->nodes_containing_object) {
                auto &bytes = bytes_local_table[node_id];
                bytes += locality_data->object_size;
                if (bytes > max_bytes) {
                    max_bytes = bytes;
                    max_bytes_node = node_id;  // 找拥有最多数据的节点
                }
            }
        }
    }
    return max_bytes_node;
}
```

**关键点**：`LocalityAwareLeasePolicy` 可能返回远端 Raylet 的地址！当 task 的参数数据集中在某个远端节点时，CoreWorker 会直接把 lease 请求发到那个远端 Raylet，目的是减少数据传输。

#### 2.4.2 `LocalLeasePolicy`（关闭 locality_aware_leasing 时）

```cpp
// src/ray/core_worker/lease_policy.cc:90-93
std::pair<rpc::Address, bool> GetBestNodeForLease(const LeaseSpecification &spec) {
    // 永远返回本地 raylet
    return std::make_pair(local_node_rpc_address_, false);
}
```

### 2.5 第 2 层：Raylet 的调度决策

Raylet 收到 `RequestWorkerLease` RPC 后，在 `ClusterLeaseManager::ScheduleAndGrantLeases` 中做最终调度决策：

```cpp
// src/ray/raylet/scheduling/cluster_lease_manager.cc:196-296
void ClusterLeaseManager::ScheduleAndGrantLeases() {
    TryScheduleInfeasibleLease();
    for (auto shapes_it = leases_to_schedule_.begin();
         shapes_it != leases_to_schedule_.end();) {
        auto &work_queue = shapes_it->second;
        bool is_infeasible = false;
        for (auto work_it = work_queue.begin(); work_it != work_queue.end();) {
            const std::shared_ptr<internal::Work> &work = *work_it;
            // ★ 核心调度：在集群范围内找最佳节点
            auto scheduling_node_id = cluster_resource_scheduler_.GetBestSchedulableNode(
                lease.GetLeaseSpecification(),
                /*preferred_node_id*/ work->PrioritizeLocalNode()
                    ? self_node_id_.Binary()
                    : lease.GetPreferredNodeID(),
                /*exclude_local_node*/ false,
                /*requires_object_store_memory*/ false,
                &is_infeasible);

            if (scheduling_node_id.IsNil()) {
                // 没有可行节点
                break;
            }

            NodeID node_id = NodeID::FromBinary(scheduling_node_id.Binary());
            ScheduleOnNode(node_id, work);  // 本地执行 or spillback
            work_it = work_queue.erase(work_it);
        }
        // ...
    }
    local_lease_manager_.ScheduleAndGrantLeases();
}
```

`GetBestSchedulableNode` 根据不同 `SchedulingStrategy` 选择调度算法：

```cpp
// src/ray/raylet/scheduling/cluster_resource_scheduler.cc:151-226
scheduling::NodeID ClusterResourceScheduler::GetBestSchedulableNode(
    const ResourceRequest &resource_request,
    const rpc::SchedulingStrategy &scheduling_strategy, ...) {

    if (scheduling_strategy == kSpreadSchedulingStrategy) {
        return scheduling_policy_->Schedule(resource_request,
            SchedulingOptions::Spread(force_spillback, ...));
    } else if (scheduling_strategy == kNodeAffinitySchedulingStrategy) {
        return scheduling_policy_->Schedule(resource_request,
            SchedulingOptions::NodeAffinity(...));
    } else if (IsAffinityWithBundleSchedule(scheduling_strategy)) {
        return scheduling_policy_->Schedule(resource_request,
            SchedulingOptions::AffinityWithBundle(bundle_id));
    } else if (has_node_label_scheduling_strategy) {
        return scheduling_policy_->Schedule(resource_request,
            SchedulingOptions::NodeLabelScheduling(...));
    } else {
        // 默认：Hybrid 调度（优先本地 + 资源可用性）
        return scheduling_policy_->Schedule(resource_request,
            SchedulingOptions::Hybrid(force_spillback, ..., preferred_node_id));
    }
}
```

### 2.6 `ScheduleOnNode`：本地执行 vs Spillback

```cpp
// src/ray/raylet/scheduling/cluster_lease_manager.cc:422-461
void ClusterLeaseManager::ScheduleOnNode(const NodeID &spillback_to,
                                         const std::shared_ptr<internal::Work> &work) {
    if (spillback_to == self_node_id_) {
        // 本地有资源 → 本地执行
        local_lease_manager_.QueueAndScheduleLease(work);
        return;
    }

    if (work->grant_or_reject_) {
        // Spillback 请求但目标也没资源 → reject，不再级联 spillback
        for (const auto &reply_callback : work->reply_callbacks_) {
            reply_callback.reply_->set_rejected(true);
            reply_callback.send_reply_callback_(Status::OK(), nullptr, nullptr);
        }
        return;
    }

    // 首次请求，本地没资源但远端有 → spillback
    // 回复 CoreWorker: retry_at_raylet_address=远端地址
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

### 2.7 Spillback 时 `grant_or_reject` 的关键作用

当 CoreWorker 向 Raylet 发送 `RequestWorkerLease` 时：

- **首次请求**：`grant_or_reject=false`
- **Spillback 请求**：`grant_or_reject=true`

```cpp
// normal_task_submitter.cc:328-330
raylet_client->RequestWorkerLease(
    lease_spec.GetMessage(),
    /*grant_or_reject=*/is_spillback,  // ★ 关键区分
    ...);
```

`grant_or_reject` 影响 `PrioritizeLocalNode()` 决策：

```cpp
// src/ray/raylet/scheduling/internal.h:104-106
bool PrioritizeLocalNode() const {
    return grant_or_reject_ || is_selected_based_on_locality_;
}
```

| 场景 | grant_or_reject | is_selected_based_on_locality | PrioritizeLocalNode | 调度行为 |
|------|----------------|------------------------------|--------------------|---------| 
| 首次请求（本地偏好） | false | false | false | 自由选择集群节点 |
| 首次请求（数据局部性偏好） | false | true | true | 偏好本地节点 |
| Spillback 请求 | true | - | true | 强制本地调度，不再 spillback |

**设计目的：spillback 时远端 Raylet 不会再做二次 spillback，避免级联。**



  为什么不能只用第 1 层决策？

  CoreWorker 没有集群的资源视图。 它只知道数据局部性——参数在哪些节点上，不知道：
  - 每个节点还有多少 CPU/GPU/memory 空闲
  - 哪些节点已经过载 
  - 其他 task 正在排队的情况
  
  而 Raylet 通过 GCS 和节点间的心跳同步，维护了集群全局资源视图。

  代码级别的关键细节

  第 1 层：CoreWorker 的 LeasePolicy (lease_policy.cc:63)

  // LocalityAwareLeasePolicy::GetBestNodeIdForLease()
  // 只看数据局部性：哪个节点拥有最多的参数数据？
  for (const ObjectID &object_id : object_ids) {
      if (auto locality_data = locality_data_provider_.GetLocalityData(object_id)) {
          for (const NodeID &node_id : locality_data->nodes_containing_object) {
              bytes += locality_data->object_size;
              // 找到拥有最多数据的节点
          }
      }
  }
  return max_bytes_node;  // 可能返回 nil（没数据偏好 → 回退到本地 raylet）

  第 2 层：Raylet 的 GetBestSchedulableNode (cluster_lease_manager.cc:214)

  auto scheduling_node_id = cluster_resource_scheduler_.GetBestSchedulableNode(
      lease.GetLeaseSpecification(),
      /*preferred_node_id*/ work->PrioritizeLocalNode() ? self_node_id_.Binary()
                                                        : lease.GetPreferredNodeID(),
      /*exclude_local_node*/ false,
      /*requires_object_store_memory*/ false,
      &is_infeasible);

  注意 work->PrioritizeLocalNode() 逻辑：

  // internal.h:104
  bool PrioritizeLocalNode() const {
      return grant_or_reject_ || is_selected_based_on_locality_;
  }

  - 首次请求（CoreWorker → 本地 Raylet）：grant_or_reject=false，is_selected_based_on_locality=false → PrioritizeLocalNode()=false → 不强制本地，可以在集群中自由选择
  - Spillback 请求（CoreWorker → 远端 Raylet）：grant_or_reject=true → PrioritizeLocalNode()=true → 强制本地调度

  关键设计：spillback 时远端 Raylet 不会再做二次 spillback。

  // cluster_lease_manager.cc:429
  if (work->grant_or_reject_) {
      // 远端 Raylet 发现本地也没资源 → 直接 reject，不再 spill
      reply->set_rejected(true);
      return;
  }

### 2.8 两个"选节点"的区别

| 维度 | LeasePolicy::GetBestNodeForLease | Raylet::GetBestSchedulableNode |
|------|----------------------------------|-------------------------------|
| **谁调用** | CoreWorker | Raylet |
| **信息量** | 只有数据局部性（参数在哪些节点） | 集群全局资源视图（每个节点 CPU/GPU/memory 空闲量） |
| **决策依据** | "数据在哪" | "哪里有资源能跑" |
| **决策精度** | 粗略路由，可能选错 | 精确调度，考虑资源可行性 |
| **能否判断 infeasible** | 不能 | 能 |
| **设计目的** | 快速选一个"最可能合适"的调度入口 | 做最终的资源分配决策 |

**设计理念**：CoreWorker 做数据局部性优化的"软路由"，Raylet 做资源精确匹配的"硬决策"。CoreWorker 选的 Raylet 只是一个"建议入口"，Raylet 可以推翻这个建议（spillback 到更合适的节点）。

### 2.9 完整流程图（场景分解）

**场景 A：本地调度成功**

```
CoreWorker                          本地 Raylet
   │                                    │
   │──RequestWorkerLease(grant_or_reject=false)──→│
   │                                    │
   │                        GetBestSchedulableNode()
   │                        → 返回 self_node_id_
   │                        → 本地有资源！
   │                                    │
   │←────── worker_address ───────────│
   │                        (直接在本地分配 worker)
```

**场景 B：首次请求到远端 Raylet（数据局部性驱动）**

```
CoreWorker                          远端 Raylet（数据所在节点）
   │                                    │
   │──LeasePolicy::GetBestNodeForLease()──→│
   │  (返回远端节点，因为参数数据在那里)    │
   │                                    │
   │──RequestWorkerLease(grant_or_reject=false)──→│
   │                                    │
   │                        GetBestSchedulableNode()
   │                        → 返回 self_node_id_（本地有资源）
   │                        → 或返回其他远端节点（spillback）
   │                                    │
   │←────── worker_address ───────────│
```

**场景 C：Spillback**

```
CoreWorker                 本地 Raylet              远端 Raylet
   │                          │                        │
   │──RequestWorkerLease────→│                        │
   │  (grant_or_reject=false) │                        │
   │                          │                        │
   │              GetBestSchedulableNode()              │
   │              → 返回 remote_node_id                │
   │              → 本地没资源，远端有！                 │
   │                          │                        │
   │←── retry_at_raylet_address ──│                    │
   │                          │                        │
   │──RequestWorkerLease────────────────────────────→│
   │  (grant_or_reject=true!)  │                        │
   │                          │                        │
   │                        GetBestSchedulableNode()
   │                        → PrioritizeLocalNode()=true
   │                        → preferred_node=self
   │                        → 本地有资源！分配 worker    │
   │                                                  │
   │←────────────── worker_address ─────────────────│
   │                                                  │
   │ (若远端也没资源 → rejected → CoreWorker 重新回本地重试)
```
完整的调度链路如下：

  ┌─────────────────────────────────────────────────────────────────────┐
  │  1. Python Worker (CoreWorker)                                      │
  │     @ray.remote def f(x): ...                                       │
  │     f.remote(arg)                                                   │
  │       ↓                                                             │
  │     remote_function.py: _remote() → resources = resources_from_ray_options()
  │       ↓                                                             │
  │     _raylet.pyx: submit_task()                                      │
  │       prepare_args_internal()  → 序列化参数                           │
  │       CCoreWorkerProcess.GetCoreWorker().SubmitTask()                │
  │       ↓                                                             │
  │     core_worker.cc: SubmitTask()                                    │
  │       → BuildCommonTaskSpec(required_resources)                     │
  │       → task_manager_->AddPendingTask()                             │
  │       → normal_task_submitter_->SubmitTask(task_spec)               │
  │       ↓                                                             │
  │     normal_task_submitter.cc: SubmitTask()                          │
  │       → resolver_.ResolveDependencies()  (解析 ObjectRef 依赖)      │
  │       → scheduling_key_entries_[key].task_queue.push_back(spec)     │
  │       → RequestNewWorkerIfNeeded()                                  │
  │         → lease_policy_->GetBestNodeForLease()  (选目标 raylet)     │
  │         → raylet_client->RequestWorkerLease()  (RPC 发给目标 raylet) │
  ├─────────────────────────────────────────────────────────────────────┤
  │  2. 目标 Raylet (NodeManager)                                       │
  │     node_manager.cc: HandleRequestWorkerLease()                     │
  │       → cluster_lease_manager_.QueueAndScheduleLease()              │
  │         → GetBestSchedulableNode()  (在本节点或集群中找最佳节点)      │
  │         → ScheduleOnNode(spillback_to, work)                        │
  │           ├─ if spillback_to == self_node_id_:                      │
  │           │    local_lease_manager_.QueueAndScheduleLease(work)     │
  │           │    → 本地调度：分配 worker 执行                           │
  │           └─ else:                                                  │
  │                回复 CoreWorker: retry_at_raylet_address = 远端 raylet│
  │                → CoreWorker 重新向远端 raylet 请求 lease (spillback) │
  ├─────────────────────────────────────────────────────────────────────┤
  │  3. 最终执行 Raylet (可能是本地，也可能是远端)                         │
  │     local_lease_manager.cc: QueueAndScheduleLease()                 │
  │       → WaitForLeaseArgsRequests() (等待参数就绪)                    │
  │       → ScheduleAndGrantLeases() → GrantScheduledLeasesToWorkers()  │
  │       → 将 lease (worker) 分配给 task 执行                           │
  └─────────────────────────────────────────────────────────────────────┘

  关键点：

  1. CoreWorker 首先向"本地 Raylet"（提交端的 raylet）发送 RequestWorkerLease RPC
  2. 本地 Raylet 的 ClusterLeaseManager 在整个集群范围内寻找最佳节点
  3. 如果本地节点资源充足 → ScheduleOnNode(self_node_id_) → 本地调度
  4. 如果本地资源不足，但远端有 → ScheduleOnNode(remote_node_id) → spillback
  5. spillback 时，CoreWorker 收到远端 raylet 地址，重新向远端 raylet 发送请求

  所以 Raylet Scheduler 最初是提交端的本地 Raylet，但它会在集群范围内做调度决策，可能最终将 task 调度到远端 Raylet。
---

## 三、`prepare_args_internal` 详细代码逻辑

### 3.1 函数签名与参数传递方式

```cython
# python/ray/_raylet.pyx:751
cdef prepare_args_internal(
        CoreWorker core_worker,          # Python CoreWorker 对象
        Language language,                # 语言类型 (PYTHON 等)
        args,                             # Python list: 函数参数列表
        c_vector[unique_ptr[CTaskArg]] *args_vector,  # ★ 指针 → 输出参数
        function_descriptor,              # 函数描述符 (用于错误信息)
        c_vector[CObjectID] *incremented_put_arg_ids): # ★ 指针 → 输出参数
```

#### 参数传递方式分析

| 参数 | Cython 类型 | 传递方式 | 说明 |
|------|------------|---------|------|
| `core_worker` | `CoreWorker` | **值传递**（Python 对象引用） | Python 对象在 Cython 中本质是引用计数的指针 |
| `language` | `Language` | **值传递** | 枚举类型，很小 |
| `args` | (无类型声明) | **值传递**（Python 对象引用） | Python list，传递的是引用 |
| `args_vector` | `c_vector[unique_ptr[CTaskArg]] *` | **指针传递** ★ | **输出参数**，调用方传入局部变量地址 |
| `function_descriptor` | (无类型声明) | **值传递**（Python 对象引用） | 仅用于错误信息 |
| `incremented_put_arg_ids` | `c_vector[CObjectID] *` | **指针传递** ★ | **输出参数**，跟踪被 put 的参数 ID |

**`args_vector` 和 `incremented_put_arg_ids` 是指针（地址）传递，本质是输出参数。**

调用方代码（在 `submit_task` 中）：

```cython
# python/ray/_raylet.pyx:3496-3525
c_vector[unique_ptr[CTaskArg]] args_vector        # 局部值变量（栈上分配）
c_vector[CObjectID] incremented_put_arg_ids        # 局部值变量（栈上分配）
# ...
prepare_args_and_increment_put_refs(
    self, language, args, &args_vector, function_descriptor,    # & 取地址传指针
    &incremented_put_arg_ids)
```

通过指针修改调用方的局部变量，填充参数数据。

### 3.2 完整代码与逐行注释

```cython
cdef prepare_args_internal(
        CoreWorker core_worker,
        Language language, args,
        c_vector[unique_ptr[CTaskArg]] *args_vector,
        function_descriptor,
        c_vector[CObjectID] *incremented_put_arg_ids):
    cdef:
        size_t size
        int64_t put_threshold
        int64_t rpc_inline_threshold
        int64_t total_inlined
        shared_ptr[CBuffer] arg_data
        c_vector[CObjectID] inlined_ids
        c_string put_arg_call_site
        c_vector[CObjectReference] inlined_refs
        CAddress c_owner_address
        CRayStatus op_status
        optional[c_string] c_tensor_transport = NULL_TENSOR_TRANSPORT

    worker = ray._private.worker.global_worker
    # max_direct_call_object_size(): 单个参数的内联阈值（默认约 10KB~100KB）
    # 小于等于此值的对象直接嵌入 RPC 消息
    put_threshold = RayConfig.instance().max_direct_call_object_size()
    total_inlined = 0
    # task_rpc_inlined_bytes_limit(): 单次 RPC 消息中所有内联参数的总大小限制
    rpc_inline_threshold = RayConfig.instance().task_rpc_inlined_bytes_limit()
    serialization_context = worker.get_serialization_context()

    for arg in args:
        from ray.experimental.compiled_dag_ref import CompiledDAGRef
        if isinstance(arg, CompiledDAGRef):
            raise TypeError("CompiledDAGRef cannot be used as Ray task/actor argument.")

        # ====== 分支 1: 参数已经是 ObjectRef ======
        if isinstance(arg, ObjectRef):
            c_arg = (<ObjectRef>arg).native()
            op_status = CCoreWorkerProcess.GetCoreWorker().GetOwnerAddress(
                    c_arg, &c_owner_address)
            check_status(op_status)
            c_tensor_transport = (<ObjectRef>arg).c_tensor_transport()
            # 创建 CTaskArgByReference —— 按引用传递
            # 只传 ObjectID 和 owner 地址，不传实际数据
            args_vector.push_back(
                unique_ptr[CTaskArg](new CTaskArgByReference(
                    c_arg,
                    c_owner_address,
                    arg.call_site(),
                    move(c_tensor_transport))))
            c_tensor_transport = NULL_TENSOR_TRANSPORT

        # ====== 分支 2: 参数是普通 Python 对象，需要序列化 ======
        else:
            try:
                serialized_arg = serialization_context.serialize(arg)
            except TypeError as e:
                sio = io.StringIO()
                ray.util.inspect_serializability(arg, print_file=sio)
                msg = (
                    "Could not serialize the argument "
                    f"{repr(arg)} for a task or actor "
                    f"{function_descriptor.repr}:\n"
                    f"{sio.getvalue()}")
                raise TypeError(msg) from e

            metadata = serialized_arg.metadata
            if language != Language.PYTHON:
                metadata_fields = metadata.split(b",")
                if metadata_fields[0] not in [
                        ray_constants.OBJECT_METADATA_TYPE_CROSS_LANGUAGE,
                        ray_constants.OBJECT_METADATA_TYPE_RAW,
                        ray_constants.OBJECT_METADATA_TYPE_ACTOR_HANDLE]:
                    raise Exception("Can't transfer {} data to {}".format(
                        metadata_fields[0], language))
            size = serialized_arg.total_bytes

            if RayConfig.instance().record_ref_creation_sites():
                get_py_stack(&put_arg_call_site)

            # ====== 分支 2a: 小对象 → 内联到 task spec ======
            # 条件: size <= put_threshold AND 总内联大小未超限
            if <int64_t>size <= put_threshold and \
                    (<int64_t>size + total_inlined <= rpc_inline_threshold):
                # 在内存中分配 buffer
                arg_data = dynamic_pointer_cast[CBuffer, LocalMemoryBuffer](
                        make_shared[LocalMemoryBuffer](size))
                if size > 0:
                    (<SerializedObject>serialized_arg).write_to(
                        Buffer.make(arg_data))
                # 提取序列化对象中内嵌的 ObjectRef
                for object_ref in serialized_arg.contained_object_refs:
                    inlined_ids.push_back((<ObjectRef>object_ref).native())
                inlined_refs = (CCoreWorkerProcess.GetCoreWorker()
                                .GetObjectRefs(inlined_ids))
                # 创建 CTaskArgByValue —— 按值传递，数据嵌入 task spec
                args_vector.push_back(
                    unique_ptr[CTaskArg](new CTaskArgByValue(
                        make_shared[CRayObject](
                            arg_data, string_to_buffer(metadata),
                            inlined_refs))))
                inlined_ids.clear()
                total_inlined += <int64_t>size

            # ====== 分支 2b: 大对象 → put 到 Plasma Store ======
            else:
                # 将序列化对象 put 到 Plasma Store，获取 ObjectID
                put_id = CObjectID.FromBinary(
                        core_worker.put_serialized_object_and_increment_local_ref(
                            serialized_arg, c_tensor_transport, pin_object=True,
                            owner_address=None, inline_small_object=False))
                # 创建 CTaskArgByReference —— 按引用传递
                # task 只持有 ObjectID，实际数据在 Plasma Store
                args_vector.push_back(unique_ptr[CTaskArg](
                    new CTaskArgByReference(
                            put_id,
                            CCoreWorkerProcess.GetCoreWorker().GetRpcAddress(),
                            put_arg_call_site,
                            c_tensor_transport
                        )))
                # 记录此 put_id，出错时需要清理引用
                incremented_put_arg_ids.push_back(put_id)
```

### 3.3 三个关键阈值

| 阈值 | 配置方法 | 默认值 | 含义 |
|------|---------|-------|------|
| `put_threshold` | `RayConfig.max_direct_call_object_size()` | ~10KB-100KB | 单个参数的内联大小上限 |
| `rpc_inline_threshold` | `RayConfig.task_rpc_inlined_bytes_limit()` | ~1MB | 单次 RPC 中内联参数总大小上限 |
| `FUNCTION_SIZE_WARN_THRESHOLD` | `ray_constants.py:219` | 10MB | 函数 pickle 大小警告阈值 |

### 3.4 参数类型决策矩阵

| 条件 | 路径 | 参数类型 | 说明 |
|------|------|---------|------|
| `arg` 本身是 `ObjectRef` | 引用传递 | `CTaskArgByReference` | 直接传 ObjectID + owner 地址，不重新序列化 |
| `size <= put_threshold` 且 `total_inlined <= rpc_inline_threshold` | 值传递 | `CTaskArgByValue` | 数据直接嵌入 task spec 的 RPC 消息中 |
| `size > put_threshold` 或 `total_inlined 超限` | 引用传递 | `CTaskArgByReference` | 数据 put 到 Plasma Store，task 只持 ObjectID |
| 序列化对象包含内嵌 ObjectRef | 值传递 | `CTaskArgByValue`（含 inlined_refs） | 对象内嵌的 ObjectRef 会被提取出来 |


rpc_inline_threshold = RayConfig.instance().task_rpc_inlined_bytes_limit()
  # 默认值: 单次 RPC 消息中所有内联参数的总大小限制
  # 防止单个 task 的 RPC 消息过大

  ┌───────────────────────────────────────────────────────────────────────┬────────┬─────────────────────┬───────────────────────────────────────────────┐
  │                                 条件                                  │  路径  │      参数类型       │                     说明                      │
  ├───────────────────────────────────────────────────────────────────────┼────────┼─────────────────────┼───────────────────────────────────────────────┤
  │ size <= put_threshold 且 size + total_inlined <= rpc_inline_threshold │ 内联   │ CTaskArgByValue     │ 数据直接嵌入 task spec                        │
  ├───────────────────────────────────────────────────────────────────────┼────────┼─────────────────────┼───────────────────────────────────────────────┤
  │ size > put_threshold 或 total_inlined 超限                            │ Plasma │ CTaskArgByReference │ 数据 put 到 Plasma Store，task 只持 ObjectRef │
  ├───────────────────────────────────────────────────────────────────────┼────────┼─────────────────────┼───────────────────────────────────────────────┤
  │ arg 本身就是 ObjectRef                                                │ 引用   │ CTaskArgByReference │ 直接传递引用，不重新序列化                    │

### 3.5 错误处理机制

外层包装函数 `prepare_args_and_increment_put_refs` 中有异常清理逻辑：

```cython
# python/ray/_raylet.pyx:734
cdef prepare_args_and_increment_put_refs(
        CoreWorker core_worker,
        Language language, args,
        c_vector[unique_ptr[CTaskArg]] *args_vector, function_descriptor,
        c_vector[CObjectID] *incremented_put_arg_ids):
    try:
        prepare_args_internal(core_worker, language, args, args_vector,
                              function_descriptor, incremented_put_arg_ids)
    except Exception as e:
        # 清理: 对所有成功 put 到 Plasma 的对象调用 RemoveLocalReference
        for put_arg_id in dereference(incremented_put_arg_ids):
            CCoreWorkerProcess.GetCoreWorker().RemoveLocalReference(
                put_arg_id)
        raise e
```

**关键场景**：假设有 3 个参数，前 2 个是大对象已 put 到 Plasma Store（`incremented_put_arg_ids` 中有 2 个 ID），第 3 个参数序列化失败。此时必须清理前 2 个对象的本地引用，否则会造成内存泄漏。

### 3.6 `args_vector` 在 `submit_task` 中的最终去向

```cython
# python/ray/_raylet.pyx:3496-3550
c_vector[unique_ptr[CTaskArg]] args_vector  # 局部变量

prepare_args_and_increment_put_refs(
    self, language, args, &args_vector, function_descriptor,
    &incremented_put_arg_ids)

# args_vector 被 move 到 CTaskOptions
task_options = CTaskOptions(
    name, num_returns, c_resources, ...)

# C++ SubmitTask 接收 args_vector（通过值传递，所有权转移）
return_refs = CCoreWorkerProcess.GetCoreWorker().SubmitTask(
    ray_function, args_vector, task_options, ...)
```

注意 `args_vector` 在 `SubmitTask` 调用后被消耗（`unique_ptr` 所有权转移），后续不需要手动清理。

---

## 四、附录：关键源码文件索引

| 文件 | 关键内容 |
|------|---------|
| `python/ray/_common/utils.py:137` | `resources_from_ray_options` — Python 层资源解析 |
| `python/ray/_raylet.pyx:650` | `prepare_resources` — Cython 资源透传 |
| `python/ray/_raylet.pyx:751` | `prepare_args_internal` — 参数序列化与分类 |
| `python/ray/_raylet.pyx:3471` | `submit_task` — task 提交入口 |
| `src/ray/core_worker/core_worker.cc:2168` | `CoreWorker::SubmitTask` — C++ task 提交 |
| `src/ray/core_worker/lease_policy.cc:24` | `LocalityAwareLeasePolicy::GetBestNodeForLease` — 第 1 层调度 |
| `src/ray/core_worker/task_submission/normal_task_submitter.cc:34` | `NormalTaskSubmitter::SubmitTask` — task 队列管理 |
| `src/ray/core_worker/task_submission/normal_task_submitter.cc:274` | `RequestNewWorkerIfNeeded` — lease 请求发送 |
| `src/ray/raylet/node_manager.cc:1794` | `HandleRequestWorkerLease` — Raylet 接收 lease 请求 |
| `src/ray/raylet/scheduling/cluster_lease_manager.cc:47` | `QueueAndScheduleLease` — 入队并调度 |
| `src/ray/raylet/scheduling/cluster_lease_manager.cc:196` | `ScheduleAndGrantLeases` — 第 2 层调度核心 |
| `src/ray/raylet/scheduling/cluster_lease_manager.cc:422` | `ScheduleOnNode` — 本地执行 / spillback 决策 |
| `src/ray/raylet/scheduling/cluster_resource_scheduler.cc:151` | `GetBestSchedulableNode` — 节点选择算法 |
| `src/ray/raylet/scheduling/scheduler_resource_reporter.cc:59` | `FillResourceUsage` — 资源需求上报 |
| `src/ray/raylet/scheduling/internal.h:104` | `PrioritizeLocalNode` — grant_or_reject 影响调度偏好 |
| `src/ray/common/bundle_spec.cc:158` | `AddPlacementGroupConstraint` — PG bundle 约束注入 |
| `src/ray/common/task/task_spec.cc:58` | `ComputeResources` — SchedulingClass 计算 |
| `src/ray/gcs/gcs_autoscaler_state_manager.cc:324` | `GetPendingResourceRequests` — Pending Demands 数据源 |
| `src/ray/gcs/state_util.cc:22` | `FillAggregateLoad` — 聚合各节点资源负载 |
| `python/ray/autoscaler/v2/utils.py:635` | `_demand_report` — Pending Demands 展示 |
| `python/ray/autoscaler/_private/util.py:688` | `format_resource_demand_summary` — 需求格式化 |
| `src/ray/common/ray_config_def.h:706` | `locality_aware_leasing_enabled` — 局部性感知开关 |
