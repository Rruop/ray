# Ray 调度器 `scheduler_avoid_gpu_nodes` 配置详解

本文档详细描述 Ray 调度器中 `scheduler_avoid_gpu_nodes` 配置的含义、作用机制、资源不足判定逻辑，以及相关的核心代码实现。

---

## 一、配置含义

### 1.1 定义位置

**文件**: `src/ray/common/ray_config_def.h` (line 859)

```cpp
/// Whether to avoid scheduling cpu requests on gpu nodes
RAY_CONFIG(bool, scheduler_avoid_gpu_nodes, true)
```

- **类型**: `bool`
- **默认值**: `true`（默认开启）
- **环境变量覆盖**: `RAY_scheduler_avoid_gpu_nodes=0` 或 `RAY_scheduler_avoid_gpu_nodes=1`

### 1.2 核心思想

GPU 是稀缺资源。开启此配置后，调度器会**优先将纯 CPU 任务调度到非 GPU 节点上**，避免 CPU-only 任务抢占 GPU 节点资源。只有在非 GPU 节点资源不足时，才回退到 GPU 节点，从而为真正需要 GPU 的任务保留资源。

---

## 二、配置传递链路

### 2.1 `SchedulingOptions` — 配置载体

**文件**: `src/ray/raylet/scheduling/policy/scheduling_options.h`

配置值通过 `RayConfig::instance().scheduler_avoid_gpu_nodes()` 读取，存入 `SchedulingOptions` 的成员 `avoid_gpu_nodes_`。

**成员声明** (line 172):

```cpp
bool avoid_gpu_nodes_;
```

**构造函数** (lines 190-208):

```cpp
SchedulingOptions(
    SchedulingType type,
    float spread_threshold,
    bool avoid_local_node,
    bool require_node_available,
    bool avoid_gpu_nodes,                         // line 195
    std::shared_ptr<SchedulingContext> scheduling_context = nullptr,
    const std::string &preferred_node_id = std::string(),
    int32_t schedule_top_k_absolute = RayConfig::instance().scheduler_top_k_absolute(),
    float scheduler_top_k_fraction = RayConfig::instance().scheduler_top_k_fraction())
    : scheduling_type_(type),
      spread_threshold_(spread_threshold),
      avoid_local_node_(avoid_local_node),
      require_node_available_(require_node_available),
      avoid_gpu_nodes_(avoid_gpu_nodes),           // line 204
      scheduling_context_(std::move(scheduling_context)),
      preferred_node_id_(preferred_node_id),
      schedule_top_k_absolute_(schedule_top_k_absolute),
      scheduler_top_k_fraction_(scheduler_top_k_fraction) {}
```

### 2.2 各调度策略的工厂方法

不同调度策略在构造 `SchedulingOptions` 时，对 `avoid_gpu_nodes` 的处理方式不同：

#### 读取配置值（受 `scheduler_avoid_gpu_nodes` 控制）:

| 工厂方法 | 行号 | 传入值 |
|---|---|---|
| `Spread()` | 59 | `RayConfig::instance().scheduler_avoid_gpu_nodes()` |
| `Hybrid()` | 70 | `RayConfig::instance().scheduler_avoid_gpu_nodes()` |
| `AffinityWithBundle()` | 106 | `RayConfig::instance().scheduler_avoid_gpu_nodes()` |
| `NodeLabelScheduling()` | 119 | `RayConfig::instance().scheduler_avoid_gpu_nodes()` |

#### 硬编码为 `false`（不受配置控制）:

| 工厂方法 | 行号 | 传入值 |
|---|---|---|
| `Random()` | 50 | `false` |
| `BundlePack()` | 132 | `false` |
| `BundleSpread()` | 141 | `false` |
| `BundleStrictPack()` | 152 | `false` |
| `BundleStrictSpread()` | 164 | `false` |

---

## 三、核心调度逻辑 — Hybrid 策略

### 3.1 `NodeFilter` 枚举

**文件**: `src/ray/raylet/scheduling/policy/hybrid_scheduling_policy.h` (lines 65-73)

```cpp
enum class NodeFilter {
    /// Default scheduling.
    kAny,
    /// Schedule on GPU only nodes.
    kGPU,
    /// Schedule on nodes that don't have GPU. Since GPUs are more scarce resources, we need
    /// special handling for this.
    kNonGpu
};
```

### 3.2 `Schedule()` — 三阶段调度入口

**文件**: `src/ray/raylet/scheduling/policy/hybrid_scheduling_policy.cc` (lines 183-221)

```cpp
scheduling::NodeID HybridSchedulingPolicy::Schedule(
    const ResourceRequest &resource_request, SchedulingOptions options) {
  RAY_CHECK(options.scheduling_type_ == SchedulingType::HYBRID)
      << "HybridPolicy policy requires type = HYBRID";

  // 阶段 1: 如果不避免 GPU 节点，或者请求本身需要 GPU，直接在所有节点中选
  if (!options.avoid_gpu_nodes_ || resource_request.Has(ResourceID::GPU())) {
    return ScheduleImpl(resource_request,
                        options.spread_threshold_,
                        options.avoid_local_node_,
                        options.require_node_available_,
                        NodeFilter::kAny,
                        options.preferred_node_id_,
                        options.schedule_top_k_absolute_,
                        options.scheduler_top_k_fraction_);
  }

  // 阶段 2: 只在非 GPU 节点中尝试调度
  auto best_node_id = ScheduleImpl(resource_request,
                                   options.spread_threshold_,
                                   options.avoid_local_node_,
                                   /*require_node_available*/ true,   // ← 注意: 硬编码为 true
                                   NodeFilter::kNonGpu,
                                   options.preferred_node_id_,
                                   options.schedule_top_k_absolute_,
                                   options.scheduler_top_k_fraction_);
  if (!best_node_id.IsNil()) {
    return best_node_id;
  }

  // 阶段 3: 非 GPU 节点资源不足，回退到所有节点（含 GPU 节点）
  return ScheduleImpl(resource_request,
                      options.spread_threshold_,
                      options.avoid_local_node_,
                      options.require_node_available_,
                      NodeFilter::kAny,
                      options.preferred_node_id_,
                      options.schedule_top_k_absolute_,
                      options.scheduler_top_k_fraction_);
}
```

**调度流程总结**:

```
请求需要 GPU?
  └─ 是 → 直接在所有节点中选 (kAny)
  └─ 否 → avoid_gpu_nodes 开启?
            └─ 否 → 直接在所有节点中选 (kAny)
            └─ 是 → 第一轮: 只在非 GPU 节点中选 (kNonGpu, require_node_available=true)
                     └─ 找到合适节点 → 返回该节点
                     └─ 未找到 (返回 Nil) → 第二轮: 在所有节点中选 (kAny) — 回退
```

### 3.3 `IsNodeFeasible()` — 节点可行性过滤

**文件**: `src/ray/raylet/scheduling/policy/hybrid_scheduling_policy.cc` (lines 23-42)

```cpp
bool HybridSchedulingPolicy::IsNodeFeasible(
    const scheduling::NodeID &node_id,
    const NodeFilter &node_filter,
    const NodeResources &node_resources,
    const ResourceRequest &resource_request) const {
  // 检查 1: 节点是否存活
  if (!is_node_alive_(node_id)) {
    return false;
  }

  // 检查 2: GPU 过滤器匹配
  if (node_filter != NodeFilter::kAny) {
    const bool has_gpu = node_resources.total.Has(ResourceID::GPU());
    if (node_filter == NodeFilter::kGPU && !has_gpu) {
      return false;           // 需要 GPU 节点但该节点没有 GPU
    } else if (node_filter == NodeFilter::kNonGpu && has_gpu) {
      return false;           // 需要 非 GPU 节点但该节点有 GPU
    }
  }

  // 检查 3: 节点总容量是否足够
  return node_resources.IsFeasible(resource_request);
}
```

三重检查:
1. **节点存活** — `is_node_alive_`
2. **GPU 过滤** — 根据 `NodeFilter` 判断节点是否匹配
3. **总容量检查** — `NodeResources::IsFeasible`

---

## 四、资源不足判定机制

调度器通过两个层面的检查来判断节点是否"资源不足"：

### 4.1 `IsFeasible` — 总容量是否够

**文件**: `src/ray/common/scheduling/cluster_resource_data.cc` (lines 94-100)

```cpp
bool NodeResources::IsFeasible(const ResourceRequest &resource_request) const {
  const auto &label_selector = resource_request.GetLabelSelector();
  if (!HasRequiredLabels(label_selector)) {
    return false;
  }
  return this->total >= resource_request.GetResourceSet();
}
```

比较节点的**总资源** (`total`) 是否 >= 请求资源。不管当前已用多少，只看节点物理上是否装得下这个任务。同时检查标签选择器是否匹配。

### 4.2 `IsAvailable` — 当前是否有空闲

**文件**: `src/ray/common/scheduling/cluster_resource_data.cc` (lines 78-92)

```cpp
bool NodeResources::IsAvailable(const ResourceRequest &resource_request,
                                bool ignore_pull_manager_at_capacity) const {
  // 检查 1: pull manager 是否已满
  if (!ignore_pull_manager_at_capacity && resource_request.RequiresObjectStoreMemory() &&
      object_pulls_queued) {
    RAY_LOG(DEBUG) << "At pull manager capacity";
    return false;
  }

  // 检查 2: 标签选择器是否匹配
  const auto &label_selector = resource_request.GetLabelSelector();
  if (!HasRequiredLabels(label_selector)) {
    return false;
  }

  // 检查 3: 可用资源是否 >= 请求资源
  return this->available >= resource_request.GetResourceSet();
}
```

比较节点的**可用资源** (`available`) 是否 >= 请求资源。还额外检查:
- **pull manager 容量**: 如果对象存储内存请求排队已满，拒绝调度
- **标签匹配**: 节点必须具备请求所要求的标签

### 4.3 核心比较逻辑 — `operator>=`

**文件**: `src/ray/common/scheduling/resource_set.cc` (lines 219-226)

```cpp
bool NodeResourceSet::operator>=(const ResourceSet &other) const {
  for (auto &entry : other.Resources()) {
    if (Get(entry.first) < entry.second) {
      return false;
    }
  }
  return true;
}
```

逐项遍历请求中的每种资源（CPU、GPU、MEM 等），只要**任意一种**资源的节点持有量 < 请求量，就判定为不满足。

**缺失资源的默认值** (lines 232-238):

```cpp
FixedPoint NodeResourceSet::ResourceDefaultValue(ResourceID resource_id) const {
  if (resource_id.IsImplicitResource()) {
    return FixedPoint(1);   // 隐式资源 (如 object_store_memory) 默认为 1
  } else {
    return FixedPoint(0);   // 显式资源 (如 CPU, GPU) 默认为 0
  }
}
```

### 4.4 `IsFeasible` vs `IsAvailable` 对比

| 维度 | `IsFeasible` | `IsAvailable` |
|---|---|---|
| 比较对象 | `total` (节点总容量) | `available` (当前空闲) |
| 含义 | 节点物理上能否容纳该任务 | 当前是否有足够空闲资源立即运行 |
| 额外检查 | 标签选择器 | 标签选择器 + pull manager 容量 |
| 用途 | 过滤不可能的节点 | 判断是否能立即调度 |

---

## 五、`ScheduleImpl` — 节点评估与选择

### 5.1 节点分类

**文件**: `src/ray/raylet/scheduling/policy/hybrid_scheduling_policy.cc` (lines 96-181)

```cpp
scheduling::NodeID HybridSchedulingPolicy::ScheduleImpl(
    const ResourceRequest &resource_request,
    float spread_threshold,
    bool force_spillback,
    bool require_node_available,
    NodeFilter node_filter,
    const std::string &preferred_node,
    int32_t schedule_top_k_absolute,
    float scheduler_top_k_fraction) {

  // 两个桶: 可用节点 和 可行但当前不可用节点
  std::vector<std::pair<scheduling::NodeID, float>> available_nodes;
  std::vector<std::pair<scheduling::NodeID, float>> feasible_and_unavailable_nodes;

  bool preferred_node_is_available = false;
  bool preferred_node_is_feasible = false;
  scheduling::NodeID preferred_node_id = local_node_id_;

  // ... 确定 preferred_node_id ...

  for (const auto &pair : nodes_) {
    const auto &node_id = pair.first;
    const auto &node_resources = pair.second.GetLocalView();

    if (force_spillback && node_id == preferred_node_id) {
      continue;
    }

    if (IsNodeFeasible(node_id, node_filter, node_resources, resource_request)) {
      bool ignore_pull_manager_at_capacity = false;
      if (node_id == preferred_node_id) {
        // 本地节点的 pull manager 满了也没关系，任务可以后续溢出
        ignore_pull_manager_at_capacity = true;
        preferred_node_is_feasible = true;
      }

      bool is_available =
          node_resources.IsAvailable(resource_request, ignore_pull_manager_at_capacity);

      if (node_id == preferred_node_id && is_available) {
        preferred_node_is_available = true;
      }

      // 计算节点评分 (关键资源利用率)
      float node_score = ComputeNodeScoreImpl(node_resources, spread_threshold);

      if (is_available) {
        available_nodes.push_back({node_id, node_score});
      } else {
        feasible_and_unavailable_nodes.push_back({node_id, node_score});
      }
    }
  }

  // 选择优先级: available_nodes > feasible_and_unavailable_nodes (仅当不要求 available 时) > Nil
  size_t num_candidate_nodes =
      std::max<int32_t>(schedule_top_k_absolute,
                        static_cast<int32_t>(nodes_.size() * scheduler_top_k_fraction));

  if (!available_nodes.empty()) {
    // 优先从 available 节点中选
    return GetBestNode(available_nodes, num_candidate_nodes,
                       /* preferred_node 优先处理 */);
  } else if (!feasible_and_unavailable_nodes.empty() && !require_node_available) {
    // 无 available 节点但允许不可用节点时，从 feasible 但 unavailable 节点中选
    return GetBestNode(feasible_and_unavailable_nodes, num_candidate_nodes,
                       /* preferred_node 优先处理 */);
  } else {
    // 两个桶都空，或要求 available 但没有 available 节点 → 返回 Nil
    return scheduling::NodeID::Nil();
  }
}
```

节点被分为三类:

| 检查结果 | 含义 | 归入哪个桶 |
|---|---|---|
| `IsFeasible == false` | 总容量都不够，节点根本装不下 | **跳过** |
| `IsFeasible == true && IsAvailable == true` | 容量够且当前有空闲 | `available_nodes` |
| `IsFeasible == true && IsAvailable == false` | 容量够但当前资源被占满 | `feasible_and_unavailable_nodes` |

### 5.2 节点评分 — `ComputeNodeScoreImpl`

```cpp
float ComputeNodeScoreImpl(const NodeResources &node_resources, float spread_threshold) {
  float critical_resource_utilization =
      node_resources.CalculateCriticalResourceUtilization();
  if (critical_resource_utilization < spread_threshold) {
    critical_resource_utilization = 0;
  }
  return critical_resource_utilization;
}
```

评分基于 **关键资源利用率** (`CalculateCriticalResourceUtilization`):

**文件**: `src/ray/common/scheduling/cluster_resource_data.cc` (lines 62-76)

```cpp
float NodeResources::CalculateCriticalResourceUtilization() const {
  float highest = 0;
  for (const auto &i : {CPU, MEM, OBJECT_STORE_MEM}) {
    const auto &cur_total = this->total.Get(ResourceID(i));
    if (cur_total == 0) {
      continue;
    }
    auto cur_available = this->available.Get(ResourceID(i)).Double();
    float utilization = 1 - (cur_available / cur_total.Double());
    if (utilization > highest) {
      highest = utilization;
    }
  }
  return highest;
}
```

计算 CPU、内存、对象存储内存三者的最高利用率。如果低于 `spread_threshold` 则截断为 0（视为同等优先）。**评分越低 = 负载越轻 = 越优先选择**。

### 5.3 `GetBestNode` — 最终选择

**文件**: `src/ray/raylet/scheduling/policy/hybrid_scheduling_policy.cc` (lines 62-94)

```cpp
scheduling::NodeID HybridSchedulingPolicy::GetBestNode(
    std::vector<std::pair<scheduling::NodeID, float>> &node_scores,
    size_t num_candidate_nodes,
    std::optional<scheduling::NodeID> preferred_node_id,
    float preferred_node_score) const {
  RAY_CHECK(!node_scores.empty());
  RAY_CHECK(num_candidate_nodes >= 1);

  // 1. 先按 NodeID 排序，保证平局时顺序确定
  std::sort(node_scores.begin(), node_scores.end(),
            [](const auto &a, const auto &b) { return a.first < b.first; });

  // 2. 按评分稳定排序 (最低评分 = 最优先)
  std::stable_sort(node_scores.begin(), node_scores.end(),
            [](const auto &a, const auto &b) { return a.second < b.second; });

  // 3. 如果 preferred_node 评分与最优节点持平，优先选 preferred_node
  if (preferred_node_id.has_value()) {
    if (preferred_node_score <= node_scores.front().second) {
      return preferred_node_id.value();
    }
  }

  // 4. 从 top-K 候选节点中随机选一个 (K = num_candidate_nodes)
  size_t node_index = absl::Uniform<size_t>(
      bitgenref_, 0u, std::min(num_candidate_nodes, node_scores.size()));
  return node_scores[node_index].first;
}
```

选择算法:
1. 按 NodeID 排序（确定性平局打破）
2. 按评分稳定排序（最低评分最优，相同评分保持 ID 顺序）
3. preferred_node 评分与最优持平时，优先返回 preferred_node
4. 否则从 top-K 候选中**随机**选一个

---

## 六、`ScheduleImpl` 返回 `Nil` 的条件

**文件**: `src/ray/common/scheduling/scheduling_ids.h` (lines 113-115)

```cpp
bool IsNil() const { return id_ == -1; }
static BaseSchedulingID Nil() { return BaseSchedulingID(-1); }
```

`ScheduleImpl` 返回 `NodeID::Nil()` 的条件:

```
available_nodes 为空
  AND
(
    feasible_and_unavailable_nodes 也为空 (没有任何节点容量够)
    OR
    require_node_available == true (要求当前可用，但所有可行节点资源已被占满)
)
```

在 `Schedule()` 方法的第一轮 `kNonGpu` 调用中，`require_node_available` 被硬编码为 `true`，所以:

- 如果所有非 GPU 节点 **不可行**（总容量不够） → Nil
- 如果所有非 GPU 节点 **可行但不可用**（资源被占满）→ Nil
- 只要有一个非 GPU 节点可行且可用 → 返回该节点

返回 Nil 后，触发第二轮 `kAny` 回退调度。

---

## 七、AffinityWithBundle 策略 — Placement Group 亲和调度

### 7.1 `IsNodeFeasibleAndAvailable`

**文件**: `src/ray/raylet/scheduling/policy/affinity_with_bundle_scheduling_policy.cc` (lines 20-42)

```cpp
bool AffinityWithBundleSchedulingPolicy::IsNodeFeasibleAndAvailable(
    const scheduling::NodeID &node_id,
    const ResourceRequest &resource_request,
    bool avoid_gpu_nodes) {
  // 基本检查: 节点存在、存活、可行、可用
  if (!(nodes_.contains(node_id) && is_node_alive_(node_id) &&
        nodes_.at(node_id).GetLocalView().IsFeasible(resource_request) &&
        nodes_.at(node_id).GetLocalView().IsAvailable(resource_request))) {
    return false;
  }
  if (!avoid_gpu_nodes) {
    return true;
  }

  // GPU 避免逻辑: 只对没有指定具体 bundle 的 PG 级别亲和生效
  // 检查节点是否含有 PG 的 GPU 通配资源 (GPU_group_<pg_id>)
  std::string gpu_wildcard_resource_name =
      "GPU_group_" +
      GetGroupIDFromResource(resource_request.ResourceIds().begin()->Binary());

  const auto &node_total = nodes_.at(node_id).GetLocalView().total;
  return !node_total.Has(scheduling::ResourceID(gpu_wildcard_resource_name));
}
```

### 7.2 `Schedule()` 方法

**文件**: `src/ray/raylet/scheduling/policy/affinity_with_bundle_scheduling_policy.cc` (lines 44-86)

```cpp
scheduling::NodeID AffinityWithBundleSchedulingPolicy::Schedule(
    const ResourceRequest &resource_request, SchedulingOptions options) {
  // ... 确定 target_node_id ...

  if (bundle_id.second != -1) {
    // 指定了具体 bundle index → 跳过 GPU 避免，直接调度到 bundle 所在节点
    if (IsNodeFeasibleAndAvailable(
            target_node_id, resource_request, /*avoid_gpu_nodes=*/false)) {
      return target_node_id;
    }
  } else {
    // 仅 PG 级别亲和 (无具体 bundle)
    if (options.avoid_gpu_nodes_) {
      // 第一轮: 尝试排除 GPU 节点
      if (IsNodeFeasibleAndAvailable(
              target_node_id, resource_request, /*avoid_gpu_nodes=*/true)) {
        return target_node_id;
      }
    }
    // 第二轮回退: 不排除 GPU 节点
    if (IsNodeFeasibleAndAvailable(
            target_node_id, resource_request, /*avoid_gpu_nodes=*/false)) {
      return target_node_id;
    }
  }
  return scheduling::NodeID::Nil();
}
```

PG 亲和调度的 GPU 避免逻辑:
- **指定了具体 bundle index**: 跳过 GPU 避免，直接调度到 bundle 所在节点
- **仅 PG 级别亲和**: 先尝试排除含 `GPU_group_<pg_id>` 通配资源的节点；不行再回退

---

## 八、Random 策略 — 断言强制不使用 GPU 避免

**文件**: `src/ray/raylet/scheduling/policy/random_scheduling_policy.cc` (lines 32-37)

```cpp
RAY_CHECK(options.spread_threshold_ == 0 && !options.avoid_local_node_ &&
          options.require_node_available_ && !options.avoid_gpu_nodes_)
    << "Random policy requires spread_threshold = 0, "
    << "avoid_local_node = false, "
    << "require_node_available = true, "
    << "avoid_gpu_nodes = false.";
```

Random 策略通过断言强制 `avoid_gpu_nodes_` 必须为 `false`，因为随机调度不做 GPU 区分。

---

## 九、各策略行为总结

| 调度策略 | `avoid_gpu_nodes` 行为 |
|---|---|
| **Hybrid** (主路径) | 纯 CPU 请求优先调度到非 GPU 节点；找不到再回退到 GPU 节点；GPU 请求不受影响 |
| **Spread** | 同 Hybrid 逻辑（读取 config 值） |
| **AffinityWithBundle** | PG 级亲和时优先选非 GPU bundle 节点；具体 bundle 亲和时不做过滤 |
| **NodeLabelScheduling** | 读取 config 值 |
| **Random** | 硬编码 `false`，且有断言强制 |
| **Bundle 系列** | 硬编码 `false`，不区分 GPU |

---

## 十、`LeastResourceScorer` — 其他策略的评分器

**文件**: `src/ray/raylet/scheduling/policy/scorer.cc` (lines 20-46)

```cpp
double LeastResourceScorer::Score(const ResourceRequest &required_resources,
                                  const NodeResources &node_resources) {
  if (!node_resources.IsAvailable(required_resources)) {
    return -1.;  // 不可用 → 返回 -1 (排除)
  }

  double node_score = 0.;
  for (auto &resource_id : required_resources.ResourceIds()) {
    const auto &request_resource = required_resources.Get(resource_id);
    const auto &node_available_resource = node_resources.available.Get(resource_id);
    node_score += Calculate(request_resource, node_available_resource);
  }
  return node_score;
}

double LeastResourceScorer::Calculate(const FixedPoint &requested,
                                      const FixedPoint &available) {
  if (available == 0) {
    return 0;
  }
  return (available - requested).Double() / available.Double();
}
```

与 Hybrid 策略的 `ComputeNodeScoreImpl` 不同:
- **Hybrid**: 用关键资源利用率（越低越好），低于 `spread_threshold` 视为同等优先
- **LeastResourceScorer**: 用剩余资源比例（越高越好），用于 Spread/Random/NodeAffinity 等策略

---

## 十一、测试验证

### 11.1 C++ 单元测试

**文件**: `src/ray/raylet/scheduling/policy/tests/scheduling_policy_test.cc`

**`AvoidSchedulingCPURequestsOnGPUNodes`** (lines 378-423):
- 本地节点有 GPU，远程节点没有 GPU
- CPU-only 请求 + `avoid_gpu_nodes=true` → 调度到远程（非 GPU 节点）
- GPU 请求 → 调度到本地（GPU 节点）
- CPU 请求 + `avoid_gpu_nodes=false` → 调度到远程（按负载选择，非 GPU 过滤无关）
- CPU+GPU 混合请求 → 调度到本地（GPU 节点）

**`SchedulenCPURequestsOnGPUNodeAsALastResort`** (lines 425-438):
- 本地节点无可用 CPU，远程节点有 GPU + CPU
- CPU-only 请求 + `avoid_gpu_nodes=true` → 调度到远程（GPU 节点，因为非 GPU 节点资源不足，触发回退）

**`NonGpuNodePreferredSchedulingTest`** (lines 469-519):
- 验证多个场景下优先调度到非 GPU 节点

### 11.2 Python 集成测试

**文件**: `python/ray/tests/test_scheduling.py` (lines 494-541)

```python
@pytest.mark.skipif(sys.platform == "win32", reason="Fails on windows")
def test_gpu(monkeypatch):
    monkeypatch.setenv("RAY_scheduler_avoid_gpu_nodes", "1")
    n = 5
    cluster, cpu_node_ids, gpu_node_ids = build_cluster(n, n)
    # ...
    # GPU 任务 (num_gpus=0.5) 启动后，内部启动 CPU-only 任务/Actor
    # 验证:
    #   - launcher_id in gpu_node_ids    (GPU 任务调度到 GPU 节点)
    #   - all CPU task/actor IDs in cpu_node_ids  (CPU 任务避开 GPU 节点)
```

---

## 十二、相关文件索引

| # | 文件路径 | 作用 |
|---|---|---|
| 1 | `src/ray/common/ray_config_def.h` | 配置定义 (line 859, 默认 `true`) |
| 2 | `src/ray/raylet/scheduling/policy/scheduling_options.h` | 配置传递: `avoid_gpu_nodes_` 成员、构造函数、工厂方法 |
| 3 | `src/ray/raylet/scheduling/policy/hybrid_scheduling_policy.h` | `NodeFilter` 枚举、`ScheduleImpl` 声明 |
| 4 | `src/ray/raylet/scheduling/policy/hybrid_scheduling_policy.cc` | 核心逻辑: 三阶段调度、`IsNodeFeasible`、`ScheduleImpl`、`GetBestNode` |
| 5 | `src/ray/raylet/scheduling/policy/affinity_with_bundle_scheduling_policy.cc` | PG 亲和调度的 GPU 避免逻辑 |
| 6 | `src/ray/raylet/scheduling/policy/random_scheduling_policy.cc` | 断言 `avoid_gpu_nodes` 必须为 `false` |
| 7 | `src/ray/common/scheduling/cluster_resource_data.h` | `NodeResources` 类: `IsAvailable`/`IsFeasible`/`CalculateCriticalResourceUtilization` 声明 |
| 8 | `src/ray/common/scheduling/cluster_resource_data.cc` | `IsAvailable` (line 78)、`IsFeasible` (line 94)、`CalculateCriticalResourceUtilization` (line 62) 实现 |
| 9 | `src/ray/common/scheduling/resource_set.cc` | `NodeResourceSet::operator>=` (line 219) — 核心资源比较逻辑 |
| 10 | `src/ray/common/scheduling/scheduling_ids.h` | `NodeID::Nil()` 和 `IsNil()` 定义 (lines 113-115) |
| 11 | `src/ray/raylet/scheduling/policy/scorer.cc` | `LeastResourceScorer` — 非 Hybrid 策略的评分器 |
| 12 | `src/ray/raylet/scheduling/policy/tests/scheduling_policy_test.cc` | C++ 单元测试 |
| 13 | `src/ray/raylet/scheduling/policy/tests/hybrid_scheduling_policy_test.cc` | C++ 单元测试 |
| 14 | `python/ray/tests/test_scheduling.py` | Python 集成测试 `test_gpu` (line 496) |
