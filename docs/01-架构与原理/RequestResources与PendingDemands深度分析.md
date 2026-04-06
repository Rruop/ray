# Ray Autoscaler: request_resources 与 Pending Demands 完整代码逻辑分析

## 概述

本文档完整分析 Ray `/api/cluster_status` 返回中 `From request_resources` 和 `Pending Demands` 两条信息的完整数据链路、代码逻辑、格式化过程，以及两者的本质区别。

### 典型输出示例

```
Resources
--------
Total Usage:
19.25/1619.0 CPU
0.0/2.0 GPU
487.11MiB/1.99TiB memory

From request_resources:
{'memory': 217258927.0, 'node:10.53.80.206': 0.0, 'object_store_memory': 0.0, 'CPU': 0.0}: 3 from request_resources()

Pending Demands:
{'CPU': 1.0}: 135+ pending tasks/actors
```

---

## 一、两者的本质区别

| 维度 | From request_resources | Pending Demands |
|------|----------------------|-----------------|
| **数据来源** | 用户/框架通过 SDK **主动声明**的集群资源约束 | Raylet 自动上报的**无法调度的 task/actor 队列** |
| **语义** | "我希望集群至少拥有这些资源" | "我有这些 task/actor 正在排队等资源" |
| **触发方式** | `ray.autoscaler.sdk.request_resources()` 调用 | Raylet 调度器发现 task 无法被调度时自动上报 |
| **存储位置** | GCS 的 `cluster_resource_constraint_` 字段 | GCS 从各 raylet 聚合的 `aggregate_load` |
| **Autoscaler 行为** | 尝试将集群扩展到约束指定的 shape | 尝试启动新节点来满足 pending tasks |
| **是否可覆盖** | 最新一次调用**覆盖**之前的 | 持续累积，随调度状态变化 |
| **格式后缀** | `: N from request_resources()` | `: N+ pending tasks/actors` |

---

## 二、From request_resources — 完整数据链路

### 第1步：Ray Data `try_trigger_scaling()` 生成 bundles

**文件**: `python/ray/data/_internal/cluster_autoscaler/default_cluster_autoscaler_v2.py:195-248`

```python
def try_trigger_scaling(self):
    self._resource_utilization_calculator.observe()

    # 频率限制
    now = time.time()
    if now - self._last_request_time < self._min_gap_between_autoscaling_requests_s:
        return

    util = self._resource_utilization_calculator.get()
    if (
        util.cpu < self._cluster_scaling_up_util_threshold
        and util.gpu < self._cluster_scaling_up_util_threshold
        and util.object_store_memory < self._cluster_scaling_up_util_threshold
    ):
        # ============ 低利用率分支: 发送当前已分配资源 ============
        curr_resources = self._autoscaling_coordinator.get_allocated_resources(
            requester_id=self._requester_id
        )
        self._send_resource_request(curr_resources)
        return

    # ============ 高利用率分支: 构建节点规格 bundles ============
    active_bundles = []   # 已有节点的规格 (must include)
    pending_bundles = []  # 需要扩容的节点规格 (best-effort)
    node_resource_spec_count = self._get_node_counts()
    for node_resource_spec, count in node_resource_spec_count.items():
        bundle = node_resource_spec.to_bundle()  # ← 只含 CPU/GPU/memory
        active_bundles.extend([bundle] * count)
        pending_bundles.extend([bundle] * self._cluster_scaling_up_delta)

    resource_request = cap_resource_request_to_limits(
        active_bundles, pending_bundles, self._resource_limits
    )
    self._send_resource_request(resource_request)
```

#### `_NodeResourceSpec.to_bundle()` — 只输出 CPU/GPU/memory

**文件**: `default_cluster_autoscaler_v2.py:65-66`

```python
def to_bundle(self):
    return {"CPU": self.cpu, "GPU": self.gpu, "memory": self.mem}
```

**关键**: `to_bundle()` 不包含 `node:<IP>`、`object_store_memory` 等资源，只输出 CPU/GPU/memory 三个维度。

#### `_get_node_resource_spec_and_count()` — 获取集群节点规格

**文件**: `default_cluster_autoscaler_v2.py:69-95`

```python
def _get_node_resource_spec_and_count() -> Dict[_NodeResourceSpec, int]:
    nodes_resource_spec_count = defaultdict(int)

    # 从集群配置获取节点组规格
    cluster_config = ray._private.state.state.get_cluster_config()
    if cluster_config and cluster_config.node_group_configs:
        for node_group_config in cluster_config.node_group_configs:
            if not node_group_config.resources or node_group_config.max_count == 0:
                continue
            node_resource_spec = _NodeResourceSpec.from_bundle(
                node_group_config.resources
            )
            nodes_resource_spec_count[node_resource_spec] = 0

    # 从 ray.nodes() 获取实际节点，过滤掉 head 节点
    node_resources = [
        node["Resources"]
        for node in ray.nodes()
        if node["Alive"] and "node:__internal_head__" not in node["Resources"]
    ]

    for r in node_resources:
        node_resource_spec = _NodeResourceSpec.from_bundle(r)  # ← 只提取 CPU/GPU/memory
        nodes_resource_spec_count[node_resource_spec] += 1

    return nodes_resource_spec_count
```

**注意**: `_NodeResourceSpec.from_bundle()` 也只提取 CPU/GPU/memory，忽略 `node:<IP>` 等资源。

### 第2步：`_send_resource_request()` → Coordinator Actor

**文件**: `default_cluster_autoscaler_v2.py:281-289`

```python
def _send_resource_request(self, resource_request):
    self._autoscaling_coordinator.request_resources(
        requester_id=self._requester_id,
        resources=resource_request,
        expire_after_s=self.AUTOSCALING_REQUEST_EXPIRE_TIME_S,
        request_remaining=True,  # ← 关键! 声明要分得剩余资源
    )
    self._last_request_time = time.time()
```

`requester_id` 格式为 `f"data-{execution_id}"`，其中 `execution_id = f"{dataset_name}_{uuid}_{run_index}"`。

### 第3步：`_AutoscalingCoordinatorActor.request_resources()`

**文件**: `python/ray/data/_internal/cluster_autoscaler/default_autoscaling_coordinator.py:281-324`

```python
def request_resources(self, requester_id, resources, expire_after_s,
                      request_remaining=False, priority=MEDIUM):
    with self._lock:
        # ① 向上取整——SDK 只接受整数值
        for r in resources:
            for k in r:
                r[k] = math.ceil(r[k])

        now = self._get_current_time()
        old_req = self._ongoing_reqs.get(requester_id)
        if old_req is not None:
            # 同一 requester_id 覆盖旧请求
            old_req.requested_resources = resources
            old_req.expiration_time = now + expire_after_s
        else:
            self._ongoing_reqs[requester_id] = OngoingRequest(
                first_request_time=now,
                requested_resources=resources,
                request_remaining=request_remaining,
                priority=priority.value,
                expiration_time=now + expire_after_s,
                allocated_resources=[],
            )

        self._merge_and_send_requests()      # ← 合并发给 Ray Autoscaler
        self._reallocate_resources()           # ← 重新分配集群资源给请求者
```

### 第4步：`_merge_and_send_requests()` → SDK

**文件**: `default_autoscaling_coordinator.py:346-352`

```python
def _merge_and_send_requests(self):
    self._purge_expired_requests()
    merged_req = []
    for req in self._ongoing_reqs.values():
        merged_req.extend(req.requested_resources)
    self._send_resources_request(merged_req)
    # → ray.autoscaler.sdk.request_resources(bundles=merged_req)
```

### 第5步：SDK `request_resources()` → GCS RPC

**文件**: `python/ray/autoscaler/_private/commands.py:186-233`

```python
def request_resources(num_cpus=None, bundles=None, bundle_label_selectors=None):
    to_request = []
    if bundles:
        for i, bundle in enumerate(bundles):
            to_request.append({"resources": bundle, "label_selector": {}})

    if is_autoscaler_v2():
        from ray.autoscaler.v2.sdk import request_cluster_resources
        gcs_address = internal_kv_get_gcs_client().address
        request_cluster_resources(gcs_address, to_request)
```

**文件**: `python/ray/autoscaler/v2/sdk.py:22-78`

```python
def request_cluster_resources(gcs_address, to_request, timeout=10):
    # 按形状分组——相同形状的 bundle 合并计数
    def keyfunc(r):
        return (
            frozenset(r.resources.items()),
            frozenset(r.label_selector.items()),
        )

    grouped_requests = Counter(keyfunc(r) for r in to_request)

    bundles, label_selectors, counts = [], [], []
    for (bundle, selector), count in grouped_requests.items():
        bundles.append(dict(bundle))
        counts.append(count)  # ← 这就是 *N 中 N 的来源

    GcsClient(gcs_address).request_cluster_resource_constraint(
        bundles, label_selectors, counts, timeout_s=timeout
    )
```

### 第6步：GCS 存储 cluster_resource_constraint

**文件**: `src/ray/gcs/gcs_autoscaler_state_manager.cc:134-145`

```cpp
void GcsAutoscalerStateManager::HandleRequestClusterResourceConstraint(
    rpc::autoscaler::RequestClusterResourceConstraintRequest request, ...) {
  cluster_resource_constraint_ =
      std::move(*request.mutable_cluster_resource_constraint());
  // 直接覆盖存储
}
```

### 第7步：展示时 — GCS 读取并格式化

**文件**: `src/ray/gcs/gcs_autoscaler_state_manager.cc:261-268`

```cpp
void GcsAutoscalerStateManager::GetClusterResourceConstraints(
    rpc::autoscaler::ClusterResourceState *state) {
  if (cluster_resource_constraint_.has_value()) {
    state->add_cluster_resource_constraints()->CopyFrom(
        cluster_resource_constraint_.value());
  }
}
```

**文件**: `python/ray/autoscaler/v2/utils.py:759-808` — 解析

```python
def _parse_resource_demands(cls, state):
    # ...
    for constraint_request in state.cluster_resource_constraints:
        demand = ClusterConstraintDemand(
            bundles_by_count=[
                ResourceRequestByCount(
                    bundle=dict(r.request.resources_bundle.items()), count=r.count
                )
                for r in constraint_request.resource_requests
            ]
        )
        constraint_demand.append(demand)
```

**文件**: `python/ray/autoscaler/v2/utils.py:599-632` — 格式化

```python
@staticmethod
def _constraint_report(cluster_constraint_demand):
    constraint_lines = []
    request_demand = [
        (bc.bundle, bc.count)
        for constraint_demand in cluster_constraint_demand
        for bc in constraint_demand.bundles_by_count
    ]
    for bundle, count in request_demand:
        constraint_lines.append(f" {bundle}: {count} from request_resources()")
    if constraint_lines:
        return "\n".join(constraint_lines)
    return " (none)"
```

### 第8步：Infeasible 消息格式化

**文件**: `python/ray/autoscaler/v2/event_logger.py:166-185`

```python
if infeasible_cluster_resource_constraints:
    for infeasible_constraint in infeasible_cluster_resource_constraints:
        log_str = "No available node types can fulfill cluster constraint: "
        for i, requests_by_count in enumerate(
            infeasible_constraint.resource_requests
        ):
            resource_map = ResourceRequestUtil.to_resource_map(
                requests_by_count.request
            )
            log_str += f"{resource_map}*{requests_by_count.count}"
            #                   ↑              ↑
            #        资源字典        该形状的重复次数 (*N)
            if i < len(infeasible_constraint.resource_requests) - 1:
                log_str += ", "
```

---

## 三、Pending Demands — 完整数据链路

### 第1步：Raylet 调度器自动上报 pending task/actor

**文件**: `src/ray/raylet/scheduling/scheduler_resource_reporter.cc:100-130`

Raylet 在每个调度周期统计无法调度的 task/actor，按资源 shape 分组上报：

```cpp
void fill_resource_usage_helper(...) {
  for (const auto &resource : resources) {
    (*by_shape_entry->mutable_shape())[label] = quantity;
  }
  if (is_infeasible) {
    by_shape_entry->set_num_infeasible_requests_queued(count);  // 不可行
  } else {
    by_shape_entry->set_num_ready_requests_queued(count);       // 可行但排队
  }
  by_shape_entry->set_backlog_size(TotalBacklogSize(scheduling_class));
}
```

**注意**: 这里的 `shape` 是 task/actor 的资源需求，**包含** task 提交时附带的 `node:<IP>: 0.001` 等节点亲和性资源。

### 第2步：GCS 聚合所有 raylet 的 pending 数据

**文件**: `src/ray/gcs/state_util.cc:22-44`

```cpp
void FillAggregateLoad(const rpc::ResourcesData &resources_data,
                       absl::flat_hash_map<ResourceDemandKey, rpc::ResourceDemand> *aggregate_load) {
  const auto &load = resources_data.resource_load_by_shape();
  for (const auto &demand : load.resource_demands()) {
    ResourceDemandKey key;
    key.shape = demand.shape();  // ← 包含 node:IP 等所有资源
    auto &aggregate_demand = (*aggregate_load)[key];
    aggregate_demand.set_num_ready_requests_queued(
        aggregate_demand.num_ready_requests_queued() + demand.num_ready_requests_queued());
    aggregate_demand.set_num_infeasible_requests_queued(
        aggregate_demand.num_infeasible_requests_queued() + demand.num_infeasible_requests_queued());
    aggregate_demand.set_backlog_size(aggregate_demand.backlog_size() + demand.backlog_size());
  }
}
```

**文件**: `src/ray/gcs/gcs_autoscaler_state_manager.cc:324-345`

```cpp
void GcsAutoscalerStateManager::GetPendingResourceRequests(
    rpc::autoscaler::ClusterResourceState *state) {
  auto aggregate_load = GetAggregatedResourceLoad();
  for (auto &[key, demand] : aggregate_load) {
    const auto &shape = key.shape;
    auto num_pending = demand.num_infeasible_requests_queued()
                     + demand.backlog_size()
                     + demand.num_ready_requests_queued();
    if (num_pending > 0) {
      auto pending_req = state->add_pending_resource_requests();
      pending_req->set_count(num_pending);
      auto req = pending_req->mutable_request();
      req->mutable_resources_bundle()->insert(shape.begin(), shape.end());
    }
  }
}
```

### 第3步：Python 端解析

**文件**: `python/ray/autoscaler/v2/utils.py:769-782`

```python
for request_count in state.pending_resource_requests:
    demand = RayTaskActorDemand(
        bundles_by_count=[
            ResourceRequestByCount(
                request_count.request.resources_bundle, request_count.count
            )
        ],
    )
    task_actor_demand.append(demand)
```

### 第4步：格式化显示

**文件**: `python/ray/autoscaler/_private/util.py:688-743`

```python
def format_resource_demand_summary(resource_demand):
    # 过滤 placement group 前缀
    def filter_placement_group_from_bundle(bundle):
        result_bundle = dict()
        using_placement_group = False
        for pg_resource_str, resource_count in bundle.items():
            (resource_name, pg_name, _) = parse_placement_group_resource_str(pg_resource_str)
            result_bundle[resource_name] = resource_count
            if pg_name:
                using_placement_group = True
        return (result_bundle, using_placement_group)

    bundle_demand = collections.defaultdict(int)
    for bundle, count in resource_demand:
        (pg_filtered_bundle, _) = filter_placement_group_from_bundle(bundle)
        if len(pg_filtered_bundle.keys()) == 0:
            continue
        bundle_demand[tuple(sorted(pg_filtered_bundle.items()))] += count

    demand_lines = []
    for bundle, count in bundle_demand.items():
        line = f" {dict(bundle)}: {count}+ pending tasks/actors"
        demand_lines.append(line)
    return demand_lines
```

**注意**: `filter_placement_group_from_bundle` 会还原 `CPU_group_xxx` → `CPU`，但**不过滤** `node:<IP>`。所以 Pending Demands 中如果 task 带有节点亲和性，也会显示 `node:IP: 0.001`。

---

## 四、`_reallocate_resources()` 完整逻辑 — 整数除法的来源

**文件**: `python/ray/data/_internal/cluster_autoscaler/default_autoscaling_coordinator.py:397-435`

```python
def _reallocate_resources(self):
    """Reallocate cluster resources."""
    now = self._get_current_time()
    cluster_node_resources = copy.deepcopy(self._cluster_node_resources)
    ongoing_reqs = sorted(
        [req for req in self._ongoing_reqs.values() if req.expiration_time >= now]
    )

    # ========== 阶段1：为每个请求者的显式请求分配资源 ==========
    for ongoing_req in ongoing_reqs:
        ongoing_req.allocated_resources = []
        for req in ongoing_req.requested_resources:
            for node_resource in cluster_node_resources:
                if self._maybe_subtract_resources(node_resource, req):
                    ongoing_req.allocated_resources.append(req)
                    break

    # ========== 阶段2：将剩余资源平分给 request_remaining=True 的请求者 ==========
    remaining_resource_requesters = [
        req for req in ongoing_reqs if req.request_remaining
    ]
    num_remaining_requesters = len(remaining_resource_requesters)
    if num_remaining_requesters > 0:
        for node_resource in cluster_node_resources:
            # 整数除法！
            divided_resource = {
                k: v // num_remaining_requesters for k, v in node_resource.items()
            }
            for ongoing_req in remaining_resource_requesters:
                if any(v > 0 for v in divided_resource.values()):
                    ongoing_req.allocated_resources.append(divided_resource)
```

### `_maybe_subtract_resources()` — 整体扣减

**文件**: `default_autoscaling_coordinator.py:361-369`

```python
def _maybe_subtract_resources(self, res1, res2):
    """If res2<=res1, subtract res2 from res1 in-place, and return True."""
    if any(res1.get(key, 0) < res2[key] for key in res2):   # ← 只检查 res2 中有的 key
        return False
    for key in res2:
        if key in res1:
            res1[key] -= res2[key]                           # ← 只扣减 res2 中有的 key
    return True
```

### 关键点分析

#### 阶段1：按 bundle 整体匹配，不拆分

- `_maybe_subtract_resources` 要求 req 中**所有维度**都 ≤ 节点剩余，才分配成功
- **不会把一个 bundle 拆开跨节点分配**
- 阶段1的 bundle（来自 `to_bundle()`）只含 CPU/GPU/memory，**不含 `node:<IP>`**
- 因此 `node:<IP>` **永远不会被阶段1扣减**，留在节点剩余资源中

#### 阶段2：整数除法

- 将无人认领的剩余资源平均分给所有 `request_remaining=True` 的请求者
- 对节点的**所有 key** 一视同仁做整数除法，包括 `node:<IP>`
- `1.0 // N = 0` (当 N≥2 时)，导致 `node:<IP>: 0.0`

#### 阶段2的设计意图

Ray Data 用 `get_total_resources()` 来**限制该作业的并发度**：

```python
# resource_manager.py:308-314
total_resources = self._get_total_resources()       # ← 来自 autoscaler 的分配
self._global_limits = default_limits.min(total_resources)  # ← 作为作业的资源上限
```

阶段2的目的是：当多个 Ray Data 作业共享集群时，把"不属于任何人的剩余资源"平均分给各请求者，让它们可以额外使用这些资源提高并发度。

### `node:<IP>` 的来源

Ray 在每个节点启动时，自动注册 `node:<IP>: 1.0` 资源：

**文件**: `python/ray/_private/resource_and_label_spec.py:235`

```python
self.resources[NODE_ID_PREFIX + node_ip_address] = 1.0
```

**文件**: `python/ray/_common/constants.py:3`

```python
NODE_ID_PREFIX = "node:"
```

这个资源的值固定为 1.0，含义是"这个节点存在"，是**标识性资源**，不是可分割的数量性资源。

---

## 五、请求者（Requester）机制

### 请求者是什么

**请求者 = 每个独立的 Ray Data 作业（Dataset 执行）**。

每个 Ray Data 作业在启动时会创建独立的 `StreamingExecutor` → 独立的 `DefaultClusterAutoscalerV2` → 独立的 `requester_id`。

**文件**: `python/ray/data/_internal/plan.py:91-104`

```python
def get_dataset_id(self) -> str:
    return (
        f"{self._dataset_name or 'dataset'}_{self._dataset_uuid}_{self._run_index}"
    )

def create_executor(self):
    self._run_index += 1
    executor = StreamingExecutor(self._context, self.get_dataset_id())
    return executor
```

**文件**: `default_cluster_autoscaler_v2.py:187`

```python
self._requester_id = f"data-{execution_id}"
# → "data-my_dataset_uuid123_0"
```

### 不同场景的请求者数量

| 场景 | 请求者数量 | 原因 |
|------|-----------|------|
| 1个 Ray Data 作业 | 1 | 1个 execution_id → 1个 requester_id |
| 2个 Ray Data 作业**同时运行** | 2 | 2个不同的 execution_id → 2个 requester_id |
| 同一 dataset **串行**执行2次 | 1（每次） | 第1次完成后 shutdown → cancel_request → 第2次创建新的 |
| 同一 dataset `.repeat(2)` | 1 | 同一个 executor，同一个 execution_id |
| 1个 Ray Data 作业 + 非 Data 作业调 `request_resources()` | 2+ | 两个系统各自注册 |

### 是整体资源请求还是差值

**是整体，不是差值。** 代码注释写得很明确：

```python
# base_autoscaling_coordinator.py:28-29
"""The requested resources should represent the full set of resources needed,
not just the incremental amount."""
```

Ray Autoscaler SDK 的 `request_resources()` 语义也是声明式的：

```python
# commands.py:220-223
# If the cluster already has `to_request` resources, this will be an no-op.
```

每次 `try_trigger_scaling()` 构建的是**完整的集群目标 shape**，autoscaler 自行计算差值决定是否需要扩容。

---

## 六、用户场景的完整还原

### 用户输出

```
Total Usage:
19.25/1619.0 CPU
0.0/2.0 GPU
487.11MiB/1.99TiB memory

From request_resources:
{'memory': 217258927.0, 'node:10.53.80.206': 0.0, 'object_store_memory': 0.0, 'CPU': 0.0}: 3 from request_resources()

Pending Demands:
{'CPU': 1.0}: 135+ pending tasks/actors

No available node types can fulfill cluster constraint:
  {'memory': 7.0, 'object_store_memory': 0.0, 'node:10.53.34.151': 0.0, 'CPU': 0.0}*9,
  {'node:10.57.36.16': 0.0, 'memory': 135722.0, 'object_store_memory': 0.0, 'CPU': 0.0}*1
```

### 数学验证

```python
# 3个相同规格的小节点
node_memory = 434517854  # ~414MiB

# 2个请求者 request_remaining=True
num_remaining_requesters = 2

# 阶段2除法
divided_memory = 434517854 // 2 = 217258927  # ← 完美匹配用户输出!
divided_node_ip = 1.0 // 2 = 0.0              # ← 匹配!
divided_cpu = 1.0 // 2 = 0.0                  # ← 匹配!
divided_obj_store = 0 // 2 = 0                 # ← 匹配!

# 3个节点产生3个相同 shaped bundle
# SDK Counter 聚合后 → count=3
```

### 各区域含义

| 区域 | 含义 |
|------|------|
| **Total Usage** | 集群整体资源使用情况，CPU利用率仅 1.2% |
| **From request_resources** | 阶段2整数除法产生的 divided_resource（含 `node:IP:0.0` 残值），3个相同形状聚合 |
| **Pending Demands** | 135+个 task 每个1 CPU，因为节点亲和性约束无法调度 |
| **Infeasible cluster constraint** | autoscheduler 判定这些含 `node:IP:0.0` 的约束无法满足 |

### 135+ pending tasks 但 CPU 空闲的根本原因

集群 CPU 利用率仅 1.2%（19.25/1619），但 135 个 task pending。问题不在资源总量，而在：

1. 这些 task 带有**节点亲和性约束**（`node:<特定IP>`），只能调度到特定节点
2. 特定节点已经满载或不存在
3. Infeasible 消息是 `request_resources` 中无意义 bundle 导致的**误报**

---

## 七、完整数据流对比图

```
┌──────────────────────────────────────────────────────────────────────────┐
│                     /api/cluster_status 输出                             │
├─────────────────────────┬────────────────────────────────────────────────┤
│ From request_resources  │ Pending Demands                               │
│                         │                                                │
│ 来源: SDK 主动声明       │ 来源: Raylet 自动上报                           │
│ 存储: GCS constraint_   │ 存储: GCS aggregate_load                       │
│ 覆盖: 最新覆盖旧值      │ 累积: 持续增减                                  │
│ *N = 相同形状bundle数   │ +N = pending task/actor数                       │
│                         │                                                │
│ 数据流:                 │ 数据流:                                         │
│ RayData V2              │ Raylet调度器                                    │
│   ↓ try_trigger         │   ↓ 检测不可调度                                │
│ Coordinator Actor       │   ↓ 按 shape 聚合                               │
│   ↓ _reallocate         │   ↓ 上报 resource_load_by_shape                │
│   ↓ (整数除法!)         │ GCS                                            │
│   ↓ _merge_and_send     │   ↓ FillAggregateLoad                          │
│ SDK request_resources   │   ↓ GetPendingResourceRequests                 │
│   ↓ GCS RPC             │ 格式化                                          │
│ GCS                     │   ↓ filter_pg_from_bundle                      │
│   ↓ constraint_         │   ↓ {dict(bundle)}: count+ pending              │
│ 格式化                  │                                                │
│   ↓ {bundle}: N         │                                                │
│     from request_       │                                                │
│     resources()         │                                                │
└─────────────────────────┴────────────────────────────────────────────────┘
```

---

## 八、`node:<IP>` 资源在各环节的处理总结

| 环节 | `node:<IP>` 是否出现 | 处理方式 |
|------|---------------------|---------|
| `to_bundle()` | ✗ 不出现 | 只输出 CPU/GPU/memory |
| `ray.nodes()["Resources"]` | ✓ 出现 (值=1.0) | 节点启动时自动添加 |
| 阶段1 `_maybe_subtract_resources` | 不扣减 | req 中不含 `node:` key，所以不检查不扣减 |
| 阶段2 `divided_resource` | ✓ 出现 (值可能=1.0或0.0) | 对所有 key 一律整数除法 |
| `get_allocated_resources()` 返回 | ✓ 出现 | 包含阶段2产生的 divided_resource |
| `try_trigger_scaling` 低利用率分支 | ✓ 发送给 autoscaler | 把 allocated 作为 request 发出 |
| Pending Demands | 可能出现 | task 自带节点亲和性时出现 |
| `format_resource_demand_summary` | 不过滤 | 没有 `node:` 前缀的过滤逻辑 |
| `parse_usage` (Total Usage) | 过滤 | `if "node:" in resource: continue` |