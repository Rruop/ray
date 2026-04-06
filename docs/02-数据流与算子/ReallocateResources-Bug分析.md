# Ray Data Autoscaler: `_reallocate_resources` Bug 分析与修复方案

## 概述

Ray Data 的 `_AutoscalingCoordinatorActor._reallocate_resources()` 存在两个相关 Bug，导致 `node:<IP>` 资源被错误处理，进而产生无效的 cluster constraint，触发 autoscaler infeasible 误报。

---

## Bug 概览

| Bug | 现象 | 触发条件 | 严重程度 |
|-----|------|---------|---------|
| **Bug A**: 阶段1不扣减 `node:<IP>` | 产生 `{node:IP:1.0, CPU:0, memory:0}` 的无效 bundle | 只需1个请求者，1次扩容 | **严重** — 产生带节点亲和性约束的无效请求 |
| **Bug B**: 阶段2对 `node:<IP>` 做整数除法 | 产生 `{node:IP:0.0, CPU:0, memory:xxx}` 的残值 bundle | ≥2 个请求者 | **中等** — 产生无意义但会被判 infeasible 的约束 |

---

## Bug A 详细分析：阶段1不扣减 `node:<IP>`

### 现象

`_reallocate_resources()` 阶段1完成后，节点剩余资源中出现：

```python
{'CPU': 0.0, 'memory': 0, 'node:10.53.80.206': 1.0, 'object_store_memory': 107374182400}
```

即 `CPU=0, memory=0` 但 `node:<IP>=1.0`。这个 bundle 被阶段2分配给请求者，后续作为 cluster constraint 发给 autoscaler，导致 autoscaler 认为"需要1个 10.53.80.206 节点上的资源"。

### 根因

**文件**: `python/ray/data/_internal/cluster_autoscaler/default_autoscaling_coordinator.py:361-369`

```python
def _maybe_subtract_resources(self, res1, res2):
    """If res2<=res1, subtract res2 from res1 in-place, and return True."""
    if any(res1.get(key, 0) < res2[key] for key in res2):   # ← 只检查 res2(req) 中的 key
        return False
    for key in res2:
        if key in res1:
            res1[key] -= res2[key]                           # ← 只扣减 res2(req) 中的 key
    return True
```

阶段1的 bundle 来自 `to_bundle()`，只含 `{CPU, GPU, memory}`，**不含 `node:<IP>`**：

```python
# default_cluster_autoscaler_v2.py:65-66
def to_bundle(self):
    return {"CPU": self.cpu, "GPU": self.gpu, "memory": self.mem}
```

因此 `_maybe_subtract_resources` 扣减时，`node:<IP>` 不在 req 的 key 中，**永远不会被扣减**。

### 推演过程

```
节点原始资源:
  {CPU: 16.0, memory: 216895848448, node:10.53.80.206: 1.0, object_store_memory: 107374182400}

阶段1 bundle (from to_bundle()):
  {CPU: 16, GPU: 0, memory: 216895848448}     ← 不含 node:IP

_maybe_subtract_resources 扣减:
  CPU: 16.0 ≥ 16 ✓ → 扣减后 0.0
  memory: 216895848448 ≥ 216895848448 ✓ → 扣减后 0
  node:10.53.80.206: 不在 req 中 → 不扣减! 仍然 1.0
  object_store_memory: 不在 req 中 → 不扣减! 仍然 107374182400

节点剩余:
  {CPU: 0.0, memory: 0, node:10.53.80.206: 1.0, object_store_memory: 107374182400}
  ↑ node:IP 残留! CPU/memory 已归零
```

### 影响链

```
阶段1不扣减 node:IP
  → node:IP 留在节点剩余资源中 (CPU=0, memory=0, node:IP=1.0)
    → 阶段2将此 divided_resource 追加到 allocated_resources
      → try_trigger_scaling 低利用率分支把 allocated 作为 request 发出
        → autoscaler 收到约束 {node:10.53.80.206:1.0, CPU:0, memory:0}
          → 认为请求需要绑定到 10.53.80.206 节点
            → 如果该节点已满/不存在 → infeasible
            → 如果存在 → 不合理的调度绑定
```

### 触发条件

- **只需1个请求者**即可触发
- 只需1次利用率超过阈值的扩容请求
- 非常常见，几乎每次 Ray Data 作业都会触发

---

## Bug B 详细分析：阶段2对 `node:<IP>` 做整数除法

### 现象

`_reallocate_resources()` 阶段2对节点剩余资源做整数除法时，`node:<IP>: 1.0 // N = 0.0`（当 N≥2 时），产生：

```python
{'CPU': 0.0, 'memory': 217258927.0, 'node:10.53.80.206': 0.0, 'object_store_memory': 0.0}
```

### 根因

**文件**: `python/ray/data/_internal/cluster_autoscaler/default_autoscaling_coordinator.py:420-426`

```python
if num_remaining_requesters > 0:
    for node_resource in cluster_node_resources:
        # Divide remaining resources equally among requesters.
        # NOTE: Integer division may leave some resources unallocated.
        divided_resource = {
            k: v // num_remaining_requesters for k, v in node_resource.items()
            #   ↑ 对所有 key 一律整数除法，包括 node:<IP>
        }
```

`node:<IP>` 的值固定为 `1.0`，含义是"这个节点存在"，是**标识性资源**。`1.0 // 2 = 0.0` 完全丢失了语义。

### 推演过程

```python
# 节点资源 (阶段1扣减后的剩余)
node_resource = {'CPU': 1.0, 'memory': 434517854, 'node:10.53.80.206': 1.0, 'object_store_memory': 0}

# 阶段2除法 (2个请求者)
divided_resource = {
    "CPU": 1.0 // 2 = 0,                      # 丢失
    "memory": 434517854 // 2 = 217258927,      # 唯一保留的有效值
    "node:10.53.80.206": 1.0 // 2 = 0,         # 丢失
    "object_store_memory": 0 // 2 = 0
}

# 3个相同节点 → 3个相同 shaped bundle → Counter聚合 → count=3
# 最终显示:
# {'memory': 217258927.0, 'node:10.53.80.206': 0.0, 'object_store_memory': 0.0, 'CPU': 0.0}: 3
```

### 影响链

```
阶段2整数除法
  → node:IP: 1.0 // N = 0.0 (N≥2)
    → divided_resource 包含 {node:IP:0.0, CPU:0, memory:xxx}
      → 追加到 allocated_resources
        → 作为 cluster constraint 发给 autoscaler
          → autoscheduler 看到约束 {node:IP:0.0, CPU:0}
            → 无法理解 → 判定 infeasible
              → "No available node types can fulfill cluster constraint"
```

### 触发条件

- 需要 ≥2 个 `request_remaining=True` 的请求者同时存在
- 即 ≥2 个 Ray Data 作业同时运行

### 用户场景验证

```python
# 用户的输出: {'memory': 217258927.0, 'node:10.53.80.206': 0.0, 'CPU': 0.0}: 3
# 反推:
#   节点 memory = 434517854
#   num_remaining_requesters = 2
#   434517854 // 2 = 217258927 ✓
#   1.0 // 2 = 0.0 ✓
#   3个相同节点 → count=3 ✓
```

---

## 两个 Bug 的关系

```
根因: _reallocate_resources() 的两个设计缺陷

Bug A (node:IP:1.0):  阶段1只扣减 req 中有的 key，node:IP 不在 bundle 中
                      → node:IP 留在节点剩余中
                      → 阶段2分配出 {node:IP:1.0, CPU:0, memory:0}

Bug B (node:IP:0.0):  阶段2对所有 key 一律整数除法
                      → node:IP:1.0 // N = 0.0 (当 N≥2)

两者独立触发:
  - 1个请求者 → Bug A (node:IP:1.0, CPU:0, memory:0)
  - 2个请求者 → Bug A + Bug B (node:IP:0.0, CPU:0, memory:xxx)

Bug A 是更根本的问题:
  - Bug A 使 node:IP 泄漏到阶段2
  - Bug B 使泄漏的 node:IP 值变为0
  - 如果先修 Bug A (不让 node:IP 泄漏到阶段2), Bug B 自然消失
```

---

## 修复方案

### 方案1: 阶段2过滤 `node:` 前缀资源（最小修复）

**改动范围**: `default_autoscaling_coordinator.py` 1行

**修复前**:

```python
divided_resource = {
    k: v // num_remaining_requesters for k, v in node_resource.items()
}
```

**修复后**:

```python
divided_resource = {
    k: v // num_remaining_requesters
    for k, v in node_resource.items()
    if not k.startswith("node:")  # ← 过滤标识性资源
}
```

**优点**: 改动最小，只修 Bug B
**缺点**: Bug A 仍然存在，`{node:IP:1.0, CPU:0, memory:0}` 仍然会从阶段2泄漏（只是当 N=1 时不做除法所以值还是 1.0）

### 方案2: 阶段1扣减时同时清理 `node:` 资源（推荐）

**改动范围**: `default_autoscaling_coordinator.py` `_maybe_subtract_resources` 或 `_reallocate_resources`

在阶段1扣减后，将匹配节点的 `node:<IP>` 也一并扣减：

```python
# 阶段1修改: _reallocate_resources 内部
for ongoing_req in ongoing_reqs:
    ongoing_req.allocated_resources = []
    for req in ongoing_req.requested_resources:
        for node_resource in cluster_node_resources:
            if self._maybe_subtract_resources(node_resource, req):
                ongoing_req.allocated_resources.append(req)
                # 新增: 扣减匹配节点的 node:IP 标识资源
                node_ip_keys = [k for k in node_resource if k.startswith("node:")]
                for k in node_ip_keys:
                    node_resource[k] = 0
                break
```

**优点**: 同时修 Bug A 和 Bug B
**缺点**: 逻辑上不太优雅，因为 `node:IP` 不在 req 中却被扣减

### 方案3: `_maybe_subtract_resources` 扣减节点所有资源（最彻底）

修改 `_maybe_subtract_resources` 使得当 req 成功匹配节点后，将节点**所有**资源都扣减归零：

```python
def _maybe_subtract_resources(self, res1, res2):
    """If res2<=res1, subtract res2 from res1 in-place, and return True."""
    if any(res1.get(key, 0) < res2[key] for key in res2):
        return False
    # 扣减 req 中指定的资源
    for key in res2:
        if key in res1:
            res1[key] -= res2[key]
    # 同时扣减所有 node: 前缀的标识性资源
    # 因为 bundle 匹配成功意味着整个节点被"占用"
    for key in list(res1.keys()):
        if key.startswith("node:"):
            res1[key] = 0
    return True
```

**优点**: 最彻底，同时修 Bug A 和 Bug B
**缺点**: 改变了 `_maybe_subtract_resources` 的语义，可能影响其他调用者

### 方案4: `get_allocated_resources` 返回时过滤 `node:` key（防御性修复）

在 `allocated_resources` 被使用之前，过滤掉 `node:` 前缀的 key：

```python
# default_cluster_autoscaler_v2.py:217-220 (低利用率分支)
curr_resources = self._autoscaling_coordinator.get_allocated_resources(
    requester_id=self._requester_id
)
# 新增: 过滤 node: 前缀
curr_resources = [
    {k: v for k, v in bundle.items() if not k.startswith("node:")}
    for bundle in curr_resources
]
self._send_resource_request(curr_resources)
```

**优点**: 在数据出口处拦截，不影响内部逻辑
**缺点**: 需要在所有使用 `allocated_resources` 的地方都加过滤，遗漏则有风险

### 推荐方案: 方案2 + 方案1 组合

同时修复阶段1和阶段2：

```python
def _reallocate_resources(self):
    now = self._get_current_time()
    cluster_node_resources = copy.deepcopy(self._cluster_node_resources)
    ongoing_reqs = sorted(
        [req for req in self._ongoing_reqs.values() if req.expiration_time >= now]
    )

    # ========== 阶段1 ==========
    for ongoing_req in ongoing_reqs:
        ongoing_req.allocated_resources = []
        for req in ongoing_req.requested_resources:
            for node_resource in cluster_node_resources:
                if self._maybe_subtract_resources(node_resource, req):
                    ongoing_req.allocated_resources.append(req)
                    # [修复 Bug A] 扣减匹配节点的 node: 标识资源
                    for k in list(node_resource.keys()):
                        if k.startswith("node:"):
                            node_resource[k] = 0
                    break

    # ========== 阶段2 ==========
    remaining_resource_requesters = [
        req for req in ongoing_reqs if req.request_remaining
    ]
    num_remaining_requesters = len(remaining_resource_requesters)
    if num_remaining_requesters > 0:
        for node_resource in cluster_node_resources:
            # [修复 Bug B] 过滤 node: 前缀的标识性资源
            divided_resource = {
                k: v // num_remaining_requesters
                for k, v in node_resource.items()
                if not k.startswith("node:")
            }
            for ongoing_req in remaining_resource_requesters:
                if any(v > 0 for v in divided_resource.values()):
                    ongoing_req.allocated_resources.append(divided_resource)
```

---

## 测试验证

### 测试1: 单请求者 + 扩容请求 → 验证 Bug A 修复

```python
# 1个请求者, 利用率超阈值, 构建 bundle
# 期望: 阶段1扣减后, node:IP 也被清零
# 期望: 阶段2 divided_resource 不含 node:IP
# 期望: allocated_resources 中无 {node:IP:1.0, CPU:0, memory:0} 的 bundle
```

### 测试2: 双请求者 + 剩余分配 → 验证 Bug B 修复

```python
# 2个请求者 request_remaining=True
# 期望: 阶段2 divided_resource 不含 node:IP
# 期望: divided_resource 中 CPU/memory 等正常除法
# 期望: 不产生 {node:IP:0.0, CPU:0, memory:xxx} 的 bundle
```

### 测试3: 低利用率分支 → 验证不再产生无效 constraint

```python
# 模拟 try_trigger_scaling 低利用率分支
# 期望: curr_resources 过滤后不含 node:IP
# 期望: autoscaler 不再产生 "No available node types can fulfill cluster constraint" 误报
```

### 回归测试

- `python/ray/data/tests/test_autoscaling_coordinator.py`
- `python/ray/data/tests/test_default_cluster_autoscaler.py`

---

## 相关 Issue 与代码位置

| 文件 | 行号 | 内容 |
|------|------|------|
| `python/ray/data/_internal/cluster_autoscaler/default_autoscaling_coordinator.py` | 361-369 | `_maybe_subtract_resources` — 不扣减 req 中不存在的 key |
| `python/ray/data/_internal/cluster_autoscaler/default_autoscaling_coordinator.py` | 420-426 | 阶段2 `divided_resource` — 对所有 key 一律整数除法 |
| `python/ray/data/_internal/cluster_autoscaler/default_cluster_autoscaler_v2.py` | 65-66 | `to_bundle()` — 只输出 CPU/GPU/memory |
| `python/ray/data/_internal/cluster_autoscaler/default_cluster_autoscaler_v2.py` | 217-220 | 低利用率分支 — 把 allocated 作为 request 发出 |
| `python/ray/_private/resource_and_label_spec.py` | 235 | `node:<IP>: 1.0` 的自动添加 |
| `python/ray/_common/constants.py` | 3 | `NODE_ID_PREFIX = "node:"` |
| `python/ray/autoscaler/v2/event_logger.py` | 166-185 | infeasible 消息格式化 |
| `python/ray/autoscaler/_private/util.py` | 634-635 | Total Usage 展示时过滤 `node:` 资源 |

---

## 附录：完整推演模拟

### 场景1: 单请求者 + 扩容 (Bug A 触发)

```
集群: 1个节点 {CPU:16, memory:202G, node:10.53.80.206:1.0, obj_store:100G}

第1轮 (初始注册, 空请求):
  阶段1: 空, 不扣减
  阶段2: num_remaining=1, divided = 完整节点资源
  allocated = [{CPU:16, memory:202G, node:10.53.80.206:1.0, obj_store:100G}]

第2轮 (利用率超阈值):
  reqA.requested_resources = [{CPU:16, memory:202G}]   ← from to_bundle()
  阶段1:
    bundle0 {CPU:16, memory:202G} → 匹配 node0
    node0 剩余: {CPU:0, memory:0, node:10.53.80.206:1.0, obj_store:100G}  ← Bug A!
  阶段2:
    divided = {CPU:0, memory:0, node:10.53.80.206:1.0, obj_store:100G}
    any(v>0) = True → 追加!
  allocated = [
    {CPU:16, memory:202G},                                         ← 阶段1
    {CPU:0, memory:0, node:10.53.80.206:1.0, obj_store:100G}       ← 阶段2 ⚠️
  ]

第3轮 (低利用率分支):
  curr_resources = allocated (含 {node:10.53.80.206:1.0, CPU:0, memory:0})
  → 发给 autoscaler 作为 cluster constraint
  → autoscheduler 判定需要绑定到 10.53.80.206 → infeasible 或不合理调度
```

### 场景2: 双请求者 + 小节点 (Bug A + Bug B 触发)

```
集群: 3个相同小节点 {CPU:1, memory:434517854, node:IP:1.0, obj_store:0}
请求者: 2个 (A和B), request_remaining=True

初始轮 (空请求):
  阶段2: num_remaining=2
  每个节点 divided:
    CPU: 1 // 2 = 0
    memory: 434517854 // 2 = 217258927
    node:IP: 1 // 2 = 0          ← Bug B!
    obj_store: 0 // 2 = 0
  → {CPU:0, memory:217258927, node:IP:0, obj_store:0}
  → 追加到 reqA.allocated 和 reqB.allocated

第2轮 (低利用率分支):
  reqA 和 reqB 都把 allocated 作为 request 发出
  → 3个相同 shaped bundle: {memory:217258927, node:IP:0, CPU:0}
  → SDK Counter 聚合: count=3
  → 显示: {'memory': 217258927.0, 'node:10.53.80.206': 0.0, 'CPU': 0.0}: 3

  autoscheduler 判定 infeasible:
    "No available node types can fulfill cluster constraint:
     {'memory': 7.0, 'node:10.53.34.151': 0.0, 'CPU': 0.0}*9"
```

### 场景3: 修复后的期望行为

```
集群: 3个相同小节点 {CPU:1, memory:434517854, node:IP:1.0, obj_store:0}

修复后阶段1:
  bundle 扣减时, node:IP 也被清零
  → 节点剩余: {CPU:0, memory:0, node:IP:0, obj_store:0}
  → 无泄漏

修复后阶段2:
  divided_resource 不含 node:IP (被过滤)
  → {CPU:0, memory:0, obj_store:0}
  → any(v>0) = False → 不追加!
  → allocated_resources 不含无效 bundle

修复后显示:
  From request_resources: (none)
  或只包含有效的 CPU/GPU/memory bundle
  不再产生 infeasible 误报
```