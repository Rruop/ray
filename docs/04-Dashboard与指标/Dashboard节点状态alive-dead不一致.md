# Ray Dashboard 首页 Node Status 显示已 Dead 节点为 Alive 问题分析

## 问题描述

Ray Dashboard 首页 **"Cluster status and autoscaler"** 下的 **Node Status** 区域中，`Active:` 下显示的 alive 节点中包含部分已经 dead 的节点。

- **集群**: `kml-hb2az1-l3-2`, namespace `lmserving`
- **Pod**: `lmserv-proj-10121-svc-222769-ray-he-eo-syp-0`
- **GCS 地址**: `10.15.6.116:6379`
- **Ray 版本**: 2.52.1（commit `ba1be9fc`），同样的 bug 在最新 master（`6fad5adf55`）和最新 release（2.55.1）中仍然存在

页面展示结构如下：
```
Node status
---------------------------------------------------------------
Active:
 1 node_b342f60a2eed557876935548df3ee10d76be104439585e5db645892a
 1 node_ac788d6b29d6d9d8633ae72fdc6ac990e11e8af6cc3ca5b0b810526c
 ... (共 14 个节点)
Pending:
 (no pending nodes)
Recent failures:
 (no failures)
```

**核心问题**：这个展示区域的数据来源是什么？是否是前端展示 bug？与 `nodes?view=summary` 接口是什么关系？在没有 autoscaler 进程的情况下为什么还能查到数据？V1 autoscaler 场景下会不会有同样的问题？

**结论**：这是 V1 autoscaler Monitor 在 readonly 模式下的 bug。`monitor.py:262-268` 中 `_set_nodes()` 从 `get_cluster_resource_state()` 获取节点列表时不过滤 DEAD 节点，且 `update_load_metrics()` 为所有节点（含 DEAD）刷新心跳，导致 dead 节点被误判为 active 并写入 KV。已提交 issue 到 Ray 社区：https://github.com/ray-project/ray/issues/63566

---

## 排查过程

### 第一步：确认数据来源

通过代码分析确认：
1. 首页 Node Status **不是**通过 `nodes?view=summary` 获取，而是调 `/api/cluster_status?format=1`
2. 该接口调 `debug_status()`，根据 `is_autoscaler_v2()` 走 V1 或 V2 分支
3. 前端 `AutoscalerStatusCards.tsx` 零过滤逻辑，原样渲染后端字符串

### 第二步：确认 V1/V2 路径

登录 Ray head 容器执行诊断：
```
RAY_enable_autoscaler_v2: NOT SET
__autoscaler_v2_enabled (GCS KV): b'0'
is_autoscaler_v2(): False
→ 走 V1 路径
```

**预期之外**：虽然使用了 `NoOpClusterAutoscaler`，但那是 Ray Data 层的组件，与 Dashboard 无关。实际 `enable_autoscaler_v2` 默认 `false`，集群走的是 V1。

### 第三步：对比 GCS 实际状态 vs KV 快照

```
GCS Actual: Total=14 Alive=4 Dead=10
KV active:  14 (全部显示为 Active)
Phantom:    10 (KV=active but GCS=dead)
```

所有 phantom 节点 IP 均为 `10.80.235.180`（已下线机器），在 GCS 中已标记 DEAD。

### 第四步：检查 Monitor 进程

Monitor 进程正在运行（PID 346），日志文件为空。说明进程正常运行但无错误/无特殊输出。

### 第五步：代码追踪定位根因

追踪 `monitor.py:262-268` → `_set_nodes()` → `ReadonlyNodeProvider` → `heartbeat_on_time()`，确认 bug 链路。

---

## 实际诊断数据

### 关键发现

```
=== GCS Actual ===
Total: 14  Alive: 4  Dead: 10

=== Dashboard Node Status (ray status) ===
Active: 14 nodes

=== KV vs GCS ===
KV active:  14
GCS alive:   4
Phantom (KV=active but GCS=dead): 10
Missing (GCS=alive but KV=not active): 0
```

**10 个 phantom 节点在 GCS 中全部标记为 DEAD，但在 Dashboard 首页仍显示为 Active。**

所有 phantom 节点均来自同一 IP `10.80.235.180`（已下线的 worker 机器）。

### 环境关键参数

```
RAY_enable_autoscaler_v2: NOT SET
__autoscaler_v2_enabled (GCS KV): b'0'
is_autoscaler_v2(): False
→ 走 V1 路径
```

### Monitor 进程状态

Monitor 进程**正在运行**（PID 346），但日志文件（monitor.out / monitor.err）均为空（0 行）。

```
root 346 ... /opt/vjepa2/bin/python -u .../ray/autoscaler/_private/monitor.py
    --gcs-address=10.15.6.116:6379
```

### 根因确认

**这是 V1 autoscaler 路径的已知设计缺陷**——`is_autoscaler_v2()` 返回 `False`，走 V1 路径，Dashboard 从 GCS Internal KV 的 `__autoscaling_status` 读取 monitor 写入的快照数据，而不是实时查询 GCS 节点状态。

V1 Monitor 的 `ReadonlyNodeProvider._set_nodes()` 在 `update_load_metrics()` 中从 `get_cluster_resource_state()` 获取节点列表，该 gRPC 返回的 `node_states` **包含 alive + dead 节点**，但 `_set_nodes` **不区分节点状态**，把所有节点都加入 provider。后续 `summary()` 中通过 `heartbeat_on_time()` 判断 active，但 `internal_ip()` 返回的是 node_id 而非真实 IP，导致心跳匹配逻辑在 readonly 模式下不一定正确。

---

## 数据来源分析

### Node Status 与 Nodes 列表页使用完全不同的数据源

| 维度 | 首页 Node Status | Nodes 列表页 |
|------|-----------------|-------------|
| **API 接口** | `/api/cluster_status?format=1` | `/nodes?view=summary` |
| **前端组件** | `AutoscalerStatusCards.tsx` → `NodeStatusCard` | `ClusterLayout.tsx` |
| **数据来源 (V2)** | 实时 gRPC → GCS `HandleGetClusterStatus` | Dashboard 内存缓存 `DataSource.nodes` |
| **数据来源 (V1)** | GCS Internal KV 缓存快照 | Dashboard 内存缓存 `DataSource.nodes` |
| **更新频率** | 前端 SWR 轮询 (`API_REFRESH_INTERVAL_MS`) | 前端 SWR 轮询 |

**结论：首页 Node Status 的数据不是通过 `nodes?view=summary` 获取的，两者是完全独立的数据通路。**

---

## 完整数据链路追踪

### 前端层

```
OverviewPage.tsx
  └── NodeStatusCard (AutoscalerStatusCards.tsx:75-87)
        └── formatNodeStatus(clusterStatus?.data.clusterStatus)
              └── 纯文本渲染，零过滤逻辑，原样展示后端返回的字符串
```

**关键文件：**
- 组件定义：`python/ray/dashboard/client/src/components/AutoscalerStatusCards.tsx`
- 页面引用：`python/ray/dashboard/client/src/pages/overview/OverviewPage.tsx:83`
- 数据获取：`python/ray/dashboard/client/src/service/status.ts:12` → `get<RayStatusResp>("api/cluster_status?format=1")`
- 轮询 Hook：`python/ray/dashboard/client/src/pages/job/hook/useClusterStatus.ts`

**前端没有做任何数据过滤或状态判断，所有分类逻辑都在后端完成。**

### 后端 API 层

`python/ray/dashboard/modules/reporter/reporter_head.py:93-145`

```python
@routes.get("/api/cluster_status")
async def get_cluster_status(self, req):
    return_formatted_output = req.query.get("format", "0") == "1"

    # 从 GCS Internal KV 中并行读取三个 key
    (legacy_status, formatted_status_string, error) = await asyncio.gather(
        *[self.gcs_client.async_internal_kv_get(key.encode(), ...)
          for key in [
              DEBUG_AUTOSCALING_STATUS_LEGACY,   # V1 旧格式纯文本
              DEBUG_AUTOSCALING_STATUS,           # V1 结构化 JSON
              DEBUG_AUTOSCALING_ERROR,            # 错误信息
          ]]
    )

    if return_formatted_output:  # format=1 (前端默认请求)
        return debug_status(formatted_status_string, error, address=self.gcs_address)
    else:  # format=0
        return { autoscaling_status, autoscaling_error, cluster_status }
```

#### 为什么从三个 KV key 获取信息？

这是**历史包袱**，三个 key 对应不同阶段的实现：

| KV Key | 常量名 | 写入者 | 内容 |
|--------|--------|--------|------|
| `__autoscaling_status_legacy` | `DEBUG_AUTOSCALING_STATUS_LEGACY` | V1 `legacy_log_info_string()` | 最早的纯文本格式，兼容旧 dashboard |
| `__autoscaling_status` | `DEBUG_AUTOSCALING_STATUS` | V1 `monitor._run()` | V1 重构后的结构化 JSON（load_metrics + autoscaler_report） |
| `__autoscaling_error` | `DEBUG_AUTOSCALING_ERROR` | V1 monitor | autoscaler 错误信息 |

当前端请求 `format=1` 时，handler 调用 `debug_status()`，该函数内部会根据 autoscaler 版本走不同分支，V2 路径会**完全忽略这些 KV 数据**。

### 核心分支：`debug_status()` 的 V1/V2 分流

`python/ray/autoscaler/_private/commands.py:116-183`

```python
def debug_status(status, error, verbose=False, address=None):
    if is_autoscaler_v2():
        # V2: 直接 gRPC 调 GCS，实时获取，不用 KV 缓存
        cluster_status = get_cluster_status(address)
        status = ClusterStatusFormatter.format(cluster_status, verbose=verbose)
    elif status:
        # V1: 解析 KV 中缓存的 JSON
        status_dict = json.loads(status)
        status = format_info_string(lm_summary, autoscaler_summary, ...)
    else:
        status = "No cluster status..."
```

---

## V2 Autoscaler 路径详解

### `is_autoscaler_v2()` 判断逻辑

`python/ray/autoscaler/v2/utils.py:1027-1076`

判断优先级：
1. **环境变量短路**：`RAY_enable_autoscaler_v2=1` → 直接返回 `True`
2. **模块缓存**：之前查过就用缓存值
3. **GCS KV 查询**：读 `__autoscaler_v2_enabled` key

```python
def is_autoscaler_v2(fetch_from_server=False, gcs_client=None):
    # 环境变量优先
    if ray._config.enable_autoscaler_v2() and not fetch_from_server:
        return True
    # 查 GCS Internal KV
    cached_is_autoscaler_v2 = (
        gcs_client.internal_kv_get("__autoscaler_v2_enabled") == b"1"
    )
    return cached_is_autoscaler_v2
```

**`__autoscaler_v2_enabled` 由 GCS Server 启动时写入**（`src/ray/gcs/gcs_server.cc:745-747`），取自 `RayConfig::enable_autoscaler_v2()`：

```cpp
kv_manager_->GetInstance().Put(
    kGcsAutoscalerStateNamespace,
    kGcsAutoscalerV2EnabledKey,   // "__autoscaler_v2_enabled"
    v2_enabled,                    // "0" 或 "1"
    /*overwrite=*/true, ...);
```

`enable_autoscaler_v2` 在 `ray_config_def.h:956` 中默认为 `false`，但：
- KubeRay 环境下 `ray start` 会自动设为 `"1"`（`scripts.py:889`）
- 可通过 `--system-config={"enable_autoscaler_v2":true}` 或环境变量 `RAY_enable_autoscaler_v2=1` 开启

### V2 实时数据获取

`python/ray/autoscaler/v2/sdk.py:81-105`

```python
def get_cluster_status(gcs_address, timeout=10):
    str_reply = GcsClient(gcs_address).get_cluster_status(timeout_s=timeout)
    reply = GetClusterStatusReply()
    reply.ParseFromString(str_reply)
    return ClusterStatusParser.from_get_cluster_status_reply(reply, ...)
```

直接 gRPC 调用 GCS Server 的 `HandleGetClusterStatus`（`gcs_autoscaler_state_manager.cc:163`），内部调 `GetNodeStates()` 构建节点列表。

### C++ 层：`GetNodeStates()` — 节点状态的权威来源

`src/ray/gcs/gcs_autoscaler_state_manager.cc:347-453`

```cpp
void GcsAutoscalerStateManager::GetNodeStates(ClusterResourceState *state) {
    auto populate_node_state = [this, state](const GcsNodeInfo &gcs_node_info) {
        auto node_state_proto = state->add_node_states();
        // ...填充基本信息...

        if (gcs_node_info.state() == GcsNodeInfo::DEAD) {
            node_state_proto->set_status(NodeStatus::DEAD);
            return;  // dead 节点不填充资源信息
        }

        // alive 节点：根据 node_resource_info_ 判断具体状态
        auto node_resource_iter = node_resource_info_.find(node_id);
        if (node_resource_data.is_draining()) {
            node_state_proto->set_status(NodeStatus::DRAINING);
        } else if (node_resource_data.idle_duration_ms() > 0) {
            node_state_proto->set_status(NodeStatus::IDLE);
        } else {
            node_state_proto->set_status(NodeStatus::RUNNING);
        }
    };

    // 分别遍历 alive 和 dead 节点
    const auto alive_nodes = gcs_node_manager_.GetAllAliveNodes();
    std::for_each(alive_nodes.begin(), alive_nodes.end(), populate_node_state);

    const auto dead_nodes = gcs_node_manager_.GetAllDeadNodes();
    std::for_each(dead_nodes.begin(), dead_nodes.end(), populate_node_state);
}
```

**此函数直接查询 `GcsNodeManager` 的 alive/dead 列表，是实时的、权威的。**

### Python 层：`_parse_nodes()` 的节点分类

`python/ray/autoscaler/v2/utils.py:890-956`

```python
for node_state in state.node_states:
    if node_state.status == NodeStatus.DEAD:
        dead_nodes.append(node_info)
    elif node_state.status == NodeStatus.IDLE:
        idle_nodes.append(node_info)
    else:
        active_nodes.append(node_info)   # RUNNING, DRAINING, UNSPECIFIED 都归入 Active
```

| NodeStatus 枚举值 | 分类 | 展示区域 |
|-------------------|------|----------|
| `RUNNING` (1) | `active_nodes` | Active: |
| `IDLE` (3) | `idle_nodes` | Idle: |
| `DEAD` (2) | `dead_nodes` → `failed_nodes` | Recent failures: |
| `DRAINING` (4) | `active_nodes`（else 分支） | Active: |
| `UNSPECIFIED` (0) | `active_nodes`（else 分支） | Active: |

### `node_resource_info_` 的生命周期管理

| 事件 | 方法 | 逻辑 |
|------|------|------|
| 节点注册 | `OnNodeAdd()` (`gcs_autoscaler_state_manager.cc:270`) | 加入 `node_resource_info_` |
| 节点上报 | `UpdateResourceLoadAndUsage()` (`:289`) | 更新资源数据和时间戳 |
| 节点死亡 | `OnNodeDead()` (`gcs_autoscaler_state_manager.h:89`) | `node_resource_info_.erase(node)` |

`OnNodeDead` 通过 `GcsNodeManager` 的 `NodeRemovedListener` 回调链调用（`gcs_server.cc:846-861`）：

```cpp
gcs_node_manager_->AddNodeRemovedListener([this](const auto &node) {
    auto node_id = NodeID::FromBinary(node->node_id());
    gcs_resource_manager_->OnNodeDead(node_id);
    gcs_placement_group_manager_->OnNodeDead(node_id);
    gcs_actor_manager_->OnNodeDead(node, node_ip_address);
    // ... 其他清理 ...
    gcs_autoscaler_state_manager_->OnNodeDead(node_id);  // line 860
});
```

**清理链路完整，`node_resource_info_` 不会残留 dead 节点数据。**

---

## V2 路径下的完整架构

```
                    ┌─────────────────────┐
                    │   Autoscaler 进程    │
                    │   (可以不运行)       │
                    │   仅负责扩缩容决策   │
                    └─────────────────────┘
                         ↕ (不需要)
┌──────────┐  gRPC   ┌───────────────────────────────────────┐
│ Dashboard │ ──────→ │          GCS Server                   │
│ /api/     │         │                                       │
│ cluster_  │         │  GcsAutoscalerStateManager            │
│ status    │         │   └─ HandleGetClusterStatus()         │
│ ?format=1 │         │       └─ GetNodeStates()              │
│           │ ←────── │           ├─ GcsNodeManager            │
└──────────┘         │           │   .GetAllAliveNodes() → RUNNING/IDLE/DRAINING │
                     │           │   .GetAllDeadNodes()  → DEAD                  │
                     │           └─ node_resource_info_  → 资源详情              │
                     └───────────────────────────────────────┘
```

**即使没有 autoscaler 进程，GCS Server 自身的 `GcsNodeManager` 通过节点心跳维护着完整的节点生死状态，`GetNodeStates()` 可以直接提供数据。**

---

## 无 Autoscaler 进程时为什么还能查到数据

### 问题场景

用户使用 `NoOpClusterAutoscaler`（`f1a537ee` 提交），且集群没有运行 autoscaler 相关进程，但 Dashboard 首页 Node Status 仍然展示了节点信息。

### 原因

`NoOpClusterAutoscaler` 属于 **Ray Data 层的 `ClusterAutoscaler`**（`python/ray/data/_internal/cluster_autoscaler/`），是 Data executor 用来决定是否触发集群扩缩容的组件，与 Dashboard 显示**完全无关**。

Dashboard 的 Node Status 数据来自 GCS Server 内置的 `GcsAutoscalerStateManager`（C++ 组件），它：
- 随 GCS Server 启动而初始化
- 通过 `GcsNodeManager` 的 listener 回调接收节点 add/dead 事件
- 通过 raylet 上报获取节点资源使用信息（`UpdateResourceLoadAndUsage`）

**这些都是 GCS Server 的核心功能，不依赖任何外部 autoscaler 进程。**

### 各层 Autoscaler 概念辨析

| 组件 | 层级 | 职责 | 是否影响 Node Status 展示 |
|------|------|------|--------------------------|
| `NoOpClusterAutoscaler` | Ray Data executor | 控制 Data pipeline 是否触发集群扩缩容 | **否** |
| `Monitor` + `StandardAutoscaler` (V1) | Autoscaler 进程 | 集群级扩缩容决策 + 写入 KV 状态 | V1 路径下**是** |
| `GcsAutoscalerStateManager` | GCS Server (C++) | 维护节点资源状态、响应 gRPC 查询 | V2 路径下**是** |

### `f1a537ee` 提交的影响范围

该提交仅修改了：
- `python/ray/data/_internal/cluster_autoscaler/__init__.py` — 默认值 `"V2"` → `"NOOP"`
- `python/ray/data/_internal/cluster_autoscaler/noop_cluster_autoscaler.py` — 新增空实现

对 GCS Server、Dashboard API、autoscaler monitor 均**无影响**。

---

## V1 Autoscaler 路径分析：是否也会出现同样的问题？

### V1 的数据链路

```
Monitor._run() 循环 (每 5 秒):
  1. update_load_metrics()              ← 从 GCS 拉负载信息
  2. autoscaler.update()                ← V1 扩缩容决策
  3. autoscaler.summary()               ← 生成 AutoscalerSummary
  4. _internal_kv_put(DEBUG_AUTOSCALING_STATUS, json)  ← 写入 KV
  5. sleep(AUTOSCALER_UPDATE_INTERVAL_S)               ← 默认 5 秒

Dashboard handler:
  → 读取 KV 中的 DEBUG_AUTOSCALING_STATUS
  → json.loads → 构造 AutoscalerSummary
  → format_info_string() → 返回格式化字符串
```

### V1 的节点状态判断逻辑

`python/ray/autoscaler/_private/autoscaler.py:1475-1566`

```python
def summary(self):
    active_nodes = Counter()
    pending_nodes = []
    failed_nodes = []
    non_failed = set()

    for node_id in self.non_terminated_nodes.all_node_ids:
        node_tags = self.provider.node_tags(node_id)
        is_active = self.heartbeat_on_time(node_id, now)

        if is_active:
            active_nodes[node_type] += 1     # 心跳正常 → Active
        else:
            status = node_tags[TAG_RAY_NODE_STATUS]
            is_pending = status not in [STATUS_UP_TO_DATE, STATUS_UPDATE_FAILED]
            if is_pending:
                pending_nodes.append(...)    # 初始化中 → Pending
            # 既不 active 也不 pending → 会被 get_all_failed_node_info 计为 failed

    failed_nodes = self.node_tracker.get_all_failed_node_info(non_failed)
```

V1 的 active 判断依赖 `heartbeat_on_time()`（`autoscaler.py:1210-1235`）：

```python
def heartbeat_on_time(self, node_id, now):
    key = self.provider.internal_ip(node_id)
    if key in self.load_metrics.last_heartbeat_time_by_ip:
        last_heartbeat_time = self.load_metrics.last_heartbeat_time_by_ip[key]
        delta = now - last_heartbeat_time
        if delta < AUTOSCALER_HEARTBEAT_TIMEOUT_S:
            return True
    return False
```

### V1 会出现同样的问题吗？

**会，而且更严重。** 有以下几个原因：

#### 原因 1：KV 缓存的固有延迟（最大 5 秒）

V1 的数据是 monitor 进程每 `AUTOSCALER_UPDATE_INTERVAL_S`（默认 5 秒）写入 KV 的快照。节点在两次写入之间 dead 的话，Dashboard 读到的是过期数据。

而 V2 路径是实时 gRPC 查询，没有这个缓存延迟。

#### 原因 2：心跳超时窗口

V1 的 active 判断依赖心跳超时 `AUTOSCALER_HEARTBEAT_TIMEOUT_S`。在节点实际 dead 到心跳超时之间的窗口期，节点仍被算作 active。

V2 直接使用 GCS `GcsNodeManager` 的 alive/dead 分类，GCS 有自己的 health check 机制（`GcsHealthCheckManager`），通常比 autoscaler 的心跳超时更灵敏。

#### 原因 3：如果 Monitor 进程挂了，数据会永久过期

V1 的 KV 数据完全依赖 monitor 进程持续写入。如果 monitor 进程崩溃或卡住：
- KV 中的数据**不会被任何其他进程更新**
- Dashboard 会一直展示 monitor 最后一次写入的状态
- 已经 dead 的节点会**永久**显示为 Active，直到 monitor 恢复

V2 路径不存在这个问题，因为每次请求都是实时 gRPC 查 GCS。

#### 原因 4：没有运行 autoscaler 进程时，V1 完全无数据

如果集群没有运行 V1 autoscaler monitor，`DEBUG_AUTOSCALING_STATUS` 这个 KV key 根本不会被写入，Dashboard 会显示：

```
No cluster status. It may take a few seconds
for the Ray internal services to start up.
```

### V1 vs V2 对比总结

| 维度 | V1 | V2 |
|------|----|----|
| **数据新鲜度** | 最多延迟 5 秒（KV 快照） | 实时（gRPC 查询） |
| **依赖 autoscaler 进程** | 是，monitor 必须运行 | 否，GCS Server 自带 |
| **active 判断** | 心跳超时（`AUTOSCALER_HEARTBEAT_TIMEOUT_S`） | GCS `GcsNodeManager` alive/dead 状态 |
| **monitor 挂了** | 数据永久过期，dead 节点永久显示为 Active | 不受影响 |
| **显示 dead 为 active 的风险** | **高**：缓存延迟 + 心跳窗口 + monitor 故障 | **低**：仅取决于 GCS health check 检测延迟 |

---

## V2 路径下 Dead 节点显示为 Alive 的可能原因

在确认使用 V2 路径（`is_autoscaler_v2() == True`）的前提下，如果仍然观察到 dead 节点显示为 alive，可能的原因：

### 1. GCS 节点健康检测延迟

GCS 的 `GcsHealthCheckManager` 通过定期 health check 判断节点是否存活。节点实际死亡到 GCS 标记为 DEAD 之间有一个检测窗口，取决于：
- `raylet_heartbeat_period_milliseconds`（心跳间隔）
- `num_heartbeats_timeout`（超时心跳次数）
- `health_check_period_ms` / `health_check_timeout_ms`（健康检查参数）

### 2. `DRAINING` 状态被归入 Active

`_parse_nodes()` 中 `DRAINING` 状态走 `else` 分支，归入 `active_nodes`。正在 drain 的节点虽然即将终止，但仍显示为 Active。这是设计如此（drain 期间仍在运行 workload）。

### 3. `UNSPECIFIED` 状态被归入 Active

如果 `node_state.status` 为 `UNSPECIFIED`（protobuf 默认值 0），也会归入 Active。这可能在节点状态转换的瞬态出现。

### 4. 前端轮询间隔

前端使用 SWR 以 `API_REFRESH_INTERVAL_MS` 间隔轮询，在两次轮询之间节点状态变化不会反映到界面上。

---

## 关键源码索引

| 文件 | 位置 | 职责 |
|------|------|------|
| `python/ray/dashboard/client/src/components/AutoscalerStatusCards.tsx` | :75-87 | NodeStatusCard 前端组件 |
| `python/ray/dashboard/client/src/service/status.ts` | :12 | 前端 API 调用定义 |
| `python/ray/dashboard/client/src/pages/job/hook/useClusterStatus.ts` | — | SWR 数据轮询 Hook |
| `python/ray/dashboard/modules/reporter/reporter_head.py` | :93-145 | `/api/cluster_status` 后端 handler |
| `python/ray/autoscaler/_private/commands.py` | :116-183 | `debug_status()` V1/V2 分流 |
| `python/ray/autoscaler/v2/utils.py` | :1027-1076 | `is_autoscaler_v2()` 判断 |
| `python/ray/autoscaler/v2/sdk.py` | :81-105 | V2 `get_cluster_status()` gRPC 调用 |
| `python/ray/autoscaler/v2/utils.py` | :890-956 | V2 `_parse_nodes()` 节点分类 |
| `python/ray/autoscaler/v2/utils.py` | :364 | V2 `ClusterStatusFormatter.format()` |
| `python/ray/autoscaler/_private/monitor.py` | :370-448 | V1 Monitor 主循环 + KV 写入 |
| `python/ray/autoscaler/_private/autoscaler.py` | :1475-1566 | V1 `summary()` 节点分类 |
| `python/ray/autoscaler/_private/autoscaler.py` | :1210-1235 | V1 `heartbeat_on_time()` |
| `python/ray/autoscaler/_private/util.py` | :850 | V1 `format_info_string()` |
| `python/ray/autoscaler/_private/legacy_info_string.py` | :14-27 | V1 Legacy 格式写入 |
| `python/ray/autoscaler/_private/constants.py` | :64 | `AUTOSCALER_UPDATE_INTERVAL_S = 5` |
| `src/ray/gcs/gcs_autoscaler_state_manager.cc` | :347-453 | C++ `GetNodeStates()` 核心逻辑 |
| `src/ray/gcs/gcs_autoscaler_state_manager.cc` | :163-175 | C++ `HandleGetClusterStatus` gRPC handler |
| `src/ray/gcs/gcs_autoscaler_state_manager.cc` | :270-287 | C++ `OnNodeAdd()` |
| `src/ray/gcs/gcs_autoscaler_state_manager.h` | :89 | C++ `OnNodeDead()` → erase |
| `src/ray/gcs/gcs_server.cc` | :745-747 | GCS 启动时写入 `__autoscaler_v2_enabled` |
| `src/ray/gcs/gcs_server.cc` | :846-861 | NodeRemovedListener 回调链注册 |
| `src/ray/common/ray_config_def.h` | :956 | `enable_autoscaler_v2` 默认值 `false` |
| `src/ray/protobuf/autoscaler.proto` | :115-126 | `NodeStatus` 枚举定义 |
| `python/ray/data/_internal/cluster_autoscaler/noop_cluster_autoscaler.py` | — | NoOp 实现（与 Node Status 无关） |

---

## 根因深度分析（本案例）

### 确认走 V1 路径

本集群的关键配置：
- `RAY_enable_autoscaler_v2`: 未设置
- `__autoscaler_v2_enabled` (GCS KV): `b'0'`
- `ray start` 启动参数无 `enable_autoscaler_v2`
- `enable_autoscaler_v2` 默认为 `false`（`ray_config_def.h:956`）

因此 `is_autoscaler_v2()` 返回 `False`，`debug_status()` 走 V1 分支——从 KV 的 `__autoscaling_status` 读取 monitor 写入的 JSON 快照。

### V1 Monitor 的节点列表构建缺陷

Monitor 的 `update_load_metrics()`（`monitor.py:243-268`）每 5 秒执行一次：

```python
def update_load_metrics(self):
    # 1. 通过 gRPC 从 GCS 获取 ClusterResourceState
    cluster_resource_state = get_cluster_resource_state(self.gcs_client)
    ray_node_states = cluster_resource_state.node_states

    # 2. 把 node_states 中的所有节点传给 ReadonlyNodeProvider
    if self.readonly_config:
        new_nodes = []
        for msg in list(cluster_resource_state.node_states):
            node_id = msg.node_id.hex()
            new_nodes.append((node_id, msg.node_ip_address))
        self.autoscaler.provider._set_nodes(new_nodes)  # ← 不区分 alive/dead!
```

**关键问题在这里**：`get_cluster_resource_state()` 返回的 `node_states` 包含 **alive + dead** 节点（因为 C++ 层 `GetNodeStates()` 遍历了 `GetAllAliveNodes()` + `GetAllDeadNodes()`），但 `_set_nodes()` **全部接收**，不过滤 dead 节点。

### ReadonlyNodeProvider 的无过滤设计

`python/ray/autoscaler/_private/readonly/node_provider.py:28-43`

```python
def _set_nodes(self, nodes: List[Tuple[str, str]]):
    new_nodes = {}
    for node_id, node_manager_address in nodes:
        new_nodes[node_id] = {
            "node_type": format_readonly_node_type(node_id),
            "ip": node_manager_address,
        }
    self.nodes = new_nodes  # ← 直接替换，不检查节点是否 alive

def non_terminated_nodes(self, tag_filters):
    return list(self.nodes.keys())  # ← 返回所有节点，包括 dead 的
```

### 心跳判断在 Readonly 模式下失效

`summary()` 中通过 `heartbeat_on_time()` 判断节点是否 active：

```python
def heartbeat_on_time(self, node_id, now):
    key = self.provider.internal_ip(node_id)  # ReadonlyNodeProvider 返回 node_id 本身
    if key in self.load_metrics.last_heartbeat_time_by_ip:
        ...
```

但 `ReadonlyNodeProvider.internal_ip()` 返回的是 `node_id`（而非真实 IP），而 `load_metrics.last_heartbeat_time_by_ip` 以**真实 IP** 为 key。

因此心跳查找可能匹配不上 → `heartbeat_on_time` 返回 `False` → 节点不算 active → 按理应该进入 failed。

**但实际观察到 KV 中 14 个节点全部显示为 active。** 这说明在 readonly config 模式下，`update_load_metrics()` 中还有一段逻辑（`monitor.py:279-333`）会把所有来自 `cluster_resource_state.node_states` 的节点直接注册到 `load_metrics` 的心跳表中：

```python
for resource_message in cluster_resource_state.node_states:
    node_id = resource_message.node_id
    # ... 解析 total_resources, available_resources ...
    ip = resource_message.node_ip_address
    use_node_id_as_ip = ... .get("use_node_id_as_ip", False)
    if use_node_id_as_ip:
        ip = node_id.hex()
    # 更新 load_metrics（包括心跳时间）
    self.load_metrics.update(ip, ...)
```

这段代码为**所有** `node_states`（包括 dead 节点）更新了心跳时间，导致 `heartbeat_on_time()` 对 dead 节点也返回 `True`，从而将它们算作 active。

### 完整的 Bug 链路

```
1. C++ GetNodeStates() 返回 alive + dead 节点列表（设计如此，供 V2 使用）
   ↓
2. Monitor.update_load_metrics() 调用 get_cluster_resource_state()
   ↓
3. _set_nodes() 把 alive + dead 节点全部加入 ReadonlyNodeProvider（不过滤 dead）
   ↓
4. load_metrics.update() 为所有节点更新心跳时间（不检查节点 dead 状态）
   ↓
5. summary() → heartbeat_on_time() → True（因为刚更新过心跳）
   ↓
6. dead 节点被计入 active_nodes
   ↓
7. 写入 KV → Dashboard 展示 dead 节点为 Active
```

### 为什么 V2 不受影响

V2 路径在 Python 层的 `_parse_nodes()`（`v2/utils.py:949-954`）**显式检查 `node_state.status == NodeStatus.DEAD`**：

```python
if node_state.status == NodeStatus.DEAD:
    dead_nodes.append(node_info)      # ← 明确归入 dead
elif node_state.status == NodeStatus.IDLE:
    idle_nodes.append(node_info)
else:
    active_nodes.append(node_info)
```

而 V1 的 `summary()` 不检查 protobuf 的 `NodeStatus`，而是依赖心跳判断，加上 `update_load_metrics()` 为所有节点（包括 dead）更新了心跳，导致 dead 节点被误判为 active。

---

## 排查建议

1. **确认 autoscaler 版本**：检查 `RAY_enable_autoscaler_v2` 环境变量和 `--system-config` 中的 `enable_autoscaler_v2` 值
2. **确认延迟时长**：如果是秒级延迟，属于 GCS health check 正常检测窗口；如果是分钟级甚至更长，可能需要排查 GCS health check 配置或 GCS Server 本身的问题
3. **检查 GCS 日志**：搜索 `OnNodeDead` 相关日志，确认 GCS 是否及时检测到节点死亡
4. **直接查 gRPC**：通过 `ray status` 命令（V2 下会走实时 gRPC）对比 Dashboard 展示，确认数据源是否一致
5. **修复方案**：在 `monitor.py:262-268` 的 `_set_nodes` 调用前过滤掉 dead 节点，或者启用 V2 autoscaler（`RAY_enable_autoscaler_v2=1`）从根本上规避此问题

### 快速诊断脚本

```python
import ray, json
ray.init(address="auto", ignore_reinit_error=True)

# 对比 GCS 实际状态 vs KV 快照
nodes = ray.nodes()
alive = [n for n in nodes if n["Alive"]]
dead = [n for n in nodes if not n["Alive"]]
print(f"GCS: Total={len(nodes)} Alive={len(alive)} Dead={len(dead)}")

from ray.experimental.internal_kv import _internal_kv_get
status_raw = _internal_kv_get("__autoscaling_status", namespace=None)
if status_raw:
    d = json.loads(status_raw)
    active = d.get("autoscaler_report", {}).get("active_nodes", {})
    kv_ids = set(name.replace("node_", "") for name in active.keys())
    gcs_alive_ids = set(n["NodeID"] for n in alive)
    phantom = kv_ids - gcs_alive_ids
    print(f"KV active: {len(kv_ids)}, Phantom: {len(phantom)}")
    for nid in phantom:
        m = [n for n in dead if n["NodeID"] == nid]
        print(f"  {nid[:16]}: {'DEAD in GCS' if m else 'NOT in GCS'}")
```
