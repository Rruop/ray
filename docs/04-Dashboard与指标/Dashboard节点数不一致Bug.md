# Ray Dashboard `/nodes?view=summary` 节点数不一致 Bug 深度分析

## 问题描述

从 Ray Dashboard 的 `/nodes?view=summary` 接口获取到的 cluster alive 节点数量，与 `ray list nodes` 命令结果不一致。

- **环境**: `https://search-kibana-sgp.corp.kuaishou.com/nodes?view=summary`
- **集群**: `kml-hb2az1-l3-2`, namespace `lmserving`
- **Ray 版本**: 2.54.0 (`ray_commit=cc121a56b92f65ea80d7ff9b64df4acfd4f9714a`)
- **集群启动时间**: 2026-05-13 10:14:14

---

## 数据对比

### 两个接口的数据差异

| 数据来源 | alive | dead | total |
|---------|-------|------|-------|
| `/nodes?view=summary` (Dashboard 内存缓存) | **2900** | 1000 | 3906 (含1000重复) |
| `ray list nodes --limit 10000` (State API -> GCS gRPC) | **1900** | 1000 | 2900 |
| `ray list nodes` 默认 (limit=100) | 100 (被截断) | - | - |

### 关键发现

通过精确的集合对比确认：

```
Dashboard alive node IDs: 2900 个 (含重复)
GCS alive node IDs:       1900 个
Phantom 节点 (Dashboard=ALIVE, GCS=DEAD): 1000 个
GCS 中有但 Dashboard 没有的: 0 个
```

**1000 个 phantom 节点在 GCS 中全部是 DEAD 状态，但在 Dashboard 中仍显示为 ALIVE。**

更关键的发现：**Dashboard 的 `/nodes?view=summary` 返回了 3906 条记录，但只有 2906 个唯一 node ID，有 1000 个 node ID 各出现了 2 次**（一次 ALIVE，一次 DEAD）。

---

## 两套接口的实现差异

### 1. `/nodes?view=summary` (Dashboard REST API)

**代码路径**:

```
python/ray/dashboard/modules/node/node_head.py   -- 路由 handler (line 385)
python/ray/dashboard/modules/node/datacenter.py   -- DataOrganizer.get_all_node_summary() (line 177)
python/ray/dashboard/modules/node/datacenter.py   -- DataSource.nodes (line 19)
```

**工作方式**:

1. **数据源**: Dashboard 进程内的内存缓存 `DataSource.nodes`（一个 Python dict）
2. **初始化**: 启动时通过 `async_get_all_node_info()` 获取 GCS 全量快照（无 limit），然后通过 pub/sub 接收增量更新
3. **状态更新**: 通过 `_update_node()` 处理每个节点状态变更（ALIVE/DEAD）
4. **Dead node 缓存**: 最多保留 `MAX_DEAD_NODES_TO_CACHE=1000` 个死节点，超过后驱逐最老的
5. **无分页/无 limit**: 返回 `DataSource.nodes` 中的所有节点，响应可以非常大
6. **响应大小**: 本集群为 **33.5 MB**

**关键代码** (`node_head.py:188-236`):

```python
async def _subscribe_for_node_updates(self) -> AsyncGenerator[dict, None]:
    subscriber = GcsAioNodeInfoSubscriber(address=self.gcs_address)
    await subscriber.subscribe()
    # 仅此一次全量快照
    all_node_info = await self.gcs_client.async_get_all_node_info(timeout=None)
    for node in all_node_infos:
        yield node
    # 之后永远只靠增量 pub/sub
    while True:
        try:
            node_id_updated_info_tuples = await subscriber.poll(batch_size=200)
            for node in updated_infos:
                yield node
        except Exception:
            logger.exception("Failed handling updated nodes.")
```

### 2. `ray list nodes` / `/api/v0/nodes` (State API)

**代码路径**:

```
python/ray/util/state/api.py              -- list_nodes() (line 885)
python/ray/dashboard/state_aggregator.py   -- StateAPIManager.list_nodes() (line 177)
python/ray/util/state/state_manager.py     -- StateDataSourceClient.get_all_node_info() (line 308)
src/ray/gcs/gcs_node_manager.cc            -- HandleGetAllNodeInfo() (line 235)
```

**工作方式**:

1. **数据源**: 每次请求直接通过 gRPC 查询 GCS
2. **默认 limit**: CLI 默认 100，API 默认 10000
3. **GCS 侧硬上限**: `RAY_MAX_LIMIT_FROM_DATA_SOURCE=10000`
4. **支持 state_filter**: 可在 GCS C++ 侧过滤 `alive_nodes_` / `dead_nodes_`
5. **截断漏斗**:
   ```
   GCS 全量 → RAY_MAX_LIMIT_FROM_DATA_SOURCE (10000) 截断
            → Python 侧 filter
            → min(RAY_MAX_LIMIT_FROM_API_SERVER, user_limit) 截断
   ```

### 关键差异总结

| 对比项 | `/nodes?view=summary` | `ray list nodes` |
|--------|----------------------|------------------|
| 数据源 | Dashboard 内存缓存 (pub/sub 维护) | 直接 gRPC 查询 GCS |
| 默认 limit | **无** (返回全部) | **100** (CLI) / 10000 (API) |
| 包含 dead 节点 | 是 (alive + dead 混合) | 可通过 `--filter state=ALIVE` 过滤 |
| 响应大小 | 无上限 (本集群 33.5MB) | 受 limit 约束 |
| 数据新鲜度 | 增量更新 (可能滞后) | 每次查 GCS 实时数据 |

---

## 根因分析

### Bug 定位: `node_head.py:285` 变量名覆盖

**Bug 代码** (`python/ray/dashboard/modules/node/node_head.py:283-288`):

```python
async def _update_node(self, node: dict):
    node_id = node["nodeId"]  # hex
    # ... (省略 ALIVE 处理逻辑)
    assert node["state"] in ["ALIVE", "DEAD"]
    is_alive = node["state"] == "ALIVE"
    if not is_alive:
        # ... (省略 KV 删除逻辑)

        self._dead_node_queue.append(node_id)
        if len(self._dead_node_queue) > node_consts.MAX_DEAD_NODES_TO_CACHE:
            node_id = self._dead_node_queue.popleft()  # ← BUG: 覆盖了函数参数 node_id
            DataSource.nodes.pop(node_id, None)         # 删除的是被驱逐的旧 dead node ✓
            self._stubs.pop(node_id, None)              # 删除的是被驱逐的旧 dead node ✓
    DataSource.nodes[node_id] = node                    # ← BUG: 写到了错误的 key 上！
```

### Bug 触发机制

当 `_dead_node_queue` 长度超过 `MAX_DEAD_NODES_TO_CACHE`（默认 1000）时:

**假设场景**: 收到节点 `dead_new` 的 DEAD 通知，queue 已满

```
步骤 1: node_id = "dead_new"                          # 函数参数
步骤 2: _dead_node_queue.append("dead_new")            # queue: [..., dead_new]
步骤 3: len(queue) > 1000 → True
步骤 4: node_id = _dead_node_queue.popleft()           # node_id 被覆盖为 "dead_oldest"
步骤 5: DataSource.nodes.pop("dead_oldest")            # 删除最老的 dead node ✓
步骤 6: DataSource.nodes["dead_oldest"] = dead_new_data  # ← 写到了错误的 key！
```

**结果**:
- `"dead_new"` 的原始 ALIVE 数据在 `DataSource.nodes` 中**永远不会被更新为 DEAD** → 变成 **phantom ALIVE 节点**
- `"dead_oldest"` 先被删除，又被以错误数据重新写入 → 一个 key 和 value 不匹配的 DEAD 条目

### 触发条件

1. 集群运行期间累计 dead node 超过 1000 个
2. 每超过 1000 后的每一个新 DEAD 通知都会产生一个 phantom ALIVE 节点
3. **不需要任何网络问题、GCS failover、pub/sub 丢失** — 纯粹是代码逻辑 Bug

### 验证数据

| 验证项 | 值 | 说明 |
|--------|------|------|
| Dashboard 重复 node ID | **1000 个** | 每个 phantom 有 ALIVE(旧) + DEAD(被错放到别的key) |
| Phantom 全在 GCS dead_nodes 中 | **1000/1000** | GCS 正确标记 DEAD，Dashboard 没更新 |
| Dashboard "Cannot reach" 错误 | **422,262 条**覆盖 **1052 个节点** | phantom 被 `_update_node_stats` 反复尝试连接 |
| Phantom 与 unreachable 交集 | **1000/1000** | 完全重叠 |
| Dashboard 日志无 pub/sub 错误 | `Failed handling: 0` | 确认不是消息丢失 |
| GCS 无 failover | 日志无 restart/failover 记录 | 确认不是 GCS 重启 |

---

## 完整排查过程

### 排查思路总览

```
初始假设: 响应太大导致数据截断 或 ray list nodes 默认 limit 截断
    ↓ 源码阅读 + 实际数据验证
修正为假设 A: pub/sub 消息丢失 (Dashboard 未收到 DEAD 通知)
    ↓ 日志排查：无 failover、无 gRPC 错误、无处理异常
排除假设 A → 深入讨论 pub/sub 消息丢失的理论可能性
    ↓ 精确集合对比：发现恰好 1000 个 phantom
发现数字巧合：phantom 数 = MAX_DEAD_NODES_TO_CACHE = 1000
    ↓ 检查重复 node ID
发现 1000 个 node ID 在 API 返回中各出现 2 次 (ALIVE + DEAD)
    ↓ 审查 _update_node 代码
定位根因: 变量名覆盖 Bug (node_id = popleft() 覆盖了函数参数)
    ↓ "Cannot reach" 日志交叉验证
1000 phantom 与 1052 unreachable 交集 = 1000，完全吻合
```

### 第一阶段: 源码分析 — 理解两个接口的实现差异

**目的**: 在去集群验证前，先从源码层面理解 `/nodes?view=summary` 和 `ray list nodes` 为什么可能给出不同结果。

**分析方法**: 沿代码路径逐层追踪，对比两个接口的数据源和处理逻辑。

1. **Dashboard API 路径分析**:

   - `node_head.py:385` — HTTP GET `/nodes` 路由 handler
   - `datacenter.py:177` — `get_all_node_summary()` 遍历 `DataSource.nodes.keys()`
   - `DataSource.nodes` 是一个 Python dict，通过 pub/sub 增量维护
   - 启动时通过 `async_get_all_node_info(timeout=None)` 从 GCS 获取全量快照（`gcs_client.pxi:377`），**无 limit 参数**
   - 底层 C++ 调用 `NodeInfoAccessor::AsyncGetAll`（`accessor.cc:287`），构造的 `GetAllNodeInfoRequest` **不设 limit**
   - GCS 侧（`gcs_node_manager.cc:239-240`）: `limit = max_int64`，返回所有节点
   - 返回 alive + dead 混合数据，**无分页、无大小限制**

2. **State API 路径分析**:

   - `state_manager.py:308` — `get_all_node_info()` 每次通过 gRPC 直接查 GCS
   - 发送 `GetAllNodeInfoRequest` 带 `limit` 参数，默认 `RAY_MAX_LIMIT_FROM_DATA_SOURCE=10000`（`common.py:59`）
   - 支持 `state_filter`（ALIVE/DEAD），在 GCS C++ 侧过滤只遍历对应的 map
   - CLI 默认 `DEFAULT_LIMIT=100`（`common.py:48`），**大集群会被截断**

3. **初步结论**: 三个可能的差异来源:

   - `ray list nodes` 默认 limit=100 导致截断（最直接的原因）
   - `/nodes?view=summary` 返回 alive+dead 混合数据，如果不按 state 过滤会把 dead 也算进去
   - Dashboard 内存缓存与 GCS 实际状态不一致（pub/sub 延迟或丢失）

### 第二阶段: 集群实际数据采集

**目的**: 获取真实的数字来定量描述差异。

通过 KML Web Shell 连接到集群 head 节点 pod 执行命令。

1. **Dashboard API 数据**:

```bash
curl -s http://localhost:8265/nodes?view=summary | python3 -c "
import sys,json
data = json.load(sys.stdin)
nodes = data['data']['summary']
alive = [n for n in nodes if n['raylet']['state'] == 'ALIVE']
dead = [n for n in nodes if n['raylet']['state'] == 'DEAD']
print(f'Total: {len(nodes)}, Alive: {len(alive)}, Dead: {len(dead)}')
"
# 结果: Total: 3828, Alive: 2828, Dead: 1000
```

2. **State API 数据** (注意 limit 问题):

```bash
# 默认 limit=100，被截断
ray list nodes --filter state=ALIVE 2>&1 | grep -E '(Total|truncated)'
# UserWarning: Limit last 100 entries (Total 1900).
# Total: 100

# 指定 limit=10000
ray list nodes --filter state=ALIVE --limit 10000 2>&1 | grep 'Total:'
# Total: 1900
```

3. **响应大小确认**:

```bash
curl -s http://localhost:8265/nodes?view=summary | python3 -c "
import sys; print(f'Response size: {len(sys.stdin.read())} bytes')
"
# 结果: 33,542,665 bytes (33.5 MB)
```

4. **State API HTTP 接口详细数据**:

```bash
curl -s 'http://localhost:8265/api/v0/nodes?limit=10000&detail=false' > /tmp/stateapi_nodes.json
python3 -c '
import json
data = json.load(open("/tmp/stateapi_nodes.json"))
r = data["data"]["result"]
print(f"total: {r['total']}")
print(f"num_after_truncation: {r['num_after_truncation']}")
nodes = r["result"]
alive = [n for n in nodes if n["state"] == "ALIVE"]
dead = [n for n in nodes if n["state"] == "DEAD"]
print(f"result_alive: {len(alive)}, result_dead: {len(dead)}, result_len: {len(nodes)}")
'
# total: 2900
# num_after_truncation: 2900
# result_alive: 1900, result_dead: 1000, result_len: 2900
```

**阶段结论**: Dashboard 报告 2828 alive（后续刷新时变为 2900），GCS 报告 1900 alive。差异约 1000 个节点。Dead 数量两者一致（1000）。总量差异: Dashboard 3828 vs GCS 2900 = 928（后续刷新后变为 1000）。

### 第三阶段: 排查假设 A — pub/sub 消息丢失

**思路**: 既然 Dashboard 是通过 pub/sub 增量接收 node state 更新的，差异最可能来自 Dashboard 丢失了部分 DEAD 通知。

#### 3.1 检查 GCS Failover

GCS 重启会导致新 publisher 无旧消息，Dashboard 不重新拉快照。

```bash
# 检查 GCS 日志中的 failover 记录
grep -i 'restart\|failover\|recover' /tmp/ray/session_latest/logs/gcs_server.out | tail -10
# 仅有无关的 JSON 报文中包含 "actor_to_restart" 字样，无实际 failover

# 确认 GCS 启动时间
head -5 /tmp/ray/session_latest/logs/gcs_server.out
# [2026-05-13 10:14:14,394] gcs_server_main.cc:98: Ray cluster metadata ray_version=2.54.0
```

**结论**: GCS 无 failover，与 Dashboard 同时启动于 10:14。排除 GCS 重启导致消息丢失。

#### 3.2 检查 gRPC 连接问题

Dashboard 的 pub/sub subscriber 使用 gRPC long-poll。如果 gRPC 返回 `UNAVAILABLE` 或 `DEADLINE_EXCEEDED`，`_should_terminate_polling` 会静默返回 True（`gcs_pubsub.py:63-71`），不留日志。

```bash
# 检查 Dashboard 主日志中的 gRPC 错误
grep -c 'UNAVAILABLE\|StatusCode' /tmp/ray/session_latest/logs/dashboard.log
# 结果: 0

# 检查 Dashboard 中 subscriber 相关日志
grep -i 'gcs\|restart\|failover\|reconnect\|subscriber' /tmp/ray/session_latest/logs/dashboard.log | wc -l
# 结果: 1 (仅模块加载日志)
```

**结论**: Dashboard 日志中无 gRPC 错误。但由于 `_should_terminate_polling` 静默处理 DEADLINE_EXCEEDED 和 UNAVAILABLE（不 raise、不 log），日志为空并不能完全排除这种场景。

#### 3.3 检查 pub/sub 消息处理异常

如果 `_subscribe_for_node_updates` 的 `try` 块内发生异常，异常会被 `except Exception: logger.exception(...)` 捕获并记录。

```bash
grep -c 'Failed handling' /tmp/ray/session_latest/logs/dashboard.log
# 结果: 0

grep -c 'Failed handling' /tmp/ray/session_latest/logs/dashboard_NodeHead.log
# 结果: 0
```

**结论**: 无处理异常。pub/sub 消息确实被正常接收和处理了。

#### 3.4 检查 Publisher 侧 subscriber 清理

GCS 的 `CheckDeadSubscribers` 周期性运行，如果 subscriber 超过 `subscriber_timeout_ms`（默认 300s）无活跃连接，会销毁 subscriber state 和 mailbox。

```bash
grep 'Publisher.CheckDeadSubscribers' /tmp/ray/session_latest/logs/gcs_server.out | tail -5
# Publisher.CheckDeadSubscribers - 33 total (1 active), Execution time: mean = 19.27ms...
```

注意 `33 total` 表示 `CheckDeadSubscribers` 总共执行了 33 次，但 `(1 active)` 表示始终只有 1 个活跃 subscriber。这是正常的——Dashboard 只有一个 subscriber。

```bash
# 检查是否有 subscriber 被注销
grep 'Unregistering subscriber' /tmp/ray/session_latest/logs/gcs_server.out
# 结果: 无 (但这是 DEBUG 级别日志，当前日志级别看不到)
```

**结论**: 无法从当前日志级别直接确认 subscriber 是否被清理过。但日志显示 `CheckDeadSubscribers` 持续运行，且始终有 1 个活跃 subscriber，说明 Dashboard subscriber 一直保持活跃。

#### 3.5 阶段性判断

所有已知的 pub/sub 消息丢失场景（GCS failover、gRPC 断连、处理异常、subscriber 被清理）在日志中均未找到直接证据。但差异确实存在。需要进一步精确定位哪些节点有差异。

### 第四阶段: 精确集合对比 — 找出 phantom 节点

**思路**: 不再猜测原因，而是精确导出两个数据源的 node ID 集合，计算差集，用数据说话。

1. **导出 Dashboard alive node ID 集合**:

```bash
curl -s http://localhost:8265/nodes?view=summary | python3 -c '
import sys,json
data = json.load(sys.stdin)
alive_ids = sorted(set(
    n["raylet"]["nodeId"]
    for n in data["data"]["summary"]
    if n["raylet"]["state"] == "ALIVE"
))
open("/tmp/dash_alive.txt","w").write("\n".join(alive_ids))
print("Dashboard alive:", len(alive_ids))
'
# Dashboard alive: 2900
```

2. **导出 GCS alive node ID 集合** (通过 State API HTTP 接口):

```bash
curl -s 'http://localhost:8265/api/v0/nodes?limit=10000&detail=false' > /tmp/stateapi_nodes.json
python3 -c '
import json
data = json.load(open("/tmp/stateapi_nodes.json"))
nodes = data["data"]["result"]["result"]
alive = sorted(n["node_id"] for n in nodes if n["state"] == "ALIVE")
open("/tmp/gcs_alive.txt","w").write("\n".join(alive))
print("GCS alive:", len(alive))
'
# GCS alive: 1900
```

> **注意**: `ray list nodes --filter state=ALIVE -f yaml` 命令因为 `--filter` 和 `-f yaml` 的参数格式冲突会报错 `ValueError: Cannot find the predicate`。实际排查中改用了 State API 的 HTTP 接口获取 JSON 数据。

3. **计算差集**:

```bash
# Dashboard 有但 GCS 没有的 (phantom)
comm -23 /tmp/dash_alive.txt /tmp/gcs_alive.txt > /tmp/phantom.txt
wc -l /tmp/phantom.txt
# 1000

# GCS 有但 Dashboard 没有的
comm -13 /tmp/dash_alive.txt /tmp/gcs_alive.txt | wc -l
# 0 (GCS 所有 alive 节点在 Dashboard 中都存在)
```

4. **验证 phantom 节点在 GCS 中的真实状态**:

```python
python3 -c '
import json
data = json.load(open("/tmp/stateapi_nodes.json"))
nodes = data["data"]["result"]["result"]
dead_ids = set(n["node_id"] for n in nodes if n["state"] == "DEAD")
phantoms = open("/tmp/phantom.txt").read().strip().split("\n")
in_dead = sum(1 for p in phantoms if p in dead_ids)
not_in_any = sum(1 for p in phantoms if p not in dead_ids)
print(f"Phantom total: {len(phantoms)}")
print(f"In GCS dead_nodes: {in_dead}")
print(f"NOT in GCS at all (evicted): {not_in_any}")
'
# Phantom total: 1000
# In GCS dead_nodes: 1000
# NOT in GCS at all (evicted): 0
```

**关键结论**: 1000 个 phantom 节点在 GCS 中全部是 DEAD 状态，0 个被从 GCS 中驱逐。这说明 Dashboard **确实收到了 GCS 的数据**（因为 GCS 知道它们 DEAD），但 Dashboard 内存中它们仍然是 ALIVE。

**注意到的数字巧合**: phantom 数量 = `MAX_DEAD_NODES_TO_CACHE` = 1000。这不太像是随机的 pub/sub 消息丢失，更像是跟 dead node 缓存驱逐逻辑有关。

### 第五阶段: 发现重复 node ID — 推翻 pub/sub 假设

**思路**: 既然 phantom 数量恰好等于 `MAX_DEAD_NODES_TO_CACHE`，且 phantom 节点在 GCS 中确实是 DEAD，那 Dashboard 是否在内部保存了这些节点的 DEAD 信息但没有正确覆盖 ALIVE？

```bash
# 检查 phantom 节点在 Dashboard 中的状态分布
curl -s http://localhost:8265/nodes?view=summary | python3 -c '
import sys,json
d = json.load(sys.stdin)
phantoms = set(open("/tmp/phantom.txt").read().strip().split("\n"))
pn = [n for n in d["data"]["summary"] if n["raylet"]["nodeId"] in phantoms]
states = {}
for n in pn:
    s = n["raylet"]["state"]
    states[s] = states.get(s, 0) + 1
print("Dashboard state for phantoms:", states)
'
# Dashboard state for phantoms: {'ALIVE': 1000, 'DEAD': 1000}
```

**发现**: 1000 个 phantom node ID 在 Dashboard 返回中**同时出现了 ALIVE 和 DEAD 两条记录**！

进一步确认重复:

```bash
curl -s http://localhost:8265/nodes?view=summary | python3 -c '
import sys, json
from collections import Counter
d = json.load(sys.stdin)
nodes = d["data"]["summary"]
all_ids = [n["raylet"]["nodeId"] for n in nodes]
c = Counter(all_ids)
dups = [(nid, cnt) for nid, cnt in c.items() if cnt > 1]
print(f"Total entries: {len(all_ids)}")
print(f"Unique node IDs: {len(set(all_ids))}")
print(f"Duplicates: {len(all_ids) - len(set(all_ids))}")
print(f"Nodes with >1 entry: {len(dups)}")
'
# Total entries: 3906
# Unique node IDs: 2906
# Duplicates: 1000
# Nodes with >1 entry: 1000
```

**矛盾**: `DataSource.nodes` 是一个 Python dict（key=node_id），dict 不可能有重复 key。`get_all_node_summary()` 遍历 `DataSource.nodes.keys()` 时每个 key 只出现一次。

**解释**: dict 本身没有重复 key，但由于 Bug 导致同一个节点的数据被写到了**两个不同的 key** 下——它自己的原始 key（保持 ALIVE 旧数据）和被驱逐的旧 dead node 的 key（错误写入了当前 DEAD 数据）。从 API 返回看，同一个 `nodeId` 出现在两个不同 dict entry 的 `raylet.nodeId` 字段中。

### 第六阶段: 代码审查 — 定位 Bug

**思路**: 审查 `_update_node()` 中 dead node 驱逐逻辑，特别关注变量名和作用域。

```python
# node_head.py:283-288
self._dead_node_queue.append(node_id)
if len(self._dead_node_queue) > node_consts.MAX_DEAD_NODES_TO_CACHE:
    node_id = self._dead_node_queue.popleft()  # ← 变量名 node_id 被覆盖!
    DataSource.nodes.pop(node_id, None)
    self._stubs.pop(node_id, None)
DataSource.nodes[node_id] = node                # ← 此时 node_id 是被弹出的旧 ID
```

第 285 行 `node_id = self._dead_node_queue.popleft()` 使用了与函数参数同名的变量 `node_id`，导致后续第 288 行 `DataSource.nodes[node_id] = node` 写到了错误的 key。

**这是经典的 Python 变量作用域问题**: `if` 块内没有独立作用域，`node_id` 赋值直接覆盖了外层的 `node_id`。

### 第七阶段: 交叉验证 — "Cannot reach the node" 日志

**思路**: 如果 phantom ALIVE 节点是实际已死的，那 Dashboard 的 `_update_node_stats` 每 15 秒对它们发 gRPC 请求时应该会失败。

```bash
# 统计 "Cannot reach" 错误数
grep -c 'Cannot reach the node' /tmp/ray/session_latest/logs/dashboard_NodeHead.log
# 422262

# 时间范围 (10:14 启动到 13:12)
# First: 2026-05-13 10:14:50,244
# Last:  2026-05-13 13:12:43,701

# 唯一不可达节点数
# Unique unreachable nodes: 1052

# 每个节点被重试次数 (top 5)
# 9c66a09d18eaef98... : 462 times
# 63f9b7a55b9a3de5... : 462 times
# 957247e266fb97f1... : 462 times (每 15s 一次 × 3h ≈ 720, 部分时段可能未统计)

# 与 phantom 的交集
# Unreachable nodes: 1052
# Overlap with phantoms: 1000
# Unreachable NOT in phantom (recently dead, not yet bug-affected): 52
```

**完全吻合**: 1052 个不可达节点中，1000 个与 phantom 完全重叠，52 个是最近刚死的（DEAD 通知已收到但尚未轮到下一次 stats 更新检查）。

---

## 关于 pub/sub 消息丢失的深度讨论

在定位到真正的根因之前，我们对"pub/sub 消息丢失是否会导致 Dashboard 节点状态不一致"进行了深入的源码分析和理论讨论。**虽然最终证实本次问题的根因不是 pub/sub 消息丢失，但这个分析对理解 Ray pub/sub 的可靠性机制和潜在风险仍有价值。**

### Ray Pub/Sub 架构

```
Dashboard (Python)                              GCS Server (C++)
┌──────────────────────┐                     ┌────────────────────────┐
│ GcsAioNodeInfoSubscriber │  gRPC long-poll │ Publisher               │
│                          │ ←──────────────→│  ├─ SubscriptionIndex   │
│ _subscriber_id           │                 │  │   └─ EntityState     │
│ _publisher_id            │  PollRequest    │  │       ├─ subscribers_│
│ _max_processed_seq_id    │  (seq_id, ...)  │  │       └─ pending_msgs│
│ _queue (deque)           │                 │  └─ subscribers_ map    │
│                          │  PollReply      │      └─ SubscriberState │
│ gcs_pubsub.py            │  (messages)     │          ├─ mailbox_    │
└──────────────────────────┘                 │          ├─ reply_      │
                                             │          └─ last_time_  │
                                             │                         │
                                             │  publisher.cc           │
                                             └─────────────────────────┘
```

### Long-Poll 工作流

```
Subscriber                                   Publisher
    |                                            |
    |--- PollRequest(seq_id=100) -------------->|
    |                                            | (持有 request，等待新消息)
    |                                            | (消息 101, 102 到达 → 入 mailbox)
    |<-- PollReply(msgs=[101, 102]) ------------|
    |                                            |
    | (处理消息, 更新 seq_id=102)                  |
    |--- PollRequest(seq_id=102) -------------->|
    |                                            | (mailbox 中 <=102 的消息被清理)
    |                                            | ...
```

### 消息投递保证分析

#### GCS_NODE_INFO_CHANNEL 的 buffer 配置

```cpp
// publisher.cc:257-262 — CreateEntityState
case rpc::ChannelType::GCS_NODE_INFO_CHANNEL:
    // Critical if messages are dropped.
    return std::make_unique<EntityState>(
        RayConfig::instance().max_grpc_message_size(),
        /*max_buffered_bytes=*/-1);  // 无限缓冲，不因 buffer 满而丢弃
```

与非关键 channel（如 `RAY_NODE_RESOURCE_USAGE_CHANNEL` 使用有限 buffer）不同，node info channel 被标记为"消息丢弃是致命的"，使用无限缓冲。

#### Mailbox 消息保留机制

```cpp
// publisher.cc:282-286 — ConnectToSubscriber
// 只有 subscriber 确认收到的消息才从 mailbox 删除
while (!mailbox_.empty() &&
       mailbox_.front()->sequence_id() <= max_processed_sequence_id) {
    mailbox_.pop_front();
}
```

消息在 subscriber 通过 `max_processed_sequence_id` 确认之前，**始终保留在 publisher 的 mailbox 中**。

#### PublishIfPossible 不删除 mailbox 消息

```cpp
// publisher.cc:306-349 — PublishIfPossible
void SubscriberState::PublishIfPossible(bool force_noop) {
    if (!long_polling_connection_) return;
    // 遍历 mailbox 中的消息，拷贝到 reply 中发送
    for (auto it = mailbox_.begin(); it != mailbox_.end(); it++) {
        *long_polling_connection_->pub_messages_->Add() = msg;
    }
    // 发送 reply
    long_polling_connection_->send_reply_callback_(Status::OK(), ...);
    long_polling_connection_.reset();
    // 注意: mailbox 中的消息没有被删除！只在 ConnectToSubscriber 时清理
}
```

### 场景分析: 简单 gRPC 超时是否会丢消息？

**结论: 不会。系统会自愈。**

详细流程:

```
1. Subscriber 发送 PollRequest(seq_id=100)
2. Publisher 持有该请求 (long_polling_connection_ = reply callback)
3. gRPC 超时 (DEADLINE_EXCEEDED, 比如 30s 后)
   └─ Subscriber 侧: _should_terminate_polling() → True, _poll() 正常返回
   └─ Publisher 侧: long_polling_connection_ 仍持有 (gRPC server 端不知道 client 超时)
4. Subscriber 立即重新 poll (seq_id 仍=100, 因为没有处理任何消息)
5. Publisher 收到新 PollRequest → ConnectToSubscriber():
   ├─ 检查 mailbox: 保留 seq>100 的所有消息 (从未被清理)
   ├─ 旧 reply_ 被 flush (PublishIfPossible(force_noop=true))
   ├─ 新 reply_ 被存储
   └─ PublishIfPossible(): 如果 mailbox 有消息，立即发送
6. ✅ 消息不丢失，完整恢复
```

Python 侧的处理 (`gcs_pubsub.py:125-162`):

```python
async def _poll(self, timeout=None) -> None:
    while len(self._queue) == 0:
        req = self._poll_request()  # 包含 max_processed_sequence_id
        try:
            poll = ... await self._poll_call(req, timeout=timeout)
            # 正常处理消息
            for msg in poll.result().pub_messages:
                if msg.sequence_id <= self._max_processed_sequence_id:
                    continue  # 跳过已处理的 (去重)
                self._max_processed_sequence_id = msg.sequence_id
                self._queue.append(msg)
        except grpc.RpcError as e:
            if self._should_terminate_polling(e):
                return  # 静默返回，_queue 为空
                        # 外层循环会重新调用 poll()
            raise
```

**关键**: `_max_processed_sequence_id` 在 `DEADLINE_EXCEEDED` 时不会被推进（因为没有处理任何消息），下次 poll 会带相同的 seq_id，publisher 从 mailbox 中重放。

### 场景分析: 什么条件下消息会永久丢失？

#### 场景 1: Publisher 清理 subscriber state (唯一的非 failover 丢失路径)

**条件**: Subscriber 连续 `subscriber_timeout_ms`（默认 300s）无法成功 poll。

```cpp
// publisher.cc:476-498 — CheckDeadSubscribers (每 subscriber_timeout_ms 执行一次)
void Publisher::CheckDeadSubscribers() {
    for (subscriber : subscribers_) {
        if (subscriber->IsActive()) continue;
        // IsActive(): 检查 last_connection_update_time_ms_ 是否在 connection_timeout_ms_ 内

        if (subscriber->ConnectionExists()) {
            // 第一次发现不活跃: flush 当前 connection，给一次恢复机会
            subscriber->PublishIfPossible(/*force_noop*/ true);
        } else {
            // 第二次发现不活跃且无 connection: 判定为死亡
            dead_subscribers.push_back(id);
        }
    }
    for (id : dead_subscribers) {
        UnregisterSubscriberInternal(id);
        // ← subscriber state 被销毁，mailbox 中所有未投递的消息永久丢失
    }
}
```

**时间线**:

```
T=0:      Subscriber 最后一次成功 poll
T=300s:   CheckDeadSubscribers 第一次发现不活跃 → flush connection
T=600s:   CheckDeadSubscribers 第二次发现不活跃 → 销毁 subscriber state
          所有 mailbox 中的消息永久丢失
```

**之后发生什么?**

```
T=600s+:  Subscriber 恢复，发送新的 PollRequest
          Publisher 检查 subscribers_ map → 找不到该 subscriber_id
          → 创建全新的空 SubscriberState (publisher.cc:376-384)
          → 新 subscriber 只收到此刻之后的新消息
          ← T=0 到 T=600s 期间的消息永久丢失
```

**Dashboard 代码没有重新获取全量快照的恢复逻辑**:

```python
# node_head.py:298-321 — _update_nodes
async def _update_nodes(self):
    async for node in self._subscribe_for_node_updates():
        await self._update_node(node)
    # _subscribe_for_node_updates 中 subscribe() 只调一次
    # 如果 publisher 清理了 subscriber state, poll() 会静默创建新 subscriber
    # 但不会重新 get_all_node_info → 丢失的状态永远不会被补回
```

**什么原因会导致 subscriber 连续 300s+ 无法 poll?**

- Dashboard asyncio event loop 被阻塞（处理大响应、GC 暂停）
- 网络长时间中断
- GCS gRPC server 队列满，新请求被拒绝

#### 场景 2: GCS Failover / 重启

```python
# gcs_pubsub.py:144-152
if poll.result().publisher_id != self._publisher_id:
    # GCS 重启了，publisher_id 变了
    self._publisher_id = poll.result().publisher_id
    self._max_processed_sequence_id = 0  # 重置 sequence
```

**GCS 重启时的行为**:

1. 旧 GCS 进程终止，所有 publisher state（包括 mailbox）丢失
2. 新 GCS 进程启动，从存储加载节点状态
3. 新 publisher 有全新的 `publisher_id`
4. Subscriber 检测到 `publisher_id` 变更，重置 `_max_processed_sequence_id = 0`
5. **但不重新拉取全量快照** → 旧 GCS 期间已发布但 subscriber 未消费的消息永久丢失

**GCS 重启时的节点状态处理** (`gcs_node_manager.cc:719-752`):

```cpp
// GCS Initialize() — 从存储恢复
// 1. 从 Redis/GCS storage 加载所有节点信息
// 2. 活着的节点重新加入 alive_nodes_
// 3. 发送 NotifyGCSRestart 给 raylets
// 4. !! 不重新发布历史 node death events !!
```

#### 场景 3: _subscribe_for_node_updates 异常被 catch

```python
# node_head.py:216-236
while True:
    try:
        node_id_updated_info_tuples = await subscriber.poll(batch_size=200)
        # ... 处理消息
    except Exception:
        logger.exception("Failed handling updated nodes.")
        # 消息已从 subscriber queue 消费，但 _update_node 未被调用
        # 这些消息永久丢失
```

如果在 protobuf 转换 (`_convert_to_dict`) 或其他处理中抛异常，消息已被消费但状态未更新。

### 为什么 pub/sub 丢失后不能自动更正？

根本原因: **Dashboard 的 node state 管理是纯增量模式，没有任何定期对账 (reconciliation) 机制**。

```python
# 整个 Dashboard 生命周期中对 node state 的处理:
# 1. 启动时: 全量快照 (一次性)
# 2. 运行中: 增量 pub/sub (永远)
# 3. 无定期 re-snapshot
# 4. 无 subscriber 恢复逻辑
# 5. 无 GCS 状态比对
```

**对比 Kubernetes controller 的 List-Watch 模式**:

| 机制 | Kubernetes controller | Ray Dashboard |
|------|----------------------|---------------|
| 初始化 | List (全量) | `get_all_node_info()` (全量) |
| 增量更新 | Watch (增量) | pub/sub poll (增量) |
| 定期 re-list | 有 (configurable re-sync period) | **无** |
| 资源版本检查 | 有 (resourceVersion) | **无** |
| Watch 断开恢复 | 自动 re-list + 从 resourceVersion 续传 | **无** (静默创建空 subscriber) |

**一旦增量流中丢失一条消息，对应节点的状态就永远不会被修正**，除非手动重启 Dashboard。

### 本次问题中 pub/sub 是否正常工作？

**是的，pub/sub 工作完全正常。** 证据:

1. Dashboard 确实收到了所有 DEAD 通知（1000 个 phantom 节点在 `DataSource.nodes` 中有 DEAD 数据，只是被写到了错误的 key）
2. 无 `Failed handling` 异常日志
3. 无 gRPC 连接错误日志
4. 无 GCS failover
5. GCS `CheckDeadSubscribers` 显示始终有 1 个活跃 subscriber

**根因是 `_update_node()` 中处理收到的 DEAD 消息时的写入逻辑 Bug，不是 pub/sub 传输层问题。**

---

## 修复方案

### 修复代码

**文件**: `python/ray/dashboard/modules/node/node_head.py`

**修改前** (line 283-288):

```python
self._dead_node_queue.append(node_id)
if len(self._dead_node_queue) > node_consts.MAX_DEAD_NODES_TO_CACHE:
    node_id = self._dead_node_queue.popleft()
    DataSource.nodes.pop(node_id, None)
    self._stubs.pop(node_id, None)
DataSource.nodes[node_id] = node
```

**修改后**:

```python
self._dead_node_queue.append(node_id)
if len(self._dead_node_queue) > node_consts.MAX_DEAD_NODES_TO_CACHE:
    evicted_node_id = self._dead_node_queue.popleft()
    DataSource.nodes.pop(evicted_node_id, None)
    self._stubs.pop(evicted_node_id, None)
DataSource.nodes[node_id] = node
```

**改动**: 将 `node_id = self._dead_node_queue.popleft()` 改为 `evicted_node_id = self._dead_node_queue.popleft()`，避免覆盖函数参数 `node_id`。

### 修复效果

修复后:

1. `DataSource.nodes[node_id] = node` 正确地将当前死亡节点的 DEAD 状态写到正确的 key 上
2. 被驱逐的旧 dead node 通过 `evicted_node_id` 正确删除
3. 不再产生 phantom ALIVE 节点
4. "Cannot reach the node" 错误将大幅减少（仅在节点刚死、GCS 通知到达前的短暂窗口期出现）

### 临时缓解 (不需要重新部署)

重启 Dashboard 可以清空 `_dead_node_queue`，重新从 GCS 拉取全量快照:

```bash
# Dashboard 会被 raylet 自动重启
ray stop --force dashboard
```

**注意**: 重启后如果 dead node 数量再次超过 1000，问题会重现。

### 建议的额外改进

1. **添加定期 reconciliation**: 在 `_update_nodes` 中添加定期（如每 10 分钟）重新调用 `async_get_all_node_info()` 与 `DataSource.nodes` 对比，修正漂移。类似 Kubernetes controller 的 re-list 机制
2. **添加 Dashboard 监控指标**: 暴露 `DataSource.nodes` 中 ALIVE/DEAD 节点数的 Prometheus metric，便于发现类似问题
3. **减少 "Cannot reach" 日志噪音**: 对持续不可达的节点进行指数退避或限流日志（当前 3h 内产生了 42 万条错误日志）

---

## 附: `ray list nodes` 使用注意事项

在大集群中使用 `ray list nodes` 时需注意默认 limit=100 的截断:

```bash
# 错误: 默认只返回 100 个节点
ray list nodes
# UserWarning: Limit last 100 entries (Total 2900)

# 正确: 指定足够大的 limit 和 state 过滤
ray list nodes --filter state=ALIVE --limit 10000

# 注意: --filter 和 -f (format) 参数语法不同
# --filter 使用 key=value 格式
# -f 用于输出格式 (table/json/yaml)
# 在 yaml 格式下 --filter 使用 key=value 会报错:
#   ValueError: The format of a given filter yaml is invalid:
#   Cannot find the predicate.
```

State API HTTP 接口返回结构:

```json
{
  "result": "SUCCESS",
  "msg": "...",
  "data": {
    "result": {
      "total": 2900,
      "num_after_truncation": 2900,
      "num_filtered": 2900,
      "result": [...],
      "partial_failure_warning": null,
      "warnings": null
    }
  }
}
```

其中 `data.result.total` 来自 GCS C++ 层 (`gcs_node_manager.cc:267,372`):

```cpp
const size_t total_num_nodes = alive_nodes_.size() + dead_nodes_.size();
// ...
reply->set_total(total_num_nodes);
```

该 `total` 字段包含 alive + dead 节点总数，**不是** alive 节点数。即使使用了 `state_filter=ALIVE`，`total` 仍然是 alive+dead 的总和。
