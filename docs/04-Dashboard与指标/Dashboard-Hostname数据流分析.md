# Ray Dashboard Hostname 字段来源分析

本文档详细分析 Ray Dashboard `/nodes` API 返回数据中 `hostname` 字段的获取流程。

## 问题背景

在 Ray Dashboard 的 `/nodes?view=summary` API 返回的数据中，每个节点包含一个 `hostname` 字段：

```json
{
  "data": {
    "summary": [
      {
        "now": 1776937538.618742,
        "hostname": "aiplatform-bjmt-ge58-115.idchb1az4.hb1.kwaidc.com",
        "ip": "10.15.7.171",
        "cpu": 0.5,
        ...
      }
    ]
  }
}
```

本文追踪该字段的完整数据流。

## 数据流架构

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Ray Cluster                                  │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌──────────────────┐    ┌──────────────────┐    ┌──────────────┐  │
│  │   Worker Node 1  │    │   Worker Node 2  │    │  Head Node   │  │
│  │                  │    │                  │    │              │  │
│  │ ┌──────────────┐ │    │ ┌──────────────┐ │    │ ┌──────────┐ │  │
│  │ │ReporterAgent │ │    │ │ReporterAgent │ │    │ │Reporter  │ │  │
│  │ │              │ │    │ │              │ │    │ │Agent     │ │  │
│  │ │ hostname =   │ │    │ │ hostname =   │ │    │ │          │ │  │
│  │ │ socket.get   │ │    │ │ socket.get   │ │    │ │          │ │  │
│  │ │ hostname()   │ │    │ │ hostname()   │ │    │ │          │ │  │
│  │ └──────┬───────┘ │    │ └──────┬───────┘ │    │ └────┬─────┘ │  │
│  └────────┼─────────┘    └────────┼─────────┘    └──────┼───────┘  │
│           │                       │                      │          │
│           └───────────────┬───────┴──────────────────────┘          │
│                           ▼                                          │
│                 ┌─────────────────────┐                              │
│                 │   GCS PubSub        │                              │
│                 │ (Resource Usage)    │                              │
│                 └──────────┬──────────┘                              │
│                            │                                         │
│                            ▼                                         │
│                 ┌─────────────────────┐                              │
│                 │     NodeHead        │                              │
│                 │  (Dashboard Head)   │                              │
│                 │                     │                              │
│                 │ _update_node_       │                              │
│                 │ physical_stats()    │                              │
│                 └──────────┬──────────┘                              │
│                            │                                         │
│                            ▼                                         │
│                 ┌─────────────────────┐                              │
│                 │    DataSource.      │                              │
│                 │ node_physical_stats │                              │
│                 └──────────┬──────────┘                              │
│                            │                                         │
│                            ▼                                         │
│                 ┌─────────────────────┐                              │
│                 │   DataOrganizer.    │                              │
│                 │ get_all_node_       │                              │
│                 │ summary()           │                              │
│                 └──────────┬──────────┘                              │
│                            │                                         │
│                            ▼                                         │
│                 ┌─────────────────────┐                              │
│                 │  /nodes?view=summary│                              │
│                 │     API Response    │                              │
│                 └─────────────────────┘                              │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

## 详细代码分析

### 1. 数据源 - ReporterAgent

**文件**: `python/ray/dashboard/modules/reporter/reporter_agent.py`

#### 1.1 初始化时获取 hostname

```python
# reporter_agent.py:428
class ReporterAgent(DashboardAgentModule):
    def __init__(self, dashboard_agent):
        ...
        self._hostname = socket.gethostname()  # <-- 核心：使用 Python socket 库获取主机名
        ...
```

**关键点**：
- 使用 Python 标准库 `socket.gethostname()` 获取主机名
- 在 Linux 系统上，底层调用 `gethostname(2)` 系统调用
- 返回值通常等同于 `/etc/hostname` 文件内容或 `hostname` 命令的输出

#### 1.2 构建统计数据时包含 hostname

```python
# reporter_agent.py:1115-1141
async def _get_all_stats(self):
    now = time.time()
    ...
    stats = {
        "now": now,
        "hostname": self._hostname,  # <-- hostname 被包含在统计数据中
        "ip": self._ip,
        "cpu": self._get_cpu_percent(IN_KUBERNETES_POD),
        "cpus": self._cpu_counts,
        "mem": self._get_mem_usage(),
        "shm": self._get_shm_usage(),
        "workers": await self._async_get_workers_and_agents(gpus),
        "raylet": raylet,
        "agent": self._get_agent(),
        "bootTime": self._get_boot_time(),
        "loadAvg": self._get_load_avg(),
        "disk": self._get_disk_usage(),
        "disk_io": disk_stats,
        "disk_io_speed": disk_speed_stats,
        "gpus": gpus,
        "tpus": self._get_tpu_usage(),
        "network": network_stats,
        "network_speed": network_speed_stats,
        "cmdline": raylet.get("cmdline", []) if raylet else [],
    }
    if self._is_head_node:
        stats["gcs"] = self._get_gcs()
    return stats
```

### 2. 数据传输 - GCS PubSub

ReporterAgent 将统计数据通过 GCS 的 Resource Usage PubSub 机制上报。

### 3. 数据接收 - NodeHead

**文件**: `python/ray/dashboard/modules/node/node_head.py`

#### 3.1 订阅并更新物理统计数据

```python
# node_head.py:523-549
async def _update_node_physical_stats(self):
    """
    Update DataSource.node_physical_stats by subscribing to the GCS resource usage.
    """
    subscriber = GcsAioResourceUsageSubscriber(address=self.gcs_address)
    await subscriber.subscribe()

    while True:
        try:
            # The key is b'RAY_REPORTER:{node id hex}',
            # e.g. b'RAY_REPORTER:2b4fbd...'
            key, data = await subscriber.poll()
            if key is None:
                continue

            # 解析数据
            parsed_data = await self._loop.run_in_executor(
                self._node_executor, _parse_node_stats, data
            )

            node_id = key.split(":")[-1]
            DataSource.node_physical_stats[node_id] = parsed_data  # <-- 存储到 DataSource
        except Exception:
            logger.exception(...)
```

### 4. 数据存储 - DataSource

**文件**: `python/ray/dashboard/modules/node/datacenter.py`

```python
# datacenter.py:19-36
class DataSource:
    # {node id hex(str): node stats(dict of GetNodeStatsReply in node_manager.proto)}
    node_stats = {}
    # {node id hex(str): node physical stats(dict from reporter_agent.py)}
    node_physical_stats = {}  # <-- hostname 存储在这里
    # {actor id hex(str): actor table data(dict of ActorTableData in gcs.proto)}
    actors = {}
    # {node id hex(str): gcs node info(dict of GcsNodeInfo in gcs.proto)}
    nodes = {}
    ...
```

### 5. 数据组装 - DataOrganizer

**文件**: `python/ray/dashboard/modules/node/datacenter.py`

```python
# datacenter.py:130-174
@classmethod
async def get_node_info(cls, node_id, get_summary=False):
    node_physical_stats = dict(DataSource.node_physical_stats.get(node_id, {}))
    node_stats = dict(DataSource.node_stats.get(node_id, {}))
    node = DataSource.nodes.get(node_id, {})

    ...

    node_info = node_physical_stats  # <-- hostname 在 node_physical_stats 中
    # Merge node stats to node physical stats under raylet
    node_info["raylet"] = node_stats
    ...
    return node_info

# datacenter.py:176-183
@classmethod
async def get_all_node_summary(cls):
    return [
        await DataOrganizer.get_node_info(node_id, get_summary=True)
        for node_id in DataSource.nodes.keys()
    ]
```

### 6. API 响应 - NodeHead

**文件**: `python/ray/dashboard/modules/node/node_head.py`

```python
# node_head.py:385-402
@routes.get("/nodes")
@dashboard_optional_utils.aiohttp_cache
async def get_all_nodes(self, req) -> aiohttp.web.Response:
    view = req.query.get("view")
    if view == "summary":
        all_node_summary_task = DataOrganizer.get_all_node_summary()
        nodes_logical_resource_task = self.get_nodes_logical_resources()

        all_node_summary, nodes_logical_resources = await asyncio.gather(
            all_node_summary_task, nodes_logical_resource_task
        )

        return dashboard_optional_utils.rest_response(
            status_code=dashboard_utils.HTTPStatusCode.OK,
            message="Node summary fetched.",
            summary=all_node_summary,  # <-- 返回包含 hostname 的数据
            node_logical_resources=nodes_logical_resources,
        )
    ...
```

## 总结

| 步骤 | 组件 | 文件位置 | 说明 |
|------|------|----------|------|
| 1 | ReporterAgent 初始化 | `reporter_agent.py:428` | `socket.gethostname()` 获取主机名 |
| 2 | 统计数据构建 | `reporter_agent.py:1117` | 将 hostname 包含在 stats 字典中 |
| 3 | GCS PubSub | - | 通过 Resource Usage 订阅机制传输 |
| 4 | NodeHead 接收 | `node_head.py:523-549` | 订阅并解析数据 |
| 5 | DataSource 存储 | `datacenter.py:24` | 存储在 `node_physical_stats` 中 |
| 6 | DataOrganizer 组装 | `datacenter.py:130-183` | 组装节点信息 |
| 7 | API 响应 | `node_head.py:385-402` | 返回给客户端 |

## 相关文件

- `python/ray/dashboard/modules/reporter/reporter_agent.py` - 数据采集
- `python/ray/dashboard/modules/node/node_head.py` - API 入口和数据订阅
- `python/ray/dashboard/modules/node/datacenter.py` - 数据存储和组装

## 底层实现

`socket.gethostname()` 在不同操作系统上的行为：

- **Linux**: 调用 `gethostname(2)` 系统调用，返回 `/etc/hostname` 或通过 `sethostname(2)` 设置的值
- **macOS**: 同样调用 `gethostname(2)`
- **Windows**: 调用 `gethostname()` Winsock 函数

在 Kubernetes 环境中，hostname 通常是 Pod 的名称，除非在 Pod spec 中显式设置了 `hostname` 字段。
