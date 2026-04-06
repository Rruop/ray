# ReporterAgent GPU/CPU 利用率指标深度分析

> 分析日期: 2026-07-07
> 分支: release-syp-analyze
> 核心文件: `python/ray/dashboard/modules/reporter/reporter_agent.py`, `python/ray/dashboard/modules/reporter/gpu_providers.py`

---

## 目录

- [一、核心结论](#一核心结论)
- [二、CPU 利用率 — 周期平均值](#二cpu-利用率--周期平均值)
- [三、GPU 节点级利用率 — NVML 采样窗口](#三gpu-节点级利用率--nvml-采样窗口)
- [四、GPU 进程级利用率 — 周期值与回退机制](#四gpu-进程级利用率--周期值与回退机制)
- [五、component_gpu_percentage 指标名](#五component_gpu_percentage-指标名)
- [六、Dashboard Node GPU Usage 数据流](#六dashboard-node-gpu-usage-数据流)
- [七、Grafana 指标面板 — ray_node_gpus_utilization](#七grafana-指标面板--ray_node_gpus_utilization)
- [八、GPU 相关 Prometheus 指标全景](#八gpu-相关-prometheus-指标全景)
- [九、关键代码索引](#九关键代码索引)

---

## 一、核心结论

| 指标类型 | 数据来源 | 采样语义 | 采样周期 |
|---------|---------|---------|---------|
| **CPU 利用率** | `psutil.cpu_percent()` | 自上次调用到本次调用之间的平均 CPU 使用率 | 5 秒（`REPORTER_UPDATE_INTERVAL_MS`） |
| **GPU 节点级利用率** | NVML `nvmlDeviceGetUtilizationRates` | 驱动内部最近一个采样窗口（约 1/6 秒）内 GPU 有 kernel 执行的时间占比 | ~1/6 秒（NVML 内部） |
| **GPU 进程级利用率**（新驱动 550+） | NVML `nvmlDeviceGetProcessesUtilizationInfo` | 自上次采样时间戳以来的进程 SM 利用率 | 5 秒（Reporter 采样间隔） |
| **GPU 进程级利用率**（老驱动回退） | `nvmlDeviceGetComputeRunningProcesses` | 不可用，`gpu_utilization=None` | — |

**CPU 和 GPU 利用率都不是瞬时值**，而是周期平均值。但两者的"周期"含义不同：CPU 是 Reporter 两次采集间隔（5 秒），GPU 节点级是 NVML 驱动内部窗口（~1/6 秒），GPU 进程级是两次 NVML 采样调用之间的间隔。

---

## 二、CPU 利用率 — 周期平均值

### 2.1 代码位置

```python
# reporter_agent.py:392
@staticmethod
def _get_cpu_percent(in_k8s: bool):
    if in_k8s:
        return k8s_utils.cpu_percent()
    else:
        return psutil.cpu_percent()
```

### 2.2 psutil.cpu_percent() 语义

`psutil.cpu_percent()` 的返回值是**自上次调用到本次调用之间的平均 CPU 使用率百分比**。

- 第一次调用返回 `0.0`（无基准时间）
- 后续调用返回 `(当前CPU时间 - 上次CPU时间) / (当前时间 - 上次时间) * 100`

### 2.3 采样周期

Reporter Agent 的 `_run_loop`（reporter_agent.py:1773-1782）每 `REPORTER_UPDATE_INTERVAL_MS` 毫秒执行一次采集：

```python
# reporter_consts.py:6
REPORTER_UPDATE_INTERVAL_MS = ray_constants.env_integer(
    "REPORTER_UPDATE_INTERVAL_MS", 5000
)
```

默认 5000ms = 5 秒。因此 `_get_cpu_percent()` 返回的是 **5 秒内的 CPU 平均使用率**。

### 2.4 K8s 环境的特殊处理

在 K8s 环境中，`psutil.cpu_percent()` 读取的是宿主机全局 CPU，可能不准确。因此使用 `k8s_utils.cpu_percent()` 替代，从 cgroup 读取容器级 CPU 使用率。

### 2.5 进程级 CPU

进程级 CPU 使用率通过 `psutil.Process.cpu_percent()` 获取，语义相同——返回自上次调用到本次调用之间的平均值。在 `_async_get_workers_and_agents()` 中，每个 worker 进程通过 `as_dict(attrs=PSUTIL_PROCESS_ATTRS)` 采集 `cpu_percent` 字段。

**注意**：代码中特别提到（reporter_agent.py:~720）：

> We should keep `raylet_proc.children()` in `self` because when `cpu_percent` is first called, it returns the meaningless 0. See more: https://github.com/ray-project/ray/issues/29848

首次调用 `cpu_percent()` 返回 0，因此 Reporter 会持续维护 `self._workers` 字典，确保每个进程的 `cpu_percent()` 有基准值。

---

## 三、GPU 节点级利用率 — NVML 采样窗口

### 3.1 数据采集链路

```
ReporterAgent._get_gpu_usage()
  → GpuMetricProvider.get_gpu_usage()
    → NvidiaGpuProvider.get_gpu_utilization()
      → NvidiaGpuProvider._get_pynvml_gpu_usage()
        → 遍历每块 GPU:
          → nvmlDeviceGetHandleByIndex(i)
          → _get_gpu_info(gpu_handle, i)
            → nvmlDeviceGetUtilizationRates(gpu_handle)  ← 节点级利用率
            → nvmlDeviceGetMemoryInfo(gpu_handle)         ← 显存信息
            → nvmlDeviceGetProcessesUtilizationInfo(...)  ← 进程级利用率（新API）
```

### 3.2 nvmlDeviceGetUtilizationRates 语义

```python
# gpu_providers.py:243
utilization_info = self._pynvml.nvmlDeviceGetUtilizationRates(gpu_handle)
utilization = int(utilization_info.gpu)
```

NVML `nvmlDeviceGetUtilizationRates` 返回的 `gpu` 字段是**驱动内部最近一个采样窗口（约 1/6 秒）内 GPU 有 kernel 在执行的时间占比**。

- 值范围：0-100（百分比）
- 采样窗口：NVML 驱动内部维护，约 1/6 秒（~167ms）
- 不是 Reporter 两次采集间隔的平均值，而是 NVML 驱动自身的短窗口

### 3.3 MIG 设备

对于 MIG 模式启用的 GPU，使用 `nvmlDeviceGetUtilizationRates(mig_handle)` 获取 MIG 设备利用率。但 MIG 模式下进程级 `gpu_utilization` 不可用（设为 `None`）。

### 3.4 AMD GPU

AMD GPU 使用 `pyamdsmi.smi_get_device_utilization(i)` 获取设备利用率，语义类似。

---

## 四、GPU 进程级利用率 — 周期值与回退机制

### 4.1 新驱动 API（驱动版本 550+）

```python
# gpu_providers.py:296-310
current_ts_ms = int(time.time() * 1000)
last_ts_ms = self._gpu_process_last_sample_ts.get(gpu_index, 0)
nv_processes = self._pynvml.nvmlDeviceGetProcessesUtilizationInfo(
    gpu_handle, last_ts_ms
)
self._gpu_process_last_sample_ts[gpu_index] = current_ts_ms

for nv_process in nv_processes:
    processes_pids[int(nv_process.pid)] = ProcessGPUInfo(
        pid=int(nv_process.pid),
        gpu_memory_usage=int(nv_process.memUtil) / 100 * int(memory_info.total) // MB,
        gpu_utilization=int(nv_process.smUtil),
    )
```

#### 关键机制

1. **时间戳传递**：`_gpu_process_last_sample_ts` 字典维护每块 GPU 上次采样的时间戳，传递给 `nvmlDeviceGetProcessesUtilizationInfo(gpu_handle, last_ts_ms)`
2. **返回值语义**：返回自 `last_ts_ms` 以来该 GPU 上各进程的 SM（Streaming Multiprocessor）利用率，是一个**周期平均值**
3. **首次调用**：`last_ts_ms=0`，NVML 返回驱动默认窗口的利用率
4. **周期**：实际周期 = Reporter 两次采集间隔 = 5 秒（`REPORTER_UPDATE_INTERVAL_MS`）

#### 进程显存计算

新 API 中进程显存不直接返回字节数，而是返回 `memUtil`（百分比），需要计算：

```python
gpu_memory_usage = int(nv_process.memUtil) / 100 * int(memory_info.total) // MB
```

### 4.2 老驱动回退 API

当新 API 不可用时（驱动版本 < 550 或 NVML 不支持），回退到：

```python
# gpu_providers.py:318-335
nv_comp_processes = self._pynvml.nvmlDeviceGetComputeRunningProcesses(gpu_handle)
nv_graphics_processes = self._pynvml.nvmlDeviceGetGraphicsRunningProcesses(gpu_handle)

for nv_process in nv_comp_processes + nv_graphics_processes:
    processes_pids[int(nv_process.pid)] = ProcessGPUInfo(
        pid=int(nv_process.pid),
        gpu_memory_usage=(
            int(nv_process.usedGpuMemory) // MB
            if nv_process.usedGpuMemory else 0
        ),
        gpu_utilization=None,  # Not available with older API
    )
```

#### 老 API 限制

- **只能获取进程显存**（`usedGpuMemory`），无法获取进程级 GPU 利用率
- `gpu_utilization` 设为 `None`
- 进程显存单位为字节，直接除以 MB 转换

### 4.3 进程级 GPU 利用率在 Dashboard 中的展示

进程级 GPU 利用率通过 `_async_get_workers_and_agents()` 中的 `gpu_pid_mapping` 聚合到每个 worker：

```python
# reporter_agent.py:~720
if worker_pid in gpu_pid_mapping:
    for gpu_proc in gpu_pid_mapping[worker_pid]:
        gpu_memory_usage += gpu_proc["gpu_memory_usage"]
        utilization = gpu_proc["gpu_utilization"] or 0
        gpu_utilization += utilization

worker_info["gpu_memory_usage"] = gpu_memory_usage    # in MB
worker_info["gpu_utilization"] = gpu_utilization      # percentage
```

如果一个进程使用多块 GPU，`gpu_utilization` 会**累加**各 GPU 上的利用率。

---

## 五、component_gpu_percentage 指标名

### 5.1 Gauge 定义

```python
# reporter_agent.py:216-221
"component_gpu_percentage": Gauge(
    "component_gpu_percentage",
    "GPU usage of all components on the node.",
    "percentage",
    COMPONENT_GPU_TAG_KEYS,
),
```

Gauge 的第一个参数就是 Prometheus 导出的指标名。

### 5.2 Prometheus 导出名

Prometheus 导出时会添加 `ray_` 前缀（reporter_agent.py:140，`namespace="ray"`）：

- **最终 Prometheus 指标名：`ray_component_gpu_percentage`**

### 5.3 OpenTelemetry 路径

如果启用 `RAY_ENABLE_OPEN_TELEMETRY`，走 OpenTelemetry remote_write 路径（reporter_agent.py:~1750）：

```python
if RAY_ENABLE_OPEN_TELEMETRY:
    self._open_telemetry_metric_recorder.record_and_export(
        records,
        global_tags=global_tags,
    )
```

OpenTelemetry 路径中，`register_gauge_metric` 使用的也是 `component_gpu_percentage` 这个名字，但 **无 `ray_` 前缀**（OpenTelemetry metric recorder 内部有自己的 namespace 处理）。

### 5.4 聚合逻辑

`_generate_system_stats_record()` 中（reporter_agent.py:~1050），对每个组件的所有进程累加 GPU 利用率：

```python
for stat in stats:
    total_gpu_percentage += float(stat.get("gpu_utilization", 0.0))
    total_gpu_memory += float(stat.get("gpu_memory_usage", 0.0))
```

然后导出：

```python
if total_gpu_percentage > 0.0:
    records.append(
        Record(
            gauge=METRICS_GAUGES["component_gpu_percentage"],
            value=total_gpu_percentage,
            tags=tags,
        )
    )
```

**注意**：`total_gpu_percentage` 是组件内所有进程在所有 GPU 上利用率的总和，可能超过 100。例如一个组件有 2 个进程各使用 1 块 GPU，每块 GPU 利用率 80%，则 `total_gpu_percentage = 160`。

---

## 六、Dashboard Node GPU Usage 数据流

Dashboard 前端的 Node GPU Usage **不使用 Prometheus 指标**，而是来自 Reporter Agent 通过 GCS pub/sub 发布的实时物理状态数据。

### 6.1 完整数据流

```
1. NvidiaGpuProvider._get_gpu_info()
   → nvmlDeviceGetUtilizationRates(gpu_handle)
   → 返回 utilization_gpu (snake_case)

2. ReporterAgent._async_collect_stats()
   → stats["gpus"] = self._get_gpu_usage()
   → 返回 List[GpuUtilizationInfo]

3. ReporterAgent._generate_stats_payload()
   → to_google_style(recursive_asdict(stats))
   → utilization_gpu → utilizationGpu (camelCase)
   → StatsPayload.parse_obj(stats_dict)
   → json.dumps(parsed_stats.dict())

4. ReporterAgent._run_loop()
   → self._gcs_client.async_publish_node_resource_usage(self._key, json_payload)
   → 发布到 GCS pub/sub channel: RAY_NODE_RESOURCE_USAGE_CHANNEL

5. NodeHead._update_node_physical_stats()  (head 节点)
   → GcsAioResourceUsageSubscriber.poll()
   → 解析 JSON，存入 DataSource.node_physical_stats[node_id]

6. 前端 GET /nodes?view=summary
   → DataOrganizer.get_all_node_summary()
   → 从 DataSource.node_physical_stats 读取
   → 返回 node.gpus[].utilizationGpu

7. GPUColumn.tsx (前端组件)
   → node.gpus.map((gpu, i) => ...)
   → <UsageBar percent={gpu.utilizationGpu} text={`${gpu.utilizationGpu.toFixed(1)}%`} />
```

### 6.2 关键代码位置

| 步骤 | 文件 | 行号 | 说明 |
|------|------|------|------|
| 1 | `gpu_providers.py` | 243 | `nvmlDeviceGetUtilizationRates` 获取节点级 GPU 利用率 |
| 2 | `reporter_agent.py` | ~830 | `stats["gpus"]` 打包进 stats dict |
| 3 | `reporter_agent.py` | ~940 | `to_google_style()` 转 camelCase + StatsPayload 序列化 |
| 4 | `reporter_agent.py` | 1779 | `async_publish_node_resource_usage` 发布到 GCS |
| 5 | `node_head.py` | 524-551 | `_update_node_physical_stats` 订阅 GCS 更新 |
| 6 | `node_head.py` | 341-361 | `GET /nodes?view=summary` API 端点 |
| 7 | `GPUColumn.tsx` | 21 | `<UsageBar percent={gpu.utilizationGpu} />` |

### 6.3 前端类型定义

```typescript
// node.d.ts:65-79
export type GPUStats = {
  uuid: string;
  index: number;
  name: string;
  utilizationGpu?: number;       // ← Dashboard Node GPU Usage 使用的字段
  memoryUsed: number;            // ← GRAM 列使用
  memoryTotal: number;
  processesPids?: ProcessGPUUsage[];
};
```

### 6.4 前端渲染

```tsx
// GPUColumn.tsx:6-29
const NodeGPUEntry = ({ gpu, slot }) => {
  return (
    <div>
      {gpu.utilizationGpu !== undefined ? (
        <UsageBar
          percent={gpu.utilizationGpu}
          text={`${gpu.utilizationGpu.toFixed(1)}%`}
        />
      ) : (
        <span>Not available</span>
      )}
    </div>
  );
};

// GPUColumn.tsx:31-45
const NodeGPUView = ({ node }) => {
  return (
    <div>
      {node.gpus.map((gpu, i) => (
        <NodeGPUEntry key={gpu.uuid} gpu={gpu} slot={gpu.index} />
      ))}
    </div>
  );
};
```

### 6.5 Dashboard GPU Usage 与 Prometheus 指标的区别

| 维度 | Dashboard Node GPU Usage | Prometheus `ray_node_gpus_utilization` |
|------|--------------------------|---------------------------------------|
| 数据源 | NVML `nvmlDeviceGetUtilizationRates` | 同左 |
| 传输路径 | GCS pub/sub → HTTP API | Prometheus pull / OTLP push |
| 延迟 | ~5 秒（Reporter 采集间隔） | 取决于 Prometheus scrape 间隔 |
| 粒度 | 每块 GPU 单独显示 | 每 GPU 一个时间序列（`GpuIndex` tag） |
| 用途 | 实时查看 | 历史趋势、告警 |

**两者数据源相同**，但走不同的传输和展示路径。Dashboard 直接从 GCS pub/sub 获取实时数据，Prometheus 从 Reporter 导出的 metrics endpoint 拉取（或通过 OpenTelemetry push）。

---

## 七、Grafana 指标面板 — ray_node_gpus_utilization

Grafana 面板中 Node GPU 使用率对应的是 Prometheus 指标 **`ray_node_gpus_utilization`**。

### 7.1 指标定义

```python
# reporter_agent.py:100-105
"node_gpus_utilization": Gauge(
    "node_gpus_utilization",
    "Total GPUs usage on a ray node",
    "percentage",
    GPU_TAG_KEYS,
),
```

Prometheus 导出名：`ray_node_gpus_utilization`

### 7.2 导出逻辑

在 `_to_records()` 中（reporter_agent.py:~970），对每块 GPU 单独导出：

```python
for gpu in gpus:
    gpus_utilization = 0
    if gpu["utilization_gpu"] is not None:
        gpus_utilization += gpu["utilization_gpu"]

    if gpu_index is not None:
        gpu_tags = {**node_tags, "GpuIndex": str(gpu_index)}
        if gpu_name:
            gpu_tags["GpuDeviceName"] = gpu_name

        gpus_utilization_record = Record(
            gauge=METRICS_GAUGES["node_gpus_utilization"],
            value=gpus_utilization,
            tags=gpu_tags,
        )
        records_reported.append(gpus_utilization_record)
```

### 7.3 标签

每条 `ray_node_gpus_utilization` 时间序列的标签：

| Label | 含义 | 示例 |
|-------|------|------|
| `ip` | 节点 IP | `10.15.6.116` |
| `NodeId` | 节点唯一 ID | `ed7040e992fa61...` |
| `RayNodeType` | 节点类型 | `head` / `worker` |
| `IsHeadNode` | 是否为 head | `true` / `false` |
| `GpuIndex` | GPU 索引 | `0`, `1`, `2`... |
| `GpuDeviceName` | GPU 设备名 | `NVIDIA A10G`, `NVIDIA H100`... |
| `Version` | Ray 版本 | `2.54.4+kuaishou.7730c6befe` |
| `SessionName` | Ray session 名 | `session_2026-07-07_...` |
| `ray_io_cluster` | 集群名（可选） | 来自 `RAY_CLUSTER_NAME` |

### 7.4 Grafana 常用查询

```promql
# 单节点所有 GPU 平均利用率
avg by (GpuIndex) (ray_node_gpus_utilization{NodeId="xxx"})

# 集群所有 GPU 利用率分布
avg by (GpuDeviceName) (ray_node_gpus_utilization)

# GPU 利用率 Top N
topk(10, max by (NodeId, GpuIndex) (ray_node_gpus_utilization))
```

---

## 八、GPU 相关 Prometheus 指标全景

| Prometheus 指标名 | 含义 | 单位 | 粒度 | 来源 |
|-------------------|------|------|------|------|
| `ray_node_gpus_utilization` | 每块 GPU 的利用率 | percentage | 每块 GPU | NVML `nvmlDeviceGetUtilizationRates` |
| `ray_node_gpus_available` | GPU 是否可用 | count (1) | 每块 GPU | 有 GPU 设备则为 1 |
| `ray_node_gram_used` | GPU 显存已用 | bytes | 每块 GPU | NVML `nvmlDeviceGetMemoryInfo.used` |
| `ray_node_gram_available` | GPU 显存可用 | bytes | 每块 GPU | `memory_total - memory_used` |
| `ray_component_gpu_percentage` | 组件级 GPU 利用率（聚合） | percentage | 每组件 | NVML 进程级 `smUtil`（新驱动）/ `None`（老驱动） |
| `ray_component_gpu_memory_mb` | 组件级 GPU 显存占用（聚合） | MB | 每组件 | NVML 进程级 `memUtil`（新驱动）/ `usedGpuMemory`（老驱动） |

### 指标间关系

```
NVML 驱动层
├── nvmlDeviceGetUtilizationRates  → utilization_gpu  → ray_node_gpus_utilization
├── nvmlDeviceGetMemoryInfo        → memory_used/total → ray_node_gram_used / ray_node_gram_available
└── nvmlDeviceGetProcessesUtilizationInfo (驱动 550+)
    ├── smUtil   → gpu_utilization  → ray_component_gpu_percentage
    └── memUtil  → gpu_memory_usage → ray_component_gpu_memory_mb

回退 API (驱动 < 550)
├── nvmlDeviceGetComputeRunningProcesses
│   └── usedGpuMemory → gpu_memory_usage → ray_component_gpu_memory_mb
└── gpu_utilization = None (不可用)
```

### 采样语义对比

| 指标 | 采样语义 | 周期长度 |
|------|---------|---------|
| `ray_node_gpus_utilization` | NVML 驱动内部采样窗口 | ~1/6 秒（NVML 驱动内部） |
| `ray_component_gpu_percentage` | 两次 NVML 采样调用之间的平均值 | 5 秒（Reporter 采集间隔） |
| `ray_node_cpu_utilization` | psutil 两次调用之间的平均值 | 5 秒（Reporter 采集间隔） |
| `ray_component_cpu_percentage` | psutil 进程级两次调用之间的平均值 | 5 秒（Reporter 采集间隔） |

---

## 九、关键代码索引

### Reporter Agent

| 文件 | 行号 | 说明 |
|------|------|------|
| `reporter_agent.py` | 100-105 | `node_gpus_utilization` Gauge 定义 |
| `reporter_agent.py` | 108-113 | `node_gram_used` Gauge 定义 |
| `reporter_agent.py` | 216-221 | `component_gpu_percentage` Gauge 定义 |
| `reporter_agent.py` | 222-227 | `component_gpu_memory_mb` Gauge 定义 |
| `reporter_agent.py` | 392 | `_get_cpu_percent()` — psutil.cpu_percent() |
| `reporter_agent.py` | ~830 | `_async_collect_stats()` — 采集 stats dict |
| `reporter_agent.py` | ~720 | `_async_get_workers_and_agents()` — 进程级 GPU 聚合 |
| `reporter_agent.py` | ~940 | `_generate_stats_payload()` — camelCase 转换 + GCS 发布 |
| `reporter_agent.py` | ~970 | `_to_records()` — 按 GPU 导出 Prometheus records |
| `reporter_agent.py` | ~1050 | `_generate_system_stats_record()` — 组件级 GPU 聚合 |
| `reporter_agent.py` | 1773-1782 | `_run_loop()` — 5 秒采集循环 |
| `reporter_consts.py` | 6 | `REPORTER_UPDATE_INTERVAL_MS = 5000` |

### GPU Providers

| 文件 | 行号 | 说明 |
|------|------|------|
| `gpu_providers.py` | 33-42 | `GpuUtilizationInfo` TypedDict 定义 |
| `gpu_providers.py` | 27-31 | `ProcessGPUInfo` TypedDict 定义 |
| `gpu_providers.py` | 243 | `nvmlDeviceGetUtilizationRates` — 节点级利用率 |
| `gpu_providers.py` | 296-310 | `nvmlDeviceGetProcessesUtilizationInfo` — 进程级利用率（新 API） |
| `gpu_providers.py` | 318-335 | 回退到 `nvmlDeviceGetComputeRunningProcesses`（老 API） |
| `gpu_providers.py` | 457-564 | `GpuMetricProvider` — GPU 指标提供者入口 |

### 前端

| 文件 | 行号 | 说明 |
|------|------|------|
| `GPUColumn.tsx` | 21 | `<UsageBar percent={gpu.utilizationGpu} />` |
| `GPUColumn.tsx` | 31-45 | `NodeGPUView` — 遍历 `node.gpus` 渲染 |
| `GRAMColumn.tsx` | 11-22 | `gpu.memoryUsed` / `gpu.memoryTotal` 渲染 |
| `node.d.ts` | 65-79 | `GPUStats` 类型定义 |
| `service/node.ts` | 5 | `getNodeList()` → `GET /nodes?view=summary` |

### 后端 API

| 文件 | 行号 | 说明 |
|------|------|------|
| `node_head.py` | 341-361 | `GET /nodes?view=summary` 端点 |
| `node_head.py` | 524-551 | `_update_node_physical_stats` — GCS pub/sub 订阅 |
| `datacenter.py` | 148-186 | `get_node_info()` — 合并 physical_stats |
| `reporter_models.py` | 27-37 | `GpuUtilizationInfo` Pydantic 模型 |
| `reporter_models.py` | 14-19 | `ProcessGPUInfo` Pydantic 模型 |
