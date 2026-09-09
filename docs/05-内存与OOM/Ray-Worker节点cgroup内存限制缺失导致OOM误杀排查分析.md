# Ray Worker 节点 cgroup 内存限制缺失导致 OOM 误杀排查分析

## 问题现象

Ray 集群中部分 Worker 节点的 worker 被 OOM Killer 杀掉：

```
ray.exceptions.OutOfMemoryError: 1 worker(s) were killed due to the node running low on memory.
Memory on the node (IP: 10.83.10.20) was 957.55GB / 1006.84GB (0.951040),
which exceeds the memory usage threshold of 0.950000
```

同时 `/cluster` 页面显示的 memory 和 object_store_memory 与启动参数指定的值不一致：

- 启动指定 `--memory=93834534912`（≈87.5GB），但页面显示远大于此
- `object_store_memory` 显示 200GB，而非按比例计算的较小值

## 根因分析

### 核心结论

**Pod 未设置 `resources.limits.memory`，导致 cgroup memory limit 无效（9.2EB），Ray 误将宿主机全局内存作为可用内存计算资源，同时 OOM 监控也看到宿主机全局内存使用率，受同机其他 Pod 连累触发误杀。**

### 两种节点的对比数据

| 指标 | 问题节点 (10.48.34.140) | 正常节点 (10.106.229.76) |
|------|------------------------|-------------------------|
| **cgroup memory.limit_in_bytes** | **9223372036854771712 (无限制)** | **526133493760 (491GB)** |
| 宿主机物理内存 | 1081 GB | 1081 GB |
| KubeRay `--memory` | 87.5 GB | 491 GB |
| **object_store_memory** | **200 GB (触顶截断)** | **157.7 GB (30%自然值)** |
| /dev/shm | 512 GB | 490 GB |
| cgroup cfs_quota_us | -1 (无限制) | 12400000 (124核) |
| **cgroup 路径** | **kubemidpods.slice** | **kubepods.slice** |

### WezTerm 实际验证

通过 WezTerm 登录 `aiplatform-wlf3-ge92-71` 节点实际验证：

```bash
# cgroup 内存限制
$ cat /sys/fs/cgroup/memory/memory.limit_in_bytes
9223372036854771712          # 9.2EB — 无限制，与问题节点一致

# cgroup 实际内存使用
$ cat /sys/fs/cgroup/memory/memory.usage_in_bytes
86248562688                  # ~80GB — Pod 内真实内存使用

# 宿主机内存（/proc/meminfo 在容器中显示宿主机信息）
$ cat /proc/meminfo | head -5
MemTotal:       1056140440 kB    # ~1003GB — 宿主机全局物理内存
MemFree:        614419316 kB
MemAvailable:   837998744 kB     # ~799GB — 宿主机可用内存
Buffers:          863588 kB
Cached:         349061328 kB
```

**验证结论**：该节点 cgroup `memory.limit_in_bytes` 为 9.2EB（无限制），与文档分析的问题节点完全一致。Ray 的 `ThresholdMemoryMonitor` 会走 `/proc/meminfo` fallback 路径，用宿主机全局内存而非 cgroup 内的值。cgroup `usage_in_bytes` (~80GB) 才是 Pod 内真实内存使用，但 Ray 看不到它。

---

## Ray 内存数据源全景分析

Ray 中涉及内存的场景有两类截然不同的数据来源，容易混淆：

| 场景 | 数据来源 | 是否受 cgroup 影响 | 说明 |
|------|---------|-------------------|------|
| **每进程内存**（Dashboard 显示 231.40MB） | `psutil.Process.memory_info().rss` | **不受** | 读 `/proc/PID/statm`，是进程级 RSS |
| **节点级内存 total/used**（Dashboard 显示节点总内存） | `get_system_memory()` / `get_used_memory()` | **受** | 读 cgroup 文件，fallback 到 `/proc/meminfo` |
| **C++ OOM 监控**（raylet ThresholdMemoryMonitor） | `TakeSystemMemorySnapshot()` | **受** | 读 cgroup 文件，fallback 到 `/proc/meminfo` |
| **C++ 进程级内存**（raylet TopN 排序日志） | `TakePerProcessMemorySnapshot()` | **不受** | 读 `/proc/PID/smaps_rollup` 的 USS |

---

## 数据流详解 1：Dashboard 每进程内存（如 `ray::DashboardAgent, 231.40MB`）

### 完整数据流

```
psutil.Process(pid).as_dict(attrs=["memory_info", ...])
       │
       ▼
ReporterAgent._async_collect_stats()  (每5s)
  → stats = {workers: [...], raylet: {...}, agent: {...}, gcs: {...}, mem: (total, avail, pct, used)}
       │
       ▼
ReporterAgent._generate_stats_payload() → JSON string
       │
       ▼
GCS pub/sub: async_publish_node_resource_usage("RAY_REPORTER:{node_id}", json_payload)
       │
       ▼  [GCS pub/sub 传输]
       │
NodeHead._update_node_physical_stats()
  → GcsAioResourceUsageSubscriber.poll()
  → DataSource.node_physical_stats[node_id] = parsed_data
       │
       ▼
DataOrganizer._extract_workers_for_node()
  → 合并 node_physical_stats.workers + node_stats.coreWorkerStats
  → DataSource.node_workers[node_id]
       │
       ▼
HTTP GET /nodes/{node_id}
  → DataOrganizer.get_node_info()
  → 返回 node_info，workers[].memoryInfo.rss (bytes)
       │
       ▼
React: NodeRow.tsx WorkerRow
  → cmdline[0] = 进程名 (e.g. "ray::DashboardAgent")
  → memoryConverter(memoryInfo.rss) = "231.40MB"
```

### 采集端：ReporterAgent（每节点每 5 秒采集一次）

**文件**: `python/ray/dashboard/modules/reporter/reporter_agent.py`

ReporterAgent 的 `_run_loop()` 方法（约 line 1830）以 `REPORTER_UPDATE_INTERVAL_MS`（默认 5 秒）为周期调用 `_async_collect_stats()`。

#### 采集属性定义

```python
# reporter_agent.py:393
PSUTIL_PROCESS_ATTRS = (
    [
        "pid",
        "create_time",
        "cpu_percent",
        "cpu_times",
        "cmdline",
        "memory_info",        # ← 关键：采集进程的 RSS
    ]
    + (["num_fds"] if sys.platform != "win32" else [])
    + (["memory_full_info"] if sys.platform == "darwin" else [])
)
```

#### 各进程的采集方式

```python
# reporter_agent.py:1060 — raylet 进程
def _get_raylet(self):
    raylet_proc = self._get_raylet_proc()
    if raylet_proc is None:
        return None
    else:
        return raylet_proc.as_dict(attrs=PSUTIL_PROCESS_ATTRS)

# reporter_agent.py:1069 — agent 进程（自身）
def _get_agent(self):
    if not self._agent_proc:
        self._agent_proc = psutil.Process()
    return self._agent_proc.as_dict(attrs=PSUTIL_PROCESS_ATTRS)

# reporter_agent.py:1052 — GCS 进程（仅 head 节点）
def _get_gcs(self):
    if self._gcs_pid:
        if not self._gcs_proc or self._gcs_pid != self._gcs_proc.pid:
            self._gcs_proc = psutil.Process(self._gcs_pid)
        if self._gcs_proc:
            dictionary = self._gcs_proc.as_dict(attrs=PSUTIL_PROCESS_ATTRS)
            return dictionary
    return {}

# reporter_agent.py:960 — worker 和其他 agent 进程
async def _async_get_worker_processes(self):
    pids = await self._async_get_worker_pids_from_raylet()
    for pid in pids:
        try:
            proc = psutil.Process(pid)
            workers[self._generate_proc_key(proc)] = proc
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return workers

# reporter_agent.py:1003 — 调用 as_dict 采集
for w in self._workers.values():
    try:
        if w.status() == psutil.STATUS_ZOMBIE:
            continue
        worker_info = w.as_dict(attrs=PSUTIL_PROCESS_ATTRS)  # ← 关键调用
        result.append(worker_info)
    except psutil.NoSuchProcess:
        continue
```

#### `psutil.Process.memory_info()` 的底层实现

`psutil.Process.memory_info()` 返回 `pmem` 命名元组，关键字段是 `rss`（Resident Set Size）：

```python
# 返回值示例
pmem(rss=231400000, vms=..., shared=..., text=..., data=...)
```

在 Linux 上，psutil 内部通过读取 `/proc/PID/statm` 来计算 RSS：

```
/proc/PID/statm 格式:
size resident shared text lib data dt
```

其中 `resident` 字段（单位：页）× 页大小 = RSS 字节数。这与 `/proc/PID/status` 中的 `VmRSS` 值一致，也等于 `top` 命令中的 `RES` 列。

**关键**：`psutil.Process.memory_info().rss` 是进程级物理内存使用，**不受 cgroup 限制影响**，即使 cgroup limit 为 9.2EB，进程的 RSS 值也是准确的。

#### `_async_collect_stats()` 组装完整 stats

```python
# reporter_agent.py:1130
async def _async_collect_stats(self):
    stats = {
        "now": now,
        "hostname": self._hostname,
        "ip": self._ip,
        "cpu": self._get_cpu_percent(IN_KUBERNETES_POD),
        "cpus": self._cpu_counts,
        "mem": self._get_mem_usage(),                          # 节点级 total/avail/pct/used
        "shm": self._get_shm_usage(),
        "workers": await self._async_get_workers_and_agents(gpus),  # 每进程 psutil 信息
        "raylet": raylet,                                     # raylet 进程信息
        "agent": self._get_agent(),                           # agent 进程信息
        "gcs": self._get_gcs(),                               # GCS 进程信息 (仅 head)
    }
```

#### `_get_mem_usage()` — 节点级内存（受 cgroup 影响）

```python
# reporter_agent.py:910
@staticmethod
def _get_mem_usage():
    total = get_system_memory()     # ← 读 cgroup limit + psutil，取 min
    used = utils.get_used_memory()   # ← 读 cgroup usage - cache，fallback psutil
    available = total - used
    percent = round(used / total, 3) * 100
    return total, available, percent, used
```

### 传输端：GCS Pub/Sub

```python
# reporter_agent.py:1843
await self._gcs_client.async_publish_node_resource_usage(
    self._key, json_payload       # key = "RAY_REPORTER:{node_id_hex}"
)
```

### 接收端：NodeHead 订阅

**文件**: `python/ray/dashboard/modules/node/node_head.py:494`

```python
subscriber = GcsAioResourceUsageSubscriber(address=self.gcs_address)
await subscriber.subscribe()
# ...
key, data = await subscriber.poll()
parsed_data = await self._loop.run_in_executor(
    self._node_executor, _parse_node_stats, data
)
node_id = key.split(":")[-1]
DataSource.node_physical_stats[node_id] = parsed_data
```

### 数据组织：DataOrganizer 合并

**文件**: `python/ray/dashboard/modules/node/datacenter.py:105`

```python
@classmethod
def _extract_workers_for_node(cls, node_physical_stats, node_stats):
    workers = []
    # 将 coreWorkerStats (来自 raylet 的 gRPC) 合并到 workers (来自 ReporterAgent)
    pid_to_worker_stats = {}
    for core_worker_stats in node_stats.get("coreWorkersStats", []):
        pid = core_worker_stats["pid"]
        pid_to_worker_stats[pid] = core_worker_stats

    for worker in node_physical_stats.get("workers", []):
        worker = dict(worker)
        pid = worker["pid"]
        core_worker_stats = pid_to_worker_stats.get(pid)
        worker["coreWorkerStats"] = [core_worker_stats] if core_worker_stats else []
        workers.append(worker)
    return workers
```

### 前端展示：NodeRow.tsx

**文件**: `python/ray/dashboard/client/src/pages/node/NodeRow.tsx:229`

```tsx
export const WorkerRow = ({ node, worker }: WorkerRowProps) => {
  const { mem, raylet: { nodeId } } = node;
  const { pid, cpuPercent: cpu, memoryInfo, cmdline } = worker;

  return (
    <TableRow>
      {/* 进程名：cmdline[0]，如 "ray::DashboardAgent" */}
      <TableCell align="center">{cmdline[0]}</TableCell>

      {/* 内存：memoryInfo.rss / mem[0] = 进程RSS / 节点总内存 */}
      <TableCell>
        {mem && (
          <PercentageBar num={memoryInfo.rss} total={mem[0]}>
            {memoryConverter(memoryInfo.rss)}/{memoryConverter(mem[0])}(
            {((memoryInfo.rss / mem[0]) * 100).toFixed(1)}%)
          </PercentageBar>
        )}
      </TableCell>
    </TableRow>
  );
};
```

**文件**: `python/ray/dashboard/client/src/util/converter.ts`

```typescript
export const memoryConverter = (bytes: number) => {
  if (bytes < 1024 ** 3) {
    return `${(bytes / 1024 ** 2).toFixed(2)}MB`;  // 如 231.40MB
  }
  if (bytes < 1024 ** 4) {
    return `${(bytes / 1024 ** 3).toFixed(2)}GB`;
  }
};
```

**注意**：WorkerRow 中 `mem[0]` 是节点总内存（来自 `_get_mem_usage()` 的 total，受 cgroup 影响），而 `memoryInfo.rss` 是进程 RSS（不受 cgroup 影响）。当 cgroup limit 无效时，`mem[0]` = 宿主机总内存，百分比会偏低。

### Prometheus 指标导出

ReporterAgent 还将每进程内存导出为 Prometheus 指标：

```python
# reporter_agent.py:1220
def _generate_system_stats_record(self, stats, component_name, pid=None):
    for stat in stats:
        memory_info = stat.get("memory_info")
        if memory_info:
            total_rss += float(memory_info.rss) / 1.0e6   # 除以 1e6，近似 MB
            if hasattr(memory_info, "shared"):
                total_shm += float(memory_info.shared)

        # Linux: USS ≈ RSS - shared
        mem_full_info = stat.get("memory_full_info")
        if memory_info is not None and hasattr(memory_info, "shared"):
            total_uss += float(memory_info.rss - memory_info.shared) / 1.0e6

    # 导出指标
    records.append(Record(gauge=METRICS_GAUGES["component_rss_mb"], value=total_rss, tags=tags))
    records.append(Record(gauge=METRICS_GAUGES["component_uss_mb"], value=total_uss, tags=tags))
```

---

## 数据流详解 2：Python 侧节点级内存计算

### `get_system_memory()` — 节点总内存

**文件**: `python/ray/_common/utils.py:385`

```python
def get_system_memory(
    memory_limit_filename: str = "/sys/fs/cgroup/memory/memory.limit_in_bytes",
    memory_limit_filename_v2: str = "/sys/fs/cgroup/memory/memory.max",
):
    docker_limit = None
    if os.path.exists(memory_limit_filename):
        with open(memory_limit_filename, "r") as f:
            docker_limit = int(f.read().strip())    # 读 cgroup v1 limit
    elif os.path.exists(memory_limit_filename_v2):
        with open(memory_limit_filename_v2, "r") as f:
            max_file = f.read().strip()
            if max_file.isnumeric():
                docker_limit = int(max_file)        # 读 cgroup v2 limit
            else:
                docker_limit = None                 # "max" = 未设置

    psutil_memory_in_bytes = psutil.virtual_memory().total   # 宿主机物理内存

    if docker_limit is not None:
        return min(docker_limit, psutil_memory_in_bytes)      # 取较小值
    return psutil_memory_in_bytes                             # 无 cgroup → 用宿主机
```

**问题节点的计算路径**：

```
docker_limit = 9223372036854771712 (9.2EB, 合法数值，不是 None)
psutil_memory_in_bytes = 1056140440 * 1024 = 1081487791360 (~1003GB)
min(9.2EB, 1003GB) = 1003GB  ← 返回宿主机总量
```

**正常节点的计算路径**：

```
docker_limit = 526133493760 (491GB)
psutil_memory_in_bytes = 1081GB
min(491GB, 1081GB) = 491GB  ← 返回 cgroup 限制值
```

### `get_used_memory()` — 节点已用内存

**文件**: `python/ray/_private/utils.py:584`

```python
def get_used_memory():
    docker_usage = None
    memory_usage_filename_v1 = "/sys/fs/cgroup/memory/memory.usage_in_bytes"
    memory_stat_filename_v1 = "/sys/fs/cgroup/memory/memory.stat"
    memory_usage_filename_v2 = "/sys/fs/cgroup/memory.current"
    memory_stat_filename_v2 = "/sys/fs/cgroup/memory.stat"

    if os.path.exists(memory_usage_filename_v1) and os.path.exists(memory_stat_filename_v1):
        docker_usage = get_cgroup_used_memory(
            memory_stat_filename_v1,
            memory_usage_filename_v1,
            "total_inactive_file",
            "total_active_file",
        )
    elif os.path.exists(memory_usage_filename_v2) and os.path.exists(memory_stat_filename_v2):
        docker_usage = get_cgroup_used_memory(
            memory_stat_filename_v2,
            memory_usage_filename_v2,
            "inactive_file",
            "active_file",
        )

    if docker_usage is not None:
        return docker_usage          # cgroup used = usage_in_bytes - file_cache
    return psutil.virtual_memory().used  # fallback: 宿主机已用
```

**`get_cgroup_used_memory()` 计算逻辑**（与 C++ 侧 `GetCGroupMemoryUsedBytes` 一致）：

```python
# python/ray/_private/utils.py:502
def get_cgroup_used_memory(memory_stat_filename, memory_usage_filename,
                            inactive_file_key, active_file_key):
    inactive_file_bytes = -1
    active_file_bytes = -1
    with open(memory_stat_filename, "r") as f:
        for line in f.readlines():
            if f"{inactive_file_key} " in line:
                inactive_file_bytes = int(line.split()[1])
            elif f"{active_file_key} " in line:
                active_file_bytes = int(line.split()[1])

    with open(memory_usage_filename, "r") as f:
        cgroup_usage_in_bytes = int(f.readline().strip())

    if inactive_file_bytes == -1 or active_file_bytes == -1:
        return None

    # 关键：usage_in_bytes - inactive_file - active_file = 真实使用
    return cgroup_usage_in_bytes - inactive_file_bytes - active_file_bytes
```

**注意**：Python 侧的 `get_used_memory()` **不区分** cgroup limit 是否有效——只要 cgroup 文件存在就从中读取 usage。这与 C++ 侧不同（C++ 侧在 cgroup limit 无效时 fallback 到 `/proc/meminfo`）。

### `resolve_object_store_memory()` — Object Store 内存计算

**文件**: `python/ray/_private/utils.py:534`

```python
def resolve_object_store_memory(available_memory_bytes, object_store_memory=None):
    if object_store_memory is None:
        object_store_memory_cap = ray_constants.DEFAULT_OBJECT_STORE_MAX_MEMORY_BYTES  # 200GB

        if sys.platform == "linux" or sys.platform == "linux2":
            shm_avail = get_shared_memory_bytes() * 0.95
            shm_cap = max(ray_constants.REQUIRE_SHM_SIZE_THRESHOLD, shm_avail)
            object_store_memory_cap = min(object_store_memory_cap, shm_cap)

        # 默认取可用内存的 30%
        object_store_memory = int(
            available_memory_bytes
            * ray_constants.DEFAULT_OBJECT_STORE_MEMORY_PROPORTION  # 0.3
        )

        # 被硬上限截断
        if object_store_memory > object_store_memory_cap:
            object_store_memory = object_store_memory_cap  # 200GB

    return object_store_memory
```

**问题节点**：
```
available_memory_bytes = 1081GB - used (used 来自 cgroup usage)
→ available ≈ 1000GB (因为 cgroup used 远小于 1081GB)
→ object_store_memory = 1000GB * 0.3 = 300GB
→ 300GB > 200GB → 被截断为 200GB
```

---

## 数据流详解 3：C++ 侧 OOM 监控（ThresholdMemoryMonitor）

### 整体架构

```
ThresholdMemoryMonitor (构造函数)
  → 初始化时计算阈值: computed_threshold_bytes_ = max(total * 0.95, total - min_free)
  → PeriodicalRunner 每 monitor_interval_ms (默认250ms) 轮询:
    → TakeSystemMemorySnapshot(root_cgroup_path_)
    → IsUsageAboveThreshold(snapshot, computed_threshold_bytes_)
    → if (used > threshold && IsEnabled()):
        → Disable()
        → kill_workers_callback_(snapshot)  ← 触发杀 worker
```

### `TakeSystemMemorySnapshot()` — 核心内存计算

**文件**: `src/ray/common/memory_monitor_utils.cc:30`

```cpp
const SystemMemorySnapshot MemoryMonitorUtils::TakeSystemMemorySnapshot(
    const std::string root_cgroup_path, const std::string proc_dir) {
  auto [cgroup_used_bytes, cgroup_total_bytes] = GetCGroupMemoryBytes(root_cgroup_path);
  auto [system_used_bytes, system_total_bytes] = GetLinuxMemoryBytes(proc_dir);

  /// cgroup memory limit can be higher than system memory limit when it is
  /// not used. We take its value only when it is less than or equal to system memory
  /// limit. TODO(clarng): find a better way to detect cgroup memory limit is used.
  system_total_bytes = NullableMin(system_total_bytes, cgroup_total_bytes);

  /// This assumes cgroup total bytes will look different than system (meminfo)
  if (system_total_bytes == cgroup_total_bytes) {
    system_used_bytes = cgroup_used_bytes;     // cgroup 生效 → 用 cgroup used
  }
  // else: system_used_bytes 保持不变 → 用 /proc/meminfo 的宿主机 used

  return SystemMemorySnapshot{system_used_bytes, system_total_bytes};
}
```

**`NullableMin()` 实现**：

```cpp
// memory_monitor_utils.cc:404
int64_t MemoryMonitorUtils::NullableMin(int64_t left, int64_t right) {
  RAY_CHECK_GE(left, MemoryMonitorInterface::kNull);  // kNull = -1
  RAY_CHECK_GE(right, MemoryMonitorInterface::kNull);

  if (left == MemoryMonitorInterface::kNull) return right;
  else if (right == MemoryMonitorInterface::kNull) return left;
  else return std::min(left, right);
}
```

### `GetCGroupMemoryBytes()` — cgroup 内存读取

**文件**: `src/ray/common/memory_monitor_utils.cc:103`

```cpp
std::tuple<int64_t, int64_t> MemoryMonitorUtils::GetCGroupMemoryBytes(
    const std::string root_cgroup_path) {
  // cgroup v1 路径
  std::string cgroupV1MemoryMaxPath = root_cgroup_path + "/" + kCgroupsV1MemoryMaxPath;
  // = "/sys/fs/cgroup/memory/memory.limit_in_bytes"

  // cgroup v2 路径
  std::string cgroupV2MemoryMaxPath = root_cgroup_path + "/" + kCgroupsV2MemoryMaxPath;
  // = "/sys/fs/cgroup/memory.max"

  int64_t total_bytes = MemoryMonitorInterface::kNull;  // kNull = -1

  // 优先读 v2，再读 v1
  if (std::filesystem::exists(cgroupV2MemoryMaxPath)) {
    std::ifstream mem_file(cgroupV2MemoryMaxPath);
    mem_file >> total_bytes;
  } else if (std::filesystem::exists(cgroupV1MemoryMaxPath)) {
    std::ifstream mem_file(cgroupV1MemoryMaxPath);
    mem_file >> total_bytes;   // 问题节点: 读到 9223372036854771712
  }

  // cgroup v2 未设置限制时 max 文件内容为 "max"，读入失败 total_bytes 保持 0
  // 代码显式将 0 转为 kNull
  if (total_bytes == 0) {
    total_bytes = MemoryMonitorInterface::kNull;
  }

  // ★ 但 cgroup v1 中 9.2EB 是合法数值，不会触发此判断
  // 9223372036854771712 != 0 → total_bytes = 9.2EB (合法)

  // 读取 used
  int64_t used_bytes = MemoryMonitorInterface::kNull;
  if (/* cgroup v2 存在 */) {
    used_bytes = GetCGroupMemoryUsedBytes(..., "inactive_file", "active_file");
  } else if (/* cgroup v1 存在 */) {
    used_bytes = GetCGroupMemoryUsedBytes(..., "total_inactive_file", "total_active_file");
  }

  // used >= total 时修正为 total（仅 total_bytes != kNull 时）
  if (total_bytes != MemoryMonitorInterface::kNull) {
    if (used_bytes >= total_bytes) {
      used_bytes = total_bytes;
    }
  }

  return {used_bytes, total_bytes};
}
```

### `GetCGroupMemoryUsedBytes()` — cgroup 已用内存

**文件**: `src/ray/common/memory_monitor_utils.cc:52`

```cpp
int64_t MemoryMonitorUtils::GetCGroupMemoryUsedBytes(
    const char *stat_path, const char *usage_path,
    const char *inactive_file_key, const char *active_file_key) {
  // 读 memory.stat，提取 inactive_file 和 active_file
  int64_t inactive_file_bytes = kNull;
  int64_t active_file_bytes = kNull;
  while (std::getline(memstat_ifs, line)) {
    std::istringstream iss(line);
    iss >> title >> value;
    if (title == inactive_file_key) inactive_file_bytes = value;
    else if (title == active_file_key) active_file_bytes = value;
  }

  // 读 usage_in_bytes
  int64_t current_usage_bytes = kNull;
  memusage_ifs >> current_usage_bytes;

  // 关键计算：usage - file_cache = working set
  return current_usage_bytes - inactive_file_bytes - active_file_bytes;
}
```

### `GetLinuxMemoryBytes()` — /proc/meminfo 读取

**文件**: `src/ray/common/memory_monitor_utils.cc:175`

```cpp
std::tuple<int64_t, int64_t> MemoryMonitorUtils::GetLinuxMemoryBytes(
    const std::string proc_dir) {
  std::string meminfo_path = proc_dir + "/meminfo";  // "/proc/meminfo"

  int64_t mem_total_bytes = kNull;
  int64_t mem_available_bytes = kNull;
  while (std::getline(meminfo_ifs, line)) {
    iss >> title >> value >> unit;
    value = value * 1024;  // kB → bytes
    if (title == "MemAvailable:") mem_available_bytes = value;
    else if (title == "MemTotal:") mem_total_bytes = value;
  }

  int64_t used_bytes = mem_total_bytes - mem_available_bytes;
  return {used_bytes, mem_total_bytes};
}
```

### 问题节点的完整执行路径

```
1. GetCGroupMemoryBytes("/sys/fs/cgroup"):
   total_bytes = 9223372036854771712 (9.2EB, 从 memory.limit_in_bytes 读入)
   used_bytes  = 86248562688 (~80GB, 从 usage_in_bytes - file_cache 计算)
   → 返回 {86248562688, 9223372036854771712}

2. GetLinuxMemoryBytes("/proc"):
   mem_total = 1056140440 * 1024 = 1081487791360 (~1003GB)
   mem_available = 837998744 * 1024 = 860155901952 (~799GB)
   used = 1081487791360 - 860155901952 = 221332889408 (~207GB)
   → 返回 {221332889408, 1081487791360}

3. NullableMin(1081487791360, 9223372036854771712):
   → min(1003GB, 9.2EB) = 1003GB
   → system_total_bytes = 1081487791360

4. 判断: system_total_bytes == cgroup_total_bytes?
   → 1081487791360 == 9223372036854771712? → NO
   → system_used_bytes 保持 /proc/meminfo 值 = 221332889408 (~207GB 宿主机已用)

5. 返回 SystemMemorySnapshot{221332889408, 1081487791360}
   → used/total = 207/1003 = 20.6%（当前正常，但宿主机其他 Pod 增加负载后会飙升至 >95%）
```

### 正常节点的执行路径

```
1. GetCGroupMemoryBytes:
   total_bytes = 526133493760 (491GB)
   used_bytes  = cgroup_used

2. GetLinuxMemoryBytes:
   mem_total = 1081GB
   used = 宿主机已用

3. NullableMin(1081GB, 491GB) = 491GB
   → system_total_bytes = 526133493760

4. 判断: 526133493760 == 526133493760? → YES (cgroup_total == system_total)
   → system_used_bytes = cgroup_used_bytes  ← 只用 Pod 内的内存

5. 返回 SystemMemorySnapshot{cgroup_used, 526133493760}
   → 不受宿主机其他 Pod 影响
```

---

## 数据流详解 4：C++ 侧进程级内存（raylet TopN 日志）

C++ raylet 还有一个独立的进程级内存监控，用于 OOM 日志中打印 TopN 内存进程：

### `TakePerProcessMemorySnapshot()` — 进程级内存快照

**文件**: `src/ray/common/memory_monitor_utils.cc:368`

```cpp
const ProcessesMemorySnapshot MemoryMonitorUtils::TakePerProcessMemorySnapshot(
    const std::string proc_dir) {
  std::vector<pid_t> pids = GetPidsFromDir(proc_dir);  // 扫描 /proc/[pid]/
  absl::flat_hash_map<pid_t, int64_t> pid_to_memory_usage;

  for (int32_t pid : pids) {
    int64_t memory_used_bytes = GetProcessMemoryBytes(pid, proc_dir);
    if (memory_used_bytes != kNull) {
      pid_to_memory_usage.insert({pid, memory_used_bytes});
    }
  }
  return pid_to_memory_usage;
}
```

### `GetProcessMemoryBytes()` — 读取进程 USS

```cpp
int64_t MemoryMonitorUtils::GetProcessMemoryBytes(pid_t pid, const std::string proc_dir) {
  std::stringstream smaps_path;
  smaps_path << proc_dir << "/" << std::to_string(pid) << "/smaps_rollup";
  // = "/proc/{pid}/smaps_rollup"
  return GetLinuxProcessMemoryBytesFromSmap(smaps_path.str());
}
```

### `GetLinuxProcessMemoryBytesFromSmap()` — 解析 smaps_rollup

```cpp
int64_t MemoryMonitorUtils::GetLinuxProcessMemoryBytesFromSmap(const std::string smap_path) {
  std::ifstream smap_ifs(smap_path);
  int64_t uss = 0;

  std::string line;
  std::getline(smap_ifs, line);  // 跳过 header
  while (std::getline(smap_ifs, line)) {
    std::istringstream iss(line);
    iss >> title >> value >> unit;
    RAY_CHECK(unit == "kB");
    // 累加所有 Private_* 字段：Private_Clean, Private_Dirty, Private_Hugetlb
    if (boost::starts_with(title, "Private_")) {
      uss += value * 1024;
    }
  }
  return uss;
}
```

**关键区别**：C++ 侧的进程级内存读取的是 `/proc/PID/smaps_rollup` 中的 **USS**（Unique Set Size，即 Private_Clean + Private_Dirty + Private_Hugetlb），而 Python Dashboard 侧读取的是 `psutil.Process.memory_info().rss`（RSS，Resident Set Size）。

| 指标 | 含义 | 数据源 | Dashboard 用 | C++ TopN 日志用 |
|------|------|--------|-------------|----------------|
| **RSS** | 驻留物理内存（含共享库） | `/proc/PID/statm` | ✅ | ❌ |
| **USS** | 进程独占物理内存 | `/proc/PID/smaps_rollup` | ❌ | ✅ |

RSS ≥ USS，因为 RSS 包含了共享库映射等非独占内存。

---

## 问题链路汇总

### 问题链路 1：object_store_memory 为何是 200GB

```
cgroup memory.limit_in_bytes = 9.2EB
  → get_system_memory() = min(9.2EB, 1081GB) = 1081GB
  → estimate_available_memory() ≈ 1000GB (因为 get_used_memory 读的是 cgroup used ~80GB)
  → resolve_object_store_memory() = min(1000GB * 0.3, 200GB) = 200GB ← 被硬上限截断
```

硬上限常量：

```python
# python/ray/_private/ray_constants.py:90
DEFAULT_OBJECT_STORE_MAX_MEMORY_BYTES = 200 * 10**9  # 200 GB
DEFAULT_OBJECT_STORE_MEMORY_PROPORTION = 0.3         # 30%
```

### 问题链路 2：OOM 误杀 — 宿主机全局内存被算入 used

```
C++ raylet: ThresholdMemoryMonitor (每 250ms)
  → cgroup_total = 9.2EB (合法值，不是 kNull)
  → system_total = NullableMin(MemTotal=1003GB, cgroup_total=9.2EB) = 1003GB
  → system_total(1003GB) == cgroup_total(9.2EB)? → NO
  → system_used_bytes = /proc/meminfo used (宿主机全局已用)
  → 同机其他 Pod 吃内存 → used/total > 0.95 → 杀 worker (误杀)
```

### 问题链路 3：Object Store 共享内存被计入 used

Object Store 使用 `/dev/shm` 共享内存，在内存统计中：

| 来源 | Object Store 内存是否计入 | 说明 |
|------|--------------------------|------|
| `/proc/meminfo` MemAvailable | **计入 MemTotal 但减少 MemAvailable** | shm 占用减少可用内存 |
| cgroup `usage_in_bytes` | **计入** | cgroup stat 中 `shmem` 列 |
| cgroup used (减去 file cache) | **计入** | `inactive_file + active_file` 很小，大部分 usage 是 shm + rss |

Object Store 的 200GB 共享内存被完整计入了 used，推高内存使用率。

### cgroup 路径的含义

| cgroup 路径前缀 | 含义 |
|---|---|
| `kubepods.slice` | 标准 K8s Pod，设了 `resources.limits` |
| `kubepods-burstable.slice` | Burstable QoS，设了 request 但没设 limit |
| `kubemidpods.slice` | 内部自定义中优先级，可能不设硬 limit |

**cgroup 路径不直接导致问题**，但它反映该 Pod 被调度到了不设硬内存限制的优先级类别。

## 完整因果链

```
Pod 未设 resources.limits.memory
  → K8s cgroup memory.limit_in_bytes = 9.2EB (无限制)
    → Python: get_system_memory() = 1081GB (宿主机总量)
      → object_store_memory = min(1081G*0.3, 200GB) = 200GB (被硬上限截断)
      → memory 资源可能远超 Pod 实际可用

    → Python Dashboard: _get_mem_usage()
      → total = 1081GB (宿主机总量)
      → used = cgroup usage - cache (~80GB, Pod 内真实值)
      → 显示节点内存使用率偏低（~8%），与实际不符

    → C++ raylet: ThresholdMemoryMonitor
      → cgroup_total = 9.2EB, system_total = min(1003GB, 9.2EB) = 1003GB
      → 1003GB != 9.2EB → 走 /proc/meminfo fallback
      → used = 宿主机 MemTotal - MemAvailable (含所有 Pod + 所有 shm)
      → 同机其他 Pod 吃内存 → used/total > 0.95 → 杀 worker (误杀)
```

## 修复方案

- cluster 展示以及 Object store 计算时，如果 cgroup 值 > system_total，就使用启动参数中 memory 进行 base 计算
- raylet Monitor 这里的机制暂不修改，以及考虑后面 v2 场景下如何支持适配

## 解决方案

### 方案 1：给 Pod 设置 `resources.limits.memory`（推荐）

在 KubeRay 的 RayCluster CR 中为 Pod 设置 memory limit：

```yaml
spec:
  workerGroupSpecs:
  - template:
      spec:
        containers:
        - name: ray-worker
          resources:
            limits:
              memory: "500Gi"
            requests:
              memory: "500Gi"
```

设了 limit 后：
- cgroup `memory.limit_in_bytes` = 500GB
- `get_system_memory()` = 500GB → object_store_memory 按比例计算
- `ThresholdMemoryMonitor` 走 cgroup 路径 → used 只算 Pod 内
- 不受同机其他 Pod 影响

### 方案 2：显式指定 `--object-store-memory`

绕过自动计算，在 KubeRay 的 `rayStartParams` 中指定：

```yaml
rayStartParams:
  object-store-memory: "100000000000"  # 100GB
```

但这只解决 object_store_memory 问题，不解决 OOM 误杀问题。

### 方案 3：调整 OOM 监控阈值（临时缓解）

```
RAY_memory_usage_threshold=0.99     # 提高阈值（默认0.95）
RAY_memory_monitor_refresh_ms=0    # 禁用 OOM 监控
```

不推荐：只是掩盖问题，不能根本解决。

## 如何诊断此类问题

### 1. 检查 cgroup memory limit

```bash
cat /sys/fs/cgroup/memory/memory.limit_in_bytes
# 9223372036854771712 = 无限制（有问题）
# 正常数字如 526133493760 = 有限制（正常）
```

### 2. 检查 /proc/meminfo vs cgroup 差异

```bash
grep MemTotal /proc/meminfo          # 宿主机总内存
cat /sys/fs/cgroup/memory/memory.usage_in_bytes  # cgroup 实际使用
grep total_inactive_file /sys/fs/cgroup/memory/memory.stat
grep total_active_file /sys/fs/cgroup/memory/memory.stat
```

### 3. 检查 raylet 启动参数

```bash
ps -ef | grep raylet | grep -oP 'static_resource_list=\S+'
# 查看 memory 和 object_store_memory 的实际值
```

### 4. 检查 KubeRay 生成的启动命令

```bash
env | grep KUBERAY_GEN_RAY_START_CMD
# 看是否显式指定了 --memory
```

## 相关代码文件

| 文件 | 作用 |
|------|------|
| `python/ray/_common/utils.py:385` | `get_system_memory()` — 读 cgroup limit + psutil，取 min |
| `python/ray/_private/utils.py:502` | `get_cgroup_used_memory()` — cgroup usage - file cache |
| `python/ray/_private/utils.py:584` | `get_used_memory()` — 读 cgroup used，fallback psutil |
| `python/ray/_private/utils.py:623` | `estimate_available_memory()` — total - used |
| `python/ray/_private/utils.py:534` | `resolve_object_store_memory()` — 自动计算，200GB 硬上限 |
| `python/ray/_private/ray_constants.py:90` | `DEFAULT_OBJECT_STORE_MAX_MEMORY_BYTES = 200GB` |
| `python/ray/dashboard/modules/reporter/reporter_agent.py:393` | `PSUTIL_PROCESS_ATTRS` — 定义采集的进程属性 |
| `python/ray/dashboard/modules/reporter/reporter_agent.py:960` | `_async_get_worker_processes()` — 获取 worker 进程 |
| `python/ray/dashboard/modules/reporter/reporter_agent.py:1003` | `_async_get_workers_and_agents()` — 组装 worker 内存信息 |
| `python/ray/dashboard/modules/reporter/reporter_agent.py:1130` | `_async_collect_stats()` — 组装完整 stats dict |
| `python/ray/dashboard/modules/reporter/reporter_agent.py:910` | `_get_mem_usage()` — 节点级内存 total/used/avail |
| `python/ray/dashboard/modules/reporter/reporter_agent.py:1220` | `_generate_system_stats_record()` — Prometheus 指标导出 |
| `python/ray/dashboard/modules/reporter/reporter_agent.py:1830` | `_run_loop()` — 每 5s 采集并发布到 GCS |
| `python/ray/dashboard/modules/node/datacenter.py:105` | `_extract_workers_for_node()` — 合并物理 stats 与 coreWorkerStats |
| `python/ray/dashboard/modules/node/datacenter.py:130` | `get_node_info()` — 组装最终 API 响应 |
| `python/ray/dashboard/modules/node/node_head.py:494` | `_update_node_physical_stats()` — 订阅 GCS pub/sub |
| `python/ray/dashboard/client/src/pages/node/NodeRow.tsx:229` | `WorkerRow` — 前端进程内存展示 |
| `python/ray/dashboard/client/src/util/converter.ts` | `memoryConverter()` — bytes → MB/GB 格式化 |
| `src/ray/common/memory_monitor_utils.cc:30` | `TakeSystemMemorySnapshot()` — C++ OOM 监控核心 |
| `src/ray/common/memory_monitor_utils.cc:103` | `GetCGroupMemoryBytes()` — C++ 读 cgroup limit + used |
| `src/ray/common/memory_monitor_utils.cc:52` | `GetCGroupMemoryUsedBytes()` — C++ cgroup used = usage - cache |
| `src/ray/common/memory_monitor_utils.cc:175` | `GetLinuxMemoryBytes()` — C++ 读 /proc/meminfo |
| `src/ray/common/memory_monitor_utils.cc:368` | `TakePerProcessMemorySnapshot()` — C++ 进程级内存快照 |
| `src/ray/common/memory_monitor_utils.cc:381` | `GetLinuxProcessMemoryBytesFromSmap()` — 读 /proc/PID/smaps_rollup USS |
| `src/ray/common/threshold_memory_monitor.cc:30` | `ThresholdMemoryMonitor` — 每 250ms 轮询，超阈值杀 worker |
| `src/ray/common/memory_monitor_interface.h:57` | `kNull = -1`，`kDefaultCgroupPath = "/sys/fs/cgroup"` |
| `src/ray/common/memory_monitor_utils.h:121` | cgroup v1/v2 路径常量定义 |
| `src/ray/common/ray_config_def.h:74` | `memory_usage_threshold = 0.95` |

---

## 修复方案实现

### 改动 1：Python 侧 — 当 cgroup 值无效时使用 `--memory` 启动参数

**问题**：`get_system_memory()` 在 cgroup limit 为 9.2EB 时返回宿主机全局内存，导致 `object_store_memory` 和 `memory` 资源计算异常。

**方案**：不修改 `get_system_memory()` 本身（保持无参纯函数特性，避免破坏 14+ 个调用点），而是在上层调用处检测 cgroup 无效后使用 `--memory` 参数替代。

#### 新增 `is_cgroup_memory_limit_valid()`

**文件**：`python/ray/_common/utils.py`

```python
def is_cgroup_memory_limit_valid(
    memory_limit_filename: str = "/sys/fs/cgroup/memory/memory.limit_in_bytes",
    memory_limit_filename_v2: str = "/sys/fs/cgroup/memory.max",
) -> bool:
    docker_limit = None
    if os.path.exists(memory_limit_filename):
        with open(memory_limit_filename, "r") as f:
            docker_limit = int(f.read().strip())
    elif os.path.exists(memory_limit_filename_v2):
        with open(memory_limit_filename_v2, "r") as f:
            max_file = f.read().strip()
            if max_file.isnumeric():
                docker_limit = int(max_file)
            else:
                return False

    if docker_limit is None:
        return False

    psutil_memory_in_bytes = psutil.virtual_memory().total
    return docker_limit <= psutil_memory_in_bytes
```

判断逻辑：当 `cgroup_limit > psutil_total` 时返回 `False`（即 cgroup limit 无效）。

#### 修改 `_resolve_memory_resources()`

**文件**：`python/ray/_private/resource_and_label_spec.py`

```python
def _resolve_memory_resources(self):
    system_memory = ray._common.utils.get_system_memory()
    if not ray._common.utils.is_cgroup_memory_limit_valid() and self.memory is not None:
        system_memory = min(self.memory, psutil.virtual_memory().total)
        self.available_memory_bytes = system_memory - ray._private.utils.get_used_memory()
    elif self.available_memory_bytes is None:
        self.available_memory_bytes = ray._private.utils.estimate_available_memory()
    # ... 后续逻辑不变
```

**效果**：当 cgroup 无效且指定了 `--memory` 时，`available_memory_bytes` 基于 `--memory` 计算，`object_store_memory` 也随之正确。

#### 修改 `start()` 中 `available_memory_bytes` 计算

**文件**：`python/ray/scripts/scripts.py`

```python
if not ray._common.utils.is_cgroup_memory_limit_valid() and memory is not None:
    available_memory_bytes = (
        min(memory, psutil.virtual_memory().total)
        - ray._private.utils.get_used_memory()
    )
else:
    available_memory_bytes = ray._private.utils.estimate_available_memory()
```

### 改动 2：C++ 侧 — 修复 `TakeSystemMemorySnapshot()` 的 cgroup 判断

**问题**：C++ 侧 `NullableMin(system_total, cgroup_total)` 当 cgroup_total = 9.2EB 时取 system_total，但 `system_total != cgroup_total` 导致走 `/proc/meminfo` fallback，used 取宿主机全局值。

**文件**：`src/ray/common/memory_monitor_utils.cc`

```cpp
const SystemMemorySnapshot MemoryMonitorUtils::TakeSystemMemorySnapshot(
    const std::string root_cgroup_path, const std::string proc_dir) {
  auto [cgroup_used_bytes, cgroup_total_bytes] = GetCGroupMemoryBytes(root_cgroup_path);
  auto [system_used_bytes, system_total_bytes] = GetLinuxMemoryBytes(proc_dir);

  // 新增：当 cgroup_total > system_total 时，视为 cgroup limit 无效
  // 此时使用 cgroup_used（Pod 内真实使用）+ system_total
  if (cgroup_total_bytes != MemoryMonitorInterface::kNull &&
      cgroup_total_bytes > system_total_bytes) {
    return SystemMemorySnapshot{cgroup_used_bytes, system_total_bytes};
  }

  // 原有逻辑...
  system_total_bytes = NullableMin(system_total_bytes, cgroup_total_bytes);
  if (system_total_bytes == cgroup_total_bytes) {
    system_used_bytes = cgroup_used_bytes;
  }
  return SystemMemorySnapshot{system_used_bytes, system_total_bytes};
}
```

**效果**：当 cgroup limit 无效（9.2EB）时，OOM 监控使用 cgroup_used（Pod 内真实内存使用），不再因宿主机其他 Pod 内存压力而误杀。

**现有开关**：`RAY_memory_monitor_refresh_ms=0` 可完全禁用 monitor（返回 NoopMemoryMonitor），不需要新增开关。

### 改动 3：Dashboard 追加 `worker_mem_used` 字段

**问题**：Dashboard 节点内存展示使用系统级 `mem[0]-mem[1]`，不能反映 Ray 进程实际内存占用。

**方案**：在 ReporterAgent 采集时计算所有 Ray 进程 RSS 之和，追加为 `worker_mem_used` 字段。

#### 后端：ReporterAgent

**文件**：`python/ray/dashboard/modules/reporter/reporter_agent.py`

```python
@staticmethod
def _compute_worker_mem_used(workers, raylet, agent, gcs):
    total_rss = 0
    for w in workers or []:
        mi = w.get("memory_info")
        if mi:
            total_rss += mi.rss
    for proc in [raylet, agent, gcs]:
        if proc:
            mi = proc.get("memory_info")
            if mi:
                total_rss += mi.rss
    return total_rss
```

在 `_async_collect_stats()` 中追加 `"worker_mem_used"` 字段到 stats dict。

#### 前端：NodeRow.tsx / NodeCard

**文件**：`python/ray/dashboard/client/src/type/node.d.ts`
- `NodeDetail` 新增 `worker_mem_used?: number`

**文件**：`python/ray/dashboard/client/src/pages/node/NodeRow.tsx`
- NodeRow 新增一列展示 "Ray 进程内存"，使用 `PercentageBar` 组件

**文件**：`python/ray/dashboard/client/src/pages/node/index.tsx`
- NodeCard 新增 "Ray Process Memory" 网格项

### 修复后问题节点的行为

```
1. Python 侧:
   is_cgroup_memory_limit_valid() → False (9.2EB > 1081GB)
   --memory=93834534912 (≈87.5GB) 已指定
   → system_memory = min(87.5GB, 1081GB) = 87.5GB
   → available_memory = 87.5GB - used
   → object_store_memory = min(available * 0.3, 200GB) ≈ 26GB (30% 自然值)
   → memory 资源 = available - object_store ≈ 61GB

2. C++ 侧:
   cgroup_total = 9.2EB > system_total = 1081GB
   → 走新分支: used = cgroup_used (~80GB, Pod 内真实值)
   → total = system_total (1081GB)
   → used/total = 80/1081 ≈ 7.4% (不会误杀)
   注: 此处 total 用 system_total 是因为 cgroup limit 无效，
   但 used 用 cgroup_used 保证了只统计 Pod 内的内存使用

3. Dashboard:
   worker_mem_used = 所有进程 RSS 之和
   → 前端可看到 Ray 进程实际占用 vs 系统总内存的对比
```

### 修改文件清单

| 文件 | 改动 |
|------|------|
| `python/ray/_common/utils.py` | 新增 `is_cgroup_memory_limit_valid()` |
| `python/ray/_private/resource_and_label_spec.py` | `_resolve_memory_resources()` cgroup 无效时用 `--memory` |
| `python/ray/scripts/scripts.py` | `start()` 中 `available_memory_bytes` 同步修复 |
| `src/ray/common/memory_monitor_utils.cc` | `TakeSystemMemorySnapshot()` 修复 cgroup > system 判断 |
| `python/ray/dashboard/modules/reporter/reporter_agent.py` | 追加 `worker_mem_used` 计算 |
| `python/ray/dashboard/client/src/type/node.d.ts` | 追加 `worker_mem_used` 类型定义 |
| `python/ray/dashboard/client/src/pages/node/NodeRow.tsx` | 新增 Ray 进程内存列 |
| `python/ray/dashboard/client/src/pages/node/index.tsx` | NodeCard 展示 worker_mem_used |
