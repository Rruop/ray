# Ray 节点内存异常排查分析报告

## 问题现象

### 环境信息

| 项目 | 值 |
|------|-----|
| 节点 | public-bjx-c58-kce-node24.idchb1az1.hb1.kwaidc.com |
| IP | 10.57.23.226 |
| 内核 | 5.14.0-3.1.8.kwai.x86_64 |
| cgroup 版本 | v1 |
| 容器 cgroup 内存限制 | 128 GB (`memory.limit_in_bytes` = 137,438,953,472) |
| Ray 版本 | 2.55.1 |
| 运行时间 | 9 天（自 2026-05-16） |
| Ray 启动参数 | `--memory=120000000000 --object-store-memory=30000000000 --num-cpus=32` |

### 现象描述

1. **Dashboard 显示**：节点内存使用 119.89GB / 128.00GB (93.7%)，接近阈值
2. **OOM Kill 错误**：
   ```
   Task _map_task failed due to oom. There are infinite oom retries remaining.
   Memory on the node (IP: 10.57.3.22, ID: 08283a27...) was 121.60GB / 128.00GB (0.950009),
   which exceeds the memory usage threshold of 0.95.
   Ray killed this worker (ID: f1632a7a...) because it was the most recently scheduled task.
   ```
3. **矛盾现象**：`ps aux` 显示的进程 RSS 总和远低于 Dashboard 报告的内存使用量

---

## 排查过程

### 第一步：确认容器 cgroup 内存数据

```bash
# 容器 cgroup 内存使用（内核计数器）
cat /sys/fs/cgroup/memory/memory.usage_in_bytes
# 结果: 122439880704 ~ 129409110016 (波动，约 114-120 GB)

# 容器 cgroup 内存限制
cat /sys/fs/cgroup/memory/memory.limit_in_bytes
# 结果: 137438953472 (128 GB)

# cgroup memory.stat 详细统计
cat /sys/fs/cgroup/memory/memory.stat
```

关键 `memory.stat` 数据：

```
cache 5669003264          (5.3 GB)   - 文件页缓存
rss 66885103616           (62.3 GB)  - 匿名内存
shmem 4614643712          (4.3 GB)   - 共享内存(tmpfs/Plasma)
mapped_file 4829868032    (4.5 GB)
inactive_anon 69814243328 (65.0 GB)
active_anon 1872998400    (1.7 GB)
inactive_file 344481792   (0.33 GB)
active_file 258990080     (0.25 GB)
total_inactive_file 518524928  (0.48 GB)  ← Ray 公式中要减去的值
total_active_file 824619008    (0.77 GB)  ← Ray 公式中要减去的值
```

### 第二步：确认进程级别内存使用

```bash
# 按内存排序列出进程
ps -eo pid,rss,vsz,comm --sort=-rss | head -40
```

关键发现：

| 进程 | PID | RSS | 说明 |
|------|-----|-----|------|
| ray::QwenVLCPUP | 976815 | 11.1 GB | 7 个 worker，单个 ~10 GB |
| ray::QwenVLCPUP | 977121 | 10.8 GB | |
| ray::QwenVLCPUP | 977158 | 10.6 GB | |
| ray::QwenVLCPUP | 976947 | 10.1 GB | |
| ray::QwenVLCPUP | 977242 | 10.1 GB | |
| ray::QwenVLCPUP | 977009 | 9.7 GB | |
| ray::QwenVLCPUP | 977305 | 9.2 GB | |
| raylet | 99 | 5.7 GB | 含 4.8 GB shmem |
| ray::DashboardAgent | 169 | 163 MB | |
| ray::RuntimeEnvAgent | 171 | 86 MB | |

QwenVLCPUP workers 合计：

```bash
ps -eo pid,rss,comm | grep 'ray::QwenVL' | awk '{sum+=$2; count++} END{print "QwenVLCPUP workers:", count, "total RSS:", sum/1024/1024, "GB"}'
# 结果: QwenVLCPUP workers: 7 total RSS: 70.5 GB
```

### 第三步：分析 smaps_rollup（精确去重的内存统计）

```bash
cat /proc/*/smaps_rollup 2>/dev/null | grep -E '^(Rss|Pss|Shared|Private|Swap)' | \
  awk '{a[$1]+=$2} END{for(k in a) print k,a[k],"kB"}'
```

结果：
```
Rss: 81651488 kB         (77.9 GB - 含共享页面重复计算)
Pss: 70193286 kB         (66.9 GB - 按比例分摊，不重复)
Pss_Anon: 65586600 kB    (62.5 GB)
Pss_Shmem: 4506429 kB    (4.3 GB)
Pss_File: 100255 kB      (0.1 GB)
Private_Dirty: 68463032 kB
Shared_Dirty: 11299928 kB
```

### 第四步：发现 kmem 异常

```bash
# 内核内存使用
cat /sys/fs/cgroup/memory/memory.kmem.usage_in_bytes
# 结果: 57742995456 (53.8 GB!)

# 历史最大值
cat /sys/fs/cgroup/memory/memory.kmem.max_usage_in_bytes
# 结果: 57872842752 (53.9 GB - 几乎等于当前值，说明从未被回收)

# TCP 内核内存
cat /sys/fs/cgroup/memory/memory.kmem.tcp.usage_in_bytes
# 结果: 0
```

### 第五步：验证数值对应关系

```python
# 同一时刻采集的数据验证
usage = 123747065856          # memory.usage_in_bytes (115.25 GB)
kmem = 57742995456            # memory.kmem.usage_in_bytes (53.78 GB)
rss = 59873988608             # memory.stat rss (55.76 GB)
cache = 5898461184            # memory.stat cache (5.49 GB)

# 验证: usage ≈ user_memory + kmem
user_mem = rss + cache        # 61.26 GB
total = user_mem + kmem       # 115.03 GB ≈ 115.25 GB ✓ 对上了！
```

### 第六步：分析 kmem 构成

```bash
# 全局 slab 统计
slabtop -o -s c | head -30
```

结果（全机 slab）：
```
OBJS     ACTIVE  USE OBJ SIZE  SLABS  OBJ/SLAB  CACHE SIZE  NAME
694855098 694855010 99%  0.19K  16544169  42  132353352K  dentry
262927056 262927056 100% 0.09K  6260168   42  25040672K   kmalloc-rcl-96
246530432 246530432 100% 0.06K  3852038   64  15408152K   kmalloc-rcl-64
27446208  27405609  99%  0.50K  428847    64  13723104K   kmalloc-512
14317072  11317490  79%  0.57K  255662    56  8181184K    radix_tree_node
```

**关键发现**：全机有 **6.95 亿个 dentry 对象**，占用 126 GB slab 内存。

### 第七步：排除其他原因

```bash
# Page tables（排除）
grep VmPTE /proc/99/status /proc/976815/status ...
# 结果: 所有进程 page table 合计仅 ~207 MB

# 打开的文件描述符数量（排除）
# 结果: 容器内总共 5664 个 fd

# 已删除但仍打开的文件（排除）
ls -la /proc/99/fd/ | grep deleted
# 结果: 仅 1 个 (Plasma store 文件)
```

### 第八步：确认 Object Store 状态

```bash
# /dev/shm 使用情况
du -sh /dev/shm
# 结果: 2.3G（不含已删除的 Plasma 文件）

df -h /dev/shm
# 结果: tmpfs 64G Used 4.3G

# Raylet 的 Plasma 映射
cat /proc/99/smaps | grep -A5 '/dev/shm/plasma'
# 映射1: Size=28GB, Rss=0
# 映射2: Size=28GB, Rss=4.3GB
# 映射3: Size=28GB, Rss=0.4GB

# Plasma 文件状态
ls -la /proc/99/fd/ | grep '/dev/shm'
# lrwx------ 1 root root 64 ... 17 -> /dev/shm/plasmalVMTmq (deleted)
```

### 第九步：验证 Ray OOM 计算公式

```bash
# Ray 使用的 cgroup v1 公式
# ray_used = memory.usage_in_bytes - total_inactive_file - total_active_file

cat /sys/fs/cgroup/memory/memory.usage_in_bytes    # 122439880704
cat /sys/fs/cgroup/memory/memory.stat | grep -E 'total_inactive_file|total_active_file'
# total_inactive_file 518524928
# total_active_file 824619008

# 计算
# ray_used = 122439880704 - 518524928 - 824619008 = 121096736768 (112.8 GB)
# threshold = 137438953472 * 0.95 = 130566905798 (121.6 GB)
# ratio = 121096736768 / 137438953472 = 0.881

# 当波峰时 usage_in_bytes 达到 ~130GB:
# ray_used ≈ 130GB - 1.2GB = 128.8GB > 121.6GB → 触发 OOM
```

---

## 排查结论

### memory.usage_in_bytes 的真实构成

```
memory.usage_in_bytes = 用户态内存 + 内核态内存(kmem)

当前节点（同一时刻采样）：
memory.usage_in_bytes:   115.25 GB
├── 用户态 (rss + cache):  61.26 GB (53.6%)
│   ├── rss (匿名内存):     55.76 GB   ← QwenVL workers + raylet
│   └── cache (文件+shmem):  5.49 GB
│       ├── shmem (Plasma):   4.30 GB   ← Object Store 物理页面
│       └── 纯文件缓存:       1.20 GB
│
└── 内核态 (kmem):          53.78 GB (46.4%)  ← 问题根因！
    主要是 dentry/inode slab cache
```

### Dashboard 119.89GB 的来源

Ray Dashboard 和 OOM 检测使用的公式（源码 `src/ray/common/memory_monitor_utils.cc:96`）：

```cpp
// cgroup v1:
return current_usage_bytes - inactive_file_bytes - active_file_bytes;
// = memory.usage_in_bytes - total_inactive_file - total_active_file
// = ~120 GB - ~0.5 GB - ~0.8 GB ≈ ~119 GB
```

此公式：
- **减去了**：纯文件页缓存（inactive_file + active_file），因为可被内核回收
- **没有减去**：shmem（Plasma Object Store 的 tmpfs 页面）
- **没有减去**：kmem（内核 slab 内存，53.8 GB）

### Object Store 相关内存的区别

| 概念 | 含义 | 数据来源 | 本节点值 |
|------|------|---------|---------|
| Dashboard "Object Store Used" | Plasma 中存活 Ray Object 的逻辑大小之和 | `ObjectManager.used_memory_`（应用层计数） | **424 MB** |
| Object Store 物理页面 | `/dev/shm/plasma*` 文件实际驻留的物理 page | cgroup memory.stat 中的 `shmem` | **4.3 GB** |
| Raylet RssShmem | Raylet 进程 mmap Plasma 后触碰的共享页面 | `/proc/99/status` RssShmem | **4.8 GB** |
| Worker RssShmem | Worker mmap 同一文件的共享页面（不额外占物理内存） | `/proc/<pid>/status` RssShmem | **~1.25 GB/worker** |
| Ray OOM 中的 Object Store 贡献 | 按物理页面（4.3 GB）计入 ray_used，不是 424 MB | cgroup 层面自动统计 | **4.3 GB** |

关键：7 个 worker 和 raylet 映射同一个 Plasma 文件的同一批物理页面，cgroup 只计一次。

### --memory 参数与 OOM 的关系

`--memory` 是纯调度资源声明（源码 `python/ray/scripts/scripts.py:436`），**与 OOM 检测完全无关**。OOM 检测由以下环境变量控制：

| 环境变量 | 默认值 | 说明 |
|----------|--------|------|
| `RAY_memory_usage_threshold` | 0.95 | 超过此比例触发 OOM kill |
| `RAY_min_memory_free_bytes` | -1 (禁用) | 最小可用内存绝对值 |
| `RAY_memory_monitor_refresh_ms` | 250 | 监控间隔，0 禁用 |

---

## 问题根因

### 直接原因

**53.8 GB 的内核内存（kmem）被 cgroup v1 记账到容器的 `memory.usage_in_bytes` 中，但 Ray 的 OOM 公式没有减去这部分内存，导致 Ray 误判节点内存不足并 kill worker。**

实际进程只使用了约 61 GB，但 Ray 认为使用了约 115 GB。

### 深层原因：cgroup v1 kmem 记账的已知内核 Bug

cgroup v1 的 kmem 记账是以 **slab page（4KB）** 为粒度，而不是以单个对象为粒度：

```
┌─── 一个 Slab Page (4096 bytes) ─────────────────────┐
│ [dentry] [dentry] [dentry] ... [dentry]  共 ~21 个  │
│  容器A    容器B    容器A       容器C                   │
│                                                      │
│ Page 的 charge 归属 → 容器 A（首次分配者）             │
│ 即使容器 A 的 dentry 全部释放：                        │
│   - Page 上还有 B/C 的对象 → page 无法归还            │
│   - memory.kmem.usage_in_bytes 不减少                 │
└──────────────────────────────────────────────────────┘
```

在本场景中：
1. QwenVLCPUP 处理视频/图片，9 天内数亿次 `open()`/`stat()` 操作
2. 每次文件操作在内核中创建 dentry/inode → charge 到容器 cgroup 的 kmem
3. 内核 shrinker 回收了 dentry 对象（对象级别释放）
4. 但 slab page 上混杂了其他 cgroup 的对象，page 无法完全清空
5. `memory.kmem.usage_in_bytes` 只增不减（53.89 GB ≈ max 53.90 GB）

---

## 相关源码分析

### Ray OOM 检测流程

```
src/ray/common/memory_monitor_utils.cc (v2.55.1)
src/ray/common/memory_monitor.cc (master)
     │
     ├── TakeSystemMemorySnapshot()          ← 每 250ms 调用
     │     ├── GetCGroupMemoryBytes()        ← 读 cgroup 文件
     │     │     ├── 读 memory.limit_in_bytes → total
     │     │     └── GetCGroupMemoryUsedBytes()
     │     │           ├── 读 memory.usage_in_bytes → current_usage
     │     │           ├── 读 memory.stat → total_inactive_file, total_active_file
     │     │           └── return current_usage - inactive_file - active_file
     │     └── GetLinuxMemoryBytes()         ← 读 /proc/meminfo
     │           └── 取 min(system_total, cgroup_total) 作为 total
     │
     ├── IsUsageAboveThreshold()             ← 判断是否超阈值
     │     └── used_memory_bytes > threshold_bytes ?
     │
     └── kill_workers_callback()             ← 超阈值时触发
           ├── SelectWorkersToKill()         ← 选择要 kill 的 worker
           └── DestroyWorker(NODE_OUT_OF_MEMORY)
```

### 关键源码文件

| 文件 | 作用 |
|------|------|
| `src/ray/common/memory_monitor_utils.cc` (v2.55.1) | cgroup 内存读取和 OOM 公式 |
| `src/ray/common/memory_monitor_utils.h` (v2.55.1) | 路径常量和接口定义 |
| `src/ray/common/memory_monitor.cc` (master) | master 分支重构后的内存监控 |
| `src/ray/common/memory_monitor.h` (master) | master 分支头文件 |
| `src/ray/common/ray_config_def.h` | `memory_usage_threshold`(0.95) 等配置 |
| `src/ray/raylet/node_manager.cc:3034` | `CreateKillWorkersCallback()` OOM kill 逻辑 |
| `src/ray/object_manager/object_manager.cc:170,199` | Object Store `used_memory_` 计数 |
| `python/ray/scripts/scripts.py:436` | `--memory` 参数定义（仅调度资源） |
| `src/ray/common/cgroup2/cgroup_manager.cc` (master) | Resource Isolation cgroup v2 管理 |
| `src/ray/common/cgroup2/cgroup_manager_interface.h` (master) | cgroup v2 接口定义 |

### 完整源码：OOM 内存计算（v2.55.1 分支）

文件：`src/ray/common/memory_monitor_utils.cc`

```cpp
// Copyright 2026 The Ray Authors.
// ...

#include "ray/common/memory_monitor_utils.h"

namespace ray {

const SystemMemorySnapshot MemoryMonitorUtils::TakeSystemMemorySnapshot(
    const std::string root_cgroup_path, const std::string proc_dir) {
  auto [cgroup_used_bytes, cgroup_total_bytes] = GetCGroupMemoryBytes(root_cgroup_path);
  auto [system_used_bytes, system_total_bytes] = GetLinuxMemoryBytes(proc_dir);
  /// cgroup memory limit can be higher than system memory limit when it is
  /// not used. We take its value only when it is less than or equal to system memory
  /// limit.
  system_total_bytes = NullableMin(system_total_bytes, cgroup_total_bytes);
  /// This assumes cgroup total bytes will look different than system (meminfo)
  if (system_total_bytes == cgroup_total_bytes) {
    system_used_bytes = cgroup_used_bytes;
  }
  return SystemMemorySnapshot{system_used_bytes, system_total_bytes};
}

int64_t MemoryMonitorUtils::GetCGroupMemoryUsedBytes(const char *stat_path,
                                                     const char *usage_path,
                                                     const char *inactive_file_key,
                                                     const char *active_file_key) {
  // CGroup reported memory usage includes file page caches
  // and we should exclude those since they are reclaimable
  // by the kernel and are considered available memory from
  // the OOM killer's perspective.
  std::ifstream memstat_ifs(stat_path, std::ios::in | std::ios::binary);
  if (!memstat_ifs.is_open()) {
    RAY_LOG_EVERY_MS(WARNING, MemoryMonitorInterface::kLogIntervalMs)
        << " memory stat file not found: " << stat_path;
    return MemoryMonitorInterface::kNull;
  }
  std::ifstream memusage_ifs(usage_path, std::ios::in | std::ios::binary);
  if (!memusage_ifs.is_open()) {
    RAY_LOG_EVERY_MS(WARNING, MemoryMonitorInterface::kLogIntervalMs)
        << " memory usage file not found: " << usage_path;
    return MemoryMonitorInterface::kNull;
  }

  std::string title;
  int64_t value;
  std::string line;

  int64_t inactive_file_bytes = MemoryMonitorInterface::kNull;
  int64_t active_file_bytes = MemoryMonitorInterface::kNull;
  while (std::getline(memstat_ifs, line)) {
    std::istringstream iss(line);
    iss >> title >> value;
    if (title == inactive_file_key) {
      inactive_file_bytes = value;
    } else if (title == active_file_key) {
      active_file_bytes = value;
    }
  }

  int64_t current_usage_bytes = MemoryMonitorInterface::kNull;
  memusage_ifs >> current_usage_bytes;
  if (current_usage_bytes == MemoryMonitorInterface::kNull ||
      inactive_file_bytes == MemoryMonitorInterface::kNull ||
      active_file_bytes == MemoryMonitorInterface::kNull) {
    RAY_LOG_EVERY_MS(WARNING, MemoryMonitorInterface::kLogIntervalMs)
        << "Failed to parse cgroup memory usage. memory usage "
        << current_usage_bytes << " inactive file " << inactive_file_bytes
        << " active file " << active_file_bytes;
    return MemoryMonitorInterface::kNull;
  }
  // The total file cache is inactive + active per
  // https://access.redhat.com/documentation/en-us/red_hat_enterprise_linux/6/html/
  //   resource_management_guide/sec-memory
  return current_usage_bytes - inactive_file_bytes - active_file_bytes;
}

std::tuple<int64_t, int64_t> MemoryMonitorUtils::GetCGroupMemoryBytes(
    const std::string root_cgroup_path) {
  std::string cgroupV1MemoryMaxPath = root_cgroup_path + "/" + kCgroupsV1MemoryMaxPath;
  std::string cgroupV1MemoryUsagePath =
      root_cgroup_path + "/" + kCgroupsV1MemoryUsagePath;
  std::string cgroupV1MemoryStatPath = root_cgroup_path + "/" + kCgroupsV1MemoryStatPath;
  std::string cgroupV2MemoryMaxPath = root_cgroup_path + "/" + kCgroupsV2MemoryMaxPath;
  std::string cgroupV2MemoryUsagePath =
      root_cgroup_path + "/" + kCgroupsV2MemoryUsagePath;
  std::string cgroupV2MemoryStatPath = root_cgroup_path + "/" + kCgroupsV2MemoryStatPath;

  int64_t total_bytes = MemoryMonitorInterface::kNull;
  if (std::filesystem::exists(cgroupV2MemoryMaxPath)) {
    std::ifstream mem_file(cgroupV2MemoryMaxPath, std::ios::in | std::ios::binary);
    mem_file >> total_bytes;
  } else if (std::filesystem::exists(cgroupV1MemoryMaxPath)) {
    std::ifstream mem_file(cgroupV1MemoryMaxPath, std::ios::in | std::ios::binary);
    mem_file >> total_bytes;
  }

  int64_t used_bytes = MemoryMonitorInterface::kNull;
  if (std::filesystem::exists(cgroupV2MemoryUsagePath) &&
      std::filesystem::exists(cgroupV2MemoryStatPath)) {
    used_bytes = GetCGroupMemoryUsedBytes(cgroupV2MemoryStatPath.c_str(),
                                          cgroupV2MemoryUsagePath.c_str(),
                                          kCgroupsV2MemoryStatInactiveFileKey,
                                          kCgroupsV2MemoryStatActiveFileKey);
  } else if (std::filesystem::exists(cgroupV1MemoryStatPath) &&
             std::filesystem::exists(cgroupV1MemoryUsagePath)) {
    used_bytes = GetCGroupMemoryUsedBytes(cgroupV1MemoryStatPath.c_str(),
                                          cgroupV1MemoryUsagePath.c_str(),
                                          kCgroupsV1MemoryStatInactiveFileKey,
                                          kCgroupsV1MemoryStatActiveFileKey);
  }

  /// This can be zero if the memory limit is not set for cgroup v2.
  if (total_bytes == 0) {
    total_bytes = MemoryMonitorInterface::kNull;
  }

  if (used_bytes < 0) {
    RAY_LOG_EVERY_MS(WARNING, MemoryMonitorInterface::kLogIntervalMs)
        << "Got negative used memory for cgroup " << used_bytes
        << ", setting it to zero";
    used_bytes = 0;
  }
  if (total_bytes != MemoryMonitorInterface::kNull) {
    if (used_bytes >= total_bytes) {
      RAY_LOG_EVERY_MS(WARNING, MemoryMonitorInterface::kLogIntervalMs)
          << "Used memory is greater than or equal to total memory used. "
          << "Used " << used_bytes << ", total " << total_bytes
          << ", setting used to be equal to total";
      used_bytes = total_bytes;
    }
  }

  return {used_bytes, total_bytes};
}
```

### 路径常量定义（v2.55.1 分支）

文件：`src/ray/common/memory_monitor_utils.h`

```cpp
// cgroup v1 路径（相对于 root_cgroup_path）
static constexpr char kCgroupsV1MemoryMaxPath[] = "memory/memory.limit_in_bytes";
static constexpr char kCgroupsV1MemoryUsagePath[] = "memory/memory.usage_in_bytes";
static constexpr char kCgroupsV1MemoryStatPath[] = "memory/memory.stat";
static constexpr char kCgroupsV1MemoryStatInactiveFileKey[] = "total_inactive_file";
static constexpr char kCgroupsV1MemoryStatActiveFileKey[] = "total_active_file";

// cgroup v2 路径（相对于 root_cgroup_path）
static constexpr char kCgroupsV2MemoryMaxPath[] = "memory.max";
static constexpr char kCgroupsV2MemoryUsagePath[] = "memory.current";
static constexpr char kCgroupsV2MemoryStatPath[] = "memory.stat";
static constexpr char kCgroupsV2MemoryStatInactiveFileKey[] = "inactive_file";
static constexpr char kCgroupsV2MemoryStatActiveFileKey[] = "active_file";
```

### OOM 阈值配置

文件：`src/ray/common/ray_config_def.h`

```cpp
/// Threshold when the node is beyond the memory capacity. If the memory is above the
/// memory_usage_threshold and free space is below the min_memory_free_bytes then
/// it will start killing processes to free up the space.
/// Ranging from [0, 1]
RAY_CONFIG(float, memory_usage_threshold, 0.95)

/// The interval between runs of the memory usage monitor.
/// Monitor is disabled when this value is 0.
RAY_CONFIG(uint64_t, memory_monitor_refresh_ms, 250)

/// The minimum amount of free space. If the memory is above the
/// memory_usage_threshold and free space is below min_memory_free_bytes then it
/// will start killing processes to free up the space. Disabled if it is -1.
RAY_CONFIG(int64_t, min_memory_free_bytes, (int64_t)-1)
```

### OOM Kill 回调实现

文件：`src/ray/raylet/node_manager.cc:3034`

```cpp
// Picks the workers and kills the process if the memory usage is above the threshold.
KillWorkersCallback NodeManager::CreateKillWorkersCallback() {
  return [this](const SystemMemorySnapshot &system_memory_snapshot) {
    io_service_.post(
        [this, system_memory = system_memory_snapshot]() {
          ProcessesMemorySnapshot process_memory_snapshot =
              MemoryMonitorUtils::TakePerProcessMemorySnapshot();
          std::vector<std::shared_ptr<WorkerInterface>> workers =
              worker_pool_.GetAllRegisteredWorkers(/* filter_dead_workers */ true,
                                                   /* filter_io_workers */ true);
          if (workers.empty()) {
            RAY_LOG_EVERY_MS(WARNING, 5000)
                << "Memory usage above threshold but no workers are available "
                << "for killing.";
            memory_monitor_->Enable();
            return;
          }
          std::vector<std::pair<std::shared_ptr<WorkerInterface>, bool>>
              workers_to_kill_and_should_retry =
                  worker_killing_policy_->SelectWorkersToKill(
                      workers, process_memory_snapshot, system_memory);
          // ...

          for (const auto &[worker_to_kill, should_retry] :
               workers_to_kill_and_should_retry) {
            rpc::RayErrorInfo worker_failure_reason;
            worker_failure_reason.set_error_message(worker_exit_message);
            worker_failure_reason.set_error_type(rpc::ErrorType::OUT_OF_MEMORY);

            DestroyWorker(worker_to_kill,
                          rpc::WorkerExitType::NODE_OUT_OF_MEMORY,
                          worker_exit_message,
                          true /* force */);
          }
          memory_monitor_->Enable();
        },
        "NodeManager.KillWorkersCallback");
  };
}
```

### Object Store 内存计数

文件：`src/ray/object_manager/object_manager.cc`

```cpp
void ObjectManager::HandleObjectAdded(const ObjectInfo &object_info) {
  const ObjectID &object_id = object_info.object_id;
  RAY_LOG(DEBUG) << "Object added " << object_id;
  RAY_CHECK(local_objects_.count(object_id) == 0);
  local_objects_[object_id].object_info = object_info;
  used_memory_ += object_info.data_size + object_info.metadata_size;  // ← 逻辑大小
  object_directory_->ReportObjectAdded(object_id, self_node_id_, object_info);
  // ...
}

void ObjectManager::HandleObjectDeleted(const ObjectID &object_id) {
  auto it = local_objects_.find(object_id);
  RAY_CHECK(it != local_objects_.end());
  auto object_info = it->second.object_info;
  local_objects_.erase(it);
  used_memory_ -= object_info.data_size + object_info.metadata_size;
  RAY_CHECK(!local_objects_.empty() || used_memory_ == 0);
  object_directory_->ReportObjectRemoved(object_id, self_node_id_, object_info);
  // ...
}

void ObjectManager::FillObjectStoreStats(rpc::GetNodeStatsReply *reply) const {
  auto stats = reply->mutable_store_stats();
  stats->set_object_store_bytes_used(used_memory_);           // ← Dashboard 显示 424 MB
  stats->set_object_store_bytes_fallback(
      plasma::plasma_store_runner->GetFallbackAllocated());
  stats->set_object_store_bytes_avail(config_.object_store_memory);  // ← 30 GB
  stats->set_num_local_objects(local_objects_.size());
  // ...
}
```

### --memory 参数定义

文件：`python/ray/scripts/scripts.py:436`

```python
@click.option(
    "--memory",
    required=False,
    hidden=True,
    type=int,
    help="The amount of memory (in bytes) to make available to workers. "
    "By default, this is set to the available memory on the node.",
)
```

该参数仅用于 Ray 调度器的资源声明（`NodeResources`），不影响内存监控和 OOM 判断。

---

## cgroup v2 详解

### cgroup v2 vs v1 对比

| 方面 | cgroup v1 | cgroup v2 |
|------|-----------|-----------|
| **kmem 记账粒度** | per-page（4KB 页面级） | per-object（单对象级，obj_cgroup） |
| **对象释放行为** | 对象 free 后，page 上有其他 cgroup 对象就不 uncharge | 对象 free 立即 uncharge |
| **dying cgroup** | 已删除 cgroup 的 charge 不 reparent，永久泄漏 | 自动 reparent 给 parent |
| **memory.stat** | 没有 slab 细分 | 提供 `slab_reclaimable` / `slab_unreclaimable` |
| **hierarchy 结构** | 多棵独立 hierarchy（memory, cpu, blkio 分开） | 单一统一 hierarchy |
| **memory 使用文件** | `memory.usage_in_bytes`（包含虚高的 kmem） | `memory.current`（准确反映实际使用） |
| **子 cgroup 委托** | 受限，需要特殊权限 | 天然支持 delegated subtree |
| **内存控制精度** | `memory.limit_in_bytes`（硬限制） | `memory.max`（硬限制）+ `memory.high`（软限制，触发回收）+ `memory.low`（保护） |

### 对本场景的直接影响

```
cgroup v1（当前状态）：
  memory.usage_in_bytes = 115 GB
  其中 kmem = 54 GB（虚高，不可回收，只增不减）
  Ray 公式: 115 GB - 1.2 GB(file cache) = ~114 GB
  threshold = 128 GB * 0.95 = 121.6 GB
  → 波峰时触发 OOM kill

cgroup v2（切换后）：
  memory.current ≈ 65-70 GB（准确反映进程实际使用 + 准确的 slab）
  kmem 部分准确记账，对象释放后立即减少，不会虚高到 54 GB
  Ray 公式: ~67 GB - 1.2 GB(file cache) = ~66 GB
  threshold = 128 GB * 0.95 = 121.6 GB
  → 远低于阈值，不会触发 OOM
```

### 如何启用 cgroup v2

cgroup v2 需要在**宿主机**层面启用，不是容器内可以单独配置的：

```bash
# 1. 修改内核启动参数（GRUB）
vi /etc/default/grub
# 添加到 GRUB_CMDLINE_LINUX:
GRUB_CMDLINE_LINUX="systemd.unified_cgroup_hierarchy=1"

# 2. 更新 GRUB 并重启
grub2-mkconfig -o /boot/grub2/grub.cfg
reboot

# 3. 验证是否为 cgroup v2
stat -f /sys/fs/cgroup/
# 输出 Type: cgroup2fs 表示 v2
# 输出 Type: tmpfs 表示仍是 v1

mount | grep cgroup2
# 应显示: cgroup2 on /sys/fs/cgroup type cgroup2 (rw,nosuid,nodev,noexec,relatime)

# 4. 验证 memory controller 可用
cat /sys/fs/cgroup/cgroup.controllers
# 应包含: memory cpu io
```

**前提条件**：

| 组件 | 最低版本 | 本节点状态 |
|------|---------|-----------|
| Linux kernel | 4.5+（基础），5.2+（完整功能） | 5.14 ✓ |
| systemd | 232+ | 需确认 |
| containerd | 1.4+ | 需确认 |
| runc | 1.0+ | 需确认 |
| Docker | 20.10+ | 需确认 |
| Kubernetes | 1.25+ GA | 需确认 |

### cgroup v2 的 memory.stat 字段

cgroup v2 的 `memory.stat` 比 v1 提供更精确的分项数据：

```bash
# cgroup v2 memory.stat 示例
cat /sys/fs/cgroup/memory.stat
```

```
anon 65000000000                # 匿名内存（堆、栈、mmap anonymous）
file 1200000000                 # 文件页缓存
shmem 4300000000                # 共享内存（tmpfs，包括 Plasma）
kernel 2000000000               # 内核内存（准确值，不是虚高值）
kernel_stack 50000000           # 内核栈
pagetables 200000000            # 页表
slab_reclaimable 1500000000     # 可回收 slab（包括 dentry cache）
slab_unreclaimable 500000000    # 不可回收 slab
sock 0                          # TCP 缓冲区
file_mapped 4500000000          # 映射的文件页面
inactive_anon 65000000000       # 不活跃匿名页
active_anon 1700000000          # 活跃匿名页
inactive_file 300000000         # 不活跃文件页 ← Ray 公式减去
active_file 250000000           # 活跃文件页 ← Ray 公式减去
```

关键区别：
- v2 的 `kernel`/`slab_*` 字段可以让用户精确知道 slab 内存用量
- v2 的 `anon` + `shmem` 直接给出"进程实际使用"的精确值
- v2 不存在 kmem 虚高问题，`memory.current` 准确

### Ray 社区支持 cgroup v2 的原因

#### 1. cgroup v1 的内存计算历史上反复出问题

Ray 社区针对 cgroup v1 内存计算的修复历史：

| 时间 | PR/Issue | 问题 | 修复 |
|------|----------|------|------|
| 2022-10 | #28074/#29103 | `memory.usage_in_bytes` 包含 file cache → 误判 OOM | 减去 `inactive_file` |
| 2022-10 | #29709 | 不支持 cgroup v2 | 添加 v2 路径支持 |
| 2023-06 | #35989 (P0) | 大量磁盘 I/O 导致 cache 虚高 → 生产集群大面积 OOM | 调整公式 |
| 2024-02 | #42508 (Critical) | 旧公式严重低估可用内存 | 重写计算逻辑 |
| 2024-02 | #43071 | v1/v2 使用不同减除方式 | 统一为 `usage - inactive_file - active_file` |
| **2026-05** | **#63067** | **kmem 虚高导致误判（本 bug）** | **Resource Isolation: 读 `anon+shmem` 绕过** |

每次修复都是给 v1 打补丁，本质问题（`memory.usage_in_bytes` 不准确）无法根治。

#### 2. cgroup v2 的 memory.stat 提供精确分项数据

Ray master 分支的 Resource Isolation 方案利用了 cgroup v2 `memory.stat` 中的 `anon` 和 `shmem` 字段，
完全绕过了不可靠的 `memory.usage_in_bytes`/`memory.current` 顶层计数器。

#### 3. cgroup v2 支持子 cgroup 隔离

Resource Isolation 方案将容器内 cgroup 分为 system/user 两个 slice，
可以精确区分 Ray 系统进程和用户 worker 的内存使用。
这在 cgroup v2 中天然支持（delegated subtree），而 cgroup v1 中创建子 cgroup 有诸多限制。

#### 4. Kubernetes 生态已全面转向 cgroup v2

- Kubernetes 1.25+ GA 支持 cgroup v2
- 所有主流发行版新版本默认 cgroup v2（RHEL 9, Ubuntu 22.04+, Fedora 31+）
- 容器运行时（containerd, CRI-O）已完整支持
- Ray 跟随生态大方向，优先支持 cgroup v2

---

## Master 分支 Resource Isolation 方案源码

### 架构设计

master 分支的 Resource Isolation 系列（PR #62705, #63067 等）引入了全新的内存监控架构：

```
容器 cgroup v2 (根, 如 /sys/fs/cgroup)
│
└── ray-node_<node_id>/
    ├── system/                    ← raylet, dashboard_agent, log_monitor
    │   └── leaf/                  ← 实际进程 cgroup
    │       └── memory.stat        → anon, shmem
    └── user/                      ← 所有 worker 进程
        ├── workers/               ← ray worker 进程
        │   └── memory.stat        → anon, shmem
        └── non-ray/               ← 非 ray 进程
            └── memory.stat        → anon, shmem
```

### cgroup 管理器接口

文件：`src/ray/common/cgroup2/cgroup_manager_interface.h`（master 分支）

```cpp
/**
  Sets up resource isolation for a Ray node using cgroup2 using the following
  cgroup hierachy:

      base_cgroup_path (e.g. /sys/fs/cgroup)
            |
    ray-node_<node_id>
    |                 |
  system             user
    |               |    |
  leaf        workers  non-ray
*/
class CgroupManagerInterface {
 public:
  /// Moves the process into the workers leaf cgroup.
  virtual Status AddProcessToWorkersCgroup(const std::string &pid) = 0;

  /// Moves the process into the system leaf cgroup.
  virtual Status AddProcessToSystemCgroup(const std::string &pid) = 0;

  /// Cleans up the cgroup hierarchy.
  // ...
};
```

### cgroup 管理器实现

文件：`src/ray/common/cgroup2/cgroup_manager.cc`（master 分支）

```cpp
CgroupManager::CgroupManager(std::string base_cgroup,
                             const std::string &node_id,
                             std::unique_ptr<CgroupDriverInterface> cgroup_driver)
    : base_cgroup_(std::move(base_cgroup)), cgroup_driver_(std::move(cgroup_driver)) {
  node_cgroup_ = base_cgroup_ + "/" +
                 absl::StrFormat("%s_%s", kNodeCgroupName, node_id);
  system_cgroup_ = node_cgroup_ + "/" + kSystemCgroupName;
  system_leaf_cgroup_ = system_cgroup_ + "/" + kLeafCgroupName;
  user_cgroup_ = node_cgroup_ + "/" + kUserCgroupName;
  workers_cgroup_ = user_cgroup_ + "/" + kWorkersCgroupName;
  non_ray_cgroup_ = user_cgroup_ + "/" + kNonRayCgroupName;
}

StatusOr<std::unique_ptr<CgroupManager>> CgroupManager::Create(
    std::string base_cgroup,
    const std::string &node_id,
    const int64_t system_reserved_cpu_weight,
    const int64_t system_reserved_memory_bytes,
    std::unique_ptr<CgroupDriverInterface> cgroup_driver) {
  // 验证参数...
  RAY_RETURN_NOT_OK(cgroup_driver->CheckCgroupv2Enabled());  // ← 要求 cgroup v2
  RAY_RETURN_NOT_OK(cgroup_driver->CheckCgroup(base_cgroup));
  // 验证 memory controller 可用...
  // 创建 cgroup 层次结构...
}
```

### 新内存计算公式（Resource Isolation 14/n，PR #63067）

文件：`src/ray/common/memory_monitor_utils.cc`（master 分支，commit 409bc239f3）

```cpp
/// 新增的数据结构
struct CgroupMemorySnapshot {
  /// size of non-file-backed region mappings within the cgroup in bytes.
  /// This is an approximation of heap usage for the cgroup.
  int64_t anon_memory_bytes;

  /// size of shared memory mappings within the cgroup in bytes.
  int64_t shmem_memory_bytes;
};

/// 新增的路径常量
static constexpr char kCgroupsV2MemoryAnonKey[] = "anon";
static constexpr char kCgroupsV2MemoryShmemKey[] = "shmem";
static constexpr char kCgroupsV2MemoryHighPath[] = "memory.high";

/// 新的内存快照函数 - 完全绕过 memory.usage_in_bytes / memory.current
const StatusSetOr<MemoryUsageSnapshot, StatusT::NotFound>
MemoryMonitorUtils::TakeUserSliceMemoryUsageSnapshot(
    const std::string &user_cgroup_path,
    const std::string &system_cgroup_path,
    const std::string &proc_dir) {

  // 从 user cgroup 的 memory.stat 读取 anon 和 shmem
  StatusSetOr<CgroupMemorySnapshot, StatusT::NotFound> user_cgroup_memory_snapshot_or =
      TakeCgroupMemorySnapshot(user_cgroup_path);
  // 从 system cgroup 的 memory.stat 读取 anon 和 shmem
  StatusSetOr<CgroupMemorySnapshot, StatusT::NotFound> system_cgroup_memory_snapshot_or =
      TakeCgroupMemorySnapshot(system_cgroup_path);

  // 错误处理...

  CgroupMemorySnapshot user_cgroup_memory_snapshot =
      user_cgroup_memory_snapshot_or.value();
  CgroupMemorySnapshot system_cgroup_memory_snapshot =
      system_cgroup_memory_snapshot_or.value();

  // We approximate total user application memory usage with user slice anon bytes
  // for approximating heap usage and the sum of user and system cgroup shmem bytes
  // for approximating object store usage since shared memory accounting between
  // the system and user slice is in-determinant per:
  // https://docs.kernel.org/admin-guide/cgroup-v2.html#memory-ownership
  int64_t total_used_bytes = user_cgroup_memory_snapshot.anon_memory_bytes      // 用户堆内存
                           + user_cgroup_memory_snapshot.shmem_memory_bytes      // ObjStore(user)
                           + system_cgroup_memory_snapshot.shmem_memory_bytes;   // ObjStore(system)

  return MemoryUsageSnapshot{total_used_bytes, host_level_total_bytes};
}

/// 从单个 cgroup 的 memory.stat 中读取 anon 和 shmem
const StatusSetOr<CgroupMemorySnapshot, StatusT::NotFound>
MemoryMonitorUtils::TakeCgroupMemorySnapshot(const std::string &root_cgroup_path) {
  std::string v2_stat_path = root_cgroup_path + "/" + kCgroupsV2MemoryStatPath;
  std::ifstream v2_stat_f(v2_stat_path, std::ios::in | std::ios::binary);
  if (v2_stat_f) {
    CgroupMemorySnapshot snapshot;
    bool anon_found = false;
    bool shmem_found = false;
    std::string key;
    int64_t stat_value;
    while (v2_stat_f >> key >> stat_value) {
      if (key == kCgroupsV2MemoryAnonKey) {        // "anon"
        snapshot.anon_memory_bytes = stat_value;
        anon_found = true;
      } else if (key == kCgroupsV2MemoryShmemKey) {  // "shmem"
        snapshot.shmem_memory_bytes = stat_value;
        shmem_found = true;
      }
      if (anon_found && shmem_found) {
        break;
      }
    }
    if (!anon_found || !shmem_found) {
      return StatusT::NotFound("Failed to read memory stat for cgroup ...");
    }
    return snapshot;
  }
  return StatusT::NotFound("Failed to open memory stat file ...");
}
```

**关键改进**：

| 方面 | 旧公式（v2.55.1） | 新公式（Resource Isolation） |
|------|--------------------|------------------------------|
| 数据源 | `memory.usage_in_bytes`（容器根 cgroup） | `memory.stat` 中的 `anon` + `shmem`（子 cgroup） |
| kmem 影响 | 包含全部 kmem → 虚高 | 完全不包含 kmem → 准确 |
| file cache | 需要手动减除 `inactive_file + active_file` | 天然不包含（只读 anon 和 shmem） |
| cgroup 版本 | 同时支持 v1/v2（但 v1 有 kmem 问题） | 仅支持 cgroup v2 |
| Object Store | 通过 shmem 被 `memory.usage_in_bytes` 自动包含 | 显式读取 user + system 的 shmem |

---

## 相关 Issue 和社区修复历史

### 内核层面

| Issue | 时间 | 问题 | 状态 |
|-------|------|------|------|
| [kubernetes/kubernetes#61937](https://github.com/kubernetes/kubernetes/issues/61937) | 2018-03 | K8s 1.8+ 默认开启 kmem 记账，kernel 3.10 上 CSS ID 泄漏导致机器 hang | Closed |
| [opencontainers/runc#1725](https://github.com/opencontainers/runc/issues/1725) | 2018 | runc 无条件启用 kmem 的上游 issue | Closed |
| [Red Hat BZ#1507149](https://bugzilla.redhat.com/show_bug.cgi?id=1507149) | 2017-10 | RHEL 7 内核 kmem 泄漏 | Closed |
| Linux kernel 5.9 slab reparenting | 2020 | Roman Gushchin 重构 slab memcg 记账为 per-object 粒度 | Merged |

**Kubernetes #61937 核心内容**：

- **报告者**：wzhx78，从 K8s 1.6.4 升级到 1.9.0 后生产环境崩溃
- **根因**：runc 删除了 `if d.config.KernelMemory != 0` 条件检查，无条件开启 kmem 记账
- **影响**：kernel < 4.0 上 CSS ID 只增不减，超过 65535 后机器 hang
- **解决**：K8s PR #72114 添加 `nokmem` 编译选项；内核添加 `cgroup.memory=nokmem` 启动参数
- **后续**：kernel 5.9 的 slab reparenting 修复了 dying cgroup 问题，但长时间运行的容器仍有记账虚高

### cgroup v1 kmem 记账 Bug 的三个层次

| 层级 | 问题描述 | 影响内核 | 修复版本 |
|------|---------|---------|---------|
| CSS ID 泄漏 | cgroup 删除后 ID 不回收，65535 上限耗尽 | < 4.0 | kernel 4.0 |
| Page-level charge 不释放 | 对象已 free 但 page 上有其他对象，charge 不减 | < 5.9 | kernel 5.9 (部分) |
| Dying cgroup 累积 | 已删除 cgroup 的 page charge 不 reparent 给 parent | < 5.9 | kernel 5.9 |

**本节点（kernel 5.14）的情况**：第 1、3 层已修复，第 2 层仍存在（容器存活期间 kmem 只增不减）。

### cgroup v2 如何从根本上解决 kmem 问题

cgroup v2 引入了 `obj_cgroup`（对象级别的 cgroup 引用）机制（kernel 5.9, Roman Gushchin）：

```
cgroup v1 (page-level charge):
┌─── Slab Page ──────────────────────────────┐
│ [obj_A] [obj_B] [obj_A] [obj_C]           │
│ Page charge → cgroup A (首次分配者)        │
│ obj_B 释放 → 无法 uncharge (page 还活着)   │
│ obj_A 全部释放 → 仍无法 uncharge (B/C 在)  │
└────────────────────────────────────────────┘

cgroup v2 (per-object charge via obj_cgroup):
┌─── Slab Page ──────────────────────────────┐
│ [obj_A→cgA] [obj_B→cgB] [obj_C→cgC]      │
│ 每个对象独立持有 obj_cgroup 引用            │
│ obj_B 释放 → cgB 立即 uncharge 1 obj      │
│ obj_A 释放 → cgA 立即 uncharge 1 obj      │
│ 与 page 归属无关                            │
└────────────────────────────────────────────┘
```

### Ray 社区层面

| PR/Issue | 时间 | 内容 |
|----------|------|------|
| [ray#28074](https://github.com/ray-project/ray/issues/28074) | 2022-10 | `memory.usage_in_bytes` 包含 file cache 导致内存计算偏高 |
| [ray#29103](https://github.com/ray-project/ray/pull/29103) | 2022-10 | 修复：减去 `inactive_file` |
| [ray#29709](https://github.com/ray-project/ray/pull/29709) | 2022-10 | 添加 cgroup v2 支持 |
| [ray#35989](https://github.com/ray-project/ray/issues/35989) | 2023-06 | P0：大量磁盘 I/O 导致 cache 高 → 误判 OOM |
| [ray#42508](https://github.com/ray-project/ray/pull/42508) | 2024-02 | Critical Bug-fix：旧公式严重低估可用内存 |
| [ray#43071](https://github.com/ray-project/ray/pull/43071) | 2024-02 | 统一 v1/v2 公式为 `usage - inactive_file - active_file` |
| [ray#62705](https://github.com/ray-project/ray/pull/62705) | 2026-04 | Resource Isolation 1/n：统一配置 |
| [ray#63067](https://github.com/ray-project/ray/pull/63067) | 2026-05 | Resource Isolation 14/n：user slice 方案，读 `anon+shmem` 绕过 kmem |

**所有已有修复（v2.55.1 及之前）都只处理了 file cache 的减除，从未处理 kmem 问题。**
**只有 master 分支的 Resource Isolation 方案（仅 cgroup v2）从根本上解决了此问题。**

### PR #62705 详解：Resource Isolation 多监控器架构

**PR 信息**：
- **标题**：[Core] (Resource Isolation 13/n) Introduce Multi Memory Monitor Factory
- **作者**：Kunchen (David) Dai (@Kunchd)
- **创建**：2026-04-17 | **合并**：2026-04-29

#### Ray 的内存模型

PR 首先定义了 Ray 节点的三段内存模型：

```
┌──────────────────────────────────────────────────────────────┐
│                        节点总内存                              │
├────────────────┬───────────────────┬─────────────────────────┤
│ System Memory  │ Object Store      │ User Memory             │
│ (raylet 等)    │ (Plasma /dev/shm) │ (worker 堆内存)          │
│ 相对固定        │ 动态              │ 动态                     │
└────────────────┴───────────────────┴─────────────────────────┘
```

- **System Memory**: raylet、dashboard agent 等系统进程，相对固定
- **Object Store**: `ray.put()` 和函数返回值存储的共享内存（Plasma tmpfs）
- **User Memory**: 所有 worker 进程执行用户代码时的堆内存

#### 现有方案的缺陷

PR 指出了当前 `ThresholdMemoryMonitor`（即 v2.55.1 使用的方案）的不足：

| 问题 | 说明 |
|------|------|
| **轮询模型有盲区** | 每 250ms 轮询一次，内存突增（burst）可能在两次轮询之间发生，来不及反应 |
| **Kill 不够激进** | 每次只 kill 一个 worker，多个 worker 同时泄漏时来不及 |
| **无硬隔离** | 只是"检测→kill"，kill 生效前系统进程已被影响 |
| **系统进程无保护** | 无法保证 raylet 在内存紧张时仍能正常工作 |

"系统进程无保护"并非指创建顺序，而是**运行时的内存竞争**：

```
时刻 T0:    raylet 运行正常，使用 2 GB
时刻 T1:    7 个 QwenVL worker 各 10 GB，总计 70 GB
时刻 T2:    某 worker 处理大图片，内存突增 20 GB
时刻 T2+50ms: 节点只剩 1 GB 空闲
             raylet 尝试分配 gRPC 缓冲区 → malloc 极慢或失败
             raylet 无法响应 heartbeat → GCS 判定节点死亡
时刻 T2+250ms: Ray 轮询发现超阈值 → 但 raylet 已经 stall

问题核心: 250ms 轮询间隔无法捕获 50ms 内的内存突增
         "先创建"不提供任何运行时内存保护
```

#### 新方案：cgroup v2 双监控器

利用 cgroup v2 的 `memory.high` + `memory.low` 提供**内核级**保护：

```
容器总内存 (128 GB)
│
├── System Slice
│   ├── memory.low = 8 GB        ← 内核保证: 低于此值不被回收
│   └── 进程: raylet, dashboard_agent
│
└── User Slice
    ├── memory.high = 100 GB     ← 内核保证: 超过此值限速 + 事件通知
    └── 进程: 所有 worker
```

**memory.high 与 memory.low 的协同保护**：

| 属性 | memory.high（User Slice） | memory.low（System Slice） |
|------|--------------------------|---------------------------|
| 作用方向 | 限制上界：用户不能用太多 | 保护下界：系统至少保留这么多 |
| 触发行为 | 超过后内核**限速**分配 + 事件通知 | 低于此值内核**不回收**该 cgroup 的页面 |
| 强度 | 软限制（throttle，不是直接 kill） | 最佳努力（极端情况可能违反） |
| 解决问题 | 防止 user 主动抢占过多内存 | 防止 system 在内存压力下被动失去已有内存 |

**为什么两者都需要（互补关系）**：

```
只有 memory.high，没有 memory.low 的漏洞:

  容器 128 GB
  user memory.high = 100 GB
  user 使用 95 GB（未超限）
  system 使用 5 GB
  文件缓存 25 GB
  总计 125 GB / 128 GB → 内核触发 page reclaim

  问题: 内核可能回收 system slice 的页面！
    → raylet 代码页被换出 → 缺页中断 → I/O 延迟 → 性能下降
    → 即使 user 没超限，system 也可能受影响

有 memory.low = 8 GB:
  内核回收时优先从其他 cgroup 回收
  system slice 低于 8 GB 的页面受保护
  → raylet 运行正常
```

**memory.low 不是"一致对待"**——它的目的就是**打破默认的平等回收策略**，给系统进程优先级保护。没有 memory.low 时，内核确实一视同仁回收所有 cgroup 的页面；memory.low 引入了差异化，让内核优先保留系统进程的内存。

#### 双监控器设计

```
┌──────────────────────────────────────────────────────────────────┐
│                Multi Memory Monitor Factory                        │
├────────────────────────────────┬─────────────────────────────────┤
│  Event Memory Monitor          │  Threshold Memory Monitor        │
│  (cgroup memory.high 事件驱动) │  (轮询 anon+shmem)              │
├────────────────────────────────┼─────────────────────────────────┤
│ 触发条件:                       │ 触发条件:                        │
│ user cgroup 使用超过            │ user.anon + user.shmem +        │
│ memory.high 限制                │ system.shmem > threshold        │
├────────────────────────────────┼─────────────────────────────────┤
│ 覆盖场景:                       │ 覆盖场景:                        │
│ Object Store 内存在             │ Object Store 部分"逃逸"到        │
│ user cgroup 内                  │ system cgroup                   │
│                                │ (shared memory 归属不确定)        │
├────────────────────────────────┼─────────────────────────────────┤
│ 优势:                           │ 优势:                            │
│ 内核级事件，零延迟              │ 捕获 cgroup 间"逃逸"的内存       │
│ 不依赖轮询间隔                  │ 覆盖 memory.high 看不到的部分    │
└────────────────────────────────┴─────────────────────────────────┘
```

**为什么需要两个监控器（Object Store 归属不确定性）**：

cgroup v2 的 [Memory Ownership](https://docs.kernel.org/admin-guide/cgroup-v2.html#memory-ownership) 规则：
> 共享内存 charge 给第一个触发缺页的进程所在的 cgroup

```
Plasma Object Store (/dev/shm/plasma):
  - raylet (system cgroup) 创建文件并 mmap
  - worker (user cgroup) 也 mmap 同一文件
  - 第一个读/写某 page 的进程 → 该 page charge 到其 cgroup

结果: Object Store 物理页面可能分散在两个 cgroup:
  部分 page 由 raylet 先 touch → charge 到 system cgroup
  部分 page 由 worker 先 touch → charge 到 user cgroup
```

这导致 Event Monitor 可能看不到"逃逸"到 system cgroup 的 Object Store 内存：

```
user cgroup memory.high = 100 GB
user cgroup 实际:  anon=60GB + shmem(ObjStore部分)=2GB = 62GB ← 未超限，Event Monitor 不触发

但实际总内存:
  user.anon = 60 GB
  user.shmem = 2 GB (部分 Object Store)
  system.shmem = 28 GB (大部分 Object Store 逃逸到 system!)
  总计 = 90 GB → 可能已经危险了

Threshold Monitor 的轮询公式:
  total = user.anon + user.shmem + system.shmem
        = 60 + 2 + 28 = 90 GB
  → 超过阈值 → 触发 kill
```

**为什么公式是 `user.anon + user.shmem + system.shmem`（不含 system.anon）**：

| 项 | 含义 | 是否监控 | 原因 |
|----|------|---------|------|
| user.anon | worker 堆内存 | 是 | 用户代码的主要消耗 |
| user.shmem | user cgroup 中的 Object Store | 是 | 用户相关的共享内存 |
| system.shmem | 逃逸到 system 的 Object Store | 是 | 虽在 system cgroup 但本质是用户数据 |
| system.anon | raylet/agent 堆内存 | **否** | 系统进程自身开销，受 memory.low 保护 |

#### shmem 是否一定是 Object Store

**不是绝对的，但在 Ray 节点上几乎 100% 是。**

`shmem` 在 cgroup `memory.stat` 中定义为所有 tmpfs 后端的共享内存映射：

| shmem 来源 | 说明 | Ray 节点中占比 |
|-----------|------|---------------|
| `/dev/shm/plasma*` | Plasma Object Store | **主要来源 (>99%)** |
| Python `multiprocessing.shared_memory` | 进程间共享 | 极少 |
| PyTorch `torch.multiprocessing` 共享 Tensor | GPU/CPU 共享 | 看 workload |
| POSIX `shm_open()` | 通用共享内存 | 极少 |

PR #63067 的代码注释明确承认这是**近似**：

```cpp
// We approximate total user application memory usage with user slice anon bytes
// for approximating heap usage and the sum of user and system cgroup shmem bytes
// for approximating object store usage since shared memory accounting between
// the system and user slice is in-determinant per:
// https://docs.kernel.org/admin-guide/cgroup-v2.html#memory-ownership
```

近似的合理性：
- Ray 节点上 shmem 的绝对主力是 Plasma（本节点 shmem=4.3GB ≈ Plasma 4.3GB）
- 其他 shmem 来源通常 < 100 MB
- 用于 OOM 阈值判断时精度已足够
- 比 `memory.usage_in_bytes`（包含 54GB 虚高 kmem）准确得多

#### User/System Cgroup 如何区分

通过 `CgroupManager` 在进程启动时**主动移动 PID** 实现：

```cpp
// src/ray/common/cgroup2/cgroup_manager_interface.h
class CgroupManagerInterface {
 public:
  // Worker 进程启动时 → 移入 user/workers/cgroup.procs
  virtual Status AddProcessToWorkersCgroup(const std::string &pid) = 0;

  // Raylet/Agent 启动时 → 移入 system/leaf/cgroup.procs
  virtual Status AddProcessToSystemCgroup(const std::string &pid) = 0;
};
```

实际操作（内核接口）：

```bash
# raylet 启动时把自己移到 system cgroup
echo <raylet_pid> > /sys/fs/cgroup/ray-node_xxx/system/leaf/cgroup.procs

# worker 注册成功后移到 user cgroup
echo <worker_pid> > /sys/fs/cgroup/ray-node_xxx/user/workers/cgroup.procs
```

在 `node_manager.cc` 中：
- Worker 注册时: `cgroup_manager_->AddProcessToWorkersCgroup(pid)`
- Raylet 启动时: `cgroup_manager_->AddProcessToSystemCgroup(getpid())`

`memory.high` 写在 user cgroup 目录中，只限制该 cgroup 内的进程；
`memory.low` 写在 system cgroup 目录中，只保护该 cgroup 内的进程。

#### memory.high 为什么选择软限制而非硬限制

| 属性 | memory.high（PR 选择） | memory.max（未选择） |
|------|----------------------|---------------------|
| 超限行为 | 限速分配 + 事件通知 | 立即触发 OOM kill |
| 可恢复性 | 是（graceful kill） | 否（进程直接死） |
| 给 Ray 的时间 | 有（可以选择最优 worker kill） | 无（内核随机杀） |
| 适合场景 | Ray 自主做智能 kill | 只作为最后兜底 |

选 `memory.high` 是因为它给了 Ray 时间做**感知工作负载的智能 kill**（按执行时间排序，优先 kill 最近开始的 worker），而不是让内核随机杀。

#### 性能数据

PR 附带的实验结果（4 个模拟 + 3 个生产场景）：

| 指标 | 无 Resource Isolation | 有 Resource Isolation |
|------|----------------------|----------------------|
| 内核 OOM Kill | 频繁发生 | **完全消除** |
| 节点失败（node death） | 视频检测 workload 中频繁出现 | **完全消除** |
| 内存限速（throttle） | 无 | 有（但 worker 继续运行，非致命） |
| 系统进程可用性 | 可能被饿死 | 始终有保留内存 |

#### 与本问题的关系

```
我们的问题（cgroup v1 kmem 虚高）:
  memory.usage_in_bytes 包含 54 GB 虚假 kmem
  → 轮询时读到错误值 → 误判 OOM → 无辜 kill worker

Resource Isolation 方案如何一并解决:
  1. 切换 cgroup v2 → kmem 准确记账，不再虚高
  2. 使用 memory.high 事件驱动 → 不依赖轮询
  3. 读 anon+shmem 而非 memory.usage_in_bytes → 完全绕过 kmem
  4. cgroup 硬隔离 → 即使计算有误差，system 进程也不会被影响
```

注意：此 PR 并未提及 kmem 虚高问题，它的设计动机是解决"用户 workload 饿死系统进程"的问题。但其新架构恰好也解决了 kmem 问题——因为不再读取包含 kmem 的 `memory.usage_in_bytes`。

#### 相关 PR 链

| PR | 编号 | 内容 |
|----|------|------|
| Resource Isolation 1/n | #62705 早期 | 统一配置 |
| Resource Isolation 6/n | — | Worker killing policy 接口改造 |
| Resource Isolation 8/n | — | 基于时间的 kill 策略 |
| Resource Isolation 9/n | — | Pressure Memory Monitor |
| Resource Isolation 10/n | — | Event Memory Monitor |
| Resource Isolation 12/n | — | Group killing policy |
| **Resource Isolation 13/n** | **#62705** | **Multi Monitor Factory（本 PR，组装框架）** |
| Resource Isolation 14/n | #63067 | Threshold Monitor 读 anon+shmem |
| Resource Isolation 15/n | — | Wire in Event Monitor |

---

## 解决方案

### 方案 A：调高 Ray OOM 阈值（最快，无需重启）

```bash
# 在 ray start 之前设置
export RAY_memory_usage_threshold=0.98

# 或者完全禁用 OOM monitor（不推荐生产使用）
export RAY_memory_monitor_refresh_ms=0
```

适用场景：临时缓解，不需要修改平台配置。

### 方案 B：禁用 cgroup v1 kmem 记账（需修改宿主机）

在宿主机内核启动参数中添加：

```
cgroup.memory=nokmem
```

效果：`memory.kmem.usage_in_bytes` 固定为 0，不再计入 `memory.usage_in_bytes`。这是 K8s 社区最广泛使用的解决方案。

### 方案 C：切换到 cgroup v2（推荐，需平台支持）

**启用步骤**：

```bash
# 1. 修改宿主机内核启动参数
vi /etc/default/grub
# GRUB_CMDLINE_LINUX="systemd.unified_cgroup_hierarchy=1"

# 2. 更新 GRUB 并重启
grub2-mkconfig -o /boot/grub2/grub.cfg
reboot

# 3. 验证
stat -f /sys/fs/cgroup/    # 输出 Type: cgroup2fs 表示成功
mount | grep cgroup2       # 确认挂载
cat /sys/fs/cgroup/cgroup.controllers  # 确认 memory controller 可用
```

**前提条件**：
- kernel 5.2+（本节点 5.14 满足）
- systemd 232+
- 容器运行时支持 cgroup v2（containerd 1.4+, runc 1.0+）
- Kubernetes 1.25+

**cgroup v2 的优势**：
- per-object 粒度记账，对象释放立即 uncharge
- `memory.stat` 提供 `slab_reclaimable` / `slab_unreclaimable` 分别计量
- 不存在 "page 上有其他对象阻止 charge 释放" 的问题
- `memory.current` 准确反映实际使用
- 支持 `memory.high` 软限制（触发回收而非 OOM）
- 支持子 cgroup 委托（Ray Resource Isolation 需要）

**当前 Ray v2.55.1 对 cgroup v2 的支持**：
- 已支持基础公式：`memory.current - inactive_file - active_file`
- 切换到 cgroup v2 后，`memory.current` 不含虚高的 kmem，立即改善
- 但不含 Resource Isolation 的 `anon+shmem` 精确公式（需 2.56+）

### 方案 D：定期重启容器

重启容器可以重置 cgroup kmem 计数器。适合无法修改平台配置的场景。

### 方案 E：升级 Ray 版本（中长期）

等待 master 分支的 Resource Isolation 方案发布到稳定版（预计 2.56+），该方案直接读取 `memory.stat` 中的 `anon + shmem`，完全不依赖 `memory.usage_in_bytes`。

**Resource Isolation 方案的要求**：
- 必须使用 cgroup v2
- 需要容器拥有 cgroup 委托权限（创建子 cgroup）
- Ray 配置中启用 resource isolation 模式

### --memory 参数建议值

`--memory` 只是调度资源声明，不影响 OOM。合理设置为 cgroup 限制减去 Object Store 和系统开销：

```bash
# 如果 kmem 问题已解决（方案 B/C）
ray start --memory=85000000000  # 128GB - 30GB(ObjStore) - 13GB(系统)

# 如果 kmem 仍存在（当前状态）
ray start --memory=40000000000  # 留出足够余量给 kmem
```

---

## 完整排查命令速查

```bash
# === 1. 容器 cgroup 内存概况 ===
cat /sys/fs/cgroup/memory/memory.usage_in_bytes
cat /sys/fs/cgroup/memory/memory.limit_in_bytes
cat /sys/fs/cgroup/memory/memory.stat
cat /sys/fs/cgroup/memory/memory.kmem.usage_in_bytes
cat /sys/fs/cgroup/memory/memory.kmem.max_usage_in_bytes

# === 2. 进程级内存 ===
ps -eo pid,rss,vsz,comm --sort=-rss | head -30
ps -eo pid,rss,comm | grep 'ray::' | awk '{sum+=$2} END{print sum/1024/1024, "GB"}'

# === 3. 精确去重统计 ===
cat /proc/*/smaps_rollup 2>/dev/null | \
  grep -E '^(Rss|Pss|Shared|Private)' | \
  awk '{a[$1]+=$2} END{for(k in a) print k,a[k],"kB"}'

# === 4. Object Store / Plasma ===
du -sh /dev/shm
ls -la /proc/<raylet_pid>/fd/ | grep '/dev/shm'
cat /proc/<raylet_pid>/smaps | grep -A5 '/dev/shm/plasma'

# === 5. 验证 Ray OOM 公式 ===
python3 -c "
usage = int(open('/sys/fs/cgroup/memory/memory.usage_in_bytes').read())
stat = open('/sys/fs/cgroup/memory/memory.stat').read()
vals = dict(l.split() for l in stat.strip().split('\n') if len(l.split())==2)
inactive = int(vals.get('total_inactive_file', 0))
active = int(vals.get('total_active_file', 0))
limit = int(open('/sys/fs/cgroup/memory/memory.limit_in_bytes').read())
ray_used = usage - inactive - active
print(f'usage={usage/2**30:.2f}GB inactive_file={inactive/2**30:.2f}GB active_file={active/2**30:.2f}GB')
print(f'ray_used={ray_used/2**30:.2f}GB limit={limit/2**30:.2f}GB ratio={ray_used/limit:.4f}')
print(f'threshold(0.95)={limit*0.95/2**30:.2f}GB above={ray_used > limit*0.95}')
"

# === 6. kmem 分析 ===
cat /sys/fs/cgroup/memory/memory.kmem.usage_in_bytes
slabtop -o -s c | head -20
cat /proc/meminfo | grep -E 'Slab|SReclaimable|SUnreclaim'

# === 7. Page table（排除项）===
grep VmPTE /proc/*/status 2>/dev/null | sort -t: -k2 -nr | head -10

# === 8. cgroup 版本确认 ===
stat -f /sys/fs/cgroup/ | grep Type  # tmpfs=v1, cgroup2fs=v2
cat /proc/filesystems | grep cgroup
mount | grep cgroup

# === 9. cgroup v2 验证（切换后）===
cat /sys/fs/cgroup/memory.current            # 实际内存使用（准确值）
cat /sys/fs/cgroup/memory.max                # 内存限制
cat /sys/fs/cgroup/memory.stat | grep -E 'anon|shmem|slab|kernel'
cat /sys/fs/cgroup/cgroup.controllers        # 可用 controller
```

---

## Linux 进程内存模型详解

### 虚拟内存 vs 物理内存

每个 Linux 进程有独立的虚拟地址空间（通常 128 TB），但虚拟内存**不等于**物理内存消耗。一个虚拟页面可以处于以下状态：

```
┌─────────────────────────────────────────────────────────────────────────┐
│ 状态              │ 占物理RAM? │ RSS计入? │ Swap计入? │ VmSize计入? │
├───────────────────┼────────────┼──────────┼───────────┼─────────────┤
│ ① 已映射未访问    │ 否         │ 否       │ 否        │ 是          │
│ ② 驻留在 RAM     │ 是         │ 是       │ 否        │ 是          │
│ ③ 被换出到 Swap   │ 否         │ 否       │ 是        │ 是          │
│ ④ 文件页被回收    │ 否         │ 否       │ 否        │ 是          │
│ ⑤ 已释放(munmap)  │ 否         │ 否       │ 否        │ 否          │
└───────────────────┴────────────┴──────────┴───────────┴─────────────┘
```

各状态详解：

**① 已映射未访问（Mapped but never faulted）**：
- `mmap(NULL, 28GB, ...)` 只在内核中创建 VMA 记录，不分配物理帧
- 页表条目标记为 "not present"
- 示例：Plasma 映射 Size=28GB 但 Rss=0 的那段
- VmSize += 28 GB，物理消耗 = 0

**② 驻留在 RAM（Resident）**：
- 进程首次访问虚拟页面 → 缺页中断 → 内核分配物理帧 → 页表标记 "present"
- 计入 RSS 和 VmSize
- 这是进程正在使用的"活跃"内存

**③ 被换出到 Swap（Swapped out）**：
- 内存紧张时，内核将匿名页写入 swap 分区，释放物理帧
- 页表标记 "not present, in swap"
- 不计入 RSS（物理帧已释放），但计入 smaps 的 Swap 字段
- 下次访问时触发缺页，从 swap 读回

**④ 文件页被回收（File page reclaimed）**：
- 文件映射的页面长时间未访问，内核直接丢弃物理帧
- 无需写 swap（因为可以从原文件重新读取）
- 不计入 RSS，不计入 Swap
- 下次访问时缺页，从磁盘文件重新读入
- 示例：raylet 的 libc.so 代码页被回收

**⑤ 已释放（munmap/free）**：
- 虚拟映射本身被移除，VMA 从进程中删除
- 不计入任何统计

#### 本节点 raylet 的虚拟内存构成

```
/proc/99/status:
  VmSize:  84532000 kB (80.6 GB) ← 所有 VMA 虚拟大小之和
  VmRSS:    5980160 kB ( 5.7 GB) ← 当前驻留物理 RAM 的页面
  VmSwap:         0 kB ( 0   GB) ← 被换出的页面

构成分析:
  VmSize 80.6 GB = 3×28GB(Plasma映射) + ~0.9GB(堆) + ~0.1GB(代码/库) + ...
  VmRSS  5.7 GB  = 4.3GB(Plasma驻留) + 0.9GB(堆) + 0.1GB(代码) + ...
  差值   74.9 GB ← 已映射但未驻留的虚拟空间（不占物理内存）
```

关系图：

```
┌─── VmSize (虚拟内存 80.6 GB) ─────────────────────────────────────┐
│                                                                     │
│  ┌─── RSS (驻留物理内存 5.7 GB) ──┐  ┌── Swap (0 GB) ──┐         │
│  │  Private_Dirty: 0.9 GB (堆)   │  │ (本节点禁用)     │         │
│  │  Shared_Dirty:  4.7 GB(Plasma)│  │                   │         │
│  │  Private_Clean: 0.05 GB       │  │                   │         │
│  │  Shared_Clean:  0.05 GB       │  │                   │         │
│  └────────────────────────────────┘  └───────────────────┘         │
│                                                                     │
│  ┌── 未驻留 (74.9 GB，不占物理内存) ─────────────────────────┐     │
│  │  Plasma 映射未访问部分: ~74 GB (28×3 - 4.3 - 0.4)        │     │
│  │  代码页被回收: ~0.05 GB                                    │     │
│  │  其他未访问映射: ~0.85 GB                                  │     │
│  └────────────────────────────────────────────────────────────┘     │
└─────────────────────────────────────────────────────────────────────┘
```

**cgroup 记账只统计实际占用物理资源的部分**，不计入未驻留的虚拟空间。

### Swap 禁用说明

本节点 Swap 为 0 是因为 **Kubernetes 环境默认禁用 swap**：

```bash
# 验证方式
cat /proc/swaps                    # 为空 → 无 swap 设备
free -h | grep Swap                # Swap: 0B 0B 0B
cat /sys/fs/cgroup/memory/memory.memsw.limit_in_bytes
# 如果等于 memory.limit_in_bytes → swap 被禁止
```

Kubernetes 禁用 swap 的原因：

| K8s 版本 | swap 策略 |
|---------|-----------|
| < 1.22 | kubelet 默认 `--fail-swap-on=true`，节点有 swap 就拒绝启动 |
| 1.22-1.27 | swap 支持为 alpha，默认关闭 |
| 1.28+ | swap 支持 beta，仍需显式开启 |

- K8s 资源调度模型假设 Pod 的 memory request/limit 对应物理 RAM
- Swap 使内存用量不可预测，破坏 QoS 保证
- Swap I/O 导致延迟不可控

对我们场景的影响：
- 无 swap → Dirty 页面无法被换出 → 只能留在 RAM 中或被 OOM kill
- Plasma 的 Shared_Dirty 页面（4.3 GB）完全无法回收

### Dirty 与 Clean 页面

**Clean** = 页面内容与其后端存储（backing store）一致，可以直接丢弃。
**Dirty** = 页面内容被修改过，丢弃前必须先写回（或走 swap）。

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Page 状态矩阵                                    │
├───────────┬──────────────────────────────┬──────────────────────────────┤
│           │ Clean (未修改)                │ Dirty (已修改)                │
├───────────┼──────────────────────────────┼──────────────────────────────┤
│ Private   │ • 可执行代码页 (.text)        │ • 堆内存 (malloc/new)         │
│ (独占)    │ • 只读数据段 (.rodata)        │ • 栈内存                      │
│           │ • COW 页(fork后未写)          │ • COW 写入后的修改页           │
│           │                              │                              │
│           │ 回收: 直接丢弃, 从文件重读    │ 回收: 必须写入 swap            │
├───────────┼──────────────────────────────┼──────────────────────────────┤
│ Shared    │ • 共享库代码 (libc.so)        │ • Plasma 对象数据             │
│ (共享)    │ • mmap 文件 (只读访问)        │ • 进程间共享写入数据           │
│           │                              │ • tmpfs 文件内容              │
│           │ 回收: 直接丢弃, 从文件重读    │ 回收: 需 swap, 且影响所有共享者 │
└───────────┴──────────────────────────────┴──────────────────────────────┘
```

#### 各类型在本节点中的示例

**Private_Clean（独占且未修改）**：

```
raylet 加载 libc.so 代码段:
  mmap("/lib/libc.so", PROT_READ|PROT_EXEC, MAP_PRIVATE)
  → 页面从文件读入，未被修改 → Clean
  → MAP_PRIVATE 所以是 Private（即使多进程各自映射）

回收代价: 极低（丢弃后从 libc.so 文件重新读取即可）
```

**Private_Dirty（独占且已修改）**：

```
worker 分配堆内存:
  ptr = malloc(100 MB)
  memcpy(ptr, data, 100 MB)  → 写入 → 标记 Dirty

回收代价: 高
  有 swap: 写入 swap 后释放物理帧
  无 swap（本节点）: 无法回收，只能 OOM kill 进程释放
```

**Shared_Clean（共享且未修改）**：

```
多个 worker 共享 libc.so 代码:
  worker1 和 worker2 各自 mmap libc.so
  → 内核让两者的页表指向同一物理帧
  → 内容与磁盘文件一致 → Clean
  → 多进程共享同一物理帧 → Shared

回收代价: 极低（丢弃后任何进程需要时从文件重读）
```

**Shared_Dirty（共享且已修改）**：

```
Plasma Object Store (tmpfs):
  raylet:  mmap("/dev/shm/plasma", PROT_READ|PROT_WRITE, MAP_SHARED)
  worker1: mmap("/dev/shm/plasma", PROT_READ|PROT_WRITE, MAP_SHARED)

  raylet 写入对象: memcpy(plasma_addr + offset, obj_data, size)
  → 页面被写入 → Dirty
  → tmpfs 没有磁盘后端文件 → 不能"从文件重读"
  → 多进程共享 → Shared

回收代价: 最高
  - tmpfs 页面只存在于 RAM 中
  - 不能丢弃（没有磁盘副本）
  - 只能写入 swap（本节点无 swap → 完全无法回收）
```

#### 回收难度排序

```
回收难度（从低到高）:

Shared_Clean (共享库代码)
  → 直接丢弃，从 .so 文件重读
    ↓
Private_Clean (程序代码)
  → 直接丢弃，从可执行文件重读
    ↓
Private_Dirty (堆/栈)
  → 需写入 swap；无 swap 则不可回收
    ↓
Shared_Dirty - 文件映射 (MAP_SHARED + 普通文件)
  → 需 writeback 到磁盘文件
    ↓
Shared_Dirty - tmpfs (Plasma Object Store)  ← 我们的 4.3 GB
  → 无磁盘后端，只能走 swap
  → 无 swap → 完全不可回收，必须留在 RAM
```

#### 与 Resource Isolation 公式的对应关系

```
Resource Isolation 读 anon + shmem 的本质:
  anon  ≈ Private_Dirty (堆内存，不可回收)
  shmem ≈ Shared_Dirty on tmpfs (Plasma，不可回收)

两者都是"不可回收的硬占用"——正是 OOM 判断应该关注的部分。
而 Clean 页面（代码、共享库）可以随时回收，不应导致 OOM。
```

### /proc/PID/smaps 详解

`/proc/<PID>/smaps` 是 Linux procfs 虚拟文件系统中的文件，提供进程**每一段内存映射（VMA）**的详细统计。

- `/proc/` — Linux 虚拟文件系统，内核以文件形式暴露进程和系统信息
- `<PID>` — 进程 ID（如 99 = raylet）
- `smaps` — Show Memory Maps（详细内存映射）

#### 相关 procfs 文件对比

| 文件 | 内容 | 信息量 | 用途 |
|------|------|--------|------|
| `/proc/PID/maps` | 每段映射的地址和路径（简略） | 低 | 快速查看映射了哪些文件 |
| `/proc/PID/smaps` | 每段映射的详细内存统计 | 高 | 精确分析每段映射的物理内存 |
| `/proc/PID/smaps_rollup` | 所有映射的汇总统计 | 中 | 快速获取进程总内存（含PSS） |
| `/proc/PID/status` | 进程总体信息 | 中 | 粗略查看 VmRSS 等 |

#### smaps 输出格式

```bash
cat /proc/99/smaps | grep -A15 '/dev/shm/plasma'
```

输出：

```
7f0700000000-7f0E00000000 rw-s 00000000 00:01 12345  /dev/shm/plasmalVMTmq (deleted)
Size:          28835840 kB    ← VMA 虚拟地址范围 (28 GB)
KernelPageSize:        4 kB   ← 页面大小
MMUPageSize:           4 kB
Rss:             4505600 kB   ← 实际驻留的物理内存 (4.3 GB)
Pss:              563200 kB   ← 按共享比例分摊 (4.3GB / 8进程 = 0.54GB)
Shared_Clean:          0 kB   ← 共享且未修改
Shared_Dirty:    4505600 kB   ← 共享且已修改 (= Rss, 全部是写入的对象数据)
Private_Clean:         0 kB   ← 独占且未修改
Private_Dirty:         0 kB   ← 独占且已修改
Referenced:      4505600 kB   ← 最近被访问过的页面
Anonymous:             0 kB   ← 匿名页面（shared file mapping 不计入）
LazyFree:              0 kB
Swap:                  0 kB   ← 被换出的（本节点无 swap）
```

各字段含义：
- **Size**: VMA 的虚拟地址范围大小（不代表物理消耗）
- **Rss**: 该 VMA 中实际在物理 RAM 中的页面总量
- **Pss**: Rss 中共享页面按共享者数量均分后的值
- **Shared_Dirty**: 被多个进程映射且已被写入修改的页面
- **Private_Dirty**: 仅本进程独占且已被写入修改的页面
- **Referenced**: 最近访问过的页面（活跃度指标）
- **Swap**: 被换出到 swap 的页面大小

地址行格式：`起始地址-结束地址 权限 偏移 设备号 inode 路径`
- `rw-s`: r=可读, w=可写, -=不可执行, **s=shared**（p=private）
- `(deleted)`: 文件已删除但 fd 仍打开

#### 进程虚拟地址空间与 VMA 的关系

每个进程的虚拟地址空间由多个 VMA 组成，每个 VMA 是 smaps 中的一个条目：

```
进程 99 (raylet) 虚拟地址空间:

0x0000000000400000─0x0000000000800000  r-xp  /usr/local/bin/raylet  [VMA1: 代码段]
0x0000000000800000─0x0000000000900000  rw-p  /usr/local/bin/raylet  [VMA2: 数据段]
0x0000000000900000─0x0000000028000000  rw-p  [heap]                 [VMA3: 堆]
...
0x00007f0000000000─0x00007f0700000000  rw-s  /dev/shm/plasma       [VMA4: Plasma映射1]
0x00007f0700000000─0x00007f0E00000000  rw-s  /dev/shm/plasma       [VMA5: Plasma映射2]
0x00007f0E00000000─0x00007f1500000000  rw-s  /dev/shm/plasma       [VMA6: Plasma映射3]
...
0x00007f8000000000─0x00007f8000200000  r-xp  /lib/x86_64-linux-gnu/libc.so [VMA7: 共享库]
0x00007ffffe000000─0x00007fffffffffff  rw-p  [stack]                [VMA8: 栈]
```

Plasma 有 3 个映射的原因：Plasma Object Store 对同一文件做了多次 `mmap`：

```cpp
// Plasma 内部（简化）
fd = open("/dev/shm/plasmaXXXXXX", O_CREAT | O_RDWR);
ftruncate(fd, 30GB);
unlink("/dev/shm/plasmaXXXXXX");  // 删除文件名，fd 保持打开 → (deleted)

// 多次 mmap 同一 fd 的不同区域
region1 = mmap(NULL, 28GB, PROT_READ|PROT_WRITE, MAP_SHARED, fd, offset_0);
region2 = mmap(NULL, 28GB, PROT_READ|PROT_WRITE, MAP_SHARED, fd, offset_1);
region3 = mmap(NULL, 28GB, PROT_READ|PROT_WRITE, MAP_SHARED, fd, offset_2);
```

我们观察到的 3 个映射状态：

```
映射1: Size=28GB, Rss=0      ← 预留但从未访问（虚拟空间占了但物理内存为 0）
映射2: Size=28GB, Rss=4.3GB  ← 主要工作区，存储了对象
映射3: Size=28GB, Rss=0.4GB  ← 少量使用
```

#### 虚拟页面到物理帧的映射

```
VMA (虚拟空间 28GB):              物理内存 (RAM):

┌────────────────────────┐       ┌───────────────────┐
│ vpage 0: 已访问 ───────────────→│ 物理帧 #31240     │
│ vpage 1: 已访问 ───────────────→│ 物理帧 #31241     │
│ vpage 2: 未访问 (无帧)  │       │                   │
│ vpage 3: 未访问 (无帧)  │       │                   │
│ vpage 4: 已访问 ───────────────→│ 物理帧 #52001     │
│ ...                     │       │ ...               │
│ vpage N: 未访问 (无帧)  │       │                   │
└────────────────────────┘       └───────────────────┘

Size = VMA 虚拟范围 = 28 GB (包含所有 vpage, 不论是否有物理帧)
Rss  = 有物理帧的 vpage 总量 = 4.3 GB (只计已访问、驻留 RAM 的)
```

### Worker 之间的共享内存

#### 多进程共享同一 Plasma 物理页面

当多个 worker 和 raylet 映射同一个 Plasma 文件时，物理内存只有**一份**：

```
物理内存 (RAM) 中只存在一份:
┌─────────────────────────────────────────────────┐
│  Plasma 物理页面: 4.3 GB                         │
│  [page0] [page1] [page2] ... [page_N]           │
└────────────┬───────────┬───────────┬────────────┘
             │           │           │
             ▼           ▼           ▼
     ┌── raylet 页表 ──┐ ┌─ worker1 页表 ┐ ┌─ worker2 页表 ┐
     │ 映射全部页面    │ │ 映射部分页面  │ │ 映射部分页面  │
     │ Rss = 4.3 GB   │ │ Rss = 1.25 GB │ │ Rss = 1.25 GB │
     └────────────────┘ └────────────────┘ └────────────────┘

物理内存总消耗 = 4.3 GB（不是 4.3 + 7×1.25 = 13 GB）
```

Worker 访问 Object Store 的过程：

```
1. Worker1 调用 ray.get(obj_ref)
2. Raylet 告诉 worker1: 对象在 Plasma 文件 offset=X, size=Y
3. Worker1 访问 mmap 的虚拟地址 (base + offset)
4. 如果该页面已被 raylet 写入过（物理帧已存在）:
   → 内核直接将 worker1 的页表指向同一物理帧
   → 不分配新内存！零拷贝！
5. Worker2 也 ray.get(同一 obj_ref):
   → 同样指向相同物理帧
   → 仍然不分配新内存

这就是 Plasma Object Store 的核心设计优势: 共享内存零拷贝
```

#### RSS 重复计算问题

`ps` 显示的 RSS 对共享页面**重复计算**——每个映射该页面的进程都"声称"拥有它：

```
ps 视角（含重复）:
  raylet:   RSS = 0.9 GB(私有堆) + 4.3 GB(Plasma共享) = 5.2 GB
  worker1:  RSS = 9.8 GB(私有堆) + 1.25 GB(Plasma共享) = 11.0 GB
  worker2:  RSS = 9.5 GB(私有堆) + 1.25 GB(Plasma共享) = 10.8 GB
  ...
  worker7:  RSS = 8.0 GB(私有堆) + 1.25 GB(Plasma共享) = 9.2 GB
  ──────────────────────────────────────────────────────────────
  ps RSS 总和 = 77.9 GB  ← 含大量重复!

实际物理内存:
  各进程私有堆（不重复）≈ 62.5 GB
  Plasma 共享页面（一份）=  4.3 GB
  ──────────────────────────────────
  真实总计 ≈ 66.8 GB ≈ Pss 总和 66.9 GB  ✓
```

PSS（Proportional Set Size）的计算方式：

```
某 Plasma 页面被 raylet + 7 workers = 8 个进程共享:
  每个进程对该页面的 Pss 贡献 = 4096 bytes / 8 = 512 bytes

所以:
  raylet 的 Pss_Shmem = 4.3 GB / 8 ≈ 0.54 GB（而非 4.3 GB）
  各 worker 的 Pss_Shmem 类似按比例分摊
```

### smaps_rollup vs /proc/PID/status 对比

两者的 RSS 值相同（来源相同，都是遍历页表），但 smaps_rollup 提供更多维度信息：

#### /proc/PID/status 提供的内存字段

```
VmPeak:   85000000 kB   ← 虚拟内存历史峰值
VmSize:   84532000 kB   ← 当前虚拟内存大小
VmRSS:     5980160 kB   ← 总 RSS (= RssAnon + RssFile + RssShmem)
RssAnon:    921600 kB   ← 匿名页 RSS（堆、栈）
RssFile:    102400 kB   ← 文件页 RSS（代码段、共享库）
RssShmem:  4956160 kB   ← 共享内存 RSS（Plasma tmpfs）
VmPTE:       50000 kB   ← 页表占用空间
```

#### /proc/PID/smaps_rollup 提供的内存字段

```
Rss:       5980160 kB   ← 与 status 的 VmRSS 相同
Pss:       1250000 kB   ← 按比例分摊（status 没有!）
Pss_Anon:   921600 kB   ← 匿名页 PSS
Pss_File:    52000 kB   ← 文件页 PSS（共享库按进程数均分）
Pss_Shmem:  276400 kB   ← 共享内存 PSS（Plasma 按 8 进程均分）
Private_Clean:  50000 kB  ← 独占未修改（status 没有!）
Private_Dirty: 880000 kB  ← 独占已修改
Shared_Clean:  100000 kB  ← 共享未修改
Shared_Dirty: 4950160 kB  ← 共享已修改
Swap:            0 kB     ← 换出到 swap 的
```

#### 对比表

| 信息 | status | smaps_rollup | 说明 |
|------|--------|--------------|------|
| 总 RSS | VmRSS ✓ | Rss ✓ | **数值相同** |
| RSS 分类 | RssAnon/File/Shmem ✓ | — | status 独有的分类方式 |
| PSS | — | Pss ✓ | **smaps_rollup 独有**, 不重复计算 |
| PSS 分类 | — | Pss_Anon/File/Shmem ✓ | smaps_rollup 独有 |
| Private/Shared | — | Private_*/Shared_* ✓ | smaps_rollup 独有 |
| USS | — | 需计算: Private_Clean + Private_Dirty | smaps_rollup 可推导 |
| Swap | — | Swap ✓ | 换出到 swap 的量 |
| 虚拟内存 | VmSize ✓ | — | status 独有 |
| 页表 | VmPTE ✓ | — | status 独有 |

#### 各口径的含义与适用场景

| 口径 | 计算方式 | 含义 | 重复计算? | 适用场景 |
|------|---------|------|-----------|---------|
| **RSS** | 所有 VMA 的 Rss 之和 | 进程页表中有物理帧的总量 | **是** | 粗略查看 |
| **PSS** | 共享页按使用者数均分 | 真实的"公平分摊"内存 | **否** | 系统级内存审计 |
| **USS** | Private_Clean + Private_Dirty | 仅该进程独占的内存 | **否** | 判断"杀掉能释放多少" |

**Ray 选择使用 USS**（Private 内存）来报告每个进程的内存使用：

```cpp
// src/ray/common/memory_monitor.cc (GetLinuxProcessMemoryBytesFromSmap)
int64_t uss = 0;
while (std::getline(smap_ifs, line)) {
  std::istringstream iss(line);
  iss >> title >> value >> unit;
  // 只累加 Private_* 字段 → 计算 USS
  if (boost::starts_with(title, "Private_")) {
    uss += value * 1024;
  }
}
return uss;
```

原因：USS 代表"杀掉这个进程能释放的物理内存"——共享页面即使杀一个进程也不会释放（其他进程还在用）。

---

## 常用内存查看方法速查

### 进程级内存查看

```bash
# === 快速查看（适合排序、筛选）===

# 按 RSS 排序查看所有进程
ps -eo pid,rss,vsz,comm --sort=-rss | head -20
# 注意: RSS 含共享页面重复计算

# Ray 进程总 RSS
ps -eo pid,rss,comm | grep 'ray::' | awk '{sum+=$2} END{print sum/1024/1024, "GB"}'

# === 单进程详细查看 ===

# /proc/PID/status - 快速概览
cat /proc/<PID>/status | grep -E '^(VmSize|VmRSS|RssAnon|RssFile|RssShmem|VmSwap|VmPTE)'
# 输出:
#   VmSize:   总虚拟内存
#   VmRSS:    总 RSS（含共享）
#   RssAnon:  匿名页 RSS（堆/栈）
#   RssFile:  文件页 RSS（代码/库）
#   RssShmem: 共享内存 RSS（Plasma）
#   VmSwap:   换出到 swap 的量
#   VmPTE:    页表大小

# /proc/PID/smaps_rollup - 精确统计（含 PSS）
cat /proc/<PID>/smaps_rollup
# 输出: Rss, Pss, Pss_Anon, Pss_File, Pss_Shmem, Private_*, Shared_*, Swap

# /proc/PID/smaps - 每段映射的详细统计
cat /proc/<PID>/smaps | grep -A15 '/dev/shm/plasma'  # 查看 Plasma 映射
cat /proc/<PID>/smaps | grep -A15 '\[heap\]'         # 查看堆
cat /proc/<PID>/smaps | grep -A15 'libc'             # 查看共享库

# === 全进程精确统计（去重）===

# 所有进程 PSS 总和（不重复计算共享页面）
cat /proc/*/smaps_rollup 2>/dev/null | \
  grep -E '^(Rss|Pss|Pss_Anon|Pss_Shmem|Pss_File|Private|Shared)' | \
  awk '{a[$1]+=$2} END{for(k in a) printf "%s\t%.2f GB\n", k, a[k]/1024/1024}'
```

### 系统级/容器级内存查看

```bash
# === cgroup v1 (当前节点) ===

# 总使用（含 kmem，可能虚高）
cat /sys/fs/cgroup/memory/memory.usage_in_bytes | awk '{printf "%.2f GB\n", $1/1024/1024/1024}'

# 限制
cat /sys/fs/cgroup/memory/memory.limit_in_bytes | awk '{printf "%.2f GB\n", $1/1024/1024/1024}'

# 详细分项
cat /sys/fs/cgroup/memory/memory.stat | grep -E '^(rss|cache|shmem|mapped_file|swap) '
# rss:    匿名内存（进程堆/栈）
# cache:  文件页缓存（含 shmem）
# shmem:  共享内存（Plasma tmpfs 页面）
# swap:   换出量

# kmem（内核内存，cgroup v1 可能虚高）
cat /sys/fs/cgroup/memory/memory.kmem.usage_in_bytes | awk '{printf "%.2f GB\n", $1/1024/1024/1024}'

# === cgroup v2 (切换后) ===

# 总使用（准确）
cat /sys/fs/cgroup/memory.current | awk '{printf "%.2f GB\n", $1/1024/1024/1024}'

# 详细分项
cat /sys/fs/cgroup/memory.stat | grep -E '^(anon|file|shmem|kernel|slab|sock) '
# anon:             匿名内存
# file:             文件页缓存
# shmem:            共享内存
# kernel:           内核内存（准确值）
# slab_reclaimable: 可回收的 slab
# slab_unreclaimable: 不可回收的 slab

# === /proc/meminfo（宿主机级别，容器内可能不准）===

cat /proc/meminfo | grep -E '^(MemTotal|MemFree|MemAvailable|Buffers|Cached|Slab|S(Reclaimable|Unreclaim)|SwapTotal|SwapFree)'
```

### 内存用量验证公式

```bash
# === 验证 cgroup v1 usage 构成 ===
python3 -c "
import os
def read_int(path):
    return int(open(path).read().strip())

usage = read_int('/sys/fs/cgroup/memory/memory.usage_in_bytes')
kmem = read_int('/sys/fs/cgroup/memory/memory.kmem.usage_in_bytes')

stat = {}
for line in open('/sys/fs/cgroup/memory/memory.stat'):
    parts = line.split()
    if len(parts) == 2:
        stat[parts[0]] = int(parts[1])

rss = stat.get('rss', 0)
cache = stat.get('cache', 0)
shmem = stat.get('shmem', 0)

print(f'=== cgroup v1 内存构成 ===')
print(f'memory.usage_in_bytes:  {usage/2**30:.2f} GB')
print(f'├── 用户态 (rss+cache): {(rss+cache)/2**30:.2f} GB')
print(f'│   ├── rss (匿名):     {rss/2**30:.2f} GB')
print(f'│   └── cache:          {cache/2**30:.2f} GB')
print(f'│       └── shmem:      {shmem/2**30:.2f} GB (Plasma)')
print(f'└── 内核态 (kmem):      {kmem/2**30:.2f} GB')
print(f'')
print(f'验证: rss+cache+kmem = {(rss+cache+kmem)/2**30:.2f} GB ≈ usage {usage/2**30:.2f} GB')
print(f'差异: {abs(usage - rss - cache - kmem)/2**30:.4f} GB')
"

# === 验证进程 RSS vs cgroup 的关系 ===
python3 -c "
import subprocess, os

# 获取所有进程 PSS 总和（精确去重）
result = subprocess.run(
    ['sh', '-c', 'cat /proc/*/smaps_rollup 2>/dev/null'],
    capture_output=True, text=True
)
pss_total = 0
rss_total = 0
for line in result.stdout.split('\n'):
    parts = line.split()
    if len(parts) >= 2:
        if parts[0] == 'Pss:':
            pss_total += int(parts[1])
        elif parts[0] == 'Rss:':
            rss_total += int(parts[1])

print(f'所有进程 RSS 总和: {rss_total/1024/1024:.2f} GB (含共享重复)')
print(f'所有进程 PSS 总和: {pss_total/1024/1024:.2f} GB (去重)')
print(f'RSS - PSS = {(rss_total-pss_total)/1024/1024:.2f} GB (共享页面重复量)')
"
```

### 特定场景查看

```bash
# === Object Store / Plasma 相关 ===

# 查看 /dev/shm 使用
df -h /dev/shm
du -sh /dev/shm/*  2>/dev/null

# 查看 raylet 的 Plasma 映射详情
cat /proc/<raylet_pid>/smaps | grep -B1 -A15 '/dev/shm/plasma'

# 查看哪些进程映射了 Plasma
grep -l 'plasma' /proc/*/maps 2>/dev/null | head -10

# === 共享库内存 ===

# 查看 libc 等共享库的内存占用
cat /proc/<PID>/smaps | grep -A10 'libc.*\.so'

# === 堆内存 ===

# 查看进程堆大小
cat /proc/<PID>/smaps | grep -A10 '\[heap\]'

# 或通过 status 快速查看
grep -E 'VmData|VmStk' /proc/<PID>/status

# === Swap 相关 ===

# 确认 swap 状态
cat /proc/swaps                    # swap 设备列表
free -h | grep -i swap             # swap 使用量
cat /proc/<PID>/status | grep VmSwap  # 单进程 swap 使用

# === 页表开销 ===

# 大量 mmap 映射会增加页表开销
grep VmPTE /proc/*/status 2>/dev/null | sort -t: -k2 -nr | head -10
```

### 内存指标选择指南

根据不同目的选择合适的查看方式：

| 目的 | 推荐方式 | 原因 |
|------|---------|------|
| 快速排序找大进程 | `ps --sort=-rss` | 速度快，一条命令 |
| 判断系统真实占用 | smaps_rollup PSS 总和 | 不重复计算共享页 |
| 判断杀进程能释放多少 | smaps_rollup USS (Private_*) | 共享页面不随单进程释放 |
| 排查 Plasma 物理内存 | smaps grep plasma → 看 Rss | 精确到映射级别 |
| 排查 cgroup OOM | memory.stat + kmem | 对应 Ray 的计算公式 |
| 判断 cgroup v1 虚高 | usage - (rss+cache) = kmem | 差值即为虚高部分 |
| 排查内存泄漏 | 对比多个时间点的 smaps_rollup | 关注 Private_Dirty 增长 |

---

## 参考资料

- [kubernetes/kubernetes#61937](https://github.com/kubernetes/kubernetes/issues/61937) — cgroup v1 kmem 泄漏标杆 issue（117 条评论）
- [opencontainers/runc#1725](https://github.com/opencontainers/runc/issues/1725) — runc 无条件开启 kmem
- [ray-project/ray#35989](https://github.com/ray-project/ray/issues/35989) — Ray cache memory 误算 OOM (P0)
- [ray-project/ray#42508](https://github.com/ray-project/ray/pull/42508) — Critical Bug-fix: cgroup v1 内存计算
- [ray-project/ray#43071](https://github.com/ray-project/ray/pull/43071) — 统一 v1/v2 公式
- [ray-project/ray#63067](https://github.com/ray-project/ray/pull/63067) — Resource Isolation user slice 方案
- [Red Hat BZ#1507149](https://bugzilla.redhat.com/show_bug.cgi?id=1507149) — RHEL 7 kmem 泄漏
- [Linux kernel 5.9 slab reparenting](https://lwn.net/Articles/812835/) — obj_cgroup per-object 记账
- [kernel doc: cgroup-v1/memory.txt](https://www.kernel.org/doc/Documentation/cgroup-v1/memory.txt) — cgroup v1 内存记账说明
- [kernel doc: cgroup-v2.rst](https://docs.kernel.org/admin-guide/cgroup-v2.html) — cgroup v2 官方文档
- [kernel doc: cgroup-v2 memory ownership](https://docs.kernel.org/admin-guide/cgroup-v2.html#memory-ownership) — cgroup v2 内存归属规则
