# Cgroup Memory Limit 与 Dashboard 内存显示问题分析

## 1. 问题描述

当 cgroup memory limit 未生效时（如容器未设置 `resources.limits.memory`，cgroup v1 limit 值为 9.2EB，或 cgroup v2 `memory.max = "max"`），Ray Dashboard Cluster 页面的 Memory 列显示的是宿主机物理内存，而非容器实际可用的内存。

## 2. 问题根因

### 2.1 `get_system_memory()` 的逻辑

文件：`python/ray/_common/utils.py`

```python
def get_system_memory(
    memory_limit_filename="/sys/fs/cgroup/memory/memory.limit_in_bytes",
    memory_limit_filename_v2="/sys/fs/cgroup/memory.max",
):
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
                docker_limit = None  # "max" → None

    psutil_memory_in_bytes = psutil.virtual_memory().total

    if docker_limit is not None:
        return min(docker_limit, psutil_memory_in_bytes)

    return psutil_memory_in_bytes
```

**问题场景**：

| 场景 | cgroup v1 limit | cgroup v2 max | docker_limit | 返回值 |
|------|-----------------|---------------|--------------|--------|
| cgroup 生效 | 490GB | - | 490GB | min(490GB, 1007GB) = 490GB ✅ |
| cgroup v1 无效 | 9.2EB (2^63-1) | - | 9.2EB | min(9.2EB, 1007GB) = 1007GB ❌ |
| cgroup v2 无效 | - | "max" | None | 1007GB ❌ |

### 2.2 `_get_mem_usage()` 的逻辑

文件：`python/ray/dashboard/modules/reporter/reporter_agent.py`

```python
@staticmethod
def _get_mem_usage():
    total = get_system_memory()  # ← cgroup 无效时返回物理内存
    used = utils.get_used_memory()  # ← cgroup 存在时返回 cgroup usage（正确）
    available = total - used
    percent = round(used / total, 3) * 100
    return total, available, percent, used
```

**结果**：cgroup 无效时，total=1007GB（物理内存），used=cgroup usage（可能几十GB），Memory 利用率看起来极低。

### 2.3 `get_used_memory()` 的逻辑

文件：`python/ray/_private/utils.py`

```python
def get_used_memory():
    # cgroup v1
    if os.path.exists(memory_usage_filename_v1) and os.path.exists(memory_stat_filename_v1):
        docker_usage = get_cgroup_used_memory(...)
    # cgroup v2
    elif os.path.exists(memory_usage_filename_v2) and os.path.exists(memory_stat_filename_v2):
        docker_usage = get_cgroup_used_memory(...)

    if docker_usage is not None:
        return docker_usage  # cgroup usage（排除 cache）
    return psutil.virtual_memory().used  # 宿主机级别
```

`get_used_memory()` 已正确读取 cgroup usage，但 `get_system_memory()` 返回的 total 不匹配。

### 2.4 已有的 `is_cgroup_memory_limit_valid()` 修复

文件：`python/ray/_common/utils.py`

```python
def is_cgroup_memory_limit_valid(
    memory_limit_filename="/sys/fs/cgroup/memory/memory.limit_in_bytes",
    memory_limit_filename_v2="/sys/fs/cgroup/memory.max",
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
                return False  # "max" → 无效

    if docker_limit is None:
        return False

    psutil_memory_in_bytes = psutil.virtual_memory().total
    return docker_limit <= psutil_memory_in_bytes  # 9.2EB > 1007GB → False
```

此函数已在 `resource_and_label_spec.py` 中用于修正 Ray 资源计算，但 **dashboard reporter 的 `_get_mem_usage()` 未同步修复**。

### 2.5 C++ 端 `TakeSystemMemorySnapshot` 的已有修复

文件：`src/ray/common/memory_monitor_utils.cc`

```cpp
const SystemMemorySnapshot MemoryMonitorUtils::TakeSystemMemorySnapshot(
    const std::string root_cgroup_path, const std::string proc_dir,
    int64_t node_memory_limit_bytes) {
  auto [cgroup_used_bytes, cgroup_total_bytes] = GetCGroupMemoryBytes(root_cgroup_path);
  auto [system_used_bytes, system_total_bytes] = GetLinuxMemoryBytes(proc_dir);
  bool cgroup_limit_invalid =
      (cgroup_total_bytes == MemoryMonitorInterface::kNull) ||
      (system_total_bytes != MemoryMonitorInterface::kNull &&
       cgroup_total_bytes > system_total_bytes);
  if (cgroup_limit_invalid && cgroup_used_bytes != MemoryMonitorInterface::kNull) {
    int64_t total_to_use = (node_memory_limit_bytes > MemoryMonitorInterface::kNull)
                               ? node_memory_limit_bytes  // ← 使用 --memory 配置
                               : system_total_bytes;
    int64_t used_to_use = (cgroup_used_bytes > total_to_use) ? total_to_use
                                                              : cgroup_used_bytes;
    return SystemMemorySnapshot{used_to_use, total_to_use};
  }
  // cgroup 有效时取 min
  system_total_bytes = NullableMin(system_total_bytes, cgroup_total_bytes);
  ...
}
```

C++ 端在 cgroup 无效时使用 `node_memory_limit_bytes`（来自 `--memory` 参数），Python 端缺少这一逻辑。

## 3. 实际验证

### 3.1 机器1：cgroup 生效

```
hostname: aiplatform-bjy-ge58-5
cgroup v1 limit: 526133493760 (490GB)
physical mem:    1056147440 kB (1007GB)
/dev/shm total:  504GB (tmpfs, 物理50%)
→ is_cgroup_memory_limit_valid() = True ✅
→ get_system_memory() = 490GB ✅
```

### 3.2 机器2：cgroup 无效

```
hostname: klingai-wlf1-ge47-8
cgroup v1 limit: 9223372036854771712 (9.2EB)
physical mem:    1056130560 kB (1007GB)
/dev/shm total:  512GB (tmpfs, 物理50%)
→ is_cgroup_memory_limit_valid() = False ❌
→ get_system_memory() = 1007GB ❌ （应显示实际可用内存）
```

## 4. /dev/shm 分析

### 4.1 每个 Pod 的 /dev/shm 是独立的

```bash
# mount 信息
tmpfs on /dev/shm type tmpfs (rw,relatime,size=536870912k)  # 容器独立的 512GB tmpfs
tmpfs on /dev/shm/rpc-monitor type tmpfs (rw,nosuid,nodev)
tmpfs on /dev/shm/kess type tmpfs (rw,nosuid,nodev)
```

- **`hostIPC: false`（默认）**：每个 Pod 有独立的 `/dev/shm` tmpfs
- **`hostIPC: true`**：容器共享宿主机的 `/dev/shm`

### 4.2 /dev/shm 大小来源

Pod 中的 emptyDir 配置：

```yaml
volumes:
  - name: worker-storage-0
    emptyDir:
      medium: "Memory"
      sizeLimit: "512Gi"
```

- `medium: "Memory"` → tmpfs
- `sizeLimit: "512Gi"` → 上限 512GB
- 这个 volume mount 到 `/dev/shm`

**所以 Dashboard 的 Shared Memory 列显示的 total = 512Gi 是 Pod 的 emptyDir sizeLimit，不是物理内存。**

### 4.3 /dev/shm 与 cgroup 的关系

`/dev/shm` 的 tmpfs 内存占用**计入 cgroup memory**：

- cgroup 生效（490GB limit）时：/dev/shm 512Gi + 进程 RSS 共享 490GB，/dev/shm 实际可用远小于 512Gi
- cgroup 不生效（9.2EB limit）时：/dev/shm 受物理内存限制，sizeLimit 512Gi 也在 tmpfs 层面限制

### 4.4 Shared Memory 与 Object Store Memory 的关系

Dashboard 上的列：

| 列 | 含义 | 来源 |
|----|------|------|
| Memory | cgroup/节点总内存使用 | `_get_mem_usage()` → `get_system_memory()` + `get_used_memory()` |
| Shared Memory | `/dev/shm` 的物理页使用 | `_get_shm_usage()` → `os.statvfs("/dev/shm")` |
| Object Store Memory | Ray 逻辑资源分配 | `raylet.objectStoreUsedMemory` / `objectStoreAvailableMemory` |

关系：`Shared Memory >= Object Store Memory`

- Object Store Memory：Ray 逻辑分配量
- Shared Memory：`/dev/shm` 物理页使用量（包括 Ray mmap 文件 + 已删除但 OS 未回收的页 = high watermark）

## 5. Ray Object Store 内存限制机制

### 5.1 `object_store_memory` 配置的来源

文件：`python/ray/_private/resource_and_label_spec.py`

```python
def _resolve_memory_resources(self):
    system_memory = ray._common.utils.get_system_memory()
    cgroup_invalid = not ray._common.utils.is_cgroup_memory_limit_valid()

    if self.available_memory_bytes is None:
        if cgroup_invalid and self.memory is not None:
            self.available_memory_bytes = max(
                0, self.memory - ray._private.utils.get_used_memory()
            )
        else:
            self.available_memory_bytes = ray._private.utils.estimate_available_memory()

    if self.object_store_memory is None:
        self.object_store_memory = ray._private.utils.resolve_object_store_memory(
            self.available_memory_bytes
        )

    # cgroup 无效 + --memory 有值时，total_memory = --memory
    if cgroup_invalid and self.memory is not None:
        self.total_memory = self.memory
        memory = max(0, self.available_memory_bytes - self.object_store_memory)
    elif self.memory is not None:
        memory = self.memory
    else:
        memory = self.available_memory_bytes - self.object_store_memory
```

`object_store_memory` 默认 = `available_memory_bytes * 30%`。

`total_memory` 资源仅在 cgroup 无效 + `--memory` 有值时注册。

### 5.2 C++ 端接收 `total_memory`

文件：`src/ray/raylet/main.cc`

```cpp
auto total_mem_it = static_resource_conf.find("total_memory");
if (total_mem_it != static_resource_conf.end()) {
    node_manager_config.node_memory_limit_bytes =
        static_cast<int64_t>(total_mem_it->second);
}
```

`node_memory_limit_bytes` 传入 `MemoryMonitorFactory::Create()`，在 `TakeSystemMemorySnapshot` 中使用。

### 5.3 Plasma Store 启动：预分配 mmap

文件：`src/ray/object_manager/plasma/plasma_allocator.cc`

```cpp
PlasmaAllocator::PlasmaAllocator(
    const std::string &plasma_directory,
    const std::string &fallback_directory,
    bool hugepage_enabled,
    int64_t footprint_limit)  // = object_store_memory
    : kFootprintLimit(footprint_limit), ...

{
    // 一次性分配 kFootprintLimit 大小的 mmap
    auto allocation = Allocate(kFootprintLimit - kDlMallocReserved);
    RAY_CHECK(allocation.has_value())
        << "PlasmaAllocator initialization failed."
        << " It's likely we don't have enough space in " << plasma_directory;
    // 释放回 dlmalloc 池，但不 unmap 文件
    Free(std::move(allocation.value()));
}
```

`Allocate()` → `dlmemalign()` → dlmalloc 内部调用 `fake_mmap()` → `create_and_mmap_buffer()` 在 `/dev/shm` 创建 mmap 文件。

### 5.4 `fake_mmap()` 的拒绝机制

文件：`src/ray/object_manager/plasma/dlmalloc.cc`

```cpp
bool allocated_once = false;  // 全局标志
char *initial_region_ptr = nullptr;
size_t initial_region_size = 0;

void *fake_mmap(size_t size) {
    // 关键保护：初始分配后，拒绝普通 Allocate 的 mmap 请求
    if (dlmalloc_config.fallback_enabled && allocated_once && mparams.mmap_threshold > 0) {
        RAY_LOG(DEBUG) << "refusing to overcommit: " << size;
        return MFAIL;  // ← 拒绝！
    }

    // Add gap, create buffer, mmap
    size += kMmapRegionsGap;
    void *pointer;
    MEMFD_TYPE_NON_UNIQUE fd;
    create_and_mmap_buffer(size, &pointer, &fd);
    ...
    allocated_once = true;  // ← 标记已分配
    initial_region_ptr = static_cast<char *>(*pointer);
    initial_region_size = size;
    ...
}
```

**三个条件同时满足时拒绝超限 mmap**：
1. `fallback_enabled = true`（默认）
2. `allocated_once = true`（启动时已分配过初始池）
3. `mparams.mmap_threshold > 0`（普通 Allocate 模式）

返回 `MFAIL` → `dlmemalign` 返回 `nullptr` → `Allocate` 返回 `absl::nullopt` → `PlasmaError::OutOfMemory`。

**这是 Plasma Store 不会超过 `object_store_memory` 的核心保证机制。**

### 5.5 对象创建时的分配流程

```
CreateRequestQueue::ProcessRequests()
  → ProcessRequest(fallback_allocator=false)
    → create_callback_(fallback=false, &result)
      → PlasmaStore::HandleCreateObjectRequest()
        → ObjectLifecycleManager::CreateObject()
          → ObjectStore::CreateObject(fallback_allocate=false)
            → PlasmaAllocator::Allocate(object_size)
              → dlmemalign(kAlignment, bytes)
                → dlmalloc 从初始预分配池中分配
                  → 池中有空间 → 返回指针 ✅
                  → 池耗尽 → dlmalloc 尝试 mmap
                    → fake_mmap() → MFAIL（拒绝）
                    → dlmemalign 返回 nullptr
                    → Allocate 返回 absl::nullopt
                    → PlasmaError::OutOfMemory ❌
```

### 5.6 OOM 后的 Fallback 分配

文件：`src/ray/object_manager/plasma/create_request_queue.cc`

```cpp
Status CreateRequestQueue::ProcessRequests() {
    while (!queue_.empty()) {
        auto status = ProcessRequest(/*fallback_allocator=*/false, *request_it);

        if (status.ok()) {
            FinishRequest(request_it);
        } else {
            // 1. 触发 Python GC
            if (trigger_global_gc_) { trigger_global_gc_(); }

            // 2. 触发对象溢写到磁盘
            auto spill_pending = spill_objects_callback_();
            if (spill_pending) {
                return Status::TransientObjectStoreFull("Waiting for objects to spill.");
            }

            // 3. 等待 grace period
            if (now - oom_start_time_ns_ < grace_period_ns) {
                return Status::ObjectStoreFull("Waiting for grace period.");
            }

            // 4. Fallback 分配：绕过 fake_mmap 限制
            status = ProcessRequest(/*fallback_allocator=*/true, *request_it);
            FinishRequest(request_it);
        }
    }
}
```

Fallback 分配流程：

```
PlasmaAllocator::FallbackAllocate(object_size)
  → dlmallopt(M_MMAP_THRESHOLD, 0)  ← 关键：设置阈值为 0
  → dlmemalign(kAlignment, bytes)
    → dlmalloc 调用 fake_mmap(size)
      → 此时 mparams.mmap_threshold == 0 → 绕过拒绝逻辑
      → create_and_mmap_buffer()
        → allocated_once && fallback_enabled
        → file_template = fallback_directory (/tmp)  ← 不在 /dev/shm！
        → 在磁盘创建 mmap 文件
  → dlmallopt(M_MMAP_THRESHOLD, MAX_SIZE_T)  ← 恢复阈值
```

**Fallback 分配写的是磁盘 `/tmp`，不占 `/dev/shm` 空间。**

### 5.7 启动时 /dev/shm 空间不足的安全检查

文件：`src/ray/object_manager/plasma/store_runner.cc`

```cpp
// Linux 下检查 /dev/shm 可用空间
int shm_fd = open(plasma_directory.c_str(), O_RDONLY);
struct statvfs shm_vfs_stats;
fstatvfs(shm_fd, &shm_vfs_stats);
int64_t shm_mem_avail = shm_vfs_stats.f_bsize * shm_vfs_stats.f_bavail;
close(shm_fd);

if (shm_mem_avail < system_memory) {
    RAY_LOG(WARNING)
        << "The Plasma store only has " << shm_mem_avail << " bytes available...";
    system_memory = shm_mem_avail;  // ← 自动降级到 /dev/shm 可用空间
}
```

### 5.8 内存限制总结

| 层级 | 限制机制 | 限制值 |
|------|----------|--------|
| dlmalloc mmap | `fake_mmap()` 拒绝 `allocated_once` 后的普通 mmap | 初始池大小 = `object_store_memory` |
| dlmalloc 池 | 从预分配池中分配，池耗尽则 OOM | `object_store_memory` |
| Fallback | 超限后走磁盘 `/tmp`，不占 `/dev/shm` | 磁盘空间 |
| 启动检查 | `/dev/shm` 空间不足时自动降级 | `/dev/shm` 可用空间 |
| 资源调度 | `object_store_memory` 作为调度资源 | 防止过度调度 |

**结论：即使 `/dev/shm` 有 512Gi 可用，Plasma Object Store 也只会使用 `object_store_memory` 大小的 `/dev/shm` 空间，不会用超。**

## 6. 修复方案

### 6.1 核心思路

在 `scripts.py` 的 `ray start` 命令中，当 cgroup 无效 + `--memory` 有值时，将 `--memory` 值写入环境变量 `RAY_NODE_MEMORY_LIMIT_BYTES`。`_get_mem_usage()` 读取该环境变量作为 total。

### 6.2 修改：scripts.py

文件：`python/ray/scripts/scripts.py`

```python
cgroup_invalid = not ray._common.utils.is_cgroup_memory_limit_valid()
if cgroup_invalid and memory is not None:
    available_memory_bytes = max(
        0,
        memory - ray._private.utils.get_used_memory(),
    )
    os.environ["RAY_NODE_MEMORY_LIMIT_BYTES"] = str(memory)  # ← 新增
else:
    available_memory_bytes = ray._private.utils.estimate_available_memory()
```

**仅在 cgroup 无效 + `--memory` 有值时设置环境变量，其他情况走默认逻辑。**

### 6.3 修改：reporter_agent.py

文件：`python/ray/dashboard/modules/reporter/reporter_agent.py`

```python
@staticmethod
def _get_mem_usage():
    total = get_system_memory()
    node_memory_limit = os.environ.get("RAY_NODE_MEMORY_LIMIT_BYTES")
    if node_memory_limit is not None:
        try:
            total = int(node_memory_limit)
        except (ValueError, TypeError):
            pass
    used = utils.get_used_memory()
    if used > total:
        used = total
    available = total - used
    percent = round(used / total, 3) * 100 if total > 0 else 0.0
    return total, available, percent, used
```

**逻辑**：
1. 默认 `total = get_system_memory()`
2. 如果 `RAY_NODE_MEMORY_LIMIT_BYTES` 存在，覆盖 total
3. `used > total` 时 cap 住（防止配置异常时 available 为负）
4. `total > 0` 保护除零

### 6.4 测试

文件：`python/ray/_common/tests/test_utils.py`

```python
class TestGetMemUsageWithCgroupOverride:

    def test_with_env_override(self):
        """有 RAY_NODE_MEMORY_LIMIT_BYTES 时使用环境变量值"""
        with patch(
            "ray._private.utils.get_used_memory", return_value=100 * 1024**3
        ), patch.dict(os.environ, {"RAY_NODE_MEMORY_LIMIT_BYTES": "200000000000"}):
            from ray.dashboard.modules.reporter.reporter_agent import ReporterAgent
            total, available, percent, used = ReporterAgent._get_mem_usage()
            assert total == 200000000000
            assert used == 100 * 1024**3
            assert available == total - used

    def test_without_env_override_falls_back_to_system_memory(self):
        """无 RAY_NODE_MEMORY_LIMIT_BYTES 时回退到 get_system_memory()"""
        with patch(
            "ray._private.utils.get_used_memory", return_value=100 * 1024**3
        ), patch.dict(os.environ, {}, clear=True):
            from ray.dashboard.modules.reporter.reporter_agent import ReporterAgent
            total, available, percent, used = ReporterAgent._get_mem_usage()
            assert total == get_system_memory()

    def test_used_capped_at_total(self):
        """used 超过 total 时 cap 住"""
        with patch(
            "ray._private.utils.get_used_memory", return_value=999999999999
        ), patch.dict(os.environ, {"RAY_NODE_MEMORY_LIMIT_BYTES": "200000000000"}):
            from ray.dashboard.modules.reporter.reporter_agent import ReporterAgent
            total, available, percent, used = ReporterAgent._get_mem_usage()
            assert total == 200000000000
            assert used == total
            assert available == 0

    def test_invalid_env_value_ignored(self):
        """无效的环境变量值被忽略"""
        with patch(
            "ray._private.utils.get_used_memory", return_value=100 * 1024**3
        ), patch.dict(os.environ, {"RAY_NODE_MEMORY_LIMIT_BYTES": "not_a_number"}):
            from ray.dashboard.modules.reporter.reporter_agent import ReporterAgent
            total, available, percent, used = ReporterAgent._get_mem_usage()
            assert total == get_system_memory()
```

## 7. 修复前后对比

### 场景：cgroup 无效 + `--memory=490GB`

| 指标 | 修复前 | 修复后 |
|------|--------|--------|
| Memory total | 1007GB（物理内存）| 490GB（--memory 值）|
| Memory used | cgroup usage（正确）| cgroup usage（正确）|
| Memory 利用率 | 极低（误导）| 正确 |
| Shared Memory total | 512Gi（emptyDir sizeLimit）| 512Gi（不变）|
| Object Store Memory | object_store_memory | 不变 |
