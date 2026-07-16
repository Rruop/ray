# Ray Object Store 内存管理深度分析

本文档详细分析 Ray 集群中 Memory 和 Object Store Memory 的调度、计算、共享机制、OOM 监控、内存碎片及驱逐策略。

---

## 目录

1. [内存资源调度机制](#1-内存资源调度机制)
2. [集群 Total Usage 统计方式](#2-集群-total-usage-统计方式)
3. [Object Store Memory 自动计算](#3-object-store-memory-自动计算)
4. [/dev/shm 与 mmap 共享内存](#4-devshm-与-mmap-共享内存)
5. [缺页中断与 Page Cache 机制](#5-缺页中断与-page-cache-机制)
6. [多进程零拷贝共享](#6-多进程零拷贝共享)
7. [memory_usage_threshold OOM 监控](#7-memory_usage_threshold-oom-监控)
8. [内存碎片与统计精度](#8-内存碎片与统计精度)
9. [驱逐机制完整代码链路](#9-驱逐机制完整代码链路)
10. [运维诊断命令](#10-运维诊断命令)
11. [Ray Data Streaming Executor 中 Object Store 内存统计机制](#11-ray-data-streaming-executor-中-object-store-内存统计机制)

---

## 1. 内存资源调度机制

### 1.1 `--memory` 显式指定时的行为

当节点启动命令为：
```bash
ray start --address=10.15.3.158:6379 --block \
  --memory=120000000000 \
  --object-store-memory=30000000000 \
  --num-cpus=32 --num-gpus=0
```

**Ray 直接使用 `--memory` 指定的值作为调度资源，不会扣除 object store 或 raylet 本身的内存。**

核心代码 `python/ray/_private/resource_and_label_spec.py:374-399`：

```python
def _resolve_memory_resources(self):
    # Choose a default object store size.
    system_memory = ray._common.utils.get_system_memory()
    if self.available_memory_bytes is None:
        self.available_memory_bytes = ray._private.utils.estimate_available_memory()
    if self.object_store_memory is None:
        self.object_store_memory = ray._private.utils.resolve_object_store_memory(
            self.available_memory_bytes
        )

    memory = self.memory
    if memory is None:
        # 自动计算模式：扣除 object store
        memory = self.available_memory_bytes - self.object_store_memory
        if memory < 100e6 and memory < 0.05 * system_memory:
            raise ValueError(...)

    # 显式指定时，直接使用原值，不做任何扣减
    self.memory = memory
```

### 1.2 两种模式对比

| 场景 | memory 值 | 是否扣除 object_store |
|------|-----------|----------------------|
| `--memory` 未指定 | `available_memory - object_store_memory` | 是（自动扣除） |
| `--memory=120GB` 显式指定 | 120GB 原值 | 否 |

### 1.3 资源独立注册

`memory` 和 `object_store_memory` 在调度器中是两个完全独立的资源标签：

`src/ray/common/scheduling/scheduling_ids.h:40-41`：
```cpp
inline constexpr char kObjectStoreMemory_ResourceLabel[] = "object_store_memory";
inline constexpr char kMemory_ResourceLabel[] = "memory";
```

节点资源字典构建 `python/ray/_private/resource_and_label_spec.py:75-95`：
```python
def to_resource_dict(self):
    resources = dict(
        self.resources,
        CPU=self.num_cpus,
        GPU=self.num_gpus,
        memory=int(self.memory),                    # 调度资源
        object_store_memory=int(self.object_store_memory),  # 独立资源
    )
```

### 1.4 潜在超卖问题

以上述配置为例：
- `memory` 调度资源 = 120GB
- `object_store_memory` = 30GB
- 两者之和 = 150GB

如果机器物理内存只有 128GB，则 task 可以占用 120GB + object store 占用 30GB = 150GB > 128GB，产生资源超卖。

---

## 2. 集群 Total Usage 统计方式

Dashboard 中显示的 Total Usage 格式：
```
Total Usage:
50991.0/84826.0 CPU
900.0/902.0 GPU
185.20TiB/341.93TiB memory
408.14GiB/77.66TiB object_store_memory
```

### 2.1 各资源的统计方式

| 资源 | Total（分母） | Used（分子） | 统计机制 |
|------|-------------|-------------|---------|
| `CPU` | 所有节点 CPU 之和 | 已分配给 task/actor 的 CPU | 调度预留 |
| `GPU` | 所有节点 GPU 之和 | 已分配给 task/actor 的 GPU | 调度预留 |
| `memory` | 所有节点 `--memory` 之和 | 所有 task/actor **预留**的 memory 之和 | 调度预留（非实际 RSS） |
| `object_store_memory` | 所有节点 object store 容量之和 | 实际存储在 object store 中的字节数 | 实时轮询实际使用量 |

### 2.2 memory Used = 调度预留量

计算方式 `python/ray/autoscaler/v2/utils.py:846-865`：
```python
@classmethod
def _parse_node_resource_usage(cls, node_state, usage):
    d = defaultdict(lambda: [0.0, 0.0])
    for resource_name, resource_total in node_state.total_resources.items():
        d[resource_name][1] += resource_total
        d[resource_name][0] += resource_total  # 先设为 total

    for resource_name, resource_available in node_state.available_resources.items():
        d[resource_name][0] -= resource_available  # used = total - available
```

**当一个 task 请求 `memory=4GB` 被调度时，该节点 `available` 减少 4GB。即使 task 实际只用了 100MB RAM，报告仍然显示 4GB "used"。**

### 2.3 object_store_memory Used = 实际存储字节

`src/ray/raylet/scheduling/local_resource_manager.cc:321-335`：
```cpp
void LocalResourceManager::UpdateAvailableObjectStoreMemResource() {
    const double used = get_used_object_store_memory_();  // 实际字节数
    const double total = total_instances[0].Double();
    auto new_available = std::vector<FixedPoint>{
        FixedPoint(total >= used ? total - used : 0.0)
    };
    local_resources_.available.Set(
        ResourceID::ObjectStoreMemory(), std::move(new_available));
}
```

### 2.4 `341.93TiB memory` 不包含 `object_store_memory`

两者是完全独立的资源，分开统计，分开显示。

---

## 3. Object Store Memory 自动计算

### 3.1 未指定 `--object-store-memory` 时的计算公式

`python/ray/_private/utils.py:523-578`：

```python
def resolve_object_store_memory(
    available_memory_bytes: int,
    object_store_memory: Optional[int] = None,
) -> int:
    if object_store_memory is None:
        object_store_memory_cap = ray_constants.DEFAULT_OBJECT_STORE_MAX_MEMORY_BYTES

        # Linux: 受 /dev/shm 限制
        if sys.platform == "linux" or sys.platform == "linux2":
            shm_avail = get_shared_memory_bytes() * 0.95
            shm_cap = max(ray_constants.REQUIRE_SHM_SIZE_THRESHOLD, shm_avail)
            object_store_memory_cap = min(object_store_memory_cap, shm_cap)

        # 取可用内存的 30%
        object_store_memory = int(
            available_memory_bytes
            * ray_constants.DEFAULT_OBJECT_STORE_MEMORY_PROPORTION
        )

        # macOS 上限 2GB
        if sys.platform == "darwin":
            object_store_memory = min(
                object_store_memory, ray_constants.MAC_DEGRADED_PERF_MMAP_SIZE_LIMIT
            )

        # 不超过 cap
        if object_store_memory > object_store_memory_cap:
            object_store_memory = object_store_memory_cap

    return object_store_memory
```

### 3.2 相关常量

`python/ray/_private/ray_constants.py:120-141`：
```python
DEFAULT_OBJECT_STORE_MAX_MEMORY_BYTES = 200 * (10**9)   # 200 GB 硬上限
DEFAULT_OBJECT_STORE_MEMORY_PROPORTION = 0.3            # 30% 可用内存
OBJECT_STORE_MINIMUM_MEMORY_BYTES = 75 * 1024 * 1024   # 75 MB 最小值
REQUIRE_SHM_SIZE_THRESHOLD = 10**10                     # 10 GB /dev/shm 最低门槛
MAC_DEGRADED_PERF_MMAP_SIZE_LIMIT = 2 * (2**30)        # 2 GB macOS 上限
```

### 3.3 公式总结

```
object_store_memory = min(
    available_memory × 0.3,
    min(200GB, max(10GB, /dev/shm可用 × 0.95))
)
```

### 3.4 /dev/shm 可用空间获取

`python/ray/_private/utils.py:631-649`：
```python
def get_shared_memory_bytes():
    assert sys.platform == "linux" or sys.platform == "linux2"
    shm_fd = os.open("/dev/shm", os.O_RDONLY)
    try:
        shm_fs_stats = os.fstatvfs(shm_fd)
        shm_avail = shm_fs_stats.f_bsize * shm_fs_stats.f_bavail
    finally:
        os.close(shm_fd)
    return shm_avail
```

### 3.5 C++ 层的二次校验

`src/ray/object_manager/plasma/store_runner.cc:69-91`：
```cpp
if (!hugepages_enabled) {
    int shm_fd = open(plasma_directory.c_str(), O_RDONLY);
    struct statvfs shm_vfs_stats;
    fstatvfs(shm_fd, &shm_vfs_stats);
    int64_t shm_mem_avail = shm_vfs_stats.f_bsize * shm_vfs_stats.f_bavail;
    close(shm_fd);
    // Keep some safety margin for allocator fragmentation.
    shm_mem_avail = 9 * shm_mem_avail / 10;  // 90% 安全余量
    if (system_memory > shm_mem_avail) {
        RAY_LOG(WARNING) << "System memory request exceeds memory available in "
                         << plasma_directory;
        system_memory = shm_mem_avail;  // 强制 clamp
    }
}
```

### 3.6 实际案例分析

节点启动命令：
```bash
ray start --address=10.15.7.171:6379 --block --memory=80000000000 --num-cpus=12 --num-gpus=1
```

Dashboard 显示：
- Memory 最大 = 124.84GB
- Object Store Memory = 19GB

**解释：**
- **124.84GB**：这是 Dashboard 显示的**物理系统内存**（`psutil.virtual_memory().total` 或 cgroup limit），不是 `--memory` 调度资源
- **19GB**：`--object-store-memory` 未指定，自动计算受 `/dev/shm` 容量限制（容器 `/dev/shm` 约 20GB，`20GB × 0.95 = 19GB`）
- **80GB**：`--memory` 调度资源，在 Dashboard 的 "Logical Resources" 区域显示

Dashboard Memory 进度条的数据来源 `python/ray/dashboard/modules/reporter/reporter_agent.py:865-870`：
```python
@staticmethod
def _get_mem_usage():
    total = get_system_memory()      # psutil.virtual_memory().total
    used = utils.get_used_memory()
    available = total - used
    percent = round(used / total, 3) * 100
    return total, available, percent, used
```

---

## 4. /dev/shm 与 mmap 共享内存

### 4.1 什么是 tmpfs / /dev/shm

- **tmpfs** 是 Linux 内核的纯内存文件系统，数据只存在于 RAM（和 swap）中
- `/dev/shm` 是 tmpfs 的标准挂载点
- **Size 是配额不是预留**：不会预先占用物理内存，只在写入时才分配物理页

### 4.2 Object Store 的 mmap 过程

`src/ray/object_manager/plasma/dlmalloc.cc:140-239`：

```cpp
void create_and_mmap_buffer(int64_t size, void **pointer, int *fd) {
    // 1. 确定目录（首次用 /dev/shm，fallback 用 /tmp）
    std::string file_template = dlmalloc_config.directory;
    if (allocated_once && dlmalloc_config.fallback_enabled) {
        file_template = dlmalloc_config.fallback_directory;
    }
    file_template += "/plasmaXXXXXX";

    // 2. 创建临时文件
    *fd = mkostemp(&file_name[0], O_CLOEXEC);

    // 3. 立即 unlink（文件不可见，但 fd 有效）
    unlink(&file_name[0]);

    // 4. 设置文件大小
    ftruncate(*fd, (off_t)size);

    // 5. mmap 映射（默认不用 MAP_POPULATE）
    auto flags = MAP_SHARED;
    if (RayConfig::instance().preallocate_plasma_memory()) {
        flags |= MAP_POPULATE;  // 预填充物理页
    }
    *pointer = mmap(NULL, size, PROT_READ | PROT_WRITE, flags, *fd, 0);

    // 6. 记录初始区域地址
    if (!allocated_once) {
        initial_region_ptr = static_cast<char *>(*pointer);
        initial_region_size = size;
    }
}
```

目录选择 `src/ray/object_manager/plasma/store_runner.cc:56-65`：
```cpp
if (plasma_directory.empty()) {
#ifdef __linux__
    plasma_directory = "/dev/shm";      // Linux 默认
#else
    plasma_directory = "/tmp";          // macOS 默认
#endif
}
if (fallback_directory.empty()) {
    fallback_directory = "/tmp";        // Fallback 目录
}
```

### 4.3 启动时的"全量预留"实际做了什么

`src/ray/object_manager/plasma/plasma_allocator.cc:64-85`：

```cpp
PlasmaAllocator::PlasmaAllocator(const std::string &plasma_directory,
                                 const std::string &fallback_directory,
                                 bool hugepage_enabled,
                                 int64_t footprint_limit)
    : kFootprintLimit(footprint_limit),
      kAlignment(kAllocationAlignment),
      allocated_(0),
      fallback_allocated_(0) {
    internal::SetDLMallocConfig(plasma_directory, fallback_directory,
                                hugepage_enabled, /*fallback_enabled=*/true);

    // 分配全量空间 → 目的是让 dlmalloc 记住大小
    auto allocation = Allocate(kFootprintLimit - kDlMallocReserved);
    RAY_CHECK(allocation.has_value());

    // 立即释放 → 物理内存归还，虚拟地址释放
    // This will unmap the file, but the next one created will be as large
    // as this one (this is an implementation detail of dlmalloc).
    Free(std::move(allocation.value()));
}
```

**实际发生的事情：**
1. 创建 tmpfs 文件 → `ftruncate` 设大小 → **0 物理页**
2. `mmap` → **0 物理页**（仅虚拟地址预留）
3. dlmalloc 写几字节元数据 → **约 1 页 (4KB)**
4. `Free` → `munmap` + `close(fd)` → **文件消失，0 物理页**

**结论：默认模式下，启动时 Object Store 物理内存占用 ≈ 0。**

### 4.4 MAP_POPULATE 模式

`src/ray/common/ray_config_def.h:152-156`：
```cpp
/// Whether to re-populate plasma memory. This avoids memory allocation failures
/// at runtime (SIGBUS errors creating new objects), however it will use more memory
/// upfront and can slow down Ray startup.
RAY_CONFIG(bool, preallocate_plasma_memory, false)
```

设置 `RAY_preallocate_plasma_memory=1` 后，`mmap` 使用 `MAP_POPULATE` 标志：
- 立即分配所有物理页（约需 10-15 秒）
- 好处：后续不会 SIGBUS
- 坏处：立即占满配置的内存

### 4.5 三层内存模型

```
┌─────────────────────────────────────────────────────────────┐
│  虚拟地址空间 (Virtual Address Space)                         │
│  每个进程最大 128TB (x86-64)，只是数字，几乎无成本            │
├─────────────────────────────────────────────────────────────┤
│  页表 (Page Tables)                                          │
│  映射：虚拟页 → 物理帧 (4KB 粒度)                            │
│  条目可以是：Present(指向物理帧) 或 Not-Present(触发缺页)     │
├─────────────────────────────────────────────────────────────┤
│  物理 RAM (有限资源，所有进程共享)                             │
│  4KB 帧为单位，这才是真正"花钱"的东西                         │
└─────────────────────────────────────────────────────────────┘
```

- **物理帧 (Page Frame)**：RAM 芯片上 4KB 的实际存储单元，是操作系统管理内存的最小单位
- **虚拟地址**：每个进程独立，mmap 返回的地址在不同进程中不同
- **页表**：翻译虚拟地址到物理帧的映射表

---

## 5. 缺页中断与 Page Cache 机制

### 5.1 Page Cache 是什么

Page Cache 是内核维护的文件内容的内存缓存，本质是一个映射表：

```
(文件 inode, 偏移量) → 物理帧

例如：
(plasma文件, offset=0)      → 物理帧 #8201
(plasma文件, offset=4096)   → 物理帧 #8202
(plasma文件, offset=8192)   → 物理帧 #15003
```

- 普通磁盘文件：page cache 是"加速层"，可丢弃（磁盘有副本）
- **tmpfs 文件**：page cache **就是唯一存储**，不能丢弃（没有磁盘后备）

### 5.2 缺页中断完整过程（写入 10GB 数据）

假设 Worker 执行 `memcpy(plasma_ptr, data, 10GB)`，以写入第一个 4KB 为例：

**第 1 步：CPU 执行写指令**
```
MOV [0x7f0000000000], rax    (向 mmap 返回的虚拟地址写数据)
```

**第 2 步：MMU 查页表，PTE 不存在**
```
MMU 四级页表查询:
PGD[0x0FE] → PUD 表基址     ✓
PUD[0x000] → PMD 表基址     ✓
PMD[0x000] → PTE 表基址     ✓
PTE[0x000] → ???            ✗ Present 位 = 0
→ CPU 触发 #PF (Page Fault) 异常
```

**第 3 步：内核缺页处理程序**
```c
// arch/x86/mm/fault.c → do_page_fault()
// mm/memory.c → handle_mm_fault()
handle_mm_fault(vma, address, flags) {
    // vma->vm_file = /dev/shm/plasmaXXXXXX
}
```

**第 4 步：确定文件映射缺页**
```c
// mm/memory.c → handle_pte_fault()
if (pte 不存在 && vma->vm_ops->fault 存在) {
    → vma->vm_ops->fault(vmf)  // tmpfs: 调用 shmem_fault()
}
```

**第 5 步：tmpfs fault 处理（核心关联）**
```c
// mm/shmem.c → shmem_fault() → shmem_getpage_gfp()
shmem_getpage_gfp(inode, index, ...) {
    // inode = plasmaXXXXXX 文件的 inode
    // index = 文件内第几页 (offset / 4096)

    // ① 先在 page cache 中查找
    page = find_get_page(inode->i_mapping, index);

    if (page != NULL) {
        // Page Cache 命中（其他进程已写过）→ 直接共享物理帧
        goto done;
    }

    // ② Page Cache 未命中（第一次访问）
    page = alloc_page(GFP_HIGHUSER_MOVABLE);  // 从伙伴系统分配 4KB 物理帧
    clear_highpage(page);                      // 清零

    // ③ 关键：将物理帧加入 Page Cache
    add_to_page_cache(page, inode->i_mapping, index);

done:
    return page;
}
```

**第 6 步：安装页表映射**
```c
set_pte(pte_entry, mk_pte(page, vma->vm_page_prot));
// 虚拟地址 0x7f0000000000 → 物理帧 page
// 设置 Present=1, RW=1, Dirty=1, Shared=1
```

**第 7 步：返回用户态**
```
CPU 重新执行写指令 → MMU 翻译成功 → 数据写入物理帧
```

写 10GB = 重复约 **2,621,440 次**缺页（10GB / 4KB），内核有 fault-around 优化减少实际次数。

### 5.3 数据结构关系图

```
┌─────────────────────────────────────────────────────────────────┐
│                     内核数据结构                                  │
│                                                                  │
│  tmpfs 文件 inode (plasmaXXXXXX)                                │
│  └─► i_mapping (address_space)                                  │
│       └─► xarray (基数树):                                      │
│            [0] → struct page (帧#8201)                          │
│            [1] → struct page (帧#8202)                          │
│            [2] → struct page (帧#15003)                         │
│            [3] → NULL (未访问)                                   │
│                                                                  │
│  这个结构全局唯一，不属于任何进程                                   │
│  任何进程的缺页都查同一个 Page Cache                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Worker 1 页表:             Worker 2 页表:                       │
│  0x7f0000000 → 帧#8201     0x3a0000000 → 帧#8201               │
│  0x7f0001000 → 帧#8202     0x3a0001000 → 帧#8202               │
│                                                                  │
│  虚拟地址不同，指向同一物理帧                                      │
└─────────────────────────────────────────────────────────────────┘
```

---

## 6. 多进程零拷贝共享

### 6.1 为什么使用 mmap 共享内存

```
方案 A（不用共享内存）：每次复制
  Worker 1 产出 10GB → 复制到 Worker 2 (10GB) → 复制到 Worker 3 (10GB)
  → 3 进程共占 30GB 物理内存

方案 B（共享内存）：零拷贝
  Worker 1 写入 /dev/shm (10GB) → Worker 2 mmap 直接读 → Worker 3 mmap 直接读
  → 3 进程共占 10GB 物理内存（同一份）
```

### 6.2 fd 传递机制

每个进程有独立的 fd 表，但通过 **Unix Domain Socket + SCM_RIGHTS** 传递后都指向同一个 inode：

```
Raylet 进程:     fd=5 ──► file ──► inode (plasmaXXXXXX)
                                        │ 同一个 inode
Worker 1 进程:   fd=8 ──► file ──────────┘
Worker 2 进程:   fd=12 ─► file ──────────┘
```

fd 传递代码 `src/ray/object_manager/plasma/fling.cc`：
```c
// 发送端 (Raylet):
struct cmsghdr *cmsg;
cmsg->cmsg_type = SCM_RIGHTS;    // 传递文件描述符
*(int *)CMSG_DATA(cmsg) = fd;
sendmsg(socket, &msg, 0);

// 接收端 (Worker):
recvmsg(socket, &msg, 0);
int received_fd = *(int *)CMSG_DATA(cmsg);  // 新 fd 编号，同一文件
```

Worker 端 mmap `src/ray/object_manager/plasma/shared_memory.cc:30-56`：
```cpp
ClientMmapTableEntry::ClientMmapTableEntry(MEMFD_TYPE fd, int64_t map_size) {
    length_ = map_size - kMmapRegionsGap;
    pointer_ = mmap(NULL, length_, PROT_READ | PROT_WRITE, MAP_SHARED, fd.first, 0);
    close(fd.first);  // fd 可关闭，映射仍有效
}
```

### 6.3 第二个进程的缺页过程

```c
// Worker 2 读取数据，触发缺页
shmem_getpage_gfp(inode, index=0, ...) {
    page = find_get_page(inode->i_mapping, 0);
    // ✓ 命中！Worker 1 写入时已分配该物理帧
    // 不需要分配新物理帧，直接返回
    return page;
}
// 内核在 Worker 2 页表中安装：虚拟地址 → 同一物理帧
```

### 6.4 各组件的共享关系

| 组件 | 是否共享 | 说明 |
|------|---------|------|
| **Page Cache** | ✓ 共享 | 全局唯一，内核维护 |
| **fd** | ✗ 不共享 | 编号不同，但底层指向同一 inode |
| **虚拟地址** | ✗ 不共享 | 每进程独立地址空间 |
| **物理帧** | ✓ 共享 | 通过 Page Cache 间接共享 |

### 6.5 完整数据流（ray.put → ray.get）

```
1. Worker 1: ray.put(big_array)
   → Raylet 在 mmap 区域分配空间，返回 (fd, offset, size)

2. Worker 1: mmap(fd) → 虚拟地址 ptr
   memcpy(ptr, big_array, 10GB) → 缺页 → 分配物理帧 → 写入 Page Cache

3. Worker 1: seal object（标记不可变）

4. Worker 2: ray.get(object_ref)
   → Raylet 通过 Unix socket 发送 fd（SCM_RIGHTS）

5. Worker 2: mmap(fd) → 自己的虚拟地址 ptr2
   读取 ptr2[0..10GB] → 缺页 → Page Cache 命中 → 复用同一物理帧
   → 零拷贝，无新内存分配
```

---

## 7. memory_usage_threshold OOM 监控

### 7.1 配置

`src/ray/common/ray_config_def.h:70-74`：
```cpp
/// Threshold when the node is beyond the memory capacity.
/// Ranging from [0, 1]
RAY_CONFIG(float, memory_usage_threshold, 0.95)
```

### 7.2 监控的是系统总内存使用（包含 Object Store 和 Raylet）

`src/ray/common/memory_monitor.cc:97-112`：
```cpp
std::tuple<int64_t, int64_t> MemoryMonitor::GetMemoryBytes() {
    auto [cgroup_used_bytes, cgroup_total_bytes] = GetCGroupMemoryBytes();
    auto [system_used_bytes, system_total_bytes] = GetLinuxMemoryBytes();
    system_total_bytes = NullableMin(system_total_bytes, cgroup_total_bytes);
    if (system_total_bytes == cgroup_total_bytes) {
        system_used_bytes = cgroup_used_bytes;
    }
    return std::tuple(system_used_bytes, system_total_bytes);
}
```

### 7.3 Linux 内存使用计算

`src/ray/common/memory_monitor.cc:216-286`：
```cpp
std::tuple<int64_t, int64_t> MemoryMonitor::GetLinuxMemoryBytes() {
    // 读取 /proc/meminfo
    // MemTotal, MemAvailable, MemFree, Cached, Buffers
    ...
    int64_t available_bytes = mem_available_bytes;  // 优先用 MemAvailable
    int64_t used_bytes = mem_total_bytes - available_bytes;
    return {used_bytes, mem_total_bytes};
}
```

### 7.4 CGroup 内存使用计算

`src/ray/common/memory_monitor.cc:114-164`：
```cpp
int64_t MemoryMonitor::GetCGroupMemoryUsedBytes(...) {
    // CGroup 报告的内存包含文件页缓存，需要排除可回收部分
    // used = current_usage - inactive_file - active_file
    return current_usage_bytes - inactive_file_bytes - active_file_bytes;
}
```

### 7.5 阈值计算

`src/ray/common/memory_monitor.cc:344-361`：
```cpp
int64_t MemoryMonitor::GetMemoryThreshold(int64_t total_memory_bytes,
                                          float usage_threshold,
                                          int64_t min_memory_free_bytes) {
    int64_t threshold_fraction = (int64_t)(total_memory_bytes * usage_threshold);
    if (min_memory_free_bytes > kNull) {
        int64_t threshold_absolute = total_memory_bytes - min_memory_free_bytes;
        return std::max(threshold_fraction, threshold_absolute);
    } else {
        return threshold_fraction;
    }
}
```

### 7.6 Object Store 在 threshold 中的体现

**Object Store 内存是按需增长的（lazy allocation），不是一开始就全部占用。**

- 启动时：物理占用 ≈ 0
- 随着 `ray.put()` 写入数据 → 物理页逐步分配 → `/proc/meminfo` Shmem 增加
- `memory_usage_threshold` 检测的 `used_bytes` 包含了 Shmem（即 Object Store 实际使用的物理页）

| 时间 | Object Store 物理占用 | 系统 used_bytes 变化 |
|------|---------------------|---------------------|
| T0 启动 | ≈ 0 | 不变 |
| T1 ray.put(1GB) | +1GB | +1GB |
| T2 ray.put(5GB) | +5GB | +5GB |
| T3 object 被驱逐 | 可能不立即减少 | - |

### 7.7 总结表

| 问题 | 答案 |
|------|------|
| threshold 包含 Object Store？ | **是**，Object Store 使用的 `/dev/shm` 页面计入系统 used |
| 包含 Raylet 进程？ | **是**，测量的是系统总内存 |
| Object Store 是预占还是按需？ | **按需增长**（除非 `preallocate_plasma_memory=true`） |
| 超阈值后怎么办？ | 开始杀 worker 进程释放内存 |

---

## 8. 内存碎片与统计精度

### 8.1 对齐配置：64 字节

`src/ray/object_manager/plasma/plasma_allocator.cc:54`：
```cpp
const size_t kAllocationAlignment = 64;
```

`src/ray/object_manager/plasma/dlmalloc.cc:58-61`：
```cpp
// Copied from plasma_allocator.cc variable kAllocationAlignment,
// make sure to keep in sync. This for reduce memory fragmentation.
// See https://github.com/ray-project/ray/issues/21310 for details.
#define MALLOC_ALIGNMENT 64
```

### 8.2 实际分配公式

每个对象 dlmalloc 实际分配大小：
```
actual_size = max(64, ((requested_bytes + 8 + 63) & ~63))
                        ↑ chunk头 8字节     ↑ 对齐到64字节
```

### 8.3 统计用的是请求大小（非实际分配大小）

`src/ray/object_manager/plasma/plasma_allocator.cc:87-95`：
```cpp
std::optional<Allocation> PlasmaAllocator::Allocate(size_t bytes) {
    void *mem = dlmemalign(kAlignment, bytes);
    if (!mem) {
        return absl::nullopt;
    }
    allocated_ += bytes;  // ← 统计请求的 bytes，不是 dlmalloc 实际用的
    return BuildAllocation(mem, bytes, false);
}
```

### 8.4 统计差异对比

| 请求大小 | dlmalloc 实际分配 | Ray 统计 | 隐藏开销 |
|----------|------------------|----------|----------|
| 1 字节 | 64 字节 | 1 字节 | 6300% |
| 100 字节 | 128 字节 | 100 字节 | 28% |
| 1 KB | 1088 字节 | 1024 字节 | 6% |
| 1 MB | ~1MB + 64B | 1 MB | ≈0% |
| 1 GB | ~1GB + 64B | 1 GB | ≈0% |

### 8.5 碎片产生

代码注释明确说明会产生碎片 `src/ray/object_manager/plasma/obj_lifecycle_mgr.cc:185-189`：
```cpp
// NOTE(ekl) if we can't achieve this after a number of retries, it's
// because memory fragmentation in dlmalloc prevents us from allocating
// even if our footprint tracker here still says we have free space.
```

碎片示意：
```
分配后释放部分对象：
┌───┬░░░┬───┬░░░┬───┬░░░┬───┐
│ A │空 │ C │空 │ E │空 │ G │   空闲块被在用对象隔开
└───┴░░░┴───┴░░░┴───┴░░░┴───┘   无法合并为一个大连续块
```

### 8.6 三层内存粒度

| 层次 | 粒度 | 作用 |
|------|------|------|
| OS / Page Cache | 4KB 页 | 缺页分配物理帧、/proc/meminfo 统计 |
| dlmalloc | 64 字节对齐 | 在 4KB 页面上做细粒度内存管理 |
| Ray 统计 | 精确到请求字节 | `data_size + metadata_size`，不含开销 |

---

## 9. 驱逐机制完整代码链路

### 9.1 整体架构

```
PlasmaStore / CreateRequestQueue   (请求调度、spilling 协调)
        │
ObjectLifecycleManager             (生命周期状态机、驱逐重试循环)
        │
   ┌────┴────┐
   │         │
EvictionPolicy   ObjectStore       (LRU 驱逐决策 / 实际内存分配)
(LRUCache)       (IAllocator)
```

### 9.2 第 1 步：对象创建入口

`src/ray/object_manager/plasma/obj_lifecycle_mgr.cc:41-58`：
```cpp
std::pair<const LocalObject *, flatbuf::PlasmaError>
ObjectLifecycleManager::CreateObject(
    const ray::ObjectInfo &object_info,
    plasma::flatbuf::ObjectSource source,
    bool fallback_allocator) {
  RAY_LOG(DEBUG) << "attempting to create object " << object_info.object_id
                 << " size " << object_info.data_size;
  if (object_store_->GetObject(object_info.object_id) != nullptr) {
    return {nullptr, PlasmaError::ObjectExists};
  }
  auto entry = CreateObjectInternal(object_info, source, fallback_allocator);

  if (entry == nullptr) {
    return {nullptr, PlasmaError::OutOfMemory};
  }
  eviction_policy_->ObjectCreated(object_info.object_id);  // 加入 LRU 缓存
  stats_collector_->OnObjectCreated(*entry);
  return {entry, PlasmaError::OK};
}
```

### 9.3 第 2 步：重试 + 驱逐循环（核心）

`src/ray/object_manager/plasma/obj_lifecycle_mgr.cc:181-224`：
```cpp
const LocalObject *ObjectLifecycleManager::CreateObjectInternal(
    const ray::ObjectInfo &object_info,
    plasma::flatbuf::ObjectSource source,
    bool allow_fallback_allocation) {
  // Try to evict objects until there is enough space.
  // NOTE(ekl) if we can't achieve this after a number of retries, it's
  // because memory fragmentation in dlmalloc prevents us from allocating
  // even if our footprint tracker here still says we have free space.
  for (int num_tries = 0; num_tries <= 10; num_tries++) {
    auto result =
        object_store_->CreateObject(object_info, source, /*fallback_allocate*/ false);
    if (result != nullptr) {
      return result;  // 分配成功
    }
    // Tell the eviction policy how much space we need to create this object.
    std::vector<ObjectID> objects_to_evict;
    int64_t space_needed =
        eviction_policy_->RequireSpace(object_info.GetObjectSize(), objects_to_evict);
    EvictObjects(objects_to_evict);
    // More space is still needed.
    if (space_needed > 0) {
      RAY_LOG(DEBUG) << "attempt to allocate " << object_info.GetObjectSize()
                     << " failed, need " << space_needed;
      break;  // 没有更多可驱逐对象
    }
  }

  if (!allow_fallback_allocation) {
    RAY_LOG(DEBUG) << "Fallback allocation not enabled for this request.";
    return nullptr;
  }

  RAY_LOG(INFO)
      << "Shared memory store full, falling back to allocating from filesystem: "
      << object_info.GetObjectSize();

  auto result =
      object_store_->CreateObject(object_info, source, /*fallback_allocate*/ true);
  if (result == nullptr) {
    RAY_LOG(ERROR) << "Plasma fallback allocator failed, likely out of disk space.";
  }
  return result;
}
```

### 9.4 第 3 步：RequireSpace —— 决定驱逐量

`src/ray/object_manager/plasma/eviction_policy.cc:120-134`：
```cpp
int64_t EvictionPolicy::RequireSpace(int64_t size,
                                     std::vector<ObjectID> &objects_to_evict) {
  // 计算差额
  int64_t required_space =
      allocator_.Allocated() + size - allocator_.GetFootprintLimit();
  // 贪心策略：至少释放 20% 总容量
  int64_t space_to_free =
      std::max(required_space, allocator_.GetFootprintLimit() / 5);
  // 从 LRU 缓存选择对象
  int64_t num_bytes_evicted =
      ChooseObjectsToEvict(space_to_free, objects_to_evict);
  RAY_LOG(DEBUG) << "evicting " << objects_to_evict.size()
                 << " objects to free up " << num_bytes_evicted << " bytes.";
  // 返回值 > 0: 仍不够; <= 0: 理论上够了
  return required_space - num_bytes_evicted;
}
```

### 9.5 第 4 步：LRU 选择算法

`src/ray/object_manager/plasma/eviction_policy.cc:82-94`：
```cpp
int64_t LRUCache::ChooseObjectsToEvict(int64_t num_bytes_required,
                                       std::vector<ObjectID> &objects_to_evict) {
  int64_t bytes_evicted = 0;
  auto it = item_list_.end();  // 从尾部（最旧）开始
  while (bytes_evicted < num_bytes_required && it != item_list_.begin()) {
    it--;
    objects_to_evict.push_back(it->first);    // ObjectID
    bytes_evicted += it->second;              // 对象大小
    bytes_evicted_total_ += it->second;
    num_evictions_total_ += 1;
  }
  return bytes_evicted;
}
```

LRU 数据结构：
```cpp
// eviction_policy.h
typedef std::list<std::pair<ObjectID, int64_t>> ItemList;
ItemList item_list_;  // 双向链表，FRONT=最新，BACK=最旧
absl::flat_hash_map<ObjectID, ItemList::iterator> item_map_;  // O(1) 查找
```

```
LRU 链表结构：
  FRONT (最新)                              BACK (最旧)
  ┌─────┐   ┌─────┐   ┌─────┐   ┌─────┐
  │ obj5│◄─►│ obj4│◄─►│ obj3│◄─►│ obj1│
  │ 2MB │   │ 5MB │   │ 1MB │   │ 4MB │
  └─────┘   └─────┘   └─────┘   └─────┘
     ↑ 新对象加这里                    ↑ 驱逐从这里开始
```

新对象加入 `src/ray/object_manager/plasma/eviction_policy.cc:29-36`：
```cpp
void LRUCache::Add(const ObjectID &key, int64_t size) {
  auto it = item_map_.find(key);
  RAY_CHECK(it == item_map_.end());
  item_list_.emplace_front(key, size);       // 加到 FRONT
  item_map_.emplace(key, item_list_.begin());
  used_capacity_ += size;
}
```

### 9.6 第 5 步：执行驱逐

`src/ray/object_manager/plasma/obj_lifecycle_mgr.cc:226-241`：
```cpp
void ObjectLifecycleManager::EvictObjects(
    const std::vector<ObjectID> &object_ids) {
  for (const auto &object_id : object_ids) {
    RAY_LOG(DEBUG) << "evicting object " << object_id.Hex();
    auto entry = object_store_->GetObject(object_id);
    // 三个硬性前提（不满足直接 crash）
    RAY_CHECK(entry != nullptr)
        << "To evict an object it must be in the object table.";
    RAY_CHECK(entry->state_ == ObjectState::PLASMA_SEALED)
        << "To evict an object it must have been sealed.";
    RAY_CHECK(entry->ref_count_ == 0)
        << "To evict an object, there must be no clients currently using it.";

    DeleteObjectInternal(object_id);
  }
}
```

### 9.7 第 6 步：实际删除与内存释放

`src/ray/object_manager/plasma/obj_lifecycle_mgr.cc:243-258`：
```cpp
void ObjectLifecycleManager::DeleteObjectInternal(const ObjectID &object_id) {
  auto entry = object_store_->GetObject(object_id);
  RAY_CHECK(entry != nullptr);
  bool aborted = entry->state_ == ObjectState::PLASMA_CREATED;

  stats_collector_->OnObjectDeleting(*entry);
  earger_deletion_objects_.erase(object_id);
  eviction_policy_->RemoveObject(object_id);   // 从 LRU 移除
  object_store_->DeleteObject(object_id);       // 释放内存

  if (!aborted) {
    delete_object_callback_(object_id);         // 通知上层
  }
}
```

`src/ray/object_manager/plasma/object_store.cc:81-89`：
```cpp
bool ObjectStore::DeleteObject(const ObjectID &object_id) {
  auto entry = GetMutableObject(object_id);
  if (entry == nullptr) {
    return false;
  }
  allocator_.Free(std::move(entry->allocation_));  // 实际内存释放
  object_table_.erase(object_id);
  return true;
}
```

`src/ray/object_manager/plasma/plasma_allocator.cc:122-130`：
```cpp
void PlasmaAllocator::Free(Allocation allocation) {
  RAY_CHECK(allocation.address_ != nullptr) << "Cannot free the nullptr";
  dlfree(allocation.address_);        // dlmalloc 释放，内存立即可复用
  allocated_ -= allocation.size_;     // 更新计数
  if (internal::IsOutsideInitialAllocation(allocation.address_)) {
    fallback_allocated_ -= allocation.size_;
  }
}
```

### 9.8 对象可驱逐性：Pin / Unpin 机制

**AddReference（Pin：从 LRU 移除）**

`src/ray/object_manager/plasma/obj_lifecycle_mgr.cc:128-146`：
```cpp
bool ObjectLifecycleManager::AddReference(const ObjectID &object_id) {
  auto entry = object_store_->GetObject(object_id);
  if (!entry) {
    return false;
  }
  if (entry->ref_count_ == 0) {
    // ref_count: 0 → 1，从 LRU 移走
    eviction_policy_->BeginObjectAccess(object_id);
  }
  entry->ref_count_++;
  stats_collector_->OnObjectRefIncreased(*entry);
  return true;
}
```

`src/ray/object_manager/plasma/eviction_policy.cc:136-140`：
```cpp
void EvictionPolicy::BeginObjectAccess(const ObjectID &object_id) {
  cache_.Remove(object_id);                          // 从 LRU 缓存移除
  pinned_memory_bytes_ += GetObjectSize(object_id);  // 记为 pinned
}
```

**RemoveReference（Unpin：重新加入 LRU）**

`src/ray/object_manager/plasma/obj_lifecycle_mgr.cc:148-175`：
```cpp
bool ObjectLifecycleManager::RemoveReference(const ObjectID &object_id) {
  auto entry = object_store_->GetObject(object_id);
  if (!entry || entry->ref_count_ == 0) {
    return false;
  }
  entry->ref_count_--;
  stats_collector_->OnObjectRefDecreased(*entry);

  if (entry->ref_count_ > 0) {
    return true;
  }

  // ref_count 变为 0，重新放回 LRU
  eviction_policy_->EndObjectAccess(object_id);

  RAY_CHECK(entry->Sealed())
      << object_id << " is not sealed while ref count becomes 0.";
  if (earger_deletion_objects_.count(object_id) > 0) {
    DeleteObjectInternal(object_id);  // eager deletion
  }
  return true;
}
```

`src/ray/object_manager/plasma/eviction_policy.cc:142-147`：
```cpp
void EvictionPolicy::EndObjectAccess(const ObjectID &object_id) {
  auto size = GetObjectSize(object_id);
  cache_.Add(object_id, size);        // 加到 LRU FRONT（最近使用）
  pinned_memory_bytes_ -= size;
}
```

### 9.9 对象生命周期状态机

```
ray.put(data)
     │
     ▼
PLASMA_CREATED (正在写入, 不可驱逐)
     │  seal
     ▼
PLASMA_SEALED + ref_count=1 (被创建者 pin)
     │                         ↕ 可 spill 到磁盘
     │  创建者释放引用
     ▼
PLASMA_SEALED + ref_count=0  ← 在 LRU 缓存中, 可驱逐
     │                  ↑
     │ ray.get()        │ ray.get() 结束
     ▼                  │
ref_count≥1 (pinned, 从 LRU 移除, 不可驱逐)
```

### 9.10 上层 OOM 处理：CreateRequestQueue

`src/ray/object_manager/plasma/create_request_queue.cc:85-151`：
```cpp
Status CreateRequestQueue::ProcessRequests() {
  bool logged_oom = false;
  while (!queue_.empty()) {
    auto request_it = queue_.begin();
    auto status = ProcessRequest(/*fallback_allocator=*/false, *request_it);

    // 磁盘已满直接报错
    if ((*request_it)->error_ == PlasmaError::OutOfMemory &&
        fs_monitor_.OverCapacity()) {
      (*request_it)->error_ = PlasmaError::OutOfDisk;
      FinishRequest(request_it);
      return Status::OutOfDisk("System running out of disk.");
    }

    auto now = get_time_();
    if (status.ok()) {
      FinishRequest(request_it);
      oom_start_time_ns_ = -1;
    } else {
      // ① 触发全局 GC
      if (trigger_global_gc_) {
        trigger_global_gc_();
      }

      if (oom_start_time_ns_ == -1) {
        oom_start_time_ns_ = now;
      }

      // ② 请求 spill（把对象写到磁盘）
      auto spill_pending = spill_objects_callback_();
      if (spill_pending) {
        oom_start_time_ns_ = -1;
        return Status::TransientObjectStoreFull("Waiting for objects to spill.");
      }

      // ③ 等待 grace period（默认 2 秒）
      if (now - oom_start_time_ns_ < grace_period_ns) {
        return Status::ObjectStoreFull("Waiting for grace period.");
      }

      // ④ Fallback 到磁盘分配
      status = ProcessRequest(/*fallback_allocator=*/true, *request_it);
      if (!status.ok()) {
        (*request_it)->error_ = PlasmaError::OutOfDisk;
      }
      FinishRequest(request_it);
    }
  }
  return Status::OK();
}
```

### 9.11 Fallback 分配（磁盘 mmap）

`src/ray/object_manager/plasma/plasma_allocator.cc:98-120`：
```cpp
std::optional<Allocation> PlasmaAllocator::FallbackAllocate(size_t bytes) {
  bool is_fallback_allocated = false;

  // 强制 dlmalloc 使用单独的 mmap（到 /tmp 目录）
  RAY_CHECK(dlmallopt(M_MMAP_THRESHOLD, 0));
  void *mem = dlmemalign(kAlignment, bytes);
  RAY_CHECK(dlmallopt(M_MMAP_THRESHOLD, MAX_SIZE_T));  // 恢复

  if (!mem) {
    return absl::nullopt;
  }

  allocated_ += bytes;
  if (internal::IsOutsideInitialAllocation(mem)) {
    is_fallback_allocated = true;
    fallback_allocated_ += bytes;
  }
  return BuildAllocation(mem, bytes, is_fallback_allocated);
}
```

Fallback 时 `fake_mmap` 会使用 `/tmp` 目录（`dlmalloc.cc:140-239`）：
```cpp
void create_and_mmap_buffer(int64_t size, void **pointer, int *fd) {
    std::string file_template = dlmalloc_config.directory;
    // 第二次分配以后，如果是 fallback 模式，使用 fallback_directory
    if (allocated_once && dlmalloc_config.fallback_enabled) {
        file_template = dlmalloc_config.fallback_directory;  // /tmp
    }
    ...
}
```

### 9.12 完整降级链路

```
① LRU 驱逐 (ref_count=0 的对象，最多 11 次重试)
    │ 不够
    ▼
② Global GC (让 worker 释放 Python 对象引用)
    │ 可能使部分对象 ref_count 变为 0
    ▼
③ Object Spilling (ref_count=1 的对象写到磁盘)
    │ spill 完成后 ref_count → 0 → 可驱逐
    ▼
④ Grace Period 等待 (2 秒)
    │ 等待 spill/GC 完成
    ▼
⑤ Fallback 磁盘分配 (/tmp 下 mmap)
    │ 如果磁盘也满
    ▼
⑥ OutOfDisk 错误
```

### 9.13 Object Store 容量超限时的行为总结

| 阶段 | 触发条件 | 行为 |
|------|---------|------|
| LRU 驱逐 | dlmalloc 分配失败 | 从 LRU 尾部（最旧）选择 ref_count=0 对象删除 |
| Global GC | 驱逐后仍不够 | 通知所有 worker 回收 Python 对象引用 |
| Spilling | GC 后仍不够 | 将 ref_count=1 对象序列化写入磁盘 |
| Fallback | grace period 超时 | 在 `/tmp` 创建磁盘 mmap 文件 |
| OutOfDisk | 磁盘也满 | 返回错误给客户端 |

---

## 10. 运维诊断命令

### 10.1 查看 /dev/shm 配置

```bash
# 查看 /dev/shm 总容量和使用
df -h /dev/shm
# Filesystem      Size  Used  Avail  Use%
# tmpfs            32G  6.0G   26G   19%

# 查看挂载选项（容器内）
mount | grep shm
# tmpfs on /dev/shm type tmpfs (rw,nosuid,nodev,size=20971520k)

# 查看 Object Store 文件（通常已 unlink，看不到）
ls -la /dev/shm/
```

### 10.2 查看系统内存统计

```bash
# 共享内存（Object Store）使用量
grep -E "Shmem|Cached|MemAvailable|MemTotal" /proc/meminfo

# free 命令的 shared 列 = Shmem
free -h
#               total    used    free    shared   buff/cache   available
# Mem:          128G     40G     50G      19G       38G          70G
#                                         ↑ Object Store 物理占用
```

### 10.3 查看进程级内存

```bash
# Raylet 进程的共享内存
cat /proc/$(pgrep -f raylet)/status | grep -E "VmRSS|RssShmem|VmSize"
# VmSize:  50000000 kB   ← 虚拟地址空间（含 mmap）
# VmRSS:   20000000 kB   ← 物理驻留
# RssShmem: 19000000 kB  ← 其中共享内存部分

# 查看 raylet 的 deleted 映射段（Object Store）
cat /proc/$(pgrep -f raylet)/smaps | grep -A 10 "deleted"
# Size:     19000000 kB   ← 虚拟大小
# Rss:       5000000 kB   ← 实际物理占用
```

### 10.4 查看 cgroup 内存（容器内）

```bash
# cgroup v1
cat /sys/fs/cgroup/memory/memory.stat | grep -E "cache|rss|shmem"
cat /sys/fs/cgroup/memory/memory.usage_in_bytes
cat /sys/fs/cgroup/memory/memory.limit_in_bytes

# cgroup v2
cat /sys/fs/cgroup/memory.stat | grep -E "shmem|file|anon"
cat /sys/fs/cgroup/memory.current
cat /sys/fs/cgroup/memory.max
```

### 10.5 验证 Object Store 实际占用

```bash
# 使用 pmap 查看 plasma 相关映射
pmap -x $(pgrep -f raylet) | grep "deleted\|plasma"

# 或使用 smaps 精确查看
cat /proc/$(pgrep -f raylet)/smaps | awk '/deleted/{found=1} found{print} /^[0-9a-f]/ && !/deleted/{found=0}'
```

---

## 10. `available` 资源计算与 `used_memory_` 的真实含义

### 10.1 Dashboard `object_store_memory` 的 used 计算

Dashboard 显示的 `object_store_memory` used/total 来自调度层的 `available` 资源：

```cpp
// src/ray/raylet/scheduling/local_resource_manager.cc:321-335
void LocalResourceManager::UpdateAvailableObjectStoreMemResource() {
    const double used = get_used_object_store_memory_();  // lambda 回调
    const double total = total_instances[0].Double();
    auto new_available = std::vector<FixedPoint>{
        FixedPoint(total >= used ? total - used : 0.0)
    };
    local_resources_.available.Set(
        ResourceID::ObjectStoreMemory(), std::move(new_available));
}
```

**`available = total - used`**，其中 `used` 来自 `get_used_object_store_memory_` lambda。

### 10.2 `get_used_object_store_memory_` 的注册

```cpp
// src/ray/raylet/main.cc
get_used_object_store_memory_ = [this]() {
    return object_manager_.GetUsedObjectStoreMemory();
};
```

### 10.3 `ObjectManager::GetUsedObjectStoreMemory` 的实现

```cpp
// src/ray/object_manager/object_manager.cc
int64_t ObjectManager::GetUsedObjectStoreMemory() const {
    return used_memory_;
}
```

`used_memory_` 在两个时机更新：

```cpp
// object_manager.cc:187 — Object 创建
void ObjectManager::HandleObjectAdded(const ObjectInfo &object_info) {
    used_memory_ += object_info.data_size + object_info.metadata_size;
}

// object_manager.cc:215 — Object 删除
void ObjectManager::HandleObjectDeleted(const ObjectID &object_id) {
    used_memory_ -= object_info.data_size + object_info.metadata_size;
}
```

### 10.4 `used_memory_` 与 Plasma 实际分配的差异

**`used_memory_` 统计的是 `data_size + metadata_size`（请求大小），不包含**：

| 开销来源 | 说明 | 是否计入 `used_memory_` |
|---------|------|:---:|
| dlmalloc 64 字节对齐开销 | 实际分配 ≥ `((requested + 8 + 63) & ~63)` | 否 |
| chunk header 8 字节 | dlmalloc 管理开销 | 否 |
| Fallback 分配到 /tmp | 超出 /dev/shm 容量后在 /tmp mmap | 否 |
| 内存碎片 | 无法合并的空闲块 | 否 |

**所以 `used_memory_` 低估了实际内存占用。**

### 10.5 Fallback 分配对 available 的影响

Object Store 初始分配在 `/dev/shm`（容量有限），超出后 fallback 到 `/tmp`。但 fallback 分配不体现在 `used_memory_` 中——`used_memory_` 只统计逻辑大小，不区分分配位置。

然而 `available` 的计算中有修正：

```cpp
// object_manager.cc:955-960
object_store_available_memory_gauge_.Record(
    config_.object_store_memory - used_memory_ +
    plasma::plasma_store_runner->GetFallbackAllocated());
```

**注意**：这个修正只在 metric gauge 中体现，**不影响** `UpdateAvailableObjectStoreMemResource` 的 `available` 值。调度层看到的 `available = total - used_memory_`，**没有加回 fallback_allocated**。

### 10.6 为什么 Object 实际使用远大于 Resource 中的 `object_store_memory`

在大规模 Ray Data Pipeline（如视频推理）场景中，Dashboard 可能显示 `object_store_memory` used 仅 200 GB，但实际 `/dev/shm` + `/tmp` 中 object 占用远超此值。原因如下：

#### 原因 1：`used_memory_` 只在 Object 删除时减少

```
Object 生命周期中的 used_memory_ 变化：

  创建:  used_memory_ += data_size + metadata_size    ← 增加
  Spill: used_memory_ 不变                            ← 不减少！
  删除:  used_memory_ -= data_size + metadata_size    ← 减少
```

**Spill（溢写到磁盘）不会减少 `used_memory_`！** Spill 流程：

```
SpillIfOverPrimaryObjectsThreshold()
  → GetPrimaryBytes() / 200GB >= 0.8
  → SpillObjectUptoMaxThroughput()
    → TryToSpillObjects() → 写到磁盘
    → spilled_object_pending_delete_ 加入队列
    → ProcessSpilledObjectsPendingDelete()
      → 检查 ref_count == 0
      → 如果可以删: local_objects_.erase(object_id)
        → 触发 delete_object_callback → HandleObjectDeleted()
          → used_memory_ -= data_size              ← 这里才减！
```

**Spill 只是把数据写到磁盘，object 仍然在 plasma 中，`used_memory_` 不减。** 只有 spill 完成后、object 的 ref_count 归零、被 `HandleObjectDeleted` 删除时，`used_memory_` 才减少。

这意味着：**即使 object 已经 spill 到磁盘，只要它的 `ref_count > 0`（还有引用），它就不会被删除，`used_memory_` 不会减少，`available` 也不会增加。**

之前观察到的 `evictable: 0` 一致——所有 object 都有引用，spill 了也不释放空间。

#### 原因 2：Driver 持有大量 Block Ref 不释放

在 Ray Data Pipeline 中，driver 进程持有所有 block 的 ObjectRef：

```
Head 节点 767,300 个 CreatedByWorker object（12.87 TiB）
= driver 创建的 pipeline block
```

这些 block ref 在 driver 的 Python 对象中，只要 driver 进程还在运行，这些 ref 就不会释放 → object 的 `ref_count > 0` → `used_memory_` 不减 → `available` 不增。

#### 原因 3：Dead Actor 引用泄漏

Tidal 节点被抢占后，actor 进程被 SIGKILL → plasma client 无法 graceful disconnect → raylet 无法及时 Unpin object → object ref_count 不归零 → `used_memory_` 不减。

Worker 内存驱逐 `ray_memory_manager_worker_eviction` = 72,269（44/s），每次 SIGKILL 都可能留下未释放的 pin。

#### 原因 4：Replication 副本占空间

启用了 `enable_object_replication=true` 后，每个 Tidal 节点上的 object 会被复制到另一个 Tidal 节点。两个副本都计入 `used_memory_`，但 `available` 的 total 只计算一次 object store 容量（200 GB/节点）。

```
实际占用 = 原始 object + 副本 object
available = 200GB - (原始 + 副本) 的 used_memory_
```

#### 原因 5：Fallback 分配到 /tmp 不受 /dev/shm 限制

当 `/dev/shm`（200 GB 配额）分配满后，dlmalloc 会 fallback 到 `/tmp` 目录继续分配。这些 fallback 分配的 object **也计入 `used_memory_`**，但它们实际占用的磁盘空间不受 200 GB 限制。

更严重的是，fallback 分配的 object 也会被 replication push 到其他节点 → 其他节点的 `/dev/shm` 空间也被填满 → 也 fallback 到 `/tmp` → **集群总磁盘占用膨胀**。

之前观察到的 Spill 到磁盘 4.39-9.2 TiB 就是这个效应的体现。

### 10.7 同 IP 多 NodeId 导致聚合数据虚高

Tidal 节点反复被抢占后重新启动，**同一个 IP 上先后有多个 NodeId**（旧的 DEAD，新的 ALIVE）。Prometheus 按 `instance`（IP）聚合时：

```
同一 IP 10.80.246.235 上:
  NodeId-A (ALIVE):  used_memory_ = 180GB
  NodeId-B (DEAD):   used_memory_ = 190GB  ← 僵尸数据
  NodeId-C (DEAD):   used_memory_ = 170GB  ← 僵尸数据

Prometheus 聚合: sum by (instance) = 540GB
实际只有: 180GB
```

这个问题的根因是 `ray_io_cluster` label 之前不存在，无法区分同 IP 上的不同节点实例。已修复添加了 `ClusterNameKey`（`ray_io_cluster`）和 `_add_instance_label` 支持 `NodeAddress`。

### 10.8 完整数据流图

```
┌────────────────────────────────────────────────────────────────────┐
│                   Object Store 内存全景                              │
│                                                                     │
│  /dev/shm (tmpfs, 512GB 容器配额)                                   │
│  └─ Plasma 初始 mmap: 200GB (虚拟地址，物理页按需分配)              │
│     ├─ 原始 object  ──────────► used_memory_ += data_size           │
│     ├─ 副本 object (replication) ─► used_memory_ += data_size       │
│     └─ Spilled object ───────► used_memory_ 不变 (ref_count > 0)   │
│                                                                     │
│  /tmp/ray/ (磁盘，fallback 分配)                                    │
│  └─ 超出 /dev/shm 200GB 后的分配                                    │
│     ├─ 也计入 used_memory_                                          │
│     └─ 也触发 replication → 其他节点也 overflow                     │
│                                                                     │
│  Dashboard 资源视图:                                                 │
│  ├─ total: 200GB (固定配置)                                         │
│  ├─ used: used_memory_ (逻辑大小，不含碎片/对齐开销)                │
│  ├─ available: 200GB - used_memory_ (不含 fallback 修正)             │
│  └─ 实际占用: 远大于 used_memory_ (碎片 + fallback + 副本)          │
│                                                                     │
│  /proc/meminfo 视图:                                                │
│  ├─ Shmem: /dev/shm 中的物理页 (≈ 实际 plasma 物理占用)             │
│  ├─ MemAvailable: 可用物理内存                                      │
│  └─ memory_usage_threshold 检测: used/total = Shmem + ...           │
└────────────────────────────────────────────────────────────────────┘
```

### 10.9 关键代码文件

| 文件 | 说明 |
|------|------|
| `src/ray/object_manager/object_manager.cc:187,215` | `HandleObjectAdded` / `HandleObjectDeleted` 更新 `used_memory_` |
| `src/ray/object_manager/object_manager.cc:955-960` | `RecordMetrics` — available 计算（含 fallback 修正） |
| `src/ray/raylet/scheduling/local_resource_manager.cc:321-335` | `UpdateAvailableObjectStoreMemResource` — 调度层 `available` |
| `src/ray/raylet/local_object_manager.cc:526-545` | `ProcessSpilledObjectsPendingDelete` — spill 后删除条件 |
| `src/ray/object_manager/plasma/dlmalloc.cc:140-239` | `create_and_mmap_buffer` — fallback 分配路径 |

---

## 附录：关键源码文件索引

| 文件 | 作用 |
|------|------|
| `python/ray/_private/resource_and_label_spec.py` | 内存资源计算与注册 |
| `python/ray/_private/utils.py` | `resolve_object_store_memory`、`estimate_available_memory` |
| `python/ray/_private/ray_constants.py` | 内存相关常量定义 |
| `python/ray/autoscaler/v2/utils.py` | 集群资源聚合统计 |
| `python/ray/dashboard/modules/reporter/reporter_agent.py` | Dashboard 内存展示 |
| `src/ray/object_manager/plasma/dlmalloc.cc` | 共享内存分配（fake_mmap） |
| `src/ray/object_manager/plasma/plasma_allocator.cc` | PlasmaAllocator 分配/释放 |
| `src/ray/object_manager/plasma/store_runner.cc` | Object Store 初始化、/dev/shm 校验 |
| `src/ray/object_manager/plasma/obj_lifecycle_mgr.cc` | 对象生命周期、驱逐循环 |
| `src/ray/object_manager/plasma/eviction_policy.cc` | LRU 驱逐策略 |
| `src/ray/object_manager/plasma/object_store.cc` | 对象创建/删除 |
| `src/ray/object_manager/plasma/create_request_queue.cc` | OOM 处理队列 |
| `src/ray/object_manager/plasma/shared_memory.cc` | 客户端 mmap |
| `src/ray/common/memory_monitor.cc` | 系统内存监控（threshold） |
| `src/ray/common/ray_config_def.h` | 配置项定义 |
| `src/ray/raylet/scheduling/local_resource_manager.cc` | Object Store 使用量上报 |
| `src/ray/common/scheduling/scheduling_ids.h` | 资源标签定义 |

---

## 11. Ray Data Streaming Executor 中 Object Store 内存统计机制

以上章节分析的是 Ray Core 层面的 Object Store 物理内存管理。本节分析 **Ray Data Streaming Executor** 中进度条日志（如 `13.9MiB object store`）的统计来源和计算逻辑。

### 11.1 进度条日志示例

```
2026-08-05 16:25:41,403 INFO logging_progress.py:233 -- Tasks: 2; Actors: 4; Queued blocks: 1092968 (4.2GiB); Resources: 1.0 CPU, 13.9MiB object store
2026-08-05 16:51:12,008 INFO logging_progress.py:233 -- Tasks: 4; Actors: 4; Queued blocks: 1081700 (4.1GiB)
```

这里的 `object store` 值是 Ray Data 在 **driver 端估算** 的，非 Ray Core 的实际 object store 物理内存。它通过跟踪每个算子的内部队列和运行中任务的估算输出，累加得到整个 Dataset 执行期间 driver 侧可见的 object store 内存占用量。

### 11.2 架构概览

Ray Data 的 Streaming Executor 采用**算子拓扑 (DAG)** 驱动的流式执行模型，`ResourceManager` 负责跟踪和调度每个算子的资源使用。Object store 内存的统计分三层：

```
全局汇总 (_global_running_usage.object_store_memory)
  └─ 逐算子累加 (_op_running_usages[op].object_store_memory)
       └─ _estimate_object_store_memory_usage(op, state)
            ├─ mem_op_internal   (算子内部: 运行中任务的待yield输出)
            └─ mem_op_outputs    (算子输出: 内部输出队列 + 外部输出队列 + 下游输入队列)
```

### 11.3 触发时机

`update_usages()` 在**每次执行循环迭代**中被调用（`streaming_executor` 的 step 循环），从 DAG 末端（sink）向前遍历所有算子，重新计算每个算子的资源占用并累加到全局。

`resource_manager.py:245-288`：
```python
def update_usages(self):
    self._global_usage = ExecutionResources(0, 0, 0)
    self._global_running_usage = ExecutionResources(0, 0, 0)
    self._global_pending_usage = ExecutionResources(0, 0, 0)
    self._op_usages.clear()
    self._op_running_usages.clear()
    self._op_pending_usages.clear()

    # Iterate from last to first operator.
    for op, state in reversed(self._topology.items()):
        op_usage = op.current_logical_usage()
        op_running_usage = op.running_logical_usage()
        op_pending_usage = op.pending_logical_usage()

        assert not op_usage.object_store_memory
        assert not op_running_usage.object_store_memory
        assert not op_pending_usage.object_store_memory

        # object_store_memory 完全由 _estimate_object_store_memory_usage 计算
        used_object_store = self._estimate_object_store_memory_usage(op, state)

        op_usage = op_usage.copy(object_store_memory=used_object_store)
        op_running_usage = op_running_usage.copy(
            object_store_memory=used_object_store
        )

        self._op_usages[op] = op_usage
        self._op_running_usages[op] = op_running_usage
        self._op_pending_usages[op] = op_pending_usage

        self._global_usage = self._global_usage.add(op_usage)
        self._global_running_usage = self._global_running_usage.add(op_running_usage)
        self._global_pending_usage = self._global_pending_usage.add(op_pending_usage)

        # 也更新算子自身的 obj_store_mem_used 指标
        op._metrics.obj_store_mem_used = op_usage.object_store_memory
```

注意：`op.current_logical_usage()` / `running_logical_usage()` / `pending_logical_usage()` 返回的 `ExecutionResources` 中 `object_store_memory` 始终为 0（由 `assert` 保证），这个值完全由 `_estimate_object_store_memory_usage` 独立计算。

### 11.4 单算子 Object Store 内存估算

`_estimate_object_store_memory_usage(op, state)` 是核心方法，将 object store 占用分为两大组成部分：

`resource_manager.py:189-238`：
```python
def _estimate_object_store_memory_usage(
    self, op: "PhysicalOperator", state: "OpState"
) -> int:
    # InputDataBuffer 特殊处理：不计算执行前已存在的输入 ref
    if isinstance(op, InputDataBuffer):
        if op is self._output_operator:
            self._mem_op_internal[op] = 0
            self._mem_op_outputs[op] = self._external_consumer_bytes
            return self._external_consumer_bytes
        return 0

    # ===== 部分 1: 算子内部占用 =====
    mem_op_internal = op.metrics.obj_store_mem_pending_task_outputs or 0

    # ===== 部分 2: 算子输出占用 =====
    op_outputs_bytes = (
        # 2a: 内部输出队列
        op.metrics.obj_store_mem_internal_outqueue
        +
        # 2b: 外部输出队列
        state.output_queue_bytes()
    )

    # 2c: 下游算子消费本算子输出的部分
    used_op_outputs_bytes = sum(
        (
            downstream_op.metrics.obj_store_mem_internal_inqueue_for_input(
                downstream_op.input_dependencies.index(op)
            )
            + downstream_op.metrics.obj_store_mem_pending_task_inputs
        )
        for downstream_op in op.output_dependencies
    )

    self._mem_op_internal[op] = mem_op_internal
    self._mem_op_outputs[op] = op_outputs_bytes + used_op_outputs_bytes

    # DAG 终端算子额外计入外部消费者（迭代器/streaming_split 预取）
    if op is self._output_operator:
        self._mem_op_outputs[op] += self._external_consumer_bytes

    return self._mem_op_outputs[op] + self._mem_op_internal[op]
```

各部分含义：

| 部分 | 指标 | 含义 | 数据来源 |
|------|------|------|----------|
| **1. 算子内部** | `obj_store_mem_pending_task_outputs` | 运行中任务的 streaming generator buffer 中尚未 yield 的输出 | **估算值**（driver 无法直接观测） |
| **2a. 内部输出队列** | `obj_store_mem_internal_outqueue` | 算子 `_internal_outqueue` 中的 RefBundle 字节 | `_internal_outqueue.estimate_size_bytes()` |
| **2b. 外部输出队列** | `state.output_queue_bytes()` | OpState 中的外部输出缓冲区字节 | executor 调度维护 |
| **2c. 下游输入队列** | `obj_store_mem_internal_inqueue_for_input` | 下游算子内部输入队列中**来自本上游**的部分 | `_internal_inqueues[input_index].estimate_size_bytes()` |
| **2c. 下游任务输入** | `obj_store_mem_pending_task_inputs` | 下游已提交但未完成任务中的输入字节 | `_pending_task_inputs.estimate_size_bytes()` |
| **2d. 外部消费者** | `_external_consumer_bytes` | `iter_batches`/`streaming_split` 预取缓冲字节（仅 DAG 终端算子） | driver 端迭代器跟踪 |

### 11.5 下游输入队列算入上游的设计原因

从代码可以明确看到，下游算子的 `internal_inqueue` 和 `pending_task_inputs` 被归入**上游**的 `mem_op_outputs`：

```python
used_op_outputs_bytes = sum(
    downstream_op.metrics.obj_store_mem_internal_inqueue_for_input(
        downstream_op.input_dependencies.index(op)  # 只取来自本上游的那部分
    )
    + downstream_op.metrics.obj_store_mem_pending_task_inputs
    for downstream_op in op.output_dependencies
)

self._mem_op_outputs[op] = op_outputs_bytes + used_op_outputs_bytes  # 归入上游
```

设计意图是：这些 block 的 **Ray ObjectRef 归属者是上游算子**（上游 task 产出并放入 object store），下游只是持有引用。所以 object store 中的实际内存占用应该算在产出方（上游）头上，避免跨算子重复计数。

`internal_inqueue` 通过 `for_input(input_index)` 按输入来源拆分，只把**来自本上游的那部分** bytes 计入上游的 `mem_op_outputs`。这样每个 block 在整个 DAG 中只被归属一次——算在产出它的上游头上。

### 11.6 `mem_op_internal` 详解：运行中任务的待 yield 输出

`obj_store_mem_pending_task_outputs` 是唯一使用**估算**而非精确统计的指标。

`op_runtime_metrics.py:870-895`：
```python
@metric_property(
    description="Byte size of *pending* (not yielded yet) output blocks in running tasks.",
    metrics_group=MetricsGroup.OBJECT_STORE_MEMORY,
)
def obj_store_mem_pending_task_outputs(self) -> Optional[float]:
    per_task_output = self.obj_store_mem_max_pending_output_per_task
    if per_task_output is None:
        return None

    # ActorPoolMapOperator: 同时运行的任务数受 actor 数量上限约束
    from ray.data._internal.execution.operators.actor_pool_map_operator import (
        ActorPoolMapOperator,
    )

    num_tasks_running = self.num_tasks_running
    if isinstance(self._op, ActorPoolMapOperator):
        num_tasks_running = min(
            num_tasks_running, self._op._actor_pool.num_active_actors()
        )

    return num_tasks_running * per_task_output
```

`obj_store_mem_max_pending_output_per_task` 的估算公式：

`op_runtime_metrics.py:897-915`：
```python
@property
def obj_store_mem_max_pending_output_per_task(self) -> Optional[float]:
    context = self._op.data_context
    if context._max_num_blocks_in_streaming_gen_buffer is None:
        return None

    bytes_per_output = self.average_bytes_per_output
    # 如果还没有任务产出过输出，返回 None（按 0 处理）
    if bytes_per_output is None:
        return None

    num_pending_outputs = context._max_num_blocks_in_streaming_gen_buffer
    if self.average_num_outputs_per_task is not None:
        num_pending_outputs = min(
            num_pending_outputs, self.average_num_outputs_per_task
        )

    return bytes_per_output * num_pending_outputs
```

**估算公式汇总：**

```
obj_store_mem_pending_task_outputs
    = num_tasks_running × average_bytes_per_output × min(max_buffer_blocks, avg_outputs_per_task)

其中：
- num_tasks_running: 当前正在运行的任务数
- average_bytes_per_output: 历史已完成任务的平均每块输出大小
  = bytes_task_outputs_generated / num_task_outputs_generated
- max_buffer_blocks: DataContext._max_num_blocks_in_streaming_gen_buffer
- avg_outputs_per_task: 历史平均每任务输出块数
```

如果还没有任务产出过输出（`average_bytes_per_output` 为 None），该值返回 None，在 `_estimate_object_store_memory_usage` 中按 0 处理。

### 11.7 `mem_op_outputs` 详解：算子输出队列与下游消费

#### 内部输出队列 (`obj_store_mem_internal_outqueue`)

`op_runtime_metrics.py:853-862`：
```python
@metric_property(
    description="Byte size of output blocks in the operator's internal output queue.",
    metrics_group=MetricsGroup.OBJECT_STORE_MEMORY,
)
def obj_store_mem_internal_outqueue(self) -> int:
    return self._internal_outqueue.estimate_size_bytes()
```

通过回调实时增减：

```python
def on_output_queued(self, output: RefBundle):
    self.obj_store_mem_internal_outqueue_blocks += len(output.blocks)
    self._internal_outqueue.add(output)

def on_output_dequeued(self, output: RefBundle):
    self.obj_store_mem_internal_outqueue_blocks -= len(output.blocks)
    self._internal_outqueue.remove(output)
```

#### 内部输入队列 (`obj_store_mem_internal_inqueue_for_input`)

`op_runtime_metrics.py:836-847`：
```python
@metric_property(
    description="Byte size of input blocks in the operator's internal input queues, ...",
    metrics_group=MetricsGroup.OBJECT_STORE_MEMORY,
)
def obj_store_mem_internal_inqueue(self) -> int:
    return sum(q.estimate_size_bytes() for q in self._internal_inqueues)

def obj_store_mem_internal_inqueue_for_input(self, input_index: int) -> int:
    """Return the inqueue bytes attributable to a specific input dependency."""
    return self._internal_inqueues[input_index].estimate_size_bytes()
```

通过回调实时增减：

```python
def on_input_queued(self, input: RefBundle, *, input_index: int):
    self.obj_store_mem_internal_inqueue_blocks += len(input.blocks)
    self._internal_inqueues[input_index].add(input)

def on_input_dequeued(self, input: RefBundle, *, input_index: int):
    self.obj_store_mem_internal_inqueue_blocks -= len(input.blocks)
    self._internal_inqueues[input_index].remove(input)
```

#### 待处理任务输入 (`obj_store_mem_pending_task_inputs`)

`op_runtime_metrics.py:849-851`：
```python
@metric_property(
    description="Byte size of input blocks used by pending tasks.",
    metrics_group=MetricsGroup.OBJECT_STORE_MEMORY,
)
def obj_store_mem_pending_task_inputs(self) -> int:
    return self._pending_task_inputs.estimate_size_bytes()
```

通过任务生命周期回调增减：

```python
def on_task_submitted(self, task_index: int, inputs: RefBundle, ...):
    ...
    self._pending_task_inputs.add(inputs)  # 任务提交时增加

def on_task_finished(self, task_index: int, ...):
    ...
    self._pending_task_inputs.remove(inputs)  # 任务完成时减少
```

### 11.8 BundleQueue 底层：字节统计的精确累加机制

所有队列（`_internal_inqueues`、`_internal_outqueue`、`_pending_task_inputs`）都基于 `BundleQueue` 实现，其字节统计是**精确累加/扣减**的：

`bundle_queue/base.py:125-145`：
```python
class BaseBundleQueue(BundleQueue):
    def __init__(self):
        self._nbytes: int = 0
        self._num_blocks: int = 0
        self._num_bundles: int = 0
        self._num_rows: int = 0

    def _on_enqueue_bundle(self, bundle: RefBundle):
        self._nbytes += bundle.size_bytes()       # 入队时累加
        self._num_blocks += len(bundle.block_refs)
        self._num_bundles += 1
        self._num_rows += bundle.num_rows() or 0

    def _on_dequeue_bundle(self, bundle: RefBundle):
        self._nbytes -= bundle.size_bytes()       # 出队时扣减
        self._num_blocks -= len(bundle.block_refs)
        self._num_bundles -= 1
        self._num_rows -= bundle.num_rows() or 0

    def estimate_size_bytes(self) -> int:
        return self._nbytes                        # 直接返回累计值
```

### 11.9 bytes 大小的最终来源：BlockMetadata.size_bytes

`RefBundle.size_bytes()` 的实现（`ref_bundle.py:156-180`）：

```python
def size_bytes(self) -> int:
    total = 0
    for (_, metadata), block_slice in zip(self.blocks, self.slices):
        if block_slice is None:
            # 整块：直接使用 metadata.size_bytes
            total += metadata.size_bytes
        elif metadata.num_rows is None or metadata.num_rows == 0:
            # 行数未知：使用整块 metadata.size_bytes
            total += metadata.size_bytes
        elif metadata.num_rows != block_slice.num_rows:
            # 切片块：按行数比例估算
            per_row = metadata.size_bytes / metadata.num_rows
            total += max(1, int(math.ceil(per_row * block_slice.num_rows)))
        else:
            # 切片等于整块
            total += metadata.size_bytes
    return total
```

`BlockMetadata.size_bytes` 是在 **worker 端 task 执行完成时** 由 `BlockAccessor.get_metadata()` 填入的：

`block.py:527-535`：
```python
def get_metadata(
    self,
    input_files: Optional[List[str]] = None,
    block_exec_stats: Optional[BlockExecStats] = None,
    task_exec_stats: Optional[TaskExecWorkerStats] = None,
) -> BlockMetadata:
    return BlockMetadata(
        num_rows=self.num_rows(),
        size_bytes=self.size_bytes(),     # ← 这里填入
        input_files=tuple(input_files) if input_files is not None else None,
        exec_stats=block_exec_stats,
        task_exec_stats=task_exec_stats,
    )
```

对于 Arrow block（最常见的情况），`size_bytes` 来自 `pyarrow.Table.nbytes`：

`arrow_block.py:329-330`：
```python
def size_bytes(self) -> int:
    return self._table.nbytes   # pyarrow.Table.nbytes: 表中所有列数据 buffer 大小总和
```

### 11.10 数据流动的完整生命周期

以一个 MapOperator 为例，一个 RefBundle 从输入到输出的完整流转路径：

```
上游算子产出 RefBundle
  │
  ▼ on_input_queued() ──► _internal_inqueues[i].add(bundle)
  │                        obj_store_mem_internal_inqueue_blocks += len(bundle)
  │                        → 归入上游的 mem_op_outputs (下游 inqueue 部分)
  │
  ▼ on_input_dequeued() ──► _internal_inqueues[i].remove(bundle)
  │                         obj_store_mem_internal_inqueue_blocks -= len(bundle)
  │                         → 从上游的 mem_op_outputs 中扣减
  │
  ▼ on_task_submitted() ──► _pending_task_inputs.add(bundle)
  │                         num_tasks_running += 1
  │                         → 归入上游的 mem_op_outputs (下游 pending_task_inputs 部分)
  │
  ▼ [任务执行中]
  │  streaming generator 逐步 yield 输出 block
  │  → 计入 pending_task_outputs（归入本算子的 mem_op_internal）
  │
  ▼ on_task_output_generated() ──► bytes_task_outputs_generated += ...
  │                                 (此时输出还在 generator buffer 中)
  ▼ on_output_queued() ──► _internal_outqueue.add(output)
  │                        obj_store_mem_internal_outqueue_blocks += len(output)
  │                        → 计入本算子的 mem_op_outputs (outqueue 部分)
  │
  ▼ on_task_finished() ──► num_tasks_running -= 1
  │                        _pending_task_inputs.remove(bundle)
  │                        obj_store_mem_freed += input_size
  │                        → 从上游的 mem_op_outputs 中扣减
  │
  ▼ on_output_dequeued() ──► _internal_outqueue.remove(output)
  │                          obj_store_mem_internal_outqueue_blocks -= len(output)
  │                          → 从本算子的 mem_op_outputs 中扣减
  │
  ▼ 下游算子 on_input_queued() ... (循环重复)
```

### 11.11 特殊处理

#### InputDataBuffer

如果算子是 `InputDataBuffer`（DAG 最源头的输入），其 object store 占用计为 **0**：

```python
if isinstance(op, InputDataBuffer):
    if op is self._output_operator:
        return self._external_consumer_bytes
    return 0
```

原因：这些 ref 在执行前就已存在，不属于本次执行的动态开销，避免对预加载数据的重复统计。

#### DAG 终端算子（`_output_operator`）

终端算子额外加上 `_external_consumer_bytes`，即外部消费者（如 `ds.iter_batches` 迭代器、`streaming_split` 预取）缓冲的字节数。从 DAG 视角，这些是最终输出端的消费，统一归到终端算子头上。

### 11.12 全局限额与调度控制

`ResourceManager.get_global_limits()` 返回 object store 内存的全局上限：

`resource_manager.py:317-345`：
```python
def get_global_limits(self) -> ExecutionResources:
    if (
        time.time() - self._global_limits_last_update_time
        < self.GLOBAL_LIMITS_UPDATE_INTERVAL_S
    ):
        return self._global_limits

    self._global_limits_last_update_time = time.time()
    default_limits = self._options.resource_limits
    exclude = self._options.exclude_resources
    total_resources = self._get_total_resources()
    default_mem_fraction = self._object_store_memory_limit_fraction
    total_resources = total_resources.copy(
        object_store_memory=total_resources.object_store_memory
        * default_mem_fraction    # 默认取集群 object_store_memory 的 50%
    )
    self._global_limits = default_limits.min(total_resources).subtract(exclude)
    return self._global_limits
```

默认限额比例：

| OpResourceAllocator 是否启用 | 默认比例 | 常量 |
|---|---|---|
| 启用 | 集群 `object_store_memory` × 0.5 | `DEFAULT_OBJECT_STORE_MEMORY_LIMIT_FRACTION = 0.5` |
| 未启用 | 集群 `object_store_memory` × 0.25 | `DEFAULT_OBJECT_STORE_MEMORY_LIMIT_FRACTION_NO_RESERVATION = 0.25` |

可通过 `RAY_DATA_OBJECT_STORE_MEMORY_LIMIT_FRACTION` 环境变量覆盖。

### 11.13 进度条日志输出

`ResourceManager.get_op_usage_str()` 负责将算子资源使用格式化为日志字符串：

`resource_manager.py:395-397`：
```python
usage_str = f"{self._op_running_usages[op].cpu:.1f} CPU"
if self._op_running_usages[op].gpu:
    usage_str += f", {self._op_running_usages[op].gpu:.1f} GPU"
usage_str += f", {self._op_running_usages[op].object_store_memory_str()} object store"
```

`ExecutionResources.object_store_memory_str()`（`execution_options.py:153-157`）：
```python
def object_store_memory_str(self) -> str:
    if self.object_store_memory == float("inf"):
        return "inf"
    return memory_string(self.object_store_memory)   # 转换为人类可读格式（如 "13.9MiB"）
```

verbose 模式下额外显示内部/输出拆分：

```python
if verbose:
    usage_str += (
        f" (in={memory_string(self.get_mem_op_internal(op))},"
        f"out={memory_string(self.get_mem_op_outputs(op))})"
    )
```

### 11.14 统计精度总结

| 统计项 | 数据来源 | 是否精确 | 底层来源 |
|--------|----------|----------|----------|
| `internal_inqueue` | `_internal_inqueues[i].estimate_size_bytes()` | 精确 | `RefBundle.size_bytes()` → `metadata.size_bytes` → `pyarrow.Table.nbytes` |
| `internal_outqueue` | `_internal_outqueue.estimate_size_bytes()` | 精确 | 同上 |
| `pending_task_inputs` | `_pending_task_inputs.estimate_size_bytes()` | 精确 | 同上 |
| `output_queue_bytes` | `state.output_queue_bytes()` | 精确 | 同上 |
| `pending_task_outputs` | `num_running × avg_bytes × min(buffer, avg_outputs)` | **估算** | 基于历史 `bytes_task_outputs_generated / num_task_outputs_generated` |
| `_external_consumer_bytes` | driver 端迭代器缓冲 | 精确 | 迭代器内部跟踪 |

### 11.15 关键设计要点

1. **避免重复计数**：每个 RefBundle 的 object store 内存只归咎于**产出它的算子**。下游算子的 `internal_inqueue` 和 `pending_task_inputs` 被归入上游算子的 `mem_op_outputs`，而非算子自身占用。

2. **估算 vs 精确**：`pending_task_outputs` 是唯一估算值（driver 端无法直接观测 worker 上 streaming generator buffer 中的数据量），其余（inqueue/outqueue/pending_inputs）都是基于 `RefBundle.size_bytes()` 的精确累加。

3. **不计算 InputDataBuffer**：执行前已存在的输入 ref 不算动态开销，避免对预加载数据的重复统计。

4. **外部消费者归属**：迭代器预取（`iter_batches`/`streaming_split`）的字节统一归到 DAG 终端算子（`_output_operator`），因为从 DAG 视角这些是最终输出端的消费。

5. **bytes 最终来源**：所有精确统计的最终源头都是 worker 端 task 执行时 `pyarrow.Table.nbytes` 写入 `BlockMetadata.size_bytes`，随 block ref 传回 driver 端，后续所有队列/指标只做累加/扣减，不再重新计算。

### 11.16 关键源码文件索引

| 文件 | 作用 |
|------|------|
| `python/ray/data/_internal/execution/resource_manager.py:189-238` | `_estimate_object_store_memory_usage` — 核心 object store 内存估算 |
| `python/ray/data/_internal/execution/resource_manager.py:245-288` | `update_usages` — 全局资源使用汇总 |
| `python/ray/data/_internal/execution/resource_manager.py:317-345` | `get_global_limits` — 全局限额计算 |
| `python/ray/data/_internal/execution/resource_manager.py:55-148` | `ResourceManager.__init__` — 初始化 `_mem_op_internal` / `_mem_op_outputs` |
| `python/ray/data/_internal/execution/interfaces/op_runtime_metrics.py` | `OpRuntimeMetrics` — 所有 object store 内存指标定义和回调 |
| `python/ray/data/_internal/execution/interfaces/execution_options.py:90-180` | `ExecutionResources` — 资源数据结构和 `object_store_memory_str()` |
| `python/ray/data/_internal/execution/interfaces/ref_bundle.py:156-180` | `RefBundle.size_bytes()` — bundle 字节大小计算（含切片比例估算） |
| `python/ray/data/_internal/execution/bundle_queue/base.py:125-145` | `BaseBundleQueue` — 队列入队/出队时的精确字节累加/扣减 |
| `python/ray/data/block.py:286-335` | `BlockMetadata` / `BlockStats` — `size_bytes` 字段定义 |
| `python/ray/data/block.py:511-535` | `BlockAccessor.size_bytes()` / `get_metadata()` — 元数据生成入口 |
| `python/ray/data/_internal/arrow_block.py:329-330` | `ArrowBlockAccessor.size_bytes()` — `pyarrow.Table.nbytes` |
