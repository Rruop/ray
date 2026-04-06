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
