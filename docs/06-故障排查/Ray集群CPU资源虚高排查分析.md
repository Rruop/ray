# Ray 集群 CPU 资源虚高排查分析

## 问题现象

Ray 集群 Dashboard / `ray status` 报告的 CPU 总量远超实际物理 CPU 数量：

```
Total Usage:
  2002.0/99084.0 CPU
  500.0/501.0 GPU
  0.0/1.0 head
  25.41KiB/400.86TiB memory
  864.22GiB/90.99TiB object_store_memory
  0.020000000000000018/500.0 worker-1
```

GPU 数量看起来合理（约等于 worker 数），但 CPU 总量 99084 远超预期。

## 与 ray-project/ray#63566 的关系

**本问题与 [#63566](https://github.com/ray-project/ray/issues/63566) 无关。**

#63566 描述的是 V1 Autoscaler 的 `monitor.py` 未过滤 DEAD 节点，导致 `ray status` 将 DEAD 节点也算入 Active 资源。但本集群中：

- GPU 数量与 ALIVE 节点数一致，说明 **DEAD 节点未被计入**
- CPU 虚高的原因是同机多 raylet 重复报核（见下文），不是 DEAD 节点残留

## 根因分析

### 核心结论

**同一台物理机上启动了多个 Ray raylet 进程，每个 raylet 都认为自己独占整台机器的全部 CPU 核数，没有任何机制感知同机其他 raylet 的存在来均分 CPU。**

### 数据证据

通过 `ray list nodes --format json` 获取全部节点数据：

| 指标 | 值 |
|------|-----|
| ALIVE worker raylet 进程数 | 447~500 |
| 唯一物理机 IP 数 | 96~97 |
| 平均每台物理机 raylet 数 | ~5.2 |
| 最多的 | 1 台机器起 8 个 raylet |
| 报告总 CPU | 91,246 |
| 实际总 CPU（每台机器只算一次） | ~17,659 |
| CPU 虚报倍数 | **5.5×** |

#### 同机多 raylet 示例

```
IP=10.80.240.31: 8 raylets
  CPU_list=[128, 128, 128, 128, 128, 128, 128, 128]
  GPU_list=[1, 1, 1, 1, 1, 1, 1, 1]
  CPU_sum=1024, GPU_sum=8
  实际物理机: 128 核 + 1 GPU
```

**8 个 raylet 各报 128 CPU = 1024 CPU，但物理机只有 128 核。**

### GPU 同样被虚报

| 指标 | 实际 | 报告 | 虚报倍数 |
|------|------|------|----------|
| GPU | ~76 (76台×1) | 452 | 5.9× |
| CPU | ~17,659 | 91,246 | 5.5× |

GPU 看起来"没错"只是因为 `ray status` 显示 `452.0/453.0 GPU` 碰巧接近 ALIVE 节点数，但 452 是虚数，实际只有 ~76 块物理 GPU。Ray 调度器会以为有 452 GPU 可用，同一块物理 GPU 可能被调度给多个 raylet 上的 task/actor 同时使用。

### CPU 分布不是简单的 128×N

集群中机器有两种规格：

| 机器 CPU 核数 | raylet 数 | 小计 |
|---|---|---|
| 128 核 | 169 | 21,632 |
| **256 核** | 253 | **64,768** |
| 其他（38~240） | 25 | 4,846 |
| **合计** | 447 | **91,246** |

超过一半的 raylet（253 个）跑在 256 核机器上，占总量的大头。所以不是 128×500，而是混有 256 核机器，91,246 是所有 raylet 各自报的 CPU 总和——只是这些 256 核机器上也起了多个 raylet（每个都报 256），实际物理 CPU 远小于 91,246。

## Linux CPU 可用性指标的层次关系

在排查过程中涉及多个 CPU 相关指标，它们代表不同层次的 CPU 可用性：

### 各指标的数据来源和含义

| 指标 | 数据来源 | 含义 | 示例值 |
|------|----------|------|--------|
| `os.cpu_count()` | `/proc/cpuinfo` 的 processor 条目数 | 物理机全部逻辑核数（含超线程） | 128 或 256 |
| `nproc` | `sched_getaffinity()` 系统调用 | 当前进程可用的 CPU 核数（受 cgroup 限制） | 1 或 16 |
| `nproc --all` | 忽略 cgroup 限制，等同 `os.cpu_count()` | 物理机全部逻辑核数 | 128 或 256 |
| `cfs_quota_us / cfs_period_us` | `/sys/fs/cgroup/cpu/cpu.cfs_quota_us` 等 | CPU 时间配额（时间片占比限制） | -1（无限制）或 16 |
| `cpuset.cpus` | `/sys/fs/cgroup/cpuset/cpuset.cpus` | 允许使用的 CPU 核 ID 列表（核绑定） | 0-127 或 80-87,208-215 |

### `os.cpu_count()` vs `nproc` 的根本区别

- **`os.cpu_count()`**：调用 C 库的 `sysconf(_SC_NPROCESSORS_ONLN)`，读取 `/proc/cpuinfo` 中 processor 条目数，返回**系统全部逻辑核数**，不受 cgroup 限制影响
- **`nproc`**：调用 `sched_getaffinity()` 系统调用，返回**当前进程的 CPU 亲和性掩码中的核数**，受 cgroup 限制影响

在容器中两者可能完全不同：`/proc/cpuinfo` 可能显示宿主机全部 256 核，而 `nproc` 只返回 cgroup 限制后的 16 核。

### cfs_quota 和 cpuset 的区别

两种完全不同的 CPU 限制机制：

**cfs_quota / cfs_period（CPU 时间配额）**
- 控制**时间片占比**，不绑定特定核
- 例：quota=1600000, period=100000 → 每 100ms 周期内最多用 1600ms CPU 时间
- 进程可以在**任意核**上跑，但总时间不能超标
- 相当于"最多用 16 核的算力"，但可能跨 256 个核轮转执行
- 适合限制 CPU 使用率
- 文件：`/sys/fs/cgroup/cpu/cpu.cfs_quota_us` + `/sys/fs/cgroup/cpu/cpu.cfs_period_us`

**cpuset（CPU 核绑定）**
- 控制**能在哪些核上运行**，是硬绑定
- 例：`80-87,208-215` → 只能在这 16 个核上执行，其他 240 个核完全不可见
- 不限制使用率，在这 16 个核上可以跑满 100%
- 适合核隔离、NUMA 亲和性
- 文件：`/sys/fs/cgroup/cpuset/cpuset.cpus`

### 两者组合的场景

| 场景 | cfs_quota | cpuset | 效果 | Ray 报 CPU |
|------|-----------|--------|------|------------|
| 无限制 | -1 | 0-127 | 不限时间不限核 | 128 |
| 仅时间限制 | 16 | 无 | 可在任意核跑但总时间≤16核 | 16 |
| 仅核绑定 | 无 | 16个核 | 只能在 16 核上跑满 | 16 |
| 双重限制 | 16 | 16个核 | 只能用 16 核且时间≤16核 | min(16,16)=16 |
| 双重限制(不一致) | 8 | 16个核 | 时间更严格 | min(8,16)=8 |

Ray 的 `_get_docker_cpus()` 取 `min(quota, cpuset_num)`，即**两者取较严格的**，因为无论哪个更小都代表实际可用 CPU 的上限。

### 128 核 worker 节点数据 (10.80.240.31)

| 指标 | 值 | 来源 |
|------|-----|------|
| `os.cpu_count()` | 128 | `/proc/cpuinfo` |
| `/proc/cpuinfo` processor 数 | 128 | `/proc/cpuinfo` |
| `nproc` | 1 | `sched_getaffinity()` |
| `nproc --all` | 128 | 忽略 cgroup |
| 物理 | 2 Socket × 32 Core × 2 Thread = 128 逻辑核 | `lscpu` |
| `cpu.cfs_quota_us` | -1 (无限制) | `/sys/fs/cgroup/cpu/cpu.cfs_quota_us` |
| `cpuset.cpus` | 0-127 (全可见) | `/sys/fs/cgroup/cpuset/cpuset.cpus` |

### 256 核 worker 节点数据 (10.82.234.16)

| 指标 | 值 | 来源 |
|------|-----|------|
| `os.cpu_count()` | 256 | `/proc/cpuinfo` |
| `/proc/cpuinfo` processor 数 | 256 | `/proc/cpuinfo` |
| `nproc` | 1 | `sched_getaffinity()` |
| `nproc --all` | 256 | 忽略 cgroup |
| 物理 | 2 Socket × 64 Core × 2 Thread = 256 逻辑核 | `lscpu` |
| `cpu.cfs_quota_us` | -1 (无限制) | `/sys/fs/cgroup/cpu/cpu.cfs_quota_us` |
| `cpuset.cpus` | 0-255 (全可见) | `/sys/fs/cgroup/cpuset/cpuset.cpus` |

### CPU=19 的 worker 节点数据 (10.82.236.36)

| 指标 | 值 | 来源 |
|------|-----|------|
| `os.cpu_count()` | 256 | `/proc/cpuinfo` |
| `/proc/cpuinfo` processor 数 | 256 | `/proc/cpuinfo` |
| `nproc` | 16 | `sched_getaffinity()` |
| `nproc --all` | 256 | 忽略 cgroup |
| 物理 | 2 Socket × 64 Core × 2 Thread = 256 逻辑核 | `lscpu` |
| `cpu.cfs_quota_us / cpu.cfs_period_us` | 1,600,000 / 100,000 = **16** | cgroup v1 |
| `cpuset.cpus` | 80-87,208-215 = **16 核** | cgroup v1 |

`get_num_cpus()` 走 `_get_docker_cpus()` 时：
- `cpu_quota = 1600000 / 100000 = 16`
- `cpuset_num = 16`
- `return min(16, 16) = 16`

**但 Ray 实际报了 19，不是 16！原因见 KubeRay 章节说明。**

## CPU 数量检测的完整代码逻辑

### 1. 入口：`ResourceAndLabelSpec.__init__`

`python/ray/_private/resource_and_label_spec.py:24-47`

```python
class ResourceAndLabelSpec:
    def __init__(
        self,
        num_cpus: Optional[int] = None,  # 用户显式指定
        num_gpus: Optional[int] = None,
        memory: Optional[float] = None,
        available_memory_bytes: Optional[int] = None,
        object_store_memory: Optional[float] = None,
        resources: Optional[Dict[str, float]] = None,
        labels: Optional[Dict[str, str]] = None,
    ):
        self.num_cpus = num_cpus  # None 表示需要自动检测
        self.num_gpus = num_gpus
        ...
```

### 2. 创建入口：`Node.get_resource_and_label_spec()`

`python/ray/_private/node.py:582-594`

```python
def get_resource_and_label_spec(self):
    if not self._resource_and_label_spec:
        self._resource_and_label_spec = ResourceAndLabelSpec(
            self._ray_params.num_cpus,   # 来自 --num-cpus 启动参数
            self._ray_params.num_gpus,   # 来自 --num-gpus 启动参数
            self._ray_params.memory,
            self._ray_params.available_memory_bytes,
            self._ray_params.object_store_memory,
            self._ray_params.resources,
            self._ray_params.labels,
        ).resolve(is_head=self.head, node_ip_address=self.node_ip_address)
    return self._resource_and_label_spec
```

**关键**：`self._ray_params.num_cpus` 如果用户通过 `--num-cpus` 指定了值，则为该值；如果未指定，则为 None，触发自动检测。

### 3. 自动检测触发：`ResourceAndLabelSpec.resolve()`

`python/ray/_private/resource_and_label_spec.py:129-162`

```python
def resolve(self, is_head: bool, node_ip_address: Optional[str] = None):
    self._resolve_resources(is_head=is_head, node_ip_address=node_ip_address)

    # Resolve accelerator-specific resources
    (
        accelerator_manager,
        num_accelerators,
    ) = ResourceAndLabelSpec._get_current_node_accelerator(
        self.num_gpus, self.resources
    )
    self._resolve_accelerator_resources(accelerator_manager, num_accelerators)

    # Default num_gpus value if unset by user and unable to auto-detect.
    if self.num_gpus is None:
        self.num_gpus = 0

    # Resolve and merge node labels from all sources
    self._resolve_labels(accelerator_manager)
    self._resolve_memory_resources()
    self._is_resolved = True
    return self
```

### 4. CPU 自动检测触发点：`_resolve_resources()`

`python/ray/_private/resource_and_label_spec.py:202-248`

```python
def _resolve_resources(self, is_head, node_ip_address):
    # 先合并环境变量资源和参数资源
    env_resources = ResourceAndLabelSpec._load_env_resources()
    (
        num_cpus,
        num_gpus,
        memory,
        object_store_memory,
        merged_resources,
    ) = ResourceAndLabelSpec._merge_resources(env_resources, self.resources or {})

    # 环境变量值优先级高于参数
    self.num_cpus = self.num_cpus if num_cpus is None else num_cpus
    self.num_gpus = self.num_gpus if num_gpus is None else num_gpus
    self.memory = self.memory if memory is None else memory
    ...

    # Auto-detect CPU count if not explicitly set
    if self.num_cpus is None:
        self.num_cpus = ray._private.utils.get_num_cpus()
```

**注意**：这里有三层优先级：
1. 环境变量 `RAY_RESOURCES_ENVIRONMENT_VARIABLE` 中的 CPU 值（最高优先级）
2. 启动参数 `--num-cpus` 指定的值
3. 自动检测 `get_num_cpus()`（最低优先级，仅当以上两者均为 None 时触发）

### 5. 自动检测核心：`get_num_cpus()`

`python/ray/_private/utils.py:423-490`

```python
def get_num_cpus(
    override_docker_cpu_warning: bool = ENV_DISABLE_DOCKER_CPU_WARNING,
    truncate: bool = True,
) -> float:
    # Step 1: 取 Python 的 multiprocessing.cpu_count()
    # 底层调用 C 的 sysconf(_SC_NPROCESSORS_ONLN)
    # 读取 /proc/cpuinfo 中的 processor 条目数（逻辑核数，含超线程）
    cpu_count = multiprocessing.cpu_count()

    # Step 2: 如果设置了 RAY_USE_MULTIPROCESSING_CPU_COUNT=1，直接返回
    if os.environ.get("RAY_USE_MULTIPROCESSING_CPU_COUNT"):
        return cpu_count

    try:
        # Step 3: 尝试读取 cgroup 的 CPU 限制来纠正
        docker_count = _get_docker_cpus()
        if docker_count is not None and docker_count != cpu_count:
            # 在 K8s 环境下不打印警告（KUBERNETES_SERVICE_HOST 存在时）
            if (
                "KUBERNETES_SERVICE_HOST" not in os.environ
                and not ENV_DISABLE_DOCKER_CPU_WARNING
                and not override_docker_cpu_warning
            ):
                logger.warning(
                    "Detecting docker specified CPUs. ..."
                )
            # 不支持小数 CPU，截断
            if int(docker_count) != float(docker_count):
                logger.warning(
                    f"Ray currently does not support initializing Ray "
                    f"with fractional cpus. Your num_cpus will be "
                    f"truncated from {docker_count} to {int(docker_count)}."
                )
            if truncate:
                docker_count = int(docker_count)
            # 用 cgroup 检测值覆盖 os.cpu_count()
            cpu_count = docker_count

    except Exception:
        # cgroup 检测只在 Linux 上有效，其他平台忽略
        pass

    return cpu_count
```

### 6. cgroup CPU 检测：`_get_docker_cpus()`

`python/ray/_private/utils.py:349-420`

```python
def _get_docker_cpus(
    cpu_quota_file_name="/sys/fs/cgroup/cpu/cpu.cfs_quota_us",
    cpu_period_file_name="/sys/fs/cgroup/cpu/cpu.cfs_period_us",
    cpuset_file_name="/sys/fs/cgroup/cpuset/cpuset.cpus",
    cpu_max_file_name="/sys/fs/cgroup/cpu.max",
) -> Optional[float]:
    # Docker 有两种 CPU 限制方式：
    # 1. --cpuset-cpus (cpuset)
    # 2. --cpus 或 --cpu-quota/--cpu-period (CFS quota)
    # 对于 Ray，取两者中较小的值

    cpu_quota = None

    # ---- cgroup v1: 读 cpu.cfs_quota_us / cpu.cfs_period_us ----
    # See: https://bugs.openjdk.java.net/browse/JDK-8146115
    if os.path.exists(cpu_quota_file_name) and os.path.exists(cpu_period_file_name):
        try:
            with (
                open(cpu_quota_file_name, "r") as quota_file,
                open(cpu_period_file_name, "r") as period_file,
            ):
                # cpu_quota = 1600000 / 100000 = 16.0
                cpu_quota = float(quota_file.read()) / float(period_file.read())
        except Exception:
            logger.exception("Unexpected error calculating docker cpu quota.")

    # ---- cgroup v2: 读 cpu.max ----
    elif os.path.exists(cpu_max_file_name):
        try:
            max_file = open(cpu_max_file_name).read()
            quota_str, period_str = max_file.split()
            if quota_str.isnumeric() and period_str.isnumeric():
                cpu_quota = float(quota_str) / float(period_str)
            else:
                # quota_str is "max" meaning the cpu quota is unset
                cpu_quota = None
        except Exception:
            logger.exception("Unexpected error calculating docker cpu quota.")

    # ---- 处理 quota 特殊值 ----
    if (cpu_quota is not None) and (cpu_quota < 0):
        # quota = -1 表示无限制，置为 None
        cpu_quota = None
    elif cpu_quota == 0:
        # 至少 1 核
        cpu_quota = 1

    # ---- 读 cpuset ----
    cpuset_num = None
    if os.path.exists(cpuset_file_name):
        try:
            with open(cpuset_file_name) as cpuset_file:
                ranges_as_string = cpuset_file.read()
                ranges = ranges_as_string.split(",")
                cpu_ids = []
                for num_or_range in ranges:
                    if "-" in num_or_range:
                        start, end = num_or_range.split("-")
                        cpu_ids.extend(list(range(int(start), int(end) + 1)))
                    else:
                        cpu_ids.append(int(num_or_range))
                cpuset_num = len(cpu_ids)
        except Exception:
            logger.exception("Unexpected error calculating docker cpuset ids.")

    # ---- 返回两者较小值 ----
    if cpu_quota and cpuset_num:
        return min(cpu_quota, cpuset_num)
    return cpu_quota or cpuset_num
```

**`_get_docker_cpus()` 的返回值逻辑**：

| cfs_quota | cpuset_num | 返回值 | 说明 |
|-----------|------------|--------|------|
| None（无限制，如 -1） | 128 | 128 | 只受 cpuset 限制 |
| 16 | None | 16 | 只受 cfs 限制 |
| 16 | 16 | 16 | 双重限制，取较小值 |
| 8 | 16 | 8 | cfs 更严格，取较小值 |
| None | None | None | 均无限制，返回 None |

**当 `_get_docker_cpus()` 返回 None 时**，`get_num_cpus()` 不会进入 `if docker_count is not None and docker_count != cpu_count` 分支，直接返回 `os.cpu_count()` 的值（即 `/proc/cpuinfo` 的全部逻辑核数）。

### 7. GPU 自动检测：`_get_current_node_accelerator()`

`python/ray/_private/resource_and_label_spec.py:405-448`

```python
@staticmethod
def _get_current_node_accelerator(
    num_gpus: Optional[int], resources: Dict[str, float]
) -> Tuple[AcceleratorManager, int]:
    """
    Returns the AcceleratorManager and accelerator count for the accelerator
    associated with this node.
    The resolved accelerator count uses num_gpus (for GPUs) or resources if set,
    and otherwise falls back to the count auto-detected by the AcceleratorManager.
    The resolved accelerator count is capped by the number of visible accelerators.
    """
    for resource_name in accelerators.get_all_accelerator_resource_names():
        accelerator_manager = accelerators.get_accelerator_manager_for_resource(
            resource_name
        )
        if accelerator_manager is None:
            continue
        # Respect configured value for GPUs if set
        if resource_name == "GPU":
            num_accelerators = num_gpus  # 优先用用户指定值
        else:
            num_accelerators = resources.get(resource_name)

        if num_accelerators is None:
            # 自动检测 GPU 数量
            num_accelerators = (
                accelerator_manager.get_current_node_num_accelerators()
            )
            # 关键区别：检查进程可见的加速器 ID（如 CUDA_VISIBLE_DEVICES）
            visible_accelerator_ids = (
                accelerator_manager.get_current_process_visible_accelerator_ids()
            )
            if visible_accelerator_ids is not None:
                num_accelerators = min(
                    num_accelerators, len(visible_accelerator_ids)
                )

        if num_accelerators > 0:
            return accelerator_manager, num_accelerators

    return None, 0
```

**GPU 比 CPU 多了一步 `CUDA_VISIBLE_DEVICES` 校验**，但本集群中每个 raylet 都看到 1 块 GPU，所以也报了 1。

### 8. 资源传递到 raylet 进程

`python/ray/_private/services.py:1718-1731`

```python
# 将 ResourceAndLabelSpec 转为资源字典
static_resources = resource_and_label_spec.to_resource_dict()
# to_resource_dict() 将 num_cpus → "CPU": 128, num_gpus → "GPU": 1

# 用 CPU 数限制最大并行启动 worker 数
num_cpus_static = static_resources.get("CPU", 0)
maximum_startup_concurrency = max(
    1, min(multiprocessing.cpu_count(), num_cpus_static)
)

# 格式化为命令行参数
resource_argument = ",".join(
    ["{},{}".format(*kv) for kv in static_resources.items()]
)
# 结果如: "CPU,128,GPU,1,memory,881151324160,..."

# 传给 raylet 进程
f"--static_resource_list={resource_argument}"
```

### 9. C++ raylet 接收

`src/ray/raylet/main.cc:588-592`

```cpp
// Parse the resource list.
std::istringstream resource_string(static_resource_list);
std::string resource_name;
std::string resource_quantity;

while (std::getline(resource_string, resource_name, ',')) {
    RAY_CHECK(std::getline(resource_string, resource_quantity, ','));
    static_resource_conf[resource_name] = std::stod(resource_quantity);
}
auto num_cpus_it = static_resource_conf.find("CPU");
int num_cpus = num_cpus_it != static_resource_conf.end()
                   ? static_cast<int>(num_cpus_it->second)
                   : 0;
```

C++ raylet 直接使用 Python 端传来的 CPU 值注册为节点资源，不做二次检测。

## KubeRay 对 CPU 数量的影响

通过环境变量 `KUBERAY_GEN_RAY_START_CMD` 可以看到 KubeRay operator 生成的 `ray start` 命令：

### 256 核机器（未设 `--num-cpus`）

```
KUBERAY_GEN_RAY_START_CMD=ray start  --address=10.29.132.140:6379  --block
  --dashboard-agent-listen-port=0  --num-gpus=1  --resources='{"worker-1":1}'
```

**没有 `--num-cpus`**，Ray 走自动检测 → `get_num_cpus()` → `os.cpu_count()` = 256。

对应的 raylet 启动命令：
```
--static_resource_list=worker-1,1,node:10.48.34.21,1.0,accelerator_type:G,1,CPU,128,GPU,1,memory,881151324160,object_store_memory,200000000000
--maximum_startup_concurrency=128
--num_prestart_python_workers=128
```

### CPU=19 的机器（显式设了 `--num-cpus=19`）

```
KUBERAY_GEN_RAY_START_CMD=ray start  --address=10.137.44.180:6379  --block
  --dashboard-agent-listen-port=0  --memory=134049339904
  --num-cpus=19  --num-gpus=1  --resources='{"worker-1":1}'
```

**显式 `--num-cpus=19`**，跳过自动检测，直接用 19。这个 19 与 cgroup 实际限制的 16 核不一致，是 KubeRay operator 根据 Pod CPU request/limit 计算出来的值。

对应的 raylet 启动命令：
```
--static_resource_list=worker-1,1,node:10.82.236.36,1.0,accelerator_type:G,1,CPU,19,GPU,1,memory,134049339904,object_store_memory,40170423091
--maximum_startup_concurrency=19
--num_prestart_python_workers=19
```

### KubeRay 行为不一致的核心问题

- 对部分 Pod：设了 `--num-cpus=N`（N 由 KubeRay 根据 Pod 配置计算）
- 对部分 Pod：**未设 `--num-cpus`**，Ray 走自动检测，报出 `os.cpu_count()` 的满额值
- 同一物理机多 raylet 时，无论是否设了 `--num-cpus`，只要值不是 `物理核数/raylet数`，就会导致 CPU 虚报

## 不同节点的 CPU 检测结果对比

| 节点类型 | `os.cpu_count()` | cgroup 限制 | KubeRay `--num-cpus` | Ray 最终报 CPU | 原因 |
|----------|-------------------|-------------|----------------------|---------------|------|
| 128 核 (cpuset 全核) | 128 | quota=-1, cpuset=0-127 | 未设 | **128** | 自动检测 → `os.cpu_count()` |
| 256 核 (cpuset 全核) | 256 | quota=-1, cpuset=0-255 | 未设 | **256** | 自动检测 → `os.cpu_count()` |
| 256 核 (cgroup 限 16 核) | 256 | quota=16, cpuset=16核 | **19** | **19** | KubeRay 显式指定（与 cgroup 不一致） |
| 256 核 (其他中间值) | 256 | 各种部分限制 | 可能设/未设 | 38~240 | 取决于 cgroup 限制和 KubeRay 配置 |

## CPU 虚报的两种路径

### 路径 A：无 `--num-cpus`，自动检测报满额

```
同一台 256 核机器起 8 个 raylet
  → 每个 raylet 调 get_num_cpus()
  → cfs_quota = -1 (None), cpuset = 0-255 (256)
  → _get_docker_cpus() 返回 256
  → get_num_cpus() 返回 256
  → 每个报 CPU=256
  → 总计 256×8 = 2048 CPU（实际只有 256）
```

### 路径 B：KubeRay 设了 `--num-cpus`，但值与 cgroup 不一致

```
cgroup 限制 16 核（quota=16, cpuset=16核）
  → _get_docker_cpus() 会返回 16
  → 但 KubeRay 设了 --num-cpus=19
  → Ray 跳过自动检测，直接用 19
  → 报 CPU=19（与 cgroup 实际 16 不一致）
```

## CPU 与 GPU 检测逻辑的关键区别

| | CPU 检测 | GPU 检测 |
|---|---|---|
| 自动检测函数 | `get_num_cpus()` | `_get_current_node_accelerator()` |
| 数据来源 | `os.cpu_count()` → `/proc/cpuinfo` | `get_current_node_num_accelerators()` → GPU 设备探测 |
| cgroup 校验 | 读 `cpu.cfs_quota_us` + `cpuset.cpus` | 无 |
| 进程级隔离校验 | **无** | **有** `CUDA_VISIBLE_DEVICES` |
| 默认值(检测不到时) | `os.cpu_count()` | 0 |

GPU 的 `CUDA_VISIBLE_DEVICES` 校验使得如果启动多个 raylet 时给每个设了不同的 `CUDA_VISIBLE_DEVICES`，GPU 数可以被正确均分。但 CPU 没有类似的进程级隔离机制（cgroup cpuset 对所有同机 raylet 共享），所以每个 raylet 都看到完整的 `/proc/cpuinfo`。

## 解决方案

### 方案 1：启动时显式指定 `--num-cpus`

每个 raylet 的 `--num-cpus` 应设为 `物理核数 / 同机 raylet 数`。

例如：128 核机器起 8 个 raylet → 每个 `--num-cpus=16`

### 方案 2：通过 cgroup 限制每个 raylet 可见的 CPU

利用 cgroup cpuset 为每个 raylet 分配独立的 CPU 核集合，使得 `get_num_cpus()` 通过 cgroup 检测到正确的受限核数。但需要注意当前 `_get_docker_cpus()` 的实现中，cpuset 是容器级别的（同一 Pod 内所有进程共享），不会为单个 raylet 进程区分。

### 方案 3：设置环境变量 `RAY_USE_MULTIPROCESSING_CPU_COUNT=1`

跳过 cgroup 检测，直接用 `multiprocessing.cpu_count()`。但这对多 raylet 场景没有帮助，因为同 Pod 内所有进程看到相同的 `/proc/cpuinfo`。

### 方案 4：修改 KubeRay operator

让 KubeRay 在生成 `ray start` 命令时，始终根据 Pod 的 CPU limit 和同 Pod raylet 数计算 `--num-cpus`，确保不遗漏。当前 KubeRay 对部分 Pod 设了 `--num-cpus`，对部分未设，行为不一致。

## 完整调用链总结

```
用户/平台启动 ray start
  ↓
  是否指定 --num-cpus?
  ├── 是 → self._ray_params.num_cpus = 用户值
  │        ↓
  │        ResourceAndLabelSpec(num_cpus=用户值)
  │        ↓
  │        resolve() → _resolve_resources()
  │        ↓
  │        self.num_cpus 不为 None，跳过自动检测
  │
  └── 否 → self._ray_params.num_cpus = None
           ↓
           ResourceAndLabelSpec(num_cpus=None)
           ↓
           resolve() → _resolve_resources()
           ↓
           self.num_cpus is None → 触发自动检测
           ↓
           self.num_cpus = ray._private.utils.get_num_cpus()
           ↓
           ┌── Step 1: multiprocessing.cpu_count()
           │   └── sysconf(_SC_NPROCESSORS_ONLN)
           │       └── 读 /proc/cpuinfo processor 条目数
           │           → 物理机全部逻辑核（如 128 或 256）
           │
           ├── Step 2: RAY_USE_MULTIPROCESSING_CPU_COUNT=1?
           │   └── 是 → 直接返回 cpu_count（跳过 cgroup）
           │
           └── Step 3: _get_docker_cpus()
               ├── 3a: 读 cgroup v1 文件
               │   /sys/fs/cgroup/cpu/cpu.cfs_quota_us  (如 -1 或 1600000)
               │   /sys/fs/cgroup/cpu/cpu.cfs_period_us (如 100000)
               │   → cpu_quota = quota / period
               │   → quota < 0 时置为 None
               │
               ├── 3b: 或读 cgroup v2 文件
               │   /sys/fs/cgroup/cpu.max
               │   → 同理解析 quota/period
               │
               ├── 3c: 读 cpuset
               │   /sys/fs/cgroup/cpuset/cpuset.cpus
               │   → 解析如 "0-127" 或 "80-87,208-215"
               │   → cpuset_num = 核数
               │
               └── 3d: 返回值
                   ├── 两者都有: min(cpu_quota, cpuset_num)
                   └── 只有一个: cpu_quota or cpuset_num
                   └── 都为 None: 返回 None
           ↓
           docker_count != cpu_count 且 docker_count != None?
           ├── 是 → cpu_count = docker_count（cgroup 值覆盖）
           └── 否 → cpu_count = os.cpu_count()（无 cgroup 限制）
  ↓
  self.num_cpus = 最终检测值
  ↓
  to_resource_dict() → {"CPU": self.num_cpus, "GPU": self.num_gpus, ...}
  ↓
  services.start_raylet()
  ├── static_resources = resource_and_label_spec.to_resource_dict()
  ├── maximum_startup_concurrency = min(multiprocessing.cpu_count(), num_cpus_static)
  ├── resource_argument = "CPU,128,GPU,1,memory,..."
  └── 启动命令行: --static_resource_list={resource_argument}
  ↓
  raylet main.cc 解析 --static_resource_list
  ├── static_resource_conf["CPU"] = 128.0
  └── node_manager_config.resource_config = ResourceSet(static_resource_conf)
  ↓
  GCS 汇总所有节点资源 → Dashboard / ray status 显示
```
