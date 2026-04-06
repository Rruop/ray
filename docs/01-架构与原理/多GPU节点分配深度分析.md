# Ray 多 GPU 节点 GPU 编号与分配全链路深度解析

> 版本：Ray 2.52.1
> 日期：2026-06-04

本文档完整追踪 Ray 在多 GPU 节点上，GPU 资源从物理检测、逻辑编号、注册到 Raylet、调度分配、直到 Worker 进程设置 `CUDA_VISIBLE_DEVICES` 的全链路代码逻辑。

---

## 目录

- [一、概述：完整数据流全景图](#一概述完整数据流全景图)
- [二、第 1 步：节点启动时检测 GPU 数量](#二第-1-步节点启动时检测-gpu-数量)
  - [2.1 入口：ResourceAndLabelSpec.resolve()](#21-入口resourceandlabelspecresolve)
  - [2.2 自动检测 GPU 数量](#22-自动检测-gpu-数量)
  - [2.3 NVML 检测的具体实现](#23-nvml-检测的具体实现)
  - [2.4 不同 GPU 厂商的自动选择](#24-不同-gpu-厂商的自动选择)
  - [2.5 CUDA_VISIBLE_DEVICES 对检测的影响](#25-cuda_visible_devices-对检测的影响)
- [三、第 2 步：写入 ResourceAndLabelSpec 并转换为资源字典](#三第-2-步写入-resourceandlabelspec-并转换为资源字典)
  - [3.1 _resolve_accelerator_resources()：写入 num_gpus](#31-_resolve_accelerator_resources写入-num_gpus)
  - [3.2 加速器型号注册为自定义资源](#32-加速器型号注册为自定义资源)
  - [3.3 to_resource_dict()：转换为字典](#33-to_resource_dict转换为字典)
- [四、第 3 步：通过命令行参数传递给 Raylet 进程](#四第-3-步通过命令行参数传递给-raylet-进程)
- [五、第 4 步：Raylet C++ 解析命令行参数](#五第-4-步raylet-c-解析命令行参数)
- [六、第 5 步：创建 ClusterResourceScheduler — GPU: 4.0 → [1.0, 1.0, 1.0, 1.0]](#六第-5-步创建-clusterresourcescheduler--gpu-40--1010-1010-1010)
  - [6.1 ResourceMapToNodeResources()：标量到 NodeResourceSet](#61-resourcemaptonoderesources标量到-noderesourceset)
  - [6.2 NodeResourceSet 构造：仍为标量存储](#62-noderesourceset-构造仍为标量存储)
  - [6.3 LocalResourceManager 初始化：核心转换](#63-localresourcemanager-初始化核心转换)
  - [6.4 NodeResourceInstanceSet 构造函数：展开算法](#64-noderesourceinstanceset-构造函数展开算法)
  - [6.5 IsUnitInstanceResource() 判断依据](#65-isunitinstanceresource-判断依据)
  - [6.6 Unit Instance Resource 的设计意义](#66-unit-instance-resource-的设计意义)
- [七、第 6 步：Raylet 调度——分配具体的 GPU 实例](#七第-6-步raylet-调度分配具体的-gpu-实例)
  - [7.1 调度入口：AllocateLocalTaskResources()](#71-调度入口allocatelocaltaskresources)
  - [7.2 核心分配算法：TryAllocate()](#72-核心分配算法tryallocate)
  - [7.3 分数 GPU 分配的 best-fit 算法](#73-分数-gpu-分配的-best-fit-算法)
  - [7.4 分配结果示例](#74-分配结果示例)
- [八、第 7 步：Raylet 将 GPU 实例 ID 通过 RPC 返回给 Core Worker](#八第-7-步raylet-将-gpu-实例-id-通过-rpc-返回给-core-worker)
- [九、第 8 步：Core Worker 接收并存储 GPU 分配信息](#九第-8-步core-worker-接收并存储-gpu-分配信息)
  - [9.1 提交端：PushNormalTask 携带 resource_mapping](#91-提交端pushnormaltask-携带-resource_mapping)
  - [9.2 执行端：TaskReceiver 解析 resource_mapping](#92-执行端taskreceiver-解析-resource_mapping)
  - [9.3 存入 CoreWorker::resource_ids_](#93-存入-coreworkerresource_ids_)
- [十、第 9 步：Python 层设置 CUDA_VISIBLE_DEVICES](#十第-9-步python-层设置-cuda_visible_devices)
  - [10.1 Worker 初始化时记录原始 CUDA_VISIBLE_DEVICES](#101-worker-初始化时记录原始-cuda_visible_devices)
  - [10.2 Task 执行前设置 CUDA_VISIBLE_DEVICES](#102-task-执行前设置-cuda_visible_devices)
  - [10.3 从 Core Worker 获取 GPU ID 的中间层](#103-从-core-worker-获取-gpu-id-的中间层)
  - [10.4 CUDA_VISIBLE_DEVICES 的双重映射](#104-cuda_visible_devices-的双重映射)
  - [10.5 最终设置环境变量](#105-最终设置环境变量)
  - [10.6 Task 完成后重置环境变量](#106-task-完成后重置环境变量)
- [十一、Ray Data 中的 GPU 使用](#十一ray-data-中的-gpu-使用)
  - [11.1 用户代码传入 num_gpus](#111-用户代码传入-num_gpus)
  - [11.2 Ray Data 创建 Actor 时的资源声明](#112-ray-data-创建-actor-时的资源声明)
  - [11.3 Ray Data 本身不直接管理 CUDA_VISIBLE_DEVICES](#113-ray-data-本身不直接管理-cuda_visible_devices)
- [十二、关键细节总结](#十二关键细节总结)
  - [12.1 Actor Task 不重设 CUDA_VISIBLE_DEVICES](#121-actor-task-不重设-cuda_visible_devices)
  - [12.2 普通 Task 完成后重置](#122-普通-task-完成后重置)
  - [12.3 Placement Group 场景下的 GPU 分配](#123-placement-group-场景下的-gpu-分配)
  - [12.4 资源模型对比表](#124-资源模型对比表)
- [十三、最终 Raylet 内存状态](#十三最终-raylet-内存状态)

---

## 一、概述：完整数据流全景图

以一个有 4 张 NVIDIA GPU 的节点为例，用户调用 `ds.map_batches(fn, num_gpus=1)` 时，完整的数据流如下：

```
┌────────────────────────────────────────────────────────────────────┐
│ ① Python: ray.init()                                               │
│    node.py:553  ResourceAndLabelSpec(num_gpus=None).resolve()       │
└────────────────────────┬───────────────────────────────────────────┘
                         │ num_gpus=None → 触发自动检测
                         ▼
┌────────────────────────────────────────────────────────────────────┐
│ ② 检测 GPU 数量                                                     │
│    nvidia_gpu.py:54  pynvml.nvmlDeviceGetCount() → 4               │
│    resource_and_label_spec.py:434  num_accelerators = 4             │
└────────────────────────┬───────────────────────────────────────────┘
                         │ num_accelerators=4
                         ▼
┌────────────────────────────────────────────────────────────────────┐
│ ③ 写入 ResourceAndLabelSpec                                         │
│    resource_and_label_spec.py:360  self.num_gpus = 4                │
│    resource_and_label_spec.py:367  self.resources["accelerator_    │
│                                     type:A100"] = 1                 │
│    to_resource_dict() → {"CPU":16, "GPU":4, "memory":..., ...}     │
└────────────────────────┬───────────────────────────────────────────┘
                         │
                         ▼
┌────────────────────────────────────────────────────────────────────┐
│ ④ 格式化为命令行参数                                                 │
│    services.py:1688  "CPU,16,GPU,4,memory,107374182400,..."         │
│    services.py:1887  --static_resource_list=CPU,16,GPU,4,...        │
└────────────────────────┬───────────────────────────────────────────┘
                         │
                         ▼
┌────────────────────────────────────────────────────────────────────┐
│ ⑤ Raylet C++ 解析命令行                                             │
│    main.cc:549-556  static_resource_conf["GPU"] = 4.0               │
│    ResourceSet({"GPU": 4.0, "CPU": 16.0, ...})                     │
└────────────────────────┬───────────────────────────────────────────┘
                         │
                         ▼
┌────────────────────────────────────────────────────────────────────┐
│ ⑥ 创建 ClusterResourceScheduler                                     │
│    cluster_resource_scheduler.cc:54                                 │
│      NodeResources = ResourceMapToNodeResources(                    │
│          {"GPU": 4.0}, {"GPU": 4.0}, labels)                       │
│      → NodeResourceSet: GPU → 标量 4.0                              │
└────────────────────────┬───────────────────────────────────────────┘
                         │
                         ▼
┌────────────────────────────────────────────────────────────────────┐
│ ⑦ LocalResourceManager 初始化 — ★ 核心转换 ★                        │
│    local_resource_manager.cc:48-49                                  │
│      NodeResourceInstanceSet(total)                                 │
│    resource_instance_set.cc:33-37                                   │
│      GPU IsUnitInstanceResource == true                             │
│      num_instances = 4                                              │
│      for (i=0; i<4; i++) instances.push_back(1.0)                  │
│      GPU: 4.0 → [1.0, 1.0, 1.0, 1.0]                              │
│    数组索引 = GPU 编号: [0]→GPU0, [1]→GPU1, [2]→GPU2, [3]→GPU3    │
└────────────────────────┬───────────────────────────────────────────┘
                         │
                         ▼
┌────────────────────────────────────────────────────────────────────┐
│ ⑧ Raylet 调度：分配 GPU 实例                                         │
│    resource_instance_set.cc:325-336                                 │
│      TryAllocate(GPU, demand=1.0)                                   │
│      available=[1,1,1,1] → 找到 available[0]==1 → 分配 GPU 0      │
│      allocation=[1,0,0,0], available=[0,1,1,1]                     │
└────────────────────────┬───────────────────────────────────────────┘
                         │
                         ▼
┌────────────────────────────────────────────────────────────────────┐
│ ⑨ Raylet → Core Worker RPC 传递                                     │
│    local_lease_manager.cc:1009-1026                                 │
│      resource_mapping { name:"GPU", resource_ids {                  │
│        index:0, quantity:1.0                                        │
│      }}                                                             │
└────────────────────────┬───────────────────────────────────────────┘
                         │
                         ▼
┌────────────────────────────────────────────────────────────────────┐
│ ⑩ Core Worker 接收并存储                                             │
│    task_receiver.cc:162-170  resource_ids={"GPU":[(0,1.0)]}         │
│    core_worker.cc:3081  resource_ids_ = {"GPU":[(0,1.0)]}          │
└────────────────────────┬───────────────────────────────────────────┘
                         │
                         ▼
┌────────────────────────────────────────────────────────────────────┐
│ ⑪ Python 层设置 CUDA_VISIBLE_DEVICES                                │
│    _raylet.pyx:2066  set_visible_accelerator_ids()                  │
│    utils.py:280-292  get_accelerator_ids() → {"GPU":["0"]}          │
│    nvidia_gpu.py:99-101                                             │
│      os.environ["CUDA_VISIBLE_DEVICES"] = "0"                       │
│    ★★ torch.cuda 只能看到 GPU 0 ★★                                  │
│                                                                     │
│    _raylet.pyx:2173-2176  (task完成后恢复)                          │
│      reset_visible_accelerator_env_vars()                           │
└────────────────────────────────────────────────────────────────────┘
```

```
 ┌────────────────────────────────────────────────────────────────────┐
  │ 节点启动                                                           │
  │ NVML 检测 GPU 数量 → 注册为 GPU 资源实例 [1.0, 1.0, 1.0, 1.0]    │
  │ (nvidia_gpu.py:47-56)                                             │
  └───────────────────────┬────────────────────────────────────────────┘
                          ▼
  ┌────────────────────────────────────────────────────────────────────┐
  │ Raylet 调度                                                        │
  │ TryAllocate() 从 GPU 实例数组中选择可用的 GPU（按索引分配）         │
  │ allocation[i] = 1.0 表示第 i 号 GPU 被分配                        │
  │ (resource_instance_set.cc:295-360)                                │
  └───────────────────────┬────────────────────────────────────────────┘
                          ▼
  ┌────────────────────────────────────────────────────────────────────┐
  │ Lease 授予                                                         │
  │ 将 GPU 实例索引 (inst_idx) 通过 RPC 传给 Worker                   │
  │ resource_mapping: {name: "GPU", ids: [{index: 2, quantity: 1.0}]} │
  │ (local_lease_manager.cc:1003-1027)                                │
  └───────────────────────┬────────────────────────────────────────────┘
                          ▼
  ┌────────────────────────────────────────────────────────────────────┐
  │ Worker 执行 Task                                                   │
  │ 1. core_worker.resource_ids() 获取分配的 GPU 索引                  │
  │ 2. set_visible_accelerator_ids() → 设置 CUDA_VISIBLE_DEVICES      │
  │    例如: CUDA_VISIBLE_DEVICES="2" (只看到第2号GPU)                │
  │ 3. Task 代码中 torch.cuda.device_count() == 1, 只能使用该 GPU    │
  │ (_raylet.pyx:2061-2066, utils.py:267-293, nvidia_gpu.py:92-101)  │
  ┌────────────────────────────────────────────────────────────────────┐
  │ Worker 执行 Task                                                   │
  │ 1. core_worker.resource_ids() 获取分配的 GPU 索引                  │
  │ 2. set_visible_accelerator_ids() → 设置 CUDA_VISIBLE_DEVICES      │
  │    例如: CUDA_VISIBLE_DEVICES="2" (只看到第2号GPU)                │
  │ 3. Task 代码中 torch.cuda.device_count() == 1, 只能使用该 GPU    │
  │ (_raylet.pyx:2061-2066, utils.py:267-293, nvidia_gpu.py:92-101)  │
  └───────────────────────┬────────────────────────────────────────────┘
                          ▼
  ┌────────────────────────────────────────────────────────────────────┐
  │ Task 完成 (仅普通 task)                                            │
  │ reset_visible_accelerator_env_vars() → 恢复原始 CUDA_VISIBLE_...  │
  │ Worker 可被复用执行其他 task                                       │
  │ (_raylet.pyx:2173-2176)                                           │
  │ Task 完成 (仅普通 task)                                            │
  │ reset_visible_accelerator_env_vars() → 恢复原始 CUDA_VISIBLE_...  │
  │ Worker 可被复用执行其他 task                                       │
  │ (_raylet.pyx:2173-2176)                                           │
  └────────────────────────────────────────────────────────────────────┘

  关键结论

  1. Ray 会自动编号：节点上的 GPU 通过 NVML 检测，自动编号为 0, 1, 2, ...，对应 CUDA 的设备索引。
  2. Task 级 GPU 隔离：每个 task/actor 请求 num_gpus=N 时，Raylet 会分配 N 个 GPU 实例，并通过设置 CUDA_VISIBLE_DEVICES 让 task 只能看到被分配的 GPU。例如请求
  num_gpus=1 被分配到第 2 号 GPU，则 CUDA_VISIBLE_DEVICES="2"。
  3. Actor 创建后不再重设：Actor task 不会重设 CUDA_VISIBLE_DEVICES（见 _raylet.pyx:2065），确保 Actor 生命周期内 GPU 绑定稳定。
  4. Placement Group 场景：GPU 可通过 placement group 做 bundle 级别的绑定，同一个 PG bundle 中的 GPU 实例是一致的（见 resource_instance_set.cc:140-290
  的注释）。
  5. Ray Data 中的 GPU 使用：Ray Data 通过 num_gpus 参数传给 ray.remote()，最终走的就是上述同一套 Ray Core GPU 分配机制。Ray Data 本身不直接管理
  CUDA_VISIBLE_DEVICES。
  ```

---

## 二、第 1 步：节点启动时检测 GPU 数量

### 2.1 入口：ResourceAndLabelSpec.resolve()

**文件：`python/ray/_private/node.py:550-561`**

```python
def get_resource_and_label_spec(self):
    """Resolve and return the current ResourceAndLabelSpec for the node."""
    if not self._resource_and_label_spec:
        self._resource_and_label_spec = ResourceAndLabelSpec(
            self._ray_params.num_cpus,
            self._ray_params.num_gpus,    # ← 用户未指定则为 None
            self._ray_params.memory,
            self._ray_params.available_memory_bytes,
            self._ray_params.object_store_memory,
            self._ray_params.resources,
            self._ray_params.labels,
        ).resolve(is_head=self.head, node_ip_address=self.node_ip_address)
    return self._resource_and_label_spec
```

当 `num_gpus=None` 时，`resolve()` 会触发自动检测。

**文件：`python/ray/_private/resource_and_label_spec.py:127-164`** — `resolve()` 方法：

```python
def resolve(self, is_head, node_ip_address=None):
    # 步骤 A: 先处理基础资源（CPU、自定义资源等）
    self._resolve_resources(is_head=is_head, node_ip_address=node_ip_address)

    # 步骤 B: 检测加速器（GPU/TPU/Neuron等），获取数量
    (accelerator_manager, num_accelerators) = \
        ResourceAndLabelSpec._get_current_node_accelerator(
            self.num_gpus, self.resources
        )

    # 步骤 C: 将加速器数量写入资源字段
    self._resolve_accelerator_resources(accelerator_manager, num_accelerators)

    # 步骤 D: 如果仍未检测到，默认设为 0
    if self.num_gpus is None:
        self.num_gpus = 0

    # 步骤 E: 解析并合并节点标签
    self._resolve_labels(accelerator_manager)

    # 步骤 F: 解析内存资源
    self._resolve_memory_resources()

    self._is_resolved = True
    assert self._all_fields_set()
    return self
```

### 2.2 自动检测 GPU 数量

**文件：`python/ray/_private/resource_and_label_spec.py:402-448`** — `_get_current_node_accelerator()`：

```python
@staticmethod
def _get_current_node_accelerator(
    num_gpus: Optional[int], resources: Dict[str, float]
) -> Tuple[AcceleratorManager, int]:
    """
    Returns the AcceleratorManager and accelerator count for the accelerator
    associated with this node. This assumes each node has at most one accelerator type.
    If no accelerators are present, returns None.

    The resolved accelerator count uses num_gpus (for GPUs) or resources if set, and
    otherwise falls back to the count auto-detected by the AcceleratorManager. The
    resolved accelerator count is capped by the number of visible accelerators.
    """
    for resource_name in accelerators.get_all_accelerator_resource_names():
        accelerator_manager = accelerators.get_accelerator_manager_for_resource(
            resource_name
        )
        if accelerator_manager is None:
            continue

        # ★ GPU 的数量来源优先级：
        # 1. 用户显式指定的 num_gpus
        # 2. 自动检测（NVML）
        if resource_name == "GPU":
            num_accelerators = num_gpus   # 先取用户值
        else:
            num_accelerators = resources.get(resource_name)

        if num_accelerators is None:
            # ★ 用户未指定 → 自动检测
            num_accelerators = (
                accelerator_manager.get_current_node_num_accelerators()
            )
            # 对 NVIDIA GPU: pynvml.nvmlDeviceGetCount() → 4

            # ★ 如果用户启动时设置了 CUDA_VISIBLE_DEVICES
            # 则取 min(检测数量, 可见设备数)
            visible_accelerator_ids = (
                accelerator_manager.get_current_process_visible_accelerator_ids()
            )
            if visible_accelerator_ids is not None:
                num_accelerators = min(
                    num_accelerators, len(visible_accelerator_ids)
                )

        if num_accelerators > 0:
            return accelerator_manager, num_accelerators
            # 返回: (NvidiaGPUAcceleratorManager, 4)

    return None, 0
```

**GPU 数量来源优先级总结**：

| 优先级 | 来源 | 示例 |
|--------|------|------|
| 1 | 用户显式指定 `ray.init(num_gpus=2)` | `num_gpus=2` |
| 2 | 自动检测 `pynvml.nvmlDeviceGetCount()` | 检测到 4 张 |
| 3 | 受 `CUDA_VISIBLE_DEVICES` 约束 | `min(4, 2) = 2`（如果只可见 2 张）|
| 4 | 检测不到时默认 | `num_gpus=0` |

### 2.3 NVML 检测的具体实现

**文件：`python/ray/_private/accelerators/nvidia_gpu.py:46-56`**

```python
@staticmethod
def get_current_node_num_accelerators() -> int:
    import ray._private.thirdparty.pynvml as pynvml

    try:
        pynvml.nvmlInit()
    except pynvml.NVMLError:
        return 0  # pynvml init failed
    device_count = pynvml.nvmlDeviceGetCount()  # ← 通过 NVML 获取物理 GPU 数量
    pynvml.nvmlShutdown()
    return device_count
```

Ray 通过 **NVML (NVIDIA Management Library)** 检测物理 GPU 数量。每个 GPU 会被自动编号为 `0, 1, 2, ...`，这些编号对应 `CUDA_VISIBLE_DEVICES` 中的索引。

此外，`get_current_node_accelerator_type()` 还会检测 GPU 型号：

**文件：`python/ray/_private/accelerators/nvidia_gpu.py:59-77`**

```python
@staticmethod
def get_current_node_accelerator_type() -> Optional[str]:
    import ray._private.thirdparty.pynvml as pynvml

    try:
        pynvml.nvmlInit()
    except pynvml.NVMLError:
        return None
    device_count = pynvml.nvmlDeviceGetCount()
    cuda_device_type = None
    if device_count > 0:
        handle = pynvml.nvmlDeviceGetHandleByIndex(0)  # ← 取第一张 GPU 的型号
        device_name = pynvml.nvmlDeviceGetName(handle)
        if isinstance(device_name, bytes):
            device_name = device_name.decode("utf-8")
        cuda_device_type = (
            NvidiaGPUAcceleratorManager._gpu_name_to_accelerator_type(device_name)
        )
    pynvml.nvmlShutdown()
    return cuda_device_type
```

GPU 型号通过正则提取：

```python
NVIDIA_GPU_NAME_PATTERN = re.compile(r"\w+\s+([A-Z0-9]+)")

@staticmethod
def _gpu_name_to_accelerator_type(name):
    if name is None:
        return None
    match = NVIDIA_GPU_NAME_PATTERN.match(name)
    return match.group(1) if match else None
    # "Tesla V100-SXM2-16GB" → "V100"
    # "NVIDIA A100-SXM4-40GB" → "A100"
```

### 2.4 不同 GPU 厂商的自动选择

**文件：`python/ray/_private/accelerators/__init__.py:41-72`**

Ray 支持多种 GPU 厂商，通过延迟初始化按优先级检测：

```python
def get_accelerator_manager_for_resource(
    resource_name: str,
) -> Optional[AcceleratorManager]:
    try:
        return get_accelerator_manager_for_resource._resource_name_to_accelerator_manager.get(
            resource_name, None
        )
    except AttributeError:
        # Lazy initialization.
        resource_name_to_accelerator_manager = {
            accelerator_manager.get_resource_name(): accelerator_manager
            for accelerator_manager in get_all_accelerator_managers()
        }
        # ★ Special handling for GPU resource name since multiple accelerator managers
        # have the same GPU resource name.
        if AMDGPUAcceleratorManager.get_current_node_num_accelerators() > 0:
            resource_name_to_accelerator_manager["GPU"] = AMDGPUAcceleratorManager
        elif IntelGPUAcceleratorManager.get_current_node_num_accelerators() > 0:
            resource_name_to_accelerator_manager["GPU"] = IntelGPUAcceleratorManager
        elif MetaxGPUAcceleratorManager.get_current_node_num_accelerators() > 0:
            resource_name_to_accelerator_manager["GPU"] = MetaxGPUAcceleratorManager
        else:
            resource_name_to_accelerator_manager["GPU"] = NvidiaGPUAcceleratorManager
        get_accelerator_manager_for_resource._resource_name_to_accelerator_manager = (
            resource_name_to_accelerator_manager
        )
        return resource_name_to_accelerator_manager.get(resource_name, None)
```

检测优先级：AMD GPU > Intel GPU > Metax GPU > NVIDIA GPU（默认）

所有支持的加速器管理器：

```python
def get_all_accelerator_managers() -> Set[AcceleratorManager]:
    return {
        NvidiaGPUAcceleratorManager,
        IntelGPUAcceleratorManager,
        AMDGPUAcceleratorManager,
        TPUAcceleratorManager,
        NeuronAcceleratorManager,
        HPUAcceleratorManager,
        NPUAcceleratorManager,
        RBLNAcceleratorManager,
        MetaxGPUAcceleratorManager,
    }
```

### 2.5 CUDA_VISIBLE_DEVICES 对检测的影响

当用户启动 Ray 时设置了 `CUDA_VISIBLE_DEVICES` 环境变量，检测逻辑会受到约束：

**文件：`python/ray/_private/accelerators/nvidia_gpu.py:31-44`**

```python
@staticmethod
def get_current_process_visible_accelerator_ids() -> Optional[List[str]]:
    cuda_visible_devices = os.environ.get(
        NvidiaGPUAcceleratorManager.get_visible_accelerator_ids_env_var(), None
    )
    if cuda_visible_devices is None:
        return None       # 未设置 → 返回 None（不约束）
    if cuda_visible_devices == "":
        return []         # 空字符串 → 无可见 GPU
    if cuda_visible_devices == "NoDevFiles":
        return []         # 特殊值 → 无可见 GPU
    return list(cuda_visible_devices.split(","))
    # "2,5,7,9" → ["2", "5", "7", "9"]
```

在 `_get_current_node_accelerator()` 中的约束逻辑：

```python
if num_accelerators is None:
    num_accelerators = accelerator_manager.get_current_node_num_accelerators()
    # NVML 检测到 4 张 GPU

    visible_accelerator_ids = (
        accelerator_manager.get_current_process_visible_accelerator_ids()
    )
    if visible_accelerator_ids is not None:
        num_accelerators = min(num_accelerators, len(visible_accelerator_ids))
        # CUDA_VISIBLE_DEVICES="2,5" → min(4, 2) = 2
        # ★ 只注册 2 张 GPU 到 Ray
```

**示例场景**：

| 场景 | NVML 检测 | CUDA_VISIBLE_DEVICES | 注册到 Ray |
|------|----------|---------------------|-----------|
| 未设置环境变量 | 4 | 无 | 4 |
| `CUDA_VISIBLE_DEVICES=0,1` | 4 | ["0","1"] | 2 |
| `CUDA_VISIBLE_DEVICES=2,5,7,9` | 4 | ["2","5","7","9"] | 4 |
| `CUDA_VISIBLE_DEVICES=""` | 4 | [] | 0 |
| `ray.init(num_gpus=2)` | 4 | 无 | 2（用户指定优先）|

---

## 三、第 2 步：写入 ResourceAndLabelSpec 并转换为资源字典

### 3.1 _resolve_accelerator_resources()：写入 num_gpus

**文件：`python/ray/_private/resource_and_label_spec.py:336-372`**

```python
def _resolve_accelerator_resources(self, accelerator_manager, num_accelerators):
    """Detect and update accelerator resources on a node."""
    if not accelerator_manager:
        return

    accelerator_resource_name = accelerator_manager.get_resource_name()
    visible_accelerator_ids = (
        accelerator_manager.get_current_process_visible_accelerator_ids()
    )

    # ★ 校验：请求的 GPU 数不能超过 CUDA_VISIBLE_DEVICES 中的数量
    if (
        num_accelerators is not None
        and visible_accelerator_ids is not None
        and num_accelerators > len(visible_accelerator_ids)
    ):
        raise ValueError(
            f"Attempting to start raylet with {num_accelerators} "
            f"{accelerator_resource_name}, "
            f"but {accelerator_manager.get_visible_accelerator_ids_env_var()} "
            f"contains {visible_accelerator_ids}."
        )

    # ★ 关键：写入 self.num_gpus
    if accelerator_resource_name == "GPU":
        self.num_gpus = num_accelerators   # self.num_gpus = 4
    else:
        self.resources[accelerator_resource_name] = num_accelerators
```

### 3.2 加速器型号注册为自定义资源

```python
    # ★ 注册加速器型号为自定义资源（用于调度匹配）
    accelerator_type = accelerator_manager.get_current_node_accelerator_type()
    # 例如 accelerator_type = "A100"
    if accelerator_type:
        self.resources[f"{RESOURCE_CONSTRAINT_PREFIX}{accelerator_type}"] = 1
        # self.resources["accelerator_type:A100"] = 1.0

    additional_resources = (
        accelerator_manager.get_current_node_additional_resources()
    )
    if additional_resources:
        self.resources.update(additional_resources)
```

这样用户可以通过 `@ray.remote(resources={"accelerator_type:A100": 1})` 将任务调度到特定型号的 GPU 节点。

### 3.3 to_resource_dict()：转换为字典

**文件：`python/ray/_private/resource_and_label_spec.py:75-125`**

```python
def to_resource_dict(self):
    """Returns a dict suitable to pass to raylet initialization.

    This renames num_cpus / num_gpus to "CPU" / "GPU",
    and check types and values.
    """
    assert self.resolved()

    resources = dict(
        self.resources,                   # 自定义资源 + accelerator_type:A100
        CPU=self.num_cpus,                # 16
        GPU=self.num_gpus,                # 4   ★★★
        memory=int(self.memory),          # 107374182400
        object_store_memory=int(self.object_store_memory),
    )

    # 过滤掉值为 0 的资源
    resources = {
        resource_label: resource_quantity
        for resource_label, resource_quantity in resources.items()
        if resource_quantity != 0
    }

    # 类型校验
    for resource_label, resource_quantity in resources.items():
        assert isinstance(resource_quantity, int) or isinstance(
            resource_quantity, float
        ), f"{resource_label} ({type(resource_quantity)}): {resource_quantity}"
        if isinstance(resource_quantity, float) and not resource_quantity.is_integer():
            raise ValueError(
                "Resource quantities must all be whole numbers. "
                "Violated by resource '{}' in {}.".format(resource_label, resources)
            )
        if resource_quantity < 0:
            raise ValueError(
                "Resource quantities must be nonnegative. "
                "Violated by resource '{}' in {}.".format(resource_label, resources)
            )
        if resource_quantity > ray_constants.MAX_RESOURCE_QUANTITY:
            raise ValueError(
                "Resource quantities must be at most {}. "
                "Violated by resource '{}' in {}.".format(
                    ray_constants.MAX_RESOURCE_QUANTITY, resource_label, resources
                )
            )

    return resources
    # 返回: {"CPU": 16, "GPU": 4, "memory": 107374182400,
    #         "object_store_memory": ..., "accelerator_type:A100": 1,
    #         "node:10.0.0.1": 1}
```

**注意**：`_resolve_resources()` 还会自动添加节点 ID 资源和 head 节点资源：

**文件：`python/ray/_private/resource_and_label_spec.py:202-248`**

```python
def _resolve_resources(self, is_head, node_ip_address=None):
    # 加载环境变量覆盖
    env_resources = ResourceAndLabelSpec._load_env_resources()
    (num_cpus, num_gpus, memory, object_store_memory, merged_resources) = \
        ResourceAndLabelSpec._merge_resources(env_resources, self.resources or {})

    self.num_cpus = self.num_cpus if num_cpus is None else num_cpus
    self.num_gpus = self.num_gpus if num_gpus is None else num_gpus
    self.resources = merged_resources

    if node_ip_address is None:
        node_ip_address = ray.util.get_node_ip_address()

    # ★ 自动为每个节点创建一个 node ID 资源
    self.resources[NODE_ID_PREFIX + node_ip_address] = 1.0
    # self.resources["node:10.0.0.1"] = 1.0

    # ★ Head 节点自动添加 HEAD_NODE_RESOURCE_NAME
    if is_head:
        self.resources[HEAD_NODE_RESOURCE_NAME] = 1.0
        # self.resources["head_node"] = 1.0

    # ★ Auto-detect CPU count if not explicitly set
    if self.num_cpus is None:
        self.num_cpus = ray._private.utils.get_num_cpus()
```

---

## 四、第 3 步：通过命令行参数传递给 Raylet 进程

**文件：`python/ray/_private/services.py:1543-1907`** — `start_raylet()` 函数

```python
def start_raylet(
    redis_address, gcs_address, node_id, node_ip_address,
    node_manager_port, raylet_name, plasma_store_name,
    cluster_id, worker_path, setup_worker_path,
    temp_dir, session_dir, resource_dir, log_dir,
    resource_and_label_spec,   # ← 包含 GPU 数量信息
    plasma_directory, fallback_directory, object_store_memory,
    session_name, is_head_node,
    resource_isolation_config,
    ...
):
    # ★ 获取静态资源字典
    static_resources = resource_and_label_spec.to_resource_dict()
    # {"CPU": 16, "GPU": 4, "memory": 107374182400, ...}

    labels = resource_and_label_spec.labels

    # 限制并行启动的 Worker 数量
    num_cpus_static = static_resources.get("CPU", 0)
    maximum_startup_concurrency = max(
        1, min(multiprocessing.cpu_count(), num_cpus_static)
    )

    # ★ 格式化为命令行参数: 'CPU,16,GPU,4,Custom,3'
    resource_argument = ",".join(
        ["{},{}".format(*kv) for kv in static_resources.items()]
    )
```

最终拼入 Raylet 启动命令：

```python
    command = [
        raylet_executable,
        ...
        f"--static_resource_list={resource_argument}",
        # --static_resource_list=CPU,16,GPU,4,memory,107374182400,...
        ...
        f"--labels={labels_json_str}",
    ]
```

**实际命令行示例**：

```bash
/ray/cpp/default_no_redis/bin/raylet \
  --raylet_socket_name=/tmp/ray/session_xxx/sockets/raylet \
  --store_socket_name=/tmp/ray/session_xxx/sockets/plasma_store \
  --node_ip_address=10.0.0.1 \
  --node_manager_port=12345 \
  --static_resource_list=CPU,16,GPU,4,memory,107374182400,object_store_memory,53687091200,accelerator_type:A100,1,node:10.0.0.1,1 \
  --labels={"accelerator_type":"A100"} \
  ...
```

---

## 五、第 4 步：Raylet C++ 解析命令行参数

**文件：`src/ray/raylet/main.cc:548-565`**

```cpp
// 解析 "--static_resource_list=CPU,16,GPU,4,memory,..."
std::istringstream resource_string(static_resource_list);
std::string resource_name;
std::string resource_quantity;

while (std::getline(resource_string, resource_name, ',')) {
    RAY_CHECK(std::getline(resource_string, resource_quantity, ','));
    static_resource_conf[resource_name] = std::stod(resource_quantity);
    // static_resource_conf["CPU"] = 16.0
    // static_resource_conf["GPU"] = 4.0    ★★★
    // static_resource_conf["memory"] = 107374182400.0
    // static_resource_conf["accelerator_type:A100"] = 1.0
    // static_resource_conf["node:10.0.0.1"] = 1.0
}

auto num_cpus_it = static_resource_conf.find("CPU");
int num_cpus = num_cpus_it != static_resource_conf.end()
                   ? static_cast<int>(num_cpus_it->second)
                   : 0;

node_manager_config.resource_config = ray::ResourceSet(static_resource_conf);
// ResourceSet 内部存储: {"CPU": 16.0, "GPU": 4.0, "memory": ...}
RAY_LOG(DEBUG) << "Starting raylet with static resource configuration: "
               << node_manager_config.resource_config.DebugString();
```

此时 GPU 资源还是标量 `4.0`，尚未展开为实例数组。

---

## 六、第 5 步：创建 ClusterResourceScheduler — GPU: 4.0 → [1.0, 1.0, 1.0, 1.0]

### 6.1 ResourceMapToNodeResources()：标量到 NodeResourceSet

**文件：`src/ray/raylet/main.cc:871-874`**

```cpp
cluster_resource_scheduler = std::make_unique<ray::ClusterResourceScheduler>(
    main_service,
    ray::scheduling::NodeID(raylet_node_id.Binary()),
    node_manager_config.resource_config.GetResourceMap(),
    // ↑ 传入 {"CPU": 16.0, "GPU": 4.0, "memory": ...}
    /*is_node_available_fn*/
    [&](ray::scheduling::NodeID id) {
        return gcs_client->Nodes().IsNodeAlive(ray::NodeID::FromBinary(id.Binary()));
    },
    resource_usage_gauge,
    ...
);
```

**文件：`src/ray/raylet/scheduling/cluster_resource_scheduler.cc:43-62`**

```cpp
ClusterResourceScheduler::ClusterResourceScheduler(
    instrumented_io_context &io_service,
    scheduling::NodeID local_node_id,
    const absl::flat_hash_map<std::string, double> &local_node_resources,
    std::function<bool(scheduling::NodeID)> is_node_available_fn,
    ray::observability::MetricInterface &resource_usage_gauge,
    std::function<int64_t(void)> get_used_object_store_memory,
    std::function<bool(void)> get_pull_manager_at_capacity,
    std::function<void(const rpc::NodeDeathInfo &)> shutdown_raylet_gracefully,
    const absl::flat_hash_map<std::string, std::string> &local_node_labels)
    : local_node_id_(local_node_id), is_node_available_fn_(is_node_available_fn) {

  // ★ 关键转换：{"GPU": 4.0} → NodeResources
  NodeResources node_resources = ResourceMapToNodeResources(
      local_node_resources, local_node_resources, local_node_labels);

  Init(io_service, node_resources,
       get_used_object_store_memory,
       get_pull_manager_at_capacity,
       shutdown_raylet_gracefully,
       resource_usage_gauge);
}
```

**文件：`src/ray/common/scheduling/cluster_resource_data.cc:51-60`**

```cpp
NodeResources ResourceMapToNodeResources(
    const absl::flat_hash_map<std::string, double> &resource_map_total,
    const absl::flat_hash_map<std::string, double> &resource_map_available,
    const absl::flat_hash_map<std::string, std::string> &node_labels) {
  NodeResources node_resources;
  node_resources.total = NodeResourceSet(resource_map_total);
  // ↑ NodeResourceSet({"GPU": 4.0})
  node_resources.available = NodeResourceSet(resource_map_available);
  // ↑ 初始时 total == available（全部空闲）
  node_resources.labels = node_labels;
  return node_resources;
}
```

### 6.2 NodeResourceSet 构造：仍为标量存储

**文件：`src/ray/common/scheduling/resource_set.cc:171-176`**

```cpp
NodeResourceSet::NodeResourceSet(
    const absl::flat_hash_map<std::string, double> &resource_map) {
  for (auto const &[name, quantity] : resource_map) {
    Set(ResourceID(name), FixedPoint(quantity));
    // resources_["GPU"] = 4.0   ← 还是一个标量
  }
}
```

此时 GPU 仍然存储为标量 `4.0`。

### 6.3 LocalResourceManager 初始化：核心转换

**文件：`src/ray/raylet/scheduling/cluster_resource_scheduler.cc:64-83`**

```cpp
void ClusterResourceScheduler::Init(
    instrumented_io_context &io_service,
    const NodeResources &local_node_resources, ...) {

  cluster_resource_manager_ = std::make_unique<ClusterResourceManager>(io_service);

  // ★★★ 创建 LocalResourceManager — 在这里发生核心转换
  local_resource_manager_ = std::make_unique<LocalResourceManager>(
      local_node_id_,
      local_node_resources,    // ← NodeResources，包含 NodeResourceSet
      get_used_object_store_memory,
      get_pull_manager_at_capacity,
      shutdown_raylet_gracefully,
      [this](const NodeResources &local_resource_update) {
        cluster_resource_manager_->AddOrUpdateNode(local_node_id_, local_resource_update);
      },
      resource_usage_gauge);

  RAY_CHECK(!local_node_id_.IsNil());

  // 将本地节点资源注册到集群资源管理器
  cluster_resource_manager_->AddOrUpdateNode(local_node_id_, local_node_resources);
  ...
}
```

**文件：`src/ray/raylet/scheduling/local_resource_manager.cc:31-56`**

```cpp
LocalResourceManager::LocalResourceManager(
    scheduling::NodeID local_node_id,
    const NodeResources &node_resources,
    std::function<int64_t(void)> get_used_object_store_memory,
    std::function<bool(void)> get_pull_manager_at_capacity,
    std::function<void(const rpc::NodeDeathInfo &)> shutdown_raylet_gracefully,
    std::function<void(const NodeResources &)> resource_change_subscriber,
    ray::observability::MetricInterface &resource_usage_gauge,
    std::function<absl::Time()> now_fn)
    : local_node_id_(local_node_id),
      now_fn_(now_fn ? std::move(now_fn) : []() { return absl::Now(); }),
      ... {

  RAY_CHECK(node_resources.total == node_resources.available);

  // ★★★ 关键：NodeResourceSet → NodeResourceInstanceSet
  // 这里把 GPU: 4.0 展开为 [1.0, 1.0, 1.0, 1.0]
  local_resources_.available = NodeResourceInstanceSet(node_resources.total);
  local_resources_.total = NodeResourceInstanceSet(node_resources.total);
  local_resources_.labels = node_resources.labels;

  const auto now = now_fn_();
  for (const auto &resource_id : node_resources.total.ExplicitResourceIds()) {
    idle_time_states_[resource_id] = IdleTimeState{now, absl::nullopt};
  }

  RAY_LOG(DEBUG) << "local resources: " << local_resources_.DebugString();
}
```

### 6.4 NodeResourceInstanceSet 构造函数：展开算法

**文件：`src/ray/common/scheduling/resource_instance_set.cc:29-43`**

```cpp
NodeResourceInstanceSet::NodeResourceInstanceSet(const NodeResourceSet &total) {
  for (auto &resource_id : total.ExplicitResourceIds()) {
    std::vector<FixedPoint> instances;
    auto value = total.Get(resource_id);

    if (resource_id.IsUnitInstanceResource()) {
      // ★ GPU 是 Unit Instance Resource！
      // value = 4.0
      size_t num_instances = static_cast<size_t>(value.Double());  // 4
      for (size_t i = 0; i < num_instances; i++) {
        instances.push_back(1.0);
      }
      // instances = [1.0, 1.0, 1.0, 1.0]
      // 索引 0 → GPU 0, 索引 1 → GPU 1, 索引 2 → GPU 2, 索引 3 → GPU 3
    } else {
      // CPU、memory 等非 Unit Instance 资源，保持标量
      instances.push_back(value);
      // CPU: instances = [16.0]
      // memory: instances = [107374182400.0]
    }

    Set(resource_id, instances);
  }
}
```

**这就是 `GPU: 4.0` → `[1.0, 1.0, 1.0, 1.0]` 的核心展开逻辑。**

数组索引即 GPU 编号：
- `[0]` → GPU 0
- `[1]` → GPU 1
- `[2]` → GPU 2
- `[3]` → GPU 3

值 `1.0` 表示空闲，`0` 表示已分配。

### 6.5 IsUnitInstanceResource() 判断依据

**文件：`src/ray/common/scheduling/scheduling_ids.h:158-164`**

```cpp
class ResourceID : public BaseSchedulingID<SchedulingIDTag::Resource> {
 public:
  /// Whether this resource is a unit-instance resource.
  bool IsUnitInstanceResource() const {
    return UnitInstanceResources().contains(id_);
  }
  ...
};
```

**文件：`src/ray/common/scheduling/scheduling_ids.cc:89-115`**

```cpp
absl::flat_hash_set<int64_t> &ResourceID::UnitInstanceResources() {
  static absl::flat_hash_set<int64_t> set{[]() {
    absl::flat_hash_set<int64_t> res;

    // ★ 从 RayConfig 读取预定义的 Unit Instance 资源
    // 默认值是 "GPU"
    std::string predefined_unit_instance_resources =
        RayConfig::instance().predefined_unit_instance_resources();
    if (!predefined_unit_instance_resources.empty()) {
      std::vector<std::string> results;
      boost::split(results, predefined_unit_instance_resources, boost::is_any_of(","));
      for (std::string &result : results) {
        int64_t resource_id = ResourceID(result).ToInt();
        RAY_CHECK(resource_id < PredefinedResourcesEnum_MAX)
            << result << " is not a valid predefined resource.";
        res.insert(resource_id);
      }
    }

    // ★ 从 RayConfig 读取自定义的 Unit Instance 资源
    // 默认值是 "neuron_cores,TPU,NPU,HPU,RBLN"
    std::string custom_unit_instance_resources =
        RayConfig::instance().custom_unit_instance_resources();
    if (!custom_unit_instance_resources.empty()) {
      std::vector<std::string> results;
      boost::split(results, custom_unit_instance_resources, boost::is_any_of(","));
      for (std::string &result : results) {
        int64_t resource_id = scheduling::ResourceID(result).ToInt();
        res.insert(resource_id);
      }
    }
    return res;
  }()};
  return set;
}
```

**文件：`src/ray/common/ray_config_def.h:775-785`**

```cpp
/// The scheduler will treat these predefined resource types as unit_instance.
/// Default predefined_unit_instance_resources is "GPU".
/// When set it to "CPU,GPU", we will also treat CPU as unit_instance.
RAY_CONFIG(std::string, predefined_unit_instance_resources, "GPU")

/// The scheduler will treat these custom resource types as unit_instance.
/// This allows the scheduler to provide chip IDs for custom resources like
/// "neuron_cores", "TPUs" and "FPGAs".
/// Default custom_unit_instance_resources is "neuron_cores,TPU".
/// When set it to "neuron_cores,TPU,FPGA", we will also treat FPGA as unit_instance.
RAY_CONFIG(std::string, custom_unit_instance_resources, "neuron_cores,TPU,NPU,HPU,RBLN")
```

**默认 Unit Instance Resource 列表**：

| 类型 | 资源名称 | 说明 |
|------|---------|------|
| 预定义 | `GPU` | NVIDIA/AMD/Intel/Metax GPU |
| 自定义 | `neuron_cores` | AWS Neuron 加速器 |
| 自定义 | `TPU` | Google TPU |
| 自定义 | `NPU` | 华为 NPU |
| 自定义 | `HPU` | Intel Habana HPU |
| 自定义 | `RBLN` | Rebellions RBLN |

### 6.6 Unit Instance Resource 的设计意义

Ray 区分了两种资源模型：

| 资源类型 | 模型 | 存储方式 | 调度粒度 | 举例 |
|---------|------|---------|---------|------|
| **Unit Instance** | 每个实例独立、不可分割 | `[1.0, 1.0, 1.0, 1.0]` | 按实例 ID 分配 | GPU, TPU, Neuron |
| **Scalar** | 标量总量 | `[16.0]` | 按数量分配（可分数） | CPU, memory |

**为什么 GPU 要用 Unit Instance**：

1. **GPU 不可共享**：一张 GPU 同时只能被一个 CUDA context 使用，不能像 CPU 那样时间片轮转
2. **需要明确 ID**：调度时必须知道分配了"哪张 GPU"，以便设置 `CUDA_VISIBLE_DEVICES`
3. **不可跨实例分割**：不能把 GPU 0 的一半给 Task A、另一半给 Task B（虽然 Ray 支持分数 GPU `num_gpus=0.5`，但那是在同一实例上分配容量，不是切分硬件）

如果想把 CPU 也设为 Unit Instance（让每个 CPU 核有独立 ID），可以设置环境变量：

```bash
RAY_predefined_unit_instance_resources=CPU,GPU
```

这样 CPU 也会被展开为 `[1.0, 1.0, ..., 1.0]`，每个核有独立 ID。

---

## 七、第 6 步：Raylet 调度——分配具体的 GPU 实例

### 7.1 调度入口：AllocateLocalTaskResources()

当 Worker 向 Raylet 请求 Lease 时，Raylet 做资源分配：

**文件：`src/ray/raylet/scheduling/local_lease_manager.cc:354-359`**

```cpp
auto allocated_instances = std::make_shared<TaskResourceInstances>();
bool schedulable =
    !cluster_resource_scheduler_.GetLocalResourceManager().IsLocalNodeDraining() &&
    cluster_resource_scheduler_.GetLocalResourceManager()
        .AllocateLocalTaskResources(spec.GetRequiredResources().GetResourceMap(),
                                     allocated_instances);
if (!schedulable) {
    ReleaseLeaseArgs(lease_id);
    bool did_spill = TrySpillback(work, is_infeasible);
    ...
}
```

**文件：`src/ray/raylet/scheduling/local_resource_manager.cc:90-105`**

```cpp
bool LocalResourceManager::AllocateTaskResourceInstances(
    const ResourceRequest &resource_request,
    std::shared_ptr<TaskResourceInstances> task_allocation) {
  RAY_CHECK(task_allocation != nullptr);
  auto allocation =
      local_resources_.available.TryAllocate(resource_request.GetResourceSet());
  if (allocation) {
    *task_allocation = TaskResourceInstances(*allocation);  // ← 保存分配结果
    for (const auto &resource_id : resource_request.ResourceIds()) {
      SetResourceNonIdle(resource_id);
    }
    return true;
  } else {
    return false;
  }
}
```

### 7.2 核心分配算法：TryAllocate()

**文件：`src/ray/common/scheduling/resource_instance_set.cc:139-293`** — 带 Placement Group 的分配入口

```cpp
std::optional<absl::flat_hash_map<ResourceID, std::vector<FixedPoint>>>
NodeResourceInstanceSet::TryAllocate(const ResourceSet &resource_demands) {
  absl::flat_hash_map<ResourceID, std::vector<FixedPoint>> allocations;

  // 处理 Placement Group 相关的逻辑（详见后文）
  // ...

  // 对于非 PG 的资源（如普通的 GPU 请求），直接调用单资源分配
  for (const auto &[resource_id, demand] : resource_demands.Resources()) {
    auto data = ParsePgFormattedResource(resource_id.Binary(),
                                         /*for_wildcard_resource*/ true,
                                         /*for_indexed_resource*/ true);
    if (data) {
      // PG 资源处理...
    } else {
      // ★ 直接分配非 PG 资源
      auto allocation = TryAllocate(resource_id, demand);
      if (allocation) {
        allocations[resource_id] = std::move(*allocation);
      } else {
        // 分配失败，回滚
        for (const auto &[id, allocated] : allocations) {
          Free(id, allocated);
        }
        return std::nullopt;
      }
    }
  }
  // ...
}
```

**文件：`src/ray/common/scheduling/resource_instance_set.cc:295-367`** — 单资源分配算法

```cpp
std::optional<std::vector<FixedPoint>> NodeResourceInstanceSet::TryAllocate(
    ResourceID resource_id, FixedPoint demand) {
  std::vector<FixedPoint> available = Get(resource_id);
  // available = [1.0, 1.0, 1.0, 1.0]  (4张GPU，全部空闲)
  if (available.empty()) {
    return std::nullopt;
  }

  std::vector<FixedPoint> allocation(available.size());
  // allocation = [0, 0, 0, 0]
  FixedPoint remaining_demand = demand;  // demand = 1.0 (num_gpus=1)

  if (available.size() == 1) {
    // 只有一个实例的资源（如 CPU、memory）
    if (available[0] >= remaining_demand) {
      available[0] -= remaining_demand;
      allocation[0] = remaining_demand;
      Set(resource_id, std::move(available));
      return std::make_optional<std::vector<FixedPoint>>(std::move(allocation));
    } else {
      return std::nullopt;
    }
  }

  // ★★★ 多实例资源（如 GPU）的分配算法 ★★★
  // If resources has multiple instances, each instance has total capacity of 1.
  //
  // As long as remaining_demand is greater than 1.,
  // allocate full unit-capacity instances until the remaining_demand becomes fractional.
  // Then try to find the best fit for the fractional remaining_resources.

  if (remaining_demand >= 1.) {
    for (size_t i = 0; i < available.size(); i++) {
      if (available[i] == 1.) {
        // ★ 找到第一个空闲的 GPU 实例，分配它
        allocation[i] = 1.;       // allocation = [1.0, 0, 0, 0]
        available[i] = 0;        // available = [0, 1.0, 1.0, 1.0]
        remaining_demand -= 1.;  // remaining_demand = 0
      }
      if (remaining_demand < 1.) {
        break;  // ★ 需求已满足，停止遍历
      }
    }
  }

  if (remaining_demand >= 1.) {
    // Cannot satisfy a demand greater than one if no unit capacity resource is available.
    return std::nullopt;
  }

  // 处理分数 GPU 请求（best-fit 算法，见下文）...

  Set(resource_id, std::move(available));  // ★ 更新可用资源
  return std::make_optional<std::vector<FixedPoint>>(std::move(allocation));
}
```

### 7.3 分数 GPU 分配的 best-fit 算法

当 `num_gpus=0.5` 等分数请求时，Ray 使用 best-fit 策略——选择剩余可用容量最小但仍满足需求的实例：

```cpp
  // Remaining demand is fractional. Find the best fit, if exists.
  if (remaining_demand > 0.) {
    int64_t idx_best_fit = -1;
    FixedPoint available_best_fit = 1.;
    for (size_t i = 0; i < available.size(); i++) {
      if (available[i] >= remaining_demand) {
        if (idx_best_fit == -1 ||
            (available[i] - remaining_demand < available_best_fit)) {
          available_best_fit = available[i] - remaining_demand;
          idx_best_fit = static_cast<int64_t>(i);
        }
      }
    }
    if (idx_best_fit == -1) {
      return std::nullopt;
    } else {
      allocation[idx_best_fit] = remaining_demand;
      available[idx_best_fit] -= remaining_demand;
    }
  }
```

**best-fit 示例**：

假设 `available = [0.5, 0.3, 1.0, 0.5]`，请求 `num_gpus=0.5`：

| 实例 | available | 分配后剩余 | 适合？ |
|------|----------|-----------|-------|
| 0 | 0.5 | 0.0 | ✓ 剩余最小 |
| 1 | 0.3 | -0.2 | ✗ 不够 |
| 2 | 1.0 | 0.5 | ✓ |
| 3 | 0.5 | 0.0 | ✓ 剩余最小 |

best-fit 选择实例 0（或 3），因为分配后剩余最小（0.0），减少碎片。

### 7.4 分配结果示例

假设 4 个 Actor 依次请求 `num_gpus=1`：

| 请求 | available (分配前) | allocation 返回 | available (分配后) | 含义 |
|------|-------------------|----------------|-------------------|------|
| Actor 1 | [1,1,1,1] | [1,0,0,0] | [0,1,1,1] | 分配到 GPU 0 |
| Actor 2 | [0,1,1,1] | [0,1,0,0] | [0,0,1,1] | 分配到 GPU 1 |
| Actor 3 | [0,0,1,1] | [0,0,1,0] | [0,0,0,1] | 分配到 GPU 2 |
| Actor 4 | [0,0,0,1] | [0,0,0,1] | [0,0,0,0] | 分配到 GPU 3 |
| Actor 5 | [0,0,0,0] | nullopt | - | 分配失败，无可用 GPU |

**关键**：allocation 数组的**索引就是 GPU 的编号**，值为 1.0 表示该 GPU 被分配。

---

## 八、第 7 步：Raylet 将 GPU 实例 ID 通过 RPC 返回给 Core Worker

**文件：`src/ray/raylet/scheduling/local_lease_manager.cc:1002-1027`**

```cpp
// Update our internal view of the cluster state.
std::shared_ptr<TaskResourceInstances> allocated_resources;
if (lease_spec.IsActorCreationTask()) {
    allocated_resources = worker->GetLifetimeAllocatedInstances();
    // ★ Actor 创建任务：生命周期资源（Actor 存活期间一直持有）
} else {
    allocated_resources = worker->GetAllocatedInstances();
    // ★ 普通 task：任务级资源（task 完成后释放）
}

// 遍历所有已分配的资源类型
for (auto &resource_id : allocated_resources->ResourceIds()) {
    auto instances = allocated_resources->Get(resource_id);
    // resource_id = "GPU"
    // instances = [1.0, 0, 0, 0]  (以分配到 GPU 0 为例)

    for (const auto &reply_callback : reply_callbacks) {
        ::ray::rpc::ResourceMapEntry *resource = nullptr;
        for (size_t inst_idx = 0; inst_idx < instances.size(); inst_idx++) {
            if (instances[inst_idx] > 0.) {  // ★ 只返回被分配的实例
                if (resource == nullptr) {
                    resource = reply_callback.reply_->add_resource_mapping();
                    resource->set_name(resource_id.Binary());  // "GPU"
                }
                auto rid = resource->add_resource_ids();
                rid->set_index(inst_idx);                    // ★ GPU 编号 = 0
                rid->set_quantity(instances[inst_idx].Double()); // ★ 数量 = 1.0
            }
        }
    }
}
// Send the result back to the clients.
for (const auto &reply_callback : reply_callbacks) {
    reply_callback.send_reply_callback_(Status::OK(), nullptr, nullptr);
}
```

**RPC 返回的 protobuf 数据**（以分配到 GPU 0 为例）：

```protobuf
resource_mapping {
  name: "GPU"
  resource_ids {
    index: 0        # ← GPU 编号 0
    quantity: 1.0   # ← 分配了 1.0 个
  }
}
```

**多 GPU 分配示例**（`num_gpus=2`，分配到 GPU 1 和 GPU 3）：

```protobuf
resource_mapping {
  name: "GPU"
  resource_ids {
    index: 1
    quantity: 1.0
  }
  resource_ids {
    index: 3
    quantity: 1.0
  }
}
```

**资源释放** — `local_resource_manager.cc:107-127`：

```cpp
void LocalResourceManager::FreeTaskResourceInstances(
    std::shared_ptr<TaskResourceInstances> task_allocation, bool record_idle_resource) {
  RAY_CHECK(task_allocation != nullptr);
  for (auto &resource_id : task_allocation->ResourceIds()) {
    if (!local_resources_.total.Has(resource_id)) {
      continue;
    }
    local_resources_.available.Free(resource_id, task_allocation->Get(resource_id));
    // ★ 将 GPU 实例的 available 恢复
    // 例如 available[0] 从 0 恢复到 1.0

    const auto &available = local_resources_.available.Get(resource_id);
    const auto &total = local_resources_.total.Get(resource_id);
    bool is_idle = true;
    for (size_t i = 0; i < total.size(); ++i) {
      RAY_CHECK_GE(total[i], available[i]);
      is_idle = is_idle && (available[i] == total[i]);
    }
    if (record_idle_resource && is_idle) {
      SetResourceIdle(resource_id);
    }
  }
}
```

---

## 九、第 8 步：Core Worker 接收并存储 GPU 分配信息

### 9.1 提交端：PushNormalTask 携带 resource_mapping

**文件：`src/ray/core_worker/task_submission/normal_task_submitter.cc:534-549`**

Core Worker 提交任务时，把 `resource_mapping` 放进 PushTaskRequest：

```cpp
void NormalTaskSubmitter::PushNormalTask(
    const rpc::Address &addr,
    std::shared_ptr<rpc::CoreWorkerClientInterface> client,
    const SchedulingKey &scheduling_key,
    TaskSpecification task_spec,
    const google::protobuf::RepeatedPtrField<rpc::ResourceMapEntry> &assigned_resources) {
  auto task_id = task_spec.TaskId();
  auto request = std::make_unique<rpc::PushTaskRequest>();
  // NOTE: CopyFrom is needed because if we use Swap here and the task
  // fails, then the task data will be gone when the TaskManager attempts to
  // access the task.
  request->mutable_task_spec()->CopyFrom(task_spec.GetMessage());
  request->mutable_resource_mapping()->CopyFrom(assigned_resources);
  // ★ 携带 GPU 分配信息
  request->set_intended_worker_id(addr.worker_id());
  ...
}
```

Lease 获取时 `assigned_resources` 的来源：

**文件：`src/ray/core_worker/task_submission/normal_task_submitter.cc:97-112`**

```cpp
void NormalTaskSubmitter::AddWorkerLeaseClient(
    const rpc::Address &worker_address,
    const rpc::Address &raylet_address,
    const google::protobuf::RepeatedPtrField<rpc::ResourceMapEntry> &assigned_resources,
    const SchedulingKey &scheduling_key,
    const LeaseID &lease_id) {
  core_worker_client_pool_->GetOrConnect(worker_address);
  int64_t expiration = current_time_ms() + lease_timeout_ms_;
  LeaseEntry new_lease_entry{
      raylet_address, expiration, assigned_resources, scheduling_key, lease_id};
  // ★ assigned_resources 保存到 LeaseEntry
  worker_to_lease_entry_.emplace(worker_address, new_lease_entry);
  ...
}
```

### 9.2 执行端：TaskReceiver 解析 resource_mapping

**文件：`src/ray/core_worker/task_execution/task_receiver.cc:144-171`**

Worker 收到 PushTaskRequest 后，解析 `resource_mapping`：

```cpp
void TaskReceiver::QueueTaskForExecution(rpc::PushTaskRequest request,
                                         rpc::PushTaskReply *reply,
                                         rpc::SendReplyCallback send_reply_callback) {
  TaskSpecification task_spec =
      TaskSpecification(std::move(*request.mutable_task_spec()));

  if (stopping_) {
    reply->set_was_cancelled_before_running(true);
    if (task_spec.IsActorTask()) {
      reply->set_worker_exiting(true);
    }
    send_reply_callback(Status::OK(), nullptr, nullptr);
    return;
  }

  // ★★★ Only assign resources for non-actor tasks.
  // Actor tasks inherit the resources assigned at initial actor creation time.
  std::optional<ResourceMappingType> resource_ids;
  if (!task_spec.IsActorTask()) {
    resource_ids.emplace();
    for (auto &mapping : *request.mutable_resource_mapping()) {
      std::vector<std::pair<int64_t, double>> rids;
      rids.reserve(mapping.resource_ids().size());
      for (const auto &ids : mapping.resource_ids()) {
        rids.emplace_back(ids.index(), ids.quantity());
        // ★ ids.index() = 0 (GPU编号), ids.quantity() = 1.0
      }
      resource_ids->emplace(std::move(*mapping.mutable_name()), std::move(rids));
      // ★ resource_ids = {"GPU": [(0, 1.0)]}
    }
  }

  auto execute_callback =
      [this, reply, send_reply_callback, resource_ids = std::move(resource_ids)](
          const TaskSpecification &t) mutable {
        TaskExecutionResult result;
        auto status = task_handler_(t,
                                    std::move(resource_ids),  // ← 传给 ExecuteTask
                                    &result.return_objects,
                                    ...);
        ...
      };
  ...
}
```

### 9.3 存入 CoreWorker::resource_ids_

**文件：`src/ray/core_worker/core_worker.cc:2993-3083`**

```cpp
Status CoreWorker::ExecuteTask(
    const TaskSpecification &task_spec,
    std::optional<ResourceMappingType> resource_ids,
    std::vector<std::pair<ObjectID, std::shared_ptr<RayObject>>> *return_objects,
    ...) {
  ...
  {
    absl::MutexLock lock(&mutex_);
    running_tasks_.emplace(task_spec.TaskId(), task_spec);
    if (resource_ids.has_value()) {
      resource_ids_ = std::move(*resource_ids);
      // ★ resource_ids_ = {"GPU": [(0, 1.0)]}
      // ★ GPU 编号 0 被持久化到 core worker 内存
    }
  }
  ...
}
```

**查询接口**：

```cpp
// core_worker.cc:2897-2900
ResourceMappingType CoreWorker::GetResourceIDs() const {
  absl::MutexLock lock(&mutex_);
  return resource_ids_;
}
```

**Cython 绑定**：`python/ray/_raylet.pyx:3896-3916`

```python
def resource_ids(self):
    cdef:
        ResourceMappingType resource_mapping = (
            CCoreWorkerProcess.GetCoreWorker().GetResourceIDs())
        unordered_map[
            c_string, c_vector[pair[int64_t, double]]
        ].iterator iterator = resource_mapping.begin()
        c_vector[pair[int64_t, double]] c_value

    resources_dict = {}
    while iterator != resource_mapping.end():
        key = decode(dereference(iterator).first)
        c_value = dereference(iterator).second
        ids_and_fractions = []
        for i in range(c_value.size()):
            ids_and_fractions.append(
                (c_value[i].first, c_value[i].second))
            # ★ c_value[i].first = GPU 编号 (int64_t)
            # ★ c_value[i].second = 分配数量 (double)
        resources_dict[key] = ids_and_fractions
        postincrement(iterator)

    return resources_dict
    # 返回: {"GPU": [(0, 1.0)]}
```

---

## 十、第 9 步：Python 层设置 CUDA_VISIBLE_DEVICES

### 10.1 Worker 初始化时记录原始 CUDA_VISIBLE_DEVICES

**文件：`python/ray/_private/worker.py:468-473`**

```python
# When the worker is constructed. Record the original value of the
# (CUDA_VISIBLE_DEVICES, ONEAPI_DEVICE_SELECTOR, HIP_VISIBLE_DEVICES,
# NEURON_RT_VISIBLE_CORES, TPU_VISIBLE_CHIPS, ..) environment variables.
self.original_visible_accelerator_ids = (
    ray._private.utils.get_visible_accelerator_ids()
)
```

**文件：`python/ray/_private/utils.py:210-224`**

```python
def get_visible_accelerator_ids() -> Mapping[str, Optional[List[str]]]:
    """Get the mapping from accelerator resource name
    to the visible ids."""
    from ray._private.accelerators import (
        get_accelerator_manager_for_resource,
        get_all_accelerator_resource_names,
    )

    return {
        accelerator_resource_name: get_accelerator_manager_for_resource(
            accelerator_resource_name
        ).get_current_process_visible_accelerator_ids()
        # ★ 对 NVIDIA GPU，读取 os.environ["CUDA_VISIBLE_DEVICES"]
        # 如果环境变量存在且为 "0,1,2,3"，返回 ["0", "1", "2", "3"]
        # 如果不存在，返回 None
        for accelerator_resource_name in get_all_accelerator_resource_names()
    }
```

### 10.2 Task 执行前设置 CUDA_VISIBLE_DEVICES

**文件：`python/ray/_raylet.pyx:2061-2070`**

```python
# Automatically restrict the GPUs (CUDA), neuron_core, TPU accelerator
# runtime_ids, OMP_NUM_THREADS to restrict availability to this task.
# Once actor is created, users can change the visible accelerator ids within
# an actor task and we don't want to reset it.
if (<int>task_type != <int>TASK_TYPE_ACTOR_TASK):
    original_visible_accelerator_env_vars = ray._private.utils.set_visible_accelerator_ids()
    omp_num_threads_overriden = ray._private.utils.set_omp_num_threads_if_unset()
else:
    original_visible_accelerator_env_vars = None
    omp_num_threads_overriden = False
```

**文件：`python/ray/_private/utils.py:267-294`**

```python
def set_visible_accelerator_ids() -> Mapping[str, Optional[str]]:
    """Set (CUDA_VISIBLE_DEVICES, ONEAPI_DEVICE_SELECTOR, HIP_VISIBLE_DEVICES,
    NEURON_RT_VISIBLE_CORES, TPU_VISIBLE_CHIPS , HABANA_VISIBLE_MODULES ,...)
    environment variables based on the accelerator runtime. Return the original
    environment variables.
    """
    from ray._private.ray_constants import env_bool

    original_visible_accelerator_env_vars = {}
    override_on_zero = env_bool(
        ray._private.accelerators.RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO_ENV_VAR,
        True,
    )
    for resource_name, accelerator_ids in (
        ray.get_runtime_context().get_accelerator_ids().items()
    ):
        # accelerator_ids = {"GPU": ["0"]}  (分配到 GPU 0)
        # If no accelerator ids are set, skip overriding the environment variable.
        if not override_on_zero and len(accelerator_ids) == 0:
            continue
        env_var = ray._private.accelerators.get_accelerator_manager_for_resource(
            resource_name
        ).get_visible_accelerator_ids_env_var()
        # ★ env_var = "CUDA_VISIBLE_DEVICES"
        original_visible_accelerator_env_vars[env_var] = os.environ.get(env_var, None)
        # ★ 保存原始值

        ray._private.accelerators.get_accelerator_manager_for_resource(
            resource_name
        ).set_current_process_visible_accelerator_ids(accelerator_ids)
        # ★ 设置 CUDA_VISIBLE_DEVICES = "0"
    return original_visible_accelerator_env_vars
```

### 10.3 从 Core Worker 获取 GPU ID 的中间层

**文件：`python/ray/runtime_context.py:566-586`**

```python
def get_accelerator_ids(self) -> Dict[str, List[str]]:
    """
    Get the current worker's visible accelerator ids.

    Returns:
        A dictionary keyed by the accelerator resource name. The values are a list
        of ids `{'GPU': ['0', '1'], 'neuron_cores': ['0', '1'],
        'TPU': ['0', '1']}`.
    """
    worker = self.worker
    worker.check_connected()
    ids_dict: Dict[str, List[str]] = {}
    for (
        accelerator_resource_name
    ) in ray._private.accelerators.get_all_accelerator_resource_names():
        accelerator_ids = worker.get_accelerator_ids_for_accelerator_resource(
            accelerator_resource_name,
            f"^{accelerator_resource_name}_group_[0-9A-Za-z]+$",
        )
        ids_dict[accelerator_resource_name] = [str(id) for id in accelerator_ids]
    return ids_dict
    # 返回: {"GPU": ["0"]}
```

**文件：`python/ray/_private/worker.py:1084-1130`**

```python
def get_accelerator_ids_for_accelerator_resource(
    self, resource_name: str, resource_regex: str
) -> Union[List[str], List[int]]:
    """Get the accelerator IDs that are assigned to the given accelerator resource."""
    resource_ids = self.core_worker.resource_ids()
    # ★ resource_ids 来自 C++ CoreWorker::GetResourceIDs()
    # 返回: {"GPU": [(0, 1.0)]}  → index=0, quantity=1.0

    assigned_ids = set()
    # Handle both normal and placement group accelerator resources.
    # Note: We should only get the accelerator ids from the placement
    # group resource that does not contain the bundle index!
    import re

    for resource, assignment in resource_ids.items():
        if resource == resource_name or re.match(resource_regex, resource):
            for resource_id, _ in assignment:
                assigned_ids.add(resource_id)
                # ★ resource_id = 0 (GPU 编号)

    # ★★★ 关键：如果用户启动时设置了 CUDA_VISIBLE_DEVICES
    # 需要映射回原始 ID
    if self.original_visible_accelerator_ids.get(resource_name, None) is not None:
        original_ids = self.original_visible_accelerator_ids[resource_name]
        # original_ids = ["2", "5", "7", "9"]  (假设原始 CUDA_VISIBLE_DEVICES="2,5,7,9")
        assigned_ids = {str(original_ids[i]) for i in assigned_ids}
        # ★ Ray 内部的 GPU 0 → 实际物理 GPU 2
        # ★ Ray 内部的 GPU 1 → 实际物理 GPU 5
        # 返回: {"GPU": ["2"]}  而不是 ["0"]

        # Give all accelerator ids in local_mode.
        if self.mode == LOCAL_MODE:
            if resource_name == ray_constants.GPU:
                max_accelerators = self.node.get_resource_and_label_spec().num_gpus
            else:
                max_accelerators = (
                    self.node.get_resource_and_label_spec().resources.get(
                        resource_name, None
                    )
                )
            if max_accelerators:
                assigned_ids = original_ids[:max_accelerators]
    return list(assigned_ids)
```

### 10.4 CUDA_VISIBLE_DEVICES 的双重映射

当用户启动 Ray 时如果设置了 `CUDA_VISIBLE_DEVICES="2,5,7,9"`：

- Ray 看到的逻辑 GPU 索引：`0, 1, 2, 3`（对应物理 GPU `2, 5, 7, 9`）
- NVML 检测到 4 张 GPU，但 `CUDA_VISIBLE_DEVICES` 只暴露了 4 张（2,5,7,9）
- Raylet 分配的是逻辑索引，如 `index=1`
- `worker.py:1117` 做映射：`original_ids[1]` = `"5"`
- 最终 `CUDA_VISIBLE_DEVICES` 被设为 `"5"` 而不是 `"1"`

**映射流程图**：

```
物理 GPU:       0   1   2   3   4   5   6   7   8   9
CUDA_VISIBLE:                   ✓       ✓       ✓       ✓
                                    ↓
Ray 逻辑索引:   0   1   2   3
映射关系:       0→2 1→5 2→7 3→9

Raylet 分配 index=1 → 映射 → CUDA_VISIBLE_DEVICES="5"
Raylet 分配 index=0 → 映射 → CUDA_VISIBLE_DEVICES="2"
```

### 10.5 最终设置环境变量

**文件：`python/ray/_private/accelerators/nvidia_gpu.py:92-101`**

```python
@staticmethod
def set_current_process_visible_accelerator_ids(
    visible_cuda_devices: List[str],
) -> None:
    if env_bool(NOSET_CUDA_VISIBLE_DEVICES_ENV_VAR, False):
        return  # ★ 可以通过 RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES=1 跳过

    os.environ["CUDA_VISIBLE_DEVICES"] = ",".join(
        [str(i) for i in visible_cuda_devices]
    )
    # ★ 最终结果: os.environ["CUDA_VISIBLE_DEVICES"] = "0"  (只看到 GPU 0)
```

设置后，用户代码中的 PyTorch/CUDA 只能看到被分配的 GPU：

```python
import torch
torch.cuda.device_count()  # 1 (只看到1张)
torch.cuda.current_device()  # 0 (CUDA重新编号后的0，对应物理GPU 0)
```

### 10.6 Task 完成后重置环境变量

**文件：`python/ray/_raylet.pyx:2169-2179`**

```python
    finally:
        with current_task_id_lock:
            current_task_id = None

        if (<int>task_type == <int>TASK_TYPE_NORMAL_TASK):
            if original_visible_accelerator_env_vars:
                # ★ 普通 task 完成后恢复 CUDA_VISIBLE_DEVICES
                ray._private.utils.reset_visible_accelerator_env_vars(
                    original_visible_accelerator_env_vars
                )
            if omp_num_threads_overriden:
                # Reset the OMP_NUM_THREADS environ if it was set.
                os.environ.pop("OMP_NUM_THREADS", None)
```

**文件：`python/ray/_private/utils.py:297-305`**

```python
def reset_visible_accelerator_env_vars(
    original_visible_accelerator_env_vars: Mapping[str, Optional[str]]
) -> None:
    """Reset the visible accelerator env vars to the original values."""
    for env_var, env_value in original_visible_accelerator_env_vars.items():
        if env_value is None:
            os.environ.pop(env_var, None)
        else:
            os.environ[env_var] = env_value
```

---

## 十一、Ray Data 中的 GPU 使用

### 11.1 用户代码传入 num_gpus

**文件：`python/ray/data/dataset.py:806-810`**

```python
if num_gpus is not None:
    ray_remote_args["num_gpus"] = num_gpus   # ← 用户传入 num_gpus=1
```

**文件：`python/ray/data/_internal/util.py:1752-1776`**

```python
def merge_resources_to_ray_remote_args(
    num_cpus: Optional[int],
    num_gpus: Optional[int],
    memory: Optional[int],
    ray_remote_args: Dict[str, Any],
) -> Dict[str, Any]:
    """Convert the given resources to Ray remote args."""
    ray_remote_args = ray_remote_args.copy()
    if num_cpus is not None:
        ray_remote_args["num_cpus"] = num_cpus
    if num_gpus is not None:
        ray_remote_args["num_gpus"] = num_gpus
    if memory is not None:
        ray_remote_args["memory"] = memory
    return ray_remote_args
```

### 11.2 Ray Data 创建 Actor 时的资源声明

**文件：`python/ray/data/_internal/execution/operators/actor_pool_map_operator.py:252`**

```python
# ray_remote_args 包含 {"num_gpus": 1, ...}
self._actor_cls = ray.remote(**self._ray_remote_args)(self._map_worker_cls)
```

**每个 Actor 的资源用量记录**：`actor_pool_map_operator.py:162-164`

```python
per_actor_resource_usage = ExecutionResources(
    cpu=self._ray_remote_args.get("num_cpus"),
    gpu=self._ray_remote_args.get("num_gpus"),  # ← 1.0
    memory=self._ray_remote_args.get("memory"),
)
```

**GPU 资源预算计算**：`actor_pool_map_operator.py:500-513`

```python
num_gpus_per_actor = self._ray_remote_args.get("num_gpus", 0)

min_actors = compute_strategy.min_size
max_actors = compute_strategy.max_size

self._min_resource_usage = ExecutionResources(
    cpu=num_cpus_per_actor * min_actors,
    gpu=num_gpus_per_actor * min_actors,     # ★ 最小 GPU 预算
    ...
)

self._max_resource_usage = ExecutionResources(
    cpu=0 if num_cpus_per_actor == 0 else num_cpus_per_actor * max_actors,
    gpu=0 if num_gpus_per_actor == 0 else num_gpus_per_actor * max_actors,  # ★ 最大 GPU 预算
    ...
)
```

**Ray Data 的 GPU 数量限制**：`python/ray/data/_internal/util.py:264`

```python
cluster_gpus = int(ray.cluster_resources().get("GPU", 0))
```

**默认资源策略**：`map_operator.py:951-953`

```python
if "num_cpus" not in ray_remote_args and "num_gpus" not in ray_remote_args:
    ray_remote_args["num_cpus"] = 1
    # ★ 如果用户没有指定 num_cpus 也没有指定 num_gpus，默认分配 1 CPU
```

### 11.3 Ray Data 本身不直接管理 CUDA_VISIBLE_DEVICES

Ray Data 通过 `num_gpus` 参数传给 `ray.remote()`，最终走的是 Ray Core 的同一套 GPU 分配机制。Ray Data 本身不直接设置 `CUDA_VISIBLE_DEVICES`。

搜索结果证实：

```bash
# 在 Ray Data 代码中搜索 CUDA_VISIBLE_DEVICES
$ grep -rn "CUDA_VISIBLE_DEVICES\|set_visible_accelerator" python/ray/data/ --include="*.py"
# 无结果！
```

---

## 十二、关键细节总结

### 12.1 Actor Task 不重设 CUDA_VISIBLE_DEVICES

**文件：`python/ray/_raylet.pyx:2065`**

```python
if (<int>task_type != <int>TASK_TYPE_ACTOR_TASK):
    original_visible_accelerator_env_vars = ray._private.utils.set_visible_accelerator_ids()
```

只有 `task_type != ACTOR_TASK` 时才调用 `set_visible_accelerator_ids()`。Actor 的 GPU 绑定在**创建时**（ACTOR_CREATION_TASK）就确定了，之后所有 Actor method 调用沿用同一个 GPU，不会切换。

| Task 类型 | 设置 CUDA_VISIBLE_DEVICES | 完成后重置 |
|-----------|--------------------------|-----------|
| NORMAL_TASK | ✓ | ✓ |
| ACTOR_CREATION_TASK | ✓ | ✗（Actor 生命周期持有）|
| ACTOR_TASK | ✗（沿用 Actor 创建时的绑定）| ✗ |

### 12.2 普通 Task 完成后重置

普通 task 的 Worker 进程会被复用。Task A 用 GPU 0 执行完毕后，`CUDA_VISIBLE_DEVICES` 被恢复，Task B 可能被分配到 GPU 2，此时 `CUDA_VISIBLE_DEVICES` 被设为 `"2"`。

### 12.3 Placement Group 场景下的 GPU 分配

**文件：`src/ray/common/scheduling/resource_instance_set.cc:140-180`** 中的注释详细解释了 PG 场景：

```
// For example, considering the GPU resource on a host.
// Assuming the host has 3 GPUs and 1 placement group with 2 bundles.
// The bundle with index 1 contains 1 GPU and
// the bundle with index 2 contains 2 GPU.
//
// The current node resource can be as follows:
// resource id: total, available
// GPU: [1, 1, 1], [0, 0, 0]
// GPU_<pg_id>: [1, 1, 1], [1, 1, 1]
// GPU_1_<pg_id>: [1, 0, 0], [1, 0, 0]
// GPU_2_<pg_id>: [0, 1, 1], [0, 1, 1]
//
// Now, we want to allocate a task with 2 GPUs and in the placement group <pg_id>,
// reflecting in the following resource demand:
// GPU_<pg_id> : 2
//
// We will iterate though all the bundles in the placement group and bundle with
// index=2 has the required capacity. So we will allocate the task to the 2 GPUs in
// bundle 2 in placement group <pg_id> and the same allocation should be reflected in
// the wildcard GPU resource. So the allocation will be:
// GPU_<pg_id> : [0, 1, 1]
// GPU_2_<pg_id> : [0, 1, 1]
//
// And as a result, after the allocation, current node resource will be:
// resource id: total, available
// GPU: [1, 1, 1], [0, 0, 0]
// GPU_<pg_id>: [1, 1, 1], [1, 0, 0]
// GPU_1_<pg_id>: [1, 0, 0], [1, 0, 0]
// GPU_2_<pg_id>: [0, 1, 1], [0, 0, 0]
```

**关键约束**：PG 资源分配不能跨越 bundle。如果请求 2 GPU 且 PG bundle 2 有 2 GPU 可用，则分配到 GPU 1 和 GPU 2（bundle 2 对应的实例）。

### 12.4 资源模型对比表

| 资源类型 | 模型 | 存储方式 | 调度粒度 | 是否 Unit Instance | 举例 |
|---------|------|---------|---------|-------------------|------|
| GPU | Unit Instance | `[1.0, 1.0, 1.0, 1.0]` | 按实例 ID 分配 | ✓ | NVIDIA/AMD/Intel GPU |
| TPU | Unit Instance | `[1.0, 1.0, 1.0, 1.0]` | 按实例 ID 分配 | ✓ | Google TPU |
| Neuron | Unit Instance | `[1.0, 1.0]` | 按实例 ID 分配 | ✓ | AWS Neuron |
| CPU | Scalar | `[16.0]` | 按数量分配（可分数） | ✗ | CPU 核心 |
| memory | Scalar | `[107374182400.0]` | 按数量分配 | ✗ | 内存 |
| object_store_memory | Scalar | `[53687091200.0]` | 按数量分配 | ✗ | 对象存储内存 |

---

## 十三、最终 Raylet 内存状态

初始化完成后，Raylet 内存中 `local_resources_` 的状态（4 GPU 节点）：

```
local_resources_.total:
  CPU:  [16.0]                           ← 非 Unit Instance，保持标量
  GPU:  [1.0, 1.0, 1.0, 1.0]            ← Unit Instance，4个独立实例
  memory: [107374182400.0]               ← 非 Unit Instance

local_resources_.available:
  CPU:  [16.0]                           ← 初始时 total == available
  GPU:  [1.0, 1.0, 1.0, 1.0]            ← 4张GPU全部空闲
  memory: [107374182400.0]
```

当 Task 请求 `num_gpus=1` 后（分配到 GPU 0）：

```
local_resources_.available:
  CPU:  [16.0]
  GPU:  [0, 1.0, 1.0, 1.0]              ← GPU 0 被占用
  memory: [107374182400.0]
```

当 4 个 Actor 各请求 `num_gpus=1` 后：

```
local_resources_.available:
  CPU:  [16.0]
  GPU:  [0, 0, 0, 0]                    ← 全部 GPU 被占用
  memory: [107374182400.0]
```

当 Task 完成、GPU 释放后：

```
local_resources_.available:
  CPU:  [16.0]
  GPU:  [1.0, 0, 0, 0]                  ← GPU 0 释放，可再次分配
  memory: [107374182400.0]
```


 完整的 GPU 编号与分配代码逻辑详解
  
  以一个 4 GPU 节点为例，假设你调用 ds.map_batches(fn, num_gpus=1)，以下是完整的 6 步代码链路：

  ---
  第 1 步：节点启动时检测 GPU 数量
  
  入口：python/ray/_private/node.py:550-561

  def get_resource_and_label_spec(self):
      if not self._resource_and_label_spec:
          self._resource_and_label_spec = ResourceAndLabelSpec(
              self._ray_params.num_cpus,
              self._ray_params.num_gpus,    # ← 如果用户没有指定 num_gpus，这里为 None
              ...
          ).resolve(is_head=self.head, node_ip_address=self.node_ip_address)
      return self._resource_and_label_spec

  当 num_gpus=None 时，resolve() 方法会自动调用 GPU 检测。

  GPU 检测实现：python/ray/_private/accelerators/nvidia_gpu.py:46-56

  @staticmethod
  def get_current_node_num_accelerators() -> int:
      import ray._private.thirdparty.pynvml as pynvml
      try:
          pynvml.nvmlInit()
      except pynvml.NVMLError:
          return 0
      device_count = pynvml.nvmlDeviceGetCount()  # ← 通过 NVML 获取物理 GPU 数量
      pynvml.nvmlShutdown()
      return device_count

  不同 GPU 厂商的自动选择：python/ray/_private/accelerators/__init__.py:60-68

  # 延迟初始化，按优先级检测 GPU 厂商
  if AMDGPUAcceleratorManager.get_current_node_num_accelerators() > 0:
      resource_name_to_accelerator_manager["GPU"] = AMDGPUAcceleratorManager
  elif IntelGPUAcceleratorManager.get_current_node_num_accelerators() > 0:
      resource_name_to_accelerator_manager["GPU"] = IntelGPUAcceleratorManager
  elif MetaxGPUAcceleratorManager.get_current_node_num_accelerators() > 0:
      resource_name_to_accelerator_manager["GPU"] = MetaxGPUAcceleratorManager
  else:
      resource_name_to_accelerator_manager["GPU"] = NvidiaGPUAcceleratorManager

  结果：假设检测到 4 张 GPU，节点向 GCS 注册资源 GPU: 4.0。Ray 内部将 GPU 表示为 4 个实例，每个容量 1.0：

  GPU 实例: [1.0, 1.0, 1.0, 1.0]  # 索引 0, 1, 2, 3 对应物理 GPU 0, 1, 2, 3

  ---
  第 2 步：Ray Data 用户代码传入 num_gpus
  
  入口：python/ray/data/dataset.py:806-810

  if num_gpus is not None:
      ray_remote_args["num_gpus"] = num_gpus   # ← 用户传入 num_gpus=1

  Ray Data 创建 Actor 时的资源声明：python/ray/data/_internal/execution/operators/actor_pool_map_operator.py:252

  # ray_remote_args 包含 {"num_gpus": 1, ...}
  self._actor_cls = ray.remote(**self._ray_remote_args)(self._map_worker_cls)

  每个 Actor 的资源用量记录：actor_pool_map_operator.py:162-164

  per_actor_resource_usage = ExecutionResources(
      cpu=self._ray_remote_args.get("num_cpus"),
      gpu=self._ray_remote_args.get("num_gpus"),  # ← 1.0
      memory=self._ray_remote_args.get("memory"),
  )

  这一步只是声明每个 Actor 需要 1 个 GPU，还没有实际分配。

  ---
  第 3 步：Raylet 调度——分配具体的 GPU 实例
  
  当 Worker 向 Raylet 请求 Lease 时，Raylet 做资源分配：

  入口：src/ray/raylet/scheduling/local_lease_manager.cc:354-359

  auto allocated_instances = std::make_shared<TaskResourceInstances>();
  bool schedulable =
      !cluster_resource_scheduler_.GetLocalResourceManager().IsLocalNodeDraining() &&
      cluster_resource_scheduler_.GetLocalResourceManager()
          .AllocateLocalTaskResources(spec.GetRequiredResources().GetResourceMap(),
                                       allocated_instances);

  实际分配函数：src/ray/raylet/scheduling/local_resource_manager.cc:90-105

  bool LocalResourceManager::AllocateTaskResourceInstances(
      const ResourceRequest &resource_request,
      std::shared_ptr<TaskResourceInstances> task_allocation) {
    RAY_CHECK(task_allocation != nullptr);
    auto allocation =
        local_resources_.available.TryAllocate(resource_request.GetResourceSet());
    if (allocation) {
      *task_allocation = TaskResourceInstances(*allocation);  // ← 保存分配结果
      for (const auto &resource_id : resource_request.ResourceIds()) {
        SetResourceNonIdle(resource_id);
      }
      return true;
    } else {
      return false;
    }
  }

  核心分配算法：src/ray/common/scheduling/resource_instance_set.cc:295-367

  std::optional<std::vector<FixedPoint>> NodeResourceInstanceSet::TryAllocate(
      ResourceID resource_id, FixedPoint demand) {
    std::vector<FixedPoint> available = Get(resource_id);
    // available = [1.0, 1.0, 1.0, 1.0]  (4张GPU，全部空闲)
    std::vector<FixedPoint> allocation(available.size());
    // allocation = [0, 0, 0, 0]
    FixedPoint remaining_demand = demand;  // demand = 1.0 (num_gpus=1)

    if (remaining_demand >= 1.) {
      for (size_t i = 0; i < available.size(); i++) {
        if (available[i] == 1.) {
          // ★ 找到第一个空闲的 GPU 实例，分配它
          allocation[i] = 1.;       // allocation = [1.0, 0, 0, 0]
          available[i] = 0;        // available = [0, 1.0, 1.0, 1.0]
          remaining_demand -= 1.;  // remaining_demand = 0
        }
        if (remaining_demand < 1.) {
          break;  // ★ 需求已满足，停止遍历
        }
      }
    }

    // 处理分数 GPU 请求（best-fit 算法）...
    // 略

    Set(resource_id, std::move(available));  // ★ 更新可用资源
    return std::make_optional<std::vector<FixedPoint>>(std::move(allocation));
  }

  分配结果示例（假设 4 个 Actor 依次请求 1 GPU）：

  ┌─────────┬─────────────────┬──────────────┐
  │  请求   │ allocation 返回 │     含义     │
  ├─────────┼─────────────────┼──────────────┤
  │ Actor 1 │ [1.0, 0, 0, 0]  │ 分配到 GPU 0 │
  ├─────────┼─────────────────┼──────────────┤
  │ Actor 2 │ [0, 1.0, 0, 0]  │ 分配到 GPU 1 │
  ├─────────┼─────────────────┼──────────────┤
  │ Actor 3 │ [0, 0, 1.0, 0]  │ 分配到 GPU 2 │
  ├─────────┼─────────────────┼──────────────┤
  │ Actor 4 │ [0, 0, 0, 1.0]  │ 分配到 GPU 3 │
  └─────────┴─────────────────┴──────────────┘

  关键：allocation 数组的索引就是 GPU 的编号，值为 1.0 表示该 GPU 被分配。

  ---
  第 4 步：Raylet 将 GPU 实例 ID 通过 RPC 返回给 Core Worker
  
  代码：src/ray/raylet/scheduling/local_lease_manager.cc:1002-1027

  // 取出已分配的资源实例
  std::shared_ptr<TaskResourceInstances> allocated_resources;
  if (lease_spec.IsActorCreationTask()) {
      allocated_resources = worker->GetLifetimeAllocatedInstances();  // Actor 创建任务：生命周期资源
  } else {
      allocated_resources = worker->GetAllocatedInstances();          // 普通 task：任务级资源
  }

  // 遍历所有已分配的资源类型
  for (auto &resource_id : allocated_resources->ResourceIds()) {
      auto instances = allocated_resources->Get(resource_id);
      // resource_id = "GPU"
      // instances = [1.0, 0, 0, 0]  (以分配到 GPU 0 为例)

      for (const auto &reply_callback : reply_callbacks) {
          ::ray::rpc::ResourceMapEntry *resource = nullptr;
          for (size_t inst_idx = 0; inst_idx < instances.size(); inst_idx++) {
              if (instances[inst_idx] > 0.) {  // ★ 只返回被分配的实例
                  if (resource == nullptr) {
                      resource = reply_callback.reply_->add_resource_mapping();
                      resource->set_name(resource_id.Binary());  // "GPU"
                  }
                  auto rid = resource->add_resource_ids();
                  rid->set_index(inst_idx);                    // ★ GPU 编号 = 0
                  rid->set_quantity(instances[inst_idx].Double()); // ★ 数量 = 1.0
              }
          }
      }
  }

  RPC 返回的 protobuf 数据（以分配到 GPU 0 为例）：

  resource_mapping {
    name: "GPU"
    resource_ids {
      index: 0        # ← GPU 编号 0
      quantity: 1.0   # ← 分配了 1.0 个
    }
  }

  ---
  第 5 步：Core Worker 接收并存储 GPU 分配信息
  
  接收端：src/ray/core_worker/task_submission/normal_task_submitter.cc:544-549

  Core Worker 提交任务时，把 resource_mapping 放进 PushTaskRequest：

  void NormalTaskSubmitter::PushNormalTask(...) {
      auto request = std::make_unique<rpc::PushTaskRequest>();
      request->mutable_task_spec()->CopyFrom(task_spec.GetMessage());
      request->mutable_resource_mapping()->CopyFrom(assigned_resources);  // ← 携带 GPU 分配信息
      request->set_intended_worker_id(addr.worker_id());
      ...
  }

  执行端解析：src/ray/core_worker/task_execution/task_receiver.cc:144-171

  Worker 收到 PushTaskRequest 后，解析 resource_mapping：

  void TaskReceiver::QueueTaskForExecution(rpc::PushTaskRequest request, ...) {
      TaskSpecification task_spec = TaskSpecification(std::move(*request.mutable_task_spec()));

      // ★ Actor task 不重新分配资源（沿用 Actor 创建时的分配）
      std::optional<ResourceMappingType> resource_ids;
      if (!task_spec.IsActorTask()) {
          resource_ids.emplace();
          for (auto &mapping : *request.mutable_resource_mapping()) {
              std::vector<std::pair<int64_t, double>> rids;
              rids.reserve(mapping.resource_ids().size());
              for (const auto &ids : mapping.resource_ids()) {
                  rids.emplace_back(ids.index(), ids.quantity());
                  // ★ ids.index() = 0 (GPU编号), ids.quantity() = 1.0
              }
              resource_ids->emplace(std::move(*mapping.mutable_name()), std::move(rids));
              // ★ resource_ids = {"GPU": [(0, 1.0)]}
          }
      }
      ...
  }

  存入 Core Worker：src/ray/core_worker/core_worker.cc:3077-3083

  {
      absl::MutexLock lock(&mutex_);
      running_tasks_.emplace(task_spec.TaskId(), task_spec);
      if (resource_ids.has_value()) {
          resource_ids_ = std::move(*resource_ids);
          // ★ resource_ids_ = {"GPU": [(0, 1.0)]}
      }
  }

  ---
  第 6 步：Python 层设置 CUDA_VISIBLE_DEVICES
  
  6a. Worker 初始化时记录原始 CUDA_VISIBLE_DEVICES

  python/ray/_private/worker.py:468-473

  # Worker 对象构造时，记录原始的环境变量值
  self.original_visible_accelerator_ids = (
      ray._private.utils.get_visible_accelerator_ids()
  )

  python/ray/_private/utils.py:210-224

  def get_visible_accelerator_ids() -> Mapping[str, Optional[List[str]]]:
      return {
          accelerator_resource_name: get_accelerator_manager_for_resource(
              accelerator_resource_name
          ).get_current_process_visible_accelerator_ids()
          # ★ 对 NVIDIA GPU，读取 os.environ["CUDA_VISIBLE_DEVICES"]
          # 如果环境变量存在且为 "0,1,2,3"，返回 ["0", "1", "2", "3"]
          # 如果不存在，返回 None
          for accelerator_resource_name in get_all_accelerator_resource_names()
      }

  6b. Task 执行前设置 CUDA_VISIBLE_DEVICES

  python/ray/_raylet.pyx:2061-2070

  # ★ 只有普通 task 和 actor 创建 task 会设置，actor task 不会重设
  if (<int>task_type != <int>TASK_TYPE_ACTOR_TASK):
      original_visible_accelerator_env_vars = ray._private.utils.set_visible_accelerator_ids()
  else:
      original_visible_accelerator_env_vars = None

  python/ray/_private/utils.py:267-294

  def set_visible_accelerator_ids() -> Mapping[str, Optional[str]]:
      original_visible_accelerator_env_vars = {}
      for resource_name, accelerator_ids in (
          ray.get_runtime_context().get_accelerator_ids().items()
      ):
          # accelerator_ids = {"GPU": ["0"]}  (分配到 GPU 0)
          if not override_on_zero and len(accelerator_ids) == 0:
              continue
          env_var = ray._private.accelerators.get_accelerator_manager_for_resource(
              resource_name
          ).get_visible_accelerator_ids_env_var()
          # ★ env_var = "CUDA_VISIBLE_DEVICES"
          original_visible_accelerator_env_vars[env_var] = os.environ.get(env_var, None)
          # ★ 保存原始值

          ray._private.accelerators.get_accelerator_manager_for_resource(
              resource_name
          ).set_current_process_visible_accelerator_ids(accelerator_ids)
          # ★ 设置 CUDA_VISIBLE_DEVICES = "0"
      return original_visible_accelerator_env_vars

  6c. 从 Core Worker 获取 GPU ID 的中间层

  python/ray/runtime_context.py:566-586

  def get_accelerator_ids(self) -> Dict[str, List[str]]:
      ids_dict: Dict[str, List[str]] = {}
      for accelerator_resource_name in ray._private.accelerators.get_all_accelerator_resource_names():
          accelerator_ids = worker.get_accelerator_ids_for_accelerator_resource(
              accelerator_resource_name,
              f"^{accelerator_resource_name}_group_[0-9A-Za-z]+$",
          )
          ids_dict[accelerator_resource_name] = [str(id) for id in accelerator_ids]
      return ids_dict
      # 返回: {"GPU": ["0"]}

  python/ray/_private/worker.py:1084-1130

  def get_accelerator_ids_for_accelerator_resource(
      self, resource_name: str, resource_regex: str
  ) -> Union[List[str], List[int]]:
      resource_ids = self.core_worker.resource_ids()
      # ★ resource_ids 来自 C++ CoreWorker::GetResourceIDs()
      # 返回: {"GPU": [(0, 1.0)]}  → index=0, quantity=1.0

      assigned_ids = set()
      for resource, assignment in resource_ids.items():
          if resource == resource_name or re.match(resource_regex, resource):
              for resource_id, _ in assignment:
                  assigned_ids.add(resource_id)
                  # ★ resource_id = 0 (GPU 编号)

      # ★ 关键：如果用户启动时设置了 CUDA_VISIBLE_DEVICES
      # 需要映射回原始 ID
      if self.original_visible_accelerator_ids.get(resource_name, None) is not None:
          original_ids = self.original_visible_accelerator_ids[resource_name]
          # original_ids = ["2", "5", "7", "9"]  (假设原始 CUDA_VISIBLE_DEVICES="2,5,7,9")
          assigned_ids = {str(original_ids[i]) for i in assigned_ids}
          # ★ Ray 内部的 GPU 0 → 实际物理 GPU 2
          # ★ Ray 内部的 GPU 1 → 实际物理 GPU 5
          # 返回: {"GPU": ["2"]}  而不是 ["0"]
      return list(assigned_ids)

  6d. 最终设置环境变量

  python/ray/_private/accelerators/nvidia_gpu.py:92-101

  @staticmethod
  def set_current_process_visible_accelerator_ids(
      visible_cuda_devices: List[str],
  ) -> None:
      if env_bool(NOSET_CUDA_VISIBLE_DEVICES_ENV_VAR, False):
          return  # ★ 可以通过 RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES=1 跳过
      os.environ["CUDA_VISIBLE_DEVICES"] = ",".join([str(i) for i in visible_cuda_devices])
      # ★ 最终结果: os.environ["CUDA_VISIBLE_DEVICES"] = "0"  (只看到 GPU 0)

  6e. Task 完成后重置

  python/ray/_raylet.pyx:2173-2179

  if (<int>task_type == <int>TASK_TYPE_NORMAL_TASK):
      if original_visible_accelerator_env_vars:
          # ★ 普通 task 完成后恢复 CUDA_VISIBLE_DEVICES
          ray._private.utils.reset_visible_accelerator_env_vars(
              original_visible_accelerator_env_vars
          )
      if omp_num_threads_overriden:
          os.environ.pop("OMP_NUM_THREADS", None)

  python/ray/_private/utils.py:297-305

  def reset_visible_accelerator_env_vars(
      original_visible_accelerator_env_vars: Mapping[str, Optional[str]]
  ) -> None:
      for env_var, env_value in original_visible_accelerator_env_vars.items():
          if env_value is None:
              os.environ.pop(env_var, None)
          else:
              os.environ[env_var] = env_value

  ---
  完整数据流图
  
  用户代码: ds.map_batches(fn, num_gpus=1)
           │
           ▼
  ┌─────────────────────────────────────────────────────────────────┐
  │ ① Ray Data 层                                                    │
  │    dataset.py:809   ray_remote_args["num_gpus"] = 1              │
  │    actor_pool_map_operator.py:252                                 │
  │      actor_cls = ray.remote(num_gpus=1)(MapWorker)               │
  │      → 声明每个 Actor 需要 1 个 GPU                               │
  └────────────────────────┬────────────────────────────────────────┘
                           │
                           ▼
  ┌─────────────────────────────────────────────────────────────────┐
  │ ② Raylet 调度层                                                   │
  │    local_lease_manager.cc:354-359                                 │
  │      AllocateLocalTaskResources({GPU: 1}, allocated_instances)    │
  │                         │                                         │
  │    local_resource_manager.cc:90-105                               │
  │      available.TryAllocate({GPU: 1})                              │
  │                         │                                         │
  │    resource_instance_set.cc:295-367  ★ 核心算法                    │
  │      available = [1.0, 1.0, 1.0, 1.0]  (4个空闲GPU)              │
  │      demand = 1.0                                                 │
  │      遍历找到第一个 available[i]==1.0 → i=0                       │
  │      allocation = [1.0, 0, 0, 0]  → GPU 0 被分配                 │
  │      available  = [0, 1.0, 1.0, 1.0]  → GPU 0 被标记为占用       │
  └────────────────────────┬────────────────────────────────────────┘
                           │
                           ▼
  ┌─────────────────────────────────────────────────────────────────┐
  │ ③ Raylet → Core Worker RPC 传递                                   │
  │    local_lease_manager.cc:1009-1026                               │
  │      遍历 allocated_resources:                                     │
  │        resource_id = "GPU"                                        │
  │        instances = [1.0, 0, 0, 0]                                 │
  │        inst_idx=0, quantity=1.0 → 只有索引0有值                   │
  │      构造 protobuf:                                               │
  │        resource_mapping {                                         │
  │          name: "GPU"                                              │
  │          resource_ids { index: 0, quantity: 1.0 }                 │
  │        }                                                          │
  └────────────────────────┬────────────────────────────────────────┘
                           │
                           ▼
  ┌─────────────────────────────────────────────────────────────────┐
  │ ④ Core Worker 接收并存储                                           │
  │    task_receiver.cc:160-171                                       │
  │      解析 resource_mapping → resource_ids = {"GPU": [(0, 1.0)]}   │
  │                                                                   │
  │    core_worker.cc:3080-3082                                       │
  │      resource_ids_ = {"GPU": [(0, 1.0)]}                          │
  │      ★ GPU 编号 0 被持久化到 core worker 内存                     │
  └────────────────────────┬────────────────────────────────────────┘
                           │
                           ▼
  ┌─────────────────────────────────────────────────────────────────┐
  │ ⑤ Python 层查询 GPU ID                                            │
  │    runtime_context.py:566-586  get_accelerator_ids()              │
  │      → worker.get_accelerator_ids_for_accelerator_resource("GPU")│
  │                                                                   │
  │    worker.py:1097-1130                                            │
  │      resource_ids = core_worker.resource_ids()                    │
  │        → Cython 调用 C++ CoreWorker::GetResourceIDs()             │
  │        → 返回 {"GPU": [(0, 1.0)]}                                │
  │      assigned_ids = {0}  ← GPU 编号 0                             │
  │                                                                   │
  │      ★ 如果启动时 CUDA_VISIBLE_DEVICES="2,5,7,9"                  │
  │        original_ids = ["2","5","7","9"]                           │
  │        assigned_ids = {str(original_ids[0])} = {"2"}             │
  │        → 返回物理 GPU 2，而不是逻辑索引 0                          │
  └────────────────────────┬────────────────────────────────────────┘
                           │
                           ▼
  ┌─────────────────────────────────────────────────────────────────┐
  │ ⑥ 设置 CUDA_VISIBLE_DEVICES                                      │
  │    _raylet.pyx:2065-2066  (task 执行前)                           │
  │      if task_type != ACTOR_TASK:                                  │
  │        set_visible_accelerator_ids()                              │
  │                                                                   │
  │    utils.py:280-292                                               │
  │      for resource_name, accelerator_ids in                        │
  │          get_accelerator_ids().items():                           │
  │        accelerator_ids = {"GPU": ["0"]}                           │
  │        env_var = "CUDA_VISIBLE_DEVICES"                           │
  │        保存原始值 → original["CUDA_VISIBLE_DEVICES"] = 旧值       │
  │        set_current_process_visible_accelerator_ids(["0"])          │
  │                                                                   │
  │    nvidia_gpu.py:99-101                                           │
  │      os.environ["CUDA_VISIBLE_DEVICES"] = "0"                     │
  │      ★★ 现在 torch.cuda 只能看到 GPU 0 ★★                         │
  │                                                                   │
  │    ─── task 执行用户代码 ───                                       │
  │      torch.cuda.device_count() == 1                               │
  │      torch.cuda.current_device() == 0                             │
  │      (这里的 0 是 CUDA_VISIBLE_DEVICES 重映射后的 0)               │
  │                                                                   │
  │    _raylet.pyx:2173-2176  (task 完成后)                           │
  │      if task_type == NORMAL_TASK:                                 │
  │        reset_visible_accelerator_env_vars(original)               │
  │        ★★ 恢复 CUDA_VISIBLE_DEVICES，Worker 可复用 ★★             │
  └─────────────────────────────────────────────────────────────────┘

  ---
  关键细节补充
  
  1. CUDA_VISIBLE_DEVICES 的双重映射

  当用户启动 Ray 时如果设置了 CUDA_VISIBLE_DEVICES="2,5,7,9"：

  - Ray 看到的逻辑 GPU 索引：0, 1, 2, 3（对应物理 GPU 2, 5, 7, 9）
  - Raylet 分配的是逻辑索引，如 index=1
  - worker.py:1117 做映射：original_ids[1] = "5"
  - 最终 CUDA_VISIBLE_DEVICES 被设为 "5" 而不是 "1"

  2. Actor Task 不重设 CUDA_VISIBLE_DEVICES

  _raylet.pyx:2065：只有 task_type != ACTOR_TASK 时才调用 set_visible_accelerator_ids()。Actor 的 GPU 绑定在创建时（ACTOR_CREATION_TASK）就确定了，之后所有
  Actor method 调用沿用同一个 GPU，不会切换。

  3. 普通 Task 完成后重置

  普通 task 的 Worker 进程会被复用。Task A 用 GPU 0 执行完毕后，CUDA_VISIBLE_DEVICES 被恢复，Task B 可能被分配到 GPU 2，此时 CUDA_VISIBLE_DEVICES 被设为 "2"。

  4. 分数 GPU 分配的 best-fit 算法

  resource_instance_set.cc:344-363：当 num_gpus=0.5 等分数请求时，Ray 使用 best-fit 策略——选择剩余可用容量最小但仍满足需求的实例：

  if (remaining_demand > 0.) {
      int64_t idx_best_fit = -1;
      FixedPoint available_best_fit = 1.;
      for (size_t i = 0; i < available.size(); i++) {
          if (available[i] >= remaining_demand) {
              if (idx_best_fit == -1 ||
                  (available[i] - remaining_demand < available_best_fit)) {
                  available_best_fit = available[i] - remaining_demand;
                  idx_best_fit = static_cast<int64_t>(i);
              }
          }
      }
      // idx_best_fit 就是选中的 GPU 索引
  }