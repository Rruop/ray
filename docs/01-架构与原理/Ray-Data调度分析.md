# Ray Data 调度策略与 Checkpoint 恢复性能分析

## 背景问题

在使用 Ray Data 进行 checkpoint 恢复时，发现：
1. Actor 调度不会均分，会优先调度到 CPU 节点而不是 GPU 节点
2. 从 checkpoint 恢复时，如果 head 节点逻辑 CPU 较少，恢复速度会变慢，卡在 `read_parquet` 阶段
3. `--num-cpus` 配置会影响 `read_parquet` 的性能

本文档详细分析这些问题的根因和解决方案。

---

## 一、Actor 调度策略分析

### 1.1 默认调度策略：Hybrid Policy

Ray Actor 的默认调度策略是 **Hybrid Policy**。

**关键代码位置：**
- `src/ray/raylet/scheduling/policy/hybrid_scheduling_policy.h`
- `src/ray/raylet/scheduling/policy/hybrid_scheduling_policy.cc`
- `src/ray/raylet/scheduling/cluster_resource_scheduler.cc`

### 1.2 GPU 节点回避机制

**配置定义** (`src/ray/common/ray_config_def.h:826-827`):

```cpp
/// Whether to avoid scheduling cpu requests on gpu nodes
RAY_CONFIG(bool, scheduler_avoid_gpu_nodes, true)  // 默认为 true
```

**实现逻辑** (`hybrid_scheduling_policy.cc:183-221`):

```cpp
scheduling::NodeID HybridSchedulingPolicy::Schedule(
    const ResourceRequest &resource_request, SchedulingOptions options) {

  // 如果任务需要 GPU，正常调度到任意节点
  if (!options.avoid_gpu_nodes_ || resource_request.Has(ResourceID::GPU())) {
    return ScheduleImpl(..., NodeFilter::kAny, ...);
  }

  // 第一步：只考虑非 GPU 节点
  auto best_node_id = ScheduleImpl(..., NodeFilter::kNonGpu, ...);
  if (!best_node_id.IsNil()) {
    return best_node_id;  // 找到非 GPU 节点就返回
  }

  // 第二步：回退到所有节点（包括 GPU 节点）
  return ScheduleImpl(..., NodeFilter::kAny, ...);
}
```

**GPU 节点过滤实现** (`hybrid_scheduling_policy.cc:23-42`):

```cpp
bool HybridSchedulingPolicy::IsNodeFeasible(...) {
  if (node_filter != NodeFilter::kAny) {
    const bool has_gpu = node_resources.total.Has(ResourceID::GPU());
    if (node_filter == NodeFilter::kGPU && !has_gpu) {
      return false;  // 只允许 GPU 节点，但这个节点没有 GPU
    } else if (node_filter == NodeFilter::kNonGpu && has_gpu) {
      return false;  // 只允许非 GPU 节点，但这个节点有 GPU -> 过滤掉
    }
  }
  return node_resources.IsFeasible(resource_request);
}
```

### 1.3 设计意图

这个设计是为了**保护 GPU 资源**：

| 场景 | 行为 |
|------|------|
| Actor 需要 GPU | 调度到 GPU 节点 |
| Actor 不需要 GPU | **优先**调度到 CPU 节点 |
| CPU 节点资源不足 | **回退**到 GPU 节点 |

**原因**：GPU 节点通常是稀缺和昂贵的资源，如果让不需要 GPU 的任务占用 GPU 节点的 CPU/内存，可能会导致真正需要 GPU 的任务无法调度。

### 1.4 修改 GPU 节点回避行为

**方法1：修改 Ray 配置**
```python
ray.init(_system_config={"scheduler_avoid_gpu_nodes": False})
```

**方法2：使用 SPREAD 调度策略**
```python
@ray.remote(scheduling_strategy="SPREAD")
class MyActor:
    pass
```

**方法3：使用 Placement Group**
```python
pg = placement_group([{"CPU": 1}] * num_actors, strategy="SPREAD")
actors = [MyActor.options(
    placement_group=pg,
    placement_group_bundle_index=i
).remote() for i in range(num_actors)]
```

---

## 二、Checkpoint 恢复与 read_parquet 调度分析

### 2.1 Checkpoint 恢复流程

```
checkpoint 恢复
    │
    ▼
ray.data.read_parquet()
    │
    ▼
ParquetDatasource.__init__()  [在 Driver 进程执行！]
    │
    ├── _list_files()                    [Driver 端阻塞]
    ├── get_parquet_dataset()            [Driver 端阻塞 - 读取所有 footer]
    └── _fetch_file_infos()              [Ray tasks，可能绑定到 head 节点]
    │
    ▼
repartition(num_blocks=1)               [Shuffle 操作]
    │
    ▼
iter_internal_ref_bundles()             [触发执行]
```

**关键代码** (`python/ray/data/checkpoint/checkpoint_filter.py:222-279`):

```python
def load_checkpoint(self) -> ObjectRef[Block]:
    checkpoint_ds: ray.data.Dataset = ray.data.read_parquet(
        self.checkpoint_path,
        filesystem=self.filesystem,
        partition_filter=self.checkpoint_path_partition_filter,
        override_num_blocks=self.checkpoint_read_override_num_blocks,
    )

    # Repartition to 1 block - 强制所有数据合并
    checkpoint_ds = checkpoint_ds.repartition(num_blocks=1)
```

### 2.2 本地文件 vs 分布式文件系统判断

**判断逻辑** (`python/ray/data/_internal/util.py:349-368`):

```python
_LOCAL_SCHEME = "local"

def _is_local_scheme(paths: Union[str, List[str]]) -> bool:
    num = sum(urllib.parse.urlparse(path).scheme == _LOCAL_SCHEME for path in paths)
    return num == len(paths)
```

对于 HDFS 路径 `/home/ad/renruoyu/checkpoint_test/...`：

```python
>>> urllib.parse.urlparse("/home/ad/renruoyu/checkpoint_test/xxx")
ParseResult(scheme='', netloc='', path='/home/ad/renruoyu/checkpoint_test/xxx', ...)
```

- `scheme = ''`（空字符串）
- `'' != 'local'`
- 所以 `_is_local_scheme()` 返回 **False**
- 因此 `_supports_distributed_reads = True`
- **不会触发 NodeAffinity 硬绑定到 head 节点**

### 2.3 Driver 端阻塞操作

即使使用分布式文件系统，以下操作仍在 **Driver 进程**（head 节点）执行：

**阻塞点1：文件列表展开** (`parquet_datasource.py:343-348`)
```python
listed_files = _list_files(
    paths,
    filesystem,
    partition_filter=partition_filter,
    file_extensions=file_extensions,
)
```

**阻塞点2：PyArrow ParquetDataset 构造** (`parquet_datasource.py:375`)
```python
pq_ds = get_parquet_dataset(list(paths), filesystem, dataset_kwargs)
```

这个调用会：
1. 通过 HDFS filesystem 列出所有 parquet 文件
2. 读取每个文件的 footer 元数据
3. 构建 ParquetFileFragment 列表

**PyArrow 使用物理机 CPU 数**：
```python
import pyarrow as pa
pa.cpu_count()  # 返回物理机的 CPU 核数，不是 Ray 的 --num-cpus
```

---

## 三、locality_with_output 亲和性调度分析

### 3.1 配置说明

**定义** (`python/ray/data/_internal/execution/interfaces/execution_options.py:285-288`):

```python
locality_with_output: Union[bool, List[NodeIdStr]] = False
# Set this to prefer running tasks on the same node as the output node
```

| 配置值 | 效果 |
|--------|------|
| `False` | 使用 SPREAD 策略，分散到所有节点 |
| `True` | 绑定到 driver 节点（head 节点） |
| `[node_ids]` | 轮询绑定到指定节点列表 |

### 3.2 实现逻辑

**代码** (`python/ray/data/_internal/execution/operators/map_operator.py:457-479`):

```python
if options.locality_with_output:
    if isinstance(options.locality_with_output, list):
        locs = options.locality_with_output
    else:
        locs = [ray.get_runtime_context().get_node_id()]  # Driver 所在节点

    class RoundRobinAssign:
        def __init__(self, locs):
            self.locs = locs
            self.i = 0

        def __call__(self, args):
            args = copy.deepcopy(args)
            args["scheduling_strategy"] = NodeAffinitySchedulingStrategy(
                self.locs[self.i],
                soft=True,
                _spill_on_unavailable=True,
            )
            self.i += 1
            self.i %= len(self.locs)
            return args
```

### 3.3 NodeAffinitySchedulingStrategy 参数说明

**Python 定义** (`python/ray/util/scheduling_strategies.py:43-58`):

```python
class NodeAffinitySchedulingStrategy:
    """
    Attributes:
        node_id: 目标节点的 hex id
        soft: 如果目标节点不存在或不可行：
            - soft=False: 任务失败
            - soft=True: 调度到其他节点
        _spill_on_unavailable: 如果目标节点资源暂时不可用：
            - False: 等待资源可用
            - True: spill 到其他节点
    """
```

### 3.4 C++ 层调度逻辑

**代码** (`src/ray/raylet/scheduling/policy/node_affinity_scheduling_policy.cc:20-41`):

```cpp
scheduling::NodeID NodeAffinitySchedulingPolicy::Schedule(...) {
  scheduling::NodeID target_node_id = options.node_affinity_node_id_;

  // 目标节点存在、活着、且资源可行(feasible)
  if (nodes_.contains(target_node_id) && is_node_alive_(target_node_id) &&
      nodes_.at(target_node_id).GetLocalView().IsFeasible(resource_request)) {

    if (!options.node_affinity_spill_on_unavailable_ &&
        !options.node_affinity_fail_on_unavailable_) {
      // 没有设置 spill_on_unavailable → 直接返回目标节点（等待资源）
      return target_node_id;
    } else if (nodes_.at(target_node_id).GetLocalView().IsAvailable(resource_request)) {
      // 设置了 spill_on_unavailable，且资源当前可用 → 返回目标节点
      return target_node_id;
    }
    // 设置了 spill_on_unavailable，但资源当前不可用 → 继续往下走
  }

  // soft=true 时，回退到 Hybrid 调度
  if (!options.node_affinity_soft_) {
    return scheduling::NodeID::Nil();
  }
  options.scheduling_type_ = SchedulingType::HYBRID;
  return hybrid_policy_.Schedule(resource_request, options);
}
```

### 3.5 IsFeasible vs IsAvailable

| 方法 | 含义 |
|------|------|
| `IsFeasible(request)` | 节点是否有足够的**总资源**（不管是否被占用） |
| `IsAvailable(request)` | 节点**当前**是否有空闲资源 |

### 3.6 为什么设置了 _spill_on_unavailable=True 还是会卡住？

可能原因：

1. **任务提交速度太快**：当任务几乎同时提交时，调度器可能还没更新资源视图，导致都调度到 head 节点

2. **资源视图更新延迟**：调度器使用 `GetLocalView()`，这是本地缓存的资源视图，可能有延迟

3. **Head 节点 CPU 成为瓶颈**：即使部分任务 spill 到其他节点，head 节点的有限 CPU 仍然限制了整体吞吐

---

## 四、--num-cpus 与物理 CPU 的区别

| 配置 | 影响范围 |
|------|---------|
| `--num-cpus` | Ray task/actor 调度的资源配额 |
| `os.cpu_count()` | PyArrow、NumPy、Pandas 等库的内部线程池 |

### 4.1 Ray 层面

```bash
ray start --head --num-cpus=4
```

这决定了 Ray 调度器认为该节点有多少 CPU 资源，影响：
- 同时能调度多少个 task
- Actor 的并发度

### 4.2 PyArrow 层面

```python
import pyarrow as pa
pa.cpu_count()      # 返回物理机的 CPU 核数
pa.io_thread_count() # IO 线程数
```

PyArrow 的元数据读取（在 Driver 进程执行）使用物理机 CPU。

### 4.3 为什么 --num-cpus 影响 read_parquet？

当开启了 `locality_with_output=True`：
1. 所有 map/read 任务优先调度到 head 节点
2. Head 节点的 `--num-cpus` 决定并发度
3. CPU 少 → 任务排队 → 看起来"卡住"

---

## 五、问题诊断方法

### 5.1 检查节点资源配置

```python
import ray

for node in ray.nodes():
    print(f"Node: {node['NodeID'][:8]}")
    print(f"  CPU: {node['Resources'].get('CPU', 0)}")
    print(f"  GPU: {node['Resources'].get('GPU', 0)}")
    print(f"  是 GPU 节点: {node['Resources'].get('GPU', 0) > 0}")
```

### 5.2 检查 PyArrow 元数据读取耗时

```python
import time
import pyarrow.parquet as pq
from pyarrow.fs import HadoopFileSystem

hdfs_fs = HadoopFileSystem(host='lt-router.sy', user='ad')
checkpoint_path = "/home/ad/renruoyu/checkpoint_test/..."

print("Step 1: PyArrow ParquetDataset...")
t1 = time.time()
pq_ds = pq.ParquetDataset(checkpoint_path, filesystem=hdfs_fs)
print(f"  Done in {time.time()-t1:.2f}s, files={len(pq_ds.files)}")
```

### 5.3 检查任务分布

```python
import ray
from collections import Counter

@ray.remote(num_cpus=0.5)
def check_node():
    return ray.get_runtime_context().get_node_id()

refs = [check_node.remote() for _ in range(20)]
node_ids = ray.get(refs)
print(Counter(node_ids))
```

### 5.4 检查 locality_with_output 配置

```python
ctx = ray.data.DataContext.get_current()
print(f"locality_with_output: {ctx.execution_options.locality_with_output}")
```

---

## 六、解决方案

### 6.1 关闭亲和性调度（推荐）

```python
ctx = ray.data.DataContext.get_current()
ctx.execution_options.locality_with_output = False
```

### 6.2 指定多个节点做轮询

```python
import ray

node_ids = [node["NodeID"] for node in ray.nodes() if node["Alive"]]
ctx = ray.data.DataContext.get_current()
ctx.execution_options.locality_with_output = node_ids
```

### 6.3 增加 head 节点 CPU

```bash
ray start --head --num-cpus=32
```

### 6.4 关闭 GPU 节点回避

```python
ray.init(_system_config={"scheduler_avoid_gpu_nodes": False})
```

### 6.5 使用 SPREAD 调度策略

```python
ctx = ray.data.DataContext.get_current()
ctx.scheduling_strategy = "SPREAD"
```

### 6.6 减少 checkpoint 文件数量

```python
# 写 checkpoint 时合并成更少的文件
checkpoint_ds.repartition(num_blocks=10).write_parquet(path)
```

### 6.7 设置 PyArrow 线程数

```python
import pyarrow as pa
pa.set_cpu_count(32)
pa.set_io_thread_count(32)
```

---

## 七、关键代码位置索引

| 功能 | 文件路径 |
|------|---------|
| Hybrid 调度策略 | `src/ray/raylet/scheduling/policy/hybrid_scheduling_policy.cc` |
| GPU 节点回避配置 | `src/ray/common/ray_config_def.h:826-827` |
| NodeAffinity 调度策略 | `src/ray/raylet/scheduling/policy/node_affinity_scheduling_policy.cc` |
| Checkpoint 加载 | `python/ray/data/checkpoint/checkpoint_filter.py` |
| Parquet 数据源 | `python/ray/data/_internal/datasource/parquet_datasource.py` |
| locality_with_output 配置 | `python/ray/data/_internal/execution/interfaces/execution_options.py` |
| MapOperator 调度逻辑 | `python/ray/data/_internal/execution/operators/map_operator.py` |
| 本地文件判断 | `python/ray/data/_internal/util.py:349-368` |

---

## 八、总结

### 问题根因

1. **Actor 不均分到 GPU 节点**：`scheduler_avoid_gpu_nodes=true` 默认开启，优先调度到非 GPU 节点

2. **read_parquet 卡住**：
   - 开启了 `locality_with_output=True`
   - 所有任务优先调度到 head 节点
   - Head 节点的 `--num-cpus` 限制了并发度

### 关键配置关系

```
locality_with_output=True
    │
    ▼
NodeAffinitySchedulingStrategy(soft=True, _spill_on_unavailable=True)
    │
    ▼
优先调度到 head 节点
    │
    ▼
受 --num-cpus 限制
    │
    ▼
任务排队，表现为"卡住"
```

### 最佳实践

1. 如果不需要输出亲和性，关闭 `locality_with_output`
2. 如果需要亲和性，指定多个节点做轮询
3. 合理配置 head 节点的 `--num-cpus`
4. 减少 checkpoint 文件数量以降低元数据开销
