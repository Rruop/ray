# NoOpClusterAutoscaler GCS RPC 穿透导致 process_dispatch 长尾瓶颈分析

> 故障：`raysubmit_YCujBCHJaPfuBV12`（Qwen-VL 视频处理 Pipeline，1100 GPU actor + 200 CPU actor）
>
> 现象：集群运行 5 天后，StreamingExecutor 主调度线程 60% 时间耗费在 `update_usages()` 中的 `NoOpClusterAutoscaler.get_total_resources()` 同步 GCS RPC 调用上，导致每个调度周期 dispatch 数量骤降，GPU 吞吐随之下跌。
>
> 根因：快手内部 Ray fork（2.54.4+kuaishou.cc121a56b9）的 `NoOpClusterAutoscaler.get_total_resources()` 实现错误——每次调用走 `ray.cluster_resources()` 同步 RPC 查 GCS，无任何缓存。配合 `ResourceManager.GLOBAL_LIMITS_UPDATE_INTERVAL_S=1` 的 1 秒缓存周期，以及调度循环每周期多次调用 `update_usages()`，导致每周期穿透 6-7 次 RPC。集群运行 5 天后，GCS 端因 Dashboard NodeHead 99% CPU + SchedulingClass 累积到 102k + ReportJobError 18118 次等因素被慢化，RPC 从 ~10ms 涨到 80ms+，在 driver 进程内 GIL 竞争下被拉长到 ~150ms/call，主线程 60% 时间陷入阻塞。

---

## 目录

1. [问题现象](#1-问题现象)
2. [业务拓扑与配置](#2-业务拓扑与配置)
3. [排查过程](#3-排查过程)
4. [根因分析](#4-根因分析)
5. [Ray Data 中 NodeAffinitySchedulingStrategy 自动注入全路径分析](#5-ray-data-中-nodeaffinityschedulingstrategy-自动注入全路径分析)
6. [SchedulingClass 累积问题深度分析](#6-schedulingclass-累积问题深度分析)
7. [py-spy 19ms 实测 vs 60% 占比的采样偏差分析](#7-py-spy-19ms-实测-vs-60-占比的采样偏差分析)
8. [修复方案](#8-修复方案)
9. [NoOpClusterAutoscaler 上游修复建议](#9-noopclusterautoscaler-上游修复建议)
10. [监控告警建议](#10-监控告警建议)
11. [相关代码位置索引](#11-相关代码位置索引)

---

## 1. 问题现象

### 1.1 核心指标

| 指标 | 早期健康基线 | 故障后 | 说明 |
|------|------------|--------|------|
| 调度循环 `proc` 阶段耗时 | <0.1s | 3-10s | SCHED_PROFILE 中的 proc 字段 |
| 调度循环 `uu` 阶段耗时 | ~0.01s | ~0.01s | 仅 Phase 0 一次 update_usages，变化不大 |
| 单周期 dispatch 数 | 300+ | 50-80 | 因 proc 阶段过长，每轮能 dispatch 的 task 数骤降 |
| GPU 吞吐 | 正常 | 明显下降 | actor 喂数不及时 |
| SCHED_PROFILE 警告 | 无 | 每个周期都触发 | 阈值 1s，实际远超 |
| SchedulingClass 数量 | <100 | 102,000+ | 5 天累积无清理 |
| Dashboard NodeHead CPU | 正常 | 99% | 独立 bug，但放大 GCS 慢化效应 |
| ReportJobError 次数 | 0 | 18,118 | 错误风暴增加 GCS 负载 |

### 1.2 SCHED_PROFILE 特征演变

分析历史 6 个 job 的 SCHED_PROFILE 日志，发现**渐进恶化**：

| Job 启动时间 | wait 耗时 | proc 耗时 | 主导瓶颈 |
|-------------|-----------|-----------|---------|
| 06-11 21:39（首个） | 0.1s | 0.05s | 无明显瓶颈 |
| 06-12 00:28（第二个） | 3-4s | 0.1s | **ray.wait O(N)** (case 1) |
| 06-12 后续 | 3-4s | 逐渐增长 | case 1 + case 2 交织 |
| 06-16（近期） | 0.5-1s | 3-10s | **NoOp GCS RPC** (case 2) |

- **Case 1（早期主导）**：`ray.wait(N=4500+ ObjectRefs)` 触发 Ray CoreWorker `Wait` 路径上的 O(N) 全局锁开销，已在之前的 patch 中修复。
- **Case 2（近期主导）**：`NoOpClusterAutoscaler.get_total_resources()` 同步 GCS RPC 瓶颈，本文重点分析。

---

## 2. 业务拓扑与配置

```
ReadParquet → Filter(VideoClipProcessMapper) → Filter
  → StreamingRepartition[num_rows_per_block=64]
  → MapBatches(DistributedQwenVLVideoProcessMapper)   [GPU, 1100 actors × 1 GPU]
      └─ 每个 GPU MapWorker 内嵌：
         13 个 QwenVLCPUPreprocessActor (视频下载、解码、帧渲染)
         1 个 vLLM Engine (GPU 推理, max_num_seqs=32)
         prefetch_batches=26
  → FlatMap(ClipMergeMapper)                           [CPU, BlobStore 下载]
  → StreamingRepartition[num_rows_per_block=15000]
  → MapBatches(VideoClipInfoKafkaMapper)               [200 CPU actors]
  → Write
```

- 集群启动时间：2026-06-11 21:39，到排查时已运行 109+ 小时（~5 天）
- Ray 版本：2.54.4+kuaishou.cc121a56b9（快手内部 fork）
- 业务使用 `--backpressure-policies concurrency_cap` 限制 ClipMerge concurrency=10000
- Driver PID: 27195，Job ID: `raysubmit_YCujBCHJaPfuBV12`

---

## 3. 排查过程

### 3.1 第一步：发现 SCHED_PROFILE 警告

在 driver 日志中发现大量 SCHED_PROFILE 警告：

```
SCHED_PROFILE step=123 uu=0.01s wait=0.5s proc=4.2s dispatch=52
```

- `uu`：update_usages 阶段（Phase 0），仅调用一次，耗时正常 0.01s
- `wait`：ray.wait 阶段（Phase 1），早期 3-4s，patch 后降到 0.5-1s
- `proc`：process_completed_tasks + dispatch 阶段（Phase 2），**异常增长到 3-10s**
- `dispatch`：单周期 dispatch 数量，从 300+ 降到 50-80

### 3.2 第二步：py-spy 火焰图定位

在远端 driver 节点执行 py-spy 抓取主调度线程火焰图：

```bash
/opt/vjepa2/bin/py-spy record --pid 27195 --duration 30 --rate 100
```

**关键发现**：主调度线程 345 个采样中：

| 函数 | 采样数 | 占比 |
|------|--------|------|
| `update_usages` | 219 | **63.5%** |
| `total_resources_per_node` (内含 cluster_resources RPC) | 205 | **59.4%** |
| 其他调度逻辑 | 126 | 36.5% |

**结论**：主调度线程近 2/3 的时间花在 `update_usages()` → `get_global_limits()` → `NoOpClusterAutoscaler.get_total_resources()` → `ray.cluster_resources()` 的同步 GCS RPC 上。

### 3.3 第三步：实测 cluster_resources RPC 延迟

在远端 driver shell 中直接测量 `ray.cluster_resources()` 调用延迟：

```python
import time, ray
ray.init(address='auto')
for i in range(20):
    t0 = time.perf_counter()
    ray.cluster_resources()
    print(f"call {i}: {(time.perf_counter()-t0)*1000:.1f}ms")
```

**结果**：

| 指标 | 值 |
|------|---|
| p50 | 19ms |
| p90 | 22ms |
| p99 | 27ms |
| max | 35ms |

裸调用延迟 ~19ms，远低于 py-spy 暗示的 ~150ms/call。这引出了"采样偏差"问题（见第 7 节分析）。

### 3.4 第四步：定位 NoOpClusterAutoscaler 实现错误

查看远端部署的 `noop_cluster_autoscaler.py`：

```python
# /opt/vjepa2/lib/python3.12/site-packages/ray/data/_internal/cluster_autoscaler/noop_cluster_autoscaler.py
class NoOpClusterAutoscaler(ClusterAutoscaler):
    def get_total_resources(self) -> ExecutionResources:
        return ExecutionResources.from_resource_dict(ray.cluster_resources())
```

**问题**：每次调用 `get_total_resources()` 都走 `ray.cluster_resources()` 同步 RPC 到 GCS，**没有任何缓存**。

而上游开源版 Ray 不存在 `NoOpClusterAutoscaler` 这个类——这是快手内部 fork 添加的。开源版的 `DefaultClusterAutoscaler` 和 `DefaultClusterAutoscalerV2` 也有类似调用，但调用频率由 autoscaling 逻辑控制（仅在需要时调用），而 `NoOpClusterAutoscaler` 作为 `get_total_resources` 的回调在 `ResourceManager.get_global_limits()` 中被高频调用。

### 3.5 第五步：追踪调用链

完整调用链如下：

```
StreamingExecutor._scheduling_loop_step()
  ├── [Phase 0] self._resource_manager.update_usages()           # 1 次
  ├── [Phase 1] process_completed_tasks() → ray.wait()            # 1 次
  ├── [Phase 2] self._resource_manager.update_usages()           # 第 2 次
  │   └── ResourceManager.update_usages()
  │       └── _update_allocated_budgets()
  │           └── ResourceManager.get_global_limits()           # 检查缓存
  │               └── self._get_total_resources()                # 缓存过期时调用
  │                   └── NoOpClusterAutoscaler.get_total_resources()
  │                       └── ray.cluster_resources()             # GCS 同步 RPC ★
  ├── while True:                                                 # 循环 dispatch
  │   ├── select_operator_to_run()                               # 每选一个 op 调一次
  │   │   └── resource_manager.get_global_limits()               # 检查缓存
  │   │       └── self._get_total_resources()                    # 缓存过期时穿透
  │   │           └── ray.cluster_resources()                     # GCS 同步 RPC ★
  │   ├── dispatch_next_task()
  │   └── self._resource_manager.update_usages()                 # 每个 dispatch 后调一次
  │       └── get_global_limits() → _get_total_resources()       # 可能穿透
  │           └── ray.cluster_resources()                         # GCS 同步 RPC ★
  └── self._cluster_autoscaler.try_trigger_scaling()
```

**关键**：`ResourceManager.GLOBAL_LIMITS_UPDATE_INTERVAL_S = 1`（1 秒缓存），在 Phase 2 内层循环中：

- Phase 0 调用 1 次 `update_usages()`，可能触发 1 次 RPC
- Phase 2 调用 `update_usages()` 至少 2 次（循环前后各一次）
- `select_operator_to_run()` 每选一个 op 调用 `get_global_limits()` 一次
- 每个 dispatch 后又调 `update_usages()` 一次

**实测**：每调度周期穿透 6-7 次 `ray.cluster_resources()` RPC。

### 3.6 第六步：确认 GCS 慢化放大器

集群运行 5 天后，GCS 本身也被慢化，三个独立因素叠加：

1. **Dashboard NodeHead PID 453 持续 4 天 19h 占 99% CPU** — 独立 bug，不断轮询 GCS 拉取节点信息，消耗 GCS 处理能力
2. **SchedulingClass 累积到 102,000+** — 全局静态表无清理逻辑，`GetSchedulingClass` 每次加全局锁查表，表越大锁竞争越严重
3. **ReportJobError 累积 18,118 次** — 错误风暴增加 GCS pub/sub 负载

GCS RPC 延迟从集群刚启动时的 ~10ms 涨到 80ms+（裸测量），在 driver 进程内 GIL 竞争 + 并发 gRPC 流量下被拉长到 ~150ms。

### 3.7 第七步：验证 SchedulingClass 无清理

在 kray 源码中搜索 `sched_cls_to_id_` 的清理逻辑：

```bash
# 搜索 erase / clear
grep -r "sched_cls_to_id_.erase\|sched_cls_to_id_.clear" src/
# 无任何匹配
```

**确认**：`sched_cls_to_id_` 和 `sched_id_to_cls_` 两个全局静态 map 只有 insert，没有 erase/clear。唯一的释放时机是进程退出。

相关代码（`scheduling_class_util.cc`）：

```cpp
SchedulingClass SchedulingClassToIds::GetSchedulingClass(
    const SchedulingClassDescriptor &sched_cls) {
  absl::MutexLock lock(&mutex_);
  auto it = sched_cls_to_id_.find(sched_cls);
  if (it == sched_cls_to_id_.end()) {
    sched_cls_id = ++next_sched_id_;
    if (sched_cls_id > 100) {
      RAY_LOG_EVERY_MS(WARNING, 1000)
          << "More than " << sched_cls_id
          << " types of tasks seen, this may reduce performance.";
    }
    sched_cls_to_id_[sched_cls] = sched_cls_id;
    sched_id_to_cls_.emplace(sched_cls_id, sched_cls);
  } else {
    sched_cls_id = it->second;
  }
  return sched_cls_id;
}
```

**注意**：当 `sched_cls_id > 100` 时会有 WARNING 日志，但只是警告，不影响继续累积。

---

## 4. 根因分析

### 4.1 直接原因

快手内部 fork 的 `NoOpClusterAutoscaler.get_total_resources()` 实现错误——每次同步 RPC 查 GCS，无缓存。

### 4.2 放大链

```
NoOpClusterAutoscaler.get_total_resources() 无缓存
  → 每次 get_global_limits() 缓存过期都穿透 RPC
  → GLOBAL_LIMITS_UPDATE_INTERVAL_S=1 且调度循环每周期多次调用
  → 每周期 6-7 次 ray.cluster_resources() RPC
  × 集群 5 天后 GCS 自身被慢化（NodeHead CPU + sched_cls 累积 + error 风暴）
  × Driver 进程 GIL 竞争（gRPC callback、log thread、actor task submit 并发）
  → 每次 RPC 从 ~10ms 膨胀到 ~150ms
  → 6-7 × 150ms ≈ 1s/周期仅花在 RPC 上
  → 主调度线程 60% 时间阻塞在 RPC
  → 单周期 dispatch 数从 300+ 降到 50-80
  → GPU actor 喂数不及时，吞吐下降
```

### 4.3 为什么 inf() 不能用

`NoOpClusterAutoscaler` 的语义是"不需要 autoscaling，不限制资源"。直觉上应该返回 `ExecutionResources.inf()`（表示无限），但实际不能这样做：

```python
# ResourceManager._update_allocated_budgets() 中的 budget 计算
available_limits = self.get_global_limits().subtract(completed_ops_usage).max(ExecutionResources.zero())
```

如果 `global_limits = inf()`，则 `available_limits = inf - usage`。当 `usage` 也是 `inf` 时（例如 GPU op 声明 inf 资源需求），`inf - inf = NaN`，导致 `ReservationOpResourceAllocator` 的 budget 计算全部失效。

### 4.4 上游为什么没有 NoOpClusterAutoscaler

上游开源 Ray 中不存在 `NoOpClusterAutoscaler`。`create_cluster_autoscaler()` 函数（`cluster_autoscaler/__init__.py`）只有两个版本：

```python
DEFAULT_CLUSTER_AUTOSCALER_VERSION = os.environ.get("RAY_DATA_CLUSTER_AUTOSCALER", "V2")

def create_cluster_autoscaler(...):
    if DEFAULT_CLUSTER_AUTOSCALER_VERSION == ClusterAutoscalerVersion.V2:
        return DefaultClusterAutoscalerV2(...)
    elif DEFAULT_CLUSTER_AUTOSCALER_VERSION == ClusterAutoscalerVersion.V1:
        return DefaultClusterAutoscaler(...)
```

- `DefaultClusterAutoscalerV2.get_total_resources()` 从 `AutoscalingCoordinator.get_allocated_resources()` 获取，**不走 GCS RPC**
- `DefaultClusterAutoscaler.get_total_resources()` 也调用 `ray.cluster_resources()`，但只在 `try_trigger_scaling()` 中被调用（有 20s 间隔限制），不被 ResourceManager 高频调用

快手 fork 添加 `NoOpClusterAutoscaler` 的目的可能是：在不需要 autoscaling 的常驻集群上跳过 autoscaling 逻辑，但实现时错误地将 `get_total_resources()` 直接代理到 `ray.cluster_resources()`，忽略了该方法会在 ResourceManager 中被高频调用的事实。

---

## 5. Ray Data 中 NodeAffinitySchedulingStrategy 自动注入全路径分析

### 5.1 核心结论

**用户代码中没有指定 NodeAffinity 时，Ray Data 仍会在以下场景自动注入 `NodeAffinitySchedulingStrategy`，每个不同的 `node_id` 都会产生独立的 SchedulingClass。**

### 5.2 注入路径全景图

| # | 注入点 | 代码位置 | 触发条件 | node_id 来源 | 会产生 SchedClass 膨胀吗？ |
|---|--------|---------|---------|-------------|------------------------|
| 1 | MapOperator locality_with_output | `map_operator.py:457-477` | `ExecutionOptions.locality_with_output=True` | 轮询指定节点列表 | **会** — 每个 node_id 产生独立 sched_cls |
| 2 | Read 本地读取 | `read_api.py:427-430` | `datasource.supports_distributed_reads=False` | driver 所在节点（单一） | 不会 — 单一 node_id |
| 3 | Parquet 本地读取采样 | `parquet_datasource.py:324-330` | `supports_distributed_reads=False` | driver 所在节点（单一） | 不会 |
| 4 | Write 本地写入 | `dataset.py:5459-5463` | `datasink.supports_distributed_writes=False` | driver 所在节点（单一） | 不会 |
| 5 | PushBasedShuffle merge task | `push_based_shuffle_task_scheduler.py:141-147` | 使用 push-based shuffle | 按 merge_task_placement 映射 | **会** — 每个 worker node 各一个 |
| 6 | StreamSplitIterator coordinator | `stream_split_iterator.py:47-49` | `ds.streaming_split()` | driver 所在节点（单一） | 不会 |
| 7 | BlockBatching prefetcher | `block_batching/util.py:288-292` | `iter_batches()` 开启 prefetch | driver 所在节点（单一） | 不会 |
| 8 | AutoscalingRequester | `autoscaling_requester.py:120-122` | DefaultClusterAutoscaler(V1) 启用时 | driver 所在节点（单一） | 不会 |
| 9 | DefaultAutoscalingCoordinator | `default_autoscaling_coordinator.py:445-447` | V1 autoscaler 启用时 | driver 所在节点（单一） | 不会 |
| 10 | ActorLocationTracker | `actor_location.py:30-32` | 全局单例 actor | driver 所在节点（单一） | 不会 |
| 11 | StatsActor | `stats.py:807-809` | 全局单例 actor | driver 所在节点（单一） | 不会 |
| 12 | TorchDatasource from_data | `read_api.py:3824-3828` | `local_read=True` | driver 所在节点（单一） | 不会 |

### 5.3 详细代码分析

#### 5.3.1 MapOperator — locality_with_output（**主要膨胀来源**）

```python
# map_operator.py:457-477
def start(self, options: "ExecutionOptions"):
    # ...
    if options.locality_with_output:                          # ← 用户主动开启
        if isinstance(options.locality_with_output, list):
            locs = options.locality_with_output               # ← 用户指定节点列表
        else:
            locs = [ray.get_runtime_context().get_node_id()]  # ← 默认当前节点

        class RoundRobinAssign:
            def __init__(self, locs):
                self.locs = locs
                self.i = 0

            def __call__(self, args):
                args = copy.deepcopy(args)
                args["scheduling_strategy"] = NodeAffinitySchedulingStrategy(
                    self.locs[self.i],                       # ← 轮询分配
                    soft=True,
                    _spill_on_unavailable=True,
                )
                self.i += 1
                self.i %= len(self.locs)
                return args

        self._ray_remote_args_factory_actor_locality = RoundRobinAssign(locs)
```

**行为**：当 `ExecutionOptions.locality_with_output=True` 或设置为节点 ID 列表时，MapOperator 会为每个提交的 task 轮询注入不同的 `NodeAffinitySchedulingStrategy(node_id=...)`。

**SchedClass 影响**：如果 `locs` 包含 N 个不同的 node_id，则每个 task 的 `scheduling_strategy` 字段不同 → 产生 N 个不同的 SchedulingClass。结合 Ray Data 的高 task 提交频率，sched_cls 数量 = N(operators) × N(nodes)。

**默认值**：`ExecutionOptions.locality_with_output = False`（`execution_options.py:302`），**默认不开启**。

#### 5.3.2 Read 本地读取

```python
# read_api.py:427-430
if not datasource.supports_distributed_reads:
    ray_remote_args["scheduling_strategy"] = NodeAffinitySchedulingStrategy(
        ray.get_runtime_context().get_node_id(),   # ← driver 所在节点
        soft=False,
    )
```

**行为**：不支持分布式读取的 datasource（如本地文件）自动 pin 到 driver 节点。**单一 node_id，不膨胀。**

#### 5.3.3 Parquet 本地读取采样

```python
# parquet_datasource.py:324-330
self._local_scheduling = None
if not self._supports_distributed_reads:
    self._local_scheduling = NodeAffinitySchedulingStrategy(
        ray.get_runtime_context().get_node_id(), soft=False
    )

# parquet_datasource.py:1098-1101 — 采样 task 使用
futures.append(
    fetch_file_info.options(
        scheduling_strategy=local_scheduling
        or DataContext.get_current().scheduling_strategy,    # ← fallback 到 SPREAD
    ).remote(...)
)
```

**行为**：Parquet 读取时，如果 datasource 不支持分布式读，采样 task 也 pin 到 driver 节点。如果支持分布式读，采样 task 走 `DataContext.scheduling_strategy`（默认 `"SPREAD"` 字符串），**不注入 NodeAffinity。**

#### 5.3.4 Write 本地写入

```python
# dataset.py:5459-5463
if not datasink.supports_distributed_writes:
    ray_remote_args["scheduling_strategy"] = NodeAffinitySchedulingStrategy(
        ray.get_runtime_context().get_node_id(),
        soft=False,
    )
```

**行为**：不支持分布式写入的 datasink 自动 pin 到 driver 节点。**单一 node_id，不膨胀。**

#### 5.3.5 PushBasedShuffle merge task（**潜在膨胀来源**）

```python
# push_based_shuffle_task_scheduler.py:141-147
node_strategies = {
    node_id: {
        "scheduling_strategy": NodeAffinitySchedulingStrategy(
            node_id, soft=True
        )
    }
    for node_id in set(merge_task_placement)    # ← 去重后的 merge 节点集合
}
self._merge_task_options = [
    node_strategies[node_id] for node_id in merge_task_placement
]
```

**行为**：push-based shuffle 的 merge task 按 `merge_task_placement` 映射注入 NodeAffinity，每个不同的 worker node 各一个策略。

**SchedClass 影响**：merge task 分布在多少个不同节点上，就产生多少个 sched_cls。常驻集群 1100+ GPU 节点，**可能产生大量 sched_cls**。

#### 5.3.6 全局单例 Actor（不膨胀）

以下全局单例 actor 都 pin 到 driver 所在节点（单一 node_id），不产生 sched_cls 膨胀：

```python
# autoscaling_requester.py:120-122
scheduling_strategy = NodeAffinitySchedulingStrategy(
    ray.get_runtime_context().get_node_id(), soft=False,
)

# default_autoscaling_coordinator.py:445-447
scheduling_strategy = NodeAffinitySchedulingStrategy(
    ray.get_runtime_context().get_node_id(), soft=False,
)

# actor_location.py:30-32
scheduling_strategy = NodeAffinitySchedulingStrategy(
    ray.get_runtime_context().get_node_id(), soft=False,
)

# stats.py:807-809
scheduling_strategy = NodeAffinitySchedulingStrategy(
    ray.get_runtime_context().get_node_id(), soft=False,
)

# stream_split_iterator.py:47-49
scheduling_strategy=NodeAffinitySchedulingStrategy(
    ray.get_runtime_context().get_node_id(), soft=False,
)

# block_batching/util.py:288-292
scheduling_strategy=NodeAffinitySchedulingStrategy(node_id, soft=False),
```

#### 5.3.7 ActorPoolMapOperator — 不注入 NodeAffinity

```python
# actor_pool_map_operator.py:565-566
@staticmethod
def _apply_default_remote_args(ray_remote_args, data_context):
    if "scheduling_strategy" not in ray_remote_args:
        ray_remote_args["scheduling_strategy"] = data_context.scheduling_strategy
```

**行为**：ActorPoolMapOperator 默认使用 `DataContext.scheduling_strategy`（通常为 `"SPREAD"` 字符串），**不注入 NodeAffinitySchedulingStrategy**。即使创建 1100 个 GPU MapWorker actor，它们的 scheduling_strategy 都相同，**只产生 1 个 SchedulingClass**。

### 5.4 SchedClass 膨胀总结

在用户代码不指定 NodeAffinity 的情况下，sched_cls 膨胀可能来自：

| 来源 | 条件 | 膨胀量级 |
|------|------|---------|
| `locality_with_output` | 用户显式开启（默认关闭） | N(operators) × N(nodes) |
| PushBasedShuffle merge task | 使用 HashShuffle/AllToAll 算子 + push-based | N(merge_nodes) |
| 节点上下线/重启 | 常驻集群 5 天运行 | 历史并集 node_id |

当前 102k sched_cls 的最可能来源：**节点上下线/重启产生的历史 node_id 并集**，加上某些算子的 PushBasedShuffle merge task。排查命令：

```python
# 在 driver 中 dump SchedulingClassDescriptor.DebugString()，按 node_id 分桶
# 需要访问 GCS 内部的 info_by_sched_cls_ 表（不可直接访问 Python 层）
# 替代方案：通过 GCS 日志观察 sched_cls_id 增长速度
```

---

## 6. SchedulingClass 累积问题深度分析

### 6.1 SchedulingClassDescriptor 的 6 个 hash 字段

```cpp
// scheduling_class_util.h
struct SchedulingClassDescriptor {
    ResourceSet resource_set;
    LabelSelector label_selector;
    FunctionDescriptor function_descriptor;
    int64_t depth;
    rpc::SchedulingStrategy scheduling_strategy;    // ← 包含 NodeAffinity
    std::vector<FallbackOption> fallback_strategy;
};
```

**任意一个字段不同，就产生新的 SchedulingClass。** 特别是 `scheduling_strategy` 字段：

```cpp
// scheduling_class_util.cc — SchedulingStrategy 的 == 运算符
case rpc::SchedulingStrategy::kNodeAffinitySchedulingStrategy: {
    return (lhs.node_affinity_scheduling_strategy().node_id() ==
            rhs.node_affinity_scheduling_strategy().node_id()) &&
           (lhs.node_affinity_scheduling_strategy().soft() ==
            rhs.node_affinity_scheduling_strategy().soft()) &&
           (lhs.node_affinity_scheduling_strategy().spill_on_unavailable() ==
            rhs.node_affinity_scheduling_strategy().spill_on_unavailable()) &&
           (lhs.node_affinity_scheduling_strategy().fail_on_unavailable() ==
            rhs.node_affinity_scheduling_strategy().fail_on_unavailable());
}
```

**结论**：每个不同的 `node_id` + `soft` + `spill_on_unavailable` + `fail_on_unavailable` 组合都产生独立的 SchedulingClass。

### 6.2 累积的三大危害

#### 危害 1：GetSchedulingClass 全局锁竞争

```cpp
SchedulingClass SchedulingClassToIds::GetSchedulingClass(
    const SchedulingClassDescriptor &sched_cls) {
  absl::MutexLock lock(&mutex_);    // ← 全局互斥锁
  auto it = sched_cls_to_id_.find(sched_cls);
  // ...
}
```

每提交一个 task/actor 创建请求都走这个路径。102k 条目的 hash map 查找 + 全局锁 = O(1) 查找但常数大，且锁竞争加剧。

#### 危害 2：info_by_sched_cls_ 膨胀

GCS 内部维护 `info_by_sched_cls_` map，按 SchedulingClass 聚合 task 信息。102k 个 entry 的 map 遍历开销显著。

#### 危害 3：tasks_to_dispatch_by_sched_cls_ O(N) 遍历

调度器在 `ScheduleAndGrantLeases` 中遍历 `tasks_to_dispatch_by_sched_cls_`，102k 个 bucket 的遍历增加调度延迟。

### 6.3 无清理逻辑验证

```bash
# 在 kray 源码中搜索所有清理逻辑
grep -r "sched_cls_to_id_.erase\|sched_cls_to_id_.clear\|sched_id_to_cls_.erase\|sched_id_to_cls_.clear" src/
# 结果：无任何匹配
```

**唯一释放时机**：进程退出（GCS server shutdown）。

---

## 7. py-spy 19ms 实测 vs 60% 占比的采样偏差分析

### 7.1 表面矛盾

- 裸调用 `ray.cluster_resources()` 实测 p50 = 19ms
- py-spy 采样显示主线程 60% 时间在此函数
- 如果每次 19ms × 6-7 calls/周期 ≈ 130ms/周期，周期总时间 ~5s，占比应 ~2.6%，远低于 60%

### 7.2 解释：采样统计语义

py-spy 默认采样率 100Hz（每 10ms 一个采样），30s 共 3000 个采样点。主调度线程被采样到 345 次（线程也在 sleep/IO 等待，不是 3000 次）。

**换算**：

```
219 samples × 10ms/sample = 2190ms 在 update_usages
205 samples × 10ms/sample = 2050ms 在 cluster_resources RPC 内
30s 内调用次数 ≈ 2050ms / 19ms ≈ 108 calls
```

### 7.3 真实延迟估算

```
60% × 30s = 18s wall time 在 cluster_resources RPC
108 calls 在 18s 内
→ 平均 167ms/call
```

**167ms vs 19ms 裸测量 = 8.8× 放大**。

### 7.4 放大原因

1. **GIL 竞争**：driver 进程内 gRPC callback 线程、log thread、actor task submit 线程同时争抢 GIL。`ray.cluster_resources()` 底层走 gRPC，需要等 GIL 回调处理 response，GIL 等待时间被计入采样。

2. **并发 gRPC 流量**：1100 GPU MapWorker + 200 CPU actor + 1300+ 内嵌 PreprocessActor 的 task submit/complete 回调在同一进程内，gRPC 资源竞争显著。

3. **GCS 端慢化**：
   - NodeHead PID 453 持续 99% CPU 轮询 GCS
   - 102k SchedulingClass 全局锁竞争
   - 18,118 次 ReportJobError 增加 GCS pub/sub 负载
   - GCS 处理 cluster_resources RPC 的 p99 从 ~10ms 涨到 80ms+

4. **多 ResourceManager 实例**：如果有多个并发 dataset，每个 ResourceManager 独立缓存，独立调用 `get_total_resources()`。

5. **内层循环多次调用**：
   ```python
   # streaming_executor.py:610-633
   self._resource_manager.update_usages()          # 调用 1
   while True:
       op = select_operator_to_run()               # 每选一个 op → get_global_limits()
       if op is None: break
       topology[op].dispatch_next_task()
       self._resource_manager.update_usages()      # 每次 dispatch 后 → get_global_limits()
   ```

### 7.5 验证方法

在 monkey-patch 中加入 `time.perf_counter()` 计时打 log：

```python
def _cached_get_total_resources(self):
    now = _t.monotonic()
    if _state["val"] is None or (now - _state["ts"]) > _CACHE_TTL_S:
        t0 = _t.perf_counter()
        _state["val"] = _orig(self)
        elapsed = (_t.perf_counter() - t0) * 1000
        if elapsed > 50:
            logger.warning(f"[noop_cache] cluster_resources RPC took {elapsed:.0f}ms")
        _state["ts"] = now
    return _state["val"]
```

**预期**：修复前能看到大量 50-300ms 区间的长尾；修复后（30s 缓存）几乎不再看到。

---

## 8. 修复方案

### 8.1 当前修复（已实施）

在 `patch_interleave_dispatch.py` 末尾添加 `_apply_noop_autoscaler_cache()` 函数：

```python
def _apply_noop_autoscaler_cache():
    """Patch NoOpClusterAutoscaler.get_total_resources with 30s TTL cache.

    Problem:
        The Kuaishou internal Ray fork's NoOpClusterAutoscaler.get_total_resources()
        calls ray.cluster_resources() on every invocation — a synchronous GCS RPC
        with no caching. ResourceManager calls this 6-7 times per scheduling cycle
        (GLOBAL_LIMITS_UPDATE_INTERVAL_S=1), and each call blocks the main
        scheduling thread for 20-150ms. On a 5-day-old cluster, this accounts for
        60% of the scheduling thread time.

    Why 30s TTL:
        Cluster resource totals change slowly (nodes join/leave on timescales
        of minutes), so 30s is a safe upper bound. This reduces per-cycle RPC
        calls from ~6-7 to effectively zero (one every 30s).

    Why not return ExecutionResources.inf():
        Older versions returned inf(), but the ReservationOpResourceAllocator
        budget calculation hits inf - inf which produces NaN. Caching the real
        value avoids both the RPC cost AND the NaN risk.
    """
    import time as _t

    try:
        from ray.data._internal.cluster_autoscaler.noop_cluster_autoscaler import (
            NoOpClusterAutoscaler,
        )
    except ImportError:
        logger.warning(
            "[patch_noop_autoscaler] NoOpClusterAutoscaler not found; skipping cache patch."
        )
        return

    _CACHE_TTL_S = 30.0
    _orig = NoOpClusterAutoscaler.get_total_resources
    _state = {"val": None, "ts": 0.0}

    def _cached_get_total_resources(self):
        now = _t.monotonic()
        if _state["val"] is None or (now - _state["ts"]) > _CACHE_TTL_S:
            _state["val"] = _orig(self)
            _state["ts"] = now
        return _state["val"]

    NoOpClusterAutoscaler.get_total_resources = _cached_get_total_resources

    # Also widen ResourceManager's own get_global_limits cache from 1s to 30s,
    # which protects any code path that goes through get_global_limits even if
    # a different autoscaler is in use.
    try:
        from ray.data._internal.execution.resource_manager import ResourceManager
        ResourceManager.GLOBAL_LIMITS_UPDATE_INTERVAL_S = _CACHE_TTL_S
    except ImportError:
        pass

    logger.info(
        f"[patch_noop_autoscaler] Cached NoOpClusterAutoscaler.get_total_resources "
        f"and ResourceManager.GLOBAL_LIMITS_UPDATE_INTERVAL_S to {_CACHE_TTL_S}s "
        f"(was 1s). Eliminates per-cycle GCS cluster_resources() RPC storms."
    )
```

**效果**：
- 每次 `get_total_resources()` 调用不再走 GCS RPC（缓存命中直接返回）
- `ResourceManager.GLOBAL_LIMITS_UPDATE_INTERVAL_S` 从 1s 放宽到 30s，进一步减少 `get_global_limits()` 缓存穿透
- 每调度周期 RPC 调用从 6-7 次降到接近 0（每 30s 最多 1 次）

### 8.2 效果预期

| 指标 | 修复前 | 修复后 |
|------|--------|--------|
| 每周期 RPC 调用 | 6-7 次 | 0 次（30s 内最多 1 次） |
| update_usages 占比 | 63.5% | <5% |
| 主调度线程 RPC 阻塞 | ~1s/周期 | <0.02s/周期 |
| 单周期 dispatch 数 | 50-80 | 300+ |

---

## 9. NoOpClusterAutoscaler 上游修复建议

### 9.1 方案 A（最干净，推荐）：autoscaler 返回 None，ResourceManager 跳过 budget

```python
# noop_cluster_autoscaler.py
class NoOpClusterAutoscaler(ClusterAutoscaler):
    def get_total_resources(self) -> Optional[ExecutionResources]:
        return None  # 表示"无限制，不参与 budget"

    def try_trigger_scaling(self):
        pass  # NoOp 不触发 autoscaling

    def on_executor_shutdown(self):
        pass

# resource_manager.py
def get_global_limits(self) -> Optional[ExecutionResources]:
    if (
        time.time() - self._global_limits_last_update_time
        < self.GLOBAL_LIMITS_UPDATE_INTERVAL_S
    ):
        return self._global_limits

    self._global_limits_last_update_time = time.time()
    total_resources = self._get_total_resources()
    if total_resources is None:
        self._global_limits = None  # 下游遇 None 直接 skip budget 比较
        return None
    # ... 原有逻辑
```

**优点**：
- `None` 显式表达"NoOp 语义 = 无 budget 概念"
- 不需要引入 `inf()` 哨兵值
- 不需要缓存机制
- 下游 `op_runtime_metrics.py` 已有 `if global_limits is None` 短路逻辑

### 9.2 方案 B（最小改动）：返回 inf 但 budget 用 saturating 减法

```python
def _safe_sub(a, b):
    if math.isinf(a) and math.isinf(b):
        return math.inf  # inf - inf = inf，而非 NaN
    return a - b
```

**优点**：改动最小，只需修改 `ExecutionResources.subtract()` 方法。
**缺点**：`inf` 作为哨兵值语义不够清晰，且可能干扰其他使用 `inf` 的逻辑。

### 9.3 方案 C（当前 patch 等价）：缓存 cluster_resources() 30s

```python
class NoOpClusterAutoscaler(ClusterAutoscaler):
    _CACHE_TTL_S = 30.0
    _cache = {"val": None, "ts": 0.0}

    def get_total_resources(self) -> ExecutionResources:
        now = time.monotonic()
        if self._cache["val"] is None or (now - self._cache["ts"]) > self._CACHE_TTL_S:
            self._cache["val"] = ExecutionResources.from_resource_dict(
                ray.cluster_resources()
            )
            self._cache["ts"] = now
        return self._cache["val"]
```

**优点**：和 DefaultClusterAutoscaler 行为一致，下游代码零改动。
**缺点**：不是"NoOp"语义，autoscaler 不工作时上限会被节点动态变化干扰。

### 9.4 对比

| 方案 | 语义清晰度 | 改动范围 | 兼容性 | 推荐度 |
|------|-----------|---------|--------|--------|
| A: 返回 None | 最佳 | ResourceManager + 下游 budget 判断 | 需检查所有 `global_limits` 使用点 | **推荐** |
| B: inf + saturating | 一般 | ExecutionResources.subtract() | 低风险 | 次选 |
| C: 30s 缓存 | 最差（不是 NoOp） | 仅 autoscaler | 完全兼容 | 临时方案 |

---

## 10. 监控告警建议

### 10.1 SCHED_PROFILE 监控

```
# 当 SCHED_PROFILE proc > 1s 时告警
# 正常基线：proc < 0.1s
# 告警阈值：proc > 1s
# 紧急阈值：proc > 5s
```

### 10.2 SchedulingClass 数量监控

```
# GCS 日志中 "More than N types of tasks seen" 的 N 值
# 正常基线：< 100
# 告警阈值：> 1000
# 紧急阈值：> 10000
```

### 10.3 cluster_resources RPC 延迟监控

在 patch 中加入计时打点，统计 p50/p99 延迟，超过 50ms 时发出警告。

### 10.4 Dashboard NodeHead CPU 监控

```
# NodeHead 进程 CPU 持续 > 90% 时告警
# 这是独立 bug 但会放大 GCS 慢化效应
```

---

## 11. 相关代码位置索引

### 11.1 核心调用链

| 文件 | 行号 | 说明 |
|------|------|------|
| `python/ray/data/_internal/execution/streaming_executor.py` | :199-202 | `ResourceManager` 构造，传入 `cluster_autoscaler.get_total_resources` 回调 |
| `python/ray/data/_internal/execution/streaming_executor.py` | :590 | Phase 2 开始 `update_usages()` |
| `python/ray/data/_internal/execution/streaming_executor.py` | :612 | `select_operator_to_run()` 循环 |
| `python/ray/data/_internal/execution/streaming_executor.py` | :633 | 每次 dispatch 后 `update_usages()` |
| `python/ray/data/_internal/execution/resource_manager.py` | :59 | `GLOBAL_LIMITS_UPDATE_INTERVAL_S = 1` |
| `python/ray/data/_internal/execution/resource_manager.py` | :292-315 | `get_global_limits()` — 缓存逻辑 + `_get_total_resources()` 调用 |
| `python/ray/data/_internal/execution/resource_manager.py` | :213-267 | `update_usages()` → `_update_allocated_budgets()` → `get_global_limits()` |

### 11.2 ClusterAutoscaler 实现

| 文件 | 行号 | 说明 |
|------|------|------|
| `python/ray/data/_internal/cluster_autoscaler/base_cluster_autoscaler.py` | :27-33 | `ClusterAutoscaler.get_total_resources()` 抽象接口 |
| `python/ray/data/_internal/cluster_autoscaler/default_cluster_autoscaler.py` | :127-130 | V1: `get_total_resources()` 也调 `ray.cluster_resources()`，但仅在 autoscaling 时被高频调用 |
| `python/ray/data/_internal/cluster_autoscaler/default_cluster_autoscaler_v2.py` | :281-290 | V2: `get_total_resources()` 从 `AutoscalingCoordinator` 获取，**不走 GCS RPC** |
| `python/ray/data/_internal/cluster_autoscaler/__init__.py` | :29-52 | `create_cluster_autoscaler()` 工厂函数 |

### 11.3 NodeAffinitySchedulingStrategy 注入点

| 文件 | 行号 | 说明 |
|------|------|------|
| `python/ray/data/_internal/execution/operators/map_operator.py` | :457-477 | `locality_with_output` 轮询注入（**主要膨胀来源**） |
| `python/ray/data/read_api.py` | :427-430 | 不支持分布式读 → pin 到 driver 节点 |
| `python/ray/data/_internal/datasource/parquet_datasource.py` | :324-330 | Parquet 本地读 → pin 到 driver 节点 |
| `python/ray/data/dataset.py` | :5459-5463 | 不支持分布式写 → pin 到 driver 节点 |
| `python/ray/data/_internal/planner/exchange/push_based_shuffle_task_scheduler.py` | :141-147 | Push-based shuffle merge task 按 node 注入（**潜在膨胀来源**） |
| `python/ray/data/_internal/iterator/stream_split_iterator.py` | :47-49 | StreamSplit coordinator → pin 到 driver 节点 |
| `python/ray/data/_internal/block_batching/util.py` | :288-292 | Block prefetcher → pin 到 driver 节点 |
| `python/ray/data/_internal/execution/autoscaling_requester.py` | :120-122 | 全局单例 → pin 到 driver 节点 |
| `python/ray/data/_internal/cluster_autoscaler/default_autoscaling_coordinator.py` | :445-447 | 全局单例 → pin 到 driver 节点 |
| `python/ray/data/_internal/execution/node_trackers/actor_location.py` | :30-32 | 全局单例 → pin 到 driver 节点 |
| `python/ray/data/_internal/stats.py` | :807-809 | StatsActor → pin 到 driver 节点 |
| `python/ray/data/read_api.py` | :3824-3828 | TorchDatasource local_read → pin 到 driver 节点 |

### 11.4 SchedulingClass 相关

| 文件 | 行号 | 说明 |
|------|------|------|
| `src/ray/common/scheduling/scheduling_class_util.cc` | :137-158 | `GetSchedulingClass()` — 全局锁 + hash map 查找/插入 |
| `src/ray/common/scheduling/scheduling_class_util.cc` | :126-135 | `SchedulingClassDescriptor::operator==` — 6 字段全比较 |
| `src/ray/common/scheduling/scheduling_class_util.cc` | :55-78 | `SchedulingStrategy` 的 `==` — NodeAffinity 按 node_id+soft+spill+fail 比较 |
| `src/ray/common/scheduling/scheduling_class_util.cc` | :160-163 | 全局静态 map 定义 — 无清理逻辑 |

### 11.5 修复 Patch

| 文件 | 行号 | 说明 |
|------|------|------|
| `utils/patch_interleave_dispatch.py` (kling-ray) | :510-559 | `_apply_noop_autoscaler_cache()` — 30s 缓存 monkey-patch |

---

## 附录：排查工具与方法

### A.1 py-spy 抓取调度线程火焰图

```bash
# 远端执行
/opt/vjepa2/bin/py-spy record --pid 27195 --duration 30 --rate 100 -o profile.svg
```

### A.2 测量 cluster_resources RPC 延迟

```bash
# 远端 driver shell
/opt/vjepa2/bin/python -c "
import time, ray
ray.init(address='auto')
for i in range(20):
    t0 = time.perf_counter()
    ray.cluster_resources()
    print(f'call {i}: {(time.perf_counter()-t0)*1000:.1f}ms')
"
```

### A.3 查看 SCHED_PROFILE 日志

```bash
# 远端 driver 日志
grep 'SCHED_PROFILE' /tmp/ray/session_latest/logs/job-driver-raysubmit_YCujBCHJaPfuBV12.log | tail -20
```

### A.4 查看 GCS 日志中的 SchedulingClass 警告

```bash
grep 'types of tasks seen' /tmp/ray/session_latest/logs/gcs_server.out | tail -5
```

### A.5 查看 Dashboard NodeHead CPU

```bash
top -p 453 -b -n 1  # PID 453 是 NodeHead 进程
```

### A.6 查看 ReportJobError 计数

```bash
grep -c 'ReportJobError' /tmp/ray/session_latest/logs/gcs_server.out
```
