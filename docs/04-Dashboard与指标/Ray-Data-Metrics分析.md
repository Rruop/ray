# Ray Data Metrics 分析文档

## 目录

### Part 1: MapTransformer 与 OpRuntimeMetrics
1. [MapTransformer 数据处理流程](#1-maptransformer-数据处理流程)
2. [OpRuntimeMetrics 指标分析](#2-opruntimemetrics-指标分析)
3. [实时行级指标的限制](#3-实时行级指标的限制)
4. [关键代码路径](#4-关键代码路径)

### Part 2: 自定义指标上报与清理机制
5. [概述](#5-概述)
6. [指标定义与运行环境](#6-指标定义与运行环境)
7. [指标上报完整流程](#7-指标上报完整流程)
8. [Worker 复用与指标累积](#8-worker-复用与指标累积)
9. [指标清理机制](#9-指标清理机制)
10. [维度爆炸问题分析](#10-维度爆炸问题分析)
11. [多作业场景分析](#11-多作业场景分析)
12. [Ray 内置指标对比](#12-ray-内置指标对比)
13. [Prometheus Staleness 机制](#13-prometheus-staleness-机制)
14. [最佳实践建议](#14-最佳实践建议)

### Part 3: Autoscaler 节点移除机制
15. [Autoscaler 日志分析](#15-autoscaler-日志分析)
16. [max_workers 参数来源](#16-max_workers-参数来源)
17. [节点移除优先级](#17-节点移除优先级)
18. [被终止节点上的任务处理](#18-被终止节点上的任务处理)
19. [相关代码位置](#19-相关代码位置)
20. [最佳实践建议](#20-最佳实践建议)

---

# Part 1: MapTransformer 与 OpRuntimeMetrics

## 1. MapTransformer 数据处理流程

### 1.1 `__call__` 方法执行流程

```python
def __call__(
    self,
    blocks: Iterable[Block],
    ctx: TaskContext,
) -> Iterable[Block]:
    batches = self._pre_process(blocks)
    results = self._apply_transform(ctx, batches)
    yield from self._post_process(results)
```

**关键点：流式处理，不是等所有数据处理完**

由于 Python 的生成器 (generator) 和惰性求值 (lazy evaluation) 机制：

1. `_apply_transform` 返回的是一个生成器 (Iterable)，不是已经计算好的列表
2. `yield from` 表示流式地从 `_post_process` 中产出结果

执行流程：
```
消费者请求一个 Block
    → _post_process 被调用，尝试获取 results 的下一个元素
        → _apply_transform 产出一个/一批结果
            → _pre_process 产出需要的输入
```

### 1.2 `_shape_blocks` 方法

```python
def _shape_blocks(self, results: Iterable[MapTransformFnData]) -> Iterable[Block]:
    buffer = BlockOutputBuffer(self._output_block_size_option)
    # ...
    for result in results:      # 遍历 results 生成器
        append(result)
        while buffer.has_next():  # buffer 满了就 yield
            yield buffer.next()   # 增量产出
    # ...
```

这是典型的**流式管道处理**：
- 每当 `_apply_transform` 产出一个结果
- `_post_process` 就会接收并处理它
- 当输出 buffer 达到阈值时，就会 `yield` 一个 Block

### 1.3 不同粒度的处理

根据 `MapTransformFn` 子类不同，append 的粒度也不同：

| 子类 | `_apply_transform` 产出 | append 粒度 |
|------|------------------------|-------------|
| `RowMapTransformFn` | 每行一个 `Row` | 一行一次 `buffer.add(row)` |
| `BatchMapTransformFn` | 每批一个 `DataBatch` | 一批一次 `buffer.add_batch(batch)` |
| `BlockMapTransformFn` | 每块一个 `Block` | 一块一次 `buffer.add_block(block)` |

## 2. OpRuntimeMetrics 指标分析

### 2.1 `on_output_taken` 调用时机

```python
# physical_operator.py:734
def get_next(self) -> RefBundle:
    output = self._get_next_inner()
    self._metrics.on_output_taken(output)
    return output
```

**关键点**：`on_output_taken` 只在**下游 operator 从当前 operator 取走一个 RefBundle** 时才被调用。

### 2.2 数据流和指标更新流程

```
Task 产出 Block
    ↓
on_task_output_generated()  ← 这里统计 rows_task_outputs_generated
    ↓
Block 进入 outqueue
    ↓
下游 operator 调用 get_next()
    ↓
on_output_taken()           ← 这里统计 row_outputs_taken
```

### 2.3 现有指标对比

| 指标 | 更新时机 | 含义 |
|------|----------|------|
| `rows_task_outputs_generated` | `on_task_output_generated` | Task 产出的行数（生产侧，最实时） |
| `rows_outputs_of_finished_tasks` | `on_task_finished` | 已完成 Task 产出的行数 |
| `row_outputs_taken` | `on_output_taken` | 被下游取走的行数（消费侧） |

### 2.4 `row_outputs_taken` 为 0 的原因

| 场景 | 结果 |
|------|------|
| 没有下游 operator 消费数据 | `on_output_taken` 永远不会被调用 |
| 下游消费速度慢 | `row_outputs_taken` 会延迟更新 |
| 数据还在 buffer 中未形成完整 Block | `on_output_taken` 不会被调用 |

## 3. 实时行级指标的限制

### 3.1 架构限制

由于 Ray Data 的分布式架构：
- **Worker 端**：`_map_task` 在 worker 上运行，处理数据并 yield blocks
- **Driver 端**：`OpRuntimeMetrics` 在 driver 上，只有收到 block 时才能更新

**跨进程实时回调是不可行的**。

### 3.2 可行方案

1. **使用 `rows_task_outputs_generated`**：这是目前最实时的指标，在每个 block 产出时更新

2. **减小 block 大小**：通过设置 `target_max_block_size` 更小的值，让 blocks 更频繁地 yield，从而使指标更新更频繁

## 4. 关键代码路径

- `python/ray/data/_internal/execution/operators/map_transformer.py` - MapTransformer 实现
- `python/ray/data/_internal/execution/interfaces/op_runtime_metrics.py` - 运行时指标
- `python/ray/data/_internal/execution/interfaces/physical_operator.py` - PhysicalOperator 基类
- `python/ray/data/_internal/execution/operators/map_operator.py` - MapOperator 实现
- `python/ray/data/_internal/output_buffer.py` - BlockOutputBuffer 实现

---

# Part 2: 自定义指标上报与清理机制

## 5. 概述

本部分详细分析 Ray Data 中自定义指标（如 `Counter`、`Gauge`）的统计、上报和清理机制，以及在多作业场景下可能出现的维度爆炸问题。

### 5.1 分析背景

在 Ray Data Pipeline 中定义了以下自定义指标：

```python
# video_preprocess_mapper.py
self.slice_counter = Counter(
    "multishot_pipeline_slice_total",
    description="Total number of slices processed",
    tag_keys=("blobstore_id", "status"),
)

# video_inference_mapper.py
self.segment_counter = Counter(
    "multishot_inference_segments",
    description="Total segments processed by inference stage",
    tag_keys=("blobstore_id", "status"),
)
```

### 5.2 核心问题

1. 这些指标是否在 CoreWorker 中运行？
2. 每运行完一次 Task，指标会重新初始化吗？
3. 会产生维度爆炸吗？
4. 什么时候清理指标？
5. Worker 复用时指标如何累积？

---

## 6. 指标定义与运行环境

### 6.1 指标运行位置

**是的，这些指标在 CoreWorker 中运行和上报。**

```
┌─────────────────────────────────────────────────────────────────┐
│                       Worker Process                             │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │  ray.util.metrics.Counter/Gauge                             │ │
│  │       ↓                                                     │ │
│  │  CythonCount / CythonGauge (_raylet.pyx)                    │ │
│  │       ↓                                                     │ │
│  │  CoreWorker::RecordMetrics() (C++)                          │ │
│  │       ↓ (gRPC)                                              │ │
│  └─────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                               ↓
┌─────────────────────────────────────────────────────────────────┐
│                   ReporterAgent (Dashboard Agent)                │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │  ReportOCMetrics() (reporter_agent.py:566-579)              │ │
│  │       ↓                                                     │ │
│  │  proxy_export_metrics() (metrics_agent.py:722-740)          │ │
│  │       ↓                                                     │ │
│  │  OpenCensusProxyCollector.record()                          │ │
│  │       ↓                                                     │ │
│  │  Prometheus Endpoint (/metrics)                             │ │
│  └─────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

### 6.2 Mapper 生命周期与初始化

```python
class VideoPreprocessMapper(BaseMapper):
    def __init__(self, config):
        # 构造函数 - 创建 Mapper 实例时调用
        # Driver 端调用一次（用于序列化配置）
        # 每个 Actor 端调用一次（反序列化）
        self.slice_counter: Optional[Counter] = None  # 只声明，不初始化

    def setup(self) -> None:
        # setup() - 每个 Actor/Worker 启动时调用一次（lazy_init=True）
        self.slice_counter = Counter(...)  # 在这里初始化

    def process_single(self, row):
        # 每次处理一行数据时调用
        # 同一个 Actor 处理多个 row，复用同一个 Counter 实例
        self.slice_counter.inc(...)
```

**关键行为：**

| 问题 | 答案 |
|------|------|
| 同一个 Worker 会运行多次 `process_single()` 吗？ | **会** - 一个 Actor 会处理多个数据行 |
| `setup()` 会被调用几次？ | **每个 Actor 调用 1 次** |
| `Counter` 初始化几次？ | **每个 Actor 初始化 1 次** |
| 之前的指标会保留吗？ | **会** - 同一个 Actor 内的所有 label 组合都会累积 |

---

## 7. 指标上报完整流程

### 7.1 完整生命周期

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              指标完整生命周期                                     │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  阶段 1: 指标创建 (Worker 进程内)                                                │
│  ════════════════════════════════                                               │
│                                                                            │
│  VideoPreprocessMapper.setup()                                                  │
│       ↓                                                                         │
│  Counter("multishot_pipeline_slice_total", tag_keys=("blobstore_id", "status")) │
│       ↓                                                                         │
│  CythonCount (ray/_raylet.pyx)                                                  │
│       ↓                                                                         │
│  CoreWorker::RegisterMetric() (C++)                                             │
│       ↓                                                                         │
│  创建 OpenTelemetry/OpenCensus Metric 对象                                       │
│                                                                                 │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  阶段 2: 指标记录 (每次 process 调用)                                            │
│  ════════════════════════════════════                                           │
│                                                                                 │
│  self.slice_counter.inc(10, tags={"blobstore_id": "video_001", "status": "ok"}) │
│       ↓                                                                         │
│  CythonCount.record(value, tags)                                                │
│       ↓                                                                         │
│  CoreWorker 内部 metrics 数据结构更新:                                           │
│                                                                                 │
│  worker_metrics_map = {                                                         │
│      "multishot_pipeline_slice_total": {                                        │
│          ("video_001", "ok"): 10,    ← 新增                                     │
│      }                                                                          │
│  }                                                                              │
│                                                                                 │
│  继续处理...                                                                     │
│                                                                                 │
│  self.slice_counter.inc(15, tags={"blobstore_id": "video_002", "status": "ok"}) │
│       ↓                                                                         │
│  worker_metrics_map = {                                                         │
│      "multishot_pipeline_slice_total": {                                        │
│          ("video_001", "ok"): 10,                                               │
│          ("video_002", "ok"): 15,    ← 新增，video_001 仍保留                   │
│      }                                                                          │
│  }                                                                              │
│                                                                                 │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  阶段 3: 定期上报 (Worker → Dashboard Agent)                                    │
│  ════════════════════════════════════════════                                   │
│                                                                                 │
│  CoreWorker 每隔 ~1s 通过 gRPC 上报到本节点的 Dashboard Agent                    │
│                                                                                 │
│  ReportOCMetrics RPC:                                                           │
│  {                                                                              │
│      worker_id: "worker_abc123",                                                │
│      metrics: [                                                                 │
│          {name: "multishot_pipeline_slice_total",                               │
│           labels: [("video_001", "ok"), ("video_002", "ok")],                   │
│           values: [10, 15]}                                                     │
│      ]                                                                          │
│  }                                                                              │
│       ↓                                                                         │
│  reporter_agent.py:566-579 ReportOCMetrics()                                    │
│       ↓                                                                         │
│  metrics_agent.proxy_export_metrics(metrics, worker_id)                         │
│                                                                                 │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  阶段 4: Dashboard Agent 存储                                                   │
│  ════════════════════════════                                                   │
│                                                                                 │
│  OpenCensusProxyCollector.record(metrics, worker_id_hex)                        │
│       ↓                                                                         │
│  self._components[worker_id] = Component(worker_id)                             │
│  component.record(metrics)                                                      │
│       ↓                                                                         │
│  Component._metrics[metric_name] = OpencensusProxyMetric(...)                   │
│  OpencensusProxyMetric._data[label_values] = aggregation_data                   │
│                                                                                 │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  阶段 5: Prometheus 拉取                                                        │
│  ═══════════════════════                                                        │
│                                                                                 │
│  Prometheus 定期 (15s/30s) 请求 Dashboard Agent 的 /metrics 端点                │
│       ↓                                                                         │
│  OpenCensusProxyCollector.collect() 被调用                                      │
│       ↓                                                                         │
│  遍历所有 components，所有 metrics，所有 label 组合                              │
│       ↓                                                                         │
│  生成 Prometheus 格式:                                                          │
│                                                                                 │
│  ray_multishot_pipeline_slice_total{blobstore_id="video_001",status="ok"} 10    │
│  ray_multishot_pipeline_slice_total{blobstore_id="video_002",status="ok"} 15    │
│  ray_multishot_pipeline_slice_total{blobstore_id="video_003",status="ok"} 8     │
│  ... (所有历史 label 组合都会输出)                                              │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 7.2 Dashboard Agent 内存结构

```python
# Dashboard Agent 内存中的数据结构
self._components = {
    "worker_abc123": Component {
        _last_reported_time: 1712900000.123,  # 每次上报更新
        _metrics: {
            "multishot_pipeline_slice_total": OpencensusProxyMetric {
                _data: {
                    ("video_001", "processed"): CountAggregationData(10),
                    ("video_002", "processed"): CountAggregationData(15),
                    ("video_003", "processed"): CountAggregationData(8),
                    # ... (所有处理过的视频，持续累积，永不删除单个 label)
                }
            }
        }
    },
    "worker_def456": Component { ... },
    "worker_ghi789": Component { ... },
}
```

---

## 8. Worker 复用与指标累积

### 8.1 ActorPool 模式（Ray Data 默认）

```
┌─────────────────────────────────────────────────────────────────────┐
│                      ActorPool 模式                                  │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│   Actor 1 (长期存活)          Actor 2 (长期存活)                    │
│   ┌─────────────────┐         ┌─────────────────┐                   │
│   │ __init__() ─────│─────────│→ 1次            │                   │
│   │ setup()   ─────│─────────│→ 1次            │                   │
│   │                 │         │                 │                   │
│   │ process(row1)   │         │ process(row2)   │                   │
│   │ process(row3)   │         │ process(row4)   │ ← 同一进程复用    │
│   │ process(row5)   │         │ process(row6)   │                   │
│   │ ...             │         │ ...             │                   │
│   │                 │         │                 │                   │
│   │ Counter 实例    │         │ Counter 实例    │ ← 累积所有 labels │
│   │ (持续存在)      │         │ (持续存在)      │                   │
│   └─────────────────┘         └─────────────────┘                   │
│                                                                     │
│   ✅ setup() 只调用一次                                              │
│   ✅ Counter 复用，累积所有数据                                       │
│   ⚠️ 维度爆炸：所有 blobstore_id 都保留                              │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 8.2 TaskPool 模式

```
┌─────────────────────────────────────────────────────────────────────┐
│                      TaskPool 模式                                   │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│   Task 1 (短期)    Task 2 (短期)    Task 3 (短期)    Task 4 (短期)  │
│   ┌───────────┐    ┌───────────┐    ┌───────────┐    ┌───────────┐  │
│   │__init__() │    │__init__() │    │__init__() │    │__init__() │  │
│   │setup()    │    │setup()    │    │setup()    │    │setup()    │  │
│   │process()  │    │process()  │    │process()  │    │process()  │  │
│   │  ↓ 结束   │    │  ↓ 结束   │    │  ↓ 结束   │    │  ↓ 结束   │  │
│   └───────────┘    └───────────┘    └───────────┘    └───────────┘  │
│        ↓                ↓                ↓                ↓         │
│   Worker 可能          可能复用         可能新建         可能复用    │
│   被复用               同一Worker       新Worker         同一Worker  │
│                                                                     │
│   ⚠️ 每个 Task：__init__() + setup() 都会调用                        │
│   ⚠️ Counter 每次重新初始化                                          │
│   ⚠️ 但 Worker 进程可能复用，metrics 在 worker 层面累积              │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 8.3 Worker 和 Task 的关系

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Worker Process (CoreWorker)                       │
│                                                                     │
│   Ray Worker 是一个长期运行的进程，可以执行多个 Task                  │
│                                                                     │
│   ┌─────────────────────────────────────────────────────────────┐   │
│   │  Worker Process (PID: 12345)                                │   │
│   │                                                             │   │
│   │  时间 t1: 执行 Task 1                                       │   │
│   │           └─ VideoPreprocessMapper.__init__()               │   │
│   │           └─ VideoPreprocessMapper.setup()     ← Counter A  │   │
│   │           └─ VideoPreprocessMapper.process()                │   │
│   │           └─ Task 完成，对象被销毁                           │   │
│   │                                                             │   │
│   │  时间 t2: 执行 Task 2 (同一 Worker 进程)                    │   │
│   │           └─ VideoPreprocessMapper.__init__()               │   │
│   │           └─ VideoPreprocessMapper.setup()     ← Counter B  │   │
│   │           └─ VideoPreprocessMapper.process()   (新实例!)    │   │
│   │           └─ Task 完成，对象被销毁                           │   │
│   │                                                             │   │
│   └─────────────────────────────────────────────────────────────┘   │
│                                                                     │
│   关键：Counter A, B 是不同的 Python 对象，但它们都通过             │
│   同一个 CoreWorker 上报 metrics，最终汇聚到同一个 worker_id         │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 8.4 Metrics 在 TaskPool 模式下的行为

```
Worker Process (被多个 Task 复用)
├── Task 1: Counter.inc(tags={"blobstore_id": "video_001"})
│           ↓ 上报到 CoreWorker
├── Task 2: Counter.inc(tags={"blobstore_id": "video_002"})
│           ↓ 上报到 CoreWorker (同一个 worker_id)
├── Task 3: Counter.inc(tags={"blobstore_id": "video_003"})
│           ↓ 上报到 CoreWorker
└── ...

CoreWorker 内部的 metrics_map:
{
    "multishot_pipeline_slice_total": {
        ("video_001", "processed"): 10,  // 来自 Task 1
        ("video_002", "processed"): 15,  // 来自 Task 2
        ("video_003", "processed"): 8,   // 来自 Task 3
        ...  // 所有 Task 的数据都累积在同一个 Worker 中
    }
}
```

### 8.5 对比总结

| 维度 | ActorPool (默认) | TaskPool |
|------|------------------|----------|
| `__init__()` 调用次数 | Driver 1次 + 每个 Actor 1次 | **每个 Task 都调用** |
| `setup()` 调用次数 | 每个 Actor 1次 | **每个 Task 都调用** |
| Counter 实例 | 每个 Actor 1个，复用 | **每个 Task 新建** |
| Worker 进程 | 每个 Actor 独占 1个 | 共享 Worker Pool |
| Metrics 累积 | 在 Actor 内累积 | **在 Worker 进程内累积** |
| 维度爆炸 | ⚠️ 有 | ⚠️ **同样有** |

---

## 9. 指标清理机制

### 9.1 清理逻辑代码

```python
# metrics_agent.py:323-343
def clean_stale_components(self):
    """Clean dead worker's metrics."""
    with self._components_lock:
        stale_components = []
        stale_component_ids = []
        for id, component in self._components.items():
            elapsed = time.monotonic() - component.last_reported_time
            if elapsed > self._component_timeout_s:  # 默认 60s
                stale_component_ids.append(id)
                logger.info(
                    "Metrics from a worker ({}) is cleaned up due to "
                    "timeout. Time since last report {}s".format(id, elapsed)
                )
        for id in stale_component_ids:
            stale_components.append(self._components.pop(id))
        return stale_components
```

### 9.2 清理时机详解

| 场景 | 行为 |
|------|------|
| Worker 持续运行 | ❌ **不清理** - `last_reported_time` 持续更新 |
| 单个 blobstore_id 处理完毕，但 Worker 还在运行 | ❌ **不清理** - 没有单个 label 的清理机制 |
| Worker 进程退出 | ✅ **60s 后清理** - 整个 Component 被移除 |
| Ray Data 作业完成 | 取决于 Worker 是否退出 |
| Ray 集群关闭 | ✅ **清理** - 所有进程退出 |

### 9.3 清理时机图示

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                              清理时机                                         │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  情况 1: Worker 持续运行                                                     │
│  ═══════════════════════                                                     │
│                                                                              │
│  Worker 每 ~1s 上报 metrics → last_reported_time 持续更新                   │
│  → elapsed 永远 < 60s                                                        │
│  → 不触发清理                                                                │
│  → 所有历史 label 组合永久保留                                               │
│                                                                              │
│  ❌ 不清理                                                                   │
│                                                                              │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  情况 2: 单个 blobstore_id 处理完毕，但 Worker 还在运行                      │
│  ═══════════════════════════════════════════════════════                     │
│                                                                              │
│  video_001 处理完成 → 不再有新的 inc() 调用                                  │
│  但 Worker 继续处理 video_002, video_003...                                  │
│  → Worker 仍在上报 (包含 video_001 的历史数据)                               │
│  → last_reported_time 持续更新                                               │
│  → video_001 的 label 组合永久保留                                           │
│                                                                              │
│  ❌ 不清理（没有单个 label 的清理机制）                                      │
│                                                                              │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  情况 3: Worker 进程退出                                                     │
│  ═══════════════════════                                                     │
│                                                                              │
│  Worker 退出 → 停止上报                                                      │
│  → last_reported_time 不再更新                                               │
│  → 等待 60s (RAY_WORKER_TIMEOUT_S)                                          │
│  → clean_stale_components() 清理整个 Component                              │
│  → 该 Worker 的所有 metrics 和所有 label 组合被移除                          │
│                                                                              │
│  ✅ 清理（整个 Worker 粒度）                                                 │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 10. 维度爆炸问题分析

### 10.1 问题代码

```python
# video_preprocess_mapper.py:280-283
self.slice_counter.inc(
    slice_count,
    tags={"blobstore_id": blobstore_id, "status": "processed"},
)

# video_inference_mapper.py:374
self.segment_counter.inc(
    processed_count,
    tags={"blobstore_id": blobstore_id, "status": "processed"}
)
```

**问题：**
- `blobstore_id` 是每个视频文件的唯一标识
- 每处理一个新的视频，就会创建一个新的 label 组合
- 这些 label 组合会**一直保留在内存中**，直到 Worker 超时或退出

### 10.2 增长模型

```
时间线 →

t=0    Worker 启动
       └─ _data = {}

t=1    处理 video_001
       └─ _data = {("video_001", "ok"): 10}
       └─ 上报时间序列数: 1

t=2    处理 video_002
       └─ _data = {("video_001", "ok"): 10, ("video_002", "ok"): 15}
       └─ 上报时间序列数: 2

t=3    处理 video_003
       └─ _data = {("video_001", "ok"): 10, ("video_002", "ok"): 15, ("video_003", "ok"): 8}
       └─ 上报时间序列数: 3

...

t=N    处理 video_N
       └─ _data = {("video_001", "ok"): 10, ..., ("video_N", "ok"): X}
       └─ 上报时间序列数: N
       └─ 即使 video_001 不再处理，它的 label 组合仍然保留
```

### 10.3 内存和性能影响

```
假设：
- 作业处理 N 个视频
- 使用 W 个 Worker
- 每个视频产生 2 个 label 组合 (status=processed, status=error)

Dashboard Agent 内存中的时间序列数:

  时间序列数 ≈ N × 2 (最坏情况，每个视频都有 processed 和 error)

  实际情况：N × 1~2 (大多数视频只有 processed)

  示例：
  - 处理 10,000 个视频 → ~10,000-20,000 个时间序列
  - 处理 100,000 个视频 → ~100,000-200,000 个时间序列
  - 处理 1,000,000 个视频 → ~1,000,000-2,000,000 个时间序列

  每个时间序列内存占用约 100-200 bytes
  1,000,000 时间序列 ≈ 100-200 MB 内存
```

---

## 11. 多作业场景分析

### 11.1 Worker 复用场景

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                    Worker 复用场景下的指标累积                               │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  时间线 →                                                                    │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                    Worker Process (PID: 12345)                      │    │
│  │                                                                     │    │
│  │  作业 A:                                                            │    │
│  │  ├─ inc({blobstore_id="a_001"}) → metrics_map["a_001"] = 10        │    │
│  │  └─ inc({blobstore_id="a_002"}) → metrics_map["a_002"] = 15        │    │
│  │                                                                     │    │
│  │  作业 B (Worker 复用):                                              │    │
│  │  ├─ inc({blobstore_id="b_001"}) → metrics_map["b_001"] = 8         │    │
│  │  └─ 此时 a_001, a_002 仍在 metrics_map 中！                        │    │
│  │                                                                     │    │
│  │  作业 C (Worker 复用):                                              │    │
│  │  ├─ inc({blobstore_id="c_001"}) → metrics_map["c_001"] = 12        │    │
│  │  └─ 此时 a_001, a_002, b_001 都仍在！                              │    │
│  │                                                                     │    │
│  │  最终 metrics_map = {                                               │    │
│  │      "a_001": 10,  ← 作业 A 的残留                                 │    │
│  │      "a_002": 15,  ← 作业 A 的残留                                 │    │
│  │      "b_001": 8,   ← 作业 B 的残留                                 │    │
│  │      "c_001": 12,  ← 当前作业 C                                    │    │
│  │  }                                                                  │    │
│  │                                                                     │    │
│  │  每次 Prometheus scrape 都会获取所有历史数据                        │    │
│  │                                                                     │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

### 11.2 Worker 退出场景

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                    Worker 退出后被清理                                       │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  作业 A 完成                                                                 │
│       ↓                                                                      │
│  Worker 退出（如果不被复用）                                                 │
│       ↓                                                                      │
│  等待 60s (RAY_WORKER_TIMEOUT_S)                                            │
│       ↓                                                                      │
│  Dashboard Agent 清理该 Worker 的所有 metrics                               │
│       ↓                                                                      │
│  作业 B 启动，新 Worker                                                      │
│       ↓                                                                      │
│  从零开始，没有作业 A 的残留                                                 │
│                                                                              │
│  ✅ 这种情况不会有历史作业的指标残留                                         │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 12. Ray 内置指标对比

### 12.1 Ray Core 内置指标的 Tag 设计

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                    Ray Core 内置指标 Tag 分析                                │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  低基数 (Low Cardinality) - ✅ 不会爆炸                            │    │
│  ├─────────────────────────────────────────────────────────────────────┤    │
│  │                                                                     │    │
│  │  ray_running_jobs                   tag_keys={}                    │    │
│  │  ray_finished_jobs                  tag_keys={}                    │    │
│  │  ray_placement_groups               tag_keys={"State", "Source"}   │    │
│  │  ray_gcs_actors_count               tag_keys={"State"}             │    │
│  │  ray_object_store_memory            tag_keys={"Location", "ObjectState"} │
│  │  ray_total_lineage_bytes            tag_keys={}                    │    │
│  │                                                                     │    │
│  │  这些 tag 的基数是有限的:                                           │    │
│  │  - State: 枚举值，如 PENDING, RUNNING, FINISHED                    │    │
│  │  - Source: 枚举值，如 "gcs", "executor"                            │    │
│  │  - Location: 枚举值，如 MMAP_SHM, MMAP_DISK                        │    │
│  │                                                                     │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  中等基数 - ⚠️ 可能累积但可控                                      │    │
│  ├─────────────────────────────────────────────────────────────────────┤    │
│  │                                                                     │    │
│  │  ray_tasks                                                          │    │
│  │    tag_keys={"State", "Name", "Source", "IsRetry", "JobId"}        │    │
│  │                                                                     │    │
│  │  ray_actors                                                         │    │
│  │    tag_keys={"State", "Name", "Source", "JobId"}                   │    │
│  │                                                                     │    │
│  │  ray_job_duration_s                                                 │    │
│  │    tag_keys={"JobId"}                                              │    │
│  │                                                                     │    │
│  │  ⚠️ JobId: 每个 Ray Job 有唯一 ID                                  │    │
│  │  ⚠️ Name: Task/Actor 的函数名/类名 (通常有限)                      │    │
│  │                                                                     │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

### 12.2 Ray Data 内置指标

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                    Ray Data 内置指标 - 会累积                                │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  位置: _StatsActor (Driver 节点)                                             │
│                                                                              │
│  tag_keys:                                                                   │
│    - ("dataset", "operator")                                                │
│    - ("dataset", "job_id", "start_time")                                    │
│    - ("dataset",)                                                           │
│                                                                              │
│  dataset tag 格式: "dataset_<uuid>" 或类似的唯一标识                         │
│                                                                              │
│  作业 A (3 个 Dataset):                                                      │
│    data_output_rows{dataset="ds_abc", operator="Map"} 1000                  │
│    data_output_rows{dataset="ds_def", operator="Map"} 2000                  │
│    data_output_rows{dataset="ds_ghi", operator="Write"} 1500                │
│                                                                              │
│  作业 B (2 个 Dataset):                                                      │
│    data_output_rows{dataset="ds_jkl", operator="Map"} 800                   │
│    data_output_rows{dataset="ds_mno", operator="Filter"} 600                │
│                                                                              │
│  ⚠️ 所有历史 Dataset 的指标都会持续上报（如果 _StatsActor 存活）            │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

### 12.3 时间序列数量对比

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                    时间序列数量估算（长期运行集群）                           │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  假设: 集群运行 30 天，每天 10 个 Ray Data 作业，每个作业处理 10,000 个视频  │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  Ray Core 指标 (JobId 维度)                                        │    │
│  │  ────────────────────────────                                       │    │
│  │  Jobs: 30 × 10 = 300 个                                            │    │
│  │  ray_job_duration_s: 300 时间序列                                  │    │
│  │  ray_tasks (假设 5 种 task × 5 states): 300 × 25 = 7,500 时间序列 │    │
│  │  总计: ~10,000 - 50,000 时间序列                                   │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  Ray Data 指标 (dataset 维度)                                      │    │
│  │  ────────────────────────────                                       │    │
│  │  假设每个作业 2 个 Dataset，每个 Dataset 3 个 Operator             │    │
│  │  Datasets: 300 × 2 = 600 个                                        │    │
│  │  data_output_rows: 600 × 3 = 1,800 时间序列                        │    │
│  │  其他 ~20 种指标: 600 × 20 = 12,000 时间序列                       │    │
│  │  总计: ~15,000 - 30,000 时间序列                                   │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  自定义指标 (blobstore_id 维度) 🔴                                 │    │
│  │  ────────────────────────────────                                   │    │
│  │  Videos: 30 × 10 × 10,000 = 3,000,000 个                           │    │
│  │  multishot_pipeline_slice_total: 3,000,000 × 2 = 6,000,000 时间序列│    │
│  │  multishot_inference_segments: 3,000,000 × 2 = 6,000,000 时间序列  │    │
│  │  总计: ~12,000,000 时间序列 🔴🔴🔴                                 │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  对比:                                                                       │
│  - Ray Core: ~50,000                                                        │
│  - Ray Data: ~30,000                                                        │
│  - 自定义指标: ~12,000,000  (差 2 个数量级!)                                │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

### 12.4 维度基数对比

| 维度 | 基数 | 增长速度 |
|------|------|----------|
| State | 5-10 | 固定 |
| JobId | ~10/天 | 线性，缓慢 |
| dataset | ~20/天 | 线性，缓慢 |
| Name (Task/Actor) | ~10-50 | 基本固定 |
| **blobstore_id** | **~10,000+/天** | **线性，极快** |

---

## 13. Prometheus Staleness 机制

### 13.1 为什么查不到之前的 dataset 数据？

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                    Prometheus Staleness 机制                                 │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Prometheus 有一个 5 分钟的 staleness timeout：                              │
│                                                                              │
│  如果一个时间序列在最近 5 分钟内没有新的数据点，                             │
│  Prometheus 认为这个时间序列已经 "stale"（过期）                             │
│                                                                              │
│  时间线 →                                                                    │
│                                                                              │
│  t=0        t=1min      t=5min      t=10min     t=15min (现在)              │
│  │          │           │           │           │                           │
│  ▼          ▼           ▼           ▼           ▼                           │
│  ●──────────●───────────●───────────○           ○                           │
│  ↑          ↑           ↑           ↑           ↑                           │
│  上报       上报        最后上报     无数据      查询                        │
│                         (dataset A)  (stale)     (返回空)                   │
│                                                                              │
│  5 分钟后，data_output_rows{dataset="ds_abc"} 变成 stale                    │
│  → 在 Grafana 中查询当前值时，返回 "no data"                                │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

### 13.2 两个不同的概念

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                    指标上报 vs 指标查询                                      │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  1. Dashboard Agent 内存中的指标                                             │
│  ════════════════════════════════                                            │
│                                                                              │
│  OpencensusProxyMetric._data = {                                            │
│      ("dataset_abc", "Map"): 1000,   ← 一直保留在内存中                     │
│      ("dataset_def", "Map"): 2000,   ← 一直保留在内存中                     │
│  }                                                                           │
│                                                                              │
│  ✅ 这些数据确实一直在内存中，每次 scrape 都会返回                           │
│                                                                              │
│  2. Prometheus 存储的时间序列                                                │
│  ════════════════════════════════                                            │
│                                                                              │
│  Prometheus 每 15-30s scrape 一次:                                          │
│                                                                              │
│  t=0:   data_output_rows{dataset="ds_abc"} 1000  → 存储                    │
│  t=15s: data_output_rows{dataset="ds_abc"} 1000  → 存储                    │
│  t=30s: data_output_rows{dataset="ds_abc"} 1000  → 存储                    │
│  ...                                                                         │
│                                                                              │
│  ✅ 历史数据都在 Prometheus 中                                               │
│                                                                              │
│  3. Grafana 查询                                                             │
│  ════════════════                                                            │
│                                                                              │
│  情况 A: _StatsActor 仍在上报                                               │
│    → 每次 scrape 都有新数据点                                               │
│    → 查询返回最新值 1000                                                    │
│                                                                              │
│  情况 B: _StatsActor 退出了 (Driver 退出)                                   │
│    → 没有新数据点                                                            │
│    → 5 分钟后 staleness timeout                                             │
│    → 查询 "当前值" 返回 no data                                             │
│    → 但查询 "历史范围" 仍能看到数据                                         │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

### 13.3 _StatsActor 存活 vs 退出

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                             │
│  _StatsActor 存活时:                                                        │
│  ═══════════════════                                                        │
│                                                                             │
│  Dataset A 完成 → Dataset B 完成 → Dataset C 运行中                        │
│                                                                             │
│  _StatsActor 持续上报:                                                      │
│    data_output_rows{dataset="ds_A"} 1000  ← 仍在上报                       │
│    data_output_rows{dataset="ds_B"} 2000  ← 仍在上报                       │
│    data_output_rows{dataset="ds_C"} 500   ← 正在上报                       │
│                                                                             │
│  ✅ Grafana 查询当前值：能看到 ds_A, ds_B, ds_C 所有数据                   │
│  ✅ 这就是"维度累积"场景                                                   │
│                                                                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  _StatsActor 退出后:                                                        │
│  ═══════════════════                                                        │
│                                                                             │
│  Driver 退出 → _StatsActor 退出 → 停止上报                                 │
│                                                                             │
│  Prometheus scrape:                                                         │
│    t=0:    收到 ds_A=1000, ds_B=2000, ds_C=500                             │
│    t=15s:  scrape 目标不存在 / 返回空                                      │
│    t=5min: staleness timeout                                                │
│                                                                             │
│  ❌ Grafana 查询当前值：no data                                             │
│  ✅ Grafana 查询历史范围：能看到退出前的数据                                │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 13.4 查询总结

| 状态 | 上报 | 当前查询 | 历史查询 |
|------|------|----------|----------|
| _StatsActor 存活 | ✅ 持续上报所有历史 dataset | ✅ 能查到 | ✅ 能查到 |
| _StatsActor 退出 | ❌ 停止 | ❌ 5分钟后 no data | ✅ 能查到 |

### 13.5 验证方法

```
方法 1: 使用 Range Query 查看历史
════════════════════════════════

在 Grafana 中：
- 选择一个 Graph panel (不是 Stat 或 Table)
- 时间范围选择包含之前作业的时间段
- 查询: data_output_rows
- 应该能看到历史曲线

方法 2: 使用 last_over_time() 函数
══════════════════════════════════

# 获取过去 1 小时内最后一个值（即使现在 stale）
last_over_time(data_output_rows{dataset="ds_xxx"}[1h])

方法 3: 直接查询 Prometheus API
════════════════════════════════

# 查询历史范围
curl 'http://prometheus:9090/api/v1/query_range?
  query=data_output_rows&
  start=2024-04-10T00:00:00Z&
  end=2024-04-12T00:00:00Z&
  step=60s'
```

---

## 14. 最佳实践建议

### 14.1 避免维度爆炸

**核心原则：** Prometheus 指标的 label 应该是**有限集合**（如 status, bucket_name, node_id），而不是**无限集合**（如 blobstore_id, request_id, user_id）。

### 14.2 推荐的修复方案

```python
# 方案1：移除高基数 tag
self.slice_counter = Counter(
    "multishot_pipeline_slice_total",
    description="Total number of slices processed",
    tag_keys=("status",),  # 只保留低基数 tag
)

# 方案2：使用聚合维度（推荐）
self.slice_counter = Counter(
    "multishot_pipeline_slice_total",
    description="Total number of slices processed",
    tag_keys=("status", "bucket_name"),  # bucket_name 基数有限
)

# 方案3：如果需要追踪每个视频，使用日志而不是指标
self.logger.info(f"Processed video blobstore_id={blobstore_id} slices={slice_count}")
```

### 14.3 Tag 基数设计原则

| Tag 类型 | 基数 | 推荐度 | 示例 |
|----------|------|--------|------|
| 枚举值 | 固定 | ✅ 推荐 | status, state, type |
| 有限集合 | 有限 | ✅ 推荐 | bucket_name, node_id, operator |
| 聚合维度 | 较少 | ⚠️ 谨慎 | job_id, dataset |
| 唯一标识 | 无限 | ❌ 避免 | blobstore_id, request_id, user_id |

---

## 附录：关键代码位置

| 文件 | 行号 | 说明 |
|------|------|------|
| `reporter_agent.py` | 566-579 | `ReportOCMetrics()` 接收 Worker 上报 |
| `metrics_agent.py` | 722-740 | `proxy_export_metrics()` 代理导出 |
| `metrics_agent.py` | 323-343 | `clean_stale_components()` 清理机制 |
| `metrics_agent.py` | 277-278 | `_component_timeout_s` 默认 60s |
| `stats.py` | 687-693 | Ray Data 的 dataset eviction |
| `ray/util/metrics.py` | 172-237 | Counter/Gauge 实现 |

---

# Part 3: Autoscaler 节点移除机制

## 15. Autoscaler 日志分析

### 15.1 日志含义

当看到以下日志时：

```
(autoscaler +3m12s) Removing 1 nodes of type node_091f... (max number of worker nodes reached)
```

**含义：**
- Autoscaler 检测到当前 Worker 节点数量超过了允许的最大值
- 正在移除 1 个类型为 `node_091f...` 的节点
- 原因：达到了 `max_workers` 限制

### 15.2 触发原因

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                    max_workers 限制触发场景                                   │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  场景 1: 手动扩容后超限                                                      │
│  ═══════════════════════                                                     │
│                                                                              │
│  max_workers = 10                                                            │
│  当前节点数 = 11 (通过 kubectl scale 或其他方式增加)                         │
│  → Autoscaler 检测到超限                                                     │
│  → 移除 1 个节点                                                             │
│                                                                              │
│  场景 2: 配置变更后超限                                                      │
│  ═══════════════════════                                                     │
│                                                                              │
│  原配置: maxReplicas = 15                                                    │
│  当前节点数 = 12                                                             │
│  新配置: maxReplicas = 10                                                    │
│  → 当前 12 > 新的 max_workers 10                                            │
│  → 移除 2 个节点                                                             │
│                                                                              │
│  场景 3: Autoscaler 收缩                                                     │
│  ═══════════════════════                                                     │
│                                                                              │
│  资源需求下降 → Autoscaler 缩减节点数                                        │
│  但在缩减过程中仍需遵守 max_workers 限制                                     │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 16. max_workers 参数来源

### 16.1 KubeRay 中的配置

在 KubeRay 中，即使没有显式配置 `max_workers`，该参数也会从 RayCluster CR 的 workerGroup 配置中计算得出。

```yaml
# RayCluster CR 示例
apiVersion: ray.io/v1alpha1
kind: RayCluster
spec:
  workerGroupSpecs:
    - groupName: worker-group-1
      replicas: 2           # 当前副本数
      minReplicas: 1        # 最小副本数
      maxReplicas: 10       # 最大副本数 ← 这里决定 max_workers
      numOfHosts: 1         # 每个副本的主机数 (可选，默认1)
      ...
    - groupName: worker-group-2
      replicas: 1
      minReplicas: 1
      maxReplicas: 5        # 另一个 workerGroup 的最大副本数
      ...
```

### 16.2 max_workers 计算逻辑

```python
# autoscaling_config.py 中的计算逻辑

# 每个 workerGroup 的 max_workers
max_workers = group_spec["maxReplicas"] * group_spec.get("numOfHosts", 1)

# 全局 max_workers = 所有 workerGroup 的 max_workers 之和
global_max_workers = sum(
    node_type["max_workers"] for node_type in available_node_types.values()
)
```

**示例计算：**

```
workerGroupSpecs:
  - groupName: worker-group-1
    maxReplicas: 10
    numOfHosts: 1
    → max_workers = 10 × 1 = 10

  - groupName: worker-group-2
    maxReplicas: 5
    numOfHosts: 2
    → max_workers = 5 × 2 = 10

全局 max_workers = 10 + 10 = 20
```

### 16.3 关键代码路径

```python
# python/ray/autoscaler/_private/kuberay/autoscaling_config.py

def _get_node_type(
    group_spec: Dict[str, Any],
    head_node: bool = False,
    redis_password: Optional[str] = None,
) -> Tuple[str, Dict[str, Any]]:
    # ...

    max_workers = group_spec["maxReplicas"] * group_spec.get("numOfHosts", 1)

    node_type = {
        "max_workers": max_workers,
        "min_workers": group_spec["minReplicas"] * group_spec.get("numOfHosts", 1),
        # ...
    }

    return name, node_type
```

---

## 17. 节点移除优先级

### 17.1 问题：运行任务的节点会被移除吗？

**不是一定会移除正在运行任务的节点**，但如果所有节点都在运行任务，仍然会选择移除。

### 17.2 节点选择排序逻辑

```python
# python/ray/autoscaler/v2/scheduler.py:1196-1229

def _sort_nodes_for_termination(node: SchedulingNode) -> Tuple:
    """
    排序节点以确定终止优先级
    返回的元组用于排序，值越小的越先被终止
    """
    # 1. Ray 是否在运行 (False=0 排在 True=1 前面)
    running_ray = len(node.ray_node_id) > 0

    # 2. 空闲时间 (负数，越大的负数=空闲越久，排在前面)
    idle_dur = -1 * node.idle_duration_ms

    # 3. 平均资源利用率 (越低越先被终止)
    utils_per_resources = {}
    for resource, total in node.total_resources.items():
        if total > 0:
            available = node.available_resources.get(resource, 0)
            utils_per_resources[resource] = (total - available) / total

    avg_util = sum(utils_per_resources.values()) / len(utils_per_resources)

    return (running_ray, idle_dur, avg_util)
```

### 17.3 终止优先级（从高到低）

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                    节点终止优先级                                             │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  优先级 1: Ray 未运行的节点 (running_ray=False)                              │
│  ═══════════════════════════════════════════════                             │
│  - 节点上的 Ray 进程还没启动                                                 │
│  - 最先被移除，因为影响最小                                                  │
│                                                                              │
│  优先级 2: 空闲时间更长的节点 (idle_dur 更小/负数更大)                       │
│  ═══════════════════════════════════════════════════                         │
│  - 长时间没有任务调度到该节点                                                │
│  - 空闲 10 分钟的节点比空闲 1 分钟的节点更先被移除                          │
│                                                                              │
│  优先级 3: 资源利用率更低的节点 (avg_util 更小)                              │
│  ═══════════════════════════════════════════════                             │
│  - 正在运行的任务较少                                                        │
│  - 利用率 20% 的节点比利用率 80% 的节点更先被移除                           │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

### 17.4 关键点

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                    关键理解                                                   │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ⚠️ 排序只是优先级，不是保护机制                                             │
│                                                                              │
│  如果需要移除 N 个节点，会选择排序后的前 N 个                                │
│                                                                              │
│  即使所有节点都在运行任务，仍然会移除利用率最低的那些                        │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

### 17.5 场景示例

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                    场景 1: 存在空闲节点                                       │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  max_workers = 10，当前有 11 个节点                                          │
│  - 5 个节点空闲 (idle_dur > 0)                                              │
│  - 6 个节点运行任务 (idle_dur = 0)                                          │
│                                                                              │
│  结果: 会优先移除 1 个空闲时间最长的空闲节点                                 │
│                                                                              │
│  ✅ 运行任务的节点不受影响                                                   │
│                                                                              │
├──────────────────────────────────────────────────────────────────────────────┤
│                    场景 2: 所有节点都在运行任务                               │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  max_workers = 10，当前有 11 个节点                                          │
│  - 所有节点都在运行任务 (idle_dur = 0)                                      │
│  - 节点利用率: 80%, 60%, 70%, 50%, 90%, 40%, 85%, 75%, 65%, 55%, 45%        │
│                                                                              │
│  结果: 会移除利用率最低的 1 个节点 (40% 那个)                                │
│                                                                              │
│  ⚠️ 该节点上的任务会被中断                                                   │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 18. 被终止节点上的任务处理

### 18.1 任务中断后果

当一个运行任务的节点被终止时：

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                    任务中断处理                                               │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  节点被终止                                                                   │
│       ↓                                                                      │
│  该节点上的所有任务失败                                                       │
│       ↓                                                                      │
│  Ray Fault Tolerance 机制启动                                                │
│       ↓                                                                      │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  如果配置了重试 (max_retries > 0):                                 │    │
│  │    → 任务会在其他节点重新调度执行                                   │    │
│  │    → 可能导致重复执行（对于非幂等任务）                             │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  如果没有配置重试:                                                  │    │
│  │    → 任务永久失败                                                   │    │
│  │    → 错误传播到调用方                                               │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

### 18.2 Actor 的处理

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                    Actor 中断处理                                             │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Actor 节点被终止                                                            │
│       ↓                                                                      │
│  Actor 状态丢失                                                              │
│       ↓                                                                      │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  如果配置了 max_restarts:                                          │    │
│  │    → Actor 在其他节点重新创建                                       │    │
│  │    → 需要重新初始化状态                                             │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  如果没有配置重启:                                                  │    │
│  │    → Actor 永久失败                                                 │    │
│  │    → 所有挂起的方法调用失败                                         │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 19. 相关代码位置

| 文件 | 行号/函数 | 说明 |
|------|-----------|------|
| `scheduler.py` | `_enforce_max_workers_global()` | 强制执行全局 max_workers 限制 |
| `scheduler.py` | `_select_nodes_to_terminate()` | 选择要终止的节点 |
| `scheduler.py` | `_sort_nodes_for_termination()` | 节点终止优先级排序 |
| `autoscaling_config.py` | `_get_node_type()` | 计算每个 workerGroup 的 max_workers |
| `event_logger.py` | `print_cluster_event()` | 输出 autoscaler 日志 |

---

## 20. 最佳实践建议

### 20.1 配置建议

```yaml
# RayCluster CR 配置建议
spec:
  workerGroupSpecs:
    - groupName: worker-group
      minReplicas: 1
      maxReplicas: 20         # 设置足够的上限，避免意外移除
      # ...
```

### 20.2 避免意外移除

1. **设置足够的 maxReplicas**：确保 maxReplicas 大于等于预期的最大节点数

2. **监控 autoscaler 日志**：关注 "max number of worker nodes reached" 消息

3. **使用 Placement Group**：对于关键任务，使用 Placement Group 保证资源分配

4. **配置任务重试**：
   ```python
   @ray.remote(max_retries=3)
   def my_task():
       ...
   ```

5. **Actor 配置重启**：
   ```python
   @ray.remote(max_restarts=3, max_task_retries=3)
   class MyActor:
       ...
   ```
