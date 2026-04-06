# Ray Data Dataset 状态分析：PENDING → RUNNING 转换机制

## 背景

分析作业 Job 27000000（`search-kibana-usa.corp.kuaishou.com/#/jobs/27000000`）中 Dataset 状态一直显示 PENDING 的原因，以及日志中间行为空、没有 Input 节点信息的问题。

## 作业概况

- **Pipeline**: `multishot_video_classifier_pipeline_checkpoint.py`
- **Processing Mode**: streaming（`--processing-mode streaming --streaming-mode qwenvl`）
- **集群资源**: 36620 CPU, 500 GPU, 18.2TiB object store
- **关键参数**:
  - `--cpu-concurrency 500`
  - `--gpu-concurrency 2`
  - `--streaming-gpu-concurrency 500`
  - `--streaming-num-gpus 1`（每个 QwenVL Actor 占 1 GPU）

## 问题现象

### 现象 1：Dataset 长时间 PENDING

Dashboard 上 Dataset 和 Operator 状态持续显示 PENDING。

### 现象 2：日志中间行为空

```
2026-05-07 09:49:59,806 INFO logging_progress.py:227 -- Active & requested resources: 350/3.662e+04 CPU, 350/500 GPU, 0.0B/18.2TiB object store (pending: 150 CPU, 150 GPU)
2026-05-07 09:49:59,806 INFO logging_progress.py:181 --
2026-05-07 09:49:59,806 INFO logging_progress.py:231 -- ReadParquet->Filter(is_multi_shot_detect_enabled)->Map(VideoClipProcessMapper)->Filter(<lambda>): 0/1
```

中间行（`logging_progress.py:181`）一直为空。

### 现象 3：没有 Input 节点信息

正常作业有 `Input` 节点（如 `Input: 10000/10000 FINISHED`），但本作业没有。

---

## 根因分析

### 一、Dataset/Operator 显示 PENDING 的原因

#### 1.1 状态初始化

**文件**: `python/ray/data/_internal/stats.py:600-622`

```python
def register_dataset(self, ...):
    self.datasets[dataset_tag] = {
        "state": DatasetState.PENDING.name,          # Dataset 初始状态 PENDING
        "operators": {
            operator: {
                "state": DatasetState.PENDING.name,  # 每个 Operator 初始状态也是 PENDING
            }
            for operator in operator_tags
        },
    }
```

#### 1.2 状态更新为 RUNNING 的代码

**文件**: `python/ray/data/_internal/execution/streaming_executor.py:885`

```python
def _scheduling_loop_step(self, topology: Topology) -> bool:
    # ... 调度逻辑 ...
    update_operator_states(topology)
    self._refresh_progress_manager(topology)
    self._update_stats_metrics(state=DatasetState.RUNNING.name)  # ← 每次调度循环设为 RUNNING
```

#### 1.3 更新节流机制（关键！）

**文件**: `python/ray/data/_internal/execution/streaming_executor.py:108, 1088-1100`

```python
class StreamingExecutor:
    UPDATE_METRICS_INTERVAL_S: float = 5.0  # ← 每 5 秒才更新一次 StatsActor

    def _update_stats_metrics(self, state: str, force_update: bool = False):
        now = time.time()
        if (
            force_update
            or (now - self._metrics_last_updated) > self.UPDATE_METRICS_INTERVAL_S
        ):
            _StatsManager.update_execution_metrics(
                self._dataset_id,
                [op.metrics for op in self._topology],
                self._get_operator_tags(),
                self._get_state_dict(state=state),
            )
            self._metrics_last_updated = now
```

`UPDATE_METRICS_INTERVAL_S = 5.0`，即调度循环虽然每步都想设 RUNNING，但实际每 5 秒才向 StatsActor 发送一次更新。

#### 1.4 `_stats_actor.get_datasets.remote(job_id)` 调用返回较慢的影响

Dashboard 前端通过 API `GET /api/data/{job_id}` 获取 Dataset 状态：

**文件**: `python/ray/dashboard/modules/data/data_head.py:76-83`

```python
async def get_datasets(self, req: Request) -> Response:
    job_id = req.match_info["job_id"]
    _stats_actor = get_or_create_stats_actor()
    datasets = await _stats_actor.get_datasets.remote(job_id)  # ← RPC 调用 StatsActor
```

**影响因素**:

1. **StatsActor 是单点单线程 Actor**：
   ```python
   @ray.remote(num_cpus=0)  # stats.py:158
   class _StatsActor:
       # 单线程串行处理所有消息
   ```
   所有 Dataset 的状态更新（`update_execution_metrics`）和查询（`get_datasets`）都通过同一个 StatsActor **串行处理**。在高负载集群（3233 tasks、500 GPU 全占、665 tasks waiting）下，StatsActor 的消息队列积压严重。

2. **fire-and-forget 更新 vs 同步查询**：
   ```python
   # stats.py:873 - StreamingExecutor 发送更新（不等待结果）
   get_or_create_stats_actor().update_execution_metrics.remote(*args)

   # data_head.py:83 - Dashboard 查询（await 等待结果）
   datasets = await _stats_actor.get_datasets.remote(job_id)
   ```
   更新调用是异步的（立即返回），但 Dashboard 查询需要等待 StatsActor 处理完队列中所有排在前面的消息。

3. **RPC 序列化开销**：`get_datasets` 返回的是完整的 Dataset 状态字典（包含所有 operator 信息），数据量大时序列化/反序列化耗时增加

4. **Dashboard 轮询周期**：前端每隔几秒请求一次，如果在 StreamingExecutor 首次成功 `_update_stats_metrics`（至少 5 秒后）之前请求，看到的就是初始 PENDING 状态

**注意**：Ray Actor 方法调用走 Raylet 之间的 gRPC 直连，**不经过 GCS 主线程**。GCS 只在 Actor 创建/名称解析时参与（`get_or_create_stats_actor()` 首次调用时查询 GCS 获取 Actor 地址，后续调用使用缓存的 ActorHandle）。

**综合效果**：
```
t=0:     register_dataset() → 状态 = PENDING
t=0~5s:  调度循环启动但 UPDATE_METRICS_INTERVAL_S 未到，不更新 StatsActor
t≈5s:    首次 _update_stats_metrics 发出 → 但 StatsActor RPC 可能排队
t≈5~10s: StatsActor 实际处理更新，状态变为 RUNNING
t=?:     Dashboard 下次轮询才能看到新状态
```

在大规模集群上，这个延迟可能从理论的 5-10 秒扩大到几十秒甚至更久。

#### 1.5 Operator 状态逻辑

**文件**: `python/ray/data/_internal/execution/streaming_executor.py:1048-1050`

```python
op_info = {
    "state": DatasetState.FINISHED.name
        if op.has_execution_finished()   # 完成了就是 FINISHED
        else state,                      # 否则直接使用 dataset 整体状态（RUNNING）
}
```

Operator 没有独立的 PENDING → RUNNING 转换逻辑，它直接跟随 Dataset 整体状态。所以 Dataset PENDING 时所有 Operator 也显示 PENDING。

---

### 二、日志中间行为空的原因

中间行（`logging_progress.py:181`）打印的是 operator 详细进度信息。

**文件**: `python/ray/data/_internal/progress/logging_progress.py:180-181`

```python
# log operator-level progress
if len(self._op_progress_metrics.keys()) > 0:
    logger.info("")  # ← 这就是空行！它是分隔符
```

空行是一个固定的分隔符，在 "Total Progress / Active resources" 和 "Operator 进度" 之间打印。它本身就是设计如此，不是"应该有内容但为空"。

---

### 三、没有 Input 节点的原因

#### 3.1 Driver 日志中不显示 Input

**文件**: `python/ray/data/_internal/progress/logging_progress.py:123-126`

```python
def __init__(self, ...):
    for state in self._topology.values():
        op = state.op
        if isinstance(op, InputDataBuffer):
            continue  # ← InputDataBuffer 直接跳过，不创建进度条
```

**文件**: `python/ray/data/_internal/execution/streaming_executor.py:930-931`

```python
def _refresh_progress_manager(self, topology: Topology):
    for op_state in topology.values():
        if not isinstance(op_state.op, InputDataBuffer):  # ← 跳过 InputDataBuffer
            self._progress_manager.update_operator_progress(op_state, ...)
```

**原因**：`InputDataBuffer` 是 DAG 的虚拟根节点，只持有初始 RefBundle（metadata），不执行实际计算。Ray Data 认为它是内部实现细节，不向用户展示。

#### 3.2 Dashboard 上 InputDataBuffer 的行为

**文件**: `python/ray/data/_internal/stats.py:761-764`

```python
# Handle outlier case for InputDataBuffer, which is marked as finished
# immediately and does not have a RUNNING state.
if not operator.execution_start_time:
    operator.execution_start_time = update_time
```

InputDataBuffer 会立即标记为 FINISHED（`op.has_execution_finished()` 返回 True），在 Dashboard 中显示为 0/0 FINISHED，没有实际意义。

#### 3.3 为什么正常作业有 "Input" 节点而本作业没有

正常作业（非 streaming 模式或不同的 pipeline 结构）可能会在 ReadParquet 之后创建一个名为 `Input` 的实际算子（例如 checkpoint 恢复时的数据注入节点）。在本作业的 streaming 模式下，所有 `ReadParquet → Filter → Map → Filter` 被**算子融合（operator fusion）**为一个物理算子，不存在独立的 Input 阶段。

---

## Pipeline 执行架构

### 代码结构

**文件**: `pipeline/multi_video_classifier_merge/pipeline_builder.py`

```python
def build_pipeline(ds, ..., processing_mode="streaming", streaming_mode="qwenvl", ...):
    # Stage 0: Filter
    ds = ds.filter(is_multi_shot_detect_enabled, concurrency=500, num_cpus=1)

    # Stage 1: Map
    ds = ds.map(VideoClipProcessMapper, concurrency=500, num_cpus=1)

    # 路由过滤
    ds = ds.filter(lambda row: row.get("__route__") == "streaming", concurrency=500, num_cpus=1)

    # Repartition
    ds = ds.repartition(target_num_rows_per_block=100)

    # QwenVL Streaming（GPU 密集）
    ds = ds.map_batches(DistributedQwenVLVideoProcessMapper,
                         num_gpus=1, concurrency=500)  # 500 × 1 GPU = 500 GPU

    # Stage 5: ClipMerge
    ds = ds.flat_map(ClipMergeMapper, concurrency=500, num_cpus=1, memory=16GB)

    return ds
```

### 执行触发

**文件**: `multishot_video_classifier_pipeline_checkpoint.py`

```python
result_ds = build_pipeline(ds, ...)
result_ds.write_json(output_path, concurrency=50)  # ← 触发惰性 DAG 执行
```

所有 `filter/map/map_batches/flat_map` 都是惰性操作，只在 `write_json()` 调用时才触发 `StreamingExecutor.execute()`。

---

## 状态枚举定义

**文件**: `python/ray/data/_internal/execution/dataset_state.py`

```python
class DatasetState(enum.IntEnum):
    UNKNOWN = 0
    RUNNING = 1
    FINISHED = 2
    FAILED = 3
    PENDING = 4
```

---

## 完整状态转换流程

```
write_json()
  → Dataset._plan.execute()
    → StreamingExecutor.execute()                    # streaming_executor.py:196
      → register_dataset (Dataset=PENDING, Ops=PENDING)   # stats.py:600
      → self.start()                                 # streaming_executor.py:275
        → run()                                      # :486
          → while True:
              _scheduling_loop_step()               # :600
                → Phase 0: update_usages()
                → Phase 1: collect active tasks + backpressure
                → Phase 2: GPU-first batch processing + dispatch
                → Phase 3: Error accounting
                → Phase 4: Final dispatch
                → Phase 5: Housekeeping
                  → _update_stats_metrics(RUNNING)  # :885（每 5s 实际发送一次）
              → 循环直到所有 operator 完成
      → shutdown()
        → _update_stats_metrics(FINISHED/FAILED)    # :315-319
```

---

## 关键常量

| 常量 | 值 | 位置 | 作用 |
|------|-----|------|------|
| `UPDATE_METRICS_INTERVAL_S` | 5.0 | `streaming_executor.py:108` | StatsActor 更新节流间隔 |
| `LOG_REPORT_INTERVAL_SEC` | 10 | `logging_progress.py:99` | 日志打印间隔（可通过 `RAY_DATA_NON_TTY_PROGRESS_LOG_INTERVAL` 环境变量覆盖） |

---

## 总结

| 问题 | 根因 | 关键代码 |
|------|------|----------|
| Dataset/Operator PENDING 持续久 | 初始注册为 PENDING + 5s 更新节流 + StatsActor RPC 延迟 | `stats.py:603,613` + `streaming_executor.py:108,1088-1100` |
| `_stats_actor.get_datasets.remote()` 慢的影响 | StatsActor 单线程串行处理导致消息队列积压（**不涉及 GCS 主线程**，Actor 方法调用走 Raylet 直连），Dashboard 获取到的仍是旧 PENDING 状态 | `data_head.py:83` + `stats.py:158` |
| 日志中间行为空 | 这是固定分隔符，不是缺失内容 | `logging_progress.py:181` |
| 没有 Input 节点（日志） | `InputDataBuffer` 被显式跳过 | `logging_progress.py:125` |
| 没有 Input 节点（Dashboard） | 算子融合 + InputDataBuffer 立即 FINISHED | `streaming_executor.py:1048-1050` + `stats.py:761-764` |
