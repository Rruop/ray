# Dashboard Agent OTel 指标注册/采集死锁导致 ray job stop 卡死分析

**时间**: 2026-08-28
**集群**: kml-task-100047126-record-100658632-prod-head-h-0
**Job submission_id**: no_match_20260826_20260826_143538_face_decup
**Job ID**: 0b000000
**Driver PID**: 459801
**Dashboard Agent PID**: 996
**Session**: session_2026-08-25_15-26-35_515994_1
**Ray 版本**: 2.52.0 ~ 2.56.1（受影响版本）
**关联 PR**: https://github.com/ray-project/ray/pull/64946

## 1. 问题现象

执行 `ray job stop no_match_20260826_20260826_143538_face_decup` 后 CLI 一直卡住无响应，无法停止作业。

同时该作业中有一个 Write task（task_id=c5407dd97481f342ffffffffffffffffffffffff0b000000）已 RUNNING 12 小时。

## 2. 结论

**根因是 Ray 的 OpenTelemetryMetricRecorder 中 register 与 collect 之间存在 ABBA 死锁，冻结了 Dashboard Agent 的 asyncio event loop，导致所有 HTTP 请求（包括 job stop）无法被处理。**

死锁发生在 `open_telemetry_metric_recorder.py` 中：
- 注册路径：`self._lock` → SDK consumer lock
- 采集路径：SDK consumer lock → `self._lock`

当两条路径并发执行时形成死锁，Dashboard Agent 主线程被永久阻塞在 `futex_wait_queue` 上。

这是 Ray 的已知 bug，已在 [PR #64946](https://github.com/ray-project/ray/pull/64946) 中修复。

## 3. 排查过程

### 3.1 确认 Write task 长时间 RUNNING 的原因

**Step 1**: Head 节点确认 task 状态

```bash
ray list tasks --filter state=RUNNING --limit 1000 | grep c5407dd
```

结果：task 仍为 RUNNING，类型 `NORMAL_TASK`，函数名 `_map_task`，task 名 `Write`

**Step 2**: Head 节点确认 worker 存活

```bash
ray list workers --filter worker_id=6de62f7b698f2926ad8fc5b4ec36751409ebae9734f62df83004a0e6
```

结果：IS_ALIVE=True，PID=22662，IP=10.56.187.248

**Step 3**: Worker 节点检查进程状态

```bash
cat /proc/22662/status     # 进程存在，State: S (sleeping)
cat /proc/22662/wchan       # futex_wait_queue（在 futex 上阻塞）
cat /proc/22662/io          # rchar: 58GB, wchar: 41MB, write_bytes: 17MB
```

**Step 4**: Worker 节点获取 Python 调用栈

```bash
py-spy dump --pid 22662     # 非侵入，只读进程内存
```

结果：

```
Thread 22662 (idle): "MainThread"
    close (hdfs_native/__init__.py:120)
    close (pypaimon/filesystem/hdfs_native_file_io.py:91)
    write_once (pypaimon/filesystem/hdfs_native_file_io.py:1092)
    _run_with_retry (pypaimon/filesystem/hdfs_native_file_io.py:726)
    _write_with_retry (pypaimon/filesystem/hdfs_native_file_io.py:778)
    write_parquet (pypaimon/filesystem/hdfs_native_file_io.py:1094)
    _write_data_to_file (pypaimon/write/writer/data_writer.py:183)
    _roll_write (pypaimon/write/writer/key_value_data_writer.py:152)
    _flush_all (pypaimon/write/writer/key_value_data_writer.py:139)
    prepare_commit (pypaimon/write/writer/key_value_data_writer.py:100)
    prepare_commit (pypaimon/write/file_store_write.py:136)
    prepare_commit (pypaimon/write/table_write.py:202)
    write (pypaimon/write/ray_datasink.py:344)
    fn (ray/data/_internal/planner/plan_write_op.py:31)
    _map_task (ray/data/_internal/execution/operators/map_operator.py:776)
    main_loop (ray/_private/worker.py:1028)
```

**Write task 卡在 HDFS 文件 close 操作上**：pypaimon 写完 Parquet 文件后调用 `hdfs_native.close()` 关闭 HDFS 文件句柄，底层 Rust 库在等待 HDFS RPC 响应（NameNode complete 或 DataNode ack），可能 HDFS 连接已断开但客户端未感知。

### 3.2 确认 ray job stop 卡住的原因

**Step 1**: 测试 Dashboard API 是否可达

```bash
curl -s --max-time 5 http://127.0.0.1:8265/api/jobs/ | head -3
```

结果：**GET 请求正常返回**，Dashboard 主进程不卡。

**Step 2**: 测试 POST stop 请求

```bash
curl -s --max-time 10 -X POST http://127.0.0.1:8265/api/jobs/no_match_.../stop
```

结果：**超时无响应（exit code 28）**。

**Step 3**: 测试 Dashboard Agent 端口

```bash
curl -v --max-time 5 http://10.141.168.27:42497/
```

结果：TCP 连接能建立（`Connected to 10.141.168.27 port 42497`），但 **5 秒内 0 字节响应**。

> 注意：之前通过 `grep /proc/net/tcp` 找不到 42497 端口，是因为容器网络命名空间问题。正确方式是查 agent 进程自己的 `/proc/996/net/tcp`。

**Step 4**: 确认 Agent 进程状态

```bash
ps aux | grep "DashboardAgent"
# PID 996, 父进程是 raylet (PID 940)
cat /proc/996/status   # State: S (sleeping), 活着
cat /proc/996/wchan    # futex_wait_queue
```

Agent 进程还活着，端口在监听，但 HTTP 无响应。

**Step 5**: 获取 Agent Python 调用栈

```bash
py-spy dump --pid 996
```

关键发现：

```
Thread 996 (idle): "MainThread"
    register_asynchronous_instrument (metrics/_internal/measurement_consumer.py:89)
    create_observable_counter (metrics/_internal/__init__.py:207)
    register_counter_metric (ray/_private/telemetry/open_telemetry_metric_recorder.py:366)
    _export_number_data (ray/dashboard/modules/reporter/reporter_agent.py:645)
    Export (ray/dashboard/modules/reporter/reporter_agent.py:683)
    _run (asyncio/events.py:88)
    _run_once (asyncio/base_events.py:1999)
    run_forever (asyncio/base_events.py:645)
    run_until_complete (asyncio/base_events.py:678)
    <module> (ray/dashboard/agent.py:518)

Thread 1110 (active): "OtelPeriodicExportingMetricReader"
    callback (ray/_private/telemetry/open_telemetry_metric_recorder.py:96)
    callback (metrics/_internal/instrument.py:165)
    collect (metrics/_internal/measurement_consumer.py:119)
    collect (metrics/_internal/export/__init__.py:357)
    _ticker (metrics/_internal/export/__init__.py:551)
```

**Agent 主线程（asyncio event loop）卡在 OTel 的 `register_asynchronous_instrument` 上**，等待 SDK 内部锁；同时 OTel Reader 线程在执行 `collect` 并调用 observable callback。两条路径竞争同一把锁形成死锁。

**Step 6**: 查找 Driver PID

通过 Dashboard Job API 返回的 JSON：

```bash
curl http://127.0.0.1:8265/api/jobs/no_match_.../
```

返回：

```json
"driver_info": {"id": "0b000000", "node_ip_address": "10.141.168.27", "pid": "459801"}
```

**Step 7**: 确认 GCS OTel 指标失败

```bash
grep -c "Failed to export metrics" /tmp/ray/session_latest/logs/gcs_server.out
# 结果：15910 次
```

大量 OTel 指标导出失败印证了 OTel 管道有问题。

## 4. `ray job stop` 卡死的完整因果链

```
ray job stop
  → Dashboard 主进程(PID 334) 收到 POST /api/jobs/.../stop
    → JobHead(PID 453) 处理请求
      → 通过 driver_agent_http_address (http://10.141.168.27:42497)
        向 Dashboard Agent(PID 996) 发送 stop 请求
          → Agent 的 TCP accept 了连接（内核层面）
            → 但 Agent 主线程（asyncio event loop）卡在 OTel 死锁
              → HTTP 请求永远得不到响应
                → ray job stop 一直等
```

**关键点**：
- Dashboard 的 GET 接口正常（它不经过 Agent）
- Agent 进程还活着（PID 996），42497 端口在监听
- TCP 连接能建立（内核 accept），但 event loop 卡死导致 aiohttp 无法处理请求
- 死锁是永久的，Agent 无法自行恢复

## 5. OTel 死锁的详细代码逻辑

### 5.1 Dashboard Agent 主线程

**文件**: `python/ray/dashboard/agent.py` (line 516)

```python
loop = get_or_create_event_loop()
agent = DashboardAgent(...)
loop.run_until_complete(agent.run())    # Agent 的 asyncio event loop
```

Agent 启动后，event loop 持续运行。OTLP gRPC 的 `Export` handler 是一个 **async 函数**，被 schedule 到 event loop 中执行：

```python
async def Export(self, request, context):
    for resource_metrics in request.resource_metrics:
        for scope_metrics in resource_metrics.scope_metrics:
            for metric in scope_metrics.metrics:
                if metric.WhichOneof("data") == "histogram":
                    self._export_histogram_data(metric)
                else:
                    self._export_number_data(metric)    # 同步调用
    return metrics_service_pb2.ExportMetricsServiceResponse()
```

`_export_number_data` 是**同步方法**，在 event loop 的回调中被同步执行：

```python
def _export_number_data(self, metric: Metric) -> None:
    if metric.WhichOneof("data") == "gauge":
        self._open_telemetry_metric_recorder.register_gauge_metric(
            metric.name, metric.description,
        )
    if metric.WhichOneof("data") == "sum":
        if metric.sum.is_monotonic:
            self._open_telemetry_metric_recorder.register_counter_metric(
                metric.name, metric.description,
            )
        else:
            self._open_telemetry_metric_recorder.register_sum_metric(
                metric.name, metric.description,
            )
    for data_point in data_points:
        self._open_telemetry_metric_recorder.set_metric_value(...)
```

### 5.2 OpenTelemetryMetricRecorder 的锁结构

**文件**: `python/ray/_private/telemetry/open_telemetry_metric_recorder.py`

```python
class OpenTelemetryMetricRecorder:
    def __init__(self):
        self._lock = threading.Lock()                        # 唯一的非可重入锁
        self._registered_instruments = {}
        self._gauge_observations_by_name = defaultdict(dict)
        self._counter_observations_by_name = defaultdict(dict)
        self._sum_observations_by_name = defaultdict(dict)
        self._histogram_bucket_midpoints = defaultdict(list)
        self._init_metrics()
        self.meter = metrics.get_meter(__name__)
```

### 5.3 注册路径（Event Loop 主线程）

```python
def register_gauge_metric(self, name: str, description: str) -> None:
    with self._lock:                                          # ① 获取 self._lock
        if name in self._registered_instruments:
            return
        callback = self._create_observable_callback(name, MetricType.GAUGE)
        instrument = self.meter.create_observable_gauge(     # ② 调用 SDK，获取 SDK consumer lock
            name=f"{NAMESPACE}_{name}",
            description=description,
            unit="1",
            callbacks=[callback],
        )
        self._registered_instruments[name] = instrument
        self._gauge_observations_by_name[name] = {}

def register_counter_metric(self, name: str, description: str) -> None:
    with self._lock:                                          # ① 获取 self._lock
        if name in self._registered_instruments:
            return
        callback = self._create_observable_callback(name, MetricType.COUNTER)
        instrument = self.meter.create_observable_counter(   # ② 调用 SDK，获取 SDK consumer lock
            name=f"{NAMESPACE}_{name}",
            description=description,
            unit="1",
            callbacks=[callback],
        )
        self._registered_instruments[name] = instrument
        self._counter_observations_by_name[name] = {}
```

### 5.4 采集路径（OTel PeriodicReader 后台线程）

```python
def _create_observable_callback(self, metric_name, metric_type):
    def callback(options):
        with self._lock:                              # ③ 需要获取 self._lock
            if metric_type == MetricType.GAUGE:
                observations = self._gauge_observations_by_name.get(metric_name, {})
                self._gauge_observations_by_name[metric_name] = {}
            elif metric_type == MetricType.COUNTER:
                observations = self._counter_observations_by_name.get(metric_name, {})
            # ... 聚合逻辑 ...
            return [Observation(agg_fn(values), attributes=dict(filtered))
                    for filtered, values in values_by_filtered_tags.items()]
    return callback
```

这个 callback 在 OTel SDK 的 `PeriodicExportingMetricReader` 后台线程中被调用：

```
OtelPeriodicExportingMetricReader 线程:
    _ticker → collect → measurement_consumer.collect
      → 遍历所有 observable instrument
        → 调用 callback(options)
          → with self._lock:     ← ③ 需要 self._lock
```

### 5.5 死锁的形成（ABBA 锁序反转）

**注册路径**（主线程 / event loop）：

```
self._lock → SDK consumer lock
```

1. 获取 `self._lock`（①）
2. 在持有 `self._lock` 的情况下，调用 `meter.create_observable_counter()`
3. `create_observable_counter()` 内部获取 SDK `measurement_consumer` 的**内部锁**（②）

**采集路径**（OTel Reader 后台线程）：

```
SDK consumer lock → self._lock
```

1. SDK 的 `PeriodicExportingMetricReader` 定期触发 `collect()`
2. `collect()` 获取 SDK `measurement_consumer` 的**内部锁**
3. 在持有 SDK 内部锁的情况下，遍历所有 observable instrument 并调用 callback
4. callback 内部需要获取 `self._lock`（③）

**死锁场景**：

```
时间线                    主线程 (event loop)                     OTel Reader 线程
─────────────────────────────────────────────────────────────────────────────
T1                     acquire self._lock ✓
T2                                                           acquire SDK consumer lock ✓
T3                     create_observable_counter()            collect()
                       → 等待 SDK consumer lock              → 遍历 instrument
                       → (SDK lock 被 Reader 持有)           → callback()
                       → 阻塞                               → acquire self._lock
                                                                → (self._lock 被主线程持有)
                                                                → 阻塞
─────────────────────────────────────────────────────────────────────────────
                       → 永久死锁
```

### 5.6 为什么发生在 asyncio event loop 中就特别严重

**因为 Agent 主线程就是 asyncio event loop**。

`Export` gRPC handler 是 async 方法，被 schedule 到 event loop 中。但 `_export_number_data` 是**同步阻塞调用**，在 event loop 回调中直接执行。当它卡在 `register_counter_metric` 的 `with self._lock` 中等待 SDK 锁时，**整个 event loop 被冻结**。

event loop 冻结后：
- **所有 aiohttp HTTP 请求无法处理**（job stop、job submit、日志获取等全部卡住）
- **Agent 不再输出任何日志**（日志也需要 event loop）
- **进程还活着**（TCP 连接能 accept，但无法处理）
- **没有自动重启机制**（Agent 的健康检查也在 event loop 中，同样无法执行）

### 5.7 触发条件

PR #64946 描述的典型触发场景：

> The common trigger is the burst of first-seen metric registrations right after a new worker connects (e.g. the lazy ray.init performed by the first job submission) racing a Prometheus scrape; a tight scrape loop against the metrics endpoint reproduces the hang readily, and CPU-constrained 2-vCPU heads widen the race window.

本案例中：
- 集群有 512 个节点，大量 worker 不断注册新指标
- Worker 心跳上报触发 `ReportOCMetrics` gRPC 调用 → `Export` handler → `_export_number_data` → `register_*_metric`
- 同时 OTel Reader 线程在周期性 collect
- 两条路径竞争导致死锁

### 5.8 受影响版本

`RAY_enable_open_telemetry` 从 **Ray 2.52.0** 开始默认为 `True`（PR #56432），因此以下版本均受影响：
- Ray 2.52.0 ~ 2.52.1
- Ray 2.53.0
- Ray 2.54.0 ~ 2.54.1
- Ray 2.55.0 ~ 2.55.1
- Ray 2.56.0 ~ 2.56.1

Ray 2.51.2 及之前版本不受影响（默认 `RAY_enable_open_telemetry=False`）。

## 6. PR #64946 的修复方案

修复引入了一个专用的 `_registration_lock`，将注册和采集的锁序统一：

### 6.1 修复后的锁结构

```python
class OpenTelemetryMetricRecorder:
    def __init__(self):
        self._lock = threading.Lock()               # 观察/数据存储的轻量锁
        self._registration_lock = threading.Lock()  # 注册序列化锁
        # Lock ordering contract:
        #   _registration_lock -> SDK consumer lock -> _lock
        #   _lock is NEVER held across any SDK call
```

### 6.2 修复后的注册路径

```python
def register_counter_metric(self, name, description):
    with self._registration_lock:                   # ① 获取 _registration_lock
        if name in self._registered_instruments:
            return
        callback = self._create_observable_callback(name, MetricType.COUNTER)
        instrument = self.meter.create_observable_counter(  # ② 获取 SDK consumer lock
            name=f"{NAMESPACE}_{name}",
            description=description,
            callbacks=[callback],
        )
        # instrument 创建完成后，再获取 _lock 写入注册表
        with self._lock:                            # ③ 获取 _lock（SDK 调用已完成）
            self._registered_instruments[name] = instrument
            self._counter_observations_by_name[name] = {}
```

锁序：`_registration_lock` → SDK consumer lock → `_lock`

### 6.3 修复后的采集路径

```python
def _create_observable_callback(self, metric_name, metric_type):
    def callback(options):
        with self._lock:                    # 只获取 _lock（不获取 _registration_lock）
            # ... 读取/聚合观察值 ...
    return callback
```

采集路径只获取 `_lock`，**不获取 `_registration_lock`**。

### 6.4 锁序统一

| 路径 | 锁获取顺序 |
|------|-----------|
| 注册 | `_registration_lock` → SDK consumer lock → `_lock` |
| 采集 | SDK consumer lock → `_lock` |

两条路径的锁序一致（`_registration_lock` 只在注册路径中出现，且采集路径不需要它），消除了 ABBA 死锁的可能。

关键改动：`self._lock` 变成了**叶子锁**（leaf lock），永远不会在持有它的情况下调用任何 SDK 方法。

## 7. Dashboard Agent 架构补充

### 7.1 Agent 不是每个 driver 一个

**Dashboard Agent 是每个节点一个**，由 raylet 作为子进程启动。

```
PID 1 (ray start --head --block)
  └─ PID 940 (raylet)
       ├─ PID 996 (ray::DashboardAgent)    ← 每个节点只有一个
       ├─ PID 998 (ray::RuntimeEnvAgent)
       └─ PID 16724, 16856, ... (ray::Worker)
```

Agent 负责：日志收集、指标上报、job 生命周期管理（启动/停止 driver）。所有在该节点上运行的 job 的 driver 共用同一个 agent。

### 7.2 ray job stop 的请求链路

```
CLI: ray job stop <submission_id>
  → HTTP POST http://<head_ip>:8265/api/jobs/<submission_id>/stop
    → Dashboard 主进程 (PID 334) aiohttp handler
      → JobHead (PID 453) stop_job()
        → 通过 driver_agent_http_address 发送 HTTP 请求给 Agent
          → POST http://<node_ip>:42497/api/job_agent/jobs/<job_id>/stop
            → Agent 的 JobAgent.stop_job()
              → JobManager.stop_job()
                → JobSupervisorActor.stop.remote()
                  → JobSupervisor 设置 _stop_event
                    → SIGTERM driver + 子进程
                    → 等待 RAY_JOB_STOP_WAIT_TIME_S (3s)
                    → 超时 SIGKILL
```

### 7.3 Agent event loop 冻结的影响范围

Agent event loop 冻结后，以下功能全部不可用：
- `POST /api/job_agent/jobs/` — 新 job 提交（无法创建 driver）
- `POST /api/job_agent/jobs/{id}/stop` — 停止 job
- `GET /api/job_agent/jobs/{id}` — 查询 job 状态
- `ReportOCMetrics` gRPC — 指标上报（worker 端指标丢失）
- 日志收集 — `log_agent.py` 的 tail 请求无法处理
- 健康检查 — Agent 无法响应任何探测

## 8. 其他性能问题（独立于死锁）

### 8.1 NodeHead CPU 100%

PID 454 (`ray-dashboard-NodeHead-0`) CPU 持续 100%。512 个节点的状态轮询导致，是独立的性能问题，但不是 job stop 卡住的直接原因（NodeHead 是 multiprocessing 子进程，不与 Dashboard 主进程共享 event loop）。

### 8.2 autoscaler monitor CPU 66.9%

PID 332 (`ray/autoscaler/v2/monitor.py`) CPU 66.9%，也是高负载问题，独立于本次死锁。

### 8.3 GCS task events 溢出

```
Max number of tasks event (100000) allowed is reached. Old task events will be overwritten.
```

建议调大 `RAY_task_events_max_num_task_in_gcs` 从 100000 到 500000。

### 8.4 OTel 指标导出持续失败

GCS 日志中 15910 次 `Failed to export metrics to the metrics agent`。因为 Agent 已经卡死，GCS 的 OTel 指标无法导出。

## 9. 解决方案

### 9.1 立即解决（当前集群）

直接 kill driver 进程：

```bash
kill -9 459801
```

Agent 卡死，`ray job stop` 走不通，但 driver 可以直接 kill。Ray 会将 job 标记为 STOPPED/FAILED。

### 9.2 临时规避（新集群）

启动 Ray 时关闭 OTel：

```bash
export RAY_enable_open_telemetry=0
ray start --head --block ...
```

### 9.3 永久修复

升级到包含 [PR #64946](https://github.com/ray-project/ray/pull/64946) 修复的 Ray 版本。

### 9.4 参数调整建议

| 参数 | 当前值 | 建议值 | 说明 |
|------|--------|--------|------|
| `RAY_enable_open_telemetry` | True (默认) | 0 | 临时关闭 OTel 避免死锁 |
| `RAY_task_events_max_num_task_in_gcs` | 100000 | 500000 | 减少 GCS task 事件覆写压力 |

## 10. 排查方法总结

### 10.1 ray job stop 卡住的排查决策树

```
ray job stop 卡住
  │
  ├─ 测试 Dashboard GET 接口 → 正常返回？
  │    ├─ 是 → Dashboard 不卡，问题在下游
  │    │    ├─ 测试 Agent 端口 curl http://<ip>:42497/ → 有响应？
  │    │    │    ├─ 否 → Agent 卡死
  │    │    │    │    ├─ py-spy dump --pid <agent_pid> → 查调用栈
  │    │    │    │    │    ├─ 卡在 register_asynchronous_instrument → OTel 死锁
  │    │    │    │    │    ├─ 卡在其他同步操作 → Agent event loop 阻塞
  │    │    │    │    │    └─ 无输出 → Agent 进程可能 zombie
  │    │    │    │    └─ 解决：kill -9 <driver_pid>
  │    │    │    └─ 是 → Agent 正常，问题在 JobSupervisor/driver
  │    │    │
  │    │    └─ 查 driver 进程 → py-spy dump → 卡在 get_output_blocking 等 task 完成
  │    │
  │    └─ 否 → Dashboard 自身卡死
  │         ├─ 检查 Dashboard 各子进程 CPU
  │         └─ 检查 GCS 响应速度
  │
  └─ 找 Agent PID: pgrep -P <raylet_pid> | 查 /proc/<pid>/cmdline
     找 Driver PID: curl /api/jobs/<submission_id> → driver_info.pid
```

### 10.2 Write task 长时间 RUNNING 的排查决策树

```
task RUNNING 超过预期时间
  │
  ├─ ray list tasks --filter state=RUNNING → 确认 task 状态
  ├─ ray list workers --filter worker_id=... → 确认 worker 存活
  │
  ├─ Worker 节点检查
  │    ├─ cat /proc/<pid>/status → 进程是否存在、State
  │    ├─ cat /proc/<pid>/wchan → 阻塞点
  │    ├─ cat /proc/<pid>/io → I/O 统计
  │    └─ py-spy dump --pid <pid> → Python 调用栈
  │
  ├─ 典型根因
  │    ├─ wchan=futex_wait_queue + hdfs_native.close() → HDFS 连接问题
  │    ├─ wchan=do_readv/do_writev → 磁盘 I/O 瓶颈
  │    ├─ wchan=epoll_wait → 等待网络/gRPC 响应
  │    └─ 纯 CPU 计算 → 业务逻辑耗时
  │
  └─ 解决
       ├─ HDFS 阻塞：kill worker 让 Ray 重试
       ├─ I/O 瓶颈：检查磁盘/网络
       └─ 业务逻辑：优化代码或增加超时
```

## 11. 代码文件索引

| 组件 | 文件路径 | 关键行号 | 说明 |
|------|----------|---------|------|
| Agent 主入口 | `python/ray/dashboard/agent.py` | 516 | event loop run_until_complete |
| Export gRPC handler | `python/ray/dashboard/modules/reporter/reporter_agent.py` | 691-710 | 接收 OTLP 指标导出请求 |
| _export_number_data | `python/ray/dashboard/modules/reporter/reporter_agent.py` | 657-689 | 同步调用 register_*_metric |
| ReportOCMetrics gRPC | `python/ray/dashboard/modules/reporter/reporter_agent.py` | 591-604 | worker 心跳指标上报 |
| OTel Metric Recorder | `python/ray/_private/telemetry/open_telemetry_metric_recorder.py` | 123 | _lock 定义 |
| register_gauge_metric | `python/ray/_private/telemetry/open_telemetry_metric_recorder.py` | 590-604 | 持 _lock 调 SDK create |
| register_counter_metric | `python/ray/_private/telemetry/open_telemetry_metric_recorder.py` | 606-627 | 持 _lock 调 SDK create |
| register_sum_metric | `python/ray/_private/telemetry/open_telemetry_metric_recorder.py` | 629-650 | 持 _lock 调 SDK create |
| _create_observable_callback | `python/ray/_private/telemetry/open_telemetry_metric_recorder.py` | 132-185 | callback 内部获取 _lock |
| set_metric_value | `python/ray/_private/telemetry/open_telemetry_metric_recorder.py` | 699-745 | 持 _lock 写观察值 |
| OTel 默认开启 | PR #56432 | - | RAY_enable_open_telemetry 默认 True |
| 死锁修复 PR | https://github.com/ray-project/ray/pull/64946 | - | 引入 _registration_lock |
| Job Stop API | `python/ray/dashboard/modules/job/job_head.py` | 422 | POST handler |
| Job Agent stop | `python/ray/dashboard/modules/job/job_agent.py` | 75 | Agent 侧 handler |
| JobManager.stop_job | `python/ray/dashboard/modules/job/job_manager.py` | 646 | fire-and-forget stop |
| JobSupervisor.stop | `python/ray/dashboard/modules/job/job_supervisor.py` | 481 | SIGTERM/SIGKILL |
