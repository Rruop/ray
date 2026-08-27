# Ray Task 状态管理与调度机制详解

## 目录

- [1. 日志配置](#1-日志配置)
  - [1.1 RAY_LOG_DIR 与日志目录](#11-ray_log_dir-与日志目录)
  - [1.2 GCS 日志配置](#12-gcs-日志配置)
  - [1.3 日志不重定向直接输出](#13-日志不重定向直接输出)
- [2. Task 状态体系](#2-task-状态体系)
  - [2.1 Protobuf 全量状态](#21-protobuf-全量状态)
  - [2.2 Raylet 内部 WorkStatus](#22-raylet-内部-workstatus)
  - [2.3 UnscheduledWorkCause 细分](#23-unscheduledworkcause-细分)
  - [2.4 状态流转总览](#24-状态流转总览)
  - [2.5 各状态变更的代码触发点](#25-各状态变更的代码触发点)
  - [2.6 PENDING_ARGS_FETCH 与 PENDING_OBJ_STORE_MEM_AVAIL](#26-pending_args_fetch-与-pending_obj_store_mem_avail)
- [3. 调度器架构](#3-调度器架构)
  - [3.1 ClusterLeaseManager vs LocalLeaseManager](#31-clusterleasemanager-vs-localleasemanager)
  - [3.2 ScheduleAndGrantLeases 调用时机](#32-scheduleandgrantleases-调用时机)
  - [3.3 PrestartWorkers 逻辑](#33-prestartworkers-逻辑)
- [4. LeaseDependencyManager 与 PullManager 交互](#4-leasedependencymanager-与-pullmanager-交互)
  - [4.1 TaskMetricsKey 定义](#41-taskmetricskey-定义)
  - [4.2 缺失依赖判定](#42-缺失依赖判定)
  - [4.3 完整交互流程](#43-完整交互流程)
  - [4.4 Metrics 回调推算逻辑](#44-metrics-回调推算逻辑)
  - [4.5 数据流转示例](#45-数据流转示例)
- [5. PullManager 限流机制](#5-pullmanager-限流机制)
  - [5.1 Bundle 激活/去激活粒度](#51-bundle-激活去激活粒度)
  - [5.2 优先级排序](#52-优先级排序)
  - [5.3 Quota 计算规则](#53-quota-计算规则)
- [6. 双层内存控制](#6-双层内存控制)
  - [6.1 两个阶段两个门槛](#61-两个阶段两个门槛)
  - [6.2 max_pinned_lease_arguments_bytes_ 详解](#62-max_pinned_lease_arguments_bytes_-详解)
  - [6.3 为什么需要两层内存判断](#63-为什么需要两层内存判断)
  - [6.4 PullManager 与 max_pinned 的关系](#64-pullmanager-与-max_pinned-的关系)
  - [6.5 Pin vs 不 Pin 的本质区别](#65-pin-与不-pin-的本质区别)
- [7. Actor Task Args 拉取](#7-actor-task-args-拉取)
  - [7.1 Actor Task 与普通 Task 的差异](#71-actor-task-与普通-task-的差异)
  - [7.2 完整交互链路](#72-完整交互链路)
  - [7.3 详细代码流程](#73-详细代码流程)
  - [7.4 Actor Task 为什么没有 PENDING_ARGS_FETCH/PENDING_OBJ_STORE_MEM_AVAIL](#74-actor-task-为什么没有-pending_args_fetchpending_obj_store_mem_avail)
  - [7.5 状态转变总结](#75-状态转变总结)
- [8. 前端展示与 API](#8-前端展示与-api)
  - [8.1 /api/v0/tasks 返回的状态](#81-apiv0tasks-返回的状态)
  - [8.2 前端 7 大类合并映射](#82-前端-7-大类合并映射)
  - [8.3 Task Table vs Ray Core Overview](#83-task-table-vs-ray-core-overview)
  - [8.4 PENDING_ARGS_FETCH 和 PENDING_OBJ_STORE_MEM_AVAIL 在 API 中的处理](#84-pending_args_fetch-和-pending_obj_store_mem_avail-在-api-中的处理)
- [9. PENDING_ACTOR_TASK_ARGS_FETCH 代码逻辑详解](#9-pending_actor_task_args_fetch-代码逻辑详解)
  - [9.1 触发位置](#91-触发位置)
  - [9.2 完整触发链](#92-完整触发链)
  - [9.3 关键区别](#93-关键区别)
- [10. 普通 Task 完整生命周期](#10-普通-task-完整生命周期)
  - [10.1 阶段 1：提交（Owner CoreWorker）](#101-阶段-1提交owner-coreworker)
  - [10.2 阶段 2：依赖解析（Owner CoreWorker）](#102-阶段-2依赖解析owner-coreworker)
  - [10.3 阶段 3：调度（Raylet ClusterLeaseManager）](#103-阶段-3调度raylet-clusterleasemanager)
  - [10.4 阶段 4：本地调度与分配 Worker（Raylet LocalLeaseManager）](#104-阶段-4本地调度与分配-workerraylet-localleasemanager)
  - [10.5 阶段 5：执行（Worker CoreWorker）](#105-阶段-5执行worker-coreworker)
  - [10.6 阶段 6：完成与重试（Owner CoreWorker）](#106-阶段-6完成与重试owner-coreworker)
  - [10.7 普通 Task 状态转变全图（GCS/Dashboard 视角）](#107-普通-task-状态转变全图gcsdashboard-视角)
  - [10.8 GCS TaskEvent 记录方式](#108-gcs-taskevent-记录方式)
- [11. Actor Task 完整生命周期](#11-actor-task-完整生命周期)
  - [11.1 阶段 1：提交（Owner CoreWorker）](#111-阶段-1提交owner-coreworker)
  - [11.2 阶段 2：依赖解析（Owner CoreWorker）](#112-阶段-2依赖解析owner-coreworker)
  - [11.3 阶段 3：直接发给 Actor Worker — SUBMITTED_TO_WORKER](#113-阶段-3直接发给-actor-worker--submitted_to_worker)
  - [11.4 阶段 4：Actor Worker 内排队与 Args 拉取](#114-阶段-4actor-worker-内排队与-args-拉取)
  - [11.5 阶段 5：执行](#115-阶段-5执行)
  - [11.6 阶段 6：完成（Owner CoreWorker）](#116-阶段-6完成owner-coreworker)
  - [11.7 Actor Task 状态转变全图（GCS/Dashboard 视角）](#117-actor-task-状态转变全图gcsdashboard-视角)
- [12. 普通 Task vs Actor Task 状态对比](#12-普通-task-vs-actor-task-状态对比)
- [附录：相关环境变量汇总](#附录相关环境变量汇总)

---

## 1. 日志配置

### 1.1 RAY_LOG_DIR 与日志目录

`RAY_LOG_DIR` **不是**一个真实的环境变量，只是文档中的占位符。实际日志目录的配置方式：

1. **通过 `temp_dir` 间接配置**：日志目录 = `<temp_dir>/session_<timestamp>_<pid>/logs/`

代码位置：`python/ray/_private/node.py:537-568`

```python
self.temp_dir = self._ray_params.temp_dir
if self.temp_dir is None:
    self.temp_dir = ray._common.utils.get_default_ray_temp_dir()  # typically /tmp/ray
self._session_name = f"session_{date_str}_{os.getpid()}"
self._session_dir = os.path.join(self.temp_dir, self._session_name)
session_symlink = os.path.join(self.temp_dir, ray_constants.SESSION_LATEST)
self._logs_dir = os.path.join(self._session_dir, "logs")
```

2. **通过 `RAY_TMPDIR` 环境变量**：最优先设置 Ray 临时目录

代码位置：`python/ray/_common/utils.py:328-335`

优先级为：
1. `RAY_TMPDIR` — 最优先（`:328`）
2. `TMPDIR` — Linux 下次优（`:330`）
3. 默认 `/tmp`（`:335`）

3. **通过 CLI/API 显式指定**：
- `ray start --temp-dir=/data/your_path`
- `ray.init(temp_dir="/data/your_path")`

4. **日志输出到文件**：当 `log_dir` 非空时，日志写入轮转文件；为空则输出到 stderr。

代码位置：`python/ray/_private/ray_logging/__init__.py:40-97`

```python
def setup_component_logger(*, logging_level, logging_format, log_dir, filename, ...):
    if not filename or not log_dir:
        handler = logging.StreamHandler()
    else:
        handler = logging.handlers.RotatingFileHandler(
            os.path.join(log_dir, filename), ...
        )
```

5. **日志目录传递给子进程**：

代码位置：`python/ray/_private/services.py:1497-1554`

```python
command = [GCS_SERVER_EXECUTABLE, f"--log_dir={log_dir}", ...]    # GCS server
f"--log-dir={log_dir}",    # Raylet
f"--ray_logs_dir={log_dir}",    # CPP worker
```

### 1.2 GCS 日志配置

GCS 日志文件位于日志目录下：
- `gcs_server.*.out` — 所有日志
- `gcs_server.*.err` — 错误日志

**GCS 日志文件名生成**：`python/ray/_private/node.py:1105-1107`

```python
stdout_log_fname, stderr_log_fname = self.get_log_file_names(
    "gcs_server", unique=True, create_out=True, create_err=True
)
```

**GCS 启动失败时自动读取错误**：`python/ray/_private/node.py:761-777`

```python
if self._gcs_client is None:
    if hasattr(self, "_logs_dir"):
        with open(os.path.join(self._logs_dir, "gcs_server.err")) as err:
            errors = [e for e in err.readlines() if " C " in e or " E " in e][-10:]
        error_msg = "\n" + "".join(errors) + "\n"
        raise RuntimeError(
            f"Failed to {'start' if self.head else 'connect to'} GCS. "
            f" Last {len(errors)} lines of error files:{error_msg}."
            f"Please check {os.path.join(self._logs_dir, 'gcs_server.out')} for details."
        )
```

**Log Monitor 监控 GCS 错误日志**：`python/ray/_private/log_monitor.py:248-249`

```python
monitor_log_paths += glob.glob(f"{self.logs_dir}/gcs_server*.err")
```

**C++ 层 GCS 日志初始化**：`src/ray/gcs/gcs_server_main.cc:45-94`

```cpp
DEFINE_string(log_dir, "", "The path of the dir where log files are created.");
DEFINE_string(stdout_filepath, "", "The filepath to dump gcs server stdout.");
DEFINE_string(stderr_filepath, "", "The filepath to dump gcs server stderr.");

if (!FLAGS_stdout_filepath.empty()) {
    ray::StreamRedirectionOption stdout_redirection_options;
    stdout_redirection_options.file_path = FLAGS_stdout_filepath;
    ray::RedirectStdoutOncePerProcess(stdout_redirection_options);
}

InitShutdownRAII ray_log_shutdown_raii(ray::RayLog::StartRayLog,
                                       ray::RayLog::ShutDownRayLog,
                                       argv[0], ray::RayLogLevel::INFO,
                                       /*log_filepath=*/"",
                                       /*err_log_filepath=*/"",
                                       /*log_rotation_max_size=*/0,
                                       /*log_rotation_file_num=*/1);
```

**环境变量控制 C++ 日志级别**：`src/ray/util/logging.cc:283-307`

```cpp
void RayLog::InitSeverityThreshold(RayLogLevel severity_threshold) {
    const char *var_value = std::getenv("RAY_BACKEND_LOG_LEVEL");
    if (var_value != nullptr) {
        // Parse: trace, debug, info, warning, error, fatal
    }
    severity_threshold_ = severity_threshold;
}
```

**日志轮转配置**：`src/ray/util/logging.cc:322-346`

```cpp
// RAY_ROTATION_MAX_BYTES - max bytes for log rotation (0 = no rotation)
// RAY_ROTATION_BACKUP_COUNT - number of rotating log files
```

**JSON 格式日志**：`src/ray/util/logging.cc:309-319`

```cpp
void RayLog::InitLogFormat() {
    if (const char *var_value = std::getenv("RAY_BACKEND_LOG_JSON"); ...) {
        // JSON format if RAY_BACKEND_LOG_JSON=1
    }
}
```

### 1.3 日志不重定向直接输出

有两种方式让 GCS 日志直接输出到 stderr（不重定向到文件）：

**方式 1：环境变量**

```bash
export RAY_LOG_TO_STDERR=1
```

这会让 `should_redirect_logs()` 返回 `False`（`python/ray/_private/node.py:854`），`get_log_file_names()` 返回 `(None, None)`（`:882`），从而 GCS 启动时不传 `--stdout_filepath` 和 `--stderr_filepath`，日志直接打到 stderr。

**方式 2：Python API**

```python
ray.init(log_to_stderr=True)
```

`RayParams.log_to_stderr` 优先级高于环境变量（`:845`）。

**C++ 层说明**：当 `stdout_filepath`/`stderr_filepath` 为空时，C++ 的 `RedirectStdoutOncePerProcess` 不会被调用（`gcs_server_main.cc:63`），stdout/stderr 保持原始终端输出。`StartRayLog` 的 `log_filepath` 和 `err_log_filepath` 本来就是空的（`:91`），所以 C++ 日志也直接输出到 stdout/stderr。

---

## 2. Task 状态体系

### 2.1 Protobuf 全量状态

定义位置：`src/ray/protobuf/common.proto:903`

```
NIL = 0                           // 无状态（非 owner 或已删除）
PENDING_ARGS_AVAIL = 1            // 等 deps 创建
PENDING_NODE_ASSIGNMENT = 2      // 等 Ray 调度分配节点/worker
PENDING_OBJ_STORE_MEM_AVAIL = 3 // 等 plasma 内存释放（子状态，metrics 用）
PENDING_ARGS_FETCH = 4           // 正在拉取 deps 到节点（子状态，metrics 用）
SUBMITTED_TO_WORKER = 5          // 已提交给 worker，即将执行
PENDING_ACTOR_TASK_ARGS_FETCH = 6  // Actor task 正在拉取 args
PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY = 7  // Actor task 等待顺序/并发约束
RUNNING = 8                       // 正在执行
RUNNING_IN_RAY_GET = 9           // 执行中但阻塞在 ray.get()（子状态）
RUNNING_IN_RAY_WAIT = 10        // 执行中但阻塞在 ray.wait()（子状态）
FINISHED = 11                     // 执行完成
FAILED = 12                       // 执行失败
GETTING_AND_PINNING_ARGS = 13   // 正在获取和 pin args（子状态）
```

Python 侧定义：`python/ray/_private/custom_types.py:33-48`

```python
TASK_STATUS = [
    "NIL", "PENDING_ARGS_AVAIL", "PENDING_NODE_ASSIGNMENT",
    "PENDING_OBJ_STORE_MEM_AVAIL", "PENDING_ARGS_FETCH",
    "SUBMITTED_TO_WORKER", "PENDING_ACTOR_TASK_ARGS_FETCH",
    "PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY",
    "RUNNING", "RUNNING_IN_RAY_GET", "RUNNING_IN_RAY_WAIT",
    "FINISHED", "FAILED", "GETTING_AND_PINNING_ARGS",
]
```

### 2.2 Raylet 内部 WorkStatus

定义位置：`src/ray/raylet/scheduling/internal.h:27`

```
WAITING              ↔ PENDING_NODE_ASSIGNMENT / PENDING_ARGS_FETCH
WAITING_FOR_WORKER   ↔ SUBMITTED_TO_WORKER（已分配资源等 worker）
CANCELLED            ↔ FAILED（调度取消）
```

### 2.3 UnscheduledWorkCause 细分

```
WAITING_FOR_RESOURCE_ACQUISITION    → 集群内无节点满足资源
WAITING_FOR_RESOURCES_AVAILABLE    → 本节点资源暂时不足
WAITING_FOR_AVAILABLE_PLASMA_MEMORY → plasma 内存不够 pin args
WORKER_NOT_FOUND_JOB_CONFIG_NOT_EXIST → worker 的 job config 还没注册
WORKER_NOT_FOUND_REGISTRATION_TIMEOUT → worker 注册超时
```

### 2.4 状态流转总览

```
┌─────────────────────────────────────────────────────────────────────┐
│ 1. RequestWorkerLease RPC 到达                                      │
│    Work 初始状态: WAITING (cause: WAITING_FOR_RESOURCE_ACQUISITION) │
│    Owner 侧: PENDING_ARGS_AVAIL → PENDING_NODE_ASSIGNMENT           │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 2. ClusterLeaseManager::ScheduleAndGrantLeases()                    │
│    ├─ 无节点可用 → WAITING, 移入 infeasible_leases_               │
│    ├─ 选到远端节点 → spillback reply                                │
│    └─ 选到本节点 → local_lease_manager_.QueueAndScheduleLease()    │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 3. WaitForLeaseArgsRequests()                                       │
│    ├─ 无 deps / args 已就绪 → 入 leases_to_grant_                   │
│    └─ args 未就绪 → 入 waiting_lease_queue_                         │
│       Raylet metrics: PENDING_ARGS_FETCH / PENDING_OBJ_STORE_MEM   │
└──────────────────────────┬──────────────────────────────────────────┘
                           ▼ (args 就绪后 LeasesUnblocked 回调)
┌─────────────────────────────────────────────────────────────────────┐
│ 4. GrantScheduledLeasesToWorkers()                                  │
│    a. PinLeaseArgsIfMemoryAvailable                                 │
│       ├─ args 被 evict → 回 waiting_lease_queue_                    │
│       ├─ plasma 内存不足 → WAITING_FOR_AVAILABLE_PLASMA_MEMORY     │
│       └─ pin 成功 → 继续                                            │
│    b. AllocateLocalTaskResources                                    │
│       ├─ 资源不足 → TrySpillback 到远端                             │
│       └─ 资源足够 → 继续                                            │
│    c. PopWorker                                                     │
│       → WAITING_FOR_WORKER                                          │
│       Owner 侧: PENDING_NODE_ASSIGNMENT → SUBMITTED_TO_WORKER       │
└──────────────────────────┬──────────────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 5. PoppedWorkerHandler (PopWorker 回调)                              │
│    ├─ 拿到 worker → Grant()                                        │
│    ├─ worker=null, JobConfigMissing → WAITING                      │
│    ├─ worker=null, WorkerPendingRegistration → WAITING/CANCELLED    │
│    ├─ worker=null, RuntimeEnvSetupFailed → CANCELLED               │
│    └─ worker=null, JobFinished → 从队列移除                         │
└──────────────────────────┬──────────────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 6. Lease 执行完成 / worker 返还                                      │
│    Owner 侧: RUNNING → FINISHED / FAILED                            │
│    CleanupLease → ReleaseLeaseArgs → ReleaseWorkerResources        │
└─────────────────────────────────────────────────────────────────────┘
```

**重试路径**：
- `FAILED` → `PENDING_ARGS_AVAIL`（重试）：`task_manager.cc:1235-1239`
- `FINISHED` → `PENDING_ARGS_AVAIL`（重试）：`task_manager.cc:421-425`

### 2.5 各状态变更的代码触发点

Owner 侧，`SetTaskStatus()` 是统一入口（`task_manager.cc:1705`），所有状态变更都经过它，同时写入 `task_event_buffer_` 上报 GCS。

| 转换 | 触发函数 | 位置 |
|---|---|---|
| 初始 → `PENDING_ARGS_AVAIL` | `AddTask()` | `task_manager.cc:342-348` |
| `PENDING_ARGS_AVAIL` → `PENDING_NODE_ASSIGNMENT` | `MarkDependenciesResolved()` | `task_manager.cc:1675-1686` |
| `PENDING_NODE_ASSIGNMENT` → `SUBMITTED_TO_WORKER` | `MarkTaskWaitingForExecution()` | `task_manager.cc:1688-1703` |
| → `FINISHED` | `CompletePendingTask()` | `task_manager.cc:1059` |
| → `FAILED`（应用异常） | `CompletePendingTask()` | `task_manager.cc:1053-1057` |
| → `FAILED`（系统错误） | `FailPendingTask()` | `task_manager.cc:1316-1318` |
| → `FAILED`（intended exit → FINISHED） | `FailPendingTask()` | `task_manager.cc:1309` |
| `FAILED` → `PENDING_ARGS_AVAIL`（重试） | `FailOrRetryPendingTask()` | `task_manager.cc:1235-1239` |
| `FINISHED` → `PENDING_ARGS_AVAIL`（重试） | `SetupTaskEntryForResubmit()` | `task_manager.cc:421-425` |

Executor 侧子状态：

| 子状态 | 触发位置 |
|---|---|
| `RUNNING_IN_RAY_GET` | `core_worker.cc:1511`（进入 ray.get 时） |
| `RUNNING_IN_RAY_WAIT` | `core_worker.cc:1696`（进入 ray.wait 时） |
| `GETTING_AND_PINNING_ARGS` | `core_worker.cc:3012-3016`（执行前获取 args 时） |

Actor Task 子状态（写入 GCS TaskEvent）：

| 子状态 | 触发位置 |
|---|---|
| `PENDING_ACTOR_TASK_ARGS_FETCH` | `ordered_actor_task_execution_queue.cc:116` |
| `PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY` | `ordered_actor_task_execution_queue.cc:143, 155` |

上报链路：

```
SetTaskStatus()
  → task_event_buffer_.RecordTaskStatusEventIfNeeded()    // task_event_buffer.cc:81
    → GCS TaskStateUpdate (protobuf TaskStatus + timestamp)
      → Dashboard State API (common.py:1669-1696)
        → 取最新 event 的 state 作为展示状态
```

Dashboard 侧解析（`common.py:1669-1696`）：

```python
if "state_ts_ns" in state_updates:
    state_ts_ns = state_updates["state_ts_ns"]
    for state_name, state in TaskStatus.items():
        key = str(state)
        if key in state_ts_ns:
            ts_ms = int(state_ts_ns[key]) // 1e6
            events.append({"state": state_name, "created_ms": ts_ms})
            if state == TaskStatus.PENDING_ARGS_AVAIL:
                task_state["creation_time_ms"] = ts_ms
            if state == TaskStatus.RUNNING:
                task_state["start_time_ms"] = ts_ms
            if state == TaskStatus.FINISHED or state == TaskStatus.FAILED:
                task_state["end_time_ms"] = ts_ms

task_state["events"] = events
if len(events) > 0:
    latest_state = events[-1]["state"]
else:
    latest_state = "NIL"
task_state["state"] = latest_state
```

### 2.6 PENDING_ARGS_FETCH 与 PENDING_OBJ_STORE_MEM_AVAIL

这两个状态**不是通过 Owner 侧 `SetTaskStatus` 显式记录的**，而是由 Raylet 侧的 **PullManager** 通过 metrics 推算出来的。它们只存在于 Prometheus metrics（`ray_tasks` gauge）和 Dashboard 聚合的 `state_counts` 中，不会写入 GCS 的 `TaskStateUpdate.state_ts_ns`。

详细逻辑见 [第 4 节](#4-leasedependencymanager-与-pullmanager-交互)。

---

## 3. 调度器架构

### 3.1 ClusterLeaseManager vs LocalLeaseManager

**ClusterLeaseManager** — 集群级调度（选节点）

代码位置：`src/ray/raylet/scheduling/cluster_lease_manager.cc`

1. 遍历 `leases_to_schedule_` 队列，调用 `GetBestSchedulableNode` 在集群中选最优节点
2. 选到本节点 → `local_lease_manager_.QueueAndScheduleLease(work)`（`:424-426`）
3. 选到远端节点 → spillback reply（`:437-461`）
4. 不可行 → 移入 `infeasible_leases_` 队列

**LocalLeaseManager** — 本地级调度（分 worker）

代码位置：`src/ray/raylet/scheduling/local_lease_manager.cc`

1. 等待依赖 args 就绪（`WaitForLeaseArgsRequests`）
2. 公平调度策略（Fair scheduling）
3. 从 `leases_to_grant_` 队列中 pop worker 分配

**调用链路**：

```
ClusterLeaseManager::ScheduleAndGrantLeases()
  ├── TryScheduleInfeasibleLease()
  ├── 遍历 leases_to_schedule_:
  │     ├── 选到本节点 → local_lease_manager_.QueueAndScheduleLease(work)
  │     ├── 选到远端 → spillback reply
  │     └── 不可行 → 移入 infeasible_leases_
  └── local_lease_manager_.ScheduleAndGrantLeases()  // 最后一行 :295
```

| | Cluster | Local |
|---|---|---|
| 关注点 | 选**哪个节点** | 选**哪个 worker** |
| 队列 | `leases_to_schedule_`（待调度） | `leases_to_grant_`（待分配） |
| 资源视角 | 集群所有节点资源 | 本节点本地资源 |
| 依赖处理 | 不关心 | 等待 args 就绪、公平调度 |

### 3.2 ScheduleAndGrantLeases 调用时机

**事件驱动（立即触发）**：

- 新 lease 入队：`ClusterLeaseManager::QueueAndScheduleLease`（`:67`）
- Worker 注册/空闲：`NodeManager` 中 worker 注册（`:556`）、断连（`:889`）
- 资源释放：lease 归还/完成时（`:1266`、`:1386`）
- 依赖就绪：lease 的 args 拉取完成时（`:1547`）
- 节点资源变更：收到集群资源更新时（`:1933`、`:1985`）
- lease 取消：cancel 后重试调度（`:2076`、`:2307`）

**定时触发（兜底）**：

```cpp
periodical_runner_->RunFnPeriodically(
    [this]() { cluster_lease_manager_.ScheduleAndGrantLeases(); },
    RayConfig::instance().worker_cap_initial_backoff_delay_ms(),
    "NodeManager.ScheduleAndGrantLeases");
```

### 3.3 PrestartWorkers 逻辑

代码位置：`src/ray/raylet/worker_pool.cc:1512-1543`

```cpp
int64_t num_available_cpus = get_num_cpus_available_();
auto desired_usable_workers = std::min<int64_t>(num_available_cpus, backlog_size);
if (num_usable_workers < desired_usable_workers) {
    int64_t num_needed = desired_usable_workers - num_usable_workers;
    PrestartWorkersInternal(lease_spec, num_needed);
}
```

Prestart 数量 = **`min(available_cpus, backlog_size) - 已有可用worker`**

- `available_cpus` — 当前节点剩余可用 CPU 数，不是总 CPU
- `backlog_size` — 调用方报告的该 shape 的排队 lease 数量
- 两者取小值作为上限

**去重逻辑**：`node_manager.cc:1840-1858` — 如果 lease 已经在队列中，直接追加 callback 就 return，不会重复 Prestart。

---

## 4. LeaseDependencyManager 与 PullManager 交互

### 4.1 TaskMetricsKey 定义

代码位置：`src/ray/object_manager/pull_manager.h:38`

```cpp
using TaskMetricsKey = std::pair<std::string, bool>;
```

- **first**：task 的函数名（`task_name`），如 `"my_func"`
- **second**：是否是重试（`is_retry`）

### 4.2 缺失依赖判定

代码位置：`src/ray/raylet/lease_dependency_manager.h:241-289`

```cpp
LeaseDependencies(deps, counter_map, task_key)
    : num_missing_dependencies_(dependencies_.size()) {  // 初始 = 所有依赖数
  if (num_missing_dependencies_ > 0) {
    waiting_task_counter_map_.Increment(task_key);  // 有缺失 → +1
  }
}
```

**初始值** = 依赖的 object 总数，认为全部缺失。然后在 `RequestLeaseDependencies` 中检查已在本地的：

```cpp
for (const auto &obj_id : lease_entry->dependencies_) {
  if (local_objects_.contains(obj_id)) {
    lease_entry->DecrementMissingDependencies();  // 本地已有 → 减 1
  }
}
```

**缺失依赖 = 总依赖数 - 已在本地的依赖数**

动态更新：
- 对象变 local → `HandleObjectLocal` → `DecrementMissingDependencies()`
- 对象被 eviction → `HandleObjectMissing` → `IncrementMissingDependencies()`
- `num_missing_dependencies_ == 0` → 依赖就绪，触发 `Decrement(task_key)`

### 4.3 完整交互流程

```
LocalLeaseManager
  │  WaitForLeaseArgsRequests()
  │  调用 lease_dependency_manager_.RequestLeaseDependencies(lease_id, deps, task_key)
  ▼
LeaseDependencyManager
  │  记录 lease → deps 映射
  │  waiting_leases_counter_[task_key]++  (有缺失依赖时)
  │  调用 object_manager_.Pull(deps, TASK_ARGS, task_key)
  ▼
PullManager
  │  AddBundlePullRequest → 初始 inactive
  │  UpdatePullsBasedOnAvailableMemory → 决定 active/inactive
  ▼ (callback 触发)
LeaseDependencyManager::SetOnChangeCallback
  │  从 waiting_leases_counter_ 和 PullManager 读数
  │  写入 task_by_state_counter_ (Prometheus gauge)
```

对象到达/丢失回调：
- **对象变 local**：`HandleObjectLocal`（`lease_dependency_manager.cc:307`）→ `DecrementMissingDependencies()` → ready_lease_ids → `LocalLeaseManager::LeasesUnblocked()`
- **对象被 eviction**：`HandleObjectMissing`（`:276`）→ `IncrementMissingDependencies()` → `waiting_leases_counter_.Increment(task_key)`
- **Lease 被取消**：`RemoveLeaseDependencies`（`:254`）→ 析构 `LeaseDependencies`，`CancelPull`

### 4.4 Metrics 回调推算逻辑

代码位置：`src/ray/raylet/lease_dependency_manager.h:67-96`

```cpp
waiting_leases_counter_.SetOnChangeCallback(
    [this](std::pair<std::string, bool> key) mutable {
      int64_t num_total = waiting_leases_counter_.Get(key);
      int64_t num_inactive = std::min(
          num_total, object_manager_.PullManagerNumInactivePullsByTaskName(key));

      // 抵消 Owner 上报的 PENDING_NODE_ASSIGNMENT
      task_by_state_counter_.Record(-num_total, {{"State", "PENDING_NODE_ASSIGNMENT"}...});
      // 活跃拉取 → PENDING_ARGS_FETCH
      task_by_state_counter_.Record(num_total - num_inactive, {{"State", "PENDING_ARGS_FETCH"}...});
      // 不活跃拉取 → PENDING_OBJ_STORE_MEM_AVAIL
      task_by_state_counter_.Record(num_inactive, {{"State", "PENDING_OBJ_STORE_MEM_AVAIL"}...});
    });
```

`RecordMetrics` 刷写到 Prometheus：`lease_dependency_manager.cc:373-375`

```cpp
void LeaseDependencyManager::RecordMetrics() {
  waiting_leases_counter_.FlushOnChangeCallbacks();
}
```

### 4.5 数据流转示例

假设有 100 个同 key 的 lease 在等待 deps，其中 20 个因内存不足被 PullManager deactivate：

```
waiting_leases_counter_[("f", false)] = 100
PullManager.NumInactivePulls(("f", false)) = 20

Prometheus gauge 写入:
  PENDING_NODE_ASSIGNMENT:  -100   (抵消 Owner 上报)
  PENDING_ARGS_FETCH:        80    (100 - 20，活跃拉取)
  PENDING_OBJ_STORE_MEM_AVAIL: 20  (等内存)

最终 Dashboard:
  PENDING_NODE_ASSIGNMENT: 0 (Owner 报 100, Raylet 抵消 -100)
  PENDING_ARGS_FETCH:     80
  PENDING_OBJ_STORE_MEM_AVAIL: 20
  合计 = 100 ✓
```

---

## 5. PullManager 限流机制

### 5.1 Bundle 激活/去激活粒度

代码位置：`src/ray/object_manager/pull_manager.h:221-243`

```cpp
struct BundlePullRequest {
  std::vector<ObjectID> objects_;              // 该 task 的所有依赖对象
  absl::flat_hash_set<ObjectID> pullable_objects_;
  TaskMetricsKey task_key_;
  bool IsPullable() const { return pullable_objects_.size() == objects_.size(); }
};
```

**整体激活/去激活，不能部分激活**。每个 `BundlePullRequest` 是一个 task 的全部 args 作为一个整体。

**Activate**（`pull_manager.cc:109-178`）：取 inactive 队列头部的一个 request，计算所有 objects 总 bytes，做 quota 检查。通过后，将 bundle 内所有 objects 加入 `active_object_pull_requests_` 并开始拉取。整个 bundle 从 `inactive_requests` 移到 `active_requests`。

**Deactivate**（`pull_manager.cc:180-203`）：将 bundle 内所有 objects 从 `active_object_pull_requests_` 移除，整个 bundle 移回 `inactive_requests`。

### 5.2 优先级排序

`UpdatePullsBasedOnAvailableMemory`（`pull_manager.cc:232-315`）按优先级处理：

```
优先级: GET_REQUEST > WAIT_REQUEST > TASK_ARGS
```

1. 先激活所有 GET 请求（可无条件抢占其他类的配额）
2. 再激活 WAIT 请求（有配额才激活）
3. 最后激活 TASK_ARGS（有配额才激活）
4. 如果仍 OverQuota，从后往前 deactivate TASK_ARGS 和 WAIT（各至少保留 1 个 active）

### 5.3 Quota 计算规则

代码位置：`src/ray/object_manager/pull_manager.cc:224-230`

```cpp
int64_t PullManager::RemainingQuota() {
  int64_t bytes_left_to_pull = num_bytes_being_pulled_ - pinned_objects_size_;
  return num_bytes_available_ - bytes_left_to_pull;
}

bool PullManager::OverQuota() { return RemainingQuota() < 0L; }
```

PullManager 内部三种状态：

```
active_requests    → 正在活跃拉取（有足够 object store 内存）
inactive_requests  → 等待可用内存（被 deactivate 了）
unpullable         → 对象丢失，等重建
```

转换规则：
- 新请求 → **inactive**
- 有配额时 → **inactive → active**（`ActivateBundlePullRequest`）
- 内存不足需腾位置 → **active → inactive**（`DeactivateBundlePullRequest`）

---

## 6. 双层内存控制

### 6.1 两个阶段两个门槛

```
阶段 1: PullManager 拉取 args 到 plasma
  ┌─────────────────────────────────────────────────────┐
  │ args 在远端 → PullManager 拉到本地 plasma store      │
  │                                                      │
  │ 内存不够 → PENDING_OBJ_STORE_MEM_AVAIL               │
  │ 判定: RemainingQuota = num_bytes_available            │
  │       - (bytes_being_pulled - pinned_objects_size)    │
  │       < 0 → OverQuota → deactivate bundle            │
  └──────────────────────────────────────────────────────┘

阶段 2: LocalLeaseManager 从 plasma pin args 到 worker
  ┌─────────────────────────────────────────────────────┐
  │ args 已在 plasma → PinLeaseArgsIfMemoryAvailable     │
  │ 从 plasma 取 RayObject 引用并 pin 住                 │
  │                                                      │
  │ 内存不够 → 不 grant，留在 leases_to_grant_ 等待      │
  │ 判定: pinned_lease_arguments_bytes_                  │
  │       > max_pinned_lease_arguments_bytes_             │
  │       → ReleaseLeaseArgs + return false              │
  └──────────────────────────────────────────────────────┘
```

### 6.2 max_pinned_lease_arguments_bytes_ 详解

代码位置：`src/ray/raylet/scheduling/local_lease_manager.cc:782-856`

`PinLeaseArgsIfMemoryAvailable` 流程：
1. `get_lease_arguments_(deps, &args)` — 从 plasma 获取 RayObject 引用
2. 检查 args 是否被 eviction（nullptr）
3. `PinLeaseArgs` — 记录引用计数，累加 `pinned_lease_arguments_bytes_`
4. 检查 `max_pinned`：累计超限 → `ReleaseLeaseArgs` + return false

`PinLeaseArgs` 逻辑（`:840-856`）：

```cpp
void LocalLeaseManager::PinLeaseArgs(const LeaseSpecification &lease_spec,
                                     std::vector<std::unique_ptr<RayObject>> args) {
  const auto &deps = lease_spec.GetDependencyIds();
  auto executed_lease_inserted =
      granted_lease_args_.emplace(lease_spec.LeaseId(), deps).second;

  if (executed_lease_inserted) {
    for (size_t i = 0; i < deps.size(); i++) {
      auto [it, pinned_lease_inserted] =
          pinned_lease_arguments_.emplace(deps[i], std::make_pair(std::move(args[i]), 0));
      if (pinned_lease_inserted) {
        pinned_lease_arguments_bytes_ += it->second.first->GetSize();
      }
      it->second.second++;  // 引用计数 +1
    }
  }
}
```

| | PENDING_OBJ_STORE_MEM_AVAIL | max_pinned_lease_arguments_bytes_ |
|---|---|---|
| **阶段** | PullManager 拉取阶段 | PinLeaseArgs 阶段 |
| **管理者** | PullManager | LocalLeaseManager |
| **内存类型** | plasma store 可用空间 | 已 pin 的 lease args 总字节数 |
| **状态体现** | Metrics gauge | WorkStatus: WAITING_FOR_AVAILABLE_PLASMA_MEMORY |
| **恢复条件** | plasma 释放空间 → activate | 其他 lease 完成 → ReleaseLeaseArgs → 下降 → 重试 |

### 6.3 为什么需要两层内存判断

**PullManager quota** — 控制**拉取速率**（流量控制）：

```
场景：100 个 task 同时等 args，每个 10MB
如果不限 → 同时拉 1000MB → plasma store 被瞬间塞满 → OOM eviction
有 quota → 只激活够放得下的几个 bundle，其余 deactivate 等空间释放
```

**max_pinned** — 控制**累计占用**（存量控制）：

```
场景：20 个 lease 已 grant，每个 pin 了 50MB = 1000MB
新 lease 又需 50MB → total = 1050MB → 超限 → 等 ReleaseLeaseArgs 释放
```

**只靠一层不够**：
- 只靠 PullManager → pin 无限累积 → raylet 内存爆炸
- 只靠 max_pinned → 不限速拉取 → plasma 瞬间满 → 恶性循环

```
PullManager quota = 水龙头限流（控制进水速度）
max_pinned_args = 水池限高（控制蓄水量）
一个管"流得进"，一个管"存得下"，缺一不可。
```

### 6.4 PullManager 与 max_pinned 的关系

两者是**先后关系**：

1. **先**过 PullManager quota → args 拉到 plasma
2. **再**过 `PinLeaseArgsIfMemoryAvailable` 的 max_pinned 检查 → 才能 grant

一个 lease 可能先卡在 `PENDING_OBJ_STORE_MEM_AVAIL`（拉不到本地），等拉到后又卡在 `max_pinned`（pin 不了）。

PullManager 不根据 max_pinned 限流的原因：
1. 两者在不同模块，没有交互
2. 从拉取到 pin 有时间差，提前限制过于保守
3. 不 pin 的 args 仍在 plasma 里（evictable），下次 grant 可能直接拿到

PullManager quota 间接考虑了 pin 的影响：

```cpp
RemainingQuota = num_bytes_available_ - (bytes_being_pulled_ - pinned_objects_size_)
```

### 6.5 Pin 与不 Pin 的本质区别

```
pin（锁定）:
  - RayObject 持有 plasma 引用，引用计数 > 0
  - plasma 不能 evict 这个对象
  - 内存被锁定直到 ReleaseLeaseArgs

不 pin（释放引用）:
  - ReleaseLeaseArgs 释放 RayObject 引用
  - plasma 引用计数降为 0 → 对象变成 evictable
  - plasma 在内存不足时可以 LRU evict 回收空间
  - 数据暂时还在内存里，但是"可被回收的"
```

`max_pinned_lease_arguments_bytes_` 的目的是**给 plasma 留出足够的可回收空间**。

完整流转：

```
1. PullManager 拉取 args 到 plasma → plasma 引用计数 > 0 (被 PullManager pin)
2. PullManager 拉完 → 去掉自己的 pin → 对象变成 evictable
3. PinLeaseArgsIfMemoryAvailable:
   a. get_lease_arguments_ → 从 plasma 获取 RayObject 引用（重新 pin）
   b. 检查 max_pinned
      - 不超限 → pin 住，grant 给 worker → 对象不可 evict
      - 超限 → ReleaseLeaseArgs 释放引用 → 对象变回 evictable
4. 如果 args 在等待期间被 plasma evict 了
   → 下次 get_lease_arguments_ 返回 nullptr
   → lease 回到 waiting_lease_queue_ 重新拉取
```

---

## 7. Actor Task Args 拉取

### 7.1 Actor Task 与普通 Task 的差异

```
普通 task:
  Owner → raylet 调度 → PullManager 拉取 args → grant 给 worker → RUNNING
  (args 拉取发生在 raylet 侧，状态用 PENDING_ARGS_FETCH metrics 衡量)

Actor task:
  Owner → raylet 调度 → 提交给 actor worker → actor worker 内部排队
  → actor worker 内异步拉取 args → 排队等执行顺序 → RUNNING
  (args 拉取发生在 worker 进程内部)
```

### 7.2 完整交互链路

```
Actor Worker (CoreWorker)                    Raylet (NodeManager)
     │                                              │
     │  1. EnqueueTask(task)                         │
     │  → 有 deps                                    │
     │  2. Record PENDING_ACTOR_TASK_ARGS_FETCH     │
     │  3. IPC: WaitForActorCallArgs(args, tag)  ──→ │
     │                                              │  4. AsyncWait + 拉取 deps
     │                                              │
     │                                              │  5. 所有 args 到齐
     │                                              │  6. worker->ActorCallArgWaitComplete(tag)
     │  ← RPC: ActorCallArgWaitComplete(tag)    ──── │
     │  7. MarkReady(tag)                           │
     │  8. Record PENDING_ACTOR_TASK_ORDERING...    │
     │  9. MarkDependenciesResolved()                │
     │  10. ExecuteQueuedTasks() → RUNNING           │
```

### 7.3 详细代码流程

见 [第 9 节](#9-pending_actor_task_args_fetch-代码逻辑详解) 和 [第 11 节](#11-actor-task-完整生命周期)。

### 7.4 Actor Task 为什么没有 PENDING_ARGS_FETCH/PENDING_OBJ_STORE_MEM_AVAIL

Actor task 的 args 拉取走 `WaitForActorCallArgs` IPC 路径（`node_manager.cc:1689`），用的是 `wait_manager_` + `AsyncWait`，**不经过 LeaseDependencyManager**，所以不产生 `PENDING_ARGS_FETCH` / `PENDING_OBJ_STORE_MEM_AVAIL` 的 metrics。

而且 `wait_manager_.Wait` 不做 active/inactive quota 管理，所以也不存在 `PENDING_OBJ_STORE_MEM_AVAIL` 的等价状态。

### 7.5 状态转变总结

```
Actor task 到达 worker:
  ├─ 有 deps → PENDING_ACTOR_TASK_ARGS_FETCH
  │           → (异步等 raylet 拉取)
  │           → args 就绪回调
  │           → PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY
  │           → MarkDependenciesResolved()
  │           → ExecuteQueuedTasks()
  │           → ExecuteRequest() → RUNNING
  │
  └─ 无 deps → PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY
             → ExecuteQueuedTasks()
             → 可能立即执行(RUNNING) 或排队等顺序
```

---

## 8. 前端展示与 API

### 8.1 /api/v0/tasks 返回的状态

`/api/v0/tasks` 返回 GCS TaskEvent 中记录的精确状态。可能返回的状态（除 `PENDING_ARGS_FETCH` 和 `PENDING_OBJ_STORE_MEM_AVAIL` 外的 13 个）：

```
NIL, PENDING_ARGS_AVAIL, PENDING_NODE_ASSIGNMENT,
SUBMITTED_TO_WORKER, PENDING_ACTOR_TASK_ARGS_FETCH,
PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY,
RUNNING, RUNNING_IN_RAY_GET, RUNNING_IN_RAY_WAIT,
FINISHED, FAILED, GETTING_AND_PINNING_ARGS
```

`PENDING_ARGS_FETCH` 和 `PENDING_OBJ_STORE_MEM_AVAIL` 只存在于 Prometheus metrics gauge 中，不写入 GCS TaskEvent。

### 8.2 前端 7 大类合并映射

代码位置：`python/ray/dashboard/client/src/pages/job/hook/useJobProgress.ts:19-46`

```typescript
export enum TaskStatus {
  PENDING_ARGS_AVAIL = "PENDING_ARGS_AVAIL",
  PENDING_NODE_ASSIGNMENT = "PENDING_NODE_ASSIGNMENT",
  SUBMITTED_TO_WORKER = "SUBMITTED_TO_WORKER",
  RUNNING = "RUNNING",
  FINISHED = "FINISHED",
  FAILED = "FAILED",
  UNKNOWN = "UNKNOWN",
}

const TASK_STATE_NAME_TO_PROGRESS_KEY: Record<TypeTaskStatus, TaskStatus> = {
  [TypeTaskStatus.PENDING_ARGS_AVAIL]: TaskStatus.PENDING_ARGS_AVAIL,
  [TypeTaskStatus.PENDING_NODE_ASSIGNMENT]: TaskStatus.PENDING_NODE_ASSIGNMENT,
  [TypeTaskStatus.PENDING_OBJ_STORE_MEM_AVAIL]: TaskStatus.PENDING_NODE_ASSIGNMENT,
  [TypeTaskStatus.PENDING_ARGS_FETCH]: TaskStatus.PENDING_NODE_ASSIGNMENT,
  [TypeTaskStatus.SUBMITTED_TO_WORKER]: TaskStatus.SUBMITTED_TO_WORKER,
  [TypeTaskStatus.PENDING_ACTOR_TASK_ARGS_FETCH]: TaskStatus.SUBMITTED_TO_WORKER,
  [TypeTaskStatus.PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY]: TaskStatus.SUBMITTED_TO_WORKER,
  [TypeTaskStatus.RUNNING]: TaskStatus.RUNNING,
  [TypeTaskStatus.RUNNING_IN_RAY_GET]: TaskStatus.RUNNING,
  [TypeTaskStatus.RUNNING_IN_RAY_WAIT]: TaskStatus.RUNNING,
  [TypeTaskStatus.FINISHED]: TaskStatus.FINISHED,
  [TypeTaskStatus.FAILED]: TaskStatus.FAILED,
  [TypeTaskStatus.NIL]: TaskStatus.UNKNOWN,
};
```

完整映射：

| Protobuf TaskStatus | Dashboard TaskStatus | 展示名称 | 颜色 |
|---|---|---|---|
| PENDING_ARGS_AVAIL | PENDING_ARGS_AVAIL | Waiting for dependencies | 橙色 |
| PENDING_NODE_ASSIGNMENT | PENDING_NODE_ASSIGNMENT | Waiting for scheduling | 橙色 |
| PENDING_OBJ_STORE_MEM_AVAIL | PENDING_NODE_ASSIGNMENT | Waiting for scheduling | 橙色 |
| PENDING_ARGS_FETCH | PENDING_NODE_ASSIGNMENT | Waiting for scheduling | 橙色 |
| SUBMITTED_TO_WORKER | SUBMITTED_TO_WORKER | Waiting for execution | 橙色 |
| PENDING_ACTOR_TASK_ARGS_FETCH | SUBMITTED_TO_WORKER | Waiting for execution | 橙色 |
| PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY | SUBMITTED_TO_WORKER | Waiting for execution | 橙色 |
| RUNNING | RUNNING | Running | 蓝色 |
| RUNNING_IN_RAY_GET | RUNNING | Running | 蓝色 |
| RUNNING_IN_RAY_WAIT | RUNNING | Running | 蓝色 |
| FINISHED | FINISHED | Finished | 绿色 |
| FAILED | FAILED | Failed | 红色 |
| NIL | UNKNOWN | Unknown | 灰色 |

### 8.3 Task Table vs Ray Core Overview

**Task Table**：展示每个 task 的精确状态（GCS TaskEvent 最新 state），能看到所有细粒度状态。

**Ray Core Overview / Job Progress**：展示汇总统计，15 个细粒度状态合并为 7 个大类。

### 8.4 PENDING_ARGS_FETCH 和 PENDING_OBJ_STORE_MEM_AVAIL 在 API 中的处理

**不会少 task 数**。task 数按 task_id 计数。

当普通 task 在 raylet 侧等待 args 拉取时：
- Owner 侧状态始终是 `PENDING_NODE_ASSIGNMENT`
- GCS TaskEvent 中 `state` = `PENDING_NODE_ASSIGNMENT`
- Raylet 侧 metrics 拆分为 `PENDING_ARGS_FETCH` / `PENDING_OBJ_STORE_MEM_AVAIL`，但不写入 TaskEvent

如果需要区分"等调度"还是"等内存"还是"正在拉取"，只能通过 Prometheus metrics (`ray_tasks` gauge) 查看。

---

## 9. PENDING_ACTOR_TASK_ARGS_FETCH 代码逻辑详解

PENDING_ACTOR_TASK_ARGS_FETCH 是 Actor task 在 **executor worker 内部**等待 args 就绪的状态。与普通 task 的 `PENDING_ARGS_FETCH` 不同，它发生在 `SUBMITTED_TO_WORKER` 之后、`RUNNING` 之前，由 actor worker 的 task execution queue 内部通过 `task_event_buffer_` 记录并**写入 GCS TaskEvent**。

### 9.1 触发位置

代码位置：`src/ray/core_worker/task_execution/ordered_actor_task_execution_queue.cc:110-117`

```cpp
if (!dependencies.empty()) {
  RAY_UNUSED(task_event_buffer_.RecordTaskStatusEventIfNeeded(
      task_spec.TaskId(), task_spec.JobId(), task_spec.AttemptNumber(),
      task_spec, rpc::TaskStatus::PENDING_ACTOR_TASK_ARGS_FETCH,
      /* include_task_info */ false));
  waiter_.AsyncWait(dependencies, callback);
}
```

同样位置：`src/ray/core_worker/task_execution/unordered_actor_task_execution_queue.cc:178`

### 9.2 完整触发链

```
1. Actor Worker 收到 PushActorTask RPC
   → TaskReceiver::QueueTaskForExecution() (task_receiver.cc:144)
   → task_spec.IsActorTask() → actor_task_execution_queue_->EnqueueTask()

2. EnqueueTask 检查 dependencies
   → 不为空 → RecordTaskStatusEvent(PENDING_ACTOR_TASK_ARGS_FETCH)
   → waiter_.AsyncWait(deps, callback)
     → ActorTaskExecutionArgWaiter::AsyncWait() (common.cc:65)
       → 生成 tag，存 callback 到 in_flight_waits_
       → async_wait_for_args_(args, tag)
         = raylet_ipc_client_->WaitForActorCallArgs(args, tag) (core_worker.cc:390)

3. Raylet 收到 IPC
   → ProcessWaitForActorCallArgsRequestMessage (node_manager.cc:1689)
   → AsyncWait(client, refs)  // 拉取缺失对象
   → wait_manager_.Wait(ids, -1, all, callback)

4. 所有 args 到齐
   → worker->ActorCallArgWaitComplete(tag) (worker.cc:229)
   → RPC: ActorCallArgWaitComplete(tag) → actor worker

5. Actor Worker 收到 RPC
   → HandleActorCallArgWaitComplete (core_worker.cc:3635)
   → actor_task_execution_arg_waiter_->MarkReady(tag) (common.cc:72)
   → 执行 callback（Step 2 中注册的）

6. Callback
   → RecordTaskStatusEvent(PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY) (:138-144)
   → MarkDependenciesResolved() (:145)  // pending_dependencies_.clear()
   → ExecuteQueuedTasks() (:146)
```

### 9.3 关键区别

- 普通 task 的 args 拉取发生在 **raylet 侧**（LeaseDependencyManager + PullManager），通过 metrics gauge 报告 `PENDING_ARGS_FETCH` / `PENDING_OBJ_STORE_MEM_AVAIL`，**不写入 GCS TaskEvent**
- Actor task 的 args 拉取发生在 **actor worker 侧**（通过 IPC 让 raylet 拉取），通过 `task_event_buffer_` 记录 `PENDING_ACTOR_TASK_ARGS_FETCH`，**写入 GCS TaskEvent**
- Actor task 没有等 plasma 内存的子状态，因为 `wait_manager_.Wait` 不做 active/inactive quota 管理

---

## 10. 普通 Task 完整生命周期

### 10.1 阶段 1：提交（Owner CoreWorker）

用户调用 `func.remote(*args)` → Owner CoreWorker 创建 TaskSpecification

代码：`src/ray/core_worker/task_manager.cc:334-351`

```cpp
// AddTask
absl::MutexLock lock(&mu_);
auto inserted = submissible_tasks_.try_emplace(
    spec.TaskId(), spec, max_retries, num_returns, task_counter_, max_oom_retries);
num_pending_tasks_++;

// 初始状态 → PENDING_ARGS_AVAIL
RAY_UNUSED(task_event_buffer_.RecordTaskStatusEventIfNeeded(
    spec.TaskId(), spec.JobId(), spec.AttemptNumber(), spec,
    rpc::TaskStatus::PENDING_ARGS_AVAIL, /* include_task_info */ true));
```

**GCS**：`PENDING_ARGS_AVAIL`，记录 `creation_time_ms`
**Dashboard**：Waiting for dependencies

### 10.2 阶段 2：依赖解析（Owner CoreWorker）

代码：`src/ray/core_worker/task_submission/normal_task_submitter.cc:127-149`

```cpp
void NormalTaskSubmitter::SubmitTask(TaskSpecification task_spec) {
  resolver_.ResolveDependencies(task_spec, [this, task_spec](Status status) mutable {
    task_manager_.MarkDependenciesResolved(task_spec.TaskId());
    if (!status.ok()) {
      task_manager_.FailOrRetryPendingTask(
          task_spec.TaskId(), rpc::ErrorType::DEPENDENCY_RESOLUTION_FAILED, &status);
      return;
    }
    // 加入 scheduling_key_entries_ 队列，等待请求 worker
  });
}
```

`MarkDependenciesResolved`（`task_manager.cc:1675-1686`）：

```cpp
void TaskManager::MarkDependenciesResolved(const TaskID &task_id) {
  RAY_CHECK(it->second.GetStatus() == rpc::TaskStatus::PENDING_ARGS_AVAIL);
  SetTaskStatus(it->second, rpc::TaskStatus::PENDING_NODE_ASSIGNMENT);
}
```

**GCS**：`PENDING_NODE_ASSIGNMENT`
**Dashboard**：Waiting for scheduling

### 10.3 阶段 3：调度（Raylet ClusterLeaseManager）

Owner 向 Raylet 发送 `RequestWorkerLease` RPC → Raylet ClusterLeaseManager 调度

- 选到本节点 → `local_lease_manager_.QueueAndScheduleLease()`
- 选到远端 → spillback reply
- 无节点可用 → 移入 `infeasible_leases_`

**GCS 状态不变**：仍然是 `PENDING_NODE_ASSIGNMENT`

Raylet 内部细分（不写入 GCS，仅 Prometheus metrics）：

```
WaitForLeaseArgsRequests (local_lease_manager.cc:99-124)
  ├─ 无 deps / args 已就绪 → 入 leases_to_grant_
  └─ args 未就绪 → 入 waiting_lease_queue_
     LeaseDependencyManager.RequestLeaseDependencies (lease_dependency_manager.cc:213)
       → waiting_leases_counter_.Increment(task_key)
       → object_manager_.Pull(deps, TASK_ARGS, task_key)

     PullManager.Pull (pull_manager.cc:55-107)
       → AddBundlePullRequest → 初始 inactive
       → UpdatePullsBasedOnAvailableMemory → 决定 active/inactive

     Metrics 回调:
       num_total = waiting_leases_counter_[key]
       num_inactive = PullManager.NumInactivePulls(key)
       Record(-num_total, PENDING_NODE_ASSIGNMENT)    // 抵消 Owner 上报
       Record(num_total - num_inactive, PENDING_ARGS_FETCH)
       Record(num_inactive, PENDING_OBJ_STORE_MEM_AVAIL)
```

### 10.4 阶段 4：本地调度与分配 Worker（Raylet LocalLeaseManager）

`LocalLeaseManager::GrantScheduledLeasesToWorkers`：

1. `PinLeaseArgsIfMemoryAvailable` — 从 plasma 获取 RayObject 引用并 pin
   - args 被 evict → 回 `waiting_lease_queue_`
   - `pinned_lease_arguments_bytes_ > max_pinned_lease_arguments_bytes_` → 不 grant
   - pin 成功 → 继续
2. `AllocateLocalTaskResources` — 分配本地资源
3. `PopWorker` → 分配 idle worker

当 `PopWorker` 成功拿到 worker → Owner 收到回复

代码：`src/ray/core_worker/task_submission/normal_task_submitter.cc:739-758`

```cpp
void NormalTaskSubmitter::PushNormalTask(...) {
  // PENDING_NODE_ASSIGNMENT → SUBMITTED_TO_WORKER
  task_manager_.MarkTaskWaitingForExecution(task_id,
                                            NodeID::FromBinary(addr.node_id()),
                                            WorkerID::FromBinary(addr.worker_id()));
  client->PushNormalTask(std::move(request), callback);
}
```

`MarkTaskWaitingForExecution`（`task_manager.cc:1688-1703`）：

```cpp
void TaskManager::MarkTaskWaitingForExecution(const TaskID &task_id,
                                              const NodeID &node_id,
                                              const WorkerID &worker_id) {
  RAY_CHECK(it->second.GetStatus() == rpc::TaskStatus::PENDING_NODE_ASSIGNMENT);
  it->second.SetNodeId(node_id);
  SetTaskStatus(it->second, rpc::TaskStatus::SUBMITTED_TO_WORKER,
                TaskStatusEvent::TaskStateUpdate(node_id, worker_id));
}
```

**GCS**：`SUBMITTED_TO_WORKER`，带 `node_id` 和 `worker_id`
**Dashboard**：Waiting for execution

### 10.5 阶段 5：执行（Worker CoreWorker）

Worker 收到 `PushTaskRequest` → `TaskReceiver::QueueTaskForExecution`

代码：`src/ray/core_worker/task_execution/task_receiver.cc:144-248`

```cpp
void TaskReceiver::QueueTaskForExecution(rpc::PushTaskRequest request, ...) {
  TaskSpecification task_spec(std::move(*request.mutable_task_spec()));
  // 普通 task 直接入 NormalTaskExecutionQueue
  normal_task_execution_queue_->EnqueueTask(
      TaskToExecute(execute_callback, cancel_callback, std::move(task_spec)));
}
```

`NormalTaskExecutionQueue::ExecuteQueuedTasks`（`normal_task_execution_queue.cc:72-76`）：

```cpp
void NormalTaskExecutionQueue::ExecuteQueuedTasks() {
  while (auto task = TryPopQueuedTask()) {
    task->Execute();  // → execute_callback_ → task_handler_()
  }
}
```

注意：普通 task 在 worker 端**立即执行**，没有 worker 内部的中间状态。

子状态（metrics only，不写入 GCS）：
- `RUNNING_IN_RAY_GET` — task 内调用 `ray.get()`（`core_worker.cc:1511`，`ScopedTaskMetricSetter` RAII）
- `RUNNING_IN_RAY_WAIT` — task 内调用 `ray.wait()`（`core_worker.cc:1696`）
- `GETTING_AND_PINNING_ARGS` — 执行前获取 args（`core_worker.cc:3012`）

**GCS**：`RUNNING`，记录 `start_time_ms`
**Dashboard**：Running

### 10.6 阶段 6：完成与重试（Owner CoreWorker）

Worker 执行完成 → `PushTaskReply` 返回结果给 Owner

`CompletePendingTask`（`task_manager.cc:1000-1060`）：

```cpp
if (is_application_error) {
  SetTaskStatus(it->second, rpc::TaskStatus::FAILED,
      TaskStatusEvent::TaskStateUpdate(gcs::GetRayErrorInfo(
          rpc::ErrorType::TASK_EXECUTION_EXCEPTION, reply.task_execution_error())));
} else {
  SetTaskStatus(it->second, rpc::TaskStatus::FINISHED);
}
num_pending_tasks_--;
// 如果有重试次数且返回了 plasma 对象 → 保留 task entry 以备重试
// 否则 → submissible_tasks_.erase(it)
```

`FailOrRetryPendingTask`（`task_manager.cc:1165-1262`）：

```cpp
if (will_retry) {
  // FAILED → PENDING_ARGS_AVAIL（重新开始生命周期，attempt_number + 1）
  SetTaskStatus(task_entry, rpc::TaskStatus::PENDING_ARGS_AVAIL,
                /* state_update */ std::nullopt,
                /* include_task_info */ true,
                task_entry.spec_.AttemptNumber() + 1);
  async_retry_task_callback_(spec, delay_ms);
} else {
  SetTaskStatus(it->second, rpc::TaskStatus::FAILED, ...);
  submissible_tasks_.erase(it);
}
```

`FailPendingTask`（`task_manager.cc:1264-1341`）：

```cpp
if (status != nullptr && status->IsIntentionalSystemExit()) {
  // intentional exit → FINISHED（不标记 FAILED）
  SetTaskStatus(it->second, rpc::TaskStatus::FINISHED);
} else {
  SetTaskStatus(it->second, rpc::TaskStatus::FAILED,
      TaskStatusEvent::TaskStateUpdate(error_info));
}
submissible_tasks_.erase(it);
num_pending_tasks_--;
```

**GCS**：`FINISHED` 或 `FAILED`，记录 `end_time_ms`
**Dashboard**：Finished / Failed

### 10.7 普通 Task 状态转变全图（GCS/Dashboard 视角）

```
用户调用 func.remote()
         │
         ▼
   PENDING_ARGS_AVAIL ──────────── GCS 记录：creation_time_ms
         │                           Dashboard: "Waiting for dependencies"
         │  MarkDependenciesResolved()
         │  (normal_task_submitter.cc:132)
         ▼
   PENDING_NODE_ASSIGNMENT ────── GCS 记录：无额外字段
         │                           Dashboard: "Waiting for scheduling"
         │                           Raylet metrics 细分（不写入 GCS）:
         │                             PENDING_ARGS_FETCH (活跃拉取)
         │                             PENDING_OBJ_STORE_MEM_AVAIL (等内存)
         │
         │  MarkTaskWaitingForExecution()
         │  (normal_task_submitter.cc:756)
         ▼
   SUBMITTED_TO_WORKER ─────────── GCS 记录：node_id, worker_id
         │                           Dashboard: "Waiting for execution"
         │
         │  Worker 开始执行
         ▼
   RUNNING ─────────────────────── GCS 记录：start_time_ms
         │                           Dashboard: "Running"
         │                           子状态（metrics only）:
         │                             RUNNING_IN_RAY_GET
         │                             RUNNING_IN_RAY_WAIT
         │                             GETTING_AND_PINNING_ARGS
         │
    ┌────┴────┐
    │         │
    ▼         ▼
 FINISHED    FAILED ─────────────── GCS 记录：end_time_ms
                                    Dashboard: "Finished" / "Failed"

 FAILED + 有重试次数:
    │
    ▼
 PENDING_ARGS_AVAIL ─────────── 回到阶段 2 重新流转（attempt_number + 1）
```

### 10.8 GCS TaskEvent 记录方式

`SetTaskStatus`（`task_manager.cc:1705-1727`）写入 `task_event_buffer_`：

```cpp
void TaskManager::SetTaskStatus(TaskEntry &task_entry, rpc::TaskStatus status, ...) {
  task_entry.SetStatus(status);
  RAY_UNUSED(task_event_buffer_.RecordTaskStatusEventIfNeeded(
      task_entry.spec_.TaskId(), task_entry.spec_.JobId(),
      attempt_number_to_record, task_entry.spec_, status,
      include_task_info, state_update_to_record));
}
```

`RecordTaskStatusEventIfNeeded` 向 GCS 发送 `TaskStateUpdate`，包含：
- `task_id`
- `task_status` (protobuf TaskStatus)
- `attempt_number`
- `state_ts_ns` (Map<state, timestamp_ns>) — 每个状态变更的时间戳
- `task_info` (函数名、资源等，仅在 PENDING_ARGS_AVAIL 时带 full info)
- `error_info` (仅 FAILED 时)

---

## 11. Actor Task 完整生命周期

### 11.1 阶段 1：提交（Owner CoreWorker）

用户调用 `actor.method.remote(*args)` → Owner CoreWorker 创建 TaskSpecification

与普通 task 相同：`AddTask` → `PENDING_ARGS_AVAIL`

```cpp
RAY_UNUSED(task_event_buffer_.RecordTaskStatusEventIfNeeded(
    spec.TaskId(), spec.JobId(), spec.AttemptNumber(), spec,
    rpc::TaskStatus::PENDING_ARGS_AVAIL, /* include_task_info */ true));
```

**GCS**：`PENDING_ARGS_AVAIL`

### 11.2 阶段 2：依赖解析（Owner CoreWorker）

代码：`src/ray/core_worker/task_submission/actor_task_submitter.cc:196-244`

```cpp
if (task_queued) {
  resolver_.ResolveDependencies(task_spec,
    [this, send_pos, concurrency_group, actor_id, task_id](Status status) {
      task_manager_.MarkDependenciesResolved(task_id);

      if (status.ok()) {
        actor_submit_queue->MarkDependencyResolved(concurrency_group, send_pos);
        SendPendingTasks(actor_id);
      } else {
        actor_submit_queue->MarkDependencyFailed(concurrency_group, send_pos);
        task_manager_.FailOrRetryPendingTask(
            task_id, rpc::ErrorType::DEPENDENCY_RESOLUTION_FAILED, &status);
      }
    });
}
```

**GCS**：`PENDING_NODE_ASSIGNMENT`

### 11.3 阶段 3：直接发给 Actor Worker — SUBMITTED_TO_WORKER

Actor task **不经 raylet 调度**，Owner 直接通过 RPC 将 task 发给已经运行的 actor worker。

代码：`src/ray/core_worker/task_submission/actor_task_submitter.cc:596-641`

```cpp
void ActorTaskSubmitter::PushActorTask(...) {
  // PENDING_NODE_ASSIGNMENT → SUBMITTED_TO_WORKER
  task_manager_.MarkTaskWaitingForExecution(task_id,
                                            NodeID::FromBinary(addr.node_id()),
                                            WorkerID::FromBinary(addr.worker_id()));
  core_worker_client_pool_.GetOrConnect(addr)->PushActorTask(
      std::move(request), skip_queue, std::move(wrapped_callback));
}
```

**GCS**：`SUBMITTED_TO_WORKER`

注意：从此之后，Actor task 的状态变更由 **actor worker 的 task execution queue** 内部记录并上报 GCS，Owner 不再主动更新状态。

### 11.4 阶段 4：Actor Worker 内排队与 Args 拉取

Actor Worker 收到 `PushActorTask` RPC → `TaskReceiver::QueueTaskForExecution`

代码：`src/ray/core_worker/task_execution/task_receiver.cc:208-248`

```cpp
if (task_spec.IsActorTask()) {
  auto it = actor_task_execution_queues_.find(task_spec.CallerWorkerId());
  if (it == actor_task_execution_queues_.end()) {
    it = actor_task_execution_queues_
             .emplace(task_spec.CallerWorkerId(),
                      allow_out_of_order_execution_
                          ? UnorderedActorTaskExecutionQueue(...)
                          : OrderedActorTaskExecutionQueue(...))
             .first;
  }
  it->second->EnqueueTask(
      request.sequence_number(), request.client_processed_up_to(),
      TaskToExecute(execute_callback, cancel_callback, std::move(task_spec)));
}
```

**EnqueueTask 内部逻辑**（`ordered_actor_task_execution_queue.cc:67-160`）：

```
有 dependencies:
  → RecordTaskStatusEvent(PENDING_ACTOR_TASK_ARGS_FETCH)  ← 写入 GCS TaskEvent
  → waiter_.AsyncWait(deps, callback)
    → IPC: WaitForActorCallArgs → raylet
    → raylet: AsyncWait + wait_manager_.Wait
    → 所有 args 到齐 → worker->ActorCallArgWaitComplete(tag)
    → RPC 回 actor worker → MarkReady(tag) → 执行 callback
  → callback:
    → RecordTaskStatusEvent(PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY)
    → MarkDependenciesResolved()
    → ExecuteQueuedTasks()

无 dependencies:
  → RecordTaskStatusEvent(PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY)
  → ExecuteQueuedTasks()
```

**GCS**：
- `PENDING_ACTOR_TASK_ARGS_FETCH` — 等 args 拉到本地
- `PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY` — 等 执行顺序/并发度

**Dashboard**：两者都合并展示为 "Waiting for execution"

### 11.5 阶段 5：执行

`ExecuteQueuedTasks`（`ordered_actor_task_execution_queue.cc:174-287`）：

```
对每个 concurrency group:
  1. 清理过期 task (seq_no < next_seq_no → cancel)
  2. 处理 retry tasks (不要求顺序, deps 已 resolve 即 ExecuteRequest)
  3. 处理顺序 tasks:
     seq_no == next_seq_no 且 DependenciesResolved() → ExecuteRequest() → next_seq_no++
     否则 break（等更早 seq_no）
  4. 如果队头 deps 已就绪但 seq_no 不对 → 启动 reorder_wait_timer（超时则 cancel all）
```

`ExecuteRequest` → `pool->Post(AcceptRequestOrRejectIfCanceled)` → `request.Execute()` → `task_handler_()` → RUNNING

**GCS**：`RUNNING`

### 11.6 阶段 6：完成（Owner CoreWorker）

代码：`src/ray/core_worker/task_submission/actor_task_submitter.cc:643-735`

```cpp
void ActorTaskSubmitter::HandlePushTaskReply(...) {
  if (status.ok() && !is_retryable_exception) {
    if (task_manager_.IsTaskCanceled(task_id) && !reply.is_application_error()) {
      task_manager_.FailPendingTask(task_id, rpc::ErrorType::TASK_CANCELLED, ...);
    } else {
      task_manager_.CompletePendingTask(task_id, reply, addr, reply.is_application_error());
    }
  } else if (is_retryable_exception) {
    task_manager_.FailOrRetryPendingTask(task_id, ...);
  } else {
    // 网络错误/actor 死亡
    task_manager_.FailOrRetryPendingTask(task_id, error_type, &status, &error_info, ...);
  }
}
```

`CompletePendingTask` 内部：

```cpp
if (is_application_error) {
  SetTaskStatus(it->second, rpc::TaskStatus::FAILED, ...);
} else {
  SetTaskStatus(it->second, rpc::TaskStatus::FINISHED);
}
```

**GCS**：`FINISHED` 或 `FAILED`

### 11.7 Actor Task 状态转变全图（GCS/Dashboard 视角）

```
用户调用 actor.method.remote()
         │
         ▼
   PENDING_ARGS_AVAIL ──────────── GCS 记录：creation_time_ms
         │                           Dashboard: "Waiting for dependencies"
         │
         │  MarkDependenciesResolved()
         │  (actor_task_submitter.cc:215)
         ▼
   PENDING_NODE_ASSIGNMENT ────── GCS 记录：无额外字段
         │                           Dashboard: "Waiting for scheduling"
         │
         │  MarkTaskWaitingForExecution()
         │  (actor_task_submitter.cc:636)
         ▼
   SUBMITTED_TO_WORKER ─────────── GCS 记录：node_id, worker_id
         │                           Dashboard: "Waiting for execution"
         │
    ┌────┴─────────────────────────────┐
    │ (Actor Worker 内部状态)          │
    │                                  │
    │ 有 deps:                         │ 无 deps:
    │   │                              │   │
    │   ▼                              │   ▼
    │ PENDING_ACTOR_TASK_ARGS_FETCH    │ PENDING_ACTOR_TASK_ORDERING_...
    │   │                              │   │
    │   │ args 就绪 callback            │   │
    │   ▼                              │   │
    │ PENDING_ACTOR_TASK_ORDERING_...  │   │
    │   │                              │   │
    └───┴──────────────────────────────┘
         │
         │ ExecuteQueuedTasks() → request.Execute()
         ▼
   RUNNING ─────────────────────── GCS 记录：start_time_ms
         │                           Dashboard: "Running"
         │                           子状态（metrics only）:
         │                             RUNNING_IN_RAY_GET
         │                             RUNNING_IN_RAY_WAIT
         │
    ┌────┴────┐
    │         │
    ▼         ▼
 FINISHED    FAILED ─────────────── GCS 记录：end_time_ms
                                    Dashboard: "Finished" / "Failed"

 FAILED + 有重试次数:
    │
    ▼
 PENDING_ARGS_AVAIL ─────────── 回到阶段 2 重新流转（attempt_number + 1）
```

注意：`PENDING_ACTOR_TASK_ARGS_FETCH` 和 `PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY` 是 **actor worker 内部的 task execution queue** 通过 `task_event_buffer_` 写入 GCS 的，Owner 侧不感知这些状态。Dashboard 取最新 event 的 state 展示。

---

## 12. 普通 Task vs Actor Task 状态对比

| 生命周期阶段 | 普通 Task GCS 状态 | Actor Task GCS 状态 | 区别 |
|---|---|---|---|
| 1. 提交 | `PENDING_ARGS_AVAIL` | `PENDING_ARGS_AVAIL` | 相同 |
| 2. 依赖解析 | `PENDING_NODE_ASSIGNMENT` | `PENDING_NODE_ASSIGNMENT` | 相同 |
| 3. 调度 | `PENDING_NODE_ASSIGNMENT`（raylet 内部调度） | `PENDING_NODE_ASSIGNMENT` → `SUBMITTED_TO_WORKER`（直接发给 actor） | Actor task 不经 raylet 调度，owner 直接发 RPC |
| 4. 等待资源/args | `PENDING_NODE_ASSIGNMENT`（GCS 不细分，Raylet metrics 细分为 `PENDING_ARGS_FETCH` / `PENDING_OBJ_STORE_MEM_AVAIL`） | `PENDING_ACTOR_TASK_ARGS_FETCH`（写入 GCS） | 普通 task 的 args 拉取在 raylet 侧；Actor task 的 args 拉取在 worker 侧 |
| 5. 等待执行 | `SUBMITTED_TO_WORKER` | `PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY`（写入 GCS） | Actor task 在 worker 内部排队等顺序/并发度 |
| 6. 执行 | `RUNNING` | `RUNNING` | 相同 |
| 7. 完成 | `FINISHED` / `FAILED` | `FINISHED` / `FAILED` | 相同 |
| 8. 重试 | `FAILED` → `PENDING_ARGS_AVAIL` | `FAILED` → `PENDING_ARGS_AVAIL` | 相同 |

**核心差异**：

1. **调度路径不同**：普通 task 经 raylet 调度（ClusterLeaseManager + LocalLeaseManager），actor task owner 直接发给 actor worker
2. **Args 拉取位置不同**：普通 task 在 raylet 侧（LeaseDependencyManager + PullManager），actor task 在 worker 侧（WaitForActorCallArgs IPC）
3. **中间状态不同**：普通 task 在 raylet 侧等待时 GCS 只有 `PENDING_NODE_ASSIGNMENT`，actor task 在 worker 侧有 `PENDING_ACTOR_TASK_ARGS_FETCH` 和 `PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY` 两个 GCS 可见状态
4. **排队机制不同**：普通 task 在 worker 端立即执行（NormalTaskExecutionQueue 不排队），actor task 在 worker 端按 seq_no 排队（OrderedActorTaskExecutionQueue）
5. **Actor task 没有 `PENDING_ARGS_FETCH` / `PENDING_OBJ_STORE_MEM_AVAIL`**：因为走 `wait_manager_.Wait` 路径，不做 active/inactive quota 管理

---

## 附录：相关环境变量汇总

| 变量 | 定义位置 | 作用 |
|---|---|---|
| `RAY_TMPDIR` | `python/ray/_common/utils.py:328` | 最优先，设置 Ray 临时目录 |
| `TMPDIR` | `python/ray/_common/utils.py:330` | Linux 下次优临时目录 |
| `RAY_BACKEND_LOG_LEVEL` | `src/ray/util/logging.cc:284` | C++ 日志级别 (trace/debug/info/warning/error/fatal) |
| `RAY_BACKEND_LOG_JSON` | `src/ray/util/logging.cc:314` | JSON 格式日志 (1=开启) |
| `RAY_ROTATION_MAX_BYTES` | `src/ray/util/logging.cc:324` | 日志轮转大小 |
| `RAY_ROTATION_BACKUP_COUNT` | `src/ray/util/logging.cc:337` | 轮转备份数 |
| `RAY_LOG_TO_STDERR` | `python/ray/_private/node.py:854` | 日志直接输出 stderr |
| `RAY_LOG_MONITOR_MANY_FILES_THRESHOLD` | `python/ray/_private/log_monitor.py:36` | Log monitor 背压阈值 |
| `RAY_RUNTIME_ENV_LOG_TO_DRIVER_ENABLED` | `python/ray/_private/log_monitor.py:39` | 转发 runtime_env 日志到 driver |
| `TPU_LOG_DIR` | `python/ray/_private/node.py:573` | TPU 日志目录符号链接 |
