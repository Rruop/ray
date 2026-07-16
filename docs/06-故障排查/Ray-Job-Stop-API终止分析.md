# Ray Job 被 KML 平台 Stop API 终止的完整分析

**时间**: 2026-07-24 05:42
**集群**: kml-task-100035634-record-100501055
**Job ID**: kml-task-100035634-record-100501055-prod-2ktx8
**Head 节点**: 10.137.44.253 (kml-task-...-prod-head-h-0)
**Session**: session_2026-07-22_12-18-30_167526_1
**运行时长**: ~41.4 小时 (148970930ms)

## 1. 结论

**作业是被 KML 平台通过 Ray Dashboard Stop API 主动停止的，不是 Ray 自身问题。**

关键证据来自 `dashboard.log`:
```
2026-07-24 05:42:32,261  10.109.202.137  POST /api/jobs/kml-task-100035634-record-100501055-prod-2ktx8/stop  200  Go-http-client/1.1
```

- 请求方是 `10.109.202.137`，使用 `Go-http-client/1.1`（KML 平台控制面 Go 程序）
- 调用了 Ray Dashboard 的 `POST /api/jobs/<job_id>/stop` 接口
- 返回 `200`，停止请求成功

## 2. 完整事件时间线

| 时间 | 事件 | 来源 |
|------|------|------|
| **05:42:32,261** | **`POST /api/jobs/.../stop` 被调用** — KML 平台主动停止作业 | dashboard.log |
| 05:42:32,641 | Driver CoreWorker (pid=2462) 开始 `Destructing CoreWorkerProcessImpl` — 优雅退出 | python-core-driver log |
| 05:42:34,965 | GCS 定期输出 Debug state（正常） | gcs_server.out |
| 05:42:35,264 | JobSupervisor: SIGTERM 超时 3 秒，强制 SIGKILL | worker-2ec776c1d err |
| 05:42:35,299 | Raylet: `Disconnecting driver, graceful=false, connection error code 2: End of file` | raylet.out |
| 05:42:35,299 | Raylet: `Disconnecting worker, graceful=true` (多个 worker 依次断开) | raylet.out |
| 05:42:35,300 | GCS WorkerManager: `exit_type=SYSTEM_ERROR, Worker unexpectedly exits with a connection error code 2` — 误报为异常退出 | gcs_server.out |
| 05:42:35,300 | GCS ActorManager: Worker `02000000ff...ff` 退出，级联销毁 ~30 个 actor (`force_kill=1, timeout_ms=-1`) | gcs_server.out |
| 05:42:35,300-306 | GCS ActorManager: `Worker ... failed, destroying actor child, job id = 02000000`（`force_kill=0, timeout_ms=30000`） | gcs_server.out |
| 05:42:35,320 | **`Marking job as finished. job_id=02000000`** | gcs_server.out |
| 05:42:35,328 | JobSupervisor actor (job_id=01000000) 被标记失败 | gcs_server.out |
| 05:42:35,334 | `_ray_internal_job_actor` 被 `force_kill=1` 销毁 | gcs_server.out |
| 05:42:35,405 | Raylet: `Force exiting worker whose job has exited` | raylet.out |
| 05:42:37,301 | Raylet: Core worker 不可用超过 1 秒 | raylet.out |
| 05:42:38,274-280 | Head 节点上剩余 actor 失败，`datasets_stats_actor` 被清理 | gcs_server.out |
| **05:42:38,283** | **`Marking job as finished. job_id=03000000/01000000/04000000`** — 所有 job 被标记结束 | gcs_server.out |
| 05:42:38,433+ | Worker 节点收到 SIGTERM — KML 平台回收 Pod 资源 | gcs_server.out |
| 05:42:38,440 | GCS 收到 SIGTERM，shutdown | gcs_server.out |
| 05:42:38,678 | Ray Client Server: 健康检查失败 `RPC error: CANCELLED` | ray_client_server.err |

## 3. 因果链详解

### 3.1 Stop API 调用（根因）

KML 平台组件 (10.109.202.137) 使用 Go HTTP 客户端调用了 Ray Dashboard 的 Stop 接口。

### 3.2 Stop 请求在 Ray 内部的流转

```
POST /api/jobs/<job_id>/stop
    │
    ▼
job_head.py: stop_job()                [HTTP handler, line 422]
    │  委托给 job agent
    ▼
job_agent.py: stop_job()               [Agent handler, line 75]
    │  调用 JobManager
    ▼
job_manager.py: stop_job()             [line 646]
    │  job_supervisor_actor.stop.remote()  [fire-and-forget Ray call]
    ▼
job_supervisor.py: stop()               [line 481]
    │  设置 self._stop_event
    ▼
job_supervisor.py: run()               [line 377]
    │  检测到 _stop_event.is_set()
    │  1. 发送 SIGTERM 给 driver + 子进程
    │  2. 等待 RAY_JOB_STOP_WAIT_TIME_S (默认 3s)
    │  3. 超时后升级为 SIGKILL
    │  4. 更新 job 状态为 STOPPED
    ▼
Driver 进程退出 (SIGTERM/SIGKILL)
    │
    ▼
node_manager.cc: HandleClientConnectionError() [line 1088]
    │  "Worker unexpectedly exits with a connection error code 2"
    │  OR node_manager.cc: DisconnectClient() [line 1421]
    ▼
node_manager.cc: DisconnectClient()    [line 1404]
    │  "Disconnecting driver, graceful=false"
    │  gcs_client_.Jobs().AsyncMarkFinished(job_id, nullptr)
    ▼
gcs_job_manager.cc: HandleMarkJobFinished() [line 199]
    │  获取 job table data，调用 MarkJobAsFinished()
    ▼
gcs_job_manager.cc: MarkJobAsFinished() [line 148]
    │  "Marking job as finished."
    │  设置 is_dead=true, end_time, timestamp
    │  发布 job 事件，清理 runtime env, function refs
    │  ClearJobInfos() → 通知 job_finished_listeners_
```

### 3.3 Worker 断连导致 GCS 级联销毁 Actor

当 Driver 被 SIGKILL 后，其与 raylet 的连接断开：

```
Driver 被 SIGKILL
    │
    ▼
raylet store.cc: 连接断开 error code 2 (EOF)
    │
    ▼
node_manager.cc: HandleClientConnectionError()   [line 1088]
    │  构造错误消息："Worker unexpectedly exits with a connection error code 2..."
    │  调用 DisconnectClient(graceful=false, SYSTEM_ERROR)
    ▼
node_manager.cc: DisconnectClient()              [line 1404]
    │  检测到是 Driver
    │  "Disconnecting driver, graceful=false"
    │  gcs_client_.Jobs().AsyncMarkFinished(job_id, nullptr)
    │  "Driver (pid=2462) is disconnected."
    ▼
gcs_worker_manager.cc: ReportWorkerFailure()     [line 42]
    │  "Reporting worker exit, exit_type = SYSTEM_ERROR"
    │  通知 worker_dead_listeners_
    ▼
gcs_actor_manager.cc: OnWorkerDead()             [line 1184]
    │  "Worker 02000000ff...ff exits, type=SYSTEM_ERROR"
    │  遍历该 worker 拥有的所有 child actor
    │  DestroyActor(child_id, GenOwnerDiedCause(...))  [force_kill=true, timeout_ms=-1]
    │  → "Destroying actor, force_kill=1, timeout_ms=-1"
    ▼
gcs_actor_manager.cc: WaitForActorRefDeleted callback  [line 953]
    │  RPC 失败（owner 已死）
    │  "Worker ... failed, destroying actor child, job id = 02000000"
    │  DestroyActor(actor_id, ..., force_kill=false, timeout_ms=30000)
    │  → "Destroying actor, force_kill=0, timeout_ms=30000"
    │  → "Tried to destroy actor that does not exist"（已被 force_kill=1 先销毁）
```

### 3.4 GCS 报告 `Marking job as finished` 的两条路径

本案例中出现了 **两条** `Marking job as finished` 路径：

1. **05:42:35,320** — `job_id=02000000`：由 raylet 的 `DisconnectClient()` 调用 `AsyncMarkFinished()` 触发，因为 Driver 进程退出
2. **05:42:38,283** — `job_id=03000000/01000000/04000000`：由 `gcs_node_manager.cc:641` 的 `Node is dead, marking all jobs with drivers on this node as finished` 触发，因为节点收到 SIGTERM 后注销

### 3.5 KML 平台回收 Pod 资源

作业被标记 STOPPED 后，KML 平台开始回收所有 Pod 资源：
- 所有 Worker 节点收到 SIGTERM，向 GCS 报告 `EXPECTED_TERMINATION`
- Head 节点 raylet 收到 SIGTERM，触发 `graceful shutdown`
- GCS 自身收到 SIGTERM，执行 `Stopping GCS server`

## 4. GCS "SYSTEM_ERROR" 误报分析

本案例中 GCS 将 Driver 退出报告为 `SYSTEM_ERROR`，这是一个**误报**。根因分析：

### 4.1 误报原因

`node_manager.cc:HandleClientConnectionError()` (line 1088):

```cpp
void NodeManager::HandleClientConnectionError(
    const std::shared_ptr<ClientConnection> &client,
    const boost::system::error_code &error) {
  const std::string err_msg = absl::StrCat(
      "Worker unexpectedly exits with a connection error code ",
      error.value(),
      ". ",
      error.message(),
      ". Some common causes include: (1) the process was killed by the OOM killer "
      "due to high memory usage, (2) ray stop --force was called, or (3) the worker "
      "crashed unexpectedly due to SIGSEGV or another unexpected error.");

  DisconnectClient(
      client, /*graceful=*/false, ray::rpc::WorkerExitType::SYSTEM_ERROR, err_msg);
}
```

当 JobSupervisor 发送 SIGKILL 给 Driver 时，Driver 的 TCP 连接被强制关闭（EOF, error code 2）。Raylet 的连接监控检测到连接断开，但**无法区分**以下场景：
- OOM Kill 导致的进程死亡
- `ray stop --force` 导致的进程死亡
- Job Stop API 导致的 SIGKILL
- Worker 自身 Crash (SIGSEGV 等)

所有这些场景都走 `HandleClientConnectionError`，统一标记为 `graceful=false` + `SYSTEM_ERROR`。

### 4.2 正常停止时的正确标记

如果是 Driver 主动优雅退出（exit code 0），走的是 `DisconnectClient` 的 `graceful=true` 路径，标记为 `INTENDED_SYSTEM_EXIT`。但 JobSupervisor 的 SIGKILL 不会触发优雅退出路径。

### 4.3 误报的级联影响

`SYSTEM_ERROR` 导致 GCS 以为 Worker 异常退出：
1. `OnWorkerDead` 以 `SYSTEM_ERROR` 类型销毁所有 child actor（`force_kill=true`，无优雅关闭）
2. 如果某些 actor 有 `remaining_restarts > 0`，GCS 会尝试重新调度它们（本案例中 `05:42:38,274` 出现了 `need_reschedule=1` 的 actor 尝试重新调度，但因为节点已被 SIGTERM 所以失败）
3. 日志中充斥大量 `SYSTEM_ERROR` 警告，干扰故障排查

## 5. 代码逻辑详解

### 5.1 HTTP Stop API Handler

**文件**: `python/ray/dashboard/modules/job/job_head.py` (line 422)

```python
@routes.post("/api/jobs/{job_or_submission_id}/stop")
async def stop_job(self, req: Request) -> Response:
    job_or_submission_id = req.match_info["job_or_submission_id"]
    job = await find_job_by_ids(
        self.gcs_client,
        self._job_info_client,
        job_or_submission_id,
    )
    if not job:
        return Response(
            text=f"Job {job_or_submission_id} does not exist",
            status=aiohttp.web.HTTPNotFound.status_code,
        )
    if job.type is not JobType.SUBMISSION:
        return Response(
            text="Can only stop submission type jobs",
            status=aiohttp.web.HTTPBadRequest.status_code,
        )

    try:
        job_agent_client = await self.get_target_agent()
        resp = await job_agent_client.stop_job_internal(job.submission_id)
    except Exception:
        return Response(
            text=traceback.format_exc(),
            status=aiohttp.web.HTTPInternalServerError.status_code,
        )

    return Response(
        text=json.dumps(dataclasses.asdict(resp)), content_type="application/json"
    )
```

Head 节点的 handler 将请求委托给 agent:

```python
async def stop_job_internal(self, job_id: str) -> JobStopResponse:
    async with self._session.post(
        f"{self._agent_address}/api/job_agent/jobs/{job_id}/stop",
        headers=self._get_headers(),
    ) as resp:
        ...
```

### 5.2 Job Agent Handler

**文件**: `python/ray/dashboard/modules/job/job_agent.py` (line 75)

```python
@routes.post("/api/job_agent/jobs/{job_or_submission_id}/stop")
@optional_utils.init_ray_and_catch_exceptions()
async def stop_job(self, req: Request) -> Response:
    ...
    stopped = self.get_job_manager().stop_job(job.submission_id)
    resp = JobStopResponse(stopped=stopped)
    ...
```

### 5.3 JobManager.stop_job()

**文件**: `python/ray/dashboard/modules/job/job_manager.py` (line 646)

```python
def stop_job(self, job_id) -> bool:
    job_supervisor_actor = self._get_actor_for_job(job_id)
    if job_supervisor_actor is not None:
        job_supervisor_actor.stop.remote()
        return True
    else:
        return False
```

Fire-and-forget 的 Ray remote call，不等待结果。

### 5.4 JobSupervisor.stop() 和 SIGTERM/SIGKILL 逻辑

**文件**: `python/ray/dashboard/modules/job/job_supervisor.py`

**stop() 方法 (line 481):**

```python
def stop(self):
    self._stop_event.set()
```

**run() 中的停止逻辑 (line 377-424):**

```python
finished, _ = await asyncio.wait(
    [polling_task, create_task(self._stop_event.wait())],
    return_when=FIRST_COMPLETED,
)

if self._stop_event.is_set():
    polling_task.cancel()
    if sys.platform == "win32" and self._win32_job_object:
        win32job.TerminateJobObject(self._win32_job_object, -1)
    elif sys.platform != "win32":
        stop_signal = os.environ.get("RAY_JOB_STOP_SIGNAL", "SIGTERM")
        if stop_signal not in self.VALID_STOP_SIGNALS:
            self._logger.warning(
                f"{stop_signal} not a valid stop signal. Terminating "
                "job with SIGTERM."
            )
            stop_signal = "SIGTERM"

        job_process = psutil.Process(child_pid)
        proc_to_kill = [job_process] + job_process.children(recursive=True)

        self._kill_processes(proc_to_kill, getattr(signal, stop_signal))
        try:
            stop_job_wait_time = int(
                os.environ.get(
                    "RAY_JOB_STOP_WAIT_TIME_S",
                    self.DEFAULT_RAY_JOB_STOP_WAIT_TIME_S,  # 3 秒
                )
            )
            poll_job_stop_task = create_task(self._poll_all(proc_to_kill))
            await asyncio.wait_for(poll_job_stop_task, stop_job_wait_time)
            self._logger.info(
                f"Job {self._job_id} has been terminated gracefully "
                f"with {stop_signal}."
            )
        except asyncio.TimeoutError:
            self._logger.warning(
                f"Attempt to gracefully terminate job {self._job_id} "
                f"through {stop_signal} has timed out after "
                f"{stop_job_wait_time} seconds. Job is now being "
                "force-killed with SIGKILL."
            )
            self._kill_processes(proc_to_kill, signal.SIGKILL)

    await self._job_info_client.put_status(self._job_id, JobStatus.STOPPED)
```

**关键常量:**

```python
DEFAULT_RAY_JOB_STOP_WAIT_TIME_S = 3
SUBPROCESS_POLL_PERIOD_S = 0.1
VALID_STOP_SIGNALS = ["SIGINT", "SIGTERM"]
```

**_kill_processes() 辅助方法 (line 328):**

```python
def _kill_processes(self, processes: List[psutil.Process], sig: signal.Signals):
    for proc in processes:
        try:
            os.kill(proc.pid, sig)
        except ProcessLookupError:
            pass
```

### 5.5 GCS Worker Exit 上报

**文件**: `src/ray/gcs/gcs_worker_manager.cc` (line 42)

```cpp
const auto &worker_address = request.worker_failure().worker_address();
const auto node_id = NodeID::FromBinary(worker_address.node_id());
std::string message =
    absl::StrCat("Reporting worker exit, worker id = ",
                 worker_id.Hex(),
                 ", node id = ", node_id.Hex(),
                 ", address = ", worker_address.ip_address(),
                 ", exit_type = ",
                 rpc::WorkerExitType_Name(request.worker_failure().exit_type()),
                 ", exit_detail = ",
                 request.worker_failure().exit_detail());
if (IsIntentionalWorkerFailure(request.worker_failure().exit_type())) {
  RAY_LOG(DEBUG) << message;
} else {
  RAY_LOG(WARNING) << message
      << ". Unintentional worker failures have been reported...";
}

worker_failure_data->set_is_alive(false);
for (auto &listener : worker_dead_listeners_) {
  listener(worker_failure_data);
}
```

### 5.6 GCS Actor Manager: Worker 死亡后级联销毁 Actor

**文件**: `src/ray/gcs/actor/gcs_actor_manager.cc`

**OnWorkerDead (line 1184):**

```cpp
void GcsActorManager::OnWorkerDead(const ray::NodeID &node_id,
                                   const ray::WorkerID &worker_id) {
  OnWorkerDead(node_id, worker_id, "",
               rpc::WorkerExitType::SYSTEM_ERROR,
               "Worker exits unexpectedly.");
}

void GcsActorManager::OnWorkerDead(const ray::NodeID &node_id,
                                   const ray::WorkerID &worker_id,
                                   const std::string &worker_ip,
                                   const rpc::WorkerExitType disconnect_type,
                                   const std::string &disconnect_detail,
                                   const rpc::RayException *creation_task_exception) {
  std::string message = absl::StrCat("Worker ", worker_id.Hex(),
                                     " on node ", node_id.Hex(),
                                     " exits, type=",
                                     rpc::WorkerExitType_Name(disconnect_type));
  if (disconnect_type == rpc::WorkerExitType::INTENDED_USER_EXIT ||
      disconnect_type == rpc::WorkerExitType::INTENDED_SYSTEM_EXIT) {
    RAY_LOG(DEBUG).WithField(worker_id) << message;
  } else {
    RAY_LOG(WARNING).WithField(worker_id) << message;
  }

  bool need_reconstruct = disconnect_type != rpc::WorkerExitType::INTENDED_USER_EXIT &&
                          disconnect_type != rpc::WorkerExitType::USER_ERROR;
  // 遍历该 worker 拥有的所有 child actor 并销毁
  const auto it = owners_.find(node_id);
  if (it != owners_.end() && it->second.count(worker_id)) {
    auto owner = it->second.find(worker_id);
    const auto children_ids = owner->second.children_actor_ids_;
    for (const auto &child_id : children_ids) {
      DestroyActor(child_id,
                   GenOwnerDiedCause(GetActor(child_id),
                                     worker_id, disconnect_type,
                                     "Owner's worker process has crashed.",
                                     worker_ip));
    }
  }
```

**DestroyActor (line 985):**

```cpp
void GcsActorManager::DestroyActor(const ActorID &actor_id,
                                   const rpc::ActorDeathCause &death_cause,
                                   bool force_kill,
                                   std::function<void()> done_callback,
                                   int64_t graceful_shutdown_timeout_ms) {
  RAY_LOG(INFO).WithField(actor_id.JobId()).WithField(actor_id)
      << "Destroying actor, force_kill=" << force_kill
      << ", timeout_ms=" << graceful_shutdown_timeout_ms;
```

两种调用模式：
- `force_kill=true, timeout_ms=-1`：Owner Worker 死亡时（默认参数），直接强杀
- `force_kill=false, timeout_ms=30000`：Actor 引用归零或 `WaitForActorRefDeleted` RPC 失败时，先尝试优雅关闭 30 秒

**WaitForActorRefDeleted 回调 (line 953):**

```cpp
client->WaitForActorRefDeleted(
    std::move(wait_request),
    [this, owner_node_id, owner_id, actor_id](
        Status status, const rpc::WaitForActorRefDeletedReply &reply) {
      if (!status.ok()) {
        RAY_LOG(INFO) << "Worker " << owner_id
                      << " failed, destroying actor child, job id = "
                      << actor_id.JobId();
      } else {
        RAY_LOG(INFO) << "Actor " << actor_id
                      << " has no references, destroying actor, job id = "
                      << actor_id.JobId();
      }

      auto node_it = owners_.find(owner_node_id);
      if (node_it != owners_.end() && node_it->second.count(owner_id)) {
        int64_t timeout_ms = RayConfig::instance().actor_graceful_shutdown_timeout_ms();
        DestroyActor(actor_id,
                     GenActorRefDeletedCause(GetActor(actor_id)),
                     /*force_kill=*/false, nullptr, timeout_ms);
      }
    });
```

**Graceful shutdown 超时后升级为 force kill (line 1045):**

```cpp
timer->async_wait(
    [weak_self = weak_from_this(), actor_id, worker_id, death_cause,
     graceful_shutdown_timeout_ms](const boost::system::error_code &error) {
      RAY_LOG(WARNING).WithField(actor_id).WithField(worker_id)
          << "Graceful shutdown timeout (" << graceful_shutdown_timeout_ms
          << "ms) exceeded. Falling back to force kill.";

      auto actor_iter = self->registered_actors_.find(actor_id);
      if (actor_iter != self->registered_actors_.end() &&
          actor_iter->second->GetWorkerID() == worker_id) {
        self->NotifyRayletToKillActor(
            actor_iter->second, death_cause, /*force_kill=*/true);
      }
    });
```

### 5.7 Raylet Driver 断连处理

**文件**: `src/ray/raylet/node_manager.cc`

**HandleClientConnectionError (line 1088):**

```cpp
void NodeManager::HandleClientConnectionError(
    const std::shared_ptr<ClientConnection> &client,
    const boost::system::error_code &error) {
  const std::string err_msg = absl::StrCat(
      "Worker unexpectedly exits with a connection error code ",
      error.value(), ". ", error.message(),
      ". Some common causes include: (1) the process was killed by the OOM killer "
      "due to high memory usage, (2) ray stop --force was called, or (3) the worker "
      "crashed unexpectedly due to SIGSEGV or another unexpected error.");
  DisconnectClient(client, /*graceful=*/false,
                   ray::rpc::WorkerExitType::SYSTEM_ERROR, err_msg);
}
```

**DisconnectClient 中的 Driver 处理 (line 1404):**

```cpp
void NodeManager::DisconnectClient(const std::shared_ptr<ClientConnection> &client,
                                   bool graceful,
                                   rpc::WorkerExitType disconnect_type,
                                   const std::string &disconnect_detail, ...) {
  if ((worker = worker_pool_.GetRegisteredDriver(client))) {
    is_driver = true;
    RAY_LOG(INFO).WithField(worker->WorkerId()).WithField(worker->GetAssignedJobId())
        << "Disconnecting driver, graceful=" << std::boolalpha << graceful
        << ", disconnect_type=" << disconnect_type;
  }
  // ...
  if (is_driver) {
    const auto job_id = worker->GetAssignedJobId();
    gcs_client_.Jobs().AsyncMarkFinished(job_id, nullptr);  // 通知 GCS 标记 job 完成
    worker_pool_.DisconnectDriver(worker);
    RAY_LOG(INFO).WithField(worker->WorkerId()).WithField(worker->GetAssignedJobId())
        << "Driver (pid=" << worker->GetProcess().GetId() << ") is disconnected.";
    if (disconnect_type == rpc::WorkerExitType::SYSTEM_ERROR) {
      RAY_EVENT(ERROR, "RAY_DRIVER_FAILURE") << "Driver died...";
    }
  }
```

### 5.8 GCS Job Manager: MarkJobAsFinished

**文件**: `src/ray/gcs/gcs_job_manager.cc` (line 148)

```cpp
void GcsJobManager::MarkJobAsFinished(rpc::JobTableData job_table_data,
                                      std::function<void(Status)> done_callback) {
  const JobID job_id = JobID::FromBinary(job_table_data.job_id());
  RAY_LOG(INFO).WithField(job_id) << "Marking job as finished.";

  auto time = current_sys_time_ms();
  job_table_data.set_timestamp(time);
  job_table_data.set_end_time(time);
  job_table_data.set_is_dead(true);
  auto on_done = [this, job_id, job_table_data,
                   done_callback = std::move(done_callback)](
                     const Status &status) {
    if (!status.ok()) {
      RAY_LOG(ERROR).WithField(job_id) << "Failed to mark job as finished.";
    } else {
      gcs_publisher_.PublishJob(job_id, job_table_data);
      runtime_env_manager_.RemoveURIReference(job_id.Hex());
      ClearJobInfos(job_table_data);  // 通知 job_finished_listeners_
    }
    function_manager_.RemoveJobReference(job_id);
    WriteDriverJobExportEvent(job_table_data,
                              rpc::events::DriverJobLifecycleEvent::FINISHED);
    done_callback(status);
  };
  gcs_table_storage_.JobTable().Put(
      job_id, job_table_data, {std::move(on_done), io_context_});
}
```

**ClearJobInfos (line 231):**

```cpp
void GcsJobManager::ClearJobInfos(const rpc::JobTableData &job_data) {
  for (auto &listener : job_finished_listeners_) {
    listener(job_data);
  }
}
```

`ClearJobInfos` 本身不直接销毁 Actor。Actor 的销毁是通过 Worker 死亡的间接路径触发的。

## 6. 本案例中的特殊问题

### 6.1 `Marking job as finished. job_id=03000000` 的问题

用户注意到的这条日志：

```
[05:42:38,280] Destroying actor, force_kill=1, timeout_ms=-1 job_id=02000000 actor_id=d106d347d97096063090457e02000000
[05:42:38,280] Actor name datasets_stats_actor is cleaned up.
[05:42:38,283] Marking job as finished. job_id=03000000
```

这里 `job_id=03000000` 是 Ray 内部 job（dashboard agent 等），不是用户的业务 job。时间在 05:42:38 是因为：

1. `datasets_stats_actor` 是 job `02000000` 的内部 actor，被最后一个断连的 worker 拥有
2. 当该 worker 在 `05:42:38,274` 断连时，触发了 `OnWorkerDead` → `DestroyActor`
3. `job_id=03000000` 的 `Marking job as finished` 是因为 `gcs_node_manager.cc:489` 的 `Node is dead, marking all jobs with drivers on this node as finished` 逻辑 — 当节点被 SIGTERM 注销时，GCS 会把该节点上所有有 driver 的 job 都标记为 finished

**这不是问题**，是正常的节点注销清理逻辑。

### 6.2 Raylet 磁盘监控误报

`raylet.err` 中大量的 `over 95% full` 警告是误报：
```
/tmp/ray/session_... is over 95% full, available space: 159525 GB; capacity: 1.04858e+07 GB
```

可用空间 159525 GB，总容量 1e+07 GB。这是浮点精度导致的大数百分比误报，`159525 / 10485800 ≈ 1.5%`，实际使用率极低，不影响运行。

## 7. 排查方法总结

### 7.1 如何区分"用户停止"和"Ray 自身异常"

| 特征 | 用户/平台停止 | Ray 自身异常 |
|------|-------------|-------------|
| dashboard.log 中有 `POST /api/jobs/.../stop` | ✅ 有 | ❌ 无 |
| JobSupervisor 日志中有 `SIGTERM timed out, force-killed with SIGKILL` | ✅ 有（stop 流程中发送） | ❌ 无（OOM Kill 不会经过 JobSupervisor） |
| CoreWorker 日志中有 `Destructing CoreWorkerProcessImpl` | ✅ 有（优雅退出） | 不一定（OOM Kill 直接死亡，无析构） |
| dmesg 中有当天 OOM Kill 记录 | ❌ 无 | ✅ 可能有 |
| Worker exit_type | `SYSTEM_ERROR`（误报，实际是 stop 导致） | `SYSTEM_ERROR`（真实） |

### 7.2 关键排查日志文件优先级

1. **`dashboard.log`** — 确认是否有外部 Stop API 调用（**最关键**）
2. **`worker-*JobSupervisor*.err`** — 确认 JobSupervisor 的 stop/signal 逻辑
3. **`python-core-driver-*.log`** — 确认 Driver 是优雅退出还是崩溃
4. **`raylet.out`** — 确认 Driver 断连原因和时间
5. **`gcs_server.out`** — 确认 Worker/Job/Actor 状态变化
6. **`dmesg`** — 确认是否有 OOM Kill

## 8. 代码文件索引

| 组件 | 文件路径 | 关键行号 |
|------|----------|---------|
| HTTP Stop API Handler | `python/ray/dashboard/modules/job/job_head.py` | 422 |
| Job Agent Handler | `python/ray/dashboard/modules/job/job_agent.py` | 75 |
| JobManager.stop_job | `python/ray/dashboard/modules/job/job_manager.py` | 646 |
| JobSupervisor.stop | `python/ray/dashboard/modules/job/job_supervisor.py` | 481 |
| JobSupervisor SIGTERM/SIGKILL | `python/ray/dashboard/modules/job/job_supervisor.py` | 377-424 |
| JobSupervisor._kill_processes | `python/ray/dashboard/modules/job/job_supervisor.py` | 328 |
| GCS MarkJobAsFinished | `src/ray/gcs/gcs_job_manager.cc` | 148 |
| GCS HandleMarkJobFinished | `src/ray/gcs/gcs_job_manager.cc` | 199 |
| GCS ClearJobInfos | `src/ray/gcs/gcs_job_manager.cc` | 231 |
| GCS ReportWorkerFailure | `src/ray/gcs/gcs_worker_manager.cc` | 42 |
| GCS OnWorkerDead | `src/ray/gcs/actor/gcs_actor_manager.cc` | 1184 |
| GCS DestroyActor | `src/ray/gcs/actor/gcs_actor_manager.cc` | 985 |
| GCS WaitForActorRefDeleted callback | `src/ray/gcs/actor/gcs_actor_manager.cc` | 953 |
| GCS Graceful shutdown timeout | `src/ray/gcs/actor/gcs_actor_manager.cc` | 1045 |
| Raylet HandleClientConnectionError | `src/ray/raylet/node_manager.cc` | 1088 |
| Raylet DisconnectClient (Driver) | `src/ray/raylet/node_manager.cc` | 1404 |
| Raylet AsyncMarkFinished | `src/ray/raylet/node_manager.cc` | 1538 |
| GCS Node dead mark jobs finished | `src/ray/gcs/gcs_node_manager.cc` | 489 |
