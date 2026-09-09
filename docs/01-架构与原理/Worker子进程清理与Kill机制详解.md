# Ray Worker 子进程清理与 Kill 机制详解

本文档详细分析 Ray 中 Worker 子进程清理的三种机制、`ray.kill()` 完整调用链、Worker 优雅退出路径，以及 Raylet 检测 Worker 断连的原理。

---

## 目录

1. [配置总览](#1-配置总览)
2. [机制一：kill_child_processes_on_worker_exit（默认启用）](#2-机制一kill_child_processes_on_worker_exit)
3. [机制二：kill_child_processes_on_worker_exit_with_raylet_subreaper（已废弃）](#3-机制二kill_child_processes_on_worker_exit_with_raylet_subreaper)
4. [机制三：process_group_cleanup_enabled（推荐）](#4-机制三process_group_cleanup_enabled)
5. [三种机制对比](#5-三种机制对比)
6. [ray.kill() 完整调用链](#6-raykill-完整调用链)
7. [Worker Graceful Kill 路径](#7-worker-graceful-kill-路径)
8. [Raylet Worker 断连检测](#8-raylet-worker-断连检测)
9. [GCS 为什么要绕道 Raylet](#9-gcs-为什么要绕道-raylet)
10. [vLLM 场景风险分析](#10-vllm-场景风险分析)

---

## 1. 配置总览

三个配置定义在 `src/ray/common/ray_config_def.h`：

```cpp
// Line 975: Worker 退出时自己杀直系子进程（默认 true）
RAY_CONFIG(bool, kill_child_processes_on_worker_exit, true)

// Line 982: 让 Raylet/CoreWorker 成为 Linux subreaper，Raylet 周期杀未知子进程（默认 false，已废弃）
RAY_CONFIG(bool, kill_child_processes_on_worker_exit_with_raylet_subreaper, false)

// Line 987: Worker 放入独立 process group，Raylet 用 killpg 清理（默认 false，推荐）
RAY_CONFIG(bool, process_group_cleanup_enabled, false)
```

相关超时配置（`src/ray/common/ray_config_def.h`）：

```cpp
// Line 287: SIGTERM 后等多久发 SIGKILL（默认 5000ms）
RAY_CONFIG(int64_t, kill_worker_timeout_milliseconds, 5000)

// Line 293: GCS 等待 Actor 优雅退出的超时（默认 30000ms）
RAY_CONFIG(int64_t, actor_graceful_shutdown_timeout_ms, 30000)
```

---

## 2. 机制一：kill_child_processes_on_worker_exit

### 2.1 核心思路

Worker 进程在退出时，自己扫描 `/proc` 找到所有直系子进程，然后 SIGKILL 它们。

**仅在 Worker 正常退出时生效，Worker crash 时无法执行。**

### 2.2 代码实现

#### KillChildProcs — 旧版入口（`src/ray/core_worker/core_worker.cc:611-659`）

```cpp
void CoreWorker::KillChildProcs() {
  if (!RayConfig::instance().kill_child_processes_on_worker_exit()) {
    return;
  }
  // 扫描 /proc，找到所有 ppid == 当前 worker PID 的进程
  auto maybe_child_procs = GetAllProcsWithPpid(GetPID());
  if (!maybe_child_procs) return;

  for (const auto &child_pid : *maybe_child_procs) {
    // 直接 SIGKILL，没有 SIGTERM 优雅退出窗口
    auto error_code = KillProc(child_pid);  // → kill(pid, SIGKILL)
  }
}
```

#### KillChildProcessesImmediately — 新版入口（`src/ray/core_worker/core_worker_shutdown_executor.cc:268-302`）

逻辑与 `KillChildProcs` 完全一致，但集成到了新的 shutdown coordinator 框架中：

```cpp
void CoreWorkerShutdownExecutor::KillChildProcessesImmediately() {
  if (!RayConfig::instance().kill_child_processes_on_worker_exit()) {
    return;
  }
  auto maybe_child_procs = GetAllProcsWithPpid(GetPID());
  if (!maybe_child_procs) return;

  for (const auto &child_pid : *maybe_child_procs) {
    auto maybe_error_code = KillProc(child_pid);  // SIGKILL
  }
}
```

#### 底层工具函数（`src/ray/util/process_utils.cc`）

**KillProc** — 对单个 PID 发 SIGKILL：

```cpp
// Line 165-171
static inline std::error_code KillProcLinux(pid_t pid) {
  std::error_code error;
  if (kill(pid, SIGKILL) != 0) {
    error = std::error_code(errno, std::system_category());
  }
  return error;
}
```

**GetAllProcsWithPpid** — 扫描 `/proc` 找直系子进程：

```cpp
// Line 195-246
static inline std::vector<pid_t> GetAllProcsWithPpidLinux(pid_t parent_pid) {
  std::vector<pid_t> child_pids;
  std::filesystem::directory_iterator dir(kProcDirectory);
  for (const auto &file : dir) {
    if (!file.is_directory()) continue;
    const auto filename = file.path().filename().string();
    if (!std::all_of(filename.begin(), filename.end(), ::isdigit)) continue;

    pid_t pid = std::stoi(filename);
    std::ifstream status_file(file.path() / "status");
    if (!status_file.is_open()) continue;

    std::string line;
    const std::string key = "PPid:";
    while (std::getline(status_file, line)) {
      if (line.substr(0, key.size()) != key) continue;
      pid_t ppid = std::stoi(line.substr(key.size()));
      if (ppid == parent_pid) child_pids.push_back(pid);
      break;
    }
  }
  return child_pids;
}
```

#### 触发时机

- **ForceExit 路径**：`ExecuteForceShutdown()` → `KillChildProcessesImmediately()` → `QuickExit()`
  （`core_worker_shutdown_executor.cc:112-117`）

- **Exit 路径**：`ExecuteExit()` 的 `shutdown_callback` 中调用 `KillChildProcessesImmediately()`
  （`core_worker_shutdown_executor.cc:154, 164`）

- **Raylet 死亡路径**：`ExitIfParentRayletDies()` → `KillChildProcs()` → `QuickExit()`
  （`core_worker.cc:791-804`）

### 2.3 局限性

| 局限 | 说明 |
|------|------|
| Worker crash 时无效 | 只有正常/强制退出才能执行 |
| 只杀直系子进程 | 不递归，杀不到孙子进程（如 vLLM Engine Core） |
| 直接 SIGKILL | 没有 SIGTERM 优雅退出窗口 |
| /proc 扫描竞态 | 扫描后、杀之前新 fork 的子进程可能被漏掉 |

---

## 3. 机制二：kill_child_processes_on_worker_exit_with_raylet_subreaper

### 3.1 核心思路

通过 `prctl(PR_SET_CHILD_SUBREAPER, 1)` 让 Raylet 和 CoreWorker 都成为 Linux subreaper。Worker 死后其子进程被 reparent 到 Raylet，Raylet 每 10 秒扫描一次并杀掉"未知"子进程。

### 3.2 Raylet 启动时设置 subreaper（`src/ray/raylet/main.cc:440-464`）

```cpp
auto enable_subreaper = [&]() {
#ifdef __linux__
    if (ray::SetThisProcessAsSubreaper()) {
      ray::KnownChildrenTracker::instance().Enable();
      ray::SetupSigchldHandlerRemoveKnownChildren(main_service);
      auto runner = ray::PeriodicalRunner::Create(main_service);
      runner->RunFnPeriodically([runner]() { ray::KillUnknownChildren(); },
                                /*period_ms=*/10000,
                                "Raylet.KillUnknownChildren");
      RAY_LOG(INFO) << "Set this process as subreaper. Will kill unknown children every "
                       "10 seconds.";
    } else {
      ray::SetSigchldIgnore();
    }
#else
    ray::SetSigchldIgnore();
#endif
};
```

启用条件（`src/ray/raylet/main.cc:567`）：

```cpp
if (subreaper_enabled && !pg_enabled) {
    enable_subreaper();
} else {
    ray::SetSigchldIgnore();
}
```

### 3.3 CoreWorker 也设自己为 subreaper（`src/ray/core_worker/core_worker_process.cc:145-164`）

```cpp
if (RayConfig::instance().kill_child_processes_on_worker_exit_with_raylet_subreaper()) {
#ifdef __linux__
    if (SetThisProcessAsSubreaper()) {
      RAY_LOG(INFO) << "Set this core_worker process as subreaper: " << pid
                    << " (deprecated; prefer per-worker process groups).";
      SetSigchldIgnore();
    } else {
      RAY_LOG(WARNING) << "Failed to set this core_worker process as subreaper...";
    }
#else
    RAY_LOG(WARNING) << "Subreaper is not supported on this platform.";
#endif
}
```

### 3.4 Subreaper 核心实现（`src/ray/util/subreaper.cc`）

#### SetThisProcessAsSubreaper

```cpp
// Line 85-91
bool SetThisProcessAsSubreaper() {
  if (prctl(PR_SET_CHILD_SUBREAPER, 1) == -1) {
    RAY_LOG(WARNING) << "Failed to set this process as subreaper: " << strerror(errno);
    return false;
  }
  return true;
}
```

#### SIGCHLD 处理器 — 收割僵尸并从 tracker 移除

```cpp
// Line 56-79
void SigchldHandlerReapZombieAndRemoveKnownChildren(
    const boost::system::error_code &error, int signal_number) {
  int status;
  pid_t pid;
  while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
    if (WIFEXITED(status)) {
      RAY_LOG(INFO) << "Child process " << pid << " exited with status "
                    << WEXITSTATUS(status);
    } else if (WIFSIGNALED(status)) {
      RAY_LOG(INFO) << "Child process " << pid << " exited from signal "
                    << WTERMSIG(status);
    }
    KnownChildrenTracker::instance().RemoveKnownChild(pid);
  }
}
```

#### KillUnknownChildren — 周期杀未知子进程

```cpp
// Line 109-128
void KillUnknownChildren() {
  auto to_kill =
      KnownChildrenTracker::instance().ListUnknownChildren([]() -> std::vector<pid_t> {
        auto child_procs = GetAllProcsWithPpid(GetPID());
        if (!child_procs) { return {}; }
        return *child_procs;
      });
  for (auto pid : to_kill) {
    RAY_LOG(INFO) << "Killing leaked child process " << pid;
    auto error = KillProc(pid);  // SIGKILL
  }
}
```

#### KnownChildrenTracker — 已知子进程追踪器

```cpp
// Line 130-163
void KnownChildrenTracker::AddKnownChild(std::function<pid_t()> create_child_fn) {
  if (!enabled_) { create_child_fn(); return; }
  absl::MutexLock lock(&m_);
  pid_t pid = create_child_fn();
  children_.insert(pid);
}

void KnownChildrenTracker::RemoveKnownChild(pid_t pid) {
  if (!enabled_) { return; }
  absl::MutexLock lock(&m_);
  children_.erase(pid);
}

std::vector<pid_t> KnownChildrenTracker::ListUnknownChildren(
    std::function<std::vector<pid_t>()> list_pids_fn) {
  if (!enabled_) { return list_pids_fn(); }
  absl::MutexLock lock(&m_);
  std::vector<pid_t> pids = list_pids_fn();
  std::vector<pid_t> result;
  for (pid_t pid : pids) {
    if (children_.count(pid) == 0) {
      result.push_back(pid);
    }
  }
  return result;
}
```

### 3.5 检测周期

**10 秒**，硬编码不可配置。

### 3.6 风险

| 风险 | 说明 |
|------|------|
| 直接 SIGKILL | 没有 SIGTERM 优雅退出窗口 |
| PID 回收竞态 | 代码中 TODO 注释指出存在 PID recycling 风险 |
| 10 秒延迟 | Worker crash 后最多 10 秒才能清理孤儿进程 |

---

## 4. 机制三：process_group_cleanup_enabled

### 4.1 核心思路

Worker 启动时通过 `setpgrp()` 放入独立 process group（PGID = PID），Raylet 在 Worker 断连时用 `killpg(pgid, SIGTERM)` 给整个进程组发 SIGTERM，200ms 后升级为 SIGKILL。

### 4.2 Worker 创建时设置独立 process group

#### WorkerPool 决定使用新 process group（`src/ray/raylet/worker_pool.cc:687-697`）

```cpp
const bool new_process_group = RayConfig::instance().process_group_cleanup_enabled();
std::unique_ptr<ProcessInterface> child =
    std::make_unique<Process>(argv.data(), ec,
                              /*decouple=*/false, env,
                              /*pipe_to_stdin=*/false,
                              add_to_cgroup_hook_,
                              new_process_group);
```

#### 子进程中调用 setpgrp（`src/ray/util/process.cc:313-341`）

```cpp
if (pid == 0) {  // 子进程
    if (new_process_group) {
        // setpgrp() 等价于 setpgid(0, 0)
        // 将子进程放入以自己 PID 为 PGID 的新 process group
        if (setpgrp() == -1) {
            dprintf(STDERR_FILENO,
                    "ray: setpgrp() failed in child: errno=%d (%s)\n", err, msg);
        }
    }
    // ... exec worker binary
}
```

### 4.3 Worker 注册时保存 PGID（`src/ray/raylet/worker_pool.cc:820-835`）

```cpp
std::unique_ptr<ProcessInterface> process = std::make_unique<Process>(pid);
worker->SetProcess(std::move(process));
#if !defined(_WIN32)
  pid_t pgid = -1;
  errno = 0;
  pgid = getpgid(pid);
  if (pgid != -1) {
    worker->SetSavedProcessGroupId(pgid);
  } else {
    RAY_LOG(WARNING) << "getpgid(" << pid << ") failed at registration: " << strerror(errno);
  }
#endif
```

如果 `setpgrp()` 成功，pgid == pid；如果失败，pgid == raylet 的 PGID（安全防护会拦截此情况）。

### 4.4 Worker 断连时清理（`src/ray/raylet/node_manager.cc:1511-1544`）

```cpp
// DisconnectClient 中：
#if !defined(_WIN32)
    const bool pg_enabled = RayConfig::instance().process_group_cleanup_enabled();
    if (pg_enabled) {
      auto saved = worker->GetSavedProcessGroupId();
      if (saved.has_value()) {
        // 第一阶段：SIGTERM 给整个 process group
        CleanupProcessGroupSend(*saved, worker->WorkerId(), "DisconnectClient", SIGTERM);
        // 第二阶段：200ms 后探测，若进程组仍存在则 SIGKILL
        auto timer = std::make_shared<boost::asio::deadline_timer>(
            io_service_, boost::posix_time::milliseconds(200));
        auto wid = worker->WorkerId();
        auto pgid = *saved;
        timer->async_wait(
            [timer, wid, pgid](const boost::system::error_code &ec) mutable {
              if (!ec) {
                auto probe = KillProcessGroup(pgid, 0);  // signal 0 探测
                const bool group_absent = (probe && probe->value() == ESRCH);
                if (!group_absent) {
                  CleanupProcessGroupSend(pgid, wid, "DisconnectClient", SIGKILL);
                }
              }
            });
      }
    }
#endif
```

### 4.5 安全防护（`src/ray/raylet/node_manager.cc:107-133`）

```cpp
void CleanupProcessGroupSend(pid_t saved_pgid, const WorkerID &wid,
                             const std::string &ctx, int sig) {
  // 防止误杀 raylet 自身的 process group
  pid_t raylet_pgid = getpgid(0);
  if (raylet_pgid == saved_pgid) {
    RAY_LOG(WARNING).WithField(wid)
        << ctx << ": skipping PG cleanup: worker pgid equals raylet pgid (isolation failed)";
    return;
  }
  RAY_LOG(INFO).WithField(wid) << ctx << ": sending "
                               << (sig == SIGKILL ? "SIGKILL" : "SIGTERM")
                               << " to pgid=" << saved_pgid;
  auto err = KillProcessGroup(saved_pgid, sig);
}
```

### 4.6 KillProcessGroup 实现（`src/ray/util/process_utils.cc:182-192`）

```cpp
std::optional<std::error_code> KillProcessGroup(pid_t pgid, int sig) {
#if !defined(_WIN32)
  std::error_code error;
  if (killpg(pgid, sig) != 0) {
    error = std::error_code(errno, std::system_category());
  }
  return {error};
#else
  return std::nullopt;
#endif
}
```

### 4.7 Raylet 关闭时的清理（`src/ray/raylet/node_manager.cc:3070-3095`）

同步执行（不用定时器，因为 shutdown 期间定时器不可靠）：

```cpp
void NodeManager::Stop() {
#if !defined(_WIN32)
  if (RayConfig::instance().process_group_cleanup_enabled()) {
    for (const auto &w : workers) {
      auto saved = w->GetSavedProcessGroupId();
      if (saved.has_value()) {
        CleanupProcessGroupSend(*saved, w->WorkerId(), "Stop", SIGTERM);
        auto probe = KillProcessGroup(*saved, 0);
        const bool group_absent = (probe && probe->value() == ESRCH);
        if (!group_absent) {
          CleanupProcessGroupSend(*saved, w->WorkerId(), "Stop", SIGKILL);
        }
      }
    }
  }
#endif
}
```

---

## 5. 三种机制对比

| 特性 | kill_child_processes_on_worker_exit | subreaper（已废弃） | process_group_cleanup（推荐） |
|------|------|------|------|
| 默认值 | true | false | false |
| 执行者 | Worker 自身 | Raylet（周期扫描） | Raylet（断连时） |
| Worker crash 安全 | 否 | 是 | 是 |
| 信号 | SIGKILL（无优雅退出） | SIGKILL（无优雅退出） | SIGTERM → 200ms → SIGKILL |
| 覆盖范围 | 仅直系子进程 | 递归所有后代 | 整个 process group |
| 响应速度 | 即时 | 最多 10 秒 | 即时 + 200ms 升级 |
| 平台限制 | Linux | Linux >= 3.4 | POSIX（非 Windows） |
| 逃逸方式 | 无 | 无 | 子进程调用 `setsid()` 可脱离 |

---

## 6. ray.kill() 完整调用链

### 6.1 调用链全景

```
Python: ray.kill(actor)                     [python/ray/_private/worker.py:3320]
  │  force_kill 始终为 True
  ↓
Cython: kill_actor(actor_id, True)          [python/ray/_raylet.pyx:3906]
  │  第二个参数 True = force_kill
  ↓
C++ CoreWorker::KillActor()                 [src/ray/core_worker/core_worker.cc:2776]
  │  验证 actor handle 存在后，发 RPC 到 GCS
  ↓
GCS: AsyncKillActor(force_kill=True)        [src/ray/gcs_rpc_client/accessors/actor_info_accessor.cc:207]
  │
  ↓
GCS: HandleKillActorViaGcs()                [src/ray/gcs/actor/gcs_actor_manager.cc:636]
  ├─ no_restart=True  →  DestroyActor()     // 永久销毁
  └─ no_restart=False →  KillActor()        // 可能重启
  │
  ↓
GCS: NotifyRayletToKillActor()              [src/ray/gcs/actor/gcs_actor_manager.cc:1894]
  │  发 KillLocalActor RPC 到 Raylet
  ↓
Raylet: HandleKillLocalActor()              [src/ray/raylet/node_manager.cc:3560]
  │  1) 向 Worker 发 KillActor RPC
  │  2) 同时启动 kill_worker_timeout_milliseconds 定时器（5秒）
  ↓
Worker: HandleKillActor()                   [src/ray/core_worker/core_worker.cc:4273]
  ├─ force_kill=True  →  ForceExit()        // ray.kill() 走这条
  └─ force_kill=False →  Exit()             // __ray_terminate__ 走这条
```

### 6.2 Python API（`python/ray/_private/worker.py:3320-3359`）

```python
@PublicAPI
@client_mode_hook
def kill(actor: "ray.actor.ActorHandle", *, no_restart: bool = True):
    """Kill an actor forcefully.

    This will interrupt any running tasks on the actor, causing them to fail
    immediately. ``atexit`` handlers installed in the actor will not be run.

    If you want to kill the actor but let pending tasks finish,
    you can call ``actor.__ray_terminate__.remote()`` instead.
    """
    worker = global_worker
    worker.check_connected()
    if not isinstance(actor, ray.actor.ActorHandle):
        raise ValueError("ray.kill() only supported for actors.")
    try:
        worker.core_worker.kill_actor(actor._ray_actor_id, no_restart)
    except ActorHandleNotFoundError as e:
        raise ActorHandleNotFoundError(...) from e
```

### 6.3 Cython 绑定（`python/ray/_raylet.pyx:3906-3918`）

```cython
def kill_actor(self, ActorID actor_id, c_bool no_restart):
    cdef:
        CActorID c_actor_id = actor_id.native()
        CRayStatus status = CRayStatus.OK()

    with nogil:
        status = CCoreWorkerProcess.GetCoreWorker().KillActor(
            c_actor_id, True, no_restart)  # 第二个参数 True = force_kill

    if status.IsNotFound():
        raise ActorHandleNotFoundError(status.message().decode())
    check_status(status)
```

**关键：`force_kill` 始终为 `True`。**

### 6.4 CoreWorker::KillActor（`src/ray/core_worker/core_worker.cc:2776-2806`）

```cpp
Status CoreWorker::KillActor(const ActorID &actor_id, bool force_kill, bool no_restart) {
  std::promise<Status> p;
  auto f = p.get_future();
  io_service_.post(
      [this, p = &p, actor_id, force_kill, no_restart]() {
        auto cb = [this, p, actor_id, force_kill, no_restart](Status status) mutable {
          if (status.ok()) {
            gcs_client_->Actors().AsyncKillActor(
                actor_id, force_kill, no_restart, nullptr);
          }
          p->set_value(std::move(status));
        };
        if (actor_creator_->IsActorInRegistering(actor_id)) {
          actor_creator_->AsyncWaitForActorRegisterFinish(actor_id, std::move(cb));
        } else if (actor_manager_->CheckActorHandleExists(actor_id)) {
          cb(Status::OK());
        } else {
          cb(Status::NotFound("Failed to find actor handle"));
        }
      }, "CoreWorker.KillActor");
  const auto &status = f.get();
  if (status.ok()) {
    actor_manager_->OnActorKilled(actor_id);
  }
  return status;
}
```

### 6.5 GCS HandleKillActorViaGcs（`src/ray/gcs/actor/gcs_actor_manager.cc:636-663`）

```cpp
void GcsActorManager::HandleKillActorViaGcs(rpc::KillActorViaGcsRequest request,
                                            rpc::KillActorViaGcsReply *reply,
                                            rpc::SendReplyCallback send_reply_callback) {
  const auto &actor_id = ActorID::FromBinary(request.actor_id());
  auto it = registered_actors_.find(actor_id);
  if (it != registered_actors_.end()) {
    bool force_kill = request.force_kill();
    bool no_restart = request.no_restart();
    if (no_restart) {
      DestroyActor(actor_id, GenKilledByApplicationCause(GetActor(actor_id)));
    } else {
      KillActor(actor_id, force_kill);
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  }
}
```

### 6.6 GCS DestroyActor 的 graceful 升级逻辑（`src/ray/gcs/actor/gcs_actor_manager.cc:1009-1069`）

```cpp
// 取消已有的 graceful shutdown 定时器（仅 force_kill 时）
if (force_kill) {
  auto timer_it = graceful_shutdown_timers_.find(actor->GetWorkerID());
  if (timer_it != graceful_shutdown_timers_.end()) {
    timer_it->second->cancel();
  }
}

// 发送 kill 请求到 Raylet
NotifyRayletToKillActor(actor, death_cause, force_kill);

// 非强制模式：启动优雅退出超时定时器
if (!force_kill && graceful_shutdown_timeout_ms > 0 &&
    graceful_shutdown_timers_.find(worker_id) == graceful_shutdown_timers_.end()) {
  auto timer = std::make_unique<boost::asio::deadline_timer>(io_context_);
  timer->expires_from_now(
      boost::posix_time::milliseconds(graceful_shutdown_timeout_ms));  // 默认 30s
  timer->async_wait(
      [weak_self = weak_from_this(), actor_id, worker_id, death_cause](const auto &error) {
        auto self = weak_self.lock();
        if (!self) { return; }
        self->graceful_shutdown_timers_.erase(worker_id);
        if (error == boost::asio::error::operation_aborted) { return; }
        RAY_LOG(WARNING) << "Graceful shutdown timeout exceeded. Falling back to force kill.";
        // 30 秒后升级为 force kill
        self->NotifyRayletToKillActor(actor_iter->second, death_cause, /*force_kill=*/true);
      });
  graceful_shutdown_timers_[worker_id] = std::move(timer);
}
```

### 6.7 GCS → Raylet：NotifyRayletToKillActor（`src/ray/gcs/actor/gcs_actor_manager.cc:1894-1926`）

```cpp
void GcsActorManager::NotifyRayletToKillActor(const std::shared_ptr<GcsActor> &actor,
                                              const rpc::ActorDeathCause &death_cause,
                                              bool force_kill) {
  rpc::KillLocalActorRequest request;
  request.set_intended_actor_id(actor->GetActorID().Binary());
  request.set_worker_id(actor->GetWorkerID().Binary());
  request.mutable_death_cause()->CopyFrom(death_cause);
  request.set_force_kill(force_kill);

  auto actor_raylet_client =
      raylet_client_pool_.GetOrConnectByAddress(actor->LocalRayletAddress().value());
  actor_raylet_client->KillLocalActor(request, [...] {});
}
```

### 6.8 Raylet：HandleKillLocalActor（`src/ray/raylet/node_manager.cc:3560-3627`）

```cpp
void NodeManager::HandleKillLocalActor(rpc::KillLocalActorRequest request,
                                       rpc::KillLocalActorReply *reply,
                                       rpc::SendReplyCallback send_reply_callback) {
  auto worker = worker_pool_.GetRegisteredWorker(WorkerID::FromBinary(request.worker_id()));
  if (!worker || worker->IsDead()) {
    send_reply_callback(Status::OK(), nullptr, nullptr);
    return;
  }

  auto worker_id = worker->WorkerId();
  rpc::KillActorRequest kill_actor_request;
  kill_actor_request.set_intended_actor_id(request.intended_actor_id());
  kill_actor_request.set_force_kill(request.force_kill());
  kill_actor_request.mutable_death_cause()->CopyFrom(request.death_cause());
  std::shared_ptr<bool> replied = std::make_shared<bool>(false);

  // 启动 5 秒兜底定时器
  auto timer = execute_after(
      io_service_,
      [this, send_reply_callback, worker_id, replied]() {
        if (*replied) { return; }
        auto current_worker = worker_pool_.GetRegisteredWorker(worker_id);
        if (current_worker) {
          RAY_LOG(INFO) << "Worker did not exit after "
                        << RayConfig::instance().kill_worker_timeout_milliseconds()
                        << "ms, force killing with SIGKILL.";
          DestroyWorker(current_worker,
                        rpc::WorkerExitType::INTENDED_SYSTEM_EXIT,
                        "Actor killed by GCS", /*force=*/true);
        }
        *replied = true;
        send_reply_callback(Status::OK(), nullptr, nullptr);
      },
      std::chrono::milliseconds(
          RayConfig::instance().kill_worker_timeout_milliseconds()));  // 默认 5000ms

  // 向 Worker 发 KillActor RPC
  worker->rpc_client()->KillActor(kill_actor_request, [...] {});
}
```

### 6.9 Worker：HandleKillActor（`src/ray/core_worker/core_worker.cc:4273-4305`）

```cpp
void CoreWorker::HandleKillActor(rpc::KillActorRequest request,
                                 rpc::KillActorReply *reply,
                                 rpc::SendReplyCallback send_reply_callback) {
  ActorID intended_actor_id = ActorID::FromBinary(request.intended_actor_id());
  if (intended_actor_id != worker_context_->GetCurrentActorID()) {
    send_reply_callback(Status::Invalid("Mismatched ActorID"), nullptr, nullptr);
    return;
  }

  const auto &kill_actor_reason = gcs::GenErrorMessageFromDeathCause(request.death_cause());

  if (request.force_kill()) {
    // ray.kill() 走这条路径
    ForceExit(rpc::WorkerExitType::INTENDED_SYSTEM_EXIT,
              absl::StrCat("Worker exits because the actor is killed. ", kill_actor_reason));
  } else {
    // __ray_terminate__ 走这条路径
    Exit(rpc::WorkerExitType::INTENDED_SYSTEM_EXIT,
         absl::StrCat("Worker exits because the actor is killed. ", kill_actor_reason));
  }
}
```

---

## 7. Worker Graceful Kill 路径

### 7.1 Exit vs ForceExit（`src/ray/core_worker/core_worker.cc:661-688`）

```cpp
void CoreWorker::Exit(const rpc::WorkerExitType exit_type, const std::string &detail, ...) {
  ShutdownReason reason = ConvertExitTypeToShutdownReason(exit_type);
  shutdown_coordinator_->RequestShutdown(
      /*force_shutdown=*/false, reason, detail,
      ShutdownCoordinator::kInfiniteTimeout, ...);
}

void CoreWorker::ForceExit(const rpc::WorkerExitType exit_type, const std::string &detail) {
  ShutdownReason reason = ConvertExitTypeToShutdownReason(exit_type, true);
  shutdown_coordinator_->RequestShutdown(
      /*force_shutdown=*/true, reason, detail, std::chrono::milliseconds{0}, nullptr);
}
```

### 7.2 ForceExit 执行路径

```
ForceExit()
  → RequestShutdown(force=true, timeout=0ms)
    → ExecuteForceShutdown()
      ├─ KillChildProcessesImmediately()    ← SIGKILL 所有直系子进程
      ├─ DisconnectServices()
      └─ QuickExit()                        ← 立即 _exit(1)
```

代码（`src/ray/core_worker/core_worker_shutdown_executor.cc:112-117`）：

```cpp
void CoreWorkerShutdownExecutor::ExecuteForceShutdown(std::string_view exit_type,
                                                      std::string_view detail) {
  KillChildProcessesImmediately();
  DisconnectServices(exit_type, detail, nullptr);
  QuickExit();
}
```

**特点**：不做 task drain，不等 RPC 完成，直接强杀退出。

### 7.3 Exit 执行路径

```
Exit()
  → RequestShutdown(force=false, timeout=kInfiniteTimeout)
    → ExecuteExit()
      → DrainAndShutdown(drain_references_callback)
        → shutdown_callback:
            ├─ DrainServerCallExecutor()
            ├─ KillChildProcessesImmediately()    ← 也杀子进程
            ├─ DisconnectServices()
            └─ ExecuteGracefulShutdown(timeout=30000ms)
                → WaitForCompletion(30s) → 超时则 QuickExit()
```

代码（`src/ray/core_worker/core_worker_shutdown_executor.cc:119-157`）：

```cpp
void CoreWorkerShutdownExecutor::ExecuteExit(...) {
  auto shutdown_callback = [this, weak_core_worker, ...]() {
    auto worker = weak_core_worker.lock();
    if (!worker) { NotifyComplete(); return; }

    if (!worker->event_loops_running_.load()) {
      rpc::DrainServerCallExecutor();
      KillChildProcessesImmediately();              // 杀子进程
      DisconnectServices(exit_type, detail, ...);
      ExecuteGracefulShutdown(exit_type, "Post-exit graceful shutdown",
                              std::chrono::milliseconds{30000});
      return;
    }

    worker->task_execution_service_.post([this, ...]() {
      rpc::DrainServerCallExecutor();
      KillChildProcessesImmediately();              // 杀子进程
      DisconnectServices(exit_type, detail, ...);
      ExecuteGracefulShutdown(exit_type, "Post-exit graceful shutdown",
                              std::chrono::milliseconds{30000});
    }, "CoreWorker.Shutdown");
  };
  core_worker->task_manager_->DrainAndShutdown(drain_references_callback);
}
```

**特点**：先 drain 所有 pending tasks 和 object refs，等待 RPC 服务排空，最多等 30 秒，超时后 `QuickExit()`。

### 7.4 Raylet 的 SIGTERM → SIGKILL 升级

#### Worker::KillAsync（`src/ray/raylet/worker.cc:64-100`）

```cpp
void Worker::KillAsync(instrumented_io_context &io_service, bool force) {
  bool expected = false;
  if (!killing_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
    return;  // 幂等：只第一次调用有效
  }
  const auto worker = shared_from_this();
  if (force) {
    proc_->Kill();    // 直接 SIGKILL
    return;
  }
  // 优雅模式：先 SIGTERM
  kill(proc_->GetId(), SIGTERM);

  // kill_worker_timeout_milliseconds 后升级 SIGKILL（默认 5000ms）
  auto retry_timer = std::make_shared<boost::asio::deadline_timer>(io_service);
  auto timeout = RayConfig::instance().kill_worker_timeout_milliseconds();
  retry_timer->expires_from_now(boost::posix_time::milliseconds(timeout));
  retry_timer->async_wait(
      [timeout, retry_timer, worker](const boost::system::error_code &error) {
        if (worker->proc_->IsAlive()) {
          RAY_LOG(INFO) << "Worker did not exit after " << timeout
                        << "ms, force killing with SIGKILL.";
        } else {
          return;
        }
        worker->proc_->Kill();   // 升级 SIGKILL
      });
}
```

#### DestroyWorker（`src/ray/raylet/node_manager.cc:530-545`）

```cpp
void NodeManager::DestroyWorker(std::shared_ptr<WorkerInterface> worker,
                                rpc::WorkerExitType disconnect_type,
                                const std::string &disconnect_detail,
                                bool force) {
  DisconnectClient(worker->Connection(), /*graceful=*/false, disconnect_type, disconnect_detail);
  worker->KillAsync(io_service_, force);
}
```

### 7.5 ray.kill() 完整时序

```
t=0s    Python: ray.kill(actor)
        │
        ├─ CoreWorker → GCS: KillActor(force_kill=True, no_restart=True)
        │
t=~0s   GCS: DestroyActor()
        │
        ├─ GCS → Raylet: KillLocalActor(force_kill=True)
        │
t=~0s   Raylet: HandleKillLocalActor
        │
        ├─ Raylet → Worker: KillActor RPC (force_kill=True)
        │   同时启动 5s 定时器
        │
t=~0s   Worker: HandleKillActor → ForceExit()
        │
        ├─ KillChildProcessesImmediately()   ← SIGKILL 直系子进程
        ├─ DisconnectServices()
        └─ QuickExit()                        ← Worker 进程退出
        │
t=~0s   Raylet 检测到 Worker 断连
        │
        ├─ DisconnectClient()
        │   ├─ pg_enabled: killpg(SIGTERM) → 200ms → killpg(SIGKILL)
        │   └─ !pg_enabled: 无额外清理
        │
t=5s    Raylet 定时器到期 → Worker 已死，跳过 DestroyWorker

        ★ 如果 Worker 没有响应 KillActor RPC（卡死/挂起）：
t=5s    Raylet 定时器到期
        → DestroyWorker(worker, force=True) → SIGKILL（只有父进程能做）
```

---

## 8. Raylet Worker 断连检测

### 8.1 连接模型

Worker 通过 **Unix Domain Socket** 主动连接到 Raylet：

```
Raylet (acceptor on socket path)
  ↑
  │  Unix Domain Socket
  │
Worker (connects TO raylet socket)
```

Raylet 在 `main.cc:1100-1102` 创建 acceptor：

```cpp
boost::asio::basic_socket_acceptor<ray::local_stream_protocol> acceptor(
    main_service, ray::ParseUrlEndpoint(raylet_socket_name));
```

Worker 在 `core_worker_process.cc:200-219` 连接：

```cpp
auto raylet_ipc_client = std::make_shared<ray::ipc::RayletIpcClient>(
    io_service_, options.raylet_socket, /*num_retries=*/-1, /*timeout=*/-1);
Status status = raylet_ipc_client->RegisterClient(
    worker_context->GetWorkerID(), options.worker_type, ...);
```

### 8.2 检测机制一：Socket async_read 错误（即时）

当 Worker 死亡时，内核关闭 socket，Raylet 的 `async_read` 收到 EOF/error：

```cpp
// src/ray/raylet/node_manager.cc:1090-1105
void NodeManager::HandleClientConnectionError(
    const std::shared_ptr<ClientConnection> &client,
    const boost::system::error_code &error) {
  const std::string err_msg = absl::StrCat(
      "Worker unexpectedly exits with a connection error code ",
      error.value(), ". ", error.message(),
      ". Some common causes include: (1) OOM killer, (2) ray stop --force, "
      "(3) SIGSEGV or another unexpected error.");

  DisconnectClient(client, /*graceful=*/false,
                    ray::rpc::WorkerExitType::SYSTEM_ERROR, err_msg);
}
```

### 8.3 检测机制二：周期性 POLLHUP 轮询（每 1 秒）

```cpp
// src/ray/raylet/node_manager.cc:260-263
periodical_runner_->RunFnPeriodically(
    [this]() { CheckForUnexpectedWorkerDisconnects(); },
    RayConfig::instance().raylet_check_for_unexpected_worker_disconnect_interval_ms(),
    "NodeManager.CheckForUnexpectedWorkerDisconnects");
```

检查实现（`src/ray/raylet/node_manager.cc:593-618`）：

```cpp
void NodeManager::CheckForUnexpectedWorkerDisconnects() {
  std::vector<std::shared_ptr<ClientConnection>> all_connections;
  std::vector<std::shared_ptr<WorkerInterface>> all_workers =
      worker_pool_.GetAllRegisteredWorkers();
  for (const auto &worker : all_workers) {
    all_connections.push_back(worker->Connection());
  }
  // ... 同样收集 drivers ...

  std::vector<bool> disconnects = CheckForClientDisconnects(all_connections);
  for (size_t i = 0; i < disconnects.size(); i++) {
    if (disconnects[i]) {
      DestroyWorker(all_workers[i], rpc::WorkerExitType::SYSTEM_ERROR,
                    "Worker connection closed unexpectedly.");
    }
  }
}
```

底层 poll 实现（`src/ray/raylet_ipc_client/client_connection.cc:530-557`）：

```cpp
std::vector<bool> CheckForClientDisconnects(
    const std::vector<std::shared_ptr<ClientConnection>> &conns) {
  std::vector<bool> result(conns.size(), false);
  std::vector<pollfd> poll_fds(conns.size());
  for (size_t i = 0; i < conns.size(); ++i) {
    // POLLHUP is populated in revents, no need to specify it in events.
    poll_fds[i] = {conns[i]->GetNativeHandle(), /*events=*/0, /*revents=*/0};
  }

  int ret = poll(poll_fds.data(), poll_fds.size(), /*timeout=*/0);  // 非阻塞
  if (ret > 0) {
    for (size_t i = 0; i < conns.size(); ++i) {
      if (poll_fds[i].revents & POLLHUP) {
        result[i] = true;
      }
    }
  }
  return result;
}
```

**特点**：一次 `poll()` syscall 检查所有 Worker 的 socket FD，非阻塞。

### 8.4 检测机制三：注册超时

```cpp
// src/ray/raylet/worker_pool.cc:630-635
void WorkerPool::MonitorPopWorkerRequestForRegistration(
    std::shared_ptr<PopWorkerRequest> pop_worker_request) {
  auto timer = std::make_shared<boost::asio::deadline_timer>(
      *io_service_,
      boost::posix_time::seconds(
          RayConfig::instance().worker_register_timeout_seconds()));  // 默认 60s
```

### 8.5 没有心跳机制

Raylet 和本地 Worker 之间**没有心跳**。检测完全依赖：
1. Socket async_read 错误（即时）
2. POLLHUP 轮询（每 1 秒）
3. 注册超时（60 秒）

GCS 和 Raylet 之间**有**心跳（gRPC health check，每 3 秒），但这是节点级别的，不是 Worker 级别的。

---

## 9. GCS 为什么要绕道 Raylet

### 9.1 进程树关系

```
Raylet (PID=100)
  ├─ Worker-1 (PID=200)  ← Raylet fork 出来的
  ├─ Worker-2 (PID=300)
  └─ Worker-3 (PID=400)
```

Raylet 是 Worker 的**父进程**，拥有 PID，能发送 UNIX 信号（SIGTERM/SIGKILL）。GCS 是独立服务，跟 Worker 没有父子关系。

### 9.2 能力对比

| 能力 | GCS | Raylet |
|------|-----|--------|
| 发 SIGTERM/SIGKILL | 不能（不是父进程） | **能** |
| 检测 Worker 进程存活 | 不能（无持久连接） | **能**（Unix socket + poll） |
| killpg 清理进程组 | 不能 | **能**（保存了 PGID） |
| 释放本地资源 | 不能 | **能**（管 GPU/CPU/lease） |
| 强杀卡死的 Worker | 不能 | **能**（SIGKILL 无视进程状态） |

### 9.3 连接拓扑

```
Worker ──Unix Socket──→ Raylet ──gRPC──→ GCS
Worker ──gRPC──→ 其他 Worker（跨节点通信）
GCS ──gRPC Health Check──→ Raylet（每 3 秒）
```

- GCS → Raylet：有 gRPC 连接 + 健康检查
- Worker → GCS：Worker 主动连 GCS
- **GCS → Worker：没有持久连接**

### 9.4 如果 GCS 直接杀 Worker 会怎样

| 场景 | GCS 直接杀 | GCS → Raylet 杀 |
|------|-----------|-----------------|
| Worker 卡死不响应 | GCS 无能为力（发不了信号） | Raylet 5 秒后 SIGKILL |
| Worker 假退出 | GCS 无从验证 | Raylet 检测到 socket 断连 |
| Worker 死后资源释放 | GCS 无法操作 | Raylet 立即释放 GPU/CPU |
| 进程组清理 | GCS 不知道 PGID | Raylet 用 killpg 清理 |
| 子进程遗留 | GCS 不管 | Raylet 用 subreaper 或 killpg |

**本质**：GCS 是**决策者**（决定杀谁），Raylet 是**执行者**（实际动手杀）。

---

## 10. vLLM 场景风险分析

### 10.1 进程层级

```
Ray Worker
  └─ vLLM 进程（直系子进程）
       └─ Engine Core 进程（孙子进程）
```

### 10.2 各清理路径对 Engine Core 的效果

| 清理路径 | 对 Engine Core 的效果 |
|---------|---------------------|
| Worker `KillChildProcessesImmediately()` | **杀不到**（Engine Core 是孙子进程） |
| `process_group_cleanup_enabled` | 能杀到，但 **200ms** 后就 SIGKILL，GPU 来不及释放 |
| `kill_child_processes_on_worker_exit_with_raylet_subreaper` | 能杀到，但 **10s** 后才 SIGKILL，无 SIGTERM |
| 都不开 | Engine Core 变成孤儿，**GPU 泄漏** |

### 10.3 最严重场景：Worker crash

```
Worker OOM crash
  ↓
kill_child_processes_on_worker_exit: 只杀直系子进程，杀不到 Engine Core
  ↓
Engine Core 变成孤儿
  ↓
如果 pg_enabled:    killpg(SIGTERM) → 200ms → SIGKILL → GPU 泄漏
如果 subreaper:     10s 后 SIGKILL → GPU 泄漏
如果都不开:        Engine Core 没人管 → GPU 泄漏
```

### 10.4 建议

| 方案 | 推荐度 | 原因 |
|------|--------|------|
| 都不开 | 取决于场景 | Engine Core 变成孤儿，占用 GPU |
| 只开 `process_group_cleanup_enabled` | 中等 | 能清理，但 200ms 对 GPU 进程太暴力 |
| 只开 subreaper | 较好 | 10s 窗口相对宽裕，但仍是 SIGKILL |
| **自定义清理** | **最佳** | Worker 退出前主动 SIGTERM vLLM → 等待足够时间 → SIGKILL |

**最佳实践**：在 Worker 的 shutdown hook 中自己实现优雅退出（先 SIGTERM、等待 5-10 秒、再 SIGKILL），避免依赖 Ray 的暴力清理机制。对于 GPU 进程，至少需要 5-10 秒的清理时间。

---

## 关键源码文件索引

| 文件 | 关键行号 | 内容 |
|------|---------|------|
| `src/ray/common/ray_config_def.h` | 287, 293, 975, 982, 987 | 超时和清理配置 |
| `src/ray/raylet/worker.cc` | 64-100 | `Worker::KillAsync` (SIGTERM → SIGKILL) |
| `src/ray/raylet/node_manager.cc` | 107-133 | `CleanupProcessGroupSend` 安全防护 |
| `src/ray/raylet/node_manager.cc` | 260-263 | 周期性断连检测注册 |
| `src/ray/raylet/node_manager.cc` | 530-545 | `DestroyWorker` |
| `src/ray/raylet/node_manager.cc` | 593-618 | `CheckForUnexpectedWorkerDisconnects` |
| `src/ray/raylet/node_manager.cc` | 1090-1105 | `HandleClientConnectionError` |
| `src/ray/raylet/node_manager.cc` | 1511-1544 | `DisconnectClient` 中 PG 清理 |
| `src/ray/raylet/node_manager.cc` | 3070-3095 | `NodeManager::Stop` 中 PG 清理 |
| `src/ray/raylet/node_manager.cc` | 3560-3627 | `HandleKillLocalActor` |
| `src/ray/raylet/worker_pool.cc` | 687-697 | Worker 创建时传入 `new_process_group` |
| `src/ray/raylet/worker_pool.cc` | 820-835 | Worker 注册时保存 PGID |
| `src/ray/raylet/main.cc` | 440-464 | `enable_subreaper` lambda |
| `src/ray/raylet/main.cc` | 567 | subreaper 启用条件 |
| `src/ray/raylet_ipc_client/client_connection.cc` | 530-557 | `CheckForClientDisconnects` (POLLHUP) |
| `src/ray/core_worker/core_worker.cc` | 611-659 | `KillChildProcs` (旧版) |
| `src/ray/core_worker/core_worker.cc` | 661-688 | `Exit` vs `ForceExit` |
| `src/ray/core_worker/core_worker.cc` | 4273-4305 | `HandleKillActor` |
| `src/ray/core_worker/core_worker_shutdown_executor.cc` | 112-117 | `ExecuteForceShutdown` |
| `src/ray/core_worker/core_worker_shutdown_executor.cc` | 119-170 | `ExecuteExit` |
| `src/ray/core_worker/core_worker_shutdown_executor.cc` | 268-302 | `KillChildProcessesImmediately` |
| `src/ray/core_worker/core_worker_process.cc` | 145-164 | CoreWorker 设为 subreaper |
| `src/ray/util/process_utils.cc` | 164-192 | `KillProc`, `KillProcessGroup` |
| `src/ray/util/process_utils.cc` | 194-255 | `GetAllProcsWithPpid` (/proc 扫描) |
| `src/ray/util/process.cc` | 313-341 | 子进程中 `setpgrp()` |
| `src/ray/util/subreaper.cc` | 85-128 | `SetThisProcessAsSubreaper`, `KillUnknownChildren` |
| `src/ray/util/subreaper.cc` | 130-163 | `KnownChildrenTracker` 实现 |
| `src/ray/gcs/actor/gcs_actor_manager.cc` | 636-663 | `HandleKillActorViaGcs` |
| `src/ray/gcs/actor/gcs_actor_manager.cc` | 1009-1069 | `DestroyActor` graceful 升级逻辑 |
| `src/ray/gcs/actor/gcs_actor_manager.cc` | 1894-1926 | `NotifyRayletToKillActor` |
| `python/ray/_private/worker.py` | 3320-3359 | Python `ray.kill()` API |
| `python/ray/_raylet.pyx` | 3906-3918 | Cython 绑定 (`force_kill=True`) |
