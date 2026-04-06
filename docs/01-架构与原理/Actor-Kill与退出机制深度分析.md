# Actor Kill 与退出机制完整深度分析

## 一、完整调用链：ray.kill() 的三级跳

`ray.kill()` **必须经过 GCS**，不是直连 Actor 节点。完整路径是三级跳：

```
CoreWorker (调用方)
  ──gRPC──→ GCS (GcsActorManager)
             ──gRPC──→ Raylet (Actor所在节点)
                       ──gRPC──→ CoreWorker (Actor Worker)
```

### 必须经过 GCS 的原因

1. **GCS 是 Actor 状态的唯一权威** — `HandleKillActorViaGcs` 决定走 `DestroyActor`（永久死）还是 `KillActor`（可重启），需要更新 GCS 中的 Actor 状态表
2. **GCS 负责 Actor 生命周期调度** — 如果 `no_restart=False`，GCS 在 kill 后还要调用 `RestartActor()` 重新调度
3. **调用方不知道 Actor 在哪个节点** — `CoreWorker::KillActor()` 只验证本地 handle 存在，不感知 Actor 的物理位置，GCS 通过 `registered_actors_` 查到 `NodeID` 和 `WorkerID` 后才调用 `NotifyRayletToKillActor()`

### 详细调用链

```
ray.kill(actor, no_restart=True)                        # python/ray/_private/worker.py:3270
  │
  ▼
_raylet.pyx::kill_actor(actor_id, no_restart)           # python/ray/_raylet.pyx:3846
  │  force_kill = True (硬编码!)
  │  调用 CCoreWorkerProcess.GetCoreWorker().KillActor(actor_id, True, no_restart)
  ▼
CoreWorker::KillActor(actor_id, force_kill=True, no_restart)  # src/ray/core_worker/core_worker.cc:2749
  │  1. 验证本地 ActorHandle 存在 (actor_manager_->CheckActorHandleExists)
  │  2. 调用 gcs_client_->Actors().AsyncKillActor(actor_id, force_kill, no_restart)
  │  3. 成功后: actor_manager_->OnActorKilled(actor_id)
  ▼
ActorInfoAccessor::AsyncKillActor()                      # src/ray/gcs_rpc_client/accessors/actor_info_accessor.cc:207
  │  发送 KillActorViaGcs RPC 到 GCS
  ▼
GcsActorManager::HandleKillActorViaGcs()                 # src/ray/gcs/actor/gcs_actor_manager.cc:635
  │
  ├── no_restart=True  → DestroyActor()  → 永久死亡
  └── no_restart=False → KillActor()      → 可能重启
       │
       ▼
GcsActorManager::NotifyRayletToKillActor()                # src/ray/gcs/actor/gcs_actor_manager.cc:1831
  │  发送 KillLocalActorRequest 到 Raylet
  ▼
NodeManager::HandleKillLocalActor()                      # src/ray/raylet/node_manager.cc:3399
  │  转发 KillActorRequest 到 Worker 的 CoreWorker
  │  启动安全定时器 (kill_worker_timeout_milliseconds)
  ▼
CoreWorker::HandleKillActor()                            # src/ray/core_worker/core_worker.cc:4352
  │
  ├── force_kill=True  → ForceExit() → 立即退出
  └── force_kill=False → Exit()      → 优雅退出
```

---

## 二、_raylet.pyx kill_actor 源码

```python
# python/ray/_raylet.pyx:3846-3857
def kill_actor(self, ActorID actor_id, c_bool no_restart):
    cdef:
        CActorID c_actor_id = actor_id.native()
        CRayStatus status = CRayStatus.OK()

    with nogil:
        status = CCoreWorkerProcess.GetCoreWorker().KillActor(
            c_actor_id, True, no_restart)  # <-- force_kill 硬编码为 True

    if status.IsNotFound():
        raise ActorHandleNotFoundError(status.message().decode())

    check_status(status)
```

**关键点**：`force_kill` 参数被硬编码为 `True`。Python API 不暴露 `force_kill=False` 选项。对于优雅终止，用户必须使用 `actor.__ray_terminate__.remote()`。

---

## 三、Actor 一定会被 Kill 掉吗？三重保障

**是的，有三重保障机制：**

| 保障层 | 机制 | 超时配置 | 作用 |
|--------|------|----------|------|
| Worker 层 | `HandleKillActor` 收到 `force_kill=True` 时调用 `ForceExit()`，`timeout=0ms` | 0ms | 进程立即退出 |
| Raylet 层 | 安全定时器，超时后 SIGKILL | `kill_worker_timeout_milliseconds` = **5000ms** | 进程没退出则强杀 |
| GCS 层 | 优雅关闭定时器，超时后重发 force kill | `actor_graceful_shutdown_timeout_ms` = **30000ms** | 仅 DestroyActor 路径 |

**唯一例外**：如果 Actor 已不在 `registered_actors_` 中（已被 destroy），`HandleKillActorViaGcs` 返回 `NotFound`，不会执行 kill——但此时 Actor 已经是 DEAD 状态。

---

## 四、两种进程退出方式：Exit() vs ForceExit()

Ray 里的 "Kill" 是 gRPC 消息名，**不代表真的 SIGKILL 进程**。实际退出方式取决于 `force_kill` 参数：

### Exit() — 优雅退出（force_kill=False）

```cpp
// src/ray/core_worker/core_worker.cc:658-672
void CoreWorker::Exit(const rpc::WorkerExitType exit_type,
                      const std::string &detail, ...) {
    ShutdownReason reason = ConvertExitTypeToShutdownReason(exit_type);
    shutdown_coordinator_->RequestShutdown(
        /*force_shutdown=*/false,
        reason,
        detail,
        ShutdownCoordinator::kInfiniteTimeout,    // ← 无限超时！
        creation_task_exception_pb_bytes);
}
```

**流程**：Worker 主循环检测到退出标志 → 等待当前 task 完成 → 正常退出 → Python 解释器正常 shutdown → 跑 `__del__`、`atexit`

### ForceExit() — 立即退出（force_kill=True）

```cpp
// src/ray/core_worker/core_worker.cc:675-686
void CoreWorker::ForceExit(const rpc::WorkerExitType exit_type,
                           const std::string &detail) {
    ShutdownReason reason = ConvertExitTypeToShutdownReason(exit_type, true);
    shutdown_coordinator_->RequestShutdown(
        /*force_shutdown=*/true, reason, detail,
        std::chrono::milliseconds{0}, nullptr);   // ← 立即退出！
}
```

**流程**：进程立即退出，不等任何清理 → Python 解释器没有机会跑 `__del__`、`atexit`

### 各路径对应表

| 触发方式 | force_kill | 进程退出方式 | `__del__` |
|---------|-----------|-------------|-----------|
| `ray.kill()` | True（硬编码） | `ForceExit()` → 立即退出 | ❌ |
| 引用计数出作用域 | False | `Exit()` → 正常退出 | ✅ |
| `__ray_terminate__` | 不走 gRPC | `raise SystemExit` → Python 正常退出 | ✅ |
| Raylet 超时兜底 | — | `DestroyWorker(SIGKILL)` | ❌ |

---

## 五、进程自己退出的完整流程

### Exit() 后 Worker 怎么自己退出的

```
1. 设置 shutdown 状态
   shutdown_coordinator_->RequestShutdown(force_shutdown=false, reason, detail, timeout=无限)

2. 状态机转换
   kRunning → kShuttingDown → kDisconnecting

3. 执行退出序列（异步回调链）
   ExecuteExit()
     → task_manager_->DrainAndShutdown()        // 等当前 task 完成
       → drain_references_callback:
           → task_receiver_->Stop()              // ⚠️ 可能永远阻塞！
           → NotifyWorkerBlocked()               // 释放 CPU 资源
           → shutdown_callback:
               → DrainServerCallExecutor()        // 停止接收新 RPC
               → DisconnectServices()             // 断开 GCS/Raylet 连接
               → ExecuteGracefulShutdown():
                   → actor shutdown callback
                   → task_execution_service_.stop()  // ⬅️ 关键：停止事件循环
                   → Flush task events
                   → Stop IO service
                   → Shutdown gRPC server
                   → Disconnect GCS client

4. RunTaskExecutionLoop() 返回
   task_execution_service_.run() 因 stop() 而返回

5. Python main_loop() 调用 sys.exit(0)
   → Python 正常退出流程 → 跑 __del__、atexit
```

### Worker 主循环中检测退出标志

```cpp
// src/ray/core_worker/core_worker.cc:2908-2935
void CoreWorker::RunTaskExecutionLoop() {
    auto signal_checker = PeriodicalRunner::Create(task_execution_service_);
    if (options_.check_signals) {
        signal_checker->RunFnPeriodically(
            [this] {
                // 每 10ms 检查一次
                if (worker_context_->GetCurrentActorShouldExit()) {
                    Exit(rpc::WorkerExitType::INTENDED_USER_EXIT,
                         "User requested to exit the actor.", nullptr);
                }
                // ... signal 检查 ...
            },
            10, "CoreWorker.CheckSignal");
    }
    event_loops_running_ = true;
    task_execution_service_.run();  // ← 被 stop() 后从这里返回
}
```

### Python 侧入口

```python
# python/ray/_private/worker.py:1018-1027
def main_loop(self):
    def sigterm_handler(signum, frame):
        raise_sys_exit_with_custom_error_message("The process receives a SIGTERM.", exit_code=1)
    ray._private.utils.set_sigterm_handler(sigterm_handler)
    self.core_worker.run_task_loop()  # 调用 C++ RunTaskExecutionLoop
    sys.exit(0)                        # 循环返回后正常退出
```

---

## 六、核心问题：Exit() 可能卡住，但有超时强杀兜底

### Exit() 内部没有超时机制

`Exit()` 调用链中有一步 `task_receiver_->Stop()`，它会等待当前正在执行的 task 完成。**如果 Actor 正在跑一个死循环或长时间阻塞的 task，这里就会卡住永远不退出。**

```
Exit()
  → RequestShutdown(force=false, timeout=无限)     // ← 无超时！
    → ExecuteExit()
      → task_manager_->DrainAndShutdown()
        → task_receiver_->Stop()                     // ← 可能永远阻塞
        → NotifyWorkerBlocked()                       // 只释放CPU资源，不解决卡住问题
```

### 两层超时兜底机制

#### 第 1 层：Raylet 超时（5 秒）— 最关键

```cpp
// src/ray/raylet/node_manager.cc:3399-3470
void NodeManager::HandleKillLocalActor(rpc::KillLocalActorRequest request, ...) {
    // 发 KillActor RPC 给 Worker
    worker->rpc_client()->KillActor(kill_actor_request, ...);

    // 启动 5s 安全定时器
    auto timer = execute_after(io_service_, [=]() {
        if (*replied) { return; }
        auto current_worker = worker_pool_.GetRegisteredWorker(worker_id);
        if (current_worker) {
            RAY_LOG(INFO) << "Worker did not exit after "
                          << RayConfig::instance().kill_worker_timeout_milliseconds()
                          << "ms, force killing with SIGKILL.";
            DestroyWorker(current_worker, ..., /*force=*/true);  // → SIGKILL
        }
        *replied = true;
        send_reply_callback(Status::OK(), nullptr, nullptr);
    }, std::chrono::milliseconds(
        RayConfig::instance().kill_worker_timeout_milliseconds()));  // 默认 5000ms
}
```

**这是最关键的兜底**。无论 Worker 内部 `Exit()` 卡在哪，5 秒后 Raylet 直接 SIGKILL 进程。

#### 第 2 层：GCS 超时（30 秒）— 仅 DestroyActor 路径

```cpp
// src/ray/gcs/actor/gcs_actor_manager.cc:988-1179
void GcsActorManager::DestroyActor(const ActorID &actor_id,
                                   const rpc::ActorDeathCause &death_cause,
                                   bool force_kill,
                                   std::function<void()> done_callback,
                                   int64_t graceful_shutdown_timeout_ms) {
    // ...
    if (!force_kill && graceful_shutdown_timeout_ms > 0 &&
        graceful_shutdown_timers_.find(worker_id) == graceful_shutdown_timers_.end()) {
        auto timer = std::make_unique<boost::asio::deadline_timer>(io_context_);
        timer->expires_from_now(
            boost::posix_time::milliseconds(graceful_shutdown_timeout_ms));

        timer->async_wait([=](const boost::system::error_code &error) {
            if (error == boost::asio::error::operation_aborted) { return; }
            RAY_LOG(WARNING) << "Graceful shutdown timeout exceeded. Falling back to force kill.";
            self->NotifyRayletToKillActor(actor, death_cause, /*force_kill=*/true);
        });
        graceful_shutdown_timers_[worker_id] = std::move(timer);
    }
}
```

**注意**：`ray.kill(no_restart=False)` 走的是 `KillActor()` 路径，**不设 GCS 层定时器**，只靠 Raylet 5 秒兜底。

### 配置项

```cpp
// src/ray/common/ray_config_def.h:273
RAY_CONFIG(int64_t, actor_graceful_shutdown_timeout_ms, 30000)  // GCS 层，默认 30s

// src/ray/common/ray_config_def.h:267
RAY_CONFIG(int64_t, kill_worker_timeout_milliseconds, 5000)    // Raylet 层，默认 5s
```

### 完整超时保障图

```
路径                          GCS 30s 定时器    Raylet 5s 定时器    最终保障
─────────────────────────────────────────────────────────────────────────
引用计数出作用域               ✅                ✅                 30s 内必死
ray.kill(no_restart=True)     ❌(直接force)      ✅(但已是force)    立即强杀
ray.kill(no_restart=False)    ❌                ✅                 5s 内必死
__ray_terminate__.remote()    ❌不走GCS          ❌不经过Raylet     ⚠️ 无兜底！
```

### 卡住场景的完整 fallback 链

```
Exit() 被调用但 actor 卡在长任务中
    │
    ├── CoreWorker 内部：无超时，task_receiver_->Stop() 可能永远阻塞
    │
    ├── [5s 后] Raylet 超时定时器触发
    │     └── DestroyWorker(worker, force=true) → SIGKILL → 进程死亡 ✅
    │
    └── [30s 后] GCS 优雅关闭定时器触发（仅 DestroyActor 路径）
          └── NotifyRayletToKillActor(force_kill=true)
                └── Raylet 再次发 KillActor(force_kill=true) + 5s 定时器
                      └── CoreWorker: ForceExit() → 立即退出
                            └── 若仍未退出 → Raylet 5s 后 SIGKILL ✅
```

---

## 七、三条 Actor 终止路径的对比

### 总览

```
┌─────────────────────────────────────────────────────────────────┐
│              Actor 终止的三条路径                                  │
├──────────────┬──────────────────┬───────────────────────────────┤
│ ray.kill()   │ __ray_terminate__ │ __del__ (引用计数出作用域)     │
├──────────────┼──────────────────┼───────────────────────────────┤
│ force_kill   │ 优雅排队退出       │ 优雅超时退出                   │
│ =True 硬编码 │ =False           │ =False                        │
├──────────────┼──────────────────┼───────────────────────────────┤
│ 立即强杀      │ 排队等待，跑完     │ 有 grace timeout              │
│ 不跑 atexit  │ 前面的 task 后退出 │ 超时后强杀                     │
│ 不跑清理代码  │ 跑 atexit        │ 超时前可清理                   │
├──────────────┼──────────────────┼───────────────────────────────┤
│ no_restart   │ 不涉及重启逻辑     │ DestroyActor(                │
│ 控制是否重启  │                   │  force_kill=False,            │
│              │                   │  graceful_timeout=30s)        │
├──────────────┼──────────────────┼───────────────────────────────┤
│ Raylet 5s    │ 无超时兜底 ⚠️     │ GCS 30s + Raylet 5s          │
│ 定时器兜底   │                   │ 双重兜底                      │
└──────────────┴──────────────────┴───────────────────────────────┘
```

### 路径 1：ray.kill() — 强制退出

```
ray.kill(actor, no_restart=True)  # 默认 no_restart=True
  → _raylet.pyx::kill_actor(actor_id, no_restart)     # force_kill 硬编码 True
    → C++ CoreWorker::KillActor(actor_id, force_kill=True, no_restart)
      → GCS AsyncKillActor RPC
        → HandleKillActorViaGcs()
          → no_restart=True: DestroyActor() → 永久死亡
          → no_restart=False: KillActor() → 可能重启
            → NotifyRayletToKillActor(force_kill=True)
              → Raylet HandleKillLocalActor()
                → KillActor RPC(force_kill=True) + 5s 安全定时器
                  → HandleKillActor(force_kill=True)
                    → ForceExit() → 进程立即退出
```

**特点**：`atexit` 不执行，`__del__` 不执行，正在运行的任务立即失败

### 路径 2：__ray_terminate__ — 优雅排队退出

```
actor.__ray_terminate__.remote()     # 像调用普通 actor 方法一样
  → _modify_class() 注入的默认方法
    → ray.actor.exit_actor()
      → worker.core_worker.set_current_actor_should_exit()  # 设置 C++ 标志
      → raise SystemExit (或 AsyncioActorExit)
        → Python 异常退出 → atexit 执行 → __del__ 执行
```

**关键**：
- `__ray_terminate__` 是作为普通 actor task 排队执行的，需等前面的 task 完成
- 只有用户显式调用才会触发，Ray 不会自动调用
- **这条路径没有超时兜底！** 如果前面的 task 卡住，terminate task 永远排不到

### 路径 3：引用计数出作用域 — 优雅超时退出

```
所有 ActorHandle 引用被回收 (Python __del__)
  → ActorHandle.__del__()
    → core_worker.remove_actor_handle_reference()
      → C++ reference counter → ReportActorOutOfScope to GCS
        → HandleReportActorOutOfScope()
          → DestroyActor(actor_id, force_kill=false, timeout=30s)
            → NotifyRayletToKillActor(force_kill=false)
            → 启动 30s 优雅关闭定时器
              → Raylet HandleKillLocalActor(force_kill=false)
                → KillActor RPC(force_kill=false) + 5s 定时器
                  → HandleKillActor(force_kill=false)
                    → Exit() → 优雅退出
                    → 若 5s 未退出 → SIGKILL
              → 30s 后若仍存活 → 重发 force_kill=true
```

**特点**：有 grace timeout 窗口（30s），在此期间 Actor 有机会执行清理逻辑，超时后强杀

---

## 八、`__del__` 和 `__ray_terminate__` 的触发机制

### `__del__` — Python GC 触发，被动

**不是 Ray 主动调用的，是 Python 进程退出时 GC 触发。**

当 Actor Worker 进程退出（无论哪种路径），Python 解释器做 GC 清理时会调用 actor 对象的 `__del__`。能否执行取决于进程**怎么退出**：

| 退出方式 | 进程退出行为 | `__del__` 是否执行 |
|---------|------------|-------------------|
| `Exit()`（graceful） | 走正常 Python 退出流程 | ✅ |
| `exit_actor()` 抛 SystemExit | Python 正常处理异常退出 | ✅ |
| `ForceExit()` / SIGKILL | 进程被强杀，解释器无机会 GC | ❌ |

### `__ray_terminate__` — 用户主动提交 task，主动

**只有用户显式调用才会触发，Ray 不会自动调用。**

```python
actor.__ray_terminate__.remote()  # 像调用普通 actor 方法一样
```

它被 `_modify_class()` 注入到 actor 类中，作为一个普通 actor task 排队执行。内部逻辑：

```python
# python/ray/actor.py:2398-2420
def __ray_terminate__(self):
    worker = ray._private.worker.global_worker
    if worker.mode != ray.LOCAL_MODE:
        ray.actor.exit_actor()
```

`exit_actor()` 实现：

```python
# python/ray/actor.py:2446-2487
def exit_actor():
    worker = ray._private.worker.global_worker
    if worker.mode == ray.WORKER_MODE and not worker.actor_id.is_nil():
        worker.core_worker.set_current_actor_should_exit()
        if worker.core_worker.current_actor_is_asyncio():
            raise AsyncioActorExit()
        raise_sys_exit_with_custom_error_message("exit_actor() is called.")
```

**关键区别**：引用计数出作用域（`ActorHandle.__del__` → GCS `DestroyActor(force_kill=False)`）走的是 gRPC `KillActorRequest(force_kill=False)` → `Exit()` 路径，**不会调用 `__ray_terminate__`**。

---

## 九、在 Actor 退出时做清理工作

### 三种方式对比

| 退出方式 | `__del__` | `atexit` | 自定义清理方法 |
|---------|-----------|----------|-------------|
| `__ray_terminate__` | ✅ | ✅ | ✅ |
| 引用计数出作用域 | ✅ | ❌(超时后) | ❌ |
| `ray.kill()` | ❌ | ❌ | ❌ |

### 方式 1：`__del__` 方法（最推荐）

```python
class MyActor:
    def __del__(self):
        self._cleanup()

    def _cleanup(self):
        # 关闭连接、释放资源等
        ...
```

**适用路径**：`__ray_terminate__`、引用计数出作用域（带 grace timeout）。`ray.kill()` 强杀时**不执行**。

### 方式 2：`atexit` 注册

```python
import atexit

class MyActor:
    def __init__(self):
        atexit.register(self._cleanup)
```

**适用路径**：仅 `__ray_terminate__` 退出时执行。`ray.kill()` 和引用计数超时强杀都**不执行**。

### 方式 3：自定义优雅关闭方法

```python
actor = MyActor.remote()
actor.cleanup.remote()       # 先清理
actor.__ray_terminate__.remote()  # 再优雅退出
```

### 最佳实践

把清理逻辑写在 `__del__` 中，退出时用 `__ray_terminate__.remote()` 而非 `ray.kill()`，这样两条优雅路径都能覆盖。如果必须用 `ray.kill()`，应先手动调清理方法再 kill。

---

## 十、ActorPool 中 Actor 无法退出的常见原因

### 原因 1：引用计数未归零

ActorPool 内部的 `_idle_actors` / `_future_to_actor` 仍持有 handle，GCS 不会触发 `ReportActorOutOfScope`。

### 原因 2：`__ray_terminate__` 排队排不到

如果前面有大量 pending task，terminate task 排不到，且**这条路径没有超时兜底**。

### 原因 3：Exit() 卡在 `task_receiver_->Stop()`

当前 task 卡住（如无限循环），`Exit()` 内部没有超时，要等 Raylet 5 秒超时来 SIGKILL。

### 原因 4：streaming_gen 引用链未断

Ray Data 场景下，`DataOpTask` 持有 `ObjectRefGenerator._generator_ref`（ObjectRef），只要 streaming task 未完成，这个引用链就不断，actor 就不会被 GC。详细分析参见 [ActorPool僵尸Actor排查文档](../06-故障排查/ActorPool僵尸Actor不释放排查.md)。

### 建议

对 ActorPool 中的 Actor，用 `ray.kill()` 而非 `__ray_terminate__`，因为有超时兜底保证退出；如果需要清理，可以先调清理方法再 `ray.kill()`。

---

## 十一、GCS Actor Manager 的 Kill vs Destroy 区别

### HandleKillActorViaGcs — RPC 入口

```cpp
// src/ray/gcs/actor/gcs_actor_manager.cc:635-667
void GcsActorManager::HandleKillActorViaGcs(rpc::KillActorViaGcsRequest request, ...) {
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
    }
}
```

**关键分支**：`no_restart=True` 走 `DestroyActor`（永久死），`no_restart=False` 走 `KillActor`（可能重启）。

### KillActor() — 不设 GCS 层定时器

```cpp
// src/ray/gcs/actor/gcs_actor_manager.cc:1865-1903
void GcsActorManager::KillActor(const ActorID &actor_id, bool force_kill) {
    auto it = registered_actors_.find(actor_id);
    if (it == registered_actors_.end()) { return; }
    const auto &actor = it->second;

    if (actor->GetState() == rpc::ActorTableData::DEAD ||
        actor->GetState() == rpc::ActorTableData::DEPENDENCIES_UNREADY) {
        return;
    }

    auto node_it = created_actors_.find(node_id);
    if (node_it != created_actors_.end() && node_it->second.count(worker_id)) {
        NotifyRayletToKillActor(actor, GenKilledByApplicationCause(...), force_kill);
    } else {
        // Actor 还没创建，取消调度并重启
        NotifyRayletToKillActor(actor, GenKilledByApplicationCause(...), force_kill);
        CancelActorInScheduling(actor);
        RestartActor(actor_id, /*need_reschedule=*/true, ...);
    }
}
```

**关键**：`KillActor` **不设 GCS 层优雅关闭定时器**，直接发 `NotifyRayletToKillActor`，靠 Raylet 5s 定时器兜底。

### DestroyActor() — 设 GCS 层优雅关闭定时器

```cpp
// src/ray/gcs/actor/gcs_actor_manager.cc:988-1179
void GcsActorManager::DestroyActor(const ActorID &actor_id,
                                   const rpc::ActorDeathCause &death_cause,
                                   bool force_kill = true,
                                   std::function<void()> done_callback = nullptr,
                                   int64_t graceful_shutdown_timeout_ms = -1) {
    // force_kill=true 时取消现有定时器
    if (force_kill) {
        auto timer_it = graceful_shutdown_timers_.find(actor->GetWorkerID());
        if (timer_it != graceful_shutdown_timers_.end()) {
            timer_it->second->cancel();
        }
    }

    // 发送 kill 请求到 Raylet
    NotifyRayletToKillActor(actor, death_cause, force_kill);

    // 非 force_kill 且有 timeout → 设置优雅关闭定时器
    if (!force_kill && graceful_shutdown_timeout_ms > 0 && ...) {
        auto timer = ...;
        timer->expires_from_now(milliseconds(graceful_shutdown_timeout_ms));
        timer->async_wait([=](error) {
            if (error == operation_aborted) { return; }
            // 超时后重发 force_kill=true
            NotifyRayletToKillActor(actor, death_cause, /*force_kill=*/true);
        });
        graceful_shutdown_timers_[worker_id] = std::move(timer);
    }

    // 更新 actor 状态为 DEAD，持久化，发布通知等
    // ...
}
```

### 对比总结

| 方法 | GCS 层定时器 | 用途 | 结果 |
|------|-------------|------|------|
| `KillActor()` | ❌ 不设 | `no_restart=False` | Actor 被杀后可能重启 |
| `DestroyActor(force_kill=True)` | ❌ 不需要 | `no_restart=True` / `ray.kill()` | 立即强杀，永久死亡 |
| `DestroyActor(force_kill=False)` | ✅ 设 30s | 引用计数出作用域 | 先优雅退出，超时后强杀 |

---

## 十二、Raylet 层 Worker::KillAsync() — SIGTERM → SIGKILL

```cpp
// src/ray/raylet/worker.cc:63-100
void Worker::KillAsync(instrumented_io_context &io_service, bool force) {
    bool expected = false;
    if (!killing_.compare_exchange_strong(expected, true)) {
        return;  // Already being killed
    }
    if (force) {
        worker->GetProcess().Kill();  // SIGKILL immediately
        return;
    }
    kill(worker->GetProcess().GetId(), SIGTERM);  // Graceful first
    auto retry_timer = std::make_shared<boost::asio::deadline_timer>(io_service);
    auto timeout = RayConfig::instance().kill_worker_timeout_milliseconds();
    retry_timer->expires_from_now(boost::posix_time::milliseconds(timeout));
    retry_timer->async_wait([=](const boost::system::error_code &error) {
        if (worker->GetProcess().IsAlive()) {
            RAY_LOG(INFO) << "Worker did not exit, force killing with SIGKILL.";
            worker->GetProcess().Kill();  // SIGKILL fallback
        }
    });
}
```

- `force=true` → 直接 SIGKILL
- `force=false` → 先 SIGTERM，等 `kill_worker_timeout_milliseconds`（5s），超时后 SIGKILL

---

## 十三、关键源码位置索引

| 组件 | 文件 | 行号 | 函数 |
|------|------|------|------|
| Python 入口 | `python/ray/_private/worker.py` | 3270-3300 | `kill()` |
| Cython 桥接 | `python/ray/_raylet.pyx` | 3846-3857 | `kill_actor()` |
| C++ CoreWorker | `src/ray/core_worker/core_worker.cc` | 2749-2806 | `KillActor()` |
| C++ CoreWorker | `src/ray/core_worker/core_worker.cc` | 4352-4385 | `HandleKillActor()` |
| C++ CoreWorker | `src/ray/core_worker/core_worker.cc` | 658-693 | `Exit()` / `ForceExit()` |
| C++ CoreWorker | `src/ray/core_worker/core_worker.cc` | 2908-2935 | `RunTaskExecutionLoop()` |
| Shutdown 协调器 | `src/ray/core_worker/shutdown_coordinator.cc` | 39-88 | `RequestShutdown()` |
| Shutdown 执行器 | `src/ray/core_worker/core_worker_shutdown_executor.cc` | 118-246 | `ExecuteExit()` |
| Shutdown 执行器 | `src/ray/core_worker/core_worker_shutdown_executor.cc` | 51-115 | `ExecuteGracefulShutdown()` |
| Worker Context | `src/ray/core_worker/context.cc` | 416-426 | `SetCurrentActorShouldExit()` |
| GCS Actor Manager | `src/ray/gcs/actor/gcs_actor_manager.cc` | 635-667 | `HandleKillActorViaGcs()` |
| GCS Actor Manager | `src/ray/gcs/actor/gcs_actor_manager.cc` | 988-1179 | `DestroyActor()` |
| GCS Actor Manager | `src/ray/gcs/actor/gcs_actor_manager.cc` | 1865-1903 | `KillActor()` |
| GCS Actor Manager | `src/ray/gcs/actor/gcs_actor_manager.cc` | 1831-1863 | `NotifyRayletToKillActor()` |
| GCS Actor Manager | `src/ray/gcs/actor/gcs_actor_manager.cc` | 276-300 | `HandleReportActorOutOfScope()` |
| GCS RPC Client | `src/ray/gcs_rpc_client/accessors/actor_info_accessor.cc` | 207-225 | `AsyncKillActor()` |
| Raylet | `src/ray/raylet/node_manager.cc` | 3399-3470 | `HandleKillLocalActor()` |
| Raylet Worker | `src/ray/raylet/worker.cc` | 63-100 | `KillAsync()` |
| Actor Handle | `python/ray/actor.py` | 2061-2074 | `ActorHandle.__del__()` |
| Actor 退出 | `python/ray/actor.py` | 2398-2420 | `__ray_terminate__()` |
| Actor 退出 | `python/ray/actor.py` | 2446-2487 | `exit_actor()` |
| Cython 标志 | `python/ray/_raylet.pyx` | 4522-4529 | `set_current_actor_should_exit()` |
| Cython 主循环 | `python/ray/_raylet.pyx` | 2834-2836 | `run_task_loop()` |
| 超时配置 | `src/ray/common/ray_config_def.h` | 267-273 | 两个超时配置 |
| Proto | `src/ray/protobuf/gcs_service.proto` | 155-163 | `KillActorViaGcsRequest` |
| Proto | `src/ray/protobuf/core_worker.proto` | 276-285 | `KillActorRequest` |
| Proto | `src/ray/protobuf/node_manager.proto` | 417-426 | `KillLocalActorRequest` |
