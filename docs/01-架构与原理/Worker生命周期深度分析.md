# Ray Worker 进程完整生命周期深度分析

本文从代码层面详细分析 Ray 中 Raylet 如何启动 Worker 进程、如何在 Worker 进程中运行 Python 代码、Worker 完整的生命周期管理、资源管控机制（包括 cgroupv2）、Worker 预创建策略以及跨语言复用机制。

## 整体架构概览

### Worker 在 Ray 架构中的位置

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                              Ray 集群                                        │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │                          Head Node                                      │ │
│  │  ┌─────────┐  ┌─────────┐  ┌──────────┐  ┌──────────────────────────┐ │ │
│  │  │   GCS   │  │Dashboard│  │Autoscaler│  │       Raylet             │ │ │
│  │  │ Server  │  │         │  │          │  │  ┌─────────────────────┐ │ │ │
│  │  └────┬────┘  └─────────┘  └──────────┘  │  │   WorkerPool        │ │ │ │
│  │       │                                    │  │  ┌───┐┌───┐┌───┐  │ │ │ │
│  │       │                                    │  │  │W1 ││W2 ││W3 │  │ │ │ │
│  │       │                                    │  │  └───┘└───┘└───┘  │ │ │ │
│  │       │                                    │  └─────────────────────┘ │ │ │
│  │       │                                    └──────────────────────────┘ │ │
│  └───────┼────────────────────────────────────────────────────────────────┘ │
│          │                                                                   │
│  ┌───────┼────────────────────────────────────────────────────────────────┐ │
│  │       │                      Worker Node                                │ │
│  │  ┌────▼────┐                                                            │ │
│  │  │  GCS    │  ┌──────────────────────────────────────────────────────┐ │ │
│  │  │ Client  │  │                    Raylet                            │ │ │
│  │  └─────────┘  │                                                      │ │ │
│  │               │  ┌────────────────────────────────────────────────┐  │ │ │
│  │               │  │              WorkerPool                        │  │ │ │
│  │               │  │                                                │  │ │ │
│  │               │  │  ┌──────────┐  ┌──────────┐  ┌──────────┐    │  │ │ │
│  │               │  │  │ Worker 1 │  │ Worker 2 │  │ Worker N │    │  │ │ │
│  │               │  │  │(Python)  │  │(Python)  │  │(Java)    │    │  │ │ │
│  │               │  │  │CoreWorker│  │CoreWorker│  │CoreWorker│    │  │ │ │
│  │               │  │  └──────────┘  └──────────┘  └──────────┘    │  │ │ │
│  │               │  │                                                │  │ │ │
│  │               │  │  ┌──────────┐  ┌──────────┐                   │  │ │ │
│  │               │  │  │ Spill IO │  │Restore IO│  (IO Workers)    │  │ │ │
│  │               │  │  │ Worker   │  │ Worker   │                   │  │ │ │
│  │               │  │  └──────────┘  └──────────┘                   │  │ │ │
│  │               │  └────────────────────────────────────────────────┘  │ │ │
│  │               │                                                      │ │ │
│  │               │  ┌────────────────┐  ┌──────────────────────────┐  │ │ │
│  │               │  │LocalResourceMgr│  │  LocalLeaseManager      │  │ │ │
│  │               │  │(逻辑资源记账)   │  │  (Lease 分配/回收)       │  │ │ │
│  │               │  └────────────────┘  └──────────────────────────┘  │ │ │
│  │               └──────────────────────────────────────────────────────┘ │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────────────────┘
```

### Worker 核心设计理念

Ray 的 Worker 进程管理体现了以下关键设计理念：

1. **Worker 是通用容器**：Worker 进程启动时不知道要执行什么函数。函数通过 `FunctionDescriptor` 在 Task 到达时动态查找加载（本地 import 或从 GCS 下载 pickle 后反序列化）。这使得同一个 Worker 可以串行执行不同的 Task。

2. **资源管理是逻辑记账而非物理隔离**：Ray 的资源调度（`num_cpus=2, num_gpus=1`）本质上是调度准入控制。调度器保证不会超额分配，但 OS 层面没有强制隔离——声明 `num_cpus=1` 的 Task 实际上可以使用所有 CPU 核心。cgroupv2 仅做 system/user 粗粒度划分，没有 per-task 隔离。

3. **Worker 复用优先**：通过 `FindAndPopIdleWorker()` 优先匹配最近活跃的 Worker（LIFO 策略），避免冷启动开销。匹配条件不包含资源需求量，因为资源在调度层面管理，不在 Worker 层面隔离。

4. **跨语言不可复用**：Python Worker 是 Python 解释器进程，Java Worker 是 JVM 进程，C++ Worker 是原生二进制。每种语言独立的 `State` 和独立的启动命令模板。

5. **优雅退出与强制退出分层**：空闲回收用 Exit RPC（Worker 可拒绝，如持有 object 引用时），OOM/Owner 死亡/强制取消用 SIGTERM→超时→SIGKILL 的两阶段杀死。

### Worker 完整生命周期一览

```
用户提交 Task/Actor
       │
       ▼
GCS/调度器 选择目标节点
       │
       ▼
RequestWorkerLease ──→ Raylet (NodeManager)
       │
       ├─── PrestartWorkers()     ← 预测性启动更多 Worker
       │
       ▼
WorkerPool::PopWorker()
       │
       ├─── FindAndPopIdleWorker()  ← 优先复用空闲 Worker (LIFO)
       │         │
       │         ├── 找到匹配 → 直接返回 Worker
       │         │
       │         └── 未找到 ──→ StartNewWorker()
       │                           │
       │                           ├── GetOrCreateRuntimeEnv()  (如有)
       │                           │
       │                           └── StartWorkerProcess()
       │                                 │
       │                                 ├── BuildProcessCommandArgs()
       │                                 │     构建: python default_worker.py <args>
       │                                 │
       │                                 ├── fork() + execvpe()
       │                                 │     子进程: add_to_cgroup → setpgrp → exec
       │                                 │
       │                                 ├── AdjustWorkerOomScore()
       │                                 │
       │                                 └── MonitorStartingWorkerProcess()
       │                                       (注册超时监控)
       │
       ▼
Worker 进程启动
  │  python default_worker.py
  │    → connect(raylet, gcs, object_store)
  │    → CoreWorker 初始化
  │    → RegisterClient → AnnounceWorkerPort
  │    → main_loop() → run_task_loop()
  │
  ▼
IDLE (空闲池)  ◄─────────────────────────────────────┐
  │                                                    │
  │  PopWorker() 匹配成功                               │
  ▼                                                    │
LEASED/BUSY                                            │
  │  Grant() → PushTask gRPC → 执行用户代码             │
  │                                                    │
  │  ├── ray.get 阻塞 → NotifyWorkerBlocked            │
  │  │                   (临时释放 CPU 资源)             │
  │  │   ray.get 完成 → NotifyWorkerUnblocked           │
  │  │                   (恢复 CPU 资源)                │
  │  │                                                  │
  │  Task 完成                                          │
  │  ReturnWorkerLease → ReleaseWorkerResources ────────┘
  │
  │  [Actor 模式: 永不回到 idle, 生命期 = Actor 生命期]
  │
  ▼
退出 (11种触发路径)
  ├── 空闲超时回收 (Exit RPC)
  ├── Job 结束 (Exit RPC force)
  ├── OOM (SIGKILL)
  ├── Owner 死亡 (SIGTERM→SIGKILL)
  ├── PG 移除 (DestroyWorker)
  ├── GCS 释放 Actor (DestroyWorker)
  ├── ray.kill(actor) (KillActor RPC→超时SIGKILL)
  ├── ray.cancel(force=True) (CancelTask RPC→超时SIGKILL)
  ├── Worker 异常断连 (DestroyWorker)
  ├── Worker 自主退出 (max_calls/信号/sys.exit)
  └── Worker 启动超时 (SIGKILL)
```

## 目录

- [一、Raylet 启动 Worker 进程](#一raylet-启动-worker-进程)
  - [1.1 触发时机：PopWorker](#11-触发时机popworker)
  - [1.2 匹配空闲 Worker：FindAndPopIdleWorker](#12-匹配空闲-workerfindandpopidleworker)
  - [1.3 Worker 匹配条件：WorkerFitForLease](#13-worker-匹配条件workerfitforlease)
  - [1.4 启动新进程：StartWorkerProcess](#14-启动新进程startworkerprocess)
  - [1.5 构建命令行：BuildProcessCommandArgs](#15-构建命令行buildprocesscommandargs)
  - [1.6 实际 Fork/Exec：StartProcess 与 spawnvpe](#16-实际-forkexecstartprocess-与-spawnvpe)
- [二、Worker 如何运行 Python 代码](#二worker-如何运行-python-代码)
  - [2.1 Python Worker 启动链](#21-python-worker-启动链)
  - [2.2 default_worker.py 入口逻辑](#22-default_workerpy-入口逻辑)
  - [2.3 连接 Raylet 与 CoreWorker 初始化](#23-连接-raylet-与-coreworker-初始化)
  - [2.4 主事件循环：run_task_loop](#24-主事件循环run_task_loop)
  - [2.5 Task 接收与执行链路](#25-task-接收与执行链路)
  - [2.6 函数的序列化与动态加载机制](#26-函数的序列化与动态加载机制)
- [三、Worker 完整生命周期](#三worker-完整生命周期)
  - [3.1 Worker 状态定义](#31-worker-状态定义)
  - [3.2 完整生命周期状态机](#32-完整生命周期状态机)
  - [3.3 空闲 Worker 管理：PushWorker](#33-空闲-worker-管理pushworker)
  - [3.4 空闲 Worker 回收：TryKillingIdleWorkers](#34-空闲-worker-回收trykillingidleworkers)
  - [3.5 Worker 断连与清理：DisconnectClient](#35-worker-断连与清理disconnectclient)
  - [3.6 Worker 杀死：KillAsync](#36-worker-杀死killasync)
- [四、资源管控机制](#四资源管控机制)
  - [4.1 逻辑资源管理（主要机制）](#41-逻辑资源管理主要机制)
  - [4.2 资源分配/回收完整流程](#42-资源分配回收完整流程)
  - [4.3 cgroupv2 实现（可选机制）](#43-cgroupv2-实现可选机制)
  - [4.4 cgroup 层次结构与约束](#44-cgroup-层次结构与约束)
  - [4.5 cgroup 对 Worker 的具体操作](#45-cgroup-对-worker-的具体操作)
  - [4.6 Worker 复用时资源不一致的处理](#46-worker-复用时资源不一致的处理)
  - [4.7 GPU 隔离：环境变量方式](#47-gpu-隔离环境变量方式)
- [五、Worker 预创建与适配](#五worker-预创建与适配)
  - [5.1 启动时预创建](#51-启动时预创建)
  - [5.2 请求驱动预创建](#52-请求驱动预创建)
  - [5.3 空闲 Worker 的保活策略](#53-空闲-worker-的保活策略)
- [六、跨语言 Worker 复用](#六跨语言-worker-复用)
  - [6.1 不同语言之间无法复用](#61-不同语言之间无法复用)
  - [6.2 每种语言独立的 Worker Pool State](#62-每种语言独立的-worker-pool-state)
- [七、Worker 进程退出的所有触发时机](#七worker-进程退出的所有触发时机)
  - [7.1 退出路径概览](#71-退出路径概览)
  - [7.2 路径一：空闲超时回收（TryKillingIdleWorkers）](#72-路径一空闲超时回收trykillingidleworkers)
  - [7.3 路径二：Job 结束强制退出（HandleJobFinished）](#73-路径二job-结束强制退出handlejobjobfinished)
  - [7.4 路径三：OOM 内存不足被杀（Memory Monitor）](#74-路径三oom-内存不足被杀memory-monitor)
  - [7.5 路径四：Owner 死亡（节点/Worker 失败）](#75-路径四owner-死亡节点worker-失败)
  - [7.6 路径五：Placement Group 移除](#76-路径五placement-group-移除)
  - [7.7 路径六：GCS 请求释放未使用的 Actor Worker](#77-路径六gcs-请求释放未使用的-actor-worker)
  - [7.8 路径七：GCS 请求杀死 Actor（KillActor）](#78-路径七gcs-请求杀死-actorkillactor)
  - [7.9 路径八：ray.cancel(force=True) 强制取消](#79-路径八raycancelforcerue-强制取消)
  - [7.10 路径九：Worker 异常断连](#710-路径九worker-异常断连)
  - [7.11 路径十：Worker 自主退出（max_calls / 信号 / sys.exit）](#711-路径十worker-自主退出max_calls--信号--sysexit)
  - [7.12 路径十一：Worker 启动超时](#712-路径十一worker-启动超时)
  - [7.13 Worker 端 Exit RPC 处理（HandleExit）](#713-worker-端-exit-rpc-处理handleexit)
  - [7.14 退出路径总结表](#714-退出路径总结表)
- [八、核心数据结构汇总](#八核心数据结构汇总)
- [九、关键文件索引](#九关键文件索引)
- [十、架构设计总结与核心洞察](#十架构设计总结与核心洞察)

---

## 一、Raylet 启动 Worker 进程

Worker 进程的启动由调度器的 lease 请求驱动。当一个 Task 或 Actor 需要执行时，调度器选择目标节点并发送 `RequestWorkerLease`，Raylet 的 `WorkerPool` 负责提供匹配的 Worker。核心策略是**先复用后创建**：优先从空闲池找匹配 Worker，找不到才启动新进程。

### 启动流程总览

```
RequestWorkerLease (gRPC)
  │
  ▼
NodeManager::HandleRequestWorkerLease()
  │
  ├── worker_pool_.PrestartWorkers()         ← 根据 backlog 预测性启动更多 Worker
  │
  └── cluster_lease_manager_.QueueAndScheduleLease()
        │
        ▼
      LocalLeaseManager::GrantScheduledLeasesToWorkers()
        │
        ▼
      WorkerPool::PopWorker(lease_spec, callback)
        │
        ├── [1] FindAndPopIdleWorker()
        │     │
        │     ├── 遍历 idle_of_all_languages_ (从后往前, LIFO)
        │     │   检查 10 个匹配条件 (WorkerFitForLease)
        │     │
        │     ├── 匹配成功 → 从 idle 池移除 → callback 返回 Worker
        │     │
        │     └── 全部不匹配 → 进入 [2]
        │
        └── [2] StartNewWorker(pop_worker_request)
              │
              ├── IsRuntimeEnvEmpty?
              │   ├── YES → StartWorkerProcess() 直接启动
              │   └── NO  → GetOrCreateRuntimeEnv() → 成功后 StartWorkerProcess()
              │
              └── StartWorkerProcess()
                    │
                    ├── 并发控制检查 (starting_workers < maximum_startup_concurrency_)
                    ├── WorkerID::FromRandom()                    生成唯一 ID
                    ├── BuildProcessCommandArgs()                 构建命令行和环境变量
                    ├── StartProcess() → fork() + execvpe()       创建 OS 进程
                    ├── AdjustWorkerOomScore()                    调整 OOM score
                    ├── MonitorStartingWorkerProcess()            设置注册超时监控
                    └── AddWorkerProcess()                        记录进程信息
```

### Worker 匹配条件一览（WorkerFitForLease 10 项检查）

| 序号 | 检查项 | 说明 | 不匹配原因枚举 |
|------|--------|------|---------------|
| 1 | 死亡检查 | `worker.IsDead()` | OTHERS |
| 2 | 正在退出 | `pending_exit_idle_workers_` 中 | OTHERS |
| 3 | 语言匹配 | Python/Java/C++ 必须一致 | OTHERS |
| 4 | Worker 类型 | WORKER/SPILL_WORKER/RESTORE_WORKER | OTHERS |
| 5 | Root Detached Actor ID | 必须匹配（如果双方都非空） | ROOT_MISMATCH |
| 6 | Job ID | Worker 未分配 Job 或与请求一致 | ROOT_MISMATCH |
| 7 | GPU 标记 | `is_gpu_` 匹配或任一为空 | OTHERS |
| 8 | Actor Worker 标记 | `is_actor_worker_` 匹配或任一为空 | OTHERS |
| 9 | Runtime Env Hash | 必须精确匹配 | RUNTIME_ENV_MISMATCH |
| 10 | Dynamic Options | 必须精确匹配（如 JVM 参数） | DYNAMIC_OPTIONS_MISMATCH |

**关键设计点：资源需求（num_cpus、num_gpus 等）不是匹配条件。** 任何空闲 Worker 都可以执行任何资源量的 Task，因为资源是在调度层面逻辑分配的，不在 Worker 进程层面隔离。

### 1.1 触发时机：PopWorker

当调度器需要一个 Worker 来执行 Task/Actor 时，核心调用入口是 `WorkerPool::PopWorker()`。

**文件**: `src/ray/raylet/worker_pool.cc:1478`

```cpp
void WorkerPool::PopWorker(std::shared_ptr<PopWorkerRequest> pop_worker_request) {
  // 先尝试从空闲池中找到一个匹配的 Worker
  auto worker = FindAndPopIdleWorker(*pop_worker_request);
  if (worker == nullptr) {
    // 没有合适的空闲 Worker，启动新的
    StartNewWorker(pop_worker_request);
    return;
  }
  // 找到匹配的空闲 Worker，通过回调返回给调用方
  PopWorkerCallbackAsync(pop_worker_request->callback_, worker, PopWorkerStatus::OK);
}
```

调用链路为：

```
RequestWorkerLease (gRPC 到达 NodeManager)
  → NodeManager::HandleRequestWorkerLease()    // node_manager.cc:1781
    → worker_pool_.PrestartWorkers()            // 预测性地启动更多 Worker
    → cluster_lease_manager_.QueueAndScheduleLease()
      → LocalLeaseManager::GrantScheduledLeasesToWorkers()  // local_lease_manager.cc:136
        → worker_pool_.PopWorker(spec, callback)            // 获取一个可用 Worker
```

### 1.2 匹配空闲 Worker：FindAndPopIdleWorker

**文件**: `src/ray/raylet/worker_pool.cc:1430`

```cpp
std::shared_ptr<WorkerInterface> WorkerPool::FindAndPopIdleWorker(
    const PopWorkerRequest &pop_worker_request) {
  absl::flat_hash_map<WorkerUnfitForLeaseReason, size_t> skip_reason_count;

  auto worker_fit_for_lease_fn = [this, &pop_worker_request, &skip_reason_count](
                                     const IdleWorkerEntry &entry) -> bool {
    WorkerUnfitForLeaseReason reason =
        WorkerFitForLease(*entry.worker, pop_worker_request);
    if (reason == WorkerUnfitForLeaseReason::NONE) {
      return true;
    }
    skip_reason_count[reason]++;
    // ... 记录 metrics
    return false;
  };

  auto &state = GetStateForLanguage(pop_worker_request.language_);
  // 从 idle_of_all_languages_ 列表中反向查找（后进先出，优先复用最近活跃的 Worker）
  auto worker_it = std::find_if(idle_of_all_languages_.rbegin(),
                                idle_of_all_languages_.rend(),
                                worker_fit_for_lease_fn);
  if (worker_it == idle_of_all_languages_.rend()) {
    return nullptr;  // 没找到匹配的
  }

  // 从 idle 集合和 idle_of_all_languages_ 列表中移除
  state.idle.erase(worker_it->worker);
  auto lit = worker_it.base();
  lit--;
  std::shared_ptr<WorkerInterface> worker = std::move(lit->worker);
  idle_of_all_languages_.erase(lit);
  return worker;
}
```

核心要点：
- 查找方向是**从后往前**（`rbegin`），即优先匹配最近变为空闲的 Worker（最热的进程）
- 查找范围是 `idle_of_all_languages_` 这个跨语言列表，但匹配条件中会检查语言

### 1.3 Worker 匹配条件：WorkerFitForLease

**文件**: `src/ray/raylet/worker_pool.cc:1267`

```cpp
WorkerUnfitForLeaseReason WorkerPool::WorkerFitForLease(
    const WorkerInterface &worker, const PopWorkerRequest &pop_worker_request) const {
  // 1. 死亡检查
  if (worker.IsDead()) {
    return WorkerUnfitForLeaseReason::OTHERS;
  }
  // 2. 正在退出检查
  if (pending_exit_idle_workers_.contains(worker.WorkerId())) {
    return WorkerUnfitForLeaseReason::OTHERS;
  }
  // 3. 语言必须匹配（Python Worker 不能执行 Java Task）
  if (worker.GetLanguage() != pop_worker_request.language_) {
    return WorkerUnfitForLeaseReason::OTHERS;
  }
  // 4. Worker 类型必须匹配（WORKER / SPILL_WORKER / RESTORE_WORKER）
  if (worker.GetWorkerType() != pop_worker_request.worker_type_) {
    return WorkerUnfitForLeaseReason::OTHERS;
  }
  // 5. Root Detached Actor ID 匹配
  if (!pop_worker_request.root_detached_actor_id_.IsNil() &&
      !worker.GetRootDetachedActorId().IsNil() &&
      pop_worker_request.root_detached_actor_id_ != worker.GetRootDetachedActorId()) {
    return WorkerUnfitForLeaseReason::ROOT_MISMATCH;
  }
  // 6. Job ID 匹配（未分配 Job 的 Worker 可以被任何 Job 使用）
  const auto worker_job_id = worker.GetAssignedJobId();
  if (!worker_job_id.IsNil() && pop_worker_request.job_id_ != worker_job_id) {
    return WorkerUnfitForLeaseReason::ROOT_MISMATCH;
  }
  // 7. GPU 标记匹配
  if (!OptionalsMatchOrEitherEmpty(pop_worker_request.is_gpu_, worker.GetIsGpu())) {
    return WorkerUnfitForLeaseReason::OTHERS;
  }
  // 8. Actor Worker 标记匹配
  if (!OptionalsMatchOrEitherEmpty(pop_worker_request.is_actor_worker_,
                                   worker.GetIsActorWorker())) {
    return WorkerUnfitForLeaseReason::OTHERS;
  }
  // 9. Runtime Env Hash 必须精确匹配
  if (worker.GetRuntimeEnvHash() != pop_worker_request.runtime_env_hash_) {
    return WorkerUnfitForLeaseReason::RUNTIME_ENV_MISMATCH;
  }
  // 10. Dynamic Options 必须精确匹配（如 JVM 参数）
  if (LookupWorkerDynamicOptions(worker.WorkerId()) !=
      pop_worker_request.dynamic_options_) {
    return WorkerUnfitForLeaseReason::DYNAMIC_OPTIONS_MISMATCH;
  }
  return WorkerUnfitForLeaseReason::NONE;
}
```

**关键点：资源需求（num_cpus、num_gpus 等）不是匹配条件。** 任何空闲 Worker 都可以执行任何资源量的 Task，因为资源是在调度层面逻辑分配的，不在 Worker 进程层面隔离。

### 1.4 启动新进程：StartWorkerProcess

**文件**: `src/ray/raylet/worker_pool.cc:456`

```cpp
std::tuple<Process, WorkerID> WorkerPool::StartWorkerProcess(
    const Language &language,
    const rpc::WorkerType worker_type,
    const JobID &job_id,
    PopWorkerStatus *status,
    const std::vector<std::string> &dynamic_options,
    const int runtime_env_hash,
    const std::string &serialized_runtime_env_context,
    const rpc::RuntimeEnvInfo &runtime_env_info,
    std::optional<absl::Duration> worker_startup_keep_alive_duration) {
  // ...

  auto &state = GetStateForLanguage(language);

  // 并发控制：统计当前正在启动的同类型 Worker 数量
  int starting_workers = 0;
  for (auto &entry : state.worker_processes) {
    if (entry.second.worker_type == worker_type) {
      starting_workers += entry.second.is_pending_registration ? 1 : 0;
    }
  }
  // 如果超过并发上限，排队等待
  if (starting_workers >= maximum_startup_concurrency_) {
    *status = PopWorkerStatus::TooManyStartingWorkerProcesses;
    return {Process(), WorkerID::Nil()};
  }

  // 生成唯一 WorkerID（Worker 注册时要用这个 ID 来关联）
  WorkerID worker_id = WorkerID::FromRandom();

  // 构建命令行参数和环境变量
  auto [worker_command_args, env] =
      BuildProcessCommandArgs(language, job_config, worker_type, job_id,
                              worker_id, dynamic_options, runtime_env_hash,
                              serialized_runtime_env_context, state);

  // 实际 fork+exec 创建进程
  Process proc = StartProcess(worker_command_args, env, worker_id);

  // 调整 OOM score（Linux 上让 Worker 比 Raylet 更容易被 OOM killer 杀死）
  if (!IsIOWorkerType(worker_type)) {
    AdjustWorkerOomScore(proc.GetId());
  }

  // 设置注册超时监控
  MonitorStartingWorkerProcess(worker_id, language, worker_type);

  // 将进程信息记录到 worker_processes 映射中
  AddWorkerProcess(state, worker_id, worker_type, proc, start,
                   runtime_env_info, dynamic_options,
                   worker_startup_keep_alive_duration);

  *status = PopWorkerStatus::OK;
  return {proc, worker_id};
}
```

### 1.5 构建命令行：BuildProcessCommandArgs

**文件**: `src/ray/raylet/worker_pool.cc:259`

Worker 的命令模板在 Raylet 启动时由 Python 端 `python/ray/_private/services.py:1543-1770` 构建传入。

Python Worker 的命令模板形如：

```bash
python setup_worker.py <flags> default_worker.py \
  --node-ip-address=<ip> \
  --node-manager-port=RAY_NODE_MANAGER_PORT_PLACEHOLDER \
  --object-store-name=<socket> \
  --raylet-name=<socket> \
  --gcs-address=<addr> \
  --session-name=<name> \
  RAY_WORKER_DYNAMIC_OPTION_PLACEHOLDER
```

`BuildProcessCommandArgs` 的核心处理逻辑：

```cpp
std::pair<std::vector<std::string>, ProcessEnvironment>
WorkerPool::BuildProcessCommandArgs(...) const {
  std::vector<std::string> options;

  // Java/C++ 的 code_search_path
  if (language == Language::JAVA || language == Language::CPP) {
    // ... 构建 code_search_path
  }

  // 用户自定义 per-process options
  options.insert(options.end(), dynamic_options.begin(), dynamic_options.end());

  // 遍历模板命令，替换占位符
  std::vector<std::string> worker_command_args;
  for (const auto &token : state.worker_command) {
    // 替换 RAY_WORKER_DYNAMIC_OPTION_PLACEHOLDER → 实际的 dynamic_options
    if (token == kWorkerDynamicOptionPlaceholder) {
      worker_command_args.insert(worker_command_args.end(),
                                 options.begin(), options.end());
      continue;
    }
    // 替换 RAY_NODE_MANAGER_PORT_PLACEHOLDER → 实际端口号
    auto node_manager_port_position = token.find(kNodeManagerPortPlaceholder);
    if (node_manager_port_position != std::string::npos) {
      auto replaced_token = token;
      replaced_token.replace(node_manager_port_position,
                             strlen(kNodeManagerPortPlaceholder),
                             std::to_string(node_manager_port_));
      worker_command_args.push_back(std::move(replaced_token));
      continue;
    }
    worker_command_args.push_back(token);
  }

  // Python 特有参数
  if (language == Language::PYTHON) {
    worker_command_args.push_back("--worker-id=" + worker_id.Hex());
    worker_command_args.push_back("--worker-launch-time-ms=" +
                                  std::to_string(current_sys_time_ms()));
    worker_command_args.push_back("--node-id=" + node_id_.Hex());
    worker_command_args.push_back("--runtime-env-hash=" +
                                  std::to_string(runtime_env_hash));
  }

  // 关键逻辑：是否保留 setup_worker.py
  if (serialized_runtime_env_context != "{}" &&
      !serialized_runtime_env_context.empty()) {
    // 有 runtime env → 保留 setup_worker.py 作为引导
    worker_command_args.push_back("--language=" + Language_Name(language));
    worker_command_args.push_back("--serialized-runtime-env-context=" +
                                  serialized_runtime_env_context);
  } else if (language == Language::PYTHON &&
             worker_command_args[1].find(kSetupWorkerFilename) != std::string::npos) {
    // 无 runtime env → 移除 setup_worker.py，直接运行 default_worker.py
    worker_command_args.erase(worker_command_args.begin() + 1,
                              worker_command_args.begin() + 2);
  }

  // 设置环境变量
  ProcessEnvironment env;
  if (!IsIOWorkerType(worker_type)) {
    env.emplace(kEnvVarKeyJobId, job_id.Hex());        // RAY_JOB_ID
  }
  env.emplace(kEnvVarKeyRayletPid, std::to_string(GetPID()));  // RAY_RAYLET_PID

  return {worker_command_args, env};
}
```

最终生成的命令行：

```bash
# 有 runtime_env 时：
python setup_worker.py --serialized-runtime-env-context=<ctx> --language=PYTHON \
  default_worker.py --worker-id=<hex> --node-id=<hex> --runtime-env-hash=<hash> ...

# 无 runtime_env 时（setup_worker.py 被移除）：
python default_worker.py --worker-id=<hex> --node-id=<hex> --runtime-env-hash=0 ...
```

### 1.6 实际 Fork/Exec：StartProcess 与 spawnvpe

**文件**: `src/ray/util/process.cc:121` — `ProcessFD::spawnvpe()`

```cpp
static pid_t ProcessFD::spawnvpe(
    const char *argv[], ...,
    std::function<void(const std::string &)> add_to_cgroup,
    bool new_process_group) {

  // 合并父进程环境变量与额外的环境变量
  auto merged_env = merge_environments(environ, env);

  // 创建管道：用于父进程跟踪子进程存活
  int parent_lifetime_pipe[2];
  pipe(parent_lifetime_pipe);

  pid_t pid = fork();

  if (pid == 0) {
    // ===== 子进程 =====

    // 立即加入 cgroup（如果启用了资源隔离）
    add_to_cgroup(std::to_string(getpid()));

    // 重置 SIGCHLD handler
    signal(SIGCHLD, SIG_DFL);

    // 创建新进程组（防止信号传播）
    if (new_process_group) {
      setpgrp();
    }

    // 重定向 stdin 到父进程的存活检测管道
    // 当父进程死亡时，管道断裂，子进程可以检测到
    dup2(parent_lifetime_pipe[0], STDIN_FILENO);

    // 写入自己的 PID 给父进程
    write(fd, &pid_to_report, sizeof(pid_to_report));

    // 替换进程镜像为目标程序
    execvpe(argv[0], const_cast<char **>(argv), envp);
    // 如果 exec 失败，退出
    _exit(127);
  }

  // ===== 父进程 =====
  // 保存管道 FD 用于后续存活检测
  // 等待子进程写入 PID
  return child_pid;
}
```

---

## 二、Worker 如何运行 Python 代码

Worker 进程启动时并不知道要执行什么函数——它是一个**通用的语言运行时容器**。函数代码在 Task 到达时通过 `FunctionDescriptor` 动态查找或从 GCS 下载。这是 Ray 能够实现 Worker 复用的核心机制。

### Task 执行全链路

```
Driver 端:                           Worker 端:

@ray.remote                          python default_worker.py
def my_func(x):                        │
    return x + 1                       ├── connect(raylet, gcs, object_store)
                                       ├── CoreWorker 初始化 (C++)
首次调用:                               ├── RegisterClient → AnnounceWorkerPort
  PythonFunctionDescriptor =           ├── main_loop()
    {module: "__main__",               │     └── run_task_loop() ← 阻塞等待
     function: "my_func",             │           (boost::asio io_context::run)
     hash: "a1b2c3..."}               │
                                       │
  pickle.dumps(my_func)               │
    → 存入 GCS Internal KV            │
                                       │
ray.remote(my_func).remote(42)        │
  │                                    │
  ▼                                    │
构建 TaskSpec:                         │
  language = PYTHON                    │
  function_descriptor = {上述}          │
  args = [serialize(42)]              │
  ↓ 提交到调度器                        │
                                       │
        ···· gRPC PushTask ·····►     │
                                       │
                              HandlePushTask (C++)
                                │
                                ▼
                              ExecuteTask (C++)
                                │ 拉取参数、构建 RayFunction
                                │ task_execution_callback → Cython
                                │
                                ▼
                              task_execution_handler (Cython)
                                │ with gil:
                                │
                                ▼
                              execute_task_with_cancellation_handler
                                │
                                ├── 函数查找 (三级查找):
                                │   [1] 本地缓存 (O(1) dict 查找)
                                │   [2] importlib.import_module() + getattr()
                                │   [3] GCS pickle 下载 → pickle.loads()
                                │
                                ▼
                              execute_task
                                │ function_executor = execution_info.function
                                │ args = deserialize(c_args)
                                │ outputs = function_executor(*args, **kwargs)
                                │            ← 真正执行 my_func(42)
                                │ serialize(outputs) → Object Store
                                ▼
                              返回结果
```

### 函数传递方式总结

```
方式1: load_code_from_local（本地加载）
  ────────────────────────────────────────────────
  Worker 的 PYTHONPATH 上能 import 到用户的模块
  → importlib.import_module(module_name)
  → getattr(module, function_name)
  适用于: 所有节点部署了相同代码的场景（如 K8s 集群）
  优势: 不依赖 GCS，启动更快

方式2: GCS pickle 传输（默认）
  ────────────────────────────────────────────────
  Driver:  pickle.dumps(function) → GCS internal_kv_put
  Worker:  GCS internal_kv_get → pickle.loads(bytes)
  → 恢复函数对象（包括字节码和闭包变量）
  适用于: 交互式开发，Worker 上没有源代码（如 Jupyter Notebook）
  限制: 闭包变量也被 pickle，大对象需注意
```

### 2.1 Python Worker 启动链

```
Raylet fork+exec
    │
    ├─ 有 runtime_env:
    │   python setup_worker.py --serialized-runtime-env-context=... default_worker.py <args>
    │     └── setup_worker.py:
    │           1. 解析 runtime_env_context
    │           2. 设置环境（pip 包、conda env、working_dir 等）
    │           3. exec_worker() → execvpe(python, default_worker.py, ...)
    │
    └─ 无 runtime_env:
        python default_worker.py <args>
```

**文件**: `python/ray/_private/workers/setup_worker.py`

`setup_worker.py` 是一个引导程序，它解析 `--serialized-runtime-env-context` 参数，调用 `runtime_env_context.exec_worker()` 来设置运行时环境（如激活 conda 环境、设置 PYTHONPATH），然后通过 `execvpe` 替换自己为 `default_worker.py`。

### 2.2 default_worker.py 入口逻辑

**文件**: `python/ray/_private/workers/default_worker.py:203`

```python
if __name__ == "__main__":
    args = parser.parse_args()

    # 确定 Worker 模式（普通 Task Worker / Spill IO Worker / Restore IO Worker）
    if args.worker_type == "SPILL_WORKER":
        mode = ray.SPILL_WORKER_MODE
    elif args.worker_type == "RESTORE_WORKER":
        mode = ray.RESTORE_WORKER_MODE
    else:
        mode = ray.WORKER_MODE

    # 构建连接参数
    ray_params = RayParams(
        node_ip_address=args.node_ip_address,
        node_manager_port=args.node_manager_port,
        raylet_socket_name=args.raylet_name,
        plasma_store_socket_name=args.object_store_name,
        # ...
    )

    # 创建 Node 对象（仅连接模式，不启动任何本地服务）
    node = ray._private.node.Node(
        ray_params, head=False, connect_only=True, default_worker=True
    )

    # 连接到 Raylet、GCS、Object Store
    ray._private.worker.connect(
        node, session_name, mode=mode, worker_id=worker_id
    )

    worker = ray._private.worker.global_worker

    # 设置日志、preload 模块、运行 setup hooks...

    # 进入主循环（阻塞）
    if mode == ray.WORKER_MODE:
        worker.main_loop()
```

### 2.3 连接 Raylet 与 CoreWorker 初始化

**文件**: `python/ray/_private/worker.py:2482` — `connect()`

```python
def connect(node, session_name, mode=WORKER_MODE, worker_id=None, ...):
    """Connect this worker to the raylet, to Plasma, and to GCS."""

    # 初始化 GCS 客户端
    worker.gcs_client = node.get_gcs_client()
    _initialize_internal_kv(worker.gcs_client)

    # 创建 CoreWorker（C++ 对象，通过 Cython 桥接）
    # 这一步建立了与 Raylet 的所有通信通道
    worker.core_worker = ray._raylet.CoreWorker(
        mode,
        node.plasma_store_socket_name,    # 连接到 Object Store
        node.raylet_socket_name,          # 连接到 Raylet (IPC)
        job_id,
        gcs_options,                      # 连接到 GCS
        logs_dir,
        node.node_ip_address,
        node.node_manager_port,           # Raylet 的 gRPC 端口
        # ...
    )

    worker.set_is_connected(True)
```

CoreWorker C++ 构造函数（`src/ray/core_worker/core_worker.cc:287`）内部会：
- 建立与 Raylet 的 gRPC 连接（`local_raylet_rpc_client`）
- 建立与 Raylet 的 IPC 连接（`raylet_ipc_client`）
- 连接到 Plasma Object Store
- 启动自身的 gRPC Server（用于接收 Raylet 推送的 PushTask 请求）
- 向 GCS 注册

### 2.4 主事件循环：run_task_loop

**文件**: `python/ray/_private/worker.py:1018`

```python
def main_loop(self):
    """The main loop a worker runs to receive and execute tasks."""
    def sigterm_handler(signum, frame):
        raise_sys_exit_with_custom_error_message(
            "The process receives a SIGTERM.", exit_code=1)
    ray._private.utils.set_sigterm_handler(sigterm_handler)

    # 调用 C++ CoreWorker 的任务执行循环（阻塞）
    self.core_worker.run_task_loop()
    sys.exit(0)
```

**文件**: `python/ray/_raylet.pyx:2834`

```cython
def run_task_loop(self):
    with nogil:
        CCoreWorkerProcess.RunTaskExecutionLoop()
```

**文件**: `src/ray/core_worker/core_worker.cc:2713`

```cpp
void CoreWorker::RunTaskExecutionLoop() {
  auto signal_checker = PeriodicalRunner::Create(task_execution_service_);
  if (options_.check_signals) {
    signal_checker->RunFnPeriodically([this] {
      auto status = options_.check_signals();
      // 处理 IntentionalSystemExit / UnexpectedSystemExit
    }, 10, "CoreWorker.CheckSignal");
  }
  event_loops_running_ = true;
  // boost::asio io_context::run() — 阻塞等待事件
  task_execution_service_.run();
}
```

### 2.5 Task 接收与执行链路

#### C++ 层：接收 PushTask gRPC

**文件**: `src/ray/core_worker/core_worker.cc:3435`

```cpp
void CoreWorker::HandlePushTask(rpc::PushTaskRequest request,
                                rpc::PushTaskReply *reply,
                                rpc::SendReplyCallback send_reply_callback) {
  // 设置 Job 信息
  if (request.task_spec().type() == TaskType::ACTOR_CREATION_TASK ||
      request.task_spec().type() == TaskType::NORMAL_TASK) {
    auto job_id = JobID::FromBinary(request.task_spec().job_id());
    worker_context_->MaybeInitializeJobInfo(job_id, request.task_spec().job_config());
  }

  // 对于 Actor Task：直接 post 到 task_execution_service_
  if (request.task_spec().type() == TaskType::ACTOR_TASK) {
    task_execution_service_.post([...] {
      task_receiver_->QueueTaskForExecution(std::move(request), reply, callback);
    }, "CoreWorker.HandleActorTask");
  }

  // 对于普通 Task：入队后触发执行
  task_receiver_->QueueTaskForExecution(std::move(request), reply, callback);
  task_execution_service_.post([...] {
    task_receiver_->ExecuteQueuedNormalTasks();
  }, "CoreWorker.HandleNormalTask");
}
```

#### C++ 层：ExecuteTask — 从 TaskSpec 构建 RayFunction

**文件**: `src/ray/core_worker/core_worker.cc:2798-2968`

```cpp
Status CoreWorker::ExecuteTask(const TaskSpecification &task_spec, ...) {
  // 拉取并 pin 住 Task 参数
  Status pin_args_request_status =
      GetAndPinArgsForExecutor(task_spec, &args, &arg_refs, &borrowed_ids);

  // 从 TaskSpec 中提取语言和函数描述符，构建 RayFunction
  RayFunction func{task_spec.GetLanguage(), task_spec.FunctionDescriptor()};

  // 通过回调函数调用到 Python/Cython 层
  Status status = options_.task_execution_callback(
      task_spec.CallerAddress(),
      task_type,
      task_spec.GetName(),
      func,                        // ← 包含 FunctionDescriptor
      task_spec.GetRequiredResources().GetResourceUnorderedMap(),
      args,                        // ← 序列化的参数
      arg_refs,
      // ...
  );
}
```

#### Cython 层：task_execution_handler — 桥接 C++ 与 Python

**文件**: `python/ray/_raylet.pyx:2235`

```cython
cdef CRayStatus task_execution_handler(
        const CAddress &caller_address,
        CTaskType task_type,
        const c_string task_name,
        const CRayFunction &ray_function,
        ...) nogil:
    with gil, disable_client_hook():
        # 初始化 job_config
        maybe_initialize_job_config()

        # 调用执行逻辑
        execute_task_with_cancellation_handler(
            caller_address, task_type, task_name,
            ray_function, c_resources, c_args, ...)
```

#### Cython 层：execute_task_with_cancellation_handler — 函数查找

**文件**: `python/ray/_raylet.pyx:2022-2139`

```cython
cdef execute_task_with_cancellation_handler(...):
    worker = ray._private.worker.global_worker
    manager = worker.function_actor_manager

    # 从 C++ RayFunction 中提取 Python FunctionDescriptor
    function_descriptor = CFunctionDescriptorToPython(
        ray_function.GetFunctionDescriptor())

    # 如果是 Actor 创建任务，先加载 Actor 类
    if task_type == TASK_TYPE_ACTOR_CREATION_TASK:
        actor_class = manager.load_actor_class(job_id, function_descriptor)
        actor = actor_class.__new__(actor_class)
        worker.actors[actor_id] = actor

    # 查找函数的执行信息（核心！）
    execution_info = execution_infos.get(function_descriptor)
    if not execution_info:
        execution_info = manager.get_execution_info(job_id, function_descriptor)
        execution_infos[function_descriptor] = execution_info

    # 调用 execute_task 执行
    execute_task(..., execution_info, ...)
```

#### Cython 层：execute_task — 实际执行用户代码

**文件**: `python/ray/_raylet.pyx:1652-1818`

```cython
cdef void execute_task(..., execution_info, ...) except *:
    # 对于普通 Task：直接获取函数
    if task_type == TASK_TYPE_NORMAL_TASK:
        function_executor = execution_info.function

    # 对于 Actor Task：包装为 actor.method() 调用
    else:
        actor = worker.actors[actor_id]
        def function_executor(*arguments, **kwarguments):
            func = execution_info.function
            return func(actor, *arguments, **kwarguments)

    # 反序列化参数
    metadata_pairs = RayObjectsToSerializedRayObjects(c_args, object_refs)
    args = worker.deserialize_objects(metadata_pairs, object_refs)
    args, kwargs = ray._common.signature.recover_args(args)

    # ← 真正执行用户代码
    outputs = function_executor(*args, **kwargs)

    # 序列化返回值，存入 Object Store
    # ...
```

### 2.6 函数的序列化与动态加载机制

这是回答"先创建 Worker 再执行 Python 代码"的核心：**Worker 启动时不知道要执行什么函数，函数是通过 FunctionDescriptor 动态查找或从 GCS 下载的。**

#### Driver 端：序列化函数并存入 GCS

**文件**: `python/ray/remote_function.py:350-372`

```python
def _remote(self, args=None, kwargs=None, ...):
    # 首次调用时，构建函数描述符并导出
    if self._last_export_cluster_and_job != worker.current_cluster_and_job:
        # 构建 FunctionDescriptor（只是元数据：模块名+函数名+哈希）
        self._function_descriptor = PythonFunctionDescriptor.from_function(
            self._function, self._uuid
        )
        # pickle 序列化整个函数对象（包括字节码和闭包变量）
        self._pickled_function = pickle_dumps(self._function, ...)

        self._last_export_cluster_and_job = worker.current_cluster_and_job
        worker.function_actor_manager.export(self)  # ← 导出到 GCS
```

**文件**: `python/ray/_private/function_manager.py:197-244`

```python
def export(self, remote_function):
    # 如果配置了 load_code_from_local 且本地能找到该函数，则不导出
    if self._worker.load_code_from_local:
        if self.load_function_or_class_from_local(module_name, function_name) is not None:
            return

    # 构建 GCS key
    key = make_function_table_key(
        b"RemoteFunction", self._worker.current_job_id,
        remote_function._function_descriptor.function_id.binary()
    )

    # 序列化并存入 GCS
    val = pickle.dumps({
        "job_id": self._worker.current_job_id.binary(),
        "function_id": function_id,
        "function_name": remote_function._function_name,
        "module": function.__module__,      # 模块路径（如 "__main__"）
        "function": pickled_function,        # pickle化的函数体
        "max_calls": remote_function._max_calls,
    })
    self._worker.gcs_client.internal_kv_put(key, val, True, KV_NAMESPACE_FUNCTION_TABLE)
```

#### Task 提交：只携带 FunctionDescriptor（不含代码）

**文件**: `src/ray/protobuf/common.proto:507-515`

```protobuf
message TaskSpec {
  TaskType type = 1;
  string name = 2;
  Language language = 3;                        // PYTHON / JAVA / CPP
  FunctionDescriptor function_descriptor = 4;   // ← 只是描述符，不是代码
  bytes job_id = 5;
  repeated TaskArg args = 11;                   // ← 参数
  // ...
}
```

**文件**: `src/ray/protobuf/common.proto:142-147`

```protobuf
message PythonFunctionDescriptor {
  string module_name = 1;     // e.g. "__main__"
  string class_name = 2;      // e.g. "" (普通函数) 或 "MyActor"
  string function_name = 3;   // e.g. "my_func"
  string function_hash = 4;   // e.g. "a1b2c3..." (唯一标识)
}
```

FunctionDescriptor 只是函数的"地址"，不包含函数代码本身。

#### Worker 端：三级查找机制

**文件**: `python/ray/_private/function_manager.py:334-373`

```python
def get_execution_info(self, job_id, function_descriptor):
    function_id = function_descriptor.function_id

    # 第1级：本地缓存（之前已经加载过的函数，O(1)查找）
    if function_id in self._function_execution_info:
        return self._function_execution_info[function_id]

    # 第2级：从本地 Python 模块导入（load_code_from_local 模式）
    if self._worker.load_code_from_local:
        if self._load_function_from_local(function_descriptor):
            return self._function_execution_info[function_id]

    # 第3级：从 GCS 拉取序列化的函数体并反序列化
    self._wait_for_function(function_descriptor, job_id)
    return self._function_execution_info[function_id]
```

**第2级 — 本地模块导入**（`python/ray/_private/function_manager.py:375`）：

```python
def _load_function_from_local(self, function_descriptor):
    module_name = function_descriptor.module_name      # e.g. "my_module"
    function_name = function_descriptor.function_name  # e.g. "my_func"

    # 动态导入模块
    module = importlib.import_module(module_name)
    # 通过 getattr 获取函数对象
    parts = [part for part in function_name.split(".") if part]
    object = module
    for part in parts:
        object = getattr(object, part)

    self._function_execution_info[function_id] = FunctionExecutionInfo(
        function=object, function_name=function_name, max_calls=0
    )
```

**第3级 — GCS 拉取**（`python/ray/_private/function_manager.py:266`）：

```python
def fetch_and_register_remote_function(self, key):
    # 从 GCS internal_kv 拉取
    vals = self._worker.gcs_client.internal_kv_get(key, KV_NAMESPACE_FUNCTION_TABLE)
    remote_function_info = pickle.loads(vals)
    serialized_function = remote_function_info.function

    # pickle.loads 反序列化得到原始 Python 函数对象
    function = pickle.loads(serialized_function)
    function.__module__ = module  # 修正模块名

    # 存入本地缓存
    self._function_execution_info[function_id] = FunctionExecutionInfo(
        function=function, function_name=function_name, max_calls=max_calls
    )
```

#### 函数传递方式总结

```
方式1: load_code_from_local（本地加载）
  Worker 的 PYTHONPATH 上能 import 到用户的模块
  → importlib.import_module() + getattr() 直接加载
  适用于: 所有节点部署了相同代码的场景（如 K8s 集群）

方式2: GCS pickle 传输（默认）
  Driver pickle.dumps(function) → 存入 GCS Internal KV
  Worker pickle.loads(bytes) → 恢复函数对象（包括字节码和闭包）
  适用于: 交互式开发，Worker 上没有源代码的场景（如 Jupyter Notebook）
```

---

## 三、Worker 完整生命周期

Worker 进程从创建到销毁经历多个状态转换。理解这些状态对于诊断 Worker 泄漏、资源未释放等问题至关重要。

### Worker 与 Actor Worker 生命周期对比

```
普通 Task Worker:
  创建 → 注册 → [空闲 ←→ 执行Task] (循环复用) → 退出
                    ↑          ↓
                    └──────────┘  (Task完成后回到空闲池)

Actor Worker:
  创建 → 注册 → 空闲(短暂) → Actor创建 → [执行ActorTask] (循环) → Actor退出 → 进程退出
                                              ↑          ↓
                                              └──────────┘  (Actor方法调用间复用)
                              ※ Actor Worker 不回到空闲池，生命期 = Actor 生命期
```

### 空闲池管理策略

```
idle_of_all_languages_ (跨语言有序列表):

  队头 (Front)                                              队尾 (Back)
  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐
  │ Worker A │  │ Worker B │  │ Worker C │  │ Worker D │  │ Worker E │
  │ 从未执行  │  │ 从未执行  │  │ 执行过   │  │ 执行过   │  │ 最近执行  │
  │ (最冷)   │  │          │  │          │  │          │  │ (最热)   │
  └──────────┘  └──────────┘  └──────────┘  └──────────┘  └──────────┘
       ↑                                                        ↑
       │                                                        │
  TryKillingIdleWorkers:                              FindAndPopIdleWorker:
  从队头开始杀 (FIFO)                                 从队尾开始找 (LIFO)
  最冷的先杀                                          最热的先用

设计理由:
- 执行过 Task 的 Worker 已加载模块和缓存 → 复用更快 → 放队尾保留
- 从未执行的 Worker 无 warm-up 优势 → 放队头 → 优先杀死
- 软上限 = 节点 CPU 数量，超过时从队头 FIFO 回收
```

### 3.1 Worker 状态定义

#### Worker 类型

**文件**: `src/ray/protobuf/common.proto:33`

```protobuf
enum WorkerType {
  WORKER = 0;           // 普通 Task/Actor Worker
  DRIVER = 1;           // Driver 进程
  SPILL_WORKER = 2;     // 对象溢出 IO Worker
  RESTORE_WORKER = 3;   // 对象恢复 IO Worker
}
```

#### Worker 退出类型

**文件**: `src/ray/protobuf/common.proto:1039`

```protobuf
enum WorkerExitType {
  SYSTEM_ERROR = 0;            // 系统错误（crash）
  INTENDED_SYSTEM_EXIT = 1;    // 系统主动退出
  USER_ERROR = 2;              // 用户代码异常
  INTENDED_USER_EXIT = 3;      // 用户主动退出（如 ray.actor.exit_actor()）
  NODE_OUT_OF_MEMORY = 4;      // OOM 被杀
}
```

#### PopWorker 状态

**文件**: `src/ray/raylet/worker_pool.h:56`

```cpp
enum PopWorkerStatus {
  OK = 0,
  JobConfigMissing = 1,                 // Job 配置还未到达本节点
  TooManyStartingWorkerProcesses = 2,   // 超过并发启动上限
  WorkerPendingRegistration = 3,        // Worker 已启动但尚未注册
  RuntimeEnvCreationFailed = 4,         // Runtime Env 创建失败
  JobFinished = 5,                      // Job 已结束
};
```

#### Worker 内部状态标志

**文件**: `src/ray/raylet/worker.h:205-208`

```cpp
class Worker : public WorkerInterface {
  std::atomic<bool> killing_;   // 是否正在被杀死
  bool blocked_;                // 是否阻塞在 ray.get/ray.wait
};
```

### 3.2 完整生命周期状态机

```
┌──────────────────────────────────────────────────────────────────────────┐
│                       Worker 完整生命周期                                  │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  [Prestart 或 PopWorker 触发]                                             │
│       │                                                                  │
│       ▼                                                                  │
│  StartWorkerProcess() ──→ fork() + execvpe() ──→ OS 进程创建             │
│       │                   (worker_pool.cc:456)                           │
│       │   状态：is_pending_registration = true                           │
│       ▼                                                                  │
│  Worker 启动, 通过 Unix Socket 连接 Raylet                               │
│  发送 RegisterClientRequest                                              │
│       │   → WorkerPool::RegisterWorker() (worker_pool.cc:785)            │
│       ▼                                                                  │
│  Worker 的 gRPC Server 就绪                                              │
│  发送 AnnounceWorkerPort                                                 │
│       │   → OnWorkerStarted() [is_pending_registration = false]          │
│       │   → HandleWorkerAvailable() → PushWorker()                       │
│       ▼                                                                  │
│  ┌─── IDLE (空闲池) ◄─────────────────────────────────────────┐          │
│  │    state.idle + idle_of_all_languages_                      │          │
│  │         │                                                   │          │
│  │         ▼                                                   │          │
│  │    PopWorker() 匹配成功 → 从 idle 池移除                     │          │
│  │         │                                                   │          │
│  │         ▼                                                   │          │
│  │    LocalLeaseManager::Grant()                               │          │
│  │    → worker->GrantLease()                                   │          │
│  │    → worker->SetAllocatedInstances()                        │          │
│  │    状态：LEASED/BUSY                                        │          │
│  │         │                                                   │          │
│  │         ▼                                                   │          │
│  │    PushTask gRPC → 执行用户代码                               │          │
│  │         │                                                   │          │
│  │         │──(ray.get 阻塞)──→ NotifyWorkerBlocked            │          │
│  │         │                     ReleaseCpuResources            │          │
│  │         │──(ray.get 完成)──→ NotifyWorkerUnblocked           │          │
│  │         │                     ReturnCpuResources             │          │
│  │         │                                                   │          │
│  │         ▼                                                   │          │
│  │    Task 完成: ReturnWorkerLease RPC                         │          │
│  │    → ReleaseWorkerResources()                               │          │
│  │    → HandleWorkerAvailable() → PushWorker() ───────────────┘          │
│  │                                                                        │
│  │    [或: Actor 模式 → 永不回到 idle, Worker 生命期 = Actor 生命期]      │
│  │                                                                        │
│  └──→ TryKillingIdleWorkers() (定期执行)                                   │
│            │                                                              │
│            ▼                                                              │
│       KillIdleWorker() → Exit RPC → 优雅退出                              │
│            │                                                              │
│       [或: 异常断连 / 进程 crash]                                          │
│            │                                                              │
│            ▼                                                              │
│       DisconnectClient() → 清理资源 → 从 Pool 移除                        │
│            │                                                              │
│            ▼                                                              │
│       KillAsync() → SIGTERM → [超时] → SIGKILL                            │
│                                                                           │
└───────────────────────────────────────────────────────────────────────────┘
```

### 3.3 空闲 Worker 管理：PushWorker

**文件**: `src/ray/raylet/worker_pool.cc:1077`

当一个 Worker 变为空闲时，`PushWorker` 决定它的去向：

```cpp
void WorkerPool::PushWorker(const std::shared_ptr<WorkerInterface> &worker) {
  RAY_CHECK(worker->GetGrantedLeaseId().IsNil());  // 确认已释放 lease
  auto &state = GetStateForLanguage(worker->GetLanguage());

  // 第1步：尝试匹配 pending_registration_requests 队列中的请求
  auto it = std::find_if(
      state.pending_registration_requests.begin(),
      state.pending_registration_requests.end(),
      [this, &worker](const auto &request) {
        return WorkerFitForLease(*worker, *request) == WorkerUnfitForLeaseReason::NONE;
      });
  if (it != state.pending_registration_requests.end()) {
    pop_worker_request = *it;
    state.pending_registration_requests.erase(it);
  }

  // 第2步：如果第1步没匹配到，尝试 pending_start_requests 队列
  if (!pop_worker_request) {
    // 类似逻辑...
  }

  if (pop_worker_request) {
    // 找到匹配的请求，直接交给它
    pop_worker_request->callback_(worker, PopWorkerStatus::OK, "");
  } else {
    // 没有匹配的请求，放入空闲池
    state.idle.insert(worker);

    // 决定在 idle_of_all_languages_ 列表中的位置
    if (worker->GetGrantedLeaseTime() == absl::Time()) {
      // 从未被分配过 lease 的 Worker → 放到队头（最先被杀死）
      // 理由：没有被 "warm up"，比用过的 Worker 冷启动慢
      idle_of_all_languages_.emplace_front(IdleWorkerEntry{worker, keep_alive_until});
    } else {
      // 曾经执行过 Task 的 Worker → 放到队尾（尽量保留）
      // 理由：已经加载了各种模块和缓存，复用更快
      idle_of_all_languages_.emplace_back(IdleWorkerEntry{worker, keep_alive_until});
    }
  }
}
```

### 3.4 空闲 Worker 回收：TryKillingIdleWorkers

**文件**: `src/ray/raylet/worker_pool.cc:1154`

定期执行（`kill_idle_workers_interval_ms`），策略：

```cpp
void WorkerPool::TryKillingIdleWorkers() {
  const absl::Time now = get_time_();

  // 第1步：立即杀死已死亡的和 Job 已结束的 Worker
  for (auto it = idle_of_all_languages_.begin(); it != idle_of_all_languages_.end();) {
    if (it->worker->IsDead()) {
      it = idle_of_all_languages_.erase(it);
      continue;
    }
    const auto &job_id = it->worker->GetAssignedJobId();
    if (finished_jobs_.contains(job_id)) {
      KillIdleWorker(*it);              // 立即杀死
      it = idle_of_all_languages_.erase(it);
    } else {
      if (entry.keep_alive_until < now) {
        num_killable_idle_workers++;     // 统计可杀死的 Worker 数量
      }
      it++;
    }
  }

  // 第2步：软限制 = 节点 CPU 数量
  const auto num_desired_idle_workers = get_num_cpus_available_();

  // 第3步：从队头开始杀（FIFO — 最冷的先杀）
  auto it = idle_of_all_languages_.begin();
  while (num_killable_idle_workers > num_desired_idle_workers && ...) {
    if (entry.keep_alive_until < now) {
      KillIdleWorker(*it);
      it = idle_of_all_languages_.erase(it);
      num_killable_idle_workers--;
    } else {
      it++;
    }
  }
}
```

### 3.5 Worker 断连与清理：DisconnectClient

**文件**: `src/ray/raylet/node_manager.cc:1404`

当 Worker 断连时（无论是主动还是被动），Raylet 执行全面清理：

```cpp
void NodeManager::DisconnectClient(const std::shared_ptr<ClientConnection> &client,
                                   bool intentional_disconnect,
                                   rpc::WorkerExitType disconnect_type, ...) {
  // 从 WorkerPool 中查找对应的 Worker
  auto worker = worker_pool_.GetRegisteredWorker(client);

  // 取消该 Worker 上的 ray.get / ray.wait
  CancelGetAndWait(client);

  // 如果 Worker 持有 lease，清理 lease 并归还资源
  auto lease_it = leased_workers_.find(worker->GetGrantedLeaseId());
  if (lease_it != leased_workers_.end()) {
    local_lease_manager_.CleanupLease(worker, &lease);
    leased_workers_.erase(lease_it);
  }

  // 向 GCS 报告 Worker 失败
  // ...

  // 从 WorkerPool 中移除
  worker_pool_.DisconnectWorker(worker, disconnect_type);
}
```

**文件**: `src/ray/raylet/worker_pool.cc:1563`

```cpp
void WorkerPool::DisconnectWorker(const std::shared_ptr<WorkerInterface> &worker,
                                  rpc::WorkerExitType disconnect_type) {
  MarkPortAsFree(worker->AssignedPort());

  auto &state = GetStateForLanguage(worker->GetLanguage());
  // 从 worker_processes 中移除
  state.worker_processes.erase(worker->WorkerId());
  // 从 registered_workers 中移除
  state.registered_workers.erase(worker);
  // 从 idle 池中移除
  state.idle.erase(worker);

  // 如果该 runtime_env 不再被其他 Worker 使用，删除它
  DeleteRuntimeEnvIfPossible(serialized_runtime_env);

  // 尝试启动队列中等待的请求
  TryPendingStartRequests(worker->GetLanguage());
}
```

### 3.6 Worker 杀死：KillAsync

**文件**: `src/ray/raylet/worker.cc:63`

```cpp
void Worker::KillAsync(instrumented_io_context &io_service, bool force) {
  bool expected = false;
  if (!killing_.compare_exchange_strong(expected, true)) {
    return;  // 已经在杀了，幂等
  }

  if (force) {
    // 强制杀死：直接 SIGKILL
    worker->GetProcess().Kill();
  } else {
    // 优雅杀死：先 SIGTERM，等超时后再 SIGKILL
    kill(worker->GetProcess().GetId(), SIGTERM);

    // 设置超时定时器
    auto timer = std::make_shared<boost::asio::deadline_timer>(io_service);
    timer->expires_from_now(boost::posix_time::milliseconds(
        RayConfig::instance().kill_worker_timeout_milliseconds()));
    timer->async_wait([worker, timer](const boost::system::error_code &ec) {
      if (!ec) {
        worker->GetProcess().Kill();  // 超时，SIGKILL
      }
    });
  }
}
```

---

## 四、资源管控机制

Ray 的资源管理分为两个层面：**逻辑记账**（主要机制）和 **OS 级 cgroup 隔离**（可选机制）。两者的关系和区别是理解 Ray 资源模型的关键。

### 资源管理架构总览

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         资源管理全景                                      │
│                                                                         │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │  第1层: 调度准入控制 (核心机制)                                     │  │
│  │                                                                   │  │
│  │  ClusterResourceScheduler                                        │  │
│  │    → 选择节点: GetBestSchedulableNode()                           │  │
│  │    → 检查: 节点 available >= task required                        │  │
│  │                                                                   │  │
│  │  LocalResourceManager                                            │  │
│  │    → 分配: available -= task_resources                            │  │
│  │    → 释放: available += task_resources                            │  │
│  │    → 记账型: 只是数字加减，无 OS 级强制                              │  │
│  │                                                                   │  │
│  │  结果: 保证同一节点不会同时运行超过逻辑资源上限的 Task               │  │
│  │  限制: 单个 Task 可实际使用所有 CPU/内存，无硬上限                   │  │
│  └───────────────────────────────────────────────────────────────────┘  │
│                                                                         │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │  第2层: cgroupv2 粗粒度隔离 (opt-in, Linux only, v2.51.0+)       │  │
│  │                                                                   │  │
│  │  /sys/fs/cgroup/ray-node_<id>/                                   │  │
│  │    ├── system/leaf/           ← Raylet, GCS, Dashboard           │  │
│  │    │     cpu.weight = ~5%     (保障系统进程不被饿死)               │  │
│  │    │     memory.min = 保底    (保障最低内存)                       │  │
│  │    │                                                              │  │
│  │    └── user/                  ← 所有 Worker 进程                  │  │
│  │          cpu.weight = ~95%                                        │  │
│  │          ├── workers/         ← 所有 Worker (不区分资源需求)       │  │
│  │          └── non-ray/         ← 非 Ray 进程                       │  │
│  │                                                                   │  │
│  │  特点:                                                            │  │
│  │  - 只有 cpu.weight (比例权重)，没有 cpu.max (硬上限)               │  │
│  │  - 只有 memory.min (保底)，没有 memory.max (硬上限)               │  │
│  │  - 没有 per-worker 或 per-task 的 cgroup 子目录                   │  │
│  │  - Worker 在整个生命周期中始终在同一个 workers/ cgroup 中           │  │
│  └───────────────────────────────────────────────────────────────────┘  │
│                                                                         │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │  第3层: GPU 隔离 (环境变量方式)                                     │  │
│  │                                                                   │  │
│  │  每次 Task 执行前设置 CUDA_VISIBLE_DEVICES                        │  │
│  │  → 不是 cgroup，是 CUDA 运行时的软件约定                           │  │
│  │  → 同一 Worker 复用时会根据新 Task 重新设置                        │  │
│  │  → 恶意代码可以绕过                                                │  │
│  └───────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
```

### Worker 复用时资源变化示例

```
Worker A (一个 Python 进程, PID=12345)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

时间线   │ 调度层面                     │ OS/cgroup 层面
─────────┼──────────────────────────────┼────────────────────────
  t1     │ 分配: Task1(cpu=4, gpu=1)   │ cgroup: workers/ (不变)
         │ available -= {cpu:4, gpu:1}  │ CUDA_VISIBLE_DEVICES=0
         │                              │
  t2     │ 执行 Task1                   │ 进程可使用所有核心
         │                              │ (调度只是逻辑保证)
         │                              │
  t3     │ 释放: Task1 完成             │ cgroup: workers/ (不变)
         │ available += {cpu:4, gpu:1}  │
         │ Worker → idle 池             │
         │                              │
  t4     │ 分配: Task2(cpu=1)          │ cgroup: workers/ (不变)
         │ available -= {cpu:1}         │ CUDA_VISIBLE_DEVICES 不设置
         │                              │
  t5     │ 执行 Task2                   │ 进程可使用所有核心
         │                              │
  t6     │ 释放: Task2 完成             │ cgroup: workers/ (不变)
         │ available += {cpu:1}         │

结论: 资源隔离本质是调度准入控制，不是 OS 级物理隔离
```

### 4.1 逻辑资源管理（主要机制）

Ray 的资源管理**主要是逻辑/记账式的**，不是 OS 层面的强制隔离。核心类是 `LocalResourceManager`。

**文件**: `src/ray/raylet/scheduling/local_resource_manager.cc:90-105`

```cpp
bool LocalResourceManager::AllocateTaskResourceInstances(
    const ResourceRequest &resource_request,
    std::shared_ptr<TaskResourceInstances> task_allocation) {
  // 尝试从可用资源池中分配
  auto allocation =
      local_resources_.available.TryAllocate(resource_request.GetResourceSet());
  if (allocation) {
    *task_allocation = TaskResourceInstances(*allocation);
    for (const auto &resource_id : resource_request.ResourceIds()) {
      SetResourceNonIdle(resource_id);  // 标记资源为非空闲
    }
    return true;
  } else {
    return false;  // 资源不足
  }
}
```

核心数据结构：
- `local_resources_.total` — 节点总资源（如 8 CPU, 2 GPU）
- `local_resources_.available` — 当前可用资源

### 4.2 资源分配/回收完整流程

```
1. Client 提交 Task: @ray.remote(num_cpus=2, num_gpus=1)
     │
2. GCS 或 Raylet 收到 lease 请求
     │
3. ClusterResourceScheduler::GetBestSchedulableNode()
   → 检查所有节点资源 → 选择最佳节点
     │
4. LocalLeaseManager::GrantScheduledLeasesToWorkers()
     │
5. LocalResourceManager::AllocateLocalTaskResources(resource_request)
   → local_resources_.available.TryAllocate({CPU:2, GPU:1})
   → 返回 TaskResourceInstances（具体的资源实例 ID）
     │
6. PopWorker() → 找到匹配 Worker
     │
7. Grant() → worker->SetAllocatedInstances(task_allocation)
   → 对 Actor: worker->SetLifetimeAllocatedInstances()
     │
8. Task 执行完毕
     │
9. ReturnWorkerLease → ReleaseWorkerResources()
   → LocalResourceManager::FreeTaskResourceInstances(allocated)
   → 归还资源到 available 池
   → Worker 回到 idle pool（不再持有资源）
```

### 4.3 cgroupv2 实现（可选机制）

Ray 从 v2.51.0 开始支持 cgroupv2，但这是 **opt-in 功能**，需通过 `--enable-resource-isolation` 启用，且**仅支持 Linux**。

#### 关键文件

| 文件 | 作用 |
|------|------|
| `src/ray/common/cgroup2/cgroup_manager_interface.h` | 抽象接口 |
| `src/ray/common/cgroup2/cgroup_manager.h/cc` | 具体实现 |
| `src/ray/common/cgroup2/cgroup_driver_interface.h` | 底层文件系统操作接口 |
| `src/ray/common/cgroup2/sysfs_cgroup_driver.h/cc` | Linux sysfs 实现 |
| `src/ray/common/cgroup2/linux_cgroup_manager_factory.cc` | Linux 工厂 |
| `src/ray/common/cgroup2/noop_cgroup_manager.h` | 非 Linux 空操作实现 |
| `python/ray/_private/resource_isolation_config.py` | Python 端配置 |

### 4.4 cgroup 层次结构与约束

**文件**: `src/ray/common/cgroup2/cgroup_manager_interface.h:30-40`

```
/sys/fs/cgroup (base_cgroup_path)
      │
  ray-node_<node_id>/
      ├── system/              ← Raylet, GCS, Dashboard, RuntimeEnv Agent
      │     └── leaf/          ← 实际放进程（cgroupv2 规则要求）
      │           cpu.weight = system_reserved (默认 ~5%)
      │           memory.min = system_reserved_memory
      │
      └── user/                ← 所有用户进程
            │  cpu.weight = 10000 - system_reserved (~95%)
            │
            ├── workers/       ← **所有** Worker 进程（不区分资源需求）
            └── non-ray/       ← 节点上预先存在的非 Ray 进程
```

**文件**: `src/ray/common/cgroup2/cgroup_manager.cc:292-306`

```cpp
Status CgroupManager::Initialize() {
  // ... 创建 cgroup 目录 ...
  // ... 启用 cpu 和 memory 控制器 ...

  // 设置 system cgroup 的 CPU 权重
  cgroup_driver_->AddConstraint(system_cgroup_,
                                "cpu.weight",
                                std::to_string(system_reserved_cpu_weight));
  // 设置 system cgroup 的内存保底
  cgroup_driver_->AddConstraint(system_cgroup_,
                                "memory.min",
                                std::to_string(system_reserved_memory_bytes));
  // 设置 user cgroup 的 CPU 权重
  int64_t user_cpu_weight = 10000 - system_reserved_cpu_weight;
  cgroup_driver_->AddConstraint(user_cgroup_,
                                "cpu.weight",
                                std::to_string(user_cpu_weight));
}
```

**注意**：
- 只使用了 `cpu.weight`（比例权重，非硬限制）和 `memory.min`（内存保底）
- **没有** `cpu.max`（CPU 硬上限）
- **没有** `memory.max`（内存硬上限）
- **没有** per-worker 或 per-task 的 cgroup 子目录

### 4.5 cgroup 对 Worker 的具体操作

Worker 加入 cgroup 的时机是 **fork 后、exec 前**（一次性操作）：

**文件**: `src/ray/raylet/main.cc:282-302`

```cpp
// 创建 cgroup manager
std::unique_ptr<CgroupManagerInterface> cgroup_manager =
    CgroupManagerFactory::Create(enable_resource_isolation, ...);

// Worker 进程的 cgroup 钩子
AddProcessToCgroupHook add_process_to_workers_cgroup_hook =
    [&cgroup_mgr = *cgroup_manager](const std::string &pid) {
      RAY_CHECK_OK(cgroup_mgr.AddProcessToWorkersCgroup(pid));
    };

// 系统进程的 cgroup 钩子
AddProcessToCgroupHook add_process_to_system_cgroup_hook =
    [&cgroup_mgr = *cgroup_manager](const std::string &pid) {
      RAY_CHECK_OK(cgroup_mgr.AddProcessToSystemCgroup(pid));
    };
```

**文件**: `src/ray/util/process.cc:216`

```cpp
pid = fork();
if (pid == 0) {
    // 子进程：立即加入 workers cgroup
    add_to_cgroup(std::to_string(getpid()));
    // ... 后续 exec ...
}
```

**文件**: `src/ray/common/cgroup2/cgroup_manager.cc:327-329`

```cpp
Status CgroupManager::AddProcessToWorkersCgroup(const std::string &pid) {
  // 写 PID 到 <user>/workers/cgroup.procs 文件
  return AddProcessToCgroup(workers_cgroup_, pid);
}
```

底层实现是写文件：

```cpp
// sysfs_cgroup_driver.cc
Status SysFsCgroupDriver::AddProcessToCgroup(const std::string &cgroup,
                                             const std::string &pid) {
  std::string procs_file = cgroup + "/cgroup.procs";
  int fd = open(procs_file.c_str(), O_RDWR);
  write(fd, pid.c_str(), pid.size());
  close(fd);
  return Status::OK();
}
```

CgroupManagerInterface 只暴露两个方法：

```cpp
class CgroupManagerInterface {
 public:
  virtual Status AddProcessToWorkersCgroup(const std::string &pid) = 0;
  virtual Status AddProcessToSystemCgroup(const std::string &pid) = 0;
  // 没有: CreatePerWorkerCgroup, UpdateWorkerResources, MoveWorkerBetweenCgroups...
};
```

### 4.6 Worker 复用时资源不一致的处理

**核心结论：cgroup 层面完全不处理。** Worker 复用执行不同资源需求的 Task 时：

```
Worker A (一个 Python 进程)
  ─────────────────────────────────
  第1次执行: Task (num_cpus=4, num_gpus=1)
    → 调度器逻辑扣减 4 CPU + 1 GPU
    → 设置 CUDA_VISIBLE_DEVICES=0
    → Task 完成 → ReturnWorkerLease → 归还 4 CPU + 1 GPU
    → cgroup: 无任何变化，始终在 workers/ cgroup 中

  第2次执行: Task (num_cpus=1)
    → 调度器逻辑扣减 1 CPU
    → 不设置 GPU 环境变量
    → Task 完成 → ReturnWorkerLease → 归还 1 CPU
    → cgroup: 无任何变化，始终在 workers/ cgroup 中

  OS 层面: Worker A 在整个生命周期中一直在同一个 workers/ cgroup 里
  OS 层面: 没有 CPU/Memory 硬限制施加到这个进程
```

资源隔离的本质是**调度准入控制**：
- 调度器保证不会把超过节点资源的 Task 同时调度到同一节点
- 但没有 OS 级强制：一个声明 `num_cpus=1` 的 Task 实际上可以使用所有 CPU 核心
- Worker 复用时，资源在调度层面切换，cgroup 设置不变

### 4.7 GPU 隔离：环境变量方式

GPU 隔离**不是通过 cgroup 实现的**，而是通过 `CUDA_VISIBLE_DEVICES` 环境变量。

**文件**: `python/ray/_private/accelerators/nvidia_gpu.py:93-101`

```python
@staticmethod
def set_current_process_visible_accelerator_ids(visible_cuda_devices):
    if env_bool(NOSET_CUDA_VISIBLE_DEVICES_ENV_VAR, False):
        return
    os.environ["CUDA_VISIBLE_DEVICES"] = ",".join(
        [str(i) for i in visible_cuda_devices]
    )
```

这是在**每次 Task 执行前**设置的（`python/ray/_raylet.pyx:2065-2066`）：

```cython
if (<int>task_type != <int>TASK_TYPE_ACTOR_TASK):
    original_visible_accelerator_env_vars = \
        ray._private.utils.set_visible_accelerator_ids()
```

所以同一个 Worker 复用执行不同 GPU 需求的 Task 时，会根据新 Task 的 GPU 分配重新设置 `CUDA_VISIBLE_DEVICES`。但这是软件约定，恶意代码可以绕过。

---

## 五、Worker 预创建与适配

Worker 预创建是 Ray 减少冷启动延迟的关键优化。由于 Python 进程启动、模块导入、CoreWorker 初始化等开销显著，提前准备好 Worker 可以大幅降低首次 Task 调度的延迟。

### 预创建策略决策流程

```
Raylet 启动
    │
    ├── [1] 启动时预创建 (WorkerPool::Start)
    │     │
    │     ├── enable_worker_prestart = true?
    │     │     │
    │     │     ├── YES → PrestartWorkersInternal(Python, num_prestart_python_workers)
    │     │     │           │
    │     │     │           ├── 每个 Worker: 无 runtime_env → 直接 StartWorkerProcess()
    │     │     │           │
    │     │     │           └── 创建后放入 idle 池队头（从未执行过）
    │     │     │
    │     │     └── NO → 不预创建，等待首个 lease 请求
    │     │
    │     └── 同时启动 TryKillingIdleWorkers 定时器（定期清理多余 idle Worker）
    │
    └── [2] 请求驱动预创建 (PrestartWorkers)
          │  每次 RequestWorkerLease 到达时触发
          │
          ├── 计算 num_usable_workers = idle.size() + starting_workers
          │
          ├── 计算 desired = min(num_available_cpus, backlog_size)
          │
          ├── num_usable < desired?
          │     │
          │     ├── YES → PrestartWorkersInternal(lease_spec, desired - num_usable)
          │     │           │
          │     │           ├── 有 runtime_env → GetOrCreateRuntimeEnv() 后启动
          │     │           │
          │     │           └── 无 runtime_env → 直接 StartWorkerProcess()
          │     │
          │     └── NO → 当前 Worker 数量足够，不额外启动
          │
          └── 并发控制: starting_workers < maximum_startup_concurrency_
```

### 预创建与空闲池保活策略

```
预创建 Worker 的完整生命周期：

  PrestartWorkers()
       │
       ▼
  StartWorkerProcess()
       │ fork() + execvpe()
       ▼
  Worker 进程启动、注册
       │
       ▼
  PushWorker() → idle 池队头  ←── 从未执行过 Task，插入队头
       │
       │  keep_alive_until = now + worker_startup_keep_alive_duration
       │  (在保活期内，TryKillingIdleWorkers 不会杀它)
       │
       │  ┌─────────────────────────────────────────────┐
       │  │  保活期内：                                    │
       │  │    即使 idle_workers > num_cpus               │
       │  │    也不会被 TryKillingIdleWorkers 杀死         │
       │  │                                               │
       │  │  保活期过后：                                   │
       │  │    如果仍未被使用 → 可被杀死                    │
       │  │    杀死顺序: FIFO（它在队头，优先被杀）         │
       │  └─────────────────────────────────────────────┘
       │
       ├── [Case A] PopWorker() 匹配到 → 从 idle 池取出 → 执行 Task
       │     Task 完成后:
       │     PushWorker() → 放入 idle 池队尾（已执行过 Task，变热了）
       │
       └── [Case B] 一直没被用 → 保活期过后被 TryKillingIdleWorkers 杀死

关键参数:
  ┌──────────────────────────────────────────────────────────────┐
  │ enable_worker_prestart          → 是否在 Raylet 启动时预创建  │
  │ num_prestarted_python_workers   → 启动时预创建的 Python Worker │
  │ maximum_startup_concurrency_    → 并发启动上限（通常 = CPU 数）  │
  │ idle_worker_killing_time_threshold_ms → 空闲保活阈值         │
  │ kill_idle_workers_interval_ms   → 清理定时器周期              │
  │ worker_register_timeout_seconds → Worker 注册超时             │
  └──────────────────────────────────────────────────────────────┘
```

### 5.1 启动时预创建

**文件**: `src/ray/raylet/worker_pool.cc:178`

```cpp
void WorkerPool::Start() {
  // 定期杀死多余的空闲 Worker
  periodical_runner_->RunFnPeriodically(
      [this] { TryKillingIdleWorkers(); },
      RayConfig::instance().kill_idle_workers_interval_ms(), ...);

  // Raylet 启动时预创建 Worker
  if (RayConfig::instance().enable_worker_prestart()) {
    LeaseSpecification lease_spec{...};  // Python, 无 runtime_env
    PrestartWorkersInternal(lease_spec, num_prestart_python_workers);
  }
}
```

### 5.2 请求驱动预创建

**文件**: `src/ray/raylet/worker_pool.cc:1492`

每当 Raylet 收到 `RequestWorkerLease` 时，根据 backlog_size 预测性启动更多 Worker：

```cpp
void WorkerPool::PrestartWorkers(const LeaseSpecification &lease_spec,
                                 int64_t backlog_size) {
  int64_t num_available_cpus = get_num_cpus_available_();
  auto &state = GetStateForLanguage(lease_spec.GetLanguage());

  // 计算当前可用的 Worker 数量（空闲 + 正在启动的）
  int num_usable_workers = state.idle.size();
  for (auto &entry : state.worker_processes) {
    num_usable_workers += entry.second.is_pending_registration ? 1 : 0;
  }

  // 期望的可用 Worker 数 = min(可用CPU数, 积压任务数)
  auto desired_usable_workers = std::min<int64_t>(num_available_cpus, backlog_size);

  if (num_usable_workers < desired_usable_workers) {
    int64_t num_needed = desired_usable_workers - num_usable_workers;
    PrestartWorkersInternal(lease_spec, num_needed);
  }
}
```

**文件**: `src/ray/raylet/worker_pool.cc:1525`

```cpp
void WorkerPool::PrestartWorkersInternal(const LeaseSpecification &lease_spec,
                                         int64_t num_needed) {
  for (int ii = 0; ii < num_needed; ++ii) {
    if (IsRuntimeEnvEmpty(lease_spec.SerializedRuntimeEnv())) {
      // 无 runtime env：直接启动
      PopWorkerStatus status;
      StartWorkerProcess(lease_spec.GetLanguage(), rpc::WorkerType::WORKER,
                         lease_spec.JobId(), &status);
    } else {
      // 有 runtime env：先创建 runtime env 再启动
      GetOrCreateRuntimeEnv(lease_spec.SerializedRuntimeEnv(), ...,
          [this, lease_spec](bool successful, const std::string &context, ...) {
            if (successful) {
              PopWorkerStatus status;
              StartWorkerProcess(lease_spec.GetLanguage(), ...,
                                 lease_spec.GetRuntimeEnvHash(), context, ...);
            }
          });
    }
  }
}
```

### 5.3 空闲 Worker 的保活策略

预创建的 Worker 放入空闲池后有保活时间：

```cpp
// worker_pool.cc:1124-1145 (PushWorker 中)
absl::Time keep_alive_until =
    now + absl::Milliseconds(
        RayConfig::instance().idle_worker_killing_time_threshold_ms());

if (worker->GetGrantedLeaseTime() == absl::Time()) {
  // 从未执行过 Task 的 Worker → 队头（最先被杀）
  // 可能有 worker_startup_keep_alive_duration 保护
  idle_of_all_languages_.emplace_front(IdleWorkerEntry{worker, keep_alive_until});
} else {
  // 曾经执行过 Task 的 Worker → 队尾（尽量保留）
  idle_of_all_languages_.emplace_back(IdleWorkerEntry{worker, keep_alive_until});
}
```

空闲 Worker 保留上限 = `get_num_cpus_available_()`（节点 CPU 数量），超过则按 FIFO 杀死。

---

## 六、跨语言 Worker 复用

不同语言的 Worker 进程**绝对不可以复用**。这是由 Worker 的本质决定的：Worker 进程就是特定语言运行时的实例，一个 Python 解释器不可能执行 Java 字节码。

### 跨语言隔离架构

```
WorkerPool
┌──────────────────────────────────────────────────────────────────────┐
│                                                                      │
│  states_by_lang_ (按语言分片的独立状态)                                 │
│                                                                      │
│  ┌─── Language::PYTHON ──────────────────────────────────────────┐   │
│  │  State {                                                      │   │
│  │    worker_command: ["python", "default_worker.py", ...]      │   │
│  │    idle: {W1, W3, W7}          // 空闲 Python Worker         │   │
│  │    registered_workers: {W1, W2, W3, W4, W5, W6, W7}         │   │
│  │    worker_processes: {wid→ProcessInfo, ...}                  │   │
│  │    pending_registration_requests: [req1, req2]               │   │
│  │    pending_start_requests: [req3]                            │   │
│  │  }                                                            │   │
│  └───────────────────────────────────────────────────────────────┘   │
│                                                                      │
│  ┌─── Language::JAVA ────────────────────────────────────────────┐   │
│  │  State {                                                      │   │
│  │    worker_command: ["java", "-cp", ..., "io.ray.runtime..."] │   │
│  │    idle: {J1}                  // 空闲 Java Worker           │   │
│  │    registered_workers: {J1, J2}                              │   │
│  │    worker_processes: {wid→ProcessInfo, ...}                  │   │
│  │  }                                                            │   │
│  └───────────────────────────────────────────────────────────────┘   │
│                                                                      │
│  ┌─── Language::CPP ─────────────────────────────────────────────┐   │
│  │  State {                                                      │   │
│  │    worker_command: ["./my_binary", ...]                       │   │
│  │    idle: {}                    // 空闲 C++ Worker             │   │
│  │    registered_workers: {C1}                                  │   │
│  │  }                                                            │   │
│  └───────────────────────────────────────────────────────────────┘   │
│                                                                      │
│  idle_of_all_languages_ (跨语言有序列表 — 仅用于统一管理杀死策略)        │
│  ┌────┐  ┌────┐  ┌────┐  ┌────┐                                    │
│  │ J1 │──│ W1 │──│ W3 │──│ W7 │   ← 查找时仍检查语言匹配            │
│  └────┘  └────┘  └────┘  └────┘                                    │
│  队头(冷)               队尾(热)                                     │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘

匹配过程 (PopWorker 请求 Python Worker):
  idle_of_all_languages_: [J1, W1, W3, W7]
                                          ↑ 从队尾开始查找 (LIFO)
  W7: language==PYTHON? ✓ → 其他条件检查 → 匹配! → 取出 W7

匹配过程 (PopWorker 请求 Java Worker):
  idle_of_all_languages_: [J1, W1, W3]  (W7 已取出)
                                    ↑ 从队尾开始
  W3: language==JAVA? ✗ → 跳过
  W1: language==JAVA? ✗ → 跳过
  J1: language==JAVA? ✓ → 其他条件检查 → 匹配! → 取出 J1
```

### 不同语言 Worker 的进程本质

```
Python Worker 进程:
  PID=12345  python default_worker.py --worker-id=xxx ...
  ┌──────────────────────────────────────────┐
  │  Python 解释器 (CPython)                  │
  │    ├── CoreWorker (C++, via Cython)      │
  │    ├── FunctionManager (动态加载)         │
  │    ├── import module → getattr(func)     │
  │    └── pickle.loads(bytes) → function    │
  └──────────────────────────────────────────┘

Java Worker 进程:
  PID=12346  java -cp ... io.ray.runtime.runner.RunManager
  ┌──────────────────────────────────────────┐
  │  JVM (Java Virtual Machine)              │
  │    ├── CoreWorker (C++, via JNI)         │
  │    ├── Class.forName() → Method.invoke() │
  │    └── Java 反射调用用户代码              │
  └──────────────────────────────────────────┘

C++ Worker 进程:
  PID=12347  ./compiled_binary --worker-id=xxx ...
  ┌──────────────────────────────────────────┐
  │  原生二进制                                │
  │    ├── CoreWorker (C++, 直接链接)         │
  │    ├── dlopen() → dlsym() 动态符号查找    │
  │    └── 直接调用编译好的函数指针            │
  └──────────────────────────────────────────┘

共同点: 所有语言的 Worker 都内嵌 CoreWorker (C++)
不同点: 运行时环境完全不同，进程镜像不可互换
```

### 6.1 不同语言之间无法复用

**不同语言的 Worker 进程绝对不可以复用。**

**文件**: `src/ray/raylet/worker_pool.cc:1276`

```cpp
WorkerUnfitForLeaseReason WorkerPool::WorkerFitForLease(...) const {
  // 语言不匹配 → 直接拒绝（这是第一个检查条件）
  if (worker.GetLanguage() != pop_worker_request.language_) {
    return WorkerUnfitForLeaseReason::OTHERS;
  }
  // ...
}
```

根本原因：Worker 进程本身就是特定语言的运行时实例：

```
Python Worker = python default_worker.py    → Python 解释器进程
Java Worker   = java io.ray.runtime...      → JVM 进程
C++ Worker    = 编译好的二进制               → 原生进程

一个 Python 进程无法执行 Java 字节码，反之亦然。
```

### 6.2 每种语言独立的 Worker Pool State

**文件**: `src/ray/raylet/worker_pool.h:650-686`

```cpp
struct State {
  std::vector<std::string> worker_command;           // 该语言的启动命令模板
  std::unordered_set<std::shared_ptr<WorkerInterface>> idle;  // 空闲 Worker 池
  IOWorkerState spill_io_worker_state;
  IOWorkerState restore_io_worker_state;
  std::unordered_set<std::shared_ptr<WorkerInterface>> registered_workers;  // 已注册
  std::unordered_set<std::shared_ptr<WorkerInterface>> registered_drivers;  // Driver
  absl::flat_hash_map<WorkerID, WorkerProcessInfo> worker_processes;  // 进程信息
  std::deque<std::shared_ptr<PopWorkerRequest>> pending_registration_requests;  // 等注册
  std::deque<std::shared_ptr<PopWorkerRequest>> pending_start_requests;  // 等启动
};

/// 每种语言独立的 State
absl::flat_hash_map<Language, State, std::hash<int>> states_by_lang_;

/// 跨语言的空闲列表（仅用于 FIFO 杀死策略，查找时仍检查语言匹配）
std::list<IdleWorkerEntry> idle_of_all_languages_;
```

`WorkerPool` 构造函数中按语言初始化：

```cpp
// worker_pool.cc:137-142
for (const auto &entry : worker_commands) {
  auto &state = states_by_lang_[entry.first];   // entry.first = Language enum
  state.multiple_for_warning = maximum_startup_concurrency_;
  state.worker_command = entry.second;           // 该语言的命令模板
}
```

---

## 七、Worker 进程退出的所有触发时机

Worker 进程的退出是整个生命周期中最复杂的部分，共有 **11 种不同的触发路径**，分为 Raylet 端主动杀死和 Worker 端自主退出两大类。理解这些路径对于排查 Worker 泄漏、资源未释放和意外退出问题至关重要。

### 退出决策树

```
Worker 退出触发
    │
    ├──── [Raylet 端主动] ──────────────────────────────────────────────────┐
    │                                                                       │
    │  ┌─ 优雅退出 (Exit RPC) ─────────────────────────────────────────┐   │
    │  │                                                                │   │
    │  │  [1] 空闲超时回收                                               │   │
    │  │      TryKillingIdleWorkers → KillIdleWorker                    │   │
    │  │      → Exit RPC (force=false, Job未结束时)                     │   │
    │  │      → Worker 可拒绝 (持有 object ref 时)                      │   │
    │  │      → 拒绝: 放回队尾稍后重试                                   │   │
    │  │                                                                │   │
    │  │  [2] Job 结束                                                   │   │
    │  │      HandleJobFinished → Exit RPC (force=true)                 │   │
    │  │      → Worker 无法拒绝                                          │   │
    │  │      → RPC 失败: 降级为 SIGKILL                                 │   │
    │  │                                                                │   │
    │  └────────────────────────────────────────────────────────────────┘   │
    │                                                                       │
    │  ┌─ 强制退出 (DestroyWorker / KillAsync) ────────────────────────┐   │
    │  │                                                                │   │
    │  │  [3] OOM                                                       │   │
    │  │      Memory Monitor → DestroyWorker(force=true)                │   │
    │  │      → 直接 SIGKILL (不等 SIGTERM 超时)                         │   │
    │  │                                                                │   │
    │  │  [4] Owner 死亡 (节点/Worker)                                   │   │
    │  │      NodeRemoved / HandleUnexpectedWorkerFailure                │   │
    │  │      → KillAsync() → SIGTERM → 超时 → SIGKILL                  │   │
    │  │      ※ detached actor 不受影响                                  │   │
    │  │                                                                │   │
    │  │  [5] Placement Group 移除                                       │   │
    │  │      HandleReturnBundle / HandleReleaseUnusedBundles            │   │
    │  │      → DestroyWorker(INTENDED_SYSTEM_EXIT)                     │   │
    │  │                                                                │   │
    │  │  [6] GCS 释放未使用 Actor                                       │   │
    │  │      HandleReleaseUnusedActorWorkers                           │   │
    │  │      → DestroyWorker(INTENDED_SYSTEM_EXIT)                     │   │
    │  │                                                                │   │
    │  │  [7] GCS 杀死 Actor (ray.kill)                                  │   │
    │  │      HandleKillActor → KillActor RPC → 超时 DestroyWorker     │   │
    │  │      两阶段: 先 RPC 优雅 → 超时后 SIGKILL                       │   │
    │  │                                                                │   │
    │  │  [8] ray.cancel(force=True)                                     │   │
    │  │      HandleCancelLocalTask → CancelTask RPC → 超时 DestroyWorker│  │
    │  │      两阶段: 先 RPC 优雅 → 超时后 SIGKILL                       │   │
    │  │                                                                │   │
    │  │  [9] Worker 异常断连                                            │   │
    │  │      CheckForUnexpectedWorkerDisconnects                       │   │
    │  │      → DestroyWorker(SYSTEM_ERROR)                             │   │
    │  │                                                                │   │
    │  │  [11] Worker 启动超时                                           │   │
    │  │       MonitorStartingWorkerProcess → proc.Kill()                │   │
    │  │       → 直接 SIGKILL                                            │   │
    │  │                                                                │   │
    │  └────────────────────────────────────────────────────────────────┘   │
    │                                                                       │
    └──── [Worker 端自主] ──────────────────────────────────────────────────┘
          │
          │  [10] Worker 自主退出
          │      ├── max_calls 达上限 → raise SystemExit
          │      ├── ray.actor.exit_actor() → CoreWorker::Exit
          │      ├── Python sys.exit() / SIGTERM handler → IntentionalSystemExit
          │      └── Worker 退出后 → Raylet 通过 socket 断连检测到
          │                         → DisconnectClient → 清理资源
```

### 两大退出机制对比

```
┌────────────────────────────────────────────────────────────────────────┐
│                     Exit RPC (优雅退出)                                  │
│                                                                        │
│  Raylet ──── Exit RPC ────→ Worker                                     │
│                                │                                       │
│                          HandleExit()                                  │
│                                │                                       │
│                    ┌──── is_idle? ────┐                                │
│                    │                  │                                │
│                  YES                 NO                                │
│                    │                  │                                │
│              ┌── force? ──┐    ┌── force? ──┐                         │
│              │            │    │            │                          │
│            YES          NO   YES          NO                          │
│              │            │    │            │                          │
│         强制退出     优雅退出  强制退出   拒绝退出                        │
│         reply=true  reply=true reply=true reply=false                  │
│              │            │    │            │                          │
│              ▼            ▼    ▼            ▼                          │
│        shutdown()   shutdown() shutdown() Raylet 放回队尾重试           │
│                                                                        │
│  适用于: 空闲 Worker 回收、Job 结束                                      │
│  特点: Worker 有拒绝权（非 force 模式下）                                │
└────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────┐
│                  DestroyWorker + KillAsync (强制退出)                    │
│                                                                        │
│  触发:                                                                  │
│    DestroyWorker(worker, exit_type, detail, force)                     │
│        │                                                               │
│        ├── DisconnectClient()    ← 先清理资源                           │
│        │     ├── 取消 ray.get/ray.wait                                 │
│        │     ├── 释放 lease + 归还资源                                  │
│        │     ├── 报告 GCS                                              │
│        │     └── 从 WorkerPool 移除                                    │
│        │                                                               │
│        └── KillAsync(force)      ← 再杀进程                            │
│              │                                                         │
│         ┌── force? ──┐                                                │
│         │            │                                                │
│       YES          NO                                                 │
│         │            │                                                │
│     SIGKILL     SIGTERM ──→ 等待 kill_worker_timeout_ms               │
│     (立即)        │                 │                                  │
│                   │          进程退出? ──┐                             │
│                   │            │         │                             │
│                   │          YES        NO                             │
│                   │            │         │                             │
│                   │          完成      SIGKILL                         │
│                   │                    (强制杀死)                       │
│                   └────────────────────────────┘                       │
│                                                                        │
│  适用于: OOM、异常断连、强制取消、PG 移除                                 │
│  特点: Worker 无法拒绝                                                   │
└────────────────────────────────────────────────────────────────────────┘
```

### 7.1 退出路径概览

Worker 进程退出有两大类路径：

1. **Raylet 端主动杀死**：Raylet 通过 `DestroyWorker()` 或 `KillIdleWorker()` 发起
2. **Worker 端自主退出**：Worker 内部通过 `CoreWorker::Exit()` 发起

底层杀死机制有两种：

```
方式1: Exit RPC（优雅退出）
  Raylet → 发送 Exit RPC 给 Worker → Worker 执行 HandleExit → 清理后退出
  适用于: 空闲 Worker 回收、Job 结束

方式2: DestroyWorker（强制退出）
  Raylet → DisconnectClient(清理资源) → worker->KillAsync()
  KillAsync: SIGTERM → 超时(kill_worker_timeout_ms) → SIGKILL
  适用于: OOM、异常断连、强制取消
```

**DestroyWorker 核心实现**：

**文件**: `src/ray/raylet/node_manager.cc:524`

```cpp
void NodeManager::DestroyWorker(std::shared_ptr<WorkerInterface> worker,
                                rpc::WorkerExitType disconnect_type,
                                const std::string &disconnect_detail,
                                bool force) {
  // 先断开连接、清理资源（释放 lease、归还资源）
  DisconnectClient(
      worker->Connection(), /*graceful=*/false, disconnect_type, disconnect_detail);
  // 再杀死进程
  worker->KillAsync(io_service_, force);
  if (disconnect_type == rpc::WorkerExitType::SYSTEM_ERROR) {
    number_workers_killed_++;
  } else if (disconnect_type == rpc::WorkerExitType::NODE_OUT_OF_MEMORY) {
    number_workers_killed_by_oom_++;
  }
}
```

### 7.2 路径一：空闲超时回收（TryKillingIdleWorkers）

**触发条件**: 定时器周期执行（`kill_idle_workers_interval_ms`），空闲 Worker 数量超过软上限（节点 CPU 数量）

**文件**: `src/ray/raylet/worker_pool.cc:1154`

```
定时器触发 TryKillingIdleWorkers()
  → 遍历 idle_of_all_languages_ 列表
  → 立即杀死: 已死亡的 Worker、已结束 Job 的 Worker
  → 软限制杀死: 可杀 Worker 数 > num_desired_idle_workers(CPU 数量)
    → 从队头开始杀（FIFO，最冷的先杀）
    → KillIdleWorker(): 发送 Exit RPC
```

**KillIdleWorker 发送 Exit RPC**:

**文件**: `src/ray/raylet/worker_pool.cc:1214`

```cpp
void WorkerPool::KillIdleWorker(const IdleWorkerEntry &entry) {
  pending_exit_idle_workers_.emplace(idle_worker->WorkerId(), idle_worker);
  rpc::ExitRequest request;
  // 如果 Job 已结束且不是 detached actor → 强制退出
  if (finished_jobs_.contains(job_id) && idle_worker->GetRootDetachedActorId().IsNil()) {
    request.set_force_exit(true);
  }
  rpc_client->Exit(request, [this, entry](...) {
    if (!status.ok() || r.success()) {
      // Worker 同意退出 → 标记为死亡，从 idle 池移除
      worker->MarkDead();
    } else {
      // Worker 拒绝退出（如它拥有 object 引用）→ 放回队尾稍后重试
      idle_of_all_languages_.push_back(entry);
    }
  });
}
```

**关键点**：Worker 可以拒绝 Exit RPC（当它持有 object 引用时），这时不会被强制杀死，而是放回队尾稍后重试。

### 7.3 路径二：Job 结束强制退出（HandleJobFinished）

**触发条件**: GCS 通知 Raylet 某个 Job 结束

**文件**: `src/ray/raylet/node_manager.cc:553`

```cpp
void NodeManager::HandleJobFinished(const JobID &job_id, const JobTableData &job_data) {
  // 遍历所有 leased workers
  for (const auto &pair : leased_workers_) {
    auto &worker = pair.second;
    // 属于该 Job 且不是 detached actor → 强制退出
    if (worker->GetRootDetachedActorId().IsNil() &&
        (worker->GetAssignedJobId() == job_id)) {
      rpc::ExitRequest request;
      request.set_force_exit(true);
      worker->rpc_client()->Exit(request, [this, worker](...) {
        if (!status.ok()) {
          // Exit RPC 失败 → 直接 SIGKILL
          worker->KillAsync(io_service_, /* force */ true);
        }
      });
    }
  }
  // 标记 Job 为已结束（后续 TryKillingIdleWorkers 会清理空闲 Worker）
  worker_pool_.HandleJobFinished(job_id);
}
```

**两阶段清理**：
1. **立即阶段**：遍历 `leased_workers_`，对属于该 Job 的 Worker 发送 force Exit RPC
2. **后续阶段**：`TryKillingIdleWorkers` 定期检查 `finished_jobs_` 集合，杀死空闲池中该 Job 的 Worker

### 7.4 路径三：OOM 内存不足被杀（Memory Monitor）

**触发条件**: 节点内存使用超过阈值（`memory_usage_threshold`），由 Memory Monitor 选择一个 Worker 杀死

**文件**: `src/ray/raylet/node_manager.cc:3090-3118`

```cpp
// Memory Monitor 回调中：
DestroyWorker(high_memory_eviction_target_,
              rpc::WorkerExitType::NODE_OUT_OF_MEMORY,
              worker_exit_message,
              true /* force */);  // ← 强制 SIGKILL
```

**特点**：
- 使用 `force=true`，直接 SIGKILL，不等 SIGTERM 超时
- `WorkerExitType::NODE_OUT_OF_MEMORY` 会被上报给 GCS 和 Dashboard
- 分别统计 Driver/Task/Actor 的 OOM 驱逐次数

### 7.5 路径四：Owner 死亡（节点/Worker 失败）

**触发条件**: Worker 的 owner 节点死亡 或 owner worker 死亡

#### 节点死亡

**文件**: `src/ray/raylet/node_manager.cc:936-949`

```cpp
void NodeManager::NodeRemoved(const NodeID &node_id) {
  // 遍历 leased workers，找到 owner 在死亡节点上的
  for (const auto &[_, worker] : leased_workers_) {
    const auto owner_node_id = NodeID::FromBinary(worker->GetOwnerAddress().node_id());
    if (worker->IsDetachedActor() || owner_node_id != node_id) {
      continue;  // detached actor 不受影响
    }
    // Owner 节点死亡 → 杀死 Worker
    worker->KillAsync(io_service_);  // SIGTERM → 超时 → SIGKILL
  }
}
```

#### Owner Worker 死亡

**文件**: `src/ray/raylet/node_manager.cc:973-993`

```cpp
void NodeManager::HandleUnexpectedWorkerFailure(const WorkerID &worker_id) {
  for (const auto &[_, worker] : leased_workers_) {
    const auto owner_worker_id =
        WorkerID::FromBinary(worker->GetOwnerAddress().worker_id());
    if (worker->IsDetachedActor() || owner_worker_id != worker_id) {
      continue;  // detached actor 不受影响
    }
    // Owner worker 死亡 → 杀死被 own 的 Worker
    worker->KillAsync(io_service_);
  }
}
```

**关键点**：`IsDetachedActor()` 的 Worker 不会因 owner 死亡而被杀，因为 detached actor 的生命周期独立于 driver。

### 7.6 路径五：Placement Group 移除

**触发条件**: Placement Group 被删除，或 GCS 重启后 bundle 不再注册

**文件**: `src/ray/raylet/node_manager.cc:1958-1976`

```cpp
// HandleReturnBundle 中：
for (const auto &worker : workers_associated_with_pg) {
  DestroyWorker(worker, rpc::WorkerExitType::INTENDED_SYSTEM_EXIT,
                "Destroying worker since its placement group was removed.");
}
```

**文件**: `src/ray/raylet/node_manager.cc:644-670`

```cpp
// HandleReleaseUnusedBundles 中（GCS 重启后调用）：
for (const auto &worker : workers_associated_with_unused_bundles) {
  DestroyWorker(worker, rpc::WorkerExitType::INTENDED_SYSTEM_EXIT,
                "Worker exits because it uses placement group bundles that are not "
                "registered to GCS. It can happen upon GCS restart.");
}
```

### 7.7 路径六：GCS 请求释放未使用的 Actor Worker

**触发条件**: GCS 发现某些 Actor Worker 不再需要

**文件**: `src/ray/raylet/node_manager.cc:2209-2233`

```cpp
void NodeManager::HandleReleaseUnusedActorWorkers(...) {
  for (auto &iter : leased_workers_) {
    // Actor Worker 不在 GCS 的 in-use 列表中
    if (!iter.second->GetActorId().IsNil() &&
        !in_use_worker_ids.contains(iter.second->WorkerId())) {
      unused_actor_workers.push_back(iter.second);
    }
  }
  for (auto &worker : unused_actor_workers) {
    DestroyWorker(worker, rpc::WorkerExitType::INTENDED_SYSTEM_EXIT,
                  "Worker is no longer needed by the GCS.");
  }
}
```

### 7.8 路径七：GCS 请求杀死 Actor（KillActor）

**触发条件**: `ray.kill(actor)` 或 GCS 判定 Actor 需要被杀死

**文件**: `src/ray/raylet/node_manager.cc:3380-3441`

```cpp
void NodeManager::HandleKillActor(...) {
  // 先通过 RPC 通知 Worker 杀死 Actor
  worker->rpc_client()->KillActor(kill_actor_request, ...);

  // 设置超时：如果 Worker 没有在 kill_worker_timeout_ms 内退出
  auto timer = execute_after(io_service_, [this, worker_id, ...]() {
    auto current_worker = worker_pool_.GetRegisteredWorker(worker_id);
    if (current_worker) {
      // 超时 → 强制杀死
      DestroyWorker(current_worker,
                    rpc::WorkerExitType::INTENDED_SYSTEM_EXIT,
                    "Actor killed by GCS",
                    /*force=*/true);  // SIGKILL
    }
  }, kill_worker_timeout_milliseconds);
}
```

**两阶段**：先发 KillActor RPC 让 Worker 优雅退出，超时后 `DestroyWorker(force=true)` 强杀。

### 7.9 路径八：ray.cancel(force=True) 强制取消

**触发条件**: 用户调用 `ray.cancel(object_ref, force=True)`

**文件**: `src/ray/raylet/node_manager.cc:3471-3497`

```cpp
void NodeManager::HandleCancelLocalTask(...) {
  if (request.force_kill()) {
    // 设置超时定时器
    timer = execute_after(io_service_, [this, worker_id, ...]() {
      auto current_worker = worker_pool_.GetRegisteredWorker(worker_id);
      if (current_worker) {
        DestroyWorker(current_worker,
                      rpc::WorkerExitType::INTENDED_SYSTEM_EXIT,
                      "Force-killed by ray.cancel(force=True)",
                      /*force=*/true);
      }
    }, kill_worker_timeout_milliseconds);
  }
  // 先尝试通过 CancelTask RPC 优雅取消
  worker->rpc_client()->CancelTask(cancel_task_request, ...);
}
```

### 7.10 路径九：Worker 异常断连

**触发条件**: Worker 进程 crash、网络中断等导致连接断开

**文件**: `src/ray/raylet/node_manager.cc:587-611`

```cpp
void NodeManager::CheckForUnexpectedWorkerDisconnects() {
  // 检查所有 Worker 的 socket 连接是否断开
  std::vector<bool> disconnects = CheckForClientDisconnects(all_connections);
  for (size_t i = 0; i < disconnects.size(); i++) {
    if (disconnects[i]) {
      DestroyWorker(all_workers[i], rpc::WorkerExitType::SYSTEM_ERROR,
                    "Worker connection closed unexpectedly.");
    }
  }
}
```

此外，Worker 主动断连（如 `sys.exit()`）会走 `DisconnectClient` 路径：

**文件**: `src/ray/raylet/node_manager.cc:1404`

```cpp
void NodeManager::DisconnectClient(const std::shared_ptr<ClientConnection> &client,
                                   bool graceful, ...) {
  // 1. 取消 ray.get / ray.wait
  lease_dependency_manager_.CancelGetRequest(worker->WorkerId());
  // 2. 释放 lease 和资源
  if (leased_workers_.contains(worker->GetGrantedLeaseId())) {
    ReleaseWorker(worker->GetGrantedLeaseId());
  }
  // 3. 向 GCS 报告 Worker 失败
  gcs_client_.Workers().AsyncReportWorkerFailure(worker_failure_data_ptr, nullptr);
  // 4. 从 WorkerPool 中移除
  worker_pool_.DisconnectWorker(worker, disconnect_type);
}
```

### 7.11 路径十：Worker 自主退出（max_calls / 信号 / sys.exit）

这些退出是 Worker 进程内部发起的，不经过 Raylet 的 `DestroyWorker`。

#### max_calls 达到上限

**文件**: `python/ray/_raylet.pyx:2182-2191`

```cython
if execution_info.max_calls != 0:
    manager.increase_task_counter(function_descriptor)
    task_counter = manager.get_task_counter(function_descriptor)
    if task_counter == execution_info.max_calls:
        raise_sys_exit_with_custom_error_message(
            f"Exited because worker reached max_calls={execution_info.max_calls}"
            " for this method.")
```

当用 `@ray.remote(max_calls=N)` 装饰的函数执行次数达到 N 次时，Worker 通过 `raise SystemExit` 退出。

#### 信号检查（RunTaskExecutionLoop 中）

**文件**: `src/ray/core_worker/core_worker.cc:2713-2738`

```cpp
void CoreWorker::RunTaskExecutionLoop() {
  signal_checker->RunFnPeriodically([this] {
    // 检查 Actor 是否应该退出（ray.actor.exit_actor()）
    if (worker_context_->GetCurrentActorShouldExit()) {
      Exit(rpc::WorkerExitType::INTENDED_USER_EXIT,
           "User requested to exit the actor.", nullptr);
    }
    // 检查 Python 信号（如 KeyboardInterrupt）
    auto status = options_.check_signals();
    if (status.IsIntentionalSystemExit()) {
      Exit(rpc::WorkerExitType::INTENDED_USER_EXIT, ...);
    }
    if (status.IsUnexpectedSystemExit()) {
      Exit(rpc::WorkerExitType::SYSTEM_ERROR, ...);
    }
  }, 10 /*ms*/, "CoreWorker.CheckSignal");
}
```

#### CoreWorker::Exit 实现

**文件**: `src/ray/core_worker/core_worker.cc:657`

```cpp
void CoreWorker::Exit(const rpc::WorkerExitType exit_type,
                      const std::string &detail, ...) {
  ShutdownReason reason = ...;
  shutdown_coordinator_->RequestShutdown(
      /*force_shutdown=*/false, reason, detail,
      ShutdownCoordinator::kInfiniteTimeout, ...);
}
```

Worker 自主退出后，Raylet 通过 socket 断连检测到 Worker 死亡，触发 `DisconnectClient` 清理资源。

### 7.12 路径十一：Worker 启动超时

**触发条件**: Worker 进程已 fork 但未在 `worker_register_timeout_seconds` 内完成注册

**文件**: `src/ray/raylet/worker_pool.cc:566-608`

```cpp
void WorkerPool::MonitorStartingWorkerProcess(const WorkerID &worker_id, ...) {
  auto timer = std::make_shared<boost::asio::deadline_timer>(
      *io_service_,
      boost::posix_time::seconds(
          RayConfig::instance().worker_register_timeout_seconds()));
  timer->async_wait([...](const boost::system::error_code e) {
    auto it = state.worker_processes.find(worker_id);
    if (it != state.worker_processes.end() && it->second.is_pending_registration) {
      // Worker 启动超时，未完成注册
      if (it->second.proc.IsAlive()) {
        it->second.proc.Kill();  // 直接 Kill
      }
      RemoveWorkerProcess(state, worker_id);
      starting_worker_timeout_callback_();
    }
  });
}
```

### 7.13 Worker 端 Exit RPC 处理（HandleExit）

当 Raylet 向 Worker 发送 Exit RPC 时，Worker 端的处理逻辑：

**文件**: `src/ray/core_worker/core_worker.cc:4373-4414`

```cpp
void CoreWorker::HandleExit(rpc::ExitRequest request,
                            rpc::ExitReply *reply,
                            rpc::SendReplyCallback send_reply_callback) {
  bool is_idle = IsIdle();
  bool force_exit = request.force_exit();

  // 判断是否退出：空闲 或 强制退出
  const bool will_exit = is_idle || force_exit;
  reply->set_success(will_exit);  // ← 告诉 Raylet 是否同意退出

  send_reply_callback(Status::OK(), [this, will_exit, force_exit]() {
    if (!will_exit) {
      return;  // Worker 不空闲且非强制 → 拒绝退出
    }
    ShutdownReason reason;
    if (force_exit) {
      reason = ShutdownReason::kForcedExit;
      detail = "Worker force exited because its job has finished";
    } else {
      reason = ShutdownReason::kIdleTimeout;
      detail = "Worker exited because it was idle for a long time";
    }
    shutdown_coordinator_->RequestShutdown(force_exit, reason, detail);
  }, ...);
}
```

**关键行为**：
- `is_idle && !force_exit` → 同意退出，优雅关闭
- `!is_idle && !force_exit` → **拒绝退出**（`reply->set_success(false)`），Worker 持有 object 引用时会走这个分支
- `force_exit` → 无论是否空闲，强制退出

### 7.14 退出路径总结表

| 退出路径 | 触发者 | 退出方式 | ExitType | Force | 文件:行号 |
|---------|--------|---------|----------|-------|----------|
| 空闲超时 | WorkerPool 定时器 | Exit RPC | - | 否(Job结束时是) | worker_pool.cc:1214 |
| Job 结束 | GCS→Raylet | Exit RPC (force) | - | 是 | node_manager.cc:553 |
| OOM | Memory Monitor | DestroyWorker | NODE_OUT_OF_MEMORY | 是(SIGKILL) | node_manager.cc:3115 |
| Owner 节点死亡 | NodeRemoved 回调 | KillAsync | - | 否 | node_manager.cc:948 |
| Owner Worker 死亡 | Worker 失败回调 | KillAsync | - | 否 | node_manager.cc:992 |
| PG 移除 | GCS→Raylet | DestroyWorker | INTENDED_SYSTEM_EXIT | 否 | node_manager.cc:1975 |
| PG Bundle 未注册 | GCS 重启→Raylet | DestroyWorker | INTENDED_SYSTEM_EXIT | 否 | node_manager.cc:666 |
| GCS 释放未用 Actor | GCS→Raylet | DestroyWorker | INTENDED_SYSTEM_EXIT | 否 | node_manager.cc:2230 |
| GCS 杀死 Actor | GCS→Raylet | KillActor RPC + 超时 DestroyWorker | INTENDED_SYSTEM_EXIT | 是(超时后) | node_manager.cc:3407 |
| ray.cancel(force) | 用户→Raylet | CancelTask RPC + 超时 DestroyWorker | INTENDED_SYSTEM_EXIT | 是(超时后) | node_manager.cc:3485 |
| Worker 异常断连 | 连接检测 | DestroyWorker | SYSTEM_ERROR | 否 | node_manager.cc:609 |
| max_calls 达上限 | Worker 内部 | raise SystemExit | INTENDED_USER_EXIT | - | _raylet.pyx:2188 |
| ray.actor.exit_actor() | Worker 内部 | CoreWorker::Exit | INTENDED_USER_EXIT | - | core_worker.cc:2720 |
| Python sys.exit() | Worker 内部 | IntentionalSystemExit | INTENDED_USER_EXIT | - | _raylet.pyx:2325 |
| Worker 启动超时 | WorkerPool 定时器 | proc.Kill() | - | 是(SIGKILL) | worker_pool.cc:590 |

---

## 八、核心数据结构汇总

### WorkerProcessInfo

**文件**: `src/ray/raylet/worker_pool.h:632`

```cpp
struct WorkerProcessInfo {
  bool is_pending_registration = true;  // 是否等待注册
  rpc::WorkerType worker_type;          // Worker 类型
  Process proc;                         // OS 进程句柄
  std::chrono::high_resolution_clock::time_point start_time;  // 启动时间
  rpc::RuntimeEnvInfo runtime_env_info; // Runtime Env 信息
  std::vector<std::string> dynamic_options;  // 动态选项
  std::optional<absl::Duration> worker_startup_keep_alive_duration;  // 保活时长
};
```

### PopWorkerRequest

包含查找 Worker 所需的所有匹配条件：

```
- language_             : 语言
- worker_type_          : Worker 类型
- job_id_               : Job ID
- root_detached_actor_id_ : Root Detached Actor ID
- runtime_env_hash_     : Runtime Env 哈希
- dynamic_options_      : 动态选项
- is_gpu_               : 是否 GPU Worker
- is_actor_worker_      : 是否 Actor Worker
- callback_             : 获取到 Worker 后的回调
```

### FunctionDescriptor（Protobuf）

```protobuf
// Python
message PythonFunctionDescriptor {
  string module_name = 1;     // 模块路径
  string class_name = 2;      // 类名（Actor）
  string function_name = 3;   // 函数名
  string function_hash = 4;   // 唯一标识
}

// Java
message JavaFunctionDescriptor {
  string class_name = 1;
  string function_name = 2;
  string signature = 3;
}

// C++
message CppFunctionDescriptor {
  string function_name = 1;
  string caller = 2;
  string class_name = 3;
}
```

### FunctionExecutionInfo

**文件**: `python/ray/_private/function_manager.py:39`

```python
FunctionExecutionInfo = namedtuple(
    "FunctionExecutionInfo",
    ["function", "function_name", "max_calls"]
)
```

---

## 九、关键文件索引

### C++ 核心

| 文件 | 作用 |
|------|------|
| `src/ray/raylet/worker_pool.h` | WorkerPool 类定义、State 结构、枚举 |
| `src/ray/raylet/worker_pool.cc` | Worker 生命周期管理核心实现 |
| `src/ray/raylet/worker_interface.h` | Worker 抽象接口 |
| `src/ray/raylet/worker.h/cc` | Worker 类（状态标志、KillAsync） |
| `src/ray/raylet/node_manager.cc` | NodeManager：处理 lease 请求、Worker 断连 |
| `src/ray/raylet/scheduling/local_lease_manager.cc` | 本地 lease 管理（Grant、Release） |
| `src/ray/raylet/scheduling/local_resource_manager.cc` | 逻辑资源分配/释放 |
| `src/ray/core_worker/core_worker.cc` | CoreWorker：HandlePushTask、ExecuteTask、RunTaskExecutionLoop |
| `src/ray/util/process.cc` | fork+exec 封装（spawnvpe） |
| `src/ray/common/function_descriptor.h` | FunctionDescriptor C++ 类（Python/Java/C++） |
| `src/ray/protobuf/common.proto` | TaskSpec、FunctionDescriptor、WorkerType protobuf 定义 |

### Cgroup

| 文件 | 作用 |
|------|------|
| `src/ray/common/cgroup2/cgroup_manager_interface.h` | 抽象接口 + 约束定义 |
| `src/ray/common/cgroup2/cgroup_manager.h/cc` | 具体实现（创建层次、设置约束） |
| `src/ray/common/cgroup2/sysfs_cgroup_driver.h/cc` | Linux 文件系统操作 |
| `src/ray/common/cgroup2/linux_cgroup_manager_factory.cc` | Linux 工厂 |
| `src/ray/common/cgroup2/noop_cgroup_manager.h` | 非 Linux 空操作 |
| `src/ray/raylet/main.cc:282-302` | cgroup hook 注入 |

### Python

| 文件 | 作用 |
|------|------|
| `python/ray/_private/workers/default_worker.py` | Worker 进程入口 |
| `python/ray/_private/workers/setup_worker.py` | Runtime Env 引导程序 |
| `python/ray/_private/worker.py` | Worker 类、connect()、main_loop() |
| `python/ray/_private/function_manager.py` | 函数导出/导入/缓存 |
| `python/ray/_raylet.pyx` | Cython 桥接：task_execution_handler、execute_task |
| `python/ray/remote_function.py` | @ray.remote 装饰器实现 |
| `python/ray/_private/services.py:1543-1770` | Worker 命令模板构建 |
| `python/ray/_private/resource_isolation_config.py` | cgroup 配置验证 |
| `python/ray/_private/accelerators/nvidia_gpu.py` | GPU 环境变量设置 |

---

## 十、架构设计总结与核心洞察

### Worker 生命周期端到端全景

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Worker 完整生命周期端到端                               │
│                                                                             │
│  用户代码                                                                    │
│  ═══════                                                                    │
│  @ray.remote                                                                │
│  def my_func(x):        ray.remote(my_func).remote(42)                     │
│      return x + 1       ─────────────────────────────────────               │
│                                │                                            │
│  Driver 端                      │                                            │
│  ═══════════                    ▼                                            │
│  1. 构建 PythonFunctionDescriptor                                            │
│     {module:"__main__", function:"my_func", hash:"a1b2c3"}                 │
│  2. pickle.dumps(my_func) → GCS Internal KV                                │
│  3. 构建 TaskSpec (只含描述符，不含代码)                                      │
│  4. 提交到调度器                                                             │
│                                │                                            │
│  调度层                         │                                            │
│  ══════                         ▼                                            │
│  5. GCS/Scheduler 选择目标节点                                               │
│  6. RequestWorkerLease → 目标 Raylet                                        │
│                                │                                            │
│  Raylet 端                      │                                            │
│  ═════════                      ▼                                            │
│  7. PrestartWorkers()          (根据 backlog 预启动)                         │
│  8. PopWorker()                                                              │
│     ├── FindAndPopIdleWorker() (10 条件匹配, LIFO)                          │
│     │     ├── 找到 → 跳到 [11]                                               │
│     │     └── 未找到 ↓                                                       │
│     └── StartNewWorker()                                                     │
│           ├── GetOrCreateRuntimeEnv() (如有)                                 │
│           └── StartWorkerProcess()                                           │
│                 ├── BuildProcessCommandArgs()                                │
│                 ├── fork() + execvpe()                                       │
│                 │     子进程: add_to_cgroup → setpgrp → exec                │
│                 ├── AdjustWorkerOomScore()                                   │
│                 └── MonitorStartingWorkerProcess()                           │
│                                │                                            │
│  Worker 进程                    │                                            │
│  ═══════════                    ▼                                            │
│  9.  python default_worker.py                                               │
│      → Node(connect_only=True)                                              │
│      → ray._private.worker.connect()                                        │
│      → CoreWorker(C++) 初始化                                                │
│        ├── 连接 Raylet (gRPC + IPC)                                         │
│        ├── 连接 Object Store (shared memory)                                │
│        ├── 启动自身 gRPC Server                                              │
│        └── 向 GCS 注册                                                       │
│  10. RegisterClient → AnnounceWorkerPort → 注册完成                          │
│      → PushWorker() → idle 池                                               │
│                                │                                            │
│  Task 执行                      │                                            │
│  ════════                       ▼                                            │
│  11. PopWorker() 匹配成功 → Grant()                                          │
│      → SetAllocatedInstances(资源)                                           │
│      → PushTask gRPC → HandlePushTask                                       │
│  12. ExecuteTask (C++)                                                       │
│      → task_execution_callback → Cython                                     │
│  13. task_execution_handler (Cython, with GIL)                              │
│      → 函数查找 (3级: 缓存 → import → GCS pickle)                           │
│      → execute_task                                                          │
│  14. function_executor(*args, **kwargs)  ← 真正执行 my_func(42)             │
│      → outputs = 43                                                         │
│      → serialize(outputs) → Object Store                                    │
│                                │                                            │
│  Task 完成                      │                                            │
│  ════════                       ▼                                            │
│  15. ReturnWorkerLease → ReleaseWorkerResources()                           │
│      → available += {cpu:N, gpu:M}                                          │
│      → PushWorker() → idle 池 (队尾, LIFO 优先复用)                          │
│      → [回到 11, 等待下一个 Task]                                            │
│                                │                                            │
│  退出 (11种路径)                │                                            │
│  ═══════════════                ▼                                            │
│  16. 触发退出 (空闲超时/Job结束/OOM/Owner死/...)                              │
│      → Exit RPC (优雅) 或 DestroyWorker (强制)                              │
│      → DisconnectClient: 清理 lease/资源/GCS 报告                           │
│      → KillAsync: SIGTERM → 超时 → SIGKILL                                  │
│      → DisconnectWorker: 从 Pool 移除, 释放 runtime_env                     │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 核心设计决策与权衡

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                          5 个核心设计决策                                      │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │ 决策1: Worker 是通用容器，不绑定特定函数                                 │  │
│  │                                                                        │  │
│  │ 选择: Worker 启动时不知道要执行什么 → 函数通过 FunctionDescriptor        │  │
│  │       在 Task 到达时动态查找/下载                                       │  │
│  │                                                                        │  │
│  │ 优势: 同一 Worker 可串行执行不同函数 → 大幅减少进程创建开销              │  │
│  │ 代价: 每次 Task 可能需要 import/pickle 查找 → 首次调用有冷启动延迟       │  │
│  │ 缓解: 3 级缓存 (本地dict → import → GCS) + Worker 预创建               │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │ 决策2: 资源管理是逻辑记账，不是物理隔离                                   │  │
│  │                                                                        │  │
│  │ 选择: num_cpus=2 只影响调度准入控制 → 不限制 OS 层面实际 CPU 使用       │  │
│  │                                                                        │  │
│  │ 优势: 简单高效，无 cgroup 开销，Worker 复用无需迁移 cgroup              │  │
│  │ 代价: 恶意/bug Task 可能抢占同节点其他 Task 的资源                      │  │
│  │ 缓解: cgroupv2 (opt-in) 做 system/user 粗粒度划分保护系统进程           │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │ 决策3: Worker 复用优先 (LIFO 策略)                                      │  │
│  │                                                                        │  │
│  │ 选择: 从 idle 池查找时从队尾开始 (最近活跃的优先)                        │  │
│  │       杀死 idle Worker 时从队头开始 (从未使用的优先)                     │  │
│  │                                                                        │  │
│  │ 优势: 复用热 Worker → Python import 缓存、JIT 编译结果都已就绪          │  │
│  │ 代价: 可能保留过多 idle Worker 占用内存                                  │  │
│  │ 缓解: 软上限 = CPU 数量 + 定期清理 + 保活时间窗口                       │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │ 决策4: 跨语言完全隔离                                                    │  │
│  │                                                                        │  │
│  │ 选择: 每种语言独立的 State + 独立的命令模板 + 不可复用                   │  │
│  │                                                                        │  │
│  │ 原因: 本质约束 — Python 解释器 ≠ JVM ≠ 原生二进制                      │  │
│  │ 统一: 所有语言的 Worker 都内嵌 CoreWorker (C++) 作为通信层              │  │
│  │ 管理: idle_of_all_languages_ 跨语言列表统一管理杀死策略                 │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │ 决策5: 优雅退出与强制退出分层                                             │  │
│  │                                                                        │  │
│  │ 层级:                                                                   │  │
│  │   Exit RPC (Worker 可拒绝)                                              │  │
│  │     ↓ 失败/超时                                                         │  │
│  │   SIGTERM (Worker 可 handle)                                            │  │
│  │     ↓ 超时 (kill_worker_timeout_ms)                                     │  │
│  │   SIGKILL (无法拦截)                                                    │  │
│  │                                                                        │  │
│  │ 优势: Worker 有机会清理资源、flush 日志、报告状态                        │  │
│  │ 保障: 最终一定能杀死进程 (SIGKILL 不可拦截)                              │  │
│  │ 特例: OOM 直接 SIGKILL (内存紧急，不等待)                               │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

### 关键数据流一览

```
┌──────────────────────────────────────────────────────────────────────────┐
│                         Worker 核心数据流                                  │
│                                                                          │
│  FunctionDescriptor (函数地址)                                            │
│  ────────────────────────────                                            │
│  Driver → pickle.dumps(func) → GCS Internal KV                          │
│                                      ↓                                   │
│  Worker ← pickle.loads(bytes) ← GCS Internal KV                         │
│         或 importlib.import_module() (本地模式)                           │
│                                                                          │
│  TaskSpec (任务描述)                                                      │
│  ──────────────────                                                      │
│  包含: FunctionDescriptor + 序列化参数 + 资源需求                         │
│  不含: 函数代码本身                                                       │
│  流向: Driver → GCS/Scheduler → Raylet → Worker (PushTask gRPC)         │
│                                                                          │
│  资源 (逻辑记账)                                                          │
│  ────────────────                                                        │
│  分配: available -= {cpu:N, gpu:M}    (AllocateTaskResourceInstances)    │
│  释放: available += {cpu:N, gpu:M}    (FreeTaskResourceInstances)        │
│  流向: LocalResourceManager ↔ LocalLeaseManager ↔ WorkerPool            │
│                                                                          │
│  Worker 状态 (WorkerPool 管理)                                            │
│  ─────────────────────────────                                           │
│  starting → registered → idle ←→ leased/busy → disconnected             │
│                            ↑           ↓                                 │
│                            └───────────┘ (Task 完成后回到 idle)           │
│                                                                          │
│  进程控制 (OS 层面)                                                       │
│  ──────────────────                                                      │
│  创建: fork() + execvpe() + add_to_cgroup()                             │
│  通信: gRPC (PushTask/Exit/KillActor) + IPC (RegisterClient)            │
│  杀死: SIGTERM → 超时 → SIGKILL                                         │
│  检测: socket 断连 + parent_lifetime_pipe                                │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

### 故障排查指南

```
常见问题诊断路径:

问题1: Worker 进程泄漏（只增不减）
  ─────────────────────────────
  检查: TryKillingIdleWorkers 定时器是否正常运行
  检查: finished_jobs_ 中是否包含相关 Job
  检查: Worker 是否拒绝了 Exit RPC (持有 object ref)
  检查: idle_of_all_languages_ 列表大小 vs num_cpus

问题2: Task 启动延迟过高
  ─────────────────────
  检查: FindAndPopIdleWorker 是否命中 (匹配条件)
  检查: runtime_env_hash 是否一致 (不同 runtime_env 无法复用)
  检查: starting_workers >= maximum_startup_concurrency_ (启动限流)
  检查: Worker 注册是否超时 (MonitorStartingWorkerProcess)
  优化: 启用 worker_prestart + 增大 backlog 预创建

问题3: Worker 意外退出
  ───────────────────
  检查 WorkerExitType:
    NODE_OUT_OF_MEMORY → 内存不足，增大节点内存或减少并发
    SYSTEM_ERROR → Worker crash，检查用户代码异常
    INTENDED_SYSTEM_EXIT → 系统主动退出（PG移除/GCS请求/Owner死亡）
    INTENDED_USER_EXIT → 用户触发（max_calls/exit_actor/sys.exit）
  检查 owner 是否存活 (detached actor 不受 owner 死亡影响)

问题4: 资源不释放
  ─────────────
  检查: Worker ray.get() 阻塞 → NotifyWorkerBlocked 是否正确释放 CPU
  检查: Task 完成后 ReturnWorkerLease → ReleaseWorkerResources 是否被调用
  检查: DisconnectClient 中 lease 清理是否执行
```
