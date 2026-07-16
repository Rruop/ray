# Ray 容器生命周期：PID 1、Raylet、GCS 与 Pod/容器关系深度分析

## 目录

- [1. 基础概念：Pod vs 容器](#1-基础概念pod-vs-容器)
- [2. `ray start --head --block` 作为 PID 1：进程全景](#2-ray-start---head---block-作为-pid-1进程全景)
- [2.1 PID 1 的特殊性](#21-pid-1-的特殊性)
- [2.2 Head 节点进程启动顺序与 all_processes 构建](#22-head-节点进程启动顺序与-all_processes-构建)
- [2.3 dashboard_agent / runtime_env_agent：Python 不直接监控](#23-dashboard_agent--runtime_env_agentpython-不直接监控)
- [2.4 每个进程退出后的影响分析](#24-每个进程退出后的影响分析)
- [2.5 Agent 死亡 → Raylet 级联自杀（fate-sharing）](#25-agent-死亡--raylet-级联自杀fate-sharing)
- [2.6 进程重启逻辑：全链路无 restart](#26-进程重启逻辑全链路无-restart)
- [2.7 `--block` 循环的所有退出路径](#27---block-循环的所有退出路径)
- [2.8 汇总：每个进程死亡 → PID 1 行为 → 容器结果](#28-汇总每个进程死亡--pid-1-行为--容器结果)
- [3. 子进程监控：`--block` 循环详解](#3-子进程监控---block-循环详解)
- [4. Raylet 被 Kill 后的完整处理链](#4-raylet-被-kill-后的完整处理链)
- [5. GCS Server 被 Kill 后的完整处理链](#5-gcs-server-被-kill-后的完整处理链)
- [6. PID 1 自身被 Kill 的处理](#6-pid-1-自身被-kill-的处理)
- [7. 进程 Reaper 机制](#7-进程-reaper-机制)
- [8. kill_all_processes 杀进程顺序与策略](#8-kill_all_processes-杀进程顺序与策略)
- [9. 全场景汇总表](#9-全场景汇总表)
- [10. 已知隐患与半死状态](#10-已知隐患与半死状态)
- [11. 容器退出码与重启关系](#11-容器退出码与重启关系)
- [12. Kill Raylet 方式对比分析](#12-kill-raylet-方式对比分析)
- [13. GCS 健康检查机制与节点死亡检测](#13-gcs-健康检查机制与节点死亡检测)
- [14. drain_node 机制详解](#14-drain_node-机制详解)
- [15. `kill -15 raylet` 后的完整时间线](#15-kill--15-raylet-后的完整时间线)

---

## 1. 基础概念：Pod vs 容器

| | Pod | 容器 |
|---|---|---|
| 定义 | K8s 最小调度单元 | Pod 内的运行单元 |
| 关系 | 包含容器 | 被Pod包含 |
| 数量 | 1个Pod可包含1~N个容器 | 属于某个Pod |
| 重建/重启 | 删掉Pod才会新建Pod（新IP、新名字） | 容器崩溃后Pod内原地重启（Pod不变） |
| IP/身份 | Pod IP在Pod生命周期内不变 | Container ID在每次重启后变化 |
| 文件系统 | 共享Pod级别的Volume | 容器内非挂载卷的写入在重启后丢失 |

**对应到 Ray 场景：**

- `ray start --block` 退出导致的是**容器重启**（Pod 不变，restartCount++，Container ID 变，Pod IP/名字不变）
- 如果是 Pod 重建，则是全新 Pod（新 IP、新名字、新容器）

**容器重启后的关键行为：**
- 文件系统重置为镜像原始状态（容器运行时被删的二进制会恢复）
- 但如果镜像本身缺少该二进制，每次重启都会重复失败，进入 CrashLoopBackOff

---

## 2. `ray start --head --block` 作为 PID 1：进程全景

在 K8s 容器中，`ray start --head --block` 是容器的入口命令，因此成为 PID 1。

### 2.1 PID 1 的特殊性

- Linux 中 PID 1 是所有进程的祖先
- PID 1 退出 → 容器立即终止
- PID 1 负责回收子进程（避免僵尸进程）
- PID 1 收到的信号处理与普通进程不同（部分信号默认被忽略）
- **内核不允许 SIGKILL PID 1**（`kill -9 1` 无效）

### 2.2 Head 节点进程启动顺序与 `all_processes` 构建

`Node.__init__()` 中按以下顺序启动子进程，每个进程被加入 `self.all_processes` dict：

#### 启动顺序与代码位置

| 步骤 | 方法 | `all_processes` key | 进程类型 | 二进制/脚本 | 条件 | 代码位置 |
|:---:|------|---------------------|---------|-----------|------|---------|
| 1 | `start_reaper_process()` | `"reaper"` | Python | `ray/_private/ray_process_reaper.py` | `spawn_reaper=True` 且非 kernel fate-share | `node.py:983-999` |
| 2 | `start_gcs_server()` | `"gcs_server"` | C++ | `ray/core/src/ray/gcs/gcs_server` | Head 必须 | `node.py:1066-1109` |
| 3 | `start_monitor()` | `"monitor"` | Python | `ray/autoscaler/_private/monitor.py` | `not no_monitor` (默认启动) | `node.py:1209-1235` |
| 4 | `start_ray_client_server()` | `"ray_client_server"` | Python | `ray/util/client/server` | `ray_client_server_port` 设置 (默认 10001) | `node.py:1236-1258` |
| 5 | `start_api_server()` | `"dashboard"` | Python | `ray/dashboard/dashboard.py` | Head 必须 | `node.py:1021-1065` |
| 6 | `start_log_monitor()` | `"log_monitor"` | Python | `ray/_private/log_monitor.py` | `include_log_monitor` (默认 True) | `node.py:1000-1020` |
| 7 | `start_raylet()` | `"raylet"` | C++ | `ray/core/src/ray/raylet/raylet` | 所有节点必须 | `node.py:1111-1208` |

#### Worker 节点的 `all_processes`

| 步骤 | key | 条件 |
|:---:|-----|------|
| 1 | `"reaper"` | 同上 |
| 2 | `"log_monitor"` | 默认 True |
| 3 | `"raylet"` | 必须 |

#### `all_processes` dict 结构

```python
# node.py:170
self.all_processes: dict = {}

# 每次启动一个进程后:
self.all_processes[PROCESS_TYPE_RAYLET] = [process_info]
# process_info 包含: process (Popen对象), use_valgrind, stdout_file, stderr_file
```

`all_processes` 的 key 来自 `ray_constants.py:296-306`：

```python
PROCESS_TYPE_REAPER = "reaper"
PROCESS_TYPE_MONITOR = "monitor"
PROCESS_TYPE_RAY_CLIENT_SERVER = "ray_client_server"
PROCESS_TYPE_LOG_MONITOR = "log_monitor"
PROCESS_TYPE_DASHBOARD = "dashboard"
PROCESS_TYPE_DASHBOARD_AGENT = "dashboard_agent"      # ← 但 NOT 在 all_processes 中！
PROCESS_TYPE_RUNTIME_ENV_AGENT = "runtime_env_agent"   # ← 同上
PROCESS_TYPE_WORKER = "worker"
PROCESS_TYPE_RAYLET = "raylet"
PROCESS_TYPE_REDIS_SERVER = "redis_server"
PROCESS_TYPE_GCS_SERVER = "gcs_server"
```

### 2.3 dashboard_agent / runtime_env_agent：Python 不直接监控

这两个 agent **不在 `all_processes` 中**，Python `--block` 循环**不直接监控它们**。

#### 启动流程

Python 侧只构建命令字符串，传给 raylet 作为 CLI flag：

```python
# services.py:~1810-1870
dashboard_agent_command = [
    python_executable, os.path.join(RAY_PATH, "dashboard", "agent.py"),
    f"--node-id={node_id}", f"--node-ip-address={node_ip_address}", ...
]
runtime_env_agent_command = [
    python_executable, os.path.join(RAY_PATH, "_private", "runtime_env", "agent", "main.py"),
    f"--node-id={node_id}", f"--node-ip-address={node_ip_address}", ...
]

# 传给 raylet binary:
command.append("--dashboard_agent_command={}".format(subprocess.list2cmdline(dashboard_agent_command)))
command.append("--runtime_env_agent_command={}".format(subprocess.list2cmdline(runtime_env_agent_command)))
```

C++ raylet 侧读取这些 flag，通过 `AgentManager` 实际 spawn 进程：

```cpp
// main.cc:100-101, 599-607
DEFINE_string(dashboard_agent_command, "", "Dashboard agent command.");
DEFINE_string(runtime_env_agent_command, "", "Runtime env agent command.");
node_manager_config.dashboard_agent_command = dashboard_agent_command;
node_manager_config.runtime_env_agent_command = runtime_env_agent_command;
```

```cpp
// node_manager.cc:3290-3315
auto options = AgentManager::Options({
    self_node_id,
    "dashboard_agent",
    agent_command_line,
    /*fate_shares=*/true    // ← 关键：agent 死 → raylet 也死
});
return std::make_unique<AgentManager>(std::move(options), ...);
```

```cpp
// agent_manager.cc:33-99
void AgentManager::StartAgent(AddProcessToCgroupHook add_to_cgroup) {
    process_ = Process(argv.data(), nullptr, ec, false, env,
                       /*pipe_to_stdin*/ enable_pipe_based_health_check,
                       std::move(add_to_cgroup));
    // 启动 monitor_thread_ 监控 agent 进程
}
```

#### 进程层级关系

```
PID 1 (ray start --head --block)           ← Python 进程
  │
  ├─ reaper (Python subprocess)             ← all_processes["reaper"]
  ├─ gcs_server (C++ binary)                ← all_processes["gcs_server"]
  ├─ monitor (Python subprocess)            ← all_processes["monitor"]
  ├─ ray_client_server (Python subprocess)  ← all_processes["ray_client_server"]
  ├─ dashboard (Python subprocess)          ← all_processes["dashboard"]
  ├─ log_monitor (Python subprocess)        ← all_processes["log_monitor"]
  │
  └─ raylet (C++ binary)                   ← all_processes["raylet"]
       │                                     ← Python 只监控到这一层
       │
       ├─ dashboard_agent (Python subprocess)  ← raylet 的 AgentManager spawn
       │                                        ← NOT 在 all_processes 中
       │
       ├─ runtime_env_agent (Python subprocess) ← raylet 的 AgentManager spawn
       │                                         ← NOT 在 all_processes 中
       │
       └─ worker processes (Python/C++ subprocess) ← raylet 的 worker_pool spawn
                                                     ← NOT 在 all_processes 中
```

**关键点：** Python PID 1 只监控直接 spawn 的 7 个子进程，对 raylet 的 3 个孙子进程（dashboard_agent、runtime_env_agent、workers）**不直接监控**。它们的健康由 C++ raylet 管理，通过 fate-sharing 机制间接影响 PID 1。

### 2.4 每个进程退出后的影响分析

#### Python `--block` 循环直接监控的 7 个进程

| 进程 key | 功能 | expected 退出 (0/-15/143) | unexpected 退出 (非 expected) |
|---------|------|:---:|:---:|
| `"reaper"` | PID 1 死后清理所有子进程 | 忽略，继续阻塞（但 PID 1 死后无 reaper 保护） | `kill_all` + `os._exit(1)` → 容器 exit(1) |
| `"gcs_server"` | 集群控制平面：节点管理、actor调度、资源管理 | 忽略（但整个集群控制平面消失） | `kill_all` + `os._exit(1)` → 容器 exit(1) |
| `"monitor"` | Autoscaler 监控 | 忽略（autoscaler 停止工作） | `kill_all` + `os._exit(1)` → 容器 exit(1) |
| `"ray_client_server"` | Ray Client API | 忽略（client 连接断开） | `kill_all` + `os._exit(1)` → 容器 exit(1) |
| `"dashboard"` | Dashboard UI/API | 忽略（dashboard 不可用） | `kill_all` + `os._exit(1)` → 容器 exit(1) |
| `"log_monitor"` | 日志聚合 | 忽略（日志不再聚合） | `kill_all` + `os._exit(1)` → 容器 exit(1) |
| `"raylet"` | 节点调度、对象管理、worker 管理 | 忽略（**但节点功能完全丧失**） | `kill_all` + `os._exit(1)` → 容器 exit(1) |

#### Python 不直接监控的 3 个孙子进程

| 进程 | 父进程 | 退出后的级联效果 |
|------|--------|----------------|
| `dashboard_agent` | raylet (AgentManager) | AgentManager monitor_thread 检测退出 → `fate_shares=true` → raylet 执行 `shutdown_raylet_gracefully_(UNEXPECTED_TERMINATION)` → raylet 退出 → Python `--block` 检测到 `"raylet"` 死亡 → 级联到容器 |
| `runtime_env_agent` | raylet (AgentManager) | 同上 |
| `worker processes` | raylet (worker_pool) | worker 退出不会触发 raylet suicide；raylet 只重建该 worker。但如果 raylet 已死（被 SIGKILL），workers 变孤儿 |

#### 核心规则：任何 `all_processes` 中的进程以 unexpected code 退出 → 全部终止

```
任何子进程 unexpected 退出
  │
  ├─ 日志: "Some Ray subprocesses exited unexpectedly:"
  │         "  {process_type} [exit code={returncode}]"
  │         "Remaining processes will be killed."
  │
  ├─ 写入 ray_process_exit.log
  │
  ├─ node.kill_all_processes(check_alive=False, allow_graceful=False)
  │    ├─ raylet → SIGKILL
  │    ├─ gcs_server → SIGKILL
  │    ├─ 所有其他 → SIGKILL
  │    └─ reaper → SIGKILL (最后)
  │
  └─ os._exit(1) → 容器终止
```

**不区分进程重要性**：无论是 gcs_server 还是 log_monitor 以 unexpected code 退出，处理逻辑完全相同——全部 SIGKILL + `os._exit(1)`。

### 2.5 Agent 死亡 → Raylet 级联自杀（fate-sharing）

**文件：** `src/ray/raylet/agent_manager.cc:72-96`

```cpp
monitor_thread_ = std::make_unique<std::thread>([this]() mutable {
    SetThreadName("agent.monitor." + options_.agent_name);
    int exit_code = process_.Wait();
    RAY_LOG(INFO) << "Agent process with name " << options_.agent_name
                  << " exited, exit code " << exit_code << ".";
    if (fate_shares_.load()) {
      RAY_LOG(ERROR)
          << "The raylet exited immediately because one Ray agent failed, "
          << "agent_name = " << options_.agent_name << ".\n"
          << "The raylet fate shares with the agent. This can happen because\n"
          << "- The version of `grpcio` doesn't follow Ray's requirement.\n"
          << "- The agent failed to start because of unexpected error or port conflict.\n"
          << "- The agent is killed by the OS (e.g., out of memory).";
      rpc::NodeDeathInfo node_death_info;
      node_death_info.set_reason(rpc::NodeDeathInfo::UNEXPECTED_TERMINATION);
      node_death_info.set_reason_message(options_.agent_name +
                                         " failed and raylet fate-shares with it.");
      shutdown_raylet_gracefully_(node_death_info);
      // 10 秒后强制退出
      delay_executor_([]() { QuickExit(); }, 10000);
    }
});
```

**级联链路：**

```
dashboard_agent 或 runtime_env_agent 异常退出
  │
  ├─ AgentManager monitor_thread 检测 (process_.Wait() 返回)
  │
  ├─ fate_shares_ == true → 触发级联:
  │    │
  │    ├─ shutdown_raylet_gracefully_(UNEXPECTED_TERMINATION)
  │    │    ├─ gcs_client->Nodes().UnregisterSelf()  ← GCS 立即标记 dead
  │    │    ├─ node_manager->Stop() (杀 workers, 停 agents)
  │    │    └─ main_service.stop() → raylet 进程退出
  │    │
  │    └─ delay_executor_(QuickExit, 10000) ← 保底：10秒后强杀
  │
  └─ raylet 退出 → Python --block 检测:
       │
       ├─ raylet returncode ∈ expected (0/-15/143)
       │    └─ 忽略 → 容器不重建，但节点功能丧失
       │       (raylet 是 graceful shutdown, 虽然原因是非 graceful)
       │
       └─ raylet returncode ∉ expected
       │    └─ kill_all + os._exit(1) → 容器重建
       │
       └─ 实际情况: shutdown_raylet_gracefully_ 调用后 raylet 以
          EXPECTED_TERMINATION 注销 GCS, 然后 main_service.run() 返回
          → 进程正常退出 (returncode=0 或 -15)
          → Python 视为 expected → 容器不重建 → **半死状态**
```

**特别注意：** agent 死亡触发 raylet "graceful shutdown"，但 death reason 是 `UNEXPECTED_TERMINATION`（"agent failed and raylet fate-shares with it"）。raylet 会向 GCS 注销自己（reason=UNEXPECTED_TERMINATION），GCS 会广播 `RAY_NODE_REMOVED` 错误到所有 drivers。但在 Python `--block` 侧，raylet 以 expected code 退出 → 不触发杀进程 → 容器不重建 → **半死状态**。

---

### 2.6 进程重启逻辑：全链路无 restart

**结论：整个 Ray 节点进程树中，没有任何层级实现了子进程重启逻辑。**

#### PID 1（`ray start --block`）：无重启

`--block` 循环只做两件事：
1. 检测子进程是否死亡
2. 如果 unexpected 死亡 → 杀全部 + `os._exit(1)`

```python
# scripts.py:1178-1228
while True:
    time.sleep(1)
    deceased = node.dead_processes()
    # ... 判定 expected/unexpected ...
    if len(unexpected_deceased) > 0:
        node.kill_all_processes(check_alive=False, allow_graceful=False)
        os._exit(1)    # ← 直接退出，无重启
# not-reachable
```

没有任何 `try: start_xxx()` 重启逻辑。`Node` 类中的 `start_ray_processes()` / `start_head_processes()` 只在初始化时调用一次。

#### Raylet C++（对 Agent）：无重启

**文件：** `src/ray/raylet/agent_manager.cc:63-93`

```cpp
monitor_thread_ = std::make_unique<std::thread>([this]() mutable {
    int exit_code = process_->Wait();
    RAY_LOG(INFO) << "Agent process exited, exit code " << exit_code << ".";
    if (fate_shares_.load()) {
      // raylet 自杀（级联到 PID 1）
      shutdown_raylet_gracefully_(node_death_info);
      delay_executor_([]() { QuickExit(); }, 10000);
    }
    // fate_shares=false 时：monitor thread 直接结束，无重启
});
```

Agent 死后只有两个结局：
- `fate_shares=true`（当前默认）→ raylet 自杀 → PID 1 检测到 raylet 死
- `fate_shares=false` → 什么都不做，agent 永久丢失

**当前配置**（`node_manager.cc:3340-3345`）：两个 agent 都是 `fate_shares=true`：

```cpp
auto options = AgentManager::Options({self_node_id,
                                      agent_name,
                                      agent_command_line,
                                      /*fate_shares=*/true});  // ← 当前默认
```

代码中有一个 TODO 注释暗示未来可能改变：

```cpp
// TODO(ryw): after thorough testing, we can disable the fate_shares flag
// and let a dashboard agent crash no longer lead to a raylet crash.
```

#### Raylet C++（对 Worker）：无重启

Worker 死后 `DisconnectWorker()` 只做清理，不重启：

```cpp
// worker_pool.cc:1563-1644
void WorkerPool::DisconnectWorker(...) {
  MarkPortAsFree(worker->AssignedPort());
  // ... 从 registered_workers, idle, worker_processes 中移除 ...
  RemoveWorkerProcess(state, worker->WorkerId());
  // 如果有 pending lease requests，TryPendingStartRequests()
  // 会启动**新的** worker 进程，但**不是重启**死的那个
}
```

`TryPendingStartRequests` 的语义：有新的 lease 请求等待 → 启动新 worker 来满足 → 不是"重启死掉的 worker"。

**Worker 重启只存在于 Actor 层级**（通过 `max_restarts` 配置，由 GCS 管理），不在 raylet 层级。

#### 全链路重启逻辑总结

| 层级 | 进程 | 有重启逻辑？ | 死后行为 |
|------|------|:----------:|---------|
| PID 1 | reaper, gcs_server, monitor, dashboard, ray_client_server, log_monitor, raylet | **无** | unexpected → `kill_all` + `os._exit(1)`；expected → 忽略（进程丢失但不补） |
| Raylet | dashboard_agent | **无** | `fate_shares=true` → raylet 自杀；`fate_shares=false` → agent 永久丢失 |
| Raylet | runtime_env_agent | **无** | 同上 |
| Raylet | worker | **无** | 清理状态，有 pending demand 时可能启动新 worker（非重启） |
| GCS | actor (跨节点) | **有** | `max_restarts` 控制，GCS 在其他节点重建 actor |

---

### 2.7 `--block` 循环的所有退出路径

`--block` 的 `while True` 循环只有以下退出方式：

| 退出路径 | 退出方式 | 退出码 | kill 其他进程？ | kill 策略 |
|---------|---------|:------:|:------------:|----------|
| 任何子进程 unexpected 退出 | `os._exit(1)` | 1 | ✅ 是 | `allow_graceful=False` → 全部 SIGKILL |
| PID 1 收到 SIGTERM | `sys.exit(1)` | 1 | ✅ 是 | `allow_graceful=True` → 先 SIGTERM 等 1s 再 SIGKILL |
| PID 1 收到 SIGKILL | 内核强杀 | N/A | ❌ 否 | 由 Reaper 代劳：SIGTERM pgroup → 等 1s → SIGKILL |
| PID 1 崩溃 (SIGSEGV等) | 内核强杀 | N/A | ❌ 否 | 同上 |

**关键：不存在正常退出路径。** `while True` 循环要么 `os._exit(1)`，要么被外部信号杀死。代码注释也确认了这一点：

```python
# scripts.py:1229
# not-reachable
```

循环永远不可能自然结束（break/return），意味着：
- 如果所有子进程都以 expected code 退出，PID 1 会永远空转
- 如果任一子进程以 unexpected code 退出，PID 1 立即 `os._exit(1)`

---

### 2.8 汇总：每个进程死亡 → PID 1 行为 → 容器结果

#### 直接监控的 7 个子进程

| 进程 | expected 退出 (0/-15/143) | unexpected 退出 |
|------|:---:|:---:|
| **gcs_server** | PID 1 忽略 → 容器存活（但集群控制平面消失）→ raylet 最终也可能死 → 全节点空转 | `kill_all` + `os._exit(1)` → 容器 exit(1) |
| **raylet** | PID 1 忽略 → 容器存活（**节点功能完全丧失**）→ 半死状态 | `kill_all` + `os._exit(1)` → 容器 exit(1) |
| **monitor** | PID 1 忽略 → 容器存活（autoscaler 停止） | `kill_all` + `os._exit(1)` → 容器 exit(1) |
| **dashboard** | PID 1 忽略 → 容器存活（Dashboard UI 不可用） | `kill_all` + `os._exit(1)` → 容器 exit(1) |
| **ray_client_server** | PID 1 忽略 → 容器存活（Ray Client 断开） | `kill_all` + `os._exit(1)` → 容器 exit(1) |
| **log_monitor** | PID 1 忽略 → 容器存活（日志不再聚合） | `kill_all` + `os._exit(1)` → 容器 exit(1) |
| **reaper** | PID 1 忽略 → 容器存活（但 PID 1 死后无清理保护） | `kill_all` + `os._exit(1)` → 容器 exit(1) |

**规则：PID 1 不区分进程重要性**——无论哪个子进程 unexpected 退出，都是全杀 + exit(1)。

#### 间接影响的孙子进程

| 进程 | 父进程 | 死亡后的级联 | 最终容器结果 |
|------|--------|------------|------------|
| **dashboard_agent** | raylet (AgentManager) | `fate_shares=true` → raylet graceful shutdown → GCS 注销 (UNEXPECTED_TERMINATION) → raylet 以 expected code 退出 → PID 1 忽略 | 容器存活，**半死状态** |
| **runtime_env_agent** | raylet (AgentManager) | 同上 | 同上 |
| **worker** | raylet (worker_pool) | 不级联到 raylet。raylet 清理该 worker 状态，可能启动新 worker 满足 pending demand | 容器存活 |

#### 全链路决策树

```
子进程死亡
  │
  ├─ 是 all_processes 中的进程？
  │    │
  │    ├─ 是 → returncode ∈ expected?
  │    │    ├─ 是 → PID 1 忽略 → 容器存活 → 可能半死/空转
  │    │    │
  │    │    └─ 否 → kill_all(allow_graceful=False) + os._exit(1)
  │    │         → 容器 exit(1) → K8s restartPolicy 决定重启
  │    │
  │    └─ 否（是 raylet 的孙子进程）
  │         │
  │         ├─ 是 agent (dashboard_agent / runtime_env_agent)?
  │         │    ├─ fate_shares=true → raylet shutdown_raylet_gracefully_
  │         │    │    → raylet 以 expected code 退出
  │         │    │    → PID 1 忽略
  │         │    │    → 容器存活，半死状态
  │         │    │
  │         │    └─ fate_shares=false → agent 永久丢失
  │         │         → PID 1 不知道（不在 all_processes 中）
  │         │         → 容器存活，agent 功能缺失
  │         │
  │         └─ 是 worker?
  │              → raylet DisconnectWorker() 清理
  │              → 可能启动新 worker（不是重启死 worker）
  │              → 容器存活，worker 级别自愈
```

---

## 3. 子进程监控：`--block` 循环详解

### 3.1 监控循环代码

**文件：** `python/ray/scripts/scripts.py:1161-1228`

```python
if block:
    logs_dir = node.get_logs_dir_path()
    process_exit_log_path = os.path.join(logs_dir, "ray_process_exit.log")
    # ... 初始化日志 ...

    while True:
        time.sleep(1)
        deceased = node.dead_processes()

        expected_return_codes = [
            0,                     # 正常退出
            signal.SIGTERM,        # 15
            -1 * signal.SIGTERM,   # -15 (Python Unix 约定)
            128 + signal.SIGTERM,  # 143 (Shell 约定: 128 + signal number)
        ]
        unexpected_deceased = [
            (process_type, process)
            for process_type, process in deceased
            if process.returncode not in expected_return_codes
        ]
        if len(unexpected_deceased) > 0:
            cli_logger.error("Some Ray subprocesses exited unexpectedly:")
            # ... 记录日志 ...
            cli_logger.error("Remaining processes will be killed.")
            node.kill_all_processes(check_alive=False, allow_graceful=False)
            os._exit(1)
```

### 3.2 死进程检测

**文件：** `python/ray/_private/node.py:1700-1712`

```python
def dead_processes(self):
    """Return a list of the dead processes.
    Note that this ignores processes that have been explicitly killed,
    e.g., via a command like node.kill_raylet().
    """
    result = []
    for process_type, process_infos in self.all_processes.items():
        for process_info in process_infos:
            if process_info.process.poll() is not None:
                result.append((process_type, process_info.process))
    return result
```

检测机制：每 1 秒轮询一次 `process.poll()`，返回非 None 表示进程已退出。

### 3.3 Expected Return Codes 判定逻辑

| 退出码 | 来源 | 是否 Expected |
|--------|------|:---:|
| 0 | 正常退出 | Y |
| 15 | 被 SIGTERM 杀（某些系统） | Y |
| -15 | 被 SIGTERM 杀（Python Unix 约定） | Y |
| 143 | 被 SIGTERM 杀（Shell 约定 128+15） | Y |
| -9 | 被 SIGKILL 杀 | **N** |
| -11 | 被 SIGSEGV 杀 | **N** |
| -6 | 被 SIGABRT 杀 | **N** |
| 1 | 应用层错误退出 | **N** |

**设计原因**（代码注释）：

```python
# We are explicitly expecting SIGTERM because this is how `ray stop` sends
# shutdown signal to subprocesses, i.e. log_monitor, raylet...
# NOTE(rickyyx): We are treating 128+15 as an expected return code since
# this is what autoscaler/_private/monitor.py does upon SIGTERM handling.
```

---

## 4. Raylet 被 Kill 后的完整处理链

### 4.1 场景 A：Raylet 收到 SIGTERM（优雅退出）

这是 `ray stop` 或正常缩容时的路径。

#### C++ 侧：SIGTERM handler

**文件：** `src/ray/raylet/main.cc:1141-1170`

```cpp
auto signal_handler = [&node_manager, shutdown_raylet_gracefully](
                          const boost::system::error_code &error, int signal_number) {
    ray::rpc::NodeDeathInfo node_death_info;
    std::optional<ray::rpc::DrainRayletRequest> drain_request =
        node_manager->GetLocalDrainRequest();
    RAY_LOG(INFO) << "received SIGTERM. Existing local drain request = "
                  << (drain_request.has_value() ? drain_request->DebugString() : "None");
    if (drain_request.has_value() &&
        drain_request->reason() ==
            ray::rpc::autoscaler::DrainNodeReason::DRAIN_NODE_REASON_PREEMPTION &&
        drain_request->deadline_timestamp_ms() != 0 &&
        drain_request->deadline_timestamp_ms() < ray::current_sys_time_ms()) {
      node_death_info.set_reason(ray::rpc::NodeDeathInfo::AUTOSCALER_DRAIN_PREEMPTED);
      node_death_info.set_reason_message(drain_request->reason_message());
    } else {
      node_death_info.set_reason(ray::rpc::NodeDeathInfo::EXPECTED_TERMINATION);
      node_death_info.set_reason_message("received SIGTERM");
    }
    shutdown_raylet_gracefully(node_death_info);
};
boost::asio::signal_set signals(main_service);
signals.add(SIGTERM);
signals.async_wait(signal_handler);
```

#### C++ 侧：graceful shutdown 流程

**文件：** `src/ray/raylet/main.cc:457-490`

```cpp
auto shutdown_raylet_gracefully =
    [raylet_node_id, &shutting_down, &node_manager, &main_service,
     &raylet_socket_name, &gcs_client, &object_manager_rpc_threads]
    (const ray::rpc::NodeDeathInfo &node_death_info) {
        if (shutting_down.exchange(true)) {
            RAY_LOG(INFO) << "Raylet shutdown already triggered, ignoring death info: "
                          << node_death_info.DebugString();
            return;
        }
        RAY_LOG(INFO) << "Raylet graceful shutdown triggered with death info: "
                      << node_death_info.DebugString();

        auto unregister_done_callback = [&main_service, &raylet_socket_name,
                                         &node_manager, &gcs_client,
                                         &object_manager_rpc_threads]() {
            node_manager->Stop();
            gcs_client->Disconnect();
            ray::stats::Shutdown();
            main_service.stop();
            for (size_t i = 0; i < object_manager_rpc_threads.size(); i++) {
                if (object_manager_rpc_threads[i].joinable()) {
                    object_manager_rpc_threads[i].join();
                }
            }
            remove(raylet_socket_name.c_str());
        };

        gcs_client->Nodes().UnregisterSelf(
            raylet_node_id, node_death_info, std::move(unregister_done_callback));
    };
```

完整流程：

```
SIGTERM → signal_handler()
  │
  ├─ 检查 drain request
  │    ├─ PREEMPTION 且已过期 → reason=AUTOSCALER_DRAIN_PREEMPTED
  │    └─ 其他 → reason=EXPECTED_TERMINATION, message="received SIGTERM"
  │
  └─ shutdown_raylet_gracefully(node_death_info)
       │
       ├─ shutting_down.exchange(true)  ← 原子操作，防止重复触发
       │
       ├─ gcs_client->Nodes().UnregisterSelf()  ← 向 GCS 注销本节点
       │    │
       │    └─ unregister_done_callback:
       │         ├─ node_manager->Stop()
       │         │    ├─ store_client_->Disconnect()         ← 断开 plasma store
       │         │    ├─ 遍历所有 worker 的 process group:
       │         │    │    发 SIGTERM → probe → 没死则 SIGKILL  ← 杀自己的 child workers
       │         │    ├─ object_manager_.Stop()
       │         │    ├─ dashboard_agent_manager_.reset()    ← 停止 dashboard agent
       │         │    ├─ runtime_env_agent_manager_.reset() ← 停止 runtime env agent
       │         │    └─ acceptor_.close()                  ← 关闭 RPC acceptor
       │         │
       │         ├─ gcs_client->Disconnect()  ← 断开 GCS 连接
       │         ├─ ray::stats::Shutdown()    ← 停止指标导出
       │         ├─ main_service.stop()       ← 停止事件循环
       │         ├─ join 所有 RPC 线程
       │         └─ remove(raylet_socket_name) ← 删除 unix socket 文件
       │
       └─ main_service.run() 返回 → raylet 进程退出 (returncode = 0 或 -15 或 143)
```

#### NodeManager::Stop() 详解

**文件：** `src/ray/raylet/node_manager.cc:2945-2970`

```cpp
void NodeManager::Stop() {
  store_client_->Disconnect();
#if !defined(_WIN32)
  if (RayConfig::instance().process_group_cleanup_enabled()) {
    auto workers = worker_pool_.GetAllRegisteredWorkers(
        /* filter_dead_workers=*/true, /* filter_io_workers=*/false);
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
  object_manager_.Stop();
  dashboard_agent_manager_.reset();
  runtime_env_agent_manager_.reset();
  acceptor_.close();
}
```

#### Python 侧：expected code → 忽略

raylet 以 `returncode` ∈ `{0, 15, -15, 143}` 退出 → 在 expected 列表中 → `--block` 循环**忽略**，继续阻塞。

**结果：容器不死，但节点已无 raylet，功能残废。**

---

### 4.2 场景 B：Raylet 收到 SIGKILL（或崩溃 SIGSEGV/SIGABRT）

#### C++ 侧

无 signal handler 可触发，进程**立即死亡**。没有机会：
- 注销 GCS
- 杀 child workers
- 清理 socket 文件
- 停止 dashboard/runtime_env agent

Worker 进程成为孤儿进程。

#### Python 侧处理链

```
--block 循环 (每秒轮询 node.dead_processes())
  │
  ├─ process.poll() != None → raylet 已死
  │
  ├─ raylet.returncode = -9 (SIGKILL)  ∉ expected_return_codes
  │
  ├─ unexpected_deceased 非空 → 进入错误处理:
  │    │
  │    ├─ 日志输出:
  │    │    "Some Ray subprocesses exited unexpectedly:"
  │    │    "  raylet [exit code=-9]"
  │    │    "Remaining processes will be killed."
  │    │
  │    ├─ 写入 ray_process_exit.log
  │    │
  │    ├─ node.kill_all_processes(check_alive=False, allow_graceful=False)
  │    │    │
  │    │    ├─ 1. 先杀 raylet (已死，跳过)
  │    │    ├─ 2. 杀 GCS server (直接 SIGKILL，allow_graceful=False)
  │    │    ├─ 3. 杀其他所有进程 (dashboard, log_monitor, monitor,
  │    │    │     ray_client_server, dashboard_agent, runtime_env_agent)
  │    │    │     全部直接 SIGKILL，不等待
  │    │    └─ 4. 最后杀 reaper
  │    │
  │    └─ os._exit(1)  ← PID 1 以退出码 1 退出，跳过 atexit handler
  │
  └─ 容器因 PID 1 退出而终止 (exit code=1)
       └─ K8s restartPolicy 决定是否重启
```

**关键点：**
- `allow_graceful=False` → 所有剩余子进程直接 SIGKILL，无优雅退出机会
- `os._exit(1)` → 跳过 Python atexit handler，进程立刻死
- raylet 的 child workers 没有被 raylet 杀 → 变成孤儿进程
- Python 层的 `kill_all_processes` 只杀 Python 直接 spawn 的子进程，不杀 raylet 的孙子进程（workers）

---

## 5. GCS Server 被 Kill 后的完整处理链（仅 Head 节点）

### 5.1 场景 A：GCS Server 收到 SIGTERM（优雅退出）

#### C++ 侧：SIGTERM handler

**文件：** `src/ray/gcs/gcs_server_main.cc:237-257`

```cpp
auto handler = [&main_service, &gcs_server](const boost::system::error_code &error,
                                            int signal_number) {
    RAY_LOG(INFO) << "GCS server received SIGTERM, shutting down...";
    main_service.stop();
    ray::rpc::DrainServerCallExecutor();
    gcs_server.Stop();
    ray::stats::Shutdown();
};
boost::asio::signal_set signals(main_service);
signals.add(SIGTERM);
signals.async_wait(handler);
```

#### C++ 侧：GcsServer::Stop() 详解

**文件：** `src/ray/gcs/gcs_server.cc:324-349`

```cpp
void GcsServer::Stop() {
  if (!is_stopped_) {
    RAY_LOG(INFO) << "Stopping GCS server.";
    if (ray_event_recorder_) {
      ray_event_recorder_->StopExportingEvents();
    }
    io_context_provider_.StopAllDedicatedIOContexts();
    ray_syncer_.reset();
    pubsub_handler_.reset();
    rpc_server_.Shutdown();
    kv_manager_.reset();
    is_stopped_ = true;
    RAY_LOG(INFO) << "GCS server stopped.";
  }
}
```

完整流程：

```
SIGTERM → handler()
  ├─ main_service.stop()               ← 停止事件循环
  ├─ ray::rpc::DrainServerCallExecutor() ← 排空正在执行的 RPC
  ├─ gcs_server.Stop()
  │    ├─ 停止 event recorder 导出
  │    ├─ 停止所有 dedicated IO contexts
  │    ├─ reset ray_syncer_
  │    ├─ reset pubsub_handler_
  │    ├─ rpc_server_.Shutdown()
  │    ├─ reset kv_manager_
  │    └─ is_stopped_ = true
  ├─ ray::stats::Shutdown()
  └─ main_service.run() 返回 → GCS 进程退出 (returncode = 0 或 -15)
```

#### Python 侧：expected code → 忽略

GCS 以 expected code 退出 → `--block` 循环**忽略**，继续阻塞。

**但连锁效应：** GCS 死后，Head 节点上的 raylet 依赖 GCS → GCS 心跳超时 → raylet 也可能退出。如果 raylet 也以 expected code 退出，`--block` 仍然不触发杀进程。最终所有子进程都自然退出，但 `while True` 空转，容器活着但全节点功能丧失。

---

### 5.2 场景 B：GCS Server 收到 SIGKILL（或崩溃）

#### C++ 侧

立即死亡，无任何清理。

#### Python 侧处理链

```
--block 循环检测到 gcs_server 死亡
  │
  ├─ returncode = -9 ∉ expected_return_codes
  │
  ├─ unexpected_deceased 非空 → 进入错误处理:
  │    │
  │    ├─ 日志: "Some Ray subprocesses exited unexpectedly: gcs_server [exit code=-9]"
  │    │
  │    ├─ node.kill_all_processes(check_alive=False, allow_graceful=False)
  │    │    │
  │    │    ├─ 1. 先杀 raylet (直接 SIGKILL)
  │    │    │    ↑ 设计原因：raylet 应先被杀，否则后杀 GCS 会导致 raylet 异常退出
  │    │    │
  │    │    ├─ 2. GCS 已死 (跳过)
  │    │    │
  │    │    ├─ 3. 杀其他所有进程 (直接 SIGKILL)
  │    │    │
  │    │    └─ 4. 杀 reaper
  │    │
  │    └─ os._exit(1)
  │
  └─ 容器 exit(1) → K8s restartPolicy 决定是否重启
```

**关键区别：** GCS 被 kill 时，kill 顺序是 raylet 先（即使 raylet 还活着也要先杀），确保 raylet 在 GCS 彻底不可用前被清理。但此处 `allow_graceful=False`，raylet 直接被 SIGKILL，没有机会清理自己的 workers。

---

## 6. PID 1 自身被 Kill 的处理

### 6.1 PID 1 收到 SIGTERM

**文件：** `python/ray/_private/node.py:516-527`

```python
def _register_shutdown_hooks(self):
    def atexit_handler(*args):
        self.kill_all_processes(check_alive=False, allow_graceful=True)

    atexit.register(atexit_handler)

    def sigterm_handler(signum, frame):
        self.kill_all_processes(check_alive=False, allow_graceful=True)
        sys.exit(1)

    ray._private.utils.set_sigterm_handler(sigterm_handler)
```

流程：

```
PID 1 收到 SIGTERM → sigterm_handler()
  │
  ├─ kill_all_processes(check_alive=False, allow_graceful=True)
  │    │
  │    ├─ raylet: SIGTERM → 等 1 秒 → SIGKILL (如果还活着)
  │    ├─ GCS: SIGTERM → 等 1 秒 → SIGKILL
  │    ├─ 其他进程: 同上
  │    └─ reaper: 最后杀
  │
  └─ sys.exit(1)  ← 触发 atexit handler (再次调用 kill_all_processes)
     └─ 容器 exit(1)
```

与异常退出路径的区别：
- `allow_graceful=True` → 先 SIGTERM 等 1 秒再 SIGKILL
- `sys.exit(1)` → 触发 atexit handler（与 `os._exit(1)` 不同）
- 退出码仍为 1

### 6.2 PID 1 收到 SIGKILL（或崩溃）

PID 1 立即死亡，无法执行任何清理。此时 **Reaper 进程** 负责收尾（见下一节）。

---

## 7. 进程 Reaper 机制

**文件：** `python/ray/_private/ray_process_reaper.py`

```python
"""
This is a lightweight "reaper" process used to ensure that ray processes are
cleaned up properly when the main ray process dies unexpectedly (e.g.,
segfaults or gets SIGKILLed). Note that processes may not be cleaned up
properly if this process is SIGTERMed or SIGKILLed.
"""

SIGTERM_GRACE_PERIOD_SECONDS = 1

def reap_process_group(*args):
    def sigterm_handler(*args):
        time.sleep(SIGTERM_GRACE_PERIOD_SECONDS)
        if sys.platform == "win32":
            atexit.unregister(sigterm_handler)
            os.kill(0, signal.CTRL_BREAK_EVENT)
        else:
            os.killpg(0, signal.SIGKILL)

    if sys.platform == "win32":
        atexit.register(sigterm_handler)
    else:
        signal.signal(signal.SIGTERM, sigterm_handler)

    # Our parent must have died, SIGTERM the group (including ourselves).
    if sys.platform == "win32":
        os.kill(0, signal.CTRL_C_EVENT)
    else:
        os.killpg(0, signal.SIGTERM)

def main():
    # Read from stdin forever. Because stdin is a file descriptor
    # inherited from our parent process, we will get an EOF if the parent
    # dies, which is signaled by an empty return from read().
    while len(sys.stdin.read()) != 0:
        pass
    reap_process_group()
```

### Reaper 工作原理

```
Reaper 进程启动时:
  ├─ 继承父进程 (PID 1) 的 stdin (pipe)
  ├─ 阻塞在 sys.stdin.read()
  │
  └─ 检测父进程死亡的方式:
       ├─ 方式1 (stdin EOF): 父进程死亡 → stdin pipe EOF → read() 返回空串
       └─ 方式2 (PR_SET_PDEATHSIG): Linux 内核支持时，
            子进程通过 prctl(PR_SET_PDEATHSIG, SIGKILL)
            在父进程死亡时收到 SIGKILL

当父进程死亡后:
  │
  ├─ reap_process_group()
  │    ├─ 注册自己的 SIGTERM handler (等 1 秒后 SIGKILL 整个 pgroup)
  │    │
  │    └─ os.killpg(0, signal.SIGTERM)  ← 向整个进程组发 SIGTERM
  │         │
  │         ├─ 所有子进程收到 SIGTERM
  │         ├─ 等 1 秒
  │         └─ os.killpg(0, signal.SIGKILL)  ← 强杀所有剩余进程
  │
  └─ Reaper 自身也死亡
```

### Reaper 的启动

**文件：** `python/ray/_private/services.py:1082-1120`

```python
def start_reaper(fate_share=None):
    try:
        if sys.platform != "win32":
            os.setpgrp()  # 让主进程成为进程组 leader
    except OSError as e:
        return None

    reaper_filepath = os.path.join(RAY_PATH, RAY_PRIVATE_DIR, "ray_process_reaper.py")
    command = [sys.executable, "-u", reaper_filepath]
    process_info = start_ray_process(
        command,
        ray_constants.PROCESS_TYPE_REAPER,
        pipe_stdin=True,   # 关键：stdin pipe 从父进程继承
        fate_share=fate_share,
    )
    return process_info
```

### Reaper 不触发的场景

**文件：** `python/ray/_private/node.py:~164`

```python
if not connect_only and spawn_reaper and not self.kernel_fate_share:
    self.start_reaper_process()
```

当 Linux 内核支持 `PR_SET_PDEATHSIG`（fate-sharing）时，reaper 不启动，因为内核已经在父进程死亡时自动 SIGKILL 子进程。

---

## 8. kill_all_processes 杀进程顺序与策略

### 8.1 杀进程顺序

**文件：** `python/ray/_private/node.py:1642-1720`

```python
def kill_all_processes(self, check_alive=True, allow_graceful=False, wait=False):
    # 1. 先杀 raylet
    if ray_constants.PROCESS_TYPE_RAYLET in self.all_processes:
        self._kill_process_type(
            ray_constants.PROCESS_TYPE_RAYLET,
            check_alive=check_alive,
            allow_graceful=allow_graceful,
            wait=wait,
        )

    # 2. 再杀 GCS server
    if ray_constants.PROCESS_TYPE_GCS_SERVER in self.all_processes:
        self._kill_process_type(
            ray_constants.PROCESS_TYPE_GCS_SERVER,
            check_alive=check_alive,
            allow_graceful=allow_graceful,
            wait=wait,
        )

    # 3. 杀其他所有进程 (reaper 除外)
    for process_type in list(self.all_processes.keys()):
        if process_type != ray_constants.PROCESS_TYPE_REAPER:
            self._kill_process_type(
                process_type,
                check_alive=check_alive,
                allow_graceful=allow_graceful,
                wait=wait,
            )

    # 4. 最后杀 reaper
    if ray_constants.PROCESS_TYPE_REAPER in self.all_processes:
        self._kill_process_type(
            ray_constants.PROCESS_TYPE_REAPER,
            check_alive=check_alive,
            allow_graceful=allow_graceful,
            wait=wait,
        )
```

**设计原因**（代码注释）：

```python
# Kill the raylet first. This is important for suppressing errors at
# shutdown because we give the raylet a chance to exit gracefully and
# clean up its child worker processes. If we were to kill the plasma
# store (or Redis) first, that could cause the raylet to exit
# ungracefully, leading to more verbose output from the workers.
```

### 8.2 单个进程的 Kill 策略

**文件：** `python/ray/_private/node.py:1545-1600`

```python
def _kill_process_impl(self, process_type, allow_graceful=False,
                       check_alive=True, wait=False):
    for process_info in process_infos:
        process = process_info.process
        if process.poll() is not None:
            if check_alive:
                raise RuntimeError(...)
            else:
                continue

        if allow_graceful:
            process.terminate()  # 发送 SIGTERM
            timeout_seconds = 1
            try:
                process.wait(timeout_seconds)
            except subprocess.TimeoutExpired:
                pass

        # 如果进程还没退出，强制 kill
        if process.poll() is None:
            process.kill()  # 发送 SIGKILL
            if wait:
                process.wait()

    del self.all_processes[process_type]
```

| `allow_graceful` | 行为 |
|:-:|---|
| `True` | SIGTERM → 等 1 秒 → 未退出则 SIGKILL |
| `False` | 直接 SIGKILL，不等待 |

### 8.3 `ray stop` 的杀进程顺序（对比）

**文件：** `python/ray/autoscaler/_private/constants.py:109-132`

```python
RAY_PROCESSES = [
    ["raylet", True],         # Bucket 1: 先杀
    ["plasma_store", True],
    ["monitor.py", False],
    # ... 其他进程 ...         # Bucket 2: 中间杀
    ["gcs_server", True],     # Bucket 3: 最后杀
]
```

`ray stop` 杀进程的顺序分 3 桶：
- Bucket 1：raylet、plasma_store（先杀，避免 fate-sharing agent 报错）
- Bucket 2：其他所有进程（monitor、workers、log_monitor、dashboard、agents、reaper）
- Bucket 3：gcs_server（最后杀，否则其他进程可能因 GCS 不可用而异常退出）

---

## 9. 全场景汇总表

| 场景 | 触发信号 | 死亡进程 returncode | Python 检测 | Python 处理 | raylet workers | 容器结果 |
|------|---------|---------------------|------------|------------|---------------|---------|
| raylet SIGTERM | SIGTERM(15) | 0 / -15 / 143 | expected → 忽略 | 无操作 | raylet 自己杀 | **容器不死，节点残废** |
| raylet SIGKILL | SIGKILL(9) | -9 | unexpected | kill_all(allow_graceful=False) + `os._exit(1)` | **没机会杀，变孤儿** | exit(1)，K8s 重启 |
| raylet SIGSEGV | SIGSEGV(11) | -11 | unexpected | 同上 | **变孤儿** | exit(1)，K8s 重启 |
| raylet SIGABRT | SIGABRT(6) | -6 | unexpected | 同上 | **变孤儿** | exit(1)，K8s 重启 |
| GCS SIGTERM | SIGTERM(15) | 0 / -15 / 143 | expected → 忽略 | 无操作 | GCS 死后 raylet 可能连锁退出 | **容器不死但可能空转** |
| GCS SIGKILL | SIGKILL(9) | -9 | unexpected | kill_all(allow_graceful=False) + `os._exit(1)` | raylet 被先 SIGKILL，没机会杀 | exit(1)，K8s 重启 |
| GCS 崩溃 | SIGSEGV等 | 非 expected | unexpected | 同上 | 同上 | exit(1)，K8s 重启 |
| PID 1 SIGTERM | SIGTERM(15) | N/A | N/A | kill_all(allow_graceful=True) + `sys.exit(1)` | 先 SIGTERM 等 1s 再 SIGKILL | exit(1)，K8s 重启 |
| PID 1 SIGKILL | SIGKILL(9) | N/A | N/A | Reaper 检测 → SIGTERM pgroup → 等 1s → SIGKILL | 被 Reaper 杀 | 容器被杀 |
| PID 1 崩溃 | SIGSEGV等 | N/A | N/A | Reaper 检测 (stdin EOF 或 PDEATHSIG) → 同上 | 被 Reaper 杀 | 容器被杀 |

---

## 10. 已知隐患与半死状态

### 10.1 隐患 1：Raylet SIGTERM 后 `--block` 不退出

**现象：** raylet 被 SIGTERM 优雅退出后，`--block` 循环将 returncode 视为 expected，不触发任何操作。容器继续运行，但节点已无 raylet，无法调度新 worker，已运行的 worker 也可能逐步失败。

**影响：** K8s 不会重启容器（PID 1 还活着），节点进入半死状态。

**可能的缓解方案：**
- 在 `--block` 循环中对 raylet 以 expected code 退出也做特殊处理
- 利用 K8s liveness probe 检测 raylet 健康状态

### 10.2 隐患 2：GCS SIGTERM 后全节点空转

**现象：** GCS 被 SIGTERM 退出后，Head 节点上的 raylet 因 GCS 心跳超时也可能退出。如果 raylet 也以 expected code 退出，`--block` 同样不触发杀进程。最终 `while True` 空转，容器活着但全节点功能丧失。

**影响：** Head 节点看似存活但完全不可用，且 K8s 不会重启。

### 10.3 隐患 3：Raylet SIGKILL 后 Worker 孤儿进程

**现象：** raylet 被 SIGKILL 后，Python 层的 `kill_all_processes` 只杀 Python 直接 spawn 的子进程（raylet、GCS、dashboard 等），**不杀 raylet 的孙子进程**（即 raylet spawn 的 worker 进程）。

**影响：** Worker 进程变成孤儿，可能继续运行、占用资源，直到容器被 K8s 重启。

### 10.4 隐患 4：`os._exit(1)` 跳过 atexit

**现象：** 异常退出路径使用 `os._exit(1)` 而非 `sys.exit(1)`，跳过所有 Python atexit handler。

**影响：** atexit handler 中注册的 `kill_all_processes(allow_graceful=True)` 不会被执行。但代码在调用 `os._exit(1)` 前已经手动调用了 `kill_all_processes`，所以这实际上是设计意图——确保进程立刻死，不被 atexit 拖慢。

---

## 11. 容器退出码与重启关系

### 11.1 退出码传递链

```
raylet/GCS 异常退出 (returncode = -9 等)
  → Python --block 循环检测到 unexpected
  → os._exit(1)          ← PID 1 退出码 = 1
  → 容器 exit code = 1   ← 容器的 exit code 就是 PID 1 的退出码
  → K8s 看到非零退出码
```

### 11.2 K8s restartPolicy 对重启的决定

| restartPolicy | 退出码=0 | 退出码≠0 |
|:---:|:---:|:---:|
| Always | 重启 | 重启 |
| OnFailure | 不重启 | 重启 |
| Never | 不重启 | 不重启 |

### 11.3 容器重启 vs Pod 重建

| | 容器重启 | Pod 重建 |
|---|---|---|
| Pod 对象 | 不变 | 删除后新建 |
| Pod IP | 不变 | 新 IP |
| Pod 名字 | 不变 | 新名字 |
| Container ID | 变 | 变 |
| restartCount | ++ | 重置为 0 |
| 文件系统 | 重置为镜像原始状态 | 重置为镜像原始状态 |

### 11.4 CrashLoopBackOff

如果容器每次重启后都立即退出（例如镜像缺少 raylet binary），K8s 会进入 CrashLoopBackOff 状态：
- 重启延迟指数递增：1s → 2s → 4s → 8s → ... → 最大 5min
- `kubectl describe pod` 可看到 `Back-off restarting failed container`
- 此时需要修复镜像或配置，而不是反复重启

---

---

## 12. Kill Raylet 方式对比分析

### 12.1 方式对比表

| 方式 | 可靠性 | 容器是否重建 | 原因 |
|------|:------:|:----------:|------|
| `drain_node` | ❌ 不可靠 | 取决于实现 | V1 直接杀 raylet (等同于 SIGTERM)；V2 标记 draining，等 idle 后再杀。都不直接处理 plasma_store，对象仍可通过 plasma_store 访问 |
| `kill -9 PID 1` | ❌ 无效 | N/A | Linux 内核对 PID 1 的 SIGKILL 免疫（`kill -9 1` 不会杀死 init 进程） |
| `kill -9 raylet`（不 rename） | ❌ 容器重建 | ✅ 重建 | returncode=-9 ∉ `expected_return_codes` → PID 1 执行 `os._exit(1)` → 容器 exit(1) |
| `rename raylet` + `kill -9` | ❌ 容器重建 | ✅ 重建 | PID 1 无法重启 raylet（无重启逻辑）→ raylet 死后 returncode=-9 → `os._exit(1)` |
| `chmod 000 raylet` + `kill -9` | ❌ 容器重建 | ✅ 重建 | 同上，raylet 死后 returncode=-9 → PID 1 `os._exit(1)` |
| `kill -15 raylet` | ✅ 可靠 | ❌ 不重建 | returncode=-15 ∈ `expected_return_codes` → PID 1 不退出 → 容器不重建 |

### 12.2 各方式详细分析

#### `drain_node`

**文件：** `src/ray/gcs/gcs_node_manager.cc:220-237`

V1 版本的 `DrainNode()` 实际上是直接发 `ShutdownRaylet(graceful=true)` RPC：

```cpp
// NOTE(sang): Drain API is not supposed to kill the raylet, but we are doing
// this until the proper "drain" behavior is implemented.
raylet_client->ShutdownRaylet(
    node_id,
    /*graceful*/ true,
    [node_id](const Status &status, const rpc::ShutdownRayletReply &reply) {
      RAY_LOG(INFO).WithField(node_id) << "Raylet is drained. Status " << status;
    });
```

代码注释明确说：**Drain API 设计上不应该杀 raylet，但当前实现是直接杀**（临时方案）。

V2 版本（`GcsAutoscalerStateManager::HandleDrainNode`）发送 `DrainRaylet` RPC，raylet 进入 draining 状态：
- 停止接受新任务
- 等待当前任务完成
- 当节点 idle 且无 pinned objects 时自动 shutdown
- 但**不杀 plasma_store**，对象仍可通过 plasma_store 直接访问

**结论：** `drain_node` 的语义是"排空节点"而非"杀进程"，且不直接影响 plasma_store 生命周期。

---

#### `kill -9 PID 1`

Linux 内核对 PID 1 有特殊保护：

```
# 尝试 SIGKILL PID 1
$ kill -9 1
# 无效：内核忽略对 PID 1 的 SIGKILL
# PID 1 不会被杀死，容器继续运行
```

原因：PID 1 是 init 进程，内核设计上不允许 SIGKILL PID 1，防止系统/容器失去 init 进程导致孤儿进程泛滥。

---

#### `kill -9 raylet`（不 rename）

```
kill -9 <raylet_pid>
  │
  ├─ raylet 立即死亡，returncode=-9
  │
  ├─ --block 循环检测：-9 ∉ expected_return_codes
  │
  ├─ kill_all_processes(allow_graceful=False) + os._exit(1)
  │
  └─ 容器 exit(1) → K8s 重启容器
```

**问题：** 容器重建后 raylet 重新启动，但旧 NodeID 已被 GCS 标记为 dead（健康检查超时），新 raylet 以新 NodeID 注册。如果有对象副本在旧节点上，GCS 会清除旧节点的 location。

---

#### `rename raylet` + `kill -9`

目的：让 raylet 死后无法被自动重启（如果有人实现了重启逻辑的话）。

但在当前代码中，`--block` 循环**没有任何重启逻辑**，所以 rename 与否不影响结果——raylet 死后 PID 1 都会 `os._exit(1)`。

```
rename raylet_binary raylet_binary.bak
kill -9 <raylet_pid>
  │
  ├─ raylet 死亡 (returncode=-9)
  │
  ├─ --block: unexpected → os._exit(1)
  │
  └─ 容器 exit(1) → K8s 重启
       │
       └─ 重启后 raylet binary 从镜像恢复（容器文件系统重置）
          → 如果镜像本身缺 binary → CrashLoopBackOff
```

---

#### `chmod 000 raylet` + `kill -9`

与 rename 类似，当前代码无重启逻辑，结果相同。

如果未来有人加重启逻辑：`chmod 000` 会导致 `exec(raylet)` 返回 `EACCES`，重启失败 → PID 1 退出 → 容器重建。

---

#### `kill -15 raylet` ✅

```
kill -15 <raylet_pid>
  │
  ├─ raylet 收到 SIGTERM
  │    ├─ signal_handler() 构造 NodeDeathInfo
  │    ├─ shutdown_raylet_gracefully()
  │    │    ├─ gcs_client->Nodes().UnregisterSelf()
  │    │    │    └─ GCS 立即标记节点 dead (EXPECTED_TERMINATION)
  │    │    ├─ node_manager->Stop() (杀 workers, 停 agents)
  │    │    ├─ gcs_client->Disconnect()
  │    │    ├─ main_service.stop()
  │    │    └─ remove(raylet_socket_name)
  │    │
  │    └─ raylet 进程退出 (returncode = -15)
  │
  ├─ --block 循环检测：-15 ∈ expected_return_codes → 忽略
  │
  └─ PID 1 继续运行，容器不重建
       │
       ├─ plasma_store 仍在运行 → 对象仍可访问
       │
       └─ GCS 已收到 UnregisterSelf → 标记节点 dead
            ├─ 清除该节点的资源
            ├─ 重建该节点上的 actors
            ├─ 清除对象 location 中该节点的条目
            └─ 触发对象恢复 (reconstruction)
```

**关键优势：**
1. raylet 优雅退出 → 有机会杀 child workers、清理 socket、注销 GCS
2. PID 1 不退出 → 容器不重建 → plasma_store 继续运行 → 对象仍可访问
3. GCS 立即得知节点死亡（非心跳超时）→ 快速触发对象恢复
4. 如果对象有副本在 stable 节点，恢复速度快；无副本则需 reconstruction

---

## 13. GCS 健康检查机制与节点死亡检测

### 13.1 健康检查参数

**文件：** `src/ray/common/ray_config_def.h:906-912`

```cpp
RAY_CONFIG(int64_t, health_check_initial_delay_ms, 5000)    // 首次检查延迟: 5s
RAY_CONFIG(int64_t, health_check_period_ms, 3000)           // 检查间隔: 3s
RAY_CONFIG(int64_t, health_check_timeout_ms, 10000)          // 检查超时: 10s
RAY_CONFIG(int64_t, health_check_failure_threshold, 5)       // 失败阈值: 5次
```

### 13.2 最坏检测延迟计算

```
最坏情况检测延迟 = initial_delay_ms + failure_threshold × max(period_ms, timeout_ms)
                 = 5000 + 5 × max(3000, 10000)
                 = 5000 + 5 × 10000
                 = 55,000 ms (55 秒)

理想情况 (每次 check 快速返回错误):
检测延迟 = initial_delay_ms + failure_threshold × period_ms
         = 5000 + 5 × 3000
         = 20,000 ms (20 秒)
```

### 13.3 健康检查工作流程

**文件：** `src/ray/gcs/gcs_health_check_manager.cc`

```
节点注册 → GCS AddNode() → gcs_healthcheck_manager_->AddNode(node_id, channel)
  │
  ├─ 创建 HealthCheckContext
  │    └─ health_check_remaining_ = failure_threshold (5)
  │
  ├─ 等待 initial_delay_ms (5s)
  │
  └─ 进入检查循环:
       │
       ├─ 检查 latest_known_healthy_timestamp_
       │    └─ 如果 RaySyncer 在 period_ms 内报告过该节点活跃 → 跳过本次检查
       │
       ├─ 发送 gRPC Health::Check 请求 (deadline = timeout_ms = 10s)
       │
       ├─ 结果处理:
       │    ├─ 成功 (SERVING): health_check_remaining_ = failure_threshold (重置为5)
       │    └─ 失败: health_check_remaining_-- (5→4→3→2→1→0)
       │
       ├─ health_check_remaining_ == 0:
       │    └─ FailNode(node_id) → node_death_callback_
       │         └─ gcs_node_manager_->OnNodeFailure(node_id, nullptr)
       │
       └─ health_check_remaining_ > 0:
            └─ 等 period_ms (3s) → 继续检查
```

### 13.4 RaySyncer 的健康信号优化

**文件：** `src/ray/gcs/gcs_server.cc:591`

```cpp
ray_syncer_ = std::make_unique<syncer::RaySyncer>(
    ...
    [this](const NodeID &node_id) {
      gcs_healthcheck_manager_->MarkNodeHealthy(node_id);
    });
```

当 GCS 收到某节点的资源同步消息时，调用 `MarkNodeHealthy` 更新 `latest_known_healthy_timestamp_`。如果同步消息在 `period_ms` 内到达，健康检查被跳过。

**优化效果：** 正常运行的节点几乎不需要实际发送 gRPC 健康检查请求，减少了 GCS 和 raylet 之间的网络开销。只有当节点真正静默时（无 sync 消息 + 健康检查失败），才触发死亡检测。

### 13.5 两种节点死亡路径对比

| 方面 | 显式注销 (SIGTERM → UnregisterSelf) | 健康检查超时 (SIGKILL/崩溃) |
|------|:----------------------------------:|:------------------------:|
| 触发方式 | raylet 主动调用 `UnregisterSelf()` | GCS 健康检查连续 5 次失败 |
| 死亡原因 | `EXPECTED_TERMINATION` 或 `AUTOSCALER_DRAIN_PREEMPTED` | `UNEXPECTED_TERMINATION` ("health check failed due to missing too many heartbeats") |
| 检测延迟 | **立即** (RPC 同步) | **20~55 秒** |
| 广播 RAY_NODE_REMOVED | **否** | **是** (广播到所有 drivers) |
| GCS 监听器触发 | 相同 | 相同 |

### 13.6 GCS 收到 UnregisterSelf 后的连锁反应

**文件：** `src/ray/gcs/gcs_node_manager.cc:157-198`

```cpp
void GcsNodeManager::HandleUnregisterNode(rpc::UnregisterNodeRequest request, ...) {
  NodeID node_id = NodeID::FromBinary(request.node_id());
  auto node = RemoveNodeFromCache(
      node_id, request.node_death_info(), rpc::GcsNodeInfo::DEAD, current_sys_time_ms());
  AddDeadNodeToCache(node);
  // 持久化到存储 + 发布到 pubsub
  gcs_table_storage_->NodeTable().Put(node_id, *node, {on_put_done, io_context_});
}
```

**文件：** `src/ray/gcs/gcs_server.cc:844-860` (NodeRemoved 监听器)

```cpp
gcs_node_manager_->AddNodeRemovedListener(
    [this](const std::shared_ptr<const rpc::GcsNodeInfo> &node) {
      auto node_id = NodeID::FromBinary(node->node_id());
      gcs_resource_manager_->OnNodeDead(node_id);           // 清除资源
      gcs_placement_group_manager_->OnNodeDead(node_id);     // 重建 PG
      gcs_actor_manager_->OnNodeDead(node, node_ip_address); // 重建 actors
      gcs_job_manager_->OnNodeDead(node_id);                 // 清理 jobs
      raylet_client_pool_.Disconnect(node_id);              // 断开连接池
      worker_client_pool_.Disconnect(node_id);               // 断开 worker 连接池
      gcs_healthcheck_manager_->RemoveNode(node_id);        // 停止健康检查
      pubsub_handler_->AsyncRemoveSubscriberFrom(...);      // 移除订阅
      gcs_autoscaler_state_manager_->OnNodeDead(node_id);   // 通知 autoscaler
    });
```

**文件：** `src/ray/gcs/gcs_node_manager.cc:686-716` (OnNodeFailure 心跳超时路径)

```cpp
void GcsNodeManager::InternalOnNodeFailure(const NodeID &node_id, ...) {
  auto maybe_node = GetAliveNodeFromCache(node_id);
  if (maybe_node.has_value()) {
    rpc::NodeDeathInfo death_info = InferDeathInfo(node_id);
    // InferDeathInfo 逻辑:
    //   - 如果节点在 draining 且 deadline 已过期 且 reason=PREEMPTION
    //     → AUTOSCALER_DRAIN_PREEMPTED
    //   - 否则 → UNEXPECTED_TERMINATION, "health check failed due to missing too many heartbeats"
    auto node = RemoveNodeFromCache(node_id, death_info, ...);
    AddDeadNodeToCache(node);
    // 持久化 + 发布
  }
}
```

### 13.7 对象 Location 清除机制

当 GCS 标记节点 dead 后，对象 location 的清除不是由 GCS 直接操作，而是由各 Core Worker 的 ReferenceCounter 在检测到节点死亡后清除：

**文件：** `src/ray/core_worker/reference_counter.cc:937-945`

```cpp
// 当对象 owner 检测到 pin 节点已死
if (!is_node_dead_(node_id)) {
  it->second.pinned_at_node_id_ = node_id;  // 节点还活着，保留 location
} else {
  UnsetObjectPrimaryCopy(it);               // 清除 primary location
  objects_to_recover_.push_back(object_id);  // 加入恢复队列
}
```

**文件：** `src/ray/core_worker/reference_counter.cc:1542-1553`

```cpp
// 当 spilled 节点已死
bool spilled_location_alive =
    spilled_node_id.IsNil() || !is_node_dead_(spilled_node_id);
if (spilled_location_alive) {
  it->second.spilled_url = spilled_url;       // 保留 spilled location
} else {
  // spilled location 也死了 → 对象需要恢复
}
```

**`is_node_dead_` 回调来源：** Core Worker 构造时注入，通过 GCS pubsub 订阅节点状态变化。

---

## 14. drain_node 机制详解

### 14.1 V1 Drain (GcsNodeManager::DrainNode)

**文件：** `src/ray/gcs/gcs_node_manager.cc:220-237`

```cpp
void GcsNodeManager::DrainNode(const NodeID &node_id) {
  RAY_LOG(INFO).WithField(node_id) << "DrainNode() for node";
  auto maybe_node = GetAliveNode(node_id);
  if (!maybe_node.has_value()) {
    RAY_LOG(WARNING).WithField(node_id) << "Skip draining node which is already removed";
    return;
  }
  auto &node = maybe_node.value();
  auto remote_address = rpc::RayletClientPool::GenerateRayletAddress(...);
  auto raylet_client = raylet_client_pool_->GetOrConnectByAddress(remote_address);
  RAY_CHECK(raylet_client);
  // NOTE(sang): Drain API is not supposed to kill the raylet, but we are doing
  // this until the proper "drain" behavior is implemented.
  raylet_client->ShutdownRaylet(
      node_id,
      /*graceful*/ true,
      [node_id](const Status &status, const rpc::ShutdownRayletReply &reply) {
        RAY_LOG(INFO).WithField(node_id) << "Raylet is drained. Status " << status;
      });
}
```

**行为：** 直接发送 `ShutdownRaylet(graceful=true)` → raylet 收到后执行 `shutdown_raylet_gracefully_()` → 优雅退出。

**等价于：** `kill -15 raylet`，但通过 RPC 通道而非信号。

### 14.2 V2 Drain (GcsAutoscalerStateManager::HandleDrainNode)

**文件：** `src/ray/gcs/gcs_autoscaler_state_manager.cc:~460-525`

```cpp
void GcsAutoscalerStateManager::HandleDrainNode(
    rpc::autoscaler::DrainNodeRequest request, ...) {
  const NodeID node_id = NodeID::FromBinary(request.node_id());
  auto maybe_node = gcs_node_manager_.GetAliveNode(node_id);
  if (!maybe_node.has_value()) {
    reply->set_is_accepted(true);  // 已死，视为 drained
    return;
  }
  // 标记 actors 为 preempted
  gcs_actor_manager_.SetPreemptedAndPublish(node_id);
  // 发送 DrainRaylet RPC (非 ShutdownRaylet)
  raylet_client->DrainRaylet(
      request.reason(), request.reason_message(),
      draining_deadline_timestamp_ms, ...);
}
```

### 14.3 Raylet 侧：HandleDrainRaylet

**文件：** `src/ray/raylet/node_manager.cc:2141-2202`

```cpp
void NodeManager::HandleDrainRaylet(rpc::DrainRayletRequest request, ...) {
  if (cluster_resource_scheduler_.GetLocalResourceManager().IsLocalNodeDraining()) {
    reply->set_is_accepted(true);  // 已经在 draining
    return;
  }

  if (request.reason() == DRAIN_NODE_REASON_IDLE_TERMINATION) {
    const bool is_idle = cluster_resource_scheduler_
        .GetLocalResourceManager().IsLocalNodeIdle();
    if (is_idle) {
      // 接受 drain
      cluster_resource_scheduler_.GetLocalResourceManager().SetLocalNodeDraining(request);
      reply->set_is_accepted(true);
    } else {
      // 拒绝：节点不空闲
      reply->set_is_accepted(false);
      reply->set_rejection_reason_message("The node to be idle terminated is no longer idle.");
    }
  } else {
    // DRAIN_NODE_REASON_PREEMPTION - 不可拒绝
    cluster_resource_scheduler_.GetLocalResourceManager().SetLocalNodeDraining(request);
    reply->set_is_accepted(true);
  }

  if (is_drain_accepted) {
    // 取消所有 local lease，重新入队调度
    auto cancelled_works = local_lease_manager_.CancelLeasesWithoutReply(...);
    for (const auto &work : cancelled_works) {
      cluster_lease_manager_.QueueAndScheduleLease(work->lease_, ...);
    }
  }
}
```

### 14.4 Draining 状态到 Shutdown 的触发

**文件：** `src/ray/raylet/scheduling/local_resource_manager.cc:447-478`

```cpp
void LocalResourceManager::OnResourceOrStateChanged() {
  if (IsLocalNodeDraining() && IsLocalNodeIdle()) {
    bool ready_to_shutdown = true;

    // Object-aware drain: 如果有 pinned objects，延迟 shutdown
    if (RayConfig::instance().enable_object_aware_drain() &&
        has_pinned_objects_ && has_pinned_objects_()) {
      ready_to_shutdown = false;
      // 等待对象被消费/迁移，超时后触发主动迁移
    }

    if (ready_to_shutdown) {
      RAY_LOG(INFO) << "The node is drained, continue to shut down raylet...";
      rpc::NodeDeathInfo node_death_info = DeathInfoFromDrainRequest();
      shutdown_raylet_gracefully_(std::move(node_death_info));
    }
  }
}
```

**V2 Drain 完整流程：**

```
DrainRaylet RPC → HandleDrainRaylet()
  │
  ├─ SetLocalNodeDraining(request)  ← 标记节点为 draining
  │    └─ 停止接受新 lease
  │    └─ CancelLeasesWithoutReply() → 取消已有 lease，重新调度到其他节点
  │
  └─ 进入等待循环 (OnResourceOrStateChanged 每次资源变化时检查):
       │
       ├─ IsLocalNodeDraining() && IsLocalNodeIdle()?
       │    ├─ 否 → 继续等待
       │    │
       │    └─ 是 → 检查 pinned objects:
       │         ├─ 有 pinned objects 且 enable_object_aware_drain:
       │         │    └─ 等待对象被消费/迁移 (超时后主动迁移)
       │         │
       │         └─ 无 pinned objects (或已迁移完):
       │              └─ shutdown_raylet_gracefully_() → 优雅退出
       │
       └─ raylet 退出后 → PID 1 --block 循环检测 expected code → 容器不重建
```

### 14.5 V1 vs V2 Drain 对比

| 方面 | V1 Drain | V2 Drain |
|------|----------|----------|
| RPC 类型 | `ShutdownRaylet` (直接杀) | `DrainRaylet` (标记 draining) |
| 杀 raylet 时机 | **立即** | 等 idle + 无 pinned objects |
| 对象保护 | 无 | 有 (enable_object_aware_drain 时延迟 shutdown) |
| 取消 lease | 无 | 有 (CancelLeasesWithoutReply → 重新调度) |
| 代码注释 | "临时方案，直到 proper drain 实现为止" | 正式 drain 实现 |

---

## 15. `kill -15 raylet` 后的完整时间线

### 15.1 时间线总览

| 时间 | 事件 | client_locations (有副本) | client_locations (无副本) |
|:----:|------|:------------------------:|:------------------------:|
| t=0 | `kill -15 raylet` → raylet 收到 SIGTERM | [tidal, stable] | [tidal] |
| t~0+ | raylet 执行 graceful shutdown：UnregisterSelf → GCS 标记节点 dead → 杀 workers → 停 agents | [tidal, stable] | [tidal] |
| t~0+ | raylet 进程退出，returncode=-15 | [tidal, stable] | [tidal] |
| t~1s | PID 1 `--block` 循环检测 raylet 退出 (expected code) → 忽略，继续运行 | [tidal, stable] | [tidal] |
| t~1s | GCS node removed listeners 触发：OnNodeDead → 清除资源/重建 actors/actors | [tidal, stable] | [tidal] |
| t~1s+ | Core Workers 通过 pubsub 收到节点 dead 通知 → `is_node_dead_(tidal_node_id)=true` | [tidal, stable] | [tidal] |
| t~1s+ | ReferenceCounter 检测到 pin 节点 (tidal) 已死 → 清除 tidal location → 加入 `objects_to_recover_` | [stable] | [] |
| t~1s+ | 有副本：owner 从 stable 节点获取对象 → 恢复成功 | **[stable]** ✅ | - |
| t~1s+ | 无副本：owner 发现无可用 location → 触发对象 reconstruction | - | ⏳ 重算中 |
| t~30~55s | (对比) 如果用 `kill -9`，GCS 健康检查需要 20~55s 才能检测到节点死亡 | 延迟 20~55s 后才清除 tidal location | 延迟 20~55s 后才触发 reconstruction |

### 15.2 关键时间差：`kill -15` vs `kill -9` 的 GCS 检测延迟

```
kill -15 raylet:
  ┌───────────────────────────────────────────────┐
  │ t=0: SIGTERM → raylet graceful shutdown       │
  │ t=0+: UnregisterSelf → GCS 立即标记 dead     │
  │ t=~1s: Core Workers 收到通知，清除 location   │
  │ t=~1s: 有副本 → 立即恢复 ✅                   │
  └───────────────────────────────────────────────┘
  恢复延迟: ~1 秒

kill -9 raylet:
  ┌───────────────────────────────────────────────┐
  │ t=0: SIGKILL → raylet 立即死亡               │
  │ t=0+: PID 1 os._exit(1) → 容器终止           │
  │ t=0+: GCS 开始健康检查失败                    │
  │ t=5s: 首次健康检查 (initial_delay=5s)         │
  │ t=5s~55s: 连续 5 次失败                       │
  │ t=20~55s: GCS 标记节点 dead                   │
  │ t=20~55s: Core Workers 收到通知               │
  │ t=20~55s: 清除 location → 触发恢复            │
  └───────────────────────────────────────────────┘
  恢复延迟: 20~55 秒
```

### 15.3 `kill -15` 后 plasma_store 的角色

```
kill -15 raylet → raylet 退出
  │
  ├─ plasma_store 仍由 PID 1 管理 (作为独立子进程)
  │    └─ PID 1 (--block) 仍在运行 → 不会杀 plasma_store
  │
  ├─ plasma_store 继续运行 → 对象仍可通过 plasma_store 客户端访问
  │    │
  │    ├─ 如果 Core Worker 仍持有 plasma store 客户端连接:
  │    │    └─ 可以直接从 plasma_store 读取对象 (但 raylet 不再调度)
  │    │
  │    └─ 如果 Core Worker 也随 raylet 退出 (被 raylet 杀了):
  │         └─ 无法通过 worker 访问，但 plasma_store 仍在内存中持有对象
  │
  └─ 对象 location 更新:
       ├─ GCS 已标记节点 dead
       ├─ Core Worker 的 ReferenceCounter 清除 tidal location
       └─ 但 plasma_store 中的对象仍物理存在 (直到容器最终被销毁)
```

### 15.4 对象恢复的完整路径

**有副本 (client_locations: [tidal, stable] → [stable]):**

```
1. GCS 标记 tidal 节点 dead
2. Core Worker ReferenceCounter 检测 is_node_dead_(tidal_node_id) = true
3. 清除 tidal location: UnsetObjectPrimaryCopy(it)
4. 加入 objects_to_recover_ 队列
5. Owner 尝试从剩余 location (stable) 获取对象
6. stable 节点响应 → 对象恢复成功 ✅
```

**无副本 (client_locations: [tidal] → []):**

```
1. GCS 标记 tidal 节点 dead
2. Core Worker ReferenceCounter 检测 is_node_dead_(tidal_node_id) = true
3. 清除 tidal location: 所有 location 为空
4. 加入 objects_to_recover_ 队列
5. 触发对象 reconstruction (需要 lineage 信息)
6. 从原始数据源或上游任务重新计算 ⏳ (可能很慢或失败)
```

### 15.5 `kill -15` 为什么是最优方式

| 优势 | 说明 |
|------|------|
| **容器不重建** | returncode=-15 ∈ expected → PID 1 不退出 → 容器存活 → 无重启开销 |
| **GCS 立即感知** | raylet UnregisterSelf → GCS 立即标记 dead → 1s 内触发恢复 |
| **raylet 清理 workers** | raylet 有机会杀 child workers、停 agents、清 socket → 无孤儿进程 |
| **plasma_store 存活** | 对象仍物理存在于内存中，可被其他节点读取或迁移 |
| **与 `kill -9` 的恢复延迟差** | 1s vs 20~55s，快 20~50 倍 |

---

## 附录：关键文件索引

### A. 进程管理相关

| 文件 | 关键行 | 内容 |
|------|--------|------|
| `python/ray/scripts/scripts.py` | 1161-1228 | `--block` 监控循环 |
| `python/ray/_private/node.py` | 983-999 | `start_reaper_process()` |
| `python/ray/_private/node.py` | 1066-1109 | `start_gcs_server()` |
| `python/ray/_private/node.py` | 1209-1235 | `start_monitor()` |
| `python/ray/_private/node.py` | 1236-1258 | `start_ray_client_server()` |
| `python/ray/_private/node.py` | 1021-1065 | `start_api_server()` (dashboard) |
| `python/ray/_private/node.py` | 1000-1020 | `start_log_monitor()` |
| `python/ray/_private/node.py` | 1111-1208 | `start_raylet()` |
| `python/ray/_private/node.py` | 516-527 | shutdown hooks 注册 |
| `python/ray/_private/node.py` | 1450-1510 | `_kill_process_impl` 单进程杀逻辑 |
| `python/ray/_private/node.py` | 1540-1580 | `kill_all_processes` 全进程杀逻辑 |
| `python/ray/_private/node.py` | 1700-1712 | `dead_processes` 死进程检测 |
| `python/ray/_private/ray_process_reaper.py` | 全文 | Reaper 机制 |
| `python/ray/_private/services.py` | 1082-1120 | Reaper 启动 |
| `python/ray/_private/ray_constants.py` | 296-306 | 进程类型常量定义 |
| `python/ray/autoscaler/_private/constants.py` | 109-132 | `ray stop` 杀进程顺序 |

### B. Raylet / GCS 信号处理

| 文件 | 关键行 | 内容 |
|------|--------|------|
| `src/ray/raylet/main.cc` | 457-490 | raylet graceful shutdown lambda |
| `src/ray/raylet/main.cc` | 1141-1170 | raylet SIGTERM handler |
| `src/ray/raylet/node_manager.cc` | 2945-2970 | NodeManager::Stop() |
| `src/ray/gcs/gcs_server_main.cc` | 237-257 | GCS SIGTERM handler |
| `src/ray/gcs/gcs_server.cc` | 324-349 | GcsServer::Stop() |

### C. Agent 管理 (fate-sharing)

| 文件 | 关键行 | 内容 |
|------|--------|------|
| `src/ray/raylet/agent_manager.cc` | 33-99 | AgentManager::StartAgent + monitor_thread |
| `src/ray/raylet/node_manager.cc` | 3290-3315 | CreateDashboardAgentManager |
| `src/ray/raylet/node_manager.cc` | 2141-2202 | HandleDrainRaylet |

### D. 健康检查与节点死亡检测

| 文件 | 关键行 | 内容 |
|------|--------|------|
| `src/ray/common/ray_config_def.h` | 906-912 | 健康检查参数定义 |
| `src/ray/gcs/gcs_health_check_manager.h` | 全文 | 健康检查管理器 |
| `src/ray/gcs/gcs_health_check_manager.cc` | 全文 | 健康检查实现 |
| `src/ray/gcs/gcs_node_manager.cc` | 157-198 | HandleUnregisterNode |
| `src/ray/gcs/gcs_node_manager.cc` | 539-564 | InferDeathInfo |
| `src/ray/gcs/gcs_node_manager.cc` | 680-716 | OnNodeFailure |
| `src/ray/gcs/gcs_server.cc` | 360-389 | InitGcsHealthCheckManager |
| `src/ray/gcs/gcs_server.cc` | 838-875 | InstallEventListeners (NodeRemoved) |

### E. 对象恢复与 Drain

| 文件 | 关键行 | 内容 |
|------|--------|------|
| `src/ray/core_worker/reference_counter.cc` | 937-945 | 对象 location 清除 (pin 节点死亡) |
| `src/ray/core_worker/reference_counter.cc` | 1542-1553 | 对象 location 清除 (spilled 节点死亡) |
| `src/ray/gcs/gcs_node_manager.cc` | 220-237 | V1 DrainNode (ShutdownRaylet) |
| `src/ray/gcs/gcs_autoscaler_state_manager.cc` | ~460-525 | V2 HandleDrainNode |
| `src/ray/raylet/scheduling/local_resource_manager.cc` | 447-478 | Draining→Shutdown 触发 |
