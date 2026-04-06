# Ray GCS 大规模集群问题排查完全指南

> 适用版本：Ray 2.52.1
> 覆盖场景：800~2500 节点规模集群的 GCS 性能瓶颈、FD 耗尽、调度阻塞、节点误判死亡
> 问题日期：2026-04-30 ~ 2026-05-16

---

## 目录

- [概述](#概述)
- [一、Ray 集群架构与 GCS 角色](#一ray-集群架构与-gcs-角色)
  - [1.1 整体架构](#11-整体架构)
  - [1.2 GCS 线程架构](#12-gcs-线程架构)
  - [1.3 Ray Data 两层调度架构](#13-ray-data-两层调度架构)
  - [1.4 Task 状态机](#14-task-状态机)
  - [1.5 心跳与健康检查机制](#15-心跳与健康检查机制)
  - [1.6 Actor 调度机制](#16-actor-调度机制)
- [二、案例一：FD 耗尽导致调度阻塞（2246 节点）](#二案例一fd-耗尽导致调度阻塞2246-节点)
  - [2.1 问题现象](#21-问题现象)
  - [2.2 排查思路](#22-排查思路)
  - [2.3 排查过程（完整交互记录）](#23-排查过程完整交互记录)
  - [2.4 根因分析](#24-根因分析)
  - [2.5 FD 耗尽的完整影响链](#25-fd-耗尽的完整影响链)
  - [2.6 端口与网络影响](#26-端口与网络影响)
  - [2.7 容器级 vs 进程级 ulimit](#27-容器级-vs-进程级-ulimit)
  - [2.8 解决方案](#28-解决方案)
- [三、案例二：GCS CPU 高与 Syncer 瓶颈（800+ 节点）](#三案例二gcs-cpu-高与-syncer-瓶颈800-节点)
  - [3.1 问题现象](#31-问题现象)
  - [3.2 GCS 负载分析](#32-gcs-负载分析)
  - [3.3 Syncer 瓶颈与级联影响](#33-syncer-瓶颈与级联影响)
  - [3.4 Actor 调度问题分析](#34-actor-调度问题分析)
  - [3.5 节点死亡案例分析](#35-节点死亡案例分析)
  - [3.6 健康检查 Connection refused 深度分析](#36-健康检查-connection-refused-深度分析)
- [四、GCS 相关源码深度分析](#四gcs-相关源码深度分析)
  - [4.1 io_context 分配策略](#41-io_context-分配策略)
  - [4.2 RaySyncer 连接与广播逻辑](#42-raysyncer-连接与广播逻辑)
  - [4.3 健康检查核心逻辑](#43-健康检查核心逻辑)
  - [4.4 节点死亡处理流程](#44-节点死亡处理流程)
  - [4.5 Actor 调度完整流程](#45-actor-调度完整流程)
  - [4.6 ClusterLeaseManager 调度逻辑](#46-clusterleasermanager-调度逻辑)
  - [4.7 Ray Data StreamingExecutor 控制循环](#47-ray-data-streamingexecutor-控制循环)
- [五、排查命令速查表](#五排查命令速查表)
- [六、配置参数与调优建议](#六配置参数与调优建议)
- [七、故障时间线汇总](#七故障时间线汇总)
- [八、总结与最佳实践](#八总结与最佳实践)

---

## 概述

本文整合了两次大规模 Ray 集群故障的完整排查过程：

| 案例 | 集群规模 | 核心问题 | 根因 |
|------|----------|----------|------|
| 案例一 | 2246 节点 | Task 调度阻塞 | GCS 进程 FD 耗尽（ulimit 设置过低） |
| 案例二 | 800+ 节点 | Actor 启动慢/节点误判死亡 | RaySyncer 单线程瓶颈 + 节点 OOM |

两个案例都指向同一个核心问题：**GCS 作为集群的"大脑"，在大规模场景下成为瓶颈**。理解 GCS 的内部架构是排查此类问题的关键。

---

## 一、Ray 集群架构与 GCS 角色

### 1.1 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│                     Ray 应用层                                │
│  Ray Serve │ Ray Data │ Ray Train │ Ray Tune │ RLlib        │
├─────────────────────────────────────────────────────────────┤
│                     Ray Core                                 │
│  Tasks │ Actors │ Objects │ Placement Groups                │
├─────────────────────────────────────────────────────────────┤
│                   集群基础设施层                               │
│  Raylet │ GCS │ Object Store │ Autoscaler                   │
└─────────────────────────────────────────────────────────────┘
```

**GCS（Global Control Service）** 是 Ray 集群的中心协调服务，负责：
- 集群资源视图管理与广播（RaySyncer）
- 节点注册/注销与健康检查
- Actor 生命周期与调度管理
- Task 状态追踪与事件发布
- Placement Group 管理
- Job 管理

**GCS 不可用 = 整个集群调度瘫痪**

### 1.2 GCS 线程架构

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              GCS Server                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐       │
│  │ server.poll*    │     │ server.poll*    │     │ server.poll*    │       │
│  │ (gRPC 线程 1)   │     │ (gRPC 线程 2)   │     │ (gRPC 线程 N)   │       │
│  └────────┬────────┘     └────────┬────────┘     └────────┬────────┘       │
│           │                       │                       │                 │
│           └───────────────────────┼───────────────────────┘                 │
│                                   │                                         │
│                                   ↓                                         │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                     主线程 (gcs_server, 默认 io_context)              │  │
│  │                                                                       │  │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐      │  │
│  │  │ GcsHealthCheck  │  │ GcsActorManager │  │ GcsNodeManager  │      │  │
│  │  │ Manager         │  │ GcsActorScheduler│  │                 │      │  │
│  │  └─────────────────┘  └─────────────────┘  └─────────────────┘      │  │
│  │                                                                       │  │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐      │  │
│  │  │ GcsResourceMgr  │  │ GcsPlacementGrp │  │ GcsJobManager   │      │  │
│  │  │                 │  │ Manager         │  │                 │      │  │
│  │  └─────────────────┘  └─────────────────┘  └─────────────────┘      │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐       │
│  │ray_syncer   │  │task_io      │  │pubsub_io    │  │ray_event    │       │
│  │_io_context  │  │_context     │  │_context     │  │_io_context  │       │
│  │(独立线程)   │  │(独立线程)   │  │(独立线程)   │  │(独立线程)   │       │
│  │             │  │             │  │             │  │             │       │
│  │ RaySyncer   │  │GcsTaskMgr   │  │GcsPublisher │  │EventRecorder│       │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘       │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### io_context 分配策略

```cpp
// src/ray/gcs/gcs_server_io_context_policy.h
struct GcsServerIOContextPolicy {
  // 专用 io_context（独立线程）
  constexpr static std::array<std::string_view, 4> kAllDedicatedIOContextNames{
      "task_io_context",        // GcsTaskManager
      "pubsub_io_context",      // GcsPublisher
      "ray_syncer_io_context",  // RaySyncer ← 800 节点集群中打满 89%
      "ray_event_io_context"    // RayEventRecorder
  };
  // 其他所有组件使用默认 io_context（主线程）
};
```

**关键点**：
- 只有 4 个组件有专用线程
- GcsHealthCheckManager、GcsActorScheduler、GcsNodeManager 等核心组件共享主线程
- RaySyncer 虽然有独立线程，但在大规模集群中可能成为单线程瓶颈

### 1.3 Ray Data 两层调度架构

Ray Data 的调度是一个**两层架构**：

#### 第 1 层：应用层调度（集中在 Driver 节点）

`StreamingExecutor` 在 driver 上的一个专用线程中运行控制循环，决定**何时**提交 task、**哪个算子**获得执行机会。

```python
# python/ray/data/_internal/execution/streaming_executor.py:569-665
def _scheduling_loop_step(self, topology: Topology) -> bool:
    """Run one step of the scheduling loop.
    This runs a few general phases:
        1. Waiting for the next task completion using ray.wait().
        2. Pulling completed refs into operator outqueues.
        3. Selecting and dispatching new inputs to operators.
    """
    self._resource_manager.update_usages()
    errored_blocks_per_op, _ = process_completed_tasks(topology, ...)

    while True:
        op = select_operator_to_run(topology, self._resource_manager, ...)
        if op is None:
            break
        topology[op].dispatch_next_task()  # ← 提交新 task
```

task 实际通过 `TaskPoolMapOperator._try_schedule_task()` 提交：

```python
# python/ray/data/_internal/execution/operators/task_pool_map_operator.py:108-143
def _try_schedule_task(self, bundle: RefBundle, strict: bool):
    dynamic_ray_remote_args = self._get_dynamic_ray_remote_args(input_bundle=bundle)
    gen = self._map_task.options(**dynamic_ray_remote_args).remote(
        self._map_transformer_ref,
        data_context, ctx,
        *bundle.block_refs,
        slices=bundle.slices,
        **self.get_map_task_kwargs(),
    )
    self._submit_data_task(gen, bundle)
```

#### 第 2 层：Ray Core 分布式调度（每个节点的 Raylet）

一旦 task 通过 `.remote()` 提交，driver 节点的 Raylet 做初始调度决策：

```cpp
// src/ray/raylet/scheduling/cluster_lease_manager.cc:196-244
void ClusterLeaseManager::ScheduleAndGrantLeases() {
  TryScheduleInfeasibleLease();
  for (auto shapes_it = leases_to_schedule_.begin(); ...) {
    auto &work_queue = shapes_it->second;
    for (auto work_it = work_queue.begin(); work_it != work_queue.end();) {
      auto scheduling_node_id = cluster_resource_scheduler_.GetBestSchedulableNode(
          lease.GetLeaseSpecification(), ...);
      if (scheduling_node_id.IsNil()) {
        break;  // 找不到可用节点，task 留在队列中
      }
      ScheduleOnNode(node_id, work);
    }
  }
}
```

如果本地满了，会 "spill back" 到远端节点：

```cpp
// src/ray/raylet/scheduling/cluster_lease_manager.cc:422-461
void ClusterLeaseManager::ScheduleOnNode(const NodeID &spillback_to,
                                         const std::shared_ptr<internal::Work> &work) {
  if (spillback_to == self_node_id_) {
    local_lease_manager_.QueueAndScheduleLease(work);  // 本地执行
    return;
  }
  // spillback 到远端节点
  auto node_info = get_node_info_(spillback_to);
  RAY_CHECK(node_info.has_value());
  for (const auto &reply_callback : work->reply_callbacks_) {
    auto reply = reply_callback.reply_;
    reply->mutable_retry_at_raylet_address()->set_ip_address(
        (*node_info).node_manager_address());
    reply->mutable_retry_at_raylet_address()->set_port((*node_info).node_manager_port());
    reply_callback.send_reply_callback_(Status::OK(), nullptr, nullptr);
  }
}
```

#### 两层调度的 GCS 依赖

```
Driver (StreamingExecutor)
  ↓
select_operator_to_run() → 选择算子
  ↓
TaskPoolMapOperator._try_schedule_task() → 调用 .remote()
  ↓
Ray Core (CoreWorker on driver node)
  ↓
Local Raylet (ClusterLeaseManager) → GetBestSchedulableNode
  ↓                                    ↑
  ↓                          GCS/RaySyncer 提供资源视图
  ↓
Target Node's Raylet (LocalLeaseManager) → 分配 Worker
  ↓
Worker 执行 task
```

- Raylet 通过 GCS（RaySyncer）获取集群资源视图
- Task 状态通过 GCS 广播
- 节点心跳通过 GCS 管理

### 1.4 Task 状态机

Dashboard 上的 "Waiting for scheduling" 实际对应 Ray Core 的多个状态：

```protobuf
// src/ray/protobuf/common.proto:900-942
enum TaskStatus {
  NIL = 0;
  PENDING_ARGS_AVAIL = 1;          // 等待依赖数据创建
  PENDING_NODE_ASSIGNMENT = 2;     // 等待调度器分配节点（"Waiting for scheduling"）
  PENDING_OBJ_STORE_MEM_AVAIL = 3; // 等待 Object Store 内存释放
  PENDING_ARGS_FETCH = 4;          // 依赖数据正在下载到目标节点
  SUBMITTED_TO_WORKER = 5;         // 已发送到 worker，在 worker 队列中
  RUNNING = 8;                     // 正在执行
  FINISHED = 11;                   // 完成
  FAILED = 12;                     // 失败
}
```

#### SUBMITTED_TO_WORKER 状态详解

`SUBMITTED_TO_WORKER` 表示 task 已被调度器分配到某个 worker 进程，但 worker 当前正忙，task 在 worker 本地队列中排队。

| 类型 | SUBMITTED_TO_WORKER 含义 | 持续时间 |
|------|--------------------------|---------|
| **Actor task** | task 已发到 Actor 的 worker，排队等 Actor 串行执行 | 可能较长 |
| **普通 task** | Raylet 已选中 worker，task 做执行前准备（反序列化等） | 通常毫秒级 |

普通 task 的 worker 是**独占**的 —— Raylet 为 task 分配空闲 worker，拿到后立即执行，没有排队概念。

Actor task 默认串行执行（`max_concurrency=1`），Ray Data 使用 prefetch 机制故意提前发送多个 task：

```python
# python/ray/data/_internal/execution/operators/actor_pool_map_operator.py:1190-1198
def schedulable_actors(self) -> List[ray.actor.ActorHandle]:
    return [
        actor
        for actor, state in available_actors.items()
        if state.num_tasks_in_flight < self.max_tasks_in_flight_per_actor()
        and not state.is_restarting
    ]
```

每个 Actor 有 1~2 个 task 在 `SUBMITTED_TO_WORKER` 是正常的 prefetch 行为。

### 1.5 心跳与健康检查机制

#### 心跳流程

```
Raylet (Worker 节点)                    GCS (Head 节点)
       │                                      │
       │──── 心跳 RPC ────────────────────────→│ server.poll* 接收
       │                                      │      │
       │                                      │      ↓
       │                                      │ 主线程处理
       │                                      │ GcsHealthCheckManager
       │←──── 响应 ───────────────────────────│
       │                                      │
```

#### 节点死亡判定流程

```
GcsHealthCheckManager 定时检查
       │
       ↓
向 Raylet 发送健康检查请求
       │
       ├── 响应正常 → 重置计数器 (health_check_remaining_ = failure_threshold)
       │
       └── 响应失败 → health_check_remaining_--
              │
              ↓
       health_check_remaining_ == 0 ?
              │
              ├── 否 → 等待 period_ms 后再次检查
              │
              └── 是 → FailNode() → GcsNodeManager.OnNodeFailure()
                         │
                         ↓
                   标记节点 DEAD
                   销毁该节点上的 Actor
                   重新调度 Placement Group
                   广播节点死亡事件
```

#### 两种心跳相关日志的区别

| 日志 | 含义 | 场景 |
|------|------|------|
| `lagging heartbeats` | 心跳延迟，但节点可能还活着 | 网络慢或负载高 |
| `Connection refused` | 节点完全无响应 | Raylet 进程已死（OOM等） |
| `health check failed` | 健康检查多次失败后判死 | 最终结果 |

### 1.6 Actor 调度机制

#### Actor 创建流程

```
┌─────────────────────────────────────────────────────────────────────┐
│  Schedule(actor)                                                     │
│    └─ SelectForwardingNode() → 选择 owner 节点                       │
│       └─ LeaseWorkerFromNode(actor, owner_node)                     │
│          打印: "Leasing worker for actor ... node_id=owner..."      │
└─────────────────────────────────────────────────────────────────────┘
                                 ↓
┌─────────────────────────────────────────────────────────────────────┐
│  Owner 节点处理 lease 请求                                           │
│    - 选择一个 spillback 节点，返回其地址                             │
│    - reply.rejected = false                                         │
│    - reply.worker_address = 空                                      │
│    - reply.retry_at_raylet_address = spillback 节点地址             │
└─────────────────────────────────────────────────────────────────────┘
                                 ↓
┌─────────────────────────────────────────────────────────────────────┐
│  HandleWorkerLeaseGrantedReply()                                     │
│    - worker_address.empty() == true                                 │
│    - 继续去 spillback 节点 lease（GrantOrReject=true）              │
└─────────────────────────────────────────────────────────────────────┘
                                 ↓
┌─────────────────────────────────────────────────────────────────────┐
│  Spillback 节点处理 lease 请求                                       │
│    - GrantOrReject=true，必须明确回复                               │
│    - 资源够 → reply.rejected=false, worker_address=有效地址         │
│    - 资源不够 → reply.rejected = true → Reschedule                 │
└─────────────────────────────────────────────────────────────────────┘
                                 ↓
                         资源够的情况:
                    CreateActorOnWorker → "Actor created successfully"
```

#### 关键日志含义

| 日志 | 含义 | 是否成功 |
|------|------|---------|
| `Leasing worker for actor ...` | 开始对某节点发起 lease | 开始 |
| `Finished leasing worker from ...` | lease RPC 完成，**但不一定拿到 worker** | 中间状态 |
| `Failed to lease ... resources are not enough` | 该节点资源不足，reject | 失败 |
| `Submitting actor creation task to worker` | **真正拿到 worker**，开始创建 | 即将成功 |
| `Actor creation task succeeded` | Actor 创建成功 | 成功 |

**"Finished leasing" 后还会继续 lease 的原因**：

```cpp
// src/ray/gcs/actor/gcs_actor_scheduler.cc:296-325
void GcsActorScheduler::HandleWorkerLeaseGrantedReply(...) {
  const auto &worker_address = reply.worker_address();
  if (worker_address.node_id().empty()) {
    // worker_address 是空的！虽然打印了 "Finished leasing"，
    // 但实际只拿到了 spillback 节点地址，继续去 spillback 节点 lease
    LeaseWorkerFromNode(actor, spill_back_node);
  } else {
    // worker_address 不为空，真正拿到了 worker
    CreateActorOnWorker(actor, leased_worker);
  }
}
```

---

## 二、案例一：FD 耗尽导致调度阻塞（2246 节点）

> 集群规模：2246 节点 | Head 节点内存：1TB | Ray 版本：2.52.1
> 问题日期：2026-05-16

### 2.1 问题现象

1. Ray Data 作业中大量 task 处于 **"Waiting for scheduling"** 状态，调度极慢
2. `ray status` 命令超时，无法连接 GCS：
   ```
   Failed to connect to GCS at address 10.15.3.158:6379 within 5 seconds.
   Timed out while waiting for GCS to become available.
   ```
3. `ray job stop` 同样超时失败
4. GCS 进程 CPU 占用异常高（248%），大量时间花在重试 socket 创建上

### 2.2 排查思路

```
Task 调度慢
  → ray status 超时，GCS 无响应
    → 检查 GCS 进程是否存活
      → 存活但 CPU 异常高
        → 查看 GCS 错误日志
          → 发现 FD 耗尽
            → 分析 FD 被什么占用
              → 99.95% 是 socket
                → 分析 socket 连向哪些 IP
                  → 2246 个节点 × 29 连接 = 65134 ≈ 65536 limit
                    → 确认是节点规模导致的 socket 连接数爆炸
```

### 2.3 排查过程（完整交互记录）

#### 第一步：尝试 ray status（发现 GCS 不可用）

```bash
$ ray status
[2026-05-16 14:11:34,979 W 103990 103990] rpc_client.h:153: Failed to connect to GCS at address 10.15.3.158:6379 within 5 seconds.
[2026-05-16 14:12:04,980 W 103990 103990] gcs_client.cc:205: Failed to get cluster ID from GCS server: TimedOut: Timed out while waiting for GCS to become available.
```

**分析**：所有 Ray CLI 命令都需要通过 gRPC 连接 GCS（端口 6379），连接失败说明 GCS 要么挂了，要么无法接受新连接。

#### 第二步：检查环境变量和集群信息

```bash
$ echo RAY_ADDRESS=$RAY_ADDRESS
RAY_ADDRESS=                    # 未设置，使用默认自动发现

$ cat /tmp/ray/ray_current_cluster
10.15.3.158:6379                # GCS 地址确认

$ ls /tmp/ray/session_latest/   # session 目录存在，集群在运行
logs  metrics  sockets  ...
```

#### 第三步：确认进程状态

```bash
$ ps aux | grep -E '(raylet|gcs_server|ray::)' | grep -v grep
root    1   0.0  ... ray start --head --port=6379 --num-cpus=0 --num-gpus=0 --system-config={...}
root   74  248   ... gcs_server --gcs_server_port=6379 ...    ← CPU 248%！异常高
root  992  11.6  ... raylet --node_ip_address=10.15.3.158 ... ← Head 节点 Raylet
root 1042  28.1  ... ray::DashboardAgent
root 1044   0.0  ... ray::RuntimeEnvAgent
root 3012   0.4  ... ray::_StatsActor
root 3109   0.3  ... ray::ActorLocationTracker
root 99148  0.4  ... ray::JobSupervisor
```

**关键发现**：
- GCS Server（PID 74）**CPU 占用 248%**，极其异常
- Head 节点配置了 `--num-cpus=0 --num-gpus=0`（不承担计算任务）
- Raylet 配置了 `memory=352GB, object_store_memory=150GB`

#### 第四步：检查端口监听状态

```bash
$ netstat -tlnp | grep -E '(6379|8265)'
tcp   0     0 0.0.0.0:8265   0.0.0.0:*  LISTEN  272/python     ← Dashboard
tcp6  129   0 :::6379        :::*       LISTEN  74/gcs_server  ← GCS RPC 端口
```

**关键发现**：GCS 端口 6379 的 **`Recv-Q = 129`**，说明有 129 个连接请求积压在内核队列中无法被 `accept()`。正常情况 `Recv-Q` 应该为 0。

#### 第五步：查看 GCS 错误日志（定位根因）

```bash
$ tail -100 /tmp/ray/session_2026-05-16_00-44-22_781709_1/logs/gcs_server.err
```

**结果（每秒都在刷）**：
```
E0516 14:14:28.975400943  148 socket_utils_common_posix.cc:477
  socket(10, 1, 0) returned -1 with error: |Too many open files|.
  This process might not have a sufficient file descriptor limit
  for the number of connections grpc wants to open (which is
  generally a function of the number of grpc channels, the lb policy
  of each channel, and the number of backends each channel is load
  balancing across).

E0516 14:14:29.363658164  1383 tcp_server_posix.cc:378
  File descriptor limit reached. Retrying.
```

**确认根因：GCS 进程的文件描述符（FD）耗尽！**

统计错误日志：
```bash
$ grep 'File descriptor' gcs_server.err | head -3
E0516 01:46:21.063  File descriptor limit reached. Retrying.   ← 首次出现
E0516 01:54:54.242  File descriptor limit reached. Retrying.
E0516 01:58:29.243  File descriptor limit reached. Retrying.

$ grep 'File descriptor' gcs_server.err | wc -l
1454                                                            ← 共 1454 次
```

从凌晨 01:46 就开始报错了。

#### 第六步：确认 FD 使用情况

```bash
# GCS 进程的 FD 上限（进程级别）
$ cat /proc/74/limits | grep 'open files'
Max open files            65536                65536                files

# GCS 实际已用 FD 数量
$ ls /proc/74/fd | wc -l
65536                            ← 100% 打满！

# 容器级别的 FD 上限（当前 shell 继承的值）
$ ulimit -n
1048576                          ← 容器允许 100 万
```

**关键发现**：容器的 FD limit 是 1048576，但 GCS 进程只有 65536。原因是用户启动脚本中显式设置了 `ulimit -n 65536`，GCS 作为子进程继承了这个值。

#### 第七步：分析 FD 被什么类型占用

```bash
$ ls /proc/74/fd | xargs -I{} readlink /proc/74/fd/{} | sed 's/:.*//' | sort | uniq -c | sort -rn
65504 socket          ← 99.95%！
   19 anon_inode      ← epoll fd
    3 /dev/null
    2 pipe
    2 gcs_server.out
    2 gcs_server.err
    1 event_EXPORT_NODE.log
    1 event_EXPORT_DRIVER_JOB.log
    1 event_EXPORT_ACTOR.log
    1 event_GCS.log
```

**几乎所有 FD 都是 socket（网络连接）**。

#### 第八步：分析 socket 连接来源

```bash
# GCS 进程总 TCP 连接数
$ ss -tnp | grep ',pid=74,' | wc -l
65210

# 连接到多少个不同的 IP（节点）
$ ss -tn6p | grep ',pid=74,' | awk '{print $5}' | rev | cut -d: -f2- | rev | sort -u | wc -l
2246

# 每节点平均连接数
$ ss -tn6p | grep ',pid=74,' | awk '{print $5}' | rev | cut -d: -f2- | rev | sort | uniq -c | sort -rn | awk '{sum+=$1; count++} END{print "total_conns="sum, "unique_ips="count, "avg_per_ip="sum/count}'
total_conns=65210 unique_ips=2246 avg_per_ip=29.03
```

**关键数据**：
- GCS 与 **2246 个不同节点** 建立了连接
- 每个节点平均 **~29 条 gRPC 连接**
- 总计 65,210 条 ≈ 65,536 FD limit

#### 第九步：分析每节点连接数分布

```bash
$ ss -tn6p | grep ',pid=74,' | awk '{print $5}' | rev | cut -d: -f2- | rev | sort | uniq -c | sort -rn | head -5
173 [::ffff:10.83.10.37]
172 [::ffff:10.83.10.23]
172 [::ffff:10.82.234.24]
171 [::ffff:10.82.238.167]
171 [::ffff:10.82.236.35]
```

大部分节点与 GCS 维持 **15~31 条 gRPC 连接**。

#### 第十步：检查其他系统指标

```bash
$ free -g
              total   used   free   shared  buff/cache  available
Mem:          1006     52     238    1       716         950

$ cat /proc/74/status | grep -E '(VmRSS|VmSize|Threads)'
VmSize:  61791500 kB     ← 虚拟内存 ~59GB
VmRSS:   12242280 kB     ← 物理内存 ~12GB
Threads: 540             ← 540 个线程

$ ss -s
Total: 91,312
TCP:   91,232 (estab 90,445, closed 602, orphaned 0, timewait 302)
```

**关键发现**：
- Head 节点内存充足（总 1TB，已用 52GB）
- GCS 进程本身 12GB 内存、540 线程
- 整个 head 节点 **90,445 条 ESTABLISHED TCP 连接**

#### 第十一步：检查 Raylet 日志

```bash
$ tail -30 /tmp/ray/session_*/logs/raylet.out
[2026-05-16 14:15:27,199 W 992] (raylet) scheduling_class_util.cc:184:
  More than 13995 types of tasks seen, this may reduce performance.
[2026-05-16 14:16:57,813 W 992] (raylet) scheduling_class_util.cc:184:
  More than 14031 types of tasks seen, this may reduce performance.
```

**发现第二个问题**：14000+ 种 scheduling class（task 类型），会增加调度开销。

### 2.4 根因分析

#### FD 耗尽的直接原因：节点规模过大

```
2,246 节点 × 每节点 ~29 条 gRPC 连接 = ~65,134 条 ≈ 65,536 FD limit
```

**是节点数量导致的，不是 task 数量直接导致的。**

#### 每节点 ~29 条连接的构成

| 组件 | 连接数/节点 | 用途 |
|------|------------|------|
| Raylet → GCS | 3~5 | 资源上报（RaySyncer）、心跳、lease 请求、节点注册 |
| 每个 Worker 进程 → GCS | 2~3 | CoreWorker 注册、task event 上报、对象引用追踪 |
| Dashboard Agent → GCS | 1~2 | 指标采集 |
| Runtime Env Agent → GCS | 1~2 | 运行环境管理 |

如果每节点有 ~8 个 worker 进程：`5 + 8×3 = ~29`，**完全吻合实测数据**。

**CoreWorker 连接 GCS 的代码路径**：

```python
# python/ray/_private/worker.py:2604-2609
gcs_options = ray._raylet.GcsClientOptions.create(
    node.gcs_address,         # → "10.15.3.158:6379"
    node.cluster_id.hex(),
    allow_cluster_id_nil=False,
    fetch_cluster_id_if_nil=False,
)
# python/ray/_raylet.pyx:2806
CCoreWorkerProcess.Initialize(options)  # ← 建立到 GCS 的 gRPC channel
```

#### task 数量的间接影响

scheduling class 告警的相关代码：

```cpp
// src/ray/common/scheduling/scheduling_class_util.cc:175-194
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
  }
  return sched_cls_id;
}
```

每个 `SchedulingClassDescriptor` 是以下属性的唯一组合：
- `ResourceSet`（CPU、GPU、memory 需求）
- `FunctionDescriptor`（函数签名）
- `SchedulingStrategy`（调度策略）
- `LabelSelector`（标签选择器）

14000+ 种 scheduling class 会增加遍历开销，但**不是 FD 耗尽的直接原因**。

### 2.5 FD 耗尽的完整影响链

当 GCS 进程的 `ulimit -n` 达到上限后，内核拒绝 `socket()` 和 `accept()` 系统调用（返回 `EMFILE`）。

#### 影响总览（级联故障图）

```
GCS FD 耗尽 (socket()/accept() 返回 EMFILE)
    │
    ├─→ [影响1] gRPC Server 无法 accept 新 TCP 连接
    │       ├─→ ray status / ray job stop 等 CLI 超时
    │       ├─→ 新 Worker 注册失败
    │       └─→ RequestWorkerLease RPC 超时 → task 卡在 PENDING_NODE_ASSIGNMENT
    │
    ├─→ [影响2] RaySyncer 双向流断开，资源广播停止
    │       ├─→ Raylet 持有过期的集群资源视图
    │       ├─→ 调度器找不到可用节点（GetBestSchedulableNode 返回 Nil）
    │       └─→ 每 2 秒重连一次，持续失败
    │
    ├─→ [影响3] 健康检查出向 RPC 失败
    │       ├─→ health_check_remaining_ 递减到 0
    │       ├─→ 健康节点被误判为死亡（FailNode）
    │       └─→ 该节点上的 Actor/Task 被强制 kill
    │
    ├─→ [影响4] GCS 资源负载拉取失败
    │       └─→ Autoscaler 无法获取最新负载 → 无法正确扩缩容
    │
    └─→ [影响5] Ray Data StreamingExecutor 停滞
            ├─→ 提交的 task 永远无法被调度
            ├─→ ray.wait() 持续返回空 → 无前进进度
            └─→ Pipeline 完全卡住
```

#### 影响1：gRPC Server 无法接受新连接

gRPC 底层检测到 `accept()` 失败后每秒重试：
```
File descriptor limit reached. Retrying.
```

同时出向连接创建也报错：
```
socket(10, 1, 0) returned -1 with error: |Too many open files|.
```

**直接后果**：`ray status`、`ray job stop` 等 CLI 超时；新 Worker 无法注册到 GCS。

#### 影响2：RaySyncer 资源广播中断

RaySyncer 断开后重连逻辑：

```cpp
// src/ray/ray_syncer/ray_syncer.cc:103-111
if (restart) {
  execute_after(
      io_context_,
      [this, remote_node_id, channel]() {
        RAY_LOG(INFO) << "Connection to the node was broken, reconnecting.";
        Connect(remote_node_id, channel);  // ← 2秒后重连，但 FD 满仍失败
      },
      std::chrono::milliseconds(2000));
}
```

资源广播变成空操作：

```cpp
// src/ray/ray_syncer/ray_syncer.cc:209-224
void RaySyncer::BroadcastMessage(std::shared_ptr<const RaySyncMessage> message) {
  io_context_.dispatch([this, message] {
    if (!node_state_->ConsumeSyncMessage(message)) { return; }
    for (auto &reactor : sync_reactors_) {
      reactor.second->PushToSendingQueue(message);
      // ← 当 sync_reactors_ 为空时，这个循环体不执行
      //   资源更新不再传递到任何 Raylet
    }
  }, "RaySyncer.BroadcastMessage");
}
```

**直接后果**：Raylet 收不到资源释放通知，认为远端节点资源已满，调度器做出错误决策。

#### 影响3：健康检查失败 → 节点误判死亡

GCS 的出向健康检查 RPC 也需要 socket FD：

```cpp
// src/ray/gcs/gcs_health_check_manager.cc:168-214
stub_->async()->Check(context_ptr, &request_, response_ptr,
    [this, ...](::grpc::Status status) {
      if (status.ok() && response->status() == HealthCheckResponse::SERVING) {
        health_check_remaining_ = mgr->failure_threshold_;  // 重置计数器
      } else {
        --health_check_remaining_;  // FD 满导致 gRPC 无法建连，status 不 ok
      }
      if (health_check_remaining_ == 0) {
        mgr->FailNode(node_id_);  // 判定节点死亡
        delete this;
      }
    });
```

> **注意**：已建立的 TCP 连接不需要新的 FD，可以继续收发数据。FD 耗尽只阻止**新连接**的创建。因此如果健康检查使用的连接已经建立，它们可能仍然正常工作。本案例中主要表现为调度阻塞而非大规模节点误判。

#### 影响4：Raylet 调度决策失败

```cpp
// src/ray/raylet/node_manager.cc:2982-3001
void NodeManager::ConsumeSyncMessage(...) {
  if (message->message_type() == syncer::MessageType::RESOURCE_VIEW) {
    const bool capacity_updated = ResourceCreateUpdated(node_id, resources);
    const bool usage_update = UpdateResourceUsage(node_id, resource_view_sync_message);
    if (capacity_updated || usage_update) {
      cluster_lease_manager_.ScheduleAndGrantLeases();
      // ← 资源变化时触发重调度
    }
  }
}
```

RaySyncer 已断开 → `ConsumeSyncMessage` 不再被调用 → **调度循环永远无法推进** → 调度器"冻结"。

#### 影响5：Ray Data StreamingExecutor 停滞

```python
# python/ray/data/_internal/execution/streaming_executor_state.py:482-545
def process_completed_tasks(topology, ...):
    if active_tasks:
        ready, _ = ray.wait(
            list(active_tasks.keys()),
            num_returns=len(active_tasks),
            fetch_local=False,
            timeout=0.1,  # ← 100ms 超时
        )
    # ready 为空 → 没有 task 完成 → 下游算子饥饿 → pipeline 卡死
```

#### 影响对比总结

| 影响 | 严重程度 | 用户可见现象 | 代码位置 |
|------|----------|-------------|---------|
| gRPC 无法 accept | 中 | CLI 超时 | `grpc_server.cc:192` |
| 资源广播中断 | **致命** | task 卡在 Waiting for scheduling | `ray_syncer.cc:209` |
| 节点误判死亡 | 高 | Actor 被 kill、task 失败 | `gcs_health_check_manager.cc:168` |
| 调度器冻结 | **致命** | 新 task 永远无法被调度 | `cluster_lease_manager.cc:196` |
| 资源拉取失败 | 中 | Autoscaler 无法正确决策 | `gcs_server.cc:404` |
| Ray Data 停滞 | 高 | Pipeline 无进度 | `streaming_executor.py:569` |

### 2.6 端口与网络影响

#### GCS 端口 6379 的影响

| 影响 | 表现 | 后果 |
|------|------|------|
| **无法 accept 新连接** | `Recv-Q` 积压（实测 129）| `ray status`、新 worker 注册失败 |
| **无法创建出向连接** | GCS 主动推送失败 | Raylet 收不到最新资源视图 |
| **现有连接不受影响** | 已建立的 65504 条连接仍可通信 | 部分节点仍能工作 |

#### 受影响的关键端口

| 端口 | 服务 | FD 耗尽后的影响 |
|------|------|----------------|
| 6379 (GCS RPC) | 集群元数据、调度协调、PubSub | 新调度请求阻塞 |
| 8265 (Dashboard) | Ray Dashboard Web UI、Job API | Dashboard 数据不更新 |
| Raylet Node Manager Port | 节点间 Lease 请求和 Spillback | Spillback 失败 |
| Object Manager Port | 对象传输 | 不直接受影响，但间接因调度失败而闲置 |

### 2.7 容器级 vs 进程级 ulimit

#### 如何观察

| 级别 | 命令 | 本次实测值 | 含义 |
|------|------|-----------|------|
| 容器级（新 shell 默认值） | `ulimit -n` | 1048576 | 容器内新进程的默认 FD limit |
| 进程级（GCS 实际值） | `cat /proc/74/limits \| grep 'open files'` | 65536 | GCS 进程实际能打开的最大 FD 数 |
| 系统级 | `cat /proc/sys/fs/file-nr` | 99328 / 105500894 | 当前系统已打开 FD / 系统允许最大值 |

#### 为什么 GCS 只有 65536

容器默认 `ulimit -n` 是 1048576，但用户在启动脚本中**显式设置了** `ulimit -n 65536`：

```bash
ulimit -n 65536; RAY_METRICS_EXPO...   # ← 启动 ray 之前降低了 FD limit
```

`ray start --head` 作为子进程继承了这个值。

#### ulimit -n 的影响范围

| 进程 | 是否受影响 | 当前 FD 用量 |
|------|-----------|-------------|
| GCS Server (PID 74) | 是 | 65,536 / 65,536（满了） |
| Head 节点 Raylet (PID 992) | 是 | 9,468 / 65,536（正常） |
| Dashboard Agent | 是 | FD 需求不大 |
| Worker 节点进程 | 否 | Worker 节点有自己的启动脚本和 ulimit |

### 2.8 解决方案

#### 立即修复：调整 FD limit

**原始启动脚本**：
```bash
ulimit -n 65536; RAY_METRICS_EXPO...
```

**修改为**：
```bash
ulimit -n 1048576; RAY_METRICS_EXPO...
```

修改后**重启 Ray 集群**生效。

#### 为什么设置为 1048576

| 考量 | 说明 |
|------|------|
| 容器 hard limit 已允许 | 1048576 不需要额外权限 |
| 内存开销几乎为零 | `ulimit -n` 只是上限，不预分配。每个 FD 内核开销 ~1KB |
| 安全性 | 只影响 head 节点上由该 shell 启动的进程 |
| 一劳永逸 | 避免集群扩缩容后再次打满 |

#### 长期优化建议

| 措施 | 优先级 | 说明 |
|------|--------|------|
| 提高 GCS FD limit | P0 | 改为 1048576，重启即可 |
| 监控 FD 使用率 | P1 | 添加 `ls /proc/$(pgrep gcs_server)/fd \| wc -l` 到监控 |
| 减少 scheduling class 数量 | P2 | 检查 Ray Data pipeline 是否给不同 task 设置了不同的 resources |
| 评估集群拆分 | P3 | 2246 节点是超大集群，考虑拆成多个小集群 |

#### 其他排查中遇到的报错

**`ray job stop` 超时**：与 FD 耗尽完全是同一个问题。GCS 无法 accept 新连接。

**`Batch timer error: Success`**：
```
[2026-05-16 00:44:30,353 E 74 99] (gcs_server) ray_syncer_bidi_reactor_base.h:112: Batch timer error: Success
```
与 FD 耗尽**无关**，是 RaySyncer 的已知无害告警（boost::asio timer cancel 竞争条件误报），只在启动瞬间出现一次。

---

## 三、案例二：GCS CPU 高与 Syncer 瓶颈（800+ 节点）

> 集群规模：800+ 节点
> 问题日期：2026-04-30 ~ 2026-05-07

### 3.1 问题现象

1. Actor 启动后 dead，日志显示：`"The actor is dead because all references to the actor were removed including lineage ref count"`
2. 节点被误判死亡：`"health check failed due to missing too many heartbeats"`
3. Actor 调度缓慢，大量 lease 重试
4. 短时间内多个节点连续失败

### 3.2 GCS 负载分析

#### GCS 进程资源占用

```bash
$ top -p $(pgrep -f gcs_server)
PID USER  PR  NI    VIRT    RES    SHR S  %CPU  %MEM     TIME+ COMMAND
 26 root  20   0  163.9g  16.6g  18776 R 750.2   3.3 521:49.94 gcs_server
```

| 指标 | 值 | 说明 |
|------|-----|------|
| GCS CPU | **750%** | 使用了 ~7.5 个核心 |
| GCS 内存 | **16.6 GB** | 对于大集群偏高 |
| 系统空闲 | 89.1% | 机器本身不忙，但 GCS 进程很忙 |
| Head 节点核数 | 79 | 配置了 64 线程但只用了 7.5 核 |

#### GCS 线程分析

```bash
$ ps -T -p $(pgrep -f gcs_server) -o tid,comm,%cpu | sort -k3 -rn | head -10
TID   COMMAND         %CPU
103   ray_syncer_io_c 89.2   ← 状态同步线程，单线程瓶颈！
78    gcs_server      20.5   ← 主线程
101   task_io_context 10.6   ← 任务 IO
102   pubsub_io_conte 10.4   ← 发布订阅
215   server.poll27    3.0   ← gRPC 线程
171   nexting_thread   2.7   ← gRPC 相关
...   server.poll*     2-3%  ← gRPC 服务器线程
```

**关键发现**：
1. `ray_syncer_io_c` **单线程打满 89.2%**，是最大瓶颈
2. gRPC 线程（`server.poll*`）负载很低，每个只有 2-3%
3. 心跳处理能力是足够的，瓶颈在 syncer

### 3.3 Syncer 瓶颈与级联影响

#### Syncer 与心跳的关系

**直接影响：不会**（不同线程/io_context）

```
ray_syncer_io_c (89%)     心跳处理 (GcsHealthCheckManager)
        │                          │
        ↓                          ↓
  独立线程/io_context        主线程/默认 io_context
        │                          │
        └────── 不同线程 ───────────┘
```

**间接影响路径**：

```
ray_syncer_io_c 打满 (89%)
        ↓
资源视图同步延迟（秒级）
        ↓
GCS 基于过时的资源信息调度
        ↓
Owner 节点选错 spillback 候选
        ↓
实际 lease 时发现资源不足
        ↓
反复失败，消耗大量时间和 RPC
        ↓
主线程处理大量调度请求
        ↓
间接影响健康检查回调处理
```

### 3.4 Actor 调度问题分析

#### 调度问题日志实例

```
19:19:16,132 ─ 注册 Actor b333205b...
     │
     ├─ 19:19:16 ~ 19:19:27 (约 11 秒)
     │    反复尝试 lease worker:
     │    - 500d8a37... → Finished (但没真正分配)
     │    - 其他节点 → Failed: resources not enough
     │    - 循环 20+ 次...
     │
19:19:27,900 ─ 尝试节点 e47e0ecc...
     │
19:19:29,122 ─ 从 e47e0ecc... 成功 lease worker
     │         └─ Submitting actor creation task
     │
19:19:41,754 ─ Actor 创建成功 ✓
     │         (从注册到成功: 25 秒)
```

一个 Actor 创建耗时 25 秒，其中 11 秒花在反复 lease 失败重试上。根因是 Syncer 延迟导致资源视图不准确。

### 3.5 节点死亡案例分析

#### 完整时间线

```
19:19:41 ─ Actor 创建成功在节点 10.48.35.79

     ══════ 正常运行 16 分钟 ══════

19:35:59 ─ ⚠️ 开始异常：无法检查 worker 状态
           "Failed to check if worker is dead"

19:36:08 ─ ❌ 第一次健康检查失败
           "Connection refused" to 10.48.35.79:42541
           remaining checks: 9

19:36:09 ─ ray_syncer 连接断开
           "Connection is broken"

19:36:08 ~ 19:37:42 ─ 健康检查持续失败（每 10 秒一次）
           remaining: 9 → 8 → 7 → 6 → 5 → 4 → 3 → 2 → 1 → 0

19:37:42 ─ 💀 节点被标记为死亡
           - death reason = UNEXPECTED_TERMINATION
           - death message = health check failed due to missing too many heartbeats
           - 重建 7 个 Actor
           - 重新调度 Placement Groups
```

#### 节点死亡原因判定

| 原因 | 日志特征 | 验证方式 |
|------|----------|----------|
| **Raylet OOM/崩溃** | `Connection refused` | 查看节点 `dmesg` |
| **心跳延迟** | 只有 `health check failed` | 检查网络和负载 |
| **K8s 驱逐** | K8s events 显示 Evicted | `kubectl describe pod` |
| **抢占** | `preempted = 1` | 检查日志 |

### 3.6 健康检查 Connection refused 深度分析

#### 问题日志

```
Health check failed for node 28e5744f...,
remaining checks 1,
status 14,
response status 0,
status message failed to connect to all addresses;
last error: UNKNOWN: ipv4:10.48.32.149:41029: Failed to connect to remote host: Connection refused
```

#### 日志字段解析

| 字段 | 值 | 含义 |
|------|-----|------|
| `remaining checks 1` | 1 | 还剩 1 次机会，下次失败就判死 |
| `status 14` | gRPC UNAVAILABLE | 服务不可达 |
| `status message` | failed to connect | gRPC 无法连接 |
| `last error` | Connection refused | TCP RST，目标端口无进程监听 |

**关键判断**：`Connection refused` = 网络通 + Raylet 进程已不在（如果网络不通会是 `Connection timed out`）。

#### 不同场景对比

| 场景 | status | 错误信息 | Raylet 进程 | 根因 |
|------|--------|---------|------------|------|
| Raylet 崩溃 (OOM) | 14 | Connection refused | 不在 | OOM Kill |
| Raylet 主线程阻塞 | 4 | Deadline Exceeded | 在 | 主线程阻塞/死锁 |
| 网络分区 | 14 | Connection timed out | 在 | 网络不通 |
| 容器重启 | 14 | Connection refused | 不在 | K8s 驱逐 |

#### 排查步骤

```bash
# 1. 确认 Raylet 进程是否还在
ssh <node_ip> "ps aux | grep raylet"

# 2. 检查 OOM
ssh <node_ip> "dmesg | grep -i 'oom\|killed' | tail -5"

# 3. 检查 Raylet 日志
ssh <node_ip> "tail -50 /tmp/ray/session_latest/logs/raylet.out"

# 4. 区分：
#   有 "marked as dead" → Raylet 因健康检查超时主动退出
#   无 "marked as dead" → Raylet 被系统杀死（OOM），来不及输出日志
```

---

## 四、GCS 相关源码深度分析

### 4.1 io_context 分配策略

```cpp
// src/ray/gcs/gcs_server_io_context_policy.h
struct GcsServerIOContextPolicy {
  template <typename T>
  static constexpr int GetDedicatedIOContextIndex() {
    if constexpr (std::is_same_v<T, GcsTaskManager>) {
      return IndexOf("task_io_context");           // 索引 0
    } else if constexpr (std::is_same_v<T, pubsub::GcsPublisher>) {
      return IndexOf("pubsub_io_context");         // 索引 1
    } else if constexpr (std::is_same_v<T, syncer::RaySyncer>) {
      return IndexOf("ray_syncer_io_context");     // 索引 2
    } else if constexpr (std::is_same_v<T, observability::RayEventRecorder>) {
      return IndexOf("ray_event_io_context");      // 索引 3
    } else {
      return -1;  // ← 返回 -1 表示使用默认 io_context（主线程）
    }
  }
};
```

**所有核心组件（HealthCheck、ActorScheduler、NodeManager）都在主线程上运行**。

### 4.2 RaySyncer 连接与广播逻辑

#### 客户端连接（Raylet → GCS）

```cpp
// src/ray/ray_syncer/ray_syncer.cc:81-124
void RaySyncer::Connect(const std::string &node_id,
                        std::shared_ptr<grpc::Channel> channel) {
  auto stub = ray::rpc::syncer::RaySyncer::NewStub(channel);
  auto reactor = std::make_shared<RayClientBidiReactor>(
      node_id, GetLocalNodeID(), io_context_,
      [this](auto msg) { BroadcastMessage(std::move(msg)); },  // 消息处理
      [this, channel](RaySyncerBidiReactor *bidi_reactor, bool restart) {
        // 清理回调
        sync_reactors_.erase(iter);
        if (restart) {
          execute_after(io_context_,
            [this, remote_node_id, channel]() {
              Connect(remote_node_id, channel);  // ← 2秒后重连
            }, std::chrono::milliseconds(2000));
        } else {
          node_state_->RemoveNode(remote_node_id);
        }
      },
      std::move(stub), max_batch_size_, max_batch_delay_ms_);
  reactor->StartCall();  // ← 发起 BiDi 流，需要 socket FD
}
```

#### 广播消息

```cpp
// src/ray/ray_syncer/ray_syncer.cc:209-224
void RaySyncer::BroadcastMessage(std::shared_ptr<const RaySyncMessage> message) {
  io_context_.dispatch([this, message] {
    if (!node_state_->ConsumeSyncMessage(message)) { return; }
    for (auto &reactor : sync_reactors_) {
      reactor.second->PushToSendingQueue(message);
    }
  }, "RaySyncer.BroadcastMessage");
}
```

当所有 reactor 断开后，`sync_reactors_` 为空，广播变成空操作。

#### 服务端接收（GCS 侧）

```cpp
// src/ray/ray_syncer/ray_syncer.cc:226-276
ServerBidiReactor *RaySyncerService::StartSync(grpc::CallbackServerContext *context) {
  auto reactor = std::make_shared<RayServerBidiReactor>(...,
      [this](RaySyncerBidiReactor *bidi_reactor, bool reconnect) {
        RAY_CHECK(!reconnect);  // 服务端不重连
        syncer_.sync_reactors_.erase(iter);
        RAY_LOG(INFO) << "Connection is broken.";
        syncer_.node_state_->RemoveNode(node_id);
      }, ...);
  syncer_.Disconnect(reactor->GetRemoteNodeID());  // 断开旧连接
  syncer_.Connect(reactor);  // 注册新连接
  return reactor.get();
}
```

### 4.3 健康检查核心逻辑

```cpp
// src/ray/gcs/gcs_health_check_manager.cc:122-227
void GcsHealthCheckManager::HealthCheckContext::StartHealthCheck() {
  using ::grpc::health::v1::HealthCheckResponse;

  auto manager = manager_.lock();
  if (manager == nullptr) { delete this; return; }
  RAY_CHECK(manager->thread_checker_.IsOnSameThread());

  if (stopped_) { delete this; return; }

  // 检查最新健康状态时间戳
  const auto now = absl::Now();
  absl::Time next_check_time =
      latest_known_healthy_timestamp_ + absl::Milliseconds(manager->period_ms_);
  if (now <= next_check_time) {
    // 信息足够新鲜，跳过本次检查
    timer_.expires_from_now(...);
    timer_.async_wait([this](auto) { StartHealthCheck(); });
    return;
  }

  // 发起异步健康检查
  auto context = std::make_shared<grpc::ClientContext>();
  auto response = std::make_shared<HealthCheckResponse>();
  context->set_deadline(absl::ToChronoTime(now + absl::Milliseconds(manager->timeout_ms_)));

  stub_->async()->Check(context_ptr, &request_, response_ptr,
      [this, ...](::grpc::Status status) {
        // 回调在 gRPC 线程池中执行，post 回主线程处理
        gcs_health_check_manager->io_service_.post([this, status, response]() {
          if (status.ok() && response->status() == HealthCheckResponse::SERVING) {
            health_check_remaining_ = mgr->failure_threshold_;  // 通过，重置
          } else {
            --health_check_remaining_;  // 失败，递减
            RAY_LOG(WARNING) << "Health check failed for node " << node_id_
                << ", remaining checks " << health_check_remaining_ << ...;
          }
          if (health_check_remaining_ == 0) {
            mgr->FailNode(node_id_);  // 判定节点死亡
            delete this;
          } else {
            timer_.expires_from_now(boost::posix_time::milliseconds(mgr->period_ms_));
            timer_.async_wait([this](auto) { StartHealthCheck(); });
          }
        }, "HealthCheck");
      });
}

void GcsHealthCheckManager::FailNode(const NodeID &node_id) {
  RAY_LOG(WARNING) << "Node is dead because the health check failed.";
  auto iter = health_check_contexts_.find(node_id);
  if (iter != health_check_contexts_.end()) {
    on_node_death_callback_(node_id);  // 触发 GcsNodeManager::OnNodeFailure
    health_check_contexts_.erase(iter);
  }
}
```

### 4.4 节点死亡处理流程

#### 死亡原因推断

```cpp
// src/ray/gcs/gcs_node_manager.cc:539-565
rpc::NodeDeathInfo GcsNodeManager::InferDeathInfo(const NodeID &node_id) {
  auto iter = draining_nodes_.find(node_id);
  rpc::NodeDeathInfo death_info;

  if (iter != draining_nodes_.end() &&
      iter->second->deadline_timestamp_ms() != 0 &&
      current_sys_time_ms() > iter->second->deadline_timestamp_ms() &&
      iter->second->reason() == rpc::autoscaler::DRAIN_NODE_REASON_PREEMPTION) {
    // 抢占导致的强制终止
    death_info.set_reason(rpc::NodeDeathInfo::AUTOSCALER_DRAIN_PREEMPTED);
  } else {
    // 默认：意外终止
    death_info.set_reason(rpc::NodeDeathInfo::UNEXPECTED_TERMINATION);
    death_info.set_reason_message(
        "health check failed due to missing too many heartbeats");
  }
  return death_info;
}
```

#### 节点失败完整处理

```cpp
// src/ray/gcs/gcs_node_manager.cc:686-717
void GcsNodeManager::InternalOnNodeFailure(const NodeID &node_id, ...) {
  auto maybe_node = GetAliveNodeFromCache(node_id);
  if (maybe_node.has_value()) {
    // 1. 推断死亡原因
    rpc::NodeDeathInfo death_info = InferDeathInfo(node_id);
    // 2. 从缓存移除节点
    auto node = RemoveNodeFromCache(node_id, death_info, rpc::GcsNodeInfo::DEAD, ...);
    // 3. 添加到死亡节点缓存
    AddDeadNodeToCache(node);
    // 4. 持久化到存储
    gcs_table_storage_->NodeTable().Put(node_id, *node, {
      [this, ...](const Status &status) {
        WriteNodeExportEvent(*node, false);
        // 5. 通过 PubSub 广播节点死亡事件
        PublishNodeInfoToPubsub(node_id, node_info_delta);
      }, io_context_
    });
  }
}
```

节点移除时的广播（这就是用户看到的 `lagging heartbeats` 消息来源）：

```cpp
if (node_death_info.reason() == rpc::NodeDeathInfo::UNEXPECTED_TERMINATION) {
  std::ostringstream error_message;
  error_message << "The node with node id: " << node_id
      << " has been marked dead because the detector"
      << " has missed too many heartbeats from it. This can happen when a "
      << "\t(1) raylet crashes unexpectedly (OOM, etc.) \n"
      << "\t(2) raylet has lagging heartbeats due to slow network or busy workload.";
  RAY_LOG(WARNING) << error_message.str();
  gcs_publisher_->PublishError(node_id.Hex(), std::move(error_data));
}
```

### 4.5 Actor 调度完整流程

#### Schedule 入口

```cpp
// src/ray/gcs/actor/gcs_actor_scheduler.cc:49-81
void GcsActorScheduler::Schedule(std::shared_ptr<GcsActor> actor) {
  RAY_CHECK(actor->GetNodeID().IsNil() && actor->GetWorkerID().IsNil());
  // 1. 选择转发节点（优先 owner 节点）
  auto node_id = SelectForwardingNode(actor);
  auto node = gcs_node_manager_.GetAliveNode(node_id);
  if (!node.has_value()) {
    schedule_failure_handler_(std::move(actor), ...);
    return;
  }
  // 2. 绑定到选中的节点
  actor->UpdateAddress(address);
  node_to_actors_when_leasing_[actor->GetNodeID()].emplace(actor->GetActorID());
  // 3. GrantOrReject=false（owner 可以返回 spillback）
  actor->SetGrantOrReject(false);
  // 4. 开始 lease worker
  LeaseWorkerFromNode(actor, node.value());
}
```

#### 发起 Lease 请求

```cpp
// src/ray/gcs/actor/gcs_actor_scheduler.cc:234-271
void GcsActorScheduler::LeaseWorkerFromNode(...) {
  RAY_LOG(INFO) << "Leasing worker for actor.";  // ← "Leasing worker" 日志
  auto raylet_client = raylet_client_pool_.GetOrConnectByAddress(remote_address);
  raylet_client->RequestWorkerLease(
      actor->GetLeaseSpecification().GetMessage(),
      actor->GetGrantOrReject(),
      [this, actor, node](const Status &status,
                          const rpc::RequestWorkerLeaseReply &reply) {
        HandleWorkerLeaseReply(actor, node, status, reply);
      }, 0);
}
```

#### 处理 Lease 响应

```cpp
// src/ray/gcs/actor/gcs_actor_scheduler.cc:519-599
void GcsActorScheduler::HandleWorkerLeaseReply(...) {
  if (status.ok()) {
    if (reply.rejected()) {
      // 资源不足
      RAY_LOG(INFO) << "Failed to lease worker from node ... resources are not enough";
      HandleWorkerLeaseRejectedReply(actor, reply);
    } else {
      // RPC 成功（但可能只是 spillback）
      RAY_LOG(INFO) << "Finished leasing worker from ...";
      HandleWorkerLeaseGrantedReply(actor, reply, node);
    }
  } else {
    RetryLeasingWorkerFromNode(actor, node);  // RPC 失败，重试
  }
}
```

#### 判断是否真正拿到 Worker

```cpp
// src/ray/gcs/actor/gcs_actor_scheduler.cc:296-365
void GcsActorScheduler::HandleWorkerLeaseGrantedReply(...) {
  const auto &worker_address = reply.worker_address();
  if (worker_address.node_id().empty()) {
    // 没有真正拿到 worker，只拿到 spillback 地址
    actor->SetGrantOrReject(true);  // spillback 节点必须明确回复
    LeaseWorkerFromNode(actor, spill_back_node);  // 继续 lease
  } else {
    // 真正拿到 worker
    auto leased_worker = std::make_shared<GcsLeasedWorker>(worker_address, ...);
    CreateActorOnWorker(actor, leased_worker);  // 创建 Actor
  }
}
```

#### 在 Worker 上创建 Actor

```cpp
// src/ray/gcs/actor/gcs_actor_scheduler.cc:382-452
void GcsActorScheduler::CreateActorOnWorker(...) {
  RAY_LOG(INFO) << "Submitting actor creation task to worker.";  // ← 真正拿到 worker
  auto request = std::make_unique<rpc::PushTaskRequest>();
  request->mutable_task_spec()->CopyFrom(actor->GetCreationTaskSpecification().GetMessage());
  auto client = worker_client_pool_.GetOrConnect(worker->GetAddress());
  client->PushNormalTask(std::move(request),
      [this, actor, worker](Status status, const rpc::PushTaskReply &reply) {
        if (status.ok()) {
          RAY_LOG(INFO) << "Actor creation task succeeded.";
          schedule_success_handler_(actor, reply);
        } else {
          RAY_LOG(INFO) << "Actor creation task failed, will be retried.";
          RetryCreatingActorOnWorker(actor, worker);
        }
      });
}
```

#### 完整调度时序图

```
                    GCS                            Owner Node                 Spillback Node
                     │                                  │                          │
  Schedule(actor)    │                                  │                          │
        │            │                                  │                          │
        ▼            │                                  │                          │
  SelectForwardingNode()                                │                          │
  (选择 owner 节点)  │                                  │                          │
        │            │                                  │                          │
        ▼            │                                  │                          │
  LeaseWorkerFromNode()                                 │                          │
  GrantOrReject=false│ ──RequestWorkerLease────────────▶│                          │
        │            │                                  │ 选择 spillback 节点       │
        │            │ ◀─────Reply (spillback addr)────│                          │
        │            │                                  │                          │
        ▼            │                                  │                          │
  HandleWorkerLeaseGrantedReply()                       │                          │
  worker_address.empty() == true                        │                          │
        │            │                                  │                          │
        ▼            │                                  │                          │
  LeaseWorkerFromNode()                                 │                          │
  GrantOrReject=true │ ──RequestWorkerLease────────────────────────────────────────▶│
        │            │                                  │                 检查资源  │
        │            │ ◀─────Reply (worker_address)────────────────────────────────│
        ▼            │                                  │                          │
  CreateActorOnWorker()                                 │                          │
        │            │ ──PushNormalTask────────────────────────────────────────────▶│
        │            │ ◀─────Reply─────────────────────────────────────────────────│
        ▼            │                                  │                          │
  "Actor creation task succeeded"                       │                          │
```

### 4.6 ClusterLeaseManager 调度逻辑

```cpp
// src/ray/raylet/scheduling/cluster_lease_manager.cc:196-296
void ClusterLeaseManager::ScheduleAndGrantLeases() {
  TryScheduleInfeasibleLease();  // 检查之前不可行的任务是否现在可行

  for (auto shapes_it = leases_to_schedule_.begin(); ...) {
    auto &work_queue = shapes_it->second;
    bool is_infeasible = false;

    for (auto work_it = work_queue.begin(); work_it != work_queue.end();) {
      const std::shared_ptr<internal::Work> &work = *work_it;
      RayLease lease = work->lease_;

      // 在集群资源视图中查找最佳节点
      auto scheduling_node_id = cluster_resource_scheduler_.GetBestSchedulableNode(
          lease.GetLeaseSpecification(),
          /*preferred_node_id*/ ...,
          /*exclude_local_node*/ false,
          /*requires_object_store_memory*/ false,
          &is_infeasible);

      if (scheduling_node_id.IsNil()) {
        // 找不到可用节点！task 留在队列中
        // task 状态保持为 PENDING_NODE_ASSIGNMENT
        break;  // 跳过这个 shape 的所有 task
      }

      ScheduleOnNode(node_id, work);
      work_it = work_queue.erase(work_it);
    }

    if (is_infeasible) {
      // 整个 scheduling class 不可行，移到 infeasible 队列
      infeasible_leases_[shapes_it->first] = std::move(shapes_it->second);
      leases_to_schedule_.erase(shapes_it++);
    }
  }

  local_lease_manager_.ScheduleAndGrantLeases();
}
```

**`ScheduleAndGrantLeases` 的触发路径**：

```cpp
// src/ray/raylet/node_manager.cc:2982-3001
void NodeManager::ConsumeSyncMessage(...) {
  if (message->message_type() == syncer::MessageType::RESOURCE_VIEW) {
    const bool capacity_updated = ResourceCreateUpdated(node_id, resources);
    const bool usage_update = UpdateResourceUsage(node_id, resource_view_sync_message);
    if (capacity_updated || usage_update) {
      cluster_lease_manager_.ScheduleAndGrantLeases();  // ← 资源变化时触发
    }
  }
}
```

**关键点**：RaySyncer 断开 → `ConsumeSyncMessage` 不再被调用 → 调度循环无法推进。

### 4.7 Ray Data StreamingExecutor 控制循环

```python
# python/ray/data/_internal/execution/streaming_executor.py:569-665
def _scheduling_loop_step(self, topology: Topology) -> bool:
    self._resource_manager.update_usages()

    # 1. 等待已提交 task 完成（100ms 超时）
    errored_blocks_per_op, _ = process_completed_tasks(
        topology, self._backpressure_policies, self._max_errored_blocks,
    )

    # 2. 选择下一个要执行的算子
    while True:
        op = select_operator_to_run(
            topology, self._resource_manager, self._backpressure_policies,
            ensure_liveness=self._consumer_idling(), ranker=self._ranker,
        )
        if op is None:
            break
        topology[op].dispatch_next_task()

# python/ray/data/_internal/execution/streaming_executor_state.py:482-545
def process_completed_tasks(topology, ...):
    active_tasks = {}
    for op, state in topology.items():
        for task in op.get_active_tasks():
            active_tasks[task.get_waitable()] = (state, task)

    if active_tasks:
        ready, _ = ray.wait(
            list(active_tasks.keys()),
            num_returns=len(active_tasks),
            fetch_local=False,
            timeout=0.1,  # ← 100ms 超时
        )
    # GCS 故障时：ready 为空 → 无 task 完成 → pipeline 卡死
```

---

## 五、排查命令速查表

### GCS 状态检查

```bash
# 1. 检查 GCS 是否存活及 CPU
ps aux | grep gcs_server | grep -v grep

# 2. GCS 线程分布（定位瓶颈线程）
ps -T -p $(pgrep -f gcs_server) -o tid,comm,%cpu | sort -k3 -rn | head -30

# 3. 检查 GCS 错误日志
tail -50 /tmp/ray/session_latest/logs/gcs_server.err

# 4. 查看 GCS 压力日志
grep -E "slow|timeout|backlog|lag|took [0-9]{3,}ms" \
  /tmp/ray/session_latest/logs/gcs_server.out | tail -50
```

### FD 相关检查

```bash
# 5. 检查 FD limit 和使用量
cat /proc/$(pgrep -f gcs_server)/limits | grep 'open files'
ls /proc/$(pgrep -f gcs_server)/fd | wc -l

# 6. 检查容器级 ulimit（对比进程级）
ulimit -n

# 7. 分析 FD 类型分布
ls /proc/$(pgrep -f gcs_server)/fd | xargs -I{} readlink /proc/$(pgrep -f gcs_server)/fd/{} | sed 's/:.*//' | sort | uniq -c | sort -rn

# 8. 系统级 FD 使用
cat /proc/sys/fs/file-nr
```

### 网络连接检查

```bash
# 9. GCS 进程 TCP 连接数和节点数
ss -tn6p | grep "pid=$(pgrep -f gcs_server)" | wc -l
ss -tn6p | grep "pid=$(pgrep -f gcs_server)" | awk '{print $5}' | rev | cut -d: -f2- | rev | sort -u | wc -l

# 10. 每节点平均连接数
ss -tn6p | grep "pid=$(pgrep -f gcs_server)" | awk '{print $5}' | rev | cut -d: -f2- | rev | sort | uniq -c | sort -rn | awk '{sum+=$1; count++} END{print "total="sum, "nodes="count, "avg="sum/count}'

# 11. 查看端口积压（Recv-Q > 0 说明连接排队）
ss -tlnp | grep -E '(6379|8265)'

# 12. 全局 TCP 连接统计
ss -s
```

### 心跳与调度检查

```bash
# 13. 查看心跳延迟日志
grep "lagging heartbeats" /tmp/ray/session_latest/logs/gcs_server.out

# 14. 查看节点死亡日志
grep -E "Node.*dead|marked dead" /tmp/ray/session_latest/logs/gcs_server.out

# 15. 查看 Raylet 调度告警
tail -30 /tmp/ray/session_latest/logs/raylet.out | grep -i 'scheduling_class\|types of tasks'

# 16. Actor 调度日志
grep -E "Leasing worker|Finished leasing|Failed to lease|Submitting actor" \
  /tmp/ray/session_latest/logs/gcs_server.out | tail -100

# 17. 统计 lease 失败次数
grep "Failed to lease.*resources are not enough" \
  /tmp/ray/session_latest/logs/gcs_server.out | wc -l
```

### 节点状态检查

```bash
# 18. GCS 进程详细状态
cat /proc/$(pgrep -f gcs_server)/status | grep -E '(VmRSS|VmSize|Threads)'

# 19. 特定节点的所有日志
grep "<node_id>" /tmp/ray/session_latest/logs/gcs_server.out

# 20. 集群状态
ray status
```

---

## 六、配置参数与调优建议

### 关键配置参数

```json
{
  "raylet_report_resources_period_milliseconds": 500,
  "ray_syncer_message_refresh_interval_ms": 10000,
  "health_check_period_ms": 10000,
  "health_check_timeout_ms": 10000,
  "health_check_failure_threshold": 10,
  "num_heartbeats_timeout": 30,
  "gcs_server_rpc_server_thread_num": 64
}
```

### 参数调整建议

| 参数 | 默认/当前值 | 建议值 | 影响 |
|------|-----------|--------|------|
| `ulimit -n`（启动脚本） | 65536 | **1048576** | **P0**：FD 上限 |
| `raylet_report_resources_period_milliseconds` | 500 | 1000 | 减少 GCS 请求量 |
| `ray_syncer_message_refresh_interval_ms` | 10000 | 5000~20000 | 平衡准确性和负载 |
| `num_heartbeats_timeout` | 30 | 60 | 减少误判死亡 |
| `health_check_failure_threshold` | 5 | 10 | 增加容忍次数 |
| `health_check_timeout_ms` | 10000 | 30000 | 增加单次超时容忍 |

### 参数调整的权衡

```
                 稳定性                    实时性
                   ↑                         ↑
   放宽间隔/超时 ──────────────────────────────── 收紧间隔/超时
                   │                         │
           减少 GCS 压力              资源视图精准
           减少误判死亡              快速检测故障
           集群更稳定                调度更准确
```

### 优化方案汇总

#### 短期（配置调整，无需改代码）

| 措施 | 优先级 | 说明 |
|------|--------|------|
| 提高 GCS FD limit 至 1048576 | P0 | 修改启动脚本，重启集群 |
| 监控 GCS FD 使用率 | P1 | 定期采集 `/proc/$(pgrep gcs_server)/fd` |
| 放宽心跳超时至 60 | P1 | 减少 GCS 压力下的误判 |
| 降低资源上报频率至 1000ms | P2 | 减少 GCS 接收的请求量 |

#### 中期（架构调整）

| 措施 | 优先级 | 说明 |
|------|--------|------|
| 使用 Placement Group 预留资源 | P2 | 减少 lease 重试次数 |
| 批量创建 Actor 时降低并发 | P2 | 避免瞬时调度压力 |
| 减少 scheduling class 数量 | P2 | 统一 Ray Data task 的资源规格 |

#### 长期

| 措施 | 优先级 | 说明 |
|------|--------|------|
| 拆分集群 | P3 | 2000+ 节点拆成 2~3 个小集群 |
| 升级 Ray 版本 | P3 | 关注 syncer 优化相关更新 |
| GCS 高可用部署 | P3 | 使用外部 Redis 作为存储后端 |

---

## 七、故障时间线汇总

### 案例一：FD 耗尽（2246 节点，2026-05-16）

| 时间 | 事件 |
|------|------|
| 00:44:22 | Ray 集群启动（启动脚本中 `ulimit -n 65536`） |
| 00:44:30 | RaySyncer 初始化，出现无害的 `Batch timer error: Success` |
| **01:46:21** | **首次出现** "File descriptor limit reached"，FD 开始耗尽 |
| 01:46 ~ 14:17 | FD 持续满载，GCS 无法接受新连接（共 1454+ 次报错） |
| 14:11 | `ray status` 确认 GCS 连接超时 |
| 14:15 | 确认 GCS FD **65536/65536** 打满，99.95% 为 socket |
| 14:17 | 确认 2246 节点 × 29 连接 = 65134，发现 14031 种 scheduling class |
| 14:36 | `ray job stop` 也确认超时 |

### 案例二：Syncer 瓶颈（800+ 节点，2026-04-30）

| 时间 | 事件 |
|------|------|
| 19:19:16 | Actor 注册，开始调度 |
| 19:19:16~27 | lease 反复失败（syncer 延迟导致资源视图不准） |
| 19:19:41 | Actor 创建成功（耗时 25 秒） |
| 19:35:59 | 开始异常：无法检查 worker 状态 |
| 19:36:08 | 第一次健康检查失败（Connection refused） |
| 19:36:08~37:42 | 健康检查持续失败（10 次，每 10 秒一次） |
| 19:37:42 | 节点被标记为死亡，重建 7 个 Actor |

---

## 八、总结与最佳实践

### 两次故障的根因对比

| 维度 | 案例一（2246 节点） | 案例二（800+ 节点） |
|------|-------|-------|
| 核心症状 | Task 调度阻塞 | Actor 调度慢 + 节点误判死亡 |
| 根因 | FD 耗尽（ulimit 过低） | RaySyncer 单线程瓶颈 |
| GCS CPU | 248%（重试 socket 创建） | 750%（syncer 89% + 调度压力） |
| 影响机制 | 无法建新连接 → 资源广播中断 → 调度冻结 | 资源同步延迟 → 调度选错节点 → 反复重试 |
| 修复 | 调大 ulimit -n | 调整 syncer 参数 / 集群拆分 |

### 通用排查流程

```
1. 确认现象
   └─ ray status 能否连通？task/actor 卡在哪个状态？

2. 检查 GCS 进程
   └─ CPU 多高？哪个线程打满？
      ├─ ray_syncer_io_c 打满 → Syncer 瓶颈（案例二）
      └─ 整体 CPU 高 + 错误日志 → FD/其他问题（案例一）

3. 检查 FD（如果 GCS 有错误日志）
   └─ /proc/<pid>/fd 数量 vs limits
      └─ 接近上限 → 分析 socket 来源（节点数 × 连接数）

4. 检查网络连接
   └─ ss -s 全局连接数
   └─ ss -tnp 分析连接来源

5. 检查健康检查
   └─ 是否有节点误判死亡？
      ├─ Connection refused → 节点真死（OOM）
      └─ Deadline Exceeded → 主线程阻塞

6. 分析心跳/Syncer 日志
   └─ 确认是 GCS 端还是 Raylet 端问题

7. 制定修复方案
   └─ P0: 紧急修复（ulimit、配置）
   └─ P1: 监控告警
   └─ P2: 架构优化
```

### 大规模集群 Head 节点最佳实践

| 建议 | 说明 |
|------|------|
| `ulimit -n 1048576` | FD 上限设为容器允许的最大值 |
| `--num-cpus=0 --num-gpus=0` | Head 节点不承担计算任务 |
| 监控 GCS FD 使用率 | 80% 即告警 |
| 监控 RaySyncer 线程 CPU | 接近 100% 需要优化 |
| 监控 GCS 主线程 CPU | 持续 > 80% 说明请求压力过大 |
| 预留充足内存 | GCS 在 2000+ 节点下可能占用 10~20GB |
| 集群规模评估 | 单集群建议不超过 2000 节点 |

### 关键源码位置速查

| 文件 | 关键函数 | 说明 |
|------|---------|------|
| `gcs_server_io_context_policy.h:31` | `GcsServerIOContextPolicy` | io_context 分配策略 |
| `ray_syncer.cc:81` | `Connect` | BiDi 流建立与重连 |
| `ray_syncer.cc:209` | `BroadcastMessage` | 资源广播 |
| `gcs_health_check_manager.cc:122` | `StartHealthCheck` | 健康检查核心 |
| `gcs_health_check_manager.cc:83` | `FailNode` | 判定节点死亡 |
| `gcs_node_manager.cc:539` | `InferDeathInfo` | 死亡原因推断 |
| `gcs_node_manager.cc:686` | `InternalOnNodeFailure` | 节点失败处理 |
| `gcs_actor_scheduler.cc:49` | `Schedule` | Actor 调度入口 |
| `gcs_actor_scheduler.cc:234` | `LeaseWorkerFromNode` | 发起 lease 请求 |
| `gcs_actor_scheduler.cc:296` | `HandleWorkerLeaseGrantedReply` | 判断是否拿到 worker |
| `gcs_actor_scheduler.cc:382` | `CreateActorOnWorker` | 在 worker 上创建 Actor |
| `cluster_lease_manager.cc:196` | `ScheduleAndGrantLeases` | Task 调度主循环 |
| `cluster_lease_manager.cc:422` | `ScheduleOnNode` | 本地/spillback 调度 |
| `node_manager.cc:2982` | `ConsumeSyncMessage` | 资源更新触发重调度 |
| `scheduling_class_util.cc:175` | `GetSchedulingClass` | scheduling class 告警 |
| `streaming_executor.py:569` | `_scheduling_loop_step` | Ray Data 调度循环 |
| `streaming_executor_state.py:482` | `process_completed_tasks` | ray.wait 等待 |

---

## 相关文档

- [RaySyncer 资源同步机制与配置参数深度分析](./ray-syncer-resource-sync-mechanism.md)
- [RaySyncer IO/CPU 高排查](./ray-syncer-io-cpu-high-troubleshooting.md)
- [Ray Data Task 状态分析](./ray_data_task_status_analysis.md)
- [Ray Data 调度循环优化](./ray_data_schedule_loop_optimization.md)
