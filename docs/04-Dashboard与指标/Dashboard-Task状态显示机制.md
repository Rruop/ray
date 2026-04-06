# Ray Dashboard 任务状态显示分析指南

## 目录

1. [概述](#1-概述)
2. [Ray Core 任务状态详解](#2-ray-core-任务状态详解)
3. [Dashboard 状态映射机制](#3-dashboard-状态映射机制)
4. [Progress Bar 与 Task Table 数据不一致问题](#4-progress-bar-与-task-table-数据不一致问题)
5. [Unaccounted 状态详解](#5-unaccounted-状态详解)
6. [Actor Task 特殊状态分析](#6-actor-task-特殊状态分析)
7. [Ray Data 任务与 Ray Core 任务的关系](#7-ray-data-任务与-ray-core-任务的关系)
8. [配置调优建议](#8-配置调优建议)
9. [关键代码位置](#9-关键代码位置)
10. [Task Event Buffer 完整数据流](#10-task-event-buffer-完整数据流)
11. [GCS 淘汰机制详解](#11-gcs-淘汰机制详解)
12. [GCS stats_counter_ 详解](#12-gcs-stats_counter_-详解)
13. [Task Attempt 与 State 的对应关系](#13-task-attempt-与-state-的对应关系)
14. [GCS 查询与 Dashboard 过滤截断深度分析](#114-gcs-查询与-dashboard-过滤截断深度分析)
  - 11.10 GCS 端 filter_fn 与 limit 的先后顺序
  - 11.11 Dashboard 端二次过滤与截断
  - 11.12 /api/v0/tasks 与 /api/v0/tasks/summarize 的区别
  - 11.13 按 job_id 查询时的顺序问题
  - 11.13a 不带 commit 9be153f7ae 时的状态统计行为分析
  - 11.14 僵尸 Entry 与 include_task_info 传递路径
  - 11.15 Progress Bar RUNNING 数量完整统计链路
  - 11.16 /api/v0/tasks 的 task state 推导（Python 侧）
  - 11.17 诊断方法：区分僵尸 vs drop
  - 11.18 /api/v0/tasks 的 RUNNING 数量统计方式
  - 11.19 /api/v0/tasks 与 /api/v0/tasks/summarize 的 task 状态区别
  - 11.20 PENDING_NODE_ASSIGNMENT vs PENDING_ARGS_FETCH 状态可见性
  - 11.21 Progress Bar 状态合并完整链路
  - 11.22 events_by_task / num_after_truncation / num_filtered 精确关系
  - 11.23 task_info 与 state_updates 的生命周期
  - 11.24 total 中 limit 截断为何统计 has_state_updates
  - 11.25 node_id_to_summary 中 total_tasks / state_counts 统计来源
  - 11.26 不带 commit 时 Unaccounted 的精确计算

---

## 1. 概述

### 1.1 问题背景

在使用 Ray Dashboard 监控作业时，经常会遇到以下困惑：

- Progress Bar 显示大量 "Unaccounted" 任务
- "Waiting for scheduling" 数量与 Task Table 中 `submitted_to_worker` 状态不一致
- Ray Data 日志显示的任务数与 Dashboard 显示的数量不匹配
- 不清楚各种任务状态（如 `PENDING_ACTOR_TASK_ARGS_FETCH`）的含义
- `PENDING_ARGS_FETCH` 和 `PENDING_OBJ_STORE_MEM_AVAIL` 状态在 Dashboard 上看不到

### 1.2 本文档目标

- 解释 Ray Core 的任务状态机制
- 说明 Dashboard 如何映射和显示这些状态
- 区分 GCS Task Event 系统与 Raylet Prometheus Metrics 系统
- 分析 Owner 侧依赖解析和 Raylet 侧 args fetch 的完整流程
- 分析数据不一致的原因和解决方案
- 提供配置调优建议

---

## 2. Ray Core 任务状态详解

### 2.1 完整任务状态列表

Ray Core 定义了以下任务状态（来自 `src/ray/protobuf/common.proto:885-920`）：

```protobuf
enum TaskStatus {
  // We don't have a status for this task because we are not the owner or the
  // task metadata has already been deleted.
  NIL = 0;
  // The task is waiting for its dependencies to be created. For actor tasks, this
  // can also indicate Ray is waiting for the target actor to be created.
  PENDING_ARGS_AVAIL = 1;
  // All dependencies have been created and Ray is confirming a node / worker for the
  // task to be executed on. This step is part of Ray's distributed scheduling protocol.
  PENDING_NODE_ASSIGNMENT = 2;
  // The task has been tentatively assigned to a node, but Ray is waiting for enough
  // object store memory on the node to free up for downloading task dependencies.
  // This state is a sub-state of PENDING_NODE_ASSIGNMENT and used for metrics only.
  PENDING_OBJ_STORE_MEM_AVAIL = 3;
  // The task has been tentatively assigned to a node, and its dependencies are being
  // actively downloaded onto the node.
  // This state is a sub-state of PENDING_NODE_ASSIGNMENT and used for metrics only.
  PENDING_ARGS_FETCH = 4;
  // A node / worker for the task has been selected, and the task has been submitted
  // to the worker. It will be executed shortly. For actor tasks, execution may be
  // delayed to satisfy ordering constraints or argument fetching.
  SUBMITTED_TO_WORKER = 5;
  // The actor task is fetching arguments. This happens after the actor worker
  // receives the task and the actor task has object refs as arguments.
  PENDING_ACTOR_TASK_ARGS_FETCH = 6;
  // The actor task is waiting due to ordering or concurrency constraints.
  PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY = 7;
  // The task is running on a worker.
  RUNNING = 8;
  // The task is running on a worker, but is blocked in a ray.get() call.
  // This state is a sub-state of RUNNING and used for metrics only.
  RUNNING_IN_RAY_GET = 9;
  // The task is running on a worker, but is blocked in a ray.wait() call.
  // This state is a sub-state of RUNNING and used for metrics only.
  RUNNING_IN_RAY_WAIT = 10;
  // The task has finished.
  FINISHED = 11;
  // The task has finished but failed with an Exception or system error.
  FAILED = 12;
  // The task is attempting to pin args and fetch them for execution, used for metrics
  // only and is a sub state of RUNNING
  GETTING_AND_PINNING_ARGS = 13;
}
```

**关键标注：proto 中明确标注了 5 个状态为 "sub-state ... used for metrics only"：**

| 状态 | 所属父状态 | 标注 |
|------|----------|------|
| `PENDING_OBJ_STORE_MEM_AVAIL` (3) | `PENDING_NODE_ASSIGNMENT` (2) | metrics only |
| `PENDING_ARGS_FETCH` (4) | `PENDING_NODE_ASSIGNMENT` (2) | metrics only |
| `RUNNING_IN_RAY_GET` (9) | `RUNNING` (8) | metrics only |
| `RUNNING_IN_RAY_WAIT` (10) | `RUNNING` (8) | metrics only |
| `GETTING_AND_PINNING_ARGS` (13) | `RUNNING` (8) | metrics only |

这些 "metrics only" 状态**不会出现在 GCS Task Event 中，因此 Dashboard 不可见**（详见 §2.3）。

### 2.2 任务状态流转图

状态流转需要区分两套系统：**GCS Task Event（Dashboard 数据源，CoreWorker 上报）** 和 **Raylet Prometheus Metrics（metrics 系统，不上报 GCS）**。

#### 2.2.1 GCS Task Event 中的真实状态转换

以下状态转换由 CoreWorker（Owner/Executor）通过 `TaskEventBuffer` 上报到 GCS，是 Dashboard 和 `ray list tasks` 的数据源：

```
普通 Task (NORMAL_TASK):

  PENDING_ARGS_AVAIL ──→ PENDING_NODE_ASSIGNMENT ──→ SUBMITTED_TO_WORKER ──→ RUNNING ──→ FINISHED / FAILED
    (Owner 上报)            (Owner 上报)                (Owner 上报)           (Executor 上报)  (Owner 上报)
       │                        │
       │  task_manager.cc:343     │  task_manager.cc:1678
       │  AddPendingTask          │  MarkDependenciesResolved
       │                          │
       │                          │  Owner 视角：等 lease 回复
       │                          │  不知道 raylet 内部在做什么
       │                          │  ↓
       │                          │  ┌─ Raylet 内部子状态（metrics only）────┐
       │                          │  │                                       │
       │                          │  │  PENDING_ARGS_FETCH                   │
       │                          │  │  (PullManager 活跃拉取 args)          │
       │                          │  │                                       │
       │                          │  │  PENDING_OBJ_STORE_MEM_AVAIL          │
       │                          │  │  (object store 满，拉取暂停)          │
       │                          │  │                                       │
       │                          │  │  这些子状态不上报 GCS                │
       │                          │  │  Dashboard 看不到                    │
       │                          │  └───────────────────────────────────────┘
       │                          │
       │                          │  task_manager.cc:1694
       │                          │  MarkTaskWaitingForExecution
       │                          │  (收到 lease grant 回复)
       ↓                          ↓
  (如果上游 task              PENDING_NODE_ASSIGNMENT 直接跳到
   还没完成，                   SUBMITTED_TO_WORKER
   卡在这里)


Actor Task (ACTOR_TASK):

  PENDING_ARGS_AVAIL ──→ SUBMITTED_TO_WORKER ──→ PENDING_ACTOR_TASK_ARGS_FETCH ──→ RUNNING ──→ FINISHED / FAILED
    (Owner 上报)            (Owner 上报)              (Actor Worker 上报)              (Executor 上报)
                                │                              │
                                │                      PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY
                                │                      (Actor Worker 上报)
                                │
                                │  Actor task 先发送到 Actor Worker，
                                │  再在 Worker 内部等参数/顺序
```

#### 2.2.2 Raylet Metrics 子状态（不上报 GCS）

`PENDING_NODE_ASSIGNMENT` 在 Raylet 内部被 `LeaseDependencyManager` 拆分为两个子状态，仅存在于 Prometheus Metrics 中：

```
Raylet LeaseDependencyManager (lease_dependency_manager.h:77-95):
                                                        Metrics 中的状态
                                                        (task_by_state_counter_)

  Owner 上报的 PENDING_NODE_ASSIGNMENT (正数)
    │
    ├── Raylet 抵消: Record(-num_total, "PENDING_NODE_ASSIGNMENT")  → 净值 0
    │
    ├── 拆分: Record(num_total - num_inactive, "PENDING_ARGS_FETCH")
    │         ↑ PullManager 活跃拉取的 lease 数
    │
    └── 拆分: Record(num_inactive, "PENDING_OBJ_STORE_MEM_AVAIL")
              ↑ PullManager 因内存不足暂停的 lease 数
```

**重要：Raylet 从不调用 `RecordTaskStatusEventIfNeeded`。** 搜索整个 `src/ray/raylet/` 目录，没有任何 task event 上报代码。Raylet 只有 `task_by_state_counter_`（Prometheus gauge），用于聚合 metrics，不用于 per-task event。

### 2.3 两套状态系统的独立性

Ray 的 task 状态通过两个完全独立的系统暴露，它们的**数据源、上报方式、可见渠道**都不同：

| 维度 | GCS Task Event | Raylet Prometheus Metrics |
|------|---------------|---------------------------|
| **谁上报** | CoreWorker (Owner/Executor) 的 `TaskEventBuffer` | Raylet 的 `LeaseDependencyManager` |
| **上报方式** | 每个状态转换发 `RecordTaskStatusEventIfNeeded` → flush 到 GCS | 周期性 `task_by_state_counter_.Record()` 写 Prometheus gauge |
| **粒度** | **per-task**（每个 task attempt 一条记录） | **聚合计数**（按 func_name + state 分组） |
| **可见渠道** | Dashboard task 列表、`ray list tasks`、`ray memory` | Prometheus/Grafana 指标面板 |
| **包含哪些状态** | CoreWorker 上报的所有状态（不含 metrics-only 子状态） | 额外包含 `PENDING_ARGS_FETCH`、`PENDING_OBJ_STORE_MEM_AVAIL` |
| **淘汰影响** | GCS 存储 100K 限制，满后按优先级淘汰 | 无淘汰，始终是实时聚合计数 |

**这意味着：**
- Dashboard 上看到的 `PENDING_NODE_ASSIGNMENT` 是一个"黑盒"——Driver 只知道"lease 请求发出去了，还没收到回复"，不知道 Raylet 内部到底卡在 `PENDING_ARGS_FETCH` 还是 `PENDING_OBJ_STORE_MEM_AVAIL`
- 要看 Raylet 内部子状态，需查 Prometheus metrics 或 raylet 日志
- `ray memory` 命令能看到 `RUNNING_IN_RAY_GET` / `RUNNING_IN_RAY_WAIT` / `GETTING_AND_PINNING_ARGS`，因为 `ray memory` 查的是 Owner 侧 `TaskManager` 的 in-memory 状态（`AddTaskStatusInfo`），不是 GCS task event

### 2.4 TaskStatus GCS 可见性对照表

以下是全部 14 个 TaskStatus 状态在 GCS Task Event 中的可见性：

| # | 状态 | proto 标注 | 上报者 | GCS 可见？ | 说明 |
|---|------|----------|--------|-----------|------|
| 0 | `NIL` | 初始状态 | — | 否 | 无数据记录 |
| 1 | `PENDING_ARGS_AVAIL` | — | Owner (CoreWorker) | **是** | task_manager.cc:343, AddPendingTask |
| 2 | `PENDING_NODE_ASSIGNMENT` | — | Owner (CoreWorker) | **是** | task_manager.cc:1678, MarkDependenciesResolved |
| 3 | `PENDING_OBJ_STORE_MEM_AVAIL` | "sub-state, **metrics only**" | Raylet (metrics) | **否** | lease_dependency_manager.h:89-95 |
| 4 | `PENDING_ARGS_FETCH` | "sub-state, **metrics only**" | Raylet (metrics) | **否** | lease_dependency_manager.h:83-88 |
| 5 | `SUBMITTED_TO_WORKER` | — | Owner (CoreWorker) | **是** | task_manager.cc:1694, MarkTaskWaitingForExecution |
| 6 | `PENDING_ACTOR_TASK_ARGS_FETCH` | — | Actor Worker (CoreWorker) | **是** | ordered/unordered_actor_task_execution_queue.cc |
| 7 | `PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY` | — | Actor Worker (CoreWorker) | **是** | ordered/unordered_actor_task_execution_queue.cc |
| 8 | `RUNNING` | — | Executor (CoreWorker) | **是** | core_worker.cc:3063, ExecuteTask |
| 9 | `RUNNING_IN_RAY_GET` | "sub-state, **metrics only**" | Executor (metrics) | **否** | Executor 侧 metrics gauge |
| 10 | `RUNNING_IN_RAY_WAIT` | "sub-state, **metrics only**" | Executor (metrics) | **否** | Executor 侧 metrics gauge |
| 11 | `FINISHED` | — | Owner (CoreWorker) | **是** | task_manager.cc:1053, CompletePendingTask |
| 12 | `FAILED` | — | Owner (CoreWorker) | **是** | task_manager.cc:1053, CompletePendingTask |
| 13 | `GETTING_AND_PINNING_ARGS` | "sub-state, **metrics only**" | Executor (metrics) | **否** | core_worker.cc:3028-3032, ExecuteTask |

**6 个 "metrics only" 状态不出现在 GCS Task Event 中**，Dashboard 不可见：
- `NIL`（无数据）
- `PENDING_OBJ_STORE_MEM_AVAIL`
- `PENDING_ARGS_FETCH`
- `RUNNING_IN_RAY_GET`
- `RUNNING_IN_RAY_WAIT`
- `GETTING_AND_PINNING_ARGS`

**注意：** `RUNNING_IN_RAY_GET`、`RUNNING_IN_RAY_WAIT`、`GETTING_AND_PINNING_ARGS` 虽然不上报 GCS Task Event，但 `ray memory` 命令能看到它们——因为 `ray memory` 查的是 Owner 侧 `TaskManager` 的 in-memory 状态（`AddTaskStatusInfo`），不是 GCS task event。

### 2.5 PENDING_NODE_ASSIGNMENT 的 Raylet 子状态深度分析

#### 2.5.1 PENDING_ARGS_FETCH — 正在通过网络拉取 plasma 对象

Raylet 已为 task 分配了资源/worker，但 task 的 plasma args 还不在本地节点。`PullManager` **已激活** pull 请求，开始网络传输：

```
PullManager 激活 pull:
  → TryPinObject(obj_id) — 在本地 plasma 预留空间
  → TryToMakeObjectLocal(obj_id)
    → SendPullRequest(obj_id, remote_node_id) — 发 PullRequest RPC 到持有 object 的远端节点
      → 远端节点 HandlePull → Push(object_id, requesting_node) — 发 object 数据块回来
        → 本端 ReceiveObjectChunk → buffer_pool_.WriteChunk
          → 所有 chunk 到齐 → plasma seal object → HandleObjectAdded
            → NodeManager::HandleObjectLocal
              → LeaseDependencyManager::HandleObjectLocal (减少 missing deps)
                → 所有 deps 就绪 → LocalLeaseManager::LeasesUnblocked
                  → 租约从 waiting_lease_queue_ 移到 leases_to_grant_
                    → GrantScheduledLeasesToWorkers → Grant
                      → 回复 Owner lease grant
                        → Owner: MarkTaskWaitingForExecution → SUBMITTED_TO_WORKER
```

**一句话：对象正在网络传输中。**

关键代码位置：
- `pull_manager.cc:112-153` — `ActivateNextBundlePullRequest`：激活 pull，发送网络请求
- `object_manager.cc:199` — `SendPullRequest`：发送 PullRequest RPC 到远端节点
- `object_manager.cc:352` — `HandlePush`：接收远端发来的 object 数据块
- `lease_dependency_manager.cc:307-340` — `HandleObjectLocal`：object 到达本地后减少 missing deps
- `local_lease_manager.cc:711-731` — `LeasesUnblocked`：所有 args 就绪后移到 grant 队列

#### 2.5.2 PENDING_OBJ_STORE_MEM_AVAIL — 拉取暂停，等 object store 腾空间

`PullManager` 检查配额后决定**不激活** pull 请求：

```cpp
// pull_manager.cc:130-152
if (respect_quota && bytes_to_pull > RemainingQuota()) {
    return false;  // 留在 inactive_requests
}
// RemainingQuota = num_bytes_available_ - (num_bytes_being_pulled_ - pinned_objects_size_)
// num_bytes_available_ 来自 plasma_store_runner->GetAvailableMemoryAsync()
```

- 没有发 PullRequest RPC，没有网络传输
- object 没有被 pin，不占配额
- 等待 `ObjectManager::Tick()` 周期性检查 plasma 可用内存，内存够了再激活

**注意：这里检查的是目标执行节点（Raylet 所在节点）的 plasma object store 可用内存，不是 Owner 节点的。**

关键代码位置：
- `pull_manager.cc:167-228` — `UpdatePullsBasedOnAvailableMemory`：根据 plasma 可用内存激活/暂停 pull
- `pull_manager.cc:224-228` — `RemainingQuota`：配额计算
- `object_manager.cc:799-834` — `Tick`：周期性查询 plasma 可用内存并更新 pull 状态

#### 2.5.3 PENDING_ARGS_FETCH 和 PENDING_OBJ_STORE_MEM_AVAIL 会交替吗

**不会在同一个 pull request 上交替。**

`PullManager` 对一个 bundle pull request 的处理是单向的：

```
inactive_requests  ──激活──→  active_requests  ──完成/取消──→  从队列移除
     ↑                              ↑
PENDING_OBJ_STORE_MEM_AVAIL    PENDING_ARGS_FETCH
```

一旦激活，object 开始传输，传输完就 `HandleObjectLocal` → lease 移到 grant 队列。**不会"传到一半暂停，然后等内存，再继续传"。**

对于**同一 task 的多个 args**，可能一部分在 `PENDING_ARGS_FETCH`（已激活），一部分在 `PENDING_OBJ_STORE_MEM_AVAIL`（等内存）。但 metrics 是按 task_name 聚合的，不是按单个 object 报的。

#### 2.5.4 Owner 端 object store 满了的影响

`PENDING_OBJ_STORE_MEM_AVAIL` 检查的是**目标执行节点**的 object store，不是 Owner 节点的。Owner 端 object store 满了的影响路径不同：

```
Owner 节点 object store 满:
  → Owner 节点上的 object 被 spill 到磁盘
  → 远端节点要拉 args 时需要先从磁盘 restore
  → restore 速度慢 → 拉取耗时长 → task 长期卡在 PENDING_ARGS_FETCH
  → 如果目标节点 object store 也满了 → PENDING_OBJ_STORE_MEM_AVAIL

两个节点的 object store 容量是独立的问题。
```

### 2.6 Owner 侧依赖解析机制

#### 2.6.1 PENDING_ARGS_AVAIL 具体做什么

`PENDING_ARGS_AVAIL` 阶段做的是**本地 in-memory store 查询**，不是从远端拉数据。

```
TaskManager::AddPendingTask()
  → 状态 = PENDING_ARGS_AVAIL

LocalDependencyResolver::ResolveDependencies()  (dependency_resolver.cc:81)
  → 遍历 task 的所有 ArgByRef 参数
  → 对每个 ObjectID 调用 in_memory_store_.GetAsync(obj_id, callback)
      ↓
      memory_store.cc:116-126:
      if (obj_id in objects_):    // 查本地 heap 上的 hash map
          callback 立即触发       // 已就绪
      else:
          callback 排队等待       // 还没就绪，等 Put() 被调用时触发
```

**关键：`GetAsync` 查的是 Worker 进程自己的堆内存（`objects_` map），不是 plasma object store，也不是从远端拉。** 它只是检查"这个 ObjectRef 对应的值是否已经在我本地内存中可用"。

`CoreWorkerMemoryStore` 没有**容量限制**——它存储在 Worker 的堆内存中，与 plasma object store 完全独立。

#### 2.6.2 OBJECT_IN_PLASMA 哨兵机制

当 Executor Worker 完成 task 后，通过 `PushTaskReply` RPC 回复 Owner，Owner 的 `TaskManager::CompletePendingTask()` 处理返回值：

```cpp
// task_manager.cc:548-603
StatusOr<bool> TaskManager::HandleTaskReturn(object_id, return_object, worker_node_id, ...) {
  if (return_object.in_plasma()) {
    // 大对象：已被 promote 到 plasma
    reference_counter_.UpdateObjectPinnedAtRaylet(object_id, worker_node_id);
    in_memory_store_.Put(RayObject(OBJECT_IN_PLASMA), object_id, ...);  // 放哨兵
  } else {
    // 小对象：直接在堆内存中
    if (store_in_plasma) {
      put_in_local_plasma_callback_(object, object_id);  // 放到本地 plasma
    } else {
      in_memory_store_.Put(object, object_id, ...);  // 放真实数据
    }
  }
}
```

| Object 状态 | in-memory store 中的内容 | InlineDependencies 行为 |
|------------|------------------------|----------------------|
| **小对象**（直接返回值） | 真实数据 `RayObject(data, metadata)` | 把原始 bytes **内联**到 task spec 中，清除 ObjectRef |
| **大对象**（已 promote 到 plasma） | 哨兵 `RayObject(OBJECT_IN_PLASMA_ERROR)` | **保留 ObjectRef**，不内联数据 |

大对象被写入 plasma 时，Executor 会在 Owner 的 in-memory store 放一个 `OBJECT_IN_PLASMA` 哨兵（`task_manager.cc:568`），所以 `GetAsync` 对 plasma 对象也会**立即返回**。

#### 2.6.3 为什么 Owner 侧要做依赖解析

Owner 侧依赖解析不是为了"拉数据来用"，而是为了**决定 task spec 里参数的传递方式**——这直接影响 Worker 侧怎么拿到数据。

`InlineDependencies` (`dependency_resolver.cc:30-73`) 做的是一个**分类决策**：

```
对每个 ArgByRef 参数:
  if (!IsInPlasmaError):
      → 内联：把原始 bytes 直接写进 task spec，清除 ObjectRef
      → Worker 收到时直接有数据，不需要任何 fetch
  else:  (object 在 plasma 中)
      → 保留 ObjectRef，不内联
      → Worker 侧需要通过 raylet PullManager 从远端 plasma 拉取
```

| 原因 | 说明 |
|------|------|
| 只有 Owner 知道 object 在不在 plasma | 小对象返回值直接存在 Owner 的 in-memory store，大对象被 promote 到 plasma 后会放哨兵 |
| 内联能省掉一次远端 fetch | 如果小对象（几十 KB 的元数据）不内联，Worker 就得通过 raylet 从 Owner 节点的 plasma 里拉，完全不值得 |
| task spec 是 protobuf 消息，序列化时就确定了 | `RequestWorkerLease` 发给 raylet 时 task spec 已经定型，后续不能改 |
| Owner 是 ObjectRef 的持有者 | 引用计数、pin 状态、object 位置信息都在 Owner 的 TaskManager/reference_counter_ 中维护 |

#### 2.6.4 完整数据流图

```
Owner (Driver)                          Executor Worker (远端)
───────────────                         ──────────────────────

Task A: .remote() 提交
  → PENDING_ARGS_AVAIL (无依赖，立即通过)
  → PENDING_NODE_ASSIGNMENT → lease
  → SUBMITTED_TO_WORKER
  → PushTask(task_spec) ──────────────→ HandlePushTask
                                          → ExecuteTask
                                            → GetAndPinArgsForExecutor
                                            → 执行用户代码
                                            → 产出返回值
                                          → PushTaskReply (含 return_objects)
  ←────────────────────────────────────
CompletePendingTask():
  HandleTaskReturn():
    大对象 → in_memory_store.Put(哨兵)   ← Owner 侧 in-memory store
    小对象 → in_memory_store.Put(数据)   ← Owner 侧 in-memory store
  状态 = FINISHED

Task B: .remote() 提交 (依赖 A 的输出)
  → PENDING_ARGS_AVAIL
  → GetAsync(A 的输出)
    → 哨兵/数据已在 → callback 立即触发
    → 大对象: 保留 ObjectRef
    → 小对象: 内联到 task spec
  → PENDING_NODE_ASSIGNMENT → lease
  → SUBMITTED_TO_WORKER
  → PushTask(task_spec) ──────────────→ HandlePushTask
                                        → ExecuteTask
                                          → GetAndPinArgsForExecutor:
                                            → 自己的 in-memory store 放 OBJECT_IN_PLASMA 哨兵
                                            → plasma_store_provider_->Get() 从本地 plasma 拉数据
                                              (raylet PullManager 已提前拉到本地 plasma)
                                          → 执行用户代码
```

**不是"先提交 B 再在 Worker 侧等数据可用"，而是上游 task 必须执行完、Owner 收到回复并在 in-memory store 中写入结果后，下游 task 的 PENDING_ARGS_AVAIL 才能通过。**

---

## 3. Dashboard 状态映射机制

### 3.1 状态合并规则

Dashboard 前端将 Ray Core 的细粒度状态合并为更易理解的分类：

```typescript
// useJobProgress.ts:29-46
const TASK_STATE_NAME_TO_PROGRESS_KEY: Record<TypeTaskStatus, TaskStatus> = {
  // 等待依赖
  PENDING_ARGS_AVAIL: TaskStatus.PENDING_ARGS_AVAIL,

  // 等待调度 - 合并多个状态
  PENDING_NODE_ASSIGNMENT: TaskStatus.PENDING_NODE_ASSIGNMENT,
  PENDING_OBJ_STORE_MEM_AVAIL: TaskStatus.PENDING_NODE_ASSIGNMENT,  // 合并
  PENDING_ARGS_FETCH: TaskStatus.PENDING_NODE_ASSIGNMENT,           // 合并

  // 已提交到 Worker - 合并 Actor 任务状态
  SUBMITTED_TO_WORKER: TaskStatus.SUBMITTED_TO_WORKER,
  PENDING_ACTOR_TASK_ARGS_FETCH: TaskStatus.SUBMITTED_TO_WORKER,              // 合并
  PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY: TaskStatus.SUBMITTED_TO_WORKER, // 合并

  // 运行中 - 合并阻塞状态
  RUNNING: TaskStatus.RUNNING,
  RUNNING_IN_RAY_GET: TaskStatus.RUNNING,  // 合并
  RUNNING_IN_RAY_WAIT: TaskStatus.RUNNING, // 合并

  // 终态
  FINISHED: TaskStatus.FINISHED,
  FAILED: TaskStatus.FAILED,
  NIL: TaskStatus.UNKNOWN,
};
```

**重要说明：** 虽然前端合并规则中包含了 `PENDING_OBJ_STORE_MEM_AVAIL` 和 `PENDING_ARGS_FETCH` 的映射，但实际上这两个状态**不会出现在 GCS Task Event 数据中**（它们是 "metrics only" 子状态，Raylet 不上报 GCS）。因此这个合并规则对这两个状态实际上是"死代码"——GCS 返回的数据中永远不会包含这些状态，前端合并规则存在但不会被触发。

同理，`RUNNING_IN_RAY_GET`、`RUNNING_IN_RAY_WAIT` 和 `GETTING_AND_PINNING_ARGS` 也不会出现在 GCS Task Event 中（虽然前端合并规则中有映射）。

### 3.2 Progress Bar 显示分类

```typescript
// TaskProgressBar.tsx:34-73
const progress: ProgressBarSegment[] = [
  { label: "Finished", value: numFinished },
  { label: "Failed", value: numFailed },
  { label: "Running", value: numRunning },
  { label: "Waiting for scheduling", value: numPendingNodeAssignment + numSubmittedToWorker },  // 合并
  { label: "Waiting for dependencies", value: numPendingArgsAvail },
  { label: "Cancelled", value: numCancelled },
  { label: "Unknown", value: numUnknown },
];
```

**重要**："Waiting for scheduling" 包含了：
- `numPendingNodeAssignment` (等待节点分配)
- `numSubmittedToWorker` (已提交到 Worker，包括 Actor 任务的等待状态)

### 3.3 状态映射完整表格

| Ray Core 状态 | Dashboard TaskStatus | Progress Bar 显示 | GCS Task Event 可见？ |
|--------------|---------------------|------------------|---------------------|
| `PENDING_ARGS_AVAIL` | `PENDING_ARGS_AVAIL` | Waiting for dependencies | 是 |
| `PENDING_NODE_ASSIGNMENT` | `PENDING_NODE_ASSIGNMENT` | Waiting for scheduling | 是 |
| `PENDING_OBJ_STORE_MEM_AVAIL` | `PENDING_NODE_ASSIGNMENT` | Waiting for scheduling | **否** (metrics only) |
| `PENDING_ARGS_FETCH` | `PENDING_NODE_ASSIGNMENT` | Waiting for scheduling | **否** (metrics only) |
| `SUBMITTED_TO_WORKER` | `SUBMITTED_TO_WORKER` | Waiting for scheduling | 是 |
| `PENDING_ACTOR_TASK_ARGS_FETCH` | `SUBMITTED_TO_WORKER` | Waiting for scheduling | 是 |
| `PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY` | `SUBMITTED_TO_WORKER` | Waiting for scheduling | 是 |
| `RUNNING` | `RUNNING` | Running | 是 |
| `RUNNING_IN_RAY_GET` | `RUNNING` | Running | **否** (metrics only) |
| `RUNNING_IN_RAY_WAIT` | `RUNNING` | Running | **否** (metrics only) |
| `GETTING_AND_PINNING_ARGS` | — | — | **否** (metrics only) |
| `FINISHED` | `FINISHED` | Finished | 是 |
| `FAILED` | `FAILED` | Failed | 是 |
| `NIL` | `UNKNOWN` | Unknown | 否 |

### 3.4 /api/v0/tasks 和 /api/v0/tasks/summarize 接口处理逻辑

#### 3.4.1 /api/v0/tasks?detail=1&limit=30000 — Task 列表接口

**完整调用链：**

```
前端 GET /api/v0/tasks?detail=1&limit=30000&filter_keys=job_id&filter_predicates=%3D&filter_values=0b000000
  → state_head.py: StateHead.list_tasks()
    → state_api_utils.py: handle_list_api(self._state_api.list_tasks, req)
      → options_from_req(req):
          limit = 30000
          detail = True  # 返回完整字段
          filters = [("job_id", "=", "0b000000")]
          exclude_driver = True (默认)
      → state_aggregator.py: StateAPIManager.list_tasks(option)
        → self._client.get_all_task_info(filters=..., exclude_driver=True)
          → gRPC GetTaskEvents → GCS
            → GCS 按 job_id 在服务端预过滤 (num_filtered_on_gcs)
            → 返回 reply.events_by_task (当前内存中的 task events)
        → transform(reply):
            result = [protobuf_to_task_state_dict(message) for message in reply.events_by_task]
            result = do_filter(result, option.filters, TaskState, option.detail)  # 本地二次过滤
            result.sort(key=lambda entry: entry["task_id"])
            result = list(islice(result, option.limit))  # 截断到 30000 条
```

**过滤参数处理：** `filter_keys=job_id & filter_predicates=%3D(=) & filter_values=0b000000` 被 `_get_filters_from_req` 解析为 `[("job_id", "=", "0b000000")]`。过滤经历两层：
1. **GCS 侧预过滤**：`get_all_task_info` 把 filters 传给 GCS，GCS 在内存中按 job_id 粗筛
2. **Dashboard 侧二次过滤**：`do_filter()` 对返回结果做精确匹配（字符串大小写不敏感）

**状态来源：** Dashboard 展示的 task 状态来自 GCS 内存中存储的 `state_ts_ns` map。`protobuf_to_task_state_dict` (`common.py:1605`) 的关键逻辑：

```python
# 遍历 state_ts_ns 中记录的所有状态及其时间戳
for state_name, state in TaskStatus.items():
    key = str(state)
    if key in state_ts_ns:
        events.append({"state": state_name, "created_ms": ts_ms})

# 取最后一个 event 的状态作为 task 的当前状态
if len(events) > 0:
    latest_state = events[-1]["state"]
else:
    latest_state = "NIL"
task_state["state"] = latest_state
```

**状态不是由某个字段直接存储的，而是从 `state_ts_ns` map 中的时间戳推导出来的——最后一个时间戳最晚的状态就是当前状态。**

#### 3.4.2 /api/v0/tasks/summarize — Task 状态汇总接口

**完整调用链：**

```
前端 GET /api/v0/tasks/summarize?filter_keys=job_id&filter_predicates=%3D&filter_values=1b000000
  → state_head.py: StateHead.summarize_tasks()
    → state_api_utils.py: handle_summary_api(self._state_api.summarize_tasks, req)
      → summary_options_from_req(req):
          timeout = req.query.get("timeout", 30)
          filters = [("job_id", "=", "1b000000")]
          summary_by = req.query.get("summary_by", "func_name")  # 默认按函数名聚合
      → state_aggregator.py: StateAPIManager.summarize_tasks(option)
        → 内部调用 self.list_tasks(limit=RAY_MAX_LIMIT_FROM_API_SERVER, filters=option.filters, detail=False)
          → 同 /api/v0/tasks 的逻辑：gRPC → GCS GetTaskEvents → do_filter
        → TaskSummaries.to_summary_by_func_name(tasks=result.result)
          → 按 func_or_class_name 分组，统计每组各状态的计数
        → 返回 {summary: {"func_name": {state_counts: {"RUNNING": 5, "FINISHED": 100}}, ...}}
```

**核心逻辑：** `summarize` 本质上就是 `list_tasks` + 客户端聚合。它先拿到该 job 的所有 task，然后按 `func_or_class_name` 分组，每组统计各状态数量。

返回格式示例：
```json
{
  "summary": {
    "cluster": {
      "summary": {
        "_map_task": {
          "func_or_class_name": "_map_task",
          "type": "NORMAL_TASK",
          "state_counts": {
            "FINISHED": 33112,
            "PENDING_NODE_ASSIGNMENT": 254,
            "SUBMITTED_TO_WORKER": 10,
            "PENDING_ARGS_AVAIL": 1
          }
        }
      },
      "total_tasks": 33177,
      "total_actor_tasks": 0,
      "total_actor_scheduled": 0
    }
  }
}
```

#### 3.4.3 两个接口的对比

| 维度 | `/api/v0/tasks?detail=1` | `/api/v0/tasks/summarize` |
|------|--------------------------|---------------------------|
| 数据源 | GCS task event (per-task) | GCS task event (per-task) → 聚合 |
| 返回格式 | 具体 task 列表 | 按 func_name 分组的状态计数 |
| `detail=1` 时额外字段 | events 时间线、profiling_data 等 | 不适用（detail 固定为 False，除非 summary_by=lineage） |
| limit 行为 | 截断到 limit 条 | 用 RAY_MAX_LIMIT_FROM_API_SERVER 尽量多拿 |
| 可见状态 | CoreWorker 上报的所有状态 | 同左 |
| `PENDING_ARGS_FETCH` | **不可见** | **不可见** |
| `PENDING_OBJ_STORE_MEM_AVAIL` | **不可见** | **不可见** |

**两个接口的数据源完全相同**，都是 GCS `GetTaskEvents` RPC。`summarize` 只是做了聚合，不改变可见的状态范围。

**要在 Dashboard 侧区分 task 是卡在"拉 args"还是"等内存"，只能通过查 raylet 的 Prometheus metrics 或 raylet 日志，GCS task event 和 Dashboard API 都看不到这个区分。**

#### 3.4.4 /api/v0/tasks 默认 limit=100 的逻辑链

当 URL 不带 `limit` 参数时，默认返回 100 条。完整调用链如下：

```
HTTP 请求 (无 limit 参数)
  → state_head.py:108  routes.get("/api/v0/tasks") → handle_list_api(self._state_api.list_tasks, req)
    → state_api_utils.py:76  options_from_req(req)
      → line 78: limit = int(req.query.get("limit") if req.query.get("limit") is not None else DEFAULT_LIMIT)
        → common.py:48: DEFAULT_LIMIT = 100
      → line 82: if limit > RAY_MAX_LIMIT_FROM_API_SERVER: raise ValueError(...)
      → return ListApiOptions(limit=100, ...)
    → state_aggregator.py:300  list_tasks(option=ListApiOptions(limit=100))
      → line 306: reply = await self._client.get_all_task_info(...)   // 没传 limit，用默认 RAY_MAX_LIMIT_FROM_DATA_SOURCE=10000
      → line 332: result = list(islice(result, option.limit))  // islice(result, 100) → 只取前 100 条
```

**默认 100 的来源**：`common.py:48` 的 `DEFAULT_LIMIT = 100`，在 `options_from_req` 中当 URL 没带 `limit` 参数时使用。

#### 3.4.5 /api/v0/tasks?detail=1&limit=30000 的截断行为

当请求 `limit=30000` 时，会经历 4 层截断：

```
options_from_req:
  limit = 30000  (从 query param 读取)
  detail = True
  → line 82: if 30000 > RAY_MAX_LIMIT_FROM_API_SERVER (10000):
      raise ValueError("Given limit 30000 exceeds the supported limit 10000...")
```

**直接报 400 BAD REQUEST**。`limit=30000` 超过 `RAY_MAX_LIMIT_FROM_API_SERVER`(默认 10,000)，请求在 `options_from_req` 就被拒绝，根本不会到达 `list_tasks`。

要让 30,000 生效，必须同时设置环境变量 `RAY_MAX_LIMIT_FROM_API_SERVER=30000`（或更高）。即使设置了环境变量让 30,000 通过了 API Server 校验，数据源还有第二层截断：`get_all_task_info` 默认只从 GCS 拉取 `RAY_MAX_LIMIT_FROM_DATA_SOURCE`(默认 10,000) 条。所以还需要同步设置 `RAY_MAX_LIMIT_FROM_DATA_SOURCE=30000`，否则 GCS 只返回前 10,000 条，`islice(result, 30000)` 实际最多拿到 10,000 条。

#### 3.4.6 完整截断漏斗

| 层级 | 配置 | 默认值 | 作用 |
|------|------|--------|------|
| API Server 校验 | `RAY_MAX_LIMIT_FROM_API_SERVER` | 10,000 | `options_from_req` 中 `limit > 此值` 直接报错 |
| 数据源拉取 | `RAY_MAX_LIMIT_FROM_DATA_SOURCE` | 10,000 | `get_all_task_info` 发给 GCS 的 `GetTaskEventsRequest.limit` |
| GCS 存储上限 | `RAY_task_events_max_num_task_in_gcs` | 100,000 | GCS 存储中最多保留的 task attempts 数 |
| 最终截取 | `islice(result, option.limit)` | 用户指定 | 在 API server 对过滤后的结果做最终截取 |

要让 `limit=30000` 完整生效，需要同时设置：
```bash
RAY_MAX_LIMIT_FROM_API_SERVER=30000
RAY_MAX_LIMIT_FROM_DATA_SOURCE=30000
# 可选：如果任务数 >100k 还需要
RAY_task_events_max_num_task_in_gcs=300000
```

---

## 4. Progress Bar 与 Task Table 数据不一致问题

### 4.1 数据流架构

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                       Dashboard 数据获取流程                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Worker 节点                                                                  │
│     │                                                                        │
│     │ 定期上报 task events                                                    │
│     │ (RAY_task_events_report_interval_ms, 默认 1000ms)                      │
│     ▼                                                                        │
│  GCS (Global Control Store)                                                  │
│     │                                                                        │
│     │ task_events_max_num_task_in_gcs (默认 100,000)                         │
│     │ ← 超过限制的任务事件会被丢弃以节省内存                                      │
│     ▼                                                                        │
│  State API Server                                                            │
│     │                                                                        │
│     ├─→ /api/v0/tasks/summarize (Progress Bar 使用)                          │
│     │      │                                                                 │
│     │      │ RAY_MAX_LIMIT_FROM_DATA_SOURCE (默认 10,000)                    │
│     │      │ ← 从数据源最多拉取 10k 条                                         │
│     │      ▼                                                                 │
│     │   num_after_truncation                                                 │
│     │      │                                                                 │
│     │      │ 应用 filters (如 job_id)                                        │
│     │      ▼                                                                 │
│     │   num_filtered ← Progress Bar 的 "total"                              │
│     │      │                                                                 │
│     │      │ 聚合 state_counts                                               │
│     │      ▼                                                                 │
│     │   Progress Bar segments (各状态计数)                                    │
│     │                                                                        │
│     └─→ /api/v0/tasks (Task Table 使用)                                      │
│            │                                                                 │
│            │ 实时查询任务状态                                                   │
│            ▼                                                                 │
│         Task Table 列表                                                       │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 不一致的原因

| 原因 | 说明 | 表现 |
|------|------|------|
| **不同数据源** | Progress Bar 使用 summarize API 的聚合数据，Task Table 使用 list API 的实时数据 | 数量可能不同 |
| **聚合 vs 实时** | Progress Bar 基于已记录的 task events 聚合，Task Table 显示实时快照 | 状态分布不同 |
| **GCS 事件丢弃** | 任务数超过 `task_events_max_num_task_in_gcs` 时丢弃旧事件 | Unaccounted 增加 |
| **数据源截断** | 任务数超过 `RAY_MAX_LIMIT_FROM_DATA_SOURCE` 时截断 | 部分任务不可见 |
| **时序问题** | 任务状态快速变化，不同 API 获取时机不同 | 状态统计不一致 |

### 4.3 常见不一致场景

**场景 1**: Progress Bar 显示 Waiting for scheduling: 100，Task Table 显示 submitted_to_worker: 500

- **原因**：Task Table 使用实时查询，Progress Bar 使用聚合数据
- **说明**：聚合数据可能未包含最新的状态变化

**场景 2**: Progress Bar 显示大量 Unaccounted，Task Table 显示正常

- **原因**：GCS 丢弃了旧的 task events
- **说明**：这些任务仍在运行，只是状态事件被丢弃了

---

## 5. Unaccounted 状态详解

### 5.1 Unaccounted 的计算方式

```typescript
// ProgressBar.tsx:85-102
const segmentTotal = progress.reduce((acc, { value }) => acc + value, 0);
const finalTotal = total ?? segmentTotal;

const segments =
  segmentTotal < finalTotal
    ? [
        ...progress,
        {
          value: finalTotal - segmentTotal,  // Unaccounted
          label: "Unaccounted",
          hint: "Unaccounted tasks can happen when there are too many tasks. " +
                "Ray drops older tasks to conserve memory.",
        },
      ]
    : progress;
```

### 5.2 两条计算路径

当前代码（`useJobProgress.ts`）有**两条路径**计算 progress 和 total：

```typescript
// 路径 A: 优先使用 total_state_counts（来自 GCS 的全量状态统计）
const progressFromTotalStateCounts = data?.totalStateCounts
    ? formatStateCountsToProgress(data.totalStateCounts)
    : null;

const totalFromStateCounts = data?.totalStateCounts
    ? Object.values(data.totalStateCounts).reduce((acc, count) => acc + count, 0)
    : undefined;

// 路径 B: fallback 到 summary 聚合 + num_filtered
return {
    progress: progressFromTotalStateCounts ?? summed,       // ★ 优先 A，退化 B
    totalTasks: totalFromStateCounts ?? data?.totalTasks,    // ★ 优先 A，退化 B
};
```

代码注释明确说明：
```typescript
// Prefer total_state_counts for progress segments when available.
// total_state_counts includes ALL entries in the GCS buffer (including
// zombie entries without task_info), providing accurate state distribution.
// The summary-based `summed` only counts entries with task_info, which
// can be much lower due to GCS buffer eviction in high-throughput scenarios.
```

#### 路径 A：`total_state_counts` 可用（当前主流场景）

GCS 在 `HandleGetTaskEvents`（`gcs_task_manager.cc:593-598`）中遍历**存储中的所有** task events 统计状态：

```cpp
for (auto &task_event : *task_events | boost::adaptors::reversed) {
    if (task_event.has_state_updates()) {
        auto latest_state = GetLatestTaskStatus(task_event);
        total_state_counts[ray::rpc::TaskStatus_Name(latest_state)]++;
    }
    // ...
}
```

- `progress` = `formatStateCountsToProgress(total_state_counts)` — 每个状态都映射到某个 segment，未知状态 fallback 到 `numUnknown`
- `totalTasks` = `sum(total_state_counts values)` — 同一份数据求和
- `segmentTotal` = `sum(progress segments)` = `sum(total_state_counts)` = `totalTasks`
- **Unaccounted = totalTasks - segmentTotal = 0**

**结论：当 `total_state_counts` 可用时，Unaccounted = 0，不会产生。**

#### 路径 B：`total_state_counts` 不可用（退化场景）

`total_state_counts` 不可用的条件：GCS 返回的 `total_state_counts` protobuf map 为空 → Python 侧 `dict(reply.total_state_counts)` 得到 `{}` → `if reply.total_state_counts` 为 False → 前端 `data?.totalStateCounts` 为 null。

此时：
- `progress` = `summed`（从 summarize 的 per-func-name 聚合结果累加）
- `totalTasks` = `data?.totalTasks` = `num_filtered`（来自 `list_tasks`）
- **Unaccounted = `num_filtered` - `sum(summed)`**

### 5.3 产生 Unaccounted 的条件

#### 路径 A 不产生 Unaccounted 的原因

`total_state_counts` 统计的是 GCS 存储中**全部**有 `state_updates` 的条目（包括没有 `task_info` 的"僵尸"条目），不受 limit 截断、不受 filter_fn 影响。`progress` 和 `totalTasks` 都来自同一份数据，两者一致。

即使有未知状态不在 `TASK_STATE_NAME_TO_PROGRESS_KEY` 映射中，`formatStateCountsToProgress` 也会 fallback 到 `numUnknown`，仍然计入 segmentTotal。所以 **Unaccounted = 0**。

#### 路径 B 产生 Unaccounted 的条件

| 条件 | 原因 | 具体机制 |
|------|------|---------|
| **islice 截断（两 limit 不一致时）** | `RAY_MAX_LIMIT_FROM_DATA_SOURCE` > `RAY_MAX_LIMIT_FROM_API_SERVER` | GCS 返回最多 `RAY_MAX_LIMIT_FROM_DATA_SOURCE` 条，`num_filtered` 在 islice 前计算，但 `to_summary_by_func_name` 遍历的是 islice 后的 `result.result`（limit=`RAY_MAX_LIMIT_FROM_API_SERVER`），两者数量不一致 → `summed` < `num_filtered` → 差值产生 |
| **do_filter 二次过滤（不产生 Unaccounted）** | GCS filter_fn 和 API server do_filter 不完全一致 | do_filter 只会减少条目（不会增加），`num_filtered` = do_filter 后的条数。由于 `num_filtered` 在 islice 前计算 = `to_summary_by_func_name` 遍历的上界，do_filter 减少条目不会导致 `num_filtered > summed` |
| **无 func_name 的 task（不产生 Unaccounted）** | `summarize_tasks` 按 func_name 分组 | 即使 `func_or_class_name` 为 None/空，也会被分到 `summary[None]` 组，仍计入 `state_counts` 和 `total_tasks`。不会遗漏 |

**关键结论**：在默认配置下（`RAY_MAX_LIMIT_FROM_DATA_SOURCE=10000`，`RAY_MAX_LIMIT_FROM_API_SERVER=10000`），GCS 最多返回 10000 条，Dashboard islice limit 也是 10000，`num_filtered` = `len(result.result)` = `summed`，**Unaccounted 恒为 0**。只有当 `RAY_MAX_LIMIT_FROM_DATA_SOURCE` > `RAY_MAX_LIMIT_FROM_API_SERVER`（即 GCS 端允许返回更多条目但 Dashboard 端 islice 截断）时，才可能产生 Unaccounted > 0。详见 §11.26。

#### GCS 淘汰的 task attempts 不产生 Unaccounted

被 GCS 淘汰的 task attempts 从 GCS 存储中物理删除后：
- 不在 `total_state_counts` 中（路径 A）
- 不在 `events_by_task` 中（路径 B 的 `num_filtered`）
- 不在 `summed` 中（路径 B）
- 它们只体现在 `num_status_task_events_dropped` 计数器中

前端 `useJobProgress.ts` 中 `totalTasks` = `num_filtered`（不包含 `num_status_task_events_dropped`），所以被淘汰的 task attempts 不参与 Unaccounted 计算。

### 5.4 Unaccounted 的影响

| 影响 | 说明 |
|------|------|
| **仅影响显示** | Unaccounted 只影响 Dashboard 的显示，不影响实际任务执行 |
| **正常现象** | 在大规模任务场景下，这是正常的内存保护机制 |
| **不影响调度** | Ray Core 的调度器不依赖 Dashboard 的状态显示 |

### 5.5 减少 Unaccounted 的方法

```bash
# 方法 1: 增加 GCS 中保留的 task events 数量
export RAY_task_events_max_num_task_in_gcs=500000  # 默认 100,000

# 方法 2: 增加 State API 返回的数据量
export RAY_MAX_LIMIT_FROM_DATA_SOURCE=100000  # 默认 10,000
export RAY_MAX_LIMIT_FROM_API_SERVER=100000   # 默认 10,000

# 方法 3: 加快 task events 上报频率
export RAY_task_events_report_interval_ms=500  # 默认 1000
```

**注意**：增加这些限制会增加内存使用，需要权衡。

### 5.6 完整产生条件图

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      Unaccounted 产生条件                                │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  前提: total_state_counts 不可用 (空或 null)                              │
│  ─────────────────────────────────────────────────                          │
│                                                                         │
│  Unaccounted = num_filtered - sum(summed)                               │
│                                                                         │
│  num_filtered 来源:                                                      │
│    GCS GetTaskEvents(limit=10k)                                         │
│    → events_by_task (最多 10k 条)                                       │
│    → protobuf_to_task_state_dict 转换                                    │
│    → do_filter (二次过滤: exclude_driver, filters)                      │
│    → num_filtered = len(filtered result)  ★ 截断前                      │
│    → islice(limit) → result (最多 limit 条)                             │
│                                                                         │
│  summed 来源:                                                            │
│    summarize_tasks → list_tasks(limit=10k)                               │
│    → 同上流程得到 result (最多 10k 条)                                    │
│    → TaskSummaries.to_summary_by_func_name(result)                      │
│    → 按 func_name 分组，每组统计 state_counts                            │
│    → formatStateCountsToProgress → TaskProgress                         │
│    → summed = sum across all groups                                     │
│                                                                         │
│  差值来源:                                                               │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │ 1. islice 截断:                                                │    │
│  │    do_filter 后 15k > islice limit 10k                         │    │
│  │    summary 只聚合 10k, num_filtered=15k                        │    │
│  │    → Unaccounted = 15k - 10k = 5k                              │    │
│  │                                                                │    │
│  │ 2. GCS 数据源截断:                                              │    │
│  │    GCS 存储 50k, GetTaskEvents limit=10k 只返回 10k             │    │
│  │    但 40k 既不在 summary 也不在 num_filtered                    │    │
│  │    → 不产生 Unaccounted (两者都不统计它们)                      │    │
│  │                                                                │    │
│  │ 3. GCS 淘汰的 task attempts:                                    │    │
│  │    从存储中物理删除, 不在 total_state_counts / num_filtered     │    │
│  │    → 不产生 Unaccounted                                        │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                                                         │
│  注意: 当前版本优先用 total_state_counts 路径, 此场景 Unaccounted=0     │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 6. Actor Task 特殊状态分析

### 6.1 Actor Task 预取机制

Ray Data 使用 Actor Pool 执行任务时，会预先分发任务到 Actor：

```python
# max_tasks_in_flight_per_actor 参数控制预取数量
# 默认值: 2 (可通过 DataContext 配置)

# 例如: 有 100 个 Actors，max_tasks_in_flight = 2
# 可有 200 个任务处于 SUBMITTED_TO_WORKER 状态
# 但实际 RUNNING 的只有 100 个
```

### 6.2 状态分布示例

```
场景: 600 个 Actors，max_tasks_in_flight_per_actor = 2

实际分布:
- Running: 313 (部分 Actor GPU 算力 < 1，实际运行数 < Actor 数)
- PENDING_ACTOR_TASK_ARGS_FETCH: 287 (等待参数)
- PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY: 600 (在队列中等待)

Dashboard 显示:
- Running: 313
- Waiting for scheduling: 887 (287 + 600 合并显示)
```

### 6.3 理解任务数差异

```
┌─────────────────────────────────────────────────────────────────────────────┐
│              Ray Data Tasks vs Ray Core Tasks                                │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Ray Data 日志:                                                              │
│    "Running: 313/550"     ← Ray Data 层面的活跃任务数                         │
│    "Tasks: 1200"          ← Ray Data 统计的总任务数                           │
│                                                                              │
│  Ray Dashboard:                                                              │
│    "Running: 313"         ← Ray Core 层面实际运行的 Actor Tasks               │
│    "Submitted: 600"       ← 已提交到 Worker 的 Actor Tasks (含预取)           │
│    "Actors: 600"          ← Actor 总数                                        │
│                                                                              │
│  关系说明:                                                                    │
│  - Ray Data Tasks 可能映射到多个 Ray Core Actor Tasks                        │
│  - 预取机制导致 Submitted > Running                                          │
│  - Actor 数量决定了 Running 的上限                                            │
│  - GPU 资源碎片化可能导致 Running < Actors                                    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 7. Ray Data 任务与 Ray Core 任务的关系

### 7.1 任务层次结构

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Ray Data 与 Ray Core 任务关系                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Ray Data Layer                                                              │
│  ─────────────────────────────────────────────────────────────────────────── │
│                                                                              │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                      │
│  │ DataOpTask  │    │ DataOpTask  │    │ DataOpTask  │   (Ray Data 任务)    │
│  │  Block 1    │    │  Block 2    │    │  Block 3    │                      │
│  └──────┬──────┘    └──────┬──────┘    └──────┬──────┘                      │
│         │                  │                  │                              │
│         │   调度到 Actor Pool                  │                       │
│         ▼                  ▼                  ▼                              │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │                        Actor Pool                                       │ │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐       │ │
│  │  │ Actor 1 │  │ Actor 2 │  │ Actor 3 │  │ Actor 4 │  │ Actor 5 │       │ │
│  │  │ (GPU)   │  │ (GPU)   │  │ (GPU)   │  │ (GPU)   │  │ (GPU)   │       │ │
│  │  └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘       │ │
│  │       │            │            │            │            │             │ │
│  └───────┼────────────┼────────────┼────────────┼────────────┼─────────────┘ │
│          │            │            │            │            │               │
│  Ray Core Layer                                                              │
│  ─────────────────────────────────────────────────────────────────────────── │
│          ▼            ▼            ▼            ▼            ▼               │
│  ┌───────────┐ ┌───────────┐ ┌───────────┐ ┌───────────┐ ┌───────────┐      │
│  │Actor Task │ │Actor Task │ │Actor Task │ │Actor Task │ │Actor Task │      │
│  │ (RUNNING) │ │ (PENDING) │ │ (RUNNING) │ │ (PENDING) │ │ (RUNNING) │      │
│  └───────────┘ └───────────┘ └───────────┘ └───────────┘ └───────────┘      │
│                                                                              │
│  Dashboard 显示的是 Ray Core 层面的 Actor Tasks                               │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 7.2 数量关系公式

```python
# Ray Data 层面
ray_data_active_tasks = 运行中 + 待调度的 DataOpTask 数量

# Ray Core 层面 (Dashboard 显示)
ray_core_running = 实际在 Actor 上执行的任务数
ray_core_submitted = running + 在 Actor 队列中等待的任务数
                   = running + (pending_args_fetch + pending_ordering)

# 关系
ray_core_submitted ≈ min(ray_data_active_tasks, actors * max_tasks_in_flight)
ray_core_running ≤ actors  # 受 Actor 数量和资源限制
```

---

## 8. 配置调优建议

### 8.1 Dashboard 显示相关配置

| 配置项 | 默认值 | 说明 | 调优建议 |
|--------|--------|------|---------| 
| `RAY_task_events_max_num_task_in_gcs` | 100,000 | GCS 中保留的最大 task events 数 | 大规模作业可增加到 500,000 |
| `RAY_MAX_LIMIT_FROM_DATA_SOURCE` | 10,000 | State API 从数据源获取的最大条目数 | 需要完整数据时可增加 |
| `RAY_MAX_LIMIT_FROM_API_SERVER` | 10,000 | State API 返回给前端的最大条目数 | 与上述配置同步调整 |
| `RAY_task_events_report_interval_ms` | 1,000 | Worker 上报 task events 的间隔 | 需要实时性可减小到 500 |

### 8.2 Ray Data 性能相关配置

| 配置项 | 默认值 | 说明 | 调优建议 |
|--------|--------|------|---------|
| `max_tasks_in_flight_per_actor` | 2 | 每个 Actor 预取的任务数 | 减少可降低内存占用 |
| `target_max_block_size` | 128MB | 目标 block 大小 | 太小会增加调度开销 |

### 8.3 配置示例

```bash
# 大规模任务场景 (>100k tasks)
export RAY_task_events_max_num_task_in_gcs=500000
export RAY_task_events_report_interval_ms=500

# 需要精确监控时
export RAY_MAX_LIMIT_FROM_DATA_SOURCE=100000
export RAY_MAX_LIMIT_FROM_API_SERVER=100000

# 减少内存占用时 (接受部分监控数据丢失)
export RAY_task_events_max_num_task_in_gcs=50000
```

---

## 10. Task Event Buffer 完整数据流

本节从代码层面完整描述 Task Event 从产生、缓冲、上报到 GCS 合并的全过程。

### 10.1 状态产生的源头：RecordTaskStatusEventIfNeeded

所有通过 GCS Task Event 上报的状态转换都经过 `TaskEventBufferImpl::RecordTaskStatusEventIfNeeded`（`task_event_buffer.cc:420`），它有两层过滤：

```cpp
bool TaskEventBufferImpl::RecordTaskStatusEventIfNeeded(
    const TaskID &task_id, const JobID &job_id, int32_t attempt_number,
    const TaskSpecification &spec, rpc::TaskStatus status,
    bool include_task_info, ...) {
  // 过滤条件 1: 全局开关
  // RAY_task_events_report_interval_ms=0 时整个 task event 系统关闭
  if (!Enabled()) return false;

  // 过滤条件 2: 单 task 级别开关
  // 用户可通过 ray.remote(enable_task_events=False) 禁用
  // 默认 kDefaultTaskEventEnabled = true
  // Actor Task 从 ActorHandle 继承此设置
  if (!spec.EnableTaskEvents()) return false;

  // 通过检查 → 构造 TaskStatusEvent → AddTaskEvent
  auto task_event = std::make_unique<TaskStatusEvent>(...);
  AddTaskEvent(std::move(task_event));
  return true;
}
```

普通 Task 的状态转换通过 `TaskManager::SetTaskStatus`（`task_manager.cc:1702`）统一入口调用此方法。Actor Task 的专用状态（`PENDING_ACTOR_TASK_ARGS_FETCH`、`PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY`）由 Actor Task Execution Queue 直接调用 `task_event_buffer_.RecordTaskStatusEventIfNeeded`。

**状态转换触发点汇总：**

| 状态转换 | 触发代码位置 | 触发条件 |
|----------|-------------|---------|
| → `PENDING_ARGS_AVAIL` | `task_manager.cc:348` | `AddTaskAndWaitForArguments`：用户提交任务 |
| `PENDING_ARGS_AVAIL` → `PENDING_NODE_ASSIGNMENT` | `task_manager.cc:1682` | `MarkDependenciesResolved`：所有参数依赖已解决 |
| `PENDING_NODE_ASSIGNMENT` → `SUBMITTED_TO_WORKER` | `task_manager.cc:1697` | `MarkTaskWaitingForExecution`：raylet 已分配节点和 worker |
| → `RUNNING` | `core_worker.cc:3067` | `ExecuteTask`：worker 开始实际执行 task 函数体 |
| → `FINISHED` | `task_manager.cc:1053` | `CompletePendingTask`：task 执行完成且无应用错误 |
| → `FAILED` | `task_manager.cc:1049` | `CompletePendingTask`：task 执行产生应用错误 |
| → `FAILED` (系统错误) | `task_manager.cc:1190` | `FailTask`：worker 死亡、OOM 等 |
| `FAILED` → `PENDING_ARGS_AVAIL` (retry) | `task_manager.cc:1229` | `FailTask` 中 `will_retry=true`：先记录旧 attempt 为 FAILED，用 `attempt_number+1` 记录新 attempt |
| → `PENDING_ACTOR_TASK_ARGS_FETCH` | `ordered/unordered_actor_task_execution_queue.cc` | Actor task 有 ObjectRef 依赖 |
| → `PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY` | 同上 | Actor task 依赖解决但等并发/顺序 |

**注意：`RUNNING_IN_RAY_GET`、`RUNNING_IN_RAY_WAIT`、`GETTING_AND_PINNING_ARGS` 不产生 TaskStatusEvent，不上报到 GCS。** 它们通过 `ScopedTaskMetricSetter`（`core_worker.cc:58-87`）只影响 metrics gauge，在构造时 SetMetricStatus、析构时 UnsetMetricStatus，是瞬态子状态。

### 10.2 AddTaskEvent 分流

```cpp
// task_event_buffer.cc:1000
void TaskEventBufferImpl::AddTaskEvent(std::unique_ptr<TaskEvent> task_event) {
  if (task_event->IsProfileEvent()) {
    AddTaskProfileEvent(std::move(task_event));
  } else {
    AddTaskStatusEvent(std::move(task_event));
  }
}
```

Task Event 分两类，走完全不同的缓冲和淘汰机制：
- **StatusEvent**：状态变更事件（`PENDING_ARGS_AVAIL` → `RUNNING` → `FINISHED` 等）
- **ProfileEvent**：性能事件（函数执行时间、ray.get 等带时间戳的性能数据）

### 10.3 AddTaskStatusEvent 详解

```cpp
// task_event_buffer.cc:1009
void TaskEventBufferImpl::AddTaskStatusEvent(
    std::unique_ptr<TaskEvent> status_event) {
  absl::MutexLock lock(&mutex_);          // 全局互斥锁
  if (!enabled_) return;

  std::shared_ptr<TaskEvent> status_event_shared_ptr = std::move(status_event);

  // ===== 3.1 Export API 副本 (独立于 GCS 路径) =====
  if (export_event_write_enabled_) {
    // status_events_for_export_ 是一个独立的 circular buffer
    // 容量: RAY_task_events_max_num_export_status_events_buffer_on_worker
    // 这里的数据用于 Export API (旧版数据导出路径)
    // 满时自动 FIFO 淘汰, 但不会触发 dropped_task_attempts 逻辑!
    status_events_for_export_.push_back(status_event_shared_ptr);
  }

  // ===== 3.2 已丢弃 task attempt 拦截 =====
  if (dropped_task_attempts_unreported_.count(
          status_event_shared_ptr->GetTaskAttempt()) != 0u) {
    // 这个 task attempt 之前的事件已被淘汰
    // 后续所有事件一律丢弃
    stats_counter_.Increment(kNumTaskStatusEventDroppedSinceLastFlush);
    return;   // 直接返回, 不进入 status_events_
  }

  // ===== 3.3 GCS 路径: status_events_ circular buffer =====
  if (status_events_.full()) {
    // 缓冲区已满 (容量: RAY_task_events_max_num_status_events_buffer_on_worker, 默认 100,000)

    // 3.3.1 淘汰最旧的事件
    const auto &to_evict = status_events_.front();

    // 3.3.2 将被淘汰的 task attempt 加入"待上报丢弃"集合
    auto inserted = dropped_task_attempts_unreported_.insert(
        to_evict->GetTaskAttempt());

    stats_counter_.Increment(kNumTaskStatusEventDroppedSinceLastFlush);

    // 3.3.3 如果是首次插入该 task attempt
    if (inserted.second) {
      stats_counter_.Increment(kNumDroppedTaskAttemptsStored);
    }
  } else {
    stats_counter_.Increment(kNumTaskStatusEventsStored);
  }

  // 3.3.4 推入新事件
  // boost::circular_buffer::push_back:
  //   - 未满: 直接追加到尾部
  //   - 已满: 先弹出 front(), 再追加到尾部
  status_events_.push_back(status_event_shared_ptr);
}
```

**`status_events_` 的关键设计：**
- 数据结构：`boost::circular_buffer<std::shared_ptr<TaskEvent>>`，容量 100,000（默认）
- 淘汰策略：FIFO——`push_back` 时如果满，自动弹出 `front()`（最旧的事件）
- **淘汰粒度是"事件级别"而非"task attempt 级别"**：同一个 task attempt 的多次状态变更（如 `PENDING_ARGS_AVAIL` → `RUNNING` → `FINISHED`）是三条独立的 event，可能只有最旧的那条被淘汰
- **淘汰后会标记整个 task attempt 为 dropped**：一旦某个 task attempt 的任意一条 event 被淘汰，整个 task attempt 进入 `dropped_task_attempts_unreported_`，后续该 attempt 的所有事件都会在 3.2 被直接 return

**双缓冲设计对比：**

| 维度 | `status_events_` (GCS 路径) | `status_events_for_export_` (Export API) |
|------|-------------------------------|------------------------------------------|
| 数据结构 | `circular_buffer` (扁平) | `circular_buffer` (独立) |
| 容量 | `RAY_task_events_max_num_status_events_buffer_on_worker` (默认 100k) | `RAY_task_events_max_num_export_status_events_buffer_on_worker` |
| 淘汰方式 | FIFO 淘汰最旧 | FIFO 自动淘汰 |
| 淘汰后果 | 标记 task attempt 为 dropped，后续事件永久丢弃 | **不标记 dropped**，仅自动淘汰 |
| 用途 | flush 时上报 GCS | 旧版 Export API 数据导出 |

### 10.4 AddTaskProfileEvent 详解

```cpp
// task_event_buffer.cc:1057
void TaskEventBufferImpl::AddTaskProfileEvent(
    std::unique_ptr<TaskEvent> profile_event) {
  absl::MutexLock lock(&profile_mutex_);    // 不同于 status_events_ 的锁
  if (!enabled_) return;

  // 按 task attempt 分组存储
  auto profile_events_itr =
      profile_events_.find(profile_event_shared_ptr->GetTaskAttempt());
  if (profile_events_itr == profile_events_.end()) {
    // 新 task attempt → 创建 vector
    profile_events_.insert({profile_event_shared_ptr->GetTaskAttempt(),
                            std::vector<std::shared_ptr<TaskEvent>>()});
  }

  // ===== 限制检查 =====
  auto max_num_profile_event_per_task =
      RayConfig::instance().task_events_max_num_profile_events_per_task();  // 默认 1,000
  auto max_profile_events_stored =
      RayConfig::instance().task_events_max_num_profile_events_buffer_on_worker(); // 默认 10,000

  // 如果: 单 task 的 profile events 超过 1,000 或 全局总数超过 10,000
  if ((per_task >= max_per_task) || (global >= max_global)) {
    // ★ 丢弃新事件 (不是淘汰旧事件!)
    stats_counter_.Increment(kNumTaskProfileEventDroppedSinceLastFlush);
    return;   // 直接返回, 不存储
  }

  // 存入
  profile_events_itr->second.push_back(profile_event_shared_ptr);
}
```

**Profile Event 的淘汰策略与 Status Event 完全不同：**

| 维度 | StatusEvent (`status_events_`) | ProfileEvent (`profile_events_`) |
|------|-------------------------------|----------------------------------|
| 数据结构 | `circular_buffer` (扁平) | `flat_hash_map<TaskAttempt, vector>` (按 task 分组) |
| 容量限制 | 全局 100,000 条 | 全局 10,000 条 + 单 task 1,000 条 |
| 淘汰方式 | FIFO 淘汰最旧 | **丢弃新事件** (drop new) |
| 淘汰后果 | 标记 task attempt 为 dropped，后续事件永久丢弃 | **不标记 dropped**，仅计数 |
| 互斥锁 | `mutex_` | `profile_mutex_` (独立锁) |

Profile Event 溢出时丢弃**新事件**而非旧事件，设计理由：profile event 是性能采样数据，旧数据更有参考价值（包含了完整的执行时间段）；丢弃新数据不会导致状态不一致。

### 10.5 Flush 流程

每 `RAY_task_events_report_interval_ms`（默认 1000ms）触发一次 `FlushEvents`：

```
FlushEvents (task_event_buffer.cc:908):
  1. 检查 enabled_ / stopping_
  2. 检查背压: if gcs_grpc_in_progress_ > 0 && !forced → skip
  3. GetTaskStatusEventsToSend:
     a. 从 status_events_ circular buffer 取出最多 batch_size 条
        → status_events_.erase(begin, begin+num_to_send)  ★ 清空已取出的
     b. 从 dropped_task_attempts_unreported_ 取出最多 batch_size 条
        → dropped_task_attempts_unreported_.erase(itr)     ★ 清空已取出的
  4. GetTaskProfileEventsToSend:
     → 同理从 profile_events_ map 中取出并 erase
  5. CreateDataToSend → 聚合为 TaskEventData
  6. ResetCountersForFlush → 重置 since_last_flush 计数器
  7. SendTaskEventsToGCS(data)  → gRPC 发送
```

#### dropped_task_attempts_unreported_ 的生命周期

```
Worker circular buffer 满
  → AddTaskStatusEvent:
    dropped_task_attempts_unreported_.insert(evicted_task_attempt)
    // 后续该 task attempt 的事件直接 return (拦截)

下次 flush:
  → GetTaskStatusEventsToSend (line 620-630):
    for each in dropped_task_attempts_unreported_:
      moved to dropped_task_attempts_to_send
      erased from dropped_task_attempts_unreported_  ★ 清理

GCS 收到后:
  → RecordDataLossFromWorker:
    job_task_summary_.RecordTaskAttemptDropped(task_attempt)  // GCS 接管
    if exists in storage: RemoveTaskAttempt(loc)  // GCS 删除已有数据

flush 完成后:
  → dropped_task_attempts_unreported_ 已清空已上报的部分
  → 该 task attempt 的新事件不再被 Worker 拦截
  → 但 GCS 的 dropped_task_attempts_ 仍包含该 task attempt
  → GCS 的 ShouldDropTaskAttempt 会永久拦截
```

**`dropped_task_attempts_unreported_` 是一个"待上报到 GCS"的过渡集合。** 一旦 GCS 确认接收，丢弃职责就从 Worker 转移到 GCS。

**注意：** flush 受 `task_events_dropped_task_attempt_batch_size` 限制，一次 flush 只发送一定数量的 dropped attempts。如果集合中有 5000 个但 batch_size 是 1000，这次只清理 1000 个，剩余 4000 个下次再发。

**flush 失败（gRPC error）时：** 已取出的数据**不会回滚**到 buffer 中。status events 和 dropped task attempts 都已被 erase，不会重发。这是设计上的 tradeoff——宁可丢失部分 events 也不让 buffer 回滚导致复杂的状态管理。

#### ToRpcTaskEvents 的 state_ts_ns map 累积机制

```cpp
// task_event_buffer.cc:79
void TaskStatusEvent::ToRpcTaskEvents(rpc::TaskEvents *rpc_task_events) {
  // Base fields (每次都 set, 覆盖)
  rpc_task_events->set_task_id(task_id_.Binary());
  rpc_task_events->set_job_id(job_id_.Binary());
  rpc_task_events->set_attempt_number(attempt_number_);

  // Task info (如果有, 覆盖)
  if (task_spec_) {
    gcs::FillTaskInfo(rpc_task_events->mutable_task_info(), *task_spec_);
  }

  // ★ 状态更新: 向 state_ts_ns map 中插入/更新
  auto dst_state_update = rpc_task_events->mutable_state_updates();
  gcs::FillTaskStatusUpdateTime(task_status_, timestamp_, dst_state_update);
  // → (*state_updates->mutable_state_ts_ns())[task_status] = timestamp
  // 如果同一 task attempt 有多条 status event (如 PENDING_ARGS_AVAIL + RUNNING)
  // → state_ts_ns 会有多个 entry: {1: ts1, 6: ts2}
  // → 这就是 "合并" 的核心: 状态被累积到同一个 map 中

  // 附加信息 (node_id, worker_id, error_info 等, 按状态附加)
  if (state_update_->node_id_.has_value()) {
    // 仅 SUBMITTED_TO_WORKER 时有 node_id/worker_id
    dst_state_update->set_node_id(state_update_->node_id_->Binary());
  }
  if (state_update_->error_info_.has_value()) {
    // 仅 FAILED 时有 error_info
    *(dst_state_update->mutable_error_info()) = *state_update_->error_info_;
  }
}
```

#### CreateDataToSend 中按 task attempt 聚合 + dropped 过滤

```cpp
// task_event_buffer.cc:729
TaskEventDataToSend TaskEventBufferImpl::CreateDataToSend(...) {
  // 按 TaskAttempt 聚合
  absl::flat_hash_map<TaskAttempt, rpc::TaskEvents> agg_task_events;

  auto to_rpc_event_fn = [...] (const std::shared_ptr<TaskEvent> &event) {
    // ★ 如果该 task attempt 在本批 dropped_task_attempts_to_send 中
    //   (即本次 flush 要上报其数据丢失)
    //   → 跳过该事件，不上报到 GCS
    if (dropped_task_attempts_to_send.contains(event->GetTaskAttempt())) {
      return;  // skip
    }

    // 按 task attempt 聚合: try_emplace 保证同 task attempt 的事件合并到同一个 rpc::TaskEvents
    auto [itr, _] = agg_task_events.try_emplace(event->GetTaskAttempt());
    event->ToRpcTaskEvents(&(itr->second));
  };

  // 遍历所有 status events 和 profile events
  std::for_each(status_events_to_send.begin(), status_events_to_send.end(), to_rpc_event_fn);
  std::for_each(profile_events_to_send.begin(), profile_events_to_send.end(), to_rpc_event_fn);
  // ...
}
```

**关键细节：** 如果一个 task attempt 被标记为 dropped（在 `dropped_task_attempts_to_send` 中），那么它在 buffer 中还未上报的剩余 events 也会被跳过——因为与 dropped 通知一起发送时会被 `to_rpc_event_fn` 过滤掉。GCS 收到 dropped 通知后也会主动删除该 task attempt 的已有存储。这保证了状态一致性——不会有部分状态残留在 GCS 中。

### 10.6 GCS 侧接收与合并

```cpp
// gcs_task_manager.cc:665
void GcsTaskManager::RecordTaskEventData(rpc::AddTaskEventDataRequest &request) {
  auto data = std::move(*request.mutable_data());
  // 1. 先处理数据丢失上报
  task_event_storage_->RecordDataLossFromWorker(data);

  // 2. 再处理每个 task 的事件
  for (auto &events_by_task : *data.mutable_events_by_task()) {
    stats_counter_.Increment(kTotalNumTaskEventsReported);
    task_event_storage_->AddOrReplaceTaskEvent(std::move(events_by_task));
  }
}
```

#### RecordDataLossFromWorker

```cpp
// gcs_task_manager.cc:639
void GcsTaskManager::GcsTaskManagerStorage::RecordDataLossFromWorker(
    const rpc::TaskEventData &data) {
  for (const auto &dropped_attempt : data.dropped_task_attempts()) {
    auto task_id = TaskID::FromBinary(dropped_attempt.task_id());
    auto attempt_number = dropped_attempt.attempt_number();
    auto job_id = task_id.JobId();

    // ★ 标记为 dropped
    job_task_summary_[job_id].RecordTaskAttemptDropped(
        std::make_pair(task_id, attempt_number));
    stats_counter_.Increment(kTotalNumTaskAttemptsDropped);

    // ★ 如果 GCS 里还有这个 task attempt 的数据，直接删除！
    const auto &loc_iter = primary_index_.find(std::make_pair(task_id, attempt_number));
    if (loc_iter != primary_index_.end()) {
      RemoveTaskAttempt(loc_iter->second);
    }
  }
  // ... 处理 profile events dropped ...
}
```

#### AddOrReplaceTaskEvent

```cpp
// gcs_task_manager.cc:351
void GcsTaskManager::GcsTaskManagerStorage::AddOrReplaceTaskEvent(
    rpc::TaskEvents &&events_by_task) {
  // 1. 检查是否已被 dropped
  if (job_task_summary_[job_id].ShouldDropTaskAttempt(GetTaskAttempt(events_by_task))) {
    // 已 dropped → 直接丢弃，不存储、不合并、不更新任何状态
    return;
  }

  // 2. 获取或创建 locator
  std::shared_ptr<TaskEventLocator> loc =
      UpdateOrInitTaskEventLocator(std::move(events_by_task));

  // 3. 如果超过存储上限 → 淘汰
  if (max_num_task_events_ > 0 &&
      static_cast<size_t>(stats_counter_.Get(kNumTaskEventsStored)) > max_num_task_events_) {
    EvictTaskEvent();
  }
}
```

#### UpdateExistingTaskAttempt 中的 MergeFrom 和优先级迁移

```cpp
// gcs_task_manager.cc:167
void GcsTaskManager::GcsTaskManagerStorage::UpdateExistingTaskAttempt(
    const std::shared_ptr<TaskEventLocator> &loc,
    const rpc::TaskEvents &task_events) {
  auto &existing_task = loc->GetTaskEventsMutable();

  // protobuf MergeFrom 会合并 state_ts_ns map
  // 同一个状态的 timestamp 会被覆盖为最新值
  // 不同状态的 timestamp 会被保留
  existing_task.MergeFrom(task_events);

  // profile events 超过 per-task 限制时截断最旧的
  if (existing_task.profile_events().events_size() > max_num_profile_events_per_task) {
    auto to_drop = existing_task.profile_events().events_size() - max_num_profile_events_per_task;
    existing_task.mutable_profile_events()->mutable_events()->DeleteSubrange(0, to_drop);
  }

  // ★ 优先级迁移: 状态变化可能导致 GC 优先级变化
  auto target_list_index = gc_policy_->GetTaskListPriority(existing_task);
  auto cur_list_index = loc->GetCurrentListIndex();
  if (target_list_index != cur_list_index) {
    // 从旧 list 移到新 list 的 front
    task_events_list_[target_list_index].push_front(std::move(existing_task));
    task_events_list_[cur_list_index].erase(loc->GetCurrentListIterator());
    loc->SetCurrentList(target_list_index, task_events_list_[target_list_index].begin());
  }
}
```

典型场景：一个普通 task 从 `RUNNING`（Priority 2）变为 `FINISHED`（Priority 0），会从 Priority 2 list 移到 Priority 0 list 的 front。刚完成的 task 在 Priority 0 list 中是最新的，不会被立即淘汰。

### 10.7 完整数据流图

```
┌─────────────────────────────────────────────────────────────────────────┐
│                   TaskEventBuffer 完整数据流                              │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  状态变更触发点                                                          │
│  ─────────────                                                          │
│  TaskManager::SetTaskStatus (提交方进程)                                 │
│    → PENDING_ARGS_AVAIL, PENDING_NODE_ASSIGNMENT,                        │
│      SUBMITTED_TO_WORKER, FINISHED, FAILED                               │
│  CoreWorker::ExecuteTask (执行方进程)                                    │
│    → RUNNING                                                            │
│  Actor Task Execution Queue (执行方进程)                                 │
│    → PENDING_ACTOR_TASK_ARGS_FETCH,                                     │
│      PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY                          │
│  CoreWorker::Disconnect (Driver)                                        │
│    → FINISHED                                                           │
│                                                                         │
│  所有触发点 → RecordTaskStatusEventIfNeeded                              │
│    → 检查 1: Enabled() (全局开关)                                       │
│    → 检查 2: spec.EnableTaskEvents() (单 task 开关, 默认 true)           │
│    → 构造 TaskStatusEvent → AddTaskEvent                                 │
│                                                                         │
│  AddTaskEvent                                                            │
│    ├─ IsProfileEvent? → AddTaskProfileEvent                              │
│    └─ else → AddTaskStatusEvent                                         │
│                                                                         │
│  AddTaskStatusEvent                                                      │
│  ┌───────────────────────────────────────────────────────────┐          │
│  │ 1. Export API 副本 (如果启用)                              │          │
│  │    status_events_for_export_.push_back(event)              │          │
│  │    → circular buffer FIFO 自动淘汰, 无 dropped 标记         │          │
│  │                                                            │          │
│  │ 2. dropped_task_attempts_unreported_ 检查                   │          │
│  │    if task_attempt in dropped_set:                         │          │
│  │      → return (丢弃, 计数)                                  │          │
│  │                                                            │          │
│  │ 3. status_events_ circular buffer                         │          │
│  │    if full (≥100k):                                        │          │
│  │      to_evict = front()                                    │          │
│  │      dropped_task_attempts_unreported_.insert(              │          │
│  │          to_evict->GetTaskAttempt())                       │          │
│  │      → 该 task attempt 后续事件将被步骤 2 拦截              │          │
│  │    push_back(event)                                       │          │
│  └───────────────────────────────────────────────────────────┘          │
│                                                                         │
│  AddTaskProfileEvent                                                     │
│  ┌───────────────────────────────────────────────────────────┐          │
│  │ profile_events_[task_attempt].push_back(event)             │          │
│  │ if per-task ≥1,000 or global ≥10,000:                      │          │
│  │   → return (丢弃新事件, 不淘汰旧, 不标记 dropped)            │          │
│  └───────────────────────────────────────────────────────────┘          │
│                                                                         │
│  ─────── 每 1000ms (RAY_task_events_report_interval_ms) ───────         │
│  FlushEvents                                                             │
│  ┌───────────────────────────────────────────────────────────┐          │
│  │ 1. 背压检查: gcs_grpc_in_progress_ > 0 ? skip              │          │
│  │ 2. GetTaskStatusEventsToSend                               │          │
│  │    a. dropped_task_attempts_unreported_ → 取出并 erase      │          │
│  │    b. status_events_ → 取出前 batch_size 条, erase           │          │
│  │ 3. GetTaskProfileEventsToSend                              │          │
│  │ 4. CreateDataToSend: 按 task attempt 聚合                   │          │
│  │    if task_attempt in dropped_task_attempts_to_send:      │          │
│  │      → skip (不上报)                                       │          │
│  │    agg_task_events[task_attempt].ToRpcTaskEvents(...)      │          │
│  │    → state_ts_ns[status] = timestamp (累积到 map)            │          │
│  │ 5. ResetCountersForFlush                                   │          │
│  │ 6. SendTaskEventsToGCS → gRPC AddTaskEventData              │          │
│  └───────────────────────────────────────────────────────────┘          │
│                                                                         │
│  GCS 侧接收                                                              │
│  ┌───────────────────────────────────────────────────────────┐          │
│  │ RecordTaskEventData                                        │          │
│  │   1. RecordDataLossFromWorker(data)                        │          │
│  │      → for each dropped_task_attempt:                      │          │
│  │        job_summary.RecordTaskAttemptDropped(attempt)        │          │
│  │        if exists in storage: RemoveTaskAttempt(loc)         │          │
│  │                                                            │          │
│  │   2. for each events_by_task:                              │          │
│  │        AddOrReplaceTaskEvent(events_by_task)                │          │
│  │        → ShouldDropTaskAttempt? → return                   │          │
│  │        → UpdateOrInitTaskEventLocator:                      │          │
│  │          if exists: existing.MergeFrom(new)                 │          │
│  │            → state_ts_ns 合并 (protobuf map merge)          │          │
│  │            → GC 优先级可能变化 → 移动到新 list               │          │
│  │          if new: AddNewTaskEvent → push_front to list       │          │
│  │        → if > max (100k): EvictTaskEvent                   │          │
│  └───────────────────────────────────────────────────────────┘          │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 10.8 一个 task 完整生命周期的 event 序列示例

假设一个普通 task 从提交到完成：

```
时间线                Worker 侧 (提交方)        Worker 侧 (执行方)         GCS 存储最终状态
────────────────────────────────────────────────────────────────────────────────────
t0  用户提交 task
t1  SetTaskStatus →    AddTaskStatusEvent
    PENDING_ARGS_AVAIL  event{status=1, ts=t1, include_task_info=true}
                        → status_events_.push_back

t2  依赖解决
    SetTaskStatus →    AddTaskStatusEvent
    PENDING_NODE_       event{status=2, ts=t2}
    ASSIGNMENT           → status_events_.push_back

t3  Raylet 分配节点
    SetTaskStatus →    AddTaskStatusEvent
    SUBMITTED_TO_       event{status=5, ts=t3, node_id, worker_id}
    WORKER              → status_events_.push_back

─── flush (t3 + 1000ms) ───
    3 条 event 聚合为 1 个 rpc::TaskEvents:
      task_id, job_id, attempt_number
      task_info (来自 t1 的 include_task_info=true)
      state_ts_ns: {1: t1, 2: t2, 5: t3}     ← 3 个状态都在一个 map 中
      node_id, worker_id (来自 t3)

    GCS 收到 → AddOrReplaceTaskEvent → 新条目 → push_front to priority list

t4  执行方 Worker 开始执行
    RecordTaskStatus    AddTaskStatusEvent
    EventIfNeeded →     event{status=6 (RUNNING), ts=t4,
    RUNNING               include_task_info=取决于配置}
                         → status_events_.push_back

─── flush (t4 + 1000ms) ───
    1 条 event → 1 个 rpc::TaskEvents:
      state_ts_ns: {6: t4}

    GCS 收到 → AddOrReplaceTaskEvent → 已存在 → MergeFrom
      state_ts_ns: {1: t1, 2: t2, 5: t3, 6: t4}  ← 合并后 4 个状态
      GetLatestTaskStatus → 从高到低遍历 → RUNNING(6)

t5  task 完成
    SetTaskStatus →    AddTaskStatusEvent
    FINISHED            event{status=9, ts=t5}
                       → status_events_.push_back

─── flush (t5 + 1000ms) ───
    GCS 收到 → MergeFrom
      state_ts_ns: {1: t1, 2: t2, 5: t3, 6: t4, 9: t5}
      GetLatestTaskStatus → FINISHED(9)
      GC 优先级变化: Priority 2 → Priority 0 (finished tasks 先淘汰)
      → 移动到 priority 0 list 的 front
```

### 10.9 Circular Buffer 满时的淘汰场景

假设 Worker 有 100k 容量的 `status_events_`，当前有 100k 条事件。一个 task A 有 3 条 event（PENDING_ARGS_AVAIL, RUNNING, FINISHED），其中 PENDING_ARGS_AVAIL 在 buffer 最前面：

```
status_events_ (circular buffer, 已满):
  front → [A: PENDING_ARGS_AVAIL(t1)] [B: ...] [C: ...] ... [Z: RUNNING(t100k)] ← back

新 event 到达 (task W 的 RUNNING):
  → status_events_.full() == true
  → to_evict = front() = A: PENDING_ARGS_AVAIL(t1)
  → dropped_task_attempts_unreported_.insert(A.task_attempt)
  → status_events_.push_back(W: RUNNING)
    → circular_buffer 自动弹出 front (A: PENDING_ARGS_AVAIL)
    → 现在 buffer: [B: ...] [C: ...] ... [Z: RUNNING] [W: RUNNING]

此时 task A 在 buffer 中还剩 2 条 (RUNNING, FINISHED)
但 task A 已在 dropped_task_attempts_unreported_ 中

下次 task A 的新 event 到达:
  → AddTaskStatusEvent
  → dropped_task_attempts_unreported_.count(A) != 0
  → return (直接丢弃)

下次 flush:
  → A 的 RUNNING 和 FINISHED 仍在 status_events_ 中
  → 但 A.task_attempt 在 dropped_task_attempts_to_send 中
  → CreateDataToSend 中 to_rpc_event_fn 检查:
    dropped_task_attempts_to_send.contains(A.task_attempt) → true
    → skip (不上报这两条 event)
  → GCS 收到 dropped 通知 → RecordTaskAttemptDropped(A)
    → 如果 GCS 中有 A 的数据 → RemoveTaskAttempt
  → A 的所有状态从 GCS 中消失
  → total_state_counts 不再统计 A
```

**关键理解：** circular buffer 的淘汰是**事件粒度**的，但 dropped 标记是 **task attempt 粒度**的。一旦一个 task attempt 的任意一条 event 被淘汰，该 task attempt 的所有剩余 event（在 buffer 中但未上报的）也会在下一次 flush 时被跳过。GCS 也会主动删除该 task attempt 的已有存储。这保证了状态一致性——不会有部分状态残留在 GCS 中。

---

## 11. GCS 淘汰机制详解

### 11.1 存储数据结构

GCS 的 `GcsTaskManagerStorage` 使用 3 个优先级列表存储 task events：

```cpp
// gcs_task_manager.h:530
std::vector<std::list<rpc::TaskEvents>> task_events_list_;
// 大小 = gc_policy_->MaxPriority() = 3
// task_events_list_[0] = Priority 0: 已完成的任务 (FINISHED)
// task_events_list_[1] = Priority 1: Actor 未完成任务
// task_events_list_[2] = Priority 2: 其他未完成任务 (普通 task)
```

每个 task attempt 在存储中只有**一个 `rpc::TaskEvents` 条目**（通过 `MergeFrom` 累积状态），存在于某个优先级列表的某个位置。通过 `TaskEventLocator` 定位，并维护 4 个索引：

| 索引 | Key | 用途 |
|------|-----|------|
| `primary_index_` | `TaskAttempt (task_id, attempt_number)` | 主索引，快速查找/合并 |
| `task_index_` | `TaskID` | 按 task_id 查询 |
| `job_index_` | `JobID` | 按 job_id 查询 |
| `worker_index_` | `WorkerID` | 按 worker_id 查询（worker 死亡时标记 task 失败）|

### 11.2 优先级判定规则

```cpp
// gcs_task_manager.h:76-87
class FinishedTaskActorTaskGcPolicy : public TaskEventsGcPolicyInterface {
  size_t MaxPriority() const override { return 3; }

  size_t GetTaskListPriority(const rpc::TaskEvents &task_events) const override {
    if (IsTaskFinished(task_events)) return 0;    // 已完成 → 优先淘汰
    if (IsActorTask(task_events)) return 1;       // Actor 未完成 → 次优先淘汰
    return 2;                                      // 其他未完成 → 最后淘汰
  }
};
```

辅助函数区别：
- `IsTaskFinished`（`protobuf_utils.cc:342`）：`state_ts_ns` 包含 `FINISHED` → true（不含 FAILED）
- `IsTaskTerminated`（`protobuf_utils.cc:310`）：`state_ts_ns` 包含 `FINISHED` **或** `FAILED` → true
- `IsActorTask`（`protobuf_utils.cc:332`）：`task_info.type == ACTOR_TASK || ACTOR_CREATION_TASK` → true

**完整的状态→优先级映射（含 FAILED 的归属）：**

| 状态 | IsTaskFinished | IsActorTask | Priority | 说明 |
|------|---------------|-------------|----------|------|
| `FINISHED` | true | — | 0 | 已完成，最先淘汰 |
| `FAILED` (普通 task) | false | false | 2 | 可能重试，保留最久 |
| `FAILED` (Actor task) | false | true | 1 | 可能重试，保留较久 |
| `RUNNING` (普通) | false | false | 2 | 未完成 |
| `RUNNING` (Actor) | false | true | 1 | 未完成 |
| `PENDING_*` (普通) | false | false | 2 | 未完成 |
| `PENDING_*` (Actor) | false | true | 1 | 未完成 |

**设计意图：** `GetTaskListPriority` 只用 `IsTaskFinished`（不含 FAILED），因为 FAILED 的 task 可能需要重试，不应优先淘汰。FINISHED 的 task 不会重试，可以安全淘汰。

### 11.3 列表内部顺序

```cpp
// AddNewTaskEvent (gcs_task_manager.cc:237):
task_events_list_.at(target_list_index).push_front(std::move(task_events));
// 新 task → push_front (插入到头部)

// UpdateExistingTaskAttempt 中状态变化导致优先级迁移 (gcs_task_manager.cc:195):
task_events_list_[target_list_index].push_front(std::move(existing_task));
// 同样 push_front 到新 list

// EvictTaskEvent (gcs_task_manager.cc:344):
const auto &to_evict = task_events_list_[list_index].back();
// 淘汰从 back() (尾部 = 最旧的)
```

每个 list 的顺序：**front** = 最新插入/更新的 task attempt，**back** = 最旧的 task attempt。淘汰从 back（最旧的）开始。

### 11.4 淘汰触发时机

```cpp
// AddOrReplaceTaskEvent (gcs_task_manager.cc:377):
if (max_num_task_events_ > 0 &&
    static_cast<size_t>(stats_counter_.Get(kNumTaskEventsStored)) > max_num_task_events_) {
    EvictTaskEvent();
}
```

触发条件：`kNumTaskEventsStored` 计数器 > `task_events_max_num_task_in_gcs`（默认 100,000）。这个检查在**每次** `AddOrReplaceTaskEvent` 之后执行。`kNumTaskEventsStored` 是实时计数器，每次 `AddNewTaskEvent` 时 `Increment`，每次 `RemoveTaskAttempt` 时 `Decrement`，精确反映当前存储的 task attempt 数量。

### 11.5 淘汰执行：EvictTaskEvent

```cpp
void GcsTaskManager::GcsTaskManagerStorage::EvictTaskEvent() {
  // Step 1: 找最低优先级的非空 list
  size_t list_index = 0;
  for (; list_index < gc_policy_->MaxPriority(); ++list_index) {
    if (!task_events_list_[list_index].empty()) break;
  }
  // 优先级: 0=已完成 > 1=Actor未完成 > 2=其他未完成

  // Step 2: 取该 list 的 back() (最旧)
  const auto &to_evict = task_events_list_[list_index].back();
  const auto &loc_iter = primary_index_.find(GetTaskAttempt(to_evict));

  // Step 3: 执行删除
  RemoveTaskAttempt(loc_iter->second);
}
```

淘汰策略总结：
1. **优先级选择**：从 Priority 0 开始，空了才到 Priority 1，再到 Priority 2
2. **同优先级内**：从 back（最旧的）淘汰
3. **每次只淘汰一个** task attempt

### 11.6 RemoveTaskAttempt 详解

```cpp
void GcsTaskManager::GcsTaskManagerStorage::RemoveTaskAttempt(
    std::shared_ptr<TaskEventLocator> loc) {
  const auto &to_remove = loc->GetTaskEventsMutable();
  const auto job_id = JobID::FromBinary(to_remove.job_id());

  // 1. 更新 JobTaskSummary: 标记 task attempt 为 dropped
  job_task_summary_[job_id].RecordProfileEventsDropped(NumProfileEvents(to_remove));
  job_task_summary_[job_id].RecordTaskAttemptDropped(GetTaskAttempt(to_remove));
  // → dropped_task_attempts_.insert(task_attempt)

  // 2. 更新 stats_counter_
  stats_counter_.Decrement(kNumTaskEventsStored);
  stats_counter_.Increment(kTotalNumTaskAttemptsDropped);
  stats_counter_.Increment(kTotalNumProfileTaskEventsDropped,
                           NumProfileEvents(to_remove));

  // 3. 从所有索引中删除
  RemoveFromIndex(loc);
  // → primary_index_.erase(task_attempt)
  // → task_index_[task_id].erase(loc)
  // → job_index_[job_id].erase(loc)
  // → worker_index_[worker_id].erase(loc)

  // 4. 从 list 中物理删除
  task_events_list_[loc->GetCurrentListIndex()].erase(loc->GetCurrentListIterator());
}
```

`RemoveTaskAttempt` 有两个调用场景：

| 场景 | 调用来源 | 触发条件 |
|------|---------|---------|
| **GCS 自己淘汰** | `EvictTaskEvent` → `RemoveTaskAttempt` | 存储超过 `max_num_task_events_` (100k) |
| **Worker 上报数据丢失** | `RecordDataLossFromWorker` → `RemoveTaskAttempt` | Worker 的 circular buffer 溢出，上报 dropped task attempts |

两种场景执行相同的 `RemoveTaskAttempt`，效果一致：task attempt 从存储中完全删除，进入 `dropped_task_attempts_`，后续事件被 `ShouldDropTaskAttempt` 永久拦截。

### 11.7 优先级迁移

当一个已存在的 task attempt 收到新 event 并 `MergeFrom` 后，其状态可能变化，导致优先级变化：

```cpp
// gcs_task_manager.cc:193-202
auto target_list_index = gc_policy_->GetTaskListPriority(existing_task);
auto cur_list_index = loc->GetCurrentListIndex();
if (target_list_index != cur_list_index) {
  // 从旧 list 移到新 list 的 front
  task_events_list_[target_list_index].push_front(std::move(existing_task));
  task_events_list_[cur_list_index].erase(loc->GetCurrentListIterator());
  loc->SetCurrentList(target_list_index, task_events_list_[target_list_index].begin());
}
```

典型场景：普通 task 从 `RUNNING`（Priority 2）变为 `FINISHED`（Priority 0），从 Priority 2 list 移到 Priority 0 list 的 front。刚完成的 task 在 Priority 0 list 中是最新的，不会被立即淘汰。

### 11.8 完整淘汰流程图

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      GCS 淘汰完整流程                                     │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  新 task event 到达                                                      │
│  → AddOrReplaceTaskEvent (line 351)                                     │
│    → ShouldDropTaskAttempt? → return (已 dropped)                       │
│    → UpdateOrInitTaskEventLocator:                                     │
│      ├─ 已存在: UpdateExistingTaskAttempt                              │
│      │   → MergeFrom (累积 state_ts_ns)                                  │
│      │   → profile events 截断 (超 1000 删最旧)                          │
│      │   → 优先级迁移 (如 RUNNING→FINISHED, list 2→list 0)              │
│      └─ 不存在: AddNewTaskEvent                                         │
│          → push_front to 对应优先级 list                                  │
│          → stats_counter_.Increment(kNumTaskEventsStored)               │
│    → 检查: kNumTaskEventsStored > max_num_task_events_ (100k)?          │
│      └─ Yes → EvictTaskEvent                                            │
│                                                                         │
│  EvictTaskEvent (line 332)                                              │
│  ┌───────────────────────────────────────────────────────────┐          │
│  │ Step 1: 找最低优先级非空 list                               │          │
│  │   for i in [0, 1, 2]:                                     │          │
│  │     if !task_events_list_[i].empty(): break               │          │
│  │   优先级: 0=已完成 > 1=Actor未完成 > 2=其他未完成             │          │
│  │                                                            │          │
│  │ Step 2: 取该 list 的 back() (最旧)                         │          │
│  │   to_evict = task_events_list_[list_index].back()          │          │
│  │                                                            │          │
│  │ Step 3: RemoveTaskAttempt                                  │          │
│  │   → RecordTaskAttemptDropped → dropped_task_attempts_      │          │
│  │   → stats_counter_.Decrement(kNumTaskEventsStored)         │          │
│  │   → stats_counter_.Increment(kTotalNumTaskAttemptsDropped) │          │
│  │   → RemoveFromIndex (primary/task/job/worker)              │          │
│  │   → list.erase(iterator) (物理删除)                         │          │
│  └───────────────────────────────────────────────────────────┘          │
│                                                                         │
│  查询时 (HandleGetTaskEvents, line 442)                                 │
│  ┌───────────────────────────────────────────────────────────┐          │
│  │ GetTaskEvents():                                           │          │
│  │   for i in [2, 1, 0] (高→低优先级):                        │          │
│  │     for itr in list[i] from rbegin to rend:               │          │
│  │       ret.push_back(*itr)                                  │          │
│  │   → 返回顺序: P2最新→P2最旧→P1最新→P1最旧→P0最新→P0最旧      │          │
│  │   → 最重要的任务 (未完成)在前, 最不重要的 (已完成)在后        │          │
│  │                                                            │          │
│  │ 遍历统计:                                                  │          │
│  │   for each task_event (reversed):                         │          │
│  │     if has_state_updates:                                  │          │
│  │       total_state_counts[latest_status]++                  │          │
│  │     if !filter_fn(task_event): num_filtered++; continue    │          │
│  │     if count < limit: reply.add_events_by_task()           │          │
│  │     else: num_limit_truncated++;                          │          │
│  │                                                            │          │
│  │ 回填数据丢失信息:                                           │          │
│  │   reply.num_status_task_events_dropped =                   │          │
│  │     JobTaskSummary.NumTaskAttemptsDropped()                │          │
│  │     + 被limit截断的status event数                           │          │
│  │   reply.num_total_stored = task_events->size()             │          │
│  │   reply.total_state_counts = {状态名: 计数}                 │          │
│  └───────────────────────────────────────────────────────────┘          │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 11.9 GCS 查询：HandleGetTaskEvents 中 limit/count/num_status_event_limit/num_limit_truncated 的关系

#### 定义

```cpp
auto limit = request.has_limit() ? request.limit() : -1;  // 来自 API 请求的 limit 参数
auto count = 0;                                            // 已返回的条目计数器
int64_t num_status_event_limit = 0;                        // 被截断的条目中,有 state_updates 的数量
int64_t num_limit_truncated = 0;                           // 被截断的条目总数 (通过 filter 但超过 limit)
```

#### 遍历逻辑

对每一条通过 `filter_fn` 的 task event：

```cpp
if (limit < 0 || count++ < limit) {
    auto events = reply->add_events_by_task();
    events->Swap(&task_event);       // 加入返回结果
} else {
    // 被截断
    num_profile_event_limit += task_event.has_profile_events()
        ? task_event.profile_events().events_size() : 0;
    num_status_event_limit += task_event.has_state_updates() ? 1 : 0;
    num_limit_truncated++;
}
```

#### 状态变化示例

假设 GCS 存储有 10 条 task events，全部通过 filter，`limit=4`：

```
条目顺序 (reversed, 最新→最旧):
  #1 RUNNING     count=0 < 4 → add to reply, count=1
  #2 FINISHED    count=1 < 4 → add to reply, count=2
  #3 RUNNING     count=2 < 4 → add to reply, count=3
  #4 FINISHED    count=3 < 4 → add to reply, count=4
  ──────── count 已达 limit ────────
  #5 RUNNING     截断: num_limit_truncated=1, num_status_event_limit=1
  #6 (仅profile) 截断: num_limit_truncated=2, num_status_event_limit=1 (无state_updates)
  #7 FAILED      截断: num_limit_truncated=3, num_status_event_limit=2
  #8 RUNNING     截断: num_limit_truncated=4, num_status_event_limit=3
  #9 FINISHED    截断: num_limit_truncated=5, num_status_event_limit=4
  #10 RUNNING    截断: num_limit_truncated=6, num_status_event_limit=5

最终:
  reply->events_by_task: 4 条 (#1-#4)
  num_limit_truncated = 6
  num_status_event_limit = 5 (6条中有1条无state_updates)
```

#### num_status_task_events_dropped 的两部分相加逻辑

```cpp
reply->set_num_status_task_events_dropped(
    reply->num_status_task_events_dropped() + num_status_event_limit);
```

最终值 = **两部分相加**：

| 来源 | 含义 | 来自 |
|------|------|------|
| **base 值** | 被永久 dropped 的 task attempts 数（`JobTaskSummary.NumTaskAttemptsDropped()`） | line 480/492，在遍历前设置 |
| **+ `num_status_event_limit`** | 本次查询因 limit 被截断、且有 state_updates 的条目数 | line 621-622 |

**设计意图：** `num_status_task_events_dropped` 返回给客户端的含义是"有多少 task 的状态你看不到"。一部分是永久数据丢失（dropped），另一部分是临时截断（limit 限制）。两者叠加让客户端知道完整的"不可见"数量。

`num_status_event_limit` 只统计有 `state_updates` 的条目，因为只有截断包含状态更新的条目才会导致"状态不可见"。仅含 profile events 的条目截断不影响状态统计。

#### reply 字段对应关系

```
reply 字段                        值                                    用途
────────────────────────────────────────────────────────────────────────────
events_by_task                    前 limit 条 (通过 filter 的)           Task Table 列表
num_total_stored                  GCS 存储的 task events 总数            诊断
num_truncated                     = num_limit_truncated                  被截断条目数
num_filtered_on_gcs               通过 filter 的条目中无 task_info 的数   islice 前的过滤计数
num_status_task_events_dropped    dropped永久丢失 + 截断中有state的        不可见状态数
num_profile_task_events_dropped   dropped永久丢失 + 截断中profile条目数    不可见profile数
total_state_counts                遍历全部条目统计的最新状态分布            Progress Bar 使用
```

### 11.10 GCS 端 filter_fn 与 limit 的先后顺序

#### 源码遍历逻辑

`HandleGetTaskEvents`（`gcs_task_manager.cc:593-620`）对每条 task_event 的处理顺序：

```cpp
for (auto &task_event : *task_events | boost::adaptors::reversed) {
    // ① total_state_counts：在 filter/limit 之前，无条件统计
    if (task_event.has_state_updates()) {
        auto latest_state = GetLatestTaskStatus(task_event);
        total_state_counts[ray::rpc::TaskStatus_Name(latest_state)]++;
    }

    // ② filter_fn：过滤掉不符合条件的条目
    if (!filter_fn(task_event)) {
        num_filtered++;
        continue;  // 不进入 limit 计数
    }

    // ③ limit：通过 filter 的条目才参与 limit 检查
    if (limit < 0 || count++ < limit) {
        auto events = reply->add_events_by_task();
        events->Swap(&task_event);       // 加入返回结果
    } else {
        // 被截断
        num_limit_truncated++;
        num_status_event_limit += task_event.has_state_updates() ? 1 : 0;
    }
}
```

**顺序：total_state_counts → filter_fn → limit**

| 步骤 | 统计/操作 | 受 filter 影响 | 受 limit 影响 |
|------|----------|---------------|---------------|
| ① total_state_counts | 有 state_updates 的条目按最新状态计数 | ❌ | ❌ |
| ② filter_fn | 不通过的条目 num_filtered++; continue | — | ❌（在 limit 之前） |
| ③ limit | 通过 filter 的条目中前 limit 条加入 events_by_task | ✅ | — |

**关键：filter 在 limit 之前。** 被 filter_fn 过滤掉的条目不消耗 limit 额度。只有通过 filter 的条目才参与 limit 计数。截断的是通过 filter 但超出 limit 的条目。

#### filter_fn 的过滤条件

```cpp
auto filter_fn = [&filters](const rpc::TaskEvents &task_event) {
    if (!task_event.has_task_info()) return false;     // 无 task_info 的僵尸条目
    if (filters.exclude_driver() && type == DRIVER_TASK) return false;
    // task_id / job_id / actor_id / task_name / state 的 EQUAL/NOT_EQUAL 谓词
    ...
    return true;
};
```

#### 截断截的是什么数据

截断发生在遍历的尾部。遍历顺序取决于查询路径：
- **无 job_id filter**：走 `GetTaskEvents()` → `task_events_list_` 优先级遍历（P2→P1→P0），截断的是低优先级（已完成）中最旧的条目
- **有 job_id filter**（Dashboard 实际使用）：走 `GetTaskEvents(job_id)` → `job_index_` 的 `flat_hash_set`，遍历顺序由 hash 决定，截断的是 hash 排在尾部的条目（随机，与优先级/时间无关）

### 11.11 Dashboard 端二次过滤与截断

#### do_filter 与 islice 的先后顺序

`list_tasks`（`state_aggregator.py:316-331`）收到 GCS 返回后：

```python
result = [protobuf_to_task_state_dict(message) for message in reply.events_by_task]

num_after_truncation = len(result)    # ← GCS 返回的 events_by_task 条数

result = do_filter(result, option.filters, TaskState, option.detail)
num_filtered = len(result)            # ← Dashboard 侧 do_filter 后的条数

result.sort(key=lambda entry: entry["task_id"])
result = list(islice(result, option.limit))  # ← Dashboard 侧最终截断
```

**顺序：do_filter → sort → islice**

#### num_after_truncation 和 num_filtered 的归属

| 字段 | 含义 | 在哪算的 |
|------|------|---------|
| `num_after_truncation` | GCS 返回的 `events_by_task` 条数（GCS 截断后剩余的数量） | **Dashboard 端**读取 GCS 结果后计数 |
| `num_filtered` | Dashboard 侧 `do_filter` 后的条数 | **Dashboard 端** |
| `num_filtered_on_gcs` | GCS 侧 `filter_fn` 过滤掉的条目数 | **GCS 端**（`reply.num_filtered_on_gcs`） |
| `num_truncated` | GCS 侧 limit 截断的条目数 | **GCS 端**（`reply.num_truncated`） |

`num_after_truncation` 的名字容易误导——它不是"被截断的数量"，而是"GCS 截断后**剩余**的数量"。

#### 为什么做两次过滤

GCS `filter_fn` 和 Dashboard `do_filter` 逻辑不完全一致：

| | GCS filter_fn | Dashboard do_filter |
|---|---|---|
| 数据格式 | protobuf TaskEvents | Python dict（转换后） |
| 过滤无 task_info | ✅ | ❌（GCS 已过滤，不存在） |
| exclude_driver | ✅ | ✅ |
| 字段匹配 | 基于 protobuf 字段，大小写敏感 | 支持 str/int/bool 类型转换，大小写不敏感 |
| 精度 | 粗筛 | 精筛 |

GCS 侧做粗筛（基于 protobuf 字段），Dashboard 侧做精确过滤（基于转换后的 Python dict，支持类型转换和大小写不敏感匹配）。

#### 四层 limit 完整表格

| 层级 | 变量名 | 默认值 | 来源 |
|------|--------|--------|------|
| 前端 HTTP | URL ?limit=N | 100 (`DEFAULT_LIMIT`) | `state_api_utils.py:79` |
| Dashboard→GCS RPC | `get_all_task_info(limit=)` | 10000 (`RAY_MAX_LIMIT_FROM_DATA_SOURCE`) | `state_manager.py:234` |
| GCS 遍历 | `request.limit()` | 10000 | 从 RPC 请求读取 |
| Dashboard 最终截断 | `islice(result, option.limit)` | 100 (`DEFAULT_LIMIT`) 或 10000 (`summarize` 调用时) | `state_aggregator.py:331` |

**注意**：`list_tasks` 调用 `get_all_task_info` 时没有传 `limit` 参数（`state_aggregator.py:306-309`），所以用默认值 `RAY_MAX_LIMIT_FROM_DATA_SOURCE=10000`。GCS 收到 limit=10000，最多返回 10000 条 `events_by_task`。

`summarize_tasks` 调用 `list_tasks` 时传的 `option.limit=RAY_MAX_LIMIT_FROM_API_SERVER=10000`（`state_aggregator.py:583`），所以 summarize 的最终 islice 也是 10000。

### 11.12 /api/v0/tasks 与 /api/v0/tasks/summarize 的区别

#### 调用链对比

```
/api/v0/tasks?detail=1&limit=N
  → options_from_req: limit=N (默认100, 上限10000)
  → list_tasks(option=ListApiOptions(limit=N))
    → get_all_task_info(limit=10000)        ← GCS RPC
      → GCS HandleGetTaskEvents(limit=10000)
        → events_by_task (最多 10000 条)
        → total_state_counts (全量统计, 不受 filter/limit)  ← 但不返回给 /api/v0/tasks
    → do_filter → islice(result, N)         ← 最终只返回 N 条
  → 返回: ListApiResponse(result=[具体 task 列表])

/api/v0/tasks/summarize?filter_keys=job_id&...
  → summary_options_from_req: 无 limit 参数
  → summarize_tasks(option=SummaryApiOptions)
    → list_tasks(option=ListApiOptions(limit=10000))  ← 用 RAY_MAX_LIMIT_FROM_API_SERVER
      → get_all_task_info(limit=10000)      ← 同一个 GCS RPC
        → GCS HandleGetTaskEvents(limit=10000)
          → events_by_task (最多 10000 条)
          → total_state_counts (全量统计)    ← 返回给 summarize
      → do_filter → islice(result, 10000)
    → to_summary_by_func_name(tasks=result.result)
      → 按 func_name 分组, 每组统计 state_counts
    → 返回: SummaryApiResponse(
        result=StateSummary(node_id_to_summary={"cluster": summary}),
        total_state_counts=...,           ← 全量统计, Progress Bar 使用
        num_after_truncation=...,
        num_filtered=...,
      )
```

#### 关键区别

| | `/api/v0/tasks` | `/api/v0/tasks/summarize` |
|---|---|---|
| 返回内容 | 具体 task 列表（每条含 state、events 时间线等） | 按 func_name 分组的状态计数 |
| 最终 limit | 100（默认）或 URL 指定 | 10000（`RAY_MAX_LIMIT_FROM_API_SERVER`） |
| `total_state_counts` | 不返回 | **返回**（GCS 全量统计，不受 filter/limit） |
| Progress Bar 使用 | 不用 | `total_state_counts`（路径 A）或 `node_id_to_summary` 聚合（路径 B） |
| Task Table 使用 | 用 `result` 列表 | 不用 |
| 数据精度 | 受 GCS filter + limit + Dashboard do_filter + islice 四层截断 | `total_state_counts` 不受任何 filter/limit 影响；`node_id_to_summary` 受截断 |

**核心区别**：`summarize` 额外返回 `total_state_counts`，它是 GCS 遍历全部存储条目时的全量统计，不受 filter_fn 和 limit 影响。Progress Bar 优先用它。`/api/v0/tasks` 只返回截断后的 `events_by_task` 列表，没有全量统计。

两者底层都调 `list_tasks` → 同一个 GCS RPC，但 `summarize` 的 limit 更大（10000 vs 100），且额外携带 `total_state_counts`。

### 11.13 按 job_id 查询时的顺序问题（设计缺陷）

#### 两条查询路径

| | 无 job_id filter | 有 job_id filter（Dashboard 实际使用） |
|---|---|---|
| 调用路径 | `GetTaskEvents()` | `GetTaskEvents(job_id)` |
| 数据结构 | `task_events_list_`（3 个优先级 list） | `job_index_[job_id]`（`flat_hash_set`） |
| 遍历顺序 | P2 最新→最旧 → P1 → P0（优先级 + 时间） | **hash 顺序（本质随机）** |
| RUNNING 在前 | ✅ | ❌ 不保证 |
| 截断行为 | 优先截断 P0（已完成）最旧 | **随机截断** |

#### 源码路径

```cpp
// GetTaskEvents() — 无 job_id
for (int i = MaxPriority() - 1; i >= 0; --i) {  // P2→P1→P0
    for (auto itr = list[i].rbegin(); itr != list[i].rend(); ++itr) {
        ret.push_back(*itr);  // 优先级 + 最新→最旧
    }
}

// GetTaskEvents(job_id) — 有 job_id（Dashboard 实际使用）
auto task_locators_itr = job_index_.find(job_id);
return GetTaskEvents(task_locators_itr->second);  // 传入 flat_hash_set

// GetTaskEvents(flat_hash_set<locator>)
for (const auto &task_attempt_loc : task_locators) {
    result.push_back(task_attempt_loc->GetTaskEventsMutable());  // hash 顺序遍历
}
```

`flat_hash_set` 的遍历顺序由元素的 hash 值决定，与插入顺序、优先级、时间都无关。Dashboard 总是带 `job_id` filter 查询，所以走的是 `job_index_` 路径——**丢失了优先级排序信息**。

#### 大规模作业下的影响

```
假设 job 有 50000 个 FINISHED + 200 个 RUNNING Actor + 100 个 RUNNING 普通
limit=10000

GetTaskEvents(job_id) → flat_hash_set 遍历 → 50300 条, hash 随机序
reversed → 仍然随机序
前 10000 条 → 可能全是 FINISHED（取决于 hash 分布）
后 40300 条被截断 → 可能包含所有 RUNNING Actor

结果：Task Table 里看不到 Actor running task
但 total_state_counts（不受 limit 影响）→ Progress Bar 显示 RUNNING: 300
```

Progress Bar 正确（用 `total_state_counts`，不受 limit），Task Table 看不到（用 `events_by_task`，受 limit 截断，而截断顺序是随机的）。

### 11.13a 不带 commit 9be153f7ae 时的状态统计行为分析

> **Commit**: `9be153f7ae` — `feat: expose total_state_counts in Dashboard for all GCS buffer entries`
>
> 该 commit 新增了 `total_state_counts` 字段，使 Dashboard Progress Bar 能显示 GCS buffer 中**所有**有条目（包括无 `task_info` 的僵尸条目）的状态统计。以下分析不带该 commit 时的行为。

#### 1. 不带该 commit 时，两个接口都不返回 `total_state_counts`

该 commit 是**新增**字段，涉及三个层面：

| 文件 | 改动 |
|------|------|
| `src/ray/protobuf/gcs_service.proto` | `GetTaskEventsReply` 新增 `map<string, int64> total_state_counts = 8;`（field 8） |
| `python/ray/util/state/common.py` | `ListApiResponse` 和 `SummaryApiResponse` 新增 `total_state_counts`、`num_total_stored`、`num_filtered_on_gcs` 字段 |
| `python/ray/dashboard/state_aggregator.py` | `list_tasks` 的 `transform` 函数和 `summarize_tasks` 都新增传递 `total_state_counts` 的逻辑 |

不带该 commit 时，`GetTaskEventsReply` 只有 fields 1-7（`status`、`events_by_task`、`num_profile_task_events_dropped`、`num_status_task_events_dropped`、`num_total_stored`、`num_filtered_on_gcs`、`num_truncated`），**没有 `total_state_counts`**。`ListApiResponse` 和 `SummaryApiResponse` 也没有该字段。`/api/v0/tasks` 和 `/api/v0/tasks/summarize` 都不返回该字段。

#### 2. 不带该 commit 时，状态统计仅基于返回的可见 task

不带该 commit 时的数据流：

1. **GCS `filter_fn`**（`gcs_task_manager.cc:527-529`）直接跳过没有 `task_info` 的条目：
   ```cpp
   if (!task_event.has_task_info()) {
       return false;  // 被 filter 掉，不计入结果
   }
   ```

2. **`summarize_tasks`** 调用 `list_tasks`，然后 `TaskSummaries.to_summary_by_func_name`（`common.py:1043`）只遍历 `result.result`（即可见的、有 `task_info` 且通过过滤的 task）来统计 `state_counts`：
   ```python
   for task in tasks:          # tasks = result.result（截断后的可见 task 列表）
       key = task["func_or_class_name"]
       state = task["state"]
       task_summary.state_counts[state] += 1
   ```

3. 因此 RUNNING/FINISHED/FAILED 等状态计数**只反映了有 `task_info` 的 task**，没有 `task_info` 但有 `state_updates` 的"僵尸"条目被完全忽略

#### 3. GCS buffer 中为什么会有无 `task_info` 的条目

`TaskEvents` protobuf 可以有 `state_updates` 但没有 `task_info`，因为：

- Task event 是 Worker 增量上报的，Worker 可能先上报了 `state_updates`（如 RUNNING）但还没有/永远不会上报 `task_info`
- `MergeFrom` 操作会合并字段，如果第一个 event 有 `state_updates`，后续 event 有 `task_info`，它们会合并。但如果 `task_info` 永远不上报，该条目就永远没有 `task_info`
- `MarkTaskAttemptFailedIfNeeded`（`gcs_task_manager.cc:164-181`）可以给还没有 `task_info` 的条目添加 `FAILED` 状态

#### 4. job_id 不会导致随机返回 task，但遍历顺序不确定 + limit 截断会导致统计不准

**job_id 不会导致随机选 task**。GCS 使用 `job_index_` 做索引查询（`gcs_task_manager.cc:478-480`）：
```cpp
if (job_ids.size() == 1) {
    const JobID &job_id = *job_ids.begin();
    task_events = task_event_storage_->GetTaskEvents(job_id);  // 索引查找，确定性
}
```
该 job 的所有 task 都会被选为候选，不是随机的。

**但统计不准的问题确实存在**，原因有两个：

##### (a) 没有 `task_info` 的条目被系统性遗漏

Worker 上报 task event 时，可能先上报了 `state_updates`（如 RUNNING）但还没有/永远不会上报 `task_info`。这些条目有状态但被 `filter_fn` 过滤掉，其状态不被统计。在 high-throughput 场景下，这类条目可能很多，导致 RUNNING 计数严重偏低。

##### (b) limit 截断导致部分状态丢失

| 查询路径 | 遍历顺序 | 截断行为 |
|----------|---------|---------|
| 不带 job_id | `task_events_list_` 按优先级遍历（P2→P1→P0），每个优先级内按插入逆序 | 优先截断 P0（已完成）最旧的条目，RUNNING task 优先保留 |
| 带 job_id（Dashboard 实际使用） | `job_index_[job_id]` 的 `flat_hash_set`，hash 顺序 | **不区分优先级，RUNNING 和 FINISHED 被截断的概率均等** |

不带 job_id 时按优先级遍历，截断时优先保留 RUNNING task（priority 2），FINISHED task（priority 0）最先被截断。

带 job_id 时按 `flat_hash_set` 遍历，不区分优先级，**RUNNING 和 FINISHED 被截断的概率均等**。如果 limit 触发截断，可能丢掉 RUNNING task 而保留 FINISHED task，使状态统计不准。

`summarize_tasks` 虽然用了 `RAY_MAX_LIMIT_FROM_API_SERVER`（默认 10000）作为 limit，如果单 job 的 task 数不超过 10000，不会有截断问题。但如果 task 数量超过 10000 且没有 `total_state_counts`，统计就不准了。

#### 5. 带 job_id 与不带 job_id 遍历顺序差异的具体代码

##### 不带 job_id：按优先级遍历

`GetTaskEvents()`（`gcs_task_manager.cc:56-67`）遍历 `task_events_list_`，这是一个**按 GC 优先级分层的 vector**：

```cpp
// FinishedTaskActorTaskGcPolicy: MaxPriority() = 3
// priority 0: FINISHED task (最先被 GC)
// priority 1: ACTOR_TASK
// priority 2: 其他非 FINISHED 的普通 task (最后被 GC)
for (int i = MaxPriority() - 1; i >= 0; --i) {  // P2→P1→P0
    for (auto itr = task_events_list_[i].rbegin(); itr != task_events_list_[i].rend(); ++itr) {
        ret.push_back(*itr);  // 优先级 + 最新→最旧
    }
}
```

返回顺序：**priority 2 (running normal) → priority 1 (actor task) → priority 0 (finished)**，每个优先级内按插入逆序（最新优先）。然后再经过 `filter_fn` 过滤、limit 截断。

##### 带 job_id：按 locator 遍历，不按优先级

`GetTaskEvents(job_id)`（`gcs_task_manager.cc:71-77`）通过 `job_index_` 拿到一个 `flat_hash_set<shared_ptr<TaskEventLocator>>`，然后直接遍历 locator 取数据：

```cpp
for (const auto &task_attempt_loc : task_locators) {
    result.push_back(task_attempt_loc->GetTaskEventsMutable());  // hash 顺序遍历
}
```

`flat_hash_set` 的遍历顺序是**哈希序**，不保证任何特定顺序。然后再经过 `filter_fn` 过滤、limit 截断。

##### 对比

| | 不带 job_id | 带 job_id |
|---|---|---|
| 优先级保留 | ✅ RUNNING 优先保留 | ❌ 不保证 |
| 截断是否随机 | 不随机，优先截断 FINISHED | **随机截断** |
| limit 触发时统计准确性 | 较好（RUNNING 保留在前） | **可能严重不准** |

#### 6. commit 9be153f7ae 如何修复这些问题

该 commit 在 `filter_fn` **之前**、limit 截断**之前**，对所有有 `state_updates` 的条目统一计数：

```cpp
int64_t num_filtered = 0;
absl::flat_hash_map<std::string, int64_t> total_state_counts;
Status status = Status::OK();
try {
    for (auto &task_event : *task_events | boost::adaptors::reversed) {
        // ① 在 filter 之前统计所有有条目的条目
        if (task_event.has_state_updates()) {
            auto latest_state = GetLatestTaskStatus(task_event);
            total_state_counts[ray::rpc::TaskStatus_Name(latest_state)]++;
        }

        // ② 过滤掉无 task_info 的
        if (!filter_fn(task_event)) {
            num_filtered++;
            continue;
        }

        // ③ limit 截断也在这之后
        if (limit < 0 || count++ < limit) {
            auto events = reply->add_events_by_task();
            events->Swap(&task_event);
        } else {
            num_limit_truncated++;
            ...
        }
    }
}
```

**顺序：total_state_counts → filter_fn → limit**

| 步骤 | 统计/操作 | 受 filter 影响 | 受 limit 影响 |
|------|----------|---------------|---------------|
| ① total_state_counts | 有 state_updates 的条目按最新状态计数 | ❌ | ❌ |
| ② filter_fn | 不通过的条目 num_filtered++; continue | — | ❌（在 limit 之前） |
| ③ limit | 通过 filter 的条目中前 limit 条加入 events_by_task | ✅ | — |

这样 `total_state_counts` 就包含了**所有候选集**（包括被 filter 掉的、被 limit 截断的）中有 `state_updates` 的条目状态，且范围与 job_id 过滤一致（因为 `task_events` 已经通过 `job_index_` 限定到该 job）。

Dashboard 前端在 `useJobProgress.ts:135-143` 优先使用 `total_state_counts` 而非 summary 聚合的 `state_counts`：

```typescript
// Prefer total_state_counts for progress segments when available.
// total_state_counts includes ALL entries in the GCS buffer (including
// zombie entries without task_info), providing accurate state distribution.
// The summary-based `summed` only counts entries with task_info, which
// can be much lower due to GCS buffer eviction in high-throughput scenarios.
const progressFromTotalStateCounts = data?.totalStateCounts
    ? formatStateCountsToProgress(data.totalStateCounts)
    : null;
```

#### 7. 总结：不带该 commit 的影响

| 问题 | 不带 commit | 带 commit |
|------|------------|----------|
| 僵尸条目（有 state 无 task_info）的状态 | 完全遗漏，不被统计 | 计入 `total_state_counts` |
| limit 截断的条目状态 | 完全遗漏 | 计入 `total_state_counts` |
| 带 job_id 时 hash 随机序截断 | RUNNING 可能被随机截断掉 | `total_state_counts` 不受截断影响 |
| Progress Bar RUNNING 数 | 严重偏低 | 准确（全量统计） |
| 状态统计来源 | 仅基于 `events_by_task`（可见 task） | `total_state_counts`（全量）+ `events_by_task`（可见 task） |

**核心结论**：不带该 commit，状态统计确实只基于可见 task，会遗漏无 `task_info` 的条目和被 limit 截断的条目，导致统计不准。job_id 本身不会导致随机返回 task，但带 job_id 时的 `flat_hash_set` 遍历顺序不确定 + limit 截断 = 状态统计可能严重不准。该 commit 通过在 filter/limit 之前做全量统计解决了这个问题。

### 11.14 僵尸 Entry 与 include_task_info 传递路径

#### SetTaskStatus 的 include_task_info 默认值

```cpp
// task_manager.h:697
void SetTaskStatus(
    TaskEntry &task_entry,
    rpc::TaskStatus status,
    std::optional<TaskStateUpdate> state_update = std::nullopt,
    bool include_task_info = false,          // ← 默认 false
    std::optional<int32_t> attempt_number = std::nullopt);
```

#### 各状态转换的 include_task_info 完整表格

| 步骤 | 状态转换 | 角色 | include_task_info | 代码位置 |
|------|---------|------|-------------------|---------|
| 1 | → `PENDING_ARGS_AVAIL` (首次提交) | Submitter | **true** | `task_manager.cc:349` |
| 2 | → `PENDING_NODE_ASSIGNMENT` | Submitter | **false** (默认) | `task_manager.cc:1682` |
| 3 | → `SUBMITTED_TO_WORKER` | Submitter | **true** | `task_manager.cc:1697` |
| 4 | → `RUNNING` | Executor | **false** (默认, `task_events_executor_include_task_info=false`) | `core_worker.cc:3070` |
| 5 | → `FINISHED` (正常完成) | Submitter | **false** (默认) | `task_manager.cc:1053` |
| 6 | → `FAILED` (执行失败) | Submitter | **false** (默认) | `task_manager.cc:1047` |
| 7 | → `FAILED` (重试, 旧 attempt) | Submitter | **false** (默认) | `task_manager.cc:1190` |
| 8 | → `PENDING_ARGS_AVAIL` (重试新 attempt) | Submitter | **true** | `task_manager.cc:1232` |
| 9 | → `PENDING_ACTOR_TASK_ARGS_FETCH` | Actor Worker | **false** (默认) | `ordered/unordered_actor_task_execution_queue.cc` |
| 10 | → `PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY` | Actor Worker | **false** (默认) | `ordered/unordered_actor_task_execution_queue.cc` |

#### 僵尸 Entry 产生原因

正常流程：Submitter 的 `PENDING_ARGS_AVAIL`（include_task_info=true）先到达 GCS，创建带 task_info 的完整 entry。后续状态更新通过 MergeFrom 累积。

僵尸产生的三种情况：

**情况 A：Executor RUNNING 先于 Submitter 到达 GCS**

```
时间线:
  t1: Submitter 上报 PENDING_ARGS_AVAIL (include_task_info=true)
  t2: Executor 上报 RUNNING (include_task_info=false) → GCS 创建无 task_info 的 entry ← 僵尸
  t3: Submitter 的 PENDING_ARGS_AVAIL 到达 → MergeFrom → 补全 task_info ← 僵尸被修复
```

如果 t1 的上报丢失（buffer 溢出），t2 产生的僵尸无法修复。

**情况 B：Submitter 第一条 event 丢失，后续 event 到达**

```
t1: PENDING_ARGS_AVAIL (true) → 丢失
t2: PENDING_NODE_ASSIGNMENT (false) → GCS 创建无 task_info 的 entry ← 僵尸
t3: SUBMITTED_TO_WORKER (true) → MergeFrom → 补全 ← 僵尸被修复
```

如果 t3 也丢失，僵尸无法修复。

**情况 C：所有带 task_info 的 event 都丢失**

```
t1: PENDING_ARGS_AVAIL (true) → 丢失
t2: RUNNING (false) → 丢失
t3: FINISHED (false) → GCS 创建无 task_info 的 entry ← 永久僵尸
```

#### GCS MergeFrom 补全机制

```cpp
// UpdateExistingTaskAttempt (gcs_task_manager.cc:172)
if (task_events.has_task_info() && !existing_task.has_task_info()) {
    // 只有新数据有 task_info 而旧数据没有时，才更新类型计数
    stats_counter_.Increment(...)
}
existing_task.MergeFrom(task_events);
```

protobuf `MergeFrom`：如果新数据有 `task_info`，合并后才会有；如果新数据没有 `task_info`，合并后保持旧值。

#### 修复方案：task_events_executor_include_task_info

```cpp
// ray_config_def.h:501
RAY_CONFIG(bool, task_events_executor_include_task_info, false)
```

开启后 Executor 的 RUNNING 上报也带 `task_info`，可防止僵尸产生。代价：每个 task 的 `task_info` 从 submitter 和 executor 各发送一次，带宽翻倍。

```bash
# 启用方式
export RAY_task_events_executor_include_task_info=true
# 或
ray.init(_system_config={"task_events_executor_include_task_info": True})
```

### 11.15 Progress Bar RUNNING 数量完整统计链路

#### GCS 侧 total_state_counts 统计

```cpp
// gcs_task_manager.cc:593-598
for (auto &task_event : *task_events | boost::adaptors::reversed) {
    if (task_event.has_state_updates()) {
        auto latest_state = GetLatestTaskStatus(task_event);
        total_state_counts[ray::rpc::TaskStatus_Name(latest_state)]++;
    }
    // filter_fn 和 limit 在后面，不影响 total_state_counts
}
```

`total_state_counts` 在 `filter_fn` 之前统计，遍历全部条目，包括无 `task_info` 的僵尸条目，不受 filter/limit 影响。

#### GetLatestTaskStatus 逻辑

```cpp
// gcs_task_manager.cc:434
ray::rpc::TaskStatus GetLatestTaskStatus(const rpc::TaskEvents &task_event) {
    if (!task_event.has_state_updates()) return ray::rpc::TaskStatus::NIL;
    const auto *descriptor = ray::rpc::TaskStatus_descriptor();
    // 从最高枚举值往低遍历，找到第一个在 state_ts_ns 中存在的状态
    for (int i = descriptor->value_count() - 1; i >= 0; --i) {
        if (task_event.state_updates().state_ts_ns().contains(
                descriptor->value(i)->number())) {
            return static_cast<ray::rpc::TaskStatus>(descriptor->value(i)->number());
        }
    }
    return ray::rpc::TaskStatus::NIL;
}
```

按**枚举值最高**遍历（不是时间戳最新），返回枚举值最大的状态。TaskStatus 枚举值顺序：`NIL=0, PENDING_ARGS_AVAIL=1, ..., RUNNING=8, ..., FINISHED=13`。

#### 前端两条计算路径

```typescript
// useJobProgress.ts:140-155

// 路径 A（优先）：直接用 total_state_counts
const progressFromTotalStateCounts = data?.totalStateCounts
    ? formatStateCountsToProgress(data.totalStateCounts)
    : null;
const totalFromStateCounts = data?.totalStateCounts
    ? Object.values(data.totalStateCounts).reduce((acc, count) => acc + count, 0)
    : undefined;

// 路径 B（退化）：从 node_id_to_summary 的 func_name 分组累加
const summed = (data?.summary ?? []).reduce((acc, task) => {
    Object.entries(task.progress).forEach(([k, count]) => {
        acc[k] = (acc[k] ?? 0) + count;
    });
    return acc;
}, {} as TaskProgress);

return {
    progress: progressFromTotalStateCounts ?? summed,       // 优先 A，退化 B
    totalTasks: totalFromStateCounts ?? data?.totalTasks,   // 优先 A，退化 B
};
```

`formatStateCountsToProgress`（`useJobProgress.ts:227`）把 `total_state_counts` 中每个状态的计数映射到 Progress Bar segment：

```typescript
const formatStateCountsToProgress = (stateCounts: { [stateName: string]: number }) => {
    const formattedProgress: TaskProgress = {};
    Object.entries(stateCounts).forEach(([state, count]) => {
        const taskStatus = TASK_STATE_NAME_TO_PROGRESS_KEY[state];
        const key = TaskStatusToTaskProgressMapping[taskStatus] ?? "numUnknown";
        formattedProgress[key] = (formattedProgress[key] ?? 0) + count;
    });
    return formattedProgress;
};
```

**路径 A**（当前主流）：`data.totalStateCounts` 来自 `summarize_tasks` 返回的 `result.total_state_counts`，即 GCS 直接返回的全量状态统计 map。Progress Bar 的 RUNNING 数 = `total_state_counts["RUNNING"]`。

**路径 B**（退化）：`summed` 从 `node_id_to_summary.cluster.summary`（按 func_name 分组）中每个分组的 `state_counts` 累加。但 `node_id_to_summary` 来自 `to_summary_by_func_name(tasks=result.result)`，统计的是 `list_tasks` 返回的 `events_by_task`（受 GCS filter/limit 影响），所以路径 B 的 RUNNING 数可能偏低。

#### Progress Bar RUNNING 数量的准确性

Progress Bar 的 RUNNING 数 = `total_state_counts["RUNNING"]` = GCS 存储中当前状态为 RUNNING 的所有 task attempt 数（含僵尸 entry，不含被 drop 的 task attempt）。

**统计条件**：一个 task attempt 被计入 `total_state_counts["RUNNING"]` 需要：
1. 该 task attempt 在 GCS 存储中（未被 EvictTaskEvent 或 RecordDataLossFromWorker 删除）
2. `has_state_updates() == true`（至少有一个状态被上报过）
3. `GetLatestTaskStatus` 返回 `RUNNING`（`state_ts_ns` 中存在 RUNNING，且不存在枚举值更大的状态如 FINISHED/FAILED）

**不被计入的情况**：
- task attempt 被 GCS 淘汰 → 不在存储中 → 不统计
- task attempt 被 Worker 端 drop 后上报到 GCS → `RemoveTaskAttempt` → 不在存储中 → 不统计
- 只有 profile events，无 state_updates → 不统计
- 最新状态已变为 FINISHED → 计入 FINISHED 而非 RUNNING

**不会因为 filter 导致数量错误**：`total_state_counts` 在 `filter_fn` 之前统计，不受 filter 影响。僵尸 entry（无 task_info）的状态也会被统计到。

**但被 drop 的 task 不在统计范围内**：被 drop 的 task 数体现在 `num_status_task_events_dropped` 中，Progress Bar 的 RUNNING 数本身不包含这些丢失的 task。

#### num_dropped_task_attempts_evicted_ 的含义

```cpp
class JobTaskSummary {
    int64_t num_task_attempts_dropped_tracked_ = 0;
    // per-job 当前在 dropped_task_attempts_ 集合中的 task attempt 数

    int64_t num_dropped_task_attempts_evicted_ = 0;
    // per-job 从 dropped_task_attempts_ 集合中被 GC 清理掉的数量

    absl::flat_hash_set<TaskAttempt> dropped_task_attempts_;
    // per-job 的 dropped task attempts 集合
};
```

`dropped_task_attempts_` 集合本身也有上限（`task_events_max_dropped_task_attempts_tracked_per_job_in_gcs`，默认 1,000,000），每 5 秒由 `GcJobSummary()` → `GcOldDroppedTaskAttempts()` 检查，超过时淘汰最旧的（多淘汰 10% 防 thrashing）。

`NumTaskAttemptsDropped()` = `num_task_attempts_dropped_tracked_` + `num_dropped_task_attempts_evicted_`，代表该 job 的 task attempts 被 dropped 的总数（包括仍在集合中的 + 已被 GC 清理的）。

Worker 端 circular buffer 溢出和 GCS 端 EvictTaskEvent 两种 drop 来源最终都走 `RemoveTaskAttempt` → `RecordTaskAttemptDropped` → 递增 `num_task_attempts_dropped_tracked_`。

### 11.16 /api/v0/tasks 的 task state 推导（Python 侧）

#### protobuf_to_task_state_dict 的 state 推导逻辑

Dashboard Python 侧（`common.py:1678-1694`）从 `state_ts_ns` map 推导 task 当前状态：

```python
events = []
if "state_ts_ns" in state_updates:
    state_ts_ns = state_updates["state_ts_ns"]
    for state_name, state in TaskStatus.items():  # 按枚举值从低到高遍历 (0→13)
        key = str(state)
        if key in state_ts_ns:
            ts_ms = int(state_ts_ns[key]) // 1e6
            events.append({"state": state_name, "created_ms": ts_ms})

# 取 events 列表的最后一个 = 枚举值最大的状态
if len(events) > 0:
    latest_state = events[-1]["state"]
else:
    latest_state = "NIL"
task_state["state"] = latest_state
```

Python 侧按**枚举值从低到高**遍历 `TaskStatus.items()`，`events[-1]` 取的是**枚举值最大的状态**——与 GCS 侧 `GetLatestTaskStatus`（从高到低遍历）逻辑一致但实现不同。

**注意**：Python 侧取的是枚举值最大的状态，不是时间戳最新的状态。在正常状态流转（枚举值递增）下两者一致。

#### PENDING_ACTOR_TASK_ARGS_FETCH 在 Dashboard 显示的原因

`PENDING_ACTOR_TASK_ARGS_FETCH` 枚举值 = 6，`RUNNING` = 8。当一个 Actor task 的 `state_ts_ns` 只有 `{PENDING_ACTOR_TASK_ARGS_FETCH: t1}`（RUNNING 还没上报）时，`latest_state = PENDING_ACTOR_TASK_ARGS_FETCH`。

这说明该 Actor task **已经到达 Actor Worker**，但还在**等待 ObjectRef 依赖参数拉取**，尚未进入 RUNNING。这是 Actor task 的正常中间状态。

```
Actor Task 状态流转:
  PENDING_ARGS_AVAIL (Submitter, include_task_info=true)
    → SUBMITTED_TO_WORKER (Submitter, include_task_info=true)
      → PENDING_ACTOR_TASK_ARGS_FETCH (Actor Worker, include_task_info=false)  ← 有 ObjectRef 依赖时
        → PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY (Actor Worker, include_task_info=false)
          → RUNNING (Executor, include_task_info=false)
            → FINISHED/FAILED (Submitter, include_task_info=false)
```

前端 `TASK_STATE_NAME_TO_PROGRESS_KEY` 将 `PENDING_ACTOR_TASK_ARGS_FETCH` 映射为 `SUBMITTED_TO_WORKER`，合并到 Progress Bar 的 "Waiting for scheduling" segment。

#### /api/v0/tasks?limit=30000 的限制

`limit=30000` 会被 `options_from_req`（`state_api_utils.py:82`）拒绝：

```python
if limit > RAY_MAX_LIMIT_FROM_API_SERVER:
    raise ValueError(f"Given limit {limit} exceeds the supported limit ...")
```

`RAY_MAX_LIMIT_FROM_API_SERVER` 默认 10000，所以 `limit=30000` 直接返回 400 BAD REQUEST。

`/api/v0/tasks` 返回的 task 列表是 `events_by_task`，受 GCS filter_fn（过滤僵尸）和 limit 截断（hash 随机序）影响，不是全量数据，RUNNING 计数可能偏低。如需全量 RUNNING 数，应使用 `/api/v0/tasks/summarize` 返回的 `total_state_counts`。

### 11.17 诊断方法：区分僵尸 vs drop

#### 比较 sum(total_state_counts) 和 num_after_truncation

```bash
curl -s "http://localhost:8265/api/v0/tasks/summarize?filter_keys=job_id&filter_predicates=%3D&filter_values=05000000" | python3 -c "
import json, sys
r = json.load(sys.stdin)['data']['result']
tsc = r.get('total_state_counts', {})
total_state = sum(tsc.values()) if tsc else 0
visible = r['num_after_truncation']
dropped = r.get('total', 0) - total_state  # num_status_task_events_dropped
print(f'GCS 存储中有 state_updates 的条目: {total_state}')
print(f'GCS 返回的 events_by_task 条数: {visible}')
print(f'被永久 dropped 的 task attempt 数: {dropped}')
print(f'僵尸 entry 数（有 state 无 task_info）: {total_state - visible}')
print(f'状态分布: {tsc}')
"
```

| 情况 | 特征 | 原因 |
|------|------|------|
| `sum(total_state_counts) > num_after_truncation` | 僵尸 entry 存在 | 有 state_updates 但无 task_info 的条目被 filter_fn 过滤 |
| `sum(total_state_counts) ≈ num_after_truncation` | 无僵尸 | GCS filter_fn 和 Dashboard do_filter 一致 |
| `num_status_task_events_dropped` 很大 | 大量 task 被 drop | GCS 存储淘汰或 Worker buffer 溢出导致永久丢失 |

#### 检查 RUNNING 数是否准确

```
Progress Bar RUNNING = total_state_counts["RUNNING"]
                     = GCS 存储中当前状态为 RUNNING 的 task attempt 数
                     = 含僵尸 entry（有 RUNNING 状态但无 task_info）
                     ≠ 实际 RUNNING 数（被 drop 的 task 不计入）

Task Table 中的 RUNNING = events_by_task 中 state=RUNNING 的条目数
                        = 通过 filter_fn（有 task_info）且未被 limit 截断的条目
                        ≤ Progress Bar RUNNING（可能远小于）
```

### 11.18 /api/v0/tasks 的 RUNNING 数量统计方式

#### /api/v0/tasks 逐条 state 推导

`/api/v0/tasks` 返回的每条 task 的 `state` 字段由 Dashboard Python 侧逐条推导（`common.py:1678-1694`）：

```python
# 按枚举值从低到高遍历 state_ts_ns
for state_name, state in TaskStatus.items():  # 0→13
    if str(state) in state_ts_ns:
        events.append({"state": state_name, "created_ms": ts_ms})

# 取 events 列表的最后一个 = 枚举值最大的状态
latest_state = events[-1]["state"]
task_state["state"] = latest_state
```

`/api/v0/tasks` 返回的 RUNNING 数 = `events_by_task` 中 `state == "RUNNING"` 的条目数。

这不是全量统计，而是**截断后的列表中** RUNNING 的条数。受四层截断影响：
1. GCS `filter_fn` 过滤掉无 `task_info` 的僵尸条目（有 RUNNING 状态但无 task_info → 不出现）
2. GCS limit=10000 截断（按 hash 随机序，RUNNING 不优先）
3. Dashboard `do_filter` 二次过滤
4. Dashboard `islice(limit)` 最终截断（默认 100）

所以 `/api/v0/tasks` 的 RUNNING 数**可能远小于实际**。如需全量 RUNNING 数，应使用 `/api/v0/tasks/summarize` 返回的 `total_state_counts`。

### 11.19 /api/v0/tasks 与 /api/v0/tasks/summarize 的 task 状态区别

两个 API 底层调用同一个 GCS RPC（`GetTaskEvents`），状态来源完全相同。区别在于返回方式和展示粒度：

| | `/api/v0/tasks` | `/api/v0/tasks/summarize` |
|---|---|---|
| 逐条 state | ✅ 每条 task 有 `state` 字段（原始状态名） | ❌ 不返回逐条 task |
| 分组 state_counts | ❌ | ✅ 按 func_name 分组，每组有 `state_counts` |
| `total_state_counts` | ❌ | ✅ 全量统计（不受 filter/limit） |
| RUNNING 来源 | `events_by_task` 中 state=RUNNING 的条数（截断后） | `total_state_counts["RUNNING"]`（全量）或 `state_counts` 累加（截断后） |
| Task Table 使用 | ✅ | ❌ |
| Progress Bar 使用 | ❌ | ✅ |

`summarize` 的 `state_counts` 来自 `to_summary_by_func_name(tasks=result.result)`，遍历 `list_tasks` 返回的截断后列表，逐条按 `task["state"]` 分组计数。所以 `node_id_to_summary` 中的 state_counts 也是截断后的，和 `/api/v0/tasks` 一样会偏低。

**只有 `total_state_counts` 是全量的**（GCS 遍历全部存储条目统计，不受 filter/limit）。

### 11.20 PENDING_NODE_ASSIGNMENT vs PENDING_ARGS_FETCH 状态可见性

#### 两个容易混淆的状态

| | `PENDING_NODE_ASSIGNMENT` (枚举值 2) | `PENDING_ARGS_FETCH` (枚举值 4) |
|---|---|---|
| proto 标注 | — | "sub-state of PENDING_NODE_ASSIGNMENT, **metrics only**" |
| 上报者 | Owner (CoreWorker) → GCS | Raylet → Prometheus metrics |
| GCS 可见？ | ✅ 是 | ❌ 否 |
| Dashboard 可见？ | ✅ 是 | ❌ 否 |
| 含义 | Owner 已解析依赖，等待 Raylet 分配节点 | Raylet 已分配节点，正在下载依赖到本地 object store |

`PENDING_NODE_ASSIGNMENT` 是 GCS Task Event 中可见的状态——Owner 侧通过 `TaskManager::SetTaskStatus` 上报。

`PENDING_ARGS_FETCH` 是 `PENDING_NODE_ASSIGNMENT` 的**子状态**——Raylet 内部的 Prometheus metrics 拆分的细粒度状态，不上报 GCS。GCS 的 `state_ts_ns` map 中永远不会出现 `PENDING_ARGS_FETCH`(4) 和 `PENDING_OBJ_STORE_MEM_AVAIL`(3)。

proto 定义（`common.proto:903-911`）：
```protobuf
// This state is a sub-state of PENDING_NODE_ASSIGNMENT and used for metrics only.
PENDING_OBJ_STORE_MEM_AVAIL = 3;
// This state is a sub-state of PENDING_NODE_ASSIGNMENT and used for metrics only.
PENDING_ARGS_FETCH = 4;
```

前端映射表中虽然有它们的合并规则（映射到 `PENDING_NODE_ASSIGNMENT`），但实际是**死代码**——永远不会被触发，因为 GCS 返回的数据中不会包含这些状态。

GCS 中一个 task 停在 `PENDING_NODE_ASSIGNMENT` 时，Raylet 内部可能处于 `PENDING_ARGS_FETCH`（正在拉参数）或 `PENDING_OBJ_STORE_MEM_AVAIL`（等 object store 内存），但 GCS 和 Dashboard 都看不到，只能通过 Raylet 的 Prometheus metrics 区分。

#### PENDING_ACTOR_TASK_ARGS_FETCH（枚举值 6）是另一个状态

| | `PENDING_ARGS_FETCH` (4) | `PENDING_ACTOR_TASK_ARGS_FETCH` (6) |
|---|---|---|
| 上报者 | Raylet (metrics only) | **Actor Worker (CoreWorker)** |
| GCS 可见？ | ❌ 否 | ✅ **是** |
| Dashboard 可见？ | ❌ 否 | ✅ 是 |
| 含义 | Raylet 侧下载依赖到 object store | Actor Worker 收到 task 但在等 ObjectRef 参数 |
| 代码位置 | `lease_dependency_manager.h` | `ordered/unordered_actor_task_execution_queue.cc` |

名字相似但完全不同的状态。GCS 不可见的是 `PENDING_ARGS_FETCH`（4），不是 `PENDING_ACTOR_TASK_ARGS_FETCH`（6）。

Actor Task 完整状态流转：
```
PENDING_ARGS_AVAIL (Submitter, include_task_info=true)
  → SUBMITTED_TO_WORKER (Submitter, include_task_info=true)
    → PENDING_ACTOR_TASK_ARGS_FETCH (Actor Worker, include_task_info=false)     ← 有 ObjectRef 依赖时
      → PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY (Actor Worker, include_task_info=false)
        → RUNNING (Executor, include_task_info=false)
          → FINISHED/FAILED (Submitter, include_task_info=false)
```

### 11.21 Progress Bar 状态合并完整链路

Progress Bar 的状态合并涉及三个文件，三层映射：

#### 第 1 层：原始状态 → 前端 TaskStatus（`useJobProgress.ts:29-48`）

```typescript
const TASK_STATE_NAME_TO_PROGRESS_KEY: Record<TypeTaskStatus, TaskStatus> = {
  PENDING_ARGS_AVAIL:                          TaskStatus.PENDING_ARGS_AVAIL,
  PENDING_NODE_ASSIGNMENT:                     TaskStatus.PENDING_NODE_ASSIGNMENT,
  PENDING_OBJ_STORE_MEM_AVAIL:                 TaskStatus.PENDING_NODE_ASSIGNMENT,   // 合并 (死代码)
  PENDING_ARGS_FETCH:                          TaskStatus.PENDING_NODE_ASSIGNMENT,   // 合并 (死代码)
  SUBMITTED_TO_WORKER:                          TaskStatus.SUBMITTED_TO_WORKER,
  PENDING_ACTOR_TASK_ARGS_FETCH:               TaskStatus.SUBMITTED_TO_WORKER,       // 合并
  PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY:  TaskStatus.SUBMITTED_TO_WORKER,       // 合并
  RUNNING:                                      TaskStatus.RUNNING,
  RUNNING_IN_RAY_GET:                          TaskStatus.RUNNING,                   // 合并 (死代码)
  RUNNING_IN_RAY_WAIT:                         TaskStatus.RUNNING,                   // 合并 (死代码)
  FINISHED:                                     TaskStatus.FINISHED,
  FAILED:                                       TaskStatus.FAILED,
  GETTING_AND_PINNING_ARGS:                    TaskStatus.RUNNING,                   // 合并 (死代码)
  NIL:                                          TaskStatus.UNKNOWN,
};
```

把 14 个原始状态合并为 7 个前端 TaskStatus。其中 `PENDING_OBJ_STORE_MEM_AVAIL`、`PENDING_ARGS_FETCH`、`RUNNING_IN_RAY_GET`、`RUNNING_IN_RAY_WAIT`、`GETTING_AND_PINNING_ARGS` 的映射规则实际是死代码——GCS 返回的数据中永远不会包含这些状态。

#### 第 2 层：前端 TaskStatus → Progress 字段名（`useJobProgress.ts:50-58`）

```typescript
const TaskStatusToTaskProgressMapping: Record<TaskStatus, keyof TaskProgress> = {
  PENDING_ARGS_AVAIL:      "numPendingArgsAvail",
  PENDING_NODE_ASSIGNMENT: "numPendingNodeAssignment",
  SUBMITTED_TO_WORKER:     "numSubmittedToWorker",
  RUNNING:                 "numRunning",
  FINISHED:                "numFinished",
  FAILED:                  "numFailed",
  UNKNOWN:                 "numUnknown",
};
```

#### 第 3 层：Progress 字段名 → Progress Bar segment（`TaskProgressBar.tsx:34-73`）

```typescript
const progress: ProgressBarSegment[] = [
  { label: "Finished",                 value: numFinished },
  { label: "Failed",                   value: numFailed },
  { label: "Running",                  value: numRunning },
  { label: "Waiting for scheduling",   value: numPendingNodeAssignment + numSubmittedToWorker },  // 再次合并
  { label: "Waiting for dependencies", value: numPendingArgsAvail },
  { label: "Cancelled",                value: numCancelled },
  { label: "Unknown",                  value: numUnknown },
];
```

第 3 层把 `numPendingNodeAssignment` 和 `numSubmittedToWorker` **再次合并**为 "Waiting for scheduling"。

#### formatStateCountsToProgress 函数（`useJobProgress.ts:227-241`）

```typescript
const formatStateCountsToProgress = (stateCounts: { [stateName: string]: number }) => {
  const formattedProgress: TaskProgress = {};
  Object.entries(stateCounts).forEach(([state, count]) => {
    // 第 1 层：原始状态名 → 前端 TaskStatus
    const taskStatus: TaskStatus =
      TASK_STATE_NAME_TO_PROGRESS_KEY[state as TypeTaskStatus];
    // 第 2 层：前端 TaskStatus → Progress 字段名
    const key: keyof TaskProgress =
      TaskStatusToTaskProgressMapping[taskStatus] ?? "numUnknown";
    // 累加
    formattedProgress[key] = (formattedProgress[key] ?? 0) + count;
  });
  return formattedProgress;
};
```

#### 完整合并链路示例（以 PENDING_ACTOR_TASK_ARGS_FETCH 为例）

```
total_state_counts: { "PENDING_ACTOR_TASK_ARGS_FETCH": 50, "RUNNING": 30, "FINISHED": 10000 }

→ formatStateCountsToProgress:
    state = "PENDING_ACTOR_TASK_ARGS_FETCH"
    → 第 1 层: TASK_STATE_NAME_TO_PROGRESS_KEY["PENDING_ACTOR_TASK_ARGS_FETCH"]
      = TaskStatus.SUBMITTED_TO_WORKER
    → 第 2 层: TaskStatusToTaskProgressMapping[SUBMITTED_TO_WORKER]
      = "numSubmittedToWorker"
    → formattedProgress["numSubmittedToWorker"] += 50

    state = "RUNNING"
    → 第 1 层: TaskStatus.RUNNING
    → 第 2 层: "numRunning"
    → formattedProgress["numRunning"] += 30

    state = "FINISHED"
    → 第 1 层: TaskStatus.FINISHED
    → 第 2 层: "numFinished"
    → formattedProgress["numFinished"] += 10000

→ TaskProgressBar:
    第 3 层:
    "Finished"               = numFinished = 10000
    "Failed"                 = numFailed = 0
    "Running"                = numRunning = 30
    "Waiting for scheduling" = numPendingNodeAssignment + numSubmittedToWorker = 0 + 50 = 50
    "Waiting for dependencies" = numPendingArgsAvail = 0
    "Unknown"                = numUnknown = 0
```

`PENDING_ACTOR_TASK_ARGS_FETCH` 最终显示在 Progress Bar 的 **"Waiting for scheduling"** segment 中。

#### 完整状态→Progress Bar segment 映射表

| 原始状态 | 枚举值 | GCS 可见？ | 第 1 层合并 → | 第 2 层映射 | 第 3 层 Progress Bar segment |
|----------|--------|-----------|--------------|------------|-------------------------------|
| `NIL` | 0 | — | UNKNOWN | numUnknown | Unknown |
| `PENDING_ARGS_AVAIL` | 1 | ✅ | PENDING_ARGS_AVAIL | numPendingArgsAvail | Waiting for dependencies |
| `PENDING_NODE_ASSIGNMENT` | 2 | ✅ | PENDING_NODE_ASSIGNMENT | numPendingNodeAssignment | Waiting for scheduling |
| `PENDING_OBJ_STORE_MEM_AVAIL` | 3 | ❌ | PENDING_NODE_ASSIGNMENT (死代码) | numPendingNodeAssignment | Waiting for scheduling |
| `PENDING_ARGS_FETCH` | 4 | ❌ | PENDING_NODE_ASSIGNMENT (死代码) | numPendingNodeAssignment | Waiting for scheduling |
| `SUBMITTED_TO_WORKER` | 5 | ✅ | SUBMITTED_TO_WORKER | numSubmittedToWorker | Waiting for scheduling |
| `PENDING_ACTOR_TASK_ARGS_FETCH` | 6 | ✅ | SUBMITTED_TO_WORKER | numSubmittedToWorker | Waiting for scheduling |
| `PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY` | 7 | ✅ | SUBMITTED_TO_WORKER | numSubmittedToWorker | Waiting for scheduling |
| `RUNNING` | 8 | ✅ | RUNNING | numRunning | Running |
| `RUNNING_IN_RAY_GET` | 9 | ❌ | RUNNING (死代码) | numRunning | Running |
| `RUNNING_IN_RAY_WAIT` | 10 | ❌ | RUNNING (死代码) | numRunning | Running |
| `FINISHED` | 11 | ✅ | FINISHED | numFinished | Finished |
| `FAILED` | 12 | ✅ | FAILED | numFailed | Failed |
| `GETTING_AND_PINNING_ARGS` | 13 | ❌ | RUNNING (死代码) | numRunning | Running |

**Progress Bar 7 个 segment 的最终组成：**

| segment | 包含的原始状态 | 备注 |
|---------|-------------|------|
| Finished | FINISHED | |
| Failed | FAILED | |
| Running | RUNNING, RUNNING_IN_RAY_GET (死), RUNNING_IN_RAY_WAIT (死), GETTING_AND_PINNING_ARGS (死) | |
| Waiting for scheduling | PENDING_NODE_ASSIGNMENT, PENDING_OBJ_STORE_MEM_AVAIL (死), PENDING_ARGS_FETCH (死), SUBMITTED_TO_WORKER, PENDING_ACTOR_TASK_ARGS_FETCH, PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY | 6 个状态合并 |
| Waiting for dependencies | PENDING_ARGS_AVAIL | |
| Cancelled | — | 前端独立处理，GCS 无对应状态 |
| Unknown | NIL | |

---

### 11.22 events_by_task / num_after_truncation / num_filtered 精确关系

#### 三个字段的定义

| 字段 | 含义 | 计算位置 | 计算公式 |
|------|------|---------|---------|
| `events_by_task` | GCS gRPC 返回的 `repeated TaskEvents`，通过 `filter_fn` 且未被 GCS limit 截断的条目列表 | **GCS 端** `gcs_task_manager.cc:606-608` | `reply->add_events_by_task()` 逐条加入 |
| `num_after_truncation` | GCS 返回的 `events_by_task` 条数（GCS 截断后**剩余**的数量，不是被截断的数量） | **Dashboard 端** `state_aggregator.py:325` | `len([protobuf_to_task_state_dict(msg) for msg in reply.events_by_task])` |
| `num_filtered` | Dashboard 侧 `do_filter` 后、`islice` 前的条数 | **Dashboard 端** `state_aggregator.py:330-331` | `len(do_filter(result, option.filters, TaskState, option.detail))` |

#### 计算顺序（state_aggregator.py:319-334）

```python
# Step 1: GCS 返回的 events_by_task → 转为 Python dict 列表
result = [
    protobuf_to_task_state_dict(message) for message in reply.events_by_task
]

# Step 2: num_after_truncation = GCS 返回的条数
num_after_truncation = len(result)

# Step 3: total = events_by_task 条数 + 被 drop 的总数
num_total = len(result) + reply.num_status_task_events_dropped

# Step 4: Dashboard 侧 do_filter（精确过滤：类型转换 + 大小写不敏感）
result = do_filter(result, option.filters, TaskState, option.detail)

# Step 5: num_filtered = do_filter 之后的条数（islice 之前）
num_filtered = len(result)

# Step 6: sort + islice 最终截断
result.sort(key=lambda entry: entry["task_id"])
result = list(islice(result, option.limit))   # ← result.result 传给 to_summary_by_func_name
```

**关键时序**：`num_filtered` 在 Step 5 计算（islice 之前），`to_summary_by_func_name` 在 Step 6 之后使用 `result.result`（islice 之后）。在默认配置下两者相等（GCS limit = islice limit = 10000），但若 `RAY_MAX_LIMIT_FROM_DATA_SOURCE` > `RAY_MAX_LIMIT_FROM_API_SERVER` 则可能不一致。详见 §11.26。

#### 两次过滤的详细逻辑

##### 第一层：GCS filter_fn（`gcs_task_manager.cc:525-587`）

GCS 遍历该 job 的所有存储条目时，逐条用 `filter_fn` 判断是否放入 `events_by_task`：

```cpp
auto filter_fn = [&filters](const rpc::TaskEvents &task_event) {
    // 条件 1: 没有 task_info → 过滤（僵尸条目）
    if (!task_event.has_task_info()) {
        return false;
    }
    // 条件 2: driver task 且 exclude_driver=true → 过滤
    if (filters.exclude_driver() &&
        task_event.task_info().type() == rpc::TaskType::DRIVER_TASK) {
        return false;
    }
    // 条件 3: task_id 过滤（按 protobuf 字段，大小写敏感）
    if (filters.task_filters_size() > 0) {
        if (!std::all_of(...)) { return false; }
    }
    // 条件 4: job_id 过滤
    if (filters.job_filters_size() > 0) {
        if (!std::all_of(...)) { return false; }
    }
    // 条件 5: actor_id 过滤
    if (filters.actor_filters_size() > 0) {
        if (!std::all_of(...)) { return false; }
    }
    // 条件 6: task_name 过滤（大小写不敏感）
    if (filters.task_name_filters_size() > 0) {
        if (!std::all_of(...)) { return false; }
    }
    // 条件 7: state 过滤（按最新状态匹配）
    if (filters.state_filters_size() > 0) {
        if (!std::all_of(...)) { return false; }
    }
    return true;  // 全部通过
};
```

特点：
- 基于 protobuf 字段直接匹配
- **不检查 `has_state_updates()`**，只检查 `has_task_info()`
- `has_task_info()` 为 false 的"僵尸条目"在这里被硬过滤，不会进入 `events_by_task`、不会计入 `num_after_truncation`、不会计入 `total`
- 被过滤的条目计入 `num_filtered_on_gcs`（但不带 commit 时 Dashboard 不返回这个字段）
- 当传入 `job_id` 时，GCS 侧先通过 `job_index_` 索引预筛选该 job 的条目，再由 `filter_fn` 条件 4 二次确认

##### 第二层：Dashboard do_filter（`state_api_utils.py:193-243`）

Dashboard 将 GCS 返回的 `events_by_task` 从 protobuf 转为 Python dict 后，再用 `do_filter` 精确过滤：

```python
def do_filter(data, filters, state_dataclass, detail):
    filters = convert_filters_type(filters, state_dataclass)  # 类型转换
    result = []
    for datum in data:
        match = True
        for filter_column, filter_predicate, filter_value in filters:
            filter_column = filter_column.lower()
            if filter_column not in filterable_columns:
                raise ValueError(...)
            if filter_column not in datum:
                match = False                          # 字段不存在 → 不匹配
            elif filter_predicate == "=":
                if isinstance(filter_value, str) and isinstance(datum[filter_column], str):
                    match = datum[filter_column].lower() == filter_value.lower()  # 字符串: 大小写不敏感
                elif isinstance(filter_value, str) and isinstance(datum[filter_column], bool):
                    match = datum[filter_column] == convert_string_to_type(filter_value, bool)
                elif isinstance(filter_value, str) and isinstance(datum[filter_column], int):
                    match = datum[filter_column] == convert_string_to_type(filter_value, int)  # int 类型转换
                else:
                    match = datum[filter_column] == filter_value
            elif filter_predicate == "!=":
                if isinstance(filter_value, str) and isinstance(datum[filter_column], str):
                    match = datum[filter_column].lower() != filter_value.lower()
                else:
                    match = datum[filter_column] != filter_value
            if not match:
                break
        if match:
            result.append(datum)
    return result
```

特点：
- 基于 Python dict 字段匹配
- 支持类型转换（str→int、str→bool）
- 字符串匹配**大小写不敏感**
- **不会检查 `has_task_info` 或 `has_state_updates`**（GCS 已过滤掉无 `task_info` 的）
- 只会减少条目，不会增加

##### 两层过滤对比

| | GCS filter_fn | Dashboard do_filter |
|---|---|---|
| 代码位置 | `gcs_task_manager.cc:525-587` | `state_api_utils.py:193-243` |
| 数据格式 | protobuf `TaskEvents` | Python dict（转换后） |
| 过滤无 `task_info` | ✅ 硬过滤 | ❌（GCS 已过滤，不存在） |
| `exclude_driver` | ✅ | ✅ |
| `job_id` 匹配 | protobuf 字段，大小写敏感 | Python dict，str→int 类型转换 |
| `task_id` / `actor_id` | protobuf 字段，大小写敏感 | Python dict，str→int 类型转换 |
| `name` 匹配 | protobuf 字段，大小写不敏感 | Python dict，大小写不敏感 |
| `state` 匹配 | protobuf 枚举，大小写不敏感 | Python dict，大小写不敏感 |
| 精度 | 粗筛（基于 protobuf 原始字段） | 精筛（类型转换 + 大小写不敏感） |
| 被过滤的条目去向 | `num_filtered_on_gcs`（不带 commit 不返回） | 不计入 `num_filtered`，但在 `num_after_truncation` 中 |

##### 实际场景示例（job_id 查询）

```
GCS 存储中该 job 的所有 entry (job_index_[25000000])
    │
    │ GCS filter_fn:
    │   ├── !has_task_info() → 过滤 (僵尸条目)     → num_filtered_on_gcs++
    │   ├── driver task → 过滤 (exclude_driver=true)
    │   ├── job_id != 25000000 → 过滤               (但 job_index_ 已预筛选, 不会到这里)
    │   └── 通过 → ↓
    │       GCS limit (10000) 截断
    │       ├── count < 10000 → events_by_task     → num_after_truncation = 1645
    │       └── count >= 10000 → 截断 (未触发, 1645 < 10000)
    │
    ▼
Dashboard do_filter:
    result = [protobuf_to_dict(msg) for msg in events_by_task]   # 1645 条
    result = do_filter(result, [("job_id", "=", "25000000")])
    # job_id 在 protobuf 中是 bytes, 转为 dict 后是 hex 字符串
    # do_filter 做 str 匹配: datum["job_id"].lower() == "25000000".lower()
    # GCS 已通过 job_index_ 精确筛选, do_filter 不会额外过滤
    → num_filtered = 1645 (不变)

islice(1645, 10000) → 不截断 → result.result = 1645 条

to_summary_by_func_name(tasks=result.result) → node_id_to_summary 基于 1645 条统计
```

两层过滤在此场景中没有过滤掉任何条目（GCS 的 `job_index_` 已精确匹配，Dashboard 的 `do_filter` 只是二次确认），所以 `num_after_truncation = num_filtered = len(result.result) = 1645`。

#### filter_fn 不检查 has_state_updates

**GCS `filter_fn` 只检查 `has_task_info()`，不检查 `has_state_updates()`。** 只要一个 entry 有 `task_info` 且匹配其他过滤条件，即使没有 `state_updates`（如只有 `profile_events`），也会通过 `filter_fn`，进入 `events_by_task`，计入 `num_after_truncation`。

```cpp
// gcs_task_manager.cc:525-529
auto filter_fn = [&filters](const rpc::TaskEvents &task_event) {
    if (!task_event.has_task_info()) {
        return false;   // ← 唯一的隐式硬过滤条件
    }
    // ... driver/task_id/job_id/actor_id/name/state 过滤
};
```

无 `state_updates` 但有 `task_info` 的条目进入 `events_by_task` 后，在 `to_summary_by_func_name` 中被统计为 `state_counts["NIL"]`（映射为 `numUnknown`），仍计入 `total_tasks`。详见 §11.25。

#### 完整数据流图

```
GCS 存储中该 job 的所有 task events (job_index_[job_id] → flat_hash_set)
    │
    │ GCS 遍历 (gcs_task_manager.cc:593-616)
    │
    ├── has_state_updates? → total_state_counts[latest_state]++ (在 filter/limit 之前)
    │   (不带 commit 时此统计不返回给 Dashboard)
    │
    ├── filter_fn 不通过 (无 task_info / driver / job_id 不匹配等)
    │   → num_filtered_on_gcs++
    │   → 不进入 events_by_task, 不计入 num_after_truncation, 不计入 total
    │
    ├── filter_fn 通过 + count < GCS limit (10000)
    │   → reply->add_events_by_task()   ──→ events_by_task
    │
    └── filter_fn 通过 + count >= GCS limit
        → num_limit_truncated++
        → num_status_event_limit++ (如果有 state_updates)
        → 不进入 events_by_task, 但 num_status_event_limit 累加到 num_status_task_events_dropped → 间接计入 total

    events_by_task (GCS 返回)
        │
        ▼
Dashboard (state_aggregator.py:319-334)
    result = [protobuf_to_dict(msg) for msg in reply.events_by_task]
    num_after_truncation = len(result)                      ← = len(events_by_task)
    num_total = len(result) + reply.num_status_task_events_dropped  ← = total
    result = do_filter(result, ...)
    num_filtered = len(result)                              ← islice 之前
    result = islice(result, option.limit)                  ← 最终 result.result
```

---

### 11.23 task_info 与 state_updates 的生命周期

#### protobuf 结构

```protobuf
// src/ray/protobuf/gcs.proto:218-231
message TaskEvents {
    bytes task_id = 1;                          // 始终有
    int32 attempt_number = 2;                   // 始终有
    optional TaskInfoEntry task_info = 3;        // 可选 — 任务元信息
    optional TaskStateUpdate state_updates = 4;  // 可选 — 状态变更记录
    optional ProfileEvents profile_events = 5;   // 可选 — profiling 数据
    bytes job_id = 6;                            // 始终有
}
```

`TaskStateUpdate` 包含（`gcs.proto:196-215`）：

```protobuf
message TaskStateUpdate {
    optional bytes node_id = 1;
    optional bytes worker_id = 8;
    optional RayErrorInfo error_info = 9;
    optional TaskLogInfo task_log_info = 10;
    optional string actor_repr_name = 11;
    optional int32 worker_pid = 12;
    optional bool is_debugger_paused = 13;
    map<int32, int64> state_ts_ns = 14;  // key=TaskStatus枚举值, value=纳秒时间戳
}
```

#### Worker 两种 Event 类型

Worker 侧有**两种独立的 Event 类**，它们填充 `TaskEvents` 的不同字段：

**a) TaskStatusEvent — 状态变更事件**

```cpp
// task_event_buffer.cc:78-115
void TaskStatusEvent::ToRpcTaskEvents(rpc::TaskEvents *rpc_task_events) {
    rpc_task_events->set_task_id(...);
    rpc_task_events->set_job_id(...);
    rpc_task_events->set_attempt_number(...);

    // task_info: 仅当 include_task_info=true 时设置
    if (task_spec_) {    // task_spec_ 仅在 include_task_info=true 时非 null
        gcs::FillTaskInfo(rpc_task_events->mutable_task_info(), *task_spec_);
    }

    // state_updates: 总是创建，但 state_ts_ns 可能为空
    auto dst_state_update = rpc_task_events->mutable_state_updates();  // 总是调用
    gcs::FillTaskStatusUpdateTime(task_status_, timestamp_, dst_state_update);
}
```

`FillTaskStatusUpdateTime`（`protobuf_utils.cc:351-358`）：

```cpp
void FillTaskStatusUpdateTime(const ray::rpc::TaskStatus &task_status,
                              int64_t timestamp,
                              ray::rpc::TaskStateUpdate *state_updates) {
    if (task_status == rpc::TaskStatus::NIL) {
        return;  // ← 不写入 state_ts_ns, 但 state_updates 字段已被 mutable_state_updates() 创建
    }
    (*state_updates->mutable_state_ts_ns())[task_status] = timestamp;
}
```

**b) TaskProfileEvent — profiling 事件**

```cpp
// task_event_buffer.cc:348-365
void TaskProfileEvent::ToRpcTaskEvents(rpc::TaskEvents *rpc_task_events) {
    auto profile_events = rpc_task_events->mutable_profile_events();
    rpc_task_events->set_task_id(...);
    rpc_task_events->set_job_id(...);
    rpc_task_events->set_attempt_number(...);
    // ← 不调用 mutable_task_info()
    // ← 不调用 mutable_state_updates()
}
```

#### include_task_info 的默认值和各调用点

`SetTaskStatus` 的默认值是 **false**（`task_manager.h:700`）：

```cpp
void SetTaskStatus(
    TaskEntry &task_entry,
    rpc::TaskStatus status,
    std::optional<worker::TaskStatusEvent::TaskStateUpdate> state_update = std::nullopt,
    bool include_task_info = false,    // ← 默认 false
    std::optional<int32_t> attempt_number = std::nullopt)
```

| 步骤 | 状态转换 | 角色 | include_task_info | 代码位置 |
|------|---------|------|:-:|------|
| 1 | → `PENDING_ARGS_AVAIL` (首次提交) | Submitter | **true** | `task_manager.cc:349` |
| 2 | → `PENDING_NODE_ASSIGNMENT` | Submitter | **false** (默认) | `task_manager.cc:1682` |
| 3 | → `SUBMITTED_TO_WORKER` | Submitter | **false** (默认) | `task_manager.cc:1697` |
| 4 | → `RUNNING` | Executor | **false** (默认, `task_events_executor_include_task_info=false`) | `core_worker.cc:3070` |
| 5 | → `FINISHED` (正常完成) | Submitter | **false** (默认) | `task_manager.cc:1053` |
| 6 | → `FAILED` (执行失败) | Submitter | **false** (默认) | `task_manager.cc:1047` |
| 7 | → `FAILED` (重试, 旧 attempt) | Submitter | **false** (默认) | `task_manager.cc:1190` |
| 8 | → `PENDING_ARGS_AVAIL` (重试新 attempt) | Submitter | **true** | `task_manager.cc:1232` |
| 9 | → `PENDING_ACTOR_TASK_ARGS_FETCH` | Actor Worker | **false** | `actor_task_execution_queue.cc:109/179` |
| 10 | → `PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY` | Actor Worker | **false** | `actor_task_execution_queue.cc:132/192` |
| 11 | → `NIL` (日志/调试器事件) | Executor | **false** | `core_worker.cc:4872/4896/4914` |

**只有步骤 1（首次提交）和步骤 8（重试新 attempt）携带 `task_info`。** 其余所有状态变更都不带 `task_info`。

#### state_updates 什么时候有

**几乎总是有。** 因为 `TaskStatusEvent::ToRpcTaskEvents` 总是调用 `mutable_state_updates()`：

| 场景 | `has_state_updates()` | `state_ts_ns` 是否为空 | `GetLatestTaskStatus` 返回 |
|------|:-:|:-:|------|
| 正常状态变更 (RUNNING/FINISHED 等) | true | 有内容 | 对应状态 |
| `NIL` 状态 (日志/调试器事件) | true | **空** | `NIL` |
| Profile event (无状态变更) | **false** | — | `NIL` |
| GCS 侧 `MarkTaskAttemptFailedIfNeeded` | true (被设置) | 有内容 (FAILED) | `FAILED` |

#### GCS MergeFrom 合并语义

GCS 收到同一 task attempt 的多次 event 时，通过 `MergeFrom` 合并（`gcs_task_manager.cc:177`）：

```cpp
existing_task.MergeFrom(task_events);
```

protobuf `MergeFrom` 语义：
- `task_info`：如果新 event 有 → 覆盖；如果新 event 没有 → **保留旧值**
- `state_updates.state_ts_ns`：map merge → 新状态加入，旧状态保留
- `profile_events`：追加

一个 task 的完整生命周期在 GCS 中合并后：

```
步骤1 (Submitter, PENDING_ARGS_AVAIL, include_task_info=true)
  → GCS entry: {task_info: ✓, state_updates: {1: ts1}}

步骤2 (Submitter, PENDING_NODE_ASSIGNMENT, include_task_info=false)
  → MergeFrom → GCS entry: {task_info: ✓(保留), state_updates: {1: ts1, 2: ts2}}

步骤4 (Executor, RUNNING, include_task_info=false)
  → MergeFrom → GCS entry: {task_info: ✓, state_updates: {1,2,8: ts4}}

步骤5 (Submitter, FINISHED, include_task_info=false)
  → MergeFrom → GCS entry: {task_info: ✓, state_updates: {1,2,8,11: ts5}}
```

#### 僵尸条目产生条件

僵尸条目 = 有 `state_updates` 但无 `task_info` 的 GCS 存储条目。

典型场景：Executor 先于 Submitter 到达 GCS：

```
Executor 上报 RUNNING (include_task_info=false)
  → GCS 创建 entry: {task_info: ✗, state_updates: {8: ts1}}  ← 僵尸!

Submitter 的 PENDING_ARGS_AVAIL (include_task_info=true) 还没到达 GCS
  → 该 entry 保持僵尸状态
  → filter_fn 过滤 (!has_task_info()) → 不进入 events_by_task
```

如果后续 Submitter 的带 `task_info` 的事件到达，`MergeFrom` 会补全 `task_info`，僵尸被修复。但如果带 `task_info` 的事件丢失（Worker buffer 溢出），僵尸永久存在。

修复方案：开启 `task_events_executor_include_task_info=true`（`ray_config_def.h:501`，默认 false），使 Executor 的 RUNNING 上报也带 `task_info`。

#### 有 task_info 但 state_ts_ns 为空的场景

极少见。需要 `include_task_info=true` 的事件到达（带 `task_info`），但其 `task_status=NIL`（不写入 `state_ts_ns`），且后续非 NIL 状态事件全部丢失。这在实际中几乎不会发生，因为 `include_task_info=true` 的调用点（步骤1: `PENDING_ARGS_AVAIL`，步骤8: 重试）都是非 NIL 状态。

---

### 11.24 total 中 limit 截断为何统计 has_state_updates

#### 代码

GCS `HandleGetTaskEvents` 遍历循环中（`gcs_task_manager.cc:606-618`）：

```cpp
if (limit < 0 || count++ < limit) {
    auto events = reply->add_events_by_task();     // 加入返回结果
    events->Swap(&task_event);
} else {
    // 被截断
    num_profile_event_limit += task_event.has_profile_events()
                                   ? task_event.profile_events().events_size()
                                   : 0;
    num_status_event_limit += task_event.has_state_updates() ? 1 : 0;  // ← 按 state_updates 计数
    num_limit_truncated++;
}

// 累加到 dropped
reply->set_num_status_task_events_dropped(reply->num_status_task_events_dropped() +
                                           num_status_event_limit);
```

#### 为什么统计 has_state_updates 而非 has_task_info

**原因 1：被截断的条目已通过 filter_fn（保证有 task_info）**

被 limit 截断的条目已经通过了 `filter_fn`，所以它们**一定有 `task_info`**。在这种条目上检查 `has_task_info` 永远为 true，没有意义。

```cpp
// 先过滤
if (!filter_fn(task_event)) {
    num_filtered++;
    continue;    // ← 不进入下面的 limit 逻辑
}

// 只有通过 filter_fn 的（一定有 task_info）才到达 limit 逻辑
if (limit < 0 || count++ < limit) {
    ...
} else {
    num_status_event_limit += task_event.has_state_updates() ? 1 : 0;
}
```

**原因 2：`num_status_task_events_dropped` 语义是"状态信息丢失数量"**

字段名中的 "status" 指的就是 `state_updates`（状态变更）。被截断且**有状态变更**的条目才会被计入，因为这代表一个有状态的 task 的状态信息因 limit 截断而对 Dashboard 不可见了。

**原因 3：无 state_updates 的条目截断不影响 Progress Bar**

通过 `filter_fn` 说明有 `task_info`。如果没有 `state_updates`（如只有 `profile_events`），那这个条目本来就没有状态信息可供 Progress Bar 统计，截断它不会造成"状态丢失"。

#### 被截断条目的分类

```
被 limit 截断的条目 (都通过了 filter_fn, 即都有 task_info)
    │
    ├── has_state_updates = true (有状态变更)
    │   → num_status_event_limit++    → 计入 num_status_task_events_dropped → 计入 total
    │   → 含义: 这个 task 的状态信息因截断丢失了
    │
    └── has_state_updates = false (无状态变更, 只有 profile_events)
        → num_status_event_limit 不增加
        → 不计入 num_status_task_events_dropped, 不计入 total
        → 但 num_limit_truncated++ (记录在 num_truncated 中)
        → 含义: 这个 task 没有状态信息, 截断不影响 Progress Bar 统计
```

#### total 完整公式

```
total = len(events_by_task)
      + job_summary.NumTaskAttemptsDropped()          # 历史被 drop 的 task attempt 总数
      + num_status_event_limit                        # 被 GCS limit 截断且有 state_updates 的条目数
```

Dashboard 侧（`state_aggregator.py:325-326`）：

```python
num_after_truncation = len(result)
num_total = len(result) + reply.num_status_task_events_dropped
```

其中 `reply.num_status_task_events_dropped` = GCS 侧 `NumTaskAttemptsDropped()` + `num_status_event_limit`。

---

### 11.25 node_id_to_summary 中 total_tasks / state_counts 统计来源

#### 数据来源说明

`node_id_to_summary` 是从 `result.result` 统计的，而 `result.result` 经过了 **do_filter + islice** 两步处理，是 `num_after_truncation` 的子集：

```
events_by_task (GCS返回)          → num_after_truncation = len(这部分)
  → do_filter                     → num_filtered = len(这部分)
    → islice(limit)               → result.result = 最终传给 to_summary_by_func_name 的
      → to_summary_by_func_name   → node_id_to_summary
```

在默认配置下（两个 limit 都是 10000），`do_filter` 不过滤 + `islice` 不截断时，三者的数据范围相等：

```
num_after_truncation = num_filtered = len(result.result) = 1645
node_id_to_summary 基于 1645 条统计
```

但严格来说，`node_id_to_summary` 基于 `result.result`（islice 之后），而非 `num_after_truncation`（islice 之前）。

#### 调用链

```
summarize_tasks (state_aggregator.py:574-625)
  → result = await self.list_tasks(limit=RAY_MAX_LIMIT_FROM_API_SERVER)
  → summary_results = TaskSummaries.to_summary_by_func_name(tasks=result.result)
  → summary = StateSummary(node_id_to_summary={"cluster": summary_results})
  → return SummaryApiResponse(result=summary, ...)
```

`result.result` 是经过 GCS filter + GCS limit + Dashboard do_filter + Dashboard islice 后的**最终 task dict 列表**。

#### to_summary_by_func_name 逻辑

`python/ray/util/state/common.py:1043-1079`：

```python
@classmethod
def to_summary_by_func_name(cls, *, tasks: List[Dict]) -> "TaskSummaries":
    summary = {}
    total_tasks = 0
    total_actor_tasks = 0
    total_actor_scheduled = 0

    for task in tasks:                    # ← 遍历 result.result (islice 后的最终列表)
        key = task["func_or_class_name"]  # ← 来自 task_info, 按函数名分组
        if key not in summary:
            summary[key] = TaskSummaryPerFuncOrClassName(
                func_or_class_name=task["func_or_class_name"],
                type=task["type"],
            )
        task_summary = summary[key]

        # ===== state_counts 统计 =====
        state = task["state"]             # ← 来自 state_updates 的最新状态
        if state not in task_summary.state_counts:
            task_summary.state_counts[state] = 0
        task_summary.state_counts[state] += 1   # ← 不区分有没有 state_updates, 全部统计

        # ===== total_tasks / total_actor_tasks 统计 =====
        type_enum = TaskType.DESCRIPTOR.values_by_name[task["type"]].number
        if type_enum == TaskType.NORMAL_TASK:
            total_tasks += 1
        elif type_enum == TaskType.ACTOR_CREATION_TASK:
            total_actor_scheduled += 1
        elif type_enum == TaskType.ACTOR_TASK:
            total_actor_tasks += 1

    return TaskSummaries(
        summary=summary,
        total_tasks=total_tasks,
        total_actor_tasks=total_actor_tasks,
        total_actor_scheduled=total_actor_scheduled,
        summary_by="func_name",
    )
```

#### task["state"] 的来源

`protobuf_to_task_state_dict`（`common.py:1605-1725`）将 protobuf `TaskEvents` 转换为 dict：

```python
events = []
if "state_ts_ns" in state_updates:
    state_ts_ns = state_updates["state_ts_ns"]
    for state_name, state in TaskStatus.items():   # 按枚举值从小到大遍历
        key = str(state)
        if key in state_ts_ns:
            ts_ms = int(state_ts_ns[key]) // 1e6
            events.append({"state": state_name, "created_ms": ts_ms})

task_state["events"] = events
if len(events) > 0:
    latest_state = events[-1]["state"]    # ← 取最后一个（最高编号的状态 = 最新状态）
else:
    latest_state = "NIL"                   # ← 无 state_updates 时
task_state["state"] = latest_state
```

`events` 列表按 `TaskStatus` 枚举值从小到大排列（`TaskStatus.items()` 按枚举值遍历），最后一个就是编号最大的状态 = 最新状态。与 GCS 侧 `GetLatestTaskStatus` 逻辑一致。

| 条件 | `task["state"]` | 在 state_counts 中 | 在 total_tasks 中 |
|------|----------------|-------------------|------------------|
| 有 state_updates + 有正常状态 | 正常状态名 (RUNNING 等) | ✅ 统计到对应状态 | ✅ |
| 有 state_updates + state_ts_ns 为空 (NIL 事件) | "NIL" | ✅ 统计到 state_counts["NIL"] | ✅ |
| 无 state_updates (纯 profile) | "NIL" | ✅ 统计到 state_counts["NIL"] | ✅ |

**所有条目都被统计，不因缺少 state_updates 而遗漏。** "NIL" 状态在前端映射为 `numUnknown`（`useJobProgress.ts:44`），仍计入 Progress Bar 的 segmentTotal。

#### 验证示例

```json
{
  "node_id_to_summary": {
    "cluster": {
      "summary": {
        "_map_task": {
          "func_or_class_name": "_map_task",
          "type": "NORMAL_TASK",
          "state_counts": {
            "RUNNING": 1590,
            "PENDING_NODE_ASSIGNMENT": 41,
            "SUBMITTED_TO_WORKER": 3
          }
        },
        "QwenVLCPUPreprocessActor.preprocess_video": {
          "func_or_class_name": "QwenVLCPUPreprocessActor.preprocess_video",
          "type": "ACTOR_TASK",
          "state_counts": {
            "SUBMITTED_TO_WORKER": 11
          }
        }
      },
      "total_tasks": 1634,           // = 1590 + 41 + 3 (NORMAL_TASK)
      "total_actor_tasks": 11,       // = 11 (ACTOR_TASK)
      "total_actor_scheduled": 0,    // = 0 (无 ACTOR_CREATION_TASK)
      "summary_by": "func_name"
    }
  }
}
```

验证：`total_tasks + total_actor_tasks + total_actor_scheduled = 1634 + 11 + 0 = 1645 = num_filtered`。

---

### 11.26 不带 commit 时 Unaccounted 的精确计算

#### 前端数据来源（不带 commit）

`useJobProgress.ts:82-99`：

```typescript
const totalStateCounts = rsp.data.data.result.total_state_counts;  // ← null (不带 commit)
return {
    summary,                                           // ← node_id_to_summary
    totalTasks: rsp.data.data.result.num_filtered,      // ← num_filtered, 不是 total!
    totalStateCounts,                                   // ← null
};
```

`useJobProgress.ts:127-149`：

```typescript
// 路径 B: 没有 total_state_counts 时
const summed = (data?.summary ?? []).reduce((acc, task) => {
    Object.entries(task.progress).forEach(([k, count]) => {
        acc[key] = (acc[key] ?? 0) + count;
    });
    return acc;
}, {} as TaskProgress);

return {
    progress: progressFromTotalStateCounts ?? summed,      // ← summed (路径 B)
    totalTasks: totalFromStateCounts ?? data?.totalTasks,  // ← num_filtered (路径 B)
};
```

**前端用的 `totalTasks` = `num_filtered`，不是 API 返回的 `total`（66M）。** 那 66M 的 `total` 字段前端 Progress Bar 根本没用到。

#### Unaccounted 计算

`ProgressBar.tsx:85-103`：

```typescript
const segmentTotal = progress.reduce((acc, { value }) => acc + value, 0);
const finalTotal = total ?? segmentTotal;

const segments =
    segmentTotal < finalTotal
        ? [...progress, { value: finalTotal - segmentTotal, label: "Unaccounted" }]
        : progress;
```

**Unaccounted = `totalTasks` - `segmentTotal`**，仅当 `segmentTotal < totalTasks` 时显示。

#### 默认配置下 Unaccounted 恒为 0

在默认配置下（`RAY_MAX_LIMIT_FROM_DATA_SOURCE=10000`，`RAY_MAX_LIMIT_FROM_API_SERVER=10000`）：

```
GCS limit (RAY_MAX_LIMIT_FROM_DATA_SOURCE) = 10000
Dashboard islice limit (RAY_MAX_LIMIT_FROM_API_SERVER) = 10000

GCS 返回 events_by_task ≤ 10000 条
  → num_after_truncation = len(events_by_task) ≤ 10000

Dashboard do_filter 后:
  → num_filtered = len(do_filter result) ≤ num_after_truncation ≤ 10000

islice(≤10000, 10000) → 不截断
  → result.result = num_filtered 条

to_summary_by_func_name(tasks=result.result) 遍历 num_filtered 条
  → 每条都计入 state_counts (即使 state="NIL" → numUnknown)
  → summed = sum(state_counts) = num_filtered

前端:
  totalTasks = num_filtered
  segmentTotal = summed = num_filtered
  Unaccounted = num_filtered - num_filtered = 0
```

**`num_filtered` 和 `summed` 来源于同一批 `result.result`**，`to_summary_by_func_name` 遍历每一条都会计入 `state_counts`（即使 state="NIL" 也会映射到 `numUnknown`），所以两者恒等，**Unaccounted = 0**。

#### 产生 Unaccounted > 0 的条件

只有当 **`RAY_MAX_LIMIT_FROM_DATA_SOURCE` > `RAY_MAX_LIMIT_FROM_API_SERVER`** 时才可能产生：

```
RAY_MAX_LIMIT_FROM_DATA_SOURCE=50000   (GCS 端允许返回 50000 条)
RAY_MAX_LIMIT_FROM_API_SERVER=10000    (Dashboard 端 islice limit = 10000)

GCS 返回 30000 条 events_by_task
  → num_after_truncation = 30000

Dashboard do_filter 后 = 30000 (假设不过滤)
  → num_filtered = 30000              ← islice 之前计算

islice(30000, 10000) = 10000         ← 截断!
  → result.result = 10000 条

to_summary_by_func_name(tasks=result.result) 遍历 10000 条
  → summed = 10000

前端:
  totalTasks = num_filtered = 30000
  segmentTotal = summed = 10000
  Unaccounted = 30000 - 10000 = 20000
```

#### 服务端 warning

`state_aggregator.py:604-613` 在 `summarize_tasks` 中检测这个差异：

```python
if (
    summary_results.total_actor_scheduled
    + summary_results.total_actor_tasks
    + summary_results.total_tasks
    < result.num_filtered
):
    warnings = warnings or []
    warnings.append(
        "There is missing data in this aggregation. "
        "Possibly due to task data being evicted to preserve memory."
    )
```

当 `total_tasks + total_actor_tasks + total_actor_scheduled < num_filtered` 时，说明 `to_summary_by_func_name` 遍历的条目数 < `num_filtered`，即 islice 截断了。warning 提示"数据缺失可能是内存淘汰导致"，但实际原因是 **islice 截断**而非 GCS 淘汰。

#### 其他可能产生 Unaccounted 的场景

| 场景 | 机制 | 是否在默认配置下产生 |
|------|------|:-:|
| 两 limit 不一致 | `num_filtered` (islice 前) > `summed` (islice 后) | ❌ (默认两 limit 相等) |
| lineage 视图 parent 不可见 | `to_summary_by_lineage` 无法正确分组 parent 丢失的 task | 可能 (展开 Advanced Progress Bar 时) |
| 两个 API 请求时间差 | `progress` 和 `totalTasks` 来自不同请求 | ❌ (各自内部一致) |
| 前端使用 API 的 `total` 字段 | `total` = 66M, `summed` = 1645 → Unaccounted = 66M | ❌ (标准前端用 `num_filtered`) |

**结论：在默认配置 + 标准前端代码下，`/api/v0/tasks/summarize` 的 Unaccounted 恒为 0。** 如果实际看到较大的 Unaccounted，应检查：
1. 是否自定义修改了 `RAY_MAX_LIMIT_FROM_DATA_SOURCE` 或 `RAY_MAX_LIMIT_FROM_API_SERVER`
2. 是否展开了 Advanced Progress Bar（lineage 视图）
3. 是否有自定义前端代码使用了 API 的 `total` 字段而非 `num_filtered`

---

## 12. GCS stats_counter_ 详解

### 12.1 计数器定义

```cpp
// gcs_task_manager.h:36-44
enum GcsTaskManagerCounter : std::uint8_t {
  kTotalNumTaskEventsReported,       // 0: 累计收到的 task events 数
  kTotalNumTaskAttemptsDropped,      // 1: 累计被 dropped 的 task attempts 数
  kTotalNumProfileTaskEventsDropped, // 2: 累计被 dropped 的 profile events 数
  kNumTaskEventsStored,              // 3: 当前存储的 task attempts 数 (实时)
  kTotalNumActorCreationTask,        // 4: 累计 Actor 创建任务数
  kTotalNumActorTask,                // 5: 累计 Actor 任务数
  kTotalNumNormalTask,               // 6: 累计普通任务数
  kTotalNumDriverTask,               // 7: 累计 Driver 任务数
};
```

### 12.2 每个计数器的触发点和语义

#### kTotalNumTaskEventsReported（累计收到数）

每收到一个 `events_by_task` 条目就 +1。**累计值**，只增不减。统计的是 GCS 收到的 protobuf 条目数（经过 Worker 端聚合后的），不是原始 event 数。

#### kNumTaskEventsStored（当前存储数，实时）

```cpp
// AddNewTaskEvent: +1
// RemoveTaskAttempt: -1
```

实时值，精确反映当前存储中有多少个 task attempt 条目。被 `AddOrReplaceTaskEvent` 用于判断是否触发淘汰。

#### kTotalNumTaskAttemptsDropped（累计 dropped 数）

```cpp
// RemoveTaskAttempt: +1  (GCS 淘汰)
// RecordDataLossFromWorker: +1  (Worker 上报数据丢失)
```

累计值，只增不减。统计被 dropped 的 task attempt 总数（无论来自 GCS 淘汰还是 Worker 上报）。

#### kTotalNumProfileTaskEventsDropped（累计 profile 被丢弃数）

三个触发点：
- task attempt 被删除时，其携带的 profile events 一起丢失
- 已存在条目 merge 后 profile events 超过 per-task 限制（1000），截断最旧的
- Worker 上报 profile events 被丢弃的数量

#### kTotalNumActorCreationTask / kTotalNumActorTask / kTotalNumNormalTask / kTotalNumDriverTask（按类型累计数）

只统计 `attempt_number == 0` 的（首次执行，非重试）。一个 task 不管重试多少次，只计一次。

### 12.3 计数器的三个用途

| 用途 | 使用哪些计数器 | 说明 |
|------|--------------|------|
| **触发淘汰** | `kNumTaskEventsStored` | 唯一被用于逻辑判断的计数器 |
| **上报给查询方** | `JobTaskSummary` 的 per-job 计数器 | `HandleGetTaskEvents` 中返回 `num_status_task_events_dropped` |
| **Metrics 导出** | 所有计数器 | `RecordMetrics` 中导出到 gauge 和 usage stats |

```cpp
// RecordMetrics (gcs_task_manager.cc:714):
task_events_reported_gauge_.Record(counters[kTotalNumTaskEventsReported]);
task_events_dropped_gauge_.Record(counters[kTotalNumTaskAttemptsDropped], {{"Type", "STATUS_EVENT"}});
task_events_dropped_gauge_.Record(counters[kTotalNumProfileTaskEventsDropped], {{"Type", "PROFILE_EVENT"}});
task_events_stored_gauge_.Record(counters[kNumTaskEventsStored]);
// Usage stats: Actor/Normal/Driver 任务数
```

### 12.4 计数器总览

| 计数器 | 类型 | 语义 | 谁递增 | 谁递减 | 用途 |
|--------|------|------|--------|--------|------|
| `kTotalNumTaskEventsReported` | 累计 | GCS 收到的 task event 条目总数 | `RecordTaskEventData` | 无 | Metrics gauge |
| `kNumTaskEventsStored` | 实时 | 当前存储的 task attempt 数 | `AddNewTaskEvent` | `RemoveTaskAttempt` | **触发淘汰** + Metrics gauge |
| `kTotalNumTaskAttemptsDropped` | 累计 | 被 dropped 的 task attempts 总数 | `RemoveTaskAttempt` + `RecordDataLossFromWorker` | 无 | Metrics gauge |
| `kTotalNumProfileTaskEventsDropped` | 累计 | 被 dropped 的 profile events 总数 | `RemoveTaskAttempt` + `UpdateExistingTaskAttempt` | 无 | Metrics gauge |
| `kTotalNumActorCreationTask` | 累计 | Actor 创建任务总数 (attempt 0) | `AddNewTaskEvent` + `UpdateExistingTaskAttempt` | 无 | Usage stats |
| `kTotalNumActorTask` | 累计 | Actor 任务总数 (attempt 0) | 同上 | 无 | Usage stats |
| `kTotalNumNormalTask` | 累计 | 普通任务总数 (attempt 0) | 同上 | 无 | Usage stats |
| `kTotalNumDriverTask` | 累计 | Driver 任务总数 (attempt 0) | 同上 | 无 | Usage stats |

### 12.5 JobTaskSummary vs stats_counter_

`JobTaskSummary` 维护 per-job 的统计，与 `stats_counter_`（全局）互补：

```cpp
class JobTaskSummary {
  int64_t num_profile_events_dropped_ = 0;
  // per-job 被丢弃的 profile events 数

  int64_t num_task_attempts_dropped_tracked_ = 0;
  // per-job 当前在 dropped_task_attempts_ 集合中的 task attempt 数

  int64_t num_dropped_task_attempts_evicted_ = 0;
  // per-job 从 dropped_task_attempts_ 集合中被 GC 清理掉的数量

  absl::flat_hash_set<TaskAttempt> dropped_task_attempts_;
  // per-job 的 dropped task attempts 集合
};
```

`NumTaskAttemptsDropped()` 返回 `num_task_attempts_dropped_tracked_ + num_dropped_task_attempts_evicted_`，即"当前在集合中的 + 已从集合中清理的"，代表该 job 的 task attempts 被丢弃的总数。在 `HandleGetTaskEvents` 中返回给查询方，用于前端计算 Unaccounted 或显示数据丢失警告。

`dropped_task_attempts_` 集合自身也有清理机制：每 5 秒由 `GcJobSummary()` → `GcOldDroppedTaskAttempts()` 触发，上限 `task_events_max_dropped_task_attempts_tracked_per_job_in_gcs`（默认 1,000,000），超过时从 begin() 淘汰最旧的（多淘汰 10% 防 thrashing）。Job 结束时 `OnJobEnds()` 清空整个集合。

---

## 13. Task Attempt 与 State 的对应关系

### 13.1 GCS 存储粒度

在 GCS 存储中，**一个 task attempt 对应一个 `rpc::TaskEvents` 条目**，这个条目内部用一个 `state_ts_ns` map 保存该 task attempt 经历过的所有状态及时间戳：

```
GCS 存储:
  TaskAttempt (task_id, attempt_number) → 1 个 rpc::TaskEvents
    └─ state_updates.state_ts_ns: map<TaskStatus, timestamp>
         例: {PENDING_ARGS_AVAIL: t1, PENDING_NODE_ASSIGNMENT: t2, RUNNING: t3, FINISHED: t4}
```

`TaskAttempt = std::pair<TaskID, int32_t>`（task_id + attempt_number）。同一个 task 的不同重试是不同的 task attempt，在 GCS 中是独立的条目。

### 13.2 GetLatestTaskStatus

```cpp
// gcs_task_manager.cc:434
ray::rpc::TaskStatus GetLatestTaskStatus(const rpc::TaskEvents &task_event) {
  if (!task_event.has_state_updates()) return ray::rpc::TaskStatus::NIL;
  const auto *descriptor = ray::rpc::TaskStatus_descriptor();
  // 从最高枚举值往低遍历，找到第一个在 state_ts_ns 中存在的状态
  for (int i = descriptor->value_count() - 1; i >= 0; --i) {
    if (task_event.state_updates().state_ts_ns().contains(
            descriptor->value(i)->number())) {
      return static_cast<ray::rpc::TaskStatus>(descriptor->value(i)->number());
    }
  }
  return ray::rpc::TaskStatus::NIL;
}
```

**从高枚举值到低遍历意味着返回的是"枚举值最大的状态"**，而非"时间戳最新的状态"。这在大多数情况下正确（因为状态流转是枚举值递增的），但有一个例外：**retry 场景**——`FAILED(12)` → `PENDING_ARGS_AVAIL(1)`（新 attempt），由于是不同的 `attempt_number`，GCS 中是不同的条目，所以不影响。

### 13.3 total_state_counts 统计

在 `HandleGetTaskEvents` 中：

```cpp
for (auto &task_event : *task_events | boost::adaptors::reversed) {
    if (task_event.has_state_updates()) {
        auto latest_state = GetLatestTaskStatus(task_event);
        total_state_counts[ray::rpc::TaskStatus_Name(latest_state)]++;
    }
    // ...
}
```

`total_state_counts` 是 **task attempt 粒度**的统计——每个 task attempt 只贡献 +1 给它最新的状态，不会因为经历过 4 个状态就计数 4 次。`total_state_counts` 各值之和 = GCS 存储中有 `state_updates` 的 task attempt 总数。

**关键：** `total_state_counts` 统计的是 GCS 存储中的**全部**条目（包括没有 `task_info` 的"僵尸"条目），不受 `filter_fn` 和 `limit` 影响。而 `events_by_task`（返回给客户端的列表）受 `filter_fn` 和 `limit` 影响。这使得 `total_state_counts` 可以作为 Progress Bar 的准确数据源。

---

## 9. 关键代码位置

### 9.1 前端代码

| 文件路径 | 说明 |
|---------|------|
| `python/ray/dashboard/client/src/pages/job/hook/useJobProgress.ts` | Progress Bar 数据获取和状态映射 |
| `python/ray/dashboard/client/src/pages/job/TaskProgressBar.tsx` | Progress Bar 渲染逻辑 |
| `python/ray/dashboard/client/src/components/ProgressBar/ProgressBar.tsx` | ProgressBar 组件，Unaccounted 计算 |
| `python/ray/dashboard/client/src/type/task.ts` | TypeTaskStatus 枚举定义 |
| `python/ray/dashboard/client/src/service/task.ts` | 前端 API 客户端 (getTasks, getTask) |

### 9.2 后端代码 — State API

| 文件路径 | 关键行 | 说明 |
|---------|--------|------|
| `python/ray/dashboard/modules/state/state_head.py` | 104-108, 188-192 | 路由定义 (list_tasks, summarize_tasks) |
| `python/ray/dashboard/state_api_utils.py` | 52-98 | 过滤参数解析、handle_list_api、handle_summary_api |
| `python/ray/dashboard/state_api_utils.py` | 100-267 | convert_filters_type, do_filter |
| `python/ray/dashboard/state_aggregator.py` | 257-320 | list_tasks 服务端逻辑 |
| `python/ray/dashboard/state_aggregator.py` | 569-655 | summarize_tasks 服务端逻辑 |
| `python/ray/util/state/common.py` | 1605-1745 | protobuf_to_task_state_dict (状态推导) |
| `python/ray/util/state/common.py` | 731-820 | TaskState 数据类定义 |
| `python/ray/util/state/common.py` | 1029-1075 | TaskSummaries.to_summary_by_func_name |
| `python/ray/util/state/state_manager.py` | 232-294 | get_all_task_info → gRPC GetTaskEvents |

### 9.3 后端代码 — Task 状态机

| 文件路径 | 关键行 | 说明 |
|---------|--------|------|
| `src/ray/protobuf/common.proto` | 885-920 | TaskStatus 枚举定义 (含 metrics-only 标注) |
| `src/ray/core_worker/task_manager.cc` | 343-349 | PENDING_ARGS_AVAIL 设置 (AddPendingTask) |
| `src/ray/core_worker/task_manager.cc` | 1651-1663 | PENDING_ARGS_AVAIL → PENDING_NODE_ASSIGNMENT (MarkDependenciesResolved) |
| `src/ray/core_worker/task_manager.cc` | 1685-1700 | PENDING_NODE_ASSIGNMENT → SUBMITTED_TO_WORKER (MarkTaskWaitingForExecution) |
| `src/ray/core_worker/task_manager.cc` | 909-1053 | FINISHED/FAILED 设置 (CompletePendingTask) |
| `src/ray/core_worker/task_manager.cc` | 548-603 | HandleTaskReturn (OBJECT_IN_PLASMA 哨兵写入) |
| `src/ray/core_worker/task_submission/dependency_resolver.cc` | 30-73 | InlineDependencies (内联 vs 保留 ObjectRef) |
| `src/ray/core_worker/task_submission/dependency_resolver.cc` | 81-153 | ResolveDependencies (GetAsync 依赖解析) |
| `src/ray/core_worker/store_provider/memory_store/memory_store.cc` | 116-126 | GetAsync (本地 in-memory 查询) |
| `src/ray/core_worker/core_worker.cc` | 2993-3110 | ExecuteTask (RUNNING 上报) |
| `src/ray/core_worker/core_worker.cc` | 3531-3627 | GetAndPinArgsForExecutor (Executor 侧从 plasma 读 args) |
| `python/ray/_private/custom_types.py` | 30-44 | TASK_STATUS Python 列表 (与 proto 同步) |

### 9.4 后端代码 — Raylet 子状态

| 文件路径 | 关键行 | 说明 |
|---------|--------|------|
| `src/ray/raylet/lease_dependency_manager.h` | 69-95 | PENDING_ARGS_FETCH / PENDING_OBJ_STORE_MEM_AVAIL metrics 拆分 |
| `src/ray/raylet/lease_dependency_manager.cc` | 175-222 | RequestLeaseDependencies (PullManager 拉取) |
| `src/ray/raylet/lease_dependency_manager.cc` | 307-340 | HandleObjectLocal (object 到达后减少 missing deps) |
| `src/ray/raylet/scheduling/local_lease_manager.cc` | 93-126 | WaitForLeaseArgsRequests (决定等 args 还是直接 grant) |
| `src/ray/raylet/scheduling/local_lease_manager.cc` | 711-731 | LeasesUnblocked (args 就绪后移到 grant 队列) |
| `src/ray/raylet/scheduling/local_lease_manager.cc` | 735-793 | Grant (发送 lease grant 回复) |
| `src/ray/object_manager/pull_manager.cc` | 112-153 | ActivateNextBundlePullRequest (激活 pull) |
| `src/ray/object_manager/pull_manager.cc` | 167-304 | UpdatePullsBasedOnAvailableMemory (配额管理) |
| `src/ray/object_manager/object_manager.cc` | 199-260 | SendPullRequest / HandlePull (网络传输) |
| `src/ray/object_manager/object_manager.cc` | 799-834 | Tick (周期性查询 plasma 可用内存) |

### 9.5 GCS Task Event 存储

| 文件路径 | 关键行 | 说明 |
|---------|--------|------|
| `src/ray/gcs/gcs_task_manager.cc` | 426-621 | HandleGetTaskEvents (GCS 查询) |
| `src/ray/gcs/gcs_task_manager.cc` | 167-207 | UpdateExistingTaskAttempt (MergeFrom 合并状态, 优先级迁移) |
| `src/ray/gcs/gcs_task_manager.cc` | 332-389 | EvictTaskEvent (GCS 淘汰) |
| `src/ray/gcs/gcs_task_manager.cc` | 311-330 | RemoveTaskAttempt (索引删除 + dropped 标记) |
| `src/ray/gcs/gcs_task_manager.cc` | 351-389 | AddOrReplaceTaskEvent (ShouldDropTaskAttempt 拦截) |
| `src/ray/gcs/gcs_task_manager.cc` | 639-663 | RecordDataLossFromWorker (Worker 上报数据丢失处理) |
| `src/ray/gcs/gcs_task_manager.cc` | 665-673 | RecordTaskEventData (GCS 接收入口) |
| `src/ray/gcs/gcs_task_manager.cc` | 699-712 | DebugString (计数器调试输出) |
| `src/ray/gcs/gcs_task_manager.cc` | 714-742 | RecordMetrics (Metrics 导出) |
| `src/ray/gcs/gcs_task_manager.cc` | 789-825 | GcOldDroppedTaskAttempts (dropped 集合 GC) |
| `src/ray/gcs/gcs_task_manager.h` | 71-86 | FinishedTaskActorTaskGcPolicy (淘汰优先级) |
| `src/ray/gcs/gcs_task_manager.h` | 300-380 | JobTaskSummary (per-job 统计) |
| `src/ray/gcs/gcs_task_manager.h` | 440-530 | GcsTaskManagerStorage (存储 + 索引) |
| `src/ray/gcs/gcs_task_manager.cc` | 426-438 | GetLatestTaskStatus (commit 9be153f7ae 新增，提取最新状态) |
| `src/ray/gcs/gcs_task_manager.cc` | 590-596 | total_state_counts 统计 (commit 9be153f7ae，在 filter/limit 之前) |
| `src/ray/protobuf/gcs_service.proto` | 873-876 | GetTaskEventsReply.total_state_counts (field 8, commit 9be153f7ae 新增) |
| `python/ray/util/state/common.py` | 970-977 | ListApiResponse.total_state_counts (commit 9be153f7ae 新增) |
| `python/ray/util/state/common.py` | 1550-1555 | SummaryApiResponse.total_state_counts (commit 9be153f7ae 新增) |
| `python/ray/dashboard/state_aggregator.py` | 329-340 | list_tasks transform 传递 total_state_counts (commit 9be153f7ae) |
| `python/ray/dashboard/state_aggregator.py` | 620-627 | summarize_tasks 传递 total_state_counts (commit 9be153f7ae) |
| `python/ray/dashboard/client/src/pages/job/hook/useJobProgress.ts` | 82-143 | 前端优先使用 total_state_counts (commit 9be153f7ae) |
| `src/ray/common/protobuf_utils.cc` | 310-349 | IsTaskTerminated / IsTaskFinished / IsActorTask |
| `src/ray/common/protobuf_utils.cc` | 351-359 | FillTaskStatusUpdateTime (state_ts_ns 写入) |
| `src/ray/util/counter_map.h` | 151-213 | CounterMapThreadSafe (线程安全计数器) |

### 9.6 Worker 侧 Task Event Buffer

| 文件路径 | 关键行 | 说明 |
|---------|--------|------|
| `src/ray/core_worker/task_event_buffer.cc` | 420-450 | RecordTaskStatusEventIfNeeded (两层过滤) |
| `src/ray/core_worker/task_event_buffer.cc` | 1000-1007 | AddTaskEvent (Status/Profile 分流) |
| `src/ray/core_worker/task_event_buffer.cc` | 1009-1055 | AddTaskStatusEvent (circular buffer + dropped 标记) |
| `src/ray/core_worker/task_event_buffer.cc` | 1057-1100 | AddTaskProfileEvent (per-task 分组 + drop new) |
| `src/ray/core_worker/task_event_buffer.cc` | 568-630 | GetTaskStatusEventsToSend (flush 时取出 + erase) |
| `src/ray/core_worker/task_event_buffer.cc` | 660-680 | GetTaskProfileEventsToSend |
| `src/ray/core_worker/task_event_buffer.cc` | 729-790 | CreateDataToSend (按 task attempt 聚合 + dropped 过滤) |
| `src/ray/core_worker/task_event_buffer.cc` | 79-135 | TaskStatusEvent::ToRpcTaskEvents (state_ts_ns 累积) |
| `src/ray/core_worker/task_event_buffer.cc` | 908-970 | FlushEvents (flush 流程) |
| `src/ray/core_worker/task_event_buffer.cc` | 980-1000 | ResetCountersForFlush |
| `src/ray/core_worker/task_event_buffer.h` | 574-618 | status_events_ / dropped_task_attempts_unreported_ 等成员 |
| `src/ray/core_worker/task_manager.cc` | 1702-1720 | SetTaskStatus (状态转换统一入口) |
| `src/ray/core_worker/core_worker.cc` | 58-87 | ScopedTaskMetricSetter (RUNNING_IN_RAY_GET/WAIT 瞬态) |

### 9.7 Ray Data 代码

| 文件路径 | 说明 |
|---------|------|
| `python/ray/data/_internal/execution/streaming_executor_state.py` | process_completed_tasks, 任务状态处理 |
| `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py` | ActorPool 任务分发 |
| `python/ray/data/_internal/execution/interfaces/physical_operator.py` | DataOpTask 定义 |

---

## 附录

### A. 快速诊断指南

**问题**: Dashboard 显示大量 Unaccounted

1. 检查任务总数是否超过 100,000
2. 如果是，考虑增加 `RAY_task_events_max_num_task_in_gcs`
3. 这是正常的内存保护，不影响任务执行

**问题**: Waiting for scheduling 数量与预期不符

1. 理解 Progress Bar 合并了多个状态
2. 查看 Task Table 获取细粒度状态
3. 考虑 Actor 预取机制的影响

**问题**: Running 数量小于 Actor 数量

1. 检查 GPU 资源分配（可能存在碎片化）
2. 检查是否有任务在等待参数
3. 检查 `max_concurrency` 配置

**问题**: Dashboard 上 PENDING_NODE_ASSIGNMENT 数量很高但不知道卡在哪

1. Dashboard 上的 `PENDING_NODE_ASSIGNMENT` 是一个"黑盒"，Driver 只知道 lease 请求发出去了还没收到回复
2. 要看 Raylet 内部子状态，需查 Prometheus metrics（`PENDING_ARGS_FETCH` vs `PENDING_OBJ_STORE_MEM_AVAIL`）或 raylet 日志
3. 检查 raylet 日志中的 `RequestWorkerLease - N total (M active)` 确认是否有 lease 卡死
4. 检查 object store 使用率：`ray memory --address=auto | grep "Plasma memory usage"`

### B. 常见误解澄清

| 误解 | 正确理解 |
|------|---------| 
| "Unaccounted 表示任务丢失" | Unaccounted 只是显示问题，任务仍在正常执行 |
| "Submitted 表示任务还没开始" | Submitted 包含正在 Actor 队列中等待的任务 |
| "Task Table 和 Progress Bar 应该完全一致" | 它们使用不同的数据源和聚合方式 |
| "Running 应该等于 Actor 数" | Running 受资源和并发限制，可能小于 Actor 数 |
| "PENDING_ARGS_FETCH 和 PENDING_OBJ_STORE_MEM_AVAIL 在 Dashboard 上可见" | 它们是 "metrics only" 子状态，Raylet 不上报 GCS，Dashboard 不可见 |
| "PENDING_NODE_ASSIGNMENT → PENDING_ARGS_FETCH 是真实状态转换" | 不是。GCS 中 task 一直是 PENDING_NODE_ASSIGNMENT，PENDING_ARGS_FETCH 是 Raylet metrics 拆分的子状态 |
| "Owner 侧依赖解析会把数据拉到 Owner 的 object store" | 不是。Owner 侧只查 in-memory store（堆内存），不拉数据。大对象走 plasma 由 Raylet 侧拉取 |
| "GCS 淘汰后该 task 的后续事件仍能被统计" | 不是。一旦 GCS 淘汰了某个 task attempt，后续事件会被 `ShouldDropTaskAttempt` 永久丢弃，不存储不合并 |
| "Worker circular buffer 淘汰的是整个 task attempt" | 不是。淘汰是事件粒度的（弹出最旧的一条 event），但 dropped 标记是 task attempt 级别的 |
| "当前版本仍会产生大量 Unaccounted" | 当前版本优先使用 `total_state_counts`，progress 和 total 来自同一数据源，Unaccounted = 0。只有退化路径 B 才可能产生 |
| "一个 task 经历 4 个状态，total_state_counts 统计 4 次" | 不是。`total_state_counts` 是 task attempt 粒度统计，每个 attempt 只贡献 +1 给最新状态 |
| "`dropped_task_attempts_unreported_` 在 Worker 侧永久保留" | 不是。每次 flush 时已上报的部分会被 erase 清理，丢弃职责转移给 GCS |
| "GCS 淘汰只淘汰 FINISHED 的 task" | 不完全。优先淘汰 FINISHED (Priority 0)，但 Priority 0 空了才到 Priority 1 (Actor 未完成)，再到 Priority 2 (其他未完成)。FAILED 的普通 task 在 Priority 2 |
| "按 job_id 查询时 RUNNING task 排在前面" | 不是。按 job_id 查询走 `job_index_`（flat_hash_set），遍历顺序由 hash 决定，与优先级/时间无关。只有不带 job_id 查询才走优先级 list |
| "num_after_truncation 是 GCS 端的截断计数" | 不是。它是 Dashboard 端读取 GCS 返回的 events_by_task 后的计数（GCS 截断后**剩余**的数量）。GCS 端截断计数是 `reply.num_truncated` |
| "/api/v0/tasks 和 /api/v0/tasks/summarize 的状态来源不同" | 不是。两者底层调同一个 GCS RPC，状态来源相同。区别在于 summarize 额外返回 `total_state_counts`（全量统计不受 filter/limit） |
| "PENDING_ACTOR_TASK_ARGS_FETCH 和 PENDING_ARGS_FETCH 是同一个状态" | 不是。PENDING_ACTOR_TASK_ARGS_FETCH(6) 由 Actor Worker 上报，GCS 可见。PENDING_ARGS_FETCH(4) 由 Raylet metrics 上报，GCS 不可见。名字相似但完全不同 |
| "Progress Bar 的 'Waiting for scheduling' 只包含 PENDING_NODE_ASSIGNMENT" | 不是。它合并了 PENDING_NODE_ASSIGNMENT + SUBMITTED_TO_WORKER + PENDING_ACTOR_TASK_ARGS_FETCH + PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY（以及死代码的 PENDING_OBJ_STORE_MEM_AVAIL 和 PENDING_ARGS_FETCH） |
| "/api/v0/tasks 返回的 RUNNING 数是准确的" | 不是。它受 GCS filter_fn（过滤僵尸）+ GCS limit（hash 随机序截断）+ Dashboard do_filter + Dashboard islice 四层截断，可能远小于实际。全量 RUNNING 数应看 /api/v0/tasks/summarize 的 `total_state_counts["RUNNING"]` |
| "不带 commit 9be153f7ae 时 /api/v0/tasks/summarize 也返回 total_state_counts" | 不是。该 commit 是新增 `total_state_counts` 字段，不带时两个接口都没有该字段，状态统计仅基于可见 task，会遗漏僵尸条目和被 limit 截断的条目 |
| "不带 commit 9be153f7ae 时带 job_id 查询会随机返回 task" | 不是随机选 task（该 job 的所有 task 都会被选为候选），但 `flat_hash_set` 遍历顺序不确定 + limit 截断 = 截断是随机的，RUNNING 和 FINISHED 被截断的概率均等，导致统计不准 |

### C. 相关文档

- [Task-Event 数据流与淘汰分析](../01-架构与原理/Task-Event数据流与淘汰分析.md) — Dashboard 数据获取链路、两层淘汰机制、Owner/Executor Buffer 分工
- [Task-Event 淘汰与 Lease 卡死分析](../01-架构与原理/Task-Event淘汰与Lease卡死分析.md) — Worker buffer FIFO 淘汰、GCS 优先级淘汰、RequestWorkerLease gRPC 永久卡死
- [Ray Object Store Full Analysis](../ray-object-store-full-analysis.md) — Object Store 满导致 Task Pending 的根因分析
- [IsInPlasmaError 机制解析](../01-架构与原理/IsInPlasmaError机制解析.md) — OBJECT_IN_PLASMA 哨兵的详细分析
- [Lease 卡死深度分析](../01-架构与原理/Lease卡死深度分析.md) — RequestWorkerLease gRPC 卡死的完整调用链分析

---

## 15. 核心要点总结

### 15.1 GCS 查询与过滤截断

**GCS `HandleGetTaskEvents` 遍历顺序**：对每条 task_event，先无条件统计 `total_state_counts`，再 `filter_fn` 过滤，最后 `limit` 截断。filter 在 limit 之前——被过滤的条目不消耗 limit 额度。

**按 job_id 查询的设计缺陷**：Dashboard 总是带 job_id 查询，走 `job_index_`（`flat_hash_set`），遍历顺序由 hash 决定，与优先级/时间无关。只有不带 job_id 查询才走 `task_events_list_` 优先级排序（P2 未完成→P1 Actor 未完成→P0 已完成）。导致大规模作业下 limit 截断是随机的，RUNNING task 不保证排在前面。

**四层 limit**：前端 HTTP（100）→ GCS RPC（10000）→ GCS 遍历（10000）→ Dashboard islice（100 或 10000）。`num_after_truncation` 和 `num_filtered` 都是 **Dashboard 端**计数，不是 GCS 端。GCS 端截断数是 `reply.num_truncated`，过滤数是 `reply.num_filtered_on_gcs`。

**两次过滤的原因**：GCS `filter_fn` 基于 protobuf 字段做粗筛（含过滤无 task_info 的僵尸条目），Dashboard `do_filter` 基于转换后的 Python dict 做精筛（支持类型转换、大小写不敏感）。

### 15.2 两个 API 的区别

`/api/v0/tasks` 和 `/api/v0/tasks/summarize` 底层调同一个 GCS RPC，状态来源相同。核心区别：

- `/api/v0/tasks`：返回截断后的逐条 task 列表，每条有 `state` 字段（原始状态名），**不返回 `total_state_counts`**。RUNNING 数 = 列表中 state=RUNNING 的条数，受四层截断影响，可能远小于实际。
- `/api/v0/tasks/summarize`：返回按 func_name 分组的 `state_counts`（截断后）**和 `total_state_counts`（全量统计，不受 filter/limit）**。Progress Bar 优先用 `total_state_counts`。

**不带 commit 9be153f7ae 时**：两个接口都没有 `total_state_counts` 字段，状态统计仅基于 `events_by_task`（通过 `filter_fn` 且未被 limit 截断的可见 task），会遗漏无 `task_info` 的僵尸条目和被 limit 截断的条目。详见 [11.13a 节](#1113a-不带-commit-9be153f7ae-时的状态统计行为分析)。

### 15.3 Progress Bar RUNNING 数量的准确性

Progress Bar 的 RUNNING 数 = `total_state_counts["RUNNING"]`，来自 GCS 遍历全部存储条目的统计，**不受 filter_fn 和 limit 影响**，包含僵尸 entry（有 state_updates 但无 task_info）。

**不带 commit 9be153f7ae 时**：Progress Bar 没有 `total_state_counts`，只能用 `summarize` 的 `state_counts`（按 func_name 分组聚合），该聚合仅基于截断后的可见 task，RUNNING 数严重偏低。且带 job_id 查询时 `flat_hash_set` 遍历顺序不确定 + limit 截断 = 状态统计可能严重不准。

**但被 drop 的 task 不在统计范围内**：被 GCS `EvictTaskEvent` 或 Worker 端 `RecordDataLossFromWorker` 删除的 task attempt 从 GCS 存储中物理移除，不在 `total_state_counts` 中。这些丢失的 task 数体现在 `num_status_task_events_dropped` 中。

**诊断方法**：比较 `sum(total_state_counts)` 和 `num_after_truncation`：
- 差值 > 0 → 有僵尸 entry（有状态无 task_info，被 filter_fn 过滤）
- `num_status_task_events_dropped` 很大 → 大量 task 被 drop（GCS 存储淘汰或 Worker buffer 溢出）

### 15.4 僵尸 Entry

僵尸 entry = 有 `state_updates` 但无 `task_info` 的 GCS 存储条目。产生原因：Executor 的 RUNNING 上报（`include_task_info=false`）先于 Submitter 到达 GCS，或 Submitter 的带 task_info 的事件丢失。修复方案：开启 `task_events_executor_include_task_info=true`。

僵尸 entry 在 `total_state_counts` 中有计数（Progress Bar 可见），但被 `filter_fn` 过滤（Task Table 不可见）。

### 15.5 状态可见性

GCS 中可见的状态（8 个）：`PENDING_ARGS_AVAIL`、`PENDING_NODE_ASSIGNMENT`、`SUBMITTED_TO_WORKER`、`PENDING_ACTOR_TASK_ARGS_FETCH`、`PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY`、`RUNNING`、`FINISHED`、`FAILED`。

GCS 中不可见的状态（6 个，metrics only）：`NIL`、`PENDING_OBJ_STORE_MEM_AVAIL`、`PENDING_ARGS_FETCH`、`RUNNING_IN_RAY_GET`、`RUNNING_IN_RAY_WAIT`、`GETTING_AND_PINNING_ARGS`。这些状态的前端映射规则是死代码，永远不会被触发。

`PENDING_ACTOR_TASK_ARGS_FETCH`(6) 和 `PENDING_ARGS_FETCH`(4) 名字相似但完全不同：前者由 Actor Worker 上报、GCS 可见；后者由 Raylet metrics 上报、GCS 不可见。

### 15.6 Progress Bar 状态合并

Progress Bar 通过三层映射将 14 个原始状态合并为 7 个 segment：

1. 原始状态 → 前端 TaskStatus（`TASK_STATE_NAME_TO_PROGRESS_KEY`）
2. 前端 TaskStatus → Progress 字段名（`TaskStatusToTaskProgressMapping`）
3. Progress 字段名 → Progress Bar segment（`TaskProgressBar.tsx`，此层再次合并 `numPendingNodeAssignment + numSubmittedToWorker`）

"Waiting for scheduling" segment 合并了 6 个状态：`PENDING_NODE_ASSIGNMENT`、`SUBMITTED_TO_WORKER`、`PENDING_ACTOR_TASK_ARGS_FETCH`、`PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY`（+ 2 个死代码状态）。

### 15.7 num_dropped_task_attempts_evicted_

`dropped_task_attempts_` 集合本身也有上限（默认 1,000,000），超过时由 `GcOldDroppedTaskAttempts` 淘汰最旧的。`num_dropped_task_attempts_evicted_` 记录被这个二级 GC 清理掉的数量。`NumTaskAttemptsDropped()` = `num_task_attempts_dropped_tracked_`（当前在集合中）+ `num_dropped_task_attempts_evicted_`（已清理），涵盖 Worker 端 drop 和 GCS 端淘汰两种来源。

### 15.8 Worker 上报周期与时序偏差

#### 上报机制

每个 Worker 进程独立运行一个 `PeriodicalRunner`，定时调用 `FlushEvents`：

```cpp
// task_event_buffer.cc:477-519
auto report_interval_ms = RayConfig::instance().task_events_report_interval_ms();
RAY_CHECK(report_interval_ms > 0);
// ...
periodical_runner_->RunFnPeriodically(
    [this] { FlushEvents(/*forced=*/false); },
    report_interval_ms,
    "TaskEventBuffer.flush");
RAY_LOG(INFO) << "Reporting task events to GCS every " << report_interval_ms << "ms.";
```

默认间隔 **1000ms**（`RAY_task_events_report_interval_ms`，`ray_config_def.h:456`）。每个 Worker **独立**计时，互不同步。

#### 背压跳过

```cpp
// task_event_buffer.cc:912-930
void TaskEventBufferImpl::FlushEvents(bool forced) {
  if ((gcs_grpc_in_progress_.load() > 0 ||
       event_aggregator_grpc_in_progress_.load() > 0) && !forced) {
    // GCS 还没处理完上一批 → 跳过本次 flush
    RAY_LOG_EVERY_N_OR_DEBUG(WARNING, 100)
        << "GCS or the event aggregator hasn't replied to the previous flush events "
           "call (likely overloaded). Skipping reporting task state events and retry later."
        << "[gcs_grpc_in_progress=" << gcs_grpc_in_progress_.load() << "]";
    return;  // ← 直接返回，不发送
  }
  // ... 正常 flush 逻辑
}
```

如果上一次 flush 的 gRPC 还没返回（GCS 过载），本次 flush 直接跳过，延迟更长。`forced=true` 时跳过此检查（用于 shutdown 时的最终 flush）。

#### 时序偏差对查询结果的影响

查询 `/api/v0/tasks` 或 `/api/v0/tasks/summarize` 时，GCS 返回的是**当前 GCS 存储中的数据**，但各 Worker 的最新状态变更可能还没 flush 到 GCS。偏差来源：

**1. 时间窗口偏差（最多 1 秒）**

每个 Worker 每 1 秒 flush 一次。查询时可能有些 Worker 刚 flush 完、有些还没到下一次 flush。状态变更最多有 1 秒延迟才到达 GCS。

```
t=0.0s: Worker A 状态 RUNNING → FINISHED（写入本地 buffer）
t=0.3s: 查询 /api/v0/tasks → GCS 中还是 RUNNING（Worker A 还没 flush）
t=1.0s: Worker A flush → GCS 更新为 FINISHED
t=1.2s: 再次查询 → GCS 中是 FINISHED
```

**2. 背压导致的更长延迟**

GCS 过载时 Worker 跳过 flush，延迟可能远超 1 秒。在大量 Worker 同时 flush 的场景下（如万级 Worker 集群），GCS 处理 gRPC 的速度可能跟不上，导致多轮 flush 被跳过。

**3. 两个 API 不同步**

`/api/v0/tasks` 和 `/api/v0/tasks/summarize` 是两次独立的 HTTP 请求，各自触发一次 GCS 查询。两次查询之间可能有 Worker 完成了新的 flush，导致结果不一致：

```
t=0.0s: Worker A 状态 RUNNING → FINISHED
t=0.3s: /api/v0/tasks 查询 → GCS 中还是 RUNNING
t=0.5s: Worker A flush → GCS 更新为 FINISHED
t=0.7s: /api/v0/tasks/summarize 查询 → GCS 中是 FINISHED
```

此时 Task Table 显示 RUNNING，Progress Bar 显示 FINISHED，产生不一致。

**4. `total_state_counts` 的时序特性**

`total_state_counts` 是 GCS 在**同一次查询**中遍历全部存储条目统计的，不受 Worker 上报延迟影响——它统计的是 GCS 当前存储中的数据。但这些数据本身可能有最多 1 秒（或更长，如果有背压）的上报延迟。`total_state_counts` 和 `events_by_task` 来自同一次 GCS 遍历，两者之间不会有时序不一致。

#### 缓解方法

| 方法 | 效果 | 代价 |
|------|------|------|
| 减小 `RAY_task_events_report_interval_ms`（如 500ms） | 减少时间窗口偏差 | 增加 gRPC 带宽和 GCS CPU |
| 增大 `RAY_task_events_max_num_task_in_gcs` | 减少 drop 导致的偏差 | 增加 GCS 内存 |
| 同一请求中同时返回 events_by_task 和 total_state_counts | 消除两个 API 间的时序不一致 | 需代码改动 |
| 开启 `task_events_executor_include_task_info` | 减少僵尸 entry | task_info 带宽翻倍 |
