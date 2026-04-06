# Ray Data Task 状态与调度机制深度分析

## 目录

1. [概述](#1-概述)
2. [Ray Core TaskStatus 状态详解](#2-ray-core-taskstatus-状态详解)
3. [Ray Data Tasks vs Ray Core Task Table 的关系](#3-ray-data-tasks-vs-ray-core-task-table-的关系)
4. [ActorPool Prefetch 机制与任务分配策略](#4-actorpool-prefetch-机制与任务分配策略)
5. [Dashboard 状态显示逻辑](#5-dashboard-状态显示逻辑)
6. [GCS 同步延迟分析与诊断](#6-gcs-同步延迟分析与诊断)
7. [Scheduling Loop 性能分析](#7-scheduling-loop-性能分析)
8. [如何在日志中区分 Running 和排队的 Tasks](#8-如何在日志中区分-running-和排队的-tasks)
9. [Actor 并发参数详解](#9-actor-并发参数详解)
10. [Batch Input 并行逻辑 (Ray Core C++ 层面)](#10-batch-input-并行逻辑-ray-core-c-层面)
11. [问题诊断指南](#11-问题诊断指南)

---

## 1. 概述

在 Ray Data 执行过程中，用户经常会看到日志中显示的 `Tasks: N; Actors: M` 与 Ray Dashboard 中 Ray Core Overview 显示的 Task 状态不一致。本文档详细分析这种差异的原因、底层机制，以及如何诊断相关问题。

### 核心概念区分

| 概念 | 层级 | 含义 |
|------|------|------|
| Ray Data `Tasks` | 应用层 | Ray Data 已提交但未完成的数据处理请求数量 |
| Ray Core Task | 系统层 | GCS 中记录的 Task 实体及其状态 |
| Dashboard Task Table | 展示层 | 从 GCS 查询并展示的 Task 状态信息 |

---

## 2. Ray Core TaskStatus 状态详解

### 2.1 完整状态定义

根据 `src/ray/protobuf/common.proto:885-920` 中的定义：

```protobuf
enum TaskStatus {
  NIL = 0;
  PENDING_ARGS_AVAIL = 1;
  PENDING_NODE_ASSIGNMENT = 2;
  // sub-state of PENDING_NODE_ASSIGNMENT, metrics only
  PENDING_OBJ_STORE_MEM_AVAIL = 3;
  // sub-state of PENDING_NODE_ASSIGNMENT, metrics only
  PENDING_ARGS_FETCH = 4;
  SUBMITTED_TO_WORKER = 5;
  PENDING_ACTOR_TASK_ARGS_FETCH = 6;
  PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY = 7;
  RUNNING = 8;
  // sub-state of RUNNING, metrics only
  RUNNING_IN_RAY_GET = 9;
  // sub-state of RUNNING, metrics only
  RUNNING_IN_RAY_WAIT = 10;
  FINISHED = 11;
  FAILED = 12;
  // sub-state of RUNNING, metrics only
  GETTING_AND_PINNING_ARGS = 13;
}
```

**5 个 "metrics only" 子状态不会出现在 GCS Task Event 中**，Dashboard 不可见：
- `PENDING_OBJ_STORE_MEM_AVAIL` (3) — Raylet 侧 metrics
- `PENDING_ARGS_FETCH` (4) — Raylet 侧 metrics
- `RUNNING_IN_RAY_GET` (9) — Executor 侧 metrics
- `RUNNING_IN_RAY_WAIT` (10) — Executor 侧 metrics
- `GETTING_AND_PINNING_ARGS` (13) — Executor 侧 metrics

> **详见 [Dashboard-Task状态显示机制](../04-Dashboard与指标/Dashboard-Task状态显示机制.md) §2.3 两套状态系统的独立性和 §2.4 GCS 可见性对照表**

### 2.2 状态转换流程

#### GCS Task Event 中的真实状态转换（Dashboard 可见）

以下状态由 CoreWorker（Owner/Executor）通过 `TaskEventBuffer` 上报到 GCS：

```
普通 Task (NORMAL_TASK):

  PENDING_ARGS_AVAIL ──→ PENDING_NODE_ASSIGNMENT ──→ SUBMITTED_TO_WORKER ──→ RUNNING ──→ FINISHED / FAILED
    (Owner 上报)            (Owner 上报)                (Owner 上报)           (Executor 上报)  (Owner 上报)
       │                        │
       │ task_manager.cc:343     │ task_manager.cc:1678
       │ AddPendingTask          │ MarkDependenciesResolved
       │                          │
       │                          │ Owner 视角：等 lease 回复
       │                          │ 不知道 raylet 内部在做什么
       │                          │ ↓
       │                          │ ┌─ Raylet 内部子状态（metrics only，不上报 GCS）─┐
       │                          │ │                                                 │
       │                          │ │  PENDING_ARGS_FETCH                             │
       │                          │ │  (PullManager 活跃拉取 args)                    │
       │                          │ │                                                 │
       │                          │ │  PENDING_OBJ_STORE_MEM_AVAIL                    │
       │                          │ │  (object store 满，拉取暂停)                    │
       │                          │ │                                                 │
       │                          │ │  这些子状态不上报 GCS，Dashboard 看不到        │
       │                          │ └─────────────────────────────────────────────────┘
       │                          │
       │                          │ task_manager.cc:1694
       │                          │ MarkTaskWaitingForExecution
       │                          │ (收到 lease grant 回复)
       ↓                          ↓
  (如果上游 task              PENDING_NODE_ASSIGNMENT 直接跳到
   还没完成，                   SUBMITTED_TO_WORKER
   卡在这里)                    (GCS 中没有中间子状态记录)


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

#### Raylet Metrics 子状态（不上报 GCS，Dashboard 不可见）

`PENDING_NODE_ASSIGNMENT` 在 Raylet 内部被 `LeaseDependencyManager` 拆分为两个子状态，
仅存在于 Prometheus Metrics 中（`task_by_state_counter_` gauge）：

- **PENDING_ARGS_FETCH** — PullManager 已激活 pull 请求，正在通过网络从远端节点拉取 plasma 对象
- **PENDING_OBJ_STORE_MEM_AVAIL** — PullManager 因目标节点 object store 可用内存不足而暂停拉取

Raylet 用负数抵消 Owner 上报的 `PENDING_NODE_ASSIGNMENT` 计数，然后重新分类为两个子状态。
详见 `lease_dependency_manager.h:77-95`。

> **详见 [Dashboard-Task状态显示机制](../04-Dashboard与指标/Dashboard-Task状态显示机制.md) §2.5 PENDING_NODE_ASSIGNMENT 的 Raylet 子状态深度分析**

### 2.3 `SUBMITTED_TO_WORKER` 状态详解

**含义**：
- 调度器已经选定了目标节点和 Worker
- Task 已被发送到目标 Worker 的任务队列
- **但 Worker 可能正在执行其他任务，所以这个 Task 在排队等待**

**对于 Actor Task**：
- Actor 是单线程执行的（除非设置 `max_concurrency > 1`）
- 当 Actor 正在处理一个任务时，新提交的任务会进入 `SUBMITTED_TO_WORKER` 状态
- 这就是 "prefetch" 任务的典型状态

---

## 3. Ray Data Tasks vs Ray Core Task Table 的关系

### 3.1 Ray Data `Tasks: N` 的统计逻辑

根据 `streaming_executor_state.py` 和 `map_operator.py` 的代码：

```python
# streaming_executor_state.py 第 873-879 行
def format_op_state_summary(op_state: OpState, ...) -> str:
    active = op_state.op.num_active_tasks()  # 调用 operator 的方法
    desc = f"Tasks: {active}"
    desc += f"; {_actor_info_summary_str(op_state.op.get_actor_info())}"
    return desc

# map_operator.py 第 725-734 行
def num_active_tasks(self) -> int:
    # 只统计 _data_tasks，不包含 _metadata_tasks
    return len(self._data_tasks)
```

**关键点**：
- `Tasks` = `len(self._data_tasks)` = **已提交但未收到完成回调的任务数量**
- 这是 Ray Data 层面的逻辑计数，不是 Ray Core 的 Task 状态

### 3.2 数量差异的根本原因

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        两个层面的统计差异                                        │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  Ray Data 层面 (Python):                                                         │
│    _data_tasks = {task1, task2, task3, task4, task5, task6}                     │
│    Tasks: 6  (所有已提交未完成的)                                                │
│                                                                                 │
│  Ray Core 层面 (GCS):                                                            │
│    task1: RUNNING                    (Actor 1 正在执行)                          │
│    task2: RUNNING                    (Actor 2 正在执行)                          │
│    task3: RUNNING                    (Actor 3 正在执行)                          │
│    task4: SUBMITTED_TO_WORKER        (Actor 1 队列中等待)                        │
│    task5: SUBMITTED_TO_WORKER        (Actor 2 队列中等待)                        │
│    task6: SUBMITTED_TO_WORKER        (Actor 3 队列中等待)                        │
│                                                                                 │
│  Dashboard 显示:                                                                 │
│    Running: 3                                                                   │
│    Waiting for scheduling: 3                                                    │
│                                                                                 │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  结论: Ray Data "Tasks: 6" = Ray Core "RUNNING: 3" + "SUBMITTED_TO_WORKER: 3"   │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 3.3 差异场景分析

| 场景 | Ray Data `Tasks` | Ray Core 状态分布 | 原因 |
|------|------------------|-------------------|------|
| 正常 prefetch | 高 | RUNNING + SUBMITTED_TO_WORKER | 每个 Actor 有多个排队任务 |
| 资源不足 | 高 | 大量 PENDING_NODE_ASSIGNMENT | 调度器无法分配足够节点 |
| Object Store 满 | 高 | 大量 PENDING_OBJ_STORE_MEM_AVAIL | 内存反压 |
| 上游数据慢 | 低 | 大量 PENDING_ARGS_AVAIL | 等待上游 stage 输出 |
| GCS 同步延迟 | 不一致 | 状态滞后 | GCS 更新延迟 |

---

## 4. ActorPool Prefetch 机制与任务分配策略

### 4.1 Prefetch 配置参数

根据 `actor_pool_map_operator.py` 第 168-186 行：

```python
max_actor_concurrency = self._ray_remote_args.get("max_concurrency", 1)

self._actor_pool = _ActorPool(
    ...
    max_actor_concurrency=max_actor_concurrency,
    max_tasks_in_flight_per_actor=(
        compute_strategy.max_tasks_in_flight_per_actor
        or data_context.max_tasks_in_flight_per_actor
        or max_actor_concurrency * DEFAULT_ACTOR_MAX_TASKS_IN_FLIGHT_TO_MAX_CONCURRENCY_FACTOR
    ),
)
```

**参数说明**：
- `max_actor_concurrency`: Actor 内部最大并发执行数（默认 1，即单线程）
- `max_tasks_in_flight_per_actor`: 每个 Actor 最多可以排队的任务数

### 4.2 任务是否调用了 `submit` 方法？

**是的，prefetch 的任务已经调用了 `actor.submit.remote()`**：

```python
# actor_pool_map_operator.py 第 381-390 行
gen = actor.submit.options(
    num_returns="streaming",
    **self._ray_actor_task_remote_args,
).remote(                              # ★ 这里调用了 .remote()
    self.data_context,
    ctx,
    *input_blocks,
    slices=bundle.slices,
    **self.get_map_task_kwargs(),
)
```

调用 `.remote()` 后：
1. Ray Core 立即创建一个 Task 实体
2. Task 被发送到目标 Actor 所在的 Worker
3. 如果 Actor 正忙，Task 进入 `SUBMITTED_TO_WORKER` 状态在 Worker 队列中等待

### 4.3 任务分配策略：是否均匀分配？优先空闲 Actor？

根据 `_ActorTaskSelectorImpl._rank_actors()` 方法（第 833-880 行）：

```python
def _rank_actors(
    self,
    actors: List[ActorHandle],
    bundle: Optional[RefBundle],
) -> List[Tuple[int, int]]:
    """
    排序规则（rank 值越小越优先）：
    1. 首先考虑数据本地性（locality_rank）
    2. 其次考虑当前负载（num_tasks_in_flight）
    """
    ranks = [
        (
            # 1. 数据本地性优先级（数据在该节点的大小，越大越优先）
            locs_priorities.get(
                self._actor_pool.running_actors()[actor].actor_location, INT32_MAX
            ),
            # 2. 当前任务数（越少越优先）
            self._actor_pool.running_actors()[actor].num_tasks_in_flight,
        )
        for actor in actors
    ]
    return ranks
```

**分配策略总结**：

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          任务分配优先级                                          │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  1. 数据本地性优先（Locality First）                                             │
│     - 如果数据所在节点有可用 Actor，优先选择该 Actor                              │
│     - 根据数据大小排序（数据越大，优先级越高）                                     │
│                                                                                 │
│  2. 负载均衡（Load Balancing）                                                   │
│     - 在满足本地性的前提下，选择 num_tasks_in_flight 最少的 Actor                 │
│     - 这实现了"优先调度给空闲/负载低的 Actor"                                     │
│                                                                                 │
│  3. 可调度性检查（Schedulability Check）                                         │
│     - 只有 num_tasks_in_flight < max_tasks_in_flight_per_actor 的 Actor 才可接收 │
│     - 正在重启的 Actor 不接收新任务                                               │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

**代码证据**（第 1182-1190 行）：

```python
def schedulable_actors(self) -> List[ray.actor.ActorHandle]:
    available_actors = self.get_available_actors()
    return [
        actor
        for actor, state in available_actors.items()
        if state.num_tasks_in_flight < self.max_tasks_in_flight_per_actor()
        and not state.is_restarting
    ]
```

### 4.4 任务分配示例

```
配置: max_tasks_in_flight_per_actor = 4, 3 个 Actor

初始状态:
  Actor 1 (Node A): num_tasks_in_flight = 0
  Actor 2 (Node B): num_tasks_in_flight = 0
  Actor 3 (Node A): num_tasks_in_flight = 0

提交 Bundle 1 (数据在 Node A):
  → 选择 Actor 1 或 Actor 3（本地性优先，两者 tasks=0，随机选一个）
  → 假设选择 Actor 1

提交 Bundle 2 (数据在 Node B):
  → 选择 Actor 2（本地性优先）

提交 Bundle 3 (数据在 Node A):
  → Actor 1: tasks=1, Actor 3: tasks=0
  → 选择 Actor 3（负载更低）

提交 Bundle 4 (数据在 Node C，无本地 Actor):
  → 所有 Actor 本地性相同（都是 INT32_MAX）
  → 选择 num_tasks_in_flight 最少的 Actor
```

---

## 5. Dashboard 状态显示逻辑

### 5.1 状态合并规则

根据 `TaskProgressBar.tsx` 第 51-59 行：

```typescript
{
  label: "Waiting for scheduling",
  value: numPendingNodeAssignment + numSubmittedToWorker,  // 两个状态合并
  color: theme.palette.warning.light,
},
{
  label: "Waiting for dependencies",
  value: numPendingArgsAvail,
  color: theme.palette.warning.main,
},
```

### 5.2 Dashboard 状态映射表

| Dashboard 显示 | 包含的 Ray Core TaskStatus |
|---------------|---------------------------|
| **Finished** | `FINISHED` |
| **Failed** | `FAILED` |
| **Running** | `RUNNING`, `RUNNING_IN_RAY_GET`, `RUNNING_IN_RAY_WAIT` |
| **Waiting for scheduling** | `PENDING_NODE_ASSIGNMENT` + `SUBMITTED_TO_WORKER` |
| **Waiting for dependencies** | `PENDING_ARGS_AVAIL` |
| **Cancelled** | `TASK_CANCELLED` |
| **Unknown** | `NIL` 或其他未分类状态 |

### 5.3 为什么 "Waiting for scheduling" 包含 `SUBMITTED_TO_WORKER`？

从用户视角来看：
- `PENDING_NODE_ASSIGNMENT`: 调度器还没选好在哪个节点执行
- `SUBMITTED_TO_WORKER`: 已选好节点，但 Worker 还没开始执行

这两个状态都是 **"等待被调度执行"** 的状态，用户通常不关心这个细节差异，所以 Dashboard 合并显示。

---

## 6. GCS 同步延迟分析与诊断

### 6.1 Task 状态上报机制

根据 `ray_config_def.h` 第 456-466 行：

```cpp
// Task 状态上报间隔（默认 1000ms = 1秒）
RAY_CONFIG(int64_t, task_events_report_interval_ms, 1000)

// GCS 中最多存储的 Task 数量（默认 100000）
RAY_CONFIG(int64_t, task_events_max_num_task_in_gcs, 100000)
```

**上报流程**：
1. Worker 执行任务时产生 `TaskStatusEvent`
2. 事件先缓存在 Worker 的 `TaskEventBuffer` 中
3. 每隔 `task_events_report_interval_ms` 批量上报到 GCS
4. Dashboard 从 GCS 查询状态

### 6.2 GCS 同步延迟的来源

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          GCS 同步延迟来源                                        │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  1. 上报间隔延迟 (task_events_report_interval_ms)                                │
│     - 默认 1 秒上报一次                                                          │
│     - Task 状态变化后最多 1 秒才会上报到 GCS                                      │
│                                                                                 │
│  2. GCS 处理延迟                                                                 │
│     - GCS 接收并处理 TaskEvents                                                  │
│     - 写入存储（内存或 Redis）                                                    │
│     - 指标: gcs_storage_operation_latency_ms                                    │
│                                                                                 │
│  3. Dashboard 查询延迟                                                           │
│     - Dashboard 定期从 GCS 拉取数据                                              │
│     - 前端渲染延迟                                                               │
│                                                                                 │
│  4. 网络延迟                                                                     │
│     - Worker → GCS 的网络延迟                                                    │
│     - 跨节点通信延迟                                                             │
│                                                                                 │
│  总延迟 ≈ 上报间隔 + GCS处理 + 查询间隔 + 网络 ≈ 1-3 秒                          │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 6.3 GCS 相关监控指标

根据 `src/ray/gcs/metrics.h`：

| 指标名称 | 描述 | 诊断用途 |
|---------|------|---------|
| `gcs_task_manager_task_events_reported` | GCS 收到的 task events 总数 | 确认上报正常 |
| `gcs_task_manager_task_events_dropped` | GCS 丢弃的 task events 数量 | 检查是否有丢失 |
| `gcs_task_manager_task_events_stored` | GCS 存储的 task events 数量 | 检查存储状态 |
| `gcs_storage_operation_latency_ms` | GCS 存储操作延迟 | 检查 GCS 性能 |
| `health_check_rpc_latency_ms` | 健康检查 RPC 延迟 | 检查 GCS 响应性 |

### 6.4 如何确认是 GCS 同步问题

**诊断步骤**：

1. **检查上报指标**：
   ```bash
   # 查看 Prometheus/Grafana 中的指标
   gcs_task_manager_task_events_reported  # 应持续增长
   gcs_task_manager_task_events_dropped   # 应为 0 或很低
   ```

2. **检查存储延迟**：
   ```bash
   gcs_storage_operation_latency_ms{Operation="Put"}  # 应 < 100ms
   ```

3. **对比时间戳**：
   - 记录 Ray Data 日志中的 `Tasks` 变化时间
   - 对比 Dashboard Task Table 的更新时间
   - 如果延迟 > 2 秒，可能存在同步问题

4. **检查 GCS 负载**：
   ```bash
   # GCS 进程的 CPU/内存使用
   # Redis 连接数和延迟（如果使用 Redis 后端）
   ```

### 6.5 GCS 同步延迟的影响

| 影响 | 表现 |
|------|------|
| Dashboard 状态滞后 | 显示的 Running/Waiting 数量与实际不符 |
| 进度条不准确 | 任务已完成但进度条还未更新 |
| 调试困难 | 无法实时观察任务状态 |
| 不影响执行 | GCS 同步延迟不影响实际任务调度和执行 |

---

## 7. Scheduling Loop 性能分析

### 7.1 Scheduling Loop 是什么

根据 `streaming_executor.py` 第 569-599 行：

```python
def _scheduling_loop_step(self, topology: Topology) -> bool:
    """Run one step of the scheduling loop.

    This runs a few general phases:
        1. Waiting for the next task completion using `ray.wait()`.
        2. Pulling completed refs into operator outqueues.
        3. Selecting and dispatching new inputs to operators.

    Returns:
        True if we should continue running the scheduling loop.
    """
    self._resource_manager.update_usages()

    # 处理已完成的任务（调用 ray.wait()）
    errored_blocks_per_op = process_completed_tasks(
        topology,
        self._backpressure_policies,
        self._max_errored_blocks,
    )
    # ... 后续处理
```

### 7.2 Scheduling Loop 延迟的影响

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                     Scheduling Loop 延迟 vs GCS 同步延迟                         │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  Scheduling Loop 延迟影响:                                                       │
│  ─────────────────────────                                                      │
│    1. 新任务提交延迟                                                             │
│       - Loop 每执行一轮才能提交新任务                                             │
│       - 如果 Loop 耗时长，任务提交会变慢                                          │
│                                                                                 │
│    2. 完成回调处理延迟                                                           │
│       - 任务完成后需要等待 Loop 执行 process_completed_tasks()                    │
│       - 影响 Ray Data _data_tasks 的更新                                         │
│                                                                                 │
│    3. 进度更新延迟                                                               │
│       - 进度条和日志在 Loop 中更新                                                │
│       - Loop 慢会导致日志中的 Tasks 数字更新不及时                                 │
│                                                                                 │
│  GCS 同步延迟影响:                                                               │
│  ─────────────────                                                              │
│    1. Dashboard Task Table 显示滞后                                              │
│    2. 不影响实际任务调度                                                          │
│    3. 不影响 Ray Data 层面的 Tasks 统计                                          │
│                                                                                 │
│  两者叠加效应:                                                                   │
│  ─────────────                                                                  │
│    Ray Data Tasks (由 Scheduling Loop 更新)                                      │
│    Dashboard Task Table (由 GCS 同步更新)                                        │
│    两者独立更新，可能产生更大的不一致性                                            │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 7.3 Scheduling Loop 监控指标

```python
# streaming_executor.py 第 139-143, 489-492 行
self._sched_loop_duration_s = Gauge(
    "data_sched_loop_duration_s",
    description="Duration of the scheduling loop in seconds",
    tag_keys=("dataset",),
)

def update_metrics(self, sched_loop_duration: int):
    self._sched_loop_duration_s.set(
        sched_loop_duration, tags={"dataset": self._dataset_id}
    )
```

**指标**：`data_sched_loop_duration_s`

**健康值**：
- 正常：< 100ms
- 警告：100ms - 500ms
- 异常：> 500ms

### 7.4 Scheduling Loop 延迟的常见原因

| 原因 | 诊断方法 | 解决方案 |
|------|---------|---------|
| `ray.wait()` 超时 | 检查网络和 Object Store | 增加节点或内存 |
| 大量任务完成处理 | 检查批量完成的任务数 | 调整 batch size |
| 反压策略计算 | Profile backpressure_policies | 简化反压策略 |
| Python GIL 竞争 | 检查 Driver CPU 使用 | 减少 Driver 端计算 |
| GC 暂停 | 检查 Python GC 频率 | 调整 GC 参数 |

---

## 8. 如何在日志中区分 Running 和排队的 Tasks

### 8.1 当前日志格式

当前 Ray Data 日志只显示总的 `Tasks` 数量：

```
2026-04-09 18:02:58,619 INFO logging_progress.py:231 -- Map(VideoClipProcessMapper): 17621744/19741737
2026-04-09 18:02:58,619 INFO logging_progress.py:233 -- Tasks: 189; Actors: 189; Queued blocks: 0
```

### 8.2 如何增强日志以区分 Running 和排队的 Tasks

**方法 1：修改 `format_op_state_summary()` 函数**

在 `streaming_executor_state.py` 中修改：

```python
def format_op_state_summary(op_state: OpState, resource_manager: ResourceManager, verbose: bool = False) -> str:
    """Get a formatted summary of the OpState for progress reporting."""

    # 获取 ActorPool 的详细信息
    actor_info = op_state.op.get_actor_info()

    # 如果 operator 有 ActorPool，获取详细任务分布
    if hasattr(op_state.op, '_actor_pool'):
        pool = op_state.op._actor_pool
        total_tasks = pool.num_tasks_in_flight()
        running_actors = pool.num_running_actors()
        max_concurrency = pool.max_actor_concurrency()

        # 估算 Running 任务数（每个 Actor 最多并发执行 max_concurrency 个任务）
        estimated_running = min(total_tasks, running_actors * max_concurrency)
        estimated_queued = total_tasks - estimated_running

        desc = f"Tasks: {total_tasks} (running≈{estimated_running}, queued≈{estimated_queued})"
    else:
        active = op_state.op.num_active_tasks()
        desc = f"Tasks: {active}"

    # ... 其余代码保持不变
```

**方法 2：通过 Ray Core API 查询实际状态**

```python
import ray
from ray._private.state_api_utils import summarize_tasks

def get_task_status_breakdown(job_id: str):
    """获取 Task 状态分布"""
    tasks = ray.state.list_tasks(filters=[("job_id", "=", job_id)])

    status_counts = {}
    for task in tasks:
        status = task.get("scheduling_state", "UNKNOWN")
        status_counts[status] = status_counts.get(status, 0) + 1

    return status_counts

# 使用示例
breakdown = get_task_status_breakdown("14000000")
print(f"RUNNING: {breakdown.get('RUNNING', 0)}")
print(f"SUBMITTED_TO_WORKER: {breakdown.get('SUBMITTED_TO_WORKER', 0)}")
print(f"PENDING_NODE_ASSIGNMENT: {breakdown.get('PENDING_NODE_ASSIGNMENT', 0)}")
```

**方法 3：通过 ActorPool 内部状态获取**

```python
# 在 ActorPoolMapOperator 中添加方法
def get_task_distribution(self) -> dict:
    """获取任务分布详情"""
    result = {
        "total_tasks_in_flight": self._actor_pool.num_tasks_in_flight(),
        "per_actor": {}
    }

    for actor, state in self._actor_pool._running_actors.items():
        actor_id = self._actor_pool.get_actor_id(actor)
        result["per_actor"][actor_id] = {
            "num_tasks": state.num_tasks_in_flight,
            "is_restarting": state.is_restarting,
            "location": state.actor_location,
        }

    return result
```

### 8.3 建议的增强日志格式

```
# 当前格式
Tasks: 189; Actors: 189

# 建议的增强格式
Tasks: 189 (running=100, queued=89); Actors: 189 (active=100, idle=89)

# 或更详细的格式
Tasks: 189 [RUNNING=100, SUBMITTED_TO_WORKER=89]; Actors: 189/189 active
```

---

## 9. Actor 并发参数详解

### 9.1 核心参数定义

#### `max_actor_concurrency`（也叫 `max_concurrency`）

**定义**：Actor 内部可以同时执行的 UDF 调用数量。

**来源**：`actor_pool_map_operator.py` 第 168 行：

```python
max_actor_concurrency = self._ray_remote_args.get("max_concurrency", 1)
```

**默认值**：1（单线程执行）

#### `max_tasks_in_flight_per_actor`

**定义**：每个 Actor 最多可以排队的任务总数（包括正在执行和等待执行的）。

**来源**：`actor_pool_map_operator.py` 第 179-183 行：

```python
max_tasks_in_flight_per_actor=(
    compute_strategy.max_tasks_in_flight_per_actor
    or data_context.max_tasks_in_flight_per_actor
    or max_actor_concurrency * DEFAULT_ACTOR_MAX_TASKS_IN_FLIGHT_TO_MAX_CONCURRENCY_FACTOR
)
```

**默认值**：`max_actor_concurrency * 2`

**相关常量**（`context.py` 第 253-255 行）：

```python
DEFAULT_ACTOR_MAX_TASKS_IN_FLIGHT_TO_MAX_CONCURRENCY_FACTOR = env_integer(
    "RAY_DATA_ACTOR_DEFAULT_MAX_TASKS_IN_FLIGHT_TO_MAX_CONCURRENCY_FACTOR", 2
)
```

### 9.2 参数之间的关系

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                     参数关系与任务状态                                            │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  max_tasks_in_flight_per_actor = 4                                              │
│  max_actor_concurrency = 1                                                      │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                           Actor 内部                                     │   │
│  │                                                                          │   │
│  │   ┌───────────────┐   ┌───────────────────────────────────────────────┐ │   │
│  │   │   执行中 (1)   │   │              排队中 (3)                        │ │   │
│  │   │               │   │                                               │ │   │
│  │   │    Task 1     │   │   Task 2  →  Task 3  →  Task 4                │ │   │
│  │   │   (RUNNING)   │   │  (SUBMITTED_TO_WORKER × 3)                    │ │   │
│  │   │               │   │                                               │ │   │
│  │   └───────────────┘   └───────────────────────────────────────────────┘ │   │
│  │                                                                          │   │
│  │   num_tasks_in_flight = 4                                               │   │
│  │   estimated_running = min(4, 1) = 1                                     │   │
│  │   estimated_queued = 4 - 1 = 3                                        │   │
│  │                                                                          │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
│  如果 max_actor_concurrency = 4:                                                │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                           Actor 内部                                     │   │
│  │                                                                          │   │
│  │   ┌───────────────────────────────────────────────────────────────────┐ │   │
│  │   │                     并发执行中 (4)                                  │ │   │
│  │   │                                                                    │ │   │
│  │   │   Task 1  |  Task 2  |  Task 3  |  Task 4                         │ │   │
│  │   │   (RUNNING × 4)                                                   │ │   │
│  │   │                                                                    │ │   │
│  │   └───────────────────────────────────────────────────────────────────┘ │   │
│  │                                                                          │   │
│  │   排队中: 无                                                              │   │
│  │   estimated_running = min(4, 4) = 4                                     │   │
│  │   estimated_queued = 4 - 4 = 0                                          │   │
│  │                                                                          │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 9.3 默认值计算示例

| max_concurrency | max_tasks_in_flight_per_actor (默认) | 说明 |
|-----------------|--------------------------------------|------|
| 1 | 1 × 2 = 2 | 1 个执行 + 1 个预取 |
| 2 | 2 × 2 = 4 | 2 个执行 + 2 个预取 |
| 4 | 4 × 2 = 8 | 4 个执行 + 4 个预取 |

### 9.4 什么场景需要设置 `max_concurrency > 1`？

根据 `compute.py` 第 93-107 行的文档：

```python
class ActorPoolStrategy(ComputeStrategy):
    """
    Parameters:
        max_concurrency:
            The max number of concurrent calls to the actor's work method
            per actor. If True multi-threading is not enabled
            (i.e., `enable_true_multi_threading=False`), then the
            actor will batch up to `max_concurrency` input rows to
            the actor's work method at once.
            This allows for efficient pipelining of I/O-bound workloads
            such as GPU inference where the actor can be processing
            one batch while the next batch is being fetched.
    """
```

**适用场景**：

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    max_concurrency > 1 的使用场景                                │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  1. GPU 推理 + 数据预取流水线                                                    │
│     ─────────────────────────                                                   │
│     场景: GPU 推理时，数据加载是瓶颈                                              │
│     方案: max_concurrency=4 允许 4 个 Task 同时进入 Actor                        │
│     效果: 当 Task 1 在 GPU 上推理时，Task 2/3/4 可以并行加载数据                  │
│                                                                                 │
│     ┌──────────────────────────────────────────────────────────────────────┐   │
│     │  Time  │ Task 1      │ Task 2      │ Task 3      │ Task 4           │   │
│     │────────┼─────────────┼─────────────┼─────────────┼──────────────────│   │
│     │  t0    │ 数据加载    │ 数据加载    │ 数据加载    │ 数据加载 (并行)   │   │
│     │  t1    │ GPU 推理    │ 等待        │ 等待        │ 等待              │   │
│     │  t2    │ 返回结果    │ GPU 推理    │ 等待        │ 等待              │   │
│     │  t3    │             │ 返回结果    │ GPU 推理    │ 等待              │   │
│     │  t4    │             │             │ 返回结果    │ GPU 推理          │   │
│     └──────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
│  2. I/O 密集型 UDF                                                              │
│     ─────────────────                                                           │
│     场景: UDF 需要从远程服务获取数据                                              │
│     方案: max_concurrency > 1 + enable_true_multi_threading=False              │
│     效果: 多个请求可以并发发起，但 UDF 执行仍然串行                               │
│                                                                                 │
│  3. 真正的多线程执行                                                             │
│     ─────────────────                                                           │
│     场景: UDF 是线程安全的，且 CPU 密集                                           │
│     方案: max_concurrency > 1 + enable_true_multi_threading=True               │
│     效果: 多个 UDF 调用真正并行执行                                               │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 9.5 `enable_true_multi_threading` 参数

**默认值**：`False`

**行为差异**：

| 参数组合 | UDF 执行方式 | 数据获取 |
|---------|-------------|---------|
| `max_concurrency=1` | 串行 | 串行 |
| `max_concurrency=4, enable_true_multi_threading=False` | 串行（通过 ThreadPoolExecutor(1)） | **并行** |
| `max_concurrency=4, enable_true_multi_threading=True` | **并行** | **并行** |

**实现机制**（`util.py` 第 58-82 行）：

```python
def make_callable_class_single_threaded(callable_cls: CallableClass) -> CallableClass:
    """Wrap a callable class to execute in a single-threaded manner.

    This is used to ensure that the callable class is only executed by
    one thread at a time, even if the actor is running with max_concurrency > 1.
    """

    class _SingleThreadedWrapper(callable_cls):
        def __init__(self, *args, **kwargs):
            # 只允许一个线程执行
            self.thread_pool_executor = ThreadPoolExecutor(max_workers=1)
            super().__init__(*args, **kwargs)

        def __call__(self, *args, **kwargs):
            # 提交到单线程池，保证串行执行
            future = self.thread_pool_executor.submit(super().__call__, *args, **kwargs)
            return future.result()

        def __del__(self):
            self.thread_pool_executor.shutdown(wait=False)

    return _SingleThreadedWrapper
```

**为什么默认是 `False`？**

- 大多数 UDF 不是线程安全的（使用全局状态、非线程安全的库等）
- `enable_true_multi_threading=False` + `max_concurrency > 1` 可以实现：
  - 数据获取并行化（利用 Ray Core 的异步参数获取）
  - UDF 执行串行化（保证安全性）
- 这是一种安全的优化方式

---

## 10. Batch Input 并行逻辑 (Ray Core C++ 层面)

### 10.1 整体架构

当 `max_concurrency > 1` 时，多个 Task 可以同时进入 Actor。Ray Core C++ 层实现了参数的并行获取：

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        Ray Core C++ 层面                                         │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  ┌──────────────────────────────────────────────────────────────────────────┐   │
│  │            UnorderedActorTaskExecutionQueue                               │   │
│  │                                                                           │   │
│  │   Task 1 ──┐                                                              │   │
│  │   Task 2 ──┼──> waiter_.AsyncWait() ──> PENDING_ACTOR_TASK_ARGS_FETCH    │   │
│  │   Task 3 ──┤        (并行异步等待)                                         │   │
│  │   Task 4 ──┘                                                              │   │
│  └───────────────────────────────────────────────────────────────────────────┘   │
│                     │                                                            │
│                     ▼                                                            │
│  ┌──────────────────────────────────────────────────────────────────────────┐   │
│  │            ActorTaskExecutionArgWaiter                                    │   │
│  │                                                                           │   │
│  │   async_wait_for_args_(args, tag) ─────────────────────────────────────► │   │
│  │                     │                                                     │   │
│  │                     ▼ IPC call                                            │   │
│  │        raylet_ipc_client_->WaitForActorCallArgs()                         │   │
│  └───────────────────────────────────────────────────────────────────────────┘   │
│                     │                                                            │
│                     ▼                                                            │
│  ┌──────────────────────────────────────────────────────────────────────────┐   │
│  │                 NodeManager (Raylet)                                      │   │
│  │                                                                           │   │
│  │   ProcessWaitForActorCallArgsRequestMessage()                             │   │
│  │        │                                                                  │   │
│  │        ├─> AsyncWait() ─> LeaseDependencyManager                          │   │
│  │        │                       │                                          │   │
│  │        │                       └─> object_manager_.Pull()                 │   │
│  │        │                                (发起 Object Pull)                │   │
│  │        │                                                                  │   │
│  │        └─> wait_manager_.Wait() ─> 等待所有 Object 就绪                    │   │
│  │                                       │                                   │   │
│  │                                       └─> worker->ActorCallArgWaitComplete(tag)
│  └───────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 10.2 关键代码路径

#### 步骤 1：Actor 收到多个 Task

`unordered_actor_task_execution_queue.cc` 第 170-207 行：

```cpp
void UnorderedActorTaskExecutionQueue::RunRequest(TaskToExecute request) {
  if (!request.PendingDependencies().empty()) {
    // ① 记录状态为 PENDING_ACTOR_TASK_ARGS_FETCH
    task_event_buffer_.RecordTaskStatusEventIfNeeded(
        task_spec.TaskId(),
        task_spec.JobId(),
        task_spec.AttemptNumber(),
        task_spec,
        rpc::TaskStatus::PENDING_ACTOR_TASK_ARGS_FETCH,
        /* include_task_info */ false);

    // ② 发起异步等待（非阻塞！可以同时为多个 Task 发起）
    auto dependencies = request.PendingDependencies();
    waiter_.AsyncWait(dependencies, [this, request = std::move(request)]() mutable {
      // ③ 参数准备好后的回调
      const TaskSpecification &task = request.TaskSpec();
      task_event_buffer_.RecordTaskStatusEventIfNeeded(
          task.TaskId(),
          task.JobId(),
          task.AttemptNumber(),
          task,
          rpc::TaskStatus::PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY,
          /* include_task_info */ false);

      request.MarkDependenciesResolved();
      RunRequestWithResolvedDependencies(std::move(request));
    });
  } else {
    // 无依赖，直接执行
    request.MarkDependenciesResolved();
    RunRequestWithResolvedDependencies(std::move(request));
  }
}
```

**关键点**：`waiter_.AsyncWait()` 是**非阻塞**的，可以同时为多个 Task 发起参数获取。

#### 步骤 2：AsyncWait 实现

`common.cc` 第 65-70 行：

```cpp
void ActorTaskExecutionArgWaiter::AsyncWait(
    const std::vector<rpc::ObjectReference> &args,
    std::function<void()> on_args_ready) {
  auto tag = next_tag_++;
  in_flight_waits_.emplace(tag, std::move(on_args_ready));  // 保存回调
  async_wait_for_args_(args, tag);  // 发起异步等待
}
```

`in_flight_waits_` 是一个 map，可以同时保存**多个 Task 的回调**。

#### 步骤 3：实际的参数获取（Raylet 层）

`core_worker.cc` 第 386-390 行：

```cpp
actor_task_execution_arg_waiter_ = std::make_unique<ActorTaskExecutionArgWaiter>(
    [this](const std::vector<rpc::ObjectReference> &args, int64_t tag) {
      // 通过 IPC 调用 Raylet 的 WaitForActorCallArgs
      RAY_CHECK_OK(raylet_ipc_client_->WaitForActorCallArgs(args, tag))
          << "WaitForActorCallArgs IPC failed unexpectedly";
    });
```

#### 步骤 4：Raylet 处理请求

`node_manager.cc` 第 1683-1709 行：

```cpp
void NodeManager::ProcessWaitForActorCallArgsRequestMessage(
    const std::shared_ptr<ClientConnection> &client, const uint8_t *message_data) {
  auto message = flatbuffers::GetRoot<protocol::WaitForActorCallArgsRequest>(message_data);
  auto object_ids = FlatbufferToObjectIds(*message->object_ids());
  int64_t tag = message->tag();

  // ① 发起 Object Pull（异步从其他节点拉取数据）
  const auto refs = FlatbufferToObjectReferences(*message->object_ids(), *message->owner_addresses());
  AsyncWait(client, refs);  // -> LeaseDependencyManager.StartOrUpdateWaitRequest()

  // ② 等待所有 Object 就绪
  wait_manager_.Wait(object_ids, -1, object_ids.size(),
    [this, client, tag](const std::vector<ObjectID> &ready,
                        const std::vector<ObjectID> &remaining) {
      RAY_CHECK(remaining.empty());
      std::shared_ptr<WorkerInterface> worker = worker_pool_.GetRegisteredWorker(client);
      if (worker) {
        // ③ 所有参数就绪后通知 Worker
        worker->ActorCallArgWaitComplete(tag);
      }
    });
}
```

#### 步骤 5：LeaseDependencyManager 发起 Object Pull

`lease_dependency_manager.cc` 第 69-93 行：

```cpp
void LeaseDependencyManager::StartOrUpdateWaitRequest(
    const WorkerID &worker_id,
    const std::vector<rpc::ObjectReference> &required_objects) {
  auto &wait_request = wait_requests_[worker_id];
  for (const auto &ref : required_objects) {
    const auto obj_id = ObjectRefToId(ref);
    if (local_objects_.contains(obj_id)) {
      // Object 已在本地，无需拉取
      continue;
    }

    if (wait_request.insert(obj_id).second) {
      auto it = GetOrInsertRequiredObject(obj_id, ref);
      it->second.dependent_wait_requests.insert(worker_id);
      if (it->second.wait_request_id == 0) {
        // 发起异步 Pull 请求
        it->second.wait_request_id =
            object_manager_.Pull({ref}, BundlePriority::WAIT_REQUEST, {"", false});
      }
    }
  }
}
```

### 10.3 并行性体现

| 层级 | 并行机制 |
|------|----------|
| **Actor Task Queue** | 多个 Task 可以同时调用 `AsyncWait()`，每个获得一个唯一的 `tag` |
| **IPC 调用** | 每个 `WaitForActorCallArgs` 是独立的 IPC 调用，Raylet 可以并行处理 |
| **Object Pull** | `PullManager` 内部维护请求队列，可以并行从多个节点拉取多个 Object |
| **回调通知** | 每个 Task 的参数就绪后，通过 `tag` 独立触发对应的回调 |

### 10.4 执行流程示例

假设 `max_concurrency=4`，Actor 同时收到 4 个 Task：

```
时间线:
t0: Task1 入队 -> AsyncWait(args1, tag=1) -> PENDING_ACTOR_TASK_ARGS_FETCH
t0: Task2 入队 -> AsyncWait(args2, tag=2) -> PENDING_ACTOR_TASK_ARGS_FETCH
t0: Task3 入队 -> AsyncWait(args3, tag=3) -> PENDING_ACTOR_TASK_ARGS_FETCH
t0: Task4 入队 -> AsyncWait(args4, tag=4) -> PENDING_ACTOR_TASK_ARGS_FETCH

              ┌── Object Store Pull (并行) ──┐
              │   args1 从 Node A 拉取       │
              │   args2 从 Node B 拉取       │
              │   args3 从 Node A 拉取       │
              │   args4 从 Node C 拉取       │
              └─────────────────────────────┘

t1: args2 就绪 -> MarkReady(tag=2) -> Task2 进入 PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY
t2: args4 就绪 -> MarkReady(tag=4) -> Task4 进入 PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY
t3: args1 就绪 -> MarkReady(tag=1) -> Task1 进入 PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY
t4: args3 就绪 -> MarkReady(tag=3) -> Task3 进入 PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY
```

**关键结论**：参数获取是**乱序完成**的（谁的数据先到谁先就绪），但 UDF 执行可以是串行的（取决于 `enable_true_multi_threading`）。

### 10.5 与 Python 层的关系

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                     参数获取与 UDF 执行的解耦                                     │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  Python 层（enable_true_multi_threading=False 时）:                             │
│  ─────────────────────────────────────────────────                              │
│                                                                                 │
│  ┌───────────────────────────────────────────────────────────────────────────┐ │
│  │                     Actor (Python)                                        │ │
│  │                                                                           │ │
│  │   ThreadPoolExecutor(max_workers=1)                                       │ │
│  │        │                                                                  │ │
│  │        ├─> UDF(args1) ─────────────────┐                                  │ │
│  │        │                               │ 串行执行                          │ │
│  │        └─> UDF(args2) ─ wait ──────────┘                                  │ │
│  │                                                                           │ │
│  └───────────────────────────────────────────────────────────────────────────┘ │
│                                                                                 │
│  C++ 层:                                                                        │
│  ───────                                                                        │
│                                                                                 │
│  ┌───────────────────────────────────────────────────────────────────────────┐ │
│  │                     Ray Core (C++)                                        │ │
│  │                                                                           │ │
│  │   AsyncWait(args1, tag=1) ──┐                                             │ │
│  │   AsyncWait(args2, tag=2) ──┼──> Object Pull (并行)                       │ │
│  │   AsyncWait(args3, tag=3) ──┤                                             │ │
│  │   AsyncWait(args4, tag=4) ──┘                                             │ │
│  │                                                                           │ │
│  └───────────────────────────────────────────────────────────────────────────┘ │
│                                                                                 │
│  效果:                                                                          │
│  ─────                                                                          │
│    - 数据拉取在 C++ 层并行进行                                                   │
│    - UDF 执行在 Python 层串行进行（通过 ThreadPoolExecutor(1)）                  │
│    - 实现了 "数据预取" 与 "UDF 执行" 的流水线化                                   │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

这就是为什么文档说 `max_concurrency > 1` 可以用于 "GPU inference 场景下的数据预取"——**数据拉取和 UDF 执行可以流水线化**，即使 UDF 本身是串行执行的。

---

## 11. 问题诊断指南

### 11.1 常见问题与诊断流程

#### 问题 1：Ray Data Tasks 与 Dashboard 显示差距大

**诊断步骤**：

```
Step 1: 确认差距类型
  ├─ Ray Data Tasks > Dashboard Running+Waiting
  │   → 可能是 GCS 同步延迟
  │
  └─ Ray Data Tasks < Dashboard 总数
      → Dashboard 包含已完成的 Task

Step 2: 检查 GCS 同步
  ├─ 查看 gcs_task_manager_task_events_reported 指标
  ├─ 查看 gcs_storage_operation_latency_ms 指标
  └─ 对比时间戳

Step 3: 检查 Scheduling Loop
  ├─ 查看 data_sched_loop_duration_s 指标
  └─ 检查 Driver 节点 CPU/内存

Step 4: 检查 Prefetch 配置
  ├─ max_tasks_in_flight_per_actor 是否过大
  └─ 导致大量 SUBMITTED_TO_WORKER 状态
```

#### 问题 2："Waiting for scheduling" 数量很高

**诊断步骤**：

```
Step 1: 区分是 PENDING_NODE_ASSIGNMENT 还是 SUBMITTED_TO_WORKER
  ├─ 使用 ray.state.list_tasks() 查看详细状态
  │
  ├─ 如果大量 PENDING_NODE_ASSIGNMENT:
  │   → 资源不足，检查集群资源
  │   → 检查 request_resources 是否合理
  │
  └─ 如果大量 SUBMITTED_TO_WORKER:
      → 这是正常的 prefetch 行为
      → 可以调小 max_tasks_in_flight_per_actor

Step 2: 检查 Actor 状态
  └─ 是否有 Actor 在 RESTARTING 状态
```

#### 问题 3：Task 进度停滞

**诊断步骤**：

```
Step 1: 检查是否有任务卡住
  ├─ 查看 Task Table 中长时间 RUNNING 的任务
  └─ 检查对应 Worker 的日志

Step 2: 检查反压状态
  ├─ 查看日志中是否有 [backpressured:...] 提示
  └─ 检查 Object Store 内存使用

Step 3: 检查上游依赖
  └─ 上游 Stage 是否正常产出数据
```

### 11.2 GCS 相关监控指标

| 指标 | 正常范围 | 异常表现 |
|------|---------|---------|
| `data_sched_loop_duration_s` | < 0.1s | > 0.5s 说明调度慢 |
| `gcs_storage_operation_latency_ms` | < 100ms | > 1000ms 说明 GCS 慢 |
| `gcs_task_manager_task_events_dropped` | 0 | > 0 说明有丢失 |
| `object_store_memory` 使用率 | < 80% | > 90% 触发反压 |
| `SUBMITTED_TO_WORKER` 比例 | < 50% of Tasks | > 80% 说明 prefetch 过多 |

### 11.3 配置调优建议

| 场景 | 配置 | 建议值 |
|------|------|-------|
| 减少 "Waiting for scheduling" | `max_tasks_in_flight_per_actor` | 2-4 |
| 加快 GCS 同步 | `RAY_task_events_report_interval_ms` | 500 |
| 减少 Dashboard 延迟 | Dashboard 刷新间隔 | 1s |
| 减少调度延迟 | 增加 Driver 资源 | 4+ CPU |

---

## 附录

### A. 相关源代码文件

| 文件 | 描述 |
|------|------|
| `src/ray/protobuf/common.proto` | TaskStatus 枚举定义 |
| `python/ray/data/_internal/execution/streaming_executor_state.py` | 状态管理和日志格式化 |
| `python/ray/data/_internal/execution/operators/map_operator.py` | MapOperator 基类 |
| `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py` | ActorPool 实现 |
| `python/ray/data/_internal/execution/util.py` | 工具函数，包含 `make_callable_class_single_threaded()` |
| `python/ray/data/_internal/compute.py` | ComputeStrategy 定义，包含 `ActorPoolStrategy` |
| `python/ray/data/context.py` | Ray Data 全局配置，包含默认参数 |
| `python/ray/data/_internal/progress/logging_progress.py` | 日志进度管理 |
| `python/ray/dashboard/client/src/pages/job/TaskProgressBar.tsx` | Dashboard 进度条 |
| `src/ray/core_worker/task_event_buffer.h` | Task 事件缓冲 |
| `src/ray/core_worker/task_execution/unordered_actor_task_execution_queue.cc` | Actor Task 队列实现 |
| `src/ray/core_worker/task_execution/common.cc` | ActorTaskExecutionArgWaiter 实现 |
| `src/ray/raylet/node_manager.cc` | Raylet 节点管理，处理 WaitForActorCallArgs |
| `src/ray/raylet/lease_dependency_manager.cc` | 租约依赖管理，处理 Object Pull |
| `src/ray/gcs/metrics.h` | GCS 监控指标 |

### B. 环境变量

| 变量 | 默认值 | 描述 |
|------|-------|------|
| `RAY_task_events_report_interval_ms` | 1000 | Task 状态上报间隔 |
| `RAY_task_events_max_num_task_in_gcs` | 100000 | GCS 最大 Task 数量 |
| `RAY_DATA_NON_TTY_PROGRESS_LOG_INTERVAL` | 10 | 非 TTY 日志间隔(秒) |
| `RAY_DATA_ACTOR_DEFAULT_MAX_TASKS_IN_FLIGHT_TO_MAX_CONCURRENCY_FACTOR` | 2 | max_tasks_in_flight 与 max_concurrency 的倍数关系 |

### C. 参考链接

- [Ray Data 官方文档](https://docs.ray.io/en/latest/data/data.html)
- [Ray Core Task 状态文档](https://docs.ray.io/en/latest/ray-core/tasks.html)
- [Ray Dashboard 文档](https://docs.ray.io/en/latest/ray-observability/ray-dashboard.html)
- [Ray Actor 并发文档](https://docs.ray.io/en/latest/ray-core/actors/async_api.html)
