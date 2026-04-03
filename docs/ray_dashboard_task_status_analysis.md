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

---

## 1. 概述

### 1.1 问题背景

在使用 Ray Dashboard 监控作业时，经常会遇到以下困惑：

- Progress Bar 显示大量 "Unaccounted" 任务
- "Waiting for scheduling" 数量与 Task Table 中 `submitted_to_worker` 状态不一致
- Ray Data 日志显示的任务数与 Dashboard 显示的数量不匹配
- 不清楚各种任务状态（如 `PENDING_ACTOR_TASK_ARGS_FETCH`）的含义

### 1.2 本文档目标

- 解释 Ray Core 的任务状态机制
- 说明 Dashboard 如何映射和显示这些状态
- 分析数据不一致的原因和解决方案
- 提供配置调优建议

---

## 2. Ray Core 任务状态详解

### 2.1 完整任务状态列表

Ray Core 定义了以下任务状态（来自 `gcs.proto`）：

```protobuf
enum TaskStatus {
  NIL = 0;
  PENDING_ARGS_AVAIL = 1;        // 等待参数可用
  PENDING_NODE_ASSIGNMENT = 2;   // 等待节点分配
  PENDING_OBJ_STORE_MEM_AVAIL = 3; // 等待 Object Store 内存
  PENDING_ARGS_FETCH = 4;        // 等待参数拉取
  SUBMITTED_TO_WORKER = 5;       // 已提交到 Worker
  RUNNING = 6;                   // 正在运行
  RUNNING_IN_RAY_GET = 7;        // 在 ray.get() 中运行
  RUNNING_IN_RAY_WAIT = 8;       // 在 ray.wait() 中运行
  FINISHED = 9;                  // 已完成
  FAILED = 10;                   // 已失败

  // Actor Task 专用状态
  PENDING_ACTOR_TASK_ARGS_FETCH = 11;              // Actor 任务等待参数拉取
  PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY = 12; // Actor 任务等待排序或并发控制
}
```

### 2.2 任务状态流转图

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Ray Core 任务状态流转                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  普通 Task 流程:                                                              │
│  ──────────────────────────────────────────────────────────────────────────  │
│                                                                              │
│  PENDING_ARGS_AVAIL ─→ PENDING_NODE_ASSIGNMENT ─→ PENDING_ARGS_FETCH        │
│         │                      │                         │                   │
│         │                      ▼                         ▼                   │
│         │            PENDING_OBJ_STORE_MEM_AVAIL    SUBMITTED_TO_WORKER     │
│         │                      │                         │                   │
│         │                      └─────────────────────────┤                   │
│         │                                                ▼                   │
│         └──────────────────────────────────────────→ RUNNING                │
│                                                          │                   │
│                                           ┌──────────────┼──────────────┐    │
│                                           ▼              ▼              ▼    │
│                                    RUNNING_IN_GET  RUNNING_IN_WAIT  FINISHED │
│                                                                      FAILED  │
│                                                                              │
│  Actor Task 流程:                                                            │
│  ──────────────────────────────────────────────────────────────────────────  │
│                                                                              │
│  PENDING_ARGS_AVAIL ─→ PENDING_ACTOR_TASK_ARGS_FETCH                        │
│         │                         │                                          │
│         │                         ▼                                          │
│         │            PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY              │
│         │                         │                                          │
│         │                         ▼                                          │
│         └───────────────────→ RUNNING ───→ FINISHED / FAILED                │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.3 Actor Task 专用状态解释

| 状态 | 含义 | 触发条件 |
|------|------|---------|
| `PENDING_ACTOR_TASK_ARGS_FETCH` | Actor 任务正在等待参数对象从 Object Store 拉取 | 任务参数是 ObjectRef，需要从其他节点获取 |
| `PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY` | Actor 任务在 Actor 的任务队列中等待 | Actor 有 `max_concurrency` 限制，或前置任务未完成 |

**关键理解**：这两个状态表示任务**已经发送到 Actor 所在的 Worker**，但由于参数未就绪或并发限制，尚未开始执行。

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
  PENDING_OBJ_STORE_MEM_AVAIL: TaskStatus.PENDING_NODE_ASSIGNMENT,  // ★ 合并
  PENDING_ARGS_FETCH: TaskStatus.PENDING_NODE_ASSIGNMENT,           // ★ 合并

  // 已提交到 Worker - 合并 Actor 任务状态
  SUBMITTED_TO_WORKER: TaskStatus.SUBMITTED_TO_WORKER,
  PENDING_ACTOR_TASK_ARGS_FETCH: TaskStatus.SUBMITTED_TO_WORKER,              // ★ 合并
  PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY: TaskStatus.SUBMITTED_TO_WORKER, // ★ 合并

  // 运行中 - 合并阻塞状态
  RUNNING: TaskStatus.RUNNING,
  RUNNING_IN_RAY_GET: TaskStatus.RUNNING,  // ★ 合并
  RUNNING_IN_RAY_WAIT: TaskStatus.RUNNING, // ★ 合并

  // 终态
  FINISHED: TaskStatus.FINISHED,
  FAILED: TaskStatus.FAILED,
  NIL: TaskStatus.UNKNOWN,
};
```

### 3.2 Progress Bar 显示分类

```typescript
// TaskProgressBar.tsx:34-73
const progress: ProgressBarSegment[] = [
  { label: "Finished", value: numFinished },
  { label: "Failed", value: numFailed },
  { label: "Running", value: numRunning },
  { label: "Waiting for scheduling", value: numPendingNodeAssignment + numSubmittedToWorker },  // ★
  { label: "Waiting for dependencies", value: numPendingArgsAvail },
  { label: "Cancelled", value: numCancelled },
  { label: "Unknown", value: numUnknown },
];
```

**重要**："Waiting for scheduling" 包含了：
- `numPendingNodeAssignment` (等待节点分配)
- `numSubmittedToWorker` (已提交到 Worker，包括 Actor 任务的等待状态)

### 3.3 状态映射完整表格

| Ray Core 状态 | Dashboard TaskStatus | Progress Bar 显示 |
|--------------|---------------------|------------------|
| `PENDING_ARGS_AVAIL` | `PENDING_ARGS_AVAIL` | Waiting for dependencies |
| `PENDING_NODE_ASSIGNMENT` | `PENDING_NODE_ASSIGNMENT` | Waiting for scheduling |
| `PENDING_OBJ_STORE_MEM_AVAIL` | `PENDING_NODE_ASSIGNMENT` | Waiting for scheduling |
| `PENDING_ARGS_FETCH` | `PENDING_NODE_ASSIGNMENT` | Waiting for scheduling |
| `SUBMITTED_TO_WORKER` | `SUBMITTED_TO_WORKER` | Waiting for scheduling |
| `PENDING_ACTOR_TASK_ARGS_FETCH` | `SUBMITTED_TO_WORKER` | Waiting for scheduling |
| `PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY` | `SUBMITTED_TO_WORKER` | Waiting for scheduling |
| `RUNNING` | `RUNNING` | Running |
| `RUNNING_IN_RAY_GET` | `RUNNING` | Running |
| `RUNNING_IN_RAY_WAIT` | `RUNNING` | Running |
| `FINISHED` | `FINISHED` | Finished |
| `FAILED` | `FAILED` | Failed |
| `NIL` | `UNKNOWN` | Unknown |

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
          value: finalTotal - segmentTotal,  // ★ Unaccounted
          label: "Unaccounted",
          hint: "Unaccounted tasks can happen when there are too many tasks. " +
                "Ray drops older tasks to conserve memory.",
        },
      ]
    : progress;
```

**公式**：
```
Unaccounted = num_filtered - sum(state_counts)
            = 总任务数 - 各状态计数之和
```

### 5.2 产生 Unaccounted 的原因

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Unaccounted 产生机制                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  1. GCS Task Events 存储限制                                         │
│     ┌─────────────────────────────────────────────────────────────────────┐  │
│     │  task_events_max_in_gcs = 100,000 (默认)                   │  │
│     │                                                             │  │
│     │  当任务事件数 > 100,000 时:                                          │  │
│     │    - GCS 丢弃最旧的任务事件                                     │  │
│     │    - 被丢弃的任务仍计入 num_filtered (总数)                           │  │
│     │    - 但无法归_counts (各状态计数)                           │  │
│     │    - 结果: Unaccounted = num_filtered - sum(state_counts) > 0       │└─────────────────────────────────────────────────────────────────────┘  │
│                                                                    │
│  2. 数据源截断                                                                │
│     ┌──────────────────────────────────────────────────────────┐  │
│     │  RAY_MAX_LIMIT_FROM_DATA_SOURCE = 10,000 (默认)                     │  │
│     │                                                            │  │
│     │  当查询结果 > 10,000 时:                                        │
│     │    - State API 只返回前 10,000 条                                    │  │
│     │    - 但 num_filtered 记录的是过滤后的总数                    │  │
│     │    - 超出部分的状态无法被正确统计                                      │  │
│     └──────────────────────────────────────────────────────────────┘  │
│                                                                              │
│  3. 状态同步延迟                                                              │
│     ┌─────────────────────────────────────────────────────────────────────┐  │
│     │  task_events_erval_ms = 1000 (默认)                       │  │
│     │                                                                  │  │
│     │  Worker 每秒上报一次 task events:                                    │  │
│     │    - 快速变化的任务状态可能未及时上报                                  │  │
│     │    - 聚合时可能遗漏部分状态变化                                        │  │
│     └─────────────────────────────────────────────────────────┘  │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

.3 Unaccounted 的影响

| 影响 | 说明 |
|------|------|
| **仅影响显示** | Unaccounted 只影响 Dashboard 的显示，不影响实际任务执行 |
| **正常现象** | 在大规模任务场景下，这是正常的内存保护机制 |
| **不影响调度** | Ray Core 的调度器不依赖 Dashboard 的状态显示 |

### 5.4 减少 Unaccounted 的方法

```bash
# 方法 1: 增加 GCS 中保留的 task events 数量
export RAY_task_events_max_num_task_in_gcs=500000  # 默认 100,000

# 方法 2: 增加 State API 返回的export RAY_MAX_LIMIT_FROM_DATA_SOURCE=100000  # 默认 10,000
export RAY_MAX_LIMIT_FROM_API_SERVER=100000   # 默认 10,000

# 方法 3: 加快 task events 上报频率
export RAY_task_events_report_interval_ms=500  # 默认 1000
```

**注意**：增加这些限制会增加内存使用，需要权衡。

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
├──────────────────────────────────────────────────────────────────┤
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
│  ───────────────────────────────────────────────────────────────── │
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
| `RAY_MAX_LIMIT_FROM_DATA_SOURCE` |,000 | State API 从数据源获取的最大条目数 | 需要完整数据时可增加 |
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

## 9. 关键代码位置

### 9.1 前端代码

| 文件路径 | 说明 |
|---------|------|
| `python/ray/dashboard/client/src/pages/job/hook/useJobProgress.ts` | Progress Bar 数据获取和状态映射 |
| `python/ray/dashboard/client/src/pages/job/TaskProgressBar.tsx` | Progress Bar 渲染逻辑 |
| `python/ray/dashboard/client/src/components/ProgressBar/ProgressBar.tsx` | ProgressBar 组件，Unaccounted 计算 |
| `python/ray/dashboard/client/src/type/task.ts` | TypeTaskStatus 枚举定义 |

### 9.2 后端代码

| 文件路径 | 说明 |
|---------|------|
| `python/ray/dashboard/state_aggregator.py` | State API 聚合逻辑，summarize_tasks |
| `python/ray/util/state/common.py` | TaskSummaries, RAY_MAX_LIMIT_FROM_* 定义 |
| `src/ray/protobuf/gcs.proto` | TaskStatus 枚举定义 |
| `src/ray/gcs/gcs_server/gcs_task_manager.cc` | GCS Task Manager，task events 存储 |

### 9.3 Ray Data 代码

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

### B. 常见误解澄清

| 误解 | 正确理解 |
|------|---------|
| "Unaccounted 表示任务丢失" | Unaccounted 只是显示问题，任务仍在正常执行 |
| "Submitted 表示任务还没开始" | Submitted 包含正在 Actor 队列中等待的任务 |
| "Task Table 和 Progress Bar 应该完全一致" | 它们使用不同的数据源和聚合方式 |
| "Running 应该等于 Actor 数" | Running 受资源和并发限制，可能小于 Actor 数 |

### C. 相关文档

- [Ray Data Schedule Loop 性能优化指南](./ray_data_schedule_loop_optimization.md)
- [Ray Data Metrics 分析指南](./ray_data_metrics_guide.md)
