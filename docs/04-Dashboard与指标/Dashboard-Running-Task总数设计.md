# Dashboard 添加 RUNNING task 真实总数（含无 task_info 的 entry）

本文档描述 Dashboard 进度条如何暴露 GCS buffer 中**所有** entry 的状态分布（包括因缺失 `task_info` 而被过滤掉的僵尸 entry），使用户在 Dashboard 上感知到真实的 RUNNING task 数量。

> **关联问题**: 当 GCS buffer 中存在大量仅有 `state_updates` 但无 `task_info` 的 entry 时，Dashboard 显示的 RUNNING 数远低于实际数量，用户无法判断集群中真正有多少 task 在运行。

---

## 目录

1. [问题背景](#1-问题背景)
2. [设计目标与约束](#2-设计目标与约束)
3. [整体数据流](#3-整体数据流)
4. [各层改动详解](#4-各层改动详解)
5. [前端展示效果](#5-前端展示效果)
6. [设计决策与取舍](#6-设计决策与取舍)
7. [验证方式](#7-验证方式)
8. [关键文件索引](#8-关键文件索引)

---

## 1. 问题背景

### 1.1 现有查询流程

GCS 存储 task event 时，每条 entry 由两部分组成：

| 字段 | 含义 | 是否必有 |
|------|------|----------|
| `task_info` | task 的元数据（名称、类型、job_id 等） | 否 |
| `state_updates` | task 的状态流转时间戳 | 否 |

当 Dashboard 查询 task 列表时，`HandleGetTaskEvents` 中的 `filter_fn` 会过滤掉所有没有 `task_info` 的 entry：

**文件**: `src/ray/gcs/gcs_task_manager.cc:508`

```cpp
auto filter_fn = [&filters](const rpc::TaskEvents &task_event) {
    if (!task_event.has_task_info()) {
        return false;  // 跳过无 task_info 的 entry
    }
    // ... 其他过滤 ...
};
```

### 1.2 问题根因

在高吞吐场景下，GCS buffer 中可能存在大量仅有 `state_updates` 而无 `task_info` 的 entry（例如 worker 先上报了状态变更，但 task_info 因 GC 淘汰或延迟未到达）。这些 entry 被 `filter_fn` 完全过滤，导致 Dashboard 显示的 task 数量与实际集群中运行的 task 数量严重不一致。

```
GCS buffer: 100,000 entries
  ├── 有 task_info: 1,511 entries → Dashboard 可见
  └── 无 task_info: 98,489 entries → 完全不可见
      其中 RUNNING 状态: 94,937 entries → 用户无法感知
```

---

## 2. 设计目标与约束

### 2.1 目标

- 在 GCS 查询 task event 时，统计**所有** entry（含无 `task_info` 的）的状态分布
- 通过 REST API 暴露给 Dashboard 前端
- 在进度条的 Running 指标旁显示真实总数

### 2.2 约束

- **不修改 GCS 过滤逻辑**：`has_task_info()` 过滤行为保持不变，Dashboard 表格仍只展示有完整元数据的 task
- **不修改淘汰策略**：GC priority 逻辑不变
- **仅在计数层暴露信息**：不暴露无 `task_info` entry 的详细内容

---

## 3. 整体数据流

```
┌──────────────────────────────────────────────────────────────────────┐
│  C++ GCS Layer                                                       │
│                                                                      │
│  HandleGetTaskEvents                                                 │
│  ┌─────────────────────────────────────────────┐                     │
│  │  for each entry in buffer:                  │                     │
│  │    ① 统计状态 → total_state_counts          │ ← 所有 entry       │
│  │    ② filter_fn → 过滤无 task_info 的 entry  │ ← 仅有 task_info   │
│  │    ③ 写入 events_by_task                    │                     │
│  └─────────────────────────────────────────────┘                     │
│                         │                                            │
│            GetTaskEventsReply                                        │
│            ├── events_by_task (过滤后的 task 列表)                    │
│            ├── num_total_stored (buffer 总条数)                       │
│            ├── num_filtered_on_gcs (被过滤条数)                       │
│            └── total_state_counts (全量状态分布) ← 新增               │
└──────────────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────────────────────────┐
│  Python State Aggregator                                             │
│                                                                      │
│  list_tasks() → ListApiResponse                                      │
│    ├── result: [task_dict, ...]                                      │
│    ├── total_state_counts: {"RUNNING": 95000, ...}  ← 透传           │
│    ├── num_total_stored: 100000                     ← 透传           │
│    └── num_filtered_on_gcs: 98489                   ← 透传           │
│                          │                                           │
│  summarize_tasks() → SummaryApiResponse                              │
│    └── (同上字段透传)                                                 │
└──────────────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────────────────────────┐
│  Frontend (React)                                                    │
│                                                                      │
│  useJobProgress hook                                                 │
│    └── 解析 total_state_counts → totalStateCounts                    │
│                          │                                           │
│  JobProgressBar                                                      │
│    └── 透传 totalStateCounts 给 TaskProgressBar                      │
│                          │                                           │
│  TaskProgressBar                                                     │
│    └── Running segment 添加 hint:                                    │
│        "95000 tasks running in total (94937 not shown...)"           │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 4. 各层改动详解

### 4.1 Proto 层

**文件**: `src/ray/protobuf/gcs_service.proto`

在 `GetTaskEventsReply` 末尾新增 field 8：

```protobuf
message GetTaskEventsReply {
  // ... 已有字段 1-7 ...
  // State counts for ALL entries in the candidate set, including entries
  // without task_info that are normally filtered from query results.
  // Key is TaskStatus enum name (e.g. "RUNNING", "FINISHED"), value is count.
  map<string, int64> total_state_counts = 8;
}
```

**设计选择**：使用 `map<string, int64>` 而非 `map<int32, int64>`，key 为 `TaskStatus` 枚举名称字符串。这与现有 Python/前端代码中使用状态名称字符串的模式保持一致，避免前端需要维护枚举值到名称的映射。

### 4.2 C++ 层

**文件**: `src/ray/gcs/gcs_task_manager.cc`

#### 4.2.1 提取公共辅助函数

将原有 `filter_fn` 中确定 entry 最新状态的逻辑提取为独立函数，供 filter 和计数两处复用：

```cpp
ray::rpc::TaskStatus GetLatestTaskStatus(const rpc::TaskEvents &task_event) {
  if (!task_event.has_state_updates()) {
    return ray::rpc::TaskStatus::NIL;
  }
  const auto *descriptor = ray::rpc::TaskStatus_descriptor();
  for (int i = descriptor->value_count() - 1; i >= 0; --i) {
    if (task_event.state_updates().state_ts_ns().contains(
            descriptor->value(i)->number())) {
      return static_cast<ray::rpc::TaskStatus>(descriptor->value(i)->number());
    }
  }
  return ray::rpc::TaskStatus::NIL;
}
```

逻辑：从 `TaskStatus` 枚举的最高值向下遍历，找到第一个在 `state_ts_ns` 中存在的状态，即为该 entry 的最新状态。

#### 4.2.2 在查询循环中统计

在 `HandleGetTaskEvents` 的主循环中，**先于** `filter_fn` 调用，对每条有 `state_updates` 的 entry 统计状态：

```cpp
absl::flat_hash_map<std::string, int64_t> total_state_counts;

for (auto &task_event : *task_events | boost::adaptors::reversed) {
    // 统计所有有 state_updates 的 entry（含无 task_info 的）
    if (task_event.has_state_updates()) {
        auto latest_state = GetLatestTaskStatus(task_event);
        total_state_counts[ray::rpc::TaskStatus_Name(latest_state)]++;
    }

    // 然后才走过滤逻辑（过滤无 task_info 的 entry）
    if (!filter_fn(task_event)) { ... }
}
```

循环结束后写入 reply：

```cpp
for (const auto &[state_name, state_count] : total_state_counts) {
    (*reply->mutable_total_state_counts())[state_name] = state_count;
}
```

#### 4.2.3 计数范围说明

`total_state_counts` 的统计范围取决于查询的 candidate set：

| 查询条件 | candidate set | 计数范围 |
|----------|---------------|----------|
| 按 `job_id` 过滤 | 该 job 下所有 entry | 该 job 的全量状态分布 |
| 按 `task_id` 过滤 | 该 task 的 entry | 该 task 的状态 |
| 无过滤 | 全量 entry | 整个集群的状态分布 |

注意：非索引过滤条件（`task_name`、`state`、`actor_id`、`exclude_driver`）**不影响** `total_state_counts` 的统计。这是因为这些过滤在 `filter_fn` 中执行，而 `total_state_counts` 在 `filter_fn` 之前就已计数。对于 Dashboard 进度条的主要使用场景（按 `job_id` 查询），这是正确的行为。

### 4.3 Python 层

**文件**: `python/ray/util/state/common.py`

在 `ListApiResponse` 和 `SummaryApiResponse` 两个 dataclass 中新增三个可选字段：

```python
@dataclass(init=not IS_PYDANTIC_2)
class ListApiResponse:
    # ... 已有字段 ...
    total_state_counts: Optional[Dict[str, int]] = None
    num_total_stored: Optional[int] = None
    num_filtered_on_gcs: Optional[int] = None
```

**文件**: `python/ray/dashboard/state_aggregator.py`

在 `list_tasks()` 的 `transform` 函数中从 proto reply 提取并传入 `ListApiResponse`：

```python
total_state_counts=dict(reply.total_state_counts)
if reply.total_state_counts
else None,
```

在 `summarize_tasks()` 中从 `ListApiResponse` 透传到 `SummaryApiResponse`。

**关于 `None` vs 空 dict 的语义**：proto3 的 map 字段默认为空 map（falsy），因此当 GCS buffer 中没有任何有 `state_updates` 的 entry 时，`total_state_counts` 会是 `None` 而非 `{}`。这与 "该字段不可用" 的语义一致。

### 4.4 前端层

**文件**: `python/ray/dashboard/client/src/type/job.ts`

在 `StateApiJobProgressByTaskNameRsp` 类型中新增可选字段：

```typescript
total_state_counts?: { [stateName: string]: number };
num_total_stored?: number;
num_filtered_on_gcs?: number;
```

**文件**: `python/ray/dashboard/client/src/pages/job/hook/useJobProgress.ts`

`useFetchStateApiProgressByTaskName` hook 解析 `total_state_counts` 并通过 `useJobProgress` 的返回值暴露为 `totalStateCounts`。

**文件**: `python/ray/dashboard/client/src/pages/job/TaskProgressBar.tsx`

`TaskProgressBarProps` 新增可选的 `totalStateCounts` 属性。在 Running segment 的 `ProgressBarSegment` 中，利用已有的 `hint` 机制显示差异信息：

```typescript
{
  label: "Running",
  value: numRunning,
  color: theme.palette.primary.main,
  hint:
    totalStateCounts?.RUNNING != null &&
    totalStateCounts.RUNNING !== numRunning
      ? `${totalStateCounts.RUNNING} tasks running in total ...`
      : undefined,
}
```

**文件**: `python/ray/dashboard/client/src/pages/job/JobProgressBar.tsx`

从 `useJobProgress` hook 解构 `totalStateCounts` 并透传给 `<TaskProgressBar>`。

---

## 5. 前端展示效果

### 5.1 正常场景（无差异）

当所有 RUNNING entry 都有 `task_info` 时，`totalStateCounts.RUNNING === numRunning`，hint 不显示：

```
Total: 1486  Running: 63  Waiting for scheduling: 207  ...
```

### 5.2 存在僵尸 entry（有差异）

当 GCS buffer 中存在无 `task_info` 的 RUNNING entry 时，Running 旁显示 (?) 图标：

```
Total: 1486  Running: 63 (?)  Waiting for scheduling: 207  ...
                         ↑
                  hover 显示:
                  "95000 tasks running in total
                   (94937 not shown due to incomplete metadata)"
```

### 5.3 REST API 响应示例

`GET /api/v0/tasks/summarize`

```json
{
  "data": {
    "result": {
      "total": 614407715,
      "num_after_truncation": 1231,
      "num_filtered": 0,
      "total_state_counts": {
        "RUNNING": 95000,
        "PENDING_NODE_ASSIGNMENT": 3000,
        "SUBMITTED_TO_WORKER": 1500,
        "FINISHED": 500
      },
      "num_total_stored": 100000,
      "num_filtered_on_gcs": 98489,
      "result": { "..." : "..." }
    }
  }
}
```

---

## 6. 设计决策与取舍

### 6.1 计数位置：`filter_fn` 之前 vs 之后

| 方案 | 优点 | 缺点 |
|------|------|------|
| **`filter_fn` 之前（采用）** | 统计所有 entry（含无 task_info 的），真正反映 GCS buffer 全貌 | 非索引过滤条件（task_name 等）不影响计数 |
| `filter_fn` 之后 | 计数与过滤条件完全一致 | 无法统计到无 `task_info` 的 entry，失去核心价值 |

选择 `filter_fn` 之前统计，因为本功能的核心诉求就是暴露那些因缺失 `task_info` 而被过滤掉的 entry。

### 6.2 仅在 Running segment 显示 hint

当前仅对 Running 状态显示差异 hint，而不是所有状态。原因：

- Running 是用户最关心的实时指标，差异对运维判断影响最大
- 对所有状态都显示 hint 会使 UI 过于嘈杂
- 后续可根据需求扩展到其他状态

### 6.3 `totalStateCounts` 仅通过 `useJobProgress` 传递

`useJobProgressByLineage`（高级进度条）未传递 `totalStateCounts`。原因：

- 两个 hook 使用相同的轮询间隔（`API_REFRESH_INTERVAL_MS`），数据时效性差异可忽略
- `JobProgressBar` 中 `totalStateCounts` 始终取自 `useJobProgress`，即使进度条数据来自 lineage hook
- 保持改动最小化，避免不必要的数据链路扩展

### 6.4 不单独暴露无 task_info entry 的详细信息

只暴露聚合计数，不暴露具体的 entry 内容。原因：

- 无 `task_info` 的 entry 缺少 task 名称、类型等元数据，在 Dashboard 表格中无法有意义地展示
- 聚合计数已足够让用户判断集群的真实 task 规模
- 避免引入额外的 API 端点和前端复杂度

---

## 7. 验证方式

### 7.1 REST API 验证

```bash
curl -s "http://localhost:8265/api/v0/tasks/summarize" | python3 -c "
import json, sys
data = json.load(sys.stdin)
r = data['data']['result']
print(f'Dashboard 可见 task: {r[\"num_after_truncation\"]}')
print(f'Buffer 总条数: {r.get(\"num_total_stored\", \"N/A\")}')
print(f'GCS 过滤条数: {r.get(\"num_filtered_on_gcs\", \"N/A\")}')
tsc = r.get('total_state_counts', {})
print(f'全量状态分布: {tsc}')
print(f'真实 RUNNING 总数: {tsc.get(\"RUNNING\", 0)}')
"
```

### 7.2 前端验证

1. 打开 Dashboard → 进入 Job 详情页
2. 查看 "Ray Core Overview" 进度条
3. 若存在不可见的 RUNNING entry，Running 旁应出现 (?) 图标
4. hover 可看到真实 RUNNING 数及差异说明

### 7.3 无差异场景验证

在测试集群中所有 task 都有完整 `task_info` 时，(?) 图标不应出现。

---

## 8. 关键文件索引

| 文件 | 改动内容 |
|------|----------|
| `src/ray/protobuf/gcs_service.proto` | `GetTaskEventsReply` 新增 `total_state_counts` 字段（field 8） |
| `src/ray/gcs/gcs_task_manager.cc` | 提取 `GetLatestTaskStatus` 辅助函数；查询循环中统计全量状态分布 |
| `python/ray/util/state/common.py` | `ListApiResponse` / `SummaryApiResponse` 新增三个可选字段 |
| `python/ray/dashboard/state_aggregator.py` | `list_tasks()` / `summarize_tasks()` 透传新字段 |
| `python/ray/dashboard/client/src/type/job.ts` | API 响应类型新增 `total_state_counts` 等字段 |
| `python/ray/dashboard/client/src/pages/job/hook/useJobProgress.ts` | Hook 解析并传递 `totalStateCounts` |
| `python/ray/dashboard/client/src/pages/job/TaskProgressBar.tsx` | Running segment 添加 hint 显示真实总数 |
| `python/ray/dashboard/client/src/pages/job/JobProgressBar.tsx` | 透传 `totalStateCounts` 给 TaskProgressBar |
