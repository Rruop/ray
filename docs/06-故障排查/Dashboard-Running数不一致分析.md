# Dashboard Ray Core Overview 与 Task Table Running 数量不一致根因分析与修复

> 集群：search-kibana-sgp（150+ 节点）| Job ID：05000000 | Ray 版本：2.54.x+kuaishou
> 问题日期：2026-05-22
> 关联文档：
> - [Task Event 淘汰与僵尸 Entry 分析](./dashboard-task-count-low-eviction-zombie-entry-analysis.md)
> - [total_state_counts 功能设计](../design/dashboard-total-state-counts.md)

---

## 一、问题现象

### 1.1 用户观察

在 `https://search-kibana-sgp.corp.kuaishou.com/#/jobs/05000000` 页面，**Ray Core Overview** 进度条与下方 **Task Table** 的状态统计chip 显示的 Running task 数量严重不一致：

**Ray Core Overview 进度条：**

```
Total: 10000
Running: 343
Waiting for scheduling: 1
Unaccounted: 9656
```

**Task Table 状态统计 chips：**

```
TOTAL x 10000
RUNNING x 2070
SUBMITTED_TO_WORKER x 1559
FINISHED x 6336
PENDING_ACTOR_TASK_ARGS_FETCH x 34
FAILED x 1
```

**核心差异：Overview Running = 343 vs Task Table RUNNING = 2070，相差 6 倍。**

同时 Overview 中出现了一个巨大的 "Unaccounted: 9656" 灰色段，占进度条的 96.5%，严重影响用户对任务执行状态的判断。

### 1.2 Job 参数

```bash
python multishot_video_classifier_pipeline_checkpoint.py \
  --cpu-concurrency 2000 \
  --streaming-gpu-concurrency 1000 \
  --preprocess-concurrency 12000 \
  --sink-concurrency 100 \
  --streaming-num-gpus 0.5 \
  --streaming-cpu-actor-pool-size 12 \
  --processing-mode streaming \
  --streaming-mode qwenvl \
  --override-num-blocks 2000
```

高并发参数 + 150+ 节点集群，属于高吞吐场景。

---

## 二、排查方法与手段

### 2.1 Dashboard 页面抓取

使用 Playwright 自动化浏览器访问 Dashboard，绕过 HTTPS 证书和 SSO 登录限制：

```javascript
const browser = page.context().browser();
const ctx = await browser.newContext({ ignoreHTTPSErrors: true });
const p = await ctx.newPage();
await p.goto(base + '/#/jobs/05000000', { waitUntil: 'domcontentloaded' });
// SSO 一键登录
if (p.url().includes('sso.corp.kuaishou.com')) {
  await p.getByText('一键登录').click();
}
```

**获取 Ray Core Overview 数据：** 从页面 `innerText` 提取进度条各状态计数。

**获取 Task Table 数据：** 点击 "Task Table" tab，通过 State 过滤器选择 RUNNING，读取 chips 上的计数值。

### 2.2 前端代码追踪

追踪了从 GCS C++ 层到 Dashboard 前端的完整数据流，确定两个数字分别来自不同数据源。

#### 2.2.1 Ray Core Overview 数据来源

**API 调用链：**

```
前端 JobProgressBar.tsx
  → useJobProgress hook
    → useFetchStateApiProgressByTaskName
      → GET /api/v0/tasks/summarize?filter_keys=job_id&filter_values=05000000
        → Python state_aggregator.summarize_tasks()
          → GCS gRPC GetTaskEvents()
```

**关键代码路径：**

`useJobProgress.ts:126-132`（修改前）— 从 summary 汇总 progress：

```typescript
const summed = (data?.summary ?? []).reduce((acc, task) => {
    Object.entries(task.progress).forEach(([k, count]) => {
        const key = k as keyof TaskProgress;
        acc[key] = (acc[key] ?? 0) + count;
    });
    return acc;
}, {} as TaskProgress);
```

这个 `summed` 的值来自 `summary.node_id_to_summary.cluster.summary` 的 `state_counts`，而 `state_counts` 只包含**有 `task_info` 的 entry**。

#### 2.2.2 Task Table 数据来源

Task Table 的状态统计 chips（`RUNNING x 2070` 等）来自 GCS 返回的 `total_state_counts` 字段，该字段统计 GCS buffer 中**所有 entry**（含无 `task_info` 的僵尸 entry）。

#### 2.2.3 "Unaccounted" 生成逻辑

`ProgressBar.tsx:85-103`：

```typescript
const segmentTotal = progress.reduce((acc, { value }) => acc + value, 0); // = 344
const finalTotal = total ?? segmentTotal; // = 10000

const segments = segmentTotal < finalTotal
    ? [...progress, {
        value: finalTotal - segmentTotal,  // 10000 - 344 = 9656
        label: "Unaccounted",
        hint: "Unaccounted tasks can happen when there are too many tasks..."
      }]
    : progress;
```

### 2.3 GCS C++ 层代码分析

**GCS `total_state_counts` 计算逻辑** — `src/ray/gcs/gcs_task_manager.cc:590-629`：

```cpp
absl::flat_hash_map<std::string, int64_t> total_state_counts;

for (auto &task_event : *task_events | boost::adaptors::reversed) {
    // ① 先统计状态：ALL entries（含无 task_info 的僵尸 entry）
    if (task_event.has_state_updates()) {
        auto latest_state = GetLatestTaskStatus(task_event);
        total_state_counts[ray::rpc::TaskStatus_Name(latest_state)]++;
    }

    // ② 再执行过滤：只有有 task_info 的 entry 才能通过
    if (!filter_fn(task_event)) {
        num_filtered++;
        continue;
    }

    // ③ 通过过滤的 entry 加入 reply（summary 用这部分数据）
    reply->add_events_by_task(...)
}
```

**`filter_fn` 中的硬过滤** — `src/ray/gcs/gcs_task_manager.cc:507-510`：

```cpp
auto filter_fn = [&filters](const rpc::TaskEvents &task_event) {
    if (!task_event.has_task_info()) {
        return false;  // 无 task_info → 被过滤
    }
    // ...
};
```

**`GetLatestTaskStatus` 辅助函数** — `src/ray/gcs/gcs_task_manager.cc:428-440`：

```cpp
ray::rpc::TaskStatus GetLatestTaskStatus(const rpc::TaskEvents &task_event) {
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

### 2.4 Job 索引确认

`HandleGetTaskEvents` 在处理 job_id 过滤时，通过 `job_index_` 二级索引只提取该 Job 的 entry — `gcs_task_manager.cc:464-474`：

```cpp
} else if (filters.job_filters_size() > 0) {
    // ...
    if (job_ids.size() == 1) {
        const JobID &job_id = *job_ids.begin();
        task_events = task_event_storage_->GetTaskEvents(job_id);
    }
}
```

所以 `total_state_counts` 的统计范围是**该 Job 的所有 entry**，不会混入其他 Job 的数据。

---

## 三、排查结论

### 3.1 根因：Overview 和 Task Table 使用不同口径的数据源

| 数据 | 数据源 | 统计范围 | 值 |
|------|--------|---------|-----|
| Overview "Running: 343" | `summary` → per-task-name `state_counts` | 仅有 `task_info` 的 entry | 343 |
| Task Table "RUNNING x 2070" | `total_state_counts["RUNNING"]` | **所有** entry（含僵尸） | 2070 |
| Overview "Total: 10000" | `sum(total_state_counts)` | 所有 entry | 10000 |
| Overview "Unaccounted: 9656" | `Total - segmentTotal` = 10000 - 344 | 差值兜底 | 9656 |

**不一致的本质：**

```
Overview 进度条各 segment 的总和（分子）= 344，来自 summary（只有 task_info 的 entry）
Overview 进度条的 total（分母）= 10000，来自 total_state_counts（所有 entry）
分子分母口径不一致 → 产生巨大的 "Unaccounted" 缺口
```

### 3.2 GCS Buffer 中的 entry 分布

对于 Job 05000000，GCS buffer 中共 10000 条 entry：

```
有 task_info 的 entry（能被 summary 统计）: ~344 条
  ├── RUNNING:                 343  → Overview 显示为 "Running: 343"
  └── PENDING_NODE_ASSIGNMENT:   1  → Overview 显示为 "Waiting: 1"

无 task_info 的僵尸 entry（被 filter_fn 过滤）: ~9656 条  → "Unaccounted"
  ├── RUNNING:                 1727  (= total_state_counts 2070 - 有task_info的 343)
  ├── SUBMITTED_TO_WORKER:    ~1559
  ├── FINISHED:               ~6336
  ├── PENDING_ACTOR_TASK_ARGS_FETCH: ~34
  └── FAILED:                  ~1
```

### 3.3 僵尸 entry 产生原因

僵尸 entry（有 `state_updates` 但无 `task_info`）的产生机制已在[僵尸 Entry 分析文档](./dashboard-task-count-low-eviction-zombie-entry-analysis.md)中详细说明，核心链路：

1. Driver 只在 `PENDING_ARGS_AVAIL` 状态**首次提交时发送一次 `task_info`**
2. 高吞吐场景下 GCS buffer（默认 100,000 条）周转时间很短（秒级）
3. 原始 entry（含 `task_info`）被 GC 淘汰后，Worker 后续上报 RUNNING 状态创建新 entry，但**不携带 `task_info`**
4. 这些新 entry 被 `total_state_counts` 统计但被 `filter_fn` 过滤
5. 僵尸 entry 不可恢复——Driver 不会重发 `task_info`

### 3.4 前端数据流图解

```
GCS GetTaskEventsReply
  ├── events_by_task（过滤后，仅有 task_info 的 entry）
  │     └─→ Python state_aggregator.summarize_tasks()
  │           └─→ REST API /api/v0/tasks/summarize 的 summary 字段
  │                 └─→ 前端 useFetchStateApiProgressByTaskName
  │                       └─→ formatSummaryToTaskProgress → summary[]
  │                             └─→ useJobProgress 的 summed（reduce 汇总）
  │                                   └─→ progress prop（各 segment 值）
  │                                         Running: 343 ← 只有 task_info 的
  │
  └── total_state_counts（全量统计，含僵尸 entry）
        └─→ REST API response 的 total_state_counts 字段
              └─→ 前端 totalStateCounts
                    ├─→ Task Table chips 直接显示：RUNNING x 2070
                    └─→ （修改前）仅用于 hint tooltip，未用于 segment

修改前的 ProgressBar：
  total = sum(total_state_counts) = 10000
  segments = summed（来自 summary）= 344
  Unaccounted = 10000 - 344 = 9656
```

---

## 四、解决方案

### 4.1 修复思路

**让 Overview 进度条的各 segment 也使用 `total_state_counts` 数据**，消除分子分母口径不一致。

修改前：
- `total`（分母）= `sum(total_state_counts)` = 10000（全量）
- `segments`（分子）= `summed` from summary = 344（仅 task_info）
- `Unaccounted` = 10000 - 344 = 9656

修改后：
- `total`（分母）= `sum(total_state_counts)` = 10000（全量）
- `segments`（分子）= `formatStateCountsToProgress(total_state_counts)` = 10000（全量）
- `Unaccounted` = 0（消除）

### 4.2 代码修改

#### 4.2.1 核心修改：`useJobProgress.ts`

在 `useJobProgress` hook 中，优先使用 `total_state_counts` 构建 progress：

```typescript
// 修改前
return {
    progress: summed,
    totalTasks: data?.totalTasks,
    totalStateCounts: data?.totalStateCounts,
    // ...
};

// 修改后
const progressFromTotalStateCounts = data?.totalStateCounts
    ? formatStateCountsToProgress(data.totalStateCounts)
    : null;

const totalFromStateCounts = data?.totalStateCounts
    ? Object.values(data.totalStateCounts).reduce(
          (acc, count) => acc + count, 0)
    : undefined;

return {
    progress: progressFromTotalStateCounts ?? summed,
    totalTasks: totalFromStateCounts ?? data?.totalTasks,
    // ...
};
```

**关键设计决策：**

- 复用已有的 `formatStateCountsToProgress` 函数，它使用 `TASK_STATE_NAME_TO_PROGRESS_KEY` 映射表将原始状态名正确归类到进度条的各个段
- 当 `total_state_counts` 不可用时（旧版 GCS 不返回此字段），`progressFromTotalStateCounts` 为 `null`，fallback 到原始的 `summed` 逻辑，保证向后兼容
- `totalTasks` 也从 `total_state_counts` 的值之和获取，确保 `total` 和 `segments` 口径一致

#### 4.2.2 清理：移除 `TaskProgressBar` 的 `totalStateCounts` prop

修改前，`TaskProgressBar` 接收 `totalStateCounts` prop 用于在 Running 段显示 hint tooltip：

```typescript
// 修改前
hint:
    totalStateCounts?.RUNNING != null &&
    totalStateCounts.RUNNING !== numRunning
        ? `${totalStateCounts.RUNNING} tasks running in total (${totalStateCounts.RUNNING - numRunning} not shown due to incomplete metadata)`
        : undefined,
```

修改后 `numRunning` 已经等于 `totalStateCounts.RUNNING`，hint 不再有意义，移除该 prop。

#### 4.2.3 补充：新增 `GETTING_AND_PINNING_ARGS` 状态映射

Code review 发现 protobuf `TaskStatus` 枚举中 `GETTING_AND_PINNING_ARGS = 13`（`RUNNING` 的子状态）在前端 `TypeTaskStatus` 中缺失。修改前会被 fallback 到 `numUnknown`，显示为 "Unknown"。

**修复：**

`type/task.ts` 新增枚举值：

```typescript
export enum TypeTaskStatus {
    // ...
    GETTING_AND_PINNING_ARGS = "GETTING_AND_PINNING_ARGS",
}
```

`useJobProgress.ts` 新增映射：

```typescript
[TypeTaskStatus.GETTING_AND_PINNING_ARGS]: TaskStatus.RUNNING,
```

### 4.3 修改文件清单

| 文件 | 改动内容 |
|------|---------|
| `src/pages/job/hook/useJobProgress.ts` | `useJobProgress` hook 优先使用 `total_state_counts`；新增 `GETTING_AND_PINNING_ARGS → RUNNING` 映射；移除导出 `totalStateCounts` |
| `src/pages/job/TaskProgressBar.tsx` | 移除 `totalStateCounts` prop 及 hint 逻辑 |
| `src/pages/job/JobProgressBar.tsx` | 移除向 TaskProgressBar 传递 `totalStateCounts` |
| `src/type/task.ts` | `TypeTaskStatus` 新增 `GETTING_AND_PINNING_ARGS` |

### 4.4 修改后效果

以 Job 05000000 为例：

| 指标 | 修改前 | 修改后 |
|------|--------|--------|
| Running | 343（仅 task_info 可见） | **2070**（全量，与 Task Table 一致） |
| Waiting for scheduling | 1 | **~1593**（包含 SUBMITTED_TO_WORKER + PENDING 等） |
| Finished | 不显示 | **6336** |
| Failed | 不显示 | **1** |
| Unaccounted | **9656**（96.5%） | **0**（消除） |
| Total | 10000 | 10000 |

---

## 五、关键代码索引

### 5.1 GCS C++ 层

| 文件 | 行号 | 功能 |
|------|------|------|
| `src/ray/gcs/gcs_task_manager.cc` | 428-440 | `GetLatestTaskStatus()`: 从 `state_ts_ns` 取最新状态 |
| `src/ray/gcs/gcs_task_manager.cc` | 464-474 | `HandleGetTaskEvents()`: job_id 索引查询 |
| `src/ray/gcs/gcs_task_manager.cc` | 507-510 | `filter_fn`: `has_task_info()` 硬过滤 |
| `src/ray/gcs/gcs_task_manager.cc` | 590-629 | 主循环：先统计 `total_state_counts`，再执行 `filter_fn` |
| `src/ray/protobuf/common.proto` | 930-941 | `TaskStatus` 枚举定义（含 `GETTING_AND_PINNING_ARGS = 13`） |
| `src/ray/protobuf/gcs_service.proto` | 876-879 | `GetTaskEventsReply.total_state_counts` 字段定义 |

### 5.2 Python 层

| 文件 | 行号 | 功能 |
|------|------|------|
| `python/ray/dashboard/state_aggregator.py` | 314-347 | `list_tasks()`: 提取 `total_state_counts` 透传 |
| `python/ray/dashboard/state_aggregator.py` | 574-626 | `summarize_tasks()`: 传递 `total_state_counts` 到响应 |
| `python/ray/dashboard/modules/state/state_head.py` | 300-304 | `GET /api/v0/tasks/summarize` REST 端点 |

### 5.3 前端

| 文件 | 行号 | 功能 |
|------|------|------|
| `client/src/type/task.ts` | 1-16 | `TypeTaskStatus` 枚举（新增 `GETTING_AND_PINNING_ARGS`） |
| `client/src/pages/job/hook/useJobProgress.ts` | 29-47 | `TASK_STATE_NAME_TO_PROGRESS_KEY` 映射表 |
| `client/src/pages/job/hook/useJobProgress.ts` | 108-160 | `useJobProgress` hook（核心修改） |
| `client/src/pages/job/hook/useJobProgress.ts` | 211-226 | `formatStateCountsToProgress()`: 状态名 → TaskProgress 转换 |
| `client/src/pages/job/TaskProgressBar.tsx` | 17-93 | `TaskProgressBar`: 进度条 segment 渲染 |
| `client/src/pages/job/JobProgressBar.tsx` | 16-113 | `JobProgressBar`: 数据选择与传递 |
| `client/src/components/ProgressBar/ProgressBar.tsx` | 85-103 | `ProgressBar`: "Unaccounted" segment 生成逻辑 |

---

## 六、验证方式

### 6.1 TypeScript 编译验证

```bash
cd python/ray/dashboard/client
npx tsc --noEmit --pretty
# TypeScript: No errors found
```

### 6.2 单元测试

```bash
cd python/ray/dashboard/client
npx jest useJobProgress.unit.test.ts
```

### 6.3 Dashboard 页面验证

修改后访问 `https://search-kibana-sgp.corp.kuaishou.com/#/jobs/05000000`：

1. **Ray Core Overview** 进度条的 Running 数字应与 Task Table 的 RUNNING chip 数字一致
2. **"Unaccounted"** 灰色段应消失或接近 0
3. 进度条各段之和应等于 Total
4. **向后兼容**：在不返回 `total_state_counts` 的旧版 GCS 集群上，应 fallback 到原始 summary 数据展示

---

## 七、总结

| 维度 | 说明 |
|------|------|
| 问题本质 | Overview 进度条 segments（分子）和 total（分母）使用不同口径的数据源 |
| 根因 | segments 来自 `summary`（仅 task_info entry），total 来自 `total_state_counts`（所有 entry） |
| 为什么差异大 | 高吞吐场景下 GCS buffer 淘汰严重，大量 entry 丢失 task_info 成为僵尸 |
| 修复方案 | segments 也使用 `total_state_counts`，口径对齐，消除 Unaccounted |
| 兼容性 | fallback 到 summary 数据，兼容不返回 total_state_counts 的旧版 GCS |
| 额外修复 | 补充 `GETTING_AND_PINNING_ARGS` 状态映射，避免错误归类为 Unknown |
