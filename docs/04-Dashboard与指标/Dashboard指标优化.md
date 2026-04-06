# T11609366 Dashboard Metrics Optimization

## 概述

本文档记录了 Ray Data Dashboard 指标修复和排序功能增强的完整分析与实现过程。

涉及两个 commit：
1. `aea190f60d` — Fix Blocks Input metric and add Queued Blocks display
2. `69ead34f7a` — Add duration/uptime column sort and GPU sort for dashboard

---

## 一、Blocks Input 指标修复

### 1.1 问题发现

commit `0962632e` 中新增了 `Rows Input` 和 `Blocks Input` 指标，但观察到 **Blocks Input 远小于 Ray Data 日志中的 `Queued blocks` 数量**。

日志示例：
```
logging_progress.py:233 -- Tasks: 6; Actors: 3 (running=2, restarting=1, pending=0); Queued blocks: 5770 (5.4GiB)
```

### 1.2 根因分析

#### `num_inputs_received`（Blocks Input 使用的指标）

- 定义在 `op_runtime_metrics.py:239`
- 在 `on_input_received` 回调中每次 **+1**（`op_runtime_metrics.py:795`）
- `on_input_received` 在 `physical_operator.py:600` 中，每收到一个 **RefBundle** 调用一次
- 因此 `num_inputs_received` 计算的是**收到的 RefBundle 数量**，不是 block 数量

#### `Queued blocks`（日志中的 5770）

- 定义在 `streaming_executor_state.py:212-223` 的 `total_enqueued_input_blocks()` 中
- 统计的是 input_queues 和 internal_queues 中所有 **block 的实际数量**（`q.num_blocks`）
- 一个 RefBundle 可以包含**多个 blocks**（见 `ref_bundle.py:48`：`blocks: Tuple[Tuple[ObjectRef[Block], BlockMetadata], ...]`）

#### 结论

**一个 RefBundle != 一个 Block**。差异来自两层原因叠加：
1. **计数单位不同**：`num_inputs_received` 数的是 RefBundle，`Queued blocks` 数的是 block
2. **含义不同**：`num_inputs_received` 是已经从队列取出交给 operator 的累计量；`Queued blocks` 是队列中正在等待的瞬时量

### 1.3 数据流架构

```
上游 Operator                          下游 Operator
+----------+                          +----------+
|          |--output_queue----------->-|          |
|  Op A    |   (= 下游的 input_queue,  |  Op B    |
|          |    是同一个 OpBufferQueue) |          |
+----------+                          +----------+
                                        |
                        dispatch_next_task() 从 input_queue pop
                                        |
                                        v
                              op.add_input(ref)
                                        |
                              metrics.on_input_received(ref)
                              +-- num_inputs_received += 1      (RefBundle数)
                              +-- num_row_inputs_received += rows (行数)
```

关键代码（`streaming_executor_state.py:184-193`）：
- 上游的 `output_queue` 和下游的 `input_queue` 是**同一个 `OpBufferQueue` 实例**
- 初始化时：`inqueues.append(parent_state.output_queue)`（行 387）
- `Queued blocks` 报告在下游 operator 的 `format_op_state_summary` 里，表示"下游 operator 前面排队等待处理的 block 数"
- 包含两部分：external（input_queue 里还没 dispatch 的）+ internal（operator 内部队列如 ref-bundler 里的）

### 1.4 修复方案

新增 `num_block_inputs_received` 字段（不修改 `num_inputs_received` 的语义，因为并发控制逻辑依赖它）：

```python
# op_runtime_metrics.py
num_block_inputs_received: int = metric_field(
    default=0,
    description="Number of input blocks received by operator.",
    metrics_group=MetricsGroup.INPUTS,
)

def on_input_received(self, input: RefBundle):
    self.num_inputs_received += 1  # 保持不动，并发控制依赖
    self.num_block_inputs_received += len(input.blocks)  # 新增
    self.num_row_inputs_received += input.num_rows() or 0
    self.bytes_inputs_received += input.size_bytes()
```

Dashboard 的 `input_blocks` 改用 `num_block_inputs_received`。

---

## 二、Queued Blocks 指标展示

### 2.1 背景

`queued_blocks` 是观察瓶颈最直观的指标（如 5770 blocks 堆积说明下游处理不过来）。后端 per-operator 级别已有（`streaming_executor.py:1066`），但：
- 前端未展示
- dataset 顶层未汇总

### 2.2 两个指标的定位对比

| 指标 | 类型 | 用途 |
|------|------|------|
| Blocks Input | 累计值，单调递增 | 看总共处理了多少 |
| Queued Blocks | 瞬时值，会波动 | 看当前积压/瓶颈 |

### 2.3 实现

- **后端**：dataset 顶层新增 `queued_blocks`，取 `last_state.total_enqueued_input_blocks()`
- **前端**：`DataOverviewTable.tsx` 新增 "Queued Blocks" 列；`data.ts` 类型定义增加 `queued_blocks`
- **默认值**：`stats.py` 中 dataset 级别初始值增加 `"queued_blocks": 0`

---

## 三、InputDataBuffer 的 output_rows 为 0 问题

### 3.1 问题发现

commit `0962632e` 将 `output_rows` 从 `row_outputs_taken` 改为 `rows_task_outputs_generated`，导致 InputDataBuffer 的 output_rows 永远为 0。

### 3.2 根因分析

`InputDataBuffer` 是一个特殊的 operator——**它没有 task**：

```
InputDataBuffer
+-- start() 时: 遍历 _input_data，调用 on_input_received()  <- Rows/Blocks Input 有值
+-- has_next() / _get_next_inner(): 直接从 _input_data 列表中按 index 取出 bundle
|   +-- get_next() 调用 on_output_taken()  <- row_outputs_taken 有值
+-- 没有 task 提交，不会调用 on_task_output_generated()  <- rows_task_outputs_generated = 0
```

- `rows_task_outputs_generated` 只在 `on_task_output_generated` 回调中累加，而这个回调只有 `MapOperator` 和 `HashShuffle` 才会调用
- `row_outputs_taken` 在 `get_next()` 中被调用，是通用的输出计数，任何 operator 都会触发

### 3.3 修复

对 InputDataBuffer 使用 `row_outputs_taken`：

```python
# streaming_executor.py
if isinstance(op, InputDataBuffer):
    op_output_rows = op.metrics.row_outputs_taken
else:
    op_output_rows = op.metrics.rows_task_outputs_generated
```

### 3.4 InputDataBuffer 的 Rows Input / Blocks Input 含义

对于 Read 场景，`InputDataBuffer._input_data` 中每个 RefBundle 是一个 **read task 的描述符**：

```python
# plan_read_op.py:48
BlockMetadata(
    num_rows=1,          # 硬编码为 1
    size_bytes=task_size,
)
```

因此：
- `Rows Input` = read task 数量（每个描述符 num_rows=1）
- `Blocks Input` = read task 数量（每个描述符 1 个 block）

这在技术上是正确的——InputDataBuffer 的"输入"就是这些 task 描述符，它只负责把描述符传递给下游 MapOperator，由 MapOperator 执行 read task 后才产出真正的数据行。所以 InputDataBuffer 的这两个指标本质上等于"有多少个 read task"。

---

## 四、修改的文件汇总（Metrics 部分）

| 文件 | 改动 |
|------|------|
| `op_runtime_metrics.py` | 新增 `num_block_inputs_received` 字段和累加逻辑 |
| `streaming_executor.py` | `input_blocks` 改用 `num_block_inputs_received`；dataset 顶层加 `queued_blocks`；InputDataBuffer 用 `row_outputs_taken`；metrics table 加 `num_block_inputs_received` |
| `stats.py` | dataset 级别初始值增加 `queued_blocks` |
| `DataOverviewTable.tsx` | 新增 "Queued Blocks" 列 |
| `data.ts` | 类型定义增加 `queued_blocks` |
| `DataOverview.component.test.tsx` | 测试数据补上 `queued_blocks` |

---

## 五、Dashboard 排序功能增强

### 5.1 TaskTable — Duration 列排序

**文件**: `python/ray/dashboard/client/src/components/TaskTable.tsx`

原有排序支持（commit `eb4f846be0` 加入）：
- `start_time_ms` / `end_time_ms` 列头点击排序（通过 `TableSortLabel`）

新增：
- `sortField` 类型扩展加入 `"duration"`
- 新增 `getTaskDuration` 函数，从 `start_time_ms` 和 `end_time_ms` 计算 duration（运行中的 task 用 `Date.now()` 作为结束时间）
- Duration 列加上 `sortKey: "duration"`
- 排序逻辑新增 `duration` 分支

```typescript
const getTaskDuration = (task: Task): number => {
    if (!task.start_time_ms || task.start_time_ms <= 0) {
        return 0;
    }
    const end = task.end_time_ms && task.end_time_ms > 0 ? task.end_time_ms : Date.now();
    return end - task.start_time_ms;
};
```

排序是纯前端的，不需要改后端接口。

### 5.2 ActorTable — Uptime 列头排序

**文件**: `python/ray/dashboard/client/src/components/ActorTable.tsx`

ActorTable 已有 dropdown 方式的 "Sort By" 支持 Uptime 排序（使用 `useSorter` hook + `SearchSelect`）。

新增：
- 导入 `TableSortLabel`
- Uptime 列加上 `sortKey: uptimeSorterKey`
- 列头渲染支持 `sortKey`，当列头有 `sortKey` 时渲染 `TableSortLabel`
- 点击列头调用已有的 `setSortKey` / `setOrderDesc`，与 dropdown 联动，不会冲突

```typescript
// 列定义
{ label: "Uptime", sortKey: uptimeSorterKey },

// 列头渲染
{sortKey ? (
    <TableSortLabel
        active={sorterKey === sortKey}
        direction={sorterKey === sortKey ? (descVal ? "desc" : "asc") : "asc"}
        onClick={() => {
            if (sorterKey === sortKey) {
                setOrderDesc(!descVal);
            } else {
                setSortKey(sortKey);
                setOrderDesc(false);
            }
        }}
    >
        {label}
    </TableSortLabel>
) : (
    label
)}
```

**组件使用关系**：Jobs tab 详情页（`JobDetail.tsx`）中 Actor Table 使用的是 `ActorList` -> `ActorTable` 组件链，所以对 `ActorTable.tsx` 的改动覆盖了 Jobs tab 下的 actor table。

### 5.3 Cluster 页面 — GPU 排序

**文件**:
- `python/ray/dashboard/client/src/pages/node/hook/useNodeList.ts`
- `python/ray/dashboard/client/src/pages/node/index.tsx`

问题：Cluster 页面的 "Sort By" dropdown 支持 CPU 排序但不支持 GPU。`useSorter` 用 `lodash.get()` 按路径取值，但 `gpus` 是数组（每个 GPU 一个 `GPUStats` 对象），无法直接用路径访问。

解决方案：在 `useNodeList.ts` 的 `nodeListWithAdditionalInfo` mapping 中预计算 `gpuUtilization` 聚合值：

```typescript
// useNodeList.ts
const nodeListWithAdditionalInfo = nodeList.map((e) => ({
    ...e,
    state: e.raylet.state,
    logicalResources: nodeLogicalResources[e.raylet.nodeId],
    gpuUtilization: (e.gpus ?? []).reduce(
        (sum, gpu) => sum + (gpu.utilizationGpu ?? 0),
        0,
    ),
}));
```

然后在 Sort By dropdown 加入 GPU 选项：

```typescript
// index.tsx
["gpuUtilization", "GPU"],  // 放在 CPU 后面
```

### 5.4 修改的文件汇总（排序部分）

| 文件 | 改动 |
|------|------|
| `TaskTable.tsx` | Duration 列头排序，`getTaskDuration` 计算函数 |
| `ActorTable.tsx` | 导入 `TableSortLabel`，Uptime 列头排序与 dropdown 联动 |
| `useNodeList.ts` | 预计算 `gpuUtilization` 聚合值 |
| `node/index.tsx` | Sort By dropdown 新增 GPU 选项 |

---

## 六、Commit 记录

```
69ead34f7a [T11609366] Add duration/uptime column sort and GPU sort for dashboard
aea190f60d [T11609366] Fix Blocks Input metric and add Queued Blocks display
```
