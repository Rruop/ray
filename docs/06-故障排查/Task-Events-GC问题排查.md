# Ray Task Events GC 问题排查指南

## 问题背景

在使用 Ray Dashboard 查询 Task 信息时，发现通过 `/api/v0/tasks/summarize` 接口查询特定 Job 的 task 时，返回结果显示 `num_after_truncation: 0`，表示 GCS 中没有存储任何 task 记录，但 `total` 字段显示历史上有数万个 task。

### 典型问题现象

```json
{
  "total": 52914,              // 历史上有 52914 个 task
  "num_after_truncation": 0,   // GCS 当前存储: 0 个
  "num_filtered": 0,           // 过滤后: 0 个
  "result": {
    "node_id_to_summary": {
      "cluster": {
        "summary": {},         // 空的
        "total_tasks": 0,
        "total_actor_tasks": 0,
        "total_actor_scheduled": 0
      }
    }
  }
}
```

---

## 一、相关 API 接口说明

### 1.1 `/api/v0/tasks` 接口

**示例请求：**
```
/api/v0/tasks?detail=1&limit=10000&filter_keys=job_id&filter_predicates=%3D&filter_values=05000000
```

**参数说明：**
- `detail=1`: 返回详细信息
- `limit=10000`: 最多返回 10000 条
- `filter_keys=job_id`: 按 job_id 过滤
- `filter_predicates=%3D`: 等于操作 (`=`)
- `filter_values=05000000`: job_id 值

### 1.2 `/api/v0/tasks/summarize` 接口

**示例请求：**
```
/api/v0/tasks/summarize?filter_keys=job_id&filter_predicates=%3D&filter_values=36000000
```

**处理流程：**

```
┌─────────────────────────────────────────────────────────────────────┐
│                     1. HTTP 请求解析                                 │
│  state_head.py:302 → handle_summary_api()                          │
└──────────────────────────────┬──────────────────────────────────────┘
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     2. summarize_tasks()                            │
│  state_aggregator.py:569-618                                        │
│  内部调用 list_tasks()，使用 limit = RAY_MAX_LIMIT_FROM_API_SERVER   │
└──────────────────────────────┬──────────────────────────────────────┘
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     3. list_tasks() → GCS RPC                       │
│  state_aggregator.py:298-356                                        │
│  调用 get_all_task_info() 发送 gRPC 请求到 GCS                       │
└──────────────────────────────┬──────────────────────────────────────┘
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│               4. GCS TaskManager 处理 (C++ 层)                       │
│  gcs_task_manager.cc:450-620                                        │
│  - 源端过滤（job_id, task_id, actor_id, name, state）               │
│  - Limit 截断                                                       │
└──────────────────────────────┬──────────────────────────────────────┘
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   5. Python 端二次过滤                               │
│  state_aggregator.py:330 - do_filter()                              │
│  应用 GCS 不支持的过滤条件（node_id, type, func_or_class_name 等）   │
└──────────────────────────────┬──────────────────────────────────────┘
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│               6. TaskSummaries.to_summary_by_func_name()            │
│  common.py:1036-1072                                                │
│  按 func_or_class_name 分组，统计每个状态的数量                      │
└─────────────────────────────────────────────────────────────────────┘
```

### 1.3 返回字段说明

| 字段 | 含义 |
|------|------|
| `total` | 历史总数 = 当前存储 + 已被 GC 清理 |
| `num_after_truncation` | GCS 当前实际存储的 task 数量 |
| `num_filtered` | 应用 filter 后的数量 |
| `num_status_task_events_dropped` | 因 limit 或内存限制丢弃的数量 |

---

## 二、GC 清理机制详解

### 2.1 GC vs Filter 的区别

| 操作 | 时机 | 目的 | 是否可恢复 |
|------|------|------|-----------|
| **GC 清理** | Task 存储时，超过容量限制 | 释放内存，删除旧数据 | ❌ 永久删除 |
| **Filter 过滤** | 查询时，根据条件筛选 | 返回符合条件的数据 | ✅ 数据还在存储中 |

### 2.2 GCS 存储结构

```cpp
// gcs_task_manager.h:190
// 按优先级分成 3 个 list 存储
task_events_list_[3] = {
    list[0]: FINISHED task (优先级最低，最先清理)
    list[1]: Actor task (优先级中等)
    list[2]: 其他未完成 task (优先级最高，最后清理)
}
```

### 2.3 优先级判定逻辑

**代码位置：** `src/ray/common/protobuf_utils.cc`

```cpp
// 判断是否已完成 - 只看 FINISHED 状态
bool IsTaskFinished(const rpc::TaskEvents &task_event) {
    return state_updates.state_ts_ns().contains(rpc::TaskStatus::FINISHED);
}

// 判断是否是 Actor task
bool IsActorTask(const rpc::TaskEvents &task_event) {
    return task_info.type() == ACTOR_TASK ||
           task_info.type() == ACTOR_CREATION_TASK;
}
```

**代码位置：** `src/ray/gcs/gcs_task_manager.h:75-85`

```cpp
size_t GetTaskListPriority(const rpc::TaskEvents &task_events) {
    if (IsTaskFinished(task_events)) return 0;  // FINISHED → 优先级 0，最先清理
    if (IsActorTask(task_events)) return 1;     // Actor task → 优先级 1
    return 2;                                    // 其他未完成 → 优先级 2，最后清理
}
```

### 2.4 GC 清理触发条件和逻辑

**代码位置：** `src/ray/gcs/gcs_task_manager.cc:380-388`

```cpp
// 当存储数量超过限制时触发
if (stats_counter_.Get(kNumTaskEventsStored) > max_num_task_events_) {
    RAY_LOG_EVERY_MS(WARNING, 10000)
        << "Max number of tasks event (" << max_num_task_events_
        << ") allowed is reached. Old task events will be overwritten.";
    EvictTaskEvent();  // 清理一个 task
}

void EvictTaskEvent() {
    // 从优先级 0 开始找非空的 list
    for (list_index = 0; list_index < 3; ++list_index) {
        if (!task_events_list_[list_index].empty()) break;
    }
    // 从该 list 的末尾（最老的）清理
    auto &to_evict = task_events_list_[list_index].back();
    RemoveTaskAttempt(to_evict);  // 删除并记录 dropped 计数
}
```

### 2.5 重要：GC 是全局的，不是按 Job 隔离的

```
┌─────────────────────────────────────────────────────────────┐
│          GCS Task Event Storage (全局)                       │
│          max_num = RAY_task_events_max_num_task_in_gcs       │
├─────────────────────────────────────────────────────────────┤
│  list[0] (FINISHED tasks) ─────────────────────────────────│
│  │ Job A: task1✓ task2✓ task3✓ ...                         │
│  │ Job B: task1✓ task2✓ ...          ← 最先被清理          │
│  │ Job C: task1✓ task2✓ ...                                │
│                                                             │
│  list[1] (Actor tasks, 未完成) ────────────────────────────│
│  │ Job A: actor_task_x                                      │
│  │ Job C: actor_task_y                                      │
│                                                             │
│  list[2] (其他未完成 tasks) ───────────────────────────────│
│  │ Job B: running_task_y              ← 最后被清理          │
└─────────────────────────────────────────────────────────────┘
```

---

## 三、相关配置参数

### 3.1 GCS 层配置

**代码位置：** `src/ray/common/ray_config_def.h`

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `task_events_max_num_task_in_gcs` | 100,000 | GCS 存储的最大 task 数量，`-1` 表示无限制 |
| `task_events_max_num_profile_events_per_task` | 1,000 | 每个 task 的最大 profile event 数量 |

### 3.2 Worker 层配置

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `task_events_max_num_status_events_buffer_on_worker` | 100,000 | Worker 上的 task 状态缓冲 |
| `task_events_send_batch_size` | 10,000 | 每次发送给 GCS 的批次大小 |
| `task_events_report_interval_ms` | 1,000 | 发送间隔 |

### 3.3 API 层配置

**代码位置：** `python/ray/util/state/common.py`

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `RAY_MAX_LIMIT_FROM_API_SERVER` | 10,000 | API 返回给客户端的最大条数 |
| `RAY_MAX_LIMIT_FROM_DATA_SOURCE` | 10,000 | 数据源返回的最大条数 |

---

## 四、问题排查步骤

### 4.1 查看当前 GCS 存储状态

```bash
# 查看 task 统计
curl -s "http://<dashboard>:8265/api/v0/tasks?limit=1" | python3 -c "
import sys, json
data = json.load(sys.stdin)['data']['result']
print(f\"GCS 当前存储: {data['num_after_truncation']} 个 task\")
print(f\"总数(含已清理): {data['total']} 个 task\")
print(f\"已被 GC 清理: {data['total'] - data['num_after_truncation']} 个 task\")
"
```

### 4.2 查看配置值

**方法一：通过 Python**
```python
import ray
ray.init(address="auto")
print(ray._config.task_events_max_num_task_in_gcs())
```

**方法二：检查环境变量**
```bash
echo $RAY_task_events_max_num_task_in_gcs
```

### 4.3 查看 GCS 日志确认 GC 发生

```bash
# 搜索 GC Warning
grep "Max number of tasks event" /tmp/ray/session_latest/logs/gcs_server.out

# 输出示例（每 10 秒一次）:
# [2026-04-25 16:22:58,285 W 74 97] (gcs_server) gcs_task_manager.cc:382:
# Max number of tasks event (10000) allowed is reached.
# Old task events will be overwritten.
```

### 4.4 查看 Worker 日志

```bash
# 检查 Worker 是否有 task event 丢弃
grep -i "dropped\|task_event" /tmp/ray/session_latest/logs/worker*.out
```

### 4.5 分析集群 task 量

通过 Worker 日志中的统计信息判断：

```
# 关键指标
NodeManagerService.grpc_client.RequestWorkerLease - 3,263,998 total
                                                    ↑↑↑↑↑↑↑↑↑↑
                                                约 326 万次 task 调度
```

---

## 五、问题根因分析

### 5.1 案例分析

**日志发现：**
```
Max number of tasks event (10000) allowed is reached.
                          ↑↑↑↑↑
                      只有 1 万！
```

**问题：** 配置的 `task_events_max_num_task_in_gcs=10000`，远小于默认值 100,000。

**实际情况对比：**

| 配置 | 值 |
|------|-----|
| 实际配置 | 10,000 |
| 默认值 | 100,000 |
| Job task 数 | 52,914 |
| 全局 task 数 | ~3,260,000 |

### 5.2 GC 清理过程

```
时刻 T1: Job 开始运行
├── Task 1 RUNNING → FINISHED ✓
├── Task 2 RUNNING → FINISHED ✓
├── ...
└── Task 52914 RUNNING → FINISHED ✓
    所有 FINISHED task 存储在 GCS 的 list[0] 中

时刻 T2: 集群 task 总数超过 10,000
├── GC 开始清理 list[0]（FINISHED task）
└── 按 FIFO 顺序清理最老的 task

时刻 T3: 你查询 Job 的 task
├── GCS 存储中该 Job 的 task 已被清理
└── 返回结果: 0 个 task
```

---

## 六、解决方案

### 6.1 增大 GCS 存储限制

```bash
# 停止集群
ray stop --force

# 设置环境变量（根据 task 量设置，建议 500 万）
export RAY_task_events_max_num_task_in_gcs=5000000

# 或者无限制（注意内存消耗）
export RAY_task_events_max_num_task_in_gcs=-1

# 重启集群
ray start --head
```

### 6.2 同时增大 Worker Buffer

```bash
export RAY_task_events_max_num_status_events_buffer_on_worker=1000000
```

### 6.3 完整启动命令

```bash
RAY_task_events_max_num_task_in_gcs=5000000 \
RAY_task_events_max_num_status_events_buffer_on_worker=1000000 \
ray start --head
```

### 6.4 通过 ray.init() 设置

```python
ray.init(
    _system_config={
        "task_events_max_num_task_in_gcs": 5000000,
        "task_events_max_num_status_events_buffer_on_worker": 1000000,
    }
)
```

### 6.5 内存估算

```
task 数量 × 平均大小 ≈ 内存消耗

100,000 tasks × 2KB = ~200 MB
1,000,000 tasks × 2KB = ~2 GB
5,000,000 tasks × 2KB = ~10 GB
```

---

## 七、相关性能问题：对象重建

### 7.1 问题现象

在 Worker 日志中发现大量对象重建：

```
CoreWorker.RecoverObjects - 38,292 total (1 active)
                            ↑↑↑↑↑↑
                        3.8 万次对象恢复！
```

### 7.2 触发原因

| 原因 | 说明 |
|------|------|
| **对象丢失** | 存储对象的 Worker 挂了，需要重新计算 |
| **对象被驱逐** | 内存不足，对象被 evict，后续又需要使用 |
| **节点故障** | 某个节点挂掉，上面的对象需要重建 |

### 7.3 问题链分析

```
内存压力大
    ↓
对象被驱逐 (evict)
    ↓
后续 task 需要该对象
    ↓
触发 RecoverObjects (重新计算)
    ↓
PushTask 执行时间变长 (142秒/task)
    ↓
整体性能下降
```

### 7.4 排查方法

**检查内存使用：**
```bash
ray status
curl "http://<dashboard>:8265/api/v0/nodes?detail=1"
```

**检查 Object Store：**
```python
import ray
ray.init(address="auto")

for node in ray.nodes():
    print(f"Node: {node['NodeID'][:8]}")
    print(f"  Object Store Memory: {node.get('ObjectStoreAvailableMemory', 'N/A')}")
```

**检查磁盘 Spillover：**
```bash
ls -la /tmp/ray/session_latest/spill/
```

### 7.5 优化建议

| 方案 | 做法 |
|------|------|
| 增加 Object Store 内存 | `ray start --object-store-memory=50000000000` (50GB) |
| 减少中间对象 | 优化代码，及时 `del` 不需要的对象 |
| 使用 `ray.put()` 复用 | 共享大对象避免重复传输 |
| 检查 Worker 健康状态 | `ray status` 查看节点是否频繁挂掉 |

---

## 八、常用排查命令汇总

### 8.1 查看 Task 信息

```bash
# 查看所有 task
curl "http://<dashboard>:8265/api/v0/tasks?limit=10000&detail=1"

# 查看 task 汇总
curl "http://<dashboard>:8265/api/v0/tasks/summarize"

# 按 job 过滤
curl "http://<dashboard>:8265/api/v0/tasks?filter_keys=job_id&filter_predicates=%3D&filter_values=<JOB_ID>"

# 按状态过滤
curl "http://<dashboard>:8265/api/v0/tasks?filter_keys=state&filter_predicates=%3D&filter_values=RUNNING"
```

### 8.2 Ray CLI 命令

```bash
# 列出所有 task
ray list tasks --limit 10000

# 带详细信息
ray list tasks --detail --limit 10000

# 按状态过滤
ray list tasks --filter "state=RUNNING"
```

### 8.3 Python API

```python
from ray.util.state import list_tasks, summarize_tasks
from collections import Counter

# 列出所有 task
tasks = list_tasks(limit=10000)
print(f"当前存储的 task 数量: {len(tasks)}")

# 按状态统计
states = Counter(t["state"] for t in tasks)
print(states)

# 按 job 统计
jobs = Counter(t["job_id"] for t in tasks)
print(jobs)

# 汇总
summary = summarize_tasks()
print(summary)
```

### 8.4 日志搜索

```bash
# GCS GC 日志
grep "Max number of tasks event" /tmp/ray/session_latest/logs/gcs_server.out

# Task Event 相关
grep -i "task_event\|evict\|dropped" /tmp/ray/session_latest/logs/gcs_server.out

# Worker 日志
grep -i "dropped\|task_event" /tmp/ray/session_latest/logs/worker*.out
```

---

## 九、Event Stats 监控指南

GCS 和 Core Worker 日志中的 Event Stats 是性能排查的重要信息来源。

### 9.1 GCS Event Stats 概览

GCS 日志中有多个 IO Context 的 Event Stats：

| Context | 说明 | 关注点 |
|---------|------|--------|
| `Main service Event stats` | GCS 主服务统计 | Actor/Job/Node 管理 |
| `task_io_context Event stats` | Task 事件处理 | Task 上报和查询 |
| `pubsub_io_context Event stats` | Pub/Sub 系统 | 消息订阅分发 |
| `ray_event_io_context Event stats` | Ray Event 系统 | 事件处理 |

### 9.2 Main Service Event Stats

**日志位置：** `gcs_server.out`

```
Main service Event stats:
  ActorInfoGcsService.grpc_server.RegisterActor - 156 total
  ActorInfoGcsService.grpc_server.GetAllActorInfo - 89 total (2 active)
  JobInfoGcsService.grpc_server.AddJob - 12 total
  JobInfoGcsService.grpc_server.GetAllJobInfo - 234 total
  NodeInfoGcsService.grpc_server.GetAllNodeInfo - 567 total
```

**关键指标：**

| 指标 | 正常范围 | 异常信号 |
|------|----------|----------|
| `RegisterActor` | 与 Actor 数量匹配 | 远大于预期 → Actor 频繁重建 |
| `GetAllActorInfo (active)` | < 5 | > 10 → 查询堆积，性能瓶颈 |
| `AddJob` | 与 Job 数量匹配 | 极高 → Driver 频繁重启 |
| `GetAllNodeInfo (active)` | < 3 | > 10 → Dashboard 查询压力大 |

### 9.3 Task IO Context Event Stats

**日志位置：** `gcs_server.out`

```
task_io_context Event stats:
  TaskInfoGcsService.grpc_server.AddTaskEventData - 45678 total (3 active)
  TaskInfoGcsService.grpc_server.GetTaskEvents - 123 total (1 active)
```

**关键指标：**

| 指标 | 正常范围 | 异常信号 |
|------|----------|----------|
| `AddTaskEventData total` | 与 task 总量匹配 | 远小于 task 量 → Worker 上报问题 |
| `AddTaskEventData (active)` | < 10 | > 50 → 上报堆积，GCS 处理慢 |
| `GetTaskEvents (active)` | < 5 | > 20 → 查询压力大 |

### 9.4 Pub/Sub IO Context Event Stats

**日志位置：** `gcs_server.out`

```
pubsub_io_context Event stats:
  InternalPubSubGcsService.grpc_server.GcsSubscriberPoll - 12345 total (8 active)
  InternalPubSubGcsService.grpc_server.GcsSubscriberCommandBatch - 678 total
```

**关键指标：**

| 指标 | 说明 | 异常信号 |
|------|------|----------|
| `GcsSubscriberPoll total` | 订阅者轮询次数 | 极高 → 大量订阅者 |
| `GcsSubscriberPoll (active)` | 当前活跃轮询 | > 100 → 订阅压力大 |
| `GcsSubscriberCommandBatch` | 命令批次数 | 与订阅者数量相关 |

### 9.5 Core Worker Event Stats

**日志位置：** `worker-*.out` 或 `core_worker.cc` 输出

```
Event stats:
  CoreWorker.RecoverObjects - 38292 total (1 active)
  CoreWorkerService.grpc_server.PushTask - 156789 total (12 active), 142.5s avg
  NodeManagerService.grpc_client.RequestWorkerLease - 3263998 total (5 active)
  ObjectManager.grpc_client.Pull - 4567 total (3 active)
  CoreWorkerService.grpc_server.GetObjectLocationsOwner - 8901 total
  RayletClient::ReportWorkerBacklog - 234 total
  GCS.grpc_client.AddTaskEventData - 12345 total (2 active)
```

**关键指标详解：**

| 指标 | 正常范围 | 异常信号 | 说明 |
|------|----------|----------|------|
| `RecoverObjects` | < 100 | > 1000 | 对象丢失/驱逐后重建 |
| `PushTask avg time` | < 1s | > 10s | Task 执行平均耗时 |
| `PushTask (active)` | < 20 | > 100 | 并发执行的 task |
| `RequestWorkerLease` | 与 task 量匹配 | 远大于 task 量 → 频繁调度 |
| `Pull total` | < 1000 | > 10000 | 对象远程拉取次数 |
| `GetObjectLocationsOwner` | - | 极高 → 对象查找频繁 |
| `AddTaskEventData (active)` | < 5 | > 20 | Task 事件上报堆积 |

### 9.6 异常模式识别

#### 模式 1：对象重建过多

```
RecoverObjects - 38292 total        ← 异常：大量对象重建
Pull - 45678 total                  ← 异常：大量远程拉取
PushTask avg - 142.5s               ← 异常：执行时间过长
```

**根因：** 内存不足导致对象被驱逐，后续需要重建
**解决：** 增加 Object Store 内存，优化数据流

#### 模式 2：Task 调度压力大

```
RequestWorkerLease - 3263998 total  ← 大量调度请求
PushTask (active) - 156             ← 高并发执行
GetObjectLocationsOwner - 890123    ← 大量对象查找
```

**根因：** 大量小 task，调度开销大
**解决：** 合并小 task，使用 batch 处理

#### 模式 3：Task 事件上报堆积

```
GCS AddTaskEventData (active) - 25  ← 上报堆积
task_io_context AddTaskEventData (active) - 50  ← GCS 处理慢
```

**根因：** Task 事件量超过 GCS 处理能力
**解决：** 增加 `task_events_report_interval_ms`，减少上报频率

#### 模式 4：Pub/Sub 压力

```
GcsSubscriberPoll (active) - 150    ← 大量活跃订阅
```

**根因：** Dashboard 或其他客户端频繁查询
**解决：** 减少查询频率，检查是否有异常客户端

### 9.7 Event Stats 日志搜索命令

```bash
# GCS Main Service Stats
grep -A 20 "Main service Eve stats" /tmp/ray/session_latest/logs/gcs_server.out | tail -25

# Task IO Context Stats
grep -A 10 "task_io_context Event stats" /tmp/ray/session_latest/logs/gcs_server.out | tail -15

# Pub/Sub Stats
grep -A 10 "pubsub_io_context Event stats" /tmp/ray/session_latest/logs/gcs_server.out | tail -15

# Core Worker Stats (所有 Worker)
grep -A 20 "Event stats" /tmp/ray/session_latest/logs/worker-*.out | head -100

# 查找对象重建
grep "RecoverObjects" /tmp/ray/session_latest/logs/worker-*.out

# 查找 PushTask 执行时间
grep "PushTask" /tmp/ray/session_latest/logs/worker-*.out | grep "avg"
```

---

## 十、性能排查快速参考

### 10.1 问题分类与排查入口

| 问题类型 | 主要症状 | 首先检查 |
|----------|----------|----------|
| **Task 不可见** | API 返回 0 个 task | GCS GC 日志，`num_after_truncation` |
| **Task 执行慢** | PushTask avg 时间长 | RecoverObjects，Pull 次数 |
| **调度慢** | 任务排队时间长 | RequestWorkerLease (active) |
| **内存压力** | OOM，对象驱逐 | Object Store 使用率，Spill 目录 |
| **GCS 响应慢** | API 超时 | Event Stats 中的 (active) 数量 |

### 10.2 关键日志文件

| 文件 | 内容 | 排查场景 |
|------|------|----------|
| `gcs_server.out` | GCS 服务日志 | Task GC、Event Stats、错误 |
| `gcs_server.err` | GCS 错误日志 | 严重错误、崩溃 |
| `raylet.out` | Raylet 日志 | 调度、资源管理 |
| `worker-*.out` | Worker 日志 | Task 执行、对象操作 |
| `dashboard.log` | Dashboard 日志 | API 请求、查询 |
| `monitor.log` | 监控日志 | 集群状态变化 |

### 10.3 快速诊断流程

```
1. 确定问题类型
   ├── Task 不可见 → 检查 GC (第四章)
   ├── 性能慢 → 检查 Event Stats (第九章)
   └── 错误/崩溃 → 检查 .err 日志

2. 查看 Event Stats
   ├── (active) 数量异常 → 处理堆积
   ├── total 数量异常 → 量级问题
   └── avg time 异常 → 执行效率问题

3. 定位根因
   ├── RecoverObjects 高 → 内存/对象问题
   ├── RequestWorkerLease 高 → 调度问题
   └── AddTaskEventData 堆积 → Task 事件问题

4. 应用解决方案
   └── 参考各章节的优化建议
```

### 10.4 常用配置参数速查

| 参数 | 默认值 | 调整场景 |
|------|--------|----------|
| `task_events_max_num_task_in_gcs` | 100,000 | Task 被 GC 清理 |
| `task_events_report_interval_ms` | 1,000 | 上报压力大 (设为 0 禁用) |
| `object_store_memory` | 自动 | 对象驱逐频繁 |
| `task_events_max_num_status_events_buffer_on_worker` | 100,000 | Worker 缓冲不足 |

### 10.5 告警阈值建议

| 指标 | 警告阈值 | 严重阈值 |
|------|----------|----------|
| `RecoverObjects total` | > 1,000 | > 10,000 |
| `PushTask avg time` | > 10s | > 60s |
| `Any (active) count` | > 50 | > 200 |
| `GC Warning 频率` | 每分钟 > 1 次 | 持续出现 |
| `num_after_truncation / total` | < 50% | < 10% |

---

## 十一、关键代码位置参考

| 模块 | 文件路径 | 说明 |
|------|----------|------|
| GCS Task Manager | `src/ray/gcs/gcs_task_manager.cc` | GC 清理逻辑 |
| GC 策略 | `src/ray/gcs/gcs_task_manager.h` | 优先级定义 |
| 配置定义 | `src/ray/common/ray_config_def.h` | 所有配置参数 |
| Task 状态判断 | `src/ray/common/protobuf_utils.cc` | IsTaskFinished, IsActorTask |
| API 处理 | `python/ray/dashboard/state_aggregator.py` | list_tasks, summarize_tasks |
| Filter 逻辑 | `python/ray/dashboard/state_api_utils.py` | do_filter |
| HTTP Handler | `python/ray/dashboard/modules/state/state_head.py` | API 入口 |

---

## 十二、总结

### 问题诊断流程

```
1. 查看 API 返回的 num_after_truncation 和 total
   ↓
2. 如果 num_after_truncation << total，说明有 GC
   ↓
3. 检查 GCS 日志确认 "Max number of tasks event" Warning
   ↓
4. 检查 task_events_max_num_task_in_gcs 配置值
   ↓
5. 根据实际 task 量调整配置
   ↓
6. 重启集群
```

### 关键配置建议

| 场景 | 建议配置 |
|------|----------|
| 小规模 (<10万 task) | 使用默认值 100,000 |
| 中等规模 (10万-100万 task) | 500,000 - 1,000,000 |
| 大规模 (>100万 task) | 5,000,000 或 -1（无限制） |

### 注意事项

1. 配置修改需要**重启集群**才能生效
2. 增大配置会增加 GCS **内存消耗**
3. GC 是**全局的**，不区分 Job
4. 被 GC 清理的数据**无法恢复**
5. 如需保留历史数据，考虑使用 Task Event **导出功能**
