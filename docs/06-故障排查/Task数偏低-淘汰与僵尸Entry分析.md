# Ray Dashboard Task 显示数量异常少：GCS Task Event 淘汰与僵尸 Entry 分析

> 集群规模：300+ 节点，902 GPU | Ray 版本：2.54.4+kuaishou.cc121a56b9
> 问题日期：2026-05-21
> 关联文档：[Task Event 数据链路与淘汰分析](../ray-task-event-data-flow-and-eviction-analysis.md)

---

## 一、问题现象

### 1.1 Dashboard 显示

Ray Core Overview 页面显示的 task 数量远低于实际运行数：

```
Total: 1486
Running: 1279
Waiting for scheduling: 207
```

而通过 `ray status` 查看资源占用：

```
900.0/902.0 GPU in use    → 实际约 900 个 task 正在执行
```

**Dashboard 显示 RUNNING ~1,279 vs 实际 RUNNING ~900（GPU 占用数），两者数量级接近但来源不同：
Dashboard 的 1,279 来自 GCS 过滤后仅有 task_info 的 entry 中状态为 RUNNING 的部分，
而实际 RUNNING 的 task 中大部分因僵尸 entry 问题在 Dashboard 上不可见。**

具体差异：
- 实际 RUNNING task：~900（以 GPU 占用数为准）
- GCS 能查到有 task_info 的 RUNNING entry：仅 63 条（gRPC 直查）
- **差值 ~837 个 RUNNING task 完全不可见**
- **RUNNING 的 actor task 完全不显示**（`total_actor_tasks: 0`）
- **FINISHED 的 task 完全不显示**
- 显示的总数远低于 GCS buffer 容量（100,000）和 API 返回上限（10,000）

### 1.2 GCS 日志告警

GCS 进程日志持续输出告警（每 10 秒一次）：

```
[WARNING] Max number of tasks event (100000) allowed is reached.
Old task events will be overwritten. Set `RAY_task_events_max_num_task_in_gcs`
to a higher value to store more.
```

累计已输出 41,746 次此告警。

### 1.3 Dashboard NodeHead CPU 100%

```bash
$ ps aux | grep NodeHead
ray  528  100%  3.2g  ... ray-dashboard-NodeHead-0
```

NodeHead 进程 CPU 满载运行超过 5 天，日志中有 5,735 条 `DEADLINE_EXCEEDED` 错误，
原因是需要轮询 300+ 节点的 `GetNodeStats`，大量节点超时。

---

## 二、排查手段

### 2.1 集群资源状态

```bash
$ ray status
```

关键输出：
```
Resources
---------------------------------------------------------------
Usage:
 900.0/902.0 GPU
 57372.0/84826.0 CPU
 ...

Total Coverage:
 Active Nodes: 305
```

结论：GPU 几乎满载（900/902），大量 task 排队等待调度。

### 2.2 Task Summarize API 调用

```bash
$ curl -s "http://localhost:8265/api/v0/tasks/summarize" | python3 -m json.tool
```

返回结果：
```json
{
  "result": {
    "node_id_to_summary": {
      "cluster": {
        "summary": {
          "_map_task": {
            "state_counts": {
              "RUNNING": 1229,
              "PENDING_NODE_ASSIGNMENT": 2
            }
          }
        },
        "total_tasks": 1231,
        "total_actor_tasks": 0,
        "total_actor_scheduled": 0
      }
    }
  },
  "data": {
    "result": {
      "total": 614407715,
      "num_after_truncation": 1231,
      "num_filtered": 0
    }
  }
}
```

关键数据：
- `total`: 614,407,715（历史提交 6.14 亿个 task）
- `num_after_truncation`: 1,231（GCS 实际返回给 Dashboard 的 task 数）
- `total_actor_tasks`: 0（**actor task 全部不可见**）

### 2.3 直接 gRPC 调用 GCS（绕过 Dashboard）

通过 Python 直接调用 GCS 的 `GetTaskEvents` gRPC 接口获取内部统计：

```python
import ray
from ray.core.generated import gcs_service_pb2, gcs_service_pb2_grpc
from ray.core.generated.gcs_pb2 import GetTaskEventsRequest
import grpc

channel = grpc.insecure_channel("localhost:6379")  # GCS port
stub = gcs_service_pb2_grpc.TaskInfoGcsServiceStub(channel)
request = GetTaskEventsRequest(limit=2000)
reply = stub.GetTaskEvents(request, timeout=30)

print(f"num_total_stored: {reply.num_total_stored}")
print(f"num_filtered_on_gcs: {reply.num_filtered_on_gcs}")
print(f"events_by_task count: {len(reply.events_by_task)}")
```

结果：
```
num_total_stored: 100,000    ← buffer 已满
num_filtered_on_gcs: 98,489  ← 被 GCS 内部过滤掉
events_by_task count: 1,511  ← 实际返回
```

**关键发现：100,000 条中有 98,489 条被 GCS 过滤，只有 1,511 条通过过滤。**

### 2.4 返回 entry 的状态分布分析

对返回的 1,511 条 task event 分析其最高状态：

```python
from collections import Counter
state_counter = Counter()
for event in reply.events_by_task:
    max_state = max(event.state_updates.state_ts_ns.keys()) if event.state_updates.state_ts_ns else 0
    state_counter[max_state] += 1
print(state_counter)
```

结果：
```
PENDING_NODE_ASSIGNMENT(2): 1,296   ← 绝大多数停留在等调度状态
SUBMITTED_TO_WORKER(5): 151
RUNNING(8): 63                      ← 只有 63 个在 RUNNING 状态
FINISHED(11): 1
```

**验证：所有返回的 1,511 条都有 `task_info`，且大部分只有 Driver 侧状态，Worker 的 RUNNING 状态尚未合并。**

### 2.5 Driver 端 Task Event 上报状态检查

在 head 节点检查 Driver 进程的 task event buffer：

```bash
$ grep -c "num status task events dropped" /tmp/ray/session_*/logs/python-core-driver-*.log
```

结果：
```
num status task events dropped: 0     ← Driver 没有丢事件
flush_task_events 执行: 883 次
AddTaskEventData gRPC 调用: 855 次
当前 buffer 大小: ~200 事件
```

**结论：Driver 端上报正常，未丢弃事件，buffer 没有溢出。**

### 2.6 环境变量和配置

```bash
$ python3 -c "import ray; ray.init(address='auto'); print(ray._config.task_events_max_num_task_in_gcs())"
# 或通过 system_config 查看
$ cat /tmp/ray/session_*/params.json
```

关键配置：
```
RAY_task_events_max_num_task_in_gcs: 100,000 (默认值)
task_events_report_interval_ms: 10,000 (10秒 flush 间隔)
RAY_MAX_LIMIT_FROM_API_SERVER: 10,000
RAY_STATE_SERVER_MAX_HTTP_REQUEST_ALLOWED: 1,000
```

### 2.7 验证实际 RUNNING task 总数

Dashboard 显示的 RUNNING 数不可信（受僵尸 entry 影响），需要通过以下方式获取真实 RUNNING 数：

#### 方式一：通过 GPU 占用数推算

```bash
$ ray status | grep GPU
# 输出：900.0/902.0 GPU
# → 如果每个 task 用 1 GPU，则实际 RUNNING ≈ 900
```

#### 方式二：Worker 进程计数（需登录 Worker 节点）

```bash
# 在任意 Worker 节点上
$ ps aux | grep "ray::" | grep -v grep | wc -l
# 乘以节点数 → 集群总 RUNNING task 数
```

#### 方式三：Actor 列表（验证 actor task）

```bash
$ curl -s "http://localhost:8265/api/v0/actors?limit=1000" | python3 -c "
import json, sys
data = json.load(sys.stdin)
actors = data['data']['result']
alive = [a for a in actors if a.get('state') == 'ALIVE']
print(f'Total actors: {len(actors)}')
print(f'ALIVE actors: {len(alive)}')
# 每个 ALIVE actor 都在处理 actor task，但 Dashboard task 页看不到它们
"
```

#### 交叉验证

```
实际 RUNNING task 数（真值）≈ GPU in use = 900
Dashboard 显示 RUNNING = 1,279（表面值，含 Driver 侧 PENDING 被误计的）
gRPC 查到有 task_info 且状态=RUNNING 的 entry = 63 条

差值 = 900 - 63 = ~837 个 RUNNING task 因僵尸 entry 问题不可见
```

### 2.8 确认哪些 entry 缺少 task_info：API 透传问题

#### REST API 无法直接诊断

`GetTaskEventsReply` gRPC 响应包含以下字段（`gcs_service.proto:861-876`）：

```protobuf
message GetTaskEventsReply {
  GcsStatus status = 1;
  repeated TaskEvents events_by_task = 2;
  int32 num_profile_task_events_dropped = 3;
  int32 num_status_task_events_dropped = 4;
  int64 num_total_stored = 5;          // ← buffer 中总条数
  int64 num_filtered_on_gcs = 6;       // ← GCS 侧过滤掉的条数（关键！）
  int64 num_truncated = 7;             // ← 因 limit 截断的条数
}
```

但 Python 侧 `list_tasks`（`state_aggregator.py:325-341`）**丢弃了关键字段**：

```python
# state_aggregator.py:325-341 — 只用了这些字段
num_after_truncation = len(result)                          # ← events_by_task 的条数
num_total = len(result) + reply.num_status_task_events_dropped  # ← 加上 dropped 数

# ⚠️ 以下字段完全未使用，也未透传到 REST API：
# reply.num_total_stored      → 不可见
# reply.num_filtered_on_gcs   → 不可见
# reply.num_truncated         → 不可见
```

#### 各接口能力对比

| 方法 | 能看到 `num_total_stored` | 能看到 `num_filtered_on_gcs` | 用途 |
|------|:---:|:---:|------|
| REST API `/api/v0/tasks` | **否** | **否** | 只能看到过滤后结果 |
| REST API `/api/v0/tasks/summarize` | **否** | **否** | 同上 |
| `ray list tasks` CLI | **否** | **否** | 同上 |
| **gRPC 直连 GCS** | **是** | **是** | **唯一可诊断僵尸 entry 问题的方式** |

#### gRPC 直连诊断（唯一有效方式）

```python
# 在 head 节点执行
python3 -c "
import grpc
from ray.core.generated import gcs_service_pb2_grpc
from ray.core.generated.gcs_pb2 import GetTaskEventsRequest

channel = grpc.insecure_channel('localhost:6379')
stub = gcs_service_pb2_grpc.TaskInfoGcsServiceStub(channel)

# 不带任何 filter，limit 设小即可（只需要统计字段）
reply = stub.GetTaskEvents(GetTaskEventsRequest(limit=10), timeout=10)

print(f'Buffer 中总条数 (num_total_stored):    {reply.num_total_stored}')
print(f'GCS 侧过滤条数 (num_filtered_on_gcs): {reply.num_filtered_on_gcs}')
print(f'因 limit 截断 (num_truncated):         {reply.num_truncated}')
print(f'实际返回条数 (events_by_task):          {len(reply.events_by_task)}')
print(f'历史丢弃数 (dropped):                  {reply.num_status_task_events_dropped}')
print()
if reply.num_total_stored > 0:
    pct = reply.num_filtered_on_gcs / reply.num_total_stored * 100
    print(f'无 task_info 占比: {reply.num_filtered_on_gcs}/{reply.num_total_stored} = {pct:.1f}%')
    if pct > 50:
        print('⚠️  超过一半的 entry 缺少 task_info —— 存在僵尸 entry 问题')
"
```

当不传任何 `state_filters` / `task_filters` 时，`num_filtered_on_gcs` **全部来自 `has_task_info()` 过滤**（这是唯一的隐式硬过滤条件）。

#### 验证返回 entry 是否全有 task_info

```python
# 检查返回的 entry 是否确实都有 task_info
python3 -c "
import grpc
from ray.core.generated import gcs_service_pb2_grpc
from ray.core.generated.gcs_pb2 import GetTaskEventsRequest

channel = grpc.insecure_channel('localhost:6379')
stub = gcs_service_pb2_grpc.TaskInfoGcsServiceStub(channel)
reply = stub.GetTaskEvents(GetTaskEventsRequest(limit=2000), timeout=30)

has_info = sum(1 for e in reply.events_by_task if e.HasField('task_info'))
no_info = sum(1 for e in reply.events_by_task if not e.HasField('task_info'))
print(f'返回的 entry 中有 task_info: {has_info}')
print(f'返回的 entry 中无 task_info: {no_info}')  # 应为 0（无 task_info 的都被过滤了）

# 分析有 task_info 的 entry 的状态分布
from collections import Counter
state_names = {0:'NIL', 2:'PENDING_NODE_ASSIGN', 5:'SUBMITTED_TO_WORKER',
               8:'RUNNING', 11:'FINISHED', 12:'FAILED'}
state_counter = Counter()
for event in reply.events_by_task:
    if event.state_updates.state_ts_ns:
        max_state = max(event.state_updates.state_ts_ns.keys())
        state_counter[state_names.get(max_state, str(max_state))] += 1
    else:
        state_counter['NO_STATE_UPDATE'] += 1

print()
print('可见 entry 状态分布:')
for state, count in state_counter.most_common():
    print(f'  {state}: {count}')
"
```

---

## 三、根因分析

### 3.1 GCS 过滤条件：`has_task_info()`

GCS 在查询时对每条 entry 进行过滤，核心过滤逻辑：

**文件：`src/ray/gcs/gcs_task_manager.cc:492-495`**

```cpp
if (!task_event.has_task_info()) {
    // Skip task events w/o task info.
    return false;
}
```

这意味着：**没有 `task_info` 的 entry 对查询完全不可见。** 98,489 条被过滤就是因为它们没有 `task_info` 字段。

### 3.2 task_info 的唯一来源

`task_info` 只在一个时机被设置——Driver 在 `PENDING_ARGS_AVAIL` 状态首次上报时：

**文件：`src/ray/core_worker/task_event_buffer.cc:444`**

```cpp
auto task_event = std::make_unique<TaskStatusEvent>(
    task_id, job_id, attempt_number, status,
    /* timestamp */ absl::GetCurrentTimeNanos(),
    /*is_actor_task_event=*/spec.IsActorTask(),
    session_name_, node_id_,
    include_task_info ? std::make_shared<const TaskSpecification>(spec) : nullptr,  // ← 只此一次
    std::move(state_update));
```

各状态上报的 `include_task_info` 参数：

| 状态 | 上报方 | `include_task_info` | 代码位置 |
|------|--------|---------------------|----------|
| `PENDING_ARGS_AVAIL` | Driver | **`true`** | `task_manager.cc:348-349` |
| `PENDING_NODE_ASSIGNMENT` | Driver | `false`（默认值） | `task_manager.cc:1682` |
| `SUBMITTED_TO_WORKER` | Driver | `false`（默认值） | `task_manager.cc:1697-1699` |
| `RUNNING` | Worker | `false` | `core_worker.cc:2868-2874` |
| `FINISHED` | Driver | `false`（默认值） | `task_manager.cc:1053` |
| `FAILED` | Driver | `false`（默认值） | `task_manager.cc:1313-1315` |

**`SetTaskStatus` 的函数签名确认默认值为 `false`：**

**文件：`src/ray/core_worker/task_manager.h:697-701`**

```cpp
void SetTaskStatus(
    TaskEntry &task_entry,
    rpc::TaskStatus status,
    std::optional<worker::TaskStatusEvent::TaskStateUpdate> state_update = std::nullopt,
    bool include_task_info = false,       // ← 默认不带 task_info
    std::optional<int32_t> attempt_number = std::nullopt);
```

### 3.3 Task 状态转换全链路

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                              Driver 进程                                      │
│  (提交 task 的 core_worker，运行在 head node 或 actor/task 提交者上)           │
│                                                                              │
│  ray.remote(fn).remote(args)                                                 │
│       │                                                                      │
│       ▼                                                                      │
│  ① PENDING_ARGS_AVAIL      ← 唯一携带 task_info 的上报                       │
│       │  "task 已提交，等待 ObjectRef 参数就绪"                                │
│       │  触发：SubmitTask() → task_manager.cc:343-349                         │
│       ▼                                                                      │
│  ② PENDING_NODE_ASSIGNMENT  ← 无 task_info                                   │
│       │  "所有参数已就绪，等待调度器分配执行节点"                                │
│       │  触发：MarkDependenciesResolved() → task_manager.cc:1682              │
│       ▼                                                                      │
│  ③ SUBMITTED_TO_WORKER      ← 无 task_info                                   │
│       │  "调度器已选择目标 Worker，请求已发送"                                  │
│       │  触发：MarkTaskWaitingForExecution() → task_manager.cc:1697-1699      │
│       │                                                                      │
└───────┼──────────────────────────────────────────────────────────────────────┘
        │  (gRPC RequestWorkerLease → Raylet → PushTask → Worker)
        ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                              Worker 进程                                      │
│  (实际执行 task 的 core_worker，运行在调度器分配的节点上)                       │
│                                                                              │
│  ④ RUNNING                  ← 无 task_info                                   │
│       │  "Worker 开始执行 task 函数体"                                        │
│       │  触发：ExecuteTask() → core_worker.cc:2868-2874                       │
│       │                                                                      │
└───────┼──────────────────────────────────────────────────────────────────────┘
        │  (Worker 执行完毕，通过 gRPC 返回结果给 Driver)
        ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                          Driver 进程（收到结果）                               │
│                                                                              │
│  ⑤ FINISHED                 ← 无 task_info                                   │
│       "Driver 收到 Worker 的执行结果（成功）"                                  │
│       触发：HandleTaskReturn() → task_manager.cc:1053                         │
│                                                                              │
│  ⑤' FAILED                  ← 无 task_info                                   │
│       "Driver 收到 Worker 的执行错误"                                         │
│       触发：FailPendingTask() → task_manager.cc:1313-1315                     │
└──────────────────────────────────────────────────────────────────────────────┘
```

### 3.4 Task Event Buffer 的上报-合并机制

所有 task event 都**先缓冲在本地**（Driver/Worker 各自的 `TaskEventBufferImpl`），
每 `task_events_report_interval_ms`（本集群配置为 10 秒）flush 一次到 GCS：

**文件：`src/ray/core_worker/task_event_buffer.cc:420-449`**

```cpp
bool TaskEventBufferImpl::RecordTaskStatusEventIfNeeded(...) {
    auto task_event = std::make_unique<TaskStatusEvent>(
        task_id, job_id, attempt_number, status,
        absl::GetCurrentTimeNanos(),
        spec.IsActorTask(), session_name_, node_id_,
        include_task_info ? std::make_shared<const TaskSpecification>(spec) : nullptr,
        std::move(state_update));
    AddTaskEvent(std::move(task_event));  // 加入本地 buffer
    return true;
}
```

GCS 收到 flush 的批量事件后，对每个 task_id 调用 `UpdateOrInitTaskEventLocator`：

**文件：`src/ray/gcs/gcs_task_manager.cc:292-309`**

```cpp
std::shared_ptr<TaskEventLocator>
GcsTaskManagerStorage::UpdateOrInitTaskEventLocator(rpc::TaskEvents &&events_by_task) {
    TaskAttempt task_attempt = std::make_pair(task_id, attempt_number);

    auto loc_itr = primary_index_.find(task_attempt);
    if (loc_itr != primary_index_.end()) {
        // ✅ entry 已存在 → MergeFrom 合并（task_info 保留）
        UpdateExistingTaskAttempt(loc_itr->second, events_by_task);
        return loc_itr->second;
    }

    // ⚠️ entry 不存在 → 创建新 entry
    // 如果上报不含 task_info → 新 entry 永远没有 task_info
    auto loc = AddNewTaskEvent(std::move(events_by_task));
    return loc;
}
```

**MergeFrom 合并逻辑：**

**文件：`src/ray/gcs/gcs_task_manager.cc:167-207`**

```cpp
void GcsTaskManagerStorage::UpdateExistingTaskAttempt(
    const std::shared_ptr<TaskEventLocator> &loc,
    const rpc::TaskEvents &task_events) {
    auto &existing_task = loc->GetTaskEventsMutable();

    // 如果新报的有 task_info 而旧的没有 → 统计计数
    if (task_events.has_task_info() && !existing_task.has_task_info()) {
        stats_counter_.Increment(kTaskTypeToCounterType.at(task_events.task_info().type()));
    }

    // protobuf MergeFrom: 新字段补充，已有字段保留
    existing_task.MergeFrom(task_events);

    // 合并后重新计算 GC 优先级，可能需要移动到不同 list
    auto target_list_index = gc_policy_->GetTaskListPriority(existing_task);
    auto cur_list_index = loc->GetCurrentListIndex();
    if (target_list_index != cur_list_index) {
        // 从旧 list 移到新 list 的最前面（最新位置）
        task_events_list_[target_list_index].push_front(std::move(existing_task));
        task_events_list_[cur_list_index].erase(loc->GetCurrentListIterator());
        loc->SetCurrentList(target_list_index, task_events_list_[target_list_index].begin());
    }

    UpdateIndex(loc);
}
```

**关键：如果 Worker 先到（创建了无 task_info 的 entry），Driver 后到时 MergeFrom 会把 task_info 合并进去——前提是 entry 还在 buffer 里。**

### 3.5 GC 淘汰策略

#### 3.5.1 优先级判定

**文件：`src/ray/gcs/gcs_task_manager.h:71-86`**

```cpp
class FinishedTaskActorTaskGcPolicy : public TaskEventsGcPolicyInterface {
 public:
  size_t MaxPriority() const override { return 3; }

  size_t GetTaskListPriority(const rpc::TaskEvents &task_events) const override {
    if (IsTaskFinished(task_events)) {
      return 0;   // FINISHED → 最先淘汰
    }
    if (IsActorTask(task_events)) {
      return 1;   // Actor task（非 FINISHED）→ 次优先
    }
    return 2;     // Normal task（非 FINISHED）→ 最后淘汰
  }
};
```

**`IsTaskFinished` 判定逻辑：**

**文件：`src/ray/common/protobuf_utils.cc:342-349`**

```cpp
bool IsTaskFinished(const rpc::TaskEvents &task_event) {
  if (!task_event.has_state_updates()) {
    return false;
  }
  const auto &state_updates = task_event.state_updates();
  // 只看 state_ts_ns map 中是否包含 FINISHED 键
  return state_updates.state_ts_ns().contains(rpc::TaskStatus::FINISHED);
}
```

**结论：**
- 淘汰优先级**不区分** PENDING / RUNNING 等中间状态，全部视为 priority 2
- 淘汰优先级**不区分**有无 `task_info`
- 同一 priority list 内按 **FIFO（最老的先淘汰）** 处理

#### 3.5.2 淘汰执行逻辑

**文件：`src/ray/gcs/gcs_task_manager.cc:332-349`**

```cpp
void GcsTaskManagerStorage::EvictTaskEvent() {
  // 找到第一个非空的最低优先级 list
  size_t list_index = 0;
  for (; list_index < gc_policy_->MaxPriority(); ++list_index) {
    if (!task_events_list_[list_index].empty()) {
      break;
    }
  }
  RAY_CHECK(list_index < gc_policy_->MaxPriority());

  // 从 list 尾部淘汰（back = 最老的 entry）
  const auto &to_evict = task_events_list_[list_index].back();
  const auto &loc_iter = primary_index_.find(GetTaskAttempt(to_evict));
  RAY_CHECK(loc_iter != primary_index_.end());
  RemoveTaskAttempt(loc_iter->second);
}
```

**插入和淘汰方向：**
```
task_events_list_[priority]:
  push_front (新 entry 插入) ──→ [新][新][旧][旧][最老] ←── back() (淘汰取这里)
```

#### 3.5.3 淘汰触发时机

每次 `AddOrReplaceTaskEvent` 插入新 entry 后检查是否超限：

**文件：`src/ray/gcs/gcs_task_manager.cc:379-388`**

```cpp
// If limit enforced, replace one.
if (max_num_task_events_ > 0 &&
    static_cast<size_t>(stats_counter_.Get(kNumTaskEventsStored)) > max_num_task_events_) {
    RAY_LOG_EVERY_MS(WARNING, 10000)
        << "Max number of tasks event (" << max_num_task_events_
        << ") allowed is reached. Old task events will be overwritten.";
    EvictTaskEvent();  // 淘汰一条
}
```

### 3.6 淘汰优先级总结

| 优先级 | 条件 | 淘汰顺序 | 包含的状态 |
|--------|------|----------|-----------|
| 0 | `state_ts_ns` 含 FINISHED 键 | 最先淘汰 | FINISHED（不论 task 类型） |
| 1 | 是 Actor Task 且未 FINISHED | 其次 | Actor task 的任何非终态状态 |
| 2 | 其他 | 最后淘汰 | Normal task 的 PENDING_ARGS_AVAIL / PENDING_NODE_ASSIGNMENT / SUBMITTED_TO_WORKER / RUNNING |

**同一优先级内：不区分有无 task_info，不区分具体状态，纯按插入时间 FIFO 淘汰最老的。**

---

## 四、僵尸 Entry 产生机制

### 4.1 核心矛盾

```
Driver 只在 PENDING_ARGS_AVAIL 时发送一次 task_info（不可重发）
           ×
Entry 在 buffer 中的存活时间 ≈ 100,000 / task_throughput ≈ 71 秒
           ×
Task 从提交到实际被 Worker 执行的等待时间可能 >> 71 秒（GPU 满载排队）
```

### 4.2 Buffer 周转时间计算

```
总历史 task 数：614,407,715
集群运行时间：~5 天（432,000 秒）
平均吞吐：614,407,715 / 432,000 ≈ 1,422 tasks/s

Buffer 容量：100,000
周转时间：100,000 / 1,422 ≈ 70 秒
```

**含义：任何 entry 在 GCS buffer 中平均存活约 70 秒后就会被淘汰。**

### 4.3 僵尸 Entry 的完整诞生过程

#### 正常情况（task 在 buffer 周转时间内开始执行）

```
t=0s:    Driver: SubmitTask()
         → 缓冲 PENDING_ARGS_AVAIL (include_task_info=true)

t=1s:    Driver: MarkDependenciesResolved()
         → 缓冲 PENDING_NODE_ASSIGNMENT (include_task_info=false)

t=5s:    Driver flush → 发送 GCS
         → GCS 创建 entry：{task_info ✓, state=PENDING_NODE_ASSIGNMENT, priority=2}

t=20s:   调度器分配 Worker
         Driver: MarkTaskWaitingForExecution()
         → 缓冲 SUBMITTED_TO_WORKER

t=21s:   Worker 收到 task，开始执行
         Worker: RecordTaskStatusEventIfNeeded(RUNNING, include_task_info=false)

t=25s:   Driver flush → GCS MergeFrom → entry: {task_info ✓, state=SUBMITTED_TO_WORKER}
t=31s:   Worker flush → GCS MergeFrom → entry: {task_info ✓, state=RUNNING}
         ✅ entry 有 task_info + RUNNING → Dashboard 可见
```

#### 异常情况（task 等待调度时间 > buffer 周转时间 → 产生僵尸）

```
t=0s:    Driver: SubmitTask()
         → 缓冲 PENDING_ARGS_AVAIL (include_task_info=true)

t=5s:    Driver flush → 发送 GCS
         → GCS 创建 entry：{task_info ✓, state=PENDING_NODE_ASSIGNMENT, priority=2}
         → entry 进入 task_events_list_[2] 的最前面

t=5~76s: 持续有新 entry 进入 buffer，旧 entry 被淘汰
         这个 entry 从 list 前面逐渐"滑向"尾部

t=76s:   ⚠️ 这个 entry 成为 list[2] 中最老的
         → 新 entry 需要空间
         → list[0] (FINISHED) 为空（FINISHED 的早被淘汰完了）
         → list[1] (Actor) 也可能为空或不够
         → 淘汰 list[2] 的 back() = 这个 entry
         → ⚡ task_info 永久丢失

t=80s:   终于有空闲 GPU，调度器分配 Worker
         Driver: MarkTaskWaitingForExecution()
         → 缓冲 SUBMITTED_TO_WORKER (include_task_info=false)

t=81s:   Worker 收到 task，开始执行
         → 缓冲 RUNNING (include_task_info=false)

t=85s:   Driver flush → 发送 [SUBMITTED_TO_WORKER] 到 GCS
         → GCS: primary_index_ 查找 task_id → 不存在（t=76s 已淘汰）
         → 创建新 entry：{task_info ✗, state=SUBMITTED_TO_WORKER, priority=2}
         → ⚡ 僵尸 entry 诞生！

t=91s:   Worker flush → 发送 [RUNNING] 到 GCS
         → GCS: entry 存在 → MergeFrom → state 更新为 RUNNING
         → 但 MergeFrom 不会凭空创建 task_info
         → entry 仍然：{task_info ✗, state=RUNNING, priority=2}

t=∞:     Driver 不会再发 task_info（只在 PENDING_ARGS_AVAIL 时发过一次）
         → 这个 entry 永远没有 task_info
         → 查询时被 has_task_info() 过滤
         → Dashboard 完全看不到这个 task
```

### 4.4 为什么 FINISHED entries 不够淘汰

用户可能疑问：*"不是 FINISHED 的先淘汰吗？只要有 FINISHED entry，RUNNING entry 就不会被动吧？"*

在高吞吐稳态下：
- 一个 task 完成 → entry 变为 FINISHED（priority 0）
- 下一个新 entry 到来 → 淘汰这个 FINISHED entry
- 但如果**同时有多个新 entry 需要空间**而只有一个 FINISHED entry → 不够用
- 此时就会淘汰 Actor task (priority 1) 和 Normal task (priority 2)

更关键的是，在稳态下 FINISHED entries 几乎**不累积**：它们一出现就被淘汰。Buffer 中 100,000 条 entry 绝大多数是**非 FINISHED 状态**（RUNNING / PENDING 等），当新 entry 到来且没有 FINISHED entry 可淘汰时，只能淘汰最老的非 FINISHED entry。

### 4.5 恶性循环

```
                    ┌─────────────────────────────────┐
                    ▼                                 │
原始 entry（有 task_info）被淘汰                      │
        │                                            │
        ▼                                            │
Worker/Driver 后续上报 → 创建新 entry（无 task_info） │
        │                                            │
        ▼                                            │
新 entry 处于 RUNNING 状态 → priority 2               │
        │                                            │
        ▼                                            │
不会被优先淘汰 → 长期占据 buffer 空间                  │
        │                                            │
        ▼                                            │
buffer 更拥挤 → 有效 entry 的存活时间更短 ────────────┘
```

僵尸 entry（RUNNING，无 task_info，priority 2）与有效 entry（有 task_info，priority 2）在**同一淘汰队列**里，GC 策略无法区分它们。僵尸 entry 被 Worker 创建后 `push_front` 到 list 最前面（最新位置），和有效 entry 一样"年轻"，不会更快被淘汰。

### 4.6 各类型 Task 不可见原因总结

| 问题 | 直接原因 | 深层原因 |
|------|----------|----------|
| FINISHED task 不显示 | Entry 变为 FINISHED 后被优先淘汰(priority 0)，淘汰后消失 | Buffer 满时 FINISHED entry 存活时间极短 |
| RUNNING actor task 不显示 | 原始 entry 被淘汰后 Worker 重建无 task_info 的 entry | Actor task 淘汰优先级=1，比 Normal task 更容易被淘汰 |
| 大量 RUNNING normal task 不显示 | 同上，entry 淘汰后 Worker 重建无 task_info | 调度等待时间 > buffer 周转时间 (~71s) |
| 显示的 task 多为 PENDING 状态 | 它们是最近 ~71s 内 Driver 刚创建的 entry | Driver 最新 flush 的 entry 还没被淘汰 |

---

## 五、关键代码索引

### 5.1 Task Event 上报侧

| 文件 | 行号 | 功能 |
|------|------|------|
| `src/ray/core_worker/task_manager.cc` | 343-349 | Driver: `PENDING_ARGS_AVAIL` + `include_task_info=true`（唯一的 task_info 来源） |
| `src/ray/core_worker/task_manager.cc` | 1672-1683 | Driver: `MarkDependenciesResolved()` → `PENDING_NODE_ASSIGNMENT` |
| `src/ray/core_worker/task_manager.cc` | 1685-1700 | Driver: `MarkTaskWaitingForExecution()` → `SUBMITTED_TO_WORKER` |
| `src/ray/core_worker/task_manager.cc` | 1048-1054 | Driver: 收到结果 → `FINISHED` |
| `src/ray/core_worker/core_worker.cc` | 2863-2874 | Worker: 开始执行 → `RUNNING` (`include_task_info=false`) |
| `src/ray/core_worker/task_event_buffer.cc` | 420-449 | `RecordTaskStatusEventIfNeeded`: 核心上报入口，`include_task_info` 控制 task_spec 是否设入 |
| `src/ray/core_worker/task_event_buffer.cc` | 78-87 | `ToRpcTaskEvents`: 只有 `task_spec_` 非空才设 `task_info` |

### 5.2 GCS 存储侧

| 文件 | 行号 | 功能 |
|------|------|------|
| `src/ray/gcs/gcs_task_manager.cc` | 292-309 | `UpdateOrInitTaskEventLocator`: 存在→合并，不存在→创建新 |
| `src/ray/gcs/gcs_task_manager.cc` | 167-207 | `UpdateExistingTaskAttempt`: MergeFrom 合并 + 重计算优先级 |
| `src/ray/gcs/gcs_task_manager.cc` | 209-232 | `AddNewTaskEvent`: push_front 到对应优先级 list |
| `src/ray/gcs/gcs_task_manager.cc` | 332-349 | `EvictTaskEvent`: 从最低优先级 list 的 back() 淘汰 |
| `src/ray/gcs/gcs_task_manager.cc` | 379-388 | 插入后检查是否超限，触发淘汰 |
| `src/ray/gcs/gcs_task_manager.cc` | 492-495 | 查询过滤：`!has_task_info()` → 跳过 |
| `src/ray/gcs/gcs_task_manager.h` | 71-86 | `FinishedTaskActorTaskGcPolicy`: 优先级判定 |
| `src/ray/common/protobuf_utils.cc` | 342-349 | `IsTaskFinished`: 判断 entry 是否含 FINISHED 时间戳 |

### 5.3 查询侧

| 文件 | 行号 | 功能 |
|------|------|------|
| `src/ray/gcs/gcs_task_manager.cc` | 426-613 | `HandleGetTaskEvents`: gRPC 查询主入口 |
| `python/ray/dashboard/state_aggregator.py` | 306-340 | `list_tasks`: Python 侧调用 gRPC |
| `python/ray/util/state/state_manager.py` | 232-290 | `get_all_task_info`: 构建 gRPC 请求 |

---

## 六、影响范围

在以下条件同时满足时，此问题**必然出现**：

1. **高 task 吞吐**：平均 > 100,000 / (task 平均调度等待时间) tasks/s
2. **资源紧张**：GPU/CPU 满载，task 需要排队等待调度
3. **Buffer 未调大**：使用默认 `RAY_task_events_max_num_task_in_gcs=100000`
4. **长时间运行**：集群运行时间长，buffer 长期处于满载状态

---

## 七、解决方案

### 7.1 短期缓解：增大 Buffer

```bash
# 在 ray start 时通过 system_config 设置
ray start --head --system-config='{"task_events_max_num_task_in_gcs": 1000000}'
```

将 buffer 从 100,000 增大到 1,000,000，使周转时间从 ~71 秒增大到 ~700 秒，显著降低 entry 被淘汰的概率。

**代价**：GCS 内存占用增加（每条 entry 约 1-5 KB，100 万条约 1-5 GB）。

### 7.2 短期缓解：减少 flush 间隔

```bash
ray start --head --system-config='{"task_events_report_interval_ms": 2000}'
```

将 flush 间隔从 10 秒减为 2 秒，加快 Driver/Worker 的 task_info 合并速度。但不解决根本问题。

### 7.3 中期方案：修改 GC 策略优先淘汰无 task_info 的 entry

在淘汰策略中加入"无 task_info"作为更低优先级：

```cpp
// 建议修改
size_t GetTaskListPriority(const rpc::TaskEvents &task_events) const override {
    if (IsTaskFinished(task_events)) return 0;       // 最先淘汰
    if (!task_events.has_task_info()) return 1;      // 新增：无 task_info 的优先淘汰
    if (IsActorTask(task_events)) return 2;
    return 3;                                         // 有 task_info 的 Normal task 最后淘汰
}
```

这样僵尸 entry 会被优先淘汰，不再占据 buffer 空间。

### 7.4 长期方案：Driver 支持 task_info 重发

修改 Driver 端逻辑，在后续状态上报时，如果发现 GCS 返回"entry 不存在"或周期性携带 task_info：

- 方案 A：GCS 返回"entry 已淘汰"标记 → Driver 重新发送带 task_info 的事件
- 方案 B：Driver 在 `SUBMITTED_TO_WORKER` 或 `FINISHED` 状态也携带 `include_task_info=true`（代价是增加上报数据量）

### 7.5 NodeHead CPU 100% 问题

独立于 task event 问题，NodeHead 因轮询 300+ 节点导致 CPU 满载：

```bash
# 临时重启 Dashboard（不影响 Ray 集群运行）
kill 343  # Dashboard 主进程 PID，会被 supervisor 自动拉起

# 或只 kill NodeHead 子进程
kill 528  # NodeHead PID，Dashboard 会自动重启它
```

---

## 八、诊断命令速查

```bash
# 1. 检查实际 RUNNING task 数（通过 GPU 占用推算）
ray status | grep GPU

# 2. 检查 GCS buffer 状态（REST API，只能看到过滤后结果）
curl -s "http://localhost:8265/api/v0/tasks/summarize" | python3 -m json.tool

# 3. 检查 GCS task event 警告
grep -c "Max number of tasks event" /tmp/ray/session_*/logs/gcs_server.log

# 4. 检查 Driver task event 是否有丢弃
grep "num status task events dropped" /tmp/ray/session_*/logs/python-core-driver-*.log

# 5. 检查 NodeHead CPU
ps aux | grep NodeHead

# 6. 检查 task_events_max_num_task_in_gcs 配置
python3 -c "import ray; ray.init(address='auto'); print(ray._config.task_events_max_num_task_in_gcs())"

# 7. gRPC 直连 GCS 诊断僵尸 entry（唯一能看到 num_total_stored / num_filtered_on_gcs 的方式）
python3 -c "
import grpc
from ray.core.generated import gcs_service_pb2_grpc
from ray.core.generated.gcs_pb2 import GetTaskEventsRequest
channel = grpc.insecure_channel('localhost:6379')
stub = gcs_service_pb2_grpc.TaskInfoGcsServiceStub(channel)
reply = stub.GetTaskEvents(GetTaskEventsRequest(limit=10), timeout=10)
print(f'buffer总条数={reply.num_total_stored}, GCS过滤条数={reply.num_filtered_on_gcs}, 返回={len(reply.events_by_task)}')
if reply.num_total_stored > 0:
    print(f'无task_info占比: {reply.num_filtered_on_gcs/reply.num_total_stored*100:.1f}%')
"

# 8. 检查 ALIVE actor 数（验证 actor task 不可见问题）
curl -s "http://localhost:8265/api/v0/actors?limit=1000" | python3 -c "
import json,sys; d=json.load(sys.stdin)['data']['result']
print(f'ALIVE actors: {sum(1 for a in d if a.get(\"state\")==\"ALIVE\")}/{len(d)}')"

# 9. Worker 节点采样验证实际 RUNNING 数
# ssh <worker_ip> "ps aux | grep 'ray::' | grep -v grep | wc -l"
```

---

## 九、结论

Dashboard 显示 task 数量远低于实际值的根本原因是 **GCS Task Event Buffer 的淘汰-重建循环导致 task_info 永久丢失**：

1. Driver 对每个 task **只在首次提交时发送一次 task_info**
2. 在高吞吐场景下（~1,400 tasks/s），Buffer（100,000 条）的周转时间仅约 **71 秒**
3. 当 task 调度等待时间超过 71 秒时，原始 entry 被淘汰，task_info 丢失
4. 后续 Driver/Worker 的状态上报创建新 entry，但**不携带 task_info**
5. 查询时 `has_task_info()` 过滤将这些僵尸 entry 排除
6. 僵尸 entry 与有效 entry 在同一优先级队列中竞争，无法被优先淘汰，形成恶性循环

最终效果：100,000 条 entry 中 98,489 条是僵尸 entry（无 task_info），只有 ~1,511 条（最近 71 秒内创建的）能被 Dashboard 展示。

### 实际 RUNNING task 数 vs Dashboard 可见数

| 指标 | 数据 | 来源 |
|------|------|------|
| 实际 RUNNING task | ~900 | `ray status` GPU 占用数 |
| Dashboard 显示 RUNNING | ~1,279 | REST API（含部分 PENDING 被统计为 RUNNING） |
| GCS 中有 task_info 且状态=RUNNING | 63 | gRPC 直查 `GetTaskEvents` |
| GCS buffer 总条数 | 100,000 | gRPC `num_total_stored` |
| 无 task_info 被过滤 | 98,489 (98.5%) | gRPC `num_filtered_on_gcs` |
| 有 task_info 可见 | 1,511 (1.5%) | gRPC `len(events_by_task)` |

**注意：REST API（`/api/v0/tasks`、`ray list tasks`）不透传 `num_total_stored` 和 `num_filtered_on_gcs` 字段，只有通过 gRPC 直连 GCS 才能获取这些关键诊断数据。**
