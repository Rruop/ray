# Ray Data 任务卡死：Task Event 淘汰 + RequestWorkerLease gRPC 永久卡死

> 集群规模：26 节点 | Ray 版本：2.52.1 | 作业总 task 数：5200 万+
> 问题日期：2025-05-14

## 问题现象

1. Ray Data 流式作业 `ReadParquet->Filter->Map->Filter` 卡在 `9999/10000`（或 `64835964/64842448`），永远无法完成
2. Ray Data 日志显示有 1 个 task 正在处理（`Tasks: 1`），但 dashboard 上对应 task 状态不可见
3. Raylet 日志显示 `RequestWorkerLease - 2095790 total (1 active)`，1 个 lease 请求卡死 20+ 小时无回复
4. 集群无 infeasible、无 cancellation，看起来完全正常

**根因是两个问题叠加：**
1. **Task Event 淘汰**：GCS 和 Worker 的 task event 存储满后，`PENDING_NODE_ASSIGNMENT` 状态的 task 被淘汰，导致 dashboard 不可见（可观测性丢失）
2. **RequestWorkerLease gRPC 卡死**：节点死亡后 spillback 到死节点的 lease 请求无限超时，导致 task 永远无法被调度（调度死锁）

---

## 一、分析方法

### 1.1 排查思路

```
Ray Data 进度卡死 (9999/10000)
  → 检查 Ray Data 内部 task 状态
    → 有 1 个 task 在 self._data_tasks 中，认为已提交
      → 检查 Ray Core task 状态 (dashboard/ray list tasks)
        → 看不到该 task
          → 是 task 真的不存在，还是被淘汰了？
            → 检查 GCS task event 淘汰日志
              → 确认 task event 被淘汰（可观测性问题）
            → 但 task 确实也没在执行
              → 检查 Raylet lease 请求状态
                → 发现 1 active lease 请求卡死 20+ 小时
                  → 分析 RequestWorkerLease gRPC 为什么不返回
                    → 节点死亡 + spillback + 无限超时 + 恢复机制失效
```

### 1.2 关键排查工具

| 步骤 | 命令/方法 | 目的 |
|------|-----------|------|
| 确认作业状态 | `grep "Running:" job-driver-*.log` | 确认哪个 operator 卡住 |
| 检查 GCS 淘汰 | `grep "Max number of tasks event" gcs_server.out` | 确认 GCS 存储是否已满 |
| 检查 Worker buffer | `grep "Dropping task status events" worker-*.out` | 确认 Worker 侧是否溢出 |
| 检查 dropped count | `grep "dropped_task_attempts" gcs_server.out` | 淘汰了多少 task |
| 检查 Lease 状态 | `grep "RequestWorkerLease" raylet.out` | 看 active lease 请求数 |
| 检查节点死亡 | `grep "dead\|died\|lost plasma" raylet.out` | 是否有节点故障 |
| 检查 spillback | `grep "Redirect lease" raylet.out` | 是否有 spillback 到死节点 |

---

## 二、分析过程

### 2.1 第一步：确认作业卡死位置

连接到运行作业的 Worker 节点 `kml-task-661218-record-15739495-prod-worker-0-8qhgk`，查看 Ray Data 日志：

```
Running: 64835964/64842448 CPU tasks, 0 GPU tasks
  ReadParquet->Filter->Map->Filter: 1 active, 0 queued [4484 finished] Tasks: 1
```

- Operator 有 1 个 task 处于"已提交但未完成"状态
- Task 从 00:44:45 开始就一直卡住（20+ 小时）

### 2.2 第二步：检查 Task 为什么在 Dashboard 不可见

```
[GCS] Max number of tasks event (100000) allowed is reached.
      Old task events will be overwritten.
[GCS] Pub/Sub channel: RAY_TASK_INFO_CHANNEL has 23740 messages dropped.
[GCS] Evict extra dropped task attempts(1013938 > 1000000) tracked in GCS for job=...
```

- GCS 只能存储 100,000 个 task event，但作业执行了 5200 万+ task
- Pub/Sub 通道也有 23,740 条消息被丢弃
- 已淘汰的 task attempt 数量达到 1,013,938，超过了跟踪上限 1,000,000

### 2.3 第三步：确认 task 是卡在调度还是真的丢了

```
[Raylet] RequestWorkerLease - 2095790 total (1 active)
         num_infeasible_scheduling_classes: 0
         num_cancelled_leases: 0
         Cluster size: 26
```

- **1 个 active 的 lease 请求**卡死不返回 = task 确实卡在调度阶段
- 不是 infeasible，不是被 cancel，就是 gRPC 没有回复

### 2.4 第四步：定位触发时间和原因

```
[2025-05-14 16:03:18] Worker a21f2c55... died or disconnected.
[2025-05-14 16:03:18] Lost plasma object for task <task_id>.
[2025-05-14 16:03:18] Resubmitting task <task_id> (max_retries=-1).
```

节点死亡 → plasma object 丢失 → task 被 resubmit → 重新请求 lease → **lease 请求卡死**

---

## 三、日志现场

### 3.1 GCS Task Event 存储溢出

```
[2025-05-14 16:03:18] Max number of tasks event (100000) allowed is reached.
Old task events will be overwritten. Set `RAY_task_events_max_num_task_in_gcs`
to a higher value to store more.
```

### 3.2 Worker 侧 Buffer 溢出

```
Dropping task status events for task: <task_id>, set a higher value for
RAY_task_events_max_num_status_events_buffer_on_worker(100000) to avoid this.
```

### 3.3 Pub/Sub 消息丢失

```
Pub/Sub channel: RAY_TASK_INFO_CHANNEL has 23740 messages dropped.
```

### 3.4 Dropped Task Attempts 超限

```
Evict extra dropped task attempts(1013938 > 1000000) tracked in GCS for job=<job_hex>.
Setting the RAY_task_events_max_dropped_task_attempts_tracked_per_job_in_gcs to a higher value.
```

### 3.5 Raylet Lease 请求卡死

```
RequestWorkerLease - 2095790 total (1 active)
num_infeasible_scheduling_classes: 0
num_cancelled_leases: 0
Cluster size: 26
```

### 3.6 节点死亡

```
[2025-05-14 16:03:18] Worker a21f2c55... died or disconnected.
Lost plasma object for task <task_id>.
```

---

## 四、排查命令脚本

### 4.1 综合诊断脚本

```bash
#!/bin/bash
# ray_stuck_task_diagnosis.sh
# 用法: bash ray_stuck_task_diagnosis.sh [ray_session_dir]

RAY_DIR="${1:-/tmp/ray/session_latest}"

echo "========================================="
echo "Ray Task 卡死问题诊断"
echo "========================================="

echo ""
echo "=== 1. GCS Task Event 淘汰状态 ==="
echo "--- 淘汰警告次数 ---"
grep -c "Max number of tasks event" "$RAY_DIR/logs/gcs_server.out" 2>/dev/null || echo "0"
echo "--- 最近的淘汰日志 ---"
grep "Evict extra dropped task attempts" "$RAY_DIR/logs/gcs_server.out" 2>/dev/null | tail -3
echo "--- Pub/Sub 消息丢失 ---"
grep "messages dropped" "$RAY_DIR/logs/gcs_server.out" 2>/dev/null | tail -3

echo ""
echo "=== 2. Worker 侧 Buffer 溢出 ==="
echo "--- 溢出告警次数 ---"
grep -c "Dropping task status events" "$RAY_DIR/logs/"worker-*.out 2>/dev/null || echo "0"
echo "--- GCS flush 背压 ---"
grep -c "hasn't replied to the previous flush" "$RAY_DIR/logs/"worker-*.out 2>/dev/null || echo "0"

echo ""
echo "=== 3. Lease 请求状态 ==="
grep "RequestWorkerLease.*active" "$RAY_DIR/logs/raylet.out" 2>/dev/null | tail -5

echo ""
echo "=== 4. 调度失败 ==="
echo "--- Infeasible ---"
grep "num_infeasible" "$RAY_DIR/logs/raylet.out" 2>/dev/null | tail -3
echo "--- Cancelled ---"
grep "num_cancelled_leases" "$RAY_DIR/logs/raylet.out" 2>/dev/null | tail -3

echo ""
echo "=== 5. 节点死亡 ==="
grep -i "node.*dead\|node.*died\|lost plasma" "$RAY_DIR/logs/raylet.out" 2>/dev/null | tail -10

echo ""
echo "=== 6. Spillback ==="
grep "Redirect lease\|Spilling lease\|retry_at_raylet" "$RAY_DIR/logs/raylet.out" 2>/dev/null | tail -10

echo ""
echo "=== 7. gRPC 连接问题 ==="
grep "has been unavailable\|Disconnecting raylet\|Failed to get node info" "$RAY_DIR/logs/"*.out 2>/dev/null | tail -10

echo ""
echo "=== 8. Ray Data 作业进度 ==="
grep "Running:" "$RAY_DIR/logs/"job-driver-*.log 2>/dev/null | tail -5

echo ""
echo "========================================="
echo "诊断完成"
echo "========================================="
```

### 4.2 Python 诊断脚本

```python
#!/usr/bin/env python3
"""检查 Ray 集群中卡死的 task 和 lease 请求"""
import ray
from ray.util.state import list_tasks, list_nodes

ray.init()

# 1. 检查 PENDING_NODE_ASSIGNMENT 状态的 task
print("=== PENDING_NODE_ASSIGNMENT Tasks ===")
pending_tasks = list_tasks(
    filters=[("state", "=", "PENDING_NODE_ASSIGNMENT")],
    limit=50
)
for t in pending_tasks:
    print(f"  {t.task_id} name={t.name} type={t.type}")
print(f"Total: {len(pending_tasks)} tasks")

# 2. 检查节点状态
print("\n=== Node Status ===")
nodes = list_nodes()
alive = sum(1 for n in nodes if n.state == "ALIVE")
dead = sum(1 for n in nodes if n.state == "DEAD")
print(f"Alive: {alive}, Dead: {dead}")

# 3. 检查被 drop 的 task 数量
print("\n=== Dropped Task Events ===")
try:
    from ray._private.gcs_utils import GcsClient
    gcs_client = GcsClient(address=ray.get_runtime_context().gcs_address)
    # 通过 internal API 获取 task event stats
    print("(Check GCS logs for 'dropped_task_attempts' count)")
except Exception as e:
    print(f"Cannot query: {e}")
```

### 4.3 单条命令快速排查

```bash
# 快速确认是否有 task event 淘汰
grep -c "Max number of tasks event" /tmp/ray/session_latest/logs/gcs_server.out

# 快速确认是否有 lease 卡死
grep "active" /tmp/ray/session_latest/logs/raylet.out | grep "RequestWorkerLease" | tail -1

# 快速确认是否有节点死亡
grep -c "lost plasma" /tmp/ray/session_latest/logs/raylet.out

# 检查 dropped task attempts 数量
grep "dropped_task_attempts\|Dropped task attempts" /tmp/ray/session_latest/logs/gcs_server.out | tail -3

# 查看 gRPC channel 状态问题
grep "unavailable for more than\|Retrying.*schedule lease" /tmp/ray/session_latest/logs/*.out | tail -10
```

---

## 五、相关代码逻辑

### 5.1 问题一：Task Event 淘汰机制

#### Worker 侧 Buffer 淘汰（纯 FIFO，不区分 task 状态）

**文件**: `src/ray/core_worker/task_event_buffer.cc:1009-1054`

```cpp
void TaskEventBufferImpl::AddTaskStatusEvent(std::unique_ptr<TaskEvent> status_event) {
  absl::MutexLock lock(&mutex_);
  // ...
  if (dropped_task_attempts_unreported_.count(
          status_event_shared_ptr->GetTaskAttempt()) != 0u) {
    // 这个 task attempt 之前已被淘汰，后续所有事件直接丢弃
    return;
  }

  if (status_events_.full()) {
    // Buffer 满了，FIFO 淘汰最前面（最老）的事件
    const auto &to_evict = status_events_.front();
    // 标记整个 task attempt 为 dropped
    dropped_task_attempts_unreported_.insert(to_evict->GetTaskAttempt());
  }
  status_events_.push_back(status_event_shared_ptr);
}
```

**问题**：使用 `boost::circular_buffer`（容量 100,000），满时淘汰 `front()`（最老事件）。完全不考虑 task 是否仍在执行。长期卡在 `PENDING_NODE_ASSIGNMENT` 的 task，其初始事件是 buffer 中最老的，最先被淘汰。

**配置**: `RAY_task_events_max_num_status_events_buffer_on_worker`，默认 100,000

#### GCS 侧 Storage 淘汰（按优先级，但最终也会淘汰活跃 task）

**文件**: `src/ray/gcs/gcs_task_manager.h:71-86`

```cpp
class FinishedTaskActorTaskGcPolicy : public TaskEventsGcPolicyInterface {
  size_t MaxPriority() const override { return 3; }
  size_t GetTaskListPriority(const rpc::TaskEvents &task_events) const override {
    if (IsTaskFinished(task_events)) return 0;  // 已完成 → 最先淘汰
    if (IsActorTask(task_events))    return 1;  // Actor task → 次之
    return 2;                                    // 其他(含 PENDING) → 最后淘汰
  }
};
```

**文件**: `src/ray/gcs/gcs_task_manager.cc:332-349`

```cpp
void GcsTaskManagerStorage::EvictTaskEvent() {
  size_t list_index = 0;
  for (; list_index < gc_policy_->MaxPriority(); ++list_index) {
    // 找第一个非空的优先级列表
    if (!task_events_list_[list_index].empty()) break;
  }
  // 从该列表的 back（最老）淘汰
  const auto &to_evict = task_events_list_[list_index].back();
  RemoveTaskAttempt(loc_iter->second);
}
```

**GCS 淘汰顺序**：
| 优先级 | 状态 | 淘汰顺序 |
|--------|------|----------|
| 0 | FINISHED（已完成） | 最先被淘汰 |
| 1 | Actor task（未完成） | 其次 |
| 2 | 其他（含 PENDING_NODE_ASSIGNMENT） | 最后 |

**为什么 PENDING_NODE_ASSIGNMENT 也会被淘汰**：
1. GCS 存储上限是 100,000 个 task event
2. 作业执行了 5200 万+ task，每个 task 至少产生 1 个 event
3. 已完成 task（priority 0）很快全部被淘汰完
4. 之后只剩 priority 1 和 2，PENDING 状态的 task 也不得不被淘汰
5. 在 priority 2 内部，是按插入顺序（FIFO）淘汰的 — 越早卡住的 task 越早被淘汰

#### dropped_task_attempts 集合本身的 GC

**文件**: `src/ray/gcs/gcs_task_manager.cc:773-809`

```cpp
void JobTaskSummary::GcOldDroppedTaskAttempts(const JobID &job_id) {
  // dropped_task_attempts_ 集合超过 100 万时，移除最老的记录
  if (dropped_task_attempts_.size() > max_tracked) {
    num_to_evict = dropped_task_attempts_.size() - max_tracked;
    // Evict ignoring timestamp.
    dropped_task_attempts_.erase(dropped_task_attempts_.begin(),
                                 std::next(dropped_task_attempts_.begin(), num_to_evict));
  }
}
```

**配置**: `RAY_task_events_max_dropped_task_attempts_tracked_per_job_in_gcs`，默认 1,000,000

当 dropped 集合被 GC 后，如果 worker 再次上报被 GC 的 task attempt 的状态，GCS 会当作新 task 接受 — 但信息不完整。

---

### 5.2 问题二：RequestWorkerLease gRPC 卡死

#### 完整调用链

```
Task Resubmit (object loss)
  → NormalTaskSubmitter::SubmitTask
    → ResolveDependencies callback
      → RequestNewWorkerIfNeeded(scheduling_key, nullptr)
        → lease_policy_->GetBestNodeForLease → 选本地 Raylet
        → raylet_client->RequestWorkerLease(grant_or_reject=false)
          → 本地 Raylet ClusterLeaseManager::ScheduleAndGrantLeases
            → GetBestSchedulableNode → 选到已死节点（资源视图有延迟）
            → ScheduleOnNode(dead_node_id, work)
              → reply: retry_at_raylet_address = dead_node
                → 回调: RequestNewWorkerIfNeeded(scheduling_key, &dead_node_addr)
                  → is_spillback = true
                  → raylet_client->RequestWorkerLease(grant_or_reject=true)
                    → INVOKE_RETRYABLE_RPC_CALL(..., method_timeout_ms=-1)
                      → gRPC 失败(retryable error)
                        → RetryableGrpcClient::Retry(timeout=InfiniteFuture)
                          → 永久卡死
```

#### Lease 请求超时设置

**文件**: `src/ray/raylet_rpc_client/raylet_client.cc:53-71`

```cpp
void RayletClient::RequestWorkerLease(
    const rpc::LeaseSpec &lease_spec,
    bool grant_or_reject,
    const rpc::ClientCallback<rpc::RequestWorkerLeaseReply> &callback,
    const int64_t backlog_size,
    const bool is_selected_based_on_locality) {
  rpc::RequestWorkerLeaseRequest request;
  request.mutable_lease_spec()->CopyFrom(lease_spec);
  request.set_grant_or_reject(grant_or_reject);
  request.set_backlog_size(backlog_size);
  request.set_is_selected_based_on_locality(is_selected_based_on_locality);
  INVOKE_RETRYABLE_RPC_CALL(retryable_grpc_client_,
                            NodeManagerService,
                            RequestWorkerLease,
                            request,
                            callback,
                            grpc_client_,
                            /*method_timeout_ms*/ -1);  // ← 无限超时
}
```

#### 重试队列中的无限等待

**文件**: `src/ray/rpc/retryable_grpc_client.cc:131-173`

```cpp
void RetryableGrpcClient::Retry(std::shared_ptr<RetryableGrpcRequest> request) {
  const auto timeout = request->GetTimeoutMs() == -1
                           ? absl::InfiniteFuture()    // 永远不超时
                           : now + absl::Milliseconds(request->GetTimeoutMs());
  pending_requests_.emplace(timeout, std::move(request));
  if (!server_unavailable_timeout_time_.has_value()) {
    server_unavailable_timeout_time_ =
        now + absl::Seconds(server_reconnect_timeout_base_seconds_);
    SetupCheckTimer();
  }
}
```

#### Channel 状态检查与恢复机制

**文件**: `src/ray/rpc/retryable_grpc_client.cc:52-129`

```cpp
void RetryableGrpcClient::CheckChannelStatus(bool reset_timer) {
  // 清理超时的 pending requests（但 InfiniteFuture 永远不超时）
  while (!pending_requests_.empty()) {
    auto iter = pending_requests_.begin();
    if (iter->first > now) break;  // InfiniteFuture > now 永远成立
    // 不会走到这里
  }

  auto status = channel_->GetState(false);
  switch (status) {
  case GRPC_CHANNEL_TRANSIENT_FAILURE:
  case GRPC_CHANNEL_CONNECTING: {
    if (server_unavailable_timeout_time_ < now) {
      server_unavailable_timeout_callback_();  // 触发节点死亡检查
      attempt_number_++;
      server_unavailable_timeout_time_ = now + absl::Seconds(backoff);
    }
    SetupCheckTimer();
    break;
  }
  case GRPC_CHANNEL_READY:
  case GRPC_CHANNEL_IDLE: {
    // Channel 看起来正常 → 重发所有 pending requests
    while (!pending_requests_.empty()) {
      pending_requests_.begin()->second->CallMethod();  // 重发
      pending_requests_.erase(pending_requests_.begin());
    }
    attempt_number_ = 0;  // 重置计数！callback 永远不会被调用
    break;
  }
  }
}
```

**三种失效场景**：

| 场景 | Channel 状态 | 后果 |
|------|-------------|------|
| A | TRANSIENT_FAILURE | 触发 unavailable callback，但 GCS 查询可能失败不处理 |
| B | READY/IDLE | 无限重发 → 再失败 → 再 Retry → 循环。且 attempt_number_ 被重置，callback 永远不触发 |
| C | callback 成功 Disconnect | 只移除 pool 引用，不 fail pending requests |

#### 节点死亡恢复 callback

**文件**: `src/ray/raylet_rpc_client/raylet_client_pool.cc:24-79`

```cpp
std::function<void()> RayletClientPool::GetDefaultUnavailableTimeoutCallback(...) {
  return [addr, gcs_client, raylet_client_pool]() {
    gcs_client->Nodes().AsyncGetAllNodeAddressAndLiveness(
        [...](const Status &status, ...) {
          if (!status.ok()) {
            RAY_LOG(INFO) << "Failed to get node info from GCS";
            return;  // ← GCS 查询失败，什么都不做！
          }
          if (nodes[0].state() != rpc::GcsNodeInfo::ALIVE) {
            raylet_client_pool->Disconnect(node_id);  // 移除但不 fail pending
          }
        }, -1, {node_id});
  };
}
```

#### Disconnect 不 fail pending requests

**文件**: `src/ray/raylet_rpc_client/raylet_client_pool.cc:100-107`

```cpp
void RayletClientPool::Disconnect(ray::NodeID id) {
  absl::MutexLock lock(&mu_);
  auto it = client_map_.find(id);
  if (it == client_map_.end()) return;
  client_map_.erase(it);  // 只删引用，不析构 RetryableGrpcClient
}
```

只有 `~RetryableGrpcClient` 析构时才会 fail pending requests：
```cpp
RetryableGrpcClient::~RetryableGrpcClient() {
  while (!pending_requests_.empty()) {
    request->Fail(Status::Disconnected("GRPC client is shut down."));
  }
}
```

但只要有闭包持有 shared_ptr，就不会析构。

#### 无重试上限的 TODO

**文件**: `src/ray/core_worker/task_submission/normal_task_submitter.cc:437-450`

```cpp
} else if (NodeID::FromBinary(raylet_address.node_id()) != local_node_id_) {
  // A lease request to a remote raylet failed. Retry locally if the lease is
  // still needed.
  // TODO(swang): Fail after some number of retries?
  RAY_LOG_EVERY_MS(INFO, 30 * 1000) << "Retrying attempt to schedule lease...";
  RequestNewWorkerIfNeeded(scheduling_key);
}
```

---

### 5.3 核心文件索引

| 功能 | 文件 | 关键行 |
|------|------|--------|
| Worker task event buffer (FIFO淘汰) | `src/ray/core_worker/task_event_buffer.cc` | 1009-1054 |
| GCS task event 存储与淘汰 | `src/ray/gcs/gcs_task_manager.cc` | 332-389 |
| GC 优先级策略 | `src/ray/gcs/gcs_task_manager.h` | 71-86 |
| dropped_task_attempts GC | `src/ray/gcs/gcs_task_manager.cc` | 773-809 |
| Lease 请求发送 | `src/ray/core_worker/task_submission/normal_task_submitter.cc` | 274-499 |
| Lease 请求无限超时 | `src/ray/raylet_rpc_client/raylet_client.cc` | 53-71 |
| gRPC 重试队列 (InfiniteFuture) | `src/ray/rpc/retryable_grpc_client.cc` | 131-173 |
| Channel 状态检查 | `src/ray/rpc/retryable_grpc_client.cc` | 52-129 |
| Spillback 决策 | `src/ray/raylet/scheduling/cluster_lease_manager.cc` | 422-461 |
| 节点死亡恢复 callback | `src/ray/raylet_rpc_client/raylet_client_pool.cc` | 24-79 |
| Disconnect (不 fail pending) | `src/ray/raylet_rpc_client/raylet_client_pool.cc` | 100-107 |
| 远程 lease 失败无重试上限 | `src/ray/core_worker/task_submission/normal_task_submitter.cc` | 437-450 |
| Task 状态上报到 buffer | `src/ray/core_worker/task_manager.cc` | 1702-1724 |
| PENDING_NODE_ASSIGNMENT 设置 | `src/ray/core_worker/task_manager.cc` | 1672-1683 |

---

### 5.4 关键配置参数

| 参数 | 默认值 | 含义 |
|------|--------|------|
| `RAY_task_events_max_num_task_in_gcs` | 100,000 | GCS 最多存储的 task event 数量 |
| `task_events_max_num_status_events_buffer_on_worker` | 100,000 | Worker 侧 task event buffer 容量 |
| `task_events_max_dropped_task_attempts_tracked_per_job_in_gcs` | 1,000,000 | GCS 记录的已淘汰 task attempt 上限 |
| `task_events_report_interval_ms` | 1000 | Worker 向 GCS 上报间隔(ms) |
| `task_events_send_batch_size` | 10,000 | 每次 flush 最大事件数 |
| `raylet_rpc_server_reconnect_timeout_base_s` | 1 | gRPC 重连超时基准(s) |
| `raylet_rpc_server_reconnect_timeout_max_s` | 60 | gRPC 重连超时上限(s) |
| `grpc_client_check_connection_status_interval_milliseconds` | (见配置) | Channel 检查间隔(ms) |

---

## 六、修复办法

### 6.1 临时规避方案（不改代码，立即可用）

#### 方案A：增大 Task Event 存储容量 + 恢复 report_interval（解决可观测性）

```python
ray.init(
    _system_config={
        "task_events_max_num_task_in_gcs": 1000000,
        "task_events_max_num_status_events_buffer_on_worker": 500000,
        "task_events_max_dropped_task_attempts_tracked_per_job_in_gcs": 5000000,
        "task_events_report_interval_ms": 1000,  # 从 10000 改回默认值
    }
)
```

或环境变量：
```bash
export RAY_task_events_max_num_task_in_gcs=1000000
export RAY_task_events_max_num_status_events_buffer_on_worker=500000
export RAY_task_events_report_interval_ms=1000
```

**注意**: 只解决 dashboard 可见性问题，**不解决 lease 卡死**。

#### 方案B：设置 task 级别超时兜底

```python
# 在 Ray Data 作业层面设置 task 超时和重试
@ray.remote(max_retries=3, retry_exceptions=True)
def my_task(...):
    ...

# 或 Ray Data 作业层面
ray.data.read_parquet(...).map(
    fn,
    ray_remote_args={"max_retries": 3}
)
```

**效果**: task 层面有重试机制，但无法解决调度层面的永久卡死。

#### 方案C：监控 + 人工干预

```bash
#!/bin/bash
# lease_stuck_monitor.sh — 定期检查是否有 lease 卡死
# 用法: bash lease_stuck_monitor.sh [ray_session_dir] [check_interval_seconds]

RAY_DIR="${1:-/tmp/ray/session_latest}"
INTERVAL="${2:-60}"

while true; do
    # 检查 active lease 请求
    ACTIVE=$(grep "RequestWorkerLease" "$RAY_DIR/logs/raylet.out" 2>/dev/null | \
             tail -1 | grep -oP '\d+ active' | grep -oP '^\d+')
    TOTAL=$(grep "RequestWorkerLease" "$RAY_DIR/logs/raylet.out" 2>/dev/null | \
            tail -1 | grep -oP '\d+ total' | grep -oP '^\d+')

    if [ -n "$ACTIVE" ] && [ "$ACTIVE" -gt 0 ]; then
        echo "[$(date)] WARNING: $ACTIVE active lease requests (total: $TOTAL)"
        echo "  → 如果 active 数量持续不变，可能存在 lease 卡死"
        echo "  → 考虑重启 driver 或 kill 卡死 task"
    else
        echo "[$(date)] OK: No stuck lease requests"
    fi

    sleep "$INTERVAL"
done
```

#### 方案D：为 RequestWorkerLease 添加超时（需改代码，最小改动最大收益）

修改 `src/ray/raylet_rpc_client/raylet_client.cc:70`：

```cpp
// Before:
/*method_timeout_ms*/ -1

// After (5 分钟超时):
/*method_timeout_ms*/ 300000
```

超时后回调会被调用（status 为 TimedOut），进入 `normal_task_submitter.cc:437-450` 的远程失败处理，重新在本地调度。**这是最小改动最大收益的修复。**

超时触发后的完整调用链：

```
RequestWorkerLease gRPC 超时 (300s)
  → RetryableGrpcClient::CheckChannelStatus 检测到 timeout > now
    → request->Fail(Status::TimedOut(...))
      → NormalTaskSubmitter lease 回调被调用 (status = TimedOut)
        → 进入 else if (NodeID != local_node_id_) 分支
          → "Retrying attempt to schedule lease..."
          → RequestNewWorkerIfNeeded(scheduling_key)  // 重新本地调度
            → 选择新节点重试
```

### 6.2 根本修复方案

#### Fix 1: Worker 侧 Buffer 应优先淘汰已完成 task

**问题**: Worker buffer 纯 FIFO，不区分 task 状态。长期卡在 PENDING_NODE_ASSIGNMENT 的 task 事件最老，最先被淘汰。

**当前代码** (`src/ray/core_worker/task_event_buffer.cc:1009-1054`):
```cpp
void TaskEventBufferImpl::AddTaskStatusEvent(std::unique_ptr<TaskEvent> status_event) {
  absl::MutexLock lock(&mutex_);
  if (status_events_.full()) {
    // Buffer 满了，FIFO 淘汰最前面（最老）的事件 — 不考虑 task 状态
    const auto &to_evict = status_events_.front();
    dropped_task_attempts_unreported_.insert(to_evict->GetTaskAttempt());
  }
  status_events_.push_back(status_event_shared_ptr);
}
```

**修复方案**: 参考 GCS 的优先级策略，Worker buffer 也应该先淘汰已完成的 task event。

```cpp
// 改进方向：维护两个 buffer 或使用优先级淘汰
// 方案 A: 双 buffer
//   completed_events_buffer (已完成 task 的事件，优先淘汰)
//   active_events_buffer (活跃 task 的事件，最后淘汰)
//
// 方案 B: 单 buffer + 淘汰时扫描
//   淘汰时优先找 FINISHED/FAILED 状态的 task 的最老事件
//   只有找不到时才淘汰活跃 task 的事件
//
// 方案 C: 维护 task_id → latest_status 的索引
//   淘汰时查索引，只淘汰已终止 task 的事件
```

#### Fix 2: RequestWorkerLease 必须有超时

**问题**: `method_timeout_ms = -1` 导致 gRPC 永远不超时。

**当前代码** (`src/ray/raylet_rpc_client/raylet_client.cc:53-71`):
```cpp
void RayletClient::RequestWorkerLease(...) {
  INVOKE_RETRYABLE_RPC_CALL(retryable_grpc_client_,
                            NodeManagerService,
                            RequestWorkerLease,
                            request,
                            callback,
                            grpc_client_,
                            /*method_timeout_ms*/ -1);  // ← 无限超时
}
```

**修复方案**: 设置合理超时（如 5-10 分钟）。超时后回调被调用，触发本地重试。

```cpp
// 修复：添加可配置的超时
void RayletClient::RequestWorkerLease(...) {
  INVOKE_RETRYABLE_RPC_CALL(retryable_grpc_client_,
                            NodeManagerService,
                            RequestWorkerLease,
                            request,
                            callback,
                            grpc_client_,
                            /*method_timeout_ms*/
                            RayConfig::instance().worker_lease_timeout_ms());
}

// ray_config_def.h 中添加配置:
// RAY_CONFIG(int64_t, worker_lease_timeout_ms, 300000)  // 默认 5 分钟
```

#### Fix 3: RetryableGrpcClient READY/IDLE 状态限制重试次数

**问题**: Channel READY/IDLE 时无限重试且重置 attempt_number_。

**当前代码** (`src/ray/rpc/retryable_grpc_client.cc:113-123`):
```cpp
case GRPC_CHANNEL_READY:
case GRPC_CHANNEL_IDLE: {
  server_unavailable_timeout_time_ = std::nullopt;
  while (!pending_requests_.empty()) {
    pending_requests_.begin()->second->CallMethod();  // 重发
    pending_requests_.erase(pending_requests_.begin());
  }
  pending_requests_bytes_ = 0;
  attempt_number_ = 0;  // ← 完全重置！永远不会因超过阈值而触发 callback
  break;
}
```

**失效场景**: 当 channel 处于 READY/IDLE 但目标节点已死时，请求会被重发 → 失败 → 重新入队 → channel 仍然 READY → 再次重发 → `attempt_number_` 被重置为 0 → 无限循环。

**修复方案**: 添加总重试次数计数器，即使 channel 恢复也不完全重置。

```cpp
case GRPC_CHANNEL_READY:
case GRPC_CHANNEL_IDLE: {
  server_unavailable_timeout_time_ = std::nullopt;
  // 改进：添加独立的总重试计数，不随 channel 状态重置
  total_retry_count_++;
  if (total_retry_count_ > max_total_retries_) {
    RAY_LOG(WARNING) << "Max total retries (" << max_total_retries_
                     << ") exceeded for " << server_name_
                     << ". Failing all pending requests.";
    while (!pending_requests_.empty()) {
      pending_requests_.begin()->second->Fail(
          Status::TimedOut("Max retries exceeded"));
      pending_requests_bytes_ -= pending_requests_.begin()->second->GetRequestBytes();
      pending_requests_.erase(pending_requests_.begin());
    }
    pending_requests_bytes_ = 0;
    break;
  }
  // 正常重发逻辑
  while (!pending_requests_.empty()) {
    pending_requests_.begin()->second->CallMethod();
    pending_requests_.erase(pending_requests_.begin());
  }
  pending_requests_bytes_ = 0;
  attempt_number_ = 0;
  break;
}
```

#### Fix 4: Disconnect 应主动 fail pending requests

**问题**: `Disconnect` 只从 pool 移除引用，不 fail `RetryableGrpcClient` 中的 pending requests。

**当前代码** (`src/ray/raylet_rpc_client/raylet_client_pool.cc:100-107`):
```cpp
void RayletClientPool::Disconnect(ray::NodeID id) {
  absl::MutexLock lock(&mu_);
  auto it = client_map_.find(id);
  if (it == client_map_.end()) return;
  client_map_.erase(it);  // ← 只删引用，不析构 RetryableGrpcClient
  // RetryableGrpcClient 的 pending_requests_ 中仍有卡死的请求
  // 只有析构时才会 fail pending，但闭包持有 shared_ptr 阻止析构
}
```

**析构时才会 fail pending** (`src/ray/rpc/retryable_grpc_client.cc:23-38`):
```cpp
RetryableGrpcClient::~RetryableGrpcClient() {
  timer_.cancel();
  while (!pending_requests_.empty()) {
    auto iter = pending_requests_.begin();
    io_context_.post([request = std::move(iter->second)]() {
      request->Fail(Status::Disconnected("GRPC client is shut down."));
    }, "~RetryableGrpcClient");
    pending_requests_.erase(iter);
  }
}
```

**修复方案**: `Disconnect` 应主动触发 `RetryableGrpcClient` shutdown。

```cpp
// 方案 A: 在 RayletClientInterface 中添加 Shutdown 接口
void RayletClientPool::Disconnect(ray::NodeID id) {
  absl::MutexLock lock(&mu_);
  auto it = client_map_.find(id);
  if (it == client_map_.end()) return;
  auto client = std::move(it->second);
  client_map_.erase(it);
  // 新增: 主动 fail 所有 pending requests
  client->Shutdown();
}

// 方案 B: 在 RayletClient 中暴露 RetryableGrpcClient 的 shutdown
void RayletClient::Shutdown() {
  retryable_grpc_client_->Shutdown();
}

// RetryableGrpcClient 添加 Shutdown 方法
void RetryableGrpcClient::Shutdown() {
  timer_.cancel();
  while (!pending_requests_.empty()) {
    auto iter = pending_requests_.begin();
    io_context_.post([request = std::move(iter->second)]() {
      request->Fail(Status::Disconnected("Node is dead, failing pending requests."));
    }, "RetryableGrpcClient::Shutdown");
    pending_requests_.erase(iter);
  }
  pending_requests_bytes_ = 0;
}
```

#### Fix 5: Spillback 前验证目标节点存活

**问题**: `ScheduleOnNode` 中不验证 spillback 目标节点是否存活。

**当前代码** (`src/ray/raylet/scheduling/cluster_lease_manager.cc:422-461`):
```cpp
void ClusterLeaseManager::ScheduleOnNode(const NodeID &spillback_to,
                                         const std::shared_ptr<internal::Work> &work) {
  if (spillback_to == self_node_id_) {
    local_lease_manager_.QueueAndScheduleLease(work);
    return;
  }
  // ... 不检查 spillback_to 节点是否存活 ...
  auto node_info = get_node_info_(spillback_to);
  RAY_CHECK(node_info.has_value());  // 只检查 node_info 存在，不检查 liveness
  // 直接返回 retry_at_raylet_address 给 CoreWorker
}
```

**修复方案**: 在 `GetBestSchedulableNode` 返回后，额外检查节点的 liveness 状态。

```cpp
void ClusterLeaseManager::ScheduleOnNode(const NodeID &spillback_to,
                                         const std::shared_ptr<internal::Work> &work) {
  if (spillback_to == self_node_id_) {
    local_lease_manager_.QueueAndScheduleLease(work);
    return;
  }

  // 新增: 验证目标节点是否存活
  auto node_info = get_node_info_(spillback_to);
  if (!node_info.has_value()) {
    RAY_LOG(WARNING) << "Spillback target node " << spillback_to
                     << " is no longer available. Retrying scheduling.";
    // 重新放回调度队列
    leases_to_schedule_[work->lease_.GetLeaseSpecification().GetSchedulingClass()]
        .emplace_back(work);
    return;
  }
  // ... 原有逻辑 ...
}
```

#### Fix 6: NormalTaskSubmitter 需要 spillback 重试上限

**问题**: 远程 lease 失败无限重试（代码有 TODO 注释承认）。

**当前代码** (`src/ray/core_worker/task_submission/normal_task_submitter.cc:437-450`):
```cpp
} else if (NodeID::FromBinary(raylet_address.node_id()) != local_node_id_) {
  // A lease request to a remote raylet failed. Retry locally if the lease is
  // still needed.
  // TODO(swang): Fail after some number of retries?   ← 承认需要重试上限
  RAY_LOG_EVERY_MS(INFO, 30 * 1000)
      << "Retrying attempt to schedule lease...";
  RequestNewWorkerIfNeeded(scheduling_key);  // ← 无限重试
}
```

**修复方案**: 添加重试计数，超过 N 次后 fail task 或强制回退到本地调度。

```cpp
} else if (NodeID::FromBinary(raylet_address.node_id()) != local_node_id_) {
  // 新增: 记录重试次数
  auto &sched_entry = scheduling_key_entries_[scheduling_key];
  sched_entry.remote_lease_retry_count++;

  if (sched_entry.remote_lease_retry_count >
      RayConfig::instance().max_remote_lease_retries()) {
    RAY_LOG(WARNING) << "Remote lease retry limit exceeded ("
                     << sched_entry.remote_lease_retry_count
                     << "). Failing pending tasks.";
    error_type = rpc::ErrorType::TASK_UNSCHEDULABLE_ERROR;
    error_info.set_error_message(
        "Task scheduling failed: remote lease request exceeded max retries.");
    tasks_to_fail = std::move(sched_entry.task_queue);
    sched_entry.task_queue.clear();
    sched_entry.remote_lease_retry_count = 0;
  } else {
    RAY_LOG_EVERY_MS(INFO, 30 * 1000)
        << "Retrying attempt to schedule lease (retry "
        << sched_entry.remote_lease_retry_count << ")...";
    RequestNewWorkerIfNeeded(scheduling_key);
  }
}

// ray_config_def.h 中添加配置:
// RAY_CONFIG(int64_t, max_remote_lease_retries, 100)  // 默认 100 次
```

### 6.3 修复优先级

| 优先级 | 修复 | 影响范围 | 难度 | 推荐行动 |
|--------|------|----------|------|----------|
| **P0** | Fix 2: RequestWorkerLease 加超时 | 防止永久卡死 | 低（改一行） | 立即修改，提交社区 PR |
| **P0** | Fix 4: Disconnect fail pending | 确保死节点请求被清理 | 中 | 立即修改，提交社区 PR |
| P1 | Fix 3: READY/IDLE 限制重试 | 防止无限循环 | 中 | 提交社区 PR |
| P1 | Fix 5: Spillback 前验证存活 | 减少无效 spillback | 低 | 提交社区 PR |
| P1 | Fix 6: Spillback 重试上限 | 防止回调永远不调用场景下的兜底 | 低 | 提交社区 PR |
| P2 | Fix 1: Worker buffer 优先淘汰完成 task | 改善可观测性 | 中 | 提交社区 PR |

### 6.4 社区修复状态验证（2025-05-17 对比 upstream master）

通过逐文件对比 Ray 2.52.1 和 upstream `ray-project/ray` master 分支最新代码，确认 **6 个核心问题在 upstream 中全部未修复**：

| 修复项 | 关键文件 | upstream master 现状 | 验证结论 |
|--------|----------|---------------------|----------|
| Fix 1: Worker buffer 优先淘汰 | `task_event_buffer.cc` | 仍是纯 FIFO `circular_buffer` | **未修复** |
| Fix 2: Lease 加超时 | `raylet_client.cc` | `method_timeout_ms` 仍为 `-1` | **未修复** |
| Fix 3: 重试限制 | `retryable_grpc_client.cc` | READY/IDLE 仍 `attempt_number_ = 0`，`InfiniteFuture` 存在 | **未修复** |
| Fix 4: Disconnect fail pending | `raylet_client_pool.cc` | `Disconnect()` 仍只 `erase(it)` | **未修复** |
| Fix 5: Spillback 验证存活 | `cluster_lease_manager.cc` | `ScheduleOnNode` 无 liveness 检查 | **未修复** |
| Fix 6: 重试上限 | `normal_task_submitter.cc` | TODO 注释 `Fail after some number of retries?` 仍在 | **未修复** |

**upstream master 关键代码片段验证**：

```cpp
// raylet_client.cc (upstream master) — 仍然无限超时
void RayletClient::RequestWorkerLease(
    rpc::RequestWorkerLeaseRequest &&request,
    const rpc::ClientCallback<rpc::RequestWorkerLeaseReply> &callback) {
  INVOKE_RETRYABLE_RPC_CALL(retryable_grpc_client_, NodeManagerService,
                            RequestWorkerLease, request, callback, grpc_client_,
                            /*method_timeout_ms*/ -1);  // ← 仍然 -1
}

// retryable_grpc_client.cc (upstream master) — 仍然 InfiniteFuture
void RetryableGrpcClient::Retry(std::shared_ptr<RetryableGrpcRequest> request) {
  const auto timeout = request->GetTimeoutMs() == -1
                           ? absl::InfiniteFuture()    // ← 仍然永不超时
                           : now + absl::Milliseconds(request->GetTimeoutMs());
  pending_requests_.emplace(timeout, std::move(request));
}

// raylet_client_pool.cc (upstream master) — Disconnect 仍然只删引用
void RayletClientPool::Disconnect(ray::NodeID id) {
  absl::MutexLock lock(&mu_);
  auto it = client_map_.find(id);
  if (it == client_map_.end()) return;
  client_map_.erase(it);  // ← 仍然不 fail pending requests
}

// normal_task_submitter.cc (upstream master) — TODO 仍然存在
} else if (NodeID::FromBinary(raylet_address.node_id()) != local_node_id_) {
  // TODO(swang): Fail after some number of retries?  // ← 仍未实现
  RequestNewWorkerIfNeeded(scheduling_key);
}
```

### 6.5 推荐行动计划

| 优先级 | 行动 | 工作量 | 效果 |
|--------|------|--------|------|
| **立即** | 调整 `task_events_*` 配置 + `report_interval_ms=1000` | 0（配置） | 恢复 dashboard 可见性 |
| **立即** | 部署 lease 卡死监控脚本 | 低 | 提前发现问题 |
| **短期** | 改 `method_timeout_ms` 从 -1 到 300000 | 改一行代码 | **根本解决 lease 卡死** |
| **短期** | 修改 `Disconnect` fail pending | 中 | 确保死节点请求被清理 |
| **中期** | 向 Ray 社区提 issue + PR | 中 | 推动 upstream 修复 |

### 6.6 社区 Issue

已提交 GitHub Issue: [ray-project/ray#63405](https://github.com/ray-project/ray/issues/63405)

---

## 七、问题时间线

```
T0 (作业启动)
  → 大量 task 持续执行，task event 快速积累
  → Worker buffer (100K) + GCS storage (100K) 持续被消耗

T1 (buffer 满)
  → Worker buffer 满 → FIFO 淘汰最老事件 → PENDING_NODE_ASSIGNMENT task 的事件被淘汰
  → GCS storage 满 → 先淘汰 FINISHED task，后淘汰其他 → PENDING task 也被淘汰
  → Dashboard 上部分 task 变为不可见

T2 (节点死亡 - 16:03:18)
  → 节点 a21f2c55 死亡
  → Plasma object 丢失
  → 相关 task 被 resubmit (max_retries=-1)

T3 (Spillback 竞争条件)
  → 重新提交的 task 请求 lease
  → 本地 Raylet 资源视图尚未更新（GCS 广播有延迟）
  → 选择已死节点作为 spillback 目标
  → 返回 retry_at_raylet_address = 已死节点

T4 (gRPC 卡死)
  → Core Worker 向已死节点发送 RequestWorkerLease (method_timeout=-1)
  → gRPC 失败 → 进入 RetryableGrpcClient 重试队列 (timeout=InfiniteFuture)
  → CheckChannelStatus 恢复机制失效:
    - READY/IDLE 状态 → 无限重试循环，attempt_number_ 被重置
    - 或 TRANSIENT_FAILURE → GCS 查询失败不处理
    - 或 Disconnect 只移除引用不 fail pending

T5 (永久卡死 - 当前状态, 20+ 小时)
  → NormalTaskSubmitter 的 callback 永远不会被调用
  → pending_lease_requests 中的 lease_id 永远不会被移除
  → scheduling_key_entry 的 task_queue 永远卡住
  → Ray Data operator 显示 Tasks: 1，但 task 永远无法被调度
  → 由于 task event 已被淘汰，dashboard 上也看不到这个卡死的 task
```

---

## 八、总结

本问题是 **Ray Core 调度层面的多个设计缺陷在大规模长时间作业中的叠加效应**：

| 层面 | 问题 | 根因 |
|------|------|------|
| 可观测性 | PENDING_NODE_ASSIGNMENT task 在 dashboard 不可见 | Worker FIFO 淘汰 + GCS 容量不足 |
| 调度正确性 | RequestWorkerLease 永久卡死 | 无限超时 + spillback 竞争条件 + gRPC 恢复失效 |
| 设计遗留 | 无重试上限 | `// TODO(swang): Fail after some number of retries?` |

两者叠加造成了"task 既不可见又不执行"的假象：Ray Data 内部认为 task 已提交（`self._data_tasks` 中有记录），但 Ray Core 调度层面已经永久卡死，且因为 task event 被淘汰，dashboard 上也无法看到这个卡死的 task。

---

## 九、相关文档

- [Task Event 数据链路与淘汰机制分析](./ray-task-event-data-flow-and-eviction-analysis.md) — Dashboard 数据获取链路、两层淘汰关系、Owner/Executor Buffer 分工、report_interval 影响
- [RequestWorkerLease gRPC 卡死深入分析](./ray-lease-stuck-deep-dive.md) — 完整调用链代码级分析、Disconnect 不 fail pending 的根因、READY/IDLE 循环死锁机制、确认方法和修复方案
- [Ray GCS FD 耗尽排查](./ray-gcs-fd-exhaustion-troubleshooting.md) — 另一个导致调度阻塞的问题
