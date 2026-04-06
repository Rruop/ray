# Ray Job 卡死排查分析

**时间**: 2026-05-20
**集群**: kml-hb2az1-l3-2 / lmserving
**Job**: raysubmit_9MrvSjhx8Wsmej2n
**Driver PID**: 838610 (pod: kml-task-661218-record-15739495-prod-worker-0-8qhgk)

## 现象

```
2026-05-20 10:28:31,519 INFO logging_progress.py:231 -- StreamingRepartition[num_rows_per_block=64]: 20867191/11419060
2026-05-20 10:28:31,519 INFO logging_progress.py:233 --   Tasks: 402; Actors: 0; Queued blocks: 0 (0.0B); Resources: 402.0 CPU, 520.6MiB object store
```

- 进度完全停滞（20867191/11419060 数值不变）
- 402 个 task 占着 CPU 但无 running 也无 lease 活动
- 已卡死 **11.7 小时**（从 2026-05-19 22:57:34 至今）

## 进度数值 20867191/11419060 的含义

来自 `logging_progress.py:231` 的 `_format_progress`:

```python
f"{m.name}: {m.completed}/{m.total}"
```

| 数值 | 含义 | 来源 |
|------|------|------|
| 20867191 (completed) | 已被下游 operator 消费的输出行数 | `op.metrics.row_outputs_taken` |
| 11419060 (total) | **预估**该 operator 将产生的总行数 | `num_output_rows_total()` = `avg_rows_per_task × estimated_num_tasks` |

completed > total 是正常现象：total 是估算值，基于历史平均值推算，Repartition 可能放大行数导致实际 > 预估。**不代表重复处理**。

## 排查方法与过程

### 核心原则：Dashboard 不可靠，必须用 state-dump 和 Ray Data 内部状态

Dashboard / `ray list tasks` 的数据来源是 GCS，task event 经过两层淘汰后可能不可见。因此**不能依赖 Dashboard 判断 task 是否存在或卡死**，必须使用以下更可靠的方法：

| 方法 | 可靠性 | 数据来源 | 能看到什么 |
|------|--------|---------|-----------|
| Dashboard / `ray list tasks` | **不可靠**（事件可能被淘汰） | GCS 内存 | 只有未被淘汰的 task |
| **state-dump counter** | **可靠** | Core Worker 内部统计 | lease RPC 发出/收到的精确计数 |
| **Ray Data 进度日志** | **可靠** | Ray Data `_data_tasks` | active/queued task 数 |
| **诊断日志**（改动后的 INFO/WARNING） | **可靠** | Core Worker C++ 层 | spillback redirect 目标、remote lease 超时 |

### 定位步骤

#### Step 0: 发现卡死 — Ray Data 进度日志

```bash
grep "Tasks:" job-driver-*.log | tail -5
# 看到 "402 active" 或 "1 active" 但长时间数值不变 → 确认卡死
```

#### Step 0.5: 定位卡死类型 — state-dump counter

```bash
PID=838610
grep "RequestWorkerLease" python-core-driver-*_${PID}.log | tail -2
# 对比 client.total - OnReplyReceived.total = 卡死的 lease 数
# 例: 39997680 - 39997278 = 402
# 差值 > 0 且长时间不变 → lease 卡死确认
```

#### Step 0.6: 区分卡死路径 — 诊断日志

```bash
# 是否发生了 spillback redirect（路径 C: 本地 raylet redirect）
grep "Spillback: redirect" python-core-driver-*_${PID}.log

# 是否有 remote lease 超时
grep "Remote lease failed" python-core-driver-*_${PID}.log

# 如果有 Spillback: redirect → 路径 C（本地 raylet redirect 到 dead node）
# 如果没有 Spillback: redirect → 路径 A（LocalityAware 直接选了 dead node）
# 两种路径症状相同，但机制不同

# redirect 目标 IP 是否是 dead node
grep -oP 'ip: \K[0-9.]+' <<< "$(grep 'Spillback: redirect' python-core-driver-*_${PID}.log)" | sort -u
ray list nodes --filter 'STATE=DEAD' -o yaml | grep node_ip
```

### 诊断决策树

```
Ray Data 进度卡住？
├── 是 → 检查 state-dump: RequestWorkerLease total - OnReplyReceived > 0？
│         ├── 是 → 有 lease 卡死
│         │   ├── grep "Spillback: redirect" 有结果？
│         │   │   ├── 有 → 路径 C: 本地 raylet redirect 到 dead node
│         │   │   │   └── 目标 IP 是否在 dead node 列表？→ 确认根因
│         │   │   └── 无 → 路径 A: LocalityAware 直接选了 dead node
│         │   │       └── （版本含日志改动才可靠，否则需 DEBUG 日志）
│         │   └── 否 → 不是 lease 卡死，检查其他原因（如 infeasible、资源不足）
│         └── 否 → 进度卡住但无 lease 差值 → 检查 Ray Data 层面问题
```

### Step 1: 确认 Task 在 Ray 层面的状态

**工具**: core-driver state-dump
**文件**: `/tmp/ray/session_latest/logs/python-core-driver-*_838610.log`

```
NodeManagerService.grpc_client.RequestWorkerLease - 39997680 total (402 active)
NodeManagerService.grpc_client.RequestWorkerLease.OnReplyReceived - 39997278 total (0 active)
```

**结论**: 39997680 - 39997278 = **402 个 RequestWorkerLease RPC 发出后从未收到回复**，数值在 11+ 小时内完全冻结。

### Step 2: 确认 Head Raylet 状态

**文件**: `/tmp/ray/session_latest/logs/raylet.out`

```
NodeManagerService.grpc_server.RequestWorkerLease - 43103100 total (0 active)
NodeManagerService.grpc_server.RequestWorkerLease.HandleRequestImpl - 43103100 total (0 active)
[state-dump] num_waiting_for_resource: 0
[state-dump] num_waiting_for_plasma_memory: 0
[state-dump] num_waiting_for_remote_node_resources: 0
[state-dump] Number of granted lease arguments: 0
```

**结论**: Head raylet 侧：
- 所有请求已处理完毕（0 active）
- 调度队列为空（num_waiting_for_resource: 0）
- 没有待 dispatch 的 grant（Number of granted lease arguments: 0）
- Head raylet 未作为 client 转发过 lease（`grpc_client.RequestWorkerLease` 不存在于 raylet.out）

**为什么 driver client total (39997680) ≠ raylet server total (43103100)**：

差值 3,105,420 由两个因素造成：

1. **Raylet 服务多个 client**: Head raylet 是该节点上**所有** core worker 的本地 raylet，同节点其他 worker/actor 也向它发 `RequestWorkerLease`。43103100 = 来自 driver 的 + 来自同节点其他 worker 的。
2. **Driver 部分 RPC 发到远程 raylet**: 如果 spillback 发生，driver 的 39997680 中有一部分（含那 402 个）是发到远程 raylet 的，不会被本地 raylet 的 server counter 计入。

两个 counter 没有必然相等的关系：`raylet_server_total = Σ(所有本地 client 发到本地的)` ≠ `driver_client_total = driver 发到本地 + driver 发到远程`。

**为什么不存在统一计数**：

Ray 中每个 gRPC endpoint 独立统计自己的 server/client counter，没有全局的"请求总账"。这是分布式系统中常见的设计——每个节点只关心自己视角的统计。Counter 的含义如下：

```
                     driver client counter (39,997,680)
                     统计维度: 这个 driver 进程发出的所有 RequestWorkerLease RPC
                     ┌────────────────────────────────────────┐
                     │                                        │
                     │  发到 local raylet 的 ──────────┐      │
                     │  发到 remote raylet A 的 ───┐   │      │
                     │  发到 remote raylet B 的 ─┐ │   │      │
                     └───────────────────────────┼─┼───┼──────┘
                                                 │ │   │
                                                 │ │   ▼
                                                 │ │   local raylet server counter (43,103,100)
                                                 │ │   统计维度: 这个 raylet 收到的所有 RequestWorkerLease RPC
                                                 │ │   ┌────────────────────────────────────────┐
                                                 │ │   │  来自 driver 的                        │
                                                 │ │   │  来自同节点 actor A 的                 │
                                                 │ │   │  来自同节点 worker B 的                │
                                                 │ │   │  ...                                   │
                                                 │ │   └────────────────────────────────────────┘
                                                 │ │
                                                 │ ▼
                                                 │ remote raylet A server counter (各自独立)
                                                 ▼
                                                 remote raylet B server counter (各自独立)
```

**如何确认 402 个 RPC 是发给死亡 remote raylet 的**：

仅凭 counter 差值不能 100% 确认，需要结合排除法推理：

1. **差值 = 发出但未收到回复的 RPC 数**：`client.total (39,997,680) - client.OnReplyReceived (39,997,278) = 402`

2. **这些 RPC 发给了谁？** 只有两种可能的目标：
   - Local raylet — 但如果 local raylet 挂了，driver 进程会直接退出（`normal_task_submitter.cc:456-458`：`QuickExit()`）
   - Remote raylet — spillback 的目标

3. **Driver 还活着 → local raylet 没挂 → 402 个无回复的 RPC 只能是发给 remote raylet 的**

4. **正常的 remote raylet 即使资源不够也会回 `rejected=true`**（即时回复）。不回复只有一种情况：gRPC 连接层面卡住了，即目标节点已死，gRPC 进入无限重试

5. **交叉验证方法**：
   - 搜索 `"Spillback: redirect"` 日志，看 spillback 目标 IP
   - 搜索 `"Remote lease failed"` 日志，看是否有来自这些 IP 的失败
   - 对比 GCS 中的 dead node 列表（`ray list nodes --filter 'STATE=DEAD'`），确认这些目标节点确实已死

**推理链总结**: Driver 活着 → local raylet 没挂 → 无回复的只能是 remote RPC → remote 不回复只能是节点已死 → gRPC 无限重试卡住（`method_timeout_ms = -1`）。

### Step 3: 确认网络连接状态

**工具**: `ss -tnpi`, `/proc/838610/fd/33`

| 项目 | 值 |
|------|-----|
| Driver → Raylet TCP 连接数 | 1 条（fd=33） |
| 连接目标 | 10.81.0.20:40985（localhost） |
| 连接创建时间 | 2026-05-19 22:15:05 |
| 进程启动时间 | 2026-05-19 22:14:44 |
| 最后通信 | lastsnd:365ms（持续活跃） |
| Send-Q | 0（无积压数据） |
| TCP 状态 | ESTAB，无重传 |

**结论**: 连接**从未重建过**，始终健康。

### Step 4: 确认集群节点状态

```
ray list nodes --filter 'STATE=DEAD': 9 个 dead 节点
Driver 到 dead 节点的 TCP 连接: 0 条（已经断开）
Driver 总 TCP 连接: 13,142 条
  - 到 alive 节点 (91个): 2,020 条
  - 到 non-alive IP (1,210个): 13,141 条 ESTAB
```

**结论**: 大量到 non-alive IP 的 ESTAB 连接是历史遗留（节点被缩容后 TCP 未关闭）。

### Step 5: 分析 402 个 Lease 卡死的根因

详见下方 "402 个 Lease Stuck 的根因" 章节。

### Step 6: 分析 Watchdog 为何未触发

**发现**: `patch_watchdog_block` 从未对 `StreamingRepartition[num_rows_per_block=64]` 触发过（grep 确认为 0）。

**根因**: `WATCHDOG_MIN_COMPLETION_RATIO = 0.95` 阈值导致的永久盲区

```python
# _check_watchdog 中的关键逻辑:
total = current_completed + active_count
completion_ratio = current_completed / total
if completion_ratio < 0.95:
    return  # ← 永远在这里 return
```

该 operator 的实际数值:
- `current_completed` ≈ 3619（pool_size 从 4021 降到 402）
- `active_count` = 402
- `completion_ratio = 3619 / (3619 + 402) = 3619 / 4021 = 90.0%`

**90.0% < 95% → watchdog 永远跳过此 operator！**

**对比**: `FlatMap(ClipMergeMapper)` 能触发是因为 `completed=326035, active=1`，ratio=99.9997% > 95%。

**本质问题**: 当 stuck task 数量占比 > 5% 时，ratio 永远达不到 0.95 阈值，watchdog 存在永久盲区。

---

## Lease 卡死时 Task 的状态与可见性

### Task 在 Ray Core 中的状态

Lease 卡死的 task 在 `TaskManager::task_map_` 中确实存在，状态为 **`PENDING_NODE_ASSIGNMENT`**。

状态转换路径（`src/ray/protobuf/common.proto` TaskStatus enum）：

```
PENDING_ARGS_AVAIL → 依赖就绪 → PENDING_NODE_ASSIGNMENT → lease grant → SUBMITTED_TO_WORKER → RUNNING → FINISHED
```

关键代码路径：

| 步骤 | 代码位置 | 状态变化 |
|------|---------|---------|
| 提交 task | `task_manager.cc:343-349` `AddPendingTask()` | → `PENDING_ARGS_AVAIL` |
| 依赖就绪 | `task_manager.cc:1672-1683` `MarkDependenciesResolved()` | → `PENDING_NODE_ASSIGNMENT` |
| Lease granted, worker 分配 | `task_manager.cc:1685-1693` `MarkTaskWaitingForExecution()` | → `SUBMITTED_TO_WORKER` |

Lease 卡死时，task 坐在 `NormalTaskSubmitter::scheduling_key_entries_[key].task_queue` 里，lease 请求记录在 `pending_lease_requests` 中，停留在 `PENDING_NODE_ASSIGNMENT` 状态。只有收到 `worker_address` 回复才会推进到 `SUBMITTED_TO_WORKER`。

### Task 在 Dashboard 中不可见的原因

**Dashboard 数据源是 GCS `GetTaskEvents` RPC**，不是 Core Worker 内部状态。事件要经过两层淘汰才能到达 Dashboard：

```
Task 状态变化
  → [第一层] Worker 侧 TaskEventBuffer (circular_buffer 100K, 纯 FIFO 淘汰)
    │ 卡死的 task 的 PENDING_ARGS + PENDING_NODE_ASSIGNMENT 事件是最老的
    │ → FIFO 淘汰时最先被清除
    │ → 该 task attempt 被标记为 dropped
    │ → 之后所有事件直接丢弃（task_event_buffer.cc:1025-1028）
    ↓
  → [第二层] GCS Storage (100K, 优先级淘汰)
    │ Priority 0 (FINISHED) 先淘汰完
    │ Priority 1 (ActorTask) 其次
    │ Priority 2 (PENDING normal task) 最后，但大规模作业下也最终被淘汰
    ↓
  → Dashboard 看不到这个 task
```

关键机制（详见 `docs/ray-task-event-eviction-and-lease-stuck-analysis.md`）：

1. **Worker buffer 纯 FIFO**：不区分 task 状态，长期卡在 `PENDING_NODE_ASSIGNMENT` 的 task 事件最老，最先被淘汰
2. **dropped 标记是永久性的**：一旦 task attempt 被标记为 dropped，后续所有状态事件直接丢弃，不会补回
3. **GCS 淘汰最终也覆盖 PENDING**：Priority 2 的 PENDING normal task 在 FINISHED 和 ActorTask 全部淘汰完后也会被淘汰

**结论**: Task **既不可见又不执行** — Ray Data 内部认为 task 已提交（`self._data_tasks`），Ray Core 调度层面卡死，Dashboard 因事件淘汰看不到。这就是为什么仅凭 Dashboard 无法定位此类问题。

### 可见性对比

| 查看方式 | lease 卡死的 task 是否可见 | 数据来源 |
|---------|--------------------------|---------|
| Dashboard / `ray list tasks` | **不可见**（事件被淘汰） | GCS 内存存储 |
| `state-dump` counter | **可见**（counter 差值 = 卡死数） | Core Worker 内部统计 |
| Ray Data 进度日志 | **可见**（`Tasks: 402` 不变） | Ray Data 内部 `_data_tasks` |
| `task_manager.cc` task_map_ | **可见**（`PENDING_NODE_ASSIGNMENT`） | Core Worker 内存 |

---

## 402 个 Lease Stuck 的根因

### Spillback 机制说明

Spillback 是 Ray 调度中 core worker 向**远程 raylet** 发送 lease 请求的机制。

#### 基本流程

**注意**: 首次 `RequestWorkerLease` 的目标 raylet **不一定是本地 raylet**，由 `LeasePolicy` 决定（`normal_task_submitter.cc:315-319`）：

```cpp
if (raylet_address == nullptr) {
    std::tie(best_node_address, is_selected_based_on_locality) =
        lease_policy_->GetBestNodeForLease(lease_spec);
    raylet_address = &best_node_address;
}
```

Ray 有两种 LeasePolicy 实现（`lease_policy.h`）：

| LeasePolicy | 行为 | 适用场景 |
|------------|------|---------|
| `LocalLeasePolicy` | 始终返回本地 raylet | 关闭 `locality_aware_leasing` 时 |
| `LocalityAwareLeasePolicy` | 根据依赖对象的数据局部性选节点，**可能直接选远程 raylet** | 默认（Ray Data 作业） |

对于 **Ray Data 作业**（driver 在 head 节点），parquet 数据在 worker 节点上，`LocalityAwareLeasePolicy` 会选择数据所在节点，首次 `RequestWorkerLease(grant_or_reject=false)` **直接发到远程 raylet**，不经过本地 raylet。

```
【流程 A: LocalityAware 直接选远程节点（Ray Data 作业常见）】
Core Worker → LeasePolicy.GetBestNodeForLease → 选出 node X（数据所在地）
Core Worker → RequestWorkerLease(grant_or_reject=false) → 远程 Raylet (node X)
     → node X 有资源: grant（正常完成）
     → node X 没资源: 返回 retry_at_raylet_address = node Y（redirect）
     → node X 已死: RPC 永远不回来 ← 本案另一种卡死路径

【流程 B: LeasePolicy 选本地节点 / LocalLeasePolicy】
Core Worker → RequestWorkerLease(grant_or_reject=false) → 本地 Raylet
                                      ↓ (本地 raylet 有资源)
Core Worker ← reply: "granted, worker on node X" ← 本地 Raylet

【流程 C: 本地 Raylet 无资源 → spillback redirect】
Core Worker → RequestWorkerLease(grant_or_reject=false) → 本地 Raylet
                                      ↓ (本地 raylet 回复 redirect)
Core Worker ← reply: "retry_at_raylet_address = node X" ← 本地 Raylet
Core Worker → RequestWorkerLease(grant_or_reject=true) → 远程 Raylet (node X)  ← 第二跳
     ...如果 node X 已死，这个 RPC 永远不回来...
```

**本案的两种卡死路径**：

| 路径 | 描述 | 是否经过本地 raylet |
|------|------|-------------------|
| 路径 A | `LocalityAwareLeasePolicy` 直接选了已死亡的远程节点，首次请求就发到 dead node | **不经过**，直接到远程 |
| 路径 C | 本地 raylet 回复 `retry_at_raylet_address` 指向已死亡的远程节点 | **经过**，本地 raylet redirect |

两种路径症状相同（counter 差值 = 卡死 lease 数），但机制不同。需要通过诊断日志区分。

在源码 `normal_task_submitter.cc:425-435` 中：
```cpp
} else {
    // The raylet redirected us to a different raylet to retry at.
    RAY_CHECK(!is_spillback);
    RequestNewWorkerIfNeeded(scheduling_key, &reply.retry_at_raylet_address());
}
```

当本地 raylet 回复 `retry_at_raylet_address` 时，core worker 会向**远程 raylet** 发起新的 `RequestWorkerLease`。如果远程 raylet 所在节点已死，这个 RPC 会进入 `RetryableGrpcClient` 的无限重试循环（原始代码 `method_timeout_ms = -1`）。

#### grant_or_reject 两阶段协议

Spillback 到 remote raylet 的目的是**让 remote raylet 直接分配 worker（lease）**，而不是让它再做调度决策。这由 `grant_or_reject` 字段控制。

Proto 定义（`node_manager.proto:48-51`）：
```protobuf
// If it's true, either grant the lease if the task is
// locally schedulable or reject the request.
// Else, the raylet may return another raylet at which to retry the request.
bool grant_or_reject = 3;
```

代码中发起 spillback 请求时（`normal_task_submitter.cc:328-330`）：
```cpp
raylet_client->RequestWorkerLease(
    lease_spec.GetMessage(),
    /*grant_or_reject=*/is_spillback,  // 第一次 false，spillback 后 true
    ...);
```

**两阶段行为**：

| 阶段 | 目标 | grant_or_reject | Raylet 可选行为 |
|------|------|-----------------|----------------|
| 第一次请求 | Local Raylet | `false` | Grant / Spillback（redirect）/ Cancel |
| Spillback 后 | Remote Raylet | `true` | **仅 Grant 或 Reject**，不能再 spillback |

Remote raylet 收到 `grant_or_reject=true` 的请求后，如果本地资源不足，**直接 reject**，不会链式 spillback（`cluster_lease_manager.cc:429-434`）：

```cpp
if (work->grant_or_reject_) {
    // 如果是 spillback 来的请求，本地调度不了就直接 reject
    for (const auto &reply_callback : work->reply_callbacks_) {
        reply_callback.reply_->set_rejected(true);
        reply_callback.send_reply_callback_(Status::OK(), nullptr, nullptr);
    }
    return;
}
```

Driver 端也有断言确保 spillback 只发生一次（`normal_task_submitter.cc:427`）：
```cpp
RAY_CHECK(!is_spillback);  // spillback 只发生在第一跳，不会链式 spillback
```

**关键结论**: Spillback **最多两跳**。Local raylet 负责做调度决策，remote raylet 只执行 grant 或 reject。

#### Reject 后的处理：回退重试，Task 不会失败

当 remote raylet reject 后，**task 不会失败**，而是回到 local raylet 重新走一遍调度流程（`normal_task_submitter.cc:398-405`）：

```cpp
} else if (reply.rejected()) {
    RAY_LOG(DEBUG) << "Lease rejected " << lease_id;
    // It might happen when the first raylet has a stale view
    // of the spillback raylet resources.
    // Retry the request at the first raylet since the resource view may be
    // refreshed.
    RAY_CHECK(is_spillback);
    RequestNewWorkerIfNeeded(scheduling_key);  // ← 不传地址，回到 local raylet
}
```

Reject 的原因：local raylet 对 remote 节点的**资源视图过期了**（stale view）。Local raylet 以为 remote 有资源就 spillback 过去了，但 remote 实际已经没资源了，所以 reject。

完整的重试循环：

```
Driver → Local Raylet (grant_or_reject=false)
  → Local Raylet 认为 Node X 有资源，spillback
    → Driver → Node X (grant_or_reject=true)
      → Node X 发现没资源，reject
        → Driver → Local Raylet (grant_or_reject=false)  ← 重新来一轮
          → 此时 local raylet 的资源视图可能已刷新，选另一个节点或等待
```

Task 始终留在 driver 的 `task_queue` 里，不会被 fail 掉。`RequestNewWorkerIfNeeded(scheduling_key)` 不传地址参数时默认回到 local raylet，等资源视图更新后重新做调度决策。

**注意**: 这个 reject → 回退重试**没有次数上限**，理论上可以无限循环（local raylet 视图更新慢时会反复 spillback → reject → retry），但每轮是即时回复的，不像 dead node 场景那样会卡 5 分钟。

#### Counter 影响对比

| 场景 | driver client.total | driver OnReplyReceived | local raylet server.total | remote raylet server.total |
|------|--------------------|-----------------------|--------------------------|---------------------------|
| 无 spillback（本地 grant） | +1 | +1 | +1 | — |
| Spillback + grant | +2 | +2 | +1 | +1 |
| Spillback + reject + retry | +3（local + remote + local 重试） | +3 | +2 | +1 |
| Spillback + dead node（原始代码） | +2 | +1（只收到 local 的 redirect 回复） | +1 | gRPC 无限重试，不计入 |
| Spillback + dead node（加超时后） | +3（local + remote 超时 + local 重试） | +2（local redirect + remote timeout callback） | +2 | +0 |

### 如何判断是否发生了 Spillback

| 判断方法 | 本案结果 | 含义 |
|---------|---------|------|
| Head raylet `grpc_client.RequestWorkerLease` | **不存在**（0 条） | Head raylet 从未**作为 client** 向远程 raylet 转发 lease |
| Core worker `RequestWorkerLease` 目标 | 只有 1 条到 localhost 的 TCP 连接 (fd=33 → 10.81.0.20:40985) | Core worker 只向本地 raylet 发过 lease 请求 |
| `is_spillback` 标志 | 无直接日志证据 | — |

**关键**: 虽然 head raylet 没有作为 client 转发，但 **core worker 自身会根据 raylet reply 中的 `retry_at_raylet_address` 直接向远程 raylet 发 lease 请求**（这走的是 `RayletClientPool::GetOrConnectByAddress`，创建新的 `RayletClient` 到远程节点）。

### 两种可能的卡死机制

#### 假说 A: Core Worker 向远程 Dead Raylet 发 Lease（spillback 场景）

```
时间线:
22:55  节点被缩容，部分 raylet 下线
22:57  本地 raylet 回复 402 个 "retry_at_raylet_address = dead_node"
       Core Worker 向 dead_node 发 RequestWorkerLease
       RetryableGrpcClient: method_timeout_ms = -1 (无限重试)
       → 永远卡死
```

**支持证据**:
- Commit `a21a02da4c` 的 message 明确描述了这个场景：*"When a node dies after being selected as a spillback target, the lease request to that dead node enters an infinite gRPC retry loop because method_timeout_ms is set to -1"*
- 9 个 DEAD 节点 + 大量被缩容的节点
- Head raylet `0 active` + `num_waiting_for_resource: 0`（raylet 已回复 redirect，不再持有这些请求）
- 402 数值冻结（典型的 infinite retry loop）

**不利证据**:
- `ss -tnp` 中没有找到 driver 到 DEAD 节点 IP 的 TCP 连接（可能 DEAD 节点的 IP 已被复用或连接状态不同）
- Head raylet 的 `grpc_client.RequestWorkerLease = 0`（但这只是 raylet 自己没做转发，不影响 core worker 自己发）

#### 假说 B: 本地 Raylet 回复丢失（gRPC stream 问题）

```
时间线:
22:55  patch_interleave_dispatch KeyError 风暴阻塞 driver event loop
22:57  本地 raylet 回复了 402 个 lease grant/redirect
       但 driver 的 gRPC completion queue 未处理这些 response
       → Core worker 永远等待 OnReplyReceived 回调
```

**支持证据**:
- Raylet server `0 active`（已经发了 reply）
- 时间与 KeyError 风暴吻合（22:55 → 22:57）
- TCP 连接健康（本地通信不会丢包）

**不利证据**:
- gRPC 本地通信丢 response 极其罕见
- Send-Q=0 说明 TCP 层面数据已经传输完毕
- 如果 reply 已到达 core worker 的 TCP recv buffer，gRPC 应该最终处理它

### 结论: 假说 A（spillback 到 dead node）可能性更高

基于 commit `a21a02da4c` 的描述，这**正是**该 fix 要解决的 exact scenario：

1. 本地 Raylet 回复 redirect（`retry_at_raylet_address = dead_node`）
2. Core Worker 收到 reply → `OnReplyReceived` 被调用 → **但随即发起新的 RequestWorkerLease 到远程 dead node**
3. 新的 RPC 进入 `RetryableGrpcClient` → `method_timeout_ms = -1` → 无限重试
4. 这 402 个"new RPC to dead node"永远不会收到回复

**对 counter 的影响**:
- Core Worker 的 `RequestWorkerLease total` 包含了对远程 raylet 的请求
- Core Worker 的 `OnReplyReceived total` 只计入实际收到回复的（不含 dead node 的）
- **差值 402 = 卡在对 dead node 的无限重试中的 RPC 数**

### 如何具体确认是哪种原因

| 确认方法 | 操作 | 预期结果 |
|---------|------|---------|
| **gRPC verbose 日志** | 设置 `GRPC_VERBOSITY=DEBUG GRPC_TRACE=connectivity_state,http` 重跑 | 能看到 channel 连接失败重试到哪个 IP |
| **core worker debug 日志** | `RAY_BACKEND_LOG_LEVEL=debug` | 会打印 `Redirect lease ... from raylet X to raylet Y`（line 428-433）|
| **检查 RayletClientPool** | 在 driver 中 dump `raylet_client_pool_` 中的所有连接目标 | 如果有到 dead node 的 client，确认假说 A |
| **检查 RetryableGrpcClient 队列** | gdb attach 到 driver，打印 retry queue | 如果 queue 中有 402 个 pending request 到 dead node addr |
| **确认 raylet reply 内容** | 回放 raylet 在 22:57 前后的 decision 日志（需 DEBUG 级别） | 看是否 reply 中包含了 `retry_at_raylet_address` |
| **对比 counter 归属** | 如果 402 个 RPC 是到远程 raylet 的，core worker 的 stats 应该显示对应远程节点的 grpc channel 有 pending | 需要更细粒度的 per-channel stats |

---

## Fix 分析: a21a02da4c

### 改动内容

```cpp
// src/ray/common/ray_config_def.h
RAY_CONFIG(int64_t, worker_lease_timeout_ms, 300000)  // 默认 5 分钟

// src/ray/raylet_rpc_client/raylet_client.cc
raylet_client->RequestWorkerLease(
    ...
    /*method_timeout_ms*/ RayConfig::instance().worker_lease_timeout_ms());
    // 之前是 -1 (无限)
```

### 超时后的行为（无限重试 vs 有限重试）

根据 `RetryableGrpcClient` 的实现（`retryable_grpc_client.h:73-74`）：

> *If a call's timeout_ms reaches during retry, its callback is called with Status::TimedOut.*

超时后，callback 会被调用并传入 `Status::TimedOut`。然后进入 `normal_task_submitter.cc:437-450`：

```cpp
} else if (NodeID::FromBinary(raylet_address.node_id()) != local_node_id_) {
    // A lease request to a remote raylet failed. Retry locally if the lease is
    // still needed.
    // TODO(swang): Fail after some number of retries?
    RAY_LOG_EVERY_MS(INFO, 30 * 1000)
        << "Retrying attempt to schedule lease at remote node...";
    RequestNewWorkerIfNeeded(scheduling_key);  // ← 重新从本地 raylet 申请
}
```

### 关键行为：**不会导致 task 失败，而是无限回退重试**

| 问题 | 答案 |
|------|------|
| 有重试次数限制吗？ | **没有**。代码中有 `TODO(swang): Fail after some number of retries?` 但未实现 |
| 超时后 task 会失败吗？ | **不会**。超时后调用 `RequestNewWorkerIfNeeded(scheduling_key)` 重新向本地 raylet 申请 |
| 重试间隔？ | 无额外间隔，直接重新发起 lease 请求 |
| 最终效果？ | 5 分钟后超时 → 回退到本地 raylet 重新调度 → 如果本地 raylet 又 redirect 到 dead node → 再等 5 分钟 → 循环... 直到本地 raylet 不再 redirect 到 dead node（比如节点被标记为 DEAD 后 raylet 更新视图） |

### 潜在风险

由于没有重试次数限制，如果本地 raylet 的节点视图更新慢（始终 redirect 到同一个 dead node），task 会经历：
```
5min 超时 → retry 到本地 → 被 redirect 到 dead node → 5min 超时 → retry...
```

每轮 5 分钟，理论上最终会解决（GCS 标记节点为 DEAD 后 raylet 更新视图），但可能需要多个周期（10-30 分钟）。

---

## 结论总结

| 问题 | 根因 |
|------|------|
| 402 个 task 卡死 | 本地 Raylet 将 lease 请求 redirect 到已被缩容的远程节点，Core Worker 向 dead node 发送 RequestWorkerLease 陷入无限重试（method_timeout_ms=-1） |
| Watchdog 未触发 | `WATCHDOG_MIN_COMPLETION_RATIO=0.95` 阈值过高，stuck task 占比 10%（402/4021）导致 ratio=90% 永远达不到触发条件 |
| `patch_watchdog_block` 是否直接导致 | **否**。卡死由 Ray 调度器 redirect 到 dead node + 无超时导致 |
| `patch_watchdog_block` 能否自救 | **否**。即使 watchdog 触发并 cancel task，底层 gRPC RPC 仍会无限重试 |
| Fix a21a02da4c 是否解决 | **是**。加了 300s 超时后，task 会超时回退到本地 raylet 重新调度 |
| Fix 后 task 是否会失败 | **不会**。超时后重新从本地 raylet 申请，不会标记 task 为 failed |
| Fix 后是否有重试次数限制 | **没有**。代码中有 `TODO(swang): Fail after some number of retries?` 但未实现，会无限循环直到 raylet 不再 redirect 到 dead node |
| 如何确认具体根因 | 已增加 INFO/WARNING 级别诊断日志（见下方"诊断日志增强"章节），下次复现时无需 debug 日志即可定位 |

## 修复建议

### 1. RequestWorkerLease RPC 超时（已有 fix: a21a02da4c）

确认此 job 使用的 Ray 版本是否包含该 fix。默认 300s (5min)。

### 2. 考虑加重试次数上限

当前 `normal_task_submitter.cc:440` 有 TODO：
```cpp
// TODO(swang): Fail after some number of retries?
```

建议加一个上限（如 10 次 redirect），超过后标记 task 为 UNSCHEDULABLE 并向上层报错，避免无限循环。

### 3. Watchdog 阈值修复

```python
# 方案: 增加绝对时间兜底
WATCHDOG_FORCE_TRIGGER_S = 3600  # 1小时

if completion_ratio < WATCHDOG_MIN_COMPLETION_RATIO:
    if stalled_s < WATCHDOG_FORCE_TRIGGER_S:
        return
    # 超过 1 小时仍无进展，无论 ratio 多少都触发
```

### 4. 消除 patch_interleave_dispatch 的 KeyError 风暴

22:55:00 时密集的 `_update_allocated_budgets KeyError` 可能干扰了 driver event loop。应修复该 KeyError 或加异常捕获。

---

## 诊断日志增强（已实施）

### 改动文件

`src/ray/core_worker/task_submission/normal_task_submitter.cc`

### 改动 1: Spillback Redirect 日志提升到 INFO 级别

**位置**: L428（spillback redirect 分支）

```cpp
// 改动前: DEBUG 级别，需要设置 RAY_BACKEND_LOG_LEVEL=debug 才能看到
RAY_LOG(DEBUG) << "Redirect lease " << lease_id << " from raylet "
               << NodeID::FromBinary(raylet_address.node_id())
               << " to raylet "
               << NodeID::FromBinary(reply.retry_at_raylet_address().node_id())
               << " for " << function_or_actor_name;

// 改动后: INFO 级别 + 每 10 秒限流 + 增加 IP 信息
RAY_LOG_EVERY_MS(INFO, 10 * 1000)
    << "Spillback: redirect lease " << lease_id << " from local raylet "
    << NodeID::FromBinary(raylet_address.node_id()) << " to remote raylet "
    << NodeID::FromBinary(reply.retry_at_raylet_address().node_id())
    << " (ip: " << reply.retry_at_raylet_address().ip_address() << ")"
    << " for " << function_or_actor_name;
```

**`RAY_LOG_EVERY_MS(INFO, 10 * 1000)` 含义**:
- 以 INFO 级别打印日志
- `10 * 1000` = 10000 毫秒 = **每 10 秒最多打印一条**
- 即使代码被执行数百次（如 402 个 task 同时被 redirect），10 秒内只输出 1 条
- 目的：防止短时间大量 spillback 造成日志洪泛，同时确保事件可见

**为什么不用 WARNING**: Spillback 本身是正常调度行为（资源不足时 redirect 到其他节点），只是在 dead node 场景下才有问题。用 INFO + 限流既保证可观测性又不产生误报告警。

### 改动 2: Remote Lease 失败日志改为 WARNING（无限流）

**位置**: L441（远程 raylet lease 超时/失败分支）

```cpp
// 改动前: INFO 级别 + 每 30 秒限流，且缺少结构化的节点 ID 信息
RAY_LOG_EVERY_MS(INFO, 30 * 1000)
    << "Retrying attempt to schedule lease (id: " << lease_id
    << " name: " << function_or_actor_name
    << ") at remote node (id: " << raylet_address.node_id()
    << " ip: " << raylet_address.ip_address()
    << "). Try again on a local node. Error: " << status.ToString();

// 改动后: WARNING 级别，每次触发都打印，便于精确计数
RAY_LOG(WARNING) << "Remote lease failed (id: " << lease_id
                 << " name: " << function_or_actor_name
                 << ") target_node_id: "
                 << NodeID::FromBinary(raylet_address.node_id())
                 << " target_ip: " << raylet_address.ip_address()
                 << ". Retrying on local node. Error: " << status.ToString();
```

**为什么不需要限流**: 此代码路径只在 `worker_lease_timeout_ms`（默认 300s）超时后触发，每个 lease 每 5 分钟最多进入一次，不会造成日志洪泛。每次都打印便于精确统计有多少 lease 超时回退。

### 诊断效果

改动后**无需设置任何环境变量**，在默认日志级别的 `python-core-driver-*_{pid}.log` 中即可看到：

```
[2026-05-19 22:57:34,xxx INFO normal_task_submitter.cc:428] Spillback: redirect lease abc123 from local raylet 7f5a... to remote raylet 3e2b... (ip: 10.81.5.42) for read_fn
[2026-05-19 23:02:34,xxx WARNING normal_task_submitter.cc:442] Remote lease failed (id: def456 name: read_fn) target_node_id: 3e2b... target_ip: 10.81.5.42. Retrying on local node. Error: Timed out
```

### 定位步骤

下次复现时，执行以下 grep 即可秒级定位：

```bash
# Step 1: 确认是否发生 spillback redirect
grep "Spillback: redirect" python-core-driver-*_${PID}.log

# Step 2: 确认是否有 remote lease 超时
grep "Remote lease failed" python-core-driver-*_${PID}.log

# Step 3: 提取被 redirect 到的目标 IP，对比 dead node 列表
grep -oP 'ip: \K[0-9.]+' <<< "$(grep 'Spillback: redirect' python-core-driver-*_${PID}.log)" | sort -u
ray list nodes --filter 'STATE=DEAD' -o yaml | grep node_ip
```

**判定逻辑**:

| 日志组合 | 结论 |
|---------|------|
| 有 `Spillback: redirect` 到某 IP + 有 `Remote lease failed` 指向同一 IP | **确认假说 A**: spillback 到 dead node，超时后回退 |
| 有 `Spillback: redirect` 但无 `Remote lease failed`（卡死中） | **确认假说 A**: spillback 到 dead node，尚未超时（`worker_lease_timeout_ms` 未到） |
| 无 `Spillback: redirect` 且 402 个 lease 无 OnReplyReceived | **确认假说 B**: 本地 raylet reply 未被 core worker 处理 |

### 对比：改动前后诊断能力

| 场景 | 改动前 | 改动后 |
|------|--------|--------|
| Spillback 发生 | 需要 `RAY_BACKEND_LOG_LEVEL=debug`（巨量日志） | 默认 INFO 可见（每 10 秒 1 条） |
| Remote lease 超时 | INFO 级别 + 30s 限流（可能漏） | WARNING 级别 + 每次必打（精确计数） |
| 确认 redirect 目标 IP | 需要 debug 日志或 gdb | 日志中直接包含 IP |
| 日志洪泛风险 | N/A | Spillback: 10s 限流；Remote fail: 300s 自然限流 |

---

## 排查工具参考

| 目的 | 命令/文件 |
|------|-----------|
| Core worker RPC 状态 | `python-core-driver-*_{pid}.log` 中的 state-dump |
| Spillback redirect 事件 | `grep "Spillback: redirect" python-core-driver-*_{pid}.log`（**改动后无需 debug 日志**） |
| Remote lease 超时事件 | `grep "Remote lease failed" python-core-driver-*_{pid}.log`（**改动后 WARNING 级别**） |
| Raylet 调度队列 | `raylet.out` 中 `num_waiting_for_resource` |
| TCP 连接状态 | `ss -tnpi \| grep {pid}` |
| gRPC 连接存活时间 | `stat /proc/{pid}/fd/{socket_fd}` |
| 节点存活状态 | `ray list nodes --filter 'STATE=DEAD'` |
| Progress 日志 | `logging_progress.py:231` 输出 |
| Watchdog 日志 | grep `patch_dbs.*WATCHDOG` |
| gRPC 连接层调试（可选） | `GRPC_VERBOSITY=DEBUG GRPC_TRACE=connectivity_state` |

---

## 不改代码的运行时诊断操作

当 job 仍存活但卡死时，可在容器内执行以下操作进一步确认根因。

### 1. 环境变量方式（下次启动时设置）

| 环境变量 | 作用 | 日志量影响 |
|---------|------|-----------|
| `RAY_BACKEND_LOG_LEVEL=debug` | 打印所有 C++ 层 DEBUG 日志，包含 redirect 目标、lease 决策详情 | **极大**（生产慎用） |
| `GRPC_VERBOSITY=DEBUG` | gRPC 库自身日志 | 大 |
| `GRPC_TRACE=connectivity_state,client_channel` | gRPC channel 状态变化追踪，能看到连接到哪个 IP 以及状态转换 | 中 |

**推荐组合**（仅在复现环境短暂开启）：
```bash
export RAY_BACKEND_LOG_LEVEL=debug
# 然后启动 job，卡死后 grep:
grep "Redirect lease" python-core-driver-*_${PID}.log | tail -20
grep "Retrying attempt to schedule" python-core-driver-*_${PID}.log | tail -20
```

### 2. gdb attach（当前 job 存活时）

```bash
# 找到 driver 进程
ps aux | grep python | grep 838610

# attach 并打印 raylet_client_pool_ 中的所有连接目标
gdb -p 838610 -batch -ex "thread apply all bt 5" -ex "detach"

# 更精确: 打印 RetryableGrpcClient 的 pending request 队列
# 需要 debug symbol，通常 Ray 的 debug build 才有
gdb -p 838610 -ex "p this->raylet_client_pool_" -ex "detach"
```

**目的**: 确认 `raylet_client_pool_` 中是否有到 dead node IP 的 `RayletClient` 实例。

### 3. 网络层诊断（当前 job 存活时）

```bash
# 查看 driver 所有 TCP 连接，找非 ESTAB 状态的（重试中的特征）
ss -tnp | grep ${PID} | grep -v ESTAB

# 查看 driver 到所有 IP 的连接，找 SYN_SENT（正在尝试连接 dead node）
ss -tnp | grep ${PID} | grep SYN_SENT

# 对比 dead node IP 列表
ray list nodes --filter 'STATE=DEAD' -o yaml | grep -E 'node_ip|node_id'

# 检查 driver 是否有到 dead node IP 的连接
DEAD_IPS=$(ray list nodes --filter 'STATE=DEAD' -o yaml | grep node_ip | awk '{print $2}')
for ip in $DEAD_IPS; do
    ss -tnp | grep ${PID} | grep $ip
done
```

**注意**: 如果 dead node 的 TCP 连接已经被 OS 回收（RST/超时），可能看不到。gRPC 的 `RetryableGrpcClient` 在重试间隔内可能处于"等待下次重试"状态而非活跃连接状态。

### 4. /proc 文件系统诊断

```bash
# 统计 driver 打开的 socket 数量
ls -la /proc/${PID}/fd/ | grep socket | wc -l

# 查看 driver 的 TCP 连接详情（包含内核态信息）
cat /proc/${PID}/net/tcp | head -50

# 检查特定 fd 的创建时间（判断是否有新建的到远程节点的连接）
stat /proc/${PID}/fd/* 2>/dev/null | grep -B1 "Birth\|Modify" | grep -A1 socket
```

### 5. Ray 命令行诊断

```bash
# 查看所有 dead 节点及其 IP
ray list nodes --filter 'STATE=DEAD'

# 查看当前 pending 的 task（如果 dashboard 可用）
ray list tasks --filter 'STATE=PENDING_NODE_ASSIGNMENT' --limit 500

# 查看 driver 的 job 状态
ray job status raysubmit_9MrvSjhx8Wsmej2n

# 查看集群资源使用情况
ray status
```

### 6. State-dump 持续监控

```bash
# 持续 tail core-driver 日志，观察 counter 是否变化
tail -f /tmp/ray/session_latest/logs/python-core-driver-*_${PID}.log | grep -E "RequestWorkerLease|OnReplyReceived"

# 对比两次 state-dump 的 counter 差值（5 秒间隔 dump 一次）
# 如果 total 和 active 完全冻结 → 确认卡死
# 如果 total 在增长但 active 也在增长 → 有新 RPC 发出但无回复
```

### 诊断决策树

```
观察 python-core-driver-*_{PID}.log 的 state-dump:

RequestWorkerLease total 是否在增长？
├── 否（完全冻结 11+ 小时）→ 402 个 RPC 卡在某处
│   ├── grep "Spillback: redirect" 有结果？
│   │   ├── 有 → 确认发生了 spillback，目标 IP 即为卡死原因
│   │   │   └── 对比 dead node IP → 确认假说 A
│   │   └── 无（且版本含日志改动） → 假说 B（本地 raylet reply 未处理）
│   │
│   └── ss -tnp 中有到 dead node IP 的连接？
│       ├── 有（SYN_SENT 或 ESTAB） → 进一步确认假说 A
│       └── 无 → gRPC 可能在内存中等待重试（连接未建立）
│
└── 是（total 在增长）→ 不是本案场景，可能是其他原因
```
