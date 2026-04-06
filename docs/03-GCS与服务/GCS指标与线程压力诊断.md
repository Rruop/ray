# GCS Server 指标含义与线程压力诊断深度分析

> 相关文档：[RaySyncer 资源同步机制与配置参数深度分析](./ray-syncer-resource-sync-mechanism.md)

## 一、GCS 核心指标含义

### 1.1 业务指标

| 指标 | 类型 | 含义 |
|------|------|------|
| `actors{State=...}` | Gauge | 各状态 Actor 数量（DEPENDENCIES_UNREADY/PENDING_CREATION/ALIVE/DEAD 等） |
| `gcs_actors_count{State=...}` | Gauge | Actor 统计（Created/Destroyed/Unresolved/Pending） |
| `running_jobs` | Gauge | 当前运行 Job 数 |
| `finished_jobs` | Count | 已完成 Job 数 |
| `placement_groups{State=...}` | Gauge | 各状态 PG 数量 |
| `gcs_placement_group_creation_latency_ms` | Histogram | PG 创建端到端延迟 |
| `gcs_placement_group_scheduling_latency_ms` | Histogram | PG 调度延迟 |
| `scheduler_placement_time_ms{WorkloadType=...}` | Histogram | 从依赖解析到资源预留的调度耗时 |

### 1.2 存储/Redis 指标

| 指标 | 类型 | 含义 |
|------|------|------|
| `gcs_storage_operation_latency_ms{Operation=...}` | Histogram | 存储操作延迟（Put/Get/GetAll/Delete 等） |
| `gcs_storage_operation_count{Operation=...}` | Count | 存储操作计数 |
| `gcs_latency{CustomKey=...}` | Histogram | Redis 操作延迟 |

Operations tracked: Put, Get, GetAll, MultiGet, Delete, BatchDelete, GetKeys, Exists

### 1.3 健康检查指标

| 指标 | 类型 | 含义 |
|------|------|------|
| `health_check_rpc_latency_ms` | Histogram | 健康检查 RPC 延迟 |
| `node_failures_total` | Count | 节点故障总数 |
| `unintentional_worker_failures_total` | Count | 非预期 Worker 故障数 |

### 1.4 事件循环指标（线程压力诊断核心）

| 指标 | 类型 | 含义 |
|------|------|------|
| `io_context_event_loop_lag_ms{Name=...}` | Gauge | 事件从 post 到执行的延迟（直接反映线程压力） |
| `operation_queue_time_ms{Name=...}` | Histogram | 操作排队等待时间 |
| `operation_run_time_ms{Name=...}` | Histogram | 操作执行时间 |
| `operation_active_count{Name=...}` | Gauge | 当前活跃操作数（队列深度） |
| `operation_count{Name=...}` | Count | 操作总计数 |

其中 Name 标签对应各 io_context：gcs_server(主线程)、ray_syncer_io_context、task_io_context、pubsub_io_context、ray_event_io_context

### 1.5 任务事件指标

| 指标 | 类型 | 含义 |
|------|------|------|
| `gcs_task_manager_task_events_reported` | Gauge | 报告到 GCS 的所有任务事件数 |
| `gcs_task_manager_task_events_dropped` | Gauge | 按类型丢弃的任务事件数（PROFILE_EVENT, STATUS_EVENT） |
| `gcs_task_manager_task_events_stored` | Gauge | GCS 中存储的任务事件数 |

### 1.6 事件记录指标

| 指标 | 类型 | 含义 |
|------|------|------|
| `ray_event_recorder_dropped_events` | Count | 事件记录器丢弃的事件数 |

### 1.7 调度指标

| 指标 | 类型 | 含义 |
|------|------|------|
| `scheduler_placement_time_ms{WorkloadType=...}` | Histogram | 工作负载从依赖解析到资源预留的放置时间 |


---

## 二、三个延迟指标之间的关系

事件循环延迟、操作排队时间、健康检查延迟之间是**因果关系链**，从底层到上层：

- 操作排队时间 (operation_queue_time_ms)：单个操作从 post() 到开始执行的等待时间，直接原因是队列中排在前面的操作太多/执行太慢
- 事件循环延迟 (io_context_event_loop_lag_ms)：事件循环整体响应能力，通过周期性探测任务测量，反映的是 io_context 线程的"繁忙程度"，lag 高意味着所有 post 到该线程的任务都会延迟
- 健康检查延迟 (health_check_rpc_latency_ms)：端到端延迟 = 网络往返 + Raylet 处理 + GCS 主线程回调处理，前两者通常稳定，主要变量是 GCS 主线程回调延迟，而主线程回调延迟 = 操作排队时间 + 执行时间

因果关系链：操作排队时间升高 -> 事件循环延迟升高 -> 健康检查延迟升高 -> 节点误判死亡

**关键点**：健康检查的回调处理在**主线程 (gcs_server 默认 io_context)** 上执行，不是在 ray_syncer 线程上。所以只有主线程事件循环延迟才会直接影响健康检查。

---

## 三、post 之后不会立即执行

post() 只是把任务放入 io_context 的队列，**不会立即执行**。单线程 io_context 模型下，任务必须等前面所有任务执行完才会被轮到。这就是排队时间的来源。

---

## 四、各指标的具体统计机制

### 4.1 统计流程总览

post(handler, name) 执行时：

1. **RecordStart(name)**：记录 start_time = 当前时间，curr_count++（队列深度+1），cum_count++（累计计数+1），输出 operation_active_gauge = curr_count，输出 operation_count += 1
2. 任务在队列中等待（排队时间）
3. **RecordExecution(handler)**：任务开始执行，running_count++，queue_time = now - start_time（排队时间），输出 operation_queue_time_ms
4. 执行 handler()（执行时间）
5. 执行完毕：curr_count--（队列深度-1），running_count--，execution_time = now - start_execution，输出 operation_run_time_ms，输出 operation_active_gauge = curr_count

### 4.2 队列深度 (operation_active_count)

**统计方式**：RecordStart 时 ++curr_count，RecordExecution 结束时 --curr_count，就是当前已 post 但还没执行完的任务数。

**源码位置**：src/ray/common/event_stats.cc:97-98 (curr_count++)，src/ray/common/event_stats.cc:147 (curr_count--)

```cpp
// event_stats.cc:97-98 - RecordStart 中
++stats->stats.cum_count;
curr_count = ++stats->stats.curr_count;

// event_stats.cc:147 - RecordExecution 中
curr_count = --stats->stats.curr_count;
```

**含义**：当前 io_context 中已 post 但尚未执行完毕的任务数量。curr_count 大 -> 积压严重 -> 后续任务排队时间长 -> 事件循环延迟高。它是**最先升高的指标**，比延迟更早暴露问题，是压力的"前置信号"。

### 4.3 操作排队时间 (operation_queue_time_ms)

**统计方式**：RecordExecution 时计算 start_execution - handle->start_time，即从 post 到开始执行的等待时间。

**源码位置**：src/ray/common/event_stats.cc:142

```cpp
const auto queue_time_ns = start_execution - handle->start_time;
```

### 4.4 操作执行时间 (operation_run_time_ms)

**统计方式**：RecordExecution 中 end_execution - start_execution，handler 实际运行时间。

**源码位置**：src/ray/common/event_stats.cc:139-141

```cpp
int64_t start_execution = ray::current_time_ns();
fn();  // 执行实际函数
int64_t end_execution = ray::current_time_ns();
const auto execution_time_ns = end_execution - start_execution;
```

### 4.5 事件循环延迟 (io_context_event_loop_lag_ms)

**统计方式**：周期性 post 一个探测任务，测量从 post 到实际执行的耗时。是排队时间的低频采样，但覆盖所有事件而非单个。

**源码位置**：src/ray/common/asio/instrumented_io_context.cc:24-30

```cpp
// instrumented_io_context.cc - LagProbeLoop
auto begin = std::chrono::steady_clock::now();
io_context.post(
    [&io_context, begin, interval_ms, context_name]() {
      auto end = std::chrono::steady_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin);
      io_context.io_context_event_loop_lag_ms_gauge_metric.Record(
          duration.count(), {{"Name", context_name.value_or(GetThreadName())}});
    },
    "event_loop_lag_probe");
```

### 4.6 事件循环延迟 vs 排队时间的区别

- operation_queue_time_ms：每个 post 的任务都会记录，是精确的**逐任务**统计
- io_context_event_loop_lag_ms：通过周期性探测任务采样，反映的是**整体**事件循环的响应能力，是低频采样的近似值

### 4.7 指标升高时间顺序

队列深度先升 -> 排队时间后升 -> 事件循环延迟再升 -> 业务指标（健康检查/调度延迟）最后恶化

### 4.8 EventStats 数据结构

```cpp
// src/ray/common/event_stats.h
struct EventStats {
  int64_t cum_count = 0;        // 累计事件数
  int64_t curr_count = 0;       // 当前活跃数（队列深度）
  int64_t cum_execution_time = 0;
  int64_t cum_queue_time = 0;
  int64_t min_queue_time = std::numeric_limits<int64_t>::max();
  int64_t max_queue_time = -1;
  int64_t running_count = 0;    // 正在执行的任务数
};
```

## 五、确认 GCS 线程压力的方法

### 5.1 指标查询（Prometheus）

```promql
# 1. 事件循环延迟 - 最核心指标，>100ms 说明线程压力大
io_context_event_loop_lag_ms{Name=~"gcs_server|ray_syncer_io_context"}

# 2. 操作排队时间 P99
histogram_quantile(0.99, rate(operation_queue_time_ms_bucket{Name="gcs_server"}[5m]))

# 3. 队列深度 - 最早暴露压力的前置信号
operation_active_count{Name=~"gcs_server|ray_syncer_io_context"}

# 4. Actor 创建延迟趋势
histogram_quantile(0.99, rate(scheduler_placement_time_ms_bucket{WorkloadType="Actor"}[5m]))

# 5. 健康检查延迟 - 过高会导致误判节点死亡
histogram_quantile(0.99, rate(health_check_rpc_latency_ms_bucket[5m]))

# 6. 存储操作延迟 - Redis 慢会阻塞 GCS
histogram_quantile(0.99, rate(gcs_storage_operation_latency_ms_bucket[5m]))
```

### 5.2 告警规则

```yaml
- alert: GCSEventLoopLag
  expr: io_context_event_loop_lag_ms{Name=~"gcs_server|ray_syncer_io_context"} > 100
  for: 2m
  labels:
    severity: warning
  annotations:
    summary: "GCS 事件循环延迟过高，可能存在线程瓶颈"

- alert: GCSHealthCheckHighLatency
  expr: histogram_quantile(0.99, rate(health_check_rpc_latency_ms_bucket[5m])) > 5000
  for: 5m
  labels:
    severity: critical
  annotations:
    summary: "健康检查 P99 > 5s，可能误判节点死亡"

- alert: RayNodeFailureRate
  expr: rate(ray_node_failures_total[5m]) > 0.1
  for: 2m
  labels:
    severity: critical
  annotations:
    summary: "Ray 节点故障率过高"
```

### 5.3 命令行诊断

```bash
# 1. 查看 GCS 进程整体资源
ps aux | grep gcs_server

# 2. 查看各线程 CPU 分布 - 定位瓶颈线程
ps -T -p $(pgrep -f gcs_server) -o tid,comm,%cpu | sort -k3 -rn | head -30

# 3. top 按线程查看
top -H -p $(pgrep -f gcs_server)

# 4. 查看 GCS 压力相关日志
grep -E "slow|timeout|backlog|lag|took [0-9]{3,}ms" /tmp/ray/session_latest/logs/gcs_server.out | tail -50

# 5. 心跳延迟日志
grep "lagging heartbeats" /tmp/ray/session_latest/logs/gcs_server.out

# 6. 节点死亡日志
grep -E "Node.*dead|marked dead" /tmp/ray/session_latest/logs/gcs_server.out

# 7. Actor 调度失败日志
grep -E "Failed to lease|Leasing worker|Finished leasing" /tmp/ray/session_latest/logs/gcs_server.out | tail -100

# 8. 通过 Dashboard API 获取事件队列深度
curl http://localhost:8265/metrics | grep -E "io_context_event_loop_lag|operation_queue_time|operation_active_count"

# 9. 使用 perf 分析热点
perf top -p $(pgrep -f gcs_server) -t

# 10. 统计心跳延迟出现频率
grep "lagging heartbeats" /tmp/ray/session_latest/logs/gcs_server.out | cut -d',' -f1 | cut -d':' -f1-2 | uniq -c
```

### 5.4 线程瓶颈定位判断

| 线程 | 高压表现 | 影响 |
|------|----------|------|
| gcs_server(主线程) | CPU > 80%, event_loop_lag > 100ms | Actor 创建慢、心跳响应延迟、PG 调度阻塞 |
| ray_syncer_io_context | CPU 接近 100% | 资源视图同步延迟->调度选错节点->大量 lease 失败 |
| task_io_context | CPU 高 | 任务事件处理积压 |
| pubsub_io_context | CPU 高 | 事件发布延迟 |
| server.poll*(gRPC线程) | CPU 高 | RPC 接收/响应延迟 |

**典型瓶颈模式**：ray_syncer_io_context 单线程打满（O(N) 广播），在 500+ 节点集群最为明显，CPU 接近 100% 后导致资源视图过时，进而引发调度失败、Actor 创建超时、节点误判死亡的级联故障。

---

## 六、健康检查回调与重试风暴都在主线程

### 6.1 健康检查回调在主线程

源码证据链：

**第一步**：GCS Server 创建 HealthCheckManager 时传入的是 GetDefaultIOContext()：

```cpp
// gcs_server.cc:376
gcs_healthcheck_manager_ = GcsHealthCheckManager::Create(
    io_context_provider_.GetDefaultIOContext(),  // <- 主线程
    ...);
```

**第二步**：HealthCheckManager 内部的 io_service_ 就是主线程的 io_context：

```cpp
// gcs_health_check_manager.h:158
instrumented_io_context &io_service_;  // = 主线程
```

**第三步**：健康检查 RPC 回调 post 回主线程：

```cpp
// gcs_health_check_manager.cc:184
// gRPC 回调在 gRPC 线程池执行，但把结果 post 回主线程
gcs_health_check_manager->io_service_.post(
    [this, status, response]() {
      // 这个 lambda 在主线程执行！
      if (status.ok() && response->status() == SERVING) {
        health_check_remaining_ = failure_threshold_;
      } else {
        --health_check_remaining_;
      }
      if (health_check_remaining_ == 0) {
        mgr->FailNode(node_id_);  // 也在主线程
      }
    },
    "HealthCheck");
```

**第四步**：FailNode -> on_node_death_callback_ -> GcsNodeManager::RemoveNode，全部在主线程。

### 6.2 重试风暴也在主线程

Actor 调度重试路径：

```
gRPC client.poll 线程收到 LeaseWorker 回复
  |
  |  main_service_.post(OnReplyReceived)    <- post 到主线程
  v
主线程执行:
  HandleWorkerLeaseReply()                    <- gcs_actor_scheduler.cc
  -> "resources are not enough" 失败
  -> Reschedule()                              <- 又 post 到主线程
  -> LeaseWorkerFromNode()                     <- 又 post 到主线程
  -> gRPC 发出新的 Lease 请求
  -> 回到 gRPC client.poll 线程等回复
  -> 回复后又 post 回主线程
  -> 循环...
```

**关键**：每次重试循环，主线程至少执行 2 次 post 处理（收回复 + 发新请求）。大规模重试时，主线程队列中积压大量回调。

---

## 七、ray_syncer_io_context 打满如何导致级联故障

ray_syncer 和主线程是**独立线程**，syncer 打满**不会直接阻塞**主线程。级联故障走的是**间接路径**：

### 第一层：ray_syncer_io_context 打满 (CPU ~100%)

RaySyncer.BroadcastMessage() O(N) 广播，800节点集群 x 每100ms一次资源更新 = 8000次push/100ms。syncer 来不及处理 -> 消息积压。

### 第二层：GCS 资源视图过时

GcsResourceManager 的视图来自 RaySyncer 同步。syncer 积压 -> GCS 看到的节点资源状态是旧的。例如某节点实际已满，但 GCS 认为它还有资源。

### 第三层：Actor 调度大量失败

GcsActorScheduler 基于过时视图选择节点 -> 发起 LeaseWorker RPC -> raylet 回复 "resources are not enough" -> GCS 收到失败 -> 重新调度 -> 又选错 -> 又失败 -> 循环重试。每次重试都是一次主线程 post() + RPC 收发，大规模重试 = 大量主线程负载。

### 第四层：主线程事件循环延迟升高

主线程 (gcs_server) 串行处理：健康检查回调 + Actor调度 + 节点管理 + 重试处理 + ...。重试风暴占满主线程 -> 队列深度上升 -> 排队时间上升 -> 所有回调都被延迟，包括健康检查回调。

### 第五层：节点误判死亡

GCS 每3s 发一次健康检查 RPC，RPC 本身很快返回，但回调在主线程排队等待处理。主线程积压 -> 回调延迟数秒才被处理 -> GCS 以为节点没有响应 -> 连续超时 -> 判定节点死亡。节点死亡 -> 该节点上所有 Actor 被销毁 -> 更多重启/重调度 -> 主线程压力进一步增大 -> 恶性循环。

### 级联故障核心逻辑

```
syncer 打满 -> 数据过时 -> 调度失败 -> 重试风暴打满主线程 -> 健康检查回调延迟 -> 误判节点死亡 -> 更多 Actor 重启 -> 更大压力
```

**核心逻辑**：syncer 打满导致**数据过时** -> 错误调度 -> 重试风暴打满主线程。主线程一旦积压，所有 post 到它的回调（包括健康检查、Actor 管理、节点管理）都被延迟，形成恶性循环。这是一个通过"数据不正确"而非"直接阻塞"传播的级联故障。

---

## 八、关键源码文件索引

| 文件 | 关键内容 |
|------|----------|
| src/ray/common/asio/instrumented_io_context.h/cc | instrumented_io_context 实现，post/dispatch 统计封装，LagProbeLoop |
| src/ray/common/event_stats.h/cc | EventStats/EventTracker 实现，RecordStart/RecordExecution 统计逻辑 |
| src/ray/common/metrics.h | Prometheus 指标定义（operation_queue_time_ms, operation_active_count 等） |
| src/ray/gcs/gcs_server.cc | GcsServer 初始化，HealthCheckManager 使用 GetDefaultIOContext() |
| src/ray/gcs/gcs_health_check_manager.h/cc | 健康检查实现，回调 post 回主线程 |
| src/ray/gcs/gcs_server_io_context_policy.h | io_context 分配策略，4个专用 io_context 定义 |
| src/ray/gcs/actor/gcs_actor_scheduler.cc | Actor 调度重试逻辑，HandleWorkerLeaseReply |
| src/ray/ray_syncer/ray_syncer.cc | RaySyncer O(N) 广播实现 |
| src/ray/gcs/metrics.h | GCS 专用指标定义 |
