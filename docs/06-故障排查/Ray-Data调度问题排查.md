# Ray Data 作业调度问题排查报告

**日期**: 2026-05-14
**集群**: kml-hb2az1-l3-2 / namespace: lmserving
**Head 节点**: kml-task-661218-record-15697521-prod-worker-0-98h59 (IP: 10.81.0.20)
**Ray 版本**: 2.52.1（内部定制版）

---

## 一、问题描述

Ray Data 作业中出现两个调度异常：

1. **4800 个 `FlatMap(ClipMergeMapper)` task 处于 "waiting for scheduling" 状态**，长时间无法被调度执行。
2. **`MapWorker(MapBatches(DistributedQwenVLVideoProcessMapper))` GPU actor（ID: `e5616cefce97f0d745206f4d07000000`）** 处于 RESTARTING 状态超过 14 小时，且 Dashboard 显示其 Node ID 为 head 节点，疑似被调度到了 `--num-gpus=0` 的 head 节点上。

---

## 二、集群资源概况

### 初始状态

| 资源类型 | 已使用 | 总量 | 剩余 | 使用率 |
|---------|-------|------|------|--------|
| CPU | 36,073 | 49,100 | 13,027 | 73.5% |
| GPU | 499 | 500 | 1 | 99.8% |
| NPU | 0 | 1 | 1 | 0% |
| Memory | 184.64 TiB | 196.79 TiB | 12.15 TiB | 93.8% |
| Object Store | 561.74 GiB | 33.07 TiB | - | 1.7% |

### 补充节点后

| 资源类型 | 已使用 | 总量 | 剩余 | 使用率 |
|---------|-------|------|------|--------|
| CPU | 38,294 | 55,530 | 17,236 | 68.9% |
| GPU | 499 | 502 | 3 | 99.4% |
| Memory | 206.32 TiB | 218.83 TiB | 12.51 TiB | 94.3% |

集群活跃节点数：**1300+**

### Head 节点启动参数

```bash
ray start --head --port=6379 --dashboard-host=0.0.0.0 --block \
  --num-cpus=0 --num-gpus=0 \
  --system-config='{
    "raylet_report_resources_period_milliseconds": 20000,
    "ray_syncer_message_refresh_interval_ms": 60000,
    "gcs_resource_broadcast_max_batch_size": 1500,
    "gcs_resource_broadcast_max_batch_delay_ms": 1500,
    "health_check_period_ms": 10000,
    "health_check_failure_threshold": 10,
    "gcs_server_rpc_server_thread_num": 64,
    "scheduler_avoid_gpu_nodes": false,
    "event_stats_print_interval_ms": 180000,
    "task_events_report_interval_ms": 10000
  }'
```

---

## 三、问题一：4800 个 ClipMergeMapper Task 等待调度

### 3.1 作业代码

```python
ds = ds.flat_map(
    ClipMergeMapper(config=clip_merge_config),
    concurrency=cpu_concurrency,
    num_cpus=merge_num_cpus,    # 值为 1
    memory=merge_memory,         # 值为 10GB
)
```

### 3.2 Task vs Actor 判定

Ray Data 中：
- 传 **类**（如 `map_batches(ClassName)`）→ 创建 **Actor**（MapWorker）
- 传 **实例/函数**（如 `flat_map(ClassName(config=...))`）→ 创建 **Task**

`ClipMergeMapper(config=clip_merge_config)` 传入的是实例，因此创建的是 **Task**，不是 Actor。

### 3.3 Task 调度机制

Task 调度是**去中心化**的，由提交 task 的 worker 所在节点的**本地 raylet** 直接调度，**不经过 GCS 也不经过 Head 节点**。

```
Worker 提交 task → 本地 raylet → ClusterLeaseManager::GetBestSchedulableNode
  → 本地执行 或 spillback 到其他节点
```

### 3.4 排查过程

#### 3.4.1 排除 GPU 瓶颈

`ray status` 显示的 Pending Demands：

```
Pending Demands:
{'CPU': 1.0, 'GPU': 0.5}: 2+ pending tasks/actors
```

此 Pending Demand 是其他 GPU actor 的需求，**不是 ClipMergeMapper 的**。ClipMergeMapper 只需要 CPU + Memory，不需要 GPU。

#### 3.4.2 定位内存瓶颈

通过 `ray.available_resources()` 和 `ray.cluster_resources()` 获取集群资源：

- 集群总剩余内存：**12.15 TiB = 12,442 GB**
- 活跃节点数：**~1,300 个**
- 每节点平均剩余内存：12,442 GB / 1,300 ≈ **9.6 GB/节点**
- 每个 ClipMergeMapper 需要：**10 GB/task**

**9.6 GB < 10 GB → 几乎没有任何节点能容纳一个新的 ClipMergeMapper task。**

### 3.5 根因

**节点级内存碎片化**。虽然全局聚合还有 12 TiB 剩余内存，但分散到 1300+ 个节点后，每个节点平均剩余不足 10 GB。Ray scheduler 按节点逐个检查是否能放下 task（需要同一节点同时满足 1 CPU + 10 GB memory），绝大多数节点的剩余内存已不足 10 GB，导致 4800 个 task 无法被调度。

### 3.6 解决方案

| 优先级 | 方案 | 说明 |
|-------|------|------|
| P0 | **降低 `merge_memory`** | 如果 ClipMergeMapper 实际峰值内存不到 10 GB，降到 5-8 GB 让 task 能在更多节点上调度 |
| P1 | **释放集群内存** | 检查占用大量内存的其他 actor/task，特别是 GPU serving 相关的 actor |
| P2 | **减少并发 task 数** | 降低 `cpu_concurrency`，减少同时 pending 的 task 数，降低 scheduler 压力 |
| P3 | **扩容节点** | 增加更多 worker 节点，提供更多可用内存 |

---

## 四、问题二：GPU Actor 卡在 RESTARTING 状态

### 4.1 Actor 信息

```
Actor ID:     e5616cefce97f0d745206f4d07000000
Class:        MapWorker(MapBatches(DistributedQwenVLVideoProcessMapper))
State:        RESTARTING
Job ID:       07000000
Required:     {'GPU': 0.5, 'CPU': 1.0}
Runtime Env:  {"env_vars": {"PYTHONPATH": "/ytech_m2v5_hdd/shiyanpeng03/kling-ray"},
               "working_dir": "gcs://_ray_pkg_dc3414666d1b4612.zip"}
num_restarts: 1
```

### 4.2 原始节点死亡原因

Actor 原运行在节点 `d537b84...`（IP: `10.48.34.143`）。

```
Alive: False
DeathReason: 1
DeathReasonMessage: received SIGTERM
```

节点被外部系统强制终止（SIGTERM），可能是 K8s Pod 驱逐、节点缩容或维护。

### 4.3 Dashboard 显示 Head Node ID 的原因

Dashboard `/logical/actors` 页面显示该 actor 的 Node ID 为 head 节点 `b285183f...`，但 IP 为 `-`（空），Memory 显示 `NaN%`。

**actor 并没有真正调度到 head 上运行。** 这是因为：

1. Actor 处于 RESTARTING 状态，并未实际运行在任何节点上
2. GCS（运行在 head 节点）负责管理 actor 的重启流程
3. Dashboard 显示的 Node ID 是 GCS owner/manager 节点的 ID
4. 空的 IP 和 NaN 的 Memory 也证实 actor 并未实际运行

### 4.4 GCS 调度死循环分析

通过查看 GCS 日志定位到调度过程：

```bash
grep -i 'e5616cefce97f0d745206f4d07000000' /tmp/ray/session_latest/logs/gcs_server.out
```

日志揭示了一个**持续 6+ 小时的低效调度循环**：

```
06:31 → head raylet → spillback 到 f3a87a...(worker) → reject(resources not enough)
06:31 → head raylet → 06:38 完成 → spillback 到 b2a931...(worker) → 06:51 reject
06:51 → head raylet → 06:58 完成 → spillback 到 e826f0...(worker) → 08:36 reject
08:36 → head raylet → 08:45 完成 → spillback 到 079778...(worker) → 08:54 reject
08:54 → head raylet → 09:03 完成 → spillback 到 af126e...(worker) → 09:05 reject
09:05 → head raylet → 09:14 完成 → spillback 到 520113...(worker) → 09:54 reject
09:54 → head raylet → 10:04 完成 → spillback 到 e6ecde...(worker) → 11:07 reject
11:07 → head raylet → 11:17 完成 → spillback 到 8a26ef...(worker) → 12:11 reject
12:11 → head raylet → 12:20 完成 → spillback 到 2c86d2...(worker) → 12:20 成功 ✓
```

### 4.5 源码级调度流程分析

#### Step 1: GCS `SelectForwardingNode`

文件：`src/ray/gcs/actor/gcs_actor_scheduler.cc:83-99`

```cpp
NodeID GcsActorScheduler::SelectForwardingNode(std::shared_ptr<GcsActor> actor) {
  if (!lease_spec.GetRequiredResources().IsEmpty()) {
    // 有资源需求的 actor，优先选 owner 节点
    auto maybe_node = gcs_node_manager_.GetAliveNode(actor->GetOwnerNodeID());
    node = maybe_node.has_value() ? maybe_node.value()
                                  : gcs_node_manager_.SelectRandomAliveNode();
  }
}
```

**关键点**：GCS 不自己做调度决策，而是把 lease 请求**转发到 owner 节点的 raylet**，让该 raylet 利用集群资源视图来选目标节点。

由于 Ray Data 的 driver 运行在 head 节点上，所有 MapWorker actor 的 owner 都是 head 节点，所以 `SelectForwardingNode` **每次都先选 head 节点**。

#### Step 2: Head Raylet 做调度决策

Head raylet 收到 lease 请求后，在 `ClusterLeaseManager::ScheduleAndGrantLeases`（`cluster_lease_manager.cc:196-296`）中调用 `GetBestSchedulableNode` 查找可调度节点。

Head 本地没有 GPU（`--num-gpus=0`），所以在集群资源视图中找到一个看起来有空闲 GPU 的 spillback 节点，通过 `retry_at_raylet_address` 返回给 GCS。

首次 lease 时 `SetGrantOrReject(false)`（`gcs_actor_scheduler.cc:79`），不要求本地必须有资源。

#### Step 3: GCS 转发到 Spillback 节点

文件：`gcs_actor_scheduler.cc:296-325`

GCS 收到 spillback 回复后，将 lease 请求转发到 spillback 节点，并设置 `SetGrantOrReject(true)`（`gcs_actor_scheduler.cc:318`）。

**`grant_or_reject=true` 意味着**：spillback 节点必须要么本地有资源直接 grant，要么直接 reject，**不会再次 spillback**。

#### Step 4: Spillback 节点 Reject

文件：`cluster_lease_manager.cc:429-435`

```cpp
void ClusterLeaseManager::ScheduleOnNode(const NodeID &spillback_to,
                                         const std::shared_ptr<internal::Work> &work) {
  if (work->grant_or_reject_) {
    // grant_or_reject=true，本地资源不足直接 reject
    for (const auto &reply_callback : work->reply_callbacks_) {
      reply_callback.reply_->set_rejected(true);
      reply_callback.send_reply_callback_(Status::OK(), nullptr, nullptr);
    }
    return;
  }
}
```

Spillback 节点本地 GPU 已被占用 → 直接 reject。

#### Step 5: GCS 收到 Reject，回到 Step 1

文件：`gcs_actor_scheduler.cc:601-610`

```cpp
void GcsActorScheduler::HandleWorkerLeaseRejectedReply(
    std::shared_ptr<GcsActor> actor, const rpc::RequestWorkerLeaseReply &reply) {
  actor->UpdateAddress(rpc::Address());
  Reschedule(actor);  // → Schedule(actor) → SelectForwardingNode → 又选 owner(head)
}
```

`Reschedule` → `Schedule` → `SelectForwardingNode` → **再次选中 head 节点** → 完整循环重复。

### 4.6 为什么有 1 个空闲 GPU 却调度不上？

根因是 **Head 节点 raylet 的集群资源视图过期**。

配置值：
- `raylet_report_resources_period_milliseconds: 20000`（资源上报周期 20 秒）
- `ray_syncer_message_refresh_interval_ms: 60000`（Syncer 刷新间隔 60 秒）

在 1300+ 节点的大集群中，head raylet 看到的资源视图可能滞后 20~60 秒。它选的 spillback 节点在被选中时看起来有空闲 GPU，但实际上 GPU 已被其他 actor 占用。到 spillback 节点后用 `grant_or_reject=true` 检查真实资源 → reject。

### 4.7 为什么每轮循环耗时 8~60 分钟？

从日志看每轮循环耗时远超预期，原因：

1. **Head 节点 raylet 调度队列积压** — Ray Data job 的所有 MapWorker actor 的 owner 都是 head 节点，lease 请求全部堆在 head raylet 的调度队列中。从日志看 head raylet 的 lease 完成时间：
   - `06:31 → 06:38`（7 分钟排队）
   - `08:36 → 08:45`（9 分钟排队）
   - `11:07 → 11:17`（10 分钟排队）

2. **每轮必经 head 节点** — `SelectForwardingNode` 固定先选 owner（head），每次 reject 后都回到 head 排队：
   ```
   head 排队(8~10分钟) → spillback 节点 reject(秒级) → 回到 head 排队(8~10分钟) → ...
   ```

3. **资源视图延迟放大效果** — 20s/60s 的上报间隔导致 head 持续选错 spillback 节点，每次选错就浪费一整轮循环。

### 4.8 补充节点后为什么还卡了一段时间？

补充新 GPU 节点后（集群从 500 GPU 增加到 502 GPU），actor 仍需等待 GCS scheduler 轮到下一轮尝试。从日志看：
- 12:11 最近一次 worker reject
- 12:11→12:20 又经过一轮 head 节点 lease（~9 分钟排队）
- **12:20:06** 终于选中了新的可用节点 `2c86d26a...`
- **12:20:09** Actor 创建成功

### 4.9 解决方案

#### 配置优化（立即可做）

| 参数 | 当前值 | 建议值 | 原因 |
|------|--------|--------|------|
| `raylet_report_resources_period_milliseconds` | 20000 | **5000** | 加快资源视图更新频率，减少 spillback 选错节点的概率 |
| `ray_syncer_message_refresh_interval_ms` | 60000 | **10000** | 加快节点间资源同步 |

#### 架构优化

| 方案 | 说明 | 效果 |
|------|------|------|
| **Driver 不跑在 head 上** | 从 worker 节点提交 job（`ray job submit --address=...`），让 driver owner 分散 | actor scheduling 不再全部压在 head raylet |
| **减少同时 pending 的 actor 数** | 降低 `concurrency` | 减轻 head raylet 排队压力 |

#### 代码层面可改进点

1. **`SelectForwardingNode` 不应总选 owner 节点** — 对于 RESTARTING 的 actor，owner 节点（head）明确没有所需资源（GPU），应该直接用集群资源视图选最佳节点，而不是每次都走 head → spillback 的两跳路径。
2. **`grant_or_reject=true` 阻断了二次 spillback** — spillback 节点如果 reject，应可尝试其他节点而不是回到 owner。当前代码（`cluster_lease_manager.cc:429-435`）直接 reject 返回 GCS，没有二次 spillback 机会。
3. **应将已知无资源的节点排除** — head 节点设了 `--num-gpus=0`，但 GCS scheduler 仍然反复尝试在 head 上 lease，每次都浪费 8~10 分钟的排队时间。

---

## 五、Ray 调度架构总结

### 5.1 Task 调度（去中心化）

```
Worker 提交 task
  → 本地 raylet (ClusterLeaseManager)
  → GetBestSchedulableNode（基于集群资源视图）
  → 本地执行 或 spillback 到其他节点的 raylet
```

**不经过 GCS，不经过 Head 节点。**

### 5.2 Actor 创建/重启（中心化，经过 GCS）

```
GCS Actor Manager 触发 actor 创建/重启
  → GCS Actor Scheduler::SelectForwardingNode
     → 优先选 owner 节点（actor 的 driver 所在节点）
  → GCS 发送 RequestWorkerLease RPC 到 owner 节点的 raylet
  → Owner 节点 raylet 做调度决策:
     → 本地有资源 → grant
     → 本地无资源 → 在集群视图中选 spillback 节点，返回 retry_at_raylet_address
  → GCS 转发到 spillback 节点（grant_or_reject=true）
     → 有资源 → grant，actor 创建成功
     → 无资源 → reject，回到 GCS 重新 SelectForwardingNode
```

### 5.3 Actor 上的 Task 调用（直接通信）

```
Caller worker → 直接 RPC → Actor worker 进程
```

**Actor 创建完成后，调用 actor method 不经过 GCS，不经过 Head。**

### 5.4 Ray Data 中 Task vs Actor 的区分

| API 用法 | 传参方式 | 创建类型 | 调度方式 |
|----------|---------|---------|---------|
| `ds.flat_map(Fn(config=...))` | 传实例/函数 | Task | 本地 raylet |
| `ds.map_batches(ClassName)` | 传类 | Actor (MapWorker) | GCS → owner raylet |

---

## 六、排查方法汇总

### 6.1 查看集群资源状态

```bash
ray status
```

关注 Resources 部分的 Total Usage 和 Pending Demands。

### 6.2 查看可用/总资源（Python API）

```python
import ray
ray.init(address='auto')
print("Available:", ray.available_resources())
print("Total:", ray.cluster_resources())
```

### 6.3 查看 actor 详细信息

```python
from ray.util.state import list_actors
actors = list_actors(
    filters=[('actor_id', '=', '<actor_id>')],
    detail=True,
    raise_on_missing_output=False
)
for a in actors:
    print(f'state={a.state}, node_id={a.node_id}, '
          f'required_resources={a.required_resources}, '
          f'num_restarts={a.num_restarts}')
```

### 6.4 查看节点死亡原因

```python
import ray
ray.init(address='auto')
nodes = ray.nodes()
dead = [n for n in nodes if n['NodeID'] == '<node_id>']
if dead:
    print('Alive:', dead[0]['Alive'])
    print('DeathReason:', dead[0].get('DeathReason'))
    print('DeathReasonMessage:', dead[0].get('DeathReasonMessage'))
```

### 6.5 查看 Head 节点 ID

```python
import ray
ray.init(address='auto')
nodes = ray.nodes()
head = [n for n in nodes if n.get('Resources', {}).get('node:__internal_head__')]
if head:
    print('Head NodeID:', head[0]['NodeID'])
    print('Head IP:', head[0]['NodeManagerAddress'])
```

### 6.6 查看 GCS Actor 调度日志

```bash
# 查看特定 actor 的调度过程
grep '<actor_id>' /tmp/ray/session_latest/logs/gcs_server.out

# 关键日志模式:
# "Leasing worker for actor" → GCS 发起 lease 请求
# "Finished leasing worker" → lease 完成（可能是 spillback）
# "Failed to lease worker ... resources are not enough" → spillback 节点 reject
# "Actor creation task succeeded" → actor 创建成功
```

### 6.7 查看有空闲 GPU 的节点

```python
import ray
ray.init(address='auto')
for n in ray.nodes():
    r = n.get('Resources', {})
    gpu = r.get('GPU', 0)
    if gpu > 0 and n['Alive']:
        print(f"IP={n['NodeManagerAddress']}  GPU={gpu}  CPU={r.get('CPU',0)}")
```
