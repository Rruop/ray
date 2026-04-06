# Ray Data 作业 Actor 创建慢问题排查与优化

## 问题现象

Ray Data 作业中 Actor 创建特别慢，集群有大量节点但 Actor 调度效率极低。

## 排查过程

### 第一步：通过 WebShell 连接 Head 节点，检查 GCS 进程状态

```bash
# 连接 head 节点后，检查 GCS 进程
ps aux | grep gcs_server
top -b -n1 -p <gcs_pid>
```

**发现：**
- GCS 进程 CPU 占用 **506.7%**（5 个核），远超正常水平（应 <100%）
- GCS 内存 RSS **15.4 GB**，线程数 **470**
- GCS 日志文件 **101 GB**，磁盘写入 **109 GB**
- 运行时间 ~33 小时，累计 CPU 时间 4500+ 分钟

<details>
<summary>原始日志 - ps / top 输出</summary>

```
$ ps aux | grep gcs_server
root      74  227  3.0 53717424 16234048 ?   Rl   May07 4519:55 /opt/vjepa2/lib/python3.12/site-packages/ray/core/src/ray/gcs/gcs_server ...

$ top -b -n1 -p 74
PID USER      PR  NI    VIRT    RES    SHR S  %CPU  %MEM     TIME+ COMMAND
74  root      20   0   51.2g  15.4g  19164 R 506.7   3.1   4535:31 gcs_server

$ cat /proc/74/status | grep -E 'VmRSS|VmSize|Threads'
VmSize: 53717424 kB
VmRSS:  16302416 kB
Threads: 470

$ ls -lh /tmp/ray/session_2026-05-07_11-59-38_801864_1/logs/gcs_server.out
-rw-r--r-- 1 root root 101G May  8 21:20 gcs_server.out

$ cat /proc/74/io | head -6
rchar: 113139
wchar: 109214158000
read_bytes: 0
write_bytes: 109266448384

$ free -h
              total        used        free      shared  buff/cache   available
Mem:           502Gi        44Gi       231Gi       132Mi       227Gi       454Gi
Swap:             0B          0B          0B
```
</details>

### 第二步：检查 Actor 状态分布

```python
import ray
ray.init(address='auto', namespace='diag', ignore_reinit_error=True)
at = ray._private.state.actors()
from collections import Counter
states = Counter(a.get('State') for a in at.values())
print(states)
# {'PENDING_CREATION': 3268, 'ALIVE': 12733, 'DEAD': 100000}
```

**发现：** 10 万 DEAD actor，3268 个 PENDING actor，仅 12733 个 ALIVE。

<details>
<summary>原始日志 - Actor 状态详情</summary>

```
$ ray list actors --filter 'state=PENDING_CREATION' --limit 3
======== List: 2026-05-08 21:40:00.607569 ========
Stats:
Total: 3
Table:
ACTOR_ID                          CLASS_NAME                STATE               JOB_ID  NAME    NODE_ID      PID  RAY_NAMESPACE
0  0001008cd3129f5a75cafe5713000000  QwenVLCPUPreprocessActor  PENDING_CREATION  13000000                         0  011fe874-f3d2-4635-b552-67073e97478b
1  000f36271fd3721d6dfad90a13000000  QwenVLCPUPreprocessActor  PENDING_CREATION  13000000                         0  011fe874-f3d2-4635-b552-67073e97478b
2  001c509451bae2a6b16d5b0913000000  QwenVLCPreprocessActor  PENDING_CREATION  13000000                         0  011fe874-f3d2-4635-b552-67073e97478b

# Actor class + state breakdown
ActorClassName                                               State                Count
QwenVLCPUPreprocessActor                                     ALIVE                11732
QwenVLCPUPreprocessActor                                     DEAD                 93992
QwenVLCPUPreprocessActor                                     PENDING_CREATION     3268
MapWorker(MapBatches(DistributedQwenVLVideoProcessMapper))   ALIVE                1000
MapWorker(MapBatches(DistributedQwenVLVideoProcessMapper))   DEAD                 6000
JobSupervisor                                                ALIVE                1
JobSupervisor                                                DEAD                 8
_AutoscalingCoordinatorActor                                 ALIVE                1
_StatsActor                                                  ALIVE                1
```
</details>

### 第三步：分析 GCS 日志中的调度行为

```bash
# 统计调度相关日志
grep -c 'resources are not enough' gcs_server.out   # 1.17 亿次
grep -c 'Leasing worker' gcs_server.out              # 每秒约 8700 次
grep -c 'Failed to lease' gcs_server.out              # 每秒约 4350 次（50% 失败率）
```

**发现：** GCS 在疯狂地进行调度自旋——每秒尝试 8700 次 Lease，50% 因 "resources are not enough" 失败后立即重试。

<details>
<summary>原始日志 - GCS 调度自旋</summary>

```
$ grep -c 'resources are not enough' gcs_server.out
116760558

$ grep 'Leasing worker' gcs_server.out | grep '21:20:54' | wc -l
8699

$ grep 'Failed to lease' gcs_server.out | grep '21:20:54' | wc -l
4350

# GCS 日志尾部，可见高频调度循环
[2026-05-08 21:20:54,347 I 74 74] (gcs_server) gcs_actor_scheduler.cc:577: Failed to lease worker from node 615a2081... for actor 14c96c52... as the resources are not enough, job id = 13000000
[2026-05-08 21:20:54,347 I 74 74] (gcs_server) gcs_actor_scheduler.cc:239: Leasing worker for actor. actor_id=14c96c52... job_id=13000000 node_id=8b6f604c...
[2026-05-08 21:20:54,347 I 74 74] (gcs_server) gcs_actor_scheduler.cc:577: Failed to lease worker from node 872c504c... for actor f19ad6e1... as the resources are not enough, job id = 13000000
[2026-05-08 21:20:54,348 I 74 74] (gcs_server) gcs_actor_scheduler.cc:239: Leasing worker for actor. actor_id=f19ad6e1... job_id=13000000 node_id=8e23b18b...
[2026-05-08 21:20:54,348 I 74 74] (gcs_server) gcs_actor_scheduler.cc:577: Failed to lease worker from node 938bd805... for actor f3cbea9c... as the resources are not enough, job id = 13000000
[2026-05-08 21:20:54,348 I 74 74] (gcs_server) gcs_actor_scheduler.cc:239: Leasing worker for actor. actor_id=f3cbea9c... job_id=13000000 node_id=d77ab302...
[2026-05-08 21:20:54,348 I 74 74] (gcs_server) gcs_actor_scheduler.cc:583: Finished leasing worker from e4586bcc... for actor 8950447e..., job id = 13000000
[2026-05-08 21:20:54,348 I 74 74] (gcs_server) gcs_actor_scheduler.cc:583: Finished leasing worker from 50ea4f85... for actor 40118b95..., job id = 13000000
```
</details>

### 第四步：检查集群资源利用率

```bash
ray status 2>&1 | grep -E 'Resources|GPU|memory'
```

```
Resources
500.0/500.0 GPU
151.02TiB/184.62TiB memory
```

**发现：** 表面上 GPU 100% 用完，但实际上资源请求量与集群总资源匹配：

```
{'GPU': 0, 'memory': 120G, 'CPU': 32}: 961 台 CPU 节点
{'GPU': 1, 'memory': 120G, 'CPU': 15}: 501 台 GPU 节点
{'memory': 490G, 'CPU': 120, 'GPU': 0}: 57 台大节点
```

集群物理资源是够的，但 GCS 显示 100% 用尽，说明资源视图有严重延迟或错误。

<details>
<summary>原始日志 - 集群资源状态</summary>

```
$ ray status 2>&1 | grep -E 'Resources|GPU|memory'
Resources
44940.0/44940.0 CPU
500.0/500.0 GPU
151.02TiB/184.62TiB memory
391.07GiB/43.65TiB object_store_memory

# 资源需求
{'GPU': 0, 'memory': 120000000000, 'CPU': 32}: 961 from request_resources()
{'GPU': 1, 'memory': 120000000000, 'CPU': 15}: 501 from request_resources()
{'memory': 490000000000, 'CPU': 120, 'GPU': 0}: 57 from request_resources()

# 通过 Python API 查询节点资源
Total: CPU=44940 GPU=500 MEM=202992.2GB
Avail: CPU=0 GPU=0 MEM=0.0GB
Used:  CPU=44940 GPU=500 MEM=202992.2GB
Util:  CPU=100.0% GPU=100.0% MEM=100.0%
Nodes with ~0 CPU available: 1517/1517
```
</details>

### 第五步：GCS 线程级 CPU 分析（关键定位步骤）

```bash
# 查看各线程 CPU 占用
top -H -b -n2 -d2 -p 74 | awk '/^top/ {snap++} snap==2 && $9>1 {print}'
```

**发现核心瓶颈线程：**

| 线程名 | CPU% | 累计用户态时间 (jiffies) | 职责 |
|--------|------|------------------------|------|
| **`ray_syncer_io_c`** (tid=99) | **94%** | **8,519,449** (最高) | 资源同步广播线程 |
| **`gcs_server`** (tid=74) | **88.6%** | 3,016,019 | GCS 主事件循环 |
| **`pubsub_io_conte`** (tid=98) | **47.3%** | 1,031,900 | 发布订阅 IO |
| `task_io_context` (tid=97) | 10% | 642,420 | 任务 IO |
| `server.poll*` (64 个) | 各 1.5-16% | - | gRPC 服务端线程 |

**关键定位：`ray_syncer_io_c` 是最大瓶颈**，累计 CPU 时间是 gcs_server 主线程的 2.8 倍。

<details>
<summary>原始日志 - 线程级 CPU 快照</summary>

```
$ top -H -b -n1 -p 74 | awk 'NR>7 && $9>10 {print}' | sort -k9 -rn | head -10
 97 root  20  0  51.2g  15.8g  19164 R  99.9  3.1  124:54.04 task_io_context
 99 root  20  0  51.2g  15.8g  19164 R  93.8  3.1   1548:47 ray_syncer_io_c
 74 root  20  0  51.2g  15.8g  19164 R  93.8  3.1   582:30.43 gcs_server
 98 root  20  0  51.2g  15.8g  19164 S  37.5  3.1   223:35.69 pubsub_io_conte
265 root  20  0  51.2g  15.8g  19164 S  12.5  3.1    34:54.59 server.poll39
191 root  20  0  51.2g  15.8g  19164 R  12.5  3.1    56:49.10 nexting_thread
189 root  20  0  51.2g  15.8g  19164 S  12.5  3.1    56:46.67 nexting_thread

# 累计用户态时间 (jiffies) - 确认 ray_syncer 长期最高
$ for tid in 74 97 98 99 265; do echo -n "tid=$tid comm="; cat /proc/74/task/$tid/comm; echo -n " utime="; cat /proc/74/task/$tid/stat | awk '{print $14}'; done
tid=74 comm=gcs_server       utime=3016019
tid=97 comm=task_io_context  utime=642420
tid=98 comm=pubsub_io_conte  utime=1031900
tid=99 comm=ray_syncer_io_c  utime=8519449   ← 最高
tid=265 comm=server.poll39   utime=164761

# 第二次快照（2 秒间隔），确认实时 CPU 占用
$ top -H -b -n2 -d2 -p 74 | awk '/^top/ {snap++} snap==2 && $9>1 {printf "%-8s %6s %6s %s\n", $1, $9"%", $10"%", $12}'
99        94.0%   3.1% ray_syncer_io_c
74        88.6%   3.1% gcs_server
98        47.3%   3.1% pubsub_io_conte
265       16.4%   3.1% server.poll39
97        10.0%   3.1% task_io_context
186        6.5%   3.1% nexting_thread
188        6.5%   3.1% nexting_thread
...
```
</details>

### 第六步：统计 GCS 线程类型分布

```python
import os
from collections import Counter
tasks_dir = '/proc/74/task'
names = Counter()
for tid in os.listdir(tasks_dir):
    with open(f'{tasks_dir}/{tid}/comm') as f:
        names[f.read().strip()] += 1
for name, cnt in names.most_common():
    print(f'{cnt:4d} {name}')
```

```
 150 event_engine        # 事件引擎线程
 133 default-executo     # 线程池默认执行器
  33 pubsub_io_conte     # 发布订阅 IO 连接
  16 nexting_thread      # 内部调度线程
   2 gcs_server          # GCS 主线程
   1 ray_syncer_io_c     # ray_syncer IO 线程 ← 94% CPU
   1 task_io_context     # 任务 IO
   1 ray_event_io_co     # 事件 IO
  64 server.poll*        # gRPC 服务端（= gcs_server_rpc_server_thread_num）
  32 client.poll*        # gRPC 客户端
```

### 第七步：验证 Actor 创建速率

```python
import ray, time
ray.init(address='auto', namespace='diag', ignore_reinit_error=True)
at1 = ray._private.state.actors()
time.sleep(10)
at2 = ray._private.state.actors()
alive1 = sum(1 for a in at1.values() if a.get('State')=='ALIVE')
alive2 = sum(1 for a in at2.values() if a.get('State')=='ALIVE')
print(f'ALIVE: {alive1} -> {alive2} (delta={alive2-alive1})')
# ALIVE: 12749 -> 12749 (delta=0)  ← 10 秒内无新增
```

**发现：** 虽然 GCS 日志显示还在进行 Lease 操作，但 10 秒内 ALIVE 数没有变化——创建几乎停滞。

<details>
<summary>原始日志 - Actor 创建速率验证</summary>

```
# 10 秒前后对比
ALIVE: 12749 -> 12749 (delta=0)
PENDING: 3255 -> 3255 (delta=0)

# PENDING actor 分布在 888 个不同 owner 节点上
PENDING actors by owner NodeID (top 5):
  owner_node=d4e43485a8aefe319400... : 25
  owner_node=09622208f1f5ed23fc5f... : 22
  owner_node=544043dfe2d3a4ecd58e... : 21
  owner_node=ab0be0d465733a6c7d7f... : 21
  owner_node=2762a460770a996af0a1... : 20

# ALIVE preprocess actor 的 owner IP 分布（每个 GPU 节点约 180 个 owner）
ALIVE QwenVLCPUPreprocessActor owner IP distribution (top 5):
  owner_ip=10.48.32.145 : 195
  owner_ip=10.48.32.33 : 190
  owner_ip=10.48.35.88 : 187
  owner_ip=10.48.34.213 : 181
  owner_ip=10.48.74.235 : 180
```
</details>

### 第八步：分析 PENDING Actor 聚集原因

```python
# 检查 PENDING actor 的 owner 节点分布
pending = [a for a in at.values() if a.get('State') == 'PENDING_CREATION']
owner_node = Counter(a.get('OwnerAddress', {}).get('NodeID') for a in pending)
# PENDING 分布到 888 个不同 owner 节点，每个约 3-12 个
```

**源码定位**（`gcs_actor_scheduler.cc:89-106`）：

```cpp
NodeID GcsActorScheduler::SelectForwardingNode(std::shared_ptr<GcsActor> actor) {
    if (!lease_spec.GetRequiredResources().IsEmpty()) {
        // 有资源需求的 actor 优先转发到 owner 所在节点
        auto maybe_node = gcs_node_manager_.GetAliveNode(actor->GetOwnerNodeID());
        node = maybe_node.has_value() ? maybe_node.value()
                                      : gcs_node_manager_.SelectRandomAliveNode();
    } else {
        node = gcs_node_manager_.SelectRandomAliveNode();
    }
}
```

**GCS 不做资源感知调度！** 只是将 actor 转发到 owner 节点或随机节点，实际资源调度由 Raylet 完成。

---

## 根因分析

### 核心问题：`ray_syncer` 资源同步线程过载

当前配置：
```python
{
    "raylet_report_resources_period_milliseconds": 500,
    "ray_syncer_message_refresh_interval_ms": 10000,
    "gcs_server_rpc_server_thread_num": 64,
    "scheduler_avoid_gpu_nodes": false
}
# gcs_resource_broadcast_max_batch_size 未设置（默认 1，即禁用批处理）
# gcs_resource_broadcast_max_batch_delay_ms 未设置（默认 0，即立即发送）
```

### 因果链

```
1517 个节点 × raylet_report_resources_period=500ms
  = 每秒 3034 次资源变更上报到 GCS

GCS 收到变更后需广播给所有 1517 个 raylet
  + batch_size=1, batch_delay=0（零批处理）
  = 每秒 3034 × 1517 = 460 万条 gRPC 写入

→ ray_syncer_io_c CPU 94% 饱和

ray_syncer 过载 → 资源视图同步延迟 → Raylet 看到的集群资源是过期的

Raylet spillback 时使用过期视图 → 找不到有资源的节点 → LeaseFailed
  → GCS 收到 LeaseFailed → Reschedule → 又触发新的资源变更
  → 1.17 亿次 "resources are not enough"
  → 更多广播 → ray_syncer 更忙

→ 恶性循环 → Actor 创建几乎停滞
```

### `ray_syncer_message_refresh_interval_ms` 的作用

源码（`cluster_resource_manager.cc:31-43`）：

```cpp
// Raylet 端的定时刷新逻辑
timer_->RunFnPeriodically(
    [this]() {
        auto syncer_delay = absl::Milliseconds(
            RayConfig::instance().ray_syncer_message_refresh_interval_ms());
        for (auto &[node_id, resource] : received_node_resources_) {
            auto modified_ts = GetNodeResourceModifiedTs(node_id);
            if (modified_ts && *modified_ts + syncer_delay < absl::Now()) {
                AddOrUpdateNode(node_id, resource);  // 重新应用过期资源视图
            }
        }
    },
    RayConfig::instance().ray_syncer_message_refresh_interval_ms(),
    "ClusterResourceManager.ResetRemoteNodeView");
```

这个参数控制：**如果 Raylet 超过该时间未收到某节点的资源更新，则重新应用上次的资源视图**。这是 ray_syncer 协议的消息丢失补偿机制。

**关键约束：** `ray_syncer_message_refresh_interval_ms` 必须 **远大于** `raylet_report_resources_period_milliseconds`，否则 Raylet 会误判节点资源视图过期，频繁触发不必要的刷新操作。

### `gcs_resource_broadcast_max_batch_size` 和 `gcs_resource_broadcast_max_batch_delay_ms`

源码（`ray_syncer_bidi_reactor_base.h:82-115`）：

```cpp
bool PushToSendingQueue(std::shared_ptr<const RaySyncMessage> message) override {
    sending_buffer_[key] = std::move(message);

    // batch_size 达到上限 或 batch_delay=0 时立即发送
    if (sending_buffer_.size() >= max_batch_size_ || max_batch_delay_ms_.count() == 0) {
        StartSend();  // 立即发送
    } else {
        // 启动定时器，延迟发送
        if (!batch_timer_active_) {
            batch_timer_active_ = true;
            batch_timer_.expires_after(max_batch_delay_ms_);
            batch_timer_.async_wait([this](const auto &ec) {
                if (!ec) StartSend();  // 超时后发送
            });
        }
    }
}
```

- `batch_size=1`（默认）：**批处理禁用**，每条资源变更单独发送
- `batch_size>1` + `batch_delay>0`：将多个节点的资源变更合并为一条 gRPC 消息，减少网络 IO

### DEAD Actor 为什么会导致这个问题

**核心结论：DEAD actor 本身不持有资源，但它的产生过程（owner 死亡 → actor 被 kill → raylet 崩溃/重启 → 资源视图剧变）是 ray_syncer 压力的核心来源。**

#### DEAD actor 的死因分析

通过排查发现，93992 个 DEAD 的 `QwenVLCPUPreprocessActor` 几乎全部因为同一个原因死亡：

```
death_cause: "The actor is dead because its owner has died."
NumRestarts: 0（93991 个重启 0 次，1 个重启 1 次）
```

这意味着这些 actor 不是自身 OOM 或异常退出，而是 **创建它们的 owner（MapWorker）先死了，导致它们被级联 kill**。

同时还有 6000 个 DEAD 的 `MapWorker(MapBatches(DistributedQwenVLVideoProcessMapper))`，来自之前已失败的 8 个 job：

```
Jobs: {'12000000': 16000, '0e000000': 16000, '0f000000': 16000,
       '11000000': 16000, '0c000000': 3992, '13000000': 16000,
       '0d000000': 16000, '01000000': 8}
```

并且集群中还出现了 raylet 崩溃：

<details>
<summary>原始日志 - raylet 崩溃堆栈</summary>

```
(raylet, ip=10.56.58.11) [2026-05-08 15:00:23,783 C 159 159] (raylet) node_manager.cc:3309:
  An unexpected system state has occurred. Check failed: statussetor3309.has_value()
  Timed out waiting for file /tmp/ray/session_2026-05-07_11-59-38_801864_1/metrics_agent_port_a71b5ed9...

(raylet, ip=10.57.192.14) [2026-05-08 15:00:26,661 E 159 228] (raylet) agent_manager.cc:87:
  The raylet exited immediately because one Ray agent failed, agent_name = dashboard_agent.
  The raylet fate shares with the agent. This can happen because
  - The version of grpcio doesn't follow Ray's requirement.
  - The agent failed to start because of unexpected error or port conflict.
  - The agent is killed by the OS (e.g., out of memory).
```
</details>

#### 因果链：DEAD actor 如何导致 ray_syncer 压力

```
1. MapWorker(owner) 死亡（job 失败/raylet 崩溃）
   ↓
2. GcsActorManager 检测到 owner 死亡 → 将其所有 QwenVLCPUPreprocessActor 标记为 DEAD
   ↓
3. 对每个 DEAD actor 调用 OnActorDestruction → ReturnActorAcquiredResources
   → 返回资源到 raylet（ReturnWorkerLease RPC）
   ↓
4. raylet 收到 ReturnWorkerLease → 更新本地资源视图（可用资源突然增加）
   ↓
5. raylet 在下一个 report_resources_period (500ms) 上报资源变更到 GCS
   ↓
6. GCS 通过 ray_syncer 广播给 1517 个节点：节点 X 的资源发生了变化
   ↓
7. 所有 raylet 收到广播 → 更新本地集群视图
   ↓
8. 新的 MapWorker 被调度 → 创建新的 QwenVLCPUPreprocessActor
   → 又有大量 actor 同时请求资源 → 调度自旋
   → LeaseFailed → 资源变更 → 又一次广播
   ↓
9. 回到步骤 1（新的一批 actor 又可能因同样的原因死亡）
```

**关键量化：**

- 每次 owner 死亡，平均会级联 kill ~15 个 QwenVLCPUPreprocessActor（93992 / 6000 ≈ 15.7）
- 每次 kill → 资源释放 → 上报 → 广播 → 重新调度 → 又可能失败 → 又触发广播
- 整个过程在 500ms 的 report 周期下被极度放大：**每个周期都可能产生数十到数百次资源变更广播**

#### 为什么 `ReturnActorAcquiredResources` 本身不触发 ray_syncer 广播

源码（`gcs_actor_scheduler.cc:618-620`）：

```cpp
void GcsActorScheduler::ReturnActorAcquiredResources(std::shared_ptr<GcsActor> actor) {
    actor->SetAcquiredResources(ResourceRequest());  // 仅清除 GCS 内存中的 acquired_resources_ 记录
}
```

这个函数只是清除了 GCS 内存中的一个字段，**没有调用任何 ray_syncer 相关接口**。资源释放的传播路径是：

```
GCS → ReturnWorkerLease RPC → raylet → 更新本地资源 → 下次 report 周期上报 → GCS → ray_syncer 广播
```

所以 DEAD actor 的资源释放走的是**正常的周期性资源上报通道**，不会产生额外的即时广播。但问题在于：
- 大量 actor 同时死亡 → 大量 ReturnWorkerLease RPC → raylet 资源视图剧烈变化
- 剧烈变化在每个 500ms 周期被上报 → 每次上报都触发 ray_syncer 广播
- 广播后其他 raylet 更新视图 → 触发新的调度决策 → 可能又失败 → 又触发变更

**本质是：DEAD actor 的产生过程造成了资源视图的高频抖动，在 500ms 上报周期 + 零批处理的配置下，这种抖动被 ray_syncer 放大为每秒 460 万条 gRPC 消息。**

#### `remove_dead_actors()` 是否需要

**不需要为了释放资源而调用**（DEAD actor 不持有资源）。但有两个边际好处：
1. 减小 GCS actor 表遍历开销（当前 116,004 条记录）
2. 让 `ray._private.state.actors()` 等查询更快，间接减轻 GCS 主线程压力

---

## 深入分析：ray_syncer 机制细节

### Raylet 资源上报是否为无条件周期上报？

**不是。Ray 使用版本号差分机制，资源无变更时不会上报。**

源码（`src/ray/raylet/scheduling/local_resource_manager.cc`）：

```cpp
std::optional<syncer::RaySyncMessage> LocalResourceManager::CreateSyncMessage(
    int64_t after_version, syncer::MessageType message_type) const {
  RAY_CHECK_EQ(message_type, syncer::MessageType::RESOURCE_VIEW);
  const_cast<LocalResourceManager *>(this)->UpdateAvailableObjectStoreMemResource();

  if (version_ <= after_version) {
    return std::nullopt;  // 资源无变更，不上报
  }

  syncer::RaySyncMessage msg;
  // ... 构造消息并返回
  msg.set_version(version_);
  return std::make_optional(std::move(msg));
}

void LocalResourceManager::OnResourceOrStateChanged() {
  ++version_;  // 只有资源变更时才递增版本号
  // ...
}
```

**机制说明：**

1. `LocalResourceManager` 维护一个 `version_` 计数器，每次资源变化（分配、释放、增删）时通过 `OnResourceOrStateChanged()` 递增
2. RaySyncer 定时器每个周期调用 `OnDemandBroadcasting(RESOURCE_VIEW)`，内部调用 `CreateSyncMessage(after_version)`
3. 如果 `version_ <= after_version`（自上次上报后无变更），直接返回 `std::nullopt`，不产生任何消息
4. 只有版本号递增后，才生成实际的同步消息并广播

**结论：正常平稳状态下，1517 个节点的资源如果没变化，定时器虽然每个周期都触发，但全部返回 nullopt，几乎零开销。ray_syncer_io_c CPU 很低正是因为资源无变化时不上报。**

本案中 94% CPU 的根因是：大量 actor 同时死亡/创建 → 资源视图高频抖动 → version_ 不断递增 → 每次变更都触发全量广播 → 460 万条/秒 gRPC 消息。

### Raylet 之间资源视图是否必须通过 GCS 同步？

**是的，Ray 的 ray_syncer 架构是 hub-and-spoke 模型，raylet 之间没有直接通信：**

```
raylet → 上报本地资源 → GCS → ray_syncer 广播 → 所有其他 raylet
```

所有资源变更都必须经过 GCS 中转。GCS 是唯一的资源分发中心，承担 N×N 的广播压力。目前不支持 peer-to-peer 资源同步，如需实现需改动 Ray 核心架构。

### GCS 中 Task 状态更新是否在主线程处理？

**不在。`GcsTaskManager` 有自己独立的 `task_io_context` 线程，与 GCS 主事件循环隔离。**

源码（`src/ray/gcs/gcs_task_manager.h`）：

```cpp
/// This class has its own io_context and io_thread, that's separate from other GCS
/// services. All handling of all rpc should be posted to the single thread it owns.
class GcsTaskManager : public rpc::TaskInfoGcsServiceHandler,
                       public rpc::events::RayEventExportGcsServiceHandler { ... };
```

源码（`src/ray/gcs/gcs_server_io_context_policy.h`）：

```cpp
template <typename T>
static constexpr int GetDedicatedIOContextIndex() {
  if constexpr (std::is_same_v<T, GcsTaskManager>) {
    return IndexOf("task_io_context");
  } else if constexpr (std::is_same_v<T, pubsub::GcsPublisher>) {
    return IndexOf("pubsub_io_context");
  } ...
}
```

**Task 状态更新本身不会被 gcs_server 主线程的调度自旋直接阻塞，但间接影响仍然存在：**

- gRPC 线程池（64 个 server.poll 线程）共享同一进程的网络资源，ray_syncer 每秒 460 万条 gRPC 消息会抢占连接和线程
- gcs_server 主线程（88.6% CPU）处理调度自旋，task 状态更新和其他 GCS 请求排队等待主事件循环
- 网络带宽被 ray_syncer 广播占满，其他 RPC 响应变慢

**所以 task 状态更新延迟更主要的原因是 GCS 主线程被调度自旋占满 + gRPC 线程池被 ray_syncer 流量挤占，而非 ray_syncer_io_c 单线程的直接阻塞。**

### GCS 重启后 Raylet 是否会重新上报资源？

**会，通过 RaySyncer 重连机制自动完成：**

1. GCS 重启后，raylet 的 bidi-stream 连接断开
2. raylet 2 秒后自动重连，**重连时推送整个本地集群视图**

源码（`src/ray/ray_syncer/ray_syncer.cc`）：

```cpp
// 重连逻辑
auto reactor = std::make_shared<RayClientBidiReactor>(
    ...
    /* cleanup_cb */
    [this, channel](RaySyncerBidiReactor *bidi_reactor, bool restart) {
      if (restart) {
        execute_after(
            io_context_,
            [this, remote_node_id, channel]() {
              RAY_LOG(INFO).WithField(NodeID::FromBinary(remote_node_id))
                  << "Connection to the node was broken, reconnecting.";
              Connect(remote_node_id, channel);  // 2 秒后重连
            },
            std::chrono::milliseconds(2000));
      }
    }, ...);

// 重连时推送整个集群视图
void RaySyncer::Connect(std::shared_ptr<RaySyncerBidiReactor> reactor) {
  ...
  // Send the view for new connections.
  for (const auto &[_, messages] : node_state_->GetClusterView()) {
    for (const auto &message : messages) {
      if (!message) continue;
      reactor->PushToSendingQueue(message);  // 全量推送
    }
  }
}
```

3. 周期性定时器继续运行，资源上报自然恢复

不需要 raylet 显式做"全量同步"动作，重连即全量推送 + 周期上报，GCS 的资源视图会自动重建。

### GCS 收到 Raylet 的 `HandleNotifyGCSRestart` 后的处理

源码（`src/ray/raylet/node_manager.cc`）：

```cpp
void NodeManager::HandleNotifyGCSRestart(rpc::NotifyGCSRestartRequest request,
                                         rpc::NotifyGCSRestartReply *reply,
                                         rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(INFO)
      << "The GCS has restarted. Resubscribing to pubsub and notifying local workers.";
  gcs_client_.AsyncResubscribe();
  auto workers = worker_pool_.GetAllRegisteredWorkers(/* filter_dead_workers */ true);
  for (const auto &worker : workers) {
    worker->AsyncNotifyGCSRestart();
  }
}
```

GCS 重启时会通知所有 raylet 重新订阅 pubsub，raylet 再通知所有 worker 重连。资源视图则通过上述 RaySyncer 重连机制自动恢复。

---

## 监控指标分析：`ray_io_context_event_loop_lag_ms` 消失问题

### 现象

在排查期间，Prometheus 中查询不到 `ray_io_context_event_loop_lag_ms{Name=~"gcs_server_main_io_context|ray_syncer_io_context"}` 指标，怀疑未上报。

### 指标采集机制

**定义**（`src/ray/common/metrics.h`）：

```cpp
inline ray::stats::Gauge GetIoContextEventLoopLagMsGaugeMetric() {
  return ray::stats::Gauge{
      /*name=*/"io_context_event_loop_lag_ms",
      /*description=*/"The latency of a task from post to execution",
      /*unit=*/"ms",
      /*tag_keys=*/{"Name"},
  };
}
```

**类型：Gauge**，使用 `Aggregation::LastValue()` 聚合。

**探测机制**（`src/ray/common/asio/instrumented_io_context.cc`）：

```cpp
void LagProbeLoop(instrumented_io_context &io_context,
                  int64_t interval_ms,
                  const std::optional<std::string> &context_name) {
  auto begin = std::chrono::steady_clock::now();
  io_context.post(
      [&io_context, begin, interval_ms, context_name]() {
          auto end = std::chrono::steady_clock::now();
          auto duration =
              std::chrono::duration_cast<std::chrono::milliseconds>(end - begin);
          io_context.io_context_event_loop_lag_ms_gauge_metric.Record(
              duration.count(),
              {
                  {"Name", context_name.value_or(GetThreadName())},
              });

          // 上一轮 probe 执行完才调度下一轮
          auto delay = interval_ms - duration.count();
          if (delay <= 0) {
              LagProbeLoop(io_context, interval_ms, context_name);
          } else {
              execute_after(io_context,
                  [&io_context, interval_ms, context_name]() {
                      LagProbeLoop(io_context, interval_ms, context_name);
                  },
                  std::chrono::milliseconds(delay));
          }
      },
      "event_loop_lag_probe");
}
```

**工作机制：**

1. 记录 `begin` 时间戳，向 io_context 事件队列 post 一个 probe 任务
2. 当 probe 被执行时，计算 `end - begin` 的时间差即为事件循环 lag
3. 将 lag 值通过 `Record()` 写入 Gauge 指标
4. 上一轮 probe 执行完后才调度下一轮（串行，非并行）
5. 默认间隔 10 秒（`io_context_event_loop_lag_collection_interval_ms = 10000`）

**各 io_context 的名称**（`src/ray/gcs/gcs_server_io_context_policy.h`）：

```cpp
constexpr static std::array<std::string_view, 4> kAllDedicatedIOContextNames{
    "task_io_context",
    "pubsub_io_context",
    "ray_syncer_io_context",
    "ray_event_io_context"};

// 主线程名称在构造时传入（src/ray/gcs/gcs_server_main.cc）：
instrumented_io_context main_service(
    /*enable_metrics=*/RayConfig::instance().emit_main_service_metrics(),
    /*running_on_single_thread=*/true,
    "gcs_server_main_io_context");
```

### Gauge 指标的注册与导出机制

**View 惰性注册**（`src/ray/stats/metric.cc`）：

```cpp
void Metric::Record(double value, TagsType tags) {
  if (StatsConfig::instance().IsStatsDisabled()) {
    return;
  }

  absl::MutexLock lock(&registration_mutex_);
  if (measure_ == nullptr) {
    MeasureDouble registered_measure =
        opencensus::stats::MeasureRegistry::GetMeasureDoubleByName(name_);
    if (registered_measure.IsValid()) {
      measure_ = std::make_unique<MeasureDouble>(MeasureDouble(registered_measure));
    } else {
      measure_ = std::make_unique<MeasureDouble>(
          MeasureDouble::Register(name_, description_, unit_));
    }
    RegisterView();  // View 在第一次 Record() 时才注册
  }
}

void Gauge::RegisterView() {
  opencensus::stats::ViewDescriptor view_descriptor =
      opencensus::stats::ViewDescriptor()
          .set_name(name_)
          .set_description(description_)
          .set_measure(name_)
          .set_aggregation(opencensus::stats::Aggregation::LastValue());
  internal::RegisterAsView(view_descriptor, tag_keys_);
}
```

**关键：View 在第一次调用 `Record()` 时才注册到 StatsExporter。** 但一旦注册成功，后续 Prometheus 每次 scrape 都能拿到 `LastValue()` 的上一次记录值。

### 指标消失的可能原因分析

**情况一：GCS 重启后 probe 还未执行过**

如果 GCS 重启后 `ray_syncer_io_context` 线程立刻过载，probe 任务排在海量 gRPC 回调后面迟迟得不到执行，`Record()` 从未被调用，View 未注册，指标完全不存在。**但已确认 GCS 没有重启，排除此情况。**

**情况二：GCS 未重启，View 已注册，指标不应消失**

Gauge 使用 `LastValue()` 聚合，一旦 `Record()` 调用过至少一次，View 就注册了。即使后续 probe 因为线程过载未能执行，Prometheus 每次 scrape 仍能拿到上一次记录的值，**指标不会消失，只会停留在最后记录的值不再更新。**

**因此，如果 GCS 没有重启但指标消失，更可能的原因是：**

1. **Ray 的 metrics agent（dashboard_agent）挂了或过载** — Ray 的指标不是 GCS 直接暴露给 Prometheus 的，而是通过 head 节点上的 metrics agent 采集转发。如果 agent 挂了或卡住，所有指标都会断
2. **Prometheus scrape 失败** — GCS 过载导致 `/metrics` 端点响应超时，Prometheus 抓取失败
3. **Prometheus 查询窗口问题** — 如果用的是 PromQL range query，且 scrape 有间断，某些时间点可能没有数据

**排查建议：** 检查那段时间其他 Ray 指标（如 `ray_gcs_*`）是否也同时消失。如果是，说明是 metrics agent 或 scrape 层面的问题，不是单个指标不上报。

### 指标消失作为故障信号的局限性

当前 `ray_io_context_event_loop_lag_ms` 的设计存在一个监控盲区：

- **线程轻度过载**：probe 延迟执行，lag 值升高 → 指标可见，能发现问题 ✓
- **线程极度过载**：probe 长时间得不到执行，lag 值停留在旧值 → 指标存在但不再更新，可能误导 ✗
- **GCS 重启后立刻过载**：probe 从未执行，View 未注册 → 指标完全消失 ✗

**建议：** 在监控告警中，除了关注 lag 值的绝对大小，还应关注指标是否长时间不更新（例如 `stale` 检测），以及指标是否从存在变为不存在。

---

## 优化方案

### 参数优化

| 参数 | 当前值 | 建议值 | 理由 |
|------|--------|--------|------|
| `raylet_report_resources_period_milliseconds` | 500 | **5000** | 500ms 对 1517 节点太激进，改为 5s 可减少 10 倍同步量，资源视图 5s 延迟可接受 |
| `ray_syncer_message_refresh_interval_ms` | 10000 | **30000** | 必须 **远大于** report_period，保持 6 倍比率，避免 Raylet 误判节点过期触发无效刷新 |
| `gcs_resource_broadcast_max_batch_size` | 1（默认） | **100** | 开启批处理，将多个节点的资源变更合并为一条 gRPC 消息广播 |
| `gcs_resource_broadcast_max_batch_delay_ms` | 0（默认） | **500** | 最多等 500ms 积攒一批再发送，需 < report_period |

### 推荐完整配置

```python
{
    "raylet_report_resources_period_milliseconds": 5000,
    "ray_syncer_message_refresh_interval_ms": 30000,
    "gcs_resource_broadcast_max_batch_size": 100,
    "gcs_resource_broadcast_max_batch_delay_ms": 500,
    "gcs_server_rpc_server_thread_num": 64,
    "scheduler_avoid_gpu_nodes": false
}
```

### 参数约束关系

```
约束1: ray_syncer_message_refresh_interval_ms >> raylet_report_resources_period_milliseconds
       （否则 Raylet 误判节点过期，频繁触发无效刷新）

约束2: gcs_resource_broadcast_max_batch_size > 1 时 batch_delay 才生效
       （batch_size=1 时批处理完全禁用）

约束3: gcs_resource_broadcast_max_batch_delay_ms < raylet_report_resources_period_milliseconds
       （否则广播延迟超过一个上报周期，导致资源视图更滞后）
```

### 预期效果

改完后 ray_syncer 的消息处理量估算：

```
之前：1517 节点 × 2 次/秒 × 1517 广播 × batch=1 = 每秒 460 万条 gRPC 消息
之后：1517 节点 × 0.2 次/秒 × (1517/100) 广播 × batch=100 = 每秒 4600 条 gRPC 消息
```

**约 1000 倍下降**，ray_syncer CPU 从 94% 降到 <10%，Actor 创建恢复正常速度。

### 其他优化建议

1. **清理 DEAD actor**（可选）：不影响资源释放，但减小 GCS actor 表遍历开销
2. **降低并发 actor 创建数**：减少同时 PENDING 的 actor 数量，降低调度自旋频率
3. **GCS 日志清理**：当前 101GB 的 gcs_server.out 需要轮转清理，减少磁盘 IO 压力

---

## GCS 重启后 `ray_syncer_io_c` 使用率分析

### 背景

在上述瓶颈分析之后，GCS 发生了一次重启。重启后观察到 `ray_syncer_io_c` 的 CPU 使用率并不高，需要判断这是否符合预期。

### 关键问题

**GCS 重启后，raylet 重连是否会触发全量资源上报，进而导致 `ray_syncer_io_c` 使用率升高？**

### 实地观测数据

#### 阶段一：GCS 刚重启，集群无负载（~15:01 - 15:20）

GCS 于 2026-05-09 15:01:58 重启，连接 Head 节点检查：

```
# GCS 进程信息
root  74  81.1  0.4  7504628 2348892 ?  Sl  15:01  4:36 /opt/vjepa2/.../gcs_server ...

# 运行时间
ps -p 74 -o pid,lstart,etime,rss,pcpu,pmem --no-headers
74  Sat May  9 15:01:58 2026  08:43  2435160  66.0  0.4

# 节点注册情况
NodeInfoGcsService.grpc_server.RegisterNode - 1511 total (0 active)

# ray_syncer_io_c 线程状态
tid=99 comm=ray_syncer_io_c utime=640 stime=228

# 5 秒内 utime 变化
utime=640 → utime=640  （零增长！）

# GCS 主线程状态（正常活动）
utime=9991 → utime=10029  （增长 38）

# 集群资源使用率 —— 全部为零
0.0/39820.0 CPU
0.0/500.0 GPU
0B/165.12TiB memory

# Event stats 增长率（每分钟）
15:10  Global stats: 127014 total
15:11  Global stats: 128534 total
15:12  Global stats: 128540 total
15:13  Global stats: 128546 total
...
15:19  Global stats: 128582 total
（从 ~6 events/minute 降到近乎零）

# 实时 top 快照（CPU > 0.5% 的线程）
98       6.7% pubsub_io_conte
197      6.7% nexting_thread
188      6.7% nexting_thread
（ray_syncer_io_c 未出现，CPU < 0.5%）

# 调度相关日志
grep -c 'resources are not enough' gcs_server.out  → 0
grep -c 'Leasing worker' gcs_server.out  → 0
```

**结论**：GCS 重启后 ~18 分钟内，1511 个节点已全部注册连接，但 `ray_syncer_io_c` 几乎完全空闲。

#### 阶段二：作业恢复运行（~15:42）

```
# 集群资源使用率 —— 已恢复负载
39295.0/39820.0 CPU  (98.7%)
475.0/500.0 GPU     (95%)
111.52TiB/165.12TiB memory (67.5%)

# 线程 CPU 快照（2 秒间隔）
99      98.5% ray_syncer_io_c    ← 飙到 98.5%！
98      14.4% pubsub_io_conte
97      10.9% task_io_context
74      10.4% gcs_server

# ray_syncer_io_c utime 增长
2 秒内 delta=151 jiffies  （活跃状态）

# RaySyncer.BroadcastMessage 统计
64279 total (0 active), Execution time: mean = 1.44ms

# Pub/sub 消息溢出
Pub/sub message is dropped to stay under the maximum configured buffer size=1073741824B
channel_type: RAY_NODE_RESOURCE_USAGE_CHANNEL
```

**结论**：作业恢复后，`ray_syncer_io_c` 立刻回到 98.5%，与之前瓶颈分析完全一致。

### 源码级根因分析

#### 1. raylet 资源上报走 ray_syncer 通道

源码（`node_manager.cc:345-350`）：

```cpp
ray_syncer_.Register(
    /* message_type */ syncer::MessageType::RESOURCE_VIEW,
    /* reporter */ &cluster_resource_scheduler_.GetLocalResourceManager(),
    /* receiver */ this,
    /* pull_from_reporter_interval_ms */ report_resources_period_ms_);  // 当前配置 500ms
```

raylet 的资源上报通过 `RaySyncer::Register` 注册了一个定时拉取 reporter 的周期任务，每 500ms 调用一次 `OnDemandBroadcasting(RESOURCE_VIEW)`。

#### 2. 版本号去重机制——资源无变化时不上报

源码（`local_resource_manager.cc:422-447`）：

```cpp
std::optional<syncer::RaySyncMessage> LocalResourceManager::CreateSyncMessage(
    int64_t after_version, syncer::MessageType message_type) const {
  RAY_CHECK_EQ(message_type, syncer::MessageType::RESOURCE_VIEW);
  const_cast<LocalResourceManager *>(this)->UpdateAvailableObjectStoreMemResource();

  if (version_ <= after_version) {
    return std::nullopt;  // 资源无变更，不上报
  }

  syncer::RaySyncMessage msg;
  // ... 构造消息
  msg.set_version(version_);
  return std::make_optional(std::move(msg));
}

void LocalResourceManager::OnResourceOrStateChanged() {
  ++version_;  // 只有资源变更时才递增
  // ...
}
```

**关键机制**：

1. `LocalResourceManager` 维护 `version_` 计数器，每次资源变化（分配、释放、增删）时通过 `OnResourceOrStateChanged()` 递增
2. `OnDemandBroadcasting` 每 500ms 触发，调用 `CreateSyncMessage(after_version)`
3. 如果 `version_ <= after_version`（自上次上报后无变更），返回 `std::nullopt`，不产生任何消息
4. 只有版本号递增后，才生成实际的同步消息并广播

**结论：资源无变化时，定时器虽然每个周期都触发，但全部返回 nullopt，几乎零开销。**

#### 3. GCS 端版本去重——广播端也有过滤

源码（`node_state.cc:62-76`）：

```cpp
bool NodeState::ConsumeSyncMessage(std::shared_ptr<const RaySyncMessage> message) {
  auto &current = cluster_view_[message->node_id()][message->message_type()];

  if (current && current->version() >= message->version()) {
    RAY_LOG(INFO) << "Dropping sync message with stale version.";
    return false;  // 过期消息，不广播
  }

  current = message;
  auto receiver = receivers_[message->message_type()];
  if (receiver != nullptr) {
    receiver->ConsumeSyncMessage(message);
  }
  return true;
}
```

源码（`ray_syncer_bidi_reactor_base.h:47-62`）：

```cpp
bool PushToSendingQueue(std::shared_ptr<const RaySyncMessage> message) override {
  // 不发回消息来源节点
  if (message->node_id() == GetRemoteNodeID()) {
    return false;
  }

  // 版本号去重：已发送过更新版本，不再发送
  auto &node_versions = GetNodeComponentVersions(message->node_id());
  if (node_versions[message->message_type()] >= message->version()) {
    return false;
  }

  node_versions[message->message_type()] = message->version();
  sending_buffer_[key] = std::move(message);
  // ... 批处理或立即发送逻辑
}
```

**每一层都有版本号去重**：raylet reporter 层、GCS NodeState 层、GCS BidiReactor 发送层。

#### 4. raylet 重连时 GCS 推送全量集群视图

源码（`ray_syncer.cc:126-148`）：

```cpp
void RaySyncer::Connect(std::shared_ptr<RaySyncerBidiReactor> reactor) {
  boost::asio::dispatch(
      io_context_.get_executor(), std::packaged_task<void()>([this, reactor]() {
        auto is_new = sync_reactors_.emplace(reactor->GetRemoteNodeID(), reactor).second;
        RAY_CHECK(is_new);

        // 新连接推送全量集群视图
        for (const auto &[_, messages] : node_state_->GetClusterView()) {
          for (const auto &message : messages) {
            if (!message) continue;
            reactor->PushToSendingQueue(message);  // 全量推送
          }
        }
      }))
      .get();
}
```

当 raylet 重连时，GCS 会遍历 `node_state_->GetClusterView()` 中所有节点的资源视图，推送到新连接。但这个推送只发给**这一个新连接**，不会广播给其他已连接的 raylet。

#### 5. raylet 重连时 BidiReactor 的版本号表重置

源码（`ray_syncer_bidi_reactor_base.h:278-285`）：

```cpp
std::array<int64_t, kComponentArraySize> &GetNodeComponentVersions(
    const std::string &node_id) {
  auto iter = node_versions_.find(node_id);
  if (iter == node_versions_.end()) {
    iter = node_versions_.emplace(node_id, std::array<int64_t, kComponentArraySize>())
               .first;
    iter->second.fill(-1);  // 新连接版本号初始化为 -1
  }
  return iter->second;
}
```

**重连时 `node_versions_` 被重置**（新 Reactor 对象），所有节点的版本号初始化为 -1。这意味着：
- GCS 推送全量集群视图时，所有消息版本都 > -1，不会被去重，全部会发送
- raylet 端同理，重连后首次上报的资源变更也会被 GCS 接受并广播

#### 6. 广播逻辑——O(N) 复杂度

源码（`ray_syncer.cc:209-224`）：

```cpp
void RaySyncer::BroadcastMessage(std::shared_ptr<const RaySyncMessage> message) {
  io_context_.dispatch(
      [this, message] {
        if (!node_state_->ConsumeSyncMessage(message)) {
          return;  // 版本去重，跳过过期消息
        }
        // 广播给所有已连接的 raylet
        for (auto &reactor : sync_reactors_) {
          reactor.second->PushToSendingQueue(message);
        }
      },
      "RaySyncer.BroadcastMessage");
}
```

### 综合结论

#### GCS 重启后 ray_syncer_io_c 是否应该高？

**是的，只要有负载就应该高。** 但实际观测到的低使用率是因为集群还没恢复作业。

完整的三阶段模型：

| 阶段 | 时间窗口 | ray_syncer_io_c 压力 | 原因 |
|------|---------|---------------------|------|
| **阶段1：节点重连** | 0~5min | 低 | raylet 逐个重建 bidi stream，GCS 向每个新连接推送当前集群视图（此时视图还很小），event stats 显示 ~128K 总事件 |
| **阶段2：作业恢复前** | 5~20min | **极低** | 节点已全部连上，但**无作业运行** → 资源零变化 → `version_` 不变 → `CreateSyncMessage` 返回 `nullopt` → 无广播 → ray_syncer_io_c 空闲 |
| **阶段3：作业恢复后** | 20min+ | **高（98.5%）** | 作业调度 → actor 创建/销毁 → 资源分配/释放 → `version_` 递增 → 每次变更触发广播 → 1500 节点 × 2 次/秒 × 1517 广播 × batch=1 → ray_syncer_io_c 饱和 |

#### 为什么阶段2的 ray_syncer_io_c 极低？

核心原因：**版本号去重机制 + 零负载**

1. raylet 重连后，周期性定时器（500ms）正常触发 `OnDemandBroadcasting(RESOURCE_VIEW)`
2. 但集群没有作业运行，资源无任何变化，`LocalResourceManager::version_` 不变
3. `CreateSyncMessage(after_version)` 判断 `version_ <= after_version`，返回 `nullopt`
4. 没有消息产生 → 没有广播 → ray_syncer_io_c 完全空闲
5. 即使 raylet 刚重连时 GCS 推送了全量视图，这也只是一次性开销（1511 个节点的资源快照），推完后就没有后续消息了

**实测验证**：5 秒内 `ray_syncer_io_c` 的 utime 从 640→640，零增长，确认完全空闲。

#### 为什么阶段3 ray_syncer_io_c 飙高？

作业恢复后：

1. Actor 创建/销毁 → raylet 分配/释放资源 → `OnResourceOrStateChanged()` → `version_++`
2. 500ms 定时器触发 → `CreateSyncMessage` 返回实际消息（version_ > after_version）
3. GCS 收到消息 → `BroadcastMessage` → 遍历所有 1510 个 sync_reactors 推送
4. 大量 LeaseFailed → 资源抖动 → 更多变更 → 更多广播 → 恶性循环
5. ray_syncer_io_c 从 <0.5% 飙到 98.5%

#### 关键洞察

**GCS 重启本身不是 ray_syncer_io_c 压力的来源，负载才是。** 重连时的全量推送只是一次性开销（约 128K 事件），而稳态运行时的资源变更广播才是持续的 O(N²) 压力。在当前配置下（500ms 上报周期 + 零批处理），只要集群有负载，ray_syncer_io_c 就会饱和，与 GCS 是否重启无关。
