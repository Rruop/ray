# GCS 重启场景下 ray_syncer_io_c 线程 CPU 分析

## 分析目标

验证两个问题：
1. GCS 重启（或 head 节点重启）时，`ray_syncer_io_c` 线程 CPU 是否会升高？
2. Raylet 没有资源变更时，`ray_syncer_io_c` 线程 CPU 是否会升高？

## 分析方法

### 方法概述

通过 KML WebShell 连接 head 节点容器，在 GCS 刚启动后的不同时间窗口采集线程级 CPU 数据和 event_stats 日志，对比启动阶段与稳态阶段的差异。

### 工具链

| 工具 | 用途 |
|------|------|
| KML WebShell | 远程连接 head 节点容器执行命令 |
| `kml-webshell-direct` 技能 | 通过 WebSocket 拦截自动化 shell 交互 |
| `/proc/<pid>/task/<tid>/stat` | 获取各线程累计 CPU 时间（jiffies） |
| `top -H` | 获取线程实时 CPU 占用率 |
| GCS event_stats 日志 | 获取 `RaySyncer.BroadcastMessage` 等事件计数和耗时 |

### 连接方式

```bash
# 使用 kml-webshell-direct 技能连接 head 节点
python3 scripts/kml_ws_exec.py \
  --url "https://kml.corp.kuaishou.com/v2/#/system/terminal?clusterName=<CLUSTER>&namespace=<NS>&pod=<POD>&mode=shell&fullScreen=1&auth=gaia" \
  --wait-for-ws 120 \
  --cmd "<command>"
```

---

## 分析过程

### 第一步：确认 GCS 进程和集群基本信息

```bash
ps aux | grep gcs_server | grep -v grep
```

**结果：**

```
root  74  121  0.4 13054480 2619188 ?  Sl  16:07  8:13  /opt/vjepa2/lib/python3.12/site-packages/ray/core/src/ray/gcs/gcs_server ...
```

```bash
cat /proc/74/status | grep -E 'VmRSS|Threads'
```

```
VmRSS:   2740784 kB
Threads: 270
```

```bash
ray status 2>&1 | grep -c 'node_'
```

```
1510
```

**集群基本信息：**

| 指标 | 值 |
|------|-----|
| GCS PID | 74 |
| GCS 启动时间 | 2026-05-09 16:07:58 |
| Ray 版本 | 2.54.0 |
| 集群节点数 | 1510 |
| GCS 内存 RSS | 2.7 GB |
| GCS 线程数 | 270 |

**已应用的优化配置：**

```json
{
    "raylet_report_resources_period_milliseconds": 5000,
    "ray_syncer_message_refresh_interval_ms": 30000,
    "gcs_resource_broadcast_max_batch_size": 100,
    "gcs_resource_broadcast_max_batch_delay_ms": 500,
    "health_check_period_ms": 10000,
    "gcs_server_rpc_server_thread_num": 64,
    "scheduler_avoid_gpu_nodes": false,
    "event_stats_print_interval_ms": 180000
}
```

### 第二步：检查 GCS 日志中的重连事件

```bash
grep -c 'Connection is broken' /tmp/ray/session_latest/logs/gcs_server.out
```

```
1508
```

```bash
grep 'Connection is broken' /tmp/ray/session_latest/logs/gcs_server.out | \
  awk '{print $2}' | cut -d, -f1 | cut -d: -f1,2 | sort | uniq -c
```

```
1508 16:08
```

**全部 1508 次连接断开集中在 16:08 这一分钟内。**

查看详细时间分布：

```bash
# 最早的连接断开
grep 'Connection is broken' /tmp/ray/session_latest/logs/gcs_server.out | head -3
```

```
[2026-05-09 16:08:05,156 I 74 99] (gcs_server) ray_syncer.cc:253: Connection is broken. node_id=a699...
[2026-05-09 16:08:10,896 I 74 99] (gcs_server) ray_syncer.cc:253: Connection is broken. node_id=4531...
[2026-05-09 16:08:12,716 I 74 99] (gcs_server) ray_syncer.cc:253: Connection is broken. node_id=c3ed...
```

```bash
# 最晚的连接断开
grep 'Connection is broken' /tmp/ray/session_latest/logs/gcs_server.out | tail -3
```

```
[2026-05-09 16:08:23,800 I 74 99] (gcs_server) ray_syncer.cc:253: Connection is broken. node_id=4078...
[2026-05-09 16:08:28,580 I 74 99] (gcs_server) ray_syncer.cc:253: Connection is broken. node_id=6221...
[2026-05-09 16:08:29,622 I 74 99] (gcs_server) ray_syncer.cc:253: Connection is broken. node_id=88ef...
```

**连接断开窗口：16:08:05 ~ 16:08:29（约 24 秒）。**

日志中还发现 cluster ID 不匹配的警告，确认这是旧 session 的 raylet 连接到新 GCS：

```
[2026-05-09 16:08:00,885 W 74 253] (gcs_server) server_call.h:228: Wrong cluster ID token in request!
  Expected: c3ab2d9a..., but got: a3aa0666...
```

### 第三步：分析启动阶段的 BroadcastMessage 统计

GCS 配置了 `event_stats_print_interval_ms: 180000`（每 3 分钟打印一次事件统计）。

```bash
grep -n 'RaySyncer.BroadcastMessage' /tmp/ray/session_latest/logs/gcs_server.out
```

```
# 第 1 次统计（~16:08:00 后，启动后约 2 秒）— ray_syncer_io_context 线程
line 6276:  RaySyncer.BroadcastMessage - 3105 total (0 active),
            Execution time: mean = 0.01ms, total = 23.19ms,
            Queueing time: mean = 0.00ms, max = 0.00ms, total = 0.27ms

# 第 2 次统计（16:11:00，启动后约 3 分钟）— gcs_server_main_io_context 线程
line 387196: RaySyncer.BroadcastMessage - 6078 total (0 active),
             Execution time: mean = 0.48ms, total = 2930.07ms,
             Queueing time: mean = 0.00ms, max = 0.00ms, total = 0.84ms

# 第 3 次统计（启动后约 6 分钟）
line 387402: RaySyncer.BroadcastMessage - 6079 total (0 active),
             Execution time: mean = 0.48ms, total = 2934.51ms

# 第 4 次统计（启动后约 9 分钟）
line 387606: RaySyncer.BroadcastMessage - 6080 total (0 active),
             Execution time: mean = 0.48ms, total = 2943.70ms

# 第 5 次统计（启动后约 12 分钟）
line 387809: RaySyncer.BroadcastMessage - 6080 total (0 active),
             Execution time: mean = 0.48ms, total = 2943.70ms
```

**关键发现：**

| 时间窗口 | BroadcastMessage 总数 | 增量 | mean 执行时间 |
|----------|---------------------|------|--------------|
| 启动 ~2s | 3105 | — | 0.01ms |
| 启动 ~3min | 6078 | **+2973** | **0.48ms** |
| 启动 ~6min | 6079 | +1 | 0.48ms |
| 启动 ~9min | 6080 | +1 | 0.48ms |
| 启动 ~12min | 6080 | **+0** | 0.48ms |

- 启动后前 3 分钟内产生了 **6078 次** BroadcastMessage（占总量的 99.97%）
- 之后 9 分钟内仅增加了 **2 次**
- mean 执行时间从 0.01ms 飙升到 0.48ms（**48 倍**），说明启动阶段后期消息变大（包含更多节点的 cluster view）

### 第四步：检查线程实时 CPU 占用（稳态）

```bash
top -H -b -n2 -d2 -p 74 | awk '/^top/ {snap++} snap==2 && $9>0.5 {printf "%-8s %6s %6s %s\n", $1, $9"%", $10"%", $12}'
```

```
74         5.0%   0.5% gcs_server
184        1.0%   0.5% timer_m+
191        1.0%   0.5% nexting+
194        1.0%   0.5% nexting+
195        1.0%   0.5% nexting+
196        1.0%   0.5% nexting+
199        1.0%   0.5% nexting+
```

**`ray_syncer_io_c` 没有出现在 >0.5% 的列表中 — 稳态下几乎为 0。**

### 第五步：10 秒间隔双快照精确计算各线程 CPU 增量

```bash
# 快照 1
date +%s
for tid in 74 99 98 97; do
  comm=$(cat /proc/74/task/$tid/comm)
  utime=$(awk '{print $14}' /proc/74/task/$tid/stat)
  stime=$(awk '{print $15}' /proc/74/task/$tid/stat)
  echo "tid=$tid comm=$comm utime=$utime stime=$stime"
done

sleep 10

# 快照 2（同样的命令）
```

**结果：**

| 线程 | 快照 1 utime | 快照 2 utime | 增量 (10s) | 实时 CPU% |
|------|-------------|-------------|-----------|----------|
| `gcs_server` (tid=74) | 5536 | 5612 | **76** | ~7.6% |
| `ray_syncer_io_c` (tid=99) | 2091 | **2091** | **0** | **~0%** |
| `pubsub_io_conte` (tid=98) | 2730 | 2741 | **11** | ~1.1% |
| `task_io_context` (tid=97) | 0 | 1 | **1** | ~0.1% |

### 第六步：检查累计 CPU 时间分布

```bash
for tid in $(ls /proc/74/task/); do
  comm=$(cat /proc/74/task/$tid/comm)
  utime=$(awk '{print $14}' /proc/74/task/$tid/stat)
  echo "tid=$tid comm=$comm utime=$utime"
done | sort -t= -k3 -rn | head -15
```

```
tid=74  comm=gcs_server        utime=4355   ← 最高，GCS 主线程
tid=98  comm=pubsub_io_conte   utime=2568
tid=99  comm=ray_syncer_io_c   utime=2088   ← 第三高（但稳态 CPU=0%）
tid=302 comm=server.poll63     utime=273
tid=301 comm=server.poll62     utime=301
tid=300 comm=server.poll61     utime=273
...
```

**`ray_syncer_io_c` 的累计 utime=2088 jiffies（约 20.9 秒 CPU 时间），全部消耗在启动阶段。**

---

## 分析结论

### 结论 1：GCS 重启时 `ray_syncer_io_c` CPU **会升高**

**证据链：**

```
GCS 重启 → 1510 个 raylet 连接到新 GCS
    ↓
旧 cluster ID 不匹配 → GCS 断开连接（1508 次 "Connection is broken"，集中在 24 秒内）
    ↓
raylet 2 秒后重连 → 调用 Connect() → 全量推送 cluster view
    ↓
GCS 端 StartSync() → 也调用 Connect() → 向每个 raylet 推送 GCS 持有的全部 cluster view
    ↓
ray_syncer_io_c 线程处理大量 BroadcastMessage（前 3 分钟 6078 次）
    ↓
CPU 时间集中消耗：2088 jiffies（~20.9 秒），占 GCS 运行 12 分钟内总量的近 100%
```

**量化：**
- 启动阶段（前 ~3 分钟）：6078 次 BroadcastMessage，累计执行时间 2930ms
- 稳态（后 ~9 分钟）：仅 2 次 BroadcastMessage
- 累计 CPU 时间：2088 jiffies，全部在启动阶段消耗

### 结论 2：Raylet 无资源变更时 `ray_syncer_io_c` CPU **不会升高**

**证据链：**

```
稳态下无资源变更 → version_ 不递增
    ↓
定时器触发 CreateSyncMessage(after_version) → version_ <= after_version → 返回 nullopt
    ↓
OnDemandBroadcasting 不调用 BroadcastMessage
    ↓
ray_syncer_io_c 无消息需要处理
```

**量化：**
- 10 秒采样 utime 增量 = 0（CPU = 0%）
- BroadcastMessage 在稳态 3 分钟内仅增加 0~1 次
- `resources are not enough` 日志 = 0 次（无调度自旋）

### 结论 3：优化配置有效缓解了启动冲击

对比之前的案例（相同规模 1517 节点集群）：

| 指标 | 之前（未优化） | 本次（已优化） |
|------|-------------|-------------|
| `ray_syncer_io_c` 峰值 CPU | **94%** | 启动阶段短暂升高后回落 |
| `ray_syncer_io_c` 累计 utime | **8,519,449** jiffies | **2,088** jiffies |
| BroadcastMessage/秒（稳态） | ~460 万条 | ~0 条 |
| `resources are not enough` | 1.17 亿次 | 0 次 |
| GCS 日志大小 | 101 GB | 87 MB |

之前持续 94% 的根因不是重启本身，而是**调度自旋导致资源视图不断变化 → version_ 持续递增 → 每次变更触发广播 → 恶性循环**。优化后资源上报周期从 500ms → 5000ms，批处理 100 条合并发送，从根本上消除了放大效应。

---

## 源码级机制说明

### 重连时的全量推送

`src/ray/ray_syncer/ray_syncer.cc:126-152`：

```cpp
void RaySyncer::Connect(std::shared_ptr<RaySyncerBidiReactor> reactor) {
  boost::asio::dispatch(
      io_context_.get_executor(), std::packaged_task<void()>([this, reactor]() {
        sync_reactors_.emplace(reactor->GetRemoteNodeID(), reactor);
        // Send the view for new connections.
        for (const auto &[_, messages] : node_state_->GetClusterView()) {
          for (const auto &message : messages) {
            if (!message) continue;
            reactor->PushToSendingQueue(message);  // 全量推送
          }
        }
      })).get();
}
```

### 版本号差分机制

`src/ray/raylet/scheduling/local_resource_manager.cc:422-445`：

```cpp
std::optional<syncer::RaySyncMessage> LocalResourceManager::CreateSyncMessage(
    int64_t after_version, syncer::MessageType message_type) const {
  const_cast<LocalResourceManager *>(this)->UpdateAvailableObjectStoreMemResource();
  if (version_ <= after_version) {
    return std::nullopt;  // 资源无变更，不上报
  }
  // ... 构造消息
}
```

### 重连延迟

`src/ray/ray_syncer/ray_syncer.cc:103-111`：

```cpp
if (restart) {
  execute_after(io_context_,
    [this, remote_node_id, channel]() {
      Connect(remote_node_id, channel);
    },
    std::chrono::milliseconds(2000));  // 2 秒后重连
}
```

---

## 可复用的诊断命令

### 1. 获取 GCS 进程基本信息

```bash
# GCS 进程
ps aux | grep gcs_server | grep -v grep

# GCS 内存和线程数
cat /proc/<GCS_PID>/status | grep -E 'VmRSS|Threads'

# GCS 运行时间
python3 -c "
import os, time
boot = os.stat('/proc/<GCS_PID>').st_ctime
uptime = time.time() - boot
print(f'GCS uptime: {uptime:.0f}s ({uptime/60:.1f}min)')
"
```

### 2. 线程级 CPU 分析

```bash
# 实时 CPU 占用率（2 秒间隔第二次采样）
top -H -b -n2 -d2 -p <GCS_PID> | \
  awk '/^top/ {snap++} snap==2 && $9>0.5 {printf "%-8s %6s %6s %s\n", $1, $9"%", $10"%", $12}'

# 累计用户态时间（按 utime 降序排列）
for tid in $(ls /proc/<GCS_PID>/task/); do
  comm=$(cat /proc/<GCS_PID>/task/$tid/comm 2>/dev/null)
  utime=$(awk '{print $14}' /proc/<GCS_PID>/task/$tid/stat 2>/dev/null)
  echo "tid=$tid comm=$comm utime=$utime"
done | sort -t= -k3 -rn | head -15
```

### 3. 10 秒间隔双快照精确计算 CPU 增量

```bash
echo '=== snapshot 1 ==='; date +%s
for tid in <GCS_PID> <SYNCER_TID> <PUBSUB_TID> <TASK_TID>; do
  comm=$(cat /proc/<GCS_PID>/task/$tid/comm 2>/dev/null)
  utime=$(awk '{print $14}' /proc/<GCS_PID>/task/$tid/stat 2>/dev/null)
  stime=$(awk '{print $15}' /proc/<GCS_PID>/task/$tid/stat 2>/dev/null)
  echo "tid=$tid comm=$comm utime=$utime stime=$stime"
done

sleep 10

echo '=== snapshot 2 ==='; date +%s
# 重复同样的循环，对比两次 utime 差值计算实时 CPU%
# CPU% ≈ (utime2 - utime1) / (HZ * interval) * 100
# 其中 HZ=100（x86 Linux 默认），interval=10s
```

### 4. 检查重连事件

```bash
# 连接断开次数
grep -c 'Connection is broken' /tmp/ray/session_latest/logs/gcs_server.out

# 连接断开时间分布（按分钟聚合）
grep 'Connection is broken' /tmp/ray/session_latest/logs/gcs_server.out | \
  awk '{print $2}' | cut -d, -f1 | cut -d: -f1,2 | sort | uniq -c

# cluster ID 不匹配（旧 session raylet 连接新 GCS）
grep -c 'Wrong cluster ID token' /tmp/ray/session_latest/logs/gcs_server.out
```

### 5. 检查 BroadcastMessage 事件统计

```bash
# 查看所有 BroadcastMessage 统计（每 event_stats_print_interval_ms 打印一次）
grep 'RaySyncer.BroadcastMessage' /tmp/ray/session_latest/logs/gcs_server.out

# 查看调度自旋（资源不足重试）
grep -c 'resources are not enough' /tmp/ray/session_latest/logs/gcs_server.out

# GCS 日志大小（过大说明有异常循环）
wc -l /tmp/ray/session_latest/logs/gcs_server.out
ls -lh /tmp/ray/session_latest/logs/gcs_server.out
```

### 6. 集群资源状态

```bash
# 节点数和资源概览
ray status 2>&1 | head -30

# 资源使用率
ray status 2>&1 | grep -E 'Resources|GPU|memory|CPU'
```

---

## 附录：线程名对照表

| 线程名（`/proc/.../comm`） | 全名 | 职责 |
|---------------------------|------|------|
| `gcs_server` | gcs_server main | GCS 主事件循环，处理调度、节点管理 |
| `ray_syncer_io_c` | ray_syncer_io_context | 资源视图同步广播（hub-and-spoke） |
| `pubsub_io_conte` | pubsub_io_context | Pub/Sub 消息分发（actor/job/node 状态） |
| `task_io_context` | task_io_context | Task 状态管理 |
| `server.poll*` | gRPC server poll | gRPC 服务端线程池 |
| `nexting_thread` | nexting_thread | 内部调度线程 |
| `timer_m*` | timer_manager | 定时器管理 |
| `event_engine` | event_engine | gRPC 事件引擎 |
