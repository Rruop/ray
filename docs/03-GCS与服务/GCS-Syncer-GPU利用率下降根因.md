# GCS ray_syncer_io_c CPU 高 & GPU 利用率下降根因分析

## 1. 问题描述

**集群信息**：
- 集群名称：`kml-hb2az1-l3-2`
- 命名空间：`lmserving`
- Head 节点：`kml-task-661218-record-15690739-prod-worker-0-g965q`（`klingai-wlf1-ge114-4.idchb2az1.hb2.kwaidc.com`）
- Head IP：`10.81.0.20:6379`
- Ray 版本：commit `cc121a56b92f65ea80d7ff9b64df4acfd4f9714a`
- 集群规模：600 GPU，约 50568 CPU

**现象**：
1. `ray_syncer_io_c` 线程 CPU 持续占用 56.9%
2. `gcs_server` 主线程 CPU 持续占用 26.6%（进程整体 344%，298 线程，RSS 16GB）
3. `raylet` 主线程 CPU 持续占用 99.9%
4. 14:15 左右 GPU 利用率明显下降
5. 怀疑有节点挂掉和 actor 重启

---

## 2. 排查方法

### 2.1 线程级 CPU 采集

```bash
# 方法一：ps 按线程列出 CPU（-L 展示线程）
ps -eo pid,tid,pcpu,pmem,comm -L 2>/dev/null \
  | grep -E 'ray_syncer|gcs_server|raylet' \
  | sort -k3 -rn | head -30

# 方法二：top 按线程模式（-H）
top -b -n 1 -H 2>/dev/null \
  | grep -E 'ray_syncer|gcs_server|raylet' | head -20

# 方法三：查看进程详情
cat /proc/<pid>/status | grep -E 'Name|Pid|Threads|VmRSS'

# 方法四：查看进程 CPU 排行
ps -eo pid,pcpu,rss,comm --sort=-pcpu 2>/dev/null | head -15
```

**实测结果**：

| 线程/进程 | PID/TID | CPU % (ps) | CPU % (top) | 说明 |
|-----------|---------|-----------|-------------|------|
| `ray_syncer_io_c` | tid=103 (pid=78) | 56.9% | — | gcs_server 进程内的 syncer IO 线程 |
| `gcs_server` 主线程 | tid=78 (pid=78) | 26.6% | — | GCS 主事件循环 |
| `raylet` 主线程 | tid=950 (pid=950) | 31.6% | 99.9% | raylet 主事件循环 |
| `gcs_server` 进程整体 | pid=78 | 344% | — | 298 线程，RSS=16GB |

### 2.2 节点故障时间线采集

```bash
# 查看节点被标记为 dead 的记录
grep 'has been marked dead' /tmp/ray/session_latest/logs/gcs_server.out

# 按小时统计 dead 事件分布
grep 'has been marked dead' /tmp/ray/session_latest/logs/gcs_server.out \
  | awk '{print substr($0,2,19)}' | cut -d: -f1-2 \
  | sort | uniq -c | sort -rn | head -20

# 查看 14:15-14:16 受影响的节点 IP
grep 'has been marked dead' /tmp/ray/session_latest/logs/gcs_server.out \
  | grep '14:1[56]' \
  | awk -F'address: ' '{print $2}' | awk '{print $1}' | sort -u

# 查看 Health Check 失败分布
grep 'Health check failed' /tmp/ray/session_latest/logs/gcs_server.out \
  | awk '{print substr($0,2,19)}' | cut -d: -f1-2 \
  | sort | uniq -c | sort -rn | head -15

# 查看 Actor 失败和重调度
grep 'Actor is failed' /tmp/ray/session_latest/logs/gcs_server.out \
  | grep '14:16' | wc -l

# 查看集群当前状态
ray status 2>/dev/null | head -40
```

### 2.3 system-config 生效验证

```bash
# 方法一：查看 gcs_server 进程命令行参数
cat /proc/<gcs_pid>/cmdline | tr '\0' '\n'

# 方法二：解码 config_list base64 参数
cat /proc/<gcs_pid>/cmdline | tr '\0' '\n' | grep config_list
# 提取 base64 部分后解码
echo '<base64_string>' | base64 -d | python3 -m json.tool

# 方法三：通过 event_stats 打印间隔验证
grep 'gcs_server.cc:922' /tmp/ray/session_latest/logs/gcs_server.out \
  | awk '{print substr($0,2,19)}' | tail -10

# 方法四：通过 health_check remaining checks 验证
grep 'remaining checks' /tmp/ray/session_latest/logs/gcs_server.out | head -5

# 方法五：用定时器执行次数反推间隔
grep 'debug_state_event_stats_print' /tmp/ray/session_latest/logs/gcs_server.out | tail -3
# 查看总执行次数 N，集群运行时间 T 小时，实际间隔 = T*3600/N 秒
```

---

## 3. 排查结论

### 3.1 14:15 GPU 利用率下降事件时间线

```
14:12:31     首个节点 10.82.233.95 被标记 dead
    ↓
14:14~14:15  多个网段节点网络不可达
             GCS Health Check Manager 记录 536 次连接失败
             "Connection refused" 到多个 raylet 端口
    ↓
14:15:15     Health check 批量失败高峰（536次/分钟）
             涉及 10.48.x.x, 10.82.x.x, 10.51.x.x 等多网段
    ↓
14:16:11~19  107 个节点被标记为 dead（42 个不同 IP）
    ↓
14:16        427 个 Actor 失败（need_reschedule=1）
             GCS 触发大规模 actor reschedule
    ↓
14:16+       GCS 尝试将 actor 重新调度到剩余存活节点
             大量 "Failed to lease worker... resources are not enough"
             GPU 利用率断崖式下降（actor fail→reschedule→重启的空窗期）
```

**受影响 IP 网段分布**（42 个不同 IP）：
- `10.48.32.x` — 10 个 IP
- `10.48.33.x` — 10 个 IP
- `10.48.34.x` — 4 个 IP
- `10.82.23x.x` — 13 个 IP
- `10.51.x.x` — 2 个 IP
- 其他 — 3 个 IP

**结论**：多网段同时故障，非单机/单交换机问题，可能是上层网络设备或机房层面问题。

### 3.2 全天节点 dead 事件统计

```
550 次  2026-05-13 10:17   ← 最严重，集群刚启动时
107 次  2026-05-13 14:16   ← 第二波，导致 GPU 下降
 43 次  2026-05-13 11:08
 30 次  2026-05-13 14:34
 28 次  2026-05-13 12:22
  8 次  2026-05-13 15:15
  7 次  2026-05-13 15:26
  ...
总计: 938 次 node dead 事件（全天）
```

### 3.3 system-config 未生效（关键发现）

**启动命令中的分号 bug**：

```bash
ray start --head --port=6379 --dashboard-host=0.0.0.0 --block \
  --num-cpus=0 --num-gpus=0 \
  --dashboard-agent-listen-port=0; --system-config='...'
#                               ^^^
#                           分号导致命令截断！
```

bash 中 `;` 是命令分隔符，实际执行的是：
1. `ray start --head ... --dashboard-agent-listen-port=0`（不带 system-config 启动，使用全部默认值）
2. `--system-config='...'`（因为 `ray start --block` 阻塞，此命令永远不会执行）

**验证证据**：

| 验证方式 | 结果 | 说明 |
|----------|------|------|
| `/proc/78/cmdline` 的 `config_list` base64 解码 | 只有 `object_spilling_config`，无 system-config 参数 | 若 `--system-config` 传入，会序列化到 `config_list` 中 |
| `event_stats_print_interval_ms` 打印间隔 | 60s（默认值），非配置的 180s | `gcs_server.cc:922` 时间戳间隔 60s |
| 定时器执行次数 | `debug_state_event_stats_print` 执行 331 次 / 5.5h = 60s | 若 180s 则预期 ~110 次 |

**`event_stats_print_interval_ms` 生效验证方法详解**：

GCS 日志中 `gcs_server.cc:922`（Main service）和 `gcs_server.cc:926`（其他 io_context）的打印就是由 `event_stats_print_interval_ms` 控制的定时器。对应定时器名称为 `GCSServer.deadline_timer.debug_state_event_stats_print`。

```
观测到的打印时间戳：
  15:42:34 → 15:43:34 → 15:44:34 → 15:45:34 → 15:46:34
  间隔恒定为 60 秒 = 默认值

若 180s 生效，应该看到：
  15:42:34 → 15:45:34 → 15:48:34
  间隔应为 180 秒
```

**`health_check_failure_threshold` 看起来是 10（remaining checks 9），这是因为 Ray 2.52.1 的默认值就是 10，而非配置生效。**

### 3.4 实际运行参数 vs 期望参数

由于 system-config 完全未传入，所有参数均为默认值：

| 参数 | 期望值 | 实际值（默认） | 影响 |
|------|--------|---------------|------|
| `raylet_report_resources_period_milliseconds` | 20000 | **100** | 每 raylet 每 100ms 上报，600 节点 = 6000 次/秒（期望的 200 倍） |
| `ray_syncer_message_refresh_interval_ms` | 60000 | **100** | syncer 每 100ms 刷新（期望的 600 倍），**直接导致 ray_syncer_io_c 高 CPU** |
| `gcs_resource_broadcast_max_batch_delay_ms` | 1500 | **0** | 无批量延迟，每条资源变更立即广播 |
| `gcs_resource_broadcast_max_batch_size` | 1500 | 默认值 | — |
| `health_check_period_ms` | 10000 | **1000** | 每 1s 一次 health check（期望的 10 倍） |
| `health_check_failure_threshold` | 10 | **10** | 默认值恰好与配置一致 |
| `gcs_server_rpc_server_thread_num` | 64 | **1** | GCS RPC 只有 1 个线程 |
| `event_stats_print_interval_ms` | 180000 | **60000** | 每 60s 打印 stats |
| `task_events_report_interval_ms` | 10000 | 默认值 | — |
| `scheduler_avoid_gpu_nodes` | false | **false** | 默认一致 |

---

## 4. Ray 内部架构：节点 dead/alive 与 ray_syncer_io_c 的关系

### 4.1 三条独立路径

```
┌──────────────────────────────────────────────────────────────────────┐
│                     GCS Server 内部线程模型                           │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  路径1: 节点存活检测（GCS 主线程）                                     │
│  ┌────────────────────────────────────┐                              │
│  │ GCS Health Check Manager            │                              │
│  │ health_check_period_ms              │  gRPC: 逐节点发送            │
│  │ health_check_failure_threshold      │  IsAlive / health check      │
│  │ → 连续失败超阈值 → mark node dead  │                              │
│  └──────────┬─────────────────────────┘                              │
│             │ 触发                                                    │
│             ▼                                                         │
│  ┌────────────────────────────────────┐                              │
│  │ gcs_node_manager                    │                              │
│  │ → 发布 NODE_DEAD 事件              │── 路径2: pubsub 广播 ──→     │
│  │ → 触发 actor reschedule            │   (pubsub_io_context)        │
│  │ → 更新 resource view               │                              │
│  └──────────┬─────────────────────────┘                              │
│             │ 资源视图变更                                             │
│             ▼                                                         │
│  路径3: Ray Syncer（ray_syncer_io_c 线程）                            │
│  ┌────────────────────────────────────┐                              │
│  │ A) 定时同步（受参数控制）：          │                              │
│  │   ray_syncer_message_refresh_ms    │  ← 定期刷新消息              │
│  │   raylet_report_resources_period   │  ← 定期上报资源              │
│  │                                    │                              │
│  │ B) 事件驱动同步（不受定时参数控制）：│                              │
│  │   节点加入   → 建立 gRPC stream    │  ← 立即触发                  │
│  │   节点退出   → 连接断开/清理       │  ← 立即触发                  │
│  │   资源视图变更 → 推送给存活节点     │  ← 立即触发                  │
│  │   连接失败   → 重试逻辑            │  ← 立即触发                  │
│  └────────────────────────────────────┘                              │
└──────────────────────────────────────────────────────────────────────┘
```

### 4.2 关键结论

**节点 dead/alive 检测本身**：
- 走 GCS 主线程的 Health Check Manager
- **不经过** `raylet_report_resources` 定时上报
- **不经过** `ray_syncer_message_refresh` 定时同步
- Health check 是 GCS 主动发 gRPC 探测，与 raylet 上报资源是两条独立路径

**节点 dead/alive 的后续处理会经过 `ray_syncer_io_c`**：
1. 节点 dead → syncer 发现到该节点的 gRPC stream 断开 → **连接错误处理和清理**在 `ray_syncer_io_c`
2. 节点 dead → 资源视图变更 → syncer 向所有存活节点**推送更新后的 resource view** → 经过 `ray_syncer_io_c`
3. 新节点加入 → syncer **建立新的 gRPC stream** → 经过 `ray_syncer_io_c`

**这些都是事件驱动的即时操作，不受 `ray_syncer_message_refresh_interval_ms` 控制。**

### 4.3 ray_syncer_io_c 高 CPU 的两个负载来源

| 负载来源 | 受定时参数控制？ | 当前状况 |
|----------|-----------------|---------|
| 定时资源同步（每 100ms 刷新 × 数百节点） | 是 | 默认 100ms，贡献高基线负载 |
| 节点 dead/alive 事件驱动（连接断开/重建/资源视图推送） | **否**，立即触发 | 今天 938 次 node dead，是主要负载来源 |

**结论**：即使把定时同步间隔调大到 60s 并生效，只要节点持续频繁 dead/alive，`ray_syncer_io_c` 仍然会高。

---

## 5. 关键代码路径

### 5.1 GCS Health Check 路径

```
src/ray/gcs/gcs_server/gcs_health_check_manager.cc:205
  → Health check failed for node XXX, remaining checks N
  → 当 remaining checks = 0 时触发 node dead 回调

src/ray/gcs/gcs_server/gcs_node_manager.cc:666
  → "The node with node id: XXX has been marked dead because
     the detector has missed too many heartbeats"
  → 触发 NODE_DEAD 事件
  → 触发 actor reschedule
```

### 5.2 Actor 失败与重调度路径

```
src/ray/gcs/gcs_server/gcs_actor_manager.cc:1493
  → "Actor is failed on worker XXX at node YYY,
     need_reschedule = 1, death context type = ActorDiedErrorContext,
     remaining_restarts = -1"
  → remaining_restarts = -1 表示无限重启

src/ray/gcs/gcs_server/gcs_actor_scheduler.cc:239
  → "Leasing worker for actor. actor_id=XXX node_id=YYY"
  → GCS 向节点发起 worker lease 请求

src/ray/gcs/gcs_server/gcs_actor_scheduler.cc:577
  → "Failed to lease worker from node XXX for actor YYY
     as the resources are not enough"
  → 资源不足，需要重试其他节点

src/ray/gcs/gcs_server/gcs_actor_scheduler.cc:583
  → "Finished leasing worker from XXX for actor YYY"
  → Worker lease 成功
```

### 5.3 Ray Syncer 路径

```
ray_syncer_io_c 线程:
  → 管理与所有节点的 gRPC stream 连接
  → 定时刷新由 ray_syncer_message_refresh_interval_ms 控制
  → 节点变化时事件驱动的同步不受该参数控制

resource broadcast:
  → gcs_resource_broadcast_max_batch_size 控制单次广播批量大小
  → gcs_resource_broadcast_max_batch_delay_ms 控制批量延迟
```

### 5.4 GCS Event Stats 路径

```
src/ray/gcs/gcs_server/gcs_server.cc:922
  → "Main service Event stats:"
  → 主服务事件循环统计

src/ray/gcs/gcs_server/gcs_server.cc:926
  → "ray_syncer_io_context Event stats:"
  → "pubsub_io_context Event stats:"
  → "task_io_context Event stats:"
  → "ray_event_io_context Event stats:"
  → 各 IO context 事件统计

定时器名称: GCSServer.deadline_timer.debug_state_event_stats_print
打印间隔由 event_stats_print_interval_ms 控制
```

### 5.5 GCS Resource RPC 路径

```
src/ray/gcs/gcs_server/gcs_server.cc:429
  → "Failed to get the resource load: RpcError"
  → GCS 尝试获取节点资源负载失败（节点已不可达）

src/ray/gcs/gcs_server/gcs_server.cc:116
  → "Failed to check if worker is dead on request to raylet"
  → GCS 向 raylet 确认 worker 状态失败
```

---

## 6. GCS Event Stats 关键指标解读

最后一次 stats 输出（15:44-15:46）：

| IO Context | 总事件数 | 活跃数 | 说明 |
|------------|---------|--------|------|
| `pubsub_io_context` | **182,852,970** | 34,097 | 最高事件量，大量 pubsub 订阅连接 |
| `Main service` | **123,582,385** | 17,801 | 主事件循环负载极高 |
| `ray_syncer_io_context` | **101,931,745** | 3,794 | syncer 事件量过亿 |
| `task_io_context` | 31,098,374 | 1 | task event 负载 |
| `ray_event_io_context` | 1,983 | 0 | 可忽略 |

关键定时器队列延迟：
```
debug_state_event_stats_print:
  Queueing time: mean = 52ms, max = 2264ms
  → 最大排队延迟 2.26 秒，说明主线程严重繁忙
```

---

## 7. 优化建议

### 7.1 紧急修复：启动命令分号 bug

**问题**：`--dashboard-agent-listen-port=0;` 后面的分号导致 `--system-config` 未传入。

**修复**（删除分号）：

```bash
ray start --head --port=6379 --dashboard-host=0.0.0.0 --block \
  --num-cpus=0 --num-gpus=0 \
  --dashboard-agent-listen-port=0 \
  --system-config='{"raylet_report_resources_period_milliseconds":20000,"ray_syncer_message_refresh_interval_ms":60000,"gcs_resource_broadcast_max_batch_size":1500,"gcs_resource_broadcast_max_batch_delay_ms":1500,"health_check_period_ms":10000,"health_check_failure_threshold":10,"gcs_server_rpc_server_thread_num":64,"scheduler_avoid_gpu_nodes":false,"event_stats_print_interval_ms":180000,"task_events_report_interval_ms":10000}'
```

### 7.2 参数进一步优化

在修复分号使现有参数生效后，建议进一步调整：

| 参数 | 现有配置 | 建议值 | 原因 |
|------|---------|--------|------|
| `health_check_failure_threshold` | 10 | **20~30** | 10 次 × 10s = 100s 容忍窗口。938 次 node dead 中很多可能是网络抖动误判，加大到 20~30（200~300s 容忍窗口）可减少误判 |
| `health_check_period_ms` | 10000 | **15000~20000** | 600+ 节点每 10s 一次 health check，减轻 GCS 主线程负载 |
| `gcs_resource_broadcast_max_batch_delay_ms` | 1500 | **3000~5000** | 增大批量广播延迟，减少 pubsub_io_context 负载（当前 1.8 亿事件） |
| `event_stats_print_interval_ms` | 180000 | **600000** | stats 打印需遍历所有事件统计，Main service 有 1.2 亿事件，降低频率减轻主线程 |
| 其他参数 | — | 保持现有配置 | `ray_syncer_message_refresh=60000`, `raylet_report_resources=20000` 等已经合理 |

### 7.3 建议新增参数

```json
{
  "num_heartbeats_timeout": 300,
  "gcs_grpc_max_request_queued_max_size": 1000
}
```

- `num_heartbeats_timeout`：增加 raylet→GCS heartbeat 超时次数，与 `health_check_failure_threshold` 配合
- `gcs_grpc_max_request_queued_max_size`：限制 GCS gRPC 队列大小，防止 lease worker 请求 flood

### 7.4 优先级排序

1. **P0 - 修复分号**：让 system-config 生效，降低定时同步基线负载
2. **P1 - 排查节点频繁 dead 根因**：938 次/天是事件驱动负载的根源，也是 GPU 利用率下降的直接原因。需排查物理基础设施（交换机/网络链路）和节点 OOM 情况
3. **P2 - 调大 health check 容忍窗口**：减少网络抖动导致的误判
4. **P3 - 持续监控**：修复后通过 event stats 打印间隔确认参数生效，观察 syncer 事件量是否下降

---

## 8. 验证修复效果的方法

修复后重启集群，通过以下方式确认：

```bash
# 1. 确认 system-config 已传入 gcs_server
cat /proc/<gcs_pid>/cmdline | tr '\0' '\n' | grep config_list
# 解码 base64，应包含 raylet_report_resources_period_milliseconds 等参数

# 2. 确认 event_stats_print_interval_ms 生效
grep 'gcs_server.cc:922' /tmp/ray/session_latest/logs/gcs_server.out \
  | awk '{print substr($0,2,19)}' | tail -5
# 间隔应为 180s（或你配置的值）

# 3. 确认 ray_syncer_io_c CPU 下降
ps -eo pid,tid,pcpu,comm -L | grep ray_syncer
# CPU 应明显低于 56.9%

# 4. 持续观察 node dead 事件
grep 'has been marked dead' /tmp/ray/session_latest/logs/gcs_server.out | wc -l
# 应显著减少（如果同时调大了 health check 容忍窗口）

# 5. 对比 syncer 事件量
grep 'ray_syncer_io_context' /tmp/ray/session_latest/logs/gcs_server.out \
  | grep 'Global stats' | tail -3
# total 事件数增长速率应大幅降低
```
