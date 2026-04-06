# ray_syncer_io_c 线程 CPU 高问题排查实战

> 相关文档：
> - [RaySyncer 资源同步机制与配置参数深度分析](./ray-syncer-resource-sync-mechanism.md)
> - [Ray 调度、Spillback 与资源视图同步机制深度分析](./ray-scheduling-spillback-resource-view-deep-dive.md)
> - [Ray 2.54+ 调度与 GCS 优化](./ray-scheduling-gcs-optimizations-post-2.54.md)

---

## 目录

- [一、问题现象](#一问题现象)
- [二、OnDemandBroadcasting 代码逻辑](#二ondemandbroadcasting-代码逻辑)
- [三、配置参数是否生效的验证方法](#三配置参数是否生效的验证方法)
- [四、BroadcastMessage 与 OnDemandBroadcasting 的关系](#四broadcastmessage-与-ondemandbroadcasting-的关系)
- [五、问题根因分析](#五问题根因分析)
- [六、参数调整建议](#六参数调整建议)
- [七、调整后验证方法](#七调整后验证方法)
- [八、Ray 2.54+ syncer 广播优化（RFC 57640）](#八ray-254-syncer-广播优化rfc-57640)
- [九、实战案例：kml-hb2az1-l3-2 集群（1511 节点，常驻 Actor）](#九实战案例kml-hb2az1-l3-2-集群1511-节点常驻-actor)
- [十、指标采集与分析方法论](#十指标采集与分析方法论)
  - [10.10 batch 参数对 CPU 影响的实测分析](#1010-batch-参数对-cpu-影响的实测分析)
- [十一、实战调参效果验证（2026-05-11）](#十一实战调参效果验证2026-05-11)

---

## 一、问题现象

Head 节点 `ray_syncer_io_c` 线程持续高 CPU（80-100%），常见于 1000+ 节点的大规模集群。即使已调整 `raylet_report_resources_period_milliseconds` 和 `gcs_resource_broadcast_max_batch_size` 等参数，syncer IO 线程 CPU 仍然居高不下。

### 快速确认命令

```bash
# Head 节点执行，确认 syncer 线程 CPU 使用率
top -H -b -n1 | grep ray_syncer_io_c
# 典型输出: 99 root 20 0 26.9g 6.8g 18720 R 99.9 1.4 33:20.24 ray_syncer_io_c
```

### 问题本质

GCS 的 `ray_syncer_io_context` 是**单线程**。所有 Raylet ↔ GCS 的资源同步消息都在这个线程上处理。当集群规模 N 较大时，每条入站消息需要遍历 N 个 reactor 做 `PushToSendingQueue`，总工作量为 O(N²)，单线程容易打满。

---

## 二、OnDemandBroadcasting 代码逻辑

### 源码位置

`src/ray/ray_syncer/ray_syncer.cc`

### 触发机制：定时器驱动，非事件驱动

**关键认知：OnDemandBroadcasting 就是定时广播本身，不是"按需"广播。**

在 `node_manager.cc` 中注册 RESOURCE_VIEW 时：

```cpp
ray_syncer_.Register(
    syncer::MessageType::RESOURCE_VIEW,
    &cluster_resource_scheduler_.GetLocalResourceManager(),
    this,
    report_resources_period_ms_);  // <- raylet_report_resources_period_milliseconds
```

Register 内部创建周期定时器，每隔 `report_resources_period_ms_` 调用一次 OnDemandBroadcasting：

```cpp
void RaySyncer::Register(..., int64_t pull_from_reporter_interval_ms) {
    if (reporter != nullptr && pull_from_reporter_interval_ms > 0) {
        timer_->RunFnPeriodically(
            [this, message_type]() { OnDemandBroadcasting(message_type); },
            pull_from_reporter_interval_ms,
            "RaySyncer.OnDemandBroadcasting");
    }
}
```

### OnDemandBroadcasting 的实现

```cpp
bool RaySyncer::OnDemandBroadcasting(MessageType message_type) {
    auto msg = node_state_->CreateSyncMessage(message_type);
    if (msg) {  // 只有 version_ 递增过（资源有变化）才广播
        BroadcastMessage(std::make_shared<RaySyncMessage>(std::move(*msg)));
        return true;
    }
    return false;  // 资源没变化，跳过，不产生实际广播
}
```

### 版本检查机制

```cpp
std::optional<syncer::RaySyncMessage> LocalResourceManager::CreateSyncMessage(
    int64_t after_version, syncer::MessageType message_type) const {
    if (version_ <= after_version) {
        return std::nullopt;  // 没有新版本，跳过广播
    }
    syncer::RaySyncMessage msg;
    msg.set_version(version_);
    return std::make_optional(std::move(msg));
}
```

`version_` 在资源变化时递增：

```cpp
void LocalResourceManager::OnResourceOrStateChanged() {
    ++version_;
    // ...
}
```

### 总结

```
定时器触发 OnDemandBroadcasting (每 N 毫秒1次, N = raylet_report_resources_period_milliseconds)
    ↓
调用 LocalResourceManager::CreateSyncMessage(after_version)
    ↓
if version_ <= after_version:
    return nullopt  ← 资源没变化，不广播，本次轮空
    ↓
else:
    return 新消息   ← 资源有变化，才真正广播
```

**OnDemandBroadcasting 不一定产生实际上报**。只有资源实际变化（version_ 递增）时才广播，否则跳过。

### `ray_syncer_message_refresh_interval_ms` 的实际作用

**不控制广播频率**，是接收端的消息新鲜度检测，处理协议缺陷导致的消息丢失：

```cpp
timer_->RunFnPeriodically([this]() {
    auto syncer_delay = absl::Milliseconds(
        RayConfig::instance().ray_syncer_message_refresh_interval_ms());
    for (auto &[node_id, resource] : received_node_resources_) {
        if (modified_ts + syncer_delay < absl::Now()) {
            AddOrUpdateNode(node_id, resource);
        }
    }
}, refresh_interval_ms, "ClusterResourceManager.ResetRemoteNodeView");
```

---

## 三、配置参数是否生效的验证方法

### 方法1：从 raylet.out 的 state-dump 指标反推（最可靠）

`event_stats_print_interval_ms` 控制每隔多久输出一次统计。

**验证 `raylet_report_resources_period_milliseconds` 是否生效**：

```bash
grep 'state-dump.*OnDemandBroadcasting' /tmp/ray/session_latest/logs/raylet.out | \
  awk -F'- ' '{print $2}' | awk -F' total' '{print $1}' | \
  awk 'NR>1{diff=$1-prev; print diff} {prev=$1}' | tail -5
```

判断公式：
```
OnDemandBroadcasting 增量 = event_stats_print_interval_ms / raylet_report_resources_period_milliseconds
```

实测数据：
| 节点 | 每 180s 增量 | 计算值 | 对应配置 |
|------|-------------|--------|---------|
| Head | 36 | 180000/5000 = 36 | `raylet_report_resources_period_milliseconds=5000` 生效 |
| Worker | 36 | 180000/5000 = 36 | `raylet_report_resources_period_milliseconds=5000` 生效 |

如果配置没生效（默认 100ms），增量应为 180000/100 = 1800。

**验证 `ray_syncer_message_refresh_interval_ms` 是否生效**：

```bash
grep 'state-dump.*ResetRemoteNodeView' /tmp/ray/session_latest/logs/raylet.out | \
  awk -F'- ' '{print $2}' | awk -F' total' '{print $1}' | \
  awk 'NR>1{diff=$1-prev; print diff} {prev=$1}' | tail -5
```

实测数据：
| 节点 | 每 180s 增量 | 计算值 | 对应配置 |
|------|-------------|--------|---------|
| Head | ~6 | 180000/30000 = 6 | `ray_syncer_message_refresh_interval_ms=30000` 生效 |
| Worker | ~6 | 180000/30000 = 6 | `ray_syncer_message_refresh_interval_ms=30000` 生效 |

### 方法2：从 raylet 启动日志确认

```bash
grep 'GetInternalConfig' /tmp/ray/session_latest/logs/raylet.out | head -3
```

raylet 启动时会调用 `GetInternalConfig` RPC 从 GCS 获取配置。

### 方法3：从 head 的 ray start 命令确认

```bash
ps aux | grep "ray start" | grep -v grep | head -1
```

确认 `--system-config=...` 中包含配置值。

### Worker 侧配置生效确认

Worker 不需要通过 `ray start` 传入 `--system-config`。`--system-config` 只在 head 的 `ray start --head` 时传入，GCS 将配置写入内部 KV。Worker 的 raylet 启动时通过 `GetInternalConfig` RPC 从 GCS 读取，因此 **Worker 的配置也会生效**。

从 Worker 的 raylet.out 日志中同样的 OnDemandBroadcasting 增量（36/180s）和 ResetRemoteNodeView 增量（6/180s）可以确认。

---

## 四、BroadcastMessage 与 OnDemandBroadcasting 的关系

### 不是一一对应的

BroadcastMessage 的统计来源有两处：

**来源1：本地 OnDemandBroadcasting 产生（少量）**

```cpp
bool RaySyncer::OnDemandBroadcasting(MessageType message_type) {
    auto msg = node_state_->CreateSyncMessage(message_type);
    if (msg) {
        BroadcastMessage(std::move(msg));
        return true;
    }
    return false;
}
```

**来源2：收到 GCS 转发的消息（占绝大多数）**

```cpp
void OnReadDone(bool ok) override {
    auto message = std::make_shared<RaySyncMessage>(std::move(read_message_));
    parent_.BroadcastMessage(message);  // 也调了 BroadcastMessage
    StartRead();
}
```

### 用实测数据验证

```
Head:  OnDemandBroadcasting 每180秒 +36,  BroadcastMessage 每180秒 +45000
Worker: OnDemandBroadcasting 每180秒 +36,  BroadcastMessage 每180秒 +46000
```

- OnDemandBroadcasting 36 次/180s = 0.2次/s，大部分返回 false，实际产生广播远少于 36
- BroadcastMessage ~46000 次/180s，Head 和 Worker 数量几乎一致

### 为什么 Head 和 Worker 数量一致

BroadcastMessage 统计的是 **该节点看到的全集群广播流**。Hub-and-Spoke 模型下，GCS 将每条消息转发给所有节点：

```
Worker A 资源变化
  → Worker A OnDemandBroadcasting → BroadcastMessage(1条) → 发给 GCS
    → GCS 转发给其他所有 2510 个节点
      → Head reactor OnReadDone → BroadcastMessage(1条)
      → Worker B reactor OnReadDone → BroadcastMessage(1条)
      → Worker C reactor OnReadDone → BroadcastMessage(1条)
      → ...
```

**每产生 1 次资源变化广播，所有 2512 个节点的 BroadcastMessage 都 +1。**

### 不是所有 Worker 都有资源变化

- 36 x 2511 = **90,396**，但实际 ~46,000，只占一半
- OnDemandBroadcasting 每 5 秒触发一次，大部分 `CreateSyncMessage` 返回 nullopt
- 真正产生广播的只是少数资源频繁变化的节点
- 每次资源变化广播被所有节点接收，所以每个节点看到的 BroadcastMessage 总量相同

### 实际源头广播数推算

```
每 180s BroadcastMessage 增量 ≈ 50,000（每个节点统计到的都一样）
考虑 GCS batching (batch_size=100)：合并后每批广播转发给所有节点
源头广播数 ≈ 50,000 / 2512 ≈ 20 次/180s ≈ 每 9 秒有 1 个节点产生 1 次资源变化
```

---

## 五、问题根因分析

### 根因链条

```
2511 节点超大规模集群
    ↓
大量 actor 调度失败 (532万次 "resources are not enough"，持续不间断)
    ↓
资源视图频繁变动 → 节点 OnDemandBroadcasting 检测到变化 → 广播
    ↓
GCS 收到后转发给所有 2511 个节点 → O(N) 扩散
    ↓
Head 节点 ray_syncer_io_c 线程处理大量 gRPC 读写 → CPU 89.5%
```

### 为什么调大 raylet_report_resources_period_milliseconds 效果不大

1. **只控制"检查资源变化"的频率，不控制"资源变化时是否广播"**
   - 从 100ms 调到 5000ms，每秒从检查 10 次降到 0.2 次
   - 但只要资源持续变化，每次检查都能发现变化并广播
   - 定时器频率降低只减少了空闲时的无效轮询，不减少活跃期的实际广播量

2. **`ray_syncer_message_refresh_interval_ms` 不控制广播频率**
   - 只控制接收端的消息新鲜度检测
   - 对 syncer CPU 几乎无影响

3. **真正驱动 BroadcastMessage 数量的是资源实际变化的频率**，由业务负载决定

### 关键数据

| 指标 | 每 180 秒增量 | 每秒速率 | 来源 |
|------|-------------|---------|------|
| OnDemandBroadcasting (Head) | 36 | 0.2/s | Head 定时器（5秒一次） |
| OnDemandBroadcasting (Worker) | 36 | 0.2/s | Worker 定时器（5秒一次） |
| BroadcastMessage (Head) | ~45,000 | ~250/s | 全集群广播流 |
| BroadcastMessage (Worker) | ~46,000 | ~256/s | 全集群广播流 |
| Failed to lease worker | ~100,000 | ~555/s | actor 调度失败 |

---

## 六、参数调整建议

### 核心原则

**降低 syncer CPU 的核心杠杆是 `raylet_report_resources_period_milliseconds`（减少消息源头），而不是 batch 参数。**

> **实测结论（2026-05-11 验证）：** batch 参数（batch_size/batch_delay）对 syncer CPU 的影响极为有限（< 5%）。
> 增大 batch_delay 会减少 gRPC 写入次数，但每次写入的 protobuf 序列化 + 内核 send buffer 拷贝耗时
> 近似正比增加，总 gRPC IO 工作量几乎不变。详见 [10.10 batch 参数对 CPU 影响的实测分析](#1010-batch-参数对-cpu-影响的实测分析)。

### 当前配置 vs 推荐配置对比

> 以下基于实战案例（kml-hb2az1-l3-2 集群，1511 节点，常驻 Actor）的实测数据。

| 参数 | 当前值 | 推荐值 | 变更 | 效果说明 |
|------|--------|--------|------|---------|
| `raylet_report_resources_period_milliseconds` | 5000 | **10000** | ✅ 需调整 | **最关键**。消息源头速率减半，BroadcastMessage CPU 和 gRPC IO CPU 同步线性下降 |
| `ray_syncer_message_refresh_interval_ms` | 30000 | **60000** | ✅ 需调整 | 常驻 Actor 不需要频繁刷新远端视图，减少定时器开销 |
| `gcs_resource_broadcast_max_batch_size` | 500 | **1000** | 可选调整 | 对 syncer CPU 影响极小（< 5%）。仅在消息洪峰时有去重合并作用 |
| `gcs_resource_broadcast_max_batch_delay_ms` | 500 | **1500** | 可选调整 | 对 syncer CPU 影响极小（< 5%）。增大会恶化调度延迟，不建议超过 1500ms |
| `health_check_period_ms` | 10000 | 10000 | 不变 | — |
| `gcs_server_rpc_server_thread_num` | 64 | 64 | 不变 | — |
| `scheduler_avoid_gpu_nodes` | false | false | 不变 | — |
| `event_stats_print_interval_ms` | 180000 | 180000 | 不变 | — |

### 推荐配置（完整）

```json
{
    "raylet_report_resources_period_milliseconds": 10000,
    "ray_syncer_message_refresh_interval_ms": 60000,
    "gcs_resource_broadcast_max_batch_size": 1000,
    "gcs_resource_broadcast_max_batch_delay_ms": 1500,
    "health_check_period_ms": 10000,
    "gcs_server_rpc_server_thread_num": 64,
    "scheduler_avoid_gpu_nodes": false,
    "event_stats_print_interval_ms": 180000
}
```

### 各参数调整说明

#### `raylet_report_resources_period_milliseconds`: 5000 -> 10000（最关键）

**这是降低 syncer CPU 的核心杠杆。**

syncer 线程的 CPU 由两部分构成：
- **BroadcastMessage**（~12-23%）：ConsumeSyncMessage + PushToSendingQueue × N，与消息到达率**线性正比**
- **gRPC IO 回调**（~72-77%）：protobuf 序列化 + 网络发送，与总数据量**线性正比**

两部分都与消息到达率成正比。减半 report_period → 消息源头减半 → 两部分 CPU 同步减半。

- 每 5 秒检查 -> 每 10 秒检查
- 无资源变化时本来就不广播（version 检查跳过），但有资源变化的节点检测频率减半
- 实测效果：BroadcastMessage/180s 从 ~43,000 降到 ~23,000
- 不要超过 30000ms

#### `gcs_resource_broadcast_max_batch_size`: 500 -> 1000（效果有限）

> **实测结论：batch 参数对 syncer CPU 的影响 < 5%。** 详见 [10.10 实测分析](#1010-batch-参数对-cpu-影响的实测分析)。

GCS 收到节点的 sync 消息后，等聚合到 batch_size 条或等 batch_delay 超时后再广播一次合并后的消息。

**batch 对 gRPC IO 的影响分析：**

增大 batch_delay 会减少 gRPC 写入次数，但每次写入包含更多消息，protobuf 序列化 + 内核 send buffer 拷贝耗时近似正比增加。总工作量 ≈ 写入次数 × per-write 耗时 ≈ 常数。

| 参数组 | 写入频率 | per-write | gRPC IO 总耗时/s |
|--------|---------|-----------|------------------|
| batch_delay=500ms | 3,022/s | 0.26ms | 786ms (77.2%) |
| batch_delay=1500ms | 1,007/s | 0.72ms | 725ms (72.6%) |
| 差异 | ↓67% | ↑177% | **↓仅 7.8%** |

**batch 真正有用的场景：**

batch 的去重合并功能只在**同一节点在 batch_delay 窗口内发送多条更新**时生效。当前场景 report_period(10s) >> batch_delay(1.5s)，同一节点在 1.5s 内最多发 1 条，没有去重机会，batch 退化为纯粹的"打包"。

**无需调整 gRPC 消息大小限制**：
- Ray 默认 gRPC 最大消息限制 = 100MB
- 单条 RaySyncMessage 约 1~5KB
- batch_size=1000 时单次 RPC 约 5MB，远低于限制
- 实测集群中 "message too large" 错误为 0

#### `gcs_resource_broadcast_max_batch_delay_ms`: 500 -> 1500（效果有限）

- 对 syncer CPU 影响 < 5%（见上表）
- 代价：资源视图延迟增加约 1 秒
- 增大 batch_delay 会恶化调度爆发期的性能（更多调度"撞车"）
- 不要超过 3000ms，否则调度延迟明显恶化（实测：batch_delay=3000ms 导致启动调度期额外增加约 3.5 分钟）

#### `ray_syncer_message_refresh_interval_ms`: 30000 -> 60000

- 影响最小，只控制接收端新鲜度检测
- 对 syncer CPU 几乎无影响
- 常驻 Actor 场景下可以放宽，减少定时器开销

### 预期效果

| 指标 | 调整前 | 调整后预期 | 主要贡献参数 |
|------|-------|-----------|-------------|
| 源头消息率 | ~126/s | ~63/s | `report_period` 翻倍 |
| GCS BroadcastMessage | ~239/s | ~120/s | `report_period` 翻倍 |
| BroadcastMessage CPU | ~22.7% | ~11.4% | `report_period`（线性下降） |
| gRPC IO CPU | ~77.2% | ~70-73% | `report_period`（线性下降）+ batch（< 5%） |
| `ray_syncer_io_c` 总 CPU | **99.9%** | **~82-85%** | 主要来自 `report_period` |
| BroadcastMessage /180s | ~43,000 | ~22,000 | `report_period` |
| OnDemandBroadcasting /180s | 36 | 18 | `report_period` |
| ResetRemoteNodeView /180s | 6 | 3 | `refresh_interval` |
| 资源视图最大延迟 | <1s | <12s | `report_period` + `batch_delay` |

> **注意：** 原文档预期 syncer CPU 降到 35-50%，实测为 84.2%（见第十一章）。
> 差异原因：batch 参数对 gRPC IO CPU 的影响被高估，实际影响 < 5%。
> CPU 降幅的 71% 来自 `report_period` 对 BroadcastMessage CPU 的减少，
> 仅 29% 来自 gRPC IO 的微小下降。

### 调参约束

```
约束1: ray_syncer_message_refresh_interval_ms >> raylet_report_resources_period_milliseconds
       60000 >> 10000 ✓ (否则 Raylet 会误判远端节点过期)

约束2: gcs_resource_broadcast_max_batch_size > 1 时，gcs_resource_broadcast_max_batch_delay_ms 才生效
       1000 > 1 ✓ (batch_size=1 时批处理禁用)

约束3: gcs_resource_broadcast_max_batch_delay_ms 应 < raylet_report_resources_period_milliseconds
       1500 < 10000 ✓ (否则广播延迟超过一次上报周期)

约束4: 调大 batch_size 无需调整 gRPC 消息大小限制
       (Ray 默认 100MB，batch_size=1000 时约 5MB)

约束5: 资源视图最大延迟 ≈ report_period + batch_delay + network_latency
       ≈ 10000 + 1500 + ~100 ≈ 11.6s
       占分钟级 task 时长 < 20% ✓
```

### 效果不够时的进一步方案

如果上述调整后 syncer CPU 仍然偏高（实测约 84%），**核心杠杆是继续增大 `report_period`**：

```json
{
    "raylet_report_resources_period_milliseconds": 20000
}
```

`report_period` 从 10000 调到 20000，消息源头再减半，预期 syncer CPU 线性下降约 50%（84% → ~42%）。

对常驻 Actor 来说 `report_period=20s` 完全可接受——Actor 创建时的调度延迟最坏增加 20s，但只发生在启动阶段，运行态零影响。资源视图最大延迟约 21.5s。

> **不建议通过增大 batch 参数来降低 CPU**：实测证明 batch_size/batch_delay 对 syncer CPU 影响 < 5%，
> 且增大 batch_delay 会恶化调度爆发期的性能（实测 batch_delay=3000ms 导致启动调度期额外 +3.5 分钟）。

---

## 七、调整后验证方法

### 1. 确认配置生效

```bash
# OnDemandBroadcasting 每180秒增量应为 180000/10000 = 18
grep 'state-dump.*OnDemandBroadcasting' /tmp/ray/session_latest/logs/raylet.out | \
  awk -F'- ' '{print $2}' | awk -F' total' '{print $1}' | \
  awk 'NR>1{diff=$1-prev; print diff} {prev=$1}' | tail -5
# 预期输出: 18 18 18 18 18

# ResetRemoteNodeView 每180秒增量应为 180000/60000 = 3
grep 'state-dump.*ResetRemoteNodeView' /tmp/ray/session_latest/logs/raylet.out | \
  awk -F'- ' '{print $2}' | awk -F' total' '{print $1}' | \
  awk 'NR>1{diff=$1-prev; print diff} {prev=$1}' | tail -5
# 预期输出: 3 3 3 3 3
```

### 2. 确认广播量下降

```bash
# BroadcastMessage 每180秒增量应从 ~50000 降到 ~10000-15000
grep 'state-dump.*BroadcastMessage' /tmp/ray/session_latest/logs/raylet.out | \
  awk -F'- ' '{print $2}' | awk -F' total' '{print $1}' | \
  awk 'NR>1{diff=$1-prev; print diff} {prev=$1}' | tail -5
```

### 3. 确认 syncer CPU 下降

```bash
top -H -b -n1 | grep ray_syncer_io_c
```

### 4. 确认 gRPC 无消息超限错误

```bash
grep -c 'message too large' /tmp/ray/session_latest/logs/gcs_server.out
# 预期输出: 0
```

### 预期效果

| 指标 | 调整前 | 调整后预期 | 实测值 |
|------|-------|-----------|--------|
| BroadcastMessage /180s | ~50,000 | ~22,000 | ~23,000 |
| ray_syncer_io_c CPU | ~89% | ~82-85% | 84.2% |
| OnDemandBroadcasting /180s | 36 | 18 | 18 |
| ResetRemoteNodeView /180s | 6 | 3 | 3 |
| 资源视图延迟 | <1s | <12s | <12s |

> **注意：** 原预期 syncer CPU 降到 30-50%，实测为 84.2%。
> 原因是 batch 参数对 gRPC IO CPU 的影响被高估（实测 < 5%），
> CPU 降幅主要来自 `report_period` 翻倍。详见 [10.10 实测分析](#1010-batch-参数对-cpu-影响的实测分析)。

---

## 八、Ray 2.54+ syncer 广播优化（RFC 57640）

### 核心 Issue

**[ray-project/ray #57640] [Core][RFC] Improve Large-Scale Resource View Synchronization Through Sync Message Batching**
- 链接：https://github.com/ray-project/ray/issues/57640
- 提出时间：2025年10月

### 问题描述

现有 RaySyncer 是 push 模型：raylet 每次资源变化 -> 立即推送到 GCS -> GCS 立即广播给所有 raylet。在大集群中形成 O(N^2) 同步放大和级联更新正反馈循环。

在 1000 节点集群中，创建 placement group 并调用 `pg.ready()` 需长达 10 分钟。

### 优化方案

GCS 侧引入 batch 机制：

1. GCS 不再立即转发每条 sync 消息
2. 等待可配置超时聚合多条 sync 消息
3. 合并资源差异
4. 一次广播合并后的资源视图给所有 raylet

```
旧模式: raylet变化 -> GCS -> 立即广播 -> 所有raylet处理 -> 可能触发新变化 -> ...
新模式: raylet变化 -> GCS等待batch -> 合并多条diff -> 一次广播 -> 所有raylet处理
```

### 新增配置参数

| 参数 | 说明 | 推荐值（1000节点） |
|------|------|------|
| `gcs_resource_broadcast_max_batch_size` | 批次最大容量 | 1000 |
| `gcs_resource_broadcast_max_batch_delay_ms` | 广播前最大等待时间 | 500ms |

### 效果

- 1000 节点集群：placement group 调度从 **10分钟降至1分钟内**
- 广播消息数量大幅减少
- 集群资源视图收敛更快

### 对当前版本的影响

当前版本 (2.54.4+kuaishou) 已包含 `gcs_resource_broadcast_max_batch_size` 和 `gcs_resource_broadcast_max_batch_delay_ms` 参数，说明快手版已合入部分 batching 优化。

但 #57640 是 2025年10月的 RFC，可能还有更深入的优化未合入。从 master 分支看，`gcs_resource_broadcast_max_batch_size` 默认值是 1（即默认不启用 batching），当前设置 100 已在使用 batching。

### 其他相关 issue

- **gcs_server 100% CPU**：https://discuss.ray.io/t/gcs-server-takes-almost-100-cpu-even-though-theres-no-running-task/6475
- **大集群 GCS 元数据积累导致调度延迟**：https://discuss.ray.io/t/gcs-metadata-accumulation-causing-scheduling-delays/23341

---

## 九、实战案例：kml-hb2az1-l3-2 集群（1511 节点，常驻 Actor）

> 环境信息：
> - 集群：kml-hb2az1-l3-2，namespace=lmserving
> - 节点数：1511
> - Head 节点 CPU：128 核
> - 业务类型：**常驻 Actor 执行分钟级 task**（LLM Serving 场景）
> - 排查时间：2026-05-10

### 9.1 排查步骤总览

```
步骤1: 确认 syncer 线程 CPU          → top -H 看 ray_syncer_io_c
步骤2: 确认当前配置参数              → 从 /proc/1/cmdline 提取 system-config
步骤3: 采集 state-dump 关键指标      → 从 raylet.out / gcs_server.out 提取增量
步骤4: 对比 Head 与 Worker 行为差异  → 判断资源变化的源头
步骤5: 计算 CPU 瓶颈公式            → 量化 O(N) 遍历开销
步骤6: 确定参数调整方案              → 针对常驻 Actor 场景优化
```

### 9.2 步骤1：确认 syncer 线程 CPU

**命令（Head 节点执行）：**

```bash
top -H -b -n1 | grep ray_syncer_io_c
```

**实际输出：**

```
99 root      20   0   26.9g   6.8g  18720 R  99.9   1.4  33:20.24 ray_syncer_io_c
```

**结论：** `ray_syncer_io_c` 线程 CPU **99.9%**，单线程完全打满。

**补充：确认系统整体负载**

```bash
top -H -b -n1 | head -7
```

```
top - 19:26:16 up 268 days,  4:08,  0 users,  load average: 22.24, 22.33, 21.79
Threads: 1922 total,   9 running, 1913 sleeping,   0 stopped,   0 zombie
%Cpu(s):  7.8 us,  3.3 sy,  0.0 ni, 87.8 id,  0.0 wa,  0.4 hi,  0.6 si,  0.0 st
MiB Mem : 515034.8 total, 324606.5 free,  37170.0 used, 153258.4 buff/cache
```

系统整体 idle 87.8%，只是 syncer 单线程打满。128 核 Head 节点 CPU 资源充裕，瓶颈在单线程。

### 9.3 步骤2：确认当前配置参数

**命令（Head 节点执行）：**

从 Head 进程的 cmdline 提取各配置项：

```bash
cat /proc/1/cmdline | tr '\0' '\n' | grep batch_size
cat /proc/1/cmdline | tr '\0' '\n' | grep batch_delay
cat /proc/1/cmdline | tr '\0' '\n' | grep report_resources
cat /proc/1/cmdline | tr '\0' '\n' | grep refresh_interval
```

**实际输出：**

```
"gcs_resource_broadcast_max_batch_size": 500,
"gcs_resource_broadcast_max_batch_delay_ms": 500,
"raylet_report_resources_period_milliseconds": 5000,
"ray_syncer_message_refresh_interval_ms": 30000,
```

**当前配置总结：**

```json
{
    "raylet_report_resources_period_milliseconds": 5000,
    "ray_syncer_message_refresh_interval_ms": 30000,
    "gcs_resource_broadcast_max_batch_size": 500,
    "gcs_resource_broadcast_max_batch_delay_ms": 500,
    "health_check_period_ms": 10000,
    "gcs_server_rpc_server_thread_num": 64,
    "scheduler_avoid_gpu_nodes": false,
    "event_stats_print_interval_ms": 180000
}
```

**判断方式说明：**

| 方法 | 命令 | 适用场景 |
|------|------|---------|
| 从 cmdline 提取 | `cat /proc/1/cmdline \| tr '\0' '\n' \| grep <参数名>` | Head 节点，直接看 ray start 启动参数 |
| 从 raylet.out 反推 | 看 OnDemandBroadcasting 增量是否等于 `180000/report_period` | Head 和 Worker 都可用，最可靠 |
| 从 ps 提取 | `ps aux \| grep 'ray start' \| grep -v grep` | Head 节点 |

### 9.4 步骤3：采集 state-dump 关键指标

**数据来源说明：** Ray 的 `event_stats` 机制会按 `event_stats_print_interval_ms`（本集群 180000ms = 3 分钟）的间隔，将各事件的累计统计写入日志文件。通过计算相邻两次统计的差值（增量），可以得到每 180 秒的实际触发次数。

#### 9.4.1 Raylet 端指标（Head 节点的 raylet.out）

**命令：**

```bash
# BroadcastMessage 增量：反映该节点看到的全集群广播流量
grep 'state-dump.*BroadcastMessage' /tmp/ray/session_latest/logs/raylet.out | \
  awk -F'- ' '{print $2}' | awk -F' total' '{print $1}' | \
  awk 'NR>1{diff=$1-prev; print diff} {prev=$1}' | tail -5

# OnDemandBroadcasting 增量：验证 report_period 配置
grep 'state-dump.*OnDemandBroadcasting' /tmp/ray/session_latest/logs/raylet.out | \
  awk -F'- ' '{print $2}' | awk -F' total' '{print $1}' | \
  awk 'NR>1{diff=$1-prev; print diff} {prev=$1}' | tail -5

# ResetRemoteNodeView 增量：验证 refresh_interval 配置
grep 'state-dump.*ResetRemoteNodeView' /tmp/ray/session_latest/logs/raylet.out | \
  awk -F'- ' '{print $2}' | awk -F' total' '{print $1}' | \
  awk 'NR>1{diff=$1-prev; print diff} {prev=$1}' | tail -5
```

**实际输出：**

```
# BroadcastMessage 增量
42002
43882
42583
44028
43119

# OnDemandBroadcasting 增量
36
36
36
36
36

# ResetRemoteNodeView 增量
6
6
6
5
6
```

**解读指标含义：**

| 指标 | 每 180s 增量 | 含义 | 数据来源（日志文件） |
|------|-------------|------|-----------------|
| `BroadcastMessage` | ~42,000-44,000 | 该节点看到的全集群资源广播总量 | `raylet.out`（Raylet 主线程统计） |
| `OnDemandBroadcasting` | 36 | 本地定时器触发次数 = 180000/5000 | `raylet.out`（Raylet 主线程统计） |
| `ResetRemoteNodeView` | 6 | 脏视图重置定时器触发次数 = 180000/30000 | `raylet.out`（Raylet 主线程统计） |

**配置生效验证公式：**

```
OnDemandBroadcasting 增量 = event_stats_print_interval_ms / raylet_report_resources_period_milliseconds
预期: 180000 / 5000 = 36  ← 与实际 36 吻合 ✓

ResetRemoteNodeView 增量 = event_stats_print_interval_ms / ray_syncer_message_refresh_interval_ms
预期: 180000 / 30000 = 6  ← 与实际 6 吻合 ✓
```

**如果配置没生效（使用默认值），预期增量为：**

| 参数 | 默认值 | 默认增量 | 实际增量 | 生效？ |
|------|--------|---------|---------|--------|
| `report_period` | 100ms | 180000/100 = 1800 | 36 | ✓ 生效 |
| `refresh_interval` | 3000ms | 180000/3000 = 60 | 6 | ✓ 生效 |

#### 9.4.2 Raylet 端指标（Head 节点详细统计行）

**命令：**

```bash
# 查看完整统计行（含 mean/max/min）
grep 'state-dump.*OnDemandBroadcasting' /tmp/ray/session_latest/logs/raylet.out | tail -1
grep 'state-dump.*BroadcastMessage' /tmp/ray/session_latest/logs/raylet.out | tail -1
```

**实际输出：**

```
# Head 的 OnDemandBroadcasting
RaySyncer.OnDemandBroadcasting - 719 total (1 active),
  Execution time: mean = 0.01ms, total = 5.31ms,
  Queueing time: mean = 7.40ms, max = 59.21ms, min = 0.00ms, total = 5318.31ms

# Head 的 BroadcastMessage
RaySyncer.BroadcastMessage - 582753 total (0 active),
  Execution time: mean = 0.01ms, total = 8149.98ms,
  Queueing time: mean = 0.00ms, max = 0.05ms, min = 0.00ms, total = 59.89ms
```

**关键解读：**

- **Head 的 `OnDemandBroadcasting` exec mean = 0.01ms**：极短，说明 Head 本地没有资源变化（`CreateSyncMessage` 返回 `nullopt`，version 没递增）。Head 节点主要是 GCS 进程，不运行用户 Actor/Task。
- **Head 的 `BroadcastMessage` exec mean = 0.01ms**：这是 Raylet 主线程统计，Raylet 只需要处理 GCS 转发来的消息（`ConsumeSyncMessage` 版本检查 + 更新本地资源视图），开销很小。
- **Queueing time max = 59.21ms**：偶尔有主线程繁忙导致定时器排队，但 mean = 7.40ms 尚在可接受范围。

#### 9.4.3 GCS 端指标（gcs_server.out）

**命令：**

```bash
# GCS 的 BroadcastMessage 统计
grep 'BroadcastMessage' /tmp/ray/session_latest/logs/gcs_server.out | tail -3

# GCS 的 BroadcastMessage 增量
grep 'BroadcastMessage' /tmp/ray/session_latest/logs/gcs_server.out | \
  awk '{print $3}' | awk 'NR>1{print $1-prev} {prev=$1}' | tail -10
```

**实际输出：**

```
# GCS BroadcastMessage 详细统计
RaySyncer.BroadcastMessage - 457452 total (0 active),
  Execution time: mean = 0.95ms, total = 435321.06ms,
  Queueing time: mean = 0.00ms, max = 2.47ms

RaySyncer.BroadcastMessage - 500341 total (1 active, 1 running),
  Execution time: mean = 0.95ms, total = 476710.57ms

RaySyncer.BroadcastMessage - 541299 total (0 active),
  Execution time: mean = 0.95ms, total = 516129.33ms

# GCS BroadcastMessage 增量
43610
45049
44008
42220
43833
42645
43974
43625
42889
40958
```

**关键解读：**

- **GCS `BroadcastMessage` exec mean = 0.95ms**：比 Raylet 端（0.01ms）高两个数量级。因为 GCS 的 `BroadcastMessage` 包含**遍历所有 1511 个 reactor 的 `PushToSendingQueue`**（O(N) 操作）。
- **GCS BroadcastMessage 增量 ~43,000/180s**：与 Raylet 端的 BroadcastMessage 增量一致。GCS 统计的是入站消息数（收到多少就处理多少），Raylet 统计的是收到 GCS 转发的消息数。在 Hub-and-Spoke 模型下两者一致。
- **注意：** GCS 的 `BroadcastMessage` 运行在 **`ray_syncer_io_context` 独立线程**上，与 Raylet 的 `BroadcastMessage`（运行在 Raylet 主线程）是不同线程的统计。

#### 9.4.4 其他辅助指标

**命令：**

```bash
# 集群节点数
ray status 2>/dev/null | grep -c 'node_'

# 调度失败计数
grep -c 'resources are not enough' /tmp/ray/session_latest/logs/gcs_server.out 2>/dev/null

# Head 节点 CPU 核数
nproc
```

**实际输出：**

```
# 集群节点数
1511

# "resources are not enough" 计数
5801

# Head CPU 核数
128
```

### 9.5 步骤4：对比 Head 与 Worker 行为差异

**目的：** 确认资源变化的真实来源。Head 节点主要运行 GCS，不跑用户 Actor。资源变化来自 Worker 节点。

**Worker 节点命令：**

```bash
# Worker 的 OnDemandBroadcasting 详细统计
grep 'state-dump.*OnDemandBroadcasting' /tmp/ray/session_latest/logs/raylet.out | tail -3

# Worker 的 BroadcastMessage 详细统计
grep 'state-dump.*BroadcastMessage' /tmp/ray/session_latest/logs/raylet.out | tail -3

# Worker 的 ResetRemoteNodeView 详细统计
grep 'state-dump.*ResetRemoteNodeView' /tmp/ray/session_latest/logs/raylet.out | tail -3
```

**Worker 实际输出：**

```
# Worker OnDemandBroadcasting
RaySyncer.OnDemandBroadcasting - 648 total, Execution time: mean = 0.17ms
RaySyncer.OnDemandBroadcasting - 684 total, Execution time: mean = 0.17ms
RaySyncer.OnDemandBroadcasting - 720 total, Execution time: mean = 0.18ms

# Worker BroadcastMessage
RaySyncer.BroadcastMessage - 542216 total, Execution time: mean = 0.03ms
RaySyncer.BroadcastMessage - 584589 total, Execution time: mean = 0.03ms
RaySyncer.BroadcastMessage - 629172 total, Execution time: mean = 0.03ms

# Worker ResetRemoteNodeView
ClusterResourceManager.ResetRemoteNodeView - 108 total, Execution time: mean = 0.27ms
ClusterResourceManager.ResetRemoteNodeView - 114 total, Execution time: mean = 0.27ms
ClusterResourceManager.ResetRemoteNodeView - 120 total, Execution time: mean = 0.27ms
```

**Head vs Worker 对比：**

| 指标 | Head exec mean | Worker exec mean | 差异原因 |
|------|---------------|-----------------|---------|
| `OnDemandBroadcasting` | **0.01ms** | **0.17ms** | Head 全部空跑(nullopt)；Worker 有实际广播(version++) |
| `BroadcastMessage` | 0.01ms | 0.03ms | 两端都只做 ConsumeSyncMessage，差异不大 |
| `ResetRemoteNodeView` | — | 0.27ms | 需遍历所有远端节点视图 |

**关键结论：Worker 的 `OnDemandBroadcasting` exec mean = 0.17ms，远高于 Head 的 0.01ms，说明 Worker 确实在持续产生真实广播（`CreateSyncMessage` 返回了实际消息，version 在递增）。**

### 9.6 步骤5：计算 CPU 瓶颈公式

#### 9.6.1 资源变化频率推算

Worker 的 `OnDemandBroadcasting` 每 180s 触发 36 次，exec mean = 0.17ms。对比 Head 空跑时的 0.01ms，推算实际广播比例：

```
空跑执行时间: ~0.01ms (Head 实测)
实际广播执行时间: ~0.4ms (含 CreateSyncMessage + BroadcastMessage)
加权平均: 0.17 = p × 0.4 + (1-p) × 0.01
解得: p ≈ 41%
```

即每个 Worker 每 180s 的 36 次 OnDemandBroadcasting 中，约 **15 次**（41%）检测到资源变化并产生真实广播。

```
每 Worker 源头广播: ~15 次/180s
总源头广播: 15 × 1511 = 22,665 次/180s = 126 条/s
```

#### 9.6.2 GCS CPU 瓶颈分析

GCS `BroadcastMessage` 统计的 43,000/180s = 239/s，这是 GCS 实际处理的入站消息数（包含版本去重后通过的消息）。

```
GCS BroadcastMessage 处理速率: 239 条/s
每条 BroadcastMessage 的工作:
  1. ConsumeSyncMessage 版本检查 (纳秒级)
  2. PushToSendingQueue × 1511 个 reactor (O(N) 遍历)
  3. 统计的 exec mean = 0.95ms

仅 BroadcastMessage 执行时间占用:
  239/s × 0.95ms = 227ms/s = 22.7% CPU
```

**但实际 CPU 是 99.9%，差距来自未计入 exec mean 的异步 gRPC IO 工作：**

```
被统计的 (0.95ms/次):
  └─ ConsumeSyncMessage + PushToSendingQueue × 1511

未被统计的 (在同一线程上执行):
  ├─ gRPC OnWriteDone 回调 (每个 reactor 写入完成后触发)
  ├─ StartSend → OnSendDone 链式回调 (发送缓冲区排空)
  ├─ gRPC OnReadDone 回调 (收到 Raylet 消息时触发)
  ├─ gRPC stream 管理开销 (连接维护、心跳等)
  └─ 批处理定时器回调 (batch_delay 超时触发发送)

总 PushToSendingQueue 调用:
  239/s × 1511 = 361,129 次/s

每次 PushToSendingQueue 后续可能触发:
  └─ StartSend (如果是第一条或 batch 满) → gRPC StreamWrite → OnWriteDone
```

**完整 CPU 开销模型：**

```
CPU ≈ BroadcastMessage 执行 (22.7%)
    + gRPC OnWriteDone 回调 (~30-40%)
    + gRPC OnReadDone 回调 (~10-15%)
    + batch 定时器 + stream 管理 (~10-15%)
    ≈ 100%
```

### 9.7 步骤6：确定参数调整方案

#### 9.7.1 常驻 Actor 场景的特点

| 特征 | 影响 |
|------|------|
| Actor 一旦创建就长期存在 | 调度延迟只在创建阶段有意义 |
| Task 是分钟级别的 | 资源视图延迟 10-20s 完全可接受（占 task 时长 <30%） |
| 资源变化主要来自 **primary object 的 pin/unpin 周期**（Object Store 内存波动） | 对调度决策无实际影响，但持续触发 version++（详见 9.9 根因分析）。注意：Actor Worker 的 `ray.get()` 不触发 CPU block/unblock |
| 不需要频繁重新调度 | 可以大幅降低资源上报频率 |

#### 9.7.2 推荐配置

```json
{
    "raylet_report_resources_period_milliseconds": 10000,
    "ray_syncer_message_refresh_interval_ms": 60000,
    "gcs_resource_broadcast_max_batch_size": 1000,
    "gcs_resource_broadcast_max_batch_delay_ms": 1500,
    "health_check_period_ms": 10000,
    "gcs_server_rpc_server_thread_num": 64,
    "scheduler_avoid_gpu_nodes": false,
    "event_stats_print_interval_ms": 180000
}
```

#### 9.7.3 各参数调整说明

| 参数 | 调整前 | 调整后 | 效果 |
|------|--------|--------|------|
| `report_period` | 5000 | **10000** | 检测频率减半，源头消息从 ~126/s 降到 ~63/s |
| `batch_size` | 500 | **1000** | 提高合并上限，配合更大 delay 使用 |
| `batch_delay` | 500 | **1500** | 等更久攒更多消息，GCS 实际发送次数减少 ~60% |
| `refresh_interval` | 30000 | **60000** | 常驻 Actor 不需要频繁刷新远端视图，减少定时器开销 |

**核心原理：** `report_period` 控制源头消息产生速率，`batch_size` + `batch_delay` 控制 GCS 发送次数。两者配合可显著降低 syncer 线程的 O(N) 遍历频率。

#### 9.7.4 预期效果量化

```
调整前:
  源头消息率: ~126/s (1511 nodes × 15 broadcasts/180s)
  GCS BroadcastMessage: ~239/s
  GCS PushToSendingQueue: ~361,129/s
  syncer CPU: 99.9%

调整后:
  源头消息率: ~63/s (report_period 翻倍)
  batch_delay 1500ms 合并更多消息: 实际 GCS 发送降到 ~80/s
  GCS PushToSendingQueue: ~120,000/s (降 67%)
  syncer CPU: ~35-50%
```

#### 9.7.5 如果仍然不够的激进方案

```json
{
    "raylet_report_resources_period_milliseconds": 20000,
    "gcs_resource_broadcast_max_batch_size": 2000,
    "gcs_resource_broadcast_max_batch_delay_ms": 3000
}
```

对常驻 Actor 来说 `report_period=20s` 完全可接受——Actor 创建时的调度延迟最坏增加 20s，但只发生在启动阶段，运行态零影响。资源视图最大延迟约 23.5s。

### 9.8 采集到的完整数据汇总

| 指标 | 值 | 数据来源 | 采集命令 |
|------|-----|---------|---------|
| `ray_syncer_io_c` CPU | **99.9%** | Head 节点 `top -H` | `top -H -b -n1 \| grep ray_syncer_io_c` |
| 集群节点数 | **1511** | `ray status` | `ray status \| grep -c 'node_'` |
| Head CPU 核数 | 128 | `nproc` | `nproc` |
| GCS `BroadcastMessage` /180s | **~43,000** | `gcs_server.out` state-dump | `grep 'BroadcastMessage' gcs_server.out` 增量 |
| GCS `BroadcastMessage` exec mean | **0.95ms** | `gcs_server.out` state-dump | 同上，读 `Execution time: mean` |
| Raylet `BroadcastMessage` /180s | **~43,000** | Head `raylet.out` state-dump | `grep 'BroadcastMessage' raylet.out` 增量 |
| Raylet `OnDemandBroadcasting` /180s | **36** | Head `raylet.out` state-dump | `grep 'OnDemandBroadcasting' raylet.out` 增量 |
| Head `OnDemandBroadcasting` exec mean | **0.01ms** | Head `raylet.out` state-dump | 确认 Head 全空跑 |
| Worker `OnDemandBroadcasting` exec mean | **0.17ms** | Worker `raylet.out` state-dump | 确认 Worker 有实际广播 |
| Raylet `ResetRemoteNodeView` /180s | **6** | Head `raylet.out` state-dump | `grep 'ResetRemoteNodeView' raylet.out` 增量 |
| `resources are not enough` | **5801** | `gcs_server.out` | `grep -c 'resources are not enough' gcs_server.out` |
| `batch_size` 配置 | 500 | Head `/proc/1/cmdline` | `cat /proc/1/cmdline \| tr '\0' '\n' \| grep batch_size` |
| `batch_delay` 配置 | 500 | Head `/proc/1/cmdline` | 同上 |
| `report_period` 配置 | 5000 | Head `/proc/1/cmdline` | 同上 |
| `refresh_interval` 配置 | 30000 | Head `/proc/1/cmdline` | 同上 |

### 9.9 根因链条总结

#### 9.9.1 `version_++` 触发源码分析

`version_` 只在 `LocalResourceManager::OnResourceOrStateChanged()` 中递增（`local_resource_manager.cc:455`）。对于**常驻 Actor 场景**，各触发路径的活跃程度：

| 触发路径 | 代码位置 | 触发条件 | 常驻 Actor 下是否持续触发 |
|---------|---------|---------|------------------------|
| **Object Store 内存变化（primary copy pin/unpin）** | `UpdateAvailableObjectStoreMemResource` (line 350) | `CreateSyncMessage` 内 `const_cast` 惰性调用，仅当 `GetPrimaryBytes()` 变化时触发 | **是 — 唯一的主要源头**（详见 9.9.2） |
| CPU block/unblock | `AddResourceInstances` (line 218) / `SubtractResourceInstances` (line 243) | Worker 调用 `ray.get()` 阻塞时释放 CPU | **否 — Actor Worker 不触发**（详见 9.9.3） |
| Task 资源分配 | `AllocateLocalTaskResources` (line 286) | 仅 Actor 创建时调用一次 | 否（一次性） |
| Task 资源释放 | `ReleaseWorkerResources` (line 308) | 仅 Actor 销毁时调用一次 | 否（一次性） |
| NODE_WORKERS busy/idle | `MarkFootprintAsBusy` (line 143) / `MarkFootprintAsIdle` (line 192) | 首次 lease 授予时 busy，最后一个 lease 释放时 idle | 否（有 guard 防重复） |
| PULLING_TASK_ARGUMENTS | `MaybeMarkFootprintAsBusy` (line 166) | 等待拉取 task 参数时 | 否（Actor 不经历调度） |
| 动态资源增删 | `AddLocalResourceInstances` (line 63) / `DeleteLocalResource` (line 70) | Placement Group 操作或资源 resize | 否（非常规操作） |
| Draining 状态 | `SetLocalNodeDraining` (line 531) | 自动扩缩器 drain 请求 | 否（一次性） |

**关键结论：常驻 Actor 场景下，`version_++` 的唯一持续触发源是 Object Store 内存波动（primary object 的 pin/unpin）。**

**关于 Actor busy/idle 和 CPU block/unblock 的澄清：**

- `MarkFootprintAsBusy(NODE_WORKERS)` 在 `Grant()` 时调用一次，且有 guard：如果已经 busy 则直接 return，不会递增 version（`local_resource_manager.cc:130-132`）
- `MarkFootprintAsIdle(NODE_WORKERS)` 只在 `leased_workers_` 变空时调用（`node_manager.h:371`），常驻 Actor 场景下不会发生
- Actor 后续 task 执行是 Core Worker → Actor Worker 直连，**不经过 Raylet 调度系统**，不触发任何资源分配/释放
- Actor Worker 内部的 `ray.get()` 不触发 CPU block/unblock（详见 9.9.3）

#### 9.9.2 Object Store 内存波动机制（唯一的 `version_++` 持续触发源）

`UpdateAvailableObjectStoreMemResource` 在每次 `CreateSyncMessage` 时被 `const_cast` 调用（`local_resource_manager.cc:428`）：

```cpp
const_cast<LocalResourceManager *>(this)->UpdateAvailableObjectStoreMemResource();
```

当 `scheduler_report_pinned_bytes_only=true`（默认）时，读取的是 `LocalObjectManager::GetPrimaryBytes()` = `pinned_objects_size_ + num_bytes_pending_spill_`。

**关键区分：只有 primary copy 的 pin/unpin 才会改变 `pinned_objects_size_`。**

`pinned_objects_size_` 的 4 个修改点（`local_object_manager.cc`）：

| 代码行 | 操作 | 触发时机 |
|-------|------|---------|
| line 50 | `+= object->GetSize()` | `PinObjectsAndWaitForFree()` — Owner 通过 `PinObjectIDs` RPC 固定 primary copy |
| line 130 | `-= pinned_objects_it->second->GetSize()` | `ReleaseFreedObject()` — Owner 释放引用（Python 对象被 GC）或 Owner 进程死亡 |
| line 315 | `-= object_size` | `SpillObjectsInternal()` — Object 从 pinned 状态转入 pending-spill |
| line 373 | `+= it->second->GetSize()` | Spill 失败回调 — Object 从 pending-spill 回到 pinned |

**Primary copy vs Secondary copy 的区别：**

```
Node A (GPU Worker, 运行 Mapper Actor)        Node B (CPU Worker, 运行预处理 Actor)
─────────────────────────────────────         ──────────────────────────────────────

actor.preprocess_video.remote(data) ──────→ 执行 preprocess_video()
                                              │
                                              ├─ 返回值序列化为 Ray Object
                                              ├─ Object 存入 Node B 的 Plasma（PRIMARY COPY）
                                              ├─ Core Worker 调用 PinObjectIDs RPC → Node B Raylet
                                              └─ pinned_objects_size_ += size  ← version_++ 在 Node B!

ray.get(future) ──────────────────────────→ Object Pull: Node B → Node A
  │                                           │
  ├─ Object 存入 Node A 的 Plasma              ├─ 这是 SECONDARY COPY
  │  （仅 ObjectManager::used_memory_ 增加）   │  （LocalObjectManager::pinned_objects_size_ 不变）
  │                                           │
  ├─ scheduler_report_pinned_bytes_only=true  │
  │  → GetPrimaryBytes() 不变                  │
  │  → version 不递增 ← Node A 不触发!        │
  │                                           │
  └─ 处理完毕，Python 释放引用
       → Owner 发布 WORKER_OBJECT_EVICTION
       → Node B: ReleaseFreedObject()
       → pinned_objects_size_ -= size  ← version_++ 又在 Node B!
```

**对 LLM Serving 场景的具体影响：**

- **触发 `version_++` 的节点是 CPU Actor 所在的 Worker 节点**（Object 的 primary copy 在那里）
- GPU Worker 节点通过 `ray.get()` 拉取的是 secondary copy，**不触发** `version_++`
- 每个 CPU Actor 返回一个结果 → 对应节点 `pinned_objects_size_` 先增后减 → 两次 `version_++`
- 1511 节点中运行 CPU Actor 的节点持续产生资源广播

#### 9.9.3 CPU block/unblock 机制（Actor Worker 不触发）

**`ray.get()` 的 block/unblock 判断逻辑（`core_worker/context.cc:385-393`）：**

```cpp
bool WorkerContext::ShouldReleaseResourcesOnBlockingCalls() const {
  // Actor Worker: CurrentActorIsDirectCall() = true → 返回 false
  // 普通 Task Worker: 返回 true
  return worker_type_ != WorkerType::DRIVER &&
         !CurrentActorIsDirectCall() &&      // ← Actor Worker 被豁免
         CurrentThreadIsMain();
}
```

**只有普通 Task Worker 调用 `ray.get()` 才会触发 CPU block/unblock**。判断依据是**调用者的身份**（Actor vs Task），与 ObjectRef 的来源（Actor 方法 vs 普通 task）无关。

| 调用者类型 | `ray.get()` 触发 block/unblock? | 原因 |
|-----------|------|------|
| **Actor Worker**（本场景） | **不触发** | `CurrentActorIsDirectCall()` = true，Actor 使用 lifetime 资源 |
| 普通 Task Worker | 触发 | 每次 `ray.get()` 产生 2 次 `version_++` |
| Driver | 不触发 | `worker_type_ == DRIVER`，Driver 不持有资源 |

**本集群场景分析：**

- `DistributedQwenVLVideoProcessMapper` 作为 Ray Data 的 `map_batches()` Actor Worker 运行
- Actor Worker 内部调用 `ray.get(future)` 获取 CPU Actor 的预处理结果
- 由于 `ShouldReleaseResourcesOnBlockingCalls()` 返回 false，**不触发** `ReleaseCpuResourcesFromBlockedWorker` / `ReturnCpuResourcesToUnblockedWorker`
- 因此 `ray.get()` **不会**通过 CPU block/unblock 路径导致 `version_++`

#### 9.9.4 完整根因链条

```
1511 节点常驻 Actor 集群 (LLM Serving)
    ↓
GPU Worker 节点（Mapper Actor）调用 CPU Actor 进行视频预处理
    ↓
CPU Actor 返回结果 → 返回值作为 primary object 存入 CPU Actor 所在节点的 Plasma
    ↓
pinned_objects_size_ += size → version_++ (pin 时)
    ↓
GPU Worker ray.get(future) 拉取结果（secondary copy），不触发 version_++
    ↓
GPU Worker 处理完毕，Python 引用释放 → Owner 通知 CPU Actor 节点释放
    ↓
pinned_objects_size_ -= size → version_++ (unpin 时)
    ↓
每个 CPU Actor 返回值生命周期产生 2 次 version_++（仅在 CPU Actor 所在节点）
    ↓
1511 节点中持续运行 CPU Actor 的节点，每 5 秒检测几乎都发现 version 递增
    ↓ (每节点约 15 条/180s，总计 ~126 条/s)
GCS ray_syncer_io_context 线程收到 ~239 条/s 入站消息
    ↓ (每条消息遍历 1511 个 reactor PushToSendingQueue)
总计 361,129 次/s PushToSendingQueue + 对应 gRPC 异步 IO 回调
    ↓
单线程 CPU 99.9% 打满
```

**根因总结：**

1. **`version_++` 的唯一持续触发源**：CPU Actor 返回值的 primary object pin/unpin 周期（Object Store 内存波动）
2. **CPU block/unblock 不触发**：Actor Worker 的 `ray.get()` 被 `ShouldReleaseResourcesOnBlockingCalls()` 豁免
3. **Actor busy/idle 不触发**：`MarkFootprintAsBusy` 有 guard 防重复，Actor 后续 task 不经过 Raylet
4. **瓶颈本质**：`batch_size=500, batch_delay=500ms` 对 1511 节点集群仍然不够激进，需要加大 batch 合并力度并降低源头消息率

### 9.10 调参约束（常驻 Actor 场景适用）

```
约束1: ray_syncer_message_refresh_interval_ms >> raylet_report_resources_period_milliseconds
       60000 >> 10000 ✓

约束2: gcs_resource_broadcast_max_batch_delay_ms < raylet_report_resources_period_milliseconds
       1500 < 10000 ✓

约束3: gcs_resource_broadcast_max_batch_size > 1 时 batch_delay 才生效
       1000 > 1 ✓

约束4: 资源视图最大延迟 ≈ report_period + batch_delay + network_latency
       ≈ 10000 + 1500 + ~100 ≈ 11.6s
       占分钟级 task 时长 < 20% ✓
```

---

## 十、指标采集与分析方法论

### 10.1 数据来源

Ray 的运行时指标主要来自 3 个数据源，都在 `/tmp/ray/session_latest/logs/` 下：

| 文件 | 进程 | 内容 |
|------|------|------|
| `raylet.out` | Raylet（每个节点一个） | Raylet 主线程的事件统计 |
| `gcs_server.out` | GCS Server（仅 Head 节点） | GCS 线程的事件统计 |
| `/proc/1/cmdline` | Ray 启动进程 | 启动参数（含 system-config） |

### 10.2 state-dump 机制

Ray 内部使用 `event_stats` 机制，每隔 `event_stats_print_interval_ms`（推荐 180000ms = 3 分钟）将各事件的**累计统计**写入日志。格式：

```
[state-dump]  事件名 - N total (M active), Execution time: mean = Xms, total = Yms, Queueing time: mean = Ams, max = Bms, min = Cms, total = Dms
```

| 字段 | 含义 |
|------|------|
| `N total` | 该事件从进程启动到现在的**累计**触发次数 |
| `M active` | 当前正在执行的实例数 |
| `Execution time mean` | 每次执行的平均耗时 |
| `Execution time total` | 累计执行总耗时（用于 CPU 时间预算分析） |
| `Queueing time mean` | 事件在队列中等待被执行的平均时间（反映堆积程度） |
| `Queueing time max` | 最大排队等待时间（反映线程繁忙峰值） |

**关键原理：** 通过计算相邻两次 state-dump 的 `total` 字段差值（增量），可以得到每个 `event_stats_print_interval_ms` 周期内的实际触发次数和 CPU 消耗。

### 10.3 逐项采集与分析方法

#### 10.3.1 syncer 线程 CPU

**采集命令：**
```bash
top -H -b -n1 | grep ray_syncer_io_c
```

**输出示例：**
```
99 root  20 0  22.8g 6.5g 18744 R 84.2 1.3 14:51.57 ray_syncer_io_c
```

**分析：**
- 第 9 列是 CPU%，`ray_syncer_io_c` 是 GCS 的 syncer IO **单线程**
- 状态列 `R`=运行中（繁忙），`S`=睡眠（空闲）
- 100% 意味着单线程打满，但不代表系统整体 CPU 瓶颈（128 核节点只占 ~0.8%）
- 配合 `top -H -b -n1 | head -7` 查看系统整体 CPU idle%

#### 10.3.2 配置参数确认

**采集命令：**
```bash
# 从 Head 节点进程 cmdline 提取
cat /proc/1/cmdline | tr '\0' '\n' | grep -E 'batch_size|batch_delay|report_resources|refresh_interval'
```

**原理：** Head 节点的 `ray start --head --system-config='{...}'` 命令行参数保存在 `/proc/1/cmdline` 中（PID 1 是容器内主进程）。`\0` 是参数分隔符，用 `tr` 转为换行后 grep 各配置项。

**注意：** Worker 节点不需要通过 cmdline 确认配置。Worker 的 Raylet 启动时通过 `GetInternalConfig` RPC 从 GCS 获取配置，用 state-dump 增量反推即可验证。

#### 10.3.3 OnDemandBroadcasting 增量

**采集命令：**
```bash
grep 'state-dump.*OnDemandBroadcasting' /tmp/ray/session_latest/logs/raylet.out | \
  awk -F'- ' '{print $2}' | awk -F' total' '{print $1}' | \
  awk 'NR>1{diff=$1-prev; print diff} {prev=$1}' | tail -10
```

**命令拆解：**
```
grep 'state-dump.*OnDemandBroadcasting'   → 提取含事件名的行
awk -F'- ' '{print $2}'                   → 以 "- " 分割，取第2段（"N total..."）
awk -F' total' '{print $1}'               → 以 " total" 分割，取累计数字 N
awk 'NR>1{diff=$1-prev; print diff}'      → 相邻两行做差，得到每个周期的增量
tail -10                                   → 取最近 10 个周期
```

**验证公式：**
```
预期增量 = event_stats_print_interval_ms / raylet_report_resources_period_milliseconds
```

| 配置值 | 预期增量 | 含义 |
|--------|---------|------|
| 100ms（默认） | 180000/100 = 1800 | 配置未生效 |
| 5000ms | 180000/5000 = 36 | — |
| 10000ms | 180000/10000 = 18 | — |
| 20000ms | 180000/20000 = 9 | — |

实测值等于预期值 → 配置生效。

#### 10.3.4 ResetRemoteNodeView 增量

**采集命令：**
```bash
grep 'state-dump.*ResetRemoteNodeView' /tmp/ray/session_latest/logs/raylet.out | \
  awk -F'- ' '{print $2}' | awk -F' total' '{print $1}' | \
  awk 'NR>1{diff=$1-prev; print diff} {prev=$1}' | tail -10
```

**验证公式：**
```
预期增量 = event_stats_print_interval_ms / ray_syncer_message_refresh_interval_ms
例: 180000 / 60000 = 3
```

#### 10.3.5 BroadcastMessage 增量

**Raylet 端（raylet.out）：**
```bash
grep 'state-dump.*BroadcastMessage' /tmp/ray/session_latest/logs/raylet.out | \
  awk -F'- ' '{print $2}' | awk -F' total' '{print $1}' | \
  awk 'NR>1{diff=$1-prev; print diff} {prev=$1}' | tail -10
```

**GCS 端（gcs_server.out）：**
```bash
grep 'BroadcastMessage' /tmp/ray/session_latest/logs/gcs_server.out | \
  awk '{print $3}' | awk 'NR>1{print $1-prev} {prev=$1}' | tail -10
```

注意 GCS 日志格式略有不同（无 `[state-dump]` 前缀），用 `awk '{print $3}'` 直接取第 3 列即 total 数字。

**含义：**

| 来源 | 含义 |
|------|------|
| Raylet BroadcastMessage | 该节点**接收到**的全集群广播流量（来自 GCS 转发） |
| GCS BroadcastMessage | GCS **处理**的入站消息数（来自所有 Raylet） |

Hub-and-Spoke 模型下两者应基本一致。差异说明有消息丢失或积压。

#### 10.3.6 BroadcastMessage 详细统计

**采集命令：**
```bash
grep 'BroadcastMessage' /tmp/ray/session_latest/logs/gcs_server.out | tail -3
```

**输出示例：**
```
RaySyncer.BroadcastMessage - 141869 total (0 active),
  Execution time: mean = 0.95ms, total = 134118.12ms,
  Queueing time: mean = 0.00ms, max = 0.09ms
```

**CPU 时间预算分析方法：**

```
① BroadcastMessage 执行耗时（单个 180s 周期内）
   = 两次 state-dump 间 exec time total 的差值
   例: 134118 - 113272 = 20,845ms

② BroadcastMessage 占 CPU%
   = 20,845ms / 180,000ms = 11.6%

③ 总 CPU（top 实测）= 84.2%

④ gRPC IO 回调占 CPU%（倒推）
   = 84.2% - 11.6% = 72.6%

⑤ 空闲
   = 100% - 84.2% = 15.8%
```

**完整 CPU 时间预算表：**
```
总工作时间:         151,560ms (84.2%)
├─ BroadcastMessage: 20,845ms (11.6%)  ← 有统计，含 ConsumeSyncMessage + PushToSendingQueue×N
└─ gRPC IO 回调:    130,715ms (72.6%)  ← 无直接统计，倒推
    ├─ OnReadDone (收消息)
    ├─ OnWriteDone (发消息)
    ├─ StartSend (stream 写入)
    └─ 连接/timer 管理

空闲:               28,440ms (15.8%)
```

### 10.4 堆积检测方法

#### 方法1：Queueing time 判断

```
Queueing time mean ≈ 0ms  → 无堆积（消息即到即处理）
Queueing time mean >> 0   → 有堆积（线程忙不过来）
Queueing time max 持续增大 → 堆积在恶化
```

#### 方法2：GCS 处理量 vs Raylet 接收量对比

```
GCS  处理: 23,765 条/180s
Head 接收: 23,679 条/180s
Worker接收: 23,408 条/180s
三者一致 → 无消息丢失、无积压
```

#### 方法3：gRPC 消息超限检查

```bash
grep -c 'message too large' /tmp/ray/session_latest/logs/gcs_server.out
# 预期: 0（batch_size=2000 时单次 RPC 约 10MB，远低于 100MB 限制）
```

### 10.5 gRPC 写入开销估算

**推导公式：**
```
消息到达率 = GCS BroadcastMessage 增量 / 180s
每个 reactor 每 batch 攒消息数 = 到达率 × batch_delay_s
gRPC 写入频率 = reactor数量 / batch_delay_s
每次写入耗时 = gRPC IO 总 CPU 时间 / (写入频率 × 180s)
```

**实例：**
```
消息到达率: 132/s
batch_delay: 1.5s
每 reactor 攒: 132 × 1.5 = 198 条 (< batch_size 1000)
gRPC 写入频率: 1511 / 1.5 = 1007 次/s
per-write 耗时: 130,715ms / (1007 × 180) = 0.72ms/次
```

### 10.6 吞吐极限估算（CPU 预算法）

**基于单线程 CPU 预算推算最大可承受消息率：**

```
设当前消息率为 R，CPU 占比为 C，线性假设下：
最大消息率 R_max = R × (100% / C)

例: R=132/s, C=84.2%
R_max = 132 × (100/84.2) = 157/s
```

**双参数组估算法（更准确，需要两组数据点）：**

```
假设 CPU = F（固定开销）+ R × V（可变开销/消息）

数据点1: R1=239/s → C1=99.9%  (batch_delay=500ms)
数据点2: R2=131/s → C2=84.2%  (batch_delay=1500ms)

注意：不同 batch 参数下 per-message 成本不同，不能直接用两组数据联立。
需要在同一组 batch 参数下采集不同负载的数据才能准确建模。
```

### 10.7 调度性能分析

#### RequestWorkerLease 统计

**采集命令：**
```bash
# 详细统计
grep -E 'RequestWorkerLease ' /tmp/ray/session_latest/logs/gcs_server.out | tail -3

# 增量
grep 'RequestWorkerLease ' /tmp/ray/session_latest/logs/gcs_server.out | \
  awk -F'- ' '{print $2}' | awk -F' total' '{print $1}' | \
  awk 'NR>1{diff=$1-prev; print diff} {prev=$1}' | tail -10
```

**关键字段：**

| 字段 | 含义 | 说明 |
|------|------|------|
| `exec mean` | 每次 lease RPC 的平均端到端耗时 | 包含网络 RTT + 目标节点处理时间 + Worker 冷启动 |
| `OnReplyReceived queueing mean` | 回复到达后在 GCS 主线程排队时间 | 偏高说明 GCS 主线程繁忙 |
| `total count` 增量 | 每 180s 新增的 lease 请求数 | 增量=0 说明无新调度 |

#### 调度失败分析

**采集命令：**
```bash
# 失败总数
grep -c 'Failed to lease worker' /tmp/ray/session_latest/logs/gcs_server.out

# 时间范围
grep 'Failed to lease worker' /tmp/ray/session_latest/logs/gcs_server.out | head -3  # 首次
grep 'Failed to lease worker' /tmp/ray/session_latest/logs/gcs_server.out | tail -3  # 末次
```

**分析公式：**
```
失败率 = Failed count / RequestWorkerLease total
每 Actor 平均尝试次数 = RequestWorkerLease total / Registered actors count
调度爆发时间窗口 = 末次失败时间 - 首次失败时间
```

#### 其他调度指标

```bash
# Pending placement group（应为 0）
grep 'Scheduling pending placement group count' /tmp/ray/session_latest/logs/gcs_server.out | tail -3

# Infeasible placement group（应为 0）
grep 'Infeasible placement groups count' /tmp/ray/session_latest/logs/gcs_server.out | tail -3

# 注册 Actor 数
grep 'Registered actors count' /tmp/ray/session_latest/logs/gcs_server.out | tail -3

# "resources are not enough" 总数
grep -c 'resources are not enough' /tmp/ray/session_latest/logs/gcs_server.out
```

### 10.8 Head vs Worker 对比分析

**目的：** 确认资源变化的真实来源。Head 节点主要运行 GCS，不跑用户 Actor，资源变化来自 Worker。

**对比 OnDemandBroadcasting exec mean：**

| 节点 | exec mean | 说明 |
|------|-----------|------|
| Head ≈ 0.01ms | `CreateSyncMessage` 返回 nullopt | Head 无资源变化 |
| Worker > 0.01ms | 含实际广播耗时 | Worker 有资源变化 |

**推算实际广播比例：**
```
设广播概率为 p：
p × (广播耗时 ~0.4ms) + (1-p) × (空跑耗时 ~0.01ms) = Worker 实测 exec mean
解出 p = 实际广播占比
每 Worker 实际广播数/180s = p × OnDemandBroadcasting 增量
```

### 10.9 排查流程总览

```
第1步: top -H 确认 syncer CPU              → 定位是否有问题
第2步: cmdline 确认参数                     → 确认配置是否生效
第3步: OnDemandBroadcasting 增量验证        → 用公式交叉验证配置
第4步: BroadcastMessage 增量趋势            → 看广播量变化方向
第5步: GCS exec mean + total 做 CPU 时间预算 → 量化 CPU 花在哪
第6步: Queueing time 检测堆积               → 判断是否处理不过来
第7步: Head vs Worker 对比                  → 定位资源变化源头
第8步: RequestWorkerLease 分析调度延迟       → 判断调度是否受影响
```

### 10.10 batch 参数对 CPU 影响的实测分析

#### 问题

直觉上，增大 `batch_delay` 减少 gRPC 写入次数，应该降低 gRPC IO CPU。但实测发现效果极为有限。

#### syncer 线程 CPU 构成

`ray_syncer_io_c` 是单线程事件循环，所有工作串行执行。CPU 由两部分构成：

```
总 CPU = BroadcastMessage CPU + gRPC IO 回调 CPU

BroadcastMessage (event_stats 可统计):
  工作: ConsumeSyncMessage + PushToSendingQueue × N_reactors
  本质: 纯内存操作（版本比较 + 队列 push）
  耗时: msg_rate × exec_mean

gRPC IO 回调 (event_stats 统计不到，倒推):
  工作: protobuf 序列化 + gRPC StreamWrite + OnWriteDone/OnReadDone 回调
  本质: 序列化 + 系统调用 + 网络 IO
  耗时: 总 CPU - BroadcastMessage CPU
```

#### BroadcastMessage 与 batch 参数的关系

**BroadcastMessage CPU 与 batch 参数完全无关。**

BroadcastMessage 在 `OnReadDone` 回调中被调用，每收到一条入站消息调用一次。batch 参数控制的是 GCS 的**出站**行为（何时发送给 reactor），不影响入站处理。

```
入站: Raylet → gRPC stream → OnReadDone → BroadcastMessage
  → 次数 = 消息到达率，与 batch 无关

出站: PushToSendingQueue → 队列 → batch 定时器/满触发 → StreamWrite
  → gRPC 写入次数受 batch_delay 控制
```

#### gRPC IO 与 batch 参数的关系

增大 `batch_delay` 改变 gRPC 写入的**粒度**，但不改变**总数据量**：

```
总发送数据量/s = 消息到达率 × 每条消息大小 × N_reactors
             → 与 batch 参数无关（不管怎么打包，总量不变）
```

gRPC StreamWrite 的主要耗时是 **protobuf 序列化 + 内核 send buffer 拷贝**，与数据量正比。固定开销（syscall 进出、回调注册）占比不大。因此：

```
gRPC IO 总耗时 ≈ 写入次数 × per-write 耗时
              = (N_reactors / batch_delay) × (固定开销 + batch_msgs × 单条序列化耗时)
              ≈ N_reactors × 消息率 × 单条序列化耗时 + N_reactors × 固定开销 / batch_delay

第一项（序列化）: 与 batch 无关，是常数
第二项（固定开销摊销）: batch_delay 越大越小，但占比本来就小
```

#### 实测数据验证

| 参数组 | batch_delay | 消息率 | 写入频率 | per-batch 条数 | per-write 耗时 | gRPC IO 总耗时/s |
|--------|-----------|--------|---------|---------------|---------------|------------------|
| 基线 | 500ms | 239/s | 3,022/s | 120 条 | 0.26ms | 786ms (77.2%) |
| 推荐 | 1500ms | 131/s | 1,007/s | 198 条 | 0.72ms | 725ms (72.6%) |

**写入次数减少 3x（3022→1007），per-write 增大 2.8x（0.26→0.72ms），总 gRPC IO 仅降 7.8%。**

#### CPU 降幅的真实贡献分解

```
总 CPU 降幅: 99.9% → 84.2% = 15.7 个百分点

├─ BroadcastMessage CPU 降幅: 22.7% → 11.6% = 11.1pp (占 71%)
│   原因: 消息率 239→131/s (report_period 5000→10000)
│   → 100% 来自 report_period，与 batch 无关
│
└─ gRPC IO CPU 降幅: 77.2% → 72.6% = 4.6pp (占 29%)
    原因: 消息率下降（主） + batch 增大（微小）
    其中 batch 贡献 ≈ gRPC IO 总降幅的 7.8% × 29% ≈ ~2% 的 CPU 降幅
```

#### batch 参数的真正适用场景

batch 的去重合并功能只在**同一节点在 batch_delay 窗口内发送多条更新**时生效：

| 条件 | 是否有去重 | 说明 |
|------|-----------|------|
| `report_period` >> `batch_delay` | ❌ 无 | 同一节点在窗口内最多 1 条，无法去重 |
| `report_period` << `batch_delay` | ✅ 有 | 同一节点多次更新可被合并 |
| 消息洪峰（如 PG 创建） | ✅ 有 | 短时间大量消息，batch 真正减少转发量 |

当前场景 `report_period=10000ms >> batch_delay=1500ms`，无去重机会，batch 退化为纯"打包"。

#### 结论

```
                        report_period          batch 参数
                     (消息源头速率控制)    (batch_size / batch_delay)
──────────────────────────────────────────────────────────────────
BroadcastMessage CPU    ✅ 线性下降           ❌ 无影响
gRPC IO CPU             ✅ 线性下降           ❌ < 5% 影响
syncer 总 CPU           ✅ 线性下降           ❌ < 5% 影响
调度延迟                ⚠️ 增加              ⚠️ 增加（batch_delay）
──────────────────────────────────────────────────────────────────
核心杠杆                  ✅ 是                  ❌ 不是
```

---

## 十一、实战调参效果验证（2026-05-11）

> 环境信息：
> - 集群：kml-hb2az1-l3-2，namespace=lmserving
> - 节点数：1511
> - 业务类型：常驻 Actor 执行分钟级 task（LLM Serving）
> - 排查时间：2026-05-11

### 11.1 三轮调参对比

#### 第一轮：初始配置（基线）

```json
{
    "raylet_report_resources_period_milliseconds": 5000,
    "ray_syncer_message_refresh_interval_ms": 30000,
    "gcs_resource_broadcast_max_batch_size": 500,
    "gcs_resource_broadcast_max_batch_delay_ms": 500
}
```

| 指标 | 值 |
|------|-----|
| syncer CPU | **99.9%** |
| BroadcastMessage/180s | ~43,000 |
| GCS exec mean | 0.95ms |
| PushToSendingQueue/s（推算） | ~361,129 |
| 资源视图最大延迟 | ~5.5s |

#### 第二轮：推荐配置

```json
{
    "raylet_report_resources_period_milliseconds": 10000,
    "ray_syncer_message_refresh_interval_ms": 60000,
    "gcs_resource_broadcast_max_batch_size": 1000,
    "gcs_resource_broadcast_max_batch_delay_ms": 1500
}
```

| 指标 | 值 | vs 基线 |
|------|-----|---------|
| syncer CPU | **84.2%** | ↓15.7pp |
| BroadcastMessage/180s | ~23,000 | ↓46% |
| GCS exec mean | 0.95ms | 不变 |
| PushToSendingQueue/s（推算） | ~197,941 | ↓45% |
| gRPC 写入频率 | ~1,007/s | — |
| per-write 耗时 | ~0.72ms | — |
| Queueing time mean | 0.00ms | 无堆积 |
| 资源视图最大延迟 | ~11.5s | +6s |

**CPU 时间预算：**

```
总 CPU:               84.2%
├─ BroadcastMessage:  11.6% (20,845ms/180s)
└─ gRPC IO 回调:      72.6% (130,715ms/180s)
空闲:                 15.8%
```

**结论：** 有效降低广播量 46%，但 syncer 线程仍在 84% 负荷。BroadcastMessage 增量从初期的 ~3,500 跳升到 ~23,000，说明业务负载恢复后广播量回升。

#### 第三轮：激进配置

```json
{
    "raylet_report_resources_period_milliseconds": 10000,
    "ray_syncer_message_refresh_interval_ms": 60000,
    "gcs_resource_broadcast_max_batch_size": 2000,
    "gcs_resource_broadcast_max_batch_delay_ms": 3000
}
```

| 指标 | 值 | vs 基线 | vs 第二轮 |
|------|-----|---------|-----------|
| syncer CPU | **0.0% (S)** | ↓99.9pp | ↓84.2pp |
| BroadcastMessage/180s | **0** | ↓100% | ↓100% |
| GCS exec mean | 1.00ms | — | — |
| GCS BroadcastMessage total | 72,527（不再增长） | — | — |
| Queueing time mean | 0.00ms | 无堆积 | — |
| message too large | 0 | — | — |
| 资源视图最大延迟 | ~13s | +7.5s | +1.5s |

**BroadcastMessage 增量趋势：**
```
406 → 3 → 0 → 0 → 0   (最近 9 分钟无任何广播)
```

**结论：** syncer 线程完全空闲。集群进入稳态后无资源变化，广播量为 0。

### 11.2 调度性能影响

激进配置下观察到 Actor 调度较慢。

**调度时间线：**

| 时间 | 事件 |
|------|------|
| 14:37:41 | 首次 lease 失败 |
| 14:41:14 | 末次 lease 失败 |
| 15:03:37 | 当前时间（调度已全部完成） |

**调度指标：**

| 指标 | 值 |
|------|-----|
| RequestWorkerLease 总数 | 41,340 |
| 失败次数 | 5,668（13.7%） |
| 注册 Actor 数 | 15,003 |
| 每 Actor 平均尝试次数 | 2.76 |
| RequestWorkerLease exec mean | 571ms |
| OnReplyReceived queueing mean | 71ms（max 848ms） |
| 调度持续时间 | ~3.5 分钟 |

**RequestWorkerLease 增量（每 180s）：**
```
0, 19831, 19503, 0, 0, 0, 0, 0, 0, 0
```
调度集中在 2 个 state-dump 周期（~6 分钟）内完成，之后无新请求。

**Pending / Infeasible placement group：** 均为 0。

### 11.3 调度慢的根因分析

| 因素 | 影响程度 | 说明 |
|------|---------|------|
| **一次性爆发调度 15,003 Actor** | **主因** | 集群重启后所有 Actor 同时创建 |
| **Worker 冷启动耗时** | **次因** | RequestWorkerLease exec mean = 571ms |
| **batch_delay=3000ms** | **加剧因素** | 资源视图延迟 13s，增加调度"撞车"概率 |
| **资源竞争** | **环境因素** | 5,668 次 "resources are not enough" |

**参数对调度延迟的影响量化：**

| 参数组 | 资源视图最大延迟 | 说明 |
|--------|----------------|------|
| 基线 (5000/500) | 5.5s | — |
| 推荐 (10000/1500) | 11.5s | +6s |
| 激进 (10000/3000) | 13s | +7.5s |

视图延迟从 5.5s 增大到 13s（2.4x），导致调度爆发期 GCS 更容易选中已无资源的节点，增加 spillback 重试。但失败率 13.7% 仍在可控范围。

**关键结论：** 调度慢主要发生在集群启动阶段（14:37-14:41 的 ~3.5 分钟），属于一次性成本。稳态下 syncer CPU=0%，调度已无压力。

### 11.4 三轮调参总览

```
                        syncer CPU    BroadcastMsg/180s   视图延迟    调度影响
基线 (500/500)          99.9%         ~43,000             5.5s       基准
推荐 (1000/1500)        84.2%         ~23,000             11.5s      轻微
激进 (2000/3000)         0.0%              0              13.0s      启动期+3.5min
```

> **关键发现：** 基线→推荐的 15.7pp CPU 降幅中，71% 来自 `report_period` 翻倍（BroadcastMessage CPU 从 22.7%→11.6%），
> 仅 ~2pp 来自 batch 参数变化。激进方案 CPU=0% 是因为集群稳态下无资源变化（BroadcastMessage=0），
> 与 batch 参数无关。
>
> **调参核心杠杆是 `report_period`，不是 batch 参数。**

### 11.5 参数选择建议

基于实测数据的修正建议：

| 场景 | 推荐配置 | 理由 |
|------|---------|------|
| **频繁调度**（短 task、大量 Actor 创建销毁） | report_period=10000, batch 保持默认 | 调度响应优先，batch 对 CPU 无显著帮助 |
| **常驻 Actor + 分钟级 task**（LLM Serving 等） | report_period=20000, batch 保持默认 | 消息源头减半是真正有效的降 CPU 手段 |
| **syncer CPU 仍然偏高** | report_period=30000 | 继续增大 report_period，效果线性 |

推荐配置（常驻 Actor 场景）：
```json
{
    "raylet_report_resources_period_milliseconds": 20000,
    "ray_syncer_message_refresh_interval_ms": 60000,
    "gcs_resource_broadcast_max_batch_size": 512,
    "gcs_resource_broadcast_max_batch_delay_ms": 500,
    "health_check_period_ms": 10000,
    "gcs_server_rpc_server_thread_num": 64,
    "scheduler_avoid_gpu_nodes": false,
    "event_stats_print_interval_ms": 180000
}
```

预期效果：`report_period=20000` 使消息源头从基线的 ~126/s 降到 ~32/s，syncer CPU 预期降到 ~42%（基于 CPU 与消息率的线性关系）。batch 参数保持较小值，避免增加资源视图延迟和调度爆发期的性能恶化。资源视图最大延迟 ≈ 20s + 0.5s = 20.5s，对常驻 Actor 的分钟级 task 完全可接受。
