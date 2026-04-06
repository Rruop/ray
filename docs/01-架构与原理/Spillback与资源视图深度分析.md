# Ray 调度、Spillback 与资源视图同步机制深度分析

> 相关文档：
> - [RaySyncer 资源同步机制与配置参数深度分析](./ray-syncer-resource-sync-mechanism.md)
> - [GCS Server 线程瓶颈分析与定位指南](./gcs-thread-analyze.md)
> - [Ray GCS 问题排查指南](./ray-gcs-troubleshooting-guide.md)

---

## 目录

- [一、系统配置参数影响分析](#一系统配置参数影响分析)
  - [1.1 `ray_syncer_message_refresh_interval_ms`](#11-ray_syncer_message_refresh_interval_ms-30000-默认-3000)
  - [1.2 `gcs_resource_broadcast_max_batch_size`](#12-gcs_resource_broadcast_max_batch_size-100-默认-1)
  - [1.3 `gcs_resource_broadcast_max_batch_delay_ms`](#13-gcs_resource_broadcast_max_batch_delay_ms-500-默认-0)
  - [1.4 `health_check_period_ms`](#14-health_check_period_ms-10000-默认-3000)
  - [1.5 `gcs_server_rpc_server_thread_num`](#15-gcs_server_rpc_server_thread_num-64-默认-cpu4)
  - [1.6 `scheduler_avoid_gpu_nodes`](#16-scheduler_avoid_gpu_nodes-false-默认-true)
  - [1.7 `event_stats_print_interval_ms`](#17-event_stats_print_interval_ms-180000-默认-60000)
  - [1.8 参数综合影响总览](#18-参数综合影响总览)
- [二、RaySyncer 资源同步机制](#二raysyncer-资源同步机制)
  - [2.1 星型拓扑：所有同步经过 GCS](#21-星型拓扑所有同步经过-gcs)
  - [2.2 版本去重协议及其缺陷](#22-版本去重协议及其缺陷)
  - [2.3 资源视图的"脏"与"干净"](#23-资源视图的脏与干净)
  - [2.4 定时刷新机制：syncer 的兜底](#24-定时刷新机制syncer-的兜底)
  - [2.5 `ray_syncer_message_refresh_interval_ms` 的线程模型](#25-ray_syncer_message_refresh_interval_ms-的线程模型)
  - [2.6 GCS 在 Syncer 中的角色](#26-gcs-在-syncer-中的角色)
  - [2.7 On-Demand（按需）资源上报机制](#27-on-demand按需资源上报机制)
  - [2.8 `RaySyncer.BroadcastMessage` vs `RaySyncer.OnDemandBroadcasting` 指标](#28-raysyncerbroadcastmessage-vs-raysyncerondemandbroadcasting-指标)
- [三、调度决策流程](#三调度决策流程)
  - [3.1 Task 调度的完整链路](#31-task-调度的完整链路)
  - [3.2 ClusterLeaseManager：集群级调度](#32-clusterlasemanager集群级调度)
  - [3.3 LocalLeaseManager：本地调度与二次 Spillback](#33-localleasemanager本地调度与二次-spillback)
- [四、Spillback 机制详解](#四spillback-机制详解)
  - [4.1 Spillback 不是转发，是重定向](#41-spillback-不是转发是重定向)
  - [4.2 Client 与 Raylet 的关系](#42-client-与-raylet-的关系)
  - [4.3 `grant_or_reject` 协议：最多一跳](#43-grant_or_reject-协议最多一跳)
  - [4.4 Reject 后的重试机制](#44-reject-后的重试机制)
  - [4.5 推测性资源扣减](#45-推测性资源扣减)
  - [4.6 扣减失败仍继续 Spillback 的原因](#46-扣减失败仍继续-spillback-的原因)
- [五、Hybrid 调度策略打分逻辑](#五hybrid-调度策略打分逻辑)
  - [5.1 打分使用的资源视图](#51-打分使用的资源视图)
  - [5.2 分数计算：Critical Resource Utilization](#52-分数计算critical-resource-utilization)
  - [5.3 Spread Threshold 截断](#53-spread-threshold-截断)
  - [5.4 Feasible vs Available](#54-feasible-vs-available)
  - [5.5 Top-K 随机选择](#55-top-k-随机选择)
  - [5.6 本地节点的特殊待遇](#56-本地节点的特殊待遇)
  - [5.7 GPU 节点避让](#57-gpu-节点避让)
  - [5.8 完整打分流程图](#58-完整打分流程图)
- [六、资源视图过时的安全性分析](#六资源视图过时的安全性分析)
- [七、系统配置参数调优建议](#七系统配置参数调优建议)
  - [7.1 参数关注点](#71-参数关注点)
  - [7.2 `ray_syncer_message_refresh_interval_ms=30000` 的实际影响](#72-ray_syncer_message_refresh_interval_ms30000-的实际影响)
  - [7.3 参数间的约束关系](#73-参数间的约束关系)
  - [7.4 按集群规模推荐配置](#74-按集群规模推荐配置)
  - [7.5 当前配置组合评估](#75-当前配置组合评估)

---

## 一、系统配置参数影响分析

以下分析基于一组常见的大规模集群调优参数：

| 参数 | 设置值 | 默认值 | 定义位置 |
|------|--------|--------|----------|
| `ray_syncer_message_refresh_interval_ms` | 30000 | 3000 | `ray_config_def.h:442` |
| `gcs_resource_broadcast_max_batch_size` | 100 | 1 | `ray_config_def.h:1044-1048` |
| `gcs_resource_broadcast_max_batch_delay_ms` | 500 | 0 | `ray_config_def.h:1054` |
| `health_check_period_ms` | 10000 | 3000 | `ray_config_def.h:900` |
| `gcs_server_rpc_server_thread_num` | 64 | max(1, CPU/4) | `ray_config_def.h:356-359` |
| `scheduler_avoid_gpu_nodes` | false | true | `ray_config_def.h:827` |
| `event_stats_print_interval_ms` | 180000 | 60000 | `ray_config_def.h:48` |

### 1.1 `ray_syncer_message_refresh_interval_ms`: 30000 (默认 3000)

**作用：** Raylet 端定期刷新对远端节点资源视图的间隔。这是 syncer 版本去重协议缺陷的兜底机制（详见[第二章](#二raysyncer-资源同步机制)）。由于 RaySyncer 协议的版本去重机制，当远端节点的资源状态没有变化时（version 不递增），即使本地的推测性扣减已经让资源视图失真，syncer 也无法推送纠正消息。这个定时器就是用来定期将"脏"视图重置回最后一次收到的干净快照。

**影响：**
- 将定时器从 3s 增加到 30s
- 如果远端节点资源没有变化（version 不递增），本地推测性扣减造成的脏视图最长需要 30s 才能重置
- 如果远端节点资源有变化，syncer 正常推送会覆盖脏视图，定时器不需要介入
- **优点：** 减少 GCS/Raylet 间不必要的资源刷新开销
- **风险：** 如果某个节点的资源更新丢失，最长需要 30s 才能通过定期刷新恢复，期间调度可能基于过时的资源视图做决策

### 1.2 `gcs_resource_broadcast_max_batch_size`: 100 (默认 1)

**作用：** GCS RaySyncer 将最多 100 条资源消息打包后再通过 gRPC streaming write 发送。

- **优点：** 大幅减少 gRPC streaming write 次数，降低网络开销和 `ray_syncer_io_context` 线程的 CPU 占用
- **风险：** 引入发送延迟（需配合 `max_batch_delay_ms` 使用）

**关于 gRPC 消息大小限制（是否需要调整 RPC size）：** `max_grpc_message_size` 默认 512 MB（`ray_config_def.h:203`）。每条 `RaySyncMessage` 包含一个节点的资源视图（`resources_available`, `resources_total`, `labels`, `node_activity`），典型大小在几 KB 级别。batch_size=100 的最坏情况约几百 KB，远低于 512 MB 限制。**不需要调整 RPC size。**

发送缓冲区按 `(node_id, message_type)` 去重（`ray_syncer_bidi_reactor_base.h:287-289`）：

```cpp
absl::flat_hash_map<std::pair<std::string, MessageType>,
                    std::shared_ptr<const RaySyncMessage>>
    sending_buffer_;
```

每个节点最多 2 种 MessageType（`RESOURCE_VIEW` 和 `COMMANDS`），所以缓冲区大小自然受限于 `2 * 节点数`。当 `StartSend()` 被调用时（`ray_syncer_bidi_reactor_base.h:172`），**整个** `sending_buffer_` 会被一次性排空到一个 `RaySyncMessageBatch` protobuf 中发送——不只是 `max_batch_size_` 条。`max_batch_size_` 只控制**何时触发发送**（阈值），不控制单次发送的数量。如果前一次写入还在进行中（`sending_ = true`），新消息会继续累积，缓冲区可能超过 `max_batch_size_`，如代码注释所述（`ray_syncer_bidi_reactor_base.h:85`）：

```cpp
// sending_buffer_ size can be greater than max_batch_size_ as previous
// message batch might be sending in progress
```

即使 10000 节点集群的最坏情况：`2 * 10000 * ~5KB ≈ 100MB`，仍远低于 512MB 限制。

### 1.3 `gcs_resource_broadcast_max_batch_delay_ms`: 500 (默认 0)

**作用：** 与 batch_size=100 配合使用。在 `gcs_resource_broadcast_max_batch_size != 1` 时生效，控制批量发送的最大等待延迟。逻辑为：先到 100 条或先到 500ms，哪个先满足就发送。

- **优点：** 平衡了延迟和吞吐
- **风险：** 资源更新传播最多延迟 500ms，调度决策基于最多 500ms 前的资源视图

使用位置（`gcs_server.cc:586-602`）：

```cpp
// 配置冲突检测
if (RayConfig::instance().gcs_resource_broadcast_max_batch_delay_ms() > 0 &&
    RayConfig::instance().gcs_resource_broadcast_max_batch_size() == 1) {
  RAY_LOG(WARNING) << "Configuration inconsistency detected";
}
```

批处理触发逻辑（`ray_syncer_bidi_reactor_base.h:87-115`）：如果 `max_batch_delay_ms_ == 0`，无论缓冲区大小都立即发送；如果 `max_batch_delay_ms_ > 0`，第一条消息入队时启动定时器，在定时器到期或达到 batch_size 上限时触发发送。

### 1.4 `health_check_period_ms`: 10000 (默认 3000)

**作用：** GCS 健康检查管理器探测注册节点存活状态的间隔（`gcs_health_check_manager.h:62`）。结合 `health_check_failure_threshold`（默认 5），节点故障检测时间从 **15s（3s x 5） 变为 50s（10s x 5）**。

- **优点：** 减少健康检查 RPC 开销
- **风险：** 节点宕机后发现时间明显变长，影响故障恢复速度

相关配置（`ray_config_def.h:898-904`）：
```cpp
RAY_CONFIG(int64_t, health_check_initial_delay_ms, 5000)   // 首次检查延迟
RAY_CONFIG(int64_t, health_check_period_ms, 3000)          // 检查间隔
RAY_CONFIG(int64_t, health_check_timeout_ms, 10000)        // 单次超时
RAY_CONFIG(int64_t, health_check_failure_threshold, 5)     // 连续失败次数阈值
```

**故障检测总延迟 = `health_check_period_ms` x `health_check_failure_threshold`**，即连续 N 次探测失败后才判定节点死亡。

### 1.5 `gcs_server_rpc_server_thread_num`: 64 (默认 CPU/4)

**作用：** GCS gRPC 服务端 polling 线程数，负责从 socket 缓冲区读取入站请求并反序列化生成 proto 请求对象。同时影响 `gcs_max_active_rpcs_per_handler`（默认为该值 x 100 = 6400，`ray_config_def.h:789-791`），控制每个 RPC handler 的最大并发数。

- **优点：** 提高 GCS 并发处理能力，适合大规模集群
- **注意：** 如果 GCS 节点 CPU 核数不足 64，会导致线程过多引起上下文切换开销

使用位置：`gcs_server_main.cc:167` 作为 `gcs_server_config.grpc_server_thread_num`。

### 1.6 `scheduler_avoid_gpu_nodes`: false (默认 true)

**作用：** 关闭"CPU 任务避开 GPU 节点"策略，允许纯 CPU 任务调度到 GPU 节点。当默认值 `true` 时，调度器会优先选择非 GPU 节点来调度不需要 GPU 的任务，保留 GPU 节点给真正需要 GPU 的任务。

- **适用场景：** GPU 节点有大量空闲 CPU，希望充分利用

该参数在多个调度策略构造函数中被引用（`scheduling_options.h:59,70,106,119`）：Spread、Hybrid、AffinityWithBundle、NodeLabelScheduling 策略均使用此配置。

影响调度策略：`hybrid_scheduling_policy.cc:183-221` 中的 GPU 过滤逻辑——当 `avoid_gpu_nodes=true` 且 task 不需要 GPU 时，先只在非 GPU 节点中选，没有可用非 GPU 节点时才回退到所有节点（包括 GPU 节点）。

### 1.7 `event_stats_print_interval_ms`: 180000 (默认 60000)

**作用：** 控制多个组件的 debug 状态日志输出间隔，从 60s 增加到 180s。需要注意：此功能需要 `event_stats=1` 开启才有效。

- **优点：** 减少日志量
- **风险：** 排查问题时日志粒度变粗

使用位置：
- `node_manager.cc:434-445` — NodeManager 通过 `DebugString()` 打印状态和 OOM kill 统计
- `gcs_server.cc:311, 919-928` — GCS 通过 `PrintDebugState()` 打印状态，并有条件地打印主 io_context 和各专用 io_context 的事件循环统计
- `core_worker.cc:449-464` — CoreWorker 打印 `io_service_` 和 `task_execution_service_` 的事件统计
- `plasma/store.cc:114, 576` — PlasmaStore 通过 `PrintAndRecordDebugDump()` 打印调试信息

### 1.8 参数综合影响总览

```
┌───────────────────────────────────────────────────────────────────────────┐
│                        参数影响分布                                       │
├───────────────────────┬───────────────────────────────────────────────────┤
│     GCS 端            │     Raylet 端                                    │
│                       │                                                   │
│  batch_size=100 ──────┤                                                   │
│  batch_delay=500ms ───┤   ray_syncer_refresh=30000ms ──→ 脏视图重置间隔  │
│  rpc_threads=64 ──────┤   avoid_gpu_nodes=false ────────→ 调度策略        │
│                       │                                                   │
│  health_check=10000ms ┤ ← 双向影响（GCS 检测 Raylet 存活）               │
│                       │                                                   │
│  event_stats=180000ms ┤ ← 双向影响（两端都有状态打印）                    │
└───────────────────────┴───────────────────────────────────────────────────┘
```

---

## 二、RaySyncer 资源同步机制

### 2.1 星型拓扑：所有同步经过 GCS

RaySyncer 采用 Hub-and-Spoke（中心辐射）架构，**Raylet 之间不直连**：

```
Raylet A ←──bidi gRPC──→ GCS ←──bidi gRPC──→ Raylet B
                          ↑
Raylet C ←──bidi gRPC────┘
```

每个 Raylet 在启动时通过 `node_manager.cc:366-367` 连接到 GCS：

```cpp
auto gcs_channel = gcs_client_.GetGcsRpcClient().GetChannel();
ray_syncer_.Connect(kGCSNodeID.Binary(), gcs_channel);
```

**完整消息流（Raylet B → GCS → Raylet A）：**

```
[Raylet B]                        [GCS]                         [Raylet A]
    │                               │                               │
    │ LocalResourceManager          │                               │
    │ 资源变化, version++           │                               │
    │                               │                               │
    │── RaySyncMessage ────────────>│                               │
    │   (bidi gRPC stream)          │                               │
    │                               │ NodeState::ConsumeSyncMessage  │
    │                               │ ├─ 版本检查通过                │
    │                               │ ├─ 更新 cluster_view          │
    │                               │ ├─ 通知 GcsResourceManager    │
    │                               │ └─ BroadcastMessage 给所有连接 │
    │                               │                               │
    │                               │── RaySyncMessage ────────────>│
    │                               │   (bidi gRPC stream)          │
    │                               │                               │
    │                               │           NodeState::ConsumeSyncMessage
    │                               │           ├─ 版本检查通过
    │                               │           └─ NodeManager::ConsumeSyncMessage
    │                               │               └─ UpdateResourceUsage
    │                               │                   └─ ClusterResourceManager::UpdateNode
    │                               │                       ├─ nodes_[B] = 新数据
    │                               │                       └─ received_node_resources_[B] = 新数据
```

### 2.2 版本去重协议及其缺陷

RaySyncer 使用**版本号去重**避免重复传输。去重发生在三个环节：

**环节 1：源端生成（`local_resource_manager.cc:422-444`）：**

```cpp
std::optional<syncer::RaySyncMessage> LocalResourceManager::CreateSyncMessage(
    int64_t after_version, syncer::MessageType message_type) const {
  if (version_ <= after_version) {
    return std::nullopt;  // version 没变，不生成消息
  }
  msg.set_version(version_);
  // ...
}
```

**环节 2：发送队列（`ray_syncer_bidi_reactor_base.h:60-82`）：**

```cpp
bool PushToSendingQueue(std::shared_ptr<const RaySyncMessage> message) override {
    auto &node_versions = GetNodeComponentVersions(message->node_id());
    if (node_versions[message->message_type()] >= message->version()) {
      return false;  // 已发送过该版本或更新版本，丢弃
    }
    node_versions[message->message_type()] = message->version();
    // ...
}
```

**环节 3：接收端（`node_state.cc:59-83`）：**

```cpp
bool NodeState::ConsumeSyncMessage(std::shared_ptr<const RaySyncMessage> message) {
  auto &current = cluster_view_[message->node_id()][message->message_type()];
  if (current && current->version() >= message->version()) {
    return false;  // 版本没增加，丢弃
  }
  current = message;
  // ...
}
```

**协议缺陷：** 如果远端节点的资源状态没有发生任何变化（version 不递增），即使本地对该节点的资源视图已经被推测性扣减弄"脏"了，也不会有新的 syncer 消息来纠正。这就是 `ray_syncer_message_refresh_interval_ms` 定时器存在的原因。

**Step-by-Step：本地视图如何变得过时**

以下详细说明脏视图产生的全过程：

```
Step 1: Raylet B 报告资源 (version=5) 到 GCS
        LocalResourceManager::CreateSyncMessage(version=5, 2 CPUs available)
        ↓ bidi gRPC stream
        GCS BroadcastMessage → 所有其他 Raylet

Step 2: GCS 广播到 Raylet A，A 存储 version=5
        NodeState::ConsumeSyncMessage → 版本检查通过
        → NodeManager::ConsumeSyncMessage
        → ClusterResourceManager::UpdateNode
          → nodes_[B] = {2 CPUs}                     ← 正确
          → received_node_resources_[B] = {2 CPUs}    ← 干净快照

Step 3: Raylet A 调度器 spillback 一个 task 到 B，推测性扣减
        SubtractNodeAvailableResources(B, 1 CPU)
        → GetMutableLocalView() → modified_ts = Now()
        → nodes_[B].available = {1 CPU}               ← 脏了！
        → received_node_resources_[B] = {2 CPUs}      ← 没动

Step 4: B 的资源状态没变化（或 task 还没到达 B），version 仍=5

        ┌─ 源端（Raylet B）：
        │  CreateSyncMessage(after_version=5)
        │  version_(5) <= after_version(5) → return nullopt
        │  不生成新消息
        │
        ├─ 发送队列（GCS reactor）：
        │  node_versions[B][RESOURCE_VIEW] == 5 >= 5
        │  即使有消息也会被丢弃
        │
        └─ 接收端（Raylet A）：
           cluster_view_[B] 已有 version=5
           新消息 version=5 → 被丢弃

        结果：Raylet A 的脏视图（1 CPU）永远得不到纠正！
        → 只能等定时器（ray_syncer_message_refresh_interval_ms）兜底
```

**更新丢失的几种场景：**

| 场景 | 原因 | 影响 |
|------|------|------|
| 远端节点资源没变 | version 不递增，不生成新消息 | 本地脏视图持续到定时器重置 |
| 相同 version 重发 | 发送队列和接收端的版本去重丢弃 | 同上 |
| 网络丢包 | 消息丢失后，若 version 不递增则不会重传 | 同上 |
| Task 被 reject/取消 | B 的资源实际未变化，version 不递增 | spillback 扣减的资源长期不纠正 |

### 2.3 资源视图的"脏"与"干净"

`ClusterResourceManager` 内部维护**两份**远端节点资源数据（`cluster_resource_manager.h:188-191`）：

```cpp
absl::flat_hash_map<scheduling::NodeID, Node> nodes_;                    // 工作视图（可能脏）
absl::flat_hash_map<scheduling::NodeID, NodeResources> received_node_resources_;  // 干净快照
```

| 数据结构 | 谁写入 | 用途 |
|----------|--------|------|
| `nodes_` | syncer 更新（`UpdateNode`）+ 调度器扣减（`SubtractNodeAvailableResources`） | 调度器直接用于打分和选节点 |
| `received_node_resources_` | 仅 syncer 更新（`UpdateNode`）+ 节点 draining 状态变更（`SetNodeDraining`） | 定时器重置脏视图的"复原点" |

`received_node_resources_` 的写入点有三个：
1. `UpdateNode`（`cluster_resource_manager.cc:111`）— syncer 消息到达时保存干净快照
2. `RemoveNode`（`cluster_resource_manager.cc:116`）— 节点移除时删除
3. `SetNodeDraining`（`cluster_resource_manager.cc:131-135`）— 调整 draining 状态

**数据流：**

```
时刻 T=0:  syncer 消息到达，B 有 4 CPU
           → UpdateNode()
           → nodes_[B] = Node({4 CPU}), modified_ts = nullopt   ← 干净
           → received_node_resources_[B] = {4 CPU}              ← 干净快照

时刻 T=1:  调度器 spillback 到 B，扣减 1 CPU
           → SubtractNodeAvailableResources(B, 1 CPU)
           → GetMutableLocalView() → modified_ts = T1           ← 标记修改时间
           → nodes_[B].available = 3 CPU                        ← 脏了
           → received_node_resources_[B] 不变，仍然是 4 CPU

时刻 T=5:  新 syncer 消息到达（B 资源有变化, version 递增）
           → UpdateNode() → it->second = Node(新数据)
           → nodes_[B] = 新数据, modified_ts = nullopt          ← 又干净了
           → received_node_resources_[B] = 新数据
```

**关键代码 — `Node` 结构体（`cluster_resource_data.h:384-406`）：**

```cpp
struct Node {
  explicit Node(const NodeResources &resources) : local_view_(resources) {}

  NodeResources *GetMutableLocalView() {
    local_view_modified_ts_ = absl::Now();  // 调度器扣减时标记修改时间
    return &local_view_;
  }

  const NodeResources &GetLocalView() const { return local_view_; }

  std::optional<absl::Time> GetViewModifiedTs() const { return local_view_modified_ts_; }

 private:
  /// Our local view of the remote node's resources. This may be dirty
  /// because it includes any resource requests that we allocated to this
  /// node through spillback since our last heartbeat tick.
  NodeResources local_view_;
  std::optional<absl::Time> local_view_modified_ts_;  // 默认 nullopt
};
```

**`AddOrUpdateNode` — 用干净数据覆盖时重建 Node 对象（`cluster_resource_manager.cc:64-74`）：**

```cpp
void ClusterResourceManager::AddOrUpdateNode(scheduling::NodeID node_id,
                                             const NodeResources &node_resources) {
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    nodes_.emplace(node_id, node_resources);
  } else {
    it->second = Node(node_resources);  // 整个 Node 被替换，modified_ts 重置为 nullopt
  }
}
```

### 2.4 定时刷新机制：syncer 的兜底

**定时器逻辑（`cluster_resource_manager.cc:27-44`）：**

```cpp
ClusterResourceManager::ClusterResourceManager(instrumented_io_context &io_service)
    : timer_(PeriodicalRunner::Create(io_service)) {
  timer_->RunFnPeriodically(
      [this]() {
        auto syncer_delay = absl::Milliseconds(
            RayConfig::instance().ray_syncer_message_refresh_interval_ms());
        for (auto &[node_id, resource] : received_node_resources_) {
          auto modified_ts = GetNodeResourceModifiedTs(node_id);
          if (modified_ts && *modified_ts + syncer_delay < absl::Now()) {
            AddOrUpdateNode(node_id, resource);  // 用干净快照覆盖脏视图
          }
        }
      },
      RayConfig::instance().ray_syncer_message_refresh_interval_ms(),
      "ClusterResourceManager.ResetRemoteNodeView");
}
```

**条件解析：`modified_ts && *modified_ts + syncer_delay < absl::Now()`**

- `modified_ts` 为 `nullopt`（syncer 刚更新过，或从未被扣减）→ 条件为 false → **不处理**
- `modified_ts` 有值且距今超过 `syncer_delay` → 条件为 true → **重置为干净快照**

**状态转换图：**

```
状态1: 干净
  nodes_[B] = syncer 数据
  modified_ts = nullopt
  定时器: 不处理（modified_ts 为空）
       │
       │ SubtractNodeAvailableResources (spillback 扣减)
       ▼
状态2: 脏
  nodes_[B] = syncer 数据 - 推测扣减
  modified_ts = absl::Now()
  定时器: 等待中...
       │
       ├─── 路径A: 新 syncer 消息到达 (version 递增)
       │     → UpdateNode() → it->second = Node(新数据)
       │     → modified_ts = nullopt
       │     → 回到状态1  (syncer 纠正了)
       │
       └─── 路径B: 没有新 syncer 消息 (version 没变)
             → 等 N 秒（ray_syncer_message_refresh_interval_ms）
             → 定时器触发: modified_ts + N秒 < Now()
             → AddOrUpdateNode(B, received_node_resources_[B])
             → 用最后的干净快照覆盖脏视图
             → modified_ts = nullopt
             → 回到状态1  (定时器纠正了)
```

**两种纠正方式的对比：**

| 纠正方式 | 触发条件 | 延迟 |
|----------|---------|------|
| **Syncer 推送**（主路径） | B 的资源变化 → version 递增 → GCS 广播 | 通常 < 1s |
| **定时器重置**（兜底） | B 的 version 不变，且脏视图超过 N 秒 | 最多 N 秒 |

定时器只在 syncer 无法工作时才有意义——即 B 的资源恰好没有任何变化（没有 task 启动/结束），导致 version 不递增。

### 2.5 `ray_syncer_message_refresh_interval_ms` 的线程模型

**这是 Raylet 端配置，运行在 Raylet 主线程上，不会单独起线程。**

线程归属追踪：

```
main.cc:310-313
  instrumented_io_context main_service (单线程事件循环, "raylet_main_io_context")
       │
main.cc:871-872
       ├── ClusterResourceScheduler(main_service, ...)
       │      │
       │      └── cluster_resource_scheduler.cc:71
       │          ClusterResourceManager(io_service)  ← 定时器注册在此
       │
node_manager.cc:235
       └── ray_syncer_(io_service_, ...)  ← syncer 也在主线程
```

**Raylet vs GCS 的 Syncer 线程模型对比：**

| 组件 | Syncer 运行线程 | 代码位置 |
|------|---------------|---------|
| **Raylet** | `main_service`（主线程） | `node_manager.cc:235`: `ray_syncer_(io_service_, ...)` |
| **GCS** | 独立线程 `ray_syncer_io_context` | `gcs_server.cc:597`: `io_context_provider_.GetIOContext<syncer::RaySyncer>()` |

GCS 的 syncer 要处理所有 Raylet 的连接和广播（O(N^2) 消息量），所以用了独立线程。

**功能意义：** 虽然 GCS 也创建了 `ClusterResourceManager`（用于 Actor 调度和 Placement Group 调度，`gcs_server.cc:439-448`），理论上也跑着同一个定时器，但 GCS 端不会调用 `SubtractNodeAvailableResources`（它主要用资源视图做只读查询：自动扩缩容、获取可用资源 RPC），所以 `nodes_` 和 `received_node_resources_` 在 GCS 端几乎不会分歧，定时器在 GCS 端实质上是空操作。

**GCS 的 `ClusterResourceManager` 初始化（`gcs_server.cc:439-448`）：**

```cpp
void GcsServer::InitClusterResourceScheduler() {
  cluster_resource_scheduler_ = std::make_shared<ClusterResourceScheduler>(
      io_context_provider_.GetDefaultIOContext(),   // 运行在 GCS 默认 io_context（主线程）
      scheduling::NodeID(kGCSNodeID.Binary()),
      NodeResources(),
      /*is_node_available_fn=*/
      [](auto) { return true; },
      /*is_local_node_with_raylet=*/false);
}
```

注意：GCS 的 `ClusterResourceManager` 运行在**默认 GCS io_context**（主线程），而其 `RaySyncer` 运行在**独立的 `ray_syncer_io_context`** 线程。当 syncer 收到消息后，`GcsResourceManager::ConsumeSyncMessage` 会 dispatch 到主线程执行：

```cpp
// ConsumeSyncMessage is called by ray_syncer which might not run
// in a dedicated thread for performance.
io_context_.dispatch([this, message]() { ... }, "GcsResourceManager::Update");
```

### 2.6 GCS 在 Syncer 中的角色

GCS 在 sync 机制中充当**消息中继/广播器（Hub）**：

```
                    ┌──────────────────────────────────────────┐
                    │                GCS                        │
                    │                                           │
                    │  ray_syncer_io_context 线程               │
                    │  ├─ 接收每个 Raylet 的资源汇报            │
                    │  ├─ NodeState::ConsumeSyncMessage (去重)  │
                    │  ├─ BroadcastMessage → N 个 reactor       │
                    │  │  (遍历所有连接，逐个 PushToSendingQueue)│
                    │  └─ 通知 GcsResourceManager (dispatch)   │
                    │                                           │
                    │  main_service 主线程                      │
                    │  └─ GcsResourceManager::UpdateFromResView │
                    │     ├─ ClusterResourceManager::UpdateNode │
                    │     └─ 更新自动扩缩容视图                 │
                    └────────────┬───────────────┬──────────────┘
                                 │               │
                           bidi stream      bidi stream
                                 │               │
                          ┌──────┴─────┐  ┌──────┴─────┐
                          │  Raylet A  │  │  Raylet B  │
                          └────────────┘  └────────────┘
```

**GCS 的三重角色：**
1. **接收：** 接收每个 Raylet 的资源汇报
2. **广播：** 将更新广播给所有其他 Raylet
3. **消费：** 自己也消费更新（用于 GCS 端的 Actor 调度、Placement Group 调度和自动扩缩容决策）

### 2.7 On-Demand（按需）资源上报机制

#### 2.7.1 名称含义：定时轮询 + 按需生成

`OnDemandBroadcasting` 的名字有一定误导性。它**不是**纯事件驱动的"有变化才推送"模式，而是**定时轮询 + 版本去重**的模式：

```
定时触发 (每 raylet_report_resources_period_milliseconds 毫秒, 默认 100ms)
    │
    ▼
OnDemandBroadcasting(RESOURCE_VIEW)
    │
    ├── version_ 没变 → CreateSyncMessage 返回 nullopt → 空跑，不广播
    │
    └── version_ 递增了 → CreateSyncMessage 返回消息 → BroadcastMessage → 推送到 GCS
```

**核心代码（`ray_syncer.cc:199-207`）：**

```cpp
bool RaySyncer::OnDemandBroadcasting(MessageType message_type) {
  auto msg = node_state_->CreateSyncMessage(message_type);
  if (msg) {
    RAY_CHECK(msg->node_id() == GetLocalNodeID());
    BroadcastMessage(std::make_shared<RaySyncMessage>(std::move(*msg)));
    return true;
  }
  return false;  // version 没变，不生成消息，空跑
}
```

#### 2.7.2 `raylet_report_resources_period_milliseconds` 的传导路径

`raylet_report_resources_period_milliseconds`（默认 100ms，`ray_config_def.h:65`）通过以下路径传导为 `OnDemandBroadcasting` 的定时调用间隔：

```
ray_config_def.h:65
  raylet_report_resources_period_milliseconds = 100
      │
main.cc:609-610
      ├── node_manager_config.report_resources_period_ms = 100
      │
node_manager.cc:204
      ├── report_resources_period_ms_ = 100
      │
node_manager.cc:345-350
      └── ray_syncer_.Register(
              RESOURCE_VIEW,
              &LocalResourceManager,        // reporter
              this,                          // receiver
              report_resources_period_ms_)   // ← 100ms
                  │
ray_syncer.cc:182-188
                  └── timer_->RunFnPeriodically(
                          [](){ OnDemandBroadcasting(RESOURCE_VIEW); },
                          100,  // ← pull_from_reporter_interval_ms
                          "RaySyncer.OnDemandBroadcasting")
```

**定时器注册代码（`ray_syncer.cc:168-197`）：**

```cpp
void RaySyncer::Register(MessageType message_type,
                         const ReporterInterface *reporter,
                         ReceiverInterface *receiver,
                         int64_t pull_from_reporter_interval_ms) {
  io_context_.dispatch([this, message_type, reporter, receiver,
                        pull_from_reporter_interval_ms]() mutable {
    if (!node_state_->SetComponent(message_type, reporter, receiver)) {
      return;
    }
    if (reporter != nullptr && pull_from_reporter_interval_ms > 0) {
      timer_->RunFnPeriodically(
          [this, stopped = stopped_, message_type]() {
            if (*stopped) return;
            OnDemandBroadcasting(message_type);
          },
          pull_from_reporter_interval_ms,
          "RaySyncer.OnDemandBroadcasting");
    }
  }, "RaySyncerRegister");
}
```

**Raylet vs GCS 的注册差异：**

| 端 | reporter | pull_interval | 是否有定时器 |
|----|----------|--------------|-------------|
| **Raylet** RESOURCE_VIEW | `LocalResourceManager` | 100ms | **有** — 每 100ms 轮询 |
| **Raylet** COMMANDS | `NodeManager` | 0 | **无** — 只在 GC 时手动调用 |
| **GCS** RESOURCE_VIEW | `nullptr` | 无 | **无** — GCS 不主动 pull |
| **GCS** COMMANDS | `nullptr` | 无 | **无** — GCS 不主动 pull |

GCS 端注册时 `reporter=nullptr`（`gcs_server.cc:606-609`），所以不会设置定时器，GCS 只作为中继/广播器。

#### 2.7.3 哪些情况下触发真正的资源上报

定时器每 100ms 调用 `OnDemandBroadcasting`，但只有 `version_` 递增了才会真正生成消息。`version_` 只在 `OnResourceOrStateChanged()` 中递增（`local_resource_manager.cc:454`）：

```cpp
void LocalResourceManager::OnResourceOrStateChanged() {
  // ...
  ++version_;
  if (resource_change_subscriber_ == nullptr) return;
  resource_change_subscriber_(ToNodeResources());
}
```

**所有触发 `version_++` 的场景：**

| 触发场景 | 代码位置 | 说明 |
|---------|---------|------|
| `AllocateLocalTaskResources` | `local_resource_manager.cc:286` | Task 分配资源（仅成功时） |
| `ReleaseWorkerResources` | `local_resource_manager.cc:308` | Task 释放资源 |
| `AddLocalResourceInstances` | `local_resource_manager.cc:63` | 动态增加资源 |
| `DeleteLocalResource` | `local_resource_manager.cc:70` | 动态删除资源 |
| `AddResourceInstances` | `local_resource_manager.cc:218` | 归还资源实例 |
| `SubtractResourceInstances` | `local_resource_manager.cc:243` | 消耗资源实例 |
| `MarkFootprintAsBusy` | `local_resource_manager.cc:143` | 节点变忙（仅状态变化时） |
| `MaybeMarkFootprintAsBusy` | `local_resource_manager.cc:166` | 推测性标记忙 |
| `MarkFootprintAsIdle` | `local_resource_manager.cc:192` | 节点变空闲（仅状态变化时） |
| `UpdateAvailableObjectStoreMemResource` | `local_resource_manager.cc:350` | Object Store 内存变化 |
| `SetLocalNodeDraining` | `local_resource_manager.cc:531` | 节点进入 draining |

**关键细节：**
- **失败的资源分配不递增 version**：`AllocateLocalTaskResources` 返回 false 时不调用 `OnResourceOrStateChanged()`
- **空闲状态变化也递增 version**：即使资源量没变，只是从 busy 变 idle，也会递增（影响自动扩缩容的 drain 决策）
- **Object Store 内存是惰性检测**：`UpdateAvailableObjectStoreMemResource` 通过 `const_cast` 在 `CreateSyncMessage` 内部被调用，仅在定时轮询生成消息时才检查内存变化

```cpp
// local_resource_manager.cc:428 — CreateSyncMessage 内部的惰性检测
const_cast<LocalResourceManager *>(this)->UpdateAvailableObjectStoreMemResource();
```

#### 2.7.4 COMMANDS 通道的手动触发

COMMANDS 通道（用于全局 GC 命令）注册时 `pull_from_reporter_interval_ms=0`，不设定时器。只在需要全局 GC 时手动调用（`node_manager.cc:2876-2881`）：

```cpp
if (should_global_gc_) {
  gc_command_sync_version_++;
  ray_syncer_.OnDemandBroadcasting(syncer::MessageType::COMMANDS);
  should_global_gc_ = false;
}
```

### 2.8 `RaySyncer.BroadcastMessage` vs `RaySyncer.OnDemandBroadcasting` 指标

这两个都是 `event_stats=1` 时的事件循环统计指标。

#### 2.8.1 指标归属和含义

| 指标 | 所属端 | 运行线程 | 触发方式 | 统计内容 |
|------|-------|---------|---------|---------|
| `RaySyncer.OnDemandBroadcasting` | **仅 Raylet** | Raylet 主线程 | 定时器每 100ms | 拉取本地资源快照 + 广播(如有变化) |
| `RaySyncer.BroadcastMessage` | **Raylet + GCS** | Raylet 主线程 / GCS `ray_syncer_io_context` | OnDemandBroadcasting 内部 或 收到远端消息 | 版本检查 + 推到发送队列 |

**GCS 端没有 `OnDemandBroadcasting` 指标**（因为 `reporter=nullptr`，不注册定时器）。GCS 端只有 `BroadcastMessage` 指标。

#### 2.8.2 两个指标在 Raylet 端的包含关系

在 Raylet 端，`OnDemandBroadcasting` **包含** `BroadcastMessage`：

```
PeriodicalRunner 定时器到期
    │
    ▼
[RaySyncer.OnDemandBroadcasting 开始计时 ─────────────────────────┐
    │                                                               │
    ├── CreateSyncMessage(RESOURCE_VIEW)                           │
    │     ├── UpdateAvailableObjectStoreMemResource() (惰性检测)   │
    │     └── version check + serialize protobuf                   │
    │                                                               │
    ├── 如果有消息: BroadcastMessage(msg)                           │
    │     └── io_context_.dispatch(...)                             │
    │         ├── 同线程: 内联执行                                  │
    │         │   [RaySyncer.BroadcastMessage 开始/结束计时]       │
    │         │     ├── ConsumeSyncMessage (版本检查)               │
    │         │     └── PushToSendingQueue (1 个 reactor → GCS)    │
    │         │                                                     │
    │         └── 不同线程: 入队后返回                               │
    │                                                               │
    RaySyncer.OnDemandBroadcasting 结束计时 ──────────────────────┘
```

**注意：** 由于 Raylet 的 `io_context_` 就是主线程的 `io_service_`，`dispatch()` 在同线程上是内联执行（不入队），所以 `BroadcastMessage` 的执行时间包含在 `OnDemandBroadcasting` 内。

#### 2.8.3 两个指标在 GCS 端的独立性

GCS 端只有 `BroadcastMessage`，当收到 Raylet 的 gRPC 消息时触发：

```
[GCS ray_syncer_io_context 线程]

gRPC BidiReactor 收到消息
    │
    ▼
RaySyncer::BroadcastMessage(msg)
    │
    ▼
io_context_.dispatch(
    [RaySyncer.BroadcastMessage 开始计时 ──────────────────┐
        ├── ConsumeSyncMessage (版本检查)                    │
        │     └── 通知 GcsResourceManager (dispatch 到主线程)│
        └── PushToSendingQueue x N 个 Raylet reactor        │
            (O(N) 遍历, 这是 GCS CPU 的主要开销)            │
    RaySyncer.BroadcastMessage 结束计时 ───────────────────┘
)
```

**GCS 端 `BroadcastMessage` 的开销远大于 Raylet 端**，因为 GCS 需要遍历 N 个 Raylet 的 reactor 逐个 `PushToSendingQueue`。

#### 2.8.4 指标异常分析

当观察到 **`RaySyncer.OnDemandBroadcasting`: 1.6万次 total, mean=16ms, max=1986ms** 时：

**这是 Raylet 端指标。**

**次数分析：** 按默认 100ms 间隔，16000 次 = 1600 秒 ≈ 26.7 分钟的运行时间。如果 `event_stats_print_interval_ms=180000`（180s），则 180s 内应约 1800 次。16000 次可能是多个 print interval 累积，或者 `raylet_report_resources_period_milliseconds` 被调小了。

**mean=16ms 分析：** 正常值应在微秒到亚毫秒级。16ms 偏高，说明 **Raylet 主线程存在事件循环压力**。这 16ms 包含两部分：
- **排队等待时间**（`operation_queue_time`）：定时器到期后等主线程空闲才执行
- **执行时间**（`operation_run_time`）：实际函数执行

```
定时器到期          开始执行                     执行完毕
    │                  │                            │
    │<── 排队等待 ──>│<──── 执行时间 ────────────>│
    │                  │                            │
    │<────────── mean=16ms (总计) ────────────────>│
```

如果主线程忙于大量调度决策（`ScheduleAndGrantLeases` 循环）、gRPC 回调处理等，定时器到期后可能排队很久才得到执行。

**max=1986ms 分析：** 最大延迟接近 2 秒，说明 **Raylet 主线程曾出现严重的事件循环阻塞**，可能原因：
- 大量 task 同时到达，调度决策循环耗时长
- 大量 gRPC 回调排队
- GC 操作阻塞主线程
- 大量 `ScheduleAndGrantLeases` 循环处理 pending leases

**与 `ray_syncer_io_context` 的关系：** `RaySyncer.OnDemandBroadcasting` 指标**与 GCS 的 `ray_syncer_io_context` 线程无直接关系**。它反映的是 Raylet 主线程的压力。

```
┌─────── Raylet ─────────────────────────────┐
│ 主线程 (raylet_main_io_context)             │
│                                              │
│  OnDemandBroadcasting ← mean=16ms 反映这里  │
│      └── BroadcastMessage (内联)             │
│           └── PushToSendingQueue             │
│                └── 消息进入 gRPC 发送缓冲    │
│                                              │
│  [主线程压力来源: 调度、RPC回调、GC等]       │
└──────────────────┬───────────────────────────┘
                   │ gRPC BidiStream
                   ▼
┌─────── GCS ──────────────────────────────────┐
│ ray_syncer_io_context 线程                    │
│                                               │
│  收到消息 → BroadcastMessage → PushToSendingQ │
│  [这个线程的压力看 GCS 端的                    │
│   RaySyncer.BroadcastMessage 指标]            │
└───────────────────────────────────────────────┘
```

**排查建议：** 要区分排队时间和执行时间，需要同时关注：
- `operation_queue_time_ms{Name="RaySyncer.OnDemandBroadcasting"}` — 排队延迟
- `operation_run_time_ms{Name="RaySyncer.OnDemandBroadcasting"}` — 执行耗时
- `io_context_event_loop_lag_ms{Name="raylet_main_io_context"}` — 主线程整体响应能力

---

## 三、调度决策流程

### 3.1 Task 调度的完整链路

```
TASK 提交 (ray.remote().get())
    │
    ▼
Core Worker (与 Raylet A 同节点)
    │ RequestWorkerLease RPC
    ▼
Raylet A: ClusterLeaseManager::QueueAndScheduleLease()
    │
    ▼
ClusterLeaseManager::ScheduleAndGrantLeases()
    │
    ├── GetBestSchedulableNode() 评估所有节点
    │     │
    │     ├─ [INFEASIBLE] NodeID=Nil, is_infeasible=true
    │     │   → 集群中没有节点有足够总资源
    │     │   → 进入 infeasible_leases_ 队列，等集群扩容
    │     │
    │     ├─ [WAITING] NodeID=Nil, is_infeasible=false
    │     │   → 有节点 feasible 但当前都没空闲资源
    │     │   → 留在 leases_to_schedule_ 队列等待
    │     │
    │     └─ [SCHEDULABLE] NodeID=某节点
    │         │
    │         ├── NodeID == self (本地) → LocalLeaseManager 处理
    │         └── NodeID == 远端     → Spillback
    │
    ▼
LocalLeaseManager::ScheduleAndGrantLeases()
    │
    ├── GrantScheduledLeasesToWorkers()
    │     ├─ 调度类容量检查
    │     ├─ 本地资源分配
    │     └─ PopWorker → Grant Lease
    │
    └── SpillWaitingLeases()
          └─ 依赖拉取被阻塞 → 强制 Spillback
```

### 3.2 ClusterLeaseManager：集群级调度

**核心代码（`cluster_lease_manager.cc:196-296`）：**

```cpp
void ClusterLeaseManager::ScheduleAndGrantLeases() {
  TryScheduleInfeasibleLease();  // 重新检查之前 infeasible 的任务

  for (auto shapes_it = leases_to_schedule_.begin(); ...) {
    for (auto work_it = work_queue.begin(); ...) {
      auto scheduling_node_id = cluster_resource_scheduler_.GetBestSchedulableNode(
          lease.GetLeaseSpecification(),
          /*preferred_node_id*/ work->PrioritizeLocalNode()
              ? self_node_id_.Binary()
              : lease.GetPreferredNodeID(),
          /*exclude_local_node*/ false,
          /*requires_object_store_memory*/ false,
          &is_infeasible);

      if (scheduling_node_id.IsNil()) {
        // INFEASIBLE 或 WAITING
        break;
      }

      NodeID node_id = NodeID::FromBinary(scheduling_node_id.Binary());
      ScheduleOnNode(node_id, work);  // 路由到本地或远端
      work_it = work_queue.erase(work_it);
    }
  }

  local_lease_manager_.ScheduleAndGrantLeases();  // 处理本地队列
}
```

**`ScheduleOnNode` 的路由逻辑（`cluster_lease_manager.cc:422-461`）：**

```cpp
void ClusterLeaseManager::ScheduleOnNode(const NodeID &spillback_to,
                                         const std::shared_ptr<internal::Work> &work) {
  // 路由1: 本地节点
  if (spillback_to == self_node_id_) {
    local_lease_manager_.QueueAndScheduleLease(work);
    return;
  }

  // 路由2: 远端节点且 grant_or_reject=true → 拒绝
  if (work->grant_or_reject_) {
    for (const auto &reply_callback : work->reply_callbacks_) {
      reply_callback.reply_->set_rejected(true);
      reply_callback.send_reply_callback_(Status::OK(), nullptr, nullptr);
    }
    return;
  }

  // 路由3: Spillback 到远端
  // 推测性扣减（best-effort）
  if (!cluster_resource_scheduler_.AllocateRemoteTaskResources(
          scheduling::NodeID(spillback_to.Binary()),
          lease_spec.GetRequiredResources().GetResourceMap())) {
    RAY_LOG(DEBUG) << "Tried to allocate resources for request "
                   << "on a remote node that are no longer available";
  }

  // 回复 client: 去远端重试
  for (const auto &reply_callback : work->reply_callbacks_) {
    auto reply = reply_callback.reply_;
    reply->mutable_retry_at_raylet_address()->set_ip_address(...);
    reply->mutable_retry_at_raylet_address()->set_port(...);
    reply->mutable_retry_at_raylet_address()->set_node_id(spillback_to.Binary());
    reply_callback.send_reply_callback_(Status::OK(), nullptr, nullptr);
  }
}
```

### 3.3 LocalLeaseManager：本地调度与二次 Spillback

即使 ClusterLeaseManager 选了本地节点，进入 LocalLeaseManager 后仍可能被 spillback，有三种触发场景：

**场景 1：调度类容量超限（`local_lease_manager.cc:280-312`）**

```cpp
if (sched_cls_cap_enabled_ &&
    sched_cls_info.granted_leases.size() >= sched_cls_info.capacity &&
    work->GetState() == internal::WorkStatus::WAITING) {
  if (get_time_ms_() < sched_cls_info.next_update_time) {
    bool did_spill = TrySpillback(work, is_infeasible);
    // ...
  }
}
```

容量计算（`local_lease_manager.cc:1182-1193`）：

```cpp
uint64_t MaxGrantedLeasesPerSchedulingClass(SchedulingClass sched_cls_id) const {
  double cpu_req = sched_cls.resource_set.Get(ResourceID::CPU()).Double();
  uint64_t total_cpus = ...GetNumCpus();
  if (cpu_req == 0 || total_cpus == 0) return MAX;
  return static_cast<uint64_t>(std::round(total_cpus / cpu_req));
}
```

例如：8 CPU 节点，1 CPU/task，容量 = 8。超过后触发指数退避，并尝试 spillback。

**场景 2：本地资源分配失败（`local_lease_manager.cc:354-373`）**

```cpp
bool schedulable =
    !cluster_resource_scheduler_.GetLocalResourceManager().IsLocalNodeDraining() &&
    cluster_resource_scheduler_.GetLocalResourceManager()
        .AllocateLocalTaskResources(spec.GetRequiredResources().GetResourceMap(),
                                    allocated_instances);
if (!schedulable) {
  bool did_spill = TrySpillback(work, is_infeasible);
  if (!did_spill) {
    work->SetStateWaiting(UnscheduledWorkCause::WAITING_FOR_RESOURCES_AVAILABLE);
    break;
  }
}
```

**场景 3：等待依赖被阻塞（`local_lease_manager.cc:439-514`）**

```cpp
void LocalLeaseManager::SpillWaitingLeases() {
  for (auto it = waiting_lease_queue_.end(); it != waiting_lease_queue_.begin(); ) {
    it--;
    bool lease_dependencies_blocked =
        lease_dependency_manager_.LeaseDependenciesBlocked(lease_id);

    scheduling_node_id = cluster_resource_scheduler_.GetBestSchedulableNode(
        lease_spec,
        /*preferred_node_id*/ self_node_id_.Binary(),
        /*exclude_local_node*/ lease_dependencies_blocked,  // 依赖阻塞时强制选远端
        /*requires_object_store_memory*/ true,
        &is_infeasible);

    if (scheduling_node_id != self_scheduling_node_id_ && !scheduling_node_id.IsNil()) {
      Spillback(node_id, *it);
    }
  }
}
```

**`TrySpillback` 本身（`local_lease_manager.cc:516-541`）：**

```cpp
bool LocalLeaseManager::TrySpillback(const std::shared_ptr<internal::Work> &work,
                                     bool &is_infeasible) {
  auto scheduling_node_id = cluster_resource_scheduler_.GetBestSchedulableNode(
      spec,
      /*preferred_node_id=*/self_node_id_.Binary(),  // 偏向本地
      /*exclude_local_node=*/false,                   // 不排除本地
      /*requires_object_store_memory=*/false,
      &is_infeasible);

  if (is_infeasible || scheduling_node_id.IsNil() ||
      scheduling_node_id == self_scheduling_node_id_) {
    return false;  // 没找到更好的远端节点，留在本地
  }

  Spillback(node_id, work);
  return true;
}
```

---

## 四、Spillback 机制详解

### 4.1 Spillback 不是转发，是重定向

**核心概念：Spillback 不是 Raylet A 把 task 发给 Raylet B 执行。** 它是 Raylet A 告诉 Core Worker（client）："去 Raylet B 重新提交你的 lease 请求"。

```
Core Worker ---RequestWorkerLease--→ Raylet A
                                        │
                                        │ 调度器选了 B
                                        │ 推测性扣减 B 的资源视图
                                        │
Core Worker ←--reply: retry_at=B-------─┘
    │
    │  (client 自己重新发 RPC 给 B)
    │
    └───RequestWorkerLease(grant_or_reject=true)──→ Raylet B
```

### 4.2 Client 与 Raylet 的关系

**Client（Core Worker）和 Raylet A 是同一个物理节点上的进程。**

```
┌─────── Node A ───────┐
│                       │
│  Core Worker (client) │  ← 用户的 task 在这里提交
│       │               │
│       │ 本地 gRPC     │
│       ▼               │
│  Raylet A (调度器)    │  ← 负责该节点上所有 worker 的调度
│                       │
└───────────────────────┘
```

每个 Core Worker 启动时就绑定了本机的 Raylet（`core_worker_process.cc:242-247`）：

```cpp
auto raylet_address = rpc::RayletClientPool::GenerateRayletAddress(
    local_node_id, options.node_ip_address, options.node_manager_port);
auto local_raylet_rpc_client =
    std::make_shared<rpc::RayletClient>(std::move(raylet_address), ...);
```

首次 lease 请求通常走 `LocalLeasePolicy`（`lease_policy.cc:90-94`），总是返回本地 Raylet：

```cpp
std::pair<rpc::Address, bool> LocalLeasePolicy::GetBestNodeForLease(
    const LeaseSpecification &spec) {
  return std::make_pair(local_node_rpc_address_, false);
}
```

### 4.3 `grant_or_reject` 协议：最多一跳

Spillback 时 client 向远端 Raylet 发送的请求带有 `grant_or_reject = true`（`normal_task_submitter.cc:330`）：

```cpp
raylet_client->RequestWorkerLease(
    lease_spec.GetMessage(),
    /*grant_or_reject=*/is_spillback,  // spillback 时为 true
    ...);
```

远端 Raylet 收到 `grant_or_reject=true` 的请求后，**只能 grant 或 reject，不能再 spillback 到第三个节点**（`cluster_lease_manager.cc:429-435`）：

```cpp
if (work->grant_or_reject_) {
    for (const auto &reply_callback : work->reply_callbacks_) {
      reply_callback.reply_->set_rejected(true);
      reply_callback.send_reply_callback_(Status::OK(), nullptr, nullptr);
    }
    return;  // 不会再 spillback
}
```

Proto 定义（`node_manager.proto:47-51`）：

```protobuf
// If true, then the raylet should either find the resources to be
// locally schedulable or reject the request.
// Else, the raylet may return another raylet at which to retry the request.
bool grant_or_reject = 3;
```

### 4.4 Reject 后的重试机制

Client 端处理 reject（`normal_task_submitter.cc:398-405`）：

```cpp
} else if (reply.rejected()) {
    RAY_LOG(DEBUG) << "Lease rejected " << lease_id;
    // It might happen when the first raylet has a stale view
    // of the spillback raylet resources.
    // Retry the request at the first raylet since the resource view may be
    // refreshed.
    RAY_CHECK(is_spillback);
    RequestNewWorkerIfNeeded(scheduling_key);  // 无地址参数 → 回到本地 Raylet 重新调度
}
```

**完整生命周期：**

```
                    ┌─────────────────────────────────────────┐
                    │              Core Worker                 │
                    └──────┬──────────────────────┬───────────┘
                           │                      ▲
               ① RequestWorkerLease          ⑤ rejected=true
               (grant_or_reject=false)            │
                           ▼                      │
                    ┌──────────────┐              │
                    │   Raylet A   │              │
                    │ 调度器选 B    │              │
                    │ 扣减 B 视图  │              │
                    └──────┬───────┘              │
                           │                      │
               ② reply: retry_at=B               │
                           ▼                      │
                    ┌──────────────┐              │
                    │ Core Worker  │              │
                    └──────┬───────┘              │
                           │                      │
               ③ RequestWorkerLease(B)            │
               (grant_or_reject=true)             │
                           ▼                      │
                    ┌──────────────┐              │
                    │   Raylet B   │              │
                    │ 有资源？      │              │
                    │ ├─ Yes → ④ Grant            │
                    │ └─ No  → ⑤ Reject ──────────┘
                    └──────────────┘

              ⑥ Client 收到 reject → 回 Raylet A 重新来一轮
```

**同一个 task 可以被反复 spillback（每次回到 A 重新选目标），没有次数上限。** 代码中有 TODO 但未实现（`normal_task_submitter.cc:440`）：

```cpp
// TODO(swang): Fail after some number of retries?
```

重试循环：`A → spillback B → B reject → 回 A → A spillback C → C grant`

### 4.5 推测性资源扣减

**Spillback 时 A 会推测性扣减对 B 的资源视图（`cluster_resource_manager.cc:211-228`）：**

```cpp
bool ClusterResourceManager::SubtractNodeAvailableResources(
    scheduling::NodeID node_id, const ResourceRequest &resource_request) {
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    return false;
  }
  NodeResources *resources = it->second.GetMutableLocalView();  // 标记 modified_ts
  resources->available -= resource_request.GetResourceSet();
  resources->available.RemoveNegative();  // 不会变负数
  return true;
}
```

**扣减的目的：** 防止短时间内大量 task 涌向同一个节点。扣减让该节点在后续调度打分中利用率升高，从而自然分散 task。

**所有调用 `SubtractNodeAvailableResources` 的地方：**

| 调用者 | 位置 | 场景 |
|--------|------|------|
| `ClusterLeaseManager::ScheduleOnNode` | `cluster_lease_manager.cc:444-446` | Spillback 到远端节点 |
| `LocalLeaseManager::Spillback` | `local_lease_manager.cc:689-691` | LocalLeaseManager 的 spillback 路径 |
| `BundlePackSchedulingPolicy::Schedule` | `bundle_scheduling_policy.cc:197,207` | Placement Group PACK 调度模拟 |
| `BundleSpreadSchedulingPolicy::Schedule` | `bundle_scheduling_policy.cc:269,278` | Placement Group SPREAD 调度模拟 |
| `GcsPlacementGroupScheduler` | `gcs_placement_group_scheduler.cc:647` | GCS 端 Placement Group 调度 |

### 4.6 扣减失败仍继续 Spillback 的原因

`AllocateRemoteTaskResources` 返回 false 时（本地视图已经显示 B 没资源了），spillback 仍然继续：

```cpp
if (!cluster_resource_scheduler_.AllocateRemoteTaskResources(...)) {
    RAY_LOG(DEBUG) << "...on a remote node that are no longer available";
}
// ↓ 无论成败，都发 spillback reply
reply->mutable_retry_at_raylet_address()->set_ip_address(B的地址);
```

**原因：**

1. **本地视图不是真相。** `AllocateRemoteTaskResources` 操作的是 A 对 B 的缓存视图。返回 false 只说明 A 的缓存认为 B 没资源了，但 B 的实际资源可能已经释放。

2. **真正的资源判断由 B 做。** B 收到请求后，用自己的真实资源状态决定 grant 或 reject。

3. **避免 task 卡死。** 如果因为缓存不够就中止 spillback，task 就无处可去（调度器已经决定本地也不行）。

4. **扣减只是 best-effort 的防雷暴机制。** 它存在是为了避免 A 在短时间内给 B 发太多 task，而不是作为调度决策的准入条件。

---

## 五、Hybrid 调度策略打分逻辑

### 5.1 打分使用的资源视图

**打分用的是 `nodes_`——即可能被 spillback 扣减过的脏视图。**

`HybridSchedulingPolicy` 持有 `nodes_` 的 const 引用（`hybrid_scheduling_policy.h:52-59,130`）：

```cpp
HybridSchedulingPolicy(scheduling::NodeID local_node_id,
                       const absl::flat_hash_map<scheduling::NodeID, Node> &nodes, ...)
    : ..., nodes_(nodes), ...
```

通过 `CompositeSchedulingPolicy` 传入（`composite_scheduling_policy.h:38-39`）：

```cpp
hybrid_policy_(local_node_id, cluster_resource_manager.GetResourceView(), ...)
//                             ↑ GetResourceView() 返回 nodes_
```

遍历时使用 `GetLocalView()`（`hybrid_scheduling_policy.cc:120-122`）：

```cpp
for (const auto &pair : nodes_) {
    const auto &node_id = pair.first;
    const auto &node_resources = pair.second.GetLocalView();  // 可能是脏的
```

**设计意图：** 使用脏视图是有意为之。Spillback 扣减让目标节点利用率升高、分数升高，后续调度自然分散到其他节点，避免短时间内 task 扎堆。

### 5.2 分数计算：Critical Resource Utilization

**核心公式（`cluster_resource_data.cc:62-83`）：**

```cpp
float NodeResources::CalculateCriticalResourceUtilization() const {
  float highest = 0;
  for (const auto &i : {CPU, MEM, OBJECT_STORE_MEM}) {  // 只看三种资源，不看 GPU
    const auto &cur_total = this->total.Get(ResourceID(i));
    if (cur_total == 0) {
      continue;  // 总量为 0 跳过，避免除零
    }
    auto cur_available = this->available.Get(ResourceID(i)).Double();
    float utilization = 1 - (cur_available / cur_total.Double());
    if (utilization > highest) {
      highest = utilization;
    }
  }
  return highest;
}
```

**公式：**

```
score = max(CPU利用率, MEM利用率, OBJ_STORE_MEM利用率)

其中: 利用率 = 1 - available / total = (total - available) / total
```

**关键点：GPU 不参与打分。** 即使 GPU 全部占满也不影响节点分数。

**举例：**

```
节点 B: total=(8 CPU, 32GB MEM, 10GB OBJ_STORE)
        available=(2 CPU, 28GB MEM, 8GB OBJ_STORE)

CPU 利用率:      1 - 2/8   = 0.75
MEM 利用率:      1 - 28/32 = 0.125
OBJ_STORE 利用率: 1 - 8/10  = 0.2

score = max(0.75, 0.125, 0.2) = 0.75  ← 瓶颈资源是 CPU
```

### 5.3 Spread Threshold 截断

打完原始分后有一步截断（`hybrid_scheduling_policy.cc:44-52`）：

```cpp
float ComputeNodeScoreImpl(const NodeResources &node_resources, float spread_threshold) {
  float critical_resource_utilization =
      node_resources.CalculateCriticalResourceUtilization();
  if (critical_resource_utilization < spread_threshold) {  // 默认 0.5
    critical_resource_utilization = 0;  // 低于阈值统一归零
  }
  return critical_resource_utilization;
}
```

`spread_threshold` 默认 0.5（`ray_config_def.h:178`）：

```cpp
RAY_CONFIG(float, scheduler_spread_threshold, 0.5)
```

**效果：利用率低于 50% 的节点分数全部为 0，视为"同样空闲"。**

```
节点 A: 利用率 30% → score=0      ← 低于阈值，归零
节点 B: 利用率 45% → score=0      ← 低于阈值，归零
节点 C: 利用率 60% → score=0.6    ← 超过阈值，保留
节点 D: 利用率 80% → score=0.8    ← 超过阈值，保留
```

这实现了**"先 pack 后 spread"**策略：空闲节点之间随机打散，繁忙节点被自然避开。

### 5.4 Feasible vs Available

调度分两层检查：

**Feasible 检查（`hybrid_scheduling_policy.cc:23-42`）—— 基于 `total`：**

```cpp
bool HybridSchedulingPolicy::IsNodeFeasible(
    const scheduling::NodeID &node_id, const NodeFilter &node_filter,
    const NodeResources &node_resources, const ResourceRequest &resource_request) const {
  if (!is_node_alive_(node_id)) return false;
  // GPU 过滤
  if (node_filter == NodeFilter::kNonGpu && node_resources.total.Has(ResourceID::GPU()))
    return false;
  return node_resources.IsFeasible(resource_request);
}
```

**`IsFeasible`（`cluster_resource_data.cc:106-112`）：**

```cpp
bool NodeResources::IsFeasible(const ResourceRequest &resource_request) const {
  if (!HasRequiredLabels(label_selector)) return false;
  return this->total >= resource_request.GetResourceSet();  // 用 total 判断
}
```

**Available 检查（`cluster_resource_data.cc:85-104`）—— 基于 `available`：**

```cpp
bool NodeResources::IsAvailable(const ResourceRequest &resource_request,
                                bool ignore_pull_manager_at_capacity) const {
  if (!ignore_pull_manager_at_capacity && resource_request.RequiresObjectStoreMemory()
      && object_pulls_queued) {
    return false;  // pull manager 满了
  }
  if (!HasRequiredLabels(label_selector)) return false;
  return this->available >= resource_request.GetResourceSet();  // 用 available 判断
}
```

**对比：**

| 检查 | 依据 | 含义 |
|------|------|------|
| Feasible | `total >= request` | 节点总容量能否装下这个 task |
| Available | `available >= request` | 节点当前空闲资源是否够用 |

节点选择优先级：**available_nodes > feasible_and_unavailable_nodes**。只有所有节点都不 available 时，才退到 feasible-but-unavailable 的节点中选（task 会排队等资源释放）。

### 5.5 Top-K 随机选择

**K 的计算（`hybrid_scheduling_policy.cc:155-157`）：**

```cpp
size_t num_candidate_nodes =
    std::max<int32_t>(schedule_top_k_absolute,                  // 默认 1
                      static_cast<int32_t>(nodes_.size() * scheduler_top_k_fraction));  // 默认 20%
```

默认配置（`ray_config_def.h:178-189`）：

```cpp
RAY_CONFIG(float, scheduler_top_k_fraction, 0.2);
RAY_CONFIG(int32_t, scheduler_top_k_absolute, 1);
```

10 个节点的集群：`K = max(1, 10*0.2) = 2`，从最低分的 2 个节点中随机选一个。

**`GetBestNode` 选择逻辑（`hybrid_scheduling_policy.cc:62-94`）：**

```cpp
scheduling::NodeID HybridSchedulingPolicy::GetBestNode(
    std::vector<std::pair<scheduling::NodeID, float>> &node_scores,
    size_t num_candidate_nodes,
    std::optional<scheduling::NodeID> preferred_node_id,
    float preferred_node_score) const {

  // 步骤 1: 按 NodeID 排序（确定性打破平局）
  std::sort(node_scores.begin(), node_scores.end(),
      [](auto &a, auto &b) { return a.first < b.first; });

  // 步骤 2: 按 score 稳定排序（低分在前，同分保持 ID 顺序）
  std::stable_sort(node_scores.begin(), node_scores.end(),
      [](auto &a, auto &b) { return a.second < b.second; });

  // 步骤 3: 如果本地节点的 score <= 最低分，直接选本地（不走随机）
  if (preferred_node_id.has_value()) {
    if (preferred_node_score <= node_scores.front().second) {
      return preferred_node_id.value();
    }
  }

  // 步骤 4: 从前 K 个节点中随机选一个
  size_t node_index = absl::Uniform<size_t>(
      bitgenref_, 0u, std::min(num_candidate_nodes, node_scores.size()));
  return node_scores[node_index].first;
}
```

### 5.6 本地节点的特殊待遇

本地节点**打分公式完全一样**，没有加分。但有两个特殊处理：

**特殊处理 1：分数相同时确定性选本地**

在 `GetBestNode` 中（`hybrid_scheduling_policy.cc:86-90`）：

```cpp
if (preferred_node_id.has_value()) {
    if (preferred_node_score <= node_scores.front().second) {
        return preferred_node_id.value();  // 本地 score <= 最优 score → 确定性选本地
    }
}
```

如果本地和远端都是 score=0，一定选本地，不走随机。

**特殊处理 2：忽略 pull manager 容量限制**

```cpp
// hybrid_scheduling_policy.cc:131-135
if (node_id == preferred_node_id) {
    ignore_pull_manager_at_capacity = true;  // 本地节点豁免 pull manager 限制
}
```

远端节点如果 `object_pulls_queued=true` 会被标记为不可用，但本地节点不会。因为后续可以通过 `SpillWaitingLeases` 再 spillback 出去。

### 5.7 GPU 节点避让

当 `scheduler_avoid_gpu_nodes=true`（默认）且 task 不需要 GPU 时（`hybrid_scheduling_policy.cc:183-221`）：

```cpp
scheduling::NodeID HybridSchedulingPolicy::Schedule(
    const ResourceRequest &resource_request, SchedulingOptions options) {
  if (!options.avoid_gpu_nodes_ || resource_request.Has(ResourceID::GPU())) {
    // 不需要避开 GPU，或者 task 需要 GPU → 正常调度
    return ScheduleImpl(..., NodeFilter::kAny, ...);
  }

  // 步骤 1: 只在非 GPU 节点中选（必须 available）
  auto best_node_id = ScheduleImpl(..., /*require_node_available*/ true,
                                    NodeFilter::kNonGpu, ...);
  if (!best_node_id.IsNil()) return best_node_id;

  // 步骤 2: 回退到所有节点（包括 GPU）
  return ScheduleImpl(..., NodeFilter::kAny, ...);
}
```

GPU 过滤在 `IsNodeFeasible` 中实现（`hybrid_scheduling_policy.cc:33-38`）：

```cpp
if (node_filter == NodeFilter::kNonGpu && has_gpu) {
    return false;  // 跳过 GPU 节点
}
```

### 5.8 完整打分流程图

```
遍历 nodes_ (脏视图)
    │
    ▼
┌─ IsNodeFeasible? ─────────────────────────────────────┐
│  ① 节点存活？                                          │
│  ② GPU 过滤（avoid_gpu_nodes=true 且 task 不要 GPU）   │
│  ③ Label 匹配？                                       │
│  ④ total >= request？                                  │
│  任一不满足 → 跳过                                     │
└────────────────────────────────────────────────────────┘
    │ 满足
    ▼
┌─ IsAvailable? ─────────────────────────────────────────┐
│  ① pull manager 队列满？（本地节点豁免）                  │
│  ② available >= request？                              │
└────────────────────────────────────────────────────────┘
    │
    ├─ Yes → 加入 available_nodes
    └─ No  → 加入 feasible_and_unavailable_nodes
    │
    ▼
计算 score = max(CPU利用率, MEM利用率, OBJ_STORE利用率)
    │
    ▼
score < spread_threshold(0.5) ? → 截为 0
    │
    ▼
┌─ GetBestNode ──────────────────────────────────────────┐
│  优先从 available_nodes 选（没有则从 unavailable 选）    │
│  ① 按 score 排序（低分优先）                             │
│  ② 本地 score <= 最低分？→ 确定性选本地                  │
│  ③ 否则从 top-K 低分节点中随机选                         │
│     K = max(1, 节点数 x 20%)                           │
└────────────────────────────────────────────────────────┘
    │
    ├── 选中本地 → 本地调度（进入 LocalLeaseManager）
    └── 选中远端 → Spillback（推测性扣减 + 回复 retry_at）
```

**打分举例（10 节点集群，task 需要 1 CPU）：**

```
节点      total    available  利用率    score   备注
A(本地)   8 CPU    6 CPU      25%      0       低于 0.5 归零
B         8 CPU    2 CPU      75%      0.75    超过阈值
C         8 CPU    5 CPU      37%      0       低于 0.5 归零
D(GPU)    16 CPU   16 CPU     0%       0       avoid_gpu=true → 跳过
E         8 CPU    4 CPU      50%      0.5     等于阈值，保留

排序后: A(0), C(0), E(0.5), B(0.75)  (D 被过滤)
K = max(1, 4*0.2) = 1

本地 A 的 score=0 <= 最低分 0 → 确定性选 A → 本地调度
```

```
节点      total    available  利用率    score   备注
A(本地)   8 CPU    1 CPU      87%      0.87    高利用率
B         8 CPU    6 CPU      25%      0       低于阈值
C         8 CPU    5 CPU      37%      0       低于阈值

排序后: B(0), C(0), A(0.87)
K = max(1, 3*0.2) = 1

本地 A 的 score=0.87 > 最低分 0 → 不选本地
从 top-1 中选 → B → Spillback 到 B
```

---

## 六、资源视图过时的安全性分析

| 场景 | 后果 | 是否有正确性问题 |
|------|------|----------------|
| A 低估 B 的资源（悲观过时） | A 不选 B，选其他节点或本地排队 | 无。只是调度不够优化 |
| A 高估 B 的资源（乐观过时） | A spillback 到 B，B reject，client 重试 | 无。有 reject 兜底 |
| A 对 B 做了推测性扣减，但 task 被 B reject | A 的视图仍然是"脏"的 | 无。定时器会在 N 秒后重置 |

**安全保障机制：**

1. **`grant_or_reject` 协议：** spillback 最多一跳，不会无限链式传递

2. **`RemoveNegative()`（`cluster_resource_manager.cc:221`）：** 推测性扣减不会让视图变成负数

3. **`AllocateRemoteTaskResources` 软失败：** 扣减失败不阻止 spillback，因为远端才是资源判断的权威

4. **定时器兜底：** `ResetRemoteNodeView` 保证脏视图不会永远存在

5. **无限重试：** client 被 reject 后会回到本地 raylet 重新调度，此时资源视图可能已被 syncer 更新或定时器重置

---

## 七、系统配置参数调优建议

### 7.1 参数关注点

| 参数 | 主要关注点 |
|------|-----------|
| `health_check_period_ms=10000` | 故障检测延迟从 15s → 50s，确认业务能容忍 |
| `ray_syncer_message_refresh_interval_ms=30000` | 脏视图最长存活 30s，在节点资源频繁变化的集群中影响较小 |
| `gcs_server_rpc_server_thread_num=64` | 确认 GCS 节点有足够的 CPU 核心，避免过多上下文切换 |
| `gcs_resource_broadcast_max_batch_size=100` | 不需要调整 gRPC message size（远低于 512MB 限制） |
| `scheduler_avoid_gpu_nodes=false` | 纯 CPU 任务也会被调度到 GPU 节点，确认这是期望行为 |

### 7.2 `ray_syncer_message_refresh_interval_ms=30000` 的实际影响

```
时刻 T=0:    A spillback task 到 B，本地扣减 B 的视图 (4 CPU → 3 CPU)
时刻 T=0.1:  B reject（B 实际没空闲资源）
时刻 T=0.2:  Client 回到 A 重新调度

此后 A 对 B 的视图仍显示 3 CPU（脏的）：
- 路径 A: B 的资源变了（其他 task 结束）→ syncer 推新 version，几秒内修正
- 路径 B: B 的资源没变（version 不递增）→ 需等定时器 30s 后重置
```

**影响程度取决于：**
- 集群规模大、节点多 → 影响小（调度有很多其他节点可选）
- 任务频繁 spillback 且 reject → 影响累积（同一节点被反复扣减）
- 节点资源变化频繁 → 影响小（syncer 正常推送会覆盖脏视图）

### 7.3 参数间的约束关系

```
约束 1: ray_syncer_message_refresh_interval_ms >> raylet_report_resources_period_milliseconds
        (否则 Raylet 会频繁触发不必要的"刷新旧视图"操作)

约束 2: gcs_resource_broadcast_max_batch_size > 1 时，batch_delay_ms 才生效
        (batch_size=1 时批处理禁用)

约束 3: gcs_resource_broadcast_max_batch_delay_ms 应 < raylet_report_resources_period_milliseconds
        (否则广播延迟超过一次上报周期)

约束 4: raylet_report_resources_period_milliseconds x 集群节点数 = GCS RaySyncer 消息入队速率
        需配合 batch_size/batch_delay 确保 RaySyncer 线程 CPU 不被打满
```

### 7.4 按集群规模推荐配置

| 规模 | `report_period` | `batch_size` | `batch_delay` | `refresh_interval` |
|------|----------------|-------------|--------------|-------------------|
| 小集群 (<20 节点) | 100ms (默认) | 1 (默认) | 0 (默认) | 3000ms (默认) |
| 中集群 (20-100) | 200-500ms | 10-20 | 5-10ms | 5000-10000ms |
| 大集群 (100+) | 500-1000ms | 50-100 | 10-20ms | 10000-20000ms |

**说明：**
- `report_period` = `raylet_report_resources_period_milliseconds`，控制 Raylet 主动上报频率
- `batch_size` = `gcs_resource_broadcast_max_batch_size`，控制 GCS 广播批量大小
- `batch_delay` = `gcs_resource_broadcast_max_batch_delay_ms`，控制批量等待延迟
- `refresh_interval` = `ray_syncer_message_refresh_interval_ms`，控制脏视图重置间隔

**大集群核心矛盾：** N 个 Raylet 每 `report_period` 上报一次，GCS 收到后广播给 N-1 个 Raylet。消息总量为 O(N^2/report_period)。当 N=100, report_period=100ms 时，GCS 每秒处理 1000 条入站消息，并产生约 99000 条出站消息。批处理和降低上报频率是缓解 GCS RaySyncer 线程 CPU 瓶颈的两个核心手段。

### 7.5 当前配置组合评估

当前设置的参数组合：

```
ray_syncer_message_refresh_interval_ms = 30000  (10x 默认)
gcs_resource_broadcast_max_batch_size = 100     (100x 默认)
gcs_resource_broadcast_max_batch_delay_ms = 500 (从 0 调大)
health_check_period_ms = 10000                  (3.3x 默认)
gcs_server_rpc_server_thread_num = 64           (固定值)
scheduler_avoid_gpu_nodes = false               (关闭避让)
event_stats_print_interval_ms = 180000          (3x 默认)
```

**整体评估：** 这组参数明显是为**大规模集群**优化的，核心策略是：
1. **降低 GCS 负载**：batch_size=100 + batch_delay=500ms 大幅减少 gRPC 写入次数
2. **降低健康检查开销**：health_check_period=10s 减少探测 RPC
3. **放宽视图刷新**：refresh_interval=30s 减少不必要的视图重置
4. **提高并发能力**：rpc_threads=64 提升 GCS gRPC 吞吐
5. **充分利用资源**：关闭 GPU 避让，让 CPU 任务也能用 GPU 节点的 CPU

**需要注意的风险点：**
- 故障检测延迟从 15s 增加到 50s（`health_check_period_ms * failure_threshold = 10s * 5`）
- 脏视图在极端情况下可能存活 30s（但有 syncer 主路径兜底）
- 资源更新广播延迟最多 500ms（对大多数场景可接受）
