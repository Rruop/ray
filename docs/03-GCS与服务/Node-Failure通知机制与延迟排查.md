# Node Failure 通知机制与延迟排查

本文档基于实际线上问题排查过程，详细分析 Ray 中节点失败（Node Failure）的检测、传播机制，以及核心 worker 与 raylet 感知节点失败时出现延迟的根本原因。

---

## 一、问题现象

### 现象 1：CoreWorker 收到节点失败通知

CoreWorker 进程日志中观察到：

```
[2026-06-11 09:56:09,354 I 20139 20359] accessor.cc:436: Received address and liveness notification for node, IsAlive = 1 node_id=59bb10dcec98927e2fcecbe99660352f992ea46f3391efc5caf16c1d
[2026-06-11 09:56:09,355 I 20139 20359] normal_task_submitter.cc:840: Number of alive nodes:1
[2026-06-11 09:56:09,355 W 20139 20359] normal_task_submitter.cc:834: Node change state to DEAD but num_alive_node is 0.
[2026-06-11 09:56:09,356 I 20139 20359] accessor.cc:436: Received address and liveness notification for node, IsAlive = 0 node_id=ca73da20fa56f584e6a0ebcda29624d8c108f12c3832d05412f0ebeb
[2026-06-11 09:56:09,356 I 20139 20359] core_worker.cc:751: Node failure. All objects pinned on that node will be lost if object reconstruction is not enabled. node_id=ca73da20fa56f584e6a0ebcda29624d8c108f12c3832d05412f0ebeb
```

### 现象 2：Worker 节点 raylet 收到通知（同一事件，不同节点）

```
[2026-06-11 09:30:13,819 I 90 90] (raylet) accessor.cc:436: Received address and liveness notification for node, IsAlive = 0 node_id=ca73da20fa56f584e6a0ebcda29624d8c108f12c3832d05412f0ebeb
```

### 现象 3：Head 节点 GCS 与 raylet 时间戳

```
[2026-06-11 17:07:37,108 I 47 47] (gcs_server) gcs_job_manager.cc:489: Node is dead, marking all jobs with drivers on this node as finished. node_id=ca73da20fa56f584e6a0ebcda29624d8c108f12c3832d05412f0ebeb
[2026-06-11 17:07:37,125 I 933 933] (raylet) accessor.cc:436: Received address and liveness notification for node, IsAlive = 0 node_id=ca73da20fa56f584e6a0ebcda29624d8c108f12c3832d05412f0ebeb
```

### 关键问题

1. 同一节点（`ca73da...`）的 DEAD 事件，head 节点的 GCS 与 raylet 间隔仅 17ms，但 worker 节点的 raylet 与 worker 进程之间间隔约 26 分钟（同时区下），与 head GCS 间隔约 23 分钟（统一时区后）。
2. 出现 WARNING `Node change state to DEAD but num_alive_node is 0`。
3. 为什么不同节点感知 node failure 的延迟差异如此之大？

---

## 二、Ray Node Failure 检测与传播机制

### 2.1 整体架构

Ray 采用**集中式**的节点失败检测模型 —— GCS 是 node liveness 的唯一权威源。**没有 node-to-node 直接监控**，所有感知通过 GCS pub/sub 完成。

### 2.2 完整事件流

```
[Raylet 进程崩溃/卡死]
        ↓
GcsHealthCheckManager (GCS 周期性 gRPC 健康检查)
        ↓ 连续 health_check_failure_threshold(=5) 次失败
GcsHealthCheckManager::FailNode()              [gcs/gcs_health_check_manager.cc:78]
        ↓ on_node_death_callback_
GcsNodeManager::OnNodeFailure()                [gcs/gcs_node_manager.cc:680]
        ↓ InternalOnNodeFailure()
        ↓   1. InferDeathInfo() -> UNEXPECTED_TERMINATION
        ↓   2. RemoveNodeFromCache() -> alive→dead
        ↓   3. 触发内部 listener (ActorManager/ResourceManager 等)
        ↓   4. 持久化到 NodeTable
        ↓
GcsNodeManager::PublishNodeInfoToPubsub()      [gcs/gcs_node_manager.cc:766]
        ↓ 发布到两个 channel:
        ↓  - GCS_NODE_INFO_CHANNEL
        ↓  - GCS_NODE_ADDRESS_AND_LIVENESS_CHANNEL
        ↓
GcsPublisher::PublishNodeAddressAndLiveness()  [pubsub/gcs_publisher.cc:55]
        ↓ EntityState::Publish() 将消息放入每个 subscriber 的 mailbox
        ↓ PublishIfPossible() 通过 long-polling reply 推送
        ↓
        ├──→ CoreWorker 端 GcsSubscriber
        │        ↓ NodeInfoAccessor::HandleNotification()  [accessor.cc:416]
        │        ↓ 回调 CoreWorker::on_node_change         [core_worker.cc:748]
        │        ↓ RAY_LOG "Node failure. All objects pinned..."
        │        ↓ ResetObjectsOnRemovedNode() + Disconnect RPC pools
        │
        └──→ Raylet 端 GcsSubscriber
                ↓ NodeInfoAccessor::HandleNotification()
                ↓ 回调 NodeManager::NodeRemoved()          [node_manager.cc:919]
                ↓ CancelLeases / KillWorkers / RemoveResources
```

### 2.3 GCS 健康检查参数（默认值）

定义于 `src/ray/common/ray_config_def.h`：

| 参数 | 默认值 | 含义 |
|------|--------|------|
| `health_check_initial_delay_ms` | 5000 (5s) | 首次健康检查的延迟 |
| `health_check_period_ms` | 3000 (3s) | 两次健康检查的间隔 |
| `health_check_timeout_ms` | 10000 (10s) | 单次健康检查的超时 |
| `health_check_failure_threshold` | 5 | 连续失败几次后宣告死亡 |
| `subscriber_timeout_ms` | 300000 (300s) | subscriber 不活跃多久后被 GCS 清理 |

故障检测时间窗口：约 `5 * 3s = 15s`（理想情况），叠加超时可达 `5 * (3s + 10s) = 65s`。

### 2.4 订阅入口

**CoreWorker 端**（`src/ray/core_worker/core_worker.cc:738`）：

```cpp
void CoreWorker::SubscribeToNodeChanges() {
  std::call_once(subscribe_to_node_changes_flag_, [this]() {
    auto on_node_change = [...](const NodeID &node_id,
                                const rpc::GcsNodeAddressAndLiveness &data) {
      if (data.state() == rpc::GcsNodeInfo::DEAD) {
        RAY_LOG(INFO).WithField(node_id)
            << "Node failure. All objects pinned on that node will be lost if object "
               "reconstruction is not enabled.";
        reference_counter->ResetObjectsOnRemovedNode(node_id);
        raylet_client_pool->Disconnect(node_id);
        core_worker_client_pool->Disconnect(node_id);
      }
      // ... 更新 rate limiter 计数
    };

    gcs_client_->Nodes().AsyncSubscribeToNodeAddressAndLivenessChange(...);
  });
}
```

**Raylet 端**（`src/ray/raylet/node_manager.cc:334`）：

```cpp
void NodeManager::RegisterGcs() {
  auto on_node_change = [this](const NodeID &node_id,
                               const rpc::GcsNodeAddressAndLiveness &data) {
    if (data.state() == GcsNodeInfo::ALIVE) {
      NodeAdded(data);
    } else {
      RAY_CHECK(data.state() == GcsNodeInfo::DEAD);
      NodeRemoved(node_id);
    }
  };
  gcs_client_.Nodes().AsyncSubscribeToNodeAddressAndLivenessChange(...);
}
```

---

## 三、Long-Polling 机制详解

Ray 的 GCS pub/sub 不是传统的"publisher 主动推送"，而是**基于 gRPC 的长轮询**。

### 3.1 工作流程

```
Subscriber                          GCS (Publisher)
    |                                    |
    |--- PubsubLongPollingRequest ------>|   (1) 发起请求，挂起等待
    |                                    |
    |         ... 无消息时连接保持 ...      |
    |                                    |
    |         [有事件发布时]               |
    |                                    |  (2) 将消息填入 reply
    |<-- PubsubLongPollingReply ---------|  (3) 立即回复
    |                                    |
    |--- PubsubLongPollingRequest ------>|  (4) 立即发起新请求
    |                                    |
```

### 3.2 关键特性

- **每个 subscriber 与 GCS 之间只有 1 条 long-polling 连接**，所有 channel（actor、node、worker、resource 等）共用
- **无消息时连接挂起**，不周期性轮询，避免无效流量
- **有消息时立即回复**，实时性接近毫秒级
- 回复后 subscriber **立即发起下一个请求**，保持连接持久

### 3.3 代码路径

**Subscriber 侧建立连接**（`src/ray/pubsub/subscriber.cc:297`）：

```cpp
void Subscriber::MakeLongPollingPubsubConnection(const rpc::Address &publisher_address) {
  rpc::PubsubLongPollingRequest long_polling_request;
  long_polling_request.set_subscriber_id(subscriber_id_.Binary());
  long_polling_request.set_publisher_id(last_publisher_id.Binary());
  long_polling_request.set_max_processed_sequence_id(max_processed_sequence_id);
  subscriber_client->PubsubLongPolling(
      std::move(long_polling_request),
      [this, publisher_address](const Status &status,
                                rpc::PubsubLongPollingReply &&reply) {
        absl::MutexLock lock(&mutex_);
        HandleLongPollingResponse(publisher_address, status, std::move(reply));
      });
}
```

**Subscriber 侧处理回复并自动重连**（`src/ray/pubsub/subscriber.cc:316`）：

```cpp
void Subscriber::HandleLongPollingResponse(...) {
  if (!status.ok()) {
    // RPC 失败：触发 failure callback
    for (const auto &channel_it : channels_) {
      channel_it.second->HandlePublisherFailure(publisher_address, status);
    }
    commands_.erase(publisher_id);
  } else {
    // 处理消息
    for (int i = 0; i < reply.pub_messages_size(); i++) {
      Channel(channel_type)->HandlePublishedMessage(...);
    }
  }

  // 注意：这段逻辑在 if/else 外面 —— 无论成功失败，
  // 只要还有订阅，就立即发起下一个 long-polling 请求
  if (SubscriptionExists(publisher_id)) {
    MakeLongPollingPubsubConnection(publisher_address);
  }
}
```

**重要纠正**：long-polling RPC 失败后 subscriber **会自动重连**，并不会陷入"断了就断了"的状态。底层 gRPC client 还会在瞬时网络故障时自动重试。

### 3.4 GCS 侧 subscriber 超时清理

Publisher 每 `subscriber_timeout_ms`（默认 300s）执行一次 `CheckDeadSubscribers`（`src/ray/pubsub/publisher.cc:476`）：

```cpp
void Publisher::CheckDeadSubscribers() {
  for (const auto &it : subscribers_) {
    const auto &subscriber = it.second;
    if (subscriber->IsActive()) continue;  // 最近 300s 内有活动则跳过
    
    if (subscriber->ConnectionExists()) {
      // 有挂起的 long-polling 连接，发空回复 flush 掉
      subscriber->PublishIfPossible(/*force_noop*/ true);
    } else {
      // 已无连接，标记为 dead，后续删除其元数据
      dead_subscribers.push_back(it.first);
    }
  }
  for (const auto &subscriber_id : dead_subscribers) {
    UnregisterSubscriberInternal(subscriber_id);
  }
}
```

`IsActive()` 判断条件：`get_time_ms() - last_connection_update_time_ms < connection_timeout_ms`。

### 3.5 Resubscribe 机制

GCS 重启时，会主动通知所有 raylet `NotifyGCSRestart`（`src/ray/raylet/node_manager.cc:1062`），触发：

```cpp
void NodeManager::HandleNotifyGCSRestart(...) {
  RAY_LOG(INFO) << "The GCS has restarted. Resubscribing to pubsub and notifying local workers.";
  gcs_client_.AsyncResubscribe();  // raylet 自己重新订阅
  for (auto &worker : worker_pool_.GetAllRegisteredWorkers(...)) {
    worker->AsyncNotifyGCSRestart();  // 通知 worker 重新订阅
  }
}
```

`AsyncResubscribe` 链路（`src/ray/gcs_rpc_client/accessor.cc:461`）：
1. 调用 `SubscribeAllNodeAddressAndLiveness` 重新订阅
2. 完成后调用 `AsyncGetAllNodeAddressAndLiveness` 拉取**全量**节点数据
3. 通过 `HandleNotification` 处理每个节点 → 触发回调（如 DEAD 状态）

**这就是为什么 worker 在 long-polling 长时间断开后，仍可能通过"重新订阅 + 全量拉取"发现 DEAD 节点 —— 但延迟较大。**

---

## 三点五、Node 与 Worker 接收 Node Failure 通知的方式与开销

### 3.5.1 是否独立订阅？

**是，每个 core_worker 进程和每个 raylet 进程都独立订阅 GCS pub/sub。** 但它们共享同一套 long-polling 机制：

- 每个 core_worker 的 `GcsSubscriber` 与 GCS 之间维护 **1 条** long-polling 连接
- 每个 raylet 的 `GcsSubscriber` 同样维护 **1 条** long-polling 连接
- 这条连接上**复用所有 channel** 的消息（actor、node、worker、resource 等），不是 per-channel 一条

订阅时 `key_id = std::nullopt`（`gcs_subscriber.cc:128`），表示订阅所有节点状态变更（不关心具体 node_id）。

### 3.5.2 一个节点 fail 后，需要向所有 subscriber 推送吗？

**是，需要向所有订阅了该 channel 的 node 和 worker 推送，但开销可控。**

#### 推送路径

`GCS_NODE_ADDRESS_AND_LIVENESS_CHANNEL` 发布时（`publisher.cc:421`），走 `subscribers_to_all_` 路径（`publisher.cc:123`），因为 core_worker 和 raylet 都用 `key_id = nullopt` 订阅"所有节点"。

**`EntityState::Publish()`**（`publisher.cc:28`）的核心逻辑：

```cpp
for (auto &[id, subscriber] : subscribers_) {
    subscriber->QueueMessage(msg);  // 放入每个 subscriber 的 mailbox
}
```

消息被放入**每个 subscriber 的 mailbox**，等待该 subscriber 的下一次 long-polling reply 时批量推送。

#### 为什么开销可控

| 关键设计 | 说明 |
|---------|------|
| **不是逐一发 RPC** | GCS 不向每个 subscriber 主动发 RPC，而是放入 mailbox，等 subscriber 的 long-polling 请求来时顺带返回 |
| **长轮询复用** | 每个 subscriber 只有 **1 条** long-polling 连接，node failure 消息和其他消息（actor、resource）共用 |
| **批量合并** | `PublishIfPossible()`（`publisher.cc:306`）会将 mailbox 中多条消息批量填入同一个 long-polling reply |
| **事件低频** | node failure 是低频事件，不会持续产生消息洪峰 |
| **shared_ptr 引用** | 同一条消息在所有 mailbox 中是 `shared_ptr` 引用同一份数据，内存开销小 |

### 3.5.3 大集群下的开销估算

假设集群 N 个节点，每节点 M 个 worker：

| 项目 | 数量 | 说明 |
|------|------|------|
| GCS 健康检查连接 | N | GCS → 每个 raylet，1 条 gRPC |
| Long-polling 连接 | N × (1 + M) | 每个 raylet + 每个 worker 一条 |
| 节点宕机时的推送消息 | N × (1 + M) | 每个 subscriber 的 mailbox 收到 1 条 |
| Mailbox 内存（单事件） | 几 KB × N × (1 + M) | shared_ptr 共享同一份消息 |

**对比全连接模型**：如果是 node-to-node 直接感知（O(N²) 健康检查），需要 N² 条连接。Ray 的集中式 GCS 模型将其降到 O(N) 健康检查 + O(N×M) 通知推送，且通知是低频的。

### 3.5.4 潜在瓶颈

大规模集群（数千节点 × 数万 worker）下：

1. **GCS pubsub 单线程瓶颈**：`Publisher::Publish()` 持有全局 `mutex_`，对每个 subscriber 调 `QueueMessage`。如果 subscriber 数量过大且事件频繁，可能成为热点。
2. **Reply 序列化开销**：long-polling reply 中批量序列化 protobuf，受 `max_grpc_message_size` 限制；超过则分批多次回复。
3. **Mailbox 累积**：subscriber 长时间未 ack（无 long-polling 来取），mailbox 累积；超过 `publisher_entity_buffer_max_bytes` 会丢弃旧消息（但 node liveness channel 配置为 `-1`，不丢弃，可能导致内存膨胀）。
4. **Subscriber 清理**：`subscriber_timeout_ms`（300s）后清理元数据，期间累积的消息一并丢失。

### 3.5.5 结论

- **机制开销不大**：1 条 long-polling 连接复用所有 channel，按需推送，低频事件
- **不是 per-event-per-RPC**：消息进 mailbox，跟随长轮询 reply 批量发送
- **真正风险在 long-polling 断连**：见后续章节 4.1

---

## 四、问题排查与原因分析

### 4.1 现象解读

#### 现象 3 分析（head 节点 17ms 延迟）

GCS 17:07:37,108 → raylet 17:07:37,125，**仅相差 17ms**，完全正常。

包含的过程：
1. GCS `OnNodeFailure` 内部 listener（包括 `GcsJobManager::OnNodeDead` 打印的日志）
2. 持久化 `NodeTable.Put`
3. `PublishNodeInfoToPubsub`
4. gRPC 网络传输（同机或同 K8S Pod）
5. raylet 处理 long-polling reply

这证明 **GCS 发布侧机制本身没有问题**。

#### 现象 1 分析（WARNING：num_alive_node is 0）

来自 `src/ray/core_worker/task_submission/normal_task_submitter.cc:861`：

```cpp
void ClusterSizeBasedLeaseRequestRateLimiter::OnNodeChanges(
    const rpc::GcsNodeAddressAndLiveness &data) {
  if (data.state() == rpc::GcsNodeInfo::DEAD) {
    if (num_alive_nodes_ != 0) {
      num_alive_nodes_--;
    } else {
      RAY_LOG(WARNING) << "Node" << data.node_manager_address()
                       << " change state to DEAD but num_alive_node is 0.";
    }
  } else {
    num_alive_nodes_++;
  }
}
```

**根本原因**：`num_alive_nodes_` 初始化为 0，且只在 `OnNodeChanges` 回调中增减。但全量初始化时（`AsyncGetAllNodeAddressAndLiveness`）**不会触发 `OnNodeChanges`**，因为 `is_notif_new` 判断为 false（`accessor.cc:423`：`is_notif_new = was_alive && !is_alive`）。

这意味着初始已存在的 ALIVE 节点没被计数，后续这些节点变 DEAD 时就出现 `num_alive_nodes_` 已经是 0 的情况。**这是一个计数偏差 bug**，不影响功能，但日志显示集群可能只剩自身节点。

#### 现象 2 分析（worker raylet 23 分钟延迟，关键问题）

用户明确提到：worker raylet 与 head GCS 之间约 23 分钟延迟（统一时区后）。

**首先排除**的解释：
- ❌ 时区差 8 小时：实际差 23 分钟，对不上
- ❌ 同一事件 raylet 早于 GCS：因果上不可能
- ❌ GCS 发布慢：head raylet 仅 17ms 收到，证明发布快

**最可能的根因**（按可能性排序）：

##### (1) 机器间时钟漂移

如果 worker 节点缺少 NTP 同步或同步异常，两台机器系统时钟差几十分钟很常见。

**这是最简单的解释**，但无法仅从代码分析得出。

**验证方法**：

```bash
ssh head_node "date -u"
ssh worker_node "date -u"
# 或
ssh worker_node "chronyc tracking" / "timedatectl"
```

##### (2) Worker raylet 的 long-polling 真的断开过

可能场景：
- GCS failover/重启
- worker 节点本身网络抖动、抓包工具拦截、防火墙规则变更
- raylet 进程暂时卡死（如 GC、IO 阻塞）

如果断开时间超过 `subscriber_timeout_ms` (300s)，GCS 侧会清理该 subscriber 的元数据，期间 mailbox 中的消息丢失。最终通过 `AsyncResubscribe` + 全量拉取发现节点已 DEAD。

**验证方法**：在 worker raylet 日志中搜索：

| 关键词 | 含义 |
|--------|------|
| `"A worker is dead. subscription_failure_callback"` | long-polling RPC 失败 |
| `"Subscription to NodeAddressAndLiveness channel failed"` | 订阅失败 |
| `"Resubscribing to GCS tables"` | 触发 AsyncResubscribe |
| `"The GCS has restarted"` | GCS 重启事件 |
| `"Reestablishing subscription for node info"` | 重连订阅 |
| `"Long polling request has been replied"` (DEBUG) | long-polling 正常活动 |

##### (3) GCS 处理某个 subscriber 时阻塞

如果 GCS pubsub 模块在处理某个 subscriber 时遇到阻塞（如 mailbox 累积过多消息、某个 subscriber 的网络回包慢），可能影响其他 subscriber 的推送。但这种情况在 head raylet 17ms 收到的对照下不太成立。

### 4.2 推荐排查命令

```bash
# 1. 时钟对比
ssh head_node "date -u"
ssh worker_node "date -u"

# 2. 看 worker raylet 在 17:00 ~ 17:30 区间的关键事件
grep -E "Resubscrib|GCS has restarted|Subscription.*failed|Long polling" worker_raylet.log

# 3. 看 GCS 是否有 failover 记录
grep -E "GCS server is started|started GCS|failover" gcs_server.log

# 4. 看 worker 节点网络
ssh worker_node "dmesg | grep -E 'eth|net' | tail -30"

# 5. 看 raylet 进程是否有长时间无活动
grep "raylet" worker_raylet.log | awk '{print $1, $2}' | uniq -c | tail
```

---

## 五、设计层面的反思与建议

### 5.1 当前设计的取舍

Ray 选择 long-polling + GCS 集中式发布，有明确权衡：

**优点**：
- 简单：每个进程对 GCS 只有 1 条连接，O(N) 而非 O(N²)
- 实时性好：正常情况毫秒级延迟
- 资源开销小：node failure 是低频事件，连接挂起不消耗 CPU

**潜在问题**：
- 长时间断连后**消息会丢失**（subscriber_timeout 300s 后清理）
- Resubscribe 后需要全量拉取，时延依赖 GCS 响应
- 没有为 long-polling 失败设计独立的重连退避策略，全部依赖底层 gRPC retry

### 5.2 改进方向

1. **更主动的健康自检**：subscriber 侧周期性检查上次收到 long-polling reply 的时间，超阈值主动重连
2. **持久化 mailbox**：GCS 侧关键 channel 消息持久化，避免清理 subscriber 时丢失
3. **节点状态版本号**：subscriber 重连后基于版本号增量同步，而非全量拉取
4. **修复 `num_alive_nodes_` 计数 bug**：初始化时遍历全量数据，正确统计 ALIVE 节点数

---

## 六、关键代码位置速查

| 功能 | 文件 | 行号 |
|------|------|------|
| GCS 健康检查 | `src/ray/gcs/gcs_health_check_manager.cc` | 78 (`FailNode`), 143 (`StartHealthCheck`) |
| 节点失败处理 | `src/ray/gcs/gcs_node_manager.cc` | 680 (`OnNodeFailure`), 766 (`PublishNodeInfoToPubsub`) |
| 节点信息发布 | `src/ray/pubsub/gcs_publisher.cc` | 55 (`PublishNodeAddressAndLiveness`) |
| Publisher 核心 | `src/ray/pubsub/publisher.cc` | 28 (`EntityState::Publish`), 306 (`PublishIfPossible`), 476 (`CheckDeadSubscribers`) |
| Subscriber 核心 | `src/ray/pubsub/subscriber.cc` | 297 (`MakeLongPollingPubsubConnection`), 316 (`HandleLongPollingResponse`) |
| GcsSubscriber 包装 | `src/ray/pubsub/gcs_subscriber.cc` | 112 (`SubscribeAllNodeAddressAndLiveness`) |
| NodeInfoAccessor | `src/ray/gcs_rpc_client/accessor.cc` | 301 (`AsyncSubscribeToNodeAddressAndLivenessChange`), 416 (`HandleNotification`), 461 (`AsyncResubscribe`) |
| CoreWorker 订阅 | `src/ray/core_worker/core_worker.cc` | 738 (`SubscribeToNodeChanges`), 751 (`Node failure` 日志) |
| Raylet 订阅 | `src/ray/raylet/node_manager.cc` | 334 (`RegisterGcs`), 919 (`NodeRemoved`), 1062 (`HandleNotifyGCSRestart`) |
| 计数 bug | `src/ray/core_worker/task_submission/normal_task_submitter.cc` | 854, 861 (`ClusterSizeBasedLeaseRequestRateLimiter`) |

---

## 七、配置参数速查

| 参数 | 默认值 | 文件 |
|------|--------|------|
| `health_check_initial_delay_ms` | 5000 | `src/ray/common/ray_config_def.h` |
| `health_check_period_ms` | 3000 | 同上 |
| `health_check_timeout_ms` | 10000 | 同上 |
| `health_check_failure_threshold` | 5 | 同上 |
| `subscriber_timeout_ms` | 300000 | 同上 |
| `publish_batch_size` | （见配置） | 同上 |
| `max_grpc_message_size` | （见配置） | 同上 |
