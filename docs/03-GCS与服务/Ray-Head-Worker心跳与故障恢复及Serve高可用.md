# Ray Head/Worker 心跳、故障恢复与 Ray Serve 高可用实践

> 适用版本：Ray 2.55 / kray  
> 代码基线：`release-2.55-analyze`  
> 范围：Head/Worker 心跳、节点死亡检测、Head OOM 重启恢复、GCS Fault Tolerance、Worker 断联恢复、Head 换节点/不换节点、Ray Serve 高可用配置与验证。  
> 远端同步：<https://docs.corp.kuaishou.com/d/home/fcADjCV4Xu6nylA6yzwA93Dca>

---

## 目录

- [1. 结论速览](#1-结论速览)
- [2. 角色、连接与术语](#2-角色连接与术语)
- [3. 心跳机制](#3-心跳机制)
- [4. 节点死亡判定完整链路](#4-节点死亡判定完整链路)
- [5. Head OOM 重启恢复机制](#5-head-oom-重启恢复机制)
- [6. Head 重启场景矩阵：Pod 不变 vs Pod 变化](#6-head-重启场景矩阵pod-不变-vs-pod-变化)
- [7. Worker 断联与恢复机制](#7-worker-断联与恢复机制)
- [8. Worker 独立服务窗口：Head 挂掉后还能撑多久](#8-worker-独立服务窗口head-挂掉后还能撑多久)
- [9. Ray Serve 高可用配置](#9-ray-serve-高可用配置)
- [10. 监控、日志与验证](#10-监控日志与验证)
- [11. 常见误区与修正](#11-常见误区与修正)
- [12. 代码索引](#12-代码索引)
- [13. 参考资料](#13-参考资料)

---

## 1. 结论速览

1. **Ray 2.55 已经没有传统的 `ReportHeartbeat` RPC**。节点存活判定由三层组成：
   - RaySyncer 双向流（主信号，资源变更时发送）
   - GCS 主动拉取 HealthCheck（兜底，真正的周期性探活）
   - Raylet 反向自检 GCS 是否还认为自己活着（防脑裂/防 GCS 状态丢失）
2. **Head 和 Worker 在心跳机制上完全对称**，唯一区别是 Head 上多跑了一个 GCS 进程。
3. **GCS 判死一个节点需要连续 5 次 HealthCheck 失败**，典型 15~20 秒，最坏 65~70 秒。
4. **Head OOM 会触发整个 Head 容器重启**，因为 `ray start --block` 看门狗把 `-9` 视为意外退出并 `os._exit(1)`。
5. **只有外部 Redis 模式才能恢复 GCS 状态**。内存模式下新 ClusterID 会导致旧 Raylet 被鉴权拒绝，并在 60 秒自检后自杀。
6. **Head 不换节点且端口不变**：外部 Redis + 稳定端口 → 可以原地恢复。
7. **Head 换节点/Pod**：KubeRay 通常使用稳定的 Head Service DNS，Worker 有机会重新解析并重连；如果 Worker 写死旧 Pod IP，则 Ray 自身无法跨节点发现新 Head。
8. **Worker 进程断联**靠 socket 扫描（1 秒），**Worker 节点断联**靠 GCS HealthCheck（5 次失败），恢复动作最终都汇聚到 `GcsActorManager::RestartActor`。
9. **Serve 高可用不是只开 GCS FT 就够了**：还需要 `proxy_location: EveryNode`、`*-serve-svc` 全节点流量、Head `num-cpus: "0"`、多节点副本分散、客户端重试。
10. **Head 挂掉后 Worker 不是立刻不可用**：KubeRay 默认给 Worker 注入 600 秒重连超时，期间已有 Serve 副本和 Proxy 可以继续服务。

---

## 2. 角色、连接与术语

### 2.1 拓扑

```text
┌──────────────────────────────────────────────┐
│                   Head Node                  │
│  ┌────────────┐    gRPC     ┌─────────────┐  │
│  │ GCS Server │◄───────────►│ Head Raylet │  │
│  └─────┬──────┘  RaySyncer  └──────┬──────┘  │
│        │ HealthCheck(pull)          │         │
│        │                            │ socket  │
│  ┌─────▼──────┐              ┌──────▼──────┐  │
│  │   Redis    │              │ CoreWorker  │  │
│  │ (GCS HA)   │              │  (driver)   │  │
│  └────────────┘              └─────────────┘  │
└──────────────────────────────────────────────┘
              ▲  RaySyncer(双向流) + HealthCheck(反向pull)
              │
┌─────────────┴────────────────────────────────┐
│                 Worker Node                  │
│  ┌────────────┐   gRPC/RaySyncer  ┌────────┐ │
│  │   Raylet   │◄─────────────────►│  GCS   │ │
│  └─────┬──────┘                   └────────┘ │
│        │ socket / pid check                   │
│  ┌─────▼──────┐                               │
│  │ CoreWorker │                               │
│  └────────────┘                               │
└──────────────────────────────────────────────┘
```

### 2.2 关键术语

| 术语 | 含义 |
|---|---|
| ClusterID | 集群唯一 ID，GCS FT 模式下持久化在 Redis |
| NodeID | 每次 Raylet 进程启动生成，重启后会变化 |
| SessionName | Ray 会话名，外部 Redis 模式下持久化 |
| GCS Address | Worker Raylet 启动时拿到的 `host:port`，之后固定不变 |
| RaySyncer | Raylet 与 GCS 之间的双向资源同步流 |
| HealthCheck | GCS 主动向 Raylet 发起的 gRPC 探活 |
| CheckAlive | Raylet 反向询问 GCS“我还活着吗” |
| GCS FT | GCS Fault Tolerance，依赖外部 Redis 持久化元数据 |

### 2.3 关键组件

| 组件 | 位置 | 是否随 Head 重启而死亡 |
|---|---|---|
| `gcs_server` | Head | 是 |
| Head Raylet | Head | 是（看门狗或容器重启） |
| Serve Controller | Head（`HEAD_NODE_RESOURCE_NAME`） | 是，随后在新 Head 重建 |
| Head HTTP Proxy | Head | 是，随后重建 |
| Worker Raylet | Worker | 否（在超时窗口内） |
| Worker CoreWorker | Worker | 否（本机 Raylet 活着即可） |
| Serve Replica Actor | Worker | 否（detached actor） |
| Worker HTTP Proxy | Worker | 否（detached actor） |

---

## 3. 心跳机制

### 3.1 CoreWorker ↔ Raylet：没有心跳，靠 socket + 进程检查

Worker 进程与 Raylet 之间不是心跳协议，而是 gRPC 连接 + 周期性断连扫描。

Raylet 每 1 秒扫描所有 worker/driver 连接：

`src/ray/common/ray_config_def.h:1049`

```cpp
RAY_CONFIG(int64_t, raylet_check_for_unexpected_worker_disconnect_interval_ms, 1000)
```

`src/ray/raylet/node_manager.cc:587-613`

```cpp
void NodeManager::CheckForUnexpectedWorkerDisconnects() {
  ...
  std::vector<bool> disconnects = CheckForClientDisconnects(all_connections);
  for (size_t i = 0; i < disconnects.size(); i++) {
    if (disconnects[i]) {
      std::string msg = "Worker connection closed unexpectedly.";
      DestroyWorker(all_workers[i], rpc::WorkerExitType::SYSTEM_ERROR, msg);
    }
  }
}
```

CoreWorker 每 1 秒检查父 Raylet 进程是否还活着：

`src/ray/common/ray_config_def.h:261`

```cpp
RAY_CONFIG(uint64_t, raylet_death_check_interval_milliseconds, 1000)
```

`src/ray/core_worker/core_worker.cc:779-790`

```cpp
void CoreWorker::ExitIfParentRayletDies() {
  static auto raylet_pid =
      static_cast<pid_t>(std::stoi(RayConfig::instance().RAYLET_PID()));
  bool should_shutdown = !IsProcessAlive(raylet_pid);
  if (should_shutdown) {
    RAY_LOG(WARNING) << "Shutting down the core worker because the local raylet failed.";
    KillChildProcs();
    QuickExit();
  }
}
```

CoreWorker 自己的 `InternalHeartbeat` **不是存活心跳**，只做内部簿记和重试：

`src/ray/core_worker/core_worker.cc:795-840`

```cpp
void CoreWorker::InternalHeartbeat() {
  // 1. 重试提交失败的任务
  // 2. 检查等待 death info 的 actor 任务超时
  // 3. 上报 worker backlog
  // 4. 处理未处理异常
}
```

周期：

`src/ray/common/ray_config_def.h:748`

```cpp
RAY_CONFIG(int64_t, core_worker_internal_heartbeat_ms, 1000)
```

### 3.2 Raylet ↔ GCS：RaySyncer 双向流（主信号）

Raylet 在 `RegisterGcs()` 里注册 `RESOURCE_VIEW` reporter，并连接到 GCS：

`src/ray/raylet/node_manager.cc:338-381`

```cpp
ray_syncer_.Register(
    syncer::MessageType::RESOURCE_VIEW,
    &cluster_resource_scheduler_.GetLocalResourceManager(),
    this,
    report_resources_period_ms_);   // 100ms

auto gcs_channel = gcs_client_.GetGcsRpcClient().GetChannel();
ray_syncer_.Connect(kGCSNodeID.Binary(), gcs_channel);
```

GCS 侧收到任意 syncer 消息就调用 `MarkNodeHealthy`：

`src/ray/gcs/gcs_server.cc:596-609`

```cpp
ray_syncer_ = std::make_unique<syncer::RaySyncer>(
    ...,
    [this](const NodeID &node_id) {
      gcs_healthcheck_manager_->MarkNodeHealthy(node_id);
    });
```

`src/ray/ray_syncer/ray_syncer_bidi_reactor_base.h:243-250`

```cpp
if (on_rpc_completion_) {
  on_rpc_completion_(NodeID::FromBinary(remote_node_id_));
}
```

**关键细节：这不是严格意义的 100ms 心跳。**

Raylet 每 100ms 轮询一次，但 `CreateSyncMessage` 会比较版本号，资源没变化就返回 `nullopt`，不发送任何消息：

`src/ray/raylet/scheduling/local_resource_manager.cc:435-439`

```cpp
const_cast<LocalResourceManager *>(this)->UpdateAvailableObjectStoreMemResource();
if (version_ <= after_version) {
  return std::nullopt;   // 无变化，不发送
}
```

因此：

- **繁忙集群**：资源变化频繁，RaySyncer 消息持续刷新健康时间戳
- **空闲集群**：没有周期消息，真正的周期心跳是 GCS 的 HealthCheck

RaySyncer 断流后，Raylet 侧每 2 秒自动重连：

`src/ray/ray_syncer/ray_syncer.cc:99-112`

```cpp
if (restart) {
  execute_after(io_context_, [this, remote_node_id, channel]() {
    RAY_LOG(INFO) << "Connection to the node was broken, reconnecting.";
    Connect(remote_node_id, channel);
  }, std::chrono::milliseconds(2000));
}
```

### 3.3 GCS → Raylet：主动 HealthCheck（兜底探活）

GCS 为每个已注册节点创建一个 `HealthCheckContext`，定期向 Raylet 的 gRPC Health 服务发起 `Check`。

Raylet 启动时把自己的健康状态设置为 `SERVING`，service name 就是 node id：

`src/ray/raylet/node_manager.cc:268-272`

```cpp
node_manager_server_.GetServer().GetHealthCheckService()->SetServingStatus(
    self_node_id_.Hex(), true);
```

默认参数：

`src/ray/common/ray_config_def.h:930-936`

```cpp
RAY_CONFIG(int64_t, health_check_initial_delay_ms, 5000)
RAY_CONFIG(int64_t, health_check_period_ms, 3000)
RAY_CONFIG(int64_t, health_check_timeout_ms, 10000)
RAY_CONFIG(int64_t, health_check_failure_threshold, 5)
```

核心逻辑：

`src/ray/gcs/gcs_health_check_manager.cc:122-225`

```cpp
void GcsHealthCheckManager::HealthCheckContext::StartHealthCheck() {
  const auto now = absl::Now();
  absl::Time next_check_time =
      latest_known_healthy_timestamp_ + absl::Milliseconds(manager->period_ms_);

  if (now <= next_check_time) {
    // RaySyncer 刚刚更新过健康时间戳，跳过本次 RPC
    timer_.expires_from_now(...);
    timer_.async_wait([this](auto) { StartHealthCheck(); });
    return;
  }

  ...
  stub_->async()->Check(context_ptr, &request_, response_ptr,
    [this, ...](::grpc::Status status) {
      if (status.ok() && response->status() == HealthCheckResponse::SERVING) {
        health_check_remaining_ = mgr->failure_threshold_;  // 成功则重置
      } else {
        --health_check_remaining_;                          // 失败则递减
      }

      if (health_check_remaining_ == 0) {
        mgr->FailNode(node_id_);                            // 连续失败 5 次判死
      } else {
        timer_.expires_from_now(milliseconds(mgr->period_ms_));
        timer_.async_wait([this](auto) { StartHealthCheck(); });
      }
    });
}
```

判死回调：

`src/ray/gcs/gcs_health_check_manager.cc:70-80`

```cpp
void GcsHealthCheckManager::FailNode(const NodeID &node_id) {
  RAY_LOG(WARNING) << "Node is dead because the health check failed.";
  auto iter = health_check_contexts_.find(node_id);
  if (iter != health_check_contexts_.end()) {
    on_node_death_callback_(node_id);
    health_check_contexts_.erase(iter);
  }
}
```

### 3.4 Raylet → GCS：CheckAlive 反向自检（防脑裂）

Raylet 每 60 秒反向问 GCS：“你还认为我活着吗？”

`src/ray/raylet/node_manager.cc:451-483`

```cpp
periodical_runner_->RunFnPeriodically(
    [this] {
      gcs_client_.Nodes().AsyncCheckAlive(
          {self_node_id_},
          /* timeout_ms = */ 30000,
          [this, checking_ptr = &checking](const auto &status,
                                           const auto &alive_vec) mutable {
            bool alive = alive_vec[0];
            if ((status.ok() && !alive)) {
              RAY_LOG(FATAL)
                  << "GCS consider this node to be dead. This may happen when "
                  << "GCS is not backed by a DB and restarted or there is data loss "
                  << "in the DB.";
            } else if (status.IsUnauthenticated()) {
              RAY_LOG(FATAL)
                  << "GCS returned an authentication error. This may happen when "
                  << "GCS is not backed by a DB and restarted or there is data loss "
                  << "in the DB. Local cluster ID: " << gcs_client_.GetClusterId();
            }
            *checking_ptr = false;
          });
    },
    RayConfig::instance().raylet_liveness_self_check_interval_ms(),  // 60000
    "NodeManager.GcsCheckAlive");
```

GCS 侧实现：

`src/ray/gcs/gcs_node_manager.cc:155-166`

```cpp
void GcsNodeManager::HandleCheckAlive(...) {
  absl::ReaderMutexLock lock(&mutex_);
  for (const auto &id : request.node_ids()) {
    const auto node_id = NodeID::FromBinary(id);
    const bool is_alive = alive_nodes_.contains(node_id);
    reply->mutable_raylet_alive()->Add(is_alive);
  }
}
```

注意：

- `status.ok() && alive == false` → FATAL
- `Unauthenticated` → FATAL
- **普通网络错误 `status` 不 ok → 不会 FATAL**

### 3.5 GCS ↔ Redis：GCS HA 存储健康

`src/ray/gcs/gcs_server.cc:166-183`

```cpp
periodical_runner_->RunFnPeriodically(
    [redis_store_client, &io_context] {
      redis_store_client->AsyncCheckHealth(
          {[](const Status &status) {
             RAY_CHECK_OK(status) << "Redis connection failed unexpectedly.";
           },
           io_context});
    },
    RayConfig::instance().gcs_redis_heartbeat_interval_milliseconds(),
    "GCSServer.redis_health_check");
```

默认 100ms：

`src/ray/common/ray_config_def.h:65`

```cpp
RAY_CONFIG(uint64_t, raylet_report_resources_period_milliseconds, 100)
```

> Redis 不可用时，GCS 通过 `RAY_CHECK_OK` 直接崩溃。所以 Redis 必须做 HA。

### 3.6 时间常数汇总

| 参数 | 默认值 | 作用 |
|---|---|---|
| `raylet_report_resources_period_milliseconds` | 100 | 资源视图轮询周期（有变化才发） |
| `health_check_initial_delay_ms` | 5000 | 节点加入后首次探活延迟 |
| `health_check_period_ms` | 3000 | 探活周期 |
| `health_check_timeout_ms` | 10000 | 单次探活 RPC 超时 |
| `health_check_failure_threshold` | 5 | 连续失败次数判死 |
| `raylet_liveness_self_check_interval_ms` | 60000 | Raylet 反向自检 GCS 周期 |
| `gcs_rpc_server_reconnect_timeout_s` | 60 | GCS 不可用总超时后进程退出 |
| `grpc_client_check_connection_status_interval_milliseconds` | 1000 | 连接状态检测周期 |
| `gcs_grpc_initial_reconnect_backoff_ms` | 100 | gRPC 初始重连退避 |
| `gcs_grpc_min_reconnect_backoff_ms` | 1000 | gRPC 最小重连退避 |
| `gcs_grpc_max_reconnect_backoff_ms` | 2000 | gRPC 最大重连退避 |
| `raylet_check_for_unexpected_worker_disconnect_interval_ms` | 1000 | Worker socket 断连扫描周期 |
| `raylet_death_check_interval_milliseconds` | 1000 | Worker 检查父 Raylet 周期 |
| `core_worker_internal_heartbeat_ms` | 1000 | CoreWorker 内部簿记周期 |
| `gcs_redis_heartbeat_interval_milliseconds` | 100 | GCS 检查 Redis 健康周期 |
| `maximum_gcs_dead_node_cached_count` | 1000 | 死节点缓存上限 |
| `subscriber_timeout_ms` | 300000 | pubsub 订阅者超时 |

### 3.7 判死时间估算

| 场景 | 时间 |
|---|---|
| Raylet 进程崩溃/端口关闭，RPC 立即 connection refused | 约 3s（等待 timestamp 过期）+ 5×3s ≈ **15~20s** |
| Raylet 进程卡死，RPC 每次吃满 10s 超时 | 3s + 5×(10s+3s) ≈ **65~70s** |
| 首次加入节点 | 先等 `health_check_initial_delay_ms=5s` |

---

## 4. 节点死亡判定完整链路

### 阶段 1：RaySyncer 流断开

`OnReadDone(ok=false)` → `Disconnect()`。GCS 侧只做两件事：移除 reactor、清理节点资源缓存，**不会直接判死**。

`src/ray/ray_syncer/ray_syncer_bidi_reactor_base.h:238-246`

```cpp
if (!ok) {
  RAY_LOG_EVERY_MS(INFO, 1000) << "Failed to read a message from node: ...";
  Disconnect();
  return;
}
```

### 阶段 2：`MarkNodeHealthy` 停止

节点不再有 syncer 消息，`latest_known_healthy_timestamp_` 不再更新。

### 阶段 3：HealthCheck 连续失败

`FailNode` → `on_node_death_callback_`：

`src/ray/gcs/gcs_server.cc:367-376`

```cpp
auto node_death_callback = [this](const NodeID &node_id) {
  this->io_context_provider_.GetDefaultIOContext().post(
      [this, node_id] { return gcs_node_manager_->OnNodeFailure(node_id, nullptr); },
      "GcsServer.NodeDeathCallback");
};
```

### 阶段 4：`OnNodeFailure` 判死并刷表

`src/ray/gcs/gcs_node_manager.cc:682-719`

```cpp
void GcsNodeManager::OnNodeFailure(
    const NodeID &node_id,
    const std::function<void()> &node_table_updated_callback) {
  absl::MutexLock lock(&mutex_);
  InternalOnNodeFailure(node_id, node_table_updated_callback);
}

void GcsNodeManager::InternalOnNodeFailure(...) {
  auto maybe_node = GetAliveNodeFromCache(node_id);
  if (maybe_node.has_value()) {
    rpc::NodeDeathInfo death_info = InferDeathInfo(node_id);
    auto node = RemoveNodeFromCache(
        node_id, death_info, rpc::GcsNodeInfo::DEAD, current_sys_time_ms());

    AddDeadNodeToCache(node);
    ...
    gcs_table_storage_->NodeTable().Put(node_id, *node, {std::move(on_done), io_context_});
  }
}
```

死亡原因判定：

`src/ray/gcs/gcs_node_manager.cc:541-567`

```cpp
rpc::NodeDeathInfo GcsNodeManager::InferDeathInfo(const NodeID &node_id) {
  auto iter = draining_nodes_.find(node_id);
  ...
  if (expect_force_termination) {
    death_info.set_reason(rpc::NodeDeathInfo::AUTOSCALER_DRAIN_PREEMPTED);
  } else {
    death_info.set_reason(rpc::NodeDeathInfo::UNEXPECTED_TERMINATION);
    death_info.set_reason_message(
        "health check failed due to missing too many heartbeats");
  }
  return death_info;
}
```

### 阶段 5：`RemoveNodeFromCache` 级联回调

`src/ray/gcs/gcs_node_manager.cc:624-680`

```cpp
alive_nodes_.erase(iter);
draining_nodes_.erase(node_id);

if (node_death_info.reason() == rpc::NodeDeathInfo::UNEXPECTED_TERMINATION) {
  // 发布 RAY_NODE_REMOVED 错误事件
  ...
}

for (auto &listener : node_removed_listeners_) {
  listener.Post("NodeManager.RemoveNodeCallback", removed_node);
}
```

`src/ray/gcs/gcs_server.cc:846-862` 注册的监听器会依次执行：

```cpp
gcs_resource_manager_->OnNodeDead(node_id);
gcs_placement_group_manager_->OnNodeDead(node_id);
gcs_actor_manager_->OnNodeDead(node, node_ip_address);
gcs_job_manager_->OnNodeDead(node_id);
raylet_client_pool_.Disconnect(node_id);
worker_client_pool_.Disconnect(node_id);
gcs_healthcheck_manager_->RemoveNode(node_id);
pubsub_handler_->AsyncRemoveSubscriberFrom(node_id.Binary());
gcs_autoscaler_state_manager_->OnNodeDead(node_id);
```

### 阶段 6：各模块恢复动作

| 模块 | 动作 |
|---|---|
| ResourceManager | 移除该节点资源 |
| PlacementGroupManager | 标记 bundle 失败，重新调度 PG |
| ActorManager | 重建该节点上所有 Actor |
| JobManager | 把 driver 在该节点的 Job 标记为 finished |
| RayletClientPool | 断开到该节点 raylet 的连接 |
| PubSub | 移除订阅者 |
| Autoscaler | 更新状态 |

Actor 重建逻辑：

`src/ray/gcs/actor/gcs_actor_manager.cc:1289-1385`

```cpp
// 1. 杀掉 dead node 上 owner 的所有子 actor
// 2. 取消正在该节点调度的 actor，并 RestartActor
// 3. 该节点上已经创建的 actor 全部 RestartActor
// 4. owner 在该节点且依赖未解析的 actor 直接 DestroyActor
for (auto &entry : created_actors) {
  RestartActor(entry.second,
               /*need_reschedule=*/true,
               GenNodeDiedCause(GetActor(entry.second), node));
}
```

重启次数判定：

`src/ray/gcs/actor/gcs_actor_manager.cc:1477-1495`

```cpp
int64_t max_restarts = mutable_actor_table_data->max_restarts();
uint64_t num_restarts = mutable_actor_table_data->num_restarts();
uint64_t num_restarts_due_to_node_preemption =
    mutable_actor_table_data->num_restarts_due_to_node_preemption();

if (!need_reschedule) {
  remaining_restarts = 0;
} else if (max_restarts == -1) {
  remaining_restarts = -1;                 // 无限重启
} else {
  // 节点抢占导致的重启不计入 max_restarts
  const auto effective_restarts = num_restarts - num_restarts_due_to_node_preemption;
  remaining_restarts = std::max(max_restarts - (int64_t)effective_restarts, 0L);
}
```

### 4.7 防脑裂 Fencing

两道 fencing：

1. **GCS 通知 Raylet 已死**：

`src/ray/raylet/node_manager.cc:907-925`

```cpp
void NodeManager::NodeRemoved(const NodeID &node_id) {
  if (node_id == self_node_id_) {
    if (!shutting_down_) {
      RAY_LOG(FATAL) << "[Timeout] Exiting because this node manager has mistakenly "
                        "been marked as dead by the GCS...";
      RAY_EVENT(FATAL, "RAYLET_MARKED_DEAD")...
    }
  }
}
```

2. **Raylet 自检发现 GCS 不认自己**：

见 3.4 的 `CheckAlive`。这两个机制保证网络分区恢复后，旧 Raylet 不会和新 Raylet 同时服务。

---

## 5. Head OOM 重启恢复机制

### 5.1 为什么 GCS OOM 会导致整个 Head 容器重启

在 K8s 中，`ray start --head --block` 通常是 PID 1。它有一个看门狗循环：

`python/ray/scripts/scripts.py:1179-1229`

```python
while True:
    time.sleep(1)
    deceased = node.dead_processes()

    expected_return_codes = [
        0,
        signal.SIGTERM,
        -1 * signal.SIGTERM,
        128 + signal.SIGTERM,
    ]
    unexpected_deceased = [
        (process_type, process)
        for process_type, process in deceased
        if process.returncode not in expected_return_codes
    ]
    if len(unexpected_deceased) > 0:
        ...
        node.kill_all_processes(check_alive=False, allow_graceful=False)
        os._exit(1)
```

OOM Kill 是 `SIGKILL(9)`，退出码 `-9` 或 `137`，不在预期列表中，因此：

1. 看门狗判定 `gcs_server` 意外退出
2. `kill_all_processes` 杀掉 raylet、dashboard、monitor 等所有子进程
3. `os._exit(1)` 让容器退出
4. K8s 按 `restartPolicy` 重启容器

所以 **GCS OOM 不会只重启 GCS 一个进程，而是整个 Head 容器重启**。

> 不同部署方式下，Head 退出时间可能是秒级（看门狗）或约 60 秒（GCS 客户端重连超时）。只要小于 Worker 的 600 秒窗口，就不影响 Worker 存活。

### 5.2 两种存储模式

GCS 存储类型选择：

`src/ray/gcs/gcs_server.cc:567-583`

```cpp
GcsServer::StorageType GcsServer::GetStorageType() const {
  if (RayConfig::instance().gcs_storage() == kInMemoryStorage) {
    if (!config_.redis_address.empty()) {
      return StorageType::REDIS_PERSIST;
    }
    return StorageType::IN_MEMORY;
  }
  if (RayConfig::instance().gcs_storage() == kRedisStorage) {
    RAY_CHECK(!config_.redis_address.empty());
    return StorageType::REDIS_PERSIST;
  }
}
```

#### 模式 A：默认内存存储（无外部 Redis）

- GCS 状态全在内存
- 重启后 `GcsInitData` 读到空表
- `GetOrGenerateClusterId` 生成全新的 ClusterID
- 旧 Raylet 的 gRPC 请求携带旧 ClusterID，被新 GCS 鉴权拦截器拒绝
- Raylet 60 秒自检 `CheckAlive` 返回 `alive=false` → `RAY_LOG(FATAL)`
- 最终所有旧 Raylet 退出，Pod 重启后以新节点身份加入一个全新的空集群

**结论：内存模式下 Head 重启 = 集群状态丢失，无法原地恢复。**

#### 模式 B：外部 Redis（GCS Fault Tolerance）

GCS 启动时从 Redis 加载 5 张表：

`src/ray/gcs/gcs_init_data.cc:23-40`

```cpp
// There are 5 kinds of table data need to be loaded.
AsyncLoadJobTableData(on_load_finished);
AsyncLoadNodeTableData(on_load_finished);
AsyncLoadActorTableData(on_load_finished);
AsyncLoadActorTaskSpecTableData(on_load_finished);
AsyncLoadPlacementGroupTableData(on_load_finished);
```

ClusterID 也持久化在 Redis：

`src/ray/gcs/gcs_server.cc:238-258`

```cpp
if (!provided_cluster_id.has_value()) {
  ClusterID cluster_id = ClusterID::FromRandom();
  RAY_LOG(INFO) << "Generated new cluster ID.";
  ...
}
```

Redis key 带 external storage namespace，避免多集群互相污染：

`src/ray/gcs/store_client/redis_store_client.cc:68-72`

```cpp
std::string RedisKey::ToString() const {
  // 形如 RAY<namespace>@NODE
  return absl::StrCat("RAY", external_storage_namespace, kClusterSeparator, table_name);
}
```

### 5.3 外部 Redis 模式下 Head 重启的完整恢复时序

```text
T+0     gcs_server 被 OOM Kill
T+0     Head raylet 的 GCS gRPC channel 进入 TRANSIENT_FAILURE
T+0     待发送 RPC 进入 RetryableGrpcClient 队列
T+1s    ray start --block 看门狗发现 -9，kill 所有子进程，os._exit(1)
T+?     K8s 重启 Head 容器
T+?     新 gcs_server 启动，连接 Redis
T+?     GcsInitData 异步加载 Job/Node/Actor/ActorTaskSpec/PlacementGroup
T+?     GetOrGenerateClusterId 从 Redis 读回原 ClusterID
T+?     GcsNodeManager::Initialize 对 Redis 中所有 ALIVE 节点发送 NotifyGCSRestart
T+?     Raylet 收到 NotifyGCSRestart → AsyncResubscribe + 通知本地 CoreWorker
T+?     RaySyncer 每 2s 重连，资源视图恢复
T+?     GcsHealthCheckManager 重新为 ALIVE 节点建立 HealthCheckContext
```

GCS 初始化时对存活节点发送 `NotifyGCSRestart`：

`src/ray/gcs/gcs_node_manager.cc:721-750`

```cpp
void GcsNodeManager::Initialize(const GcsInitData &gcs_init_data) {
  for (const auto &[node_id, node_info] : gcs_init_data.Nodes()) {
    if (node_info.state() == rpc::GcsNodeInfo::ALIVE) {
      AddNodeToCache(std::make_shared<rpc::GcsNodeInfo>(node_info));

      // Ask the raylet to do initialization in case of GCS restart.
      auto remote_address = rpc::RayletClientPool::GenerateRayletAddress(
          node_id, node_info.node_manager_address(), node_info.node_manager_port());
      auto raylet_client = raylet_client_pool_->GetOrConnectByAddress(remote_address);
      raylet_client->NotifyGCSRestart(
          [](const Status &status, const rpc::NotifyGCSRestartReply &reply) {
            if (!status.ok()) {
              RAY_LOG(WARNING) << "NotifyGCSRestart failed. This is expected if the "
                                  "target node has died.";
            }
          });
    }
  }
}
```

Raylet 处理：

`src/ray/raylet/node_manager.cc:1050-1066`

```cpp
void NodeManager::HandleNotifyGCSRestart(...) {
  RAY_LOG(INFO) << "The GCS has restarted. Resubscribing to pubsub and notifying local workers.";
  gcs_client_.AsyncResubscribe();

  auto workers = worker_pool_.GetAllRegisteredWorkers(true);
  for (const auto &worker : workers) {
    worker->AsyncNotifyGCSRestart();
  }
  auto drivers = worker_pool_.GetAllRegisteredDrivers(true);
  for (const auto &driver : drivers) {
    driver->AsyncNotifyGCSRestart();
  }
}
```

GCS 客户端重订阅：

`src/ray/gcs_rpc_client/gcs_client.cc:177-183`

```cpp
resubscribe_func_ = [this]() {
  RAY_LOG(INFO) << "Resubscribing to GCS tables.";
  job_accessor_->AsyncResubscribe();
  actor_accessor_->AsyncResubscribe();
  node_accessor_->AsyncResubscribe();
  worker_accessor_->AsyncResubscribe();
};
```

CoreWorker 处理：

`src/ray/core_worker/core_worker.cc:3439-3447`

```cpp
void CoreWorker::HandleRayletNotifyGCSRestart(...) {
  gcs_client_->AsyncResubscribe();
  send_reply_callback(Status::OK(), nullptr, nullptr);
}
```

Actor 表恢复策略：

`src/ray/gcs/actor/gcs_actor_manager.cc:1713-1790`

```cpp
// 只加载应该存活的 actor
if (OnInitializeActorShouldLoad(gcs_init_data, actor_id)) {
  ...
  if (actor_table_data.state() == ALIVE) {
    created_actors_[actor->GetNodeID()].emplace(actor->GetWorkerID(), actor->GetActorID());
  }
  ...
}

// 只有 PENDING_CREATION / RESTARTING 的 actor 需要重新调度
for (auto &item : registered_actors_) {
  auto &actor = item.second;
  if (actor->GetState() == PENDING_CREATION ||
      actor->GetState() == RESTARTING) {
    gcs_actor_scheduler_->Reschedule(actor);
  }
}
```

> 注意：**ALIVE 的 actor 不会被重新调度**，因为 GCS 认为对应 worker 还在原节点上运行。只有 HEAD 那个节点上的 worker 随容器重启一起死了，所以新 Head 注册时会触发旧 Head 节点的 actor 重建。

### 5.4 GCS 重连超时：Raylet 不会无限等待

GCS gRPC channel 退避参数：

`src/ray/common/ray_config_def.h:459-461`

```cpp
RAY_CONFIG(int32_t, gcs_grpc_max_reconnect_backoff_ms, 2000)
RAY_CONFIG(int32_t, gcs_grpc_min_reconnect_backoff_ms, 1000)
RAY_CONFIG(int32_t, gcs_grpc_initial_reconnect_backoff_ms, 100)
```

重试客户端检测周期：

`src/ray/common/ray_config_def.h:469`

```cpp
RAY_CONFIG(int32_t, grpc_client_check_connection_status_interval_milliseconds, 1000)
```

不可用超时直接 `_Exit`：

`src/ray/gcs_rpc_client/rpc_client.h:193-205`

```cpp
/*server_unavailable_timeout_callback=*/
[]() {
  RAY_LOG(ERROR) << "Failed to connect to GCS within "
                 << RayConfig::instance().gcs_rpc_server_reconnect_timeout_s()
                 << " seconds. ... The program will terminate.";
  std::_Exit(EXIT_FAILURE);
},
```

默认 60 秒：

`src/ray/common/ray_config_def.h:450`

```cpp
RAY_CONFIG(uint32_t, gcs_rpc_server_reconnect_timeout_s, 60)
```

> KubeRay 会把 Worker Pod 的该值设成 **600s**，Head Pod 保持 60s，保证 Worker 不会比 Head 先退出。这是 GCS FT 能工作的关键运维参数。

### 5.5 RPC 重试队列与超时起算

`src/ray/rpc/retryable_grpc_client.cc:129-160`

```cpp
pending_requests_bytes_ += request_bytes;
const auto timeout = request->GetTimeoutMs() == -1
                         ? absl::InfiniteFuture()
                         : now + absl::Milliseconds(request->GetTimeoutMs());
pending_requests_.emplace(timeout, std::move(request));
if (!server_unavailable_timeout_time_.has_value()) {
  // First request to retry.
  server_unavailable_timeout_time_ =
      now + absl::Seconds(server_reconnect_timeout_base_seconds_);
}
```

`src/ray/rpc/retryable_grpc_client.cc:78-100`

```cpp
case GRPC_CHANNEL_TRANSIENT_FAILURE:
case GRPC_CHANNEL_CONNECTING: {
  if (server_unavailable_timeout_time_ < now) {
    RAY_LOG(WARNING) << server_name_ << " has been unavailable for more than ...";
    server_unavailable_timeout_callback_();
    ...
  }
}
```

> 60s / 600s 是“总不可用超时”，从第一条进入重试队列的 GCS RPC 开始计时。实际运行中 Raylet 每 60s 会发一次 `CheckAlive`，所以窗口基本稳定。

### 5.6 Serve Controller 在 Head 重建

Serve Controller 是 detached actor，并且固定要求 Head 资源：

`python/ray/serve/_private/default_impl.py:244-255`

```python
controller_impl = ray.remote(
    name=SERVE_CONTROLLER_NAME,
    namespace=SERVE_NAMESPACE,
    num_cpus=0,
    lifetime="detached",
    max_restarts=-1,
    max_task_retries=-1,
    resources={HEAD_NODE_RESOURCE_NAME: 0.001},
    max_concurrency=CONTROLLER_MAX_CONCURRENCY,
    enable_task_events=RAY_SERVE_ENABLE_TASK_EVENTS,
)(ServeController)
```

- `lifetime="detached"`：不随 driver 退出
- `max_restarts=-1`：允许 GCS 反复拉起
- `HEAD_NODE_RESOURCE_NAME`：确保 Controller 落在新的 Head 上

Controller 重启后从 GCS KV 读取 Serve 配置 checkpoint：

`python/ray/serve/_private/controller.py:755-800`

```python
def _recover_state_from_checkpoint(self):
    ...
    checkpoint = self.kv_store.get(CONFIG_CHECKPOINT_KEY)
    ...
```

GCS KV 在 FT 模式下由 Redis 持久化，所以应用列表、Deployment 配置、副本状态可以恢复。

### 5.7 旧 Head 节点被标记为 DEAD

新 Head 的 Raylet 注册时，GCS 会先杀掉旧的 Head 节点：

`src/ray/gcs/gcs_node_manager.cc:123-146`

```cpp
if (node_info.is_head_node()) {
    // mark all old head nodes as dead if exists:
    std::vector<NodeID> head_nodes;
    ...
    if (head_nodes.size() == 1) {
      OnNodeFailure(head_nodes[0],
                    [this, node_id, node_info, on_done = std::move(on_done)]() {
                      gcs_table_storage_->NodeTable().Put(...);
                    });
    }
}
```

旧 Head 上的 Actor（包括旧 Controller、Head Proxy）会被 `OnNodeDead` 重建。

---

## 6. Head 重启场景矩阵：Pod 不变 vs Pod 变化

### 6.1 四种场景

| 场景 | Pod | IP | Node | GCS 端口 | Worker 地址 | 能否自动恢复 |
|---|---|---|---|---|---|---|
| A0 | 不变 | 不变 | 不变 | 不变 | 任意 | ✅ 完全无缝 |
| A1 | 不变（容器重启） | 不变 | 不变 | 不变 | 不变 | ✅ 可恢复 |
| B1 | 重建 | 变 | 可能变 | 不变 | Head Service DNS | ✅ 可恢复 |
| B2 | 重建 | 变 | 可能变 | 不变 | 旧 Pod IP | ❌ Worker 最终退出 |
| C | 任意 | 任意 | 任意 | 任意 | 任意 | ❌ 无 Redis 时不可恢复 |

### 6.2 场景 A0：只重启 GCS 进程，Head Raylet 不重启

- Head Pod 不变
- Head Raylet 不变
- Serve Controller 不变
- 新 GCS 从 Redis 加载状态
- 对所有 ALIVE 节点发送 `NotifyGCSRestart`
- Worker 重订阅
- 服务完全无感

### 6.3 场景 A1：Head 容器重启，Pod 不变

#### 时序

```text
T+0     head 容器内的 gcs_server 被 OOM Kill
T+0     容器内 raylet / controller / head proxy 随后被看门狗清理
T+1s    ray start --block 看门狗发现 -9，kill 所有子进程，os._exit(1)
T+?     K8s 重启同一个 Pod 的容器
        ├─ Pod UID 不变
        ├─ Pod IP 不变
        ├─ Node 不变
        └─ GCS 端口仍为 6379
T+?     新 gcs_server 启动，连接 Redis，加载 Node/Actor/PG/Job 表
T+?     新 head raylet 启动，生成新的 NodeID，注册到 GCS
T+?     GCS 发现旧 head 节点，标记为 DEAD
T+?     GCS 对 Redis 中所有 ALIVE Worker 发送 NotifyGCSRestart
T+?     Worker Raylet 重订阅，通知本地 CoreWorker
T+?     Serve Controller 在“新 head 节点”上重建
T+?     Controller 从 GCS KV checkpoints 恢复 Serve 配置
```

> 虽然 Pod IP 没变，但 **Head Raylet 的 NodeID 变了**，所以旧 Head 节点仍然会被判死。

#### Worker 侧行为

| 组件 | 行为 |
|---|---|
| Worker Raylet | GCS channel 断开，按 100ms / 1s / 2s 退避重连 |
| RaySyncer | 每 2s 重连一次 |
| 待发 GCS RPC | 进入 `pending_requests_` 队列，等 channel READY 后重放 |
| Worker CoreWorker | 已缓存的 Handle、已建立的 Actor 连接继续工作 |
| Serve 副本 | **完全不重启**，继续处理请求 |
| Worker HTTP Proxy | **完全不重启**，继续用缓存路由转发 |
| Serve Controller | 在旧 Head 上，已死；在新 Head 上重建 |
| Head HTTP Proxy | 已死；由新 Controller 在 Head 上重建 |

#### 最终状态

```text
ray.nodes()
├─ old-head-node   Alive=False  DeathReason=UNEXPECTED_TERMINATION
├─ new-head-node   Alive=True
└─ worker-node-*   Alive=True   ← 从未重启
```

#### 关键条件

- 使用外部 Redis
- Worker 的 `RAY_gcs_rpc_server_reconnect_timeout_s` > Head 重启时间
- Head Pod IP / GCS 端口不变

### 6.4 场景 B1：Head Pod 重建，Worker 用 Head Service DNS

#### 时序

```text
T+0     Head Pod 被删除 / 被驱逐
T+0     Head Service 的 Endpoint 暂时为空
T+?     KubeRay 在某个节点重建 Head Pod
        ├─ Pod 名字可能相同
        ├─ Pod UID 变化
        ├─ Pod IP 变化
        └─ Node 可能变化
T+?     Head Service 的 Endpoint 更新为新 Pod IP
T+?     新 GCS 启动，从 Redis 加载状态
T+?     新 head raylet 注册，旧 head 节点被判死
T+?     GCS 向 Worker 发送 NotifyGCSRestart
T+?     Worker gRPC 通过 head-svc DNS 重新解析到新 IP，连接成功
T+?     Worker 重订阅，Serve 继续
```

#### 为什么能连回来

Worker 的 `--address` 类似：

```text
raycluster-xxx-head-svc:6379
```

不是 Pod IP。gRPC 使用 DNS resolver，在连接失败后会重新解析域名；Head Service 更新 Endpoint 后，DNS 返回新 Pod IP，Worker 就能连上。

#### 风险

| 风险 | 说明 |
|---|---|
| DNS 缓存 / gRPC 未重新解析 | 极少数情况下需要重启 Worker Pod |
| Head 重建时间 > 600s | Worker 先退出 |
| Redis 不可用 | 新 GCS 无法恢复状态 |
| Worker 用旧 Pod IP 而不是 head-svc | 退化为 B2 |

### 6.5 场景 B2：Head Pod 重建，Worker 用旧 Pod IP（自管理集群）

#### 时序

```text
T+0     Head Pod 重建，新 IP = 10.0.0.20
T+0     Worker 的 gcs_address 仍是旧 IP = 10.0.0.10:6379
T+0     Worker 的 gRPC channel 连旧 IP，失败
T+0     Worker RaySyncer 连旧 IP，每 2s 失败一次
T+0     Worker 上的 Serve 副本和 Proxy 继续用缓存状态服务
T+0     新 GCS 从 Redis 加载 Worker 节点，状态仍是 ALIVE
T+0     新 GCS 反向健康检查 Worker，成功
        → 所以新 GCS 不会立刻把 Worker 判死
T+600s  Worker 的 RetryableGrpcClient 超时
        → std::_Exit(EXIT_FAILURE)
        → ray start --block 看门狗清理进程
        → Worker 容器重启
T+600s  新容器仍然读到旧 IP，继续失败
T+600s  Worker Raylet 死了，新 GCS 健康检查开始失败
T+600s+ GCS 将 Worker 节点标记为 DEAD
T+600s+ ActorManager 把该 Worker 上的 Serve 副本重建到其他节点
```

#### 为什么新 GCS 不立刻判死 Worker

健康检查方向是：

```text
GCS（新 Head） ──HealthCheck──► Worker Raylet
```

不是：

```text
Worker Raylet ──Heartbeat──► GCS（新 Head）
```

Worker Raylet 还活着，所以 GCS 的健康检查成功，节点继续是 `ALIVE`。

但 Worker 到 GCS 的 RPC 全部失败，所以：

- Worker 收不到 `NotifyGCSRestart`
- Worker 无法重订阅
- Worker 无法接收新的 Controller 指令
- Worker 只能靠缓存状态继续服务

这就是“单向断联”。

#### 最终恢复方式

1. Worker 600s 后退出
2. Worker 容器重启
3. 如果 Worker 仍然读旧 IP → 继续失败
4. 必须由 KubeRay/运维删除 Worker Pod，让它重新注入新的 Head 地址
5. 或者把 Worker 的 `--address` 改成 Head Service DNS

> 自管理集群强烈建议：Worker 使用稳定 DNS / VIP / Service，不要写死 Head Pod IP。

### 6.6 场景 C：没有外部 Redis

无论 Pod 变不变，都无法真正恢复：

- 新 GCS 内存为空
- 生成新 ClusterID
- 旧 Worker 的 RPC 因 ClusterID 不匹配被拒绝
- Raylet 60s 自检 `CheckAlive` 返回 `alive=false`
- Raylet `RAY_LOG(FATAL)` 退出
- 新 Head 把旧 Worker 视为 unknown worker
- 集群等价于重建
- Serve 配置 checkpoint 丢失，需要重新 `serve.run()` / 重新 apply RayService

---

## 7. Worker 断联与恢复机制

### 7.1 Worker 进程与 Raylet 断联

**检测方式：gRPC socket 关闭 + 周期扫描**

`src/ray/raylet/node_manager.cc:587-613`

```cpp
void NodeManager::CheckForUnexpectedWorkerDisconnects() {
  ...
  std::vector<bool> disconnects = CheckForClientDisconnects(all_connections);
  for (size_t i = 0; i < disconnects.size(); i++) {
    if (disconnects[i]) {
      std::string msg = "Worker connection closed unexpectedly.";
      DestroyWorker(all_workers[i], rpc::WorkerExitType::SYSTEM_ERROR, msg);
    }
  }
}
```

Worker 主动退出时走 `DisconnectClientRequest`：

`src/ray/raylet/node_manager.cc:1577-1595`

```cpp
void NodeManager::ProcessDisconnectClientMessage(...) {
  ...
  DisconnectClient(client,
                   /*graceful=*/true,
                   disconnect_type,
                   disconnect_detail,
                   creation_task_exception.get());
}
```

`DisconnectClient` 的核心动作：

`src/ray/raylet/node_manager.cc:1404-1575`

```cpp
// 1. 取消 ray.get / ray.wait
lease_dependency_manager_.CancelGetRequest(worker->WorkerId());
lease_dependency_manager_.CancelWaitRequest(worker->WorkerId());

// 2. 释放 lease 资源
ReleaseWorker(worker->GetGrantedLeaseId());
local_lease_manager_.CleanupLease(worker, &lease);
local_lease_manager_.ReleaseWorkerResources(worker);

// 3. 上报 worker failure 给 GCS
gcs_client_.Workers().AsyncReportWorkerFailure(worker_failure_data_ptr, nullptr);

// 4. 从 worker pool 移除
worker_pool_.DisconnectWorker(worker, disconnect_type);

// 5. 重新调度 lease
cluster_lease_manager_.ScheduleAndGrantLeases();
```

GCS 收到 worker failure 后通知 `GcsActorManager::OnWorkerDead`：

`src/ray/gcs/actor/gcs_actor_manager.cc:1185-1287`

```cpp
bool need_reconstruct = disconnect_type != INTENDED_USER_EXIT &&
                        disconnect_type != USER_ERROR;
...
// owner 死了，销毁其所有子 actor
for (const auto &child_id : children_ids) {
  DestroyActor(child_id, GenOwnerDiedCause(...));
}
...
// 重建该 worker 上的 actor
RestartActor(actor_id, /*need_reschedule=*/need_reconstruct, death_cause);
```

### 7.2 Worker 节点与 GCS 断联

| 情况 | 检测方式 | 结果 |
|---|---|---|
| Raylet 进程崩溃 | GCS HealthCheck 连续失败 5 次 | 节点 DEAD，Actor 重建，任务重新调度 |
| 网络分区，Raylet 还活着 | GCS HealthCheck 失败 | GCS 判死；分区恢复后 Raylet 收到自身 DEAD 通知 → `RAYLET_MARKED_DEAD` FATAL |
| 网络短暂抖动，未达阈值 | RaySyncer 2s 重连 | 不判死，无状态丢失 |
| Raylet 重启 | 新 NodeID 注册 | 旧节点最终 DEAD；新节点作为全新节点加入 |
| GCS 重启 | NotifyGCSRestart | Raylet 重订阅，Worker 重订阅 |

### 7.3 Serve 相关组件在 Head 故障期间的行为

| 组件 | 状态 | 说明 |
|---|---|---|
| Serve Controller | ❌ 死亡 | 在 Head 上，随 Head 一起挂 |
| Serve Replica Actor | ✅ 存活 | detached actor，不依赖 Head 进程 |
| Worker HTTP Proxy | ✅ 存活 | 路由表是缓存的，可继续转发请求 |
| Head HTTP Proxy | ❌ 死亡 | Head 容器挂了 |
| 新建/更新 Deployment | ❌ 不可用 | 需要 Controller + GCS |
| 自动扩缩容 | ❌ 暂停 | 需要 Controller |
| 副本健康检查重启 | ❌ 暂停 | 需要 Controller |
| `ray.get_actor` / 新建 Actor | ❌ 会阻塞/失败 | 需要 GCS |
| 已缓存 Handle 的 Actor 调用 | ✅ 可用 | 不需要实时查 GCS |
| 跨节点对象位置查询 | ⚠️ 可能失败 | Object Directory 在 GCS；已缓存位置可用 |

---

## 8. Worker 独立服务窗口：Head 挂掉后还能撑多久

### 8.1 结论

**能，但要看你配置的 `RAY_gcs_rpc_server_reconnect_timeout_s`。**

- Ray 默认：**60 秒**后 Worker Raylet 主动 `_Exit`
- KubeRay 默认给 Worker 注入：**600 秒（10 分钟）**

KubeRay 文档确认：

`doc/source/cluster/kubernetes/user-guides/kuberay-gcs-ft.md:178-209`

```text
# No RAY_gcs_rpc_server_reconnect_timeout_s environment variable is set.
# Hence, the Ray head uses its default value of 60.

# KubeRay injects the RAY_gcs_rpc_server_reconnect_timeout_s environment
# variable with the value 600 to the worker Pod.
```

在这段时间内：

- Worker 上的 Raylet、CoreWorker、Serve 副本、Worker Proxy 都还活着
- 已经建立的 Serve 路由、已缓存的 Actor Handle 可以继续工作
- 对外 HTTP 服务可以继续提供

但这是一个有上限的窗口，不是永久。

### 8.2 时间线

```text
T+0        Head 容器死亡 / GCS 进程消失
           ├─ Worker HTTP Proxy 仍在监听
           ├─ Serve 副本仍在处理请求
           ├─ Worker Raylet channel 进入 TRANSIENT_FAILURE
           └─ 待发 GCS RPC 进入重试队列

T+0~T+600s Worker 继续对外服务
           ├─ 每 1s 检查 channel 状态
           ├─ 每 2s RaySyncer 尝试重连
           ├─ 每 60s Raylet 自检（失败不会判死）
           ├─ 已缓存路由/Handle 的请求正常
           └─ 新的部署、扩缩容、Actor 创建不可用

T+600s     仍连不上 GCS
           ├─ RetryableGrpcClient 触发 server_unavailable_timeout_callback_
           ├─ Raylet std::_Exit(EXIT_FAILURE)
           ├─ ray start --block 看门狗发现异常退出
           ├─ kill_all_processes + os._exit(1)
           └─ Worker 容器重启，服务中断
```

如果 Head 在 600s 内恢复：

```text
T+120s     新 GCS 启动，Redis 加载状态
           ├─ channel 恢复 READY
           ├─ 队列中的 RPC 自动重放
           ├─ GCS 发送 NotifyGCSRestart
           ├─ Raylet AsyncResubscribe
           ├─ CoreWorker AsyncResubscribe
           ├─ Serve Controller 在新 Head 重建
           └─ Worker 不需要重启，服务无中断
```

### 8.3 窗口内可用 / 不可用

| 操作 | 窗口内是否可用 | 原因 |
|---|---|---|
| 已缓存路由的 HTTP 请求 | ✅ | Proxy 不依赖实时 GCS |
| 已缓存 Handle 的 Actor 调用 | ✅ | 不需要实时查 GCS |
| 新建 Actor | ❌ | 需要 GCS |
| `ray.get_actor` | ❌ | 需要 GCS |
| 新建/更新 Serve Deployment | ❌ | 需要 Controller + GCS |
| 自动扩缩容 | ❌ | 需要 Controller |
| 副本健康检查重启 | ❌ | 需要 Controller |
| 跨节点对象位置查询 | ⚠️ | Object Directory 在 GCS；已缓存位置可用 |
| Dashboard / `serve status` | ❌ | 需要 Head |
| 单节点 metrics 端点 | ✅ | 可直接抓取 Worker 的 8080 端口 |

### 8.4 如何拉长/做稳窗口

1. Worker 设置 `RAY_gcs_rpc_server_reconnect_timeout_s=600` 或 900
2. Head `num-cpus: "0"`，副本不落 Head
3. `proxy_location: EveryNode`
4. 流量走 `*-serve-svc`，不要走 `*-head-svc`
5. `num_replicas >= 2`，`max_replicas_per_node: 1`
6. 请求路径不要实时依赖 GCS
7. 客户端幂等 + 重试
8. Redis 做 HA
9. 保证 Head Service DNS / GCS 端口稳定
10. 尽量缩短 Head 重启时间

### 8.5 验证方法

```bash
# 方式一：只杀 GCS 进程
kubectl exec $HEAD_POD -- pkill -9 gcs_server

# 方式二：删除 Head Pod
kubectl delete pod $HEAD_POD
```

```bash
# 持续请求 Worker Proxy
while true; do
  curl -s -o /dev/null -w "%{http_code}\n" http://$WORKER_IP:8000/
  sleep 0.2
done
```

观察 Worker 日志：

```bash
kubectl logs $WORKER_POD -c ray-worker -f | grep -E \
  "Failed to connect to GCS|Connection to the node was broken|GCS has restarted|Resubscribing"
```

如果出现下面这条，说明 Worker 到达了 600s 上限：

```text
Failed to connect to GCS within ... seconds. ... The program will terminate.
```

---

## 9. Ray Serve 高可用配置

### 9.1 四层配置

1. **集群层**：KubeRay `RayService` + GCS FT（外部 HA Redis）
2. **流量层**：走 `*-serve-svc`（所有节点 endpoint），不要只走 `head-svc`
3. **调度层**：Head `num-cpus: "0"`，副本不落 Head，且多节点分散
4. **超时层**：Worker 的 `RAY_gcs_rpc_server_reconnect_timeout_s` 必须大于 Head 重启时间

### 9.2 最小可用 KubeRay RayService 配置

```yaml
apiVersion: ray.io/v1
kind: RayService
metadata:
  name: my-serve
  annotations:
    ray.io/ft-enabled: "true"
    ray.io/external-storage-namespace: "my-serve-ns"
spec:
  serveConfigV2: |
    proxy_location: EveryNode
    http_options:
      port: 8000
    applications:
      - name: my_app
        import_path: app:deployment
        route_prefix: /
        deployments:
          - name: MyDeployment
            num_replicas: 2
            max_replicas_per_node: 1
            max_ongoing_requests: 16
            graceful_shutdown_timeout_s: 20
            health_check_period_s: 10
            health_check_timeout_s: 30
            ray_actor_options:
              num_cpus: 1
              max_restarts: 0
              max_task_retries: -1

  rayClusterConfig:
    headGroupSpec:
      rayStartParams:
        num-cpus: "0"          # 关键：Head 不跑 Serve 副本
      template:
        spec:
          containers:
            - name: ray-head
              env:
                - name: RAY_REDIS_ADDRESS
                  value: "redis:6379"
                # Head 保持默认 60s；不要调大
                - name: RAY_gcs_rpc_server_reconnect_timeout_s
                  value: "60"
                # Serve Controller 访问 GCS KV 的超时
                - name: RAY_SERVE_KV_TIMEOUT_S
                  value: "30"

    workerGroupSpecs:
      - groupName: worker
        replicas: 2
        minReplicas: 2
        maxReplicas: 4
        rayStartParams: {}
        template:
          spec:
            containers:
              - name: ray-worker
                env:
                  # 必须显著大于 Head 重启时间，KubeRay 默认注入 600
                  - name: RAY_gcs_rpc_server_reconnect_timeout_s
                    value: "600"
```

### 9.3 配置项说明

| 配置 | 作用 |
|---|---|
| `ray.io/ft-enabled: "true"` + `RAY_REDIS_ADDRESS` | 开启 GCS FT，Head 重启后从 Redis 恢复 Node/Actor/PG/Job 表 |
| `external-storage-namespace` | 多集群共享 Redis 时隔离 key |
| `num-cpus: "0"`（Head） | Head 容器重启时不会连带杀死 Serve 副本 |
| `proxy_location: EveryNode` | 每个有副本的 Worker 节点都跑 Proxy，Head 挂了 Worker 还能接流量 |
| `num_replicas >= 2` + `max_replicas_per_node: 1` | 至少两个节点各有一个副本，单节点故障仍可服务 |
| Worker `reconnect_timeout_s=600` | Worker Raylet 等 Head 最多 10 分钟，不会先于 Head 退出 |
| `max_restarts: 0` | 副本重启交给 Serve Controller 管理，避免 Ray 层和 Serve 层重复重启 |
| `max_task_retries: -1` | 幂等请求可在 Worker/Head 短暂故障时重试 |

> **流量入口必须使用 `my-serve-serve-svc:8000`，不要用 `my-serve-head-svc:8000`。**

KubeRay 的 `serve-svc` 会把所有跑 Proxy 的节点（Head + Worker）都加入 Endpoints。Head 挂掉后，Service 自动把流量摘到 Worker Proxy。

参考：`doc/source/cluster/kubernetes/user-guides/rayservice-high-availability.md:45-67`

### 9.4 Serve 应用代码建议

#### 副本必须无状态或外置状态

GCS FT 只恢复 Ray 元数据（Actor 表、Node 表、PG、Job），**不恢复 Actor 内部 Python 状态**：

`src/ray/gcs/actor/gcs_actor_manager.cc:1713-1750`

```cpp
// 只加载应该存活的 actor
// ALIVE 的 actor 不会被重新调度
```

所以：

- 不要把会话、计数器、缓存等关键状态只放在副本内存里
- 需要状态的场景放到外部 Redis / DB / 对象存储
- 请求尽量幂等，客户端配置重试

#### 自定义健康检查

```python
@serve.deployment(
    num_replicas=2,
    max_replicas_per_node=1,
    health_check_period_s=10,
    health_check_timeout_s=30,
)
class MyDeployment:
    def check_health(self):
        if not self.db.ping():
            raise RuntimeError("DB unavailable")

    async def __call__(self, request):
        ...
```

#### 缓存 Actor Handle

不要在每次请求里做：

```python
# 错误：Head 挂掉时这里会阻塞/失败
actor = ray.get_actor("my_actor")
```

推荐在 `__init__` 里缓存 Handle：

`python/ray/serve/tests/test_gcs_failure.py:142-163`

```python
class WithDeploymentActor:
    def __init__(self):
        # 缓存 handle，GCS 挂掉期间不需要重新 get_actor
        self._actor = serve.get_deployment_actor("shared")
```

同理，`ray.get_actor`、`ray.list_named_actors`、`serve.get_app_handle` 这类实时查询都应放在启动阶段，而不是请求路径上。

#### 客户端重试

Head 挂掉瞬间，落在 Head Proxy 的在途请求可能失败。客户端需要：

- 幂等
- 重试
- 合理超时

KubeRay HA 文档建议：`serve-svc` 会把流量分到 Head 和 Worker，Head 挂掉时只有恰好落在 Head Proxy 的少量请求失败，客户端重试即可。

### 9.5 副本分散

```yaml
num_replicas: 2
max_replicas_per_node: 1
```

避免一个 Worker 挂了所有副本都没了。

### 9.6 反模式

| 反模式 | 问题 |
|---|---|
| 只用 `head-svc` 接流量 | Head 挂了服务入口就没了 |
| Head 跑 Serve 副本 | Head 重启会杀死副本 |
| 所有副本在同一节点 | 单节点故障全挂 |
| 请求路径实时 `ray.get_actor` | GCS 挂掉时请求阻塞 |
| 副本保存关键内存状态 | Head/Worker 重启后状态丢失 |
| 无客户端重试 | Head 切换瞬间请求失败 |
| 单点 Redis | Redis 挂掉 GCS 再次崩溃 |
| Worker 写死 Head Pod IP | Head 换 Pod 后无法重连 |

---

## 10. 监控、日志与验证

### 10.1 确认 Serve Service 有多个 Endpoint

```bash
kubectl describe svc my-serve-serve-svc
# 期望 Endpoints 至少包含 Head + 每个有副本的 Worker
```

### 10.2 确认 Head 不跑副本

```bash
ray.nodes()
# Head 节点 Resources 应无 CPU（num-cpus: "0"）
```

### 10.3 关键日志关键字

| 组件 | 日志 |
|---|---|
| GCS | `Using existing cluster ID from external storage` |
| GCS | `Loading node table data` |
| Raylet | `The GCS has restarted. Resubscribing to pubsub...` |
| CoreWorker | `Resubscribing to GCS tables` |
| GCS | `Node is dead because the health check failed`（旧 Head） |
| Serve Controller | `Recovered config from checkpoint` |
| Worker | `Failed to connect to GCS within ... seconds`（窗口到期） |

### 10.4 验证服务不中断

```bash
while true; do
  curl -s -o /dev/null -w "%{http_code}\n" http://my-serve-serve-svc:8000/
  sleep 0.2
done
```

预期：Head 重启期间大部分请求成功；只有恰好落在 Head Proxy 的在途请求可能失败，客户端重试即可。

### 10.5 验证节点状态

```bash
ray.nodes()
```

期望：

```text
旧 Head: Alive=False, DeathReason=UNEXPECTED_TERMINATION
新 Head: Alive=True
Worker:  Alive=True（在 600s 窗口内重连成功）
```

---

## 11. 常见误区与修正

| 误区 | 正确理解 |
|---|---|
| Ray 有传统心跳 RPC | 2.55 没有；靠 RaySyncer + HealthCheck + CheckAlive |
| 100ms 就是心跳 | 只是资源轮询周期；无变化不发消息 |
| 空闲集群也每 100ms 发心跳 | 不会；空闲时真正的周期探活是 HealthCheck |
| Head 挂了 Worker 立刻失败 | 不会；Worker 有 60s/600s 窗口 |
| 600s 是固定倒计时 | 从第一条进入重试队列的 GCS RPC 开始计时 |
| 新 GCS 会立刻把 Worker 判死 | 不会；GCS→Worker 健康检查仍成功 |
| KubeRay Worker 连的是 Head Pod IP | 通常连 Head Service DNS |
| GCS 端口是随机的 | 默认 6379；动态端口才依赖 port file |
| 内存模式也能恢复 | 不能；ClusterID 变化 + 状态丢失 |
| 开了 GCS FT 就万事大吉 | 还需要 Serve 侧流量、调度、超时、客户端重试配合 |

---

## 12. 代码索引

| 主题 | 位置 |
|---|---|
| Raylet 注册 RaySyncer | `src/ray/raylet/node_manager.cc:338-381` |
| RaySyncer 重连 | `src/ray/ray_syncer/ray_syncer.cc:99-112` |
| GCS MarkNodeHealthy | `src/ray/gcs/gcs_server.cc:596-609` |
| HealthCheck 核心逻辑 | `src/ray/gcs/gcs_health_check_manager.cc:122-225` |
| FailNode 判死 | `src/ray/gcs/gcs_health_check_manager.cc:70-80` |
| Raylet CheckAlive 自检 | `src/ray/raylet/node_manager.cc:451-483` |
| GCS HandleCheckAlive | `src/ray/gcs/gcs_node_manager.cc:155-166` |
| OnNodeFailure | `src/ray/gcs/gcs_node_manager.cc:682-719` |
| InferDeathInfo | `src/ray/gcs/gcs_node_manager.cc:541-567` |
| RemoveNodeFromCache | `src/ray/gcs/gcs_node_manager.cc:624-680` |
| GCS 监听器注册 | `src/ray/gcs/gcs_server.cc:811-862` |
| Actor 节点死亡重建 | `src/ray/gcs/actor/gcs_actor_manager.cc:1289-1385` |
| Actor 重启次数 | `src/ray/gcs/actor/gcs_actor_manager.cc:1477-1495` |
| GCS 初始化加载表 | `src/ray/gcs/gcs_init_data.cc:23-40` |
| GCS NotifyGCSRestart | `src/ray/gcs/gcs_node_manager.cc:721-750` |
| Raylet 处理 NotifyGCSRestart | `src/ray/raylet/node_manager.cc:1050-1066` |
| CoreWorker 重订阅 | `src/ray/core_worker/core_worker.cc:3439-3447` |
| GCS 客户端重订阅 | `src/ray/gcs_rpc_client/gcs_client.cc:177-183` |
| 旧 Head 标记 DEAD | `src/ray/gcs/gcs_node_manager.cc:123-146` |
| Worker 断连扫描 | `src/ray/raylet/node_manager.cc:587-613` |
| DisconnectClient | `src/ray/raylet/node_manager.cc:1404-1575` |
| Worker 检测 Raylet 死亡 | `src/ray/core_worker/core_worker.cc:779-790` |
| GCS 重连超时退出 | `src/ray/gcs_rpc_client/rpc_client.h:193-205` |
| RPC 重试队列 | `src/ray/rpc/retryable_grpc_client.cc:129-160` |
| ray start --block 看门狗 | `python/ray/scripts/scripts.py:1179-1229` |
| Serve Controller 创建 | `python/ray/serve/_private/default_impl.py:244-255` |
| Serve Controller checkpoint 恢复 | `python/ray/serve/_private/controller.py:755-800` |
| Replica Actor 创建 | `python/ray/serve/_private/deployment_state.py:1060-1090` |
| Serve GCS 故障测试 | `python/ray/serve/tests/test_gcs_failure.py:96-163` |

---

## 13. 参考资料

### 仓库内文档

- `docs/03-GCS与服务/GCS心跳机制与节点死亡检测深度分析.md`
- `docs/05-内存与OOM/GCS-Server-OOM-Kill导致Pod重启深度分析.md`
- `docs/03-GCS与服务/Ray_GCS_Redis连接验证与数据存储.md`
- `doc/source/cluster/kubernetes/user-guides/kuberay-gcs-ft.md`
- `doc/source/cluster/kubernetes/user-guides/rayservice-high-availability.md`
- `doc/source/serve/production-guide/fault-tolerance.md`

### 代码与测试

- `python/ray/tests/test_gcs_fault_tolerance.py`
- `python/ray/tests/test_gcs_ha_e2e.py`
- `python/ray/serve/tests/test_gcs_failure.py`

### 关键配置

- `src/ray/common/ray_config_def.h:450-469`
- `src/ray/common/ray_config_def.h:930-936`
- `src/ray/common/ray_config_def.h:962`
- `src/ray/common/ray_config_def.h:1049`
- `src/ray/common/ray_config_def.h:261`
- `src/ray/common/ray_config_def.h:748`
- `src/ray/common/ray_config_def.h:65`

---

## 附：一句话总结

**GCS FT 保证的是“Ray 集群元数据不丢、Worker 不掉线”；Serve 的高可用还需要 `EveryNode` Proxy + `serve-svc` 全节点流量 + Head 零副本 + 多节点副本分散 + 客户端重试一起配合。Head 挂掉后 Worker 可以继续服务，但窗口上限是 `RAY_gcs_rpc_server_reconnect_timeout_s`（KubeRay 默认 600 秒）。**
