# Ray gRPC 通信机制详解

本文档详细分析 Ray 中 GCS Server RPC 客户端连接池、ClientCallManager 与 gRPC 异步通信、异步调用与连接建立、Polling 线程与 CompletionQueue、tag/reply/call 关系与生命周期等核心机制。

---

## 目录

1. [GCS Server RPC 客户端连接池机制](#1-gcs-server-rpc-客户端连接池机制)
2. [ClientCallManager 与 gRPC 异步通信](#2-clientcallmanager-与-grpc-异步通信)
3. [gRPC 异步调用与连接建立完整流程](#3-grpc-异步调用与连接建立完整流程)
4. [Polling 线程与 CompletionQueue 机制](#4-polling-线程与-completionqueue-机制)
5. [tag/reply/call 关系与生命周期](#5-tagreplycall-关系与生命周期)

---

## 1. GCS Server RPC 客户端连接池机制

### 1.1 整体架构

GCS Server 作为 Ray 集群的中心控制面，需要**主动向 Raylet 和 CoreWorker 发起 RPC 调用**。为此它维护了两个连接池：

- **`raylet_client_pool_`** (`RayletClientPool`) — 管理到各节点 Raylet 的 gRPC 连接，按 `NodeID` 索引
- **`worker_client_pool_`** (`CoreWorkerClientPool`) — 管理到各 CoreWorker 的 gRPC 连接，按 `WorkerID` 索引，同时维护 `NodeID → WorkerID` 的二级映射

GCS **分别与 Raylet 和 CoreWorker 建立独立的 gRPC 连接**，用途不同：

| 连接对象 | gRPC Service | 主要用途 |
|---------|-------------|---------|
| **RayletClient** → Raylet | `NodeManagerService` | 请求 Worker Lease、归还 Worker、资源查询(`GetResourceLoad`)、Pin Object、Shutdown Raylet、检查 Worker 是否死亡(`IsLocalWorkerDead`)、PlacementGroup 资源预留等 |
| **CoreWorkerClient** → CoreWorker | `CoreWorkerService` | 推送 Actor 任务(`PushActorTask/PushNormalTask`)、Job 相关消息 |

### 1.2 连接池初始化 — 注册 Factory Lambda

**文件**: `src/ray/gcs/gcs_server.cc:51-77`

Pool 在构造时**不建立任何实际网络连接**，只是注册了 factory 函数：

```cpp
raylet_client_pool_(
    [this](const rpc::Address &addr) {
        return std::make_shared<ray::rpc::RayletClient>(
            addr,
            this->client_call_manager_,
            [this, addr]() {
                const NodeID node_id = NodeID::FromBinary(addr.node_id());
                auto alive_node = this->gcs_node_manager_->GetAliveNode(node_id);
                if (!alive_node.has_value()) {
                    this->raylet_client_pool_.Disconnect(node_id);
                }
            });
    })

worker_client_pool_(
    [this](const rpc::Address &addr) {
        return std::make_shared<rpc::CoreWorkerClient>(
            addr,
            this->client_call_manager_,
            [this, addr]() {
                const NodeID node_id = NodeID::FromBinary(addr.node_id());
                const WorkerID worker_id = WorkerID::FromBinary(addr.worker_id());
                auto alive_node = this->gcs_node_manager_->GetAliveNode(node_id);
                if (!alive_node.has_value()) {
                    this->worker_client_pool_.Disconnect(worker_id);
                    return;
                }
                auto &node_info = alive_node.value();
                auto remote_address = rpc::RayletClientPool::GenerateRayletAddress(
                    node_id,
                    node_info->node_manager_address(),
                    node_info->node_manager_port());
                auto raylet_client =
                    this->raylet_client_pool_.GetOrConnectByAddress(remote_address);
                raylet_client->IsLocalWorkerDead(
                    worker_id,
                    [this, worker_id, node_id](const Status &status, const auto &reply) {
                        if (!status.ok()) {
                            RAY_LOG(INFO).WithField(worker_id).WithField(node_id)
                                << "Failed to check if worker is dead on request to raylet";
                            return;
                        }
                        if (reply.is_dead()) {
                            this->worker_client_pool_.Disconnect(worker_id);
                        }
                    });
            });
    })
```

**Factory 类型定义**：

```cpp
// raylet_client_pool.h:30
using RayletClientFactoryFn =
    std::function<std::shared_ptr<ray::RayletClientInterface>(const rpc::Address &)>;

// core_worker_client_pool.h:27
using CoreWorkerClientFactoryFn =
    std::function<std::shared_ptr<CoreWorkerClientInterface>(const rpc::Address &)>;
```

此时 Pool 内部的 `client_map_` / `worker_client_map_` 都是空的，**没有任何 gRPC channel 被创建**。两个 pool 的 factory 共享同一个 `client_call_manager_` 实例。

### 1.3 RayletClientPool — 获取/创建连接

**文件**: `src/ray/raylet_rpc_client/raylet_client_pool.cc:84-97`

```cpp
std::shared_ptr<ray::RayletClientInterface> RayletClientPool::GetOrConnectByAddress(
    const rpc::Address &address) {
  RAY_CHECK(address.node_id() != "");
  absl::MutexLock lock(&mu_);
  auto node_id = NodeID::FromBinary(address.node_id());
  auto it = client_map_.find(node_id);
  if (it != client_map_.end()) {
    RAY_CHECK(it->second != nullptr);
    return it->second;
  }
  auto connection = client_factory_(address);
  client_map_[node_id] = connection;
  RAY_LOG(DEBUG) << "Connected to raylet " << node_id << " at "
                 << BuildAddress(address.ip_address(), address.port());
  RAY_CHECK(connection != nullptr);
  return connection;
}
```

**流程**：
1. 加锁 `mu_`
2. 在 `client_map_` 中查找 `node_id`
3. 找到 → 返回已有的 `shared_ptr<RayletClient>`
4. 没找到 → 调用 `client_factory_(address)` 创建新的 `RayletClient`
5. 存入 `client_map_[node_id]`，返回

**触发连接的时机**：
- 节点注册时（`InstallEventListeners` 中的 `AddNodeAddedListener`）
- GCS 启动加载历史节点时（`InitGcsHealthCheckManager`）
- 定期拉取资源负载时（`InitGcsResourceManager` 中的 `RunFnPeriodically`）
- Actor 调度/PlacementGroup 调度时

**辅助方法**：

```cpp
// raylet_client_pool.cc:107-113
rpc::Address RayletClientPool::GenerateRayletAddress(const NodeID &node_id,
                                                     const std::string &ip_address,
                                                     int port) {
  rpc::Address address;
  address.set_ip_address(ip_address);
  address.set_port(port);
  address.set_node_id(node_id.Binary());
  return address;
}
```

### 1.4 CoreWorkerClientPool — 获取/创建连接

**文件**: `src/ray/core_worker_rpc_client/core_worker_client_pool.cc:79-97`

```cpp
std::shared_ptr<CoreWorkerClientInterface> CoreWorkerClientPool::GetOrConnect(
    const Address &addr_proto) {
  RAY_CHECK_NE(addr_proto.worker_id(), "");
  absl::MutexLock lock(&mu_);

  RemoveIdleClients();

  CoreWorkerClientEntry entry;
  auto node_id = NodeID::FromBinary(addr_proto.node_id());
  auto worker_id = WorkerID::FromBinary(addr_proto.worker_id());
  auto it = worker_client_map_.find(worker_id);
  if (it != worker_client_map_.end()) {
    entry = *it->second;
    client_list_.erase(it->second);
  } else {
    entry = CoreWorkerClientEntry(
        worker_id, node_id, core_worker_client_factory_(addr_proto));
  }
  client_list_.emplace_front(entry);
  worker_client_map_[worker_id] = client_list_.begin();
  node_clients_map_[node_id][worker_id] = client_list_.begin();

  RAY_LOG(DEBUG) << "Connected to worker " << worker_id << " with address "
                 << BuildAddress(addr_proto.ip_address(), addr_proto.port());
  return entry.core_worker_client_;
}
```

**流程**：
1. 加锁 `mu_`，先调用 `RemoveIdleClients()` 清理空闲连接
2. 在 `worker_client_map_` 中查找 `worker_id`
3. 找到 → 将其移到 `client_list_` 前端（LRU 更新），返回
4. 没找到 → 调用 `core_worker_client_factory_(addr_proto)` 创建新的 `CoreWorkerClient`
5. 插入 `client_list_` 前端、`worker_client_map_` 和 `node_clients_map_`

**触发连接的时机**：主要是 Actor 调度相关 — `GcsActorScheduler` 在 actor 创建成功后需要通过 `worker_client_pool_` 向目标 worker 推送任务（`PushTask`），以及 Job 提交时向 driver worker 发消息。

### 1.5 CoreWorkerClientPool — LRU 空闲清理

**文件**: `src/ray/core_worker_rpc_client/core_worker_client_pool.cc:99-119`

```cpp
void CoreWorkerClientPool::RemoveIdleClients() {
  while (!client_list_.empty()) {
    auto worker_id = client_list_.back().worker_id_;
    auto node_id = client_list_.back().node_id_;
    if (client_list_.back().core_worker_client_->IsIdleAfterRPCs()) {
      worker_client_map_.erase(worker_id);
      EraseFromNodeClientMap(node_id, worker_id);
      client_list_.pop_back();
      RAY_LOG(DEBUG) << "Remove idle client to worker " << worker_id
                     << " , num of clients is now " << client_list_.size();
    } else {
      auto entry = client_list_.back();
      client_list_.pop_back();
      client_list_.emplace_front(entry);
      worker_client_map_[worker_id] = client_list_.begin();
      node_clients_map_[node_id][worker_id] = client_list_.begin();
      break;
    }
  }
}
```

`client_list_` 按 LRU 排序（最近访问在前），从尾部检查：如果 `IsIdleAfterRPCs()` 为 true，移除；遇到第一个非 idle 的，移到前端后停止。

### 1.6 连接断开与清理

**主动断开**：

```cpp
// RayletClientPool::Disconnect (raylet_client_pool.cc:99-105)
void RayletClientPool::Disconnect(ray::NodeID id) {
  absl::MutexLock lock(&mu_);
  auto it = client_map_.find(id);
  if (it == client_map_.end()) { return; }
  client_map_.erase(it);
}

// CoreWorkerClientPool::Disconnect(WorkerID) (core_worker_client_pool.cc:121-130)
void CoreWorkerClientPool::Disconnect(const WorkerID &id) {
  absl::MutexLock lock(&mu_);
  auto it = worker_client_map_.find(id);
  if (it == worker_client_map_.end()) { return; }
  EraseFromNodeClientMap(it->second->node_id_, id);
  client_list_.erase(it->second);
  worker_client_map_.erase(it);
}

// CoreWorkerClientPool::Disconnect(NodeID) (core_worker_client_pool.cc:132-144)
void CoreWorkerClientPool::Disconnect(const NodeID &node_id) {
  absl::MutexLock lock(&mu_);
  auto node_client_map_it = node_clients_map_.find(node_id);
  if (node_client_map_it == node_clients_map_.end()) { return; }
  auto &node_worker_id_client_map = node_client_map_it->second;
  for (auto &[worker_id, client_iterator] : node_worker_id_client_map) {
    worker_client_map_.erase(worker_id);
    client_list_.erase(client_iterator);
  }
  node_clients_map_.erase(node_client_map_it);
}
```

**触发断开的时机**：

| 事件 | 代码位置 | 操作 |
|------|---------|------|
| 节点死亡 | `gcs_server.cc` `AddNodeRemovedListener` | `raylet_client_pool_.Disconnect(node_id)` + `worker_client_pool_.Disconnect(node_id)` |
| Worker 死亡 | `gcs_server.cc` `AddWorkerDeadListener` | `worker_client_pool_.Disconnect(worker_id)` |
| Raylet 不可用超时 | Raylet 不可用回调 | 检查节点存活 → 不存活则 `raylet_client_pool_.Disconnect(node_id)` |
| CoreWorker 不可用超时 | CoreWorker 不可用回调 | 检查节点存活 → 不存活则 `Disconnect(worker_id)`；节点存活则问 Raylet `IsLocalWorkerDead` → 已死则 `Disconnect(worker_id)` |

**引用计数保活**：Pool 中移除连接后，如果有其他代码仍持有 `shared_ptr`，gRPC 连接会继续保持，直到最后一个 `shared_ptr` 释放时连接才真正关闭。

### 1.7 不可用超时回调详细逻辑

#### Raylet 不可用回调（GCS 侧）

```cpp
[this, addr]() {
    const NodeID node_id = NodeID::FromBinary(addr.node_id());
    auto alive_node = this->gcs_node_manager_->GetAliveNode(node_id);
    if (!alive_node.has_value()) {
        this->raylet_client_pool_.Disconnect(node_id);
    }
}
```

- 检查节点是否还活着 → 如果节点已死，从 `raylet_client_pool_` 中断开连接

#### CoreWorker 不可用回调（GCS 侧）

```cpp
[this, addr]() {
    const NodeID node_id = NodeID::FromBinary(addr.node_id());
    const WorkerID worker_id = WorkerID::FromBinary(addr.worker_id());
    auto alive_node = this->gcs_node_manager_->GetAliveNode(node_id);
    if (!alive_node.has_value()) {
        this->worker_client_pool_.Disconnect(worker_id);
        return;
    }
    // 节点还活着 → 问对应 Raylet 该 worker 是否已死
    auto &node_info = alive_node.value();
    auto remote_address = rpc::RayletClientPool::GenerateRayletAddress(...);
    auto raylet_client = this->raylet_client_pool_.GetOrConnectByAddress(remote_address);
    raylet_client->IsLocalWorkerDead(worker_id, [this, worker_id](...) {
        if (reply.is_dead()) {
            this->worker_client_pool_.Disconnect(worker_id);
        }
    });
}
```

1. 先检查节点是否活着 → 如果节点已死，从 `worker_client_pool_` 中断开
2. 如果节点还活着 → 通过 `raylet_client_pool_` 获取对应 Raylet 的连接，调用 `IsLocalWorkerDead()` 检查 worker 是否已死 → 如果已死，从 `worker_client_pool_` 中断开

**注意**：CoreWorker 不可用回调中可能触发 Raylet 连接的创建（如果之前没有到该 Raylet 的连接），体现了两个 pool 之间的协作关系。

### 1.8 连接池结构总览

```
GCS Server
  │
  ├── raylet_client_pool_ (RayletClientPool)
  │     ├── client_factory_ = lambda
  │     │   └── [this](addr) {
  │     │        return RayletClient(addr, this->client_call_manager_, unavailable_cb);
  │     │      }
  │     └── client_map_: NodeID → shared_ptr<RayletClient>
  │          └── 每个 RayletClient:
  │               ├── GrpcClient<NodeManagerService>  ←→  Raylet (NodeManagerService gRPC server)
  │               └── RetryableGrpcClient (自动重试)
  │
  └── worker_client_pool_ (CoreWorkerClientPool)
        ├── core_worker_client_factory_ = lambda
        │   └── [this](addr) {
        │        return CoreWorkerClient(addr, this->client_call_manager_, unavailable_cb);
        │      }
        ├── client_list_: LRU list of CoreWorkerClientEntry
        ├── worker_client_map_: WorkerID → iterator
        ├── node_clients_map_: NodeID → {WorkerID → iterator}
        │
        │   每个 CoreWorkerClient:
        │     ├── GrpcClient<CoreWorkerService>  ←→  CoreWorker (CoreWorkerService gRPC server)
        │     └── RetryableGrpcClient (自动重试)
        │
        ├── GetOrConnect(addr) → 按需懒连接 + LRU 空闲清理
        ├── Disconnect(worker_id) → worker 死亡时清理
        └── Disconnect(node_id) → 节点死亡时清理该节点所有 worker 连接
```

---

## 2. ClientCallManager 与 gRPC 异步通信

### 2.1 ClientCallManager 的含义与作用

`client_call_manager_` 是 `ClientCallManager` 类的实例，定义在 `src/ray/rpc/client_call.h`，是 **Ray 中所有 gRPC 客户端发出请求的统一调度中心**。

GCS Server 在构造时创建它（`gcs_server.cc:47-50`）：

```cpp
client_call_manager_(main_service,                    // 主事件循环 (instrumented_io_context)
                     /*record_stats=*/true,            // 记录 RPC 统计
                     config.node_ip_address,           // 本机 IP
                     ClusterID::Nil(),                 // 初始无 cluster_id
                     RayConfig::instance()
                       .gcs_server_rpc_client_thread_num())  // polling 线程数
```

### 2.2 ClientCallManager 核心职责

#### 2.2.1 异步 gRPC 请求的创建与发送（CreateCall）

**文件**: `src/ray/rpc/client_call.h:296-327`

```cpp
template <class GrpcService, class Request, class Reply>
std::shared_ptr<ClientCall> CreateCall(
    typename GrpcService::Stub &stub,
    const PrepareAsyncFunction<GrpcService, Request, Reply> prepare_async_function,
    const Request &request,
    const ClientCallback<Reply> &callback,
    std::string call_name,
    int64_t method_timeout_ms = -1) {
  auto stats_handle = main_service_.stats()->RecordStart(std::move(call_name));
  if (method_timeout_ms == -1) {
    method_timeout_ms = call_timeout_ms_;
  }

  auto call = std::make_shared<ClientCallImpl<Reply>>(
      callback, cluster_id_, std::move(stats_handle), record_stats_, method_timeout_ms);
  call->response_reader_ = (stub.*prepare_async_function)(
      &call->context_, request, cqs_[rr_index_++ % num_threads_].get());
  call->response_reader_->StartCall();
  auto tag = new ClientCallTag(call);
  call->response_reader_->Finish(
      &call->reply_, &call->status_, static_cast<void *>(tag));
  return call;
}
```

这个方法做了以下事情：
1. **创建 `ClientCallImpl<Reply>`** — 封装了回调函数、gRPC `ClientContext`（含 deadline/cluster_id 元数据）、stats 句柄
2. **调用 stub 的 `PrepareAsync*` 方法** — 将请求注册到某个 `CompletionQueue`（round-robin 选择）
3. **`StartCall()` + `Finish()`** — 启动异步调用，把 `ClientCallTag` 作为 completion tag 绑定

#### 2.2.2 多线程轮询 CompletionQueue

构造函数中根据 `num_threads` 创建多个 `grpc::CompletionQueue`，每个 CQ 有一个专属线程轮询：

```cpp
// client_call.h:271-275
for (int i = 0; i < num_threads_; i++) {
    cqs_.emplace_back(std::make_unique<grpc::CompletionQueue>());
    polling_threads_.emplace_back(
        &ClientCallManager::PollEventsFromCompletionQueue, this, i);
}
```

#### 2.2.3 共享给所有 gRPC 客户端

`ClientCallManager` 被所有通过它创建的 `GrpcClient` 共享引用：

```
GCS Server
  └── client_call_manager_ (1个实例)
        ├── RayletClient → GrpcClient<NodeManagerService>(..., client_call_manager_)
        │                    └── CallMethod → client_call_manager_.CreateCall(...)
        ├── CoreWorkerClient → GrpcClient<CoreWorkerService>(..., client_call_manager_)
        │                       └── CallMethod → client_call_manager_.CreateCall(...)
        └── 所有 RPC 客户端共享同一组 CompletionQueue 和 polling 线程
```

### 2.3 client_call_manager_ 在调用链中的传递

从 GCS Server 的 `client_call_manager_` 出发，追踪到最终 gRPC 网络层：

#### 第一层：GCS Server 构造 ClientCallManager

```cpp
client_call_manager_(main_service,
                     /*record_stats=*/true,
                     config.node_ip_address,
                     ClusterID::Nil(),
                     RayConfig::instance().gcs_server_rpc_client_thread_num())
```

#### 第二层：factory lambda 传递引用

```
GCS Server
  │
  ├── raylet_client_pool_ 构造时捕获 this
  │     └── factory lambda 内: this->client_call_manager_
  │
  └── worker_client_pool_ 构造时捕获 this
        └── factory lambda 内: this->client_call_manager_
```

#### 第三层：RayletClient / CoreWorkerClient 构造时接收引用

**RayletClient**（`raylet_client.cc:28-42`）：

```cpp
RayletClient::RayletClient(const rpc::Address &address,
                            rpc::ClientCallManager &client_call_manager,
                            std::function<void()> raylet_unavailable_timeout_callback)
    : grpc_client_(std::make_shared<rpc::GrpcClient<rpc::NodeManagerService>>(
          address.ip_address(),
          address.port(),
          client_call_manager)),           // ← 传递给 GrpcClient
      retryable_grpc_client_(rpc::RetryableGrpcClient::Create(
          grpc_client_->Channel(),         // ← 共享 channel
          client_call_manager.GetMainService(),  // ← 取 main_service 给 RetryableGrpcClient
          /*max_pending_requests_bytes=*/UINT64_MAX,
          RayConfig::instance().grpc_client_check_connection_status_interval_milliseconds(),
          RayConfig::instance().raylet_rpc_server_reconnect_timeout_base_s(),
          RayConfig::instance().raylet_rpc_server_reconnect_timeout_max_s(),
          std::move(raylet_unavailable_timeout_callback),
          std::string("Raylet ") + address.ip_address()))
```

**`client_call_manager` 流向两个地方**：
1. `GrpcClient<NodeManagerService>(ip, port, client_call_manager)` → 存储为引用
2. `client_call_manager.GetMainService()` → 取出 `instrumented_io_context` 传给 `RetryableGrpcClient`

**CoreWorkerClient**（`core_worker_client.cc:24-40`）：结构完全一致，只是 Service 换成 `CoreWorkerService`，超时配置换成 `core_worker_rpc_server_reconnect_timeout_*`。

#### 第四层：GrpcClient 构造 — 存储引用 + 创建 channel + stub

**文件**: `src/ray/rpc/grpc_client.h:88-97`

```cpp
GrpcClient(const std::string &address,
           const int port,
           ClientCallManager &call_manager,
           grpc::ChannelArguments channel_arguments = CreateDefaultChannelArguments())
    : client_call_manager_(call_manager),          // ← 存储引用
      channel_(BuildChannel(address, port, ...)),   // ← 创建 gRPC channel
      stub_(GrpcService::NewStub(channel_)),        // ← 创建 stub
      skip_testing_intra_node_rpc_failure_(...)
```

#### 第五层：发 RPC 时 — 调用 client_call_manager_.CreateCall

**GrpcClient::CallMethod**（`grpc_client.h:108-149`）：

```cpp
template <class Request, class Reply>
void CallMethod(
    const PrepareAsyncFunction<GrpcService, Request, Reply> prepare_async_function,
    const Request &request,
    const ClientCallback<Reply> &callback,
    std::string call_name = "UNKNOWN_RPC",
    int64_t method_timeout_ms = -1) {
  // RPC chaos/fault injection for testing
  testing::RpcFailure failure = skip_testing_intra_node_rpc_failure_
                                    ? testing::RpcFailure::None
                                    : testing::GetRpcFailure(call_name);
  if (failure == testing::RpcFailure::Request) {
    // 模拟请求阶段失败
    client_call_manager_.GetMainService().post(
        [callback]() { callback(Status::RpcError(...), Reply()); }, "RpcChaos");
  } else if (failure == testing::RpcFailure::Response) {
    // 模拟响应阶段失败
    client_call_manager_.CreateCall<GrpcService, Request, Reply>(
        *stub_, prepare_async_function, request,
        [callback](const Status &status, const Reply &) {
            callback(Status::RpcError(...), Reply());
        }, std::move(call_name), method_timeout_ms);
  } else {
    // 正常路径
    auto call = client_call_manager_.CreateCall<GrpcService, Request, Reply>(
        *stub_, prepare_async_function, request,
        callback, std::move(call_name), method_timeout_ms);
    RAY_CHECK(call != nullptr);
  }
  call_method_invoked_.store(true);
}
```

### 2.4 ClientCallManager 关键设计总结

| 设计点 | 说明 |
|--------|------|
| **多 CQ + 多线程** | `num_threads` 个 CompletionQueue，每个一个 polling 线程，round-robin 分发请求，提高并发吞吐 |
| **回调投递到 main_service_** | gRPC I/O 与业务逻辑解耦，回调在主事件循环串行执行，线程安全 |
| **所有客户端共享** | GCS Server 中一个 `client_call_manager_` 实例被所有 `RayletClient`/`CoreWorkerClient` 共享，统一管理网络 I/O |
| **超时与 Cluster ID** | 每个 `ClientCallImpl` 的 `ClientContext` 可设 deadline 和 cluster_id 元数据，用于多集群隔离 |
| **统计记录** | `record_stats=true` 时记录每个 RPC 的延迟和失败计数 |
| **线程数配置** | 通过 `gcs_server_rpc_client_thread_num` 控制 |

---

## 3. gRPC 异步调用与连接建立完整流程

### 3.1 阶段一：Pool 初始化 — 无真实连接

GCS Server 构造 `raylet_client_pool_` 和 `worker_client_pool_` 时，只注册了 factory lambda。此时没有任何 gRPC channel 被创建，没有任何 TCP 连接。

### 3.2 阶段二：首次 GetOrConnect* — 惰性创建客户端对象

以 Raylet 为例，当 GCS 需要向某个 Raylet 发 RPC 时：

```cpp
auto remote_address = rpc::RayletClientPool::GenerateRayletAddress(
    node_id, node->node_manager_address(), node->node_manager_port());
auto raylet_client = raylet_client_pool_.GetOrConnectByAddress(remote_address);
```

`GetOrConnectByAddress` 在 `client_map_` 中找不到已有连接时，调用 `client_factory_(address)` 创建新的 `RayletClient`。

### 3.3 阶段三：RayletClient/CoreWorkerClient 构造 — 创建 gRPC Channel

**RayletClient 构造**（`raylet_client.cc:28-42`）：

```cpp
RayletClient(address, client_call_manager, unavailable_timeout_callback)
    : grpc_client_(std::make_shared<GrpcClient<NodeManagerService>>(
          address.ip_address(), address.port(), client_call_manager))
    , retryable_grpc_client_(RetryableGrpcClient::Create(
          grpc_client_->Channel(),
          client_call_manager.GetMainService(), ...))
```

**GrpcClient 构造**（`grpc_client.h:88-97`）：

```cpp
GrpcClient(address, port, call_manager, channel_arguments = CreateDefaultChannelArguments())
    : client_call_manager_(call_manager)
    , channel_(BuildChannel(address, port, std::move(channel_arguments)))
    , stub_(GrpcService::NewStub(channel_))
```

### 3.4 阶段四：BuildChannel — gRPC Channel 的真正创建

**文件**: `src/ray/rpc/grpc_client.cc:22-77`

```cpp
std::shared_ptr<grpc::Channel> BuildChannel(
    const std::string &address,
    int port,
    std::optional<grpc::ChannelArguments> arguments) {
  if (!arguments.has_value()) {
    arguments = grpc::ChannelArguments();
  }

  // 设置 channel 参数
  arguments->SetInt(GRPC_ARG_ENABLE_HTTP_PROXY,
                    ::RayConfig::instance().grpc_enable_http_proxy() ? 1 : 0);
  arguments->SetMaxSendMessageSize(::RayConfig::instance().max_grpc_message_size());
  arguments->SetMaxReceiveMessageSize(::RayConfig::instance().max_grpc_message_size());
  arguments->SetInt(GRPC_ARG_HTTP2_WRITE_BUFFER_SIZE,
                    ::RayConfig::instance().grpc_stream_buffer_size());

  // 选择凭证：TLS 或 Insecure
  std::shared_ptr<grpc::ChannelCredentials> channel_creds;
  if (::RayConfig::instance().USE_TLS()) {
    // 读取证书，创建 SslCredentials
    grpc::SslCredentialsOptions ssl_opts;
    ssl_opts.pem_root_certs = cacert;
    ssl_opts.pem_private_key = private_key;
    ssl_opts.pem_cert_chain = server_cert_chain;
    channel_creds = grpc::SslCredentials(ssl_opts);
  } else {
    channel_creds = grpc::InsecureChannelCredentials();
  }

  std::string target_address = BuildAddress(address, port);

  // Token auth 模式: 创建带 interceptor 的 channel
  if (GetAuthenticationMode() == AuthenticationMode::TOKEN) {
    return grpc::experimental::CreateCustomChannelWithInterceptors(
        target_address, channel_creds, *arguments,
        CreateTokenAuthInterceptorFactories());
  } else {
    return grpc::CreateCustomChannel(target_address, channel_creds, *arguments);
  }
}
```

**CreateDefaultChannelArguments**（`src/ray/common/grpc_util.h:230-244`）：

```cpp
inline grpc::ChannelArguments CreateDefaultChannelArguments() {
  grpc::ChannelArguments arguments;
  if (::RayConfig::instance().grpc_client_keepalive_time_ms() > 0) {
    arguments.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS,
                     ::RayConfig::instance().grpc_client_keepalive_time_ms());
    arguments.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS,
                     ::RayConfig::instance().grpc_client_keepalive_timeout_ms());
    arguments.SetInt(GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA, 0);
  }
  arguments.SetInt(GRPC_ARG_CLIENT_IDLE_TIMEOUT_MS,
                   ::RayConfig::instance().grpc_client_idle_timeout_ms());
  return arguments;
}
```

**关键点**：gRPC 的 `CreateCustomChannel` 返回的 channel 是**惰性连接**的——此时并没有建立 TCP 连接，channel 处于 `GRPC_CHANNEL_IDLE` 状态。真正的 TCP 连接会在**首次 RPC 调用时**触发。

### 3.5 阶段五：首次 RPC 调用 — TCP 连接真正建立 + 请求注册到 CompletionQueue

当 GCS 首次调用 `raylet_client->RequestWorkerLease(...)` 时的完整调用链：

```
调用入口:
  raylet_client->RequestWorkerLease(lease_spec, grant_or_reject, callback, ...)
    │
    ▼
RayletClient::RequestWorkerLease (raylet_client.cc:52-61):
  rpc::RequestWorkerLeaseRequest request;
  request.mutable_lease_spec()->CopyFrom(lease_spec);
  request.set_grant_or_reject(grant_or_reject);
  ...
  INVOKE_RETRYABLE_RPC_CALL(retryable_grpc_client_,
                            NodeManagerService,
                            RequestWorkerLease,
                            request, callback, grpc_client_, -1);
    │
    ▼ 展开宏:
retryable_grpc_client_->CallMethod<NodeManagerService,
                                   RequestWorkerLeaseRequest,
                                   RequestWorkerLeaseReply>(
    &NodeManagerService::Stub::PrepareAsyncRequestWorkerLease,
    grpc_client_,
    "NodeManagerService.grpc_client.RequestWorkerLease",
    std::move(request),
    callback,
    -1)
    │
    ▼
RetryableGrpcClient::CallMethod (retryable_grpc_client.h:263-275):
  num_active_requests_++;
  RetryableGrpcRequest::Create(weak_from_this(),
                               std::move(prepare_async_function),
                               std::move(grpc_client),
                               std::move(call_name),
                               std::move(request),
                               std::move(callback),
                               timeout_ms)
      ->CallMethod();
    │
    ▼
RetryableGrpcRequest::Create 内部构建的 executor lambda:
  grpc_client->CallMethod<Request, Reply>(
      prepare_async_function,
      request,
      [weak_retryable_grpc_client, retryable_grpc_request, callback](
          const ray::Status &status, Reply &&reply) {
          auto retryable_grpc_client = weak_retryable_grpc_client.lock();
          if (status.ok() || !IsGrpcRetryableStatus(status) || !retryable_grpc_client) {
              callback(status, std::move(reply));
              retryable_grpc_client->num_active_requests_--;
          } else {
              retryable_grpc_client->Retry(retryable_grpc_request);
          }
      },
      call_name,
      timeout_ms);
    │
    ▼
GrpcClient::CallMethod (grpc_client.h:108-149):
  auto call = client_call_manager_.CreateCall<NodeManagerService, ...>(
      *stub_,
      &NodeManagerService::Stub::PrepareAsyncRequestWorkerLease,
      request,
      callback,
      "NodeManagerService.grpc_client.RequestWorkerLease",
      -1);
```

**`ClientCallManager::CreateCall` 的具体步骤**（`client_call.h:296-327`）：

```cpp
// 1. 记录 RPC 开始统计
auto stats_handle = main_service_.stats()->RecordStart(call_name);

// 2. 创建 ClientCallImpl<Reply> 对象
auto call = std::make_shared<ClientCallImpl<RequestWorkerLeaseReply>>(
    callback, cluster_id_, std::move(stats_handle), record_stats_, method_timeout_ms);
  // → 设置 ClientContext deadline
  // → 添加 cluster_id 元数据到 context

// 3. 将请求注册到 CompletionQueue (round-robin 选择 CQ)
call->response_reader_ =
    (stub.*prepare_async_function)(
        &call->context_,                         // ← grpc::ClientContext
        request,                                 // ← protobuf request
        cqs_[rr_index_++ % num_threads_].get()); // ← 选择一个 CQ
  // 【此时 gRPC 内部: channel IDLE → CONNECTING → 开始 TCP 连接 + HTTP2 握手】

// 4. 启动异步调用
call->response_reader_->StartCall();

// 5. 注册 Finish 到 CompletionQueue
auto tag = new ClientCallTag(call);
call->response_reader_->Finish(
    &call->reply_,              // ← 接收 response 的 buffer
    &call->status_,             // ← 接收 gRPC status
    static_cast<void *>(tag));  // ← tag 绑定到 CompletionQueue

return call;
```

**PrepareAsync* → StartCall → Finish 三步含义**：

| 步骤 | 做了什么 | 是否有网络 I/O |
|------|---------|:---:|
| `PrepareAsync*` | 创建异步调用，绑定 CQ，还不发请求 | ❌ |
| `StartCall()` | 真正发起：TCP 连接 + HTTP2 握手 + 发送请求 | ✅ |
| `Finish()` | 注册完成事件：响应到达时 tag 出现在 CQ 中 | ✅ |

### 3.6 RetryableGrpcClient::CallMethod 的详细逻辑

**文件**: `src/ray/rpc/retryable_grpc_client.h:263-275`

```cpp
template <typename Service, typename Request, typename Reply>
void RetryableGrpcClient::CallMethod(
    PrepareAsyncFunction<Service, Request, Reply> prepare_async_function,
    std::shared_ptr<GrpcClient<Service>> grpc_client,
    std::string call_name,
    Request request,
    ClientCallback<Reply> callback,
    int64_t timeout_ms) {
  num_active_requests_++;
  RetryableGrpcRequest::Create(weak_from_this(),
                               std::move(prepare_async_function),
                               std::move(grpc_client),
                               std::move(call_name),
                               std::move(request),
                               std::move(callback),
                               timeout_ms)
      ->CallMethod();
}
```

### 3.7 RetryableGrpcRequest::CallMethod — executor 的调用

**文件**: `src/ray/rpc/retryable_grpc_client.h:169-172`

```cpp
void CallMethod() { executor_(shared_from_this()); }
```

`executor_` 是在 `RetryableGrpcRequest::Create` 中构建的 lambda（`retryable_grpc_client.h:196-216`）：

```cpp
auto executor = [
    weak_retryable_grpc_client,
    prepare_async_function = std::move(prepare_async_function),
    grpc_client = std::move(grpc_client),          // ← GrpcClient<Service> 的 shared_ptr
    call_name = std::move(call_name),
    request = std::move(request),
    callback                                        // ← 被包装过的 callback
](std::shared_ptr<RetryableGrpcClient::RetryableGrpcRequest> retryable_grpc_request) {

    // ★ 调用 GrpcClient::CallMethod
    grpc_client->template CallMethod<Request, Reply>(
        prepare_async_function,
        request,
        [weak_retryable_grpc_client, retryable_grpc_request, callback](
            const ray::Status &status, Reply &&reply) {
            auto retryable_grpc_client = weak_retryable_grpc_client.lock();
            if (status.ok() || !IsGrpcRetryableStatus(status) || !retryable_grpc_client) {
                callback(status, std::move(reply));   // 成功或不可重试 → 原始回调
                retryable_grpc_client->num_active_requests_--;
                return;
            }
            retryable_grpc_client->Retry(retryable_grpc_request);  // 可重试 → 加入重试队列
        },
        call_name,
        retryable_grpc_request->GetTimeoutMs());
};
```

调用链路：

```
RetryableGrpcRequest::CallMethod()
  → executor_(shared_from_this())
    → grpc_client->CallMethod<Request, Reply>(...)       // GrpcClient::CallMethod
      → client_call_manager_.CreateCall<Service, ...>(...)  // 注册到 CompletionQueue
```

**关键点**：`executor_` 通过类型擦除（`std::function`）隐藏了模板参数，但内部实际调用的是 `GrpcClient<Service>::CallMethod`。而 `CallMethod` 传入的 callback 又被 `RetryableGrpcClient` 包装了一层——成功时调原始回调，瞬态失败时调 `Retry()` 加入重试队列。

### 3.8 RetryableGrpcClient::Retry — 失败重试

**文件**: `src/ray/rpc/retryable_grpc_client.cc:136-176`

```cpp
void RetryableGrpcClient::Retry(std::shared_ptr<RetryableGrpcRequest> request) {
  const auto now = absl::Now();
  const auto request_bytes = request->GetRequestBytes();
  auto self = shared_from_this();

  if (pending_requests_bytes_ + request_bytes > max_pending_requests_bytes_) {
    // 背压：阻塞当前线程，轮询 channel 状态直到恢复
    RAY_LOG(WARNING) << "Pending queue for failed request has reached the limit. "
                     << "Blocking the current thread until network is recovered";
    RAY_CHECK(server_unavailable_timeout_time_.has_value());
    while (server_unavailable_timeout_time_.has_value()) {
      std::this_thread::sleep_for(
          std::chrono::milliseconds(check_channel_status_interval_milliseconds_));
      if (self.use_count() == 2) { break; }
      CheckChannelStatus(false);
    }
    request->CallMethod();
    return;
  }

  // 正常路径：加入 pending 队列
  pending_requests_bytes_ += request_bytes;
  const auto timeout = request->GetTimeoutMs() == -1
                           ? absl::InfiniteFuture()
                           : now + absl::Milliseconds(request->GetTimeoutMs());
  pending_requests_.emplace(timeout, std::move(request));

  if (!server_unavailable_timeout_time_.has_value()) {
    // 第一个需要重试的请求 → 启动定时器
    server_unavailable_timeout_time_ =
        now + absl::Seconds(server_reconnect_timeout_base_seconds_);
    SetupCheckTimer();
  }
}
```

### 3.9 CheckChannelStatus — 定期检查 channel 状态

**文件**: `src/ray/rpc/retryable_grpc_client.cc:59-106`

```cpp
void RetryableGrpcClient::CheckChannelStatus(bool reset_timer) {
  const auto now = absl::Now();

  // 1. 清理已超时的 pending requests
  while (!pending_requests_.empty()) {
    auto iter = pending_requests_.begin();
    if (iter->first > now) { break; }
    iter->second->Fail(ray::Status::TimedOut(
        absl::StrFormat("Timed out while waiting for %s to become available.",
                        server_name_)));
    pending_requests_bytes_ -= iter->second->GetRequestBytes();
    pending_requests_.erase(iter);
  }

  if (pending_requests_.empty()) {
    server_unavailable_timeout_time_ = std::nullopt;
    return;
  }

  // 2. 检查 channel 状态
  auto status = channel_->GetState(false);

  switch (status) {
  case GRPC_CHANNEL_TRANSIENT_FAILURE:
  case GRPC_CHANNEL_CONNECTING: {
    // 不可用超时 → 调用 server_unavailable_timeout_callback_
    if (server_unavailable_timeout_time_ < now) {
      RAY_LOG(WARNING) << server_name_ << " has been unavailable for more than "
                       << ExponentialBackoff::GetBackoffMs(...) / 1000
                       << " seconds";
      server_unavailable_timeout_callback_();
      // 指数退避重置超时时间
      attempt_number_++;
      server_unavailable_timeout_time_ =
          now + absl::Seconds(ExponentialBackoff::GetBackoffMs(...) / 1000);
    }
    if (reset_timer) { SetupCheckTimer(); }
    break;
  }
  case GRPC_CHANNEL_READY:
  case GRPC_CHANNEL_IDLE: {
    // Channel 恢复！重发所有 pending requests
    server_unavailable_timeout_time_ = std::nullopt;
    while (!pending_requests_.empty()) {
      pending_requests_.begin()->second->CallMethod();
      pending_requests_.erase(pending_requests_.begin());
    }
    pending_requests_bytes_ = 0;
    attempt_number_ = 0;
    break;
  }
  case GRPC_CHANNEL_SHUTDOWN: {
    RAY_LOG(FATAL) << "Channel should never go to this status.";
    break;
  }
  }
}
```

### 3.10 完整时序图

```
GCS 主线程                     Polling 线程 (N个)              gRPC 网络
    │                                │                            │
    │ GetOrConnectByAddress(addr)     │                            │
    │ → factory → RayletClient(addr)  │                            │
    │   → GrpcClient(ip, port)        │                            │
    │     → BuildChannel → grpc::CreateCustomChannel("ip:port")   │
    │       [channel: IDLE, 无TCP连接]│                            │
    │     → NewStub(channel_)         │                            │
    │   → RetryableGrpcClient(channel)│                            │
    │ ← 返回 raylet_client            │                            │
    │                                 │                            │
    │ raylet_client->RequestWorkerLease(req, cb)                   │
    │ → RetryableGrpcClient::CallMethod                            │
    │   → RetryableGrpcRequest::CallMethod()                       │
    │     → executor_ → GrpcClient::CallMethod                     │
    │       → client_call_manager_.CreateCall(...)                 │
    │         → ClientCallImpl(cb, timeout, cluster_id)            │
    │         → stub.PrepareAsync*(ctx, req, cq[rr_index])         │
    │           【gRPC: IDLE→CONNECTING, 开始 TCP 握手】             │
    │           ◄─────────────────────────────── TCP SYN ──►       │
    │           ◄────────────────────── TCP SYN+ACK ──────────►    │
    │           ◄────────────────────── HTTP2 Settings ────────►   │
    │           【channel: READY】      │                            │
    │         → StartCall()             │                            │
    │         → Finish(reply, status, tag)                          │
    │           【HTTP2 HEADERS + DATA 发出】──►                     │
    │ ← 返回（异步，不阻塞）          │                            │
    │                                 │                            │
    │  ... 做其他事情 ...              │   cq.AsyncNext(tag, ok)    │
    │                                 │   ← 得到 tag (响应到达)     │
    │                                 │   SetReturnStatus()        │
    │  main_service_.post(cb)  ◄──────│                            │
    │  执行 callback(status, reply)    │                            │
    │                                 │                            │
    │  如果失败(瞬态网络错误):          │                            │
    │  → RetryableGrpcClient::Retry   │                            │
    │    → 加入 pending_requests_     │                            │
    │    → SetupCheckTimer()          │                            │
    │    → 定期 CheckChannelStatus    │                            │
    │      → channel RECONNECTING     │                            │
    │      → 超时 → unavailable callback (检查节点/worker存活)     │
    │      → channel READY → 重发所有 pending requests ──────────►│
```

### 3.11 各阶段网络 I/O 总结

| 步骤 | 时机 | 做了什么 | 是否有真实网络 I/O |
|------|------|---------|:---:|
| 1. Pool 构造 | GCS Server 启动 | 注册 factory lambda | ❌ |
| 2. GetOrConnect* | 首次需要和目标通信 | 调用 factory → 构造 RayletClient/CoreWorkerClient | ❌ |
| 3. GrpcClient 构造 | 同 2 | `BuildChannel` → `CreateCustomChannel` → `NewStub` | ❌ (channel IDLE) |
| 4. 首次 RPC 调用 | 首次实际发请求 | `PrepareAsync*` + `StartCall` → **TCP 连接建立** + HTTP2 握手 | ✅ |
| 5. Finish | 同 4 | 请求发出，tag 注册到 CompletionQueue | ✅ |
| 6. Polling 线程 | 持续运行 | `AsyncNext` 获取响应，post 回调到主循环 | ✅ |

---

## 4. Polling 线程与 CompletionQueue 机制

### 4.1 Polling 线程启动

在 `ClientCallManager` 构造函数中（`client_call.h:271-275`）：

```cpp
for (int i = 0; i < num_threads_; i++) {
    cqs_.emplace_back(std::make_unique<grpc::CompletionQueue>());
    polling_threads_.emplace_back(
        &ClientCallManager::PollEventsFromCompletionQueue, this, i);
}
```

每个 CQ 对应一个 polling 线程，线程名 `client.poll0`、`client.poll1`、...。

GCS Server 中 `num_threads_` 由配置决定：
```cpp
RayConfig::instance().gcs_server_rpc_client_thread_num()
```

### 4.2 PollEventsFromCompletionQueue 完整实现

**文件**: `src/ray/rpc/client_call.h:341-388`

```cpp
void PollEventsFromCompletionQueue(int index) {
    SetThreadName("client.poll" + std::to_string(index));
    void *got_tag = nullptr;
    bool ok = false;
    while (true) {
        auto deadline = gpr_time_add(gpr_now(GPR_CLOCK_REALTIME),
                                     gpr_time_from_millis(250, GPR_TIMESPAN));
        auto status = cqs_[index]->AsyncNext(&got_tag, &ok, deadline);
        if (status == grpc::CompletionQueue::SHUTDOWN) {
            break;
        } else if (status == grpc::CompletionQueue::TIMEOUT && shutdown_) {
            break;
        } else if (status != grpc::CompletionQueue::TIMEOUT) {
            auto tag = static_cast<ClientCallTag *>(got_tag);
            got_tag = nullptr;
            tag->GetCall()->SetReturnStatus();
            std::shared_ptr<StatsHandle> stats_handle = tag->GetCall()->GetStatsHandle();
            RAY_CHECK_NE(stats_handle, nullptr);
            if (ok && !main_service_.stopped() && !shutdown_) {
                main_service_.post(
                    [tag]() {
                        tag->GetCall()->OnReplyReceived();
                        delete tag;
                    },
                    stats_handle->event_name + ".OnReplyReceived",
                    ray::asio::testing::GetDelayUs(stats_handle->event_name));
                main_service_.stats()->RecordEnd(std::move(stats_handle));
            } else {
                delete tag;
            }
        }
    }
}
```

### 4.3 AsyncNext — 非阻塞轮询 CompletionQueue

```cpp
auto deadline = gpr_time_add(gpr_now(GPR_CLOCK_REALTIME),
                             gpr_time_from_millis(250, GPR_TIMESPAN));
auto status = cqs_[index]->AsyncNext(&got_tag, &ok, deadline);
```

**三个参数**：

| 参数 | 含义 |
|------|------|
| `&got_tag` | 输出：gRPC 完成事件对应的 `void*` tag（就是 `Finish` 时传入的那个） |
| `&ok` | 输出：`true` = RPC 成功完成；`false` = RPC 失败（如服务端拒绝、超时等） |
| `deadline` | 最大等待时间，250ms |

**返回值 `status`** 有三种：

| 返回值 | 含义 |
|--------|------|
| `GRPC_QUEUE_OK` (0) | 取到一个事件，`got_tag` 和 `ok` 有效 |
| `GRPC_QUEUE_TIMEOUT` | 在 deadline 内没有事件 |
| `GRPC_QUEUE_SHUTDOWN` | CQ 已关闭 |

**为什么用 `AsyncNext` 而不是 `Next`**？

```cpp
// NOTE(edoakes): we use AsyncNext here because for some unknown reason,
// synchronous cq_.Next blocks indefinitely in the case that the process
// received a SIGTERM.
```

同步 `Next` 在收到 SIGTERM 时会无限阻塞，`AsyncNext` 的 250ms 超时可以周期性检查 `shutdown_` 标志。

**250ms 超时不是为了等待响应**——gRPC 有响应时 `AsyncNext` 会立刻返回 `OK`，不需要等 250ms。250ms 超时的目的是：
1. 周期性检查 `shutdown_` 标志
2. 避免无限阻塞（防止 SIGTERM 场景下 `Next()` 永远不返回的 bug）
3. 有事件时立刻返回，实际延迟远小于 250ms

### 4.4 三种返回分支详解

#### 分支一：SHUTDOWN

```cpp
if (status == grpc::CompletionQueue::SHUTDOWN) {
    break;  // 退出 while 循环，线程结束
}
```

CQ 被关闭时返回，发生在 `~ClientCallManager()` 析构时：

```cpp
~ClientCallManager() {
    shutdown_ = true;
    for (auto &cq : cqs_) {
        cq->Shutdown();    // ← 触发 SHUTDOWN 事件
    }
    for (auto &polling_thread : polling_threads_) {
        polling_thread.join();
    }
}
```

#### 分支二：TIMEOUT + shutdown_

```cpp
else if (status == grpc::CompletionQueue::TIMEOUT && shutdown_) {
    break;
}
```

workaround：gRPC 有时在 `Shutdown()` 后不正确返回 SHUTDOWN，而是持续返回 TIMEOUT。加了这个检查——如果已经 shutdown 了，遇到 TIMEOUT 就退出。

#### 分支三：得到事件（正常路径）

```cpp
else if (status != grpc::CompletionQueue::TIMEOUT) {
    // 处理响应
}
```

TIMEOUT 时什么都不做，回到 while 循环继续轮询。非 TIMEOUT 就是拿到了事件。

### 4.5 得到事件后的完整处理流程

```cpp
// 1. 从 void* 恢复 ClientCallTag
auto tag = static_cast<ClientCallTag *>(got_tag);
got_tag = nullptr;

// 2. 设置返回状态 (在 polling 线程中执行)
tag->GetCall()->SetReturnStatus();

// 3. 获取统计句柄
std::shared_ptr<StatsHandle> stats_handle = tag->GetCall()->GetStatsHandle();
RAY_CHECK_NE(stats_handle, nullptr);

// 4. 根据条件决定如何处理
if (ok && !main_service_.stopped() && !shutdown_) {
    // 情况 A：成功且服务未停止
    main_service_.post(
        [tag]() {
            tag->GetCall()->OnReplyReceived();   // 执行回调
            delete tag;                           // 释放 tag
        },
        stats_handle->event_name + ".OnReplyReceived",
        ray::asio::testing::GetDelayUs(stats_handle->event_name));
    main_service_.stats()->RecordEnd(std::move(stats_handle));
} else {
    // 情况 B：失败或服务已停止
    delete tag;  // 直接释放 tag，不执行回调
}
```

### 4.6 逐步深入每个步骤

#### 步骤 1：恢复 tag

```
Finish() 注册时:
  static_cast<void*>(tag)  →  void* 放入 CompletionQueue

AsyncNext() 返回时:
  got_tag = void*           →  static_cast<ClientCallTag*>(got_tag) 恢复
```

gRPC 的 CompletionQueue 只认 `void*`，不关心类型。Ray 用 `ClientCallTag` 包装了 `shared_ptr<ClientCall>`，所以通过 `tag->GetCall()` 可以安全地拿回原始的 `ClientCallImpl<Reply>`。

#### 步骤 2：SetReturnStatus()

```cpp
// ClientCallImpl<Reply> 内部 (client_call.h:91-95):
void SetReturnStatus() override {
    absl::MutexLock lock(&mutex_);
    return_status_ = GrpcStatusToRayStatus(status_);  // gRPC Status → Ray Status
}
```

这一步在 polling 线程中执行，将 gRPC 底层的 `grpc::Status` 转换为 Ray 的 `ray::Status`。加锁是因为 `status_` 由 gRPC 内部写入，而 `return_status_` 可能被其他线程通过 `GetStatus()` 读取。

#### 步骤 3：main_service_.post() — 投递到主事件循环

**核心设计决策**：回调不在 polling 线程执行，而是投递到 GCS 的主事件循环。

原因：
- **线程安全**：GCS 的大量状态（actor manager、node manager 等）不是线程安全的，回调如果访问这些状态，必须串行执行
- **避免阻塞 polling**：如果回调耗时长，会阻塞其他 RPC 响应的检测
- **与 Ray 架构一致**：Ray 的核心逻辑都运行在 asio 事件循环中

#### 步骤 4：OnReplyReceived() — 在主事件循环中执行

```cpp
// ClientCallImpl<Reply> 内部 (client_call.h:97-109):
void OnReplyReceived() override {
    ray::Status status;
    {
        absl::MutexLock lock(&mutex_);
        status = return_status_;
    }
    if (record_stats_ && !status.ok()) {
        grpc_client_req_failed_counter_.Record(1.0,
            {{\"Method\", stats_handle_->event_name}});
    }
    if (callback_ != nullptr) {
        callback_(status, std::move(reply_));  // ★ 执行用户回调
    }
}
```

如果经过了 `RetryableGrpcClient` 包装，这里的 callback 实际上是：

```cpp
[weak_retryable_grpc_client, retryable_grpc_request, original_callback](
    const ray::Status &status, Reply &&reply) {
    auto retryable_grpc_client = weak_retryable_grpc_client.lock();
    if (status.ok() || !IsGrpcRetryableStatus(status) || !retryable_grpc_client) {
        original_callback(status, std::move(reply));   // 成功 → 最终回调
        retryable_grpc_client->num_active_requests_--;
    } else {
        retryable_grpc_client->Retry(retryable_grpc_request);  // 瞬态失败 → 重试
    }
};
```

#### 步骤 5：delete tag — 释放 tag

`delete tag` 会让 `ClientCallTag` 析构，内部 `shared_ptr<ClientCall>` 引用计数减 1。如果 `CreateCall` 返回的 `shared_ptr` 已被释放，此时 `ClientCallImpl` 才真正析构。

### 4.7 ok = false 的处理

`ok = false` 时 gRPC 告知本次调用失败，可能原因：

| 失败场景 | `ok` 值 | `status_` |
|----------|---------|----------|
| 服务端返回错误 | `false` | `UNAVAILABLE` / `DEADLINE_EXCEEDED` 等 |
| 请求超时 | `false` | `DEADLINE_EXCEEDED` |
| 网络断开 | `false` | `UNAVAILABLE` |
| 服务端拒绝 | `false` | 对应错误码 |

在 polling 线程中：
```cpp
if (ok && !main_service_.stopped() && !shutdown_) {
    // 投递到主循环
} else {
    delete tag;  // 直接丢弃，不执行回调
}
```

**注意**：即使 `ok = false`，如果走了 `RetryableGrpcClient` 路径，瞬态失败会在 `OnReplyReceived()` 的包装回调中被捕获并重试，而不是在这里被丢弃。因为 `ok = false` 但 `main_service_` 没有停止时，回调仍然会被投递到主循环执行——`OnReplyReceived()` 会先调用 `SetReturnStatus()` 将 `status_` 转为 `Ray::Status::RpcError`，然后 `callback_` 会收到非 OK 的 status。

真正被丢弃的情况是 `main_service_.stopped()` 或 `shutdown_` 为 true——这意味着 GCS 正在关闭，不关心剩余的响应了。

### 4.8 多 CQ 并行示意

```
                    CQ[0]                 CQ[1]                 CQ[N-1]
                    ┌──────┐              ┌──────┐              ┌──────┐
  CreateCall(rr=0)  │ ReqA │              │      │              │      │
                    │ tagA │              │      │              │      │
  CreateCall(rr=1)  │ ReqA │              │ ReqB │              │      │
                    │ tagA │              │ tagB │              │      │
  CreateCall(rr=2)  │ ReqA │              │ ReqB │              │ ReqC │
                    │ tagA │              │ tagB │              │ tagC │
  CreateCall(rr=3)  │ ReqA │              │ ReqB │              │ ReqC │
  (rr=3%3=0)        │ ReqD │              │ tagB │              │ tagC │
                    │ tagA │              │      │              │      │
                    │ tagD │              │      │              │      │
                    └──────┘              └──────┘              └──────┘
                       ▲                      ▲                      ▲
                       │                      │                      │
                  polling                  polling                polling
                  thread 0                 thread 1              thread N-1
                  client.poll0             client.poll1          client.pollN-1
                       │                      │                      │
                  AsyncNext()            AsyncNext()            AsyncNext()
                  250ms deadline         250ms deadline         250ms deadline
                       │                      │                      │
                  ◄─── tagA ──►         ◄─── tagB ──►         ◄─── tagC ──►
                       │                      │                      │
                  SetReturnStatus()      SetReturnStatus()      SetReturnStatus()
                       │                      │                      │
                  main_service_.post()   main_service_.post()   main_service_.post()
                  [tagA callback]        [tagB callback]        [tagC callback]
                       │                      │                      │
                  继续轮询...             继续轮询...             继续轮询...
```

### 4.9 Polling 线程循环总结

```
Polling 线程的循环:
═════════════════

  while (true):
    AsyncNext(tag, ok, 250ms)

    ├── SHUTDOWN → 退出线程
    ├── TIMEOUT + shutdown_ → 退出线程
    ├── TIMEOUT → 继续（无事件，空转）
    └── 得到事件 (tag, ok):
         │
         ├── SetReturnStatus()          ← polling 线程中执行（转换 gRPC → Ray status）
         ├── main_service_.post([tag]() { OnReplyReceived(); delete tag; })
         │                              ← 投递到主事件循环（不在 polling 线程执行回调）
         └── RecordEnd(stats_handle)   ← 记录延迟统计
```

**设计精髓**：polling 线程只做最轻量的工作——取 tag、转状态、投递回调。所有业务逻辑（回调执行）都在主事件循环串行完成，保证线程安全。

---

## 5. tag/reply/call 关系与生命周期

### 5.1 三者关系

**call 是核心对象，持有 reply 和 callback；tag 是 call 的轻量包装，是 CQ 识别 call 的标识；reply 是 call 内部的数据缓冲区，gRPC 直接写入。**

```
tag (ClientCallTag)
  │
  │  持有 (shared_ptr)
  ▼
call (ClientCallImpl<Reply>)
  │
  ├── reply_          ← gRPC 写入响应数据
  ├── status_         ← gRPC 写入调用状态
  ├── callback_       ← 用户注册的回调函数
  ├── context_        ← gRPC ClientContext (deadline, metadata)
  ├── response_reader_← gRPC 异步调用句柄 (Finish 后不再需要)
  └── stats_handle_   ← 统计句柄
```

**tag 指向 call，call 包含 reply**。三者不是平级关系，而是嵌套关系。

### 5.2 ClientCallTag 的定义

**文件**: `src/ray/rpc/client_call.h:159-168`

```cpp
class ClientCallTag {
 public:
  explicit ClientCallTag(std::shared_ptr<ClientCall> call) : call_(std::move(call)) {}
  const std::shared_ptr<ClientCall> &GetCall() const { return call_; }
 private:
  std::shared_ptr<ClientCall> call_;
};
```

### 5.3 为什么需要 tag？不能直接用 call 吗？

**不能**，原因在代码注释中已经说明：

> `response_reader_->Finish` only accepts a raw pointer (`void*`)

而 `call` 是 `shared_ptr<ClientCallImpl>`，不能直接转 `void*`——如果转了，`shared_ptr` 的引用计数机制就失效了，可能导致 use-after-free。

tag 的存在就是为了解决这个**类型不兼容**问题：

```
                    gRPC 要求的接口
                    ┌──────────────────┐
  Finish(&reply, &status, void* tag)  │  ← 只接受 void*
                    └──────────────────┘

                    Ray 内部的对象管理
                    ┌──────────────────┐
  call = shared_ptr<ClientCallImpl>   │  ← 必须用 shared_ptr 管理生命周期
                    └──────────────────┘

  矛盾！void* 会丢失 shared_ptr 的引用计数

  解决方案：
  ┌─────────────────┐
  │ ClientCallTag    │  ← new 出裸指针，可以安全转 void*
  │   shared_ptr<Call>│  ← 内部持有 shared_ptr，保证 call 不被释放
  └─────────────────┘
```

### 5.4 tag 中是否包含 response 信息

**不包含。** tag 只是一个通知标识符，response 数据存在 call 中的 `reply_`。

```
tag (ClientCallTag)
  └── shared_ptr<ClientCall>      ← 只持有 ClientCallImpl 的引用
        │
        ├── reply_    (Reply 类型)   ★ response 数据在这里
        ├── status_   (grpc::Status) ★ gRPC 状态在这里
        ├── callback_ (ClientCallback<Reply>) ★ 用户回调在这里
        └── context_  (grpc::ClientContext)
```

`Finish()` 调用时，告诉 gRPC 把 response 写到哪里：

```cpp
call->response_reader_->Finish(
    &call->reply_,      // ← gRPC 收到响应后，反序列化写入这个地址
    &call->status_,     // ← gRPC 写入本次调用的状态
    static_cast<void*>(tag)  // ← 只是通知用的标识，不含数据
);
```

gRPC 内部收到服务端响应后：
1. 将 protobuf 字节流反序列化 → 写入 `call->reply_`
2. 将 gRPC 状态 → 写入 `call->status_`
3. 将 tag 放入 CompletionQueue

**tag 的作用仅仅是"通知"**——告诉 polling 线程"第 X 个 RPC 完成了，去 `call->reply_` 和 `call->status_` 取结果"。

### 5.5 生命周期中的三者关系

```
时间线 ─────────────────────────────────────────────────────►

阶段1: CreateCall
  ┌─────────────────────────────────┐
  │ call = make_shared<ClientCallImpl<Reply>>(callback, ...)     │
  │   → call 内部创建了空的 reply_    │
  │                                 │
  │ tag = new ClientCallTag(call)   │
  │   → tag 持有 call 的 shared_ptr │
  │   → call 引用计数 = 2           │
  │     (tag 持有 + CreateCall 返回值持有) │
  │                                 │
  │ Finish(&call->reply_, &call->status_, void*(tag))           │
  │   → 告诉 gRPC:                 │
  │     响应写入 call->reply_       │
  │     状态写入 call->status_      │
  │     完成后把 tag 放入 CQ        │
  │                                 │
  │ return call;                    │
  │   → 调用者可能持有，也可能立刻释放  │
  └─────────────────────────────────┘

阶段2: 等待响应
  ┌─────────────────────────────────┐
  │ gRPC 内部:                       │
  │   收到服务端 protobuf 字节流      │
  │   反序列化 → 写入 call->reply_   │  ★ reply 数据已经就位
  │   写入 call->status_             │  ★ status 数据已经就位
  │   将 tag 放入 CQ                 │  ★ tag 出现在 CQ 中
  │                                 │
  │ polling 线程:                    │
  │   AsyncNext() → 得到 tag        │
  │                                 │
  │ 此时:                           │
  │   call 引用计数 ≥ 1 (tag 至少持有1个) │
  │   call->reply_ 已填充            │
  │   call->status_ 已填充           │
  └─────────────────────────────────┘

阶段3: 处理响应
  ┌─────────────────────────────────┐
  │ polling 线程:                    │
  │   tag = static_cast<ClientCallTag*>(got_tag)                 │
  │   tag->GetCall() → 拿到 call    │  ★ 通过 tag 找到 call
  │   tag->GetCall()->SetReturnStatus()                          │
  │     → 把 call->status_ 转为 Ray Status                       │
  │                                 │
  │   main_service_.post([tag]() { │
  │     tag->GetCall()->OnReplyReceived()                        │
  │       → callback_(status, std::move(reply_))                 │
  │              ↑        ↑        │
  │              │        └── 来自 call->reply_                   │
  │              └── 来自 call->return_status_                    │
  │     delete tag;                 │
  │       → tag 析构                │
  │       → tag 内的 shared_ptr 释放 │
  │       → call 引用计数 -1        │
  │       → 如果调用者已释放返回值:   │
  │         call 引用计数 = 0 → 析构 │
  │       → 如果调用者仍持有返回值:  │
  │         call 引用计数 = 1 → 继续存活 │
  │   })                           │
  └─────────────────────────────────┘
```

### 5.6 三者各司其职

| 对象 | 职责 | 谁创建 | 谁销毁 |
|------|------|--------|--------|
| **call** (`ClientCallImpl<Reply>`) | 承载数据和逻辑：持有 reply、status、callback | `CreateCall` 中 `make_shared` | 引用计数归零时自动析构 |
| **reply** (`call->reply_`) | 数据缓冲区：gRPC 直接写入反序列化后的响应 | call 构造时创建 | call 析构时随 call 一起销毁 |
| **tag** (`ClientCallTag`) | CQ 标识符：是 call 在 CQ 中的"名片"，同时保证 call 生命周期安全 | `CreateCall` 中 `new` | polling 线程处理完后 `delete` |

### 5.7 类比

把一次 RPC 调用比作**寄快递**：

```
call = 快递单 + 收货地址
  ├── reply_ = 收货箱（快递员把包裹放进去）
  ├── callback_ = 收到包裹后通知谁
  └── status_ = 配送状态（签收/拒收/丢失）

tag = 取件通知短信
  ├── 内容只有："你的快递到了，单号是 call"
  ├── 不包含包裹本身（包裹在 reply_ 里）
  └── 收到短信后，拿着单号去找 call 拿包裹

CQ = 短信服务器
  └── 所有"快递到了"的短信都发到这里，polling 线程定期查看
```

**快递员（gRPC 网络）把包裹直接放进收货箱（reply_），然后发一条短信（tag）告诉你去取。短信本身不包含包裹，只是一个通知。**

### 5.8 Proactor 模式

gRPC 的异步模型是 **Proactor 模式**：

| 模式 | 机制 | 数据谁读 |
|------|------|---------|
| Reactor | 通知"数据可读了"，用户自己 read | 用户 |
| Proactor | 通知"数据已读完了"，系统已读好 | 系统（gRPC） |

gRPC 选择了 Proactor：它**自己完成反序列化**，把结果写入你提供的 buffer（`&reply_`、`&status_`），然后用 tag 通知你"数据已经准备好了"。你不需要自己从 socket 读数据、不需要自己解析 protobuf——tag 到达时，一切都是现成的。

---

## 关键代码文件索引（GCS RPC 通信相关）

| 文件 | 关键内容 |
|------|---------|
| `src/ray/gcs/gcs_server.cc` | GCS Server 构造、raylet_client_pool_ 和 worker_client_pool_ 初始化、事件监听器 |
| `src/ray/gcs/gcs_server.h` | GcsServer 类定义，成员变量声明 |
| `src/ray/raylet_rpc_client/raylet_client_pool.h` | RayletClientPool 类定义，RayletClientFactoryFn 类型 |
| `src/ray/raylet_rpc_client/raylet_client_pool.cc` | GetOrConnectByAddress, Disconnect, GenerateRayletAddress |
| `src/ray/raylet_rpc_client/raylet_client.cc` | RayletClient 构造、RequestWorkerLease 等 RPC 方法 |
| `src/ray/raylet_rpc_client/raylet_client.h` | RayletClient 类定义 |
| `src/ray/core_worker_rpc_client/core_worker_client_pool.h` | CoreWorkerClientPool 类定义 |
| `src/ray/core_worker_rpc_client/core_worker_client_pool.cc` | GetOrConnect, Disconnect, RemoveIdleClients |
| `src/ray/core_worker_rpc_client/core_worker_client.cc` | CoreWorkerClient 构造、PushActorTask/PushNormalTask |
| `src/ray/core_worker_rpc_client/core_worker_client.h` | CoreWorkerClient 类定义 |
| `src/ray/rpc/client_call.h` | ClientCallManager, ClientCallImpl, ClientCallTag 定义 |
| `src/ray/rpc/grpc_client.h` | GrpcClient 模板类, BuildChannel 声明, INVOKE_RPC_CALL 宏 |
| `src/ray/rpc/grpc_client.cc` | BuildChannel 实现（TLS/Insecure/Token Auth） |
| `src/ray/common/grpc_util.h` | CreateDefaultChannelArguments, GrpcStatusToRayStatus |
| `src/ray/rpc/retryable_grpc_client.h` | RetryableGrpcClient 类定义, INVOKE_RETRYABLE_RPC_CALL 宏 |
| `src/ray/rpc/retryable_grpc_client.cc` | Retry, CheckChannelStatus, SetupCheckTimer 实现 |
| `src/ray/rpc/rpc_callback_types.h` | ClientCallback 类型定义 |
