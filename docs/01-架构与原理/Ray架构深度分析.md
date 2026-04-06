# Ray 进程与线程架构深度分析

## 目录

1. [集群架构概述](#1-集群架构概述)
2. [Head Node 进程组件](#2-head-node-进程组件)
3. [Worker Node 进程组件](#3-worker-node-进程组件)
4. [GCS Server 线程架构](#4-gcs-server-线程架构)
5. [Raylet 线程架构](#5-raylet-线程架构)
6. [CoreWorker 线程架构](#6-coreworker-线程架构)
7. [线程交互机制](#7-线程交互机制)
8. [性能瓶颈分析](#8-性能瓶颈分析)
9. [性能监控与诊断](#9-性能监控与诊断)
10. [优化建议](#10-优化建议)
11. [心跳检测机制深度解析](#11-心跳检测机制深度解析)
    - [11.1 架构概览](#111-架构概览)
    - [11.2 配置参数](#112-配置参数)
    - [11.3 GCS 端心跳检测实现](#113-gcs-端心跳检测实现)
    - [11.4 Raylet 端健康检查服务](#114-raylet-端健康检查服务)
    - [11.5 线程模型](#115-线程模型)
    - [11.6 诊断心跳超时原因](#116-诊断心跳超时原因)
    - [11.7 心跳超时后的处理流程](#117-心跳超时后的处理流程)
    - [11.8 错误消息详解](#118-错误消息详解)
    - [11.9 优化建议](#119-优化建议)
    - [11.10 关键源码文件索引](#1110-关键源码文件索引)
    - [11.11 GCS 到 Redis 心跳检测](#1111-gcs-到-redis-心跳检测)
    - [11.12 Worker 生命周期与连接管理](#1112-worker-生命周期与连接管理)
    - [11.13 完整心跳时序图](#1113-完整心跳时序图)
    - [11.14 常见故障场景与解决方案](#1114-常见故障场景与解决方案)
    - [11.15 监控指标汇总](#1115-监控指标汇总)
    - [11.16 心跳机制设计原则总结](#1116-心跳机制设计原则总结)

---

## 1. 集群架构概述

Ray 集群采用 Head Node + Worker Node 的架构模式：

```
┌─────────────────────────────────────────────────────────────────────┐
│                          Ray Cluster                                 │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │                      Head Node                               │    │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐   │    │
│  │  │  GCS Server  │  │   Raylet     │  │ Dashboard Agent  │   │    │
│  │  │  (全局元数据) │  │  (本地调度)  │  │   (监控指标)     │   │    │
│  │  └──────────────┘  └──────────────┘  └──────────────────┘   │    │
│  │  ┌──────────────┐  ┌──────────────┐                         │    │
│  │  │ Runtime Env  │  │   Workers    │                         │    │
│  │  │    Agent     │  │  (任务执行)  │                         │    │
│  │  └──────────────┘  └──────────────┘                         │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │                    Worker Node (N个)                         │    │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐   │    │
│  │  │   Raylet     │  │   Workers    │  │ Dashboard Agent  │   │    │
│  │  │  (本地调度)  │  │  (任务执行)  │  │   (监控指标)     │   │    │
│  │  └──────────────┘  └──────────────┘  └──────────────────┘   │    │
│  │  ┌──────────────┐                                           │    │
│  │  │ Runtime Env  │                                           │    │
│  │  │    Agent     │                                           │    │
│  │  └──────────────┘                                           │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### 1.1 进程列表概览

| 进程 | Head Node | Worker Node | 主要职责 |
|------|-----------|-------------|----------|
| GCS Server | ✅ | ❌ | 全局元数据管理、Actor 调度 |
| Raylet | ✅ | ✅ | 本地资源管理、任务调度 |
| Plasma Store | ✅ (内嵌) | ✅ (内嵌) | 对象存储 |
| Dashboard Agent | ✅ | ✅ | 指标收集、日志聚合 |
| Runtime Env Agent | ✅ | ✅ | 运行时环境管理 |
| Worker | ✅ | ✅ | 任务/Actor 执行 |

---

## 2. Head Node 进程组件

### 2.1 GCS Server (Global Control Service)

GCS Server 是 Ray 集群的核心控制平面，负责全局元数据管理。

**源码位置**: `src/ray/gcs/gcs_server/gcs_server.cc`

```cpp
// src/ray/gcs/gcs_server/gcs_server_main.cc
int main(int argc, char *argv[]) {
  // 初始化 GCS Server
  ray::gcs::GcsServer gcs_server(gcs_server_config, main_service);
  gcs_server.Start();

  // 运行主事件循环
  main_service.run();
}
```

**核心组件**:

| 组件 | 职责 | 源码位置 |
|------|------|----------|
| GcsActorManager | Actor 生命周期管理 | `gcs_actor_manager.cc` |
| GcsPlacementGroupManager | Placement Group 管理 | `gcs_placement_group_manager.cc` |
| GcsResourceManager | 全局资源视图 | `gcs_resource_manager.cc` |
| GcsNodeManager | 节点管理 | `gcs_node_manager.cc` |
| GcsJobManager | Job 管理 | `gcs_job_manager.cc` |
| GcsTaskManager | 任务元数据管理 | `gcs_task_manager.cc` |
| GcsPublisher | 事件发布 | `gcs_publisher.cc` |
| RaySyncer | 集群状态同步 | `ray_syncer.cc` |

### 2.2 Raylet

即使在 Head Node 上，也需要 Raylet 来管理本地资源和执行任务。

**源码位置**: `src/ray/raylet/main.cc`

### 2.3 Dashboard Agent

收集本地监控指标，通过 gRPC 上报到 Dashboard Server。

**源码位置**: `python/ray/dashboard/agent.py`

### 2.4 Runtime Env Agent

管理任务执行所需的运行时环境（pip 包、conda 环境等）。

**源码位置**: `python/ray/_private/runtime_env/agent/`

---

## 3. Worker Node 进程组件

Worker Node 不运行 GCS Server，其他组件与 Head Node 相同。

### 3.1 Raylet

负责本地任务调度和资源管理。

### 3.2 Worker 进程

执行实际的 Task 和 Actor。每个 Worker 进程内部运行 CoreWorker。

---

## 4. GCS Server 线程架构

### 4.1 线程总览

```
GCS Server 进程
├── main (gcs_server) - 主业务线程
├── server.poll.0~N - gRPC 服务端轮询线程
├── client.poll.0~N - gRPC 客户端轮询线程
├── task_io_context - GcsTaskManager 专用 IO
├── pubsub_io_context - GcsPublisher 专用 IO
├── ray_syncer_io_context - RaySyncer 专用 IO
└── ray_event_io_context - 事件记录专用 IO
```

### 4.2 专用 IO Context 定义

```cpp
// src/ray/gcs/gcs_server/gcs_server_io_context_policy.h
class DedicatedGcsServerIOContextPolicy : public GcsServerIOContextPolicy {
 public:
  /// Names for all of the dedicated io contexts.
  constexpr static std::array<std::string_view, 4> kAllDedicatedIOContextNames{
      "task_io_context",        // GcsTaskManager 使用
      "pubsub_io_context",      // GcsPublisher 使用
      "ray_syncer_io_context",  // RaySyncer 使用
      "ray_event_io_context"    // RayEventRecorder 使用
  };
};
```

### 4.3 各线程职责详解

#### 4.3.1 主线程 (gcs_server)

**职责**: 所有核心业务逻辑的串行处理

```cpp
// src/ray/gcs/gcs_server/gcs_server.cc
void GcsServer::Start() {
  // 初始化各管理器
  InitGcsNodeManager();
  InitGcsActorManager();
  InitGcsPlacementGroupManager();
  InitGcsResourceManager();
  InitGcsJobManager();
  InitGcsWorkerManager();

  // 启动 RPC 服务
  rpc_server_.RegisterService(node_info_grpc_service_);
  rpc_server_.RegisterService(actor_info_grpc_service_);
  // ...
  rpc_server_.Run();
}
```

**处理的核心事件**:
- Actor 创建/销毁/重启
- Placement Group 调度
- 节点注册/注销
- 资源更新
- Job 管理

#### 4.3.2 gRPC 服务端轮询线程 (server.poll.N)

**职责**: 接收和反序列化 gRPC 请求

```cpp
// src/ray/rpc/grpc_server.cc:148-155
void GrpcServer::Run() {
  // 启动轮询线程
  for (int i = 0; i < num_threads_; i++) {
    polling_threads_.emplace_back(
        [this, i] {
          SetThreadName("server.poll." + std::to_string(i));
          void *tag;
          bool ok;
          while (cqs_[i]->Next(&tag, &ok)) {
            auto *server_call = static_cast<ServerCallTag *>(tag);
            server_call->ProcessRequest();
          }
        });
  }
}
```

**线程数配置**:
```cpp
// src/ray/common/ray_config_def.h:358-360
RAY_CONFIG(uint32_t,
           gcs_server_rpc_server_thread_num,
           std::max(1U, std::thread::hardware_concurrency() / 4U))
```

#### 4.3.3 gRPC 客户端轮询线程 (client.poll.N)

**职责**: 轮询 gRPC 响应并触发回调

```cpp
// src/ray/rpc/client_call.h:312-358
void ClientCallManager::PollEventsFromCompletionQueue(int index) {
  SetThreadName("client.poll" + std::to_string(index));
  void *got_tag = nullptr;
  bool ok = false;

  while (true) {
    auto status = cqs_[index]->AsyncNext(&got_tag, &ok, deadline);
    if (status == grpc::CompletionQueue::SHUTDOWN) {
      break;
    }
    if (status != grpc::CompletionQueue::TIMEOUT) {
      auto tag = static_cast<ClientCallTag *>(got_tag);
      tag->GetCall()->SetReturnStatus();

      // 将回调 post 到主线程执行
      main_service_.post(
          [tag]() {
            tag->GetCall()->OnReplyReceived();
            delete tag;
          },
          stats_handle->event_name + ".OnReplyReceived",
          ray::asio::testing::GetDelayUs(stats_handle->event_name));
    }
  }
}
```

#### 4.3.4 task_io_context 线程

**职责**: GcsTaskManager 的任务元数据处理

```cpp
// GcsTaskManager 在专用 io_context 上处理任务事件
void GcsTaskManager::HandleTaskEvent(const rpc::TaskEventData &data) {
  // 在 task_io_context 线程上执行
  task_io_context_.post([this, data]() {
    ProcessTaskEvent(data);
  });
}
```

#### 4.3.5 pubsub_io_context 线程

**职责**: GcsPublisher 的事件发布

```cpp
// 发布事件到订阅者
void GcsPublisher::Publish(const std::string &channel,
                           const std::string &message) {
  pubsub_io_context_.post([this, channel, message]() {
    // 序列化并发送到所有订阅者
    for (auto &subscriber : subscribers_[channel]) {
      subscriber->Send(message);
    }
  });
}
```

#### 4.3.6 ray_syncer_io_context 线程

**职责**: RaySyncer 的集群状态同步

```cpp
// src/ray/ray_syncer/ray_syncer.cc:209-224
void RaySyncer::BroadcastMessage(std::shared_ptr<const RaySyncMessage> message) {
  io_context_.dispatch(
      [this, message] {
        if (!node_state_->ConsumeSyncMessage(message)) {
          return;
        }
        // O(N) 广播到所有连接的节点
        for (auto &reactor : sync_reactors_) {
          reactor.second->PushToSendingQueue(message);
        }
      },
      "RaySyncer.BroadcastMessage");
}
```

**同步的信息类型**:
- `RESOURCE_VIEW`: 节点资源视图
- `COMMANDS`: 调度命令

#### 4.3.7 ray_event_io_context 线程

**职责**: 事件记录和导出

---

## 5. Raylet 线程架构

### 5.1 线程总览

```
Raylet 进程
├── main (raylet) - 主业务线程
├── server.poll.0~N - gRPC 服务端轮询线程
├── client.poll.0~N - gRPC 客户端轮询线程
├── ray_syncer_io_context - RaySyncer 专用 IO
├── plasma_store - 对象存储线程
└── object_manager_rpc - 对象传输 RPC
```

### 5.2 各线程职责详解

#### 5.2.1 主线程 (raylet)

**源码位置**: `src/ray/raylet/main.cc:308`

```cpp
int main(int argc, char *argv[]) {
  SetThreadName("raylet");

  // 创建主 io_context
  instrumented_io_context main_service{/*enable_event_stats=*/true};

  // 初始化 NodeManager
  ray::raylet::NodeManager server(
      main_service,
      self_node_id,
      raylet_config,
      object_manager_config,
      gcs_client);

  // 运行主事件循环
  main_service.run();
}
```

**处理的核心事件**:
- 本地任务调度
- Worker 进程管理
- 资源分配
- 心跳发送

#### 5.2.2 NodeManager 核心组件

```cpp
// src/ray/raylet/node_manager.h
class NodeManager : public rpc::NodeManagerServiceHandler {
 private:
  // Worker 池管理
  WorkerPool worker_pool_;

  // 本地调度器
  std::shared_ptr<ClusterTaskManager> cluster_task_manager_;

  // 对象管理
  ObjectManager object_manager_;

  // 依赖管理
  LocalDependencyResolver local_dependency_resolver_;

  // 等待队列
  WaitManager wait_manager_;
};
```

#### 5.2.3 Plasma Store 线程

**职责**: 内存对象存储管理

```cpp
// src/ray/object_manager/plasma/store_runner.cc
void PlasmaStoreRunner::Start() {
  // 创建专用线程运行 Plasma Store
  store_thread_ = std::thread([this]() {
    SetThreadName("plasma_store");
    store_->RunEventLoop();
  });
}
```

**核心功能**:
- 共享内存分配
- 对象创建/删除
- 内存回收 (LRU)
- 对象封装 (Seal)

#### 5.2.4 ObjectManager RPC 线程

**职责**: 处理跨节点对象传输

```cpp
// src/ray/object_manager/object_manager.cc
ObjectManager::ObjectManager(...)
    : rpc_service_(io_service, *this),
      object_directory_(std::move(object_directory)) {
  // 注册 RPC 服务
  rpc_server_.RegisterService(rpc_service_);
}
```

---

## 6. CoreWorker 线程架构

### 6.1 线程总览

```
Worker 进程
├── main - Python/Java 主线程
├── core_worker.io - CoreWorker IO 线程
├── core_worker.direct_actor_submitter - Actor 调用提交
├── core_worker.memory_monitor - 内存监控
├── server.poll.0~N - gRPC 服务端轮询线程
└── client.poll.0~N - gRPC 客户端轮询线程
```

### 6.2 各线程职责

```cpp
// src/ray/core_worker/core_worker_process.cc
void CoreWorkerProcess::Initialize() {
  // 创建 IO 线程
  io_thread_ = std::thread([this]() {
    SetThreadName("core_worker.io");
    io_service_.run();
  });

  // 创建 Actor 提交线程
  direct_actor_submitter_thread_ = std::thread([this]() {
    SetThreadName("core_worker.direct_actor_submitter");
    direct_actor_submitter_service_.run();
  });

  // 创建内存监控线程
  memory_monitor_thread_ = std::thread([this]() {
    SetThreadName("core_worker.memory_monitor");
    memory_monitor_.Run();
  });
}
```

---

## 7. 线程交互机制

### 7.1 核心模式: io_context.post()

Ray 使用 Boost.Asio 的 `io_context.post()` 实现线程间通信：

```
┌─────────────────┐       post()        ┌─────────────────┐
│ gRPC 轮询线程    │ ─────────────────→ │    主线程       │
│ (server.poll.N) │                     │  (io_context)   │
└─────────────────┘                     └─────────────────┘
        │                                       │
        │ 1. 接收 gRPC 请求                      │ 3. 执行业务逻辑
        │ 2. 反序列化 protobuf                   │ 4. 序列化响应
        │                                       │
        └───────────────────────────────────────┘
                    5. 发送 gRPC 响应
```

### 7.2 请求处理流程示例

```cpp
// 1. gRPC 轮询线程接收请求
void ServerCall::ProcessRequest() {
  // 反序列化请求
  Request request;
  ParseFromGrpc(request);

  // 2. Post 到主线程处理
  main_service_.post([this, request]() {
    // 3. 在主线程执行业务逻辑
    HandleRequest(request, &response_);

    // 4. 发送响应
    SendReply();
  });
}
```

### 7.3 回调处理流程

```cpp
// src/ray/rpc/client_call.h:340-352
// 1. gRPC 客户端轮询线程收到响应
void PollEventsFromCompletionQueue() {
  auto tag = static_cast<ClientCallTag *>(got_tag);
  tag->GetCall()->SetReturnStatus();

  // 2. Post 回调到主线程
  main_service_.post(
      [tag]() {
        // 3. 在主线程执行回调
        tag->GetCall()->OnReplyReceived();
        delete tag;
      },
      stats_handle->event_name + ".OnReplyReceived");
}
```

### 7.4 instrumented_io_context

Ray 使用 `instrumented_io_context` 包装标准 io_context，添加事件统计功能：

```cpp
// src/ray/common/asio/instrumented_io_context.h
class instrumented_io_context : public boost::asio::io_context {
 public:
  void post(std::function<void()> handler,
            const std::string &event_name,
            int64_t delay_us = 0) {
    // 记录事件开始
    auto stats_handle = stats_->RecordStart(event_name);

    boost::asio::io_context::post([=]() {
      handler();
      // 记录事件结束
      stats_->RecordEnd(stats_handle);
    });
  }

  // 获取事件统计
  EventStats* stats() { return stats_.get(); }
};
```

---

## 8. 性能瓶颈分析

### 8.1 GCS 主线程瓶颈

**问题**: GCS 主线程处理所有核心业务逻辑，是串行执行的

**表现**:
- 高并发 Actor 创建时延迟增加
- 大量节点同时心跳时响应变慢
- Placement Group 调度阻塞其他操作

**代码证据**:
```cpp
// 所有 RPC 回调都 post 到同一个主线程
main_service_.post([this, request, reply, send_reply]() {
  // 业务逻辑在主线程串行执行
  HandleActorCreation(request, reply);
  send_reply();
});
```

### 8.2 ray_syncer_io_context 瓶颈

**问题**: RaySyncer 使用 O(N) 广播算法

**源码位置**: `src/ray/ray_syncer/ray_syncer.cc:209-224`

```cpp
void RaySyncer::BroadcastMessage(std::shared_ptr<const RaySyncMessage> message) {
  io_context_.dispatch(
      [this, message] {
        if (!node_state_->ConsumeSyncMessage(message)) {
          return;
        }
        // O(N) 复杂度 - 遍历所有连接的节点
        for (auto &reactor : sync_reactors_) {
          reactor.second->PushToSendingQueue(message);
        }
      },
      "RaySyncer.BroadcastMessage");
}
```

**影响**:
- 1000 节点集群：每次资源更新需要 1000 次 push 操作
- 高频更新场景下 ray_syncer_io_context 线程 CPU 100%
- 资源视图同步延迟增加，影响调度决策

### 8.3 瓶颈量化分析

| 集群规模 | 单次广播操作 | 100ms 内广播次数 | 总操作数 |
|----------|--------------|------------------|----------|
| 100 节点 | 100 | ~10 | 1,000 |
| 500 节点 | 500 | ~10 | 5,000 |
| 1000 节点 | 1000 | ~10 | 10,000 |
| 5000 节点 | 5000 | ~10 | 50,000 |

---

## 9. 性能监控与诊断

### 9.1 识别 GCS 压力的方法

#### 9.1.1 检查事件队列深度

```python
# 通过 Ray Dashboard API 获取
import requests
response = requests.get("http://localhost:8265/api/stats")
stats = response.json()

# 检查 GCS 事件队列
for event_name, event_stats in stats.get("gcs_events", {}).items():
    if event_stats["queue_depth"] > 100:
        print(f"Warning: {event_name} queue depth: {event_stats['queue_depth']}")
```

#### 9.1.2 检查 RPC 延迟

```bash
# 通过 metrics 端点
curl http://localhost:8265/metrics | grep gcs_rpc

# 关键指标
# gcs_rpc_latency_ms{method="RegisterNode"}
# gcs_rpc_latency_ms{method="CreateActor"}
# gcs_rpc_latency_ms{method="GetAllResourceUsage"}
```

#### 9.1.3 使用 Ray 内置诊断

```python
import ray

# 获取集群状态
ray.cluster_resources()

# 获取 GCS 内部状态
from ray._private.gcs_utils import GcsClient
gcs_client = GcsClient(address="auto")

# 检查所有节点
nodes = gcs_client.get_all_node_info()
print(f"Total nodes: {len(nodes)}")

# 检查 Actor 数量
actors = gcs_client.get_all_actor_info()
print(f"Total actors: {len(actors)}")
```

### 9.2 线程级别监控

#### 9.2.1 使用 perf 分析线程 CPU 使用

```bash
# 找到 GCS 进程
pgrep -f gcs_server

# 分析线程 CPU 使用
perf top -p <gcs_pid> -t

# 或者使用 top
top -H -p <gcs_pid>
```

#### 9.2.2 线程名称识别

```bash
# 列出进程的所有线程
ps -T -p <pid>

# 典型输出
# PID    SPID  CMD
# 12345  12345 gcs_server      <- 主线程
# 12345  12346 server.poll.0   <- gRPC 服务端轮询
# 12345  12347 client.poll.0   <- gRPC 客户端轮询
# 12345  12348 ray_syncer_io   <- RaySyncer IO
```

### 9.3 事件统计分析

```cpp
// 通过 EventStats 获取统计信息
EventStats* stats = main_service_.stats();

// 获取所有事件统计
for (const auto& [event_name, event_stat] : stats->GetAllStats()) {
  RAY_LOG(INFO) << "Event: " << event_name
                << " count: " << event_stat.count
                << " avg_latency_us: " << event_stat.avg_latency_us
                << " max_latency_us: " << event_stat.max_latency_us;
}
```

### 9.4 调度延迟诊断

```python
import ray
import time

@ray.remote
def test_task():
    return True

# 测量调度延迟
start = time.time()
refs = [test_task.remote() for _ in range(100)]
ray.get(refs)
end = time.time()

avg_latency = (end - start) / 100 * 1000
print(f"Average scheduling latency: {avg_latency:.2f} ms")

# 如果 > 50ms，可能存在 GCS 压力
```

---

## 10. 优化建议

### 10.1 短期优化

#### 10.1.1 增加 gRPC 线程数

```python
# ray_config.yaml
ray:
  _system_config:
    gcs_server_rpc_server_thread_num: 8  # 默认 = CPU/4
    gcs_server_rpc_client_thread_num: 8
```

#### 10.1.2 调整 RaySyncer 参数

```python
ray:
  _system_config:
    # 增加批量大小，减少同步频率
    ray_syncer_max_batch_size: 100  # 默认 50
    ray_syncer_max_batch_delay_ms: 50  # 默认 10
```

#### 10.1.3 启用 Pull-based 资源同步

```python
ray:
  _system_config:
    # 使用 pull 模式替代 push 模式
    enable_pull_based_resource_sync: true
```

### 10.2 中期优化

#### 10.2.1 主线程业务分离

将独立业务逻辑分离到专用 IO context:

```cpp
// 建议的架构
class GcsServer {
  instrumented_io_context main_service_;           // 核心调度
  instrumented_io_context actor_io_context_;       // Actor 管理
  instrumented_io_context pg_io_context_;          // Placement Group
  instrumented_io_context resource_io_context_;    // 资源管理
};
```

#### 10.2.2 RaySyncer 树形广播

```
当前: 星型拓扑 O(N)
     GCS
    / | \
   N1 N2 ... Nn

建议: 树形拓扑 O(log N)
       GCS
      /   \
    N1     N2
   / \    / \
  N3 N4  N5 N6
```

### 10.3 长期优化

#### 10.3.1 GCS 水平扩展

- 按功能分片 GCS (Actor GCS, Resource GCS, Job GCS)
- 使用一致性哈希分配请求

#### 10.3.2 分层资源管理

```
     GCS (全局视图)
       ↓ 聚合
   Region Manager
     ↓     ↓
  Rack 1  Rack 2
   ↓  ↓    ↓  ↓
  N1 N2   N3 N4
```

### 10.4 配置建议

#### 大规模集群 (>500 节点)

```python
ray:
  _system_config:
    # 增加线程数
    gcs_server_rpc_server_thread_num: 16
    gcs_server_rpc_client_thread_num: 16
    raylet_rpc_server_thread_num: 8

    # 减少同步频率
    ray_syncer_max_batch_delay_ms: 100
    ray_syncer_max_batch_size: 200

    # 增加超时
    gcs_rpc_timeout_ms: 30000

    # 启用优化
    enable_pull_based_resource_sync: true
```

#### 中等规模集群 (100-500 节点)

```python
ray:
  _system_config:
    gcs_server_rpc_server_thread_num: 8
    gcs_server_rpc_client_thread_num: 8
    ray_syncer_max_batch_delay_ms: 50
    ray_syncer_max_batch_size: 100
```

---

## 附录

### A. 关键源码文件索引

| 功能 | 文件路径 |
|------|----------|
| GCS Server 主入口 | `src/ray/gcs/gcs_server/gcs_server_main.cc` |
| GCS Server 实现 | `src/ray/gcs/gcs_server/gcs_server.cc` |
| GCS IO Context 策略 | `src/ray/gcs/gcs_server/gcs_server_io_context_policy.h` |
| Raylet 主入口 | `src/ray/raylet/main.cc` |
| NodeManager | `src/ray/raylet/node_manager.cc` |
| RaySyncer | `src/ray/ray_syncer/ray_syncer.cc` |
| ClientCallManager | `src/ray/rpc/client_call.h` |
| gRPC Server | `src/ray/rpc/grpc_server.cc` |
| CoreWorker | `src/ray/core_worker/core_worker.cc` |
| Plasma Store | `src/ray/object_manager/plasma/store.cc` |
| 配置定义 | `src/ray/common/ray_config_def.h` |

### B. 性能监控检查清单

- [ ] GCS 主线程 CPU 使用率
- [ ] ray_syncer_io_context 线程 CPU 使用率
- [ ] gRPC 轮询线程 CPU 使用率
- [ ] 事件队列深度
- [ ] RPC 调用延迟 (p50, p95, p99)
- [ ] Actor 创建延迟
- [ ] 任务调度延迟
- [ ] 集群节点数量

### C. 常见问题排查

| 症状 | 可能原因 | 排查方法 |
|------|----------|----------|
| Actor 创建慢 | GCS 主线程瓶颈 | 检查 GCS CPU 和事件队列 |
| 调度延迟高 | RaySyncer 同步慢 | 检查 ray_syncer_io 线程 |
| 节点注册慢 | gRPC 线程不足 | 增加 gRPC 线程数 |
| 资源视图不一致 | 同步延迟 | 检查网络和同步频率 |

---

## 11. 心跳检测机制深度解析

Ray 使用基于 gRPC Health Check Protocol 的心跳机制来检测节点存活状态。心跳检测是**单向的 Pull 模式**：由 GCS 主动向 Raylet 发起健康检查，而非 Raylet 推送心跳。

### 11.1 架构概览

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          心跳检测架构                                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   ┌──────────────────────────────────────────────────────────────────────┐  │
│   │                        GCS Server (Head Node)                         │  │
│   │  ┌────────────────────────────────────────────────────────────────┐  │  │
│   │  │ GcsHealthCheckManager                                           │  │  │
│   │  │  - HealthCheckContext (per node)                                │  │  │
│   │  │  - gRPC Health Check Stub                                       │  │  │
│   │  │  - Timer for periodic checks                                    │  │  │
│   │  └────────────────────────────────────────────────────────────────┘  │  │
│   │            │                           │                              │  │
│   │            │ 1. Health Check RPC       │ 4. OnNodeFailure callback    │  │
│   │            ▼                           ▼                              │  │
│   │  ┌────────────────────┐    ┌────────────────────────────┐            │  │
│   │  │  gRPC Client Pool  │    │   GcsNodeManager           │            │  │
│   │  └────────────────────┘    │   - RemoveNode()           │            │  │
│   │            │               │   - PublishNodeDeath()     │            │  │
│   │            │               └────────────────────────────┘            │  │
│   └────────────│────────────────────────────────────────────────────────┘  │
│                │                                                            │
│                │ gRPC Health Check Request                                  │
│                ▼                                                            │
│   ┌──────────────────────────────────────────────────────────────────────┐  │
│   │                        Raylet (Worker Node)                           │  │
│   │  ┌────────────────────────────────────────────────────────────────┐  │  │
│   │  │ gRPC Server (with built-in Health Check Service)                │  │  │
│   │  │  - grpc::EnableDefaultHealthCheckService(true)                  │  │  │
│   │  │  - Auto-responds SERVING status                                 │  │  │
│   │  └────────────────────────────────────────────────────────────────┘  │  │
│   │                                                                       │  │
│   │  ┌────────────────────────────────────────────────────────────────┐  │  │
│   │  │ NodeManager                                                     │  │  │
│   │  │  - AsyncCheckAlive() to GCS (self-liveness check)               │  │  │
│   │  │  - NodeRemoved() callback handler                               │  │  │
│   │  └────────────────────────────────────────────────────────────────┘  │  │
│   └──────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 11.2 配置参数

**源码位置**: `src/ray/common/ray_config_def.h:898-904`

```cpp
/// The following are configs for the health check. They are borrowed
/// from k8s health probe (shorturl.at/jmTY3)
/// The delay to send the first health check.
RAY_CONFIG(int64_t, health_check_initial_delay_ms, 5000)
/// The interval between two health check.
RAY_CONFIG(int64_t, health_check_period_ms, 3000)
/// The timeout for a health check.
RAY_CONFIG(int64_t, health_check_timeout_ms, 10000)
/// The threshold to consider a node dead.
RAY_CONFIG(int64_t, health_check_failure_threshold, 5)
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `health_check_initial_delay_ms` | 5000ms | 节点注册后首次健康检查的延迟 |
| `health_check_period_ms` | 3000ms | 两次健康检查的间隔 |
| `health_check_timeout_ms` | 10000ms | 单次健康检查的超时时间 |
| `health_check_failure_threshold` | 5 | 连续失败次数阈值，超过后判定节点死亡 |

**计算节点死亡判定时间**:
```
最短判定时间 = initial_delay + (failure_threshold × period)
             = 5000 + (5 × 3000) = 20000ms = 20s

最长判定时间 = initial_delay + (failure_threshold × (period + timeout))
             = 5000 + (5 × (3000 + 10000)) = 70000ms = 70s
```

### 11.3 GCS 端心跳检测实现

#### 11.3.1 GcsHealthCheckManager 初始化

**源码位置**: `src/ray/gcs/gcs_server.cc:367-389`

```cpp
void GcsServer::InitGcsHealthCheckManager(const GcsInitData &gcs_init_data) {
  RAY_CHECK(gcs_node_manager_);

  // 节点死亡时的回调函数
  auto node_death_callback = [this](const NodeID &node_id) {
    this->io_context_provider_.GetDefaultIOContext().post(
        [this, node_id] {
          return gcs_node_manager_->OnNodeFailure(node_id, nullptr);
        },
        "GcsServer.NodeDeathCallback");
  };

  // 创建 HealthCheckManager
  gcs_healthcheck_manager_ =
      GcsHealthCheckManager::Create(
          io_context_provider_.GetDefaultIOContext(),  // 运行在 GCS 主线程
          node_death_callback,
          metrics_.health_check_rpc_latency_ms_histogram);

  // 为已存在的存活节点添加健康检查
  for (const auto &item : gcs_init_data.Nodes()) {
    if (item.second.state() == rpc::GcsNodeInfo::ALIVE) {
      auto remote_address = rpc::RayletClientPool::GenerateRayletAddress(...);
      auto raylet_client = raylet_client_pool_.GetOrConnectByAddress(remote_address);
      gcs_healthcheck_manager_->AddNode(item.first, raylet_client->GetChannel());
    }
  }
}
```

#### 11.3.2 HealthCheckContext 健康检查逻辑

**源码位置**: `src/ray/gcs/gcs_health_check_manager.cc:122-227`

```cpp
void GcsHealthCheckManager::HealthCheckContext::StartHealthCheck() {
  using ::grpc::health::v1::HealthCheckResponse;

  // 检查是否需要发送新的健康检查
  const auto now = absl::Now();
  absl::Time next_check_time =
      latest_known_healthy_timestamp_ + absl::Milliseconds(manager->period_ms_);

  if (now <= next_check_time) {
    // 最近收到过健康状态更新，跳过本次检查
    int64_t next_schedule_millisec = (next_check_time - now) / absl::Milliseconds(1);
    timer_.expires_from_now(boost::posix_time::milliseconds(next_schedule_millisec));
    timer_.async_wait([this](auto) { StartHealthCheck(); });
    return;
  }

  // 创建 gRPC 健康检查请求
  auto context = std::make_shared<grpc::ClientContext>();
  auto response = std::make_shared<HealthCheckResponse>();

  const auto deadline = now + absl::Milliseconds(manager->timeout_ms_);
  context->set_deadline(absl::ToChronoTime(deadline));

  // 异步发送健康检查 RPC
  stub_->async()->Check(
      context_ptr,
      &request_,
      response_ptr,
      [this, start = now, context, response](::grpc::Status status) {
        // ⚠️ 此回调在 gRPC 线程池中执行
        gcs_health_check_manager->health_check_rpc_latency_ms_histogram_.Record(
            absl::ToInt64Milliseconds(absl::Now() - start));

        // Post 回 GCS 主线程处理结果
        gcs_health_check_manager->io_service_.post(
            [this, status, response]() {
              if (status.ok() && response->status() == HealthCheckResponse::SERVING) {
                // ✅ 健康检查通过，重置计数器
                health_check_remaining_ = mgr->failure_threshold_;
              } else {
                // ❌ 健康检查失败，递减计数器
                --health_check_remaining_;
                RAY_LOG(WARNING)
                    << "Health check failed for node " << node_id_
                    << ", remaining checks " << health_check_remaining_
                    << ", status " << status.error_code()
                    << ", status message " << status.error_message();
              }

              if (health_check_remaining_ == 0) {
                // 🔴 连续失败次数达到阈值，判定节点死亡
                mgr->FailNode(node_id_);
                delete this;
              } else {
                // 调度下一次健康检查
                timer_.expires_from_now(boost::posix_time::milliseconds(mgr->period_ms_));
                timer_.async_wait([this](auto) { StartHealthCheck(); });
              }
            },
            "HealthCheck");
      });
}
```

#### 11.3.3 节点死亡处理

**源码位置**: `src/ray/gcs/gcs_health_check_manager.cc:83-91`

```cpp
void GcsHealthCheckManager::FailNode(const NodeID &node_id) {
  RAY_LOG(WARNING).WithField(node_id) << "Node is dead because the health check failed.";
  RAY_CHECK(thread_checker_.IsOnSameThread());  // 确保在 GCS 主线程

  auto iter = health_check_contexts_.find(node_id);
  if (iter != health_check_contexts_.end()) {
    on_node_death_callback_(node_id);  // 触发 GcsNodeManager::OnNodeFailure
    health_check_contexts_.erase(iter);
  }
}
```

**源码位置**: `src/ray/gcs/gcs_node_manager.cc:686-713`

```cpp
void GcsNodeManager::InternalOnNodeFailure(
    const NodeID &node_id, const std::function<void()> &node_table_updated_callback) {

  auto maybe_node = GetAliveNodeFromCache(node_id);
  if (maybe_node.has_value()) {
    // 推断死亡原因
    rpc::NodeDeathInfo death_info = InferDeathInfo(node_id);

    // 从存活节点列表移除
    auto node = RemoveNodeFromCache(
        node_id, death_info, rpc::GcsNodeInfo::DEAD, current_sys_time_ms());

    // 添加到死亡节点缓存
    AddDeadNodeToCache(node);

    // 持久化到存储并发布事件
    gcs_table_storage_->NodeTable().Put(
        node_id, *node, {std::move(on_done), io_context_});
  }
}
```

#### 11.3.4 死亡原因推断

**源码位置**: `src/ray/gcs/gcs_node_manager.cc:538-565`

```cpp
rpc::NodeDeathInfo GcsNodeManager::InferDeathInfo(const NodeID &node_id) {
  auto iter = draining_nodes_.find(node_id);
  rpc::NodeDeathInfo death_info;

  bool expect_force_termination;
  if (iter == draining_nodes_.end()) {
    expect_force_termination = false;
  } else if (iter->second->deadline_timestamp_ms() == 0) {
    expect_force_termination = false;
  } else {
    expect_force_termination =
        (current_sys_time_ms() > iter->second->deadline_timestamp_ms()) &&
        (iter->second->reason() == rpc::autoscaler::DrainNodeReason::DRAIN_NODE_REASON_PREEMPTION);
  }

  if (expect_force_termination) {
    // 预期的强制终止（抢占）
    death_info.set_reason(rpc::NodeDeathInfo::AUTOSCALER_DRAIN_PREEMPTED);
    death_info.set_reason_message(iter->second->reason_message());
  } else {
    // 意外终止 - 心跳超时
    death_info.set_reason(rpc::NodeDeathInfo::UNEXPECTED_TERMINATION);
    death_info.set_reason_message(
        "health check failed due to missing too many heartbeats");
  }
  return death_info;
}
```

### 11.4 Raylet 端健康检查服务

#### 11.4.1 gRPC 默认健康检查服务注册

**源码位置**: `src/ray/rpc/grpc_server.cc:37-45`

```cpp
void GrpcServer::Init() {
  RAY_CHECK(num_threads_ > 0) << "Num of threads in gRPC must be greater than 0";
  cqs_.resize(num_threads_);

  // 启用 gRPC 内置健康检查服务
  // https://github.com/grpc/grpc/blob/master/doc/health-checking.md
  grpc::EnableDefaultHealthCheckService(true);

  grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  grpc::channelz::experimental::InitChannelzService();
}
```

**关键点**: Raylet 使用 gRPC 内置的健康检查服务，无需自定义实现。当 gRPC 服务正常运行时，健康检查会自动返回 `SERVING` 状态。

#### 11.4.2 Raylet 自我存活检查

Raylet 还会周期性地主动向 GCS 查询自己是否被标记为存活，用于检测 GCS 侧的故障。

**源码位置**: `src/ray/raylet/node_manager.cc:447-483`

```cpp
// Raylet 周期性检查自己是否在 GCS 中被标记为存活
periodical_runner_->RunFnPeriodically(
    [this] {
      static bool checking = false;
      if (checking) {
        return;
      }
      checking = true;

      // 向 GCS 查询自己的存活状态
      gcs_client_.Nodes().AsyncCheckAlive(
          {self_node_id_},
          /* timeout_ms = */ 30000,
          [this, checking_ptr = &checking](const auto &status, const auto &alive_vec) {
            bool alive = alive_vec[0];

            if (status.ok() && !alive) {
              // GCS 认为此 Raylet 已死亡
              RAY_LOG(FATAL)
                  << "GCS consider this node to be dead. This may happen when "
                  << "GCS is not backed by a DB and restarted or there is data loss "
                  << "in the DB.";
            } else if (status.IsUnauthenticated()) {
              // 认证失败，可能是 GCS 重启
              RAY_LOG(FATAL)
                  << "GCS returned an authentication error...";
            }
            *checking_ptr = false;
          });
    },
    RayConfig::instance().raylet_liveness_self_check_interval_ms(),  // 默认 60s
    "NodeManager.GcsCheckAlive");
```

#### 11.4.3 Raylet 收到自身死亡通知

当 GCS 将节点标记为死亡后，会通过 PubSub 广播此事件，Raylet 会收到通知。

**源码位置**: `src/ray/raylet/node_manager.cc:907-927`

```cpp
void NodeManager::NodeRemoved(const NodeID &node_id) {
  RAY_LOG(DEBUG).WithField(node_id) << "[NodeRemoved] Received callback from node id ";

  if (node_id == self_node_id_) {
    if (!shutting_down_) {
      // 自己被 GCS 标记为死亡，记录错误并退出
      std::ostringstream error_message;
      error_message
          << "[Timeout] Exiting because this node manager has mistakenly been marked "
             "as dead by the GCS: GCS failed to check the health of this node for "
          << RayConfig::instance().health_check_failure_threshold() << " times."
          << " This is likely because the machine or raylet has become overloaded.";

      RAY_EVENT(FATAL, "RAYLET_MARKED_DEAD").WithField("node_id", self_node_id_.Hex())
          << error_message.str();
      RAY_LOG(FATAL) << error_message.str();
    } else {
      // 已经在关闭中，正常处理
      RAY_LOG(INFO).WithField(node_id)
          << "Node is marked as dead by GCS as it's already shutting down.";
      return;
    }
  }
  // ... 处理其他节点死亡
}
```

### 11.5 线程模型

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          心跳检测线程模型                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   GCS Server                                                                 │
│   ┌──────────────────────────────────────────────────────────────────────┐  │
│   │  Main Thread (gcs_server)                                             │  │
│   │  ├─ GcsHealthCheckManager::AddNode()                                  │  │
│   │  ├─ GcsHealthCheckManager::FailNode()                                 │  │
│   │  ├─ GcsNodeManager::OnNodeFailure()                                   │  │
│   │  └─ Health check result processing (post from gRPC thread)            │  │
│   └──────────────────────────────────────────────────────────────────────┘  │
│   ┌──────────────────────────────────────────────────────────────────────┐  │
│   │  gRPC Client Thread Pool                                              │  │
│   │  └─ Health check RPC callback execution                               │  │
│   │     └─ Records latency metrics                                        │  │
│   │     └─ Posts result back to main thread                               │  │
│   └──────────────────────────────────────────────────────────────────────┘  │
│   ┌──────────────────────────────────────────────────────────────────────┐  │
│   │  Timer (boost::asio::deadline_timer)                                  │  │
│   │  └─ Runs on main thread io_context                                    │  │
│   │  └─ Triggers periodic StartHealthCheck()                              │  │
│   └──────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
│   Raylet                                                                     │
│   ┌──────────────────────────────────────────────────────────────────────┐  │
│   │  gRPC Server Thread Pool (server.poll.N)                              │  │
│   │  └─ Handles incoming Health Check RPC                                 │  │
│   │  └─ Auto-responds via grpc::EnableDefaultHealthCheckService           │  │
│   └──────────────────────────────────────────────────────────────────────┘  │
│   ┌──────────────────────────────────────────────────────────────────────┐  │
│   │  Main Thread (raylet)                                                 │  │
│   │  ├─ Periodical self-liveness check (AsyncCheckAlive)                  │  │
│   │  └─ NodeRemoved() callback handler                                    │  │
│   └──────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 11.6 诊断心跳超时原因

#### 11.6.1 区分 GCS 端和 Raylet 端问题

**问题来源判断方法**:

| 现象 | 可能原因 | 诊断方法 |
|------|----------|----------|
| GCS 日志显示 "Health check failed" 但 Raylet 仍在运行 | 网络问题或 Raylet gRPC 线程阻塞 | 检查 Raylet 的 gRPC server.poll 线程 CPU |
| Raylet 日志显示 "mistakenly been marked as dead" | GCS 健康检查超时 | 检查 GCS 主线程负载和网络延迟 |
| Raylet 进程直接退出（无 "marked as dead" 日志） | Raylet 崩溃 (OOM, segfault) | 检查系统日志 dmesg, /var/log/messages |
| GCS 日志显示多个节点同时健康检查失败 | GCS 端问题（主线程阻塞或网络） | 检查 GCS 主线程事件队列和 CPU |

#### 11.6.2 GCS 端日志分析

```bash
# GCS 健康检查失败日志
grep "Health check failed" /tmp/ray/session_*/logs/gcs_server.out

# 输出示例
# WARNING: Health check failed for node 1234abcd, remaining checks 4,
#          status 14, response status 0, status message Deadline Exceeded
```

**status 错误码说明**:
| gRPC Status Code | 含义 | 常见原因 |
|------------------|------|----------|
| 4 (DEADLINE_EXCEEDED) | 超时 | Raylet 响应慢或网络延迟 |
| 14 (UNAVAILABLE) | 服务不可用 | Raylet 进程崩溃或端口不可达 |
| 1 (CANCELLED) | 请求被取消 | GCS 端主动取消 |
| 13 (INTERNAL) | 内部错误 | 服务端异常 |

#### 11.6.3 Raylet 端日志分析

```bash
# Raylet 被标记为死亡的日志
grep "mistakenly been marked as dead" /tmp/ray/session_*/logs/raylet.out

# Raylet 自检发现 GCS 认为自己死亡
grep "GCS consider this node to be dead" /tmp/ray/session_*/logs/raylet.out
```

#### 11.6.4 网络延迟诊断

```bash
# 测试 GCS 到 Raylet 的 gRPC 连通性
grpc_health_probe -addr=<raylet_ip>:<raylet_port> -service=<node_id_hex>

# 测试网络延迟
ping -c 100 <raylet_ip> | tail -1

# 检查 TCP 连接状态
ss -tn | grep <raylet_port>
```

#### 11.6.5 线程负载诊断

```bash
# GCS 进程线程 CPU 使用
top -H -p $(pgrep -f gcs_server)

# 关注线程
# - gcs_server (主线程) - 如果 CPU 高说明业务逻辑繁忙
# - client.poll.N - 如果 CPU 高说明 RPC 回调处理繁忙

# Raylet 进程线程 CPU 使用
top -H -p $(pgrep -f raylet)

# 关注线程
# - raylet (主线程)
# - server.poll.N - 如果这些线程阻塞会导致健康检查无响应
```

### 11.7 心跳超时后的处理流程

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        心跳超时处理流程                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   1. GCS 检测到心跳超时 (health_check_remaining_ == 0)                       │
│      │                                                                       │
│      ▼                                                                       │
│   2. GcsHealthCheckManager::FailNode(node_id)                                │
│      │  - 记录 WARNING 日志                                                  │
│      │  - 调用 on_node_death_callback_                                       │
│      │                                                                       │
│      ▼                                                                       │
│   3. GcsNodeManager::OnNodeFailure(node_id)                                  │
│      │  - 从 alive_nodes_ 移除                                               │
│      │  - 添加到 dead_nodes_                                                 │
│      │  - 设置 death_info (UNEXPECTED_TERMINATION)                           │
│      │                                                                       │
│      ▼                                                                       │
│   4. 持久化节点状态到存储                                                     │
│      │  gcs_table_storage_->NodeTable().Put(...)                             │
│      │                                                                       │
│      ▼                                                                       │
│   5. 发布节点死亡事件                                                         │
│      │  PublishNodeInfoToPubsub(node_id, node_info_delta)                    │
│      │                                                                       │
│      ▼                                                                       │
│   6. 广播错误通知到所有 Driver                                                │
│      │  gcs_publisher_->PublishError(...)                                    │
│      │  消息: "The node with node id: xxx has been marked dead because       │
│      │         the detector has missed too many heartbeats from it."         │
│      │                                                                       │
│      ▼                                                                       │
│   7. 通知其他组件                                                             │
│      ├─ GcsActorManager: 处理该节点上的 Actor 故障                            │
│      ├─ GcsPlacementGroupManager: 重新调度 Placement Group                   │
│      └─ ClusterResourceScheduler: 更新资源视图                               │
│                                                                              │
│   8. Raylet 端处理 (收到 PubSub 通知)                                         │
│      │  NodeManager::NodeRemoved(self_node_id_)                              │
│      │                                                                       │
│      ▼                                                                       │
│   9. Raylet 退出                                                              │
│      RAY_LOG(FATAL) << "Exiting because this node manager has mistakenly    │
│                        been marked as dead by the GCS..."                    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 11.8 错误消息详解

**源码位置**: `src/ray/gcs/gcs_node_manager.cc:653-666`

```cpp
// GCS 端发送的错误消息
std::ostringstream error_message;
error_message << "The node with node id: " << node_id
              << " and address: " << removed_node->node_manager_address()
              << " and node name: " << removed_node->node_name()
              << " has been marked dead because the detector"
              << " has missed too many heartbeats from it. This can happen when a "
                 "\t(1) raylet crashes unexpectedly (OOM, etc.) \n"
              << "\t(2) raylet has lagging heartbeats due to slow network or busy "
                 "workload.";
```

### 11.9 优化建议

#### 11.9.1 调整健康检查参数

```python
# 对于网络延迟较高的环境
ray.init(_system_config={
    "health_check_timeout_ms": 30000,    # 增加超时时间
    "health_check_period_ms": 5000,      # 增加检查间隔
    "health_check_failure_threshold": 10  # 增加容忍次数
})
```

#### 11.9.2 监控健康检查指标

```bash
# 健康检查延迟指标
curl http://<gcs_address>:8265/metrics | grep health_check_rpc_latency

# 关键指标
# ray_health_check_rpc_latency_ms_bucket
# ray_health_check_rpc_latency_ms_count
# ray_health_check_rpc_latency_ms_sum
```

#### 11.9.3 使用 MarkNodeHealthy 优化

GCS 支持通过其他途径（如 RaySyncer）确认节点健康状态，减少不必要的健康检查 RPC：

**源码位置**: `src/ray/gcs/gcs_health_check_manager.cc:103-119`

```cpp
void GcsHealthCheckManager::MarkNodeHealthy(const NodeID &node_id) {
  io_service_.dispatch(
      [this, node_id]() {
        auto iter = health_check_contexts_.find(node_id);
        if (iter == health_check_contexts_.end()) {
          return;
        }
        auto *ctx = iter->second;
        // 更新最后已知健康时间戳
        ctx->SetLatestHealthTimestamp(absl::Now());
      },
      "GcsHealthCheckManager::MarkNodeHealthy");
}
```

当收到 RaySyncer 的资源更新消息时，可以调用此方法标记节点健康，从而跳过下一次健康检查 RPC。

### 11.10 关键源码文件索引

| 功能 | 文件路径 | 关键函数/类 |
|------|----------|------------|
| GCS 健康检查管理器 | `src/ray/gcs/gcs_health_check_manager.cc/h` | `GcsHealthCheckManager`, `HealthCheckContext` |
| GCS 节点管理器 | `src/ray/gcs/gcs_node_manager.cc` | `OnNodeFailure`, `InferDeathInfo`, `RemoveNodeFromCache` |
| GCS Server 初始化 | `src/ray/gcs/gcs_server.cc` | `InitGcsHealthCheckManager` |
| gRPC 健康检查服务 | `src/ray/rpc/grpc_server.cc` | `grpc::EnableDefaultHealthCheckService` |
| Raylet 节点管理 | `src/ray/raylet/node_manager.cc` | `NodeRemoved`, `GcsCheckAlive` |
| 配置定义 | `src/ray/common/ray_config_def.h` | `health_check_*` 配置项 |

### 11.11 GCS 到 Redis 心跳检测

当 GCS 使用 Redis 作为持久化存储时，会周期性检查 Redis 连接状态。

**源码位置**: `src/ray/gcs/gcs_server.cc:166-184`

```cpp
case StorageType::REDIS_PERSIST: {
  auto redis_store_client =
      std::make_shared<RedisStoreClient>(io_context, GetRedisClientOptions());

  // 周期性检查 Redis 健康状态，如果 Redis 不可用则崩溃
  // 注意：periodical_runner_ 必须与 Redis client 运行在同一个 IO context
  periodical_runner_->RunFnPeriodically(
      [redis_store_client, &io_context] {
        redis_store_client->AsyncCheckHealth(
            {[](const Status &status) {
               // Redis 连接失败时直接崩溃
               RAY_CHECK_OK(status) << "Redis connection failed unexpectedly.";
             },
             io_context});
      },
      RayConfig::instance().gcs_redis_heartbeat_interval_milliseconds(),  // 默认 100ms
      "GCSServer.redis_health_check");

  store_client = redis_store_client;
  break;
}
```

**配置参数**:

```cpp
// src/ray/common/ray_config_def.h:370
RAY_CONFIG(uint64_t, gcs_redis_heartbeat_interval_milliseconds, 100)
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `gcs_redis_heartbeat_interval_milliseconds` | 100ms | GCS 检查 Redis 健康的间隔 |

**Redis 故障处理**:
- 如果 Redis 连接失败，GCS Server 会直接崩溃 (`RAY_CHECK_OK`)
- 这是因为 GCS 依赖 Redis 存储集群状态，Redis 不可用时无法继续服务
- 上层需要通过进程监控重启 GCS Server

### 11.12 Worker 生命周期与连接管理

#### 11.12.1 Worker 注册流程

```
┌──────────────────────────────────────────────────────────────────────────┐
│                        Worker 注册流程                                    │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│   CoreWorker (Worker Process)                    Raylet                   │
│   ┌─────────────────────────┐                   ┌─────────────────────┐  │
│   │                         │                   │                     │  │
│   │  1. 启动                │                   │                     │  │
│   │     │                   │                   │                     │  │
│   │     ▼                   │                   │                     │  │
│   │  2. Connect to Raylet   │───────────────────│ 3. Accept           │  │
│   │     (Unix Socket)       │ RegisterClient    │    Connection       │  │
│   │     │                   │ Request           │     │               │  │
│   │     │                   │                   │     ▼               │  │
│   │     │                   │                   │  4. Add to          │  │
│   │     │                   │                   │     WorkerPool      │  │
│   │     │                   │◄──────────────────│     │               │  │
│   │     │                   │ RegisterClient    │     │               │  │
│   │     ▼                   │ Reply             │     ▼               │  │
│   │  5. Ready to execute    │                   │  6. Worker ready    │  │
│   │     tasks               │                   │     for scheduling  │  │
│   │                         │                   │                     │  │
│   └─────────────────────────┘                   └─────────────────────┘  │
│                                                                           │
└──────────────────────────────────────────────────────────────────────────┘
```

**注册超时配置**:

```cpp
// src/ray/common/ray_config_def.h:277
/// Worker 注册超时时间
RAY_CONFIG(int64_t, worker_register_timeout_seconds, 60)
```

#### 11.12.2 Worker 断开连接处理

**源码位置**: `src/ray/raylet/node_manager.cc:1404-1568`

```cpp
void NodeManager::DisconnectClient(
    const std::shared_ptr<ClientConnection> &client,
    rpc::WorkerExitType disconnect_type,
    const std::string &disconnect_detail,
    bool skip_graceful_exit) {

  // 1. 从 WorkerPool 获取 worker 信息
  const std::shared_ptr<WorkerInterface> worker = worker_pool_.GetRegisteredWorker(client);

  if (worker != nullptr) {
    // 2. 取消该 worker 的所有待处理租约
    cluster_lease_manager_.CancelAllLeasesOwnedBy(worker->WorkerId());

    // 3. 根据退出类型处理
    if (disconnect_type == rpc::WorkerExitType::INTENDED_USER_EXIT ||
        disconnect_type == rpc::WorkerExitType::INTENDED_SYSTEM_EXIT) {
      // 正常退出
      RAY_LOG(DEBUG) << "Worker " << worker->WorkerId() << " disconnected normally.";
    } else {
      // 异常退出，需要处理失败的任务
      RAY_LOG(WARNING) << "Worker " << worker->WorkerId()
                       << " disconnected abnormally: " << disconnect_detail;
    }

    // 4. 清理进程组 (发送 SIGTERM)
    CleanupProcessGroupSend(*saved, worker->WorkerId(), "DisconnectClient", SIGTERM);

    // 5. 如果 SIGTERM 后进程仍存在，发送 SIGKILL
    // ... 延迟后检查并 SIGKILL

    // 6. 从 WorkerPool 移除
    worker_pool_.DisconnectWorker(worker, disconnect_type);

    // 7. 发送断开连接回复
    SendDisconnectClientReply(worker->WorkerId(), client);
  }
}
```

#### 11.12.3 空闲 Worker 管理

Raylet 会周期性清理空闲 Worker 以节省资源。

**源码位置**: `src/ray/raylet/worker_pool.cc:179-183`

```cpp
// 启动空闲 Worker 清理定时器
if (RayConfig::instance().kill_idle_workers_interval_ms() > 0) {
  periodical_runner_->RunFnPeriodically(
      [this] { TryKillIdleWorkers(); },
      RayConfig::instance().kill_idle_workers_interval_ms(),  // 默认 200ms
      "RayletWorkerPool.deadline_timer.kill_idle_workers");
}
```

**配置参数**:

```cpp
// src/ray/common/ray_config_def.h:607-610
// 空闲 Worker 清理检查间隔
RAY_CONFIG(uint64_t, kill_idle_workers_interval_ms, 200)
// Worker 空闲多久后才能被清理
RAY_CONFIG(int64_t, idle_worker_killing_time_threshold_ms, 1000)
```

**清理逻辑** (`src/ray/raylet/worker_pool.cc:1157-1207`):

```cpp
void WorkerPool::TryKillIdleWorkers() {
  // 1. 计算可清理的空闲 Worker 数量
  int64_t num_killable_idle_workers = 0;
  for (auto &entry : idle_of_all_languages_) {
    // 排除以下情况的 Worker:
    // - 已死亡
    // - 关联的 Job 已完成
    // - 持有对象引用（需要保持活跃）
    // - 还在保活时间内
    if (CanKillWorker(entry)) {
      num_killable_idle_workers++;
    }
  }

  // 2. 计算期望保留的空闲 Worker 数量 (软限制 = 可用 CPU 数)
  const auto num_desired_idle_workers = get_num_cpus_available_();

  // 3. 如果空闲 Worker 超过限制，开始清理
  while (num_killable_idle_workers > num_desired_idle_workers &&
         !idle_of_all_languages_.empty()) {
    auto entry = idle_of_all_languages_.back();
    idle_of_all_languages_.pop_back();

    // 4. 发送退出请求给 Worker
    SendExitRequest(entry.worker);
    num_killable_idle_workers--;
  }
}
```

### 11.13 完整心跳时序图

```
时间轴 →

GCS Server                                          Raylet (Worker Node)
    │                                                      │
    │  ═══ 节点注册阶段 ═══                                  │
    │                                                      │
    │◄─────────────────── RegisterNode RPC ────────────────│
    │                                                      │
    │──── AddNode to health check ────────────────────────►│
    │     (设置 initial_delay_ms 延迟)                      │
    │                                                      │
    │  ═══ 周期性健康检查阶段 ═══                            │
    │                                                      │
    │  [等待 initial_delay_ms = 5000ms]                    │
    │     │                                                │
    │     ▼                                                │
t=5s│──── gRPC Health Check Request ──────────────────────►│
    │     │                                                │
    │     │  [timeout = 10000ms]                           │
    │     │                                                │
    │◄──── HealthCheckResponse::SERVING ───────────────────│
    │     │                                                │
    │  health_check_remaining_ = 5 (重置)                   │
    │     │                                                │
    │  [等待 period_ms = 3000ms]                           │
    │     │                                                │
t=8s│──── gRPC Health Check Request ──────────────────────►│
    │◄──── HealthCheckResponse::SERVING ───────────────────│
    │     │                                                │
    │  ═══ 故障场景: Raylet 无响应 ═══                       │
    │     │                                                │
t=11s│──── gRPC Health Check Request ──────────────────────►│
    │     │                                     ╔═══════╗  │
    │     │  [等待 10s 超时]                    ║ Raylet ║  │
    │     │                                     ║ 无响应 ║  │
    │     ▼                                     ╚═══════╝  │
t=21s│  DEADLINE_EXCEEDED                                   │
    │  health_check_remaining_ = 4                         │
    │  WARNING: "Health check failed, remaining 4"         │
    │     │                                                │
t=24s│──── gRPC Health Check Request ──────────────────────►│
t=34s│  DEADLINE_EXCEEDED                                   │
    │  health_check_remaining_ = 3                         │
    │     │                                                │
t=37s│──── gRPC Health Check Request ──────────────────────►│
t=47s│  health_check_remaining_ = 2                         │
    │     │                                                │
t=50s│──── gRPC Health Check Request ──────────────────────►│
t=60s│  health_check_remaining_ = 1                         │
    │     │                                                │
t=63s│──── gRPC Health Check Request ──────────────────────►│
t=73s│  health_check_remaining_ = 0                         │
    │     │                                                │
    │  ═══ 节点死亡处理阶段 ═══                              │
    │     │                                                │
    │  FailNode(node_id)                                   │
    │     │                                                │
    │  OnNodeFailure(node_id)                              │
    │     │                                                │
    │  RemoveNodeFromCache()                               │
    │     │                                                │
    │  Publish to GCS_NODE_INFO_CHANNEL ───────────────────►│ (如果 Raylet 恢复)
    │                                                      │
    │                                                      │ NodeRemoved(self_node_id_)
    │                                                      │     │
    │                                                      │  RAY_LOG(FATAL)
    │                                                      │  "Exiting because..."
    │                                                      │     │
    │                                                      │  进程退出
```

### 11.14 常见故障场景与解决方案

#### 场景 1: Raylet 进程崩溃 (OOM)

**现象**:
- GCS 日志: `"Health check failed for node xxx, status 14 (UNAVAILABLE)"`
- 系统日志: `oom-killer` 相关记录
- Raylet 日志: 无 "marked as dead" 日志（因为进程已死）

**诊断**:
```bash
# 检查系统 OOM 日志
dmesg | grep -i "oom\|killed process"
journalctl -k | grep -i oom

# 检查 Raylet 进程内存使用历史 (如果有监控)
# 检查 /tmp/ray/session_*/logs/raylet.out 是否有内存相关错误
```

**解决方案**:
```python
# 增加节点内存或限制 worker 内存使用
ray.init(_system_config={
    "object_store_memory": 10 * 1024 * 1024 * 1024,  # 限制对象存储大小
})
```

#### 场景 2: Raylet 主线程阻塞

**现象**:
- GCS 日志: `"Health check failed, status 4 (DEADLINE_EXCEEDED)"`
- Raylet 日志: 最终出现 `"mistakenly been marked as dead"`
- Raylet 进程仍在运行，但 CPU 使用率可能很高

**诊断**:
```bash
# 检查 Raylet 线程 CPU 使用
top -H -p $(pgrep -f "raylet")

# 查看 server.poll 线程是否响应
# 如果 raylet 主线程 CPU 100%，说明主线程阻塞

# 检查 Raylet 事件统计
grep "event_loop_stats" /tmp/ray/session_*/logs/raylet.out | tail -20
```

**解决方案**:
```python
# 增加健康检查容忍度
ray.init(_system_config={
    "health_check_timeout_ms": 30000,      # 增加超时
    "health_check_failure_threshold": 10,  # 增加容忍次数
})
```

#### 场景 3: 网络分区

**现象**:
- 多个节点同时被标记为死亡
- GCS 日志: 多个 `"Health check failed"` 同时出现
- 节点之间网络可能仍然连通，只是与 GCS 断开

**诊断**:
```bash
# 从 Worker 节点测试到 GCS 的连接
nc -zv <gcs_ip> <gcs_port>
grpc_health_probe -addr=<gcs_ip>:<gcs_port>

# 检查网络延迟
ping -c 100 <gcs_ip> | tail -5
```

**解决方案**:
```python
# 增加网络容忍度
ray.init(_system_config={
    "health_check_period_ms": 10000,       # 降低检查频率
    "health_check_timeout_ms": 60000,      # 大幅增加超时
    "health_check_failure_threshold": 20,  # 大幅增加容忍次数
})
```

#### 场景 4: GCS 主线程过载

**现象**:
- 健康检查发起但结果处理延迟
- GCS 日志显示高延迟的 RPC
- 多个节点健康检查"伪失败"

**诊断**:
```bash
# 检查 GCS 主线程负载
top -H -p $(pgrep -f "gcs_server")

# 检查 GCS 事件队列深度
curl http://<gcs_ip>:8265/metrics | grep queue

# 检查健康检查 RPC 延迟
curl http://<gcs_ip>:8265/metrics | grep health_check_rpc_latency
```

**解决方案**:
```python
# 优化 GCS 配置
ray.init(_system_config={
    "gcs_server_rpc_server_thread_num": 16,  # 增加 gRPC 线程
    "gcs_server_rpc_client_thread_num": 16,
})
```

### 11.15 监控指标汇总

| 指标名称 | 类型 | 说明 |
|----------|------|------|
| `ray_health_check_rpc_latency_ms` | Histogram | 健康检查 RPC 延迟分布 |
| `ray_node_failures_total` | Counter | 节点故障总数 |
| `ray_gcs_rpc_latency_ms{method="CheckAlive"}` | Histogram | CheckAlive RPC 延迟 |
| `ray_raylet_event_loop_stats` | Gauge | Raylet 事件循环统计 |

**Prometheus 告警规则示例**:

```yaml
groups:
- name: ray_health_check
  rules:
  - alert: RayHealthCheckHighLatency
    expr: histogram_quantile(0.99, ray_health_check_rpc_latency_ms_bucket) > 5000
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "Ray 健康检查延迟过高"
      description: "健康检查 P99 延迟超过 5s，可能导致误判节点死亡"

  - alert: RayNodeFailureRate
    expr: rate(ray_node_failures_total[5m]) > 0.1
    for: 2m
    labels:
      severity: critical
    annotations:
      summary: "Ray 节点故障率过高"
      description: "5 分钟内节点故障率超过 0.1/s"
```

### 11.16 心跳机制设计原则总结

1. **Pull vs Push**: Ray 选择 Pull 模式（GCS 主动检查）而非 Push 模式（Raylet 推送心跳），原因：
   - 简化 Raylet 实现，无需维护心跳发送逻辑
   - GCS 可以控制检查频率和策略
   - 使用标准 gRPC Health Check Protocol，通用性好

2. **故障判定策略**: 连续 N 次失败才判定死亡，避免瞬时网络抖动导致误判

3. **双向确认**: Raylet 通过 `AsyncCheckAlive` 反向确认自己在 GCS 中的状态，用于检测 GCS 故障/重启

4. **快速恢复 vs 稳定性权衡**: 默认配置偏向稳定性（20-70s 判定时间），生产环境可根据需求调整
