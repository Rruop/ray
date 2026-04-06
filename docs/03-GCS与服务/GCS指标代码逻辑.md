# GCS Server 指标与线程压力诊断 - 关键代码逻辑详解

> 本文档是 [GCS Server 指标含义与线程压力诊断深度分析](gcs-metrics-and-thread-pressure-diagnosis.md) 的补充，详细展示各关键路径的源码逻辑。
> 资源同步机制详见：[RaySyncer 资源同步机制与配置参数深度分析](./ray-syncer-resource-sync-mechanism.md)

---

## 一、IO Context Provider 路由策略

GCS Server 通过 `IOContextProvider<GcsServerIOContextPolicy>` 决定每个组件使用哪个 io_context。

策略定义在 `src/ray/gcs/gcs_server_io_context_policy.h`：

```cpp
// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <array>
#include <string_view>
#include <type_traits>

#include "ray/gcs/gcs_task_manager.h"
#include "ray/observability/ray_event_recorder.h"
#include "ray/pubsub/gcs_publisher.h"
#include "ray/ray_syncer/ray_syncer.h"
#include "ray/util/array.h"
#include "ray/util/type_traits.h"

namespace ray {
namespace gcs {

struct GcsServerIOContextPolicy {
  GcsServerIOContextPolicy() = delete;

  // IOContext name for each handler.
  // If a class needs a dedicated io context, it should be specialized here.
  // If a class does NOT have a dedicated io context, returns -1;
  template <typename T>
  static constexpr int GetDedicatedIOContextIndex() {
    if constexpr (std::is_same_v<T, GcsTaskManager>) {
      return IndexOf("task_io_context");
    } else if constexpr (std::is_same_v<T, pubsub::GcsPublisher>) {
      return IndexOf("pubsub_io_context");
    } else if constexpr (std::is_same_v<T, syncer::RaySyncer>) {
      return IndexOf("ray_syncer_io_context");
    } else if constexpr (std::is_same_v<T, observability::RayEventRecorder>) {
      return IndexOf("ray_event_io_context");
    } else {
      // default io context
      return -1;
    }
  }

  // This list must be unique and complete set of names returned from
  // GetDedicatedIOContextIndex. Or you can get runtime crashes when accessing a missing
  // name, or get leaks by creating unused threads.
  constexpr static std::array<std::string_view, 4> kAllDedicatedIOContextNames{
      "task_io_context",
      "pubsub_io_context",
      "ray_syncer_io_context",
      "ray_event_io_context"};
  constexpr static std::array<bool, 4> kAllDedicatedIOContextEnableLagProbe{
      true, true, true, true};

  constexpr static size_t IndexOf(std::string_view name) {
    return ray::IndexOf(kAllDedicatedIOContextNames, name);
  }
};

}  // namespace gcs
}  // namespace ray
```

---

## 二、gRPC Server 请求处理完整代码链

一个 RPC 请求从到达 GCS 到业务逻辑执行的完整路径（5步）：

```
[Raylet/CoreWorker] --gRPC--> [GCS Server]

第1步：server.poll.N 线程从 CompletionQueue 取出请求
  源码: src/ray/rpc/grpc_server.cc PollEventsFromCompletionQueue()
第2步：HandleRequest() post 到 io_service_（组件对应的 io_context）
  源码: src/ray/rpc/server_call.h HandleRequest()
第3步：HandleRequestImpl() 调用业务 Handler
第4步：业务 Handler 调用 send_reply_callback 发送回复
第5步：server.poll.N 线程收到 SENDING_REPLY 事件
```

### 2.1 server.poll 线程取出请求

源码: `src/ray/rpc/grpc_server.cc`

```cpp
void GrpcServer::PollEventsFromCompletionQueue(int index) {
  SetThreadName("server.poll" + std::to_string(index));
  void *tag;
  bool ok;

  // Keep reading events from the `CompletionQueue` until it's shutdown.
  while (true) {
    auto deadline = gpr_time_add(gpr_now(GPR_CLOCK_REALTIME),
                                 gpr_time_from_millis(250, GPR_TIMESPAN));
    auto status = cqs_[index]->AsyncNext(&tag, &ok, deadline);
    if (status == grpc::CompletionQueue::SHUTDOWN) {
      // If the completion queue status is SHUTDOWN, meaning the queue has been
      // drained. We can now exit the loop.
      break;
    } else if (status == grpc::CompletionQueue::TIMEOUT) {
      continue;
    }
    auto *server_call = static_cast<ServerCall *>(tag);
    bool delete_call = false;
    // A new call is needed after the server sends a reply, no matter the reply is
    // successful or failed.
    bool need_new_call = false;
    if (ok) {
      switch (server_call->GetState()) {
      case ServerCallState::PENDING:
        // We've received a new incoming request. Now this call object is used to
        // track this request.
        server_call->HandleRequest();
        break;
      case ServerCallState::SENDING_REPLY:
        // GRPC has sent reply successfully, invoking the callback.
        server_call->OnReplySent();
        // The rpc call has finished and can be deleted now.
        delete_call = true;
        // A new call should be suplied.
        need_new_call = true;
        break;
      default:
        RAY_LOG(FATAL) << "Shouldn't reach here.";
        break;
      }
    } else {
      // `ok == false` will occur in two situations:

      // First, server has sent reply to client and failed, the server call's status is
      // SENDING_REPLY. This can happen, for example, when the client deadline has
      // exceeded or the client side is dead.
      if (server_call->GetState() == ServerCallState::SENDING_REPLY) {
        server_call->OnReplyFailed();
        // A new call should be suplied.
        need_new_call = true;
      }
      // Second, the server has been shut down, the server call's status is PENDING.
      // And don't need to do anything other than deleting this call.
      // See
      // https://grpc.github.io/grpc/cpp/classgrpc_1_1_completion_queue.html#a86d9810ced694e50f7987ac90b9f8c1a
      // for more details.
      delete_call = true;
    }
    if (delete_call) {
      if (need_new_call && server_call->GetServerCallFactory().GetMaxActiveRPCs() != -1) {
        // Create a new `ServerCall` to accept the next incoming request.
        server_call->GetServerCallFactory().CreateCall();
      }
      delete server_call;
    }
  }
}```

### 2.2 HandleRequest() post 到 io_context

源码: `src/ray/rpc/server_call.h`

关键代码：`io_service_.post([...] { HandleRequestImpl(...); }, call_name_ + ".HandleRequestImpl");`

这里的 `io_service_` 在 GrpcService 构造时传入，决定请求在哪个线程执行。GCS 多数 Service 传入的是主线程 io_context。

---

## 三、gRPC Client 回复接收与回调代码链

源码: `src/ray/rpc/client_call.h`

关键代码：`main_service_.post([tag]() { tag->GetCall()->OnReplyReceived(); delete tag; }, ...);`

GCS 发出的所有 RPC 调用（LeaseWorker、CreateActorOnWorker 等），回调都 post 回 `main_service_`（主线程）。

 ## 九、关键代码逻辑详解

 ### 9.1 IO Context Provider 路由策略

 GCS Server 通过 `IOContextProvider<GcsServerIOContextPolicy>` 决定每个组件使用哪个 io_context。策略定义在 `src/ray/gcs/gcs_server_io_context_policy.h`：

 ```cpp
 struct GcsServerIOContextPolicy {
   GcsServerIOContextPolicy() = delete;

   template <typename T>
   static constexpr int GetDedicatedIOContextIndex() {
     if constexpr (std::is_same_v<T, GcsTaskManager>) {
       return IndexOf("task_io_context");        // GcsTaskManager -> task_io_context 线程
     } else if constexpr (std::is_same_v<T, pubsub::GcsPublisher>) {
       return IndexOf("pubsub_io_context");      // GcsPublisher -> pubsub_io_context 线程
     } else if constexpr (std::is_same_v<T, syncer::RaySyncer>) {
       return IndexOf("ray_syncer_io_context");  // RaySyncer -> ray_syncer_io_context 线程
     } else if constexpr (std::is_same_v<T, observability::RayEventRecorder>) {
       return IndexOf("ray_event_io_context");   // RayEventRecorder -> ray_event_io_context 线程
     } else {
       return -1;  // 其他组件 -> 默认 io_context（主线程）
     }
   }

   constexpr static std::array<std::string_view, 4> kAllDedicatedIOContextNames{
       "task_io_context",
       "pubsub_io_context",
       "ray_syncer_io_context",
       "ray_event_io_context"};
 };
 ```

 **路由规则**：返回 -1 的组件全部使用主线程，包括：
 - GcsHealthCheckManager
 - GcsNodeManager
 - GcsActorManager / GcsActorScheduler
 - GcsResourceManager
 - GcsPlacementGroupManager
 - GcsJobManager
 - GcsWorkerManager

 GcsServer 初始化各组件时的路由示例（`src/ray/gcs/gcs_server.cc`）：

 ```cpp
 // GcsNodeManager 使用 GetIOContext<GcsNodeManager>()，因 GcsNodeManager 不在专用列表中，返回默认 io_context
 gcs_node_manager_ = std::make_unique<GcsNodeManager>(
     gcs_publisher_.get(),
     gcs_table_storage_.get(),
     io_context_provider_.GetIOContext<GcsNodeManager>(),  // -> 主线程
     &raylet_client_pool_, ...);

 // HealthCheckManager 显式使用 GetDefaultIOContext()
 gcs_healthcheck_manager_ =
     GcsHealthCheckManager::Create(io_context_provider_.GetDefaultIOContext(),  // -> 主线程
                                   node_death_callback, ...);

 // ResourceManager 显式使用 GetDefaultIOContext()
 gcs_resource_manager_ = std::make_unique<GcsResourceManager>(
     io_context_provider_.GetDefaultIOContext(),  // -> 主线程
     cluster_resource_scheduler_->GetClusterResourceManager(), ...);

 // NodeInfoGrpcService 使用 GetIOContext<GcsNodeManager>()，与 Manager 同一线程
 rpc_server_.RegisterService(std::make_unique<rpc::NodeInfoGrpcService>(
     io_context_provider_.GetIOContext<GcsNodeManager>(),  // -> 主线程
     *gcs_node_manager_, ...));
 ```

 ### 9.2 gRPC 请求从接收到处理的完整代码链

 一个 RPC 请求从到达 GCS 到业务逻辑执行的完整路径：

 ```
 [Raylet/CoreWorker] --gRPC--> [GCS Server]

 第1步：gRPC server.poll.N 线程从 CompletionQueue 取出请求
        src/ray/rpc/grpc_server.cc:263 - PollEventsFromCompletionQueue()
        server_call->HandleRequest();  // PENDING -> 处理

 第2步：HandleRequest() 中 post 到 io_service_（组件对应的 io_context）
        src/ray/rpc/server_call.h:260-270
        io_service_.post(
            [this, auth_success, ...] {
              HandleRequestImpl(auth_success, ...);  // 在 io_context 线程执行
            },
            call_name_ + ".HandleRequestImpl");

 第3步：HandleRequestImpl() 调用业务 Handler
        src/ray/rpc/server_call.h:286-300
        (service_handler_.*handle_request_function_)(request_, *reply_, send_reply_callback);

 第4步：业务 Handler 处理完后调用 send_reply_callback
        send_reply_callback 内部通过 boost::asio::post(GetServerCallExecutor(), ...) 发送回复

 第5步：gRPC server.poll.N 线程收到 SENDING_REPLY 事件
        server_call->OnReplySent() -> delete server_call -> 创建新 ServerCall 等待下一个请求
 ```

 **关键**：第2步的 `io_service_` 是在 GrpcService 构造时传入的，决定了请求处理在哪个线程执行。对于 GCS 的多数 Service，这就是主线程。

 ### 9.3 gRPC 客户端回复从接收到处理的完整代码链

 GCS 作为 gRPC 客户端（如向 Raylet 发 LeaseWorker 请求）时的回调路径：

 ```cpp
 // src/ray/rpc/client_call.h:340-360 - PollEventsFromCompletionQueue()
 // client.poll.N 线程收到回复后，post 回 main_service_（主线程）
 main_service_.post(
     [tag]() {
       tag->GetCall()->OnReplyReceived();  // 在主线程执行回调
       delete tag;
     },
     stats_handle->event_name + ".OnReplyReceived");
 ```

 这意味着 GCS 发出的所有 RPC 调用（如 LeaseWorker、CreateActorOnWorker 等），其回调都在 `main_service_`（主线程）上执行。

 ### 9.4 GcsHealthCheckManager 完整生命周期代码

 #### 9.4.1 初始化与节点注册

 ```cpp
 // src/ray/gcs/gcs_server.cc:367-396
 void GcsServer::InitGcsHealthCheckManager(const GcsInitData &gcs_init_data) {
   // 节点死亡回调 -> post 到主线程
   auto node_death_callback = [this](const NodeID &node_id) {
     this->io_context_provider_.GetDefaultIOContext().post(
         [this, node_id] { return gcs_node_manager_->OnNodeFailure(node_id, nullptr); },
         "GcsServer.NodeDeathCallback");
   };

   gcs_healthcheck_manager_ =
       GcsHealthCheckManager::Create(io_context_provider_.GetDefaultIOContext(),  // 主线程
                                     node_death_callback,
                                     metrics_.health_check_rpc_latency_ms_histogram);
   // 对所有 ALIVE 节点注册健康检查
   for (const auto &item : gcs_init_data.Nodes()) {
     if (item.second.state() == rpc::GcsNodeInfo::ALIVE) {
       gcs_healthcheck_manager_->AddNode(item.first, raylet_client->GetChannel());
     }
   }
 }
 ```

 #### 9.4.2 AddNode -> HealthCheckContext 创建

 ```cpp
 // src/ray/gcs/gcs_health_check_manager.cc:231-240
 void GcsHealthCheckManager::AddNode(const NodeID &node_id,
                                     std::shared_ptr<grpc::Channel> channel) {
   io_service_.dispatch(  // dispatch 到主线程
       [this, channel = std::move(channel), node_id]() {
         auto context = new HealthCheckContext(shared_from_this(), channel, node_id);
         auto [_, is_new] = health_check_contexts_.emplace(node_id, context);
       },
       "GcsHealthCheckManager::AddNode");
 }
 ```

 HealthCheckContext 构造时启动首次定时器：
 ```cpp
 // src/ray/gcs/gcs_health_check_manager.h:131-137
 HealthCheckContext(std::shared_ptr<GcsHealthCheckManager> manager,
                    std::shared_ptr<grpc::Channel> channel,
                    NodeID node_id)
     : manager_(manager), node_id_(node_id),
       timer_(manager->io_service_),  // Timer 在主线程的 io_context 上
       health_check_remaining_(manager->failure_threshold_) {
   timer_.expires_from_now(boost::posix_time::milliseconds(manager->initial_delay_ms_));
   timer_.async_wait([this](auto) { StartHealthCheck(); });  // 主线程回调
 }
 ```

 #### 9.4.3 StartHealthCheck 发起检查 -> gRPC 回调 post 回主线程

 ```cpp
 // src/ray/gcs/gcs_health_check_manager.cc:122-227
 void GcsHealthCheckManager::HealthCheckContext::StartHealthCheck() {
   // [在主线程执行]

   // 优化：如果最近已知健康时间足够新，跳过本次检查
   const auto now = absl::Now();
   absl::Time next_check_time =
       latest_known_healthy_timestamp_ + absl::Milliseconds(manager->period_ms_);
   if (now <= next_check_time) {
     // 跳过，延迟到 next_check_time 再检查
     timer_.expires_from_now(boost::posix_time::milliseconds(next_schedule_millisec));
     timer_.async_wait([this](auto) { StartHealthCheck(); });
     return;
   }

   // 设置 gRPC 超时
   const auto deadline = now + absl::Milliseconds(manager->timeout_ms_);
   context->set_deadline(absl::ToChronoTime(deadline));

   // 发起异步健康检查 RPC
   stub_->async()->Check(context_ptr, &request_, response_ptr,
       [this, start = now, context, response](
           ::grpc::Status status) {
         // [在 gRPC 线程池执行此回调]

         auto gcs_health_check_manager = manager_.lock();
         if (gcs_health_check_manager == nullptr) {
           delete this;
           return;
         }

         // 记录 RPC 延迟指标
         gcs_health_check_manager->health_check_rpc_latency_ms_histogram_.Record(
             absl::ToInt64Milliseconds(absl::Now() - start));

         // *** 关键：将结果 post 回主线程 ***
         gcs_health_check_manager->io_service_.post(
             [this, status, response = std::move(response)]() {
               // [在主线程执行]

               if (status.ok() && response->status() == HealthCheckResponse::SERVING) {
                 health_check_remaining_ = mgr->failure_threshold_;  // 重置计数
               } else {
                 --health_check_remaining_;  // 失败计数-1
               }

               if (health_check_remaining_ == 0) {
                 mgr->FailNode(node_id_);  // 在主线程判定节点死亡
                 delete this;
               } else {
                 // 调度下一次检查
                 timer_.expires_from_now(boost::posix_time::milliseconds(mgr->period_ms_));
                 timer_.async_wait([this](auto) { StartHealthCheck(); });
               }
             },
             "HealthCheck");
       });
 }
 ```

 #### 9.4.4 FailNode -> OnNodeFailure 完整链路

 ```cpp
 // gcs_health_check_manager.cc:83-91
 void GcsHealthCheckManager::FailNode(const NodeID &node_id) {
   // [在主线程执行]
   RAY_LOG(WARNING).WithField(node_id) << "Node is dead because the health check failed.";
   RAY_CHECK(thread_checker_.IsOnSameThread());  // 确认在主线程
   auto iter = health_check_contexts_.find(node_id);
   if (iter != health_check_contexts_.end()) {
     on_node_death_callback_(node_id);  // 调用 GcsServer 注册的回调
     health_check_contexts_.erase(iter);
   }
 }

 // on_node_death_callback_ 在 GcsServer::InitGcsHealthCheckManager 中定义：
 auto node_death_callback = [this](const NodeID &node_id) {
   this->io_context_provider_.GetDefaultIOContext().post(
       [this, node_id] { return gcs_node_manager_->OnNodeFailure(node_id, nullptr); },
       "GcsServer.NodeDeathCallback");
 };
 // 注意：这里又 post 了一次，但 FailNode 已经在主线程，
 // 所以这个 post 只是排队等待，不会立即执行
 ```

 ### 9.5 GcsActorScheduler LeaseWorker 重试完整代码链

 #### 9.5.1 发起 Lease 请求

 ```cpp
 // src/ray/gcs/actor/gcs_actor_scheduler.cc:239-283
 void GcsActorScheduler::LeaseWorkerFromNode(
     std::shared_ptr<GcsActor> actor, std::shared_ptr<const rpc::GcsNodeInfo> node) {
   auto node_id = NodeID::FromBinary(node->node_id());
   RAY_LOG(INFO) << "Leasing worker for actor.";

   // 如果节点正在释放 worker，延迟重试
   if (nodes_of_releasing_unused_workers_.contains(node_id)) {
     RetryLeasingWorkerFromNode(actor, node);
     return;
   }

   auto raylet_client = raylet_client_pool_.GetOrConnectByAddress(remote_address);
   // 发起 Lease RPC（异步），回调通过 client.poll 线程 post 回主线程
   raylet_client->RequestWorkerLease(
       actor->GetLeaseSpecification().GetMessage(),
       actor->GetGrantOrReject(),
       [this, actor, node](const Status &status,
                           const rpc::RequestWorkerLeaseReply &reply) {
         HandleWorkerLeaseReply(actor, node, status, reply);  // 在主线程执行
       },
       0);
 }
 ```

 #### 9.5.2 处理 Lease 回调 -> 重试

 ```cpp
 // src/ray/gcs/actor/gcs_actor_scheduler.cc:555-624
 void GcsActorScheduler::HandleWorkerLeaseReply(
     std::shared_ptr<GcsActor> actor, std::shared_ptr<const rpc::GcsNodeInfo> node,
     const Status &status, const rpc::RequestWorkerLeaseReply &reply) {
   // [在主线程执行]

   if (status.ok()) {
     if (reply.rejected()) {
       // 资源不足被拒绝 -> Reschedule 重试
       RAY_LOG(INFO) << "Failed to lease worker from node " << node_id
                     << " as the resources are not enough";
       HandleWorkerLeaseRejectedReply(actor, reply);
     } else {
       // 成功获得 worker
       RAY_LOG(INFO) << "Finished leasing worker from " << node_id;
       HandleWorkerLeaseGrantedReply(actor, reply, node);
     }
   } else {
     // RPC 失败（节点不可达等） -> 延迟重试
     RetryLeasingWorkerFromNode(actor, node);
   }
 }
 ```

 #### 9.5.3 HandleWorkerLeaseRejectedReply -> Reschedule

 ```cpp
 // src/ray/gcs/actor/gcs_actor_scheduler.cc:604-611
 void GcsActorScheduler::HandleWorkerLeaseRejectedReply(
     std::shared_ptr<GcsActor> actor, const rpc::RequestWorkerLeaseReply &reply) {
   // [在主线程执行]
   if (!actor->GetAcquiredResources().IsEmpty()) {
     ReturnActorAcquiredResources(actor);  // 释放占用的资源
   }
   actor->UpdateAddress(rpc::Address());
   Reschedule(actor);  // 重新调度 -> 又一次 LeaseWorkerFromNode
 }
 ```

 #### 9.5.4 RetryLeasingWorkerFromNode 延迟重试

 ```cpp
 // src/ray/gcs/actor/gcs_actor_scheduler.cc:285-292
 void GcsActorScheduler::RetryLeasingWorkerFromNode(
     std::shared_ptr<GcsActor> actor, std::shared_ptr<const rpc::GcsNodeInfo> node) {
   // 延迟 gcs_lease_worker_retry_interval_ms 后重试
   RAY_UNUSED(execute_after(
       io_context_,  // 主线程 io_context
       [this, node, actor] { DoRetryLeasingWorkerFromNode(actor, node); },
       std::chrono::milliseconds(
           RayConfig::instance().gcs_lease_worker_retry_interval_ms())));
 }
 ```

 **重试循环对主线程的影响**：
 1. HandleWorkerLeaseReply（主线程）-> HandleWorkerLeaseRejectedReply -> Reschedule
 2. Reschedule -> LeaseWorkerFromNode -> RequestWorkerLease（异步 RPC）
 3. client.poll 线程收到回复 -> post 回主线程 -> HandleWorkerLeaseReply
 4. 如果又被拒绝 -> 回到第1步

 每次循环在主线程上至少产生 2 次 post 处理。N 个 Actor 同时重试时，主线程积压 2N 个回调。

 ### 9.6 RaySyncer BroadcastMessage O(N) 广播代码

 ```cpp
 // src/ray/ray_syncer/ray_syncer.cc:209-224
 void RaySyncer::BroadcastMessage(std::shared_ptr<const RaySyncMessage> message) {
   io_context_.dispatch(  // dispatch 到 ray_syncer_io_context 线程
       [this, message] {
         if (!node_state_->ConsumeSyncMessage(message)) {
           return;  // 消息过时，跳过
         }
         // O(N) 遍历所有连接的节点
         for (auto &reactor : sync_reactors_) {
           reactor.second->PushToSendingQueue(message);
         }
       },
       "RaySyncer.BroadcastMessage");
 }
 ```

 **O(N) 广播的触发路径**：

 Raylet 每隔 `raylet_report_resources_period_milliseconds`（默认100ms）上报资源变化，通过 gRPC 双向流发送到 GCS 的 RaySyncer：

 ```
 Raylet (每个节点)
   |  每100ms 上报资源变化
   v
 RaySyncerService::StartSync (gRPC 双向流)
   |  message_processor = syncer_.BroadcastMessage(msg)
   v
 RaySyncer::BroadcastMessage()
   |  io_context_.dispatch() -> ray_syncer_io_context 线程
   v
 遍历所有 sync_reactors_ (N个节点)
   |  reactor->PushToSendingQueue(message)
   v
 每个节点一个 gRPC Write 操作
 ```

 800 节点集群，100ms 内可能有 800 次资源上报，每次广播 800 次 push = 640,000 次 push/100ms，单线程无法处理。