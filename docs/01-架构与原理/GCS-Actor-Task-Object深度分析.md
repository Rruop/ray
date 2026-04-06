# Ray GCS / Actor / Task / Object 创建与存储链路深度分析

## 目录

- [一、总结结论](#一总结结论)
- [二、Actor 创建 — 必须经过 GCS](#二actor-创建--必须经过-gcs)
  - [2.1 完整代码链路](#21-完整代码链路)
  - [2.2 GCS 端处理流程](#22-gcs-端处理流程)
  - [2.3 为什么 Actor 创建必须经过 GCS](#23-为什么-actor-创建必须经过-gcs)
  - [2.4 Actor 创建能否绕过 GCS？](#24-actor-创建能否绕过-gcs)
- [三、Normal Task 提交 — 不经过 GCS](#三normal-task-提交--不经过-gcs)
  - [3.1 完整代码链路](#31-完整代码链路)
  - [3.2 为什么 Task 不经过 GCS](#32-为什么-task-不经过-gcs)
- [四、Actor 方法调用 — 直连 Actor Worker](#四actor-方法调用--直连-actor-worker)
- [五、Object 存储 — 不经过 GCS（Ownership 模型）](#五object-存储--不经过-gcdownership-模型)
  - [5.1 Object Put 链路](#51-object-put-链路)
  - [5.2 Object Get 链路（本地）](#52-object-get-链路本地)
  - [5.3 Object Get 链路（跨节点）](#53-object-get-链路跨节点)
  - [5.4 Owner 地址的完整生命周期](#54-owner-地址的完整生命周期)
  - [5.5 Object 多副本机制](#55-object-多副本机制)
  - [5.6 Owner 死亡场景分析](#56-owner-死亡场景分析)
- [六、架构总览图](#六架构总览图)

---

## 一、总结结论

| 操作 | 是否经过 GCS | 经过的组件 |
|---|---|---|
| **Actor 创建** | **是** (两次 RPC: RegisterActor + CreateActor) | CoreWorker -> GCS -> Raylet -> Worker |
| **Normal Task 提交** | **否** | CoreWorker -> 本地 Raylet (RequestWorkerLease) -> Worker |
| **Actor 方法调用** | **否** | CoreWorker -> 直接发到 Actor Worker |
| **Object Put** | **否** | CoreWorker -> 本地 Plasma 共享内存 |
| **Object Get (本地)** | **否** | CoreWorker -> 本地 Plasma |
| **Object Get (跨节点)** | **否** | CoreWorker -> Raylet -> Owner Worker (查位置) -> 远程 ObjectManager |

**核心设计原则**：GCS 只管理需要全局协调的低频操作（Actor 生命周期），高频操作（Task 提交、Object 存取）走去中心化路径（Raylet + Owner Worker），避免 GCS 成为瓶颈。

---

## 二、Actor 创建 — 必须经过 GCS

Actor 创建分为两阶段，发送两次 RPC 到 GCS：**RegisterActor** 和 **CreateActor**。

### 2.1 完整代码链路

```
Python: ActorClass._remote()                        (python/ray/actor.py:1810)
  |
  v
Cython: CoreWorker.create_actor()                   (python/ray/_raylet.pyx:3580)
  |  将 Python 类型转为 C++ 类型（资源、标签、并发组、调度策略）
  |  调用 CCoreWorkerProcess.GetCoreWorker().CreateActor(...)
  |
  v
C++: CoreWorker::CreateActor()                      (src/ray/core_worker/core_worker.cc:2057)
  |
  |-- 1. 生成 ActorID                               (line 2083-2085)
  |     const ActorID actor_id = ActorID::Of(
  |         worker_context_->GetCurrentJobID(),
  |         worker_context_->GetCurrentTaskID(),
  |         next_task_index);
  |
  |-- 2. 构建 TaskSpec                              (line 2090-2172)
  |     TaskSpecBuilder builder;
  |     BuildCommonTaskSpec(builder, ...);
  |     builder.SetActorCreationTaskSpec(...);
  |
  |-- 3. 创建 ActorHandle 注册到 ActorManager       (line 2176-2180)
  |     auto actor_handle = std::make_unique<ActorHandle>(actor_id, ...);
  |     actor_manager_->EmplaceNewActorHandle(std::move(actor_handle), ...);
  |
  |-- 4. 注册到 TaskManager (max_retries=0)          (line 2243-2248)
  |     task_manager_->AddPendingTask(
  |         rpc_address_, task_spec, CurrentCallSite(),
  |         // Actor creation task retry happens on GCS not on core worker.
  |         /*max_retries=*/0);
  |
  +===== 阶段1: RegisterActor RPC -> GCS =====
  |
  |  非命名 Actor (异步):                            (line 2250-2264)
  |    actor_creator_->AsyncRegisterActor(task_spec, callback);
  |
  |  命名 Actor (同步):                              (line 2265-2278)
  |    auto status = actor_creator_->RegisterActor(task_spec);
  |    // 注释: "For named actor, we still go through the sync way because
  |    //  for functions like list actors these actors need to be there"
  |
  |    ActorCreator::RegisterActor()                 (src/ray/core_worker/actor_management/actor_creator.cc:24)
  |      -> actor_client_.SyncRegisterActor(task_spec)
  |        -> ActorInfoAccessor::AsyncRegisterActor()
  |           (src/ray/gcs_rpc_client/accessors/actor_info_accessor.cc:183)
  |             rpc::RegisterActorRequest request;
  |             request.mutable_task_spec()->CopyFrom(task_spec.GetMessage());
  |             context_->GetGcsRpcClient().RegisterActor(std::move(request), ...);
  |
  +===== 阶段2: CreateActor RPC -> GCS =====
  |
  |  actor_task_submitter_->SubmitActorCreationTask(task_spec)
  |    (src/ray/core_worker/task_submission/actor_task_submitter.cc:93)
  |    |
  |    |-- resolver_.ResolveDependencies(task_spec, callback)  // 先解析依赖
  |    |
  |    |-- 依赖就绪后:
  |    |     // 注释 (line 113-116):
  |    |     // "The actor creation task will be sent to gcs server directly
  |    |     //  after the in-memory dependent objects are resolved."
  |    |
  |    +-- actor_creator_.AsyncCreateActor(task_spec, callback)
  |          -> ActorInfoAccessor::AsyncCreateActor()
  |             (src/ray/gcs_rpc_client/accessors/actor_info_accessor.cc:226)
  |               rpc::CreateActorRequest request;
  |               request.mutable_task_spec()->CopyFrom(task_spec.GetMessage());
  |               context_->GetGcsRpcClient().CreateActor(std::move(request), ...);
```

#### Proto 定义

```protobuf
// src/ray/protobuf/gcs_service.proto:181-199
service ActorInfoGcsService {
  rpc RegisterActor(RegisterActorRequest) returns (RegisterActorReply);
  rpc CreateActor(CreateActorRequest) returns (CreateActorReply);
  rpc GetActorInfo(GetActorInfoRequest) returns (GetActorInfoReply);
  rpc GetNamedActorInfo(GetNamedActorInfoRequest) returns (GetNamedActorInfoReply);
  rpc GetAllActorInfo(GetAllActorInfoRequest) returns (GetAllActorInfoReply);
  // ...
}
```

### 2.2 GCS 端处理流程

#### 阶段 1: HandleRegisterActor — 元数据注册

```
GcsActorManager::RegisterActor()                    (src/ray/gcs/actor/gcs_actor_manager.cc:664-796)
  |
  |-- 1. 去重处理                                   (line 671-687)
  |     检查 actor_to_register_callbacks_，处理网络重试导致的重复注册
  |
  |-- 2. 命名 Actor 唯一性校验                       (line 701-726)
  |     auto &actors_in_namespace = named_actors_[actor->GetRayNamespace()];
  |     auto it = actors_in_namespace.find(actor->GetName());
  |     if (it != actors_in_namespace.end()) {
  |         return Status::AlreadyExists("Actor with name '...' already exists...");
  |     }
  |     actors_in_namespace.emplace(actor->GetName(), actor->GetActorID());
  |
  |-- 3. Owner 生命周期追踪                          (line 737-744)
  |     if (!actor->IsDetached()) {
  |         PollOwnerForActorRefDeleted(actor);       // 长轮询监控 owner
  |     } else {
  |         runtime_env_manager_.AddURIReference(...); // detached actor 特殊处理
  |     }
  |
  |-- 4. 持久化到存储                                (line 748-793)
  |     写入 ActorTaskSpecTable -> 写入 ActorTable
  |
  |-- 5. 发布 Actor 状态                             (line 780-781)
  |     gcs_publisher_->PublishActor(...)             // 通知 Dashboard 等订阅者
```

#### 阶段 2: HandleCreateActor — 触发调度

```
GcsActorManager::CreateActor()                      (src/ray/gcs/actor/gcs_actor_manager.cc:798-876)
  |
  |-- 1. 查找 registered_actors_ 中的 actor          (line 824)
  |
  |-- 2. 更新状态为 PENDING_CREATION                  (line 859)
  |
  |-- 3. 发布状态变更                                 (line 865)
  |
  |-- 4. 触发调度                                    (line 874)
  |     gcs_actor_scheduler_->Schedule(actor);
  |
  v
GcsActorScheduler::Schedule()                       (src/ray/gcs/actor/gcs_actor_scheduler.cc:49-81)
  |
  |-- SelectForwardingNode(actor)                    (line 83-99)
  |   // 选择目标节点（优先 owner 所在节点，或随机存活节点）
  |
  |-- LeaseWorkerFromNode(actor, node)               (line 234-270)
  |   // 向目标 Raylet 发送 RequestWorkerLease RPC
  |   // Raylet 分配/创建 worker 进程
  |
  |-- HandleWorkerLeaseReply()                       (line 296-365)
  |   // 处理结果: worker 已分配 / spillback 到其他节点 / 失败
```

#### Actor 状态机

```
// src/ray/gcs/actor/gcs_actor_manager.h:49-91

              0                       1                   2        3
    --->DEPENDENCIES_UNREADY--->PENDING_CREATION--->ALIVE ---> RESTARTING
              |                      |              |   <---      ^
            8 |                    7 |            6 |     4       | 9
              |                      v              |             |
               ------------------> DEAD <-------------------------
                                         5
```

### 2.3 为什么 Actor 创建必须经过 GCS

| 功能 | 代码位置 | 为什么 Raylet 不能替代 |
|---|---|---|
| **命名 Actor 全局唯一性** | `gcs_actor_manager.cc:701-726` `named_actors_` map | 需要全局注册表，单个 Raylet 无全局视图 |
| **持久化到存储** | `gcs_actor_manager.cc:748-793` | Raylet 没有持久化层 |
| **Actor 重启 (max_restarts>0)** | `RestartActor()` at `gcs_actor_manager.cc:1445-1573` | 提交者死了就没人重启了 |
| **Detached Actor 生命周期** | `gcs_actor_manager.cc:737-744` | 没有 owner，无人管理 |
| **节点死亡时批量重启** | `OnNodeDead()` at `gcs_actor_manager.cc:1288-1414` | 需要全局 actor-to-node 映射 |
| **Owner 死亡检测和清理** | `PollOwnerForActorRefDeleted()` at `gcs_actor_manager.cc:938-982` | 需要跨节点监控 |
| **Actor 状态查询 (Dashboard)** | `HandleGetAllActorInfo()` at `gcs_actor_manager.cc:487-586` | 需要全局视图 |
| **Actor 地址解析** | `HandleGetActorInfo()` at `gcs_actor_manager.cc:460-485` | 新 caller 需要知道 actor 在哪 |

#### 关键场景分析

**场景 1：Actor 重启**

`core_worker.cc:2243-2248` 明确注释：
```cpp
task_manager_->AddPendingTask(
    rpc_address_, task_spec, CurrentCallSite(),
    // Actor creation task retry happens on GCS not on core worker.
    /*max_retries=*/0);
```

GCS 的 `RestartActor()` 流程（`gcs_actor_manager.cc:1445-1573`）：
- 计算剩余重启次数（排除因节点抢占导致的重启）
- 递增 `num_restarts`，重置 actor 地址
- 持久化新状态，重新调度

如果提交者（owner）也死了，GCS 仍然可以重启 Actor（因为它持有 TaskSpec）。

**场景 2：节点死亡**

`OnNodeDead()` (`gcs_actor_manager.cc:1288-1414`) 遍历该节点上的所有 Actor，逐个判断是否需要重启。单个 Raylet 不知道其他 Raylet 上有哪些 Actor。

**场景 3：Actor Handle 传递**

Worker A 创建 Actor，Worker B 收到 Actor Handle。Worker B 调用 `HandleGetActorInfo()` 从 GCS 获取 Actor 地址。没有 GCS，Worker B 无法解析 Actor 地址。

### 2.4 Actor 创建能否绕过 GCS？

即使是最简单的 Actor（非命名、非 detached、`max_restarts=0`），绕过 GCS 仍然会丢失：

| 功能 | 丢失后果 |
|---|---|
| 地址解析 | 新 caller 无法通过 ActorID 找到 Actor 地址 |
| Dashboard 可观测性 | `ray list actors` 无法显示该 Actor |
| Owner 死亡清理 | Owner 死后 Actor Worker 会泄漏 |
| 状态 Pub/Sub | `ActorTaskSubmitter` 无法知道 Actor 何时 ALIVE/DEAD |
| 放置组集成 | 需要跨 GCS 协调 |

**类比**：GCS 之于 Actor，类似 Kubernetes API Server 之于 Pod — API Server 不直接运行 Pod（kubelet 做的），但它是 Pod 状态的**权威来源**。GCS 不直接调度 Worker（Raylet 做的），但它是 Actor 状态的**权威来源**。

---

## 三、Normal Task 提交 — 不经过 GCS

Normal Task 直接提交到本地 Raylet，由 Raylet 的集群调度器处理。

### 3.1 完整代码链路

```
Python: RemoteFunction._remote()                    (python/ray/remote_function.py:491-509)
  |  object_refs = worker.core_worker.submit_task(
  |      self._language, self._function_descriptor,
  |      list_args, name, num_returns, resources,
  |      max_retries, scheduling_strategy, ...)
  |
  v
Cython: CoreWorker.submit_task()                     (python/ray/_raylet.pyx:3471-3563)
  |  return_refs = CCoreWorkerProcess.GetCoreWorker().SubmitTask(
  |      ray_function, args_vector, task_options, ...)
  |
  v
C++: CoreWorker::SubmitTask()                        (src/ray/core_worker/core_worker.cc:1973-2055)
  |
  |-- 1. 构建 TaskSpec                               (line 2002-2038)
  |     BuildCommonTaskSpec(builder, ..., rpc_address_, ...);
  |     builder.SetNormalTaskSpec(...);
  |
  |-- 2. 注册为 pending task                          (line 2045)
  |     returned_refs = task_manager_->AddPendingTask(
  |         task_spec.CallerAddress(), task_spec, CurrentCallSite(), max_retries);
  |
  |-- 3. 提交到 NormalTaskSubmitter                   (line 2048-2052)
  |     io_service_.post([this, task_spec]() mutable {
  |         normal_task_submitter_->SubmitTask(std::move(task_spec));
  |     }, "CoreWorker.SubmitTask");
  |
  v
NormalTaskSubmitter::SubmitTask()                     (src/ray/core_worker/task_submission/normal_task_submitter.cc:34-95)
  |
  |-- 1. 解析依赖                                    (line 42)
  |     resolver_.ResolveDependencies(task_spec, callback);
  |
  |-- 2. 依赖就绪后入队                               (line 65)
  |     scheduling_key_entry.task_queue.push_back(std::move(task_spec));
  |
  |-- 3. 请求新 Worker                                (line 72)
  |     RequestNewWorkerIfNeeded(scheduling_key);
  |
  v
NormalTaskSubmitter::RequestNewWorkerIfNeeded()       (line 274-532)
  |
  |-- 1. 选择目标 Raylet                              (line 317)
  |     lease_policy_->GetBestNodeForLease()
  |     // 默认 LocalityAwareLeasePolicy: 基于数据局部性选择，回退到本地 Raylet
  |     // (src/ray/core_worker/lease_policy.h:55-82)
  |
  |-- 2. 发送 RequestWorkerLease RPC 给 Raylet        (line 328)
  |     raylet_client->RequestWorkerLease(lease_spec, ...)
  |     // 这是 NodeManagerService RPC -> 发到 Raylet，不是 GCS!
  |     // (src/ray/raylet_rpc_client/raylet_client.cc:53-74)
  |
  v
Raylet: NodeManager::HandleRequestWorkerLease()      (src/ray/raylet/node_manager.cc:1781-1862)
  |
  |-- cluster_lease_manager_.QueueAndScheduleLease(lease, ...)   (line 1857)
  |
  |-- 返回结果:
  |     - Worker 已分配: 返回 Worker 地址
  |     - Spillback: 重定向到另一个 Raylet (仍不经过 GCS)
  |     - 拒绝: 错误处理
  |
  v
NormalTaskSubmitter::PushNormalTask()                 (line 534-640)
  |  // Worker 已分配后，直接发送 task 到 Worker 进程
  |  auto request = std::make_unique<rpc::PushTaskRequest>();
  |  request->mutable_task_spec()->CopyFrom(task_spec.GetMessage());
  |  request->set_intended_worker_id(addr.worker_id());
  |  // PushNormalTask 是 CoreWorkerService RPC -> 发到执行者 Worker
  |  client->PushNormalTask(std::move(request), ...);
```

### 3.2 为什么 Task 不经过 GCS

1. **性能瓶颈**：Normal Task 是 Ray 中最频繁的操作，经过 GCS 会使其成为严重瓶颈
2. **Raylet 就是调度器**：每个 Raylet 拥有集群感知的调度器 (`cluster_lease_manager_`)，资源不足时通过 spillback 重定向到其他 Raylet，全程不涉及 GCS
3. **Task 是临时性的**：不需要全局协调或持久化
4. **Worker 复用**：Worker Lease 模式允许相同 SchedulingKey 的 Task 复用空闲 Worker（`OnWorkerIdle` at line 141），无需反复联系 Raylet

---

## 四、Actor 方法调用 — 直连 Actor Worker

Actor 方法调用（`actor.method.remote()`）既不经过 GCS，也不经过 Raylet。

```
CoreWorker::SubmitActorTask()                        (src/ray/core_worker/core_worker.cc:2353-2461)
  |  actor_task_submitter_->SubmitTask(task_spec);    (line 2457)
  |
  v
ActorTaskSubmitter::SubmitTask()                     (src/ray/core_worker/task_submission/actor_task_submitter.cc:168-266)
  |-- ResolveDependencies() -> SendPendingTasks()    (line 224)
  |     -> PushActorTask()                           (line 573)
  |
  v
PushActorTask()                                      (line 634)
  |  // 直接发到 Actor Worker 进程
  |  core_worker_client_pool_.GetOrConnect(addr)->PushActorTask(
  |      std::move(request), skip_queue, std::move(wrapped_callback));
```

Actor 的地址是通过 GCS 的 Actor 状态订阅获得的（创建时已知），后续方法调用直接走 worker-to-worker RPC。

---

## 五、Object 存储 — 不经过 GCS（Ownership 模型）

这是 Ray 架构的一个重要演进：**早期版本使用 GCS 的 `ObjectTable` 追踪对象位置，现在已完全替换为基于 Ownership 的对象目录**。搜索 `GcsObjectDirectory` 返回零结果，确认 GCS 对象目录已被完全移除。

### 5.1 Object Put 链路

```
Python: ray.put(value)                               (python/ray/_private/worker.py:3019-3081)
  |
  v
Cython: put_serialized_object_and_increment_local_ref (python/ray/_raylet.pyx:3154-3250)
  |  1. CCoreWorkerProcess.GetCoreWorker()
  |       .CreateOwnedAndIncrementLocalRef(...)       // 分配 buffer
  |  2. CCoreWorkerProcess.GetCoreWorker()
  |       .SealOwned(...)                             // 密封对象
  |
  v
C++: CoreWorker::CreateOwnedAndIncrementLocalRef()   (src/ray/core_worker/core_worker.cc:1038-1137)
  |
  |-- 1. 生成 ObjectID
  |     *object_id = ObjectID::FromIndex(
  |         worker_context_->GetCurrentInternalTaskId(),
  |         worker_context_->GetNextPutIndex());
  |
  |-- 2. 注册 ownership (owner = 当前 Worker)         (line 1059-1067)
  |     reference_counter_->AddOwnedObject(
  |         *object_id, contained_object_ids,
  |         rpc_address_,                             // <--- owner = self
  |         CurrentCallSite(), data_size + metadata->Size(),
  |         LineageReconstructionEligibility::INELIGIBLE_PUT,
  |         /*add_local_ref=*/true,
  |         NodeID::FromBinary(rpc_address_.node_id()));
  |
  |-- 3. 在本地 Plasma 分配共享内存                    (line 1111)
  |     plasma_store_provider_->Create(metadata, data_size, *object_id, ...);
  |
  v
CoreWorker::PutInLocalPlasmaStore()                  (src/ray/core_worker/core_worker.cc:987-1014)
  |
  |-- plasma_store_provider_->Put(object, object_id, rpc_address_, ...)
  |     (src/ray/core_worker/store_provider/plasma_store_provider.cc:98-124)
  |     // memcpy 数据到共享内存, 然后 Seal 使其不可变
  |
  |-- local_raylet_rpc_client_->PinObjectIDs(rpc_address_, {object_id}, ...)
  |     // 通知本地 Raylet pin 对象，防止被驱逐
```

**关键**：数据直接写入本地 Plasma 共享内存，ownership 元数据保存在创建者 Worker 的进程内 `ReferenceCounter`（`reference_counter.h:437` 的 `owner_address_` 字段），**完全不涉及 GCS**。

### 5.2 Object Get 链路（本地）

```
CoreWorker::Get(object_ids)                          (src/ray/core_worker/core_worker.cc:1300)
  |
  v
CoreWorker::GetObjects()                             (line 1353-1417)
  |
  |-- 1. 先查进程内 memory_store                      (line 1385)
  |     memory_store_->Get(memory_object_ids, timeout_ms, ...)
  |     // 小对象/inline 对象在内存中
  |
  |-- 2. 发现 IsInPlasmaError 标记 -> 转向 Plasma     (line 1391-1398)
  |     // 大对象在 Plasma 共享内存中
  |
  |-- 3. 读取 Plasma                                  (line 1416)
  |     plasma_store_provider_->Get(object_ids, owner_addresses, timeout_ms, ...)
  |     // 本地 Plasma 读取是纯本地操作，不联系任何远程组件
```

### 5.3 Object Get 链路（跨节点）

当 object 不在本地时，需要从远程节点拉取。核心问题：**调用方怎么知道 object 在哪？**

**答案：调用方不需要知道 object 在哪个节点上，它只需要知道 owner 是谁，然后问 owner 要位置信息。**

#### 完整链路

```
CoreWorker::Get(object_ids)                          (core_worker.cc:1300)
  |
  v
CoreWorker::GetObjects()                             (core_worker.cc:1353)
  |
  |-- owner_addresses = reference_counter_
  |       ->GetOwnerAddresses(object_ids)             (reference_counter.cc:678-701)
  |   // 从本地 ref table 查出每个 object 的 owner 地址
  |   // owner_address 是 ObjectRef 创建时嵌入的，一路传递不变
  |
  v
plasma_store_provider_->Get(
    object_ids, owner_addresses, ...)                 (plasma_store_provider.cc:253)
  |
  |-- raylet_ipc_client_->AsyncGetObjects(
  |       batch_ids, batch_owner_addresses, ...)      (raylet_ipc_client.cc:194-214)
  |   // 通过 Unix socket 发 flatbuffer IPC 给本地 Raylet
  |   // 消息格式 (src/ray/flatbuffers/node_manager.fbs:128-133):
  |   //   table AsyncGetObjectsRequest {
  |   //     object_ids: [string];
  |   //     owner_addresses: [Address];   <--- owner 地址随 object_id 一起发
  |   //     get_request_id: long;
  |   //   }
  |
  === Worker -> Raylet (本地 IPC, Unix Domain Socket) ===
  |
  v
NodeManager::HandleAsyncGetObjectsRequest()          (node_manager.cc:1598-1604)
  |-- FlatbufferToObjectReferences(...)               // 重建 rpc::ObjectReference(id + owner_addr)
  |     (node_manager.cc:68-90)
  |     // 每个 ObjectReference 包含 object_id + owner_address
  |
  v
NodeManager::AsyncGet()                              (node_manager.cc:2316-2326)
  |
  v
LeaseDependencyManager::StartGetRequest()            (lease_dependency_manager.cc:118-141)
  |-- 存储 owner_address 到 ObjectDependencies 结构体
  |-- object_manager_.Pull(
  |       std::move(required_objects),                // 包含 owner_address
  |       BundlePriority::GET_REQUEST, ...)           (line 132)
  |
  v
ObjectManager::Pull(object_refs)                     (object_manager.cc:214-245)
  |
  |-- pull_manager_->Pull(object_refs, ..., &objects_to_locate)  (line 218)
  |   // PullManager 只返回首次遇到的 object (pull_manager.cc:76-80)
  |   //   auto it = object_pull_requests_.find(obj_id);
  |   //   if (it == object_pull_requests_.end()) {
  |   //       objects_to_locate->push_back(ref);  // 需要订阅位置
  |   //   }
  |
  |-- 对每个新 object，发起位置订阅:                    (line 234-242)
  |     for (const auto &ref : objects_to_locate) {
  |         object_directory_->SubscribeObjectLocations(
  |             callback_id, object_id,
  |             ref.owner_address(),                  // <--- 用 owner 地址发起订阅
  |             callback);
  |     }
  |
  v
OwnershipBasedObjectDirectory
::SubscribeObjectLocations()                         (ownership_object_directory.cc:320-418)
  |
  |-- 构建 WorkerObjectLocationsSubMessage:
  |     request.set_intended_worker_id(owner_address.worker_id());
  |     request.set_object_id(object_id.Binary());
  |
  |-- 通过 pubsub 订阅 owner worker:                  (line 366-373)
  |     object_location_subscriber_->Subscribe(
  |         std::move(sub_message),
  |         rpc::ChannelType::WORKER_OBJECT_LOCATIONS_CHANNEL,
  |         owner_address,                            // <--- 目标: owner worker
  |         object_id.Binary());                      // <--- key: object_id
  |
  v
Subscriber::Subscribe()                              (pubsub/subscriber.cc:257-285)
  |-- SendCommandBatchIfPossible(owner_address)       // gRPC: PubsubCommandBatch -> owner
  |-- MakeLongPollingConnection(owner_address)        // gRPC: PubsubLongPolling  -> owner
  |     (subscriber.cc:297-314)
  |     // 建立长轮询连接，owner 有数据时回复
  |
  === Raylet -> Owner Worker (gRPC 长轮询) ===
  |
  v
Owner Worker 收到订阅请求:

  CoreWorker::HandlePubsubCommandBatch()             (core_worker.cc:3752-3788)
    -> ProcessSubscribeMessage()                     (core_worker.cc:3710-3739)
      |
      |-- Publisher::RegisterSubscription()           (publisher.cc:393-419)
      |   // 在 SubscriptionIndex 中注册订阅者
      |
      |-- ProcessSubscribeObjectLocations()           (core_worker.cc:3907-3923)
      |     // 验证 intended_worker_id 是否是自己
      |
      v
      reference_counter_
        ->PublishObjectLocationSnapshot(object_id)    (reference_counter.cc:1731-1755)
        |
        |  Owner 的 ReferenceCounter 维护着 locations:
        |    object_id -> Reference {
        |      locations: {NodeA, NodeB},       // 哪些节点有副本
        |      object_size_: 1024,
        |      spilled_url: "",                 // spill 到外部存储的 URL
        |      spilled_node_id: Nil,
        |      pending_creation_: false,
        |    }
        |
        v
      PushToLocationSubscribers()                    (reference_counter.cc:1678-1698)
        -> FillObjectInformationInternal()            (reference_counter.cc:1716-1729)
        |    // 填充 node_ids, object_size, spilled_url, pending_creation
        -> Publisher::Publish(pub_message)            (publisher.cc:421-434)
          -> SubscriptionIndex::Publish()
            -> EntityState::Publish()                 (publisher.cc:28-91)
              -> SubscriberState::QueueMessage()       (publisher.cc:300)
                -> PublishIfPossible()                 (publisher.cc:306-349)
                   // 填充长轮询回复，发送 gRPC reply
  |
  === Owner Worker -> Raylet (gRPC 回复) ===
  |
  v
Subscriber::HandleLongPollingResponse()              (subscriber.cc:316-387)
  |-- SubscriberChannel::HandlePublishedMessage()     (subscriber.cc:118-148)
  |     // post callback 到事件循环
  |
  v
OwnershipBasedObjectDirectory
::ObjectLocationSubscriptionCallback()               (ownership_object_directory.cc:260-318)
  |-- UpdateObjectLocations()                         // 解析 node_ids, spilled_url
  |-- 调用所有注册的 callback
  |
  v
pull_manager_->OnLocationChange(
    object_id, {NodeA, NodeB}, ...)                   (pull_manager.cc:362)
  |-- 更新 client_locations
  |
  v
PullManager::TryToMakeObjectLocal()                  (pull_manager.cc:446)
  |
  v
PullManager::PullFromRandomLocation()                (pull_manager.cc:511-545)
  |  // 从 {NodeA, NodeB} 随机选一个
  |  std::uniform_int_distribution<int> distribution(0, node_vector.size() - 1);
  |  int node_index = distribution(gen_);
  |  NodeID node_id = node_vector[node_index];
  |  send_pull_request_(object_id, node_id);
  |
  v
ObjectManager::SendPullRequest(object_id, NodeB)     (object_manager.cc:255-281)
  |  auto rpc_client = GetRpcClient(client_id);
  |  rpc_client->Pull(pull_request, callback);
  |
  === Raylet -> 远程 Raylet (gRPC) ===
  |
  v
远程 ObjectManager::HandlePull()                     (object_manager.cc:616-627)
  -> Push(object_id, requesting_node_id)
    -> PushObjectInternal() -> SendObjectChunk()      // 分块推送数据
  |
  v
本地 ObjectManager::HandlePush()
  -> ReceiveObjectChunk()                             (object_manager.cc:543-614)
    -> buffer_pool_.CreateChunk()                     // 在本地 Plasma 分配空间
    -> buffer_pool_.WriteChunk()                      // 写入数据
    -> store_client_->Seal()                          // 密封，数据可读
  |
  v
ray.get() 从本地 Plasma 返回数据
```

### 5.4 Owner 地址的完整生命周期

**核心不变量**：无论 ObjectRef 经过多少次传递（A -> B -> C -> D），`owner_address` 始终指向最初的创建者，不会变。

#### ObjectReference Proto 定义

```protobuf
// src/ray/protobuf/common.proto:713-725
message ObjectReference {
  bytes object_id = 1;
  Address owner_address = 2;   // <--- 每个 ObjectRef 都携带 owner 地址
  string call_site = 3;
  optional string tensor_transport = 4;
}

message Address {
  bytes node_id = 1;
  string ip_address = 2;
  int32 port = 3;
  bytes worker_id = 4;
}
```

#### 场景 1：Task 返回值 — `result = func.remote(args)`

调用者（driver/worker）就是 owner。

```
CoreWorker::SubmitTask()                             (core_worker.cc:2002)
  |-- BuildCommonTaskSpec(..., rpc_address_, ...)     // 自己的地址作为 caller_address
  |
  v
TaskManager::AddPendingTask(caller_address, ...)     (task_manager.cc:238-318)
  |
  |-- 对每个 return value (line 275-308):
  |     reference_counter_.AddOwnedObject(
  |         return_id, caller_address, ...);          // owner = caller
  |
  |     rpc::ObjectReference ref;
  |     ref.set_object_id(return_id);
  |     ref.mutable_owner_address()
  |         ->CopyFrom(caller_address);               // 嵌入 owner 地址
  |     returned_refs.push_back(ref);
```

**注意**：owner 是提交 task 的 worker，不是执行 task 的 worker。

#### 场景 2：ray.put() — owner 就是自己

```
CoreWorker::Put()                                    (core_worker.cc:966)
  |-- reference_counter_->AddOwnedObject(
  |       object_id, rpc_address_, ...);              // owner = self
```

#### 场景 3：ObjectRef 作为 task 参数传递

```
=== 提交方 ===

GetOwnershipInfo(object_id)                          (core_worker.cc:913-940)
  -> reference_counter_->GetOwner(object_id, &owner_address)
     (reference_counter.cc:657-676)
     // 从本地 ref table 取出 owner_address

TaskArgByReference(object_id, owner_address)         (task_util.h:55-83)
  -> arg_proto.object_ref.owner_address = owner_address  // 序列化进 TaskSpec

=== 网络传输: TaskSpec 通过 gRPC 发到执行者 ===

=== 执行方 ===

GetAndPinArgsForExecutor(task_spec)                  (core_worker.cc:3336-3412)
  |-- task.ArgRef(i).owner_address()                  // 从 TaskSpec 中取出 owner_address
  |     (task_spec.cc:287-290)
  |
  |-- reference_counter_->AddBorrowedObject(
  |       arg_id, ObjectID::Nil(),
  |       task.ArgRef(i).owner_address());            // 存入本地 ref table
  |     (reference_counter.cc:115-155)
  |     // 设置 it->second.owner_address_ = owner_address
```

#### 场景 4：ObjectRef 嵌入 Python 对象中（pickle 序列化）

例如 `func.remote([ref1, ref2])` 把 ObjectRef 放在 list 里 pass by value：

```
=== 序列化方 ===

serialization.py:218:
  worker.core_worker.serialize_object_ref(obj)
    -> GetOwnershipInfo() -> 取出 owner_address
    -> pickle 时把 owner_address 一起序列化

=== 反序列化方 ===

serialization.py:81:
  _object_ref_deserializer(binary, owner_address, ...)
    -> worker.core_worker.deserialize_and_register_object_ref(
           object_id, owner_address)
      -> RegisterOwnershipInfoAndResolveFuture()      (core_worker.cc:943-963)
        -> reference_counter_->AddBorrowedObject(object_id, owner_address)
```

#### 完整流转图

```
创建                           传输                           消费
===                           ===                           ===

func.remote(args)             TaskSpec protobuf              执行者收到 task
  |                             |                             |
  v                             v                             v
CoreWorker::SubmitTask()      caller_address (field 10)     GetAndPinArgsForExecutor()
  |                                                           |
  v                           ObjectReference protobuf        v
TaskManager::AddPendingTask() owner_address (field 2)       AddBorrowedObject()
  |                             |                             |
  v                           Python pickle                   v
AddOwnedObject(rpc_address_)  owner_address 一起序列化       Reference.owner_address_
  |                             |                             |
  v                             v                             v
Reference.owner_address_      _object_ref_deserializer()    ray.get() 时用
  = rpc_address_ (SELF)       RegisterOwnership...          owner_address 找 owner
```

### 5.5 Object 多副本机制

Object 会在多个节点上产生副本，这不是主动复制，而是**按需拉取的自然结果**。

#### 副本产生过程

```
初始: Task 在 Node A 执行，产出 object

1. Node A Plasma Seal 触发 add_object_callback_
   (src/ray/object_manager/plasma/store.cc:275-286)
     void PlasmaStore::SealObjects(const std::vector<ObjectID> &object_ids) {
       for (size_t i = 0; i < object_ids.size(); ++i) {
         auto entry = object_lifecycle_mgr_.SealObject(object_ids[i]);
         add_object_callback_(entry->GetObjectInfo());  // 每次 Seal 都触发
       }
     }

   -> ObjectManager::HandleObjectAdded()              (object_manager.cc:171-198)
     -> OwnershipBasedObjectDirectory::ReportObjectAdded()
        (ownership_object_directory.cc:121-142)
          // 构建 ObjectLocationUpdate{ADDED}
          // 发送 UpdateObjectLocationBatch RPC -> Owner
     -> Owner: reference_counter_->AddObjectLocation(objID, NodeA)
        (reference_counter.cc:1443-1467)
     -> Owner: PushToLocationSubscribers() -> 发布 {NodeA}

2. Node B 需要此 object (task 参数依赖)
   -> 订阅 Owner，得知 locations={NodeA}
   -> PullFromRandomLocation() -> SendPullRequest(NodeA)
   -> Node A Push 数据给 Node B
   -> Node B Plasma Seal 触发同样的 add_object_callback_   // 关键: 同一个回调!
     -> ReportObjectAdded() RPC -> Owner
     -> Owner: AddObjectLocation(objID, NodeB)
     -> Owner.locations = {NodeA, NodeB}

3. Node C 也需要此 object
   -> 从 Owner 得知 locations={NodeA, NodeB}
   -> PullFromRandomLocation() 随机选 NodeA 或 NodeB    // 负载分散
   -> 拉取成功后 Seal -> ReportObjectAdded -> Owner
   -> Owner.locations = {NodeA, NodeB, NodeC}

4. Node D, E, F... 同理 -> 自然形成 P2P 加速效果
```

**核心机制**：Plasma 的 `SealObjects()` 是统一触发点。无论 object 是本地 task 创建的还是从远程 pull 来的，Seal 时都会触发 `add_object_callback_`，向 owner 上报自己的位置。这使得 locations 集合自然增长，后续节点拉取时随机选择已有副本，负载自动分散。

#### locations 集合缩小的情况

```
- Plasma LRU 驱逐:
    ReportObjectRemoved() RPC -> Owner
    -> reference_counter_->RemoveObjectLocation(objID, nodeID)
       (reference_counter.cc:1469-1490)

- 节点死亡:
    GCS 广播 NodeRemoved 事件
    -> reference_counter_->ResetObjectsOnRemovedNode(nodeID)
       (reference_counter.cc:893-908)
       // 移除死节点上所有 object 的位置记录
```

### 5.6 Owner 死亡场景分析

#### 结论

| 场景 | ray.get() 能否成功 | 原因 |
|---|---|---|
| Object **已在本地 Plasma** | **能**（大概率） | 直接从本地 Plasma 读，不需要 owner |
| Object **在远程节点**，owner 挂了 | **不能** | 无法发现远程副本在哪 |
| Owner 挂了但 lineage 可重建 | **有条件能** | 只有 owner 自己能重建 |

#### 场景 1：Object 已在本地 Plasma — 能成功

`CoreWorker::GetObjects()` 的流程是：先查进程内存，再查本地 Plasma。本地 Plasma 读取是纯本地操作（共享内存 mmap），不联系 owner。

Owner 死亡后，系统会在 Plasma 中写入错误对象：

```
OwnershipBasedObjectDirectory failure callback       (ownership_object_directory.cc:341-346)
  -> mark_as_failed_(obj_id, rpc::ErrorType::OWNER_DIED)
    -> NodeManager::MarkObjectsAsFailed()            (node_manager.cc:2251-2290)
      -> store_client_->TryCreateImmediately(...)    // 试图写入错误对象
```

但 `TryCreateImmediately` 如果发现 object 已存在于 Plasma，**不会覆盖**，返回 `ObjectExists`。所以真实数据先到时，`ray.get()` 正常返回。

#### 场景 2：Object 在远程节点，Owner 挂了 — 不能成功

即使 Node B 上有完整的 object 副本，Node C 也**无法发现**：

```
Node C 想 get object:
  -> SubscribeObjectLocations(owner_address)
    -> Subscriber 建立长轮询到 Owner Worker
    -> Owner 已死，gRPC 失败

       Subscriber::HandleLongPollingResponse()       (subscriber.cc:322-330)
         if (!status.ok()) {
           // "A worker is dead. subscription_failure_callback will be invoked."
           HandlePublisherFailure(publisher_address, status);
         }

    -> HandlePublisherFailure()                      (subscriber.cc:150-175)
      -> failure_callback 触发

       OwnershipBasedObjectDirectory:                (ownership_object_directory.cc:341-346)
         if (!status.ok()) {
           mark_as_failed_(obj_id, rpc::ErrorType::OWNER_DIED);
         }

    -> NodeManager::MarkObjectsAsFailed()            // 在 Plasma 写入 OWNER_DIED 错误
    -> ray.get() 返回 OwnerDiedError 异常
```

同时，`FutureResolver` 也会检测到 owner 死亡：

```
FutureResolver::ResolveFutureAsync()                 (future_resolver.cc:23-57)
  -> GetObjectStatus RPC -> Owner
  -> RPC 失败:
       if (!status.ok()) {
         in_memory_store_->Put(
             RayObject(rpc::ErrorType::OWNER_DIED),
             object_id, ...);                         // 在内存中放入错误对象
       }
```

Python 层抛出异常：

```python
# python/ray/exceptions.py:744-773
class OwnerDiedError(ObjectLostError):
    """Indicates that the owner of the object has died while there is still a
    reference to the object."""

# python/ray/_private/serialization.py:519-522
elif error_type == ErrorType.Value("OWNER_DIED"):
    return OwnerDiedError(
        object_ref.hex(), object_ref.owner_address(), object_ref.call_site()
    )
```

**Node B 上的 object 副本变成孤儿数据**，无人引用，最终被 Plasma LRU 驱逐。

#### 场景 3：Lineage 重建 — 有条件能成功

满足以下所有条件时，object 可通过重新执行原始 task 恢复：

1. 调用 `ray.get()` 的 worker **就是 owner 本身**
2. Object 是 **task 返回值**（不是 `ray.put()` 的，put 没有 task lineage）
3. `max_retries > 0` 且重试次数未耗尽
4. Lineage 没有被内存压力驱逐

```
// src/ray/core_worker/reference_counter_interface.h:33-76
enum class LineageReconstructionEligibility {
    ELIGIBLE,                        // 可重建
    INELIGIBLE_PUT,                  // ray.put() 没有 task lineage
    INELIGIBLE_NO_RETRIES,           // max_retries=0
    INELIGIBLE_LOCAL_MODE,           // 本地模式
    INELIGIBLE_LINEAGE_EVICTED,      // lineage 被驱逐
    INELIGIBLE_LINEAGE_DISABLED,     // 系统级禁用
    INELIGIBLE_REF_NOT_FOUND,        // 引用未找到
};
```

恢复流程（每 100ms 检查一次）：

```
CoreWorker 定期任务                                  (core_worker.cc:467-491)
  -> reference_counter_->FlushObjectsToRecover()
    -> ObjectRecoveryManager::RecoverObject()        (object_recovery_manager.cc:24-91)
      |
      |-- 1. 检查是否 pinned 或 spilled -> 如果有副本，pin 它
      |     (line 93-138)
      |
      |-- 2. 如果没有副本，重建:
      |     ReconstructObject()                       (line 140-188)
      |       -> GetLineageReconstructionEligibility()
      |       -> task_manager_.ResubmitTask(task_id)  // 重新提交原始 task
      |       -> 递归恢复 task 的输入依赖
```

#### Owner 死亡完整时序

```
t0: Owner Worker 所在节点崩溃

t1: GCS 检测到节点死亡，广播 NodeRemoved 事件

t2: 各节点的 Subscriber 检测到长轮询 gRPC 失败
    |
    +-- Raylet 路径:
    |     OwnershipBasedObjectDirectory failure callback
    |     -> mark_as_failed_(OWNER_DIED)
    |     -> NodeManager::MarkObjectsAsFailed()
    |     -> Plasma 写入 OWNER_DIED 错误对象
    |
    +-- CoreWorker 路径:
          FutureResolver: GetObjectStatus RPC 失败
          -> memory_store_->Put(OWNER_DIED error)

t3: ray.get() 读到错误对象
    -> Python 抛出 OwnerDiedError

t4: 远程节点上的 object 副本成为孤儿
    -> 无人引用，最终被 Plasma LRU 驱逐
```

#### 设计权衡

**为什么不把位置信息备份到 GCS？**

早期 Ray 确实这么做过（GCS `ObjectTable` 方案），但被替换了：

1. **GCS 瓶颈**：大规模集群中 object 数量极大（百万级），每个 object 的位置更新都走 GCS 会使 GCS 成为严重瓶颈
2. **延迟**：位置查询多一跳（worker -> GCS -> worker），对延迟敏感的场景影响大
3. **Owner 模型更自然**：owner 就是提交 task 的 worker，天然需要知道返回值在哪里。让 owner 充当"微型目录服务"，把全局问题分解成分布式的局部问题

**代价**：Object 与 Owner 命运绑定（fate sharing）。Owner 死了，即使 object 数据还散布在集群的多个节点上，也无法被发现和使用。这是有意的设计取舍：用 owner 单点风险换取去中心化的性能优势。

---

## 六、架构总览图

```
                          ┌────────────────────────┐
                          │      GCS Server         │
                          │                        │
                          │  - Actor 注册/调度      │ <── Actor 创建必须经过
                          │  - 节点管理             │
                          │  - 命名 Actor 管理      │
                          │  - 放置组管理           │
                          │  - Actor 重启协调       │
                          └───────────┬────────────┘
                                      │ (仅 Actor 创建 + 节点存活)
                                      │
   ┌───────────────┐          ┌───────────────┐          ┌───────────────┐
   │   Worker A     │          │   Raylet       │          │   Worker B     │
   │ (Owner)        │          │               │          │               │
   │                │          │               │          │               │
   │ Task 提交 ──────────────> │ Task 调度     │          │               │
   │                │          │ Worker Lease   │          │               │
   │ Object Put ──> │ Plasma   │               │          │               │
   │                │ (共享内存)│               │          │               │
   │ Owner: 追踪    │          │ Object Pull   │          │               │
   │ 对象位置       │<──────── │ 管理           │          │               │
   │                │ 位置上报  │               │          │               │
   │ ReferenceCounter:         │               │          │               │
   │  obj1 -> {NodeA,NodeB}    │               │          │               │
   │  obj2 -> {NodeA}          │               │          │               │
   └───────────────┘          └───────────────┘          └───────────────┘
         │                                                      ^
         │                                                      │
         │   Actor 方法调用 (直连, 不经过 GCS 和 Raylet)          │
         └──────────────────────────────────────────────────────┘

=== 各操作路径对比 ===

Normal Task:   CoreWorker ---> Raylet ---> Worker        (去中心化，短生命周期)

Actor 创建:    CoreWorker ---> GCS ---> Raylet ---> Worker
                                ^
                                |
                          权威状态管理者

Actor 调用:    CoreWorker --------直连--------> Actor Worker  (最短路径)

Object Put:    CoreWorker ---> 本地 Plasma (共享内存)         (纯本地)

Object Get:    CoreWorker -> Raylet -> Owner Worker(查位置) -> 远程 ObjectManager
(跨节点)                                                     (去中心化)
```
