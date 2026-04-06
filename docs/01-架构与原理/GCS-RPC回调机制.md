# GCS RPC 回调机制与回复链路深度分析

> 关联文档: [GCS Server 线程瓶颈分析与定位指南](./gcs-thread-analyze.md)

---

## 一、Service 回调注册与 IO Context 路由

### 1.1 回调注册流程（4步）

**Step 1: 定义 Handler 接口** (`src/ray/gcs/grpc_service_interfaces.h`)

```cpp
class ActorInfoGcsServiceHandler {
 public:
  virtual void HandleRegisterActor(RegisterActorRequest request,
                                   RegisterActorReply *reply,
                                   SendReplyCallback send_reply_callback) = 0;
  virtual void HandleCreateActor(CreateActorRequest request,
                                 CreateActorReply *reply,
                                 SendReplyCallback send_reply_callback) = 0;
};
```

**Step 2: GrpcService 包装** (`src/ray/gcs/grpc_services.cc`)

```cpp
void ActorInfoGrpcService::InitServerCallFactories(...) {
  RPC_SERVICE_HANDLER(ActorInfoGcsService, RegisterActor, -1)
  RPC_SERVICE_HANDLER(ActorInfoGcsService, CreateActor, -1)
}
```

RPC_SERVICE_HANDLER 宏创建 ServerCallFactoryImpl，将 gRPC AsyncService 的请求方法与 Handler 的处理方法绑定。

**Step 3: Manager 实现接口** (`src/ray/gcs/actor/gcs_actor_manager.h`)

```cpp
class GcsActorManager : public rpc::ActorInfoGcsServiceHandler {
 public:
  void HandleRegisterActor(...) override;
  void HandleCreateActor(...) override;
};
```

**Step 4: GcsServer 注册服务时指定 IO Context** (`src/ray/gcs/gcs_server.cc`)

```cpp
// ActorManager -> 默认主线程
rpc_server_.RegisterService(std::make_unique<rpc::ActorInfoGrpcService>(
    io_context_provider_.GetDefaultIOContext(), *gcs_actor_manager_, ...));

// TaskManager -> 独立 IO Context
rpc_server_.RegisterService(std::make_unique<rpc::TaskInfoGrpcService>(
    io_context_provider_.GetIOContext<GcsTaskManager>(), *gcs_task_manager_, ...));
```

### 1.2 IO Context 路由策略

**编译期类型分派** (`src/ray/gcs/gcs_server_io_context_policy.h`):

```cpp
struct GcsServerIOContextPolicy {
  template <typename T>
  static constexpr int GetDedicatedIOContextIndex() {
    if constexpr (std::is_same_v<T, GcsTaskManager>)       -> task_io_context (0)
    else if constexpr (std::is_same_v<T, pubsub::GcsPublisher>) -> pubsub_io_context (1)
    else if constexpr (std::is_same_v<T, syncer::RaySyncer>)    -> ray_syncer_io_context (2)
    else if constexpr (std::is_same_v<T, observability::RayEventRecorder>) -> ray_event_io_context (3)
    else -> -1  // 返回 default (主线程)
  }
};
```

**判断规则**：

| Manager/组件 | IO Context | 判断依据 |
|-------------|-----------|---------|
| GcsTaskManager | task_io_context | 返回 0 |
| GcsPublisher | pubsub_io_context | 返回 1 |
| RaySyncer | ray_syncer_io_context | 返回 2 |
| RayEventRecorder | ray_event_io_context | 返回 3 |
| 其他所有（Actor/Node/Job/KV/PG...） | 主线程(default) | 返回 -1 |

---

## 二、四个线程池/机制之间的关系

### 2.1 完整请求生命周期

```
1. SERVER POLLING 线程: AsyncNext() -> 收到请求 -> HandleRequest() -> post 到 io_context
2. IO_CONTEXT 线程: HandleRequestImpl() -> 执行业务逻辑 -> handler 完成
3. SendReply 全局线程池: response_writer_.Finish() - gRPC 网络 I/O
4. 回到 SERVER POLLING 线程: delete call -> CreateCall() (背压释放)
```

### 2.2 各机制详解

**1. Server Polling 线程** — 入口
- 每个线程独占一个 CompletionQueue
- 工作极轻量：反序列化 + post() 到 io_context，不执行业务逻辑
- 还负责回收已发送完回复的 ServerCall 并创建新的（背压释放点）

**2. Client Polling 线程** — 出口（GCS 作为客户端）
- GCS 有两个 ClientCallManager：
  - client_call_manager_：挂载在主线程，轮询 raylet/worker 的响应
  - event_aggregator_client_call_manager_：挂载在 ray_event_io_context
- 与 Server Polling 不竞争（独立 CompletionQueue + 独立线程）
- 但客户端回调 post 回 main_service_，会与入站请求的回调共享主线程

**3. Max Active RPCs** — 背压控制
- 不是显式计数器，而是通过预创建 ServerCall 数量隐式控制
- 启动时每个线程预创建 max_active_rpcs / num_threads 个 PENDING 状态的 ServerCall
- 所有预创建的 call 都在 PROCESSING 时，没有 PENDING call 接受新请求
- 特殊：RegisterActor、CreateActor 等设为 -1（无限），避免死锁

**4. SendReply 全局线程池** — 回复发送
- 全局单例 boost::asio::thread_pool (src/ray/rpc/server_call.cc:23-38)
- 隔离 response_writer_.Finish() 的网络 I/O，防止阻塞业务线程

### 2.3 关键配置关系

| 参数 | 默认值 | 关系 |
|-----|--------|------|
| gcs_server_rpc_server_thread_num | max(1, CPU/4) | 每线程一个 CQ，分摊入站负载 |
| gcs_server_rpc_client_thread_num | max(1, CPU/4) | 每个 ClientCallManager 独立 N 线程 |
| gcs_max_active_rpcs_per_handler | server_thread_num x 100 | 每线程预创建 100 个 call 做背压 |
| num_server_call_thread | max(1, CPU/4) | 全局共享，处理所有服务的回复发送 |

---

## 三、post() 机制

post() 是 boost::asio io_context 的任务调度原语，本质是将函数投递到事件队列，由 io_context 的事件循环择机执行。

### 3.1 核心语义

- post() = 非阻塞投递：把回调塞进 io_context 的任务队列，立即返回
- 不会立即执行：回调在 io_context 的事件循环轮到它时才执行
- 类比：发消息到消息队列，消费者（事件循环线程）异步消费

### 3.2 单线程 io_context 的关键特性

GCS 主线程是单线程事件循环：

```
时间线：
T0: post(task_A)  -> 入队
T1: post(task_B)  -> 入队
T2: 事件循环取出 task_A 执行（耗时5秒）
T3: task_A 执行期间，task_B 在队列等待
T7: task_A 完成，取出 task_B 执行
```

post() 不等于立即执行，而是排队等轮到才执行。

### 3.3 为什么要 post 而不直接调用

1. **线程安全**：不同线程不能直接调用同一个对象的成员函数，post 保证回调在目标线程执行
2. **非阻塞**：投递方不用等回调完成，继续做自己的事
3. **串行化**：单线程 io_context 保证所有回调串行执行，无需加锁

---

## 四、SendReply 回调链详解

### 4.1 一次请求的 3 次 post

```
Server Polling线程          组件io_context线程           SendReply全局线程池
     |                           |                          |
     | 1. AsyncNext->PENDING     |                          |
     | HandleRequest()           |                          |
     | --post(io_service_)-->    |                          |
     |                    HandleRequestImpl()               |
     |                    handler调用send_reply_callback    |
     |                           --post(Executor())-->      |
     |                           |                   SendReply()
     |                           |                   response_writer_.Finish()
     | 2. AsyncNext->SENDING_REPLY                       |
     | OnReplySent()              |                          |
     | --post(io_service_)-->    |                          |
     |                    success_callback()                |
```

### 4.2 四个回调的关系

| 回调 | 在哪定义 | 在哪调用 | 运行线程 | 作用 |
|------|---------|---------|---------|------|
| send_reply_callback | server_call.h:243 | handler 业务代码中调用 | 组件 io_context 线程 | handler 完成后触发回复发送 |
| SendReply() | server_call.h:384 | send_reply_callback 内 post 到全局线程池 | SendReply 全局线程池 | 调用 Finish() 发送网络回复 |
| OnReplySent() | server_call.h:260 | Server Polling 线程检测到 SENDING_REPLY 事件 | Server Polling 线程 | 回复发送成功后清理 + post success_callback |
| success_callback() | handler 传入 | OnReplySent 内 post 到 io_context | 组件 io_context 线程 | 回复成功后的业务回调 |

### 4.3 为什么不能直接在当前线程完成

**SendReply 不能在 io_context 线程执行**：response_writer_.Finish() 是 gRPC 网络操作，可能阻塞。在单线程 io_context 上阻塞 = 所有业务逻辑停摆。

**success_callback 不能在 Server Polling 线程执行**：回调访问组件内部状态，不是线程安全的。post 回 io_context 保证串行执行。

### 4.4 同进程 vs 跨进程

| 操作 | 类型 | 说明 |
|------|------|------|
| post(io_service_, HandleRequestImpl) | 同进程 | 线程间任务投递 |
| handler 业务逻辑 | 同进程 | 本地计算 |
| post(Executor(), SendReply) | 同进程 | 线程间任务投递 |
| **response_writer_.Finish()** | **跨进程** | gRPC 网络发送回复给客户端 |
| OnReplySent() | 同进程 | gRPC CQ 事件回调 |
| post(io_service_, success_callback) | 同进程 | 线程间任务投递 |

**唯一跨进程的是 Finish()**，其余全部是同进程内线程切换。

### 4.5 不同 IO Context 线程之间的关系

**不会互相阻塞，但会竞争 CPU 和共享资源。**

- 各自独立事件循环：每个 io_context 有自己的任务队列和线程
- 不互相阻塞：A 线程慢不影响 B 线程处理自己的队列
- 间接竞争：CPU 时间片、Redis 连接池、内存 cache line

---

## 五、同步回复 vs 异步回复

### 5.1 核心区别

```
同步回复：handler 内直接调用 GCS_RPC_SEND_REPLY -> 立即触发回复链
异步回复：handler 捕获 send_reply_callback -> 返回 -> 异步操作完成后才调用
```

### 5.2 GCS_RPC_SEND_REPLY 宏

```cpp
// src/ray/gcs/grpc_service_interfaces.h
#define GCS_RPC_SEND_REPLY(send_reply_callback, reply, status)        \
  reply->mutable_status()->set_code(static_cast<int>(status.code())); \
  reply->mutable_status()->set_message(status.message());             \
  send_reply_callback(ray::Status::OK(), nullptr, nullptr)
```

### 5.3 SendReplyCallback 签名

```cpp
// src/ray/rpc/rpc_callback_types.h
using SendReplyCallback = std::function<void(
    Status status,                    // 回复状态
    std::function<void()> success,    // 回复成功后的回调
    std::function<void()> failure     // 回复失败后的回调
)>;
```

### 5.4 同步回复示例

```cpp
// GcsActorManager::HandleGetActorInfo
void HandleGetActorInfo(request, reply, send_reply_callback) {
  auto &actor = registered_actors_.find(actor_id);  // 内存查找
  if (actor != end()) *reply->mutable_actor_table_data() = actor->GetActorTableData();
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());  // 立即回复
}
```

### 5.5 异步回复示例

```cpp
// GcsKVManager::HandleInternalKVGet - 等 Redis 返回
void HandleInternalKVGet(request, reply, send_reply_callback) {
  auto callback = [reply, send_reply_callback](std::optional<std::string> val) {
    if (val) { reply->set_value(*val); }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  };
  kv_instance_->Get(ns, key, {std::move(callback), io_context_});
  // handler 返回，此时还没回复客户端！
}
```

### 5.6 回复时序对比

```
同步：
  io_context线程:  handler执行 -> GCS_RPC_SEND_REPLY -> [post SendReply] -> 返回
                  |<------ 整个过程在一次事件循环中 ------>|

异步：
  io_context线程:  handler执行 -> 发起异步操作 -> 返回（未回复！）
  Redis线程:       异步操作完成 -> Postable.Post() -> post回io_context
  io_context线程:  callback执行 -> GCS_RPC_SEND_REPLY -> [post SendReply]
                  |<-- 跨越多次事件循环 -->|<-- 可能很久 -->|
```

### 5.7 服务端没有超时机制

如果异步 handler 永远不调用 send_reply_callback，服务端没有任何清理机制，ServerCall 对象会一直存在（内存泄漏）。只有客户端有 deadline 超时保护。

---

## 六、Actor 创建完整流程与回调链

### 6.1 三跳 RPC 链

```
CoreWorker(调用方)  --1.CreateActor-->  GCS  --2.RequestWorkerLease-->  Raylet
                                                      ^ 回复worker地址
                                                      |
                                          GCS  --3.PushTask-->  CoreWorker(执行方)
                                                                ^ 执行__init__
                                                                |
                                          GCS <-- 回调链完成 ---+
CoreWorker(调用方) <-- CreateActorReply -----------------------+
```

### 6.2 客户端/服务端角色切换

| RPC | GCS 角色 | 对端角色 | 含义 |
|-----|---------|---------|------|
| 1. CreateActor | SERVER | CoreWorker (CLIENT) | GCS 接收请求 |
| 2. RequestWorkerLease | CLIENT | Raylet (SERVER) | GCS 主动调用 Raylet |
| 3. PushTask | CLIENT | CoreWorker (SERVER) | GCS 主动调用 Worker |

GCS 既是 server（接收1）又是 client（发起2和3）。

### 6.3 gRPC 如何保证回复到达正确的客户端

gRPC 的回复路由不靠请求 ID，而是靠 gRPC 连接 + ServerCall 对象：

- CoreWorker 发起 CreateActor -> GCS 创建一个 ServerCall 对象
- ServerCall 持有 response_writer_（绑定到该客户端连接）
- 不管中间经过多少步，最终调用 send_reply_callback
- response_writer_.Finish() -> 数据通过原 TCP 连接写回 CoreWorker
- gRPC 保证数据走同一条连接返回

关键：response_writer_ 在 CreateCall() 注册时绑定 context_（含客户端连接信息），Finish() 根据 context_ 原路返回。

---

## 七、回调保存与执行机制详解

### 7.1 闭包捕获的本质

C++ lambda 捕获的本质是把变量存到编译器生成的闭包对象中。每个 lambda 在编译期生成一个匿名类，捕获的变量成为其成员字段：

```cpp
// HandleCreateActor 中构造的 callback
[reply, send_reply_callback, actor_id]
  (const shared_ptr<GcsActor> &actor, const PushTaskReply &task_reply, const Status &status) {
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  }

// 编译器等价生成：
class __Closure {
    CreateActorReply* reply;           // 捕获：指向 ServerCall 的 reply_ 成员
    SendReplyCallback send_reply_callback; // 捕获：ServerCall 创建的 lambda
    ActorID actor_id;
 public:
    void operator()(shared_ptr<GcsActor> actor, PushTaskReply reply, Status status) {
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
    }
};
```

关键：send_reply_callback 本身又是一个 lambda（在 server_call.h:243 创建），它捕获了 this（即 ServerCallImpl*），这个指针持有 response_writer_。

### 7.2 闭包嵌套结构（俄罗斯套娃）

```
最外层：HandleCreateActor 中的 callback
  +-- 捕获: reply (CreateActorReply*), send_reply_callback, actor_id
  |
  +-- send_reply_callback 本身是 ServerCallImpl 创建的 lambda:
      +-- 捕获: this (ServerCallImpl*)
      |   +-- ServerCallImpl 持有:
      |       +-- response_writer_ (grpc::ServerAsyncResponseWriter<Reply>)
      |       |   +-- 绑定了客户端的 gRPC 连接
      |       +-- reply_ (Reply*) — 实际回复数据
      |       +-- io_service_ — 组件的 io_context
      |       +-- send_reply_success_callback_ / failure_callback_
      |
      +-- 参数: status, success, failure
          +-- 执行动作:
              1. 保存 success/failure 到 ServerCallImpl 成员
              2. post(SendReply线程池, SendReply(status))
```

### 7.3 Actor 创建回调的 4 个保存点

**保存点1：HandleCreateActor -> CreateActor 的参数 callback**

```
保存到：actor_to_create_callbacks_[actor_id].push_back(callback)
数据结构：absl::flat_hash_map<ActorID, vector<CreateActorCallback>>
触发方式：RunAndClearActorCreationCallbacks() 主动调用
```

**保存点2：LeaseWorkerFromNode -> RequestWorkerLease 的回调**

```
保存到：ClientCallImpl<Reply> 的 callback_ 字段
由 ClientCallManager 管理
触发方式：Client Polling 线程收到响应 -> post(main_service_) -> OnReplyReceived()
```

**保存点3：HandleWorkerLeaseGrantedReply -> ActorTable.Put 的回调**

```
保存到：Postable<void(Status)> — 包含 callback + io_context 引用
触发方式：Redis 线程写入完成 -> Postable.Post() -> post(io_context)
```

**保存点4：CreateActorOnWorker -> PushNormalTask 的回调**

```
保存到：ClientCallImpl<Reply> 的 callback_ 字段
触发方式：Worker 执行完 __init__ 后 Client Polling 线程收到响应 -> post(main_service_)
```

### 7.4 回调触发的完整时序

```
[GCS主线程] HandleCreateActor 被调用
    |
    +-- 构造 callback(含 send_reply_callback)  <-- 保存点1
    +-- actor_to_create_callbacks_[id].push_back(callback)
    +-- gcs_actor_scheduler_->Schedule(actor)
    |   +-- LeaseWorkerFromNode -> raylet_client->RequestWorkerLease(保存点2)
    +-- HandleCreateActor返回（此时客户端还没收到回复！）

                    ... 等待 Raylet 响应 ...

[GCS Client Poll线程] 收到 RequestWorkerLease 响应
    +-- main_service_.post([tag]() {        <-- 第1次 post
         tag->GetCall()->OnReplyReceived(); <-- 执行保存点2的callback
       })

[GCS主线程] HandleWorkerLeaseReply -> HandleWorkerLeaseGrantedReply
    +-- gcs_actor_table_.Put(actor_id, ..., 保存点3)

                    ... 等待 Redis 写入 ...

[Redis线程] 写入完成 -> Postable.Post()     <-- 第2次 post
    +-- io_context_.post(callback)

[GCS主线程] ActorTable.Put 回调
    +-- CreateActorOnWorker -> client->PushNormalTask(保存点4)

                    ... 等待 Worker 执行 __init__ ...

[GCS Client Poll线程] 收到 PushTask 响应
    +-- main_service_.post([tag]() {        <-- 第3次 post
         tag->GetCall()->OnReplyReceived();
       })

[GCS主线程] PushTask回调
    +-- schedule_success_handler_(actor, reply)
    +-- OnActorCreationSuccess
    +-- gcs_table_storage_->ActorTable().Put(...)

[Redis线程] 写入完成 -> Postable.Post()     <-- 第4次 post

[GCS主线程] RunAndClearActorCreationCallbacks
    +-- callback(actor, reply, Status::OK()) <-- 执行保存点1！
    +-- GCS_RPC_SEND_REPLY(send_reply_callback, reply, status)
    +-- post(SendReply线程池) -> Finish() -> 网络回复CoreWorker
```

### 7.5 回调为什么能正确路由回调用方

不是靠请求 ID，而是靠闭包捕获。每一层回调都是 lambda，捕获了上一层的回调引用：

```
保存点1的 callback 捕获了 send_reply_callback（ServerCall的response_writer_）
  -> 保存在 actor_to_create_callbacks_ 中
  -> 最终在保存点4的 PushTask 回调链末尾被取出执行
  -> send_reply_callback -> post SendReply线程池 -> Finish()
  -> response_writer_ 绑定了发起 CreateActor 的 CoreWorker 的 TCP 连接
  -> gRPC 保证数据通过原连接返回
```

整个链路是闭包嵌套闭包，像俄罗斯套娃一样，最内层的 callback 在最外层创建时就捕获了 send_reply_callback，一路传递保存，直到最终被调用时触发 Finish() 把回复写回原始连接。

---

## 八、Finish() 触发逻辑与回复机制

### 8.1 Finish() 的作用

```cpp
// server_call.h:384
void SendReply(const Status &status) {
  state_ = ServerCallState::SENDING_REPLY;
  response_writer_.Finish(*reply_, RayStatusToGrpcStatus(status), this);
}
```

response_writer_.Finish() 做了三件事：

1. **序列化 reply_**：将 protobuf *reply_ 对象序列化为字节流
2. **构造 gRPC 帧头**：添加 gRPC 帧长度前缀、压缩标志等
3. **提交异步写入**：将数据投递到 gRPC 内核缓冲区，不等待实际发送完成

```cpp
response_writer_.Finish(*reply_, grpc_status, /*tag=*/this);
//                                   ^              ^
//                              gRPC状态码      完成后的通知标签
//                          (OK/NotFound等)   (=this指针，CQ事件关联)
```

this 作为 tag：gRPC 完成网络发送后，将 this 作为事件标签放入 CompletionQueue。Server Polling 线程通过 AsyncNext() 取出 tag，static_cast<ServerCall*>(tag) 还原回 ServerCall 对象，检测 state_ == SENDING_REPLY，调用 OnReplySent()。

### 8.2 完整回复流程

```
步骤1 [GCS主线程] RunAndClearActorCreationCallbacks
  | callback(actor, reply, Status::OK())
  | GCS_RPC_SEND_REPLY(send_reply_callback, reply, status)
  | send_reply_callback(Status::OK(), nullptr, nullptr)

步骤2 [GCS主线程] send_reply_callback lambda 执行
  | send_reply_success_callback_ = nullptr
  | boost::asio::post(GetServerCallExecutor(), [this, status] { SendReply(status); });
  | 返回，主线程继续处理其他事件

步骤3 [SendReply线程池] SendReply(status) 执行
  | state_ = SENDING_REPLY;
  | response_writer_.Finish(*reply_, grpc_status, this);
  | gRPC 将 reply_ 序列化并投递到内核写缓冲区
  | gRPC 注册 this 到 CompletionQueue 等待完成通知

步骤4 [网络层] TCP 发送 reply 数据帧到客户端

步骤5 [客户端 Client Polling 线程] 收到响应事件
  | main_service_.post(callback)
  | 客户端 callback(status, reply) 执行

步骤6 [Server Polling线程] AsyncNext() 返回
  | tag = this (ServerCallImpl*), state_ == SENDING_REPLY, ok == true
  | server_call->OnReplySent()
  |   -> io_service_.post(success_callback)  (如果有的话)
  | delete_call = true, need_new_call = true
  | factory_.CreateCall()  // 释放背压
  | delete server_call     // 销毁本次请求的 ServerCall 对象
```

### 8.3 回复数据的流转

```
reply_ (protobuf对象，在 Arena 上分配)
  | Handler 填充 reply_
  | 例: reply->mutable_actor_address()->CopyFrom(actor->GetAddress());
  |
  v SendReply() 调用 Finish(*reply_, ...)
  | gRPC 内部: SerializeToString -> 字节流
  | -> 添加 5 字节帧头: [压缩标志1字节][长度4字节]
  | -> 写入 gRPC 内核发送缓冲区
  |
  v TCP 传输
  | 客户端 gRPC: 读取帧头 -> 读取消息体 -> 反序列化为 Reply 对象
  | -> ClientCallImpl::OnReplyReceived()
  | -> callback_(status, std::move(reply_))
```

### 8.4 response_writer_ 如何绑定到正确的客户端

关键在 CreateCall() 时 gRPC 的注册机制：

```cpp
// ServerCallFactoryImpl::CreateCall()
auto call = new ServerCallImpl(..., response_writer_(&context_), ...);

// 向 gRPC 注册
(service_.*request_call_function_)(
    &call->context_,        // gRPC 在这里写入客户端的连接信息
    &call->request_,        // gRPC 在这里反序列化请求
    &call->response_writer_,// gRPC 绑定到这个 writer，Finish() 时原路返回
    cq_.get(), cq_.get(),
    call                    // tag
);
```

context_ 保存了客户端的连接元数据（IP、端口、metadata 等），response_writer_ 在构造时绑定了 context_。Finish() 时 gRPC 根据 context_ 中的连接信息，将回复通过同一条 TCP 连接发回。

不需要请求 ID——gRPC 的 ServerAsyncResponseWriter 是一对一绑定的：一个 writer 对应一个请求-响应对，Finish() 只会回复到发出该请求的那个客户端连接。

---

## 九、客户端收到回复的机制

### 9.1 客户端收回复流程

客户端收到回复的机制和服务端收到请求的机制完全对称，都依赖 gRPC 的 CompletionQueue 异步通知。

**第1步：发起请求时注册等待**

```cpp
// ClientCallManager::CreateCall (client_call.h:285)
auto call = std::make_shared<ClientCallImpl<Reply>>(callback, ...);

// 发起异步RPC，绑定到某个 CompletionQueue
call->response_reader_ = stub.PrepareAsyncBar(&call->context_, request, cq);
call->response_reader_->StartCall();

// 注册 Finish 等待
auto tag = new ClientCallTag(call);
call->response_reader_->Finish(&call->reply_, &call->status_, tag);
//                          ^ 回复写入这里    ^ 状态写这里   ^ CQ事件标签
```

Finish() 的含义：请 gRPC 在收到回复后，把回复数据反序列化到 call->reply_，把 gRPC 状态写入 call->status_，然后把 tag 放入 CompletionQueue 通知我。

**第2步：Client Polling 线程检测到回复**

```cpp
// ClientCallManager::PollEventsFromCompletionQueue
while (true) {
    auto status = cqs_[index]->AsyncNext(&got_tag, &ok, deadline);
    if (status != TIMEOUT) {
        auto tag = static_cast<ClientCallTag *>(got_tag);
        tag->GetCall()->SetReturnStatus();
        if (ok && !main_service_.stopped()) {
            main_service_.post([tag]() {
                tag->GetCall()->OnReplyReceived();  // post到主线程
                delete tag;
            });
        }
    }
}
```

**第3步：主线程执行回调**

```cpp
void OnReplyReceived() override {
    callback_(status, std::move(reply_));  // 调用用户回调
}
```

### 9.2 服务端/客户端对比

```
           服务端（收请求）                    客户端（收回复）
           --------------                    --------------
注册方式    RequestBar(&context_, &request_,   response_reader_->Finish(
            &response_writer_, cq, tag)          &reply_, &status_, tag)

CQ事件含义  有新请求到达                       收到服务端回复

Polling线程  server.poll.N                     client.poll.N

CQ返回后    HandleRequest()                   OnReplyReceived()
            -> post(io_service_)               -> post(main_service_)
            -> HandleRequestImpl()             -> callback_(status, reply)

tag类型     ServerCall* (直接用call)           ClientCallTag* (包装了shared_ptr)
```

核心机制相同：都是 gRPC 异步 API + CompletionQueue + Polling 线程 + post 到主线程。区别仅在于服务端是等待请求到达，客户端是等待回复到达。

Finish() 在客户端的含义是"帮我等待并读取回复"，在服务端的含义是"帮我发送回复并等待发送完成"。

### 9.3 客户端回调不通知服务端

callback_ 是客户端自己处理服务端回复的逻辑，完全在客户端进程内执行，不会再发任何东西回服务端。

---

## 十、主线程忙碌与健康检查超时

### 10.1 GcsHealthCheckManager 运行在主线程

```cpp
// gcs_server.cc
gcs_healthcheck_manager_ = GcsHealthCheckManager::Create(
    io_context_provider_.GetDefaultIOContext(), ...);
```

健康检查结果回调 post 回主线程。如果主线程忙碌，回调排队等待，挂钟时间继续流逝，deadline 超时。

### 10.2 超时场景

```
T=0s:   健康检查 RPC 发出，deadline = now + 10s
T=5s:   Raylet 正常响应，gRPC 线程收到回复
T=5s:   post(结果回调) 到主线程队列
T=5-15s: 主线程忙于处理其他 handler，回调排队等待...
T=10s:  deadline 过期！RPC 状态变为 DEADLINE_EXCEEDED
T=15s:  回调终于执行，status.ok()=false -> health_check_remaining_--
        连续5次 -> FailNode() -> 节点被误判死亡
```

### 10.3 相关配置参数

| 参数 | 默认值 | 含义 |
|-----|--------|------|
| health_check_timeout_ms | 10000 | 单次健康检查 RPC 超时 |
| health_check_failure_threshold | 5 | 连续失败次数标记节点死亡 |
| health_check_period_ms | 3000 | 健康检查间隔 |
| handler_warning_timeout_ms | 1000 | handler 执行超时告警阈值 |

优化建议：
```bash
export RAY_health_check_timeout_ms=30000
export RAY_health_check_failure_threshold=10
export RAY_health_check_period_ms=5000
```

根本解决需要将健康检查从主线程迁移到独立 IO Context。
