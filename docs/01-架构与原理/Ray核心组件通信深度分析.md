# Ray 核心组件通信机制深度分析

> 基于源码 ray 2.52.1，详细分析 TaskManager、CoreWorker、Raylet 之间的通信架构，
> 包括 IPC Socket 全链路、gRPC 连接池管理、消息帧格式等。

---

## 目录

1. [TaskManager 的进程归属](#1-taskmanager-的进程归属)
2. [整体通信架构图](#2-整体通信架构图)
3. [IPC Socket 通信全链路详解](#3-ipc-socket-通信全链路详解)
   - [Socket 文件创建](#31-socket-文件创建raylet-端)
   - [连接建立](#32-连接建立)
   - [消息帧格式](#33-消息帧格式)
   - [三种通信模式](#34-三种通信模式)
   - [Raylet 端消息分发](#35-raylet-端消息分发)
   - [完整时序示例：Worker 注册](#36-完整时序示例worker-注册)
4. [CoreWorker 与 Raylet 的双通道通信](#4-coreworker-与-raylet-的双通道通信)
5. [CoreWorker 与远程 Raylet 的 gRPC 通信](#5-coreworker-与远程-raylet-的-grpc-通信)
6. [CoreWorker 与 CoreWorker 的 gRPC 通信](#6-coreworker-与-coreworker-的-grpc-通信)
   - [跨节点通信架构图](#60-跨节点通信架构图)
   - [任务推送的直接通信代码链路](#61-任务推送的直接通信代码链路)
7. [RayletClientPool ↔ RayletClient 关系](#7-rayletclientpool--rayletclient-关系)
8. [CoreWorkerClientPool ↔ CoreWorkerClient 关系](#8-coreworkerclientpool--coreworkerclient-关系)
9. [两个 Pool 的关键差异对比](#9-两个-pool-的关键差异对比)
10. [端口与 Socket 文件资源总结](#10-端口与-socket-文件资源总结)
11. [Ray 核心组件间全交互矩阵](#11-ray-核心组件间全交互矩阵)
    - [交互关系总览图](#111-交互关系总览图)
    - [Raylet ↔ GCS 交互](#112-raylet--gcs-交互)
    - [Worker ↔ GCS 交互](#113-worker--gcs-交互)
    - [Raylet ↔ Raylet 交互](#114-raylet--raylet-交互)
    - [Worker ↔ Worker 交互补充](#115-worker--worker-交互补充)
    - [NodeManagerService 完整 RPC 列表](#116-nodemanagerservice-完整-rpc-列表)
    - [交互矩阵总结表](#117-交互矩阵总结表)
12. [gRPC 端口绑定失败 Crash 深度分析](#12-grpc-端口绑定失败-crash-深度分析)
    - [典型报错](#121-典型报错)
    - [Crash 发生点：GrpcServer::Run()](#122-crash-发生点grpcserverrun)
    - [CoreWorker 初始化链路](#123-coreworker-初始化链路从端口分配到-crash)
    - [Raylet 端口分配逻辑](#124-raylet-端口分配逻辑workerpoolgetnextfreeport)
    - [端口可用性检查与竞态条件](#125-端口可用性检查checkportfree--存在竞态条件)
    - [Worker/Driver 注册时的端口分配触发点](#126-workerdriver-注册时的端口分配触发点)
    - [各组件端口来源与冲突可能性](#127-各组件端口来源--端口冲突的可能来源)
    - [完整 Crash 链路总结](#128-完整的-crash-链路总结)
    - [排查与解决方案](#129-排查与解决方案)
13. [Raylet GCS 连接超时深度分析](#13-raylet-gcs-连接超时深度分析)
    - [典型报错](#131-典型报错)
    - [三条超时路径](#132-三条超时路径)
    - [路径一：FetchClusterId 同步超时 → RAY_CHECK crash](#133-路径一fetchclusterid-同步超时--ray_check-crash)
    - [路径二：RetryableGrpcClient 连接超时 → 进程强制退出](#134-路径二retryablegrpcclient-连接超时--进程强制退出)
    - [路径三：请求级超时 → TimedOut 回调](#135-路径三请求级超时--timedout-回调)
    - [RetryableGrpcClient 完整工作机制](#136-retryablegrpcclient-完整工作机制)
    - [超时后的级联影响](#137-超时后的级联影响)
    - [两个报错的关联性分析](#138-两个报错的关联性分析)
    - [排查与解决方案](#139-排查与解决方案)
    - [关键配置参数](#1310-关键配置参数)
14. [两个报错的完整调用堆栈与因果关系](#14-两个报错的完整调用堆栈与因果关系)
    - [报错一：chttp2_server.cc:1063 完整调用堆栈](#141-报错一chttp2_servercc1063-pid2596-ip10487618)
    - [报错二：GCS 连接超时三条报错完整调用堆栈](#142-报错二gcs-连接超时三条报错ip10487544)
    - [两个报错的因果关系链](#143-两个报错的因果关系链)
    - [Node.__init__() 与 chttp2_server.cc 的代码关系](#144-node__init__-与-chttp2_servercc-的代码关系)
    - [Worker 进程完整启动流程](#144-worker-进程完整启动流程node__init__--coreworker--main_loop)
    - [Cluster ID 完整传递链路与 FetchClusterId 触发机制](#145-cluster-id-完整传递链路与-fetchclusterid-触发机制)
      - [Cluster ID 从生成到 Worker 进程的完整传递路径](#cluster-id-从生成到-worker-进程的完整传递路径)
      - [cluster_id 为 nil 与非 nil 的条件分析](#cluster_id-为-nil-与非-nil-的条件分析)
      - [关键 Bug 发现：common.pxi:80 参数传递错误](#关键-bug-发现commonpixi80-参数传递错误)
      - [报错二中 pid=2672 的根本原因](#报错二中-pid2672-的根本原因commonpixi80-参数传递-bug)
      - [FetchClusterId 触发机制总结](#fetchclusterid-触发机制总结)
      - [FetchClusterId 成功后的行为分析](#fetchclusterid-成功后的行为分析)
      - [Bug 的隐性影响与设计意图对比](#bug-的隐性影响与设计意图对比)

---

## 1. TaskManager 的进程归属

TaskManager **不是独立进程**，它是 CoreWorker 进程内部的一个类，定义在
`src/ray/core_worker/task_manager.h:175`。

TaskManager 被 CoreWorker 持有并以 `shared_ptr<TaskManager>` 注入：

```cpp
// src/ray/core_worker/core_worker.h:191
std::shared_ptr<TaskManager> task_manager,
```

TaskManager 的构造参数也全是 CoreWorker 内部的组件：

```cpp
// src/ray/core_worker/task_manager.h:177
TaskManager(
    CoreWorkerMemoryStore &in_memory_store,
    ReferenceCounterInterface &reference_counter,
    ...
```

**结论：TaskManager 和 CoreWorker 在同一个进程内，是函数调用关系，不存在跨进程通信。**

---

## 2. 整体通信架构图

```
┌─────────────────────────────────────────────────────────────────┐
│  Node A                                                         │
│                                                                 │
│  ┌──────────────────┐          ┌──────────────────┐             │
│  │   CoreWorker 1   │◄──gRPC──►│   CoreWorker 2   │             │
│  │                  │          │                  │             │
│  │ ┌──────────────┐ │          │ ┌──────────────┐ │             │
│  │ │ TaskManager  │ │          │ │ TaskManager  │ │             │
│  │ └──────────────┘ │          │ └──────────────┘ │             │
│  └────────┬─────────┘          └────────┬─────────┘             │
│           │                             │                       │
│     IPC Socket                     IPC Socket                  │
│     + gRPC                         + gRPC                      │
│           │                             │                       │
│  ┌────────▼─────────────────────────────▼──────────┐            │
│  │              Raylet (Node Manager)               │            │
│  └────────────────────┬────────────────────────────┘            │
│                       │                                         │
└───────────────────────┼─────────────────────────────────────────┘
                        │ gRPC (NodeManagerService)
                        │
┌───────────────────────┼─────────────────────────────────────────┐
│  Node B               ▼                                         │
│  ┌────────────────────────────────────────────────┐              │
│  │              Raylet (Node Manager)               │              │
│  └────────┬────────────────────────┬──────────────┘              │
│           │ IPC Socket             │ IPC Socket                  │
│           ▼                        ▼                             │
│  ┌──────────────────┐     ┌──────────────────┐                   │
│  │   CoreWorker 3   │◄───►│   CoreWorker 4   │                   │
│  └──────────────────┘ gRPC└──────────────────┘                   │
└──────────────────────────────────────────────────────────────────┘
```

---

## 3. IPC Socket 通信全链路详解

整个 IPC 通信基于 **Unix Domain Socket + FlatBuffer**，核心是 Raylet 作为服务端监听一个 socket 文件，
CoreWorker 作为客户端连接上去。通信模型是**请求-响应**（同步）或**单向通知**（异步），不走 gRPC。

```
┌──────────────────────┐                      ┌──────────────────────────────────┐
│   CoreWorker 进程     │   Unix Domain Socket │       Raylet 进程                 │
│                      │   (文件系统路径)       │                                  │
│  RayletIpcClient     │                      │  NodeManager                     │
│    └─ conn_          │ ════════════════════► │    └─ acceptor_                  │
│       (ServerConn)   │  write: cookie+type+  │    └─ socket_                    │
│                      │        length+payload │    └─ HandleAccept()             │
│                      │                      │       └─ ClientConnection         │
│                      │ ◄════════════════════ │          └─ ProcessMessages()   │
│                      │  read: cookie+type+   │          └─ message_handler_    │
│                      │        length+payload │             → ProcessClientMsg() │
└──────────────────────┘                      └──────────────────────────────────┘
```

### 3.1 Socket 文件创建（Raylet 端）

Raylet 启动时，由 Python 层创建 socket 文件路径并传递给 C++：

```python
# python/ray/_private/node.py:290
self._raylet_socket_name = self._prepare_socket_file(
    self._ray_params.raylet_socket_name, default_prefix="raylet"
)
```

`_prepare_socket_file` 的逻辑：

```python
# python/ray/_private/node.py
def _prepare_socket_file(self, socket_path, default_prefix):
    if sys.platform == "win32":
        # Windows 不支持 Unix socket，回退到 TCP
        result = f"tcp://{build_address(self._localhost, self._get_unused_port())}"
    else:
        # Linux/Mac: 在 session 目录下创建 socket 文件
        result = self._make_inc_temp(
            prefix=default_prefix, directory_name=self._sockets_dir
        )
        # self._sockets_dir = /tmp/ray/session_XXX/sockets/
    return result
```

最终路径形如：`/tmp/ray/session_2026-06-05_10-30-00_123456/sockets/raylet`

同时，session 目录和 sockets 目录在 `node.py:528-532` 中创建：

```python
# python/ray/_private/node.py:528-532
try_to_create_directory(self._session_dir)
# Create a directory to be used for socket files.
self._sockets_dir = os.path.join(self._session_dir, "sockets")
try_to_create_directory(self._sockets_dir)
```

### 3.2 连接建立

#### CoreWorker 端（客户端）— `RayletIpcClient` 构造函数

```cpp
// src/ray/raylet_ipc_client/raylet_ipc_client.cc:78
RayletIpcClient::RayletIpcClient(instrumented_io_context &io_service,
                                 const std::string &address,   // socket 文件路径
                                 int num_retries,
                                 int64_t timeout) {
  local_stream_socket socket(io_service);
  // 连接到 socket 文件，带重试（num_retries=-1 表示无限重试）
  Status s = ConnectSocketRetry(socket, address, num_retries, timeout);
  if (!s.ok()) {
    RAY_LOG(FATAL) << "Failed to connect to socket at address:" << address;
  }
  // 创建 ServerConnection 封装 socket fd
  conn_ = ServerConnection::Create(std::move(socket));
}
```

`ConnectSocketRetry` 底层使用 `boost::asio::connect()`，逻辑如下：

```cpp
// src/ray/raylet_ipc_client/client_connection.cc:59
Status ConnectSocketRetry(local_stream_socket &socket,
                          const std::string &endpoint,
                          int num_retries,
                          int64_t timeout_in_ms) {
  RAY_CHECK(num_retries != 0);
  // Pick the default values if the user did not specify.
  if (num_retries < 0) {
    num_retries = RayConfig::instance().raylet_client_num_connect_attempts();
  }
  if (timeout_in_ms < 0) {
    timeout_in_ms = RayConfig::instance().raylet_client_connect_timeout_milliseconds();
  }
  boost::system::error_code ec;
  for (int num_attempts = 0; num_attempts < num_retries; ++num_attempts) {
    // 连接 Unix socket 文件
    socket.connect(ParseUrlEndpoint(endpoint), ec);
    if (!ec) {
      break;
    }
    if (num_attempts > 0) {
      RAY_LOG(INFO) << "Retrying to connect to socket for endpoint " << endpoint
                    << " (num_attempts = " << num_attempts
                    << ", num_retries = " << num_retries << ")";
    }
    // Sleep for timeout milliseconds.
    std::this_thread::sleep_for(std::chrono::milliseconds(timeout_in_ms));
  }
  return boost_to_ray_status(ec);
}
```

**重要细节**：`local_stream_socket` 的类型定义在 `client_connection.h:33-34`：

```cpp
// src/ray/raylet_ipc_client/client_connection.h:33-34
using local_stream_protocol = boost::asio::generic::stream_protocol;
using local_stream_socket = boost::asio::basic_stream_socket<local_stream_protocol>;
```

在 Linux/Mac 上，`ParseUrlEndpoint` 会将文件路径转为 Unix Domain Socket 的 endpoint；
在 Windows 上会转为 TCP endpoint（因为 Windows 不支持 Unix Domain Socket）。

#### Raylet 端（服务端）— 接受连接

Raylet 启动时通过 `acceptor_` 异步等待连接：

```cpp
// src/ray/raylet/node_manager.cc:328
acceptor_.async_accept(socket_,
    boost::bind(&NodeManager::HandleAccept, this, boost::asio::placeholders::error));
```

当有 CoreWorker 连接时触发 `HandleAccept`：

```cpp
// src/ray/raylet/node_manager.cc:497
void NodeManager::HandleAccept(const boost::system::error_code &error) {
  if (!error) {
    // 设置消息回调 — 收到消息时调 ProcessClientMessage
    ConnectionErrorHandler error_handler =
        [this](const std::shared_ptr<ClientConnection> &client,
               const boost::system::error_code &err) {
          this->HandleClientConnectionError(client, err);
        };

    MessageHandler message_handler = [this](
        const std::shared_ptr<ClientConnection> &client,
        int64_t message_type,
        const std::vector<uint8_t> &message) {
      this->ProcessClientMessage(client, message_type, message.data());
    };

    // 创建 ClientConnection 封装这个 socket
    auto conn = ClientConnection::Create(
        message_handler,
        error_handler,
        std::move(socket_),
        "worker",
        node_manager_message_enum);

    // 开始异步读取消息
    conn->ProcessMessages();
  } else {
    RAY_LOG(ERROR) << "Raylet failed to accept new connection: " << error.message();
  };

  // 继续等待下一个连接
  acceptor_.async_accept(
      socket_,
      boost::bind(&NodeManager::HandleAccept, this, boost::asio::placeholders::error));
}
```

关键点：
- Raylet 为每个连入的 CoreWorker 创建一个 `ClientConnection`
- `ClientConnection` 继承自 `ServerConnection`，增加了异步消息读取能力
- 消息回调绑定到 `NodeManager::ProcessClientMessage`
- 连接错误回调绑定到 `NodeManager::HandleClientConnectionError`

#### 两个核心连接类的继承关系

```
ServerConnection                          ClientConnection
├── socket_ (local_stream_socket)         ├── 继承 ServerConnection 的所有功能
├── WriteMessage()  同步写消息             ├── message_handler_   消息处理回调
├── ReadMessage()   同步读消息             ├── connection_error_handler_  错误回调
├── WriteMessageAsync()  异步写消息        ├── registered_  是否已注册
├── WriteBuffer()   底层写 buffer          ├── ProcessMessages()  异步读消息循环
├── ReadBuffer()    底层读 buffer          ├── ProcessMessageHeader()
└── async_write_queue_  异步写队列         ├── ProcessMessage()
                                          └── CheckRayCookie()
```

`ServerConnection` 用于 CoreWorker 端（客户端），提供同步读写能力。
`ClientConnection` 用于 Raylet 端（服务端），增加了异步消息处理循环。

### 3.3 消息帧格式

每条 IPC 消息的帧格式如下（**自定义二进制协议，不是 gRPC**）：

```
┌──────────┬──────────┬──────────┬──────────────────┐
│  cookie  │   type   │  length  │     payload       │
│ 8 bytes  │ 8 bytes  │ 8 bytes  │   length bytes    │
│ (int64)  │ (int64)  │ (uint64) │  (FlatBuffer)     │
└──────────┴──────────┴──────────┴──────────────────┘
```

- **cookie**: Ray 会话的唯一标识，用于验证消息来自同一个 Ray 集群，防止外部程序误连。
  来源为 `RayConfig::instance().ray_cookie()`。
- **type**: 消息类型枚举，如 `RegisterClientRequest`、`NotifyWorkerBlocked` 等。
  定义在 `src/ray/flatbuffers/node_manager_generated.h`。
- **length**: FlatBuffer 载荷的字节长度。
- **payload**: FlatBuffer 序列化的消息体。

#### 写入逻辑

```cpp
// src/ray/raylet_ipc_client/client_connection.cc:162
ray::Status ServerConnection::WriteMessage(int64_t type,
                                           int64_t length,
                                           const uint8_t *message) {
  sync_writes_ += 1;
  bytes_written_ += length;

  auto write_cookie = RayConfig::instance().ray_cookie();
  return WriteBuffer({
      boost::asio::buffer(&write_cookie, sizeof(write_cookie)),  // 8 bytes cookie
      boost::asio::buffer(&type, sizeof(type)),                  // 8 bytes 消息类型
      boost::asio::buffer(&length, sizeof(length)),              // 8 bytes 载荷长度
      boost::asio::buffer(message, length),                      // FlatBuffer 载荷
  });
}
```

`WriteBuffer` 底层调用 `boost::asio::write_some()` 循环写入，处理 `EINTR` 中断：

```cpp
// src/ray/raylet_ipc_client/client_connection.cc:112
Status ServerConnection::WriteBuffer(
    const std::vector<boost::asio::const_buffer> &buffer) {
  boost::system::error_code error;
  for (const auto &b : buffer) {
    uint64_t bytes_remaining = boost::asio::buffer_size(b);
    uint64_t position = 0;
    while (bytes_remaining != 0) {
      size_t bytes_written =
          socket_.write_some(boost::asio::buffer(b + position, bytes_remaining), error);
      position += bytes_written;
      bytes_remaining -= bytes_written;
      if (error.value() == EINTR) {
        continue;  // 被信号中断，重试
      } else if (error.value() != boost::system::errc::errc_t::success) {
        return boost_to_ray_status(error);
      }
    }
  }
  return ray::Status::OK();
}
```

#### 读取逻辑

```cpp
// src/ray/raylet_ipc_client/client_connection.cc:177
Status ServerConnection::ReadMessage(int64_t type, std::vector<uint8_t> *message) {
  int64_t read_cookie, read_type, read_length;
  // Wait for a message header from the client. The message header includes the
  // protocol version, the message type, and the length of the message.
  RAY_RETURN_NOT_OK(ReadBuffer({
      boost::asio::buffer(&read_cookie, sizeof(read_cookie)),   // 读 cookie
      boost::asio::buffer(&read_type, sizeof(read_type)),       // 读类型
      boost::asio::buffer(&read_length, sizeof(read_length)),   // 读长度
  }));
  // 验证 cookie
  if (read_cookie != RayConfig::instance().ray_cookie()) {
    std::ostringstream ss;
    ss << "Ray cookie mismatch for received message. "
       << "Received cookie: " << read_cookie;
    return Status::IOError(ss.str());
  }
  // 验证消息类型（仅同步请求-响应模式使用）
  if (type != read_type) {
    std::ostringstream ss;
    ss << "Connection corrupted. Expected message type: " << type
       << ", receviced message type: " << read_type;
    return Status::IOError(ss.str());
  }
  message->resize(read_length);
  return ReadBuffer({boost::asio::buffer(*message)});  // 读载荷
}
```

#### 异步写入逻辑

Raylet 端向 CoreWorker 回复消息时使用异步写入，通过 `async_write_queue_` 实现背压：

```cpp
// src/ray/raylet_ipc_client/client_connection.cc:202
void ServerConnection::WriteMessageAsync(
    int64_t type, int64_t length, const uint8_t *message,
    const std::function<void(const ray::Status &)> &handler) {
  async_writes_ += 1;
  bytes_written_ += length;

  auto write_buffer = std::make_unique<AsyncWriteBuffer>();
  write_buffer->write_cookie = RayConfig::instance().ray_cookie();
  write_buffer->write_type = type;
  write_buffer->write_length = length;
  write_buffer->write_message.assign(message, message + length);
  write_buffer->handler = handler;

  auto size = async_write_queue_.size();
  auto size_is_power_of_two = (size & (size - 1)) == 0;
  if (size > 1000 && size_is_power_of_two) {
    RAY_LOG(WARNING) << "ServerConnection has " << size << " buffered async writes";
  }

  async_write_queue_.push_back(std::move(write_buffer));

  if (!async_write_in_flight_) {
    DoAsyncWrites();
  }
}
```

`DoAsyncWrites` 将队列中的消息批量发送：

```cpp
// src/ray/raylet_ipc_client/client_connection.cc:230
void ServerConnection::DoAsyncWrites() {
  RAY_CHECK(!async_write_in_flight_);
  async_write_in_flight_ = true;

  // Do an async write of everything currently in the queue to the socket.
  std::vector<boost::asio::const_buffer> message_buffers;
  int num_messages = 0;
  for (const auto &write_buffer : async_write_queue_) {
    message_buffers.push_back(boost::asio::buffer(&write_buffer->write_cookie,
                                                  sizeof(write_buffer->write_cookie)));
    message_buffers.push_back(
        boost::asio::buffer(&write_buffer->write_type, sizeof(write_buffer->write_type)));
    message_buffers.push_back(boost::asio::buffer(&write_buffer->write_length,
                                                  sizeof(write_buffer->write_length)));
    message_buffers.push_back(boost::asio::buffer(write_buffer->write_message));
    num_messages++;
    if (num_messages >= async_write_max_messages_) {
      break;  // 每次最多写 1 条消息（async_write_max_messages_ = 1）
    }
  }
  // ... boost::asio::async_write 发送 ...
}
```

### 3.4 三种通信模式

#### 模式 A：单向通知（Fire-and-forget）

Worker 向 Raylet 发消息，**不等回复**。如 `NotifyWorkerBlocked`、`ActorCreationTaskDone`：

```cpp
// src/ray/raylet_ipc_client/raylet_ipc_client.cc:225
Status RayletIpcClient::NotifyWorkerBlocked() {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = protocol::CreateNotifyWorkerBlocked(fbb);
  fbb.Finish(message);
  // 只写不读 — 单向通知
  return WriteMessage(MessageType::NotifyWorkerBlocked, &fbb);
}
```

底层走 `WriteMessage` → `conn_->WriteMessage()` → `WriteBuffer()`，直接把帧写入 socket。

其他单向通知方法：
- `ActorCreationTaskDone()` — Actor 创建任务完成
- `CancelGetRequest(request_id)` — 取消对象获取请求
- `NotifyWorkerUnblocked()` — Worker 恢复运行
- `FreeObjects(object_ids, local_only)` — 释放对象
- `SubscribePlasmaReady(object_id, owner_address)` — 订阅 Plasma 对象就绪通知
- `PushError(job_id, type, error_message, timestamp)` — 推送错误信息

#### 模式 B：同步请求-响应（Request-Reply）

Worker 发请求并**同步阻塞等回复**。如 `RegisterClient`、`Wait`：

```cpp
// src/ray/raylet_ipc_client/raylet_ipc_client.cc:91
Status RayletIpcClient::RegisterClient(const WorkerID &worker_id,
                                       rpc::WorkerType worker_type,
                                       const JobID &job_id,
                                       int runtime_env_hash,
                                       const rpc::Language &language,
                                       const std::string &ip_address,
                                       const std::string &serialized_job_config,
                                       NodeID *node_id,
                                       int *assigned_port,
                                       bool *is_preemptible_node) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message =
      protocol::CreateRegisterClientRequest(fbb,
                                            static_cast<int>(worker_type),
                                            flatbuf::to_flatbuf(fbb, worker_id),
                                            getpid(),
                                            flatbuf::to_flatbuf(fbb, job_id),
                                            runtime_env_hash,
                                            language,
                                            fbb.CreateString(ip_address),
                                            /*port=*/0,
                                            fbb.CreateString(serialized_job_config));
  fbb.Finish(message);
  std::vector<uint8_t> reply;
  // 1. 写请求 → 2. 阻塞读回复
  Status status = AtomicRequestReply(
      MessageType::RegisterClientRequest, MessageType::RegisterClientReply,
      &reply, &fbb);
  RAY_RETURN_NOT_OK(status);

  // 3. 解析 FlatBuffer 回复
  auto reply_message = flatbuffers::GetRoot<protocol::RegisterClientReply>(reply.data());
  bool success = reply_message->success();
  if (!success) {
    return Status::Invalid(reply_message->failure_reason()->str());
  }

  *node_id = NodeID::FromBinary(reply_message->node_id()->str());
  *assigned_port = reply_message->port();
  *is_preemptible_node = reply_message->is_preemptible_node();
  return Status::OK();
}
```

`AtomicRequestReply` 是关键——它用 **mutex 保证同一时刻只有一个请求-响应在执行**：

```cpp
// src/ray/raylet_ipc_client/raylet_ipc_client.cc:327
Status RayletIpcClient::AtomicRequestReply(MessageType request_type,
                                           MessageType reply_type,
                                           std::vector<uint8_t> *reply_message,
                                           flatbuffers::FlatBufferBuilder *fbb) {
  std::unique_lock<std::mutex> guard(mutex_);   // 加锁：串行化请求-响应
  RAY_RETURN_NOT_OK(WriteMessage(request_type, fbb));       // 写请求
  auto status = conn_->ReadMessage(static_cast<int64_t>(reply_type),
                                   reply_message);           // 阻塞读回复
  ShutdownIfLocalRayletDisconnected(status);  // 如果 Raylet 已死，直接退出进程
  return status;
}
```

为什么需要 mutex？因为 Unix Domain Socket 是全双工的字节流，如果两个线程同时发请求-等回复，
响应的顺序可能和请求不匹配。mutex 确保了请求-响应的原子性。

其他同步请求-响应方法：
- `Disconnect(exit_type, exit_detail, ...)` — 断开连接
- `AnnounceWorkerPortForDriver(port, entrypoint)` — Driver 通知端口
- `Wait(object_ids, ..., timeout_milliseconds)` — 等待对象就绪

#### 模式 C：异步请求（半异步）

如 `AsyncGetObjects`，发请求不等回复，但返回一个 `ScopedResponse`，析构时自动发取消请求：

```cpp
// src/ray/raylet_ipc_client/raylet_ipc_client.cc:196
StatusOr<ScopedResponse> RayletIpcClient::AsyncGetObjects(
    const std::vector<ObjectID> &object_ids,
    const std::vector<rpc::Address> &owner_addresses,
    int64_t get_request_id) {
  RAY_CHECK(object_ids.size() == owner_addresses.size());
  flatbuffers::FlatBufferBuilder fbb;
  auto object_ids_message = flatbuf::to_flatbuf(fbb, object_ids);
  auto message =
      protocol::CreateAsyncGetObjectsRequest(fbb,
                                             object_ids_message,
                                             AddressesToFlatbuffer(fbb, owner_addresses),
                                             get_request_id);
  fbb.Finish(message);
  // 发送请求，不等回复
  RAY_RETURN_NOT_OK(WriteMessage(MessageType::AsyncGetObjectsRequest, &fbb));
  // 返回 ScopedResponse，析构时自动发送 CancelGetRequest
  return ScopedResponse([this, request_id_to_cleanup = get_request_id]() {
    return CancelGetRequest(request_id_to_cleanup);
  });
}
```

`ScopedResponse` 是 RAII 封装，当对象析构时（如离开作用域），会调用 cleanup 回调发送取消请求。
这确保了即使发生异常，请求也不会泄漏。

### 3.5 Raylet 端消息分发

Raylet 的 `ClientConnection` 异步循环读取消息，读到后调回调：

```
ProcessMessages()                    // 异步读 header (cookie + type + length)
  → ProcessMessageHeader(error)      // 读到 header，校验 cookie
    → async_read(body)               // 异步读 body (payload)
      → ProcessMessage(error)        // 读到完整消息
        → message_handler_(...)      // 回调 NodeManager::ProcessClientMessage
            → switch(message_type)   // 根据 type 分发
```

`ProcessMessages` 启动异步读循环：

```cpp
// src/ray/raylet_ipc_client/client_connection.cc:373
void ClientConnection::ProcessMessages() {
  // Wait for a message header from the client.
  std::vector<boost::asio::mutable_buffer> header{
      boost::asio::buffer(&read_cookie_, sizeof(read_cookie_)),
      boost::asio::buffer(&read_type_, sizeof(read_type_)),
      boost::asio::buffer(&read_length_, sizeof(read_length_)),
  };
  boost::asio::async_read(
      ServerConnection::socket_,
      header,
      boost::bind(&ClientConnection::ProcessMessageHeader,
                  shared_ClientConnection_from_this(),
                  boost::asio::placeholders::error));
}
```

`ProcessMessageHeader` 校验 cookie 并读取 body：

```cpp
// src/ray/raylet_ipc_client/client_connection.cc:407
void ClientConnection::ProcessMessageHeader(const boost::system::error_code &error) {
  if (error) {
    read_length_ = 0;
    ProcessMessage(error);
    return;
  }

  if (closed_) {
    return;
  }

  if (!CheckRayCookie()) {
    RAY_LOG(WARNING) << "Mismatched Ray cookie, closing client connection.";
    Close();
    return;
  }

  // Resize the message buffer to match the received length.
  read_message_.resize(read_length_);
  ServerConnection::bytes_read_ += read_length_;
  // Wait for the message to be read.
  boost::asio::async_read(ServerConnection::socket_,
                          boost::asio::buffer(read_message_),
                          boost::bind(&ClientConnection::ProcessMessage,
                                      shared_ClientConnection_from_this(),
                                      boost::asio::placeholders::error));
}
```

`CheckRayCookie` 验证 cookie 的安全性逻辑：

```cpp
// src/ray/raylet_ipc_client/client_connection.cc:455
bool ClientConnection::CheckRayCookie() {
  if (read_cookie_ == RayConfig::instance().ray_cookie()) {
    return true;
  }

  // Cookie is not matched.
  std::ostringstream ss;
  ss << " ray cookie mismatch for received message. "
     << "received cookie: " << read_cookie_ << ", debug label: " << debug_label_;
  auto remote_endpoint_info = RemoteEndpointInfo();

  if (registered_) {
    // This is from a known client, which indicates a bug.
    RAY_LOG(FATAL) << ss.str();  // 已注册客户端 cookie 不匹配 → FATAL
  } else {
    // It's not from a known client, log this message, and stop processing the connection.
    RAY_LOG(WARNING) << ss.str();  // 未注册客户端 → WARNING 并关闭连接
  }
  return false;
}
```

`ProcessMessage` 调用消息处理回调：

```cpp
// src/ray/raylet_ipc_client/client_connection.cc:487
void ClientConnection::ProcessMessage(const boost::system::error_code &error) {
  auto this_ptr = shared_ClientConnection_from_this();
  if (error) {
    return connection_error_handler_(std::move(this_ptr), error);
  }

  if (closed_) {
    return;
  }

  int64_t start_ms = current_time_ms();
  message_handler_(std::move(this_ptr), read_type_, read_message_);
  int64_t interval = current_time_ms() - start_ms;
  if (interval > RayConfig::instance().handler_warning_timeout_ms()) {
    // 处理超时告警
    RAY_LOG(WARNING) << "[" << debug_label_ << "]ProcessMessage with type "
                     << message_type << " took " << interval << " ms.";
  }
}
```

`ProcessClientMessage` 的分发逻辑：

```cpp
// src/ray/raylet/node_manager.cc:1113
void NodeManager::ProcessClientMessage(
    const std::shared_ptr<ClientConnection> &client,
    int64_t message_type, const uint8_t *message_data) {
  auto registered_worker = worker_pool_.GetRegisteredWorker(client);
  auto message_type_value = static_cast<protocol::MessageType>(message_type);

  // 已死亡 worker 的消息一律忽略，除了 DisconnectClientRequest
  if (registered_worker && registered_worker->IsDead()) {
    if (message_type_value != protocol::MessageType::DisconnectClientRequest) {
      client->ProcessMessages();  // 继续读下一条
      return;
    }
  }

  switch (message_type_value) {
  case protocol::MessageType::RegisterClientRequest:
    ProcessRegisterClientRequestMessage(client, message_data); break;
  case ray::protocol::MessageType::AnnounceWorkerPort:
    ProcessAnnounceWorkerPortMessage(client, message_data); break;
  case protocol::MessageType::ActorCreationTaskDone:
    if (registered_worker) {
      HandleWorkerAvailable(registered_worker);
    } break;
  case protocol::MessageType::DisconnectClientRequest:
    ProcessDisconnectClientMessage(client, message_data);
    return;  // 断开连接，不再继续读消息
  case protocol::MessageType::AsyncGetObjectsRequest:
    HandleAsyncGetObjectsRequest(client, message_data); break;
  case protocol::MessageType::NotifyWorkerBlocked:
    HandleNotifyWorkerBlocked(registered_worker); break;
  case protocol::MessageType::NotifyWorkerUnblocked:
    HandleNotifyWorkerUnblocked(registered_worker); break;
  case protocol::MessageType::CancelGetRequest:
    CancelGetRequest(client, message_data); break;
  case protocol::MessageType::WaitRequest:
    ProcessWaitRequestMessage(client, message_data); break;
  case protocol::MessageType::WaitForActorCallArgsRequest:
    ProcessWaitForActorCallArgsRequestMessage(client, message_data); break;
  case protocol::MessageType::PushErrorRequest:
    ProcessPushErrorRequestMessage(message_data); break;
  case protocol::MessageType::FreeObjectsInObjectStoreRequest: {
    auto message = flatbuffers::GetRoot<protocol::FreeObjectsRequest>(message_data);
    auto object_ids = FlatbufferToObjectIds(*message->object_ids());
    object_manager_.FreeObjects(object_ids, message->local_only());
  } break;
  case protocol::MessageType::SubscribePlasmaReady:
    ProcessSubscribePlasmaReady(client, message_data); break;
  default:
    RAY_LOG(FATAL) << "Received unexpected message type " << message_type;
  }

  // 处理完后继续读下一条消息
  client->ProcessMessages();
}
```

**关键设计**：消息处理完成后，必须调用 `client->ProcessMessages()` 来继续读取下一条消息。
这形成了一个异步读取循环。对于 `DisconnectClientRequest`，直接 return 不再继续读。

### 3.6 完整时序示例：Worker 注册

```
CoreWorker                                  Raylet
    │                                          │
    │  RayletIpcClient(socket_path)            │
    │  ──── connect ─────────────────────────► │  acceptor_.async_accept()
    │                                          │  → HandleAccept()
    │                                          │  → ClientConnection::Create()
    │                                          │  → conn->ProcessMessages()
    │                                          │     (等待读 header...)
    │                                          │
    │  RegisterClient(worker_id, ...)          │
    │  ──── WriteMessage ──────────────────► │  读到 header+body
    │    [cookie|RegisterClientRequest|fbb]    │  → message_handler_
    │                                          │  → ProcessClientMessage()
    │                                          │  → ProcessRegisterClientRequestMessage()
    │                                          │    → Register worker, assign port
    │                                          │
    │  ◄── ReadMessage ──────────────────── │  写回复
    │    [cookie|RegisterClientReply|fbb]      │  → client->ProcessMessages()
    │                                          │    (继续等待下一条消息)
    │  解析回复: node_id, assigned_port        │
    │                                          │
    │  AnnounceWorkerPort(assigned_port)       │
    │  ──── WriteMessage(fire&forget) ─────► │  → ProcessAnnounceWorkerPortMessage()
    │                                          │    → 记录 worker 的 gRPC 端口
    │                                          │    → client->ProcessMessages()
```

更详细的 CoreWorker 启动全流程（`core_worker_process.cc:200-267`）：

```cpp
// 1. 创建 IPC 客户端连接
auto raylet_ipc_client = std::make_shared<ray::ipc::RayletIpcClient>(
    io_service_, options.raylet_socket, /*num_retries=*/-1, /*timeout=*/-1);

// 2. 通过 IPC 向 Raylet 注册，获得 assigned_port (初始值=0)
NodeID local_node_id;
int assigned_port = 0;
bool is_preemptible_node = false;
Status status = raylet_ipc_client->RegisterClient(
    worker_context->GetWorkerID(),
    options.worker_type,
    worker_context->GetCurrentJobID(),
    options.runtime_env_hash,
    options.language,
    options.node_ip_address,
    options.serialized_job_config,
    &local_node_id,
    &assigned_port,          // Raylet 分配的端口（0 = 让 OS 随机分配）
    &is_preemptible_node);

// 3. 用 assigned_port 创建 gRPC Server
auto core_worker_server = std::make_unique<rpc::GrpcServer>(
    WorkerTypeString(options.worker_type),
    assigned_port,            // 0 → OS 随机分配
    options.node_ip_address == "127.0.0.1");
core_worker_server->RegisterService(
    std::make_unique<rpc::CoreWorkerGrpcService>(
        io_service_, *service_handler_, /*max_active_rpcs_per_handler_=*/-1),
    false /* token_auth */);
core_worker_server->Run();

// 4. 通过 IPC 通知 Raylet 自己的实际 gRPC 端口
// Driver 和 Worker 的通知方式不同
if (options.worker_type == rpc::WorkerType::DRIVER) {
    raylet_ipc_client_->AnnounceWorkerPortForDriver(
        core_worker_server_->GetPort(), options_.entrypoint);
} else {
    raylet_ipc_client_->AnnounceWorkerPortForWorker(
        core_worker_server_->GetPort());
}

// 5. 设置自己的 rpc_address（包含 IP + 实际端口）
rpc::Address rpc_address;
rpc_address.set_ip_address(options.node_ip_address);
rpc_address.set_port(core_worker_server->GetPort());  // 实际分配的端口
rpc_address.set_node_id(local_node_id.Binary());
rpc_address.set_worker_id(worker_context->GetWorkerID().Binary());
```

### 3.7 IPC 断连检测

当 IPC 通信失败时，CoreWorker 会检测 Raylet 是否存活，如果不存活则直接退出进程：

```cpp
// src/ray/raylet_ipc_client/raylet_ipc_client.cc:54
void ShutdownIfLocalRayletDisconnected(const Status &status) {
  // Check if the Raylet process is still alive.
  bool raylet_alive = true;
  auto raylet_pid = RayConfig::instance().RAYLET_PID();
  if (!raylet_pid.empty()) {
    if (!IsProcessAlive(static_cast<pid_t>(std::stoi(raylet_pid)))) {
      raylet_alive = false;
    }
  } else if (!IsParentProcessAlive()) {
    raylet_alive = false;
  }

  if (!status.ok() && !raylet_alive) {
    RAY_LOG(WARNING) << "Exiting because the Raylet IPC connection failed and the local "
                        "Raylet is dead. Status: "
                     << status;
    QuickExit();
  }
}
```

这是 Ray 的一个重要设计原则：**Worker 无法在没有 Raylet 的情况下运行**，
如果 Raylet 死了，Worker 也会退出。

---

## 4. CoreWorker 与 Raylet 的双通道通信

CoreWorker 和本机 Raylet 之间有 **两条通信路径**：

### 4.1 Unix Domain Socket (IPC) — `RayletIpcClient`

定义在 `src/ray/raylet_ipc_client/raylet_ipc_client.h`，使用 **FlatBuffer** 编码的本地 socket 通信。

用于**低延迟、高频**的操作：

| 方法 | 用途 | 通信模式 |
|------|------|---------|
| `RegisterClient` | Worker 向 Raylet 注册 | 同步请求-响应 |
| `Disconnect` | 通知 Raylet 断开连接 | 同步请求-响应 |
| `AnnounceWorkerPort` | 告知 Raylet 自己的 gRPC 端口 | 单向通知/同步 |
| `AsyncGetObjects` | 请求 Raylet 拉取远端对象到本地 | 异步请求 |
| `CancelGetRequest` | 取消对象获取请求 | 单向通知 |
| `Wait` | 等待对象就绪 | 同步请求-响应 |
| `NotifyWorkerBlocked/Unblocked` | 通知 Worker 阻塞/恢复（资源释放） | 单向通知 |
| `SubscribePlasmaReady` | 订阅 Plasma 对象就绪通知 | 单向通知 |
| `WaitForActorCallArgs` | 等待 Actor 调用参数就绪 | 单向通知 |
| `PushError` | 推送错误信息 | 单向通知 |
| `FreeObjects` | 释放对象 | 单向通知 |
| `ActorCreationTaskDone` | Actor 创建任务完成 | 单向通知 |

### 4.2 gRPC (NodeManagerService) — `RayletClient`

定义在 `src/ray/raylet_rpc_client/raylet_client.h`，使用 protobuf 定义的 gRPC 服务
（`src/ray/protobuf/node_manager.proto:463`）。

用于**资源管理、调度**等操作：

| 方法 | 用途 |
|------|------|
| `RequestWorkerLease` | 请求租用 Worker |
| `ReturnWorkerLease` | 归还 Worker 租约 |
| `PrestartWorkers` | 预启动 Worker |
| `PrepareBundleResources` | 预留 Placement Group 资源 |
| `CommitBundleResources` | 提交 Placement Group 资源 |
| `CancelResourceReserve` | 取消资源预留 |
| `ReportWorkerBacklog` | 报告任务积压情况 |
| `PinObjectIDs` | Pin 住对象防止被回收 |
| `ReleaseUnusedActorWorkers` | 释放未使用的 Actor Worker |
| `CancelWorkerLease` | 取消 Worker 租约请求 |
| `GetWorkerFailureCause` | 获取 Worker 失败原因 |
| `ShutdownRaylet` | 关闭 Raylet |
| `DrainRaylet` | 排空 Raylet |
| `ResizeLocalResourceInstances` | 调整本地资源实例数量 |
| `RegisterMutableObjectReader` | 注册可变对象读取器 |
| `PushMutableObject` | 推送可变对象 |

**为什么需要双通道？**

IPC Socket 适合低延迟的本地通信（对象获取、阻塞通知），gRPC 适合需要重试、超时、
负载均衡的调度操作（Worker 租用、资源管理）。两者互补。

---

## 5. CoreWorker 与远程 Raylet 的 gRPC 通信

**是的，CoreWorker 可以通过 gRPC 与其他节点的 Raylet 通信。**

CoreWorker 持有 `RayletClientPool`（`src/ray/raylet_rpc_client/raylet_client_pool.h`），
通过 `GetOrConnectByAddress(address)` 可以与任意节点的 Raylet 建立 gRPC 连接。

典型场景：当 CoreWorker 需要在远程节点调度任务时，会通过 RayletClientPool 获取
远端 Raylet 的 gRPC 客户端，然后发送 `RequestWorkerLease` 等 RPC。

---

## 6. CoreWorker 与 CoreWorker 的 gRPC 通信

CoreWorker 之间（无论本机还是远机）**统一使用 gRPC (CoreWorkerService)** 通信，
**不需要经过 Raylet 中转，是直接点对点通信**。

Raylet 只负责**调度决策**（决定任务由哪个 Worker 执行）和**资源分配**
（Worker lease 管理），但任务的实际数据传输（PushTask RPC）是 Worker A → Worker B
**直连 gRPC 通信**，不经过 Raylet转发。

### 6.0 跨节点通信架构图

```
Worker A (CoreWorker)                    Worker B (CoreWorker, 另一台机器)
    │                                         │
    │ ─── PushTask RPC (gRPC直连) ──────────> │  ← 任务数据直接传输
    │    通过 CoreWorkerClientPool            │    不经过 Raylet
    │    .GetOrConnect(addr)                  │
    │                                         │
    │ ←── PushTaskReply ─────────────────── │  ← 结果直接返回
    
Raylet (本节点)                             Raylet (远端节点)
    │                                         │
    │ ─── 调度决策、资源分配 ────────────────> │  ← 仅调度，不转发任务数据
    │    (决定哪个 Worker 执行任务)            │
```

### 6.1 任务推送的直接通信代码链路

#### Normal Task（普通任务）的直接推送

```cpp
// src/ray/core_worker/task_submission/normal_task_submitter.cc:541-561
// NormalTaskSubmitter::PushTask 直接向目标 Worker 发送 PushTaskRequest

auto task_id = task_spec.TaskId();
auto request = std::make_unique<rpc::PushTaskRequest>();
// ... 填充 request ...

// 关键：通过 CoreWorkerClientPool 直接连接目标 Worker，不经过 Raylet
core_worker_client_pool_->GetOrConnect(worker_address);

// 构造回调
rpc::ClientCallback<rpc::PushTaskReply> reply_callback =
    [this, addr, task_spec](const Status &status, const rpc::PushTaskReply &reply) {
      HandlePushTaskReply(status, reply, addr, task_spec);
    };

// 直接发送 RPC 到目标 Worker
core_worker_client_pool_->GetOrConnect(addr)
    ->PushNormalTask(std::move(request), std::move(reply_callback));
```

#### Actor Task（Actor 任务）的直接推送

```cpp
// src/ray/core_worker/task_submission/actor_task_submitter.cc:577-636
// ActorTaskSubmitter::PushActorTask 直接向目标 Actor Worker 发送 PushActorTask

void ActorTaskSubmitter::PushActorTask(ClientQueue &queue,
                                       const TaskSpecification &task_spec,
                                       bool skip_queue) {
  auto request = std::make_unique<rpc::PushTaskRequest>();
  // ... 填充 request ...

  auto &addr = queue.client_address_.value();  // 目标 Actor Worker 的地址

  // 构造回调
  rpc::ClientCallback<rpc::PushTaskReply> reply_callback =
      [this, addr, task_spec](const Status &status, const rpc::PushTaskReply &reply) {
        HandlePushTaskReply(status, reply, addr, task_spec);
      };

  // 关键：直接通过 CoreWorkerClientPool 连接目标 Worker，不经过 Raylet
  core_worker_client_pool_.GetOrConnect(addr)->PushActorTask(
      std::move(request), skip_queue, std::move(wrapped_callback));
}
```

，#### 目标 Worker 的地址获取方式

Worker A 获取 Worker B 的地址（IP + 端口）有以下途径：

1. **Normal Task**：Raylet 通过 `RequestWorkerLeaseReply` 返回目标 Worker 的地址，
   Worker A 用这个地址通过 `CoreWorkerClientPool::GetOrConnect(addr)` 直连目标 Worker
2. **Actor Task**：GCS 返回 Actor 的地址（包含 IP + 篇口 + WorkerID），
   Worker A 用这个地址直连 Actor Worker
3. **对象 Owner 查询**：通过 GCS 获取对象 Owner 的地址，直连 Owner Worker

Raylet **只在调度阶段参与**，告诉 Worker A 去哪个 Worker B 执行任务；
任务的实际数据传输是 CoreWorker 之间的直接 gRPC 点对点通信。

#### Raylet Lease 分配与 Worker 直连的关系

```cpp
// src/ray/core_worker/task_submission/normal_task_submitter.cc:103
// AddWorkerLeaseClient — 获得 lease 后立即直连目标 Worker
void NormalTaskSubmitter::AddWorkerLeaseClient(
    const rpc::Address &worker_address,       // 目标 Worker 地址
    const rpc::Address &raylet_address,       // Raylet 地址（仅用于归还 lease）
    ...) {
  // Raylet 分配了 Worker lease，立即通过 Pool 直连目标 Worker
  core_worker_client_pool_->GetOrConnect(worker_address);
  // ... 记录 lease 信息 ...
}
```

注意 `AddWorkerLeaseClient` 同时接收 `worker_address` 和 `raylet_address`：
- `worker_address` 用于 CoreWorker 直连（通过 CoreWorkerClientPool）
- `raylet_address` 用于归还 lease（通过 RayletClientPool，向 Raylet 发送 ReturnWorkerLease）

### 6.2 CoreWorker gRPC Server 定义

```cpp
// src/ray/core_worker/grpc_service.h:149
class CoreWorkerGrpcService : public GrpcService {
 public:
  CoreWorkerGrpcService(instrumented_io_context &main_service,
                        CoreWorkerServiceHandler &service_handler,
                        int64_t max_active_rpcs_per_handler)
      : GrpcService(main_service),
        service_handler_(service_handler),
        max_active_rpcs_per_handler_(max_active_rpcs_per_handler) {}

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(...) override;

 private:
  CoreWorkerService::AsyncService service_;  // gRPC 异步服务
  CoreWorkerServiceHandler &service_handler_;
  int64_t max_active_rpcs_per_handler_;
};
```

### 主要的 CoreWorker ↔ CoreWorker gRPC 方法

定义在 `src/ray/core_worker_rpc_client/core_worker_client.h`：

| 方法 | 用途 | RPC 类型 |
|------|------|---------|
| `PushActorTask` | 推送 Actor 任务到目标 Worker 执行 | 普通 RPC（有背压控制） |
| `PushNormalTask` | 推送普通任务到目标 Worker 执行 | 普通 RPC |
| `GetObjectStatus` | 查询对象状态（是否就绪/丢失） | 可重试 RPC |
| `KillActor` | 杀死远程 Actor | 普通 RPC |
| `CancelTask` | 取消任务 | 普通 RPC |
| `RequestOwnerToCancelTask` | 请求任务 Owner 取消任务 | 可重试 RPC |
| `WaitForActorRefDeleted` | 等待 Actor 引用删除 | 可重试 RPC |
| `PubsubLongPolling` | 对象位置 pubsub 长轮询 | 可重试 RPC |
| `PubsubCommandBatch` | 对象位置 pubsub 批量命令 | 可重试 RPC |
| `UpdateObjectLocationBatch` | 批量更新对象位置信息 | 可重试 RPC |
| `GetObjectLocationsOwner` | 获取对象位置（Owner 查询） | 普通 RPC |
| `ReportGeneratorItemReturns` | Streaming Generator 返回值上报 | 可重试 RPC |
| `PlasmaObjectReady` | 通知 Plasma 对象就绪 | 普通 RPC |
| `AssignObjectOwner` | 分配对象所有权 | 普通 RPC |
| `LocalGC` | 触发本地 GC | 普通 RPC |
| `DeleteObjects` | 删除对象 | 普通 RPC |
| `SpillObjects` | 溢出对象到外部存储 | 普通 RPC |
| `RestoreSpilledObjects` | 从外部存储恢复对象 | 普通 RPC |
| `DeleteSpilledObjects` | 删除已溢出对象 | 普通 RPC |
| `RayletNotifyGCSRestart` | 通知 Raylet GCS 重启 | 普通 RPC |
| `Exit` | 退出 Worker | 普通 RPC |
| `RegisterMutableObjectReader` | 注册可变对象读取器 | 普通 RPC |
| `GetCoreWorkerStats` | 获取 CoreWorker 统计信息 | 普通 RPC |
| `NumPendingTasks` | 查询待处理任务数 | 普通 RPC |

### CoreWorkerClient 的背压控制

Actor 任务推送实现了基于字节数的背压控制：

```cpp
// src/ray/core_worker_rpc_client/core_worker_client.h:35
inline constexpr int64_t kMaxBytesInFlight = 16L * 1024 * 1024;  // 16MB

// src/ray/core_worker_rpc_client/core_worker_client.h:198
void PushActorTask(std::unique_ptr<PushTaskRequest> request,
                   bool skip_queue,
                   ClientCallback<PushTaskReply> &&callback) override;

// 内部有发送队列
std::deque<std::pair<std::unique_ptr<PushTaskRequest>, ClientCallback<PushTaskReply>>>
    send_queue_ ABSL_GUARDED_BY(mutex_);
int64_t rpc_bytes_in_flight_ ABSL_GUARDED_BY(mutex_) = 0;
```

当 `rpc_bytes_in_flight_` 超过 `kMaxBytesInFlight` (16MB) 时，新任务会排队等待，
避免远端 Worker 的调度队列被压垮。

---

## 7. RayletClientPool ↔ RayletClient 关系

```
┌─────────────────────────────────────────────────────────────┐
│                    CoreWorker 进程                            │
│                                                             │
│  ┌────────────────────────────────────────────────────┐     │
│  │              RayletClientPool                       │     │
│  │                                                    │     │
│  │  client_map_: {                                    │     │
│  │    NodeID_1 → RayletClient (本地 Raylet gRPC)      │     │
│  │    NodeID_2 → RayletClient (远端 Raylet gRPC)      │     │
│  │    NodeID_3 → RayletClient (远端 Raylet gRPC)      │     │
│  │  }                                                 │     │
│  │                                                    │     │
│  │  GetOrConnectByAddress(addr) → 查缓存/建新连接      │     │
│  │  Disconnect(node_id)       → 从缓存中移除           │     │
│  └──────────────┬─────────────────────────────────────┘     │
│                 │                                           │
│     ┌───────────┴───────────┐                               │
│     ▼                       ▼                               │
│  ┌──────────────┐   ┌──────────────┐                        │
│  │ RayletClient │   │ RayletClient │   ← 轻量 gRPC stub    │
│  │  (本地)      │   │  (远端)      │                        │
│  │              │   │              │                        │
│  │ grpc_client_ │   │ grpc_client_ │                        │
│  │ retryable_   │   │ retryable_   │                        │
│  │ grpc_client_ │   │ grpc_client_ │                        │
│  └──────┬───────┘   └──────┬───────┘                        │
│         │                  │                                │
└─────────┼──────────────────┼────────────────────────────────┘
          │ gRPC              │ gRPC
          ▼                  ▼
   ┌──────────────┐   ┌──────────────┐
   │ 本地 Raylet   │   │ 远端 Raylet   │
   │ NodeManager   │   │ NodeManager   │
   │ Service       │   │ Service       │
   └──────────────┘   └──────────────┘
```

**RayletClientPool 是连接池**，管理多个 `RayletClient` 实例。数据结构简单：

```cpp
// src/ray/raylet_rpc_client/raylet_client_pool.h:34
class RayletClientPool {
 private:
  absl::Mutex mu_;
  RayletClientFactoryFn client_factory_;  // 工厂函数

  // 以 NodeID 为键的连接缓存
  absl::flat_hash_map<ray::NodeID, std::shared_ptr<ray::RayletClientInterface>>
      client_map_ ABSL_GUARDED_BY(mu_);
};
```

核心方法 `GetOrConnectByAddress`：

```cpp
// src/ray/raylet_rpc_client/raylet_client_pool.cc:81
std::shared_ptr<ray::RayletClientInterface>
RayletClientPool::GetOrConnectByAddress(const rpc::Address &address) {
  RAY_CHECK(address.node_id() != "");
  absl::MutexLock lock(&mu_);
  auto node_id = NodeID::FromBinary(address.node_id());

  // 1. 缓存命中 → 直接返回已有连接
  auto it = client_map_.find(node_id);
  if (it != client_map_.end()) {
    RAY_CHECK(it->second != nullptr);
    return it->second;   // 复用已有 gRPC 连接
  }

  // 2. 缓存未命中 → 用工厂函数创建新 RayletClient
  auto connection = client_factory_(address);  // 新建 gRPC channel
  client_map_[node_id] = connection;           // 缓存起来

  RAY_LOG(DEBUG) << "Connected to raylet " << node_id << " at "
                 << BuildAddress(address.ip_address(), address.port());
  RAY_CHECK(connection != nullptr);
  return connection;
}
```

`Disconnect` 移除指定节点的连接：

```cpp
// src/ray/raylet_rpc_client/raylet_client_pool.cc:100
void RayletClientPool::Disconnect(ray::NodeID id) {
  absl::MutexLock lock(&mu_);
  auto it = client_map_.find(id);
  if (it == client_map_.end()) {
    return;
  }
  client_map_.erase(it);
}
```

`GenerateRayletAddress` 辅助方法，根据 NodeID + IP + 端口构造 RPC 地址：

```cpp
// src/ray/raylet_rpc_client/raylet_client_pool.cc:109
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

**RayletClient** 是 gRPC 客户端封装，持有到某个 Raylet 的 gRPC channel：

```cpp
// src/ray/raylet_rpc_client/raylet_client.cc:32
RayletClient::RayletClient(const rpc::Address &address,
                           rpc::ClientCallManager &client_call_manager,
                           std::function<void()> raylet_unavailable_timeout_callback)
    : grpc_client_(std::make_shared<rpc::GrpcClient<rpc::NodeManagerService>>(
          address.ip_address(), address.port(), client_call_manager)),  // gRPC channel
      retryable_grpc_client_(rpc::RetryableGrpcClient::Create(
          grpc_client_->Channel(),
          client_call_manager.GetMainService(),
          /*max_pending_requests_bytes=*/std::numeric_limits<uint64_t>::max(),
          /*check_channel_status_interval_milliseconds=*/
          ::RayConfig::instance()
              .grpc_client_check_connection_status_interval_milliseconds(),
          /*server_reconnect_timeout_base_seconds=*/
          ::RayConfig::instance().raylet_rpc_server_reconnect_timeout_base_s(),
          /*server_reconnect_timeout_max_seconds=*/
          ::RayConfig::instance().raylet_rpc_server_reconnect_timeout_max_s(),
          /*server_unavailable_timeout_callback=*/
          std::move(raylet_unavailable_timeout_callback),
          /*server_name=*/std::string("Raylet ") + address.ip_address())),
      pins_in_flight_(std::make_shared<std::atomic<int64_t>>(0)) {}
```

`RayletClient` 有两个 gRPC 客户端：
- `grpc_client_`：普通 gRPC 客户端，用于不需要重试的 RPC（如 `ReturnWorkerLease`、`PrestartWorkers`）
- `retryable_grpc_client_`：可重试的 gRPC 客户端，用于需要自动重试的 RPC（如 `RequestWorkerLease`）

断连检测机制——当 gRPC 连不上远端 Raylet 时，`RetryableGrpcClient` 触发超时回调，
Pool 通过 GCS 检查节点是否死亡，若死亡则移除缓存：

```cpp
// src/ray/raylet_rpc_client/raylet_client_pool.cc:24
// 超时回调逻辑：
// 1. 如果 GCS 订阅了节点变化 → 查本地缓存
//    → 节点死亡 → raylet_client_pool->Disconnect(node_id)
// 2. 如果没有订阅 → 主动查 GCS
//    → 节点死亡 → raylet_client_pool->Disconnect(node_id)
```

---

## 8. CoreWorkerClientPool ↔ CoreWorkerClient 关系

```
┌───────────────────────────────────────────────────────────────────┐
│                       CoreWorker 进程 A                           │
│                                                                   │
│  ┌─────────────────────────────────────────────────────────┐      │
│  │              CoreWorkerClientPool                        │      │
│  │                                                         │      │
│  │  worker_client_map_: {                                  │      │
│  │    WorkerID_1 → iterator → CoreWorkerClientEntry        │      │
│  │    WorkerID_2 → iterator → CoreWorkerClientEntry        │      │
│  │    WorkerID_3 → iterator → CoreWorkerClientEntry        │      │
│  │  }                                                     │      │
│  │                                                         │      │
│  │  node_clients_map_: {                                   │      │
│  │    NodeID_1 → { WorkerID_1, WorkerID_2 }               │      │
│  │    NodeID_2 → { WorkerID_3 }                            │      │
│  │  }                                                     │      │
│  │                                                         │      │
│  │  client_list_: [最近访问 ... 最久访问]  ← LRU 清理      │      │
│  │                                                         │      │
│  │  GetOrConnect(addr) → 查缓存/建新连接/移至队首           │      │
│  │  Disconnect(worker_id) → 移除单个                       │      │
│  │  Disconnect(node_id)  → 移除节点上所有 worker 连接       │      │
│  │  RemoveIdleClients()  → LRU 淘汰空闲连接                │      │
│  └────────┬──────────────┬──────────────┬──────────────────┘      │
│           │              │              │                         │
│     ┌─────▼─────┐  ┌────▼──────┐  ┌───▼──────────┐              │
│     │CoreWorker │  │CoreWorker │  │CoreWorker     │              │
│     │Client     │  │Client     │  │Client         │              │
│     │(同节点W1) │  │(同节点W2) │  │(远端节点W3)   │              │
│     │           │  │           │  │               │              │
│     │grpc_client│  │grpc_client│  │grpc_client    │              │
│     │retryable_ │  │retryable_ │  │retryable_     │              │
│     │grpc_client│  │grpc_client│  │grpc_client    │              │
│     └─────┬─────┘  └────┬──────┘  └──────┬────────┘              │
│           │              │                │                       │
└───────────┼──────────────┼────────────────┼───────────────────────┘
            │ gRPC         │ gRPC           │ gRPC
            ▼              ▼                ▼
     ┌────────────┐ ┌────────────┐   ┌────────────┐
     │CoreWorker  │ │CoreWorker  │   │CoreWorker  │
     │进程 W1     │ │进程 W2     │   │进程 W3     │
     │gRPC Server │ │gRPC Server │   │gRPC Server │
     └────────────┘ └────────────┘   └────────────┘
```

**CoreWorkerClientPool** 比 RayletClientPool 复杂得多，因为它需要管理更多连接、
支持 LRU 淘汰、支持按节点批量断连。

### 数据结构

```cpp
// src/ray/core_worker_rpc_client/core_worker_client_pool.h:34
class CoreWorkerClientPool {
 private:
  CoreWorkerClientFactoryFn core_worker_client_factory_;  // 工厂函数

  absl::Mutex mu_;

  struct CoreWorkerClientEntry {
    WorkerID worker_id_;
    NodeID node_id_;
    std::shared_ptr<CoreWorkerClientInterface> core_worker_client_;
  };

  // LRU 列表：最近访问的在队首，最久访问的在队尾
  std::list<CoreWorkerClientEntry> client_list_ ABSL_GUARDED_BY(mu_);

  // WorkerID → LRU 列表迭代器（O(1) 查找）
  absl::flat_hash_map<WorkerID, std::list<CoreWorkerClientEntry>::iterator>
      worker_client_map_ ABSL_GUARDED_BY(mu_);

  // NodeID → { WorkerID → LRU 列表迭代器 }（支持按节点批量操作）
  absl::flat_hash_map<NodeID, WorkerIdClientMap>
      node_clients_map_ ABSL_GUARDED_BY(mu_);
};
```

三个数据结构的协作关系：
- `client_list_`: LRU 列表，用于空闲连接淘汰
- `worker_client_map_`: 按 WorkerID 快速查找
- `node_clients_map_`: 按节点组织，支持节点级批量断连

### 核心方法 GetOrConnect

```cpp
// src/ray/core_worker_rpc_client/core_worker_client_pool.cc:118
std::shared_ptr<CoreWorkerClientInterface>
CoreWorkerClientPool::GetOrConnect(const Address &addr_proto) {
  RAY_CHECK_NE(addr_proto.worker_id(), "");
  absl::MutexLock lock(&mu_);

  RemoveIdleClients();  // 先淘汰空闲连接

  CoreWorkerClientEntry entry;
  auto node_id = NodeID::FromBinary(addr_proto.node_id());
  auto worker_id = WorkerID::FromBinary(addr_proto.worker_id());

  auto it = worker_client_map_.find(worker_id);
  if (it != worker_client_map_.end()) {
    // 缓存命中：从 LRU 列表中取出，后面移到队首
    entry = *it->second;
    client_list_.erase(it->second);
  } else {
    // 缓存未命中：用工厂函数创建新 CoreWorkerClient
    entry = CoreWorkerClientEntry(
        worker_id, node_id, core_worker_client_factory_(addr_proto));
  }

  // 放到 LRU 队首（最近访问）
  client_list_.emplace_front(entry);
  worker_client_map_[worker_id] = client_list_.begin();
  node_clients_map_[node_id][worker_id] = client_list_.begin();

  RAY_LOG(DEBUG) << "Connected to worker " << worker_id << " with address "
                 << BuildAddress(addr_proto.ip_address(), addr_proto.port());
  return entry.core_worker_client_;
}
```

### LRU 空闲淘汰

每次 `GetOrConnect` 时检查队尾（最久未访问），如果空闲就淘汰：

```cpp
// src/ray/core_worker_rpc_client/core_worker_client_pool.cc:145
void CoreWorkerClientPool::RemoveIdleClients() {
  while (!client_list_.empty()) {
    auto worker_id = client_list_.back().worker_id_;
    auto node_id = client_list_.back().node_id_;
    // 队尾 = 最久未访问
    if (client_list_.back().core_worker_client_->IsIdleAfterRPCs()) {
      // 空闲 → 从所有数据结构中移除
      worker_client_map_.erase(worker_id);
      EraseFromNodeClientMap(node_id, worker_id);
      client_list_.pop_back();
      RAY_LOG(DEBUG) << "Remove idle client to worker " << worker_id
                     << " , num of clients is now " << client_list_.size();
    } else {
      // 非空闲 → 移到队首，停止淘汰
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

`IsIdleAfterRPCs()` 的判断逻辑在 `CoreWorkerClient` 中：

```cpp
// src/ray/core_worker_rpc_client/core_worker_client.h:57
bool IsIdleAfterRPCs() const override {
  return grpc_client_->IsChannelIdleAfterRPCs() &&
         retryable_grpc_client_->NumActiveRequests() == 0;
}
```

只有当 gRPC channel 空闲且没有活跃请求时，才认为该客户端可以淘汰。

### 按 WorkerID 断连

```cpp
// src/ray/core_worker_rpc_client/core_worker_client_pool.cc:167
void CoreWorkerClientPool::Disconnect(const WorkerID &id) {
  absl::MutexLock lock(&mu_);
  auto it = worker_client_map_.find(id);
  if (it == worker_client_map_.end()) {
    return;
  }
  EraseFromNodeClientMap(it->second->node_id_, /*worker_id=*/id);
  client_list_.erase(it->second);
  worker_client_map_.erase(it);
}
```

### 按节点批量断连

当整个节点死亡时，一次断开该节点上所有 Worker 的连接：

```cpp
// src/ray/core_worker_rpc_client/core_worker_client_pool.cc:178
void CoreWorkerClientPool::Disconnect(const NodeID &node_id) {
  absl::MutexLock lock(&mu_);
  auto node_client_map_it = node_clients_map_.find(node_id);
  if (node_client_map_it == node_clients_map_.end()) {
    return;
  }
  auto &node_worker_id_client_map = node_client_map_it->second;
  // 遍历该节点上所有 worker，逐个移除
  for (auto &[worker_id, client_iterator] : node_worker_id_client_map) {
    worker_client_map_.erase(worker_id);
    client_list_.erase(client_iterator);
  }
  node_clients_map_.erase(node_client_map_it);
}
```

### 断连检测链

CoreWorkerClient 的 `RetryableGrpcClient` 检测到对端不可达时，触发超时回调，
回调逻辑是多层检查：

```
gRPC 不可达 → 超时回调
  → 查 GCS：节点是否存活？
    → 节点死亡 → pool->Disconnect(node_id)   // 移除该节点所有连接
    → 节点存活 → 查 Raylet：Worker 是否死亡？
      → Worker 死亡 → pool->Disconnect(worker_id)  // 移除单个连接
      → Worker 存活 → 重试
```

详细代码：

```cpp
// src/ray/core_worker_rpc_client/core_worker_client_pool.cc:28
std::function<void()> CoreWorkerClientPool::GetDefaultUnavailableTimeoutCallback(
    gcs::GcsClient *gcs_client,
    rpc::CoreWorkerClientPool *worker_client_pool,
    rpc::RayletClientPool *raylet_client_pool,
    const rpc::Address &addr) {
  return [addr, gcs_client, worker_client_pool, raylet_client_pool]() {
    const NodeID node_id = NodeID::FromBinary(addr.node_id());
    const WorkerID worker_id = WorkerID::FromBinary(addr.worker_id());

    // 检查 Worker 是否存活的 lambda
    auto check_worker_alive = [raylet_client_pool,
                               worker_client_pool,
                               worker_id,
                               node_id](const rpc::GcsNodeAddressAndLiveness &node_info) {
      // 通过 Raylet 的 gRPC 接口检查 Worker 是否死亡
      auto raylet_addr = RayletClientPool::GenerateRayletAddress(
          node_id, node_info.node_manager_address(), node_info.node_manager_port());
      auto raylet_client = raylet_client_pool->GetOrConnectByAddress(raylet_addr);
      raylet_client->IsLocalWorkerDead(
          worker_id,
          [worker_client_pool, worker_id, node_id](const Status &status,
                                                   rpc::IsLocalWorkerDeadReply &&reply) {
            if (!status.ok()) {
              RAY_LOG(INFO).WithField(worker_id).WithField(node_id)
                  << "Failed to check if worker is dead on request to raylet";
              return;
            }
            if (reply.is_dead()) {
              RAY_LOG(INFO).WithField(worker_id).WithField(node_id)
                  << "Disconnecting core worker client because the worker is dead";
              worker_client_pool->Disconnect(worker_id);
            }
          });
    };

    // 检查节点是否存活的 lambda
    auto gcs_check_node_alive = [...]() {
      gcs_client->Nodes().AsyncGetAllNodeAddressAndLiveness(
          [...](const Status &status,
                std::vector<rpc::GcsNodeAddressAndLiveness> &&nodes) {
            if (nodes.empty() || nodes[0].state() != rpc::GcsNodeInfo::ALIVE) {
              worker_client_pool->Disconnect(node_id);  // 节点死亡
              return;
            }
            check_worker_alive(nodes[0]);  // 节点存活，进一步检查 Worker
          },
          -1,
          {node_id});
    };

    if (gcs_client->Nodes().IsSubscribedToNodeChange()) {
      // 已订阅节点变化 → 直接查本地缓存
      auto node_info = gcs_client->Nodes().GetNodeAddressAndLiveness(
          node_id, /*filter_dead_nodes=*/false);
      if (!node_info) {
        gcs_check_node_alive();  // 缓存未命中，主动查 GCS
        return;
      }
      if (node_info->state() == rpc::GcsNodeInfo::DEAD) {
        worker_client_pool->Disconnect(node_id);  // 节点死亡
        return;
      }
      check_worker_alive(*node_info);  // 节点存活，检查 Worker
      return;
    }
    // 未订阅 → 主动查 GCS
    gcs_check_node_alive();
  };
}
```

### CoreWorkerClient 的结构

`CoreWorkerClient` 本身是到某个远端 CoreWorker 的 gRPC 客户端封装：

```cpp
// src/ray/core_worker_rpc_client/core_worker_client.h:41
class CoreWorkerClient : public std::enable_shared_from_this<CoreWorkerClient>,
                         public CoreWorkerClientInterface {
 private:
  absl::Mutex mutex_;

  /// Address of the remote worker.
  rpc::Address addr_;

  /// The RPC client. (普通 gRPC，用于不需要重试的调用)
  std::shared_ptr<GrpcClient<CoreWorkerService>> grpc_client_;

  /// The retryable RPC client. (可重试 gRPC，用于需要自动重试的调用)
  std::shared_ptr<RetryableGrpcClient> retryable_grpc_client_;

  /// Queue of requests to send. (Actor 任务发送队列，用于背压控制)
  std::deque<std::pair<std::unique_ptr<PushTaskRequest>,
                       ClientCallback<PushTaskReply>>> send_queue_;

  /// The number of bytes currently in flight. (飞行中字节数，上限 16MB)
  int64_t rpc_bytes_in_flight_ = 0;

  /// The max sequence number we have processed responses for.
  std::optional<int64_t> max_finished_seq_no_;
};
```

---

## 9. 两个 Pool 的关键差异对比

| 维度 | RayletClientPool | CoreWorkerClientPool |
|------|-----------------|---------------------|
| **连接目标** | Raylet（每节点1个） | CoreWorker（每节点N个） |
| **索引方式** | 单层 map: `NodeID → Client` | 三层数据结构: `WorkerID→iterator` + `NodeID→{WorkerID→iterator}` + `client_list_` LRU |
| **连接数量级** | 小（=集群节点数） | 大（=集群 Worker 总数） |
| **淘汰策略** | 无（节点死亡才断） | **LRU 淘汰空闲连接** |
| **批量断连** | `Disconnect(node_id)` | `Disconnect(node_id)` + `Disconnect(worker_id)` |
| **线程安全** | `absl::Mutex` | `absl::Mutex` + `ABSL_GUARDED_BY` 注解 |
| **工厂函数** | `RayletClientFactoryFn` → `RayletClient` | `CoreWorkerClientFactoryFn` → `CoreWorkerClient` |
| **gRPC Service** | `NodeManagerService` | `CoreWorkerService` |
| **断连检测** | GCS 查节点存活 → 移除 | GCS 查节点存活 → Raylet 查 Worker 存活 → 移除 |

CoreWorkerClientPool 需要 LRU 的原因很直观：一个集群可能有成千上万个 Worker，
但一个 CoreWorker 通常只和其中一小部分通信。如果不淘汰空闲连接，gRPC channel
会无限累积，占用大量内存和文件描述符。而 Raylet 数量 = 节点数，通常几十到几百，
不需要淘汰。

---

## 10. 端口与 Socket 文件资源总结

| 通信方式 | 占用资源 | 每 Worker 独占？ |
|----------|----------|-----------------|
| **Unix Domain Socket** (CoreWorker ↔ 本机 Raylet) | 文件系统路径（如 `/tmp/ray/.../sockets/raylet`），**不占 TCP 端口** | ❌ 所有 Worker 共享同一个 socket 文件 |
| **gRPC / CoreWorkerService** (CoreWorker ↔ CoreWorker) | **TCP 端口**（OS 随机分配） | ✅ 每个 CoreWorker 独占一个端口 |
| **gRPC / NodeManagerService** (CoreWorker ↔ Raylet) | **TCP 端口**（Raylet 的固定端口） | ❌ 所有 Worker 连同一个 Raylet 端口 |

在 Ray 集群中：
- **端口消耗 = Raylet 数量 + CoreWorker 数量**（每节点一个 Raylet 端口 + N 个 Worker 端口）
- IPC socket 是"免费"的，不走网络栈，也不消耗端口
- Windows 是特例——因为不支持 Unix Domain Socket，IPC 也会回退到 `tcp://localhost:random_port`，此时也会消耗端口

### CoreWorker gRPC 端口分配流程

```cpp
// src/ray/core_worker/core_worker_process.cc:204-267

// 1. 初始 port 为 0
int assigned_port = 0;

// 2. 通过 IPC 向 Raylet 注册，Raylet 可能分配端口（通常仍为 0）
Status status = raylet_ipc_client->RegisterClient(
    ..., &assigned_port, ...);

// 3. 用 assigned_port 创建 gRPC Server（0 → OS 随机分配）
auto core_worker_server = std::make_unique<rpc::GrpcServer>(
    WorkerTypeString(options.worker_type),
    assigned_port,  // 0 → OS 随机分配可用端口
    options.node_ip_address == "127.0.0.1");
core_worker_server->RegisterService(
    std::make_unique<rpc::CoreWorkerGrpcService>(...));
core_worker_server->Run();

// 4. 获取实际端口
int actual_port = core_worker_server->GetPort();

// 5. 通过 IPC 通知 Raylet 自己的实际 gRPC 端口
raylet_ipc_client_->AnnounceWorkerPortForWorker(actual_port);

// 6. 设置自己的 rpc_address
rpc_address.set_port(actual_port);
```

`GrpcServer` 构造函数的注释（`grpc_server.h:91`）：

> `port` The port to bind this server to. **If it's 0, a random available port will be chosen.**

---

## 11. Ray 核心组件间全交互矩阵

> 本节系统梳理 Ray 集群中四大组件（GCS、Raylet、Worker/CoreWorker、ObjectManager）之间的
> 全部交互场景，包括通信协议、触发条件、代码链路和源码依据。

### 11.1 交互关系总览图

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                                GCS Server                                   │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐        │
│  │NodeInfo │  │WorkerInfo│  │JobInfo  │  │ActorInfo│  │PlacementGroup│      │
│  │Accessor │  │Accessor  │  │Accessor │  │Accessor │  │InfoAccessor  │      │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘  └──────┬───────┘    │
│       │              │              │              │              │            │
│       │  PubSub      │  PubSub      │  PubSub      │  RPC         │  RPC       │
│       │  + RPC       │  + RPC       │  + RPC       │             │            │
└───────┼──────────────┼──────────────┼──────────────┼─────────────┼────────────┘
        │              │              │              │             │
        ▼              ▼              ▼              ▼             ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│                              Raylet (每节点1个)                                │
│  ┌────────────┐  ┌──────────────┐  ┌──────────────────┐  ┌──────────────┐    │
│  │NodeManager │  │ClusterLease  │  │PlacementGroup    │  │ObjectManager│    │
│  │             │  │Manager      │  │ResourceManager   │  │              │    │
│  └──────┬─────┘  └──────┬──────┘  └──────────────────┘  └──────┬───────┘    │
│         │               │                                        │             │
│    IPC Socket        Spillback Reply                    Push/Pull/Free       │
│    + gRPC            (via reply)                        (ObjectManagerService) │
└─────────┼───────────────┼──────────────────────────────────┼──────────────┘
          │               │                                  │
          ▼               │                                  ▼
┌──────────────────┐      │                    ┌──────────────────────────────┐
│  CoreWorker 1    │      │                    │  远端 ObjectManager          │
│  (本节点)         │      │                    │  (Push/Pull/FreeObjects)     │
│  ┌─────────────┐ │      │                    └──────────────────────────────┘
│  │GcsClient    │─┼──────┼─────────────────────────────→ GCS RPC
│  │RayletClientPool│    │
│  │CoreWorkerClientPool │
│  │TaskEventBuffer│ │   │
│  │ActorCreator  │ │   │
│  └─────────────┘ │   │
│                  │   │  gRPC (CoreWorkerService)
│                  │   ▼
│                  │  ┌──────────────────┐
│                  │  │  CoreWorker 2    │  (远端节点)
│                  └─►│  (PushTask 等)  │
│                     └──────────────────┘
│
│  Raylet↔Raylet 交互路径:
│  ① Spillback: Raylet A → reply(retry_at_raylet_address) → CoreWorker → Raylet B
│  ② ObjectManager: ObjectManager A → Push/Pull/FreeObjects RPC → ObjectManager B
│  ③ FormatGlobalMemoryInfo: Raylet A → GetNodeStats RPC → Raylet B (唯一直接调用)
│  ④ ray_syncer: Raylet A → GCS channel → Raylet B (资源视图同步)
└──────────────────────────────────────────────────────────────────────────────────
```

### 11.2 Raylet ↔ GCS 交互

Raylet 通过 `gcs::GcsClient` 与 GCS 交互，涵盖注册、订阅、上报、查询四大模式。

#### 11.2.1 Raylet 启动注册

**触发时机**：Raylet 进程启动后，所有组件初始化完毕时。

```cpp
// src/ray/raylet/node_manager.cc:314-327
void NodeManager::Start(rpc::GcsNodeInfo &&self_node_info) {
  auto register_callback = [this](const Status &status) {
    RAY_CHECK_OK(status);
    this->RegisterGcs();  // 注册成功后订阅 GCS 事件
  };
  gcs_client_.Nodes().RegisterSelf(std::move(self_node_info), register_callback);
}
```

`self_node_info` 在 `main.cc:1071-1112` 中构建，包含 node_id、node_manager_address、
node_manager_port、object_manager_port、raylet_socket_name 等节点元信息。
底层 RPC 为 `RegisterNode`。

**注销**（优雅关闭时）：

```cpp
// src/ray/raylet/main.cc:494-496
gcs_client->Nodes().UnregisterSelf(
    raylet_node_id, node_death_info, std::move(unregister_done_callback));
```

#### 11.2.2 Raylet 订阅 GCS 事件

在 `RegisterGcs()` 中设置三条订阅通道：

| 订阅通道 | GCS API | 回调处理 | 源码位置 |
|---------|---------|---------|---------|
| 节点增减 | `Nodes().AsyncSubscribeToNodeAddressAndLivenessChange()` | `NodeAdded()` / `NodeRemoved()` | `node_manager.cc:387` |
| Worker 失败 | `Workers().AsyncSubscribeToWorkerFailures()` | `HandleUnexpectedWorkerFailure()` | `node_manager.cc:399` |
| Job 事件 | `Jobs().AsyncSubscribeAll()` | `HandleJobStarted()` / `HandleJobFinished()` | `node_manager.cc:416` |

**节点增减回调**：

```cpp
// src/ray/raylet/node_manager.cc:334-340
auto on_node_change = [this](const NodeID &node_id,
                             const rpc::GcsNodeAddressAndLiveness &data) {
    if (data.state() == GcsNodeInfo::ALIVE) {
        NodeAdded(data);       // 存储远端 Raylet 地址，更新资源视图
    } else {
        NodeRemoved(node_id);  // 取消 lease，断连 Worker，清理资源映射
    }
};
```

**Worker 失败回调**：当集群中任意 Raylet 上报 Worker 失败后，GCS 通过 PubSub 通知所有
其他 Raylet。收到通知后 `HandleUnexpectedWorkerFailure()` 取消该失败 Worker 持有的所有
lease，并杀死以该 Worker 为 owner 的本地 Worker。

**Job 事件回调**：Job 启动时 `HandleJobStarted()` 通知 WorkerPool 新 Job 配置并重新调度；
Job 结束时 `HandleJobFinished()` 强制杀死该 Job 下所有 Worker 进程（detached actor 除外）。

#### 11.2.3 Raylet 向 GCS 上报状态

| 事件 | GCS API | 底层 RPC | 触发条件 | 源码位置 |
|------|---------|---------|---------|---------|
| Worker 失败 | `Workers().AsyncReportWorkerFailure()` | `ReportWorkerFailure` | Worker 断连（crash/OOM/被杀） | `node_manager.cc:1467` |
| Job 完成 | `Jobs().AsyncMarkFinished()` | `MarkJobFinished` | Driver 断连 | `node_manager.cc:1559` |
| Job 错误 | `Errors().AsyncReportJobError()` | `ReportJobError` | Worker 异常退出 / Worker 推送错误 / Plasma 写入失败 | `node_manager.cc:1508, 1734, 2313` |
| Job 新增 | `Jobs().AsyncAdd()` | `AddJob` | Driver 连接注册时 | `node_manager.cc:1333` |

**Worker 失败上报示例**：

```cpp
// src/ray/raylet/node_manager.cc:1455-1467
auto worker_failure_data_ptr = gcs::CreateWorkerFailureData(
    worker->WorkerId(), self_node_id_, ..., disconnect_type, disconnect_detail, ...);
gcs_client_.Workers().AsyncReportWorkerFailure(worker_failure_data_ptr, nullptr);
```

GCS 收到后，`GcsWorkerManager::HandleReportWorkerFailure` 将 Worker 标记为不存活，
持久化到 WorkerTable，通过 PubSub 发布给所有订阅的 Raylet，并通知 ActorManager、
PlacementGroupScheduler、TaskManager 处理级联影响。

#### 11.2.4 GCS 向 Raylet 下发指令（RPC Handler）

GCS 主动向 Raylet 发送 RPC 请求，Raylet 作为服务端响应：

| RPC 方法 | 触发条件 | Raylet 处理逻辑 | 源码位置 |
|---------|---------|-----------------|---------|
| `PrepareBundleResources` | Placement Group 创建 2PC 第一阶段 | 检查并预留资源，返回成功/失败 | `node_manager.cc:1909` |
| `CommitBundleResources` | Placement Group 创建 2PC 第二阶段 | 提交资源预留，重新触发调度 | `node_manager.cc:1926` |
| `CancelResourceReserve` | Placement Group 删除/创建失败 | 取消 lease、杀死关联 Worker、归还资源 | `node_manager.cc:1941` |
| `ReleaseUnusedBundles` | GCS 重启后清理 | 释放无主 bundle 资源 | `node_manager.cc` |
| `NotifyGCSRestart` | GCS 重启后 | `AsyncResubscribe()` + 通知本地 Worker | `node_manager.cc:1062` |
| `DrainRaylet` | Autoscaler 触发节点排空 | 标记 draining、重新调度本地 lease | `node_manager.cc:2142` |
| `ShutdownRaylet` | 排空完成后关闭 Raylet | 优雅退出或立即退出 | `node_manager.cc:2205` |

#### 11.2.5 资源同步 (ray_syncer)

Raylet 通过 `ray_syncer` 机制经 GCS gRPC channel 与其他 Raylet 同步资源视图。

```cpp
// src/ray/raylet/node_manager.cc:342-382
ray_syncer_.Register(
    syncer::MessageType::RESOURCE_VIEW,
    &cluster_resource_scheduler_.GetLocalResourceManager(),  // reporter
    this,                                                    // receiver
    report_resources_period_ms_);

auto gcs_channel = gcs_client_.GetGcsRpcClient().GetChannel();
ray_syncer_.Connect(kGCSNodeID.Binary(), gcs_channel);
```

定期收集本地资源使用情况并广播，收到远端资源更新时调用 `UpdateResourceUsage()`
更新 `ClusterResourceManager` 的节点资源视图。

#### 11.2.6 GCS 重启恢复

```cpp
// src/ray/raylet/node_manager.cc:1062-1078
void NodeManager::HandleNotifyGCSRestart(...) {
  gcs_client_.AsyncResubscribe();  // 重新建立所有 PubSub 订阅
  // 通知所有本地 Worker 和 Driver GCS 已重启
  for (const auto &worker : worker_pool_.GetAllRegisteredWorkers(true))
    worker->AsyncNotifyGCSRestart();
  for (const auto &driver : worker_pool_.GetAllRegisteredDrivers(true))
    driver->AsyncNotifyGCSRestart();
}
```

`AsyncResubscribe()` 重新订阅 Job、Actor、Node、Worker 四个数据通道。

#### 11.2.7 周期性存活检查

```cpp
// src/ray/raylet/node_manager.cc:461-492
gcs_client_.Nodes().AsyncCheckAlive({self_node_id_}, /* timeout_ms */ 30000,
    [this](const auto &status, const auto &alive_vec) {
      if (status.ok() && !alive_vec[0]) {
        RAY_LOG(FATAL) << "GCS consider this node to be dead.";
      }
    });
```

定期检查 GCS 是否仍认为自己存活，若 GCS 认为节点已死则 FATAL 退出。

#### 11.2.8 启动时获取配置

```cpp
// src/ray/raylet/main.cc:498-500
gcs_client->InternalKV().AsyncGetInternalConfig(
    [&](Status status, const std::optional<std::string> &stored_raylet_config) {
      RayConfig::instance().initialize(*stored_raylet_config);
      // 后续所有初始化都在此回调内完成
    });
```

Raylet 启动后第一个 GCS 交互是从 InternalKV 获取集群配置，**若 GCS 不可达则 Raylet 无法启动**。

---

### 11.3 Worker ↔ GCS 交互

CoreWorker 在 `core_worker_process.cc:275-277` 创建自己的 `GcsClient` 并连接 GCS。
该 `gcs_client_` 被传递给多个子系统，覆盖 Actor 管理、任务事件、对象恢复等场景。

#### 11.3.1 Worker 启动注册

```cpp
// src/ray/core_worker/core_worker.cc:735
gcs_client_->Workers().AsyncAdd(worker_data, nullptr);
```

Worker 启动时将自身元信息（worker type、PID、start time、launch time）注册到 GCS。

#### 11.3.2 Worker 订阅节点变化

```cpp
// src/ray/core_worker/core_worker.cc:766-772
gcs_client_->Nodes().AsyncSubscribeToNodeAddressAndLivenessChange(
    std::move(on_node_change), [this](const Status &) {
      gcs_client_node_cache_populated_ = true;
      gcs_client_node_cache_populated_cv_.notify_all();
    });
```

订阅 GCS 节点增减事件，`on_node_change` 回调在节点死亡时：
- 重置该节点上的对象引用
- 断开 `RayletClientPool` 和 `CoreWorkerClientPool` 中该节点的连接
- 更新 rate limiter

#### 11.3.3 Actor 创建全流程

Actor 创建是 Worker ↔ GCS 最复杂的交互，分为**注册**和**创建**两阶段：

**阶段一：Actor 注册**

```cpp
// src/ray/core_worker/actor_management/actor_creator.cc:24-30
Status ActorCreator::RegisterActor(const TaskSpecification &task_spec) const {
  const auto status = actor_client_.SyncRegisterActor(task_spec);
  if (status.IsTimedOut()) {
    return Status::TimedOut("GCS server is dead or high load.");
  }
  return status;
}
```

- 命名 Actor：同步调用 `SyncRegisterActor` → GCS `RegisterActor` RPC
- 非命名 Actor：异步调用 `AsyncRegisterActor`，回调成功后提交创建任务

**阶段二：Actor 创建（调度）**

```cpp
// src/ray/core_worker/task_submission/actor_task_submitter.cc:118-145
actor_creator_.AsyncCreateActor(task_spec,
    [this, actor_id, task_id](Status status, const rpc::CreateActorReply &reply) {
      if (status.ok()) {
        task_manager_.CompletePendingTask(task_id, push_task_reply,
                                           reply.actor_address(), ...);
      }
    });
```

GCS 收到 `CreateActor` RPC 后，由 `GcsActorManager` 调度 Actor 到合适节点，
通过 `GcsActorScheduler` 向目标 Raylet 发送 `RequestWorkerLease`，
Worker 启动后返回 Actor 地址。

**Actor 状态订阅**：

```cpp
// src/ray/core_worker/actor_management/actor_manager.cc:319-334
gcs_client_->Actors().AsyncSubscribe(actor_id, actor_notification_callback, ...);
```

Worker 创建 ActorHandle 后订阅该 Actor 的状态通知（死亡/重启）。

**其他 Actor 相关 GCS 交互**：

| 操作 | GCS API | 源码位置 |
|------|---------|---------|
| Kill Actor | `Actors().AsyncKillActor()` | `core_worker.cc:2759` |
| List Named Actors | `Actors().SyncListNamedActors()` | `core_worker.cc:2859` |
| Get Named Actor | `Actors().SyncGetByName()` | `actor_manager.cc:78` |
| Lineage 重启 | `Actors().AsyncRestartActorForLineageReconstruction()` | `actor_creator.cc:57` |
| Actor 脱离作用域 | `Actors().AsyncReportActorOutOfScope()` | `actor_creator.cc:70` |

#### 11.3.4 任务事件上报

CoreWorker 拥有**独立的 TaskEventBuffer**，使用自己的 `GcsClient` 实例和 IO 线程。

```cpp
// src/ray/core_worker/task_event_buffer.cc:519-520
periodical_runner_->RunFnPeriodically([this] { FlushEvents(false); },
                                      report_interval_ms, "flush_task_events");
```

```cpp
// src/ray/core_worker/task_event_buffer.cc:825-866
void TaskEventBufferImpl::SendTaskEventsToGCS(std::unique_ptr<rpc::TaskEventData> data) {
  auto &task_accessor = gcs_client_->Tasks();
  task_accessor.AsyncAddTaskEventData(std::move(data), on_complete);
}
```

定期（`task_events_report_interval_ms`）将任务状态事件和 profile 事件批量上报到 GCS。
若上一次 RPC 仍在进行中，跳过本次非强制 flush。

#### 11.3.5 对象位置查询与恢复

对象位置查询主要通过 **Worker ↔ Worker** 直接 RPC（`GetObjectLocationsOwner`），
但以下场景涉及 GCS：

**对象恢复中的节点地址解析**：

```cpp
// src/ray/core_worker/core_worker_process.cc:600-655
auto node_info = core_worker->gcs_client_->Nodes().GetNodeAddressAndLiveness(node_id);
if (!node_info) {
  // 缓存未命中，查 GCS
  core_worker->gcs_client_->Nodes().AsyncGetAllNodeAddressAndLiveness(
      [callback, object_id, locations](const Status &,
          const std::vector<rpc::GcsNodeAddressAndLiveness> &node_infos) {
        // 添加存活节点的地址
      }, -1, nodes_to_lookup);
}
```

`ObjectRecoveryManager` 恢复对象时，需要将对象副本所在的 NodeID 解析为物理地址，
优先查本地 GCS 缓存，未命中时异步查 GCS。

**添加对象位置前的节点存活检查**：

```cpp
// src/ray/core_worker/core_worker.cc:4065
if (gcs_client_->Nodes().IsNodeDead(node_id)) {
  return;  // 死亡节点的位置不添加
}
```

#### 11.3.6 Placement Group 操作

```cpp
// src/ray/core_worker/core_worker.cc:2508-2537
auto status = gcs_client_->PlacementGroups().SyncCreatePlacementGroup(spec);     // 创建
auto status = gcs_client_->PlacementGroups().SyncRemovePlacementGroup(pg_id);     // 删除
auto status = gcs_client_->PlacementGroups().SyncWaitUntilReady(pg_id, timeout);  // 等待就绪
```

三个同步阻塞 RPC，由用户代码通过 `ray.util.placement_group()` API 触发。

#### 11.3.7 RuntimeEnv 管理

RuntimeEnv 的上传由 `runtime_env_agent`（Raylet 端）完成，Worker 仅通过 GCS pin URI：

```cpp
// src/ray/gcs_rpc_client/accessor.cc:1079-1088
Status RuntimeEnvAccessor::PinRuntimeEnvUri(const std::string &uri,
                                            int expiration_s, int64_t timeout_ms) {
  rpc::PinRuntimeEnvURIRequest request;
  request.set_uri(uri);
  request.set_expiration_s(expiration_s);
  return client_impl_->GetGcsRpcClient().SyncPinRuntimeEnvURI(...);
}
```

仅对 `gcs://` 前缀的 URI 生效，GCS 持有引用 `expiration_s` 秒后自动减少引用计数。

#### 11.3.8 GCS 重启恢复

```cpp
// src/ray/core_worker/core_worker.cc:3733-3740
void CoreWorker::HandleRayletNotifyGCSRestart(...) {
  gcs_client_->AsyncResubscribe();
  send_reply_callback(Status::OK(), nullptr, nullptr);
}
```

Raylet 检测到 GCS 重启后，通过 `RayletNotifyGCSRestart` RPC 通知本节点所有 Worker。
Worker 重新订阅 Job、Actor、Node、Worker 四个数据通道。

#### 11.3.9 断连检测中的 GCS 查询

CoreWorker 和 RayletClient 的连接池在检测到远端不可达时，通过 GCS 确认节点存活状态：

```cpp
// src/ray/core_worker_rpc_client/core_worker_client_pool.cc:64-101
gcs_client->Nodes().AsyncGetAllNodeAddressAndLiveness(
    [...](const Status &, std::vector<rpc::GcsNodeAddressAndLiveness> &&nodes) {
      if (nodes.empty() || nodes[0].state() != rpc::GcsNodeInfo::ALIVE) {
        worker_client_pool->Disconnect(worker_id);  // 节点死亡 → 断连
        return;
      }
      check_worker_alive(nodes[0]);  // 节点存活 → 进一步查 Raylet 确认 Worker 是否死亡
    }, -1, {node_id});
```

两级检测链：GCS 查节点存活 → Raylet 查 Worker 存活。

---

### 11.4 Raylet ↔ Raylet 交互

Raylet 之间的交互模式分为**直接交互**（一个 Raylet gRPC 调用另一个 Raylet）和
**间接交互**（通过 CoreWorker 中转或通过 GCS 中介）两类。

#### 11.4.1 Spillback 调度（间接交互，最常见）

**核心机制**：Raylet **不直接** gRPC 调用远端 Raylet，而是在 `RequestWorkerLease` 回复中
填写 `retry_at_raylet_address`，让 CoreWorker 自行重试到远端 Raylet。

```cpp
// src/ray/raylet/scheduling/cluster_lease_manager.cc:380-415
void ClusterLeaseManager::ScheduleOnNode(const NodeID &spillback_to,
                                         const std::shared_ptr<internal::Work> &work) {
  if (spillback_to == self_node_id_) {
    local_lease_manager_.QueueAndScheduleLease(work);  // 本地调度
    return;
  }
  // 远端调度：在 reply 中填写远端 Raylet 地址
  auto node_info = get_node_info_(spillback_to);
  for (const auto &reply_callback : work->reply_callbacks_) {
    reply->mutable_retry_at_raylet_address()->set_ip_address(
        (*node_info).node_manager_address());
    reply->mutable_retry_at_raylet_address()->set_port(
        (*node_info).node_manager_port());
    reply->mutable_retry_at_raylet_address()->set_node_id(spillback_to.Binary());
    reply_callback.send_reply_callback_(Status::OK(), nullptr, nullptr);
  }
}
```

**完整链路**：

```
CoreWorker A → Raylet A: RequestWorkerLease
  → Raylet A 本地资源不足
  → ClusterLeaseManager::ScheduleAndGrantLeases()
  → GetBestSchedulableNode() → 找到 Raylet B
  → reply.retry_at_raylet_address = Raylet B 的地址
  → CoreWorker A 收到回复
  → CoreWorker A → Raylet B: RequestWorkerLease (重试)
  → Raylet B 分配 Worker → 返回 Worker 地址
  → CoreWorker A 直连远端 Worker B
```

#### 11.4.2 ObjectManager 跨节点对象传输（直接交互）

ObjectManager 运行在 Raylet 进程内，监听独立的 `--object-manager-port`，使用
**`ObjectManagerService`**（不同于 `NodeManagerService`）进行节点间对象传输。

```protobuf
// src/ray/protobuf/object_manager.proto:53-60
service ObjectManagerService {
  rpc Push(PushRequest) returns (PushReply);
  rpc Pull(PullRequest) returns (PullReply);
  rpc FreeObjects(FreeObjectsRequest) returns (FreeObjectsReply);
}
```

**Pull 请求（请求远端节点发送对象）**：

```cpp
// src/ray/object_manager/object_manager.cc:224-242
void ObjectManager::SendPullRequest(const ObjectID &object_id, const NodeID &client_id) {
  auto rpc_client = GetRpcClient(client_id);
  rpc::PullRequest pull_request;
  pull_request.set_object_id(object_id.Binary());
  pull_request.set_node_id(self_node_id_.Binary());
  rpc_client->Pull(pull_request, ...);
}
```

**Push（发送对象数据块到远端）**：

```cpp
// src/ray/object_manager/object_manager.cc:360-400
void ObjectManager::SendObjectChunk(...) {
  rpc::PushRequest push_request;
  push_request.set_object_id(object_id.Binary());
  push_request.set_data(std::move(optional_chunk.value()));
  rpc_client->Push(push_request, callback);
}
```

**FreeObjects（广播删除对象请求到所有节点）**：

```cpp
// src/ray/object_manager/object_manager.cc:458-510
void ObjectManager::FreeObjects(const std::vector<ObjectID> &object_ids, bool local_only) {
  if (!local_only) {
    for (const auto &[node_id, _] : node_info_map) {
      if (node_id == self_node_id_) continue;
      auto rpc_client = GetRpcClient(node_id);
      // 向所有远端 ObjectManager 发送 FreeObjects RPC
    }
  }
}
```

**完整对象传输流程**：

```
Raylet A 需要 Object X
  → PullManager → SendPullRequest(X, Node B)
  → ObjectManager A → Pull RPC → ObjectManager B

ObjectManager B 收到 Pull
  → HandlePull → Push(X, Node A)
  → SendObjectChunk (分块) → Push RPC → ObjectManager A
  → HandlePush → ReceiveObjectChunk → 写入 Plasma Store
```

#### 11.4.3 Placement Group 2PC（GCS 中介）

GCS `GcsPlacementGroupScheduler` 作为协调者，对多个 Raylet 执行两阶段提交：

```
GCS ─── Phase 1: PrepareBundleResources ──→ Raylet A (预留资源)
GCS ─── Phase 1: PrepareBundleResources ──→ Raylet B (预留资源)
  ↓ 全部成功
GCS ─── Phase 2: CommitBundleResources ──→ Raylet A (提交资源)
GCS ─── Phase 2: CommitBundleResources ──→ Raylet B (提交资源)
  ↓
GCS ─── 任一 Prepare 失败 → CancelResourceReserve ──→ 已 Prepare 的 Raylet
```

```cpp
// src/ray/gcs/gcs_placement_group_scheduler.cc:150-180
void GcsPlacementGroupScheduler::PrepareResources(...) {
  const auto raylet_client = GetRayletClientFromNode(node.value());
  raylet_client->PrepareBundleResources(bundles,
      [callback](const Status &status, const rpc::PrepareBundleResourcesReply &reply) {
        callback(reply.success() ? Status::OK() : Status::IOError("Failed"));
      });
}
```

Raylet 端不主动发起交互，仅响应 GCS 的 `PrepareBundleResources`、`CommitBundleResources`、
`CancelResourceReserve` RPC。

#### 11.4.4 节点 Drain 与对象迁移

当 Raylet 收到 `DrainRaylet` RPC 后，将本地 lease 重新调度到其他节点，
并迁移 pinned 对象：

```cpp
// src/ray/raylet/node_manager.cc:3610-3625
void NodeManager::MigratePinnedObjectsForDrain(std::function<void()> on_complete) {
  local_object_manager_.MigrateAllPinnedObjects(
      [this](const ObjectID &obj_id, const NodeID &target) {
        object_manager_.Push(obj_id, target);  // 通过 ObjectManagerService 传输
      }, std::move(on_complete));
}
```

Drain 过程中触发 Raylet↔Raylet 间接交互：
- 本地 lease 重新入队 `ClusterLeaseManager` → spillback 到远端 Raylet
- Pinned 对象通过 `ObjectManager.Push()` 迁移到其他节点

#### 11.4.5 全局内存信息聚合（唯一直接 Raylet→Raylet 调用）

这是 **Raylet 进程直接 gRPC 调用其他 Raylet 的 NodeManagerService 的唯一场景**：

```cpp
// src/ray/raylet/node_manager.cc:2838-2877
void NodeManager::HandleFormatGlobalMemoryInfo(...) {
  for (const auto &[node_id, address] : remote_node_manager_addresses_) {
    auto addr = rpc::RayletClientPool::GenerateRayletAddress(
        node_id, address.first, address.second);
    auto raylet_client = raylet_client_pool_.GetOrConnectByAddress(addr);
    raylet_client->GetNodeStats(stats_req,
        [store_reply](const Status &status, rpc::GetNodeStatsReply &&r) {
          store_reply(std::move(r));
        });
  }
  // 同时获取本地 stats
  HandleGetNodeStats(stats_req, local_reply.get(), ...);
}
```

Fan-out 到所有已知远端 Raylet，调用 `GetNodeStats` RPC 聚合全局内存信息。

#### 11.4.6 Mutable Object 推送

`PushMutableObject` RPC 通过 `NodeManagerService` 在 Raylet 之间推送可变对象数据块：

```cpp
// src/ray/raylet_rpc_client/raylet_client.cc:195-230
void RayletClient::PushMutableObject(...) {
  for (uint64_t i = 0; i < total_num_chunks; i++) {
    rpc::PushMutableObjectRequest request;
    request.set_writer_object_id(writer_object_id.Binary());
    request.set_data(static_cast<char *>(data) + offset, chunk_size);
    INVOKE_RPC_CALL(NodeManagerService, PushMutableObject, request, callback, grpc_client_);
  }
}
```

由 `MutableObjectProvider` 创建 `RayletClient` 连接到远端 Raylet 并分块推送。

#### 11.4.7 对象复制 (ReplicateObject)

GCS Autoscaler 发送 `ReplicateObject` RPC 到源 Raylet，源 Raylet 再通过
`ObjectManager.Push()` 将对象复制到目标稳定节点：

```cpp
// src/ray/raylet/node_manager.cc:3621-3700
void NodeManager::HandleReplicateObject(...) {
  NodeID target_node = SelectStableNode();
  object_manager_.Push(object_id, target_node);  // ObjectManagerService Push
}
```

---

### 11.5 Worker ↔ Worker 交互补充

第 6 节已详细描述了 CoreWorker 间的 `PushTask` 直连通信，此处补充其他 Worker↔Worker RPC。

#### 11.5.1 对象位置 Owner 查询

对象位置信息由对象 Owner Worker 持有（而非 GCS），其他 Worker 通过 gRPC 直接查询 Owner：

```cpp
// src/ray/core_worker/core_worker.cc:1836-1900
auto client = core_worker_client_pool_->GetOrConnect(owner_address);
rpc::GetObjectLocationsOwnerRequest request;
request.set_intended_worker_id(owner_address.worker_id());
request.add_object_ids(object_id.Binary());
client->GetObjectLocationsOwner(request, ...);
```

Owner Worker 在 `HandleGetObjectLocationsOwner` 中通过 `ReferenceCounter::FillObjectInformation()`
填充对象位置信息返回。

#### 11.5.2 对象位置批量更新

```cpp
// CoreWorkerService RPC: UpdateObjectLocationBatch (可重试 RPC)
// 用于批量更新对象在多个节点上的位置信息
```

#### 11.5.3 其他 CoreWorkerService RPC 补充

| RPC 方法 | 用途 | 触发场景 |
|---------|------|---------|
| `PlasmaObjectReady` | 通知 Plasma 对象就绪 | 对象 spill/restore 后通知 borrower |
| `AssignObjectOwner` | 分配对象所有权 | 嵌套任务创建对象引用时 |
| `LocalGC` | 触发本地 GC | 内存压力下集群级 GC 协调 |
| `DeleteObjects` | 删除对象 | 用户调用 `ray.delete()` |
| `SpillObjects` | 溢出对象到外部存储 | Plasma 内存不足时 |
| `RestoreSpilledObjects` | 从外部存储恢复对象 | Worker 本地需要远端 spill 的对象 |
| `DeleteSpilledObjects` | 删除已溢出对象 | 对象被全局删除时清理外部存储 |
| `PubsubCommandBatch` | 对象位置 pubsub 批量命令 | 订阅/取消订阅对象位置变化 |

---

### 11.6 NodeManagerService 完整 RPC 列表

`NodeManagerService` 是 Raylet 的核心 gRPC 服务，定义在 `src/ray/protobuf/node_manager.proto`，
共 31 个 RPC 方法。以下按调用方分类：

| # | RPC 方法 | 调用方 | 用途 | 可重试 |
|---|---------|--------|------|--------|
| 1 | `RequestWorkerLease` | CoreWorker, GCS Actor Scheduler | 请求 Worker 租约 | ✅ |
| 2 | `ReturnWorkerLease` | CoreWorker | 归还 Worker 租约 | ❌ |
| 3 | `CancelWorkerLease` | CoreWorker, GCS Actor Scheduler | 取消租约请求 | ✅ |
| 4 | `PrestartWorkers` | CoreWorker | 预启动 Worker | ❌ |
| 5 | `ReportWorkerBacklog` | CoreWorker | 报告任务积压 | ❌ |
| 6 | `PinObjectIDs` | CoreWorker ObjectRecoveryManager | Pin 对象防止回收 | ✅ |
| 7 | `PrepareBundleResources` | GCS PlacementGroup Scheduler | 2PC Phase 1 预留资源 | ❌ |
| 8 | `CommitBundleResources` | GCS PlacementGroup Scheduler | 2PC Phase 2 提交资源 | ❌ |
| 9 | `CancelResourceReserve` | GCS PlacementGroup Scheduler | 取消资源预留 | ❌ |
| 10 | `ReleaseUnusedBundles` | GCS PlacementGroup Scheduler | 释放无主 bundle | ✅ |
| 11 | `ReleaseUnusedActorWorkers` | GCS Actor Manager | 释放泄漏 Actor Worker | ❌ |
| 12 | `NotifyGCSRestart` | GCS | 通知 GCS 已重启 | ✅ |
| 13 | `ShutdownRaylet` | GCS Autoscaler | 关闭 Raylet | ✅ |
| 14 | `DrainRaylet` | GCS Autoscaler | 排空 Raylet | ✅ |
| 15 | `GetResourceLoad` | GCS Autoscaler | 获取资源负载 | ❌ |
| 16 | `CancelLeasesWithResourceShapes` | GCS Autoscaler | 取消不可调度 lease | ❌ |
| 17 | `ReplicateObject` | GCS Autoscaler | 复制对象到稳定节点 | ❌ |
| 18 | `GetNodeStats` | Raylet (FormatGlobalMemoryInfo) | 获取节点统计 | ❌ |
| 19 | `GlobalGC` | CoreWorker | 触发全局 GC | ❌ |
| 20 | `GetSystemConfig` | CoreWorker | 获取系统配置 | ❌ |
| 21 | `GetWorkerFailureCause` | CoreWorker | 获取 Worker 失败原因 | ❌ |
| 22 | `GetWorkerPIDs` | CoreWorker, State API | 获取 Worker PID | ✅ |
| 23 | `GetAgentPIDs` | CoreWorker | 获取 Agent PID | ✅ |
| 24 | `KillLocalActor` | GCS Actor Manager | 杀死本地 Actor | ✅ |
| 25 | `CancelLocalTask` | GCS Actor Manager, CoreWorker | 取消本地任务 | ✅ |
| 26 | `IsLocalWorkerDead` | CoreWorker (断连检测) | 检查 Worker 是否死亡 | ❌ |
| 27 | `PushMutableObject` | CoreWorker MutableObjectProvider | 推送可变对象数据块 | ❌ |
| 28 | `RegisterMutableObject` | CoreWorker MutableObjectProvider | 注册可变对象读取器 | ❌ |
| 29 | `FormatGlobalMemoryInfo` | 用户 API | 聚合全局内存信息 | ❌ |
| 30 | `ResizeLocalResourceInstances` | 内部 | 调整本地资源实例数 | ❌ |
| 31 | `GetObjectsInfo` | 内部 | 获取节点上所有对象信息 | ❌ |

---

### 11.7 交互矩阵总结表

| 通信对 | 协议 | 主要场景 | 直接/间接 | 关键源码 |
|--------|------|---------|-----------|---------|
| **Raylet → GCS** | gRPC + PubSub | 节点注册、Worker 失败上报、Job 完成/错误上报、配置获取、存活检查 | 直接 | `node_manager.cc:326,1467,1559,1508` |
| **GCS → Raylet** | gRPC RPC | PlacementGroup 2PC、GCS 重启通知、Drain/Shutdown | 直接（GCS 主动调用） | `node_manager.cc:1909,1926,1941,1062,2142,2205` |
| **Raylet ↔ GCS** | ray_syncer | 资源视图同步（经 GCS channel 中转） | 中介 | `node_manager.cc:342-382` |
| **Worker → GCS** | gRPC | Worker 注册、Actor 注册/创建/Kill、PG 创建/删除、Task 事件上报、RuntimeEnv pin | 直接 | `core_worker.cc:735,2759; actor_creator.cc:25,82; task_event_buffer.cc:866` |
| **GCS → Worker** | 无直接 RPC | GCS 通过 PubSub 推送 Actor 状态、节点变化 | PubSub | `actor_manager.cc:319; core_worker.cc:766` |
| **Worker ↔ GCS** | PubSub | 节点变化订阅、Actor 状态订阅 | 双向 | `core_worker.cc:766; actor_manager.cc:319` |
| **Raylet ↔ Raylet** | gRPC (spillback) | 跨节点调度（通过 reply 指引 CoreWorker 重试） | 间接（CoreWorker 中转） | `cluster_lease_manager.cc:380-415` |
| **Raylet ↔ Raylet** | ObjectManagerService | 对象 Push/Pull/FreeObjects 传输 | 直接 | `object_manager.cc:224,360,458` |
| **Raylet → Raylet** | NodeManagerService | `GetNodeStats`（FormatGlobalMemoryInfo 唯一直接调用） | 直接 | `node_manager.cc:2838-2877` |
| **Raylet → Raylet** | NodeManagerService | `PushMutableObject`（Mutable Object 推送） | 直接 | `raylet_client.cc:195-230` |
| **Raylet ↔ Raylet** | ray_syncer (via GCS) | 资源视图同步 | 中介（经 GCS channel） | `node_manager.cc:342-382,1083-1093` |
| **GCS → Raylet → Raylet** | 2PC RPC | PlacementGroup 创建（GCS 协调多 Raylet） | GCS 中介 | `gcs_placement_group_scheduler.cc:150-250` |
| **Worker ↔ Raylet** | IPC Socket | 注册、对象获取、阻塞通知等低延迟操作 | 直接 | `raylet_ipc_client.cc` |
| **Worker ↔ Raylet** | gRPC (NodeManagerService) | Worker 租约、资源管理 | 直接 | `raylet_client.cc` |
| **Worker ↔ Worker** | gRPC (CoreWorkerService) | 任务推送、对象位置查询、对象 spill/restore | 直接点对点 | `core_worker.cc:1836; normal_task_submitter.cc:103` |

---

### 12. gRPC 端口绑定失败 Crash 深度分析

#### 12.1 典型报错

```
(pid=2596, ip=10.48.76.18) E0607 09:28:00.406267389 2596 chttp2_server.cc:1063]
  UNKNOWN:No address added out of total 1 resolved for '0.0.0.0:10160'
  {created_time:"...", children:[
    UNKNOWN:Failed to add any wildcard listeners {children:[
      UNKNOWN:Unable to configure socket {fd:26, children:[
        UNKNOWN:Address already in use {errno:98, os_error:"Address already in use",
          syscall:"bind"}]}]}]}}

(pid=2596, ip=10.48.76.18) [2026-06-07 09:28:00,423 C 2596 2596] grpc_server.cc:133:
  Check failed: server_ Failed to start the grpc server.
  The specified port is 10160.
```

**错误链**：gRPC `bind()` 返回 `errno:98 (Address already in use)` → `BuildAndStart()` 返回 null → `RAY_CHECK(server_)` 失败 → 进程 crash。

#### 12.2 Crash 发生点：GrpcServer::Run()

```cpp
// src/ray/rpc/grpc_server.cc:40-80
void GrpcServer::Run() {
  uint32_t specified_port = port_;
  std::string server_address =
      BuildAddress((listen_to_localhost_only_ ? "127.0.0.1" : "0.0.0.0"), port_);

  grpc::ServerBuilder builder;
  // 禁止端口复用 — 多进程不能绑定同一端口
  builder.AddChannelArgument(GRPC_ARG_ALLOW_REUSEPORT, 0);

  // 关键：尝试绑定端口，port_=0 时 gRPC 自动选端口并写回
  builder.AddListeningPort(server_address,
                           grpc::InsecureServerCredentials(), &port_);

  // 注册服务...

  // BUILD AND START — 实际 bind 在这里发生
  server_ = builder.BuildAndStart();

  // 第133行：如果 BuildAndStart() 返回 null（端口被占），直接 crash
  RAY_CHECK(server_)
      << "Failed to start the grpc server. The specified port is " << specified_port
      << ". This means that Ray's core components will not be able to function "
      << "correctly. If the server startup error message is `Address already in use`, "
      << "it indicates the server fails to start because the port is already used by "
      << "other processes (such as --node-manager-port, --object-manager-port, "
      << "--gcs-server-port, and ports between --min-worker-port, --max-worker-port). "
      << "Try running sudo lsof -i :" << specified_port
      << " to check if there are other processes listening to the port.";
  RAY_CHECK(port_ > 0);
}
```

`BuildAndStart()` 返回 null 的两种情况：
- **指定端口被占用**：`port_ != 0` 且该端口已被其他进程绑定
- **零端口全部耗尽**：`port_ = 0` 时 gRPC 让 OS 选端口，但所有临时端口都被占用（极少见）

#### 12.3 CoreWorker 初始化链路：从端口分配到 crash

```cpp
// src/ray/core_worker/core_worker_process.cc — CreateCoreWorker()

std::shared_ptr<CoreWorker> CoreWorkerProcessImpl::CreateCoreWorker(
    CoreWorkerOptions options, const WorkerID &worker_id) {

  // Step 1: 通过 IPC 向 Raylet 注册，获取分配的端口
  int assigned_port = 0;
  Status status = raylet_ipc_client->RegisterClient(
      worker_id, options.worker_type, ..., &assigned_port, ...);
  RAY_CHECK_GE(assigned_port, 0);

  // Step 2: 用 Raylet 分配的端口创建 GrpcServer
  auto core_worker_server = std::make_unique<rpc::GrpcServer>(
      WorkerTypeString(options.worker_type),
      assigned_port,  // 使用 Raylet 分配的端口
      options.node_ip_address == "127.0.0.1");

  // Step 3: 注册服务并启动
  core_worker_server->RegisterService(
      std::make_unique<rpc::CoreWorkerGrpcService>(
          io_service_, *service_handler_, /*max_active_rpcs_per_handler_=*/-1),
      false /* token_auth */);
  core_worker_server->Run();  // <-- 在这里调用，如果端口被占则 crash

  // Step 4: 设置 Worker 地址（使用实际绑定的端口）
  rpc::Address rpc_address;
  rpc_address.set_ip_address(options.node_ip_address);
  rpc_address.set_port(core_worker_server->GetPort());
}
```

#### 12.4 Raylet 端口分配逻辑：WorkerPool::GetNextFreePort()

```cpp
// src/ray/raylet/worker_pool.cc — 端口池初始化
WorkerPool::WorkerPool(... int min_worker_port, int max_worker_port,
                       const std::vector<int> &worker_ports, ...) {
  if (!worker_ports.empty()) {
    free_ports_ = std::make_unique<std::queue<int>>();
    for (int port : worker_ports) { free_ports_->push(port); }
  } else if (min_worker_port != 0) {
    if (max_worker_port == 0) { max_worker_port = 65535; }
    free_ports_ = std::make_unique<std::queue<int>>();
    for (int port = min_worker_port; port <= max_worker_port; port++) {
      free_ports_->push(port);
    }
  }
  // min_worker_port=0 且 worker_ports 空 → free_ports_ 为 null → 使用 port=0（随机）
}

// 分配端口：遍历队列，做 bind 测试检查可用性
Status WorkerPool::GetNextFreePort(int *port) {
  if (!free_ports_) {
    *port = 0;  // 无端口范围，让 gRPC 随机选
    return Status::OK();
  }
  int current_size = free_ports_->size();
  for (int i = 0; i < current_size; i++) {
    *port = free_ports_->front();
    free_ports_->pop();
    if (CheckPortFree(node_address_family_, *port)) {
      return Status::OK();  // 找到空闲端口
    }
    free_ports_->push(*port);  // 端口被占，放回队列尾部
  }
  return Status::Invalid("No available ports...");
}
```

#### 12.5 端口可用性检查：CheckPortFree() — 存在竞态条件

```cpp
// src/ray/util/network_util.cc:116-131
bool CheckPortFree(int family, int port) {
  io_context io_service;
  boost::system::error_code ec;
  // 仅仅做一次临时 bind 测试
  if (family == AF_INET6) {
    socket = std::make_unique<tcp::socket>(io_service, tcp::v6());
    socket->bind(tcp::endpoint(tcp::v6(), port), ec);
  } else {
    socket = std::make_unique<tcp::socket>(io_service, tcp::v4());
    socket->bind(tcp::endpoint(tcp::v4(), port), ec);
  }
  socket->close();  // 立即释放！不持有端口
  return !ec.failed();
}
```

**关键竞态**：`CheckPortFree()` 只是临时 bind 测试后立即释放端口，并不"占用"端口。
在 Raylet 检查端口可用 → 将端口分配给 Worker → Worker 实际 gRPC bind 之间，
**另一个进程可以抢占该端口**。

Ray 在 `worker_pool.h` 中明确注释了这个已知问题：

> *"Ray does not 'reserve' these ports from being used by other services.
> There is a race condition where another service binds to the port sometime
> after this function returns and before the Worker/Driver uses the port."*

#### 12.6 Worker/Driver 注册时的端口分配触发点

```cpp
// src/ray/raylet/worker_pool.cc
Status WorkerPool::RegisterWorker(
    const std::shared_ptr<WorkerInterface> &worker, pid_t pid, ...) {
  int port = 0;
  Status status = GetNextFreePort(&port);  // 从队列分配端口
  if (!status.ok()) { return status; }
  worker->SetAssignedPort(port);           // 设置给 Worker
  send_reply_callback(Status::OK(), port); // 通过 IPC 返回给 Worker
}

Status WorkerPool::RegisterDriver(...) {
  int port;
  Status status = GetNextFreePort(&port);  // 同样流程
  driver->SetAssignedPort(port);
  ...
}
```

#### 12.7 各组件端口来源 — 端口冲突的可能来源

| 组件 | 端口参数 | 绑定代码位置 |
|------|----------|-------------|
| **NodeManager (Raylet)** | `--node-manager-port` | `node_manager.cc`: `node_manager_server_("NodeManager", config.node_manager_port, ...)` → `.Run()` |
| **ObjectManager** | `--object-manager-port` | `object_manager.cc`: `object_manager_server_("ObjectManager", config_.object_manager_port, ...)` → `.Run()` |
| **GCS Server** | `--gcs-server-port` | `gcs_server.cc`: `rpc_server_("GcsServer", config.grpc_server_port, ...)` → `.Run()` |
| **CoreWorker** | Raylet 从 `free_ports_` 队列分配 | `core_worker_process.cc`: `GrpcServer(assigned_port, ...)` → `.Run()` |

`raylet/main.cc` 中的 flag 定义：

```cpp
DEFINE_int32(object_manager_port, -1, "The port of object manager.");
DEFINE_int32(node_manager_port, -1, "The port of node manager.");
DEFINE_int32(min_worker_port, 0, "The lowest port that workers' gRPC servers will bind on.");
DEFINE_int32(max_worker_port, 0, "The highest port that workers' gRPC servers will bind on.");
DEFINE_string(worker_port_list, "", "An explicit list of ports that workers' gRPC servers will bind on.");
```

#### 12.8 完整的 Crash 链路总结

```
1. Raylet 初始化 free_ports_ 队列 (min_worker_port ~ max_worker_port)
2. Worker 通过 IPC 连接 Raylet → RegisterClient/RegisterWorker
3. Raylet 调用 GetNextFreePort() → CheckPortFree() 做临时 bind 测试
4. CheckPortFree() 返回 true（端口暂时空闲）→ 立即释放端口
5. Raylet 将 assigned_port=10160 通过 IPC 返回给 Worker
6. Worker 创建 GrpcServer(assigned_port=10160)
7. Worker 调用 GrpcServer::Run()
8. gRPC builder.AddListeningPort("0.0.0.0:10160") → builder.BuildAndStart()
9. 此时 10160 已被其他进程占用 → bind 失败 → BuildAndStart() 返回 null
10. RAY_CHECK(server_) 失败 → 进程 crash
```

**竞态窗口**：步骤 4（CheckPortFree 释放端口）到步骤 8（gRPC 实际 bind）之间存在时间差，
这是端口被抢占的根本原因。

#### 12.9 排查与解决方案

**排查方法**：
```bash
sudo lsof -i :10160       # 查看占用端口的进程
sudo netstat -tlnp | grep 10160
```

**解决方案**：
- 残留 Ray 进程：`ray stop` 后重新启动
- 端口范围冲突：调整 `--min-worker-port` / `--max-worker-port` 范围，确保各组件端口互不重叠
- 外部进程占用：更换 Ray 使用的端口或停止占用进程
- 端口重叠检查：确保 `--node-manager-port`、`--object-manager-port`、`--gcs-server-port`
  不在 `--min-worker-port` 到 `--max-worker-port` 范围内

#### 12.10 异常影响详细分析

端口绑定失败导致 Worker crash，影响范围远不止一个进程的死亡，会对 Ray 集群的
多个层面产生连锁反应。以下从 Worker 进程、Raylet、GCS、其他 Worker、端口池、
Driver 六个维度逐一详细分析，并附源码依据。

#### 12.10.1 对当前 Worker 进程的影响

**进程立即终止**（硬 crash，非优雅退出）：

- `RAY_CHECK(server_)` 失败等效于 `abort()`，进程直接终止，**没有任何清理机会**
- Worker 进程来不及通过 IPC 向 Raylet 发送 `DisconnectClientRequest`
- Worker 进程来不及执行以下关键清理操作：
  - 通知 Raylet 自己已断连（`RayletIpcClient::Disconnect`）
  - 通知 Raylet 自己的 gRPC 端口已失效
  - 释放 Plasma Store 中自己创建的对象
  - 向对象 Owner 报告引用释放（`ReferenceCounter::CleanupBorrowersOnRefRemoved`）
  - 向 GCS 报告 Actor 任务完成（如果是 Actor Worker）
- 如果该 Worker 是 **Driver**（用户主程序），整个用户任务直接中断，抛出连接断开错误

**StackTrace 中的关键函数调用链**（从报错的堆栈可以看到完整路径）：

```
CoreWorkerProcess::Initialize()           ← Python 调入口
  → CoreWorkerProcessImpl()               ← 进程初始化
    → CreateCoreWorker()                  ← 创建 CoreWorker 对象
      → GrpcServer::Run()                 ← 启动 gRPC 服务
        → RAY_CHECK(server_) 失败         ← 端口绑定失败 → crash
```

这意味着 crash 发生在 CoreWorker 初始化的**最早期阶段**，任何后续的注册、
通知、对象创建逻辑都**完全没有执行**。

#### 12.10.2 对 Raylet（本节点调度器）的影响

**Worker 泄漏与资源浪费**：

Worker crash 后，Raylet **无法立即感知**。Raylet 发现 Worker 断连的机制是
定时调用 `CheckForUnexpectedWorkerDisconnects()`，通过检测 Unix Domain Socket
连接是否断开来判断 Worker 是否存活：

```cpp
// src/ray/raylet/node_manager.cc:598-621
void NodeManager::CheckForUnexpectedWorkerDisconnects() {
  std::vector<std::shared_ptr<WorkerInterface>> all_workers =
      worker_pool_.GetAllRegisteredWorkers();
  // ... 收集所有 Worker 的 IPC 连接 ...
  std::vector<bool> disconnects = CheckForClientDisconnects(all_connections);
  for (size_t i = 0; i < disconnects.size(); i++) {
    if (disconnects[i]) {
      std::string msg = "Worker connection closed unexpectedly.";
      DestroyWorker(all_workers[i], rpc::WorkerExitType::SYSTEM_ERROR, msg);
    }
  }
}
```

该检测是**定时执行**的（而非即时），意味着 Worker crash 到 Raylet 发现之间存在
**时间窗口**。在此窗口内：

- Raylet 认为该 Worker 仍在占用资源（CPU、GPU、内存 slot），**不会释放给其他任务**
- Raylet 的 `free_ports_` 队列中，已分配给 crash Worker 的端口不会被回收
- Raylet 可能继续向该"已死"Worker 分配任务 lease，这些任务会超时失败

**端口回收延迟**：

Raylet 发现 Worker 断连后，调用 `DisconnectClient` → `DisconnectWorker`，
其中包含端口回收：

```cpp
// src/ray/raylet/worker_pool.cc:1553-1615
void WorkerPool::DisconnectWorker(const std::shared_ptr<WorkerInterface> &worker,
                                  rpc::WorkerExitType disconnect_type) {
  MarkPortAsFree(worker->AssignedPort());  // 回收端口
  // ... 从各数据结构中移除 Worker ...
}

// src/ray/raylet/worker_pool.cc:714-720
void WorkerPool::MarkPortAsFree(int port) {
  if (free_ports_) {
    RAY_CHECK(port != 0) << "";
    free_ports_->push(port);  // 端口放回队列
  }
}
```

但端口回收依赖 `DisconnectWorker` 的执行，而 `DisconnectWorker` 的触发又依赖
`CheckForUnexpectedWorkerDisconnects` 的定时检测。整个链路有**多级延迟**。

**资源释放与重新调度**：

`DisconnectClient` 执行后，Raylet 会释放 Worker 占用的资源并重新调度：

```cpp
// src/ray/raylet/node_manager.cc:1403-1614（DisconnectClient 核心逻辑）
void NodeManager::DisconnectClient(...) {
  // 清理 Worker 的 ray.get/ray.wait 等待
  lease_dependency_manager_.CancelGetRequest(worker->WorkerId());
  lease_dependency_manager_.CancelWaitRequest(worker->WorkerId());

  // 释放 Worker 占用的 lease
  if (leased_workers_.contains(worker->GetGrantedLeaseId())) {
    ReleaseWorker(worker->GetGrantedLeaseId());
  }

  // 向 GCS 报告 Worker 失败
  auto worker_failure_data_ptr = gcs::CreateWorkerFailureData(...);
  gcs_client_.Workers().AsyncReportWorkerFailure(worker_failure_data_ptr, nullptr);

  // Worker 断连 → 回收端口、释放资源、重新调度
  worker_pool_.DisconnectWorker(worker, disconnect_type);
  local_lease_manager_.ReleaseWorkerResources(worker);
  cluster_lease_manager_.ScheduleAndGrantLeases();  // 尝试调度新任务
}
```

**调度效率下降**：

在 Raylet 发现 Worker 断连之前：
- 已提交到该 Worker 的任务会超时失败 → TaskManager 重试 → 可能再次分配到"僵尸 Worker"
- 其他健康 Worker 可能因资源被"僵尸 Worker"占用而得不到足够的 lease
- 如果 `disconnect_type == SYSTEM_ERROR`，Raylet 会向 Driver 推送 `worker_died` 错误信息

#### 12.10.3 对 GCS（全局控制服务）的影响

**Raylet → GCS 的 Worker 失败通知链路**：

Raylet 发现 Worker 断连后，通过 `AsyncReportWorkerFailure` RPC 向 GCS 报告：

```cpp
// src/ray/raylet/node_manager.cc:1467
gcs_client_.Workers().AsyncReportWorkerFailure(worker_failure_data_ptr, nullptr);
```

GCS 的 `GcsWorkerManager::HandleReportWorkerFailure` 接收报告后，执行以下操作：

```cpp
// src/ray/gcs/gcs_worker_manager.cc:32-97
void GcsWorkerManager::HandleReportWorkerFailure(...) {
  // 将 Worker 标记为不存活
  worker_failure_data->set_is_alive(false);

  // 通知所有 Worker 死亡监听器
  for (auto &listener : worker_dead_listeners_) {
    listener(worker_failure_data);
  }

  // 持久化到 WorkerTable
  gcs_table_storage_.WorkerTable().Put(worker_id, *worker_failure_data, ...);

  // 发布 Worker 失败消息（通过 PubSub）
  gcs_publisher_.PublishWorkerFailure(worker_id, std::move(worker_failure));
}
```

GCS Server 在启动时注册了 Worker 死亡监听器，监听器中执行关键联动：

```cpp
// src/ray/gcs/gcs_server.cc:870-889
gcs_worker_manager_->AddWorkerDeadListener(
    [this](const std::shared_ptr<rpc::WorkerTableData> &worker_failure_data) {
      auto worker_id = WorkerID::FromBinary(worker_address.worker_id());
      worker_client_pool_.Disconnect(worker_id);       // 清理 GCS 端连接池

      // 通知 ActorManager 处理 Actor 死亡
      gcs_actor_manager_->OnWorkerDead(node_id, worker_id, worker_ip,
                                       worker_failure_data->exit_type(),
                                       worker_failure_data->exit_detail(),
                                       creation_task_exception);
      // 通知 PlacementGroup 和 TaskManager
      gcs_placement_group_scheduler_->HandleWaitingRemovedBundles();
      gcs_task_manager_->OnWorkerDead(worker_id, worker_failure_data);
    });
```

**Actor 状态不一致**（如果 crash Worker 是 Actor Worker）：

GCS 的 `GcsActorManager::OnWorkerDead` 处理 Worker 死亡后的 Actor 状态更新：

```cpp
// src/ray/gcs/actor/gcs_actor_manager.cc:1184-1280
void GcsActorManager::OnWorkerDead(const ray::NodeID &node_id,
                                   const ray::WorkerID &worker_id, ...) {
  // 判断是否需要重建（非用户主动退出时需要重建）
  bool need_reconstruct = disconnect_type != rpc::WorkerExitType::INTENDED_USER_EXIT &&
                          disconnect_type != rpc::WorkerExitType::USER_ERROR;

  // 销毁该 Worker 拥有的所有 Actor 的子 Actor
  const auto it = owners_.find(node_id);
  if (it != owners_.end() && it->second.count(worker_id)) {
    for (const auto &child_id : children_ids) {
      DestroyActor(child_id, GenOwnerDiedCause(..., "Owner's worker process has crashed."));
    }
  }

  // 销毁该 Worker 创建但尚未完成依赖解析的 Actor
  auto unresolved_actors = GetUnresolvedActorsByOwnerWorker(node_id, worker_id);
  for (auto &actor_id : unresolved_actors) {
    DestroyActor(actor_id, GenOwnerDiedCause(..., "Owner's worker process has crashed."));
  }

  // 对该 Worker 上直接创建的 Actor 执行重启或标记为 DEAD
  RestartActor(actor_id, /*need_reschedule=*/need_reconstruct, death_cause);
}
```

**关键影响**：
- **Owner crash → 子 Actor 级联销毁**：如果 crash Worker 是 Actor Owner，
  其所有子 Actor 会被 `DestroyActor` 级联销毁（标记为 `DEAD`，不可重启）
- **Actor 重启条件**：只有 `need_reconstruct=true`（即非用户主动退出）且
  `remaining_restarts > 0` 时，Actor 才会重启；否则标记为 `DEAD`

**Actor 重启的详细逻辑**：

```cpp
// src/ray/gcs/actor/gcs_actor_manager.cc:1445-1573
void GcsActorManager::RestartActor(const ActorID &actor_id,
                                   bool need_reschedule,
                                   const rpc::ActorDeathCause &death_cause) {
  int64_t max_restarts = mutable_actor_table_data->max_restarts();
  uint64_t num_restarts = mutable_actor_table_data->num_restarts();

  // 计算剩余重启次数
  int64_t remaining_restarts;
  if (!need_reschedule) { remaining_restarts = 0; }
  else if (max_restarts == -1) { remaining_restarts = -1; }  // 无限重启
  else { remaining_restarts = max_restarts - effective_restarts; }

  if (remaining_restarts != 0) {
    // 可以重启：更新状态为 RESTARTING，调度新 Worker 重新创建
    mutable_actor_table_data->set_num_restarts(new_num_restarts);
    actor->UpdateState(rpc::ActorTableData::RESTARTING);
    gcs_actor_scheduler_->Schedule(actor);  // 重新调度
  } else {
    // 不可重启：标记为 DEAD
    actor->UpdateState(rpc::ActorTableData::DEAD);
    // 持久化并发布 Actor 状态变更通知
    gcs_publisher_->PublishActor(actor_id, ...);
  }
}
```

**对象引用泄漏**（如果 crash Worker 是对象 Owner）：

crash Worker 作为对象 Owner 死亡后，其引用计数清理流程：

- 其他 Worker 持有的 borrower 引用无法向已死 Owner 报告释放
- Owner crash → `ReferenceCounter` 的 `DeleteReferenceInternal` 无法被触发
- 对象在 Plasma Store 中持续占用内存，直到以下机制触发清理：

```cpp
// src/ray/core_worker/reference_counter.cc:740-800
void ReferenceCounter::DeleteReferenceInternal(ReferenceTable::iterator it,
                                               std::vector<ObjectID> *deleted) {
  // 当引用计数为 0 且对象超出作用域时，触发清理
  if (it->second.OutOfScope(lineage_pinning_enabled_)) {
    OnObjectOutOfScopeOrFreed(it);  // 调用回调清理 Plasma Store 中的对象
    if (it->second.ShouldDelete(lineage_pinning_enabled_)) {
      EraseReference(it);  // 从引用表中删除
    }
  }
}
```

但 Owner crash 后，`DeleteReferenceInternal` 的触发依赖以下条件之一：
1. **borrower 主动释放引用**并通过 `CleanupBorrowersOnRefRemoved` 向 Owner 报告
   → 但 Owner 已死，无法接收报告
2. **GCS 的周期性扫描**发现 Owner 已死，触发引用计数强制清理
3. **borrower 的 `RetryableGrpcClient` 超时**触发断连检测链
   → 确认 Owner 死亡 → `Disconnect` → 本地清理引用

在清理完成之前，这些对象在 Plasma Store 中持续占用内存，可能导致内存压力。

#### 12.10.4 对其他 Worker 的影响

**任务提交方（Caller Worker）的影响**：

其他 Worker 已经通过 `CoreWorkerClientPool::GetOrConnect` 建立了到 crash Worker 的
gRPC 连接。Worker crash 后，这些连接会变为不可达状态。

断连检测链（详细代码见第 8 节的 `GetDefaultUnavailableTimeoutCallback`）：

```
gRPC RPC 超时（PushTask/PushActorTask 等）
  → RetryableGrpcClient 触发超时回调
  → GetDefaultUnavailableTimeoutCallback 执行
    → 查 GCS：节点是否存活？
      → 节点死亡 → pool->Disconnect(node_id)
      → 节点存活 → 通过 Raylet gRPC 检查 Worker 是否死亡
        → Worker 死亡 → pool->Disconnect(worker_id)
        → Worker 存活 → 继续重试
```

**Actor 任务的特殊处理**：

`ActorTaskSubmitter` 对 Actor Worker crash 的处理有专门的"等待死亡信息"机制：

```cpp
// src/ray/core_worker/task_submission/actor_task_submitter.cc:770
// PushActorTask 失败后，任务不会立即重试，而是放入 wait_for_death_info_tasks_ 队列
// 等待从 GCS 获取 Actor 死亡信息后再决定是否重试
```

这是因为 Actor 可能正在重启（状态为 `RESTARTING`），如果立即重试会失败。
等待 GCS 发布 Actor 状态变更通知（`DEAD` 或 `RESTARTING`）后再处理。

**依赖对象获取方的影响**：

如果 crash Worker 是某个 `ObjectRef` 的 Owner，其他 Worker 调用 `ray.get(ref)` 时：

- 需要通过 `GetObjectStatus` RPC 向 Owner 查询对象位置
- Owner 不可达 → RPC 超时 → 获取失败
- TaskManager 将依赖该对象的任务标记为等待状态
- 直到 GCS 更新 Owner 信息或 borrower 本地清理引用

#### 12.10.5 对 Raylet 端口池的长期影响

**端口池泄漏与耗尽的连锁效应**：

端口回收依赖 `DisconnectWorker` → `MarkPortAsFree`，而 `DisconnectWorker` 的触发
又依赖 `CheckForUnexpectedWorkerDisconnects` 的定时检测。整个链路：

```
Worker crash (端口 10160 绑定失败)
  → 进程立即终止，无 IPC 通知
  → Raylet 定时检测 CheckForUnexpectedWorkerDisconnects()
    → 发现 IPC 连接断开
    → DestroyWorker(worker, SYSTEM_ERROR)
      → DisconnectClient(worker->Connection(), graceful=false)
        → DisconnectWorker(worker, disconnect_type)
          → MarkPortAsFree(worker->AssignedPort())  ← 端口终于回收
        → ReleaseWorkerResources(worker)             ← 资源终于释放
        → ScheduleAndGrantLeases()                   ← 重新调度
```

如果端口范围较小（如 `--min-worker-port=10000 --max-worker-port=10100`，仅 101 个端口），
多个 Worker 同时 crash 可能耗尽端口池：

```
端口池耗尽
  → GetNextFreePort() 返回 Invalid("No available ports")
  → RegisterWorker/RegisterDriver 失败
  → Raylet 无法分配 Worker lease
  → 任务无法调度执行
  → 所有待提交任务排队等待
  → Driver 端 ray.get() / ray.wait() 超时
  → 用户程序挂起或抛出异常
```

更严重的是，如果端口绑定失败是**系统性问题**（如端口范围与其他 Ray 组件重叠），
会导致**连续 crash**：每个新 Worker 都会因为端口冲突而 crash，形成恶性循环。

#### 12.10.6 对 Driver（用户程序）的影响

**Driver crash**（端口绑定失败发生在 Driver 进程本身）：

```cpp
// src/ray/raylet/node_manager.cc:1558-1577
// DisconnectClient 中对 Driver 的处理
} else if (is_driver) {
  const auto job_id = worker->GetAssignedJobId();
  gcs_client_.Jobs().AsyncMarkFinished(job_id, nullptr);  // 标记 Job 已完成
  worker_pool_.DisconnectDriver(worker);                   // 断连并回收端口

  if (disconnect_type == rpc::WorkerExitType::SYSTEM_ERROR) {
    RAY_EVENT(ERROR, "RAY_DRIVER_FAILURE") << "Driver died...";
  }
}
```

- Driver 是用户 Python 程序的入口进程，Driver crash = **用户程序完全中断**
- GCS 将 Job 标记为 `FINISHED`，不可恢复
- Driver 不可重启（Ray 没有 Driver 重启机制）
- 所有由该 Driver 创建的 Actor、对象引用全部失效

**其他 Worker/Actor crash 对 Driver 的影响**：

- Driver 持有的 `ObjectRef` 可能指向已死 Worker 的对象 → `ray.get()` 抛出 `RayActorError`
- Driver 提交的 Actor 任务失败 → Actor 重启（如果 `max_restarts > 0`）→ 在途任务失败重试
- 如果 Actor 的 `max_restarts=0`（默认），Actor 标记为 `DEAD`，后续所有调用都失败
- Raylet 向 Driver 推送 `worker_died` 错误信息：

```cpp
// src/ray/raylet/node_manager.cc:1487-1507
// DisconnectClient 中推送错误到 Driver
if (disconnect_type == rpc::WorkerExitType::SYSTEM_ERROR) {
  std::string type = "worker_died";
  std::ostringstream error_message;
  error_message << "A worker died or was killed while executing a task "
                   "by an unexpected system error.";
  auto error_data = gcs::CreateErrorTableData(type, error_message_str, ...);
  gcs_client_.Errors().AsyncReportJobError(std::move(error_data));
}
```

#### 12.10.7 影响严重程度分级

| 场景 | 严重程度 | 影响范围 | 恢复可能性 |
|------|----------|----------|------------|
| Driver Worker crash | **致命** | 用户程序完全中断 | 不可恢复 |
| Actor Worker crash（max_restarts=0） | **严重** | 该 Actor 所有后续调用失败，子 Actor 级联销毁 | 不可恢复 |
| Actor Worker crash（max_restarts>0） | **中等** | 在途任务失败重试，短暂服务中断 | 可恢复（重启后恢复） |
| 普通 Worker crash | **中等** | 在途任务失败重试 | Raylet 回收资源后恢复 |
| 端口池耗尽导致连续 crash | **严重** | 节点完全不可用，所有新任务无法调度 | 需手动干预（ray stop + 重启） |
| Owner Worker crash（对象引用泄漏） | **中等** | Plasma Store 内存泄漏，borrower 引用无法释放 | GCS 周期清理后恢复 |

#### 12.10.8 恢复机制与延迟

Ray 有以下机制来缓解端口绑定失败 crash 的影响，但每个机制都有延迟：

1. **IPC 断连检测**（延迟：秒级）：
   Raylet 通过 `CheckForUnexpectedWorkerDisconnects()` 定时检测 IPC 连接断开，
   发现后调用 `DestroyWorker` → `DisconnectClient` → `DisconnectWorker` → `MarkPortAsFree`

2. **Actor 重启**（延迟：秒级到分钟级）：
   `GcsActorManager::OnWorkerDead` → `RestartActor`，如果 `remaining_restarts > 0`，
   Actor 状态更新为 `RESTARTING` → `gcs_actor_scheduler_->Schedule(actor)` 重新调度。
   重启延迟包括：调度新 Worker、Worker 启动、Actor 创建任务执行

3. **任务重试**（延迟：秒级）：
   TaskManager 对失败任务自动重试（最多 `max_retries` 次），通过
   `CoreWorker::InternalHeartbeat` 定时检查重试队列：
   ```cpp
   // src/ray/core_worker/core_worker.cc:793-835
   void CoreWorker::InternalHeartbeat() {
     // 从 to_resubmit_ 优先队列中取出到期任务并重试
     while (!to_resubmit_.empty() && current_time > to_resubmit_.top().execution_time_ms) {
       tasks_to_resubmit.emplace_back(to_resubmit_.top());
       to_resubmit_.pop();
     }
   }
   ```

4. **引用计数清理**（延迟：分钟级）：
   Owner crash 后，borrower 的 `RetryableGrpcClient` 超时触发断连检测链，
   确认 Owner 死亡后 `Disconnect` → 本地引用清理。但超时时间较长（默认数十秒），
   且需要多级检查（GCS → Raylet → Worker）。

5. **Worker 重建**（延迟：秒级）：
   Raylet 会尝试创建新 Worker 替代 crash 的 Worker（如果端口池未耗尽）。

**关键局限**：
- Driver crash **不可恢复** — Ray 没有 Driver 重启机制
- 端口池耗尽 **需要手动干预** — 必须执行 `ray stop` 后重新启动
- Owner crash 导致的子 Actor 级联销毁 **不可恢复** — `DestroyActor` 会将子 Actor
  标记为 `DEAD` 并从 `registered_actors_` 中删除
3. **任务重试**：TaskManager 对失败任务自动重试（最多 `max_retries` 次）
---

## 参考源码文件索引

| 文件 | 内容 |
|------|------|
| `src/ray/core_worker/task_manager.h` | TaskManager 类定义 |
| `src/ray/core_worker/core_worker.h` | CoreWorker 类定义 |
| `src/ray/core_worker/core_worker.cc` | CoreWorker 实现 |
| `src/ray/core_worker/core_worker_process.cc` | CoreWorker 进程启动流程 |
| `src/ray/core_worker/grpc_service.h` | CoreWorker gRPC 服务定义 |
| `src/ray/core_worker_rpc_client/core_worker_client.h` | CoreWorkerClient 定义 |
| `src/ray/core_worker_rpc_client/core_worker_client_pool.h` | CoreWorkerClientPool 定义 |
| `src/ray/core_worker_rpc_client/core_worker_client_pool.cc` | CoreWorkerClientPool 实现 |
| `src/ray/raylet_rpc_client/raylet_client.h` | RayletClient 定义 |
| `src/ray/raylet_rpc_client/raylet_client.cc` | RayletClient 实现 |
| `src/ray/raylet_rpc_client/raylet_client_pool.h` | RayletClientPool 定义 |
| `src/ray/raylet_rpc_client/raylet_client_pool.cc` | RayletClientPool 实现 |
| `src/ray/raylet_ipc_client/raylet_ipc_client.h` | RayletIpcClient 定义 |
| `src/ray/raylet_ipc_client/raylet_ipc_client.cc` | RayletIpcClient 实现（IPC 通信） |
| `src/ray/raylet_ipc_client/client_connection.h` | ServerConnection / ClientConnection 定义 |
| `src/ray/raylet_ipc_client/client_connection.cc` | 连接读写实现 |
| `src/ray/raylet/node_manager.h` | NodeManager 类定义 |
| `src/ray/raylet/node_manager.cc` | NodeManager 实现（消息分发） |
| `src/ray/protobuf/node_manager.proto` | NodeManagerService gRPC 定义 |
| `src/ray/protobuf/core_worker.proto` | CoreWorkerService gRPC 定义 |
| `src/ray/rpc/grpc_server.h` | GrpcServer 通用定义 |
| `src/ray/rpc/grpc_server.cc` | GrpcServer::Run() 实现，端口绑定与 RAY_CHECK(server_) |
| `src/ray/raylet/worker_pool.cc` | WorkerPool 端口池初始化、GetNextFreePort、RegisterWorker |
| `src/ray/raylet/worker_pool.h` | WorkerPool 类定义，free_ports_ 队列 |
| `src/ray/util/network_util.cc` | CheckPortFree 端口可用性检查（临时 bind 测试） |
| `src/ray/util/network_util.h` | CheckPortFree 声明 |
| `src/ray/raylet/main.cc` | --node-manager-port, --object-manager-port, --min/max-worker-port flags |
| `src/ray/core_worker/task_submission/normal_task_submitter.cc` | NormalTaskSubmitter，PushNormalTask 直接通信 |
| `src/ray/core_worker/task_submission/actor_task_submitter.cc` | ActorTaskSubmitter，PushActorTask 直接通信 |
| `python/ray/_private/node.py` | Python 层 socket 文件创建 |

---

## 13. Raylet GCS 连接超时深度分析

### 13.1 典型报错

```
[2026-06-07 09:28:10,??? I 3051 3051] (ip=10.48.75.44)
  rpc_client.h:153: Failed to connect to GCS within 5 seconds

[2026-06-07 09:28:13,164 W 2672 2672] (ip=10.48.75.44)
  gcs_client.cc:205: Failed to get cluster ID from GCS server:
  TimedOut: Deadline Exceeded

Python traceback (default_worker.py:241):
  Node.__init__ → services.get_node → GlobalState._connect_and_get_accessor
    → RaySystemError("Failed to connect to GCS")
```

同一节点 ip=10.48.75.44 上的三个不同进程（pid=3051, 2672, Python Worker）
在连接 GCS Server 时超时。三条日志是同一故障（GCS 不可达）在不同层面的表现。

**重要修正：此报错不是 Raylet 运行阶段触发的，而是 Python Worker 初始化阶段。**
Raylet 启动时使用 `fetch_cluster_id_if_nil=false`（main.cc:338），不走 FetchClusterId 路径。
报错二来自 Python Worker 初始化时通过 `services.get_node()` → `GlobalStateAccessor` 连接 GCS，
由于 **`common.pxi:80` bug** 导致 `fetch_cluster_id_if_nil` 参数传错，意外触发 FetchClusterId。

### 13.2 三条超时路径

Ray 组件与 GCS Server 的通信超时有 **三条独立路径**，触发条件、超时阈值和后果各不相同：

| 路径 | 触发时机 | 超时阈值 | 后果 | 源码位置 |
|------|---------|---------|------|---------|
| FetchClusterId 同步超时 | Connect 阶段 | `gcs_rpc_server_connect_timeout_s`=5s | RAY_CHECK crash → 进程终止 | `gcs_client.cc:202`, `main.cc:338` |
| RetryableGrpcClient 连接超时 | 运行阶段 | `gcs_rpc_server_reconnect_timeout_s`=60s | `_Exit(EXIT_FAILURE)` → 进程强制退出 | `rpc_client.h:201-209`, `retryable_grpc_client.cc:83` |
| 请求级超时 | 运行阶段 | 每个请求的 `timeout_ms` | 回调返回 `TimedOut` Status | `retryable_grpc_client.cc:69` |

### 13.3 路径一：FetchClusterId 同步超时 → RAY_CHECK crash

**代码链路：**

```
Raylet main.cc:338
  → RAY_CHECK_OK(gcs_client->Connect(main_service))
    → GcsClient::Connect() (gcs_client.cc:121)
      → timeout_ms = gcs_rpc_server_connect_timeout_s * 1000 = 5000ms
      → FetchClusterId(timeout_ms) (gcs_client.cc:189)
        → SyncGetClusterId(request, &reply, timeout_ms) (gcs_client.cc:202)
          → 同步 gRPC 调用，deadline = 5s
          → 超时返回 Status::TimedOut("Deadline Exceeded")
        → RAY_LOG(WARNING) << "Failed to get cluster ID..." (gcs_client.cc:205)
        → Disconnect() + reset() (gcs_client.cc:206-207)
        → return s (非 OK)
      → RAY_CHECK_OK 失败 → 进程 crash
```

**注意：** Raylet 的启动参数设置了 `allow_cluster_id_nil=false` 和 `fetch_cluster_id_if_nil=false`
（`main.cc:334-335`），因此 Raylet **不会在 Connect 时调用 FetchClusterId**，cluster_id 是从启动参数传入的。
这条路径只在以下场景触发：
- Driver/CoreWorker 创建 GcsClient 时设置 `fetch_cluster_id_if_nil=true`
- 用户代码中手动调用 `gcs_client.Connect()` 且 cluster_id 为 nil

**对于本报错**，报错二来自 **Python Worker 初始化**（非 Raylet），路径一的触发场景正是：
- Python Worker 通过 `services.get_node()` → `GlobalStateAccessor` 创建 GcsClient
- Python 层设置 `fetch_cluster_id_if_nil=False`，但 **`common.pxi:80` bug 导致 C++ 层实际 `fetch=True`**
- cluster_id 为 Nil + fetch=True → `should_fetch_cluster_id_=true` → 必须调用 FetchClusterId → 5秒超时 → gcs_client.cc:205 报错

### 13.4 路径二：RetryableGrpcClient 连接超时 → 进程强制退出

**这是 Raylet 运行阶段与 GCS 断连时的主要崩溃路径。对于本报错，此路径是理论分析——
本报错是 Python Worker 初始化时的 5秒 FetchClusterId 超时，而非 Raylet 运行阶段的 60秒超时。**

**代码链路：**

```
任何 GCS RPC 请求失败（如网络抖动、GCS Server 不可达）
  → RetryableGrpcClient::Retry(request) (retryable_grpc_client.cc:175)
    → 加入 pending_requests_ 队列
    → server_unavailable_timeout_time_ = now + server_reconnect_timeout_base_seconds (60s)
    → SetupCheckTimer() 定时器启动

定时器周期触发 CheckChannelStatus(true) (retryable_grpc_client.cc:52)
  → 检查 gRPC channel 状态
  → 如果 channel 处于 GRPC_CHANNEL_TRANSIENT_FAILURE 或 GRPC_CHANNEL_CONNECTING:
    → consecutive_ready_idle_resend_count_ = 0
    → 如果 server_unavailable_timeout_time_ < now（已超过 60s）:
      → RAY_LOG(WARNING) << "GCS has been unavailable for more than 60s..."
      → server_unavailable_timeout_callback_() (rpc_client.h:201-209)
        → RAY_LOG(ERROR) << "Failed to connect to GCS within 60 seconds..."
        → std::_Exit(EXIT_FAILURE)  ← 进程立即强制退出！
      → attempt_number_++
      → server_unavailable_timeout_time_ 重算（指数退避）
```

**核心机制：**
1. GCS RPC client 内嵌 `RetryableGrpcClient`，使用指数退避重连
2. 首次失败后设置 `server_unavailable_timeout_time_ = now + 60s`
3. 定时器每隔 `check_channel_status_interval_milliseconds` 检查 channel 状态
4. 若 60s 内 channel 未恢复到 READY/IDLE 状态，触发 `server_unavailable_timeout_callback_`
5. 该回调直接调用 `std::_Exit(EXIT_FAILURE)` — **没有任何清理、没有任何异常捕获**

**指数退避行为：**
- 首次超时：60s 后触发回调 + `_Exit`
- 但回调中 `_Exit` 已退出进程，所以实际上只会触发一次
- 如果绕过了 `_Exit`（理论场景），下次超时阈值仍为 60s（base 和 max 都设为 60s）

### 13.5 路径三：请求级超时 → TimedOut 回调

**代码链路：**

```
RetryableGrpcClient::Retry(request) (retryable_grpc_client.cc:175)
  → 计算 timeout = now + request.timeout_ms
  → pending_requests_.emplace(timeout, request)

CheckChannelStatus() (retryable_grpc_client.cc:52)
  → while (!pending_requests_.empty())
    → if iter->first > now: break（未超时）
    → iter->second->Fail(Status::TimedOut("Timed out while waiting for GCS to become available."))
    → 从队列移除
```

**请求级超时在方法注册时设定：**

```cpp
// rpc_client.h:334-337
VOID_GCS_RPC_CLIENT_METHOD(NodeInfoGcsService,
                           GetClusterId,
                           node_info_grpc_client_,
                           /*method_timeout_ms*/ -1, )  ← 无限超时！
```

`GetClusterId` 的 `method_timeout_ms = -1`，意味着请求级超时为 **无限**。
因此对于 GetClusterId 请求，路径三不会触发 — 只有路径二（60s 总超时）会生效。

其他 GCS RPC 方法的 timeout_ms 也多为 -1，依赖 RetryableGrpcClient 的总超时机制。

### 13.6 RetryableGrpcClient 完整工作机制

RetryableGrpcClient 是 Ray 与 GCS Server 通信的核心容错组件，管理所有 GCS RPC 请求的重试和超时。

**创建过程（在 GcsRpcClient 构造时）：**

```cpp
// rpc_client.h:188-209
retryable_grpc_client_ = RetryableGrpcClient::Create(
    channel_,
    client_call_manager.GetMainService(),
    max_pending_requests_bytes = gcs_grpc_max_request_queued_max_bytes,
    check_channel_status_interval_milliseconds = grpc_client_check_connection_status_interval_milliseconds,
    server_reconnect_timeout_base_seconds = gcs_rpc_server_reconnect_timeout_s = 60,
    server_reconnect_timeout_max_seconds = gcs_rpc_server_reconnect_timeout_s = 60,
    server_unavailable_timeout_callback = []() {
        RAY_LOG(ERROR) << "Failed to connect to GCS within 60 seconds. "
                       << "GCS may have been killed. "
                       << "The program will terminate.";
        std::_Exit(EXIT_FAILURE);  ← 硬退出
    },
    server_name = "GCS");
```

**关键参数含义：**

| 参数 | 默认值 | 含义 |
|------|--------|------|
| `gcs_rpc_server_reconnect_timeout_s` | 60 | GCS 重连总超时（秒），超时后进程退出 |
| `gcs_rpc_server_connect_timeout_s` | 5 | Connect 阶段的 FetchClusterId 超时（秒） |
| `grpc_client_check_connection_status_interval_milliseconds` | 1000 | channel 状态检查间隔（毫秒） |
| `gcs_grpc_max_request_queued_max_bytes` | 默认值 | pending_requests 字节数上限，触发背压 |

**状态转换流程：**

```
正常状态（channel READY/IDLE）
  → RPC 请求直接发送，不进入 retry queue
  ↓ 请求失败（UNAVAILABLE 等网络错误）
  
进入重试状态
  → Retry() 加入 pending_requests_
  → 设置 server_unavailable_timeout_time_ = now + 60s
  → SetupCheckTimer() 启动定时检查
  ↓ 定时器每 1s 检查一次
  
Channel 状态检查
  → TRANSIENT_FAILURE/CONNECTING: 继续 wait
    → 超过 60s: _Exit(EXIT_FAILURE) 硬退出
  → READY/IDLE: 重发所有 pending 请求
    → 连续重发失败超过 grpc_max_ready_idle_resend_count: Fail 所有请求
  ↓ Channel 恢复
  
退出重试状态
  → server_unavailable_timeout_time_ = nullopt
  → 清空 pending_requests_
  → consecutive_ready_idle_resend_count_ = 0
```

**背压机制：**

```
Retry() 中如果 pending_requests_bytes_ + request_bytes > max_pending_requests_bytes_:
  → 阻塞当前线程（std::this_thread::sleep_for）
  → 每 1s 检查一次 channel 状态
  → 直到 channel 恢复或进程退出
  → 防止 GCS 不可达时内存无限增长
```

### 13.7 超时后的级联影响

**注意：本节描述的是路径二（RetryableGrpcClient 60秒超时）的理论级联影响。
对于本报错的实际场景，报错二是 Python Worker 初始化时 5秒 FetchClusterId 超时（common.pxi:80 bug 导致意外触发 FetchClusterId），
其影响是 Python Worker 无法初始化（见13.2节），而非 Raylet 进程退出。**

当 Raylet 因 GCS 超时而退出（路径二的 `_Exit(EXIT_FAILURE)`）时，级联影响如下：

**1. Raylet 进程硬退出**
- `std::_Exit(EXIT_FAILURE)` 不执行任何析构、不刷新缓冲区、不清理资源
- Raylet 的 IPC socket 文件残留（/tmp/ray/session_xxx/raylet.sock）
- Raylet 的 gRPC server 端口未正常释放

**2. 该节点所有 Worker 失去 Raylet 连接**
- Worker 通过 IPC socket 与 Raylet 通信，Raylet 退出后 socket 断连
- Worker 的 `CoreWorker::DisconnectRaylet()` 检测到断连
- Driver Worker 直接 crash；Task Worker 可能尝试重连但无法成功

**3. GCS 收到 Raylet 断连通知**
- GCS 通过 gRPC channel 断连检测到 Raylet 不可达
- `GcsNodeManager::HandleDisconnectNode()` 触发节点移除
- 该节点上所有 Actor 被标记为 DEAD → `OnWorkerDead()` → RestartActor 或 DestroyActor

**4. 其他节点 Raylet 受影响**
- 其他节点 Raylet 通过 pubsub 订阅 GCS 节点信息
- 收到节点移除通知后更新本地资源视图
- 将原本调度到故障节点的任务重新分配

**5. 端口资源残留**
- Raylet 管理的端口池（free_ports_）随进程退出而丢失
- 之前分配给 Worker 的端口可能仍在被已 crash 的 Worker 持有
- 新 Raylet 启动后重新初始化端口池，但可能遇到端口残留占用

**6. Actor 重启风暴**
- 如果故障节点上有大量 Actor，GCS 触发批量 Actor 重启
- 新节点上的 Worker 需要创建大量 Actor，资源压力剧增
- 可能引发新一轮端口竞态和 GCS 连接压力

### 13.8 两个报错的关联性分析

报错一（端口 10160 绑定 crash）发生在 ip=10.48.76.18（pid=2596），报错二（GCS 超时三条日志）发生在
ip=10.48.75.44（pid=3051/2672, Python Worker）。时间差为 10-13 秒（09:28:00 vs 09:28:10/13）。

**报错二是 Python Worker 初始化阶段触发的，根本原因是 `common.pxi:80` 参数传递 bug。**
Raylet 启动时使用 `fetch_cluster_id_if_nil=false`（main.cc:338），不走 FetchClusterId 路径。
但 Python Worker 的 `services.get_node()` → `GlobalStateAccessor` 中，Python 设置 `fetch=False`，
bug 导致 C++ 层实际 `fetch=True` → 意外触发 FetchClusterId → GCS 不可达时 5秒超时。

**可能的因果链：**

```
节点 A (10.48.76.18) CoreWorker 端口绑定 crash (09:28:00)
  → Worker 进程异常退出
  → Raylet 检测到 Worker 断连
  → 向 GCS 上报 Worker 死亡
  → GCS 触发 Actor 状态更新/重启

节点 B (10.48.75.44) Python Worker GCS 连接超时 (09:28:10/13)
  → 可能原因一：GCS Server 因处理节点 A 的 Worker 死亡事件而负载过高
    → 节点 B 的 GetClusterId 请求被延迟 → 超时
  → 可能原因二：集群网络抖动同时影响两个节点
    → 节点 A CoreWorker 在网络恢复后创建时遇到端口竞态 crash
    → 节点 B Python Worker 的 GCS 连接在网络中断期间超时
  → 可能原因三：GCS Server 本身重启/迁移
    → 所有组件的 GCS 连接断开
    → 节点 A CoreWorker 在 GCS 恢复期间创建，遇到端口竞态
    → 节点 B Python Worker GlobalStateAccessor 因 common.pxi:80 bug
      意外触发 FetchClusterId → GCS 不可达 → 5秒超时 crash
```

**时间线分析：**
- 端口 crash 在 09:28:00，GCS 超时在 09:28:10/13
- 10-13 秒的时间差与 GCS 重连超时机制不符（60s 超时阈值）
- 但与 FetchClusterId 的 5s deadline 超时吻合
- 更可能是 **网络层面的共因**：集群网络瞬时故障同时影响了两个节点的不同组件
- 端口 crash 是 CoreWorker 创建过程中网络恢复后的端口竞态
- GCS 超时是 Python Worker 初始化时 GlobalStateAccessor 因 common.pxi:80 bug 意外触发 FetchClusterId → 5秒超时

**独立事件判断：**
- 如果两个节点属于不同物理机，关联性较弱，更可能是独立事件
- 如果属于同一物理机/同一网络域，关联性较强，可能是共因

### 13.9 排查与解决方案

**排查步骤：**

1. **确认 GCS Server 状态**
   - 检查 GCS Server 日志（gcs_server.out）是否有重启/异常
   - 检查 GCS Server 进程是否存活（`ps aux | grep gcs_server`）
   - 检查 GCS Server 端口是否可达（`nc -zv <gcs_ip> <gcs_port>`）

2. **检查网络连通性**
   - 从故障节点 ping GCS Server IP
   - 检查是否有防火墙/安全组规则变更
   - 检查网络延迟和丢包率

3. **检查 GCS Server 负载**
   - GCS 日志中是否有 "Too many pending requests" 警告
   - CPU/内存使用率是否异常
   - gRPC 请求队列深度

4. **检查集群事件时间线**
   - `ray timeline` 或日志时间线，确认是否有集群级别的网络事件
   - 节点批量断连通常指向网络基础设施问题

**解决方案：**

| 方案 | 适用场景 | 配置调整 |
|------|---------|---------|
| 增大重连超时 | 网络恢复较慢的环境 | `gcs_rpc_server_reconnect_timeout_s=120` |
| 增大连接超时 | GCS 初始化较慢 | `gcs_rpc_server_connect_timeout_s=30` |
| GCS 高可用 | 生产环境 | 启用 GCS HA（`RAY_gcs_failover_mode=enabled`） |
| 网络监控 | 频发断连 | 部署网络质量监控，自动告警 |
| 调整检查间隔 | 需更快检测恢复 | `grpc_client_check_connection_status_interval_milliseconds=500` |
| **修复 common.pxi:80 bug** | **根本解决方案** | **将第5参数 `allow_cluster_id_nil` 改为 `fetch_cluster_id_if_nil`** |

**重要提示：**
- `gcs_rpc_server_reconnect_timeout_s` 的增大只是延迟 `_Exit`，并不能解决根本问题
- 如果 GCS Server 真的不可达，Raylet 持续等待只会延迟集群恢复
- GCS HA 是生产环境的关键保障，避免单点故障导致全集群 Raylet 退出
- **★★★ 报错二的根本原因是 `common.pxi:80` 参数传递 bug，修复该 bug 后 GlobalStateAccessor.Connect()
  不再意外触发 FetchClusterId，GCS 不可达时 Connect 仍可成功（`fetch=False` + `allow_nil=True`），从根本上消除 5秒超时风险**

**Bug 修复方案：**

```python
# common.pxi:80 — 修改前 (BUG)
self.inner.reset(
    new CGcsClientOptions(
        ip, port, c_cluster_id,
        allow_cluster_id_nil,
        allow_cluster_id_nil))    ← 第5参数传错了!

# common.pxi:80 — 修改后 (FIX)
self.inner.reset(
    new CGcsClientOptions(
        ip, port, c_cluster_id,
        allow_cluster_id_nil,
        fetch_cluster_id_if_nil))  ← 第5参数改为正确的 fetch_cluster_id_if_nil
```

修复后的行为变化：
- `_get_gcs_client_options()`: Python `allow_nil=True, fetch=False` → C++ `(Nil, True, False)` → `ShouldFetchClusterId(Nil, True, False)` → 返回 **false** → **不触发 FetchClusterId** → Connect 直接返回 OK
- Worker `_init_gcs_client()`: Python `allow_nil=False, fetch=False` → C++ `(FromHex, False, False)` → `ShouldFetchClusterId(FromHex, ...)` → 返回 false → 不变
- Head Node: Python `allow_nil=True, fetch=True` → C++ `(Nil, True, True)` → 正常触发 FetchClusterId → 不变

### 13.10 关键配置参数

| 参数 | 默认值 | 说明 | 源码 |
|------|--------|------|------|
| `gcs_rpc_server_reconnect_timeout_s` | 60 | GCS 重连总超时（秒），超时后 `_Exit(EXIT_FAILURE)` | `ray_config_def.h:418` |
| `gcs_rpc_server_connect_timeout_s` | 5 | Connect 阶段 FetchClusterId 超时（秒） | `ray_config_def.h:421` |
| `grpc_client_check_connection_status_interval_milliseconds` | 1000 | channel 状态检查间隔（毫秒） | `ray_config_def.h` |
| `gcs_grpc_max_request_queued_max_bytes` | 默认值 | pending_requests 字节数上限 | `ray_config_def.h` |
| `grpc_max_ready_idle_resend_count` | 默认值 | channel READY/IDLE 状态下最大重发次数 | `ray_config_def.h` |

---

### 参考源码索引（第 13 节新增）

| 源码文件 | 关键内容 |
|---------|---------|
| `src/ray/gcs_rpc_client/gcs_client.cc` | GcsClient::Connect(), FetchClusterId(), SyncGetClusterId 调用与超时处理 |
| `src/ray/gcs_rpc_client/rpc_client.h` | GcsRpcClient 构造 → WaitForConnected(5s) → RetryableGrpcClient 创建，server_unavailable_timeout_callback |
| `src/ray/rpc/retryable_grpc_client.cc` | Retry() 重试入队，CheckChannelStatus() 状态检查与超时触发 |
| `src/ray/rpc/retryable_grpc_client.h` | RetryableGrpcClient 类定义，RetryableGrpcRequest 定义 |
| `src/ray/common/ray_config_def.h` | gcs_rpc_server_reconnect_timeout_s=60, gcs_rpc_server_connect_timeout_s=5 |
| `src/ray/raylet/main.cc` | Raylet 启动，RAY_CHECK_OK(gcs_client->Connect())，fetch_cluster_id_if_nil=false |
| `src/ray/gcs_rpc_client/global_state_accessor.cc/.h` | GlobalStateAccessor::Connect() — Python → C++ 桥梁 |
| `python/ray/includes/global_state_accessor.pxd/.pyx` | Cython 桥梁 — Python → C++ GlobalStateAccessor |
| `python/ray/_private/state.py` | GlobalState._connect_and_get_accessor() |
| `python/ray/_private/node.py` | Node.__init__() → services.get_node() |
| `python/ray/_private/services.py` | get_node() → GlobalState.get_node() |
| `python/ray/_private/worker/default_worker.py` | main() → Node.__init__() |
| `src/ray/protobuf/gcs_service.proto` | GetClusterId RPC 定义 |
| `src/ray/gcs/gcs_node_manager.cc` | HandleGetClusterId 服务端实现 |
| `python/ray/includes/common.pxi` | ★★★ GcsClientOptions.create() 第80行参数传递 bug — 第5参数传了 allow_cluster_id_nil 而非 fetch_cluster_id_if_nil |

---

## 14. 两个报错的完整调用堆栈与因果关系

### 14.1 报错一：chttp2_server.cc:1063 (pid=2596, ip=10.48.76.18)

**原始报错：**
```
E0607 09:28:00.406267389    2596 chttp2_server.cc:1063
```

**chttp2_server.cc 是 gRPC C-core 内部文件**，不是 Ray 源码。`E` 级别日志 = ERROR。
行号 1063 对应 gRPC C-core 的 HTTP2 server 在 `grpc_chttp2_server_start` 中 TCP bind/listen 失败。

**逐行代码调用堆栈：**

```
━━━ 阶段 A: Raylet 端口分配（Raylet 进程，不同节点 ip=10.48.75.44）━━━

① node_manager.cc:1192
   void NodeManager::ProcessRegisterClientRequestMessage(client, message_data)
     → flatbuffers::GetRoot<RegisterClientRequest>(message_data)    // 解析 IPC 请求

② node_manager.cc:1194
   RAY_UNUSED(ProcessRegisterClientRequestMessageImpl(client, message))
     → 进入 Impl 函数

③ node_manager.cc:1198-1210
   Status ProcessRegisterClientRequestMessageImpl(client, message)
     → client->Register()
     → 解析 worker_id, pid, worker_type, ip_address
     → 创建 Worker 对象

④ node_manager.cc:1231
   send_reply_callback = [this, client](Status status, int assigned_port) {
       → 构建 IPC 回复的 lambda，包含 assigned_port 字段

⑤ node_manager.cc:1257
   return RegisterForNewWorker(worker, pid, std::move(send_reply_callback))

⑥ node_manager.cc:1261
   Status RegisterForNewWorker(worker, pid, send_reply_callback)
     → worker_pool_.RegisterWorker(worker, pid, send_reply_callback)

⑦ worker_pool.cc:783
   Status WorkerPool::RegisterWorker(worker, pid, send_reply_callback)
     → RAY_CHECK(worker)
     → state.worker_processes.find(worker_id)  // 查找已注册的 worker process
     → worker->SetProcess(Process::FromPid(pid))

⑧ worker_pool.cc:819-820
   int port = 0;
   Status status = GetNextFreePort(&port)           ← 端口分配入口!

⑨ worker_pool.cc:692-709
   Status WorkerPool::GetNextFreePort(int *port)
     → *port = free_ports_->front()                 // 取出 10160
     → free_ports_->pop()                           // 从队列移除

⑩ network_util.cc:116-133
   bool CheckPortFree(int family, int port)         ← 竞态窗口开始!
     → io_context io_service;
     → socket = make_unique<tcp::socket>(io_service, tcp::v4())
     → socket->bind(tcp::endpoint(tcp::v4(), 10160), ec)  // 临时 bind 测试
     → socket->close()                                     // ★ 立即释放! 竞态窗口!
     → return !ec.failed()                                 // 返回 true

⑪ worker_pool.cc:838-839
   worker->SetAssignedPort(port)                    // port = 10160
   send_reply_callback(Status::OK(), port)          // IPC 回复: assigned_port=10160

⑫ node_manager.cc:1231-1245 (send_reply_callback lambda)
   → CreateRegisterClientReply(fbb, status.ok(), ..., assigned_port=10160, ...)
   → client->WriteMessageAsync(RegisterClientReply, ...)  // 通过 IPC socket 回复

━━━ 阶段 B: Worker 进程接收端口并创建 gRPC server（pid=2596, ip=10.48.76.18）━━━

⑬ raylet_ipc_client.cc:91-103
   Status RayletIpcClient::RegisterClient(worker_id, worker_type, ...)
     → flatbuffers::FlatBufferBuilder fbb
     → protocol::CreateRegisterClientRequest(fbb, worker_type, worker_id, getpid(), ...)

⑭ raylet_ipc_client.cc:115-116
   Status status = AtomicRequestReply(
       MessageType::RegisterClientRequest, MessageType::RegisterClientReply, &reply, &fbb)
     → 同步 IPC 请求-回复，等待 Raylet 分配端口

⑮ raylet_ipc_client.cc:119-127
   auto reply_message = flatbuffers::GetRoot<RegisterClientReply>(reply.data())
     → *assigned_port = reply_message->port()       // assigned_port = 10160
     → return Status::OK()

⑯ core_worker_process.cc:206-215
   int assigned_port = 0;
   Status status = raylet_ipc_client->RegisterClient(..., &assigned_port, ...)
     → assigned_port = 10160                        ← 从 Raylet 得到的端口

⑰ core_worker_process.cc:222
   RAY_CHECK_GE(assigned_port, 0)                   // assigned_port=10160 ≥ 0 ✓

⑱ core_worker_process.cc:251-254
   auto core_worker_server = std::make_unique<rpc::GrpcServer>(
       WorkerTypeString(options.worker_type),        // "WORKER"
       assigned_port,                                // 10160 ← 从 Raylet 传入
       options.node_ip_address == "127.0.0.1")       // false
     → GrpcServer 构造: name_="WORKER", port_=10160, listen_to_localhost_only_=false

⑲ core_worker_process.cc:257-260
   core_worker_server->RegisterService(
       std::make_unique<rpc::CoreWorkerGrpcService>(io_service_, *service_handler_, -1),
       false)                                        // 注册 CoreWorkerService

⑳ core_worker_process.cc:261
   core_worker_server->Run()                         ← ★ crash 入口!

━━━ 阶段 C: GrpcServer::Run() 内部 — gRPC server 创建与端口绑定━━━

㉑ grpc_server.cc:65
   void GrpcServer::Run()
     → uint32_t specified_port = port_              // 10160

㉒ grpc_server.cc:66-67
   std::string server_address = BuildAddress(
       (listen_to_localhost_only_ ? "127.0.0.1" : "0.0.0.0"), port_)
     → server_address = "0.0.0.0:10160"

㉓ grpc_server.cc:68
   grpc::ServerBuilder builder

㉔ grpc_server.cc:72
   builder.AddChannelArgument(GRPC_ARG_ALLOW_REUSEPORT, 0)  ← ★ 禁用端口复用!
     // 如果启用，多个 worker 可能绑定同一端口
     // 但禁用后，端口被占用时 bind 会直接失败

㉕ grpc_server.cc:108-109
   builder.AddListeningPort(server_address, grpc::InsecureServerCredentials(), &port_)
     → 通知 gRPC C-core 在 "0.0.0.0:10160" 上监听

㉖ grpc_server.cc:156
   server_ = builder.BuildAndStart()                ← ★ 调用 gRPC C++/C-core

━━━ 阶段 D: gRPC C++ → C-core — 端口 bind━━━

㉗ grpc::ServerBuilder::BuildAndStart()
     → 对每个 AddListeningPort 的地址执行:
       → grpc_server_config_fetcher 通知
       → 创建 grpc_chttp2_server
       → 调用 grpc_chttp2_server_start()

㉘ chttp2_server.cc (gRPC C-core)
   grpc_chttp2_server_start()
     → 对 "0.0.0.0:10160" 执行 TCP bind()
     → bind() 失败! 端口 10160 已被其他进程占用  ← ★ 这是竞态的结果!
     → gpr_log(GPR_ERROR, ...) 输出到 stderr
     → chttp2_server.cc:1063                       ← ★ 这就是报错行号!

㉙ BuildAndStart() 返回 server_ = nullptr          // 因为 bind 失败，server 无法启动

━━━ 阶段 E: 回到 Ray — RAY_CHECK crash━━━

㉚ grpc_server.cc:157-163
   RAY_CHECK(server_)
       << "Failed to start the grpc server. The specified port is " << specified_port
       << "..."
     → server_ 为 nullptr
     → RAY_CHECK 失败
     → SIGABRT → 进程 crash!

㉛ ★ pid=2596 进程死亡，Worker ip=10.48.76.18 上的 CoreWorker crash
```

**竞态窗口总结：** 从⑩ CheckPortFree 的 `socket->close()` 到㉘ Worker gRPC server 的 `bind()` 之间，
端口 10160 处于 **无人守护** 状态。任何其他进程（如另一个 Worker、系统服务）都可以在此窗口内占用该端口。

### 14.2 报错二：GCS 连接超时三条报错（ip=10.48.75.44）

**三条原始报错（同一节点 ip=10.48.75.44，不同 pid）：**

```
[2026-06-07 09:28:10,??? I 3051 3051] rpc_client.h:153:
  Failed to connect to GCS within 5 seconds

[2026-06-07 09:28:13,164 W 2672 2672] gcs_client.cc:205:
  Failed to get cluster ID from GCS server: TimedOut: RPC error: Deadline Exceeded

Python traceback (default_worker.py:241):
  ray._private.worker.default_worker → Node.__init__ → services.get_node
    → GlobalState._connect_and_get_accessor → RaySystemError("Failed to connect to GCS")
```

**关键修正：报错二不是 Raylet 运行阶段触发的，而是 Python Worker 初始化阶段。根本原因是 `common.pxi:80` 参数传递 bug。**

三条报错来自 **同一节点 (ip=10.48.75.44) 上三个不同进程**：
- pid=3051: GcsRpcClient 构造函数中 `WaitForConnected` 5秒超时 → `rpc_client.h:153`
- pid=2672: `GcsClient::FetchClusterId` 5秒 deadline 超时 → `gcs_client.cc:205` ★ bug 导致意外触发!
- Python Worker: 初始化 `Node` 对象时通过 `GlobalStateAccessor` 连接 GCS 失败 → Python traceback

**三者是同一故障（GCS 不可达）的不同层面表现：**
- pid=3051 是 gRPC channel 层的连接超时（5秒）
- pid=2672 是 RPC 请求层的 deadline 超时（5秒 `SyncGetClusterId`）
- Python traceback 是应用层的异常传播（C++ → Cython → Python）

**Raylet 启动时使用 `fetch_cluster_id_if_nil=false`（main.cc:338），不走 FetchClusterId 路径，
所以 Raylet 本身不会产生 gcs_client.cc:205 报错。报错二来自 Python Worker 初始化，
根本原因是 `common.pxi:80` bug 导致 `GlobalStateAccessor` 意外触发 FetchClusterId。**

**逐行代码调用堆栈：**

```
━━━ 阶段 A: Python Worker/Driver 初始化入口━━━

① python/ray/_private/worker/default_worker.py:241
   def main():
     → node = ray._private.node.Node(...)              ← ★ Node 初始化入口!

② python/ray/_private/node.py:385
   class Node.__init__(head_node_ip, ...):
     → ray._private.services.get_node(head_node_ip, ...) ← 获取节点信息

③ python/ray/_private/services.py
   def get_node(ip_address, ...):
     → global_state = ray._private.state.GlobalState()
     → return global_state.get_node(ip_address)          ← 连接 GCS!

④ python/ray/_private/state.py
   class GlobalState:
     def get_node(ip_address):
       → self._connect_and_get_accessor()               ← ★ 连接 GCS 核心调用!

⑤ python/ray/_private/state.py
   def _connect_and_get_accessor(self):
     → GlobalStateAccessor.connect(address)              ← 跨 Cython 桥梁!

━━━ 阶段 B: Cython 桥梁 — Python → C++━━━

⑥ python/ray/includes/global_state_accessor.pxd
   cdef class GlobalStateAccessor:
     cdef ray_c_GlobalStateAccessor *inner               ← C++ 对象指针

⑦ python/ray/includes/global_state_accessor.pyx
   def connect(self):
     → self.inner.Connect()                              ← 调用 C++ GlobalStateAccessor::Connect()

⑧ src/ray/gcs_rpc_client/global_state_accessor.cc:50-56
   Status GlobalStateAccessor::Connect()
     → gcs_client_ = std::make_unique<GcsClient>(options_)
     → options_.cluster_id = ClusterID::Nil()           ← ★ cluster_id 为 nil
     → options_.should_fetch_cluster_id_ = true          ← ★ bug 导致! Python 设 fetch=False,
                                                           但 common.pxi:80 传错参数, C++ 实际 fetch=True
     → return gcs_client_->Connect(io_service)           ← 进入 C++ GcsClient::Connect

━━━ 阶段 C: C++ GcsClient::Connect — FetchClusterId━━━

⑨ gcs_client.cc:121
   Status GcsClient::Connect(io_service, timeout_ms=-1)
     → timeout_ms = gcs_rpc_server_connect_timeout_s * 1000 = 5000ms

⑩ gcs_client.cc:123-131
   → client_call_manager_ = make_unique<ClientCallManager>(io_service, ...)
   → gcs_rpc_client = make_shared<GcsRpcClient>(address, port, *client_call_manager_)
     → ★ GcsRpcClient 构造函数内部调用 WaitForConnected(5000ms)

⑪ rpc_client.h:188-209 (GcsRpcClient 构造函数)
   retryable_grpc_client_ = RetryableGrpcClient::Create(channel_, ...)
   → WaitForConnected(gcs_rpc_server_connect_timeout_s * 1000 = 5000ms)

⑫ rpc_client.h:143-153
   void WaitForConnected(int64_t timeout_ms)
     → deadline = now + timeout_ms (5秒)
     → while (channel_->GetState(false) != GRPC_CHANNEL_READY)
         → wait_for_state_change(deadline_remaining)
     → 5秒内 channel 未 READY
     → RAY_LOG(INFO) << "Failed to connect to GCS within 5 seconds"
       ← ★ 这就是 pid=3051 的 rpc_client.h:153 报错!

⑬ gcs_client.cc:189
   if (options_.should_fetch_cluster_id_)               ← true! 进入 FetchClusterId
     → FetchClusterId(timeout_ms=5000)

⑭ gcs_client.cc:193
   Status GcsClient::FetchClusterId(int64_t timeout_ms)
     → if (!GetClusterId().IsNil()) return OK           ← cluster_id 为 nil, 不跳过

⑮ gcs_client.cc:200-203
   Status s = client_context_->GetGcsRpcClient().SyncGetClusterId(
       std::move(request), &reply, timeout_ms=5000)    ← 5秒 deadline gRPC 调用

━━━ 阶段 D: SyncGetClusterId 内部 — gRPC 同步调用━━━

⑯ rpc_client.h:107-116 (宏展开)
   Status SyncGetClusterId(request, reply_in, timeout_ms=5000)
     → std::promise<Status> promise
     → GetClusterId(std::move(request), callback, timeout_ms)
     → return promise.get_future().get()

⑰ rpc_client.h:89-101 (宏展开 VOID_GCS_RPC_CLIENT_METHOD → GetClusterId 异步版)
   void GetClusterId(request, callback, timeout_ms=-1)
     → invoke_async_method<NodeInfoGcsService, GetClusterIdRequest, GetClusterIdReply>(
           &NodeInfoGcsService::Stub::PrepareAsyncGetClusterId,
           node_info_grpc_client_, call_name, request, callback, timeout_ms)

⑱ rpc_client.h:215-230
   void invoke_async_method(...)
     → retryable_grpc_client_->CallMethod<Service, Request, Reply>(...)

⑲ retryable_grpc_client.h:245-260
   RetryableGrpcClient::CallMethod(prepare_async_function, grpc_client, ...)
     → RetryableGrpcRequest::Create(...)
     → request->CallMethod()

━━━ 阶段 E: gRPC 通信层 — Deadline Exceeded━━━

⑳ gRPC C-core 层
   → 建立 TCP 连接到 GCS Server (ip:port)
   → 设置 deadline = now + 5000ms (5秒)
   → 5秒内无法建立 TCP 连接或收到 GCS 回复
   → Deadline Exceeded!
   → 返回 Status::TimedOut("RPC error: Deadline Exceeded")

━━━ 阶段 F: 回到 FetchClusterId — 失败处理━━━

㉑ gcs_client.cc:204
   RAY_LOG(WARNING) << "Failed to get cluster ID from GCS server: " << s
     → ★ 这就是 pid=2672 的 gcs_client.cc:205 报错!
     → s = "TimedOut: RPC error: Deadline Exceeded"

㉒ gcs_client.cc:206-207
   client_context_->Disconnect()                        ← 断开 GCS 连接
   client_call_manager_.reset()                         ← 重置（cluster_id 变为 nil）

㉓ gcs_client.cc:208
   return s                                             ← 返回 TimedOut Status (非 OK)

━━━ 阶段 G: 错误传播 — C++ → Cython → Python━━━

㉔ global_state_accessor.cc:56
   Status GlobalStateAccessor::Connect()
     → gcs_client_->Connect() 返回 Status 非 OK
     → 返回该失败 Status 给 Cython 层

㉕ global_state_accessor.pyx
   def connect(self):
     → self.inner.Connect() 返回非 OK Status
     → 抛出 RaySystemError("Failed to connect to GCS")

㉖ state.py — GlobalState._connect_and_get_accessor()
     → GlobalStateAccessor.connect() 抛出 RaySystemError
     → 异常传播到 Node.__init__ → default_worker.py:241

㉗ ★ Python Worker/Driver 进程初始化失败，无法加入集群
```

**三条报错的层级关系：**

| 报错 | 进程 (pid) | 层级 | 触发点 | 含义 |
|------|-----------|------|--------|------|
| `rpc_client.h:153` | 3051 | gRPC channel 层 | GcsRpcClient 构造 → WaitForConnected 5秒超时 | GCS gRPC channel 无法建立 |
| `gcs_client.cc:205` | 2672 | RPC 请求层 | FetchClusterId → SyncGetClusterId 5秒 deadline | GCS RPC 请求超时 |
| Python traceback | Python Worker | 应用层 | Node.__init__ → GlobalStateAccessor.connect() | Python 初始化失败 |

**rpc_client.h:153 报错的完整调用链：**

```
GcsRpcClient 构造函数 (rpc_client.h:188-209)
  → RetryableGrpcClient::Create(channel_, ..., server_unavailable_timeout_callback)
  → channel_ = grpc::CreateCustomChannel(gcs_address, InsecureServerCredentials(), ...)
  → WaitForConnected(gcs_rpc_server_connect_timeout_s * 1000 = 5000ms)  ← rpc_client.h:143

WaitForConnected (rpc_client.h:143-153)
  → deadline = gpr_time_add(gpr_now(GPR_CLOCK_MONOTONIC), timeout_ms * 1000)
  → while (channel_->GetState(false) != GRPC_CHANNEL_READY)
      → channel_->WaitForStateChange(current_state, deadline)
  → 超时退出循环
  → RAY_LOG(INFO) << "Failed to connect to GCS within 5 seconds"  ← ★ 报错行!
  → 返回（不 crash，仅日志提示）

★ 注意：WaitForConnected 超时不会导致进程 crash，只是日志提示。
但后续的 FetchClusterId 会因为 channel 不可用而超时 → gcs_client.cc:205 报错。
```

### 14.3 两个报错的因果关系链

**时间线：**
- `09:28:00.406` — pid=2596 (ip=10.48.76.18) CoreWorker gRPC 端口绑定 crash
- `09:28:10` — pid=3051 (ip=10.48.75.44) Python Worker GcsRpcClient WaitForConnected 5秒超时
- `09:28:13.164` — pid=2672 (ip=10.48.75.44) Python Worker FetchClusterId 5秒 deadline 超时

**两个报错涉及不同 IP（10.48.76.18 vs 10.48.75.44），发生在不同节点上。**

**报错一**：CoreWorker 进程启动时端口绑定失败 → RAY_CHECK crash（本地端口竞态）
**报错二**：Python Worker 初始化时 GCS 连接超时 → 无法加入集群（common.pxi:80 bug 导致意外触发 FetchClusterId → GCS 不可达 → 5秒超时）

**因果关系分析：**

```
                    ┌─────────────────────────────────────────┐
                    │        共因可能性（最可能）               │
                    │                                         │
                    │  集群层面网络故障 / GCS Server 异常       │
                    │                                         │
                    │  ┌─────────────┐    ┌──────────────────┐│
                    │  │ 节点 A       │    │ 节点 B            ││
                    │  │ 10.48.76.18  │    │ 10.48.75.44      ││
                    │  │              │    │                   ││
                    │  │ 网络恢复后   │    │ Python Worker     ││
                    │  │ Worker 创建  │    │ 初始化时          ││
                    │  │ 遇到端口     │    │ GCS 连接超时      ││
                    │  │ 竞态 crash   │    │ Deadline Exceeded ││
                    │  │              │    │                   ││
                    │  │ pid=2596     │    │ pid=3051/2672     ││
                    │  │ 09:28:00     │    │ 09:28:10/13       ││
                    │  └─────────────┘    └──────────────────┘│
                    │                                         │
                    └─────────────────────────────────────────┘

                    ┌─────────────────────────────────────────┐
                    │        级联可能性（较弱）                 │
                    │                                         │
                    │  pid=2596 crash → Raylet 检测断连        │
                    │    → Raylet 向 GCS 上报 Worker 死亡     │
                    │    → GCS 处理大量 Actor 状态更新         │
                    │    → GCS 负载过高                        │
                    │    → 节点 B Python Worker 的 GCS 请求    │
                    │    → Deadline Exceeded                  │
                    │                                         │
                    │  ★ 时间差只有 10-13 秒，级联传导不太可能  │
                    │    这么快完成                             │
                    └─────────────────────────────────────────┘

                    ┌─────────────────────────────────────────┐
                    │        独立事件可能性                     │
                    │                                         │
                    │  节点 A: 端口竞态（本地问题）             │
                    │  节点 B: GCS 网络问题（网络问题）         │
                    │  时间接近纯属巧合                         │
                    │                                         │
                    │  ★ 如果两个节点在同一物理机/网络域，       │
                    │    独立事件概率较低                       │
                    └─────────────────────────────────────────┘
```

**最可能的因果关系：共因（集群网络/GCS 故障）**

1. **网络故障发生**：集群层面出现瞬时网络问题（可能影响 GCS Server 可达性）
2. **节点 B (Python Worker)** 受影响更直接 — GCS 连接在 5s deadline 内无法完成 → `Deadline Exceeded`
3. **节点 A (CoreWorker)** 的 crash 是间接影响 — 网络恢复后 Worker 重新创建，端口分配出现竞态
4. 10-13 秒时间差解释：
   - 节点 B Python Worker 的 GCS 超时是网络中断期间的直接表现（5s deadline）
   - 节点 A CoreWorker 的端口 crash 是网络恢复后 Worker 创建过程的间接表现

**核心结论：两个报错不是直接的 A→B 级联关系，而是同一集群网络事件的两个不同症状。**
报错一（chttp2_server.cc:1063）反映的是 **CoreWorker 启动** 时端口 bind 层面的底层错误，
报错二（gcs_client.cc:205 + rpc_client.h:153 + Python traceback）反映的是 **Python Worker 初始化** 时 GCS 通信层面的超时错误。根本原因是 `common.pxi:80` bug 导致 `GlobalStateAccessor` 意外触发 `FetchClusterId`。
报错一 = 本地端口竞态 → CoreWorker 进程 crash，
报错二 = 远程 GCS 不可达 → Python Worker 初始化失败。
两者都指向集群基础设施（网络/端口管理）的稳定性问题，但触发组件和失败层面不同。

### 14.4 Worker 进程完整启动流程：Node.__init__() → CoreWorker → main_loop

**一个 Python Worker 进程从 Raylet 拉起到进入任务循环的完整代码链路，
覆盖两个报错所在的全部阶段。**

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Worker 进程完整启动流程（Python → Cython → C++ → gRPC）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

━━━ 阶段 0: Raylet 拉起 Worker 子进程 ━━━

① raylet/worker_pool.cc — WorkerPool::StartWorkerProcess()
   → subprocess.Popen / fork+exec
   → 命令行: python default_worker.py --node-ip-address=... --gcs-address=...
     --raylet-name=... --worker-id=... --cluster-id=...
   → Worker 子进程启动，参数从 Raylet 进程传入

━━━ 阶段 1: Python 层初始化 — Node.__init__() ━━━
   ★ 报错二（gcs_client.cc:205 / rpc_client.h:153 / Python traceback）发生在此阶段

② python/ray/_private/workers/default_worker.py:221
   ray_params = RayParams(
       node_ip_address=args.node_ip_address,
       gcs_address=args.gcs_address,
       cluster_id=args.cluster_id,              ← 从启动参数传入
       node_id=args.node_id,
       node_manager_port=args.node_manager_port,
       plasma_store_socket_name=args.object_store_name,
       raylet_socket_name=args.raylet_name,
       ...
   )

③ default_worker.py:241
   node = ray._private.node.Node(
       ray_params,
       head=False,                              ← 不是 head node
       shutdown_at_exit=False,
       spawn_reaper=False,
       connect_only=True,                       ← ★ 只连接，不启动新进程
       default_worker=True,
   )

④ python/ray/_private/node.py:67 — Node.__init__(ray_params, connect_only=True)
   → self._gcs_address = ray_params.gcs_address
   → self._node_id = ray_params.node_id         ← 从 Raylet 启动参数传入

⑤ node.py:159 — self.validate_ip_port(self.address)
   → 验证 gcs_address 格式

⑥ node.py:168 — self._init_gcs_client()
   → 这是 connect_only + 非 head 节点时的第一个关键操作

⑦ node.py:695-730 — Node._init_gcs_client()
   for _ in range(NUM_REDIS_GET_RETRIES):        ← 重试循环
       client = GcsClient(
           address=gcs_address,                  ← gcs_address = "ip:port"
           cluster_id=self._ray_params.cluster_id, ← 从启动参数传入 (hex string)
       )
       self.cluster_id = client.cluster_id       ← ★ 连接 GCS 获取 cluster_id
       self._gcs_client = client
       break
   → GcsClient 是 Cython 类 (from ray._raylet import GcsClient)
   → 构造函数内部: 创建 C++ GcsClient → Connect → FetchClusterId (如果 cluster_id 为 nil)

   ★ Worker 进程: cluster_id = args.cluster_id (hex string, 有值)
     → InnerGcsClient.standalone(cluster_id=hex_string) → if cluster_id: True
     → GcsClientOptions(allow_nil=False, fetch=False) → C++ 实际(False, False) ← bug 但无影响
     → cluster_id 非 Nil → ShouldFetchClusterId 返回 false → 不 FetchClusterId → 不超时

   ★ Head Node 进程: cluster_id = None
     → InnerGcsClient.standalone(cluster_id=None) → else: 分支
     → GcsClientOptions(allow_nil=True, fetch=True) → C++ 实际(True, True) ← 正好一致
     → cluster_id 为 Nil + fetch=true → ShouldFetchClusterId 返回 true → FetchClusterId

⑧ node.py:375-386 (connect_only=True 分支)
   node_info = ray._private.services.get_node(
       self.gcs_address,
       self._node_id,
   )
   → 从 GCS 获取本节点的详细信息 (node_manager_port, labels 等)

⑨ python/ray/_private/services.py:548-557 — get_node(gcs_address, node_id)
   global_state = ray._private.state.GlobalState()
   gcs_options = _get_gcs_client_options(gcs_address)
     → GcsClientOptions.create(gcs_address, None,
         allow_cluster_id_nil=True,              ← ★ 允许 cluster_id 为 nil
         fetch_cluster_id_if_nil=False)          ← ★ Python 设置不 fetch
   ★ 但由于 common.pxi:80 bug, C++ 层实际收到:
     fetch_cluster_id_if_nil = allow_cluster_id_nil = True ← ★★★ bug! fetch 变为 True!
   → ShouldFetchClusterId(Nil, True, True) → return true → ★ 触发 FetchClusterId!
   global_state._initialize_global_state(gcs_options)
     → 保存 gcs_options，后续懒初始化

⑩ global_state.get_node(node_id)
   → accessor = self._connect_and_get_accessor()
     → GlobalStateAccessor(gcs_options)           ← Cython → C++
     → self._global_state_accessor.connect()

⑪ python/ray/_private/state.py:45-56 — _connect_and_get_accessor()
   self._global_state_accessor = GlobalStateAccessor(self.gcs_options)
   connected = self._global_state_accessor.connect()
   if not connected:
       self._global_state_accessor = None
       raise RaySystemError("Failed to connect to GCS...") ← ★ Python traceback 报错!

⑫ python/ray/includes/global_state_accessor.pxd — Cython 声明
   cdef cppclass CGlobalStateAccessor "ray::gcs::GlobalStateAccessor":
       CGlobalStateAccessor(const CGcsClientOptions&)
       c_bool Connect()

⑬ src/ray/gcs_rpc_client/global_state_accessor.cc:50-56 — C++ 实现
   bool GlobalStateAccessor::Connect() {
     gcs_client_ = std::make_unique<GcsClient>(gcs_client_options_);
     → ★ gcs_client_options_ 来自 Python 层 GcsClientOptions.create()
     → ★ Python 设 fetch=False, 但 common.pxi:80 bug → C++ 实际 fetch=True!
     → options_.should_fetch_cluster_id_ = true ← ★ bug 导致!
     io_service_ = std::make_unique<instrumented_io_context>();
     → 启动独立线程运行 io_service
     thread_io_service_ = std::make_unique<std::thread>([...] {
         io_service_->run();
     });
     return gcs_client_->Connect(*io_service_).ok(); ← ★ C++ GcsClient::Connect
   }

⑭ src/ray/gcs_rpc_client/gcs_client.cc:121-196 — GcsClient::Connect(io_service, timeout_ms=-1)
   timeout_ms = gcs_rpc_server_connect_timeout_s * 1000 = 5000ms

⑮ gcs_client.cc:125-131 — 创建 RPC 客户端
   client_call_manager_ = make_unique<ClientCallManager>(io_service, ...)
   auto gcs_rpc_client = make_shared<GcsRpcClient>(
       options_.gcs_address_, options_.gcs_port_, *client_call_manager_)
     → ★ GcsRpcClient 构造函数内部:
       → 创建 grpc::Channel 到 GCS Server
       → RetryableGrpcClient::Create(channel_, ..., server_unavailable_timeout_callback)
       → WaitForConnected(5000ms)                  ← ★ rpc_client.h:143

⑯ rpc_client.h:143-153 — WaitForConnected(5000ms)
   deadline = now + 5000ms
   while (channel_->GetState(false) != GRPC_CHANNEL_READY)
       → channel_->WaitForStateChange(current_state, deadline)
   → 5秒内 channel 未 READY
   → RAY_LOG(INFO) << "Failed to connect to GCS within 5 seconds"
     ← ★ pid=3051 的 rpc_client.h:153 报错!
   → 返回（不 crash，仅日志提示）

⑰ gcs_client.cc:189 — 判断是否需要 FetchClusterId
   if (options_.should_fetch_cluster_id_)         ← ★ true! (common.pxi:80 bug 导致 fetch=True)
     → FetchClusterId(timeout_ms=5000)

⑱ gcs_client.cc:193-208 — FetchClusterId(5000ms) ← ★★★ 报错二根本原因! bug 导致意外触发
   → SyncGetClusterId(request, &reply, timeout_ms=5000)
   → 5秒 deadline gRPC 调用
   → 超时 → Status::TimedOut("Deadline Exceeded")
   → RAY_LOG(WARNING) << "Failed to get cluster ID..." ← ★ gcs_client.cc:205 报错!
   → Disconnect() + reset()
   → return TimedOut Status

⑲ global_state_accessor.cc:56 — Connect() 返回
   → gcs_client_->Connect() 返回 Status 非 OK → .ok() = false
   → 返回 false 给 Cython 层

⑳ state.py:53-56 — _connect_and_get_accessor() 异常
   → connected = False
   → self._global_state_accessor = None
   → raise RaySystemError("Failed to connect to GCS...")
     ← ★ Python traceback 报错!

   ★ 如果阶段1失败，Worker 进程直接退出，不会到达阶段2
   ★ Node.__init__() 成功后继续...

⑴ node.py:380-386 — 获取节点信息并更新参数
   node_info = services.get_node(self.gcs_address, self._node_id)
   self._ray_params.node_manager_port = node_info["node_manager_port"]
   self._ray_params.runtime_env_agent_port = node_info["runtime_env_agent_port"]
   self._ray_params.metrics_agent_port = node_info["metrics_agent_port"]
   → 从 GCS 获取 Raylet 的端口信息

━━━ 阶段 2: Python 层 — worker.connect() ━━━

⑵ default_worker.py:254
   ray._private.worker.connect(
       node,
       node.session_name,
       mode=mode,                                   ← ray.WORKER_MODE
       runtime_env_hash=args.runtime_env_hash,
       worker_id=WorkerID.from_hex(args.worker_id),
       ...
   )

⑶ python/ray/_private/worker.py:2482-2566 — connect(node, ...)
   → worker.gcs_client = node.get_gcs_client()    ← 已在阶段1获得
   → _initialize_internal_kv(worker.gcs_client)

⑷ worker.py:2566-2570 — 创建 GcsClientOptions (★ 与阶段1不同!)
   gcs_options = ray._raylet.GcsClientOptions.create(
       node.gcs_address,
       node.cluster_id.hex(),                       ← ★ cluster_id 已从阶段1获得!
       allow_cluster_id_nil=False,                  ← ★ 不允许 nil
       fetch_cluster_id_if_nil=False,               ← ★ 不主动 fetch!
   )
   ★ 关键差异: 此处 cluster_id 已知，不需要 FetchClusterId
     所以不会再次触发 gcs_client.cc:205 报错

⑸ worker.py:2695 — ★★★ 创建 C++ CoreWorker (Cython 桥梁) ★★★
   worker.core_worker = ray._raylet.CoreWorker(
       mode,                                        ← WORKER_MODE
       node.plasma_store_socket_name,               ← plasma_store_socket
       node.raylet_socket_name,                     ← raylet_socket (IPC)
       job_id,
       gcs_options,
       logs_dir,
       node.node_ip_address,
       node.node_manager_port,
       (mode == LOCAL_MODE),                        ← False
       driver_name,
       serialized_job_config,
       node.metrics_agent_port,
       runtime_env_hash,
       worker_id,
       session_name,
       node.cluster_id.hex(),
       "",
       worker_launch_time_ms,
       worker_launched_time_ms,
       debug_source,
   )
   → Cython → C++ CoreWorkerProcess::Initialize(options)

━━━ 阶段 3: C++ 层 — CoreWorkerProcessImpl 构造 ━━━

⑹ src/ray/core_worker/core_worker_process.cc — CoreWorkerProcessImpl(options)
   → InitializeSystemConfig()                      ← 从 Raylet 获取系统配置
     → 创建临时 RayletClient，RPC 调用 GetSystemConfig
     → RayConfig::instance().initialize(config)
   → stats::Init(global_tags)
   → CreateCoreWorker(options_, worker_id_)        ← ★ 核心! 创建 CoreWorker

━━━ 阶段 4: C++ 层 — CreateCoreWorker() ━━━
   ★ 报错一（chttp2_server.cc:1063 端口绑定 crash）发生在此阶段

⑺ core_worker_process.cc — CreateCoreWorker(options, worker_id)
   → io_thread_ = boost::thread(io_thread_attrs, [...] {
       io_service_.run();                           ← 启动 IO 线程
     })

⑻ core_worker_process.cc:206-215 — IPC 注册获取端口
   auto raylet_ipc_client = std::make_shared<ray::ipc::RayletIpcClient>(
       io_service_, options.raylet_socket, -1, -1)  ← IPC socket 连接 Raylet

⑨ core_worker_process.cc:206-220 — RegisterClient
   Status status = raylet_ipc_client->RegisterClient(
       worker_context->GetWorkerID(),
       options.worker_type,                         ← WORKER
       options.language,
       options.node_ip_address,
       options.serialized_job_config,
       &local_node_id,                              ← 输出: 本节点 ID
       &assigned_port,                              ← 输出: 分配的端口 ★
       &is_preemptible_node);
   → IPC 同步请求 → Raylet 分配端口 → assigned_port = 10160

⑩ core_worker_process.cc:222
   RAY_CHECK_GE(assigned_port, 0)                  ← assigned_port=10160 ≥ 0 ✓

⑪ core_worker_process.cc:251-254 — ★★★ 创建 gRPC Server ★★★
   auto core_worker_server = std::make_unique<rpc::GrpcServer>(
       WorkerTypeString(options.worker_type),       ← "WORKER"
       assigned_port,                               ← 10160 ★ 从 Raylet 传入
       options.node_ip_address == "127.0.0.1");    ← false

⑫ core_worker_process.cc:257-260 — 注册 gRPC Service
   core_worker_server->RegisterService(
       std::make_unique<rpc::CoreWorkerGrpcService>(
           io_service_, *service_handler_, -1),
       false);

⑬ core_worker_process.cc:261 — ★★★ GrpcServer::Run() ★★★
   core_worker_server->Run();
   → grpc_server.cc:65 — void GrpcServer::Run()
     → server_address = "0.0.0.0:10160"
     → builder.AddListeningPort(server_address, ...)
     → builder.BuildAndStart()
       → grpc_chttp2_server_start() → TCP bind("0.0.0.0:10160")
         → bind() 失败! → chttp2_server.cc:1063 ← ★ 报错一!
         → server_ = nullptr
     → RAY_CHECK(server_) → crash → SIGABRT

   ★ 如果端口绑定成功，继续...

⑭ core_worker_process.cc:265-270 — 设置 Worker 地址
   rpc::Address rpc_address;
   rpc_address.set_ip_address(options.node_ip_address)
   rpc_address.set_port(core_worker_server->GetPort()) ← 获取实际绑定端口
   rpc_address.set_node_id(local_node_id.Binary())
   rpc_address.set_worker_id(worker_id.Binary())

━━━ 阶段 5: C++ 层 — GcsClient 连接（CoreWorker 专用） ━━━

⑮ core_worker_process.cc:275-276 — 创建 CoreWorker 的 GcsClient
   auto gcs_client = std::make_shared<gcs::GcsClient>(
       options.gcs_options,                         ← GcsClientOptions
       options.node_ip_address,
       worker_context->GetWorkerID());

⑯ core_worker_process.cc:277 — 连接 GCS
   RAY_CHECK_OK(gcs_client->Connect(io_service_))
   → gcs_client.cc:121 — GcsClient::Connect(io_service, timeout_ms=-1)
     timeout_ms = gcs_rpc_server_connect_timeout_s * 1000 = 5000ms

   ★ 此处 gcs_options 的配置:
     allow_cluster_id_nil=False, fetch_cluster_id_if_nil=False
     cluster_id 已知 (从 Python 层传入)
     → should_fetch_cluster_id_ = false
     → 不调用 FetchClusterId，直接返回 Status::OK()

   ★ 与阶段1的关键差异:
     阶段1 (Node._init_gcs_client): cluster_id 可能为 nil → FetchClusterId → 可能超时
     阶段5 (CoreWorker gcs_client): cluster_id 已知 → 不 FetchClusterId → 直接成功

━━━ 阶段 6: C++ 层 — CoreWorker 完整构造 ━━━

⑰ core_worker_process.cc — 构建所有 CoreWorker 子组件
   → RayletClientPool (远程 Raylet gRPC 连接池)
   → CoreWorkerClientPool (远程 CoreWorker gRPC 连接池)
   → ReferenceCounter (引用计数)
   → CoreWorkerPlasmaStoreProvider (Plasma 对象存储)
   → CoreWorkerMemoryStore (内存对象存储)
   → TaskManager (任务管理)
   → ActorTaskSubmitter (Actor 任务提交)
   → NormalTaskSubmitter (普通任务提交)
   → ActorManager (Actor 管理)
   → ObjectRecoveryManager (对象恢复)
   → pubsub::Publisher / Subscriber (发布订阅)
   → TaskEventBuffer (任务事件缓冲)

⑱ core_worker_process.cc — 创建 CoreWorker 对象
   auto core_worker = std::make_shared<CoreWorker>(
       std::move(options), std::move(worker_context),
       io_service_, std::move(core_worker_client_pool),
       std::move(raylet_client_pool), std::move(periodical_runner),
       std::move(core_worker_server),              ← ★ gRPC server 移入 CoreWorker
       std::move(rpc_address),                     ← ★ Worker 地址
       std::move(gcs_client),                      ← ★ GCS client 移入 CoreWorker
       std::move(raylet_ipc_client),               ← ★ IPC client 移入 CoreWorker
       std::move(local_raylet_rpc_client),
       io_thread_,
       std::move(reference_counter), std::move(memory_store),
       std::move(plasma_store_provider),
       ...所有子组件...
   )

━━━ 阶段 7: Python 层 — 完善连接 ━━━

⑲ default_worker.py:256
   ray._private.worker._global_node = node

⑳ default_worker.py — 设置日志文件、Worker 输出重定向
   out_filepath, err_filepath = node.get_log_file_names(...)
   worker.set_out_file(out_filepath)
   worker.set_err_file(err_filepath)

⑴ default_worker.py — 如果有 worker_process_setup_hook，执行用户钩子

━━━ 阶段 8: 进入任务循环 ━━━

⑵ default_worker.py:316
   worker.main_loop()                              ← ★ Python 层进入主循环

⑶ worker.py → core_worker.main_loop() → C++ CoreWorker::RunTaskExecutionLoop()
   → task_execution_service_.run()                 ← ★ 阻塞等待 Raylet 分发任务
   → Worker 进程正式就绪，等待执行任务

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  ★★★ 两个报错在完整启动流程中的位置 ★★★

  报错一 (chttp2_server.cc:1063): 阶段4 步骤⑬ — GrpcServer::Run() 端口绑定失败
  报错二 (gcs_client.cc:205):    阶段1 步骤⑱ — FetchClusterId 5秒超时 ★★★ bug 导致意外触发!
  报错二 (rpc_client.h:153):     阶段1 步骤⑯ — WaitForConnected 5秒超时
  报错二 (Python traceback):     阶段1 步骤⑳ — RaySystemError 异常

  ★ Node.__init__() 在阶段1, CoreWorker 创建在阶段4
    如果阶段1失败 → 进程退出 → 不会到达阶段4
    如果阶段1成功 → 进入阶段4 → 端口绑定可能失败(报错一)

  ★ 本次报错: 节点B Worker 阶段1失败(报错二), 节点A Worker 阶段4失败(报错一)
    两者不可能发生在同一个 Worker 进程中
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

**启动流程中的三次 GCS 连接对比：**

| 连接 | 位置 | GcsClientOptions | cluster_id | FetchClusterId? | 超时场景 |
|------|------|------------------|-----------|----------------|----------|
| ① Node._init_gcs_client | 阶段1 步骤⑦ | `GcsClient(address, cluster_id=启动参数)` | 可能为 nil | 是 (`should_fetch_cluster_id_=true`) | 报错二 |
| ② GlobalStateAccessor.Connect | 阶段1 步骤⑬ | Python: `allow_nil=True, fetch=False`; **C++ 实际: `allow_nil=True, fetch=True`** (bug) | nil + fetch=true (**bug**) | **是** (`should_fetch_cluster_id_=true` **bug导致**) | **★★报错二根本原因** |
| ③ CoreWorker GcsClient | 阶段5 步骤⑮⑯ | `GcsClientOptions(allow_nil=False, fetch=False)` | 已知（从 Python 传入） | 否 | 不超时 |

**关键洞察：阶段1 的 GlobalStateAccessor 连接是报错二的根本原因——由于 `common.pxi:80` bug，
Python 层设置 `fetch_cluster_id_if_nil=False`，但 C++ 层实际收到 `True`，导致 GlobalStateAccessor
意外触发 FetchClusterId → GCS 不可达时 5秒超时。阶段5 的 GcsClient 连接不受此 bug 影响，
因为 cluster_id 非 Nil 时 `ShouldFetchClusterId` 直接返回 false（短路逻辑）。**

---

### 14.5 Cluster ID 完整传递链路与 FetchClusterId 触发机制

#### Cluster ID 从生成到 Worker 进程的完整传递路径

```
[1] GCS Server 启动 — 生成/读取 cluster_id (源头)
    ┌─────────────────────────────────────────────────────────────────────┐
    │ gcs_server.cc:213-264 — GcsServer::Start()                       │
    │   → GetOrGenerateClusterId()                                      │
    │     → kv_manager.Get("cluster", "ray_cluster_id")                │
    │       如果 KV 中不存在: ClusterID::FromRandom() 生成新 ID        │
    │       如果 KV 中已存在: ClusterID::FromBinary() 从存储读取       │
    │     → 持久化到 KV 存储 (namespace="cluster", key="ray_cluster_id")│
    │   → rpc_server_.SetClusterId(cluster_id)                         │
    │   → GcsNodeManager 持有 cluster_id_ → HandleGetClusterId RPC     │
    └─────────────────────────────────────────────────────────────────────┘

[2] Head Node — 从 GCS 获取 cluster_id
    ┌─────────────────────────────────────────────────────────────────────┐
    │ node.py:695-720 — Node._init_gcs_client() (Head Node)            │
    │   → GcsClient(address=gcs_address, cluster_id=None)               │
    │     → InnerGcsClient.standalone(cluster_id=None)                  │
    │       → gcs_client.pxi:61-66:                                     │
    │         cluster_id 为 None →                                      │
    │           GcsClientOptions.create(                                │
    │             gcs_address, None,                                     │
    │             allow_cluster_id_nil=True,        ← ★ 允许 nil       │
    │             fetch_cluster_id_if_nil=True)      ← ★ 必须 fetch!   │
    │       → C++ GcsClient::Connect()                                  │
    │         → should_fetch_cluster_id_ = true                         │
    │         → FetchClusterId(timeout_ms=5000)                         │
    │           → SyncGetClusterId → GetClusterId RPC → GCS 返回 ID    │
    │   → client.cluster_id → ClusterID 对象                           │
    │   → self.cluster_id = client.cluster_id   ← ★ Head Node 获得 ID │
    └─────────────────────────────────────────────────────────────────────┘

[3] Head Node → Raylet — 将 cluster_id 传入 Raylet 启动命令
    ┌─────────────────────────────────────────────────────────────────────┐
    │ node.py:1159-1164 — Node 启动 Raylet                              │
    │   → services.start_raylet(cluster_id=self.cluster_id.hex())       │
    │                                                                     │
    │ services.py:1903-1906 — Raylet 启动命令                            │
    │   → --cluster-id={cluster_id}   ← hex 字符串传入 Raylet 命令行    │
    │                                                                     │
    │ services.py:1738-1760 — start_worker_command 构造                  │
    │   → --cluster-id={cluster_id}   ← ★ 同一个 hex 串嵌入 Worker 命令 │
    │   → 序列化为 --python_worker_command=... 传入 Raylet              │
    └─────────────────────────────────────────────────────────────────────┘

[4] Raylet 进程 — 接收并持有 cluster_id
    ┌─────────────────────────────────────────────────────────────────────┐
    │ main.cc:117 — gflags 定义                                          │
    │   DEFINE_string(cluster_id, "", "...")                             │
    │                                                                     │
    │ main.cc:276-278 — Raylet 启动时解析                                │
    │   RAY_CHECK_NE(FLAGS_cluster_id, "") << "Expected cluster ID."    │
    │   ClusterID cluster_id = ClusterID::FromHex(FLAGS_cluster_id)     │
    │                                                                     │
    │ main.cc:331-335 — Raylet 自用 GcsClient                            │
    │   GcsClientOptions(gcs_address, cluster_id,                        │
    │     allow_cluster_id_nil=false,          ← ★ 不允许 nil           │
    │     fetch_cluster_id_if_nil=false)        ← ★ 不 fetch             │
    │   → should_fetch_cluster_id_ = false → 不走 FetchClusterId        │
    │                                                                     │
    │ main.cc:252 — 接收 python_worker_command                           │
    │   → ParseCommandLine(FLAGS_python_worker_command)                  │
    │   → worker_commands[PYTHON] = [包含 "--cluster-id=<hex>" 的列表]  │
    │   → 存入 WorkerPool.states_by_lang_[PYTHON].worker_command        │
    └─────────────────────────────────────────────────────────────────────┘

[5] WorkerPool → Worker 进程 — 原样传递 --cluster-id
    ┌─────────────────────────────────────────────────────────────────────┐
    │ worker_pool.cc:260-453 — BuildProcessCommandArgs()                │
    │   → 遍历 state.worker_command 的每个 token                        │
    │   → 对 kWorkerDynamicOptionPlaceholder 和 kNodeManagerPortPlaceholder │
    │     做替换，其他 token 原样传递                                    │
    │   → "--cluster-id=<hex>" 原样保留到 worker_command_args           │
    │   → StartProcess(worker_command_args)                              │
    └─────────────────────────────────────────────────────────────────────┘

[6] Worker Python 进程 — 接收 cluster_id
    ┌─────────────────────────────────────────────────────────────────────┐
    │ default_worker.py:26-31 — argparse                                 │
    │   --cluster-id  required=True  type=str                            │
    │                                                                     │
    │ default_worker.py:226-240 — 创建 RayParams                         │
    │   ray_params = RayParams(cluster_id=args.cluster_id)               │
    │   → parameter.py:185,246 — self.cluster_id = cluster_id (hex str) │
    │                                                                     │
    │ default_worker.py:241-248 — 创建 Node                              │
    │   Node(ray_params, head=False, connect_only=True, ...)             │
    │   → node.py — self._ray_params = ray_params                        │
    │   → self._ray_params.cluster_id = args.cluster_id (hex string)    │
    └─────────────────────────────────────────────────────────────────────┘

[7] Worker Node._init_gcs_client() — 使用 cluster_id 连接 GCS
    ┌─────────────────────────────────────────────────────────────────────┐
    │ node.py:710-714 — Worker Node._init_gcs_client()                  │
    │   → GcsClient(address=gcs_address, cluster_id=self._ray_params.cluster_id)│
    │     → InnerGcsClient.standalone(cluster_id=hex_string)             │
    │       → gcs_client.pxi:58-60:                                      │
    │         cluster_id 有值 →                                          │
    │           GcsClientOptions.create(                                  │
    │             gcs_address, cluster_id_hex,                            │
    │             allow_cluster_id_nil=False,       ← ★ 不允许 nil      │
    │             fetch_cluster_id_if_nil=False)      ← ★ 不 fetch      │
    │       → C++ GcsClient::Connect()                                   │
    │         → should_fetch_cluster_id_ = false                         │
    │         → 不调用 FetchClusterId，直接返回 Status::OK()            │
    │   → self.cluster_id = client.cluster_id                            │
    └─────────────────────────────────────────────────────────────────────┘

[8] CoreWorker C++ 层 — cluster_id 最终传入
    ┌─────────────────────────────────────────────────────────────────────┐
    │ _raylet.pyx:2747,2801 — CoreWorker.__cinit__()                     │
    │   → options.cluster_id = CClusterID.FromHex(cluster_id)            │
    │   → CoreWorkerProcess.Initialize(options)                          │
    │   → CoreWorker 持有 cluster_id → gRPC 认证 metadata              │
    └─────────────────────────────────────────────────────────────────────┘

[9] gRPC 认证 — cluster_id 用于 RPC 安全校验
    ┌─────────────────────────────────────────────────────────────────────┐
    │ client_call.h:86-88 — RPC 客户端                                   │
    │   → context_.AddMetadata("ray_cluster_id", cluster_id.Hex())       │
    │                                                                     │
    │ server_call.h:223-238 — RPC 服务端校验                             │
    │   → LAZY_AUTH: 检查 metadata["ray_cluster_id"] == cluster_id_.Hex()│
    │   → EMPTY_AUTH: GetClusterId RPC 不需要 cluster_id 认证           │
    └─────────────────────────────────────────────────────────────────────┘
```

#### cluster_id 为 nil 与非 nil 的条件分析

| 场景 | cluster_id 值 | GcsClientOptions | should_fetch_cluster_id | FetchClusterId? | 超时风险 |
|------|--------------|------------------|------------------------|----------------|----------|
| **GCS Server** | 由自身生成/读取 | — | — | — | 无 |
| **Head Node 首次连接 GCS** | `None` | `allow_nil=True, fetch=True` | `true` | **必须调用** | **5秒超时风险** |
| **Raylet 进程** | `FromHex(FLAGS_cluster_id)` (非 nil) | `allow_nil=False, fetch=False` | `false` | 不调用 | 无 |
| **正常 Worker 进程** | `args.cluster_id` hex 串 (非 nil) | `allow_nil=False, fetch=False` | `false` | 不调用 | 无 |
| **services.get_node() GlobalStateAccessor** | `None` (不传入) | Python: `allow_nil=True, fetch=False`; **C++ 实际: `allow_nil=True, fetch=True`** (bug) | `true` (**bug 导致**) | **意外调用** | **5秒超时风险** |
| **异常场景: Worker 启动参数丢失 cluster_id** | `None` 或空 | `allow_nil=True, fetch=True` | `true` | **必须调用** | **5秒超时风险** |

**关键判断逻辑（gcs_client.pxi:58-66 InnerGcsClient.standalone()）：**

```python
if cluster_id:    # hex string 有值（正常 Worker）
    gcs_options = GcsClientOptions.create(
        gcs_address, cluster_id,
        allow_cluster_id_nil=False,
        fetch_cluster_id_if_nil=False)    # → should_fetch = false → 不超时
else:             # cluster_id 为 None（Head Node 或异常场景）
    gcs_options = GcsClientOptions.create(
        gcs_address, None,
        allow_cluster_id_nil=True,
        fetch_cluster_id_if_nil=True)     # → should_fetch = true → 5秒超时风险!
```

★★★ **关键 Bug 发现：common.pxi:80 参数传递错误**

```python
# common.pxi:73-81 — GcsClientOptions.create() 实现
def create(cls, gcs_address, cluster_id_hex, allow_cluster_id_nil, fetch_cluster_id_if_nil):
    cdef CClusterID c_cluster_id = CClusterID.Nil()
    if cluster_id_hex:
        c_cluster_id = CClusterID.FromHex(cluster_id_hex)
    self = GcsClientOptions()
    ip, port_str = parse_address(gcs_address)
    port = int(port_str)
    self.inner.reset(
        new CGcsClientOptions(
            ip, port, c_cluster_id,
            allow_cluster_id_nil,
            allow_cluster_id_nil))    ← ★★★ BUG! 第5个参数应为 fetch_cluster_id_if_nil
                                      ← 但实际传的是 allow_cluster_id_nil!
    return self
```

**Bug 影响：**

C++ `CGcsClientOptions` 构造函数签名：
```cpp
GcsClientOptions(ip, port, cluster_id, allow_cluster_id_nil, fetch_cluster_id_if_nil)
```

第5个参数 `fetch_cluster_id_if_nil` 应传入 Python 的 `fetch_cluster_id_if_nil` 值，
但 `common.pxi:80` 传的是 `allow_cluster_id_nil` 值，导致：

1. **Worker 进程 `_init_gcs_client()` 调用** — `cluster_id` 有值 (hex string)
   Python: `GcsClientOptions.create(addr, hex, allow_nil=False, fetch=False)`
   C++ 实际: `CGcsClientOptions(addr, port, FromHex, False, False)` ← 正好 allow_nil=False
   → cluster_id 非 Nil → `ShouldFetchClusterId` 返回 false → **无影响**（cluster_id 非 Nil 时 bug 被短路）

2. **`services.get_node()` 中 `_get_gcs_client_options()` 调用** — `cluster_id=None`
   Python: `GcsClientOptions.create(addr, None, allow_nil=True, fetch=False)`
   C++ 实际: `CGcsClientOptions(addr, port, Nil, True, True)` ← ★ bug: fetch 变为 True!
   → `ShouldFetchClusterId(Nil, True, True)` → 返回 **true** → **触发 FetchClusterId!**
   → GCS 不可达时 5秒超时 → **这就是报错二 pid=2672 的根本原因!**

**C++ 层判断逻辑（gcs_client.cc ShouldFetchClusterId()）：**

```cpp
bool GcsClientOptions::ShouldFetchClusterId(ClusterID cluster_id,
                                            bool allow_cluster_id_nil,
                                            bool fetch_cluster_id_if_nil) {
  RAY_CHECK(!((!allow_cluster_id_nil) && fetch_cluster_id_if_nil))
      << " invalid config combination";
  if (!cluster_id.IsNil()) {
    return false;    // cluster_id 非 Nil → 直接返回 false，不受 fetch 参数影响
  }
  RAY_CHECK(allow_cluster_id_nil) << "Unexpected nil Cluster ID.";
  if (fetch_cluster_id_if_nil) {
    return true;     // ★ cluster_id 为 Nil + fetch=true → 返回 true → FetchClusterId!
  } else {
    return false;
  }
}
```

只有当 **cluster_id 为 Nil 且 fetch_cluster_id_if_nil 为 true** 时才会触发 FetchClusterId。

#### 报错二中 pid=2672 的根本原因：common.pxi:80 参数传递 Bug

报错二 pid=2672 的调用堆栈为 `GcsClient::FetchClusterId` → `SyncGetClusterId` → 5秒超时 → `gcs_client.cc:205`。

**pid=2672 是 Worker 进程，其 `--cluster-id` 参数有值（hex string），Worker 自己的 `_init_gcs_client()` 使用该值不会触发 FetchClusterId。**

但 Worker 进程在 Node.__init__() 中还调用了 `services.get_node()` → `_get_gcs_client_options()` → `GlobalStateAccessor` 连接 GCS。
这个 `GlobalStateAccessor` 的 `GcsClientOptions` 传入 `cluster_id=None, allow_cluster_id_nil=True, fetch_cluster_id_if_nil=False`。

**由于 `common.pxi:80` 的 bug，C++ 层实际收到 `fetch_cluster_id_if_nil=True`（而非 Python 层设置的 False），导致：**

```
services.get_node()
  → _get_gcs_client_options()
    → GcsClientOptions.create(addr, None, allow_nil=True, fetch=False)
      → common.pxi:80 bug:
        new CGcsClientOptions(addr, port, Nil, True, True)  ← fetch=True (应为 False!)
      → ShouldFetchClusterId(Nil, True, True) → return true!
  → GlobalStateAccessor.Connect()
    → GcsClient::Connect()
      → should_fetch_cluster_id_ = true
      → FetchClusterId(timeout_ms=5000) ← ★ 5秒超时 → gcs_client.cc:205!
```

**Bug 的影响范围：**

| 调用路径 | Python 传入 | C++ 实际 | should_fetch | 影响 |
|---------|------------|---------|-------------|------|
| Worker `_init_gcs_client()` (cluster_id 有值) | `allow_nil=False, fetch=False` | `allow_nil=False, fetch=False` | false | 无影响 (cluster_id 非 Nil 短路) |
| `services.get_node()` GlobalStateAccessor (cluster_id=None) | `allow_nil=True, fetch=False` | `allow_nil=True, fetch=True` | **true** | ★ **触发 FetchClusterId → 5秒超时风险** |
| Head Node `_init_gcs_client()` (cluster_id=None) | `allow_nil=True, fetch=True` | `allow_nil=True, fetch=True` | true | 正常行为 (本来就应该 fetch) |
| Raylet `main.cc` (cluster_id 有值) | `allow_nil=False, fetch=False` | (直接 C++ 调用，无 bug) | false | 无影响 |

#### FetchClusterId 触发机制总结

```
FetchClusterId 触发条件 (满足以下两个条件才会触发):

  ① cluster_id == Nil/None (Python 层为 None, C++ 层为 ClusterID::Nil())
  ② fetch_cluster_id_if_nil == True

★ 注意: 由于 common.pxi:80 bug, C++ 层的 fetch_cluster_id_if_nil
  实际值 = Python 层的 allow_cluster_id_nil 值, 而非 Python 层的 fetch_cluster_id_if_nil 值!

触发场景 (考虑 bug 后的实际行为):

  ✓ Head Node 首次连接 GCS (Python: cluster_id=None, allow_nil=True, fetch=True)
    → C++: (Nil, True, True) → should_fetch=true → 正常行为, 获取 cluster_id
    → 如果 GCS 不可达: 5秒超时 → RAY_CHECK crash

  ✓ Worker 进程 services.get_node() GlobalStateAccessor
    → Python: cluster_id=None, allow_nil=True, fetch=False
    → C++ 实际: (Nil, True, True) ← ★ bug! fetch 变为 True
    → should_fetch=true → ★ 意外触发 FetchClusterId!
    → 如果 GCS 不可达: 5秒超时 → gcs_client.cc:205 → ★★★ 这就是报错二的根本原因!

  ✓ 异常 Worker 进程 (cluster_id 参数丢失/为空)
    → Python: cluster_id=None, allow_nil=True, fetch=True
    → C++: (Nil, True, True) → should_fetch=true → FetchClusterId
    → 如果 GCS 不可达: 5秒超时 → crash

不触发场景:

  ✗ Raylet 进程 (C++ 直接调用, 无 bug)
    → cluster_id 已知 → should_fetch=false

  ✗ Worker 进程 _init_gcs_client() (cluster_id 有值)
    → Python: cluster_id=hex, allow_nil=False, fetch=False
    → C++ 实际: (FromHex, False, False) ← bug 但 allow_nil=False 恰好等于 fetch=False
    → cluster_id 非 Nil → ShouldFetchClusterId 直接返回 false → 无影响

★★★ 核心结论:

  报错二 pid=2672 是 Worker 进程, 其 --cluster-id 参数有值 (hex string)。
  Worker 自己的 _init_gcs_client() 使用该值不会触发 FetchClusterId。

  但 Worker 进程在 Node.__init__() 中还调用了 services.get_node()，
  其 GlobalStateAccessor 创建时 cluster_id=None, 由于 common.pxi:80 bug,
  C++ 层 fetch_cluster_id_if_nil 被错误设为 True → 触发 FetchClusterId → 5秒超时。

  因此报错二的根本原因是 common.pxi:80 的参数传递 bug:
    第5个参数 fetch_cluster_id_if_nil 传了 allow_cluster_id_nil 的值,
    导致 _get_gcs_client_options() 设置的 fetch=False 没生效,
    GlobalStateAccessor 实际走了 FetchClusterId → GCS 不可达时 5秒超时 crash。
```

#### FetchClusterId 成功后的行为分析

```
当 FetchClusterId 成功（GCS 可达）时, 完整流程如下:

  GlobalStateAccessor.Connect()
    → GcsClient::Connect(io_service)
      → FetchClusterId(timeout_ms=5000)
        → SyncGetClusterId(request, &reply, 5000ms)
          → gRPC GetClusterId RPC → GCS Server HandleGetClusterId
          → reply.set_cluster_id(cluster_id_.Binary())
          → ClusterID::FromBinary(reply.cluster_id()) → 获得 cluster_id
        → client_call_manager_->SetClusterId(reply_cluster_id)
        → return Status::OK()
      → should_fetch_cluster_id_ 已满足 → Connect 返回 OK
    → GlobalStateAccessor 连接成功 → 返回 True

  ★ FetchClusterId 成功后:
    ① cluster_id 不再为 Nil → 后续所有 RPC 请求都会携带 cluster_id metadata
    ② client_call_manager_ 持有 cluster_id → gRPC 认证校验通过
    ③ GlobalStateAccessor 正常工作 → get_node() 成功获取节点信息

  ★ 所以如果 GCS 可达, bug 不会导致任何功能问题:
    - FetchClusterId 成功获取 cluster_id → GlobalStateAccessor 连接正常
    - 后续 RPC 请求携带正确 cluster_id → gRPC 认证通过
    - 整个 Worker 启动流程可以继续 → Node.__init__() → worker.connect() → CoreWorker 创建
```

#### Bug 的隐性影响与设计意图对比

```
★★★ common.pxi:80 bug 的隐性影响 ★★★

  影响一: 不必要的脆弱性 (最关键)

    原设计意图 (fetch=False + allow_nil=True):
      → GlobalStateAccessor.Connect() 不需要 GCS 可达就能成功
      → cluster_id 为 Nil 也允许 → Connect 直接返回 OK
      → 后续 RPC 请求可能因为 cluster_id 缺失而失败, 但 Connect 阶段不受 GCS 影响
      → 这是一种"宽松连接"策略: 先建立连接, 再在实际使用时获取 cluster_id

    Bug 实际行为 (fetch=True + allow_nil=True):
      → GlobalStateAccessor.Connect() 必须依赖 GCS 可达才能成功
      → FetchClusterId 必须成功 → 否则 Connect 失败
      → 这是一种"严格连接"策略: 连接前必须先获取 cluster_id
      → 将"不依赖 GCS 可达就能 Connect"变成"必须 GCS 可达才能 Connect"
      → 增加了不必要的 GCS 依赖 → GCS 异常时 Worker 无法初始化

  影响二: 额外 RPC 调用

    每次 services.get_node() → GlobalStateAccessor.Connect() 时,
    bug 导致多一次 GetClusterId RPC 调用。
    虽然成功时开销不大 (一次 gRPC 请求约 <1ms),
    但在高频调用场景下会累积不必要的网络开销。
    原设计通过 fetch=False 避免了这次 RPC, bug 使其变为必需。

  影响三: 与设计文档的矛盾

    gcs_client.h:55 的 TODO(ryw) 注释写道:
      "eventually we will always have fetch_cluster_id_if_nil = true"
    说明 Ray 开发者最终想让所有场景都 fetch cluster_id,
    但当前阶段 _get_gcs_client_options() 明确设 fetch=False,
    说明 Python 层认为某些场景不应强制 fetch。
    bug 让这个"不应强制 fetch"的设计意图没有生效。

★★★ 设计意图对比: fetch=False vs fetch=True ★★★

| 维度 | 原设计 (fetch=False) | Bug 实际 (fetch=True) |
|------|---------------------|---------------------|
| **Connect 阶段 GCS 可达要求** | 不需要 | 必须需要 |
| **cluster_id 为 Nil 时 Connect** | 直接返回 OK | 必须先 FetchClusterId → GCS 可达才能 OK |
| **GCS 不可达时影响** | Connect 成功, 后续使用可能出问题 | Connect 失败, Worker 无法初始化 |
| **额外 RPC 调用** | 无 | 每次多1次 GetClusterId RPC |
| **适用场景** | GlobalStateAccessor 只做查询, 不需要严格认证 | 所有场景都需要 cluster_id 认证 |
| **超时风险** | Connect 阶段不超时 | 5秒 FetchClusterId 超时 → crash |

★ 关键洞察:

  原设计 (fetch=False + allow_nil=True) 的策略是"先连接后认证":
    GlobalStateAccessor 不关心 cluster_id, 先把 gRPC channel 建立起来,
    cluster_id 在后续使用时再通过其他方式获取或处理。

  Bug (fetch=True) 的策略是"先认证后连接":
    GlobalStateAccessor 必须先通过 FetchClusterId 获取 cluster_id,
    才能完成 Connect → GCS 不可达时 Connect 必定失败。

  在 GCS 正常运行的环境中, 两种策略都能正常工作, 区别不大。
  但在 GCS 异常或网络不稳定的环境中, "先连接后认证"更健壮,
  而"先认证后连接"会导致 Worker 初始化失败 → 无法加入集群。

  本报错的场景正是: 节点 B Worker 因 GCS 不可达 → FetchClusterId 5秒超时 → Worker crash。
  如果没有这个 bug, 原设计 (fetch=False) 的 GlobalStateAccessor.Connect() 不会超时,
  Worker 可能继续初始化, 在后续阶段通过 _init_gcs_client() 获取 cluster_id (阶段1 步骤⑦)。
```

---

### 参考源码索引（第 14 节新增）

| 源码文件 | 关键内容 |
|---------|---------|
| `src/ray/rpc/grpc_server.cc` | GrpcServer::Run() — builder.AddListeningPort → BuildAndStart → RAY_CHECK(server_) |
| `src/ray/rpc/grpc_server.h` | GrpcServer 类定义，port_, server_ 成员 |
| `src/ray/core_worker/core_worker_process.cc` | CreateCoreWorker — RegisterClient 获取端口 → GrpcServer 创建 → Run() |
| `src/ray/core_worker/core_worker.cc` | CoreWorker 构造 — core_worker_server_ 成员，AnnounceWorkerPort |
| `src/ray/raylet_ipc_client/raylet_ipc_client.cc` | RegisterClient() — IPC RegisterClientRequest/Reply，assigned_port 解析 |
| `src/ray/raylet/node_manager.cc` | ProcessRegisterClientRequestMessage → RegisterForNewWorker |
| `src/ray/raylet/worker_pool.cc` | RegisterWorker → GetNextFreePort → CheckPortFree → send_reply_callback(port) |
| `src/ray/util/network_util.cc` | CheckPortFree() — 临时 bind 测试后立即 close，存在竞态窗口 |
| `src/ray/gcs_rpc_client/gcs_client.cc` | Connect() → FetchClusterId() → SyncGetClusterId() → WARNING 日志 |
| `src/ray/gcs_rpc_client/rpc_client.h` | GcsRpcClient 构造 → WaitForConnected(5s) → RetryableGrpcClient → server_unavailable_timeout_callback |
| `src/ray/rpc/retryable_grpc_client.cc` | Retry() → CheckChannelStatus() → 60s 超时触发 callback |
| `src/ray/common/ray_config_def.h` | gcs_rpc_server_connect_timeout_s=5, gcs_rpc_server_reconnect_timeout_s=60 |
| `src/ray/raylet/main.cc` | RAY_CHECK_OK(gcs_client->Connect(main_service))，fetch_cluster_id_if_nil=false |
| `src/ray/gcs_rpc_client/global_state_accessor.cc/.h` | GlobalStateAccessor::Connect() — Python → C++ 桥梁，调用 GcsClient::Connect |
| `python/ray/includes/global_state_accessor.pxd/.pyx` | Cython 桥梁 — Python GlobalStateAccessor → C++ GlobalStateAccessor |
| `python/ray/_private/state.py` | GlobalState._connect_and_get_accessor() — Python 层连接 GCS 入口 |
| `python/ray/_private/node.py` | Node.__init__() → services.get_node() — Python Worker 初始化 |
| `python/ray/_private/services.py` | get_node() → GlobalState.get_node() — 获取节点信息 |
| `python/ray/_private/worker/default_worker.py` | main() → Node.__init__() — Python Worker 初始化入口 |
| gRPC C-core `chttp2_server.cc` | grpc_chttp2_server_start() — TCP bind/listen，端口绑定失败 ERROR |
| `src/ray/gcs/gcs_server.cc` | GcsServer::Start() → GetOrGenerateClusterId() — 生成/读取 cluster_id |
| `src/ray/gcs/gcs_node_manager.cc` | HandleGetClusterId RPC — 返回 cluster_id_ |
| `python/ray/includes/gcs_client.pxi` | InnerGcsClient.standalone() (58-66行) — 根据 cluster_id 是否存在决定 GcsClientOptions |
| `python/ray/includes/common.pxi` | GcsClientOptions.create() — ★★★ 第80行参数传递 bug: fetch_cluster_id_if_nil 传了 allow_cluster_id_nil 值 |
| `python/ray/_private/parameter.py` | RayParams.cluster_id — 初始 None，Worker 从 args.cluster_id 设置 |
| `python/ray/_private/services.py` | start_worker_command `--cluster-id={cluster_id}` — hex 串嵌入 Worker 命令 |
| `src/ray/rpc/client_call.h` | gRPC 客户端 metadata["ray_cluster_id"] = cluster_id.Hex() |
| `src/ray/rpc/server_call.h` | gRPC 服务端 LAZY_AUTH/EMPTY_AUTH cluster_id 校验 |
