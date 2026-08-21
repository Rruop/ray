# Plasma Store 三套引用计数体系与对象生命周期全链路分析

本文档详细分析 Ray Plasma Store 中三套引用计数（owner ref、client count、server ref_count）的完整工作机制，以及对象在 Create、Put、Push、Pull、ReceivedByPush 等路径下的 ref 变化全链路，含相关代码位置。

---

## 目录

- [1. 三套计数体系总览](#1-三套计数体系总览)
- [2. Client Count 详解](#2-client-count-详解)
- [3. Server ref_count 详解](#3-server-ref_count-详解)
  - [3.5 Server 端多 Client 管理架构](#35-server-端多-client-管理架构)
    - [3.5.1~3.5.15 Socket监听/Client创建/消息循环/分发/引用注册释放/断连清理/fd管理](#35-server-端多-client-管理架构)
- [4. Owner Ref 详解](#4-owner-ref-详解)
- [5. 三套计数的联动关系](#5-三套计数的联动关系)
- [6. 完整链路 1：Put（owner 本地小对象）](#6-完整链路-1putowner-本地小对象)
- [7. 完整链路 2：SealExisting（task 执行端写返回值）](#7-完整链路-2sealexistingtask-执行端写返回值)
- [8. 完整链路 3：PinExistingReturnObject（plasma 中已有对象）](#8-完整链路-3pinexistingreturnobjectplasma-中已有对象)
- [9. 完整链路 4：Pull（worker 主动拉取远程对象）](#9-完整链路-4pullworker-主动拉取远程对象)
- [10. 完整链路 5：ReceivedByPush（replication push 接收端）—— 问题所在](#10-完整链路-5receivedbypushreplication-push-接收端--问题所在)
- [11. 完整链路 6：Owner Unpin（owner out of scope）](#11-完整链路-6owner-unpinowner-out-of-scope)
- [12. Pull Pin vs Primary Pin 生命周期对比](#12-pull-pin-vs-primary-pin-生命周期对比)
- [13. 汇总对比表](#13-汇总对比表)
- [14. 关键代码索引](#14-关键代码索引)
- [15. Object Location 上报机制](#15-object-location-上报机制)
- [16. WorkerObjectEviction Pub/Sub 完整机制](#16-workerobjecteviction-pubsub-完整机制)
- [17. WorkerObjectLocations Pub/Sub 机制](#17-workerobjectlocations-pubsub-机制)
- [18. Spill 流程详解](#18-spill-流程详解)
- [19. Spill 恢复流程详解](#19-spill-恢复流程详解)
- [20. 关键代码索引（补充）](#20-关键代码索引补充)
- [21. 对象生命周期与引用保护层级](#21-对象生命周期与引用保护层级)
- [22. HandleObjectMissing vs HandleObjectFreed](#22-handleobjectmissing-vs-handleobjectfreed)
- [23. Pull 依赖对象的完整生命周期](#23-pull-依赖对象的完整生命周期)
- [24. CancelPull 的必要性](#24-cancelpull-的必要性)
- [25. PlasmaClient::Release → SendReleaseRequest → Server RemoveReference 完整 IPC 链路](#25-plasmaclientrelease--sendreleaserequest--server-removereference-完整-ipc-链路)
- [26. Owner Ref 深度分析：AddObjectOutOfScopeOrFreedCallback 与 Lineage 机制](#26-owner-ref-深度分析addobjectoutofscopeorfreedcallback-与-lineage-机制)
- [27. Recovery 场景：Streaming Generator Return Objects 完整分析](#27-recovery-场景streaming-generator-return-objects-完整分析)
- [28. HandleObjectMissing 对 spilled 条目的 Bug 分析与修复](#28-handleobjectmissing-对-spilled-条目的-bug-分析与修复)
- [29. Pull 对象不会走 PinObjectsAndWaitForFree](#29-pull-对象不会走-pinobjectsandwaitforfree)
- [30. 完整代码索引（补充二）](#30-完整代码索引补充二)
- [31. Plasma Store 内存模型：Fallback Allocation 与 dlmalloc 机制](#31-plasma-store-内存模型fallback-allocation-与-dlmalloc-机制)
- [32. OOM 与 ObjectReconstructionFailed 生产场景分析](#32-oom-与-objectreconstructionfailed-生产场景分析)
- [33. ResubmitTask / MarkGeneratorFailedAndResubmit 重建流程](#33-resubmittask--markgeneratorfailedandresubmit-重建流程)
- [34. Push/Pull 数据到达后如何通知 HandleObjectAdded](#34-pushpull-数据到达后如何通知-handleobjectadded)
- [35. available_memory_bytes 与 RayParams 内存配置详解](#35-available_memory_bytes-与-rayparams-内存配置详解)

---

## 1. 三套计数体系总览

| 计数 | 存储位置 | 数据结构 | 精确字段 | 含义 | 决定什么 |
|------|---------|---------|---------|------|---------|
| **owner ref** | CoreWorker 进程 | `ObjectRefCount` | `reference_counter.cc` → `object_id_refs_[id]` | 对象语义生命周期（谁 owns、是否 out of scope、pin 在哪个节点） | 何时发布 WorkerObjectEviction |
| **client count** | Plasma Client 进程 | `ObjectInUseEntry` | `client.h:339` → `objects_in_use_[id].count` | "这个 client 连接对该对象的 Create/Get 次数 - Release 次数" | 何时发 ReleaseRequest 给 server（count=0 时） |
| **server ref_count** | Plasma Store 进程 | `LocalObject` | `common.h:181` → `ref_count_` | "有多少个 client 连接在用此对象" | 是否可被 LRU 淘汰（=0 时加入 LRU） |

**核心规则**：
- server ref_count > 0 → `BeginObjectAccess`（从 LRU 移除）→ 不可淘汰
- server ref_count == 0 → `EndObjectAccess`（加入 LRU）→ 可淘汰
- LRU 淘汰只看 server ref_count，不看 client count 也不看 owner ref

**三者的数值关系**：

```
server ref_count = Σ (每个 client 连接的贡献)
                  = Σ [若 client_i.objects_in_use_.count > 0 → 1, 否则 → 0]

client count ≥ 1 时 → 该 client 对 server ref 贡献 1
client count = 0 时 → 该 client 对 server ref 贡献 0（已 MarkObjectUnused + SendReleaseRequest）
```

- **server ref 只看"该 client 是否还在用"，不看 client count 具体是多少**：client count=2 和 count=1 对 server ref 贡献相同（都是 +1）
- **client count 是一个 client 内部的多次引用累加**：Create 返回 count=2 是因为 InsertObjectInUse(+1) + IncrementObjectCount(+1)，但 server 只 AddReference 一次
- **只有 client count 归零时才发 ReleaseRequest**：server 收到后才 RemoveReference（ref_count -1）

---

## 2. Client Count 详解

### 2.1 存储结构

```cpp
// client.h:339
struct ObjectInUseEntry {
    int count = 0;       // Create/Get 次数 - Release 次数
    PlasmaObject object; // 客户端持有的 buffer 引用
    bool is_sealed;      // 是否已 seal
};

// client.h:323
absl::flat_hash_map<ObjectID, std::unique_ptr<ObjectInUseEntry>> objects_in_use_;
```

### 2.2 Client count 变更操作

#### InsertObjectInUse（count 0→1）

```cpp
// client.cc:110
void PlasmaClient::InsertObjectInUse(const ObjectID &object_id,
                                     std::unique_ptr<PlasmaObject> object,
                                     bool is_sealed) {
  auto inserted =
      objects_in_use_.insert({object_id, std::make_unique<ObjectInUseEntry>()});
  RAY_CHECK(inserted.second) << "Object already in use";
  auto it = inserted.first;
  it->second->object = std::move(*object);
  it->second->count = 1;  // ← count 初始为 1
  it->second->is_sealed = is_sealed;
}
```

**唯一调用者**：`client.cc:188`（Create 路径中 `HandleCreateReply` 后）

#### IncrementObjectCount（count +1）

```cpp
// client.cc:126
void PlasmaClient::IncrementObjectCount(const ObjectID &object_id) {
  auto object_entry = objects_in_use_.find(object_id);
  RAY_CHECK(object_entry != objects_in_use_.end());
  object_entry->second->count += 1;
}
```

**唯一调用者**：`client.cc:193`（Create 路径，Seal 前保护，count 1→2）

#### Release（count -1，count==0 时通知 server）

```cpp
// client.cc:490
Status PlasmaClient::Release(const ObjectID &object_id) {
  std::lock_guard<std::recursive_mutex> guard(client_mutex_);
  const auto object_entry = objects_in_use_.find(object_id);
  RAY_CHECK(object_entry != objects_in_use_.end());

  object_entry->second->count -= 1;  // ← count 减 1
  RAY_CHECK_GE(object_entry->second->count, 0);

  if (object_entry->second->count == 0) {  // ← 只有 count==0 才通知 server
    // MarkObjectUnused: 从 objects_in_use_ 移除，释放 mmap 映射
    RAY_RETURN_NOT_OK(MarkObjectUnused(object_id));
    // SendReleaseRequest: 通知 plasma server ref_count-1
    RAY_RETURN_NOT_OK(SendReleaseRequest(store_conn_, object_id, may_unmap));
    // ... (处理 may_unmap 和 deletion_cache)
  }
  return Status::OK();
}
```

**关键**：count > 0 的 Release 是**静默的**，不发 ReleaseRequest，server 不知道。只有 count==0 时才 `MarkObjectUnused`（释放 mmap）+ `SendReleaseRequest`（通知 server）。

#### MarkObjectUnused（从 objects_in_use_ 移除）

```cpp
// client.cc:480
Status PlasmaClient::MarkObjectUnused(const ObjectID &object_id) {
  auto object_entry = objects_in_use_.find(object_id);
  RAY_CHECK(object_entry != objects_in_use_.end());
  RAY_CHECK_EQ(object_entry->second->count, 0);
  objects_in_use_.erase(object_id);  // ← 只是移除条目，不释放 plasma 对象
  return Status::OK();
}
```

**注意**：`MarkObjectUnused` 只是客户端清理自己的 `objects_in_use_` 表，对象在 plasma store 服务端还活着（ref_count 可能 > 0）。真正释放对象是服务端 ref_count=0 → LRU 淘汰时 `DeleteObject`。

### 2.3 Client count 变更与 server ref 的对应关系

| client 操作 | client count 变化 | 是否通知 server | server ref 变化 |
|------------|-------------------|----------------|----------------|
| Create → 加入 | 0→1+1=2 | 是（CreateRequest → AddToClientObjectIds） | +1 |
| Seal 内部 Release | 2→1 | 否（count>0） | 不变 |
| 回调 Release | 1→0 → 退出 | 是（ReleaseRequest → RemoveFromClientObjectIds） | -1 |
| Get → 加入 | 0→1 | 是（GetRequest → AddToClientObjectIds） | +1 |
| Release | 1→0 → 退出 | 是（ReleaseRequest → RemoveFromClientObjectIds） | -1 |

**规律**：client **首次加入**（Create/Get 时出现在 server 的 client 列表中）→ server ref +1；client **退出**（Release 后 count=0，从 server 的 client 列表移除）→ server ref -1。client 内部 count 变化不影响 server ref。

### 2.4 多 Client 并存

每个和 plasma store 建立连接的进程都是一个独立的 client，各自有独立的 client count：

| client | 谁创建的 | 典型场景 |
|--------|---------|---------|
| worker 的 PlasmaClient | core_worker 进程 | Create/Get/Seal/Release |
| raylet 的 PlasmaClient | raylet 进程 | GetObjectsFromPlasma（pin 时） |
| buffer_pool 的 PlasmaClient | object_manager 线程 | CreateChunk/Seal/Release（Push/Pull 传输） |

**server ref_count = 所有 client 引用数之和**。每个 client 最多贡献 1 个 ref（加入时 +1，退出时 -1）。

---

## 3. Server ref_count 详解

### 3.1 存储结构

```cpp
// common.h:114
class LocalObject {
 public:
  explicit LocalObject(Allocation allocation)
      : allocation_(std::move(allocation)), ref_count_(0) {}

  int32_t GetRefCount() const { return ref_count_; }

 private:
  mutable int32_t ref_count_;  // 有多少个 client 在使用此对象
};
```

### 3.2 AddReference（server ref +1）

```cpp
// obj_lifecycle_mgr.cc:128
bool ObjectLifecycleManager::AddReference(const ObjectID &object_id) {
  auto entry = object_store_->GetObject(object_id);
  if (!entry) { return false; }
  if (entry->ref_count_ == 0) {
    // ref_count 从 0→1: 告诉淘汰策略"对象正在使用"
    eviction_policy_->BeginObjectAccess(object_id);  // 从 LRU 移除，不可淘汰
  }
  entry->ref_count_++;
  stats_collector_->OnObjectRefIncreased(*entry);
  return true;
}
```

**唯一调用者**：`store.cc:144 AddToClientObjectIds → AddReference`

触发场景：
1. **Create 请求**：`store.cc:149 HandleCreateObjectRequest` → `store.cc:176 CreateObject` → `store.cc:191 AddToClientObjectIds` → `AddReference`
2. **Get 请求**：`store.cc:428 PlasmaGetRequest` → `ProcessGetRequest` → Get 完成回调 `store.cc:108 AddToClientObjectIds` → `AddReference`
3. **Get 队列中对象 sealed**：`store.cc:284 MarkObjectSealed` → 遍历等待此对象的 GetRequest → `ReturnFromGet` → 为每个 client `AddToClientObjectIds` → `AddReference`

### 3.3 RemoveReference（server ref -1）

```cpp
// obj_lifecycle_mgr.cc:148
bool ObjectLifecycleManager::RemoveReference(const ObjectID &object_id) {
  auto entry = object_store_->GetObject(object_id);
  if (!entry || entry->ref_count_ == 0) { return false; }

  entry->ref_count_--;
  stats_collector_->OnObjectRefDecreased(*entry);

  if (entry->ref_count_ > 0) {
    return true;  // 还有其他 client 在用
  }

  // ref_count == 0: 告诉淘汰策略"对象不再使用"
  eviction_policy_->EndObjectAccess(object_id);  // 加入 LRU，可被淘汰

  RAY_CHECK(entry->Sealed());
  if (earger_deletion_objects_.count(object_id) > 0) {
    DeleteObjectInternal(object_id);  // 立即删除（而非等 LRU）
  }
  return true;
}
```

**唯一调用者**：`store.cc:256 RemoveFromClientObjectIds → RemoveReference`

触发场景：
1. **Release 请求**：`store.cc:434 PlasmaReleaseRequest` → `store.cc:440 ReleaseObject` → `store.cc:265 RemoveFromClientObjectIds` → `RemoveReference`
2. **Client 断连**：`store.cc:478 PlasmaDisconnectClient` → `store.cc:485 DisconnectClient` → 遍历 client 所有 object_ids → 逐个 `RemoveFromClientObjectIds` → `RemoveReference`

### 3.4 LRU 淘汰策略

```cpp
// eviction_policy.cc:105
int64_t EvictionPolicy::ChooseObjectsToEvict(int64_t num_bytes_required,
                                             std::vector<ObjectID> &objects_to_evict) {
  // 只从 LRU cache 中选对象淘汰
  int64_t bytes_evicted = cache_.ChooseObjectsToEvict(num_bytes_required, objects_to_evict);
  for (auto &object_id : objects_to_evict) {
    cache_.Remove(object_id);
  }
  return bytes_evicted;
}

// eviction_policy.cc:136
void EvictionPolicy::BeginObjectAccess(const ObjectID &object_id) {
  cache_.Remove(object_id);             // 从 LRU 移除 → 不可淘汰
  pinned_memory_bytes_ += GetObjectSize(object_id);
}

void EvictionPolicy::EndObjectAccess(const ObjectID &object_id) {
  auto size = GetObjectSize(object_id);
  cache_.Add(object_id, size);          // 加入 LRU → 可被淘汰
  pinned_memory_bytes_ -= size;
}
```

**关键**：`BeginObjectAccess` 在 ref_count 从 0→1 时调用（从 LRU 移除），`EndObjectAccess` 在 ref_count 从 1→0 时调用（加入 LRU）。LRU 只淘汰 ref_count=0 的对象。

### 3.5 Server 端多 Client 管理架构

PlasmaStore 是**单进程单线程**，通过一个 Unix domain socket 监听，所有 PlasmaClient 都连到同一个 socket。Server 端不关心"Worker"还是"Raylet"的身份标签，只认 `Client*` 指针（即 socket 连接）。

#### 3.5.1 Unix Domain Socket 监听 — DoAccept

```cpp
// store.cc:502-505
void PlasmaStore::DoAccept() {
  acceptor_.async_accept(
      socket_,
      boost::bind(&PlasmaStore::ConnectClient, this, boost::asio::placeholders::error));
}
```

**机制**：`acceptor_` 是 boost::asio 的 `local::stream_protocol::acceptor`，绑定在 `/tmp/plasma_store_socket` 上。`async_accept` 是非阻塞的——有新连接时回调 `ConnectClient`，没有时挂起不消耗 CPU。`DoAccept` 在 `ConnectClient` 末尾再次调用（line 326），形成**循环监听**。

#### 3.5.2 新连接到达 — ConnectClient 创建 Client 对象

```cpp
// store.cc:303-327
void PlasmaStore::ConnectClient(const boost::system::error_code &error) {
  if (!error) {
    auto new_connection = Client::Create(
        /*message_handler=*/
        [this](const std::shared_ptr<Client> &client,
               fb::MessageType message_type,
               const std::vector<uint8_t> &message) -> Status {
          return ProcessClientMessage(client, message_type, message);
        },
        /*connection_error_handler=*/
        [this](const std::shared_ptr<Client> &client,
               const boost::system::error_code &err) -> void {
          return HandleClientConnectionError(client, err);
        },
        std::move(socket_));

    // Start receiving messages.
    new_connection->ProcessMessages();
  }

  if (error != boost::asio::error::operation_aborted) {
    DoAccept();  // 继续等待下一个连接
  }
}
```

**关键**：两个 lambda 闭包决定了这个 Client 的整个生命周期：
- **message_handler**：每条消息到达时调用 `ProcessClientMessage(client, ...)` — `client` 参数标识了"谁发的"
- **connection_error_handler**：socket 错误/断连时调用 `HandleClientConnectionError(client, ...)`

`std::move(socket_)` 将 accept 得到的 socket 移交给 Client，此后这个 socket 的所有读写都由 Client 对象管理。

#### 3.5.3 Client::Create — 消息处理 lambda 桥接

```cpp
// connection.cc:83-111
std::shared_ptr<Client> Client::Create(
    PlasmaStoreMessageHandler message_handler,
    PlasmaStoreConnectionErrorHandler connection_error_handler,
    ray::local_stream_socket &&socket) {
  ray::MessageHandler ray_message_handler =
      [message_handler](const std::shared_ptr<ray::ClientConnection> &client,
                        int64_t message_type,
                        const std::vector<uint8_t> &message) {
        Status s = message_handler(std::static_pointer_cast<Client>(client),
                                   static_cast<MessageType>(message_type),
                                   message);
        if (!s.ok()) {
          if (!s.IsDisconnected()) {
            RAY_LOG(ERROR) << "Fail to process client message. " << s.ToString();
          }
          client->Close();
        } else {
          client->ProcessMessages();  // ★ 处理成功 → 重新发起异步读
        }
      };

  ray::ConnectionErrorHandler ray_connection_error_handler =
      [connection_error_handler](const std::shared_ptr<ray::ClientConnection> &client,
                                 const boost::system::error_code &error) {
        connection_error_handler(std::static_pointer_cast<Client>(client), error);
      };

  return std::make_shared<Client>(
      PrivateTag{}, ray_message_handler, ray_connection_error_handler, std::move(socket));
}
```

**桥接层**：`ray::MessageHandler` 的签名是 `(shared_ptr<ClientConnection>, int64_t, vector<uint8_t>)`，Plasma 的是 `(shared_ptr<Client>, MessageType, vector<uint8_t>)`。这个 lambda 做了两件事：
1. `std::static_pointer_cast<Client>` — 从基类指针向下转型为 Plasma Client
2. 处理成功后调用 `client->ProcessMessages()` — **重新发起异步读**，形成消息处理循环

#### 3.5.4 ProcessMessages — 异步读循环

```cpp
// client_connection.cc:373-404
void ClientConnection::ProcessMessages() {
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

// client_connection.cc:474-487
void ClientConnection::ProcessMessage(const boost::system::error_code &error) {
  auto this_ptr = shared_ClientConnection_from_this();
  if (error) {
    return connection_error_handler_(std::move(this_ptr), error);
  }
  if (closed_) { return; }
  message_handler_(std::move(this_ptr), read_type_, read_message_);
  // ★ message_handler_ 就是 Client::Create 中的 ray_message_handler
  //   处理成功后重新调 ProcessMessages()，形成循环
}
```

**异步读循环**：
```
ProcessMessages (async_read header)
  → ProcessMessageHeader (async_read body)
    → ProcessMessage (message_handler_)
      → Client::Create lambda → ProcessClientMessage(client, ...)
        → 成功 → ProcessMessages() [下一轮循环]
        → 失败 → Close()
```

**Client 对象的存活保证**：每次 async_read 都通过 `shared_ClientConnection_from_this()` 持有 `shared_ptr<Client>`。只要 socket 连接在、有消息要读，`shared_ptr<Client>` 就不会释放。

#### 3.5.5 ProcessClientMessage — 消息分发（完整 switch/case）

```cpp
// store.cc:370-499
Status PlasmaStore::ProcessClientMessage(const std::shared_ptr<Client> &client,
                                         fb::MessageType type,
                                         const std::vector<uint8_t> &message) {
  absl::MutexLock lock(&mutex_);
  const uint8_t *input = const_cast<uint8_t *>(message.data());
  size_t input_size = message.size();

  switch (type) {
  case fb::MessageType::PlasmaCreateRequest: {
    const auto &object_id = GetCreateRequestObjectId(message);
    const auto &request = flatbuffers::GetRoot<fb::PlasmaCreateRequest>(input);
    const size_t object_size = request->data_size() + request->metadata_size();

    auto handle_create = [this, client, message](
                             bool fallback_allocator,
                             PlasmaObject *result) ABSL_NO_THREAD_SAFETY_ANALYSIS {
      mutex_.AssertHeld();
      return HandleCreateObjectRequest(client, message, fallback_allocator, result);
    };

    if (request->try_immediately()) {
      auto result_error = create_request_queue_.TryRequestImmediately(
          object_id, client, handle_create, object_size);
      // ... 发送 CreateReply + fd
    } else {
      auto req_id = create_request_queue_.AddRequest(
          object_id, client, handle_create, object_size);
      ProcessCreateRequests();
      ReplyToCreateClient(client, object_id, req_id);
    }
  } break;

  case fb::MessageType::PlasmaCreateRetryRequest: {
    auto request = flatbuffers::GetRoot<fb::PlasmaCreateRetryRequest>(input);
    const auto &object_id = ObjectID::FromBinary(request->object_id()->str());
    ReplyToCreateClient(client, object_id, request->request_id());
  } break;

  case fb::MessageType::PlasmaAbortRequest: {
    ObjectID object_id;
    ReadAbortRequest(input, input_size, &object_id);
    RAY_CHECK(AbortObject(object_id, client) == 1);
    RAY_RETURN_NOT_OK(SendAbortReply(client, object_id));
  } break;

  case fb::MessageType::PlasmaGetRequest: {
    std::vector<ObjectID> object_ids_to_get;
    int64_t timeout_ms;
    ReadGetRequest(input, input_size, object_ids_to_get, &timeout_ms);
    ProcessGetRequest(client, object_ids_to_get, timeout_ms);
  } break;

  case fb::MessageType::PlasmaReleaseRequest: {
    bool may_unmap;
    ObjectID object_id;
    ReadReleaseRequest(input, input_size, &object_id, &may_unmap);
    bool should_unmap = ReleaseObject(object_id, client);
    // ★ ReleaseObject → RemoveFromClientObjectIds → RemoveReference
    if (may_unmap) {
      RAY_RETURN_NOT_OK(
          SendReleaseReply(client, object_id, should_unmap, PlasmaError::OK));
    }
  } break;

  case fb::MessageType::PlasmaDeleteRequest: {
    std::vector<ObjectID> object_ids;
    ReadDeleteRequest(input, input_size, &object_ids);
    for (auto &object_id : object_ids) {
      error_codes.push_back(object_lifecycle_mgr_.DeleteObject(object_id));
    }
    RAY_RETURN_NOT_OK(SendDeleteReply(client, object_ids, error_codes));
  } break;

  case fb::MessageType::PlasmaContainsRequest: {
    // 检查对象是否 sealed
  } break;

  case fb::MessageType::PlasmaSealRequest: {
    ObjectID object_id;
    ReadSealRequest(input, input_size, &object_id);
    SealObjects({object_id});
    RAY_RETURN_NOT_OK(SendSealReply(client, object_id, PlasmaError::OK));
  } break;

  case fb::MessageType::PlasmaConnectRequest: {
    RAY_RETURN_NOT_OK(SendConnectReply(client, allocator_.GetFootprintLimit()));
  } break;

  case fb::MessageType::PlasmaDisconnectClient: {
    DisconnectClient(client);
    return Status::Disconnected("The Plasma Store client is disconnected.");
  } break;

  default:
    RAY_LOG(FATAL) << "Invalid Plasma message type";
  }
  return Status::OK();
}
```

**所有消息都携带 Client 身份**：switch 的每个 case 都使用入口参数 `client` — 这就是从 socket 连接中自动识别的 client，不是消息内容中指定的。

#### 3.5.6 ProcessGetRequest — Get 请求入队

```cpp
// store.cc:238-245
void PlasmaStore::ProcessGetRequest(const std::shared_ptr<Client> &client,
                                    const std::vector<ObjectID> &object_ids,
                                    int64_t timeout_ms) {
  for (const auto &object_id : object_ids) {
    RAY_LOG(DEBUG) << "Adding get request " << object_id;
  }
  get_request_queue_.AddRequest(client, object_ids, timeout_ms);
}
```

Get 请求不立即处理，而是入 `get_request_queue_`。当对象 sealed 后，queue 的回调触发 `AddToClientObjectIds`。

#### 3.5.7 GetRequestQueue 回调 — 连接到 AddToClientObjectIds

```cpp
// store.cc:104-110 — PlasmaStore 构造函数中 get_request_queue_ 的初始化回调
[this](const ObjectID &object_id,
       std::optional<MEMFD_TYPE> fallback_allocated_fd,
       const auto &request) ABSL_NO_THREAD_SAFETY_ANALYSIS {
  mutex_.AssertHeld();
  this->AddToClientObjectIds(
      object_id, fallback_allocated_fd, request->client_);
},
```

**调用时机**：当一个 Get 请求等待的对象被 Seal 时（`MarkObjectSealed` → `ReturnFromGet`），queue 遍历所有等待此对象的请求，对每个 client 调用此回调。`request->client_` 就是发起 Get 的那个 Client。

#### 3.5.8 AddToClientObjectIds — Client 首次引用时 server ref +1

```cpp
// store.cc:136-147
void PlasmaStore::AddToClientObjectIds(const ObjectID &object_id,
                                       std::optional<MEMFD_TYPE> fallback_allocated_fd,
                                       const std::shared_ptr<ClientInterface> &client) {
  auto &object_ids = client->GetObjectIDs();
  if (object_ids.find(object_id) != object_ids.end()) {
    return;  // ★ 该 Client 已持有此对象 → 不重复 +1
  }
  RAY_CHECK(object_lifecycle_mgr_.AddReference(object_id));  // server ref +1
  client->MarkObjectAsUsed(object_id, fallback_allocated_fd); // 加入 Client 的 set
}
```

**幂等性**：同一 Client 对同一对象多次调用 `AddToClientObjectIds`，只有第一次会 `AddReference`。后续调用直接 return（`object_ids.find` 命中）。

**两个调用入口**：

| 入口 | 位置 | 场景 |
|------|------|------|
| `CreateObject` 中 | `store.cc:191` | 新建对象后立即注册 creator client |
| Get 请求回调中 | `store.cc:108` | 对象 sealed 后注册 getter client |

#### 3.5.9 RemoveFromClientObjectIds — Client 退出时 server ref -1

```cpp
// store.cc:247-262
bool PlasmaStore::RemoveFromClientObjectIds(const ObjectID &object_id,
                                            const std::shared_ptr<Client> &client) {
  auto &object_ids = client->GetObjectIDs();
  auto it = object_ids.find(object_id);
  if (it != object_ids.end()) {
    bool should_unmap = client->MarkObjectAsUnused(object_id);
    RAY_LOG(DEBUG) << "Object " << object_id
                   << " no longer in use by client, should_unmap = " << should_unmap;
    object_lifecycle_mgr_.RemoveReference(object_id);  // server ref -1
    return should_unmap;  // 返回 true 表示 client 需要munmap此fd
  } else {
    return false;  // 该 Client 不持有此对象 → 什么都不做
  }
}
```

**只移除指定 Client 的引用**：如果 Worker Client 释放了对象，Raylet Client 的引用不受影响。

#### 3.5.10 ReleaseObject — ReleaseRequest 的处理入口

```cpp
// store.cc:265-273
bool PlasmaStore::ReleaseObject(const ObjectID &object_id,
                                const std::shared_ptr<Client> &client) {
  auto entry = object_lifecycle_mgr_.GetObject(object_id);
  if (entry != nullptr) {
    return RemoveFromClientObjectIds(object_id, client);
  }
  return false;
}
```

#### 3.5.11 Client 断连 — HandleClientConnectionError → DisconnectClient

```cpp
// store.cc:362-367
void PlasmaStore::HandleClientConnectionError(const std::shared_ptr<Client> &client,
                                              const boost::system::error_code &error) {
  absl::MutexLock lock(&mutex_);
  RAY_LOG(WARNING) << "Disconnecting client due to connection error with code "
                   << error.value() << ": " << error.message();
  DisconnectClient(client);
}

// store.cc:330-359
void PlasmaStore::DisconnectClient(const std::shared_ptr<Client> &client) {
  client->Close();
  RAY_LOG(DEBUG) << "Disconnecting client on fd " << client;

  // Release all the objects that the client was using.
  absl::flat_hash_map<ObjectID, const LocalObject *> sealed_objects;
  auto &object_ids = client->GetObjectIDs();
  for (const auto &object_id : object_ids) {
    auto entry = object_lifecycle_mgr_.GetObject(object_id);
    if (entry == nullptr) {
      continue;
    }
    if (entry->Sealed()) {
      sealed_objects[object_id] = entry;  // 已Seal：收集后统一释放
    } else {
      object_lifecycle_mgr_.AbortObject(object_id);  // 未Seal：直接abort
    }
  }

  // Remove all of the client's GetRequests.
  get_request_queue_.RemoveGetRequestsForClient(client);

  for (const auto &[object_id, _] : sealed_objects) {
    RemoveFromClientObjectIds(object_id, client);  // server ref -1 (每个对象)
  }

  create_request_queue_.RemoveDisconnectedClientRequests(client);
}
```

**关键逻辑**：
1. 已 Seal 的对象：遍历 `client->GetObjectIDs()` 逐个 `RemoveFromClientObjectIds` → `RemoveReference`
2. 未 Seal 的对象（client 正在 Create 但还没 Seal）：`AbortObject` — 回收内存分配
3. 同时清理该 client 的所有 pending Get/Create 请求

**为什么先收集再释放**：不能在遍历 `object_ids` 时直接 `RemoveFromClientObjectIds`，因为 `MarkObjectAsUnused` 会从 `object_ids` set 中 erase 元素，**使迭代器失效**。所以先收集到 `sealed_objects` map，遍历完后再统一释放。

#### 3.5.12 Client 类完整定义 — object_ids set + fallback fd 管理

```cpp
// connection.h:42-51 — 抽象接口
class ClientInterface {
 public:
  virtual ~ClientInterface() = default;
  virtual ray::Status SendFd(MEMFD_TYPE fd) = 0;
  virtual const std::unordered_set<ray::ObjectID> &GetObjectIDs() = 0;
  virtual void MarkObjectAsUsed(const ray::ObjectID &object_id,
                                std::optional<MEMFD_TYPE> fallback_allocated_fd) = 0;
  virtual bool MarkObjectAsUnused(const ray::ObjectID &object_id) = 0;
};

// connection.h:54-157 — 具体实现
class Client : public ray::ClientConnection, public ClientInterface {
 public:
  const std::unordered_set<ray::ObjectID> &GetObjectIDs() override { return object_ids; }

  void MarkObjectAsUsed(const ray::ObjectID &object_id,
                        std::optional<MEMFD_TYPE> fallback_allocated_fd) override {
    const auto [_, inserted] = object_ids.insert(object_id);
    if (inserted) {
      RAY_CHECK(!object_ids_to_fallback_allocated_fds_.contains(object_id));
      if (fallback_allocated_fd.has_value()) {
        MEMFD_TYPE fd = fallback_allocated_fd.value();
        object_ids_to_fallback_allocated_fds_[object_id] = fd;
        fallback_allocated_fds_ref_count_[fd] += 1;  // fd 引用计数 +1
      }
    } else {
      // Already inserted, idempotent call.
      const auto iter = object_ids_to_fallback_allocated_fds_.find(object_id);
      if (fallback_allocated_fd.has_value()) {
        RAY_CHECK(iter != object_ids_to_fallback_allocated_fds_.end() &&
                  iter->second == fallback_allocated_fd.value());
      } else {
        RAY_CHECK(iter == object_ids_to_fallback_allocated_fds_.end());
      }
    }
  }

  bool MarkObjectAsUnused(const ray::ObjectID &object_id) override {
    size_t erased = object_ids.erase(object_id);  // ★ 从 set 移除
    if (erased == 0) {
      return false;  // 不持有此对象 → 什么都不做
    }
    auto fd_iter = object_ids_to_fallback_allocated_fds_.find(object_id);
    if (fd_iter == object_ids_to_fallback_allocated_fds_.end()) {
      return false;  // 无 fallback fd
    }
    MEMFD_TYPE fd = fd_iter->second;
    object_ids_to_fallback_allocated_fds_.erase(fd_iter);

    auto ref_cnt_iter = fallback_allocated_fds_ref_count_.find(fd);
    RAY_CHECK(ref_cnt_iter != fallback_allocated_fds_ref_count_.end());
    size_t &ref_cnt = ref_cnt_iter->second;
    RAY_CHECK_GT(ref_cnt, static_cast<size_t>(0));
    ref_cnt -= 1;
    if (ref_cnt == 0) {
      fallback_allocated_fds_ref_count_.erase(ref_cnt_iter);
      used_fds_.erase(fd);  // 下次 SendFd 会重新发送此 fd
      return true;  // ★ 返回 true → 通知 client munmap 此 fd
    }
    return false;
  }

  std::string name = "anonymous_client";

 private:
  absl::flat_hash_set<MEMFD_TYPE> used_fds_;
  std::unordered_set<ray::ObjectID> object_ids;  // ★ 核心：此 Client 在用哪些对象
  absl::flat_hash_map<MEMFD_TYPE, size_t> fallback_allocated_fds_ref_count_;  // fd → 引用数
  absl::flat_hash_map<ray::ObjectID, MEMFD_TYPE> object_ids_to_fallback_allocated_fds_;
};
```

**三层数据结构**：

| 数据结构 | 用途 | 操作 |
|---------|------|------|
| `object_ids` | 此 Client 在用哪些对象 | `MarkObjectAsUsed` insert / `MarkObjectAsUnused` erase |
| `object_ids_to_fallback_allocated_fds_` | 哪些对象用了 fallback fd | insert / erase（与 object_ids 同步） |
| `fallback_allocated_fds_ref_count_` | 每个 fallback fd 的引用计数 | +1 / -1（归零时通知 client munmap） |

**Fallback fd 机制**：当 Plasma Store 主内存（dlmalloc）分配失败时，使用 fallback allocator（memfd_create）分配。fallback fd 需要跨进程 mmap 共享，所以 Client 需要跟踪哪些 fd 对应哪些对象，释放时判断是否可以 munmap。

#### 3.5.13 Server 引用计数机制总结

**server ref_count = 有几个 Client 的 `object_ids` set 中包含该对象**

每个 Client 最多贡献 1，不论 client 内部 count 是多少：

| 场景 | 调用 | ref 变化 |
|------|------|---------|
| Create 成功 | `AddToClientObjectIds(creator_client)` | +1 |
| Get 请求满足 | `AddToClientObjectIds(getter_client)` | +1（新 client 首次） |
| 同一 client 再 Get 同一对象 | `object_ids.find != end` → return | **不变** |
| ReleaseRequest | `RemoveFromClientObjectIds(releaser_client)` | -1 |
| Client 断连 | 遍历该 client 所有 object_ids 逐个 RemoveReference | 每个对象 -1 |

**所以 server ref_count 不等于所有 client count 之和**：

```
Client A: object_ids = {X}  →  贡献 1
Client B: object_ids = {X}  →  贡献 1
Client C: 没有 X            →  贡献 0

server X.ref_count = 2

即使 Client A 内部 count=2（InsertObjectInUse+IncrementObjectCount），
对 server 而言也只贡献 1。
server 只看"该 Client 是否在 object_ids 中"，不看 client count 具体值。
```

#### 3.5.14 完整数值关系示例

```
              Worker Client              Raylet Client           server
              (socket A)                (socket B)             ref_count
              ───────────               ───────────            ─────────
              object_ids = {}           object_ids = {}

Worker Create:
  AddToClientObjectIds(worker_client)
    → worker_client.object_ids 查无 → AddReference         1
    → worker_client.MarkObjectAsUsed
    → worker_client.object_ids = {X}

Raylet Get (pin):
  AddToClientObjectIds(raylet_client)
    → raylet_client.object_ids 查无 → AddReference         2
    → raylet_client.MarkObjectAsUsed
    → raylet_client.object_ids = {X}

Worker Release:
  RemoveFromClientObjectIds(worker_client)
    → worker_client.object_ids 有 X → RemoveReference       1
    → worker_client.MarkObjectAsUnused
    → worker_client.object_ids = {}

Raylet ReleaseFreedObject:
  RemoveFromClientObjectIds(raylet_client)
    → raylet_client.object_ids 有 X → RemoveReference       0
    → raylet_client.MarkObjectAsUnused
    → raylet_client.object_ids = {}
    → EndObjectAccess → 加入 LRU
```

#### 3.5.15 PlasmaStore 维护多 Client 的运行模型

Server 不需要一个显式的 `vector<Client>` 来"维护"——每个 `Client::ProcessMessages()` 通过 **boost::asio 异步回调** 保持存活。只要 socket 连接在、有消息要读，`shared_ptr<Client>` 就不会释放。Client 断连时 `HandleClientConnectionError` 清理引用。

```
                    同一个 Unix domain socket
                    /tmp/plasma_store_socket
                              │
                    ┌─────────▼──────────┐
                    │   PlasmaStore 进程   │
                    │                     │
                    │  acceptor_ 持续监听   │
                    └──┬──────┬──────┬────┘
                       │      │      │
              ┌────────┘      │      └────────┐
              ▼               ▼               ▼
        socket A        socket B        socket C
        Worker 进程      Raylet 进程      另一个 Worker
        PlasmaClient #1  PlasmaClient #2  PlasmaClient #3
              │               │               │
              ▼               ▼               ▼
        Client {           Client {         Client {
          object_ids={X,Y}   object_ids={X,Z}  object_ids={W}
        }                 }               }

        server LocalObject:
          X: ref_count=2  (Worker + Raylet 都持有)
          Y: ref_count=1  (只有 Worker)
          Z: ref_count=1  (只有 Raylet)
          W: ref_count=1  (只有另一个 Worker)
```

---

## 4. Owner Ref 详解

Owner ref 不是数值计数，而是一组语义状态：

```cpp
// reference_counter.h 中 ObjectReference 结构
struct ObjectReference {
  bool owned_by_us_;                         // 是否是 owner
  std::optional<NodeID> pinned_at_node_id_;   // primary pin 在哪个节点
  NodeID spilled_node_id;                    // spill 在哪个节点
  std::optional<NodeID> pin_transferred_from_; // pin 转移来源
  absl::flat_hash_set<NodeID> locations;      // 对象副本在哪些节点
  // OutOfScope() 判断: ref_count==0 且无 borrowed 且无 nested
};
```

### 4.1 设置 pinned_at_node_id_

```cpp
// reference_counter.cc:953
void ReferenceCounter::UpdateObjectPinnedAtRaylet(const ObjectID &object_id,
                                                  const NodeID &node_id,
                                                  bool is_pin_transfer) {
  absl::MutexLock lock(&mutex_);
  auto it = object_id_refs_.find(object_id);
  if (it != object_id_refs_.end()) {
    if (freed_objects_.contains(object_id)) { return; }  // 已释放

    if (!is_node_dead_(node_id)) {
      auto previous_node_id = it->second.pinned_at_node_id_;
      it->second.pinned_at_node_id_ = node_id;  // ← 记录 pin 节点
      if (is_pin_transfer && previous_node_id.has_value()) {
        it->second.pin_transferred_from_ = previous_node_id;
      }
    } else {
      UnsetObjectPrimaryCopy(it);
      objects_to_recover_.push_back(object_id);
    }
    UpdateOwnedObjectCounters(object_id, it->second, /*decrement=*/false);
  }
}
```

**调用者**：
1. `task_manager.cc:572`（owner 收到 task 返回，`in_plasma==true`）
2. `core_worker.cc:1001`（owner 本地 `PutInLocalPlasmaStore`）
3. Pin 转移场景

**注意**：`UpdateObjectPinnedAtRaylet` 只是元数据记录，不涉及 plasma refcount 操作。真正的保护是 raylet 的 `PinObjectsAndWaitForFree` 维持的 server ref_count=1。

### 4.2 节点死亡时的处理

```cpp
// reference_counter.cc:895
void ReferenceCounter::ResetObjectsOnRemovedNode(const NodeID &node_id) {
  for (auto it = object_id_refs_.begin(); it != object_id_refs_.end(); it++) {
    const auto &object_id = it->first;
    auto &ref = it->second;
    const bool has_copy_on_dead_node = ref.locations.contains(node_id);
    const bool primary_on_dead_node =
        ref.pinned_at_node_id_.value_or(NodeID::Nil()) == node_id;
    const bool spilled_on_dead_node = ref.spilled_node_id == node_id;
    const bool protected_by_pin_transfer =
        ref.pin_transferred_from_.has_value() && *ref.pin_transferred_from_ == node_id;

    if (primary_on_dead_node || spilled_on_dead_node) {
      // 需要恢复
      const size_t surviving_locations =
          ref.locations.size() - (has_copy_on_dead_node ? 1 : 0);
      if (surviving_locations > 0) {
        object_on_dead_node_.Record(1, {{"Status", "needs_recovery_has_replica"}});
      } else {
        object_on_dead_node_.Record(1, {{"Status", "needs_recovery_no_replica"}});
      }
      UnsetObjectPrimaryCopy(it);
      if (!ref.OutOfScope(lineage_pinning_enabled_)) {
        objects_to_recover_.push_back(object_id);
      }
    } else if (protected_by_pin_transfer) {
      object_on_dead_node_.Record(1, {{"Status", "protected_by_pin_transfer"}});
      ref.pin_transferred_from_.reset();
    } else if (has_copy_on_dead_node) {
      // 只是副本丢失，不需要恢复
      object_on_dead_node_.Record(1, {{"Status", "secondary_copy_lost"}});
    }
    RemoveObjectLocationInternal(it, node_id);
  }
}
```

### 4.3 OutOfScope → 发布 eviction → 触发 unpin

```
owner reference_counter 判定对象 OutOfScope
  → 发布 WorkerObjectEviction (pub/sub 消息)
  → raylet 收到订阅回调
  → ReleaseFreedObject → 释放 pinned_objects_
  → RayObject 析构 → PlasmaClient::Release
  → [IPC: ReleaseRequest] → server ref-1
```

---

## 5. 三套计数的联动关系

### 5.1 联动总览

```
owner ref (reference_counter)
  │ OutOfScope 时发布 WorkerObjectEviction (pub/sub)
  ↓
raylet 收到订阅回调
  │ ReleaseFreedObject → 释放 RayObject buffer
  │ RayObject 析构 → PlasmaClient::Release (raylet 的 client count)
  ↓
raylet client count 1→0
  │ SendReleaseRequest (IPC)
  ↓
plasma store 收到 ReleaseRequest
  │ RemoveFromClientObjectIds → RemoveReference
  ↓
server ref_count 1→0
  │ EndObjectAccess → 加入 LRU → 可淘汰
```

**方向是单向的**：owner → raylet → server。owner 决定"什么时候允许释放"，raylet 决定"我还要不要继续持有着"，server 决定"实际能不能淘汰"。

**反过来不联动**：server ref=0（对象被 LRU evict）不会通知 owner。owner 的 `reference_counter` 不知道对象已不在 plasma 中，直到需要用时 Get 不到 → 触发 reconstruction。

### 5.2 Owner 不直接操作 plasma store

**Owner 从不经过 PlasmaClient**。owner 不需要 plasma client 连接。owner 只管语义引用计数，发布 "这个对象可以释放了" 的消息。真正持有 plasma client 的是 **raylet**（pin 时 Get 进来的）和 **worker**（Create/Get 进来的）。

### 5.3 client count 与 server ref 的数值对应关系

**server ref_count 不等于所有 client count 之和**。server 只关心"一个 client 连接是否还在使用此对象"，不关心 client 内部 count 具体值：

```
                  client count (内部)          server ref_count (全局)
                  ─────────────────           ─────────────────────
Client A:         count = 2 (Create后)    →    贡献 1
                  count = 1 (Seal后Release) →   贡献 1 (仍 > 0)
                  count = 0 (最终Release)  →    贡献 0 (SendReleaseRequest)

Client B:         count = 1 (Get后)       →    贡献 1
                  count = 0 (Release后)    →    贡献 0

server ref_count = Client A 贡献 + Client B 贡献
```

**对应规则**：

| client count 状态 | 该 client 对 server ref 的贡献 | IPC 行为 |
|-------------------|----------------------------|---------|
| 0 → 正数（InsertObjectInUse / IncrementObjectCount） | 不变（已在 `objects_in_use_` 中则不触发 AddReference） | 无 |
| 首次引用（InsertObjectInUse，count 0→1） | +1 | Get/Create 流程中 server 端 `AddToClientObjectIds → AddReference` |
| 正数 → 正数（IncrementObjectCount / Release count>0） | 不变 | 无 |
| 正数 → 0（Release 后 count==0） | -1 | `MarkObjectUnused` + `SendReleaseRequest` → server 端 `RemoveFromClientObjectIds → RemoveReference` |

**关键代码对应**：

```cpp
// AddToClientObjectIds — 只在 client 首次引用时调用（store.cc:136-147）
void PlasmaStore::AddToClientObjectIds(...) {
  auto &object_ids = client->GetObjectIDs();
  if (object_ids.find(object_id) != object_ids.end()) {
    return;  // ★ 该 client 已注册过此对象 → 不重复 AddReference
  }
  RAY_CHECK(object_lifecycle_mgr_.AddReference(object_id));  // +1
  client->MarkObjectAsUsed(object_id, fallback_allocated_fd);
}

// RemoveFromClientObjectIds — 只在 client 完全释放时调用（store.cc:247-259）
bool PlasmaStore::RemoveFromClientObjectIds(...) {
  auto &object_ids = client->GetObjectIDs();
  auto it = object_ids.find(object_id);
  if (it != object_ids.end()) {
    client->MarkObjectAsUnused(object_id);
    object_lifecycle_mgr_.RemoveReference(object_id);  // -1
  }
}
```

**所以 Create 路径的 count=2 对 server ref 只有 +1**：`InsertObjectInUse` 时 server 端 `AddToClientObjectIds` 已经注册了此 client，`IncrementObjectCount` 只在 client 内部 count++，不会触发新的 IPC 或 AddReference。只有最终 count 归零时 `SendReleaseRequest` → server `RemoveReference`（-1）。

---

## 6. 完整链路 1：Put（owner 本地小对象）

**调用链**：`CoreWorker::Put` → `PutInLocalPlasmaStore` → `plasma_store_provider_->Put` → `Create` + `Seal` → `PinObjectIDs`

```
步骤  代码位置                                        client count   server ref   说明
──────────────────────────────────────────────────────────────────────────────────────────
1     client.cc:188 InsertObjectInUse(count=1)         0→1           -           "我在用"
2     client.cc:193 IncrementObjectCount               1→2           -           "Seal前保护"
3     store.cc:176 CreateObject
      → store.cc:191 AddToClientObjectIds
        → obj_lifecycle_mgr.cc:141 AddReference         2             0→1         server ref+1
        → BeginObjectAccess(从LRU移除)

4     plasma_store_provider.cc:118 Seal()
      → client.cc:584 is_sealed = true
      → client.cc:587 SendSealRequest
        → [IPC: SealRequest]
        → store.cc:275 SealObjects → add_object_callback_
      → client.cc:598 Release(count 2→1)                 2→1           1           不发ReleaseRequest

5     core_worker.cc:1008 PinObjectIDs(异步RPC)
      → [RPC: PinObjectIDsRequest 给 raylet]
      → node_manager.cc:2605 HandlePinObjectIDs
        → node_manager.cc:2578 GetObjectsFromPlasma
          → [IPC: GetRequest]
          → store.cc:108 AddToClientObjectIds(raylet作为新client)
            → AddReference                               1             1→2         raylet加入
        → node_manager.cc:2646 local_object_manager_
           .PinObjectsAndWaitForFree
          → local_object_manager.cc:47 local_objects_.emplace
          → local_object_manager.cc:51 pinned_objects_.emplace
          → local_object_manager.cc:68 Subscribe(WorkerObjectEviction)

6     core_worker.cc:1016 回调 Release()
      → client.cc:500 count-=1 (1→0)
      → client.cc:512 count==0:
        → client.cc:514 MarkObjectUnused (从objects_in_use_移除)
        → client.cc:515 SendReleaseRequest
          → [IPC: ReleaseRequest]
          → store.cc:256 RemoveFromClientObjectIds
            → obj_lifecycle_mgr.cc:157 RemoveReference   已移除          2→1
            → ref_count>0, 不做其他事

      ──── 最终稳态 ────
      client: worker已退出, raylet持有一份(count=1, 在raylet的PlasmaClient中)
      server ref: 1 (raylet pin持有) → spillable, 不可LRU淘汰
      owner ref: pinned_at_node_id_=本节点 (后面UpdateObjectPinnedAtRaylet设置)
```

**Owner 侧**（task_manager 处理返回值时）：

```cpp
// task_manager.cc:556
StatusOr<bool> TaskManager::HandleTaskReturn(const ObjectID &object_id,
                                             const rpc::ReturnObject &return_object,
                                             const NodeID &worker_node_id,
                                             bool store_in_plasma) {
  if (return_object.in_plasma()) {
    // 对象已在远端 plasma 中 → 只记录元数据
    reference_counter_.UpdateObjectPinnedAtRaylet(
        object_id, worker_node_id, /*is_pin_transfer=*/false);
    in_memory_store_.Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA), ...);
  } else if (store_in_plasma) {
    // 需要存到本地 plasma → PutInLocalPlasmaStore
    Status s = put_in_local_plasma_callback_(object, object_id);
  } else {
    // 直接内存对象
    in_memory_store_.Put(object, ...);
    direct_return = true;
  }
}
```

---

## 7. 完整链路 2：SealExisting（task 执行端写返回值）

### 7.1 Python 层序列化与调用

```python
# _raylet.pyx:4207 store_task_outputs
# 遍历 task 的每个返回值 output，序列化后调用 store_task_output

# _raylet.pyx:4175 store_task_output
def store_task_output(self, serialized_object, return_id, ...):
    # 步骤A: 尝试在plasma中分配buffer
    AllocateReturnObject(return_id, data_size, metadata, contained_id,
                         caller_address, &task_output_inlined_bytes, return_ptr)

    if return_ptr != NULL:    # 对象不存在于plasma，新分配了buffer
        # 步骤B: 把序列化数据写入plasma buffer
        serialized_object.write_to(Buffer.make(return_ptr.get().GetData()))
        # 步骤C: Seal + Pin
        SealReturnObject(return_id, return_ptr, generator_id, caller_address)
    else:                     # 对象已存在于plasma
        # 步骤D: Pin已有的对象
        PinExistingReturnObject(return_id, return_ptr, generator_id, caller_address)
```

### 7.2 C++ AllocateReturnObject 的大小判断

```cpp
// core_worker.cc:2707
Status CoreWorker::AllocateReturnObject(const ObjectID &object_id, ...) {
  bool object_already_exists = false;
  std::shared_ptr<Buffer> data_buffer;
  if (data_size > 0) {
    if (data_size < max_direct_call_object_size_ &&
        task_output_inlined_bytes + data_size <= task_rpc_inlined_bytes_limit) {
      // 小对象: LocalMemoryBuffer, 不进plasma, 直接通过RPC传回owner
      data_buffer = std::make_shared<LocalMemoryBuffer>(data_size);
    } else {
      // 大对象: CreateExisting → plasma Create
      RAY_RETURN_NOT_OK(CreateExisting(metadata, data_size, object_id,
                                       owner_address, &data_buffer,
                                       /*created_by_worker=*/true));
      object_already_exists = data_buffer == nullptr;  // plasma中已有对象
    }
  }
  if (!object_already_exists) {
    *return_object = std::make_shared<RayObject>(data_buffer, metadata, ...);
  }
  // object_already_exists时 return_ptr = NULL
}
```

### 7.2a AllocateReturnObject → Create 的 ref 递增完整代码链路

大对象路径：`AllocateReturnObject` → `CreateExisting` → `PlasmaStoreProvider::Create` → `PlasmaClient::CreateAndSpillIfNeeded` → IPC → server `CreateObject` + `AddToClientObjectIds`。

**Step 1: CreateExisting — 薄委托层**

```cpp
// core_worker.cc:1135
Status CoreWorker::CreateExisting(const std::shared_ptr<Buffer> &metadata,
                                  const size_t data_size,
                                  const ObjectID &object_id, ...) {
  return plasma_store_provider_->Create(
      metadata, data_size, object_id, owner_address, data, created_by_worker);
}
```

**Step 2: PlasmaStoreProvider::Create — 调用 CreateAndSpillIfNeeded**

```cpp
// plasma_store_provider.cc:126
Status CoreWorkerPlasmaStoreProvider::Create(..., std::shared_ptr<Buffer> *data, ...) {
  const auto source = created_by_worker
                          ? plasma::flatbuf::ObjectSource::CreatedByWorker
                          : plasma::flatbuf::ObjectSource::RestoredFromStorage;
  Status status = store_client_->CreateAndSpillIfNeeded(
      object_id, owner_address, is_mutable, data_size,
      metadata ? metadata->Data() : nullptr, metadata ? metadata->Size() : 0,
      data, source, /*device_num=*/0);
  // IsObjectExists → status = OK（plasma 中已有此对象）
  if (status.IsObjectExists()) {
    status = Status::OK();
  }
  return status;
}
```

**Step 3: PlasmaClient::CreateAndSpillIfNeeded — 发送 IPC CreateRequest**

```cpp
// client.cc:215-260
Status PlasmaClient::CreateAndSpillIfNeeded(const ObjectID &object_id, ...) {
  uint64_t retry_with_request_id = 0;
  {
    std::unique_lock<std::recursive_mutex> guard(client_mutex_);
    // ★ try_immediately=false → server 将请求入 CreateRequestQueue，OOM 时触发 spill
    RAY_RETURN_NOT_OK(SendCreateRequest(store_conn_, object_id, owner_address,
                                        is_experimental_mutable_object,
                                        data_size, metadata_size, source,
                                        device_num,
                                        /*try_immediately=*/false));
    status = HandleCreateReply(object_id, is_experimental_mutable_object,
                               metadata, &retry_with_request_id, data);
  }
  // 如果 store full，轮询重试
  while (retry_with_request_id > 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(
        RayConfig::instance().object_store_full_delay_ms()));
    std::unique_lock<std::recursive_mutex> guard(client_mutex_);
    RAY_RETURN_NOT_OK(SendCreateRetryRequest(store_conn_, object_id, retry_with_request_id));
    status = HandleCreateReply(...);
  }
  return status;
}
```

**Step 4: Server 端 — PlasmaStore 收到 CreateRequest → 入队处理**

```cpp
// store.cc:380-424
case fb::MessageType::PlasmaCreateRequest: {
  // try_immediately == false → 入 CreateRequestQueue
  auto req_id = create_request_queue_.AddRequest(object_id, client, handle_create, object_size);
  ProcessCreateRequests();        // 尝试分配（可能触发 GC/spill/grace period/fallback）
  ReplyToCreateClient(client, object_id, req_id);
}

// ProcessRequests 最终调用 handle_create lambda → HandleCreateObjectRequest
```

**Step 5: HandleCreateObjectRequest → CreateObject（Create + AddToClientObjectIds 同函数）**

`CreateObject` 和 `AddToClientObjectIds` **在同一个函数中顺序调用，同一把 mutex 下完成，不可分割**：

```cpp
// store.cc:149-193
PlasmaError PlasmaStore::HandleCreateObjectRequest(const std::shared_ptr<Client> &client,
                                                   const std::vector<uint8_t> &message,
                                                   bool fallback_allocator,
                                                   PlasmaObject *object) {
  ReadCreateRequest(input, input_size, &object_info, &source, &device_num);
  auto error = CreateObject(object_info, source, client, fallback_allocator, object);
  return error;
}

PlasmaError PlasmaStore::CreateObject(const ray::ObjectInfo &object_info,
                                      fb::ObjectSource source,
                                      const std::shared_ptr<Client> &client,
                                      bool fallback_allocator,
                                      PlasmaObject *result) {
  // ★ Part A: 创建对象 — ref_count_ 初始 = 0（只分配内存，不递增 ref）
  auto pair = object_lifecycle_mgr_.CreateObject(object_info, source, fallback_allocator);
  auto entry = pair.first;
  if (entry == nullptr) { return pair.second; }
  entry->ToPlasmaObject(result, /*check_sealed=*/false);

  // ★ Part B: 紧接着注册到 client → AddReference → ref_count 0→1
  //    在同一把 mutex_ 下完成，中间不可能有其他线程操作此对象
  std::optional<MEMFD_TYPE> fallback_allocated_fd = std::nullopt;
  if (entry->GetAllocation().fallback_allocated_) {
    fallback_allocated_fd = entry->GetAllocation().fd_;
  }
  AddToClientObjectIds(object_info.object_id, fallback_allocated_fd, client);
  return PlasmaError::OK;
}
```

**为什么 Create 和 AddReference 不在 `CreateObjectInternal` 中合并**：

`CreateObjectInternal` 只负责"分配内存 + 创建 LocalObject 对象"，它被设计为**纯粹的内存操作**，不知道也不关心 client 是谁。ref_count 的递增是"client 注册"语义，属于 `AddToClientObjectIds` 的职责。这种分离让 `AddToClientObjectIds` 有**两个独立的调用点**：

| 调用点 | 位置 | 场景 | ref 变化 |
|--------|------|------|---------|
| `CreateObject` 中 | `store.cc:191` | 新建对象后立即注册 client（Part A → Part B 同函数） | 0→1（首次） |
| Get 请求回调中 | `store.cc:108` | 对象已存在，新 client 通过 Get 获取时注册 | N→N+1 |

**Step 6: ObjectStore vs ObjectLifecycleManager — 分层架构与 object_table_**

Plasma Store 内部分三层，各司其职：

```
┌──────────────────────────────────────────────────────────┐
│ PlasmaStore (store.cc)                                    │
│  - IPC 收发（CreateRequest/GetRequest/ReleaseRequest）    │
│  - client 管理（AddToClientObjectIds/RemoveFromClient...） │
│  - 调用 ObjectLifecycleManager 的接口                     │
├──────────────────────────────────────────────────────────┤
│ ObjectLifecycleManager (obj_lifecycle_mgr.cc)             │
│  - ref_count 操作（AddReference/RemoveReference）         │
│  - 生命周期控制（CreateObjectInternal/EvictObjects/      │
│    DeleteObjectInternal/SealObject）                      │
│  - 持有 object_store_ (unique_ptr<IObjectStore>)          │
│  - 持有 eviction_policy_ (unique_ptr<EvictionPolicy>)     │
├──────────────────────────────────────────────────────────┤
│ ObjectStore (object_store.cc)                             │
│  - 纯内存操作（CreateObject/SealObject/DeleteObject）     │
│  - 持有 object_table_ — 所有对象的存储表                  │
│  - 分配器调用（allocator_.Allocate/Free）                 │
│  - 不知道 ref_count、client、eviction 的存在              │
└──────────────────────────────────────────────────────────┘
```

**ObjectLifecycleManager 持有 ObjectStore 的方式**：

```cpp
// obj_lifecycle_mgr.h:161
std::unique_ptr<IObjectStore> object_store_;

// obj_lifecycle_mgr.cc:28-33 构造
ObjectLifecycleManager::ObjectLifecycleManager(IAllocator &allocator, ...)
    : object_store_(std::make_unique<ObjectStore>(allocator)),
      eviction_policy_(std::make_unique<EvictionPolicy>(*object_store_, allocator)),
      delete_object_callback_(std::move(delete_object_callback)),
      stats_collector_(std::make_unique<ObjectStatsCollector>()) {}
```

**object_table_ — 所有对象的存储表**：

```cpp
// object_store.h:59-100
class ObjectStore : public IObjectStore {
 public:
  const LocalObject *CreateObject(...) override;   // emplace 到 object_table_
  const LocalObject *GetObject(const ObjectID &) const override;  // find
  const LocalObject *SealObject(const ObjectID &) override;       // 修改 state_
  bool DeleteObject(const ObjectID &) override;    // erase + allocator_.Free

 private:
  IAllocator &allocator_;
  absl::flat_hash_map<ObjectID, std::unique_ptr<LocalObject>> object_table_;  // ★ 核心存储
};

// object_store.cc:30-64
const LocalObject *ObjectStore::CreateObject(const ray::ObjectInfo &object_info,
                                              plasma::flatbuf::ObjectSource source,
                                              bool fallback_allocate) {
  RAY_CHECK(!object_table_.contains(object_info.object_id));
  auto allocation = fallback_allocate ? allocator_.FallbackAllocate(object_size)
                                      : allocator_.Allocate(object_size);
  if (!allocation.has_value()) { return nullptr; }
  auto ptr = std::make_unique<LocalObject>(std::move(allocation.value()));
  // ★ LocalObject 构造时 ref_count_ = 0（common.h:106）
  auto entry = object_table_.emplace(object_info.object_id,
                                     std::move(ptr)).first->second.get();
  entry->object_info_ = object_info;
  entry->state_ = ObjectState::PLASMA_CREATED;  // 未 Seal
  entry->source_ = source;
  return entry;
}

// object_store.cc:66-71
const LocalObject *ObjectStore::GetObject(const ObjectID &object_id) const {
  auto it = object_table_.find(object_id);
  if (it == object_table_.end()) { return nullptr; }
  return it->second.get();
}

// object_store.cc:80-88
bool ObjectStore::DeleteObject(const ObjectID &object_id) {
  auto entry = GetMutableObject(object_id);
  if (entry == nullptr) { return false; }
  allocator_.Free(std::move(entry->allocation_));  // 释放内存（dlfree）
  object_table_.erase(object_id);                  // 从存储表移除
  return true;
}
```

**LocalObject 的所有字段**：

```cpp
// common.h:104-177
class LocalObject {
 public:
  explicit LocalObject(Allocation allocation)
      : allocation_(std::move(allocation)), ref_count_(0) {}  // ★ ref_count 初始 = 0

 private:
  Allocation allocation_;              // 内存分配信息（dlmalloc 指针 + fallback fd）
  ray::ObjectInfo object_info_;       // 对象元数据（id, size, owner 等）
  mutable int32_t ref_count_;         // ★ server ref —— "多少个 client 连接在用"
  int64_t create_time_;
  int64_t construct_duration_;
  ObjectState state_;                 // PLASMA_CREATED(1) / PLASMA_SEALED(2)
  plasma::flatbuf::ObjectSource source_;  // CreatedByWorker / ReceivedByPull / ...
};

// common.h:36-40
enum class ObjectState : int {
  PLASMA_CREATED = 1,  // 已创建未 Seal
  PLASMA_SEALED = 2,   // 已 Seal，可读
};
```

**三层的职责边界**：

| 操作 | ObjectStore | ObjectLifecycleManager | PlasmaStore |
|------|------------|----------------------|-------------|
| 分配内存 | `allocator_.Allocate` | - | - |
| 创建对象（`object_table_` emplace） | ★ | → 调用 ObjectStore | - |
| 递增 ref_count | - | ★ `AddReference` | → 调用 |
| 递减 ref_count | - | ★ `RemoveReference` | → 调用 |
| 从 LRU 移除/加入 | - | → 调用 EvictionPolicy | - |
| 淘汰对象（`object_table_` erase） | ★ `DeleteObject` | → 调用 | - |
| client 注册（AddToClientObjectIds） | - | - | ★ |
| client 移除（RemoveFromClientObjectIds） | - | - | ★ |

**Step 7: AddToClientObjectIds → AddReference（ref_count 0→1）**

```cpp
// store.cc:136-147
void PlasmaStore::AddToClientObjectIds(const ObjectID &object_id, ...,
                                       const std::shared_ptr<ClientInterface> &client) {
  auto &object_ids = client->GetObjectIDs();
  if (object_ids.find(object_id) != object_ids.end()) {
    return;  // 该 client 已持有此对象，不重复 +1
  }
  RAY_CHECK(object_lifecycle_mgr_.AddReference(object_id));  // ★ server ref_count++
  client->MarkObjectAsUsed(object_id, fallback_allocated_fd);
}

// obj_lifecycle_mgr.cc:128-145
bool ObjectLifecycleManager::AddReference(const ObjectID &object_id) {
  auto entry = object_store_->GetObject(object_id);  // 从 object_table_ 查找
  if (entry->ref_count_ == 0) {
    eviction_policy_->BeginObjectAccess(object_id);  // ★ 从 LRU 移除，不可淘汰
  }
  entry->ref_count_++;  // ★ server ref_count: 0 → 1
  return true;
}
```

**Step 8: Client 端 — HandleCreateReply → PlasmaMutableBuffer + InsertObjectInUse + IncrementObjectCount**

Create 返回的 buffer 是 **`PlasmaMutableBuffer`**，不是 `Get` 返回的 `PlasmaBuffer`。两者析构行为完全不同：

```cpp
// client.cc:35-46 — Get 返回的 PlasmaBuffer：析构自动 Release
class PlasmaBuffer : public SharedMemoryBuffer {
 public:
  PlasmaBuffer(std::shared_ptr<PlasmaClient> client, const ObjectID &object_id,
               const std::shared_ptr<Buffer> &buffer)
      : SharedMemoryBuffer(buffer, 0, buffer->Size()),
        client_(std::move(client)), object_id_(object_id) {}

  ~PlasmaBuffer() override { RAY_UNUSED(client_->Release(object_id_)); }
  // ★ 析构时自动调 Release → 可能触发 SendReleaseRequest
 private:
  std::shared_ptr<PlasmaClient> client_;
  ObjectID object_id_;
};

// client.cc:48-56 — Create 返回的 PlasmaMutableBuffer：析构不 Release
class PlasmaMutableBuffer : public SharedMemoryBuffer {
 public:
  PlasmaMutableBuffer(std::shared_ptr<PlasmaClient> client,
                       uint8_t *mutable_data, int64_t data_size)
      : SharedMemoryBuffer(mutable_data, data_size), client_(std::move(client)) {}
  // ★ 析构时什么都不做！不调用 Release
  // 保持 PlasmaClient 引用只是防止 client 先于 buffer 被销毁
 private:
  std::shared_ptr<PlasmaClient> client_;
  // 注意：没有 object_id_ 字段 — 因为它不会调 Release
};
```

**为什么 PlasmaMutableBuffer 析构不调 Release**：Create 返回的 buffer 是给调用方写入数据的，写入完成后必须显式调用 `Seal()` → `Release()`。如果 buffer 析构自动 Release，数据还没写完对象就被释放了。

**HandleCreateReply 完整代码**：

```cpp
// client.cc:222-281
Status PlasmaClient::HandleCreateReply(const ObjectID &object_id,
                                       bool is_experimental_mutable_object,
                                       const uint8_t *metadata,
                                       uint64_t *retry_with_request_id,
                                       std::shared_ptr<Buffer> *data) {
  // ... 接收 CreateReply flatbuffer，获取 store_fd, mmap ...

  // ★ 创建 PlasmaMutableBuffer（析构不调 Release）
  *data = std::make_shared<PlasmaMutableBuffer>(
      shared_from_this(),
      GetStoreFdAndMmap(store_fd, mmap_size) + object->data_offset,
      object->data_size);

  // ★ InsertObjectInUse: count = 1
  InsertObjectInUse(object_id, std::move(object), /*is_sealed=*/false);

  // ★ IncrementObjectCount: count = 2
  // 注释原文: "We increment the count a second time (and the corresponding
  // decrement will happen in a PlasmaClient::Release call in plasma_seal)
  // so even if the buffer returned by PlasmaClient::Create goes out of scope,
  // the object does not get released before the call to PlasmaClient::Seal
  // happens."
  IncrementObjectCount(object_id);

  if (is_experimental_mutable_object) {
    IncrementObjectCount(object_id);  // count = 3
  }
  return Status::OK();
}
```

**client count = 2 的含义**：

| count | 来源 | 保护什么 | 何时释放 | 对 server ref 影响 |
|-------|------|---------|---------|-----------------|
| 1 | `InsertObjectInUse` | "对象正在被 client 使用" — 基础引用 | 最终显式 `Release()` | count 归零时 → `SendReleaseRequest` → server `RemoveReference` |
| 2 | `IncrementObjectCount` | "Seal 前保护" — 即使 `PlasmaMutableBuffer` 出作用域（**它析构不调 Release！**），或者用户意外多调一次 Release，count 也不会归零 | `Seal()` 内部调用 `Release()` | 无（只影响 client count，不影响 server ref） |
| 3 (mutable) | 额外 `IncrementObjectCount` | 可变对象额外保护 | 可变对象专用 Release | 无 |

**重要澄清**：`PlasmaMutableBuffer` 析构**不调 Release**，所以它出作用域时 client count 不变。count=2 的真正保护场景是：如果用户在 Seal 之前**显式调用** `Release()`，count 从 2→1（而非 1→0），不会触发 `SendReleaseRequest`。

**Create → Seal → Release 完整时序**：

```
步骤  Client 操作                         client count  server ref  LRU状态   说明
─────────────────────────────────────────────────────────────────────────────────────
1     InsertObjectInUse                   0→1           0→1         移除       Create reply
2     IncrementObjectCount                1→2           1           移除       Seal 前保护
3     PlasmaMutableBuffer 出作用域        2             1           移除       ★ 析构不调 Release！
4     Seal() → 内部 Release               2→1           1           移除       减"seal 保护"引用
5     用户显式 Release                    1→0           1→0         加入       ★ 触发 SendReleaseRequest
      → MarkObjectUnused + SendReleaseRequest
      → server RemoveReference → EndObjectAccess
```

**CreateAndSpillIfNeeded vs TryCreateImmediately**：两者 client 端 ref count 逻辑完全相同（都走 `HandleCreateReply` → count=2）。区别在 server 端调度：
- `CreateAndSpillIfNeeded`：`try_immediately=false`，请求入 `CreateRequestQueue`，OOM 时触发 spill/GC/grace period/fallback
- `TryCreateImmediately`：`try_immediately=true`，直接尝试分配（含 fallback），失败即返回

**Step 8a: Evict 如何看 ref_count — 淘汰的充要条件**

Evict 不直接"检查" ref_count==0，而是通过 **LRU cache 的成员关系**间接保证：

```
ref_count > 0 的对象 → BeginObjectAccess → 从 LRU cache 移除 → 不在 LRU 中 → 不可能被选中
ref_count = 0 的对象 → EndObjectAccess → 加入 LRU cache → 在 LRU 中 → 可被选中
```

**EvictionPolicy 三层调用链**：

```cpp
// 1. CreateObjectInternal 中需要空间时调用
// obj_lifecycle_mgr.cc:195-198
int64_t space_needed = eviction_policy_->RequireSpace(
    object_info.GetObjectSize(), objects_to_evict);
EvictObjects(objects_to_evict);

// 2. RequireSpace 计算需要释放多少空间
// eviction_policy.cc:120-134
int64_t EvictionPolicy::RequireSpace(int64_t size,
                                     std::vector<ObjectID> &objects_to_evict) {
  int64_t required_space = Allocated() + size - GetFootprintLimit();
  int64_t space_to_free = std::max(required_space, GetFootprintLimit() / 5);  // 最少淘汰 20%
  int64_t num_bytes_evicted = ChooseObjectsToEvict(space_to_free, objects_to_evict);
  return required_space - num_bytes_evicted;
}

// 3. ChooseObjectsToEvict 从 LRU cache 中选择对象
// eviction_policy.cc:103-112
int64_t EvictionPolicy::ChooseObjectsToEvict(int64_t num_bytes_required,
                                             std::vector<ObjectID> &objects_to_evict) {
  int64_t bytes_evicted = cache_.ChooseObjectsToEvict(num_bytes_required, objects_to_evict);
  for (auto &object_id : objects_to_evict) {
    cache_.Remove(object_id);  // 从 LRU 中移除（已选中淘汰）
  }
  return bytes_evicted;
}

// 4. LRUCache 从链表尾部（最久未使用）选择
// eviction_policy.cc:77-87
int64_t LRUCache::ChooseObjectsToEvict(int64_t num_bytes_required,
                                       std::vector<ObjectID> &objects_to_evict) {
  int64_t bytes_evicted = 0;
  auto it = item_list_.end();
  while (bytes_evicted < num_bytes_required && it != item_list_.begin()) {
    it--;
    objects_to_evict.push_back(it->first);   // 选中最久未使用的
    bytes_evicted += it->second;
  }
  return bytes_evicted;
}

// 5. EvictObjects 执行淘汰 — 双重 RAY_CHECK 保证安全
// obj_lifecycle_mgr.cc:225-237
void ObjectLifecycleManager::EvictObjects(const std::vector<ObjectID> &object_ids) {
  for (const auto &object_id : object_ids) {
    auto entry = object_store_->GetObject(object_id);
    RAY_CHECK(entry != nullptr) << "must be in the object table";
    RAY_CHECK(entry->state_ == ObjectState::PLASMA_SEALED)
        << "must have been sealed";              // ★ 只淘汰已 Seal 的
    RAY_CHECK(entry->ref_count_ == 0)
        << "no clients currently using it";      // ★ ref 必须为 0
    DeleteObjectInternal(object_id);              // 物理删除 + 释放内存
  }
}
```

**淘汰三要素（缺一不可）**：

| 条件 | 检查点 | 保证机制 |
|------|--------|---------|
| ref_count == 0 | `EvictObjects` RAY_CHECK | LRU cache 只包含 ref=0 的对象（BeginObjectAccess 移除 ref>0 的） |
| sealed | `EvictObjects` RAY_CHECK | `EndObjectAccess` 中也有 `RAY_CHECK(Sealed())` |
| 在 LRU cache 中 | `ChooseObjectsToEvict` 只从 LRU 选 | `BeginObjectAccess` 将 ref>0 的从 LRU 移除 |

**三个操作与 LRU 的关系**：

```cpp
// AddReference (ref 0→1) → BeginObjectAccess → 从 LRU 移除
void EvictionPolicy::BeginObjectAccess(const ObjectID &object_id) {
  cache_.Remove(object_id);                      // 不可淘汰
  pinned_memory_bytes_ += GetObjectSize(object_id);
}

// RemoveReference (ref →0) → EndObjectAccess → 加入 LRU
void EvictionPolicy::EndObjectAccess(const ObjectID &object_id) {
  auto size = GetObjectSize(object_id);
  cache_.Add(object_id, size);                   // 可淘汰
  pinned_memory_bytes_ -= size;
}

// Seal — 不改变 LRU 状态（对象创建时 ref=0，不在 LRU；AddReference 后 ref=1，也不在 LRU）
// 只有 RemoveReference 使 ref→0 时才加入 LRU
```

### 7.3 SealReturnObject

```cpp
// core_worker.cc:3220
Status CoreWorker::SealReturnObject(const ObjectID &return_id,
                                    const std::shared_ptr<RayObject> &return_object,
                                    const ObjectID &generator_id,
                                    const rpc::Address &owner_address) {
  if (return_object->GetData() != nullptr &&
      return_object->GetData()->IsPlasmaBuffer()) {
    // 大对象在plasma中 → SealExisting
    status = SealExisting(return_id, true, generator_id, owner_address_ptr);
  }
  // 小对象在LocalMemoryBuffer中 → IsPlasmaBuffer()==false → 不做任何plasma操作
  return status;
}
```

### 7.4 SealExisting

```cpp
// core_worker.cc:1198
Status CoreWorker::SealExisting(const ObjectID &object_id,
                                bool pin_object, ...) {
  RAY_RETURN_NOT_OK(plasma_store_provider_->Seal(object_id));
  if (pin_object) {
    local_raylet_rpc_client_->PinObjectIDs(
        owner_address, {object_id}, generator_id,
        [this, object_id](const Status &status, const rpc::PinObjectIDsReply &reply) {
          if (!status.ok()) { return; }
          // 回调Release: 保护转移到raylet后，worker释放自己的引用
          if (!plasma_store_provider_->Release(object_id).ok()) { ... }
        });
  } else {
    RAY_RETURN_NOT_OK(plasma_store_provider_->Release(object_id));
    reference_counter_->FreePlasmaObjects({object_id});
  }
}
```

### 7.5 AllocateReturnObject → SealExisting → PinObjectIDs 完整代码调用与 ref 时序

**Python → C++ 调用链**：

```python
# _raylet.pyx:4190 store_task_output
# Step 1: Allocate
CCoreWorkerProcess.GetCoreWorker().AllocateReturnObject(
    return_id, data_size, metadata, contained_id, caller_address, ...)

# Step 2: Write (memcpy to plasma buffer)
(<SerializedObject>serialized_object).write_to(Buffer.make(return_ptr.get().GetData()))

# Step 3: Seal + Pin
CCoreWorkerProcess.GetCoreWorker().SealReturnObject(
    return_id, return_ptr[0], generator_id, caller_address)
```

**C++ 完整调用链**：

```
AllocateReturnObject (core_worker.cc:2921)
  └─ CreateExisting (core_worker.cc:1148)
       └─ PlasmaStoreProvider::Create (plasma_store_provider.cc:126)
            └─ PlasmaClient::CreateAndSpillIfNeeded (client.cc:215)
                 ├─ SendCreateRequest [IPC → server]
                 └─ HandleCreateReply (client.cc:136)
                      ├─ PlasmaMutableBuffer 创建（析构不调 Release）
                      ├─ InsertObjectInUse(count=1, is_sealed=false)
                      └─ IncrementObjectCount(count=2)

SealReturnObject (core_worker.cc:3224)
  └─ SealExisting (core_worker.cc:1204)
       ├─ PlasmaStoreProvider::Seal (plasma_store_provider.cc:172)
       │    └─ PlasmaClient::Seal (client.cc:569)
       │         ├─ is_sealed = true
       │         ├─ SendSealRequest [IPC → server]
       │         │    └─ server: SealObject (state_ = PLASMA_SEALED)
       │         │         └─ add_object_callback_ → HandleObjectAdded + HandleObjectLocal
       │         └─ Release (client.cc:490) → count 2→1 (不发 SendReleaseRequest)
       │
       └─ PinObjectIDs [异步 RPC → raylet]
            └─ NodeManager::HandlePinObjectIDs (node_manager.cc:2661)
                 ├─ GetObjectsFromPlasma (node_manager.cc:2634)
                 │    └─ store_client_->Get() [PlasmaClient #2]
                 │         ├─ InsertObjectInUse(count=1, is_sealed=true)
                 │         ├─ IncrementObjectCount(count=2)
                 │         └─ [IPC: GetRequest] → server AddReference (ref+1)
                 │              → 返回 RayObject（持有 PlasmaBuffer）
                 │
                 └─ PinObjectsAndWaitForFree (local_object_manager.cc:31)
                      ├─ local_objects_.emplace
                      ├─ pinned_objects_.emplace(id, std::move(RayObject))
                      └─ Subscribe(WorkerObjectEviction) → 等待 owner 释放

PinObjectIDs 回调（raylet 成功后触发）:
  └─ PlasmaStoreProvider::Release (plasma_store_provider.cc:191)
       └─ PlasmaClient::Release (client.cc:490) [Worker PlasmaClient #1]
            ├─ count 1→0
            ├─ MarkObjectUnused (从 objects_in_use_ 移除)
            └─ SendReleaseRequest [IPC → server]
                 └─ RemoveFromClientObjectIds → RemoveReference (server ref-1)
```

**三端 ref 完整时序（Worker PlasmaClient / Raylet PlasmaClient / server ref）**：

```
步骤  代码位置                                Worker    Raylet    server ref  说明
                                              client    client
                                              count     count
────────────────────────────────────────────────────────────────────────────────────────
1     AllocateReturnObject→Create
      ├ InsertObjectInUse                       1         -          1       server AddReference
      ├ IncrementObjectCount                    2         -          1
      └ [IPC: CreateRequest] → AddToClientObjectIds → AddReference

2     Python write_to (memcpy)                  2         -          1       数据写入

3     SealReturnObject→SealExisting
      ├ Seal: is_sealed=true
      ├ SendSealRequest → server SealObject
      │  (state_ = PLASMA_SEALED)
      │  → add_object_callback_ (通知 raylet)
      └ Release (count 2→1)                     1         -          1       不发 ReleaseRequest

4     PinObjectIDs [异步RPC → raylet]
      ├ GetObjectsFromPlasma
      │  └ PlasmaClient #2::Get
      │     ├ InsertObjectInUse                 1         1          2       server AddReference
      │     ├ IncrementObjectCount              1         2          2       (raylet 加入)
      │     └ [IPC: GetRequest] → AddReference
      │        → 返回 RayObject（持有 PlasmaBuffer）
      │
      └ PinObjectsAndWaitForFree
         ├ pinned_objects_.emplace(id, RayObject)  1       2          2
         └ Subscribe(WorkerObjectEviction)

5     PinObjectIDs 回调 → Worker Release
      ├ PlasmaClient #1::Release (count 1→0)    0         2          2
      ├ MarkObjectUnused (从 objects_in_use_ 移除)
      └ SendReleaseRequest [IPC → server]
         └ RemoveFromClientObjectIds → RemoveReference
            (server ref 2→1)                     已移除     2          1

      ──── 最终稳态 ────
      Worker client: 已移除（无引用）
      Raylet client: count=2（pinned_objects_ 持有 RayObject → PlasmaBuffer）
      server ref: 1（raylet pin 持有）→ spillable（ref=1 可 spill 不可 evict）
      owner ref: pinned_at_node_id_ = 本节点
```

**关键交接点**：步骤 5 中 Worker Release 后 server ref 从 2→1（不是 0！），raylet 的 PinObjectsAndWaitForFree 仍然持有对象。只有 owner GC → `ReleaseFreedObject` 释放 raylet 的 pin 时，server ref 才 1→0 → 加入 LRU → 可淘汰。

---

## 8. 完整链路 3：PinExistingReturnObject（plasma 中已有对象）

**场景**：reconstruction 重执行时，返回值已在 plasma 中存在（raylet 之前 pin 的 ref=1）

```cpp
// core_worker.cc:3294
bool CoreWorker::PinExistingReturnObject(const ObjectID &return_id, ...) {
  // 1. 临时建立引用关系，否则Get不知道owner地址
  reference_counter_->AddLocalReference(return_id, "<temporary>");
  reference_counter_->AddBorrowedObject(return_id, ObjectID::Nil(), owner_address);
  auto owner_addresses = reference_counter_->GetOwnerAddresses({return_id});

  // 2. 非阻塞Get: 从plasma取对象
  Status status = plasma_store_provider_->Get({return_id}, owner_addresses, 0, &result_map);
  // Get → PlasmaClient::Get → InsertObjectInUse(count=1)
  //     → [IPC: GetRequest] → AddToClientObjectIds → AddReference (server ref +1)

  // 3. 释放临时引用（不影响plasma）
  RemoveLocalReference(return_id);

  if (result_map.contains(return_id)) {
    *return_object = std::move(result_map[return_id]);

    // 4. 异步Pin: 让raylet也持有一份
    local_raylet_rpc_client_->PinObjectIDs(
        owner_address, {return_id}, generator_id,
        [return_id, pinned_return_object=*return_object](...) {
          // 回调: pinned_return_object 释放 → worker的RayObject析构
          // → PlasmaClient::Release → count 1→0 → SendReleaseRequest
          // → server ref -1
        });
    return true;
  }
  return false;
}
```

### ref 变化时序

```
步骤  代码位置                                        client count   server ref
────────────────────────────────────────────────────────────────────────────────
1     Get(return_id, timeout=0)                        worker:0→1     原值+1
      → InsertObjectInUse(count=1)
      → [IPC: GetRequest] → AddToClientObjectIds → AddReference

2     PinObjectIDs(异步RPC)                            worker:1       原值+2
      → raylet GetObjectsFromPlasma
        → AddToClientObjectIds → AddReference (raylet加入)

3     回调 → pinned_return_object 释放                  worker:1→0     原值+1
      → RayObject析构 → PlasmaClient::Release
      → count 1→0 → MarkObjectUnused + SendReleaseRequest
      → [IPC: ReleaseRequest] → RemoveReference

      ──── 最终稳态 ────
      server ref = 原值 + 1 (raylet pin新增的)
      如果原值=1(reconstruction前raylet pin的), 最终=1
      (PinObjectsAndWaitForFree发现local_objects_已有, 不重复pin)
```

---

## 9. 完整链路 4：Pull（worker 主动拉取远程对象）

### 9.0 PlasmaClient 实例分布与共享关系

raylet 进程中有 **两个独立的 PlasmaClient 实例**，各自通过独立的 Unix socket 连接到同一个 Plasma Store 进程：

| 实例 | 创建位置 | 持有者 | 连接时机 | 用途 |
|------|---------|--------|---------|------|
| PlasmaClient #1 | `main.cc:861`（ObjectManager 构造参数） | `ObjectManager` → `ObjectBufferPool::store_client_` | `object_manager.cc:137` Init | 对象 chunk 传输（Create、Write、Seal、Release、Abort、Get、Delete） |
| PlasmaClient #2 | `main.cc:1011`（NodeManager 构造参数） | `NodeManager::store_client_` | `node_manager.cc:260` Start | Get 对象用于 pin、创建 error 对象、内存查询 |

**关键区别**：两个 PlasmaClient 是独立的 Plasma client 连接，各自维护独立的 `objects_in_use_`（client count）。同一个对象在两个 client 中可以同时持有引用，server ref_count 是两者引用之和。

**PlasmaClient #2 的共享路径**：

```
NodeManager::store_client_ (PlasmaClient #2)
  ├─ NodeManager::GetObjectsFromPlasma()
  │    → store_client_->Get() → 获取 RayObject（refcount++）
  │    被以下组件通过回调/直接调用使用：
  │    ├─ PullManager::pin_object_ 回调 (main.cc:843)
  │    ├─ LocalLeaseManager::get_lease_arguments_ 回调 (main.cc:959)
  │    └─ NodeManager::HandlePinObjectIDs (node_manager.cc:2578)
  │
  └─ 间接使用者（通过 NodeManager 中转）：
       ├─ LocalObjectManager::PinObjectsAndWaitForFree
       │    （HandlePinObjectIDs 调用 GetObjectsFromPlasma 后传入 RayObject）
       ├─ PullManager::TryPinObject
       │    （pin_object_ 回调 → GetObjectsFromPlasma → 返回 RayObject）
       └─ LocalLeaseManager::PinLeaseArgsIfMemoryAvailable
            （get_lease_arguments_ 回调 → GetObjectsFromPlasma → 返回 RayObject）
```

**PlasmaClient #1 的使用路径**：

```
ObjectBufferPool::store_client_ (PlasmaClient #1)
  ├─ 远端 Pull 到达时：Create → Write chunks → Seal → Release
  │    （buffer_pool.cc:225 EnsureBufferExists → CreateAndSpillIfNeeded）
  └─ 远端 Push 请求时：Get → 读取对象数据 → 发送 chunks
       （buffer_pool.cc:100 CreateObjectReader → store_client_->Get）
```

**三者持有 RayObject 的关系**：

| 持有者 | 存储位置 | PlasmaClient | server ref 贡献 | 释放方式 |
|--------|---------|-------------|----------------|---------|
| PullManager::pinned_objects_ | `pull_manager.cc` | #2 (通过 pin_object_ 回调) | +1 | UnpinObject → RayObject 析构 → Release |
| LocalLeaseManager::pinned_lease_arguments_ | `local_lease_manager.cc` | #2 (通过 get_lease_arguments_ 回调) | +1 | ReleaseLeaseArgs → RayObject 析构 → Release |
| LocalObjectManager::pinned_objects_ | `local_object_manager.cc` | #2 (HandlePinObjectIDs 中 Get) | +1 | ReleaseFreedObject → RayObject 析构 → Release |
| ObjectBufferPool (临时) | `object_buffer_pool.cc` | #1 | +1(创建时)/0(Seal+Release后) | Seal + Release 归零 |

**同对象的引用叠加示例**：一个 Pull 来的对象在 lease 调度期间，PullManager 和 PinLeaseArgs 各持有一个 RayObject，两者都通过 PlasmaClient #2 的 `Get()` 获得引用，server ref_count = 2。CancelPull 释放 PullManager 的引用后 ref 降到 1，PinLeaseArgs 仍保护对象不被 LRU。

### 9.0a PlasmaClient::Get() — client count 递增的完整代码链路

三种 pin 机制获取 RayObject 时，最终都经过同一条代码路径：`GetObjectsFromPlasma` → `PlasmaClient::Get()` → IPC `GetRequest` → server `AddReference`。

**Step 1: 三种回调定义（main.cc）**

```cpp
// PullManager::pin_object_ — main.cc:844
[&](const ray::ObjectID &object_id) {
  std::vector<ray::ObjectID> object_ids = {object_id};
  std::vector<std::unique_ptr<ray::RayObject>> results;
  std::unique_ptr<ray::RayObject> result;
  if (node_manager->GetObjectsFromPlasma(object_ids, &results) &&
      results.size() > 0) {
    result = std::move(results[0]);
  }
  return result;
},

// LocalLeaseManager::get_lease_arguments_ — main.cc:993
[&](const std::vector<ray::ObjectID> &object_ids,
    std::vector<std::unique_ptr<ray::RayObject>> *results) {
  return node_manager->GetObjectsFromPlasma(object_ids, results);
},

// HandlePinObjectIDs — node_manager.cc:2597（直接调用）
std::vector<std::unique_ptr<RayObject>> results;
if (!GetObjectsFromPlasma(object_ids, &results)) { ... }
```

**Step 2: GetObjectsFromPlasma — NodeManager 的统一入口**

```cpp
// node_manager.cc:2561-2577
bool NodeManager::GetObjectsFromPlasma(
    const std::vector<ObjectID> &object_ids,
    std::vector<std::unique_ptr<RayObject>> *results) {
  std::vector<plasma::ObjectBuffer> plasma_results;
  if (!store_client_->Get(object_ids, /*timeout_ms=*/0, &plasma_results).ok()) {
    return false;                              // PlasmaClient #2
  }
  for (const auto &plasma_result : plasma_results) {
    if (plasma_result.data == nullptr) {
      results->push_back(nullptr);
    } else {
      // 用 plasma buffer 构造 RayObject
      // plasma_result.data 是 SharedMemoryBuffer::Slice → 持有 PlasmaBuffer
      results->emplace_back(std::unique_ptr<RayObject>(
          new RayObject(plasma_result.data, plasma_result.metadata, {})));
    }
  }
  return true;
}
```

**Step 3: PlasmaClient::Get() → GetBuffers()**

```cpp
// client.cc:469-474
Status PlasmaClient::Get(const std::vector<ObjectID> &object_ids,
                         int64_t timeout_ms,
                         std::vector<ObjectBuffer> *out) {
  std::lock_guard<std::recursive_mutex> guard(client_mutex_);
  *out = std::vector<ObjectBuffer>(num_objects);
  return GetBuffers(object_ids.data(), num_objects, timeout_ms, out->data());
}

// client.cc:327-336 — 发送 IPC GetRequest
RAY_RETURN_NOT_OK(SendGetRequest(store_conn_, &object_ids[0], num_objects, timeout_ms));

// client.cc:370-392 — 收到 reply 后处理每个对象
for (int64_t i = 0; i < num_objects; ++i) {
  if (object->data_size != -1) {
    if (objects_in_use_.find(received_object_ids[i]) == objects_in_use_.end()) {
      // ★ 首次引用此对象：count 0→1
      InsertObjectInUse(received_object_ids[i], std::move(object), /*is_sealed=*/true);
    } else {
      // ★ 已有引用：count +=1
      IncrementObjectCount(received_object_ids[i]);
    }
    // 创建 PlasmaBuffer（析构时自动调用 Release）
    auto physical_buf = std::make_shared<PlasmaBuffer>(
        shared_from_this(),
        object_ids[i],
        std::make_shared<SharedMemoryBuffer>(
            data + object_entry->object.data_offset,
            object_entry->object.data_size + object_entry->object.metadata_size));
    object_buffers[i].data =
        SharedMemoryBuffer::Slice(physical_buf, 0, object_entry->object.data_size);
    object_buffers[i].metadata =
        SharedMemoryBuffer::Slice(physical_buf,
                                  object_entry->object.data_size,
                                  object_entry->object.metadata_size);
  }
}
```

**Step 4: InsertObjectInUse / IncrementObjectCount**

```cpp
// client.cc:110-125 — 首次引用
void PlasmaClient::InsertObjectInUse(const ObjectID &object_id,
                                     std::unique_ptr<PlasmaObject> object,
                                     bool is_sealed) {
  auto inserted =
      objects_in_use_.insert({object_id, std::make_unique<ObjectInUseEntry>()});
  auto it = inserted.first;
  it->second->object = std::move(*object);
  it->second->count = 1;       // ← client count 初始化为 1
  it->second->is_sealed = is_sealed;
}

// client.cc:126-133 — 再次引用
void PlasmaClient::IncrementObjectCount(const ObjectID &object_id) {
  auto object_entry = objects_in_use_.find(object_id);
  RAY_CHECK(object_entry != objects_in_use_.end());
  object_entry->second->count += 1;  // ← client count 递增
}
```

**Step 5: Server 端 — AddToClientObjectIds → AddReference**

```cpp
// store.cc:108-109 — GetRequest 回调触发
[this](const ObjectID &object_id, ..., const auto &request) {
  this->AddToClientObjectIds(object_id, fallback_allocated_fd, request->client_);
}

// store.cc:136-145
void PlasmaStore::AddToClientObjectIds(const ObjectID &object_id, ...,
                                       const std::shared_ptr<ClientInterface> &client) {
  auto &object_ids = client->GetObjectIDs();
  if (object_ids.find(object_id) != object_ids.end()) {
    return;  // 该 client 已持有此对象，不重复 +1
  }
  RAY_CHECK(object_lifecycle_mgr_.AddReference(object_id));  // ← server ref_count++
  client->MarkObjectAsUsed(object_id, fallback_allocated_fd);
}

// obj_lifecycle_mgr.cc:128-145
bool ObjectLifecycleManager::AddReference(const ObjectID &object_id) {
  auto entry = object_store_->GetObject(object_id);
  if (entry->ref_count_ == 0) {
    eviction_policy_->BeginObjectAccess(object_id);  // 从 LRU 移除，不可淘汰
  }
  entry->ref_count_++;  // ← server ref_count 递增
  return true;
}
```

**PlasmaBuffer — RayObject 析构时自动 Release 的桥梁**

```cpp
// client.cc:40-52
class PlasmaBuffer : public SharedMemoryBuffer {
 public:
  PlasmaBuffer(std::shared_ptr<PlasmaClient> client,
               const ObjectID &object_id,
               const std::shared_ptr<Buffer> &buffer)
      : SharedMemoryBuffer(buffer, 0, buffer->Size()),
        client_(std::move(client)),
        object_id_(object_id) {}

  ~PlasmaBuffer() override { RAY_UNUSED(client_->Release(object_id_)); }
  // ★ PlasmaBuffer 是 SharedMemoryBuffer 的父 buffer，
  //   RayObject 的 data_/metadata_ 是它的 Slice（shared_ptr 持有父引用）
  //   当所有 Slice 和父 buffer 的 shared_ptr 归零时，~PlasmaBuffer 触发 Release

 private:
  std::shared_ptr<PlasmaClient> client_;  // 持有 PlasmaClient 引用防止先释放
  ObjectID object_id_;
};
```

### 9.0b PlasmaClient::Release() — client count 递减的完整代码链路

三种 pin 机制释放 RayObject 时，最终都经过：`unique_ptr<RayObject>` 析构 → `shared_ptr<Buffer>` 归零 → `~PlasmaBuffer()` → `PlasmaClient::Release()` → IPC `ReleaseRequest` → server `RemoveReference`。

**Step 1: RayObject 析构链**

```
unique_ptr<RayObject> 被销毁（erase from pinned_objects_ / pinned_lease_arguments_）
  → RayObject 析构（编译器生成，无显式析构函数）
    → shared_ptr<Buffer> data_ 引用计数递减
    → shared_ptr<Buffer> metadata_ 引用计数递减
      → 如果 data_ 和 metadata_ 是最后持有 PlasmaBuffer 的引用：
        → PlasmaBuffer 析构
          → ~PlasmaBuffer() { client_->Release(object_id_); }
```

**Step 2: PlasmaClient::Release()**

```cpp
// client.cc:487-530
Status PlasmaClient::Release(const ObjectID &object_id) {
  std::lock_guard<std::recursive_mutex> guard(client_mutex_);
  const auto object_entry = objects_in_use_.find(object_id);
  RAY_CHECK(object_entry != objects_in_use_.end());

  object_entry->second->count -= 1;  // ← client count 递减
  RAY_CHECK_GE(object_entry->second->count, 0);

  if (object_entry->second->count == 0) {
    // count 归零 → 该 client 不再使用此对象
    RAY_RETURN_NOT_OK(MarkObjectUnused(object_id));   // 从 objects_in_use_ 移除
    RAY_RETURN_NOT_OK(SendReleaseRequest(store_conn_, object_id, may_unmap));  // IPC
  }
  return Status::OK();
}

// client.cc:480-485
Status PlasmaClient::MarkObjectUnused(const ObjectID &object_id) {
  auto object_entry = objects_in_use_.find(object_id);
  RAY_CHECK_EQ(object_entry->second->count, 0);
  objects_in_use_.erase(object_id);  // 完全移除
  return Status::OK();
}
```

**Step 3: Server 端 — RemoveFromClientObjectIds → RemoveReference**

```cpp
// store.cc:434-448 — 处理 PlasmaReleaseRequest
case fb::MessageType::PlasmaReleaseRequest: {
  ObjectID object_id;
  ReadReleaseRequest(input, input_size, &object_id, &may_unmap);
  ReleaseObject(object_id, client);  // → RemoveFromClientObjectIds
}

// store.cc:265-270
bool PlasmaStore::ReleaseObject(const ObjectID &object_id,
                                const std::shared_ptr<Client> &client) {
  return RemoveFromClientObjectIds(object_id, client);
}

// store.cc:247-259
bool PlasmaStore::RemoveFromClientObjectIds(const ObjectID &object_id,
                                            const std::shared_ptr<Client> &client) {
  auto &object_ids = client->GetObjectIDs();
  auto it = object_ids.find(object_id);
  if (it != object_ids.end()) {
    client->MarkObjectAsUnused(object_id);
    object_lifecycle_mgr_.RemoveReference(object_id);  // ← server ref_count--
    return should_unmap;
  }
  return false;
}

// obj_lifecycle_mgr.cc:148-168
bool ObjectLifecycleManager::RemoveReference(const ObjectID &object_id) {
  auto entry = object_store_->GetObject(object_id);
  entry->ref_count_--;  // ← server ref_count 递减
  if (entry->ref_count_ > 0) {
    return true;
  }
  // ref_count_ == 0 → 可淘汰
  eviction_policy_->EndObjectAccess(object_id);  // 加入 LRU → 可淘汰
  // ...
}
```

### 9.0c 三种 Pin 机制触发 Get/Release 的代码路径对比

**路径1: PullManager::TryPinObject**

```
Pin:
  pull_manager.cc:602  pin_object_(object_id)
  → main.cc:844 lambda → node_manager->GetObjectsFromPlasma()
  → node_manager.cc:2569  store_client_->Get()             [PlasmaClient #2]
  → client.cc:390  InsertObjectInUse(count=1)              [client count 0→1]
  → [IPC: GetRequest] → store.cc:144  AddReference         [server ref +1]
  → 返回 unique_ptr<RayObject> → pinned_objects_[id] = std::move(ref)

Unpin:
  pull_manager.cc:628  pinned_objects_.erase(it)           [unique_ptr<RayObject> 销毁]
  → ~RayObject → shared_ptr<Buffer> 归零 → ~PlasmaBuffer()
  → client.cc:500  count -= 1 (1→0)                       [client count →0]
  → client.cc:517  MarkObjectUnused + SendReleaseRequest
  → [IPC: ReleaseRequest] → store.cc:256  RemoveReference  [server ref -1]
  → 如果 ref=0 → EndObjectAccess → 加入 LRU
```

**路径2: PinLeaseArgs**

```
Pin:
  local_lease_manager.cc:789  get_lease_arguments_(deps, &args)
  → main.cc:993 lambda → node_manager->GetObjectsFromPlasma()
  → node_manager.cc:2569  store_client_->Get()             [PlasmaClient #2]
  → client.cc:390  InsertObjectInUse(count=1)              [client count 0→1]
  → [IPC: GetRequest] → store.cc:144  AddReference         [server ref +1]
  → local_lease_manager.cc:849  pinned_lease_arguments_.emplace(dep, {RayObject, refcount=0})
  → local_lease_manager.cc:853  refcount++ (每个使用此参数的 lease)

Unpin:
  local_lease_manager.cc:873  refcount--                   [逻辑 refcount 递减]
  → if refcount == 0:
    local_lease_manager.cc:877  pinned_lease_arguments_.erase(arg_it)  [unique_ptr 销毁]
    → ~RayObject → shared_ptr<Buffer> 归零 → ~PlasmaBuffer()
    → client.cc:500  count -= 1 (1→0)                      [client count →0]
    → client.cc:517  MarkObjectUnused + SendReleaseRequest
    → [IPC: ReleaseRequest] → store.cc:256  RemoveReference [server ref -1]
    → 如果 ref=0 → EndObjectAccess → 加入 LRU
```

**路径3: PinObjectsAndWaitForFree**

```
Pin:
  node_manager.cc:2597  GetObjectsFromPlasma(object_ids, &results)
  → node_manager.cc:2569  store_client_->Get()             [PlasmaClient #2]
  → client.cc:390  InsertObjectInUse(count=1)              [client count 0→1]
  → [IPC: GetRequest] → store.cc:144  AddReference         [server ref +1]
  → local_object_manager.cc:67  pinned_objects_.emplace(id, std::move(object))
  → local_object_manager.cc:57-98  订阅 WorkerObjectEviction

Unpin:
  local_object_manager.cc:135  pinned_objects_.erase(pinned_objects_it)  [unique_ptr 销毁]
  → ~RayObject → shared_ptr<Buffer> 归零 → ~PlasmaBuffer()
  → client.cc:500  count -= 1 (1→0)                       [client count →0]
  → client.cc:517  MarkObjectUnused + SendReleaseRequest
  → [IPC: ReleaseRequest] → store.cc:256  RemoveReference  [server ref -1]
  → 如果 ref=0 → EndObjectAccess → 加入 LRU
```

**三种路径的核心共同点**：

1. 都通过 `PlasmaClient #2` 的 `Get()` 获取 RayObject → `InsertObjectInUse(count=1)` + server `AddReference`
2. 都通过 `unique_ptr<RayObject>` 销毁 → `~PlasmaBuffer()` → `PlasmaClient::Release()` + server `RemoveReference`
3. PlasmaClient 只看到 client count（每个 client 独立计数），不知道 RayObject 被谁持有
4. Server 只看到 ref_count（所有 client 引用之和），不知道哪个 client 持有

**关键差异**：PlasmaClient #2 的 `Get()` 每次调用对同一对象都会 `IncrementObjectCount`（如果已在 `objects_in_use_` 中），所以同一 client 连续 Get 同一对象，client count 会叠加。但三种机制各持有一个 `unique_ptr<RayObject>`，每个 RayObject 持有独立的 `PlasmaBuffer` → Slice 链，释放时各走各的 `~PlasmaBuffer()` → `Release()`。

### 9.1 RequestLeaseDependencies — Pull 的触发入口

Lease 调度时，`LocalLeaseManager` 通过 `LeaseDependencyManager` 发起依赖拉取：

```cpp
// local_lease_manager.cc
void LocalLeaseManager::ScheduleAndGrantLeases() {
  for (auto &lease : leases_to_try) {
    // Step 1: 检查依赖是否本地已有
    if (!lease_dependency_manager_.IsLeaseReady(lease_id)) {
      // Step 2: 依赖不全 → RequestLeaseDependencies
      lease_dependency_manager_.RequestLeaseDependencies(
          lease_id, lease.specification().DependencyIds());
      continue;
    }
    // Step 3: 依赖就绪 → PinLeaseArgsIfMemoryAvailable → GrantLease
    ...
  }
}
```

```cpp
// lease_dependency_manager.cc
void LeaseDependencyManager::RequestLeaseDependencies(
    const LeaseID &lease_id, const std::vector<ObjectID> &dependencies) {
  auto &lease_entry = lease_entries_[lease_id];

  // 记录依赖关系到 required_objects_
  for (const auto &obj_id : dependencies) {
    required_objects_[obj_id].dependent_leases.insert(lease_id);
    lease_entry->dependencies_.insert(obj_id);
  }

  // 检查哪些依赖缺失 → 需要远程拉取
  std::vector<ObjectID> required_objects;
  for (const auto &obj_id : dependencies) {
    if (!local_objects_.contains(obj_id)) {
      required_objects.push_back(obj_id);
    }
  }

  if (!required_objects.empty()) {
    // 发起 Pull 请求
    lease_entry->pull_request_id_ = object_manager_.Pull(required_objects, TASK_ARGS);
    // → ObjectManager::Pull → PullManager::Pull
    // → PullManager 开始从远端 fetch objects → 异步回调
  } else {
    // 所有依赖都在本地 → 直接标记就绪
    lease_entry->SetReady();
  }
}
```

**Pull 请求的生命周期绑定**：`pull_request_id_` 存储在 lease_entry 中，后续 lease 取消/完成时通过 `RemoveLeaseDependencies` 使用此 ID 调用 `CancelPull`。

### 9.2 数据接收（Remote Push → 本地 Create + Write + Seal + Release）

```
远端Push数据:
  object_manager.cc:610 HandlePush → ReceivePullChunk
  → buffer_pool.cc:100 CreateChunk
    → buffer_pool.cc:225 EnsureBufferExists
      → store_client_->CreateAndSpillIfNeeded
        → client.cc:188 InsertObjectInUse(count=1)        buffer_pool:2  server:0→1
        → client.cc:193 IncrementObjectCount(count=2)      buffer_pool:2  server:1
        → [IPC: CreateRequest, source=ReceivedByPull]
        → store.cc:191 AddToClientObjectIds → AddReference

  buffer_pool.cc:124 WriteChunk (逐chunk写入)               buffer_pool:2  server:1

  最后一个chunk写完:
  buffer_pool.cc:172 Seal → store_client_->Seal()
    → client.cc:587 SendSealRequest → [IPC: SealRequest]
    → client.cc:598 Release(count 2→1)                      buffer_pool:1  server:1

  buffer_pool.cc:173 Release → store_client_->Release()
    → client.cc:500 count-=1 (1→0)
    → client.cc:512 count==0:
      → MarkObjectUnused + SendReleaseRequest
      → [IPC: ReleaseRequest]
      → store.cc:256 RemoveFromClientObjectIds
        → RemoveReference (1→0)                              buffer_pool:移除  server:0
        → EndObjectAccess → 加入LRU! evictable!
```

### 9.3 add_object_callback_ 触发 PinNewObjectIfNeeded（恢复 server ref）

```
plasma store线程: Seal → add_object_callback_(post到raylet main_service)

main.cc:806-812:
  [&](const ray::ObjectInfo &object_info, plasma::flatbuf::ObjectSource source) {
    main_service.post([&]() {
      object_manager->HandleObjectAdded(object_info);
      node_manager->HandleObjectLocal(object_info, source);
    }, "ObjectManager.ObjectAdded");
  }

HandleObjectAdded:
  → object_manager.cc:149 ReportObjectAdded (通知object_directory)
  → object_manager.cc:154 pull_manager_->PinNewObjectIfNeeded(object_id)

PinNewObjectIfNeeded:
  // pull_manager.cc:586
  void PullManager::PinNewObjectIfNeeded(const ObjectID &object_id) {
    absl::MutexLock lock(&active_objects_mu_);
    bool active = active_object_pull_requests_.count(object_id) > 0;
    if (active) {
      TryPinObject(object_id);
    }
  }

TryPinObject:
  // pull_manager.cc:598
  bool PullManager::TryPinObject(const ObjectID &object_id) {
    if (pinned_objects_.count(object_id) > 0) { return true; }
    auto ref = pin_object_(object_id);  // main.cc:843 回调
    if (ref != nullptr) {
      pinned_objects_[object_id] = std::move(ref);
      return true;
    }
    return false;
  }

pin_object_ 回调:
  // main.cc:843
  [&](const ray::ObjectID &object_id) {
    std::vector<ray::ObjectID> object_ids = {object_id};
    std::vector<std::unique_ptr<ray::RayObject>> results;
    if (node_manager->GetObjectsFromPlasma(object_ids, &results) &&
        results.size() > 0) {
      result = std::move(results[0]);
    }
    return result;
  }

  → GetObjectsFromPlasma → [IPC: GetRequest]
    → store.cc:108 AddToClientObjectIds(raylet作为新client)
      → AddReference                                  server ref: 0→1
  → 返回 RayObject → pinned_objects_.emplace (pull_manager持有)
```

### 9.4 Pull 完成 → HandleObjectLocal → Lease 依赖递减

```
Pull 数据到达本地 plasma:
  → ObjectManager::HandleObjectAdded → PullManager::PinNewObjectIfNeeded
    → TryPinObject (如果 active pull)
  → NodeManager::HandleObjectLocal
    → LeaseDependencyManager::HandleObjectLocal(object_id)
      → 遍历 required_objects_[object_id].dependent_leases
      → 对每个 lease: DecrementMissingDependencies
        → 当该 lease 所有依赖都到达:
          → lease 状态变为 ready → 进入调度队列
```

### 9.5 Lease 调度 — PinLeaseArgs 接管保护

Lease 依赖全部就绪后，`LocalLeaseManager` 尝试 pin 参数对象并调度：

```cpp
// local_lease_manager.cc
bool LocalLeaseManager::PinLeaseArgsIfMemoryAvailable(
    const LeaseSpecification &spec, bool *args_missing) {
  std::vector<std::unique_ptr<RayObject>> args;
  // get_lease_arguments_ = NodeManager::GetObjectsFromPlasma (main.cc:959)
  if (!get_lease_arguments_(spec.DependencyIds(), &args)) {
    *args_missing = true;
    return false;
  }
  // 检查 pinned_lease_arguments_bytes_ 是否超限
  int64_t args_size = ComputeArgsSize(args);
  if (pinned_lease_arguments_bytes_ + args_size > GetPinnedArgsLimit()) {
    return false;
  }
  PinLeaseArgs(spec, std::move(args));
  return true;
}
```

```cpp
// local_lease_manager.cc
void LocalLeaseManager::PinLeaseArgs(
    const LeaseSpecification &lease_spec,
    std::vector<std::unique_ptr<RayObject>> args) {
  auto &lease_entry = lease_entries_[lease_spec.LeaseId()];
  for (size_t i = 0; i < args.size(); i++) {
    const auto &dep_id = lease_spec.DependencyIds()[i];
    if (args[i] != nullptr) {
      lease_entry->pinned_lease_arguments_[dep_id] = {
          std::move(args[i]), /*refcount=*/1};
      pinned_lease_arguments_bytes_ += args[i]->GetSize();
      // RayObject 持有 plasma buffer → server ref +1 (PlasmaClient #2)
    }
  }
}
```

**PinLeaseArgs 使用的 PlasmaClient 路径**：

```
get_lease_arguments_ 回调:
  // main.cc:959-962
  [&](const std::vector<ObjectID> &object_ids,
      std::vector<std::unique_ptr<RayObject>> *results) {
    return node_manager->GetObjectsFromPlasma(object_ids, results);
  }

GetObjectsFromPlasma:
  // node_manager.cc:2578
  → store_client_->Get(object_ids, ...)     [PlasmaClient #2]
    → [IPC: GetRequest] → AddToClientObjectIds → AddReference
  → 返回 unique_ptr<RayObject>（持有 plasma buffer 引用）
```

**PinLeaseArgs 接管后立即 CancelPull**：

```cpp
// PinLeaseArgsIfMemoryAvailable 成功后：
RemoveLeaseDependencies(lease_id);
  → lease_dependency_manager_.RemoveLeaseDependencies(lease_id)

// lease_dependency_manager.cc
void LeaseDependencyManager::RemoveLeaseDependencies(
    const LeaseID &lease_id) {
  auto &lease_entry = lease_entries_[lease_id];
  for (const auto &obj_id : lease_entry->dependencies_) {
    auto it = required_objects_.find(obj_id);
    it->second.dependent_leases.erase(lease_id);
    RemoveObjectIfNotNeeded(it);  // 如果无其他 lease 依赖此对象
  }
  if (lease_entry->pull_request_id_.has_value()) {
    object_manager_.CancelPull(lease_entry->pull_request_id_.value());
    // → PullManager::CancelPull → DeactivateBundlePullRequest → UnpinObject
    // → 释放 PullManager 的 pinned_objects_ → RayObject 析构 → server ref -1
  }
}
```

**PinLeaseArgs 接管保护的含义**：CancelPull 释放 PullManager 的 pin 后，对象不被 LRU 淘汰，因为 PinLeaseArgs 持有的 `pinned_lease_arguments_` 中仍有 RayObject 引用（server ref 仍 > 0）。PinLeaseArgs 成为对象的新保护者。

### 9.6 Lease 完成 → ReleaseLeaseArgs

```cpp
// local_lease_manager.cc
void LocalLeaseManager::CleanupLease(const LeaseID &lease_id) {
  auto &lease_entry = lease_entries_[lease_id];
  ReleaseLeaseArgs(lease_id);
  // ... 其他清理
}

void LocalLeaseManager::ReleaseLeaseArgs(const LeaseID &lease_id) {
  auto it = lease_entries_.find(lease_id);
  for (auto &[dep_id, arg_entry] : it->second->pinned_lease_arguments_) {
    if (arg_entry.refcount > 0) {
      arg_entry.refcount--;
      pinned_lease_arguments_bytes_ -= arg_entry.object->GetSize();
      if (arg_entry.refcount == 0) {
        // refcount 归零 → RayObject unique_ptr 释放
        // → RayObject 析构 → 释放持有的 plasma buffer
        // → PlasmaClient::Release (PlasmaClient #2)
        //   → count-=1 → count==0 → SendReleaseRequest
        //   → [IPC: ReleaseRequest] → server ref -1
        //   → 如果无其他 pin（PinObjectsAndWaitForFree 未持有）:
        //     server ref → 0 → EndObjectAccess → 加入 LRU → 可淘汰
      }
    }
  }
  it->second->pinned_lease_arguments_.clear();
}
```

**ReleaseLeaseArgs 释放后**：如果 `PinObjectsAndWaitForFree` 没有长期 pin 此对象，则 server ref 归零，对象可被 plasma LRU 淘汰。Pull 来的临时对象通常不会走 `PinObjectsAndWaitForFree`，因此 lease 完成后即可被 LRU 回收。

### 9.7 CancelPull 的必要性（资源清理细节）

`CancelPull` 不仅是"停止拉取"，更重要的是清理 PullManager 中的残留状态和资源：

```cpp
// pull_manager.cc
void PullManager::CancelPull(const PullRequestID &pull_request_id) {
  auto it = pull_request_id_to_bundles_.find(pull_request_id);
  auto &bundles = it->second;

  // 1. DeactivateBundlePullRequest → UnpinObject
  //    释放 pinned_objects_ 中的 RayObject → plasma 引用 (server ref -1)

  // 2. 清理 object_pull_requests_ — pull 状态和重试计时器
  for (auto &[obj_id, req] : bundles.object_pull_requests) {
    req->timer_.stop();        // 取消重试定时器
    req->cancel_callback_();   // 取消位置订阅
  }

  // 3. 清理 active_object_pull_requests_ — admission control 配额释放
  for (auto &obj_id : bundles.active_objects) {
    active_object_pull_requests_.erase(obj_id);
  }

  // 4. 清理位置订阅 — object directory 位置订阅 (浪费网络开销)
  //    取消对 object_directory 的 SubscribeObjectLocations
}
```

| 资源 | 存储位置 | 影响 | 不清理后果 |
|------|---------|------|-----------|
| `pinned_objects_` | PullManager | 占用 plasma 内存配额 (server ref > 0) | 对象永不释放，内存泄漏 |
| `object_pull_requests_` | PullManager | 维护 pull 状态和重试计时器 | 定时器持续触发无效重试 |
| 位置订阅 | ObjectDirectory | 订阅对象位置变化 | 持续网络开销、回调触发 |
| `active_object_pull_requests_` | PullManager | 占用 admission control 配额 | 新 pull 被限流 |

### 9.8 Pull + Lease 完整时序（含 PinLeaseArgs 接管）

```
时间  PullManager pin   PinLeaseArgs pin   Worker client   Server ref   说明
────────────────────────────────────────────────────────────────────────────────
T1    Get → +1          -                  -               1            Pull完成,PinNewObjectIfNeeded
T2    持有 pin           -                  -               1            PullManager 保护中
T3    持有 pin           Get → +1           -               2            PinLeaseArgsIfMemoryAvailable
T4    CancelPull         持有 pin           -               1            UnpinObject 释放 PullManager ref
     → UnpinObject →    (接管保护)                                        PinLeaseArgs 独占保护
     → RayObject析构
     → ReleaseRequest
T5    -                  持有 pin           Get → +1        2            Worker 获取参数
T6    -                  持有 pin           持有引用        2            任务执行中
T7    -                  持有 pin           Release → -1    1            Worker 用完释放
T8    -                  ReleaseLeaseArgs   -               0            CleanupLease
     -                  → RayObject析构
                        → ReleaseRequest
                                                      → EndObjectAccess
                                                      → 加入LRU → 可淘汰
```

**关键转折点 T3→T4**：PinLeaseArgs 获取引用后立即 CancelPull，两者交接保护权。这是无间隙的——PinLeaseArgs 的 Get 在 CancelPull 的 UnpinObject 之前完成，所以 server ref 在交接期间为 2，始终 > 0，对象不会被 LRU 淘汰。

**Pull pin 的生命周期 = pull 请求的生命周期**，和 owner 无关。PinLeaseArgs 的生命周期 = lease 的生命周期。两者接力保护对象不被 LRU，直到 lease 完成。

### 9.9 RemoveLeaseDependencies 的调用场景

`RemoveLeaseDependencies`（含 CancelPull）在以下场景触发（均在 `local_lease_manager.cc`）：

| 行号 | 场景 | 说明 |
|------|------|------|
| ~495 | Spillback | lease 被调度到其他节点，取消本地 pull |
| ~542 | Unschedulable | lease 无法调度，spillback |
| ~604 | 等待队列移除 | lease 从等待队列移除 |
| ~905 | CancelWaitingLease | 基于谓词取消等待中的 lease |
| ~943 | CancelLeaseToGrantWithoutReply | 取消已授权但未回复的 lease |

---

## 10. 完整链路 5：ReceivedByPush（replication push 接收端）—— 问题所在

### 10.1 数据接收

```
远端raylet MaybeReplicateObject → Push:
  object_manager.cc:610 HandlePush
  → is_replication_push == true:
    → ReceiveReplicationPushChunk(node_id, object_id, owner_address, ...)

ReceiveReplicationPushChunk:
  // object_manager.cc:696
  → 检查: has_active_pull || object_already_local
    → 如果有active pull或对象已存在: 拒绝push (skipped)
    → 如果都没有: 继续

  → buffer_pool.cc:100 CreateChunk
    → EnsureBufferExists → store_client_->CreateAndSpillIfNeeded
      → InsertObjectInUse(count=1) + IncrementObjectCount(count=2)    2  1
      → [IPC: CreateRequest, source=ReceivedByPush]

  → WriteChunk (逐chunk写入)

  最后一个chunk写完:
  → buffer_pool.cc:172 Seal → store_client_->Seal()                  1  1
  → buffer_pool.cc:173 Release → store_client_->Release()
    → count 1→0 → MarkObjectUnused + SendReleaseRequest
    → [IPC: ReleaseRequest] → RemoveFromClientObjectIds → RemoveReference
    → server ref 1→0 → EndObjectAccess → 加入LRU → evictable!
```

### 10.2 add_object_callback_ 触发但无人 Pin

```
add_object_callback_ 触发:
  → HandleObjectAdded → PinNewObjectIfNeeded
    → pull_manager.cc:588:
      bool active = active_object_pull_requests_.count(object_id) > 0;
      // active == false! 没人pull这个对象
      // 不调用 TryPinObject → server ref 保持 0

  → HandleObjectLocal:
    → MaybeReplicateObject: source==ReceivedByPush, 跳过
    → SpillIfOverPrimaryObjectsThreshold:
      → pinned_used 不含 ReceivedByPush → 不触发spill
    → 没有任何后续Get/Pin操作
```

### 10.3 最终状态

```
      ──── 最终稳态 ────
      client: 全部退出 (buffer_pool Release后无后续client)
      server ref: 0 → LRU随时淘汰!
      owner ref: locations 中可能包含此节点 (ReportObjectAdded时添加)
                但无 pinned_at_node_id_ (不是primary)
```

**对比所有路径**：

| 路径 | Seal+Release后 server ref | 谁在后续 Get/Pin | 最终 server ref | 状态 |
|------|--------------------------|----------------|----------------|------|
| Put | 0 | raylet PinObjectIDs | 1 | spillable |
| SealExisting | 0 | raylet PinObjectIDs | 1 | spillable |
| PinExistingReturnObject | 0→1(Get)→2(Pin)→1(Release) | Get + raylet Pin | 1 | spillable |
| Pull | 0 | pull_manager TryPinObject | 1 | spillable |
| **ReceivedByPush** | **0** | **无** | **0** | **evictable!** |

---

## 11. 完整链路 6：Owner Unpin（owner out of scope）

```
步骤  代码位置                                        说明
────────────────────────────────────────────────────────────────────────────────
1     reference_counter.cc 判定对象OutOfScope()
      → 发布 WorkerObjectEviction (pub/sub)
        → core_worker_subscriber_->Publish(...)

2     raylet 收到订阅回调:
      local_object_manager.cc:82 subscription_callback
        → local_object_manager.cc:86 ReleaseFreedObject(obj_id)

3     local_object_manager.cc:111 ReleaseFreedObject
      → local_object_manager.cc:117 is_freed_ = true
      → local_object_manager.cc:123 pinned_objects_it != end:
        → local_object_manager.cc:129 pinned_objects_size_ -= size
        → local_object_manager.cc:130 pinned_objects_.erase(it)
        → local_object_manager.cc:131 local_objects_.erase(it)

4     RayObject 析构 → 释放持有的 plasma buffer
      → raylet的 PlasmaClient::Release
        → client.cc:500 count-=1 (1→0)
        → client.cc:512 count==0:
          → MarkObjectUnused + SendReleaseRequest
          → [IPC: ReleaseRequest]

5     store.cc:434 处理 ReleaseRequest:
      → store.cc:440 ReleaseObject
        → store.cc:265 RemoveFromClientObjectIds
          → store.cc:256 RemoveReference
            → obj_lifecycle_mgr.cc:157 entry->ref_count_--
            → obj_lifecycle_mgr.cc:161 if(ref_count_ == 0):
              → obj_lifecycle_mgr.cc:164 EndObjectAccess
                → eviction_policy.cc:145:
                  cache_.Add(object_id, size)      // 加入LRU → 可淘汰
                  pinned_memory_bytes_ -= size

6     如有spilled文件:
      → local_object_manager.cc:135 spilled_object_pending_delete_.push
      → 后续 FlushFreeObjects → FreeObjects
        → DeleteSpilledObjects RPC 给 DELETE_WORKER
```

---

## 12. Pull Pin vs Primary Pin 生命周期对比

| | Primary pin (`PinObjectsAndWaitForFree`) | Pull pin (`PullManager::TryPinObject`) |
|---|---|---|
| **触发** | `PinObjectIDs` RPC（Create/Put/SealExisting 后） | `PinNewObjectIfNeeded`（Pull 完成后） |
| **unpin 触发** | owner 发布 `WorkerObjectEviction` → raylet 收到订阅回调 → `ReleaseFreedObject` | **最后一个 pull request 取消** → `UnpinObject` |
| **生命周期** | 和 owner 的语义引用绑定，owner out of scope 才释放 | 和 pull request 绑定，pull 请求没了就释放 |
| **owner 关系** | 订阅 owner 的 `WorkerObjectEviction` 通道 | 不订阅 owner |
| **server ref** | owner 活着期间一直=1 | pull 请求取消后立刻=0 |
| **spillable** | 是（ref=1 时可 spill 不可 evict） | 是（pull 期间可 spill 不可 evict） |
| **代码** | `local_object_manager.cc:31` | `pull_manager.cc:598` |

---

## 13. 汇总对比表

### 13.1 各路径最终 server ref 状态

| 路径 | 写完后 server ref | 谁持 pin | unpin 触发 | 最终生命周期 |
|------|-------------------|---------|-----------|------------|
| Put | 1 | raylet (PinObjectsAndWaitForFree) | owner out of scope → eviction | owner 活着期间 |
| SealExisting | 1 | raylet (PinObjectsAndWaitForFree) | owner out of scope → eviction | owner 活着期间 |
| PinExistingReturnObject | 1 | raylet (PinObjectsAndWaitForFree) | owner out of scope → eviction | owner 活着期间 |
| Pull | 1 | pull_manager (TryPinObject) | pull request 取消 | pull 请求期间 |
| **ReceivedByPush** | **0** | **无** | **不适用** | **立即可被 LRU evict** |

### 13.2 Client count 与 server ref 对应关系

| client 操作 | client count | server ref | IPC |
|------------|-------------|-----------|-----|
| Create → 加入 | 0→2 | 0→1 (+1) | CreateRequest → AddToClientObjectIds |
| Seal 内部 Release | 2→1 | 不变 | 无（count>0） |
| 回调 Release → 退出 | 1→0 | -1 | ReleaseRequest → RemoveFromClientObjectIds |
| Get → 加入 | 0→1 | +1 | GetRequest → AddToClientObjectIds |
| Release → 退出 | 1→0 | -1 | ReleaseRequest → RemoveFromClientObjectIds |

---

## 14. 关键代码索引

### Client Count

| 文件 | 行号 | 函数 | 说明 |
|------|------|------|------|
| `plasma/client.h` | 339 | `ObjectInUseEntry` | client count 存储结构 |
| `plasma/client.cc` | 110 | `InsertObjectInUse` | count 0→1 |
| `plasma/client.cc` | 126 | `IncrementObjectCount` | count +1 |
| `plasma/client.cc` | 490 | `Release` | count -1，count=0 时通知 server |
| `plasma/client.cc` | 480 | `MarkObjectUnused` | 从 objects_in_use_ 移除 |

### Server ref_count

| 文件 | 行号 | 函数 | 说明 |
|------|------|------|------|
| `plasma/common.h` | 181 | `LocalObject::ref_count_` | server ref 存储 |
| `plasma/obj_lifecycle_mgr.cc` | 128 | `AddReference` | ref +1，0→1 时 BeginObjectAccess |
| `plasma/obj_lifecycle_mgr.cc` | 148 | `RemoveReference` | ref -1，→0 时 EndObjectAccess |
| `plasma/store.cc` | 134 | `AddToClientObjectIds` | client 加入 → AddReference |
| `plasma/store.cc` | 247 | `RemoveFromClientObjectIds` | client 退出 → RemoveReference |
| `plasma/eviction_policy.cc` | 105 | `ChooseObjectsToEvict` | LRU 淘汰选择 |
| `plasma/eviction_policy.cc` | 136 | `BeginObjectAccess` | 从 LRU 移除，不可淘汰 |
| `plasma/eviction_policy.cc` | 144 | `EndObjectAccess` | 加入 LRU，可淘汰 |

### Owner Ref

| 文件 | 行号 | 函数 | 说明 |
|------|------|------|------|
| `core_worker/reference_counter.cc` | 953 | `UpdateObjectPinnedAtRaylet` | 设置 pinned_at_node_id_ |
| `core_worker/reference_counter.cc` | 895 | `ResetObjectsOnRemovedNode` | 节点死亡处理 |
| `core_worker/reference_counter.cc` | 919 | `needs_recovery_has_replica` | 有存活副本 |
| `core_worker/reference_counter.cc` | 921 | `needs_recovery_no_replica` | 无存活副本 |
| `core_worker/reference_counter.cc` | 940 | `secondary_copy_lost` | 非主副本丢失 |

### Raylet Pin

| 文件 | 行号 | 函数 | 说明 |
|------|------|------|------|
| `raylet/local_object_manager.cc` | 31 | `PinObjectsAndWaitForFree` | primary pin + 订阅 owner eviction |
| `raylet/local_object_manager.cc` | 111 | `ReleaseFreedObject` | owner eviction → unpin |
| `raylet/local_object_manager.cc` | 625 | `PullManager::UnpinObject` | pull pin 释放 |
| `raylet/node_manager.cc` | 2605 | `HandlePinObjectIDs` | 处理 PinObjectIDs RPC |
| `raylet/node_manager.cc` | 2419 | `HandleObjectLocal` | 对象本地化回调 |

### Core Worker 写入路径

| 文件 | 行号 | 函数 | 说明 |
|------|------|------|------|
| `core_worker/core_worker.cc` | 2917 | `AllocateReturnObject` | 大小判断 + Create |
| `core_worker/core_worker.cc` | 3220 | `SealReturnObject` | SealExisting 入口 |
| `core_worker/core_worker.cc` | 1198 | `SealExisting` | Seal + Pin + Release |
| `core_worker/core_worker.cc` | 3294 | `PinExistingReturnObject` | 已有对象 + Get + Pin |
| `core_worker/core_worker.cc` | 1001 | `PutInLocalPlasmaStore` | Put + Pin |
| `core_worker/store_provider/plasma_store_provider.cc` | 98 | `Put` | Create + Seal 同步 |
| `core_worker/store_provider/plasma_store_provider.cc` | 256 | `Get` | Get 路径 |

### Object Manager Push/Pull

| 文件 | 行号 | 函数 | 说明 |
|------|------|------|------|
| `object_manager/object_manager.cc` | 610 | `HandlePush` | 区分 replication/pull push |
| `object_manager/object_manager.cc` | 643 | `ReceivePullChunk` | Pull chunk 接收 |
| `object_manager/object_manager.cc` | 696 | `ReceiveReplicationPushChunk` | Replication push 接收 |
| `object_manager/object_buffer_pool.cc` | 100 | `CreateChunk` | chunk 级 Create |
| `object_manager/object_buffer_pool.cc` | 225 | `EnsureBufferExists` | 确保buffer存在 |
| `object_manager/object_buffer_pool.cc` | 171-174 | Seal + Release | 所有 chunk 写完 |
| `object_manager/pull_manager.cc` | 586 | `PinNewObjectIfNeeded` | Pull 完成后 pin |
| `object_manager/pull_manager.cc` | 598 | `TryPinObject` | 尝试 pin |
| `object_manager/pull_manager.cc` | 625 | `UnpinObject` | Pull pin 释放 |
| `object_manager/pull_manager.cc` | 317 | `CancelPull` | 取消 pull |

### Lease Dependency Manager（pull request 管理）

| 文件 | 行号 | 函数 | 说明 |
|------|------|------|------|
| `raylet/lease_dependency_manager.cc` | 53 | `CancelPull` (wait) | Wait 请求完成 → cancel pull |
| `raylet/lease_dependency_manager.cc` | 156 | `CancelPull` (get) | Get 请求完成 → cancel pull |
| `raylet/lease_dependency_manager.cc` | 263 | `CancelPull` (lease) | Lease 不再需要 → cancel pull |

---

## 15. Object Location 上报机制

Ray 集群中每个节点需要将对象的位置信息（内存副本、spill 位置）上报给 Owner Worker，Owner 维护全局 location 集合并通过 pub/sub 推送给订阅者。

### 15.1 上报通道

所有 location 上报通过 `OwnershipBasedObjectDirectory` → gRPC `UpdateObjectLocationBatch` 发送给 Owner Worker，**三种更新类型复用同一通道**：

| 事件类型 | 触发点 | RPC 字段 | 含义 |
|---------|--------|---------|------|
| 内存副本增加 | `ObjectManager::HandleObjectAdded` (object_manager.cc:178) | `plasma_location_update=ADDED` | 对象进入本节点 plasma |
| 内存副本移除 | `ObjectManager::HandleObjectDeleted` (object_manager.cc:207) | `plasma_location_update=REMOVED` | 对象从本节点 plasma 移除 |
| Spill 完成 | `LocalObjectManager::OnObjectSpilled` (local_object_manager.cc:440) | `spilled_location_update{spilled_url, spilled_to_local_storage}` | 对象被 spill 到外部存储 |

### 15.2 上报流程详解

```
节点 B (Raylet)                              节点 A (Owner Worker)
────────────────                             ────────────────────

1. 对象进入/离开 plasma / spill 完成

2. OwnershipBasedObjectDirectory:
   ReportObjectAdded / ReportObjectRemoved / ReportObjectSpilled
   (ownership_object_directory.cc:121 / :144 / :167)

3. 构造 ObjectLocationUpdate，缓存到:
   location_buffers_[owner_worker_id].second[object_id]
   location_buffers_[owner_worker_id].first.emplace_back(object_id)

4. SendObjectLocationUpdateBatchIfNeeded()
   → batch 最多 kMaxObjectReportBatchSize 个 update
   → gRPC UpdateObjectLocationBatch ─────────→

5. Owner Worker 收到:                        CoreWorker::HandleUpdateObjectLocationBatch
                                              (core_worker.cc:3705)

                                              遍历 object_location_updates:
                                              ├ has_plasma_location_update?
                                              │  ├ ADDED → AddObjectLocationOwner
                                              │  │   → reference_counter_->AddObjectLocation
                                              │  │     → it->second.locations.emplace(node_id)
                                              │  │     → PushToLocationSubscribers(it)
                                              │  └ REMOVED → RemoveObjectLocationOwner
                                              │      → reference_counter_->RemoveObjectLocation
                                              │        → it->second.locations.erase(node_id)
                                              │        → PushToLocationSubscribers(it)
                                              │
                                              └ has_spilled_location_update?
                                                → AddSpilledObjectLocationOwner
                                                  → reference_counter_->HandleObjectSpilled
                                                    → it->second.spilled = true
                                                    → it->second.spilled_url = url
                                                    → it->second.spilled_node_id = node_id
                                                    → PushToLocationSubscribers(it)
```

### 15.3 关键点：Pull 对象也会上报

**Pull 路径创建的对象同样走 `HandleObjectAdded` → `ReportObjectAdded`**。无论对象来自 Push 还是 Pull，只要被 Seal 到 plasma store 且有 `object_info`（含 owner 信息），都会上报到 Owner。

**但 ReceivedByPush 对象的问题是**：Seal+Release 后 ref=0 → LRU 立即淘汰 → `HandleObjectDeleted` → `ReportObjectRemoved` → Owner 把节点从 locations 移除。**加进去又被移除了**，所以 Owner 认为该节点没有副本。

### 15.4 Owner 端 location 数据结构

```cpp
// reference_counter.cc — Reference 结构中的关键字段
struct Reference {
    absl::flat_hash_set<NodeID> locations;   // 有内存副本的节点集合
    std::string spilled_url;                 // spill URL (本地路径或 S3 URL)
    NodeID spilled_node_id;                  // spill 所在节点 (IsNil 表示外部存储)
    bool spilled;                            // 是否已被 spill
    bool did_spill;                          // 是否执行过 spill
    std::optional<NodeID> pinned_at_node_id_; // primary copy 所在节点
};
```

Owner 的 `PushToLocationSubscribers` 发布 `WorkerObjectLocationsPubMessage`，包含：
- `node_ids`：所有有内存副本的节点
- `spilled_url`：spill URL
- `spilled_node_id`：spill 所在节点
- `pending_creation`：是否正在创建中
- `object_size`：对象大小

---

## 16. WorkerObjectEviction Pub/Sub 完整机制

Owner Worker 上持有对象语义生命周期，当对象 OutOfScope 时需要通知所有 pin 了该对象的 raylet 释放。这套通知通过 **WorkerObjectEviction** pub/sub channel 实现。

### 16.1 架构：每个 Worker 进程既是 Publisher 又是 Subscriber

```
节点 A (Owner Worker)                     节点 B (Raylet)
┌──────────────────────┐                  ┌──────────────────────┐
│ CoreWorker 进程       │                  │ Raylet 进程           │
│                      │                  │                      │
│ Publisher            │                  │ core_worker_subscriber_│
│  (object_info_publisher_)               │   (Subscriber)       │
│  ├ 注册 channel:      │                  │   ├ 注册 channel:     │
│  │  WORKER_OBJECT_   │                  │   │  WORKER_OBJECT_   │
│  │  EVICTION         │                  │   │  EVICTION         │
│  │                   │                  │   │                   │
│  ├ subscribers_:      │   Long Polling   │   │                   │
│  │  subscriber_id→   │◄───gRPC──────────│   │                   │
│  │  SubscriberState  │                  │   │                   │
│  │   ├ mailbox_      │                  │   │                   │
│  │   └ long_polling_ │                  │   │                   │
│  │     connection    │                  │   │                   │
│  │                   │    消息发布       │   │                   │
│  │ Publish() ────────│───gRPC──────────→│   HandleLongPolling   │
│  │                   │   (Long Polling   │   Response           │
│  │                   │    Reply)         │   → subscription_callback│
└──────────────────────┘                  └──────────────────────┘
```

### 16.2 步骤 1：订阅（Raylet → Owner Worker）

```
1. LocalObjectManager::PinObjectsAndWaitForFree
   (local_object_manager.cc:57)
   构造 WorkerObjectEvictionSubMessage:
     {
       object_id,
       intended_worker_id = owner_address.worker_id(),
       subscriber_address = {self_node_id, ip, port}  // raylet 地址
     }

2. core_worker_subscriber_->Subscribe(
     sub_message,
     ChannelType::WORKER_OBJECT_EVICTION,
     owner_address,       // Owner 的 RPC 地址（可能跨节点）
     object_id.Binary(),
     subscription_callback,  // Owner 发布 eviction 时触发
     owner_dead_callback)    // Owner 死亡时触发

3. Subscriber::Subscribe (subscriber.cc:261)
   → 构造 CommandItem (subscribe 命令)
   → commands_[publisher_id].emplace(command)
   → SendCommandBatchIfPossible()
     → gRPC PubsubCommandBatchRequest 发给 Owner Worker
   → Channel(channel_type)->Subscribe() → 注册本地回调
   → MakeLongPollingConnectionIfNotConnected()
     → 建立长轮询连接

4. Owner Worker 收到 PubsubCommandBatchRequest
   → CoreWorker::HandlePubsubCommandBatch
   → ProcessSubscribeMessage
     → ProcessSubscribeForObjectEviction (core_worker.cc:3562)
       → 构造 unpin_object lambda:
           [this](const ObjectID &object_id) {
             PubMessage pub_message;
             pub_message.set_channel_type(WORKER_OBJECT_EVICTION);
             pub_message.mutable_worker_object_eviction_message()
               ->set_object_id(object_id.Binary());
             object_info_publisher_->Publish(std::move(pub_message));
           }
       → reference_counter_->AddObjectOutOfScopeOrFreedCallback(
             object_id, unpin_object)
         → 存入 it->second.on_object_out_of_scope_or_freed_callbacks

5. Owner Worker 的 Publisher 注册订阅:
   → Publisher::RegisterSubscription(channel_type, subscriber_id, key_id)
   → subscription_index_map_[channel].AddEntry(key_id, subscriber)
```

### 16.3 步骤 2：Long Polling（Raylet 持续等待消息）

```
6. Subscriber::MakeLongPollingPubsubConnection (subscriber.cc:296)
   → 构造 PubsubLongPollingRequest {subscriber_id, publisher_id, max_processed_sequence_id}
   → subscriber_client->PubsubLongPolling(request, callback)
     → [gRPC] 发给 Owner Worker

7. Owner Worker 收到 PubsubLongPollingRequest
   → Publisher::ConnectToSubscriber (publisher.cc:364)
   → 找到/创建 SubscriberState
   → subscriber->ConnectToSubscriber(request, ...)
     → long_polling_connection_ = make_unique<LongPollConnection>(reply_callback)
     → PublishIfPossible(force_noop=false)
       → mailbox_ 为空? → 不回复，保持长轮询挂起
       → mailbox_ 有消息? → 立刻回复

   (长轮询挂起: Owner Worker 持有 reply_callback, 不立刻回复，
    等 Publish 有消息时才回复)
```

### 16.4 步骤 3：Owner OutOfScope → 发布 eviction 消息

```
8. Owner 的 ReferenceCounter 判定对象 OutOfScope
   → OnObjectOutOfScopeOrFreed (reference_counter.cc:839)
     → 遍历 on_object_out_of_scope_or_freed_callbacks:
       → callback(object_id)

9. unpin_object lambda (core_worker.cc:3566):
   → 构造 PubMessage {channel=WORKER_OBJECT_EVICTION, object_id}
   → object_info_publisher_->Publish(std::move(pub_message))

10. Publisher::Publish (publisher.cc:421)
    → pub_message.set_sequence_id(++next_sequence_id_)
    → subscription_index.Publish(pub_message)
      → EntityState::Publish(msg)
        → mailbox_.push_back(msg)
        → PublishIfPossible(force_noop=false)
          → long_polling_connection_ 存在?
            → 从 mailbox_ 取出消息
            → pub_messages->Add(msg)
            → long_polling_connection_->send_reply_callback(...)
              → [gRPC reply] 发给 Raylet 的 Subscriber
```

### 16.5 步骤 4：Raylet 收到消息 → unpin

```
11. Subscriber::HandleLongPollingResponse (subscriber.cc:321)
    → 收到 PubsubLongPollingReply {pub_messages}
    → 遍历 pub_messages:
      → Channel(channel_type)->HandlePublishedMessage(publisher_address, msg)
        → 查找注册的 subscription_callback → 调用

12. subscription_callback (local_object_manager.cc:68):
    → ReleaseFreedObject(obj_id)
      → pinned_objects_.erase(it)
      → RayObject 析构 → PlasmaClient A Release → [IPC] server ref-1

13. 重新建立长轮询 (subscriber.cc:399):
    → if (SubscriptionExists(publisher_id)):
        MakeLongPollingPubsubConnection(publisher_address)
        → 重新发起 Long Polling，等待下一个消息
```

### 16.6 步骤 5：Owner 死亡的处理

```
Owner Worker 进程崩溃 → gRPC 连接断开
  → Subscriber::HandleLongPollingResponse 收到错误 status
    → subscriber.cc:328: status.ok() == false
    → 遍历 channels_ → HandlePublisherFailure(publisher_address, status)
      → 调用 owner_dead_callback (local_object_manager.cc:76):
        → ReleaseFreedObject(obj_id)
        → pinned_objects_.erase → RayObject 析构 → server ref-1
```

### 16.7 时序图

```
节点 B Raylet                      节点 A Owner Worker
────────────────                   ────────────────────
1. Subscribe(owner_address)
   → PubsubCommandBatch ──gRPC──→  注册 subscription
   → Long Polling ──gRPC────────→  挂起 reply_callback
                                    (等待消息)

     ... 对象 pin 期间, Long Polling 持续挂起 ...

                                    7. OutOfScope
                                    → callback → Publish()
                                    → mailbox_ 有消息
5. ←── Long Polling Reply ──gRPC──  回复 (包含 eviction 消息)

8. subscription_callback
   → ReleaseFreedObject
   → server ref-1
   → 重新 Long Polling ──gRPC──→   挂起新的 reply_callback
                                    (无更多消息)
```

### 16.8 连接粒度和资源消耗

**不是每个 object 一条连接**，是 **每个 owner worker 一条长轮询 gRPC 连接**，多个 object 共享。

Subscriber 内部数据结构：
```cpp
// subscriber.cc
publishers_connected_: Map<publisher_id, bool>
  → 每个 publisher_id (owner worker_id) 只建立一条 Long Polling gRPC 连接

commands_[publisher_id]: Queue<CommandItem>
  → 同一个 owner 的多个 subscribe 命令会 batch 到一个 PubsubCommandBatchRequest 里发送
```

关键代码 `MakeLongPollingConnectionIfNotConnected` (subscriber.cc:308):
```cpp
auto publishers_connected_it = publishers_connected_.find(publisher_id);
if (publishers_connected_it == publishers_connected_.end()) {
    publishers_connected_.emplace(publisher_id);
    MakeLongPollingPubsubConnection(publisher_address);  // 只在首次创建
}
```

资源消耗模型：

| 维度 | 粒度 | 数量级 |
|------|------|--------|
| gRPC 长轮询连接 | per owner worker | = 活跃 worker 数 (百~千级) |
| subscribe 命令 | per object | 一次性，batch 发送 |
| channel 内订阅条目 | per (publisher, key_id) | = 被 pin 的 object 数 |
| 消息投递 | per eviction event | 按需，仅 OutOfScope 时 |

- **连接数**：100 个 worker → 100 条长轮询连接（而非几万条）
- **内存**：每条连接维护一个 `mailbox_` 和 `SubscriberState`
- **CPU**：长轮询无消息时完全挂起，零开销；有消息时一次 reply 可批量携带多个 `pub_message`
- 每个 object 在 Owner 的 `ReferenceCounter` 里注册一个回调（`on_object_out_of_scope_or_freed_callbacks`）—— 纯内存，O(1) per object
- 每个 object 在 Publisher 的 `SubscriptionIndex` 里占一个 entry —— 纯内存

---

## 17. WorkerObjectLocations Pub/Sub 机制

除了 eviction 通道，还有 **WorkerObjectLocations** 通道，用于对象位置变更通知。PullManager 等组件通过订阅此通道获取对象的实时位置（内存副本节点、spill URL）。

### 17.1 订阅流程

```
PullManager 需要拉取对象 → ObjectManager::Pull
  → object_directory_->SubscribeObjectLocations(callback_id, object_id, owner_address, callback)
    → OwnershipBasedObjectDirectory::SubscribeObjectLocations (ownership_object_directory.cc:320)
      → 构造 WorkerObjectLocationsSubMessage {intended_worker_id, object_id}
      → object_location_subscriber_->Subscribe(
          sub_message,
          ChannelType::WORKER_OBJECT_LOCATIONS_CHANNEL,
          owner_address,
          object_id.Binary(),
          subscribe_done_callback,
          msg_published_callback,    // 位置变更时触发
          failure_callback)         // Owner 死亡或 ref 已删除时触发
      → 创建 LocationListenerState:
        {
          owner_address,
          current_object_locations,   // 当前已知内存副本节点
          spilled_url,                // spill URL
          spilled_node_id,            // spill 所在节点
          pending_creation,           // 是否正在创建
          object_size,                // 对象大小
          callbacks                   // 回调集合
        }
```

### 17.2 消息发布

当 Owner 端的对象位置发生变化时（`PushToLocationSubscribers`，reference_counter.cc:1678）：
```cpp
void ReferenceCounter::PushToLocationSubscribers(ReferenceTable::iterator it) {
    rpc::PubMessage pub_message;
    pub_message.set_key_id(object_id.Binary());
    pub_message.set_channel_type(WORKER_OBJECT_LOCATIONS_CHANNEL);
    auto object_locations_msg = pub_message.mutable_worker_object_locations_message();
    FillObjectInformationInternal(it, object_locations_msg);
    object_info_publisher_->Publish(std::move(pub_message));
}
```

发布内容 (`FillObjectInformationInternal`，reference_counter.cc:1713):
```cpp
for (const auto &node_id : it->second.locations) {
    object_info->add_node_ids(node_id.Binary());    // 内存副本节点
}
object_info->set_object_size(it->second.object_size_);
object_info->set_spilled_url(it->second.spilled_url);     // spill URL
object_info->set_spilled_node_id(it->second.spilled_node_id.Binary());  // spill 节点
object_info->set_pending_creation(it->second.pending_creation_);
object_info->set_did_spill(it->second.did_spill);
```

### 17.3 订阅者收到更新

```
Subscriber 收到 Long Polling Reply
  → ObjectLocationSubscriptionCallback (ownership_object_directory.cc:264)
    → UpdateObjectLocations(location_info, ..., &current_object_locations, &spilled_url, &spilled_node_id, ...)
    → 位置有变化? → 回调 PullManager 的 callback:
      callback(locations, spilled_url, spilled_node_id, pending_creation, object_size)
      → PullManager 更新 request.client_locations, request.spilled_url 等
```

### 17.4 触发 PushToLocationSubscribers 的场景

| 场景 | 触发代码 | 位置变更 |
|------|---------|---------|
| 内存副本增加 | `AddObjectLocationInternal` (reference_counter.cc:1468) | `locations +{node_id}` |
| 内存副本移除 | `RemoveObjectLocationInternal` | `locations -{node_id}` |
| Spill 完成 | `HandleObjectSpilled` (reference_counter.cc:1551) | `spilled_url`, `spilled_node_id` |
| 对象大小更新 | `UpdateObjectSize` (reference_counter.cc:414) | `object_size` |
| Pending creation 变更 | `UpdateObjectPendingCreationInternal` (reference_counter.cc:1494) | `pending_creation` |
| 首次订阅 | `PublishObjectLocationSnapshot` (reference_counter.cc:1731) | 全量快照 |

### 17.5 与 Eviction 通道的对比

| 维度 | WORKER_OBJECT_EVICTION | WORKER_OBJECT_LOCATIONS |
|------|----------------------|------------------------|
| 订阅者 | pin 了 primary copy 的 raylet | 需要拉取对象的 raylet (PullManager) |
| 触发条件 | Owner OutOfScope | 任何 location 变更 |
| 消息内容 | 仅 object_id | locations + spilled_url + object_size + ... |
| 生命周期 | primary copy pin 期间 | pull request 存续期间 |
| 连接粒度 | per owner worker | per owner worker |

两个通道共享同一套 pub/sub 基础设施（Long Polling），但独立运作，互不干扰。

---

## 18. Spill 流程详解

当 Plasma Store 内存压力过大时，raylet 会将 primary copy 对象 spill 到外部存储（本地磁盘或 S3）。

### 18.1 Spill 触发条件

```
NodeManager::SpillIfOverPrimaryObjectsThreshold()
  → LocalObjectManager::SpillObjectUptoMaxThroughput() (local_object_manager.cc:162)
    → 循环调用 TryToSpillObjects() 直到没有更多可 spill 或 worker 用满
```

### 18.2 Spillable 判定

```cpp
// store.cc:562
bool PlasmaStore::IsObjectSpillable(const ObjectID &object_id) {
    absl::MutexLock lock(&mutex_);
    auto entry = object_lifecycle_mgr_.GetObject(object_id);
    if (!entry) return false;
    return entry->Sealed() && entry->GetRefCount() == 1;
    // 只有 ref_count==1 (只有一个 client 持有) 才可 spill
}
```

**只有 primary pin 的对象可 spill**：
- Primary pin：raylet PlasmaClient A Create → Seal → Release → ref_count=1 → 满足条件
- Pull pin：同样 ref_count=1，但 `LocalObjectManager::TryToSpillObjects` 只遍历 `pinned_objects_`（primary pin 集合），不包含 PullManager 的 `pinned_objects_`
- **ReceivedByPush**：ref_count=0，不可 spill（也无需 spill，已经被 LRU 淘汰了）

### 18.3 Spill 执行流程

```
1. LocalObjectManager::TryToSpillObjects() (local_object_manager.cc:186)
   → 遍历 pinned_objects_，找 is_plasma_object_spillable_==true 的对象
   → 最多合并 max_fused_object_count_ 个对象到一个文件
   → 构造 objects_to_spill 列表

2. 对象从 pinned_objects_ 移到 objects_pending_spill_:
   → pinned_objects_.erase(it)
   → objects_pending_spill_[id] = std::move(it->second)
   → num_bytes_pending_spill_ += object_size

3. SpillObjectsInternal (local_object_manager.cc:234)
   → io_worker_pool_.PopSpillWorker()  // 获取一个 I/O worker
   → gRPC SpillObjects → CoreWorker::HandleSpillObjects (core_worker.cc:4214)
     → options_.spill_objects(object_refs)  // Python 层写外部存储
     → 返回 spilled_objects_url[]

4. Spill 成功 → OnObjectSpilled (local_object_manager.cc:417)
   → spilled_objects_url_.emplace(object_id, object_url)
   → 从 objects_pending_spill_ 移除
   → 更新 spilled_bytes_total_, spilled_objects_total_

5. 上报 Owner:
   → object_directory_->ReportObjectSpilled(
       object_id, self_node_id_, owner_address, object_url, generator_id, is_local_fs)
     → gRPC UpdateObjectLocationBatch → Owner Worker
     → Owner 更新 spilled_url, spilled_node_id
     → PushToLocationSubscribers → 通知所有订阅者
```

### 18.4 Spill 后对象在 plasma 中的状态

Spill 完成后，对象**仍然在 plasma store 中**（primary pin 持有），但 owner 已知 spill URL。当 Owner OutOfScope 时：
```
Owner → eviction pub/sub → raylet ReleaseFreedObject
  → pinned_objects_.erase → RayObject 析构 → PlasmaClient Release
  → server ref_count 0 → LRU 可淘汰
  → 如果还没被淘汰，plasma 自动清理
  → 如果对象已被 LRU 淘汰，那 spill URL 就是唯一的恢复途径
```

### 18.5 Pull pin 对象的 spill 行为

- Pull pin 的对象**不会被主动 spill**（不在 `LocalObjectManager::pinned_objects_` 中）
- Pull 请求结束后，`PullManager::UnpinObject` → RayObject 析构 → ref_count=0 → **被 LRU 淘汰**（evict，非 spill）
- Evict 和 Spill 的区别：evict 不保存到外部存储，数据直接丢失；spill 保存后可恢复

---

## 19. Spill 恢复流程详解

当 PullManager 需要拉取一个对象，但该对象没有内存副本时，可以从 spill 存储中恢复。

### 19.1 TryToMakeObjectLocal 完整优先级

```cpp
// pull_manager.cc:447
void PullManager::TryToMakeObjectLocal(const ObjectID &object_id) {
    // 优先级1: 从有内存副本的节点 pull
    bool did_pull = PullFromRandomLocation(object_id);
    if (did_pull) return;

    // 优先级2: 本地 spill 文件直接 restore
    std::string direct_restore_url = get_locally_spilled_object_url_(object_id);
    // → LocalObjectManager::GetLocalSpilledObjectURL
    //   → is_external_storage_type_fs_==true 才有值
    //   → 返回 spilled_objects_url_[object_id]

    // 优先级3: S3 等外部存储 URL restore
    if (direct_restore_url.empty()) {
        if (!request.spilled_url.empty() && request.spilled_node_id.IsNil()) {
            direct_restore_url = request.spilled_url;
        }
    }

    if (!direct_restore_url.empty()) {
        restore_spilled_object_(object_id, object_size, url, callback);
    }

    // 优先级4: 都没有 → 等待 reconstruction 或超时
}
```

### 19.2 PullFromRandomLocation 内部优先级

```cpp
// pull_manager.cc:511
bool PullManager::PullFromRandomLocation(const ObjectID &object_id) {
    auto &node_vector = request.client_locations;  // 有内存副本的节点
    auto &spilled_node_id = request.spilled_node_id;

    if (!node_vector.empty()) {
        // 子优先级 A: 从随机一个内存节点 pull
        int node_index = random(0, node_vector.size()-1);
        send_pull_request_(object_id, node_vector[node_index]);
        return true;
    }

    if (!spilled_node_id.IsNil() && spilled_node_id != self_node_id_) {
        // 子优先级 B: 向 spill 所在节点发 pull request
        // 远端会自动从磁盘恢复并推送
        send_pull_request_(object_id, spilled_node_id);
        return true;
    }

    // spilled_node_id == self_node_id_ (本地有 spill)
    // → 返回 false，让后面走本地 restore 路径
    return false;
}
```

**关键**：如果 `spilled_node_id == self_node_id_`（spill 在本节点），不会向自己发 pull request，避免 gRPC 自己跟自己通信。返回 false 后走本地 restore 路径。

### 19.3 完整优先级排序

| 优先级 | 路径 | 延迟组成 | 条件 |
|--------|------|-----------|------|
| 1 | 远端内存节点 pull | 网络传输 | owner locations 中有其他节点有内存副本 |
| 2 | 本地 spill restore | 本地磁盘读 + plasma 写入 | 本地有 spill 文件（fs 类型存储） |
| 3 | 远端 spill 节点 pull | 远端磁盘读 + 网络传输 | `spilled_node_id` 非空且非本节点 |
| 4 | S3 外部存储 restore | 网络下载 + 本地写入 | `spilled_node_id.IsNil()`，有 URL |
| 5 | 等待 reconstruction | 重新计算 | 都没有 |

### 19.4 远端 spill 节点的自动恢复

当 PullManager 向 spill 所在节点发 pull request 时：

```
节点 A (PullManager)                节点 B (spill 所在节点)
────────────────                    ────────────────────
send_pull_request_(object_id, B)
  → gRPC PullRequest ───────────→  ObjectManager::HandlePull (object_manager.cc:616)
                                     → Push(object_id, node_id=A)

                                   Push() 逻辑 (object_manager.cc:321):
                                   ├ local_objects_ 有? → PushLocalObject (内存推送)
                                   ├ 本地有 spill 文件?
                                   │  → PushFromFilesystem (从磁盘读→推送)
                                   │    → SpilledObjectReader::CreateSpilledObjectReader(url)
                                   │    → chunk_object_reader
                                   │    → PushObjectInternal(from_disk=true)
                                   └ 都没有? → 加入 unfulfilled_push_requests_
                                       → 等对象恢复后再推送
```

`PushFromFilesystem` (object_manager.cc:411) 将磁盘读取调度到 RPC 线程（off-main-thread），不阻塞主事件循环。

### 19.5 本地 restore 流程

```
1. get_locally_spilled_object_url_(object_id) 返回非空 URL

2. restore_spilled_object_(object_id, object_size, url, callback)
   → ObjectManager → LocalObjectManager::AsyncRestoreSpilledObject
     (local_object_manager.cc:470)
     → io_worker_pool_.PopRestoreWorker()  // 获取 restore I/O worker
     → gRPC RestoreSpilledObjects → CoreWorker::HandleRestoreSpilledObjects
       → options_.restore_spilled_objects(object_refs, spilled_urls)
         → Python 层读取外部存储 → 写回 plasma store

3. restore 成功 → 对象重新进入 plasma → HandleObjectAdded
   → ReportObjectAdded → Owner 更新 locations
```

### 19.6 S3 外部存储 restore

当 `spilled_node_id.IsNil()` 时，说明对象 spill 到了 S3 等外部存储，没有具体节点可以请求。这时直接从 URL 下载：

```
condition: !request.spilled_url.empty() && request.spilled_node_id.IsNil()
  → direct_restore_url = request.spilled_url  // S3 URL
  → restore_spilled_object_(...)  // 同本地 restore 路径
    → Python 层从 S3 下载 → 写入 plasma
```

---

## 20. 关键代码索引（补充）

### Pub/Sub 基础设施

| 文件 | 行号 | 函数 | 说明 |
|------|------|------|------|
| `pubsub/subscriber_interface.h` | 40 | `SubscriberInterface` | 订阅者接口 |
| `pubsub/subscriber.cc` | 261 | `Subscriber::Subscribe` | 注册订阅 + 建立长轮询 |
| `pubsub/subscriber.cc` | 296 | `MakeLongPollingPubsubConnection` | 建立/复用 gRPC 长轮询 |
| `pubsub/subscriber.cc` | 321 | `HandleLongPollingResponse` | 处理长轮询回复 |
| `pubsub/subscriber.cc` | 308 | `MakeLongPollingConnectionIfNotConnected` | 每个 publisher 只建一条连接 |
| `pubsub/publisher.cc` | 364 | `Publisher::ConnectToSubscriber` | 注册长轮询连接 |
| `pubsub/publisher.cc` | 421 | `Publisher::Publish` | 发布消息到 mailbox + 触发长轮询回复 |
| `pubsub/publisher.cc` | 28 | `EntityState::Publish` | 消息入 mailbox + PublishIfPossible |

### Object Location 上报

| 文件 | 行号 | 函数 | 说明 |
|------|------|------|------|
| `object_manager/ownership_object_directory.cc` | 121 | `ReportObjectAdded` | 上报内存副本增加 (ADDED) |
| `object_manager/ownership_object_directory.cc` | 144 | `ReportObjectRemoved` | 上报内存副本移除 (REMOVED) |
| `object_manager/ownership_object_directory.cc` | 167 | `ReportObjectSpilled` | 上报 spill 完成 (spilled_url) |
| `object_manager/ownership_object_directory.cc` | 228 | `SendObjectLocationUpdateBatchIfNeeded` | 批量发送 location 更新 |
| `core_worker/core_worker.cc` | 3705 | `HandleUpdateObjectLocationBatch` | Owner 接收 location 更新 |
| `core_worker/core_worker.cc` | 3774 | `AddObjectLocationOwner` | 添加内存副本节点 |
| `core_worker/core_worker.cc` | 3805 | `RemoveObjectLocationOwner` | 移除内存副本节点 |
| `core_worker/reference_counter.cc` | 1443 | `AddObjectLocation` | locations.emplace(node_id) |
| `core_worker/reference_counter.cc` | 1470 | `RemoveObjectLocation` | locations.erase(node_id) |
| `core_worker/reference_counter.cc` | 1520 | `HandleObjectSpilled` | 记录 spilled_url, spilled_node_id |

### WorkerObjectEviction Pub/Sub

| 文件 | 行号 | 函数 | 说明 |
|------|------|------|------|
| `raylet/local_object_manager.cc` | 57 | `PinObjectsAndWaitForFree` | 订阅 owner eviction |
| `raylet/local_object_manager.cc` | 68 | `subscription_callback` | 收到 eviction → ReleaseFreedObject |
| `raylet/local_object_manager.cc` | 76 | `owner_dead_callback` | Owner 死亡 → ReleaseFreedObject |
| `core_worker/core_worker.cc` | 3562 | `ProcessSubscribeForObjectEviction` | Owner 注册 eviction 回调 |
| `core_worker/reference_counter.cc` | 839 | `OnObjectOutOfScopeOrFreed` | 遍历 callbacks → 触发 eviction |
| `core_worker/reference_counter.cc` | 889 | `AddObjectOutOfScopeOrFreedCallback` | 注册 eviction 回调 |

### WorkerObjectLocations Pub/Sub

| 文件 | 行号 | 函数 | 说明 |
|------|------|------|------|
| `object_manager/ownership_object_directory.cc` | 320 | `SubscribeObjectLocations` | 订阅对象位置变更 |
| `object_manager/ownership_object_directory.cc` | 264 | `ObjectLocationSubscriptionCallback` | 收到位置更新 |
| `core_worker/reference_counter.cc` | 1678 | `PushToLocationSubscribers` | 发布位置快照 |
| `core_worker/reference_counter.cc` | 1713 | `FillObjectInformationInternal` | 填充 locations/spilled_url 等 |
| `core_worker/reference_counter.cc` | 1731 | `PublishObjectLocationSnapshot` | 首次订阅时发布全量快照 |
| `core_worker/core_worker.cc` | 3642 | `ProcessSubscribeObjectLocations` | Owner 处理位置订阅 |

### Spill 相关

| 文件 | 行号 | 函数 | 说明 |
|------|------|------|------|
| `object_manager/plasma/store.cc` | 560 | `IsObjectSpillable` | 判定 ref_count==1 且 sealed |
| `raylet/local_object_manager.cc` | 186 | `TryToSpillObjects` | 遍历 pinned_objects_ 找可 spill 对象 |
| `raylet/local_object_manager.cc` | 234 | `SpillObjectsInternal` | 调度 I/O worker 执行 spill |
| `raylet/local_object_manager.cc` | 417 | `OnObjectSpilled` | spill 完成回调 |
| `raylet/local_object_manager.cc` | 440 | `ReportObjectSpilled` | 上报 spill URL 给 Owner |
| `core_worker/core_worker.cc` | 4214 | `HandleSpillObjects` | I/O Worker 执行实际 spill |

### Spill 恢复相关

| 文件 | 行号 | 函数 | 说明 |
|------|------|------|------|
| `object_manager/pull_manager.cc` | 447 | `TryToMakeObjectLocal` | 恢复优先级入口 |
| `object_manager/pull_manager.cc` | 511 | `PullFromRandomLocation` | 内存节点 / 远端 spill 节点 |
| `object_manager/pull_manager.cc` | 473 | `get_locally_spilled_object_url_` | 本地 spill URL 查询 |
| `object_manager/pull_manager.cc` | 485 | `restore_spilled_object_` | 触发 restore |
| `raylet/local_object_manager.cc` | 470 | `AsyncRestoreSpilledObject` | 本地 restore 调度 |
| `raylet/local_object_manager.cc` | 449 | `GetLocalSpilledObjectURL` | 本地 spill URL 获取 |
| `core_worker/core_worker.cc` | 4230 | `HandleRestoreSpilledObjects` | I/O Worker 执行实际 restore |
| `object_manager/object_manager.cc` | 321 | `Push` | 远端 Push 入口（内存 / 磁盘 / 等待） |
| `object_manager/object_manager.cc` | 355 | `PushLocalObject` | 从内存推送 |
| `object_manager/object_manager.cc` | 411 | `PushFromFilesystem` | 从 spill 文件推送 |
| `object_manager/spilled_object_reader.cc` | 29 | `CreateSpilledObjectReader` | 从 URL 创建磁盘读取器 |

---

## 21. 对象生命周期与引用保护层级

Ray 中 pull 到的临时对象有多层引用保护，各层独立、释放互不影响：

| 保护层 | Pin 方式 | 上报 Owner | 订阅 Eviction | Spill/Delete | 释放时机 |
|--------|----------|-----------|--------------|-------------|---------|
| PullManager pin | plasma 客户端引用 | 不上报 | 不订阅 | 不涉及 | CancelPull → UnpinObject |
| PinLeaseArgs | plasma 客户端引用 | 不上报 | 不订阅 | 不涉及 | CleanupLease → ReleaseLeaseArgs |
| PinObjectsAndWaitForFree | plasma 客户端引用 + 订阅 | 上报 Owner | 订阅 WorkerObjectEviction | 完整生命周期 | Owner GC / Owner 死亡 |

**PullManager pin**：`TryPinObject` 获取 plasma 引用，`CancelPull` → `DeactivateBundlePullRequest` → `UnpinObject` 释放。只保护"拉取意图"和临时引用。

**PinLeaseArgs**：`PinLeaseArgsIfMemoryAvailable` 获取 plasma 引用（refcount++），`ReleaseLeaseArgs` 释放（refcount--，归零时释放 RayObject unique_ptr）。只在 lease 调度期间保护参数不被 LRU。

**PinObjectsAndWaitForFree**：只有 recovery / tidal transfer / restore 场景才触发，长期 pin，owner GC 时释放，走完整 spill → delete 生命周期。

PullManager 的 `PinNewObjectIfNeeded` 不等于 `PinObjectsAndWaitForFree`：
- PullManager pin 只是 plasma 客户端引用级 pin，不上报 Owner，不订阅 eviction
- `PinObjectsAndWaitForFree` 是长期 pin，上报 Owner，走完整 spill/delete 生命周期
- Pull 来的临时对象如果不走 `PinObjectsAndWaitForFree`，lease 完成后可被 LRU 淘汰

**三层保护的接力关系**（详见第9.3-9.7节完整代码逻辑）：

```
PullManager pin ──(PinLeaseArgs 接管)──→ PinLeaseArgs pin ──(lease 完成)──→ 无保护 → LRU 可淘汰
                                            │
                                            └─(如果对象被 Owner pin)──→ PinObjectsAndWaitForFree
                                                                       └─(Owner GC)──→ ReleaseFreedObject
```

---

## 22. HandleObjectMissing vs HandleObjectFreed

| | HandleObjectMissing | HandleObjectFreed |
|---|---|---|
| **触发原因** | plasma LRU 淘汰，object 从内存中消失 | owner worker 通知 object out of scope（GC 释放） |
| **语义** | "内存中没了，但磁盘副本仍有效" | "对象生命周期结束，磁盘副本也应该删除" |
| **对 spilled entry** | 保留（磁盘副本还在，可 restore） | 删除（owner 不要了，磁盘也要清掉） |
| **对 spilled_replicated_bytes_current_** | 不减（磁盘占用还在） | 减（磁盘占用释放） |
| **对 subscription** | spilled entry 保留订阅不变 | 取消订阅 |
| **是否触发磁盘删除** | 否 | 是（`on_spilled_replicated_delete_` → 入 delete 队列） |

---

## 23. Pull 依赖对象的完整生命周期（概览）

> 完整代码逻辑详见第9.1-9.8节。此处为精简的流程概览。

```
① RequestLeaseDependencies
│  ├─ 记录依赖关系到 required_objects_
│  └─ lease_entry->pull_request_id_ = object_manager_.Pull(required_objects, TASK_ARGS)
│     → PullManager 开始从远端 fetch objects → PinNewObjectIfNeeded（plasma 级 pin）

② Pull 完成 → 对象到达本地 plasma
│  ├─ HandleObjectLocal → DecrementMissingDependencies
│  │  → 所有依赖就绪 → lease 进入调度队列
│  └─ PullManager.PinNewObjectIfNeeded → TryPinObject（plasma 客户端引用级 pin）

③ Lease 调度 — PinLeaseArgs 接管保护
│  ├─ PinLeaseArgsIfMemoryAvailable → get_lease_arguments_ → PinLeaseArgs
│  │  → pinned_lease_arguments_[dep] = (RayObject, refcount++)  // 防止 LRU 淘汰
│  ├─ RemoveLeaseDependencies → CancelPull(pull_request_id_)
│  │  → PullManager.DeactivateBundlePullRequest → UnpinObject（释放 PullManager pin）
│  │  // 此时 PinLeaseArgs 已接管保护，对象不会被 LRU
│  └─ PopWorker → GrantLease → worker 执行任务

④ Lease 完成
   └─ CleanupLease → ReleaseLeaseArgs
      → pinned_lease_arguments_[dep].refcount--
      → refcount == 0 → 释放 RayObject unique_ptr
         → 如果 PinObjectsAndWaitForFree 没有长期 pin → 对象可被 plasma LRU 淘汰
```

---

## 24. CancelPull 的必要性

> 完整代码逻辑详见第9.6节。此处为精简要点。

`CancelPull` 不仅是"停止拉取"，更重要的是清理 PullManager 中的残留状态和资源：

| 资源 | 存储位置 | 影响 | 不清理后果 |
|------|---------|------|-----------|
| `pinned_objects_` | PullManager | 占用 plasma 内存配额 (server ref > 0) | 对象永不释放，内存泄漏 |
| `object_pull_requests_` | PullManager | 维护 pull 状态和重试计时器 | 定时器持续触发无效重试 |
| 位置订阅 | ObjectDirectory | 订阅对象位置变化 | 持续网络开销、回调触发 |
| `active_object_pull_requests_` | PullManager | 占用 admission control 配额 | 新 pull 被限流 |

Pull 完成后如果不 CancelPull，这些资源永远不会被清理。

---

## 25. PlasmaClient::Release → SendReleaseRequest → Server RemoveReference 完整 IPC 链路

### 25.1 Client 端：PlasmaClient::Release

```cpp
// client.cc:487-530
Status PlasmaClient::Release(const ObjectID &object_id) {
  std::lock_guard<std::recursive_mutex> guard(client_mutex_);
  const auto object_entry = objects_in_use_.find(object_id);
  RAY_CHECK(object_entry != objects_in_use_.end());

  object_entry->second->count -= 1;  // ★ count 递减
  RAY_CHECK_GE(object_entry->second->count, 0);

  if (object_entry->second->count == 0) {  // ★ 只有 count==0 才发 IPC
    RAY_RETURN_NOT_OK(MarkObjectUnused(object_id));   // 从 objects_in_use_ 移除
    RAY_RETURN_NOT_OK(SendReleaseRequest(store_conn_, object_id, may_unmap));
    // ★ SendReleaseRequest 通过 Unix domain socket 发送 PlasmaReleaseRequest flatbuffer
  }
  return Status::OK();
}
```

**关键**：count > 0 的 Release 是静默的，不发 IPC。只有 count 归零时才通知 server。

### 25.2 IPC 消息：SendReleaseRequest

```cpp
// protocol.cc:SendReleaseRequest
// 构造 PlasmaReleaseRequest flatbuffer:
//   object_id: 被释放的对象 ID
//   may_unmap: 客户端是否知道此对象用了 fallback-allocated fd
// 通过 store_conn_ (Unix domain socket) 发送给 Plasma Store
```

### 25.3 Server 端：ProcessClientMessage → PlasmaReleaseRequest

```cpp
// store.cc:433-451
case fb::MessageType::PlasmaReleaseRequest: {
    bool may_unmap;
    ObjectID object_id;
    ReadReleaseRequest(input, input_size, &object_id, &may_unmap);
    bool should_unmap = ReleaseObject(object_id, client);  // ★ client 标识释放者
    if (!may_unmap) {
      RAY_CHECK(!should_unmap)
          << "Plasma client thinks a mmap should not be unmapped but server thinks so.";
    }
    if (may_unmap) {
      RAY_RETURN_NOT_OK(
          SendReleaseReply(client, object_id, should_unmap, PlasmaError::OK));
    }
  } break;
```

**may_unmap vs should_unmap**：
- `may_unmap`：client 端判断（基于是否知道有 fallback fd）
- `should_unmap`：server 端判断（基于 `RemoveFromClientObjectIds` → `MarkObjectAsUnused` → fallback fd refcount 归零）
- server 回复 `should_unmap` 给 client，client 据此决定是否 munmap

### 25.4 ReleaseObject → RemoveFromClientObjectIds → RemoveReference

```cpp
// store.cc:265-273
bool PlasmaStore::ReleaseObject(const ObjectID &object_id,
                                const std::shared_ptr<Client> &client) {
  auto entry = object_lifecycle_mgr_.GetObject(object_id);
  if (entry != nullptr) {
    return RemoveFromClientObjectIds(object_id, client);
  }
  return false;
}

// store.cc:247-262
bool PlasmaStore::RemoveFromClientObjectIds(const ObjectID &object_id,
                                            const std::shared_ptr<Client> &client) {
  auto &object_ids = client->GetObjectIDs();
  auto it = object_ids.find(object_id);
  if (it != object_ids.end()) {
    bool should_unmap = client->MarkObjectAsUnused(object_id);
    object_lifecycle_mgr_.RemoveReference(object_id);  // ★ server ref -1
    return should_unmap;
  } else {
    return false;
  }
}
```

### 25.5 RemoveReference → EndObjectAccess → 加入 LRU

```cpp
// obj_lifecycle_mgr.cc:148-168
bool ObjectLifecycleManager::RemoveReference(const ObjectID &object_id) {
  auto entry = object_store_->GetObject(object_id);
  entry->ref_count_--;
  if (entry->ref_count_ > 0) {
    return true;  // 还有其他 client 在用
  }
  // ref_count == 0
  eviction_policy_->EndObjectAccess(object_id);  // 加入 LRU → 可淘汰
  if (earger_deletion_objects_.count(object_id) > 0) {
    DeleteObjectInternal(object_id);  // 立即删除
  }
  return true;
}
```

### 25.6 DeleteObjectInternal → delete_object_callback_ → 通知 raylet

```cpp
// obj_lifecycle_mgr.cc:245-258
void ObjectLifecycleManager::DeleteObjectInternal(const ObjectID &object_id) {
  auto entry = object_store_->GetObject(object_id);
  bool aborted = entry->state_ == ObjectState::PLASMA_CREATED;
  stats_collector_->OnObjectDeleting(*entry);
  earger_deletion_objects_.erase(object_id);
  eviction_policy_->RemoveObject(object_id);
  object_store_->DeleteObject(object_id);  // 释放内存 + 从 object_table_ 移除

  if (!aborted) {
    delete_object_callback_(object_id);  // ★ 通知 raylet
  }
}
```

### 25.7 delete_object_callback_ 的连接

```cpp
// main.cc:808-816
[&](const ray::ObjectID &object_id) {
  main_service.post(
      [&object_manager, &node_manager, object_id]() {
        object_manager->HandleObjectDeleted(object_id);
        node_manager->HandleObjectMissing(object_id);
      },
      "ObjectManager.ObjectDeleted");
}
```

**从 plasma store 线程 post 到 raylet 主线程**，执行两个操作：
1. `HandleObjectDeleted`：清理本地 bookkeeping，上报 Owner `ReportObjectRemoved`，重置 pull 重试计时器
2. `HandleObjectMissing`：通知 LeaseDependencyManager 对象不再本地，增加依赖此对象的 lease 的 missing_dependencies

### 25.8 HandleObjectDeleted 完整代码

```cpp
// object_manager.cc:200-212
void ObjectManager::HandleObjectDeleted(const ObjectID &object_id) {
  auto it = local_objects_.find(object_id);
  RAY_CHECK(it != local_objects_.end());
  auto object_info = it->second.object_info;
  local_objects_.erase(it);
  used_memory_ -= object_info.data_size + object_info.metadata_size;
  RAY_CHECK(!local_objects_.empty() || used_memory_ == 0);
  object_directory_->ReportObjectRemoved(object_id, self_node_id_, object_info);
  pull_manager_->ResetRetryTimer(object_id);
}
```

### 25.9 HandleObjectMissing 完整代码

```cpp
// node_manager.cc:2454-2469
void NodeManager::HandleObjectMissing(const ObjectID &object_id) {
  const auto waiting_lease_ids = lease_dependency_manager_.HandleObjectMissing(object_id);
  // ... 日志
}

// lease_dependency_manager.cc:276-298
std::vector<LeaseID> LeaseDependencyManager::HandleObjectMissing(
    const ray::ObjectID &object_id) {
  RAY_CHECK(local_objects_.erase(object_id));
  std::vector<LeaseID> waiting_lease_ids;
  auto object_entry = required_objects_.find(object_id);
  if (object_entry != required_objects_.end()) {
    for (auto &dependent_lease_id : object_entry->second.dependent_leases) {
      auto &lease_entry = queued_lease_requests_[dependent_lease_id];
      if (lease_entry->num_missing_dependencies_ == 0) {
        waiting_lease_ids.push_back(dependent_lease_id);
      }
      lease_entry->IncrementMissingDependencies();  // ★ 依赖计数 +1
    }
  }
  return waiting_lease_ids;
}
```

### 25.10 完整链路总结

```
PlasmaClient::Release (client count -1)
  → count == 0?
    ├─ No → 静默返回，不发 IPC
    └─ Yes → MarkObjectUnused (从 objects_in_use_ 移除)
           → SendReleaseRequest [Unix socket IPC]
             → PlasmaStore::ProcessClientMessage (PlasmaReleaseRequest)
               → ReleaseObject(object_id, client)
                 → RemoveFromClientObjectIds
                   → client->MarkObjectAsUnused (从 client object_ids 移除)
                   → RemoveReference (server ref -1)
                     → ref > 0? → 返回
                     → ref == 0?
                       → EndObjectAccess (加入 LRU，可淘汰)
                       → 或 EvictObjects/DeleteObjectInternal:
                         → object_store_->DeleteObject (释放内存)
                         → delete_object_callback_ [post 到 raylet 主线程]
                           → HandleObjectDeleted (上报 Owner, 清理 bookkeeping)
                           → HandleObjectMissing (通知 lease 依赖缺失)
```

---

## 26. Owner Ref 深度分析：AddObjectOutOfScopeOrFreedCallback 与 Lineage 机制

### 26.1 LineageReconstructionEligibility 枚举

```cpp
// reference_counter_interface.h:36-49
enum class LineageReconstructionEligibility {
  ELIGIBLE,                    // 可通过重执行 task 来恢复对象
  INELIGIBLE_PUT,              // ray.put() 创建，没有 task lineage 可重放
  INELIGIBLE_NO_RETRIES,       // max_retries=0，任务不允许重试
  INELIGIBLE_LINEAGE_EVICTED,  // lineage 因内存压力被 evict
  INELIGIBLE_LINEAGE_DISABLED,  // 系统禁用了 lineage pinning
  INELIGIBLE_REF_NOT_FOUND,    // ref table 中找不到
};
```

**ELIGIBLE 的含义**：这个对象可以通过重新执行产生它的 task 来恢复。前提是 task 的 lineage（依赖链）还在，且 task 还允许重试。

**lineage_eligibility_ 的设置**：

```cpp
// task_manager.cc:268-275 — AddPendingTask 中
if (!spec.IsActorCreationTask()) {
  LineageReconstructionEligibility lineage_eligibility;
  if (max_retries == 0) {
    lineage_eligibility = LineageReconstructionEligibility::INELIGIBLE_NO_RETRIES;
  } else {
    lineage_eligibility = LineageReconstructionEligibility::ELIGIBLE;  // ★ 通常情况
  }
  // ... 传递给 AddOwnedObject
}
```

**streaming generator return objects 继承 generator 的 eligibility**：

```cpp
// reference_counter.cc:240-274 — OwnDynamicStreamingTaskReturnRef
void ReferenceCounter::OwnDynamicStreamingTaskReturnRef(const ObjectID &object_id,
                                                        const ObjectID &generator_id) {
  auto outer_it = object_id_refs_.find(generator_id);
  RAY_CHECK(outer_it->second.owned_by_us_);
  RAY_UNUSED(AddOwnedObjectInternal(object_id,
                                    {},
                                    owner_address,
                                    outer_it->second.call_site_,
                                    /*object_size=*/-1,
                                    outer_it->second.lineage_eligibility_,  // ★ 继承
                                    /*add_local_ref=*/true,
                                    std::optional<NodeID>(),
                                    /*tensor_transport=*/std::nullopt));
}
```

### 26.2 OutOfScope 与 ShouldDelete — lineage_eligibility_ 的影响

```cpp
// reference_counter.h:197-215
bool OutOfScope(bool lineage_pinning_enabled) const {
  bool in_scope = RefCount() > 0;
  bool is_nested = !nested().contained_in_borrowed_ids.empty();
  bool has_borrowers = !borrow().borrowers.empty();
  bool was_stored_in_objects = !borrow().stored_in_objects.empty();

  bool has_lineage_references = false;
  if (lineage_pinning_enabled && owned_by_us_ &&
      lineage_eligibility_ != LineageReconstructionEligibility::ELIGIBLE) {
    // ★ 只有非 ELIGIBLE 时 lineage_ref_count 才影响 OutOfScope
    has_lineage_references = lineage_ref_count > 0;
  }
  // ELIGIBLE 时 has_lineage_references 永远是 false

  return !(in_scope || is_nested || has_nested_refs_to_report || has_borrowers ||
           was_stored_in_objects || has_lineage_references);
}

// reference_counter.h:217-224
bool ShouldDelete(bool lineage_pinning_enabled) const {
  if (lineage_pinning_enabled) {
    return OutOfScope(lineage_pinning_enabled) && (lineage_ref_count == 0);
    // ★ OutOfScope=true 但 lineage_ref_count > 0 → ShouldDelete=false
  } else {
    return OutOfScope(lineage_pinning_enabled);
  }
}
```

**为什么 ELIGIBLE 不看 lineage_ref_count，非 ELIGIBLE 要看**：

| eligibility | RefCount=0, lineage_ref>0 | OutOfScope | Plasma 副本 | 含义 |
|-------------|---------------------------|------------|-------------|------|
| ELIGIBLE | has_lineage_references=false | **true** | 释放（eviction） | 可重建，不需要保留副本 |
| INELIGIBLE_PUT | has_lineage_references=true | **false** | 保留 | 不可重建，必须保留副本 |
| INELIGIBLE_NO_RETRIES | has_lineage_references=true | **false** | 保留 | 不可重试，必须保留副本 |

**核心区别**：ELIGIBLE 对象丢了可以重建，所以 `OutOfScope=true` 时即使 `lineage_ref_count>0` 也释放 Plasma 副本。但 `ShouldDelete` 仍要求 `lineage_ref_count==0` — ref 记录保留在 `object_id_refs_` 中，只是 Plasma 副本被释放了。非 ELIGIBLE 对象不能重建，必须保留 Plasma 副本到 `lineage_ref_count==0`。

### 26.3 AddObjectOutOfScopeOrFreedCallback — 四分支详细逻辑

```cpp
// reference_counter.cc:581-595
bool ReferenceCounter::AddObjectOutOfScopeOrFreedCallback(
    const ObjectID &object_id, const std::function<void(const ObjectID &)> callback) {
  absl::MutexLock lock(&mutex_);
  auto it = object_id_refs_.find(object_id);

  // ─── 分支1: 对象不在 ref table 中 ───
  if (it == object_id_refs_.end()) {
    return false;  // 对象已完全 GC（从 ref table 中删除）
  }

  // ─── 分支2: 对象已 OutOfScope 但不能 Delete ───
  else if (it->second.OutOfScope(lineage_pinning_enabled_) &&
           !it->second.ShouldDelete(lineage_pinning_enabled_)) {
    // OutOfScope=true 但 lineage_ref_count > 0（仅 ELIGIBLE 对象）
    // OnObjectOutOfScopeOrFreed 已经执行过 → callbacks 已清空
    // 注册新 callback 永远不会被触发 → 必须返回 false → 立即 unpin
    return false;
  }

  // ─── 分支3: 对象已被 freed ───
  else if (freed_objects_.contains(object_id)) {
    // 应用层主动 free 了对象（如 ray.internal.free）
    return false;  // 立即 unpin
  }

  // ─── 分支4: 对象还在 scope → 注册回调 ───
  it->second.on_object_out_of_scope_or_freed_callbacks.emplace_back(callback);
  return true;  // 注册成功，等 OutOfScope 时触发
}
```

**为什么分支2不能只判断 OutOfScope**：

如果只判断 `OutOfScope` 就返回 false，情况 B（`OutOfScope && ShouldDelete`）也会返回 false → 立即 unpin。但此时 `DeleteReferenceInternal` 可能还没执行，`OnObjectOutOfScopeOrFreed` 也还没被调用。虽然立即 unpin 也能释放 raylet 的 Plasma 副本，但跳过了正常的 callback 流程，可能导致其他已注册的 callback 未被正确触发。

`OutOfScope && !ShouldDelete` 精确区分了：
- **不能注册 callback**：`OnObjectOutOfScopeOrFreed` 已执行过，callbacks 已清空（情况 A：ELIGIBLE + lineage_ref>0）
- **可以注册 callback**：`DeleteReferenceInternal` 还没执行，callbacks 还没被触发（情况 B：RefCount>0）

所有分支的精确条件：

| 分支 | 条件 | 返回值 | 含义 |
|------|------|--------|------|
| 1 | `object_id_refs_` 中找不到 | false | 对象已完全 GC（从 ref table 中删除） |
| 2 | `OutOfScope && !ShouldDelete` | false | RefCount=0 但 lineage_ref>0（仅 ELIGIBLE） → callbacks 已清空，立即 unpin |
| 3 | 在 `freed_objects_` 中 | false | 应用层主动 free → 立即 unpin |
| 4 | 其他（还在 scope） | true | 注册回调，等 OutOfScope 时触发 |

### 26.4 DeleteReferenceInternal — 完整代码

```cpp
// reference_counter.cc:466-503
void ReferenceCounter::DeleteReferenceInternal(ReferenceTable::iterator it,
                                               std::vector<ObjectID> *deleted) {
  const ObjectID id = it->first;

  if (it->second.RefCount() == 0 && it->second.publish_ref_removed) {
    PublishRefRemovedInternal(id);
    it->second.publish_ref_removed = false;
  }

  if (it->second.OutOfScope(lineage_pinning_enabled_)) {
    // 递归处理 nested 对象
    for (const auto &inner_id : it->second.nested().contains) {
      auto inner_it = object_id_refs_.find(inner_id);
      if (inner_it != object_id_refs_.end()) {
        if (it->second.owned_by_us_) {
          RAY_CHECK(inner_it->second.mutable_nested()->contained_in_owned.erase(id));
        } else {
          RAY_CHECK(inner_it->second.mutable_nested()->contained_in_borrowed_ids.erase(id));
        }
        DeleteReferenceInternal(inner_it, deleted);
      }
    }
    OnObjectOutOfScopeOrFreed(it);  // ★ 触发 eviction callbacks
    if (deleted != nullptr) {
      deleted->push_back(id);
    }
  }

  if (it->second.ShouldDelete(lineage_pinning_enabled_)) {
    ReleaseLineageReferences(it);  // 递归减少依赖对象的 lineage_ref_count
    EraseReference(it);            // 从 object_id_refs_ 中删除
  }
}
```

**关键**：`OutOfScope` 和 `ShouldDelete` 是两个独立判断。ELIGIBLE 对象可能 `OutOfScope=true`（触发 eviction，释放 Plasma 副本）但 `ShouldDelete=false`（`lineage_ref_count>0`，ref 记录保留）。

### 26.5 OnObjectOutOfScopeOrFreed — 触发并清空 callbacks

```cpp
// reference_counter.cc:563-571
void ReferenceCounter::OnObjectOutOfScopeOrFreed(ReferenceTable::iterator it) {
  for (const auto &callback : it->second.on_object_out_of_scope_or_freed_callbacks) {
    callback(it->first);  // ★ 触发所有已注册的回调
  }
  it->second.on_object_out_of_scope_or_freed_callbacks.clear();  // ★ 清空
  UpdateOwnedObjectCounters(it->first, it->second, /*decrement=*/true);
  UnsetObjectPrimaryCopy(it);  // pinned_at_node_id_.reset()
  UpdateOwnedObjectCounters(it->first, it->second, /*decrement=*/false);
}
```

### 26.6 FreePlasmaObjects — 应用层主动 free

```cpp
// reference_counter.cc:358-376
void ReferenceCounter::FreePlasmaObjects(const std::vector<ObjectID> &object_ids) {
  absl::MutexLock lock(&mutex_);
  for (const ObjectID &object_id : object_ids) {
    auto it = object_id_refs_.find(object_id);
    if (it == object_id_refs_.end()) {
      continue;
    }
    freed_objects_.insert(object_id);  // ★ 标记为 freed
    if (!it->second.owned_by_us_) {
      continue;
    }
    OnObjectOutOfScopeOrFreed(it);  // 立即触发 eviction
  }
}
```

**触发路径**：`ray.internal.free(object_id)` → `DeleteImpl` → `FreePlasmaObjects`（应用层主动 free）

### 26.7 TryMarkFreedObjectInUseAgain — Recovery 时重新使用

```cpp
// reference_counter.cc:349-354
bool ReferenceCounter::TryMarkFreedObjectInUseAgain(const ObjectID &object_id) {
  absl::MutexLock lock(&mutex_);
  if (!object_id_refs_.contains(object_id)) {
    return false;
  }
  return freed_objects_.erase(object_id) != 0u;  // ★ 从 freed 集合中移除
}
```

在 `UpdateReferencesForResubmit` 中被调用（`task_manager.cc:460`）—— recovery 重提交时，之前被 free 的依赖对象需要重新使用，清除 freed 标记。

### 26.8 EraseReference — 从 ref table 中完全删除

```cpp
// reference_counter.cc:505-525
void ReferenceCounter::EraseReference(ReferenceTable::iterator it) {
  object_info_publisher_->PublishFailure(
      rpc::ChannelType::WORKER_OBJECT_LOCATIONS_CHANNEL, it->first.Binary());

  RAY_CHECK(it->second.ShouldDelete(lineage_pinning_enabled_));
  // ... 清理 reconstructable 索引
  freed_objects_.erase(it->first);  // ★ 从 freed 集合中移除
  // ... 更新计数器
  for (const auto &callback : it->second.object_ref_deleted_callbacks) {
    callback(it->first);
  }
  object_id_refs_.erase(it);  // ★ 从 ref table 中删除
  ShutdownIfNeeded();
}
```

### 26.9 RemoveSubmittedTaskReferences — lineage_ref_count 递减

```cpp
// reference_counter.cc:308-322
void ReferenceCounter::RemoveSubmittedTaskReferences(
    const std::vector<ObjectID> &argument_ids,
    bool release_lineage,
    std::vector<ObjectID> *deleted) {
  for (const ObjectID &argument_id : argument_ids) {
    auto it = object_id_refs_.find(argument_id);
    if (it == object_id_refs_.end()) {
      return;
    }
    it->second.submitted_task_ref_count--;
    if (release_lineage) {  // ★ 关键分支
      if (it->second.lineage_ref_count > 0) {
        it->second.lineage_ref_count--;  // ★ 递减
      }
    }
    if (it->second.RefCount() == 0) {
      DeleteReferenceInternal(it, deleted);
    }
  }
}
```

**`release_lineage` 的来源**：任务不可再重试时 `release_lineage=true`（如 max_retries 耗尽），此时 lineage_ref_count 才递减。任务仍可重试时 `release_lineage=false`，lineage 保留。

### 26.10 object_id_refs_ 和 freed_objects_ 完整更新生命周期

#### 增加到 object_id_refs_

| 路径 | 函数 | 场景 |
|------|------|------|
| `AddOwnedObject` | `AddOwnedObjectInternal` | 对象首次创建 |
| `OwnDynamicStreamingTaskReturnRef` | `AddOwnedObjectInternal` | streaming generator return 首次上报 |
| `AddBorrowedObject` | emplace | 借用对象 |
| `AddLocalReference` | emplace（如不存在） | 本地引用 |

#### 从 object_id_refs_ 中删除（EraseReference）

**完整条件**：`RefCount() == 0` 且 `OutOfScope == true` 且 `ShouldDelete == true`（即 `lineage_ref_count == 0`）

| 场景 | lineage_eligibility_ | 触发 |
|------|---------------------|------|
| 应用层释放 + lineage 已归零 | 任何 | `RemoveLocalReference` → `RefCount==0` → `DeleteReferenceInternal` → `ShouldDelete=true` → `EraseReference` |
| 应用层释放 + ELIGIBLE + lineage>0 | ELIGIBLE | `OutOfScope=true`（eviction 触发）但 `ShouldDelete=false` → ref 保留 → 等 `lineage_ref_count==0` |
| 应用层释放 + INELIGIBLE + lineage>0 | INELIGIBLE | `OutOfScope=false` → ref 保留 → 等 `lineage_ref_count==0` |

#### freed_objects_ 的增加

| 路径 | 函数 | 场景 |
|------|------|------|
| `FreePlasmaObjects` | `freed_objects_.insert(obj)` | `ray.internal.free` / `SealExisting(pin_object=false)` |

#### freed_objects_ 的移除

| 路径 | 函数 | 场景 |
|------|------|------|
| `EraseReference` | `freed_objects_.erase(it->first)` | 对象完全 GC 时清理 |
| `TryMarkFreedObjectInUseAgain` | `freed_objects_.erase(object_id)` | recovery 重提交时重新使用 |

### 26.11 完整生命周期图

```
对象创建:
  AddOwnedObject → object_id_refs_.insert(obj)
  freed_objects_: 无

应用层使用中:
  RefCount > 0 → OutOfScope = false
  → Plasma 副本保留
  → object_id_refs_ 中有

应用层释放 (del obj):
  RemoveLocalReference → local_ref_count-- → RefCount == 0

  ┌─ ELIGIBLE 对象:
  │   OutOfScope = true (不看 lineage_ref)
  │   → OnObjectOutOfScopeOrFreed → eviction → Plasma 副本释放
  │   → ShouldDelete = (lineage_ref == 0)?
  │     ├─ yes → EraseReference → object_id_refs_.erase(obj) + freed_objects_.erase(obj)
  │     └─ no  → 保留在 object_id_refs_ 中 (无 Plasma 副本, 等 lineage 归零)
  │
  └─ INELIGIBLE 对象:
      OutOfScope = false (lineage_ref > 0 阻止)
      → 不触发 eviction → Plasma 副本保留
      → 等待 lineage_ref_count 归零 → OutOfScope=true → eviction → EraseReference

应用层主动 free (ray.internal.free):
  FreePlasmaObjects:
  → freed_objects_.insert(obj) ★ 标记为 freed
  → OnObjectOutOfScopeOrFreed → eviction → Plasma 副本释放
  → ref 保留在 object_id_refs_ 中 (保留 ownership)
  → 后续 AddObjectOutOfScopeOrFreedCallback 分支3 → return false → 立即 unpin

lineage 最终释放:
  RemoveSubmittedTaskReferences(release_lineage=true) → lineage_ref_count--
  → lineage_ref_count == 0:
    → ShouldDelete = true
    → EraseReference:
      → object_id_refs_.erase(obj)
      → freed_objects_.erase(obj)

recovery 重提交时:
  TryMarkFreedObjectInUseAgain(obj):
  → freed_objects_.erase(obj) → 对象重新可用
```

### 26.12 状态完整对照表

| RefCount | lineage_ref | eligibility | OutOfScope | ShouldDelete | 在 ref_table | Plasma 副本 |
|----------|-----------|-------------|------------|--------------|-------------|-------------|
| >0 | 任何值 | 任何 | false | false | 是 | 保留 |
| 0 | >0 | ELIGIBLE | **true** | false | 是 | 释放(eviction) |
| 0 | 0 | ELIGIBLE | true | **true** | 删除 | 释放(eviction) |
| 0 | >0 | INELIGIBLE_PUT | **false** | false | 是 | **保留** |
| 0 | 0 | INELIGIBLE_PUT | true | **true** | 删除 | 释放(eviction) |

---

## 27. Recovery 场景：Streaming Generator Return Objects 完整分析

### 27.1 场景假设

- Task 产生 3 个 return objects: obj1, obj2, obj3
- obj1 已被应用层消费（Python ref 释放），但 lineage_ref_count > 0
- obj2, obj3 仍被应用层持有
- generator 的 lineage_eligibility_ = ELIGIBLE（max_retries > 0）
- 执行节点 NodeA 被 kill → Owner 触发 recovery → NodeB 重新执行

### 27.2 NodeA Kill 后 Owner 端状态

```
ResetObjectsOnRemovedNode(NodeA):
  obj1: pinned_at_node_id_ 已 reset → 不在 objects_to_recover_ 中
  obj2: pinned_at == NodeA → UnsetObjectPrimaryCopy → objects_to_recover_
  obj3: 同 obj2
```

### 27.3 NodeB 重新执行，创建 obj1/obj2/obj3

```
NodeB executor: Create+Seal obj1/obj2/obj3
  → HandleObjectAdded (NodeB)
  → PinObjectIDs(owner_address, generator_id)
  → NodeB raylet pin → 订阅 owner 的 WORKER_OBJECT_EVICTION
```

### 27.4 Owner 收到订阅请求 — ProcessSubscribeForObjectEviction

```
Owner Worker:
  → ProcessSubscribeForObjectEviction:
    → TemporarilyOwnGeneratorReturnRefIfNeeded(generator_id, obj_id):
      → InsertToStream:
        obj1: item_index(0) < next_index_ → 已消费 → return false
        obj2: 未消费 → return true → 临时拥有
        obj3: 同 obj2

    → AddObjectOutOfScopeOrFreedCallback(obj_id, unpin_object):
      obj1: 分支2 → OutOfScope=true(ELIGIBLE,不看lineage), ShouldDelete=false(lineage>0) → return false
        ★ 立即 unpin_object(obj1) → Publish WORKER_OBJECT_EVICTION
        → NodeB raylet 收到 → ReleaseFreedObject → Plasma 删除 obj1 ★

      obj2: 分支4 → OutOfScope=false(RefCount>0) → return true
        → 注册回调，等 RefCount 归零时触发

      obj3: 同 obj2
```

### 27.5 关键分析：obj1 为什么立即被清除

对 ELIGIBLE 的 streaming generator return objects：

1. obj1 的 RefCount=0（应用层已释放），lineage_ref_count>0
2. `lineage_eligibility_ = ELIGIBLE` → `OutOfScope` 中 `has_lineage_references=false`
3. `OutOfScope = true` → `OnObjectOutOfScopeOrFreed` 已在之前被调用过 → callbacks 已清空
4. `AddObjectOutOfScopeOrFreedCallback` 分支2返回 false → 立即 unpin
5. NodeB 上 obj1 被 ReleaseFreedObject → 从 Plasma 中清除

**obj1 不会泄漏**：虽然 ref 记录仍保留在 owner 的 `object_id_refs_` 中（`ShouldDelete=false`），但 Plasma 副本不保留在 NodeB 上。

### 27.6 obj1 在 Owner 端的最终清理

```
任务不可再重试 → release_lineage=true → lineage_ref_count--
  → lineage_ref_count == 0 → ShouldDelete = true
  → DeleteReferenceInternal:
    → OutOfScope → OnObjectOutOfScopeOrFreed → eviction callbacks
    → ShouldDelete=true → ReleaseLineageReferences → EraseReference
      → object_id_refs_.erase(obj1) + freed_objects_.erase(obj1)
```

### 27.7 obj1 在 NodeB 上的清除时机总结

| obj1 在 owner 端的状态 | AddObjectOutOfScopeOrFreedCallback 返回 | NodeB 上 obj1 何时清除 |
|----------------------|--------------------------------------|---------------------|
| 已从 object_id_refs_ 中删除（完全 GC） | false（分支1） | 立即 eviction |
| 在 freed_objects_ 中（应用层主动 free） | false（分支3） | 立即 eviction |
| RefCount=0, lineage_ref>0, ELIGIBLE | false（分支2） | 立即 eviction |
| RefCount=0, lineage_ref>0, INELIGIBLE_PUT | true（分支4） | 延迟 → lineage 释放后 eviction |
| RefCount>0（应用层还在用） | true（分支4） | 延迟 → ref 归零后 eviction |

---

## 28. HandleObjectMissing 对 spilled 条目的 Bug 分析与修复

### 28.1 问题描述

`HandleObjectMissing` 由 `delete_object_callback_`（LRU evict）触发。原实现对 spilled 条目直接 erase + unsubscribe，导致整个 Scheme C（spill 后可恢复）失效：

```
HandleObjectMissing (原实现):
  对 spilled replicated entry:
    → spilled_object_ids_.erase(object_id)  // ★ spill URL 丢失
    → Unsubscribe(object_id)                  // ★ 取消 owner 订阅
```

### 28.2 Bug 产生的连锁后果

```
1. spill URL 丢失
   → GetSpilledReplicatedObjectURL 返回 ""
   → Recovery 无法 restore（找不到数据源）

2. IsSpilledReplicatedObject 返回 false
   → Push 不跳过 spilled replicated 对象
   → 走 unfulfilled_push_requests_ 等待 → 超时

3. NM 补调 ReportObjectAdded
   → 把节点加回 owner locations
   → 但下一秒 HandleObjectMissing 又清了 spill 条目
   → 循环：加进去又被移除
```

### 28.3 修复方案

**HandleObjectMissing 对 spilled 条目改为 no-op**（保留 spill 条目和订阅），只有 HandleObjectFreed（owner GC）或 ReleasePins 才真正清理：

```
HandleObjectMissing (修复后):
  if (IsSpilledReplicatedObject(object_id)):
    → return  // ★ 保留 spill 条目和订阅，不做任何操作
  // 非 spilled 对象正常处理
  → erased = spilled_object_ids_.erase(object_id)
  → ...
```

### 28.4 HandleObjectMissing vs HandleObjectFreed 语义对比

| | HandleObjectMissing | HandleObjectFreed（ReleaseFreedObject） |
|---|---|---|
| **触发** | LRU evict → `delete_object_callback_` | owner GC → eviction pub/sub → `subscription_callback` |
| **语义** | "内存中没了，磁盘副本仍有效" | "对象生命周期结束，磁盘副本也应该删除" |
| **对 spilled entry** | **保留**（修复后 no-op） | 删除（`spilled_object_pending_delete_` 入队） |
| **对 `spilled_replicated_bytes_current_`** | 不减 | 减（磁盘占用释放） |
| **对 subscription** | 保留不变 | 取消订阅 |
| **是否触发磁盘删除** | 否 | 是 |

---

## 29. Pull 对象不会走 PinObjectsAndWaitForFree

### 29.1 确认

Pull 来的对象（PullManager pin / PinLeaseArgs）**不会**走 `PinObjectsAndWaitForFree`。只有以下场景才触发 `PinObjectsAndWaitForFree`：

| 场景 | 入口 | 说明 |
|------|------|------|
| Put（owner 本地小对象） | `CoreWorker::PutInLocalPlasmaStore` → `PinObjectIDs` | Create + Seal + Pin |
| SealExisting（task 返回值） | `SealExisting` → `PinObjectIDs` | Create + Seal + Pin |
| PinExistingReturnObject | `PinExistingReturnObject` → `PinObjectIDs` | 已有对象 + Pin |
| Restore（spill 恢复后） | `OnObjectSpilled` 后恢复 | 恢复到 Plasma 后重新 Pin |

**Pull 路径的保护链**：
```
PullManager::TryPinObject (临时 pin)
  → PinLeaseArgsIfMemoryAvailable (lease 级 pin)
    → CleanupLease → ReleaseLeaseArgs → 无保护 → LRU 可淘汰
```

**关键区别**：PullManager 不订阅 owner 的 `WORKER_OBJECT_EVICTION`，不上报 owner 的 `pinned_at_node_id_`。Pull 来的临时对象 lease 完成后即可被 LRU 淘汰。

### 29.2 Pull 对象 LRU 后的 notification 流程

```
Plasma LRU 淘汰 Pull 来的对象:
  → DeleteObjectInternal → delete_object_callback_
    → HandleObjectDeleted: ReportObjectRemoved → Owner 从 locations 移除此节点
    → HandleObjectMissing: LeaseDependencyManager 依赖缺失 +1
```

Pull 对象被淘汰后，如果该对象还被 lease 依赖，LeaseDependencyManager 会增加 missing_dependencies，触发重新 pull。

---

## 31. Plasma Store 内存模型：Fallback Allocation 与 dlmalloc 机制

### 31.1 内存配置传递链路

```
Python ray.init() / ray start
  → estimate_available_memory() → available_memory_bytes
  → resolve_object_store_memory(available_memory_bytes, object_store_memory)
    → 默认: available_memory_bytes * 0.3, 上限 /dev/shm 大小
  → RayParams(available_memory_bytes=..., object_store_memory=...)
    → raylet main.cc --object_store_memory flag
      → plasma store runner: system_memory_ = object_store_memory
        → PlasmaAllocator(footprint_limit=system_memory_)
```

### 31.2 available_memory_bytes 的作用

`available_memory_bytes` 用于：
1. 计算 `object_store_memory`（Plasma Store 共享内存大小）
2. 计算调度资源 `memory = available_memory_bytes - object_store_memory`（剩余内存供 worker 进程使用）

`available_memory_bytes` 不直接传递给 raylet，而是通过 Python 层计算后分别设置 `--object-store-memory` 和 `--memory` 两个参数。

### 31.3 dlmalloc 初始分配与 /dev/shm

```cpp
// plasma_allocator.cc:44-55
PlasmaAllocator::PlasmaAllocator(const std::string &plasma_directory,
                                  const std::string &fallback_directory,
                                  bool hugepage_enabled,
                                  int64_t footprint_limit)
    : kFootprintLimit(footprint_limit), ... {
  internal::SetDLMallocConfig(plasma_directory, fallback_directory,
                              hugepage_enabled, /*fallback_enabled=*/true);
  auto allocation = Allocate(kFootprintLimit - kDlMallocReserved);
  // ★ 初始分配：footprint_limit - 256B 从 /dev/shm 预分配
  // 立即释放 → 但地址空间已预留给 dlmalloc
}
```

**"Ray allocates all plasma memory up-front at once"**（dlmalloc.cc:85）：Plasma Store 启动时在 `/dev/shm` 上一次性 mmap 预分配 `object_store_memory` 大小的文件。这块内存后续由 dlmalloc 管理，用于 `Allocate()` 调用。

### 31.4 /dev/shm 空间限制与 cap

```cpp
// store_runner.cc:48-63
if (fallback_directory.empty()) {
  fallback_directory = "/tmp";
}
shm_mem_avail = 9 * shm_mem_avail / 10;  // 90% /dev/shm 可用空间
if (system_memory > shm_mem_avail) {
  RAY_LOG(WARNING) << "System memory request exceeds memory available in "
                   << plasma_directory;
  system_memory = shm_mem_avail;  // ★ cap 到 /dev/shm 的 90%
}
```

如果 `object_store_memory` 超过 `/dev/shm` 可用空间的 90%，会被自动 cap。

### 31.5 Fallback Allocation — /dev/shm 满后的磁盘分配

```cpp
// dlmalloc.cc:168-178 — create_and_mmap_buffer
std::string file_template = dlmalloc_config.directory;  // 首次用 /dev/shm
if (allocated_once && dlmalloc_config.fallback_enabled) {
  file_template = dlmalloc_config.fallback_directory;  // 后续用 /tmp
}
file_template += "/plasmaXXXXXX";
```

**关键机制**：首次分配使用 `plasma_directory`（`/dev/shm`），后续分配使用 `fallback_directory`（`/tmp/ray/...`）。

```cpp
// dlmalloc.cc:219-228 — fake_mmap
void *fake_mmap(size_t size) {
  if (dlmalloc_config.fallback_enabled && allocated_once && mparams.mmap_threshold > 0) {
    // ★ 初始分配后，普通 Allocate() 的 mmap 被拒绝
    return MFAIL;
  }
  // FallbackAllocate() 设置 mmap_threshold=0 → 可以通过
  // ... 正常 mmap
}
```

```cpp
// plasma_allocator.cc:82-103 — FallbackAllocate
std::optional<Allocation> PlasmaAllocator::FallbackAllocate(size_t bytes) {
  RAY_CHECK(dlmallopt(M_MMAP_THRESHOLD, 0));  // ★ 强制 mmap
  void *mem = dlmemalign(kAlignment, bytes);
  RAY_CHECK(dlmallopt(M_MMAP_THRESHOLD, MAX_SIZE_T));  // 恢复
  if (internal::IsOutsideInitialAllocation(mem)) {
    is_fallback_allocated = true;  // ★ 标记为 fallback
    fallback_allocated_ += bytes;
  }
  return BuildAllocation(mem, bytes, is_fallback_allocated);
}
```

### 31.6 Fallback 不受 footprint_limit 限制

`footprint_limit`（`object_store_memory`）只限制 `/dev/shm` 中的初始区域。Fallback 分配可以**无限增长**——直到磁盘空间耗尽。

### 31.7 生产环境 MMAP_SHM 指标解释

```
配置: --object_store_memory=200GB
/dev/shm: 200 GB 初始分配

实际指标:
  Prometheus MMAP_SHM ~871 GiB → 包含了 plasma + fallback + spill 的总和
  /dev/shm 占用只有 6 GB (新启动的 Tidal 节点)

解释:
  /dev/shm 只是初始分配目录，fallback 后 mmap 可以到任意地址空间
  dlmalloc 用 mmap 分配内存，不一定在 /dev/shm 里
  /proc/meminfo 中的 Shmem: 176 GB 才是真实共享内存使用
  ray_object_store_memory = plasma + fallback + spill 总和
  ray_object_store_available_memory = 系统级可用内存（1 TB 机器内存）
  所以 used + avail ≈ 1600 GB
```

### 31.8 分配器层面的硬限制

| 分配方式 | 目录 | 是否受 footprint_limit 限制 | 触发条件 |
|---------|------|---------------------------|---------|
| `Allocate()` | `/dev/shm` | 是（初始区域用完后 fake_mmap 返回 MFAIL） | 正常创建对象 |
| `FallbackAllocate()` | `fallback_directory`（`/tmp`） | **否**，可无限增长 | `/dev/shm` 满后自动 fallback |
| Spill 文件 | 配置的外部存储 | 否 | plasma 内存压力 |

**当 `Allocate()` 返回 nullopt 时**，PlasmaStore 的 `CreateRequestQueue` 触发 spill/GC/grace period 逻辑。如果仍无法腾出空间，则使用 `FallbackAllocate()` 在磁盘上创建 mmap 文件。

---

## 32. OOM 与 ObjectReconstructionFailed 生产场景分析

### 32.1 典型错误链路

```
ray.exceptions.RayTaskError(OutOfMemoryError)
  → 节点内存使用 957.55GB / 1006.84GB (0.951040 > 0.950000)
  → Ray OOM killer 杀掉 worker
  → worker 持有的 ObjectRef 丢失
  → 下游依赖此对象的任务无法获取参数
    → ray.exceptions.ObjectReconstructionFailedError
    → [OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED]
      → max_retries 耗尽 → 对象不可恢复
```

### 32.2 QG V4 场景中的两种失败模式

**模式 A：OOM → 对象丢失 → Recovery 超过 max_retries**

```
StreamingRepartition 产生对象 → OOM 杀掉 worker
  → MapWorker(MapBatches(QGPreprocessMapper)) 依赖此对象
  → Owner 触发 RecoverObject → ResubmitTask
  → max_retries 耗尽 → OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED
  → 下游 StreamingGenerator _next_sync 收到 nil ref
    → assert not ref.is_nil() → AssertionError
```

**模式 B：对象重建后又被淘汰 → 循环重建**

```
ResubmitTask → 对象重新创建 → plasma 内存仍然紧张
  → 新创建的对象立即被 LRU 淘汰
  → Recovery 再次触发 → 重试计数递增
  → 循环直到 max_retries 耗尽
```

### 32.3 OOM 错误信息解读

```
Object store memory usage:
  - objects spillable: 21
  - bytes spillable: 41282258121 (~41 GB)
  - objects unsealed: 0
  - bytes unsealed: 0
  - objects in use: 32
  - bytes in use: 56822725626 (~56 GB)
  - objects evictable: 91
  - bytes evictable: 39174816225 (~39 GB)
  - objects created by worker: 21
  - bytes created by worker: 41282258121 (~41 GB)
  - objects received: 102
  - bytes received: 54715283730 (~54 GB)
Eviction Stats:
  (global lru) capacity: 200000000000  (200 GB)
  (global lru) used: 19.5874%
  (global lru) num objects: 91
  (global lru) num evictions: 0
```

**关键观察**：
- `bytes in use` (56 GB) + `bytes spillable` (41 GB) + `bytes evictable` (39 GB) ≈ 136 GB → 远小于 200 GB 配置
- 但进程级别内存 957.55 GB / 1006.84 GB → OOM killer 触发
- **问题不在于 Plasma Store 内存**，而在于**进程级别内存**（raylet 自身 71.62 GB + worker 进程 + 其他）
- raylet 进程占用 71.62 GB → 这是 fallback allocation 的 mmap 文件被 raylet 进程地址空间映射

### 32.4 解决方向

| 方向 | 具体措施 | 说明 |
|------|---------|------|
| 减少 plasma 内存占用 | 限制副本占用空间、及时 spill | object_replication_min_bytes 控制 |
| 减少 fallback | 限制 fallback 目录大小或禁止 | 当前 fallback 不受 footprint_limit 限制 |
| 增加 max_retries | `@ray.remote(max_retries=N)` | 给 Recovery 更多机会 |
| 减少并行度 | 增加 CPU 请求 | 降低同时活跃的对象数 |
| 修复 cgroup memory | 确保 cgroup memory limit 有效 | 避免 OOM killer 计算错误 |

---

## 33. ResubmitTask / MarkGeneratorFailedAndResubmit 重建流程

### 33.1 ResubmitTask — 普通任务重建入口

```cpp
// task_manager.cc:353-410
std::optional<rpc::ErrorType> TaskManager::ResubmitTask(
    const TaskID &task_id, std::vector<ObjectID> *task_deps) {
  TaskSpecification spec;
  bool should_queue_generator_resubmit = false;
  {
    absl::MutexLock lock(&mu_);
    auto it = submissible_tasks_.find(task_id);
    if (it == submissible_tasks_.end()) {
      return rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED;
    }
    auto &task_entry = it->second;
    if (task_entry.is_canceled_) {
      return rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE_TASK_CANCELLED;
    }

    if (task_entry.spec_.IsStreamingGenerator() &&
        task_entry.GetStatus() == rpc::TaskStatus::SUBMITTED_TO_WORKER) {
      // ★ streaming generator 正在运行 → 不能直接 resubmit
      // 需要等 generator 完成或失败后再 resubmit
      should_queue_generator_resubmit = true;
    } else if (task_entry.GetStatus() != rpc::TaskStatus::FINISHED &&
               task_entry.GetStatus() != rpc::TaskStatus::FAILED) {
      // 任务还在运行 → 不需要 resubmit
      return std::nullopt;
    } else {
      // 任务已完成或失败 → 可以直接 resubmit
      SetupTaskEntryForResubmit(task_entry);
    }
    spec = task_entry.spec_;
  }

  if (should_queue_generator_resubmit) {
    // 排队等待 generator 完成后自动 resubmit
    return queue_generator_resubmit_(spec)
               ? std::nullopt
               : std::make_optional(
                     rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE_TASK_CANCELLED);
  }

  UpdateReferencesForResubmit(spec, task_deps);
  async_retry_task_callback_(spec, /*delay_ms=*/0);
  return std::nullopt;
}
```

### 33.2 MarkGeneratorFailedAndResubmit — Streaming Generator 中断重建

```cpp
// task_manager.cc:473-496
void TaskManager::MarkGeneratorFailedAndResubmit(const TaskID &task_id) {
  TaskSpecification spec;
  {
    absl::MutexLock lock(&mu_);
    auto it = submissible_tasks_.find(task_id);
    auto &task_entry = it->second;

    // ★ 先设为 FAILED（原因：GENERATOR_TASK_FAILED_FOR_OBJECT_RECONSTRUCTION）
    rpc::RayErrorInfo error_info;
    error_info.set_error_type(
        rpc::ErrorType::GENERATOR_TASK_FAILED_FOR_OBJECT_RECONSTRUCTION);
    SetTaskStatus(task_entry, rpc::TaskStatus::FAILED, error_info);

    SetupTaskEntryForResubmit(task_entry);
    spec = task_entry.spec_;
  }
  // ★ 不需要 UpdateReferencesForResubmit
  // 因为 CompletePendingTask / FailPendingTask 没有被调用
  // RemoveFinishedTaskReferences 从未发生
  async_retry_task_callback_(spec, /*delay_ms=*/0);
}
```

### 33.3 两者的关系

| | ResubmitTask | MarkGeneratorFailedAndResubmit |
|---|---|---|
| **调用者** | `ObjectRecoveryManager::ReconstructObject` | `queue_generator_resubmit_` 回调（generator 完成后） |
| **适用场景** | 普通 task 完成/失败后，或 streaming generator 还在运行时排队 | streaming generator 需要中断执行进行 recovery |
| **状态变更** | 已完成/已失败的任务 → `SetupTaskEntryForResubmit` | 正在运行的 generator → 设 FAILED → `SetupTaskEntryForResubmit` |
| **依赖更新** | 调用 `UpdateReferencesForResubmit`（TryMarkFreedObjectInUseAgain） | **不调用**（RemoveFinishedTaskReferences 未发生） |
| **互斥性** | 不是严格互斥，而是上下游关系 | 由 `ResubmitTask` 通过 `queue_generator_resubmit_` 触发 |

**调用流程**：

```
ObjectRecoveryManager::ReconstructObject
  → ResubmitTask(task_id)
    ├─ 普通 task → SetupTaskEntryForResubmit + UpdateReferencesForResubmit + async_retry
    └─ streaming generator 正在运行 → queue_generator_resubmit_(spec)
         → generator 完成后回调
           → MarkGeneratorFailedAndResubmit(task_id)
             → SetStatus(FAILED, GENERATOR_TASK_FAILED_FOR_OBJECT_RECONSTRUCTION)
             → SetupTaskEntryForResubmit + async_retry
```

### 33.4 SetupTaskEntryForResubmit — 共享的重建准备逻辑

```cpp
// task_manager.cc:413-436
void TaskManager::SetupTaskEntryForResubmit(TaskEntry &task_entry) {
  task_entry.MarkRetry();  // ★ AttemptNumber +1
  SetTaskStatus(task_entry, rpc::TaskStatus::PENDING_ARGS_AVAIL, ...);
  num_pending_tasks_++;
  total_lineage_footprint_bytes_ -= task_entry.lineage_footprint_bytes_;
  task_entry.lineage_footprint_bytes_ = 0;
  if (task_entry.num_retries_left_ > 0) {
    task_entry.num_retries_left_--;
  }
}
```

### 33.5 UpdateReferencesForResubmit — 依赖对象重新可用

```cpp
// task_manager.cc:438-472
void TaskManager::UpdateReferencesForResubmit(const TaskSpecification &spec,
                                               std::vector<ObjectID> *task_deps) {
  // 收集任务依赖
  for (size_t i = 0; i < spec.NumArgs(); i++) {
    if (spec.ArgByRef(i)) {
      task_deps->emplace_back(spec.ArgObjectId(i));
    }
  }
  reference_counter_.UpdateResubmittedTaskReferences(*task_deps);

  // ★ 重新标记 freed 对象为可用
  for (const auto &task_dep : *task_deps) {
    bool was_freed = reference_counter_.TryMarkFreedObjectInUseAgain(task_dep);
    if (was_freed) {
      // 之前被 free 的依赖对象 → recovery 需要重新使用
      in_memory_store_.Delete({task_dep});  // 删除 freed 标记
    }
  }
}
```

### 33.6 HandleReportGeneratorItemReturns — Streaming Generator Return 过滤

```cpp
// task_manager.cc:779-879
bool TaskManager::HandleReportGeneratorItemReturns(
    const rpc::ReportGeneratorItemReturnsRequest &request, ...) {
  const auto &generator_id = ObjectID::FromBinary(request.generator_id());
  int64_t item_index = request.item_index();
  int64_t attempt_number = request.attempt_number();

  {
    absl::MutexLock lock(&mu_);
    auto it = submissible_tasks_.find(task_id);
    if (it->second.spec_.AttemptNumber() > attempt_number) {
      // ★ 过滤：当前 attempt_number > 报告的 attempt_number
      // 来自上一次执行（已被 resubmit）的 stale 报告 → 忽略
      execution_signal_callback(
          Status::NotFound("Stale object reports from the previous attempt."), -1);
      return false;
    }
  }

  const auto store_in_plasma_ids = GetTaskReturnObjectsToStoreInPlasma(task_id);

  if (request.has_returned_object()) {
    const auto object_id = ObjectID::FromBinary(returned_object.object_id());
    auto index_not_used_yet = stream_it->second.InsertToStream(object_id, item_index);
    // ★ InsertToStream 返回 false → 对象已被消费 → 不重新注册
    if (index_not_used_yet) {
      reference_counter_.OwnDynamicStreamingTaskReturnRef(object_id, generator_id);
    }
    reference_counter_.UpdateObjectPendingCreation(object_id, false);
    HandleTaskReturn(object_id, returned_object, worker_node_id,
                     /*store_in_plasma=*/store_in_plasma_ids.contains(object_id));
  }

  if (stream_it->second.IsObjectConsumed(item_index)) {
    // ★ 已消费的对象 → 直接返回 false
    execution_signal_callback(Status::OK(), total_consumed);
    return false;
  }
  // ... 背压逻辑
}
```

### 33.7 GetTaskReturnObjectsToStoreInPlasma — 首次执行 vs Recovery 过滤

```cpp
// task_manager.cc:1533-1557
absl::flat_hash_set<ObjectID> TaskManager::GetTaskReturnObjectsToStoreInPlasma(
    const TaskID &task_id, bool *first_execution_out) const {
  absl::flat_hash_set<ObjectID> store_in_plasma_ids = {};
  auto it = submissible_tasks_.find(task_id);
  bool first_execution = it->second.num_successful_executions_ == 0;
  if (!first_execution) {
    // ★ 非首次执行（Recovery）→ 只存储之前在 plasma 中的对象
    store_in_plasma_ids = it->second.reconstructable_return_ids_;
  }
  // 首次执行 → 空集合 → 按正常逻辑决定（大对象进 plasma，小对象直接返回）
  return store_in_plasma_ids;
}
```

**Recovery 时的过滤**：只有 `reconstructable_return_ids_` 中的对象才会被存入 plasma，其他对象直接返回 in-memory。这避免了重建时产生不必要的 plasma 对象。

### 33.8 Streaming Generator Recovery 完整流程

```
① OOM / 节点死亡 → 对象丢失
  → Owner: RecoverObject → ResubmitTask(task_id)

② ResubmitTask:
  → generator 还在运行 → queue_generator_resubmit_(spec)
  → generator 完成/失败 → MarkGeneratorFailedAndResubmit:
    → SetStatus(FAILED, GENERATOR_TASK_FAILED_FOR_OBJECT_RECONSTRUCTION)
    → SetupTaskEntryForResubmit → AttemptNumber +1
    → async_retry_task_callback_ → 重新提交任务

③ 重执行产生新 return objects
  → HandleReportGeneratorItemReturns:
    → 过滤 stale 报告（attempt_number < 当前 AttemptNumber）
    → InsertToStream → 已消费的对象不重新注册
    → GetTaskReturnObjectsToStoreInPlasma → 只存 reconstructable_return_ids_
    → HandleTaskReturn → in_plasma=true: UpdateObjectPinnedAtRaylet
                     → in_plasma=false: in_memory_store_.Put

④ 新产生的"无用"对象（已消费的不需要恢复）
  → InsertToStream 返回 false → 不调用 OwnDynamicStreamingTaskReturnRef
  → AddObjectOutOfScopeOrFreedCallback 分支2 → return false → 立即 eviction
  → 从 Plasma 中清除
```

---

## 34. Push/Pull 数据到达后如何通知 HandleObjectAdded

### 34.1 Push 路径

```
远端 HandlePush → ReceivePullChunk / ReceiveReplicationPushChunk
  → buffer_pool_.CreateChunk → EnsureBufferExists
    → store_client_->CreateAndSpillIfNeeded
      → [IPC: CreateRequest] → PlasmaStore HandleCreateObjectRequest
        → CreateObject → SealObjects (最后一个 chunk)
          → add_object_callback_(object_info, source)
            → [post 到 raylet 主线程]
              → ObjectManager::HandleObjectAdded
                → ReportObjectAdded → PullManager::PinNewObjectIfNeeded
              → NodeManager::HandleObjectLocal
```

### 34.2 Pull 路径

```
PullManager → send_pull_request_ → gRPC PullRequest
  → 远端 ObjectManager::HandlePull → Push
    → 本地 ReceivePullChunk
      → CreateChunk → CreateAndSpillIfNeeded → [IPC: CreateRequest]
      → WriteChunk (逐 chunk)
      → 最后 chunk: Seal → [IPC: SealRequest]
        → PlasmaStore::SealObjects → add_object_callback_
          → HandleObjectAdded + HandleObjectLocal
```

### 34.3 CreateOwnedAndIncrementLocalRef 路径

```
CoreWorker::PutInLocalPlasmaStore / AllocateReturnObject → Create
  → PlasmaClient::CreateAndSpillIfNeeded → [IPC: CreateRequest]
    → PlasmaStore::HandleCreateObjectRequest → CreateObject → AddToClientObjectIds
  → Python: write_to → Seal
    → PlasmaClient::Seal → [IPC: SealRequest]
      → PlasmaStore::SealObjects → add_object_callback_
        → HandleObjectAdded + HandleObjectLocal
```

### 34.4 add_object_callback_ — 统一通知入口

```cpp
// main.cc:806-812
[&](const ray::ObjectInfo &object_info, plasma::flatbuf::ObjectSource source) {
  main_service.post([&]() {
    object_manager->HandleObjectAdded(object_info);
    node_manager->HandleObjectLocal(object_info, source);
  }, "ObjectManager.ObjectAdded");
}
```

**所有路径最终都经过 `SealObjects` → `add_object_callback_`**：
- Push 接收：最后一个 chunk 写完 → Seal
- Pull 接收：最后一个 chunk 写完 → Seal
- Worker 创建：Create + Write + Seal

Seal 之前对象处于 `PLASMA_CREATED` 状态，不可 Get、不可 spill、不可淘汰。Seal 后变为 `PLASMA_SEALED`，触发所有后续逻辑（pin、上报、通知依赖等待者）。

---

## 35. available_memory_bytes 与 RayParams 内存配置详解

### 35.1 计算链路

```python
# utils.py:527-586
def resolve_object_store_memory(available_memory_bytes, object_store_memory=None):
    if object_store_memory is None:
        object_store_memory = available_memory_bytes * DEFAULT_OBJECT_STORE_MEMORY_PROPORTION
        # ★ 默认 30% 的可用内存给 Plasma Store
    # Linux 上：上限为 /dev/shm 大小
    return object_store_memory

# resource_and_label_spec.py:377-386
available_memory_bytes = estimate_available_memory()
object_store_memory = resolve_object_store_memory(available_memory_bytes)
memory = available_memory_bytes - object_store_memory
# ★ memory 作为调度资源，供 worker 进程使用
```

### 35.2 available_memory_bytes 的来源

```python
# utils.py — estimate_available_memory()
# 优先使用 cgroup memory.limit_in_bytes（容器环境）
# 如果 cgroup 值异常（如 9.2EB）→ fallback 到 /proc/meminfo MemTotal
# 这就是 cgroup memory limit invalid 导致 OOM killer 计算错误的根因
```

### 35.3 三个内存参数的关系

| 参数 | 用途 | 传递路径 |
|------|------|---------|
| `available_memory_bytes` | 计算 object_store_memory 和 memory 的源值 | Python 层计算 |
| `object_store_memory` | Plasma Store 共享内存大小 | `--object-store-memory` → raylet → plasma store |
| `memory` | 调度资源（worker 进程可用内存） | `--memory` → raylet 资源调度 |

**关系**：`memory = available_memory_bytes - object_store_memory`

### 35.4 cgroup memory limit 对 OOM killer 的影响

```
当 cgroup memory limit 未有效设置时：
  → /sys/fs/cgroup/memory/memory.limit_in_bytes 返回 9223372036854771712 (~9.2 EB)
  → estimate_available_memory() 用此值作为 available_memory_bytes
  → object_store_memory = 9.2EB * 0.3 → 远超实际内存
  → memory = 9.2EB - 200GB → 仍为 ~9.2EB
  → raylet 认为有无限内存 → 过度调度 worker → OOM

修复：
  → 检测 cgroup memory limit 的有效性
  → 无效时 fallback 到 /proc/meminfo MemTotal
  → 或使用 cgroup v2 memory.max
```

---

## 30. 完整代码索引（补充二）

### Owner Ref 完整代码

| 文件 | 行号 | 函数 | 说明 |
|------|------|------|------|
| `reference_counter_interface.h` | 36 | `LineageReconstructionEligibility` | enum 定义 |
| `reference_counter.h` | 197 | `OutOfScope()` | 判断对象是否 out of scope |
| `reference_counter.h` | 217 | `ShouldDelete()` | 判断是否可删除 ref |
| `reference_counter.h` | 247 | `lineage_eligibility_` | 继承自 generator |
| `reference_counter.h` | 253 | `lineage_ref_count` | lineage 引用计数 |
| `reference_counter.h` | 263 | `on_object_out_of_scope_or_freed_callbacks` | eviction 回调列表 |
| `reference_counter.h` | 727 | `object_id_refs_` | 核心引用表 |
| `reference_counter.h` | 734 | `freed_objects_` | 主动 freed 集合 |
| `reference_counter.cc` | 581 | `AddObjectOutOfScopeOrFreedCallback` | 四分支逻辑 |
| `reference_counter.cc` | 466 | `DeleteReferenceInternal` | OutOfScope → eviction + 可能 Erase |
| `reference_counter.cc` | 563 | `OnObjectOutOfScopeOrFreed` | 触发 + 清空 callbacks |
| `reference_counter.cc` | 358 | `FreePlasmaObjects` | 主动 freed + eviction |
| `reference_counter.cc` | 349 | `TryMarkFreedObjectInUseAgain` | recovery 重新使用 |
| `reference_counter.cc` | 505 | `EraseReference` | 从 ref table 完全删除 |
| `reference_counter.cc` | 308 | `RemoveSubmittedTaskReferences` | lineage_ref_count 递减 |
| `reference_counter.cc` | 240 | `OwnDynamicStreamingTaskReturnRef` | 继承 generator eligibility |
| `task_manager.cc` | 268 | `AddPendingTask` | max_retries → eligibility 设置 |

### delete_object_callback_ 完整代码

| 文件 | 行号 | 函数 | 说明 |
|------|------|------|------|
| `common.h` | 246 | `DeleteObjectCallback` | type 定义 |
| `store.h` | 251 | `delete_object_callback_` | PlasmaStore 成员 |
| `store.cc` | 85 | 构造函数传给 ObjectLifecycleManager | |
| `obj_lifecycle_mgr.cc` | 245 | `DeleteObjectInternal` | 调用 delete_object_callback_ |
| `main.cc` | 808 | delete_object_callback_ 实现 | HandleObjectDeleted + HandleObjectMissing |
| `object_manager.cc` | 200 | `HandleObjectDeleted` | 清理 + 上报 + 重置 pull |
| `node_manager.cc` | 2454 | `HandleObjectMissing` | 通知 lease 依赖缺失 |
| `lease_dependency_manager.cc` | 276 | `HandleObjectMissing` | IncrementMissingDependencies |
