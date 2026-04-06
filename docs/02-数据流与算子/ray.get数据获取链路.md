# Ray ray.get 从 Plasma Store 获取数据的完整链路解析

## 概述

本文档详细解析 `ray.get()` 从 Plasma Store 获取数据的完整链路：从 CoreWorker 分流 Memory Store 和 Plasma Store，到触发远程拉取、对象到达本地、Seal 唤醒、Worker 收到数据的全过程。重点剖析 `OnGetRequestCompleted` 与 `while` 循环之间的关系——是通知机制而非轮询，以及 while 循环存在的唯一原因。

---

## 1. 总览流程图

```
ray.get([obj_ref])
     │
     ▼
CoreWorker::GetObjects()                         ← 入口（core_worker.cc:1548）
     │
     ├─ 1. MemoryStore::Get()                    ← 先查内存
     │     发现 IsInPlasmaError → 移入 plasma_object_ids
     │
     └─ 2. PlasmaStoreProvider::Get()            ← 再查 Plasma（core_worker.cc:1611）
           │
           ├─ 2a. raylet_ipc_client_->AsyncGetObjects()  ← 通知 raylet 拉取远程对象
           │
           └─ 2b. store_client_->Get() → GetBuffers()    ← 从本地 Plasma Store 读取
                 │     SendGetRequest → Plasma Store 服务端
                 │     Plasma Store：对象不可用 → 放入 GetRequestQueue 等待
                 │
                 │     ... 远程对象到达 → Seal → MarkObjectSealed → ReturnFromGet ...
                 │
                 └─ Plasma Store 回复 → 客户端拿到共享内存指针
```

---

## 2. 第 1 步：CoreWorker::GetObjects() 分流

位于 `src/ray/core_worker/core_worker.cc:1548-1613`：

```cpp
Status CoreWorker::GetObjects(const std::vector<ObjectID> &ids,
                              const int64_t timeout_ms,
                              std::vector<std::shared_ptr<RayObject>> &results) {
  absl::flat_hash_set<ObjectID> plasma_object_ids;
  absl::flat_hash_set<ObjectID> memory_object_ids(ids.begin(), ids.end());
  absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> result_map;
  auto start_time = current_time_ms();

  // ─── ① 先查 Memory Store ───
  if (!memory_object_ids.empty()) {
    RAY_RETURN_NOT_OK(memory_store_->Get(
        memory_object_ids, timeout_ms, *worker_context_, &result_map, &got_exception));
  }

  // ─── ② 遍历结果，把 IsInPlasmaError 的条目找出来 ───
  for (auto it = result_map.begin(); it != result_map.end();) {
    auto current = it++;
    if (current->second->IsInPlasmaError()) {
      RAY_LOG(DEBUG) << current->first << " in plasma, doing fetch-and-get";
      plasma_object_ids.insert(current->first);
      result_map.erase(current);   // 从结果中删除占位符
    }
  }

  // ─── ③ 对 Plasma 对象走 PlasmaStoreProvider ───
  if (!got_exception && !plasma_object_ids.empty()) {
    std::vector<ObjectID> object_ids(
        plasma_object_ids.begin(), plasma_object_ids.end());
    auto owner_addresses = reference_counter_->GetOwnerAddresses(object_ids);

    int64_t local_timeout_ms = timeout_ms;
    if (timeout_ms >= 0) {
      local_timeout_ms = std::max(static_cast<int64_t>(0),
                                  timeout_ms - (current_time_ms() - start_time));
    }
    RAY_RETURN_NOT_OK(plasma_store_provider_->Get(
        object_ids, owner_addresses, local_timeout_ms, &result_map));
  }

  // ④ 填充结果
  for (size_t i = 0; i < ids.size(); i++) {
    const auto pair = result_map.find(ids[i]);
    if (pair != result_map.end()) {
      results[i] = pair->second;
      RAY_CHECK(!pair->second->IsInPlasmaError());
    }
  }
  return Status::OK();
}
```

**分流逻辑：** Memory Store 返回的 `IsInPlasmaError` 占位符被删除，这些 ObjectID 被交给 `PlasmaStoreProvider::Get()` 处理。同时重新计算剩余超时时间（减去在 Memory Store 等待消耗的时间）。

---

## 3. 第 2 步：PlasmaStoreProvider::Get() 的三阶段逻辑

位于 `src/ray/core_worker/store_provider/plasma_store_provider.cc:253-355`：

```cpp
Status CoreWorkerPlasmaStoreProvider::Get(
    const std::vector<ObjectID> &object_ids,
    const std::vector<rpc::Address> &owner_addresses,
    int64_t timeout_ms,
    absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> *results) {
  std::vector<ipc::ScopedResponse> get_request_cleanup_handlers;
  absl::flat_hash_map<ObjectID, int64_t> remaining_object_id_to_idx;
  bool got_exception = false;
  int64_t num_total_objects = static_cast<int64_t>(object_ids.size());

  // ─── 阶段 A：请求 raylet 拉取远程对象 + 立即尝试本地读取 ───
  for (int64_t start = 0; start < num_total_objects; start += fetch_batch_size_) {
    std::vector<ObjectID> batch_ids;
    std::vector<rpc::Address> batch_owner_addresses;
    for (int64_t i = start; i < start + fetch_batch_size_ && i < num_total_objects; i++) {
      remaining_object_id_to_idx[object_ids[i]] = i;
      batch_ids.push_back(object_ids[i]);
      batch_owner_addresses.push_back(owner_addresses[i]);
    }

    // A1: 异步通知 raylet："我需要这些对象，请拉到本地"
    StatusOr<ipc::ScopedResponse> status_or_cleanup =
        raylet_ipc_client_->AsyncGetObjects(
            batch_ids, batch_owner_addresses, get_request_counter_.fetch_add(1));
    RAY_RETURN_NOT_OK(status_or_cleanup.status());
    get_request_cleanup_handlers.emplace_back(std::move(status_or_cleanup.value()));

    // A2: 立即尝试从本地 Plasma 读取（timeout=0，不等待）
    RAY_RETURN_NOT_OK(GetObjectsFromPlasmaStore(
        remaining_object_id_to_idx, batch_ids, /*timeout_ms=*/0, results, &got_exception));
  }

  // 如果全部获取或遇到异常，直接返回
  if (remaining_object_id_to_idx.empty() || got_exception) {
    return Status::OK();
  }

  // ─── 阶段 B：短超时轮次 + 信号检查 ───
  bool should_break = false;
  bool timed_out = false;
  int64_t remaining_timeout = timeout_ms;
  auto fetch_start_time_ms = current_time_ms();
  while (!remaining_object_id_to_idx.empty() && !should_break) {
    // B1: 准备本批待获取的 object_ids
    std::vector<ObjectID> batch_ids;
    std::vector<rpc::Address> batch_owner_addresses;
    for (const auto &[id, idx] : remaining_object_id_to_idx) {
      if (static_cast<int64_t>(batch_ids.size()) == fetch_batch_size_) {
        break;
      }
      batch_ids.push_back(id);
      batch_owner_addresses.push_back(owner_addresses[idx]);
    }

    // B2: 计算本批超时（短超时，不是用完整 remaining_timeout）
    int64_t batch_timeout =
        std::max(RayConfig::instance().get_check_signal_interval_milliseconds(),
                 static_cast<int64_t>(10 * batch_ids.size()));
    if (remaining_timeout >= 0) {
      batch_timeout = std::min(remaining_timeout, batch_timeout);
      remaining_timeout -= batch_timeout;
      timed_out = remaining_timeout <= 0;
    }

    // B3: 阻塞等待对象到达（batch_timeout 毫秒）
    size_t previous_size = remaining_object_id_to_idx.size();
    RAY_RETURN_NOT_OK(GetObjectsFromPlasmaStore(
        remaining_object_id_to_idx, batch_ids, batch_timeout, results, &got_exception));
    should_break = timed_out || got_exception;

    // B4: 检查挂起告警
    if ((previous_size - remaining_object_id_to_idx.size()) < batch_ids.size()) {
      WarnIfFetchHanging(fetch_start_time_ms, remaining_object_id_to_idx);
    }

    // B5: ★ 检查信号（Ctrl-C / 任务取消）
    if (check_signals_) {
      Status status = check_signals_();
      if (!status.ok()) {
        return status;
      }
    }
  }

  if (!remaining_object_id_to_idx.empty() && timed_out) {
    return Status::TimedOut(...);
  }
  return Status::OK();
}
```

### GetObjectsFromPlasmaStore：从 Plasma Store 进程读取

位于 `plasma_store_provider.cc:180-217`：

```cpp
Status CoreWorkerPlasmaStoreProvider::GetObjectsFromPlasmaStore(
    absl::flat_hash_map<ObjectID, int64_t> &remaining_object_id_to_idx,
    const std::vector<ObjectID> &ids,
    int64_t timeout_ms,
    absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> *results,
    bool *got_exception) {
  std::vector<plasma::ObjectBuffer> plasma_results;
  // ★ 核心：调用 Plasma 客户端，带超时阻塞读取
  RAY_RETURN_NOT_OK(store_client_->Get(ids, timeout_ms, &plasma_results));

  for (size_t i = 0; i < plasma_results.size(); i++) {
    if (plasma_results[i].data != nullptr || plasma_results[i].metadata != nullptr) {
      const auto &object_id = ids[i];
      // 构造 TrackedBuffer（共享内存引用）
      std::shared_ptr<TrackedBuffer> data = nullptr;
      if (plasma_results[i].data && plasma_results[i].data->Size() > 0) {
        data = std::make_shared<TrackedBuffer>(
            std::move(plasma_results[i].data), buffer_tracker_, object_id);
        buffer_tracker_->Record(object_id, data.get(), get_current_call_site_());
      }
      std::shared_ptr<Buffer> metadata = nullptr;
      if (plasma_results[i].metadata && plasma_results[i].metadata->Size() > 0) {
        metadata = std::move(plasma_results[i].metadata);
      }
      auto result_object = std::make_shared<RayObject>(
          data, metadata, std::vector<rpc::ObjectReference>());
      remaining_object_id_to_idx.erase(object_id);
      if (result_object->IsException()) {
        RAY_CHECK(!result_object->IsInPlasmaError());
        *got_exception = true;
      }
      (*results)[object_id] = std::move(result_object);
    }
  }
  return Status::OK();
}
```

**关键：** `store_client_->Get(ids, timeout_ms, &plasma_results)` 是带超时的阻塞调用。如果 `timeout_ms > 0`，它会在 Unix domain socket 上阻塞等待，直到 Plasma Store 回复或超时。

---

## 4. 第 3 步：PlasmaClient::GetBuffers() —— 阻塞在 socket 上

位于 `src/ray/object_manager/plasma/client.cc:291-370`：

```cpp
Status PlasmaClient::GetBuffers(const ObjectID *object_ids,
                                int64_t num_objects,
                                int64_t timeout_ms,
                                ObjectBuffer *object_buffers) {
  bool all_present = true;
  // 先检查 objects_in_use_ 中已有的对象...
  for (int64_t i = 0; i < num_objects; ++i) {
    auto object_entry = objects_in_use_.find(object_ids[i]);
    if (object_entry == objects_in_use_.end()) {
      all_present = false;  // 不在本地，需要向 Plasma Store 请求
    } else if (!object_entry->second->is_sealed) {
      all_present = false;  // 在本地但未 Seal
    } else {
      // 在本地且已 Seal → 直接构造 buffer
      // ...
      object_buffers[i].data = SharedMemoryBuffer::Slice(...);
      IncrementObjectCount(object_ids[i]);
    }
  }

  if (all_present) {
    return Status::OK();  // 全部在本地，无需等待
  }

  // ★ 向 Plasma Store 进程发送 Get 请求
  RAY_RETURN_NOT_OK(SendGetRequest(store_conn_, &object_ids[0], num_objects, timeout_ms));

  // ★ 阻塞等待 Plasma Store 的回复——这是 Unix domain socket 读取
  std::vector<uint8_t> buffer;
  RAY_RETURN_NOT_OK(PlasmaReceive(store_conn_, MessageType::PlasmaGetReply, &buffer));
  //                                                                              ↑
  //                                                    阻塞！直到 Plasma Store 写回回复
  //                                                    或者超时

  // 解析回复，获得共享内存 fd
  std::vector<ObjectID> received_object_ids(num_objects);
  std::vector<PlasmaObject> object_data(num_objects);
  std::vector<MEMFD_TYPE> store_fds;
  std::vector<int64_t> mmap_sizes;
  ReadGetReply(buffer.data(), buffer.size(),
               received_object_ids.data(), object_data.data(),
               num_objects, store_fds, mmap_sizes);

  // mmap 共享内存
  for (size_t i = 0; i < store_fds.size(); i++) {
    GetStoreFdAndMmap(store_fds[i], mmap_sizes[i]);
  }
  // ...构造 ObjectBuffer 返回
}
```

**`PlasmaReceive` 是阻塞的 Unix domain socket 读取。** 它会一直等，直到：
- Plasma Store 服务端写了 `PlasmaGetReply` → 读取成功返回
- 超时 → 返回超时错误

---

## 5. 第 4 步：触发 raylet 拉取远程对象

### 5.1 AsyncGetObjects —— 异步通知 raylet

位于 `src/ray/raylet_ipc_client/raylet_ipc_client.cc:196-216`：

```cpp
StatusOr<ScopedResponse> RayletIpcClient::AsyncGetObjects(
    const std::vector<ObjectID> &object_ids,
    const std::vector<rpc::Address> &owner_addresses,
    int64_t get_request_id) {
  flatbuffers::FlatBufferBuilder fbb;
  auto object_ids_message = flatbuf::to_flatbuf(fbb, object_ids);
  auto message =
      protocol::CreateAsyncGetObjectsRequest(fbb,
                                             object_ids_message,
                                             AddressesToFlatbuffer(fbb, owner_addresses),
                                             get_request_id);
  fbb.Finish(message);
  // 通过 Unix domain socket 发送给 raylet，不等待回复
  RAY_RETURN_NOT_OK(WriteMessage(MessageType::AsyncGetObjectsRequest, &fbb));
  // 返回一个 ScopedResponse，析构时发送 CancelGetRequest
  return ScopedResponse([this, request_id_to_cleanup = get_request_id]() {
    return CancelGetRequest(request_id_to_cleanup);
  });
}
```

### 5.2 Raylet 端处理：LeaseDependencyManager → ObjectManager::Pull

位于 `src/ray/raylet/node_manager.cc:1611-1617`：

```cpp
void NodeManager::HandleAsyncGetObjectsRequest(
    const std::shared_ptr<ClientConnection> &client, const uint8_t *message_data) {
  auto request = flatbuffers::GetRoot<protocol::AsyncGetObjectsRequest>(message_data);
  std::vector<rpc::ObjectReference> refs =
      FlatbufferToObjectReferences(*request->object_ids(), *request->owner_addresses());
  AsyncGet(client, refs, request->get_request_id());
}
```

```cpp
void NodeManager::AsyncGet(const std::shared_ptr<ClientConnection> &client,
                           std::vector<rpc::ObjectReference> &object_refs,
                           int64_t get_request_id) {
  std::shared_ptr<WorkerInterface> worker = worker_pool_.GetRegisteredWorker(client);
  RAY_CHECK(worker);
  lease_dependency_manager_.StartGetRequest(
      worker->WorkerId(), std::move(object_refs), get_request_id);
}
```

### 5.3 LeaseDependencyManager::StartGetRequest

位于 `src/ray/raylet/lease_dependency_manager.cc:118-141`：

```cpp
void LeaseDependencyManager::StartGetRequest(
    const WorkerID &worker_id,
    std::vector<rpc::ObjectReference> &&required_objects,
    int64_t get_request_id) {
  std::vector<ObjectID> object_ids;
  object_ids.reserve(required_objects.size());

  for (const auto &ref : required_objects) {
    const auto obj_id = ObjectRefToId(ref);
    object_ids.emplace_back(obj_id);
    auto it = GetOrInsertRequiredObject(obj_id, ref);
    ++it->second.dependent_get_requests[worker_id];
  }

  // ★ 触发 ObjectManager 远程拉取
  uint64_t new_pull_request_id = object_manager_.Pull(
      std::move(required_objects), BundlePriority::GET_REQUEST, {"", false});

  get_requests_.emplace(std::move(worker_and_request_ids),
                        std::make_pair(std::move(object_ids), new_pull_request_id));
}
```

### 5.4 ObjectManager::Pull —— 查找位置 + 发送 Pull RPC

位于 `src/ray/object_manager/object_manager.cc:214-245`：

```cpp
uint64_t ObjectManager::Pull(const std::vector<rpc::ObjectReference> &object_refs,
                             BundlePriority prio,
                             const TaskMetricsKey &task_key) {
  std::vector<rpc::ObjectReference> objects_to_locate;
  auto request_id = pull_manager_->Pull(object_refs, prio, task_key, &objects_to_locate);

  const auto &callback = [this](const ObjectID &object_id,
                                const std::unordered_set<NodeID> &client_ids,
                                const std::string &spilled_url,
                                const NodeID &spilled_node_id,
                                bool pending_creation,
                                size_t object_size) {
    pull_manager_->OnLocationChange(object_id, client_ids, spilled_url,
                                    spilled_node_id, pending_creation, object_size);
  };

  for (const auto &ref : objects_to_locate) {
    // 订阅对象位置通知。每当对象的节点位置变化时收到通知
    auto object_id = ObjectRefToId(ref);
    object_directory_->SubscribeObjectLocations(
        object_directory_pull_callback_id_, object_id, ref.owner_address(), callback);
  }
  return request_id;
}
```

### 5.5 SendPullRequest —— 向远程节点发 Pull RPC

```cpp
// object_manager.cc:255-281
void ObjectManager::SendPullRequest(const ObjectID &object_id, const NodeID &client_id) {
  auto rpc_client = GetRpcClient(client_id);
  if (rpc_client) {
    rpc_service_.post(
        [this, object_id, client_id, rpc_client]() {
          rpc::PullRequest pull_request;
          pull_request.set_object_id(object_id.Binary());
          pull_request.set_node_id(self_node_id_.Binary());
          rpc_client->Pull(pull_request, ...);
        },
        "ObjectManager.SendPull");
  }
}
```

---

## 6. 第 5 步：远程节点响应 Pull → Push 数据

### 6.1 远程节点收到 Pull 请求

位于 `src/ray/object_manager/object_manager.cc:624-635`：

```cpp
void ObjectManager::HandlePull(rpc::PullRequest request,
                               rpc::PullReply *reply,
                               rpc::SendReplyCallback send_reply_callback) {
  ObjectID object_id = ObjectID::FromBinary(request.object_id());
  NodeID node_id = NodeID::FromBinary(request.node_id());

  // 异步推送对象到请求方节点
  main_service_->post([this, object_id, node_id]() { Push(object_id, node_id); },
                      "ObjectManager.HandlePull");
  send_reply_callback(Status::OK(), nullptr, nullptr);
}
```

### 6.2 Push 分块传输

```cpp
// object_manager.cc:329-371
void ObjectManager::Push(const ObjectID &object_id, const NodeID &node_id) {
  if (local_objects_.count(object_id) != 0) {
    return PushLocalObject(object_id, node_id);  // 对象在内存中
  }
  auto object_url = get_spilled_object_url_(object_id);
  if (!object_url.empty() && ...) {
    return PushFromFilesystem(object_id, node_id, object_url);  // 对象在磁盘上
  }
  // 对象还没到本地 → 等待，加入 unfulfilled_push_requests_
}
```

```cpp
// object_manager.cc:453-490
void ObjectManager::PushObjectInternal(
    const ObjectID &object_id, const NodeID &node_id,
    std::shared_ptr<ChunkObjectReader> chunk_reader, bool from_disk) {
  auto rpc_client = GetRpcClient(node_id);
  auto push_id = UniqueID::FromRandom();
  push_manager_->StartPush(
      node_id, object_id, chunk_reader->GetNumChunks(), [=](int64_t chunk_id) {
        // 分块发送
        SendObjectChunk(push_id, object_id, node_id, chunk_id, rpc_client, ...);
      });
}
```

---

## 7. 第 6 步：本地节点接收 chunk → Seal → 唤醒等待的 Get 请求

### 7.1 接收 chunk

位于 `src/ray/object_manager/object_manager.cc:545-614`：

```cpp
// HandlePushObjectRequest →
bool ObjectManager::ReceiveObjectChunk(const NodeID &node_id,
                                       const ObjectID &object_id,
                                       const rpc::Address &owner_address,
                                       uint64_t data_size, uint64_t metadata_size,
                                       uint64_t chunk_index,
                                       const std::string &data) {
  if (!pull_manager_->IsObjectActive(object_id)) {
    return false;  // 对象不再被需要
  }

  auto chunk_status = buffer_pool_.CreateChunk(
      object_id, owner_address, data_size, metadata_size, chunk_index);

  if (chunk_status.ok()) {
    buffer_pool_.WriteChunk(object_id, data_size, metadata_size, chunk_index, data);
    return true;
  }
  // ...
}
```

### 7.2 WriteChunk → Seal 对象

位于 `src/ray/object_manager/object_buffer_pool.cc:120-178`：

```cpp
void ObjectBufferPool::WriteChunk(const ObjectID &object_id,
                                  uint64_t data_size, uint64_t metadata_size,
                                  const uint64_t chunk_index, const std::string &data) {
  std::optional<ObjectBufferPool::ChunkInfo> chunk_info;
  {
    absl::MutexLock lock(&pool_mutex_);
    // ...校验和状态更新...
    it->second.chunk_state_.at(chunk_index) = CreateChunkState::SEALED;
    it->second.num_inflight_copies_++;
  }

  // ★ 写入数据到 Plasma 共享内存
  std::memcpy(chunk_info->data_, data.data(), chunk_info->buffer_length_);

  {
    absl::MutexLock lock(&pool_mutex_);
    it->second.num_inflight_copies_--;
    it->second.num_seals_remaining_--;
    if (it->second.num_seals_remaining_ == 0) {
      // ★ 所有 chunk 都收到了 → Seal 对象
      RAY_CHECK_OK(store_client_->Seal(object_id));
      RAY_CHECK_OK(store_client_->Release(object_id));
      create_buffer_state_.erase(it);
    }
  }
}
```

### 7.3 Plasma Store 收到 Seal → MarkObjectSealed → 唤醒 GetRequest

位于 `src/ray/object_manager/plasma/store.cc:275-286`：

```cpp
void PlasmaStore::SealObjects(const std::vector<ObjectID> &object_ids) {
  for (size_t i = 0; i < object_ids.size(); ++i) {
    auto entry = object_lifecycle_mgr_.SealObject(object_ids[i]);
    RAY_CHECK(entry) << object_ids[i] << " is missing or not sealed.";
    add_object_callback_(entry);          // → ObjectManager::HandleObjectAdded()
  }

  for (size_t i = 0; i < object_ids.size(); ++i) {
    get_request_queue_.MarkObjectSealed(object_ids[i]);  // ★ 唤醒等待的 Get 请求
  }
}
```

### 7.4 GetRequestQueue::MarkObjectSealed —— 核心唤醒逻辑

位于 `src/ray/object_manager/plasma/get_request_queue.cc:156-201`：

```cpp
void GetRequestQueue::MarkObjectSealed(const ObjectID &object_id) {
  auto it = object_get_requests_.find(object_id);
  if (it == object_get_requests_.end()) {
    return;  // 没有等待此对象的 Get 请求
  }

  auto &get_requests = it->second;
  size_t index = 0;
  size_t num_requests = get_requests.size();

  for (size_t i = 0; i < num_requests; ++i) {
    auto get_request = get_requests[index];

    // 填充此对象的 Plasma 元信息
    auto entry = object_lifecycle_mgr_.GetObject(object_id);
    RAY_CHECK(entry != nullptr);
    auto *plasma_object = &get_request->objects_[object_id];
    entry->ToPlasmaObject(plasma_object, /*check sealed*/ true);

    // 增加满足计数
    get_request->num_unique_objects_satisfied_ += 1;

    // 通知对象已满足
    object_satisfied_callback_(object_id, fallback_allocated_fd, get_request);

    // ★ 如果所有请求的对象都已就绪，回复客户端
    if (get_request->num_unique_objects_satisfied_ ==
        get_request->num_unique_objects_to_wait_for_) {
      OnGetRequestCompleted(get_request);  // ★ 触发 ReturnFromGet
    } else {
      index += 1;
    }
  }

  // 清理：不再有 Get 请求等待此对象
  it = object_get_requests_.find(object_id);
  if (it != object_get_requests_.end()) {
    object_get_requests_.erase(it);
  }
}
```

### 7.5 OnGetRequestCompleted → ReturnFromGet → SendGetReply

```cpp
// get_request_queue.cc:211-215
void GetRequestQueue::OnGetRequestCompleted(
    const std::shared_ptr<GetRequest> &get_request) {
  all_objects_satisfied_callback_(get_request);  // → PlasmaStore::ReturnFromGet()
  RemoveGetRequest(get_request);
}
```

位于 `src/ray/object_manager/plasma/store.cc:195-236`：

```cpp
void PlasmaStore::ReturnFromGet(const std::shared_ptr<GetRequest> &get_request) {
  if (get_request->IsRemoved()) {
    return;
  }

  // 计算需要发送的文件描述符
  absl::flat_hash_set<MEMFD_TYPE> fds_to_send;
  std::vector<MEMFD_TYPE> store_fds;
  std::vector<int64_t> mmap_sizes;
  for (const auto &object_id : get_request->object_ids_) {
    const PlasmaObject &object = get_request->objects_[object_id];
    MEMFD_TYPE fd = object.store_fd;
    if (object.data_size != -1 && fds_to_send.count(fd) == 0 && fd.first != INVALID_FD) {
      fds_to_send.insert(fd);
      store_fds.push_back(fd);
      mmap_sizes.push_back(object.mmap_size);
    }
  }

  // ★ 向客户端 socket 写入 Get 回复
  Status s = SendGetReply(std::dynamic_pointer_cast<Client>(get_request->client_),
                          &get_request->object_ids_[0],
                          get_request->objects_,
                          get_request->object_ids_.size(),
                          store_fds, mmap_sizes);

  // ★ 发送共享内存文件描述符
  if (s.ok()) {
    for (MEMFD_TYPE store_fd : store_fds) {
      Status send_fd_status = get_request->client_->SendFd(store_fd);
    }
  }
}
```

**`SendGetReply` 写入 Unix domain socket → Worker 端的 `PlasmaReceive` 解除阻塞。**

### 7.6 Plasma Store 端的 GetRequest 等待队列

当 Worker 的 `PlasmaClient` 发送 Get 请求时，Plasma Store 收到后如何处理（`get_request_queue.cc:54-103`）：

```cpp
void GetRequestQueue::AddRequest(const std::shared_ptr<ClientInterface> &client,
                                 const std::vector<ObjectID> &object_ids,
                                 int64_t timeout_ms) {
  const absl::flat_hash_set<ObjectID> unique_ids(object_ids.begin(), object_ids.end());
  auto get_request =
      std::make_shared<GetRequest>(io_context_, client, object_ids, unique_ids.size());

  for (const auto &object_id : unique_ids) {
    auto entry = object_lifecycle_mgr_.GetObject(object_id);
    if (entry != nullptr && entry->Sealed()) {
      // 对象已在本地 → 直接计入已满足
      auto *plasma_object = &get_request->objects_[object_id];
      entry->ToPlasmaObject(plasma_object, /*checksealed*/ true);
      get_request->num_unique_objects_satisfied_ += 1;
      object_satisfied_callback_(object_id, fallback_allocated_fd, get_request);
    } else {
      // 对象不在本地 → 挂入等待队列
      get_request->objects_[object_id].data_size = -1;
      object_get_requests_[object_id].push_back(get_request);
    }
  }

  if (get_request->num_unique_objects_satisfied_ ==
          get_request->num_unique_objects_to_wait_for_ ||
      timeout_ms == 0) {
    // 全部就绪或 timeout=0 → 立即回复
    OnGetRequestCompleted(get_request);
  } else if (timeout_ms != -1) {
    // 有超时 → 设定时器，到期后回复（即使对象未全部就绪）
    get_request->AsyncWait(timeout_ms,
                           [this, get_request](const boost::system::error_code &ec) {
                             if (ec != boost::asio::error::operation_aborted) {
                               OnGetRequestCompleted(get_request);
                             }
                           });
  }
  // timeout_ms == -1 → 不设定时器，一直等直到所有对象 Seal
}
```

---

## 8. 完整时序图

```
Python ray.get()     CoreWorker          PlasmaStoreProvider       Raylet            Remote Node
     │                   │                      │                     │                    │
     │─GetObjects(ids)──►│                      │                     │                    │
     │                   │                      │                     │                    │
     │                   │─MemoryStore::Get()──►│                     │                    │
     │                   │ IsInPlasmaError!     │                     │                    │
     │                   │ 移入 plasma_ids      │                     │                    │
     │                   │                      │                     │                    │
     │                   │─PlasmaStoreProvider::Get(ids, owners, timeout)──►               │
     │                   │                      │                     │                    │
     │                   │                      │─AsyncGetObjects()──►│                    │
     │                   │                      │ (异步，不等回复)     │                    │
     │                   │                      │                     │─Pull()──►          │
     │                   │                      │                     │ SubscribeLocations │─PullRequest──►
     │                   │                      │                     │                    │ HandlePull()
     │                   │                      │                     │                    │ → Push()
     │                   │                      │                     │◄──PushObjectChunk──│
     │                   │                      │                     │  (多个 chunk)      │
     │                   │                      │                     │                    │
     │                   │                      │  同时...            │                    │
     │                   │                      │─store_client_::Get──►│(Plasma Store进程)  │
     │                   │                      │  SendGetRequest────►│                    │
     │                   │                      │                     │                    │
     │                   │                      │     Plasma Store:    │                    │
     │                   │                      │     对象不可用 →     │                    │
     │                   │                      │     GetRequestQueue  │                    │
     │                   │                      │     等待...          │                    │
     │                   │                      │                     │                    │
     │                   │                      │  Raylet 收到 chunk:  │                    │
     │                   │                      │  ReceiveObjectChunk()│                    │
     │                   │                      │  → WriteChunk()      │                    │
     │                   │                      │  → 最后一个chunk:    │                    │
     │                   │                      │    Seal → MarkObjectSealed               │
     │                   │                      │                     │                    │
     │                   │                      │     Plasma Store:    │                    │
     │                   │                      │     MarkObjectSealed()│                   │
     │                   │                      │     → num_satisfied == num_to_wait        │
     │                   │                      │     → ReturnFromGet() │                   │
     │                   │                      │     → SendGetReply+fd│                   │
     │                   │                      │                     │                    │
     │                   │                      │◄──PlasmaGetReply────│                    │
     │                   │                      │  (共享内存 fd)       │                    │
     │                   │                      │                     │                    │
     │                   │                      │─构造 RayObject ◄────│                    │
     │                   │                      │  (TrackedBuffer包装  │                    │
     │                   │                      │   共享内存指针)      │                    │
     │                   │◄──result_map─────────│                     │                    │
     │◄──Python 对象─────│                      │                     │                    │
```

---

## 9. OnGetRequestCompleted 与 while 循环的关系

### 9.1 是通知机制，不是轮询

虽然 `PlasmaStoreProvider::Get()` 外层有个 `while` 循环，看起来像轮询，但循环体内调用的 `store_client_->Get()` 实际上是**阻塞在 Unix domain socket 读取上**。对象 Seal 后，Plasma Store 主动写 socket 回复，阻塞解除。

```
Worker 进程                                           Plasma Store 进程
    │                                                      │
    │  while (!remaining_ids.empty()) {                     │
    │    GetObjectsFromPlasmaStore(batch_timeout)           │
    │      store_client_->Get(ids, batch_timeout)           │
    │        GetBuffers()                                   │
    │          SendGetRequest() ─── Unix socket ──────────► │
    │                                                     ProcessGetRequest()
    │                                                     get_request_queue_.AddRequest()
    │                                                     对象不在本地 → 挂起等待
    │                                                      │
    │          PlasmaReceive() ◄──── 阻塞等 socket ─────── │
    │          (线程挂起，不消耗 CPU)                        │
    │                                                      │
    │                                                      │ ...远程对象到达...
    │                                                      │ ReceiveObjectChunk()
    │                                                      │ WriteChunk() → Seal
    │                                                      │ SealObjects()
    │                                                      │   → MarkObjectSealed()
    │                                                      │     num_satisfied == num_to_wait
    │                                                      │     → OnGetRequestCompleted()
    │                                                      │       → ReturnFromGet()
    │                                                      │         → SendGetReply() ──►
    │          PlasmaReceive() ◄───── socket 可读 ──────────│
    │          ★ 阻塞解除！                                  │
    │                                                      │
    │    拿到 plasma_results                               │
    │    从 remaining_ids 中移除已获取的 object             │
    │    检查 check_signals_                               │
    │    继续下一轮 while                                   │
    │  }                                                   │
```

### 9.2 为什么 while 循环存在——唯一原因：check_signals

看循环体内每次传给 Plasma 的 timeout：

```cpp
// plasma_store_provider.cc:313-315
int64_t batch_timeout =
    std::max(RayConfig::instance().get_check_signal_interval_milliseconds(),
             static_cast<int64_t>(10 * batch_ids.size()));
```

**不是传原始的 `timeout_ms`**，而是传一个短超时（几百毫秒级别）。

如果用一次 `Get(timeout_ms=300000)`（5分钟），这 5 分钟内线程完全阻塞在 `PlasmaReceive()` 上，没有任何机会执行 `check_signals_()`。用户按 Ctrl+C 无法响应，任务取消无法生效。

### 9.3 对比：如果去掉 while 循环

```cpp
// 假设方案：一次调用
Status CoreWorkerPlasmaStoreProvider::Get(..., int64_t timeout_ms, ...) {
    RAY_RETURN_NOT_OK(raylet_ipc_client_->AsyncGetObjects(...));

    // 一次性等所有对象，传入完整 timeout
    RAY_RETURN_NOT_OK(GetObjectsFromPlasmaStore(
        remaining_object_id_to_idx, batch_ids, timeout_ms, results, &got_exception));

    // ★ 没有地方检查 check_signals_！
    // ★ 如果 timeout_ms == -1，用户永远无法 Ctrl+C
}
```

### 9.4 同样的模式在 Memory Store 也存在

```cpp
// memory_store.cc:336-347
while (!timed_out && signal_status.ok() &&
       !(done = get_request->Wait(iteration_timeout))) {   // ← 短超时等待
    if (check_signals_) {
        signal_status = check_signals_();                   // ← 每轮检查信号
    }
    if (remaining_timeout >= 0) {
        remaining_timeout -= iteration_timeout;
        ...
    }
}
```

Memory Store 也是把总超时拆成短间隔，每轮 `Wait` 结束后检查信号。模式完全一致。

### 9.5 while 循环的本质

```
不是轮询（反复 timeout=0 去试）
     ↓
不是多轮部分返回的拼装（Plasma Store 一次能返回全部）
     ↓
是 "短超时阻塞 + 信号检查" 的循环包装
     ↓
本质：把一个长阻塞拆成多个短阻塞，每两个短阻塞之间插入信号检查
```

用图表示：

```
方案 A：一次长阻塞（不可行）
───────────────────────────────────────────── 5分钟 ──────────────────────────────────────────────
  PlasmaReceive 阻塞                                                                 Plasma 回复
  ★ 无法检查信号

方案 B：拆成短阻塞 + 信号检查（当前设计）
──batch_timeout──┬──check──┬──batch_timeout──┬──check──┬──batch_timeout──┬──check──
  PlasmaReceive    signals    PlasmaReceive     signals    PlasmaReceive     signals
  阻塞等待         Ctrl+C?   阻塞等待          Ctrl+C?   阻塞等待          Ctrl+C?
```

### 9.6 Plasma Store 一次 Get 能返回所有对象吗？

能。看 `AddRequest` 的逻辑：

- 如果传入 `timeout_ms = -1`（无限等待），Plasma Store **不设定时器**，直到所有对象都 Seal 后才回复
- 如果传入 `timeout_ms > 0`，设定定时器，到期后无论是否全部就绪都回复（未就绪的对象 `data_size = -1`）
- 如果传入 `timeout_ms = 0`，立即回复当前已就绪的对象

**所以 while 循环不是为了拼装部分结果，Plasma Store 一次就能返回全部对象。**

---

## 10. 通知机制总结

| 环节 | 等待方 | 通知方 | 通知手段 |
|------|--------|--------|----------|
| Memory Store 层 | `GetRequest::Wait()` | `MemoryStore::Put()` → `GetRequest::Set()` → `cv_.notify_all()` | 条件变量 |
| Plasma Store 层 | `PlasmaClient::PlasmaReceive()` | `PlasmaStore::ReturnFromGet()` → `SendGetReply()` | Unix domain socket 回复 |
| Plasma Store 内部 | `GetRequestQueue` | `SealObjects()` → `MarkObjectSealed()` | 直接函数调用（同进程） |
| 跨节点拉取 | `ObjectManager::Pull()` | 远程 `HandlePull()` → `Push()` → 本地 `ReceiveObjectChunk()` → `Seal` | gRPC + chunk 传输 |

### 本地对象 vs 远程对象的路径差异

| 场景 | 路径 | 是否走网络 |
|------|------|-----------|
| 对象在本地 Memory Store | `MemoryStore::Get()` 直接返回 | 否 |
| 对象在本地 Plasma Store | `PlasmaStoreProvider::Get()` → `store_client_->Get(timeout=0)` 立即拿到 | 否（共享内存读取） |
| 对象在远程节点 Plasma Store | `AsyncGetObjects` 触发 Pull → 远程 Push → 本地 Seal → `store_client_->Get()` 返回 | 是（跨节点传输 chunk） |

---

## 11. 核心洞察

1. **从 Plasma 获取数据的"等待-唤醒"不是条件变量，而是 Unix domain socket 的阻塞 I/O。** Worker 的 `PlasmaClient` 通过 socket 向 Plasma Store 进程发 Get 请求，Plasma Store 在对象 Seal 后才回复 socket，`PlasmaReceive` 解除阻塞。

2. **Plasma Store 内部用 `GetRequestQueue` + 定时器管理等待中的请求。** `MarkObjectSealed()` 检查是否有 Get 请求等待此对象，如果有且全部对象就绪，调用 `ReturnFromGet()` 写 socket 回复。

3. **外层 while 循环不是轮询，是"轮次"——每轮内部 `PlasmaReceive` 是阻塞在 socket 上的。** 循环只是为了处理部分结果、检查信号、控制超时，而不是反复用 timeout=0 去试。

4. **while 循环存在的唯一原因是 `check_signals_()`。** 需要在阻塞等待的间隙检查 Ctrl-C 和任务取消信号。把一个长阻塞拆成多个短阻塞，每两个短阻塞之间插入信号检查。

5. **Plasma Store 一次 `Get` 能返回所有请求的对象。** `AddRequest` 时如果不设定时器（`timeout_ms == -1`），会等所有对象 Seal 后才回复。while 循环不是为了拼装部分结果。
