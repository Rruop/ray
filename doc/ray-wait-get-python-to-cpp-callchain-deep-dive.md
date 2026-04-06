# Ray wait/get 从 Python 到 C++ CoreWorker 完整调用链深度分析

本文详细追踪 `ray.wait` 和 `ray.get` 从 Python 层到 C++ CoreWorker 层的完整代码路径，
包含每一层的具体代码逻辑和参数转换。

---

## 目录

1. [ray.wait 完整调用链](#1-raywait-完整调用链)
2. [ray.get 完整调用链](#2-rayget-完整调用链)
3. [C++ memory_store 内部机制](#3-c-memory_store-内部机制)
4. [C++ plasma_store_provider 内部机制](#4-c-plasma_store_provider-内部机制)
5. [ready/not_ready 判定的完整场景表](#5-readynot_ready-判定的完整场景表)
6. [ray.wait 与 ray.get 之间的竞争条件](#6-raywait-与-rayget-之间的竞争条件)

---

## 1. ray.wait 完整调用链

### 1.1 Python 入口: `ray.wait()`

**文件**: `python/ray/_private/worker.py:3090-3230`

```python
@PublicAPI
@client_mode_hook
def wait(
    ray_waitables: List[Union[ObjectRef, ObjectRefGenerator]],
    *,
    num_returns: int = 1,
    timeout: Optional[float] = None,
    fetch_local: bool = True,
) -> Tuple[
    List[Union[ObjectRef, ObjectRefGenerator]],
    List[Union[ObjectRef, ObjectRefGenerator]],
]:
    worker = global_worker
    worker.check_connected()

    # ... 参数校验 ...

    # 默认超时 10^6 秒（约 11.5 天，基本等于无限）
    timeout = timeout if timeout is not None else 10**6
    timeout_milliseconds = int(timeout * 1000)

    # 调用 Cython 绑定
    ready_ids, remaining_ids = worker.core_worker.wait(
        ray_waitables,
        num_returns,
        timeout_milliseconds,
        fetch_local,
    )
    return ready_ids, remaining_ids
```

**关键参数转换**:
- `timeout` (秒, float/None) → `timeout_milliseconds` (毫秒, int)
- `None` → `10**6 * 1000` = 10^9 毫秒（几乎无限等待）
- `0` → `0` 毫秒（非阻塞）
- `0.1` → `100` 毫秒

### 1.2 Cython 绑定层: `CoreWorker.wait()`

**文件**: `python/ray/_raylet.pyx:3252-3293`

```cython
def wait(self,
         object_refs_or_generators,
         int num_returns,
         int64_t timeout_ms,
         c_bool fetch_local):
    cdef:
        c_vector[CObjectID] wait_ids
        c_vector[c_bool] results

    # ===== 关键步骤1: 提取等待的 ObjectID =====
    object_refs = []
    for ref_or_generator in object_refs_or_generators:
        if isinstance(ref_or_generator, ObjectRefGenerator):
            # 对 ObjectRefGenerator，提取其 ObjectRefStream 中
            # next_index_ 对应的确定性 ObjectID
            object_refs.append(ref_or_generator._get_next_ref())
        else:
            object_refs.append(ref_or_generator)

    # 转换为 C++ ObjectID vector
    wait_ids = ObjectRefsToVector(object_refs)

    # ===== 关键步骤2: 调用 C++ CoreWorker::Wait =====
    with nogil:  # 释放 GIL，允许 C++ 线程安全执行
        op_status = CCoreWorkerProcess.GetCoreWorker().Wait(
            wait_ids, num_returns, timeout_ms, &results, fetch_local)
    check_status(op_status)

    # ===== 关键步骤3: 构造返回值 =====
    # 返回原始的 ObjectRef/ObjectRefGenerator，不是提取的 ObjectID
    ready, not_ready = [], []
    for i, object_ref_or_generator in enumerate(object_refs_or_generators):
        if results[i]:
            ready.append(object_ref_or_generator)
        else:
            not_ready.append(object_ref_or_generator)

    return ready, not_ready
```

**ObjectRefGenerator 的 `_get_next_ref()` 调用链**:

```python
# python/ray/_private/object_ref_generator.py:178
def _get_next_ref(self) -> "ray.ObjectRef":
    """Return the next reference from a generator.
    Note that the ObjectID generated from a generator is always deterministic.
    """
    self.worker.check_connected()
    core_worker = self.worker.core_worker
    return core_worker.peek_object_ref_stream(self._generator_ref)[0]
    #                                                      ^^^^
    # 只取 ObjectRef，丢弃 is_ready 布尔值
```

```cython
# python/ray/_raylet.pyx:4767
def peek_object_ref_stream(self, ObjectRef generator_id):
    cdef:
        CObjectID c_generator_id = generator_id.native()
        pair[CObjectReference, c_bool] c_object_ref_and_is_ready_pair

    with nogil:
        c_object_ref_and_is_ready_pair = (
                CCoreWorkerProcess.GetCoreWorker().PeekObjectRefStream(
                    c_generator_id))

    return (ObjectRef(
                c_object_ref_and_is_ready_pair.first.object_id(),
                c_object_ref_and_is_ready_pair.first.owner_address()
                    .SerializeAsString()),
            c_object_ref_and_is_ready_pair.second)  # is_ready
```

**C++ `PeekObjectRefStream`**:

```cpp
// src/ray/core_worker/task_manager.cc:731
std::pair<ObjectID, bool> TaskManager::PeekObjectRefStream(
    const ObjectID &generator_id) {
  ObjectID next_object_id;
  absl::MutexLock lock(&object_ref_stream_ops_mu_);
  auto stream_it = object_ref_streams_.find(generator_id);
  RAY_CHECK(stream_it != object_ref_streams_.end());

  const auto &result = stream_it->second.PeekNextItem();
  // 临时拥有这个 ref，防止 GC
  TemporarilyOwnGeneratorReturnRefIfNeededInternal(result.first, generator_id);
  return result;
}
```

```cpp
// src/ray/core_worker/task_manager.cc:162
std::pair<ObjectID, bool> ObjectRefStream::PeekNextItem() {
  // ObjectID 是确定性的：由 task_id + next_index_ 计算得出
  const auto &object_id = GetObjectRefAtIndex(next_index_);
  if (refs_written_to_stream_.find(object_id) ==
      refs_written_to_stream_.end()) {
    return {object_id, false};  // ← slot 未写入值，not ready
  } else {
    return {object_id, true};   // ← slot 已写入值，ready
  }
}
```

```cpp
// src/ray/core_worker/task_manager.cc:232
ObjectID ObjectRefStream::GetObjectRefAtIndex(int64_t generator_index) const {
  // Index 0 保留给 generator task 本身的 return
  // 从 index 2 开始是 streaming yields
  return ObjectID::FromIndex(generator_task_id_, 2 + generator_index);
}
```

**核心要点**:
- `_get_next_ref()` 是**非消费性 peek**，不会推进 `next_index_`
- ObjectID 由 `task_id + index` 确定性计算，**不依赖 Task 是否执行**
- `PeekNextItem` 的 `is_ready` 仅反映 ObjectRefStream 的写入状态，
  不反映对象值是否在 plasma store 中可用

### 1.3 C++ CoreWorker::Wait()

**文件**: `src/ray/core_worker/core_worker.cc:1678-1789`

```cpp
Status CoreWorker::Wait(const std::vector<ObjectID> &ids,
                        int num_objects,
                        int64_t timeout_ms,
                        std::vector<bool> *results,
                        bool fetch_local) {
  results->resize(ids.size(), false);

  // ===== 前置检查：所有 ObjectID 是否有 Owner =====
  size_t objs_without_owners = 0;
  for (size_t i = 0; i < ids.size(); i++) {
    if (!HasOwner(ids[i])) {
      ++objs_without_owners;
    }
    // 足够的有 owner 的对象才能继续
  }

  int64_t start_time = current_time_ms();
  absl::flat_hash_set<ObjectID> ready, plasma_object_ids;
  ready.reserve(num_objects);

  // ===== Step 1: memory_store_->Wait() =====
  RAY_RETURN_NOT_OK(memory_store_->Wait(
      memory_object_ids,
      std::min(static_cast<int>(memory_object_ids.size()), num_objects),
      timeout_ms,
      *worker_context_,
      &ready,
      &plasma_object_ids));

  // 计算剩余超时时间
  if (timeout_ms > 0) {
    timeout_ms = std::max(0,
        static_cast<int>(timeout_ms - (current_time_ms() - start_time)));
  }

  // ===== Step 2: 处理 plasma 对象 =====
  if (fetch_local) {
    // Step 2a: fetch_local=True → 通过 raylet IPC 等待对象拉到本地
    if (!plasma_object_ids.empty()) {
      std::vector<ObjectID> object_ids(
          plasma_object_ids.begin(), plasma_object_ids.end());
      auto owner_addresses = reference_counter_->GetOwnerAddresses(object_ids);

      RAY_RETURN_NOT_OK(plasma_store_provider_->Wait(
          object_ids,
          owner_addresses,
          std::min(static_cast<int>(plasma_object_ids.size()),
                   num_objects - static_cast<int>(ready.size())),
          timeout_ms,
          *worker_context_,
          &ready));
    }
  } else {
    // Step 2b: fetch_local=False → 直接标记 ready，不做任何网络操作
    for (const auto &object_id : plasma_object_ids) {
      if (ready.size() == static_cast<size_t>(num_objects)) {
        break;
      }
      ready.insert(object_id);
    }
  }

  // ===== 填充结果 =====
  for (size_t i = 0; i < ids.size(); i++) {
    if (ready.find(ids[i]) != ready.end()) {
      results->at(i) = true;
    }
  }

  return Status::OK();
}
```

### 1.4 memory_store_->Wait()

**文件**: `src/ray/core_worker/store_provider/memory_store/memory_store.cc:412-442`

```cpp
Status CoreWorkerMemoryStore::Wait(
    const absl::flat_hash_set<ObjectID> &object_ids,
    int num_objects,
    int64_t timeout_ms,
    const WorkerContext &ctx,
    absl::flat_hash_set<ObjectID> *ready,
    absl::flat_hash_set<ObjectID> *plasma_object_ids) {

  std::vector<ObjectID> id_vector(object_ids.begin(), object_ids.end());
  std::vector<std::shared_ptr<RayObject>> result_objects;

  // 调用 GetImpl，abort_if_any_object_is_exception=false
  // 这意味着异常对象（如 OBJECT_LOST）也算"已就绪"
  auto status = GetImpl(id_vector, num_objects, timeout_ms, ctx,
                        &result_objects,
                        /*abort_if_any_object_is_exception=*/false,
                        /*at_most_num_objects=*/false);

  // 忽略 TimedOut，因为我们只关心已获取到的对象
  if (!status.IsTimedOut()) {
    RAY_RETURN_NOT_OK(status);
  }

  // 根据结果分类
  for (size_t i = 0; i < id_vector.size(); i++) {
    if (result_objects[i] != nullptr) {
      if (result_objects[i]->IsInPlasmaError()) {
        // 对象在 plasma 中 → 路由到 plasma 路径
        plasma_object_ids->insert(id_vector[i]);
      } else if (ready->size() < static_cast<size_t>(num_objects)) {
        // 对象值直接在 memory_store 中（包括错误对象）→ ready
        ready->insert(id_vector[i]);
      }
    }
    // result_objects[i] == nullptr → 对象不在任何 store 中 → not_ready
  }
  return Status::OK();
}
```

### 1.5 memory_store_->GetImpl()

**文件**: `src/ray/core_worker/store_provider/memory_store/memory_store.cc:258-386`

```cpp
Status CoreWorkerMemoryStore::GetImpl(
    const std::vector<ObjectID> &object_ids,
    int num_objects,
    int64_t timeout_ms,
    const WorkerContext &ctx,
    std::vector<std::shared_ptr<RayObject>> *results,
    bool abort_if_any_object_is_exception,
    bool at_most_num_objects) {

  (*results).resize(object_ids.size(), nullptr);
  std::shared_ptr<GetRequest> get_request;
  int num_found = 0;

  {
    absl::MutexLock lock(&mu_);
    absl::flat_hash_set<ObjectID> remaining_ids;

    // ===== 第一遍：检查 objects_ map 中已有条目 =====
    for (size_t i = 0; i < object_ids.size(); i++) {
      const auto &object_id = object_ids[i];
      auto iter = objects_.find(object_id);
      if (iter != objects_.end()) {
        // 命中！对象在 memory_store 中
        iter->second->SetAccessed();
        (*results)[i] = iter->second;
        num_found += 1;
      } else {
        // 未命中，加入等待集合
        remaining_ids.insert(object_id);
      }
      if (num_found >= num_objects && at_most_num_objects) {
        break;
      }
    }

    // 所有对象都找到了，或者已有足够多的对象
    if (remaining_ids.empty() || num_found >= num_objects ||
        existing_objects_has_exception) {
      return Status::OK();
    }

    // ===== 创建 GetRequest 等待剩余对象 =====
    size_t required_objects = num_objects - num_found;
    get_request = std::make_shared<GetRequest>(
        std::move(remaining_ids), required_objects,
        abort_if_any_object_is_exception);
    for (const auto &object_id : get_request->ObjectIds()) {
      object_get_requests_[object_id].push_back(get_request);
    }
  }

  // ===== 等待对象到达或超时 =====
  bool timed_out = false;
  int64_t remaining_timeout = timeout_ms;
  int64_t iteration_timeout =
      timeout_ms == -1
          ? RayConfig::instance().get_check_signal_interval_milliseconds()
          : std::min(timeout_ms,
                     RayConfig::instance()
                         .get_check_signal_interval_milliseconds());

  while (!timed_out && signal_status.ok() &&
         !(done = get_request->Wait(iteration_timeout))) {
    if (check_signals_) {
      signal_status = check_signals_();
    }
    if (remaining_timeout >= 0) {
      remaining_timeout -= iteration_timeout;
      iteration_timeout = std::min(remaining_timeout, iteration_timeout);
      timed_out = remaining_timeout <= 0;  // ← 超时退出
    }
  }

  // 填充结果：从 GetRequest 中获取已到达的对象
  {
    absl::MutexLock lock(&mu_);
    for (size_t i = 0; i < object_ids.size(); i++) {
      if ((*results)[i] == nullptr) {
        (*results)[i] = get_request->Get(object_id);
      }
    }
    // 清理 GetRequest
    for (const auto &object_id : get_request->ObjectIds()) {
      auto iter = object_get_requests_.find(object_id);
      if (iter != object_get_requests_.end()) {
        auto &get_requests = iter->second;
        get_requests.erase(
            std::remove(get_requests.begin(), get_requests.end(), get_request),
            get_requests.end());
        if (get_requests.empty()) {
          object_get_requests_.erase(iter);
        }
      }
    }
  }

  if (!signal_status.ok()) {
    return signal_status;
  } else if (done) {
    return Status::OK();
  } else {
    return Status::TimedOut("Get timed out: some object(s) not ready.");
  }
}
```

### 1.6 GetRequest::Wait() — 条件变量等待

**文件**: `src/ray/core_worker/store_provider/memory_store/memory_store.cc:85-99`

```cpp
bool GetRequest::Wait(int64_t timeout_ms) {
  RAY_CHECK(timeout_ms >= 0 || timeout_ms == -1);
  if (timeout_ms == -1) {
    // 永远等待
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { return is_ready_; });
    return true;
  }

  // 带超时等待
  std::unique_lock<std::mutex> lock(mutex_);
  auto is_ready_status_after_timeout = cv_.wait_for(
      lock, std::chrono::milliseconds(timeout_ms),
      [this]() { return is_ready_; });
  return is_ready_status_after_timeout;
}
```

**当 `timeout_ms=0` 时**: `cv_.wait_for(0ms)` 做一次非阻塞条件检查，
如果 `is_ready_=false` 则立即返回 `false`。

### 1.7 GetRequest::Set() — 对象到达时唤醒等待者

```cpp
// memory_store.cc:101
void GetRequest::Set(const ObjectID &object_id,
                     std::shared_ptr<RayObject> object) {
  std::scoped_lock<std::mutex> lock(mutex_);
  if (is_ready_) {
    return;  // 已经满足条件
  }
  object->SetAccessed();
  objects_.emplace(object_id, object);

  if (objects_.size() == num_objects_ ||
      (abort_if_any_object_is_exception_ && object->IsException() &&
       !object->IsInPlasmaError())) {
    is_ready_ = true;
    cv_.notify_all();  // ← 唤醒所有等待线程
  }
}
```

### 1.8 memory_store_->Put() — 写入对象并触发唤醒

```cpp
// memory_store.cc:172
void CoreWorkerMemoryStore::Put(const RayObject &object,
                                const ObjectID &object_id,
                                const bool has_reference) {
  std::shared_ptr<RayObject> object_entry = std::make_shared<RayObject>(...);

  {
    absl::MutexLock lock(&mu_);
    auto iter = objects_.find(object_id);
    if (iter != objects_.end()) {
      return;  // 已存在，不覆盖
    }

    // 通知异步回调
    auto async_callback_it = object_async_get_requests_.find(object_id);
    // ...

    // 通知同步 GetRequest
    auto object_request_iter = object_get_requests_.find(object_id);
    if (object_request_iter != object_get_requests_.end()) {
      auto &get_requests = object_request_iter->second;
      for (auto &get_request : get_requests) {
        get_request->Set(object_id, object_entry);
        // ↑ Set() 中 cv_.notify_all() 唤醒等待线程
      }
    }

    // 存入 objects_ map
    if (!reference_counting_enabled_ || has_reference) {
      objects_.emplace(object_id, object_entry);
    }
  }
}
```

### 1.9 plasma_store_provider_->Wait() (fetch_local=True 时)

**文件**: `src/ray/core_worker/store_provider/plasma_store_provider.cc:362-398`

```cpp
Status CoreWorkerPlasmaStoreProvider::Wait(
    const std::vector<ObjectID> &object_ids,
    const std::vector<rpc::Address> &owner_addresses,
    int num_objects,
    int64_t timeout_ms,
    const WorkerContext &ctx,
    absl::flat_hash_set<ObjectID> *ready) {

  bool should_break = false;
  int64_t remaining_timeout = timeout_ms;
  absl::flat_hash_set<ObjectID> ready_in_plasma;

  while (!should_break) {
    int64_t call_timeout =
        RayConfig::instance().get_check_signal_interval_milliseconds();
    if (remaining_timeout >= 0) {
      call_timeout = std::min(remaining_timeout, call_timeout);
      remaining_timeout -= call_timeout;
      should_break = remaining_timeout <= 0;
    }

    // 向本地 raylet 发送 IPC，检查对象是否在本地 plasma store 中可用
    RAY_ASSIGN_OR_RETURN(
        ready_in_plasma,
        raylet_ipc_client_->Wait(
            object_ids, owner_addresses, num_objects, call_timeout));

    if (ready_in_plasma.size() >= static_cast<size_t>(num_objects)) {
      should_break = true;
    }
    // 检查信号（Ctrl+C 等）
    if (check_signals_) {
      RAY_RETURN_NOT_OK(check_signals_());
    }
  }

  for (const auto &entry : ready_in_plasma) {
    ready->insert(entry);
  }
  return Status::OK();
}
```

**raylet IPC 检查逻辑**:
- raylet 查询本地 plasma store 中是否有该对象
- 如果对象正在从远程节点拉取，raylet 会等待拉取完成
- `fetch_local=True` 确保返回 ready 的对象确实在本地可用

---

## 2. ray.get 完整调用链

### 2.1 Python 入口: `ray.get()`

**文件**: `python/ray/_private/worker.py:2868-3014`

```python
@PublicAPI
@client_mode_hook
def get(
    object_refs: Union[ObjectRef, Sequence[ObjectRef], ...],
    *,
    timeout: Optional[float] = None,
    _use_object_store: bool = False,
) -> Union[Any, List[Any]]:

    worker = global_worker
    worker.check_connected()

    with profiling.profile("ray.get"):
        # ObjectRefGenerator 不允许传入 ray.get
        if isinstance(object_refs, ObjectRefGenerator):
            return object_refs

        # 单个 ObjectRef 转为 list 统一处理
        is_individual_id = isinstance(object_refs, ray.ObjectRef)
        if is_individual_id:
            object_refs = [object_refs]

        # ===== 调用 Worker.get_objects =====
        values, debugger_breakpoint = worker.get_objects(
            object_refs, timeout, use_object_store=_use_object_store
        )

        # 检查返回值中的异常
        for i, value in enumerate(values):
            if isinstance(value, RayError):
                if isinstance(value, ray.exceptions.ObjectLostError) and \
                   not isinstance(value, ray.exceptions.OwnerDiedError):
                    worker.core_worker.log_plasma_usage()
                if isinstance(value, RayTaskError):
                    raise value.as_instanceof_cause()
                else:
                    raise value  # ← ObjectLostError, OwnerDiedError 等在此抛出

        if is_individual_id:
            values = values[0]

        return values
```

### 2.2 Worker.get_objects()

**文件**: `python/ray/_private/worker.py:939-1016`

```python
def get_objects(
    self,
    object_refs: list,
    timeout: Optional[float] = None,
    return_exceptions: bool = False,
    skip_deserialization: bool = False,
    use_object_store: bool = False,
) -> Tuple[List[serialization.SerializedRayObject], bytes]:

    # 确保所有输入都是 ObjectRef
    for object_ref in object_refs:
        if not isinstance(object_ref, ObjectRef):
            raise TypeError(...)

    # 秒 → 毫秒
    timeout_ms = (
        int(timeout * 1000) if timeout is not None and timeout != -1 else -1
    )

    # ===== 调用 Cython 绑定 =====
    serialized_objects: List[serialization.SerializedRayObject] = \
        self.core_worker.get_objects(object_refs, timeout_ms)

    if skip_deserialization:
        return None, debugger_breakpoint

    # 反序列化
    values = self.deserialize_objects(serialized_objects, object_refs, use_object_store)

    if not return_exceptions:
        for value in values:
            if isinstance(value, RayError):
                raise value  # ← GetTimeoutError 在此抛出

    return values, debugger_breakpoint
```

### 2.3 Cython 绑定: `CoreWorker.get_objects()`

**文件**: `python/ray/_raylet.pyx:2969-2978`

```cython
def get_objects(self, object_refs, int64_t timeout_ms=-1):
    cdef:
        c_vector[shared_ptr[CRayObject]] results
        c_vector[CObjectID] c_object_ids = ObjectRefsToVector(object_refs)

    with nogil:
        op_status = CCoreWorkerProcess.GetCoreWorker().Get(
            c_object_ids, timeout_ms, results)
    check_status(op_status)

    return RayObjectsToSerializedRayObjects(results, object_refs)
```

**注意**: `check_status(op_status)` 检查 C++ 返回的状态码。
- 如果 C++ 返回 `Status::TimedOut`，`check_status` 会将其转换为 Python 异常
- 对于 `Get`，TimedOut 最终在 Python 层被转换为 `GetTimeoutError`

### 2.4 C++ CoreWorker::Get()

**文件**: `src/ray/core_worker/core_worker.cc:1490-1535`

```cpp
Status CoreWorker::Get(const std::vector<ObjectID> &ids,
                       int64_t timeout_ms,
                       std::vector<std::shared_ptr<RayObject>> &results) {
  // 检查是否包含 experimental channel 对象
  // ...
  // 常规对象走 GetObjects
  return GetObjects(ids, timeout_ms, results);
}
```

### 2.5 C++ CoreWorker::GetObjects()

**文件**: `src/ray/core_worker/core_worker.cc:1548-1640`

```cpp
Status CoreWorker::GetObjects(const std::vector<ObjectID> &ids,
                              const int64_t timeout_ms,
                              std::vector<std::shared_ptr<RayObject>> &results) {

  absl::flat_hash_set<ObjectID> plasma_object_ids;
  absl::flat_hash_set<ObjectID> memory_object_ids(ids.begin(), ids.end());
  absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> result_map;
  auto start_time = current_time_ms();

  // ===== 前置检查：Owner 是否存在 =====
  StatusSet<StatusT::NotFound> objects_have_owners =
      reference_counter_->HasOwner(ids);
  if (objects_have_owners.has_error()) {
    return Status::ObjectUnknownOwner(...);
  }

  bool got_exception = false;

  // ===== Step 1: memory_store_->Get() =====
  if (!memory_object_ids.empty()) {
    RAY_RETURN_NOT_OK(memory_store_->Get(
        memory_object_ids, timeout_ms, *worker_context_,
        &result_map, &got_exception));
    // ↑ 如果返回 TimedOut，直接返回，不走 Step 2
  }

  // 分离 IsInPlasmaError 的结果（需要走 plasma 路径）
  for (auto it = result_map.begin(); it != result_map.end();) {
    auto current = it++;
    if (current->second->IsInPlasmaError()) {
      plasma_object_ids.insert(current->first);
      result_map.erase(current);
    }
  }

  // ===== Step 2: plasma_store_provider_->Get() =====
  if (!got_exception && !plasma_object_ids.empty()) {
    std::vector<ObjectID> object_ids(
        plasma_object_ids.begin(), plasma_object_ids.end());
    auto owner_addresses = reference_counter_->GetOwnerAddresses(object_ids);

    // 计算剩余超时
    int64_t local_timeout_ms = timeout_ms;
    if (timeout_ms >= 0) {
      local_timeout_ms = std::max(static_cast<int64_t>(0),
          timeout_ms - (current_time_ms() - start_time));
    }

    RAY_RETURN_NOT_OK(plasma_store_provider_->Get(
        object_ids, owner_addresses, local_timeout_ms, &result_map));
  }

  // 填充结果
  for (size_t i = 0; i < ids.size(); i++) {
    const auto pair = result_map.find(ids[i]);
    if (pair != result_map.end()) {
      results[i] = pair->second;
    }
  }

  return Status::OK();
}
```

**关键细节**: 如果 Step 1 `memory_store_->Get()` 返回 `Status::TimedOut`，
`RAY_RETURN_NOT_OK` 直接返回错误，**不会走到 Step 2 的 plasma 路径**。

### 2.6 memory_store_->Get()

**文件**: `src/ray/core_worker/store_provider/memory_store/memory_store.cc:388-410`

```cpp
Status CoreWorkerMemoryStore::Get(
    const absl::flat_hash_set<ObjectID> &object_ids,
    int64_t timeout_ms,
    const WorkerContext &ctx,
    absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> *results,
    bool *got_exception) {

  const std::vector<ObjectID> id_vector(object_ids.begin(), object_ids.end());
  std::vector<std::shared_ptr<RayObject>> result_objects;

  // 调用 GetImpl
  RAY_RETURN_NOT_OK(Get(id_vector, id_vector.size(), timeout_ms,
                         ctx, &result_objects));

  for (size_t i = 0; i < id_vector.size(); i++) {
    if (result_objects[i] != nullptr) {
      (*results)[id_vector[i]] = result_objects[i];
      if (result_objects[i]->IsException() &&
          !result_objects[i]->IsInPlasmaError()) {
        *got_exception = true;
      }
    }
  }
  return Status::OK();
}
```

**注意**: 这个 `Get()` 调用的 `GetImpl` 内部的 `abort_if_any_object_is_exception=true`
（默认值），意味着**如果 memory_store 中有任何异常对象（非 IsInPlasmaError），
会立即停止等待并返回**。这与 `Wait()` 的行为不同！

### 2.7 plasma_store_provider_->Get()

**文件**: `src/ray/core_worker/store_provider/plasma_store_provider.cc:253-355`

```cpp
Status CoreWorkerPlasmaStoreProvider::Get(
    const std::vector<ObjectID> &object_ids,
    const std::vector<rpc::Address> &owner_addresses,
    int64_t timeout_ms,
    absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> *results) {

  absl::flat_hash_map<ObjectID, int64_t> remaining_object_id_to_idx;

  // ===== 步骤1: 发起异步拉取请求 =====
  for (int64_t start = 0; start < num_total_objects; start += fetch_batch_size_) {
    // 请求 raylet 开始从远程节点拉取对象到本地
    StatusOr<ipc::ScopedResponse> status_or_cleanup =
        raylet_ipc_client_->AsyncGetObjects(
            batch_ids, batch_owner_addresses, get_request_counter_.fetch_add(1));

    // 尝试从本地 plasma store 获取已经在本地的对象
    RAY_RETURN_NOT_OK(
        GetObjectsFromPlasmaStore(remaining_object_id_to_idx, batch_ids,
                                  /*timeout_ms=*/0, results, &got_exception));
  }

  if (remaining_object_id_to_idx.empty() || got_exception) {
    return Status::OK();  // 所有对象都获取到了，或有异常
  }

  // ===== 步骤2: 轮询等待剩余对象 =====
  bool timed_out = false;
  int64_t remaining_timeout = timeout_ms;
  while (!remaining_object_id_to_idx.empty() && !should_break) {
    int64_t batch_timeout =
        std::max(RayConfig::instance().get_check_signal_interval_milliseconds(),
                 static_cast<int64_t>(10 * batch_ids.size()));
    if (remaining_timeout >= 0) {
      batch_timeout = std::min(remaining_timeout, batch_timeout);
      remaining_timeout -= batch_timeout;
      timed_out = remaining_timeout <= 0;
    }

    RAY_RETURN_NOT_OK(GetObjectsFromPlasmaStore(
        remaining_object_id_to_idx, batch_ids, batch_timeout,
        results, &got_exception));
    should_break = timed_out || got_exception;

    if (check_signals_) {
      Status status = check_signals_();
      if (!status.ok()) return status;
    }
  }

  if (!remaining_object_id_to_idx.empty() && timed_out) {
    return Status::TimedOut(absl::StrFormat(
        "Could not fetch %d objects within the timeout of %dms. "
        "%d objects were not ready.",
        object_ids.size(), timeout_ms,
        remaining_object_id_to_idx.size()));
  }
  return Status::OK();
}
```

---

## 3. C++ memory_store 内部机制

### 3.1 数据结构

```
CoreWorkerMemoryStore
├── objects_: absl::flat_hash_map<ObjectID, shared_ptr<RayObject>>
│   └── 存储所有"值在 memory_store 中"的对象
│       包括：小对象值、错误对象、IsInPlasmaError 标记
│
├── object_get_requests_: unordered_map<ObjectID, vector<shared_ptr<GetRequest>>>
│   └── 每个对象 ID 对应的等待队列
│
└── object_async_get_requests_: unordered_map<ObjectID, vector<function>>
    └── 异步回调队列
```

### 3.2 对象在 memory_store 中的三种状态

| `objects_` 中的值 | 含义 | 对 `Wait` 的效果 | 对 `Get` 的效果 |
|---|---|---|---|
| 无条目 | 对象未到达 | → GetRequest Wait | → GetImpl Wait |
| `IsInPlasmaError()` | 对象在 plasma 中 | → 路由到 plasma | → 路由到 plasma |
| 其他（正常值或错误对象） | 值在 memory_store 中 | → 直接 ready | → 直接返回 |

### 3.3 值写入 memory_store 的时机

```
1. Task 完成时（小对象直接写入 memory_store）
   → worker 完成 task → Put 小对象到 memory_store
   → 如果有人等待 → Set() → cv_.notify_all()

2. 对象被提升到 plasma 时（大对象）
   → Put(OBJECT_IN_PLASMA, object_id) 写入标记
   → 后续 Get/Wait 会路由到 plasma_store_provider

3. 重建失败时
   → recovery_failure_callback_ → Put(RayObject(OBJECT_LOST), object_id)
   → 同时写入 OBJECT_IN_PLASMA 标记到 memory_store
   → 后续 ray.get 会获取到 OBJECT_LOST 错误对象

4. 重建成功时
   → Put(OBJECT_IN_PLASMA, object_id) 写入标记
   → 后续 Get/Wait 路由到 plasma → 从 plasma 中获取重建后的值

5. 对象丢失被检测到时
   → memory_store_->Delete(lost_objects) 删除 IsInPlasmaError 条目
   → 后续 Get/Wait 找不到条目 → 等待或超时
```

---

## 4. C++ plasma_store_provider 内部机制

### 4.1 与 raylet 的 IPC 通信

```
Driver (CoreWorker)
    │
    │ IPC (Unix Domain Socket)
    ▼
本地 Raylet
    │
    ├─ 查询本地 plasma store: 对象是否存在？
    │  → 存在 → 返回 ready
    │  → 不存在 → 发起 fetch 请求到远程 raylet
    │
    └─ 远程 raylet 返回对象数据
       → 写入本地 plasma store
       → 通知 CoreWorker 对象可用
```

### 4.2 `fetch_local=True` vs `fetch_local=False` 的精确区别

| | `fetch_local=True` | `fetch_local=False` |
|---|---|---|
| memory_store 命中 IsInPlasmaError | → plasma_store_provider_->Wait() | → 直接 ready |
| plasma_store_provider_->Wait() 行为 | 向 raylet IPC 查询本地 plasma | 不调用 |
| 对象在远程 plasma 中 | 等待 raylet 拉到本地后返回 ready | 直接 ready（不拉取） |
| 对象在本地 plasma 中 | 返回 ready | 返回 ready |
| 对象丢失（标记被删除） | memory_store 无条目 → not_ready | 同左 |
| 典型用途 | `ray.wait` 需要后续 `ray.get` | `ray.wait` 只需知道对象存在 |

---

## 5. ready/not_ready 判定的完整场景表

### 5.1 Phase 1: `ray.wait(generators, fetch_local=False, timeout=0.1)`

| # | 场景 | memory_store `objects_` | Step 1 结果 | Step 2b 结果 | 最终 |
|---|------|------------------------|------------|-------------|------|
| 1 | Task 运行中，未 yield | 无条目 | nullptr → Wait 超时 | — | **not_ready** |
| 2 | Task yield 了，plasma 有值 | `IsInPlasmaError` | → plasma_object_ids | 直接 ready | **ready** |
| 3 | Task yield 了，对象丢失但 Owner 未检测到 | `IsInPlasmaError`（旧标记还在） | → plasma_object_ids | 直接 ready | **ready** ⚠️ |
| 4 | Task yield 了，对象丢失且 Owner 已检测到 | 无条目（被 Delete） | nullptr → Wait 超时 | — | **not_ready** |
| 5 | Task 永久失败 | 错误对象（非 IsInPlasmaError） | → ready | — | **ready** |
| 6 | Task 重试中，slot 已写入（同 #3/#4） | 同 #3 或 #4 | 同 #3 或 #4 | 同上 | 同上 |
| 7 | Task 重试中，slot 未写入 | 无条目 | nullptr → Wait 超时 | — | **not_ready** |

**场景 #3 是"虚假 ready"**: 对象已丢失但 memory_store 中 IsInPlasmaError 标记
还没被清除。`fetch_local=False` 不做进一步验证，直接视为 ready。
后续如果调用 `ray.get`，才会发现对象丢失并触发重建。

### 5.2 Phase 3: `ray.wait(meta_refs, fetch_local=True, timeout=0.0)`

| # | 场景 | memory_store | Step 1 | Step 2a (raylet IPC) | 最终 |
|---|------|-------------|--------|---------------------|------|
| 1 | meta 正常在本地 plasma | `IsInPlasmaError` | → plasma | 本地有 → ready | **ready** |
| 2 | meta 在远程 plasma | `IsInPlasmaError` | → plasma | 本地没有 → 超时 | **not_ready** |
| 3 | meta 丢失，Owner 未检测到 | `IsInPlasmaError`（旧） | → plasma | raylet 查不到 → 超时 | **not_ready** |
| 4 | meta 丢失，Owner 已检测到 | 无条目 | nullptr → 超时(0ms) | — | **not_ready** |
| 5 | meta 重建成功 | `IsInPlasmaError` | → plasma | 本地有 → ready | **ready** |
| 6 | meta 重建失败，错误对象已写入 | 错误对象 | → ready | — | **ready** |

**对比 Phase 1**: Phase 3 的 `fetch_local=True` 使得场景 #3 不再是"虚假 ready"——
raylet 会验证对象是否真的在本地 plasma 中。

---

## 6. ray.wait 与 ray.get 之间的竞争条件

### 6.1 触发场景

```
T1: Phase 3 ray.wait(meta_refs, fetch_local=True, timeout=0.0)
    → meta_ref 在 ready_meta_refs 中
    → 此时 memory_store 有 IsInPlasmaError 条目
    → 本地 plasma 确实有 meta 对象值

T2: ⚡ 竞争窗口（微秒级）
    可能的事件：
    a) Owner 的 100ms 恢复循环运行 → memory_store_->Delete(meta_object_id)
    b) 本地 raylet 内存压力 eviction 了 meta 对象
    c) 其他线程/进程修改了对象状态

T3: Phase 4 ray.get(meta_ref, timeout=0)
```

### 6.2 ray.get 在竞争条件下的两条路径

**路径 A: memory_store 条目被 Delete**

```
ray.get(meta_ref, timeout=0)
  → CoreWorker::GetObjects()
    → Step 1: memory_store_->Get(timeout_ms=0)
      → objects_ map 中无条目
      → 创建 GetRequest → cv_.wait_for(0ms) → is_ready_=false
      → remaining_timeout=0, timed_out=true
      → 返回 Status::TimedOut
    → RAY_RETURN_NOT_OK(Status::TimedOut)
    → 直接返回，不走 Step 2
  → Python 层抛 GetTimeoutError
```

**路径 B: memory_store 条目还在（IsInPlasmaError），但本地 plasma 中对象被 eviction**

```
ray.get(meta_ref, timeout=0)
  → CoreWorker::GetObjects()
    → Step 1: memory_store_->Get(timeout_ms=0)
      → objects_ map 中有 IsInPlasmaError 条目
      → result_map[meta_ref] = IsInPlasmaError 对象
    → 检测到 IsInPlasmaError → plasma_object_ids.insert(meta_ref)
    → Step 2: plasma_store_provider_->Get(timeout_ms≈0)
      → AsyncGetObjects → raylet 开始拉取
      → GetObjectsFromPlasmaStore(timeout=0) → 本地没有 → 超时
      → 返回 Status::TimedOut
  → Python 层抛 GetTimeoutError
```

### 6.3 Block 会丢失吗？

**当 `should_ignore = True`（max_errored_blocks > 0）**:

```python
# streaming_executor_state.py:695-728
except Exception as e:
    errored_blocks_per_op[state] += 1
    num_errored_blocks += 1
    # should_ignore = True
    logger.error(error_message, exc_info=e)
    # ← _pending_block_ref 和 _pending_meta_ref 没有被清理！
```

下次调度循环：

```
Phase 2: prepare_metadata()
  → _pending_block_ref non-nil, _pending_meta_ref non-nil
  → 直接返回 True（不做任何新操作）

Phase 3: ray.wait(meta_refs, fetch_local=True, timeout=0.0)
  → 如果 meta 重建还没完成 → not_ready → continue
  → 如果 meta 重建完成 → ready

Phase 4: ray.get(meta_ref, timeout=0)
  → 成功返回 meta_with_schema
  → complete_with_metadata(meta_with_schema)
    → RefBundle([(_pending_block_ref, meta)]) → block 被正常消费 ✅
    → _pending_block_ref = nil, _pending_meta_ref = nil
```

**结论**:
- ✅ **Block 数据不会丢失** — `_pending_refs` 未被清理，下次循环会重试
- ⚠️ **错误计数假阳性** — `errored_blocks_per_op` 多计了一次，
  误消耗 `max_errored_blocks` 配额
- ⚠️ **block_ref 的值可能仍然丢失** — `complete_with_metadata` 消费了 meta，
  但 `RefBundle` 中的 `block_ref` 指向的对象可能丢失，下游 operator
  `ray.get(block_ref)` 时才会检测到

**当 `should_ignore = False`（默认 max_errored_blocks=0）**:

```
→ raise e from None
→ 整个 Dataset 执行中止
→ 所有 block 丢失（不仅仅是这一个）
```

### 6.4 Block_ref 丢失的延迟检测

即使 meta_ref 重建成功、block 被"正常消费"，`RefBundle` 中包含的
`block_ref` 可能仍然指向丢失的对象：

```python
# physical_operator.py:355-361
self._output_ready_callback(
    RefBundle(
        [(self._pending_block_ref, meta)],  # ← block_ref 可能指向丢失的对象
        owns_blocks=True,
        schema=meta_with_schema.schema,
    ),
)
```

这个 `RefBundle` 传给下游 operator 后，下游 `ray.get(block_ref)` 时
才会发现对象丢失（第二层延迟检测）：

```
block_ref 对象丢失
  → meta_ref 重建成功，block 被"正常消费"
  → RefBundle 传给下游 operator
  → 下游 ray.get(block_ref)
    → 如果 Ray Core 重建成功 → 正常 ✅
    → 如果 Ray Core 重建失败 → 抛 ObjectLostError
      → 在下游 operator 的 task 执行中抛异常
      → 最终到达 process_completed_tasks 的 except
```

---

## 附录：异常类型与传播路径

| C++ Status/错误 | Python 异常 | 触发条件 | 在 ray.get 还是 ray.wait 中出现 |
|---|---|---|---|
| `Status::TimedOut` | `GetTimeoutError` | 对象在超时时间内未可用 | ray.get |
| `OBJECT_LOST` | `ObjectLostError` | 对象丢失且无法重建 | ray.get（通过内存存储中的错误对象） |
| `OBJECT_RECONSTRUCTION_FAILED` | `ObjectReconstructionFailedError` | 重建失败 | ray.get |
| `OWNER_DIED` | `OwnerDiedError` | 对象 Owner 进程死亡 | ray.get |
| `IsInPlasmaError` | — | 对象在 plasma 中 | 内部标记，不抛异常 |
| — | — | ray.wait 超时 | 不抛异常，返回 not_ready 列表 |

**关键区别**:
- `ray.wait` **永远不会抛异常** — 超时只是导致部分对象在 not_ready 列表
- `ray.get` **可以抛多种异常** — 超时抛 `GetTimeoutError`，对象丢失抛 `ObjectLostError`
- `ray.wait` 中 `abort_if_any_object_is_exception=false`（异常对象也算 ready）
- `ray.get` 中 `abort_if_any_object_is_exception=true`（异常对象导致立即返回）
