# ObjectFetchTimedOutError 完整分析：从异常触发到跨节点对象拉取

## 文档信息

| 项目 | 内容 |
|------|------|
| 标题 | ObjectFetchTimedOutError 异常触发、血缘重建、跨节点对象拉取机制完整分析 |
| 基于 Ray 版本 | 2.52.1+ |
| 触发场景 | Ray Data Streaming Executor 中 MapWorker task 输入参数获取超时 |
| 关联文档 | Object生命周期与恢复深度分析.md, IsInPlasmaError机制解析.md, ray.get数据获取链路.md |

---

## 目录

1. [报错堆栈与异常类继承](#1-报错堆栈与异常类继承)
2. [ray.get 与 auto_init_wrapper 的关联](#2-rayget-与-auto_init_wrapper-的关联)
3. [异常触发条件对比：ObjectFetchTimedOutError vs ObjectLostError](#3-异常触发条件对比)
4. [血缘重建机制](#4-血缘重建机制)
5. [pending_object_creation 保护机制](#5-pending_object_creation-保护机制)
6. [GetObjects 阻塞等待与超时机制](#6-getobjects-阻塞等待与超时机制)
7. ["At least one of the input arguments" 消息来源](#7-at-least-one-of-the-input-arguments-消息来源)
8. [Task 执行失败如何将异常写入 Object](#8-task-执行失败如何将异常写入-object)
9. [In-memory Store vs Plasma Store 跨节点行为](#9-in-memory-store-vs-plasma-store-跨节点行为)
10. [OBJECT_IN_PLASMA 标记写入时机](#10-object_in_plasma-标记写入时机)
11. [by-ref 参数判断机制](#11-by-ref-参数判断机制)
12. [跨节点对象拉取的四个位置](#12-跨节点对象拉取的四个位置)
13. [Owner Location 订阅机制](#13-owner-location-订阅机制)
14. [raylet 预取与 Worker Get 的关系](#14-raylet-预取与-worker-get-的关系)
15. [ObjectRef Owner 地址传递](#15-objectref-owner-地址传递)
16. ["等待参数就绪"保证机制](#16-等待参数就绪保证机制)

---

## 1. 报错堆栈与异常类继承

### 1.1 报错堆栈

```
Traceback (most recent call last):
  File "ray/data/_internal/execution/streaming_executor_state.py", line 471, in process_completed_tasks
    bytes_read = task.on_data_ready(
  File "ray/data/_internal/execution/interfaces/physical_operator.py", line 242, in on_data_ready
    raise ex from None
  File "ray/data/_internal/execution/interfaces/physical_operator.py", line 232, in on_data_ready
    ray.get(self._pending_block_ref)
  File "ray/_private/auto_init_hook.py", line 22, in auto_init_wrapper
    return fn(*args, **kwargs)
  File "ray/_private/client_mode_hook.py", line 107, in wrapper
    return func(*args, **kwargs)
  File "ray/_private/worker.py", line 3008, in get
    values, debugger_breakpoint = worker.get_objects(
  File "ray/_private/worker.py", line 1023, in get_objects
    raise value.as_instanceof_cause()
ray.exceptions.RayTaskError(ObjectFetchTimedOutError): 
  ray::MapWorker(MapBatches(KafkaFailedSink)).submit() (pid=351, ip=10.251.108.190, ...)
  At least one of the input arguments for this task could not be computed:
ray.exceptions.ObjectFetchTimedOutError: Failed to retrieve object 42081b678a4f383d...
```

### 1.2 异常类继承关系

```
RayError → ObjectLostError → ObjectFetchTimedOutError
```

定义在 `python/ray/exceptions.py:672`：

```python
@PublicAPI
class ObjectFetchTimedOutError(ObjectLostError):
    """Indicates that an object fetch timed out."""

    def __str__(self):
        return (
            self._base_str()
            + "\n\n"
            + (
                f"Fetch for object {self.object_ref_hex} timed out because no "
                "locations were found for the object. This may indicate a "
                "system-level bug."
            )
        )
```

`ObjectLostError` 的定义 (`exceptions.py:631`)：

```python
@PublicAPI
class ObjectLostError(RayError):
    def __init__(self, object_ref_hex, owner_address, call_site):
        self.object_ref_hex = object_ref_hex
        self.owner_address = owner_address
        self.call_site = call_site.replace(
            ray_constants.CALL_STACK_LINE_DELIMITER, "\n  "
        )

    def _base_str(self):
        msg = f"Failed to retrieve object {self.object_ref_hex}. "
        if self.call_site:
            msg += f"The ObjectRef was created at: {self.call_site}"
        else:
            msg += (
                "To see information about where this ObjectRef was created "
                "in Python, set the environment variable "
                "RAY_record_ref_creation_sites=1 during `ray start` and "
                "`ray.init()`."
            )
        return msg
```

### 1.3 异常如何被抛出（完整链路）

```
C++ pull_manager.cc → C++ core_worker → Python get_objects → 反序列化构造异常 → raise
```

**Step 1 — C++ 层触发超时** (`src/ray/object_manager/pull_manager.cc:495-508`)：

```cpp
void PullManager::TryToMakeObjectLocal(const ObjectID &object_id) {
    // ... 尝试从远程节点拉取 ...
    bool did_pull = PullFromRandomLocation(object_id);
    if (did_pull) { UpdateRetryTimer(request, object_id); return; }
    // ... 尝试从 spill 存储恢复 ...

    RAY_CHECK(!request.pending_object_creation);
    if (request.expiration_time_seconds == 0) {
        // 首次发现对象无 location，设置超时起点
        request.expiration_time_seconds =
            get_time_seconds_() +
            RayConfig::instance().fetch_fail_timeout_milliseconds() / 1e3;
        // ↑ fetch_fail_timeout_milliseconds 默认 600000ms = 10 分钟
    } else if (get_time_seconds_() > request.expiration_time_seconds) {
        // 超时，正式失败
        fail_pull_request_(object_id, rpc::ErrorType::OBJECT_FETCH_TIMED_OUT);
        request.expiration_time_seconds = 0;
    }
}
```

**Step 2 — raylet 将错误写入 plasma** (`src/ray/raylet/node_manager.cc:2251`)：

```cpp
void NodeManager::MarkObjectsAsFailed(const ErrorType &error_type, ...) {
    const std::string meta = std::to_string(static_cast<int>(error_type));
    // 创建 0 字节数据 + 错误类型作为元数据的 plasma 对象
    store_client_->TryCreateImmediately(
        object_id, ref.owner_address(), 0,
        reinterpret_cast<const uint8_t *>(meta.c_str()), meta.length(),
        &data, plasma::flatbuf::ObjectSource::ErrorStoredByRaylet);
    store_client_->Seal(object_id);
}
```

**Step 3 — Python 反序列化构造异常** (`python/ray/_private/serialization.py:485`)：

```python
elif error_type == ErrorType.Value("OBJECT_FETCH_TIMED_OUT"):
    return ObjectFetchTimedOutError(
        object_ref.hex(), object_ref.owner_address(), object_ref.call_site()
    )
```

**Step 4 — get_objects 抛出异常** (`python/ray/_private/worker.py:1011`)：

```python
if not return_exceptions:
    for value in values:
        if isinstance(value, RayError):
            if isinstance(value, RayTaskError):
                raise value.as_instanceof_cause()
            else:
                raise value  # ← ObjectFetchTimedOutError 走这里
```

`ObjectFetchTimedOutError` 是 `RayError` 但不是 `RayTaskError`，所以走 `else: raise value` 分支直接抛出，不经过 `as_instanceof_cause()`。

### 1.4 是否会触发重试

**不会。整条链路没有任何重试机制。**

| 层级 | 是否有重试 | 说明 |
|------|-----------|------|
| C++ pull_manager | ❌ | 10 分钟超时后直接 `fail_pull_request` |
| `core_worker.get_objects` | ❌ | 单次调用返回结果 |
| `ray.get` / `get_objects` | ❌ | 拿到 `RayError` 直接 `raise` |
| `on_data_ready` | ❌ | 只 catch `GetTimeoutError`（短超时） |
| `process_completed_tasks` | ⚠️ | 根据 `max_errored_blocks` 预算决定忽略还是中止 |

---

## 2. ray.get 与 auto_init_wrapper 的关联

### 2.1 两层装饰器嵌套

```
ray.get (最终被替换后的版本)
  │
  ├─ 第 1 层: auto_init_wrapper   ← auto_init_hook.py
  │      调用 auto_init_ray()，确保 ray 已初始化
  │      然后调用 fn(*args, **kwargs) → 即第 2 层装饰器
  │
  ├─ 第 2 层: client_mode_hook 的 wrapper  ← client_mode_hook.py
  │      检查是否处于 client mode
  │      如果是 → 走 ray client 的 get
  │      如果否 → 调用 func(*args, **kwargs) → 即原始 get 函数
  │
  └─ 第 3 层: 原始 get 函数  ← worker.py:2895
         调用 worker.get_objects(...)
         → core_worker.get_objects(...)
         → 反序列化 → raise ObjectFetchTimedOutError
```

### 2.2 装饰器叠加顺序

```python
# 步骤 1: @client_mode_hook 装饰 get (worker.py:2894)
get_with_client_hook = client_mode_hook(get_original)

# 步骤 2: wrap_auto_init 包装上一步的结果 (__init__.py:247)
ray.get = wrap_auto_init(get_with_client_hook)

# 最终 ray.get 实际是:
# auto_init_wrapper → client_mode_wrapper → get_original
```

### 2.3 auto_init_wrapper 实现

`python/ray/_private/auto_init_hook.py:18`：

```python
def wrap_auto_init(fn):
    @wraps(fn)
    def auto_init_wrapper(*args, **kwargs):
        auto_init_ray()              # 确保 ray 已初始化
        return fn(*args, **kwargs)   # fn 是 client_mode_hook 装饰过的版本
    return auto_init_wrapper
```

### 2.4 client_mode_hook wrapper 实现

`python/ray/_private/client_mode_hook.py:96`：

```python
def client_mode_hook(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if client_mode_should_convert():
            if func.__name__ != "init" or is_client_mode_enabled_by_default:
                return getattr(ray, func.__name__)(*args, **kwargs)
        return func(*args, **kwargs)  # 非客户端模式 → 调用原始函数
    return wrapper
```

**关键：** 两层装饰器都是透明透传异常的，`ObjectFetchTimedOutError` 从最内层 `get_objects` 一路抛到 `on_data_ready`，中间没有任何拦截。

### 2.5 运行时调用流程

```
ray.get(self._pending_block_ref)                         # physical_operator.py:232
  │
  # 外层: auto_init_wrapper (auto_init_hook.py:22)
  ├→ auto_init_ray()                                     # 确保 ray 已初始化
  ├→ fn(...)  # fn = client_mode_wrapper
  │
  # 中层: client_mode_hook wrapper (client_mode_hook.py:107)
  ├→ client_mode_should_convert() → False (非 client mode)
  ├→ func(*args, **kwargs)  # func = get_original
  │
  # 内层: 原始 get (worker.py:2895)
  ├→ worker.get_objects(object_refs, timeout, ...)       # worker.py:3007
  │    ├→ core_worker.get_objects(...)                   # C++ 调用
  │    ├→ deserialize_objects(...)                        # 反序列化
  │    └→ raise value                                     # worker.py:1023
  │
  └→ 异常向上传播，穿过所有装饰器层
```

---

## 3. 异常触发条件对比

### 3.1 ErrorType 枚举全表

定义在 `src/ray/protobuf/common.proto`：

```proto
enum ErrorType {
  WORKER_DIED = 0;
  ACTOR_DIED = 1;
  TASK_EXECUTION_EXCEPTION = 3;
  OBJECT_IN_PLASMA = 4;
  TASK_CANCELLED = 5;
  ACTOR_CREATION_FAILED = 6;
  RUNTIME_ENV_SETUP_FAILED = 7;
  OBJECT_LOST = 8;
  OWNER_DIED = 9;
  OBJECT_DELETED = 10;
  DEPENDENCY_RESOLUTION_FAILED = 11;
  OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED = 12;
  OBJECT_UNRECONSTRUCTABLE_LINEAGE_EVICTED = 13;
  OBJECT_FETCH_TIMED_OUT = 14;
  LOCAL_RAYLET_DIED = 15;
  TASK_PLACEMENT_GROUP_REMOVED = 16;
  ACTOR_PLACEMENT_GROUP_REMOVED = 17;
  TASK_UNSCHEDULABLE_ERROR = 18;
  ACTOR_UNSCHEDULABLE_ERROR = 19;
  OUT_OF_DISK_ERROR = 20;
  OBJECT_FREED = 21;
  OUT_OF_MEMORY = 22;
  NODE_DIED = 23;
  END_OF_STREAMING_GENERATOR = 24;
  ACTOR_UNAVAILABLE = 25;
  GENERATOR_TASK_FAILED_FOR_OBJECT_RECONSTRUCTION = 26;
  OBJECT_UNRECONSTRUCTABLE_PUT = 27;
  OBJECT_UNRECONSTRUCTABLE_RETRIES_DISABLED = 28;
  OBJECT_UNRECONSTRUCTABLE_BORROWED = 29;
  OBJECT_UNRECONSTRUCTABLE_REF_NOT_FOUND = 30;
  OBJECT_UNRECONSTRUCTABLE_TASK_CANCELLED = 31;
  OBJECT_UNRECONSTRUCTABLE_LINEAGE_DISABLED = 32;
  WORKER_STARTUP_FAILED = 33;
}
```

### 3.2 PullManager 中的两个 fail_pull_request 调用

**Call Site 1: OBJECT_FETCH_TIMED_OUT** (`pull_manager.cc:506`)：

```cpp
// 对象无 location，非 pending_creation，超过 10 分钟
fail_pull_request_(object_id, rpc::ErrorType::OBJECT_FETCH_TIMED_OUT);
```

**Call Site 2: OUT_OF_DISK_ERROR** (`pull_manager.cc:802`)：

```cpp
// 对象正在被拉取但磁盘空间不足
fail_pull_request_(object_id, rpc::ErrorType::OUT_OF_DISK_ERROR);
```

**OBJECT_LOST 从不由 PullManager 触发**，它只在 `ObjectRecoveryManager::ReconstructObject` 中作为血缘重建不合格的兜底错误。

### 3.3 对比总结

| 错误类型 | 触发位置 | 根本原因 | 是否尝试过重建 |
|----------|---------|---------|--------------|
| `OBJECT_FETCH_TIMED_OUT` | PullManager (raylet) | 对象 10 分钟内找不到 location，且 owner 没有在重建 | ❌ owner 侧未触发重建 |
| `OBJECT_LOST` | ObjectRecoveryManager (owner) | 重建时血缘不合格，枚举 fallback | ✅ 尝试了但不符合条件 |
| `OBJECT_UNRECONSTRUCTABLE_*` | ObjectRecoveryManager (owner) | 重建时各种具体原因 | ✅ 尝试了但失败 |
| `OWNER_DIED` | borrower 侧 | owner 进程死亡 | ❌ 无法联系 owner |

### 3.4 Python ErrorType → 异常映射

`python/ray/_private/serialization.py` 中的完整映射：

```python
if error_type == ErrorType.Value("TASK_EXECUTION_EXCEPTION"):
    obj = self._deserialize_msgpack_data(data, metadata_fields)
    return RayError.from_bytes(obj)
elif error_type == ErrorType.Value("WORKER_DIED"):
    return WorkerCrashedError()
elif error_type == ErrorType.Value("OBJECT_LOST"):
    return ObjectLostError(object_ref.hex(), owner_address, call_site)
elif error_type == ErrorType.Value("OBJECT_FETCH_TIMED_OUT"):
    return ObjectFetchTimedOutError(object_ref.hex(), owner_address, call_site)
elif error_type == ErrorType.Value("OUT_OF_DISK_ERROR"):
    return OutOfDiskError(object_ref.hex(), owner_address, call_site)
# ... 其他错误类型 ...
elif ErrorType.Name(error_type).startswith("OBJECT_UNRECONSTRUCTABLE_"):
    return ObjectReconstructionFailedError(object_ref.hex(), reason=error_type, ...)
```

Python 异常继承关系：

```
RayError
  |-- ObjectLostError (OBJECT_LOST = 8)
       |-- ObjectFetchTimedOutError (OBJECT_FETCH_TIMED_OUT = 14)
       |-- ReferenceCountingAssertionError (OBJECT_DELETED = 10)
       |-- ObjectFreedError (OBJECT_FREED = 21)
       |-- OwnerDiedError (OWNER_DIED = 9)
       |-- ObjectReconstructionFailedError (all OBJECT_UNRECONSTRUCTABLE_* types)
```

---

## 4. 血缘重建机制

### 4.1 重建触发时机

**重建不是由 `get_objects` 触发的**，而是由 Owner 侧的定时器异步触发：

```cpp
// core_worker.cc:471-490  Owner 侧每 100ms 定时器
periodical_runner_->RunFnPeriodically(
    [this] {
        const auto lost_objects = reference_counter_->FlushObjectsToRecover();
        if (!lost_objects.empty()) {
            RAY_LOG(ERROR) << ":info_message: Attempting to recover " 
                         << lost_objects.size() 
                         << " lost objects by resubmitting their tasks...";
            memory_store_->Delete(lost_objects);
            for (const auto &object_id : lost_objects) {
                RAY_UNUSED(object_recovery_manager_->RecoverObject(object_id));
            }
        }
    },
    100,  // ← 每 100ms
    "CoreWorker.RecoverObjects");
```

### 4.2 丢失对象如何被检测到

`src/ray/core_worker/reference_counter.cc` 中，对象在以下场景被加入 `objects_to_recover_`：

1. **节点宕机** (`ResetObjectsOnRemovedNode`)：GCS 检测到节点 Dead，通知 Owner，Owner 将该节点上 pinned 的对象加入恢复列表
2. **对象 spilled 到已死节点**：spilled_node_id 被检测为 Dead
3. **UpdateObjectPinnedAtRaylet**：尝试更新 pinned location 但新节点已 Dead

### 4.3 ObjectRecoveryManager::RecoverObject 算法

```
RecoverObject(object_id):
  1. 如果是 actor creation task → return（GCS 管理 actor 重启）
  2. 检查引用计数器：IsPlasmaObjectPinnedOrSpilled
     - 如果 ref 不存在 → return OBJECT_UNRECONSTRUCTABLE_REF_NOT_FOUND
  3. 如果不是 owner → return OBJECT_UNRECONSTRUCTABLE_BORROWED
  4. 如果需要恢复（无 pinned location 且未 spilled）:
     - 查询全局对象 location
  5. 如果已有 pinned/spilled location → 放入 OBJECT_IN_PLASMA 标记
     (恢复不需要，对象仍然存在)

PinOrReconstructObject(object_id, locations):
  - 如果有 location → 尝试 pin 一份 (PinExistingObjectCopy)
  - 如果无 location → ReconstructObject(object_id)

ReconstructObject(object_id):
  1. 检查血缘重建资格 (GetLineageReconstructionEligibility):
     - INELIGIBLE_PUT → 创建于 ray.put()，无线索
     - INELIGIBLE_NO_RETRIES → max_retries=0
     - INELIGIBLE_LINEAGE_EVICTED → 血缘被驱逐
     - INELIGIBLE_LINEAGE_DISABLED → lineage_pinning_enabled=false
     - INELIGIBLE_REF_NOT_FOUND → 引用不存在
     - ELIGIBLE → 可以重建
  2. 如果 ELIGIBLE:
     - UpdateObjectPendingCreation(object_id, true)  ← 设置 pending_creation=true
     - task_manager_.ResubmitTask(task_id, &task_deps)
     - 递归恢复 task 的依赖
  3. 如果 INELIGIBLE:
     - recovery_failure_callback → 放入对应错误对象到 in-memory store
```

### 4.4 LineageReconstructionEligibility 枚举

`src/ray/core_worker/reference_counter_interface.h`：

```cpp
enum class LineageReconstructionEligibility {
  ELIGIBLE,                       // 可以尝试重建
  INELIGIBLE_PUT,                 // ray.put() 创建，无线索
  INELIGIBLE_NO_RETRIES,          // max_retries=0
  INELIGIBLE_LINEAGE_EVICTED,     // 血缘被驱逐
  INELIGIBLE_LINEAGE_DISABLED,    // lineage_pinning_enabled=false
  INELIGIBLE_REF_NOT_FOUND,       // 引用不存在
};

inline std::optional<rpc::ErrorType> ToErrorType(
    LineageReconstructionEligibility eligibility) {
  switch (eligibility) {
  case ELIGIBLE: return std::nullopt;
  case INELIGIBLE_PUT: return rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE_PUT;
  case INELIGIBLE_NO_RETRIES: return rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE_RETRIES_DISABLED;
  case INELIGIBLE_LINEAGE_EVICTED: return rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE_LINEAGE_EVICTED;
  case INELIGIBLE_LINEAGE_DISABLED: return rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE_LINEAGE_DISABLED;
  case INELIGIBLE_REF_NOT_FOUND: return rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE_REF_NOT_FOUND;
  }
  return rpc::ErrorType::OBJECT_LOST;  // ← 兜底 fallback
}
```

### 4.5 关键结论

`get_objects` 本身**不触发重建**，它只是等待：
- 等到对象被拉取到本地 plasma
- 等到 Owner 重建成功后新对象可用
- 等到某个错误对象被写入（重建失败/超时等）

你的报错说明 Owner 侧**没有在重建**这个对象——可能是因为 lineage 不 eligible，或者根本没检测到对象丢失。

**排查方法：** 在 Owner 的 Ray 日志中搜索：
```bash
grep "Attempting to recover" *.log        # 是否触发了重建
grep "OBJECT_UNRECONSTRUCTABLE" *.log     # 重建是否失败
grep "42081b678a4f383d" *.log            # 该对象 ID 的所有日志
```

---

## 5. pending_object_creation 保护机制

### 5.1 核心机制

`pending_creation_` 在两个时点被设置：

```
task 提交时 → UpdateSubmittedTaskReferences → pending_creation_ = true
                                                    ↓ PushToLocationSubscribers
                                                    ↓ raylet PullManager 收到
                                                    ↓ pending_object_creation = true
                                                    ↓ 不会进入超时逻辑，继续等

task 完成时 → UpdateFinishedTaskReferences → pending_creation_ = false
                                                    ↓ PushToLocationSubscribers
                                                    ↓ raylet PullManager 收到
                                                    ↓ pending_object_creation = false
                                                    ↓ 可以进入超时逻辑
```

重建场景也一样：`ObjectRecoveryManager::ReconstructObject` 调用 `UpdateObjectPendingCreation(object_id, true)`。

### 5.2 代码实现

`src/ray/core_worker/reference_counter.cc`：

```cpp
void ReferenceCounter::UpdateObjectPendingCreationInternal(const ObjectID &object_id,
                                                           bool pending_creation) {
    auto it = object_id_refs_.find(object_id);
    if (it != object_id_refs_.end()) {
        push = (it->second.pending_creation_ != pending_creation);
        it->second.pending_creation_ = pending_creation;
    }
    if (push) {
        PushToLocationSubscribers(it);  // 发布到订阅者
    }
}
```

### 5.3 PullManager 中的保护

`src/ray/object_manager/pull_manager.h`：

```cpp
// An object is pullable if we know the size and it's not pending
// creation due to object reconstruction.
bool IsPullable() const { return object_size_set && !pending_object_creation; }
```

`pull_manager.cc:495`：

```cpp
void PullManager::TryToMakeObjectLocal(const ObjectID &object_id) {
    // ...
    RAY_CHECK(!request.pending_object_creation);  // ← 如果 pending=true，不会到这里
    if (request.expiration_time_seconds == 0) {
        // 设置 10 分钟超时起点
    } else if (get_time_seconds_() > request.expiration_time_seconds) {
        // 超时失败
        fail_pull_request_(object_id, rpc::ErrorType::OBJECT_FETCH_TIMED_OUT);
    }
}
```

**所以 `pending_object_creation = true` 意味着：**
- 原始 task 还在运行（还没产出对象）
- 或者对象丢失后正在被重建（task 被重新提交）

两种情况下 PullManager 都**不会触发 OBJECT_FETCH_TIMED_OUT**。

---

## 6. GetObjects 阻塞等待与超时机制

### 6.1 In-Memory Store 的 Get 机制

`src/ray/core_worker/store_provider/memory_store/memory_store.cc`：

```cpp
Status CoreWorkerMemoryStore::GetImpl(...) {
    // 先检查 objects_ map 中是否已有
    for (size_t i = 0; i < object_ids.size(); i++) {
        auto iter = objects_.find(object_id);
        if (iter != objects_.end()) {
            (*results)[i] = iter->second;  // 已有 → 直接返回
            num_found += 1;
        } else {
            remaining_ids.insert(object_id);  // 没有 → 需要等待
        }
    }

    if (remaining_ids.empty() || num_found >= num_objects) {
        return Status::OK();  // 全部找到
    }

    // 创建 GetRequest 并注册到 object_get_requests_
    get_request = std::make_shared<GetRequest>(remaining_ids, ...);
    for (const auto &object_id : get_request->ObjectIds()) {
        object_get_requests_[object_id].push_back(get_request);
    }

    // 阻塞等待
    while (!timed_out && !(done = get_request->Wait(iteration_timeout))) {
        // 检查信号、更新剩余超时
    }
}
```

### 6.2 GetRequest::Wait — condition_variable 机制

```cpp
bool GetRequest::Wait(int64_t timeout_ms) {
    if (timeout_ms == -1) {
        // 无限等待
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return is_ready_; });
        return true;
    }
    // 有限等待
    std::unique_lock<std::mutex> lock(mutex_);
    auto is_ready_status = cv_.wait_for(
        lock, std::chrono::milliseconds(timeout_ms), [this]() { return is_ready_; });
    return is_ready_status;
}
```

### 6.3 Put 如何唤醒 Get

```cpp
void CoreWorkerMemoryStore::Put(const RayObject &object, const ObjectID &object_id, ...) {
    absl::MutexLock lock(&mu_);
    // 存储对象
    objects_[object_id] = object_entry;
    // 通知等待的 GetRequest
    auto object_request_iter = object_get_requests_.find(object_id);
    if (object_request_iter != object_get_requests_.end()) {
        for (auto &get_request : object_request_iter->second) {
            get_request->Set(object_id, object_entry);
            //   ↓ GetRequest::Set
            //   → is_ready_ = true
            //   → cv_.notify_all()  ← 唤醒阻塞的 Get
        }
    }
}
```

### 6.4 超时值取决于调用方

| 调用场景 | timeout_ms | 行为 |
|----------|-----------|------|
| `ray.get(ref)` (无 timeout) | -1 | 无限等待，每 1s 检查信号 |
| `ray.get(ref, timeout=30)` | 30000 | 等待 30s，超时返回 `GetTimeoutError` |
| Task 参数获取 | -1 | 无限等待（硬编码） |
| Plasma store 轮询 | batch_timeout | 10 * batch_ids.size() ms |

**关键配置** (`ray_config_def.h`)：

```cpp
RAY_CONFIG(int64_t, fetch_fail_timeout_milliseconds, 600000)   // 10 分钟，PullManager 超时
RAY_CONFIG(int64_t, fetch_warn_timeout_milliseconds, 60000)    // 1 分钟，警告
RAY_CONFIG(int64_t, get_check_signal_interval_milliseconds, 1000) // 1s，信号检查间隔
RAY_CONFIG(int64_t, object_timeout_milliseconds, 100)           // 100ms，初始等待
RAY_CONFIG(int, object_manager_pull_timeout_ms, 10000)          // 10s，pull 重试间隔
RAY_CONFIG(int, object_manager_timer_freq_ms, 100)              // 100ms，raylet 定时器
```

---

## 7. "At least one of the input arguments" 消息来源

### 7.1 生成过程

当 `MapWorker.submit()` task 在 Worker 上执行时，需要先获取输入参数。获取参数的流程：

```python
# _raylet.pyx:1807-1840  Worker 侧 task 执行

with core_worker.profile_event(b"task:deserialize_arguments"):
    # 反序列化参数（C++ 层先拉取了参数对象）
    args = worker.deserialize_objects(metadata_pairs, object_refs)

# 检查每个参数是否是错误对象
for arg in args:
    raise_if_dependency_failed(arg)   # ← 这里检查每个参数
```

`raise_if_dependency_failed` (`_raylet.pyx:891`)：

```python
cdef raise_if_dependency_failed(arg):
    """This method is used to improve the readability of backtrace."""
    if isinstance(arg, RayError):   # 如果参数是错误对象（不是正常数据）
        raise arg                      # 直接抛出
```

### 7.2 traceback 美化

`exceptions.py:318`：

```python
if "ray._raylet.raise_if_dependency_failed" in line:
    # 检测到调用栈中有 raise_if_dependency_failed
    # 替换为用户友好消息
    out.append(
        "  At least one of the input arguments for "
        "this task could not be computed:"
    )
```

### 7.3 完整场景

```
1. MapWorker.submit() task 被调度到 Worker 上执行
   → Worker 侧 GetTaskArguments()
   → plasma_store_provider_->Get(ids, owners, timeout=-1)
   → 对象不在本地 plasma → 请求 raylet PullManager 拉取

2. PullManager 找不到 location → 等 10 分钟 → OBJECT_FETCH_TIMED_OUT
   → 错误对象写入 plasma

3. Worker 侧 GetObjects 拿到错误对象
   → 反序列化为 ObjectFetchTimedOutError
   → raise_if_dependency_failed(arg) 抛出
   → "At least one of the input arguments for this task could not be computed"
   → task 失败，RayTaskError 存为返回值

4. Driver 侧 process_completed_tasks → on_data_ready
   → ray.get(self._pending_block_ref)
   → 拿到 RayTaskError → as_instanceof_cause()
   → 抛出 RayTaskError(ObjectFetchTimedOutError)
```

---

## 8. Task 执行失败如何将异常写入 Object

### 8.1 Worker 侧：用户代码异常捕获

```python
# _raylet.pyx:1750-2050  Worker 侧 task 执行主逻辑

try:
    outputs = function_executor(*args, **kwargs)    # 用户函数执行
    task_exception = False

except BaseException as e:
    is_retryable_error[0] = determine_if_retryable(...)
    task_exception_instance = e
finally:
    if task_exception_instance is not None:
        raise task_exception_instance    # 重新抛出

# 外层 except
except BaseException as e:
    num_errors_stored = store_task_errors(
        worker, e, task_exception, actor, actor_id, function_name,
        task_type, title, caller_address, returns,
        application_error, c_tensor_transport)
```

### 8.2 store_task_errors — 创建 RayTaskError

`_raylet.pyx:966`：

```python
cdef store_task_errors(worker, exc, ...):
    # 将异常包装为 RayTaskError
    if isinstance(exc, RayTaskError):
        failure_object = RayTaskError(function_name, backtrace,
                                      exc.cause, ...)     # 避免嵌套
    else:
        failure_object = RayTaskError(function_name, backtrace,
                                      exc, ...)            # ← 原始异常被包装

    # 为每个返回值创建一个 error 对象
    errors = [failure_object for _ in range(returns[0].size())]

    # 序列化并存储到返回对象中
    num_errors_stored = core_worker.store_task_outputs(
        worker, errors, caller_address, returns, ...)
```

### 8.3 序列化 RayTaskError

`serialization.py:617`：

```python
def _serialize_to_msgpack(self, value):
    if isinstance(value, RayTaskError):
        if issubclass(value.cause.__class__, TaskCancelledError):
            metadata = str(ErrorType.Value("TASK_CANCELLED")).encode("ascii")
        else:
            metadata = str(ErrorType.Value("TASK_EXECUTION_EXCEPTION")).encode("ascii")
        value = value.to_bytes()   # pickle 序列化 RayTaskError
    # → metadata = "3" (TASK_EXECUTION_EXCEPTION 枚举值)
    # → data = pickled RayTaskError bytes
```

### 8.4 Owner 侧接收返回值

```cpp
// normal_task_submitter.cc:545  PushNormalTask 回调
client->PushNormalTask(std::move(request),
    [this, task_spec, task_id](Status status, const rpc::PushTaskReply &reply) {
        if (status.ok()) {
            if (!task_spec.GetMessage().retry_exceptions() ||
                !reply.is_retryable_error() ||
                !task_manager_.RetryTaskIfPossible(...)) {
                task_manager_.CompletePendingTask(
                    task_id, reply, addr, reply.is_application_error());
            }
        }
    });
```

```cpp
// task_manager.cc:908  CompletePendingTask
void TaskManager::CompletePendingTask(...) {
    for (const auto &return_object : reply.return_objects()) {
        HandleTaskReturn(object_id, return_object, ...);
        // ↑ 将 Worker 序列化的 RayTaskError 写入 in-memory store
    }
}
```

### 8.5 HandleTaskReturn — 写入 in-memory store

```cpp
// task_manager.cc:550
StatusOr<bool> TaskManager::HandleTaskReturn(const ObjectID &object_id,
                                             const rpc::ReturnObject &return_object,
                                             const NodeID &worker_node_id,
                                             bool store_in_plasma) {
    if (return_object.in_plasma()) {
        // 大对象在 plasma 中
        reference_counter_.UpdateObjectPinnedAtRaylet(object_id, worker_node_id);
        in_memory_store_.Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
                             object_id, ...);
    } else {
        // 直接返回（in-memory）— 包含序列化的 RayTaskError
        RayObject object(data_buffer, metadata_buffer, ...);
        in_memory_store_.Put(object, object_id, ...);
        // ↑ Put 操作会通知所有等待的 GetRequest
        //   GetRequest->Set() → cv_.notify_all() → ray.get 被唤醒
    }
}
```

### 8.6 三条失败路径

| 路径 | 触发 | 异常写入方式 |
|------|------|-------------|
| 用户代码异常 | `function_executor` 抛出 | `store_task_errors` → 序列化 RayTaskError → RPC 回复 → HandleTaskReturn → in-memory store |
| 系统级失败 | Worker 崩溃，RPC 失败 | `FailPendingTask` → `MarkTaskReturnObjectsFailed` → 构造 RayObject(error_type) → 写入 plasma/in-memory |
| Streaming Generator 失败 | Generator yield 时抛出 | `report_streaming_generator_exception` → 逐 item RPC 报告 |

---

## 9. In-memory Store vs Plasma Store 跨节点行为

### 9.1 两套存储系统

```
┌──────────────────────────────────────────────────────────────────────┐
│ 1. In-Memory Store (进程级，不跨节点)                                  │
│    每个 Worker 进程独立持有，不共享                                     │
│    Put → 存在当前进程内存中                                            │
│    Get → 只能从当前进程取，不能跨节点                                   │
│    用于小对象的 direct return（通过 RPC 直接传回）                       │
└──────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────┐
│ 2. Plasma Store (节点级，跨节点)                                       │
│    每个节点一个 plasma store，通过 Unix socket 本地访问                 │
│    Put → 写入当前节点的 plasma（shared memory）                        │
│    Get → 先查本地 plasma，没有则通过 raylet 跨节点拉取                  │
│    用于大对象（超过 inline 阈值的对象）                                 │
└──────────────────────────────────────────────────────────────────────┘
```

### 9.2 In-Memory Store 代码证据

```cpp
// memory_store.cc:172 — 纯进程内 hash_map，没有任何网络逻辑
void CoreWorkerMemoryStore::Put(const RayObject &object,
                                const ObjectID &object_id, ...) {
    absl::MutexLock lock(&mu_);
    objects_[object_id] = object_entry;  // ← 进程内 map，不共享
    // 只通知同进程内的 GetRequest
    for (auto &get_request : object_get_requests_[object_id]) {
        get_request->Set(object_id, object_entry);
    }
}
```

### 9.3 Plasma Store Put — 只写本地

```cpp
// plasma_store_provider.cc:98
Status CoreWorkerPlasmaStoreProvider::Put(const RayObject &object, ...) {
    // store_client_ 连接的是本地 plasma 的 Unix socket
    RAY_RETURN_NOT_OK(Create(object.GetMetadata(), ..., &data, ...));
    if (object.HasData()) {
        memcpy(data->Data(), object.GetData()->Data(), ...);  // ← 写入本地 plasma
    }
    RAY_RETURN_NOT_OK(Seal(object_id));
    return Status::OK();
}
```

### 9.4 Plasma Store Get — 跨节点拉取

```cpp
// plasma_store_provider.cc:253
Status CoreWorkerPlasmaStoreProvider::Get(...) {
    // Step 1: 请求本地 raylet 从远程节点拉取
    raylet_ipc_client_->AsyncGetObjects(batch_ids, batch_owner_addresses, ...);

    // Step 2: 立即尝试本地 plasma（timeout=0）
    GetObjectsFromPlasmaStore(..., /*timeout_ms=*/0, ...);

    // Step 3: 循环轮询本地 plasma，等对象被拉取过来
    while (!remaining_object_id_to_idx.empty()) {
        GetObjectsFromPlasmaStore(..., batch_timeout, ...);
    }
}
```

### 9.5 跨节点拉取完整链路

```
Worker A (Node A)                     Raylet (Node A)                    Raylet (Node B)
     │                                     │                                  │
     │ Get(obj_id)                         │                                  │
     │ → plasma_store_provider_->Get()      │                                  │
     │── AsyncGetObjects ──────────────→  │                                  │
     │   (IPC to local raylet)             │ ObjectManager::Pull()            │
     │                                     │ → 订阅对象 location              │
     │                                     │ → SendPullRequest(NodeB)         │
     │                                     │── PullRequest RPC ────────────→ │
     │                                     │                                  │ HandlePull()
     │                                     │                                  │ → Push(obj_id, NodeA)
     │                                     │←── Push (对象数据分块) ──────────│
     │ ← 轮询本地 plasma                    │  ReceiveObjectChunk → 写入 plasma│
     │   store_client_->Get(obj_id)        │  → Seal 对象                      │
     │   → 找到了! → 返回数据               │                                  │
```

---

## 10. OBJECT_IN_PLASMA 标记写入时机

### 10.1 所有写入位置

| 位置 | 文件 | 触发场景 | 谁写 |
|------|------|---------|------|
| A | `core_worker.cc:1024` `PutInLocalPlasmaStore` | `ray.put()` 写入本地 plasma | Owner 自己 |
| B | `core_worker.cc:1128` `CreateOwnedAndIncrementLocalRef` | 创建 owned 对象但 plasma 中已存在 | Owner 自己 |
| C | `core_worker.cc:1220` `SealExisting` | seal 本地 plasma 对象（task 返回值） | 执行 Worker |
| D | `core_worker.cc:3279` `GetAndPinArgsForExecutor` | Worker 准备 task 的 by-ref 参数 | 执行 Worker |
| E | `core_worker.cc:4351` `HandleAssignObjectOwner` | borrower 请求 owner 分配所有权 | Owner |
| F | `task_manager.cc:567` `HandleTaskReturn` | Owner 收到 task 执行结果 | Owner |

### 10.2 三条一般场景写入路径

#### 路径 1：Task 参数 — GetAndPinArgsForExecutor（盲写）

```cpp
// core_worker.cc:3275
if (task.ArgByRef(i)) {
    reference_counter_->AddLocalReference(arg_id, ...);
    reference_counter_->AddBorrowedObject(arg_id, ObjectID::Nil(), 
                                           task.ArgRef(i).owner_address());
    // 直接写入 OBJECT_IN_PLASMA 标记（盲写！不管对象实际在哪）
    memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
                       task.ArgObjectId(i), ...);
}
```

**为什么可以盲写？** 因为 by-ref 参数经过依赖解析后，小对象已被 inline，只有 plasma 对象保持 by-ref。`ArgByRef(i) == true` 的参数一定在 plasma 中。

#### 路径 2：反序列化 ObjectRef — FutureResolver（通过 RPC 查询）

```
Worker A 反序列化发现 ObjectRef
  → RegisterOwnershipInfoAndResolveFuture
  → AddBorrowedObject（注册 owner 地址）
  → 如果序列化快照有对象数据 → ProcessResolvedObject（快速路径）
  → 否则 → ResolveFutureAsync → GetObjectStatus RPC → Owner
    → Owner 阻塞等待对象 available
    → PopulateObjectStatus 序列化对象状态
    → 回复 Worker A
    → ProcessResolvedObject 写入 in-memory store
```

**为什么不能盲写？** 反序列化的 ObjectRef 可能是：
- plasma 中的大对象 → 需要 OBJECT_IN_PLASMA 标记
- in-memory 中的小对象 → 需要写入实际数据
- 还没产出的对象 → 需要等 Owner 通知

#### 路径 3：Owner 收到 task 结果 — HandleTaskReturn

```cpp
// task_manager.cc:561
if (return_object.in_plasma()) {
    reference_counter_.UpdateObjectPinnedAtRaylet(object_id, worker_node_id);
    in_memory_store_.Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
                         object_id, ...);
} else {
    // 小对象 → 直接内联在 RPC 回复中 → 放实际数据
    in_memory_store_.Put(object, object_id, ...);
}
```

`in_plasma` 标志是 Worker 在 `SerializeReturnObject` 时设置的：

```cpp
// common.cc:59
void SerializeReturnObject(...) {
    if (return_object->GetData()->IsPlasmaBuffer()) {
        return_object_proto->set_in_plasma(true);   // ← 大对象在 plasma 中
    } else {
        return_object_proto->set_data(return_object->GetData()->Data(), ...);  // 小对象直接内联
    }
}
```

### 10.3 "RPC 把 in_plasma 传回来" 的含义

Owner 和 Worker 是两个不同的进程，各自有独立的 in-memory store。Worker 通过 gRPC 回复（`PushTaskReply` 或 `ReportGeneratorItemReturns`）携带 `in_plasma` 标志，Owner 收到后自己写入自己的 in-memory store。

### 10.4 两条独立的通知路径

Worker seal 对象到 plasma 后，有**两条独立的异步通知**到达 Owner：

| 路径 | 作用 | 是否写 in-memory store |
|------|------|------------------------|
| raylet → `UpdateObjectLocationBatch` RPC | 更新 reference counter 的 location 集合 | ❌ 不写 |
| Worker → `PushTaskReply` / `ReportGeneratorItemReturns` RPC | 写入 OBJECT_IN_PLASMA 标记 + 更新 pinned location | ✅ 写 |

---

## 11. by-ref 参数判断机制

### 11.1 ArgByRef 定义

`src/ray/common/task/task_spec.cc:265`：

```cpp
bool TaskSpecification::ArgByRef(size_t arg_index) const {
    // has_object_ref: 参数中包含 ObjectRef
    // !is_inlined: 对象数据没有被直接内联到 task spec 中
    return message_->args(arg_index).has_object_ref() &&
           !message_->args(arg_index).is_inlined();
}
```

### 11.2 依赖解析中的 inline 逻辑

`src/ray/core_worker/task_submission/dependency_resolver.cc:35`：

```cpp
void InlineDependencies(...) {
    for (size_t i = 0; i < task.NumArgs(); i++) {
        if (task.ArgByRef(i)) {
            const auto &it = dependencies.find(id);
            if (it != dependencies.end()) {
                if (!it->second->IsInPlasmaError()) {
                    // 对象在 in-memory 中（小对象）
                    // → 直接把数据内联到 task spec
                    mutable_arg->clear_object_ref();           // 清掉 ObjectRef
                    mutable_arg->set_is_inlined(true);          // 标记为 inlined
                    mutable_arg->set_data(data->Data(), ...);  // 放入实际数据
                    // → ArgByRef() 现在返回 false
                } else {
                    // 对象在 plasma 中（大对象）
                    // → 保持 by-ref，不 inline
                    // → ArgByRef() 保持 true
                }
            }
        }
    }
}
```

### 11.3 为什么 by-ref 可以盲写

`ArgByRef(i) == true` 的参数，一定是经过依赖解析后确认在 plasma 中的对象——小对象已经被 inline（`is_inlined = true`），只有 plasma 对象保持 by-ref。

**in-memory store 中的小对象永远不会以 by-ref 方式到达执行 Worker**——它在提交阶段就被 inline 了，通过 gRPC 直接发送。

---

## 12. 跨节点对象拉取的四个位置

### 12.1 四个拉取位置

| 位置 | 触发场景 | 代码位置 | BundlePriority | 时机 |
|------|---------|---------|----------------|------|
| 1 | raylet 调度 task 前预取参数 | `lease_dependency_manager.cc:246` | TASK_ARGS | task 分配前 |
| 2 | Worker 执行 task 前获取参数 | `core_worker.cc:3329` | GET_REQUEST | 用户代码前 |
| 3 | 用户代码中 `ray.get()` | `core_worker.cc:1406` | GET_REQUEST | 用户代码中 |
| 4 | 用户代码中 `ray.wait(fetch_local=True)` | `core_worker.cc:1556` | WAIT_REQUEST | 用户代码中 |

### 12.2 不拉取的位置

```cpp
// core_worker.cc:1445  GetIfLocal — 只查本地 plasma，不拉取
plasma_store_provider_->GetIfLocal(ids, &result_map);

// core_worker.cc:3103  PinExistingReturnObject — timeout=0，不等待
plasma_store_provider_->Get(object_ids, owner_addresses, 0, &result_map);
```

### 12.3 位置 1 的详细流程

```cpp
// lease_dependency_manager.cc:246
lease_entry->pull_request_id_ =
    object_manager_.Pull(required_objects, BundlePriority::TASK_ARGS, task_key);
// ↑ 在 raylet 分配 lease 给 Worker 之前就发起 pull
//   参数就绪后才会把 task 分配给 Worker
```

```cpp
// lease_dependency_manager.cc:307
std::vector<LeaseID> LeaseDependencyManager::HandleObjectLocal(const ObjectID &object_id) {
    local_objects_.insert(object_id);  // 标记为本地可用
    for (const auto &dependent_lease_id : object_entry->second.dependent_leases) {
        lease_entry->DecrementMissingDependencies();
        if (lease_entry->num_missing_dependencies_ == 0) {
            ready_lease_ids.push_back(dependent_lease_id);  // 所有参数就绪
        }
    }
    return ready_lease_ids;
}
```

---

## 13. Owner Location 订阅机制

### 13.1 订阅发生在 ObjectManager::Pull 中

```cpp
// object_manager.cc:214
uint64_t ObjectManager::Pull(const std::vector<rpc::ObjectReference> &object_refs, ...) {
    std::vector<rpc::ObjectReference> objects_to_locate;
    auto request_id = pull_manager_->Pull(object_refs, prio, task_key, &objects_to_locate);

    // 注册 location 变更回调
    const auto &callback = [this](const ObjectID &object_id,
                                  const std::unordered_set<NodeID> &client_ids, ...) {
        pull_manager_->OnLocationChange(object_id, client_ids, ...);
    };

    for (const auto &ref : objects_to_locate) {
        object_directory_->SubscribeObjectLocations(
            object_directory_pull_callback_id_, object_id, ref.owner_address(), callback);
        // ↑ 订阅 Owner 的 WORKER_OBJECT_LOCATIONS_CHANNEL
        //   owner_address 从 ObjectRef 中获取
    }
    return request_id;
}
```

### 13.2 两个独立机制驱动 TryToMakeObjectLocal

**机制 1：location 订阅回调（事件驱动）**

```cpp
// pull_manager.cc:362  OnLocationChange
void PullManager::OnLocationChange(...) {
    it->second.client_locations.clear();
    for (const auto &client_id : client_ids) {
        if (client_id != self_node_id_) {
            it->second.client_locations.push_back(client_id);  // 有了 location
        }
    }
    TryToMakeObjectLocal(object_id);  // ← 尝试 pull
}
```

**机制 2：定时器轮询（兜底重试）**

```cpp
// object_manager.cc:828  每 100ms 触发
void ObjectManager::Tick(...) {
    pull_manager_->Tick();
    // ↓ pull_manager.cc:577
    // void PullManager::Tick() {
    //     for (auto &pair : active_object_pull_requests_) {
    //         TryToMakeObjectLocal(pair.first);  // 对每个活跃请求重试
    //     }
    // }
}
```

### 13.3 PullFromRandomLocation

```cpp
// pull_manager.cc:516
bool PullManager::PullFromRandomLocation(const ObjectID &object_id) {
    auto &node_vector = it->second.client_locations;
    if (node_vector.empty()) {
        // 无 location → 检查 spill → 返回 false
        return false;
    }
    // 随机选一个有对象的节点
    std::uniform_int_distribution<int> distribution(0, node_vector.size() - 1);
    NodeID node_id = node_vector[distribution(gen_)];
    send_pull_request_(object_id, node_id);  // ← 发起 Pull RPC
    return true;
}
```

### 13.4 重试退避

```cpp
// pull_manager.cc:556
void PullManager::UpdateRetryTimer(ObjectPullRequest &request, ...) {
    auto retry_timeout_len = (pull_timeout_ms_ / 1000.) * (1UL << request.num_retries);
    request.next_pull_time = time + retry_timeout_len;
    // pull_timeout_ms 默认 10000ms
    // num_retries=0 → 10s 后重试
    // num_retries=1 → 20s 后重试
    // num_retries=2 → 40s 后重试
    // ... 指数退避，最大 10 * 1024 = 10240s
}
```

---

## 14. raylet 预取与 Worker Get 的关系

### 14.1 位置 1（raylet 预取）和位置 2（Worker Get）是冗余设计

```
位置 1: Node B raylet 在调度 task 前预取参数
  → 对象就绪后才会把 task 分配给 Worker
  → 目的：减少 Worker 等待时间

位置 2: Worker 收到 task 后再 Get 一次
  → 如果位置 1 已拉过来 → 直接命中
  → 如果位置 1 还在拉 → 继续等
  → 目的：保证 Worker 一定能拿到参数
```

PullManager 通过 `object_pull_requests_` 去重——同一个对象 ID 只有一个 pull request，多个 bundle request 共享。不会重复发起 `SendPullRequest`。

### 14.2 完整时序

```
t=0  Owner 提交 task (task spec 中有 by-ref 参数)
     │  Owner 的 raylet 调度 task → 选择目标 Worker 在 Node B
     │  向 Node B 的 raylet 发送 RequestWorkerLease RPC
     │
t=1  Node B raylet 收到 RequestWorkerLease
     → LeaseDependencyManager::RequestTaskLease
     → object_manager_.Pull(required_objects, BundlePriority::TASK_ARGS)
       ↑ ① 发起 Pull 和 SubscribeObjectLocations
     → task 进入 waiting_lease_queue
     │
t=2  对象到达 Node B 的 plasma store (或超时失败)
     → HandleObjectAdded → HandleObjectLocal → LeasesUnblocked
     → ScheduleAndGrantLeases → Grant(worker, lease)
     → Reply to Owner
     │
t=3  Owner 收到 Reply → PushTask RPC 给 Worker
     → Worker ExecuteTask → GetAndPinArgsForExecutor
     → plasma_store_provider_->Get(timeout=-1)
       ↑ ② 对象已在本地 plasma → 直接命中
     → 用户代码执行
```

### 14.3 失败场景（你的报错）

```
raylet 等待参数 → 10 分钟超时 → OBJECT_FETCH_TIMED_OUT
→ 错误对象写入 plasma（0字节 + 错误元数据）
→ HandleObjectAdded → HandleObjectLocal → LeasesUnblocked
  ↑ 错误对象也是"对象"，也会触发 local
→ task 被调度给 Worker
→ Worker GetAndPinArgsForExecutor → 从 plasma 拿到错误对象
→ IsException() = true → 反序列化为 ObjectFetchTimedOutError
→ "At least one of the input arguments could not be computed"
```

**即使是失败的情况，"数据"（错误对象）也被拉到了节点 B 的本地 plasma 上。**

---

## 15. ObjectRef Owner 地址传递

ObjectRef 的 owner 地址在 task 提交时就固定在 task spec 中了：

```
Owner 提交 task 时：
  → 依赖解析 (ResolveDependencies)
    → for each by-ref arg:
        → in_memory_store_.GetAsync(obj_id) — 获取对象
        → InlineDependencies:
            if 小对象 → inline 到 task spec (is_inlined=true, 清掉 object_ref)
            if plasma → 保持 object_ref (包含 owner_address)
  → task spec 中每个 by-ref 参数都有：
      args[i].object_ref.object_id     — 对象 ID
      args[i].object_ref.owner_address  — Owner 地址
      args[i].object_ref.call_site      — 创建调用点
```

task spec 通过 `RequestWorkerLease` RPC 发给目标 raylet，raylet 再通过 `PushTask` RPC 发给 Worker。整个过程中 owner_address 一直跟随着 ObjectRef，不需要额外查询。

---

## 16. "等待参数就绪"保证机制

### 16.1 raylet 在参数就绪前不会把 task 发给 Worker

```cpp
// lease_dependency_manager.cc:228-251
lease_entry->pull_request_id_ =
    object_manager_.Pull(required_objects, BundlePriority::TASK_ARGS, task_key);
// ↑ 发起拉取

return lease_entry->num_missing_dependencies_ == 0;
// ↑ 只有所有参数都就绪才返回 true
//   否则 task 停在 waiting_lease_queue 中
```

### 16.2 对象到达触发调度

```cpp
// lease_dependency_manager.cc:307
std::vector<LeaseID> LeaseDependencyManager::HandleObjectLocal(const ObjectID &object_id) {
    local_objects_.insert(object_id);  // 标记为本地可用
    
    for (const auto &dependent_lease_id : object_entry->second.dependent_leases) {
        lease_entry->DecrementMissingDependencies();
        if (lease_entry->num_missing_dependencies_ == 0) {
            ready_lease_ids.push_back(dependent_lease_id);  // 所有参数就绪
        }
    }
    return ready_lease_ids;
}
```

```cpp
// local_lease_manager.cc:733
void LocalLeaseManager::LeasesUnblocked(const std::vector<LeaseID> &ready_ids) {
    for (const auto &lease_id : ready_ids) {
        // 从 waiting_lease_queue 移到 leases_to_grant_
        leases_to_grant_[scheduling_key].push_back(work);
        waiting_lease_queue_.erase(it);
    }
    ScheduleAndGrantLeases();  // 分配 Worker，回复 Owner
}
```

### 16.3 Worker 收到 task 时参数已就绪

当 Worker 收到 task 时，所有 by-ref 参数已经被 raylet 拉取到本地 plasma 了。Worker 侧 `GetAndPinArgsForExecutor` 的 `plasma_store_provider_->Get(-1)` 基本上直接命中本地 plasma，不需要再等。

### 16.4 完整流程图

```
Owner (Node A)                    Node B Raylet                    Node B Worker
     │                                 │                                │
  ①  RequestWorkerLease RPC ────────→ │                                │
     (携带 task spec + by-ref 参数)     │                                │
     │                                 │ ② Pull + SubscribeObjectLocations│
     │                                 │   task 进入 waiting_lease_queue │
     │                                 │                                │
     │                                 │ ③ 对象到达本地 plasma              │
     │                                 │   HandleObjectLocal            │
     │                                 │   LeasesUnblocked              │
     │                                 │   ScheduleAndGrantLeases       │
     │                                 │   Grant → Reply to Owner       │
     │ ←── Reply ──────────────────────│                                │
     │                                 │                                │
  ④  PushTask RPC ───────────────────────────────────────────────────→  │
     │                                 │                                │ ⑤ ExecuteTask
     │                                 │                                │   GetAndPinArgsForExecutor
     │                                 │                                │   → 对象已在本地 plasma
     │                                 │                                │   → 用户代码执行
```

---

## 附录：关键源码文件索引

| 文件 | 关键方法/位置 |
|------|-------------|
| `python/ray/exceptions.py` | `ObjectFetchTimedOutError:672`, `ObjectLostError:631`, `as_instanceof_cause:245` |
| `python/ray/_private/auto_init_hook.py` | `wrap_auto_init:18`, `auto_init_ray:9` |
| `python/ray/_private/client_mode_hook.py` | `client_mode_hook:96` |
| `python/ray/_private/worker.py` | `get:2895`, `get_objects:950` |
| `python/ray/_private/serialization.py` | `_deserialize_object:450`, `_serialize_to_msgpack:617` |
| `python/ray/_raylet.pyx` | `raise_if_dependency_failed:891`, `store_task_errors:966`, `store_task_outputs:4207` |
| `src/ray/core_worker/core_worker.cc` | `GetObjects:1343`, `SealExisting:1190`, `GetAndPinArgsForExecutor:3246`, `HandleGetObjectStatus:3449`, `PopulateObjectStatus:3489` |
| `src/ray/core_worker/task_manager.cc` | `HandleTaskReturn:550`, `FailPendingTask:1257`, `MarkTaskReturnObjectsFailed:1555`, `CompletePendingTask:908` |
| `src/ray/core_worker/store_provider/plasma_store_provider.cc` | `Get:253`, `GetIfLocal:219`, `Wait:360` |
| `src/ray/core_worker/store_provider/memory_store/memory_store.cc` | `Put:172`, `GetImpl:259`, `GetRequest::Set:88`, `GetRequest::Wait:74` |
| `src/ray/core_worker/future_resolver.cc` | `ResolveFutureAsync:27`, `ProcessResolvedObject:60` |
| `src/ray/core_worker/task_submission/dependency_resolver.cc` | `ResolveDependencies:98`, `InlineDependencies:28` |
| `src/ray/core_worker/reference_counter.cc` | `UpdateObjectPinnedAtRaylet:917`, `PushToLocationSubscribers:1678`, `AddObjectLocation:1443`, `ReportLocalityData:1608` |
| `src/ray/object_manager/pull_manager.cc` | `Pull:52`, `OnLocationChange:362`, `TryToMakeObjectLocal:446`, `PullFromRandomLocation:516`, `Tick:577` |
| `src/ray/object_manager/object_manager.cc` | `Pull:214`, `SendPullRequest:255`, `HandlePull:616`, `HandlePush:543`, `Tick:828` |
| `src/ray/object_manager/ownership_object_directory.cc` | `SubscribeObjectLocations`, `ReportObjectAdded:121` |
| `src/ray/raylet/lease_dependency_manager.cc` | `RequestTaskLease:228`, `HandleObjectLocal:307`, `StartGetRequest:118` |
| `src/ray/raylet/scheduling/local_lease_manager.cc` | `LeasesUnblocked:733`, `Grant:990` |
| `src/ray/common/task/task_spec.cc` | `ArgByRef:265` |
| `src/ray/common/ray_config_def.h` | `fetch_fail_timeout_milliseconds:276`, `object_manager_timer_freq_ms:345` |
| `src/ray/protobuf/common.proto` | `ErrorType enum` |
