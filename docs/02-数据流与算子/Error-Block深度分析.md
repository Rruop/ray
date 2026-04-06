# Ray Data Error Block 处理机制深度分析

> 分析 `streaming_executor_state.py` 中 `process_completed_tasks` 方法的 `except Exception as e` 异常捕获机制，涵盖异常原因、Task 重试行为、Object 丢失场景、Object Owner 机制、ray.wait/ray.get 判定逻辑及边缘竞争条件。

---

## 1. 两处异常捕获位置

`process_completed_tasks` 中有两处 `except Exception as e`：

| 位置 | 所在阶段 | 代码行 |
|------|----------|--------|
| 第1处 | Phase 2: `prepare_metadata()` 调用 | :606 |
| 第2处 | Phase 4: `ray.get(meta_ref)` + `complete_with_metadata()` 调用 | :695 |

### 第1处异常原因 (Phase 2, prepare_metadata)

`prepare_metadata()` 内部获取 block_ref 和 meta_ref（physical_operator.py:271-334）：

```python
self._pending_block_ref = self._streaming_gen._next_sync(timeout_s=0)
self._pending_meta_ref = self._streaming_gen._next_sync(timeout_s=METADATA_WAIT_TIMEOUT_S)
```

| 异常类型 | 触发场景 |
|---------|---------|
| Task 永久失败后的异常 | Task 重试耗尽后，streaming generator 结束。`_next_sync` 抛 StopIteration，代码 `try: ray.get(block_ref)` 重新抛出原始 UDF 异常（:314-325） |
| ObjectRefStream 异常对象 | Task 执行中 crash，generator 写入了异常对象而非正常 block，`ray.get(block_ref)` 抛出 |
| meta_ref 获取超时 | `_next_sync` 在等待 meta_ref 时内部 ray.wait 超时，返回 nil → 不抛异常，返回 False |

### 第2处异常原因 (Phase 4, complete_with_metadata)

```python
meta_with_schema = ray.get(meta_ref, timeout=0)
bytes_read = task.complete_with_metadata(meta_with_schema)
```

| 异常类型 | 触发场景 |
|---------|---------|
| ObjectLostError | 存储 metadata 的节点宕机，lineage reconstruction 不可行（如 lineage 被驱逐） |
| ObjectReconstructionFailedError | 对象需要重建但重建失败（重试耗尽、资源不足等） |
| OwnerDiedError | 拥有该 ObjectRef 的 worker 进程死亡 |
| 反序列化异常 | metadata 对象损坏或版本不兼容 |
| complete_with_metadata 内部回调异常 | `output_ready_callback` 抛出异常 |

---

## 2. 异常是否代表 block 最终处理失败？—— 是的

异常到达 `process_completed_tasks` 的 except 时，Ray Core 层面的重试已全部完成或不适用。**不会再重试这个 block，这代表该 block 最终处理异常**。

### Ray Data 的两层"重试"机制

```
┌─────────────────────────────────────────────────┐
│  Layer 1: Ray Core Task 重试（异常到达 executor 之前） │
│  — max_retries=-1（stateless task，无限重试基础设施错误）│
│  — retry_exceptions（actor task，可配置）              │
│  — 只重试：节点故障、抢占、RaySystemError 等            │
│  — 不重试：UDF 抛出的应用层异常（默认）                  │
├─────────────────────────────────────────────────┤
│  Layer 2: process_completed_tasks 异常处理            │
│  — Ray Core 的重试已经用完                             │
│  — Task 已确定性地失败                                 │
│  — 要么丢弃 block（max_errored_blocks 容忍），           │
│    要么终止整个 Dataset 执行                             │
└─────────────────────────────────────────────────┘
```

### max_errored_blocks 三种结局

```python
except Exception as e:
    errored_blocks_per_op[state] += 1
    num_errored_blocks += 1
    should_ignore = (
        max_errored_blocks < 0
        or max_errored_blocks >= num_errored_blocks
    )
```

| max_errored_blocks 配置 | 行为 |
|------------------------|------|
| 0（默认） | 任何一个 block 异常 → 立即 `raise e`，整个 Dataset 执行中止 |
| > 0（如 10） | 前 10 个异常 block 被静默丢弃（数据丢失），第 11 个触发中止 |
| < 0（如 -1） | 无限容忍，所有异常 block 都被丢弃，执行继续 |

### 让 UDF 异常也重试的配置

Ray Core 默认不重试应用层异常。配置方式：

```python
from ray.data import DataContext

# Actor Task：重试所有异常
DataContext.get_current().actor_task_retry_on_errors = True

# Actor Task：只重试特定异常类型
DataContext.get_current().actor_task_retry_on_errors = [ValueError, IOError]
```

Stateless Task 默认 `max_retries=-1` 但 `retry_exceptions=False`，只重试基础设施错误，无 DataContext 选项让其重试 UDF 异常。

---

## 3. 为什么是 Task 重试失败之后才抛？—— 完整流程追踪

### 正常执行流程

```
Driver 端                                    Worker 端
──────────                                   ──────────
ray.remote(fn).remote()  ──────────────►  Task 开始执行
                                               │
                                               │ fn 内部 yield block, metadata
                                               │ (streaming generator 写入 ObjectRefStream)
                                               ▼
ray.wait(generators)  ◄────────────────  ObjectRefStream 的 next slot 有值
                                               │
_next_sync() → 返回 block_ref                 │
_next_sync() → 返回 meta_ref                  │
ray.get(meta_ref) → 获取 metadata             │
complete_with_metadata() → 消费 block
```

### Task 失败 + Ray Core 重试期间的流程

Worker crash 后，TaskManager 调用 `FailOrRetryPendingTask`：

```
Task 执行中 Worker crash
        │
        ▼
TaskManager::FailOrRetryPendingTask()
        │
        ├─ max_retries=-1 → will_retry = true
        ├─ 关键：will_retry=true，NOT 调用 FailPendingTask()
        │   所以：
        │   ✗ 不删除 ObjectRefStream
        │   ✗ 不向 stream 写入错误对象
        │   ✗ 不调用 MarkEndOfStream
        │
        └─ 调用 RetryTaskIfPossible() → Task 重新入队
```

重试期间，从 Driver 端看：

```
ray.wait(generators, timeout=0.1)
        │
        ▼
peek_object_ref_stream(generator_ref)
→ 返回 next_index_ 对应的 ObjectID
→ is_ready = false（新 Task 还没执行到 yield）
        │
        ▼
ray.wait 超时 → generator 不在 ready 列表中 ← 关键！
```

调度循环中：

```python
ready, _ = ray.wait(list(active_tasks.keys()), ...)
# ↑ 重试中的 Task 的 generator 不在 ready 列表中
# ↓ 这个 Task 完全不会被处理
# ↓ 不会调用 prepare_metadata()
# ↓ 不会触发任何 except Exception
```

**对 Driver 来说，重试期间的 Task 就像"还在运行但还没产出数据"——没有任何可观测的区别。**

### 重试成功后

```
新 Worker 重新执行 Task，yield block_0, meta_0, block_1, meta_1, ...
        │
        ▼
ObjectRefStream.InsertToStream():
  - 已消费的 slot (index < next_index_) → 静默丢弃（不重复写入）
  - 未消费的 slot → 写入新值
        │
        ▼
peek_object_ref_stream → is_ready = true
ray.wait 返回 generator 为 ready ← 恢复正常！
```

### 重试耗尽后

```
TaskManager 确认重试次数用完
        │
        ▼
调用 FailPendingTask()
→ MarkTaskReturnObjectsFailed()
→ MarkEndOfStream()
→ 向 ObjectRefStream 写入异常标记
        │
        ▼
Driver 端：
_next_sync()
→ try_read_next_object_ref_stream → 抛 ObjectRefStreamEndOfStreamError
→ catch 后调用 ray.get(generator_ref)
→ 抛出原始异常
→ _generator_task_raised = True
→ 返回 generator_ref（指向异常对象）
```

然后进入 `prepare_metadata()`：

```python
# physical_operator.py:296
self._pending_block_ref = self._streaming_gen._next_sync(timeout_s=0)
# ↑ 返回 generator_ref（异常对象引用）

# physical_operator.py:311
self._pending_meta_ref = self._streaming_gen._next_sync(timeout_s=METADATA_WAIT_TIMEOUT_S)
# ↑ 抛 StopIteration（generator 已结束）

# physical_operator.py:314-325
except StopIteration:
    try:
        ray.get(self._pending_block_ref)  # 获取异常对象 → 重新抛出原始 UDF 异常
        assert False, "Above ray.get should raise an exception."
    except Exception as ex:
        self._task_done_callback(ex)
        self._has_finished = True
        raise ex from None  # ← 传播到 streaming_executor_state.py:606
```

---

## 4. ObjectRefStream 内部机制详解

### ObjectID 的确定性

ObjectID 由 `task_id + index` 计算得出，不依赖 Task 是否执行。无论 Task 在运行、重试、还是已完成，同一个 `next_index_` 对应的 ObjectID 永远一样。

### PeekNextItem() 的返回值

```cpp
// task_manager.cc:162
std::pair<ObjectID, bool> ObjectRefStream::PeekNextItem() {
    const auto &object_id = GetObjectRefAtIndex(next_index_);
    if (refs_written_to_stream_.find(object_id) == refs_written_to_stream_.end()) {
        return {object_id, false};  // ← ObjectID 确定性，但还没写入值
    } else {
        return {object_id, true};   // ← 值已写入，ready
    }
}
```

- **is_ready 取决于 refs_written_to_stream_**：只有当 Worker 执行 Task 并 yield 值后，InsertToStream 才会把 ObjectID 加入。

### InsertToStream 的核心逻辑

```cpp
// task_manager.cc:181
bool ObjectRefStream::InsertToStream(const ObjectID &object_id, int64_t item_index) {
    if (item_index < next_index_) {
        return false;  // ← 已消费的 slot，静默丢弃
    }
    auto [_, inserted] = refs_written_to_stream_.emplace(object_id);
}
```

重试成功后，Driver 只会看到 `next_index_` 之后的产出，之前已被消费的部分被静默丢弃。

### 正常执行时间线

```
时间    Worker 端                              Driver 端 (ObjectRefStream 状态)
────    ──────────                             ────────────────────────────────
T0      Task 开始执行                          next_index_=0, refs_written_to_stream_={}
                                               peek → (ObjectID_0, is_ready=false)
                                               ray.wait → 超时，generator 在 not_ready

T1      yield block_0                          InsertToStream(ObjectID_0, index=0)
                                               refs_written_to_stream_={ObjectID_0}

T2                                             ray.wait([ObjectID_0], timeout=0.1)
                                               → generator 在 ready 列表！

T3                                             _next_sync() → TryReadNextItem() → next_index_: 0→1
                                               → 返回 block_ref (ObjectID_0)

T4      yield meta_0                           InsertToStream(ObjectID_1, index=1)
                                               refs_written_to_stream_={ObjectID_0, ObjectID_1}

T5                                             peek → (ObjectID_1, is_ready=true)
                                               _next_sync() → 返回 meta_ref (ObjectID_1)
                                               next_index_: 1→2
```

### Task 失败 + 重试期间时间线

假设 Worker 在 yield block_0 之后 crash：

```
时间    Worker 端                              Driver 端 (ObjectRefStream 状态)
────    ──────────                             ────────────────────────────────
T0-T4   同正常执行                              同正常执行

T5      yield block_1                          InsertToStream(ObjectID_2)
                                               refs_written_to_stream_={..., ObjectID_2}

T6      ⚡ Worker crash!

T7      FailOrRetryPendingTask()
        → will_retry=true → NOT 调用 FailPendingTask()
        → 不删除 ObjectRefStream、不调用 MarkEndOfStream、不写入错误对象
        ObjectRefStream 状态保持: next_index_=2, refs_written_to_stream_={ObjectID_0,1,2}

T8      (新 Worker 尚未开始执行)                 peek → (ObjectID_2, is_ready=true)
                                               但 ObjectID_2 指向的对象可能已丢失
                                               取决于对象是否还在 plasma 中

T9      新 Task 从头执行
        yield block_0 → InsertToStream(index=0) → 静默丢弃（已消费）
        yield meta_0 → InsertToStream(index=1) → 静默丢弃（已消费）

T11     yield block_1 → InsertToStream(index=2) → 写入！
        → ray.wait 返回 ready ← 恢复正常
```

**重要细节**：T5 时 ObjectID_2 已写入 `refs_written_to_stream_`，Worker crash 后条目仍在。PeekNextItem() 返回 `(ObjectID_2, true)`，但实际对象值可能已丢失。此时 ray.wait 的行为取决于对象是否在 plasma 中。

### 重试也全部失败时

```
FailPendingTask()
→ MarkTaskReturnObjectsFailed()
→ MarkEndOfStream() → end_of_stream_index_ = max(next_index_, 0)
→ 向每个已知 streaming return ref 写入错误对象

Driver 端：
_next_sync()
→ peek → is_ready=true [错误对象已写入]
→ try_read_next_object_ref_stream() → IsFinished()=true → 抛 ObjectRefStreamEndOfStreamError
→ catch: ray.get(generator_ref) → 抛出原始 Task 异常
→ 返回 generator_ref（指向异常对象）

prepare_metadata():
→ _next_sync() 返回 generator_ref (异常对象引用)
→ _next_sync() → StopIteration
→ catch StopIteration: ray.get(block_ref) → 抛出原始 UDF 异常
→ raise ex from None ← 传播到 streaming_executor_state.py:606
```

---

## 5. Task 完成后 Object 丢失场景

### 时间线

```
T1: Worker 执行完 Task，yield block_ref + meta_ref
T2: Driver 的 ray.wait() 返回 generator 为 ready
T3: prepare_metadata() 成功获取 block_ref 和 meta_ref
T4: ⚡ 存储节点宕机，block 和 metadata 对象丢失
T5: Driver 尝试 ray.get(meta_ref)
```

在 T5 时刻，Ray Core 的行为：

| 情况 | ray.get(meta_ref) 的行为 | 会到 except 吗 |
|------|--------------------------|---------------|
| Ray Core 可以重建（task lineage 还在） | 阻塞等待重建完成，返回正常值 | 不会 |
| Ray Core 无法重建（lineage 驱逐等） | 抛 ObjectReconstructionFailedError | 会到 :695 |
| Object Owner 死亡 | 抛 OwnerDiedError | 会到 :695 |

### 新流程中的隐式重试

Phase 3 批量 ray.wait：

```python
ready_meta_refs, _ = ray.wait(
    meta_refs,
    num_returns=len(meta_refs),
    timeout=METADATA_WAIT_TIMEOUT_S,  # 0.0 秒
    fetch_local=True,
)
```

- object 重建中 → meta_ref 不在 ready_meta_set → continue → 下次循环再试
- 这是隐式重试：调度循环不断 ray.wait 轮询

重建成功：下次 ray.wait → meta_ref 在 ready_meta_set → ray.get(meta_ref, timeout=0) 成功 → 不会进 except

重建失败：下次 ray.wait/ray.get → 抛 ObjectReconstructionFailedError → 进入 :695 except

### Object 丢失后的重试机制

| 层级 | 重试？ | 机制 |
|------|--------|------|
| Ray Core 层 | 会 | Lineage reconstruction：透明重新执行 Task 重建丢失的 Object |
| Ray Data Streaming Executor 层 | 会（隐式） | 调度循环不断 ray.wait，直到 object 重建完成变为 local |

### 完整流程图

```
Task 成功 yield block + metadata
        │
        ▼
存储节点宕机，Object 丢失
        │
        ▼
Ray Core 发起 Lineage Reconstruction
        │
        ├─ 重建成功 ──────────────────────────┐
        │   (透明重新执行 Task，重新生成 Object)  │
        │                                     │
        ├─ 重建进行中                           │
        │   ray.wait → not_ready → continue    │
        │   下次循环再 ray.wait → 继续等...      │
        │                                     │
        └─ 重建失败 ────────────────────┐
            (lineage 驱逐、资源不足等)     │
                                     │
        ◄─────────────────────────────┘
        │
        ▼
  ray.get(meta_ref) 抛异常
        │
        ▼
  except Exception as e (:695)
        │
        ├─ max_errored_blocks 还有余量 → 丢弃 block，继续执行
        └─ max_errored_blocks 耗尽 → raise e，整个 Dataset 中止
```

---

## 6. Object 丢失后 ray.get 阻塞等待重建的完整流程

### 场景

Task 成功完成 yield 了 block_ref 和 meta_ref，Driver prepare_metadata() 已获取引用，但还没 ray.get(meta_ref)。⚡ 存储节点宕机，meta_ref 指向的对象丢失。

### 重建流程调用链

**Step 1: GCS 检测节点宕机**

```
GCS 监控所有 raylet 心跳 → raylet 超时 → 广播 NodeRemoval 通知
```

**Step 2: Owner ReferenceCounter 接收通知**

```
reference_counter.cc: ResetObjectsOnRemovedNode(node_id)
  │ 遍历所有 tracked 对象
  │ 找到 pinned_at_node_id_ == node_id 的对象
  │ 清除 pinned_at_node_id_
  │ 将对象加入 objects_to_recover_ 队列
  ▼
objects_to_recover_.push_back(object_id)
```

**Step 3: Owner 周期性恢复循环 (每 100ms)**

```
core_worker.cc: 定时回调
  ▼
lost_objects = reference_counter_->FlushObjectsToRecover()
  ▼
for object_id in lost_objects:
    object_recovery_manager_->RecoverObject(object_id)
```

**Step 4: ObjectRecoveryManager::RecoverObject()**

```
  ├─ 检查: owned_by_us? → 是 (Driver 就是 Owner)
  ├─ 检查: 对象是否还有其他副本? → object_lookup_ 查询 Object Directory
  │
  ├─ 如果有其他副本:
  │   → PinExistingObjectCopy() → PinObjectIDs RPC 到持有副本的 raylet
  │   → 成功 → in_memory_store_.Put(OBJECT_IN_PLASMA, object_id)
  │   → 唤醒所有等待的 ray.get
  │
  └─ 如果没有其他副本:
      → ReconstructObject(object_id)
      → 检查 lineage_eligibility_
      → 如果 ELIGIBLE:
         → task_manager_.ResubmitTask(task_id, &task_deps)
         → 递归恢复依赖对象
         → 新 Task 完成后 → in_memory_store_.Put(OBJECT_IN_PLASMA, object_id)
         → 唤醒等待的 ray.get
```

**Step 5: ray.get 阻塞端**

```
C++ CoreWorker::Get() → GetObjects()
  ├─ 小对象: memory_store_->Get(timeout_ms)
  │   → condition_variable::wait_for()
  │   → 阻塞直到: a) 重建完成 b) 重建失败 c) 超时
  │
  └─ 大对象: plasma_store_provider_->Get(timeout_ms)
      → AsyncGetObjects IPC 到 raylet
      → 阻塞直到: a) plasma 可用 b) 错误对象 c) 超时
```

**Step 6: 重建完成后解除阻塞**

```
ObjectRecoveryManager 重建成功
  → in_memory_store_.Put(OBJECT_IN_PLASMA, object_id)
  → cv_.notify_all() ← 唤醒所有 condition_variable wait
  → ray.get 返回正常值
```

### 在 Streaming Executor 中的隐式轮询等待

新流程中 Phase 4 使用 `ray.get(meta_ref, timeout=0)`——不阻塞等待重建。Phase 3 的 `ray.wait(fetch_local=True)` 先确认可用性。

```
调度循环 Iteration 1:
  Phase 3: ray.wait → meta_ref not ready（重建中）→ continue

调度循环 Iteration 2: (几十毫秒后)
  Phase 3: ray.wait → meta_ref 仍然 not ready → continue

... 反复轮询 ...

调度循环 Iteration N: (重建完成后)
  Phase 3: ray.wait → meta_ref ready！
  Phase 4: ray.get(meta_ref, timeout=0) → 成功返回 metadata → 正常消费 block
```

### 重建失败的情况

```
ObjectRecoveryManager::RecoverObject() → ReconstructObject() 失败
  → recovery_failure_callback_(object_id, error_type)
  → core_worker->Put(RayObject(error_type), object_id) → 写入错误对象

Driver 端下次 ray.wait/ray.get:
  → meta_ref 变为 ready（错误对象在 plasma 中）
  → ray.get(meta_ref, timeout=0) → 抛 ObjectReconstructionFailedError
  → 进入 except Exception as e (:695)
```

---

## 7. Object Owner 机制

### Owner 是谁？

Object Owner 是调用 `.remote()` 或 `ray.put()` 的 Worker，即 **Task 的提交方/调用方**，而不是 Task 的执行方/生成方。

Ray 官方文档：

> The owner of an object is the worker process that creates the original ObjectRef, e.g., by calling f.remote() or ray.put(). Note that this worker is usually a distinct process from the worker that creates the value of the object.

```
┌─────────────────────┐      .remote()       ┌─────────────────────┐
│  Driver (Owner)     │ ──────────────────►  │  Worker (Executor)  │
│                     │                      │                     │
│  调用 map_task.remote()│                   │  执行 _map_task()   │
│  获得 ObjectRef     │                      │  yield block, meta  │
│  reference_counter_ │                      │                     │
│  追踪对象状态        │                      │                     │
│  submissible_tasks_ │◄──────────────────  │  执行完毕            │
│  保存 Task lineage  │                      │                     │
└─────────────────────┘                      └─────────────────────┘
       ↑ Owner                                    ↑ Executor
    (提交方)                                    (生成方)
```

C++ 代码体现：

```cpp
// task_manager.cc:293 — 在 Task 提交方 (Owner) 上执行
reference_counter_.AddOwnedObject(
    return_id,
    /*contained_ids=*/{},
    caller_address,   // ← 提交方自己的地址成为 owner
);
```

### Owner 的职责

| 职责 | 说明 |
|------|------|
| 对象元数据追踪 | 追踪对象位置（pinned_at_node_id_）、是否 spilled、是否正在创建 |
| Borrower 追踪 | 追踪哪些其他 Worker 持有该 ObjectRef 的引用 |
| Task Lineage 保存 | 在 `submissible_tasks_` 中保存 Task 的完整规格，用于重建 |
| 驱动重建 | ObjectRecoveryManager 运行在 Owner 上，负责检测丢失并重新提交 Task |
| 响应 GetObjectStatus RPC | 其他 Worker 通过 RPC 查询 Owner 获取对象位置 |

### Owner 死亡后能重建吗？—— 不能

```
Owner (Driver) 死亡
      │
      ├─ reference_counter_ 销毁 → 无法追踪对象位置、检测丢失、触发重建
      ├─ submissible_tasks_ 销毁 → Task 规格永久丢失，无法重新提交
      ├─ ObjectRecoveryManager 销毁 → 没有进程驱动重建流程
      └─ 内存中的 ObjectRef 引用图销毁 → 其他 Worker 引用变为 "orphan"
```

Owner 死亡后，其他 Worker 访问该对象 → GetObjectStatus RPC 失败 → 写入 OWNER_DIED 错误对象 → ray.get 抛 OwnerDiedError。

**在 Ray Data 场景中**：Owner 就是 Driver 进程本身。Driver 死亡 → 整个 Dataset 执行已停止 → 不存在"Owner 死了但 Executor 还在运行"的情况 → OwnerDiedError 在 Ray Data 正常场景中几乎不会出现。更可能出现在 Ray Serve 的 replica 之间传递引用的场景。

---

## 8. 全场景总结对比

| 场景 | 会到 except 吗 | 能重建吗 | 重建后还会到 except 吗 |
|------|---------------|---------|---------------------|
| Task 重试期间 | 不会 | N/A（还在重试） | N/A |
| Task 重试全部失败 | 会 (:606) | 不能（重试已耗尽） | N/A |
| Task 成功 + Object 丢失 + 重建成功 | 不会 | 能 | 不会 |
| Task 成功 + Object 丢失 + 重建失败 | 会 (:695) | 不能（lineage 驱逐/重试耗尽） | N/A |
| Task 成功 + Object 丢失 + Owner 死亡 | 不会到此处 | 不能 | Driver 已死，整个执行停止 |
| UDF 应用层异常（默认不重试） | 会 (:606) | 不能（非基础设施错误，Ray Core 不重试） | N/A |

---

## 9. ray.wait ready/not_ready 详细判定逻辑

### 完整判定流程

```
ray.wait([ref1, ref2, ...], num_returns=N, timeout_ms=T, fetch_local=F)
│
▼
Step 1: memory_store_->Wait()
│
│  objects_ map 中有条目:
│    if IsInPlasmaError(): → plasma_object_ids（路由到 plasma，不计入 ready）
│    else: → ready（包括错误对象如 OBJECT_LOST, OwnerDiedError 等）
│
│  objects_ map 中无条目:
│    → 创建 GetRequest → cv_.wait_for(timeout) → 超时后 not_ready
│
├─ 如果 fetch_local=True:
│   Step 2a: plasma_store_provider_->Wait()
│     → raylet IPC 检查对象是否在本地 plasma store 中
│     → 返回本地可用的对象集合
│
├─ 如果 fetch_local=False:
│   Step 2b: 直接标记 ready
│     → plasma_object_ids 中的对象不做任何网络操作，直接视为 ready
│
▼
填充 results
```

### memory_store 三种结果分类

| result_objects[i] 值 | 含义 | 归类 |
|---------------------|------|------|
| nullptr | objects_ map 中无此条目 | not_ready |
| IsInPlasmaError() == true | 对象在 plasma 中（或曾经在） | → plasma_object_ids，交给 Step 2 |
| 其他非 nullptr（包括错误对象） | 对象值直接在 memory_store 中 | → ready |

### Phase 1: ray.wait(generators, fetch_local=False, timeout=0.1)

| # | 场景 | memory_store | 最终结果 |
|---|------|-------------|---------|
| 1 | Task 运行中，未 yield | 无条目 → Wait 超时 | not_ready |
| 2 | Task yield 了，plasma 有值 | IsInPlasmaError → 直接 ready | ready |
| 3 | Task yield 了，对象丢失但 Owner 未检测到 | IsInPlasmaError（旧标记）→ 直接 ready | ready ⚠️ 虚假 ready |
| 4 | Task yield 了，对象丢失且 Owner 已检测到 | 无条目 → Wait 超时 | not_ready |
| 5 | Task 永久失败 | 错误对象 → ready | ready |
| 6 | Task 重试中，slot 已写入 | 同 #3 或 #4 | 同上 |
| 7 | Task 重试中，slot 未写入 | 无条目 → Wait 超时 | not_ready |

**场景 #3 是"虚假 ready"**：对象已丢失但 IsInPlasmaError 标记还在 memory_store 中。fetch_local=False 不做进一步验证，直接视为 ready。

### Phase 3: ray.wait(meta_refs, fetch_local=True, timeout=0.0)

| # | 场景 | Step 1 | Step 2a (raylet IPC) | 最终 |
|---|------|--------|---------------------|------|
| 1 | meta 正常在本地 plasma | IsInPlasmaError → plasma | 本地有 → ready | ready |
| 2 | meta 在远程 plasma | IsInPlasmaError → plasma | 本地没有 → 超时 | not_ready |
| 3 | meta 丢失，Owner 未检测到 | IsInPlasmaError → plasma | raylet 查不到 → 超时 | not_ready |
| 4 | meta 丢失，Owner 已检测到 | 无条目 → 超时 | — | not_ready |
| 5 | meta 重建成功 | IsInPlasmaError → plasma | 本地有 → ready | ready |
| 6 | meta 重建失败，错误对象已写入 | 错误对象 → ready | — | ready |

对比 Phase 1：Phase 3 的 fetch_local=True 使场景 #3 不再是"虚假 ready"——raylet 会检查对象是否真的在本地 plasma 中。

---

## 10. Object 丢失时 ray.wait 的状态转换

### 四种状态

**状态 A：Object 还在 plasma 中（尚未丢失）**

```
memory_store: OBJECT_IN_PLASMA
ray.wait(fetch_local=False): → ready ✅
```

**状态 B：Owner 检测到丢失，正在重建**

```
Owner 的 ReferenceCounter 收到 NodeRemoval → ResetObjectsOnRemovedNode()
  → memory_store_->Delete(lost_objects) ← 删除 OBJECT_IN_PLASMA 标记
  → objects_to_recover_.push_back(object_id)

memory_store: 空！
ray.wait(fetch_local=False): → not_ready ❌
```

**状态 C：重建失败，错误对象已写入**

```
core_worker->Put(RayObject(OBJECT_LOST), object_id)
memory_store_->Put(OBJECT_IN_PLASMA, object_id) ← 重新写入标记

memory_store: OBJECT_IN_PLASMA
ray.wait(fetch_local=False): → ready ✅
ray.get: → 抛 ObjectLostError
```

**状态 D：重建成功**

```
新 Task 执行完成 → 新值写入 plasma
memory_store_->Put(OBJECT_IN_PLASMA, object_id)

ray.wait: → ready ✅
ray.get: → 正常返回
```

### 完整时间线

```
时间    事件                                memory_store      ray.wait 结果
────    ────                                ────────────      ────────────
T0      Worker yield block                  OBJECT_IN_PLASMA  ready ✅
T1      ⚡ 节点宕机（未检测到）               OBJECT_IN_PLASMA  ready ✅
T2      Owner 检测到丢失，删除标记            空                not_ready ❌
T3      Owner 开始重建                       空                not_ready ❌
T4      重建中...                            空                not_ready ❌
T5a     重建成功 → 新值写入 plasma            OBJECT_IN_PLASMA  ready ✅
T5b     重建失败 → 错误对象写入 plasma         OBJECT_IN_PLASMA  ready ✅
```

**T1→T2 之间有窗口期**：节点宕机了但 Owner 还没检测到。memory_store 中仍有 OBJECT_IN_PLASMA 标记，ray.wait 返回 ready。此时 ray.get 才会发现对象丢失，触发重建。

---

## 11. ray.get(meta_ref, timeout=0) 超时处理

### timeout=0 的精确含义

C++ 层 `memory_store.cc GetImpl with timeout_ms=0`：

```cpp
cv_.wait_for(0ms)  // 非阻塞检查，立即返回
→ is_ready_ == false → timed_out = true → 退出
→ return Status::TimedOut
```

Python 层转换为 `GetTimeoutError`。

### 新流程中 GetTimeoutError 不会到达 except Exception

Phase 3 ray.wait(fetch_local=True) 已确认 meta_ref 在 ready_meta_set 中，Phase 4 只对 ready 的调用 ray.get：

```python
for state, task, meta_ref in pending_meta_tasks:
    if meta_ref not in ready_meta_set:
        continue  # ← 不在 ready 中的直接跳过
    try:
        meta_with_schema = ray.get(meta_ref, timeout=0)  # ← 只对 ready 的调用
```

### ray.get(meta_ref, timeout=0) 可能抛异常的情况

| 情况 | 抛什么异常 | 是否到 except :695 |
|------|----------|-----------------|
| meta_ref 在 ready 中，对象正常 | 不抛，正常返回 | 不会 |
| meta_ref 在 ready 中，对象是错误对象（OBJECT_LOST） | ObjectLostError / ObjectReconstructionFailedError | 会 |
| ray.wait 和 ray.get 之间有竞争（对象被删除） | GetTimeoutError | 会 |
| meta_ref 不在 ready 中 | 不会调用 ray.get | 不会 |

### 新流程 vs 旧流程

**旧流程**（一步到位）：
```python
# physical_operator.py:225-238
try:
    meta_with_schema = ray.get(self._pending_meta_ref, timeout=1.0)
except ray.exceptions.GetTimeoutError:
    logger.warning("Metadata object not ready... Will retry in next iteration.")
    break
```

**新流程**（两步分离）：
```
Step 1: ray.wait(meta_refs, fetch_local=True, timeout=0.0) → 不超时，只是检测
Step 2: ray.get(meta_ref, timeout=0) → 只在 Step 1 确认可用后调用
```

新流程不需要处理 GetTimeoutError，ray.wait 超时只是 not_ready → continue。重建等待通过调度循环轮询实现，不是 ray.get 阻塞等待。

---

## 12. 边缘竞争条件：ray.wait ready 后 ray.get 之间对象被删除

### 竞争条件的精确触发

```
T1: Phase 3 ray.wait(meta_refs, fetch_local=True) → meta_ref ready
    → memory_store 有 IsInPlasmaError 条目，本地 plasma 有 meta 对象值

T2: ⚡ Owner 100ms 周期恢复循环或其他原因 → memory_store_->Delete(meta_object_id)
    → 本地 raylet 可能因内存压力 eviction 了 meta 对象

T3: Phase 4 ray.get(meta_ref, timeout=0)
```

### ray.get 的两条路径

| 场景 | memory_store 条目 | ray.get 走哪条路 | 结果 |
|------|-----------------|----------------|------|
| A: 条目被 Delete | 无 | memory_store 超时 | GetTimeoutError |
| B: 条目还在，IsInPlasmaError | 有 | → plasma_store_provider → 检查本地 | 成功或超时 |

### block 会丢失吗？

**should_ignore = True（max_errored_blocks 允许）时**：

`_pending_block_ref` 和 `_pending_meta_ref` 没被清理（`complete_with_metadata` 没被调用），下次调度循环 `prepare_metadata()` 发现两个 ref 都 non-nil → 返回 True → 重新进入 Phase 3/4 → 如果重建成功 → 正常消费 block。

但 `errored_blocks_per_op` 已经 +1 → **假阳性**：数据没有丢失，但错误计数多了一个，可能提前耗尽 max_errored_blocks 配额。

**should_ignore = False（max_errored_blocks=0，默认）时**：

`raise e` → 异常传播到 StreamingExecutor.run() → 整个 Dataset 执行中止 → 所有数据丢失。

### block_ref vs meta_ref 的丢失

meta_ref 丢失在 `ray.get(meta_ref, timeout=0)` 时检测。block_ref 丢失延迟到下游 operator `ray.get(block_ref)` 时才检测：

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

## 13. FAQ 与常见误解辨析（基于实际排查对话整理）

本节针对生产环境排查 error block 时最容易产生的误解，逐一基于源码澄清。

### 13.1 "Ray Data 的 task/actor 不是都设置了无限重试吗？为什么还会有 error block？"

**误解**：以为 `max_retries=-1` 意味着任何失败都会无限重试，因此不应产生 error block。

**事实**：Ray Data 的"无限重试"覆盖范围比想象中**窄得多**。

#### 验证默认配置

```python
# python/ray/data/_internal/remote_fn.py:37
"max_retries": -1                              # stateless task
# python/ray/data/_internal/execution/operators/actor_pool_map_operator.py:576
ray_remote_args["max_task_retries"] = -1       # actor task
```

```cpp
// src/ray/core_worker/task_manager.cc:432
if (task_entry.num_retries_left_ > 0) {
    task_entry.num_retries_left_--;
} else {
    RAY_CHECK(task_entry.num_retries_left_ == -1);  // -1 永远不递减
}
```

`num_retries_left_` 初始化为 -1 后**永远保持 -1，不会被消耗**。

#### "无限重试"的真实覆盖范围

`remote_fn.py:_add_system_error_to_retry_exceptions` 只把 **`RaySystemError`** 加进 `retry_exceptions`：

```python
elif not retry_exceptions:
    retry_exceptions = [ray.exceptions.RaySystemError]
```

也就是说 Ray Data 默认的 Python 层 `retry_exceptions` **只包含 `RaySystemError` 一类**。

而 C++ 层 `task_manager.cc:1173` 的 `is_preempted && IsRetriable()` 判定，会自动重试节点抢占、worker crash 等基础设施错误，覆盖范围比 `retry_exceptions` 大。但仍然**不覆盖**：

- UDF 应用层异常（`ValueError`、`KeyError`、`RuntimeError` 等业务异常）
- `ObjectLostError`、`NodeDiedError`、`ObjectReconstructionFailedError`、`OwnerDiedError`（这些都直接继承 `RayError`，不是 `RaySystemError` 子类）
- `OutOfMemoryError`（独立类，独立预算）

### 13.2 "RaySystemError 都包含哪些异常？"

`exceptions.py:495-509`：

```python
class RaySystemError(RayError):
    """Indicates that Ray encountered a system error.
    This exception can be thrown when the raylet is killed."""
```

| 触发场景 | 是否 RaySystemError |
|---|---|
| Raylet 进程被 kill | ✅ |
| Worker 反序列化任务参数失败 | ✅ |
| Pickle/Unpickle 失败、protobuf 解析失败 | ✅ |
| 内部 RPC 错误（部分） | ✅ |
| `WorkerCrashedError`（Worker 段错误/被 kill） | ❌ 独立类，继承 RayError |
| `OutOfMemoryError`（Memory Monitor kill） | ❌ 独立类 |
| `NodeDiedError`（节点失联） | ❌ 独立类 |
| `ObjectLostError` / `ObjectReconstructionFailedError` / `OwnerDiedError` | ❌ 都直接继承 `RayError`/`ObjectLostError` |
| `ReferenceCountingAssertionError` | ❌ |

`exceptions.py:619` 明确：

```python
class NodeDiedError(RayError):
    """Indicates that the node is either dead or unreachable."""
```

而非继承 `RaySystemError`。

### 13.3 "OOM 被 kill 也是无限重试吗？`task_oom_retries` 是什么？"

**OOM 重试预算与普通重试预算独立**。

#### 配置定义

`src/ray/common/ray_config_def.h:100`：

```cpp
RAY_CONFIG(uint64_t, task_oom_retries, -1)   // 默认 -1 = 无限
```

#### 是否每个 task 一份预算？—— 是的

`task_manager.h:588`：

```cpp
int32_t num_oom_retries_left_;   // TaskEntry 的成员（每个 task 独立）
```

`task_manager.cc:243-244`（task 创建时初始化）：

```cpp
int32_t max_oom_retries =
    (max_retries != 0) ? RayConfig::instance().task_oom_retries() : 0;
```

每个 task 在 `AddPendingTask` 时单独分配自己的 `num_oom_retries_left_`，**不是全局共享、不是 worker 维度**。

#### OOM 失败处理逻辑

`task_manager.cc:1152-1186`（`FailOrRetryPendingTask` 内）：

```cpp
bool task_failed_due_to_oom = error_info.error_type() == OUT_OF_MEMORY;
if (task_failed_due_to_oom) {
    if (num_oom_retries_left > 0) {
        will_retry = true;
        num_oom_retries_left--;            // 消耗 1 次 OOM 预算
    } else if (num_oom_retries_left == -1) {
        will_retry = true;                  // -1 = 无限，不递减
    } else {
        RAY_CHECK(num_oom_retries_left == 0);  // 用尽 → 不重试
    }
} else {
    // 非 OOM 错误走 num_retries_left 路径
    if (num_retries_left > 0 || (is_preempted && IsRetriable())) { ... }
}
```

**关键点**：

1. **OOM 和普通失败用两套独立预算**：`num_oom_retries_left_` vs `num_retries_left_`
2. OOM 失败**只**消耗 OOM 预算，不消耗普通 retry 预算
3. 反过来普通失败也不消耗 OOM 预算
4. 默认配置下 Ray Data：`max_retries=-1`、`task_oom_retries=-1` → **两个预算都是无限**

#### 风险点

部分内部部署/集群配置会**显式覆盖** `RAY_task_oom_retries=15` 等有限值，防止反复 OOM 浪费资源。如果用户/运维设置了有限值，OOM 预算会真实耗尽 → `OutOfMemoryError` 永久失败 → error block。

排查 error block 时**必须先确认集群是否覆盖了 `RAY_task_oom_retries` 默认值**。

### 13.4 "lineage 还在、资源够，重建一定会成功吗？"

**不一定**。即使在最理想的前提下，仍有以下确定性失败路径：

#### 路径分类

`object_recovery_manager.cc:140-188` → `task_manager.cc:354-410`：

```cpp
auto it = submissible_tasks_.find(task_id);
if (it == submissible_tasks_.end()) {
    return OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED;
}
if (task_entry.is_canceled_) {
    return OBJECT_UNRECONSTRUCTABLE_TASK_CANCELLED;
}
if (IsStreamingGenerator && status==SUBMITTED_TO_WORKER && num_retries_left_==0) {
    return OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED;
}
```

| 场景 | 失败原因 | 在 Ray Data 中是否可能 |
|---|---|---|
| **Task spec 被 GC** | `submissible_tasks_.find() == end()` | ✅ `max_lineage_bytes` 超限触发 |
| **依赖对象重建递归失败** | 上游 `RecoverObject(dep)` 失败 | ✅ 外部数据源不可达等 |
| **Streaming generator 最后一个 attempt 正在跑** | `num_retries_left_ == 0` | ❌ Ray Data `-1` 永远不触发 |
| **task_entry.is_canceled_** | 显式取消 | ⚠️ 罕见（用户中断、上游算子提前退出） |
| **Actor task + actor 已死** | `ResubmitTask` 返回 ACTOR_DIED 类错误 | ✅ `max_restarts` 用尽 |

### 13.5 "`should_queue_generator_resubmit = true` 是不是把重建排队，等当前 attempt 跑完后再决定是否重新提交？"

**这个表述不准确**。准确语义如下。

源码 `task_manager.cc:394-398` 和 `normal_task_submitter.cc:842-850`：

```cpp
// task_manager.cc
if (should_queue_generator_resubmit) {
    return queue_generator_resubmit_(spec)
               ? std::nullopt
               : OBJECT_UNRECONSTRUCTABLE_TASK_CANCELLED;
}

// normal_task_submitter.cc
bool NormalTaskSubmitter::QueueGeneratorForResubmit(const TaskSpecification &spec) {
    absl::MutexLock lock(&mu_);
    if (cancelled_tasks_.contains(spec.TaskId())) {
        return false;   // ← 唯一返回 false 的分支
    }
    generators_to_resubmit_.insert(spec.TaskId());
    return true;
}
```

正确语义：

- 当前 streaming generator task 还在 `SUBMITTED_TO_WORKER` 状态执行中，丢的对象重建请求被**推迟**到当前 attempt 结束之后处理
- 推迟到队列 `generators_to_resubmit_`，**只有一条放弃路径**：在等待期间 task 被显式 cancel（用户 ctrl+c、超时取消、上游算子提前结束）→ 进入 `cancelled_tasks_` → 返回 false → `TASK_CANCELLED` → 永久失败 → error block
- 否则，当前 attempt 跑完时无论成功/失败，都会重新提交一次

**不是"决定要不要"，而是"延迟触发；这段延迟窗口里若被 cancel 就作废"**。Ray Data 正常作业里 task 不会被显式 cancel，这条放弃路径在生产上极少触发。

#### "最后一个 attempt 正在跑"分支为什么对 Ray Data 不触发

```cpp
// task_manager.cc:374
if (task_entry.spec_.IsStreamingGenerator() &&
    task_entry.GetStatus() == rpc::TaskStatus::SUBMITTED_TO_WORKER) {
    if (task_entry.num_retries_left_ == 0) {
        return MAX_ATTEMPTS_EXCEEDED;
    }
    should_queue_generator_resubmit = true;
}
```

判定条件是 `num_retries_left_ == 0`——这是给**显式设置了有限 `max_retries`** 的 task 准备的（如 `@ray.remote(max_retries=3)`）。Ray Data 默认 `-1`，**这条分支永远不会进入**，会走到下一行 `should_queue_generator_resubmit = true`。

### 13.6 "上游 task 无限重试也都失败"的真实含义

**不是"重试了无数次都失败"，而是"压根触发不到无限重试"**。

`object_recovery_manager.cc:140-188` 重建当前对象时：

```cpp
auto error_type_optional = task_manager_.ResubmitTask(task_id, &task_deps);
if (!error_type_optional.has_value()) {
    for (const auto &dep : task_deps) {
        auto error = RecoverObject(dep);   // 递归重建依赖
        if (error.has_value()) {
            recovery_failure_callback_(dep, *error, /*pin_object=*/false);
        }
    }
}
```

#### 路径 1：上游 task spec 已被 lineage GC（最常见）

```cpp
auto it = submissible_tasks_.find(task_id);
if (it == submissible_tasks_.end()) {
    return OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED;
}
```

`submissible_tasks_` 是 owner 维护的 task spec 表，受 `max_lineage_bytes`（默认 1GB）限制。长跑作业里上游 read task 早已完成且数据已被消费，spec 被 GC → 重建找不到任务，**根本没机会"重试"**。这不是"重试失败"，是"连重试入口都没了"。

#### 路径 2：上游 task 的 UDF 抛业务异常

read 算子（如 `read_parquet`、`read_datasource`）实际执行用户提供的 datasource 读取逻辑。重建期间触发：

- `FileNotFoundError`（外部文件被删除）
- `PermissionDeniedError`（凭证过期）
- `pyarrow.lib.ArrowInvalid`（schema 变了）
- 自定义 datasource 抛 `RuntimeError`

这些是**应用层异常**，`retry_exceptions=False` 默认不重试 → 上游 task **每次重新执行都立即失败**。`max_retries=-1` 只覆盖系统错误，不覆盖应用错误。表面"无限重试"，实际是"对系统错误无限重试，对业务错误一次都不重试"。

#### 路径 3：递归向下还有依赖也失败

read 之上可能还有 map/filter 算子。当前对象 → input block → input block 的 task 依赖另一个 block → 那个 block 也丢了。任何一层失败整条链都失败，且失败按 `recovery_failure_callback_` 直接传播，**不会跨层"再重试"**。

#### 路径 4：上游是 actor task + actor 已死

actor 死了 + `max_restarts` 用尽，`ResubmitTask` 检测到 actor 不在，直接返回 `ACTOR_DIED`，没有"重试"动作。

---

## 14. Ray Data 产生 Error Block 的完整场景汇总（修订版）

异常到达 `process_completed_tasks` 的 `:606` 或 `:695`，意味着 Ray Core 重试已用尽或不适用。**Ray Data 默认配置（`max_retries=-1`、`max_task_retries=-1`、`task_oom_retries=-1`）下**，能真正触达 except 的场景如下。

### 14.1 Task 层永久失败（到 `:606`）

| 场景 | 触发原因 | 默认配置下风险 |
|---|---|---|
| **A1. UDF 应用层异常** | `retry_exceptions=False`（stateless）、`actor_task_retry_on_errors=False`（actor）默认不重试任何应用异常。UDF 抛 `ValueError`/`KeyError`/`RuntimeError` 等 → 立即永久失败 | 🔴 高 |
| **A2. OOM 被 Memory Monitor kill 超过 `task_oom_retries`** | OOM 重试有独立预算，默认 `-1` 无限。但**集群可能覆盖**为有限值（如 15）。耗尽后抛 `OutOfMemoryError` 永久失败 | 🟡 取决于集群配置 |
| **A3. Actor 创建失败 + actor_init 重试用尽** | `actor_init_max_retries=3`（且需 `actor_init_retry_on_errors=True` 才生效，默认 False 即不重试）。actor 起不来 → in-flight task 永久失败 | 🟢 actor 启动有问题才出现 |
| **A4. Actor 已死且不可重启** | actor `max_restarts` 用尽。task 即便 `max_task_retries=-1` 也无 actor 可调度，重提交直接失败 | 🟡 actor 反复 crash |
| **A5. Task 被显式取消** | 上游算子提前退出、Driver 中断、超时取消 | 🟢 罕见 |

### 14.2 Object 层失败（到 `:695`，少数情况到 `:606`）

Task 成功完成 yield 了 block，之后 object 在 plasma 中丢失（节点宕机、内存压力 eviction），lineage reconstruction 失败：

| 场景 | 触发原因 | 默认配置下风险 |
|---|---|---|
| **B1. Task spec 被 lineage GC** | `max_lineage_bytes`（默认 1GB）超限，Ray 主动 GC 老 task spec 防止 Driver 内存爆。`submissible_tasks_.find()` 返回 end → `MAX_ATTEMPTS_EXCEEDED` | 🔴 长跑大规模作业的最主要来源 |
| **B2. 递归依赖重建失败** | 重建当前 task 需重新拿 input block，input block 来自上游 read 算子。如果外部数据源已不可读（文件删了、对象存储凭证失效、表 schema 变了、网络隔离），上游 task 无限重试也都失败 → 递归向下传播失败 | 🟡 数据源稳定性 |
| **B3. Actor task 的对象 + actor 已死** | actor task 产出的对象重建需要 actor，actor 不在了就重建不了 | 🟡 同 A4 |

### 14.3 Driver 自身死亡（不会产生 error block，是整体失败）

`OwnerDiedError` 在 Ray Data 里几乎不会作为 error block 出现，因为 Driver = Owner。Driver 死了整个作业直接终止，根本到不了 except 分支。

### 14.4 按"业务侧 UDF 干净"前提过滤后的真实风险排序

如果业务 UDF 不会抛异常，A1（最常见来源）被排除。剩下的真实风险按出现频率排序：

| 排序 | 场景 | 触发条件 |
|---|---|---|
| 🔴 **高频** | **B1 lineage GC** | 长跑作业 + 大量 task + lineage 占用超过 `max_lineage_bytes`（默认配置下必发） |
| 🟡 **中频** | **B2 外部数据源不可达** | read 算子的源（HDFS/S3/数据库）出问题 |
| 🟡 **中频** | **A4 Actor 死亡耗尽 max_restarts** | 用 actor pool（`map_batches` with actor）+ actor 反复 crash |
| 🟢 **低频** | **A2 OOM 超 OOM 预算** | **仅当集群显式覆盖** `RAY_task_oom_retries` 为有限值；默认 -1 时不会触发 |
| 🟢 **低频** | A3 actor_init 失败 | actor 启动逻辑本身有问题 |
| 🟢 **低频** | A5 显式取消 | 用户中断、上游提前退出 |

---

## 15. Owner 机制在 Ray Data 中的具体含义

### 15.1 Owner 是 Driver

Ray Data 场景下，Driver 调 `map_task.remote()` 提交所有 map task → 所有 block_ref 和 meta_ref 的 Owner 都是 Driver：

```cpp
// task_manager.cc:293
reference_counter_.AddOwnedObject(return_id, ..., caller_address);  // 提交方=Owner
```

### 15.2 推论

- **Driver 不死，`OwnerDiedError` 在 Ray Data 里基本不会出现**
- Driver 进程内存压力过大被 OOM/被 kill 会直接让整个作业终止，**根本轮不到产生 error block**
- 唯一例外是用 `ray.put` 在 worker 内部传递 ref 给其他 worker（Ray Serve、嵌套 task），Ray Data 标准算子流不涉及

---

## 16. 如何定位与确认 Error Block 的产生原因

### 16.1 日志路径

`task_manager.cc` 是 **Core Worker C++ 层**代码，每个 Ray 进程（Driver、Worker、Actor）都内嵌了一个 Core Worker。`RAY_LOG` 输出**写到调用它的进程的日志**。

`TaskManager` 运行在 task 提交方（Owner）：

| 调用点 | 进程 | 日志位置 |
|---|---|---|
| Driver `.remote()` 提交 map task → `AddPendingTask`、`ResubmitTask`、`FailOrRetryPendingTask` | **Driver 进程** | Driver 所在节点 |
| Driver 的 `ObjectRecoveryManager` 调 `ResubmitTask` 触发 lineage 重建 | **Driver 进程** | Driver 所在节点 |
| Worker 内嵌套提交子 task（Ray Data 一般没有） | 那个 Worker 进程 | Worker 所在节点 |

Ray Data 主流场景下 **`task_manager.cc` 的日志几乎全在 Driver 节点**。

#### Driver 节点日志结构

Ray 日志默认在 `/tmp/ray/session_latest/logs/` 下：

```
/tmp/ray/session_latest/logs/
├── driver-<job_id>.log              # Python logger 输出
├── raylet.out / raylet.err          # Raylet 日志
├── gcs_server.out / gcs_server.err  # GCS 日志
├── python-core-driver-*.log         # ★ Driver Core Worker 的 C++ 日志
├── python-core-worker-*.log         # 各 Worker 的 C++ 日志
├── worker-<worker_id>-*.out / *.err
```

`task_manager.cc` 的 `RAY_LOG(INFO)` 走 C++ glog → **`python-core-driver-<driver_id>_<pid>.log`**（Driver 端）。

Python 层的 `streaming_executor_state.py` 中 `logger.error/exception` 输出 → **`driver-<job_id>.log`**。

注意事项：

- 自定义日志目录（启动 `ray start --temp-dir=/path`）→ 路径替换成 `/path/session_latest/logs/`
- KubeRay/容器化环境 → 进入 Driver Pod 看 `/tmp/ray/session_latest/logs/`

### 16.2 Error Block 出现的标志日志

#### Python 层（`driver-<job_id>.log`）

```
An exception was raised from a task of operator "<op_name>". [num_errored_blocks=N]
Ignoring this exception with remaining max_errored_blocks=...
```

或：

```
An exception was raised from a task of operator "<op_name>". [num_errored_blocks=N]
Dataset execution will now abort. To ignore this exception and continue, set
DataContext.max_errored_blocks.
```

这是 error block 最直接的标志，紧跟着是异常堆栈。

#### C++ 层（`python-core-driver-*.log`）

不同失败原因对应不同关键字：

| 关键日志 | 含义 | 对应 14 节场景 |
|---|---|---|
| `Resubmitting task that produced lost plasma object, attempt #N` | lineage 重建被触发，正在重新提交 | 中间状态 |
| `Cannot recover object: OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED` | task spec 被 GC 或重试预算耗尽 | **B1（lineage GC）** |
| `Cannot recover object: OBJECT_UNRECONSTRUCTABLE_LINEAGE_EVICTED` | lineage 信息已被驱逐 | **B1（lineage GC）** |
| `Cannot recover object: OBJECT_UNRECONSTRUCTABLE_TASK_CANCELLED` | task 被显式取消 | A5 |
| `Cannot recover object: OBJECT_UNRECONSTRUCTABLE_RETRIES_DISABLED` | task 创建时禁用了重试 | 罕见 |
| `Cannot recover dependency: ...` | 递归重建依赖失败 | **B2** |
| `task <id> failed due to oom. There are N oom retries remaining` | OOM 失败，正在消耗 OOM 预算 | A2 进行中 |
| `task <id> retries left: 0, oom retries left: 0` | 两个预算都耗尽 | A2 已发生 |

### 16.3 排查命令

#### 步骤 1：确认 error block 出现及总数

```bash
grep -E "An exception was raised from a task of operator" \
    /tmp/ray/session_latest/logs/driver-*.log
```

#### 步骤 2：判断是 Task 层失败（A）还是 Object 层失败（B）

查看错误堆栈：

| 堆栈中包含 | 大概率分类 |
|---|---|
| `ObjectReconstructionFailedError` / `ObjectLostError` | B 类（Object 层） |
| `OutOfMemoryError` | A2 |
| `RayActorError` / `ActorDiedError` | A4 |
| 业务异常（`ValueError` 等） | A1 |
| `RaySystemError` | 基础设施异常（理论上应被重试，看为何到此） |

#### 步骤 3：查 lineage 重建相关日志（确认 B1）

```bash
# 是否有 task spec 被 GC 的迹象
grep -E "OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED|OBJECT_UNRECONSTRUCTABLE_LINEAGE_EVICTED" \
    /tmp/ray/session_latest/logs/python-core-driver-*.log

# 查看 lineage 总占用是否接近上限
grep -E "lineage_footprint|max_lineage_bytes" \
    /tmp/ray/session_latest/logs/python-core-driver-*.log
```

如果出现 `MAX_ATTEMPTS_EXCEEDED` 或 `LINEAGE_EVICTED`，**强烈指向 B1**。

#### 步骤 4：查 OOM 重试预算（确认 A2）

```bash
# 查 OOM 重试相关日志
grep -E "failed due to oom|oom retries left" \
    /tmp/ray/session_latest/logs/python-core-driver-*.log

# 查集群是否覆盖了 task_oom_retries
grep -rE "task_oom_retries|RAY_task_oom_retries" \
    /tmp/ray/session_latest/logs/ /etc/ray/ 2>/dev/null
```

如果看到 `oom retries left: 0`，**直接确认 A2**。

#### 步骤 5：查节点宕机/内存压力（B 类的诱因）

```bash
# Memory Monitor kill 记录
grep -E "Workers \(tasks / actors\) killed due to memory pressure" \
    /tmp/ray/session_latest/logs/raylet.*

# 节点 dead 记录
grep -E "Node .* failed|NodeDeath|node is dead" \
    /tmp/ray/session_latest/logs/gcs_server.*
```

#### 步骤 6：查上游数据源失败（确认 B2）

业务异常类型一般是 `FileNotFoundError`、`pyarrow.lib.ArrowInvalid` 等，会出现在 `driver-<job_id>.log` 的 error block 异常堆栈中。

### 16.4 是否可以"程序化"判断 task spec 被 GC 触发的 error block？

**可以，通过异常类型 + C++ 日志组合判断**：

1. Python 层捕获到 `ObjectReconstructionFailedError`，子类型为 `OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED` 或 `OBJECT_UNRECONSTRUCTABLE_LINEAGE_EVICTED`
2. C++ 日志同时出现 `Cannot recover object: OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED`
3. **且**作业运行时间长、task 数量多、未发生节点宕机

满足以上三条 → 高度确认是 B1（lineage GC）。

可在 Python 层捕获时通过 `e.__class__.__name__` 和 `str(e)` 提取错误类型来记录 metric：

```python
except Exception as e:
    error_type = type(e).__name__
    error_msg = str(e)
    # 记录到 metrics
```

`exceptions.py:783` 中 `ObjectReconstructionFailedError` 的子类型枚举见 `common.proto:212-281`：

```
OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED  = 12
OBJECT_UNRECONSTRUCTABLE_LINEAGE_EVICTED        = 13
OBJECT_UNRECONSTRUCTABLE_PUT                    = 27
OBJECT_UNRECONSTRUCTABLE_RETRIES_DISABLED       = 28
OBJECT_UNRECONSTRUCTABLE_BORROWED               = 29
OBJECT_UNRECONSTRUCTABLE_LOCAL_MODE             = 30
OBJECT_UNRECONSTRUCTABLE_REF_NOT_FOUND          = 31
OBJECT_UNRECONSTRUCTABLE_TASK_CANCELLED         = 32
OBJECT_UNRECONSTRUCTABLE_LINEAGE_DISABLED       = 33
```

---

## 17. 总结：核心结论速查

1. **"task/actor 无限重试" ≠ "永远不会 error block"**——重试覆盖的是 task 层的基础设施错误，**对象层失败（B 类）和应用层异常（A1）不被覆盖**

2. **`max_retries=-1` 不递减**——`num_retries_left_` 永远保持 -1，所以"用尽"不存在；但这并不阻止其他失败路径

3. **OOM 有独立的预算 `task_oom_retries`**，默认 -1（无限），但**集群可能覆盖为有限值**——必须先确认实际配置

4. **`RaySystemError` 范围窄**：只覆盖 raylet kill、序列化失败等少数场景；`NodeDiedError`、`ObjectLostError`、`OutOfMemoryError` 都不是 `RaySystemError` 子类

5. **业务 UDF 干净 + 默认配置的情况下，Error Block 几乎全部来自**：
   - 🔴 **lineage GC**（`max_lineage_bytes` 超限）—— 长跑大作业必发
   - 🟡 **上游外部数据源不可达**
   - 🟡 **actor 重启耗尽**（仅用 actor pool 时）
   - 🟢 OOM 超有限预算（仅在集群覆盖了 `task_oom_retries` 默认值时）

6. **Owner = Driver**（Ray Data 场景），所以 `OwnerDiedError` 几乎不会作为 error block 出现

7. **Error Block = 该 block 数据确认丢失**（除了第 12 节那种 ray.wait/ray.get 之间的竞争窗口"假阳性"），**不会被任何机制恢复**

8. **生产排查路径**：
   - 先看 `driver-<job_id>.log` 找 `An exception was raised from a task` 标志
   - 再看堆栈判 A 类还是 B 类
   - 长跑作业重点查 `python-core-driver-*.log` 的 `OBJECT_UNRECONSTRUCTABLE_*` 关键字
   - 集群配置必查 `RAY_task_oom_retries` 是否被覆盖

---

## 18. 实战案例：OOM 导致 Error Block 完整排查记录

### 18.1 现象

生产环境 `kling-ray` pipeline 出现 error block，堆栈如下：

```
2026-06-17 21:42:46,478 - utils.patch_interleave_dispatch - ERROR - 
An exception was raised from a task of operator "FlatMap(ClipMergeMapper)". 
[num_errored_blocks=1] Ignoring this exception with remaining max_errored_blocks=9993.

Traceback (most recent call last):
  File "utils/patch_interleave_dispatch.py", line 308, in _patched_scheduling_loop_step
  File "utils/patch_watchdog_block.py", line 441, in _patched_on_data_ready
    return _orig_on_data_ready(self, max_bytes_to_read)
  File ".../physical_operator.py", line 201, in on_data_ready
    raise ex from None
  File ".../physical_operator.py", line 196, in on_data_ready
    ray.get(self._pending_block_ref)
ray.exceptions.OutOfMemoryError: Task was killed due to the node running low on memory.
Memory on the node ... was 121.83GB / 128.00GB (0.951807), 
which exceeds the memory usage threshold of 0.95. 
Ray killed this worker because it was the most recently scheduled task.

Top 10 memory users:
PID     MEM(GB) COMMAND
3000061 9.53    ray::QwenVLCPUPreprocessActor.preprocess_video
2971271 9.47    ray::QwenVLCPUPreprocessActor.preprocess_video
... (×10 个 actor，合计 ~85GB)
```

**关键观察**：

- 被杀的算子是 `FlatMap(ClipMergeMapper)`，仅占 0.51GB
- 真正吃内存的是 10 个 `QwenVLCPUPreprocessActor`，合计 ~85GB
- 节点总内存 95.18%，超过 0.95 阈值

### 18.2 第一层分析：为什么 FlatMap 被杀？

Ray Memory Monitor 的 kill 策略是 **"杀最近调度的 task"**（参见 `node_manager.cc` memory monitor 实现），**不是杀最大内存占用者**。

| 事实 | 解释 |
|------|------|
| FlatMap 仅 0.51GB | 被杀的是无辜牺牲品 |
| 为什么是它 | 节点内存超阈值瞬间，恰好是最后被调度的 task |
| 为什么不杀 actor | Memory Monitor 倾向于杀 stateless task（actor 重启代价高） |
| 真元凶 | QwenVLCPUPreprocessActor 持续占 ~85GB 不释放 |

### 18.3 第二层困惑：默认 -1 无限重试，为什么还会 error block？

用户提供的额外日志：

```
(raylet) Task _map_task failed due to oom. There are infinite oom retries remaining, 
so the task will be retried. Error: Task was killed due to the node running low on memory.
```

raylet 明确说 **infinite retries remaining**，证明 `task_oom_retries=-1` 没被改。这与 Driver 端抛 `OutOfMemoryError` 触发 error block **同时成立**——产生了第一个困惑点。

#### Ray Data 默认配置确认

| 配置 | 默认值 | 来源 |
|------|--------|------|
| `max_retries`（stateless task） | -1 | `ray/data/_internal/remote_fn.py:37` |
| `max_task_retries`（actor task） | -1 | `actor_pool_map_operator.py:576` |
| `task_oom_retries`（OOM 独立预算） | -1 | `src/ray/common/ray_config_def.h:100` |
| `retry_exceptions` | False | `_add_system_error_to_retry_exceptions` |
| `actor_task_retry_on_errors` | False | `context.py:211` |
| `max_errored_blocks` | 0 | `context.py:225` |

按理说默认配置 + 无限 OOM 重试，**OOM 不应该产生 error block**。

### 18.4 排查路径 1：是否有人改了配置（已排除）

**检查项**：

```bash
# 1. 环境变量是否被覆盖
cat /proc/<driver_pid>/environ | tr '\0' '\n' | grep -iE "oom_retries|max_retries"
grep -rE "RAY_task_oom_retries|task_oom_retries" /etc/ray/ ./*.yaml ./*.sh

# 2. 业务代码是否设置了 max_retries=0
rg -n "max_retries\s*=\s*0" --type py
rg -n "max_retries\s*:\s*0" --type py

# 3. 算子 ray_remote_args
rg -n "ClipMergeMapper" --type py -C 5
rg -n "ray_remote_args|max_retries|task_oom_retries" pipeline/
```

**结论**：

- 用户业务代码 `multishot_video_classifier_pipeline_checkpoint.py` 未修改 retry 参数
- raylet 日志确认 `infinite retries remaining`
- **配置无被覆盖**，排除 A 类（task 层失败）

特别注意 `task_manager.cc:243`：

```cpp
int32_t max_oom_retries =
    (max_retries != 0) ? RayConfig::instance().task_oom_retries() : 0;
```

**如果 `max_retries=0`，OOM 预算被强制为 0**，一次 OOM 就 error block。但本案例已确认 `max_retries=-1`，此分支不适用。

### 18.5 排查路径 2：patch 文件是否构造异常（已排除）

堆栈顶层两帧来自自定义 patch，最初怀疑 patch 主动 raise `OutOfMemoryError`：

```
patch_interleave_dispatch.py:308    ← 接管 _scheduling_loop_step
patch_watchdog_block.py:441         ← 接管 on_data_ready
physical_operator.py:196 ray.get   ← Ray 原生
```

#### `patch_interleave_dispatch.py` 审查

是 `_scheduling_loop_step` 的重新实现，except 分支与 Ray 原生**完全等价**：

```python
try:
    bytes_read = task.on_data_ready(max_bytes_to_read_per_op.get(state, None))
except Exception as e:
    errored_blocks_per_op[state] += 1
    num_errored_blocks += 1
    # ...与 streaming_executor_state.py:606 行为一致
    if should_ignore:
        logger.error(error_message, exc_info=e)
    else:
        raise e from None
```

**结论**：patch 只是**捕获并记录**异常，未构造 `OutOfMemoryError`。

#### `patch_watchdog_block.py` 审查

`_patched_on_data_ready` 只截 `TaskCancelledError`：

```python
def _patched_on_data_ready(self, max_bytes_to_read):
    try:
        return _orig_on_data_ready(self, max_bytes_to_read)
    except ray.exceptions.TaskCancelledError:    # ← 只处理这一类
        _metrics["cancellations_handled"] += 1
        ...
        return 0
    # OutOfMemoryError 不在 catch 列表 → 直接透传
```

**结论**：patch 未拦截或构造 OOM 异常，原生 `ray.get` 透传过来。

### 18.6 真正答案：Streaming Generator + OOM Retry 的陈旧 ObjectRef 竞争

#### 时间线

```
T1: Worker 执行中，yield block_4
    → ObjectID_4(attempt=0) 注册到 ObjectRefStream

T2: Driver 调度循环 ray.wait → slot 4 ready

T3: Driver 调 _next_sync()
    → 返回 ObjectID_4(attempt=0)
    → 缓存到 self._pending_block_ref     ← 关键：持有旧 attempt 的 ref

T4: ⚡ Worker 被 OOM kill

T5: Ray Core 处理：
    - task_oom_retries=-1 → will_retry=true
    - 不调用 FailPendingTask（不写错误对象到流）
    - BUT: streaming generator 已发出去的 ObjectID_4(attempt=0) 
      会被 raylet 标记为 "worker died with OOM" 错误对象
      （worker 死亡导致其在 plasma 中的 in-flight 对象不可用）

T6: Ray 重试 task → 新 worker，attempt=1
    新 worker 从头 yield block_0, block_1, ...
    InsertToStream 跳过已消费的 slot（< next_index_）
    继续 yield block_4 → ObjectID_4(attempt=1) 写入流的同一槽位
    （ObjectID 由 task_id+attempt+index 计算，不同 attempt 不同 ID）

T7: Driver 调 ray.get(self._pending_block_ref)
    → 这是 attempt=0 的旧 ID，不是 attempt=1 的新 ID
    → 拿到 worker-death 错误对象 → 抛 OutOfMemoryError
```

#### 核心机制

**`task_oom_retries=-1` 让 task 整体被无限重试**，但 Driver 已把旧 attempt 的 ObjectRef 缓存进 `_pending_block_ref`。**重试产生新 ObjectRef 写入新 slot，但旧 ref 永远是失败的**。

`raylet` 日志的 "will be retried" 是 task 层面真的在重试，与 Driver 端拿到陈旧 ref 抛 OOM **同时成立、不矛盾**。

#### 两条并行事实

| 事件 | 真假 | 来源 |
|------|------|------|
| Task 在 Ray Core 层无限重试 | ✅ 真 | raylet 日志 `infinite retries remaining` |
| 该 block 的具体 ObjectRef 被标为 OOM 失败 | ✅ 真 | Driver 端 ray.get 抛错 |
| 重试**恢复**了那个 block | ❌ 假 | 重试产出新 ObjectID，旧 ref 不变 |
| 这导致一个 error block | ✅ 真 | 但 task 整体最终会完成 |

#### 与第 12 节的关系

这是第 12 节"边缘竞争条件"的一个 OOM 特例。第 12 节描述的是 ray.wait/ray.get 之间对象被删除；本案例是 worker 死亡瞬间对象被标记失败。两者本质都是**陈旧 ref 竞争窗口**。

### 18.7 为什么 retry=-1 反而更糟（如果没有 race）

假设 race 不存在的纯净场景下：

| 配置 | 表现 | 是否解决问题 |
|------|------|-------------|
| `task_oom_retries=有限`（如 15） | 快速耗尽 → error block，数据丢失 | ❌ |
| `task_oom_retries=-1` | 无 error block，但 task 永远卡在反复 OOM kill | ❌ 更糟（隐蔽故障） |
| **降低 actor 内存占用** | 不再触发 OOM kill | ✅ 唯一根治路径 |

调 retry 配置只是改变**故障呈现形式**：

- 有限预算 → 显式 error block（容易报警发现）
- 无限预算 → 隐蔽死循环（吞吐骤降但无错误日志）

而本案例叠加了 race 窗口，**即使无限预算也会泄漏 OOM 异常**到 Driver 端。

### 18.8 完整排查命令清单

#### Step 1：确认 error block 现象

```bash
LOG_DIR=/tmp/ray/session_latest/logs

# error block 标志日志
grep -E "An exception was raised from a task of operator" \
    $LOG_DIR/driver-*.log
```

#### Step 2：识别异常类型

| 堆栈关键字 | 分类 | 章节 |
|-----------|------|------|
| `OutOfMemoryError` | OOM 类 | 本节 |
| `ObjectReconstructionFailedError` / `ObjectLostError` | Object 层 | 14.2 |
| `RayActorError` / `ActorDiedError` | Actor 死亡 | 14.1 A4 |
| 业务异常（`ValueError` 等） | 应用层 | 14.1 A1 |

#### Step 3：确认 OOM 重试预算配置

```bash
# 看 raylet 日志中 retries remaining 实际数值
grep -E "failed due to oom|oom retries (left|remaining)" \
    $LOG_DIR/raylet.* $LOG_DIR/python-core-driver-*.log

# 期望看到：
#   "infinite oom retries remaining"   → task_oom_retries=-1（默认）
#   "There are 14 oom retries remaining" → 被覆盖为有限值（15）
#   "There are 0 oom retries remaining"  → 已耗尽
```

#### Step 4：检查配置覆盖

```bash
# 环境变量
cat /proc/<driver_pid>/environ | tr '\0' '\n' | grep -iE "oom_retries|max_retries"
grep -rE "RAY_task_oom_retries|task_oom_retries" /etc/ray/ ~/.ray/ ./*.{yaml,sh,py} 2>/dev/null

# 业务代码 max_retries=0
rg -n "max_retries\s*[=:]\s*0" --type py

# 算子 ray_remote_args
rg -n "ray_remote_args" --type py -A 3
```

#### Step 5：确认是 patch 还是 Ray 原生抛异常

```bash
# 看堆栈顶层是不是自定义 patch
# 如果是，读 patch 文件看 except 分支处理什么异常
sed -n '420,470p' utils/patch_watchdog_block.py
sed -n '290,320p' utils/patch_interleave_dispatch.py

# 关键判断：patch 是否
#   1) 截获 OOM 并重新抛出？  → 是 patch 责任
#   2) 让 OOM 透传？           → 是 Ray 原生路径
```

#### Step 6：验证内存压力源

```bash
# 看哪些进程占内存
ps aux --sort=-%mem | head -20

# 看 raylet 的 OOM kill 决策日志
grep -B 2 -A 10 "memory pressure\|killed.*memory" \
    $LOG_DIR/raylet.out

# 看 actor 实际并发数
ray status | grep -i actor
ps aux | grep "ray::QwenVLCPUPreprocessActor" | wc -l
```

#### Step 7：确认陈旧 ref 竞争（高级）

```bash
# 找该 task 的完整生命周期
TID="<从堆栈或 driver-*.log 中找到的 task id>"
grep "$TID" $LOG_DIR/{raylet.*,python-core-driver-*.log}

# 期望看到：
#   raylet: "infinite oom retries remaining, will be retried"
#   driver: ray.get 抛 OutOfMemoryError
#   两者时间相近 → 确认是 race 窗口
```

### 18.9 修复方案与验证

#### 根治：降低内存压力

```python
# 方案 1：降低 actor 并发数
# 原配置：10 个 QwenVLCPUPreprocessActor 并发
# 调整：从 10 降到 6
.map_batches(
    QwenVLCPUPreprocessActor,
    concurrency=6,  # 从 10 调整
    num_cpus=...,
)

# 方案 2：检查 actor 内存释放
class QwenVLCPUPreprocessActor:
    def preprocess_video(self, video):
        result = ...
        # 主动释放大对象
        del intermediate_tensor
        gc.collect()
        return result
```

#### 缓解：调度隔离

```python
# 让 FlatMap 和 QwenVL actor 分散到不同节点
.flat_map(
    ClipMergeMapper,
    ray_remote_args={"scheduling_strategy": "SPREAD"},
)
```

#### 缓解：调高内存阈值（治标）

```bash
# 启动 Ray 时设置
export RAY_memory_usage_threshold=0.98   # 默认 0.95
ray start ...
```

#### 防御：保留 max_errored_blocks 容忍

```python
ctx = ray.data.DataContext.get_current()
ctx.max_errored_blocks = 9993  # 已设置，保留
```

#### 验证修复

```bash
# 1. 重启 pipeline 后监控 error block 数量
watch -n 30 "grep -c 'An exception was raised from a task' \
    /tmp/ray/session_latest/logs/driver-*.log"

# 2. 监控节点内存水位
watch -n 5 "free -g | head -2"

# 3. 监控 OOM kill 频次
grep -c "killed due to.*memory" /tmp/ray/session_latest/logs/raylet.*

# 4. 监控 watchdog 触发
grep -c "WATCHDOG.*stalled" /tmp/ray/session_latest/logs/driver-*.log
```

**修复成功标志**：

| 指标 | 修复前 | 修复后期望 |
|------|--------|-----------|
| 节点内存峰值 | >95%（触发阈值） | <90% |
| OOM kill 频次 | 持续出现 | 偶发或消失 |
| Error block 增长率 | 稳步增加 | 接近 0 |
| FlatMap task 平均时延 | 异常波动 | 稳定 |
| Pipeline 整体吞吐 | 抖动/降低 | 稳定 |

### 18.10 本案例核心结论

1. **Ray Data 默认 `task_oom_retries=-1` 不能阻止 OOM 类 error block**——重试针对 task 整体，但 Driver 已缓存的陈旧 ObjectRef 不会被恢复

2. **patch 不一定是元凶**——自定义 patch 出现在堆栈顶层时优先怀疑，但要读源码确认 except 是构造异常还是透传

3. **Memory Monitor 杀的是"最近调度的 task"，不是最大内存占用者**——日志中被杀的小算子常常只是无辜牺牲品

4. **OOM 类 error block 的根因永远在源头**：节点内存压力。调 retry/error_blocks 配置只能改变故障呈现形式，无法消除问题

5. **配置层级排查顺序**：
   - 环境变量 `RAY_task_oom_retries`
   - 业务代码 `max_retries`/`ray_remote_args`
   - 算子链上的 `task_oom_retries` 隐式传递
   - patch 文件对异常的处理逻辑

6. **race 窗口是 streaming generator 的固有限制**：Ray Data 设计上接受这个权衡（用 `max_errored_blocks` 兜底），不可能完全消除

---

## 附录：核心代码文件索引

| 文件 | 关键内容 |
|------|---------|
| streaming_executor_state.py | process_completed_tasks 方法，Phase 1-4 流程，:606/:695 两处 except |
| physical_operator.py | prepare_metadata() (:271-334)，complete_with_metadata() (:354-365)，StopIteration 异常处理 (:314-325) |
| task_manager.cc | ObjectRefStream::PeekNextItem() (:162)，InsertToStream() (:181)，FailOrRetryPendingTask() |
| reference_counter.cc | ResetObjectsOnRemovedNode()，AddOwnedObject() |
| core_worker.cc | Wait()，Get()，周期性恢复循环 |
| object_recovery_manager.cc | RecoverObject()，ReconstructObject()，ResubmitTask() |
| memory_store.cc | GetImpl()，GetRequest::Wait()，condition_variable 等待逻辑 |
| plasma_store_provider.cc | Wait()，GetObjectsFromPlasmaStore() |
| remote_fn.py | `cached_remote_fn` 默认 `max_retries=-1`（:37）；`_add_system_error_to_retry_exceptions`（:59-80）只加 `RaySystemError` |
| actor_pool_map_operator.py | `max_task_retries=-1`（:576）；`actor_init_max_retries`（:668-674）默认 3 且需显式开启 |
| context.py | `actor_task_retry_on_errors=False`（:211）；`max_errored_blocks=0`（:225）默认值 |
| ray_config_def.h | `task_oom_retries`（:100）默认 -1；`max_lineage_bytes` 默认 1GB |
| task_manager.cc | `AddPendingTask`（:237）初始化 OOM 预算；`ResubmitTask`（:354）lineage 重建入口；`FailOrRetryPendingTask`（:1140）双预算决策；`SetupTaskEntryForResubmit`（:412）`-1` 永不递减 |
| normal_task_submitter.cc | `QueueGeneratorForResubmit`（:842）只在 cancel 时返回 false |
| object_recovery_manager.cc | `ReconstructObject`（:140）调 `ResubmitTask` + 递归 `RecoverObject(dep)` |
| exceptions.py | `RaySystemError`（:495）独立类；`NodeDiedError`（:619）、`ObjectLostError`（:631）、`ObjectReconstructionFailedError`（:783）均直接继承 `RayError`/`ObjectLostError` |
| common.proto | `OBJECT_UNRECONSTRUCTABLE_*` 错误码枚举（:212-281） |
