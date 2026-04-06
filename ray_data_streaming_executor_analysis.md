# Ray Data Streaming Executor 调度与对象生命周期深度分析

## 目录

1. [Worker Scaling Benchmark 代码逻辑](#1-worker-scaling-benchmark-代码逻辑)
2. [多算子多 Worker 下的调度开销测试](#2-多算子多-worker-下的调度开销测试)
3. [DataOpTask.on_data_ready 的 block/meta ref 处理与异常逻辑](#3-dataopaskon_data_ready-的-blockmeta-ref-处理与异常逻辑)
4. [is_nil() 含义与 Task 失败时的 ref 状态](#4-is_nil-含义与-task-失败时的-ref-状态)
5. [ray.get 还原原始异常：Dual Exception 机制](#5-rayget-还原原始异常dual-exception-机制)
6. [ObjectLostError vs OwnerDiedError 与 Owner 身份](#6-objectlosterror-vs-ownerdiederror-与-owner-身份)
7. [ObjectID 的确定性创建与 Driver/Worker 协调](#7-objectid-的确定性创建与-driverworker-协调)
8. [Worker/Node 死亡时的失败检测、重试与 Lineage 重建](#8-workernode-死亡时的失败检测重试与-lineage-重建)
9. [Object 创建通知 Owner 与 Owner 发现 Object 地址](#9-object-创建通知-owner-与-owner-发现-object-地址)
10. [MarkTaskReturnObjectsFailed 为什么将已 yield 的 ref 也标记为失败](#10-marktaskreturnobjectsfailed-为什么将已-yield-的-ref-也标记为失败)
11. [Worker 节点宕机后 ray.get() 的完整执行路径](#11-worker-节点宕机后-rayget-的完整执行路径)
12. [Worker 节点宕机后 plasma 数据丢失的完整链路](#12-worker-节点宕机后-plasma-数据丢失的完整链路)
13. [MarkTaskReturnObjectsFailed 为何需要将已 yield 的 ref 标记为错误对象](#13-marktaskreturnobjectsfailed-为何需要将已-yield-的-ref-标记为错误对象)
14. [Caller 的 memory_store_ 中 OBJECT_IN_PLASMA 的来源](#14-caller-的-memory_store_-中-object_in_plasma-的来源)
15. [通用场景下 Owner/Caller/Producer 三方角色与交互](#15-通用场景下-ownercallerproducer-三方角色与交互)
16. [普通对象 vs Streaming Generator ref 在 Caller memory_store_ 处理上的区别](#16-普通对象-vs-streaming-generator-ref-在-caller-memory_store_-处理上的区别)

---

## 1. Worker Scaling Benchmark 代码逻辑

**文件**: `release/nightly_tests/dataset/worker_scaling_benchmark.py`

### 1.1 参数解析 (`parse_args`)

- `--num-workers`: map_batches 使用的 worker 数量
- `--worker-type`: actors 或 tasks
- `--num-scalar-cols` / `--num-array-cols`: 定义每行输出的列数，构造宽 schema
- `--num-operators`: 链式 map_batches 算子数量（worker 池平均分配给每个算子）
- `--blocks-per-worker`: 每个 worker 的输入 block 数

### 1.2 RealisticSchemaUDF 类

- `__init__`: 根据 seed 预生成 scalar 和 array 列的模板值
- `__call__`: 将输入 batch 扩展为包含 `num_scalar_cols` 个 float32 标量列 + `num_array_cols` 个 float32[32] 数组列的宽表，每列用预生成的模板值填充

### 1.3 _disable_operator_fusion()

当 `num_operators > 1` 时，从 Ray Data 物理优化规则中移除 `FuseOperators`，防止多个链式 map_batches 被融合成单个算子，确保拓扑中真正有 N 个算子。

### 1.4 main() 执行流程

1. 构建 `range(num_rows)` 数据集，按 `num_blocks` 分块
2. 根据 worker 类型（actors/tasks）配置 `map_batches` 参数
3. 循环 `num_operators` 次调用 `ds.map_batches(udf)`，形成链式管道
4. `ds.materialize()` 触发执行
5. 收集指标（调度耗时、runtime env、block/row 数、schema 序列化大小等）

### 1.5 核心目的

通过宽 schema 压力测试 `ray.get(meta_ref)` + schema 反序列化的开销，以及多算子拓扑下调度循环的 per-iteration 开销随 worker/算子数增长的变化。

---

## 2. 多算子多 Worker 下的调度开销测试

### 2.1 调度循环的计时拆分

每次调度循环步骤（`_scheduling_loop_step`）被拆成三个计时阶段：

| 阶段 | 指标 | 包含什么 |
|---|---|---|
| **ray.wait** | `ray_wait_duration_s` | `ray.wait()` 等待 task 完成的 I/O 阻塞时间 |
| **on_data_ready** | `on_data_ready_duration_s` | 对每个 ready task 调用 `task.on_data_ready()`，其中执行 `ray.get(meta_ref)` 做 schema 反序列化传播 |
| **dispatch** | `dispatch_duration_s` | 算子选择 + 任务分发的循环开销 |

另外 `update_usages()` / `_update_allocated_budgets()` 每个调度步调用两次，开销包含在 `sched_loop_duration_s` 总量中。

### 2.2 多算子多 Worker 如何放大调度开销

1. **`on_data_ready_duration_s`**：每个算子产出的每个 block 都要走 `ray.get(meta_ref)` + `pickle.loads`。`--num-workers` 大时完成 block 数量线性增长；`--num-scalar-cols` / `--num-array-cols` 大时 schema 序列化体积增大。

2. **`dispatch_duration_s`**：每个调度步的 dispatch 循环遍历所有算子做 `select_operator_to_run()`。`--num-operators` 越大，每次选择/分发要考察的算子越多。

3. **`update_usages()` / `_update_allocated_budgets()`**：这两个函数每个调度步调用两次，开销与算子数 N_ops 成正比。`_disable_operator_fusion()` 阻止融合，确保拓扑中真实存在 N 个独立算子。

### 2.3 指标收集链路

```
调度循环每步 → SchedulingLoopMetrics dataclass → 7 个 Gauge 推 Prometheus
                                      ↓
         streaming_exec_schedule_s / ray_wait_s / on_data_ready_s / dispatch_s (Timer)
                                      ↓
                      DatasetStatsSummary (total/avg/max)
                                      ↓
                   collect_dataset_stats() → benchmark 结果输出
```

---

## 3. DataOpTask.on_data_ready 的 block/meta ref 处理与异常逻辑

**文件**: `python/ray/data/_internal/execution/interfaces/physical_operator.py`

### 3.1 核心流程：streaming generator 严格交替 yield block → meta

`on_data_ready()` 内部通过 `while` 循环从 `self._streaming_gen`（`ObjectRefGenerator`）依次拉取数据，遵循 **block ref → meta ref** 的严格交替顺序，用 `_pending_block_ref` 和 `_pending_meta_ref` 两个状态字段追踪。

### 3.2 Step 1: 拉取 block ref

```python
if self._pending_block_ref.is_nil():
    self._pending_block_ref = self._streaming_gen._next_sync(timeout_s=0)
```

- **`StopIteration`**: generator 已结束，`_task_done_callback(None, ...)` 标记任务正常完成
- **返回 nil ref**: generator 暂无新输出但未停止，`break` 等下次调度步再试
- **返回有效 ref**: 调用 `_block_ready_callback`，继续进入 Step 2

### 3.3 Step 2: 拉取 meta ref

```python
if self._pending_meta_ref.is_nil():
    self._pending_meta_ref = self._streaming_gen._next_sync(timeout_s=METADATA_WAIT_TIMEOUT_S)
```

- **`StopIteration`**: block 已出但 meta 没出 → Worker 侧发生异常。`_pending_block_ref` 实际存储异常对象。`ray.get(self._pending_block_ref)` 触发异常，传给 `_task_done_callback(ex, ...)` 后 reraise
- **返回 nil ref**: block 引用有了但 meta 还没就绪，`break` 保留 pending refs
- **返回有效 ref**: 调用 `_metadata_ready_callback`，继续进入 Step 3

### 3.4 Step 3: 反序列化 meta ref → 输出 RefBundle

```python
meta_with_schema_bytes = ray.get(self._pending_meta_ref, timeout=METADATA_GET_TIMEOUT_S)
meta_with_schema = pickle.loads(meta_with_schema_bytes)
self._output_ready_callback(RefBundle([(self._pending_block_ref, meta)], ...))
```

- **`ray.exceptions.GetTimeoutError`**: meta 引用存在但对象不可用，warning 后 `break`，保留 pending refs 下次重试
- **成功**: 反序列化得到 `BlockMetadataWithSchema`，组装 `RefBundle` 回调输出，重置 pending refs 为 nil

### 3.5 异常处理汇总

| 场景 | 何时发生 | 处理方式 |
|---|---|---|
| generator 正常结束 | `_next_sync` 拉 block 时 `StopIteration` | `_task_done_callback(None, ...)`，标记完成 |
| Worker 侧异常 | 拉到 block 后拉 meta 时 `StopIteration` | `ray.get(block_ref)` 触发异常 → `_task_done_callback(ex, ...)` + reraise |
| meta 对象不可用 | `ray.get(meta_ref)` 超时 `GetTimeoutError` | warning 日志，`break` 留待下次重试（pending refs 保留） |
| meta 引用未就绪 | `_next_sync` 拉 meta 返回 nil | `break`，pending block ref 保留 |

---

## 4. is_nil() 含义与 Task 失败时的 ref 状态

### 4.1 is_nil() 的含义

`ray.ObjectRef.nil()` 是 Ray 的空引用哨兵值，等价于指针中的 `null`。`is_nil()` 检查当前引用是否为这个空值。

在 `DataOpTask` 中，`_pending_block_ref` 和 `_pending_meta_ref` 初始值都是 `ray.ObjectRef.nil()`，作为状态机标志：

| 状态 | `_pending_block_ref` | `_pending_meta_ref` | 含义 |
|---|---|---|---|
| 初始 / 已消费完 | nil | nil | 无待处理数据，下次从 block 开始取 |
| 已取 block，等 meta | **非 nil** | nil | 有 block 待配对，需拉 meta |
| block + meta 都到手 | **非 nil** | **非 nil** | 准备 `ray.get(meta)` 反序列化并输出 |

### 4.2 Task 执行失败时的 ref 状态

Ray 的 `ObjectRefGenerator` 在 task 失败时：仍然 yield 一个 block ref，但该 ref 指向的是异常对象，随后 generator 停止（不再 yield meta）。

```
1. _next_sync(timeout_s=0) → 拿到 block ref（非 nil）
   _pending_block_ref = 指向异常对象的 ObjectRef（非 nil）
   _pending_meta_ref = nil

2. _next_sync(timeout_s=METADATA_WAIT_TIMEOUT_S) → StopIteration
   _pending_block_ref 仍然非 nil
   _pending_meta_ref 仍然 nil

3. 进入 StopIteration 异常处理分支：
   ray.get(self._pending_block_ref) → 触发异常
   → _task_done_callback(ex, None, None)
   → reraise ex from None
```

Task 失败时：
- `_pending_block_ref` = **非 nil**（持有指向异常对象的 ObjectRef）
- `_pending_meta_ref` = **nil**（从未成功获取到 meta ref）

代码通过这个**非对称状态**检测异常：block 出了但 meta 没有（StopIteration），说明 "block ref 里装的其实是异常"。

---

## 5. ray.get 还原原始异常：Dual Exception 机制

### 5.1 ray.get 入口（worker.py:3014-3019）

```python
for i, value in enumerate(values):
    if isinstance(value, RayError):
        if isinstance(value, RayTaskError):
            raise value.as_instanceof_cause()   # 关键调用
        else:
            raise value
```

### 5.2 as_instanceof_cause()（exceptions.py:245-276）

```python
def as_instanceof_cause(self):
    cause_cls = self.cause.__class__
    if issubclass(RayTaskError, cause_cls):
        return self
    try:
        return self.make_dual_exception_instance()
    except TypeError as e:
        return self  # 无法子类化时降级返回原始 RayTaskError
```

### 5.3 _make_normal_dual_exception_instance()（exceptions.py:175-207）

运行时动态创建一个同时继承 `RayTaskError` 和原始异常类的类：

```python
def _make_normal_dual_exception_instance(self):
    cause_cls = self.cause.__class__      # 如 ValueError
    error_msg = str(self)                 # RayTaskError 的完整 traceback 字符串

    class cls(RayTaskError, cause_cls):
        def __init__(self, cause):
            self.cause = cause
            self._ray_task_error_args = (cause,)

        @property
        def args(self):
            return self._ray_task_error_args

        def __getattr__(self, name):
            return getattr(self.cause, name)  # 代理访问原始异常属性

        def __str__(self):
            return error_msg

    name = f"RayTaskError({cause_cls.__name__})"
    cls.__name__ = name
    cls.__qualname__ = name

    return cls(self.cause)   # 实例化并返回
```

### 5.4 具体示例

假设 task 抛出 `ValueError("bad data")`：

```
Worker 侧: ValueError("bad data")
  → Ray 序列化为 RayTaskError(function_name, traceback_str, cause=ValueError)
  → 存入 ObjectRef 所指向的 object store slot

ray.get(block_ref)
  → 从 object store 取出 RayTaskError 实例
  → 调用 .as_instanceof_cause()
  → 创建 class RayTaskError(ValueError) 的 dual 实例
  → raise 该 dual 实例

except Exception as ex:
  ex → RayTaskError(ValueError) dual 实例
  isinstance(ex, ValueError)   → True
  isinstance(ex, RayTaskError) → True
  ex.cause → ValueError("bad data")
  str(ex) → 包含完整远程 traceback
```

### 5.5 完整链路

```
Task worker 抛出 ValueError("bad data")
  → Ray 序列化为 RayTaskError(function_name, traceback_str, cause=ValueError)
  → 存入 ObjectRef 所指向的 object store slot

ray.get(block_ref)
  → 从 object store 取出 RayTaskError 实例
  → 调用 .as_instanceof_cause()
  → 创建 class RayTaskError(ValueError) 的 dual 实例
  → raise 该 dual 实例

except Exception as ex:
  ex → RayTaskError(ValueError) dual 实例
  isinstance(ex, ValueError)   → True
  isinstance(ex, RayTaskError) → True
  ex.cause → ValueError("bad data")
  str(ex) → 包含完整远程 traceback

  _task_done_callback(ex, None, None)  # 通知算子
  raise ex from None                    # 向上传播
```

---

## 6. ObjectLostError vs OwnerDiedError 与 Owner 身份

### 6.1 继承关系

```
ObjectLostError          ← 父类：对象从分布式内存中丢失
  ├── OwnerDiedError     ← 子类：owner 进程死了，导致对象丢失
  └── ObjectReconstructionFailedError ← 子类：重建失败，对象丢失
```

### 6.2 区别

| | `ObjectLostError`（非 OwnerDied） | `OwnerDiedError` |
|---|---|---|
| **触发原因** | 对象存储节点宕机/被抢占，对象数据从 plasma store 中被清除 | 对象的 owner 进程崩溃 |
| **对象数据是否还在** | 可能还在其他节点副本中 | 对象数据**可能还在**其他节点的 plasma store 中 |
| **能否重建** | 如果有 lineage（task 可重跑），Ray 尝试重新执行 task 重建 | **无法重建**，owner 负责驱动重建流程，owner 死了就没有进程能发起重建 |
| **关键差异** | 丢失的是**数据** | 丢失的是**管理该对象生命周期的进程** |

### 6.3 Owner 死了，对象数据还在，为什么不能获取？

1. **ObjectRef 的有效性依赖 owner 存活**：owner 进程维护该 ObjectRef 的引用计数和生命周期
2. **重建流程由 owner 驱动**：Ray 的 lineage reconstruction 是 owner 发现对象丢失后重新提交 task。owner 死了就没有进程能做这件事
3. **借用方不能替代 owner 发起重建**：如 `OBJECT_UNRECONSTRUCTABLE_BORROWED` 错误

### 6.4 Owner 的准确定义

在 Ray 中，**owner 是该 ObjectRef 的 "logical creator"**——即 Ray 内部为该 ref 维护引用计数和生命周期的进程。

在 Ray Data 场景下：
- **block ref / meta ref 的 owner 是 Driver**（持有 ObjectRefGenerator 的进程）
- **Worker 只是产出数据的执行者**，不是这些 ref 的 owner
- Driver 调用 `.remote()` 创建 generator ref，`_next_sync()` 从 Driver 的 core_worker 获取子 ref，这些子 ref 的引用计数由 Driver 管理

### 6.5 Ray Data 中的实际场景

| 场景 | 会发生什么 |
|---|---|
| **Task 执行中抛异常** | Worker 存活，将 `RayTaskError` 写入输出 slot → `_next_sync` 正常拿到 ref → `ray.get` 抛出 `RayTaskError` |
| **Worker 进程被杀（OOM/segfault）** | Worker 无法写入任何东西 → `_next_sync` 读 stream 时触发 `ObjectRefStreamEndOfStreamError` → `ray.get(generator_ref)` 抛出系统异常 → `_generator_ref` 作为最后一个 ref 返回 |
| **Worker 节点宕机** | 同上，task 失败，由 Ray core 通知 driver |

因为 owner 是 driver（还活着），所以 **不会出现 `OwnerDiedError`**（除非 driver 自己死了）。

### 6.6 修正后的完整场景表

| 场景 | `_next_sync` 行为 | `on_data_ready` 行为 | Ray 内部 |
|---|---|---|---|
| **Worker 死亡，有重试次数** | stream 可能暂不可用，等重试 task 执行后恢复 | `GetTimeoutError` → break 重试 | **RetryTaskIfPossible** → 重新提交 task |
| **Worker 死亡，无重试次数** | `ObjectRefStreamEndOfStreamError` → 返回 `_generator_ref` | `ray.get(generator_ref)` → `RayActorError` | **FailPendingTask** → stream 标记结束 |
| **已 yield ref 数据丢失，lineage 可用** | 返回 ref（非 nil） | `ray.get(meta_ref)` 超时 → `GetTimeoutError` → break | 后台 **lineage reconstruction** → `ResubmitTask` 重建 |
| **已 yield ref 数据丢失，lineage 不可用** | 返回 ref（非 nil） | `ray.get(meta_ref)` → `ObjectLostError`/`ObjectReconstructionFailedError` | **不重建**，异常向上传播 |
| **数据暂时不可达** | 返回 nil ref 或 ref | `GetTimeoutError` → break | 下次调度步重试 `ray.get` |
| **Driver 死亡** | — | `OwnerDiedError` | 不可能发生在 Ray Data 中（driver 挂则整个进程不在了） |

---

## 7. ObjectID 的确定性创建与 Driver/Worker 协调

### 7.1 核心原理

**Return ObjectID 是 deterministic 的**——由 `TaskID + return_index` 确定性计算得出，Driver 和 Worker 用**同一算法**独立算出相同的 ID，不需要额外通信。

### 7.2 阶段 1：Driver 侧创建 TaskID 并计算 Return ObjectID

**`core_worker.cc:1957`**（`SubmitTask`）：

```cpp
std::vector<rpc::ObjectReference> CoreWorker::SubmitTask(...) {
  const auto next_task_index = worker_context_->GetNextTaskIndex();
  const auto task_id = TaskID::ForNormalTask(
      worker_context_->GetCurrentJobID(),
      worker_context_->GetCurrentInternalTaskId(),
      next_task_index);

  BuildCommonTaskSpec(builder, ..., task_options.num_returns, ...);
  TaskSpecification task_spec = std::move(builder).ConsumeAndBuild();

  returned_refs = task_manager_->AddPendingTask(
      task_spec.CallerAddress(), task_spec, ...);

  normal_task_submitter_->SubmitTask(std::move(task_spec));
  return returned_refs;
}
```

**`task_manager.cc:~242`**（`AddPendingTask`）：

```cpp
for (size_t i = 0; i < num_returns; i++) {
  auto return_id = spec.ReturnId(i);  // = ObjectID::FromIndex(task_id, i + 1)
  reference_counter_.AddOwnedObject(return_id, ..., caller_address, ...);

  rpc::ObjectReference ref;
  ref.set_object_id(return_id.Binary());
  ref.mutable_owner_address()->CopyFrom(caller_address);  // owner = driver
  returned_refs.push_back(std::move(ref));
}
```

### 7.3 阶段 2：ObjectID 的确定性计算公式

**`task_spec.cc:215`**：

```cpp
ObjectID TaskSpecification::ReturnId(size_t return_index) const {
  return ObjectID::FromIndex(TaskId(), return_index + 1);
}
```

**`id.cc:243`**：

```cpp
ObjectID ObjectID::FromIndex(const TaskID &task_id, ObjectIDIndexType index) {
  return GenerateObjectId(task_id.Binary(), index);
}

ObjectID ObjectID::GenerateObjectId(const std::string &task_id_binary,
                                    ObjectIDIndexType object_index) {
  ObjectID ret;
  std::memcpy(ret.id_, task_id_binary.c_str(), TaskID::kLength);
  std::memcpy(ret.id_ + TaskID::kLength, &object_index, sizeof(object_index));
  return ret;
}
```

**ObjectID = TaskID 二进制 || return_index+1 的二进制**

### 7.4 阶段 3：Task Spec 通过 gRPC 发送给 Worker

**`normal_task_submitter.cc:518`**：

```cpp
void NormalTaskSubmitter::PushNormalTask(...) {
  auto request = std::make_unique<rpc::PushTaskRequest>();
  request->mutable_task_spec()->CopyFrom(task_spec.GetMessage());
  client->PushNormalTask(std::move(request), ...);
}
```

Worker 收到的是**完整的 TaskSpecification protobuf**（包含 `task_id` 和 `num_returns`），没有显式的 return ObjectID 列表。

### 7.5 阶段 4：Worker 侧从 Task Spec 自行计算 Return ObjectID

**`core_worker.cc:2756`**（`ExecuteTask`）：

```cpp
Status CoreWorker::ExecuteTask(const TaskSpecification &task_spec, ...) {
  for (size_t i = 0; i < task_spec.NumReturns(); i++) {
    return_objects->emplace_back(task_spec.ReturnId(i), nullptr);
    //               = ObjectID::FromIndex(task_spec.TaskId(), i + 1)
  }
  Status status = options_.task_execution_callback(task_spec, ...);
}
```

### 7.6 Streaming Generator 的 ObjectID 规则

```cpp
ObjectID TaskSpecification::StreamingGeneratorReturnId(size_t generator_index) const {
  RAY_CHECK_EQ(NumReturns(), 1UL);
  return ObjectID::FromIndex(TaskId(), 2 + generator_index);
}
```

- index=1: generator ref
- index≥2: `ObjectID::FromIndex(task_id, 2 + generator_index)`

### 7.7 完整对比：普通 task vs streaming generator

| | 普通 task (`num_returns=N`) | Streaming generator (`num_returns="streaming"`) |
|---|---|---|
| **Return ObjectID 公式** | `ObjectID::FromIndex(task_id, i+1)`, i∈[0,N) | index=1: generator ref; index≥2: `2+generator_index` |
| **ObjectID 何时创建** | Driver 调用 `.remote()` 时一次性全部算出 | Driver 和 Worker **各自独立**随 yield 递增 index 计算 |
| **Driver 怎么知道 return ref** | `AddPendingTask` 直接返回 N 个 `ObjectReference` | `peek_object_ref_stream` 用同公式算下一个 expected ref |
| **Worker 怎么知道写入哪个 ID** | `task_spec.ReturnId(i)` 从 task spec 算出 | `allocate_dynamic_return_id_for_generator` 用同公式算出 |
| **数据写入** | Task 执行完毕，`store_task_outputs` 批量写入 | 每次 yield，`create_generator_return_obj` 单个写入 + gRPC 通知 |
| **Owner** | Driver（`caller_address`） | Driver（`caller_address`） |

### 7.8 Streaming Generator 的完整数据流

```
Driver 提交 task.remote()  →  Ray core 调度到 Worker
         │                              │
    创建 object ref stream         执行 generator function
    (内部维护 generator_index)         │
         │                         ┌────┴────┐
         │                         │ yield val │
         │                         └────┬────┘
         │                              │
         │                    ┌─────────────────────────┐
         │                    │ Worker 侧 (create_generator_return_obj)  │
         │                    │                                          │
         │                    │ 1. allocate_dynamic_return_id_for_generator│
         │                    │    → ObjectID = f(task_id, 1+1+gen_index) │
         │                    │    → owner_address = driver 地址            │
         │                    │                                          │
         │                    │ 2. store_task_outputs(val)               │
         │                    │    → 序列化 val 写入 plasma store         │
         │                    │                                          │
         │                    │ 3. ReportGeneratorItemReturns (gRPC)      │
         │                    │    → 通知 driver: 第 N 个 ref 可读了       │
         │                    └─────────────┬───────────────────────────┘
         │                                  │
    ┌─────────────────────────────────────┐ │
    │ Driver 侧 (_next_sync)               │ │
    │                                      │ │
    │ 1. peek_object_ref_stream            │ │
    │    → 用同算法算 expected ObjectID     │ │
    │    → 检查 is_ready（等 Worker 通知） ◄─┘
    │                                      │
    │ 2. try_read_next_object_ref_stream   │
    │    → 从 stream 读出 ObjectRef         │
    │                                      │
    │ 3. ray.get(ref)                      │
    │    → 从 plasma store 取出数据         │
    └──────────────────────────────────────┘
```

---

## 8. Worker/Node 死亡时的失败检测、重试与 Lineage 重建

### 8.1 _next_sync 如何发现 stream 结束

#### 失败检测链路

```
PushNormalTask RPC 返回 !status.ok()
  → GetWorkerFailureCause 查 raylet 获取错误类型（WORKER_DIED / NODE_DIED）
  → FailOrRetryPendingTask(task_id, error_type)
```

#### MarkTaskReturnObjectsFailed 对 streaming generator 的处理

```cpp
if (spec.IsStreamingGenerator()) {
  MarkEndOfStream(generator_id, /*item_index=*/-1);
  for (size_t i = 0; i < num_streaming_generator_returns; i++) {
    const auto generator_return_id = spec.StreamingGeneratorReturnId(i);
    in_memory_store_.Put(error, generator_return_id, ...);
  }
}
```

#### MarkEndOfStream 的行为

```cpp
void ObjectRefStream::MarkEndOfStream(int64_t item_index, ObjectID *object_id_in_last_index) {
  if (end_of_stream_index_ != -1) return;
  end_of_stream_index_ = std::max(next_index_, item_index);
  *object_id_in_last_index = GetObjectRefAtIndex(end_of_stream_index_);
}
```

当传入 `item_index = -1` 时，`end_of_stream_index_` 设为 `next_index_`（Driver 当前读取位置）。

之后 Driver 调用 `_next_sync`：

```python
ref = core_worker.try_read_next_object_ref_stream(self._generator_ref)
# → IsFinished() == true
# → 返回 Status::ObjectRefEndOfStream
# → Python 侧抛出 ObjectRefStreamEndOfStreamError

except ObjectRefStreamEndOfStreamError:
    try:
        ray.get(self._generator_ref)  # generator_ref 包含系统异常
    except Exception:
        self._generator_task_raised = True
        return self._generator_ref    # 返回 generator_ref 作为异常载体
```

### 8.2 会重试吗？

**会！** Ray 支持对 streaming generator task 进行重试：

```cpp
bool TaskManager::RetryTaskIfPossible(const TaskID &task_id, ...) {
  if (num_retries_left > 0) {
    will_retry = true;
    num_retries_left--;
  }
  if (will_retry) {
    task_entry.MarkRetry();
    async_retry_task_callback_(spec, delay_ms);  // 重新提交 task
  }
}
```

重试时 stream 状态会保留，重新执行的 generator 会继续往同一组 ObjectID 写入数据。

### 8.3 已 yield ref 数据丢失，会重建吗？

**会！** Ray 的 lineage reconstruction 支持.streaming generator 的输出。

#### 重建链路

```
ray.get(meta_ref) 发现对象丢失
  → ObjectRecoveryManager::RecoverObject(object_id)
    → 检查 LineageReconstructionEligibility
    → 如果 ELIGIBLE:
      → ReconstructObject(object_id)
        → ResubmitTask(task_id)
```

#### ResubmitTask 对 streaming generator 的特殊处理

```cpp
if (task_entry.spec_.IsStreamingGenerator() &&
    task_entry.GetStatus() == rpc::TaskStatus::SUBMITTED_TO_WORKER) {
  // generator 还在运行 → 不能立即重新执行
  // 排队，等当前执行完成后再重新提交
  should_queue_generator_resubmit = true;
  queue_generator_resubmit_(spec);
}
```

当 generator 执行完成后：

```cpp
resubmit_generator = generators_to_resubmit_.erase(task_id) > 0;
if (resubmit_generator) {
  task_manager_.MarkGeneratorFailedAndResubmit(task_id);
}
```

#### 重建条件

| 条件 | 满足时 | 不满足时 |
|---|---|---|
| `max_retries > 0` | `ELIGIBLE` | `INELIGIBLE_NO_RETRIES` |
| `lineage_pinning_enabled_ = true` | `ELIGIBLE` | `INELIGIBLE_LINEAGE_DISABLED` |
| Lineage 未被内存压力 evict | `ELIGIBLE` | `INELIGIBLE_LINEAGE_EVICTED` |
| 对象由 owner 持有（不是 borrowed） | 可重建 | `INELIGIBLE_BORROWED` |
| Task 未被 cancel | 可重建 | `INELIGIBLE_TASK_CANCELLED` |
| 重试次数未耗尽 | 可重建 | `MAX_ATTEMPTS_EXCEEDED` |

Dynamically-generated streaming return ID 的 `lineage_eligibility_` **继承自 generator 的 eligibility**。

---

## 9. Object 创建通知 Owner 与 Owner 发现 Object 地址

### 9.1 Object 创建后如何通知 Owner

有两条路径，普通 task 和 streaming generator 各一条，但机制相同：

#### 路径 A：普通 task —— `PushTaskReply` RPC

```
Worker 执行 task → 写入 plasma → SealObject
  → PushTaskReply RPC 回复 Owner
    → 包含 return_objects[]（每个含 object_id + in_plasma 标志 + 数据）
    → 包含 worker_addr.node_id（执行 worker 的节点 ID）
```

**Owner 侧** `CompletePendingTask` → `HandleTaskReturn`（`task_manager.cc:550`）：

```cpp
StatusOr<bool> HandleTaskReturn(const ObjectID &object_id,
                                const rpc::ReturnObject &return_object,
                                const NodeID &worker_node_id, ...) {
  if (return_object.in_plasma()) {
    reference_counter_.UpdateObjectPinnedAtRaylet(object_id, worker_node_id);
    in_memory_store_.Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA), object_id, ...);
  } else {
    in_memory_store_.Put(object, object_id, ...);
  }
}
```

#### 路径 B：Streaming generator —— `ReportGeneratorItemReturns` RPC

```
Worker yield 一个值 → 写入 plasma → SealReturnObject
  → ReportGeneratorItemReturns RPC 发给 Owner
    → 包含 returned_object（object_id + in_plasma + 数据）
    → 包含 worker_addr.node_id
```

**Owner 侧** `HandleReportGeneratorItemReturns`（`task_manager.cc:779`）：

```cpp
bool HandleReportGeneratorItemReturns(const rpc::ReportGeneratorItemReturnsRequest &request, ...) {
  auto object_id = ObjectID::FromBinary(request.returned_object().object_id());
  reference_counter_.OwnDynamicStreamingTaskReturnRef(object_id, generator_id);
  HandleTaskReturn(object_id, request.returned_object(),
                   NodeID::FromBinary(request.worker_addr().node_id()), ...);
}
```

**两条路径最终都走到 `HandleTaskReturn`**，核心动作是：
1. `UpdateObjectPinnedAtRaylet(object_id, worker_node_id)` → 在 ReferenceCounter 中记录位置
2. 位置更新后 → `PushToLocationSubscribers` 发布给所有订阅者

### 9.2 Owner 调用 ray.get() 时如何发现 Object 地址

完整链路：

```
Python ray.get(ref)
  → CoreWorker::Get(ids, timeout)
    → GetObjects(ids, timeout)
      ├─ Phase 1: memory_store_->Get()
      │   → 如果是 OBJECT_IN_PLASMA 标记 → 进入 Phase 2
      │   → 如果是内联数据 → 直接返回
      │
      └─ Phase 2: plasma_store_provider_->Get(ids, owner_addresses, timeout)
           → raylet_client_->AsyncGetObjects(ids, owner_addresses)
             → 本地 raylet 收到请求
               → LeaseDependencyManager::StartGetRequest()
                 → ObjectManager::Pull(object_refs)
                   → SubscribeObjectLocations(object_id, owner_address)
                     → 向 Owner 的 core_worker 订阅
                       WORKER_OBJECT_LOCATIONS_CHANNEL
```

**Owner 收到订阅请求后**：

```cpp
void PublishObjectLocationSnapshot(const ObjectID &object_id) {
  for (const auto &node_id : it->second.locations) {
    object_info->add_node_ids(node_id.Binary());
  }
  // 还包含 spilled_url, spilled_node_id, pending_creation, object_size
  object_info_publisher_->Publish(std::move(pub_message));
}
```

**请求方 raylet 收到位置后**：

```
OwnershipBasedObjectDirectory 回调 → PullManager::OnLocationChange()
  → 记录 client_locations[]（哪些远程节点有该对象）
  → SendPullRequest → 远程 raylet 通过 TCP 传输对象
  → 对象到达本地 plasma → raylet 通知 core_worker
  → ray.get() 返回
```

### 9.3 位置信息的持续更新

```
对象被复制到新节点 / 被 spill 到外部存储
  → raylet 调用 ReportObjectAdded / ReportObjectSpilled
    → UpdateObjectLocationBatch RPC 发给 Owner
      → Owner 的 ReferenceCounter::AddObjectLocation(object_id, node_id)
        → locations 集合增加新节点
        → PushToLocationSubscribers 发布给所有订阅者
```

### 9.4 完整图示

```
               Object 创建通知                     Object 定位
               ──────────                         ──────────

Worker 产出数据                              ray.get() 调用
  │                                              │
  │ PushTaskReply / ReportGeneratorItemReturns    │
  │ (含 worker_addr.node_id)                     │
  ▼                                              ▼
Owner 的 HandleTaskReturn                  memory_store→OBJECT_IN_PLASMA
  │                                              │
  │ UpdateObjectPinnedAtRaylet              plasma_store_provider->Get
  │ (记录 locations 集合)                        │
  ▼                                              ▼
ReferenceCounter                    raylet → SubscribeObjectLocations
  │                                    (向 Owner 订阅位置)
  │ locations = {node_A, node_B, ...}            │
  │                                              ▼
  │ PushToLocationSubscribers           Owner 发布 locations 快照
  │ (发布给已有订阅者)                            │
  ▼                                              ▼
订阅者收到位置更新              PullManager::OnLocationChange
                               → SendPullRequest(node_A)
                                        │
                                        ▼
                               对象传到本地 plasma → ray.get() 返回
```

### 9.5 关键数据结构：ReferenceCounter::Reference

`locations` 字段（`absl::flat_hash_set<NodeID>`）是对象位置的 source of truth，来源：

1. **创建时**：`AddOwnedObject` 的 `pinned_at_node_id` 参数
2. **HandleTaskReturn**：`UpdateObjectPinnedAtRaylet`
3. **Raylet 通知**：`UpdateObjectLocationBatch` RPC
4. **Spill 通知**：`HandleObjectSpilled`

---

## 10. MarkTaskReturnObjectsFailed 为什么将已 yield 的 ref 也标记为失败

### 10.1 两层存储背景

```
in_memory_store_ (Driver 进程内)    plasma store (节点共享内存)
─────────────────────────────      ─────────────────────────
小对象 → 直接存实际数据              大对象 → 存实际数据
plasma 对象 → 存 OBJECT_IN_PLASMA 标记
                                    ↑ 哪个节点有数据由 ReferenceCounter.locations 记录
```

当 `ray.get(ref)` 时：
- 先查 `in_memory_store_`
- 如果是 `OBJECT_IN_PLASMA` 标记 → 去 plasma store 取（可能需要跨节点拉取）
- 如果是错误对象 → 直接抛异常，**不再访问 plasma store**

### 10.2 已 yield ref 的三种状态

| 状态 | 含义 | in_memory_store_ 当前内容 |
|---|---|---|
| **已消费 + 已 ray.get()** | Driver 已经取到数据 | 实际数据或已被消费释放 |
| **已消费 + 未 ray.get()** | Driver 从 stream 取出 ref，但还没 get | `OBJECT_IN_PLASMA` 标记 |
| **在 stream 中未消费** | Worker 已报告，Driver 还没从 stream 读 | `OBJECT_IN_PLASMA` |

### 10.3 写入错误对象的三个作用

**1. 对"已消费 + 未 ray.get()"的 ref：覆盖 `OBJECT_IN_PLASMA` 标记**

Worker/Node 死亡后，`OBJECT_IN_PLASMA` 标记还在，`ray.get()` 会尝试去 plasma 取数据，但数据可能已丢失，导致挂死或超时。写入错误对象后，`ray.get()` 直接从 `in_memory_store_` 取到错误 → **立即抛异常，不访问 plasma**。

**2. 对"stream 中未消费"的 ref：预置错误**

`MarkEndOfStream` 结束 stream 后，这些 ref 后续可能被读到，写入错误确保它们 `ray.get()` 时也能立即失败。

**3. 对"已消费 + 已 ray.get()"的 ref：无影响**

数据已取到，`in_memory_store_.Put` 的第二个参数 `reference_counter_.HasReference(object_id)` 检查引用是否存在。如果 ref 已被释放，Put 就是空操作。

### 10.4 如果不覆盖会怎样？

Worker 节点宕机，plasma 数据丢失，但 `in_memory_store_` 中仍是 `OBJECT_IN_PLASMA`：

```
ray.get(meta_ref)
  → memory_store 返回 OBJECT_IN_PLASMA
  → plasma_store_provider->Get()
    → raylet AsyncGetObjects
      → SubscribeObjectLocations 向 Owner 查位置
      → Owner 发现 locations 中的节点已死
      → 触发 lineage reconstruction
        → 如果可重建：等重建完成（耗时很长）
        → 如果不可重建：最终抛 ObjectReconstructionFailedError
        → 如果重建超时：GetTimeoutError 循环
```

而覆盖为错误对象后：

```
ray.get(meta_ref)
  → memory_store 直接返回 error 对象
  → 立即抛异常
```

### 10.5 执行位置

`MarkTaskReturnObjectsFailed` 在 **Owner（Driver）** 上执行。所有关键组件都在 Driver 进程中：

| 组件 | 所在进程 | 作用 |
|---|---|---|
| `TaskManager` | Driver | 管理 pending task 状态 |
| `ReferenceCounter` | Driver | 跟踪 ObjectRef 引用计数和 locations |
| `in_memory_store_` | Driver | Driver 进程内的内存 store |
| `object_ref_streams_` | Driver | 维护 generator 的 stream 状态 |

### 10.6 代码注释确认

```cpp
// It is only useful when lineage reconstruction retry is failed. In this
// case, all these objects are lost from the plasma store, so we
// can overwrite them. See the test test_dynamic_generator_reconstruction_fails
```

当 lineage reconstruction 也失败时，plasma 中的数据**确定丢失**，覆盖 `OBJECT_IN_PLASMA` 标记为错误对象是安全且必要的——否则 `ray.get()` 永远拿不到数据。

**总结**：将已 yield 的 ref 也标记为失败，是为了在 task 永久失败（无重试）的情况下**短路 `ray.get()` 流程**，避免其陷入无望的 plasma 拉取或重建等待，让失败快速传播到上层。

---

## 12. Worker 节点宕机后 ray.get() 的完整执行路径

### 12.1 三方角色定义

| 角色 | 进程 | 职责 |
|------|------|------|
| **Caller Worker** | 调用 `ray.get()` 的 worker | 发起对象获取请求，阻塞等待 |
| **Caller Raylet** | Caller Worker 所在节点的 raylet | 管理 pull 请求，向 Owner 订阅对象位置 |
| **Owner (Driver)** | 创建该 ObjectRef 的 Driver 进程 | 持有 `ReferenceCounter`，追踪 `locations`，执行 lineage reconstruction |

> **注意**：在 Ray Data 中，Owner 始终是 Driver（调用 `.remote()` 的进程），不是执行 task 的 worker。

### 12.2 正常流程：ray.get() 到 Plasma 获取

```
Caller Worker                     Caller Raylet                    Owner (Driver)
═════════════                     ═════════════                    ══════════════

ray.get(meta_ref)
  │
  ├→ core_worker.Get()
  │   ├→ memory_store_->Get()
  │   │   └→ 返回 RayObject(OBJECT_IN_PLASMA)   ← marker: "去 plasma 找"
  │   │
  │   └→ IsInPlasmaError() detected
  │       └→ plasma_store_provider_->Get()
  │           ├→ AsyncGetObjects(batch_ids, owner_addresses)  ────→  HandleAsyncGetObjects
  │           │                                                    └→ PullManager.Pull()
  │           │                                                        └→ OBD.SubscribeObjectLocations() ──RPC──→
  │           │                                                                                                  ProcessSubscribeObjectLocations()
  │           │                                                                                                  └→ PublishObjectLocationSnapshot()
  │           │                                                                                                     └→ PushToLocationSubscribers()
  │           │                                                                                                        → 发布 locations(node_ids)
  │           │
  │           │                              ←─── location update callback ────────────────────────────
  │           │                                  PullManager.OnLocationChange()
  │           │                                    → PullFromRandomLocation()
  │           │                                      → send_pull_request_() 拉取对象
  │           │
  │           └→ while 循环轮询 plasma store:
  │               GetObjectsFromPlasmaStore(batch_timeout)
  │               → store_client_->Get() 检查本地 plasma
  │               → 对象到达后返回
```

**关键代码位置**：
- `core_worker.cc:1541` — `GetObjects()` 分叉：memory_store → plasma_store
- `plasma_store_provider.cc:257` — `Get()` 发起 `AsyncGetObjects` + polling loop
- `ownership_object_directory.cc:307` — `SubscribeObjectLocations()` 通过 pubsub 订阅 Owner
- `reference_counter.cc:1731` — `PublishObjectLocationSnapshot()` 发布位置快照
- `reference_counter.cc:1678` — `PushToLocationSubscribers()` 推送位置更新

### 12.3 Worker 节点宕机后的完整链路

#### 阶段 1：Owner 检测节点死亡

```
GCS 检测到 NodeID_X 死亡
  → 通知 Owner 的 CoreWorker
    → ReferenceCounter::ResetObjectsOnRemovedNode(node_id)  // reference_counter.cc:897
      │
      ├→ 对每个 pinned_at_node_id_ == NodeID_X 的对象:
      │    1. UnsetObjectPrimaryCopy(it)       // 重置 pinned_at_node_id_
      │    2. objects_to_recover_.push_back()   // 加入恢复队列
      │
      ├→ 对每个 locations 中包含 NodeID_X 的对象:
      │    RemoveObjectLocationInternal(it, node_id)
      │      └→ PushToLocationSubscribers(it)
      │         → 发布更新后的 locations（已剔除死亡节点）
```

**Owner 的位置更新传播**：`RemoveObjectLocationInternal()` 触发 `PushToLocationSubscribers()`，通过 pubsub 通知所有订阅者（包括 Caller Raylet）。此时 Caller Raylet 的 PullManager 收到的 location update 中**已不含死亡节点**。

#### 阶段 2：Owner 启动恢复

Owner 的定时器每 100ms 检查 `objects_to_recover_`（`core_worker.cc:470`）：

```cpp
periodical_runner_->RunFnPeriodically([this] {
    const auto lost_objects = reference_counter_->FlushObjectsToRecover();
    if (!lost_objects.empty()) {
        memory_store_->Delete(lost_objects);     // ← 关键：删除 OBJECT_IN_PLASMA marker
        for (const auto &object_id : lost_objects) {
            RAY_UNUSED(object_recovery_manager_->RecoverObject(object_id));
        }
    }
}, 100, "CoreWorker.RecoverObjects");
```

**`memory_store_->Delete(lost_objects)` 的效果**：
- 从 `objects_` map 中移除 `OBJECT_IN_PLASMA` marker
- 这意味着后续任何对该 object_id 的 `memory_store_->Get()` **不再返回** `OBJECT_IN_PLASMA`
- **但这不影响已经进入 plasma_store_provider polling loop 的 caller** — caller 不查 Owner 的 memory_store，而是轮询自己的本地 plasma store

#### 阶段 3：RecoverObject 决策树

```
ObjectRecoveryManager::RecoverObject(object_id)  // object_recovery_manager.cc:25
  │
  ├→ IsPlasmaObjectPinnedOrSpilled()
  │    ├→ ref 不存在 → return OBJECT_UNRECONSTRUCTABLE_REF_NOT_FOUND
  │    └→ 不是 owner → return OBJECT_UNRECONSTRUCTABLE_BORROWED
  │
  ├→ requires_recovery = (pinned_at.IsNil() && !spilled)
  │
  ├→ [已在恢复中] → 跳过（dedup by objects_pending_recovery_）
  │
  ├→ [有 pinned/spilled location] → Put(OBJECT_IN_PLASMA) 回 memory_store
  │
  └→ [需要恢复] → 注册 GetAsync callback + object_lookup_() →
       PinOrReconstructObject(locations)
         │
         ├→ [locations 非空] → PinExistingObjectCopy()
         │    ├→ PinObjectIDs RPC 成功 → Put(OBJECT_IN_PLASMA) + UpdatePinnedAtRaylet
         │    └→ PinObjectIDs RPC 失败 → 尝试下一个 location 或 ReconstructObject()
         │
         └→ [locations 为空] → ReconstructObject(object_id)
              │
              ├→ GetLineageReconstructionEligibility()
              │    不合格的情况:
              │      INELIGIBLE_PUT → OBJECT_UNRECONSTRUCTABLE_PUT
              │      INELIGIBLE_NO_RETRIES → OBJECT_UNRECONSTRUCTABLE_RETRIES_DISABLED
              │      INELIGIBLE_LINEAGE_EVICTED → OBJECT_UNRECONSTRUCTABLE_LINEAGE_EVICTED
              │      ...
              │
              ├→ [ELIGIBLE] → reference_counter_.UpdateObjectPendingCreation(object_id, true)
              │                  task_manager_.ResubmitTask(task_id, &task_deps)
              │                  ├→ SetupTaskEntryForResubmit() → MarkRetry
              │                  └→ async_retry_task_callback_(spec, delay_ms=0)
              │                     → 重新提交 task 到 raylet 调度
              │                  └→ 对每个 dep: RecoverObject(dep)  // 递归！
              │
              └→ [ResubmitTask 失败] → recovery_failure_callback_(object_id, error, pin=true)
```

#### 阶段 4：pending_creation 与 PullManager 的协作

**关键机制**：当 Owner 设置 `pending_creation=true` 时：

1. `PushToLocationSubscribers()` 发布 `pending_creation=true` 到所有 subscriber
2. Caller Raylet 的 PullManager 收到 `OnLocationChange(..., pending_creation=true)`
3. PullManager 设置 `request.pending_object_creation = true`
4. **`IsPullable()` 返回 `false`** → 该对象从 pullable 队列移到 unpullable
5. `TryToMakeObjectLocal()` **不执行 timeout 逻辑**（`RAY_CHECK(!pending_object_creation)` 保证）
6. PullManager **安静等待**，直到 Owner 发布新的 location update

```
PullManager 行为:
  pending_creation=true  → IsPullable()=false → 不尝试 pull，不触发 timeout
  pending_creation=false → IsPullable()=true  → 正常 pull + 可触发 timeout
```

#### 阶段 5：恢复成功 — 新对象传播到 Caller

```
Owner (Driver)                     新 Worker (重建)              Caller Raylet          Caller Worker
═══════════════                    ════════════════              ══════════════          ══════════════

ResubmitTask() → async_retry_task_callback_
  └→ 提交 task 到 raylet
                                    执行 task
                                    ├→ 返回值存入 plasma
                                    └→ ReportGeneratorItemReturns RPC
  ← HandleTaskReturn / ReportGeneratorItemReturns
  ├→ AddObjectLocationOwner()
  │   └→ AddObjectLocationInternal()
  │      └→ PushToLocationSubscribers()
  │         → 发布: node_ids=[新节点], pending_creation=false
  │
  └→ memory_store_.Put(OBJECT_IN_PLASMA)
                                    raylet 报告对象已添加
                                                                ← location update
                                                                OnLocationChange(pending_creation=false)
                                                                IsPullable()=true
                                                                PullFromRandomLocation()
                                                                → send_pull_request_()
                                                                  → 对象拉到 caller plasma
                                                                HandleObjectLocal()
                                                                → PlasmaObjectReady RPC
                                                                                         ← HandlePlasmaObjectReady()
                                                                                         polling loop:
                                                                                         store_client_->Get()
                                                                                         → 发现对象在 plasma
                                                                                         → 返回
```

#### 阶段 6：恢复失败 — Error 传播到 Caller

**两条路径**：

##### 路径 A：Owner Put error 到 plasma → 传播到 Caller plasma

```
Owner:
  recovery_failure_callback_(object_id, error_type, pin_object=true)
    → CoreWorker::Put(RayObject(error_type), object_id, pin_object=true)
       → PutInLocalPlasmaStore()     // 写入 Owner 的本地 plasma
       → local_raylet 检测到新对象
          → HandleObjectLocal()
             → reference_counter_ 更新 location
             → PushToLocationSubscribers()
                → 发布: locations 包含新节点

Caller Raylet:
  ← location update: 新节点有该对象
  → PullManager.PullFromRandomLocation()
  → 拉取 error 对象到 caller plasma

Caller Worker:
  polling loop → store_client_->Get() → 发现 error 对象
  → GetObjectsFromPlasmaStore 检测 IsException() → got_exception=true → break
  → 返回 error → Python 抛异常
```

##### 路径 B：MarkObjectsAsFailed — 直接在 Caller Raylet plasma 创建 error

**触发场景**：
1. OBD 的 `failure_callback` — Owner 死亡或 ref 被 deleted
2. PullManager 的 `fail_pull_request_` — fetch 超时（`OBJECT_FETCH_TIMED_OUT`）或磁盘满（`OUT_OF_DISK_ERROR`）

```
OBD failure_callback 或 PullManager fail_pull_request_
  → mark_as_failed_(object_id, error_type)         // main.cc:747 绑定
    → NodeManager::MarkObjectsAsFailed(error_type, {ref}, job_id)
       // node_manager.cc:2275
       → store_client_->TryCreateImmediately(object_id, 0, meta, ErrorStoredByRaylet)
       → store_client_->Seal(object_id)
       // 直接在 Caller Raylet 的本地 plasma store 创建 error 对象！
       // 如果失败 → RAY_LOG(ERROR) "may hang forever"

Caller Worker:
  polling loop → store_client_->Get() → 发现本地 plasma 中的 error 对象
  → IsException() → break → 返回 error
```

### 12.4 超时行为总结

| 场景 | 结果 | 异常类型 |
|------|------|----------|
| `ray.get(timeout=X)` + 对象在 timeout 内未到达 | 返回 `Status::TimedOut` | `GetTimeoutError`（临时性，可重试） |
| 恢复成功，新值到达 | 正常返回值 | — |
| 恢复失败（无重试/lineage evicted/...） | error 写入 plasma → 传播到 caller | `ObjectLostError` / `UnreconstructableError` |
| PullManager fetch 超时（`pending_creation=false` + 长时间无 location） | `MarkObjectsAsFailed(OBJECT_FETCH_TIMED_OUT)` | `ObjectFetchTimedOutError` |
| Owner 死亡 | OBD failure_callback → `MarkObjectsAsFailed(OWNER_DIED)` | `OwnerDiedError` |
| Owner 的 ref 被 deleted | OBD failure_callback → `MarkObjectsAsFailed(OBJECT_DELETED)` | `ObjectDeletedError` |
| `MarkObjectsAsFailed` 本身的 `TryCreateImmediately` 失败 | **caller 可能永久挂起** | — (RAY_LOG ERROR "may hang forever") |

### 12.5 Ray Data 场景下的特殊考量

1. **Owner 是 Driver**：Ray Data 中所有 task 的 Owner 都是 Driver 进程。`OwnerDiedError` 基本不会发生（除非 Driver 整个崩溃）。
2. **streaming generator 的 lineage reconstruction**：`ResubmitTask()` 检测到 `IsStreamingGenerator() && Status==SUBMITTED_TO_WORKER` 时，不立即 resubmit，而是通过 `queue_generator_resubmit_()` 排队等当前执行完成后再 resubmit。
3. **`pending_creation` 阻止 PullManager timeout**：当 lineage reconstruction 正在进行时，PullManager 不会因长时间无 location 而触发 `OBJECT_FETCH_TIMED_OUT`。只有当 `pending_creation=false`（重建结束，无论成功/失败）后，PullManager 才恢复正常的 timeout 逻辑。
4. **`memory_store_->Delete(lost_objects)` 不影响已进入 polling 的 caller**：Caller 在 plasma_store_provider 的 polling loop 中轮询的是**自己的本地 plasma store**，不是 Owner 的 memory_store。Delete 的作用是让 Owner 的后续 `GetObjects()` 调用不再短路到 plasma，而需要等待恢复结果。

---

## 13. MarkTaskReturnObjectsFailed 为何需要将已 yield 的 ref 标记为错误对象

### 13.1 `MarkTaskReturnObjectsFailed` 代码逻辑详解

**文件**: `src/ray/core_worker/task_manager.cc:1559`

```cpp
void TaskManager::MarkTaskReturnObjectsFailed(
    const TaskSpecification &spec,
    rpc::ErrorType error_type,
    const rpc::RayErrorInfo *ray_error_info,
    const absl::flat_hash_set<ObjectID> &store_in_plasma_ids) {
  const TaskID task_id = spec.TaskId();
  RayObject error(error_type, ray_error_info);
  int64_t num_returns = spec.NumReturns();
  for (int i = 0; i < num_returns; i++) {
    const auto object_id = ObjectID::FromIndex(task_id, /*index=*/i + 1);
    if (store_in_plasma_ids.contains(object_id)) {
      Status s = put_in_local_plasma_callback_(error, object_id);
      if (!s.ok()) {
        // fallback: 写入 in_memory_store_
        in_memory_store_.Put(error, object_id, reference_counter_.HasReference(object_id));
      }
    } else {
      in_memory_store_.Put(error, object_id, reference_counter_.HasReference(object_id));
    }
  }
  if (spec.ReturnsDynamic()) {
    // dynamic return ids 同样处理
  }
  if (spec.IsStreamingGenerator()) {
    MarkEndOfStream(generator_id, /*item_index*/ -1);
    // 对所有 streaming generator return ids 也写入 error
    auto num_streaming_generator_returns = spec.NumStreamingGeneratorReturns();
    for (size_t i = 0; i < num_streaming_generator_returns; i++) {
      const auto generator_return_id = spec.StreamingGeneratorReturnId(i);
      // 同样: store_in_plasma_ids 中的 → put_in_local_plasma_callback_，否则 → in_memory_store_
    }
  }
}
```

**关键决策逻辑** — `store_in_plasma_ids` 的来源：

```cpp
// task_manager.cc:1549
absl::flat_hash_set<ObjectID> TaskManager::GetTaskReturnObjectsToStoreInPlasma(
    const TaskID &task_id, bool *first_execution_out) const {
  bool first_execution = it->second.num_successful_executions_ == 0;
  if (!first_execution) {
    store_in_plasma_ids = it->second.reconstructable_return_ids_;
  }
  return store_in_plasma_ids;
}
```

| 场景 | `first_execution` | `store_in_plasma_ids` | 写入位置 |
|------|-------------------|----------------------|---------|
| 首次执行失败 | `true` | 空 | 所有 ref 只写 `in_memory_store_` |
| Lineage reconstruction 重试后失败 | `false` | `reconstructable_return_ids_` | 这些 ref 写 plasma，其余写 `in_memory_store_` |

### 13.2 为什么需要区分 `store_in_plasma_ids`

**首次执行失败**（`first_execution=true`）：
- Return objects **从未产生过值**，`in_memory_store_` 中无 `OBJECT_IN_PLASMA` marker
- Error 直接写 `in_memory_store_` → `ray.get()` 在 memory_store 阶段直接拿到 error
- **短路，不需要走 plasma 路径**

**Lineage reconstruction 失败**（`first_execution=false`）：
- Return objects **曾经**在 plasma 中存在过（首次执行成功 yield 了值）
- `in_memory_store_` 中有 `OBJECT_IN_PLASMA` marker
- **必须写入 plasma**，因为 `in_memory_store_.Put()` 不覆盖已有值（`memory_store.cc:185`: `if (iter != objects_.end()) { return; }`）
- 如果只写 `in_memory_store_`，`OBJECT_IN_PLASMA` marker 已占位 → 新 Put 是 no-op → `ray.get()` 继续走 plasma 路径 → 指向已丢失的数据
- 通过 `put_in_local_plasma_callback_` 写入 Owner 本地 plasma → **覆盖 `OBJECT_IN_PLASMA` marker**（通过 Owner 的 `PromoteObjectToPlasma` 或 recovery 路径）→ location update 传播 → caller 拉到 error 对象

### 13.3 `in_memory_store_.Put()` 不覆盖已有值的机制

**文件**: `src/ray/core_worker/store_provider/memory_store/memory_store.cc:172`

```cpp
void CoreWorkerMemoryStore::Put(const RayObject &object,
                                const ObjectID &object_id,
                                const bool has_reference) {
  absl::MutexLock lock(&mu_);
  auto iter = objects_.find(object_id);
  if (iter != objects_.end()) {
    return;  // Object already exists in the store, which is fine.
  }
  // ... 不存在时才写入
}
```

这是 Ray 的设计约束：**ObjectRef 的值一旦写入（无论是真实值还是 `OBJECT_IN_PLASMA` marker），不可覆盖**。这保证了 `ray.get()` 的语义确定性——同一个 ref 总是返回同一个值。

### 13.4 Streaming generator 特殊处理的必要性

注释原文：
> "In a normal time, it is no-op because the object ref values are already written, and Ray doesn't allow to overwrite values for the object ref. It is only useful when lineage reconstruction retry is failed. In this case, all these objects are lost from the plasma store, so we can overwrite them."

**普通情况下**（Worker 崩溃但节点仍存活）：
- Streaming generator 已 yield 的 ref **已在 plasma 中有值**
- `put_in_local_plasma_callback_` 尝试写 error → plasma 中已有旧值 → 写入失败（`!s.ok()`）
- Fallback `in_memory_store_.Put()` → 已有 `OBJECT_IN_PLASMA` marker → no-op
- **结果：对已 yield 的 ref 是 no-op，数据仍在 plasma 中可正常获取**

**Lineage reconstruction 失败**（Worker 节点宕机，plasma 数据丢失）：
- 已 yield 的 ref 在 plasma 中的数据**随节点死亡而丢失**
- `put_in_local_plasma_callback_` 写 error → plasma 中无旧值冲突 → **写入成功**
- Owner 的 `in_memory_store_` 中：`OBJECT_IN_PLASMA` marker 可能已被 `memory_store_->Delete()` 移除（由 recovery 定时器执行），此时 `in_memory_store_.Put()` 也能成功写入 error
- Location update 传播 → caller 拉到 error → `ray.get()` 抛精确异常

### 13.5 三个场景的完整对比

| 场景 | ObjectRef 状态 | MarkTaskReturnObjectsFailed 效果 | `ray.get()` 行为 |
|------|---------------|--------------------------------|-----------------|
| Worker 崩溃，节点存活，有重试 | `OBJECT_IN_PLASMA` marker 在，plasma 数据在 | 对已 yield ref 是 no-op | 正常从 plasma 拉取（数据还在） |
| Worker 崩溃，节点存活，无重试 | `OBJECT_IN_PLASMA` marker 在，plasma 数据在 | 对已 yield ref 是 no-op | 正常从 plasma 拉取（数据还在） |
| Worker 节点宕机，lineage reconstruction 失败 | `OBJECT_IN_PLASMA` marker 被 Delete 或指向丢失数据，plasma 数据丢失 | **成功覆盖**：error 写入 plasma | 短路返回 error，不陷入无望的 plasma 拉取 |

### 13.6 如果不写入 error 对象会怎样

当所有恢复路径都确定失败后（无重试 + plasma 数据丢失 + lineage reconstruction 失败），如果不写入 error 对象：

1. Owner 的 `in_memory_store_` 中 `OBJECT_IN_PLASMA` marker 已被 `memory_store_->Delete()` 移除（或仍指向丢失的数据）
2. Caller 的 `memory_store_` 中仍有 `OBJECT_IN_PLASMA` marker → `ray.get()` 走 plasma 路径
3. PullManager 向 Owner 订阅位置 → Owner 发布 `locations = {}` (空，死亡节点已移除)
4. `pending_creation=false`（没有重建在进行）→ PullManager 开始 timeout 计时
5. 等待 `fetch_fail_timeout_milliseconds`（默认 60s）后 → `fail_pull_request_(OBJECT_FETCH_TIMED_OUT)`
6. `MarkObjectsAsFailed` → 在 caller raylet 本地 plasma 写 error
7. Caller 最终拿到 `ObjectFetchTimedOutError` — **耗时 60s，且错误信息不精确**

**写入 error 对象后**：
1. Owner plasma 中有 error → location update → PullManager pull → caller plasma
2. Caller polling loop 发现 error → **几乎即时返回**
3. 错误类型精确：`RayTaskError` / `OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED`

**结论**：写入 error 对象不是"主动丢弃可恢复的数据"，而是"数据已确定不可恢复后，快速通知所有等待方"。三重保障已经穷尽：Worker 重试（`RetryTaskIfPossible`）→ Lineage reconstruction（`RecoverObject` → `ResubmitTask`）→ 所有重试耗尽 → 才执行 `MarkTaskReturnObjectsFailed` / `recovery_failure_callback_`。

---

## 14. Caller 的 memory_store_ 中 OBJECT_IN_PLASMA 的来源

### 14.1 问题的核心

`ray.get()` 的第一步是查调用方自己的 `in_memory_store_`。如果返回 `OBJECT_IN_PLASMA`，则转入 plasma 路径。**关键问题**：这个 marker 是谁、什么时候写入调用方的 `memory_store_` 的？

**注意**：每个进程（Driver / Worker）都有自己独立的 `in_memory_store_`。Owner 的 `in_memory_store_` 和 Caller 的 `in_memory_store_` 是**完全不同**的内存空间。

### 14.2 Owner 的 `in_memory_store_` 中 OBJECT_IN_PLASMA 的来源

Owner 的 `TaskManager::HandleTaskReturn` 在收到 Worker 的返回值时写入：

```cpp
// task_manager.cc:568
if (return_object.in_plasma()) {
    reference_counter_.UpdateObjectPinnedAtRaylet(object_id, worker_node_id);
    in_memory_store_.Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
                         object_id,
                         reference_counter_.HasReference(object_id));
}
```

对 streaming generator，在 `HandleReportGeneratorItemReturns` 中：

```cpp
// task_manager.cc:837
StatusOr<bool> put_res = HandleTaskReturn(object_id, returned_object, worker_node_id,
    store_in_plasma_ids.contains(object_id));
```

同样调用 `HandleTaskReturn`，同样在 `in_plasma=true` 时写入 `OBJECT_IN_PLASMA`。

### 14.3 Caller（非 Owner）的 `in_memory_store_` 中 OBJECT_IN_PLASMA 的三条来源路径

Caller 不是 Owner，所以 Caller 的 `TaskManager` 不会收到 `HandleTaskReturn`。Caller 的 `memory_store_` 中的 `OBJECT_IN_PLASMA` 有以下来源：

#### 来源 1：Task 参数注入（`core_worker.cc:3560`）

当 Owner (Driver) 提交一个 task 到 Caller Worker 执行，task 的参数如果包含 ObjectRef（by-reference 参数），Caller Worker 在解析依赖时**主动写入**：

```cpp
// core_worker.cc:3557-3568
// Attach the argument's owner's address. This is needed to retrieve the
// value from plasma.
reference_counter_->AddBorrowedObject(arg_id, ObjectID::Nil(), task.ArgRef(i).owner_address());
borrowed_ids->push_back(arg_id);
// We need to put an OBJECT_IN_PLASMA error here so the subsequent call to Get()
// properly redirects to the plasma store.
// NOTE: This needs to be done after adding reference to reference counter
// otherwise, the put is a no-op.
if (!options_.is_local_mode) {
    memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
                       task.ArgObjectId(i),
                       reference_counter_->HasReference(task.ArgObjectId(i)));
}
```

**这段代码不区分普通 ObjectRef 还是 streaming generator yield 出的 ref**——只要是 by-reference 的 ObjectRef 参数，一律写入 `OBJECT_IN_PLASMA`。

**Caller Worker B 的 `ray.get(block_ref)` 流程**：
1. B 的 `memory_store_` → `OBJECT_IN_PLASMA` → 转 plasma 路径
2. B 的 raylet → `OBD.SubscribeObjectLocations` → 向 Owner A 订阅位置
3. A 的 `ReferenceCounter.locations` → 发布节点 C 有该对象
4. B 的 PullManager → 从 C 的 raylet pull 对象 → B 的本地 plasma
5. B 的 polling loop → `store_client_->Get()` → 拿到值

#### 来源 2：FutureResolver（`future_resolver.cc`）

当 Caller(B) 持有一个 borrowed ref 但 `memory_store_` 中还没有值时，B 主动向 Owner(A) 发 `GetObjectStatus` RPC：

```cpp
// future_resolver.cc:82-97
if (reply.status() == rpc::GetObjectStatusReply::CREATED) {
    // Object is either in Plasma, or returned directly in reply
    if (data.empty()) {
        // Object not returned directly → metadata 中含 OBJECT_IN_PLASMA
    }
    in_memory_store_->Put(RayObject(data_buffer, metadata_buffer, inlined_refs),
                          object_id, ...);
}
```

注意这里 **没有显式写入 `RayObject(OBJECT_IN_PLASMA)`**，而是写入 Owner 返回的 metadata。如果 Owner 返回的 metadata 包含 `OBJECT_IN_PLASMA` error code，Caller 就自然获得了这个标记。

其他情况：
- `!status.ok()`（Owner 不可达）→ 写入 `OWNER_DIED` error
- `reply.status() == OUT_OF_SCOPE` → 写入 `OBJECT_DELETED` error

#### 来源 3：HandlePinObjectIDs / PromoteObjectToPlasma（`core_worker.cc`）

当 Owner 通过 `PinObjectIDs` RPC 通知 Caller Worker "这个对象现在在 plasma 中"：

```cpp
// core_worker.cc:4648
memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
                   object_id,
                   reference_counter_->HasReference(object_id));
```

当小对象被 promote 到 plasma 时：

```cpp
// core_worker.cc:1020
memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
                   object_id,
                   reference_counter_->HasReference(object_id));
```

---

## 15. 通用场景下 Owner/Caller/Producer 三方角色与交互

### 15.1 角色定义

| 角色 | 职责 | 拥有什么 |
|------|------|---------|
| **Owner** | 持有 `ReferenceCounter`，追踪 `locations`，负责 lineage reconstruction | `in_memory_store_` 中有 `OBJECT_IN_PLASMA` marker（由 `HandleTaskReturn` 写入） |
| **Caller** | 调用 `ray.get(ref)` | 自己的 `in_memory_store_` 中有 `OBJECT_IN_PLASMA` marker（来源见第 14 节） |
| **Producer** | 执行 task，产生 object 写入 plasma | 不持有 ref 的元数据，不参与 get 路径 |

### 15.2 三方可以完全不同，也可以两两重叠

```python
# 场景1: 三方完全分离
# 进程A (Driver/Owner)     进程B (Caller Worker)     进程C (Producer Worker)
ref = foo.remote()         # A 创建 ref → A 是 Owner
bar.remote(ref)            # A 把 ref 传给 B
                           # B 调用 ray.get(ref) → B 是 Caller
                                                     # C 执行 foo() → C 是 Producer
```

| 场景 | Owner | Caller | Producer | 示例 |
|------|-------|--------|----------|------|
| Ray Data | Driver | Driver | Worker | `on_data_ready()` 中 Driver 自己 `ray.get()` |
| 典型分布式 | Driver | Worker B | Worker A | Driver 创建 ref，传给 B 的 task 作为参数 |
| 简单单进程 | Driver | Driver | Driver | local mode |
| 嵌套 task | Worker A | Worker A | Worker B | A 创建子 task，自己 get 结果 |

### 15.3 Owner 自己 `ray.get()` vs Caller（非 Owner）`ray.get()` 的对比

| | Owner 调用 `ray.get()` | Caller（非 Owner）调用 `ray.get()` |
|---|---|---|
| `memory_store_` 中 `OBJECT_IN_PLASMA` 来源 | `HandleTaskReturn` 自己写入 | Task 参数注入 / FutureResolver RPC |
| 对象位置信息 | `ReferenceCounter.locations` 本地直接查 | 通过 pubsub 向 Owner 订阅 |
| Plasma pull 触发 | 自己的 raylet 直接 pull | 自己的 raylet → Owner 订阅位置 → pull |
| 对象到达通知 | 自己的 raylet → polling | 自己的 raylet → polling（机制相同） |
| 节点死亡检测 | `ResetObjectsOnRemovedNode()` 本地触发 | Owner 推送 location update（死亡节点被移除） |
| Lineage reconstruction 触发 | Owner 自己执行 | Owner 自己执行，Caller 不感知 |

**核心差异只有一点**：Owner 本地持有 `ReferenceCounter`，是位置信息的权威来源和 reconstruction 的发起者；Caller 通过订阅 Owner 被动获取位置更新，不参与 reconstruction 决策。`ray.get()` 的 polling 机制对两者完全一致。

### 15.4 Caller（非 Owner）`ray.get()` 的完整路径

```
Caller Worker B:
  ray.get(block_ref)
    │
    ├→ B.memory_store_->Get(block_ref)
    │   └→ 返回 RayObject(OBJECT_IN_PLASMA)    ← 来源: task参数注入 或 FutureResolver
    │
    ├→ B.core_worker.GetObjects() → IsInPlasmaError() detected
    │
    ├→ B.plasma_store_provider_->Get()
    │   ├→ B.raylet.AsyncGetObjects()          → B 的 raylet 开始 pull
    │   │   └→ B.raylet.PullManager
    │   │       └→ OBD.SubscribeObjectLocations()
    │   │          → RPC 订阅到 Owner A
    │   │
    │   │                         Owner A:
    │   │           PublishObjectLocationSnapshot()
    │   │           → 发布 locations = {C}
    │   │
    │   │   B.raylet 收到 location update:
    │   │     PullFromRandomLocation() → 向 C 的 raylet send_pull_request
    │   │     → C 推送对象到 B 的 plasma store
    │   │
    │   └→ while 循环轮询 B 的本地 plasma store
    │       store_client_->Get() → 发现对象 → 返回
```

---

## 16. 普通对象 vs Streaming Generator ref 在 Caller memory_store_ 处理上的区别

### 16.1 代码层面：不区分

`core_worker.cc:3560` 的参数注入逻辑对所有 by-reference 的 ObjectRef 一视同仁：

```cpp
// 不区分 ref 类型，一律写入 OBJECT_IN_PLASMA
memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
                   task.ArgObjectId(i), ...);
```

### 16.2 使用方式层面：有本质区别

**普通 task return ref**：

```
Driver (A):  ref = foo.remote()           # A 是 Owner
Worker (B):  bar.remote(ref)               # A 把 ref 传给 B 作为参数
             → B.memory_store_ 写入 OBJECT_IN_PLASMA  ← 来源1生效
             → B 调用 ray.get(ref)         # B 是 Caller
```

普通 ref **经常被传给其他 Worker 作为 task 参数**，所以来源 1（task 参数注入）**经常触发**。

**Streaming generator 中间 ref（block_ref, meta_ref）**：

```
Driver (A):  gen_ref = generator.remote()  # A 是 Owner
             _next_sync() → 拿到 block_ref, meta_ref  # A 自己消费
             ray.get(meta_ref)              # A 自己调用 ray.get()
```

Streaming generator yield 的中间 ref **不被传给其他 Worker 作为 task 参数**。Ray Data 的 `on_data_ready()` 是 Driver 自己通过 `_next_sync()` 逐个 pull generator yield 出的 ref，然后 Driver 自己调 `ray.get()`。

**因此：中间 ref 不会走来源 1（task 参数注入）路径**。Driver 即 Owner，Driver 自己的 `in_memory_store_` 中的 `OBJECT_IN_PLASMA` 由 `HandleReportGeneratorItemReturns` → `HandleTaskReturn` 写入。

### 16.3 区别总结

| | 普通 return ref | Streaming generator 中间 ref |
|---|---|---|
| 代码逻辑是否区分 | 不区分 | 不区分 |
| 实际是否走 task 参数注入路径 | 经常走（传给其他 Worker） | 不走（Driver 自己消费） |
| Caller `memory_store_` 中 `OBJECT_IN_PLASMA` 来源 | 来源 1：task 参数注入 | 不适用（Driver 即 Owner，由 `HandleTaskReturn` 写入） |
| `ray.get()` 执行者 | 可能是 Caller Worker | 始终是 Driver (Owner) |
| Owner 与 Caller 关系 | 可能不同进程 | 同一进程 |

**核心结论**：代码不区分，但 streaming generator 的设计模式决定了中间 ref 不会被传给其他 Worker 作为参数，所以实际上不存在"Caller Worker 的 `memory_store_` 中有 streaming generator 中间 ref 的 `OBJECT_IN_PLASMA`"这种情况。在 Ray Data 场景下，所有 `ray.get()` 都在 Driver（Owner）进程中执行。

---

## 17. ObjectRef 携带 Owner 地址与非 Owner Worker 拉取 Object 实际内容的完整链路

### 17.1 ObjectRef 确实携带 Owner 地址

**Proto 定义** (`common.proto:713`)：

```protobuf
message ObjectReference {
  bytes object_id = 1;         // ObjectID
  Address owner_address = 2;   // Owner 的地址（ip, port, worker_id, node_id）
  string call_site = 3;        // 调用位置（调试用）
  optional string tensor_transport = 4;
}
```

**Python ObjectRef** (`object_ref.pxi:41`)：

```python
cdef class ObjectRef(BaseID):
    cdef:
        CObjectID data
        c_string owner_addr      # ← 持有序列化的 owner address
        c_bool in_core_worker
        c_string call_site_data
        object _tensor_transport

    def __init__(self, id, owner_addr="", call_site_data="", ...):
        self.owner_addr = owner_addr
```

### 17.2 Owner 地址的写入时机

**对 Owner 进程**：在 `AddOwnedObject` 时写入自身地址：

```cpp
// reference_counter.h:319
Reference(rpc::Address owner_address, ...)
    : owner_address_(std::move(owner_address)),
      owned_by_us_(true), ...
```

**对 Borrower 进程**：owner_address 通过以下方式传播：

#### 路径 A：Task 参数反序列化（`RegisterOwnershipInfoAndResolveFuture`）

当 ObjectRef 作为 task 参数被序列化传给另一个 Worker 时：

**序列化端** (`_raylet.pyx:4097`):
```python
def serialize_object_ref(self, ObjectRef object_ref):
    # C++ GetOwnershipInfo 获取 owner_address + object_status
    op_status = CCoreWorkerProcess.GetCoreWorker().GetOwnershipInfo(
        c_object_id, &c_owner_address, &serialized_object_status)
    return (object_ref, c_owner_address.SerializeAsString(), serialized_object_status)
```

**反序列化端** (`_raylet.pyx:4112`):
```python
def deserialize_and_register_object_ref(self, object_ref_binary, ...,
                                         serialized_owner_address, serialized_object_status):
    c_owner_address.ParseFromString(serialized_owner_address)
    CCoreWorkerProcess.GetCoreWorker().RegisterOwnershipInfoAndResolveFuture(
        c_object_id, c_outer_object_id, c_owner_address, serialized_object_status)
```

**C++ `RegisterOwnershipInfoAndResolveFuture`** (`core_worker.cc:944`):
```cpp
void CoreWorker::RegisterOwnershipInfoAndResolveFuture(
    const ObjectID &object_id, const ObjectID &outer_object_id,
    const rpc::Address &owner_address, const std::string &serialized_object_status) {
  // 将 owner_address 存入 Borrower 的 ReferenceCounter
  reference_counter_->AddBorrowedObject(object_id, outer_object_id, owner_address);

  // 如果序列化时已内联了 object status，直接处理
  if (object_status.has_object() && !reference_counter_->OwnedByUs(object_id)) {
    future_resolver_->ProcessResolvedObject(
        object_id, owner_address, Status::OK(), object_status);
  }
}
```

**`AddBorrowedObject`** (`reference_counter.cc:124`):
```cpp
bool ReferenceCounter::AddBorrowedObjectInternal(const ObjectID &object_id,
    const ObjectID &outer_id, const rpc::Address &owner_address, ...) {
  auto it = object_id_refs_.find(object_id);
  if (it == object_id_refs_.end()) {
    it = object_id_refs_.emplace(object_id, Reference()).first;
  }
  it->second.owner_address_ = owner_address;  // ← 存储 owner 地址
  ...
}
```

#### 路径 B：Task 参数注入（`core_worker.cc:3557`）

当 Owner 提交 task 到 Worker 执行，by-reference 参数直接附带 `owner_address`：

```cpp
// core_worker.cc:3557-3568
reference_counter_->AddBorrowedObject(arg_id, ObjectID::Nil(), task.ArgRef(i).owner_address());
// task.ArgRef(i) 返回 ObjectReference proto，其中包含 owner_address
```

#### 路径 C：`PushTaskReply` / `ReportGeneratorItemReturns`

Worker 执行完 task 后，RPC 回复中包含 `return_object`（`rpc::ReturnObject`），其中包含 `owner_address`（通过 `task.ArgRef(i).owner_address()` 传递给 Driver）。

### 17.3 非 Owner Worker 拉取 Object 实际内容的完整链路

以 Worker B 持有 borrowed ref、需要获取 object 实际内容为例：

```
进程 A (Owner)                    进程 B (Borrower)              进程 C (Producer)
═══════════════                   ════════════════               ═══════════════════

1. ObjectRef 传播阶段:

ref = foo.remote()                # A 创建 ref, ref.owner_address = A
                                  # A.reference_counter_.AddOwnedObject(ref, A.address)
bar.remote(ref)                   # A 序列化 ref → (id, owner_addr=A, object_status)
                                  # B 反序列化 → RegisterOwnershipInfoAndResolveFuture
                                  # B.reference_counter_.AddBorrowedObject(ref, A.address)
                                  # B.memory_store_->Put(OBJECT_IN_PLASMA, ref)

2. Object 内容拉取阶段 (B 调用 ray.get(ref)):

                                  B: ray.get(ref)
                                    ├→ B.memory_store_->Get(ref)
                                    │   └→ 返回 OBJECT_IN_PLASMA
                                    │
                                    ├→ B.core_worker.GetObjects()
                                    │   └→ reference_counter_->GetOwnerAddresses({ref})
                                    │      → 从 B 的 ReferenceCounter 查到 owner_address = A
                                    │
                                    ├→ B.plasma_store_provider_->Get(
                                    │       {ref}, owner_addresses={A.address}, timeout_ms)
                                    │   ├→ B.raylet.AsyncGetObjects({ref}, {A.address})
                                    │   │   └→ B.raylet 的 LeaseDependencyManager
                                    │   │       → PullManager.Pull({ref})
                                    │   │           → OBD.SubscribeObjectLocations(
                                    │   │                 callback_key, ref, A.address)
                                    │   │               → RPC: SubscribeSubMessage
                                    │   │                 发往 A
                                  A: ProcessSubscribeObjectLocations(ref)
                                    │   → reference_counter_->PublishObjectLocationSnapshot(ref)
                                    │   → PushToLocationSubscribers()
                                    │   → 发布: locations = {C}, pending_creation=false
                                    │
                                  B.raylet: OBD callback
                                    │   → OnLocationChange(ref, client_ids={C}, ...)
                                    │   → PullFromRandomLocation()
                                    │       → send_pull_request_(ref, C)
                                    │
                                  C.raylet: 收到 pull 请求
                                    │   → PushManager 推送对象到 B 的 plasma store
                                    │
                                  B: 对象到达 B 的本地 plasma store
                                    │   → HandleObjectLocal()
                                    │   → polling loop: store_client_->Get() 发现对象
                                    │   → 返回值

3. 特殊情况: 小对象直接内联在 GetObjectStatus 回复中:

                                  B: future_resolver_->ResolveFutureAsync(ref, A.address)
                                    → GetObjectStatus RPC → A
                                  A: 返回 { status=CREATED, object { data=<内联值> } }
                                  B: ProcessResolvedObject()
                                    → in_memory_store_->Put(RayObject(data_buffer, meta_buffer), ref)
                                    → ray.get() 在 memory_store 阶段直接拿到值，不走 plasma
```

### 17.4 Owner 地址的关键作用

Owner 地址在整条链路中有三个关键用途：

**1. Plasma Store Provider 定位 Owner**：

`plasma_store_provider_->Get()` 需要 `owner_addresses` 参数。这些地址不是从 ObjectRef Python 对象读取的，而是从 **Caller 自己的 `ReferenceCounter`** 查询的：

```cpp
// core_worker.cc:1603
auto owner_addresses = reference_counter_->GetOwnerAddresses(object_ids);
plasma_store_provider_->Get(object_ids, owner_addresses, timeout_ms, &result_map);
```

这些 `owner_addresses` 传给 raylet，raylet 用它们建立到 Owner 的 pubsub 订阅。

**2. OwnershipBasedObjectDirectory 订阅 Owner**：

`OBD.SubscribeObjectLocations()` 使用 `owner_address` 确定订阅目标：

```cpp
// ownership_object_directory.cc:315
rpc::WorkerObjectLocationsSubMessage request;
request.set_intended_worker_id(owner_address.worker_id());
request.set_object_id(object_id.Binary());
// → 发送到 owner_address 对应的 CoreWorker
```

**3. FutureResolver 直接联系 Owner**：

`ResolveFutureAsync()` 用 `owner_address` 建立 RPC 连接：

```cpp
// future_resolver.cc:28
auto conn = owner_clients_->GetOrConnect(owner_address);
rpc::GetObjectStatusRequest request;
request.set_object_id(object_id.Binary());
request.set_owner_worker_id(owner_address.worker_id());
conn->GetObjectStatus(std::move(request), callback);
```

### 17.5 Owner 地址传播的三种载体对比

| 载体 | 携带位置 | 传播路径 | 写入 Borrower RC 的时机 |
|------|---------|---------|----------------------|
| **ObjectReference proto** | `task.ArgRef(i).owner_address()` | Task 提交时附带在参数中 | `AddBorrowedObject` (core_worker.cc:3557) |
| **序列化元组** | `serialize_object_ref` 返回的 `(id, owner_addr, status)` | ObjectRef 跨进程序列化/反序列化 | `RegisterOwnershipInfoAndResolveFuture` (core_worker.cc:951) |
| **PushTaskReply** | `reply.return_objects(i)` 中隐含 | Task 完成返回给 Owner | 仅 Owner 侧，不传播给 Borrower |

### 17.6 完整时序图：从 ObjectRef 创建到 Worker 拉取内容

```
Driver (Owner A)              Worker (Borrower B)            Worker (Producer C)
═════════════════             ════════════════════           ═══════════════════════

① ref = foo.remote()
   A.reference_counter_
     .AddOwnedObject(ref, A.address)
   A.memory_store_
     .Put(OBJECT_IN_PLASMA, ref)

② bar.remote(ref)
   序列化 ref:
     serialize_object_ref(ref)
     → (ref_id, owner_addr=A, object_status)
                                  ③ 反序列化:
                                    deserialize_and_register_object_ref(...)
                                    → B.reference_counter_
                                       .AddBorrowedObject(ref, A.address)
                                    → B.memory_store_
                                       .Put(OBJECT_IN_PLASMA, ref)

                                  ④ B 执行 task, 需要 ray.get(ref):
                                    B.reference_counter_
                                      .GetOwnerAddresses({ref})
                                      → {A.address}
                                    B.plasma_store_provider_
                                      .Get({ref}, {A.address})

                                  ⑤ B.raylet 向 A 订阅:
                                    OBD.SubscribeObjectLocations(ref, A.address)
                                                                  → RPC →

   ⑥ A 发布位置:
     PublishObjectLocationSnapshot(ref)
     → PushToLocationSubscribers()
     → 发布: locations = {C}

                                  ⑦ B.raylet 收到 location update:
                                    PullFromRandomLocation()
                                    → send_pull_request_(ref, C)
                                                                                      ⑧ C 推送对象:
                                                                                        PushManager
                                                                                        → 对象写入 B 的 plasma

                                  ⑨ B polling 发现:
                                    store_client_->Get()
                                    → 返回对象值
                                    → ray.get() 完成
```

### 17.7 Ray Data 场景下的简化

在 Ray Data 中，Owner = Driver = `ray.get()` 的调用方，因此：

- **不存在 Borrower Worker 拉取 streaming generator 中间 ref 的场景**
- Driver 自己查自己的 `ReferenceCounter` 获取 `owner_addresses`
- Driver 自己的 `memory_store_` 中已有 `OBJECT_IN_PLASMA` marker（由 `HandleReportGeneratorItemReturns` 写入）
- Driver 的 raylet 订阅 Driver 自己的 `PublishObjectLocationSnapshot` — 但**因为 Driver 的 CoreWorker 既是 Owner 又是 Caller，raylet 和 CoreWorker 在同一进程，路径更短**

但非 Owner Worker 拉取普通 task 参数中的 ObjectRef 时，完整走上述链路。
