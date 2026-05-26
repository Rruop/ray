# Worker Finished but Unconsumed Task Diagnostic 设计文档

## 1. 概述

本文档描述了 Ray Data 进度日志中新增的 "Finished but Unconsumed" 任务诊断功能。该功能通过 `ObjectRefGenerator.completed()` 检测 worker 端已完成但 driver 端尚未消费完所有输出的任务，并在进度日志中展示该分解信息，帮助用户理解任务数量的 "gap"。

## 2. 背景与问题

### 2.1 现象

Ray Data 进度日志显示 `Tasks: 5000`，但 Dashboard Task Table 仅显示约 3200 个 RUNNING 任务。约 1800 个任务的差异来自 worker 已完成执行（GCS 中状态为 FINISHED），但 driver 尚未消费完 streaming generator 的所有输出。

### 2.2 原因

Ray Data 使用 streaming generator 从 worker 向 driver 流式传输数据块。一个任务在 driver 视角的生命周期为：

```
submitted → running (产出数据块) → finished (所有数据块被消费)
```

当存在背压时，worker 可能已经执行完毕并将所有数据块放入 object store，但 driver 由于下游消费速度慢，尚未读取完所有数据块。此时任务在 GCS 中已标记为 FINISHED，但在 Ray Data 的 `num_active_tasks` 中仍被计为 "active"。

### 2.3 用户痛点

- 进度日志中 `Tasks: 5000` 无法反映真实的 worker 负载
- 用户需要手动运行 State API 查询才能理解 gap 的来源
- 无法直观判断是 worker 计算瓶颈还是 driver 消费瓶颈

## 3. 解决方案

### 3.1 核心思路

利用 `ObjectRefGenerator.completed()` 返回的 `ObjectRef`（在 worker 完成时 resolve）来检测 worker 端完成状态，然后在进度日志中展示分解信息。

### 3.2 检测机制

在 `DataOpTask.on_data_ready()` 的末尾，通过非阻塞的 `ray.wait` 检测 worker 是否已完成：

```python
if not self._worker_finished and not self._has_finished:
    ready, _ = ray.wait(
        [self._streaming_gen.completed()], timeout=0, fetch_local=False
    )
    if ready:
        self._worker_finished = True
        self._worker_finished_callback()
```

**执行逻辑**：
- `_worker_finished = False` 且 worker 未完成：每次 `on_data_ready()` 调用时执行 `ray.wait` 检测（timeout=0）
- `_worker_finished = False` 且 worker 已完成：检测到后设 `_worker_finished = True`，触发回调，后续不再检测
- `_worker_finished = True`：跳过检测，零开销

**关于背压场景**：即使 `max_bytes_to_read == 0`（输出背压），仍然执行检测。背压恰好是 "worker 已完成但输出未消费" 最容易出现的场景，跳过检测会导致诊断信息延迟，不利于排查问题。单次 `ray.wait` 在 driver 进程内 HashMap 查找的开销（5-50 微秒）相对 executor 迭代周期（100ms 级）完全可以忽略。

### 3.3 输出格式

- 无 gap 时：`Tasks: 5000`（保持不变）
- 有 gap 时：`Tasks: 5000 (3200 running, 1800 finished)`

### 3.4 结合进度日志排查问题

进度日志中可能出现的组合示例：

```
# 正常运行，无 gap
Tasks: 100

# 有 gap，部分 worker 已完成但输出未消费
Tasks: 100 (60 running, 40 finished)

# 有 gap + 输出背压
Tasks: 100 (60 running, 40 finished) [backpressured:outputs(ResourceBudget)]

# 有 gap + 任务提交背压
Tasks: 100 (60 running, 40 finished) [backpressured:tasks(DownstreamCapacity)]
```

**排查指引：**

| 现象 | 状态 | 含义 | 行动 |
|------|------|------|------|
| `finished` 高 + 输出背压 | 持续 | worker 产出已完成，但 object store 预算耗尽或下游队列堆积超过阈值（`DownstreamCapacity`），driver 被限制消费输出（`max_bytes_to_read == 0`） | 增大 object store 内存，或优化下游算子消费速度 |
| `finished` 高 + 任务提交背压 | 持续 | worker 产出已完成，同时资源不足无法提交新任务 | 增加 CPU/GPU 资源 |
| `finished` 高 + 无背压 | 瞬态 | worker 刚完成，最后几个 block 的 `HandleReportGeneratorItemReturns` RPC 还在 in-flight，`_next_sync(timeout_s=0)` 暂时读不到。正常情况下几轮 executor 迭代（数百毫秒）内自动消除 | 无需干预 |
| `finished` 高 + 无背压 + 持续不降 | 异常 | block RPC 丢失、executor 循环本身存在瓶颈、或 generator 状态异常 | 检查 worker 是否存活、网络是否正常、executor 迭代耗时 |
| `finished` 为 0 + 任务提交背压 | 持续 | 所有 worker 都在 running，资源不足无法提交新任务 | 增加集群资源 |
| `finished` 为 0 + 无背压 | 正常 | 所有 worker 都在计算中，pipeline 正常流转 | 无需干预 |

**关键原理**：`finished` 高 + 无背压不应该持续存在。如果 worker 已完成但输出未消费，output queue 会持续增长，最终必然触发 `ResourceBudget` 或 `DownstreamCapacity` backpressure policy。因此 `finished` 持续高 + 无背压是需要排查的异常状态。

## 4. 修改的文件

### 4.1 `physical_operator.py` - DataOpTask 类

| 修改项 | 说明 |
|--------|------|
| 新增 `worker_finished_callback` 参数 | 构造函数新增可选回调参数，默认 `lambda: None` |
| 新增 `_worker_finished` 标志 | 布尔值，记录 worker 是否已完成 |
| 新增 `worker_finished` 属性 | 返回 `_worker_finished or _has_finished` |
| `on_data_ready()` 末尾新增检测逻辑 | 通过 `ray.wait(completed(), timeout=0)` 非阻塞检测 |

### 4.2 `op_runtime_metrics.py` - OpRuntimeMetrics 类

| 修改项 | 说明 |
|--------|------|
| 新增 `num_tasks_worker_finished` metric field | 跟踪 worker 已完成但未被完全消费的任务数 |
| 新增 `_worker_finished_task_indices` 集合 | 用于在 `on_task_finished` 时正确递减计数 |
| 新增 `on_task_worker_finished()` 方法 | 递增计数器并记录 task_index |
| 修改 `on_task_finished()` 方法 | 在开头检查并递减 worker_finished 计数 |

### 4.3 `map_operator.py` - MapOperator 类

| 修改项 | 说明 |
|--------|------|
| `_submit_data_task()` 新增 `_worker_finished_cb` 闭包 | 调用 `self._metrics.on_task_worker_finished(task_index)` |
| `DataOpTask` 构造时传入 `worker_finished_callback` | 通过 `functools.partial` 绑定 `task_index` |

### 4.4 `streaming_executor_state.py` - format_op_state_summary()

| 修改项 | 说明 |
|--------|------|
| 读取 `num_tasks_worker_finished` | 获取 worker 已完成的任务数 |
| 条件格式化任务描述 | `worker_finished > 0` 时展示分解信息 |

## 5. Ray Core 所有权协议与完成通知机制

### 5.1 `_generator_ref` 的所有权模型

Ray 使用**所有权协议（ownership protocol）**来管理对象引用。Driver 提交任务时创建 `ObjectRef`，Driver 就是该 ObjectRef 的 **owner**。完成通知通过 owner 直连传递，**不经过 GCS**。

`ObjectRefGenerator.completed()` 返回的 `self._generator_ref` 是 driver 在提交 streaming generator 任务时创建的 ObjectRef。当 worker 完成执行后，return object 通过 gRPC 直接发送给 driver（owner），写入 driver 进程内的 `CoreWorkerMemoryStore`。

### 5.2 完成通知的完整链路

```
Worker 执行完 generator task
  → Worker 通过 gRPC 直接回复 Driver (PushTaskReply)
    → Driver 的 NormalTaskSubmitter 收到回调
      → task_manager_.CompletePendingTask(task_id, reply, addr, ...)
        → 对于 streaming generator:
            MarkEndOfStream(generator_id, ...)          // task_manager.cc:1086
        → HandleTaskReturn(generator_return_id, ...)    // task_manager.cc:1103
          → in_memory_store_.Put(object, object_id, ...) // task_manager.cc:603
```

**关键路径**：`in_memory_store_.Put()` 将 return object 写入 driver **进程内** 的 `CoreWorkerMemoryStore`（一个内存 HashMap），不涉及任何网络 I/O。

### 5.3 `ray.wait` 的检查路径

当我们调用 `ray.wait([gen.completed()], timeout=0, fetch_local=False)` 时：

```
Python: ray.wait([ObjectRef], timeout=0, fetch_local=False)
  → Cython: core_worker.wait(object_refs, ...)            // _raylet.pyx:3280
    → C++: CoreWorker::Wait(ids, num_returns, 0ms, ...)    // core_worker.cc:1483
      → memory_store_->Wait(memory_object_ids, ...)        // core_worker.cc:1543
        → GetImpl(id_vector, ...)                          // memory_store.cc:421
          → objects_.find(object_id)                       // 纯 HashMap 查找
```

**整个检查路径是 driver 进程内的本地内存操作**：
- 不查 GCS
- 不查远程 node / raylet
- 不触发网络 RPC
- 只是一次 `absl::flat_hash_map::find()` 查找

### 5.4 `fetch_local` 参数的含义

`fetch_local` 控制的是**是否触发数据传输**，不影响就绪状态的检查。

源码（`core_worker.cc:1555-1583`）：

```cpp
if (fetch_local) {
    // 触发从远程 plasma store 拉取对象到本地 plasma
    plasma_store_provider_->Wait(object_ids, owner_addresses, ...);
} else {
    // 不触发数据传输，直接标记为 ready
    for (const auto &object_id : plasma_object_ids) {
        ready.insert(object_id);
    }
}
```

| `fetch_local` | 行为 |
|---------------|------|
| `True` | 对象在远程 plasma 中时，触发数据传输到本地，等待传输完成后才算 ready |
| `False` | 对象在集群任意位置可用即算 ready，不触发实际数据传输 |

对于 `_generator_ref`，其 return object 是小对象（空或 error marker），已通过 `PushTaskReply` inline 发送到 driver 并存入 `in_memory_store_`。`memory_store_->Wait` 在第一步就找到它，根本不会走到 `plasma_object_ids` 分支。因此 `fetch_local=False` 只是一个保守选择。

### 5.5 与 Executor 主循环 `ray.wait` 的对比

| | Executor 主循环 | 本功能的检测 |
|---|---|---|
| **等待对象** | `ObjectRefGenerator`（streaming generator 本体） | `ObjectRef`（`completed()` 返回的 `_generator_ref`） |
| **Cython 转换** | `_raylet.pyx:3274`: `ref_or_generator._get_next_ref()` — 获取 generator 的下一个 yield 输出 ref | 直接传入 ObjectRef，无转换 |
| **就绪含义** | generator 的**下一个 yield** 有新数据块可读 | worker 侧**整个任务**已执行完毕 |
| **timeout** | `0.1s` — 作为事件循环的节奏控制，避免空转忙循环 | `0` — 纯即时检查，不需要等待 |
| **调用场景** | executor 迭代的入口，可能没有任何数据就绪 | `on_data_ready()` 内部，已确认有数据可处理 |

Executor 用 `timeout=0.1` 是因为它是事件循环的心跳——没有新事件时需要让出 CPU 短暂休眠，否则会 100% CPU 空转。而我们的检测发生在 `on_data_ready()` 内部（只有 executor 已检测到数据就绪才会调用），不存在空转问题。

## 6. 性能开销分析

### 6.1 `ray.wait` 单次调用开销

`ray.wait([ref], timeout=0, fetch_local=False)` 是非阻塞轮询，需穿越 Python → Cython → C++ 边界，最终执行 driver 进程内的 HashMap 查找。

| 指标 | 数值 |
|------|------|
| 单次调用延迟 | 5-50 微秒 |
| 是否涉及 RPC | 否（driver 进程内 `CoreWorkerMemoryStore` 的 HashMap 查找） |
| 是否查询 GCS | 否（所有权协议，worker 直接通知 driver） |
| GIL 影响 | 需要获取/释放 GIL，单线程执行器中无竞争 |

### 6.2 `completed()` 调用开销

`ObjectRefGenerator.completed()` 仅返回 `self._generator_ref`，不分配新 ObjectRef。唯一的临时分配是 `[self._generator_ref]` 单元素 list，可忽略。

**结论**：无 GC 压力。

### 6.3 整体开销估算

| 场景 | 活跃任务数 | 轮询频率 | `ray.wait` 调用/秒 | 总开销/秒 |
|------|-----------|---------|-------------------|----------|
| 小规模 | 10 | 10/s | 100 | < 1ms |
| 中规模 | 100 | 10/s | 1,000 | 5-50ms |
| 大规模 | 1,000 | 10/s | 10,000 | 50-500ms |

**注意**：worker 一旦完成（`_worker_finished = True`），后续调用直接跳过检测，开销归零。因此实际稳态开销远低于上表估算。

### 6.4 潜在的进一步优化方向

| 方向 | 描述 | 收益 | 复杂度 |
|------|------|------|--------|
| 批量检测 | 将 `completed()` ref 合并到 `process_completed_tasks` 的统一 `ray.wait` 中 | 将 N 次 `ray.wait` 降为 0 次额外调用 | 高，需重构 executor 主循环 |
| 缓存 ref list | 在 `__init__` 中预构建 `[gen.completed()]` 列表 | 避免每次调用的临时 list 分配 | 低，但收益极小 |

### 6.5 结论

整体性能影响为 **低到中等**，大多数生产环境不会感知到。主要成本是 worker 完成前的每次轮询（driver 进程内 HashMap 查找，5-50 微秒），而 worker 完成后开销归零。`ray.wait` 的检查路径是纯本地内存操作，不涉及 GCS 或远程 node 查询。背压场景下不跳过检测，以确保诊断信息的及时性和准确性。

## 7. 验证方法

### 7.1 回归测试

```bash
pytest python/ray/data/tests/test_map.py -v -x
pytest python/ray/data/tests/test_op_runtime_metrics.py -v -x
pytest python/ray/data/tests/test_streaming_executor.py -v -x
```

### 7.2 手动验证

```python
import ray, time
ray.init(num_cpus=4)

# 创建一个下游消费较慢的 dataset，触发背压
ds = ray.data.range(1000).map(lambda x: x)  # 快速上游
for batch in ds.iter_batches(batch_size=10):
    time.sleep(0.1)  # 慢速下游
```

观察进度日志中是否出现 `Tasks: N (M running, K finished)` 格式的分解信息。
