# Ray Data Schedule Loop 性能优化指南

## 目录

1. [概述](#1-概述)
2. [Schedule Loop 工作机制](#2-schedule-loop-工作机制)
3. [_next_sync() 详解](#3-_next_sync-详解)
4. [Schedule Loop 时间过长的原因分析](#4-schedule-loop-时间过长的原因分析)
5. [Block Size 小导致性能问题的机制](#5-block-size-小导致性能问题的机制)
6. [优化方案决策](#6-优化方案决策)
7. [诊断工具使用指南](#7-诊断工具使用指南)
8. [关键代码位置](#8-关键代码位置)

---

## 1. 概述

### 1.1 Schedule Loop 指标定义

- **Prometheus 指标**: `data_sched_loop_duration_s`
- **统计 Timer**: `streaming_exec_schedule_s`
- **定义位置**: `streaming_executor.py:139-143`

### 1.2 计时范围

```python
# streaming_executor.py:463-475
while True:
    t_start = time.perf_counter()  # 开始计时
    continue_sched = self._scheduling_loop_step(self._topology)
    sched_loop_duration = time.perf_counter() - t_start  # 结束计时
```

**使用 `perf_counter()` 而非 `process_time()`**：确保包含 IO 等待时间（如 Ray Core RPC 调用）

---

## 2. Schedule Loop 工作机制

### 2.1 单次循环执行的操作

`_scheduling_loop_step()` 包含以下阶段：

| 阶段 | 操作 | 潜在耗时点 |
|------|------|-----------|
| 1 | `resource_manager.update_usages()` | 资源状态收集 |
| 2 | `process_completed_tasks()` | **ray.wait() + prepare_metadata()** |
| 3 | `resource_manager.update_usages()` | 资源状态更新 |
| 4 | `select_operator_to_run()` + `dispatch_next_task()` 循环 | operator 选择与任务分发 |
| 5 | `cluster_autoscaler.try_trigger_scaling()` | 集群扩缩容检查 |
| 6 | `actor_autoscaler.try_trigger_scaling()` | Actor 池扩缩容 |
| 7 | `update_operator_states()` | 状态更新 |
| 8 | `_refresh_progress_manager()` | 进度条刷新 |

### 2.2 ray.wait() 的机制

**关键点：`ray.wait()` 是一次性的批量等待，不是每个任务等待 100ms**

```python
# streaming_executor_state.py:448-453
if active_tasks:
    ready, _ = ray.wait(
        list(active_tasks.keys()),      # 所有 active tasks 的 waitable
        num_returns=len(active_tasks),  # 期望返回所有任务
        fetch_local=False,              # 不需要本地获取数据
        timeout=0.1,                    # 整体超时 100ms
    )
```

**工作原理**：
- 这是**一次性**等待所有 active tasks
- `timeout=0.1` 表示**整体最多等待 100ms**
- 如果在 100ms 内有任务完成，立即返回
- 如果没有任务完成，100ms 后返回空列表

**所以：N 个 active tasks 的 `ray.wait()` 开销是 O(1) 时间复杂度，最多 100ms**

---

## 3. _next_sync() 详解

### 3.1 Streaming Generator 架构

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    Ray Data Streaming Generator 架构                     │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   Worker 节点 (远程)                        Driver 节点 (本地)            │
│   ─────────────────                        ─────────────────            │
│                                                                          │
│   ┌─────────────────────┐                 ┌─────────────────────┐       │
│   │   map_task()        │                 │  ObjectRefGenerator │       │
│   │                     │   Object Ref    │                     │       │
│   │   for batch in data:│ ═══════════════>│  _next_sync()       │       │
│   │     output = fn(batch)                │    ↓                │       │
│   │     yield output    │───block_ref────>│  peek_object_ref    │       │
│   │     yield metadata  │───meta_ref─────>│  ray.wait()         │       │
│   │                     │                 │  try_read_next      │       │
│   └─────────────────────┘                 └─────────────────────┘       │
│                                                                          │
│   Object Store (plasma)                                                  │
│   ┌─────────────────────────────────────────────────────────────┐       │
│   │  block_ref_1 -> [actual data bytes]                         │       │
│   │  meta_ref_1  -> BlockMetadata(num_rows=1000, size=1MB)      │       │
│   │  block_ref_2 -> [actual data bytes]                         │       │
│   │  meta_ref_2  -> BlockMetadata(num_rows=1000, size=1MB)      │       │
│   └─────────────────────────────────────────────────────────────┘       │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### 3.2 _next_sync() 的具体步骤

```python
# object_ref_generator.py:188-242
def _next_sync(self, timeout_s):
    core_worker = self.worker.core_worker

    # Step 1: Peek - 查看下一个 ref 是否已经在 stream 中
    # 这是一个本地操作，检查 core_worker 内部的 stream buffer
    expected_ref, is_ready = core_worker.peek_object_ref_stream(self._generator_ref)

    # Step 2: Wait - 如果 ref 还没 ready，等待 timeout_s
    # is_ready=False 意味着 worker 还没 yield 这个 ref
    if not is_ready:
        _, unready = ray.wait([expected_ref], timeout=timeout_s, fetch_local=False)
        if len(unready) > 0:
            return ray.ObjectRef.nil()  # 超时，返回 nil

    # Step 3: Read - 从 stream 中消费这个 ref
    ref = core_worker.try_read_next_object_ref_stream(self._generator_ref)
    return ref
```

### 3.3 Meta Ref 是什么？

每个 Ray Data 任务使用 **streaming generator** 模式，每次 yield 两个对象：
1. **Block Ref**: 实际的数据块引用
2. **Meta Ref (BlockMetadataWithSchema)**: 包含该数据块的元信息

```python
@dataclass
class BlockMetadataWithSchema:
    metadata: BlockMetadata  # 包含 num_rows, size_bytes, exec_stats 等
    schema: Optional[Schema] # 数据的 schema
```

### 3.4 prepare_metadata() 的调用链

```
process_completed_tasks()
  └─ for task in ready_tasks:                    # ★ 串行遍历每个 task
       └─ task.prepare_metadata()                # ★ 串行调用
            ├─ _next_sync(timeout_s=0)           # 获取 block_ref (不等待)
            └─ _next_sync(timeout_s=0.1)         # 获取 meta_ref (★ 最多等待 100ms)
```

**问题**：每个 task 的 `prepare_metadata()` 最多等待 **100ms**，多个 tasks 是**串行**调用的。

---

## 4. Schedule Loop 时间过长的原因分析

### 4.1 时间分解

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Schedule Loop 时间分解                             │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  1. resource_manager.update_usages()           ~1-10ms              │
│     └─ 查询资源使用状态                                               │
│                                                                      │
│  2. process_completed_tasks():                                       │
│     ├─ ray.wait(active_tasks, timeout=0.1)     ~0-100ms (★ 固定)   │
│     ├─ 遍历 ready_tasks 调用 prepare_metadata()                      │
│     │   └─ 每个 task: _next_sync(timeout_s=0.1) ~0-100ms (★)       │
│     │   └─ N 个 blocks → N × 0-100ms            (★★★ 瓶颈)         │
│     ├─ ray.wait(meta_refs, timeout=0.1)        ~0-100ms             │
│     └─ 遍历处理 metadata                        ~O(N) 线性          │
│                                                                      │
│  3. select_operator_to_run 循环:               ~O(dispatches)       │
│     └─ 每次 dispatch 后调用 update_usages()                          │
│                                                                      │
│  4. 其他操作:                                                        │
│     ├─ autoscaler.try_trigger_scaling()        ~1-10ms              │
│     ├─ update_operator_states()                ~O(operators)        │
│     └─ _refresh_progress_manager()             ~1-10ms              │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### 4.2 主要瓶颈

**瓶颈 1：`prepare_metadata()` 中的 `_next_sync()` 串行等待**

```python
# physical_operator.py:287-291
if self._pending_meta_ref.is_nil():
    try:
        self._pending_meta_ref = self._streaming_gen._next_sync(
            timeout_s=METADATA_WAIT_TIMEOUT_S  # 100ms
        )
```

- 每个 task 的每个 block 都需要调用 `_next_sync()`
- 如果 metadata 还没 ready，最多等待 100ms
- **串行执行**：N 个 blocks → 最坏情况 N × 100ms

**瓶颈 2：小 block 导致更高的调度开销比**

```
Block Size = 1MB, 任务处理时间 = 10ms
Schedule Loop 开销 = 5ms
有效计算比 = 10ms / (10ms + 5ms) = 66%

Block Size = 128MB, 任务处理时间 = 1000ms
Schedule Loop 开销 = 5ms
有效计算比 = 1000ms / (1000ms + 5ms) = 99.5%
```

---

## 5. Block Size 小导致性能问题的机制

### 5.1 Streaming Generator 的工作方式

每个 Ray Data task 使用 **streaming generator**，会 yield 多个 blocks：

```python
@ray.remote(num_returns="streaming")
def map_task(input_block):
    for output_block in process(input_block):
        yield output_block      # block_ref
        yield block_metadata    # meta_ref
```

### 5.2 Block Size 小的影响

```
场景对比：处理 128MB 输入数据，有 10 个并发 tasks

配置 A (大 block): target_block_size = 128MB
  每个 task 输出: 1 block
  总 blocks: 10 blocks
  每次 schedule_loop 处理: ~10 个 pending_meta_tasks

配置 B (小 block): target_block_size = 1MB
  每个 task 输出: 128 blocks
  总 blocks: 1280 blocks
  每次 schedule_loop 处理: ~1280 个 pending_meta_tasks (★ 128倍!)
```

### 5.3 具体影响路径

```
┌─────────────────────────────────────────────────────────────────────────┐
│           Block Size 小 → scheduling_loop 变慢的完整路径                  │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  1. 更多的 ready_tasks 需要处理                                           │
│     └─ 同一个 task 可能多次出现在 ready 列表中                             │
│     └─ 因为 streaming generator 不断产出新的 block                        │
│                                                                          │
│  2. prepare_metadata() 调用次数增加 (★ 主要瓶颈)                          │
│     ┌─────────────────────────────────────────────────────────────┐      │
│     │ for task in ready_tasks:          # N 个 tasks              │      │
│     │     task.prepare_metadata()        # 每次最多 100ms         │      │
│     │         └─ _next_sync(timeout=0)   # 获取 block_ref         │      │
│     │         └─ _next_sync(timeout=0.1) # 获取 meta_ref (等待!)  │      │
│     └─────────────────────────────────────────────────────────────┘      │
│     最坏情况：N × 100ms                                                   │
│                                                                          │
│  3. pending_meta_tasks 列表变长                                           │
│     └─ 更多的 meta_refs 需要 batch wait                                   │
│     └─ ray.wait(meta_refs, ...) 虽然是批量，但处理循环变长                 │
│                                                                          │
│  4. 处理循环开销增加                                                       │
│     ┌─────────────────────────────────────────────────────────────┐      │
│     │ for state, task, meta_ref in pending_meta_tasks: # N 个     │      │
│     │     ray.get(meta_ref, timeout=0)                            │      │
│     │     task.complete_with_metadata(...)                         │      │
│     │     # 每次迭代：dict 查找、函数调用、对象创建                   │      │
│     └─────────────────────────────────────────────────────────────┘      │
│     时间 = O(N) × 每次迭代开销                                            │
│                                                                          │
│  5. 下游传播效应                                                          │
│     └─ 更多 blocks 进入 output_queue                                      │
│     └─ 更多 dispatch_next_task() 调用                                     │
│     └─ 更多 update_usages() 调用                                          │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### 5.4 量化分析

```python
# 假设：
# - 10 个并发 tasks
# - 每个 task 处理 128MB 数据

# 配置 A: block_size = 128MB
blocks_per_task = 1
total_blocks = 10 * 1 = 10
prepare_metadata_calls = 10
worst_case_time = 10 * 100ms = 1s (极端情况)
typical_time = 10 * ~1ms = 10ms (大部分已 ready)

# 配置 B: block_size = 1MB
blocks_per_task = 128
total_blocks = 10 * 128 = 1280
prepare_metadata_calls = 1280
worst_case_time = 1280 * 100ms = 128s (极端情况)
typical_time = 1280 * ~1ms = 1.28s (即使大部分已 ready)
```

---

## 6. 优化方案决策

### 6.1 方案对比

案 | 核心改动 | 复杂度 | 效果 | 风险 |
|------|---------|--------|------|------|
| 方案 1 | 减少 timeout 到 10ms | 低 | 有限 | 低 | 分离 peek 和批量 wait | 中 | 好 | 中 |
| **方案 3** | **完全非阻塞 (timeout=0)** | **低** | **显著** | **低** |

### 6.2 推荐方案：方案 3

**核心改动**：

```python
# physical_operator.py
# 改动前
METADATA_WAIT_TIMEOUT_S = 0.1  # 100ms

# 改动后
METADATA_WAIT_TIMEOUT_S = 0.0  # 完全非阻塞
```

**优点**：
- 改动极小（只改一个常量）
- **消除串行等待问题**：不管有多少 tasks，`prepare_metada)` 都立即返回
- 利用现有的重试机制：如果 meta_ref 没 ready，下一次 loop 会重试
- 与现有的 batch `ray.wait(meta_refs)` 机制配合良好

**风险评估**：

```
场景分析n1. 正常情况 (meta_ref 已经 ready):
   - 改动前: _next_sync(0.1) 立即返回
   - 改动后: _next_sync(0) 立即返回
   - 无差异 ✓

2. met 稍微延迟 (< 100ms):
   - 改动前: 等待直到 ready，一次完成
   - 改动后: 返回 nil，下一次 loop 处理
   - 影响: 增加 1 次 loop 迭代
   - 但: loop 频率约影响不大 ✓

3. meta_ref 严重延迟 (> 100ms):
   - 改动前: 等待 100ms 超时，下次 loop 重试
   - 改动后: 立即返回，下次 loop 重试
   - 无差异，甚至更好 ✓
```

**效果预期**
| 场景 | 改动前 | 改动后 |
|------|--------|--------|
| 100 个 tasks，meta 全部 ready | ~100ms (串行等待开销) | ~1ms |
| 100 个 tasks，50% meta 需s (50×100ms) | ~1ms + 下次 loop 处理 |
| Block size 小，1000 个 blocks | 可能 >100s | 几十 ms |

---

## 7. 诊断工具使用指南

### 7.1 启用`bash
export RAY_DATA_ENABLE_SCHED_LOOP_DIAGNOSTICS=1
```

### 7.2 诊断输出格式

当 scheduling loop 耗时超过 100ms 时，会自动输出诊断日志：

```
SchedulingLoopDiagnostics:
  ray_wait_active=5.2ms,           # ray.wait(active_tasks) 耗时
  prta=150.3ms             # 所有 prepare_metadata() 总耗时
    (calls=50,                     # 调用次数
     waited=5,              实际等待 >1ms 的次数
     max=45.2ms),                  # 单次最大耗时
  ray_wait_meta=8.1ms,             # ray.wait(meta_refs) 耗时
  process_meta=12.5ms,             # 处理 metadata 耗时
  pull_outputs=2.1ms,              # pull outputs 耗时
  active_tasks=100,                # active tasks 数量
  ready_tasks=50,                  # ready tasks 数量
  pending_meta=50,                 # pending metadata 数量
  meta_ready=48,                   # metadata ready 数量
  blocks_processed=48,             # 处理的 blocks 数量
  total=178.2ms                    # 总耗时
```

### 7.3 诊断指标解读

| 指标 | 含义 | 异常表现 |
|------|------|---------|
| `ray_wait_active` | 等待 active tasks 的时间 | >100ms 说明没有任务完成 |
| `prepare_meta` | prepare_metadata 总耗时 | >100ms 说明有串行等待 |
| `calls` | prepare_metadata 调用次数 | 数量大说明 blocks 多 |
| `waited` | 实际等待的次数 | >0 说明有 meta 延迟 |
| `max` | 单次最大耗时 | >10ms 说明有严重延迟 |
| `ray_wait_meta` | 批量等待 meta_refs 时间 | 接近 100ms 说明 meta 拉取慢 |
| `meta_ready` | ready 的 meta 数量 | 远小于 pending_meta 说明拉取慢 |

### 7.4 问题定位示例

**场景 1: `prepare_meta` 时间长**
```
prepare_meta=500ms (calls=100, waited=80, max=95ms)
```
**原因**：大量 meta_ref 需要等待 worker yield
**解决**：
- 检查 worker 端处理速度
- 检查网络延迟
- 考虑增大 block size 减少 block 数量

**场景 2: `ray_wait_meta` 时间长**
```
ray_wait_meta=95ms, meta_ready=10/100
```
**原因**：metadata 对象需要从远程节点拉取
**解决**：
- 检查 Object Store 压力
- 检查网络带宽

**场景 3: `calls` 数量很大**
```
prepare_meta=200ms (calls=1000, waited=5, max=2ms)
```
**原因**：Block size 太小，导致大量 blocks
**解决**：增大 `target_max_block_size`

### 7.5 编程方式获取诊断数据

```python
from ray.data._internal.execution.streaming_executor_state import get_recent_diagnostics

# 获取最近 100 次 scheduling loop 的诊断数据
diagnostics = get_recent_diagnostics()

# 分析
for diag in diagnostics:
    if diag.total_time_s() > 0.1:
        print(diag.to_log_string())

# 统计平均值
avg_prepare_time = sum(d.prepare_metadata_total_s for d in diagnostics) / len(diagnostics)
avg_calls = sum(d.num_prepare_metadata_calls for d in diagnostics) / len(diagnostics)
print(f"Average prepare_metadata time: {avg_prepare_time*1000:.1f}ms")
print(f"Average calls per loop: {avg_calls:.1f}")
```

---

## 8. 关键代码位置

| 文件路径 | 说明 |
|---------|------|
| `python/ray/data/_internal/execution/streaming_executor.py` | 主执行器，schedule loop 核心 |
| `python/ray/data/_internal/execution/streaming_executor_state.py` | process_completed_tasks, 诊断工具 |
| `python/ray/data/_internal/execution/interfaces/physical_operator.py` | DataOpTask, prepare_metadata, METADATA_WAIT_TIMEOUT_S |
| `python/ray/_private/object_ref_generator.py` | ObjectRefGenerator, _next_sync |
| `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py` | ActorPool, get_task_distribution |
| `python/ray/data/_internal/execution/operators/task_pool_map_operator.py` | TaskPool, get_task_distribution |

---

## 附录

### A. 完整修改列表

| 文件 | 修改内容 | 目的 |
|------|---------|------|
| `physical_operator.py` | `METADATA_WAIT_TIMEOUT_S = 0.0` | 消除串行等待 |
| `streaming_executor_state.py` | 添加 `SchedulingLoopDiagnostics` | 诊断工具 |
| `streaming_executor_state.py` | 添加计时代码 | 收集性能数据 |
| `streaming_executor.py` | 适配返回值变化 | 兼容新接口 |
| `actor_pool_map_operator.py` | `get_task_distribution()` | 显示 running/queued |
| `task_pool_map_operator.py` | `get_task_distribution()` | 显示 running/queued |

### B. 配置调优建议

| 场景 | 配置 | 建议值 |
|------|------|-------|
| 减少 blocks 数量 | `target_max_block_size` | 128MB-256MB |
| 减少 prefetch 任务 | `max_tasks_in_flight_per_actor` | 2-4 |
| 加快 GCS 同步 | `RAY_task_events_report_interval_ms` | 500 |
| 增加 Driver 资源 | Driver CPU | 4+ CPU |

### C. 验证方法

运行前后对比 `data_sched_loop_duration_s` 指标：
- 预期改进：小 block size 场景下，scheduling loop 时间大幅降低
- 预期影响：可能略微增加 loop 迭代次数，但总体吞吐应该提升

```bash
# 监控 Prometheus 指标
curl localhost:8080/metrics | grep data_sched_loop_duration_s
```
