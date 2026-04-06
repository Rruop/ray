# Data Overview Metrics 设计文档

## 概述

Ray Data Dashboard 的 Data Overview 页面展示每个 Dataset 及其 Operator 的运行指标。本文档详细说明 Rows/Blocks Input 与 Rows/Blocks Outputted 指标的来源、语义、以及如何利用这些指标检测反压。

---

## 数据流全景

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                        StreamingExecutor 主循环                                       │
│                                                                                     │
│  Task (remote worker) 产出数据 (streaming generator yield block + metadata)           │
│       ↓                                                                             │
│       ↓  ← ⚠️ 反压点 1: on_data_ready(max_bytes_to_read)                             │
│       ↓     max_task_output_bytes_to_read == 0 时不读取，数据留在 worker 侧            │
│       ↓                                                                             │
│  _output_ready_callback(output: RefBundle)                                          │
│       ↓  [触发 on_task_output_generated → rows_task_outputs_generated 递增]           │
│       ↓  [output 加入 op._output_queue]                                              │
│       ↓                                                                             │
│  op._output_queue (operator 内部 output queue, 支持按 task_index 排序)                │
│       ↓                                                                             │
│  op.has_next() == True                                                              │
│       ↓                                                                             │
│  op.get_next()  [触发 on_output_taken → row_outputs_taken 递增]                       │
│       ↓                                                                             │
│  OpState[i].add_output(ref) → OpState[i].output_queue.append(ref)                   │
│       ↓  (外部 outqueue, thread-safe deque)                                          │
│       ↓                                                                             │
│       ↓  ← ⚠️ 反压点 2: get_eligible_operators() 中 can_add_input() 返回 False        │
│       ↓     数据堆积在外部 outqueue，dispatch_next_task() 不被调用                     │
│       ↓                                                                             │
│  dispatch_next_task()                                                               │
│       ↓  [从 OpState[i].input_queues pop → op[i+1].add_input(ref, input_index)]     │
│       ↓  [触发 on_input_received → num_row_inputs_received 递增]                      │
│       ↓                                                                             │
│  Op[i+1] 内部: _add_input_inner() → bundler 聚合 → _try_schedule_task()             │
│       ↓  [触发 on_task_submitted → rows_inputs_of_submitted_tasks 递增]               │
│       ↓                                                                             │
│  ray remote task 在 worker 执行                                                      │
│       ↓                                                                             │
│  task 完成 → on_task_finished()                                                      │
│       ↓  [触发 num_task_inputs_processed 递增]                                        │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 指标定义

### Dashboard 展示的指标

| Dashboard 列 | 后端字段 | 底层 metrics 属性 | 触发位置 | 语义 |
|---|---|---|---|---|
| **Rows Input** | `input_rows` | `num_row_inputs_received` | `physical_operator.py:600` — `add_input()` | operator 从上游接收的行数（通过 dispatch_next_task 触发） |
| **Blocks Input** | `input_blocks` | `num_inputs_received` | `physical_operator.py:600` — `add_input()` | operator 从上游接收的 block 数 |
| **Rows Outputted** | `output_rows` | `rows_task_outputs_generated` | `op_runtime_metrics.py:890` — `on_task_output_generated()` | operator 的 task 产出并从 streaming generator 读回 driver 的行数 |
| **Blocks Outputted** | (progress bar) | `num_task_outputs_generated` | `op_runtime_metrics.py:888` — `on_task_output_generated()` | operator 的 task 产出的 block 数 |

### 后端相关但未直接展示的指标

| metrics 属性 | 触发位置 | 语义 | 与展示指标的关系 |
|---|---|---|---|
| `row_outputs_taken` | `physical_operator.py:638` — `get_next()` | 从内部 queue 取出放入外部 outqueue 的行数 | ≈ `rows_task_outputs_generated`，差值为内部 queue 瞬时缓冲 |
| `block_outputs_taken` | `physical_operator.py:638` — `get_next()` | 从内部 queue 取出放入外部 outqueue 的 block 数 | ≈ `num_task_outputs_generated` |
| `rows_inputs_of_submitted_tasks` | `op_runtime_metrics.py:869` — `on_task_submitted()` | 提交给 task 的输入行数 | ≤ `num_row_inputs_received`，差值为待 bundling 的输入 |
| `num_task_inputs_processed` | `op_runtime_metrics.py:974` — task 完成时 | task 已处理完的输入 block 数 | ≤ `num_inputs_received`，差值为正在运行中的 task 持有的输入 |
| `bytes_task_outputs_generated` | `op_runtime_metrics.py:889` | task 产出的总字节数 | 用于计算平均 block 大小 |
| `bytes_inputs_received` | `op_runtime_metrics.py:797` | 接收的输入总字节数 | 用于 resource budget 计算 |

---

## 调度循环详解

### _scheduling_loop_step() 核心流程

```python
# streaming_executor.py:600-665 (简化版)
def _scheduling_loop_step(self, topology):
    # Phase 1: 处理已完成的 task
    self._resource_manager.update_usages()
    errored_blocks_per_op, _ = process_completed_tasks(
        topology, self._backpressure_policies, self._max_errored_blocks
    )

    # Phase 2: 循环 dispatch 直到没有 eligible operator
    self._resource_manager.update_usages()
    while True:
        op = select_operator_to_run(
            topology, self._resource_manager, self._backpressure_policies,
            ensure_liveness=self._consumer_idling(),
            ranker=self._ranker,
        )
        if op is None:
            break
        topology[op].dispatch_next_task()  # ← 触发 add_input → on_input_received
        self._resource_manager.update_usages()

    # Phase 3: 自动伸缩 & 状态更新
    self._cluster_autoscaler.try_trigger_scaling()
    self._actor_autoscaler.try_trigger_scaling()
    update_operator_states(topology)
    return not all(op.has_completed() for op in topology)
```

### process_completed_tasks() 内部流程

`process_completed_tasks()` (streaming_executor_state.py) 负责从完成的 ray task 中读取数据：

```python
# 简化的核心流程 (streaming_executor_state.py:555-700)
def process_completed_tasks(topology, backpressure_policies, max_errored_blocks):
    # 1. 收集所有 active tasks 的 waitable refs
    active_tasks = {}
    for op, state in topology.items():
        for task in op.get_active_tasks():
            active_tasks[task.get_waitable()] = (state, task)

    # 2. 计算每个 op 的 output 反压限制
    max_bytes_to_read_per_op = {}
    for op, state in topology.items():
        for policy in backpressure_policies:
            policy_limit = policy.max_task_output_bytes_to_read(op)
            # 取所有策略的最小值
            ...

    # 3. ray.wait() 获取就绪的 task refs
    ready, _ = ray.wait(list(active_tasks.keys()), num_returns=len(active_tasks),
                        fetch_local=False, timeout=0.1)

    # 4. 对每个就绪的 DataOpTask 调用 on_data_ready
    for task in ready_tasks:
        if isinstance(task, DataOpTask):
            bytes_read = task.on_data_ready(
                max_bytes_to_read_per_op.get(state, None)  # ← 反压限制
            )
            max_bytes_to_read_per_op[state] -= bytes_read

    # 5. 拉取 operator 产出到外部 outqueue
    for op, op_state in topology.items():
        while op.has_next():
            op_state.add_output(op.get_next())  # ← 触发 on_output_taken
```

### dispatch_next_task() 逻辑

```python
# streaming_executor_state.py:296-311
def dispatch_next_task(self) -> None:
    """Move a bundle from the operator inqueue to the operator itself."""
    for i, inqueue in enumerate(self.input_queues):
        ref = inqueue.pop()
        if ref is not None:
            self.op.add_input(ref, input_index=i)  # ← 触发 on_input_received
            # 更新外部 queue 的内存计数
            self.op.metrics.num_external_inqueue_bytes -= ref.size_bytes()
            self.op.metrics.num_external_inqueue_blocks -= len(ref.blocks)
            input_op = self.op.input_dependencies[i]
            input_op.metrics.num_external_outqueue_blocks -= len(ref.blocks)
            input_op.metrics.num_external_outqueue_bytes -= ref.size_bytes()
            return
```

### add_output() 逻辑

```python
# streaming_executor_state.py:269-294
def add_output(self, ref: RefBundle) -> None:
    """Move a bundle produced by the operator to its outqueue."""
    ref, diverged = dedupe_schemas_with_validation(self._schema, ref, ...)
    self._schema = ref.schema
    self.output_queue.append(ref)
    self.num_completed_tasks += 1
    # 更新下游 operator 的外部 inqueue 计数
    for next_op in self.op.output_dependencies:
        next_op.metrics.num_external_inqueue_blocks += len(ref.blocks)
        next_op.metrics.num_external_inqueue_bytes += ref.size_bytes()
    self.op.metrics.num_external_outqueue_blocks += len(ref.blocks)
    self.op.metrics.num_external_outqueue_bytes += ref.size_bytes()
```

---

## on_data_ready 详解

`DataOpTask.on_data_ready()` 是从 remote task 的 streaming generator 读取产出数据的核心方法：

```python
# physical_operator.py:150-243
def on_data_ready(self, max_bytes_to_read: Optional[int]) -> int:
    """从 streaming generator 读取数据。

    Args:
        max_bytes_to_read: 最大读取字节数。None 表示无限制，0 表示不读取（反压）。
    Returns:
        实际读取的字节数。
    """
    bytes_read = 0
    while max_bytes_to_read is None or bytes_read < max_bytes_to_read:
        # Step 1: 尝试从 streaming generator 获取 block ref
        if self._pending_block_ref.is_nil():
            try:
                self._pending_block_ref = self._streaming_gen._next_sync(timeout_s=0)
            except StopIteration:
                self._task_done_callback(None)  # task 正常结束
                self._has_finished = True
                break

            if self._pending_block_ref.is_nil():
                break  # generator 暂时没有新输出

            self._block_ready_callback(self._pending_block_ref)

        # Step 2: 获取对应的 metadata ref
        if self._pending_meta_ref.is_nil():
            try:
                self._pending_meta_ref = self._streaming_gen._next_sync(
                    timeout_s=METADATA_WAIT_TIMEOUT_S
                )
            except StopIteration:
                # block 有但 metadata 没有 → task 异常
                ray.get(self._pending_block_ref)  # 会抛出异常
                ...

            if self._pending_meta_ref.is_nil():
                break  # metadata 还没就绪

            self._metadata_ready_callback(self._pending_meta_ref)

        # Step 3: ray.get() 获取 metadata 对象
        try:
            meta_with_schema = ray.get(self._pending_meta_ref,
                                       timeout=METADATA_GET_TIMEOUT_S)
        except ray.exceptions.GetTimeoutError:
            break  # metadata 对象还未到达本节点

        # Step 4: 构造 RefBundle 并回调
        self._output_ready_callback(
            RefBundle([(self._pending_block_ref, meta)], owns_blocks=True, ...)
        )
        self._pending_block_ref = ray.ObjectRef.nil()
        self._pending_meta_ref = ray.ObjectRef.nil()
        bytes_read += meta.size_bytes

    return bytes_read
```

**关键点**:
- 每次循环读取一个 block+metadata 对
- `max_bytes_to_read=0` 时循环条件 `bytes_read < 0` 永远为 False，直接跳过
- `_output_ready_callback` 最终触发 `on_task_output_generated` 并将数据加入 operator 内部 output queue

---

## MapOperator 内部数据流

MapOperator 是最常用的 operator 类型，其内部处理链路：

```python
# map_operator.py (简化)

# 1. 接收输入
def _add_input_inner(self, refs, input_index):
    self._block_ref_bundler.add_bundle(refs)   # 聚合小 block
    if self._block_ref_bundler.needs_finalize():
        bundled_input = self._block_ref_bundler.finalize()
        self._try_schedule_task(bundled_input)  # 提交 ray remote task

# 2. 提交 task
def _try_schedule_task(self, bundled_input):
    gen = self._ray_remote_fn.remote(bundled_input)  # → worker 执行
    self._submit_data_task(gen, bundled_input)

# 3. task 产出回调 (由 on_data_ready 触发)
def _output_ready_callback(task_index, output: RefBundle):
    assert len(output) == 1  # streaming 模式每次产出一个 block
    self._metrics.on_task_output_generated(task_index, output)
    self._output_queue.add(output, key=task_index)  # 按 task_index 排序 (preserve_order)
    self._metrics.on_output_queued(output)

# 4. executor 拉取产出
def has_next(self) -> bool:
    return self._output_queue.has_next()

def _get_next_inner(self) -> RefBundle:
    bundle = self._output_queue.get_next()
    self._metrics.on_output_dequeued(bundle)
    return bundle
```

---

## 反压机制详解

### DownstreamCapacityBackpressurePolicy

基于下游处理能力的反压策略，核心思想：如果上游产出速度远超下游消费能力，限制上游。

```python
# downstream_capacity_backpressure_policy.py

class DownstreamCapacityBackpressurePolicy(BackpressurePolicy):
    # 阈值：per-Op object store budget 利用率超过 90% 时启用反压判断
    OBJECT_STORE_BUDGET_UTIL_THRESHOLD = 0.9

    def _get_queue_size_bytes(self, op):
        """获取 operator 的 output queue 大小（含下游不可调度 op 的内存占用）"""
        op_outputs_usage = self._topology[op].output_queue_bytes()
        op_outputs_usage += sum(
            self._resource_manager.get_op_usage(next_op).object_store_memory
            for next_op in self._resource_manager._get_downstream_ineligible_ops(op)
        )
        return op_outputs_usage

    def _get_downstream_capacity_size_bytes(self, op):
        """获取下游 operator 的处理能力（pending task inputs 字节数）"""
        total = 0
        for output_dep in op.output_dependencies:
            if self._resource_manager.is_op_eligible(output_dep):
                total += output_dep.metrics.obj_store_mem_pending_task_inputs or 0
            else:
                total += self._get_downstream_capacity_size_bytes(output_dep)
        return total

    def _should_apply_backpressure(self, op):
        """判断是否需要反压"""
        # 1. budget 利用率低于阈值 → 不反压
        utilized_fraction = get_utilized_object_store_budget_fraction(...)
        if utilized_fraction <= self.OBJECT_STORE_BUDGET_UTIL_THRESHOLD:
            return False
        # 2. queue_size / downstream_capacity > ratio → 反压
        queue_ratio = self._get_queue_size_bytes(op) / self._get_downstream_capacity_size_bytes(op)
        return queue_ratio > self._backpressure_capacity_ratio

    def can_add_input(self, op):
        """Input 反压: 是否允许 dispatch 给此 operator"""
        return not self._should_apply_backpressure(op)

    def max_task_output_bytes_to_read(self, op):
        """Output 反压: 是否允许读取 task 产出"""
        if self._should_apply_backpressure(op):
            return 0  # 不读取任何数据
        return None   # 无限制
```

### ResourceBudgetBackpressurePolicy

基于资源预算的反压策略：

```python
# resource_budget_backpressure_policy.py

class ResourceBudgetBackpressurePolicy(BackpressurePolicy):
    def can_add_input(self, op):
        """是否有资源预算提交新 task"""
        return self._resource_manager._op_resource_allocator.can_submit_new_task(op)

    def max_task_output_bytes_to_read(self, op):
        """基于 object store memory budget 限制读取"""
        return self._resource_manager.max_task_output_bytes_to_read(op)

# 具体计算逻辑 (resource_manager.py:902-922)
def max_task_output_bytes_to_read(self, op):
    if op not in self._op_budgets:
        return None
    # 可读字节 = 剩余 budget + reserved_for_outputs 中未使用的部分
    res = self._op_budgets[op].object_store_memory
    op_outputs_usage = self._resource_manager.get_mem_op_outputs(op, ...)
    res += max(self._reserved_for_op_outputs[op] - op_outputs_usage, 0)
    if math.isinf(res):
        return None
    res = int(res)
    # 特殊情况: budget 为 0 但需要保证 liveness 时允许读 1 byte
    if res == 0 and self._should_unblock_streaming_output_backpressure(op):
        res = 1
    return res
```

---

## select_operator_to_run() 调度逻辑

选择下一个 dispatch 的 operator：

```python
# streaming_executor_state.py:904-939

def select_operator_to_run(topology, resource_manager, backpressure_policies,
                           ensure_liveness, ranker):
    """选择下一个执行 dispatch 的 operator。

    目标: 最大化整体 pipeline 吞吐量，满足内存/并发等约束。
    """
    # Step 1: 收集所有 eligible operators
    eligible_ops = get_eligible_operators(topology, backpressure_policies,
                                          ensure_liveness=ensure_liveness)
    if not eligible_ops:
        return None

    # Step 2: 对 eligible ops 排名，选最优
    ranks = ranker.rank_operators(eligible_ops, topology, resource_manager)
    next_op, _ = min(zip(eligible_ops, ranks), key=lambda t: t[1])
    return next_op
```

### get_eligible_operators() 判定条件

一个 operator 被认为 eligible 需要同时满足：

```python
# streaming_executor_state.py:561-901 (简化)

def get_eligible_operators(topology, backpressure_policies, ensure_liveness):
    eligible_ops = []
    for op, state in topology.items():
        # 条件 1: 反压检查
        triggered_policy = None
        for p in backpressure_policies:
            if not p.can_add_input(op):
                triggered_policy = p.name
                break
        in_backpressure = triggered_policy is not None

        # 条件 2: operator 未完成
        is_completed = op.has_completed()

        # 条件 3: operator 能接受输入 (并发未满)
        can_add = op.can_add_input()  # 检查 active_tasks < max_concurrency

        # 条件 4: input queue 有待处理的 bundle
        has_bundles = state.has_pending_bundles()

        # 全部满足且无反压 → eligible
        if not is_completed and can_add and has_bundles and not in_backpressure:
            eligible_ops.append(op)

    # ensure_liveness: 如果没有 eligible op 且有 dispatchable op，强制选一个
    # 防止 pipeline 死锁 (consumer 在等数据但所有 op 都被反压)
    ...
    return eligible_ops
```

---

## 反压诊断

### 利用 Input/Output 指标判断反压

| 观察现象 | 诊断 | 原因 | 确认方法 |
|---------|------|------|---------|
| `Op[i].Rows Outputted` 增长，`Op[i+1].Rows Input` 停滞 | **Input 反压** | `can_add_input=False`，dispatch 被阻止 | 观察 `queued_blocks` 是否持续增长 |
| `Op[i].Rows Outputted` 停滞，但 task 仍在运行 | **Output 反压** | `max_task_output_bytes_to_read=0`，数据卡在 worker | 观察 Memory Usage 是否接近上限 |
| `Rows Input` 增长但 `Rows Outputted` 不增长 | **operator 处理慢** | task 执行效率低或并发不足 | 检查 task_pool active_tasks vs max_concurrency |
| `Rows Input ≈ Rows Outputted` 同步增长 | **正常** | 无反压 | — |
| 所有 operator 都停滞 | **全局内存压力** | Object store memory 耗尽 | 检查 Memory Usage (current/max) |

### Input/Output 比值分析

| Input vs Output 关系 | 含义 | 典型场景 |
|---|---|---|
| `Input >> Output` | 数据在缩小 | filter(), aggregate(), reduce() |
| `Output >> Input` | 数据在放大 | flat_map(), explode() |
| `Output ≈ Input` | 1:1 映射 | map() (行数不变的转换) |
| `Input 增长但 Output 停滞` | 处理瓶颈 | CPU 密集型 UDF, GPU 计算 |

### 注意事项

1. **`rows_task_outputs_generated` 不等于 task 在 worker 上实际产出的行数**。它是"已从 streaming generator 读回 driver 的行数"，受 output 反压影响。真正的 "worker 侧产出量" 在 driver 侧无法直接观测。

2. **`rows_task_outputs_generated` ≈ `row_outputs_taken`**，两者差值仅为 operator 内部 output queue 的瞬时缓冲量（通常很小）。选用前者是因为它语义上更接近"产出"。

3. **`num_row_inputs_received` ≥ `rows_inputs_of_submitted_tasks`**，差值为 bundler 中还在聚合等待的输入行数（尚未达到 target_max_block_size 触发 task 提交）。

4. **配合 `queued_blocks` 辅助判断**：`queued_blocks`（`OpState.total_enqueued_input_blocks()`）反映 operator 外部 + 内部 input queue 的总 block 数。如果这个值持续增大说明上游在堆积数据。

---

## 架构图: Operator 内部队列结构

```
                    ┌───────────────────────────────────────────────────┐
                    │                    Operator                        │
                    │                                                   │
 外部 inqueue       │   ┌───────────┐  task提交  ┌────────────────┐    │  外部 outqueue
 (OpState.         │   │ internal  │ ────────→ │ running tasks  │    │  (OpState.
  input_queues)────┼──→│ inqueue / │           │ (remote)       │    │   output_queue)
                    │   │ bundler   │           │                │    │
 dispatch_next_    │   └───────────┘           └───────┬────────┘    │  op.get_next()
 task() 触发       │                                    │             │  触发
 on_input_received │   add_input()                     │ streaming   │  on_output_taken
                    │   进入 operator                    │ yield       │
                    │                                    ↓             │
                    │                           ┌──────────────┐      │
                    │                           │ internal     │      │
                    │                           │ output_queue │──────┼──→
                    │                           │ (ordered)    │      │
                    │                           └──────────────┘      │
                    │                                                   │
                    │   on_task_output_generated  _output_ready_        │
                    │   触发位置                   callback() 产出       │
                    └───────────────────────────────────────────────────┘
```

### 队列层次说明

| 队列 | 数据结构 | 位置 | 作用 | 相关 metrics |
|------|---------|------|------|-------------|
| **外部 inqueue** | `OpBufferQueue` (deque) | `OpState.input_queues` | 上游 output 等待被 dispatch 给本 operator | `num_external_inqueue_blocks/bytes` |
| **内部 inqueue / bundler** | `BlockRefBundler` | Operator 内部 | `add_input()` 后聚合小 block 直到达到 target size | `obj_store_mem_internal_inqueue_blocks` |
| **内部 output queue** | `OutputQueue` (按 task_index 排序) | `op._output_queue` | task streaming 产出、`get_next()` 取走前的有序缓冲 | `obj_store_mem_internal_outqueue_blocks` |
| **外部 outqueue** | `OpBufferQueue` (deque, thread-safe) | `OpState.output_queue` | `get_next()` 取出后、等待下游 dispatch 的缓冲 | `num_external_outqueue_blocks/bytes` |

### 队列连接关系

```
Op[i].外部 outqueue  ←→  Op[i+1].外部 inqueue
       (同一个 OpBufferQueue 对象的引用)
```

`OpState` 初始化时：
```python
# streaming_executor_state.py:270
self.input_queues: List[OpBufferQueue] = inqueues
# inqueues 来自上游 OpState 的 output_queue
```

---

## Dashboard 数据组装

### _get_state_dict() 方法

每个调度步完成后，executor 组装状态数据发送给 dashboard：

```python
# streaming_executor.py:_get_state_dict() (简化)
def _get_state_dict(self, state):
    last_op, last_state = list(self._topology.items())[-1]

    # 每个 operator 的指标
    operators_dict = {}
    for i, (op, op_state) in enumerate(self._topology.items()):
        op_output_rows = op.metrics.rows_task_outputs_generated
        op_info = {
            "name": op.name,
            "progress": op_state.num_completed_tasks,
            "total": op.num_outputs_total(),
            "total_rows": op.num_output_rows_total(),
            "queued_blocks": op_state.total_enqueued_input_blocks(),
            "num_errored_blocks": op.metrics.num_errored_blocks,
            "output_rows": op_output_rows,
            "input_rows": op.metrics.num_row_inputs_received,
            "input_blocks": op.metrics.num_inputs_received,
            "state": ...,
        }
        # TaskPool/ActorPool 附加信息
        if isinstance(op, TaskPoolMapOperator):
            op_info["task_pool"] = {
                "active_tasks": op.num_active_tasks(),
                "max_concurrency": op.get_max_concurrency_limit(),
            }
        elif isinstance(op, ActorPoolMapOperator):
            op_info["actor_pool"] = { ... }

        operators_dict[op_id] = op_info

    # Dataset 级别的聚合指标 (取最后一个 operator 的数据)
    return {
        "state": state,
        "progress": last_state.num_completed_tasks,
        "total": last_op.num_outputs_total(),
        "total_rows": last_op.num_output_rows_total(),
        "output_rows": last_op.metrics.rows_task_outputs_generated,
        "input_rows": last_op.metrics.num_row_inputs_received,
        "input_blocks": last_op.metrics.num_inputs_received,
        "num_errored_blocks": self._num_errored_blocks,
        "operators": operators_dict,
    }
```

### _StatsActor 初始化

Dashboard 通过 `_StatsActor` 获取数据，初始注册时的默认值：

```python
# stats.py:601-626
self.datasets[dataset_tag] = {
    "state": DatasetState.PENDING.name,
    "progress": 0,
    "total": 0,
    "total_rows": 0,
    "output_rows": 0,
    "input_rows": 0,
    "input_blocks": 0,
    "start_time": start_time,
    "end_time": None,
    "num_errored_blocks": 0,
    "operators": {
        operator: {
            "state": DatasetState.PENDING.name,
            "progress": 0,
            "total": 0,
            "queued_blocks": 0,
            "num_errored_blocks": 0,
            "output_rows": 0,
            "input_rows": 0,
            "input_blocks": 0,
        }
        for operator in operator_tags
    },
}
```

---

## 前端数据结构

### TypeScript 类型定义

```typescript
// type/data.ts
export type DataMetrics = {
  state: string;
  ray_data_current_bytes: { value: number; max: number };
  ray_data_output_rows: { max: number };
  ray_data_spilled_bytes: { max: number };
  ray_data_cpu_usage_cores: { value: number; max: number };
  ray_data_gpu_usage_cores: { value: number; max: number };
  num_errored_blocks: number;
  input_rows: number;     // ← 新增: num_row_inputs_received
  input_blocks: number;   // ← 新增: num_inputs_received
  output_rows: number;    // ← 改为: rows_task_outputs_generated
  progress: number;
  total: number;
};
```

### 列渲染顺序

```
Dataset/Operator Name | Blocks Outputted (progress) | State | Errored Blocks
  | Rows Input | Blocks Input | Rows Outputted | Memory | Spilled | CPU | GPU | Start | End
```

---

## 代码位置索引

| 模块 | 文件 | 关键方法/行 |
|------|------|-----------|
| 指标字段定义 (Input) | `execution/interfaces/op_runtime_metrics.py:239-277` | `num_inputs_received`, `num_row_inputs_received`, `bytes_inputs_received`, `num_task_inputs_processed`, `rows_inputs_of_submitted_tasks` |
| 指标字段定义 (Output) | `execution/interfaces/op_runtime_metrics.py:279-318` | `num_task_outputs_generated`, `rows_task_outputs_generated`, `row_outputs_taken`, `block_outputs_taken` |
| 回调: on_input_received | `execution/interfaces/op_runtime_metrics.py:793-797` | 递增 `num_inputs_received`, `num_row_inputs_received`, `bytes_inputs_received` |
| 回调: on_output_taken | `execution/interfaces/op_runtime_metrics.py:852-857` | 递增 `num_outputs_taken`, `block_outputs_taken`, `row_outputs_taken` |
| 回调: on_task_output_generated | `execution/interfaces/op_runtime_metrics.py:882-903` | 递增 `num_task_outputs_generated`, `rows_task_outputs_generated`, `bytes_task_outputs_generated` |
| 回调: on_task_submitted | `execution/interfaces/op_runtime_metrics.py:859-880` | 递增 `num_tasks_submitted`, `rows_inputs_of_submitted_tasks` |
| 回调: on_task_finished | `execution/interfaces/op_runtime_metrics.py:939-983` | 递增 `num_task_inputs_processed`, `bytes_task_inputs_processed` |
| Operator add_input | `execution/interfaces/physical_operator.py:581-601` | 调用 `on_input_received` + `_add_input_inner` |
| Operator get_next | `execution/interfaces/physical_operator.py:630-639` | 调用 `_get_next_inner` + `on_output_taken` |
| DataOpTask on_data_ready | `execution/interfaces/physical_operator.py:150-243` | 从 streaming generator 读取 block+metadata |
| MapOperator _output_ready_callback | `execution/operators/map_operator.py:598-606` | `on_task_output_generated` + queue.add |
| MapOperator _submit_data_task | `execution/operators/map_operator.py:585-636` | 提交 ray remote task |
| 调度主循环 | `execution/streaming_executor.py:600-665` | _scheduling_loop_step |
| process_completed_tasks | `execution/streaming_executor_state.py:540-700+` | 批量处理就绪 task, 拉取 output |
| dispatch_next_task | `execution/streaming_executor_state.py:296-311` | inqueue pop → add_input |
| add_output | `execution/streaming_executor_state.py:269-294` | get_next 结果放入外部 outqueue |
| select_operator_to_run | `execution/streaming_executor_state.py:904-939` | eligible ops 选择 + ranking |
| get_eligible_operators | `execution/streaming_executor_state.py:561-901` | 反压判定 + 条件检查 |
| DownstreamCapacity 策略 | `execution/backpressure_policy/downstream_capacity_backpressure_policy.py` | queue/capacity 比值反压 |
| ResourceBudget 策略 | `execution/backpressure_policy/resource_budget_backpressure_policy.py` | 资源预算反压 |
| Dashboard 数据组装 | `execution/streaming_executor.py:_get_state_dict()` | 组装 operator metrics dict |
| Stats 初始化 | `data/_internal/stats.py:601-626` | _StatsActor 注册 dataset 默认值 |
| 前端列定义 | `dashboard/client/src/components/DataOverviewTable.tsx:27-84` | columns 配置 |
| 前端类型定义 | `dashboard/client/src/type/data.ts` | DataMetrics 类型 |
| 前端测试 | `dashboard/client/src/pages/data/DataOverview.component.test.tsx` | mock 数据 |

---

## 变更历史

- **T11609366**: 将 "Blocks Generated" / "Rows Generated" 替换为 "Rows Input" / "Blocks Input"，并将 `output_rows` 从 `row_outputs_taken` 改为 `rows_task_outputs_generated`，使指标更准确反映 operator 的输入消费和输出产出
