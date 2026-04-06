# Ray Data 反压机制深度分析

基于 Ray 2.52.1 源码分析，针对 KML 平台 Ray Dashboard 任务 `07000000` 的反压问题排查。

## 1. 问题背景

### 1.1 作业基本信息

| 项目 | 值 |
|------|-----|
| Job ID | `07000000` |
| Pipeline | `e_commerce_pair_pipeline.py` |
| 状态 | RUNNING |
| 运行时长 | 2h+ |

### 1.2 集群资源状况

| 资源 | 使用量 | 总量 | 使用率 |
|------|--------|------|--------|
| CPU | 2404.0 | 3000.0 | 80.1% |
| GPU | 200.0 | 200.0 | 100% |
| Memory | 6.00 GiB | 24.32 TiB | 0.02% |
| Object Store | 28.27 GiB | 7.29 TiB | 0.4% |

### 1.3 问题现象

从日志中观察到的关键状态：

```
Stage                              | 进度           | Tasks | Queued Blocks    | Object Store
-----------------------------------|----------------|-------|------------------|-------------
FrameExtract→VlmPreprocess         | 40479/1732098  | 5     | 12192 (5.9GiB)   | 30.6 GiB
VlmInferenceBatchMapper (GPU)      | 39360/1730867  | 12    | 0 (0.0B)         | 3.7 MiB
RefImgExtractMapper                | 39360/1730912  | 0     | 0 (0.0B)         | 84.9 MiB
BuildECommercePair→...             | 21036/1477443  | 0     | 189 (84.9MiB)    | 8.3 MiB
Write                              | 5553/1104244   | 0     | 152 (8.3MiB)     | 0.0 B
```

**关键矛盾**：
- 上游 FrameExtract 有 12192 blocks 堆积
- GPU Stage 有 200 actors 但只有 12 个 tasks 在运行
- GPU 输入队列是 0（没有数据）
- 下游 Build/Write 都是 0 tasks，被标记为 `backpressured:tasks(ResourceBudget)`

---

## 2. Ray Data 反压策略源码解析

### 2.1 反压策略类型

Ray Data 有 3 种反压策略，位于 `python/ray/data/_internal/execution/backpressure_policy/`：

| 策略 | 类名 | 触发条件 |
|------|------|---------|
| **ResourceBudget** | `ResourceBudgetBackpressurePolicy` | 资源预算不足 |
| **ConcurrencyCap** | `ConcurrencyCapBackpressurePolicy` | 输出队列增长过快 |
| **DownstreamCapacity** | `DownstreamCapacityBackpressurePolicy` | 下游消费能力不足 |

### 2.2 调度判断核心逻辑

```python
# streaming_executor_state.py:831-850
def get_eligible_operators(...):
    for op, state in topology.items():
        # 1. 检查反压策略
        for p in backpressure_policies:
            if not p.can_add_input(op):
                triggered_policy = p.name
                break
        in_backpressure = triggered_policy is not None

        # 2. 判断是否可调度
        is_completed = op.has_completed()
        can_add = op.can_add_input()
        has_bundles = state.has_pending_bundles()

        if not is_completed and can_add and has_bundles:
            if not in_backpressure:
                eligible_ops.append(op)  # 可以调度
            else:
                dispatchable_ops.append(op)  # 被反压
```

**一个 operator 能被调度需要同时满足**：
1. 未完成 (`has_completed() == False`)
2. 能接收输入 (`can_add_input() == True`)
3. 有数据可处理 (`has_pending_bundles() == True`)
4. 没被任何反压策略阻止

### 2.3 ResourceBudget 反压策略

```python
# resource_budget_backpressure_policy.py:21-25
class ResourceBudgetBackpressurePolicy(BackpressurePolicy):
    @property
    def name(self) -> str:
        return "ResourceBudget"

    def can_add_input(self, op: "PhysicalOperator") -> bool:
        if self._resource_manager._op_resource_allocator is not None:
            return self._resource_manager._op_resource_allocator.can_submit_new_task(op)
        return True
```

调用 `can_submit_new_task`：

```python
# resource_manager.py:860-874
def can_submit_new_task(self, op: PhysicalOperator) -> bool:
    budget = self.get_budget(op)

    if budget is None:
        return True

    return (
        # 条件1: 增量资源需求 <= 当前预算
        op.incremental_resource_usage().satisfies_limit(budget)
        and
        # 条件2: object_store 预算 >= 单个 task 预期最大输出
        budget.object_store_memory >= (op.metrics.obj_store_mem_max_pending_output_per_task or 0)
    )
```

---

## 3. 资源预算分配机制

### 3.1 eligible_ops 判断

```python
# resource_manager.py:408-418
def is_op_eligible(self, op: PhysicalOperator) -> bool:
    """Whether the op is eligible for memory reservation."""
    return (
        not op.throttling_disabled()          # 未禁用限流
        and not op.has_execution_finished()   # 未完成执行
    )

def get_eligible_ops(self) -> List[PhysicalOperator]:
    return [op for op in self._topology if self.is_op_eligible(op)]
```

**重要**：`is_op_eligible` **不检查 concurrency**，所有未完成且未禁用限流的 ops 都是 eligible 的。

### 3.2 预算预留分配

```python
# resource_manager.py:789-858 (ReservationOpResourceAllocator._update_reservation)
def _update_reservation(self, limits: ExecutionResources):
    eligible_ops = self._resource_manager.get_eligible_ops()
    remaining = limits.copy()

    # 平均分配预留给每个 op
    default_reserved = limits.scale(self._reservation_ratio / len(eligible_ops))

    for index, op in enumerate(eligible_ops):
        # 一半给输出
        reserved_for_outputs = ExecutionResources(
            0, 0, max(default_reserved.object_store_memory / 2, 1)
        )
        # 一半给任务
        reserved_for_tasks = default_reserved.subtract(reserved_for_outputs)

        self._op_reserved[op] = reserved_for_tasks
        self._reserved_for_op_outputs[op] = reserved_for_outputs.object_store_memory

        remaining = remaining.subtract(reserved_for_tasks.add(reserved_for_outputs))

    self._total_shared = remaining  # 剩余作为共享资源
```

**关键参数**：
- `reservation_ratio`: 默认 0.5（50% 用于预留，50% 作为共享）
- 每个 op 预留 = `limits × 0.5 / eligible_ops数量`

### 3.3 Budget 计算

```python
# resource_manager.py:918-1006 (ReservationOpResourceAllocator.update_budgets)
def update_budgets(self, *, limits: ExecutionResources):
    self._op_budgets.clear()
    eligible_ops = self._resource_manager.get_eligible_ops()
    remaining_shared = self._total_shared

    for op in eligible_ops:
        # 1. 计算 op 的内存使用量
        op_mem_usage = self._resource_manager.get_mem_op_internal(op)
        op_outputs_usage = self._resource_manager.get_mem_op_outputs(
            op, include_ineligible_downstream=True  # 包含下游 ineligible ops
        )
        op_mem_usage += max(op_outputs_usage - self._reserved_for_op_outputs[op], 0)

        # 2. budget = reserved - usage（独立计算，不受其他 op 影响）
        op_reserved_remaining = op_reserved.subtract(op_usage).max(ExecutionResources.zero())
        self._op_budgets[op] = op_reserved_remaining

        # 3. 超额部分从共享资源扣除
        op_reserved_exceeded = op_usage.subtract(op_reserved).max(ExecutionResources.zero())
        remaining_shared = remaining_shared.subtract(op_reserved_exceeded)

    # 4. 共享资源再分配给各 op
    for op in reversed(eligible_ops):
        op_shared = remaining_shared.scale(1.0 / (len(eligible_ops) - i))
        self._op_budgets[op] = self._op_budgets[op].add(op_shared)
```

**重要发现**：
- 每个 op 的预留是**独立的**
- 上游超额使用影响的是 `remaining_shared`（共享资源）
- 下游的 `reserved_remaining` 不受上游影响

### 3.4 Object Store 使用量计算

```python
# resource_manager.py:158-198
def _estimate_object_store_memory_usage(self, op, state) -> int:
    # op 内部使用（pending task outputs）
    mem_op_internal = op.metrics.obj_store_mem_pending_task_outputs or 0

    # op 输出使用
    op_outputs_bytes = (
        op.metrics.obj_store_mem_internal_outqueue  # 内部输出队列
        + state.output_queue_bytes()                 # 外部输出队列
    )

    # 下游使用的部分
    used_op_outputs_bytes = sum([
        (
            downstream_op.metrics.obj_store_mem_internal_inqueue      # 下游输入队列
            + downstream_op.metrics.obj_store_mem_pending_task_inputs # 下游 task 正在用
        )
        for downstream_op in op.output_dependencies
    ])

    self._mem_op_internal[op] = mem_op_internal
    self._mem_op_outputs[op] = op_outputs_bytes + used_op_outputs_bytes

    return self._mem_op_outputs[op] + self._mem_op_internal[op]
```

---

## 4. 关于 concurrency 的误解澄清

### 4.1 concurrency 不影响预算分配

从源码分析，`concurrency` 参数**不影响 ResourceBudget 策略的预算分配**：

```python
# is_op_eligible 不检查 concurrency
def is_op_eligible(self, op: PhysicalOperator) -> bool:
    return (
        not op.throttling_disabled()
        and not op.has_execution_finished()
    )
```

无论是否设置 `concurrency`，只要 op 满足条件就是 eligible 的，就会被分配预算。

### 4.2 concurrency 影响的是 ConcurrencyCapBackpressurePolicy

```python
# concurrency_cap_backpressure_policy.py:84-91
for op, _ in self._topology.items():
    if (
        isinstance(op, TaskPoolMapOperator)
        and op.get_max_concurrency_limit() is not None
    ):
        self._concurrency_caps[op] = op.get_max_concurrency_limit()
    else:
        self._concurrency_caps[op] = float("inf")  # 无限制
```

设置 `concurrency` 会让 ConcurrencyCap 策略**保证**该 op 的最大并发度。

---

## 5. 问题分析过程

### 5.1 初始假设（已证伪）

最初假设：下游没设置 `concurrency` 导致被分配 0 资源。

**证伪**：从源码看，预算分配与 `concurrency` 无关，所有 eligible ops 都会被分配预算。

### 5.2 第二个假设（部分正确）

假设：上游 FrameExtract 使用 30.6 GiB，超额消耗共享资源，导致下游 budget 不足。

**分析**：
```
预算配置: 64 GiB
eligible ops: ~6 个
每个 op 预留: 64 × 0.5 / 6 ≈ 5.3 GiB

FrameExtract 使用: 30.6 GiB
超额: 30.6 - 5.3 = 25.3 GiB（从共享资源扣除）
```

**但问题是**：Build 的使用量只有 8.3 MiB，远小于预留的 5.3 GiB。
```
Build reserved ≈ 5.3 GiB
Build usage = 8.3 MiB
Build budget = reserved_remaining + shared ≈ 5.3 GiB + 0 = 5.3 GiB
```

5.3 GiB 应该足够运行 task！

### 5.3 待确认的可能原因

1. **CPU budget 不足**
   - `can_submit_new_task` 检查的不只是 object_store_memory，还有 CPU 和 GPU
   - GPU stage 使用了 2400 CPU (200 × 12)，可能导致 CPU budget 分配不均

2. **单 task 输出估算过大**
   ```python
   budget.object_store_memory >= op.metrics.obj_store_mem_max_pending_output_per_task
   ```
   如果 Ray 估算单个 task 输出会很大，可能触发这个条件。

---

## 6. 日志和指标判断反压原因的方法

### 6.1 关键日志格式

```
2026-04-11 17:39:22,445 INFO logging_progress.py:231 -- Stage名称: 进度/总数
2026-04-11 17:39:22,445 INFO logging_progress.py:233 -- Tasks: N [backpressured:策略名]; Actors: M; Queued blocks: X (Y); Resources: ...
```

### 6.2 关键指标解读

| 指标 | 含义 | 如何判断问题 |
|------|------|-------------|
| `Tasks: N` | 正在运行的 task 数 | 0 表示被完全阻止 |
| `backpressured:tasks(XXX)` | 被哪个策略反压 | ResourceBudget/ConcurrencyCap/DownstreamCapacity |
| `Actors: M` | actor 数量 | 对 ActorPoolMapOperator 有效 |
| `Queued blocks: X (Y)` | 输出队列中的 blocks | 上游大、下游小 = 数据卡住 |
| `Resources: A CPU, B GPU, C object store` | 当前资源使用 | 判断哪种资源紧张 |
| `[N/M objects local]` | 数据本地性 | 低于 90% 可能有跨节点传输开销 |

### 6.3 常见反压模式

#### 模式 1：下游停滞导致上游堆积

```
上游: Queued blocks 高, Tasks > 0
下游: Queued blocks 低, Tasks = 0, backpressured
```

**原因**：下游被反压，无法消费上游输出。

#### 模式 2：GPU 限流

```
GPU Stage: Tasks << Actors, Queued blocks = 0
上游: Queued blocks 高
```

**原因**：下游无法消费 GPU 输出 → GPU 被限流 → 上游数据堆积。

#### 模式 3：资源不均

```
某 Stage: Object Store 使用远超其他
其他 Stage: backpressured:ResourceBudget
```

**原因**：资源被某个 stage 占用过多。

---

## 7. 设计问题讨论

### 7.1 用户预期 vs 实际行为

**用户预期**：
- 设置了 64 GiB object_store_memory 预算
- Pipeline 应该正常流动

**实际行为**：
- 上游使用 30.6 GiB，数据堆积
- 下游被反压，0 tasks
- 数据无法流向下游

### 7.2 潜在的设计问题

1. **预算分配策略可能导致死锁**
   - 上游产出速度 >> 下游消费速度
   - 上游数据堆积，使用大量资源
   - 下游被反压，消费更慢
   - 恶性循环

2. **理想的行为**
   - 当下游被反压时，应该**优先让数据流向下游**
   - 而不是让上游继续堆积
   - 或者动态调整 budget 分配

---

## 8. 解决方案

### 8.1 增加 object_store_memory 预算

```python
from ray.data._internal.execution.interfaces import ExecutionResources

ctx = ray.data.DataContext.get_current()
ctx.execution_options.resource_limits = ExecutionResources(
    object_store_memory=256 * 1024**3  # 从 64GB 增加到 256GB
)
```

### 8.2 降低 GPU Stage 的 CPU 占用

```python
# 当前配置
.map_batches(VlmInferenceBatchMapper,
             concurrency=200,
             num_cpus=12,  # GPU 推理不需要那么多 CPU
             num_gpus=1)

# 建议修改
.map_batches(VlmInferenceBatchMapper,
             concurrency=200,
             num_cpus=1,   # 降低 CPU 占用
             num_gpus=1)
```

### 8.3 给所有 stages 显式设置 concurrency

虽然不影响 ResourceBudget，但会影响 ConcurrencyCap 策略：

```python
# Stage4
ds = ds.map(BuildECommercePairResultMapper({}),
            concurrency=100, num_cpus=1)

# JSON 写入
out = out.map_batches(_write_batch_to_jsonl, batch_format="pyarrow",
                      concurrency=50, num_cpus=1)

# Kafka 转换
kafka_ds = out.map(_to_multi_feature_data_info2,
                   concurrency=50, num_cpus=1)
```

### 8.4 限制上游产出速度

```python
# 降低上游并发度
stage1_concurrency = 50  # 从默认值降低
```

### 8.5 开启调试日志

```bash
export RAY_DATA_DEBUG_RESOURCE_MANAGER=1
```

会输出每个 op 的详细 budget 分配信息：
```
budget=(cpu=X, gpu=Y, obj_store=Z)
alloc=(cpu=X, gpu=Y, obj_store=Z)
```

---

## 9. 待进一步确认的问题

1. **Build stage 的 budget 具体值是多少？**
   - 理论上应该有 ~5.3 GiB
   - 需要通过调试日志确认

2. **`obj_store_mem_max_pending_output_per_task` 的值是多少？**
   - 如果估算过大，可能导致 budget 检查失败

3. **CPU budget 是否不足？**
   - GPU stage 占用 2400 CPU
   - 需要确认下游 stages 的 CPU budget

---

## 10. 相关源码文件

| 文件 | 功能 |
|------|------|
| `backpressure_policy/backpressure_policy.py` | 反压策略基类 |
| `backpressure_policy/resource_budget_backpressure_policy.py` | ResourceBudget 策略 |
| `backpressure_policy/concurrency_cap_backpressure_policy.py` | ConcurrencyCap 策略 |
| `backpressure_policy/downstream_capacity_backpressure_policy.py` | DownstreamCapacity 策略 |
| `resource_manager.py` | 资源管理和预算分配 |
| `streaming_executor_state.py` | 调度状态和 operator 选择 |
| `streaming_executor.py` | 流式执行器主循环 |

---

## 11. 参考资料

- Ray Data 源码: `python/ray/data/_internal/execution/`
- Ray Dashboard URL: `https://kml-task-*-prod-dashboard.kce-aip-*.corp.kuaishou.com`
- Pipeline 代码: `pipeline/user_custom/e_commerce_pair_pipeline.py`
