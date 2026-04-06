# ResourceManager update_usages 增量更新优化分析

> 基于 `resource_manager.py` 源码分析、社区 PR [#63750](https://github.com/ray-project/ray/pull/63750)、内部 commit [247c9c2d8b](Capacity-based dispatch) 的完整讨论整理
>
> 核心问题: `update_usages()` 在调度循环中被过度调用，每次全量遍历+重建 allocator reservation，是否可以优化更新频率

---

## 目录

- [一、update_usages() 逻辑详解](#一update_usages-逻辑详解)
- [二、调用频率分析](#二调用频率分析)
- [三、update_usages() 更新的目的是什么](#三update_usages-更新的目的是什么)
- [四、OpResourceAllocator 是否每次都动态重新分配](#四opresourceallocator-是否每次都动态重新分配)
- [五、是否有必要每次都更新](#五是否有必要每次都更新)
- [六、社区 PR #63750 增量更新方案](#六社区-pr-63750-增量更新方案)
- [七、PR #63750 是每 task 还是每 operator 更新](#七pr-63750-是每-task-还是每-operator-更新)
- [八、与 247c9c2d capacity-based dispatch 的组合](#八与-247c9c2d-capacity-based-dispatch-的组合)
- [九、每 op-batch 后增量更新是否有问题](#九每-op-batch-后增量更新是否有问题)
- [十、不能整个 dispatch 循环结束后才更新](#十不能整个-dispatch-循环结束后才更新)
- [十一、综合方案对比与推荐](#十一综合方案对比与推荐)

---

## 一、update_usages() 逻辑详解

### 1.1 完整逻辑拆解

`update_usages()` (`resource_manager.py:236`) 每次调用执行以下操作：

**Step 1 — 清零全局和逐算子用量缓存**

```python
self._global_usage = ExecutionResources(0, 0, 0)
self._global_running_usage = ExecutionResources(0, 0, 0)
self._global_pending_usage = ExecutionResources(0, 0, 0)
self._op_usages.clear()
self._op_running_usages.clear()
self._op_pending_usages.clear()
```

**Step 2 — 逆序遍历 topology 中每个算子**，对每个算子做：
- 调用 `op.current_logical_usage()` → 获取 CPU/GPU 逻辑用量
- 调用 `op.running_logical_usage()` → 运行中用量 = 当前用量 - pending 用量
- 调用 `op.pending_logical_usage()` → pending（如待启动 actor）用量
- 调用 `_estimate_object_store_memory_usage(op, state)` → 估算 object store 内存：
  - 读取 `op.metrics.obj_store_mem_pending_task_outputs`（运行中任务的输出估计）
  - 读取 `op.metrics.obj_store_mem_internal_outqueue`（内部输出队列字节数）
  - 读取 `state.output_queue_bytes()`（外部输出队列字节数）
  - 遍历下游算子，读取 `downstream_op.metrics.obj_store_mem_internal_inqueue_for_input()` + `obj_store_mem_pending_task_inputs`
  - 对 output_operator 额外加上 `self._external_consumer_bytes`
- 若算子实现了 `ReportsExtraResourceUsage`，加上额外用量
- 将 per-op 用量累加到全局用量

**Step 3 — 如果启用了 OpResourceAllocator，调用 `_update_allocated_budgets()`**
- 计算已完成算子的用量并排除
- 用剩余 limits 调用 `allocator.update_budgets()` → 触发 `_update_reservation()` + 重新分配 per-op budget

### 1.2 被调用的子方法开销评估

| 方法 | 实现方式 | 是否有远程调用 | 单次开销 |
|------|---------|---------------|---------|
| `op.current_logical_usage()` | TaskPoolMapOp: 读缓存计数器; ActorPoolMapOp: 读 actor pool 状态乘资源系数; HashShuffle: 读缓存的 rank 数 | 无 | O(1) 微秒级 |
| `op.running_logical_usage()` | `current - pending`，纯算术 | 无 | O(1) 微秒级 |
| `op.pending_logical_usage()` | ActorPoolMapOp: 读 pending/restarting actor 数乘系数; 其他: 返回 zero | 无 | O(1) 微秒级 |
| `_estimate_object_store_memory_usage()` | 读多个 `op.metrics.*` 属性（都是缓存的 `self._nbytes` 计数器）+ 遍历 `output_dependencies` 读取下游 input queue 字节数 | 无 | O(1 + downstream_count) |
| `op.extra_resource_usage()` | 抽象方法，由 ReportsExtraResourceUsage mixin 实现 | 取决于实现 | 通常 O(1) |
| `_update_allocated_budgets()` | 完整重建 reservation + 重算所有 budget | 无 | O(K)，详见第四节 |

**核心结论**：所有被调用的方法都是进程内缓存计数器读取，无远程调用、无 Ray runtime 调用。单次 `update_usages()` 的单算子开销是 O(1) 微秒级。但主要开销在于：
1. **累积效应**：每次遍历全 topology K 个算子，清零+重建所有 dict
2. **`_update_allocated_budgets()`**：如果 allocator 启用，每次触发完整的 reservation 重新计算
3. **`_estimate_object_store_memory_usage`**：对每个算子遍历 `output_dependencies`，整体 O(E)（E = DAG 边数）

---

## 二、调用频率分析

每次调度循环迭代（`_scheduling_loop_step`）中，`update_usages()` 被调用 **2 + N** 次：

| 调用位置 | 文件:行号 | 所在函数 | 频率 | 用途 |
|---------|----------|---------|------|------|
| `streaming_executor.py:533` | `_scheduling_loop_step` 顶部 | 每迭代 1 次 | 在 `ray.wait()` 前刷新用量 |
| `streaming_executor.py:567` | `_scheduling_loop_step` 中部 | 每迭代 1 次 | 在 `process_completed_tasks()` 后、dispatch 前刷新 |
| `streaming_executor.py:664` | `_dispatch_loop` 内 | 每选到一个算子 1 次 | 每个 op-batch 后刷新，确保 `select_operator_to_run` 看到最新用量 |
| `streaming_executor.py:298` | `shutdown()` | 执行结束时 1 次 | 最终指标刷新（非高频） |

**最坏情况**：topology 有 K 个可调度算子 → 每个调度迭代调用 2 + K 次。

源码中已有 TODO 承认了这个问题：

```python
# TODO(hchen): This method will be called frequently during the execution loop.
# And some computations are redundant. We should either remove redundant
# computations or remove this method entirely and compute usages on demand.
```

---

## 三、update_usages() 更新的目的是什么

`update_usages()` 的核心目的是**为调度决策提供实时资源快照**，具体有三个下游消费者：

### 3.1 背压决策（Backpressure）

`select_operator_to_run` 和 `BackpressurePolicy.available_capacity()` 依赖 `get_op_usage()` / `get_global_usage()` 判断：
- 全局用量是否超过 limits（`global_usage >= global_limits` → 整体停调度）
- 某算子 object store 内存是否超预算（超了就不再给它派新任务）
- `can_submit_new_task()` 用 budget 判断算子能否提交新任务

### 3.2 资源预算分配（OpResourceAllocator）

`_update_allocated_budgets()` 根据当前用量重新分配每个算子的 budget：
- `budget = allocation - usage`
- 用量变了 → budget 变了 → 下一个算子能不能调度就变了
- 如果不及时更新，可能导致：已完成的算子占着 budget 不释放，其他算子饿死

### 3.3 监控/可观测性

`op._metrics.obj_store_mem_used = op_usage.object_store_memory` 写入仪表盘指标，用于 Dashboard 展示。

**简言之**：每次调度都需要知道"现在每个算子用了多少资源、还剩多少预算"，`update_usages` 就是刷新这个快照。频繁调用的原因是任务提交/完成会持续改变用量，调度器怕用过时数据做出错误决策（比如超卖资源或死锁）。

---

## 四、OpResourceAllocator 是否每次都动态重新分配

**是的，每次 `update_usages()` 都会触发完整的动态重新分配**。调用链：

```
update_usages()
  └→ _update_allocated_budgets()
       └→ allocator.update_budgets(limits=available_limits)
            └→ _update_reservation(limits)   ← 完全重建 reservation
                 ├→ 清空 _op_reserved, _reserved_for_op_outputs
                 ├→ 重新获取 eligible_ops（集合可能变化，如算子完成执行）
                 ├→ 均分 limits: default_reserved = limits.scale(ratio / num_eligible_ops)
                 ├→ 逐算子分配：考虑 min_max_resource_requirements 约束，从 remaining 中扣除
                 └→ 剩余部分放入 total_shared
            └→ 重算每个 op 的 budget
                 budget = op_reserved_remaining + share_of_total_shared
```

**这意味着**：调度循环中 `update_usages()` 被调用 2+N 次 → reservation 重建 2+N 次 → 所有 `_op_reserved`, `_reserved_for_op_outputs`, `_op_budgets` 字典被 clear+重建 2+N 次。

其中 `_update_reservation` 涉及 `limits.scale()` + 逐算子 `subtract`/`max`/`satisfies_limit` 计算，是最重的部分。而 **reservation 的分配比例实际上只跟 eligible_ops 集合和 limits 有关**——这两个在同一个调度迭代内几乎不变，但被重复计算了 2+N 次。

---

## 五、是否有必要每次都更新

**没必要**。分析一下单次调度迭代内什么变了、什么没变：

### 5.1 `_update_reservation` 的输入

| 输入 | 同一调度迭代内是否变化 |
|------|----------------------|
| `limits` | 不变（已有 1s 限频） |
| `eligible_ops` | 几乎不变（仅算子完成时才变） |
| `reservation_ratio` | 常量 |

→ **reservation 分配比例在同一调度迭代内是幂等的**，但被重建了 2+N 次。

### 5.2 `update_budgets` 的输入

| 输入 | 同一调度迭代内是否变化 |
|------|----------------------|
| reservation | 不变（如上） |
| `op_usage` | **会变**（dispatch 一个任务后用量增加） |
| `op_mem_usage` | **会变** |

→ budget 确实需要反映最新用量，但 **只需要重算被 dispatch 的那个算子的 budget**，不需要清空重建所有算子的 reservation。

### 5.3 结论

真正需要频繁更新的是 **budget = reservation - usage** 中的 usage 部分，而 reservation（占总计算量的大部分）在同一调度迭代内完全可以复用。

---

## 六、社区 PR #63750 增量更新方案

### 6.1 PR 概述

[ray-project/ray#63750](https://github.com/ray-project/ray/pull/63750) `[data] Incrementally update resource usages after task dispatch`

核心思路：每次 dispatch 后只重算受影响的算子（当前 op + 其上游 input_dependencies + sink），而非遍历全 topology。

### 6.2 核心改动

**1. 抽取 `_recompute_op_usage(op)` — 单算子重算 + 增量更新全局**

```python
def _recompute_op_usage(self, op: "PhysicalOperator"):
    """Recompute a single operator's usage entries and apply the delta to
    the global totals.

    Subtracts the operator's previously-recorded usage from the global
    totals (if any) and adds the freshly-computed usage, so the globals
    stay consistent whether this is called for one operator or for all.
    """
    state = self._topology[op]

    op_usage = op.current_logical_usage()
    op_running_usage = op.running_logical_usage()
    op_pending_usage = op.pending_logical_usage()

    used_object_store = self._estimate_object_store_memory_usage(op, state)
    op_usage = op_usage.copy(object_store_memory=used_object_store)
    op_running_usage = op_running_usage.copy(object_store_memory=used_object_store)

    if isinstance(op, ReportsExtraResourceUsage):
        op_usage = op_usage.add(op.extra_resource_usage())

    # 增量更新：减去旧值，加上新值
    prev_usage = self._op_usages.get(op)
    if prev_usage is not None:
        self._global_usage = self._global_usage.subtract(prev_usage)
        self._global_running_usage = self._global_running_usage.subtract(
            self._op_running_usages[op]
        )
        self._global_pending_usage = self._global_pending_usage.subtract(
            self._op_pending_usages[op]
        )
    self._global_usage = self._global_usage.add(op_usage)
    self._global_running_usage = self._global_running_usage.add(op_running_usage)
    self._global_pending_usage = self._global_pending_usage.add(op_pending_usage)

    self._op_usages[op] = op_usage
    self._op_running_usages[op] = op_running_usage
    self._op_pending_usages[op] = op_pending_usage

    op._metrics.obj_store_mem_used = op_usage.object_store_memory
```

**2. 新增 `update_usages_for_ops(ops)` — 只重算脏算子**

```python
def update_usages_for_ops(self, ops: Iterable["PhysicalOperator"]):
    """Incrementally update resource usages for the given operators.

    For each changed operator we also recompute its upstream input
    dependencies: an operator's object store memory estimate
    (_estimate_object_store_memory_usage) reads its *downstream* ops'
    input-queue metrics, so changing an operator invalidates the estimates
    of the operators feeding into it.

    We always also recompute the output (sink) operator: its estimate
    includes _external_consumer_bytes which the consumer thread can
    update at any time.
    """
    dirty: Set["PhysicalOperator"] = {self._output_operator}
    for op in ops:
        dirty.add(op)
        dirty.update(op.input_dependencies)

    for op in dirty:
        self._recompute_op_usage(op)

    if self._op_resource_allocator is not None:
        self._update_allocated_budgets()
```

**脏算子集合的构造逻辑**：
- **dispatch 的 op 本身**：用量变了（dispatched task 改变了 CPU/GPU/memory 计数）
- **op 的 `input_dependencies`**（上游算子）：因为 `_estimate_object_store_memory_usage` 会读取**下游**算子的 input-queue metrics，改变一个算子会使喂入它的上游算子的估算失效
- **output_operator（sink）**：因为 `_external_consumer_bytes` 可被消费者线程随时更新，不依赖其他 op

**3. `update_usages()` 重构为调用 `_recompute_op_usage`**

全量更新变成循环调用同一个 `_recompute_op_usage`，代码复用：

```python
def update_usages(self):
    """Recalculate resource usages for all operators from scratch."""
    self._global_usage = ExecutionResources(0, 0, 0)
    self._global_running_usage = ExecutionResources(0, 0, 0)
    self._global_pending_usage = ExecutionResources(0, 0, 0)
    self._op_usages.clear()
    self._op_running_usages.clear()
    self._op_pending_usages.clear()

    for op in reversed(self._topology.keys()):
        self._recompute_op_usage(op)

    if self._op_resource_allocator is not None:
        self._update_allocated_budgets()
```

**4. `_compute_completed_ops` 抽取**：将 `_get_completed_ops_usage` 中收集已完成+ineligible 算子的逻辑抽取为 `_compute_completed_ops()` 独立方法。

### 6.3 测试覆盖

新增 `test_incremental_usages_match_full_recompute`：创建两个 ResourceManager 实例共享同一 topology。每次指标变更后，一个做全量 `update_usages()`，另一个做 `update_usages_for_ops([changed_op])`，断言所有 per-op 和全局用量完全一致。覆盖了 task 生命周期的各阶段：input queued → task submitted → task finished → output moved → external consumer bytes 变化。

---

## 七、PR #63750 是每 task 还是每 operator 更新

**PR #63750 是每个 task 调度后更新**，不是每个 operator 批次后。

PR #63750 是基于 **247c9c2d 之前的旧代码**改的。旧代码的 dispatch 循环是逐个 task 派发：

```python
# PR #63750 的 base（旧代码）
while True:
    op = select_operator_to_run(...)
    if op is None: break
    while op_state.has_pending_bundles() and op.can_add_input():
        op_state.dispatch_next_task()
        self._resource_manager.update_usages_for_ops([op])  # ← 每 task 一次增量
        i += 1
```

**调用频率没变（每 task 一次），只是每次调用的代价从全量 O(K) 降到了增量 O(1)。**

而当前代码（247c9c2d 之后）已经把结构改成了每 op-batch 一次：

```python
# 当前代码（247c9c2d 之后）
while True:
    op = select_operator_to_run(...)
    if op is None: break
    soft_capacity = self._compute_soft_capacity(op)
    while pending and can_add_input and n < soft_capacity:
        op_state.dispatch_next_task()     # 派发 N 个 task
        n += 1
    self._resource_manager.update_usages()  # ← 每 op-batch 一次全量
```

---

## 八、与 247c9c2d capacity-based dispatch 的组合

### 8.1 247c9c2d 的改动回顾

commit `247c9c2d8b` `[data] Capacity-based dispatch: zero soft-policy slack`：

- 替换了周期性检查设计（每 `batch_resource_check_interval` 次 dispatch 才检查一次背压），改为 capacity-query 设计
- 每个背压策略暴露 `available_capacity(op) -> Optional[int]`：该策略下算子还能派发多少 task 的明确上界
- dispatch 循环查询一次 soft policy 的 capacity，然后一次性派发到 capacity 上限
- 移除了 `DataContext.batch_resource_check_interval` 和 `DEFAULT_BATCH_RESOURCE_CHECK_INTERVAL`

核心 dispatch 循环：

```python
while True:
    op = select_operator_to_run(...)
    if op is None: break

    soft_capacity = self._compute_soft_capacity(op)  # min(policies' capacity)
    if soft_capacity == 0: continue

    n = 0
    while pending and can_add_input and n < soft_capacity:
        op_state.dispatch_next_task()
        n += 1

    self._resource_manager.update_usages()  # ← 每 op-batch 一次全量
```

### 8.2 组合优化：每 op-batch 后增量更新

将 `_dispatch_loop` 中的 `update_usages()` 替换为 `update_usages_for_ops([op])`：

```python
# 优化后
self._resource_manager.update_usages_for_ops([op])   # ← O(1) 增量
```

### 8.3 四种方案的效果对比

| 方案 | 调用频率 | 单次开销 | 总开销（K 个算子，每算子派 N 个 task） |
|------|---------|---------|--------------------------------------|
| 旧代码 | 每 task | O(K) 全量 | O(K × N × K) |
| PR #63750（独立） | 每 task | O(1) 增量 | O(K × N) |
| 247c9c2d（当前） | 每 op-batch | O(K) 全量 | O(K × K) |
| **两者结合** | **每 op-batch** | **O(1) 增量** | **O(K)** |

**最优组合**：在 247c9c2d 的基础上，把 `update_usages()` 替换为 `update_usages_for_ops([op])`，即每 op-batch 后做一次增量更新——频率低（每算子一次），开销低（增量只重算脏算子）。

---

## 九、每 op-batch 后增量更新是否有问题

**没有问题**。逐步论证：

### 9.1 增量更新本身的正确性

`_recompute_op_usage(op)` 是**快照式**的——直接读取当前 metrics（已经反映了 N 个 dispatched task 的变化），减去旧缓存值，加上新值。无论中间派了 1 个还是 N 个 task，结果等价：

```
调用 N 次 update_usages_for_ops([op])  ≡  调用 1 次 update_usages_for_ops([op])
```

因为都是读到同一个最新 metrics 快照，算同一个 delta。

### 9.2 跨算子的正确性

dispatch A 的 N 个 task → `update_usages_for_ops([op_A])`：

| 更新了什么 | 对下一个算子 B 的影响 |
|-----------|---------------------|
| A 的 per-op usage | 不影响 B 的 per-op usage（B 没变） |
| 全局总量（增量） | B 的 budget 通过 `_update_allocated_budgets` 基于**新的全局总量**重新分配 |
| A 的上游 + sink | 不影响 B 的调度决策 |

所以 `select_operator_to_run` 选 B 时，B 的 budget 已经扣除了 A 刚消耗的资源。

### 9.3 soft_capacity 在 op-batch 内的一致性

在 A 的 op-batch 内，`soft_capacity` 是**batch 开始前**算的。派发过程中实际 budget 在被消耗，但没有实时更新。这**不是问题**，因为：
1. `soft_capacity` 本身就是"最多还能派多少个"的安全上界，派满正好用完 budget
2. 内循环有 `can_add_input()` 硬限制兜底（actor pool 状态等快速变化的约束）
3. 247c9c2d 的 capacity-based 设计已将 soft-policy slack 降为零

---

## 十、不能整个 dispatch 循环结束后才更新

**不行，会导致调度错误**。原因：

`select_operator_to_run` 和 `_compute_soft_capacity` 都依赖 `update_usages` 刷新的快照：

| 依赖方 | 读取的数据 | 如果不更新会怎样 |
|--------|-----------|----------------|
| `select_operator_to_run` | `get_op_usage()` 判断算子是否超预算 | 已超预算的算子仍被认为可调度 → **超卖资源** |
| `ResourceBudgetPolicy.available_capacity` | `allocator.available_task_capacity()` → 依赖 `get_budget()` → 依赖 per-op budget | budget 是旧的 → capacity 偏大 → **超量派发** |
| `ConcurrencyCapPolicy.available_capacity` | `num_tasks_running` | 已 dispatch 的任务没反映到 running 计数 → capacity 偏高 → **超出并发限制** |

**具体例子**：3 个算子各剩 budget 发 2 个 task，不更新的话 `select_operator_to_run` 可能给第一个算子派 2 个后，继续给同一个算子派（budget 快照显示还够），实际已超限。

**结论**：每 op-batch 后更新是**最低正确频率**，不能更低。

---

## 十一、综合方案对比与推荐

### 11.1 方案全景

| 方案 | 频率 | 单次开销 | 正确性 | 风险 |
|------|------|---------|--------|------|
| 当前代码（全量 update_usages） | 每 op-batch | O(K) 全量 | 正确 | 无（现状） |
| 整个 dispatch 循环后更新一次 | 每调度迭代 | O(K) 全量 | **不正确** | 超卖资源 |
| PR #63750（独立，每 task 增量） | 每 task | O(1) 增量 | 正确 | 低 |
| **两者结合（每 op-batch 增量）** | **每 op-batch** | **O(1) 增量** | **正确** | **低** |

### 11.2 推荐：分阶段实施

**Phase 1 — 采纳 PR #63750 增量更新**

修改 `_dispatch_loop`：

```python
# 替换
self._resource_manager.update_usages()
# 为
self._resource_manager.update_usages_for_ops([op])
```

保留 `_scheduling_loop_step` 顶部的两次全量 `update_usages()` 调用作为**校准点**——每隔一个调度迭代做一次全量重算，保证增量更新的累积误差不会扩散。

风险：低（PR #63750 有测试覆盖，且有全量校准兜底）

**Phase 2 — Allocator reservation 缓存**

将 `_update_reservation` 的结果缓存，只在 `eligible_ops` 集合或 `limits` 变化时才重建：

```python
def _update_allocated_budgets(self):
    completed_ops_usage = self._get_completed_ops_usage()
    available_limits = (
        self.get_global_limits()
        .subtract(completed_ops_usage)
        .max(ExecutionResources.zero())
    )

    # 只在 limits 或 eligible_ops 变化时重建 reservation
    current_eligible = set(self.get_eligible_ops())
    needs_rebuild = (
        self._last_reservation_limits is None
        or available_limits != self._last_reservation_limits
        or current_eligible != self._last_reservation_eligible_ops
    )

    if needs_rebuild:
        self._last_reservation_limits = available_limits
        self._last_reservation_eligible_ops = current_eligible
        self._update_reservation(available_limits)

    # budget 更新仍用最新 usage（轻量）
    self._update_budgets_only(available_limits)
```

风险：中（需验证 `eligible_ops` 集合在 dispatch 循环内变化时的正确性，如 `LimitOperator.mark_execution_finished` 在 `_add_input_inner` 中被调用）

**Phase 2 收益**：reservation 重建从每调度迭代 2+N 次降到 0~1 次。

### 11.3 需要注意的边界情况

- **算子在 dispatch 循环内完成执行**：`LimitOperator.mark_execution_finished` 可在 `_add_input_inner` 中被调用，导致 `eligible_ops` 集合变化。reservation 缓存必须通过 `current_eligible != self._last_reservation_eligible_ops` 检测并触发重建。
- **`_compute_completed_ops` 结果变化**：已完成算子的 usage 需要从 reservation 中排除，如果 completed ops 集合变了，`available_limits` 也会变，触发 reservation 重建。
- **`_external_consumer_bytes` 随时变化**：消费者线程可能随时更新此值，`update_usages_for_ops` 通过总是重算 output_operator 来保证不遗漏。

### 11.4 最终效果预估

以 K=10 个算子、每算子派 N=50 个 task 为例：

| | dispatch 循环内 update_usages 总次数 | 每次开销 | 总开销 |
|---|---|---|---|
| 当前代码 | 10 次（每 op-batch 一次） | O(K)=O(10) 全量 | O(100) |
| +Phase 1 | 10 次 | O(1) 增量 | O(10) |
| +Phase 2 | 10 次 budget + 0~1 次 reservation | O(1) budget | O(10) + O(1) reservation |

dispatch 循环内 update_usages 相关总开销降低约 **10x**。
