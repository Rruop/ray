# Streaming Output Backpressure Escape Hatch 机制详解

## 概述

`_should_unblock_streaming_output_backpressure` 是 Ray Data streaming executor 中防止死锁的关键"逃生舱"机制。当算子的 object store 内存预算为 0 时，该方法判断是否应该临时解除反压，允许读取至少 1 个 block 的输出，从而打破循环依赖。

## 方法签名与位置

- **当前所在类**: `OpResourceAllocator`（基类），位于 `python/ray/data/_internal/execution/resource_manager.py`
- **调用点**: `ReservationOpResourceAllocator.max_task_output_bytes_to_read()` 中，当 `res == 0` 时调用

```python
if res == 0 and self._should_unblock_streaming_output_backpressure(op):
    res = 1
```

## 完整逻辑分析

### 1. 判断终端算子（无下游 eligible 算子）

```python
downstream_eligible_ops = list(self._get_downstream_eligible_ops(op))
if not downstream_eligible_ops:
    return True
```

通过 `_get_downstream_eligible_ops(op)` 返回空列表来判断"无下游 eligible 算子"。

#### `_get_downstream_eligible_ops` 的递归逻辑

该方法递归遍历 `op.output_dependencies`（下游算子），只 yield 满足 `_is_op_eligible` 的算子：

```python
def _get_downstream_eligible_ops(self, op):
    for next_op in op.output_dependencies:
        if self._is_op_eligible(next_op):
            yield next_op
        else:
            yield from self._get_downstream_eligible_ops(next_op)
```

示例：
- `cur_map -> downstream_map` → 返回 `[downstream_map]`
- `cur_map -> limit1 -> limit2 -> downstream_map` → 跳过 ineligible 的 limit1/limit2，返回 `[downstream_map]`

#### `_is_op_eligible` 判断条件

```python
@staticmethod
def _is_op_eligible(op: PhysicalOperator) -> bool:
    return (
        not op.throttling_disabled()
        and not op.has_execution_finished()
    )
```

算子满足 eligible（可参与资源分配/反压调度）需要同时满足两个条件：

1. **`not op.throttling_disabled()`** — 算子的限流未被禁用。某些算子（如 `InputDataBuffer`、`LimitOperator`）会禁用 throttling，它们不受资源预算约束，也不参与 reservation 分配。

2. **`not op.has_execution_finished()`** — 算子尚未执行完毕。已完成的算子即使输出队列里还有 block，也不需要再分配资源（block 会自然被下游消费掉），无需占用预算。

**无下游 eligible 算子** 意味着：
- `op` 没有 `output_dependencies`（DAG 末端/sink，如写入算子），或
- 所有下游算子要么 throttling 被禁用，要么已执行完毕

本质上是 DAG 的出口节点——它的输出直接被外部消费者（`iter_batches`、`streaming_split`）消费，或者整个 pipeline 已没有活跃的下游算子需要处理。对这种节点，输出不应被反压限流，因为没有下游会来消费它——如果卡住，pipeline 就整个停滞。

### 2. Case 1: 下游算子无活跃任务且无法提交新任务（资源受限）

```python
if downstream_op.num_active_tasks() == 0:
    if not self.can_submit_new_task(downstream_op):
        return True
```

下游算子没有正在运行的任务，且由于资源约束无法调度新任务。此时放行上游输出，让上游 task 尽快完成并释放 CPU/内存资源给下游。

### 3. Case 2: 下游算子无活跃任务且输入队列为空

```python
elif downstream_op_state.total_enqueued_input_blocks() == 0:
    return True
```

下游算子可以调度新任务，但输入队列中没有 block。此时放行上游输出，让下游至少获得 1 个 block 来启动任务。

### 4. 兜底: 空闲检测（IdleDetector）

```python
return self._idle_detector.detect_idle(op)
```

作为最后手段，检查算子是否长时间没有产生任何输出（可能被非 Data 任务/actor 抢占了资源）。如果空闲超过 `DETECTION_INTERVAL_S`（10秒），则放行。

- 每 10 秒检测一次
- 空闲超过 60 秒会打印警告

## 为什么需要这个补丁

在 per-op resource reservation 机制下，每个算子有严格的内存预算。当预算耗尽时会触发反压，但严格反压可能导致**死锁**：

```
上游算子: 内存预算=0 → 无法读取 task 输出 → task 无法完成 → 占用的 CPU/内存不释放
下游算子: 无资源 → 无法启动 task → 没有消费 → 上游输出无处可去
→ 循环等待，pipeline 完全卡死
```

逃生舱通过"预算为 0 时也允许读 1 个 block"来打破循环依赖，维持 pipeline 活性（liveness）。

## Commit 历史

### 最初引入

- **Commit**: `d6380d441d`
- **标题**: `[data] Enable per-op resource reservation (#43171)`
- **作者**: Hao Chen
- **日期**: 2024-02-27
- **原始实现**: 方法位于 `ReservationOpResourceAllocator` 内部，仅两个 case：
  - Case 1: 下游算子未预留最小资源
  - Case 2: 下游算子空闲超时

```python
# 原始实现 (d6380d441d)
def _should_unblock_streaming_output_backpressure(self, op):
    for next_op in op.output_dependencies:
        if not self._reserved_min_resources[next_op]:
            # Case 1: 下游算子未预留最小资源
            return True
        if self._idle_detector.detect_idle(next_op):
            # Case 2: 下游算子空闲超时
            return True
    return False
```

### 重构迁移

- **Commit**: `b27d496950`
- **标题**: `[Data] Move streaming output backpressure escape hatch to apply across all backpressure policies (#63539)`
- **作者**: Nary Yeh
- **日期**: 2026-05-20

**重构原因**: 原来该方法只在 `ReservationOpResourceAllocator` 中生效（仅当资源预算策略返回 0 字节时触发），但其他反压策略（如 `DownstreamCapacityBackpressurePolicy`）也可能独立返回 0，此时逃生舱不会被调用，仍然会死锁。

**变更内容**:
- 将 `_should_unblock_streaming_output_backpressure` 从 `ReservationOpResourceAllocator` 提升到基类 `OpResourceAllocator`
- 将 `IdleDetector` 也提升到模块级别（`streaming_executor_state.py`）
- 增加了终端算子判断、下游输入队列检查等更完善的情况覆盖
- 使逃生舱无论哪个策略触发了反压都能生效

**涉及文件**:
- `resource_manager.py` — 删除 137 行（方法从子类移到基类）
- `streaming_executor_state.py` — 新增 `IdleDetector` 和逃生舱调用逻辑
- `streaming_executor.py` — 调用点适配
- 测试文件更新：`test_backpressure_e2e.py`、`test_resource_manager.py`、`test_streaming_executor.py`
