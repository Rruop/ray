# StreamingExecutor 调度循环优化方案

> 基于社区 issue [#63544](https://github.com/ray-project/ray/issues/63544)、内部 commit 6963b2d4 分析、release-2.55.1 基线
>
> 场景: 大规模（hundreds-to-thousands actors）+ 宽 schema（hundreds of columns）
>
> 核心观测指标: `max_scheduling_loop_duration_s`

---

## 目录

- [一、问题概述](#一问题概述)
- [二、release-2.55.1 调度架构详解](#二release-2551-调度架构详解)
  - [2.1 线程模型](#21-线程模型)
  - [2.2 Driver 进程与线程模型](#22-driver-进程与线程模型)
  - [2.3 调度循环完整流程](#23-_scheduling_loop_step-完整流程)
  - [2.7 Backpressure 策略体系](#27-backpressure-策略体系)
  - [2.8 ResourceManager 工作原理](#28-resourcemanager-工作原理)
    - [2.8.1 update_usages() 逻辑拆解](#281-update_usages-做了什么)
    - [2.8.2 调用频率分析](#282-update_usages-调用频率分析)
    - [2.8.3 三个下游消费者](#283-update_usages-的三个下游消费者)
    - [2.8.4 OpResourceAllocator 动态重新分配](#284-opresourceallocator-动态重新分配机制)
    - [2.8.5 全局资源限制](#285-全局资源限制)
  - [2.9 GIL 与线程安全](#29-gil-与线程安全)
  - [2.10 Prometheus 监控指标](#210-prometheus-监控指标)
- [三、瓶颈定位与 Profiling 方法](#三瓶颈定位与-profiling-方法)
- [四、Schema 反序列化：占 60% 的首要瓶颈](#四schema-反序列化占-60-的首要瓶颈)
- [五、内部 commit 6963b2d4 分析](#五内部-commit-6963b2d4-分析)
- [六、社区 Issue #63544 Related PRs 状态](#六社区-issue-63544-related-prs-状态)
- [七、综合优化方案](#七综合优化方案)
- [八、实施细节与代码示例](#八实施细节与代码示例)
- [九、预期收益与验证方法](#九预期收益与验证方法)
- [十、实施顺序与风险评估](#十实施顺序与风险评估)
- [附录A：相关社区 commits](#附录a相关社区-commits)
- [附录B：环境变量配置汇总](#附录b环境变量配置汇总)
- [附录C：常见问题与排查指南](#附录c常见问题与排查指南)
- [附录D：核心代码文件索引](#附录d核心代码文件索引)
- [附录E：术语表](#附录e术语表)

---

## 一、问题概述

在大规模 Ray Data workloads 下，Driver 端 `StreamingExecutor` 的调度线程成为全局瓶颈，workers 空闲等 scheduler。

### 1.1 社区 Profiling 数据

社区 issue #63544 的 baseline 数据（2026-05-21，master 分支，600 列宽 schema）:

| 测试用例 | max_scheduling_loop (s) | 模式 |
|----------|------------------------|------|
| 500 actors | 0.89 | Actor Pool |
| 500 tasks | 2.40 | Task Pool |
| 1000 actors | 1.69 | Actor Pool |
| 1000 tasks | 6.58 | Task Pool |
| 2000 actors | 3.19 | Actor Pool |
| 2000 tasks | 14.63 | Task Pool |
| 5000 actors | 9.83 | Actor Pool |
| 5000 tasks | 33.61 | Task Pool |

**观察**:
- 调度循环耗时与 worker 数量近似线性增长
- Task mode 比 Actor mode 慢 3-4x（task 不复用进程，每次有更多 metadata 传输）
- 5000 actors 时单步耗时接近 10 秒，意味着 actor 平均空闲等待 10 秒

### 1.2 内部场景数据（47K CPU / 1K GPU 集群）

| 指标 | 数值 |
|------|------|
| 首次爆发调度步（16K+ ready tasks） | 452s |
| 稳态调度步 | 200-400s |
| GPU actor dispatch 延迟 | 分钟级 |

---

## 二、release-2.55.1 调度架构详解

### 2.1 线程模型

```
┌───────────────────────────────────────────────────────────────────┐
│ 用户主线程                                                         │
│                                                                   │
│  for batch in ds.iter_batches():                                  │
│    → _ClosingIterator.get_next()                                  │
│      → OpState.get_output_blocking()                              │
│        → while True:                                              │
│            ref = output_queue.pop()                                │
│            if ref: return ref                                      │
│            time.sleep(0.01)          # 每10ms轮询一次              │
│                                                                   │
│  (阻塞等待 output queue 有数据)                                     │
└───────────────────────────────────────────────────────────────────┘
        ↕ 通过 OpState.output_queue 通信 (线程安全队列)
┌───────────────────────────────────────────────────────────────────┐
│ 调度 daemon 线程 (StreamingExecutor.run)                            │
│                                                                   │
│  while True:  # streaming_executor.py:448-481, 无sleep/yield      │
│    _scheduling_loop_step(topology)                                │
│    update_metrics()                                               │
│    if not continue_sched or shutdown: break                       │
└───────────────────────────────────────────────────────────────────┘
```

**关键事实**:
- 调度线程 `run()` 的外层循环**没有 sleep/yield**，唯一的"暂停"来自 `process_completed_tasks` 内部的 `ray.wait(timeout=0.1)`
- 用户线程和调度线程通过 `OpState.output_queue` 解耦
- **每个 Dataset 执行实例都有独立的 StreamingExecutor 线程**（见下节）

### 2.2 Driver 进程与线程模型

#### 2.2.1 每个 Dataset 独立一个调度线程

`StreamingExecutor` 继承 `threading.Thread`，每次 Dataset 触发执行（如 `.iter_batches()`、`.materialize()`）都会通过 `ExecutionPlan.create_executor()` 创建一个新实例：

```python
# plan.py:98-104
class ExecutionPlan:
    def create_executor(self) -> "StreamingExecutor":
        self._run_index += 1
        executor = StreamingExecutor(self._context, self.get_dataset_id())
        return executor
```

`dataset_id` 格式为 `{dataset_name}_{uuid}_{run_index}`（如 `dataset_abc123_1`）。每个 `StreamingExecutor` 实例启动后就是一个独立的 daemon 线程：

```python
# streaming_executor.py:144-145
thread_name = f"StreamingExecutor-{self._dataset_id}"
threading.Thread.__init__(self, daemon=True, name=thread_name)
```

**如果同时执行多个 Dataset，Driver 进程中会有多个 StreamingExecutor 线程并行运行。**

#### 2.2.2 Driver 进程在哪里

StreamingExecutor 是 Python `threading.Thread`，运行在创建和执行 Dataset 的那个进程中。在典型 Ray Data 使用场景中，这个进程就是 **Driver 进程**——即调用 `ray.init()` 和 `ray.data.read_xxx()` 的那个 Python 进程。

```
┌─────────────────────────────────────────────────────────────────┐
│ Driver 进程 (单个 Python 进程)                                     │
│                                                                   │
│  ├── 主线程 (用户代码: iter_batches, materialize 等消费数据)       │
│  ├── StreamingExecutor-dataset_001 线程 (调度循环)                │
│  ├── StreamingExecutor-dataset_002 线程 (如果有第二个 dataset)     │
│  ├── GCS client 通信线程                                          │
│  ├── Metrics 上报线程                                             │
│  └── 其他后台线程                                                 │
└─────────────────────────────────────────────────────────────────┘
       │                              │
       │ ray.wait / ray.get           │ dispatch tasks / get results
       ↓                              ↓
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│ Worker 进程 1 │  │ Worker 进程 2 │  │ Worker 进程 N │
│ (Actor/Task) │  │ (Actor/Task) │  │ (Actor/Task) │
│  执行 UDF    │  │  执行 UDF    │  │  执行 UDF    │
└──────────────┘  └──────────────┘  └──────────────┘
```

调度线程的职责：
- 收集 Worker（远端 Actor/Task）的完成结果
- 决定下一步派发哪些 task 到哪些 Worker
- 管理背压（backpressure）和资源预算
- 更新 metrics 和进度

**实际数据处理（UDF 执行）发生在远端 Worker 节点，但调度决策完全在 Driver 进程内的 StreamingExecutor 线程中完成。** 这就是为什么调度循环性能瓶颈影响巨大——整个集群所有 Worker 的调度都卡在 Driver 的这一个线程里。

#### 2.2.3 Driver 是独立进程吗

**是的，Driver 是一个独立的 Python 进程。** 根据使用模式有两种情况：

| 模式 | Driver 进程位置 | 说明 |
|------|----------------|------|
| **交互式** (`ray.init()` + 直接运行脚本) | 就是你的 Python 脚本进程本身 | 通常在 head node 上运行 |
| **Ray Job** (`ray job submit`) | Ray 在集群中启动一个专门的 worker 进程来运行你的脚本 | 这个进程充当 Driver 角色 |

无论哪种模式，Driver 进程都是：
- 持有 GCS 连接
- 运行 StreamingExecutor 调度线程
- 通过 Ray Object Store 与 Worker 通信
- **不参与实际数据计算**

#### 2.2.4 如何定位 Driver 进程

在排查调度循环瓶颈时，首先需要找到 Driver 进程。以下是多种方法：

**方法 1: 通过 Ray Dashboard（最直观）**

Dashboard → Jobs 页面直接展示每个 Job 的 Driver 进程信息，包括 PID 和所在节点。

**方法 2: 通过 `ray list jobs` CLI**

```bash
ray list jobs --format yaml
```

输出中包含 `driver_pid`、`driver_node_id` 等字段。

**方法 3: 通过进程命令行参数区分**

```bash
# 在集群节点上查看 Python 进程
ps aux | grep "python.*your_script.py"

# Driver 进程不会有 --worker-server-port 参数
# Worker 进程的 cmdline 里会有 ray::worker 或 --worker-server-port
```

- **交互式模式**: Driver 就是你直接运行的那个 Python 进程（`python my_pipeline.py`）
- **Ray Job 模式**: 进程名里带有你提交的脚本路径，且 `RAY_JOB_ID` 环境变量会被设置

**方法 4: 通过 py-spy 确认（看是否有 StreamingExecutor 线程）**

```bash
# 列出节点上所有 Python 进程
ps aux | grep python

# 对可疑进程 dump 线程信息
py-spy dump --pid <PID>
```

如果输出中有以下线程，即确认为 Driver：

```
Thread 0x7f3a2c (StreamingExecutor-dataset_xxx):
    _scheduling_loop_step (streaming_executor.py:520)
    run (streaming_executor.py:448)
```

**方法 5: 代码内获取 Driver 信息**

```python
import os
import ray

ctx = ray.get_runtime_context()
print(f"Driver Node ID: {ctx.get_node_id()}")
print(f"Driver PID: {os.getpid()}")
```

**方法 6: 通过端口/连接特征**

Driver 进程会连接 GCS（默认 6379 端口）和 Raylet（本地 socket），但**不会监听 worker-server-port**。可以用：

```bash
# ray 相关 python 进程
pgrep -af "ray"

# Driver 进程不会有 --worker-server-port 参数
# Worker 进程启动参数中会包含 ray::worker 或 --worker-server-port
```

**快速判断表**:

| 特征 | Driver 进程 | Worker 进程 |
|------|-------------|-------------|
| 有 `StreamingExecutor-*` 线程 | ✅ | ❌ |
| 命令行含 `--worker-server-port` | ❌ | ✅ |
| 命令行含用户脚本名 | ✅ | ❌ |
| `RAY_JOB_ID` 环境变量 | ✅ (Job 模式) | ❌ |
| Dashboard Jobs 页面可见 | ✅ | ❌ |

### 2.3 `_scheduling_loop_step` 完整流程

基于 `streaming_executor.py:520-619` 的实际代码：

```
_scheduling_loop_step(topology) → bool:
│
├─[1] resource_manager.update_usages()
│
├─[2] process_completed_tasks(topology, backpressure_policies, max_errored_blocks)
│   │
│   ├── 收集所有 active tasks 的 waitables                    (L410-413)
│   ├── 计算 remaining_output_budget (per operator)          (L415-440)
│   │   └── 取所有 backpressure policy 的 min limit
│   ├── ray.wait(active_tasks, timeout=0.1, fetch_local=False) ← 唯一阻塞点 (L446-451)
│   ├── 按 op 分组 ready tasks, 按 task_index 排序           (L458-465)
│   ├── 对每个 DataOpTask: task.on_data_ready(budget)        (L467-476)
│   │   └── ray.get(meta_ref, timeout=1.0) ← 反序列化 BlockMetadataWithSchema
│   ├── 错误处理: log/raise based on max_errored_blocks      (L477-507)
│   └── 拉取 operator outputs → OpState.output_queue         (L513-515)
│
├─[3] resource_manager.update_usages()
│
├─[4] Dispatch while True 循环                               (L557-578)
│   ├── select_operator_to_run(topology, resource_manager,
│   │       backpressure_policies, ensure_liveness, ranker)
│   │   ├── get_eligible_operators()              O(num_ops × num_policies)
│   │   │   检查: has_completed, can_add_input, has_pending_bundles
│   │   │   对每个 op 检查所有 backpressure_policies.can_add_input(op)
│   │   └── ranker.rank_operators()               O(num_eligible_ops)
│   │       DefaultRanker: (throttling_disabled, obj_store_mem)
│   ├── topology[op].dispatch_next_task()         pop from input queue → op.add_input()
│   ├── resource_manager.update_usages()          每次 dispatch 后都更新!
│   └── (循环直到 select_operator_to_run 返回 None)
│
├─[5] cluster_autoscaler.try_trigger_scaling()
├─[6] actor_autoscaler.try_trigger_scaling()
│   └── refresh_actor_state() for each pool
├─[7] config_controller.try_apply_config()       (if enabled)
├─[8] update_operator_states(topology)           propagate input_done
├─[9] refresh_progress_manager
├─[10] update_stats_metrics + debug_dump         (周期性, 300s)
├─[11] export_operator_schema                    (if updated)
├─[12] op.refresh_state() for incomplete ops
└─[13] return: not all(op.has_completed())
```

### 2.4 关键常量

| 常量 | 值 | 位置 | 作用 |
|------|-----|------|------|
| `ray.wait timeout` | 0.1s | `streaming_executor_state.py:449` | completion 等待超时 |
| `METADATA_GET_TIMEOUT_S` | 1.0s | `physical_operator.py:44` | ray.get(meta_ref) 超时 |
| `METADATA_WAIT_TIMEOUT_S` | 0.1s | `physical_operator.py:45` | streaming gen 等待 |
| `DEBUG_LOG_INTERVAL_SECONDS` | 300s | `streaming_executor.py:69` | debug 日志间隔 |
| `UPDATE_METRICS_INTERVAL_S` | 5.0s | `streaming_executor.py:88` | metrics 更新间隔 |
| `_ACTOR_POOL_SCALE_DOWN_DEBOUNCE_PERIOD_S` | 10s | `actor_pool_map_operator.py:770` | 缩容防抖 |

### 2.5 DefaultRanker 实现

```python
# ranker.py:75-124
class DefaultRanker(Ranker[Tuple[int, int]]):
    """
    排序维度 (值越小优先级越高):
    1. throttling_disabled: 0=不可节流(如InputDataBuffer), 1=可节流
    2. object_store_memory: 当前 op 的 object store 内存使用量

    效果:
    - 不可节流的 operator 总是最先被调度
    - 可节流 operator 中，内存使用最少的优先 → 防止某个 op 独占内存
    - 自然形成向 pipeline source 方向的反压
    """
    def rank_operator(self, op, topology, resource_manager) -> Tuple[int, int]:
        throttling_disabled = 0 if op.throttling_disabled() else 1
        obj_store_mem = resource_manager.get_op_usage(op).object_store_memory
        return (throttling_disabled, obj_store_mem)
```

### 2.6 Actor 选择算法

```python
# actor_pool_map_operator.py: select_actors()
#
# 数据结构:
#   _alive_actors_to_in_flight_tasks_heap: heapdict[ActorHandle, _ActorRank]
#     → min-heap, key=(num_tasks_in_flight, actor_id)
#   _alive_node_to_actor_map: DefaultDict[NodeIdStr, Set[ActorHandle]]
#     → 按节点分组的 actor 索引
#
# 选择流程:
# 1. 若 heap 为空 → None
# 2. peek 最少在飞 task 的 actor; 若已满 → None
# 3. 如果启用 locality:
#    a. 获取 bundle 的 preferred_object_locations (node -> bytes)
#    b. 对每个 preferred node, 遍历该 node 上的 actors
#    c. 按 (-total_bytes, num_tasks_in_flight) 排序选最优
# 4. 如果无 locality match: 从 heap 顶取
```

### 2.7 Backpressure 策略体系

调度循环中 `select_operator_to_run` 的核心是 backpressure 策略——决定哪些 operator 可以接受新 input。

#### 2.7.1 策略接口

```python
# backpressure_policy.py
class BackpressurePolicy(ABC):
    def can_add_input(self, op: PhysicalOperator) -> bool:
        """如果返回 False，op 被背压，不会接收新 task dispatch。
        注意: 多个策略同时启用时，任何一个返回 False 都会阻止 dispatch。"""
        return True

    def max_task_output_bytes_to_read(self, op: PhysicalOperator) -> Optional[int]:
        """限制 on_data_ready 时可读取的最大字节数。None=无限制。
        多个策略同时生效时取最小值。"""
        return None
```

#### 2.7.2 默认启用的三个策略

```python
# backpressure_policy/__init__.py:17-21
ENABLED_BACKPRESSURE_POLICIES = [
    ConcurrencyCapBackpressurePolicy,
    ResourceBudgetBackpressurePolicy,
    DownstreamCapacityBackpressurePolicy,
]
```

| 策略 | UX 名 | 核心逻辑 | 适用场景 |
|------|--------|----------|----------|
| **ConcurrencyCapBackpressurePolicy** | `ConcurrencyCap` | 基于 EWMA 动态控制每个 op 的并发 task 数；当 output queue 超出 `level + K_DEV * dev` 时 cap 收敛 | 防止 fast producer 堆积过多 output |
| **ResourceBudgetBackpressurePolicy** | `ResourceBudget` | 委托给 `ResourceAllocator.can_submit_new_task(op)` | 全局 CPU/GPU/Memory 预算限制 |
| **DownstreamCapacityBackpressurePolicy** | `DownstreamCapacity` | 当 output_queue / 下游容量 > ratio 且 object store 利用率 > 50% 时背压 | 防止上游产出超出下游消化能力 |

#### 2.7.3 Liveness 保证机制

当所有 operator 都被 backpressure 阻止时，pipeline 可能死锁。`get_eligible_operators()` 的 liveness 保证：

```python
# streaming_executor_state.py:661-671
if (
    not eligible_ops                    # 没有任何 op eligible
    and ensure_liveness                 # 消费者在等数据 (output_queue 为空)
    and all(op.num_active_tasks() == 0  # 所有 op 完全空闲
            for op in topology)
):
    # 绕过 backpressure，返回有 pending bundles 的 ops
    return dispatchable_ops
```

`ensure_liveness` 由 `_consumer_idling()` 驱动——当用户主线程在等数据（output_queue 为空）且整个 topology 完全空闲时，强制允许 dispatch，防止死锁。

### 2.8 ResourceManager 工作原理

#### 2.8.1 `update_usages()` 做了什么

`update_usages()` 是 ResourceManager 的核心方法，负责刷新全局和逐算子的资源使用快照，供调度决策使用。

**完整逻辑拆解**：

```python
# resource_manager.py:236-289
def update_usages(self):
    """Recalculate resource usages."""
    # Step 1: 清零全局和 per-op 缓存
    self._global_usage = ExecutionResources(0, 0, 0)
    self._global_running_usage = ExecutionResources(0, 0, 0)
    self._global_pending_usage = ExecutionResources(0, 0, 0)
    self._op_usages.clear()
    self._op_running_usages.clear()
    self._op_pending_usages.clear()

    # Step 2: 逆序遍历 topology 中每个算子
    for op, state in reversed(self._topology.items()):
        # 2a: 获取逻辑资源用量 (CPU/GPU, 不含 object_store_memory)
        op_usage = op.current_logical_usage()
        op_running_usage = op.running_logical_usage()   # = current - pending
        op_pending_usage = op.pending_logical_usage()

        # 2b: 估算 object store 内存用量
        # 包含: pending task outputs + internal output queue
        #       + external output queue + downstream input buffers
        used_object_store = self._estimate_object_store_memory_usage(op, state)

        op_usage = op_usage.copy(object_store_memory=used_object_store)
        op_running_usage = op_running_usage.copy(object_store_memory=used_object_store)

        # 2c: 额外资源用量（如 ReportsExtraResourceUsage mixin）
        if isinstance(op, ReportsExtraResourceUsage):
            op_usage.add(op.extra_resource_usage())

        # 2d: 存入 per-op 缓存 + 累加到全局总量
        self._op_usages[op] = op_usage
        self._op_running_usages[op] = op_running_usage
        self._op_pending_usages[op] = op_pending_usage

        self._global_usage = self._global_usage.add(op_usage)
        self._global_running_usage = self._global_running_usage.add(op_running_usage)
        self._global_pending_usage = self._global_pending_usage.add(op_pending_usage)

        # 2e: 更新 Dashboard 可观测性指标
        op._metrics.obj_store_mem_used = op_usage.object_store_memory

    # Step 3: 如果有 OpResourceAllocator，触发预算重新分配
    if self._op_resource_allocator is not None:
        self._update_allocated_budgets()
```

**被调用的子方法开销评估**：

| 方法 | 实现方式 | 是否有远程调用 | 单次开销 |
|------|---------|---------------|---------|
| `op.current_logical_usage()` | TaskPoolMapOp: 读缓存计数器; ActorPoolMapOp: 读 actor pool 状态乘资源系数; HashShuffle: 读缓存的 rank 数 | 无 | O(1) 微秒级 |
| `op.running_logical_usage()` | `current - pending`，纯算术 | 无 | O(1) 微秒级 |
| `op.pending_logical_usage()` | ActorPoolMapOp: 读 pending/restarting actor 数乘系数; 其他: 返回 zero | 无 | O(1) 微秒级 |
| `_estimate_object_store_memory_usage()` | 读多个 `op.metrics.*` 属性（都是缓存的 `self._nbytes` 计数器）+ 遍历 `output_dependencies` 读取下游 input queue 字节数 | 无 | O(1 + downstream_count) |
| `op.extra_resource_usage()` | 抽象方法，由 mixin 实现 | 取决于实现 | 通常 O(1) |
| `_update_allocated_budgets()` | 完整重建 reservation + 重算所有 budget | 无 | O(K)，详见 2.8.4 |

**核心结论**：单次 `update_usages()` 的单算子部分都是**进程内缓存读取**，无远程调用、无 Ray runtime 调用，非常快。主要开销在于：
1. **累积效应**：每次遍历全 topology K 个算子，清零+重建所有 dict
2. **`_update_allocated_budgets()`**：如果 allocator 启用，每次触发完整的 reservation 重新计算
3. **`_estimate_object_store_memory_usage`**：对每个算子遍历 `output_dependencies`，整体 O(E)（E = DAG 边数）

#### 2.8.2 `update_usages()` 调用频率分析

每次调度循环迭代（`_scheduling_loop_step`）中，`update_usages()` 被调用 **2 + N** 次：

| 调用位置 | 文件:行号 | 所在函数 | 频率 | 用途 |
|---------|----------|---------|------|------|
| `streaming_executor.py:533` | `_scheduling_loop_step` 顶部 | 每迭代 1 次 | 在 `ray.wait()` 前刷新用量 |
| `streaming_executor.py:567` | `_scheduling_loop_step` 中部 | 每迭代 1 次 | 在 `process_completed_tasks()` 后、dispatch 前刷新 |
| `streaming_executor.py:664` | `_dispatch_loop` 内 | 每选到一个算子 1 次 | 每个 op-batch 后刷新，确保 `select_operator_to_run` 看到最新用量 |
| `streaming_executor.py:298` | `shutdown()` | 执行结束时 1 次 | 最终指标刷新（非高频） |

**最坏情况**：topology 有 K 个可调度算子 → 每个调度迭代调用 2 + K 次。

**源码中已有的 TODO 承认**：
```python
# TODO(hchen): This method will be called frequently during the execution loop.
# And some computations are redundant. We should either remove redundant
# computations or remove this method entirely and compute usages on demand.
```

#### 2.8.3 `update_usages()` 的三个下游消费者

`update_usages()` 刷新的快照供三个核心消费者使用：

**消费者 1：背压决策（Backpressure）**

`select_operator_to_run` 和 `BackpressurePolicy.available_capacity()` 依赖 `get_op_usage()` / `get_global_usage()` 判断：
- 全局用量是否超过 limits（`global_usage >= global_limits` → 整体停调度）
- 某算子 object store 内存是否超预算（超了就不再给它派新任务）
- `can_submit_new_task()` 用 budget 判断算子能否提交新任务

**消费者 2：资源预算分配（OpResourceAllocator）**

`_update_allocated_budgets()` 根据当前用量重新分配每个算子的 budget：
- `budget = allocation - usage`
- 用量变了 → budget 变了 → 下一个算子能不能调度就变了
- 如果不及时更新，可能导致：已完成的算子占着 budget 不释放，其他算子饿死

**消费者 3：监控/可观测性**

`op._metrics.obj_store_mem_used = op_usage.object_store_memory` 写入仪表盘指标，用于 Dashboard 展示。

#### 2.8.4 OpResourceAllocator 动态重新分配机制

**每次 `update_usages()` 都会触发完整的动态重新分配**。调用链：

```
update_usages()
  └→ _update_allocated_budgets()
       └→ allocator.update_budgets(limits=available_limits)
            └→ _update_reservation(limits)   ← 完全重建 reservation
                 ├→ 清空 _op_reserved, _reserved_for_op_outputs
                 ├→ 重新获取 eligible_ops（集合可能变化）
                 ├→ 均分 limits: default_reserved = limits.scale(ratio / num_eligible_ops)
                 ├→ 逐算子分配：考虑 min_max_resource_requirements 约束
                 └→ 剩余部分放入 total_shared
            └→ 重算每个 op 的 budget
                 budget = op_reserved_remaining + share_of_total_shared
```

**`_update_reservation` 的输入稳定性分析**：

| 输入 | 同一调度迭代内是否变化 |
|------|----------------------|
| `limits` | 不变（已有 1s 限频的 `get_global_limits()`） |
| `eligible_ops` | 几乎不变（仅算子完成执行时才变） |
| `reservation_ratio` | 常量 |

→ **reservation 分配比例在同一调度迭代内是幂等的**，但被重建了 2+N 次。

真正需要频繁更新的是 **budget = reservation - usage** 中的 usage 部分，而 reservation（占总计算量的大部分）在同一调度迭代内完全可以复用。

#### 2.8.5 全局资源限制

- 集群资源每 1 秒刷新一次（`GLOBAL_LIMITS_UPDATE_INTERVAL_S = 1`）
- Object Store 预算默认为集群总量的 50%（有 allocator 时）或 25%（无 allocator 时）
- `ReservationOpResourceAllocator` 使用预留制：每个 op 至少保证 `reservation_ratio=50%` 的平均份额

### 2.9 GIL 与线程安全

#### 2.9.1 GIL 影响

StreamingExecutor 虽然运行在独立线程，但受 CPython GIL 约束：
- **纯 Python 计算**（schema 解析、operator 遍历、ranker 排序）会持有 GIL
- **IO 操作**（`ray.wait`、`ray.get`、网络 RPC）会释放 GIL
- 因此调度线程和用户主线程**不会真正并行执行 Python 代码**

但这在实际中不是问题，因为：
1. 用户主线程大部分时间在 `output_queue.pop()` 后 `time.sleep(0.01)` 循环等待（主动释放 GIL）
2. 调度线程的 IO 操作（ray.wait）期间释放 GIL，用户线程可以处理拿到的数据
3. 性能瓶颈不在 GIL 竞争，而在**调度线程自身的纯 Python 计算耗时过长**

#### 2.9.2 线程安全保证

| 共享数据 | 线程安全机制 | 说明 |
|----------|-------------|------|
| `OpState.output_queue` | Python list（GIL 保护的 append/pop）| 调度线程 append，用户线程 pop |
| `_shutdown` | bool 赋值是原子的（GIL） | 用户线程写，调度线程读 |
| `_shutdown_lock` | `threading.RLock` | shutdown 操作的互斥 |
| `Topology` | 只有调度线程写入 | 用户线程只通过 output_queue 间接读 |

### 2.10 Prometheus 监控指标

调度相关的 Prometheus 指标，可通过 Ray Dashboard 或 Prometheus endpoint 获取：

#### 2.10.1 调度循环指标

| 指标名 | 类型 | 说明 |
|--------|------|------|
| `data_sched_loop_duration_s` | Gauge | 当前调度循环耗时（per dataset） |
| `streaming_exec_schedule_s` | Timer (内部) | 调度总时间 / 最大值 / 平均值 |

#### 2.10.2 Per-operator 资源指标

| 指标名 | 类型 | 说明 |
|--------|------|------|
| `data_cpu_budget` | Gauge | 分配给 op 的 CPU 预算 |
| `data_gpu_budget` | Gauge | 分配给 op 的 GPU 预算 |
| `data_memory_budget` | Gauge | 分配给 op 的内存预算 |
| `data_object_store_memory_budget` | Gauge | 分配给 op 的 object store 预算 |
| `data_max_bytes_to_read` | Gauge | streaming generator buffer 可读上限 |

#### 2.10.3 Per-operator 运行时指标

| 指标名 | 类型 | 说明 |
|--------|------|------|
| `num_tasks_running` | Gauge | 当前运行中的 task 数 |
| `task_submission_backpressure_time` | Counter | 因 backpressure 等待的累计时间 |
| `task_output_backpressure_time` | Counter | output 背压累计时间 |
| `block_generation_time` | Histogram | block 生成耗时分布 |
| `task_completion_time` | Histogram | task 完成耗时分布 |
| `block_size_bytes` | Histogram | block 大小分布 |

#### 2.10.4 如何配合 Grafana 监控调度瓶颈

```promql
# 调度循环耗时 > 5s 的告警
data_sched_loop_duration_s{dataset=~".*"} > 5

# 某个 op 持续被 backpressure 阻塞
rate(task_submission_backpressure_time{operator=~".*"}[5m]) > 0.5

# GPU 利用率低（可能是调度延迟导致）
ray_node_gpus_utilization < 0.7
```

---

## 三、瓶颈定位与 Profiling 方法

### 3.1 Profiling 方法论

社区使用了**三层递进**的 profiling 方法定位瓶颈：

```
Step 1: 内置 Timer 指标 → 宏观定位："调度循环整体有多慢"
Step 2: py-spy 采样分析 → 微观定位："慢在哪个函数"
Step 3: 代码路径分析   → 根因定位："为什么这个函数慢"
Step 4: Release Test   → 量化验证："修复后快了多少"
```

#### 3.1.1 内置 Timer 指标（宏观定位）

`StreamingExecutor` 在每次 `_scheduling_loop_step` 迭代前后用 `time.perf_counter()` 计时：

```python
# streaming_executor.py:456-467
while True:
    # 用 perf_counter 而非 process_time，以包含 IO/RPC 等待时间
    t_start = time.perf_counter()
    continue_sched = self._scheduling_loop_step(self._topology)
    sched_loop_duration = time.perf_counter() - t_start

    # 记录到 Timer（自动追踪 total/min/max/count）
    self._initial_stats.streaming_exec_schedule_s.add(sched_loop_duration)
```

`Timer` 类（`stats.py:156-191`）是 O(1) 空间的统计器：

```python
class Timer:
    def __init__(self):
        self._total: float = 0
        self._min: float = float("inf")
        self._max: float = 0          # ← 关键：单次迭代最大耗时
        self._total_count: float = 0

    def add(self, value: float):
        self._total += value
        if value > self._max:
            self._max = value
        self._total_count += 1
```

执行结束后通过 `ds.stats()` 读取：

```python
stats = ds.stats()
# stats.streaming_exec_schedule_s     → Timer.get()  总时间
# stats.streaming_exec_schedule_max_s → Timer.max()  单次最大值（PR #63345 新增）
# stats.streaming_exec_schedule_avg_s → Timer.avg()  平均值（PR #63345 新增）
```

**作用**: 能量化"调度循环有多慢"，但不知道慢在哪个子函数。

#### 3.1.2 py-spy 采样分析（微观定位）

[py-spy](https://github.com/benfred/py-spy) 是无侵入的 Python 采样 profiler，可附着到运行中的进程。

**如何识别调度线程**:

`StreamingExecutor` 继承 `threading.Thread`，线程名在初始化时设置：

```python
# streaming_executor.py:80,144-145
class StreamingExecutor(Executor, threading.Thread):
    def __init__(self, ...):
        thread_name = f"StreamingExecutor-{self._dataset_id}"
        threading.Thread.__init__(self, daemon=True, name=thread_name)
```

线程名格式为 `StreamingExecutor-<dataset_id>`（如 `StreamingExecutor-dataset_abc123`）。

**py-spy 操作步骤**:

```bash
# 1. 找到 Ray driver 进程 PID
ps aux | grep "python.*your_script.py"
# 或者通过 ray status 找到 driver node 上的 python 进程

# 2. 附着 py-spy，生成 flamegraph（SVG 格式）
sudo py-spy record \
    --pid <driver_pid> \
    --threads \              # 按线程分离采样数据
    --output profile.svg \
    --duration 60            # 采样 60 秒

# 3. 或者生成 top-like 文本输出
sudo py-spy top --pid <driver_pid>

# 4. 如果只想看特定线程，可以在 flamegraph 中过滤
# 在 SVG 中搜索 "StreamingExecutor" 即可找到调度线程的调用栈
```

**关键参数**:
- `--threads`: 区分不同线程的采样数据，**必须加**，否则所有线程混在一起
- `--native`: 可选，同时采样 C/C++ 调用栈（如 `pa.ipc.read_schema` 内部的 Arrow C++ 代码）
- 默认采样频率 100Hz（每 10ms 采一次），足以定位瓶颈

**如何在 flamegraph 中找到调度线程**:

py-spy 的 `--threads` 模式会为每个线程生成独立的火焰图段，线程名标注在顶部。寻找名为 `StreamingExecutor-*` 的线程段即可。在该段中：

- **"self" 时间**: 函数自身消耗的 CPU 时间（不含子函数调用）
- **"inclusive" 时间**: 函数及其所有子函数的总 CPU 时间
- 火焰图中最宽的底部条就是耗时最大的叶子函数

**社区的具体 profiling 条件**:

| 参数 | 值 | 说明 |
|------|-----|------|
| actors 数量 | 1000 | 足够大以暴露线性增长问题 |
| schema 列数 | 600 | 100 scalar float32 + 200 `fixed_size_list[64]` + 300 `fixed_size_list[32]` |
| extension types | `ArrowTensorType` | 增加 `pa.ipc.read_schema` 的单次解析开销 |
| block size | 16 MiB | 固定大小，确保 worker 内存恒定 |
| 采样时长 | ~158s (total scheduler thread) | 足够收集统计显著的数据 |

**py-spy 输出的关键结论**:

```
Thread: StreamingExecutor-dataset_xxx (daemon)

Top functions by self time:
  95.01s (60.2%)  pa.ipc.read_schema        ← 叶子函数，纯 CPU 计算
   7.2s  ( 4.6%)  ray.wait                  ← IO 等待
   5.1s  ( 3.2%)  select_operator_to_run    ← 遍历 ops
   ...

Top functions by inclusive time:
 137.5s (87.1%)  _scheduling_loop_step      ← 调度循环总时间
 130.5s (82.7%)  process_completed_tasks    ← completion 处理阶段
  95.0s (60.2%)  BlockMetadataWithSchema.__setstate__  ← schema 反序列化
```

#### 3.1.3 Wide-schema Release Test（可复现基准）

PR #63420 建立了标准化的 release test，用于量化验证：

```python
# release/nightly_tests/dataset/worker_scaling_benchmark.py

class RealisticSchemaUDF:
    """产出宽 schema blocks，测试目标是 driver 端 schema 传播路径"""

    def __init__(self, num_scalar_cols=100, num_array_cols=200):
        # 预生成 template 数据，UDF 本身开销极低
        # 测试目标是 driver 的 on_data_ready → ray.get(meta_ref) → 反序列化
        self.template = pre_roll_data(...)

    def __call__(self, batch):
        # block size 限 16MiB，worker 内存恒定
        return expand_to_wide_schema(batch, self.template)
```

测试矩阵（8 组）:

```yaml
matrix:
  num_workers: [500, 1000, 2000, 5000]
  worker_type: [actors, tasks]
```

每次 run 在 Buildkite CI 上执行，结果自动上报 `max_scheduling_loop_duration_s`。

#### 3.1.4 完整 Profiling 工作流复现

```bash
# === Step 1: 运行 wide-schema workload ===
python worker_scaling_benchmark.py \
    --num-workers 1000 \
    --worker-type actors \
    --num-scalar-cols 100 \
    --num-array-cols 200

# === Step 2: 在另一个终端附着 py-spy ===
# 找到 driver PID
DRIVER_PID=$(pgrep -f "worker_scaling_benchmark")

# 生成火焰图
sudo py-spy record \
    --pid $DRIVER_PID \
    --threads \
    --output scheduling_profile.svg \
    --duration 60

# === Step 3: 分析火焰图 ===
# 在浏览器中打开 scheduling_profile.svg
# 找到 "StreamingExecutor-" 线程段
# 观察最宽的底部条（self 时间最大的函数）

# === Step 4: 读取内置指标验证 ===
# 在 workload 脚本末尾添加:
stats = ds.stats()
print(f"max_scheduling_loop: {stats.streaming_exec_schedule_max_s:.2f}s")
print(f"avg_scheduling_loop: {stats.streaming_exec_schedule_avg_s:.4f}s")
print(f"total_scheduling_time: {stats.streaming_exec_schedule_s:.2f}s")
```

### 3.2 Profiling 结果：各阶段耗时分解

基于社区 py-spy profiling（1000 actors, 600 列 schema, 采样 ~158s）:

| 阶段 | 函数 | self / inclusive | 占调度线程比例 |
|------|------|-----------------|---------------|
| **schema 反序列化** | `BlockMetadataWithSchema.__setstate__` → `pa.ipc.read_schema` | 95.01s self | **60.2%** |
| completion 总计 | `process_completed_tasks` | 130.5s inclusive | 82.7% |
| dispatch 循环 | `select_operator_to_run` × N | ~20s inclusive | ~12% |
| housekeeping | autoscaling + state refresh | ~8s inclusive | ~5% |
| **total** | `_scheduling_loop_step` | 137.5s inclusive | 87.1% |

> **注**: "self" = 函数自身 CPU 时间（不含子调用）; "inclusive" = 含所有子函数

### 3.3 瓶颈热力图

```
_scheduling_loop_step 耗时分布:

  ┌─────────────────────────────────────────────────────────────┐
  │ process_completed_tasks                                      │
  │ ┌─────────────────────────────────────────────────────────┐ │
  │ │ ray.wait │ on_data_ready: schema反序列化 (60%)  │ other │ │
  │ │  (idle)  │ ████████████████████████████████████ │       │ │
  │ └─────────────────────────────────────────────────────────┘ │
  │ dispatch: select_op × N (12%)  │ housekeeping (5%) │ other  │
  │ ████████████                   │ ███               │        │
  └─────────────────────────────────────────────────────────────┘
```

### 3.4 核心瓶颈总结

| 瓶颈 | 代码位置 | 根因 | 复杂度 |
|------|----------|------|--------|
| schema 重复解析 | `block.py` `__setstate__` | 同一 op 所有 block 带相同 schema，每次都解析 | O(blocks × schema_complexity) |
| `select_operator_to_run` 冗余调用 | `streaming_executor.py:559-570` | 每 dispatch 1 个 task 就重新遍历全部 op | O(dispatches × ops × policies) |
| `resource_manager.update_usages` 频繁调用 | `streaming_executor.py:574` | 每次 dispatch 后立即更新，含全量 allocator 重建 | O(dispatches × ops) |
| `ray.wait timeout` 固定 | `streaming_executor_state.py:449` | 有 pending work 时也等 100ms | 固定 100ms 延迟 |
| completion 一次性全处理 | `streaming_executor_state.py:467-476` | 16K tasks 完成时阻塞数秒 | O(ready_tasks × deser_cost) |
| `refresh_actor_state` 全量扫描 | `actor_pool_map_operator.py:930-933` | 每轮遍历所有 running actors | O(actors) per loop |

---

## 四、Schema 反序列化：占 60% 的首要瓶颈

### 4.1 完整数据流路径

```
Worker 端 (_map_task)                          Driver 端 (调度线程)
─────────────────────────                      ────────────────────────────────────
UDF(block) → 产出 output block                 _scheduling_loop_step()
  │                                              │
  ├─ block_ref = yield block                     ├─ ray.wait([waitables], timeout=0.1)
  │   (block 存入 object store)                  │   发现有 task 输出 ready
  │                                              │
  ├─ meta = BlockMetadataWithSchema(             ├─ on_data_ready(max_bytes_to_read):
  │     num_rows=...,                            │   │
  │     size_bytes=...,                          │   ├─ block_ref = gen._next_sync(timeout=0)
  │     schema=pa_schema,  ← Arrow Schema        │   │
  │   )                                          │   ├─ meta_ref = gen._next_sync(timeout=0.1)
  │                                              │   │
  ├─ meta_ref = yield meta  ← 序列化!            │   ├─ meta = ray.get(meta_ref, timeout=1.0)
  │   序列化路径:                                 │   │   反序列化路径:
  │   cloudpickle.dumps(meta)                    │   │   cloudpickle.loads(bytes)
  │     → __getstate__                           │   │     → __setstate__
  │       → pa.ipc.write_schema(schema)          │   │       → pa.ipc.read_schema(ipc_bytes)
  │       → schema_bytes (IPC format)            │   │       → pa.Schema object
  │                                              │   │
  │                                              │   └─ output_ready_callback(RefBundle)
  │                                              │
  └─ (继续处理下一个 block)                       └─ select_operator_to_run → dispatch
```

### 4.2 为什么 Arrow Schema IPC 反序列化慢

Arrow Schema 序列化格式为 [IPC Message](https://arrow.apache.org/docs/format/Columnar.html#ipc-message-format)，反序列化需要：
1. 解析 flatbuffer 头部
2. 逐字段重建 `pa.Field` 对象
3. 对 extension types（如 `ArrowTensorType`），需要额外解析 metadata + 注册

**耗时与 `列数 × 类型复杂度` 成正比**:

```python
# 简单 schema (10 列, int64/float64):
#   pa.ipc.read_schema 耗时 ≈ 0.01ms

# 宽 schema (600 列, 含 100 scalar + 200 fixed[64] + 300 fixed[32]):
#   pa.ipc.read_schema 耗时 ≈ 0.5-1.0ms
```

### 4.3 冗余放大效应

**同一 operator 的所有 task 产出的 schema 完全相同**，但每个 block 的 metadata 都携带完整 schema:

```
1000 actors × 每 actor 产出 10 blocks = 10,000 次 pa.ipc.read_schema
每次 0.5ms → 总计 5,000ms = 5 秒
```

这就是 profiling 数据中 60.2% 的来源。

### 4.4 三个 Schema PR 的递进关系

```
                       序列化次数                    单次耗时
                    ─────────────────          ────────────────────
现状:               blocks × tasks              cloudpickle + IPC
                         │                            │
#62720 (减量):      tasks (首 block 才带)              │
                         │                            │
#62726 (加速):           │                     pickle (比 cloudpickle 快 25%)
                         │                            │
#63462 (去重):      unique schemas (≈1)         LRU cache → 只解析 1 次
```

| PR | 优化维度 | 效果 | 核心改动 |
|----|----------|------|----------|
| **#62720** | 减少传输次数 | `blocks_per_task × N_tasks` → `N_tasks` | `_map_task` 只在第一个 block 的 metadata 中附带 schema |
| **#62726** | 加速反序列化 | cloudpickle → 标准 pickle，~25% 提速 | worker 端 `pickle.dumps(meta)` 后 yield bytes |
| **#63462** | 消除重复解析 | N 次相同 bytes 解析 → 1 次 | `@functools.lru_cache(maxsize=64)` |

### 4.5 PR #63462 的 Benchmark 数据

测试环境: 600 列宽 schema，wide-schema release test

**优化前:**

| Workers | 500 | 1000 | 2000 | 5000 |
|---------|-----|------|------|------|
| Actor mode (s) | 5.84 | 12.28 | 25.24 | 67.98 |
| Task mode (s) | 3.10 | 6.35 | 11.22 | 19.56 |

**优化后:**

| Workers | 500 | 1000 | 2000 | 5000 |
|---------|-----|------|------|------|
| Actor mode (s) | 2.42 | 5.07 | 10.86 | 26.93 |
| Task mode (s) | 1.42 | 2.80 | 4.62 | 10.68 |

**所有规模下约 2x 加速**。

---

## 五、内部 commit 6963b2d4 分析

### 5.1 优化内容

标题: `[Data] GPU-optimized interleaved dispatch for scheduling loop`
作者: zhangfuxing
日期: 2026-04-13
改动: `streaming_executor.py` (+268/-52 行)

核心优化 4 项：

**1. GPU-first 优先处理**

将 ready tasks 分为 `gpu_batches` 和 `cpu_batches`，GPU batch 先处理。通过 `_op_uses_gpu()` 判断（结果缓存在模块级 dict）。

**2. 交错调度（Interleaved Dispatch）**

```
原版:  process ALL completions ──────────────── → dispatch ALL
优化:  process 512 → dispatch → process 512 → dispatch → ...
```

当有 16K+ ready tasks 时，原版第 1 步可能耗时数百秒，期间 actor 完全空闲。

**3. 批量 update_usages**

`SCHED_DISPATCH_UPDATE_INTERVAL=64`，每 64 次 dispatch 才更新一次资源视图。

**4. 调度 Profiling**

`SCHED_PROFILE_THRESHOLD_S=5.0`，超阈值输出 `[SCHED_PROFILE]` 日志。

### 5.2 实测效果

| 指标 | 优化前 | 优化后 |
|------|--------|--------|
| 首次爆发调度步（16K+ ready tasks） | 452s | 90s |
| 稳态调度步 | 200-400s | 12-22s |
| GPU actor dispatch 延迟 | 分钟级 | 即时 |

### 5.3 实现问题

| 问题 | 描述 | 影响 |
|------|------|------|
| **`_gpu_op_cache` 内存泄漏** | 用 `id(op)` 做 key 的模块级 dict，op 被 GC 后 entry 不清理；新对象可能复用地址导致错误判定 | 多次执行 pipeline 时 cache 增长 + 错误判定 |
| **绕过 `process_completed_tasks`** | 内联重写 120 行 completion 处理逻辑，两份逻辑并存 | 后续 metrics/error handling 修改需双重维护 |
| **`ray.wait` 仍固定 0.1s** | 有大量 pending dispatch 时仍等 100ms | 未完全消除无效等待 |
| **`select_operator_to_run` 未优化** | 每个 task 仍逐个调用 | dispatch 阶段开销未降低 |
| **函数膨胀** | 50 行 → 225+ 行单函数 | 难以测试和维护 |
| **`SCHED_MAX_DISPATCHES_PER_ROUND=512` 硬上限** | 大集群初始阶段需填充上千 slot | 人为限制了 dispatch 吞吐 |

### 5.4 GPU-first 评估

**结论: 实现位置放错了。**

GPU-first 实际做的是 **completion 处理优先**，不是 **dispatch 优先**：

```python
# 6963b2d4 的实现:
gpu_batches = [tasks for op that uses GPU]
cpu_batches = [tasks for op that uses CPU]

for state, batch in gpu_batches + cpu_batches:  # GPU 先处理
    for task in batch:
        task.on_data_ready(...)  # 处理 completion, 拉取 output
    # 然后 dispatch
```

处理 GPU completion 只是把 GPU operator 的 output 拉到**下游**队列里。但后面 `select_operator_to_run` 决定 dispatch 给谁时仍按 `DefaultRanker` 排序（throttling_disabled, obj_store_mem），不考虑 GPU。

**正确做法**: 在 dispatch 阶段的 ranker 中体现 GPU 优先：

```python
class GPUAwareRanker(DefaultRanker):
    def rank_operator(self, op, topology, resource_manager):
        gpu_priority = 0 if op.incremental_resource_usage().gpu > 0 else 1
        throttling_disabled = 0 if op.throttling_disabled() else 1
        obj_store_mem = resource_manager.get_op_usage(op).object_store_memory
        return (gpu_priority, throttling_disabled, obj_store_mem)
```

---

## 六、社区 Issue #63544 Related PRs 状态

### 6.1 release-2.55.1 上已有（6 个）

| PR | 标题 | 类别 | 核心改动 |
|----|------|------|----------|
| #56390 | Fix metrics query for iteration + scheduling loop | Instrumentation | 修复 metrics 采集 |
| #53454 | Remove Schema from BlockMetadata | Schema 优化 | schema 从 per-block metadata 移到 operator 级别 |
| #61288 | Prevent aggregator head-node scheduling | Dispatch | 避免在 head node 上调度 aggregator tasks |
| #61996 | Cache common _map_task args | Dispatch | 缓存 _map_task 的公共参数避免重复序列化 |
| #62114 | Heap-based actor ranking | Dispatch | actor 选择从 O(N) 线性扫描改为 heap O(log N) |
| #57971 | Remove stats-update thread | Other | 消除独立的 stats 更新线程，减少线程竞争 |

### 6.2 需要 cherry-pick（12 个，全部已在社区 master MERGED）

#### P0: 性能关键（3 个 Schema + 1 Dispatch）

| PR | 标题 | Merge Commit | 修改文件 | 改动量 |
|----|------|-------------|----------|--------|
| **#63462** | LRU-cache deserialized Arrow schemas | `6ecde5eda9bd` | `block.py` (+18/-2), test (+150) | **单项收益最大: 2x** |
| **#62720** | Yield only first schema in _map_task | `ab9a7d7d2328` | `map_operator.py` (+9/-2), `streaming_executor_state.py` (+20/-18) | 减少 schema 传输 |
| **#62726** | Regular pickle for task return | `81bda34d1f10` | `physical_operator.py`, `map_operator.py`, `hash_shuffle.py`, `block.py`, test | pickle 替代 cloudpickle |
| **#62309** | Rank actors per node in a heap | `1de3b19effd2` | `actor_pool_map_operator.py` (+43/-49) | 按节点分组 heap |

#### P1: 有价值的改进

| PR | 标题 | Merge Commit | 改动量 |
|----|------|-------------|--------|
| #62891 | Final bundle clean-up | `2eb5ec059c5a` | 8 files (+106/-176) 整合 queue 清理 |
| #63345 | Add max_scheduling_loop_duration_s metric | `944df887fbac` | 3 files (+21/-6) 关键监控指标 |

#### P2: Instrumentation / Test Infra

| PR | 标题 | Merge Commit |
|----|------|-------------|
| #62217 | Operator start/stop metrics | `554995b3269b` |
| #62249 | Task block locality metric | `20744ecca90e` |
| #62372 | Runtime-env setup time in release tests | `55fc62d8e9c2` |
| #62436 | Raylet scheduling overhead in release tests | `83dc39a34682` |
| #62453 | Use get_stats_summary() in release tests | `ea64748e8b6f` |
| #63420 | Wide-schema release tests | `d03f6d...` |

### 6.3 Cherry-pick 依赖关系图

```
#53454 (已有)        Remove Schema from BlockMetadata
      │
      ├──→ #62720 (需 pick)  每个 task 只在第一个 block 附带 schema
      │         │
      │         └──→ #62726 (需 pick)  pickle 替代 cloudpickle (依赖 #62720 的改动格式)
      │
      └──→ #63462 (需 pick)  LRU cache schema bytes (独立, 不依赖 #62720)

#62114 (已有)        Heap-based actor ranking
      │
      └──→ #62309 (需 pick)  Per-node heap actor ranking (在 #62114 基础上改进)

#62891 (需 pick)     Final bundle clean-up (独立)
#63345 (需 pick)     max_scheduling_loop metric (独立)
```

**推荐 pick 顺序**: `#63462` → `#62720` → `#62726` → `#62309` → `#62891` → `#63345`

(`#63462` 独立且收益最大，优先 pick)

### 6.4 各 PR 详细说明

#### PR #63462: LRU-cache deserialized Arrow schemas

**问题**: `BlockMetadataWithSchema.__setstate__` 在反序列化时调用 `pa.ipc.read_schema()`。同一 operator 的所有 task 携带**完全相同的 schema bytes**，但每次都重新解析。

**改动** (`block.py`):

```python
import functools

@functools.lru_cache(maxsize=64)
def _cached_read_schema(schema_bytes: bytes) -> pa.Schema:
    """Cache deserialized Arrow schemas by their IPC bytes.

    Since all tasks of the same operator produce identical schemas,
    this collapses thousands of identical re-parses into one.
    """
    return pa.ipc.read_schema(pa.BufferReader(schema_bytes))


class BlockMetadataWithSchema:
    def __setstate__(self, state):
        # ... existing deserialization ...
        if schema_bytes is not None:
            schema = _cached_read_schema(schema_bytes)  # 替代 pa.ipc.read_schema(...)
        # ...
```

**为什么 `maxsize=64`**: 一个 pipeline 通常不超过 64 个不同 schema 的 operator。LRU 保证内存有限。

#### PR #62720: Yield only first schema in _map_task

**问题**: 每个 block 的 metadata 都带完整 schema，但同一 task 内所有 block 的 schema 一定相同。

**改动** (`map_operator.py`):

```python
def _map_task(fn, input_blocks, ...):
    first_block = True
    for block in fn(input_blocks):
        meta = BlockMetadataWithSchema.from_block(block)
        if not first_block:
            # 后续 block 不带 schema，driver 端会用第一个 block 的 schema
            meta = BlockMetadataWithSchema(
                num_rows=meta.num_rows,
                size_bytes=meta.size_bytes,
                exec_stats=meta.exec_stats,
                schema=None,  # ← 关键: 不带 schema
            )
        first_block = False
        yield block
        yield meta
```

**改动** (`streaming_executor_state.py`): 在 `process_completed_tasks` 中，如果 metadata 的 schema 为 None，则使用上一个非 None 的 schema。

#### PR #62726: Regular pickle for task return

**问题**: Ray 默认用 `cloudpickle` 序列化/反序列化对象。cloudpickle 处理闭包和嵌套函数，比标准 pickle 慢。BlockMetadataWithSchema 是简单 dataclass，不需要 cloudpickle 的能力。

**改动**:
- Worker 端: `metadata_bytes = pickle.dumps(metadata)` 后 yield bytes
- Driver 端: `pickle.loads(metadata_bytes)` 获取 metadata
- 绕过 Ray 的默认 cloudpickle 路径

#### PR #62309: Rank actors per node in a heap

**问题**: 选择最优 actor 时，需遍历 preferred node 上所有 actors O(M)。

**改动**: 将 `_alive_node_to_actor_map` 从 `Set[ActorHandle]` 改为 per-node heap，选择 actor 变为 heap peek O(1) + heap push O(log M)。

---

## 七、综合优化方案

### Layer 1: Cherry-pick 社区已验证优化（P0，低风险高收益）

直接将 release-2.55.1 缺失的 PR backport。这些都已在社区 master 经过完整 CI 验证。

| 项目 | 修改文件 | 预期收益 |
|------|----------|----------|
| **1.1 LRU-cache schema (#63462)** | `python/ray/data/block.py` | max_scheduling_loop 降低 55-60% |
| **1.2 首 block yield schema (#62720)** | `operators/map_operator.py`, `streaming_executor_state.py` | 反序列化次数减少 10x |
| **1.3 pickle 替代 cloudpickle (#62726)** | `physical_operator.py`, `map_operator.py`, `block.py` | 单次反序列化快 25% |
| **1.4 Per-node actor heap (#62309)** | `operators/actor_pool_map_operator.py` | actor 选择 O(N*M) → O(N*logM) |
| **1.5 Instrumentation (#63345)** | `streaming_executor.py`, `stats.py` | 监控能力（非性能） |

### Layer 2: 调度循环结构性优化（P1，中风险核心改动）

汲取 6963b2d4 的交错调度思路，但在 release-2.55.1 的代码结构上**增量修改**，不绕过 `process_completed_tasks`。

| 项目 | 修改文件 | 预期收益 |
|------|----------|----------|
| **2.1 动态 ray.wait timeout** | `streaming_executor.py`, `streaming_executor_state.py` | 消除 100ms 无效等待 |
| **2.2 批 Dispatch per Operator** | `streaming_executor.py` | dispatch 阶段 select 调用减少 95%+ |
| **2.3 Completion 处理上限** | `streaming_executor_state.py` | 首次爆发场景交错 dispatch |
| **2.4 Profiling 基础设施** | `streaming_executor.py` | 生产排障能力 |

### Layer 3: 大规模场景专项优化（P2，按需启用）

通过环境变量/feature flag 控制，默认不启用。

| 项目 | 修改文件 | 适用场景 |
|------|----------|----------|
| **3.1 GPU-aware Ranker** | `ranker.py` | GPU/CPU 混合 pipeline |
| **3.2 Actor 状态刷新降频** | `actor_pool_map_operator.py` | 4w+ actors |
| **3.3 select 快速路径** | `streaming_executor_state.py` | 10+ 算子 pipeline |

### Layer 4: ResourceManager update_usages 增量更新优化（P1，中风险核心改动）

解决 `update_usages()` 在调度循环中被过度调用的根本问题。详见第八节 8.8-8.11。

| 项目 | 修改文件 | 预期收益 |
|------|----------|----------|
| **4.1 增量更新 update_usages_for_ops** | `resource_manager.py` | dispatch 循环内 O(K) → O(1) |
| **4.2 与 capacity-based dispatch 组合** | `streaming_executor.py` | 每 op-batch 增量更新，兼顾正确性 |
| **4.3 Allocator reservation 缓存** | `resource_manager.py` | 避免同一迭代内幂等 reservation 重建 |

---

## 八、实施细节与代码示例

### 8.1 Layer 2.1: 动态 ray.wait timeout

**原理**: 当有 pending bundles 等待 dispatch 时，`ray.wait` 应立即返回（timeout=0），让 dispatch 循环尽快开始。

**修改 `streaming_executor.py`** `_scheduling_loop_step`:

```python
def _scheduling_loop_step(self, topology: Topology) -> bool:
    # === 新增: 判断是否有 pending work ===
    has_pending_work = any(
        state.has_pending_bundles() and op.can_add_input()
        for op, state in topology.items()
        if not op.has_completed()
    )

    self._resource_manager.update_usages()

    # === 修改: 传递动态 timeout ===
    errored_blocks_per_op = process_completed_tasks(
        topology,
        self._backpressure_policies,
        self._max_errored_blocks,
        timeout=0.0 if has_pending_work else 0.1,  # ← 新参数
    )
    # ... 后续不变 ...
```

**修改 `streaming_executor_state.py`** `process_completed_tasks`:

```python
def process_completed_tasks(
    topology: Topology,
    backpressure_policies: List[BackpressurePolicy],
    max_errored_blocks: int,
    timeout: float = 0.1,  # ← 新参数，默认保持向后兼容
) -> Dict["OpState", int]:
    # ...
    if active_tasks:
        ready, _ = ray.wait(
            list(active_tasks.keys()),
            num_returns=len(active_tasks),
            fetch_local=False,
            timeout=timeout,  # ← 使用传入的 timeout
        )
    # ...
```

**收益**: 有 pending work 时循环频率从 ~10Hz 提升到仅受 process_completed_tasks 处理耗时限制（~100Hz+）。无 pending work 时回退到 0.1s 等待，避免 CPU 空转。

### 8.2 Layer 2.2: 批 Dispatch per Operator

**原理**: 选中一个 operator 后，一次性 dispatch 多个 task（直到该 op 不再 eligible），然后再调用 `select_operator_to_run` 选下一个。

**修改 `streaming_executor.py`** dispatch 循环:

```python
_BATCH_RESOURCE_CHECK_INTERVAL = 16  # 每 16 个 task 检查一次

def _scheduling_loop_step(self, topology: Topology) -> bool:
    # ... process_completed_tasks ...

    self._resource_manager.update_usages()
    self._report_current_usage()

    i = 0
    while True:
        op = select_operator_to_run(
            topology,
            self._resource_manager,
            self._backpressure_policies,
            ensure_liveness=self._consumer_idling(),
            ranker=self._ranker,
        )

        if op is None:
            break

        # === 新增: 批量 dispatch 同一个 op ===
        op_state = topology[op]
        batch_count = 0
        while op_state.has_pending_bundles() and op.can_add_input():
            op_state.dispatch_next_task()
            batch_count += 1
            i += 1

            # 定期检查资源和 backpressure
            if batch_count % _BATCH_RESOURCE_CHECK_INTERVAL == 0:
                self._resource_manager.update_usages()
                if not all(
                    p.can_add_input(op) for p in self._backpressure_policies
                ):
                    break

            if i % self._progress_manager.TOTAL_PROGRESS_REFRESH_EVERY_N_STEPS == 0:
                self._refresh_progress_manager(topology)

        # 批结束后统一更新资源
        self._resource_manager.update_usages()

    # ... autoscaling, state updates ...
```

**收益** (dispatch 100 tasks 到同一 op):

| 指标 | 社区版 (逐个) | 批 dispatch |
|------|--------------|------------|
| `select_operator_to_run` 调用 | 101 次 | 1-2 次 |
| `resource_manager.update_usages` 调用 | 100 次 | 6-7 次 |
| dispatch 阶段耗时 | ~170ms | ~52ms |
| 有效吞吐 | ~600 tasks/s | ~2000 tasks/s |

**Liveness 保证**: 由于 `_BATCH_RESOURCE_CHECK_INTERVAL=16` 保证不会过度 dispatch 超出 backpressure 限制。且外层 while True 会重新 select，保证多 operator 场景的公平性。

### 8.3 Layer 2.3: Completion 处理上限

**原理**: 限制单轮 `process_completed_tasks` 处理的 task 数量，配合动态 timeout=0，实现等效的"交错调度"效果。

**修改 `streaming_executor_state.py`**:

```python
_MAX_COMPLETIONS_PER_STEP = 512  # 可通过环境变量覆盖

def process_completed_tasks(
    topology: Topology,
    backpressure_policies: List[BackpressurePolicy],
    max_errored_blocks: int,
    timeout: float = 0.1,
    max_completions: int = _MAX_COMPLETIONS_PER_STEP,
) -> Dict["OpState", int]:
    # ... ray.wait, group by op ...

    completed_count = 0
    for state, ready_tasks in ready_tasks_by_op.items():
        ready_tasks = sorted(ready_tasks, key=lambda t: t.task_index())
        for task in ready_tasks:
            if completed_count >= max_completions:
                # 达到上限，剩余留到下一轮处理
                # 配合 timeout=0，下一轮会立即开始
                break
            if isinstance(task, DataOpTask):
                bytes_read = task.on_data_ready(
                    remaining_output_budget.get(state, None)
                )
                # ...
            completed_count += 1
        else:
            continue
        break  # 内层 break 时外层也 break

    # ...
```

**效果**: 首次爆发（16K tasks 同时 ready）时:
- 处理 512 个 completion (~1s) → dispatch → 处理 512 个 → dispatch → ...
- Actor 不再空等全部 16K 处理完才能拿到新任务
- 等效于 6963b2d4 的"交错调度"，但不需要重写 `process_completed_tasks`

### 8.4 Layer 2.4: Profiling 基础设施

```python
import os

_SCHED_PROFILE_THRESHOLD_S = float(
    os.environ.get("RAY_DATA_SCHED_PROFILE_THRESHOLD_S", "5.0")
)

def _scheduling_loop_step(self, topology: Topology) -> bool:
    t_step_start = time.perf_counter()

    # Phase 1: process completed
    t0 = time.perf_counter()
    # ... process_completed_tasks ...
    t_process = time.perf_counter() - t0
    n_completions = sum(...)  # 本轮处理的 completion 数

    # Phase 2: dispatch
    t0 = time.perf_counter()
    n_dispatched = 0
    # ... dispatch loop ...
    t_dispatch = time.perf_counter() - t0

    # Phase 3: autoscaling
    t0 = time.perf_counter()
    # ... autoscaling ...
    t_autoscale = time.perf_counter() - t0

    # Profile output
    t_total = time.perf_counter() - t_step_start
    if t_total > _SCHED_PROFILE_THRESHOLD_S:
        logger.warning(
            "[SCHED_PROFILE] total=%.2fs "
            "process_completed=%.2fs dispatch=%.2fs autoscale=%.2fs "
            "| completions=%d dispatched=%d pending_ops=%d",
            t_total, t_process, t_dispatch, t_autoscale,
            n_completions, n_dispatched,
            sum(1 for op, s in topology.items()
                if s.has_pending_bundles() and not op.has_completed()),
        )

    # ...
```

### 8.5 Layer 3.1: GPU-aware Ranker

**修改 `ranker.py`**:

```python
class GPUAwareRanker(DefaultRanker):
    """GPU operator 获得更高 dispatch 优先级。

    排序: (gpu_priority, throttling_disabled, obj_store_mem)
    GPU op: gpu_priority=0 (更高优先级)
    CPU op: gpu_priority=1

    通过 RAY_DATA_GPU_AWARE_SCHEDULING=1 启用。
    """
    def rank_operator(self, op, topology, resource_manager) -> Tuple[int, int, int]:
        gpu_priority = (
            0 if op.incremental_resource_usage().gpu > 0 else 1
        )
        throttling_disabled = 0 if op.throttling_disabled() else 1
        obj_store_mem = resource_manager.get_op_usage(op).object_store_memory
        return (gpu_priority, throttling_disabled, obj_store_mem)


def create_ranker() -> Ranker:
    if os.environ.get("RAY_DATA_GPU_AWARE_SCHEDULING", "0") == "1":
        return GPUAwareRanker()
    return DefaultRanker()
```

### 8.6 Layer 3.2: Actor 状态刷新降频

**修改 `actor_pool_map_operator.py`**:

```python
class _ActorPool(AutoscalingActorPool):
    _REFRESH_STATE_INTERVAL_S = float(
        os.environ.get("RAY_DATA_ACTOR_REFRESH_INTERVAL_S", "2.0")
    )

    def __init__(self, ...):
        # ... existing init ...
        self._last_state_refresh = 0.0

    def refresh_actor_state(self):
        """降频: 从每轮调度循环改为每 N 秒"""
        now = time.time()
        if now - self._last_state_refresh < self._REFRESH_STATE_INTERVAL_S:
            return
        self._last_state_refresh = now

        # 原有逻辑: 清除 node map, 遍历 running actors, 更新状态
        self._alive_node_to_actor_map.clear()
        for actor in list(self._running_actors.keys()):
            self._update_running_actor_state(actor)
```

### 8.7 Layer 3.3: select 快速路径（单 Operator Pipeline 优化）

**问题**: 很多 Ray Data pipeline 只有 1-2 个算子（如 `read → map`），但 `select_operator_to_run` 每次仍遍历所有 op 和所有 backpressure policy。

**原理**: 当 topology 只有一个非 InputDataBuffer 的算子时，可以跳过 ranker，直接返回该 op（如果 eligible）。

**修改 `streaming_executor_state.py`**:

```python
def select_operator_to_run(
    topology: Topology,
    resource_manager: "ResourceManager",
    backpressure_policies: List[BackpressurePolicy],
    *,
    ensure_liveness: bool,
    ranker: Ranker,
) -> Optional[PhysicalOperator]:
    # === 新增: 快速路径 ===
    # 对于简单 pipeline (≤2 non-completed ops)，跳过完整的 eligible 遍历
    active_ops = [op for op in topology if not op.has_completed()]
    if len(active_ops) == 1:
        op = active_ops[0]
        state = topology[op]
        if (
            state.has_pending_bundles()
            and op.can_add_input()
            and all(p.can_add_input(op) for p in backpressure_policies)
        ):
            return op
        return None

    # === 原有逻辑 ===
    eligible_ops = get_eligible_operators(
        topology, backpressure_policies, ensure_liveness=ensure_liveness
    )
    if not eligible_ops:
        return None
    return ranker.rank_operators(eligible_ops, topology, resource_manager)
```

**收益**: 简单 pipeline 的 `select_operator_to_run` 从遍历所有 op + 所有 policy 降为常量时间检查。对复杂 pipeline 无影响（fallback 到原有逻辑）。

### 8.8 Layer 4.1: 增量更新 — 社区 PR #63750 方案详解

**原理**: 每次 dispatch 后只重算受影响的算子（当前 op + 其上游 input_dependencies + sink），而非遍历全 topology。

**社区 PR**: [ray-project/ray#63750](https://github.com/ray-project/ray/pull/63750) `[data] Incrementally update resource usages after task dispatch`

#### 8.8.1 核心改动

**1. 抽取 `_recompute_op_usage(op)` — 单算子重算 + 增量更新全局**

原来 `update_usages()` 对每个算子算完直接加到全局总量。现在 `_recompute_op_usage` 先**减去旧值再加新值**：

```python
def _recompute_op_usage(self, op: "PhysicalOperator"):
    """Recompute a single operator's usage entries and apply the delta to
    the global totals."""
    state = self._topology[op]

    # 计算当前用量
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

    # 更新 per-op 缓存
    self._op_usages[op] = op_usage
    self._op_running_usages[op] = op_running_usage
    self._op_pending_usages[op] = op_pending_usage

    # 更新 Dashboard 指标
    op._metrics.obj_store_mem_used = op_usage.object_store_memory
```

**2. 新增 `update_usages_for_ops(ops)` — 只重算脏算子**

```python
def update_usages_for_ops(self, ops: Iterable["PhysicalOperator"]):
    """Incrementally update resource usages for the given operators.

    Recomputes only the usage of the operators that actually changed,
    applying the deltas to the global totals.

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
- **output_operator（sink）**：因为 `_external_consumer_bytes` 可被消费者线程随时更新

**3. `update_usages()` 重构为调用 `_recompute_op_usage`**

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
        self._recompute_op_usage(op)    # 复用同一方法

    if self._op_resource_allocator is not None:
        self._update_allocated_budgets()
```

**4. `_compute_completed_ops` 抽取**：将 `_get_completed_ops_usage` 中收集已完成+ineligible 算子的逻辑抽取为独立方法，便于后续 allocator reservation 缓存使用。

#### 8.8.2 PR #63750 的调用频率

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

#### 8.8.3 测试覆盖

PR #63750 新增 `test_incremental_usages_match_full_recompute` 测试：创建两个 ResourceManager 实例共享同一 topology。每次指标变更后，一个做全量 `update_usages()`，另一个做 `update_usages_for_ops([changed_op])`，断言所有 per-op 和全局用量完全一致。覆盖了 task 生命周期的各阶段：input queued → task submitted → task finished → output moved → external consumer bytes 变化。

### 8.9 Layer 4.2: 与 247c9c2d capacity-based dispatch 的组合优化

#### 8.9.1 当前代码结构（247c9c2d 之后）

247c9c2d 引入了 capacity-based dispatch，将 dispatch 循环改为按 op-batch 派发：

```python
# 当前代码（247c9c2d 之后）
def _dispatch_loop(self, topology: Topology) -> int:
    i = 0
    while True:
        op = select_operator_to_run(...)
        if op is None: break

        soft_capacity = self._compute_soft_capacity(op)  # 查询各 policy 的 capacity
        n = 0
        while (
            op_state.has_pending_bundles()
            and op.can_add_input()
            and n < soft_capacity
        ):
            op_state.dispatch_next_task()
            n += 1
            i += 1

        # 每 op-batch 后更新一次（全量）
        self._resource_manager.update_usages()    # ← O(K) 全量遍历

    return i
```

#### 8.9.2 组合优化：每 op-batch 后增量更新

将 `_dispatch_loop` 中的 `update_usages()` 替换为 `update_usages_for_ops([op])`：

```python
# 优化后
self._resource_manager.update_usages_for_ops([op])   # ← O(1) 增量
```

**效果对比**：

| 方案 | 调用频率 | 单次开销 | 总开销（K 个算子，每算子派 N 个 task） |
|------|---------|---------|--------------------------------------|
| 旧代码 | 每 task | O(K) 全量 | O(K × N × K) |
| PR #63750（独立） | 每 task | O(1) 增量 | O(K × N) |
| 247c9c2d（当前） | 每 op-batch | O(K) 全量 | O(K × K) |
| **两者结合** | **每 op-batch** | **O(1) 增量** | **O(K)** |

### 8.10 Layer 4.2 续：每 op-batch 后增量更新的正确性论证

#### 8.10.1 增量更新本身的正确性

`_recompute_op_usage(op)` 是**快照式**的——直接读取当前 metrics（已经反映了 N 个 dispatched task 的变化），减去旧缓存值，加上新值。无论中间派了 1 个还是 N 个 task，结果等价：

```
调用 N 次 update_usages_for_ops([op])  ≡  调用 1 次 update_usages_for_ops([op])
```

因为都是读到同一个最新 metrics 快照，算同一个 delta。

#### 8.10.2 跨算子的正确性

dispatch A 的 N 个 task → `update_usages_for_ops([op_A])`：

| 更新了什么 | 对下一个算子 B 的影响 |
|-----------|---------------------|
| A 的 per-op usage | 不影响 B 的 per-op usage（B 没变） |
| 全局总量（增量） | B 的 budget 通过 `_update_allocated_budgets` 基于**新的全局总量**重新分配 |
| A 的上游 + sink | 不影响 B 的调度决策 |

所以 `select_operator_to_run` 选 B 时，B 的 budget 已经扣除了 A 刚消耗的资源。

#### 8.10.3 不能整个 dispatch 循环结束后才更新

如果整个 `_dispatch_loop` 结束后才调用一次 `update_usages()`，会导致**调度错误**：

| 依赖方 | 读取的数据 | 不更新会怎样 |
|--------|-----------|-------------|
| `select_operator_to_run` | `get_op_usage()` 判断算子是否超预算 | 已超预算的算子仍被认为可调度 → **超卖资源** |
| `ResourceBudgetPolicy.available_capacity` | `allocator.available_task_capacity()` → 依赖 `get_budget()` → 依赖 per-op budget | budget 是旧的 → capacity 偏大 → **超量派发** |
| `ConcurrencyCapPolicy.available_capacity` | `num_tasks_running` | 已 dispatch 的任务没反映到 running 计数 → capacity 偏高 → **超出并发限制** |

**具体例子**：3 个算子各剩 budget 发 2 个 task，不更新的话 `select_operator_to_run` 可能给第一个算子派 2 个后，继续给同一个算子派（budget 快照显示还够），实际已超限。

**结论**：每 op-batch 后更新是**最低正确频率**，不能更低。

#### 8.10.4 soft_capacity 在 op-batch 内的一致性

在 A 的 op-batch 内，`soft_capacity` 是**batch 开始前**算的。派发过程中实际 budget 在被消耗，但没有实时更新。这**不是问题**，因为：
1. `soft_capacity` 本身就是"最多还能派多少个"的安全上界，派满正好用完 budget
2. 内循环有 `can_add_input()` 硬限制兜底（actor pool 状态等快速变化的约束）
3. 247c9c2d 的 capacity-based 设计已将 soft-policy slack 降为零

### 8.11 Layer 4.3: Allocator Reservation 缓存优化

#### 8.11.1 问题分析

当前 `_update_allocated_budgets()` 在每次 `update_usages()` / `update_usages_for_ops()` 尾部无条件调用，而 `_update_reservation` 的输入在同一调度迭代内几乎不变：

| 输入 | 同一调度迭代内是否变化 |
|------|----------------------|
| `limits` | 不变（已有 1s 限频） |
| `eligible_ops` | 几乎不变（仅算子完成时才变） |
| `reservation_ratio` | 常量 |

→ reservation 分配比例在同一调度迭代内是幂等的，但被重建 2+N 次。

真正需要频繁更新的是 **budget = reservation - usage** 中的 usage 部分，而 reservation（占总计算量的大部分）在同一调度迭代内完全可以复用。

#### 8.11.2 优化方案：缓存 reservation

```python
class ResourceManager:
    def __init__(self, ...):
        # ...
        self._last_reservation_limits: Optional[ExecutionResources] = None
        self._last_reservation_eligible_ops: Optional[Set] = None

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

**效果**：dispatch 循环中 N 次调用只做轻量的 budget 增量更新，reservation 重建从 2+N 次降到 0~1 次。

#### 8.11.3 需要注意的边界情况

- **算子在 dispatch 循环内完成执行**：如 `LimitOperator.mark_execution_finished` 在 `_add_input_inner` 中被调用，导致 `eligible_ops` 集合变化。此时必须重建 reservation。通过比较 `current_eligible != self._last_reservation_eligible_ops` 自动触发。
- **`_compute_completed_ops` 的结果变化**：已完成算子的 usage 需要从 reservation 中排除，如果 completed ops 集合变了，`available_limits` 也会变，触发重建。

#### 8.11.4 分阶段实施建议

```
Phase 4a: 采纳 PR #63750 增量更新（update_usages_for_ops）
  ├── 修改 resource_manager.py: 新增 _recompute_op_usage, update_usages_for_ops
  ├── 修改 streaming_executor.py: _dispatch_loop 中替换为 update_usages_for_ops([op])
  ├── 新增测试: 增量更新与全量更新结果一致性
  风险: 低（PR #63750 已有测试，且 _scheduling_loop_step 顶部的全量 update_usages 作为校准点）
  收益: dispatch 循环内 O(K) → O(1)

Phase 4b: Allocator reservation 缓存
  ├── 修改 resource_manager.py: 缓存 _update_reservation 的结果
  ├── 抽取 _update_budgets_only: 只重算 budget 不重建 reservation
  ├── 新增测试: reservation 缓存命中/失效场景
  风险: 中（需验证 eligible_ops 集合变化时的正确性）
  收益: reservation 重建从 2+N 次降到 0~1 次
```

---

## 九、预期收益与验证方法

### 9.1 综合性能预估

**场景: 1000 actors + 600 列宽 schema**

| 阶段 | release-2.55.1 | +Layer 1 | +Layer 2 | 降幅 |
|------|----------------|----------|----------|------|
| schema 反序列化 | ~7s (60%) | ~0.5s | ~0.5s | -93% |
| process_completed 其他 | ~2.5s (20%) | ~2s | ~0.5s | -80% |
| dispatch 循环 | ~2s (16%) | ~1.5s | ~0.4s | -80% |
| housekeeping | ~0.5s (4%) | ~0.5s | ~0.5s | 0% |
| **total max_scheduling_loop** | **~12s** | **~4.5s** | **~1.9s** | **-84%** |

**+Layer 4 后的额外收益**（增量更新 + reservation 缓存）：

| 阶段 | +Layer 2 | +Layer 4 | 说明 |
|------|----------|----------|------|
| dispatch 循环 update_usages | ~0.4s (O(K×K)) | ~0.04s (O(K)) | 增量更新只重算脏算子 |
| allocator reservation 重建 | ~0.2s (2+N 次) | ~0.02s (0~1 次) | reservation 缓存复用 |

**场景: 5000 actors + 600 列宽 schema**

| 阶段 | release-2.55.1 | +Layer 1 | +Layer 2 |
|------|----------------|----------|----------|
| **total max_scheduling_loop** | **~34s** | **~13s** | **~5s** |

### 9.2 验证方法

**1. 单元测试**:
- `test_block_metadata_schema_cache.py` (PR #63462 自带)
- 批 dispatch 的 backpressure 正确性测试
- 动态 timeout 的 liveness 测试

**2. 集成测试 (release test)**:
- 使用 `wide-schema worker_scaling` release test (PR #63420)
- 测试矩阵: [500, 1000, 2000, 5000] × [actor, task]
- 观察 `max_scheduling_loop_duration_s` 和 `streaming_exec_schedule_max_s`

**3. 线上验证**:
- 部署到内部可灵推理集群 (1K GPU)
- 观察 GPU utilization 和调度延迟
- 对比 6963b2d4 的效果

### 9.3 回归指标

| 指标 | 基线 | 目标 | 回归阈值 |
|------|------|------|----------|
| `max_scheduling_loop_duration_s` (1000 actors) | 12s | <2s | >3s |
| GPU utilization (1K GPU 集群) | ~60% | >90% | <80% |
| 端到端吞吐 (tasks/s) | baseline | >2x | <1.5x |
| 内存使用 (driver) | baseline | ≤1.1x | >1.5x |

---

## 十、实施顺序与风险评估

```
Phase 1 (Layer 1): Cherry-pick 社区优化
  ├── #63462 LRU-cache schema        ← 独立，零依赖，单项收益最大
  ├── #62720 首 block yield schema    ← 依赖 #53454（已有）
  ├── #62726 pickle 替代 cloudpickle  ← 依赖 #62720 的 metadata 格式
  ├── #62309 per-node actor heap      ← 依赖 #62114（已有）
  ├── #62891 bundle clean-up          ← 独立
  └── #63345 max_scheduling_loop 指标 ← 独立
  风险: 低（社区已验证，有 CI 覆盖）
  测试: 运行 wide-schema release test 对比数值

Phase 2 (Layer 2): 调度循环结构优化
  ├── 2.1 动态 timeout               ← 独立，改动最小（加一个参数）
  ├── 2.2 批 dispatch per operator    ← 核心改动，需验证 liveness
  ├── 2.3 completion 处理上限         ← 配合 2.1，改动小
  └── 2.4 profiling                   ← 独立，仅添加日志
  风险: 中
  - 2.2 需验证: backpressure 语义正确性（_BATCH_RESOURCE_CHECK_INTERVAL=16 够频繁）
  - 2.3 需验证: 提前返回不会丢失 completion（下一轮会处理剩余）
  测试: 全量 data test suite + release test

Phase 3 (Layer 3): 按需启用
  ├── 3.1 GPU-aware ranker            ← RAY_DATA_GPU_AWARE_SCHEDULING=1
  ├── 3.2 actor 刷新降频              ← RAY_DATA_ACTOR_REFRESH_INTERVAL_S=2
  └── 3.3 select 快速路径             ← 代码内自动判断 topology size
  风险: 中低（feature-flagged，不影响默认行为）
  测试: GPU 集群端到端验证

Phase 4 (Layer 4): ResourceManager 增量更新
  ├── 4.1 采纳 PR #63750 增量更新     ← _dispatch_loop 中 update_usages_for_ops([op]) 替代全量
  │   └── _scheduling_loop_step 顶部保留全量 update_usages() 作为校准点
  ├── 4.2 Allocator reservation 缓存  ← 避免同一迭代内幂等 reservation 重建
  └── 4.3 验证每 op-batch 增量更新的正确性（跨算子 budget 一致性）
  风险: 中低（4.1 有社区 PR + 测试覆盖；4.2 需验证 eligible_ops 变化边界）
  收益: dispatch 循环内 update_usages 从 O(K×K) 降到 O(K)；reservation 重建从 2+N 降到 0~1
```

### 与 6963b2d4 的关键设计区别

| 维度 | 6963b2d4 | 本方案 |
|------|----------|--------|
| `process_completed_tasks` | 绕过，内联重写 120 行 | **保留**，通过参数扩展（timeout/max_completions） |
| GPU 优先 | completion 处理阶段（效果间接） | **dispatch ranker** 阶段（直接影响调度决策） |
| 交错调度 | 硬编码 batch=512 | max_completions + timeout=0 实现**等效效果** |
| update_usages 降频 | 每 64 次 dispatch | 每 16 次（配合批 dispatch） → **增量更新每 op-batch 一次** |
| select_operator_to_run | 每个 task 都调用 | 批 dispatch **大幅减少调用次数** |
| 缓存 | 模块级 `_gpu_op_cache` 有泄漏风险 | 不需要全局缓存 |
| 代码结构 | 225 行单函数 | **保持原有函数边界**，增量修改 |
| schema 优化 | 无 | Cherry-pick 社区方案（单项 60% 收益） |
| 可维护性 | 与上游 diff 巨大，merge 困难 | 增量修改，每个优化独立可 revert |

---

## 附录A：相关社区 commits

### Ray Data 调度优化

| Commit | PR | 标题 | 日期 |
|--------|-----|------|------|
| `6ecde5eda9bd` | #63462 | Cache deserialized Arrow schemas in BlockMetadataWithSchema | 2026-05 |
| `ab9a7d7d2328` | #62720 | Yield only first schema in _map_task | 2026-04 |
| `81bda34d1f10` | #62726 | Regular pickle before task return | 2026-04 |
| `1de3b19effd2` | #62309 | Rank actors per node in a heap | 2026-04 |
| `2eb5ec059c5a` | #62891 | Final bundle clean-up | 2026-04 |
| `944df887fbac` | #63345 | Add max_scheduling_loop_duration_s metric | 2026-05 |
| `698b614b2c` | #57971 | Remove stats update thread | 2025-12 |

### Dashboard 性能优化（参考）

| Commit | 标题 | 说明 |
|--------|------|------|
| `b0828fb39e` | [Dashboard] Optimizing performance of Ray Dashboard (#47617) | 大规模 dashboard 优化，23 文件 |
| `811f98e612` | [core][dashboard] Update nodes on delta (#47367) | 增量更新 DataSource.nodes |
| `6c7da025c3` | Unify ThreadPoolExecutor in Dashboard (#47160) | 统一线程池 + asyncio yield |
| `9c0e1c4c1f` | [core][dashboard] configurable timeouts (#47181) | organize/purge 间隔可配 |
| `432dbce369` | [GCS] Optimize GetAllJobInfo API (#47530) | GCS job info 查询优化 |
| `4e34398229` | [core] disable memory_full_info calling (#60000) | 用 RSS 近似 USS |
| `b0e5ba97e4` | [core] Reduce event aggregator buffer size (#60826) | 防止 OOM |

### 内部优化 commits

| Commit | 标题 |
|--------|------|
| `6963b2d4cd` | [Data] GPU-optimized interleaved dispatch for scheduling loop |
| `907ed43472` | [Dashboard] Optimize /logical/actors endpoint for large-scale clusters |
| `eb4f846be0` | [Dashboard] dashboard optimization (log viewer, task table, pod name) |
| `c5314b1d38` | [Dashboard] dashboard optimization (ray_config_def.h) |
| `c162f7a870` | [core] Fixing the dashboard node_head api's dead node cache (#61185) |

---

## 附录B：环境变量配置汇总

| 环境变量 | 默认值 | 作用 | Layer |
|----------|--------|------|-------|
| `RAY_DATA_GPU_AWARE_SCHEDULING` | `0` | 启用 GPU-aware ranker | L3 |
| `RAY_DATA_ACTOR_REFRESH_INTERVAL_S` | `2.0` | actor 状态刷新间隔 | L3 |
| `RAY_DATA_SCHED_PROFILE_THRESHOLD_S` | `5.0` | profiling 输出阈值 | L2 |
| `RAY_DATA_MAX_COMPLETIONS_PER_STEP` | `512` | 单轮 completion 处理上限 | L2 |
| `RAY_DATA_BATCH_RESOURCE_CHECK_INTERVAL` | `16` | 批 dispatch 资源检查间隔 | L2 |
| `RAY_DATA_INCREMENTAL_USAGE_UPDATE` | `1` | 启用增量 update_usages_for_ops | L4 |

---

## 附录C：常见问题与排查指南

### C.1 调度循环耗时突增排查流程

```
max_scheduling_loop_duration_s 突增
│
├─ Q: 是否有大量 task 同时完成? (首次爆发 / 集群恢复)
│   └─ 查看 n_completions per step (profiling 日志)
│       → 如果 > 1000: 需要 Layer 2.3 completion 上限
│
├─ Q: 是否 schema 列数很多? (宽 schema)
│   └─ 查看 profiling 中 pa.ipc.read_schema 的 self 时间
│       → 如果占比 > 30%: 需要 Layer 1 schema 优化
│
├─ Q: 是否 actor 数量 > 1000?
│   └─ 查看 refresh_actor_state 的耗时
│       → 如果占比显著: 需要 Layer 3.2 刷新降频
│
├─ Q: 是否 dispatch 循环很慢? (select_operator_to_run 调用多)
│   └─ 查看 dispatch 阶段总耗时和 n_dispatched
│       → 如果 dispatch 阶段 > 30%: 需要 Layer 2.2 批 dispatch
│
└─ Q: 是否 ray.wait timeout 浪费? (有 pending work 时仍等 100ms)
    └─ 查看 has_pending_work 但 timeout=0.1s 的频率
        → 需要 Layer 2.1 动态 timeout
```

### C.2 GPU 利用率低排查

```
GPU utilization < 80%
│
├─ Q: 是否 GPU actor dispatch 延迟高?
│   └─ 查看 GPU op 的 task_submission_backpressure_time
│       → 如果显著: GPU op 被 backpressure 阻塞
│       → 需要 Layer 3.1 GPU-aware ranker
│
├─ Q: 是否调度循环本身太慢导致 GPU 空等?
│   └─ 查看 max_scheduling_loop_duration_s
│       → 如果 > GPU task 平均执行时间: 调度是瓶颈
│       → 需要 Layer 1 + Layer 2 组合优化
│
└─ Q: 是否数据供给不足? (上游 CPU op 产出慢)
    └─ 查看上游 op 的 output_queue size
        → 如果持续为 0: 上游产出不够，需扩大 CPU 并发
```

### C.3 Pipeline 死锁排查

```
Pipeline 停滞 (无进度)
│
├─ Q: 是否所有 op 都被 backpressure?
│   └─ 查看每个 op 的 scheduling_status
│       → runnable=False, under_resource_limits=False
│       → liveness 保证应该触发（如果 _consumer_idling=True）
│       → 如果未触发: 检查 output_queue 是否非空（用户线程卡在其他地方）
│
├─ Q: 是否有 op 的 can_add_input 永远返回 False?
│   └─ 检查 op.num_active_tasks() 是否达到 max
│       → ActorPoolMapOperator: max_tasks_in_flight = pool_size × max_tasks_per_actor
│       → 如果 pool_size = 0: actor pool 未初始化或全部崩溃
│
└─ Q: 是否 process_completed_tasks 阻塞?
    └─ ray.wait(timeout=0.1) 正常应该 ≤ 0.1s 返回
        → 如果长时间未返回: Ray Core 层面问题（GCS 不可用等）
```

### C.4 内存 OOM 排查

```
Driver 进程 OOM
│
├─ Q: 是否 output_queue 堆积过多?
│   └─ 消费者（用户主线程）处理速度 < 产出速度
│       → 检查用户代码中 iter_batches() 的消费逻辑
│       → DownstreamCapacityBackpressurePolicy 应该背压上游
│
├─ Q: 是否 schema cache 过大?
│   └─ LRU maxsize=64，每个 schema 通常 < 100KB
│       → 64 × 100KB = 6.4MB，不应是 OOM 根因
│
└─ Q: 是否 BlockMetadata 累积?
    └─ 大量 task completion 未处理完 → metadata 对象堆积
        → 配合 max_completions 限制每轮处理量
        → 确保 GC 能回收已处理的 metadata
```

---

## 附录D：核心代码文件索引

| 文件路径 | 核心类/函数 | 职责 |
|----------|-------------|------|
| `python/ray/data/_internal/execution/streaming_executor.py` | `StreamingExecutor` | 调度器主类，threading.Thread 子类 |
| `python/ray/data/_internal/execution/streaming_executor_state.py` | `process_completed_tasks`, `select_operator_to_run`, `get_eligible_operators` | 调度循环的状态管理和核心算法 |
| `python/ray/data/_internal/execution/resource_manager.py` | `ResourceManager`, `ReservationOpResourceAllocator` | 资源跟踪和预算分配 |
| `python/ray/data/_internal/execution/backpressure_policy/` | `BackpressurePolicy` 及三个实现 | 背压策略控制 |
| `python/ray/data/_internal/execution/ranker.py` | `DefaultRanker` | Operator 调度优先级排序 |
| `python/ray/data/_internal/execution/interfaces/physical_operator.py` | `DataOpTask.on_data_ready()` | Task completion 处理和 metadata 反序列化 |
| `python/ray/data/block.py` | `BlockMetadataWithSchema` | Schema 序列化/反序列化 |
| `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py` | `_ActorPool` | Actor 管理和选择 |
| `python/ray/data/_internal/plan.py` | `ExecutionPlan.create_executor()` | Executor 创建入口 |
| `python/ray/data/_internal/stats.py` | `Timer`, `_StatsActor`, `_StatsManager` | 统计指标收集和 Prometheus 暴露 |

---

## 附录E：术语表

| 术语 | 英文 | 含义 |
|------|------|------|
| 调度循环 | Scheduling Loop | StreamingExecutor 线程中 `_scheduling_loop_step` 的一次迭代 |
| 背压 | Backpressure | 限制上游 op 的 dispatch 速率以防止下游过载 |
| 拓扑 | Topology | `Dict[PhysicalOperator, OpState]`，DAG 中所有 op 及其状态 |
| Completion | Task Completion | Worker 端一个 task 执行完毕，output 可供 Driver 拉取 |
| Dispatch | Task Dispatch | Driver 将一个 input bundle 发送给 Worker 执行 |
| Waitable | Ray ObjectRef | `ray.wait` 等待的对象引用 |
| Liveness | Pipeline Liveness | 保证 pipeline 不会因所有 op 同时被背压而死锁 |
| Object Store | Plasma | Ray 的共享内存对象存储，block 数据存储位置 |
| RefBundle | Reference Bundle | 一组相关的 block refs + metadata，调度的最小单位 |
| Ranker | Operator Ranker | 给 eligible ops 排优先级，决定先 dispatch 谁 |
| Daemon Thread | 守护线程 | 随主进程退出而退出的后台线程 |
| GCS | Global Control Store | Ray 的全局元数据服务 |
| Raylet | 本地调度器 | 每个节点上的 Ray 本地资源管理进程 |
