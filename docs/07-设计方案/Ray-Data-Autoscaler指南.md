# Ray Data Autoscaler 工作机制与测试指南

## 1. 概述

Ray Data 有两种独立的 autoscaler，分别管理不同层级的资源扩缩容：

```
┌────────────────────────────────────────────────────────────────────────┐
│                       StreamingExecutor                                 │
│                                                                         │
│  ┌─────────────────────────┐     ┌──────────────────────────────────┐  │
│  │   Actor Autoscaler      │     │    Cluster Autoscaler (V2)       │  │
│  │  (管理单个 Actor Pool)   │     │   (管理整个集群节点)             │  │
│  └───────────┬─────────────┘     └────────────────┬─────────────────┘  │
│              │                                     │                    │
│              ▼                                     ▼                    │
│  ┌─────────────────────────┐     ┌──────────────────────────────────┐  │
│  │   ActorPoolMapOperator  │     │  AutoscalingCoordinator (Actor)  │  │
│  │  (map_batches + Actor)  │     │      (协调多 dataset 请求)        │  │
│  └─────────────────────────┘     └────────────────┬─────────────────┘  │
│                                                    │                    │
│                                                    ▼                    │
│                                  ┌──────────────────────────────────┐  │
│                                  │   ray.autoscaler.sdk             │  │
│                                  │   request_resources()            │  │
│                                  └──────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Actor Autoscaler 工作机制

### 2.1 核心文件

| 文件 | 说明 |
|------|------|
| `python/ray/data/_internal/actor_autoscaler/default_actor_autoscaler.py` | 默认 Actor Autoscaler 实现 |
| `python/ray/data/_internal/actor_autoscaler/actor_pool_resizing_policy.py` | 扩缩容策略计算 |
| `python/ray/data/_internal/actor_autoscaler/autoscaling_actor_pool.py` | Actor Pool 接口定义 |
| `python/ray/data/_internal/actor_autoscaler/noop_actor_autoscaler.py` | 空操作 Autoscaler |

### 2.2 利用率计算

Actor Pool 利用率的计算公式：

```python
utilization = num_tasks_in_flight / (max_actor_concurrency × num_running_actors)
```

其中：
- `num_tasks_in_flight`: 正在执行或排队中的任务数
- `max_actor_concurrency`: 每个 Actor 的最大并发数
- `num_running_actors`: 当前运行中的 Actor 数量

### 2.3 扩缩容决策逻辑

```
┌─────────────────────────────────────────────────────────────┐
│                     决策流程                                 │
├─────────────────────────────────────────────────────────────┤
│  1. 检查是否已完成或输入消费完毕 → 强制缩容                   │
│  2. 检查 pool size < min_size → 扩容到 min_size             │
│  3. 检查 pool size > max_size → 缩容到 max_size             │
│  4. 检查是否已接收输入 → 否则不操作                          │
│  5. 计算利用率:                                              │
│     - util >= upscaling_threshold → 扩容                    │
│     - util <= downscaling_threshold → 缩容                  │
│     - 其他 → 不操作                                          │
└─────────────────────────────────────────────────────────────┘
```

| 条件 | 动作 | 说明 |
|------|------|------|
| 利用率 >= `upscaling_threshold` (默认 175%) | 扩容 | 负载过高 |
| 利用率 <= `downscaling_threshold` (默认 50%) | 缩容 | 负载过低 |
| 介于两者之间 | 不操作 | 负载正常 |
| 操作已完成或输入消费完毕 | 强制缩容 | 清理资源 |
| pool size < min_size | 扩容到 min_size | 保证最小规模 |
| pool size > max_size | 缩容到 max_size | 限制最大规模 |

### 2.4 扩容 Delta 计算

```python
# 计算期望扩容数量
plan_delta = ceil(current_size × (util / upscaling_threshold - 1))

# 应用限制
delta = min(plan_delta, max_upscaling_delta, max_size - current_size)
delta = max(1, delta)  # 至少扩容 1 个
```

### 2.5 关键配置参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `actor_pool_util_upscaling_threshold` | 1.75 (175%) | 触发扩容的利用率阈值 |
| `actor_pool_util_downscaling_threshold` | 0.5 (50%) | 触发缩容的利用率阈值 |
| `actor_pool_max_upscaling_delta` | 1 | 单次最大扩容 Actor 数 |

### 2.6 配置示例

```python
from ray.data.context import DataContext, AutoscalingConfig

ctx = DataContext.get_current()
ctx.autoscaling_config = AutoscalingConfig(
    actor_pool_util_upscaling_threshold=1.0,   # 利用率达到 100% 就扩容
    actor_pool_util_downscaling_threshold=0.3, # 利用率低于 30% 就缩容
    actor_pool_max_upscaling_delta=3,          # 每次最多扩容 3 个 Actor
)
```

---

## 3. Cluster Autoscaler V2 工作机制

### 3.1 核心文件

| 文件 | 说明 |
|------|------|
| `python/ray/data/_internal/cluster_autoscaler/default_cluster_autoscaler_v2.py` | Cluster Autoscaler V2 实现 |
| `python/ray/data/_internal/cluster_autoscaler/resource_utilization_gauge.py` | 资源利用率计算 |

### 3.2 触发条件

```python
# 计算集群资源利用率
cpu_util = global_usage.cpu / global_limits.cpu
gpu_util = global_usage.gpu / global_limits.gpu
obj_store_mem_util = global_usage.object_store_memory / global_limits.object_store_memory

# 任意资源利用率 >= 阈值时触发扩容
if cpu_util >= threshold or gpu_util >= threshold or mem_util >= threshold:
    trigger_scale_up()
```

### 3.3 关键环境变量

| 环境变量 | 默认值 | 说明 |
|---------|--------|------|
| `RAY_DATA_CLUSTER_SCALING_UP_UTIL_THRESHOLD` | 0.75 | 触发扩容的利用率阈值 |
| `RAY_DATA_CLUSTER_UTIL_AVG_WINDOW_S` | 10 | 计算平均利用率的时间窗口 (秒) |
| `RAY_DATA_CLUSTER_SCALING_UP_DELTA` | 1 | 每种节点类型的扩容增量 |
| `RAY_DATA_MIN_GAP_BETWEEN_AUTOSCALING_REQUESTS` | 10 | 扩容请求最小间隔 (秒) |

---

## 4. 两种 Autoscaler 对比

| 特性 | Actor Autoscaler | Cluster Autoscaler V2 |
|------|-----------------|----------------------|
| **作用范围** | 单个 Actor Pool | 整个集群节点 |
| **触发条件** | Actor Pool 利用率 | 集群资源利用率 |
| **扩容对象** | Ray Actor | 集群节点 |
| **缩容机制** | 主动释放空闲 Actor | 依赖 Ray idle termination |
| **扩容阈值** | 175% (可配置) | 75% |
| **缩容阈值** | 50% (可配置) | 无 (仅发送续约请求) |
| **配置方式** | DataContext | 环境变量 |

---

## 5. 测试方法

### 5.1 单元测试

运行现有单元测试：

```bash
# 激活正确的 conda 环境
conda activate py312-kray-master-54

# 运行单元测试
pytest python/ray/data/tests/test_autoscaler.py -v
```

### 5.2 集成测试脚本

测试文件位置：`python/ray/data/tests/test_autoscaler_integration.py`

#### 测试用例 1: 高利用率扩容测试

```python
import ray
from ray.data import ActorPoolStrategy

ray.init(num_cpus=10)

@ray.remote(max_concurrency=100)
class MetricsCollector:
    def __init__(self):
        self.actor_ids = set()

    def record(self, actor_id):
        self.actor_ids.add(actor_id)

    def get_unique_actors(self):
        return len(self.actor_ids)

class SlowMapper:
    def __init__(self, metrics_ref):
        self.metrics = metrics_ref
        ctx = ray.runtime_context.get_runtime_context()
        self.actor_id = ctx.get_actor_id()

    def __call__(self, batch):
        ray.get(self.metrics.record.remote(self.actor_id))
        import time
        time.sleep(0.3)  # 模拟耗时任务
        return batch

metrics = MetricsCollector.remote()
ds = ray.data.range(20, override_num_blocks=20)

result = ds.map_batches(
    SlowMapper,
    fn_constructor_args=(metrics,),
    compute=ActorPoolStrategy(
        min_size=1,
        max_size=5,
        max_tasks_in_flight_per_actor=4,
    ),
    batch_size=1,
)

output = result.take_all()
unique_actors = ray.get(metrics.get_unique_actors.remote())
print(f"Unique actors used: {unique_actors}")
assert unique_actors > 1, "Actor pool should have scaled up"
```

#### 测试用例 2: Barrier 同步测试

使用 Barrier 模式精确验证扩容行为：

```python
import time
import ray
from ray.data import ActorPoolStrategy

ray.init(num_cpus=10)

@ray.remote(max_concurrency=20)
class Barrier:
    def __init__(self, target_concurrent, timeout=30.0):
        self.target = target_concurrent
        self.timeout = timeout
        self.current_waiters = 0
        self.max_waiters = 0
        self.released = False

    def wait(self):
        self.current_waiters += 1
        self.max_waiters = max(self.max_waiters, self.current_waiters)

        start_time = time.time()
        while self.current_waiters < self.target and not self.released:
            if time.time() - start_time > self.timeout:
                self.current_waiters -= 1
                raise TimeoutError(f"Only {self.max_waiters}/{self.target} reached")
            time.sleep(0.05)

        self.released = True
        self.current_waiters -= 1
        return True

    def get_max_waiters(self):
        return self.max_waiters

class BarrierWaiter:
    def __init__(self, barrier_ref):
        self.barrier = barrier_ref

    def __call__(self, batch):
        ray.get(self.barrier.wait.remote())
        return batch

# 创建需要 2 个并发任务才能解除阻塞的 Barrier
barrier = Barrier.remote(target_concurrent=2, timeout=15.0)

ds = ray.data.range(3, override_num_blocks=3)
result = ds.map_batches(
    BarrierWaiter,
    fn_constructor_args=(barrier,),
    compute=ActorPoolStrategy(
        min_size=1,
        max_size=3,
        max_tasks_in_flight_per_actor=2,
    ),
    batch_size=1,
)

# 如果扩容成功，Barrier 会解除阻塞；否则超时
output = result.take_all()
max_waiters = ray.get(barrier.get_max_waiters.remote())
print(f"Max concurrent waiters: {max_waiters}")
assert max_waiters >= 2, "Actor pool should have scaled up"
```

#### 测试用例 3: 吞吐量监控测试

监控扩容前后的处理速度变化：

```python
import time
import threading
from collections import deque
import ray
from ray.data import ActorPoolStrategy

ray.init(num_cpus=10)

@ray.remote(max_concurrency=100)
class ThroughputMetrics:
    """吞吐量指标收集器"""
    def __init__(self):
        self.total_records = 0
        self.samples = []  # (timestamp, throughput, worker_count)
        self.recent_events = deque()
        self.active_workers = {}
        self.start_time = None

    def record_process(self, worker_id: str, num_records: int):
        now = time.time()
        if self.start_time is None:
            self.start_time = now
        self.total_records += num_records
        self.active_workers[worker_id] = now
        self.recent_events.append(now)
        # 清理1秒前的事件
        cutoff = now - 1.0
        while self.recent_events and self.recent_events[0] < cutoff:
            self.recent_events.popleft()

    def take_sample(self):
        now = time.time()
        cutoff = now - 1.0
        throughput = sum(1 for t in self.recent_events if t >= cutoff)
        active = sum(1 for t in self.active_workers.values() if t >= cutoff - 5)
        self.samples.append({
            "time": now - (self.start_time or now),
            "throughput": throughput,
            "workers": active
        })

    def get_results(self):
        return {
            "total": self.total_records,
            "samples": self.samples,
            "unique_workers": len(self.active_workers)
        }

metrics = ThroughputMetrics.remote()

# 后台采样线程
stop_event = threading.Event()
def sample_loop():
    while not stop_event.is_set():
        ray.get(metrics.take_sample.remote())
        time.sleep(0.3)
thread = threading.Thread(target=sample_loop, daemon=True)
thread.start()

class WorkerMapper:
    def __init__(self, metrics_ref, work_time_ms=30):
        self.metrics = metrics_ref
        self.work_time = work_time_ms / 1000.0
        ctx = ray.runtime_context.get_runtime_context()
        self.worker_id = ctx.get_actor_id() or str(id(self))

    def __call__(self, batch):
        time.sleep(self.work_time)  # 模拟工作
        ray.get(self.metrics.record_process.remote(self.worker_id, len(batch["id"])))
        return batch

# 100 个块模拟大量数据
ds = ray.data.range(1000, override_num_blocks=100)
result = ds.map_batches(
    WorkerMapper,
    fn_constructor_args=(metrics, 30),
    compute=ActorPoolStrategy(min_size=1, max_size=5, max_tasks_in_flight_per_actor=4),
    batch_size=10,
)
output = result.take_all()

stop_event.set()
results = ray.get(metrics.get_results.remote())

print(f"总记录数: {results['total']}")
print(f"使用的 Actor 数: {results['unique_workers']}")
print("\n吞吐量变化:")
for s in results["samples"][:15]:
    print(f"  {s['time']:.1f}s: {s['throughput']} records/s, {s['workers']} workers")
```

#### 测试用例 4: Task Pool 并发测试

测试 Task Pool (非 Actor) 的并发控制：

```python
import ray

ray.init(num_cpus=10)

def process_batch(batch):
    import time
    time.sleep(0.05)  # 50ms 处理时间
    return batch

ds = ray.data.range(500, override_num_blocks=50)

# 使用 concurrency 参数控制 Task Pool 并发度
result = ds.map_batches(
    process_batch,
    batch_size=10,
    concurrency=5,  # 最多 5 个并发任务
)

output = result.take_all()
print(f"处理完成: {len(output)} 条记录")
```

### 5.3 观察 Autoscaler 指标

#### 启用调试日志

```python
import logging
logging.getLogger("ray.data").setLevel(logging.DEBUG)
```

#### 使用 Ray Dashboard

访问 `http://localhost:8265` 查看:
- **Actors 面板**: Actor 数量变化
- **Nodes 面板**: 节点数量变化
- **Data 面板**: 数据集执行进度

#### 检查 Autoscaler 日志

```bash
grep -i "autoscal\|scale up\|scale down\|utilization" /tmp/ray/session_*/logs/*
```

---

## 6. 配置验证警告

当 Actor Pool 配置无法触发扩容时，系统会发出警告：

```
⚠️  Actor Pool configuration of the ActorPoolMapOperator[...] will not allow it to scale up:
configured utilization threshold (175.0%) couldn't be reached with
configured max_concurrency=1 and max_tasks_in_flight_per_actor=1
(max utilization will be max_tasks_in_flight_per_actor / max_concurrency = 100%)
```

**解决方案**: 确保 `max_tasks_in_flight_per_actor / max_concurrency >= upscaling_threshold`

---

## 7. 常见问题

### Q1: 为什么 Actor Pool 没有扩容？

检查以下条件：
1. `max_tasks_in_flight_per_actor / max_concurrency` 是否能达到 `upscaling_threshold`
2. 是否有 `pending_actors` (上一次扩容尚未完成)
3. 是否已达到 `max_size` 限制
4. 操作是否超出资源配额 (`under_resource_limits=False`)

### Q2: 如何更快触发扩容？

降低扩容阈值：

```python
ctx.autoscaling_config = AutoscalingConfig(
    actor_pool_util_upscaling_threshold=1.0,  # 从 175% 降到 100%
)
```

### Q3: 如何禁用 Actor Autoscaler？

```python
from ray.data.context import ActorAutoscalerType, AutoscalingConfig

ctx.autoscaling_config = AutoscalingConfig(
    autoscaler_type=ActorAutoscalerType.DISABLED,
)
```

或使用固定大小的 Actor Pool：

```python
compute=ActorPoolStrategy(size=4)  # 固定 4 个 Actor
```

---

## 8. 关键代码路径

| 功能 | 文件路径 |
|------|---------|
| Actor Autoscaler 核心逻辑 | `python/ray/data/_internal/actor_autoscaler/default_actor_autoscaler.py` |
| Cluster Autoscaler V2 | `python/ray/data/_internal/cluster_autoscaler/default_cluster_autoscaler_v2.py` |
| 利用率计算 | `python/ray/data/_internal/cluster_autoscaler/resource_utilization_gauge.py` |
| 配置参数 | `python/ray/data/context.py` (AutoscalingConfig) |
| 单元测试 | `python/ray/data/tests/test_autoscaler.py` |
| 集成测试 | `python/ray/data/tests/test_autoscaler_integration.py` |
| 吞吐量测试 | `python/ray/data/tests/test_autoscaler_throughput.py` |
| Task Pool 算子 | `python/ray/data/_internal/execution/operators/task_pool_map_operator.py` |
| 执行器入口 | `python/ray/data/_internal/execution/streaming_executor.py` |

---

## 9. Task Pool vs Actor Pool

### 9.1 核心区别

| 特性 | Task Pool | Actor Pool |
|------|-----------|------------|
| **执行模型** | 每个任务是独立的 Ray Task | 任务在持久化 Actor 上执行 |
| **状态保持** | 无状态，每次重新初始化 | 有状态，Actor 保持在内存中 |
| **扩缩容** | 通过 `concurrency` 参数控制并发 | 通过 Autoscaler 动态调整 Actor 数 |
| **适用场景** | 简单的无状态转换 | 需要加载模型、初始化开销大的任务 |
| **资源效率** | 每个任务独立调度 | Actor 复用，减少初始化开销 |

### 9.2 Task Pool 使用

```python
# Task Pool (默认，不使用 compute 参数或使用 concurrency)
ds.map_batches(
    lambda batch: process(batch),
    batch_size=100,
    concurrency=5,  # 最多 5 个并发任务
)
```

Task Pool 的并发由 `concurrency` 参数控制：
- `concurrency=None`: 不限制并发 (默认)
- `concurrency=N`: 最多 N 个并发任务

### 9.3 Actor Pool 使用

```python
# Actor Pool (使用 ActorPoolStrategy)
ds.map_batches(
    MyMapper,  # 类，会被实例化为 Actor
    compute=ActorPoolStrategy(
        min_size=1,       # 最小 Actor 数
        max_size=10,      # 最大 Actor 数
        max_tasks_in_flight_per_actor=4,  # 每个 Actor 的任务队列深度
    ),
)
```

### 9.4 选择建议

**使用 Task Pool 当:**
- 处理函数是纯函数，无状态
- 处理时间短，初始化开销小
- 不需要加载大型模型或资源

**使用 Actor Pool 当:**
- 需要加载 ML 模型 (初始化开销大)
- 处理函数有状态需要保持
- 需要 GPU 资源 (Actor 绑定 GPU)
- 需要动态扩缩容

---

## 10. 吞吐量测试与性能验证

### 10.1 运行吞吐量测试

```bash
# 激活环境
conda activate py312-kray-master-54

# 复制测试文件到临时目录运行 (避免本地源码干扰)
cp python/ray/data/tests/test_autoscaler_throughput.py /tmp/
cd /tmp
pytest test_autoscaler_throughput.py -v -s
```

### 10.2 测试用例说明

| 测试 | 说明 |
|------|------|
| `test_actor_pool_throughput_scaling` | 验证 Actor Pool 扩容提升吞吐量 |
| `test_task_pool_throughput` | 测试 Task Pool 的吞吐量表现 |
| `test_compare_actor_pool_configurations` | 对比不同 Actor Pool 配置的性能 |
| `test_scaling_with_varying_load` | 测试负载变化时的扩缩容行为 |
| `test_actor_pool_scale_down_on_completion` | 验证任务完成后的缩容行为 |

### 10.3 预期结果

```
=== Actor Pool 配置性能对比 ===
配置                 耗时(s)      吞吐量(r/s)     Actor数    最大并发
----------------------------------------------------------------------
固定1个Actor         5.20         96.15          1          1
固定3个Actor         2.10         238.10         3          3
自动扩缩容(1-5)      1.85         270.27         4          4
```

关键验证点:
- 自动扩缩容配置应使用多个 Actor (`unique_workers > 1`)
- 扩容后吞吐量应有明显提升
- 后期平均吞吐量应高于早期 (扩容生效)
