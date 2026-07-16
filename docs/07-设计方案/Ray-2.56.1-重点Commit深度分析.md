# Ray 2.56.1 重点 Commit 深度分析

## 1. [Core] Publish platform events via Ray Event Recorder (#63329)

**Commit**: `080c19520a` | **作者**: Richa Banker (Google) | **变更**: 11 files, +373/-44

### 目的
将 K8s 等平台事件通过 Ray 的 C++ EventRecorder 框架发布，使事件可通过 OTel/Grafana 等标准可观测性管道消费。

### 架构

```
K8s EventProvider (watch K8s events)
    ↓ (RayEvent protobuf)
PlatformEventsHead._process_event_callback()
    ↓ 分两路
    ├─ 路径1: 缓存到内存 OrderedDict (REST API /api/v0/platform_events 消费)
    └─ 路径2: PlatformEventBuilder → EventRecorder.emit() (新增)
                    ↓
              Event Aggregator (C++ gRPC, dashboard agent 端口)
                    ↓
              OTel Exporter → Prometheus / Grafana
```

### 核心变更

1. **新增 `PlatformEventBuilder`**（`ray._common.observability.platform_events.py`，67 行）
   - 继承 `InternalEventBuilder`，构建 `PlatformEvent` protobuf
   - 字段：`event_uid`, `platform`(枚举), `object_kind/name`, `reason`, `message`, `severity`, `component`, `source_metadata`, `custom_fields`

2. **改造 `PlatformEventsHead`**（+91/-28）
   - 初始化时创建 `EventRecorder` 实例，连接到 dashboard agent 的 gRPC aggregator
   - `_process_event_callback` 变为线程安全入口：`call_soon_threadsafe` 委托到主 loop
   - 缓存逻辑简化：更新事件时直接替换整个 `RayEvent`（不再逐字段合并）
   - 事件到达时同时缓存 + 通过 `EventRecorder.emit()` 发射

3. **开关**：`RAY_ENABLE_PYTHON_RAY_EVENT_TYPES` 环境变量需包含 `"PLATFORM_EVENT"`

4. **关闭清理**：`PlatformEventsHead.shutdown()` 时调用 `EventRecorder.shutdown()`

---

## 2. [Data] Expose flag to run read tasks on isolated worker processes (#63490)

**Commit**: `6058f06806` | **作者**: Balaji Veeramani | **变更**: 6 files, +169/-1

### 问题

PyArrow 在 read task 中分配大量内存（可达数 GB），这些内存是**合法的工作集**而非泄漏。问题在于 **worker 进程复用**：read task 完成后，同一个 worker 进程被调度执行下游算子的 task，但 PyArrow 的内存分配（Arrow 内存池、Parquet 行组解码缓冲区等）不会被释放回操作系统——Python/PyArrow 没有主动归还内存给 OS 的机制（`malloc` 释放的内存仍驻留在进程的 RSS 中，直到被 `madvise(MADV_DONTNEED)` 或 `malloc_trim` 回收）。

这导致：
- 下游算子 task 即使只需少量堆内存，其 RSS 仍等于 read 阶段峰值
- 内存监控按 RSS 判断，认为该 worker 内存紧张，触发 OOM kill
- 实际上 worker 的**活跃堆**很小，但 **驻留集（RSS）** 很大

### 解决方案

**核心机制**：通过给 read task 设置一个独特的 `runtime_env`（包含 `__RAY_DATA_OPERATOR_ID` 环境变量），使 Ray Core 将其调度到**独立的 worker 进程**上。Ray Core 不会在不同 `runtime_env` 的 task 之间共享 worker 进程。

```
Read Task (runtime_env 含 __RAY_DATA_OPERATOR_ID=ReadOp1)
    → Worker 进程 A (RSS = 数 GB, PyArrow 内存池驻留)

Map Task (runtime_env 不含此变量)
    → Worker 进程 B (RSS = 正常值, 不受 PyArrow 影响)

Read task 完成后 Worker A 被 kill/回收,
Worker B 的 RSS 不受 PyArrow 内存池影响
```

### 实现细节

1. **DataContext 新增**：
   ```python
   isolate_read_workers: bool = env_bool("RAY_DATA_ISOLATE_READ_WORKERS", False)
   ```

2. **TaskPoolMapOperator._add_unique_runtime_env()**：
   ```python
   def _add_unique_runtime_env(self, ray_remote_args):
       runtime_env = ray_remote_args.get("runtime_env", {})
       env_vars = ray_remote_args.get("env_vars", {})
       env_vars["__RAY_DATA_OPERATOR_ID"] = self.id  # 唯一标识
       runtime_env["env_vars"] = env_vars
       ray_remote_args["runtime_env"] = runtime_env
       return ray_remote_args
   ```
   Ray Core 的调度逻辑：不同 `runtime_env` 的 task → 不同 worker 进程。

3. **plan_read_op.py**：创建 read 算子时传入 `isolate_workers=data_context.isolate_read_workers`

4. **operator_fusion.py**：融合规则传播 `isolate_workers` 标志（如果上游或下游任一是 isolated，融合后的算子也 isolated）

5. **ActorPoolStrategy 不受影响**：Actor 本身已是独立进程，设置 `isolate_workers` 无效果（打 debug 日志提示）

### 为什么默认关闭

- 隔离 worker 意味着需要更多 worker 进程（read 用一套，下游用另一套），增加调度开销
- 在 read 数据量小、或 PyArrow 内存可接受的场景下，不必要的隔离反而降低并发度

---

## 3. [Data] Support UDF retries for transient exceptions (#63023)

**Commit**: `0ae431773b` | **作者**: Ayush Kumar | **变更**: 8 files, +329/-19

### 目的
允许 map UDF 在遇到瞬时错误（API 限流 429、外部服务抖动）时自动重试，而非立即失败整个 pipeline。

### 实现

`_map_task` 中的核心变更：

```python
def _map_task(...):
    retry_on = data_context.retried_map_errors

    def transform_iter_factory():
        blocks_iter = _iter_sliced_blocks(blocks, slices) if slices else iter(blocks)
        return map_transformer.apply_transform(blocks_iter, ctx)

    if retry_on:
        block_iter = iterate_with_retry(
            transform_iter_factory,           # 工厂函数，每次 retry 重建迭代器
            description="apply UDF transform",
            match=None if retry_on is True else retry_on,
            max_attempts=data_context.max_map_retries + 1,
            unwrap_cause=True,                 # 解包 UserCodeException → 真实 UDF 异常
        )
    else:
        block_iter = transform_iter_factory()

    for block in block_iter:
        ...
```

关键设计：
- **工厂函数模式**：迭代器是消耗性的，每次 retry 必须重建
- **`unwrap_cause=True`**：Ray Data 将 UDF 异常包装为 `UserCodeException`，匹配时需解包到原始异常（如 `RateLimitError`）

### DataContext 新增字段

```python
retried_map_errors: Union[bool, List[str]] = False
    # False = 不重试（默认）
    # True = 重试任何用户异常
    # ["RateLimit", "429"] = 仅当异常消息匹配时重试（子串优先，再正则）

max_map_retries: int = 3  # 最大重试次数
```

### retry 基础设施增强（`ray._common/retry.py`）

1. **`format_exception(exc, include_cause=False)`**：格式化为 `"ClassName: message"`，`include_cause=True` 时追加 `__cause__`
2. **`matches_error(pattern, error_str)`**：先尝试子串匹配，再尝试正则匹配（无效正则返回 False）
3. **`iterate_with_retry`**：新增 `unwrap_cause` 参数

### 退避策略

二进制指数退避 + 20% 随机抖动（与现有 `call_with_retry` 一致）。

---

## 4. [Data] Support multiple datasets in a cluster (#63331 + #63375)

**Commit 1**: `750ef4e506` | 28 files, +431/-45
**Commit 2**: `5d2c4e709b` | 7 files, +617/-50

### 目的
允许同一集群中并行运行多个 Ray Data 数据集，通过 subcluster label 调度实现资源隔离。

### Part 1: label_selector 传播管道

1. **ExecutionOptions 新增**：
   ```python
   label_selector: Optional[Dict[str, str]] = None
   ```

2. **`merge_label_selector()`**：将 DataContext 级别的 `label_selector` 合并到 `ray_remote_args`。算子级别的 key 优先（冲突时覆盖）

3. **全算子传播**：所有算子提交 Ray task/actor 时调用 `merge_label_selector`，将 label 附加到任务上。涉及 28 个文件

### Part 2: AutoscalingCoordinator 资源分区

1. **`_AutoscalingCoordinatorActor`**：
   - `_subcluster_selectors: Dict[str, Optional[Dict[str, str]]]` 映射每个 requester 到 subcluster
   - `_cluster_node_resources` 从 `List[ResourceDict]` 改为 `Dict[Optional[str], List[ResourceDict]]`（按 subcluster label 值分桶）
   - 跨 subcluster 分配报 `ValueError`
   - 过期请求清理时同时清理 subcluster 映射

2. **Label key**：`__subcluster__`（Part 2 代码中）→ 后续 PR #63982 改为 `ray-subcluster`（K8s label 名不允许 `__` 前缀）

### 使用示例

```python
ctx1 = DataContext.get_current()
ctx1.execution_options.label_selector = {"ray-subcluster": "ds1"}
ds1 = ray.data.read_parquet("s3://data1").context(ctx1)

ctx2 = DataContext.get_current()
ctx2.execution_options.label_selector = {"ray-subcluster": "ds2"}
ds2 = ray.data.read_parquet("s3://data2").context(ctx2)
# ds1 和 ds2 的 task/actor 被调度到不同的 subcluster 节点
```

---

## 5. [Data] Add default logical memory for map operators (#63814)

**Commit**: `5fe0aa1db8` | **作者**: Balaji Veeramani | **变更**: 5 files, +141/-1

### OOM 问题

Ray Data 的逻辑内存（`memory` remote arg）用于防止过度订阅。如果用户只给**部分** UDF 设了 `memory`，未设置的默认为 0，系统仍会过度订阅这些 UDF 导致 OOM。

### 解决方案

`default_map_logical_memory_enabled=True` 时，为未设置 `memory` 的 map 算子自动填充默认值。

**默认值推导**：
```
DEFAULT_LOGICAL_MEMORY_PER_CPU = 4 GiB × (1 - 10% system_reserved - 30% object_store)
                                ≈ 2.57 GiB per CPU core
```

- 超级云节点通常 4 GiB 物理 / CPU 核
- Ray Core 默认 logical memory = physical - 10% system - 30% object_store
- 这是**不降低并发度的最大安全值**

### 实现

```python
class MapOperator:
    DEFAULT_LOGICAL_MEMORY_PER_CPU: Final[int] = int(4 * GiB * 0.6)  # ≈ 2.57 GiB

    def _set_default_logical_memory(self, ray_remote_args):
        num_cpus = ray_remote_args.get("num_cpus") or 1
        default_memory = math.ceil(self.DEFAULT_LOGICAL_MEMORY_PER_CPU * num_cpus)
        ray_remote_args.setdefault("memory", default_memory)  # 不覆盖已有值
```

在 `MapOperator.create()` 中：
```python
if default_logical_memory_enabled:
    self._set_default_logical_memory(ray_remote_args)
```

`setdefault` 语义：用户显式设置的 `memory` 不被覆盖。

---

## 6. [Data] Add Dataset.mix() and MixOperator (#62450 + #63168)

**Commit 1 (内部算子)**: `a68fc8d691` | 8 files, +691/-7
**Commit 2 (公共 API)**: `a1d1bdedb0` | 15 files, +296/-56

### 目的
实现多数据集的**流式加权混合**，支持训练中不同数据源的按比例混合。

### 是批模式还是流式？

**完全流式（streaming）**，不是批模式。MixOperator 是 `InternalQueueOperatorMixin` + `NAryOperator` 的子类，运行在 Ray Data 的 streaming executor 中，按 block 粒度增量处理。

### MixOperator 核心算法：Deficit-Adjusted Weighted Round-Robin

**不是**简单的轮询或随机，而是基于**累计行数 deficit** 的确定性调度：

```
输入: N 个上游算子，权重 w[0..N-1]（归一化后 sum=1）

维护状态:
  rows_seen[i] = 从第 i 个输入已输出的累计行数
  input_buffers[i] = FIFO 队列（暂存第 i 个输入到达的 RefBundle）

每次 _try_output():
  total = sum(rows_seen)
  对每个未耗尽的输入 i，计算 deficit:
    deficit[i] = w[i] * total - rows_seen[i]
    （正值 = 该输入落后于目标比例，应优先输出）

  选择 deficit 最大的输入 best_index
  如果 best_index 的 buffer 有数据 → 取一个 RefBundle 到 output_buffer
  如果 buffer 空 → 不从其他输入取，等待（保证确定性）
```

**关键特性**：
- **确定性**：相同的输入到达顺序产生相同的输出顺序（不依赖随机）
- **收敛性**：无论 block 大小差异多大，长期行比例收敛到目标权重
- **逐 block 输出**：每个输出 block 来自恰好一个输入（不做行级重分配）

### 停止条件

| MixStoppingCondition | 行为 |
|---------------------|------|
| `STOP_ON_SHORTEST` | 任一输入耗尽时整个 Mix 停止（其他输入截断） |
| `STOP_ON_LONGEST_DROP`（默认） | 短输入耗尽后自然 drop out，继续从长输入取数据直到最长输入也耗尽 |

### 逻辑算子：`Mix`（frozen dataclass）

```python
@dataclass(frozen=True)
class Mix(NAry):
    weights: List[float]
    stopping_condition: MixStoppingCondition
```

`estimated_num_outputs()`：
- `STOP_ON_LONGEST_DROP` → `sum(per_input_counts)`（所有输入的 block 总和）
- `STOP_ON_SHORTEST` → `min(count / (w / total_weight))`（受最短输入限制）

### 公共 API

```python
@PublicAPI(stability="alpha")
def Dataset.mix(
    self,
    *other: Dataset,
    weights: Optional[List[float]] = None,
    stopping_condition: MixStoppingCondition = MixStoppingCondition.STOP_ON_LONGEST_DROP,
) -> Dataset:
```

### Mix vs Union 对比

| 维度 | Union | Mix |
|------|-------|-----|
| 混合策略 | 顺序拼接（先输入 1 全部，再输入 2 全部...） | 按权重交叉混合 |
| 行比例 | 取决于输入顺序和大小 | **收敛到目标权重** |
| 停止条件 | 全部输入完成 | `STOP_ON_SHORTEST` / `STOP_ON_LONGEST_DROP` |
| 适用场景 | 数据集拼接 | 训练数据按比例混合 |

### 数据流示意

```
Dataset1 (1000 行, weight=0.5) ──┐
                                   │  MixOperator
Dataset2 (600 行, weight=0.3)  ──┤  deficit-adjusted
                                   │  交叉选取
Dataset3 (400 行, weight=0.2)  ──┘
                                   ↓
输出: [D1 block] [D2 block] [D1 block] [D3 block] [D2 block] ...
       行比例 ≈ 50:30:20（逐步收敛，非严格每 block 精确）
```

---

## 7. [Core] Warn before --system-reserved-memory causes node death (#64492)

**Commit**: `c67bc802d1` (cherry-pick of `0582921`) | **变更**: 8 files, +120/-49

### 问题

K8s 环境中 `--system-reserved-memory` 设置不足时，system slice（kubelet、docker daemon 等 K8s 系统进程）的内存使用可能超过预留空间。Ray 的 `ThresholdMemoryMonitor` 原来只监控 user slice，对 system slice 的内存压力**无感知**。当 system slice 内存耗尽 → 内核 OOM killer 杀死系统进程 → **节点死亡**（比单个 worker OOM 严重得多）。

### 解决方案

在 `ThresholdMemoryMonitor` 中新增 **system slice 内存超预留预警**：

```cpp
// 原来: 只取 user slice 快照
auto user_snapshot = TakeUserSliceMemoryUsageSnapshot(...);

// 现在: 同时取 user 和 system slice 快照
auto [user_snapshot, system_snapshot] = TakeUserAndSystemSliceMemoryUsageSnapshot(...);

// 新增检查: system slice 内存超过预留空间时 ERROR 级别警告
if (system_snapshot.used_bytes >
    system_snapshot.total_bytes - memory_usage_threshold_bytes_) {
    RAY_LOG_EVERY_MS(ERROR, kErrorLogIntervalMs)
        << "System slice memory usage exceeds reserved system memory. "
        << "This can lead to node deaths. "
        << "Please consider increasing --system-reserved-memory.";
}
```

### system slice 使用量计算

```
system_slice_used = max(0, host_level_used - total_user_slice_used)
```

通过从主机总使用量中减去 user slice 使用量反推，涵盖所有非 Ray 用户空间的内存（内核、K8s 系统进程、docker daemon 等）。

### OOM kill 策略改进（`297fbea75a`）

空闲 worker 的内存开销纳入 OOM kill 决策：
- 任务完成后 worker 进程可能保留数 GB 的 import 缓存、库内部缓存
- 新策略：优先杀空闲 worker（而非活跃的 task/actor），保留工作进度
- OOM kill 日志增加每个 worker 的实际内存使用量和类型（Driver / Idle Worker / Task / Actor）

---

## 8. OOM 相关 Commit 汇总

| # | Commit | 模块 | 类型 | 描述 | OOM 修复机制 |
|---|--------|------|------|------|-------------|
| 1 | `6058f06806` | Data | 功能 | isolate_read_workers | read task 隔离到独立 worker 进程，PyArrow 内存池驻留不影响下游 |
| 2 | `5fe0aa1db8` | Data | 功能 | default_map_logical_memory_enabled | 未设 `memory` 的 map 算子自动填 ~2.57 GiB/CPU，防止过度订阅 |
| 3 | `91c91e1fb9` | Data | bugfix | Fix hash-shuffle aggregator memory estimation | metadata 传播修复 + 列裁剪 + 节点容量 clamp |
| 4 | `80021c9548` | Data | bugfix | Fix iter_batches spilling (1/n) | 移除 `make_async_gen(num_workers=1)` 隐藏缓冲 |
| 5 | `2c6dd5c655` | Data | bugfix | Fix iter_batches spilling (2/n) | 用 `iter_threaded` 替代 format/collate `make_async_gen` |
| 6 | `c67bc802d1` | Core | 功能 | Warn before system-reserved-memory causes node death | system slice 内存超预留 ERROR 告警 |
| 7 | `297fbea75a` | Core | 功能 | Consider idle workers for OOM killing | 空闲 worker 内存开销纳入 OOM kill 决策，优先杀空闲 worker |
| 8 | `76fca96617` | Core | bugfix | Fix OOM kill msg wrong threshold | resource isolation 下 kill 消息阈值错误 |
| 9 | `889186bbf3` | Core | 优化 | Avoid extra memcpy when spilling | 拆分 header+body 写入为两次 write，减少 spilling 拷贝开销 |
| 10 | `3ddd76cb21` | Data | 测试 | Report spilling and fail on unexpected spills | release test 新增 spilling 检测 |
