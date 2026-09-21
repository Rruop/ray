# Ray Data 调度策略与 Lineage 容量深度分析

本文档记录 Ray Data 调度策略（scheduling_strategy / _label_selector）的关系、repartition
各子路径的调度行为、task spec 大小与 lineage 容量估算的源码级分析。

相关文档：
- [Ray-Data调度分析.md](./01-架构与原理/Ray-Data调度分析.md) — Hybrid/SPREAD 策略与 GPU 回避机制
- [Object-Reconstruction机制深度分析.md](./01-架构与原理/Object-Reconstruction机制深度分析.md) — lineage 完整机制与淘汰
- [StreamingRepartition深度分析.md](./02-数据流与算子/StreamingRepartition深度分析.md) — streaming repartition 实现细节

---

## 1. scheduling_strategy 与 _label_selector 的关系

### 1.1 两阶段过滤，非重复

两者在 Ray C++ 调度流水线中处于不同位置，是**串行关系**：

```
                  Ray 调度流水线
┌──────────────────────────────────────────────────────────┐
│  1. _label_selector 过滤（准入检查）                       │
│     → NodeResources::IsFeasible() / IsAvailable()         │
│     → 不满足 label 条件的节点直接排除，不进入候选集            │
│     → 代码: cluster_resource_data.cc:86-89                 │
│                                                            │
│  2. scheduling_strategy 选择（在候选集中做决策）              │
│     → SPREAD / HYBRID(DEFAULT) / NODE_AFFINITY / ...      │
│     → 在通过 label 过滤的节点中做负载均衡/亲和/打散           │
│     → 代码: spread_scheduling_policy.cc / hybrid_*.cc      │
└──────────────────────────────────────────────────────────┘
```

### 1.2 C++ 层执行逻辑

**`_label_selector`** 的检查在 `NodeResources::IsFeasible()` 和 `IsAvailable()` 中
**最先执行**，先于资源可用性检查：

```cpp
// src/ray/common/scheduling/cluster_resource_data.cc
bool NodeResources::IsAvailable(const ResourceRequest &resource_request, ...) const {
  const auto &label_selector = resource_request.GetLabelSelector();
  if (!HasRequiredLabels(label_selector)) {       // ← label 过滤先于资源检查
    return false;
  }
  return this->available >= resource_request.GetResourceSet();
}

bool NodeResources::IsFeasible(const ResourceRequest &resource_request) const {
  const auto &label_selector = resource_request.GetLabelSelector();
  if (!HasRequiredLabels(label_selector)) {       // ← 同样先检查 label
    return false;
  }
  return this->total >= resource_request.GetResourceSet();
}
```

`HasRequiredLabels` 支持的匹配操作：`LABEL_IN`（equals / in）和 `LABEL_NOT_IN`（not equals / not in）。

`scheduling_strategy` 在通过 label 过滤后的候选节点中选择最终目标节点，由对应策略实现
（SpreadSchedulingPolicy / HybridSchedulingPolicy / NodeLabelSchedulingPolicy 等）。

### 1.3 Python 层：两种 label 机制的差异

| 特性 | `_label_selector` | `NodeLabelSchedulingStrategy` |
|---|---|---|
| 设置方式 | `ray.remote(label_selector={"key": "value"})` | `scheduling_strategy=NodeLabelSchedulingStrategy({...})` |
| 匹配操作 | 仅 `equals` / `not equals` | `In` / `NotIn` / `Exists` / `DoesNotExist` |
| 执行阶段 | 资源可用性检查（IsFeasible/IsAvailable） | 独立调度策略（NodeLabelSchedulingPolicy） |
| 与 scheduling_strategy 关系 | **可同时使用**，先于 scheduling_strategy 执行 | 本身就是一种 scheduling_strategy，与 `_label_selector` 互补 |
| 典型场景 | 简单的节点分组过滤（如 `{"region": "us-east"}`） | 复杂的 label 匹配（如 `{"gpu_type": In("A100", "H100")}`） |

**类比**：类似 K8s 的 `nodeSelector`/`nodeAffinity`（先过滤节点）+ `podSpreadConstraints`/调度器策略（在过滤后的节点中做选择）。

### 1.4 _label_selector 在 task spec 中的传递链路

```
Python: ray.remote(label_selector={"key": "val"})
  ↓ (_raylet.pyx:3561)
C++:    prepare_label_selector(label_selector, &c_label_selector)
  ↓ (_raylet.pyx:3576)
        CTaskOptions(..., c_label_selector, ...)
  ↓ (_raylet.pyx:3584-3587)
        CCoreWorkerProcess.GetCoreWorker().SubmitTask(
            ray_function, args_vector, task_options,
            max_retries, ..., c_scheduling_strategy, ...)
```

`_label_selector` 放在 `CTaskOptions` 中，`scheduling_strategy` 单独传入 `SubmitTask`。
两者独立传递，在 C++ raylet 调度时先后应用。

---

## 2. repartition 调度策略分析

### 2.1 repartition API 不支持用户指定 scheduling_strategy

`dataset.repartition()` 的签名（`python/ray/data/dataset.py:1780-1789`）：

```python
def repartition(
    self,
    num_blocks: Optional[int] = None,
    target_num_rows_per_block: Optional[int] = None,
    *,
    strict: bool = False,
    shuffle: bool = False,
    keys: Optional[List[str]] = None,
    sort: bool = False,
) -> "Dataset":
```

没有 `**ray_remote_args` 或 `scheduling_strategy` 参数。对比 `random_shuffle` 支持该参数：

```python
def random_shuffle(
    self,
    *,
    seed: Optional[int | RandomSeedConfig] = None,
    num_blocks: Optional[int] = None,
    **ray_remote_args,       # ← 支持透传
) -> "Dataset":
```

### 2.2 各 repartition 子路径的默认调度策略

repartition 有多条内部路径，每条路径的调度策略不同：

#### A. shuffle=False：SplitRepartitionTaskScheduler

代码：`python/ray/data/_internal/planner/exchange/split_repartition_task_scheduler.py:66-72`

```python
if map_ray_remote_args is None:
    map_ray_remote_args = {}
if reduce_ray_remote_args is None:
    reduce_ray_remote_args = {}
if "scheduling_strategy" not in reduce_ray_remote_args:
    reduce_ray_remote_args = reduce_ray_remote_args.copy()
    reduce_ray_remote_args["scheduling_strategy"] = "SPREAD"
```

→ **reduce task 默认 SPREAD**

#### B. shuffle=True (pull-based)

代码：`python/ray/data/_internal/planner/exchange/pull_based_shuffle_task_scheduler.py:68-74`

```python
if "scheduling_strategy" not in reduce_ray_remote_args:
    reduce_ray_remote_args = reduce_ray_remote_args.copy()
    reduce_ray_remote_args["scheduling_strategy"] = "SPREAD"
```

→ **reduce task 默认 SPREAD**；map task 无显式设置（走 DataContext 默认）

#### C. shuffle=True (push-based)

代码：`python/ray/data/_internal/planner/exchange/push_based_shuffle_task_scheduler.py:481-485`

```python
# The placement strategy for reduce tasks is overwritten to colocate
# them with their inputs from the merge stage, so remove any
# pre-specified scheduling strategy here.
reduce_ray_remote_args = reduce_ray_remote_args.copy()
reduce_ray_remote_args.pop("scheduling_strategy", None)
```

→ **reduce/merge task 使用 NodeAffinitySchedulingStrategy** 将 task 与 merge 产出的节点亲和放置
（`push_based_shuffle_task_scheduler.py:142-152`）：

```python
node_strategies = {
    node_id: {
        "scheduling_strategy": NodeAffinitySchedulingStrategy(
            node_id, soft=True
        )
    }
    for node_id in set(merge_task_placement)
}
```

#### D. hash_shuffle (GPU shuffle)：aggregator actor

代码：`python/ray/data/_internal/execution/operators/hash_shuffle.py:1186-1189`

```python
remote_args = {
    ...
    "scheduling_strategy": "SPREAD",
    "allow_out_of_order_execution": True,
}
```

→ **aggregator actor 默认 SPREAD**

#### E. streaming repartition（target_num_rows_per_block 模式）

走 `MapOperator` 路径（非 AllToAllOperator），调度策略由
`MapOperator._get_ray_remote_args()` 决定（`map_operator.py:534-547`）：

```python
if "scheduling_strategy" not in ray_remote_args:
    ctx = self.data_context
    if input_bundle and input_bundle.size_bytes() > ctx.large_args_threshold:
        ray_remote_args["scheduling_strategy"] = ctx.scheduling_strategy_large_args  # "DEFAULT"
    else:
        ray_remote_args["scheduling_strategy"] = ctx.scheduling_strategy  # "SPREAD"
```

→ **小 bundle 用 SPREAD，大 bundle 用 DEFAULT(HYBRID)**

用户可以通过 `StreamingRepartition` 的 `ray_remote_args` 字段来覆盖 `scheduling_strategy`，
但 `dataset.repartition(target_num_rows_per_block=...)` API 目前没有暴露这个参数给用户。

### 2.3 底层已预留通道但未连通

| 层级 | streaming repartition | shuffle/split repartition |
|---|---|---|
| Logical Operator | `StreamingRepartition` 有 `ray_remote_args` 字段 | `Repartition` 继承 `AbstractAllToAll`（基类有 `ray_remote_args`），但 `__init__` 不接受该参数 |
| Planner | `plan_streaming_repartition_op` 已将 `op.ray_remote_args` 传给 `MapOperator.create` | `generate_repartition_fn` 和各 scheduler 的 `execute()` 都接受 `map_ray_remote_args`/`reduce_ray_remote_args`，但 planner 没从 `op.ray_remote_args` 传入 |
| API | `dataset.repartition(target_num_rows_per_block=...)` 没暴露 `**ray_remote_args` | 同左 |

**改动建议**：

**streaming repartition**（最简单）：
1. `dataset.repartition()` 加 `**ray_remote_args`
2. 传给 `StreamingRepartition(..., ray_remote_args=ray_remote_args)`
3. 已有的 `plan_streaming_repartition_op` → `MapOperator.create(ray_remote_args=op.ray_remote_args)` 自动生效

**shuffle/split repartition**（稍复杂）：
1. `Repartition.__init__` 加 `ray_remote_args` 参数
2. `dataset.repartition()` 加 `**ray_remote_args` 并传入
3. planner 把 `op.ray_remote_args` 传给 `generate_repartition_fn`
4. 需决定 `ray_remote_args` 同时作用于 map 和 reduce task，还是拆分

### 2.4 scheduler_avoid_gpu_nodes 与 repartition

配置定义（`src/ray/common/ray_config_def.h:859`）：

```cpp
RAY_CONFIG(bool, scheduler_avoid_gpu_nodes, true)  // 默认开启
```

各调度策略的 `avoid_gpu_nodes_` 设置（`scheduling_options.h`）：

| 策略 | avoid_gpu_nodes_ | repartition 是否使用 |
|---|---|---|
| SPREAD | `RayConfig::instance().scheduler_avoid_gpu_nodes()` | split / pull-based / hash_shuffle / streaming |
| HYBRID (DEFAULT) | `RayConfig::instance().scheduler_avoid_gpu_nodes()` | streaming (大 bundle) |
| NodeAffinity | 内部调用 `Hybrid()`，同样遵循 | push-based |
| NodeLabel | `RayConfig::instance().scheduler_avoid_gpu_nodes()` | 未使用 |
| BundlePack/Spread/StrictPack/StrictSpread | **硬编码 false** | 未使用 |

**结论**：repartition 所有路径都使用 SPREAD / HYBRID / NodeAffinity 策略，**全部遵循
`scheduler_avoid_gpu_nodes`**。非 GPU 的 repartition task 会被避免调度到 GPU 节点上。

---

## 3. RAY_max_lineage_bytes 配置

### 3.1 机制

lineage 是 Ray 用于**对象重建（reconstruction）**的机制：当一个 task 产出的 Object 丢失
（如 worker 故障），需要重新执行该 task。为此，已完成的 retryable task 的 spec 会被
**pin 在内存中**（不被 GC），直到确认不再需要重建。

```cpp
// src/ray/core_worker/task_manager.cc:1068-1079
if (task_retryable) {
    release_lineage = false;                                      // 不释放
    it->second.lineage_footprint_bytes_ = it->second.spec_.GetMessage().ByteSizeLong();
    total_lineage_footprint_bytes_ += it->second.lineage_footprint_bytes_;
    if (total_lineage_footprint_bytes_ > max_lineage_bytes_) {    // 超限
        RAY_LOG(INFO) << "Total lineage size is " << total_lineage_footprint_bytes_ / 1e6
                      << "MB, which exceeds the limit of " << max_lineage_bytes_ / 1e6
                      << "MB";
        min_lineage_bytes_to_evict =                                 // 驱逐到限额一半
            total_lineage_footprint_bytes_ - (max_lineage_bytes_ / 2);
    }
}
```

**默认值**：`RAY_CONFIG(int64_t, max_lineage_bytes, 1024 * 1024 * 1024)` = **1GB**

### 3.2 环境变量配置

`RAY_max_lineage_bytes` 环境变量设置（在 `ray start` 或 `ray.init()` 之前）：

```bash
# 设为 2GB（必须用纯整数，不支持表达式）
RAY_max_lineage_bytes=2147483648
```

或通过 `ray.init()`（Python 表达式可以）：

```python
ray.init(_system_config={"max_lineage_bytes": 2 * 1024 * 1024 * 1024})
```

> 注意：环境变量解析走 `ConvertValue<int64_t>`，底层是 `std::istringstream >> int64_t`
> （`ray_config.h:30-35`），只能识别纯整数字符串。`2*1024*1024*1024` 会解析失败。

连接已有集群时不能用 `_system_config`，只能用环境变量。

### 3.3 RAY_CONFIG 宏与环境变量自动注册

```cpp
// src/ray/common/ray_config.h:72-74
#define RAY_CONFIG(type, name, default_value)                       \
 private:                                                           \
  type name##_ = ReadEnv<type>("RAY_" #name, #type, default_value); \
 public:                                                            \
  inline type &name() { return name##_; }
```

`ReadEnv` 在进程启动时自动读取 `RAY_<config_name>` 环境变量，通过 `ConvertValue<T>`
解析为对应类型。所有 `RAY_CONFIG` 定义的配置项都支持环境变量覆盖。

### 3.4 源码引用

- 异常提示：`python/ray/exceptions.py:796` —
  `"RAY_max_lineage_bytes=<bytes> (default 1GB) during ray start."`
- 文档：`doc/source/ray-core/fault-tolerance/objects.rst:57` —
  `"set the environment variable RAY_max_lineage_bytes (default 1GB)"`

---

## 4. Task Spec 大小与 Lineage 容量分析

### 4.1 "1KB order" 的含义

Ray 源码注释（`ray_config_def.h:168-169`）：

```cpp
/// Each task spec is on the order of 1KB but can be much larger if it has many
/// inlined args.
RAY_CONFIG(int64_t, max_lineage_bytes, 1024 * 1024 * 1024)
```

**"1KB order"** 指的是 `rpc::TaskSpec` 这个 protobuf message 序列化后的二进制大小。
通过 `spec_.GetMessage().ByteSizeLong()` 计算（`task_manager.cc:1071`）。

一个 task spec 包含：
- task_id、parent_task_id、job_id
- function descriptor（模块/类/函数名）
- 资源请求（num_cpus / num_gpus / memory / custom resources）
- scheduling_strategy
- runtime_env 引用
- **参数列表**：ObjectRef（引用，极小）或**内联值**（inlined args，可能很大）
- 返回值数量（num_returns）
- label_selector / labels / fallback_strategy

### 4.2 何时 task spec 会大

| 因素 | 影响 |
|---|---|
| 内联参数（inlined args） | 直接序列化进 task spec，可能数十 KB 到 MB 级 |
| 大量 ObjectRef 参数 | 每个 ref 约 28 字节（ObjectID），1000 个 ref ≈ 28KB |
| num_returns 很大 | shuffle map 产出数百分区时，task spec 膨胀 |
| label_selector | 极小，可忽略 |
| scheduling_strategy | NodeLabelSchedulingStrategy 的 label expressions，几字节到百字节 |

**关键**：Ray Data 默认使用 ObjectRef 传递数据（引用，非内联），所以大多数 task spec 很小。

### 4.3 Actor task spec 不含 fn_constructor_kwargs

**关键结论**：`fn_constructor_kwargs` 只在 actor 创建时传一次，后续每个 submit task 不含此参数。

源码追踪：

**1. Actor 创建**（`actor_pool_map_operator.py:322-333`）：

```python
actor = self._actor_cls.options(
    _labels={self._OPERATOR_ID_LABEL_KEY: self.id, **labels}
).remote(
    ctx=self._data_context_ref,
    logical_actor_id=logical_actor_id,
    src_fn_name=self.name,
    map_transformer=self._map_transformer_ref,
    actor_location_tracker=get_or_create_actor_location_tracker(),
)
```

`fn_constructor_kwargs` 被封装在 `MapTransformer` 中（通过 `_CallableClassSpec`），
在 `_MapWorker.__init__` 里通过 `self._map_transformer.init()` 调用
（`actor_pool_map_operator.py:661-675`）：

```python
class _MapWorker:
    def __init__(self, ctx, src_fn_name, map_transformer, ...):
        self.src_fn_name = src_fn_name
        self._map_transformer = map_transformer
        DataContext._set_current(ctx)
        self._init_udf_with_retries(ctx)    # ← init() 内部调用 fn_constructor_kwargs 构造 UDF
```

`init()` 内部通过 `create_actor_context_init_fn` → `_CallableClassSpec` 构造 UDF 实例
（`plan_udf_map_op.py:387-396`）：

```python
callable_class_spec = _CallableClassSpec(
    cls=original_udf_class,
    args=fn_constructor_args,
    kwargs=fn_constructor_kwargs,    # ← 如 {"config": FsRayConfig(...)}
)
init_fn = create_actor_context_init_fn(
    udf_specs=[UDFSpec(spec=callable_class_spec, instantiation_class=udf)]
)
```

**2. 后续 submit task**（`actor_pool_map_operator.py:401-411`）：

```python
gen = actor.submit.options(
    num_returns="streaming",
    _labels={self._OPERATOR_ID_LABEL_KEY: self.id, **extra_labels},
    **actor_task_args,
).remote(
    self._data_context_ref,      # ObjectRef（引用）
    ctx,                          # TaskContext（轻量）
    *input_blocks,                # block ObjectRef（引用）
    slices=bundle.slices,         # 切片元数据
    **self.get_map_task_kwargs(), # 额外 kwargs（轻量）
)
```

数据全部通过 ObjectRef 传递，`fn_constructor_kwargs` 不在其中。

**3. _MapWorker.submit** 只转发给 `_map_task`（`actor_pool_map_operator.py:707-722`）：

```python
def submit(self, data_context, ctx, *blocks, slices=None, **kwargs):
    yield from _map_task(self._map_transformer, data_context, ctx, *blocks,
                         slices=slices, **kwargs)
```

已初始化的 UDF（callable class 实例）直接在 actor 进程内调用，不再序列化构造参数。

### 4.4 Pipeline 场景估算

#### framework runner（qg_v4）

链路：`source → repartition → map_batches(CPU actors) → map_batches(GPU actors) → sink`

| task 类型 | 参数内容 | 估算大小 |
|---|---|---|
| read task | 文件路径、读参数，无大 payload | ~0.5-1 KB |
| repartition streaming map | 输入 bundle 的 ObjectRef 列表 | ~1-3 KB |
| CPU map_batches task | actor submit，参数只有 block ObjectRef + TaskContext | ~1-2 KB |
| GPU map_batches task | 同上 | ~1-2 KB |

`fn_constructor_kwargs={"config": {"model_path": ..., "preprocess_threads": 1}}`
只在 actor 创建时使用（§4.3），每个 submit task 不含此参数。

#### fs_ray pipeline

链路（`fs_ray_pipeline.py:1226-1443 run_fs_ray_pipeline`）：

```
source → [input_map] → [skip_existing] → [mv-extract/m2v_flatten] → [limit] →
repartition(target_num_rows_per_block=N) → map_batches(FsRayActor, concurrency=C) →
[drop_columns] → [output_repartition] → [kafka_sinks] → [multi_version_wrap] →
[drop_transient] → write_sink
```

| task 类型 | 参数内容 | 估算大小 |
|---|---|---|
| read task (ksdataset/parquet) | ObjectRef 参数 | ~0.5-1 KB |
| repartition streaming map | ObjectRef 列表 | ~1-3 KB |
| **FsRayActor 创建 task** | `fn_constructor_kwargs={"config": FsRayConfig}` | **可能较大（见下）** |
| FsRayActor submit task | block ObjectRef + TaskContext | ~1-2 KB |
| kafka/drop/wrap map_batches | ObjectRef 参数 | ~1-2 KB |

**FsRayActor 创建 task** 可能较大，因为 `FsRayConfig` 被 pickle 序列化后内联到 task spec。
如果 config 含大字段（长的 model_path、多个 kafka topic 配置、hadoop classpath 等），
pickle 后可能达数十 KB。但**这只在创建 actor 时发生一次**（N_actors 次），不在每次 submit
task 中重复，不在 lineage 中反复累积。

### 4.5 lineage 容量估算

| 场景 | 同时 retryable task 数 | 单 task spec | 总 lineage | vs 1GB 限额 |
|---|---|---|---|---|
| 正常运行 | ~数十 | ~1 KB | ~100 KB | 远低于限额 |
| 大规模 worker 故障 | ~数千 | ~1-5 KB | ~5-20 MB | 仍远低 |
| 极端：shuffle + 故障 | ~数万 | ~5-10 KB | ~50-100 MB | 可达限额 5-10% |
| FsRayActor 创建 task retry | ~N_actors（2-40）| ~20-100 KB | ~2-4 MB | 远低于限额 |

**结论**：1GB 默认值对当前 pipeline 场景完全够用。只有在超大规模 shuffle +
集群故障的极端场景下才可能接近限额。

---

## 5. 当前 pipeline 框架中的调度配置

### 5.1 framework runner（pipeline/framework/runner.py）

通过 `_apply_scheduling_label` 全局覆盖 `DataContext.scheduling_strategy`：

```python
def _apply_scheduling_label(params: Dict[str, Any]) -> None:
    label = params.get("default_scheduling_label", "")
    if not label:
        return
    import ray.data
    from pipeline.framework.helpers import _label_scheduling_strategy
    ss = _label_scheduling_strategy(label)
    if ss:
        ctx = ray.data.DataContext.get_current()
        ctx.scheduling_strategy = ss
        # → 设为 NodeLabelSchedulingStrategy(ray.io/node-group=label)
```

这会覆盖**所有未显式指定 scheduling_strategy 的算子**（包括 repartition 的
streaming 模式），因为 streaming repartition 走 `MapOperator._get_ray_remote_args()`
会读取 `DataContext.scheduling_strategy`。

环境变量注入：`RAY_DEFAULT_SCHEDULING_LABEL=<label>`

### 5.2 fs_ray pipeline（ops/core/feature_service/fs_ray_pipeline.py）

fs_ray 框架当前**没有**全局调度策略覆盖。repartition 等算子走 Ray Data 默认值（SPREAD）。
如需节点分组调度，需在 `FsRayConfig.apply_datacontext_overrides()` 中添加类似逻辑。

### 5.3 DataContext 默认值

```python
# python/ray/data/context.py
DEFAULT_SCHEDULING_STRATEGY = "SPREAD"
DEFAULT_SCHEDULING_STRATEGY_LARGE_ARGS = "DEFAULT"

class DataContext:
    scheduling_strategy: SchedulingStrategyT = DEFAULT_SCHEDULING_STRATEGY        # "SPREAD"
    scheduling_strategy_large_args: SchedulingStrategyT = DEFAULT_SCHEDULING_STRATEGY_LARGE_ARGS  # "DEFAULT"
```

MapOperator 对大小 bundle 的策略选择（`map_operator.py:534-547`）：
- 小 bundle（size ≤ `large_args_threshold`）：用 `scheduling_strategy`（SPREAD）
- 大 bundle（size > `large_args_threshold`）：用 `scheduling_strategy_large_args`（DEFAULT/HYBRID）

---

## 6. C++ 调度策略详细参考

### 6.1 SchedulingType 枚举

```cpp
// src/ray/raylet/scheduling/policy/scheduling_options.h:30-41
enum class SchedulingType {
  HYBRID = 0,          // DEFAULT 策略，基于 spread_threshold 混合调度
  SPREAD = 1,          // 尽量分散到不同节点
  RANDOM = 2,
  NODE_AFFINITY = 3,   // 亲和指定节点
  BUNDLE_PACK = 4,     // PG: 尽量打包在同一节点
  BUNDLE_SPREAD = 5,   // PG: 尽量分散
  BUNDLE_STRICT_PACK = 6,
  BUNDLE_STRICT_SPREAD = 7,
  AFFINITY_WITH_BUNDLE = 8,
  NODE_LABEL = 9       // 基于 label 的调度
};
```

### 6.2 avoid_gpu_nodes 在调度策略中的应用

调度器在计算候选节点时，先通过 `IsFeasible()` / `IsAvailable()` 做 label 和资源过滤，
然后在候选节点中按策略选择。`avoid_gpu_nodes_` 字段影响节点打分/排序：
设为 true 时，有 GPU 的节点被降权，非 GPU task 优先调度到非 GPU 节点。

```cpp
// src/ray/raylet/scheduling/policy/scheduling_options.h
// 遵循 scheduler_avoid_gpu_nodes 的策略：
static SchedulingOptions Spread(...) {
    return SchedulingOptions(SchedulingType::SPREAD, ...,
                            RayConfig::instance().scheduler_avoid_gpu_nodes());
}
static SchedulingOptions Hybrid(...) {
    return SchedulingOptions(SchedulingType::HYBRID, ...,
                            RayConfig::instance().scheduler_avoid_gpu_nodes());
}
// 不遵循的（硬编码 false）：
static SchedulingOptions BundlePack()       { ... /*avoid_gpu_nodes*/ false); }
static SchedulingOptions BundleSpread()     { ... /*avoid_gpu_nodes*/ false); }
static SchedulingOptions BundleStrictPack() { ... /*avoid_gpu_nodes*/ false); }
static SchedulingOptions BundleStrictSpread(){ ... /*avoid_gpu_nodes*/ false); }
```

### 6.3 _label_selector 的 HasRequiredLabels 实现

```cpp
// src/ray/common/scheduling/cluster_resource_data.cc
bool NodeResources::HasRequiredLabels(const LabelSelector &label_selector) const {
    const auto &constraints = label_selector.GetConstraints();
    for (const auto &constraint : constraints) {
        if (!NodeLabelMatchesConstraint(constraint)) {
            return false;
        }
    }
    return true;
}

bool NodeResources::NodeLabelMatchesConstraint(const LabelConstraint &constraint) const {
    const auto &key = constraint.GetLabelKey();
    const auto &match_operator = constraint.GetOperator();
    const auto &values = constraint.GetLabelValues();
    const auto &node_labels = this->labels;

    if (match_operator == LabelSelectorOperator::LABEL_IN) {
        // key 存在且值在 values 中
        if (node_labels.contains(key) && values.contains(node_labels.at(key))) {
            return true;
        }
    } else if (match_operator == LabelSelectorOperator::LABEL_NOT_IN) {
        // key 不存在，或 key 存在但值不在 values 中
        if (!(node_labels.contains(key) && values.contains(node_labels.at(key)))) {
            return true;
        }
    }
    return false;
}
```

### 6.4 NodeLabelSchedulingPolicy 的实现

```cpp
// src/ray/raylet/scheduling/policy/node_label_scheduling_policy.cc
scheduling::NodeID NodeLabelSchedulingPolicy::Schedule(
    const std::vector<NodeResources> &cluster,
    const SchedulingOptions &options) {
    auto *label_ctx = dynamic_cast<const NodeLabelSchedulingContext *>(
        options.scheduling_context_.get());
    // 从 scheduling_strategy proto 中取出 hard/soft label expressions
    // FilterNodesByLabelMatchExpressions: 先按 hard 约束过滤节点
    // SelectFeasibleNodes: 在可行节点中选择可用节点
    // SelectAvailableNodes / SelectBestNode: 在可用节点中选最优
}
```

`NodeLabelSchedulingStrategy` 支持 hard（必须满足）和 soft（尽量满足）两组约束，
比简单的 `_label_selector` 更灵活。
