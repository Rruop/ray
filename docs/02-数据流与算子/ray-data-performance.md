# Ray Data 性能调优指南

让 Ray Data pipeline 第一次写就跑得快、内存稳、可断点续跑的实操参考。规范性约束见
`openspec/specs/pipeline-runtime.md`；共享实现见 `pipeline/runtime_setup.py`；
MaaS 平台部署见 `docs/02-product/maas-deployment.md`。

> 核心目标：**把 GPU 利用率提上去**。GPU 算子利用率低，要么是 GPU 算子并发不够，
> 要么是上游产出的 block 跟不上 GPU 消费。下面的调优都围绕这两点。

---

## 一键配置：setup_data_context()

绝大多数调优项已收口进 `pipeline/runtime_setup.py`，在 `ray.init()` 后立即调用即可：

```python
import ray
from pipeline.runtime_setup import (
    setup_data_context, make_checkpoint_config, make_hdfs_filesystem,
    WORKER_ONLY_REMOTE_ARGS,
)

ray.init(runtime_env={"env_vars": {"PYTHONPATH": project_root}})

# 1) 解析 HDFS glob -> (filesystem, paths)
fs, paths = make_hdfs_filesystem("viewfs://hadoop-lt-cluster/path/to/*.parquet")

# 2) 默认开启 checkpoint（断点续跑）
ckpt = make_checkpoint_config(
    id_column="blobstore_id",
    checkpoint_path=f"/home/ad/renruoyu/checkpoint_test/test_{data_set_id}",
    hdfs_fs=fs,
)

# 3) 一次性应用 DataContext 调优 + 注入 checkpoint
setup_data_context(checkpoint=ckpt)

# 4) 读取：显式并发 + worker-only 调度
ds = ray.data.read_parquet(
    paths, filesystem=fs,
    concurrency=2000, override_num_blocks=2000,
    ray_remote_args=WORKER_ONLY_REMOTE_ARGS,
)
```

`setup_data_context()` 默认完成：block size 上下界、object store 背压上限、关闭 issue
detectors、固定集群关闭 autoscaler、max_errored_blocks、tensor 转换关闭、注入 checkpoint。
下面逐项展开原理与可调参数。

---

## 1. Block size：控制 block 数量

block 过多会让 GCS 调度耗时突增，拖垮吞吐。调度耗时可在
**Ray Dashboard → Metrics → RAY DATA → Scheduling Loop** 查看，建议把 Running 中的 block
控制在 **10w 以内**。

```python
ctx = DataContext.get_current()
ctx.min_block_size = 1 * 1024 * 1024            # 1 MiB
ctx.target_max_block_size = 2 * 1024 * 1024 * 1024  # 2 GiB
```

（`setup_data_context()` 已默认设置，可通过 `min_block_size=` / `target_max_block_size=` 覆盖。）

---

## 2. 背压：限上限 vs 关策略

Ray 默认开启反压：下游慢会反压上游（如降低读取速度）。两种处理方式：

**A. 设 in-flight 内存上限（推荐默认）** —— 防止读入远快于消费导致对象堆积、worker OOM：

```python
from ray.data import ExecutionResources
ctx.execution_options.resource_limits = ExecutionResources(object_store_memory=int(1.8e12))
```

**B. 彻底关闭反压策略** —— 仅在确认下游不会被压垮时使用：

```python
ctx.set_config("backpressure_policies.enabled", [])
```

`setup_data_context()` 默认走 A；传 `disable_backpressure=True` 走 B。

**排查谁在反压**：Dashboard 搜索 `backpressured`，定位是哪个算子导致的反压。

---

## 3. repartition：约束 block 个数

block size 只能全局设置；当 pipeline 中数据量波动较大时，用 `repartition` 在局部决定
block 个数：

```python
# 上下游算子之间
ds = ds.repartition(target_num_rows_per_block=200)
```

**写出前尤其重要**：推理完成后若 block 过多，写出会出现严重长尾（例如总计 6h 的任务，
最后写出占了 1h）。写出前 `repartition` 聚合数据，减少写出 Task 次数。

### 3.1 input/output_repartition_rows 配置映射

JSON recipe 通过 `(_JSON_KEY_MAP)` 扁平化映射到 `FsRayConfig` dataclass 字段。映射关系：

| JSON 路径 | config 字段 | 是否生效 |
|-----------|------------|---------|
| `input.repartition_rows` | `input_repartition_rows` | ✅ |
| `output.repartition_rows` | `output_repartition_rows` | ✅ |
| `compute.input_repartition_rows` | 无映射 | ❌ 不生效 |

**常见错误**：把 `input_repartition_rows` 放在 `compute` 组下（如 captioner recipe 早期版本），
映射表中没有 `("compute", "input_repartition_rows")` 条目，该配置会被静默忽略，
实际使用的是 dataclass 默认值 `1024`（`fs_ray_config.py:277`）。

环境变量覆盖：`FS_RAY_INPUT_REPARTITION_ROWS` / `FS_RAY_OUTPUT_REPARTITION_ROWS`
也可直接设置，优先级高于 JSON recipe。

KsDatasetSource 内部也使用 `config.input_repartition_rows` 作为 `rows_per_block`
（`fs_ray_pipeline.py:136`），此时 pipeline 的 `repartition()` 调用会被跳过
（避免双重 repartition）。

### 3.2 DATASET_ENV 场景下的 repartition 陷阱

当 Hermes 平台通过 `DATASET_ENV` 注入 `hdfsFolder`（→ `input_path`）时，
`build_source()` 按 `input_path` 优先走 `_json_source` / `_parquet_source`，
recipe 中残留的 `input.namespace + input.dataset` 不影响 source 选择。

**旧版 bug**（已修复）：repartition 判断逻辑只检查 `config.input_namespace and config.input_dataset`
是否非空，当 DATASET_ENV 注入 `input_path` 但 recipe 仍保留 namespace/dataset 时，
会误判为 KsDatasetSource 而跳过 repartition。日志表现为：

```
[pre] KsDatasetSource already repartitioned (rows_per_block=20)
```

**修复**：改为与 `build_source()` 一致的优先级判断——只有当实际走 KsDatasetSource
（`use_dataset_id` 或无 `input_path` + 有 namespace+dataset）时才跳过 repartition。

```python
# 修复后逻辑（fs_ray_pipeline.py）
_used_ksdataset_source = (
    config.use_dataset_id
    or (not config.input_path and config.input_namespace and config.input_dataset)
)
if config.input_repartition_rows > 0 and _used_ksdataset_source:
    # KsDatasetSource 已内部 repartition，跳过
    ...
elif config.input_repartition_rows > 0:
    # json/parquet source，需要显式 repartition
    ds = ds.repartition(target_num_rows_per_block=config.input_repartition_rows)
```

---

## 4. 提高 parquet 读取速度

读取 parquet **必须**指定并发，否则很慢。文件个数决定 ReadParquet 并发，文件越多越快。

```python
ds = ray.data.read_parquet(
    hdfs_path,
    concurrency=2000,
    override_num_blocks=2000,
)
```

---

## 5. Checkpoint：断点续跑（默认开启）

长耗时任务默认开启 checkpoint，失败重跑时跳过已完成行而非全量重算。

```python
from ray.data import CheckpointConfig
ctx.checkpoint_config = CheckpointConfig(
    id_column="blobstore_id",                    # 行唯一标识
    override_filesystem=hdfs_fs,                  # HDFS 文件系统
    checkpoint_path=checkpoint_path,
    delete_checkpoint_on_success=True,           # 成功后清理
    checkpoint_read_override_num_blocks=2000,    # 提高 checkpoint 读取并发
)
```

用 `make_checkpoint_config(...)` 构造、`setup_data_context(checkpoint=...)` 注入即可。
**不需要续跑时**显式传 `setup_data_context(checkpoint=None)` 关闭。

---

## 6. 固定集群关闭 autoscaler

固定集群不需要自动扩缩容，关闭可减少调度开销：

```python
ctx.use_cluster_autoscaler = False
# 或环境变量
import os; os.environ["RAY_DATA_DISABLE_CLUSTER_AUTOSCALER"] = "1"
```

（`setup_data_context(fixed_cluster=True)` 默认即关闭。）

---

## 7. 动态调整算子并发（kconf）

运行过程中需动态调整并发的场景，可生成 kconf 控制：

```python
ctx.execution_config_store_type = "kconf"
ctx.enable_dynamic_execution_config_sync = True
```

---

## 8. Task chain：相同资源需求设相同并发

对资源需求相同的 Task，设置相同并发可让它们 chain 在一起，减少调度与序列化开销。

---

## 9. worker-only 调度：保持 head 空闲

把算子钉在 worker 节点，head 节点留给调度循环与 API server：

```python
from pipeline.runtime_setup import WORKER_ONLY_REMOTE_ARGS  # {"resources": {"worker-1": 0.01}}

ds = ds.map_batches(MyGpuMapper, num_gpus=1, resources={"worker-1": 0.01}, ...)
ds = ray.data.read_parquet(paths, ray_remote_args=WORKER_ONLY_REMOTE_ARGS, ...)
# 注意：write_kafka 不接受 resources= 直传，需用 ray_remote_args 包裹
ds.write_kafka(..., ray_remote_args=WORKER_ONLY_REMOTE_ARGS)
```

也可在 MaaS 的 `START_ARGS` 里把 head 的 `num-cpus` / `num-gpus` 设为 0，
见 `docs/02-product/maas-deployment.md`。

---

## 10. 提高单算子吞吐：算子内多线程

对「下载 + 解码 + 预处理」这类 I/O / C 扩展密集的算子，与其完全依赖 Task/Actor 调度，
不如在算子内用线程池并行（这些操作大多释放 GIL）：

- `torchcodec.VideoDecoder` 是 C++ 实现，decode 期间释放 GIL
- `transformers.AutoVideoProcessor` 的 resize/normalize 走 numpy/PIL C 扩展，释放 GIL
- 网络下载本身是 I/O 等待，不受 GIL 影响

```python
from concurrent.futures import ThreadPoolExecutor, as_completed

with ThreadPoolExecutor(max_workers=num_workers) as executor:
    futures = {executor.submit(_process_one, (idx, si)): idx
               for idx, si in enumerate(slice_info_list)}
    results = {}
    for future in as_completed(futures):
        result = future.result()
        results[result["segment_idx"]] = result

preprocessed_items = [results[i] for i in range(len(slice_info_list))]
```

> 线程数要设合理阈值：过大会同时下载过多视频，存在内存撑爆风险。

---

## 11. 内存安全：浅拷贝 row

Ray Data 调度时同一个 row 可能被多处引用（batch 内共享、重试机制）。若直接修改透传的
dict，row 在外层仍被引用、内存不会释放，且会污染调用方原始 dict。

框架默认 `copy_input=True`（浅拷贝），使算子处理完后能尽快释放内存。**不要**在算子里
直接改入参 dict 而绕过这一机制。另一类内存风险是**单条数据过大**（如超大单视频导致内存
突增），需要在业务侧做大小保护。

---

## 12. 终端写出语义：慎用 materialize()

`write_kafka` / `write_*` 是**终端算子**。注意：

- 在写出之前加 `ds.materialize()` 会**等前面所有 mapper 执行完**并把结果存内存，
  下游 mapper 才继续。这会造成内存压力大、端到端结果产出延迟。
- 默认**不要**在 sink 前 `materialize()`，除非确实需要「全部完成再写」的语义。

---

## 13. 算子融合（Operator Fusion）

Ray Data 在物理计划优化阶段自动融合相邻的 TaskPoolMapOperator，减少中间物化开销。
融合逻辑在 `ray/data/_internal/logical/rules/operator_fusion.py`。

### 三步融合流程

1. **Pass 1**：融合 `MapBatches → StreamingRepartition`（`_fuse_streaming_repartition_operators_in_dag`）
2. **Pass 2**：融合 `MapOperator → MapOperator` back-to-back（`_fuse_map_operators_in_dag`）
3. **Pass 3**：融合 `MapOperator → AllToAllOperator`（如 RandomShuffle、Repartition(shuffle=True)）

### StreamingRepartition 融合硬规则

`_can_fuse()` 中有硬性约束：**StreamingRepartition 只允许 MapBatches 作为上游融合**。

```python
# operator_fusion.py:276
if isinstance(down_logical_op, StreamingRepartition):
    if not (isinstance(up_logical_op, MapBatches) ...):
        return False  # 上游不是 MapBatches，拒绝融合
```

- `ReadArrowJSON(Read) → StreamingRepartition`：Read 不是 MapBatches，**不融合**
- `MapBatches(_rename_batch) → StreamingRepartition`：上游是 MapBatches，**可以融合**

**为什么 Read → StreamingRepartition 不融合？** 因为融合后 map_batches 的 task 数等于
Read 输出 block 数（≤ StreamingRepartition 输出 block 数），如果 StreamingRepartition
的目的是拆小 block 增加并行度，融合会阻止这一效果。源码注释见
`_fuse_streaming_repartition_operators_in_dag` docstring。

### input_map 的融合桥梁效应

当 recipe 配置了 `input.input_map` 时，pipeline 会调用 `_apply_input_map()`
（`fs_ray_pipeline.py:158`），内部产生 `_rename_batch(MapBatches)` 算子。
这个 MapBatches 充当融合"桥梁"：

**有 input_map 时**（如 Captioner + QG with input_map）：
```
ReadArrowJSON → _rename_batch(MapBatches) → StreamingRepartition
                              ↑ Pass 1 融合 ↓
ReadArrowJSON → [_rename_batch + StreamingRepartition]
                     ↑ Pass 2 融合 ↓
[ReadArrowJSON → _rename_batch → StreamingRepartition]  ← 三合一
```

**无 input_map 时**：
```
ReadArrowJSON(Read) → StreamingRepartition  ← 不融合，保持两个独立算子
```

### 实际案例对比

| | QG（无 input_map） | Captioner（有 input_map） |
|---|---|---|
| read_op_min_num_blocks | 1 | 8 |
| Read 输出 blocks | 1 | 8 |
| 执行计划 | `ReadArrowJSON` → `StreamingRepartition` (分开) | `ReadArrowJSON->_rename_batch->StreamingRepartition` (三合一) |

注意：QG recipe 本身没有配 `input_map`，但 kconf 动态配置可能注入 input_map。
判断是否有 `_rename_batch` 应以执行计划日志为准。

### StreamingRepartition 并行度与 Task 提交

**并行度**：`StreamingRepartition` 不预设固定输出 block 数，由 `target_num_rows_per_block`
和总行数决定：

```
num_output_blocks ≈ ceil(total_rows / target_num_rows_per_block)
```

**Task 提交时机**（`map_operator.py:_add_input_inner`）：

- **非 strict 模式**（默认）：当 bundler 累积行数 ≥ `batch_size` 时提交一个 task
- **strict 模式**：当 bundler 累积行数 ≥ `target_num_rows_per_block` 时提交
- **最终 flush**：上游发完所有输入后，`all_inputs_done()` 将剩余数据作为最后一个 task 提交

### read_op_min_num_blocks 对融合的影响

`read_op_min_num_blocks`（recipe `compute` 组或 `FS_RAY_READ_OP_MIN_NUM_BLOCKS` 环境变量）
作为 `override_num_blocks` 传给 `read_json` / `read_parquet`，决定 Read 算子输出 block 数。

当 Read 输出多 block（如 8）时，下游 `_rename_batch` 和 `StreamingRepartition` 的
并行度一致，Ray 优化器更容易将它们融合。但这不是融合的前提条件——融合只要求上游
逻辑算子类型匹配（MapBatches），不要求 block 数一致。

### 融合的利弊

| | 融合 | 不融合 |
|---|---|---|
| 中间物化 | 无（数据在 task 内存中直传） | 有（写出→读入 object store） |
| 调度开销 | 少（一个 task 做多件事） | 多（每个算子独立调度） |
| 并行度 | 受限于上游 block 数 | StreamingRepartition 可独立拆分增加并行度 |
| 内存 | 单 task 内处理更多数据 | 每个 task 处理更少数据，内存压力小 |

---

## 问题排查清单

| 现象 | 排查方向 |
|------|----------|
| GPU 利用率低 | GPU 算子并发不够（试 `num_gpus=0.x` 提并发）；或上游 block 跟不上消费 |
| 吞吐上不去 | 看调度耗时（Scheduling Loop），>1min 则调 block size / repartition |
| 某算子阻塞 | 看各算子 `Queued blocks`，最大的那个就是瓶颈（如 VideoPreprocessMapper） |
| 内存持续上涨 | 单条数据过大 / row 透传未释放（见 §11） |
| 写出长尾严重 | 写出前 repartition 聚合 block（见 §3） |
| 有反压 | Dashboard 搜 `backpressured` 定位算子（见 §2） |
| Driver 日志膨胀/刷屏 | Actor worker 的 stdout 被 Ray 转发到 driver 日志（见下方） |
| repartition 未生效 | 检查 JSON 配置路径：`input.repartition_rows` ✅ / `compute.input_repartition_rows` ❌；检查 DATASET_ENV 注入 input_path 后是否误判为 KsDatasetSource（见 §3.1/3.2） |
| 算子未融合 | 检查执行计划：`Read→StreamingRepartition` 不融合是正常的（Read 非 MapBatches）；加 `input_map` 产生 `_rename_batch` 可桥接融合（见 §13） |

### 接入 Prometheus / Grafana 获取更多指标

在 MaaS 启动的「高级设置」中增加环境变量（详见 `docs/02-product/maas-deployment.md`）：

### Driver 日志膨胀 / Actor stdout 刷屏

**现象**：`job-driver-raysubmit_*.log` 快速膨胀到 GB 级，大部分行是 `(MapWorker(...) pid=xxx, ip=xxx) ` 空行。

**根因**：Ray Worker 的 stdout 会被自动转发到 driver 日志。如果 Actor 内的第三方代码（如 FeatureServiceServer）有 `print()` / `sys.stdout.write()` 输出空行，全部会刷到 driver 日志。

**修复**：
1. Actor `_setup()` 中 `sys.stdout = open(os.devnull, "w")` — 从源头消除
2. Driver 端 `ray.init()` 前设 `RAY_DEDUP_LOGS_SKIP_REGEX=r"^\s*$"` — 兜底过滤空行

**详细排查过程与 Ray 日志转发机制**：见 `docs/fs_ray_actor_v2_validation.md` 和 `docs/fs_ray_migration_guide.md` §7.3。

```
RAY_GRAFANA_IFRAME_HOST = https://search-keling-ray-grafana.corp.kuaishou.com
RAY_METRICS_EXPORT_MODE = remote_write
RAY_PROMETHEUS_HOST = http://10.81.0.157:9090
RAY_METRICS_REMOTE_WRITE_ENDPOINT = http://10.81.0.157:9090/api/v1/write
RAY_METRICS_PUSH_INTERVAL_MS = 60000
RAY_GRAFANA_HOST = http://10.81.0.157:3000
RAY_CLUSTER_NAME = <给你的集群起个可识别的名字>
```
