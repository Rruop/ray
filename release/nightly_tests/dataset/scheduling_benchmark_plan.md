# Scheduling Loop 优化 A/B Benchmark 测试方案

## 目标

通过增量对比，量化每个调度优化的独立贡献和叠加收益。

## 优化项及控制开关

| 优化 | 环境变量 | 默认值 | 关闭方式 |
|------|---------|--------|---------|
| Dynamic ray.wait timeout | RAY_DATA_ENABLE_DYNAMIC_RAY_WAIT_TIMEOUT | True | =0 |
| Cap completions per step | RAY_DATA_MAX_COMPLETIONS_PER_STEP | 512 | =0 (无上限) |
| Capacity-based dispatch | RAY_DATA_ENABLE_CAPACITY_BASED_DISPATCH | True | =0 |
| Perf metrics 采样 | RAY_DATA_ENABLE_PERF_METRICS | 已有 | =0 |

## 测试矩阵（增量对比）

每个实验在前一步基础上加一个优化，隔离增量收益。

| 实验 | dynamic_wait | max_completions | capacity_dispatch | perf_metrics | 对比目标 |
|------|:---:|:---:|:---:|:---:|------|
| A 基线 | 0 | 0 | 0 | 0 | — |
| B | 1 | 0 | 0 | 0 | B vs A → dynamic wait 收益 |
| C | 1 | 512 | 0 | 0 | C vs B → cap completions 增量 |
| D | 1 | 512 | 1 | 0 | D vs C → capacity dispatch 增量 |
| E | 1 | 512 | 1 | 1 | E vs D → perf 开销量化 |

## 并行度维度

每组实验 × 3 个并行度: 1000 / 1500 / 2000

共 5 × 3 = 15 次运行

## 关键指标

| 指标 | 含义 | 来源 |
|------|------|------|
| time_total_s | 端到端总耗时 | DatasetStatsSummary |
| streaming_exec_schedule_s | 调度循环总耗时 | DatasetStatsSummary |
| streaming_exec_schedule_avg_s | 每步调度平均耗时 | DatasetStatsSummary |
| streaming_exec_schedule_max_s | 每步调度最大耗时 | DatasetStatsSummary |
| streaming_exec_dispatch_s | 纯 dispatch 总耗时 | DatasetStatsSummary |
| streaming_exec_dispatch_avg_s | 平均 dispatch 耗时 | DatasetStatsSummary |
| streaming_exec_ray_wait_s | ray.wait 总耗时 | DatasetStatsSummary |
| streaming_exec_ray_wait_avg_s | 平均 ray.wait 耗时 | DatasetStatsSummary |
| streaming_exec_inter_step_s | 步间间隔总耗时 | DatasetStatsSummary |
| streaming_exec_inter_step_avg_s | 平均步间间隔 | DatasetStatsSummary |
| streaming_exec_on_data_ready_s | on_data_ready 总耗时 | DatasetStatsSummary |
| scheduling_overhead.phases.scheduling_ms | raylet 侧调度延迟 | OperatorStatsSummary |
| scheduling_overhead.phases.worker_startup_ms | worker 启动耗时 | OperatorStatsSummary |

## Benchmark 脚本

使用 `release/nightly_tests/dataset/worker_scaling_benchmark.py`

参数说明:
- `--num-workers`: 并行度 (1000/1500/2000)
- `--worker-type tasks`: 用 task 模式 (无状态、启动快、调度开销更纯粹)
- `--num-scalar-cols 2`: 2 个标量列
- `--num-array-cols 2`: 2 个数组列
- `--blocks-per-worker 20`: 每个 worker 20 个 block (总 60s+ 运行时间确保指标采集)

## 执行命令

### 实验A: 基线 (全关)

```bash
RAY_DATA_ENABLE_DYNAMIC_RAY_WAIT_TIMEOUT=0 \
RAY_DATA_MAX_COMPLETIONS_PER_STEP=0 \
RAY_DATA_ENABLE_CAPACITY_BASED_DISPATCH=0 \
RAY_DATA_ENABLE_PERF_METRICS=0 \
python3 worker_scaling_benchmark.py \
  --num-workers 1000 --worker-type tasks \
  --num-scalar-cols 2 --num-array-cols 2 \
  --blocks-per-worker 20
```

### 实验B: +Dynamic ray.wait timeout

```bash
RAY_DATA_ENABLE_DYNAMIC_RAY_WAIT_TIMEOUT=1 \
RAY_DATA_MAX_COMPLETIONS_PER_STEP=0 \
RAY_DATA_ENABLE_CAPACITY_BASED_DISPATCH=0 \
RAY_DATA_ENABLE_PERF_METRICS=0 \
python3 worker_scaling_benchmark.py \
  --num-workers 1000 --worker-type tasks \
  --num-scalar-cols 2 --num-array-cols 2 \
  --blocks-per-worker 20
```

### 实验C: +Cap completions per step

```bash
RAY_DATA_ENABLE_DYNAMIC_RAY_WAIT_TIMEOUT=1 \
RAY_DATA_MAX_COMPLETIONS_PER_STEP=512 \
RAY_DATA_ENABLE_CAPACITY_BASED_DISPATCH=0 \
RAY_DATA_ENABLE_PERF_METRICS=0 \
python3 worker_scaling_benchmark.py \
  --num-workers 1000 --worker-type tasks \
  --num-scalar-cols 2 --num-array-cols 2 \
  --blocks-per-worker 20
```

### 实验D: +Capacity-based dispatch

```bash
RAY_DATA_ENABLE_DYNAMIC_RAY_WAIT_TIMEOUT=1 \
RAY_DATA_MAX_COMPLETIONS_PER_STEP=512 \
RAY_DATA_ENABLE_CAPACITY_BASED_DISPATCH=1 \
RAY_DATA_ENABLE_PERF_METRICS=0 \
python3 worker_scaling_benchmark.py \
  --num-workers 1000 --worker-type tasks \
  --num-scalar-cols 2 --num-array-cols 2 \
  --blocks-per-worker 20
```

### 实验E: +Perf metrics (全开默认)

```bash
RAY_DATA_ENABLE_DYNAMIC_RAY_WAIT_TIMEOUT=1 \
RAY_DATA_MAX_COMPLETIONS_PER_STEP=512 \
RAY_DATA_ENABLE_CAPACITY_BASED_DISPATCH=1 \
RAY_DATA_ENABLE_PERF_METRICS=1 \
python3 worker_scaling_benchmark.py \
  --num-workers 1000 --worker-type tasks \
  --num-scalar-cols 2 --num-array-cols 2 \
  --blocks-per-worker 20
```

### 其他并行度

将 `--num-workers` 替换为 1500 / 2000，其余不变。

---

## 版本兼容性详细分析

### 背景

在已运行的 KubeRay 集群上，只更换 head 节点的 Python wheel 包来测试新 feature，
会面临版本兼容性问题。以下完整分析版本检查的代码逻辑、各检查点、以及换包后的影响。

### 版本信息的生命周期

```
集群启动 (ray start --head)
  │
  ├─ Node.__init__ (head=True)
  │    └─ ray_usage_lib.put_cluster_metadata(gcs_client)
  │         └─ compute_version_info() → (ray.__version__, python_version)
  │         └─ 写入 GCS KV store (cluster_metadata)
  │
  │  版本号在集群启动时固化到 GCS，后续所有连入的进程都跟这个值比对。
  │
  ├─ raylet C++ 进程启动 (pip 安装的二进制)
  │    └─ 此进程不会被 pip install 替换，除非重启集群
  │
  └─ Python worker 进程
       └─ import ray → 加载 _raylet.so (Cython C++ 扩展)
       └─ 此 .so 文件会被 pip install 替换
```

### 版本检查的 4 个检查点

#### 检查点 1: Worker 节点启动 (`ray start`)

```python
# node.py:389
if not head and not connect_only:
    self.check_version_info()
```

当新 worker 节点执行 `ray start` 连入集群时，会调用 `check_version_info()`
与 GCS 中存储的版本比对。`raise_on_mismatch=True`（默认），不匹配直接 raise。

**影响**: 如果 head 换了新 wheel 但 GCS 仍是旧版本号，新 worker 节点启动会失败。

#### 检查点 2: Driver 连接 (`ray.init`)

```python
# worker.py:2608-2616
try:
    node.check_version_info()
except Exception as e:
    if mode == SCRIPT_MODE:
        raise e          # driver 模式直接 raise
    elif mode == WORKER_MODE:
        # worker 模式 publish 错误给 driver，不直接 raise
        ray._private.utils.publish_error_to_driver(...)
```

Driver 执行 `ray.init(address="auto")` 时走 SCRIPT_MODE，版本不匹配直接 raise RuntimeError。

#### 检查点 3: Ray Client 连接

```python
# util/client/__init__.py:232-238
def _check_versions(self, conn_info, ignore_version):
    ignore_version = ignore_version or ("RAY_IGNORE_VERSION_MISMATCH" in os.environ)
    check_version_info(
        conn_info,
        "Ray Client",
        raise_on_mismatch=not ignore_version,  # 可降级为 warning
        python_version_match_level="minor",
    )
```

**唯一支持 `RAY_IGNORE_VERSION_MISMATCH` 的检查点**。设置此环境变量后，
版本不匹配只会 log warning，不会 raise。

#### 检查点 4: CLI 命令 (`ray status`, `ray job submit`)

```python
# scripts/scripts.py:76
def _check_ray_version(gcs_client):
    cluster_metadata = ray_usage_lib.get_cluster_metadata(gcs_client)
    if cluster_metadata and cluster_metadata["ray_version"] != ray.__version__:
        raise RuntimeError(
            f"Ray version mismatch: cluster has Ray version "
            f"{cluster_metadata['ray_version']} "
            f"but local Ray version is {ray.__version__}"
        )
```

CLI 命令直接比较版本号，不支持任何跳过机制。

#### 检查点汇总

| 检查点 | 触发场景 | 代码位置 | RAY_IGNORE_VERSION_MISMATCH | 行为 |
|--------|---------|---------|:---:|------|
| Worker 启动 | `ray start` (新节点) | `node.py:389` | ❌ | 直接 raise |
| Driver 连接 | `ray.init(address="auto")` | `worker.py:2610` | ❌ | 直接 raise |
| Ray Client | `ray.client().connect()` | `client/__init__.py:234` | ✅ | 可降级 warning |
| CLI 命令 | `ray status` / `ray job submit` | `scripts.py:76` | ❌ | 直接 raise |

### 核心检查逻辑 (`check_version_info`)

```python
# ray/_private/utils.py:1318
def check_version_info(
    cluster_metadata,
    this_process_address,
    raise_on_mismatch=True,        # 控制 raise 还是 warning
    python_version_match_level=None # "minor" 或 "patch"
):
    # 从环境变量读取 Python 版本匹配精度
    if python_version_match_level is None:
        python_version_match_level = os.environ.get(
            "RAY_DEFAULT_PYTHON_VERSION_MATCH_LEVEL", "patch"
        )

    cluster_version_info = (
        cluster_metadata["ray_version"],      # 来自 GCS
        cluster_metadata["python_version"],   # 来自 GCS
    )
    my_version_info = compute_version_info()  # 当前进程的版本

    # Ray 版本必须完全匹配
    ray_matches = cluster_version_info[0] == my_version_info[0]

    # Python 版本按 match_level 匹配
    if python_version_match_level == "patch":
        python_matches = cluster_version_info[1] == my_version_info[1]
    elif python_version_match_level == "minor":
        # 只比较 major.minor，忽略 patch 差异
        python_matches = my_version_info[1].split(".")[:2] == \
                         cluster_version_info[1].split(".")[:2]

    # 版本匹配 → 正常继续
    if ray_matches and python_matches:
        # patch 不一致时 log warning
        if not python_full_matches:
            logger.warning(...)

    # 版本不匹配 → 根据 raise_on_mismatch 决定行为
    else:
        error_message = f"Version mismatch: ..."
        if raise_on_mismatch:
            raise RuntimeError(error_message)  # 检查点 1、2 走这里
        else:
            logger.warning(error_message)       # 检查点 3 (ignore_version=True) 走这里
```

### 换 Head Wheel 包后的实际影响

以集群版本 `2.55.2`、新 wheel `2.55.1` 为例：

```
集群架构:
  GCS (ray_version="2.55.2")         ← 启动时写入，不可变
  Head raylet (C++ 二进制 2.55.2)     ← pip install 不会替换运行中的进程
  Head Python (2.55.1 新 wheel)       ← pip install 替换了 .py 和 _raylet.so
  远程 Worker Python (2.55.2 旧 wheel) ← 未更换
```

#### 问题 1: Python 版本检查

| 进程 | 本地版本 | GCS 版本 | 检查结果 |
|------|---------|---------|---------|
| Driver (head) | 2.55.1 | 2.55.2 | ❌ 不匹配 → raise |
| Head worker | 2.55.1 | 2.55.2 | ❌ 不匹配 → raise |
| 远程 worker | 2.55.2 | 2.55.2 | ✅ 匹配 |

**解决**: monkey-patch `ray._private.utils.check_version_info = lambda *a, **kw: None`
跳过所有 Python 侧版本检查。

#### 问题 2: DataContext 序列化兼容性 (核心问题)

Driver 进程用新 wheel 的 DataContext（包含 `enable_capacity_based_dispatch` 等新字段），
会序列化后传播给 worker 进程：

```python
# context.py — DataContext 传播机制
class DataContext:
    @staticmethod
    @contextlib.contextmanager
    def _worker_context(context):
        """This is used internally by Dataset to propagate the driver
        context to remote workers used for parallelization."""
        global _default_context
        prev = _default_context
        _default_context = context    # 直接替换 worker 的 context 单例
        yield
        _default_context = prev
```

传播流程：
1. Driver 用新 DataContext（含新字段）提交 task
2. Ray 内部通过 cloudpickle 序列化 DataContext
3. Worker 进程反序列化 DataContext

**远程 worker 用旧 wheel**：旧 DataContext dataclass **没有** `enable_capacity_based_dispatch` 字段
→ Python dataclass 反序列化时字段不匹配 → **AttributeError / crash**

**Head worker 用新 wheel**：新 DataContext dataclass **有** 这些字段 → 反序列化正常 → **没问题**

#### 问题 3: C++ 通信层

```
Head driver (_raylet.so 2.55.1)  ←→  Head raylet (C++ 2.55.2)
远程 worker (_raylet.so 2.55.2)  ←→  远程 raylet (C++ 2.55.2)
```

- 2.55.1→2.55.2 是小版本差异，C++ gRPC 协议通常兼容
- 实际 observed: driver 连接 head raylet 成功，但远程 worker 出现 `connection error code 2`
- 原因是远程 worker crash（DataContext 反序列化失败）后，raylet 收到断连信号

### 解决方案

#### 方案 1: 所有节点安装新 wheel (推荐，彻底解决)

在所有 worker 节点上安装新 wheel 包：
```bash
# 每个 worker 节点
pip install /path/to/ray-2.55.1+kuaishou.xxx.whl --force-reinstall
```

**优点**: Python 版本检查、DataContext 序列化、C++ 通信全部兼容
**缺点**: 需要逐节点操作，KubeRay 环境 pod 可能被重建

#### 方案 2: 重启集群 (彻底替换)

```bash
ray stop
pip install /path/to/new-wheel.whl --force-reinstall
ray start --head ...
```

**优点**: raylet C++ 进程也更新，GCS 版本号刷新，完全一致
**缺点**: 集群需停服

#### 方案 3: Task 只跑在 Head 节点 (不改远程 worker)

在 benchmark 脚本中约束 task 只调度到 head 节点：
```python
# DataContext 传播给 head 上的 worker，新 wheel 能正常反序列化
ds = ds.map_batches(udf, num_cpus=0.01, resources={"head": 0.01})
```

- Head 有 19 CPU，`num_cpus=0.01` 可虚拟 ~1900 并发
- 调度指标（driver 侧）不受限制，worker 侧只执行简单 lambda
- **无需修改远程 worker，DataContext 序列化无问题**

**优点**: 无需任何集群变更
**缺点**: Head 资源有限（19 CPU），但调度指标与 CPU 数无关

#### 方案 4: 加 RAY_IGNORE_VERSION_MISMATCH 支持 (需改代码)

当前只有 Ray Client 模式支持此环境变量。建议在 `check_version_info()` 入口加：

```python
# ray/_private/utils.py check_version_info() 开头
if "RAY_IGNORE_VERSION_MISMATCH" in os.environ:
    return
```

这样所有检查点（driver 连接、worker 启动、CLI）都支持跳过版本检查。
**注意**: 这只解决 Python 版本检查问题，**不解决 DataContext 序列化兼容性**。
仍需确保 worker 的 Python 包能反序列化 driver 传来的 DataContext。

### 方案选择决策树

```
能否重启集群？
├─ 是 → 方案 2（重启集群，最干净）
└─ 否
    ├─ 能否在所有 worker 节点装新 wheel？
    │   ├─ 是 → 方案 1（全节点换包）
    │   └─ 否
    │       ├─ head 资源够用？(19 CPU + num_cpus=0.01)
    │       ├─ 是 → 方案 3（只跑 head）
    │       └─ 否 → 必须方案 1 或 2
    └─ 需要远程 worker？
        ├─ 否 → 方案 3
        └─ 是 → 必须方案 1 或 2
```

---

## 前置条件

1. 集群所有节点 (head + worker) 安装新版 wheel 包，或采用方案 3 只跑 head
2. 版本检查跳过: `RAY_IGNORE_VERSION_MISMATCH=1`（代码支持后）或 monkey-patch
3. 集群资源: 1318 CPU, 47 节点

## 结果分析

### 增量收益计算

- Dynamic wait: `(sched_loop_avg_A - sched_loop_avg_B) / sched_loop_avg_A × 100%`
- Cap completions: `(sched_loop_avg_B - sched_loop_avg_C) / sched_loop_avg_B × 100%`
- Capacity dispatch: `(sched_loop_avg_C - sched_loop_avg_D) / sched_loop_avg_C × 100%`
- Perf 开销: `(time_total_D - time_total_E) / time_total_D × 100%`

### 关注的对比维度

1. `ray_wait_avg`: 动态 timeout 应显著降低 (尤其高并发)
2. `dispatch_avg`: capacity dispatch 应降低单次 dispatch 耗时
3. `sched_loop_avg`: 三个优化叠加后总调度循环耗时下降
4. `inter_step_avg`: perf 开关对步间间隔的影响
5. `time_total_s`: 端到端总耗时变化
6. 并行度趋势: 各优化在不同并发下的收益曲线

### 预期结论

- 并发越高，dynamic wait + cap completions 收益越大 (burst 缓解)
- Capacity dispatch 在多 operator 场景下收益更明显
- Perf metrics 应有可量化的调度开销 (采样/上报线程)
