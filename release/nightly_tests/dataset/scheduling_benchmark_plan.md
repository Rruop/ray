# Scheduling Loop 优化 A/B Benchmark 测试方案 v2

## 更新背景

### 上一轮测试问题

1. **time_total 被 spilled IO 严重干扰**：spilled GB 0~1656 差异，端到端时间不可作为调度性能指标
2. **per-step avg 有误导性**：dynamic_wait 把大步拆小步，avg_step 下降但 per-task 开销不变甚至更高
3. **单 operator 场景无法体现多 operator 价值**：dynamic_wait 的防饿死收益需要多 operator 才能体现
4. **p50/p90 调度循环 = 0**：分位数指标未采集
5. **detail=True API 超时**：1000 并发时 dashboard API 扛不住

### 本轮改进

1. **降低 blocks_per_worker**：从 100 降到 10，避免 object store spill 污染指标
2. **增加多 operator 实验**：4 个 chained map_batches，模拟真实 pipeline
3. **新增 num_returns 对比**：核心验证 `RAY_DATA_RAY_WAIT_NUM_RETURNS` 的收益
4. **废弃 dynamic_wait 实验**：已删除该特性，不再测试
5. **per-block 作为主对比维度**：消除步数差异

## 优化项及控制开关

| 优化 | 环境变量 | 默认值 | 说明 |
|------|---------|--------|------|
| ray.wait num_returns | RAY_DATA_RAY_WAIT_NUM_RETURNS | 1024 | 限制 ray.wait 的 num_returns，避免 O(N) 锁全量扫描 |
| Cap completions per step | RAY_DATA_MAX_COMPLETIONS_PER_STEP | 512 | 每步 on_data_ready 处理上限 |
| Capacity-based dispatch | RAY_DATA_ENABLE_CAPACITY_BASED_DISPATCH | True | 按下游 capacity 分发 task |
| Perf metrics 采样 | RAY_DATA_ENABLE_PERF_METRICS | 已有 | 采样开销量化 |

**已废弃**：`RAY_DATA_ENABLE_DYNAMIC_RAY_WAIT_TIMEOUT`（已删除，timeout 固定 0.1s）

## 测试矩阵

### 场景一：单 operator，隔离调度开销

| 实验 | num_returns | max_completions | capacity_dispatch | perf_metrics | 对比目标 |
|------|:---:|:---:|:---:|:---:|------|
| A 基线 | -1 (无上限=原行为) | -1 (无上限) | 0 | 0 | — |
| B | 1024 (默认) | -1 | 0 | 0 | B vs A → num_returns 收益 |
| C | 1024 | 512 | 0 | 0 | C vs B → cap completions 增量 |
| D | 1024 | 512 | 1 | 0 | D vs C → capacity dispatch 增量 |
| E | 1024 | 512 | 1 | 1 | E vs D → perf 开销量化 |

### 场景二：多 operator (4 个)，模拟真实 pipeline

| 实验 | num_returns | max_completions | capacity_dispatch | num_operators | 对比目标 |
|------|:---:|:---:|:---:|:---:|------|
| F 基线 | -1 | -1 | 0 | 4 | — |
| G | 1024 | -1 | 0 | 4 | G vs F → num_returns 收益 (多 op) |
| H | 1024 | 512 | 0 | 4 | H vs G → cap completions (多 op) |
| I | 1024 | 512 | 1 | 4 | I vs H → capacity dispatch (多 op) |

**num_returns=-1 的实现**：设 `RAY_DATA_RAY_WAIT_NUM_RETURNS=-1`，代码中 `min(-1, len(active_tasks))` 仍传 -1 给 ray.wait，等价于 `num_returns=len(active_tasks)` 原行为。

> 注意：ray.wait 的 num_returns 必须 >= 1，-1 会导致报错。需要代码中处理：`num_returns = len(active_tasks) if ray_wait_num_returns < 0 else min(ray_wait_num_returns, len(active_tasks))`。

## 并行度

- 场景一：num-workers=4000, num-cpus=0.25 → 需 1000 CPU（集群 3257 够）
- 场景二：num-workers=4000, num-operators=4 (每 op 1000), num-cpus=0.25 → 需 1000 CPU

## 数据量控制

- `TARGET_BLOCK_SIZE_BYTES=4MB`：减小 block 大小，降低总数据量
- `--blocks-per-worker 50`：总 blocks = 50 × 4000 = 200K，总数据量 ~800GB
- 每 task 约 50-100ms，每 worker 跑约 5-10s，足够观察持续调度
- `RAY_DEFAULT_OBJECT_STORE_MEMORY_PROPORTION=0.4`：object store ~1.3TB，不会 spill
- `--num-cpus 0.25`：每 worker 占 0.25 CPU，4000 workers → 1000 CPU（3257 够）
- `--disable-backpressure`：关闭所有反压策略，避免反压干扰调度性能测量

## 关键指标

### 主指标（per-block，公平对比）

| 指标 | 计算方式 | 含义 |
|------|---------|------|
| sched_per_block_ms | total_scheduling_runtime / num_blocks × 1000 | 每个 block 的调度开销 |
| dispatch_per_block_ms | total_dispatch_runtime / num_blocks × 1000 | 每 block 的 dispatch 开销 |
| ray_wait_per_block_ms | total_ray_wait_runtime / num_blocks × 1000 | 每 block 的 ray.wait 开销 |
| data_ready_per_block_ms | total_on_data_ready_runtime / num_blocks × 1000 | 每 block 的 on_data_ready 开销 |

### 辅助指标

| 指标 | 含义 |
|------|------|
| avg_scheduling_loop_duration_s | 每步调度耗时（参考，不作为主对比） |
| max_scheduling_loop_duration_s | 每步调度最大耗时 |
| inter_step_total_s | 步间间隔（perf 开销指标） |
| spilled_gb | object store spill 量（监控 IO 噪声） |
| scheduling_overhead.scheduling_ms.mean | raylet 侧 per-task 调度延迟 |

### 不使用的指标

| 指标 | 原因 |
|------|------|
| time_total_s | 被 spilled IO 严重干扰 |
| avg_scheduling_loop_duration_s | 步数不一致时不可比 |
| p50/p90 scheduling loop | 当前版本未采集（值=0） |

## Benchmark 脚本

使用 `release/nightly_tests/dataset/worker_scaling_benchmark.py`

参数:
- `--num-workers 4000`: 4000 并发
- `--worker-type tasks`: task 模式
- `--num-scalar-cols 2 --num-array-cols 2`: schema 配置
- `--blocks-per-worker 10`: 每 worker 10 blocks (总 40K)
- `--num-operators 1 或 4`: 单 op 或多 op

## 执行命令

### 场景一：单 operator

```bash
# A 基线 (num_returns 无上限 + 全关)
RAY_DATA_RAY_WAIT_NUM_RETURNS=-1 \
RAY_DATA_MAX_COMPLETIONS_PER_STEP=-1 \
RAY_DATA_ENABLE_CAPACITY_BASED_DISPATCH=0 \
RAY_DATA_ENABLE_PERF_METRICS=0 \
RAY_DEFAULT_OBJECT_STORE_MEMORY_PROPORTION=0.4 \
python3 worker_scaling_benchmark.py \
  --num-workers 4000 --worker-type tasks \
  --num-scalar-cols 2 --num-array-cols 2 \
  --blocks-per-worker 50 --num-operators 1 \
  --num-cpus 0.25 --disable-backpressure

# B +num_returns=1024
RAY_DATA_RAY_WAIT_NUM_RETURNS=1024 \
RAY_DATA_MAX_COMPLETIONS_PER_STEP=-1 \
RAY_DATA_ENABLE_CAPACITY_BASED_DISPATCH=0 \
RAY_DATA_ENABLE_PERF_METRICS=0 \
RAY_DEFAULT_OBJECT_STORE_MEMORY_PROPORTION=0.4 \
python3 worker_scaling_benchmark.py \
  --num-workers 4000 --worker-type tasks \
  --num-scalar-cols 2 --num-array-cols 2 \
  --blocks-per-worker 50 --num-operators 1 \
  --num-cpus 0.25 --disable-backpressure

# C +cap completions=512
RAY_DATA_RAY_WAIT_NUM_RETURNS=1024 \
RAY_DATA_MAX_COMPLETIONS_PER_STEP=512 \
RAY_DATA_ENABLE_CAPACITY_BASED_DISPATCH=0 \
RAY_DATA_ENABLE_PERF_METRICS=0 \
RAY_DEFAULT_OBJECT_STORE_MEMORY_PROPORTION=0.4 \
python3 worker_scaling_benchmark.py \
  --num-workers 4000 --worker-type tasks \
  --num-scalar-cols 2 --num-array-cols 2 \
  --blocks-per-worker 50 --num-operators 1 \
  --num-cpus 0.25 --disable-backpressure

# D +capacity dispatch
RAY_DATA_RAY_WAIT_NUM_RETURNS=1024 \
RAY_DATA_MAX_COMPLETIONS_PER_STEP=512 \
RAY_DATA_ENABLE_CAPACITY_BASED_DISPATCH=1 \
RAY_DATA_ENABLE_PERF_METRICS=0 \
RAY_DEFAULT_OBJECT_STORE_MEMORY_PROPORTION=0.4 \
python3 worker_scaling_benchmark.py \
  --num-workers 4000 --worker-type tasks \
  --num-scalar-cols 2 --num-array-cols 2 \
  --blocks-per-worker 50 --num-operators 1 \
  --num-cpus 0.25 --disable-backpressure

# E +perf metrics
RAY_DATA_RAY_WAIT_NUM_RETURNS=1024 \
RAY_DATA_MAX_COMPLETIONS_PER_STEP=512 \
RAY_DATA_ENABLE_CAPACITY_BASED_DISPATCH=1 \
RAY_DATA_ENABLE_PERF_METRICS=1 \
RAY_DEFAULT_OBJECT_STORE_MEMORY_PROPORTION=0.4 \
python3 worker_scaling_benchmark.py \
  --num-workers 4000 --worker-type tasks \
  --num-scalar-cols 2 --num-array-cols 2 \
  --blocks-per-worker 50 --num-operators 1 \
  --num-cpus 0.25 --disable-backpressure
```

### 场景二：多 operator (4 个)

```bash
# F 基线 (num_returns 无上限 + 全关)
RAY_DATA_RAY_WAIT_NUM_RETURNS=-1 \
RAY_DATA_MAX_COMPLETIONS_PER_STEP=-1 \
RAY_DATA_ENABLE_CAPACITY_BASED_DISPATCH=0 \
RAY_DATA_ENABLE_PERF_METRICS=0 \
RAY_DEFAULT_OBJECT_STORE_MEMORY_PROPORTION=0.4 \
python3 worker_scaling_benchmark.py \
  --num-workers 4000 --worker-type tasks \
  --num-scalar-cols 2 --num-array-cols 2 \
  --blocks-per-worker 50 --num-operators 4 \
  --num-cpus 0.25 --disable-backpressure

# G +num_returns=1024
RAY_DATA_RAY_WAIT_NUM_RETURNS=1024 \
RAY_DATA_MAX_COMPLETIONS_PER_STEP=-1 \
RAY_DATA_ENABLE_CAPACITY_BASED_DISPATCH=0 \
RAY_DATA_ENABLE_PERF_METRICS=0 \
RAY_DEFAULT_OBJECT_STORE_MEMORY_PROPORTION=0.4 \
python3 worker_scaling_benchmark.py \
  --num-workers 4000 --worker-type tasks \
  --num-scalar-cols 2 --num-array-cols 2 \
  --blocks-per-worker 50 --num-operators 4 \
  --num-cpus 0.25 --disable-backpressure

# H +cap completions=512
RAY_DATA_RAY_WAIT_NUM_RETURNS=1024 \
RAY_DATA_MAX_COMPLETIONS_PER_STEP=512 \
RAY_DATA_ENABLE_CAPACITY_BASED_DISPATCH=0 \
RAY_DATA_ENABLE_PERF_METRICS=0 \
RAY_DEFAULT_OBJECT_STORE_MEMORY_PROPORTION=0.4 \
python3 worker_scaling_benchmark.py \
  --num-workers 4000 --worker-type tasks \
  --num-scalar-cols 2 --num-array-cols 2 \
  --blocks-per-worker 50 --num-operators 4 \
  --num-cpus 0.25 --disable-backpressure

# I +capacity dispatch
RAY_DATA_RAY_WAIT_NUM_RETURNS=1024 \
RAY_DATA_MAX_COMPLETIONS_PER_STEP=512 \
RAY_DATA_ENABLE_CAPACITY_BASED_DISPATCH=1 \
RAY_DATA_ENABLE_PERF_METRICS=0 \
RAY_DEFAULT_OBJECT_STORE_MEMORY_PROPORTION=0.4 \
python3 worker_scaling_benchmark.py \
  --num-workers 4000 --worker-type tasks \
  --num-scalar-cols 2 --num-array-cols 2 \
  --blocks-per-worker 50 --num-operators 4 \
  --num-cpus 0.25 --disable-backpressure
```

## 执行方式

- 串行提交（一个完成后再提交下一个），避免资源竞争
- 使用 `ray job submit --no-wait` 异步提交，轮询 REST API 等待完成
- 每个实验间隔 30s 释放资源

## 前置条件

1. 集群所有节点安装新版 wheel 包（含 num_returns 修改）
2. 集群资源: 1337 CPU, 4000 workers (num_cpus=0.5 → 需要 2000 CPU)
3. benchmark.py 已加 try/except 降级 detail=True → detail=False

## 结果分析

### 增量收益计算（per-block）

- num_returns: `(sched/B_A - sched/B_B) / sched/B_A × 100%`
- cap completions: `(sched/B_B - sched/B_C) / sched/B_C × 100%`
- capacity dispatch: `(sched/B_C - sched/B_D) / sched/B_D × 100%`
- perf 开销: `(inter_step_D - inter_step_E) / inter_step_D × 100%`

### 关注的对比维度

1. `ray_wait_per_block_ms`: num_returns 应显著降低（尤其 4000 并发）
2. `dispatch_per_block_ms`: capacity dispatch 应降低单次 dispatch 开销
3. `sched_per_block_ms`: 三优化叠加后总调度开销下降
4. `inter_step_total_s`: perf 开关对步间间隔的影响
5. 多 op vs 单 op: num_returns 在多 operator 下收益是否更大

### 预期结论

- 4000 并发下，num_returns=1024 应显著降低 ray.wait per-block 开销（从 O(4000) 锁降到 O(1024)）
- capacity dispatch 在多 operator 场景下收益更明显
- cap completions 主要控制 burst，在多 operator 下防止 raywait 膨胀
- perf metrics 有可量化的 inter_step 开销
