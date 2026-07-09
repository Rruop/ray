# Scheduling Loop 优化压测实验报告

## 1. 背景

### 1.1 目标

验证 Ray Data 调度循环优化项的性能收益，核心验证 `RAY_DATA_RAY_WAIT_NUM_RETURNS`（限制 `ray.wait` 的 `num_returns`）对调度性能的影响。

### 1.2 集群环境

| 项 | 值 |
|---|---|
| 节点数 | 45 |
| CPU | 3257 |
| 内存 | 3.28TB |
| Object Store | 1.2TB |
| Ray 版本 | 2.55.1+kuaishou |
| Python | 3.12 |

### 1.3 代码改动

#### 已完成的优化

1. **废弃 `DYNAMIC_RAY_WAIT_TIMEOUT`**
   - 删除 `enable_dynamic_ray_wait_timeout` 字段和相关逻辑
   - `ray.wait` timeout 固定为 `DEFAULT_RAY_WAIT_TIMEOUT_S = 0.1s`
   - 原因：timeout 只控制 ray.wait 的等待阶段，不控制 O(N) 锁路径的耗时。N 小时 timeout=0.1s 已足够，N 大时 O(N) 锁开销远大于 timeout 差异

2. **新增 `RAY_DATA_RAY_WAIT_NUM_RETURNS`**
   - 默认值 1024，-1 表示无上限（原行为）
   - 限制 `ray.wait` 的 `num_returns` 参数，让 C++ `CoreWorker::Wait` 的 `HasOwner` 循环提前退出
   - 修改文件：`context.py`、`streaming_executor.py`、`streaming_executor_state.py`

3. **新增 `RAY_DATA_DEFAULT_MAP_NUM_CPUS`**
   - 控制 ReadRange 等默认 map operator 的 `num_cpus`
   - 修改文件：`map_operator.py` 的 `_canonicalize_ray_remote_args`

4. **Benchmark 脚本增强**
   - 新增 `--num-cpus`、`--disable-backpressure`、`--task-duration-s` 参数
   - `--disable-backpressure` 关闭所有 Ray Data 反压策略
   - `--task-duration-s` 在 UDF 中加 sleep 模拟长运行 task
   - `benchmark.py` 加 try/except 降级 `get_stats_summary(detail=True)` → `detail=False`

#### C++ 层原理

`ray.wait(N refs, num_returns=N, timeout=0.1)` 的实际耗时链路：

```
Step 1: CoreWorker::Wait → for i in 0..N: HasOwner(ids[i])
        每次 HasOwner 独立 acquire reference_counter mutex_
        num_returns=N 时不会提前 break → 必须扫完 N 个
        N=10000 时约 1-2s [不受 timeout 控制]

Step 2: CoreWorkerMemoryStore::GetImpl → 持全局锁 mu_ 扫 N 个 hash 查询
        at_most_num_objects=false → 不会提前 break
        N=10000 时约 1ms [可忽略]

Step 3: get_request->Wait(timeout) → timeout 在这里才生效
```

改 `num_returns=1024` 后：Step 1 最多扫 1024 个就 break，省 9000+ 次 HasOwner 锁操作。

---

## 2. 第一轮测试（1000 workers，已完成）

### 2.1 参数

| 项 | 值 |
|---|---|
| num_workers | 1000 |
| blocks_per_worker | 100 |
| num_operators | 1 |
| num_cpus | 0.5 |
| block_size | 16MB |
| 总 blocks | 100K |
| 总数据量 | ~1.6TB |
| 执行方式 | 串行（一个完成再下一个） |

### 2.2 实验矩阵

| 实验 | dynamic_wait | max_completions | capacity_dispatch | perf_metrics |
|------|:-1:|:-1:|:-1:|:-1:|
| A 基线 | 0 | -1 | 0 | 0 |
| B +dyn_wait | 1 | -1 | 0 | 0 |
| C +cap512 | 1 | 512 | 0 | 0 |
| D +cap_dispatch | 1 | 512 | 1 | 0 |
| E +perf | 1 | 512 | 1 | 1 |

### 2.3 结果

#### Per-block 指标（ms/block，公平对比）

| 实验 | sched/B | disp/B | raywait/B | dataready/B | spilledGB | time(s) |
|------|---------|--------|-----------|-------------|-----------|---------|
| A 基线 | 3.88 | 2.94 | 0.30 | 0.50 | 1656 | 513 |
| B +dyn | 4.39 (+13%) | 3.04 (+3%) | 0.56 (+86%) | 0.61 (+22%) | 951 | 557 |
| C +cap | 4.02 (-9%) | 2.95 (-3%) | 0.28 (-50%) | 0.62 (+2%) | 1616 | 550 |
| D +disp | 2.84 (-29%) | 1.48 (-50%) | 0.32 (+17%) | 0.83 (+34%) | 1553 | 417 |
| E +perf | 2.79 (-2%) | 1.46 (-2%) | 0.31 (-3%) | 0.79 (-6%) | 1323 | 423 |

#### Raylet 侧 per-task 调度延迟

| 实验 | sched/task (ms) | total_overhead/task (ms) |
|------|----------------|--------------------------|
| A | 47.3 | 61.4 |
| B | 242.3 | 307.4 |
| D | 108.1 | 145.2 |
| E | 258.8 | 324.1 |

### 2.4 关键结论

1. **Dynamic ray.wait timeout（B vs A）：per-task 无收益，且有副作用**
   - sched/B 反升 13%，raylet sched/task 从 47ms→242ms（+5倍）
   - 它把大步拆小步，per-step avg 下降但 per-task 开销不变甚至更高
   - **已废弃此特性**

2. **Cap completions（C vs B）：raywait/B 降 50%**
   - 控制 burst 有效，但 dispatch/B 不变

3. **Capacity dispatch（D vs C）：dispatch/B 降 50%**
   - 唯一真正降低 per-task dispatch 开销的优化

4. **Perf metrics（E vs D）：inter_step 0.2s→25.3s（+125x）**
   - 有显著调度开销

### 2.5 测试问题

| # | 问题 | 影响 | 严重性 |
|---|------|------|--------|
| 1 | spilled GB 差异巨大（0~1656GB） | time_total 被 IO 主导 | 高 |
| 2 | p50/p90 调度循环 = 0 | 分位数指标未采集 | 中 |
| 3 | exp-C detail=True API 超时 | 丢失 raylet 侧数据 | 中 |
| 4 | 单 operator 场景 | 无法体现多 op 调度压力 | 高 |
| 5 | per-step avg vs per-block 矛盾 | 容易被 avg_step 下降误导 | 高 |
| 6 | exp-B 首次 Segfault | 随机 crash，重跑成功 | 低 |

---

## 3. 第二轮测试（4000+ workers，未完成）

### 3.1 目标

- 4 个 chained MapBatches，每 op 4000 并发，total active ≈ 16000
- 压到 ray.wait O(N) 锁瓶颈区域（N > 4000）
- 验证 `num_returns=1024` 的收益
- 多轮持续调度（不只一轮）

### 3.2 参数演进

#### 尝试 1：blocks_per_worker=100, 16MB block

| 项 | 值 |
|---|---|
| num_workers | 4000 |
| blocks_per_worker | 100 |
| block_size | 16MB |
| 总 blocks | 400K |
| 总数据量 | 6.4TB |
| object store | 1.2TB |

**问题**：总数据量 6.4TB 远超 object store 1.2TB，大量 spill。

#### 尝试 2：blocks_per_worker=10, 16MB block

**问题**：每 worker 只跑 ~2s，调度压力不够，无法观察持续调度性能。

#### 尝试 3：block_size=4MB, blocks_per_worker=50

| 项 | 值 |
|---|---|
| 总 blocks | 200K |
| 总数据量 | 800GB |

**问题**：ReadRange 200K blocks 每个默认 1 CPU，全量提交需 200K CPU，远超集群 3257 CPU，ReadRange task 排队卡死，MapBatches 无法启动。

#### 尝试 4：关闭反压 + ReadRange concurrency 限制

`ray.data.range(num_rows, override_num_blocks=200000, concurrency=4000)`

**问题**：`concurrency` 参数在 Ray 2.51 已弃用，虽然转为 `TaskPoolStrategy(size=4000)`，但 ReadRange 仍然全量提交 200K 个 task（每个 1 CPU），调度器排队 200K CPU 需求。

#### 尝试 5：ReadRange 少量 block + StreamingRepartition

```python
ds = ray.data.range(num_rows, override_num_blocks=100)
ds = ds.repartition(target_num_rows_per_block=rows_per_block)
```

**问题**：repartition 增加了一个额外 operator（`StreamingRepartition`），且 task 数 = 输入 block 数（100），不会增加 MapBatches 的 task 数。MapBatches 只有 100 个 task。

#### 尝试 6：ReadRange num_cpus=0（修改源码）

修改 `map_operator.py` 的 `_canonicalize_ray_remote_args`，加环境变量 `RAY_DATA_DEFAULT_MAP_NUM_CPUS`：

```python
if "num_cpus" not in ray_remote_args and "num_gpus" not in ray_remote_args:
    ray_remote_args["num_cpus"] = int(
        os.environ.get("RAY_DATA_DEFAULT_MAP_NUM_CPUS", "1")
    )
```

**验证**：`_canonicalize_ray_remote_args({})` 在 `RAY_DATA_DEFAULT_MAP_NUM_CPUS=0` 时返回 `{'num_cpus': 0}` ✅

**问题**：ReadRange 仍显示 4000 CPU。可能 ReadRange 的 `ray_remote_args` 在传入前已被设为 `{'num_cpus': 1}`，绕过了环境变量分支。**根因未完全确认**。

#### 尝试 7：task_duration_s + 关闭所有反压

在 UDF 中加 `time.sleep(task_duration_s)` 模拟长运行 task。

**问题**：
- `capacity_dispatch=0` 时每步只 dispatch 1 个 task
- `ray.wait` timeout=0.1s → 每步至少 0.1s
- dispatch_rate = 10/s，task_duration=5s → 稳态 active ≈ 50
- 无法达到 4000 active_tasks

#### 尝试 8：4 op × 4000 workers = 16000, blocks_per_worker=100, 2MB block

| 项 | 值 |
|---|---|
| num_workers | 16000 |
| num_operators | 4 |
| workers_per_operator | 4000 |
| blocks_per_worker | 100 |
| block_size | 2MB |
| num_cpus | 0.25 |
| task_duration_s | 5.0 |
| 总 blocks | 400K × 4MB = 1.6M (实际 ReadRange 400K) |

**结果**：
- ReadRange 16000 个 task 每个 1 CPU = 16000 CPU
- active_tasks 达到 16000+（ReadRange 15996 + MapBatches 1187）
- 但 **OOM**：大量 ReadRange task 并发执行，每个加载数据到内存，节点内存耗尽
- `3 Workers killed due to memory pressure (OOM)`

**根因**：ReadRange `num_cpus=0` 未生效，16000 个 ReadRange task 以 1 CPU 全量提交并并发执行，内存爆炸。

### 3.3 遇到的核心问题汇总

| # | 问题 | 原因 | 尝试的解决方案 | 结果 |
|---|------|------|---------------|------|
| 1 | ReadRange 全量提交卡死 | 每个 ReadRange task 默认 1 CPU，200K task = 200K CPU | `concurrency` 参数限制 | ❌ 弃用，不限制 ReadRange |
| 2 | ReadRange concurrency 无效 | Ray 2.51 弃用 `concurrency`，转 `TaskPoolStrategy` 但不影响 ReadRange 提交 | - | - |
| 3 | Repartition 不增加 task 数 | task 数 = 输入 block 数，不 = 输出 block 数 | - | - |
| 4 | ReadRange num_cpus=0 未生效 | 环境变量修改了 `_canonicalize_ray_remote_args`，但 ReadRange 可能不走此路径 | 需进一步排查 | ❌ 未解决 |
| 5 | capacity_dispatch=0 时 active 上不去 | 每步只 dispatch 1 个 task + timeout=0.1s → dispatch_rate=10/s | 开 capacity_dispatch=1 | 改变测试条件 |
| 6 | 16000 workers OOM | ReadRange 1 CPU × 16000 并发 → 内存爆炸 | 需解决 ReadRange num_cpus 问题 | ❌ 未解决 |
| 7 | object store spill 污染指标 | 数据量超过 object store | 减小 block_size / 增大 object store | 部分缓解 |
| 8 | detail=True API 超时 | 1000+ 并发时 dashboard API 扛不住 | try/except 降级 | ✅ 已解决 |
| 9 | benchmark.py 的 concurrency 弃用 | Ray 2.51 弃用 `concurrency` 参数 | 需用 `compute=TaskPoolStrategy(size=N)` | ⚠️ 未确认是否生效 |

### 3.4 未解决的关键阻塞

**ReadRange 的 `num_cpus` 无法设为 0**：

- 修改了 `map_operator.py` 的 `_canonicalize_ray_remote_args` 加环境变量
- 单独测试函数返回 `{'num_cpus': 0}` ✅
- 但实际运行时 ReadRange 仍占 1 CPU per task
- 可能原因：ReadRange 的 `ray_remote_args` 在 plan 阶段已被设了 `num_cpus=1`，绕过了 canonicalize
- 需要进一步排查 `plan_read_op.py` → `MapOperator.create` → `MapOperator.__init__` 的完整调用链

---

## 4. 代码改动文件清单

| 文件 | 改动 |
|------|------|
| `python/ray/data/context.py` | 删除 `enable_dynamic_ray_wait_timeout`，新增 `ray_wait_num_returns` |
| `python/ray/data/_internal/execution/streaming_executor.py` | 删除 dynamic wait 逻辑，固定 timeout=0.1s，传 `ray_wait_num_returns` |
| `python/ray/data/_internal/execution/streaming_executor_state.py` | `ray.wait` 的 `num_returns` 支持 -1/0/>0 三种模式 |
| `python/ray/data/_internal/execution/operators/map_operator.py` | `_canonicalize_ray_remote_args` 加 `RAY_DATA_DEFAULT_MAP_NUM_CPUS` 环境变量 |
| `release/nightly_tests/dataset/benchmark.py` | try/except 降级 `get_stats_summary(detail=True)` |
| `release/nightly_tests/dataset/worker_scaling_benchmark.py` | 新增 `--num-cpus`、`--disable-backpressure`、`--task-duration-s` 参数 |
| `release/nightly_tests/dataset/scheduling_benchmark_plan.md` | 更新测试方案 v2 |

---

## 5. 后续建议

### 5.1 解决 ReadRange num_cpus 问题

1. 排查 ReadRange 的 `ray_remote_args` 在 plan 阶段是否被预设 `num_cpus=1`
2. 或在 `plan_read_op.py` 中强制设 `ray_remote_args['num_cpus'] = 0`
3. 或不使用 `ray.data.range`，改用自定义 datasource 控制 num_cpus

### 5.2 测试方案调整

1. **先解决 ReadRange CPU 问题**，否则大规模测试无法进行
2. ReadRange num_cpus=0 后，400K blocks 可以全量提交不占 CPU
3. MapBatches `num_cpus=0.25` + `concurrency=4000` 控制并发
4. `capacity_dispatch=0` 时需要确认 dispatch_loop 是否能快速积累 active_tasks
5. 考虑 `capacity_dispatch=1` 作为基础条件

### 5.3 指标选择

- **主指标**：per-block（ms/block），消除步数差异
- **不使用**：time_total（被 spill IO 干扰）、per-step avg（步数不一致时不可比）
- **补充**：raylet 侧 scheduling_overhead（per-task 调度延迟）

### 5.4 代码优化方向

1. `RAY_DATA_RAY_WAIT_NUM_RETURNS=1024` 已实现，待大规模验证
2. C++ 层 `at_most_num_objects=true` 可进一步优化 Step 2 锁内扫描
3. `DYNAMIC_RAY_WAIT_TIMEOUT` 已废弃，不应恢复
