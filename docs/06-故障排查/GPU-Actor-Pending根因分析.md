# Ray Data GPU Actor Pending 根因分析

## 问题描述

**集群配置**：500 GPU，作业配置 `--streaming-gpu-concurrency 1000`，每个 actor 配置 `--streaming-num-gpus 0.5`。理论上 500 GPU / 0.5 = 1000 个 actor 刚好满足。

**现象**：大量 GPU actor 一直处于 `Waiting for scheduling` / `PENDING_CREATION` 状态，无法被调度。

**作业信息**：
- Job ID: `29000000`
- Submission ID: `raysubmit_Z3j4JaKDB59dXwrL`
- Pipeline: `multishot_video_classifier_pipeline_checkpoint.py`
- Processing Mode: `streaming`
- Streaming Mode: `qwenvl`
- Actor Class: `MapWorker(MapBatches(DistributedQwenVLVideoProcessMapper))`

---

## 排查过程

### 第一步：查看集群资源总览

**命令**：
```bash
ray status
python3 -c "import ray; ray.init(address='auto'); print(ray.available_resources()); print(ray.cluster_resources())"
```

**结果**：

| 资源 | 总量 | 可用 | 已占用 |
|------|------|------|--------|
| GPU | 500.0 | **0** | **500.0** |
| CPU | 55,500 | 28,300 | 27,200 |
| Memory | ~218 TiB | ~92 TiB | ~126 TiB |

**结论**：GPU 已 100% 耗尽，瓶颈确认是 GPU。

---

### 第二步：排查是否有其他作业占用 GPU

**命令**：
```python
from ray.util.state import list_actors
from collections import Counter

actors = list_actors(filters=[('state','=','ALIVE')], limit=10000, raise_on_missing_output=False)
job_counter = Counter([a.get('job_id','unknown') for a in actors])
```

**结果**：

| Job ID | ALIVE Actor 数 | 说明 |
|--------|---------------|------|
| **29000000** | **9998** | 用户当前作业 |
| 03000000 | 2 | Ray 系统 actor（`_StatsActor`、`ActorLocationTracker`，不消耗 GPU） |

**结论**：没有其他作业的 actor 占用 GPU，所有资源消耗来自用户作业 `29000000`。

---

### 第三步：按 actor 类型统计 GPU 消耗

**命令**：
```python
alive = list_actors(filters=[('state','=','ALIVE'),('class_name','=','MapWorker(MapBatches(DistributedQwenVLVideoProcessMapper))')], limit=10000, detail=True, raise_on_missing_output=False)
gpus = [a.required_resources.get("GPU",0) for a in alive]
```

**结果**：

| Actor 类型 | ALIVE | PENDING | GPU/个 | 占用 GPU |
|-----------|-------|---------|--------|---------|
| `MapWorker(MapBatches(DistributedQwenVLVideoProcessMapper))` | **727** | **999** | 0.5 | **363.5** |
| `QwenVLCPUPreprocessActor` | 9,141 | 0 | 0 | 0 |
| `MapWorker(MapBatches(VideoClipInfoKafkaMapper))` | 130 | 0 | 0 | 0 |

**发现两个异常**：
1. 配置 1000 并行度，但实际创建了 727 + 999 = **1726** 个 GPU actor
2. 727 个 ALIVE actor 只占 363.5 GPU，但集群显示 500 GPU 全部占满，有 **136.5 GPU 去向不明**

---

### 第四步：排查 136.5 GPU 去向——检查历史 DEAD actor

**命令**：
```python
dead = list_actors(filters=[('state','=','DEAD'),('class_name','=','MapWorker(MapBatches(DistributedQwenVLVideoProcessMapper))')], limit=10000, detail=True, raise_on_missing_output=False)
jobs = Counter([a.job_id for a in dead])
reasons = Counter([a.death_cause.get("actor_died_error_context",{}).get("reason","unknown") for a in dead])
```

**结果**：

DEAD GPU actor 按 Job 分布：

| Job ID | DEAD 数量 |
|--------|----------|
| 0b000000 | 175 |
| 0d000000 | 111 |
| 0c000000 | 102 |
| 20000000 | 100 |
| 16000000 | 95 |
| 15000000 | 91 |
| 0e000000 | 90 |
| 0a000000 | 81 |
| **29000000** | **0** |

死因统计：

| 死因 | 数量 | 说明 |
|------|------|------|
| `OWNER_DIED` | 656 | owner（driver）进程崩溃，actor 级联死亡 |
| `REF_DELETED` | 189 | 引用被清除（旧 pipeline 正常退出或取消） |

**结论**：136.5 GPU 被之前 8 个旧 job 的 DEAD actor 的僵尸进程占用。Actor 在 GCS 层面已标记为 DEAD，但 raylet 没有正确回收其 GPU 资源。这是 **Ray Core 的资源泄漏问题**。

---

### 第五步：确认 1726 actor 全部属于当前 job

**命令**：
```python
# PENDING actors 按 job 统计
p = list_actors(filters=[('state','=','PENDING_CREATION'),('class_name','=','MapWorker(MapBatches(DistributedQwenVLVideoProcessMapper))')], limit=10000, detail=True, raise_on_missing_output=False)
Counter([a.job_id for a in p])
```

**结果**：
```
PENDING GPU actors by job:
  29000000: 999    ← 全部属于当前作业
```

确认 727(ALIVE) + 999(PENDING) = 1726 全属于 job `29000000`，而配置只有 1000。

---

### 第六步：排除可能的原因

**检查 ALIVE actor 重启次数**：
```python
restarts = set([x.num_restarts for x in alive])
# 结果: {0}  ← 所有 ALIVE actor 从未被重启过
```

**检查 PENDING actor 节点分配**：
```python
# 结果: 999 个 PENDING actor 全部 node_id=None, pid=0
# 说明它们从未被分配到任何节点
```

**检查是否有 RUNNING task 消耗 GPU**：
```python
running_tasks = list_tasks(filters=[('state','=','RUNNING')], limit=5000, raise_on_missing_output=False)
# 结果: Total RUNNING tasks: 0
```

**排除的可能性**：
- ❌ 其他 job 的 actor 占用（只有 job 29000000）
- ❌ `max_restarts=-1` 导致的 actor 无限重启（ALIVE actor 的 `num_restarts` 全为 0）
- ❌ pipeline 代码重试（`build_pipeline` 只调用一次，无重试循环）
- ❌ Mapper 内部创建额外 GPU actor（内部只创建 CPU actor）
- ❌ GPU task（非 actor）占用（RUNNING tasks = 0）

---

### 第七步：查看 Ray Data 日志——找到直接证据

**命令**：
```bash
grep 'Scaling up actor pool' /tmp/ray/session_latest/logs/ray-data/ray-data.log | tail -10
```

**关键日志**：
```
2026-05-21 17:33:23,922  DEBUG actor_pool_map_operator.py:1067 -- Scaling up actor pool by 1000 (reason=scaling to initial size, running=0, restarting=0, pending=0)
2026-05-21 17:33:42,798  DEBUG actor_pool_map_operator.py:1067 -- Scaling up actor pool by 200 (reason=scaling to initial size, running=0, restarting=0, pending=0)
2026-05-22 09:09:24,507  DEBUG actor_pool_map_operator.py:1067 -- Scaling up actor pool by 999 (reason=pool below min size, running=1, restarting=0, pending=0)
```

**这是铁证：**
- 05-21 17:33 — Job 启动，初始化创建 1000 个 GPU actor
- 05-22 09:09（16 小时后）— Pool 检测到 `running=1, pending=0`，认为低于 `min_size=1000`，又创建了 999 个

---

## 根因分析

### 问题一：136.5 GPU 泄漏（Ray Core Bug）

之前在同一集群上跑过 8+ 次旧 job，每次因 driver crash（`OWNER_DIED`）导致大量 GPU actor 死亡。这些 actor 在 GCS 中标记为 DEAD，但其占用的 GPU 资源未被 raylet 正确回收。

- 845 个 DEAD actor 部分未释放 GPU → **136.5 GPU 泄漏**
- 导致当前 job 的 1000 个 actor 中最多 727 个能拿到 GPU 运行

### 问题二：重复创建 actor（Ray Data Bug）

这是核心 bug。根因在 `ActorPoolMapOperator` 的 `pending_to_running` 方法中存在 **先删除后校验** 的逻辑缺陷，导致 Pool 丢失对 actor 的跟踪。

---

## 代码级 Bug 分析

### ready_ref 的完整生命周期

#### 1. 创建阶段

`actor_pool_map_operator.py` 中 `_start_actor` 方法：

```python
def _start_actor(self, labels, logical_actor_id):
    # 创建 actor（提交到 GCS，等待调度）
    actor = self._actor_cls.options(...).remote(...)

    # 创建 ready_ref：actor method call，等 __init__ 完成后才能执行
    res_ref = actor.get_location.remote()

    # 注册回调
    def _task_done_callback(res_ref):
        has_actor = self._actor_pool.pending_to_running(res_ref)
        if not has_actor:
            return

    # 将 ref 注册到 streaming executor 的监控列表
    self._submit_metadata_task(res_ref, lambda: _task_done_callback(res_ref))

    return actor, res_ref
```

`_ActorPool.scale()` 中：
```python
for _ in range(req.delta):
    actor, ready_ref = self._create_actor()
    self.add_pending_actor(actor, ready_ref)
    # → self._pending_actors[ready_ref] = actor
```

`ready_ref` 同时注册到两个地方：
- `_pending_actors[ready_ref] = actor`（Pool 的字典）
- `_metadata_tasks[idx] = MetadataOpTask(res_ref, callback)`（Operator 的任务列表）

#### 2. 监控阶段

Streaming executor 主循环（`streaming_executor_state.py`）每轮：

```python
# 收集所有 active tasks（包括 metadata tasks）
all_refs = [task.ref for task in op.get_active_tasks()]

# 轮询完成状态（timeout=0.1s）
ready, _ = ray.wait(all_refs, timeout=0.1)

# 处理完成的 metadata tasks
for state, task in non_data_tasks:
    task.on_task_finished()  # ← 无 try/except 保护！
```

**关键点**：`ray.wait()` 返回 "ready" 的 ref 包括**成功 resolve** 和**以异常 resolve** 两种情况。

#### 3. 触发 `pending_to_running` 的时机

```
actor.get_location.remote() 创建 ObjectRef
         │
         ▼
ObjectRef 等待 resolve（actor 必须完成 __init__ 后才能执行 get_location）
         │
         ├── 场景 A：actor 成功启动，__init__ 完成 → ObjectRef 以正常值 resolve
         │
         └── 场景 B：actor __init__ 崩溃（OOM/异常）→ ObjectRef 以 RayActorError resolve
         │
         ▼
ray.wait() 返回该 ref（不管是场景 A 还是 B）
         │
         ▼
MetadataOpTask.on_task_finished()
         │
         ▼
_task_done_callback(res_ref)
         │
         ▼
self._actor_pool.pending_to_running(res_ref)   ← 此时触发
```

#### 4. `pending_to_running` 的 Bug

```python
def pending_to_running(self, ready_ref):
    if ready_ref not in self._pending_actors:
        return False

    actor = self._pending_actors.pop(ready_ref)   # ← 第一步：先从字典中删除
    try:
        actor_location = ray.get(ready_ref)        # ← 第二步：再获取结果
    except Exception:
        self._actor_to_logical_id.pop(actor, None)
        raise                                      # ← 异常向上传播，actor 彻底丢失

    self._running_actors[actor] = _ActorState(...) # ← 只有成功才到这里
    return True
```

**Bug 本质**：`pop` 在 `ray.get` 之前执行。

- 如果 `ray.get(ready_ref)` 成功 → actor 从 `_pending_actors` 移到 `_running_actors` ✓
- 如果 `ray.get(ready_ref)` 抛异常 → actor 已从 `_pending_actors` 删除，但未加入 `_running_actors` → **actor 从 Pool 跟踪中彻底消失** ✗

#### 5. 为什么 `ray.get(ready_ref)` 会抛异常

`ready_ref = actor.get_location.remote()` 是一个 actor method call。**Ray 中 actor method 必须等 `__init__` 完成后才能执行**。

```
ray.wait() vs ray.get() 的语义区别：

┌─────────────────────────────────────────────────────────────┐
│ ray.wait([ref]) 返回 ref    →  ref 已经 resolved（有结果了）  │
│                                 可能是正常值，也可能是错误    │
│                                                             │
│ ray.get(ref) 成功           →  ref 以正常值 resolve           │
│ ray.get(ref) 抛异常         →  ref 以错误 resolve             │
└─────────────────────────────────────────────────────────────┘
```

当 actor `__init__` 崩溃时：
1. Actor 进程死亡
2. 所有排队的 method call（包括 `get_location`）以 `RayActorError` resolve
3. `ray.wait()` 返回这个 ref 为 "ready"（因为它确实 resolved 了，只是以错误 resolve）
4. 回调链触发 `pending_to_running`
5. `ray.get(ref)` 抛出 `RayActorError`

**在当前场景中，`__init__` 崩溃的具体原因**：

`DistributedQwenVLVideoProcessMapper.__init__` 需要：
- 加载 QwenVL 模型（checkpoint-1842）
- 初始化 vLLM engine（`gpu_memory_utilization=0.3`）
- 创建 13 个 CPU preprocess actor pool

当 727 个 actor **同时**争抢 GPU 显存进行模型加载时，容易触发 CUDA OOM，导致部分 actor `__init__` 失败。

#### 6. `max_restarts=-1` 的连锁效应

Ray Data 默认设置（`actor_pool_map_operator.py` line 570-571）：
```python
if "max_restarts" not in ray_remote_args:
    ray_remote_args["max_restarts"] = -1  # 无限重启
```

这导致：

```
                Pool 视角                              GCS/Ray Core 视角
                ────────                              ──────────────────
T0: _pending_actors[ref] = actor        actor PENDING_CREATION (等 GPU)

T1: (actor 拿到 GPU)                    actor ALIVE (进程启动，开始 __init__)

T2: __init__ OOM 崩溃                   actor DEAD
    ray.wait() 返回 ref
    pending_to_running:
      pop(ref) ← 已删除
      ray.get(ref) → RayActorError
      raise → actor 丢失 ←───────────── Pool 此刻失去跟踪

T3: (Pool 不知道)                        max_restarts=-1 → 自动重启
                                         actor RESTARTING → PENDING_CREATION

T4: (Pool 不知道)                        再次拿到 GPU → __init__ 成功
                                         actor ALIVE (正常运行)

T5: Pool 永远不会给这个 actor            actor 占着 GPU，但永远不会收到任务
    分派任务（已从跟踪中丢失）
```

#### 7. 上层异常处理缺失

`streaming_executor_state.py` 中 `process_completed_tasks`：

```python
# DataOpTask 有 try/except 保护
for state, task in data_tasks:
    try:
        task.on_task_finished()
    except Exception:
        # 有错误处理逻辑
        ...

# MetadataOpTask 没有 try/except 保护！
for state, task in non_data_tasks:
    task.on_task_finished()   # ← pending_to_running 的异常直接向上传播
```

`MetadataOpTask` 的异常处理和 `DataOpTask` 不同——**没有 try/except 包裹**。这意味着 `pending_to_running` 抛出的 `RayActorError` 会直接传播到 streaming executor 的主循环。

#### 8. 完整时间线还原

```
05-21 17:33:23  Job 29000000 启动
                ActorPoolMapOperator 初始化
                scale(delta=1000, reason="scaling to initial size")
                创建 1000 个 actor，全部加入 _pending_actors
                │
                ▼
                363.5 GPU 可用（500 - 136.5 泄漏），最多 727 个 actor 能启动
                │
                ▼
                727 个 actor 陆续拿到 GPU，开始 __init__
                并发加载模型导致部分 actor OOM 崩溃
                │
                ├── 成功的 actor:
                │   get_location resolve → pending_to_running 成功
                │   从 _pending_actors 移到 _running_actors
                │
                └── OOM 崩溃的 actor:
                    get_location 以 RayActorError resolve
                    pending_to_running 中 pop 后 ray.get 抛异常
                    actor 从 _pending_actors 丢失
                    Ray Core 自动重启 → actor 变回 ALIVE
                    但 Pool 已不知道它的存在
                │
                ▼ 反复循环（actor 崩溃 → 丢失 → 重启 → 成功，但 Pool 不知道）
                │
                ▼ 16 小时后
                │
05-22 09:09:24  _pending_actors = 0（全部被 pop 走了）
                _running_actors = 1（只有 1 个从未崩溃过的幸运 actor）
                current_size() = 0 + 1 = 1
                │
                ▼ Autoscaler 检测
                │
                current_size(1) < min_size(1000)
                触发 "pool below min size"
                scale(delta=999) → 再创建 999 个新 actor
                │
                ▼
                总计: 1000(首批) + 999(补创建) = 1999 个 actor 被创建到 GCS
                GCS 中存活: 727(ALIVE) + 999(PENDING) = 1726
```

#### 9. 为什么检查时 `num_restarts=0`

检查时间是 05-22 11:xx（创建后约 18 小时），此时 727 个 ALIVE actor 的 `num_restarts=0`。

这有两种可能解释：
1. **大部分 actor 只崩溃了一次**：第一次 init OOM 时被 `pending_to_running` 丢失，Ray Core 重启后（第二次 init）成功。但因为 actor handle 是同一个，`num_restarts` 应该 = 1。检查时间的 727 可能是 **从未崩溃过**的那批 + 第二批 999 中成功启动的。
2. **727 个是首批中从未崩溃的**：首批 1000 中有 727 个 init 一次成功，但它们的 `pending_to_running` callback 在 executor 异常处理后被丢失（因为 MetadataOpTask 异常未被捕获导致整个处理循环中断，后续成功的 callback 没有被处理）。

---

## Autoscaler 触发机制

`default_actor_autoscaler.py` 中的判断逻辑：

```python
if actor_pool.current_size() < actor_pool.min_size():
    return ActorPoolScalingRequest.upscale(
        delta=actor_pool.min_size() - actor_pool.current_size(),
        reason="pool below min size",
    )
```

其中：
- `current_size()` = `num_pending_actors() + num_running_actors()`
- `num_pending_actors()` = `len(self._pending_actors)` = 0
- `num_running_actors()` = `len(self._running_actors) - self._num_restarting_actors` = 1 - 0 = 1
- `min_size()` = 1000（来自 `ActorPoolStrategy(size=1000)`，由 `concurrency=1000` 转换）

计算：`delta = 1000 - 1 = 999`，与日志完全吻合。

---

## `_pending_actors` 的所有移除路径

| 路径 | 代码位置 | 触发条件 | 是否适用当前场景 |
|------|---------|---------|----------------|
| A: `pending_to_running` 正常 | line 1273 | ready_ref 成功 resolve | ✓ 部分 actor |
| B: `pending_to_running` 异常 | line 1273 | ready_ref 以错误 resolve | **✓ 核心 bug 路径** |
| C: `_try_remove_pending_actor` | line 1383 | scale down 请求 | ✗ 固定 pool 无 scale down |
| D: `_release_pending_actors` | line 1411 | operator shutdown | ✗ job 仍在运行 |

---

## 影响总结

| 问题 | 影响 |
|------|------|
| 136.5 GPU 泄漏 | 1000 个 actor 只有 727 个拿到 GPU，吞吐量降低 ~27% |
| 999 个额外 PENDING actor | 永远无法被调度（GPU 已满），白占 GCS 内存和调度队列 |
| 726 个"幽灵" actor | 占着 GPU 但 Pool 不给它们派任务，纯粹浪费资源 |
| 实际有效并行度 | 远低于配置的 1000，可能只有 1 个 actor 在处理数据 |

---

## 修复建议

### Bug 修复（Ray Data）

**方案一：先校验后删除**
```python
def pending_to_running(self, ready_ref):
    if ready_ref not in self._pending_actors:
        return False

    actor = self._pending_actors[ready_ref]        # 先 get，不 pop
    try:
        actor_location = ray.get(ready_ref)
    except Exception:
        # 失败时才删除，并做好清理
        self._pending_actors.pop(ready_ref)
        self._actor_to_logical_id.pop(actor, None)
        raise

    self._pending_actors.pop(ready_ref)            # 成功后才 pop
    self._running_actors[actor] = _ActorState(...)
    return True
```

**方案二：异常时重新跟踪 actor**
```python
def pending_to_running(self, ready_ref):
    if ready_ref not in self._pending_actors:
        return False

    actor = self._pending_actors.pop(ready_ref)
    try:
        actor_location = ray.get(ready_ref)
    except Exception:
        # actor 会被 max_restarts 重启，重新提交 ready_ref 监控
        new_ref = actor.get_location.remote()
        self._pending_actors[new_ref] = actor      # 重新跟踪
        # 需要重新注册到 metadata_tasks
        return False

    self._running_actors[actor] = _ActorState(...)
    return True
```

**方案三：MetadataOpTask 添加异常处理**
```python
# streaming_executor_state.py 中
for state, task in non_data_tasks:
    try:
        task.on_task_finished()
    except RayActorError:
        # actor init 失败，等待 max_restarts 重启
        # 不要丢失 actor 的跟踪
        logger.warning(f"Actor init failed, will retry: {task}")
    except Exception as e:
        logger.error(f"Unexpected error in metadata task: {e}")
```

### 运维建议

| 优先级 | 措施 | 说明 |
|--------|------|------|
| **P0** | 重启 Ray 集群 | 清理旧 job 的 136.5 GPU 泄漏 |
| **P1** | 提交 Ray Data bug fix | `pending_to_running` 的先删后校验问题 |
| **P1** | 降低 `streaming-gpu-concurrency` | 避免并发 init 导致 OOM（如设为 500，分批启动） |
| **P2** | 添加 `--qwenvl-gpu-memory-utilization` 保护 | 降低单 actor GPU 显存占用，减少 init OOM 概率 |
| **P2** | 每次新 job 前检查 `ray.available_resources()["GPU"]` | 确认有足够资源再提交 |
| **P2** | 排查旧 job driver 反复 crash 原因 | 8 个旧 job 都因 `OWNER_DIED` 失败 |

---

## 附录：关键代码文件

| 文件 | 说明 |
|------|------|
| `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py` | ActorPool 管理、`pending_to_running`、`scale` |
| `python/ray/data/_internal/execution/streaming_executor_state.py` | Streaming executor 主循环、`process_completed_tasks` |
| `python/ray/data/_internal/actor_autoscaler/default_actor_autoscaler.py` | Autoscaler 触发 scale up 逻辑 |
| `python/ray/data/_internal/actor_autoscaler/actor_pool_resizing_policy.py` | Scale delta 计算 |
| `python/ray/data/_internal/compute.py` | `ActorPoolStrategy`、`concurrency` 参数转换 |
| `pipeline/multi_video_classifier_merge/pipeline_builder.py` | Pipeline 构建，GPU actor 创建参数 |

---

## 附录：排查命令速查

```bash
# 1. 集群资源总览
ray status
python3 -c "import ray; ray.init(address='auto'); print(ray.available_resources()); print(ray.cluster_resources())"

# 2. 按 job 统计存活 actor
python3 -c "
from ray.util.state import list_actors
from collections import Counter
actors = list_actors(filters=[('state','=','ALIVE')], limit=10000, raise_on_missing_output=False)
print(Counter([a.job_id for a in actors]).most_common())
"

# 3. GPU actor 各状态计数
for state in ALIVE PENDING_CREATION DEAD RESTARTING; do
  python3 -c "
from ray.util.state import list_actors
r = list_actors(filters=[('state','=','$state'),('class_name','=','MapWorker(MapBatches(DistributedQwenVLVideoProcessMapper))')], limit=10000, raise_on_missing_output=False)
print(f'$state: {len(r)}')
"
done

# 4. 检查 DEAD actor 死因
python3 -c "
from ray.util.state import list_actors
from collections import Counter
d = list_actors(filters=[('state','=','DEAD'),('class_name','=','MapWorker(MapBatches(DistributedQwenVLVideoProcessMapper))')], limit=10000, detail=True, raise_on_missing_output=False)
reasons = Counter([a.death_cause.get('actor_died_error_context',{}).get('reason','unknown') for a in d])
print(reasons.most_common())
"

# 5. Ray Data scaling 日志
grep 'Scaling up actor pool\|Scaling actor pool\|pool below min' /tmp/ray/session_latest/logs/ray-data/ray-data.log

# 6. 检查 actor num_restarts
python3 -c "
from ray.util.state import list_actors
a = list_actors(filters=[('state','=','ALIVE'),('class_name','=','MapWorker(MapBatches(DistributedQwenVLVideoProcessMapper))')], limit=10000, detail=True, raise_on_missing_output=False)
print('Restart counts:', set([x.num_restarts for x in a]))
"
```
