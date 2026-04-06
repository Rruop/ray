# Ray Data ActorPool 僵尸 Actor 不释放问题排查

**日期**: 2026-05-20
**集群**: kce-aip-bjxy-hb1az2 / kubekml
**作业**: kl_clip_process_record_20260519_10_1_e_commerce_0
**现象**: DistributedQwenVLVideoProcessMapper 的 actor pool 中大量 GPU actor 不释放，QwenVLCPUPreprocessActor 无法调度，整体管线卡死

---

## 1. 问题描述

用户的 Ray Data 视频分类管线使用 `DistributedQwenVLVideoProcessMapper` 作为 GPU actor（通过 `map_batches` API），每个 GPU actor 内部会启动 16 个 `QwenVLCPUPreprocessActor` 进行视频预处理。

问题表现：
- 管线进度卡在 99.996%（16,685,067 / 16,685,707），长时间无进展
- Ray Data 的 ActorPool 报告只有 5 个 actor，但实际有 **579 个 MapWorker actor 存活**
- 大量 QwenVLCPUPreprocessActor 处于 PENDING_CREATION 状态，无法调度
- 之前已通过 kcof 调整 actor pool 大小，但无效

---

## 2. 排查思路

### 2.1 信息收集方向

排查此类 actor pool 不释放问题，需要收集以下信息：

1. **集群资源状态** — `ray status` 查看 CPU/GPU/Memory 使用和 pending demand
2. **Actor 状态分布** — `ray summary actors` 查看各类 actor 的 ALIVE/PENDING/DEAD 数量
3. **Pipeline 进度** — driver 日志中的 progress 输出，确认卡在哪个 operator
4. **Driver 日志关键信息** — 搜索 WARNING/ERROR，定位卡住原因
5. **Pool 与实际 actor 数量对比** — Pool 报告数量 vs `ray list actors` 实际数量，判断是否有"僵尸"actor

### 2.2 排查步骤

```
Step 1: ray status             → 资源是否耗尽？有无 pending demand？
Step 2: ray summary actors     → actor 状态分布，是否大量 PENDING_CREATION
Step 3: driver 日志 tail       → pipeline progress，卡在哪个 operator
Step 4: grep "No ready actors" → GPU actor 是否卡在等 CPU actor
Step 5: 对比 Pool 报告 vs 实际 → 确认僵尸 actor 数量
Step 6: 源码分析               → 理解为什么 pool 释放不掉 actor
```

---

## 3. 现场排查过程

### 3.1 集群资源状态

连接到 head 节点执行 `ray status`：

```
======== Autoscaler status: 2026-05-20 16:31:38 ========
Node status: Active: 499 worker-1 + headgroup + 多个专用节点
Resources:
  CPU:    17478.0 / 23500.0
  GPU:    485.0 / 500.0
  Memory: 88.89TiB / 111.89TiB

Pending Demands:
  {'CPU': 2.0, 'memory': 10737418240.0}: 887+ pending tasks/actors
```

**发现**：有 887+ 个任务/actor 在等待 `CPU:2 + 10GB memory`，这正是 `QwenVLCPUPreprocessActor` 的资源需求。说明 CPU actor 调度不上。

### 3.2 Actor 状态分布

执行 `ray summary actors`：

```
CLASS_NAME                                                  STATE_COUNTS
QwenVLCPUPreprocessActor                                    ALIVE: 4311
                                                            PENDING_CREATION: 4995
                                                            RESTARTING: 6
MapWorker(MapBatches(DistributedQwenVLVideoProcessMapper))  ALIVE: 579
MapWorker(MapBatches(VideoClipInfoKafkaMapper))             ALIVE: 107
```

**关键发现**：
- 579 个 GPU MapWorker actor 存活
- 4311 个 CPU actor 存活 + 4995 个 PENDING（起不来）
- 但 Pipeline progress 报告 Pool 只有 5 个 actor

### 3.3 Pipeline 进度

Driver 日志（`job-driver-raysubmit_j4RvJdgx6KukFSET.log`）最新输出：

```
======= Running Dataset: dataset_10_0 =======
Total Progress: 18790000/55950223

ReadParquet->SplitBlocks(5000): 16685707/16685707           ← 完成
Filter->Map->Filter: 16685707/16685707                      ← 完成
StreamingRepartition[num_rows_per_block=64]: 16685707/8408016
MapBatches(DistributedQwenVLVideoProcessMapper): 16685067/16685707  ← 卡在这里！
  Tasks: 10; Actors: 5; Queued blocks: 0; Resources: 5.0 CPU, 2.5 GPU
FlatMap(ClipMergeMapper): 18989318/57213959
  Tasks: 174173; Actors: 0
...
```

**关键发现**：
- `DistributedQwenVLVideoProcessMapper` operator 只差 640 行就完成（99.996%）
- Pool 报告只有 **5 个 actor**，但实际有 **579 个** — 差异 574 个是"僵尸"actor
- 只有 5 个 actor 在工作，但它们也卡住了

### 3.4 GPU Actor 卡住原因

在 driver 日志中搜索 `"No ready actors"`：

```
(MapWorker pid=377, ip=10.82.234.37) WARNING - No ready actors, waiting for recovery... (elapsed=90822.9s, backoff=30.0s)
(MapWorker pid=378, ip=10.83.9.143) WARNING - No ready actors, waiting for recovery... (elapsed=90837.7s, backoff=30.0s)
(MapWorker pid=445, ip=10.82.234.164) WARNING - No ready actors, waiting for recovery... (elapsed=90599.6s, backoff=30.0s)
(MapWorker pid=445, ip=10.83.8.82) WARNING - No ready actors, waiting for recovery... (elapsed=90657.0s, backoff=30.0s)
```

**关键发现**：
- 4 个 MapWorker actor 已经卡了 **~25 小时**（90000+ 秒）
- 它们在等待内部 CPU actor 就绪，但 CPU actor 永远起不来
- `elapsed` 远超 `timeout_ms=18000000`（5 小时），说明恢复循环没有正确检查超时

### 3.5 Actor 配置确认

从日志中提取 MapWorker 的配置：

```
DistributedQwenVLVideoProcessMapper resolved config:
  timeout_ms=18000000 (5小时)
  cpu_actor_pool_size=16
  cpu_actor_num_cpus=2
  cpu_actor_memory=10737418240 (10GB)
  batch_size=32
  prefetch_batches=16
  batch_timeout_sec=420.0
  actor_max_restarts=-1          ← 无限重启！
  max_tasks_per_actor=2
  actor_health_check_interval=120.0
```

**关键发现**：`actor_max_restarts=-1` 导致 CPU actor 无限重启，永远不会放弃。

### 3.6 调度器性能分析

Driver 日志中的调度器 profile：

```
[SCHED_PROFILE] total=6.68s ... active=174252 ready_gpu=0 ready_cpu=40
```

- `ready_gpu=0`：没有可用 GPU 槽位
- `active=174252`：巨量活跃任务
- 调度循环每 6.7 秒一次，效率很低

---

## 4. 根因分析

### 4.1 问题本质：CPU actor 起不来导致 GPU actor 释放不了

```
CPU actor 起不来（资源不足）
  ↓
GPU actor 内部 _process_videos_batch() 卡在等 CPU actor 就绪
  ↓
GPU actor 的 streaming task 永远不返回
  ↓
Ray Data 的 DataOpTask 引用链不断
  ↓
_release_running_actor() 只删了 Pool 自己的引用，actor 不被 GC
  ↓
GPU actor 进程永远存活，占着资源
  ↓
反过来又阻止 CPU actor 调度
  ↓
循环死锁
```

### 4.2 为什么 `_release_running_actor()` 不能真正释放 actor

#### 代码逻辑

```python
# python/ray/data/_internal/execution/operators/actor_pool_map_operator.py:1442-1480
def _release_running_actor(self, actor: ray.actor.ActorHandle):
    # NOTE: By default, we remove references to the actor and let ref counting
    # garbage collect the actor, instead of using ray.kill.
    del self._running_actors[actor]      # 只删除 Pool 自己的引用
    del self._actor_to_logical_id[actor] # 只删除 Pool 自己的引用
    # 没有调用 ray.kill()！
```

#### 引用链详细分析

要理解为什么 actor 不被释放，需要追踪从任务提交到 actor 生命周期绑定的**完整引用链**。涉及 4 层对象持有关系：

##### 第 1 层：任务提交 — 创建 streaming generator

当 ActorPool 向 MapWorker actor 提交任务时（`actor_pool_map_operator.py:381-390`）：

```python
gen = actor.submit.options(
    num_returns="streaming",           # ← 关键：streaming 模式
    **self._ray_actor_task_remote_args,
).remote(
    self.data_context, ctx, *input_blocks, ...
)
```

`num_returns="streaming"` 使得这个 remote call 返回的不是普通的 `ObjectRef`，而是一个 `ObjectRefGenerator`。

##### 第 2 层：ObjectRefGenerator 的内部结构

`ObjectRefGenerator`（`python/ray/_private/object_ref_generator.py:32-65`）的核心字段：

```python
class ObjectRefGenerator:
    def __init__(self, generator_ref: "ray.ObjectRef", worker: "Worker"):
        self._generator_ref = generator_ref  # ← 这是关键！
```

**`self._generator_ref` 是一个 `ray.ObjectRef`，它指向 actor 上正在运行的 streaming task。** 在 Ray 的对象生命周期模型中：

- 只要一个 `ObjectRef` 被 Python 代码持有（引用计数 > 0），Ray 的 core worker 就会在 reference counting table 中保留该 object 的引用
- 对于 streaming task 的 generator ref，Ray **必须保持 actor 存活**才能继续从 stream 中产出新的 object
- 这是 Ray 的 **object lineage** 机制保证的：actor 是 stream object 的生产者，生产者不能在消费者还持有引用时死亡

具体调用路径（`_next_sync` 方法，`object_ref_generator.py:188-242`）：

```python
def _next_sync(self, timeout_s=None) -> "ray.ObjectRef":
    core_worker = self.worker.core_worker
    # peek stream —— 通过 _generator_ref 找到对应的 actor task stream
    expected_ref, is_ready = core_worker.peek_object_ref_stream(self._generator_ref)
    if not is_ready:
        ray.wait([expected_ref], timeout=timeout_s, fetch_local=False)
    # 从 stream 中读取下一个 object ref
    ref = core_worker.try_read_next_object_ref_stream(self._generator_ref)
    return ref
```

`core_worker.peek_object_ref_stream(self._generator_ref)` 内部通过 C++ core worker 维护 generator ref → actor task 的映射。只要 `_generator_ref` 被持有，core worker 就知道这个 stream 还有消费者，actor 不能被回收。

##### 第 3 层：DataOpTask 持有 ObjectRefGenerator

`ObjectRefGenerator` 被封装在 `DataOpTask` 中（`physical_operator.py:102-141`）：

```python
class DataOpTask(OpTask):
    def __init__(self, task_index, streaming_gen: ObjectRefGenerator, ...):
        self._streaming_gen = streaming_gen   # ← 持有 generator

    def get_waitable(self) -> ObjectRefGenerator:
        return self._streaming_gen            # ← 暴露给调度循环
```

`DataOpTask` 被存储在 `MapOperator._data_tasks` 字典中（`map_operator.py:627-636`）：

```python
data_task = DataOpTask(task_index, gen, ...)
self._data_tasks[task_index] = data_task     # ← 长期持有！
```

##### 第 4 层：调度循环持续引用

StreamingExecutor 的调度循环（`streaming_executor_state.py:503-545`）每次迭代都会：

```python
# 收集所有活跃任务
active_tasks: Dict[Waitable, Tuple[OpState, OpTask]] = {}
for op, state in topology.items():
    for task in op.get_active_tasks():          # ← 调用 MapOperator.get_active_tasks()
        active_tasks[task.get_waitable()] = ... # ← get_waitable() 返回 _streaming_gen

# ray.wait 轮询所有活跃任务的 waitable
ready, _ = ray.wait(
    list(active_tasks.keys()),    # ← 这里 key 就是 ObjectRefGenerator
    num_returns=len(active_tasks),
    fetch_local=False,
    timeout=0.1,
)
```

而 `MapOperator.get_active_tasks()` 返回的是（`map_operator.py:654-655`）：

```python
def get_active_tasks(self) -> List[OpTask]:
    return list(self._metadata_tasks.values()) + list(self._data_tasks.values())
```

所以只要 `_data_tasks[task_index]` 存在，每次调度循环都会访问 `DataOpTask._streaming_gen`，强化引用。

##### 完整引用链图示

```
┌─ StreamingExecutor 调度循环 (每 0.1s 执行一次) ─────────────────────┐
│                                                                      │
│  for task in op.get_active_tasks():                                  │
│      active_tasks[task.get_waitable()] = ...                         │
│                     │                                                │
│                     ▼                                                │
│  ┌─ MapOperator ──────────────────────────────────────────────┐      │
│  │                                                            │      │
│  │  self._data_tasks[task_index] = DataOpTask                 │      │
│  │                                      │                     │      │
│  │                                      ▼                     │      │
│  │  ┌─ DataOpTask ──────────────────────────────────┐         │      │
│  │  │                                               │         │      │
│  │  │  self._streaming_gen = ObjectRefGenerator     │         │      │
│  │  │                              │                │         │      │
│  │  │                              ▼                │         │      │
│  │  │  ┌─ ObjectRefGenerator ─────────────────┐     │         │      │
│  │  │  │                                      │     │         │      │
│  │  │  │  self._generator_ref = ObjectRef ────────────────────────── ①
│  │  │  │                                      │     │         │      │
│  │  │  └──────────────────────────────────────┘     │         │      │
│  │  └───────────────────────────────────────────────┘         │      │
│  └────────────────────────────────────────────────────────────┘      │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘

                                │
         ① _generator_ref (ObjectRef) 指向 actor 上的 streaming task
                                │
                                ▼

┌─ Ray Core Worker (C++ 层) ─────────────────────────────────────────────┐
│                                                                         │
│  reference_counting_table:                                              │
│    generator_ref → { owner: driver, task: actor_task_xyz }              │
│                                                                         │
│  只要 generator_ref 的引用计数 > 0:                                     │
│    → task 不能被标记为完成（stream 还有消费者）                          │
│    → actor 不能被回收（它是 stream 的生产者）                           │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘

                                │
                                ▼

┌─ MapWorker Actor (GPU actor 进程) ──────────────────────────────────────┐
│                                                                          │
│  正在执行: submit() → _process_videos_batch()                            │
│                                                                          │
│    while has_data:                                                        │
│      self._refill_pending_batches()       # 尝试给 CPU actor 分活        │
│      ready, _ = ray.wait(futures, timeout=0.1)  # 等 CPU actor 完成      │
│      if not ready:                                                       │
│        WARNING "No ready actors, waiting for recovery..."   ← 卡在这里！ │
│        time.sleep(backoff)                                               │
│        continue  # 永远循环                                              │
│                                                                          │
│  内部持有的子 actor:                                                     │
│    self.cpu_actors = [QwenVLCPUPreprocessActor × 16]                     │
│      → 状态: PENDING_CREATION / RESTARTING (起不来)                       │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

##### `_release_running_actor()` 切断的位置 vs 未切断的位置

```
引用路径 A (Pool → Actor):
  Pool._running_actors[actor] → ActorHandle    ──── ✂️ 被 del 切断
  Pool._actor_to_logical_id[actor] → ActorHandle ── ✂️ 被 del 切断

引用路径 B (MapOperator → streaming_gen → actor):
  MapOperator._data_tasks[idx] → DataOpTask        ── ❌ 未切断
    → DataOpTask._streaming_gen → ObjectRefGenerator   ── ❌ 未切断
      → ObjectRefGenerator._generator_ref → ObjectRef    ── ❌ 未切断
        → Ray Core reference_counting_table 维持 actor 存活  ── ❌ 未切断
```

**路径 B 完全没有被触碰**，而路径 B 才是真正让 actor 存活的根本原因。

#### DataOpTask 清理时机

`DataOpTask` 只在以下条件下被清理（`map_operator.py:608-625`）：

```python
def _task_done_callback(task_index, exception):
    self._data_tasks.pop(task_index)  # ← 只有这里才会删除 DataOpTask
    if task_done_callback:
        task_done_callback()           # ← 这里调 on_task_completed()
```

而 `_task_done_callback` 的触发条件在 `DataOpTask.on_data_ready()`（`physical_operator.py:172-178`）：

```python
def on_data_ready(self, max_bytes_to_read):
    while ...:
        try:
            self._pending_block_ref = self._streaming_gen._next_sync(timeout_s=0)
        except StopIteration:            # ← 只有 StopIteration 才触发
            self._task_done_callback(None)  # ← 清理 DataOpTask
            self._has_finished = True
            break
```

**`StopIteration` 只在 streaming generator 耗尽时抛出**。而 generator 耗尽意味着 actor 上的 `submit()` 方法已经 return（所有 yield 都完成了）。

由于 actor 的 `_process_videos_batch()` 永远卡在恢复循环中：
- `submit()` 永远不会 return
- streaming generator 永远不会耗尽
- `StopIteration` 永远不会被抛出
- `_task_done_callback` 永远不会被调用
- `_data_tasks.pop(task_index)` 永远不执行
- `DataOpTask` 和 `ObjectRefGenerator` 永远存在
- `_generator_ref` (ObjectRef) 永远被持有
- Ray Core 永远维持 actor 存活

**这就是为什么 `_release_running_actor()` 删除 Pool 的引用后 actor 仍然不会被释放 —— 因为真正维持 actor 生命的不是 Pool 的引用，而是 streaming task 的 ObjectRef 引用链。**

#### Ray Core 层面：为什么 ObjectRef 能阻止 actor 死亡

Ray 的 actor 生命周期由 **distributed reference counting** 管理。关键规则：

1. **Actor task 的 ObjectRef 被持有 → actor 不能被 GC**
   - 当 driver 持有一个 streaming task 的 `generator_ref`（ObjectRef）时，Ray 的 core worker 在 reference counting table 中记录该 ref 的 owner
   - core worker 知道这个 ObjectRef 对应一个 actor 上的 streaming task
   - 只要 ref 存在，actor 就必须保持存活（因为 stream 可能还需要产出更多 object）

2. **`del` Python 对象 ≠ 释放 ObjectRef**
   - `_release_running_actor()` 中的 `del self._running_actors[actor]` 只是删除了 Python 层面对 `ActorHandle` 的引用
   - 但 `ActorHandle` 只是 actor 的"遥控器"，不是 actor 存活的根本原因
   - 真正的存活锚点是 `ObjectRefGenerator._generator_ref`，它通过 C++ core worker 的 reference counting 系统维持

3. **`ray.kill()` 是唯一能打破这个链条的操作**
   - `ray.kill(actor)` 直接通过 GCS 发送 kill 信号给 actor 的 raylet
   - 它绕过了 reference counting，强制终止 actor 进程
   - actor 死亡后，streaming generator 会收到 actor 失败异常，触发 StopIteration，从而清理 DataOpTask

#### 对比 `shutdown(force=True)`

只有 `_ActorPool.shutdown(force=True)` 才会调用 `ray.kill()`：

```python
# actor_pool_map_operator.py:1422-1440
def _release_running_actors(self, force: bool):
    running = list(self._running_actors.keys())
    on_exit_refs = []
    for actor in running:
        ref = self._release_running_actor(actor)  # 先从 pool 移除
        if ref:
            on_exit_refs.append(ref)
    # 等待优雅关闭
    ray.wait(on_exit_refs, timeout=self._ACTOR_POOL_GRACEFUL_SHUTDOWN_TIMEOUT_S)
    # 只有 force=True 才真正 kill
    if force:
        for actor in running:
            ray.kill(actor)  # ← 这是唯一能打破引用链的操作
```

但 `shutdown()` 只在 operator 完全完成时才触发，而 operator 因为有卡住的任务永远完不成。

#### 总结：两种释放机制为何都失败

| 释放机制 | 正常情况 | 当前卡死情况 |
|----------|----------|-------------|
| **GC 释放**（`_release_running_actor`） | task 完成 → gen 耗尽 → StopIteration → `_data_tasks.pop()` → ObjectRef 引用归零 → actor 被 GC | task 永远不完成 → gen 永远不耗尽 → ObjectRef 引用永远 > 0 → actor 永远存活 |
| **强制 kill**（`shutdown(force=True)`） | operator 标记完成 → 调用 shutdown → `ray.kill()` 终止所有 actor | operator 永远有活跃任务 → 永远不标记完成 → shutdown 永远不被调用 |

根本问题：**两种释放机制都依赖"任务最终会完成"这个假设，而无限恢复循环打破了这个假设。**

### 4.3 为什么 Pool 的延迟缩容也不生效

通过 kcof 调整 pool 大小时，`_ActorPool.scale(delta=-N)` 的行为：

```python
# actor_pool_map_operator.py:1089-1115
elif req.delta < 0:
    num_to_remove = abs(req.delta)
    for _ in range(num_to_remove):
        if self._remove_inactive_actor():    # 只能移除 idle/pending actor
            num_released += 1
    # 对于 busy actor，只能延迟处理
    self._pending_scale_down_count = num_to_remove - num_released
```

延迟缩容只在 `on_task_completed()` 中执行：

```python
# actor_pool_map_operator.py:1289-1305
def on_task_completed(self, actor):
    ...
    if self._pending_scale_down_count > 0:
        self._release_running_actor(actor)
        self._pending_scale_down_count -= 1
```

但 `on_task_completed()` 永远不会被调用（因为任务卡住了），所以延迟缩容永远不执行。

### 4.4 循环死锁的完整闭环

```
┌────────────────────────────────────────────────────────────────────┐
│                                                                    │
│  574 个僵尸 GPU actor (Pool 已不跟踪，但进程还活着)                │
│    持有 ~4000 个 QwenVLCPUPreprocessActor (占用 CPU/Memory)        │
│                                                                    │
│         ↓ 资源被占满                                               │
│                                                                    │
│  5 个活跃 GPU actor 的新 CPU actor 无法调度 (PENDING_CREATION)     │
│                                                                    │
│         ↓ CPU actor 起不来                                         │
│                                                                    │
│  活跃 GPU actor 的 _process_videos_batch() 卡在恢复等待            │
│                                                                    │
│         ↓ streaming task 永远不返回                                │
│                                                                    │
│  DataOpTask 持有 streaming_gen → actor 引用不释放                  │
│                                                                    │
│         ↓ 僵尸 actor 不死                                          │
│                                                                    │
│  operator 无法标记完成 → shutdown(force=True) 不触发               │
│                                                                    │
│         ↓ ray.kill() 永远不被调用                                  │
│                                                                    │
│  回到起点：僵尸 actor 继续占用资源                                 │
│                                                                    │
└────────────────────────────────────────────────────────────────────┘
```

### 4.5 深入理解：为什么不用 `ray.kill()` 而依赖 GC

#### 设计原因：Lineage Reconstruction（血统重建）容错

`_release_running_actor()` 的注释明确说明：

```python
# NOTE: By default, we remove references to the actor and let ref counting
# garbage collect the actor, instead of using ray.kill.
#
# Otherwise, actor cannot be reconstructed for the purposes of produced
# object's lineage reconstruction.
```

**Lineage Reconstruction 是什么？**

当 actor task 产出的 object 存储在某个节点的 object store 中，如果该节点故障导致 object 丢失，下游消费者执行 `ray.get()` 时会触发 **lineage reconstruction** —— Ray 重启该 actor，重放到对应状态，重新执行 task，重新产出丢失的 object。

#### `ray.kill()` 如何破坏重建能力

从 GCS Actor Manager 源码（`src/ray/gcs/actor/gcs_actor_manager.cc:635-647`）：

```cpp
void GcsActorManager::HandleKillActorViaGcs(...) {
    if (no_restart) {
        // ray.kill() 默认 no_restart=True
        DestroyActor(actor_id, GenKilledByApplicationCause(...));
    } else {
        KillActor(actor_id, force_kill);
    }
}
```

`DestroyActor()` 会（`gcs_actor_manager.cc:984-993`）：

```cpp
void GcsActorManager::DestroyActor(const ActorID &actor_id, ...) {
    // 关键：清除 lineage reconstruction 回调
    actor_to_restart_for_lineage_reconstruction_callbacks_.erase(actor_id);
    // ...
    if (!is_restartable) {
        registered_actors_.erase(it);  // 从注册表永久删除！
    }
}
```

之后如果下游需要重建该 actor 产出的 object，`HandleRestartActorForLineageReconstruction()` 被调用时（`gcs_actor_manager.cc:341-346`）：

```cpp
auto iter = registered_actors_.find(actor_id);
if (iter == registered_actors_.end()) {
    // actor 已被永久销毁，无法重建！
    GCS_RPC_SEND_REPLY(..., Status::Invalid("Actor is permanently dead."));
    return;  // 下游收到不可恢复错误
}
```

#### 具体对比：`ray.kill()` vs GC 释放在节点故障时的行为

```
═══ 使用 GC 释放（当前设计）═══

T1: actor 执行 task → 产出 object_A → 存在 node_X
T2: Pool 释放 ActorHandle (scope ref → 0)
      → 但下游还持有 object_A 的 ObjectRef (lineage ref > 0)
      → actor 在 GCS 中仍然 registered
T3: node_X 故障，object_A 丢失
      → 下游 ray.get(object_A) 触发 lineage reconstruction
      → GCS 查 registered_actors_ → 找到 actor → 重启 → 重新产出 object_A
      → 下游成功恢复 ✅

═══ 使用 ray.kill()（如果这样实现）═══

T1: actor 执行 task → 产出 object_A → 存在 node_X
T2: Pool 调 ray.kill(actor, no_restart=True)
      → GCS 执行 DestroyActor()
      → 从 registered_actors_ 永久删除
T3: node_X 故障，object_A 丢失
      → 下游 ray.get(object_A) 触发 lineage reconstruction
      → GCS 查 registered_actors_ → 找不到！
      → 返回 "Actor is permanently dead."
      → 下游收到 RayActorError，整个管线失败 ❌
```

### 4.6 Actor 的两层引用计数与两层生命

#### Ray 的两层引用计数机制

Ray 对 actor 维护了**两种独立的引用计数**：

| 引用类型 | 含义 | 谁持有 | 归零时的效果 |
|---------|------|--------|-------------|
| **Scope reference** | 有代码在使用这个 actor | Python 层的 ActorHandle（Pool、DataOpTask 中的 streaming_gen 等） | 触发 `ReportActorOutOfScope` → 可以 kill actor 进程 |
| **Lineage reference** | actor 产出的 object 还被下游需要 | 下游 operator 持有的 ObjectRef | 归零后 actor 才从 GCS 彻底删除 |

GCS 中对应的两种 actor 死亡原因（`gcs_actor_manager.cc:116-134`）：

```cpp
// Scope ref = 0 → actor 进程被 kill，但可能仍可重启
GenActorOutOfScopeCause:
  "The actor is dead because all references to the actor were removed."

// Scope ref = 0 且 Lineage ref = 0 → actor 彻底删除
GenActorRefDeletedCause:
  "The actor is dead because all references to the actor were removed
   including lineage ref count."
```

#### Actor 的两层生命

| 层面 | 含义 | 何时结束 |
|------|------|---------|
| **进程存活** | actor worker 进程在运行中 | scope ref = 0 时进程被 kill |
| **GCS 注册** | actor 的元数据在 GCS 中，可被重启重建 | scope ref = 0 **且** lineage ref = 0 时才彻底删除 |

#### 完整的 actor 生命周期

```
创建阶段:
  ray.remote(MyActor).remote()
    → GCS 注册 actor (registered_actors_)
    → 调度到某节点
    → actor 进程启动

运行阶段:
  scope ref > 0 (有人持有 ActorHandle / streaming_gen)
  lineage ref > 0 (下游持有其输出 ObjectRef)
    → actor 进程存活
    → GCS 注册信息存在

进程终止阶段 (scope ref = 0, lineage ref > 0):
  → owner 向 GCS 发送 ReportActorOutOfScope
  → GCS 检查是否有 lineage reconstruction 进行中
      (gcs_actor_manager.cc:284-290):
      if (num_restarts_due_to_lineage_reconstruction > request版本):
          // 过时报告，忽略（actor 因 lineage 已重启过）
          return;
  → GCS 调 DestroyActor() kill actor 进程
  → 但 actor 仍在 registered_actors_ 中！
  → 如果下游 object 丢失，GCS 可以 RestartActor() 重建

彻底删除阶段 (scope ref = 0, lineage ref = 0):
  → 所有下游 ObjectRef 都被消费/释放
  → actor 从 registered_actors_ 彻底移除
  → 进入 destroyed_actors_ 缓存
```

#### 关键问题：如果下游还持有 ObjectRef，actor 会被 GC 吗？

**不会彻底 GC。** 具体行为：

1. **Actor 进程**：当 scope ref = 0 时，进程**可以被 kill**（GCS 发送 kill 信号）
2. **GCS 注册**：只要 lineage ref > 0，actor **保持注册**，可被重启用于 lineage reconstruction
3. **彻底删除**：只有当 scope ref = 0 **且** lineage ref = 0 时，actor 才被永久删除

```
下游持有 ObjectRef → lineage ref > 0
  → actor 进程可以被 kill（scope ref = 0 后）
  → 但 GCS 注册信息保留（可重启重建）
  → actor 不会被"彻底 GC"
  → 如果 object 丢失 → GCS 重启 actor → 重建 object → 容错成功
```

#### 但在我们的卡死场景中

当前场景的问题更根本——**连 scope ref 都没归零**，actor 进程都死不了：

```
正常 GC 路径:
  Pool 删引用 → scope ref = 0 → 进程死亡 → GCS 保留注册等 lineage ref 归零

当前卡死:
  Pool 删引用 → 但 streaming_gen 仍持有 scope reference
                → scope ref ≠ 0 → 进程根本不会死
                → 连"进程终止阶段"都到不了
```

streaming_gen 中的 `_generator_ref`（ObjectRef）本质是一个 **scope reference**——它代表"我还在从这个 actor 消费 stream 数据"。只要这个 ref 存在，Ray 就认为 actor 还在被使用中，不会触发 out-of-scope 报告。

#### 设计权衡总结

| 释放策略 | 优点 | 缺点 |
|----------|------|------|
| **GC（当前设计）** | 保证容错：lineage 可重建，不丢数据 | actor 可能因引用链未断而长期存活（如本 case） |
| **`ray.kill()`** | 立刻释放资源，不依赖任何引用归零 | 丧失容错：下游 object 如果丢失则不可恢复 |

在正常场景下，GC 策略是更安全的选择。问题只出现在**任务永远不完成**的异常场景——此时 GC 的前提假设（"引用最终会归零"）被打破。

---

## 5. 问题涉及的关键源码位置

| 文件 | 行号 | 内容 |
|------|------|------|
| `actor_pool_map_operator.py` | 1442-1480 | `_release_running_actor()` — 不调用 ray.kill()，依赖 GC |
| `actor_pool_map_operator.py` | 1050-1117 | `scale()` — 延迟缩容逻辑 |
| `actor_pool_map_operator.py` | 1289-1305 | `on_task_completed()` — 延迟缩容执行点 |
| `actor_pool_map_operator.py` | 1370-1400 | `_remove_inactive_actor()` — 只能移除 idle/pending |
| `actor_pool_map_operator.py` | 1422-1440 | `_release_running_actors(force)` — shutdown 时才 force kill |
| `actor_pool_map_operator.py` | 381-400 | 任务提交 — 创建 streaming_gen 引用链 |
| `map_operator.py` | 583-636 | `_submit_data_task()` — DataOpTask 存储引用 |
| `map_operator.py` | 608-625 | `_task_done_callback` — DataOpTask 清理时机 |
| `physical_operator.py` | 102-141 | `DataOpTask` — 持有 `_streaming_gen` 字段 |
| `physical_operator.py` | 152-178 | `get_waitable()` / `on_data_ready()` — StopIteration 触发清理 |
| `object_ref_generator.py` | 32-65 | `ObjectRefGenerator` — 持有 `_generator_ref` (actor 的 scope ref) |
| `object_ref_generator.py` | 188-242 | `_next_sync()` — 通过 core_worker 操作 object ref stream |
| `streaming_executor_state.py` | 503-545 | 调度循环 — `ray.wait()` 轮询所有 active tasks 的 waitable |
| `default_actor_autoscaler.py` | 104-119 | "完成时释放所有" 逻辑 — 需要 operator 标记完成 |
| `gcs_actor_manager.cc` | 276-295 | `HandleReportActorOutOfScope` — scope ref 归零处理 |
| `gcs_actor_manager.cc` | 335-424 | `HandleRestartActorForLineageReconstruction` — lineage 重建 |
| `gcs_actor_manager.cc` | 635-662 | `HandleKillActorViaGcs` — ray.kill() 的 GCS 处理 |
| `gcs_actor_manager.cc` | 984-1001 | `DestroyActor` — 永久销毁 actor |
| `gcs_actor_manager.cc` | 105-134 | 三种死亡原因：RAY_KILL / OUT_OF_SCOPE / REF_DELETED |

---

## 6. 集群现场数据快照

### 6.1 作业参数

```bash
python pipeline/multi_video_classifier_merge/multishot_video_classifier_pipeline_checkpoint.py \
  --streaming-gpu-concurrency 1000 \
  --streaming-num-gpus 0.5 \
  --streaming-cpu-actor-pool-size 16 \
  --streaming-cpu-actor-num-cpus 2 \
  --streaming-cpu-actor-memory 10737418240 \
  --streaming-prefetch-batches 16 \
  --streaming-batch-timeout-sec 420 \
  --streaming-batch-size 32
```

### 6.2 资源占用计算

```
理论最大:
  GPU actor: 1000 个 × 0.5 GPU = 500 GPU
  CPU actor: 1000 × 16 × 2 CPU = 32000 CPU（远超集群 23500 CPU）
  CPU actor memory: 1000 × 16 × 10GB = 160TB（远超集群 111.89 TiB）

实际状态:
  GPU actor alive: 579 个 × 0.5 GPU ≈ 290 GPU
  CPU actor alive: 4311 个 × 2 CPU = 8622 CPU
  CPU actor pending: 4995 个 × 2 CPU = 9990 CPU（等不到资源）
```

### 6.3 关键时间线

| 时间 | 事件 |
|------|------|
| ~2026-05-19 15:15 | GPU actor 开始报 "No ready actors, waiting for recovery..." |
| 2026-05-20 13:12 | 新的 MapWorker 仍在创建（可能是 kcof 调整后） |
| 2026-05-20 14:58 | 最后一批新 MapWorker 创建 |
| 2026-05-20 16:31 | 诊断时间点，elapsed 已超 90000 秒（~25小时） |

---

## 7. 修复建议

### 7.1 即时缓解（当前作业）

手动 kill 掉不被 Pool 跟踪的僵尸 MapWorker actor：

```python
import ray
ray.init(address='auto')

# 获取所有 alive 的 MapWorker actor
actors = ray.util.list_named_actors(all_namespaces=True)

# 方案1: 通过 ray list actors 获取 actor ID，然后逐个 kill
# 方案2: 直接 cancel 整个 job 并重新提交
```

或者直接取消作业重跑，因为只剩 640 行未处理。

### 7.2 用户代码层修复

**问题**：`_process_videos_batch()` 的恢复循环没有总超时限制。

**修复**：在恢复循环中增加总超时检查，超时后让任务失败返回：

```python
def _process_videos_batch(self):
    start_time = time.time()
    while has_data:
        # 增加总超时检查
        if time.time() - start_time > self.timeout_ms / 1000:
            raise TimeoutError(f"Task timed out after {self.timeout_ms}ms")

        self._refill_pending_batches()
        ready, _ = ray.wait(futures, num_returns=1, timeout=0.1)
        if not ready:
            # 检查是否所有 CPU actor 都不可用
            if not any_actor_ready():
                elapsed = time.time() - last_ready_time
                if elapsed > max_recovery_wait_sec:  # 如 1800 秒
                    raise RuntimeError("All CPU actors unavailable, giving up")
```

### 7.3 Ray Data 框架层修复

**问题**：`_release_running_actor()` 依赖 GC 但在长尾任务场景下 GC 失效。

**修复方案 A**：对延迟缩容增加超时强制 kill

```python
def scale(self, req):
    ...
    if self._pending_scale_down_count > 0:
        # 如果延迟缩容超过阈值时间仍未执行，强制 kill
        if time.time() - self._last_scale_down_request_time > FORCE_KILL_TIMEOUT:
            for actor, state in list(self._running_actors.items()):
                if state.num_tasks_in_flight > 0 and self._pending_scale_down_count > 0:
                    ray.kill(actor)
                    self._pending_scale_down_count -= 1
```

**修复方案 B**：`_release_running_actor()` 在 force 模式下直接调用 `ray.kill()`

```python
def _release_running_actor(self, actor, force=False):
    del self._running_actors[actor]
    del self._actor_to_logical_id[actor]
    if force:
        ray.kill(actor)  # 强制终止，打破引用链
```

### 7.4 配置层修复

**问题**：`actor_max_restarts=-1` 导致 CPU actor 无限重启。

**修复**：设置有限重启次数：

```bash
--streaming-actor-max-restarts 10  # 而非 -1
```

### 7.5 资源规划修复

**问题**：CPU actor 总资源需求远超集群容量。

**计算**：
```
1000 GPU actors × 16 CPU actors × (2 CPU + 10GB) = 32000 CPU + 160TB memory
集群只有 23500 CPU + 111.89 TiB memory
```

**修复**：降低 `--streaming-cpu-actor-pool-size` 或 `--streaming-gpu-concurrency`，确保总资源需求不超过集群容量的 70-80%。

---

## 8. 排查命令速查表

```bash
# 1. 集群资源总览
ray status

# 2. Actor 状态汇总
ray summary actors 2>/dev/null

# 3. 存活的 GPU actor 数量
ray list actors --filter 'state=ALIVE' 2>/dev/null | grep -c 'DistributedQwenVLVideoProcessMapper'

# 4. 存活的 CPU actor 数量
ray list actors --filter 'state=ALIVE' 2>/dev/null | grep -c 'QwenVLCPUPreprocessActor'

# 5. PENDING 的 CPU actor 数量
ray list actors --filter 'state=PENDING_CREATION' 2>/dev/null | grep -c 'QwenVLCPUPreprocessActor'

# 6. Driver 日志查看 pipeline 进度
cat /tmp/ray/session_latest/logs/job-driver-*.log | grep 'Total Progress' | tail -5

# 7. 查看卡住的 GPU actor
cat /tmp/ray/session_latest/logs/job-driver-*.log | grep 'No ready actors' | tail -10

# 8. 查看 actor pool 缩放日志
cat /tmp/ray/session_latest/logs/job-driver-*.log | grep -i 'scale\|pool.*size' | tail -20

# 9. 查看作业参数
ray list jobs 2>/dev/null
```

---

## 9. 经验总结

### 9.1 此类问题的识别特征

1. Pipeline 进度接近 100% 但长时间不动
2. `ray status` 显示有大量 pending demand
3. `ray summary actors` 中 ALIVE 数量远大于 Pool 报告的 Actors 数量
4. Driver 日志有 "No ready actors, waiting for recovery..." 且 elapsed 时间很长

### 9.2 核心教训

| 教训 | 说明 |
|------|------|
| GC-based 释放在长尾任务场景下失效 | 当任务永远不完成时，依赖 GC 的释放策略会彻底失败 |
| 无限重启 + 资源不足 = 死锁 | `actor_max_restarts=-1` 配合资源耗尽会形成永久性死锁 |
| 嵌套 actor 是资源黑洞 | GPU actor 内部创建大量 CPU actor，Ray Data Pool 层无感知也无法管控 |
| 超时机制必须端到端 | 即使配置了 `timeout_ms`，如果恢复循环不检查超时，配置等于无效 |
| 缩容操作需要强制手段 | 仅靠"等 actor 空闲再释放"在卡死场景下永远等不到 |

### 9.3 防范措施

1. **始终设置有限的 `actor_max_restarts`**（如 10 次），避免无限重启
2. **用户代码的恢复循环必须有总超时**，超时后应 raise exception 让 Ray Data 感知到失败
3. **资源规划要计算总量**：`GPU并发 × CPU actor数 × 单个资源需求 < 集群总量 × 0.7`
4. **监控 PENDING_CREATION actor 数量**，持续增长说明资源不足
5. **考虑给 Ray Data 的 ActorPool 增加 force-kill 超时机制**（框架层修复）

---

## 10. Actor Kill 与退出机制补充分析

> 以下内容是对 Ray Actor Kill/退出完整链路的深度分析，与本文僵尸 Actor 问题直接相关。完整文档见 [Actor Kill 与退出机制深度分析](../01-架构与原理/Actor-Kill与退出机制深度分析.md)。

### 10.1 为什么 `__ray_terminate__` 无法解决僵尸 Actor 问题

`__ray_terminate__` 是作为普通 actor task 排队执行的，必须等前面的 task 完成后才能轮到。在本 case 中，GPU actor 的 `_process_videos_batch()` 永远卡在恢复循环中，`__ray_terminate__` 永远排不到执行。

更重要的是，**`__ray_terminate__` 路径没有任何超时兜底机制**：

| 退出路径 | GCS 30s 定时器 | Raylet 5s 定时器 | 最终保障 |
|---------|---------------|-----------------|---------|
| `ray.kill()` | 视路径而定 | ✅ | 5s 内必死 |
| 引用计数出作用域 | ✅ | ✅ | 30s 内必死 |
| `__ray_terminate__` | ❌ 不经过 GCS | ❌ 不经过 Raylet | **无兜底!** |

如果 terminate task 排不到，actor 就永远不会退出。这是 `__ray_terminate__` 的根本局限。

### 10.2 Exit() 可能卡住的内部机制

当 `Exit()` 被调用时（如引用计数出作用域后 GCS 发 `KillActor(force_kill=False)`），CoreWorker 内部的退出流程是：

```
Exit()
  → RequestShutdown(force=false, timeout=无限)     // ← 内部无超时！
    → ExecuteExit()
      → task_manager_->DrainAndShutdown()
        → task_receiver_->Stop()                     // ← 可能永远阻塞！
        → NotifyWorkerBlocked()                      // 只释放 CPU 资源
        → shutdown_callback:
            → DisconnectServices()
            → ExecuteGracefulShutdown()
              → task_execution_service_.stop()        // 停止事件循环
              → Python sys.exit(0)
```

`task_receiver_->Stop()` 会等待当前正在执行的 task 完成。**如果 actor 正在跑死循环或长时间阻塞的 task（如本 case 中的恢复等待循环），这里会永远卡住。**

但 Ray 设计了两层外部超时兜底来保证最终强杀：
1. **Raylet 5s 定时器**：`HandleKillLocalActor` 在发送 KillActor RPC 后启动 5s 定时器，超时后直接 SIGKILL
2. **GCS 30s 定时器**：`DestroyActor(force_kill=False)` 在发送 kill 请求后启动 30s 定时器，超时后重发 `force_kill=True`

### 10.3 本 case 中三层退出机制均失效的原因

| 退出机制 | 正常时 | 本 case 中 | 失效原因 |
|---------|-------|-----------|---------|
| **GC 释放**（scope ref 归零） | Pool 删引用 → scope ref=0 → GCS kill 进程 | scope ref 不归零 | streaming_gen 的 `_generator_ref` 维持 scope ref > 0 |
| **`__ray_terminate__`** | 排队执行 → exit_actor() → 正常退出 | 永远排不到 | 前面 task 卡住 + 无超时兜底 |
| **`ray.kill()`** | GCS → Raylet → ForceExit → 进程死 | 从未被调用 | operator 不完成则 `shutdown(force=True)` 不触发 |

根本问题：三种退出机制**都依赖"任务最终会完成"这个前提假设**，而无限恢复循环打破了这个假设。

### 10.4 正确的 ActorPool 退出策略建议

基于以上分析，对 ActorPool 中的 Actor 退出策略建议：

1. **优先使用 `ray.kill()` 而非 `__ray_terminate__`**：因为 `ray.kill()` 有 Raylet 5s 超时兜底，保证最终退出
2. **如需清理，先调清理方法再 kill**：
   ```python
   actor.cleanup.remote()        # 先清理
   ray.kill(actor)               # 再强杀（有超时兜底）
   ```
3. **给 `__ray_terminate__` 增加超时保护**：在提交 terminate 后设一个定时器，超时则回退到 `ray.kill()`
4. **用户代码必须让 task 可中断**：恢复循环应有总超时，超时后 raise exception 让 task 返回
