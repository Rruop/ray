# Ray Data PendingActor 导致 Shutdown 死锁排查

**日期**: 2026-06-11
**集群**: kml-task-100021093-record-100304159-prod
**Job**: `62000000` / `raysubmit_McXjCBhVEqffsBjB`
**Driver**: `10.29.135.204` pid=`1058625`
**作业**: `python3 faceid_index_add_protocol.py --skip-ann --old-meta-cache-path /ytech_m2v3_hdd/zhengzhiyuan/old_meta_1 --filter-concurrency 50 ...`
**现象**: 作业逻辑已经全部执行完，但 driver 不退出，Ray Job 状态永远 `RUNNING`，dashboard 上最后一个算子 `KafkaJsonBatchWriter` 显示 `13878 / 13883 RUNNING`，`autoscaler` 持续 10+ 小时打印 `No available node types can fulfill resource requests {CPU:16, memory:70GiB}*14`。

---

## 1. 问题现象

### 1.1 用户视角

| 维度 | 现象 |
|---|---|
| Job 状态 | `RUNNING`（已运行 10h+） |
| Dashboard | `OldMetaArrayLookupEnrichBatch` FINISHED；`FaceDetectEnrichStatsBatch` FINISHED；`FillImageSize→Protocol→FullFaceGuard` FINISHED；**`KafkaJsonBatchWriter`** `13878/13883` **RUNNING** |
| Driver Log | `(autoscaler +10h+) No available node types can fulfill resource requests {'CPU': 16.0, 'memory': 75161927680.0}*14` |
| Raylet Log | `There are tasks with infeasible resource requests that cannot be scheduled` |
| 集群 ray status | `34.5/638 CPU`、`140.18 GiB / 1.55 TiB memory`（看似空闲资源充足） |

### 1.2 关键矛盾

- Dashboard 显示最后一个算子还有 5 个 block 没跑完（13878/13883），看起来"卡住了"
- 但集群总资源 638 CPU / 1.55 TiB memory，使用率才 5%，理应能调度
- `Pending Demands: {CPU:16, memory:70 GiB} : 14+ pending tasks/actors` 持续 10 小时无解

---

## 2. 分析过程

### 2.1 第一步：确认 Job 在跑什么、卡在哪

```bash
ray job list | grep raysubmit_McXjCBhVEqffsBjB
# JobStatus.RUNNING, driver_node_ip=10.29.135.204, driver_pid=1058625

ray status
# Pending Demands:
#  {'CPU': 16.0, 'memory': 75161927680.0}: 14+ pending tasks/actors
```

直接给出关键线索：14 个需要 `16 CPU + 70 GiB` 的 task/actor 卡 pending 10 小时。

### 2.2 第二步：定位是哪个 operator 的 actor 在 pending

```bash
ray list tasks --filter 'state=PENDING_NODE_ASSIGNMENT' --limit 20
```

输出全部是 `MapWorker(MapBatches(OldMetaArrayLookupEnrichBatch)).__init__` 与 `.get_location` ——
**14 个 PENDING actor 都属于 `OldMetaArrayLookupEnrichBatch`**（ActorPoolMapOperator）。

```bash
ray list actors \
  --filter 'class_name=MapWorker(MapBatches(OldMetaArrayLookupEnrichBatch))' \
  --filter 'state=PENDING_CREATION' --detail
# num_restarts: 0, death_cause: null  → 从未真正创建过
```

### 2.3 第三步：dashboard 显示矛盾——op 完成 vs actor pending

用户反馈 dashboard 上 OldMeta 已 FINISHED，但 14 个 actor 仍在 PENDING_CREATION。

直接查 streaming executor 的内部日志：

```bash
# pane 1 已 SSH 到 head node 10.29.135.204
ls /tmp/ray/session_latest/logs/ray-data/
# ray-data-dataset_500_0.log  ← 当前数据集

grep -aE 'completed\.' /tmp/ray/session_latest/logs/ray-data/ray-data-dataset_500_0.log
```

发现 7 个 operator **全部已完成**：

```
2026-06-11 01:03:50  InputDataBuffer[Input] completed.
2026-06-11 01:06:34  TaskPoolMapOperator[ReadParquet] completed.
2026-06-11 01:06:59  AddFaceDetectJoinKeyBatch completed.
2026-06-11 02:56:24  ActorPoolMapOperator[OldMetaArrayLookupEnrichBatch] completed.
2026-06-11 02:56:48  FaceDetectEnrichStatsBatch completed.
2026-06-11 03:13:27  FillImageSize→Protocol→FullFaceGuard completed.
2026-06-11 03:13:31  TaskPoolMapOperator[KafkaJsonBatchWriter] completed.   ← 最后一个
```

`dataset_500_0` 是最后一个 dataset（`logs/ray-data/` 里没有 `dataset_501`）。

### 2.4 第四步：为什么 dashboard 上 KafkaJsonBatchWriter 还显示 RUNNING

在 03:13:29 那一秒抓到的 progress log：

```
MapBatches(KafkaJsonBatchWriter): 108499519/108538609
  Tasks: 6; Actors: 0; Queued blocks: 0 (0.0B); Resources: 3.0 CPU, 6.2 GiB object store
```

**那 6 个 task 是正常 drain 的尾巴**：上游在 03:13:27 停止灌 input，KafkaJsonBatchWriter 把队列里最后几个 block 写进 Kafka，单 task 1-2 秒完成属于正常水平。**2 秒后（03:13:31）这 6 个 task 全部跑完，op 标 completed**。

但 driver stdout 里能看到大量 worker SIGSEGV（典型堆栈 `PyTraceBack_Here → gc_collect → CoreWorker::HandleLocalGC`）：

```
[36m(pid=1072917)[0m *** SIGSEGV received ***
   PC: PyTraceBack_Here
   __pyx_f_3ray_7_raylet_gc_collect()
   ray::core::CoreWorker::HandleLocalGC()
```

worker 段错误把 stats actor 的 metrics 推送链路打断，**dashboard 上的 `13878/13883` 就是 stats actor 缓存的最后一个有效快照**——op 已经在 03:13:31 真正完成，但 dashboard 永远定格在 13878。

进一步用 `ray list tasks --filter 'job_id=62000000' --filter 'state=RUNNING'` 验证：当前 raylet 里**没有任何 KafkaJsonBatchWriter task 在跑**，整个 job 只剩 28 个 task —— 14 个 OldMeta actor `__init__` + 14 个 `get_location`，**全部 PENDING_CREATION**。

### 2.5 第五步：查 GCS 日志看 actor 为什么起不来

```bash
ACTOR_ID=12eda1dda60025f89554874262000000
grep -a "$ACTOR_ID" /tmp/ray/session_latest/logs/gcs_server.out | tail -20
```

GCS 反复在两个节点之间 lease：

```
01:03:50.034  Lease worker from b4d3bf1c (worker)  → Failed: resources are not enough
01:03:50.035  Lease worker from f5afbdd7 (head)    → Finished leasing
01:03:50.038  Lease worker from b4d3bf1c           → Failed: resources are not enough
01:03:50.038  Lease worker from f5afbdd7           → Finished leasing
... 高频抖动持续 10 小时 ...
```

GCS scheduler 的 `SOFT spread strategy` 不会因为某次失败就放弃，会一直在候选节点间重试。

### 2.6 第六步：单节点资源拆解

用户疑问"集群有空余 CPU 和内存为啥调度不上去"，需要看 **per-node** 视图（Ray 是 bin-packing per node，不跨节点拼接）：

```python
import ray
ray.init(address='auto')
nodes = ray.nodes()
for n in [n for n in nodes if n['Alive']]:
    res = n['Resources']
    print(f"{n['NodeManagerAddress']:<16} CPU={res.get('CPU',0):>5.1f}  "
          f"mem={res.get('memory',0)/1024**3:>6.1f} GiB")
```

| 节点类型 | 数量 | CPU | 内存 | 能装 16 CPU + 70 GiB？ |
|---|---|---|---|---|
| worker pod | 30 | 20 | **44.7 GiB** | ❌ 单节点最大就 44.7 GiB < 70 GiB，**永久 INFEASIBLE** |
| head (`f5afbdd7`) | 1 | 19 | 124.8 GiB | 物理上能装 1 个 |
| 大 worker (`b4d3bf1c`) | 1 | 19 | 124.8 GiB | 物理上能装 1 个 |

`ray status` 里 `1.55 TiB memory` 是 32 个节点求和，但这个 demand 必须落在**单个节点**上完整分配。30 个 44.7 GiB 节点连 1 个 actor 都装不下；2 个大节点理论上各能装 1 个。

### 2.7 第七步：那 2 个大节点为什么也起不来

```python
from ray._private.state import state
avail = state.available_resources_per_node()
# head: CPU avail=3, memory avail=54.8 GiB
# b4d3bf1c: CPU avail=2, memory avail=54.8 GiB
```

**双重不够**：
- CPU：head 19 总，已用 16；b4d3bf1c 19 总，已用 17。**剩 2-3 个 CPU < 16 demand**
- Memory：两节点都剩 54.8 GiB < 70 GiB demand

### 2.8 第八步：CPU 和内存被谁占了

```python
from ray.util.state import list_actors
alive = list_actors(filters=[
    ('state','=','ALIVE'),
    ('class_name','=','MapWorker(MapBatches(OldMetaArrayLookupEnrichBatch))')
], detail=True)
# 2 个 ALIVE OldMeta actor:
#   actor_id=336ed3f02e... pid=3604102 ip=10.80.246.51 (b4d3bf1c)
#   actor_id=5b52154fed... pid=1058816 ip=10.29.135.204 (head)
```

```bash
ps -p 1058816 -o pid,etime,rss,stat
# 1058816  11:05:39  55.55GiB  SNl
```

**关键发现**：
- 2 个 ALIVE OldMeta actor 各占一个大节点
- 每个 actor Ray 预约 70 GiB（`@ray.remote(memory=...)` 声明），实际 RSS 55.55 GiB（真把 `old_meta_1` cache 加载进 Python heap 了）
- 进程状态 `SNl`（Sleeping，**完全空闲，不在跑 task**）
- `etime=11h05m`：从 dataset_500_0 开始（01:03:50）就一直活着

**这两个 ALIVE actor 已经空挂了 9 小时——op 在 02:56:24 就完成了，但它们没被 kill，70 GiB 内存预约和 16 CPU 一直占着**。

### 2.9 第九步：为什么 ALIVE actor 不被 kill

回到 `OldMetaArrayLookupEnrichBatch` op completed 后的清理流程：

```
op completed (02:56:24)
  → _OpStats.num_active_actors = 0   ← progress log 显示 "Actors: 0"
  → 但 actor 进程仍 ALIVE，等 ActorPool.shutdown() 调 ray.kill()

executor.shutdown() 启动 (在 dataset 整体收尾时触发，理论应该在 03:13:31 之后)
  → for op in dag: op.shutdown()
    → ActorPoolMapOperator.shutdown()
      → ActorPool._kill_running_actors()
        → 等所有 actor 的 init future resolve 后逐个 ray.kill()
        → 14 个 PENDING_CREATION 的 init future 永远不会 resolve  ← 卡在这里
        → 已经 ALIVE 的 2 个 actor 也无法走到 ray.kill() 那一步
```

### 2.10 第十步：闭环死锁完整还原

```
14 个 PENDING_CREATION actor
  ⤷ 等：fat node 上腾出 70 GiB
       ⤷ 等：2 个 ALIVE OldMeta actor 释放
            ⤷ 等：driver 调 ray.kill()
                 ⤷ 等：ActorPool.shutdown() 推进
                      ⤷ 等：14 个 PENDING actor 的 init future resolve
                           ⤷ ◯ 回到起点
```

**OldMeta op 自己 ALIVE 的 actor 在堵自己 PENDING 的 actor**——闭环。

### 2.11 第十一步：Ray Job 为什么一直 RUNNING

Ray Job 状态机本质规则：

| 触发条件 | 转移到 |
|---|---|
| driver 进程 exit_code = 0 | `SUCCEEDED` |
| driver 进程 exit_code ≠ 0 | `FAILED` |
| `ray job stop` 命令 | `STOPPED` |
| **driver 进程还活着，无论它在干什么** | **`RUNNING`** |

```bash
ps -p 1058625 -o pid,stat,etime,cmd
# 1058625  Rl  10:27:50  python3 faceid_index_add_protocol.py ...
```

driver 不是 deadlock 也不是 segfault，它合法地 `await` 在一个永远不 resolve 的 init future 上。OS 看到进程是 `Rl`（runnable），Ray Job Supervisor 看 driver 进程还在 → 永远 RUNNING。

**Ray 默认没有 driver hang 检测、没有 op 级 timeout、没有 actor pool startup deadline。** 唯一相关的兜底开关 `RAY_enable_infeasible_task_early_exit` 默认关闭。

---

## 3. 真实根因

> **作业逻辑已全部跑完（最后一个算子在 03:13:31 已 completed），driver 卡死在 `executor.shutdown()` → `ActorPoolMapOperator.shutdown()` → `ActorPool._kill_running_actors()`，因为它在等 14 个永远不能被创建（资源 infeasible）也不能被 cancel（Ray 2.51 `ray.kill` 对 PENDING_CREATION actor 是 noop）的 OldMeta actor 的 init future。这同时锁住了 2 个已 ALIVE 的 actor 的释放，它们各占大节点 70 GiB 内存配额，正好挡住了同 op 自己 14 个 PENDING actor 的位置——一个 op 自己 ALIVE 的 actor 在堵自己 PENDING 的 actor。**

### 3.1 根因要素链

| # | 要素 | 具体值 |
|---|---|---|
| 1 | Actor 资源需求 ≥ 任意单节点 capacity | demand 70 GiB；30 个 worker 节点单机 44.7 GiB |
| 2 | ActorPoolStrategy + 大内存依赖 | OldMetaArrayLookupEnrichBatch 加载 `old_meta_1` cache 到 55 GiB |
| 3 | 集群之前发生过节点缩容/抢占 | 27+ 节点 NodeTerminated，2 个大节点是仅存的"够格"目标 |
| 4 | 2 个大节点首批起了 2 个 ALIVE actor，吃掉自己的 70 GiB 配额 | 后续无法再容纳 14 个同样 demand 的 actor |
| 5 | op 完成后未主动 kill ALIVE actor | Ray Data ActorPool 把 kill 延迟到 dataset shutdown 阶段 |
| 6 | shutdown 阶段等所有 actor 的 init future | PENDING actor 的 init future 永远不 resolve |
| 7 | Ray 默认无 hang detection | `RAY_enable_infeasible_task_early_exit` 默认关闭 |

### 3.2 为什么 dashboard 上还有 6 个 task

回答用户具体疑问：

- 03:13:29 progress log：`Tasks: 6` 是**那一秒的瞬时快照**，6 个是 KafkaJsonBatchWriter 在 drain 最后几个 block 的活 task
- 03:13:31 op 标 completed —— 这 6 个 task 在 2 秒内正常跑完
- dashboard 显示 `13878 / 13883 RUNNING` 是因为 worker SIGSEGV 把 stats actor 的 metrics 推送通道打断，**最后那次刷新留在 13878 永远没下次**——它不是真还在跑 6 个 task，是 stats 数据过期

实时验证方法：
```bash
ray list tasks --filter 'job_id=62000000' --filter 'state=RUNNING' --limit 50
# 当前结果：只剩 _StatsActor / ActorLocationTracker / _AutoscalingCoordinator 等 Ray 内部 actor task
# 业务 task（KafkaJsonBatchWriter / OldMeta / 其他 op）一个都没有
```

### 3.3 日志指标延迟的完整代码级分析

> 本节从 Ray Data 源码层面完整追踪日志中两个关键指标——`Tasks: N` 和 `X/Y`（`row_outputs_taken/num_output_rows_total`）——从产生到显示的完整链路，解释它们为什么在正常运行时就有延迟，以及作业结束时为什么永远不会更新到最终值。

#### 3.3.1 两个指标的独立数据来源

日志中每一行涉及两个独立指标，走不同的更新链路：

| 日志内容 | 变量 | 含义 | 更新时机 |
|---------|------|------|---------|
| `108485198/108532381` | `row_outputs_taken` / `num_output_rows_total()` | 已消费行数 / 估算总行数 | 下游消费 output 时 |
| `Tasks: 20; Actors: 0` | `num_active_tasks()` / `get_actor_info()` | 在飞 task 数 / actor 池状态 | task 完成 callback 时 |

#### 3.3.2 唯一的数据驱动源：调度循环线程

**日志和 Dashboard 的所有数据都来自同一条执行链——调度循环线程**，这是 `StreamingExecutor` 继承 `Thread` 的 `run()` 方法：

```python
# streaming_executor.py:465-489
class StreamingExecutor(Thread):
    def run(self):
        while True:
            continue_sched = self._scheduling_loop_step(self._topology)
            if not continue_sched or self._shutdown:
                break
```

每一轮 `_scheduling_loop_step()` 尾部做两件事：

```python
# streaming_executor.py:647-653
update_operator_states(topology)
self._refresh_progress_manager(topology)     # ← ① 更新并打印日志
self._update_stats_metrics(...)               # ← ② 推送 dashboard 数据
```

**一旦这个 while 循环退出，日志和 dashboard 都不会再收到新数据。**

#### 3.3.3 `Tasks: N` 的更新链路

`Tasks: N` 来自 `op.num_active_tasks()`，对 `MapOperator`（包括 `TaskPoolMapOperator` 和 `ActorPoolMapOperator`）的实现为：

```python
# map_operator.py:731-740
def num_active_tasks(self) -> int:
    return len(self._data_tasks)
```

`_data_tasks` 是一个 dict，增减时机：

```python
# map_operator.py:642 — 提交 task 时加入
self._data_tasks[task_index] = data_task

# map_operator.py:621 — task 完成回调时移除
def _task_done_callback(task_index, exception):
    ...
    self._data_tasks.pop(task_index)
```

**`Tasks: N` = 当前已提交但 `_task_done_callback` 还没触发的 data task 数量。**

`_task_done_callback` 的触发链路：

```
Worker 执行 ray task 完成
  → Ray Core 将 ObjectRef 标记为 ready
  → ray.wait() 返回 ready ref                            # 0~100ms 延迟
  → process_completed_tasks() 处理 ready task
  → task._on_data_ready() / task done 逻辑
  → _task_done_callback() 触发 → _data_tasks.pop()       # Tasks: N 减 1
```

**`ray.wait()` 的 100ms 超时**是主要的微观延迟源：

```python
# streaming_executor_state.py:542-545
ready, _ = ray.wait(
    list(active_tasks.keys()),
    num_returns=len(active_tasks),
    fetch_local=False,
    timeout=0.1,        # ← 100ms 超时
)
```

即使 worker 已经把 task 跑完了，driver 端也要等到下一轮 `ray.wait()` 才能感知到。那些没被 `ray.wait()` 返回的 task，要等到下一轮调度循环才能处理。

**日志打印节流 10 秒**：

```python
# logging_progress.py:99, 167-169
LOG_REPORT_INTERVAL_SEC = env_integer("RAY_DATA_NON_TTY_PROGRESS_LOG_INTERVAL", 10)

def refresh(self):
    if current_time - self._last_log_time < self.LOG_REPORT_INTERVAL_SEC:
        return      # ← 不到 10 秒不打印
```

`update_operator_progress()` **每轮调度循环都调用**，更新 `_LoggingMetrics` 对象内部值；但 `refresh()` 只在间隔 ≥10 秒后打印。**打印时读的是那一刻的内部值，所以 `Tasks: N` 反映的是打印瞬间的真实 `len(_data_tasks)`，不是 10 秒前的旧值。**

`Tasks: N` 在正常情况下的延迟总结：

| 延迟来源 | 延迟量 | 是否"旧数据" |
|---------|--------|-------------|
| `ray.wait` 周期 | 0~100ms | 否，打印瞬间实时值 |
| 日志打印节流 | 0~10s | 否，值在每次 refresh 时已更新到最新 |

#### 3.3.4 `X/Y`（`row_outputs_taken/num_output_rows_total`）的更新链路

**这是真正有显著延迟的指标。** 一个 block 从 worker 产出到被计入 `row_outputs_taken`，需要经历 5 个阶段：

```
阶段 ❶：Worker 执行 ray task，产出 block
  ↓ (streaming generator 通过 ObjectRef 传回 driver)

阶段 ❷：ray.wait() 返回 ready ref
  ↓ (driver 端感知到有新 block 可读)

阶段 ❸：process_completed_tasks() 读取 block
  ↓ task._on_data_ready() 触发 _output_ready_callback()
  ↓
  # map_operator.py:603-607
  def _output_ready_callback(task_index, output):
      self._metrics.on_task_output_generated(task_index, output)   # ← 记录产出
      self._output_queue.add(output, key=task_index)              # ← 放入 operator 内部队列
      self._metrics.on_output_queued(output)
  ↓

阶段 ❹：process_completed_tasks() 末尾拉取 output
  ↓
  # streaming_executor_state.py:735-736
  for op, op_state in topology.items():
      while op.has_next():
          op_state.add_output(op.get_next())
  ↓
  op.get_next() → _get_next_inner() → _output_queue.get_next()
    → self._metrics.on_output_dequeued(bundle)          # ← 从内部队列出队
    → 返回 bundle
  ↓
  op_state.add_output(ref) → self.output_queue.append(ref)  # ← 放入 OpState 外部队列
  ↓

阶段 ❺：下游 operator 消费（或 _ClosingIterator 取走）
  ↓
  最终调用链到达 PhysicalOperator.get_next()：
  ↓
  # physical_operator.py:748-757
  def get_next(self) -> RefBundle:
      output = self._get_next_inner()
      self._metrics.on_output_taken(output)    # ← ★ row_outputs_taken 在这里累加
      return output
```

**`row_outputs_taken` 只在阶段 ❺ 才更新！** 而 `Tasks: N` 在阶段 ❸ 的 `_task_done_callback` 就已经更新了（`_data_tasks.pop`）。

各阶段延迟：

| 阶段 | 延迟 | 说明 |
|------|------|------|
| ❶→❷ | 0~100ms | `ray.wait(timeout=0.1s)` 采样周期 |
| ❷→❸ | 同一轮循环内 | `process_completed_tasks()` 里连续处理 |
| ❸→❹ | 同一轮循环内 | Phase 5 立即拉取 |
| ❹→❺ | **0~数秒** | 取决于下游何时消费 |

**阶段 ❹→❺ 的延迟最大**，因为：

- 如果下游 operator 的 inqueue 满了（backpressure），`dispatch_next_task()` 不会立即取走 bundle
- 如果下游是 `_ClosingIterator`（最终消费者），它由**用户线程**驱动，与调度循环线程是**不同线程**：

```python
# streaming_executor.py:1107-1123
class _ClosingIterator(OutputIterator):
    def get_next(self, output_split_idx=None):
        try:
            op, state = self._executor._output_node
            bundle = state.get_output_blocking(output_split_idx)  # ← 阻塞等 output_queue
            self._executor._progress_manager.update_total_progress(
                bundle.num_rows() or 0, op.num_output_rows_total()
            )
            return bundle
        except BaseException as e:
            self._executor.shutdown(
                force=False, exception=e if not isinstance(e, StopIteration) else None
            )
            raise
```

`state.get_output_blocking()` 从 `OpState.output_queue` 取数据，队列空时每 10ms 轮询：

```python
# streaming_executor_state.py:405-415
def get_output_blocking(self, output_split_idx):
    while True:
        if self._finished and not self.output_queue.has_next(output_split_idx):
            raise StopIteration()
        ref = self.output_queue.pop(output_split_idx)
        if ref is not None:
            ...
            return ref
        time.sleep(0.01)    # ← 队列空时每 10ms 轮询
```

**用户线程取走 bundle 时，才触发 `PhysicalOperator.get_next()` → `on_output_taken()` → `row_outputs_taken +=`**。

具体场景示例：

```
时刻 T：KafkaJsonBatchWriter 的 20 个 task 全部在 worker 上完成
  ↓
T+0.05s：ray.wait() 返回 ready refs
  ↓
T+0.05s：process_completed_tasks() 处理：
  → _output_ready_callback() 把 block 放入 _output_queue（阶段 ❸）
  → _task_done_callback() → _data_tasks.pop()  ← Tasks: 20 变成 0
  → op_state.add_output(op.get_next()) 把 bundle 放入 OpState.output_queue（阶段 ❹）
  ↓
此时：
  Tasks: 0       ← 已更新为 0（所有 task 完成了）
  X/Y: 108425114/108534492   ← row_outputs_taken 还没更新！
                                因为 _ClosingIterator 还没调 get_next()
  ↓
T+0.05s ~ T+1s：_ClosingIterator.get_next() 逐步消费 output_queue 中的 bundle
  → 每次消费一个 bundle → get_next() → on_output_taken() → row_outputs_taken += N
  ↓
T+1s：
  X/Y: 108534492/108534492   ← 现在才追平
```

**`Tasks: N` 在 T+0.05s 就变成 0 了，但 `row_outputs_taken` 可能要等到 T+1s 才追上。这之间有 0~数秒的窗口，日志会显示 `Tasks: 0` 但进度 `X < Y`。**

#### 3.3.5 `num_output_rows_total()`（Y/分母）的更新延迟

Y 不是固定值，它随 task 完成动态调整：

```python
# map_operator.py:622-632
def _task_done_callback(task_index, exception):
    ...
    (_, self._estimated_num_output_bundles,
     self._estimated_output_num_rows,          # ← ★ 更新 Y
    ) = estimate_total_num_of_blocks(
        self._next_data_task_idx, self.upstream_op_num_outputs(), self._metrics
    )
    self._data_tasks.pop(task_index)
```

**Y 只在 `_task_done_callback` 触发时更新**，也有 `ray.wait()` 100ms 的延迟。此外 Y 是估算值，随着更多 task 完成，估算会越来越精确。

#### 3.3.6 Dashboard 推送的额外延迟

Dashboard 比日志有额外的 5 秒推送节流：

```python
# streaming_executor.py:83
UPDATE_METRICS_INTERVAL_S: float = 5.0

# streaming_executor.py:839-851
def _update_stats_metrics(self, state, force_update=False):
    now = time.time()
    if force_update or (now - self._metrics_last_updated) > self.UPDATE_METRICS_INTERVAL_S:
        _StatsManager.update_execution_metrics(...)
        self._metrics_last_updated = now
```

且推送给 `_StatsActor` 是 fire-and-forget（不 `ray.get()`），有 RPC 延迟，不保证送达：

```python
# stats.py:602-621
@staticmethod
def update_execution_metrics(dataset_tag, op_metrics, operator_tags, state):
    ...
    try:
        get_or_create_stats_actor().update_execution_metrics.remote(*args)
    except Exception as e:
        logger.warning(f"Error occurred during update_execution_metrics.remote call: {e}")
        return  # ← 吞掉异常，推送丢失也不报错
```

#### 3.3.7 正常运行时的延迟总结

| 指标 | 延迟来源 | 延迟量 | 是否"旧数据" |
|------|---------|--------|-------------|
| `Tasks: N` | `ray.wait` 周期 | 0~100ms | 否，打印瞬间实时值 |
| `X/Y` 进度 | `row_outputs_taken` 需等下游消费 | 0.1s~数秒 | 是，X 落后于实际已完成量 |
| Dashboard | 5s 推送间隔 + RPC 延迟 | 5s+ | 是，比日志更旧 |

#### 3.3.8 作业结束时：日志和 Dashboard 永远不再更新到最终值

##### Shutdown 触发机制

作业逻辑全部跑完后，`_ClosingIterator.get_next()` 取不到数据抛 `StopIteration`，自动触发 `shutdown()`：

```python
# streaming_executor.py:1107-1123
class _ClosingIterator(OutputIterator):
    def get_next(self, output_split_idx=None):
        try:
            op, state = self._executor._output_node
            bundle = state.get_output_blocking(output_split_idx)
            ...
            return bundle
        except BaseException as e:
            self._executor.shutdown(
                force=False,
                exception=e if not isinstance(e, StopIteration) else None,
            )
            raise
```

同时，调度循环自身检测到所有 op 完成也会退出：

```python
# streaming_executor.py:465-489
def run(self):
    while True:
        continue_sched = self._scheduling_loop_step(self._topology)
        if not continue_sched or self._shutdown:
            break
```

```python
# streaming_executor.py:667-668
return not all(op.has_completed() for op in topology)
# ← 所有 op 完成时返回 False → while 循环 break → 线程退出
```

##### Shutdown 时序代码追踪

```python
# streaming_executor.py:272-324
def shutdown(self, force, exception=None):
    ...
    self._shutdown = True                          # ❶ 标记关闭
    self.join(timeout=2.0)                         # ❷ 等调度线程退出（最多2秒）
                                                    #    → 调度线程 while True 检测到
                                                    #      self._shutdown == True → break
                                                    #    → run() 返回 → 线程死亡
                                                    #    ★ 从此 _scheduling_loop_step() 永远不再执行
                                                    #    ★ _refresh_progress_manager() 永远不再调用
                                                    #    ★ _update_stats_metrics() 永远不再调用
    self._update_stats_metrics(                    # ❸ 最后一次推送 dashboard（force_update=True）
        state=DatasetState.FINISHED.name,
        force_update=True,
    )

    self._final_stats = self._generate_stats()     # ❹ 冻结 stats
    ...
    self._progress_manager.close_with_finishing_description(  # ❺ 关闭 progress manager
        desc, exception is None
    )
    logger.info(desc)

    timer = Timer()
    for op in self._topology.keys():               # ❻ 逐个 shutdown operator
        op.shutdown(timer, force=force)             # ← ★ 卡在这里 ★
```

**关键时序**：

| 步骤 | 事件 | 后果 |
|------|------|------|
| ❶ | `_shutdown = True` | 调度循环将在下一次迭代退出 |
| ❷ | `self.join(2.0)` | 调度线程已死亡，**日志和 dashboard 的数据源永久断开** |
| ❸ | `_update_stats_metrics(force=True)` | 最后一次推送快照给 dashboard（用的是❷那一刻的 metrics） |
| ❺ | `close_with_finishing_description()` | 对 `LoggingExecutionProgressManager` 是空操作：`pass` |
| ❻ | `op.shutdown()` | 死锁在 ActorPool（见下文） |

##### 为什么 `Tasks: N` 也不会打印出 0

调度循环最后一轮执行到 `_refresh_progress_manager()` 时，**所有 op 的 `num_active_tasks()` 一定已经是 0**（因为 `has_completed()` 要求 `num_active_tasks() == 0`）：

```python
# physical_operator.py:530-548
def has_execution_finished(self) -> bool:
    return (
        self._is_execution_marked_finished
        or (
            self._inputs_complete
            and self.num_active_tasks() == 0          # ← ★ 必须为 0
            and internal_input_queue_num_blocks == 0
        )
    )
```

而 `_refresh_progress_manager()` 在判断退出**之前**调用：

```python
# streaming_executor.py:647-668
update_operator_states(topology)
self._refresh_progress_manager(topology)     # ← ❶ 此时 num_active_tasks() = 0
self._update_stats_metrics(...)
...
return not all(op.has_completed() ...)       # ← ❷ 返回 False → 退出
```

**所以如果没有 10 秒节流，❶ 处一定会打印 `Tasks: 0`。** 但因为有节流：

```python
# logging_progress.py:167-169
def refresh(self):
    if current_time - self._last_log_time < self.LOG_REPORT_INTERVAL_SEC:
        return                    # ← 如果距上次打印 < 10s，这行不打印
```

**如果最后一轮调度循环距离上一次打印不足 10 秒，`Tasks: 0` 就不会被打印出来。** 日志最后定格在 `Tasks: 20`，不是因为 task 没跑完，而是**打印节流恰好把最后一轮的 `Tasks: 0` 吞掉了**。

##### 为什么 `X/Y` 永远不会显示 100%

最后一轮调度循环打印日志时，`_ClosingIterator` 可能还没把 `output_queue` 里剩余的 bundle 全部消费完：

```
最后一轮调度循环：
  → _refresh_progress_manager() 读取 row_outputs_taken = 108485198（还没追上）
  → refresh() 判断距上次打印 ≥ 10s → 打印 "108485198/108532381"
  → return not all(op.has_completed()...) = False → 退出
  → join(2s) → 调度线程死亡

之后：
  → _ClosingIterator 继续消费 output_queue → row_outputs_taken 继续增长
  → 但调度线程已死 → _refresh_progress_manager() 不再调用 → 不会再打印
  → 最终 row_outputs_taken 追上 Y，但没人读了
  → 日志永远定格在 "108485198/108532381"
```

##### Dashboard 定格在更旧的值

`_update_stats_metrics(force=True)` 在 `join(2s)` 之后执行，此时调度线程已死，但 `_ClosingIterator` 可能还在消费（主线程），`row_outputs_taken` 在增长。**`force_update=True` 读的是那个时刻的快照，可能比最后一轮日志更旧。** 加上 fire-and-forget 不保证送达，如果 worker SIGSEGV 打断了 RPC 通道，dashboard 定格的值可能更旧。

#### 3.3.9 作业结束时延迟总结

| 指标 | 发生了什么 | 最终值会显示吗 |
|------|-----------|--------------|
| `Tasks: N` | 调度线程最后一轮 `num_active_tasks()=0`，但可能被 10s 节流吞掉 | **可能不显示 0**，定格在之前的 `Tasks: N` |
| `X/Y` 进度 | 最后一轮打印时 `_ClosingIterator` 可能还没消费完 | **不会显示 100%**，定格在 `< 100%` |
| Dashboard | `force_update=True` 推送那一刻的快照 | **不会显示 100%**，定格在更旧的值 |
| 之后 | 调度线程死 → `op.shutdown()` 卡死 → 永远不再更新 | **永远定格** |

#### 3.3.10 Shutdown 死锁的完整代码链路

`op.shutdown()` 卡死的具体代码路径：

```
executor.shutdown()                                    # streaming_executor.py:272
  → for op: op.shutdown(timer, force)                  # ❻
    → PhysicalOperator.shutdown()                       # physical_operator.py:802
      → self._shutdown = True
      → self._do_shutdown(force)
        → ActorPoolMapOperator._do_shutdown()           # actor_pool_map_operator.py:478
          → self._actor_pool.shutdown(force=force)       # ❶ 先 shutdown actor pool
          → super()._do_shutdown(force)                  # ❷ 再调基类
```

**❶ _actor_pool.shutdown(force=False)**：

```python
# actor_pool_map_operator.py:1408-1438
def shutdown(self, force=False):
    self._release_pending_actors(force=force)     # ← 释放 pending actors
    self._release_running_actors(force=force)     # ← 释放 running actors
```

`_release_pending_actors(force=False)` 只是清空 Python 端的 dict，**不会向 GCS 发 `CancelActorCreation`**：

```python
# actor_pool_map_operator.py:1440-1449
def _release_pending_actors(self, force):
    pending = dict(self._pending_actors)
    self._pending_actors.clear()           # ← 只是清空 Python 端的 dict
    if force:
        for _, actor in pending.items():
            ray.kill(actor)                # ← force=False 时这行不执行
```

**❷ super()._do_shutdown(force)** 最终调到 `_cancel_active_tasks(force=False)`：

```python
# physical_operator.py:931-949
def _cancel_active_tasks(self, force):
    tasks = self.get_active_tasks()        # ← 获取所有 active tasks
    for task in tasks:
        task._cancel(force=force)          # ← force=False 只是"请求"取消，不等待
    if force:                              # ← force=False 不等
        for task in tasks:
            ray.get(task.get_waitable())   # ← force=True 时才等完成
```

对于 `ActorPoolMapOperator`，`get_active_tasks()` 返回 `_metadata_tasks + _data_tasks`：

```python
# map_operator.py:661
def get_active_tasks(self):
    return list(self._metadata_tasks.values()) + list(self._data_tasks.values())
```

**`_metadata_tasks` 里包含的就是 14 个 PENDING actor 的 `get_location.remote()` 的 init future**——它们在 actor 创建时作为 `MetadataOpTask` 注册：

```python
# actor_pool_map_operator.py:316-335
def _start_actor(...):
    actor = self._actor_cls.options(...).remote(...)
    res_ref = actor.get_location.remote()               # ← PENDING actor 的 ready future

    def _task_done_callback(res_ref):
        has_actor = self._actor_pool.pending_to_running(res_ref)  # ← 等 ready 后转入 running

    self._submit_metadata_task(                          # ← 作为 metadata task 提交
        res_ref,
        lambda: _task_done_callback(res_ref),
    )
```

这些 `res_ref` 指向的 ObjectRef 永远不会 resolve（GCS 无法调度 actor），所以 `MetadataOpTask._cancel(force=False)` 释放了引用，但 GCS 端 actor 仍然是 `PENDING_CREATION`，资源需求仍然占着。

##### 完整死锁因果链

```
调度线程在 join(2s) 后死亡
  → _refresh_progress_manager() 不再调用 → 日志不再打印
  → _update_stats_metrics() 不再调用 → Dashboard 不再更新

executor.shutdown() 继续执行到 ❻
  → ActorPoolMapOperator.shutdown()
    → _actor_pool.shutdown(force=False)
      → _release_pending_actors(force=False)    → 只清 dict，GCS 端 actor 仍 PENDING_CREATION
      → _release_running_actors(force=False)    → 对 2 个 ALIVE actor 调 on_exit，成功
    → super()._do_shutdown(force=False)
      → PhysicalOperator._cancel_active_tasks(force=False)
        → 14 个 MetadataOpTask 的 res_ref（get_location.remote() 的 ObjectRef）
          → PENDING actor 永远不会创建成功 → res_ref 永远不会 resolve
          → MetadataOpTask._cancel(force=False) 只是释放引用，不等完成
          → 但 driver 进程的 await/event 仍在等待这些 future
          → ★ 整个 shutdown 链条卡在这里 ★
```

同时 2 个 ALIVE actor 已经被 release 了（`on_exit.remote()` 发出），但它们的资源还没真正释放（GCS 端的 actor 进程还在），所以：

```
14 个 PENDING actor 等待 70 GiB 内存配额
  ⤷ 2 个大节点的 70 GiB 配额被 2 个 ALIVE actor 占着
       ⤷ ALIVE actor 等 driver 调 ray.kill() 释放
            ⤷ driver 卡在 _cancel_active_tasks() 等 14 个 PENDING future
                 ⤷ 14 个 PENDING future 永远不 resolve
                      ⤷ ◯ 闭环死锁
```

#### 3.3.11 Shutdown 是自动触发的

`shutdown()` 不是用户手动调用的，而是作业逻辑执行完后**自动触发**的。当数据消费完，`_ClosingIterator.get_next()` 抛出 `StopIteration`：

```python
# streaming_executor.py:1107-1123
class _ClosingIterator(OutputIterator):
    def get_next(self, output_split_idx=None):
        try:
            bundle = state.get_output_blocking(output_split_idx)
            return bundle
        except BaseException as e:
            self._executor.shutdown(
                force=False,
                exception=e if not isinstance(e, StopIteration) else None,
            )
            raise
```

`shutdown(force=False)` 中的 `force=False` 是关键——它选择"优雅关闭"而非强制 kill，而优雅关闭需要等所有 PENDING actor 的 init future resolve，这正是死锁的根源。

---

## 4. 解决办法

### 4.1 紧急处理（让当前作业立即结束）

```bash
ray job stop raysubmit_McXjCBhVEqffsBjB
# 给 driver SIGTERM → driver 退出 → job 转 STOPPED
```

⚠️ 作业实际已经完成（数据已经全部写到 Kafka），强制 stop 只是清理 driver。

如果 `ray job stop` 也卡住，直接 kill driver：
```bash
kill -9 1058625   # driver pid
# job 转 FAILED
```

### 4.2 短期规避（下次作业不再触发）

#### 方案 A：降低 actor 内存需求（推荐）

把 `OldMetaArrayLookupEnrichBatch` 单 actor 的 70 GiB 内存降到能 fit 普通 worker 节点（≤ 40 GiB）：

| 改造方式 | 实现 |
|---|---|
| **按 partition 加载 cache** | 把 `old_meta_1` 按 hash 分片，actor 只加载本分片 |
| **走 Plasma 共享内存** | `ray.put(old_meta_dict)` 后多 actor 共享 ObjectRef，不再各自 load |
| **改 mmap** | `numpy.memmap` 或 Apache Arrow 文件，按需 page，不占 anonymous heap |
| **改 Redis/外部 KV** | actor 改成查表 client，不再持有 cache |

#### 方案 B：调整并发到集群能 fit 的数量

如果暂时不能改 cache 加载方式，把 `--filter-concurrency` 调到 ≤ 大节点数（本集群 2），但吞吐会显著下降。

#### 方案 C：开启 hang 兜底

启动 driver 时加环境变量：
```bash
RAY_enable_infeasible_task_early_exit=true python3 faceid_index_add_protocol.py ...
```

效果：actor infeasible 超过阈值时 raylet 直接 fail 这个 actor，driver 收到异常退出，job 转 FAILED——**不能让作业跑通，但能避免 hang 10h**。

#### 方案 D：关闭 Ray Data autoscaler（参考社区 issue [#40604](https://github.com/ray-project/ray/issues/40604)）

```python
import ray.data
ctx = ray.data.DataContext.get_current()
# 关掉 actor pool autoscaling，用固定 size
```

### 4.3 集群侧治本

向 KubeRay / RayCluster 配置增加大内存节点 type（≥ 80 GiB），让 14 个 actor 能调度起来：

```yaml
workerGroupSpecs:
  - replicas: 0
    minReplicas: 0
    maxReplicas: 16
    groupName: large-mem
    rayStartParams:
      memory: "85899345920"   # 80 GiB
    template:
      spec:
        containers:
        - resources:
            limits: { memory: "90Gi", cpu: "20" }
```

autoscaler 看到 `{CPU:16, memory:70 GiB}` 的 pending demand 就能拉起这个 group。

### 4.4 Ray 层根治（长期方向）

社区相关 issue（**未提到本案**这种 PENDING actor 阻塞 shutdown 的具体场景，建议提交新 issue）：

| Issue | 关联点 |
|---|---|
| [#46319](https://github.com/ray-project/ray/issues/46319) | [Data] Execution hangs when there are many operators in the pipeline (Open, P1) |
| [#62746](https://github.com/ray-project/ray/issues/62746) | [Data] Dead Actor occupies actor pool slots (Open) — `actor_pool_map_operator.py` 同类缺陷的不同分支 |
| [#40604](https://github.com/ray-project/ray/issues/40604) | [data] Fix compatibility issues with autoscaler (Open) — actor pool startup timeout 不兜底 |
| [#47846](https://github.com/ray-project/ray/issues/47846) | [data] Mark restarting actors are pending actors (Open) |

修复方向（需要改 Ray 源码）：
1. `ActorPool.shutdown()` 对 PENDING_CREATION actor 显式向 GCS 发 `CancelActorCreation`，而不是只在 driver 端释放引用
2. op completed 后立即 kill ALIVE actor，不等 dataset shutdown（释放它们占的资源给 PENDING actor）
3. 增加 actor pool startup timeout，超时 fail dataset 而不是 hang

---

## 5. 排查命令速查表

```bash
# 1. 找 Job 和 driver
ray job list 2>&1 | grep <submission_id>

# 2. 集群整体
ray status

# 3. 卡住的 task / actor
ray list tasks --filter 'state=PENDING_NODE_ASSIGNMENT' --limit 20
ray list actors --filter 'state=PENDING_CREATION' --detail
ray list actors --filter 'class_name=MapWorker(MapBatches(XXX))' --filter 'state=ALIVE'

# 4. 当前 RUNNING 的真实 task（业务 task vs Ray 内部 actor task）
ray list tasks --filter 'job_id=<JOB_ID>' --filter 'state=RUNNING' --limit 50

# 5. Per-node 资源（关键：bin-packing per node）
python -c "
import ray; ray.init(address='auto')
for n in ray.nodes():
    if not n['Alive']: continue
    r = n['Resources']
    print(f\"{n['NodeManagerAddress']:<16} CPU={r.get('CPU',0):>5} mem={r.get('memory',0)/1024**3:>6.1f}GiB\")
"

# 6. Streaming executor 内部状态（op 是否真 completed）
grep -aE 'completed\.' /tmp/ray/session_latest/logs/ray-data/ray-data-dataset_*.log

# 7. GCS actor scheduler（actor 为什么起不来）
grep -a "<actor_id>" /tmp/ray/session_latest/logs/gcs_server.out

# 8. Driver / worker SIGSEGV
grep -aE 'SIGSEGV|Fatal Python error' \
  /tmp/ray/session_latest/logs/job-driver-<sub_id>.log \
  /tmp/ray/session_latest/logs/python-core-worker-*.log

# 9. Driver 进程是否还活着
ps -p <driver_pid> -o pid,stat,etime,cmd

# 10. ALIVE actor 实际 RSS
ps -p <actor_pid> -o pid,etime,rss,stat
```

---

## 6. 关键日志文件位置

全部在 `head node` 的 `/tmp/ray/session_latest/logs/`：

| 文件 | 内容 | 用途 |
|---|---|---|
| `job-driver-<sub_id>.log` | driver stdout/stderr，autoscaler 警告，worker SIGSEGV | 看作业整体进展 |
| `ray-data/ray-data-dataset_<N>.log` | streaming executor 每个 dataset 的内部日志：op 完成、scheduler 决策、backpressure | **判断 op 是否真完成，看 `completed.`** |
| `gcs_server.out` / `gcs_server.err` | GCS actor scheduler 的真实 lease 记录 | 看 actor 为什么起不来 |
| `raylet.out` / `raylet.err` | 节点级 lease/资源调度决策 | 看 worker 启动 recheck 失败原因 |
| `python-core-driver-*_<driver_pid>.log` | driver core worker | 看 actor handle 引用计数、shutdown 调用栈 |
| `python-core-worker-*_<worker_pid>.log` | worker core worker | 看 task 执行、SIGSEGV 堆栈 |

---

## 7. 教训与最佳实践

1. **Dashboard / progress log 显示 RUNNING ≠ 真有 task 在跑**
   stats actor 推送链路被 worker SIGSEGV / 其他原因打断后，dashboard 数据会过期。**实际状态以 `ray list tasks --filter 'state=RUNNING'` 为准。**

2. **`ray status` 总资源足够 ≠ 单节点能 fit**
   Ray 是 bin-packing per node，必须看 per-node 视图。

3. **Ray `memory` 资源是逻辑预约 + 实际 RSS 的双重检查**
   - 即使 actor 进程实际 RSS 远小于 declared `memory`，调度时占用配额仍按 declared 计算
   - 但 raylet 在启动 worker 时会做 second-stage 实际可用内存 recheck，可能失败

4. **`ray.kill()` 对 PENDING_CREATION actor 是 noop**
   不能用来取消还没起来的 actor，只能等 GCS 调度成功后或显式 `CancelActorCreation`（Ray 2.51 没暴露给用户）。

5. **生产环境必加兜底**
   ```bash
   RAY_enable_infeasible_task_early_exit=true   # 防 actor infeasible hang
   ```
   或者监控外挂：作业 stuck 在某个进度 > N 分钟自动 `ray job stop`。

6. **大内存依赖的 actor 优先用 Plasma 共享，避免 per-actor 重复加载**
   单 actor cache > 单 worker 节点内存时，用 ActorPoolStrategy 必死锁。

7. **日志进度指标 `X/Y` 有固有延迟，不等于实时进度**
   `row_outputs_taken` 只在下游消费 output bundle 时才累加（`PhysicalOperator.get_next()` → `on_output_taken()`），而 `Tasks: N` 在 `_task_done_callback` 时就已更新。两者存在 0.1s~数秒的时间差：task 已完成（`Tasks: 0`）但产出还没被下游消费（`X < Y`）是正常现象。

8. **日志最后定格的 `Tasks: N` 不一定是 0，可能是 10 秒节流吞掉了 `Tasks: 0`**
   调度循环退出时 `num_active_tasks()` 一定为 0（`has_completed()` 的前提），但 `LoggingExecutionProgressManager.refresh()` 有 10 秒节流。如果最后一轮循环距上次打印不足 10 秒，`Tasks: 0` 不会被打印，日志定格在之前的 `Tasks: N`。

9. **作业结束后日志和 Dashboard 永远不会更新到最终 100%**
   `shutdown()` 的 `self.join(2s)` 杀死了调度循环线程——它是日志打印和 Dashboard 推送的唯一驱动源。之后 `_ClosingIterator` 在主线程继续消费 output queue 使 `row_outputs_taken` 追上 `num_output_rows_total()`，但已经没有任何线程调 `_refresh_progress_manager()` 或 `_update_stats_metrics()` 来读取和显示这些值了。

---

## 附录 A：本案完整时间线

| 时间 | 事件 |
|---|---|
| 2026-06-10 19:03:35 | Job 提交（`raysubmit_McXjCBhVEqffsBjB`） |
| 2026-06-11 01:03:50 | dataset_500_0 启动；ActorPool 申请 16 个 OldMeta actor |
| 01:03:50 | 2 个 OldMeta actor 落到 head 和 b4d3bf1c，14 个 PENDING_CREATION |
| 01:06:34 | ReadParquet completed |
| 01:06:59 | worker pid=1072917 SIGSEGV（gc_collect 路径）；AddFaceDetectJoinKeyBatch completed |
| 02:56:24 | OldMetaArrayLookupEnrichBatch completed（progress log "Actors: 0"），但 2 个 ALIVE actor 进程未 kill |
| 02:56:48 | FaceDetectEnrichStatsBatch completed |
| 03:13:27 | FillImageSize→Protocol→FullFaceGuard completed |
| 03:13:29 | KafkaJsonBatchWriter 残留 Tasks: 6（drain 中），progress 108499519/108538609 |
| 03:13:31 | **KafkaJsonBatchWriter completed**（最后一个算子） |
| 03:13:31+ | executor.shutdown() 启动 → ActorPool 等 14 PENDING init future → 死锁 |
| 03:13:31+ | autoscaler 持续打印 "No available node types" |
| 现在 12:09 | driver 仍 `Rl` 存活，job 状态 `RUNNING`；2 ALIVE actor 已空挂 9h+，各占 70 GiB |

## 附录 B：相关代码路径

| 模块 | 路径 | 关键行 |
|---|---|---|
| ActorPoolMapOperator | `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py` | `_do_shutdown`: 478; `_start_actor` (metadata task 注册): 316-335; `_release_pending_actors`: 1440-1449 |
| ActorPool 实现 | `python/ray/data/_internal/execution/operators/actor_pool.py` | `shutdown`: 1408; `_release_pending_actors`: 1440; `_release_running_actors`: 1451 |
| MapOperator (num_active_tasks) | `python/ray/data/_internal/execution/operators/map_operator.py` | `_data_tasks.pop`: 621; `_data_tasks[idx]=`: 642; `num_active_tasks`: 731-740; `_do_shutdown`: 709-713 |
| PhysicalOperator (has_completed) | `python/ray/data/_internal/execution/interfaces/physical_operator.py` | `has_execution_finished`: 530-548; `has_completed`: 554-567; `get_next`→`on_output_taken`: 748-757; `_cancel_active_tasks`: 931-949 |
| OpRuntimeMetrics | `python/ray/data/_internal/execution/interfaces/op_runtime_metrics.py` | `row_outputs_taken`: 295; `on_output_taken`: 859-863; `on_task_output_generated`: 889 |
| StreamingExecutor | `python/ray/data/_internal/execution/streaming_executor.py` | `run` (调度循环): 465-489; `_scheduling_loop_step`: 579-668; `_refresh_progress_manager`: 876-883; `_update_stats_metrics`: 839-851; `shutdown`: 272-324; `_ClosingIterator.get_next`: 1107-1123 |
| LoggingProgressManager | `python/ray/data/_internal/progress/logging_progress.py` | `LOG_REPORT_INTERVAL_SEC=10`: 99; `refresh` (10s 节流): 167-169; `update_operator_progress`: 197-203; `close_with_finishing_description=pass`: 194 |
| StatsManager / _StatsActor | `python/ray/data/_internal/stats.py` | `get_or_create_stats_actor` (detached, pinned): 436-457; `update_execution_metrics` (fire-and-forget): 602-621 |
| process_completed_tasks | `python/ray/data/_internal/execution/streaming_executor_state.py` | `ray.wait(timeout=0.1)`: 542-545; `add_output` (OpState 外部队列): 352-372; `get_output_blocking`: 396-415; `format_op_state_summary` (Tasks: N): 1008-1042; `update_operator_states`: 748 |
| GCS Actor Scheduler | `src/ray/gcs/gcs_server/gcs_actor_scheduler.cc` | |
| Cluster Resource Scheduler | `src/ray/raylet/scheduling/cluster_resource_scheduler.cc` | |
| Job Status 机 | `python/ray/dashboard/modules/job/job_supervisor.py` | |

## 附录 C：关键代码逻辑详解

> 本附录将 3.3 节涉及的每个关键环节，用完整的源码逐行注释，方便理解数据流的每一步。

### C.1 调度循环：所有指标的唯一起源

调度循环是 `StreamingExecutor`（继承 `Thread`）的 `run()` 方法，在一个独立线程中执行：

```python
# streaming_executor.py:465-489
def run(self):
    """Run the control loop in a helper thread."""
    exc: Optional[Exception] = None
    try:
        while True:
            t_start = time.perf_counter()
            continue_sched = self._scheduling_loop_step(self._topology)
            #  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
            #  每轮循环调用 _scheduling_loop_step()
            #  返回 True = 继续，False = 所有 op 完成 → 退出

            sched_loop_duration = time.perf_counter() - t_start
            self.update_metrics(sched_loop_duration)

            if not continue_sched or self._shutdown:
                #  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                #  两个退出条件：
                #  1. 所有 op has_completed() → continue_sched = False
                #  2. shutdown() 设置了 self._shutdown = True
                break
    except Exception as e:
        exc = e
    finally:
        _, state = self._output_node
        state.mark_finished(exc)
        # ← 线程退出后，_ClosingIterator.get_next() 会感知到并触发 shutdown()
```

### C.2 _scheduling_loop_step 完整执行流程

每一轮调度循环执行以下步骤（完整代码注释）：

```python
# streaming_executor.py:579-668
def _scheduling_loop_step(self, topology: Topology) -> bool:

    # ====== Phase 1: 等待已完成的 task ======
    self._resource_manager.update_usages()

    # process_completed_tasks() 内部调用 ray.wait(timeout=0.1)
    # 返回已完成的 task，触发 _task_done_callback → _data_tasks.pop()
    # 同时把 output block 放入 operator 的 _output_queue
    errored_blocks_per_op, _ = process_completed_tasks(
        topology,
        self._backpressure_policies,
        self._max_errored_blocks,
    )

    # ====== Phase 2: 错误处理 ======
    for op_state, num_errors in errored_blocks_per_op.items():
        if num_errors > 0:
            for _ in range(num_errors):
                op_state.op.metrics.on_block_errored()

    # ====== Phase 3: 调度新 task ======
    self._resource_manager.update_usages()
    self._report_current_usage()

    i = 0
    while True:
        op = select_operator_to_run(topology, self._resource_manager, ...)
        if op is None:
            break
        topology[op].dispatch_next_task()
        #  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
        #  → op.add_input(ref) → _try_schedule_tasks_internal()
        #  → actor.submit.remote(...) → _submit_data_task(gen, ...)
        #  → _data_tasks[task_index] = data_task   ← Tasks: N 增大

        self._resource_manager.update_usages()
        i += 1
        if i % self._progress_manager.TOTAL_PROGRESS_REFRESH_EVERY_N_STEPS == 0:
            self._refresh_progress_manager(topology)
            #  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
            #  每隔 N 次 dispatch 刷新一次 progress
            #  TOTAL_PROGRESS_REFRESH_EVERY_N_STEPS = 1 (每步都刷新)

    # ====== Phase 4: 更新 operator 状态 ======
    update_operator_states(topology)
    #  → 检查上游 op 是否 has_completed() → 通知下游 op inputs_done()
    #  → 检查下游 op 是否都 has_completed() → 标记上游 op mark_execution_finished()

    # ====== Phase 5: 刷新 progress 并推送 dashboard ======
    self._refresh_progress_manager(topology)
    #  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    #  ★ 这是日志打印和 dashboard 更新的唯一调用点 ★
    #  内部调 update_operator_progress() 更新指标 + refresh() 判断是否打印

    self._update_stats_metrics(state=DatasetState.RUNNING.name)
    #  ^^^^^^^^^^^^^^^^^^^^^^^^^^^
    #  ★ 这是 dashboard 推送的唯一调用点 ★
    #  每 5 秒推送一次给 _StatsActor

    # ====== Phase 6: 记录 completed 的 op ======
    for op, state in topology.items():
        if op.has_completed() and not self._has_op_completed[op]:
            logger.info(f"Operator {op} completed.")
            self._has_op_completed[op] = True

    # ====== Phase 7: 返回是否继续 ======
    return not all(op.has_completed() for op in topology)
    #  ← 所有 op has_completed() = True 时返回 False → 调度循环退出
```

### C.3 process_completed_tasks 五阶段详解

```python
# streaming_executor_state.py:482-742
def process_completed_tasks(topology, backpressure_policies, max_errored_blocks):

    # ====== 收集所有 active tasks ======
    active_tasks: Dict[Waitable, Tuple[OpState, OpTask]] = {}
    for op, state in topology.items():
        for task in op.get_active_tasks():
            #  op.get_active_tasks() → list(_metadata_tasks.values()) + list(_data_tasks.values())
            #  ★ 包含 data tasks 和 metadata tasks（PENDING actor 的 init future）
            active_tasks[task.get_waitable()] = (state, task)

    # ====== Phase 1: ray.wait 等待完成 ======
    ready, _ = ray.wait(
        list(active_tasks.keys()),
        num_returns=len(active_tasks),  # ← 等所有，但只给 100ms
        fetch_local=False,
        timeout=0.1,                     # ← ★ 100ms 超时 ★
    )
    # ready = 在这 100ms 内完成的 task 的 waitable 列表
    # 未完成的 task 留到下一轮

    # ====== Phase 2-4: 处理 ready 的 task ======
    # 对 DataOpTask:
    #   - prepare_metadata() → 获取 block metadata
    #   - _on_data_ready() → _output_ready_callback() → _output_queue.add(output)
    #   - 如果 task 产出全部完成 → _task_done_callback()
    #       → _data_tasks.pop(task_index)   ← ★ Tasks: N 减小
    #       → estimate_total_num_of_blocks() ← ★ 更新 num_output_rows_total (Y)

    # 对 MetadataOpTask:
    #   - 直接调 task.on_task_finished() → _metadata_tasks.pop(task_index)

    # ====== Phase 5: 拉取 operator output ======
    for op, op_state in topology.items():
        while op.has_next():
            op_state.add_output(op.get_next())
            #  ^^^^^^^^^^^^^^^^^^^^^^^^^
            #  op.get_next() → _get_next_inner() → _output_queue.get_next()
            #    → _metrics.on_output_dequeued(bundle)   ← 从 operator 内部队列出队
            #    → _metrics.on_output_taken(output)      ← ★ row_outputs_taken 累加
            #
            #  op_state.add_output(ref):
            #    → self.output_queue.append(ref)          ← 放入 OpState 外部队列
            #    → self.op.metrics.num_external_outqueue_blocks += ...
            #
            #  ★ 注意：这里调 op.get_next()，会触发 on_output_taken()
            #  ★ 但这只是中间 op 的 get_next，最终 op 的 output 由 _ClosingIterator 消费
```

**关键区分**：

| 调用位置 | 代码 | 触发的 `on_output_taken` |
|---------|------|------------------------|
| `process_completed_tasks()` Phase 5 | `op_state.add_output(op.get_next())` | 中间 op（非最终 output node）的 `row_outputs_taken` 累加 |
| `_ClosingIterator.get_next()` | `state.get_output_blocking()` → `op.get_next()` | 最终 output node 的 `row_outputs_taken` 累加 |

对于链式 DAG `ReadParquet → OldMeta → FaceDetect → ... → KafkaJsonBatchWriter`：

- 中间每个 op 的 `get_next()` 都在 `process_completed_tasks()` Phase 5 被调 → 中间 op 的 `row_outputs_taken` 在调度循环内更新
- **最终 op（KafkaJsonBatchWriter）的 output 由 `_ClosingIterator` 消费** → 它的 `row_outputs_taken` 依赖用户线程

### C.4 _refresh_progress_manager 完整代码

```python
# streaming_executor.py:876-883
def _refresh_progress_manager(self, topology: Topology):
    if self._progress_manager:
        for op_state in topology.values():
            if not isinstance(op_state.op, InputDataBuffer):
                self._progress_manager.update_operator_progress(
                    op_state, self._resource_manager
                )
                #  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                #  更新内部 _LoggingMetrics 对象：
                #    op_metrics.completed = opstate.op.metrics.row_outputs_taken  ← X
                #    op_metrics.total = opstate.op.num_output_rows_total()       ← Y
                #    op_metrics.desc = format_op_state_summary(opstate, ...)      ← "Tasks: N; ..."
        self._progress_manager.refresh()
        #  ← 判断是否打印（10 秒节流）
```

```python
# logging_progress.py:166-192
def refresh(self):
    current_time = self._get_time()
    if current_time - self._last_log_time < self.LOG_REPORT_INTERVAL_SEC:
        return          # ← 不到 10 秒，不打印

    self._last_log_time = current_time

    firstline = f"======= Running Dataset: {self._dataset_id} ======="
    lastline = "=" * len(firstline)
    logger.info(firstline)

    _log_global_progress(self._global_progress_metric)    # ← 打印总进度

    for opstate in self._topology.values():
        metrics = self._op_progress_metrics.get(opstate)
        if metrics is None:
            continue
        _log_op_or_sub_progress(metrics)                  # ← 打印每个 op 的进度
        #  内部调 _format_progress(metrics) → f"{m.name}: {m.completed}/{m.total}"
        #  然后打印 f"  {m.desc}" → "Tasks: N; Actors: M; ..."

    logger.info(lastline)
```

```python
# logging_progress.py:194-195
def close_with_finishing_description(self, desc: str, success: bool):
    pass      # ← ★ LoggingExecutionProgressManager 的关闭是空操作 ★
    # ★ 永远不会打印最终进度 ★
```

### C.5 format_op_state_summary 完整代码

```python
# streaming_executor_state.py:1008-1042
def format_op_state_summary(op_state, resource_manager, verbose=False):

    # --- Tasks 计数 ---
    active = op_state.op.num_active_tasks()
    #  ← MapOperator: return len(self._data_tasks)
    #  ← 已提交但 _task_done_callback 还没触发的 data task 数量

    worker_finished = op_state.op.metrics.num_tasks_worker_finished
    #  ← worker 端已跑完但 streaming generator output 还没被读完的 task 数
    if worker_finished > 0:
        running = active - worker_finished
        desc = f"Tasks: {active} ({running} running, {worker_finished} finished)"
    else:
        desc = f"Tasks: {active}"

    # --- Backpressure 状态 ---
    if op_state.op._in_task_submission_backpressure or op_state.op._in_task_output_backpressure:
        backpressure_types = []
        if op_state.op._in_task_submission_backpressure:
            policy = op_state.op._task_submission_backpressure_policy or ""
            backpressure_types.append(f"tasks({policy})")
        if op_state.op._in_task_output_backpressure:
            policy = op_state.op._task_output_backpressure_policy or ""
            backpressure_types.append(f"outputs({policy})")
        desc += f" [backpressured:{','.join(backpressure_types)}]"

    # --- Actors 计数 ---
    desc += f"; {_actor_info_summary_str(op_state.op.get_actor_info())}"
    #  ← _actor_info_summary_str() 实现：
    #    total = info.running + info.pending + info.restarting
    #    base = f"Actors: {total}"
    #    if total == info.running: return base
    #    else: return f"{base} ({info})"
    #
    #  对 TaskPoolMapOperator (KafkaJsonBatchWriter)：get_actor_info() 返回 running=0, pending=0, restarting=0
    #    → "Actors: 0"
    #  对 ActorPoolMapOperator (OldMeta)：返回 running=2, pending=14, restarting=0
    #    → "Actors: 16 (running=2, pending=14, restarting=0)"

    # --- Queued blocks ---
    desc += f"; Queued blocks: {op_state.total_enqueued_input_blocks()} ({memory_string(...)})"

    # --- Resources ---
    desc += f"; Resources: {resource_manager.get_op_usage_str(op_state.op)}"

    return desc
```

### C.6 _data_tasks 增减完整代码

```python
# map_operator.py:593-642 — _submit_data_task：提交时 +1
def _submit_data_task(self, gen, inputs, task_done_callback=None):
    task_index = self._next_data_task_idx
    self._next_data_task_idx += 1

    def _output_ready_callback(task_index, output):
        # streaming generator 每产出一个 block 调一次
        assert len(output) == 1
        self._metrics.on_task_output_generated(task_index, output)
        self._output_queue.add(output, key=task_index)   # ← 放入内部 output queue
        self._metrics.on_output_queued(output)

    def _task_done_callback(task_index, exception):
        self._metrics.on_task_finished(task_index, exception)

        # ★ 更新 num_output_rows_total (Y)
        (_, self._estimated_num_output_bundles,
         self._estimated_output_num_rows,
        ) = estimate_total_num_of_blocks(
            self._next_data_task_idx, self.upstream_op_num_outputs(), self._metrics
        )

        self._data_tasks.pop(task_index)                 # ← ★ Tasks: N 减 1
        self._output_queue.finalize(key=task_index)
        if task_done_callback:
            task_done_callback()

    def _worker_finished_cb(task_index):
        self._metrics.on_task_worker_finished(task_index)
        # ← worker 端 ray task 完成但 streaming generator 还有 output 没读

    data_task = DataOpTask(
        task_index,
        gen,
        lambda output: _output_ready_callback(task_index, output),
        functools.partial(_task_done_callback, task_index),
        worker_finished_callback=functools.partial(_worker_finished_cb, task_index),
    )
    self._metrics.on_task_submitted(task_index, inputs, task_id=data_task.get_task_id())
    self._data_tasks[task_index] = data_task             # ← ★ Tasks: N 增 1
```

### C.7 row_outputs_taken 累加的完整调用链

```
Worker 执行 task，通过 streaming generator 产出 block
  ↓
driver 端 ray.wait() 返回 ready ref
  ↓
process_completed_tasks() 处理 ready DataOpTask
  → _output_ready_callback() → _output_queue.add(output)   # 阶段 ❸：放入 operator 内部队列
  → 如果 task 全部完成 → _task_done_callback() → _data_tasks.pop()  # Tasks: N 减 1
  ↓
process_completed_tasks() Phase 5:
  → while op.has_next(): op_state.add_output(op.get_next())
  → op.get_next() → _get_next_inner():
      # map_operator.py:689-694
      def _get_next_inner(self) -> RefBundle:
          bundle = self._output_queue.get_next()
          self._metrics.on_output_dequeued(bundle)    # ← 从内部队列出队
          self._output_blocks_stats.extend(to_stats(bundle.metadata))
          return bundle
  → physical_operator.py:748-757:
      def get_next(self) -> RefBundle:
          output = self._get_next_inner()
          self._metrics.on_output_taken(output)       # ← ★ row_outputs_taken 累加
          return output
  → op_state.add_output(ref):
      self.output_queue.append(ref)                   # ← 放入 OpState 外部队列
  ↓
_ClosingIterator.get_next() (用户线程):
  → state.get_output_blocking(output_split_idx)
  → self.output_queue.pop(output_split_idx)
  → 返回 bundle 给用户代码
  ↓
  注意：最终 output node 的 get_next() 不在 process_completed_tasks() 中被调，
  而是在 _ClosingIterator.get_next() 中，由用户线程驱动。
  最终 op 的 row_outputs_taken 在用户线程取数据时才更新。
```

### C.8 has_completed / has_execution_finished 完整判断逻辑

```python
# physical_operator.py:530-548
def has_execution_finished(self) -> bool:
    """Return True when this operator has finished execution."""
    from ..operators.base_physical_operator import InternalQueueOperatorMixin

    internal_input_queue_num_blocks = 0
    if isinstance(self, InternalQueueOperatorMixin):
        internal_input_queue_num_blocks = self.internal_input_queue_num_blocks()

    # 执行完成的条件（二者满足其一即可）：
    # 1. 被显式标记 finished（mark_execution_finished()）
    # 2. 以下三个条件同时满足：
    #    a. 所有 input 已接收完毕（_inputs_complete = True）
    #    b. 没有在飞 task（num_active_tasks() == 0）  ← ★ 关键
    #    c. 内部 input queue 为空
    return self._is_execution_marked_finished or (
        self._inputs_complete
        and self.num_active_tasks() == 0
        and internal_input_queue_num_blocks == 0
    )

# physical_operator.py:554-573
def has_completed(self) -> bool:
    """Returns whether this operator has been fully completed.

    An operator is completed iff:
        * The operator has finished execution (has_execution_finished() is True).
        * All outputs have been taken (has_next() is False).
    """
    internal_output_queue_num_blocks = 0
    if isinstance(self, InternalQueueOperatorMixin):
        internal_output_queue_num_blocks = self.internal_output_queue_num_blocks()

    return (
        self.has_execution_finished()
        and internal_output_queue_num_blocks == 0
        and not self.has_next()
        #  ← has_next() 检查 _output_queue 是否还有 bundle
        #  如果 output 还没被下游取完 → has_completed() = False
    )
```

**关键推论**：

- `has_completed() = True` → `has_execution_finished() = True` → `num_active_tasks() == 0`
- 所以当调度循环退出时（所有 op `has_completed()`），`Tasks: N` **一定**是 0
- 但 `row_outputs_taken` 可能还没追上 `num_output_rows_total()`，因为 output queue 可能还有 bundle 没被下游取完

### C.9 ActorPoolMapOperator._start_actor：MetadataOpTask 注册流程

```python
# actor_pool_map_operator.py:296-335
def _start_actor(self, labels=None, logical_actor_id=None):
    """Start a new actor and add it to the actor pool as a pending actor."""
    assert self._actor_cls is not None

    # ❶ 创建 actor handle
    actor = self._actor_cls.options(
        _labels={self._OPERATOR_ID_LABEL_KEY: self.id, **labels}
    ).remote(
        ctx=ctx,
        logical_actor_id=logical_actor_id,
        src_fn_name=self.name,
        map_transformer=self._map_transformer,
        actor_location_tracker=get_or_create_actor_location_tracker(),
    )

    # ❷ 获取 actor 就绪的 future（init 完成后 resolve）
    res_ref = actor.get_location.remote()

    # ❸ 注册 MetadataOpTask，当 res_ref resolve 时回调
    def _task_done_callback(res_ref):
        # res_ref resolve = actor 已创建并就绪
        # 将 actor 从 pending 转为 running
        has_actor = self._actor_pool.pending_to_running(res_ref)
        if not has_actor:
            return  # actor 已经被 kill 了

    self._submit_metadata_task(
        res_ref,
        lambda: _task_done_callback(res_ref),
    )
    #  ↓ _submit_metadata_task 实现：
    #  map_operator.py:670-679
    #  self._metadata_tasks[task_index] = MetadataOpTask(
    #      task_index, result_ref, _task_done_callback
    #  )
    #  ★ res_ref 作为 MetadataOpTask 存入 _metadata_tasks 字典
    #  ★ 当 GCS 无法调度 actor 时，res_ref 永远不 resolve
    #  ★ MetadataOpTask 永远留在 _metadata_tasks 中

    return actor, res_ref
```

### C.10 ActorPoolMapOperator._do_shutdown 完整调用链

```python
# actor_pool_map_operator.py:478-481
def _do_shutdown(self, force=False):
    self._actor_pool.shutdown(force=force)
    #  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    #  ❶ 先关闭 actor pool
    super()._do_shutdown(force)
    #  ^^^^^^^^^^^^^^^^^^^^^^^
    #  ❷ 再调基类 MapOperator._do_shutdown()
```

**❶ ActorPool.shutdown(force=False)**：

```python
# actor_pool_map_operator.py:1408-1475
def shutdown(self, force=False):
    self._release_pending_actors(force=force)    # ← ❶-a 释放 pending actors
    self._release_running_actors(force=force)    # ← ❶-b 释放 running actors

# ❶-a: _release_pending_actors(force=False)
def _release_pending_actors(self, force):
    pending = dict(self._pending_actors)
    self._pending_actors.clear()
    #  ← 只是清空 Python 端的 _pending_actors dict
    #  ★ 不会向 GCS 发 CancelActorCreation
    #  ★ GCS 端 actor 仍然是 PENDING_CREATION，资源需求仍占着

    if force:
        for _, actor in pending.items():
            ray.kill(actor)
    #  ← force=False 时这行不执行

# ❶-b: _release_running_actors(force=False)
def _release_running_actors(self, force):
    running = list(self._running_actors.keys())
    on_exit_refs = []

    for actor in running:
        ref = self._release_running_actor(actor)
        #  ↓ _release_running_actor() 实现：
        #    actor_state = self._running_actors[actor]
        #    self._total_num_tasks_in_flight -= actor_state.num_tasks_in_flight
        #    if actor_state.num_tasks_in_flight > 0:
        #        self._num_active_actors -= 1
        #    if actor_state.is_restarting:
        #        self._num_restarting_actors -= 1
        #    if self._enable_actor_pool_on_exit_hook:
        #        ref = actor.on_exit.remote()    # ← 调 on_exit 让 actor 清理
        #    del self._running_actors[actor]     # ← 从 running dict 删除
        #    del self._actor_to_logical_id[actor]
        #    return ref
        if ref:
            on_exit_refs.append(ref)

    # 等待所有 on_exit 完成（最多等 ACTOR_POOL_GRACEFUL_SHUTDOWN_TIMEOUT_S）
    ray.wait(on_exit_refs, timeout=self._ACTOR_POOL_GRACEFUL_SHUTDOWN_TIMEOUT_S)

    if force:
        for actor in running:
            ray.kill(actor)
    #  ← force=False 时这行不执行
    #  ★ ALIVE actor 只是被引用释放（ref counting GC），
    #    不是 ray.kill()，GCS 端的 actor 进程还在，资源仍占着
```

**❷ MapOperator._do_shutdown(force)** → PhysicalOperator._cancel_active_tasks(force)**：

```python
# map_operator.py:709-713
def _do_shutdown(self, force=False):
    super()._do_shutdown(force)          # ← 调 PhysicalOperator._do_shutdown()
    self._data_tasks.clear()             # ← 清空 data tasks dict
    self._metadata_tasks.clear()         # ← 清空 metadata tasks dict

# physical_operator.py:819-820
def _do_shutdown(self, force):
    self._cancel_active_tasks(force=force)

# physical_operator.py:931-949
def _cancel_active_tasks(self, force):
    tasks: List[OpTask] = self.get_active_tasks()
    #  ← MapOperator.get_active_tasks():
    #    return list(self._metadata_tasks.values()) + list(self._data_tasks.values())
    #  ★ 此时 _data_tasks 可能已经清空了（上面的 clear()）
    #  ★ 但 _metadata_tasks 也被 clear() 了
    #
    #  ★ 实际上 MapOperator._do_shutdown 先调 super()._do_shutdown(force)
    #  ★ 再清空 dict。所以 _cancel_active_tasks 执行时 dict 还没清空
    #  ★ 14 个 MetadataOpTask 还在 _metadata_tasks 中

    for task in tasks:
        task._cancel(force=force)
    #  ← MetadataOpTask._cancel(force=False)
    #    只是释放 ObjectRef 引用，不等完成
    #  ★ PENDING actor 的 res_ref 永远不 resolve
    #  ★ _cancel 释放了 Python 端引用，但 GCS 端 actor 仍在 PENDING_CREATION

    if force:
        for task in tasks:
            ray.get(task.get_waitable())
    #  ← force=False 时不执行
    #  ← 如果 force=True，会在这里等所有 task 完成
    #  ← 但 res_ref 永远不 resolve → ray.get() 永远阻塞 → 同样死锁
```

**所以 force=False 和 force=True 都会死锁**——区别只是死锁在哪一步：

| force | 死锁位置 | 原因 |
|-------|---------|------|
| `False` | `_cancel_active_tasks` 之后的某个等待点 | `_release_pending_actors` 只清 dict 不 kill GCS 端 actor |
| `True` | `_cancel_active_tasks` 内 `ray.get(task.get_waitable())` | res_ref 永远不 resolve |

### C.11 _ClosingIterator 完整生命周期

```python
# streaming_executor.py:1107-1123
class _ClosingIterator(OutputIterator):
    """Iterator automatically shutting down executor upon exhausting the iterable."""

    def __init__(self, executor: StreamingExecutor):
        self._executor = executor

    def get_next(self, output_split_idx=None) -> RefBundle:
        try:
            op, state = self._executor._output_node
            bundle = state.get_output_blocking(output_split_idx)
            #  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
            #  从最终 op 的 OpState.output_queue 取数据
            #  队列空时 time.sleep(0.01) 轮询
            #  所有数据取完后抛 StopIteration

            if self._executor._progress_manager:
                self._executor._progress_manager.update_total_progress(
                    bundle.num_rows() or 0, op.num_output_rows_total()
                )
            #  ↑ 只更新全局总进度，不更新 per-op 的 row_outputs_taken

            return bundle

        except BaseException as e:
            # StopIteration / 其他异常 → 触发 shutdown
            self._executor.shutdown(
                force=False,
                exception=e if not isinstance(e, StopIteration) else None,
            )
            raise

    def __del__(self):
        self._executor.shutdown(force=False)
```

**两个线程的交互时序**：

```
用户线程 (main)                        调度线程 (StreamingExecutor.run)
═════════════════                     ════════════════════════════════

_ClosingIterator.get_next()           _scheduling_loop_step()
  → get_output_blocking()               → process_completed_tasks()
    → output_queue.pop()                   → _output_ready_callback()
    → 返回 bundle                          → _output_queue.add(output)
  → 用户处理 bundle                       → op_state.add_output(op.get_next())
                                           → output_queue.append(ref)  ← 放入外部队列
                                           → _refresh_progress_manager()
                                           → _update_stats_metrics()

  → 下一次 get_next()                   → 下一轮循环...
    → get_output_blocking()
    → output_queue.pop()   ← 取走调度线程放入的 bundle
```

**关键：两个线程通过 `OpState.output_queue` 交换数据。调度线程放数据，用户线程取数据。**

当调度线程死亡后：
- output_queue 不再有新数据放入
- 用户线程取完现有数据后 `get_output_blocking()` 抛 `StopIteration`
- 触发 `shutdown(force=False)`
- `shutdown()` 中 `join(2s)` 等调度线程（已经死了，立即返回）
- 然后 `op.shutdown()` 卡死

### C.12 OpState.get_output_blocking 完整代码

```python
# streaming_executor_state.py:396-415
def get_output_blocking(self, output_split_idx) -> RefBundle:
    """Get an item from this node's output queue, blocking as needed."""
    while True:
        if self._exception is not None:
            raise self._exception
        elif self._finished and not self.output_queue.has_next(output_split_idx):
            raise StopIteration()
            #  ↑ _finished = True 时（调度线程 mark_finished(exc) 后），
            #  如果 output_queue 为空 → 抛 StopIteration
            #  → _ClosingIterator 进入 except → 触发 shutdown()

        ref = self.output_queue.pop(output_split_idx)
        if ref is not None:
            self.op.metrics.num_external_outqueue_blocks -= len(ref.blocks)
            self.op.metrics.num_external_outqueue_bytes -= ref.size_bytes()
            return ref
        time.sleep(0.01)
        #  ↑ 队列空时每 10ms 轮询
```

### C.13 update_operator_states 完整代码

```python
# streaming_executor_state.py:748-780
def update_operator_states(topology: Topology) -> None:
    """Update operator states accordingly for newly completed tasks."""

    for op, op_state in topology.items():
        # 通知 op 所有 input 已完成
        if op_state.inputs_done_called:
            continue
        all_inputs_done = True
        for idx, dep in enumerate(op.input_dependencies):
            if dep.has_completed() and not topology[dep].output_queue:
                if not op_state.input_done_called[idx]:
                    op.input_done(idx)
                    op_state.input_done_called[idx] = True
            else:
                all_inputs_done = False

        if all_inputs_done:
            op.all_inputs_done()
            op_state.inputs_done_called = True

    # 反向遍历：如果下游 op 全部 completed，上游也标记 finished
    for op, op_state in reversed(list(topology.items())):
        dependents_completed = len(op.output_dependencies) > 0 and all(
            dep.has_completed() for dep in op.output_dependencies
        )
        if dependents_completed:
            op.mark_execution_finished()
            #  ← 设置 _is_execution_marked_finished = True
            #  ← has_execution_finished() 直接返回 True，不再检查 num_active_tasks()
```

### C.14 _update_stats_metrics 完整代码

```python
# streaming_executor.py:83, 839-851
UPDATE_METRICS_INTERVAL_S: float = 5.0

def _update_stats_metrics(self, state, force_update=False):
    now = time.time()
    if (
        force_update
        or (now - self._metrics_last_updated) > self.UPDATE_METRICS_INTERVAL_S
    ):
        _StatsManager.update_execution_metrics(
            self._dataset_id,
            [op.metrics for op in self._topology],     # ← 每个 op 的 OpRuntimeMetrics
            self._get_operator_tags(),
            self._get_state_dict(state=state),
        )
        self._metrics_last_updated = now

# stats.py:602-621
@staticmethod
def update_execution_metrics(dataset_tag, op_metrics, operator_tags, state):
    per_node_metrics = _StatsManager._aggregate_per_node_metrics(op_metrics)
    op_metrics_dicts = [metric.as_dict() for metric in op_metrics]
    args = (dataset_tag, op_metrics_dicts, operator_tags, state, per_node_metrics)
    try:
        get_or_create_stats_actor().update_execution_metrics.remote(*args)
        #  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
        #  ★ fire-and-forget：不调 ray.get()，不保证送达
        #  ★ _StatsActor 是 detached actor，绑定在 driver 节点
        #  ★ 如果 driver 节点的 CoreWorker 与 _StatsActor 之间的 RPC 通道
        #    被 worker SIGSEGV 间接影响，这次推送可能失败
    except Exception as e:
        logger.warning(f"Error occurred during update_execution_metrics.remote call: {e}")
        return
        #  ← 吞掉异常，推送丢失也不报错
```

### C.15 _StatsActor 创建和定位

```python
# stats.py:436-457
def get_or_create_stats_actor() -> ActorHandle[_StatsActor]:
    scheduling_strategy = NodeAffinitySchedulingStrategy(
        ray.get_runtime_context().get_node_id(),
        soft=False,
        #  ← ★ soft=False：强制绑定在 driver 节点
        #  ← driver 节点挂了 _StatsActor 才会挂
        #  ← worker SIGSEGV 不影响 _StatsActor
    )
    return _StatsActor.options(
        name=STATS_ACTOR_NAME,
        namespace=STATS_ACTOR_NAMESPACE,
        get_if_exists=True,
        lifetime="detached",
        #  ← ★ lifetime="detached"：跨 job 生命周期
        #  ← 只有 Ray cluster 关闭或显式 kill 才会销毁
        scheduling_strategy=scheduling_strategy,
    ).remote()
```

### C.16 shutdown 完整代码（逐行注释）

```python
# streaming_executor.py:272-340
def shutdown(self, force, exception=None):
    global _num_shutdown

    with self._shutdown_lock:
        if not self._execution_started or self._shutdown:
            return    # ← 未启动或已关闭，直接返回

        start = time.perf_counter()

        # ❶ 标记关闭
        _num_shutdown += 1
        self._shutdown = True
        #  → 调度循环 while True 下一轮检测到 self._shutdown → break
        #  → 或者已经退出了（continue_sched=False）

        # ❷ 等调度线程结束（最多 2 秒）
        self.join(timeout=2.0)
        #  → 调度线程 run() 返回
        #  ★ 从此 _scheduling_loop_step() 永远不再执行
        #  ★ _refresh_progress_manager() 永远不再调用 → 日志不再打印
        #  ★ _update_stats_metrics() 永远不再调用 → Dashboard 不再更新

        # ❸ 最后一次推送 dashboard
        self._update_stats_metrics(
            state=DatasetState.FINISHED.name
            if exception is None
            else DatasetState.FAILED.name,
            force_update=True,    # ← 强制推送，不管 5s 间隔
        )
        #  ★ 此时 row_outputs_taken 可能还没追上 num_output_rows_total
        #  ★ Dashboard 定格在这个快照

        # ❹ 冻结 stats
        self._final_stats = self._generate_stats()
        stats_summary_string = self._final_stats.to_summary().to_string(
            include_parent=False
        )

        # ❺ 关闭 progress manager
        self._resource_manager.update_usages()
        self.update_metrics(0)
        if self._data_context.enable_auto_log_stats:
            logger.info(stats_summary_string)

        if exception is None:
            desc = (
                f"✅ Dataset {self._dataset_id} execution finished in "
                f"{self._final_stats.time_total_s:.2f} seconds"
            )
        else:
            desc = f"⚠️ Dataset {self._dataset_id} execution failed"

        self._progress_manager.close_with_finishing_description(
            desc, exception is None
        )
        #  ← LoggingExecutionProgressManager: pass（空操作）
        #  ← RichExecutionProgressManager: 停止 rich.Live 显示
        #  ★ 永远不会打印最终的 "100%" 进度

        logger.info(desc)

        # ❻ 逐个 shutdown operator ★ 卡死在这里 ★
        timer = Timer()
        for op in self._topology.keys():
            op.shutdown(timer, force=force)
            #  → PhysicalOperator.shutdown()
            #    → self._shutdown = True
            #    → self._do_shutdown(force)
            #      → ActorPoolMapOperator._do_shutdown()
            #        → _actor_pool.shutdown(force=False)
            #          → _release_pending_actors()  → 只清 dict
            #          → _release_running_actors()  → on_exit + 释放引用
            #        → super()._do_shutdown(force)
            #          → _cancel_active_tasks(force)
            #            → 14 个 MetadataOpTask._cancel(force=False)
            #              → 释放 ObjectRef 引用，不等完成
            #              → 但 GCS 端 actor 仍 PENDING_CREATION
            #              → ★ 死锁 ★

        # 以下代码永远执行不到：
        logger.debug(
            f"Shut down operator hierarchy for dataset {self._dataset_id}"
            f" (min/max/total={min_}/{max_}/{total}s)"
        )
        if exception is None:
            for callback in get_execution_callbacks(self._data_context):
                callback.after_execution_succeeds(self)
        else:
            for callback in get_execution_callbacks(self._data_context):
                callback.after_execution_fails(self, exception)
```
