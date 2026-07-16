# GPU 利用率周期性波动根因排查分析

## 1. 问题现象

在快手大规模 Ray 集群（~2200 节点）上运行视频分类处理 Pipeline 时，GPU 利用率呈现周期性波动（"一会儿高一会儿低"），波谷间隔约 30 分钟。Pipeline 核心算子 `DistributedQwenVLVideoProcessMapper`（1000 个 GPU actor）时而满载工作、时而空闲。

### 1.1 环境信息

| 项目 | 值 |
|------|-----|
| 集群规模 | ~2200 个 worker 节点（物理） |
| GPU Actor 数 | 1000（DistributedQwenVLVideoProcessMapper） |
| CPU Preprocess Actor 数 | 每个 GPU actor 内部 15 个（QwenVLCPUPreprocessActor） |
| Ray 版本 | 2.x（内部定制版） |
| Head 节点 | 单 Head，GCS 单线程模型 |
| 反压策略 | 已禁用（`backpressure_policies.enabled = []`） |
| prefetch_batches | 15 |
| streaming-cpu-actor-pool-size | 15 |
| max_tasks_per_actor | 4 |

### 1.2 Pipeline 架构

```
数据源
  ↓
FlatMap(ClipMergeMapper)          ← CPU 算子，55,140 个 task
  ↓
StreamingRepartition              ← 11,693 个 task
  ↓
MapBatches(DistributedQwenVLVideoProcessMapper)  ← GPU 算子，1000 actor
  │
  │  内部管线（每个 GPU actor 内）:
  │  ┌─────────────────────────────────────────┐
  │  │ 15 个 QwenVLCPUPreprocessActor          │
  │  │   (视频下载/解码/帧渲染)                  │
  │  │          ↓                               │
  │  │ GPU vLLM 推理                            │
  │  │   (等 CPU batch 完成 → GPU inference)     │
  │  └─────────────────────────────────────────┘
  ↓
后续算子...
```

### 1.3 关键 Driver 日志快照

```
MapBatches(DistributedQwenVLVideoProcessMapper):
  Tasks: 1998; Actors: 1000 (running=903, restarting=96, pending=1)
  Queued blocks: 823567 (394.7GiB)
  [950/117349 objects local]

FlatMap(ClipMergeMapper): Tasks: 55140
StreamingRepartition[num_rows_per_block=40]: Tasks: 11693
```

---

## 2. 排查过程与假设验证

### 2.1 假设一：Dashboard 显示 3178 ALIVE 节点 = 同 IP 多 Pod

**假设内容**：Dashboard 显示 3178 个 ALIVE 节点（实际只有 ~2200 个物理节点），可能是因为同一个 IP 上运行了多个 Pod。

**排查方式**：通过 KML Web Shell 连接 Head 节点，执行 `ray status` 查看实际节点状态。

**排查结论**：❌ **假设错误**。

实际情况是：
- ALIVE = 2200（与物理节点数一致）
- DEAD = 1000
- Total = 3200

DEAD 节点来自 **节点抖动（Node Flapping）**：GCS 主线程繁忙时，心跳处理延迟导致部分节点被误标为 DEAD，这些节点随后重新注册会获得新的 node_id。Dashboard 显示 3178 是 UI 缓存了部分过期数据（pubsub 广播丢失了 DEAD 通知），并非真正有 3178 个活跃节点。

**证据**：同一 IP（如 `10.80.245.172`）被标记 DEAD 多达 42 次，说明反复抖动。

---

### 2.2 假设二：反压（Backpressure）导致 GPU 空闲

**假设内容**：Ray Data 的反压策略限制了 GPU 算子的数据供给，导致 GPU actor 间歇性没有数据可处理。

**排查方式**：确认用户 Pipeline 配置。

**排查结论**：❌ **假设错误**。

用户已显式禁用反压：
```python
ctx.set_config("backpressure_policies.enabled", [])
```

因此反压不是 GPU 波动的原因。

---

### 2.3 假设三：task_io 线程繁忙导致 gRPC 线程池竞争，影响 GPU 任务下发

**假设内容**：GCS 的 `task_io_context` 线程处理 Task Event 上报，CPU 占用 99.9%。由于 GCS 的 gRPC 线程池是共享的，task_io 的高负载会挤占 `PushTask`（Actor 创建）等关键 RPC 的线程池资源，间接导致 GPU Actor 创建/重建变慢。

**排查方式**：

1. **初始观察**：通过 `top -H` 快照看到 `task_io` 线程 CPU 99.9%
2. **实时增量测量**：通过 `/proc/<pid>/task/<tid>/stat` 对比 2 秒间隔的 CPU 时间增量

**排查结论**：❌ **假设错误**。

实时增量数据显示 `task_io` 实际 CPU 仅 0.5-1.5%，之前 99.9% 是 `top` 的累积值快照，不代表当前状态。

**实时 GCS 线程 CPU 分布（2 秒增量）**：

| 线程 | CPU 增量（每 2s） | 实际利用率 |
|------|-------------------|-----------|
| ray_syncer_io_c | ~198/200 ticks | **~99%** |
| GCS 主线程 | 31-55/200 ticks | ~17-27% |
| pubsub | ~6/200 ticks | ~3% |
| task_io | 1-3/200 ticks | **~0.5-1.5%** |

`task_io` 并不繁忙，排除了 gRPC 线程池竞争假设。

---

### 2.4 假设四：GCS PushTask 影响 Actor 方法调用，导致 GPU 推理延迟

**假设内容**：GCS 主线程排队（mean 112ms/max 13.5s）会影响 Actor 方法调用（如 `actor.preprocess_video.remote()`），导致 GPU actor 内部的 CPU actor 调用变慢。

**排查方式**：阅读 Ray 源码，确认 PushTask 的用途和 Actor 方法调用的路径。

**关键源码证据**：

1. **GCS PushTask 只用于 Actor 创建**（`gcs_actor_scheduler.cc:392-405`）：
```cpp
// GCS → Worker: 创建 Actor 的 PushTask
auto request = std::make_unique<rpc::PushTaskRequest>();
client->PushNormalTask(std::move(request),
    [this, actor, worker](Status status, const rpc::PushTaskReply &reply) {
        OnActorCreationSuccess(actor, reply);
    });
```

2. **Actor 方法调用走 Worker-to-Worker 直连**（`actor_task_submitter.cc:573-582`）：
```cpp
// Worker → Worker: 直接 gRPC，完全绕过 GCS
PushActorTask(client_queue, task_spec, skip_queue);
```

**排查结论**：❌ **假设错误**。

`actor.preprocess_video.remote()` 等 Actor 方法调用是 **Worker 到 Worker 直连**，完全不经过 GCS。GCS 主线程排队只影响 Actor 创建/重建，不影响日常的 Actor 方法调用。

---

### 2.5 假设五：节点死亡导致 GPU Actor 重建，引起大规模 GPU 空闲

**假设内容**：~10 个节点死亡导致 96 个 GPU Actor 重启，Actor 重建通过 GCS 主线程（已拥堵），重建时间被拉长，大量 GPU 空闲。

**排查方式**：分析节点死亡规模与 Actor 重建数量的关系。

**排查结论**：❌ **假设不成立（影响程度不匹配）**。

- 只有 ~10 个节点死亡
- 1000 个 GPU Actor 中有 903 个 running，96 个 restarting
- 但集群有 2200 个节点，Actor 的 `max_restarts=-1`，重建会调度到其他健康节点
- 即使 96 个 Actor 在重建，903 个仍在运行，不应导致整体 GPU 利用率的大幅周期性波动

用户反馈："但是只有不到十几个节点死亡会影响这么大吗？我本身会有多个节点，单个节点死亡不该由其他节点去处理吗？"

---

### 2.6 假设六：prefetch_batches=2 导致 GPU 推理等待 CPU 预处理

**假设内容**：GPU actor 内部 `prefetch_batches=2` 太小，CPU 预处理流水线深度不够，GPU 推理频繁等待 CPU 数据。

**排查方式**：确认用户实际配置。

**排查结论**：❌ **假设错误**。

用户实际配置：
```
prefetch_batches = 15
streaming-cpu-actor-pool-size = 15
```

15 个 prefetch batch + 15 个 CPU actor，流水线深度充足，GPU 不应该因为等待 CPU 预处理而空闲。

---

### 2.7 假设七：SCHED_PROFILE 慢步骤（5-14s）导致 GPU 周期性空闲

**假设内容**：Driver 日志中的 `SCHED_PROFILE` 条目显示调度循环步骤耗时 5-14 秒（个别高达 254 秒），调度变慢导致 GPU 任务下发延迟。

**排查方式**：

1. 从 Driver 日志中提取全部 415 条 SCHED_PROFILE 条目
2. 分析时间分布和 burst 模式
3. 与 GPU 波动周期对比

**SCHED_PROFILE 数据摘要**：

| 时间段 | 条目数 | 每步耗时范围 | 特征 |
|--------|--------|-------------|------|
| 17:17-17:22 | 少量 | 5-10s | 散发 |
| 17:23-17:25 | 3 | 21-64s | 快速升级 |
| 17:32-17:59 | 大量 | 升级至 254s/步 | **严重 burst** |
| 18:01-18:10 | 大量 | 10-20s | 持续慢 |
| 22:06-22:14 | 少量 | 5-14s | 近期 burst |

关键数据：
- 5 小时内共 415 条慢步骤（>5s），平均 13.7s/步，总慢时间 5683s
- **所有 SCHED_PROFILE 条目中 `ready_gpu=0`** — 慢步骤期间没有 GPU task 完成
- `ray.wait(timeout=0.1)` 在 71,000+ refs 上偶尔耗时 8-9 秒
- `collect` 阶段（构建 active_tasks 字典）偶尔耗时 6.4 秒
- active tasks ~71,000+（主要来自 FlatMap ClipMergeMapper 的 55,140 个 task）

**排查结论**：❌ **假设不完全成立（时间对不上）**。

调度循环是 `while True` 无间隔轮询的（唯一暂停是 `ray.wait(timeout=0.1)` 的 100ms）。SCHED_PROFILE 只记录 >5s 的步骤。慢步骤呈 **burst 模式**，burst 之间有 ~30 分钟的间隔。

**如果每步都是 5-14 秒，日志应该是连续的，不会出现 30 分钟的间隔**。30 分钟间隔说明在这段时间内每步都 < 5 秒（正常），调度是顺畅的。

GPU 波谷周期也是 ~30 分钟，但 SCHED_PROFILE burst 只持续几分钟，无法解释持续 30 分钟的 GPU 波谷。SCHED_PROFILE 的 burst 可能是 GPU 波谷的**加剧因素**，但不是**根本原因**。

---

## 3. 已确认的事实

### 3.1 GCS 线程模型

```
GCS 进程
├── 主线程 (boost::asio::io_context, FIFO 队列)
│   ├── Actor 创建/销毁
│   ├── 节点注册/心跳处理
│   ├── 资源更新回调 (来自 syncer)
│   └── 排队指标: mean=112ms, max=13.5s
│
├── ray_syncer_io_c 线程 (独立 io_context)
│   ├── 广播资源更新到 2200 节点
│   ├── CPU: ~99%（持续满载）
│   └── 通过 Post 向主线程提交 GcsResourceManager::Update 回调
│
├── task_io_context 线程 (独立 io_context)
│   ├── 处理 AddTaskEventData/GetTaskEvents RPC
│   ├── CPU: ~0.5-1.5%（空闲）
│   └── 仅用于 Dashboard 展示，不影响执行
│
└── pubsub 线程 (独立 io_context)
    ├── CPU: ~3%
    └── 发布状态变更通知
```

### 3.2 Actor 方法调用路径（关键发现）

```
Actor 创建:
  Driver → GCS 主线程 (调度决策) → PushTask → Worker (创建 Actor)
  ⚠️ 受 GCS 主线程排队影响

Actor 方法调用 (如 actor.preprocess_video.remote()):
  Caller Worker → 直连 gRPC → Target Worker (执行方法)
  ✅ 完全不经过 GCS，不受 GCS 压力影响
```

### 3.3 Task Event 上报机制

- Worker 每 `task_events_report_interval_ms`（默认 1000ms）向 GCS 上报 task 状态变更
- 由 `task_io_context` 独立线程处理
- **仅用于可观测性**（Dashboard Tasks 页面、`ray list tasks` 命令）
- 设为 0 可禁用，不影响任务执行
- 当前实测 CPU 仅 0.5-1.5%，不是瓶颈

### 3.4 ray_syncer 持续满载

`ray_syncer_io_c` 线程 CPU ~99%，持续向 2200 个节点广播资源更新。这意味着：
- 每次广播一轮需遍历 2200 个节点连接
- 每次广播通过 `Post` 向主线程提交 `GcsResourceManager::Update` 回调
- 这些回调在主线程 FIFO 队列中排队，增加主线程延迟

### 3.5 节点抖动 (Node Flapping)

- 同一 IP 反复 DEAD→ALIVE 循环（如 `10.80.245.172` 被标 DEAD 42 次）
- 原因：GCS 主线程繁忙时心跳处理延迟 → 节点被误判 DEAD → 节点重新注册
- 每次重注册消耗 GCS 主线程资源（节点注册、资源同步、Actor 迁移）
- 节点死亡时间点与 SCHED_PROFILE burst 有一定相关性：
  - 17:44 → 7 个节点死亡
  - 17:50 → 16 个节点死亡
  - 17:32-17:59 → SCHED_PROFILE 严重 burst

### 3.6 OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED 错误（深层分析）

Driver 日志中出现 `[OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED]` 错误，尽管 Ray Data 的 `cached_remote_fn` 默认设置 `max_retries=-1`（无限重试），仍然出现此错误。

#### 根因：`lineage_eligibility_` 与 `submissible_tasks_` 不一致

Ray 的 `reference_counter` 和 `task_manager` 在 lineage 管理上存在不一致：

1. **`lineage_eligibility_`**（在 `reference_counter` 中）：标记 object 是否可重建。只在 `ReleaseLineageReferences` 中被降级为 `INELIGIBLE_LINEAGE_EVICTED`，且**条件是 `!OutOfScope()`**（object 仍 in scope 时才降级）。

2. **`submissible_tasks_`**（在 `task_manager` 中）：存储 task spec（用于 reconstruction 时重新执行 task）。在 `CompletePendingTask` 中，当 `task_retryable = false`（`reconstructable_return_ids_` 为空）时，直接 `submissible_tasks_.erase(it)` 删除 task spec，**不会通知 `reference_counter` 降级 `lineage_eligibility_`**。

3. **结果**：`reference_counter` 认为 lineage 还在（`ELIGIBLE`），但 `task_manager` 已把 task spec 删了 → `ResubmitTask` 返回 `MAX_ATTEMPTS_EXCEEDED`。

#### 完整错误传播链路

```
┌─────────────────────────────────────────────────────────────────┐
│ 第 1 层：StreamingRepartition 的某个 output object 丢失          │
│                                                                  │
│ Tidal 节点死亡 → plasma 中的 object 消失                         │
│ → ObjectRecoveryManager::RecoverObject(object_id)               │
│ → PinOrReconstructObject() → pin 失败 → ReconstructObject()     │
│ → GetLineageReconstructionEligibility() → ELIGIBLE               │
│ → ResubmitTask() → submissible_tasks_ 找不到 task                │
│ → 返回 MAX_ATTEMPTS_EXCEEDED                                    │
│ → recovery_failure_callback_ → 写 ErrorType 到 plasma           │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│ 第 2 层：下游 PreprocessMapper actor task 依赖解析失败            │
│                                                                  │
│ PreprocessMapper.submit() → DependencyResolver → ray.get()      │
│ → 拿到 RayError → raise_if_dependency_failed(arg)               │
│ → 抛出 ObjectReconstructionFailedError                          │
│ → ActorTaskSubmitter::FailOrRetryPendingTask()                  │
│   → RetryTaskIfPossible() → max_retries=-1 会重试               │
│   → 但依赖 object 永久不可恢复 → 最终 FailPendingTask()          │
│   → MarkTaskReturnObjectsFailed() → return objects 也标记为错误  │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│ 第 3 层：Ray Data streaming executor 看到 task 失败              │
│                                                                  │
│ DataOpTask.on_data_ready() → ray.get() 拿到 RayTaskError        │
│ → process_completed_tasks() → num_errored_blocks++              │
│ → max_errored_blocks=0 → 直接 abort 整个 pipeline               │
│                                                                  │
│ 错误层层包装显示：                                                │
│   RayTaskError(ObjectReconstructionFailedError):                 │
│     MapWorker(QGPreprocessMapper).submit()                       │
│       At least one of the input arguments...                     │
│       RayTaskError: StreamingRepartition[...]                    │
│         At least one of the input arguments...                   │
│         ObjectReconstructionFailedError: Failed to retrieve...   │
│         [OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED]         │
└─────────────────────────────────────────────────────────────────┘
```

#### 关键代码引用

| 文件 | 行号 | 说明 |
|------|------|------|
| `reference_counter.cc` | 579-582 | `ReleaseLineageReferences` 降级条件：`!OutOfScope()` |
| `task_manager.cc` | 1059-1070 | `CompletePendingTask` 中 `submissible_tasks_.erase` 不通知 reference_counter |
| `task_manager.cc` | 1459-1490 | `RemoveLineageReference` — `reconstructable_return_ids_` 清空 → spec 删除 |
| `task_manager.cc` | 357-358 | `ResubmitTask` — 找不到 task → `MAX_ATTEMPTS_EXCEEDED` |
| `object_recovery_manager.cc` | 157-169 | `ReconstructObject` — eligibility 检查 → ResubmitTask |
| `core_worker_process.cc` | 660-665 | `recovery_failure_callback_` — 写 ErrorType |
| `_raylet.pyx` | 891-895 | `raise_if_dependency_failed` — 依赖失败抛异常 |
| `exceptions.py` | 310-325 | Traceback 格式化：替换为友好提示 |
| `remote_fn.py` | 37 | `cached_remote_fn` 默认 `max_retries=-1` |
| `streaming_executor_state.py` | 481-509 | `process_completed_tasks` 错误处理 |

#### Ray Data 不重试 Operator Task

**注意**：Ray Data 的 streaming executor 不会重新提交失败的 operator task。`cached_remote_fn` 的 `max_retries=-1` 是 **Ray Core 层的 task 重试**（worker 死亡后自动重提交），不是 Ray Data 层的 operator task 重试。Ray Data 默认 `max_errored_blocks=0`，任何 block 重建失败直接 abort 整个 pipeline。

#### Object Store 内存压力导致 Lineage 被驱逐

除上述 bug 外，还存在另一种失败路径：当 lineage 总大小超过 `max_lineage_bytes`（默认 1GB）时，`EvictLineage` 会把 `lineage_eligibility_` 从 `ELIGIBLE` 降级为 `INELIGIBLE_LINEAGE_EVICTED`。此时返回的错误是 `OBJECT_UNRECONSTRUCTABLE_LINEAGE_EVICTED`（而非 `MAX_ATTEMPTS_EXCEEDED`）。

在大规模 pipeline 中，大量 Tidal 节点死亡 → 大量 object 需要重建 → lineage 累积超限 → 触发 EvictLineage → 后续重建失败 → `LINEAGE_EVICTED` 错误。

#### 修复建议

1. **修复 `lineage_eligibility_` 降级遗漏**：在 `CompletePendingTask` 中 `submissible_tasks_.erase(it)` 之前，通知 `reference_counter` 把所有 return object 的 `lineage_eligibility_` 降级为 `INELIGIBLE_LINEAGE_EVICTED`（或引入新的 `INELIGIBLE_SPEC_DELETED` 枚举值）
2. **增大 `max_lineage_bytes`**：对大规模 pipeline（数千 task + Tidal 节点场景），默认 1GB 远不够，建议设为 10-50GB
3. **Ray Data 增加 operator task 级别重试**：当依赖的 object 可恢复时（如节点恢复后 object 从副本恢复），重试依赖解析而非直接 abort

---

### 3.7 OOM 错误与 Object Store 的关系

```
ray.exceptions.OutOfMemoryError: 1 worker(s) were killed due to the node running low on memory.
Memory on the node (IP: 10.83.10.20, ID: 8c4ec54b...) was 957.55GB / 1006.84GB (0.951040)
```

**OOM 不是 Object Store 单独的问题**，而是**节点总内存**不够。Ray memory manager 监控的是整个节点的系统内存（`/proc/meminfo` 中的 MemTotal + Shmem），包括：

| 内存消费者 | 大小 | 说明 |
|---|---|---|
| Object store (plasma) | ~200 GB | `/dev/shm` mmap，固定配置 |
| Worker heap | ~500+ GB | 4000 PreprocessMapper + 600 InferMapper actor 的进程堆 |
| Page cache / 其他 | ~100+ GB | OS 文件缓存、kess 等 |
| **总计** | **~957 GB / 1007 GB** | 95.1%，触发 OOM kill |

根因是 **actor 数量太多 + 反复重启**导致 worker heap 累积超过节点剩余内存：
- 节点 1 TB 总内存
- Object store 固定占 200 GB
- 剩余 ~800 GB 给所有 worker
- 4000 个 PreprocessMapper + 600 个 InferMapper 分散在 80 个节点
- 每节点约 57 个 actor，每个 actor 加载数据 + 模型 → heap 膨胀
- Tidal 节点死亡后 actor 重启、重新加载数据 → 内存持续增长
- 最终达到 95% → memory manager 杀 worker（对应之前观察到的 72,269 次 worker eviction）

**Object Store 200 GB 是固定占用，不是导致 OOM 的主因。主因是 worker heap 累积过多**——actor 不断重启、重新加载数据，旧进程的内存可能还没完全释放，新进程又开始分配。

#### Object Store 为什么看起来没有释放空间

即使 object 已经 spill 到磁盘，只要它的 `ref_count > 0`（还有引用），它就不会被删除，`used_memory_` 不会减少，`available` 也不会增加：

```
Spill 流程：
  SpillIfOverPrimaryObjectsThreshold()
    → GetPrimaryBytes() / 200GB >= 0.8
    → SpillObjectUptoMaxThroughput() → 写到磁盘
    → spilled_object_pending_delete_ 加入队列
    → ProcessSpilledObjectsPendingDelete()
      → 检查 ref_count == 0
      → 如果可以删: HandleObjectDeleted() → used_memory_ -= data_size
      → 如果 ref_count > 0: 不删除 → used_memory_ 不变
```

在 GPU 利用率低的场景中，所有 object 都有引用（driver 持有 block ref + dead actor 引用泄漏 + replication 副本），所以 spill 了也不释放空间，`available` 接近 0。

#### 每个 Tidal 节点的 /dev/shm 是独立的

每个 Tidal 节点是独立的容器/Pod，有自己的独立 `/dev/shm` tmpfs（`size=536870912k` = 512 GB），不与其他节点共享。同一 IP 上多个 NodeId 是因为 Tidal 节点反复被抢占后重新创建——前一个 Pod 死亡后，新的 Pod 在同一 IP 上重新创建，有全新的 `/dev/shm`。旧的 plasma 数据随 Pod 死亡而消失。

---

## 4. 调度循环深入分析

### 4.1 调度循环架构

```python
# StreamingExecutor.run() — 独立守护线程
while True:
    continue_sched = self._scheduling_loop_step(self._topology)
    # 无 sleep，唯一暂停是内部 ray.wait(timeout=0.1)
    if not continue_sched or self._shutdown:
        break
```

### 4.2 _patched_scheduling_loop_step 流程

```
Phase 0: update_usages() — 全量资源更新
Phase 1: collect — 遍历所有 operator 收集 active task refs
          → 构建 active_tasks dict (71,000+ 条目)
          → 评估反压策略
Phase 2: ray.wait(active_tasks, timeout=0.1) — 等待完成的 task
          → GPU-first 分批处理
          → 每批后 pull_outputs + dispatch
Phase 3: Error accounting
Phase 4: Final dispatch — 最后一轮任务下发
Phase 5: Housekeeping — autoscaler、metrics、progress
Profiling: 如果总耗时 > 5s → 记录 SCHED_PROFILE
```

### 4.3 SCHED_PROFILE 日志格式

```
[SCHED_PROFILE] total=Xs update_usages=Xs collect=Xs wait=Xs
  process_dispatch=Xs final_dispatch=Xs housekeeping=Xs
  | active=N ready_gpu=N ready_cpu=N batches=N dispatched=N
```

### 4.4 关键性能瓶颈点

1. **`ray.wait()` 在 71,000+ refs 上**：正常 <100ms，但偶尔 8-9 秒
2. **`collect` 阶段**：遍历所有 operator 的 active tasks 构建字典，偶尔 6.4 秒
3. **`ready_gpu=0`**：所有 SCHED_PROFILE 条目中 GPU ready 任务数为 0

---

## 5. GPU Actor 内部架构分析

### 5.1 DistributedQwenVLVideoProcessMapper 结构

```python
class DistributedQwenVLVideoProcessMapper:
    """GPU Actor — 内部管理 CPU 预处理 Actor 池"""

    def __init__(self):
        self.cpu_actors = []  # 15 个 QwenVLCPUPreprocessActor
        self.prefetch_batches = 15  # 流水线深度
        self.max_tasks_per_actor = 4  # 每个 CPU actor 最大并发

    def _process_videos_batch(self):
        """主处理循环"""
        while has_data:
            # 1. 补充 CPU 预处理管线
            self._refill_pending_batches()  # 上限 prefetch_batches=15

            # 2. 等待任意 CPU batch 完成
            ready, _ = ray.wait(futures, num_returns=1, timeout=0.1)

            # 3. 收集 CPU 结果
            result = self._collect_batch_result(ready)

            # 4. GPU 推理
            self._execute_gpu_inference_batch(result)
```

### 5.2 CPU Actor 选择与健康检查

```python
def _select_actor(self):
    """选择负载最轻的 CPU actor"""
    # 选 in-flight task 最少的 actor
    # 如果所有 actor 都满载 → 带退避等待

def _check_not_ready_actors(self):
    """每 120s 检查 CPU actor 健康状态"""
    # ping actor → 超时则标记不可用

def _ping_actors(self):
    """Actor 心跳检测"""
    # 不可用 actor 不分配新任务
```

### 5.3 容错机制

- `max_restarts = -1`（无限重启）
- `max_task_retries = 3`
- Actor 健康检查间隔：120s
- 不可用 actor 被跳过，等待重建完成

---

## 6. 当前分析状态与待验证方向

### 6.1 已排除的原因

| # | 假设 | 排除原因 |
|---|------|---------|
| 1 | 同 IP 多 Pod 导致节点数虚高 | 实际 ALIVE=2200，DEAD=1000 来自抖动 |
| 2 | 反压限制数据供给 | 已禁用 backpressure |
| 3 | task_io 线程挤占 gRPC 线程池 | task_io 实际 CPU 仅 0.5-1.5% |
| 4 | GCS PushTask 影响 actor 方法调用 | Actor 方法走 Worker-Worker 直连 |
| 5 | 节点死亡导致大规模 Actor 重建 | 仅 ~10 节点死亡，影响不匹配 |
| 6 | prefetch_batches 过小 | 实际为 15，流水线充足 |
| 7 | SCHED_PROFILE 慢步骤 = GPU 波谷原因 | 时间对不上，burst 间隔与 GPU 波谷周期不匹配 |

### 6.2 仍需验证的方向

#### 方向 A：上游数据供给波动

`FlatMap(ClipMergeMapper)` 有 55,140 个 task，是 GPU 算子的上游。如果上游数据产出速率有波动（如某批数据文件大、网络波动等），GPU 算子的 `Queued blocks` 可能被周期性耗尽。

**验证方法**：
- 监控 `Queued blocks` 数量的时间序列变化
- 检查 FlatMap 算子的 task 完成速率是否有周期性波动

#### 方向 B：GPU Actor 内部 CPU→GPU 管线波动

每个 GPU actor 内部有 15 个 CPU preprocess actor 做视频下载/解码。如果某批视频的下载/解码耗时波动大（大文件、网络抖动、编解码复杂度差异），CPU 管线的产出速率会不稳定，导致 GPU 推理间歇性等待。

**验证方法**：
- 在 GPU actor 内部增加 CPU batch 等待时间的监控
- 检查 `ray.wait(futures, num_returns=1, timeout=0.1)` 的等待次数统计

#### 方向 C：Python GC 暂停

Driver 进程管理 71,000+ 个 ObjectRef，每次调度循环都 `list(active_tasks.keys())` 创建大量临时对象。Python 的分代 GC（默认阈值 700/10/10）可能周期性触发 Gen2 回收，导致几秒的暂停。

**验证方法**：
```python
import gc
gc.set_debug(gc.DEBUG_STATS)  # 查看 GC 触发频率
# 或
gc.disable()  # 临时禁用自动 GC，观察是否改善
```

#### 方向 D：Object Store 内存压力与溢写

`Queued blocks: 823567 (394.7GiB)` 说明 Object Store 中积压了大量数据。周期性的对象溢写（spilling to disk）和回收可能导致 `ray.wait()` 返回变慢，以及 `OBJECT_UNRECONSTRUCTABLE_LINEAGE_EVICTED` 错误导致 task 重试。

**验证方法**：
- 检查 Object Store 的 spill/restore 指标
- 检查 `OBJECT_UNRECONSTRUCTABLE` 错误的出现频率与 GPU 波谷的相关性

---

## 7. 关键技术要点总结

### 7.1 GCS 不影响 Actor 方法调用

这是最重要的发现之一。很多 Ray 性能问题排查中容易误认为 GCS 是所有通信的中心。实际上：

- **经过 GCS 的**：Actor 创建、节点注册、资源调度、Placement Group
- **不经过 GCS 的**：Actor 方法调用（`actor.method.remote()`）、普通 task 的结果获取、ObjectRef 的 `ray.get()`

### 7.2 task_io 线程仅用于可观测性

`task_events_report_interval_ms` 控制 Worker 向 GCS 上报 task 状态的频率，但这些数据仅用于 Dashboard 展示和 `ray list tasks` 命令。设为 0 可完全禁用上报而不影响任何执行逻辑。

### 7.3 ray_syncer 满载是大规模集群的固有问题

2200 个节点的集群中，`ray_syncer_io_c` 需要持续向所有节点广播资源变更。这是 O(N) 的开销，在超大集群中会持续满载。虽然不直接导致 GPU 波动，但通过向 GCS 主线程 Post 回调来间接增加主线程延迟。

### 7.4 SCHED_PROFILE 的正确解读

- SCHED_PROFILE 只记录 >5s 的慢步骤，**不记录正常步骤**
- burst 之间的间隔 = 正常调度期（每步 <5s）
- `ready_gpu=0` 表示在慢步骤期间没有 GPU task 完成，但不代表 GPU 没在工作
- 慢步骤的主要开销来自 `ray.wait()` 在 71K+ refs 上的调用和 `collect` 阶段

---

## 8. 排查方法论

### 8.1 使用的诊断工具

| 工具 | 用途 |
|------|------|
| KML Web Shell | 远程连接 Head 节点执行命令 |
| `ray status` | 查看集群节点状态（ALIVE/DEAD） |
| `/proc/<pid>/task/<tid>/stat` | 精确测量线程级 CPU（增量法） |
| `top -H -p <pid>` | 快速查看线程 CPU（注意是累积值） |
| Driver 日志 SCHED_PROFILE | 调度循环性能剖析 |
| Ray Dashboard | Actor/Task 状态概览 |
| 源码阅读 | 确认 RPC 路径和线程归属 |

### 8.2 线程 CPU 测量的正确方法

**错误做法**：使用 `top -H` 的累积 CPU 百分比，这反映的是进程启动以来的平均值，不代表当前状态。

**正确做法**：读取 `/proc/<pid>/task/<tid>/stat` 的第 14（utime）和 15（stime）字段，间隔 2 秒取两次差值，计算实时 CPU 使用率。

```bash
# 获取 GCS 进程所有线程的实时 CPU
PID=$(pgrep -f gcs_server)
for tid in /proc/$PID/task/*/stat; do
  # 读取 utime+stime，2 秒后再读一次，取增量
done
```

### 8.3 确认 RPC 路径的方法

在性能排查中，必须通过源码确认 RPC 的实际路径，而非凭直觉假设。关键确认点：
1. RPC 函数名（如 `PushTask`）在哪个组件中被调用
2. RPC 的 client 连接的是哪个服务端（GCS vs Worker）
3. RPC handler 运行在哪个线程/io_context 上

---

## 附录 A：关键源码位置

| 文件 | 关键内容 |
|------|---------|
| `src/ray/gcs/actor/gcs_actor_scheduler.cc:392-405` | PushTask 用于 Actor 创建 |
| `src/ray/core_worker/task_submission/actor_task_submitter.cc:573-582` | Actor 方法调用走 Worker 直连 |
| `src/ray/gcs/gcs_server.cc:494-496` | GCS schedule_success_handler |
| `src/ray/gcs/gcs_server_main.cc` | GCS 单主线程 `instrumented_io_context` |
| `python/ray/data/_internal/execution/streaming_executor.py` | 调度循环 `run()` 方法 |
| `kling-ray/utils/patch_interleave_dispatch.py` | GPU-first 调度优化补丁 |
| `kling-ray/pipeline/.../distributed_qwen_vl_video_process_mapper.py` | GPU Actor 实现 |

## 附录 B：SCHED_PROFILE 日志分布

```
时间轴 (5小时):
17:17 ─ 散发慢步骤 (5-10s)
17:23 ─ 升级 (33s, 64s)
17:32 ─┐
       │ 严重 burst: 步骤升级到 254s/步
17:59 ─┘ (与 17:44/17:50 节点死亡相关)
18:01 ─┐
       │ 持续慢 (10-20s/步)
18:10 ─┘
       │
       │ ← ~4 小时间隔（调度正常，每步 <5s）
       │
22:06 ─┐
       │ 新一轮 burst (5-14s/步)
22:14 ─┘

总计: 415 条记录, 平均 13.7s/步, 累计慢时间 5683s
```

---

## 9. 深入分析：Task 数过多导致 GPU 调度慢

### 9.1 核心论点

71K+ active tasks 共享一个 `ray.wait()` 调用是调度慢的关键放大因素。GPU 算子仅有 ~2000 个 task，但被 69K CPU task 的 ref 检查阻塞 8-9 秒，导致 GPU actor 在每个调度步骤中空闲等待。

### 9.2 Task 数量的构成

| 算子 | Task 数 | 占比 |
|------|---------|------|
| FlatMap(ClipMergeMapper) | 55,140 | 77.4% |
| StreamingRepartition | 11,693 | 16.4% |
| MapBatches(GPU) | ~2,000 | 2.8% |
| 其他 | ~2,167 | 3.4% |
| **合计** | **~71,000+** | 100% |

GPU 算子仅占总 task 数的 2.8%，但 `ray.wait()` 必须对所有 71K refs 逐一检查状态。

### 9.3 调度循环中 Task 数的具体影响

| 操作 | 正常情况 | 71K+ task 时 | 原因 |
|------|---------|-------------|------|
| `collect` 阶段 | 毫秒级 | 偶尔 6.4 秒 | 遍历 71K task，每个调 `get_waitable()` 构建 dict |
| `ray.wait()` | <100ms | 偶尔 8-9 秒 | 71K refs 的 O(N) 多遍扫描（详见 9.4） |
| `list(active_tasks.keys())` | 快 | GC 压力 | 创建 71K ObjectRef 临时列表 |

**GPU 的 1000 个 actor 实际等待时间 = collect 延迟 + ray.wait 延迟**，且两者的延迟主要由 CPU 算子的 55K+ task 贡献。

### 9.4 ray.wait() 的 C++ 层性能瓶颈（71K refs 场景）

`ray.wait()` 在 C++ 层对 N 个 ObjectRef 执行多遍 O(N) 操作：

```
Python ray.wait(refs, num_returns=N, timeout=0.1)
  │
  ├─ Cython: ObjectRefsToVector ─── O(N) 转换
  │
  ├─ CoreWorker::Wait (core_worker.cc:1483)
  │   ├─ flat_hash_set 构建（去重） ─── O(N) hash
  │   ├─ Owner 验证循环 ─── O(N) 次 mutex lock/unlock
  │   │   (reference_counter.cc:635, 每个 ref 单独加锁)
  │   │
  │   ├─ CoreWorkerMemoryStore::Wait
  │   │   ├─ 持 global mu_ 扫描 71K refs ─── O(N) under lock
  │   │   ├─ 注册 object_get_requests_ ─── O(N) map 插入
  │   │   └─ 清理 object_get_requests_ ─── O(N) map 删除
  │   │
  │   ├─ Raylet IPC (flatbuffer 序列化) ─── O(N) 序列化
  │   │
  │   └─ 结果构建 ─── O(N) 遍历
  │
  └─ Raylet 侧:
      ├─ WaitManager: is_object_local 检查 ─── O(N) hash 查找
      ├─ 注册 object_to_wait_requests_ ─── O(N) map 插入
      └─ 清理 ─── O(N) map 删除
```

**关键瓶颈**：

1. **Owner 验证循环**（`core_worker.cc:1512-1538`）：71K 次独立的 mutex lock/unlock，每次调用 `ReferenceCounter::HasOwner()`
2. **Memory Store 全局锁**（`memory_store.cc:275-311`）：持 `mu_` 扫描 71K entries，期间阻塞所有 Object Put/Get
3. **双重注册 Map**：`object_get_requests_`（memory store）和 `object_to_wait_requests_`（wait manager）各需 71K 条目的创建和销毁

虽然单次遍历是 O(N)，但 **6-8 遍完整 O(N) 扫描叠加**，在 N=71K 时总开销显著。

### 9.5 为什么 Task 数多不能完全解释 30 分钟周期

如果仅是 task 数多导致调度慢，波动应是**持续性**的（一直慢），而非**周期性**的。SCHED_PROFILE burst 之间有 ~4 小时的正常间隔，说明调度在大部分时间是正常的。

Task 数多是**放大器**，与其他因素叠加产生周期性波动：

```
节点抖动 → GCS 主线程忙 → Owner Worker 状态查询变慢
    ↓
71K refs 的 ray.wait() 延迟被放大（100ms → 8-9s）
    ↓
Object Store 内存压力（394.7 GiB）→ 触发 spilling
    ↓
ray.wait() 进一步变慢 + OBJECT_UNRECONSTRUCTABLE 错误
    ↓
GPU 调度步骤卡在 ray.wait()，GPU actor 空闲
```

---

## 10. ray.get() / ray.wait() 与 GCS 的交互机制

### 10.1 核心结论

**`ray.get()` 和 `ray.wait()` 不直接访问 GCS 来查找对象位置。它们访问的是对象的 Owner（即创建该对象的 Worker 进程）。**

### 10.2 对象获取完整流程

```
ray.get(obj_ref)
  │
  ├─① 检查 in-process memory store（小对象，inline 返回的）
  │   → 命中则直接返回，无任何远程通信
  │
  ├─② 对象在 local plasma（IsInPlasmaError 标记）
  │   → 通知本地 raylet fetch → 尝试从本地 plasma 读取
  │   → 如果本地已有，直接返回，无 GCS 交互
  │
  └─③ 对象不在本地（需要远程拉取）
      │
      ├─ CoreWorker → 本地 Raylet: AsyncGetObjects
      │
      ├─ Raylet → ObjectManager.Pull()
      │
      ├─ ObjectManager → OwnershipBasedObjectDirectory
      │                   .SubscribeObjectLocations()
      │                   ↓
      │        订阅对象 Owner 的 WORKER_OBJECT_LOCATIONS_CHANNEL
      │                   ↓
      │        Owner Worker 返回：对象在哪些 NodeID 上
      │
      ├─ PullManager → 向持有对象的远程节点发送 Pull Request
      │
      └─ 远程节点 → 本地 Plasma Store（对象传输完成）
```

### 10.3 GCS 参与的场景

| 场景 | 是否直接访问 GCS | 说明 |
|------|-----------------|------|
| 对象在本地 memory store | ❌ | 直接内存读取 |
| 对象在本地 plasma | ❌ | 直接 plasma 读取 |
| 对象需远程拉取 - 查位置 | ❌ 访问 **Owner Worker** | 不走 GCS |
| 检查目标节点是否存活 | ⚠️ 间接 | 读取本地缓存的 `gcs_client_.Nodes().IsNodeDead()` |
| Owner Worker 已死 | ✅ | 对象无法定位，触发 lineage reconstruction 或报错 |

### 10.4 Ownership-Based Object Directory（关键架构）

Ray 早期版本的对象位置表存放在 GCS 中，当前版本已改为**基于 Owner 的对象目录**：

```
旧架构（GCS-based）:
  Worker → GCS Object Table → 返回对象位置
  ⚠️ GCS 是中心瓶颈

新架构（Ownership-based）:
  Worker → 对象 Owner Worker（pubsub 订阅）→ 返回对象位置
  ✅ 去中心化，GCS 不参与
```

**实现位置**：`src/ray/object_manager/ownership_object_directory.cc`

- Owner Worker 维护每个对象的位置信息
- 节点添加/删除对象副本时，通过 `UpdateObjectLocationBatch` RPC 上报给 Owner
- 位置信息缓存在 `LocationListenerState` 中（`ownership_object_directory.h:87-108`）：
  - `current_object_locations` — 持有对象的 NodeID 集合
  - `spilled_url` — 对象溢写 URL
  - `object_size` — 缓存的对象大小

### 10.5 ray.wait() 与 ray.get() 的差异

| 方面 | `ray.get()` | `ray.wait()` |
|------|------------|--------------|
| Memory store | 获取所有对象 | 等待 `num_objects` 个就绪 |
| Plasma fetch | 必定 fetch | 仅 `fetch_local=True` 时 |
| Raylet 优先级 | `GET_REQUEST`（最高） | `WAIT_REQUEST`（较低） |
| Owner 联系 | 对象不在本地时联系 Owner | `fetch_local=False` 时不联系 Owner |

### 10.6 对当前场景的意义

虽然 `ray.wait()` 不直接访问 GCS，但在 71K+ refs + 节点抖动场景下：
- 部分 Owner Worker 已死，pubsub 订阅会超时
- `IsNodeDead()` 缓存频繁更新，`FilterRemovedNodes()` 需清理大量失效位置
- PullManager 队列膨胀，管理大量 pull request 的开销增加

---

## 11. task_events_report_interval_ms 配置详解

### 11.1 参数定义

```cpp
// src/ray/common/ray_config_def.h:456
RAY_CONFIG(int64_t, task_events_report_interval_ms, 1000)
```

控制 Worker 向 GCS 上报 task 状态变更的频率。**仅用于可观测性**（Dashboard Tasks 页面、`ray list tasks`），设为 0 可禁用而不影响任何执行逻辑。

### 11.2 三种配置方式

| 方式 | 语法 | 生效范围 |
|------|------|---------|
| 环境变量 | `export RAY_task_events_report_interval_ms=0` | 当前进程启动的 Ray 组件 |
| `ray start` | `--system-config='{"task_events_report_interval_ms":0}'` | 整个集群（Head 传播给 Worker） |
| `ray.init()` | `_system_config={"task_events_report_interval_ms":0}` | 本次启动的集群 |

**注意**：
- 环境变量需加 `RAY_` 前缀（`RAY_task_events_report_interval_ms`）
- `--system-config` 和 `_system_config` 用原名（不加 `RAY_` 前缀）
- `_system_config` 带下划线，是内部 API（"For testing purposes ONLY"）

### 11.3 配置优先级

从 `src/ray/common/ray_config.cc:31-78` 的 `initialize()` 函数可以看到：

```cpp
void RayConfig::initialize(const std::string &config_list) {
  // 第一步：读环境变量（覆盖默认值）
  name_ = ReadEnv<type>("RAY_" #name, #type, default_value);

  if (config_list.empty()) return;

  // 第二步：用 config_list 中的值覆盖（覆盖环境变量）
  if (pair.key() == #name) {
    name_ = pair.value().get<type>();  // 无条件覆盖
  }
}
```

**优先级从高到低**：

```
1. _system_config / --system-config（最高）
       ↓ 覆盖
2. 环境变量 RAY_xxx
       ↓ 覆盖
3. 代码默认值 default_value
```

### 11.4 集群传播机制

```
Head 节点启动:
  ray.init(_system_config={...}) / ray start --system-config='{...}'
      ↓
  GCS Server 通过 --config_list 命令行参数接收
  GCS 存储为 raylet_config_list_
      ↓
  GCS 提供 GetInternalConfig RPC（返回 raylet_config_list_）

Worker 节点加入:
  Raylet 启动 → gcs_client->InternalKV().AsyncGetInternalConfig()
      ↓
  从 GCS 拿到 config_list → RayConfig::instance().initialize(config_list)
      ↓
  CoreWorker 启动 → 同样从 GCS 获取 → initialize(config_list)
```

### 11.5 关键行为

| 场景 | 结果 |
|------|------|
| Head 设 `_system_config={"task_events_report_interval_ms": 0}` | 全集群生效，传播给所有 Worker |
| Worker 设 `RAY_task_events_report_interval_ms=500`，Head 设 `_system_config` 为其他值 | `_system_config` 覆盖环境变量 |
| 仅 Worker 设环境变量，Head 未设 `_system_config` | config_list 中无此项，环境变量仅在该 Worker 生效 |

### 11.6 关键源码路径

| 文件 | 说明 |
|------|------|
| `src/ray/common/ray_config_def.h:456` | 参数定义 |
| `src/ray/common/ray_config.cc:31-78` | `initialize()` 优先级逻辑 |
| `src/ray/common/ray_config.h:72-74` | `RAY_CONFIG` 宏和 `ReadEnv` |
| `src/ray/core_worker/task_event_buffer.cc:477` | Worker 使用该参数 |
| `src/ray/gcs/gcs_kv_manager.cc:151-156` | GCS `HandleGetInternalConfig` |
| `src/ray/raylet/main.cc:498-503` | Raylet 从 GCS 获取配置 |
| `python/ray/scripts/scripts.py:565` | `--system-config` CLI 选项 |
| `python/ray/_private/worker.py:1644` | `_system_config` 参数 |
| `python/ray/_private/services.py:198-199` | `serialize_config` base64 编码 |

---

## 12. 调度策略分析：patch_interleave_dispatch.py 瓶颈

### 12.1 优化前的调度流程

```
_patched_scheduling_loop_step:
  Phase 0: update_usages()（完整路径）
  Phase 1: collect ALL 71K tasks → 构建 active_tasks dict ─── 偶发 6.4s
  Phase 2: ray.wait(ALL 71K refs, timeout=0.1) ──────────── 偶发 8-9s
           → 分组为 gpu_batches / cpu_batches
           → 先处理 gpu_batches，再处理 cpu_batches
           → 每批后: pull_outputs + update_usages(完整) + dispatch
  Phase 3: Error accounting
  Phase 4: Final dispatch
  Phase 5: Housekeeping
```

### 12.2 五个性能瓶颈点

**瓶颈 1：collect 阶段 O(N) 遍历**

```python
# 每步重建 71K 条目的 dict
active_tasks = {}
for op, state in topology.items():
    for task in op.get_active_tasks():     # MapOperator: list(dict.values()) × 2
        active_tasks[task.get_waitable()] = (state, task)
```

- 71K 次 `get_waitable()` + 71K 次 dict 插入
- `get_active_tasks()` 每次创建新列表（`list(self._metadata_tasks.values()) + list(self._data_tasks.values())`）
- 55K FlatMap task 大多不会在本轮 ready，但每次都参与遍历

**瓶颈 2：ray.wait() 在 71K refs 上**

- `list(active_tasks.keys())` 创建 71K ObjectRef 临时列表（GC 压力）
- `num_returns=len(active_tasks)` 要求检查全部 ref
- C++ 层 6-8 遍 O(N) 扫描（详见 9.4）

**瓶颈 3：GPU 调度被 CPU refs 间接阻塞**

GPU-first 处理发生在 ray.wait() **之后**，但 ray.wait() 本身是对 ALL refs 的：

```
collect ALL 71K tasks ─── 6.4s（偶发）
ray.wait ALL 71K refs ─── 8-9s（偶发）
分组 GPU/CPU ──────────── 快
处理 GPU ready tasks ──── 快
处理 CPU ready tasks ──── 快
```

**GPU actor 等待时间 = collect + ray.wait 延迟之和**，主要由 CPU 算子的 55K+ task 贡献。

**瓶颈 4：batch 循环中的完整 update_usages()**

```python
# 每批（~33 批/步）后调用完整路径
self._resource_manager.update_usages()  # 完整路径，包含 op.update_resource_usage()
```

每步 ~33 次完整资源更新，包含对每个 operator 的状态刷新。

**瓶颈 5：_op_uses_gpu() 检测 Bug**

```python
# 原代码
_gpu_op_cache[op_id] = op.incremental_resource_usage().gpu > 0
```

但 `ActorPoolMapOperator.incremental_resource_usage()` 固定返回 `gpu=0`（`actor_pool_map_operator.py:541-546`），因为提交 task 到已有 actor 不需要额外 GPU。导致 GPU 算子被错误分类为 CPU，GPU-first 优化**完全失效**。

---

## 13. 优化方案：分离 GPU/CPU ray.wait()

### 13.1 优化概述

| 优化项 | 改动 | 预期效果 |
|--------|------|---------|
| 分离 ray.wait | GPU ~2K refs 和 CPU ~69K refs 分别 wait | GPU 调度延迟 8-9s → <200ms |
| 修复 GPU 检测 | 增加 `_ray_remote_args["num_gpus"]` 后备 | GPU-first 优化真正生效 |
| 快速路径 update_usages | batch 循环改用 `update_op_state=False` | 每步减少 ~33 次完整资源更新 |
| 提取公共 helper | `_handle_task_error` + `_process_ready_tasks` | 消除 GPU/CPU 路径代码重复 |

### 13.2 优化后的调度流程

```
_patched_scheduling_loop_step (优化后):
  Phase 0: update_usages()（完整路径）
  Phase 1: collect 分两个 dict ──────────── 同样 O(N)，但为后续分离做准备
           → gpu_active_tasks (~2K)
           → cpu_active_tasks (~69K)
           → backpressure 评估（不变）
  Phase 2a: ray.wait(GPU 2K refs, 0.1s) ──── <100ms
            → _process_ready_tasks + dispatch
            → GPU actor 立即获得新任务
  Phase 2b: ray.wait(CPU 69K refs, 0.01s) ── GPU 已 dispatch，不着急
            → _process_ready_tasks + dispatch
  Phase 3: Error accounting
  Phase 4: Final dispatch（完整 update_usages）
  Phase 5: Housekeeping
  Profiling: 拆分为 gpu_wait/gpu_process/cpu_wait/cpu_process
```

### 13.3 新增调优常量

```python
GPU_WAIT_TIMEOUT = 0.1    # GPU refs 少（~2K），等满 100ms 收集完成
CPU_WAIT_TIMEOUT = 0.01   # CPU refs 多（~69K），GPU 已 dispatch，快速扫一遍即可
```

### 13.4 修复 _op_uses_gpu()

```python
def _op_uses_gpu(op):
    op_id = id(op)
    if op_id not in _gpu_op_cache:
        try:
            gpu = op.incremental_resource_usage().gpu
            # ActorPoolMapOperator.incremental_resource_usage() 返回 gpu=0
            # 需要从 _ray_remote_args 读取实际 GPU 需求
            if gpu == 0 and hasattr(op, "_ray_remote_args"):
                gpu = op._ray_remote_args.get("num_gpus", 0)
            _gpu_op_cache[op_id] = gpu > 0
        except Exception:
            _gpu_op_cache[op_id] = False
    return _gpu_op_cache[op_id]
```

### 13.5 提取公共 helper

**_handle_task_error**：统一 data task 和 metadata task 的错误处理逻辑，使用 `error_ctx` 可变 dict 跨 GPU/CPU 阶段共享错误计数。

**_process_ready_tasks**：封装 ready refs 分组 → 排序 → 按 BATCH_SIZE 分批 → 处理 → `pull_outputs` → `update_usages(update_op_state=False)` → dispatch 的完整流水线。GPU 和 CPU 两条路径各调用一次。

### 13.6 GPU_FIRST=False 兼容性

```python
is_gpu = GPU_FIRST and _op_uses_gpu(op)
target = gpu_active_tasks if is_gpu else cpu_active_tasks
```

当 `GPU_FIRST=False` 时，`is_gpu` 始终为 `False`（短路求值），所有 task 归入 `cpu_active_tasks`，Phase 2a 被跳过，退化为单次 ray.wait 行为。

### 13.7 SCHED_PROFILE 日志格式（优化后）

```
[SCHED_PROFILE] total=Xs update_usages=Xs collect=Xs
  gpu_wait=Xs gpu_process=Xs cpu_wait=Xs cpu_process=Xs
  final_dispatch=Xs housekeeping=Xs
  | gpu_active=N cpu_active=N ready_gpu=N ready_cpu=N
    batches=N dispatched=N
```

新增 `gpu_wait`/`gpu_process`/`cpu_wait`/`cpu_process` 四个字段，替代原来的 `wait`/`process_dispatch`。新增 `gpu_active`/`cpu_active` 显示 ref 集合大小。

### 13.8 预期效果

| 指标 | 优化前 | 优化后 |
|------|--------|--------|
| GPU ray.wait 延迟 | 8-9s（71K refs） | <100ms（2K refs） |
| GPU dispatch 延迟 | 8-9s + batch 处理 | <200ms |
| CPU ray.wait 延迟 | 8-9s（71K refs） | 可能仍 0.5-2s（69K refs），但 GPU 不受影响 |
| batch 循环 update_usages | ~33 次完整调用 | 1 次完整 + ~33 次快速 |
| GPU actor 每步空闲时间 | 8-9s | <200ms |

### 13.9 修改文件

- `kling-ray/utils/patch_interleave_dispatch.py` — 调度补丁完整重写

---

## 14. 关键源码路径汇总

### 14.1 ray.get() / ray.wait() 对象获取路径

| 层级 | 文件 | 关键位置 |
|------|------|---------|
| Python API | `python/ray/_private/worker.py:2868-3014` | `ray.get()` 入口 |
| Python API | `python/ray/_private/worker.py:3169-3230` | `ray.wait()` 入口 |
| Cython | `python/ray/_raylet.pyx:2969-2978` | `get_objects()` → C++ |
| CoreWorker Get | `src/ray/core_worker/core_worker.cc:1300-1448` | `Get()` / `GetObjects()` |
| CoreWorker Wait | `src/ray/core_worker/core_worker.cc:1483-1594` | `Wait()` 主逻辑 |
| Memory Store | `src/ray/core_worker/store_provider/memory_store/memory_store.cc:259-377` | `GetImpl()` 内存对象获取 |
| Plasma Provider | `src/ray/core_worker/store_provider/plasma_store_provider.cc:253-355` | Plasma 对象 fetch |
| Object Directory | `src/ray/object_manager/ownership_object_directory.cc:320-419` | Owner 订阅位置 |
| Pull Manager | `src/ray/object_manager/pull_manager.h:456-459` | 请求优先级队列 |

### 14.2 GCS 配置传播路径

| 步骤 | 文件 | 说明 |
|------|------|------|
| 参数定义 | `src/ray/common/ray_config_def.h` | `RAY_CONFIG(type, name, default)` |
| 优先级逻辑 | `src/ray/common/ray_config.cc:31-78` | `initialize()`: 环境变量 → config_list 覆盖 |
| Python 传入 | `python/ray/_private/services.py:198-199` | `serialize_config()` base64 编码 |
| GCS 启动 | `src/ray/gcs/gcs_server_main.cc:108-120` | 解码 → `RayConfig::instance().initialize()` |
| GCS 存储 | `src/ray/gcs/gcs_kv_manager.cc:151-156` | `HandleGetInternalConfig` 返回 `raylet_config_list_` |
| Raylet 获取 | `src/ray/raylet/main.cc:498-503` | `AsyncGetInternalConfig` → `initialize()` |
| Worker 获取 | `src/ray/core_worker/core_worker_process.cc:971` | 同上 |

### 14.3 调度循环相关

| 文件 | 说明 |
|------|------|
| `python/ray/data/_internal/execution/streaming_executor.py` | `run()` 调度主循环 |
| `python/ray/data/_internal/execution/streaming_executor_state.py:904-939` | `select_operator_to_run()` O(N_operators) |
| `python/ray/data/_internal/execution/streaming_executor_state.py:748-788` | `update_operator_states()` |
| `python/ray/data/_internal/execution/operators/map_operator.py:654-655` | `get_active_tasks()` → `list(dict.values()) × 2` |
| `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py:541-546` | `incremental_resource_usage()` 返回 gpu=0 |
| `python/ray/data/_internal/execution/resource_manager.py` | `update_usages()` 完整路径 vs 快速路径 |
| `kling-ray/utils/patch_interleave_dispatch.py` | GPU-first 调度优化补丁（已优化） |

---

## 附录 C：验证清单

### C.1 优化效果验证

1. **SCHED_PROFILE 日志**：观察 `gpu_wait` 字段是否 <0.1s，`cpu_wait` 是否与原 `wait` 相近
2. **GPU 利用率曲线**：对比优化前后波动幅度
3. **`gpu_active` / `cpu_active` 计数**：确认 GPU refs ~2K，CPU refs ~69K（验证 GPU 检测修复生效）
4. **`ready_gpu` > 0**：确认 GPU 算子确实被正确分类

### C.2 回退验证

- 设置 `GPU_FIRST=False`，确认退化为单次 ray.wait 行为
- 确认 `SCHED_PROFILE` 中 `gpu_active=0`，`cpu_active` = 全部 task 数

### C.3 进一步优化方向

| 方向 | 预期收益 | 复杂度 |
|------|---------|--------|
| 减少 FlatMap 并行 task 数（`max_concurrency`） | 从源头减少 active_tasks 规模 | 低 |
| 增量 collect（维护持久化 task 集合） | 避免每步 O(N) dict 重建 | 中 |
| 分片 ray.wait（按 operator 独立 wait） | 进一步隔离各算子影响 | 中 |
| Object Store 内存管理（限制 Queued blocks） | 减少 spilling 引起的 wait 延迟 | 低 |
| 禁用 task event 上报（`task_events_report_interval_ms=0`） | 减少 Worker→GCS 网络开销 | 低 |
