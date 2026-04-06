# `ray.wait()` 大 N 调度瓶颈导致 GPU 吞吐下降分析

> 故障：`raysubmit_Grettjq2rF8W8qc7`（Qwen-VL 视频处理 Pipeline，1100 GPU actor + 550 GPU 卡）
>
> 现象：凌晨 1:30 后 GPU 利用率从 ~95% 跌到 ~66%，作业进度速度从 5,250 行/秒跌到 564 行/秒（**12%**），且**长时间不自愈**。
>
> 根因：业务方调度器 patch `patch_interleave_dispatch.py` 中 `ray.wait(N=4500+ ObjectRefs)` 触发 Ray CoreWorker `Wait` 路径上的 O(N) 全局锁开销，导致调度循环 wait 阶段从 0.13s 涨到 3-4s，下游 GPU actor 喂数频率掉 5-10×。

---

## 目录

1. [问题现象](#1-问题现象)
2. [时间线](#2-时间线)
3. [排查过程与误判修正](#3-排查过程与误判修正)
4. [关键证据](#4-关键证据)
5. [根因分析](#5-根因分析)
6. [Ray 内部 `ray.wait()` 实现路径](#6-ray-内部-raywait-实现路径)
7. [为什么进入稳态病态平衡且不自愈](#7-为什么进入稳态病态平衡且不自愈)
8. [解决方案](#8-解决方案)
9. [监控告警建议](#9-监控告警建议)
10. [相关代码位置](#10-相关代码位置)

---

## 1. 问题现象

### 1.1 核心指标对比

| 指标 | 健康基线（00:46-01:20）| 故障后（10:58）| 比例 |
|------|--------------------|--------------|------|
| 处理速度 | 5,250 行/秒 | **564 行/秒** | **12%** |
| GPU vLLM Running reqs（中位数）| 30/32 | 21/32 | 66% |
| GPU KV cache 占用（中位数）| ~50% | 36.75% | 74% |
| **调度循环 wait 阶段** | **0.13 秒** | **3.5-4 秒** | **30×** |
| **单调度周期 dispatch 数** | **322** | **57** | **18%** |
| topology active_tasks | ~3,000 | ~4,500 | 150% |
| GPU 资源占用 | 550/552 | 550/552 | hold（假象）|
| GPU 输入队列堆积 | 适量 | **284 GiB / 572k blocks** | 数据完全不缺 |

### 1.2 关键架构

```
ReadParquet → Filter(VideoClipProcessMapper) → Filter
  → StreamingRepartition[num_rows_per_block=64]
  → MapBatches(DistributedQwenVLVideoProcessMapper)   [GPU, 1100 actors × 1 GPU]
      └─ 每个 GPU MapWorker 内嵌：
         13 个 QwenVLCPUPreprocessActor (视频下载、解码、帧渲染)
         1 个 vLLM Engine (GPU 推理, max_num_seqs=32)
         prefetch_batches=26
  → FlatMap(ClipMergeMapper)                           [CPU, BlobStore 下载]
  → StreamingRepartition[num_rows_per_block=15000]
  → MapBatches(VideoClipInfoKafkaMapper)               [200 CPU actors]
  → Write
```

| 配置项 | 值 | 含义 |
|-------|---|------|
| `cpu_actor_pool_size` | 13 | 每个 GPU MapWorker 内 PreprocessActor 数 |
| `prefetch_batches` | 26 | 每个 vLLM Engine 预取 batch 数 |
| `batch_size` | 32 | vLLM `max_num_seqs` |
| `batch_timeout_sec` | 420 | 单 batch 超时 |
| `max_tasks_per_actor` | 2 | 每 GPU actor 同时跑 2 个 task |

### 1.3 业务侧未启用反压

```python
DataContext.get_current().backpressure_policies = []
```

这意味着下游慢**不会**通过反压传导到上游。但通过本文揭示的隐式机制（active_tasks 累积导致调度器变慢），下游慢仍可间接拖垮上游。

---

## 2. 时间线

```
00:18  数据集 dataset_367_0 启动，topology ramp up
00:42  active≈2843, wait=0.12s, dispatched=444/周期 — 健康
01:00  active≈7657, wait=0.86s — 接近临界
─────────────────────────────────────────────────────
01:02  ⚠️  wait 第一次跳到 4.03s — 进入慢路径
01:20  节点 10.83.9.122 收到 SIGTERM 被驱逐（K8s evict）
       9 个 lost objects 触发任务重新提交
01:30  active 峰值 10,542 — 任务大量堆积
01:34  CPU active 从 10,080 开始下降
─────────────────────────────────────────────────────
此后  wait 在 0.15s ↔ 3-4s 之间反复抖动，进入稳态病态平衡
现在  active≈4500（结构性下限），wait 仍频繁跳到 3-4s
```

---

## 3. 排查过程与误判修正

排查经历了 4 轮假设迭代，前 3 轮全部被证伪。这部分记录用于后续类似问题排查参考。

### 3.1 ❌ 第一轮假设：BlobStore 下载慢拖累 GPU

**假设**：日志中 `FlatMap(ClipMergeMapper)` 大量 `Error downloading file from S3` (2,403 次) 和 `Execution timeout after 1200000ms` (3,534 次) 表明 BlobStore 抖动，导致下游慢。

**证伪**：业务方明确说**未启用反压**，且 `ClipMergeMapper` 是 GPU 的**下游**（不是上游）。下游慢不应通过任务流影响上游 GPU。

### 3.2 ❌ 第二轮假设：BlobStore 抖动直接拖累 GPU 内嵌的 PreprocessActor

**假设**：每个 GPU MapWorker 内嵌 13 个 `QwenVLCPUPreprocessActor`，它们也从 BlobStore 下载视频喂给 vLLM。BlobStore 抖动让这些 actor 卡住，prefetch_batches 队列消耗光 → GPU 饥饿。

**证伪**：检查 `QwenVLCPUPreprocessActor` 的日志，**只有** `[PREPROCESS] start blobstore_id=...` 和 `Decode failed: nframes [2,1] got 0` 两类日志，**没有任何下载耗时打点**。无法用日志直接证明 PreprocessActor 卡在下载。诚实承认是合理推断而非确凿证据。

### 3.3 ❌ 第三轮假设：少数节点 GPU 处理慢

**假设**：可能是部分节点（磁盘 IO/网卡）个体异常，拉低集群平均吞吐。

**验证**：按节点统计 vLLM Running reqs。结果：

```
Total nodes: 87
=== 30 SLOWEST nodes (lowest avg Running reqs = GPU starving) ===
IP                samples   prompt_tps  avg_run   avg_kv%
10.83.9.240       4         5597        5.50      9.2
10.83.8.250       2         77          5.50      8.8
10.83.9.19        4         14236       8.50      14.2
10.83.10.164      6         16163       9.33      19.2
10.82.235.210     3         27379       9.67      20.8
...

Distribution of avg_running across 87 nodes:
  <5  (severely starving): 0
  5-10:  5      ← 仅 5 个节点严重饥饿
  10-20: 33
  >=20 (healthy): 49
  median=21.00 mean=20.50
```

**证伪**：仅 5/87 节点严重饥饿，**全集群均匀降低 30%**。最快的节点 Running 也只有 30/32（vs 健康 32/32）。这是**全局现象**而非局部异常，单节点假设不成立。

### 3.4 ✅ 第四轮（正确）：调度器 patch 中 `ray.wait()` 大 N 调用

业务方提示 "看 SCHED_PROFILE wait 时间变长"，按这个线索深入查 `[SCHED_PROFILE]` 日志，发现 `wait` 阶段实测 4 秒（远超代码里设的 `timeout=0.1`），定位到 patch 中的 `ray.wait()` 调用方式问题。

---

## 4. 关键证据

### 4.1 SCHED_PROFILE wait 与 active_tasks 的强相关

```
=== Wait 时间 vs active 数分桶（123 个 SCHED_PROFILE 样本）===

active 范围        样本数  平均 wait    wait/ObjectRef    平均 dispatched
2,000-4,000        5      0.13 秒      46  μs            322
4,000-6,000        108    2.02 秒     435 μs             57   ← 突变！
6,000-8,000        10     1.91 秒     286 μs            115
8,000-15,000       3      3.92 秒     390 μs            118
```

**突变阈值在 active≈4,000**。每个额外 ObjectRef 让 raylet/core_worker 增加 ~430 μs 处理时间。

### 4.2 SCHED_PROFILE 时间结构变化

健康（00:42:45）：

```
[SCHED_PROFILE] total=18.93s update_usages=0.99s collect=0.13s wait=0.12s
                process_dispatch=10.90s final_dispatch=7.88s
                | active=2843 ready_gpu=0 ready_cpu=0 batches=N dispatched=444
↑ wait 占比 0.6%，dispatch 高达 444/周期
```

异常（10:54:24，现在）：

```
[SCHED_PROFILE] total=7.05s update_usages=0.0s collect=0.0s wait=3.85s
                process_dispatch=2.55s final_dispatch=0.01s
                | active=4416 batches=N dispatched=131
↑ wait 占比 55%，dispatch 仅 131/周期
```

### 4.3 wait 在临界点反复抖动（不会自愈）

```
SCHED_PROFILE 时间序列（最近 1 小时，节选）：
09:38:56  wait=0.15s active=4571   ← 短暂恢复
09:39:21  wait=0.22s active=4381
09:46:12  wait=4.12s active=4596   ← 又跳回慢路径
10:01:05  wait=3.27s active=4611
10:21:15  wait=3.42s active=4328
10:26:16  wait=0.34s active=4643   ← 又短暂恢复
10:42:12  wait=0.15s active=4628   ← 又恢复
10:49:06  wait=3.24s active=4535   ← 又跳回
10:54:24  wait=3.85s active=4416
```

**wait 在 0.15s ↔ 3-4s 之间反复跳变**，是病态平衡（详见 §7）。

### 4.4 GCS 侧连带证据（次要）

```
gcs_server.out 全天持续，~117 次/小时：

publisher.cc:56: Pub/sub message is dropped to stay under the maximum
  configured buffer size=1073741824B (1GB buffer 满)
  channel_type: RAY_NODE_RESOURCE_USAGE_CHANNEL

gcs_task_manager.cc:382: Max number of tasks event (100000) allowed is reached.
gcs_task_manager.cc:781: Evict extra dropped task attempts(>1,000,000)
  tracked in GCS for job=42000000
```

GCS pub/sub 满 → raylet 收不到最新 reference_counter / location 信息 → `HasOwner()` 走慢路径 → 加重 `ray.wait` 中第一段循环的耗时。

---

## 5. 根因分析

### 5.1 一句话总结

**调度器 patch `_patched_scheduling_loop_step` 中一次性把 4500-10000 个 ObjectRef 传给 `ray.wait()`，触发 Ray CoreWorker `Wait` 路径上的 O(N) 全局锁开销，使每次调度循环耗时从 0.5 秒涨到 5-10 秒，下游 GPU actor 喂数频率掉 5-10×。**

### 5.2 问题代码位置

`/utils/patch_interleave_dispatch.py`（业务侧 monkey-patch）`_patched_scheduling_loop_step` 函数 Phase 1：

```python
def _patched_scheduling_loop_step(self, topology):
    # ...
    # ── Phase 1: Collect all ready task refs ─────────────────────────
    active_tasks = {}
    for op, state in topology.items():
        for task in op.get_active_tasks():
            active_tasks[task.get_waitable()] = (state, task)
    # active_tasks 现在包含整个 topology 的全部在飞 task ObjectRef
    # 实测：稳态 4500 个，峰值 10,500 个

    if active_tasks:
        t0 = _time.monotonic()
        ready, _ = ray.wait(
            list(active_tasks.keys()),       # ⚠️ 一次性塞 4500-10000 个 ref
            num_returns=len(active_tasks),   # ⚠️ 要求全部 ready
            fetch_local=False,
            timeout=0.1,                     # ⚠️ 这个 timeout 不控制 RPC 自身耗时
        )
        t_wait = _time.monotonic() - t0      # ← SCHED_PROFILE 里的 wait
```

`timeout=0.1` 只控制"等 ready 的最长时间"，**不控制 `ray.wait` 实现路径上 O(N) 部分的耗时**（详见 §6）。

### 5.3 为什么 active_tasks 会到 4500+

业务侧配置决定了在飞 task 数下限：

```
GPU MapBatches (1100 actor × 2 max_tasks_per_actor)         = 2,200 tasks
+ FlatMap(ClipMergeMapper)（CPU 任务池）                    ≈ 2,267 tasks
+ MapBatches(VideoClipInfoKafkaMapper) (200 actor + tasks) ≈ 230 tasks
+ Write                                                     ≈ 32 tasks
─────────────────────────────────────────────────────────────────────────
Total                                                       ≈ 4,729 tasks
                                                              (实测 ≈ 4,500)
```

**这是配置决定的稳态**，不是异常堆积。1100 actor × 2 = 2200 是 GPU 的最大并发，不会自然降低。

---

## 6. Ray 内部 `ray.wait()` 实现路径

### 6.1 Python 入口：`_raylet.pyx`

`/python/ray/_raylet.pyx:3252`：

```python
def wait(self,
         object_refs_or_generators,
         int num_returns,
         int64_t timeout_ms,
         c_bool fetch_local):
    cdef:
        c_vector[CObjectID] wait_ids
        c_vector[c_bool] results

    # ... ObjectRef 类型校验
    wait_ids = ObjectRefsToVector(object_refs)
    with nogil:
        op_status = CCoreWorkerProcess.GetCoreWorker().Wait(
            wait_ids, num_returns, timeout_ms, &results, fetch_local)
    # ...
```

### 6.2 C++ `CoreWorker::Wait`

`/src/ray/core_worker/core_worker.cc:1678`：

```cpp
Status CoreWorker::Wait(const std::vector<ObjectID> &ids,
                        int num_objects,
                        int64_t timeout_ms,
                        std::vector<bool> *results,
                        bool fetch_local) {
  // ...
  results->resize(ids.size(), false);

  // ── 第一段 O(N): 遍历每个 ID 做 HasOwner 检查 ──
  size_t objs_without_owners = 0;
  size_t objs_with_owners = 0;
  for (size_t i = 0; i < ids.size(); i++) {
    if (!HasOwner(ids[i])) {            // ← 每次查 reference_counter (有锁)
      ++objs_without_owners;
    } else {
      ++objs_with_owners;
    }
    if (objs_with_owners == static_cast<size_t>(num_objects)) {
      break;
    }
    // ...
  }

  // ── 第二段：进入 memory_store Wait ──
  RAY_RETURN_NOT_OK(memory_store_->Wait(
      memory_object_ids,
      std::min(static_cast<int>(memory_object_ids.size()), num_objects),
      timeout_ms,                       // ← timeout 才生效，但已流逝了第一段
      *worker_context_, &ready, &plasma_object_ids));
  // ...
}
```

**问题点**：第一段 `for` 循环在 timeout 控制范围**之外**。N=10000 时，仅这一段就吃掉数百毫秒。

### 6.3 C++ `CoreWorkerMemoryStore::GetImpl`（被 Wait 调用）

`/src/ray/core_worker/store_provider/memory_store/memory_store.cc:259`：

```cpp
Status CoreWorkerMemoryStore::GetImpl(const std::vector<ObjectID> &object_ids,
                                      int num_objects, ...) {
  (*results).resize(object_ids.size(), nullptr);

  {
    absl::flat_hash_set<ObjectID> remaining_ids;

    absl::MutexLock lock(&mu_);          // ⚠️ 全局锁，持锁时间 = O(N)

    // O(N): 遍历每个 ObjectID 查字典
    for (size_t i = 0; i < object_ids.size(); i++) {
      const auto &object_id = object_ids[i];
      auto iter = objects_.find(object_id);  // hash 查询 O(1)
      if (iter != objects_.end()) {
        iter->second->SetAccessed();
        (*results)[i] = iter->second;
        num_found += 1;
      } else {
        remaining_ids.insert(object_id);
      }
    }

    // O(N): 为每个 remaining_id 注册 GetRequest 回调
    get_request = std::make_shared<GetRequest>(...);
    for (const auto &object_id : get_request->ObjectIds()) {
      object_get_requests_[object_id].push_back(get_request);
    }
  }

  // 通知 raylet 当前 worker 进入 blocked 状态
  if (should_notify_raylet) {
    RAY_CHECK_OK(raylet_ipc_client_->NotifyWorkerBlocked());  // ← 还有 IPC
  }

  // 真正的等待 (timeout_ms 才在这里生效)
  while (!timed_out && signal_status.ok() &&
         !(done = get_request->Wait(iteration_timeout))) { ... }
}
```

**关键瓶颈**：`absl::MutexLock lock(&mu_);` 持锁期间执行 O(N) 循环。这把锁是 `CoreWorkerMemoryStore` 的全局锁，期间所有其他 task 完成回调都被阻塞（包括其他 actor 的 push）。

### 6.4 N 与 wait 时间的因果链

```
ray.wait(N=4500, timeout=0.1)
       ↓
1) CoreWorker::Wait 入口
   ├─ for i in range(4500): HasOwner(ids[i])
   │    每个 HasOwner = reference_counter_->HasOwner(id) (持有 reference_counter mutex)
   │    N=4500 时累计 ~1-2 秒 [不受 timeout_ms 控制]
   ↓
2) CoreWorkerMemoryStore::GetImpl
   ├─ absl::MutexLock lock(&mu_)            ← 全局锁 acquire
   ├─ for i in range(4500): objects_.find(id)
   │    hash 查询 + SetAccessed
   │    持锁 O(N) ≈ 1-2 秒
   ├─ for id in remaining_ids:
   │    object_get_requests_[id].push_back(...)
   │    持锁 O(N)
   │  锁内 lock.unlock() 结束 ≈ 2 秒后 [不受 timeout_ms 控制]
   ↓
3) NotifyWorkerBlocked() IPC                ≈ 几十 ms
   ↓
4) get_request->Wait(timeout_ms)            [真正受 timeout_ms 控制]
   ↓ 0.1 秒后超时返回

实测总耗时：3-4 秒（vs timeout=0.1s 的"上限"承诺）
```

---

## 7. 为什么进入稳态病态平衡且不自愈

### 7.1 反馈循环

```
            ┌─── active ≥ 4000 ───┐
            ↓                     │
  ray.wait() RPC 慢 (3-4s)         │
            ↓                     │
  调度 step 5-10s（vs 健康 0.5s）   │
            ↓                     │
  dispatch 数 50-100/周期 (vs 322) │
            ↓                     │
  GPU actor 槽出空速度 < 派发速度   │
            ↓                     │
  积压自动回填 active 数 ──────────┘
```

**稳态病态平衡**：active 不会自然降下来 — 因为下游消化变慢导致 task 长时间在飞，反过来维持高 active。

### 7.2 间歇性"假恢复"现象

观察到的 wait 在 0.15s ↔ 3-4s 反复跳变（§4.3）解释如下：

```
某个时刻一批 task 集中完成 → active 瞬间降到 4000 以下
      ↓
ray.wait() 短暂回到 0.15s
      ↓
调度器一周期内 dispatch 数暴增到 300+
      ↓
actor 槽空出立刻派发新 task
      ↓
active 立刻回到 4500
      ↓
ray.wait() 又跳回 3-4s
```

约 15% 的调度周期处于"假恢复"，85% 处于慢路径。平均吞吐被压到 12%。

### 7.3 不修代码、不重启不会改善

- **重启 job 也无法解决**：ramp up 后 active 还会爬到 4500，再次跨过阈值
- **等下游 BlobStore 恢复也无效**：实测下游 ClipMergeMapper task 数已从 1:20 时的 5,400 降到 26，但调度器仍然慢
- **是 patch 实现 + 集群规模决定的固有问题**

---

## 8. 解决方案

### 8.1 立刻可做（推荐）：分批 `ray.wait()`

**修改 `patch_interleave_dispatch.py` 的 ray.wait 调用为分批模式**：

```python
# ── Tuning knob ──────────────────────────────────────────────────────
# 单次 ray.wait 的 ObjectRef 上限。实测 active=4000 是 Ray 内部 O(N)
# 慢路径的突变阈值，留 25% 余量取 1500 较为安全。
WAIT_BATCH_SIZE = 1500


def _ray_wait_batched(active_refs, total_timeout_s=0.1):
    """分批调用 ray.wait 避免单次 N 过大。

    将 N 个 ObjectRef 按 WAIT_BATCH_SIZE 切片，逐批调用 ray.wait，
    把全局 timeout 在多批之间均摊。

    Returns: list[ObjectRef] -- 所有 ready 的 ref
    """
    ready = []
    remaining_timeout = total_timeout_s
    for i in range(0, len(active_refs), WAIT_BATCH_SIZE):
        batch = active_refs[i:i + WAIT_BATCH_SIZE]
        bt0 = _time.monotonic()
        r, _ = ray.wait(
            batch,
            num_returns=len(batch),
            fetch_local=False,
            timeout=remaining_timeout,
        )
        ready.extend(r)
        # 已花掉的时间从总 timeout 中扣除
        remaining_timeout = max(
            0.01, remaining_timeout - (_time.monotonic() - bt0)
        )
        # 提前退出：如果已经没有 timeout 余量，剩余批不阻塞，timeout=0
        if remaining_timeout <= 0.01:
            for j in range(i + WAIT_BATCH_SIZE, len(active_refs), WAIT_BATCH_SIZE):
                tail = active_refs[j:j + WAIT_BATCH_SIZE]
                r, _ = ray.wait(tail, num_returns=len(tail),
                                fetch_local=False, timeout=0)
                ready.extend(r)
            break
    return ready


# 替换原来的 ray.wait 调用：
if active_tasks:
    t0 = _time.monotonic()
    ready = _ray_wait_batched(list(active_tasks.keys()), total_timeout_s=0.1)
    t_wait = _time.monotonic() - t0
```

**预期效果**：
- 每批 1500 ref 的 `ray.wait` < 0.7 秒
- 3 批总耗时 ~2 秒（仍比原来 4 秒快 50%）
- 不会再有 active=10000 时 4 秒卡死
- 锁竞争分散到 3 次而非 1 次（其他 task 完成回调有机会插入）

### 8.2 中期：降低稳态 active_tasks 数

让稳态 active 低于 4000 阈值，从根本上避免触发 Ray 内部慢路径：

```python
# DistributedQwenVLVideoProcessMapper 配置调整
DistributedQwenVLVideoProcessMapper(
    cpu_actor_pool_size=8,         # 13 → 8 (-38%)
    prefetch_batches=12,           # 26 → 12 (-54%)
    max_tasks_per_actor=1,         # 2 → 1
)
```

调整后稳态：

```
GPU MapBatches: 1100 × 1 = 1,100 tasks  (-50%)
其他 op 不变                        ≈ 2,529 tasks
─────────────────────────────────────────────
Total                              ≈ 3,629 < 4,000 阈值 ✓
```

### 8.3 Ray 系统配置（需重启集群）

缓解 GCS pub/sub 缓冲区压力（治标，非主因）：

```bash
RAY_publisher_buffer_size_bytes=4294967296            # 1GB → 4GB
RAY_task_events_max_num_task_in_gcs=1000000           # 100k → 1M
RAY_task_events_max_dropped_task_attempts_tracked_per_job_in_gcs=10000000
```

### 8.4 Ray 内核侧优化方向（长期）

可以提交 PR 的方向：

1. **`CoreWorker::Wait` 的 `HasOwner` 循环加 short-circuit**：
   - 当前是无条件遍历所有 N 个 ref，应该在收集到 num_objects 个 owned 后就 break
   - 实际代码已有 `if (objs_with_owners == num_objects) break;`，但 Python 调用 `num_returns=len(active_tasks)` 等价于 `num_objects = N`，永远不会触发 short-circuit

2. **`CoreWorkerMemoryStore::GetImpl` 的全局锁拆分**：
   - 当前用单一 `mu_` 锁保护整个 `objects_` 字典
   - 可改用分片锁（按 ObjectID hash 分片），降低锁粒度

3. **`ray.wait` 增加 fast path**：
   - 当 N 很大时，先做 lock-free 的 `tryLock + atomic check`
   - 仅对未 ready 的 ref 进入慢路径

---

## 9. 监控告警建议

### 9.1 SCHED_PROFILE 指标暴露

把 patch 内的 `[SCHED_PROFILE]` 字段接入 Prometheus，新增指标：

| 指标名 | 类型 | 含义 |
|-------|------|-----|
| `ray_data_sched_loop_wait_seconds` | Histogram | 调度循环 wait 阶段耗时 |
| `ray_data_sched_loop_active_tasks` | Gauge | 当前 in-flight task 数 |
| `ray_data_sched_loop_dispatched_per_step` | Gauge | 单调度周期 dispatch 数 |

### 9.2 告警规则

```yaml
# 调度器进入慢路径
- alert: RayDataScheduleLoopWaitTooLong
  expr: histogram_quantile(0.5, ray_data_sched_loop_wait_seconds_bucket) > 1
  for: 60s
  labels: {severity: warning}

# 接近 ray.wait 突变阈值
- alert: RayDataActiveTasksApproachingThreshold
  expr: ray_data_sched_loop_active_tasks > 4000
  for: 120s
  labels: {severity: warning}

# 派发吞吐异常下降
- alert: RayDataDispatchRateDropped
  expr: avg_over_time(ray_data_sched_loop_dispatched_per_step[5m]) < 100
  for: 300s
  labels: {severity: critical}
```

### 9.3 排查 SOP（出现 GPU 利用率下降时）

1. **第一步：看 `[SCHED_PROFILE]` 日志**
   ```bash
   grep SCHED_PROFILE driver.log | tail -20
   # 关注 wait > 1s 或 active > 4000
   ```

2. **第二步：确认 active_tasks 来源**
   ```python
   # 查 driver 日志 logging_progress.py 输出
   # 各 op 的 Tasks 总和应该 ≈ active
   ```

3. **第三步：排除其他可能因素**
   - 节点死亡 (`gcs_server.out` 找 `death reason`)
   - GCS pub/sub drop (`gcs_server.out` 找 `Pub/sub message is dropped`)
   - Object spilling (`raylet.out` 找 `spill`)

4. **第四步：vLLM Engine 健康度**
   ```bash
   grep "KV cache usage" driver.log | tail -50
   # Running reqs 中位数 < 25 → GPU 喂数不足
   # KV cache < 30% → vLLM 严重饥饿
   ```

---

## 10. 相关代码位置

### 10.1 Ray 核心代码（kray repo）

| 路径 | 行号 | 内容 |
|-----|------|------|
| `python/ray/_raylet.pyx` | 3252 | Python `ray.wait` 入口 |
| `src/ray/core_worker/core_worker.cc` | 1678 | `CoreWorker::Wait` |
| `src/ray/core_worker/store_provider/memory_store/memory_store.cc` | 412 | `CoreWorkerMemoryStore::Wait` |
| `src/ray/core_worker/store_provider/memory_store/memory_store.cc` | 259 | `CoreWorkerMemoryStore::GetImpl` (含全局锁) |
| `python/ray/data/_internal/execution/streaming_executor.py` | — | 原版 `_scheduling_loop_step` |
| `python/ray/data/_internal/execution/streaming_executor_state.py` | — | `select_operator_to_run` (dispatch 选 op) |

### 10.2 业务侧 patch（kling-ray repo）

| 路径 | 行号 | 内容 |
|-----|------|------|
| `utils/patch_interleave_dispatch.py` | 240-251 | 问题代码：`ray.wait(N=4500+)` |
| `utils/patch_interleave_dispatch.py` | 213+ | `_patched_scheduling_loop_step` |

### 10.3 业务算子（kling-ray repo）

| 路径 | 内容 |
|-----|------|
| `pipeline/multi_video_classifier_merge/mappers/distributed_qwen_vl_video_process_mapper.py` | GPU 算子主类 |
| `pipeline/multi_video_classifier_merge/mappers/qwen_vl_cpu_preprocess_actor.py` | 内嵌的 PreprocessActor |
| `pipeline/multi_video_classifier_merge/mappers/clip_merge_mapper.py` | 下游 BlobStore 下载/合并 |

### 10.4 相关已有文档

| 路径 | 关联性 |
|-----|------|
| `docs/02-数据流与算子/Schedule-Loop优化.md` | Ray Data 调度循环工作机制 |
| `docs/06-故障排查/GPU利用率波动根因分析.md` | 类似 GPU 利用率问题（不同根因）|
| `docs/06-故障排查/Ray-Data调度问题排查.md` | 通用调度排查指南 |
| `docs/06-故障排查/Waiting-for-Scheduling排查.md` | 任务等待调度排查 |

---

## 11. 经验教训

### 11.1 `ray.wait` 大 N 调用是反模式

Ray 文档隐含警告（虽然没明说）：`ray.wait` 设计上假设输入 list 较小（通常 < 几千）。当 N 上万时，触发 CoreWorker 内部的 O(N) 路径。

**最佳实践**：
- 单次 `ray.wait` 的 ObjectRef 数 **建议 < 2000**
- 大批量场景必须分批
- 关注 `num_returns` 参数，避免设为 `len(refs)` —— 这会绕开 short-circuit 优化

### 11.2 业务侧 patch 必须考虑大规模场景

`patch_interleave_dispatch.py` 注释里写明设计目标是"减少 GPU actor 空闲 30-40s"，但作者**没有考虑 1100 actor + 26 prefetch_batches 配置下 active_tasks 会到 4500+ 的情形**。

业务 patch 在小规模测试时无问题，上线大规模后才暴露。**任何 monkey-patch Ray 内部调度的代码都应该在 production 等同规模下做压测**。

### 11.3 "无反压"不等于"上下游无关"

业务侧确认未启用反压策略，所以下游慢理论上不影响上游。但本案例揭示：

- 下游 BlobStore 抖动 → ClipMergeMapper 单 task 时间从 10s 涨到 1200s（download timeout）
- 长在飞 task 累积 → 整个 topology active_tasks 从 ~3000 涨到 10000+
- active_tasks 大 → `ray.wait()` 调度器调用慢 → GPU 喂数变慢

这是**通过调度器层的隐式耦合**，不需要显式反压策略。

### 11.4 误判记录有助于快速排查

本次排查走了 4 轮假设，前 3 轮全错。误判路径记录下来对后续类似问题有警示价值：

- BlobStore 下载错误数最多（3534 次）很容易被首先怀疑
- 单节点慢/快分布看起来异常但其实是均匀降级
- PreprocessActor 缺乏内部 timing 日志，无法直接证伪

正确的方法是**先看调度器自身指标**（SCHED_PROFILE），从内向外排查。

---

## 附录 A：本案例调试工具命令

### A.1 抓 SCHED_PROFILE 趋势

```bash
# 提取所有 SCHED_PROFILE 行
grep SCHED_PROFILE /tmp/ray/session_*/logs/job-driver-*.log

# 按 wait 时间排序
grep SCHED_PROFILE driver.log | \
  grep -oE "wait=[0-9.]+s" | sort -t= -k2 -n -r | head -20
```

### A.2 wait 与 active 相关性分析

```python
# 见正文 §4.1，Python 脚本统计 wait/active 分桶
import re
pat = re.compile(r"\[SCHED_PROFILE\] total=([\d.]+)s.*wait=([\d.]+)s.*\| active=(\d+)")
samples = []
with open("driver.log") as f:
    for line in f:
        m = pat.search(line)
        if m:
            samples.append((float(m.group(2)), int(m.group(3))))

# 按 active 范围分桶看 wait 平均值
buckets = {(0, 2000): [], (2000, 4000): [], (4000, 6000): [],
           (6000, 8000): [], (8000, 15000): []}
for wait, active in samples:
    for (lo, hi), lst in buckets.items():
        if lo <= active < hi:
            lst.append(wait)
            break
for (lo, hi), lst in sorted(buckets.items()):
    if lst:
        print(f"{lo:>5}-{hi:<6} n={len(lst):<5} avg_wait={sum(lst)/len(lst):.2f}s")
```

### A.3 节点级 GPU 状态分布

```bash
# 提取每个 GPU MapWorker 的 vLLM 状态
grep "KV cache usage" driver.log | \
  grep -oE "ip=[0-9.]+\).*Running: [0-9]+ reqs.*KV cache usage: [0-9.]+%"
```

---

## 附录 B：故障复盘时间表

| 时间 | 事件 | 累积影响 |
|-----|------|---------|
| 06-11 00:18 | 数据集启动 | active 从 0 ramp up |
| 06-11 00:42 | active=2843, healthy | dispatched=444/周期 |
| 06-11 01:00 | active=7657 | wait=0.86s, 接近临界 |
| **06-11 01:02** | **wait 第一次跳到 4s** | **进入慢路径** |
| 06-11 01:20 | 节点 SIGTERM 被驱逐，9 lost objects | task 重新提交，active 进一步涨 |
| 06-11 01:31 | active 峰值 10,542 | 调度器 step 10s，吞吐崩溃 |
| 06-11 01:34+ | active 慢慢降到 4500 | 但 wait 仍 3-4s |
| 06-11 之后 | 稳态病态平衡 | 平均吞吐 12% 不变 |
| 06-11 10:58 | 排查完成 | 已识别根因，等待修复 |

按当前速度跑完剩余 142M 行还需 **~70 小时**，健康速度只需 7.5 小时。当前进度 35.9M / 177.9M，已经丢失约 **62 小时算力时间**。

---

# 案例 2：长寿命 task → 全局锁竞争（raysubmit_UKN2YCeuS79euczh）

> 本案例是案例 1 的"近亲"——故障表现都是 GPU 利用率滞后下降、SCHED_PROFILE 调度循环 5-8s。
>
> **但底层瓶颈完全不同**：案例 1 的瓶颈在 `ray.wait` O(N) 锁路径，案例 2 的瓶颈在 `process_dispatch` 阶段的 **reference_counter / memory_store 全局锁竞争**，由 **task 寿命爆炸**（不是 task 数量）引起。
>
> 套用案例 1 的修复（分批 ray.wait）对案例 2 **无效**。

## 12. 案例 2 现象与初判

### 12.1 故障概述

| 项目 | 取值 |
|---|---|
| Job ID | `raysubmit_UKN2YCeuS79euczh` |
| 启动时间 | 2026-06-11 11:18 |
| GPU 资源 | 550/552 GPU（与案例 1 同规模） |
| GPU actor 配置 | 1100 actor × max_tasks_per_actor=2 |
| pipeline | ReadParquet → Filter → StreamingRepartition → MapBatches(QwenVL GPU) → FlatMap(ClipMergeMapper) → Repartition → MapBatches(KafkaMapper) → Write |
| 故障表征 | 18:30 之后 GPU 利用率从 ~75% 降到 30% 长期不恢复 |

### 12.2 初判被推翻

按案例 1 的"active>4000 → ray.wait O(N) 锁慢路径 → GPU 喂数慢"模型，期待数据：

> active 越大，wait 越慢；active 大幅增加是 GPU 下降的因。

**实测数据完全相反**：

| 时段 | active | wait | dispatched |
|---|---|---|---|
| 18:30 之前（n=3） | avg 11313 | **avg 2.08s** | 67 |
| 18:30 之后（n=117） | avg 7945 | **avg 0.76s** | 120 |

GPU 下降的 18:30 之后，调度器 wait 反而**变快**，active 反而**下降**。

按 active 分桶：

| active 区间 | 样本数 | avg_wait | avg_dispatched |
|---|---|---|---|
| 5,000 - 7,000 | 49 | **1.19s** | 82 |
| 7,000 - 9,000 | 27 | 0.49s | 117 |
| 9,000 - 12,000 | 44 | 0.54s | 161 |

active 5000-7000 反而 wait 最慢——**与"active 越大 wait 越长"完全反向**。直接证伪 ray.wait O(N) 是主因。

### 12.3 真正的大头是 process_dispatch

122 个 SCHED 样本统计：

- **SLOW_DISP（process_dispatch > 3s）：105 个 / 86%** ← 真正瓶颈
- SLOW_WAIT（wait > 1s）：21 个 / 17%

典型样本：

```
18:30:00  total=6.41s  wait=0.32s  process_dispatch=5.98s  active=11515  dispatched=42
18:32:01  total=6.91s  wait=0.57s  process_dispatch=6.28s  active=10967  dispatched=243
18:38:31  total=7.82s  wait=0.21s  process_dispatch=7.53s  active=8711   dispatched=131
20:19:48  total=5.56s  wait=0.16s  process_dispatch=5.19s  active=5517   dispatched=49
```

**wait 不是大头，process_dispatch 5-7s 是大头**。需要进一步定位 process_dispatch 内部的瓶颈。

---

## 13. 滞后效应：调度恶化在 GPU 崩之前

### 13.1 SCHED_PROFILE 是阈值警告

`patch_interleave_dispatch.py` 里的 `[SCHED_PROFILE]` 是 WARNING 级日志，**只在调度循环 > `PROFILE_THRESHOLD_S = 5.0` 时打印**。这意味着：

- 没打印 ≠ 调度健康
- 打印了 = 调度循环至少 5 秒

实测：作业 11:18 启动到 17:59 这 6.7 小时**完全没有 SCHED_PROFILE WARNING**。**第一条出现在 17:59:39**：

```
17:59:39  total=5.34s  wait=2.87s  active=10789  dispatched=106
```

### 13.2 滞后 15-30 分钟后 GPU 才崩

```
17:30 之前   调度健康（无 WARNING）              GPU ~5000 rows/s（健康基线）
17:51       拓扑 TOTAL task 数从 8333 涨到 10103  仍无 WARNING
17:59       第一条 SCHED_PROFILE 出现             GPU 仍健康（5000 rows/s）
18:13~18:38  调度持续 5-8s/cycle                   GPU 进度暴跌到 0（25 分钟卡顿）
18:30 之后   active 逐步下降但 wait/proc 仍慢      GPU 长期 30%（再没回到 5000）
```

**GPU 利用率滞后于调度恶化约 15-30 分钟**——这正是 GPU 算子的输入 queue（89GB） 和每个 actor `prefetch_batches=26` 提供的缓冲所致。

> **诊断启示**：看到 GPU 下降时，调度器恶化已经开始几十分钟。要看 SCHED_PROFILE 出现时间点，不是 GPU 下降时间点。

---

## 14. 真正的根因：Task 寿命，不是 Task 数量

### 14.1 Task 总数演变三阶段

从 `logging_progress.py` 输出按 op 提取 Tasks 数：

| 阶段 | 时间 | ClipMerge | Kafka | Write | Rep15k | TOTAL | 调度状态 |
|---|---|---|---|---|---|---|---|
| **健康期** | 11:51-17:21 | 5800-6500 | **0-2** | **0-9** | **0-1** | ~8300 | 0 个 SCHED_PROFILE |
| **中游堆积期** | 17:30-18:30 | 7300→**9778** | 0-13 | 0-12 | 0-7 | 9500→**11988** | 17:59 起 5-8s/cycle |
| **下游堵塞期** | 18:35→ | 9336→4521→3000 | **48-60** | **49-70** | **42-66** | 11500→5400 | 持续 5-8s/cycle |

### 14.2 关键反证：TOTAL 不是决定因素

| 时间 | TOTAL | ClipMerge | Kafka+Write+Rep15k | 调度状态 |
|---|---|---|---|---|
| 17:21（健康） | **8333** | 6124 | **9**（瞬时清空） | ✅ 健康 |
| 18:51（不健康） | **6892** | 4521 | **171**（持续堆积） | ❌ 5-8s/cycle |

**TOTAL 6892 < 8333（健康），但 18:51 却不健康**。这直接推翻"task 总数太多导致慢"的简单假说。

差别在 task 寿命：

- **17:21 健康**：下游 Kafka/Write 是瞬时通过的（< 10 task 同时存在），整个 pipeline 流速通畅
- **18:51 不健康**：下游 Kafka/Write 持续维持 60-70 个 in-flight task 不释放——单 task 寿命从 ms 级涨到分钟级

### 14.3 Task 寿命爆炸的源头：BlobStore + Kafka 抖动

在 driver 日志里持续出现：

```
ERROR:root:Error downloading file from S3: ...                         （ClipMergeMapper）
ERROR:kafka.conn:Connect attempt to BrokerConnection ... error 110
WARNING:lizhu:Decode failed for segment X blobstore_id=...
```

链条：

```
BlobStore 抖动（S3 download 间歇性失败/重试）
  ↓
ClipMerge 单 task 时间从秒级涨到分钟级（download timeout 1200s）
  ↓
ClipMerge.Tasks 从 6000 涨到 9778（堆积阶段 1）
  ↓
17:51 TOTAL 跨过 ~10000 临界 → 17:59 第一条 SCHED_PROFILE
  ↓ 滞后 15-30 分钟（input queue 89GB 缓冲）
GPU 单 actor prefetch 排空 → 18:13 GPU 开始崩

18:30 后叠加 Kafka 抖动（BrokerConnection error 110）
  ↓
Write/Kafka 单 task 寿命从 100ms 涨到几分钟（堆积阶段 2）
  ↓
即使 ClipMerge 自己消化下来 (9778→4521)，
长寿命 in-flight task 总量仍维持 ~5500 → 调度依然慢
```

### 14.4 量化对比：相同 task 数下 task 寿命不同

| 时间 | Kafka tasks | 估算每秒 task 完成数 | **单 task 平均寿命** |
|---|---|---|---|
| 17:21（健康，~5000 rows/s） | 2 | ~8000/s | **0.25 ms** |
| 18:51（不健康，~600 rows/s） | 60 | ~600/s | **100 ms** |

**寿命放大 400×**——这才是真正的恶化维度。

---

## 15. 为什么长寿命 task 把调度器拖崩——锁竞争模型

### 15.1 Python 层每个调用都最终进同一把 C++ 全局锁

`patch_interleave_dispatch.py` Phase 2 主循环：

```python
for state, batch in gpu_batches + cpu_batches:
    for task in batch:
        bytes_read = task.on_data_ready(...)   # 每 ready ref → 锁A 1次 + 锁B 1次
    _pull_operator_outputs(topology)            # 每 op output → 锁A 多次
    self._resource_manager.update_usages()      # Python O(N_ops)，但内部 op metrics 可能查 ref state → 锁A
    total_dispatched += _dispatch_tasks(...)    # 每 dispatch → SubmitTask → AddOwned + AddBorrowed → 锁A 多次
```

- **锁 A** = `ReferenceCounter::mutex_`（`reference_counter.cc`，56 处加锁）
- **锁 B** = `CoreWorkerMemoryStore::mu_`（`memory_store.cc`，文档 §6.3）

**Python 层 4 行代码，至少 3 行进锁 A**。

### 15.2 锁 A：`ReferenceCounter::mutex_`

`reference_counter.h:747-799`：

```cpp
mutable absl::Mutex mutex_;
ReferenceTable object_id_refs_              GUARDED_BY(mutex_);  // 主表
absl::flat_hash_set<ObjectID> freed_objects_ GUARDED_BY(mutex_);
std::list<ObjectID> reconstructable_owned_objects_ GUARDED_BY(mutex_);
absl::flat_hash_map<...> reconstructable_owned_objects_index_ GUARDED_BY(mutex_);
size_t num_objects_owned_by_us_              GUARDED_BY(mutex_);
// ... 共 10+ 个数据结构
```

**一把锁守护 10+ 数据结构，56 个方法都要持锁**。每个方法形如：

```cpp
// reference_counter.cc:634
bool ReferenceCounter::HasOwner(const ObjectID &object_id) const {
  absl::MutexLock lock(&mutex_);                                   // 全局锁
  return object_id_refs_.find(object_id) != object_id_refs_.end(); // 单次 hash 查询
}
```

critical section 极短（一次 hash 查询 ~100ns）但**调用频率极高**——经典"高频小锁"模式。

### 15.3 谁在抢锁

CoreWorker 进程内活跃的并发持锁线程：

| 线程来源 | 调用方法 | 频率（11500 active）| 持锁时间 |
|---|---|---|---|
| driver Python 调度循环 | ray.wait → HasOwner × N | 每个 wait 进 11500 次 | ~150ns/次 |
| gRPC server thread × N | AddOwned/AddBorrowed | ~500/s | ~200ns/次 |
| Python GC | RemoveLocalReference | ~300/s | ~300ns/次 |
| TaskFinish callback | 删 ref + state 更新 | ~200/s | ~200ns/次 |
| Reference broadcast | reply with refs | ~100/s | ~150ns/次 |

### 15.4 `CoreWorker::Wait` 入口的真正开销

`core_worker.cc:1707-1714`：

```cpp
for (size_t i = 0; i < ids.size(); i++) {
  if (!HasOwner(ids[i])) {                    // ← 每次单独 acquire mutex_
    ids_stream << ids[i] << " ";
    ++objs_without_owners;
  } else {
    ++objs_with_owners;
  }
  if (objs_with_owners == static_cast<size_t>(num_objects)) {
    break;                                     // patch 设 num_returns=N，永不触发
  }
}
```

**每次循环单独 acquire/release mutex_**。N=11500 时 11500 次 lock acquire，串行最优 ≈ 1.7ms。

### 15.5 排队论模型

`absl::Mutex` 非公平，竞争激烈时等待时间长尾分布。M/M/1 近似：

$$
W_{wait} = \frac{\rho \cdot T_c}{1-\rho},\quad \rho = \lambda \cdot T_c
$$

| 场景 | $\lambda$（lock acquire/秒）| $T_c$ | $\rho$ | 平均等待 | wait 总耗时（11500 acquires）|
|---|---|---|---|---|---|
| **健康期** | 1500 | 150ns | 0.022% | <1ns | **1.7ms** |
| **临界** | 5000 | 200ns | 0.1% | 0.2ns | 2.3ms |
| **不健康（长寿命）** | 30000 | 300ns | **0.9%** | 27ns | **~2.0s** |
| **极端** | 60000 | 400ns | **2.4%** | 980ns | **11s+** |

**关键洞察**：$\lambda$ 升高不是因为单 task lock 频率高，是因为**同时存在的 task 多 + ray.wait 入口对每个 active ref 查一次锁**：

$$
\lambda_{wait} = N_{active} \times f_{wait\_loop}
$$

健康：$\lambda = 3000 \times 0.3 = 900/s$；不健康：$\lambda = 11500 \times 0.5 = 5750/s$。

**ray.wait 自身放大锁竞争——而 patch 的 `num_returns=len(active_tasks)` 关掉 short-circuit，强制 N 次全循环**。

### 15.6 锁 B 叠加：`CoreWorkerMemoryStore::mu_`

```
Wait 入口完整流程：
  ┌──────────────────────────────────────────────────┐
  │ for i in 0..11500:                                │
  │   reference_counter_.HasOwner(ids[i])  ← 锁A      │
  └──────────────────────────────────────────────────┘
  ┌──────────────────────────────────────────────────┐
  │ memory_store_->Wait(ids, ...)                     │
  │   absl::MutexLock(mu_)  ← 锁B（持锁期间扫 11500）│
  │   for id in ids: objects_.find(id)                │
  │   for id in remaining: object_get_requests_[id]   │
  └──────────────────────────────────────────────────┘
```

锁 A 和锁 B 不同，但争同一批活跃线程。**串行延迟相加**：

- 锁 A 健康 1.7ms → 不健康 2s
- 锁 B 健康 0.5ms → 不健康 1-3s（持锁期间还有 IPC NotifyWorkerBlocked）

合计 wait 字段：健康 ~2ms，不健康 3-5s ✓ 与案例 2 日志吻合。

### 15.7 patch 改不动的本质原因

```python
# 任何排序/批次/GPU优先策略改动都不改变这个事实：
# 每行 Python 代码最终都进同一把 C++ 全局锁排队
```

**唯一治本是降 $\lambda$**：

1. 降同时在飞 ObjectRef 数（限 ClipMerge `concurrency`）
2. 降 ray.wait 入口循环 N（patch 改 `num_returns=1`）
3. 升级 head 节点 CPU（让锁 acquire 更快释放，降 $T_c$）

---

## 16. GCS pub/sub drop 与 task_events 警告——常见误判

### 16.1 现象数据

```bash
$ grep -c "Pub/sub message is dropped" gcs_server.out
52662                                # 累计 5+ 万次

$ grep "gcs_task_manager" gcs_server.out | tail
[20:50:04] Evict extra dropped task attempts(1002266 > 1000000)
[20:50:09] Max number of tasks event (100000) allowed is reached. Old task events will be overwritten.
[20:50:09] Evict extra dropped task attempts(1002210 > 1000000)
... 每 5 秒一次
```

直觉容易判断"GCS 被压垮 → 调度受影响"。但代码分析后发现**这两条警告对调度都几乎无影响**。

### 16.2 `task_events_max_num_task_in_gcs` 是纯观测通路

| 配置项 | 作用 | 谁读 | 影响调度? |
|---|---|---|---|
| `task_events_max_num_task_in_gcs` (10w) | UI/StateAPI 缓存 task 状态 | Ray Dashboard、`ray.util.state.list_tasks()` | ❌ |
| `task_events_max_dropped_task_attempts...` (100w) | 缓存 task 重试历史 | StateAPI 错误诊断 | ❌ |

调度决策走 **`reference_counter`（worker 进程内）+ `raylet local view` + `gcs_actor_manager`**，**不读 task_events**。evict / overwritten 警告只意味着 dashboard 数据不全。

**加大这两个配置：让 dashboard 数据更全，对 GPU 利用率毫无帮助。**

### 16.3 pub/sub buffer 1GB 满——drop oldest 也基本无害

`publisher.cc:54-67` 显示 drop 行为：

```cpp
} else if (max_buffered_bytes_ > 0 &&
           total_size_ + msg_size > max_buffered_bytes_) {
  // ...
  << ". Dropping the oldest message:\n"
  *front_msg = rpc::PubMessage();   // 清空 oldest
}
```

`publisher.h:42` 注释：

> `EntityState` — State for an **entity / topic** in a pub/sub channel.

**1GB 是 per-entity（per-node-per-channel）**，drop 的也是那个节点自己的旧 snapshot。

而 `RAY_NODE_RESOURCE_USAGE_CHANNEL` 是 **latest-wins 语义**：

```
buffer 内（最慢订阅者积压）:
[oldest] snap_T1 → snap_T2 → snap_T3 → ... → snap_T100 [newest]

drop oldest → snap_T1 没了
订阅者继续消费 snap_T2 → ... → snap_T100
最终视图 = snap_T100 一样准确
```

**drop 不会让订阅者视图变得更陈旧**，它只是丢掉历史中间态。资源调度不需要中间态。

### 16.4 buffer 满的真实意义：**订阅者跟不上 publish 速度**

`pending_messages_` 用 `weak_ptr`：消息只有在**至少一个订阅者还持有 shared_ptr**时不被自然清理。buffer 涨到 1GB ⇒ 至少一个订阅者积压 ~50000 条 22kB 消息。

但这个积压**不会因为加大 buffer 改善**：

| 配置 | 效果 |
|---|---|
| `RAY_publisher_buffer_size_bytes=4GB` | drop 警告日志变少 ✅<br>订阅者 view staleness ❌ 不变（消费速度不变）<br>SubmitTask 错派率 ❌ 不变<br>`wait` 字段慢 ❌ 不变 |

**结论：把这些 GCS 配置加大对你的故障基本无用，是治"日志难看"不是治"GPU 慢"**。

---

## 17. 案例 2 修复优先级

| 操作 | 实际收益 | 备注 |
|---|---|---|
| `RAY_task_events_max_num_task_in_gcs` ↑ | ❌ 无用 | 仅消除 evict warning |
| `RAY_publisher_buffer_size_bytes` ↑ | ❌ 基本无用 | 仅消除 drop warning |
| **限 ClipMerge `concurrency=4000`** | ✅ 直接降 active → 锁竞争降 | 防止 task 数失控 |
| **patch 改 `num_returns=1`** | ✅ 降 ray.wait 入口 N → wait 锁循环消失 | 但 process_dispatch 还在 |
| **治 BlobStore 抖动** | ✅✅ 治本 | task 寿命回归 ms 级 |
| **治 Kafka 抖动** | ✅✅ 治本 | 同上 |
| 升级 head 节点 GCS CPU | ⚪ 间接 | 让 publisher/订阅者消费快些 |

---

## 18. 案例 2 排查 SOP（出现 GPU 利用率下降时）

### 18.1 第一步：看 SCHED_PROFILE

```bash
DRV=/tmp/ray/session_latest/logs/job-driver-raysubmit_*.log
grep SCHED_PROFILE $DRV | tail -20
```

判断瓶颈段：

| 字段 | 含义 | 案例 1 大头 | 案例 2 大头 |
|---|---|---|---|
| `wait` | ray.wait 阻塞 | ✅ 主导（3-4s）| ⚪ 次要（<1s 多见）|
| `process_dispatch` | Phase 2 主循环 | ⚪ | ✅ **主导（5-7s）** |
| `update_usages` | 资源更新 | ⚪ | 偶发 2s+ |
| `collect` | Phase 1 收集 active task | ⚪ | 偶发 2s+ |

如果 `process_dispatch >> wait`，按案例 2 走；反之按案例 1 走。

### 18.2 第二步：抓各 op 的 Tasks 演变

```python
import re
path='/tmp/ray/session_latest/logs/job-driver-raysubmit_*.log'
ts_pat=re.compile(r'^2026-\d\d-\d\d (\d{2}:\d{2}):\d+')
op_pat=re.compile(r'logging_progress\.py:231 -- ([^:]+):')
task_pat=re.compile(r'logging_progress\.py:233 --\s+Tasks:\s*(\d+);')

# 提取每个 logging_progress 块，看下游 op (Kafka/Write) 的 Tasks 数
# 健康期下游应该是 0-10，不健康期会涨到 30-100
```

判断标志：

- 下游 op（Kafka/Write/最后一个 Repartition）Tasks 从 0-10 涨到 30+：**task 寿命爆炸**（案例 2）
- 中游 op（GPU 算子之后第一个 CPU 算子）Tasks 持续涨：**中游堆积**（案例 1 + 案例 2 早期）

### 18.3 第三步：抓"最早的 SCHED_PROFILE 出现时间"

```bash
grep SCHED_PROFILE $DRV | head -1
```

它**早于** GPU 利用率下降时间——这是滞后效应。诊断时间窗应该往前推 30 分钟。

### 18.4 第四步：看 task 寿命指标

最直接的方式是看下游 op 单 task 平均寿命。如果业务侧没有打点，可用：

```bash
# 看进度速率
grep "Total Progress" $DRV | tail -50 | python3 -c "
import sys, re
prev=None
for line in sys.stdin:
    m=re.search(r'(\d{2}:\d{2}:\d{2}).*Total Progress: (\d+)/', line)
    if m and prev:
        # rate = (curr - prev) / dt
        ...
"
```

健康期 ~5000 rows/s，掉到 < 1000 rows/s = task 寿命爆炸标志。

### 18.5 第五步：排除其他可能因素

```bash
# 节点死亡（案例 1 触发器）
grep -iE "SIGTERM|node.*die|evict|lost.*object|disconnect" $DRV | head

# Object spilling
grep -E "Spilled.*MB|PullManager.*timeout" /tmp/ray/session_latest/logs/raylet.out | tail

# 解码 / 业务异常
grep -iE "Decode failed|download.*fail|kafka.conn" $DRV | tail
```

**案例 2 关键标记**：raylet plasma 干净（**0 spill / 0 PullManager timeout**），只有业务侧 BlobStore/Kafka 错误。

### 18.6 第六步：最小验证实验

如果想直接证伪/确认 ray.wait 入口的 HasOwner 锁循环是否是 wait 慢的元凶，改 patch 一行：

```python
# patch_interleave_dispatch.py Phase 1
if active_tasks:
    ready, _ = ray.wait(
        list(active_tasks.keys()),
        num_returns=1,                    # ← 从 N 改成 1
        fetch_local=False,
        timeout=0.1,
    )
```

效果：

- 如果 wait 字段从 2-3s 降到 < 100ms：实证 ray.wait O(N) HasOwner 锁循环是因（案例 1 路径）
- 如果 wait 改不下来或 process_dispatch 仍 5+s：是 reference_counter 锁全局竞争（案例 2 路径），需要从 task 寿命下手

---

## 19. 案例 2 完整证据链（验证记录）

### 19.1 SCHED_PROFILE 时间序列（120 个样本）

第一条：17:59:39（作业 11:18 启动后 6.7 小时）。
最后一条：20:43:23。

按时段统计：

| 时段 | 平均 wait | 平均 process_dispatch | 平均 active |
|---|---|---|---|
| 17:59-18:30 | 2.08s | 4.9s | 11313 |
| 18:30-19:00 | 0.43s | 5.4s | 8500 |
| 19:00-20:00 | 1.36s | 4.0s | 5800 |
| 20:00-20:43 | 1.50s | 3.8s | 5400 |

### 19.2 GPU 算子吞吐变化

| 时段 | GPU 算子 rows/s |
|---|---|
| 11:00-17:30 | ~1083 |
| 18:35-19:30 | ~484 (-55%) |
| 19:30-20:30 | ~333 (-69%) |

注：ReadParquet 在 17:30 之前就已 100% 完成（37089846/37089846），GPU 后期吞吐下降发生在**消化 input queue 的 tail 阶段**。

### 19.3 GCS 状态

```
pub/sub drops 累计：52,662 次（每天 2500-3700 次）
pub/sub buffer 满：1073737809 / 1073741824 (1GB) 持续贴顶
gcs_task_manager evict：每 5 秒一次，每次 evict ~1003000 条 task attempts
gcs_task_manager max_reached：每 10 秒一次
raylet plasma：0 spill / 0 PullManager timeout（干净）
```

**plasma 完全干净 → 不是 object store 满，不是 spill 慢，不是 PullManager 卡——纯 task 数量级 + 寿命压力**。

### 19.4 业务侧错误日志

```
ERROR:root:Error downloading file from S3: ...                      （ClipMergeMapper，BlobStore 抖动）
ERROR:kafka.conn:Connect attempt to BrokerConnection error 110     （Kafka 抖动）
WARNING:lizhu:Decode failed: nframes [2,1] got 0                    （视频解码失败）
WARNING:lizhu:DECORDArrayCopyFromTo: size mismatch                  （Decord 库错）
WARNING:lizhu:Skipped ... all segments failed to merge              （ClipMerge 部分失败）
```

---

## 20. 案例 1 vs 案例 2 对比总表

| 维度 | 案例 1（Grettjq2rF8W8qc7） | 案例 2（UKN2YCeuS79euczh） |
|---|---|---|
| **GPU 滞后下降** | ✅ | ✅ |
| **SCHED_PROFILE 5-8s** | ✅ | ✅ |
| **wait 占调度大头** | ✅ 3-4s | ❌ 多数 < 1s |
| **process_dispatch 占大头** | ❌ | ✅ 5-7s |
| **wait 与 active 正相关** | ✅（active>4000 突变）| ❌（active 5000-7000 反而最慢）|
| **触发因子** | 节点 SIGTERM 驱逐 + 任务重提 | BlobStore + Kafka 双抖动 |
| **task 寿命变化** | ⚪ 不显著 | ✅ **400× 放大**（关键差异）|
| **底层瓶颈** | `ray.wait` O(N) 锁路径 | `reference_counter` / `memory_store` 全局锁竞争 |
| **TOTAL task 数预测性** | ✅（>4000 即慢） | ❌（8333 健康 vs 6892 不健康）|
| **plasma spill** | ⚪ 部分 | ❌ 完全干净 |
| **节点死亡事件** | ✅ 1:20 SIGTERM | ❌ 无 |
| **修复 #1：分批 ray.wait** | ✅ 有效 | ❌ 无效 |
| **修复 #2：限并发 concurrency** | ⚪ 间接有效 | ✅ 直接有效 |
| **修复 #3：治业务侧抖动** | N/A | ✅✅ 治本 |

---

## 21. 经验教训补充

### 21.1 task 数量是表象，task 寿命是本质

> "task 太多导致慢" 的直觉只对了一半。  
> 8333 健康 vs 6892 不健康 直接证明 task **数量**不决定结果，task **寿命分布**决定结果。

任何"per task"开销在 Ray Core 里都是常驻型（reference_counter 表条目、memory_store 字典条目、pub/sub 订阅状态）。短寿命 task 即使瞬时数高也无害——它们流转快。长寿命 task 即使数量减少，**累积持锁压力、view staleness、buffer 占用都长期存在**。

### 21.2 滞后效应是普遍现象

任何包含"输入队列缓冲 + 多 actor 并发预取"的 pipeline 都会有滞后效应：

- input queue 体量决定时间窗（你的 89GB → 15-30 分钟）
- prefetch_batches 决定单 actor 缓冲深度（你的 26 batch ≈ 几分钟单 actor 容忍）

**诊断时间窗必须往前推**——看 SCHED_PROFILE 第一次出现时间，不是 GPU 下降时间。

### 21.3 GCS 警告日志多数不是问题来源

`task_events_max_num_task_in_gcs reached`、`pub/sub message dropped` 这两类警告**不直接影响调度**：

- task_events 是 dashboard/StateAPI 的观测通路，不参与调度
- pub/sub drop oldest 对 latest-wins 类 channel（NODE_RESOURCE_USAGE）无害

加大它们对应的 buffer 配置只能消除日志，不能恢复 GPU 利用率。**先排除业务侧 task 寿命问题再考虑系统侧调参**。

### 21.4 patch 层无法修复底层锁竞争

业务 patch（如 `patch_interleave_dispatch.py`）在 Python 层做的任何排序、批次、优先级策略，最终都要落到 Ray Core 的 ObjectRef 操作。**Ray Core 全局锁的竞争不会因 Python 层重排而消失**。

修复路径必须从下面三选一/多：

1. **降同时在飞 ObjectRef 数**（业务侧限并发）
2. **降单次锁循环 N**（patch `num_returns=1`）
3. **降业务 task 寿命**（治根因抖动）

---

## 附录 C：案例 2 调试命令汇总

### C.1 提取 SCHED_PROFILE 完整时间序列

```python
import re
path='/tmp/ray/session_latest/logs/job-driver-raysubmit_*.log'
pat = re.compile(
    r'(\d{2}):(\d{2}):(\d{2}).*\[SCHED_PROFILE\] '
    r'total=([\d.]+)s.*?wait=([\d.]+)s.*?'
    r'process_dispatch=([\d.]+)s.*?\| active=(\d+).*?dispatched=(\d+)'
)
samples=[]
with open(path) as f:
    for line in f:
        m=pat.search(line)
        if m:
            samples.append((
                f'{m.group(1)}:{m.group(2)}:{m.group(3)}',
                float(m.group(4)),  # total
                float(m.group(5)),  # wait
                float(m.group(6)),  # process_dispatch
                int(m.group(7)),    # active
                int(m.group(8)),    # dispatched
            ))
# 分析 wait vs process_dispatch 哪个主导
slow_wait = sum(1 for s in samples if s[2] > 1.0)
slow_proc = sum(1 for s in samples if s[3] > 3.0)
print(f"SLOW_WAIT: {slow_wait}/{len(samples)}")
print(f"SLOW_PROC: {slow_proc}/{len(samples)}")
```

### C.2 各 op Tasks 演变

```python
# 见 §18.2，定位下游 op 是否堆积（task 寿命爆炸标志）
```

### C.3 GCS 健康度

```bash
L=/tmp/ray/session_latest/logs

# pub/sub drops 频率
grep -c "Pub/sub message is dropped" $L/gcs_server.out
grep "Pub/sub message is dropped" $L/gcs_server.out | \
  awk '{print substr($0,2,16)}' | grep "TODAY" | \
  awk '{print substr($0,12,5)}' | sort | uniq -c

# task_events evict
grep -c "Evict extra dropped task attempts" $L/gcs_server.out

# raylet plasma（确认是否干净）
grep -ciE "Spilled.*MB|PullManager.*timeout" $L/raylet.out
```

### C.4 验证 ray.wait 入口锁循环假说

修改 patch 一行（见 §18.6），观察 wait 字段是否大幅下降：

```python
ready, _ = ray.wait(list(active_tasks.keys()), num_returns=1, ...)
```

如果 wait 从 2-3s 降到 < 100ms 但 process_dispatch 仍 5+s ⇒ 主战场是 process_dispatch（案例 2 路径）。


