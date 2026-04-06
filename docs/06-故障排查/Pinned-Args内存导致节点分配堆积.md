# PENDING_NODE_ASSIGNMENT 堆积排查报告 —— Pinned Lease Args 内存预算耗尽

**日期**: 2026-06-01
**集群**: kce-aip-bjxy-hb1az2 / namespace: kubekml
**任务**: kml-task-100015259-record-100227197（kling-9-2）
**Head 节点**: kml-task-100015259-record-100227197-prod-head-h-0 (10.212.203.235)
**Job ID**: `raysubmit_4KvNRG3gPCqyDVhj`
**Ray 版本**: 2.52.1（内部定制版）

---

## 一、问题描述

用户反馈：Ray Job 持续运行 3 小时后进度停在 **18%（9,189,655 / 51,208,143）** 不再前进，`ray list tasks` 显示有 **8073+ 个 task 处于 `PENDING_NODE_ASSIGNMENT` 状态**，且过程中用户尝试**关闭 Ray Data Driver 端 backpressure（`OpResourceAllocator`）**，问题未缓解，反而 PENDING 数量进一步上升。

核心矛盾：
- `ray status` 显示 **0.0 / 838 CPU、0.0 / 2 GPU 全部空闲**
- `ray list workers --filter is_alive=True` 显示 **836 个 worker 进程在线**
- 但 PENDING_NODE_ASSIGNMENT 任务数 8000+，业务 RUNNING task 数 = **0**
- 17.88% needed (plasma) 的提示持续出现

---

## 二、误区先澄清（避免走错方向）

排查过程中先后排除了两个看似合理但**实际不成立**的假设：

| 假设 | 反证 |
|---|---|
| GPU 节点死光导致 GPU 算子卡住 | `Map(FaceDecupAnnScoreMapper)` 的 `required_resources` 实测只有 `CPU: 0.75, memory: 24MB`，**根本没有 GPU 字段** |
| Object Store 满了导致 task 拿不到 input object | `ray list tasks --filter state=PENDING_ARGS_AVAIL` = 0，`PENDING_OBJ_STORE_MEM_AVAIL` = 0 |
| Driver 端反压（OpResourceAllocator）卡住 dispatch | 用户已关闭，PENDING 反而上升 |

**真因**：raylet 内 `LocalLeaseManager` 的 **pinned lease arguments 内存预算耗尽**，导致 raylet 在 `GrantLease` 阶段**主动拒绝把已就绪的 task lease 给 worker**，task 退回 `WAITING_FOR_AVAILABLE_PLASMA_MEMORY` 子状态（外部统一展示为 `PENDING_NODE_ASSIGNMENT`）。

---

## 三、关键证据快照

### 3.1 集群资源全空闲，但有大量 PENDING

```bash
$ ray status
Resources
---------------------------------------------------------------
Total Usage:
 0.0/838.0 CPU       ← 838 CPU 全空闲
 0.0/2.0 GPU         ← 2 GPU 全空闲
 0B/3.15TiB memory   ← 内存全空闲
 75.21GiB/447.36GiB object_store_memory   ← object store 用了 75 GB（看似不高）
```

```bash
$ ray list workers --filter 'is_alive=True' --filter 'worker_type=WORKER' --limit 10000 | wc -l
836                  ← 836 个 worker 进程活着
```

```bash
$ ray list tasks --filter 'state=RUNNING' --limit 100 | head
Total: 97            ← 97 个 RUNNING，全是 _AutoscalingCoordinator/FunnelMetricsReporter 等心跳 actor
                       业务 task RUNNING = 0
```

```bash
$ ray list tasks --filter 'state=PENDING_NODE_ASSIGNMENT' --limit 10000 | head -3
8073 tasks (4,323,951 total, 4,315,878 truncated)
```

### 3.2 真正的卡点信号 —— `ray memory --stats-only`

这是诊断的**决定性证据**：

```bash
$ ray memory --stats-only
======== Object references status: 2026-06-01 17:40:26 ========
Plasma memory usage 300215 MiB, 12000 objects, 66.93% full, 17.88% needed   ← 关键!
Plasma filesystem mmap usage: 15877 MiB
Spilled 3751653 MiB, 38581 objects, avg write throughput 9122 MiB/s
Restored 1638025 MiB, 12236 objects, avg read throughput 2047 MiB/s
Object fetches queued, waiting for available memory.                         ← 关键!
```

含义：
- `66.93% full` —— plasma 物理占用 66.93%（300 GB / 447 GB）
- **`17.88% needed`** —— 当前 raylet 已经接受了一批 task，**还需要再额外预留 17.88% (~80 GB) 才能让这些 task 跑起来**
- 66.93 + 17.88 ≈ 84.81%，超过 raylet 内部 `max_task_args_memory_fraction = 0.7` 默认阈值的 1.21 倍
- `Object fetches queued, waiting for available memory.` —— raylet pull manager 因内存预算不够，**主动拒绝把 task lease 出去**

### 3.3 Raylet `state-dump` 中的 pinned arguments

```bash
$ grep 'Number of pinned\|Total size of pinned' /tmp/ray/session_latest/logs/raylet.out | tail
[state-dump] Number of pinned lease arguments: 347
[state-dump] Total size of pinned lease arguments: 12271490219    ← 12.27 GB
```

`max_pinned_lease_arguments_bytes_` 在该节点 = `object_store_capacity × 0.7 ≈ 9.31 GiB × 0.7 ≈ 6.5 GB`（head 节点 object store 9.31 GiB）。当前 pinned = **12.27 GB > 6.5 GB**，raylet 已严重超出 pin 预算 → 拒绝继续 grant lease。

### 3.4 Driver 端日志

```text
2026-06-01 14:43:36 WARNING issue_detector_manager.py:69 -- A task of operator
  Map(FaceDecupAnnScoreMapper) (pid=None, node_id=None, attempt=0) has been
  running for 7057.30s, which is longer than the average task duration of this
  operator (317.98s).
```

注意 `pid=None, node_id=None` —— task **从未真正派发到 worker**。它躺在 raylet 的 `waiting_lease_queue_` 里 7000 秒，driver 还以为它"在跑"。

### 3.5 资源声明实测（推翻"GPU 饥饿"假设）

```bash
$ ray list tasks --filter 'state=PENDING_NODE_ASSIGNMENT' \
    --filter 'name=Map(FaceDecupAnnScoreMapper)' --limit 1 --detail | grep -E 'CPU|GPU|memory'
required_resources:
  memory: 24178399.0           ← 23 MB
  CPU: 0.75
placement_group_id: null
label_selector: {}
                                ← 没有 GPU 字段
```

```bash
$ ray list tasks --filter 'state=PENDING_NODE_ASSIGNMENT' \
    --filter 'name=ReadParquet' --limit 1 --detail | grep -E 'CPU|GPU|memory'
required_resources:
  CPU: 1.0
                                ← 没有 GPU 字段，连 memory 也没声明
```

所有 PENDING 的 task **都不需要 GPU**。GPU worker 节点死掉（worker-1 5/6 SIGTERM）只是表象，不是直接死因。

---

## 四、Ray Task State 精确语义（提前澄清）

源码：`src/ray/protobuf/common.proto`

```protobuf
enum TaskStatus {
  NIL = 0;
  PENDING_ARGS_AVAIL = 1;            // 等待 input 对象创建完毕
  PENDING_NODE_ASSIGNMENT = 2;        // 调度器在为 task 寻找合适节点
  PENDING_OBJ_STORE_MEM_AVAIL = 3;    // ★ PENDING_NODE_ASSIGNMENT 的子状态，仅用于 metrics
  PENDING_ARGS_FETCH = 4;             // ★ 同上，子状态
  SUBMITTED_TO_WORKER = 5;            // 已派发，正在启动
  ...
  RUNNING = 8;
}
```

**关键**：`PENDING_OBJ_STORE_MEM_AVAIL` 在 `ray list tasks` 上**永远返回 0**，因为它对外被聚合进 `PENDING_NODE_ASSIGNMENT`（注释明写 `used for metrics only`）。所以前面看到 `PENDING_OBJ_STORE_MEM_AVAIL = 0` **不能排除**内存压力假设。

---

## 五、根因 —— Raylet 拒绝 Lease 的核心代码

### 5.1 决策点：`LocalLeaseManager::GrantLease`

源码：`src/ray/raylet/scheduling/local_lease_manager.cc:315-352`

```cpp
bool args_missing = false;
bool success = PinLeaseArgsIfMemoryAvailable(spec, &args_missing);
// An argument was evicted since this lease was added to the grant queue.
// Move it back to the waiting queue.
if (!success) {
  if (args_missing) {
    // ... input object 被 evict 了，回 waiting queue 头部
  } else {
    // 关键分支：input 还在，但 pin 不下
    RAY_LOG(DEBUG) << "Granting lease " << lease_id
                   << " would put this node over the max memory allowed for "
                      "arguments of granted leases ("
                   << max_pinned_lease_arguments_bytes_
                   << "). Waiting to grant lease until other leases are returned";
    RAY_CHECK(!granted_lease_args_.empty() && !pinned_lease_arguments_.empty())
        << "Cannot grant lease " << lease_id
        << " until another lease is returned and releases its arguments, but no "
           "other lease is granted";
    work->SetStateWaiting(
        internal::UnscheduledWorkCause::WAITING_FOR_AVAILABLE_PLASMA_MEMORY);
    //                                    ↑ 进入这个子状态，对外仍叫 PENDING_NODE_ASSIGNMENT
    work_it++;
  }
  continue;
}
```

### 5.2 内存预算判定：`PinLeaseArgsIfMemoryAvailable`

源码：`src/ray/raylet/scheduling/local_lease_manager.cc:760-815`

```cpp
bool LocalLeaseManager::PinLeaseArgsIfMemoryAvailable(
    const LeaseSpecification &lease_spec, bool *args_missing) {
  std::vector<std::unique_ptr<RayObject>> args;
  const auto &deps = lease_spec.GetDependencyIds();
  if (!deps.empty()) {
    // 从 plasma 拿 input ref
    if (!get_lease_arguments_(deps, &args)) {
      *args_missing = true;
      return false;
    }
    for (size_t i = 0; i < deps.size(); i++) {
      if (args[i] == nullptr) {
        // input 已被驱逐
        *args_missing = true;
        return false;
      }
    }
  }

  *args_missing = false;
  size_t lease_arg_bytes = 0;
  for (auto &arg : args) {
    lease_arg_bytes += arg->GetSize();
  }
  PinLeaseArgs(lease_spec, std::move(args));   // 先 pin（增加 pinned_lease_arguments_bytes_）

  if (max_pinned_lease_arguments_bytes_ == 0) {
    return true;                                // 未配置上限：直接通过
  }

  if (lease_arg_bytes > max_pinned_lease_arguments_bytes_) {
    // 单个 task 的 input 就超过总预算 → 警告但仍放行（避免饿死）
    RAY_LOG(WARNING) << "...";
  } else if (pinned_lease_arguments_bytes_ > max_pinned_lease_arguments_bytes_) {
    // 累计 pinned 超过阈值 → 拒绝
    ReleaseLeaseArgs(lease_spec.LeaseId());     // 撤销 pin
    return false;                                // ← 本案命中
  }

  return true;
}
```

### 5.3 阈值默认值：`max_task_args_memory_fraction = 0.7`

源码：`src/ray/common/ray_config_def.h:734`

```cpp
/// Maximum amount of memory that will be used by running tasks' args.
RAY_CONFIG(float, max_task_args_memory_fraction, 0.7)
```

源码：`src/ray/raylet/main.cc:935-941`

```cpp
RAY_CHECK(RayConfig::instance().max_task_args_memory_fraction() > 0 &&
          RayConfig::instance().max_task_args_memory_fraction() <= 1)
    << "max_task_args_memory_fraction must be a nonzero fraction.";
auto max_task_args_memory =
    static_cast<int64_t>(static_cast<float>(object_manager->GetMemoryCapacity()) *
                         RayConfig::instance().max_task_args_memory_fraction());
```

→ **每个节点的 pin 预算 = 该节点 object store 容量 × 0.7**。该作业 worker 节点 object_store = 9.31 GiB → 单节点 pin 预算 ≈ 6.5 GB。

### 5.4 状态转换图

```
                          submit
   driver ──────────────────────────────────►  raylet
                                                  │
                                  ┌───────────────┴───────────────┐
                                  ▼                                ▼
                       PinLeaseArgsIfMemoryAvailable      AllocateLocalTaskResources
                                  │                                │
              ┌───────────────────┼───────────────────┐            │
              ▼                   ▼                   ▼            ▼
       args_missing        pinned_bytes        OK → 继续        资源不够 →
       (input evict)       > max → false                       TrySpillback
              │                   │                                │
              ▼                   ▼                                ▼
       waiting_queue       waiting_queue,                  其他节点 / 留本节点
       (head insert)       SetStateWaiting(
                           WAITING_FOR_AVAILABLE_
                           PLASMA_MEMORY)
                                  │
                                  └─► 对外仍报 PENDING_NODE_ASSIGNMENT
```

---

## 六、为什么"关闭 OpResourceAllocator"反而更糟

### 6.1 Ray Data 的两层 backpressure

| 层 | 文件 | 作用 | 是否可关 |
|---|---|---|---|
| **Driver 端：OpResourceAllocator** | `python/ray/data/_internal/execution/resource_manager.py` | 算子级配额预留：每个 Op 占多少 CPU/GPU/memory 的硬上限 | ✅ 可关（`DataContext.op_resource_reservation_enabled = False`） |
| **Driver 端：StreamingExecutor dispatch loop** | `python/ray/data/_internal/execution/streaming_executor.py` | 每轮挑选要 dispatch 的算子，根据下游消费速度限速 | 部分参数可调 |
| **Raylet 端：max_task_args_memory_fraction** | `src/ray/common/ray_config_def.h` | 节点级 pin 内存预算 | ❌ **C++ 层硬约束**，需重启 raylet + 改 env |

### 6.2 关掉 OpResourceAllocator 后发生了什么

1. Driver 不再为 ReadParquet/FlatMap/Map 等算子预留独立的资源预算
2. 一次性 submit 了 **8000+ 个 task** 到 GCS（之前在 driver 内部排队）
3. 所有 task 都进 raylet `waiting_lease_queue_`
4. raylet 试图 pin 它们的 input arg → `pinned_lease_arguments_bytes_` 一路飙升 → 达到 0.7 × cap 阈值
5. raylet 拒绝所有后续 lease → PENDING 队列从几百飙到 8000+

→ **Backpressure 不是病因，是症状的抑制器**。关掉它就像取下温度计来"治"发烧。

---

## 七、完整因果链

```
某些 ReadParquet/FlatMap 输出对象异常大或异常多
            │
            ▼
plasma 内对象积压，部分被 spill 到磁盘（3.7 TB spilled，1.6 TB restored）
            │
            ▼
spill / restore 吞吐严重失衡（9.4 GB/s 写 vs 2.0 GB/s 读）
            │
            ▼
后续 task 需要的 input 必须从磁盘 restore → 单个 task input bytes 很大
            │
            ▼
raylet 在 PinLeaseArgs 阶段累计 pinned_bytes
            │
            ▼
pinned_bytes(12.27 GB) > max_pinned_lease_arguments_bytes_(≈6.5 GB)
            │
            ▼
raylet 拒绝 GrantLease，task 置为 WAITING_FOR_AVAILABLE_PLASMA_MEMORY
            │
            ▼
对外报 PENDING_NODE_ASSIGNMENT，836 worker 全部空转
            │
            ▼
用户看到 0.0/838 CPU + 8000+ PENDING，误判为"调度饥饿"
            │
            ▼
用户关闭 OpResourceAllocator（driver 端反压）
            │
            ▼
driver submit 更多 task → raylet 队列 8000+ → 现象加剧
```

---

## 八、排查流程（可复用 SOP）

### Step 1：确认是不是真的"调度饥饿"

```bash
# 集群资源
ray status

# Worker 是否在线
ray list workers --filter 'is_alive=True' --filter 'worker_type=WORKER' --limit 100000 | wc -l

# RUNNING task 数量（注意排除 actor 心跳）
ray list tasks --filter 'state=RUNNING' --limit 100 | grep -v -E 'AutoscalingCoordinator|FunnelMetrics|StatsActor' | wc -l
```

判定：
- worker 在线数 ≈ CPU 总数 → worker 没死，是 raylet 不派活
- RUNNING 业务 task = 0 → raylet 严重拒绝 lease

### Step 2：查 plasma 内存压力

```bash
ray memory --stats-only
```

**决定性指标**：
- `% full`：plasma 物理占用率
- `% needed`：raylet 已知还要再腾出的比例
- `Object fetches queued, waiting for available memory.` ← 出现这行**直接判定** pin 预算耗尽

阈值参考：`% full + % needed > 70%` 就触发 raylet 拒绝。

### Step 3：定位 pin 累计大小

```bash
grep 'Number of pinned\|Total size of pinned' \
  /tmp/ray/session_latest/logs/raylet.out | tail
```

对比节点 object store 容量 × 0.7：

```bash
ray list nodes --filter 'is_head_node=False' --filter 'state=ALIVE' --limit 1 \
  --detail | grep object_store_memory
```

`pinned_bytes > 0.7 × per_node_object_store_capacity` → 命中阈值。

### Step 4：核对 task 资源声明（排除"缺资源"假象）

```bash
ray list tasks --filter 'state=PENDING_NODE_ASSIGNMENT' \
  --filter 'name=<某算子名>' --limit 1 --detail | grep -E 'CPU|GPU|memory|placement'
```

如果 `required_resources` 很小 + 没有 `GPU` 字段 + 没有 `placement_group_id`，说明**不是资源不够**，是 pin 预算不够。

### Step 5：查 raylet state-dump 的 PullManager 状态

```bash
grep -A 5 'PullManager:' /tmp/ray/session_latest/logs/raylet.out | tail -30
```

关注：
- `RestoreSpilledObjects`：restore 次数和总耗时
- `SpillObjects`：spill 次数和总耗时
- spill > restore 数倍 → object 一直在外溢但消化不掉 → pin 永远释放不出来

### Step 6：driver 日志确认无 hang task

```bash
grep 'has been running for' /tmp/ray/session_latest/logs/job-driver-*.log | tail
```

如果出现 `pid=None, node_id=None` 且时长远超均值 → task 卡在 raylet 队列从未派发，验证 raylet 拒绝 lease。

---

## 九、解决方案

### 9.1 应急止血（当前 job 已死锁）

```bash
ray job stop raysubmit_4KvNRG3gPCqyDVhj
```

不要试图救活：3.7 TB spilled 永远追不回，pin 池永远释放不出。

### 9.2 重启时正确配置（按优先级）

#### A. **不要关 OpResourceAllocator**

恢复默认：
```python
ctx = ray.data.DataContext.get_current()
ctx.op_resource_reservation_enabled = True   # 默认
```

让 driver 帮你做第一层保护，避免 raylet 队列被打爆。

#### B. **减小 ReadParquet 单 block 体积**

每个 plasma object 平均 25 MiB（300 GB / 12000 obj），pin 一个就是一大块。把 block 切小让 pin 粒度更细：

```bash
# 原参数
--read-override-num-blocks 10000

# 改为
--read-override-num-blocks 50000     # 或更高
```

或在代码侧：

```python
ctx.target_max_block_size = 16 * 1024 * 1024   # 默认 128 MB → 16 MB
```

#### C. **降低 ReadParquet 并发**

```bash
--read-concurrency-ratio 0.25  →  0.1
```

让上游产生 object 的速度匹配下游消费速度，减少积压。

#### D. **提高节点级 max_task_args_memory_fraction**（谨慎）

若确认下游消费稳定且不会出现单 task 占用过大，可以放宽 raylet 拒绝阈值：

```bash
# 启动 ray 时 env
RAY_max_task_args_memory_fraction=0.8 ray start ...
```

⚠️ 风险：提高后更容易触发 ObjectStoreFullError。**先调 A/B/C，最后才动这个。**

#### E. **扩 plasma 容量**（终极方案）

head 节点 `free -g` 显示总内存 1007 GiB，object store 仅占 9.31 GiB / 节点。可在 raylet 启动时：

```bash
ray start --object-store-memory=$((30 * 1024**3))   # 30 GiB
```

注意：`--object-store-memory` 占用节点物理内存（mmap /dev/shm），需保证 free memory 足够。

### 9.3 长期改进建议

1. **Pipeline 设计**：避免大 block + 慢消费下游同时存在
2. **监控**：在 KML 平台告警面板加入 `plasma_pct_full + plasma_pct_needed > 70%` 触发告警
3. **Mapper hang 治理**：`FaceDecupAnnScoreMapper` 单 task 7000s（均值 318s）问题独立排查，根因可能在 ANN query 服务端

---

## 九-bis、附录：Object Store 容量是怎么算出来的（worker 实际 /dev/shm 只有 64M 的怪现象）

排查过程中发现一个反常现象：

- worker 节点 `df -h /dev/shm` 实测仅 **64 MB**
- KML 启动时未指定 `--object-store-memory`
- 但 Ray dashboard 和 `ray status` 显示该节点 object_store_memory = **9.31 GiB**
- raylet 日志同时显示 `plasma_directory=/tmp`（**不是** /dev/shm）

### 9b.1 现场数据（worker: 10.56.17.214）

```bash
$ df -h /dev/shm
shm   64M  204K   64M   1%  /dev/shm

$ free -g
total: 375G, used: 245G, free: 64G

$ cat /sys/fs/cgroup/memory.max
85899345920          # 80 GB 容器内存限额

$ grep -E 'Starting object store|create_and_mmap_buffer' /tmp/ray/session_latest/logs/raylet.out
Starting object store with directory /tmp, fallback /tmp/ray/...
dlmalloc.cc:153: create_and_mmap_buffer(10000007176, /tmp/plasmaXXXXXX)
                                       ^^^^^^^^^^^^
                                       10,000,007,176 字节 ≈ 9.31 GiB
```

### 9b.2 容量计算公式（源码）

源码：`python/ray/_private/utils.py:525-578`（`_get_object_store_memory`）

```python
def _get_object_store_memory(available_memory_bytes, object_store_memory=None):
    if object_store_memory is None:
        # 1. 默认上限：200 GB
        object_store_memory_cap = ray_constants.DEFAULT_OBJECT_STORE_MAX_MEMORY_BYTES

        # 2. Linux 上额外用 shm 大小 cap，但有保底
        if sys.platform == "linux":
            shm_avail = get_shared_memory_bytes() * 0.95
            shm_cap = max(ray_constants.REQUIRE_SHM_SIZE_THRESHOLD, shm_avail)
            #          ★ 即使 shm 很小，也保底 REQUIRE_SHM_SIZE_THRESHOLD = 10 GB
            object_store_memory_cap = min(object_store_memory_cap, shm_cap)

        # 3. 核心：节点可用内存 × 0.3
        object_store_memory = int(
            available_memory_bytes * ray_constants.DEFAULT_OBJECT_STORE_MEMORY_PROPORTION
        )

        # 4. 上限封顶
        if object_store_memory > object_store_memory_cap:
            object_store_memory = object_store_memory_cap

    return object_store_memory
```

源码：`python/ray/_private/ray_constants.py:120-134`

```python
DEFAULT_OBJECT_STORE_MAX_MEMORY_BYTES = 200 * 10**9    # 200 GB
DEFAULT_OBJECT_STORE_MEMORY_PROPORTION = 0.3            # 30%
REQUIRE_SHM_SIZE_THRESHOLD = 10**10                     # 10 GB
```

### 9b.3 套到 worker 节点的实际计算

`get_system_memory()` 同时读 `psutil.virtual_memory().total` 和 cgroup `memory.max`，取 min →
**available_memory_bytes = min(375 GB, 80 GB) = 80 GB**

| 步骤 | 计算 | 中间值 |
|---|---|---|
| 默认上限 | `200 GB` | 200 GB |
| shm cap | `max(10 GB, 64MB × 0.95) = max(10 GB, 61MB)` | **10 GB**（保底兜住） |
| object_store_memory_cap | `min(200 GB, 10 GB)` | **10 GB** |
| 按比例算 | `80 GB × 0.3` | 24 GB |
| 上限封顶 | `min(24 GB, 10 GB)` | **10 GB** ← 最终值 |

→ 10,000,000,000 字节 = 9.31 GiB ✅ 与实测 `create_and_mmap_buffer(10000007176)` 完全吻合（多出 7176 字节是 dlmalloc 元数据开销）。

### 9b.4 为什么 /dev/shm 不够却没报错

源码：`python/ray/_private/services.py:2148-2181`（`determine_plasma_store_config`）

```python
if plasma_directory is None:
    if sys.platform == "linux":
        shm_avail = get_shared_memory_bytes()
        if shm_avail >= object_store_memory:
            plasma_directory = "/dev/shm"                              # 路径 A
        elif (
            not os.environ.get("RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE")
            and object_store_memory > REQUIRE_SHM_SIZE_THRESHOLD       # 严格 >
        ):
            raise ValueError("/dev/shm size ({}) ... This will harm performance.")
        else:
            plasma_directory = get_user_temp_dir()                     # 路径 C：fallback /tmp
            logger.warning("WARNING: The object store is using /tmp instead of /dev/shm ...")
```

套数据：
- `shm_avail = 64M < object_store_memory = 10 GB` → 不走路径 A
- `object_store_memory = 10**10`，`REQUIRE_SHM_SIZE_THRESHOLD = 10**10`，**`10**10 > 10**10 == False`** → 不走 raise 路径
- 走路径 C：fallback 到 `/tmp`，只打 WARNING

这是一个**边界值巧合**：默认 cap 恰好等于"严格大于"阈值，让默认配置永远不会触发那个 ValueError。

### 9b.5 性能影响

| 介质 | 读写途径 |
|---|---|
| 期望：/dev/shm (tmpfs) | 纯内存，read/write 走 page cache 直接命中物理内存 |
| 实际：/tmp (overlay rootfs) | 经过 overlay → underlying fs → host kernel page cache → 持久化磁盘 |

回头看 `ray memory --stats-only`：

```
Spilled    9.4 GB/s   ← 写：kernel page cache 命中，看着快
Restored   2.1 GB/s   ← 读：cache miss 时真的从磁盘读，慢 4.5×
```

这印证了 plasma 介质就是慢盘，是 spill/restore 严重失衡的物理原因，进一步加剧了 pinned args 池被锁死的速度。

### 9b.6 修复（结合主报告 9.2 节）

**根治：让 KML 在 worker pod spec 加 emptyDir tmpfs**

```yaml
volumes:
  - name: dshm
    emptyDir:
      medium: Memory      # ★ 关键
      sizeLimit: 30Gi
volumeMounts:
  - mountPath: /dev/shm
    name: dshm
```

或 docker run 加 `--shm-size=30g`。

**验证成功标志**：

```bash
grep 'Starting object store' /tmp/ray/session_latest/logs/raylet.out
# 应输出:  Starting object store with directory /dev/shm  ← 不再是 /tmp
```

**临时缓解（不改 pod spec 的话）**：启动时显式指定

```bash
ray start --object-store-memory=$((20 * 1024**3)) ...
```

但 plasma 仍在 overlay /tmp，只增容不增速。

---

## 九-ter、附录：plasma 后端深度解析（tmpfs vs ext4/overlay）

排查过程衍生出几个相关问题，这里一并整理：

1. 为什么指定 `--object-store-memory=32GB` 直接报错，而默认值 10GB 却安静 fallback？
2. /dev/shm 和 tmpfs 是什么关系，是 Linux 规定的吗？
3. /dev/shm 让 plasma "真的是内存"，/tmp 是"磁盘 mmap"，区别在哪？
4. tmpfs 后端没磁盘，object store 是怎么 spill 的？

### 9c.1 为什么指定 32GB 报错，10GB 不报错 —— 边界值 `>` 的细节

源码：`python/ray/_private/services.py:2148-2181`

```python
if shm_avail >= object_store_memory:
    plasma_directory = "/dev/shm"                           # 路径 A
elif (
    not os.environ.get("RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE")
    and object_store_memory > REQUIRE_SHM_SIZE_THRESHOLD    # ★ 严格大于 10 GB
):
    raise ValueError(
        "The configured object store size ({} GB) exceeds /dev/shm size ({} GB). "
        "This will harm performance. Consider deleting files in /dev/shm or increasing "
        "its size with --shm-size in Docker. To ignore this warning, "
        "set RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE=1."
    )                                                       # 路径 B：raise
else:
    plasma_directory = get_user_temp_dir()                  # 路径 C：silent fallback /tmp
```

两种情况对比：

| 配置 | shm_avail | object_store_memory | 条件 1: `shm>=obj` | 条件 2: `obj > 10GB` | 结果 |
|---|---|---|---|---|---|
| **默认 10GB**（自动计算） | 64M | `10**10` | False | **False**（`10**10 > 10**10 == False`） | 路径 C，fallback /tmp，**只 WARNING** |
| **指定 `--object-store-memory=32GB`** | 64M | `3.2×10**10` | False | **True** | 路径 B，**raise ValueError** |

差别就在 `>` 这个**严格大于**：默认值 10GB 恰好踩在阈值上，**`10**10 > 10**10` 为 False**，所以默默走 fallback；指定 32GB 严格超过阈值，直接 raise。

源码常量定义（`python/ray/_private/ray_constants.py:132-134`）：

```python
# Above this number of bytes, raise an error by default unless the user sets
# RAY_ALLOW_SLOW_STORAGE=1. This avoids swapping with large object stores.
REQUIRE_SHM_SIZE_THRESHOLD = 10**10  # 10 GB
```

设计意图：Ray 团队认为 **>10 GB** 的 plasma 放在慢盘上性能损失太大，必须强制让用户主动确认；≤10 GB 静默 fallback 不打扰。

**绕过 raise 的三种方式**：

```bash
# A. 设环境变量绕过（plasma 仍在慢盘，性能不会更好）
RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE=1 ray start --object-store-memory=$((32 * 1024**3))

# B. 把 /dev/shm 调大（治本，推荐）
# pod spec:
#   volumes: - name: dshm
#       emptyDir: { medium: Memory, sizeLimit: 40Gi }
#   volumeMounts: - { mountPath: /dev/shm, name: dshm }

# C. 不显式指定，让 Ray 默认走 10GB（最稳）
ray start --address=...    # 不带 --object-store-memory
```

### 9c.2 /dev/shm 和 tmpfs 是什么关系 —— 路径惯例 vs 内核实现

它们是**两个层次的概念**：

| 概念 | 是什么 | 由谁规定 |
|---|---|---|
| `tmpfs` | Linux 内核实现的**文件系统类型**（源码：`mm/shmem.c`） | Linux kernel |
| `/dev/shm` | 发行版默认挂载 tmpfs 的**路径惯例**（来自 POSIX `shm_open(3)`） | POSIX + Linux 发行版 |

绑定关系：**Linux 发行版（systemd）默认在 `/dev/shm` 路径挂一个 tmpfs**，方便 `shm_open()` 等 POSIX 共享内存 API 使用。

```bash
$ mount | grep /dev/shm
tmpfs on /dev/shm type tmpfs (rw,nosuid,nodev,size=64M)
#       ^^^^^^^                ^^^^^
#       挂载源（虚拟）          fs 类型
```

完全可以解耦：
- 把 tmpfs 挂在 `/foo/bar` 上 → 那个目录就有 tmpfs 的全部特性
- 把 ext4 挂在 `/dev/shm` 上（不推荐）→ `/dev/shm` 退化成普通磁盘

**所以 plasma 走得快是因为 tmpfs 这个内核实现，不是因为路径叫 `/dev/shm`**。Ray 选 `/dev/shm` 只是因为发行版默认在那挂 tmpfs，约定俗成。

**为什么 Linux 内核要专门设计 tmpfs**（而不是普通 fs + ramdisk）：POSIX 规定 shared memory 要表现为"文件"（有 fd 才能被多进程 mmap），Linux 实现这个 API 的办法就是**给一个永远不落盘的"假文件系统"**。tmpfs 不是为了"快"，是为了**给共享内存提供文件接口**。它"快"是副产物。

**为什么容器 `/dev/shm` 默认只有 64M**：这是 Docker 的设计选择（不是内核或 POSIX）。容器隔离要求每个容器有独立 `/dev/shm`，默认给 64M 是保守值，避免某个容器把 host RAM 吃干。K8s 沿用，结果就是"想用 30G plasma 但 /dev/shm 只有 64M"的尴尬。

### 9c.3 tmpfs 和 ext4/overlay 在 mmap 时的根本区别

要从 **Linux 文件系统的 backing store 设计**说起。

#### tmpfs 是"没有 backing store"的特殊 fs

```
┌─────────────────────────────────────────────────┐
│  应用 mmap(MAP_SHARED, fd)                       │
└─────────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────┐
│  Linux VFS（统一虚拟文件系统层）                  │
└─────────────────────────────────────────────────┘
                    │
        ┌───────────┼───────────┐
        ▼           ▼           ▼
   ┌─────────┐ ┌─────────┐ ┌─────────┐
   │ tmpfs   │ │  ext4   │ │ overlay │
   │/dev/shm │ │ /tmp    │ │ /tmp    │
   └─────────┘ └─────────┘ └─────────┘
        │           │           │
        ▼           ▼           ▼
   inode 没有     inode 关联    inode 关联
   block 号       磁盘 block    底层 block
        │           │           │
        ▼           ▼           ▼
   inode 页表     inode 页表    inode 页表
   直接指向       指向 page     指向 page
   RAM 页         cache         cache
                    │           │
                    ▼           ▼
                后台 writeback   后台 writeback
                到磁盘 block     到底层 disk block
```

源码对比：

```c
// Linux mm/shmem.c — tmpfs 读页
static int shmem_get_folio_gfp(struct inode *inode, ...) {
    folio = shmem_alloc_folio(...);  // ← 直接分配 RAM 页
    return 0;
}

// Linux fs/ext4/inode.c — ext4 读页
static int ext4_readpage(struct file *file, struct page *page) {
    ext4_mpage_readpages(inode, NULL, page);  // ← 发起 BIO 从磁盘读
}
```

**tmpfs 文件的存储位置就是 RAM 本身**，不存在"磁盘上的文件"。`df -h /dev/shm` 显示的 size 是 mount 时 `size=` 选项的上限，不是磁盘容量。

#### mmap 在两种 fs 上的行为差异

| 行为 | tmpfs (/dev/shm) | ext4/overlay (/tmp) |
|---|---|---|
| mmap 一个 10 GB 文件 | inode 页表空的，不占 RAM | inode 关联 10 GB 磁盘 block |
| 第一次 touch 一个页 | 分配 RAM 页（**永久持有**） | 从磁盘读到 page cache 页（**可被 evict**） |
| 写入一个页 | 改 RAM 页，完成 | 改 page cache 页，标记 dirty，**后台 writeback** |
| 内存压力大 | 只能 swap 出去（要开 swap） | dirty 页 writeback、clean 页 evict |
| 页被 evict 后再访问 | 不会被 evict | **page fault → 磁盘 IO 读回**（毫秒级） |
| `df -h` 显示 size | tmpfs `size=` 参数 | 真磁盘容量 |
| `cgroup memory.stat` 归属 | shmem 项（**计入 pod working_set**） | file 项（**通常不计 working_set**） |

#### 对 plasma 的影响

plasma 假设是 **"object 一旦 put 就立即可见、读取零 IO"**。这只有在"页永远不会被 evict"的前提下成立。

**在 /dev/shm（tmpfs）上**：

```
worker_A.put(obj_1GB)
  → memcpy 到 mmap 区域 → 1 GB 写入 RAM 页（tmpfs 里）
  → put 返回，~ns 级

worker_B.get(obj)
  → 通过 plasma client 拿到 offset
  → 直接读 mmap 区域 → 命中同一组 RAM 页
  → 零 IO，~ns 级
```

**在 /tmp（overlay）上**：

```
worker_A.put(obj_1GB)
  → memcpy 到 mmap 区域 → 1 GB 写入 page cache 页（dirty）
  → put 立即返回（写看着快）

[后台异步发生]
  → kworker 把 1 GB dirty 页 writeback 到 overlay 底层磁盘
  → 写入完成后页变 clean，可被回收

worker_B.get(obj)  [若页已被回收]
  → 读 mmap 区域 → page table not present → page fault
  → ext4/overlay 从磁盘 block 读 → 重新填入 page cache 页
  → 1 GB 磁盘 IO，毫秒~秒级
  → worker 表现为"卡顿"
```

#### 隐藏的杀手：cgroup 内存计费

| | tmpfs 页 | page cache 页 |
|---|---|---|
| **回收优先级** | 几乎不回收（除非 swap） | **可随时 evict**（active/inactive LRU） |
| **K8s pod 内存计费** | 计入 `working_set` | **通常不计 `working_set`**（可回收） |
| **OOM killer 决策** | 触发 pod OOM | 内存压力时先 evict，不触发 OOM |

→ plasma 在 /tmp 上时，**K8s pod 看到的内存使用很低**（page cache 不算工作集），但 host 实际内存压力可能很大。**当 host 真的爆了，K8s 会选 working_set 大的 pod 杀，但真正的内存大户（plasma）反而看不出来 → 整台机器 NotReady → 一批 pod 集体 SIGTERM**。

**这很可能解释了 worker-1 那 5 个 SIGTERM 节点的死因**：plasma 在 /tmp 撑满了 host page cache，触发 host 级内存压力，K8s 把整台机器上的 pod 都驱逐了。

### 9c.4 tmpfs 没有磁盘，object store 是怎么 spill 的

**关键澄清**：plasma 放在哪儿（`plasma_directory`）和 spill 到哪儿（`object_spilling_config`）是**两套完全独立的配置**。

#### 两个目录的职责分工

```
┌─────────────────────────────────────────────────────────────┐
│  Ray 节点上有两个完全独立的存储位置                          │
└─────────────────────────────────────────────────────────────┘

  ┌──────────────────────────┐    ┌──────────────────────────┐
  │  plasma_directory        │    │  object_spilling 路径    │
  │  (in-memory 部分)        │    │  (overflow 部分)         │
  ├──────────────────────────┤    ├──────────────────────────┤
  │ 默认: /dev/shm           │    │ 默认: /tmp/ray/.../...   │
  │ 后端: tmpfs (RAM)        │    │ 后端: 普通磁盘文件系统    │
  │ 容量: 10 GB 左右         │    │ 容量: 默认 95% 磁盘空间   │
  │ 用途: 热数据，mmap 共享  │    │ 用途: 冷数据，spill 落盘 │
  └──────────────────────────┘    └──────────────────────────┘
            │                                ▲
            │                                │
            │  plasma 满了 → 选最 LRU 的 obj   │
            └───────► 序列化写入 spill 文件 ──┘
                                ▲
                                │
                  需要重新使用 → restore 读回 plasma
```

#### Ray 默认配置

源码：`python/ray/_private/external_storage.py`

```python
# spill 默认配置
RAY_DEFAULT_OBJECT_SPILLING_CONFIG = {
    "type": "filesystem",
    "params": {
        "directory_path": "/tmp/ray/session_xxx/ray_spilled_objects_xxx"
    }
}
```

实测在 worker 节点：

```bash
$ ls /tmp/ray/session_latest/
ray_spilled_objects_130de2ef83378488dbdb0827898749705013240ac43b0d9267054139
                    ↑
            这就是 spill 文件存放目录（在 overlay 磁盘上，不在 tmpfs）
```

#### 完整的 spill 流程

```
plasma (tmpfs, 10GB)         spill dir (磁盘, 1.9TB)
═══════════════════════      ═══════════════════════
[obj_A][obj_B]... 满了
         ↓
    选最 LRU 的 obj
         ↓
    SpillObjects RPC
         ↓
    序列化 obj → 写入 spill 文件
    ───────────────────────►   /tmp/ray/.../spill_xxx
         ↓
    plasma 里 obj 标记为 OUT_OF_PLASMA
    （元数据保留，数据释放）
         ↓
    tmpfs 页归还给 kernel
         ↓
    plasma 腾出空间放新 obj


[需要 restore 时]
    RestoreSpilledObjects RPC
         ↓
    从 /tmp/ray/.../spill_xxx 读回字节流
         ↓
    deserialize 写回 plasma 新位置
    /dev/shm/plasmaXXXXXX  ◄───
         ↓
    plasma 元数据指向新位置
```

#### 注意区分：plasma 的 `fallback_directory` ≠ spill 目录

raylet 启动日志会有两个目录：

```
Starting object store with directory /tmp,                    ← plasma 主存位置
                       fallback   /tmp/ray/session_xxx        ← plasma 内部 fallback
```

这两个都是 plasma C++ 层（dlmalloc）的目录：
- `plasma_directory`：plasma mmap 主区域（本案应是 /dev/shm，被 fallback 到 /tmp）
- `fallback_directory`：plasma 主区域写满时**临时再 mmap 一块**，仍是共享内存语义

**真正的 spill 是上层 Python/raylet 的 LRU 驱逐机制**，目录是 `/tmp/ray/.../ray_spilled_objects_xxx`，序列化字节流写入磁盘文件。

#### 为什么不能直接把 spill 配到 tmpfs

理论上可以：

```python
ray.init(_system_config={
    "object_spilling_config": json.dumps({
        "type": "filesystem",
        "params": {"directory_path": "/dev/shm/ray_spill"}
    })
})
```

但**没意义**：

1. spill 的**目的**就是"plasma 放不下了，挪到便宜存储"，挪到同一块 RAM 没省到容量
2. spill 字节流比 plasma 内对象更大（有 meta），更占空间
3. tmpfs 占的是 RAM，spill 到 tmpfs = 把"溢出"和"主存"加一起算预算

正确的部署模式：

```
plasma  =  /dev/shm  (tmpfs / RAM)     快、贵、小（10-50 GB）
spill   =  本地 NVMe SSD 目录          慢、便宜、大（数百 GB-TB）
```

大集群可把 spill 配到分布式存储：

```python
"type": "smart_open",
"params": {"uri": "s3://my-bucket/ray-spill/"}
```

#### 本案为什么尤其慢 —— plasma 和 spill 互相抢同一块磁盘

| 路径 | 应该是 | 你的实际 | 后果 |
|---|---|---|---|
| `plasma_directory` | /dev/shm (RAM) | /tmp (overlay 磁盘) | mmap 命中可能 cache miss |
| spill 目录 | /tmp 或独立 SSD | /tmp (**同一块** overlay 磁盘) | spill 写也是磁盘 IO |

正常节点：

```
plasma (RAM)  ←──►  spill (磁盘)
~ns 命中           ~ms spill/restore
```

本案节点：

```
plasma (磁盘 page cache)  ←──►  spill (同一块磁盘)
看似 ns 命中               真磁盘 IO
但页可能被 evict           且和 plasma 争 IO 带宽
```

**两者在同一块 overlay 磁盘上互相 IO 抢带宽**，这就是 `Spilled 9.4 GB/s vs Restored 2.1 GB/s` 严重失衡的真正原因：
- Spill 是顺序写 → 被 page cache 吸收 → 看着快
- Restore 是随机读 → 必须真访问磁盘 → 慢 4.5×

### 9c.5 本附录核心结论

> 1. **`/dev/shm` 是路径惯例，tmpfs 是内核实现**：Ray 选 `/dev/shm` 是因为发行版默认在那挂 tmpfs。
> 2. **tmpfs 是"没有 backing store 的特殊 fs"**：inode 的页表直接指向 RAM 页，没有磁盘 block 概念，写就是写内存。
> 3. **ext4/overlay 上 mmap 看似是内存，本质是"带 page cache 的磁盘 IO"**：clean 页可被 evict，再访问触发 page fault → 磁盘 IO；cgroup 不计这块内存导致 K8s 看不到压力。
> 4. **plasma 和 spill 是两套独立目录**：plasma 在 tmpfs 上的"满"由 Ray 应用层主动 spill 到磁盘目录处理，不依赖 kernel evict —— 这正是 plasma 必须在 tmpfs 上的核心原因（所有数据移动都是显式的）。
> 5. **32GB 报错而 10GB 不报错**：源码用严格 `>` 与 `REQUIRE_SHM_SIZE_THRESHOLD = 10**10` 比较，10GB 恰好"等于不大于"绕过 raise，32GB 严格超过触发报错。
> 6. **本案性能差的真根因**：plasma 被 fallback 到 /tmp，与 spill 目录共用同一块 overlay 磁盘互相抢 IO；且 plasma 内存压力被 cgroup "藏起来"，host 雪崩时整批 pod 集体 SIGTERM。

---

## 十、源码级深度解析：GrantScheduledLeasesToWorkers 与三层内存管控机制

> 本节是对前述第五节"根因"的代码级深度展开，完整追踪从 core_worker 发出 lease 请求到 raylet 拒绝 lease 的全链路，并详解三层独立的内存管控机制如何叠加导致 PENDING_NODE_ASSIGNMENT。

### 10.1 前置澄清：LocalLeaseManager 运行在执行节点，不是调用端节点

**`LocalLeaseManager` 是目标执行节点 raylet 的组件，不是 task 调用端（driver）节点的组件。** 完整的 lease 请求流转如下：

```
Driver 节点 A (core_worker)                     执行节点 B (raylet)
━━━━━━━━━━━━━━━━━━━━━━━                        ━━━━━━━━━━━━━━━━━━━━

① SubmitTask()
   ↓
② ResolveDependencies()  → 参数对象引用就绪
   ↓
③ TaskManager: 状态 → PENDING_NODE_ASSIGNMENT
   ↓
④ LeasePolicy.GetBestNodeForLease()
   ├─ LocalityAwareLeasePolicy: 找到参数对象最多的节点（可能不是本地！）
   │   源码: src/ray/core_worker/lease_policy.cc:24-88
   │   逻辑: 遍历 task 的所有 dependency object，统计每个节点持有的字节数
   │         → 选持有最多字节的目标节点
   └─ LocalLeasePolicy: 始终返回本地节点
   ↓
⑤ core_worker 发送 RequestWorkerLease RPC
   → 发到步骤④选出的节点 B 的 raylet
   源码: src/ray/core_worker/task_submission/normal_task_submitter.cc:328

                                                ⑥ 节点 B 的 raylet 收到请求
                                                   → ClusterLeaseManager.QueueAndScheduleLease()
                                                   → ScheduleAndGrantLeases()
                                                   源码: src/ray/raylet/scheduling/cluster_lease_manager.cc:47-67

                                                ⑦ 如果节点 B 选中自己（spillback_to == self_node_id_）
                                                   → LocalLeaseManager.QueueAndScheduleLease()
                                                   源码: cluster_lease_manager.cc:424-427
                                                   → WaitForLeaseArgsRequests()
                                                   → GrantScheduledLeasesToWorkers()

                                                ⑧ 如果节点 B 选中远程节点 C
                                                   → Reply retry_at_raylet_address=C
                                                   → core_worker 重新向 C 发请求
                                                   源码: normal_task_submitter.cc:434-444
```

**关键点**：

1. **core_worker 发请求时已经做了初步节点选择**：`LocalityAwareLeasePolicy.GetBestNodeForLease()` 基于局部性感知选择——哪个节点上有最多的 task 参数对象字节，就选哪个。这个节点很可能不是 driver 所在节点。

2. **raylet 收到请求后还可以重新选节点**：`ClusterLeaseManager` 再次调用 `GetBestSchedulableNode()` 选最终执行节点。如果自己资源不够，可以 spillback 到别的节点。

3. **`LocalLeaseManager` 中所有操作都作用于执行节点本地资源**：Plasma store、Pull Manager 配额、pinned arguments 内存，全是执行节点本地的。

### 10.2 GrantScheduledLeasesToWorkers 完整逻辑解析

源码：`src/ray/raylet/scheduling/local_lease_manager.cc:136-437`

#### 10.2.1 调度入口与整体流程

```
QueueAndScheduleLease(work)            // 第79行
  ├─ WaitForLeaseArgsRequests(work)    // 第95行：等参数就绪
  │   ├─ 参数就绪 → leases_to_grant_[scheduling_key]
  │   └─ 参数未就绪 → waiting_lease_queue_（等待 pull manager 拉取）
  └─ ScheduleAndGrantLeases()          // 第96行
      ├─ ① GrantScheduledLeasesToWorkers()  // 第127行：先尝试授予
      └─ ② SpillWaitingLeases()             // 第133行：再尝试溢出等待中的 lease
```

#### 10.2.2 GrantScheduledLeasesToWorkers 内部逻辑

对 `leases_to_grant_` 队列中的每个 scheduling class，逐个 lease 检查：

**阶段 1：Fair Scheduling（公平调度）** —— 第142~261行

**目的**：防止某类任务独占 CPU，避免 head-of-line 阻塞和生产者-消费者管道饿死。

**触发条件**：`leases_to_grant_` 队列中所有需要 CPU 的 lease 总 CPU 需求超过本节点 CPU 总量。

```cpp
// 第194-211行：计算队列中总 CPU 需求
double total_cpu_requests_ = 0.0;
size_t num_classes_with_cpu = 0;
for (const auto &[_, cur_dispatch_queue] : leases_to_grant_) {
    const auto &work = cur_dispatch_queue.front();
    auto cpu_request_ = lease_spec.GetRequiredResources()
        .Get(scheduling::ResourceID::CPU()).Double();
    if (cpu_request_ > 0) {
        num_classes_with_cpu++;
        total_cpu_requests_ += cur_dispatch_queue.size() * cpu_request_;
    }
}

// 第220-260行：公平调度判断
if (sched_cls_desc.resource_set.Get(scheduling::ResourceID::CPU()).Double() > 0 &&
    total_cpu_requests_ > total_cpus) {
    size_t fair_share = total_cpu_granted_leases / num_classes_with_cpu;
    if (sched_cls_info.granted_leases.size() > fair_share) {
        // 跳过此 scheduling class，让其他类先授予
        continue;
    }
}
```

**例子**（代码注释原文）：3 CPU，2 个 scheduling class `f` 和 `g`，各 4 个 lease：
- 初始全空，先授予 3 个 `f`
- 1 个 `f` 完成，剩余 2 个 `f` granted → fair_share = 2/2 = 1
- `f` 已授予 2 个 > fair_share 1 → 跳过 `f`，转而授予 `g`

**阶段 2：Scheduling Class Cap** —— 第263~313行

```cpp
if (sched_cls_cap_enabled_ &&
    sched_cls_info.granted_leases.size() >= sched_cls_info.capacity) {
    // 指数退避等待
    int64_t wait_time = sched_cls_cap_interval_ms_ * (1L << exp);
    // 尝试 spillback
    bool did_spill = TrySpillback(work, is_infeasible);
    if (did_spill) {
        work_it = leases_to_grant_queue.erase(work_it);
        continue;
    }
    break;
}
```

**目的**：限制同一类型任务同时授予的 lease 数量，避免嵌套任务场景下启动过多 worker 进程。超出 cap 时采用指数退避，并尝试 `TrySpillback` 将任务溢出到其他节点。

**阶段 3：⭐ Pin Lease Args 内存检查** —— 第315~349行

```cpp
bool args_missing = false;
bool success = PinLeaseArgsIfMemoryAvailable(spec, &args_missing);
if (!success) {
    if (args_missing) {
        // 参数被 evict → 移到 waiting_lease_queue_ 头部（优先重新拉取）
        auto it = waiting_lease_queue_.insert(waiting_lease_queue_.begin(),
                                              std::move(*work_it));
        cluster_resource_scheduler_.GetLocalResourceManager().MaybeMarkFootprintAsBusy(
            WorkFootprint::PULLING_TASK_ARGUMENTS);
        work_it = leases_to_grant_queue.erase(work_it);
    } else {
        // ⭐ 关键分支：pin 内存超限 → 留在队列等待，不 spillback
        work->SetStateWaiting(
            internal::UnscheduledWorkCause::WAITING_FOR_AVAILABLE_PLASMA_MEMORY);
        work_it++;
    }
    continue;
}
```

**注意**：当 pin 内存超限时，lease 只是标记为 `WAITING_FOR_AVAILABLE_PLASMA_MEMORY`，**没有尝试 spillback**。只有 scheduling class cap 和资源不足时才调用 `TrySpillback`。这是一个设计上的不足——详见 10.5 节分析。

**阶段 4：本地资源可调度性检查** —— 第354~405行

```cpp
auto allocated_instances = std::make_shared<TaskResourceInstances>();
bool schedulable =
    !cluster_resource_scheduler_.GetLocalResourceManager().IsLocalNodeDraining() &&
    cluster_resource_scheduler_.GetLocalResourceManager()
        .AllocateLocalTaskResources(spec.GetRequiredResources().GetResourceMap(),
                                    allocated_instances);
if (!schedulable) {
    ReleaseLeaseArgs(lease_id);
    bool did_spill = TrySpillback(work, is_infeasible);
    if (!did_spill) {
        work->SetStateWaiting(
            internal::UnscheduledWorkCause::WAITING_FOR_RESOURCES_AVAILABLE);
        break;
    }
    work_it = leases_to_grant_queue.erase(work_it);
} else {
    // ✅ 所有检查通过，Pop worker 执行
    sched_cls_info.granted_leases.insert(lease_id);
    work->allocated_instances_ = allocated_instances;
    work->SetStateWaitingForWorker();
    worker_pool_.PopWorker(spec, PoppedWorkerHandler...);
}
```

#### 10.2.3 PinLeaseArgsIfMemoryAvailable 详解

源码：`src/ray/raylet/scheduling/local_lease_manager.cc:760-816`

```cpp
bool LocalLeaseManager::PinLeaseArgsIfMemoryAvailable(
    const LeaseSpecification &lease_spec, bool *args_missing) {
  std::vector<std::unique_ptr<RayObject>> args;
  const auto &deps = lease_spec.GetDependencyIds();
  if (!deps.empty()) {
    // 步骤 1：从本地 Plasma store 获取参数对象引用
    // get_lease_arguments_ 绑定的是 NodeManager::GetObjectsFromPlasma()
    // 源码: src/ray/raylet/main.cc:959-962
    if (!get_lease_arguments_(deps, &args)) {
      *args_missing = true;   // Plasma store Get 失败
      return false;
    }
    // 步骤 2：检查参数是否被 evict
    for (size_t i = 0; i < deps.size(); i++) {
      if (args[i] == nullptr) {
        *args_missing = true;  // 参数已被 evict
        return false;
      }
    }
  }

  // 步骤 3：Pin 参数（持有 Plasma store 中的对象引用，防止 evict）
  *args_missing = false;
  size_t lease_arg_bytes = 0;
  for (auto &arg : args) {
    lease_arg_bytes += arg->GetSize();
  }
  PinLeaseArgs(lease_spec, std::move(args));

  // 步骤 4：内存阈值检查
  if (max_pinned_lease_arguments_bytes_ == 0) {
    return true;  // 未配置上限：直接通过
  }
  if (lease_arg_bytes > max_pinned_lease_arguments_bytes_) {
    // 单个 task 的 input 就超过总预算 → 警告但仍放行
  } else if (pinned_lease_arguments_bytes_ > max_pinned_lease_arguments_bytes_) {
    // ⭐ 累计 pinned 超过阈值 → 拒绝（本案命中点）
    ReleaseLeaseArgs(lease_spec.LeaseId());
    return false;
  }
  return true;
}
```

**Pin 操作的本质**：`get_lease_arguments_` 绑定的是 `NodeManager::GetObjectsFromPlasma()`（`main.cc:959-962`），它通过 `store_client_->Get()` 从**本地 Plasma store** 获取对象。`PinLeaseArgs` 保存的是 `unique_ptr<RayObject>`——持有本地 Plasma store 中对象缓冲区的引用。只要这个引用存在，Plasma 就不会回收（evict）该对象。

**所以 Pin 操作发生在执行节点的本地 Plasma store 上，不是去远程节点 pin object。**

#### 10.2.4 PinLeaseArgs 的引用计数机制

源码：`src/ray/raylet/scheduling/local_lease_manager.cc:818-840`

```cpp
void LocalLeaseManager::PinLeaseArgs(const LeaseSpecification &lease_spec,
                                     std::vector<std::unique_ptr<RayObject>> args) {
  const auto &deps = lease_spec.GetDependencyIds();
  auto executed_lease_inserted =
      granted_lease_args_.emplace(lease_spec.LeaseId(), deps).second;
  if (executed_lease_inserted) {
    for (size_t i = 0; i < deps.size(); i++) {
      auto [it, pinned_lease_inserted] =
          pinned_lease_arguments_.emplace(deps[i], std::make_pair(std::move(args[i]), 0));
      if (pinned_lease_inserted) {
        // 第一次有 lease 需要这个参数 → 计入 pinned 内存
        pinned_lease_arguments_bytes_ += it->second.first->GetSize();
      }
      it->second.second++;  // 引用计数 +1
    }
  }
}
```

`pinned_lease_arguments_` 是 `map<ObjectID, pair<unique_ptr<RayObject>, size_t>>`，第二个字段是引用计数。当多个 lease 依赖同一个 object 时，只计一次大小，但引用计数递增。当 `ReleaseLeaseArgs` 时，引用计数归零才真正释放内存计数。

### 10.3 Pull Manager 配额机制—— "needed" 的来源

源码：`src/ray/object_manager/pull_manager.h`、`src/ray/object_manager/pull_manager.cc`

#### 10.3.1 配额计算

```cpp
// pull_manager.h:465-469
int64_t num_bytes_being_pulled_ = 0;   // 正在拉取的对象总大小
int64_t num_bytes_available_;           // plasma store 当前可用字节数

// pull_manager.cc:224-228
int64_t PullManager::RemainingQuota() {
    // plasma 把 pinned bytes 也计为 used
    int64_t bytes_left_to_pull = num_bytes_being_pulled_ - pinned_objects_size_;
    return num_bytes_available_ - bytes_left_to_pull;
}

bool PullManager::OverQuota() { return RemainingQuota() < 0L; }
```

**`num_bytes_available_`** = plasma store 总容量 - 当前已用 = `ray memory --stats-only` 中 `(100% - %full) × capacity`。

**"X% needed"** = 当前等待被拉取的对象总大小 / plasma 容量。这些 pull 请求因配额已满（`OverQuota`）被排队，无法执行。

#### 10.3.2 三级优先级队列

Pull Manager 的请求分三个优先级队列：

| 优先级 | 队列 | 用途 | 特殊权限 |
|--------|------|------|----------|
| **最高** | `get_request_bundles_` | `ray.get()` 请求 | 可抢占低优先级的配额，无条件激活 |
| **中** | `wait_request_bundles_` | `ray.wait()` 请求 | 尊重配额限制 |
| **最低** | `task_argument_bundles_` | 任务参数拉取 | 尊重配额限制，最先被 deactivate |

源码：`pull_manager.cc:232-299`（`UpdatePullsBasedOnAvailableMemory`）

```cpp
void PullManager::UpdatePullsBasedOnAvailableMemory(int64_t num_bytes_available) {
  // 1. get 请求最高优先级，无条件激活，可抢占 task args 和 wait 的配额
  while (get_requests_remaining) {
    DeactivateUntilMarginAvailable("task args request", task_argument_bundles_, ...);
    DeactivateUntilMarginAvailable("wait request", wait_request_bundles_, ...);
    get_requests_remaining = ActivateNextBundlePullRequest(
        get_request_bundles_, /*respect_quota=*/false, &objects_to_pull);
  }

  // 2. wait 请求中等优先级，可抢占 task args 的配额
  while (wait_requests_remaining) {
    DeactivateUntilMarginAvailable("task args request", task_argument_bundles_, ...);
    wait_requests_remaining = ActivateNextBundlePullRequest(
        wait_request_bundles_, /*respect_quota=*/true, &objects_to_pull);
  }

  // 3. task args 最低优先级，尊重配额
  while (ActivateNextBundlePullRequest(
      task_argument_bundles_, /*respect_quota=*/true, &objects_to_pull)) {}

  // 4. 最后保证每个队列至少有 1 个 active bundle
  DeactivateUntilMarginAvailable("task args request", task_argument_bundles_,
                                 /*retain_min=*/1, /*quota_margin=*/0L, ...);
}
```

**当 `OverQuota` 时**：task arg 的 pull 请求被 deactivate（不再发 pull 请求），`HasPullsQueued()` 返回 true。

#### 10.3.3 Bundle Pull Request 的三种状态

```
active:    正在被拉取（已发 pull 请求）
inactive:  可拉取但被暂停（OverQuota，等配额释放）
unpullable: 不可拉取（对象大小未知或 pending object creation）

状态转换：
  inactive → active:    配额释放时激活
  active → inactive:    被 get 请求抢占配额时 deactivate
  inactive → unpullable: 对象丢失/pending reconstruction
  unpullable → inactive: 所有对象大小已知且不 pending
```

#### 10.3.4 HasPullsQueued 传播到集群资源视图

```cpp
// local_resource_manager.cc:380-382
if (get_pull_manager_at_capacity_ != nullptr) {
    resources.object_pulls_queued = get_pull_manager_at_capacity_();
    // get_pull_manager_at_capacity_ 绑定的是:
    // object_manager->PullManagerHasPullsQueued()
    // 源码: src/ray/raylet/main.cc:896
}
```

`object_pulls_queued` 随资源报告广播给集群中所有节点。

### 10.4 三层内存管控机制的叠加分析

Ray 的 plasma 内存管控由三层独立机制组成，它们分别作用在不同阶段：

```
┌─────────────────────────────────────────────────────────────────────┐
│  层级 1: ClusterLeaseManager 调度选节点                              │
│  ──────────────────────────────────────                             │
│  机制: object_pulls_queued 标记影响跨节点调度                         │
│  源码: cluster_resource_data.cc:85-91 (IsAvailable)                 │
│        cluster_resource_scheduler.cc:119-128 (IsSchedulable)        │
│  时机: ClusterLeaseManager 选节点时                                   │
│  效果: 如果远程节点 object_pulls_queued=true + requires_object_store  │
│        _memory=true → 不选该节点                                     │
│  例外: 本节点对自己跳过此检查 (ignore_object_store_memory_requirement  │
│        = true when node_id == local_node_id_)                       │
│  传播: LocalResourceManager → RaySyncer → GCS → 所有 raylet          │
├─────────────────────────────────────────────────────────────────────┤
│  层级 2: Pull Manager 配额机制                                       │
│  ──────────────────────────────                                     │
│  机制: OverQuota() 时 task arg pull 请求被 deactivate                │
│  源码: pull_manager.cc:224-230 (RemainingQuota / OverQuota)         │
│        pull_manager.cc:232-299 (UpdatePullsBasedOnAvailableMemory)  │
│  时机: lease 参数需要从远程拉取到本节点时                               │
│  效果: task arg 的 pull 被排队，LeaseDependenciesBlocked 返回 true   │
│  关键: get 请求可抢占 task args 的配额                                 │
│  表现: "Object fetches queued, waiting for available memory."        │
├─────────────────────────────────────────────────────────────────────┤
│  层级 3: LocalLeaseManager Pin 内存预算                               │
│  ──────────────────────────────────                                 │
│  机制: pinned_lease_arguments_bytes_ > max_pinned_lease_arguments_   │
│        bytes_ → 拒绝授予 lease                                        │
│  源码: local_lease_manager.cc:760-816                                │
│        (PinLeaseArgsIfMemoryAvailable)                               │
│  时机: lease 已在 leases_to_grant_ 队列，即将授予 worker 时            │
│  效果: lease 被标记 WAITING_FOR_AVAILABLE_PLASMA_MEMORY，不 spillback │
│  阈值: max_pinned_lease_arguments_bytes_ = object_store_capacity     │
│        × max_task_args_memory_fraction (默认 0.7)                    │
│  特点: 参数已在本地 plasma → pin 只是从 plasma store 拿引用           │
└─────────────────────────────────────────────────────────────────────┘
```

#### 三层机制的交互关系

```
Task 提交
  │
  ├─ ClusterLeaseManager 选节点
  │   └─ 层级 1: 检查远程节点 object_pulls_queued（本节点跳过）
  │       → 选到本节点
  │
  ├─ LocalLeaseManager.WaitForLeaseArgsRequests()
  │   └─ 参数不在本地 → waiting_lease_queue_ → pull manager 拉取
  │       └─ 层级 2: Pull Manager OverQuota → pull 被排队
  │           → LeaseDependenciesBlocked = true
  │           → SpillWaitingLeases 尝试 spillback（requires_object_store_memory=true）
  │
  ├─ 参数已在本地 → leases_to_grant_ → GrantScheduledLeasesToWorkers()
  │   └─ 层级 3: PinLeaseArgsIfMemoryAvailable → pinned 超限 → 拒绝
  │       → WAITING_FOR_AVAILABLE_PLASMA_MEMORY
  │       → ❌ 不尝试 spillback（只能等其他 lease 完成）
  │
  └─ 全部通过 → PopWorker → task 执行
```

### 10.5 `object_pulls_queued` 影响跨节点调度的代码链路

#### 10.5.1 远程节点视角

当其他节点为本节点上的 lease 做 spillback 决策时：

```cpp
// cluster_resource_data.cc:85-91
bool NodeResources::IsAvailable(const ResourceRequest &resource_request,
                                bool ignore_pull_manager_at_capacity) const {
    if (!ignore_pull_manager_at_capacity &&
        resource_request.RequiresObjectStoreMemory() &&
        object_pulls_queued) {
        return false;  // ← 这个节点的 pull manager 满了，不选它
    }
    // ... 资源标签和数值检查 ...
}
```

#### 10.5.2 本节点对自己的豁免

```cpp
// cluster_resource_scheduler.cc:119-128
bool ClusterResourceScheduler::IsSchedulable(
    const ResourceRequest &resource_request,
    scheduling::NodeID node_id) const {
    return cluster_resource_manager_->HasAvailableResources(
               node_id, resource_request,
               /*ignore_object_store_memory_requirement*/
               node_id == local_node_id_)  // ← 本节点忽略 pull manager 满载!
           && NodeAvailable(node_id);
}
```

**这意味着**：集群调度器在初始选节点时不会因为 `object_pulls_queued` 跳过本节点——task 仍然会被分配到本节点。但 `SpillWaitingLeases` 做二次 spillback 时会传 `requires_object_store_memory=true`，此时会检查远程节点是否有空间。

#### 10.5.3 SpillWaitingLeases 中的检查

```cpp
// local_lease_manager.cc:473-479
scheduling_node_id = cluster_resource_scheduler_.GetBestSchedulableNode(
    lease_spec,
    /*preferred_node_id*/ self_node_id_.Binary(),
    /*exclude_local_node*/ lease_dependencies_blocked,  // ← blocked 时排除自己
    /*requires_object_store_memory*/ true,               // ← 考虑 object store 内存!
    &is_infeasible);
```

当 lease 的依赖被 blocked（pull manager 不活跃）时，`exclude_local_node=true`，且 `requires_object_store_memory=true`——这确保 spillback 到的远程节点不仅要有 CPU/内存资源，还不能有 `object_pulls_queued`。

### 10.6 PENDING_NODE_ASSIGNMENT 的精确代码路径

```
core_worker 提交 task
    │
    ├─ TaskManager.AddPendingTask()
    │   → 状态 = PENDING_ARGS_AVAIL
    │   源码: src/ray/core_worker/task_manager.cc:238
    │
    ├─ 参数就绪后，core_worker 发送 RequestWorkerLease RPC
    │   → TaskManager.MarkTaskWaitingForExecution() 之前
    │   → 状态 = PENDING_NODE_ASSIGNMENT
    │   源码: src/ray/core_worker/task_manager.cc:1682
    │
    └─ raylet 收到 RequestWorkerLease
        │
        ├─ ClusterLeaseManager.SchedulePendingLeases()
        │   → GetBestSchedulableNode(requires_object_store_memory=false)
        │   → 选到本节点 → LocalLeaseManager.QueueAndScheduleLease()
        │
        ├─ WaitForLeaseArgsRequests()
        │   ├─ 参数就绪   → leases_to_grant_
        │   └─ 参数未就绪 → waiting_lease_queue_（pull manager 拉取）
        │
        ├─ GrantScheduledLeasesToWorkers()
        │   ├─ Fair scheduling 检查
        │   ├─ Scheduling class cap → TrySpillback
        │   ├─ ⭐ PinLeaseArgsIfMemoryAvailable
        │   │   ├─ args_missing (参数被 evict)
        │   │   │   → 移回 waiting_lease_queue_
        │   │   │   → task 仍显示 PENDING_NODE_ASSIGNMENT
        │   │   │
        │   │   └─ pinned 超限 (pinned_bytes > max)
        │   │       → WAITING_FOR_AVAILABLE_PLASMA_MEMORY
        │   │       → task 仍显示 PENDING_NODE_ASSIGNMENT
        │   │       → ❌ 不尝试 spillback
        │   │
        │   ├─ 资源不足 → TrySpillback / 留本节点等待
        │   └─ 全部通过 → PopWorker → MarkTaskWaitingForExecution
        │
        └─ SpillWaitingLeases()
            → 对 waiting_lease_queue_ 中的 lease 尝试 spillback
            → requires_object_store_memory=true
            → 如果远程节点有空间 → lease 被重新调度
```

**关键点**：在 core_worker 看来，只要 lease 还没真正授予 worker（还没到 `MarkTaskWaitingForExecution`），task 的状态就停留在 `PENDING_NODE_ASSIGNMENT`。raylet 内部的所有卡点（参数 pull 排队、pin 内存超限、资源不足）在 core_worker 端都表现为同一个状态。

#### Task 状态转换完整路径

```
NIL → PENDING_ARGS_AVAIL → PENDING_NODE_ASSIGNMENT → SUBMITTED_TO_WORKER → RUNNING
                            ↑                        ↑
                            │                        │
                     各种 raylet 内部卡点         lease 成功授予 worker
                     都停留在这里
```

### 10.7 修正与补充：关于 "needed" 和 output 预算

#### "needed" 指的是输入参数，不是 output

前文第五节和排查中提到的 "X% needed" 含义需要精确化：

1. **`pinned_lease_arguments_bytes_` 限制**检查的是 **task 输入参数**占用的内存，不是 output。它的目的是防止太多 task 的输入参数 pin 住 plasma，导致没有空间存放 output。

2. **Pull Manager 的 "needed"** 也是指**输入参数**需要拉取到本节点占用的空间，不是 output 预算。

3. **Ray 目前并没有对 task output 做内存预算预扣**。代码中有明确 TODO：

```cpp
// local_resource_manager.cc:223-225
// TODO(swang): We should also subtract object store memory if the task has
// arguments. Right now we do not modify object_pulls_queued in case of
// performance regressions in spillback.
```

所以 output 的空间问题是在 worker 实际执行写 plasma 时才暴露的——如果写不进去就是 `ObjectStoreFullError`。

#### Pin 内存超限时不 spillback 的设计缺陷

| 场景 | Spill 方式 | 代码位置 |
|------|-----------|---------|
| Scheduling class cap 超限 | `TrySpillback` | `local_lease_manager.cc:305` |
| 本地资源不足 | `TrySpillback` | `local_lease_manager.cc:364` |
| 依赖被 blocked（pull 不活跃） | `SpillWaitingLeases` | `local_lease_manager.cc:474`（`requires_object_store_memory=true`） |
| **Pin 内存超限** | ❌ **不会 spill** | 只标记 WAITING，等别的 lease 完成 |

**当 `pinned_lease_arguments_bytes_ > max_pinned_lease_arguments_bytes_` 时**，lease 被卡住但不尝试 spillback。它只能等其他已授予的 lease 完成、释放 pin 的参数后才能继续。如果所有节点都面临同样的问题，就会形成全局的死锁式等待。

**改进方向**：在 `PinLeaseArgsIfMemoryAvailable` 失败时增加 `TrySpillback`，类似 scheduling class cap 超限时的处理。

### 10.8 内存检查和 Spill 的完整时序

```
ScheduleAndGrantLeases() 被调用
    │
    ├─ ① GrantScheduledLeasesToWorkers()
    │   │
    │   ├─ 对 leases_to_grant_ 中每个 scheduling class:
    │   │   ├─ Fair scheduling 检查
    │   │   │   └─ 超限 → skip 此 class
    │   │   │
    │   │   └─ 对队列中每个 lease:
    │   │       ├─ Scheduling class cap
    │   │       │   └─ 超限 → TrySpillback（spill 到其他节点）
    │   │       │
    │   │       ├─ ⭐ PinLeaseArgsIfMemoryAvailable
    │   │       │   ├─ args_missing → waiting_lease_queue_（头部插入）
    │   │       │   └─ pinned 超限 → WAITING_FOR_AVAILABLE_PLASMA_MEMORY
    │   │       │                    （不 spillback，等别的 lease 完成）
    │   │       │
    │   │       ├─ AllocateLocalTaskResources
    │   │       │   └─ 失败 → TrySpillback
    │   │       │             └─ 也失败 → WAITING_FOR_RESOURCES_AVAILABLE
    │   │       │
    │   │       └─ 成功 → PopWorker 执行
    │   │
    │   └─ 清理空队列或 infeasible 的 scheduling class
    │
    └─ ② SpillWaitingLeases()
        │
        └─ 对 waiting_lease_queue_ 从尾部开始:
            ├─ LeaseDependenciesBlocked() 检查
            │   └─ 如果 pull manager 不活跃 → blocked = true
            │
            ├─ GetBestSchedulableNode
            │   ├─ exclude_local_node = blocked
            │   └─ requires_object_store_memory = true
            │       → 远程节点必须 object_pulls_queued = false
            │
            ├─ 找到远程节点 → Spillback，移出等待队列
            └─ 没找到 → 留在本地等待
```

---

## 十一、相关源码索引

| 路径 | 行号 | 作用 |
|---|---|---|
| `src/ray/protobuf/common.proto` | 884–921 | TaskStatus 枚举定义及子状态注释 |
| `src/ray/raylet/scheduling/internal.h` | 36–52 | `UnscheduledWorkCause` 枚举（包含 `WAITING_FOR_AVAILABLE_PLASMA_MEMORY`） |
| `src/ray/raylet/scheduling/local_lease_manager.cc` | 79–97 | `QueueAndScheduleLease` 入口：等参数就绪 + 调度 |
| `src/ray/raylet/scheduling/local_lease_manager.cc` | 99–124 | `WaitForLeaseArgsRequests`：参数就绪→grants，未就绪→waiting |
| `src/ray/raylet/scheduling/local_lease_manager.cc` | 126–134 | `ScheduleAndGrantLeases`：先 Grant 后 Spill |
| `src/ray/raylet/scheduling/local_lease_manager.cc` | 136–437 | `GrantScheduledLeasesToWorkers` 主循环（公平调度→cap→pin→资源→worker） |
| `src/ray/raylet/scheduling/local_lease_manager.cc` | 142–261 | Fair scheduling：CPU 公平性检查（fair_share 计算） |
| `src/ray/raylet/scheduling/local_lease_manager.cc` | 263–313 | Scheduling class cap：指数退避 + TrySpillback |
| `src/ray/raylet/scheduling/local_lease_manager.cc` | 315–349 | Pin 内存检查：args_missing / pinned 超限 |
| `src/ray/raylet/scheduling/local_lease_manager.cc` | 354–405 | 资源可调度性检查 + PopWorker |
| `src/ray/raylet/scheduling/local_lease_manager.cc` | 439–515 | `SpillWaitingLeases`：尝试溢出等待中 lease（requires_object_store_memory=true） |
| `src/ray/raylet/scheduling/local_lease_manager.cc` | 516–541 | `TrySpillback`：spill 到远程节点 |
| `src/ray/raylet/scheduling/local_lease_manager.cc` | 760–816 | `PinLeaseArgsIfMemoryAvailable`：从 Plasma 获取引用 + 内存阈值检查 |
| `src/ray/raylet/scheduling/local_lease_manager.cc` | 818–840 | `PinLeaseArgs`：引用计数 + pinned_lease_arguments_bytes_ 累加 |
| `src/ray/raylet/scheduling/local_lease_manager.cc` | 842–860 | `ReleaseLeaseArgs`：引用计数 -1，归零时释放内存计数 |
| `src/ray/raylet/scheduling/local_lease_manager.h` | 355–377 | 成员变量：get_lease_arguments_、pinned_lease_arguments_、pinned_lease_arguments_bytes_ |
| `src/ray/raylet/main.cc` | 935–941 | 启动期计算 `max_task_args_memory` = capacity × fraction |
| `src/ray/raylet/main.cc` | 952–964 | 构造 LocalLeaseManager，绑定 get_lease_arguments_ = GetObjectsFromPlasma |
| `src/ray/raylet/main.cc` | 895–896 | 绑定 get_pull_manager_at_capacity_ = PullManagerHasPullsQueued |
| `src/ray/raylet/node_manager.cc` | 2585–2610 | `GetObjectsFromPlasma`：从本地 Plasma store 获取对象引用 |
| `src/ray/raylet/scheduling/local_resource_manager.cc` | 380–382 | object_pulls_queued 上报到资源视图 |
| `src/ray/raylet/scheduling/cluster_lease_manager.cc` | 47–68 | `QueueAndScheduleLease`：集群级调度入口 |
| `src/ray/raylet/scheduling/cluster_lease_manager.cc` | 196–296 | `ScheduleAndGrantLeases`：集群级选节点 + 本地授予 |
| `src/ray/raylet/scheduling/cluster_lease_manager.cc` | 422–461 | `ScheduleOnNode`：本地→LocalLeaseManager，远程→retry_at_raylet_address |
| `src/ray/raylet/scheduling/cluster_resource_scheduler.cc` | 119–129 | `IsSchedulable`：本节点忽略 object_pulls_queued |
| `src/ray/raylet/scheduling/cluster_resource_scheduler.cc` | 228–248 | `GetBestSchedulableNode`：ResourceMapToResourceRequest 构建 requires_object_store_memory |
| `src/ray/raylet/scheduling/cluster_resource_manager.cc` | 240–251 | `HasAvailableResources`：调用 IsAvailable |
| `src/ray/common/scheduling/cluster_resource_data.cc` | 85–104 | `IsAvailable`：object_pulls_queued + requires_object_store_memory 检查 |
| `src/ray/common/scheduling/cluster_resource_data.h` | 39–60 | `ResourceRequest` 类：requires_object_store_memory_ 字段 |
| `src/ray/common/scheduling/cluster_resource_data.h` | 308–342 | `NodeResources` 类：object_pulls_queued、normal_task_resources 字段 |
| `src/ray/object_manager/pull_manager.h` | 50–76 | PullManager 构造函数 |
| `src/ray/object_manager/pull_manager.h` | 221–352 | BundlePullRequestQueue：active/inactive/unpullable 三状态 |
| `src/ray/object_manager/pull_manager.h` | 456–469 | 成员变量：num_bytes_being_pulled_、num_bytes_available_ |
| `src/ray/object_manager/pull_manager.cc` | 104–141 | `ActivateNextBundlePullRequest`：配额检查 + 激活 |
| `src/ray/object_manager/pull_manager.cc` | 224–230 | `RemainingQuota` / `OverQuota`：配额计算 |
| `src/ray/object_manager/pull_manager.cc` | 232–299 | `UpdatePullsBasedOnAvailableMemory`：三级优先级调度 |
| `src/ray/object_manager/object_manager.h` | 114, 282 | `PullManagerHasPullsQueued` → `pull_manager_->HasPullsQueued()` |
| `src/ray/core_worker/lease_policy.cc` | 24–88 | `LocalityAwareLeasePolicy::GetBestNodeForLease`：局部性感知选节点 |
| `src/ray/core_worker/lease_policy.cc` | 90–94 | `LocalLeasePolicy::GetBestNodeForLease`：始终返回本地节点 |
| `src/ray/core_worker/task_submission/normal_task_submitter.cc` | 34–94 | `SubmitTask`：ResolveDependencies + RequestNewWorkerIfNeeded |
| `src/ray/core_worker/task_submission/normal_task_submitter.cc` | 300–445 | `RequestNewWorkerIfNeeded`：发 RequestWorkerLease RPC + 回调处理 |
| `src/ray/core_worker/task_manager.cc` | 238 | `AddPendingTask`：初始状态 PENDING_ARGS_AVAIL |
| `src/ray/core_worker/task_manager.cc` | 1679–1683 | PENDING_ARGS_AVAIL → PENDING_NODE_ASSIGNMENT 状态转换 |
| `src/ray/raylet/lease_dependency_manager.cc` | 355–361 | `LeaseDependenciesBlocked`：检查 pull request 是否 active 或 waiting for metadata |
| `src/ray/common/ray_config_def.h` | 734 | `max_task_args_memory_fraction = 0.7` 默认值 |
| `python/ray/data/_internal/execution/resource_manager.py` | — | Driver 端 `OpResourceAllocator`（用户关掉的那层） |
| `python/ray/_private/utils.py` | 525–578 | `_get_object_store_memory` —— 默认 object store 容量计算 |
| `python/ray/_private/services.py` | 2148–2181 | `determine_plasma_store_config` —— /dev/shm vs /tmp fallback 决策 |
| `python/ray/_private/ray_constants.py` | 120–134 | 默认上限 200 GB / 比例 0.3 / SHM 保底 10 GB |
| `python/ray/_common/utils.py` | 266–310 | `get_system_memory` —— 取 min(cgroup, psutil) 作为可用内存 |
| `python/ray/_private/external_storage.py` | — | `RAY_DEFAULT_OBJECT_SPILLING_CONFIG` —— 默认 spill 到 `/tmp/ray/.../ray_spilled_objects_xxx` |
| Linux kernel `mm/shmem.c` | — | `shmem_get_folio_gfp` —— tmpfs 直接分配 RAM 页（无 backing store） |
| Linux kernel `fs/ext4/inode.c` | — | `ext4_readpage` —— ext4 走 BIO 从磁盘读（有 backing store） |

---

## 十二、一句话结论

> **CPU 闲、worker 都在、却 8000+ task PENDING_NODE_ASSIGNMENT** —— raylet 因 `pinned_lease_arguments_bytes_ > object_store_capacity × 0.7` **主动拒绝 lease**，对外报 `PENDING_NODE_ASSIGNMENT`、内部子状态 `WAITING_FOR_AVAILABLE_PLASMA_MEMORY`。Driver 端关闭 `OpResourceAllocator` 不影响 raylet 的 C++ 硬约束，反而让 driver 一次性 submit 更多 task，加剧表象。
>
> **诊断信号一行话**：`ray memory --stats-only` 输出里出现 `Object fetches queued, waiting for available memory.` + `% full + % needed > 70%`。
>
> **更深层的物理原因**：worker 容器 `/dev/shm` 默认只有 64M（K8s 不挂 tmpfs emptyDir 的默认值），Ray 自动 fallback 把 plasma mmap 到 `/tmp`（overlay 磁盘）。plasma 本应在 tmpfs（RAM）上提供"页永不消失"的内存语义，现在退化成"磁盘上的 mmap 文件 + page cache 加速"，与 spill 目录共用同一块磁盘互相抢 IO，且 plasma 内存压力被 cgroup 计费机制"藏起来"（page cache 不计 pod working_set），导致 host 雪崩时整批 worker pod 集体 SIGTERM。
>
> **根治路径**：让 KML 平台在 worker pod spec 给 `/dev/shm` 挂 `emptyDir: medium: Memory, sizeLimit: 30Gi`，让 plasma 走真正的 tmpfs，所有问题（pin 池压力、spill/restore 失衡、worker SIGTERM）随之缓解。
