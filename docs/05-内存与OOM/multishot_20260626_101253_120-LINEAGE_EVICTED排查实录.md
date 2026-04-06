# OOM 导致 Object 丢失 + Lineage 驱逐 → 不可恢复完整排查实录

## 文档信息

| 项目 | 内容 |
|------|------|
| 日期 | 2026-06-26 |
| 作业 ID | multishot_20260626_101253_120 |
| 集群 | kml-hb2az1-l3-2 / lmserving |
| Head 节点 | lmserv-proj-10121-svc-219283-ray-head-0 |
| Ray 版本 | 2.55.1 |
| 关联文档 | [Object生命周期与恢复深度分析](../01-架构与原理/Object生命周期与恢复深度分析.md), [Error-Block深度分析](../02-数据流与算子/Error-Block深度分析.md), [ObjectReconstructionFailedError分析](../06-故障排查/ObjectReconstructionFailedError分析.md) |

---

## 目录

1. [问题现象](#1-问题现象)
2. [排查方法与过程](#2-排查方法与过程)
3. [日志证据](#3-日志证据)
4. [根因分析](#4-根因分析)
5. [深度机制分析](#5-深度机制分析)
6. [时间线还原](#6-时间线还原)
7. [解决方案与预防措施](#7-解决方案与预防措施)
8. [附录](#8-附录)

---

## 1. 问题现象

### 1.1 用户报错

```
2026-06-26 19:00:32,342 - utils.patch_interleave_dispatch - ERROR -
An exception was raised from a task of operator "FlatMap(ClipMergeMapper)".
[num_errored_blocks=1] Ignoring this exception with remaining max_errored_blocks=9512.

[OBJECT_UNRECONSTRUCTABLE_LINEAGE_EVICTED] The object cannot be reconstructed
because its lineage has been evicted to reduce memory pressure. To prevent this
error, set the environment variable RAY_max_lineage_bytes=<bytes> (default 1GB)
during `ray start`.
```

### 1.2 作业信息

| 项目 | 值 |
|------|-----|
| 作业名 | multishot_20260626_101253_120 |
| 启动时间 | 2026-06-26 10:12:53 |
| 报错时间 | 2026-06-26 19:00:32 |
| 运行时长 | ~9 小时 |
| Pipeline | ReadParquet → Filter → Map(VideoClipProcessMapper) → StreamingRepartition → MapBatches(DistributedQwenVLVideoProcessMapper) → **FlatMap(ClipMergeMapper)** → StreamingRepartition → MapBatches(VideoClipInfoKafkaMapper) → Write |
| 数据规模 | 113M+ 行, FlatMap 阶段 121M+ blocks |
| 并发度 | 8000 CPU tasks (FlatMap), 1000 actors (QwenVLVideoProcessMapper), 500 GPU |
| 集群规模 | ~250 节点, 128GB/节点 |
| 关键 Actor | QwenVLCPUPreprocessActor (~9GB/个, 10+ 并发/节点) |
| 错误总数 | 900 次 OBJECT_UNRECONSTRUCTABLE_LINEAGE_EVICTED |
| patch_interleave_dispatch 错误 | 944 次 (max_errored_blocks 初始 10000, 剩余 9512) |

### 1.3 核心问题

> **是否有节点 lost 导致 object 重建？**

**答案：是的。** 节点因 OOM 被杀，导致 Object 丢失 → Ray 尝试 lineage reconstruction → lineage 已被驱逐 → `OBJECT_UNRECONSTRUCTABLE_LINEAGE_EVICTED`。

---

## 2. 排查方法与过程

### 2.1 连接 Head 节点

通过 KML WebShell 连接到 head 节点：

```
URL: https://kml.corp.kuaishou.com/#/system/terminal?
     clusterName=kml-hb2az1-l3-2&namespace=lmserving&
     pod=lmserv-proj-10121-svc-219283-ray-head-0&mode=shell
```

### 2.2 排查过程

#### Step 1: 确认日志文件存在

```bash
ls /tmp/ray/session_latest/logs/ | grep -E 'gcs_server|raylet'
```

结果：
```
gcs_server.err
gcs_server.out
raylet.err
raylet.out
```

日志文件大小：
- `gcs_server.out`: 8,813,044 行
- `raylet.out`: 1,084,622 行

#### Step 2: 在 GCS 中搜索 lineage evict 记录 — 无结果

```bash
grep -c 'OBJECT_UNRECONSTRUCTABLE\|lineage.*evict\|LINEAGE_EVICTED' \
    /tmp/ray/session_latest/logs/gcs_server.out
```
结果: **0** ← GCS 日志中没有 lineage evict 记录！

```bash
grep -c 'OBJECT_UNRECONSTRUCTABLE\|lineage.*evict\|LINEAGE_EVICTED' \
    /tmp/ray/session_latest/logs/raylet.out
```
结果: **0** ← Head 节点 raylet 也没有！

#### Step 3: 全局搜索 LINEAGE_EVICTED 来源

```bash
grep -rn 'OBJECT_UNRECONSTRUCTABLE\|LINEAGE_EVICTED' \
    /tmp/ray/session_latest/logs/ 2>/dev/null | head -20
```

结果：命中 `job-driver-multishot_*.log` ← **错误在 job driver 日志中**

#### Step 4: 定位目标作业的 driver 日志

```bash
ls /tmp/ray/session_latest/logs/ | grep 'job-driver.*multishot_20260626'
```
结果：
```
job-driver-multishot_20260626_010628_121.log
job-driver-multishot_20260626_011409_121.log
job-driver-multishot_20260626_012436_120.log
job-driver-multishot_20260626_101253_120.log  ← 目标作业
```

#### Step 5: 统计 LINEAGE_EVICTED 错误数量

```bash
grep -c 'OBJECT_UNRECONSTRUCTABLE\|LINEAGE_EVICTED' \
    /tmp/ray/session_latest/logs/job-driver-multishot_20260626_101253_120.log
```
结果: **900**

#### Step 6: 定位报错时间点

```bash
grep -n '2026-06-26 19:00:32' \
    /tmp/ray/session_latest/logs/job-driver-multishot_20260626_101253_120.log
```
结果: 行 **340028**

#### Step 7: 搜索 GCS 中的 OOM 事件

```bash
grep -c 'NODE_OUT_OF_MEMORY' /tmp/ray/session_latest/logs/gcs_server.out
```
结果: **552** 次NODE_OUT_OF_MEMORY

#### Step 8: 搜索报错时间附近的 OOM 事件

```bash
grep -n 'Reporting worker exit' /tmp/ray/session_latest/logs/gcs_server.out | \
    grep '2026-06-26 18:5[5-9]\|2026-06-26 19:00'
```
结果：
```
8734673: [2026-06-26 18:56:29,054] ... 10.17.118.239, exit_type = NODE_OUT_OF_MEMORY
8735091: [2026-06-26 19:00:00,804] ... 10.51.133.76, exit_type = NODE_OUT_OF_MEMORY
```

#### Step 9: 搜索 job driver 中的节点死亡事件

```bash
grep -c 'node.*dead.*unavailable' \
    /tmp/ray/session_latest/logs/job-driver-multishot_20260626_101253_120.log
```
结果: **9** 条 (涉及 5 个不同节点 IP)

#### Step 10: 搜索 raylet 中的 lineage evict

```bash
grep -n 'lineage_drop_ratio\|max_lineage_bytes\|lineage.*evict\|LineageEvict' \
    /tmp/ray/session_latest/logs/raylet.out
```
结果: **无** ← head 节点 raylet 没有 lineage evict 日志

#### Step 11: 验证是否有 "Node failure" 通知和节点标记 DEAD

```bash
grep -c 'Node failure' \
    /tmp/ray/session_latest/logs/job-driver-multishot_20260626_101253_120.log
```
结果: **0** ← 没有 "Node failure" 通知

```bash
grep -c 'transitioned to DEAD\|node.*DEAD\|NODE_DEAD\|MarkDead' \
    /tmp/ray/session_latest/logs/gcs_server.out
```
结果: **0** ← GCS 中没有节点标记 DEAD

### 2.3 排查决策树

```
OBJECT_UNRECONSTRUCTABLE_LINEAGE_EVICTED 报错
│
├─ 在 gcs_server.out 中搜索?
│   └─ 0 条 → GCS 不记录 lineage eviction
│
├─ 在 raylet.out (head) 中搜索?
│   └─ 0 条 → lineage eviction 发生在 Driver 进程内部, 不在 raylet
│
├─ 全局搜索所有日志文件?
│   └─ 命中 job-driver-*.log → 错误来自 job driver 进程
│       │
│       ├─ 确认错误数量: 900 次
│       ├─ 确认报错时间: 19:00:32 (行 340028)
│       ├─ 查看上下文: FlatMap(ClipMergeMapper) 的 ray.get 失败
│       │
│       └─ 追溯根因: 为什么 Object 丢失?
│           │
│           ├─ GCS 中搜索 NODE_OUT_OF_MEMORY?
│           │   └─ 552 次, 18:56 和 19:00 各一次
│           │
│           ├─ Job driver 中搜索 node dead?
│           │   └─ 9 条, 5 个不同节点
│           │
│           ├─ 验证 "Node failure" 通知?
│           │   └─ 0 条 → 没有 "Node failure" 通知
│           │
│           ├─ 验证节点标记 DEAD?
│           │   └─ 0 条 → GCS 中无 transitioned to DEAD
│           │
│           └─ 结论: 节点被 K8s 驱逐 (SIGTERM) → Plasma Store 消失
│              → Object 全部丢失 → lineage 重建失败
│              (Worker 被 OOM kill 本身不释放 Pin, 不是 Object 丢失主因)
```

### 2.4 完整排查命令清单（可复用）

```bash
LOG_DIR=/tmp/ray/session_latest/logs
JOB_LOG=job-driver-multishot_20260626_101253_120.log

# === 1. 确认 Error Block 现象 ===
grep -E "An exception was raised from a task of operator" $LOG_DIR/$JOB_LOG | head -10

# === 2. 统计 LINEAGE_EVICTED 错误数量 ===
grep -c 'OBJECT_UNRECONSTRUCTABLE\|LINEAGE_EVICTED' $LOG_DIR/$JOB_LOG

# === 3. 定位报错时间点 ===
grep -n '2026-06-26 19:00:32' $LOG_DIR/$JOB_LOG

# === 4. 确认 GCS 中 OOM 事件 ===
grep -c 'NODE_OUT_OF_MEMORY' $LOG_DIR/gcs_server.out

# === 5. 按时段统计 OOM ===
grep 'NODE_OUT_OF_MEMORY' $LOG_DIR/gcs_server.out | \
    awk '{print substr($0,1,16)}' | sort | uniq -c | sort -rn | head

# === 6. 搜索报错时间附近的 OOM 事件 ===
grep -n 'Reporting worker exit' $LOG_DIR/gcs_server.out | \
    grep '2026-06-26 18:5[5-9]\|2026-06-26 19:00'

# === 7. 搜索 job driver 中的节点死亡事件 ===
grep -c 'node.*dead.*unavailable' $LOG_DIR/$JOB_LOG

# === 8. 验证是否有 "Node failure" 通知 ===
grep -c 'Node failure' $LOG_DIR/$JOB_LOG

# === 9. 验证是否有节点标记 DEAD ===
grep -c 'transitioned to DEAD\|NODE_DEAD\|MarkDead' $LOG_DIR/gcs_server.out

# === 10. 全局搜索 LINEAGE_EVICTED 来源 ===
grep -rn 'OBJECT_UNRECONSTRUCTABLE\|LINEAGE_EVICTED' $LOG_DIR/ 2>/dev/null | head -20

# === 11. 确认 Lineage 驱逐 ===
grep -E "lineage.*exceeds|Evicted.*lineage|lineage_footprint" $LOG_DIR/$JOB_LOG

# === 12. 确认节点死亡原因 (SIGTERM vs 崩溃) ===
grep -n 'death reason' $LOG_DIR/gcs_server.out | grep '2026-06-26' | head -30
# 结果: 33 个 EXPECTED_TERMINATION (SIGTERM), 0 个 UNEXPECTED_TERMINATION

# === 13. 确认内存压力源 (在 Worker 节点上执行) ===
ps -eo pid,rss,comm --sort=-rss | head -20

# === 14. 确认 cgroup kmem 虚高 ===
cat /sys/fs/cgroup/memory/memory.usage_in_bytes
cat /sys/fs/cgroup/memory/memory.kmem.usage_in_bytes
cat /sys/fs/cgroup/memory/memory.stat | grep -E '^(rss|cache|shmem) '

# === 15. 验证 Ray OOM 计算公式 ===
python3 -c "
usage = int(open('/sys/fs/cgroup/memory/memory.usage_in_bytes').read())
stat = dict(l.split() for l in open('/sys/fs/cgroup/memory/memory.stat').read().strip().split('\n') if len(l.split())==2)
inactive = int(stat.get('total_inactive_file', 0))
active = int(stat.get('total_active_file', 0))
limit = int(open('/sys/fs/cgroup/memory/memory.limit_in_bytes').read())
ray_used = usage - inactive - active
print(f'ray_used={ray_used/2**30:.2f}GB limit={limit/2**30:.2f}GB ratio={ray_used/limit:.4f}')
print(f'threshold(0.95)={limit*0.95/2**30:.2f}GB above={ray_used > limit*0.95}')
"

# === 16. 确认 OOM 重试预算 ===
grep -E "failed due to oom|oom retries|infinite retries" $LOG_DIR/$JOB_LOG | tail -10
```

---

## 3. 日志证据

### 3.1 Job Driver 日志 — 报错原文

**文件**: `/tmp/ray/session_latest/logs/job-driver-multishot_20260626_101253_120.log`
**行号**: 340028

```
2026-06-26 19:00:32,342 - utils.patch_interleave_dispatch - ERROR -
An exception was raised from a task of operator "FlatMap(ClipMergeMapper)".
[num_errored_blocks=1] Ignoring this exception with remaining max_errored_blocks=9512.

Traceback (most recent call last):
  File "/log/output/shiyanpeng03/kling-ray/utils/patch_interleave_dispatch.py", line 314, in _patched_scheduling_loop_step
    bytes_read = task.on_data_ready(
  File "/log/output/shiyanpeng03/kling-ray/utils/patch_watchdog_block.py", line 441, in _patched_on_data_ready
    return _orig_on_data_ready(self, max_bytes_to_read)
  File "/opt/vjepa2/lib/python3.12/site-packages/ray/data/_internal/execution/interfaces/physical_operator.py", line 201, in on_data_ready
    raise ex from None
  File "/opt/vjepa2/lib/python3.12/site-packages/ray/data/_internal/execution/interfaces/physical_operator.py", line 196, in on_data_ready
    ray.get(self._pending_block_ref)
  File "/opt/vjepa2/lib/python3.12/site-packages/ray/_private/worker.py", line 2981, in get
    values, debugger_breakpoint = worker.get_objects(
  File "/opt/vjepa2/lib/python3.12/site-packages/ray/_private/worker.py", line 1012, in get_objects
    raise value.as_instanceof_cause()

ray.exceptions.RayTaskError(ObjectReconstructionFailedError):
    ray::FlatMap(ClipMergeMapper)() (pid=936649, ip=10.17.112.41)
    At least one of the input arguments for this task could not be computed:

ray.exceptions.ObjectReconstructionFailedError:
    Failed to retrieve object e4e1a93a6adc34c7dcb95b66a768450f541f30cc1400000002000000.
    To see information about where this ObjectRef was created in Python,
    set the environment variable RAY_record_ref_creation_sites=1 during `ray start` and `ray.init()`.

[OBJECT_UNRECONSTRUCTABLE_LINEAGE_EVICTED] The object cannot be reconstructed
because its lineage has been evicted to reduce memory pressure. To prevent this
error, set the environment variable RAY_max_lineage_bytes=<bytes> (default 1GB)
during `ray start`.
```

**关键信息**:
- 出错的 FlatMap task 运行在 `10.17.112.41` (pid=936649)
- 丢失的 Object ID: `e4e1a93a6adc34c7dcb95b66a768450f541f30cc1400000002000000`
- 报错关键行: **"At least one of the input arguments for this task could not be computed"** — 丢失的是输入 Object，不是当前 task 的输出
- `max_errored_blocks` 从 10000 降到 9512，说明已累积 488 个错误 block

### 3.2 GCS 日志 — NODE_OUT_OF_MEMORY 事件（Worker 被 OOM Kill）

**文件**: `/tmp/ray/session_latest/logs/gcs_server.out`

> **重要说明**: `NODE_OUT_OF_MEMORY` 是 worker exit type，不是节点状态变更。它表示 Ray Memory Monitor 检测到节点内存 >95%，**杀死 worker 进程**，节点本身没有死，Plasma Store 还在运行。

#### OOM 事件 1: 18:56:29

```
[2026-06-26 18:56:29,054 W 74 74] (gcs_server) gcs_worker_manager.cc:58:
Reporting worker exit, worker id = b3eed3a7b6cbf85221096780ad2316eaeea5785b17553841f8b17c57,
node id = 5e6336ded86e6ed249d5f4fefc12c363f3a9885f76ca9c7e3bb0bd11,
address = 10.17.118.239,
exit_type = NODE_OUT_OF_MEMORY,
exit_detail = Task was killed due to the node running low on memory.

Memory on the node (IP: 10.17.118.239, ID: 5e6336ded86e6ed249d5f4fefc12c363f3a9885f76ca9c7e3bb0bd11)
where the lease (lease ID: f5990400e0bb8422404489913bd0ed71ae629f0e8f6c2eec18af3074b7efc2d1,
name=QwenVLCPUPreprocessActor.__init__, pid=4189444, memory used=8.85GB) was running
was 121.65GB / 128.00GB (0.950368), which exceeds the memory usage threshold of 0.95.
Ray killed this worker (ID: b3eed3a7b6cbf85221096780ad2316eaeea5785b17553841f8b17c57)
because it was the most recently scheduled task.

Top 10 memory users:
PID      MEM(GB)  COMMAND
4189168  9.52     ray::QwenVLCPUPreprocessActor.preprocess_video
4189443  9.43     ray::QwenVLCPUPreprocessActor.preprocess_video
4189016  9.04     ray::QwenVLCPUPreprocessActor.preprocess_video
4189173  8.98     ray::QwenVLCPUPreprocessActor.preprocess_video
4189444  8.85     ray::QwenVLCPUPreprocessActor.preprocess_video
4189210  8.71     ray::QwenVLCPUPreprocessActor.preprocess_video
4189166  8.69     ray::QwenVLCPUPreprocessActor.preprocess_video
4189167  8.46     ray::QwenVLCPUPreprocessActor.preprocess_video
4189172  8.16     ray::QwenVLCPUPreprocessActor.preprocess_video
4188951  8.04     ray::QwenVLCPUPreprocessActor.preprocess_video
```

#### OOM 事件 2: 19:00:00

```
[2026-06-26 19:00:00,804 W 74 74] (gcs_server) gcs_worker_manager.cc:58:
Reporting worker exit, worker id = 177e577946fd1ee1e226dd91499c1b5567d662641b1ae7f0dc300195,
node id = 347e9c462c9f3e0a17c89873f41bfc4fd68da87d2f5332a71eac4347,
address = 10.51.133.76,
exit_type = NODE_OUT_OF_MEMORY,
exit_detail = Task was killed due to the node running low on memory.

Memory on the node (IP: 10.51.133.76, ID: 347e9c462c9f3e0a17c89873f41bfc4fd68da87d2f5332a71eac4347)
where the lease (lease ID: 6da90400822f3172a368a9c36e407bd41325cae4f741014b09f5f16e56161750,
name=QwenVLCPUPreprocessActor.__init__, pid=1163450, memory used=9.41GB) was running
was 121.98GB / 128.00GB (0.952936), which exceeds the memory usage threshold of 0.95.
Ray killed this worker (ID: 177e577946fd1ee1e226dd91499c1b5567d662641b1ae7f0dc300195)
because it was the most recently scheduled task.

Top 10 memory users:
PID      MEM(GB)  COMMAND
1163450  9.41     ray::QwenVLCPUPreprocessActor
1160186  9.16     ray::QwenVLCPUPreprocessActor.preprocess_video
1160392  9.05     ray::QwenVLCPUPreprocessActor
1160871  8.88     ray::QwenVLCPUPreprocessActor.preprocess_video
1160486  8.81     ray::QwenVLCPUPreprocessActor.preprocess_video
1162649  8.77     ray::QwenVLCPUPreprocessActor.preprocess_video
1162386  8.64     ray::QwenVLCPUPreprocessActor.preprocess_video
1162671  8.42     ray::QwenVLCPUPreprocessActor.preprocess_video
1163350  8.00     ray::QwenVLCPUPreprocessActor.preprocess_video
1162987  7.78     ray::QwenVLCPUPreprocessActor.preprocess_video
```

#### OOM 统计

```
grep -c 'NODE_OUT_OF_MEMORY' /tmp/ray/session_latest/logs/gcs_server.out
→ 552
```

### 3.3 GCS 日志 — 节点 SIGTERM 驱逐事件

**文件**: `/tmp/ray/session_latest/logs/gcs_server.out`

#### 排查过程

Job driver 中有 9 条 `node is dead or unavailable` 记录，需要确认节点死亡的真实原因。在 GCS 日志中搜索 `death reason`：

```bash
# 搜索节点死亡原因
grep -n 'death reason' /tmp/ray/session_latest/logs/gcs_server.out | grep '2026-06-26' | head -30

# 统计 EXPECTED_TERMINATION vs UNEXPECTED_TERMINATION
grep -n 'death reason' /tmp/ray/session_latest/logs/gcs_server.out | grep '2026-06-26' | grep -c 'EXPECTED_TERMINATION'
# 结果: 33

grep -n 'death reason' /tmp/ray/session_latest/logs/gcs_server.out | grep '2026-06-26' | grep -c 'UNEXPECTED_TERMINATION'
# 结果: 0
```

#### 关键发现

6/26 当天共 33 个节点死亡事件，**全部** 是 `EXPECTED_TERMINATION`（收到 SIGTERM 信号），**0 个** `UNEXPECTED_TERMINATION`。

**节点死亡的根因不是 raylet 崩溃，而是 K8s/集群调度系统主动驱逐节点（收到 SIGTERM 信号）**——如 spot 实例被回收、节点维护、资源调度等。

#### GCS 节点死亡日志

```
[2026-06-26 10:52:32,727 I 74 74] (gcs_server) gcs_node_manager.cc:639: ,
  death reason = EXPECTED_TERMINATION, death message = received SIGTERM
  node_id=90e5c8da07ae3610811933ecb98e021c4b5a7a49be80665bb8924e4b
  node_name=10.16.185.204

[2026-06-26 11:17:08,110 I 74 74] (gcs_server) gcs_node_manager.cc:639: ,
  death reason = EXPECTED_TERMINATION, death message = received SIGTERM
  node_id=e3e7610c03538d928c6701e82da7c7daf8a8a4735ea254a86b43e51e
  node_name=10.83.9.150

[2026-06-26 16:02:40,994 I 74 74] (gcs_server) gcs_node_manager.cc:639: ,
  death reason = EXPECTED_TERMINATION, death message = received SIGTERM
  node_id=262a3ba829352c16a66c49578b9f1c5506cea9b1503c2d65e984e53d
  node_name=10.83.9.118

[2026-06-26 16:04:40,415 I 74 74] (gcs_server) gcs_node_manager.cc:639: ,
  death reason = EXPECTED_TERMINATION, death message = received SIGTERM
  node_id=0fad7fbe24f29e2dbd9abda48554e8d3c17e177a23ed305b0e00a1a6
  node_name=10.82.234.210

[2026-06-26 16:04:46,064 I 74 74] (gcs_server) gcs_node_manager.cc:639: ,
  death reason = EXPECTED_TERMINATION, death message = received SIGTERM
  node_id=e8e3cb06029224696c1f4b6f0187ece9aceee7a54a2e54b509b1c797
  node_name=10.83.8.237
  ... (共 33 条)
```

#### Job Driver 中对应的节点死亡报错

Job driver 中的 `node is dead or unavailable` 与 GCS 中 `death reason = EXPECTED_TERMINATION` 完全对应：

| GCS 记录时间 | 节点 IP | death reason | Job driver 行号 |
|------------|---------|-------------|----------------|
| 10:52:32 | 10.16.185.204 | EXPECTED_TERMINATION, SIGTERM | 50480 |
| 11:17:08 | 10.83.9.150 | EXPECTED_TERMINATION, SIGTERM | 72225 |
| 16:02:40 | 10.83.9.118 | EXPECTED_TERMINATION, SIGTERM | 231576 |
| 16:04:40 | 10.82.234.210 | EXPECTED_TERMINATION, SIGTERM | 233092 |
| 16:04:46 | 10.83.8.237 | EXPECTED_TERMINATION, SIGTERM | 233507 |

**Job driver 日志样例**（行 231576）:

```
(raylet) Task _map_task failed. There are infinite retries remaining, so the task
will be retried. Error: Task failed because the node it was running on is dead or
unavailable. Node IP: 10.83.9.118, node ID: 262a3ba829352c16a66c49578b9f1c5506cea9b1503c2d65e984e53d.
This can happen if the node was preempted, had a hardware failure, or its raylet
crashed unexpectedly.

(raylet) Task QwenVLCPUPreprocessActor.preprocess_video failed. There are 2 retries
remaining, so the task will be retried. Error: The actor is temporarily unavailable:
RpcError: RPC error: failed to connect to all addresses; last error: UNKNOWN:
ipv4:10.83.9.118:10172: Failed to connect to remote host: Connection refused rpc_code: 14

(raylet) Task MapWorker(MapBatches(DistributedQwenVLVideoProcessMapper)).submit failed.
There are infinite retries remaining, so the task will be retried. Error: The actor is
temporarily unavailable: RpcError: RPC error: recvmsg:Connection reset by peer rpc_code: 14
```

> **注意**: Job driver 中的报错文字写着 "its raylet crashed unexpectedly"，但这是 Ray 模板化的错误消息（涵盖所有可能原因）。GCS 日志中的 `death reason = EXPECTED_TERMINATION` 才是真实的死亡原因——节点收到 SIGTERM 被 K8s 主动驱逐，**不是 raylet 自身崩溃**。

### 3.4 Job Driver 日志 — 首次 LINEAGE_EVICTED

**文件**: `/tmp/ray/session_latest/logs/job-driver-multishot_20260626_101253_120.log`
**行号**: 232675-232702

首次出现 LINEAGE_EVICTED 是在 16:04:07（比 19:00:32 的报错早约 3 小时）：

```
2026-06-26 16:04:07,286 - utils.patch_interleave_dispatch - ERROR -
An exception was raised from a task of operator "FlatMap(ClipMergeMapper)".
[num_errored_blocks=1] Ignoring this exception with remaining max_errored_blocks=9999.

ray.exceptions.RayTaskError(ObjectReconstructionFailedError):
    ray::FlatMap(ClipMergeMapper)() (pid=2099685, ip=10.17.117.105)
    At least one of the input arguments for this task could not be computed:

ray.exceptions.ObjectReconstructionFailedError:
    Failed to retrieve object b2b086c2e480e5003f2f75b62eef101ff56352d61400000002000000.

[OBJECT_UNRECONSTRUCTABLE_LINEAGE_EVICTED] The object cannot be reconstructed
because its lineage has been evicted to reduce memory pressure.
```

### 3.5 GCS 日志 — Raylet RemoveNode 统计

**文件**: `/tmp/ray/session_latest/logs/gcs_server.out`

GCS 内部统计显示节点移除总计 3253 次：

```
GcsHealthCheckManager::RemoveNode - 3253 total (0 active),
  Execution time: mean = 0.00ms, total = 2.66ms
NodeManager.RemoveNodeCallback - 3253 total (0 active),
  Execution time: mean = 0.51ms, total = 1665.96ms,
  Queueing time: mean = 12.75ms, max = 777.01ms
```

### 3.6 Job Driver 日志 — Pipeline 运行状态

报错时刻的 Pipeline 进度快照（行 340028 附近）：

```
2026-06-26 19:00:32,xxx INFO logging_progress.py:225 --
  Total Progress: 39660000/113010000

FlatMap(ClipMergeMapper): 39710737/121469129
  Tasks: 8000 [backpressured:tasks(ConcurrencyCap)]
  Queued blocks: 20173 (14.0GiB)
  Resources: 8000.0 CPU, 23.8GiB object store

MapBatches(DistributedQwenVLVideoProcessMapper): 6096096/10374048
  Tasks: 2000; Actors: 1000
  Queued blocks: 42562 (96.5GiB)
  Resources: 1000.0 CPU, 500.0 GPU, 21.2GiB object store
```

### 3.7 日志文件统计

| 日志文件 | 行数 | 说明 |
|---------|------|------|
| `gcs_server.out` | 8,813,044 | GCS 服务日志, 8.8M 行 |
| `raylet.out` (head) | 1,084,622 | Head 节点 raylet 日志 |
| `job-driver-multishot_20260626_101253_120.log` | 396,605 | 目标作业 driver 日志 |

### 3.8 日志验证修正

排查中发现旧版文档存在以下不准确描述，经日志验证后修正：

| 旧文档描述 | 实际日志验证 | 修正 |
|-----------|-------------|------|
| "GCS 心跳超时 → NODE_OUT_OF_MEMORY → 节点标记 DEAD" | `grep -c 'transitioned to DEAD'` → **0 条** | NODE_OUT_OF_MEMORY 是 worker exit type，**不是节点 DEAD**。Ray Memory Monitor 杀 worker 进程，节点本身没死 |
| "Driver 收到 Node failure 通知 × 2064" | `grep -c 'Node failure'` → **0 条** | Driver 日志中无 "Node failure" 记录。实际有 9 条 task 级别的 "node is dead or unavailable" 报错 |
| "6/26 全天 530+ 次 NODE_OUT_OF_MEMORY" | `grep -c 'NODE_OUT_OF_MEMORY'` → **552 次** | 修正为 552 次 |
| "49 个 Object 永久不可恢复" | 实际 900 次 LINEAGE_EVICTED | 修正为 900 次 |
| "Worker 被 kill → Object Pin 被释放 → LRU 驱逐" | 代码验证: Pin 由 Driver 持有, Worker 死亡不释放 Pin | 修正为: **节点收到 SIGTERM 被 K8s 驱逐 → Plasma Store 消失 → Object 全部丢失** |
| "重试 task 会重新产生 Object 导致重复" | 代码验证: ObjectID 确定性生成 + 陈旧 attempt 过滤 + ObjectRefStream 去重 | 修正为: **重试不会产生重复 block** |
| "raylet 崩溃导致节点死亡" | GCS 验证: 33 个节点死亡全部是 `EXPECTED_TERMINATION (SIGTERM)`，0 个 `UNEXPECTED_TERMINATION` | 修正为: **K8s/集群调度系统主动驱逐节点 (SIGTERM)**，不是 raylet 自身崩溃 |

---

## 4. 根因分析

### 4.1 完整因果链

```
┌──────────────────────────────────────────────────────────────────────┐
│  第一层：内存压力源                                                   │
│                                                                      │
│  QwenVLCPUPreprocessActor × 10+/128GB 节点                          │
│  单个 ~9GB, 合计 ~85-90GB → 节点内存 121.98/128GB (95.3%)           │
│  超过 Ray memory_usage_threshold (0.95)                             │
│  + cgroup v1 kmem 虚高 53GB (额外贡献) → Ray 误判 OOM                │
└────────────────────────────────┬─────────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────────┐
│  第二层：Ray OOM Kill Worker                                        │
│                                                                      │
│  Memory Monitor 每 250ms 轮询:                                       │
│    ray_used = usage_in_bytes - inactive_file - active_file            │
│    if ray_used/total > 0.95 → kill 最近调度的 worker                  │
│  → Worker 退出, exit_type=NODE_OUT_OF_MEMORY                        │
│  ★ 注意: 节点本身没有 DEAD, Plasma Store 仍在运行                   │
│                                                                      │
│  GCS 日志: 552 次 NODE_OUT_OF_MEMORY (worker exit, 非节点 DEAD)     │
│  GCS 日志: 33 个节点收到 SIGTERM 被 K8s 主动驱逐                    │
│  Job driver: 9 条 node dead 事件 (5 个不同节点, 均为 SIGTERM 驱逐)  │
└────────────────────────────────┬─────────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────────┐
│  第三层：Object 丢失                                                 │
│                                                                      │
│  ★ 主要原因: 节点收到 SIGTERM 被 K8s 驱逐 → Plasma Store 消失       │
│  → pinned_objects_ 随 raylet 进程消失                                 │
│  → 节点上所有 Plasma Object 物理删除                                  │
│                                                                      │
│  注: Worker 被 OOM kill 本身不释放 Pin (Pin 由 Driver 持有)          │
│  但 OOM 会触发大量 actor 重调度和 task 重试 → lineage 增长            │
│  节点被 K8s 驱逐 (SIGTERM) 是 Object 丢失的直接原因                   │
└────────────────────────────────┬─────────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────────┐
│  第四层：Lineage Reconstruction 失败                                  │
│                                                                      │
│  Owner (Driver) 检测到 Object 丢失                                    │
│  → 尝试 RecoverObject()                                              │
│  → 路径 A: PinExistingObjectCopy — 无副本可用 (节点已死/副本被驱逐) │
│  → 路径 B: ReconstructObject — ResubmitTask(task_id)                 │
│    → submissible_tasks_ 中找不到 Task spec                           │
│    → 因为 lineage 已被 GC (作业运行 9 小时, lineage 超 1GB 限制)     │
│    → 返回 OBJECT_UNRECONSTRUCTABLE_LINEAGE_EVICTED                    │
│  → Object 永久不可恢复                                               │
└────────────────────────────────┬─────────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────────┐
│  第五层：作业层面影响                                                │
│                                                                      │
│  patch_interleave_dispatch 捕获异常,                                  │
│  max_errored_blocks 从 10000 降至 9512 (已丢 488 个 block)          │
│  900 次 LINEAGE_EVICTED 错误                                         │
│  作业继续运行但静默丢失数据                                          │
└──────────────────────────────────────────────────────────────────────┘
```

### 4.2 为什么 head 节点 raylet/gcs 日志中没有 lineage evict 记录？

这是排查过程中的一个关键疑惑点：

| 日志文件 | 搜索结果 | 原因 |
|---------|---------|------|
| `gcs_server.out` | 0 条 LINEAGE_EVICTED | GCS 不管理 lineage, lineage 由 Driver (Owner) 进程管理 |
| `raylet.out` (head) | 0 条 lineage evict | lineage eviction 是 Driver 进程内部行为, 不在 raylet 中记录 |
| `job-driver-*.log` | 900 条 LINEAGE_EVICTED | **正确来源**: Driver 进程的 `submissible_tasks_` 超限后 GC |

**结论**: `OBJECT_UNRECONSTRUCTABLE_LINEAGE_EVICTED` 错误来自 Driver 进程的 `reference_counter.cc` 中的 `FlushObjectsToRecover()` 方法。当 `lineage_footprint_bytes_ > max_lineage_bytes_` 时，Driver 会 GC 老 Task spec。这个行为只记录在 Driver 自己的日志中，不会出现在 GCS 或 raylet 日志中。

### 4.3 三重失败模型

此案例的核心是**三重失败同时发生**，任何单一机制都无法兜底：

| 失败层 | 机制 | 后果 | 正常情况下能否单独兜底 |
|--------|------|------|----------------------|
| **1. Object 丢失** | 节点收到 SIGTERM 被 K8s 驱逐 → Plasma Store 消失 → Object 物理删除 | 900 个 Object 不可用 | **能** — Lineage reconstruction 可重建 |
| **2. Lineage 驱逐** | `max_lineage_bytes` 超限 → Task spec 被 GC | 找不到重建所需的 Task spec | **能** — 如果 Object 未丢失，不需要重建 |
| **3. 无副本** | Phase 1 Push 的副本在 LRU 中，可能被 Plasma 驱逐 | 找不到其他节点的副本 | **能** — 如果 lineage 还在，可以重算 |

三重失败叠加：Object 丢失（需要重建） + Lineage 被驱逐（无法重建） + 无副本（无法 Pin 其他节点） = **永久不可恢复**。

### 4.4 内存消耗分析

| 组件 | 内存占用 | 说明 |
|------|---------|------|
| QwenVLCPUPreprocessActor × 10 | ~85-90 GB | 主要内存消耗者, 每个 ~9GB |
| raylet + Dashboard | ~5 GB | 系统开销 |
| Plasma Object Store | ~4 GB | 共享内存 |
| FlatMap(ClipMergeMapper) tasks | ~5-10 GB | 8000 并发 CPU task |
| 其他 | ~5 GB | |
| **总计** | **~121-128 GB** | 接近或超过 128GB 限制 |

10 个 QwenVLCPUPreprocessActor 占用了 128GB 节点的 **~70%** 内存，留给其他组件的空间不足。

**Memory Monitor 杀的是"最近调度的 task"**，不是最大内存占用者。GCS 日志显示被杀的 worker 名称是 `QwenVLCPUPreprocessActor.__init__`，实际内存消耗者也是这些 actor。

### 4.5 OOM 与 Lineage Eviction 的恶性循环

三个循环互相加强，形成正反馈：

```
循环 1: OOM → Object 丢失 → Recovery → ResubmitTask → 新 lineage → lineage 增长
          ↑                                                        │
          └────────────────────────────────────────────────────────┘
          (新 task 执行又可能触发 OOM)

循环 2: lineage 超限 → GC 老 lineage → 老 Task spec 被删
          ↑                                        │
          └── 新 Object 丢失时找不到 Task spec ────┘
              → Recovery 失败 → 更多 error_block

循环 3: Recovery 失败 → error_block → max_errored_blocks 消耗
          ↑                                        │
          └── 静默丢数据 → 作业继续跑 → 更多 task ─┘
              → lineage 继续增长 → 更频繁的 GC
```

---

## 5. 深度机制分析

### 5.1 核心疑问

> **疑问 1**: Worker 被 OOM 杀死后，不是应该重试 task 吗？Object 在 node 节点的 Plasma store 中，worker 死了 object 不还在吗？为什么不能只重试 task？

> **疑问 2**: Pin 不是调用方 Driver 去做的吗？Task worker 被 OOM kill 为什么会导致生产的 Object 丢失？

> **疑问 3**: 如果 task 产生多个 block，产生部分 block 后 OOM kill 重试，会重新产生多个 Object 导致重复吗？

### 5.2 疑问 1 解答：丢失的是输入 Object，不是当前 task 的输出

**答案：丢失的不是当前 task 的输出，而是当前 task 的输入。**

看报错原文中的关键信息：

```
ray::FlatMap(ClipMergeMapper)() (pid=936649, ip=10.17.112.41)
At least one of the input arguments for this task could not be computed:
                              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
ray.exceptions.ObjectReconstructionFailedError:
    Failed to retrieve object e4e1a93a...
```

**"input arguments could not be computed"** — 丢失的是 FlatMap task 的 **输入**（上游 task 产出的 block），不是 FlatMap task 自己的输出。

完整的 task 依赖链：

```
上游 task (在节点 A 上运行)
    → 产出 Object X (存在节点 A 的 Plasma Store 中)
    → Driver 发送 PinObjectIDs RPC → Raylet Pin Object X
        ↓
        ↓  Object X 作为输入传递给下游 task
        ↓
下游 task: FlatMap(ClipMergeMapper) (在节点 B 上运行)
    → ray.get(Object X)  ← 需要节点 A 上的 Object X 作为输入
```

当节点 A 死亡时：

```
节点 A 死亡 → Plasma Store 消失 → Object X 物理丢失
    ↓
节点 B 的 FlatMap task 执行 ray.get(Object X) → 失败
    ↓
★ 重试 FlatMap task 没有用！
  因为重试 FlatMap task 还是需要同一个输入 Object X
  输入不在了，重试多少次都没用
  必须先恢复输入 Object X
```

### 5.3 疑问 2 解答：Pin 机制与 Object 丢失的真正原因

#### 5.3.1 谁来 Pin？—— Driver（Owner），不是 Worker

Object 存储在 Plasma Store（共享内存）中，不在 worker 进程内存里。**Pin 操作由 Driver（Owner）发起**，不是由执行 task 的 worker 发起：

```
Task Worker (执行者)          Driver/Owner (调用方)         Raylet
     │                              │                          │
     │ 执行 task, 产出 Object X     │                          │
     │ → Seal 到 Plasma Store      │                          │
     │                              │                          │
     │                              │ 发送 PinObjectIDs RPC ──→│
     │                              │ (Owner 持有 Object 的    │
     │                              │  ref, 请求 raylet pin)   │
     │                              │                          │
     │                              │                  pinned_objects_[X]
     │                              │                  = unique_ptr<RayObject>
     │                              │                  ← Object 被保护
     │                              │                          │
     │                              │ ← 订阅 WORKER_OBJECT_     │
     │                              │   EVICTION (当 Owner     │
     │                              │   释放 ref 时通知 raylet) │
     │                              │                          │
     │                              │ ← 注册 owner_dead_callback│
     │                              │   (Owner 死亡时释放 pin) │
```

**代码证据** (`core_worker.cc`):

```cpp
// Owner 发送 PinObjectIDs RPC 到 raylet (3 处调用)
// 1. ray.put() 时 (line 998)
local_raylet_rpc_client_->PinObjectIDs(rpc_address_, {object_id}, ...);

// 2. SealOwned 时 (line 1211)
local_raylet_rpc_client_->PinObjectIDs(owner_addr, {object_id}, generator_id, ...);

// 3. 处理 task return 时 (line 3358)
local_raylet_rpc_client_->PinObjectIDs(owner_address, {return_id}, generator_id, ...);
```

**代码证据** (`local_object_manager.cc:31`):

```cpp
Status LocalObjectManager::PinObjectsAndWaitForFree(...) {
    // 1. 插入 pinned_objects_ — 持有 RayObject 的 unique_ptr
    pinned_objects_.emplace(object_id, std::move(object));

    // 2. 记录 owner 信息
    local_objects_[object_id] = LocalObjectInfo(owner_address, ...);

    // 3. 订阅 Owner 的 eviction 通知
    //    当 Owner 释放 ref → 通知 raylet 释放 pin
    subscription_callback = ... → ReleaseFreedObject(obj_id);

    // 4. 注册 owner_dead_callback
    //    当 Owner 死亡 → 自动释放 pin
    owner_dead_callback = ... → ReleaseFreedObject(obj_id);
}
```

#### 5.3.2 Worker 被 Kill 时 Pin 会被释放吗？—— 不会

**关键发现**：raylet 的 `DisconnectClient` 方法（处理 worker 死亡）**不会释放该 worker 创建的 Object 的 Pin**。

```
| 场景 | Pin 会被释放吗？ | 原因 |
|------|------------------|------|
| Worker 被 OOM kill | ✗ 不会 | Pin 由 Owner 持有, 不是 Worker。DisconnectClient 不调用 LocalObjectManager |
| Owner (Driver) 死亡 | ✓ 会 | owner_dead_callback 触发, ReleaseFreedObject |
| Raylet 崩溃 / 节点被驱逐 | ✗ 全部丢失 | pinned_objects_ 随 raylet 进程消失, Plasma Store 一起消失 |
```

**代码证据** (`node_manager.cc:1417`):

```cpp
void NodeManager::DisconnectClient(const rpc::DisconnectClientRequest &request) {
    // 1. 取消 get/wait 请求
    // 2. 释放 lease 资源
    // 3. 从 worker pool 断开
    // 4. 释放 worker 资源

    // ★ 没有调用 local_object_manager_ 释放 pin!
    // TODO(rkn): Tell the object manager that this client has disconnected
    // so that it can clean up the wait requests for this client.
}
```

#### 5.3.3 那 Object 到底是怎么丢失的？

既然 Worker 被 kill 不会释放 Pin，那 Object 为什么会丢失？本案例中有 **两条独立的 Object 丢失路径**：

```
路径 1: 节点收到 SIGTERM 被 K8s 驱逐 → Object 全部物理丢失
  这是本案例 Object 丢失的主要原因!
  GCS 日志: 33 个节点收到 EXPECTED_TERMINATION (SIGTERM)
  Job driver: 9 条 "node is dead or unavailable" 记录, 涉及 5 个节点

  流程:
  K8s 发送 SIGTERM → raylet 收到信号退出
  → pinned_objects_ 随 raylet 进程消失
  → Plasma Store 随之消失 → 所有 Object 物理删除
  → 无需 LRU 驱逐, 直接全部丢失
  → Driver 检测到 Object 丢失 → 尝试 Recovery

路径 2: Worker 被 OOM kill (但节点没死) → Object 通常不丢失
  Pin 由 Driver 持有, Worker 死亡不释放 Pin
  Plasma Store 仍在运行, Object 仍在共享内存中

  但极端情况下可能丢失:
  - 如果节点内存极度紧张, Plasma Store 自身可能无法分配新内存
  - 如果 Object X 的上游 task 也在被 kill 的 Worker 上执行
    → 上游 task 还没产出 Object X 就被 kill
    → Object X 根本不存在 → 下游 ray.get 失败
```

#### 5.3.4 修正后的 Object 丢失因果链

```
┌──────────────────────────────────────────────────────────┐
│  节点收到 SIGTERM 被 K8s 驱逐 (5 个节点)                  │
│                                                          │
│  K8s 发送 SIGTERM 信号 → raylet 退出                     │
│  → pinned_objects_ 全部丢失 (Pin 随 raylet 进程消失)     │
│  → Plasma Store 随之消失                                 │
│  → 节点上所有 Plasma Object 物理删除                      │
│  → ★ 这是本案例 Object 丢失的主要原因                    │
│                                                          │
│  GCS 日志证据:                                            │
│    death reason = EXPECTED_TERMINATION                   │
│    death message = received SIGTERM                      │
│    (33 个节点, 0 个 UNEXPECTED_TERMINATION)              │
└──────────────────────────┬───────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────┐
│  Driver 检测到 Object 丢失                               │
│                                                          │
│  下游 task ray.get(Object X) → Object 不存在              │
│  → 触发 ObjectRecoveryManager::RecoverObject()           │
│  → 路径 A: PinExistingObjectCopy — 在其他节点找副本      │
│    → 无副本 (或副本也被驱逐) → 失败                      │
│  → 路径 B: ReconstructObject — 血缘重算                  │
│    → ResubmitTask(task_id)                               │
│    → submissible_tasks_ 中找不到 Task spec                │
│    → lineage 已被 GC (超 1GB 限制)                        │
│    → OBJECT_UNRECONSTRUCTABLE_LINEAGE_EVICTED            │
│  → Object 永久不可恢复                                   │
└──────────────────────────────────────────────────────────┘

注: Worker 被 OOM kill (552 次) 本身不会导致已完成的 Object 丢失,
    因为 Pin 由 Driver 持有。但 OOM 会触发 actor 重调度和 task 重试,
    增加 lineage 增长压力, 间接加剧 lineage GC。
    真正导致 Object 丢失的是节点被 K8s 驱逐 (收到 SIGTERM)。
```

### 5.4 疑问 3 解答：Task 重试不会产生重复 Block

#### 5.4.1 结论

**Task 产生多个 block 后因 OOM kill 或其他原因重试，不会产生重复 Object。** Ray 通过四层防护机制确保去重。

#### 5.4.2 基础：ObjectID 确定性生成

每个 streaming generator 产出的 block，其 ObjectID 由 `task_id` 和 `index` 确定性生成，不随 attempt 变化：

```cpp
// task_manager.cc:236
ObjectID ObjectRefStream::GetObjectRefAtIndex(int64_t generator_index) const {
    RAY_CHECK_LT(generator_index, RayConfig::instance().max_num_generator_returns());
    // Index 1 is reserved for the first task return from a generator task itself.
    return ObjectID::FromIndex(generator_task_id_, 2 + generator_index);
}
```

- `generator_task_id_` 是 generator task 的 TaskID，**重试时不变**（只是 attempt_number +1）
- 同一个 index `i` 永远映射到同一个 ObjectID：`FromIndex(task_id, 2+i)`
- 重试时重新 yield 相同 index 的 block，产生相同的 ObjectID

#### 5.4.3 关键：ObjectRefStream 在重试间持久存在

##### 5.4.3.1 存储位置：Driver 进程内存

ObjectRefStream 存储在 Driver（Owner）进程的 `TaskManager` 类中，是纯内存数据结构，**没有任何磁盘持久化**：

```cpp
// task_manager.h:743
class TaskManager {
    // Mapping from a streaming generator task id -> object ref stream.
    absl::flat_hash_map<ObjectID, ObjectRefStream> object_ref_streams_
        ABSL_GUARDED_BY(object_ref_stream_ops_mu_);
};
```

`TaskManager` 由 `CoreWorker` 通过 `std::shared_ptr` 持有：

```cpp
// core_worker.h:1806
class CoreWorker {
    std::shared_ptr<TaskManager> task_manager_;
};
```

```cpp
// core_worker_process.cc:431
auto task_manager = std::make_shared<TaskManager>(...);
```

> **没有任何序列化或 checkpoint 机制**。Grep 搜索 `persisted_to_disk`、`checkpoint.*object_ref_stream` 等关键词零命中。`object_ref_streams_` 是一个普通的内存 `flat_hash_map`。

##### 5.4.3.2 创建：只创建一次

ObjectRefStream 只在 task 首次提交时创建：

```cpp
// task_manager.cc:326 -- AddPendingTask (仅首次提交时调用)
if (spec.IsStreamingGenerator()) {
    const auto generator_id = spec.ReturnId(0);
    RAY_LOG(DEBUG) << "Create an object ref stream of an id " << generator_id;
    absl::MutexLock lock(&object_ref_stream_ops_mu_);
    auto inserted =
        object_ref_streams_.emplace(generator_id, ObjectRefStream(generator_id));
    ref_stream_execution_signal_callbacks_.emplace(
        generator_id, std::vector<ExecutionSignalCallback>());
    RAY_CHECK(inserted.second);  // ★ 断言: stream 不已存在, 只创建一次
}
```

`RAY_CHECK(inserted.second)` 意味着如果同一 `generator_id` 的 stream 已存在，**进程会直接 crash**。这保证了 stream 不会被重复创建。

##### 5.4.3.3 重试时不重建

重试路径全部**不触碰 `object_ref_streams_`**：

| 路径 | 是否删除 stream | 是否创建 stream | 代码位置 |
|------|---------------|---------------|---------|
| `AddPendingTask`（首次提交） | — | ✓ 创建 | `task_manager.cc:326` |
| `RetryTaskIfPossible`（task 失败重试） | ✗ 不触碰 | ✗ 不创建 | `task_manager.cc:1138` |
| `SetupTaskEntryForResubmit` | ✗ 不触碰 | ✗ 不创建 | `task_manager.cc:414` |
| `MarkGeneratorFailedAndResubmit` | ✗ 不触碰 | ✗ 不创建 | `task_manager.cc:470` |
| `ResubmitTask`（lineage 重建） | ✗ 不触碰 | ✗ 不创建 | `task_manager.cc:354` |
| `CompletePendingTask` | ✗ 不删除，只调 `MarkEndOfStream` | ✗ 不创建 | `task_manager.cc:1043` |
| `FailPendingTask` | ✗ 不删除，只调 `MarkEndOfStream` | ✗ 不创建 | `task_manager.cc:1559` |

`SetupTaskEntryForResubmit` 的实际代码——只改 task entry，不碰 stream：

```cpp
// task_manager.cc:414
void TaskManager::SetupTaskEntryForResubmit(TaskEntry &task_entry) {
    task_entry.MarkRetry();
    SetTaskStatus(task_entry,
                  rpc::TaskStatus::PENDING_ARGS_AVAIL,
                  /* state_update */ std::nullopt,
                  /* include_task_info */ true,
                  task_entry.spec_.AttemptNumber() + 1);  // ★ attempt +1
    num_pending_tasks_++;
    total_lineage_footprint_bytes_ -= task_entry.lineage_footprint_bytes_;
    task_entry.lineage_footprint_bytes_ = 0;
    if (task_entry.num_retries_left_ > 0) {
        task_entry.num_retries_left_--;
    } else {
        RAY_CHECK(task_entry.num_retries_left_ == -1);
    }
    // ★ 没有任何 object_ref_streams_ 的操作!
}
```

重试后 `async_retry_task_callback_` 被调用，它通过 `NormalTaskSubmitter::SubmitTask` 将已有 task spec 重新调度到 worker——**不经过 `AddPendingTask`**，因此不会触发 `RAY_CHECK(inserted.second)`。

##### 5.4.3.4 删除：唯一删除点，有条件保护

`object_ref_streams_` 只有一处删除：

```cpp
// task_manager.cc:623
bool TaskManager::TryDelObjectRefStream(const ObjectID &generator_id) {
    absl::MutexLock lock(&object_ref_stream_ops_mu_);
    bool can_gc_lineage = TryDelObjectRefStreamInternal(generator_id);
    if (!can_gc_lineage) {
        RAY_LOG(DEBUG) << "Generator " << generator_id
                       << " still has lineage in scope, try again later";
        return false;  // ★ 条件不满足, 不删除, 稍后重试
    }

    RAY_LOG(DEBUG) << "Deleting object ref stream of an id " << generator_id;
    object_ref_streams_.erase(generator_id);  // ★ 唯一删除点
    return true;
}
```

删除前必须满足三个条件（`TryDelObjectRefStreamInternal`）：

```cpp
// task_manager.cc:691
bool TaskManager::TryDelObjectRefStreamInternal(const ObjectID &generator_id) {
    // 条件 1: 触发所有 execution signal callbacks (通知 executor 停止阻塞)
    auto signal_it = ref_stream_execution_signal_callbacks_.find(generator_id);
    if (signal_it != ref_stream_execution_signal_callbacks_.end()) {
        for (const auto &execution_signal : signal_it->second) {
            execution_signal(Status::NotFound("Stream is deleted."), -1);
        }
        ref_stream_execution_signal_callbacks_.erase(signal_it);
    }

    auto stream_it = object_ref_streams_.find(generator_id);
    if (stream_it == object_ref_streams_.end()) {
        return true;  // 已删除
    }

    // 清理未消费的 refs
    auto unconsumed_ids = stream_it->second.PopUnconsumedItems();
    reference_counter_.TryReleaseLocalRefs(unconsumed_ids, &deleted);
    in_memory_store_.Delete(deleted);

    // 条件 2: EoF 必须已写入 (task 必须已完成或失败)
    int64_t num_objects_generated = stream_it->second.EofIndex();
    if (num_objects_generated == -1) {
        RAY_LOG(DEBUG) << "Skip streaming generator deletion, EOF not written yet";
        return false;  // ★ task 还在运行, 不删除!
    }

    // 条件 3: 所有 lineage 必须 out of scope
    bool can_gc_lineage = reference_counter_.CheckGeneratorRefsLineageOutOfScope(
        generator_id, num_objects_generated);
    return can_gc_lineage;
}
```

**三个条件**：
1. 所有 execution signal callback 已触发
2. **EoF 已写入**（`EofIndex() != -1`）——task 必须已完成或失败
3. 所有 lineage 已 out of scope

##### 5.4.3.5 删除触发时机

`TryDelObjectRefStream` 由两个路径触发：

**路径 A：Python `ObjectRefGenerator.__del__`**

```python
# object_ref_generator.py:290
def __del__(self):
    if hasattr(self.worker, "core_worker"):
        # NOTE: This can be called multiple times
        self.worker.core_worker.async_delete_object_ref_stream(self._generator_ref)
```

```cpp
// core_worker.cc:3272
void CoreWorker::AsyncDelObjectRefStream(const ObjectID &generator_id) {
    if (task_manager_->TryDelObjectRefStream(generator_id)) {
        return;  // 删除成功
    }
    // 条件不满足, 放入待删除队列, 稍后重试
    absl::MutexLock lock(&generator_ids_pending_deletion_mutex_);
    generator_ids_pending_deletion_.insert(generator_id);
}
```

**路径 B：定期 GC**

```cpp
// core_worker.cc:505
periodical_runner_->RunFnPeriodically(
    [this] { TryDelPendingObjectRefStreams(); },
    RayConfig::instance().local_gc_min_interval_s() * 1000,
    "CoreWorker.TryDelPendingObjectRefStreams");
```

```cpp
// core_worker.cc:3283
void CoreWorker::TryDelPendingObjectRefStreams() {
    absl::MutexLock lock(&generator_ids_pending_deletion_mutex_);
    for (const auto &generator_id : generator_ids_pending_deletion_) {
        if (task_manager_->TryDelObjectRefStream(generator_id)) {
            deleted.push_back(generator_id);
        }
    }
    // 删除成功的从待删除队列移除
}
```

**关键保护**：即使 Python 侧在 task 运行期间提前 GC 了 `ObjectRefGenerator`，C++ 侧 `TryDelObjectRefStream` 会发现 EoF 未设置（`EofIndex() == -1`），返回 `false`，放入 `generator_ids_pending_deletion_` 等待。Stream **不会被立即删除**，直到 task 完成/失败设置 EoF 后才会真正删除。

##### 5.4.3.6 进程崩溃场景：完全丢失，不会重建

`object_ref_streams_` 是纯内存数据结构，**没有任何序列化或 checkpoint**：

- Driver 进程崩溃 → `TaskManager` 析构 → `object_ref_streams_` 随之消失
- `submissible_tasks_`（lineage）也随进程消失
- 所有 Python `ObjectRefGenerator` 对象也消失
- 重启后创建全新的空 `object_ref_streams_`

**但 Driver 崩溃意味着 Owner 死了**，所有 ObjectRef 的 ownership 也消失了。Ray 不会在另一个进程"重建" Owner——Driver 崩溃通常意味着整个作业失败。所以 stream 丢失不会导致重复 block 的问题，因为消费端也不在了。

> 如果 Driver 使用 Ray Job 方式提交（Driver 进程由 Ray 管理），Driver 崩溃后整个 Job 标记为 FAILED，不会自动重启。

##### 5.4.3.7 ObjectRefStream 内部状态（持久跨重试）

```cpp
// task_manager.h:67
class ObjectRefStream {
    TaskID generator_task_id_;                    // 不变 (task_id 跨重试不变)
    ObjectID generator_id_;                       // 不变 (= spec.ReturnId(0))
    absl::flat_hash_set<ObjectID> refs_written_to_stream_;  // ★ 已写入的 ObjectID 集合
    int64_t end_of_stream_index_ = -1;            // EoF index, -1 表示未结束
    int64_t next_index_ = 0;                      // 消费者游标 (已读到哪)
    int64_t max_index_seen_ = -1;                 // 执行器报告的最大 index
    int64_t total_num_object_written_{};          // 总写入数
    int64_t total_num_object_consumed_{};         // 总消费数
};
```

这些字段在重试期间全部保留：
- `refs_written_to_stream_`：第一次 attempt 写入的 ObjectID 集合保留 → 第二次 attempt 重复写入会被去重
- `next_index_`：消费者游标保留 → 已消费的 index 不会被重复交付
- `max_index_seen_`：第一次 attempt 达到的最大 index 保留 → 用于 EoF 计算
- `end_of_stream_index_`：如果第一次 attempt 失败时设置了 EoF，第二次 attempt 的 report 会被 EoF 检查拦截

#### 5.4.4 防护层 1：陈旧 attempt 过滤

当 task 重试后，旧 attempt 的 block report 会被拒绝：

```cpp
// task_manager.cc:780 -- HandleReportGeneratorItemReturns
int64_t attempt_number = request.attempt_number();  // RPC 中的 attempt 号

{
    absl::MutexLock lock(&mu_);
    auto it = submissible_tasks_.find(task_id);
    if (it != submissible_tasks_.end()) {
        if (it->second.spec_.AttemptNumber() > attempt_number) {
            // Generator task reports can arrive at any time. If the first attempt
            // fails, we may receive a report from the first executor after the
            // second attempt has started. In this case, we should ignore the first
            // attempt.
            execution_signal_callback(
                Status::NotFound("Stale object reports from the previous attempt."), -1);
            return false;  // ★ 旧 attempt 的 report 被拒绝
        }
    }
}
```

**工作原理**：RPC 请求中携带 `attempt_number`（proto 定义）：

```protobuf
// core_worker.proto:434
message ReportGeneratorItemReturnsRequest {
    ReturnObject returned_object = 1;
    Address worker_addr = 2;
    int64 item_index = 3;
    bytes generator_id = 5;
    uint64 attempt_number = 6;  // ★ 0 = 第一次执行
}
```

Driver 侧比较当前 task spec 的 `AttemptNumber()`（重试时已 +1）和 RPC 中的 `attempt_number`。如果 spec 的 attempt 更高，说明已进入新重试，旧 attempt 的 report 被拒绝。

#### 5.4.5 防护层 2：ObjectRefStream 去重

即使旧 attempt 的 report 通过了 attempt 检查（如时序竞争），`InsertToStream` 还有第二层去重：

```cpp
// task_manager.cc:181
bool ObjectRefStream::InsertToStream(const ObjectID &object_id, int64_t item_index) {
    RAY_CHECK_EQ(object_id, GetObjectRefAtIndex(item_index));

    // 防护 2a: EoF 检查 — stream 已结束后的 report 被拒绝
    if (end_of_stream_index_ != -1 && item_index >= end_of_stream_index_) {
        return false;
    }

    // 防护 2b: 消费者游标检查 — 已消费的 index 被拒绝
    if (item_index < next_index_) {
        return false;
    }

    // 防护 2c: ObjectID 集合去重 — 已写入的 ObjectID 被拒绝
    auto [_, inserted] = refs_written_to_stream_.emplace(object_id);
    if (!inserted) {
        return false;  // ★ ObjectID 已在 stream 中, 不重复插入
    }

    max_index_seen_ = std::max(max_index_seen_, item_index);
    total_num_object_written_ += 1;
    return true;
}
```

三层检查：
- **2a**: `item_index >= end_of_stream_index_` → stream 已结束，拒绝（处理 race: report 发出 → worker 崩溃 → task 标记失败 → report 到达）
- **2b**: `item_index < next_index_` → 消费者已读过去，拒绝
- **2c**: `refs_written_to_stream_.emplace(object_id)` → ObjectID 已存在，拒绝

#### 5.4.6 防护层 3：EoF 处理 — 重试产出更少的 block

如果第一次 attempt 产出了 block 0-5，第二次 attempt 只产出 block 0-3：

```cpp
// task_manager.cc:212
void ObjectRefStream::MarkEndOfStream(int64_t item_index,
                                      ObjectID *object_id_in_last_index) {
    if (end_of_stream_index_ != -1) {
        return;  // 已设置过 EoF, 不重复设置
    }
    // NOTE: If the task returns a nondeterministic number of values, the second
    // try may return fewer values than the first try. If the first try fails
    // mid-execution, then on a successful second try, when we mark the end of
    // the stream here, any extra unconsumed returns from the first try will
    // be dropped.
    end_of_stream_index_ = std::max(next_index_, item_index);
}
```

- `next_index_`：消费者游标（已消费位置）
- `item_index`：执行器报告的最终 block 数量
- EoF 设为 `max(next_index_, item_index)`：确保不会把消费者游标往回拨

多余的未消费 block（第一次 attempt 写了但第二次没写的）被静默丢弃。

#### 5.4.7 防护层 4：ResubmitTask 对 streaming generator 的特殊处理

当 Object 丢失需要 lineage reconstruction 时，`ResubmitTask` 对正在运行的 streaming generator 有特殊处理：

```cpp
// task_manager.cc:354
std::optional<rpc::ErrorType> TaskManager::ResubmitTask(
    const TaskID &task_id, std::vector<ObjectID> *task_deps) {
    bool should_queue_generator_resubmit = false;
    {
        absl::MutexLock lock(&mu_);
        auto it = submissible_tasks_.find(task_id);
        auto &task_entry = it->second;

        if (task_entry.spec_.IsStreamingGenerator() &&
            task_entry.GetStatus() == rpc::TaskStatus::SUBMITTED_TO_WORKER) {
            // If the task is a running streaming generator, the object may have been
            // created, deleted, and then needed again for recovery. When the task is
            // finished / failed, ResubmitTask will be called again.
            should_queue_generator_resubmit = true;  // ★ 不立即重提交
        } else {
            SetupTaskEntryForResubmit(task_entry);  // 普通 task: 立即重提交
        }
    }

    if (should_queue_generator_resubmit) {
        return queue_generator_resubmit_(spec);  // ★ 延迟到 task 结束/失败后再重提交
    }
    async_retry_task_callback_(spec, /*delay_ms=*/0);
}
```

**不会杀掉正在运行的 generator task**。如果 task 还在运行，resubmit 被排队，等 task 自然结束/失败后再处理。

#### 5.4.8 重试时的完整流程示例

```
第一次 attempt (attempt_number=0):
  Worker yield block_0 → ObjectID = FromIndex(task_id, 2+0)
    → HandleReportGeneratorItemReturns(attempt=0)
    → attempt check: spec.AttemptNumber(0) == request.attempt(0) → 通过
    → InsertToStream(index=0)
      → refs_written_to_stream_.emplace(ObjectID_0) → ✓ 写入
  Worker yield block_1 → ObjectID = FromIndex(task_id, 2+1)
    → InsertToStream(index=1) → ✓ 写入
  Worker yield block_2 → ObjectID = FromIndex(task_id, 2+2)
    → InsertToStream(index=2) → ✓ 写入

  Driver 消费: TryReadNextItem → next_index_=0 → 读 block_0 → next_index_=1
               TryReadNextItem → next_index_=1 → 读 block_1 → next_index_=2

  ⚡ Worker 被 OOM kill / 节点被 SIGTERM 驱逐

重试触发: RetryTaskIfPossible
  → spec.AttemptNumber() 从 0 增至 1
  → ★ ObjectRefStream 不变: refs_written_to_stream_={OID_0,OID_1,OID_2}, next_index_=2
  → async_retry_task_callback_ 重新调度 task

第二次 attempt (attempt_number=1):
  Worker 重新执行 task, 重新 yield block_0
    → HandleReportGeneratorItemReturns(attempt=1)
    → attempt check: spec.AttemptNumber(1) == request.attempt(1) → 通过
    → InsertToStream(index=0)
      → item_index(0) < next_index_(2) → ✗ 拒绝 (已消费)
  Worker yield block_1
    → InsertToStream(index=1)
      → item_index(1) < next_index_(2) → ✗ 拒绝 (已消费)
  Worker yield block_2
    → InsertToStream(index=2)
      → item_index(2) >= next_index_(2) → 检查 refs_written_to_stream_
      → ObjectID_2 已在集合中 → ✗ 拒绝 (已写入)
      ★ 注意: 如果第一次的 block_2 report 在 kill 前已到达 driver, 这里被去重
      ★ 如果第一次的 block_2 report 还没到达就 kill 了, 这里会写入 (是新的)
  Worker yield block_3
    → InsertToStream(index=3)
      → item_index(3) >= next_index_(2) → ObjectID_3 不在集合 → ✓ 写入
  Worker yield block_4
    → InsertToStream(index=4) → ✓ 写入

  Task 完成 → MarkEndOfStream(item_index=5)
    → end_of_stream_index_ = max(next_index_=2, 5) = 5

  Driver 继续消费: TryReadNextItem → next_index_=2 → 读 block_2 → next_index_=3
                   TryReadNextItem → next_index_=3 → 读 block_3 → next_index_=4
                   TryReadNextItem → next_index_=4 → 读 block_4 → next_index_=5
                   TryReadNextItem → next_index_=5 == end_of_stream_index_ → EoF

结果: Driver 看到的是 block_0, block_1, block_2, block_3, block_4
      ★ 没有重复! 重试只补齐了未完成的 block
```

#### 5.4.9 race condition: 旧 attempt 的延迟 report

旧 attempt 可能在 worker 死亡后仍有 report 在网络中飞行：

```
T1: attempt=0 的 Worker yield block_3 → 发送 ReportGeneratorItemReturns(attempt=0)
T2: ⚡ Worker 被 kill
T3: Driver 收到 RetryTaskIfPossible → spec.AttemptNumber() 增至 1
T4: attempt=0 的 block_3 report 到达 Driver
    → HandleReportGeneratorItemReturns(attempt=0)
    → attempt check: spec.AttemptNumber(1) > request.attempt(0) → ✗ 拒绝
    → "Stale object reports from the previous attempt."
```

即使 attempt 检查因为时序通过了（report 在 spec.AttemptNumber 增加前到达），`InsertToStream` 的 `refs_written_to_stream_` 去重也会拦截。

#### 5.4.10 四层防护总结

| 防护层 | 位置 | 机制 | 处理场景 |
|--------|------|------|---------|
| **1. attempt 过滤** | `HandleReportGeneratorItemReturns` | `spec.AttemptNumber() > request.attempt_number()` → 拒绝 | 旧 attempt 的延迟 report |
| **2a. EoF 检查** | `InsertToStream` | `item_index >= end_of_stream_index_` → 拒绝 | stream 已结束后的 report |
| **2b. 消费游标检查** | `InsertToStream` | `item_index < next_index_` → 拒绝 | 已被消费者读过的 index |
| **2c. ObjectID 去重** | `InsertToStream` | `refs_written_to_stream_.emplace()` 失败 → 拒绝 | 同一个 ObjectID 已写入 |
| **3. EoF 修剪** | `MarkEndOfStream` | `max(next_index_, item_index)` | 重试产出更少的 block, 多余的被丢弃 |
| **4. 运行中不杀** | `ResubmitTask` | `queue_generator_resubmit_` 延迟 | generator 还在运行时不立即 resubmit |

### 5.5 两种 OOM 影响场景总结

GCS 日志中 `NODE_OUT_OF_MEMORY` 这个名字有误导性——它实际含义是"因节点内存不足而杀 worker 进程"，**不是节点本身死了**。

| 场景 | 触发条件 | Plasma Store | Pin 状态 | Object 影响 |
|------|---------|-------------|---------|-------------|
| **Worker 被 OOM kill** | Memory Monitor 检测内存 >95%，杀死最近调度的 worker | 仍在运行 | **Pin 不释放** (Driver 持有) | 已完成 Object 通常不丢失 |
| **节点被 K8s 驱逐 (SIGTERM)** | K8s/集群调度系统发送 SIGTERM，如 spot 实例回收、节点维护 | 随之消失 | Pin 随 raylet 消失 | **所有 Plasma Object 全部丢失** |

本案例两种场景都发生了：
- GCS 日志中的 552 次 `NODE_OUT_OF_MEMORY` → Worker 被 kill（场景 1），Object 通常不丢失
- GCS 日志中的 33 次 `EXPECTED_TERMINATION (SIGTERM)` → 节点被 K8s 驱逐（场景 2），**这是 Object 丢失的主要原因**

### 5.6 Plasma Store LRU 驱逐原理

当 Plasma Store 内存不足时（不是 worker OOM，而是 Plasma 自身需要空间），会从 LRU 队列中驱逐 ref_count=0 的 Object：

```
Plasma Store 内部:
┌───────────────────────────────────────────────┐
│  Object A: ref_count=1 (Raylet Pin, Driver 持有) │  ← 不被驱逐
│  Object B: ref_count=1 (Worker Get 中)        │  ← 不被驱逐
│  Object C: ref_count=0 (LRU 中)              │  ← ★ 可被驱逐
│  Object D: ref_count=0 (LRU 中)              │  ← ★ 可被驱逐
└───────────────────────────────────────────────┘

内存不足时:
  EvictionPolicy::ChooseObjectsToEvict()
    → 从 LRU 尾部选择 ref_count=0 的 Object
    → DeleteObjectInternal()
      → 回收共享内存
      → 通知 Owner: RemoveObjectLocation(node_id)
```

> **注意**: 只有 ref_count=0 的 Object 会被 LRU 驱逐。被 Driver Pin 的 Object (ref_count ≥ 1) 不会被驱逐。所以 Worker 被 kill 不会导致 Object 被 LRU 驱逐，因为 Pin 仍然由 Driver 持有。

### 5.7 Object 恢复的两条路径

当 Object 确实丢失（因节点死亡）后，`ObjectRecoveryManager::RecoverObject()` 有两条恢复路径：

```
路径 A: PinExistingObjectCopy — 在其他节点找副本
  → 向其他节点的 raylet 发 PinObjectIDs RPC
  → 如果其他节点有 Object X 的副本 → 成功恢复
  → ★ 不需要 lineage

路径 B: ReconstructObject — 血缘重算
  → ResubmitTask(task_id) — 重新提交"产生 Object X 的上游 task"
  → 上游 task 重新执行 → 重新产出 Object X
  → ★ 需要 lineage (submissible_tasks_ 中必须有上游 task 的 spec)
```

本案例中两条路径都失败了：

```
路径 A 失败原因:
  - 原始节点死亡 → 主本 Pin 丢失
  - Object X 没有被推送到其他节点（或副本也被驱逐）
  - Owner 的 locations 集合中无任何节点 → 无副本可用

路径 B 失败原因:
  - 上游 task 的 spec 已被 lineage GC 驱逐
  - submissible_tasks_.find(task_id) 返回 end
  - 返回 OBJECT_UNRECONSTRUCTABLE_LINEAGE_EVICTED
```

### 5.8 为什么 Lineage 会被驱逐？

Lineage 指的是 Driver (Owner) 进程中 `submissible_tasks_` 存储的 Task 规格信息。这些信息用于 lineage reconstruction——当 Object 丢失时，可以通过重新提交 Task 来重建。

```cpp
// reference_counter.h
class ReferenceCounter {
    // Task spec 存储 — 用于 lineage reconstruction
    absl::flat_hash_map<TaskID, TaskSpecification> submissible_tasks_;

    // Lineage 占用内存统计
    int64_t lineage_footprint_bytes_ = 0;

    // 上限，默认 1GB
    int64_t max_lineage_bytes_ = 1073741824;  // 1GB
};
```

当 `lineage_footprint_bytes_ > max_lineage_bytes_` 时，Driver 会从最老的 Task 开始逐个删除 Task spec，直到释放足够空间。

本案例的触发条件：

| 因素 | 值 | 说明 |
|------|-----|------|
| 作业运行时长 | ~9 小时 | 10:12 启动，19:00 报错 |
| 数据规模 | 113M+ 行 | 大量 task 产生大量 lineage |
| FlatMap blocks | 121M+ | 每个 block 对应一个 task spec |
| 并发度 | 8000 CPU tasks | 同时存活大量 task |
| `max_lineage_bytes` | 1 GB (默认) | 未调大 |
| Lineage 实际占用 | >1 GB | 超过限制触发 GC |

### 5.9 OOM 与 Lineage Eviction 的恶性循环

三个循环互相加强，形成正反馈：

```
循环 1: 节点被 K8s 驱逐 (SIGTERM) → Object 丢失 → Recovery → ResubmitTask → 新 lineage → lineage 增长
          ↑                                                                                  │
          └────────────────────────────────────────────────────────────────────────────────┘
          (K8s 持续驱逐节点, 新 task 执行又可能触发 OOM)

循环 2: lineage 超限 → GC 老 lineage → 老 Task spec 被删
          ↑                                        │
          └── 新 Object 丢失时找不到 Task spec ────┘
              → Recovery 失败 → 更多 error_block

循环 3: Recovery 失败 → error_block → max_errored_blocks 消耗
          ↑                                        │
          └── 静默丢数据 → 作业继续跑 → 更多 task ─┘
              → lineage 继续增长 → 更频繁的 GC
```

**关键认知**: 这三个循环不是独立的，而是互相强化的。节点被驱逐越多 → lineage 增长越快 → lineage GC 越频繁 → Recovery 失败越多 → error_block 越多。打破循环的切入点是**减少节点被驱逐的影响**（增加 Object 副本/Spilling）和**增大 lineage 容量**（提高 `max_lineage_bytes`）。

---

## 6. 时间线还原

```
10:12:53  作业启动
          Pipeline: ReadParquet → ... → FlatMap(ClipMergeMapper) → ... → Write
          集群: ~250 节点, 8000 CPU tasks, 1000 actors, 500 GPU

10:13-16:00  正常运行阶段
             FlatMap 处理进度: 0 → 39M/121M
             QwenVLVideoProcessMapper: 0 → 6M/10M

16:03:01  ★ 首次 OOM (节点 10.51.164.233)
          QwenVLCPUPreprocessActor 内存 121+/128GB
          → Worker 被 kill (Pin 不释放, Driver 持有)
          → Object 通常不丢失 (Plasma Store 仍在运行)

16:02:40  ★ 节点 10.83.9.118 收到 SIGTERM 被 K8s 驱逐
          GCS: death reason = EXPECTED_TERMINATION, SIGTERM
          → raylet 退出 → Plasma Store 消失 → Object 全部丢失

16:04:40  ★ 节点 10.82.234.210 收到 SIGTERM 被 K8s 驱逐
16:04:46  ★ 节点 10.83.8.237 收到 SIGTERM 被 K8s 驱逐

16:04:07  ★ 首次 LINEAGE_EVICTED 错误
          FlatMap(ClipMergeMapper) (pid=2099685, ip=10.17.117.105)
          max_errored_blocks: 10000 → 9999
          → 作业运行 6 小时后 lineage 已超过 1GB 限制

16:04-16:39  OOM 持续发生
             10+ 次 NODE_OUT_OF_MEMORY
             多个节点: 10.51.164.233, 10.51.164.247, 10.51.137.173,
                       10.51.151.102, 10.51.130.13, 10.17.112.11,
                       10.51.139.88

18:56:29  ★ OOM 事件 (节点 10.17.118.239)
          内存 121.65/128GB (95.04%)
          10 个 QwenVLCPUPreprocessActor 占用 ~85GB

19:00:00  ★ OOM 事件 (节点 10.51.133.76)
          内存 121.98/128GB (95.29%)
          10 个 QwenVLCPUPreprocessActor 占用 ~87GB

19:00:32  ★ 用户观察到的报错
          FlatMap(ClipMergeMapper) (pid=936649, ip=10.17.112.41)
          Object e4e1a93a6adc... 不可恢复
          max_errored_blocks 剩余 9512 (已丢 488 个 block)

19:00-19:26  后续影响
             Actor bf649046bf... 反复 lease 失败
             "Failed to lease worker from node ... resources are not enough"
             GCS CheckDeadSubscribers 持续增长 (2335 → 2343)

全程统计:
  - GCS: 552 次 NODE_OUT_OF_MEMORY (worker exit, 非节点 DEAD)
  - GCS: 33 个节点收到 SIGTERM 被 K8s 驱逐 (EXPECTED_TERMINATION)
  - GCS: 0 个 UNEXPECTED_TERMINATION (无 raylet 崩溃)
  - Job driver: 900 次 LINEAGE_EVICTED
  - Job driver: 944 次 patch_interleave_dispatch ERROR
  - Job driver: 9 条 node dead 事件 (5 个不同节点, 均为 SIGTERM 驱逐)
  - GCS: 0 条 "transitioned to DEAD" (无节点被标记 DEAD)
  - GCS: 0 条 "Node failure" 通知
  - GCS: 3253 次 RemoveNode 回调 (历史累计)
```

---

## 7. 解决方案与预防措施

### 7.1 根治方案 — 消除内存压力源

#### 方案 A：降低 Actor 并发数

```python
# 原配置: 10+ QwenVLCPUPreprocessActor / 128GB 节点
.map_batches(
    QwenVLCPUPreprocessActor,
    concurrency=6,  # 从 10 调低
)
```

**内存预估**:
- 6 个 actor × 9GB = 54GB
- raylet + 系统 ~5GB
- Plasma Object Store ~4GB
- 其他 task ~10GB
- 总计 ~73GB / 128GB = 57% — 远低于 95% 阈值

#### 方案 B：检查 Actor 内存释放

```python
class QwenVLCPUPreprocessActor:
    def preprocess_video(self, video):
        result = ...
        del intermediate_tensor
        gc.collect()
        return result
```

#### 方案 C：切换 cgroup v2（消除 kmem 虚高）

```bash
# 在宿主机修改内核启动参数
vi /etc/default/grub
# GRUB_CMDLINE_LINUX="systemd.unified_cgroup_hierarchy=1"
grub2-mkconfig -o /boot/grub2/grub.cfg
reboot

# 验证
stat -f /sys/fs/cgroup/    # Type: cgroup2fs
```

**效果**: `memory.current` 准确反映实际使用（~65-70GB vs 当前的 ~115GB），不再误判 OOM。

### 7.2 缓解方案 — 提高容忍度

#### 方案 D：提高 Lineage 容量

```python
ray.init(
    _system_config={
        "max_lineage_bytes": 5 * 1024 * 1024 * 1024,  # 5GB (默认 1GB)
    }
)
```

**权衡**: 增加 Driver 进程内存消耗。长跑大规模 pipeline 建议 2-5GB。

#### 方案 E：提高 OOM 阈值（临时）

```bash
export RAY_memory_usage_threshold=0.98   # 默认 0.95
```

**注意**: 只是推迟触发，不消除根因。如果 kmem 虚高 54GB，0.98 也可能触发。

#### 方案 F：调度隔离 — Actor 与 Task 分散

```python
# 让高内存 Actor 和普通 Task 分到不同节点
.map_batches(
    QwenVLCPUPreprocessActor,
    ray_remote_args={"scheduling_strategy": "SPREAD"},
)
```

#### 方案 G：max_errored_blocks 容忍

```python
ctx = ray.data.DataContext.get_current()
ctx.max_errored_blocks = 10000  # 当前已设置
```

**关键**: `max_errored_blocks` 不是"重试"，而是"丢弃数据继续跑"。需要监控 `num_errored_blocks` 指标，避免静默丢大量数据。

### 7.3 方案选择矩阵

| 方案 | 效果 | 代价 | 推荐度 |
|------|------|------|--------|
| **A. 降低 Actor 并发** | 根治 OOM | 降低吞吐 | ★★★★★ |
| **B. Actor 内存释放** | 减少单 Actor 占用 | 需要代码修改 | ★★★★ |
| **C. cgroup v2** | 消除 kmem 虚高 | 需要平台配合 | ★★★★ |
| **D. 提高 Lineage 容量** | 减少 lineage GC | 增加 Driver 内存 | ★★★ |
| **E. 提高 OOM 阈值** | 推迟触发 | 治标不治本 | ★★ |
| **F. 调度隔离** | 降低单节点压力 | 需要更多节点 | ★★★ |
| **G. max_errored_blocks** | 容忍丢数据 | 静默数据丢失 | ★（防御性） |

### 7.4 监控告警

| 指标 | 告警阈值 | 数据来源 |
|------|---------|---------|
| 节点内存使用率 | >85% (预警), >90% (告警) | Ray Dashboard / cgroup |
| OOM Kill 频次 | >5 次/5min | raylet 日志 |
| NODE_OUT_OF_MEMORY | 任意出现 | GCS 日志 |
| Lineage 占用 | >80% max_lineage_bytes | job-driver 日志 |
| num_errored_blocks | >0 (任何丢失) | streaming_executor stats |
| OBJECT_UNRECONSTRUCTABLE_* | 任意出现 | job-driver 日志 |

### 7.5 配置建议

```python
# 生产环境推荐配置
ray.init(
    _system_config={
        # Lineage 容量 — 长跑 pipeline 建议 2-5GB
        "max_lineage_bytes": 5 * 1024 * 1024 * 1024,

        # Object Store 内存 — 根据数据量调整
        # ray start --object-store-memory=30000000000  (30GB)
    }
)

# Pipeline 配置
ctx = ray.data.DataContext.get_current()
# max_errored_blocks: 0 (fail-fast) 或较小值用于告警
ctx.max_errored_blocks = 0

# Actor 并发 — 确保 单Actor内存 × 并发数 < 节点可用内存的 70%
# 128GB 节点, 9GB/actor → 128*0.7/9 ≈ 10 → 最多 10 个
# 但要留余量给其他进程, 建议 6-7 个
```

### 7.6 Object Spilling — 避免驱逐的关键机制

Ray 2.7+ 支持 Object Spilling，可以在 Plasma 内存不足时将 Object 写到磁盘而非直接驱逐：

```python
ray.init(
    _system_config={
        "object_spilling_config": json.dumps({
            "type": "filesystem",
            "params": {
                "directory_path": "/tmp/ray_spill"
            }
        }),
    }
)
```

**Spilling vs 驱逐的区别**:

| 方面 | LRU 驱逐 | Object Spilling |
|------|---------|----------------|
| 数据是否丢失 | **永久丢失** | **保留在磁盘** |
| 是否可恢复 | 仅通过 lineage reconstruction | 直接从磁盘读回 |
| 需要 lineage? | 是 | 否 |
| 性能影响 | 无（但恢复代价高） | 磁盘 I/O 延迟 |

**对本案例的影响**: 如果启用了 Object Spilling，即使 Worker 被 OOM Kill 导致 Object 从 Plasma 内存中移出，Object 仍然在磁盘上。Recovery 可以直接从磁盘恢复，不需要 lineage reconstruction，从而避免 LINEAGE_EVICTED 错误。

---

## 8. 附录

### 8.1 关键日志文件索引

| 日志文件 | 路径 | 行数 | 关键内容 |
|---------|------|------|---------|
| GCS 服务日志 | `/tmp/ray/session_latest/logs/gcs_server.out` | 8,813,044 | NODE_OUT_OF_MEMORY × 552, RemoveNode × 3253 |
| Head Raylet 日志 | `/tmp/ray/session_latest/logs/raylet.out` | 1,084,622 | state-dump 资源快照, 无 lineage evict 记录 |
| Job Driver 日志 | `/tmp/ray/session_latest/logs/job-driver-multishot_20260626_101253_120.log` | 396,605 | LINEAGE_EVICTED × 900, node dead × 9, patch_interleave_dispatch ERROR × 944 |

### 8.2 关键代码文件索引

| 文件 | 关键内容 |
|------|---------|
| `src/ray/core_worker/reference_counter.cc` | Lineage GC 逻辑, `FlushObjectsToRecover()`, `submissible_tasks_` |
| `src/ray/core_worker/object_recovery_manager.cc` | `RecoverObject()`, `ReconstructObject()`, `PinExistingObjectCopy()` |
| `src/ray/core_worker/task_manager.cc` | `ResubmitTask()`, `FailOrRetryPendingTask()`, `RetryTaskIfPossible()`, ObjectRefStream (`InsertToStream`, `MarkEndOfStream`, `TryReadNextItem`, `PopUnconsumedItems`), `HandleReportGeneratorItemReturns()`, `SetupTaskEntryForResubmit()`, `TryDelObjectRefStream()` (唯一删除点), `TryDelObjectRefStreamInternal()` (三条件检查), `AddPendingTask()` (stream 创建), `MarkGeneratorFailedAndResubmit()` |
| `src/ray/core_worker/task_manager.h` | `ObjectRefStream` 类定义: `next_index_`, `refs_written_to_stream_`, `max_index_seen_`, `end_of_stream_index_`; `object_ref_streams_` 声明 (line 743); `object_ref_stream_ops_mu_` 独立锁 |
| `src/ray/core_worker/core_worker.cc` | `ReportGeneratorItemReturns()` (executor 侧发送), `HandleReportGeneratorItemReturns()` (driver 侧接收), `PinObjectIDs()` (3 处调用: line 998, 1211, 3358), `AsyncDelObjectRefStream()` (Python `__del__` 触发), `TryDelPendingObjectRefStreams()` (定期 GC) |
| `src/ray/core_worker/core_worker_process.cc` | `TaskManager` 创建 (line 431: `std::make_shared<TaskManager>`), `async_retry_task_callback_` 绑定 |
| `src/ray/protobuf/core_worker.proto` | `ReportGeneratorItemReturnsRequest` 消息定义, `attempt_number` 字段 (line 434) |
| `python/ray/_private/object_ref_generator.py` | `ObjectRefGenerator`: `_get_next_ref()`, `_next_sync()`, `__del__()` — Python 侧 stream 消费者, GC 时触发 stream 删除 |
| `src/ray/object_manager/plasma/eviction_policy.cc` | `ChooseObjectsToEvict()`, LRU 驱逐逻辑 |
| `src/ray/object_manager/plasma/obj_lifecycle_mgr.cc` | `DeleteObjectInternal()`, Object 生命周期管理 |
| `src/ray/raylet/local_object_manager.cc` | `PinObjectsAndWaitForFree()`, `ReleaseFreedObject()` |
| `src/ray/raylet/node_manager.cc` | `CreateKillWorkersCallback()`, OOM kill 逻辑 |
| `src/ray/common/memory_monitor_utils.cc` | cgroup 内存计算公式 |
| `src/ray/common/ray_config_def.h` | `max_lineage_bytes`, `task_oom_retries`, `memory_usage_threshold` |
| `python/ray/data/_internal/execution/streaming_executor_state.py` | `process_completed_tasks()`, max_errored_blocks 处理 |
| `python/ray/exceptions.py` | `ObjectReconstructionFailedError`, `OutOfMemoryError` |

### 8.3 排查决策树

```
ObjectReconstructionFailedError
│
├─ LINEAGE_EVICTED?
│   ├─ Yes → 检查 lineage 占用
│   │   grep "lineage.*exceeds" job-driver-*.log
│   │   ├─ 超限 → lineage GC 导致
│   │   └─ 未超限 → 其他原因的 lineage 丢失
│   │
│   └─ No → MAX_ATTEMPTS_EXCEEDED?
│       ├─ Yes → 检查 OOM 重试预算
│       │   grep "oom retries" raylet.out
│       │   ├─ "infinite" → 陈旧 ObjectRef 竞争
│       │   └─ "0 retries" → OOM 预算耗尽
│       │
│       └─ No → 其他类型 (PUT, RETRIES_DISABLED 等)
│
└─ 根因链:
    LINEAGE_EVICTED → max_lineage_bytes 超限
      → 为什么超限? OOM Kill 导致大量 Recovery → lineage 增长
      → 为什么 OOM? 内存压力源 (QwenVL actor / kmem 虚高)

    注意排查误区:
    ✗ 在 gcs_server.out 中搜索 LINEAGE_EVICTED → 0 条
    ✗ 在 raylet.out (head) 中搜索 lineage evict → 0 条
    ✓ 在 job-driver-*.log 中搜索 LINEAGE_EVICTED → 900 条

    注意 OOM 语义误区:
    ✗ "NODE_OUT_OF_MEMORY = 节点 DEAD" → 错误
    ✓ "NODE_OUT_OF_MEMORY = Worker 被 kill, 节点没死" → 正确
    ✗ "Driver 收到 Node failure 通知 × 2064" → 不存在
    ✓ "Driver 有 9 条 task node-dead 报错, 552 次 worker OOM exit" → 正确

    注意 Pin 机制误区:
    ✗ "Worker 被 kill → Object Pin 被释放 → LRU 驱逐 Object" → 错误
    ✓ "Pin 由 Driver 持有, Worker 死亡不释放 Pin" → 正确
    ✓ "节点被 K8s 驱逐 (SIGTERM) → Plasma Store 消失 → Object 全部丢失" → 正确
    ✓ "Object 丢失的主因是节点被 K8s 驱逐, 不是 Worker 被 OOM kill" → 正确

    注意节点死亡原因误区:
    ✗ "raylet 崩溃导致节点死亡" → 错误 (0 个 UNEXPECTED_TERMINATION)
    ✓ "K8s 发送 SIGTERM 主动驱逐节点 (33 个 EXPECTED_TERMINATION)" → 正确
    ✗ Job driver 中 "raylet crashed unexpectedly" 是模板消息 → 不是真实原因
    ✓ GCS 中 "death reason = EXPECTED_TERMINATION, SIGTERM" → 真实原因

    注意 Task 重试误区:
    ✗ "Task 重试会重新产生 Object 导致重复" → 错误
    ✓ "ObjectID 确定性 + attempt 过滤 + Stream 去重 → 不产生重复" → 正确
```
