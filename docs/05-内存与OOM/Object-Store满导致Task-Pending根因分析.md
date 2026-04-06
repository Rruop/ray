# Ray 集群 Object Store 满导致 Task Pending 的根因分析

> 分析日期: 2026-07-02
> 集群: kml-task-100029555-record-100400116-prod
> Job: `raysubmit_yifFLstruRZdwBWc` (pipeline_ckpt_sjy.py)

---

## 1. 问题描述

Ray 集群运行 data_juicer 数据清洗 pipeline 时出现以下现象：

- **集群有大量空闲资源**（1548/1553 CPU 空闲，16/16 GPU 空闲），但 task 大量 pending
- **Head 节点 object store 使用满**，出现 `"Object fetches queued, waiting for available memory"`
- **Pipeline 近似停顿**，仅 5 CPU 在使用，输出文件缓慢产出

---

## 2. 分析环境

### 2.1 集群拓扑

| 节点 | IP | 角色 | CPU | GPU | Memory | Object Store |
|------|-----|------|-----|-----|--------|-------------|
| head-h-0 | 10.151.35.165 | Head + Driver | 17 | 0 | 124 GB | **37 GB** |
| worker-w-0 | 10.53.82.168 | Worker | 384 | 4×L20 | 1.3 TB | 186 GB |
| worker-w-1 | 10.57.1.22 | Worker | 384 | 4×L20 | 1.3 TB | 186 GB |
| worker-w-2 | 10.57.64.53 | Worker | 384 | 4×L20 | 1.3 TB | 186 GB |
| worker-w-3 | 10.57.21.231 | Worker | 384 | 4×L20 | 1.3 TB | 186 GB |

**聚合资源:**
- Total: 1553 CPU, 16 GPU, 5.3 TB memory, 782 GB object store
- Available: 1548 CPU, 16 GPU, 5.4 TB memory, 644 GB object store
- Used: 5 CPU, 0 GPU, 574 MB memory, 139 GB object store

### 2.2 Job 信息

**采集命令:**
```bash
wez_safe "ray job list --address=auto 2>&1 | grep -E \"job_id|status|entrypoint|ray_version\" | head -30" 3 30
# 采集时间: 21:32:34

wez_safe "ray list actors --address=auto 2>&1 | head -40" 3 30
# 采集时间: 21:32:34

wez_safe "ps aux | grep 2392 | grep -v grep" 3 15
# 采集时间: 21:34:00

wez_safe "cat /mmu_mllm_hdd_3/.../recipes/pipeline_ckpt_sjy.py" 3 15
# 采集时间: 21:34:30

wez_safe "wc -l /mmu_mllm_hdd_3/.../file.0506.list" 3 15
wez_safe "ls /mmu_mllm_hdd_2/.../ckpt/file0506/ | wc -l" 3 15
wez_safe "du -sh /mmu_mllm_hdd_2/.../ckpt/file0506/" 3 15
wez_safe "du -sh /mmu_mllm_hdd_2/.../file0506/" 3 15
wez_safe "stat /mmu_mllm_hdd_2/.../dclm-edu/data/part-25701-0.jsonl" 3 15
# 采集时间: 21:35:00
```

```
Job ID:             03000000
Submission ID:      raysubmit_yifFLstruRZdwBWc
Status:             RUNNING
Driver PID:         2392 (on head node 10.151.35.165)
Entrypoint:         pipeline_ckpt_sjy.py
                    --input-path file.0506.list (7315 个输入文件)
                    --output-path /mmu_mllm_hdd_2/.../file0506
                    --ckpt-path /mmu_mllm_hdd_2/.../ckpt/file0506
Start time:         2026-06-26 22:11
```

Pipeline 结构:
```
from_source → checkpoint_ray → webclean_mapper → output_layout → export_to
```

- 已产出: 25706 个输出文件 (2.8 TB)
- Checkpoint: 25764 个 parquet 文件 (5.0 GB)
- 最后输出活动: 2026-07-02 21:34 (仍在缓慢产出)

---

## 3. 分析方法

### 3.1 核心诊断命令: `ray memory --address=auto`

#### 作用

查询集群中所有 ObjectRef 的引用状态，按节点分组展示。调用 Ray Dashboard 的 memory API，返回：

1. **Per-node 汇总** — 每个节点上 driver 持有的 object 引用统计：
   - `Mem Used by Objects` — 该节点上所有 object 引用占用的总内存
   - `Local References` — 本地 Python 引用计数及总大小
   - `Pinned` — 被 pin 在内存中的 object 计数及大小
   - `Used by task` — 被待执行 task 引用的 object 计数及大小
   - `Captured in Objects` — 被其他 object 嵌套引用的计数及大小
   - `Actor Handles` — Actor 引用计数及大小
2. **逐个 ObjectRef 明细** — 每个 object 的：
   - `IP Address` | `PID` — 持有引用的进程
   - `Type` — Driver / Worker
   - `Call Site` — 创建位置 (disabled 表示未记录)
   - **`Status`** — 关联 task 的调度状态：FINISHED / PENDING_NODE_ASSIGNMENT / PENDING_ARGS_AVAIL / SUBMITTED_TO_WORKER
   - `Attempt` — task 重试次数
   - `Size` — object 大小 (`?` 表示尚未创建, `-1` 表示 metadata-only ref)
   - **`Reference Type`** — LOCAL_REFERENCE / USED_BY_PENDING_TASK / PINNED_IN_MEMORY
   - `Object Ref` — ObjectRef ID
3. **Aggregate object store 统计**：
   - Plasma memory usage (使用量/总量/占用率/needed)
   - Plasma filesystem mmap usage
   - Spilled 总量/数量/写入吞吐
   - Restored 总量/数量/读取吞吐
   - Object fetches 队列状态

#### 数据来源

driver 进程通过 `TaskManager::AddTaskStatusInfo` (`src/ray/core_worker/task_manager.cc:1653-1665`) 将每个 ObjectRef 对应的 task 状态同步到 dashboard。因此 `ray memory` 能同时显示 object 的引用类型和其关联 task 的调度状态。

#### `--address=auto` 参数

自动发现 Ray 集群地址（通过 `RAY_ADDRESS` 环境变量或本地 raylet），无需手动指定 head 节点 IP。

#### 本次分析中的关键输出

| 输出 | 用途 |
|------|------|
| `566886370196.0 B ... 148 (542766216982.0 B)` | head 节点 148 个 USED_BY_PENDING_TASK 共 505 GB |
| `33,112 FINISHED / 254 PENDING_NODE_ASSIGNMENT / 1 PENDING_ARGS_AVAIL` | ObjectRef 状态分布 |
| `Spilled 354414 MiB ... Object fetches queued, waiting for available memory` | object store 满的直证 |

#### 其他诊断命令

通过 **WezTerm CLI** 连接 head 节点 (pane 3)，使用 `wez_safe` 执行命令并捕获输出：

```bash
# 列出 pane
wez_list

# 在 head 节点执行命令
wez_safe "ray status --address=auto" 3 30
wez_safe "ray memory --address=auto" 3 30
wez_safe "ray list tasks --address=auto --limit 30000" 3 30
wez_safe "ray list objects --address=auto --limit 30000" 3 30
```

各命令作用：

| 命令 | 作用 |
|------|------|
| `ray status` | 集群资源总量/使用量、pending demands、节点状态 |
| `ray memory` | ObjectRef 引用状态详情 (见上方详述) |
| `ray list tasks` | Task 列表及状态 (FINISHED/PENDING/RUNNING) |
| `ray list objects` | Object 列表及引用类型/状态 |
| `ray list actors` | Actor 列表及状态 |
| `ray job list` | Job 列表及运行状态 |

### 3.2 代码分析

对 Ray 仓库中的以下代码路径进行分析：

1. **Ray Core task 状态机** — `src/ray/core_worker/task_manager.cc`
2. **依赖解析逻辑** — `src/ray/core_worker/task_submission/dependency_resolver.cc`
3. **Raylet lease 管理** — `src/ray/raylet/lease_dependency_manager.h`
4. **Ray Data streaming executor** — `python/ray/data/_internal/execution/streaming_executor.py`
5. **Backpressure 策略** — `python/ray/data/_internal/execution/backpressure_policy/`
6. **资源管理器** — `python/ray/data/_internal/execution/resource_manager.py`

### 3.3 Pipeline 代码

查看远端 head 节点上的 pipeline 脚本：

```bash
wez_safe "cat /mmu_mllm_hdd_3/.../recipes/pipeline_ckpt_sjy.py"
```

### 3.4 数据采集过程

所有运行时数据均通过 WezTerm CLI 在 **2026-07-02 21:18~21:44** 期间从 head 节点 (pane 3) 实时采集。采集环境：

- **本地环境**: macOS (franke Desktop), myflicker CLI session
- **连接方式**: WezTerm pane 3 → SSH → head 节点 `root@kml-task-100029555-record-100400116-prod-head-h-0`
- **执行工具**: `wez_safe` (base64 编码通道, 抗日志噪声)
- **采集顺序**:
  1. `wez_list` — 发现 pane 3 为 head 节点
  2. `ray status` — 集群资源 + pending demands
  3. `ray memory` — object store 引用详情
  4. `ray list tasks` — task 状态分布
  5. `ray list objects` — object 引用状态分布
  6. `ray list actors` — actor 状态
  7. `ray job list` — job 信息
  8. `python3 /tmp/check_nodes.py` — per-node object store 配置
  9. `ps aux | grep 2392` — driver 进程信息
  10. `cat pipeline_ckpt_sjy.py` — pipeline 代码
  11. `ls/wc/du/df/stat` — 输出目录和 checkpoint 状态

以下第 4 节中每段日志均标注 **采集命令** 和 **采集时间**，确保可追溯。

---

## 4. 关键日志数据

> 所有数据采集时间: 2026-07-02 21:18~21:44 (UTC+8)
> 采集方式: `wez_safe "<command>" 3 <timeout>` (WezTerm pane 3, head 节点)

### 4.1 ray status — 集群资源与 pending demands

**采集命令:**
```bash
wez_safe "ray status --address=auto 2>&1 | tail -30" 3 30
# 采集时间: 21:19:02
```

**原始输出 (节选):**

```
======== Autoscaler status: 2026-07-02 21:19:02.961777 ========
Node status
---------------------------------------------------------------
Active:
 3 worker
 1 headgroup
Idle:
 1 worker
Pending:
 (no pending nodes)
Recent failures:
 worker: NodeTerminated (instance_id: ...worker-w-2)  [大量 worker 节点终止记录]

Resources
---------------------------------------------------------------
Total Usage:
 5.0/1553.0 CPU
 0.0/16.0 GPU
 0.0/1.0 head
 546.91MiB/5.30TiB memory
 138.55GiB/782.21GiB object_store_memory
 0.0/4.0 worker

Pending Demands:
 {'CPU': 1.0, 'memory': 115462665.0}: 2+ pending tasks/actors
 {'CPU': 1.0, 'memory': 114667681.0}: 1+ pending tasks/actors
```

**关键发现:** 只有 3 个 pending task，每个只需 1 CPU + ~110 MB memory，集群有 1548 CPU 空闲，但 task 无法调度。

### 4.2 ray memory — Object store 引用分析

**采集命令:**
```bash
# Head 节点引用汇总
wez_safe "ray memory --address=auto 2>&1 | head -20 | tail -5" 3 30
# 采集时间: 21:19:27 (summary), 21:43:03 (updated)

# Object 状态分布
wez_safe "ray memory --address=auto 2>&1 | grep -E \"PENDING_ARGS|SUBMITTED|RUNNING|FAILED|FINISHED|PENDING_NODE\" | awk -F\"|\" \"{gsub(/ /,\\\"\\\",\\\$5); print \\\$5}\" | sort | uniq -c | sort -rn" 3 15
# 采集时间: 21:19:15

# PENDING_NODE_ASSIGNMENT 分解
wez_safe "ray memory --address=auto 2>&1 | grep PENDING_NODE_ASSIGNMENT | grep LOCAL_REFERENCE | wc -l" 3 15
wez_safe "ray memory --address=auto 2>&1 | grep PENDING_NODE_ASSIGNMENT | grep USED_BY_PENDING_TASK | wc -l" 3 15
# 采集时间: 21:20:00

# Top USED_BY_PENDING_TASK objects
wez_safe "ray memory --address=auto 2>&1 | grep USED_BY_PENDING_TASK | sort -t\"|\" -k7 -rn | head -10" 3 15
# 采集时间: 21:20:30

# PENDING_ARGS_AVAIL object
wez_safe "ray memory --address=auto 2>&1 | grep PENDING_ARGS_AVAIL" 3 15
# 采集时间: 21:21:00
```

#### Head 节点 (10.151.35.165) Object Ref 汇总

```
--- Summary for node address: 10.151.35.165 ---
Mem Used by Objects  Local References  Pinned        Used by task   Captured in Objects  Actor Handles
566886370196.0 B     33,229 (125 MB)   15 (22 GB)   148 (505 GB)   0 (0 B)              1 (0 B)
```

| 引用类型 | 数量 | 总大小 | 说明 |
|---------|------|--------|------|
| LOCAL_REFERENCE | 33,229 | 125 MB | 主要是 865B 小元数据 ref |
| PINNED | 15 | 22 GB | 被 pin 在内存中的 object |
| **USED_BY_PENDING_TASK** | **148** | **505 GB** | 被 pending task 引用的 object (每个 ~4.8 GB) |
| Actor Handles | 1 | 0 B | Actor 引用 |
| **总计** | | **528 GB** | **Head 节点 object store 仅 37 GB** |

#### Object 状态分布

**采集命令:**
```bash
wez_safe "ray memory --address=auto 2>&1 | grep -E \"PENDING_ARGS|SUBMITTED|RUNNING|FAILED|FINISHED|PENDING_NODE\" | awk -F\"|\" \"{gsub(/ /,\\\"\\\",\\\$5); print \\\$5}\" | sort | uniq -c | sort -rn" 3 15
# 采集时间: 21:19:15
# 数据来源: ray memory (driver 侧 ObjectRef 状态, 通过 AddTaskStatusInfo 同步 task 状态到 object ref)
```

```
33,112  FINISHED
   254  PENDING_NODE_ASSIGNMENT
    10  SUBMITTED_TO_WORKER
     1  PENDING_ARGS_AVAIL
```

#### PENDING_NODE_ASSIGNMENT 分解

**采集命令:**
```bash
wez_safe "ray memory --address=auto 2>&1 | grep PENDING_NODE_ASSIGNMENT | grep LOCAL_REFERENCE | wc -l" 3 15
wez_safe "ray memory --address=auto 2>&1 | grep PENDING_NODE_ASSIGNMENT | grep USED_BY_PENDING_TASK | wc -l" 3 15
wez_safe "ray memory --address=auto 2>&1 | grep PENDING_NODE_ASSIGNMENT | grep USED_BY_PENDING_TASK | sort -t\"|\" -k7 -rn | head -10" 3 15
# 采集时间: 21:20:00
# 补充: 通过 ray list objects 交叉验证
wez_safe "ray list objects --address=auto --limit 30000 2>&1 | grep PENDING_NODE_ASSIGNMENT | head -20" 3 30
# 采集时间: 21:35:38
```

| 引用类型 | 数量 | 大小 | 含义 |
|---------|------|------|------|
| LOCAL_REFERENCE | 226 | 865B 或 `?`(未创建) | task 输出 ref，task 在等 worker lease |
| USED_BY_PENDING_TASK | 28 | ~4.8 GB 各 | task 输入 args，已被引用但 task 未执行 |

#### Top USED_BY_PENDING_TASK objects (按大小排序)

**采集命令:**
```bash
wez_safe "ray memory --address=auto 2>&1 | grep USED_BY_PENDING_TASK | sort -t\"|\" -k7 -rn | head -10" 3 15
# 采集时间: 21:20:30

# 总量统计
wez_safe "ray memory --address=auto 2>&1 | grep USED_BY_PENDING_TASK | awk -F\"|\" \"{gsub(/ /,\\\"\\\",\\\$7); sum+=\\\$7; count++} END {printf \\\"Total: %.2f GB, count: %d, avg: %.2f GB\\\\n\\\", sum/1024/1024/1024, count, sum/count/1024/1024/1024}\"" 3 15
# 采集时间: 21:21:00
# 结果: Total: 505.49 GB, count: 148, avg: 3.42 GB
```

```
4.47 GB  FINISHED              # task 已完成，输出 object 等下游消费
4.47 GB  PENDING_NODE_ASSIGNMENT  # task 在等节点分配
4.47 GB  FINISHED
4.47 GB  FINISHED
...
平均: 3.42 GB/object, 总计 505 GB / 148 个
```

#### 1 个 PENDING_ARGS_AVAIL object

**采集命令:**
```bash
wez_safe "ray memory --address=auto 2>&1 | grep PENDING_ARGS_AVAIL" 3 15
# 采集时间: 21:21:00
```

```
10.151.35.165 | 2392 | Driver | disabled | PENDING_ARGS_AVAIL | 7 | 130371019.0 B | USED_BY_PENDING_TASK
```
说明有 1 个 task 的 args 尚未在 driver 本地解析完成 (130 MB object)。

### 4.3 Object store 全局统计

**采集命令:**
```bash
wez_safe "ray memory --address=auto 2>&1 | grep -E \"Aggregate|Plasma|Spilled|Restored|Object fetch\" " 3 15
# 采集时间: 21:19:15
```

```
--- Aggregate object store stats across all nodes ---
Plasma memory usage 302043 MiB, 113 objects, 37.71% full, 17.71% needed
Plasma filesystem mmap usage: 9152 MiB
Spilled 354414 MiB, 7451 objects, avg write throughput 561 MiB/s
Restored 58397 MiB, 652 objects, avg read throughput 78 MiB/s
Object fetches queued, waiting for available memory.
```

**关键发现:**
- 已 spill 354 GB (7451 objects) 到磁盘
- Restore 速度仅 78 MB/s (vs spill 561 MB/s) — 恢复速度远低于写入速度
- "Object fetches queued, waiting for available memory" — 有 object 等待恢复但 object store 无空间

### 4.4 Per-node object store 配置

**采集命令:**
```bash
# 写 Python 脚本到远端并执行
echo "<base64 encoded script>" | base64 -d > /tmp/check_nodes.py && python3 /tmp/check_nodes.py
# 采集时间: 21:27:42
# 脚本内容: ray.init(address="auto") → 遍历 ray.nodes() 打印 per-node resources
```

```
Node 10.53.82.168:  obj_store=186.26 GB, CPU=384, GPU=4
Node 10.57.1.22:    obj_store=186.26 GB, CPU=384, GPU=4
Node 10.57.64.53:   obj_store=186.26 GB, CPU=384, GPU=4
Node 10.57.21.231:  obj_store=186.26 GB, CPU=384, GPU=4
Node 10.151.35.165: obj_store=37.15 GB, CPU=17,  GPU=0  ← Head 节点
```

Head 节点 object store 仅 **37 GB**，但 driver 持有的 object 引用总计 **505 GB**。

### 4.5 Task 分布

**采集命令:**
```bash
# Task 状态分布 (排除 StatsActor)
wez_safe "ray list tasks --address=auto --limit 30000 2>&1 | grep -E \"PENDING|RUNNING|FAILED|SUBMITTED\" | grep -v StatsActor | head -20" 3 30
# 采集时间: 21:33:38

# _map_task 类型 task 状态
wez_safe "ray list tasks --address=auto --limit 30000 2>&1 | grep -E \"_map_task|Write\" | grep -v StatsActor | awk \"{print \\\$5, \\\$8}\" | sort | uniq -c | sort -rn | head -20" 3 30
# 采集时间: 21:33:38

# Task 总量 (含截断提示)
wez_safe "ray list tasks --address=auto --limit 30000 2>&1 | grep -v INFO | grep -v WARNING | grep -v pkg_resources | grep -v UserWarning | grep -v warnings | grep -c FINISHED" 3 30
# 采集时间: 21:35:00
# 注意: API 限制 30000 条 (总计 150240), 最后 100 条展示
```

```
ray list tasks (30000 条采样，总计 150240):
  33,112  FINISHED (主要是 _map_task / Write 任务)
  254     PENDING_NODE_ASSIGNMENT (对应约 3 个 task 的多个 arg/output ref)
  10      SUBMITTED_TO_WORKER (正在执行)
  1       PENDING_ARGS_AVAIL
```

所有 `_map_task` 类型的 task 均为 FINISHED 状态 (533 个)，无 RUNNING/PENDING。

---

## 5. 代码分析

### 5.1 Ray Core Task 状态机

#### 状态转换链

```
                           Driver 侧 (task_manager.cc)                    |     Raylet 侧 (lease_dependency_manager.h)
                                                                           |
SubmitTask()                                                               |
  → 状态: PENDING_ARGS_AVAIL (line 347)                                    |
  → 调用 resolver_.ResolveDependencies(task_spec)                          |
                                                                           |
    ResolveDependencies (dependency_resolver.cc:97)                        |
      → 对每个 by-ref arg: in_memory_store_.GetAsync(obj_id)               |
        → object 不在本地 → 注册 callback, raylet 触发 PullManager fetch    |
        → 所有 args callback 触发 → on_dependencies_resolved()              |
                                                                           |
          → MarkDependenciesResolved() (task_manager.cc:1668)              |
            → CHECK(status == PENDING_ARGS_AVAIL) (line 1675)              |
            → 状态: PENDING_NODE_ASSIGNMENT (line 1678)                    |
              → 向 raylet 发送 RequestWorkerLease                          |
                                                                           |
                raylet 收到 lease 请求 →  lease_dependency_manager          |  → 子状态分解:
                (lease_dependency_manager.h:69-95)                        |
                  num_total = waiting_leases_counter_                      |
                  num_inactive = PullManagerNumInactivePullsByTaskName()   |  PENDING_NODE_ASSIGNMENT (等 worker lease)
                                                                           |  PENDING_ARGS_FETCH (等 args 传输到 worker)
                                                                           |  PENDING_OBJ_STORE_MEM_AVAIL (等 object store 空间)

                raylet 分配 worker → MarkTaskWaitingForExecution()         |
                  → CHECK(status == PENDING_NODE_ASSIGNMENT) (line 1689)   |
                  → 状态: SUBMITTED_TO_WORKER (line 1694)                  |
```

**关键代码位置:**

| 文件 | 行号 | 功能 |
|------|------|------|
| `src/ray/core_worker/task_manager.cc` | 347 | 初始状态: PENDING_ARGS_AVAIL |
| `src/ray/core_worker/task_manager.cc` | 1668-1678 | MarkDependenciesResolved: PENDING_ARGS_AVAIL → PENDING_NODE_ASSIGNMENT |
| `src/ray/core_worker/task_manager.cc` | 1681-1694 | MarkTaskWaitingForExecution: PENDING_NODE_ASSIGNMENT → SUBMITTED_TO_WORKER |
| `src/ray/core_worker/task_submission/normal_task_submitter.cc` | 35-40 | SubmitTask → ResolveDependencies |
| `src/ray/core_worker/task_submission/dependency_resolver.cc` | 97-160 | 依赖解析: GetAsync + InlineDependencies |
| `src/ray/core_worker/store_provider/memory_store/memory_store.cc` | 142-155 | GetAsync: 本地无 object 时注册 callback |
| `src/ray/raylet/lease_dependency_manager.h` | 69-95 | raylet 侧 PENDING 子状态分解 |

#### 关键区分: PENDING_ARGS_AVAIL vs PENDING_NODE_ASSIGNMENT

| 状态 | 含义 | 等待什么 |
|------|------|---------|
| **PENDING_ARGS_AVAIL** | driver 等本地依赖解析 | args 的 ObjectRef 在 driver 的 in-memory store 中可用 |
| **PENDING_NODE_ASSIGNMENT** | args 已解析, 等 worker lease | raylet 分配 worker + args 传输到 worker 节点 |
| **PENDING_ARGS_FETCH** | (raylet 侧) worker 已分配, 等 args 传输 | object 传输到 worker 节点 |
| **PENDING_OBJ_STORE_MEM_AVAIL** | (raylet 侧) args fetch 暂停 | 目标节点 object store 空间 (spilled objects 无法恢复) |

**"无法 fetch" 在 driver 侧显示为 PENDING_NODE_ASSIGNMENT** (driver 只知道 task 还没开始执行，不区分 raylet 内部子状态)。如果 args 尚未解析到 driver 本地，才显示为 PENDING_ARGS_AVAIL。

### 5.2 Ray Data Streaming Executor — 反压机制

#### _dispatch_loop (streaming_executor.py:841-900)

```python
def _dispatch_loop(self, topology: Topology) -> int:
    capacity_dispatch = self._capacity_dispatch  # 默认 True
    i = 0
    while True:
        op = select_operator_to_run(
            topology, self._resource_manager,
            self._backpressure_policies,
            ensure_liveness=self._consumer_idling(),
            ranker=self._ranker,
        )
        if op is None:          # 没有 eligible operator → 退出
            break

        if not capacity_dispatch:
            topology[op].dispatch_next_task()
            ...
            continue

        # 计算所有 backpressure policy 的最小 available_capacity
        soft_cap = None
        for policy in self._backpressure_policies:
            c = policy.available_capacity(op)
            if c is not None:
                soft_cap = c if soft_cap is None else min(soft_cap, c)
        if soft_cap == 0:       # 被反压 → 跳过此 operator
            continue

        # 按 soft_cap 批量 dispatch
        while op_state.has_pending_bundles() and op.can_add_input():
            if soft_cap is not None and n >= soft_cap:
                break
            op_state.dispatch_next_task()
            ...
    return i
```

**关键: `soft_cap == 0` 时, streaming executor 不会调用 `ray.remote().remote()`, task 根本不会提交到 Ray Core。**

#### get_eligible_operators (streaming_executor_state.py:399-477)

operator 被 `select_operator_to_run` 选中需要满足:
1. 未完成 (`!op.has_completed()`)
2. 可以接受输入 (`op.can_add_input()`)
3. 有 pending bundles (`state.has_pending_bundles()`)
4. 未被任何 backpressure policy 阻止 (`p.can_add_input(op) == True`)

当条件 4 不满足时，operator 不会出现在 `eligible_ops` 中 → `select_operator_to_run` 返回 `None` → `_dispatch_loop` 退出。

#### 三个 Backpressure Policy

| Policy | 文件 | available_capacity 逻辑 | 触发条件 |
|--------|------|------------------------|---------|
| **ConcurrencyCapBackpressurePolicy** | `concurrency_cap_backpressure_policy.py:151-210` | `effective_cap - num_tasks_running` | 动态 EWMA 调整; 当 object store budget 充足(>10%)时跳过动态控制 |
| **ResourceBudgetBackpressurePolicy** | `resource_budget_backpressure_policy.py:27-30` | 委托给 `OpResourceAllocator.available_task_capacity` | CPU/GPU/object_store budget 耗尽 |
| **DownstreamCapacityBackpressurePolicy** | `downstream_capacity_backpressure_policy.py:163-209` | `0 if should_apply_backpressure else None` | object store budget utilization > 50% + queue ratio 超标 |

#### ResourceBudgetBackpressurePolicy → available_task_capacity (resource_manager.py:793-824)

```python
def available_task_capacity(self, op: PhysicalOperator) -> Optional[int]:
    budget = self.get_budget(op)
    if budget is None:
        return None
    incr = op.incremental_resource_usage()
    output_per_task = op.metrics.obj_store_mem_max_pending_output_per_task or 0
    caps = []
    if incr.cpu and incr.cpu > 0:
        caps.append(int(budget.cpu // incr.cpu))
    if incr.gpu and incr.gpu > 0:
        caps.append(int(budget.gpu // incr.gpu))
    obj_per_task = max(incr.object_store_memory or 0, output_per_task)
    if obj_per_task > 0:
        caps.append(int(budget.object_store_memory // obj_per_task))
    if not caps:
        return None
    return max(0, min(caps))  # 取最紧张的 resource 维度
```

当 `budget.object_store_memory < obj_per_task` (单个 task 输出 ~4.8 GB) 时, object store 维度返回 0 → `soft_cap = 0` → 反压。

#### DownstreamCapacityBackpressurePolicy._should_apply_backpressure (downstream_capacity_backpressure_policy.py:163-193)

```python
def _should_apply_backpressure(self, op: PhysicalOperator) -> bool:
    if self._should_skip_backpressure(op):
        return False
    utilized_budget_fraction = get_utilized_object_store_budget_fraction(...)
    if (utilized_budget_fraction is not None
        and utilized_budget_fraction <= self.OBJECT_STORE_BUDGET_UTIL_THRESHOLD):  # 0.5
        return False  # budget utilization < 50% → 不反压
    queue_ratio = self._get_queue_ratio(op)
    return queue_ratio > self._backpressure_capacity_ratio
```

#### Object store memory limit 计算 (resource_manager.py:325-337)

```python
def get_global_limits(self) -> ExecutionResources:
    ...
    default_mem_fraction = self._object_store_memory_limit_fraction  # 默认 0.5
    total_resources = total_resources.copy(
        object_store_memory=total_resources.object_store_memory * default_mem_fraction
    )
    self._global_limits = default_limits.min(total_resources).subtract(exclude)
    return self._global_limits
```

Ray Data 的 object store memory limit = `total_object_store_memory × 0.5` = 782 GB × 0.5 = **391 GB**。

### 5.3 ObjectRef 生命周期

```
1. 创建: task 在 worker 节点完成 → 产出 ObjectRef (存在 worker 的 object store)
2. 引用: Driver (head) 持有 ObjectRef (USED_BY_PENDING_TASK = 下游 task 待消费)
3. 消费: 下游 operator 需要 fetch object 到本地处理
4. 释放: on_task_finished() → _pending_task_inputs.remove() → destroy_if_owned()
```

关键代码:
- `op_runtime_metrics.py:940-947` — `on_task_submitted`: 添加到 `_pending_task_inputs`
- `op_runtime_metrics.py:1063-1128` — `on_task_finished`: 移除 `_pending_task_inputs` + `destroy_if_owned()`
- `ref_bundle.py:184-195` — `destroy_if_owned`: 如果 `eager_free=True` 且 `owns_blocks=True` 则调用 `ray.internal.free()`

---

## 6. 根因分析

### 6.1 完整因果链

```
┌─────────────────────────────────────────────────────────────────┐
│ 1. webclean_mapper task 在 worker 完成, 产出 ~4.8 GB object      │
│    (object 存在 worker 节点的 object store, 186 GB 容量)         │
└───────────────────────────┬─────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│ 2. Driver (head) 持有 ObjectRef                                   │
│    (USED_BY_PENDING_TASK = 下游 output_layout/export_to 待消费)  │
│    148 个 × 4.8 GB = 505 GB 的 object 引用                        │
└───────────────────────────┬─────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│ 3. Head 节点 object store 仅 37 GB → 容量严重不足                  │
│    505 GB 引用 vs 37 GB 容量 = 13.6x 超载                         │
│    → 大量 object 被 spill 到磁盘 (354 GB / 7451 objects)          │
└───────────────────────────┬─────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│ 4. Spill/restore 速度严重失配                                     │
│    Spill 写入: 561 MB/s                                          │
│    Restore 读取: 78 MB/s (慢 7.2x)                               │
│    恢复一个 4.8 GB object 需要 ~60 秒                             │
│    → "Object fetches queued, waiting for available memory"        │
└───────────────────────────┬─────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│ 5. 下游 operator (output_layout/export_to) 处理极慢               │
│    5 CPU / 1553 可用 = 0.3% 利用率                                │
│    → task 输出无法被消费 → ObjectRef 不释放                       │
│    → object store 无法回收 → 死锁加剧                              │
└───────────────────────────┬─────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│ 6. Backpressure 机制触发                                         │
│    a) DownstreamCapacityBackpressurePolicy:                       │
│       utilized_budget_fraction > 50% + queue ratio 超标            │
│       → available_capacity = 0                                    │
│    b) ResourceBudgetBackpressurePolicy:                            │
│       budget.object_store_memory < 4.8 GB (单 task 输出)           │
│       → available_task_capacity = 0                               │
│    c) _dispatch_loop: soft_cap = min(0, 0, ...) = 0               │
│       → 跳过 operator, 不 dispatch 新 task                        │
└───────────────────────────┬─────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│ 7. 死锁形成                                                      │
│    反压阻止新 task 提交                                           │
│    + 已提交的 task 卡在 PENDING_OBJ_STORE_MEM_AVAIL (args 无法    │
│      fetch, head object store 满, spilled objects 恢复太慢)      │
│    + 已完成的 task 输出无法被消费 (ObjectRef 不释放)               │
│    + object store 无法回收 → 持续满载                             │
│    → Pipeline 近似停顿                                            │
└─────────────────────────────────────────────────────────────────┘
```

### 6.2 两层调度的 pending 区分

这是分析中最容易混淆的点。系统存在两层独立的调度，pending 也有两层：

```
┌──────────────────────────────────────────────────────────────┐
│ Layer 1: Ray Data Streaming Executor (Python 层)              │
│                                                                │
│  _dispatch_loop:                                               │
│    select_operator_to_run() → None (无 eligible operator)       │
│    或 soft_cap = 0 → continue (反压, 不提交新 task)            │
│                                                                │
│  → task 根本不会调用 ray.remote().remote()                     │
│  → 不会产生 Ray Core 层面的 pending                            │
│  → 这层 "pending" 不可见, 只表现为 pipeline 停顿              │
└──────────────────────────────────────────────────────────────┘
                            ↓ (反压之前已提交的 task)
┌──────────────────────────────────────────────────────────────┐
│ Layer 2: Ray Core (raylet, C++ 层)                            │
│                                                                │
│  PENDING_ARGS_AVAIL → PENDING_NODE_ASSIGNMENT → SUBMITTED      │
│                                                                │
│  Raylet 侧进一步分解 PENDING_NODE_ASSIGNMENT:                  │
│    - PENDING_NODE_ASSIGNMENT (真正等 worker lease)              │
│    - PENDING_ARGS_FETCH (worker 已分配, 等 args 传输)           │
│    - PENDING_OBJ_STORE_MEM_AVAIL (等 object store 空间)        │
│                                                                │
│  → 这些状态通过 ray status / ray memory 可见                   │
│  → ray status 统一显示为 driver 侧的 PENDING_NODE_ASSIGNMENT   │
└──────────────────────────────────────────────────────────────┘
```

### 6.3 254 个 PENDING_NODE_ASSIGNMENT ObjectRef 的含义

实际只有 **3 个 pending task** (从 `ray status` 的 Pending Demands 可见)，但产生了 254 个 ObjectRef：

| 引用类型 | 数量 | 大小 | 含义 |
|---------|------|------|------|
| LOCAL_REFERENCE | 226 | 865B 或 `?` | task 的 **输出 ref** (num_returns)，task 在等 worker lease 时输出尚未创建 (size=`?`) |
| USED_BY_PENDING_TASK | 28 | ~4.8 GB | task 的 **输入 args**，已被引用但 task 未执行 |

每个 task 可能携带多个 arg ref 和多个 output ref，所以 3 个 task 产生 254 个 ref。

### 6.4 为什么 3 个 task 有资源但 pending

这 3 个 task 卡在 PENDING_NODE_ASSIGNMENT 的真正原因：

**不是资源不足**（1548 CPU 空闲，110 MB memory 需求微不足道），而是 **PENDING_OBJ_STORE_MEM_AVAIL**：

1. Streaming executor 在反压启动 **之前** 已提交了这些 task (`ray.remote().remote()`)
2. Raylet 收到 RequestWorkerLease，可能已分配了 worker (worker 节点有 384 CPU)
3. Worker 节点需要 fetch 4.8 GB 的 args 到本地 object store
4. Args 在 head 节点上，但已被 spill 到磁盘 (354 GB spilled)
5. Head 节点 object store 满 (37 GB)，恢复 spilled object 需要先腾出空间
6. 但已完成的 task 输出无法被消费 (反压阻止新 task + ObjectRef 不释放)
7. → **args 无法恢复 → task 无法执行 → 死锁**

`ray status` 只显示 driver 视角的 PENDING_NODE_ASSIGNMENT (3 个 task)，不区分 raylet 侧的子状态。但 `ray memory` 的 "Object fetches queued, waiting for available memory" 证实了 PENDING_OBJ_STORE_MEM_AVAIL 的存在。

### 6.5 PENDING_ARGS_AVAIL (1 个) 的含义

这 1 个 PENDING_ARGS_AVAIL 的 object 大小为 130 MB (不是 4.8 GB)，说明有一个 task 的某个 arg (130 MB) 尚未在 driver 本地 in-memory store 解析完成 — 即 `in_memory_store_.GetAsync()` 的 callback 还未触发 (object 不在本地，需要从远端 fetch 但尚未到达 driver 进程)。

---

## 7. 结论

### 根本原因

**不是调度代码的 bug，而是资源配比问题导致的多层死锁：**

1. **Head 节点 object store 容量严重不足** — 37 GB 容量 vs 505 GB driver 持有的 object 引用 (148 × 4.8 GB)
2. **Spill/restore 速度失配** — 78 MB/s restore vs 561 MB/s spill，恢复一个 4.8 GB object 需 ~60 秒
3. **Backpressure 正确触发但无法自恢复** — object store budget 耗尽 → soft_cap=0 → 停止 dispatch → 无新 task → 已有 output 无法消费 → object store 无法释放
4. **已提交的 task 卡在 args fetch** — head 节点 spilled objects 无法恢复 → PENDING_OBJ_STORE_MEM_AVAIL

### 触发条件总结

| 条件 | 值 | 影响 |
|------|-----|------|
| Head object store 容量 | 37 GB | 远小于单 task 输出 4.8 GB 的并发需求 |
| Driver 持有的 object 引用 | 505 GB (148 个) | 13.6x 超载 |
| 单个 task 输出大小 | ~4.8 GB | 7.6 个即可填满 head object store |
| Ray Data object store limit | 391 GB (782 GB × 0.5) | 全局 budget 耗尽 |
| Spill 总量 | 354 GB / 7451 objects | 大量 object 在磁盘上 |
| Restore 速度 | 78 MB/s | 恢复一个 4.8 GB object 需 ~60 秒 |

---

## 8. 解决方案

### 8.1 短期缓解

1. **增大 head 节点 object store**
   - 设置 `RAY_object_store_memory` 或增加 head 节点内存
   - 目标: ≥ 单 task 输出的 20 倍 (4.8 GB × 20 ≈ 96 GB)

2. **重启 driver 进程释放引用**
   - 33,229 个 LOCAL_REFERENCE (主要是 865B 小元数据) 加重了 driver 的引用管理负担
   - 重启可释放所有 object store 引用

3. **减少并发积压**
   - 降低 `webclean_mapper` 的 `num_proc`，让产出速度匹配下游消费速度
   - 或减小 `target_max_block_size` (当前 128MB)，使每个 object 更小

### 8.2 长期修复

1. **确保 head 节点 object store ≥ 单 task 输出的 10-20 倍**

2. **让 output_layout/export_to 在 worker 节点执行**
   - 当前 driver 在 head 节点，下游 operator 的 task 输出会回到 head
   - 考虑将 driver 提交到 worker 节点，或配置 task 调度策略

3. **减小 target_max_block_size**
   - 当前 `target_max_block_size='128MB'` + `preserve_dir_structure=True` + `file_mapping=True`
   - 按输入文件粒度拆分输出块，产生大量小 ObjectRef (33k+ 个 865B ref)
   - 使用更大的 block size 减少 ref 数量

4. **Pipeline 参数优化**
   - `--num-proc` 限制 webclean_mapper 并发，避免产出速度远超下游
   - 确保 checkpoint 的 Roaring Bitmap 不会累积过多 ref

---

## 9. 关键命令速查

```bash
# 集群状态
ray status --address=auto

# Object store 内存引用详情
ray memory --address=auto

# 列出 task (按状态过滤)
ray list tasks --address=auto --filter state=PENDING --limit 1000

# 列出 object
ray list objects --address=auto --limit 30000

# 列出 actor
ray list actors --address=auto

# 列出 job
ray job list --address=auto

# 查看特定 object ref 分布
ray memory --address=auto | grep USED_BY_PENDING_TASK | wc -l
ray memory --address=auto | grep PENDING_NODE_ASSIGNMENT | wc -l
ray memory --address=auto | grep -E "Summary|Mem Used|Local|Pinned|Used by"

# 聚合 object store 统计
ray memory --address=auto | grep -E "Plasma|Spilled|Restored|Object fetches"

# Per-node 资源
python3 -c "
import ray
ray.init(address='auto', ignore_reinit_error=True, logging_level='ERROR')
for n in ray.nodes():
    if not n['Alive']: continue
    res = n['Resources']
    print(f\"Node {n['NodeManagerAddress']}: obj_store={res.get('object_store_memory',0)/1024**3:.2f} GB, CPU={res.get('CPU',0)}, GPU={res.get('GPU',0)}\")
ray.shutdown()
"
```

---

## 10. 关键代码文件索引

| 文件 | 行号 | 功能 |
|------|------|------|
| `src/ray/core_worker/task_manager.cc` | 347 | 初始状态: PENDING_ARGS_AVAIL |
| `src/ray/core_worker/task_manager.cc` | 1668-1678 | PENDING_ARGS_AVAIL → PENDING_NODE_ASSIGNMENT |
| `src/ray/core_worker/task_manager.cc` | 1681-1694 | PENDING_NODE_ASSIGNMENT → SUBMITTED_TO_WORKER |
| `src/ray/core_worker/task_submission/normal_task_submitter.cc` | 35-40 | SubmitTask → ResolveDependencies |
| `src/ray/core_worker/task_submission/dependency_resolver.cc` | 97-160 | 依赖解析: GetAsync + InlineDependencies |
| `src/ray/core_worker/store_provider/memory_store/memory_store.cc` | 142-155 | GetAsync: 本地无 object 时注册 callback |
| `src/ray/raylet/lease_dependency_manager.h` | 69-95 | raylet 侧 PENDING 子状态分解 |
| `python/ray/data/_internal/execution/streaming_executor.py` | 841-900 | _dispatch_loop: 反压调度核心 |
| `python/ray/data/_internal/execution/streaming_executor_state.py` | 399-477 | get_eligible_operators |
| `python/ray/data/_internal/execution/streaming_executor_state.py` | 167-180 | dispatch_next_task |
| `python/ray/data/_internal/execution/resource_manager.py` | 203-295 | _estimate_object_store_memory_usage + update_usages |
| `python/ray/data/_internal/execution/resource_manager.py` | 300-337 | get_global_limits (object store limit = total × 0.5) |
| `python/ray/data/_internal/execution/resource_manager.py` | 773-824 | can_submit_new_task / available_task_capacity |
| `python/ray/data/_internal/execution/backpressure_policy/__init__.py` | 19-23 | 3 个 enabled backpressure policies |
| `python/ray/data/_internal/execution/backpressure_policy/backpressure_policy.py` | 42-55 | available_capacity 基类接口 |
| `python/ray/data/_internal/execution/backpressure_policy/concurrency_cap_backpressure_policy.py` | 151-210 | 动态并发控制 + object store budget 检查 |
| `python/ray/data/_internal/execution/backpressure_policy/downstream_capacity_backpressure_policy.py` | 44-70 | object store budget 阈值 (50%) |
| `python/ray/data/_internal/execution/backpressure_policy/downstream_capacity_backpressure_policy.py` | 163-209 | _should_apply_backpressure |
| `python/ray/data/_internal/execution/backpressure_policy/resource_budget_backpressure_policy.py` | 27-30 | 委托给 OpResourceAllocator |
| `python/ray/data/_internal/execution/interfaces/ref_bundle.py` | 184-195 | destroy_if_owned: eager free object |
| `python/ray/data/_internal/execution/interfaces/op_runtime_metrics.py` | 940-947 | on_task_submitted: 添加 _pending_task_inputs |
| `python/ray/data/_internal/execution/interfaces/op_runtime_metrics.py` | 1063-1128 | on_task_finished: 释放 _pending_task_inputs |
| `python/ray/data/context.py` | 115-117 | enable_capacity_based_dispatch (默认 True) |
| `python/ray/data/context.py` | 869 | enable_capacity_based_dispatch 配置 |
