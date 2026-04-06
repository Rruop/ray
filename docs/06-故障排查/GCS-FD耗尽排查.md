# Ray GCS 文件描述符耗尽导致调度阻塞排查指南

> 集群规模：2246 节点 | Head 节点内存：1TB | Ray 版本：2.52.1
> 问题日期：2026-05-16

---

## 目录

- [问题现象](#问题现象)
- [背景知识：Ray Data 调度架构](#背景知识ray-data-调度架构)
- [背景知识：Task 状态机](#背景知识task-状态机)
- [排查思路](#排查思路)
- [排查过程（完整交互记录）](#排查过程完整交互记录)
- [根因分析](#根因分析)
- [FD 耗尽对 Ray 作业的完整影响链分析](#fd-耗尽对-ray-作业的完整影响链分析)
- [端口与网络影响详解](#端口与网络影响详解)
- [容器级 vs 进程级 ulimit 的区别与观察方法](#容器级-vs-进程级-ulimit-的区别与观察方法)
- [解决方案](#解决方案)
- [其他排查中遇到的报错](#其他排查中遇到的报错)
- [排查命令速查表](#排查命令速查表)
- [故障时间线](#故障时间线)
- [相关文档](#相关文档)

---

## 问题现象

1. Ray Data 作业中大量 task 处于 **"Waiting for scheduling"** 状态，调度极慢
2. `ray status` 命令超时，无法连接 GCS：
   ```
   Failed to connect to GCS at address 10.15.3.158:6379 within 5 seconds.
   Timed out while waiting for GCS to become available.
   ```
3. `ray job stop` 同样超时失败：
   ```
   [2026-05-16 14:36:22,939 W 45375 45375] rpc_client.h:153: Failed to connect to GCS at address 10.15.3.158:6379 within 5 seconds.
   [2026-05-16 14:36:52,941 W 45375 45375] gcs_client.cc:205: Failed to get cluster ID from GCS server: TimedOut
   ```
4. GCS 进程 CPU 占用异常高（248%），大量时间花在重试 socket 创建上

---

## 背景知识：Ray Data 调度架构

Ray Data 的调度是一个**两层架构**：

### 第 1 层：应用层调度（集中在 Driver 节点）

`StreamingExecutor` 在 driver 上的一个专用线程中运行控制循环，决定**何时**提交 task、**哪个算子**获得执行机会。

```python
# python/ray/data/_internal/execution/streaming_executor.py:569-665
def _scheduling_loop_step(self, topology: Topology) -> bool:
    """Run one step of the scheduling loop.
    This runs a few general phases:
        1. Waiting for the next task completion using ray.wait().
        2. Pulling completed refs into operator outqueues.
        3. Selecting and dispatching new inputs to operators.
    """
    self._resource_manager.update_usages()
    errored_blocks_per_op, _ = process_completed_tasks(topology, ...)

    while True:
        op = select_operator_to_run(topology, self._resource_manager, ...)
        if op is None:
            break
        topology[op].dispatch_next_task()  # ← 提交新 task
```

task 实际通过 `TaskPoolMapOperator._try_schedule_task()` 提交：

```python
# python/ray/data/_internal/execution/operators/task_pool_map_operator.py:108-143
def _try_schedule_task(self, bundle: RefBundle, strict: bool):
    dynamic_ray_remote_args = self._get_dynamic_ray_remote_args(input_bundle=bundle)
    gen = self._map_task.options(**dynamic_ray_remote_args).remote(
        self._map_transformer_ref,
        data_context, ctx,
        *bundle.block_refs,
        slices=bundle.slices,
        **self.get_map_task_kwargs(),
    )
    self._submit_data_task(gen, bundle)
```

### 第 2 层：Ray Core 分布式调度（每个节点的 Raylet）

一旦 task 通过 `.remote()` 提交，driver 节点的 Raylet 做初始调度决策：

```cpp
// src/ray/raylet/scheduling/cluster_lease_manager.cc:196-244
void ClusterLeaseManager::ScheduleAndGrantLeases() {
  for (auto shapes_it = leases_to_schedule_.begin(); ...) {
    auto scheduling_node_id = cluster_resource_scheduler_.GetBestSchedulableNode(
        lease.GetLeaseSpecification(), ...);
    if (scheduling_node_id.IsNil()) {
      break;  // 找不到可用节点，task 留在队列中
    }
    ScheduleOnNode(node_id, work);
  }
}
```

如果本地满了，会 "spill back" 到远端节点：

```cpp
// src/ray/raylet/scheduling/cluster_lease_manager.cc:422-461
void ClusterLeaseManager::ScheduleOnNode(const NodeID &spillback_to,
                                         const std::shared_ptr<internal::Work> &work) {
  if (spillback_to == self_node_id_) {
    local_lease_manager_.QueueAndScheduleLease(work);  // 本地执行
    return;
  }
  // spillback 到远端节点
  auto node_info = get_node_info_(spillback_to);
  RAY_CHECK(node_info.has_value());
  for (const auto &reply_callback : work->reply_callbacks_) {
    auto reply = reply_callback.reply_;
    reply->mutable_retry_at_raylet_address()->set_ip_address(
        (*node_info).node_manager_address());
    reply->mutable_retry_at_raylet_address()->set_port((*node_info).node_manager_port());
    reply_callback.send_reply_callback_(Status::OK(), nullptr, nullptr);
  }
}
```

### 两层调度都依赖 GCS

- Raylet 通过 GCS（RaySyncer）获取集群资源视图
- Task 状态通过 GCS 广播
- 节点心跳通过 GCS 管理

**GCS 不可用 = 整个集群调度瘫痪**

### 完整调度流转

```
Driver (StreamingExecutor)
  ↓
select_operator_to_run() → 选择算子
  ↓
TaskPoolMapOperator._try_schedule_task() → 调用 .remote()
  ↓
Ray Core (CoreWorker on driver node)
  ↓
Local Raylet (ClusterLeaseManager) → GetBestSchedulableNode
  ↓
Target Node's Raylet (LocalLeaseManager) → 分配 Worker
  ↓
Worker 执行 task
```

---

## 背景知识：Task 状态机

Dashboard 上的 "Waiting for scheduling" 实际对应 Ray Core 的多个状态：

```protobuf
// src/ray/protobuf/common.proto:900-942
enum TaskStatus {
  NIL = 0;
  PENDING_ARGS_AVAIL = 1;          // 等待依赖数据创建
  PENDING_NODE_ASSIGNMENT = 2;     // 等待调度器分配节点（"Waiting for scheduling"）
  PENDING_OBJ_STORE_MEM_AVAIL = 3; // 等待 Object Store 内存释放
  PENDING_ARGS_FETCH = 4;          // 依赖数据正在下载到目标节点
  SUBMITTED_TO_WORKER = 5;         // 已发送到 worker，在 worker 队列中
  RUNNING = 8;                     // 正在执行
  FINISHED = 11;                   // 完成
  FAILED = 12;                     // 失败
}
```

### SUBMITTED_TO_WORKER 状态详解

`SUBMITTED_TO_WORKER` 表示 task 已被调度器分配到某个 worker 进程，但 worker 当前正忙，task 在 worker 本地队列中排队。

**普通 task vs Actor task 的区别**：

| 类型 | SUBMITTED_TO_WORKER 含义 | 持续时间 |
|------|--------------------------|---------|
| **Actor task** | task 已发到 Actor 的 worker，排队等 Actor 串行执行 | 可能较长 |
| **普通 task** | Raylet 已选中 worker，task 做执行前准备（反序列化等） | 通常毫秒级 |

普通 task 的 worker 是**独占**的 —— Raylet 为 task 分配空闲 worker，拿到后立即执行，没有排队概念。

Actor task 默认串行执行（`max_concurrency=1`），Ray Data 使用 prefetch 机制故意提前发送多个 task：

```python
# python/ray/data/_internal/execution/operators/actor_pool_map_operator.py:1190-1198
def schedulable_actors(self) -> List[ray.actor.ActorHandle]:
    return [
        actor
        for actor, state in available_actors.items()
        if state.num_tasks_in_flight < self.max_tasks_in_flight_per_actor()
        and not state.is_restarting
    ]
```

每个 Actor 有 1~2 个 task 在 `SUBMITTED_TO_WORKER` 是正常的 prefetch 行为。

### 本次问题的状态

本次场景中 task 卡在 **`PENDING_NODE_ASSIGNMENT`**（调度器还没找到节点），不是 `SUBMITTED_TO_WORKER`。前者是 GCS FD 耗尽的直接后果 —— 调度器无法获取资源视图所以无法做出分配决策。

---

## 排查思路

### 思路总览

```
Task 调度慢
  → ray status 超时，GCS 无响应
    → 检查 GCS 进程是否存活
      → 存活但 CPU 异常高
        → 查看 GCS 错误日志
          → 发现 FD 耗尽
            → 分析 FD 被什么占用
              → 99.95% 是 socket
                → 分析 socket 连向哪些 IP
                  → 2246 个节点 × 29 连接 = 65134 ≈ 65536 limit
                    → 确认是节点规模导致的 socket 连接数爆炸
```

---

## 排查过程（完整交互记录）

### 第一步：尝试 ray status（发现 GCS 不可用）

```bash
$ ray status
[2026-05-16 14:11:34,979 W 103990 103990] rpc_client.h:153: Failed to connect to GCS at address 10.15.3.158:6379 within 5 seconds.
[2026-05-16 14:12:04,980 W 103990 103990] gcs_client.cc:205: Failed to get cluster ID from GCS server: TimedOut: Timed out while waiting for GCS to become available.
```

`ray status` 完全超时，无法连接到 GCS。`ray summary tasks` 和 `ray memory` 也同样无输出。

**分析**：所有 Ray CLI 命令都需要通过 gRPC 连接 GCS（端口 6379），连接失败说明 GCS 要么挂了，要么无法接受新连接。

### 第二步：检查环境变量和集群信息

```bash
$ echo RAY_ADDRESS=$RAY_ADDRESS
RAY_ADDRESS=                    # 未设置，使用默认自动发现

$ cat /tmp/ray/ray_current_cluster
10.15.3.158:6379                # GCS 地址确认

$ ls /tmp/ray/session_latest/   # session 目录存在，集群在运行
logs  metrics  sockets  ...
```

### 第三步：确认进程状态

```bash
$ ps aux | grep -E '(raylet|gcs_server|ray::)' | grep -v grep
root    1   0.0  ... ray start --head --port=6379 --num-cpus=0 --num-gpus=0 --system-config={...}
root   74  248   ... gcs_server --gcs_server_port=6379 ...    ← CPU 248%！异常高
root  992  11.6  ... raylet --node_ip_address=10.15.3.158 ... ← Head 节点 Raylet
root 1042  28.1  ... ray::DashboardAgent
root 1044   0.0  ... ray::RuntimeEnvAgent
root 3012   0.4  ... ray::_StatsActor
root 3109   0.3  ... ray::ActorLocationTracker
root 99148  0.4  ... ray::JobSupervisor
```

**关键发现**：
- GCS Server（PID 74）**CPU 占用 248%**，极其异常
- Head 节点配置了 `--num-cpus=0 --num-gpus=0`（不承担计算任务）
- Raylet 配置了 `memory=352GB, object_store_memory=150GB`

### 第四步：检查端口监听状态

```bash
$ netstat -tlnp | grep -E '(6379|8265)'
tcp   0     0 0.0.0.0:8265   0.0.0.0:*  LISTEN  272/python     ← Dashboard
tcp6  129   0 :::6379        :::*       LISTEN  74/gcs_server  ← GCS RPC 端口
```

**关键发现**：GCS 端口 6379 的 **`Recv-Q = 129`**，说明有 129 个连接请求积压在内核队列中无法被 `accept()`。正常情况 `Recv-Q` 应该为 0。

### 第五步：查看 GCS 错误日志（定位根因）

```bash
$ tail -100 /tmp/ray/session_2026-05-16_00-44-22_781709_1/logs/gcs_server.err
```

**结果（每秒都在刷）**：
```
E0516 14:14:28.975400943  148 socket_utils_common_posix.cc:477
  socket(10, 1, 0) returned -1 with error: |Too many open files|.
  This process might not have a sufficient file descriptor limit
  for the number of connections grpc wants to open (which is
  generally a function of the number of grpc channels, the lb policy
  of each channel, and the number of backends each channel is load
  balancing across).

E0516 14:14:29.363658164  1383 tcp_server_posix.cc:378
  File descriptor limit reached. Retrying.
```

**确认根因：GCS 进程的文件描述符（FD）耗尽！**

统计错误日志：
```bash
$ grep 'File descriptor' gcs_server.err | head -3
E0516 01:46:21.063  File descriptor limit reached. Retrying.   ← 首次出现
E0516 01:54:54.242  File descriptor limit reached. Retrying.
E0516 01:58:29.243  File descriptor limit reached. Retrying.

$ grep 'File descriptor' gcs_server.err | wc -l
1454                                                            ← 共 1454 次
```

从凌晨 01:46 就开始报错了。

### 第六步：确认 FD 使用情况

```bash
# GCS 进程的 FD 上限（进程级别）
$ cat /proc/74/limits | grep 'open files'
Max open files            65536                65536                files

# GCS 实际已用 FD 数量
$ ls /proc/74/fd | wc -l
65536                            ← 100% 打满！

# 容器级别的 FD 上限（当前 shell 继承的值）
$ ulimit -n
1048576                          ← 容器允许 100 万
```

**关键发现**：容器的 FD limit 是 1048576，但 GCS 进程只有 65536。原因是用户启动脚本中显式设置了 `ulimit -n 65536`，GCS 作为子进程继承了这个值。

### 第七步：分析 FD 被什么类型占用

```bash
$ ls /proc/74/fd | xargs -I{} readlink /proc/74/fd/{} | sed 's/:.*//' | sort | uniq -c | sort -rn
65504 socket          ← 99.95%！
   19 anon_inode      ← epoll fd
    3 /dev/null
    2 pipe
    2 gcs_server.out
    2 gcs_server.err
    1 event_EXPORT_NODE.log
    1 event_EXPORT_DRIVER_JOB.log
    1 event_EXPORT_ACTOR.log
    1 event_GCS.log
```

**几乎所有 FD 都是 socket（网络连接）**。

### 第八步：分析 socket 连接来源

```bash
# GCS 进程总 TCP 连接数
$ ss -tnp | grep ',pid=74,' | wc -l
65210

# 连接到多少个不同的 IP（节点）
$ ss -tn6p | grep ',pid=74,' | awk '{print $5}' | rev | cut -d: -f2- | rev | sort -u | wc -l
2246

# 每节点平均连接数
$ ss -tn6p | grep ',pid=74,' | awk '{print $5}' | rev | cut -d: -f2- | rev | sort | uniq -c | sort -rn | awk '{sum+=$1; count++} END{print "total_conns="sum, "unique_ips="count, "avg_per_ip="sum/count}'
total_conns=65210 unique_ips=2246 avg_per_ip=29.03
```

**关键数据**：
- GCS 与 **2246 个不同节点** 建立了连接
- 每个节点平均 **~29 条 gRPC 连接**
- 总计 65,210 条 ≈ 65,536 FD limit

### 第九步：分析每节点连接数分布

```bash
$ ss -tn6p | grep ',pid=74,' | awk '{print $5}' | rev | cut -d: -f2- | rev | sort | uniq -c | sort -rn | head -10
173 [::ffff:10.83.10.37]
172 [::ffff:10.83.10.23]
172 [::ffff:10.82.234.24]
171 [::ffff:10.82.238.167]
171 [::ffff:10.82.236.35]
170 [::ffff:10.83.8.213]
170 [::ffff:10.83.10.45]
...
```

连接数分布统计（"X 个节点有 Y 条连接"）：
```bash
$ ... | awk '{print $1}' | sort -n | uniq -c | sort -rn | head -10
168 个节点有 20 条连接
164 个节点有 22 条连接
148 个节点有 21 条连接
131 个节点有 23 条连接
127 个节点有 16 条连接
118 个节点有 19 条连接
112 个节点有 24 条连接
...
```

大部分节点与 GCS 维持 **15~31 条 gRPC 连接**。

### 第十步：检查其他系统指标

```bash
$ free -g
              total   used   free   shared  buff/cache  available
Mem:          1006     52     238    1       716         950

$ df -h /dev/shm
tmpfs   504G  1.6G  502G  1%  /dev/shm

$ cat /proc/74/status | grep -E '(VmRSS|VmSize|Threads)'
VmSize:  61791500 kB     ← 虚拟内存 ~59GB
VmRSS:   12242280 kB     ← 物理内存 ~12GB
Threads: 540             ← 540 个线程

$ ss -s
Total: 91,312
TCP:   91,232 (estab 90,445, closed 602, orphaned 0, timewait 302)
```

**关键发现**：
- Head 节点内存充足（总 1TB，已用 52GB）
- Object Store（/dev/shm）使用 1.6GB / 504GB
- GCS 进程本身 12GB 内存、540 线程
- 整个 head 节点 **90,445 条 ESTABLISHED TCP 连接**

### 第十一步：检查 Raylet 日志

```bash
$ tail -30 /tmp/ray/session_*/logs/raylet.out
[2026-05-16 14:15:27,199 W 992] (raylet) scheduling_class_util.cc:184:
  More than 13995 types of tasks seen, this may reduce performance.
[2026-05-16 14:16:57,813 W 992] (raylet) scheduling_class_util.cc:184:
  More than 14031 types of tasks seen, this may reduce performance.
```

**发现第二个问题**：14000+ 种 scheduling class（task 类型），会增加调度开销。

---

## 根因分析

### FD 耗尽的直接原因：节点规模过大

```
2,246 节点 × 每节点 ~29 条 gRPC 连接 = ~65,134 条 ≈ 65,536 FD limit
```

**是节点数量导致的，不是 task 数量直接导致的。**

### 每节点 ~29 条连接的构成

每个 worker 节点会与 GCS 建立多条 gRPC channel：

| 组件 | 连接数/节点 | 用途 |
|------|------------|------|
| Raylet → GCS | 3~5 | 资源上报（RaySyncer）、心跳、lease 请求、节点注册 |
| 每个 Worker 进程 → GCS | 2~3 | CoreWorker 注册、task event 上报、对象引用追踪 |
| Dashboard Agent → GCS | 1~2 | 指标采集 |
| Runtime Env Agent → GCS | 1~2 | 运行环境管理 |

如果每节点有 ~8 个 worker 进程：`5 + 8×3 = ~29`，**完全吻合实测数据**。

**CoreWorker 连接 GCS 的代码路径**：

```python
# python/ray/_private/worker.py:2604-2609
gcs_options = ray._raylet.GcsClientOptions.create(
    node.gcs_address,         # → "10.15.3.158:6379"
    node.cluster_id.hex(),
    allow_cluster_id_nil=False,
    fetch_cluster_id_if_nil=False,
)
# python/ray/_raylet.pyx:2806
CCoreWorkerProcess.Initialize(options)  # ← 建立到 GCS 的 gRPC channel
```

每个 Worker 进程启动时都会通过 `CCoreWorkerProcess.Initialize` 建立到 GCS 的 gRPC 连接。

### task 数量的间接影响

Raylet 日志中的 scheduling class 告警：

```cpp
// src/ray/common/scheduling/scheduling_class_util.cc:175-194
SchedulingClass SchedulingClassToIds::GetSchedulingClass(
    const SchedulingClassDescriptor &sched_cls) {
  absl::MutexLock lock(&mutex_);
  auto it = sched_cls_to_id_.find(sched_cls);
  if (it == sched_cls_to_id_.end()) {
    sched_cls_id = ++next_sched_id_;
    if (sched_cls_id > 100) {
      RAY_LOG_EVERY_MS(WARNING, 1000)
          << "More than " << sched_cls_id
          << " types of tasks seen, this may reduce performance.";
    }
    sched_cls_to_id_[sched_cls] = sched_cls_id;
  }
  return sched_cls_id;
}
```

每个 `SchedulingClassDescriptor` 是以下属性的唯一组合：
- `ResourceSet`（CPU、GPU、memory 需求）
- `FunctionDescriptor`（函数签名）
- `SchedulingStrategy`（调度策略）
- `LabelSelector`（标签选择器）

14000+ 种 scheduling class 意味着 Ray Data pipeline 中每个 task 有不同的资源需求或函数描述符组合。Raylet 为每种类型维护独立的调度队列，类型过多会增加遍历开销，但**不是 FD 耗尽的直接原因**。

---

## FD 耗尽对 Ray 作业的完整影响链分析

当 GCS 进程的 `ulimit -n` 达到上限后，内核拒绝 `socket()` 和 `accept()` 系统调用（返回 `EMFILE`）。由于 gRPC 完全依赖 socket 进行通信，这会级联影响所有依赖 GCS 的子系统。

### 影响总览（级联故障图）

```
GCS FD 耗尽 (socket()/accept() 返回 EMFILE)
    │
    ├─→ [影响1] gRPC Server 无法 accept 新 TCP 连接
    │       │
    │       ├─→ ray status / ray job stop 等 CLI 超时
    │       ├─→ 新 Worker 注册失败
    │       └─→ RequestWorkerLease RPC 超时 → task 卡在 PENDING_NODE_ASSIGNMENT
    │
    ├─→ [影响2] RaySyncer 双向流断开，资源广播停止
    │       │
    │       ├─→ Raylet 持有过期的集群资源视图
    │       ├─→ 调度器找不到可用节点（GetBestSchedulableNode 返回 Nil）
    │       └─→ 每 2 秒重连一次，持续失败
    │
    ├─→ [影响3] 健康检查出向 RPC 失败
    │       │
    │       ├─→ health_check_remaining_ 递减到 0
    │       ├─→ 健康节点被误判为死亡（FailNode）
    │       └─→ 该节点上的 Actor/Task 被强制 kill
    │
    ├─→ [影响4] GCS 资源负载拉取失败
    │       │
    │       └─→ Autoscaler 无法获取最新负载 → 无法正确扩缩容
    │
    └─→ [影响5] Ray Data StreamingExecutor 停滞
            │
            ├─→ 提交的 task 永远无法被调度
            ├─→ ray.wait() 持续返回空 → 无前进进度
            └─→ Pipeline 完全卡住
```

---

### 影响1：gRPC Server 无法接受新连接

**原理**：GCS 的 gRPC server 在端口 6379 上监听。当新客户端发起 TCP 连接时，内核完成三次握手后需要 `accept()` 分配一个新的 socket FD。FD 用完后 `accept()` 返回 `EMFILE`。

**代码路径**：

```cpp
// src/ray/rpc/grpc_server.cc:131
server_ = builder.BuildAndStart();  // 开始监听 6379

// src/ray/rpc/grpc_server.cc:192-258
// gRPC 的事件循环，处理新进来的 RPC 请求
void GrpcServer::PollEventsFromCompletionQueue(int index) {
  void *tag;
  bool ok;
  while (true) {
    auto status = cqs_[index]->AsyncNext(&tag, &ok, deadline);
    if (ok) {
      switch (server_call->GetState()) {
      case ServerCallState::PENDING:
        server_call->HandleRequest();  // ← 新 RPC 在此处理
        break;                         //   FD 满后 accept() 失败，
      }                                //   这个回调不再触发
    }
  }
}
```

gRPC 底层（`tcp_server_posix.cc:378`）检测到 `accept()` 失败后输出告警并每秒重试：
```
File descriptor limit reached. Retrying.
```

同时 `socket_utils_common_posix.cc:477` 在尝试创建出向连接时也报错：
```
socket(10, 1, 0) returned -1 with error: |Too many open files|.
This process might not have a sufficient file descriptor limit
for the number of connections grpc wants to open.
```

**直接后果**：
- `ray status`、`ray job stop` 等 CLI 命令连接 GCS 超时
- 新启动的 Worker 进程无法注册到 GCS（`RegisterNode` RPC 失败）
- Raylet 的 `RequestWorkerLease` RPC 无法到达 GCS

---

### 影响2：RaySyncer 资源广播中断

**原理**：每个 Raylet 通过 RaySyncer 与 GCS 建立 gRPC 双向流（BiDi Streaming），用于实时同步集群资源视图。FD 耗尽后，断开的连接无法重建。

**BiDi 流建立过程**：

```cpp
// src/ray/ray_syncer/ray_syncer_client.cc:25-54
// Raylet 侧发起 BiDi 流连接到 GCS
RayClientBidiReactor::RayClientBidiReactor(...) {
  client_context_.AddMetadata("node_id", NodeID::FromBinary(local_node_id).Hex());
  stub_->async()->StartSync(&client_context_, this);  // ← 发起 BiDi 流，需要 socket FD
  AddHold();    // 防止流被过早关闭
  StartPull();  // 开始接收消息
}

// 流断开时的回调
void RayClientBidiReactor::OnDone(const grpc::Status &status) {
  io_context_.dispatch([this, status]() {
    cleanup_cb_(this, !status.ok());  // ← status 不 ok 时触发重连
    self_ref_.reset();
  }, "");
}
```

**连接断开后的重连逻辑**：

```cpp
// src/ray/ray_syncer/ray_syncer.cc:81-124
void RaySyncer::Connect(const std::string &node_id,
                        std::shared_ptr<grpc::Channel> channel) {
  auto reactor = std::make_shared<RayClientBidiReactor>(
      /* cleanup_cb */
      [this, channel](RaySyncerBidiReactor *bidi_reactor, bool restart) {
        sync_reactors_.erase(iter);       // 从活跃连接列表中移除
        if (restart) {
          execute_after(io_context_,
            [this, remote_node_id, channel]() {
              RAY_LOG(INFO) << "Connection to the node was broken, reconnecting.";
              Connect(remote_node_id, channel);  // ← 2秒后重连，但 FD 满仍失败
            },
            std::chrono::milliseconds(2000));    // ← 每 2 秒重试一次
        } else {
          node_state_->RemoveNode(remote_node_id);  // 不重连则移除节点状态
        }
      },
  );
  reactor->StartCall();  // ← 发起 gRPC BiDi 流，需要新 socket
}
```

**资源广播变成空操作**：

```cpp
// src/ray/ray_syncer/ray_syncer.cc:209-224
void RaySyncer::BroadcastMessage(std::shared_ptr<const RaySyncMessage> message) {
  io_context_.dispatch([this, message] {
    if (!node_state_->ConsumeSyncMessage(message)) {
      return;
    }
    for (auto &reactor : sync_reactors_) {
      reactor.second->PushToSendingQueue(message);
      // ← 当 sync_reactors_ 为空时，这个循环体不执行
      //   资源更新不再传递到任何 Raylet
    }
  }, "RaySyncer.BroadcastMessage");
}
```

当所有 reactor 都因 FD 耗尽断开后，`sync_reactors_` 变为空 map，`BroadcastMessage` 遍历空集合，资源更新不再传递到任何 Raylet。

**GCS 侧接收资源更新的逻辑也受影响**：

```cpp
// src/ray/gcs/gcs_resource_manager.cc:37-57
void GcsResourceManager::ConsumeSyncMessage(
    std::shared_ptr<const rpc::syncer::RaySyncMessage> message) {
  io_context_.dispatch([this, message] {
    if (message->message_type() == syncer::MessageType::RESOURCE_VIEW) {
      syncer::ResourceViewSyncMessage resource_view_sync_message;
      resource_view_sync_message.ParseFromString(message->sync_message());
      UpdateFromResourceView(NodeID::FromBinary(message->node_id()),
                             resource_view_sync_message);
    }
  }, "GcsResourceManager::ConsumeSyncMessage");
}

// src/ray/gcs/gcs_resource_manager.cc:129-145
void GcsResourceManager::UpdateFromResourceView(
    const NodeID &node_id,
    const syncer::ResourceViewSyncMessage &resource_view_sync_message) {
  if (node_id == local_node_id_) { return; }
  cluster_resource_manager_.UpdateNode(
      scheduling::NodeID(node_id.Binary()), resource_view_sync_message);
  UpdateNodeResourceUsage(node_id, resource_view_sync_message);
}
```

Syncer 断开后 `ConsumeSyncMessage` 不再被调用，`cluster_resource_manager_` 和 `node_resource_usages_` 持有过期数据。

**直接后果**：
- Raylet 收不到其他节点的资源释放通知
- Raylet 的 `ClusterResourceManager` 仍认为远端节点资源已满
- 调度器做出错误决策：明明有空闲资源，却认为无节点可用

---

### 影响3：健康检查失败 → 节点被误判死亡

**原理**：GCS 的 `HealthCheckManager` 定期向每个 Raylet 发起 gRPC 健康检查。这些**出向**请求也需要 socket FD。FD 满后，健康检查 RPC 创建连接失败，连续失败超过阈值后节点被宣告死亡。

**代码路径**：

```cpp
// src/ray/gcs/gcs_health_check_manager.cc:122-226
void GcsHealthCheckManager::HealthCheckContext::StartHealthCheck() {
  auto context = std::make_shared<grpc::ClientContext>();
  auto response = std::make_shared<HealthCheckResponse>();

  const auto deadline = now + absl::Milliseconds(manager->timeout_ms_);
  context->set_deadline(absl::ToChronoTime(deadline));

  // 发起健康检查 RPC（出向连接，需要 socket FD）
  stub_->async()->Check(context_ptr, &request_, response_ptr,
    [this, ...](::grpc::Status status) {
      gcs_health_check_manager->io_service_.post([this, status, response]() {

        if (status.ok() && response->status() == HealthCheckResponse::SERVING) {
          // 健康检查通过，重置计数器
          health_check_remaining_ = mgr->failure_threshold_;
        } else {
          // 健康检查失败（FD 满导致 gRPC 无法建连，status 不 ok）
          --health_check_remaining_;
          RAY_LOG(WARNING) << "Health check failed for node " << node_id_
              << ", remaining checks " << health_check_remaining_;
        }

        if (health_check_remaining_ == 0) {
          // 连续失败次数达到阈值，宣告节点死亡！
          mgr->FailNode(node_id_);
          delete this;
        } else {
          // 下一轮检查
          timer_.expires_from_now(boost::posix_time::milliseconds(mgr->period_ms_));
          timer_.async_wait([this](auto) { StartHealthCheck(); });
        }
      }, "HealthCheck");
    });
}
```

**节点被判死后的处理链**：

```cpp
// src/ray/gcs/gcs_health_check_manager.cc:83-91
void GcsHealthCheckManager::FailNode(const NodeID &node_id) {
  RAY_LOG(WARNING) << "Node is dead because the health check failed.";
  on_node_death_callback_(node_id);  // ← 触发 GcsNodeManager::OnNodeFailure
  health_check_contexts_.erase(iter);
}

// src/ray/gcs/gcs_node_manager.cc:680-717
void GcsNodeManager::OnNodeFailure(const NodeID &node_id, ...) {
  auto maybe_node = GetAliveNodeFromCache(node_id);
  if (maybe_node.has_value()) {
    rpc::NodeDeathInfo death_info = InferDeathInfo(node_id);
    auto node = RemoveNodeFromCache(
        node_id, death_info, rpc::GcsNodeInfo::DEAD, current_sys_time_ms());
    AddDeadNodeToCache(node);
    // 持久化到存储，然后通过 PubSub 广播节点死亡事件
    gcs_table_storage_->NodeTable().Put(node_id, *node, {
      [this, node_id, ...](const Status &status) {
        WriteNodeExportEvent(*node, false);
        PublishNodeInfoToPubsub(node_id, node_info_delta);
      }, io_context_
    });
  }
}
```

`OnNodeFailure` 的后果：
1. 将节点标记为 **DEAD**
2. 从活跃节点缓存中移除
3. 广播节点死亡事件（通过 PubSub）
4. 该节点上所有 **Actor 被标记为 DEAD**
5. 该节点上正在运行的 **task 被标记为失败**
6. 如果有 Placement Group 使用该节点，PG 进入重调度

**你的集群配置**：
```json
{
  "health_check_period_ms": 10000,       // 每 10 秒检查一次
  "health_check_failure_threshold": 10   // 连续 10 次失败后判死
}
```

即 FD 耗尽后 **~100秒** 健康节点可能被误判死亡。但由于你的配置比较宽松（10 次阈值），加上**已建立的连接可能仍能工作**（FD 耗尽只影响新连接创建），所以你的场景中没有出现大规模误判（主要表现为调度阻塞而非节点死亡）。

> **注意**：已建立的 TCP 连接不需要新的 FD，可以继续收发数据。FD 耗尽只阻止**新连接**的创建。因此如果健康检查使用的连接已经建立，它们可能仍然正常工作。

---

### 影响4：GCS 定期资源负载拉取失败

**原理**：GCS 通过定时器周期性地从每个 Raylet 拉取资源负载信息，用于 Autoscaler 决策。这需要 GCS 主动连接到 Raylet，也需要 socket FD。

```cpp
// src/ray/gcs/gcs_server.cc:391-437
void GcsServer::InitGcsResourceManager(const GcsInitData &gcs_init_data) {
  // ...
  periodical_runner_->RunFnPeriodically(
      [this] {
        for (const auto &alive_node : gcs_node_manager_->GetAllAliveNodes()) {
          auto remote_address = rpc::RayletClientPool::GenerateRayletAddress(
              alive_node.first,
              alive_node.second->node_manager_address(),
              alive_node.second->node_manager_port());
          // ← 需要 socket FD 来连接 Raylet
          auto raylet_client = raylet_client_pool_.GetOrConnectByAddress(remote_address);
          raylet_client->GetResourceLoad(
            [this](auto &status, auto &&load_and_usage) {
              if (status.ok()) {
                gcs_resource_manager_->UpdateResourceLoads(load_and_usage.resources());
                gcs_autoscaler_state_manager_->UpdateResourceLoadAndUsage(...);
              } else {
                RAY_LOG_EVERY_N(WARNING, 10)
                    << "Failed to get the resource load: " << status.ToString();
              }
            });
        }
      },
      RayConfig::instance().gcs_pull_resource_loads_period_milliseconds(),
      "RayletLoadPulled");
}
```

FD 满后 `GetOrConnectByAddress` 无法创建新连接，`GetResourceLoad` RPC 失败。Autoscaler 无法获取准确的负载数据。

---

### 影响5：Raylet 调度决策失败 → Task 卡在 Waiting for scheduling

**原理**：Raylet 的 `ClusterLeaseManager` 收到 task 调度请求后，调用 `GetBestSchedulableNode` 在集群资源视图中查找可用节点。由于资源视图过期（影响2），找不到节点，task 留在待调度队列中。

**代码路径**：

```cpp
// src/ray/raylet/scheduling/cluster_lease_manager.cc:196-244
void ClusterLeaseManager::ScheduleAndGrantLeases() {
  TryScheduleInfeasibleLease();
  for (auto shapes_it = leases_to_schedule_.begin(); ...) {
    auto &work_queue = shapes_it->second;
    for (auto work_it = work_queue.begin(); work_it != work_queue.end();) {
      const std::shared_ptr<internal::Work> &work = *work_it;
      RayLease lease = work->lease_;

      // 基于（过期的）集群资源视图做调度决策
      auto scheduling_node_id = cluster_resource_scheduler_.GetBestSchedulableNode(
          lease.GetLeaseSpecification(),
          /*preferred_node_id*/ ...,
          /*exclude_local_node*/ false,
          /*requires_object_store_memory*/ false,
          &is_infeasible);

      if (scheduling_node_id.IsNil()) {
        // 找不到可用节点！task 留在队列中
        // task 状态保持为 PENDING_NODE_ASSIGNMENT
        RAY_LOG(DEBUG) << "No node found to schedule a lease";
        break;  // 跳过这个 shape 的所有 task
      }

      ScheduleOnNode(node_id, work);
    }
  }
}
```

**`ScheduleAndGrantLeases` 正常被触发的路径**：

```cpp
// src/ray/raylet/node_manager.cc:2982-3001
void NodeManager::ConsumeSyncMessage(
    std::shared_ptr<const syncer::RaySyncMessage> message) {
  if (message->message_type() == syncer::MessageType::RESOURCE_VIEW) {
    syncer::ResourceViewSyncMessage resource_view_sync_message;
    resource_view_sync_message.ParseFromString(message->sync_message());
    NodeID node_id = NodeID::FromBinary(message->node_id());
    const bool capacity_updated = ResourceCreateUpdated(node_id, resources);
    const bool usage_update = UpdateResourceUsage(node_id, resource_view_sync_message);
    if (capacity_updated || usage_update) {
      cluster_lease_manager_.ScheduleAndGrantLeases();
      // ← 资源变化时触发重调度
    }
  }
}
```

**关键点**：`GetBestSchedulableNode` 返回 `Nil` 时，task 留在 `leases_to_schedule_` 队列中。只有当 `ScheduleAndGrantLeases()` 再次被调用（通常由 `ConsumeSyncMessage` 收到资源更新触发）时才会重新尝试。

但 RaySyncer 已断开 → `ConsumeSyncMessage` 不再被调用 → **调度循环永远无法推进** → 调度器"冻结"。

---

### 影响6：Ray Data StreamingExecutor 停滞

**原理**：Ray Data 的 `StreamingExecutor` 在 Driver 上运行调度循环，通过 `ray.wait()` 等待 task 完成。当所有 task 都卡在调度阶段时，没有 task 完成，循环空转。

```python
# python/ray/data/_internal/execution/streaming_executor.py:569-665
def _scheduling_loop_step(self, topology: Topology) -> bool:
    self._resource_manager.update_usages()

    # 1. 等待已提交 task 完成（100ms 超时）
    errored_blocks_per_op, _ = process_completed_tasks(
        topology, self._backpressure_policies, self._max_errored_blocks,
    )

    # 2. 选择下一个要执行的算子
    while True:
        op = select_operator_to_run(
            topology, self._resource_manager, self._backpressure_policies,
            ensure_liveness=self._consumer_idling(), ranker=self._ranker,
        )
        if op is None:
            break
        topology[op].dispatch_next_task()

# python/ray/data/_internal/execution/streaming_executor_state.py:482-545
def process_completed_tasks(topology, backpressure_policies, max_errored_blocks):
    active_tasks = {}
    for op, state in topology.items():
        for task in op.get_active_tasks():
            active_tasks[task.get_waitable()] = (state, task)

    if active_tasks:
        ready, _ = ray.wait(
            list(active_tasks.keys()),
            num_returns=len(active_tasks),
            fetch_local=False,
            timeout=0.1,  # ← 100ms 超时
        )
    # ready 为空 → 没有 task 完成 → 没有输出 → 下游算子饥饿
```

**结果**：
- `ray.wait()` 每 100ms 超时一次，返回空列表
- `select_operator_to_run()` 因为资源预算已被未完成的 task 占满，返回 `None`
- 整个 pipeline **卡死，无前进进度，但不会主动报错**（只是无限等待）

---

### 影响对比总结

| 影响 | 严重程度 | 用户可见现象 | 根本原因 | 代码位置 |
|------|----------|-------------|---------|---------|
| gRPC 无法 accept | 中 | CLI 超时 | `accept()` 返回 EMFILE | `grpc_server.cc:192` |
| 资源广播中断 | **致命** | task 卡在 Waiting for scheduling | Syncer 断开，资源视图过期 | `ray_syncer.cc:209` |
| 节点误判死亡 | 高 | Actor 被 kill、task 失败 | 健康检查 RPC 失败 | `gcs_health_check_manager.cc:168` |
| 调度器冻结 | **致命** | 新 task 永远无法被调度 | 无资源更新触发重调度 | `cluster_lease_manager.cc:196` |
| 资源拉取失败 | 中 | Autoscaler 无法正确决策 | 出向连接创建失败 | `gcs_server.cc:404` |
| Ray Data 停滞 | 高 | Pipeline 无进度 | ray.wait 空转 | `streaming_executor.py:569` |

---

## 端口与网络影响详解

### GCS 端口 6379 的影响

GCS 在端口 6379 监听 gRPC 请求。FD 耗尽后：

| 影响 | 表现 | 后果 |
|------|------|------|
| **无法 accept 新连接** | `Recv-Q` 积压（实测 129）、新客户端连接超时 | `ray status`、新 worker 注册失败 |
| **无法创建出向连接** | GCS 主动推送（资源广播、健康检查、pub/sub）失败 | Raylet 收不到最新资源视图 |
| **现有连接不受影响** | 已建立的 65504 条连接仍可通信 | 部分节点仍能工作，但无法扩展 |

### 对调度链路的具体影响

```
正常调度流程：
Driver 提交 task → 本地 Raylet → (通过 GCS/RaySyncer 获取资源视图) → 选择目标节点 → 目标 Raylet 执行

FD 耗尽后：
Driver 提交 task → 本地 Raylet → GCS 无法广播资源更新 → Raylet 认为没有可用资源
                                  → GetBestSchedulableNode 返回 Nil
                                  → task 卡在 PENDING_NODE_ASSIGNMENT（"Waiting for scheduling"）
```

### 受影响的关键端口和服务

| 端口 | 服务 | FD 耗尽后的影响 |
|------|------|----------------|
| 6379 (GCS RPC) | 集群元数据、调度协调、PubSub | 新调度请求阻塞，task 卡在 Waiting for scheduling |
| 8265 (Dashboard) | Ray Dashboard Web UI、Job API | Dashboard 数据不更新，job 提交/停止失败 |
| Raylet Node Manager Port | 节点间 Lease 请求和 Spillback | Spillback 失败，task 无法转移到远端节点 |
| Object Manager Port | 对象传输 | 不直接受 GCS FD 影响，但间接因调度失败而闲置 |

### TCP 连接状态全景

```bash
$ ss -s
Total: 91,312
TCP:   91,232 (estab 90,445, closed 602, orphaned 0, timewait 302)
```

整个 head 节点维持了 **90,445 条 ESTABLISHED TCP 连接**，其中 GCS 占 65,210 条（72%）。

---

## 容器级 vs 进程级 ulimit 的区别与观察方法

### 如何观察

| 级别 | 命令 | 本次实测值 | 含义 |
|------|------|-----------|------|
| 容器级（新 shell 默认值） | `ulimit -n` | 1048576 | 容器内新进程的默认 FD limit |
| 进程级（GCS 实际值） | `cat /proc/74/limits \| grep 'open files'` | 65536 | GCS 进程实际能打开的最大 FD 数 |
| 系统级 | `cat /proc/sys/fs/file-nr` | 99328 / 105500894 | 当前系统已打开 FD / 系统允许最大值 |

### 为什么 GCS 只有 65536

容器默认 `ulimit -n` 是 1048576，但用户在启动脚本中**显式设置了** `ulimit -n 65536`：

```bash
ulimit -n 65536; RAY_METRICS_EXPO...   # ← 启动 ray 之前降低了 FD limit
```

这个 shell 的 `ulimit -n` 被改为 65536，`ray start --head` 作为子进程继承了这个值，GCS Server 和 Raylet 也继承了 65536。

### ulimit -n 的影响范围

`ulimit -n` 在 `ray start` 之前设置，会影响该命令启动的所有子进程：

| 进程 | 是否受影响 | 当前 FD 用量 |
|------|-----------|-------------|
| GCS Server (PID 74) | ✅ 是 | 65,536 / 65,536（满了） |
| Head 节点 Raylet (PID 992) | ✅ 是 | 9,468 / 65,536（正常） |
| Dashboard Agent | ✅ 是 | FD 需求不大 |
| Worker 节点进程 | ❌ 否 | Worker 节点有自己的启动脚本和 ulimit |

---

## 解决方案

### 立即修复：调整 FD limit

**原始启动脚本**：
```bash
ulimit -n 65536; RAY_METRICS_EXPO...
```

**修改为**：
```bash
ulimit -n 1048576; RAY_METRICS_EXPO...
```

修改后**重启 Ray 集群**生效。

### 为什么设置为 1048576

| 考量 | 说明 |
|------|------|
| 容器 hard limit 已允许 | 1048576 不需要额外权限 |
| 内存开销几乎为零 | `ulimit -n` 只是上限，不预分配。每个 FD 内核开销 ~1KB |
| 安全性 | 只影响 head 节点上由该 shell 启动的进程（GCS、Raylet、Agent） |
| 一劳永逸 | 避免集群扩缩容后再次打满，无需反复调整 |

也可以选择保守值，但不推荐中间值（可能随集群扩容再次不够）：

| 方案 | 值 | 适用场景 |
|------|-----|---------|
| `ulimit -n 1048576` | **推荐** | 一劳永逸 |
| `ulimit -n 524288` | 保守 | 够用，留余量 |
| `ulimit -n 200000` | 可用 | 当前节点数够，扩容后可能不够 |
| 删掉 ulimit 设置 | 继承容器默认 1048576 | 最简单 |

### 长期优化建议

| 措施 | 优先级 | 说明 |
|------|--------|------|
| 提高 GCS FD limit | P0 | 改为 1048576，重启即可 |
| 监控 FD 使用率 | P1 | 添加 `ls /proc/$(pgrep gcs_server)/fd \| wc -l` 到监控 |
| 减少 scheduling class 数量 | P2 | 检查 Ray Data pipeline 是否给不同 task 设置了不同的 resources |
| 评估集群拆分 | P3 | 2246 节点是一个超大集群，考虑是否可以拆成多个小集群 |

---

## 其他排查中遇到的报错

### `ray job stop` 超时

```
[2026-05-16 14:36:22,939 W 45375 45375] rpc_client.h:153: Failed to connect to GCS at address 10.15.3.158:6379 within 5 seconds.
[2026-05-16 14:36:52,941 W 45375 45375] gcs_client.cc:205: Failed to get cluster ID from GCS server: TimedOut
```

**与 FD 耗尽完全是同一个问题**。GCS 无法 accept 新连接，所有需要连接 GCS 的操作全部超时。

### `Batch timer error: Success`

```
[2026-05-16 00:44:30,353 E 74 99] (gcs_server) ray_syncer_bidi_reactor_base.h:112: Batch timer error: Success
```

**与 FD 耗尽无关**，是 RaySyncer 的已知无害告警：
- 时间是 `00:44:30`（集群启动后 8 秒），FD 耗尽要到 `01:46` 才出现
- 错误信息自相矛盾：`error: Success`
- 是 boost::asio timer cancel 时的竞争条件误报
- 只在启动瞬间出现一次，不影响功能

---

## 排查命令速查表

```bash
# 1. 检查 GCS 是否存活及 CPU
ps aux | grep gcs_server | grep -v grep

# 2. 检查 GCS 错误日志
tail -50 /tmp/ray/session_latest/logs/gcs_server.err

# 3. 检查 FD limit 和使用量
cat /proc/$(pgrep -f gcs_server)/limits | grep 'open files'
ls /proc/$(pgrep -f gcs_server)/fd | wc -l

# 4. 检查容器级 ulimit（对比进程级）
ulimit -n

# 5. 分析 FD 类型分布
ls /proc/$(pgrep -f gcs_server)/fd | xargs -I{} readlink /proc/$(pgrep -f gcs_server)/fd/{} | sed 's/:.*//' | sort | uniq -c | sort -rn

# 6. 分析 TCP 连接数和来源节点数
ss -tn6p | grep "pid=$(pgrep -f gcs_server)" | wc -l
ss -tn6p | grep "pid=$(pgrep -f gcs_server)" | awk '{print $5}' | rev | cut -d: -f2- | rev | sort -u | wc -l

# 7. 每节点平均连接数
ss -tn6p | grep "pid=$(pgrep -f gcs_server)" | awk '{print $5}' | rev | cut -d: -f2- | rev | sort | uniq -c | sort -rn | awk '{sum+=$1; count++} END{print "total="sum, "nodes="count, "avg="sum/count}'

# 8. 查看端口积压
ss -tlnp | grep -E '(6379|8265)'
# Recv-Q > 0 说明连接请求在排队

# 9. 检查 Raylet 调度告警
tail -30 /tmp/ray/session_latest/logs/raylet.out | grep -i 'scheduling_class\|types of tasks'

# 10. GCS 进程详细状态
cat /proc/$(pgrep -f gcs_server)/status | grep -E '(VmRSS|VmSize|Threads)'

# 11. 全局 TCP 连接统计
ss -s

# 12. 系统级 FD 使用情况
cat /proc/sys/fs/file-nr
```

---

## 故障时间线

| 时间 | 事件 |
|------|------|
| 2026-05-16 00:44:22 | Ray 集群启动（启动脚本中 `ulimit -n 65536`） |
| 2026-05-16 00:44:30 | RaySyncer 初始化，出现无害的 `Batch timer error: Success` |
| 2026-05-16 01:46:21 | **首次出现** "File descriptor limit reached"，FD 开始耗尽 |
| 2026-05-16 01:46 ~ 14:17 | FD 持续满载，GCS 无法接受新连接，每秒刷错误日志（共 1454+ 次） |
| 2026-05-16 14:11 | `ray status` 确认 GCS 连接超时 |
| 2026-05-16 14:15 | 确认 GCS FD **65536/65536** 打满，99.95% 为 socket |
| 2026-05-16 14:17 | 确认 2246 节点 × 29 连接 = 65134 条，Raylet 有 14031 种 scheduling class |
| 2026-05-16 14:36 | `ray job stop` 也确认超时，与 FD 耗尽是同一问题 |

---

## 相关文档

- [Ray GCS 问题排查指南](./ray-gcs-troubleshooting-guide.md) — GCS CPU 高、Syncer 瓶颈分析
- [RaySyncer IO/CPU 高排查](./ray-syncer-io-cpu-high-troubleshooting.md) — Syncer 线程优化
- [Ray Data Task 状态分析](./ray_data_task_status_analysis.md) — "Waiting for scheduling" 状态含义
- [Ray Data 调度循环优化](./ray_data_schedule_loop_optimization.md) — StreamingExecutor 调度机制
