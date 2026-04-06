# Ray Dashboard NodeHead 子进程 CPU 100% 深度分析

> 集群：kml-hb2az1-l3-2 / lmserving / kml-task-661218
> Pod：kml-task-661218-record-15739495-prod-worker-0-8qhgk
> 问题日期：2026-05-22
> 关联组件：`ray-dashboard-NodeHead-0` (PID 455)

---

## 一、问题现象

### 1.1 Top 观察

```
PID   USER  PR  NI  VIRT    RES     SHR   S  %CPU  %MEM  TIME+       COMMAND
78    root  20  0   41.5g   9.0g    18812 S  493.3 1.8   35769:53    gcs_server
1243154 root 20 0  246.5g  38.2g   2.1g  S  133.3 7.6   278:11.85   python
270   root  20  0  534704  125144  38076 R  100.0 0.0   500:59.07   python
455   root  20  0  28.7g   2.3g    50924 S  100.0 0.4   8598:21     ray-dashboard-N
949   root  20  0  447.6g  4.0g    3.5g  S  26.7  0.8   2104:49     raylet
```

`ray-dashboard-N`（PID 455）持续占用 100% CPU（单核打满），累计 CPU 时间 8598 分钟（~143 小时），自 May 15 启动以来持续不断。

### 1.2 `top -H -p 455` 线程级观察

使用 `top -H -p 455` 查看线程，发现共 51 个线程，其中 TID 773 独占 ~87% CPU：

```
PID   USER  PR  NI  VIRT   RES    SHR   S  %CPU  %MEM  TIME+       COMMAND
773   root  20  0   28.7g  2.3g   50924 R  86.7  0.5   7852:12     ray-dashboard-N
455   root  20  0   28.7g  2.3g   50924 S  0.0   0.5   569:52.37   ray-dashboard-N
731   root  20  0   28.7g  2.3g   50924 S  0.0   0.5   6:32.83     ray-dashboard-N
732   root  20  0   28.7g  2.3g   50924 S  0.0   0.5   10:25.29    event_engine
734   root  20  0   28.7g  2.3g   50924 S  0.0   0.5   10:19.62    event_engine
735   root  20  0   28.7g  2.3g   50924 S  0.0   0.5   10:21.11    event_engine
...（共 16 个 event_engine 线程，每个约 10 分钟）
748   root  20  0   28.7g  2.3g   50924 S  0.0   0.5   0:07.34     lifeguard
753   root  20  0   28.7g  2.3g   50924 S  0.0   0.5   0:48.68     client.poll0
730   root  20  0   28.7g  2.3g   50924 S  0.0   0.5   0:12.08     grpc_global_tim
```

### 1.3 进程资源概况

```bash
$ cat /proc/455/status | grep -E 'Name|State|Threads|VmRSS|VmSize'
Name:   ray-dashboard-N
State:  S (sleeping)
VmSize: 30115420 kB    # ~28.7 GB 虚拟内存
VmRSS:   2340892 kB    # ~2.3 GB 物理内存
Threads: 51
```

---

## 二、`ray-dashboard-N` 是什么？

### 2.1 Dashboard 进程架构

Ray Dashboard 采用**主进程 + 多子进程（SubprocessModule）**架构：

```
DashboardHead (PID 273, 主进程)
├── ray-dashboard-MetricsHead-0  (PID 451)  ← CPU 0%
├── ray-dashboard-DataHead-0     (PID 452)  ← CPU 0.3%
├── ray-dashboard-EventHead-0    (PID 453)  ← CPU 0.2%
├── ray-dashboard-JobHead-0      (PID 454)  ← CPU 0.1%
├── ray-dashboard-NodeHead-0     (PID 455)  ← CPU 101% ★★★
├── ray-dashboard-ReportHead-0   (PID 456)  ← CPU 0.8%
├── ray-dashboard-ServeHead-0    (PID 457)  ← CPU 0.3%
├── ray-dashboard-StateHead-0    (PID 458)  ← CPU 1.2%
└── ray-dashboard-TrainHead-0    (PID 459)  ← CPU 0%
```

每个子进程通过 `multiprocessing.Process` 以 `spawn` 方式启动，拥有独立的 Python 解释器和 GIL。

### 2.2 进程名设置

进程名在 `python/ray/dashboard/subprocesses/module.py:242-243` 设置：

```python
ray._raylet.setproctitle(
    f"ray-dashboard-{module_name}-{incarnation} ({current_proctitle})"
)
```

- `module_name` = 模块类名（如 `NodeHead`、`StateHead`）
- `incarnation` = 重启计数器，从 0 开始
- 示例：`ray-dashboard-NodeHead-0`

子进程通过 `multiprocessing.Process` 创建（`python/ray/dashboard/subprocesses/handle.py:123-134`）：

```python
self.process = self.mp_context.Process(
    target=run_module,
    args=(self.module_cls, self.config, self.incarnation, child_conn),
    daemon=True,
    name=f"{self.module_cls.__name__}-{self.incarnation}",
)
```

### 2.3 各子模块职责

| 模块 | 代码位置 | 职责 |
|------|----------|------|
| `NodeHead` | `python/ray/dashboard/modules/node/node_head.py` | 节点状态、Actor 信息、物理资源统计 |
| `StateHead` | `python/ray/dashboard/modules/state/state_head.py` | State API（list tasks/actors/objects） |
| `JobHead` | `python/ray/dashboard/modules/job/job_head.py` | Job 提交与管理 |
| `ServeHead` | `python/ray/dashboard/modules/serve/serve_head.py` | Ray Serve 部署管理 |
| `ReportHead` | `python/ray/dashboard/modules/reporter/reporter_head.py` | 指标上报与 Prometheus 集成 |
| `DataHead` | `python/ray/dashboard/modules/data/data_head.py` | Ray Data 数据集管理 |
| `TrainHead` | `python/ray/dashboard/modules/train/train_head.py` | Ray Train 训练任务管理 |
| `MetricsHead` | `python/ray/dashboard/modules/metrics/metrics_head.py` | Grafana/Metrics 页面代理 |
| `EventHead` | （通过代码发现，实际存在） | 事件流处理 |

---

## 三、线程名为什么都是 `ray-dashboard-N`？

### 3.1 Linux 线程名 15 字符截断

Linux 内核对线程名（`task->comm`，即 `/proc/[pid]/task/[tid]/comm`）有 **TASK_COMM_LEN = 16** 字节的限制（含 `\0`），即最多显示 **15 个字符**。

```
原始进程名                    top/ps 截断后
─────────────────────────    ──────────────────
ray-dashboard-NodeHead-0     ray-dashboard-N     （15 字符）
ray-dashboard-MetricsHead-0  ray-dashboard-M
ray-dashboard-DataHead-0     ray-dashboard-D
ray-dashboard-StateHead-0    ray-dashboard-S
ray-dashboard-EventHead-0    ray-dashboard-E
```

这就是为什么 `top` 中看到的全是 `ray-dashboard-N` — 实际上 `N` 是 `NodeHead-0` 的第一个字符。

### 3.2 同一进程内多个同名线程

NodeHead-0 (PID 455) 内部有 3 个线程都显示为 `ray-dashboard-N`：

| TID | 显示名 | 累计时间 | 实际身份 |
|-----|--------|----------|----------|
| **773** | `ray-dashboard-N` | **7852 min** (87% CPU) | **asyncio 事件循环线程** — 真正干活的 |
| 455 | `ray-dashboard-N` | 569 min | 进程主线程 — 启动后让出给 asyncio loop |
| 731 | `ray-dashboard-N` | 6 min | ThreadPoolExecutor 工作线程 |

原因：Python 的 `threading.Thread` **不会自动设置 OS 级别的线程 comm**（需要显式调用 `prctl(PR_SET_NAME)`），所以所有 Python 线程都继承了进程名 `ray-dashboard-NodeHead-0`，经截断后统一显示为 `ray-dashboard-N`。

相比之下，gRPC 的 C++ 线程会主动调用 `pthread_setname_np()` 设置线程名，所以能看到区分：

```
event_engine      ← gRPC C++ I/O 线程（16 个）
grpc_global_tim   ← gRPC 定时器线程
client.poll0      ← gRPC 客户端轮询
lifeguard         ← gRPC 看门狗线程
PipeReaderThd     ← 管道读取线程
```

### 3.3 NodeHead 完整线程组成

```
NodeHead-0 进程 (PID 455) — 共 51 个线程
│
├─ Python 线程（3 个，受 GIL 约束）
│  ├── TID 773: asyncio 事件循环  ← 86.7% CPU，7852 min
│  ├── TID 455: 进程主线程         ← 0% CPU，569 min
│  └── TID 731: node_head_node_executor (TPE, max_workers=1)
│
├─ gRPC event_engine 线程（16 个，C++ 层，释放了 GIL）
│  └── TID 732-747: 各 ~10 min，处理网络 I/O
│
├─ gRPC 辅助线程
│  ├── TID 730, 752: grpc_global_timer
│  ├── TID 753: client.poll0
│  ├── TID 748: lifeguard
│  └── TID 749: gcs_client_io_server
│
├─ 管道通信线程
│  ├── TID 726, 728: PipeReaderThd
│  └── TID 727, 729: PipeDumpThd
│
└─ 其他
   ├── TID 750: default-executor
   └── TID 751: resolver-executor
```

---

## 四、Python 线程为什么不能并行？—— GIL 机制

### 4.1 GIL 限制

Python 的 GIL（Global Interpreter Lock）规定：**同一时刻只有一个 Python 线程能执行 Python 字节码**。

```
┌──────────────────────────────────────────────────────┐
│           NodeHead-0 进程 (PID 455)                   │
│                                                      │
│  ┌─ Python 线程（受 GIL 约束，同一时刻只能跑一个）──┐  │
│  │  TID 773: asyncio 事件循环  ← 持续持有 GIL      │  │
│  │  TID 455: 主线程            ← 等待 GIL           │  │
│  │  TID 731: TPE worker        ← 等待 GIL           │  │
│  └──────────────────────────────────────────────────┘  │
│                                                      │
│  ┌─ C/C++ 线程（释放了 GIL，可真正并行）────────────┐  │
│  │  TID 732-747: event_engine ×16  ← gRPC I/O      │  │
│  │  TID 730, 752: grpc_global_timer                 │  │
│  │  TID 753: client.poll0                           │  │
│  └──────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────┘
```

- **Python 线程**：protobuf→dict 转换、Actor 数据清理、DataOrganizer 等都是纯 Python 操作，受 GIL 限制无法并行
- **C/C++ 线程**：gRPC 的 `event_engine` 线程在 C++ 层运行时释放了 GIL，16 个线程可以真正并行做网络 I/O

### 4.2 代码中的 GIL 注释

Ray 代码中明确承认了这个限制。在 `python/ray/dashboard/modules/node/node_head.py:51-56`：

```python
# NOTE: Executor in this head is intentionally constrained to just 1 thread by
#       default to limit its concurrency, therefore reducing potential for
#       GIL contention
```

同样在 `python/ray/dashboard/head.py:41-43`：

```python
# NOTE: Executor in this head is intentionally constrained to just 1 thread by
#       default to limit its concurrency, therefore reducing potential for
#       GIL contention
```

多开 ThreadPoolExecutor worker 不但不能提升 Python 并行度，反而增加 GIL 争抢开销。

### 4.3 为什么拆成多进程？

这正是 Ray Dashboard 采用 SubprocessModule 架构的原因 — **每个进程有独立的 Python 解释器和 GIL**，进程间才能真正并行：

```
DashboardHead 进程 [GIL-1]
    ├── NodeHead-0 进程  [GIL-2]  ← 独立 GIL，独占 1 核
    ├── StateHead-0 进程 [GIL-3]  ← 独立 GIL
    ├── JobHead-0 进程   [GIL-4]  ← 独立 GIL
    └── ...
```

---

## 五、NodeHead CPU 100% 的根因

### 5.1 NodeHead 的常驻后台任务

NodeHead 的 `run()` 方法在 `node_head.py:753-764` 启动了多个常驻 asyncio 任务：

| 任务 | 频率 | 数据源 | CPU 开销 |
|------|------|--------|----------|
| `_update_actors()` | 持续订阅，批量 200 | GCS Actor 订阅流 | **最高** — 大量 protobuf→dict |
| `_update_nodes()` | 持续订阅 | GCS 节点订阅流 | 中 |
| `_update_node_stats()` | 每 15 秒 | 轮询所有 Raylet | 高 — 所有节点聚合 |
| `_update_node_physical_stats()` | 持续订阅 | GCS 资源使用订阅 | 中 — JSON + Pydantic |
| `_cleanup_actors()` | 每 1 秒 | 本地内存 | 低 |
| `DataOrganizer.organize()` | 每 15 秒 | 本地内存 | 中 — 遍历所有节点 |
| `DataOrganizer.purge()` | 每 600 秒 | 本地内存 | 低 |

### 5.2 CPU 消耗的主要来源

**TID 773（asyncio 事件循环线程）占 86.7% CPU 的原因：**

1. **`_update_actors()` — Actor 订阅的 protobuf 转换**
   - GCS 持续推送 Actor 变更事件
   - 每批 200 条，每条调用 `_actor_table_data_to_dict()` 转换
   - 初始化时 `_get_all_actors()` 一次性拉取所有 Actor（`node_head.py:670-682`）
   - 在大集群中 Actor 数量巨大，转换几乎不停歇

2. **`_update_node_stats()` — 节点统计聚合**
   - 每 15 秒向所有存活节点发 `GetNodeStats` RPC
   - 响应通过 `node_stats_to_dict()` 在 TPE 中转换
   - 分批 100 节点（`node_head.py:463`），但后处理仍是串行

3. **`_update_node_physical_stats()` — 资源统计解析**
   - 订阅 GCS 资源使用数据
   - 每次调用 `_parse_node_stats()`（JSON 解析 + `StatsPayload.parse_obj()` Pydantic 验证）
   - 在 `_node_executor` TPE 中执行，但受 GIL 限制

### 5.3 为什么是 100%？

```
时间线（示意）：

asyncio 事件循环（TID 773）
│
├── [0ms]   收到 Actor 订阅批次 → protobuf→dict 转换（CPU 密集）
├── [50ms]  转换完成 → 立即收到下一批次
├── [100ms] 转换完成 → 检查 node_stats 定时器到期
├── [110ms] 发送 GetNodeStats → 等待响应（短暂释放 CPU）
├── [120ms] 收到响应 → node_stats_to_dict 转换（CPU 密集）
├── [200ms] 又收到 Actor 订阅批次 → ...
└── 循环不停歇，CPU 永远忙碌
```

在大规模集群中，GCS 的事件流量大到 NodeHead 的单个 asyncio 线程**永远处理不完**，形成持续的 100% CPU。

---

## 六、NodeHead CPU 100% 对 GCS 的影响

### 6.1 NodeHead 的数据获取方式

```
NodeHead 的数据来源分析：

订阅模式（Push，GCS 主动推送）：
├── _update_actors()              → GCS Actor 订阅流
├── _update_nodes()               → GCS 节点订阅流
└── _update_node_physical_stats() → GCS 资源使用订阅流

轮询模式（Poll，NodeHead 主动拉取）：
└── _update_node_stats()          → 直接轮询 Raylet（不经过 GCS）

本地操作（无外部请求）：
├── _cleanup_actors()             → 本地内存清理
├── DataOrganizer.organize()      → 本地数据重组
└── DataOrganizer.purge()         → 本地数据清理
```

### 6.2 影响评估

**结论：NodeHead CPU 100% 基本不会增加 GCS 压力。**

| 方面 | 分析 |
|------|------|
| GCS 订阅（1-3） | Push 模式，GCS 按事件产生速率推送。NodeHead 处理慢不会让 GCS 推送更多。但 gRPC stream 可能因消费慢产生背压 |
| Raylet 轮询（4） | 直接连 Raylet，不经过 GCS。`_update_node_stats()` 每 15 秒一次，开销固定 |
| 本地操作（5-7） | 纯内存操作，无外部交互 |

### 6.3 因果关系

```
因果链（正确理解）：

集群规模大 / Actor 创建销毁频繁
    ↓
GCS 产生大量 Actor/Node 变更事件（GCS 是源头）
    ↓
GCS 通过订阅流推送到 NodeHead
    ↓
NodeHead 不停做 protobuf→dict 转换  ← CPU 100% 是果，不是因
    ↓
转换后数据缓存在内存中（2.3GB）供 Dashboard HTTP API 使用
```

NodeHead 的 100% CPU 是**大集群活跃度高的结果**，不是 GCS 压力的原因。

### 6.4 真正给 GCS 带压力的

当前集群中 `gcs_server` 本身就在高负载运行：

```
gcs_server  PID 78  → 493% CPU（~5 个核）
```

GCS 的高 CPU 更可能来自：
- 大量 Actor/Task 的状态管理和存储
- 大量 worker 的心跳处理
- 多个订阅者（NodeHead、StateHead、各节点的 DashboardAgent、Raylet）的流推送开销
- 任务事件（TaskEvent）的写入和查询

---

## 七、对业务的影响

### 7.1 影响矩阵

| 影响范围 | 程度 | 说明 |
|---------|------|------|
| **推理任务运行** | **无影响** | NodeHead 只占 1 个 CPU 核，机器总内存 515GB，CPU 资源充足 |
| **Dashboard UI 响应** | **可能变慢** | asyncio 事件循环饱和导致 HTTP API 请求排队等待处理 |
| **State API 查询** | **可能超时** | `ray list actors` / `ray list tasks` 走 StateHead，但 NodeHead 的数据延迟可能影响关联查询 |
| **内存占用** | **2.3GB 偏高** | 大量 Actor/Node 数据缓存在内存中 |
| **集群稳定性** | **无直接影响** | NodeHead 是 daemon 进程，挂掉会被自动重启（incarnation +1） |

### 7.2 系统整体负载对比

```
进程                 CPU%      内存       风险评估
────────────────    ─────    ────────    ──────────────
gcs_server          493%     9.0 GB     ★★★ 高风险 — 集群核心组件
python (worker)     133%     38.2 GB    正常 — 推理负载
python              100%     125 MB     待确认
ray-dashboard-N     100%     2.3 GB     ★★ 中风险 — Dashboard 可用性
raylet              26.7%    4.0 GB     正常
```

---

## 八、缓解建议

### 8.1 短期（无需重启）

NodeHead 的 CPU 100% **不影响推理业务**，如果 Dashboard 可用性不是关键需求，可暂时不处理。

### 8.2 中期（调整配置参数）

如果需要降低 NodeHead CPU 占用，可调整以下环境变量：

```bash
# 降低节点统计轮询频率（默认 15s → 60s）
export RAY_DASHBOARD_NODE_STATS_UPDATE_INTERVAL_SECONDS=60

# 增加 NodeHead 线程池大小（但受 GIL 限制，效果有限）
export RAY_DASHBOARD_NODE_HEAD_TPE_MAX_WORKERS=2

# 增加 StateHead 线程池大小
export RAY_DASHBOARD_STATE_HEAD_TPE_MAX_WORKERS=2
```

### 8.3 长期（代码优化方向）

1. **懒转换（Lazy Conversion）**：不在订阅回调中立即做 protobuf→dict，而是在 API 请求时按需转换
2. **增量更新**：Actor 订阅只更新变更的字段，避免全量 `_actor_table_data_to_dict()`
3. **C 扩展加速**：将热路径的 protobuf 转换用 C/Cython 实现，减少 Python 字节码开销
4. **限制缓存大小**：对 dead Actor 设置 TTL 淘汰，减少内存和遍历开销

### 8.4 关注 GCS 压力

当前 `gcs_server` 493% CPU 是更值得关注的问题，建议：
- 检查 GCS 的 Task Event 存储是否过多（参考 [GCS 僵尸 Entry 分析](./dashboard-task-count-low-eviction-zombie-entry-analysis.md)）
- 检查集群 Actor 总量和变更频率
- 考虑启用 GCS 的 Task Event 淘汰限制

---

## 九、诊断命令参考

```bash
# 1. 查看所有 dashboard 子进程
ps aux | grep ray-dashboard | grep -v grep

# 2. 查看 NodeHead 线程级 CPU 分布
top -H -b -n 1 -p $(pgrep -f "ray-dashboard-NodeHead")

# 3. 查看 NodeHead 进程详情
cat /proc/$(pgrep -f "ray-dashboard-NodeHead")/status | grep -E 'Name|State|Threads|VmRSS|VmSize'

# 4. 查看 NodeHead 线程数
ls /proc/$(pgrep -f "ray-dashboard-NodeHead")/task/ | wc -l

# 5. 所有 dashboard 子进程的线程详情
ps -eLf | grep ray-dashboard | grep -v grep

# 6. 持续监控 NodeHead CPU（每 2 秒刷新）
top -H -d 2 -p $(pgrep -f "ray-dashboard-NodeHead")
```

---

## 十、关键代码引用

| 文件 | 行号 | 说明 |
|------|------|------|
| `python/ray/dashboard/subprocesses/module.py` | 242-243 | 子进程名设置 (`setproctitle`) |
| `python/ray/dashboard/subprocesses/handle.py` | 123-134 | `multiprocessing.Process` 启动子进程 |
| `python/ray/dashboard/modules/node/node_head.py` | 51-56 | GIL 约束注释 |
| `python/ray/dashboard/modules/node/node_head.py` | 162-177 | ThreadPoolExecutor 创建（`_node_executor` + `_actor_executor`） |
| `python/ray/dashboard/modules/node/node_head.py` | 430-518 | `_update_node_stats()` — 节点统计轮询 |
| `python/ray/dashboard/modules/node/node_head.py` | 523-549 | `_update_node_physical_stats()` — 资源统计订阅 |
| `python/ray/dashboard/modules/node/node_head.py` | 551-617 | `_update_actors()` — Actor 订阅 |
| `python/ray/dashboard/modules/node/node_head.py` | 670-682 | `_get_all_actors()` — 初始全量拉取 |
| `python/ray/dashboard/modules/node/node_head.py` | 753-764 | `run()` — 启动所有后台任务 |
| `python/ray/dashboard/head.py` | 41-43 | Dashboard Head GIL 约束注释 |
| `python/ray/dashboard/state_aggregator.py` | 298-361 | `list_tasks()` — protobuf 转换热路径 |
