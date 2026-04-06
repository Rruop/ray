# fd 耗尽致 Worker 注册超时分析

## 一、问题概述

在 Ray Data 作业中,大量出现 `Failed to startup worker after retrying 5 times` 错误,导致 block 被静默丢弃、作业不收敛。根因是 **fd(文件描述符)耗尽**导致 Worker 进程无法完成注册。

本分析基于 KRay 源码(`local_lease_manager.cc`、`worker_pool.cc`、`raylet_ipc_client.cc`、`core_worker_process.cc`),详细拆解 fd 耗尽如何在不同阶段阻断 Worker 注册流程。

---

## 二、错误产生的 raylet 侧代码链路

"Failed to startup worker after retrying 5 times" 产生在 **raylet 进程**的 `LocalLeaseManager::PoppedWorkerHandler` 中(`local_lease_manager.cc:525-538`)。

### 2.1 从调度到报错的完整流程

```
LocalLeaseManager::GrantScheduledLeasesToWorkers()
  → worker_pool_.PopWorker(spec, callback)           // 请求 Worker
    → FindAndPopIdleWorker() → 无空闲 Worker
    → StartNewWorker()                                // 启动新进程
      → StartWorkerProcess() → fork Worker 进程
      → MonitorPopWorkerRequestForRegistration()      // 设置60秒超时
  → 60秒内 Worker 未注册
    → PopWorkerStatus::WorkerPendingRegistration
    → PoppedWorkerHandler(worker=nullptr, status=WorkerPendingRegistration)
      → IncrementPopWorkerRetries()
      → retries > pop_worker_max_retries(默认5)
        → CancelLeases("Failed to startup worker after retrying 5 times.")
```

### 2.2 关键代码段

**PopWorker 请求与回调**(`local_lease_manager.cc:555-563`):

```cpp
worker_pool_.PopWorker(spec, [this, ...](const std::shared_ptr<WorkerInterface> worker,
                                        PopWorkerStatus status, ...) -> bool {
    return PoppedWorkerHandler(worker, status, lease_id, ...);
});
```

**60秒超时定时器**(`worker_pool.cc:630-649`):

```cpp
void WorkerPool::MonitorPopWorkerRequestForRegistration(
    std::shared_ptr<PopWorkerRequest> pop_worker_request) {
    auto timer = std::make_shared<boost::asio::deadline_timer>(
        *io_service_,
        boost::posix_time::seconds(
            RayConfig::instance().worker_register_timeout_seconds()));  // 默认60秒

    timer->async_wait([timer, pop_worker_request, this](const auto e) mutable {
        auto &requests = state.pending_registration_requests;
        auto it = std::find(requests.begin(), requests.end(), pop_worker_request);
        if (it != requests.end()) {
            requests.erase(it);
            PopWorkerStatus status = PopWorkerStatus::WorkerPendingRegistration;
            PopWorkerCallbackAsync(pop_worker_request->callback_, nullptr, status);
        }
    });
}
```

**并行超时监控 — Kill 注册超时的进程**(`worker_pool.cc:553-627`):

```cpp
// MonitorStartingWorkerProcess,同样60秒超时
if (it->second.proc->IsAlive()) {
    it->second.proc->Kill();
}
RemoveWorkerProcess(state, worker_id);
starting_worker_timeout_callback_();
```

**超过5次重试 → 报错**(`local_lease_manager.cc:490-538`):

```cpp
if (!worker) {
    cluster_resource_scheduler_.GetLocalResourceManager().ReleaseWorkerResources(
        work->allocated_instances_);

    if (status == PopWorkerStatus::WorkerPendingRegistration) {
        cause = internal::UnscheduledWorkCause::WORKER_NOT_FOUND_REGISTRATION_TIMEOUT;
        work->IncrementPopWorkerRetries();
    }

    auto max_retries = RayConfig::instance().pop_worker_max_retries();  // 默认5
    if (max_retries >= 0 && work->GetPopWorkerRetries() > max_retries) {
        CancelLeases(...,
            rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_WORKER_STARTUP_FAILED,
            absl::StrCat("Failed to startup worker after retrying ",
                         RayConfig::instance().pop_worker_max_retries(),
                         " times."));
    } else {
        work->SetStateWaiting(cause);  // 还没超过5次,重新调度
    }
}
```

### 2.3 关键配置参数

| 参数 | 默认值 | 源码位置 | 作用 |
|---|---|---|---|
| `worker_register_timeout_seconds` | 60 | `ray_config_def.h:297` | Worker 注册超时阈值 |
| `pop_worker_max_retries` | 5 | `ray_config_def.h:306` | 注册超时最大重试次数 |
| `maximum_startup_concurrency_` | CPU 数 | `worker_pool.cc:507` | 同时启动的最大 Worker 进程数 |

---

## 三、Worker 注册的两步流程

Worker 进程注册到 raylet 分为**两步**,fd 耗尽可在不同步骤阻断:

```
Step1: Worker进程 → raylet (Unix Domain Socket IPC)
       "我是新 Worker,我要注册"

Step2: raylet 收到注册 → 分配端口 → 回复 Worker
       "你的 gRPC server 用这个端口"

Step3: Worker进程 → 启动 gRPC Server → 通知 raylet
       "我的 gRPC server 就绪了"
```

---

## 四、fd 耗尽阻断 Worker 注册的三条路径

### 4.1 三条路径总览

| | 路径B: Worker 进程 socket() | 路径C: CheckPortFree() socket() | 路径D: gRPC server 起不来 |
|---|---|---|---|
| **谁调 socket()** | Worker 进程 | raylet 进程 | Worker 进程 |
| **断在哪一步** | Step1(注册请求发不出去) | Step2(raylet 分配不出端口) | Step3(gRPC server 绑定失败) |
| **fd 耗尽在哪个进程** | Worker 进程自身 | raylet 进程 | Worker 进程自身 |
| **socket 类型** | Unix Domain Socket(AF_UNIX) | TCP socket(AF_INET,临时探测) | TCP socket(AF_INET,长期监听) |
| **占用端口** | 无(走文件路径) | 检测 free_ports_ 队列中的端口 | 绑定 assigned_port |
| **fd 生命周期** | 长连接,Worker 存活期间一直持有 | 临时,socket+bind+close 用完即释放 | 长连接,gRPC server 存活期间一直持有 |
| **Worker 进程知道吗** | 不知道,卡在 ConnectSocketRetry 无限重试 | 知道,收到 raylet 回复 "No available ports" | 知道,GrpcServer::Run 失败 |
| **raylet 知道吗** | 不知道,只看到 60 秒超时 | 知道,GetNextFreePort 返回错误 | 不知道,只看到 60 秒超时 |

### 4.2 路径B: Worker 进程 socket() 失败 — IPC 连不上 raylet

Worker 进程启动后,第一步是建立到 raylet 的 Unix Domain Socket 连接:

```cpp
// core_worker_process.cc:200-201
auto raylet_ipc_client = std::make_shared<ray::ipc::RayletIpcClient>(
    io_service_, options.raylet_socket, /*num_retries=*/-1, /*timeout=*/-1);
```

```cpp
// raylet_ipc_client.cc:46-50  构造函数
local_stream_socket socket(io_service);
Status s = ConnectSocketRetry(socket, address, num_retries=-1, timeout=-1);
//                                          ↑ 无限重试    ↑ 无限等待
// socket() 返回 -1 (EMFILE) → ConnectSocketRetry 一直重试
// fd 不释放 → 永远连不上 → Worker 进程卡死
// raylet 端完全不知道这个 Worker 的存在
```

Worker 进程卡住,raylet 那边只有 `MonitorPopWorkerRequestForRegistration` 的 60 秒定时器在倒计时。超时后收到 `PopWorkerStatus::WorkerPendingRegistration` → 进入 `PoppedWorkerHandler` 重试逻辑。

此路径不涉及任何 TCP 端口,靠文件系统路径(`/tmp/ray/session_xxx/raylet_socket`)寻址。占 1 个 fd,类型是 Unix Domain Socket,Worker 存活期间一直持有。

### 4.3 路径C: raylet 进程 CheckPortFree() socket() 失败 — 端口分配不了

Worker **成功**通过 IPC 连上了 raylet,raylet 收到注册请求:

```cpp
// node_manager.cc:1124-1125
case protocol::MessageType::RegisterClientRequest: {
    ProcessRegisterClientRequestMessage(client, message_data);
}
```

raylet 调 `RegisterWorker` → 分配端口:

```cpp
// worker_pool.cc:837-843
int port = 0;
Status status = GetNextFreePort(&port);   // raylet 进程内执行
if (!status.ok()) {
    send_reply_callback(status, /*port=*/0);  // 注册失败,回复 Worker
    return status;
}
```

`GetNextFreePort` 内部:

```cpp
// worker_pool.cc:720-731
for (int i = 0; i < current_size; i++) {
    *port = free_ports_->front();
    free_ports_->pop();
    if (CheckPortFree(node_address_family_, *port)) {
        // CheckPortFree 内部: socket() + bind() + close()
        // raylet 进程 fd 耗尽 → socket() 返回 EMFILE
        // → 无法检测端口是否空闲 → 返回 false
        return Status::OK();
    }
    free_ports_->push(*port);
}
return Status::Invalid("No available ports. Please specify a wider port range ...");
```

**关键:** `CheckPortFree` 每次检测都需要 `socket()` 系统调用。fd 耗尽时 `socket()` 返回 EMFILE,所有端口都被**误判为"占用"** → 返回 `Status::Invalid("No available ports")` → `RegisterWorker` 失败。

**端口本身可能是空闲的,只是 raylet 的检测手段失效了。** `CheckPortFree` 是临时性的探测:内部逻辑是 `socket() → bind(port) → close()`,整个过程 <1ms,fd 和端口都是瞬态的。fd 耗尽时它连探测的 socket 都创建不出来,就误以为所有端口都被占用了。

日志中 raylet 端 fd 耗尽的直接证据:

```
E0707 17:04:09 ... File descriptor limit reached. Retrying.
RPC error: failed to connect to all addresses; last error:
UNKNOWN: ipv4:10.80.93.102:14280: Too many open files
```

### 4.4 路径D: Worker 进程 gRPC Server 启动失败 — 绑定不了端口

Worker IPC 注册**成功了**,raylet 给了端口号。Worker 尝试启动 gRPC server:

```cpp
// core_worker_process.cc:248-258
auto core_worker_server = std::make_unique<rpc::GrpcServer>(
    WorkerTypeString(options.worker_type),
    assigned_port,      // raylet 给的端口
    ...);
core_worker_server->RegisterService(...);
core_worker_server->Run();   // gRPC 内部: socket() + bind() + listen()
// fd 耗尽 → socket() 失败 → gRPC server 起不来
```

gRPC server 启动后还需通知 raylet:

```cpp
raylet_ipc_client->AnnounceWorkerPortForWorker(core_worker_server->GetPort());
// 此步也可能因 fd 耗尽失败
```

raylet 不知道 Worker 的 gRPC server 失败了,因为 `AnnounceWorkerPort` 消息发不出去,只看到 60 秒超时。

### 4.5 三条路径完整因果链路图

```
fd 耗尽 (ulimit 太低 + 高并发 → 单节点数百 Worker)

┌─────────────────────────────────────────────────────────┐
│ 路径A: fork 新进程时 fd 耗尽(最极端)                      │
│                                                          │
│   StartProcess() → Process() 构造函数 → fork()           │
│   → ec.value()==24 → RAY_LOG(FATAL) → raylet 崩溃       │
│   (worker_pool.cc:700-702)                               │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│ 路径B: fork 成功,但 Worker 进程内 socket() 失败          │
│                                                          │
│   Worker进程启动                                          │
│     ↓                                                    │
│   RayletIpcClient 构造函数                               │
│     → ConnectSocketRetry() → socket() → -1 (EMFILE)      │
│     → num_retries=-1 无限重试但 fd 不释放 → 卡住         │
│     ↓ 60秒后                                             │
│   MonitorPopWorkerRequestForRegistration 超时             │
│     → PopWorkerStatus::WorkerPendingRegistration          │
│     → PoppedWorkerHandler(worker=nullptr)                 │
│     → IncrementPopWorkerRetries()                         │
│     → retries > 5 → "Failed to startup worker"            │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│ 路径C: IPC 连接成功,但 raylet 端端口分配失败              │
│                                                          │
│   Worker进程 → RegisterClient IPC → raylet 收到注册请求   │
│     → WorkerPool::RegisterWorker()                       │
│     → GetNextFreePort()                                  │
│       → CheckPortFree() → socket() → -1 (EMFILE)         │
│       → 所有端口都判定为"不空闲"                          │
│     → Status::Invalid("No available ports")               │
│     → send_reply_callback(status, port=0)                 │
│     → Worker 进程收到注册失败回复                         │
│     → 60秒内未完成完整注册 → 同路径B 超时链条             │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│ 路径D: 注册成功,但 gRPC Server 启动失败                   │
│                                                          │
│   Worker IPC 注册成功 → 分配到 port                       │
│     → GrpcServer::Run() → socket()+bind()+listen()        │
│     → socket() 返回 -1 (EMFILE) → gRPC server 起不来     │
│     → AnnounceWorkerPortForWorker() 无法发送              │
│     → 60秒超时 → 同路径B                                 │
└─────────────────────────────────────────────────────────┘
```

---

## 五、fd 与端口的关系详解

### 5.1 三条路径的 fd 和端口使用对比

|  | 路径B | 路径C | 路径D |
|---|---|---|---|
| **进程** | Worker 进程 | raylet 进程 | Worker 进程 |
| **socket 类型** | AF_UNIX | AF_INET(临时) | AF_INET(长期) |
| **占用端口** | 无 | 检测 free_ports_ 端口 | 绑定 assigned_port |
| **fd 生命周期** | 长连接 | 临时,用完即释放 | 长连接 |

### 5.2 单个 Worker 进程的 fd 消耗

| fd 用途 | 数量 | 类型 | 说明 |
|---|---|---|---|
| Unix Domain Socket → raylet | 1 | AF_UNIX | 控制面 IPC |
| gRPC server listening socket | 1 | TCP | 数据面,assigned_port |
| gRPC client → GCS | ~1-3 | TCP | 与 GCS 通信 |
| gRPC client → raylet RPC | ~1 | TCP | 与 raylet 的 gRPC 通道 |
| Plasma store client | 1-2 | 共享内存 fd | 与 object store 通信 |
| 日志文件 | 1-2 | 文件 fd | 写日志 |
| Python 运行时内部 | 若干 | 混合 | import cudf 等探测临时占 fd |

**单个 Worker 进程保守估计 ~10 个 fd**。

### 5.3 raylet 进程的 fd 消耗

| fd 用途 | 数量 | 说明 |
|---|---|---|
| IPC listening socket | 1 | 接受 Worker Unix Domain Socket 连接 |
| N 个已接受的 Worker IPC 连接 | **N** | 每个 Worker 1 个 fd |
| Node Manager gRPC server | 1 | 接受其他 raylet/Driver 的 RPC |
| gRPC client → GCS | ~1-3 | |
| gRPC client → 其他 raylet | ~数个 | 跨节点通信 |
| CheckPortFree 临时 fd | 瞬时1个 | 用完释放 |
| 日志/其他 | ~5 | |

**当 N=400 个 Worker 时,raylet 仅 IPC 连接就占 400 个 fd**,加上 gRPC 通道等轻松超过 1024(默认 ulimit -n)。

### 5.4 fd 表是 per-process 的,端口是协作关系

fd 表在进程间完全隔离:

```
┌─────────────────────────────────┐    ┌─────────────────────────────────┐
│        raylet 进程               │    │        Worker 进程               │
│                                 │    │                                 │
│  fd 0: stdin                    │    │  fd 0: stdin                    │
│  fd 1: stdout                   │    │  fd 1: stdout                   │
│  fd 2: stderr                   │    │  fd 2: stderr                   │
│  fd 3: IPC listening socket     │    │  fd 3: IPC → raylet (AF_UNIX)   │
│  fd 4: Worker1 IPC conn         │    │  fd 4: gRPC server socket      │
│  fd 5: Worker2 IPC conn         │    │    bind(52347)  ← assigned_port │
│  ...                            │    │  ...                            │
│  fd 1023: ← ulimit 上限         │    │  fd 1023: ← ulimit 上限         │
│                                 │    │                                 │
│  ulimit -n = 1024 (独立计数)    │    │  ulimit -n = 1024 (独立计数)    │
└─────────────────────────────────┘    └─────────────────────────────────┘
```

- raylet fd 耗尽 ≠ Worker fd 耗尽,反之亦然
- 两者是独立进程,fd 表完全隔离
- 但**同一根因**(并发量过大)会**同时**让两个进程都 fd 耗尽

### 5.5 路径 C 和路径 D 的端口协作关系

路径 C 和路径 D 操作的是**同一个端口号**,但场景不同:

```
t0: Worker --IPC--> raylet: "我要注册"

t1: raylet 执行 GetNextFreePort()
    → CheckPortFree(52347)    ← raylet 进程内 socket+bind+close (临时)
    → 成功,端口空闲
    → assigned_port = 52347

t2: raylet --IPC--> Worker: "用端口 52347"

t3: Worker 进程 GrpcServer::Run()
    → socket() + bind(52347) + listen()   ← Worker 进程内 (长期持有)
    → gRPC server 在 52347 端口上监听
```

raylet 在 t1 时刻替 Worker "试了一下"端口,试完就释放。Worker 在 t3 时刻才真正绑定并长期占用。两者是**先后协作**关系,不是冲突关系。

### 5.6 路径 C 和路径 D 互斥 — 不会同时发生

它们是同一条流水线上的两个步骤,C 在前 D 在后:

| 场景 | 路径C 能成功? | 路径D 能成功? | 说明 |
|---|---|---|---|
| raylet fd 充足,Worker fd 充足 | 能 | 能 | 正常情况 |
| **raylet fd 耗尽**,Worker fd 充足 | **不能** | 能(但走不到这步) | raylet 分配不出端口 |
| raylet fd 充足,**Worker fd 耗尽** | 能 | **不能** | raylet 给了端口,Worker 绑不上 |
| **两者都 fd 耗尽** | 不能 | 不能 | 实际场景 |

实际出现的场景:

```
场景1: Worker fd 先耗尽
  → 路径B(连 IPC 都建不起来)
  → 不会走到路径C和D

场景2: raylet fd 先耗尽
  → 路径C(端口分配失败)
  → Worker 收到注册失败回复
  → 不会走到路径D

场景3: 都还够 fd,但 Worker 启动 gRPC 时 fd 不够了
  → 路径B成功,路径C成功
  → 路径D(gRPC server 起不来)
```

---

## 六、raylet 侧代码优化建议

### 6.1 `GetNextFreePort` — 区分端口占用与 fd 耗尽

`CheckPortFree` 失败时应区分"端口被占用"和"socket 创建失败(EMFILE)",而非一律判定为端口占用,导致返回 "No available ports" 错误。

当前代码(`worker_pool.cc:720-731`)无法区分这两种完全不同的失败原因。

### 6.2 `MonitorPopWorkerRequestForRegistration` — 超时时增加诊断信息

超时时没有诊断信息。应检查当前进程 fd 使用量(`/proc/self/fd`),在超时日志中输出,帮助定位是 fd 限制还是其他原因。

当前代码(`worker_pool.cc:630-649`)只输出 `WorkerPendingRegistration`,无根因信息。

### 6.3 `PoppedWorkerHandler` — 引入指数退避 + spillback

当 `status == WorkerPendingRegistration` 时,应引入指数退避 + spillback 机制,而非简单的固定5次重试后取消。当前 `SetStateWaiting(cause)` 会让 lease 重新进入调度队列,但不会降低并发压力,只会加速 fd 耗尽。建议:

- 对 `WorkerPendingRegistration` 失败采用**指数退避重试**而非立即重试
- 达到重试上限后不是直接取消 lease,而是尝试 **spillback 到其他节点**(当前只在 `sched_cls_cap` 场景走 spillback)

### 6.4 `GrantScheduledLeasesToWorkers` — fairness 逻辑加入 fd 维度

当前 fairness 策略只考虑 CPU(`total_cpu_requests_ > total_cpus` 时才启用),但在 `--num-cpus 0.25` 场景下,CPU 总量可能远够用,而 **fd 才是瓶颈**。建议扩展 fairness/cap 机制,加入对 fd 使用量的感知。

### 6.5 `StartProcess` — fd 耗尽时降级而非 FATAL

当前 `worker_pool.cc:700-702` 中 `ec.value() == 24` 时直接 `RAY_LOG(FATAL)` crash raylet,应改为降级处理(如返回错误状态、触发 spillback),避免单个节点 fd 耗尽导致整个 raylet 崩溃。
