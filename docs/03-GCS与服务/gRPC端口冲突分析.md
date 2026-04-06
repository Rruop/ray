# Ray gRPC 端口冲突问题分析

## 1. 问题描述

Ray 作业运行时出现以下报错，Worker 进程因 gRPC 端口绑定失败而 crash：

    (pid=3366, ip=10.48.75.107) UNKNOWN:No address added out of total 1 resolved for '0.0.0.0:10014'
    ... Address already in use ... errno:98
    Check failed: server_ Failed to start the grpc server. The specified port is 10014.

同时在另一节点上也出现类似报错，端口为 10031：

    (pid=7093, ip=10.48.74.144) UNKNOWN:No address added out of total 1 resolved for '0.0.0.0:10031'

## 2. 影响

### 2.1 Worker crash

Worker 进程直接 crash（RAY_CHECK(server_) 失败），导致：
- 该 Worker 上的 task/actor 全部失败
- Raylet 会重新拉起 Worker，但如果端口仍冲突，会反复 crash
- 用户看到的表象是任务偶发失败

### 2.2 对 Actor 影响更严重

| 维度 | 普通 Task | Actor |
|------|----------|-------|
| Worker crash 后 | 任务重新调度到其他 Worker 即可 | Actor 需要重建，所有状态丢失 |
| 重试代价 | 低，无状态 | 高，__init__ 重新执行，内存状态全部清空 |
| 下游影响 | 调用方收到异常后重试 | 所有持有该 actor reference 的调用方都会收到 ActorDiedError |
| 端口冲突循环 | Worker crash -> Raylet 新建 Worker -> 新端口，容易恢复 | Actor 重建 -> 可能分配到同一端口 -> 再次 crash -> 反复重建 |
| 端口占用时间 | 短暂（执行完可释放复用） | 长期（Actor 存活期间持续占用） |
| 重启限制 | 无限制 | 受 max_restarts 配额约束，耗尽后永久死亡 |
| 调度灵活性 | 可换节点重试 | 受资源约束/placement group 限制，可能只能原地重建 |

Actor 重建时 Raylet 倾向于在同一节点重新拉起，如果该节点端口紧张，actor 会反复因端口冲突 crash，陷入循环：

    Actor 重建 -> 分配端口 -> bind 失败 -> worker crash -> Actor 重建 -> ...

而且 actor 的调用方会阻塞等待 actor 恢复，导致级联超时。普通 task 失败后调度器可以换节点，actor 则更容易卡在原地。

## 3. Ray gRPC 端口体系

### 3.1 节点内 gRPC Server 架构

一个节点上的 gRPC server 关系：

- Raylet gRPC Server（端口由 --node-manager-port 控制，默认随机）
  - 服务对象：GCS、其他 Raylet、其他 Worker
  - 职责：资源调度、worker 租赁、节点管理
- Worker gRPC Server（端口由 --min-worker-port/max-worker-port 控制，默认 10002-19999）
  - 服务对象：其他 Worker（跨 Worker 通信，如 actor 调用、对象传输）
  - 职责：接收 task 执行请求、对象状态查询
  - 每个Worker 进程有自己独立的 gRPC Server
- Raylet 与 Worker 之间用 Unix domain socket（IPC）通信，不走 gRPC
  - Raylet 分配端口给 Worker 是通过 IPC 完成的，不占用 worker-port

### 3.2 端口分配流程

    1. Worker 启动，通过 IPC 向 Raylet 注册
    2. Raylet 调用 GetNextFreePort() 分配一个端口
    3. Raylet 通过 IPC 把端口号返回给 Worker
    4. Worker 用该端口创建自己的 gRPC Server 并 bind
    5. Worker 把实际端口上报 Raylet -> Raylet 上报 GCS
    6. 其他 Worker 从 GCS 查到该端口，直接 gRPC 连接

报错发生在第 4 步：Raylet 分配了端口，Worker 拿着这个端口去 bind 时发现已被占用，直接 crash。

### 3.3 端口类型与默认值

| 端口类型 | 配置参数 | 默认值 |
|---------|---------|-------|
| Node Manager Port | --node-manager-port | 0（随机） |
| Object Manager Port | --object-manager-port | 0（随机） |
| GCS Server Port | --gcs-server-port | 6379 |
| Ray Client Server | --ray-client-server-port | 10001 |
| Min Worker Port | --min-worker-port | 10002 |
| Max Worker Port | --max-worker-port | 19999 |
| Dashboard Agent Listen Port | --dashboard-agent-listen-port | 52365 |

### 3.4 gRPC Server 实例关系

每个 Worker 进程有自己独立的 gRPC Server，绑定 Raylet 分配的端口。Raylet 自身也有自己的 gRPC Server，但两者完全独立。

从 core_worker_process.cc:248-258：

    auto core_worker_server =
        std::make_unique<rpc::GrpcServer>(WorkerTypeString(options.worker_type),
                                          assigned_port,   // raylet 分配的端口
                                          options.node_ip_address == "127.0.0.1");
    core_worker_server->RegisterService(...);
    core_worker_server->Run();  // 绑定端口，失败则 crash

## 4. 根因分析

### 4.1 CheckPortFree 的 TOCTOU 竞态（核心问题）

CheckPortFree 在 src/ray/util/network_util.cc:116-132：

    bool CheckPortFree(int family, int port) {
        io_context io_service;
        boost::system::error_code ec;
        if (family == AF_INET6) {
            socket = std::make_unique<tcp::socket>(io_service, tcp::v6());
            socket->bind(tcp::endpoint(tcp::v6(), port), ec);
        } else {
            socket = std::make_unique<tcp::socket>(io_service, tcp::v4());
            socket->bind(tcp::endpoint(tcp::v4(), port), ec);
        }
        socket->close();
        return !ec.failed();
    }

CheckPortFree 的检测方式：创建临时 TCP socket -> 尝试 bind -> 成功则端口空闲 -> close socket -> 返回结果。

核心问题：CheckPortFree 是"先检查再释放"——close 之后这个 socket 就没了，端口并没有被"占住"——从 CheckPortFree 返回到 Worker 实际 bind 之间，端口处于无保护状态，任何进程都可以抢占。这是典型的 TOCTOU (Time-of-Check to Time-of-Use) 竞态问题。

竞态时间线：

    T1: Raylet CheckPortFree(10031) -> 空闲 (临时 socket bind 后立即 close)
    T2: Raylet 将 10031 分配给 Worker A
    T3: 另一个进程抢先 bind 了 10031        <- 竞态发生
    T4: Worker A GrpcServer::Run() bind(10031) -> EADDRINUSE
    T5: RAY_CHECK 失败，Worker A 崩溃

### 4.2 初始化时不检查端口空闲

free_ports_ 队列在初始化时盲目入队，不做任何空闲检查（worker_pool.cc:146-161）：

    if (!worker_ports.empty()) {
        free_ports_ = std::make_unique<std::queue<int>>();
        for (int port : worker_ports) {
            free_ports_->push(port);  // 盲目入队，不检查
        }
    } else if (min_worker_port != 0) {
        free_ports_ = std::make_unique<std::queue<int>>();
        for (int port = min_worker_port; port <= max_worker_port; port++) {
            free_ports_->push(port);  // 盲目入队，不检查
        }
    }

### 4.3 CheckPortFree 只检查单一协议族

node_address_family_ 的取值由节点 IP 决定（worker_pool.cc:105）：

    node_address_family_(IsIPv6(node_address_) ? AF_INET6 : AF_INET)

节点 IP 是 IPv4 就只检查 AF_INET，不检查 IPv6 侧是否也被占用。

### 4.4 gRPC 禁用了 SO_REUSEPORT

从 grpc_server.cc:82：

    builder.AddChannelArgument(GRPC_ARG_ALLOW_REUSEPORT, 0);  // 禁用了 SO_REUSEPORT

### 4.5 关于 IPv4/IPv6 双栈问题的澄清

报错信息 "No address added out of total 1 resolved" 中的 "1 resolved" 说明 gRPC 只解析出 1 个地址（IPv4），不是 IPv4+IPv6 双栈问题。两个 Unable to configure socket 是 gRPC 对同一个地址的重试。

不过，如果系统上 bindv6only=0（Linux 默认），绑定 0.0.0.0:port 会隐式占用 IPv6 侧的同一端口，这可能加剧端口冲突。

### 4.6 完整的问题链

| 阶段 | 行为 | 问题 |
|------|------|------|
| 初始化 | free_ports_->push(port) | 不检查端口是否空闲 |
| 分配时 | CheckPortFree(AF_INET, 10031) | 只检查 IPv4，不检查 IPv6；且检查后立即释放端口 |
| Worker 实际绑定 | gRPC bind 10031 | 检查到绑定之间有时间差，端口可能已被抢占 -> bind 失败 -> crash |

## 5. 为什么 Ray 不默认使用 port=0

Ray 默认指定端口范围 (10002-19999) 的原因是端口可预测性——K8s/Pod 环境中需要提前开放防火墙规则。如果用 port=0，端口由 OS 随机分配，范围不可控，在受限网络环境（K8s NetworkPolicy、防火墙）下可能导致 Worker 间无法通信。这是安全/运维便利性 vs 端口冲突风险的权衡，Ray 选择了前者。

## 6. 为什么 port=0 能解决竞态

| 方式 | 流程 | 问题 |
|------|------|------|
| 指定端口 | Raylet CheckPortFree(临时 bind+close) -> 告诉 Worker 端口号 -> Worker bind | close 到 Worker bind 之间有窗口，端口可被抢占 |
| port=0 | Raylet 告诉 Worker port=0 -> Worker gRPC bind(0) -> OS 原子性地选一个空闲端口并直接占用 | 无窗口，bind 成功即持有 |

port=0 时，OS 内核在 bind 系统调用中原子地选择一个空闲端口并立即绑定，不存在"检查完又释放"的竞态窗口。

## 7. 为什么问题时有时无

### 7.1 节点 IPv6 配置不同

| 配置 | 行为 | 是否冲突 |
|------|------|---------|
| disable_ipv6=1 | gRPC 只解析到 AF_INET，只绑一次 IPv4 | 不冲突 |
| bindv6only=1 | IPv4 bind 不占 IPv6 端口，两次 bind 都成功 | 不冲突 |
| bindv6only=0（默认） | IPv4 bind 隐式占 IPv6，第二次 bind 失败 | 可能冲突 |

### 7.2 端口分配方式不同

| 方式 | 行为 |
|------|------|
| 指定 min-worker-port/max-worker-port | 分配具体端口号如 10031，可能触发竞态冲突 |
| 未指定端口范围 | port=0，gRPC bind(0) 时 OS 选端口，不存在冲突 |

### 7.3 端口占用时机不确定

CheckPortFree 通过和 Worker 实际 bind 之间的时间窗口，其他进程可能抢占该端口。并发 Worker 启动数量越多，竞态概率越大。

## 8. 排查方法

在报错节点上执行：

    ss -antlp | grep 10031        # 检查 TIME_WAIT
    sudo lsof -i :10031          # 检查端口占用
    ps aux | grep ray             # 检查残留 worker 进程
    sysctl net.ipv6.conf.all.disable_ipv6   # IPv6 配置
    sysctl net.ipv6.bindv6only               # 双栈配置

## 9. 解决方案

### 方案一：不指定端口范围（推荐）

    ray start --head --min-worker-port=0 --max-worker-port=0

注意：CLI 默认值是 --min-worker-port=10002 和 --max-worker-port=19999，所以必须显式传 0。

### 方案二：杀掉占用端口的进程

    sudo lsof -i :10014
    sudo kill -9 <PID>

### 方案三：清理残留 Ray 进程

    ray stop && ray start

### 方案四：调整 IPv6 配置

    sysctl -w net.ipv6.bindv6only=1    # IPv4/IPv6 独立绑定
    sysctl -w net.ipv6.conf.all.disable_ipv6=1  # 关闭 IPv6

### 方案五：扩大端口范围

    ray start --head --min-worker-port=10002 --max-worker-port=65535

## 10. 关键源码位置

| 文件 | 位置 | 功能 |
|------|------|------|
| src/ray/rpc/grpc_server.cc:82 | GRPC_ARG_ALLOW_REUSEPORT | 禁用 SO_REUSEPORT |
| src/ray/rpc/grpc_server.cc:133 | GrpcServer::Run() | gRPC 服务启动与 RAY_CHECK 失败点 |
| src/ray/raylet/worker_pool.cc:105 | node_address_family_ | 决定 CheckPortFree 检查哪个协议族 |
| src/ray/raylet/worker_pool.cc:146-161 | 端口池初始化 | 根据配置构建 free_ports_ 队列（盲目入队，不检查） |
| src/ray/raylet/worker_pool.cc:692-711 | GetNextFreePort() | 端口分配与空闲检测 |
| src/ray/raylet/worker_pool.cc:714-718 | MarkPortAsFree() | 端口回收 |
| src/ray/util/network_util.cc:116-132 | CheckPortFree() | 端口可用性检测（临时 bind + close） |
| src/ray/core_worker/core_worker_process.cc:248-258 | CreateCoreWorker() | Worker 接收端口并创建 gRPC Server |
