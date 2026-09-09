# Ray 文档索引

本目录包含 Ray 分布式计算框架的深度分析、故障排查、设计方案等文档，共 185 篇，按主题分为 9 个子目录。

## 目录结构

| 子目录 | 主题 | 文档数 |
|--------|------|--------|
| [01-架构与原理](01-架构与原理/) | 核心架构、生命周期、组件通信、调度机制等原理性深度分析 | 52 |
| [02-数据流与算子](02-数据流与算子/) | Ray Data 算子机制、Block 计算、数据读写、内部机制 | 23 |
| [03-GCS与服务](03-GCS与服务/) | GCS Server、Syncer、gRPC、线程架构、Redis 存储 | 13 |
| [04-Dashboard与指标](04-Dashboard与指标/) | Dashboard 显示机制、Prometheus 查询、指标体系 | 16 |
| [05-内存与OOM](05-内存与OOM/) | 内存管理、OOM 排查、CUDA OOM、cgroup 内存 | 11 |
| [06-故障排查](06-故障排查/) | 生产环境故障排查案例 | 35 |
| [07-设计方案](07-设计方案/) | 功能设计、优化方案、Cherry-Pick 计划、社区 PR 分析 | 20 |
| [08-视频推理](08-视频推理/) | 视频推理 Pipeline、共享 GPU | 1 |
| [09-工具与环境](09-工具与环境/) | 开发工具、测试配置、环境集成、KDev AgentRunner | 14 |

## 按主题快速查找

### Actor 相关
- Actor 创建与重启流程 → [01-架构与原理/Actor创建与重启流程.md](01-架构与原理/Actor创建与重启流程.md)
- Actor Kill 与退出机制 → [01-架构与原理/Actor-Kill与退出机制深度分析.md](01-架构与原理/Actor-Kill与退出机制深度分析.md)
- Actor-Unavailable 连接拒绝分析 → [06-故障排查/Actor-Unavailable连接拒绝分析.md](06-故障排查/Actor-Unavailable连接拒绝分析.md)
- ActorPool 自动伸缩 → [02-数据流与算子/ActorPool自动伸缩分析.md](02-数据流与算子/ActorPool自动伸缩分析.md)
- ActorPool 僵尸 Actor 不释放 → [06-故障排查/ActorPool僵尸Actor不释放排查.md](06-故障排查/ActorPool僵尸Actor不释放排查.md)
- GPU Actor 未释放根因 → [06-故障排查/GPU-Actor未释放根因分析.md](06-故障排查/GPU-Actor未释放根因分析.md)
- GPU Actor Pending 根因 → [06-故障排查/GPU-Actor-Pending根因分析.md](06-故障排查/GPU-Actor-Pending根因分析.md)
- GPU Actor 感知缩容设计 → [07-设计方案/GPU-Actor感知缩容设计.md](07-设计方案/GPU-Actor感知缩容设计.md)

### 调度相关
- Ray-Data 调度分析 → [01-架构与原理/Ray-Data调度分析.md](01-架构与原理/Ray-Data调度分析.md)
- Ray 任务执行与调度机制 → [01-架构与原理/Ray-任务执行与调度机制深度分析.md](01-架构与原理/Ray-任务执行与调度机制深度分析.md)
- 作业 Task 调度和状态变化 → [01-架构与原理/作业Task调度和状态变化详解.md](01-架构与原理/作业Task调度和状态变化详解.md)
- Schema 与调度优化 → [01-架构与原理/Schema与调度优化分析.md](01-架构与原理/Schema与调度优化分析.md)
- 调度阻塞分析 → [01-架构与原理/调度阻塞分析.md](01-架构与原理/调度阻塞分析.md)
- Spillback 与资源视图 → [01-架构与原理/Spillback与资源视图深度分析.md](01-架构与原理/Spillback与资源视图深度分析.md)
- RequestResources vs PendingDemands → [01-架构与原理/RequestResources与PendingDemands深度分析.md](01-架构与原理/RequestResources与PendingDemands深度分析.md)
- Custom-Resource 到 NodeLabelSchedulingStrategy 迁移 → [01-架构与原理/Custom-Resource到NodeLabelSchedulingStrategy迁移指南.md](01-架构与原理/Custom-Resource到NodeLabelSchedulingStrategy迁移指南.md)
- PendingDemands 内存与两层调度 → [06-故障排查/PendingDemands内存与两层调度.md](06-故障排查/PendingDemands内存与两层调度.md)
- Waiting-for-Scheduling 排查 → [06-故障排查/Waiting-for-Scheduling排查.md](06-故障排查/Waiting-for-Scheduling排查.md)
- 任务永久卡在 PENDING_NODE_ASSIGNMENT → [06-故障排查/Ray任务永久卡在PENDING_NODE_ASSIGNMENT的排查报告.md](06-故障排查/Ray任务永久卡在PENDING_NODE_ASSIGNMENT的排查报告.md)
- Driver 调度优化方案 → [07-设计方案/Driver调度优化方案.md](07-设计方案/Driver调度优化方案.md)
- RayWait 调度瓶颈导致 GPU 吞吐下降 → [06-故障排查/RayWait调度瓶颈导致GPU吞吐下降分析.md](06-故障排查/RayWait调度瓶颈导致GPU吞吐下降分析.md)

### Object / 内存相关
- Object 生命周期与恢复 → [01-架构与原理/Object生命周期与恢复深度分析.md](01-架构与原理/Object生命周期与恢复深度分析.md)
- Object-Store 内存深度分析 → [01-架构与原理/Object-Store内存深度分析.md](01-架构与原理/Object-Store内存深度分析.md)
- Ray 对象生命周期机制 → [01-架构与原理/Ray-对象生命周期机制深度分析.md](01-架构与原理/Ray-对象生命周期机制深度分析.md)
- Ray 对象管理与引用计数 → [01-架构与原理/Ray-对象管理与引用计数机制深度分析.md](01-架构与原理/Ray-对象管理与引用计数机制深度分析.md)
- Plasma-Store 三套引用计数体系 → [01-架构与原理/Plasma-Store-三套引用计数体系与对象生命周期全链路分析.md](01-架构与原理/Plasma-Store-三套引用计数体系与对象生命周期全链路分析.md)
- plasma 对象生命周期与 Lease 调度依赖 → [01-架构与原理/plasma对象生命周期与Lease调度依赖解析.md](01-架构与原理/plasma对象生命周期与Lease调度依赖解析.md)
- plasma-store object lifecycle and pull/push → [01-架构与原理/plasma-store-object-lifecycle-and-pull-push.md](01-架构与原理/plasma-store-object-lifecycle-and-pull-push.md)
- Object-Reconstruction 机制 → [01-架构与原理/Object-Reconstruction机制深度分析.md](01-架构与原理/Object-Reconstruction机制深度分析.md)
- ObjectReconstructionFailedError → [06-故障排查/ObjectReconstructionFailedError分析.md](06-故障排查/ObjectReconstructionFailedError分析.md)
- 不稳定节点 Object 存储 → [06-故障排查/不稳定节点Object存储分析.md](06-故障排查/不稳定节点Object存储分析.md)
- 可抢占节点 Object 副本迁移 → [07-设计方案/可抢占节点Object副本迁移设计.md](07-设计方案/可抢占节点Object副本迁移设计.md)
- Object 副本推送实现计划 → [07-设计方案/Object副本推送实现计划.md](07-设计方案/Object副本推送实现计划.md)
- Object Replication 优化方案 → [07-设计方案/object_replication_optimization.md](07-设计方案/object_replication_optimization.md)
- 副本机制作业分析与 Spill 指标对账 → [04-Dashboard与指标/副本机制作业分析与Spill指标对账.md](04-Dashboard与指标/副本机制作业分析与Spill指标对账.md)
- Ray 节点内存排查 → [05-内存与OOM/Ray节点内存排查.md](05-内存与OOM/Ray节点内存排查.md)
- Ray-Job-OOM 排查指南 → [05-内存与OOM/Ray-Job-OOM排查指南.md](05-内存与OOM/Ray-Job-OOM排查指南.md)
- Ray-Data 内存排查指南 → [05-内存与OOM/Ray-Data内存排查指南.md](05-内存与OOM/Ray-Data内存排查指南.md)
- Ray Worker 节点 cgroup 内存限制缺失导致 OOM 误杀 → [05-内存与OOM/Ray-Worker节点cgroup内存限制缺失导致OOM误杀排查分析.md](05-内存与OOM/Ray-Worker节点cgroup内存限制缺失导致OOM误杀排查分析.md)
- GCS-Server OOM Kill 导致 Pod 重启 → [05-内存与OOM/GCS-Server-OOM-Kill导致Pod重启深度分析.md](05-内存与OOM/GCS-Server-OOM-Kill导致Pod重启深度分析.md)
- Object-Store 满导致 Task-Pending → [05-内存与OOM/Object-Store满导致Task-Pending根因分析.md](05-内存与OOM/Object-Store满导致Task-Pending根因分析.md)
- Plasma-LRU 淘汰与 Spill 机制 → [05-内存与OOM/Plasma-LRU淘汰与Spill机制深度分析.md](05-内存与OOM/Plasma-LRU淘汰与Spill机制深度分析.md)
- Pinned-Args 内存导致节点分配堆积 → [06-故障排查/Pinned-Args内存导致节点分配堆积.md](06-故障排查/Pinned-Args内存导致节点分配堆积.md)
- cgroup 内存与 Dashboard dev/shm 显示问题 → [04-Dashboard与指标/cgroup内存与dashboard dev shm显示问题分析.md](04-Dashboard与指标/cgroup内存与dashboard dev shm显示问题分析.md)

### GCS 相关
- GCS-Actor-Task-Object 深度分析 → [01-架构与原理/GCS-Actor-Task-Object深度分析.md](01-架构与原理/GCS-Actor-Task-Object深度分析.md)
- GCS 心跳机制与节点死亡检测 → [03-GCS与服务/GCS心跳机制与节点死亡检测深度分析.md](03-GCS与服务/GCS心跳机制与节点死亡检测深度分析.md)
- GCS 锁机制与 io_context 线程模型 → [03-GCS与服务/GCS锁机制与io_context线程模型深度分析.md](03-GCS与服务/GCS锁机制与io_context线程模型深度分析.md)
- GCS 指标与线程压力诊断 → [03-GCS与服务/GCS指标与线程压力诊断.md](03-GCS与服务/GCS指标与线程压力诊断.md)
- GCS 指标代码逻辑 → [03-GCS与服务/GCS指标代码逻辑.md](03-GCS与服务/GCS指标代码逻辑.md)
- GCS 线程分析 → [03-GCS与服务/GCS线程分析.md](03-GCS与服务/GCS线程分析.md)
- GCS-Syncer GPU 利用率下降根因 → [03-GCS与服务/GCS-Syncer-GPU利用率下降根因.md](03-GCS与服务/GCS-Syncer-GPU利用率下降根因.md)
- GCS-Syncer 过载导致 Worker 注册超时 → [03-GCS与服务/GCS-Syncer过载导致Worker注册超时.md](03-GCS与服务/GCS-Syncer过载导致Worker注册超时.md)
- GCS Redis 连接验证与数据存储 → [03-GCS与服务/Ray_GCS_Redis连接验证与数据存储.md](03-GCS与服务/Ray_GCS_Redis连接验证与数据存储.md)
- Ray GCS 外部存储命名空间 → [03-GCS与服务/ray-external-storage-namespace.md](03-GCS与服务/ray-external-storage-namespace.md)
- Node-Failure 通知机制与延迟排查 → [03-GCS与服务/Node-Failure通知机制与延迟排查.md](03-GCS与服务/Node-Failure通知机制与延迟排查.md)
- GCS 综合排查指南 → [06-故障排查/GCS综合排查指南.md](06-故障排查/GCS综合排查指南.md)
- GCS-FD 耗尽排查 → [06-故障排查/GCS-FD耗尽排查.md](06-故障排查/GCS-FD耗尽排查.md)
- GCS-Ghost RUNNING Task 根因分析 → [06-故障排查/GCS-Ghost-RUNNING-Task根因分析与修复方案.md](06-故障排查/GCS-Ghost-RUNNING-Task根因分析与修复方案.md)
- GCS-RPC 穿透导致 process_dispatch 瓶颈 → [06-故障排查/NoOpClusterAutoscaler-GCS-RPC穿透导致process_dispatch瓶颈分析.md](06-故障排查/NoOpClusterAutoscaler-GCS-RPC穿透导致process_dispatch瓶颈分析.md)

### Worker / 容器相关
- Worker 生命周期深度分析 → [01-架构与原理/Worker生命周期深度分析.md](01-架构与原理/Worker生命周期深度分析.md)
- Worker 子进程清理与 Kill 机制 → [01-架构与原理/Worker子进程清理与Kill机制详解.md](01-架构与原理/Worker子进程清理与Kill机制详解.md)
- Ray 容器生命周期 PID1 Raylet GCS → [01-架构与原理/ray-container-lifecycle-pid1-raylet-gcs-deep-dive.md](01-架构与原理/ray-container-lifecycle-pid1-raylet-gcs-deep-dive.md)
- fd 耗尽致 Worker 注册超时 → [06-故障排查/fd耗尽致Worker注册超时分析.md](06-故障排查/fd耗尽致Worker注册超时分析.md)

### StreamingExecutor / 反压相关
- StreamingExecutor 深度分析 → [01-架构与原理/StreamingExecutor深度分析.md](01-架构与原理/StreamingExecutor深度分析.md)
- StreamingGenerator 机制 → [01-架构与原理/StreamingGenerator机制.md](01-架构与原理/StreamingGenerator机制.md)
- StreamingGenerator Recovery 机制 → [01-架构与原理/streaming_generator_recovery_analysis.md](01-架构与原理/streaming_generator_recovery_analysis.md)
- Ray-Data 反压机制 → [01-架构与原理/Ray-Data反压机制分析.md](01-架构与原理/Ray-Data反压机制分析.md)
- Streaming-Output-Backpressure-Escape-Hatch → [01-架构与原理/Streaming-Output-Backpressure-Escape-Hatch机制详解.md](01-架构与原理/Streaming-Output-Backpressure-Escape-Hatch机制详解.md)
- 调度与反压详细分析 → [01-架构与原理/调度与反压详细分析.md](01-架构与原理/调度与反压详细分析.md)
- StreamingGeneratorReturn 完整深度分析 → [01-架构与原理/StreamingGeneratorReturn完整深度分析.md](01-架构与原理/StreamingGeneratorReturn完整深度分析.md)
- Tidal 节点抢占导致 Pipeline 反压死锁 → [06-故障排查/Tidal节点抢占导致Pipeline反压死锁与GCS-Ghost-Running-Task分析.md](06-故障排查/Tidal节点抢占导致Pipeline反压死锁与GCS-Ghost-Running-Task分析.md)

### Dashboard / 指标相关
- Dashboard 节点数不一致 Bug → [04-Dashboard与指标/Dashboard节点数不一致Bug.md](04-Dashboard与指标/Dashboard节点数不一致Bug.md)
- Dashboard 节点状态 alive-dead 不一致 → [04-Dashboard与指标/Dashboard节点状态alive-dead不一致.md](04-Dashboard与指标/Dashboard节点状态alive-dead不一致.md)
- Dashboard Task 状态显示机制 → [04-Dashboard与指标/Dashboard-Task状态显示机制.md](04-Dashboard与指标/Dashboard-Task状态显示机制.md)
- Dashboard 指标优化 → [04-Dashboard与指标/Dashboard指标优化.md](04-Dashboard与指标/Dashboard指标优化.md)
- Dashboard-Worker 节点物理指标丢失 → [04-Dashboard与指标/Dashboard-Worker节点物理指标丢失根因分析.md](04-Dashboard与指标/Dashboard-Worker节点物理指标丢失根因分析.md)
- Prometheus Histogram Quantile → [04-Dashboard与指标/Prometheus-Histogram-Quantile分析.md](04-Dashboard与指标/Prometheus-Histogram-Quantile分析.md)
- Ray 指标过滤指南 → [04-Dashboard与指标/Ray指标过滤指南.md](04-Dashboard与指标/Ray指标过滤指南.md)
- ReporterAgent GPU/CPU 利用率指标 → [04-Dashboard与指标/ReporterAgent-GPU-CPU利用率指标分析.md](04-Dashboard与指标/ReporterAgent-GPU-CPU利用率指标分析.md)

### Ray Data 算子相关
- Ray-Data 使用指南 → [02-数据流与算子/Ray-Data使用指南.md](02-数据流与算子/Ray-Data使用指南.md)
- ReadParquet 并行度分析 → [02-数据流与算子/ReadParquet并行度分析.md](02-数据流与算子/ReadParquet并行度分析.md)
- Block 数量计算逻辑 → [02-数据流与算子/Block数量计算逻辑.md](02-数据流与算子/Block数量计算逻辑.md)
- Operator Config 写入分析 → [02-数据流与算子/Operator-Config写入分析.md](02-数据流与算子/Operator-Config写入分析.md)
- Schedule-Loop 优化 → [02-数据流与算子/Schedule-Loop优化.md](02-数据流与算子/Schedule-Loop优化.md)
- Ray-Data 内部机制指南 → [02-数据流与算子/Ray-Data内部机制指南.md](02-数据流与算子/Ray-Data内部机制指南.md)
- map_batches concurrency 与 compute 关系 → [02-数据流与算子/map_batches-concurrency与compute关系深度分析.md](02-数据流与算子/map_batches-concurrency与compute关系深度分析.md)
- ray.get 数据获取链路 → [02-数据流与算子/ray.get数据获取链路.md](02-数据流与算子/ray.get数据获取链路.md)

### 工具与环境
- KDev AgentRunner 使用与排查 → [09-工具与环境/KDev-AgentRunner使用与排查指南.md](09-工具与环境/KDev-AgentRunner使用与排查指南.md)
- Bazel 构建与缓存机制 → [09-工具与环境/Bazel构建与缓存机制详解.md](09-工具与环境/Bazel构建与缓存机制详解.md)
- 自定义 SO 库集成 → [09-工具与环境/自定义SO库集成指南.md](09-工具与环境/自定义SO库集成指南.md)
- GitNexus 配置指南 → [09-工具与环境/GitNexus配置指南.md](09-工具与环境/GitNexus配置指南.md)

### 社区 / 版本相关
- Ray 社区重要 PR 分析 → [07-设计方案/Ray社区重要PR分析.md](07-设计方案/Ray社区重要PR分析.md)
- Ray-2.55.1 Cherry-Pick 方案 → [07-设计方案/Ray-2.55.1-Cherry-Pick方案.md](07-设计方案/Ray-2.55.1-Cherry-Pick方案.md)
- Ray-2.56.1 Cherry-Pick 方案 → [07-设计方案/Ray-2.56.1-Cherry-Pick方案.md](07-设计方案/Ray-2.56.1-Cherry-Pick方案.md)
- Ray-2.56.1 重点 Commit 深度分析 → [07-设计方案/Ray-2.56.1-重点Commit深度分析.md](07-设计方案/Ray-2.56.1-重点Commit深度分析.md)
- 2.54 后调度与 GCS 优化 → [03-GCS与服务/2.54后调度与GCS优化.md](03-GCS与服务/2.54后调度与GCS优化.md)

## 各子目录详细内容

### 01-架构与原理

核心架构、生命周期、组件通信、调度机制、反压等原理性深度分析文档。

| 文档 | 内容描述 |
|------|----------|
| Ray架构深度分析.md | Ray 进程与线程架构深度分析，包括 Head/Worker Node 进程组件、GCS/Raylet/CoreWorker 线程架构和心跳检测机制 |
| Worker生命周期深度分析.md | Ray Worker 进程完整生命周期，包括 Raylet 启动、Python 代码执行、资源管控和预创建策略 |
| Worker子进程清理与Kill机制详解.md | Ray Worker 子进程清理的三种机制、ray.kill() 调用链与 Worker 断连检测原理 |
| Object生命周期与恢复深度分析.md | Ray Object 生命周期、血缘重建、Pin 机制、引用计数和 OBJECT_IN_PLASMA 哨兵模式 |
| Object-Store内存深度分析.md | Ray Object Store 内存管理，包括调度机制、共享内存、OOM 监控和驱逐机制 |
| Object-Reconstruction机制深度分析.md | Ray Object Reconstruction 血缘重建机制的完整代码链路 |
| Ray-对象生命周期机制深度分析.md | Ray 对象生命周期机制的深度分析 |
| Ray-对象管理与引用计数机制深度分析.md | Ray 对象管理与引用计数机制深度分析 |
| Plasma-Store-三套引用计数体系与对象生命周期全链路分析.md | Plasma Store 三套引用计数体系的完整分析 |
| plasma对象生命周期与Lease调度依赖解析.md | Plasma 对象三层引用机制、Spill/Evict/Delete 流程、Lease 依赖解析与调度授予原理 |
| plasma-store-object-lifecycle-and-pull-push.md | Plasma Store 对象生命周期与 pull/push 机制 |
| 多GPU节点分配深度分析.md | 多 GPU 节点上 GPU 从物理检测到 CUDA_VISIBLE_DEVICES 设置的全链路代码逻辑 |
| GCS-Actor-Task-Object深度分析.md | Actor/Task/Object 创建与存储链路，Actor 经 GCS，Normal Task 和 Object 走去中心化路径 |
| SystemConfig传播机制分析.md | _system_config 参数的传播机制、初始化和动态更新流程 |
| Async-Await机制分析.md | Python async/await 机制原理及 Ray Dashboard 优化应用 |
| GCS-RPC回调机制.md | GCS RPC 回调注册流程、IO Context 路由策略和 Service 回调执行链路 |
| RaySyncer资源同步机制.md | RaySyncer Hub-and-Spoke 架构、版本去重协议和 On-Demand 上报机制 |
| Spillback与资源视图深度分析.md | Ray 调度、Spillback 与资源视图同步机制，含 Hybrid 策略和安全性分析 |
| Lease卡死深度分析.md | RequestWorkerLease gRPC 卡死问题完整调用链和 RetryableGrpcClient 设计缺陷 |
| Task-Event数据流与淘汰分析.md | Dashboard Task 数据获取链路、GCS task event 淘汰机制和可见性问题 |
| Task-Event淘汰与Lease卡死分析.md | Ray Data 任务卡死：Task Event 淘汰 + RequestWorkerLease gRPC 卡死 |
| RequestResources与PendingDemands深度分析.md | request_resources 与 Pending Demands 的本质区别、数据链路 |
| Ray-Data错误处理与重试深度分析.md | Ray Data 错误处理与重试机制，包括 Ray Core 重试、错误块计数和 Object 重建 |
| StreamingExecutor深度分析.md | StreamingExecutor 的错误处理、Task Ready 判断、通知机制和 Actor 空闲超时释放 |
| StreamingGenerator机制.md | streaming generator 的 ray.wait() 参数、prepare_metadata() 和 Object Ref Stream 底层机制 |
| streaming_generator_recovery_analysis.md | Streaming Generator 对象的 Recovery 机制、Lineage 重建与对象生命周期管理 |
| Ray-Streaming-Generator机制深度分析.md | Ray Streaming Generator 机制深度分析 |
| StreamingGenerator-ObjectRefStream机制.md | StreamingGenerator ObjectRefStream 机制 |
| StreamingGenerator-Task提交与回调机制.md | StreamingGenerator Task 提交与回调机制 |
| StreamingGeneratorReturn完整深度分析.md | STREAMING_GENERATOR_RETURN 的工作机制、调用端/执行端数据流和反压设计 |
| StreamingGenerator通知与反压机制.md | Streaming Generator 通知机制与背压设计 |
| Streaming-Output-Backpressure-Escape-Hatch机制详解.md | Streaming Output Backpressure Escape Hatch 机制详解 |
| Ray-Data调度分析.md | Ray Data 调度策略（Hybrid Policy）和 checkpoint 恢复性能问题 |
| Schema与调度优化分析.md | Ray Data 中 schema 优化链和 actor 调度优化链 |
| Ray-Data反压机制分析.md | ResourceBudgetBackpressurePolicy 和 ConcurrencyCap 策略源码解析 |
| 调度与反压详细分析.md | StreamingExecutor 调度逻辑、update_usages() 和 ResourceBudgetBackpressurePolicy |
| 调度阻塞分析.md | StreamingExecutor 调度阻塞根因：API Server 响应慢 + HangingExecutionIssueDetector |
| Ray-任务执行与调度机制深度分析.md | Ray 任务执行与调度机制深度分析 |
| Ray-任务提交与依赖机制深度分析.md | Ray 任务提交与依赖机制深度分析 |
| Ray核心组件通信深度分析.md | TaskManager/CoreWorker/Raylet 之间的 IPC Socket 通信全链路 |
| Ray远程对象创建与并发组机制深度分析.md | Ray 远程对象创建与并发组机制 |
| Actor创建与重启流程.md | Actor 创建与重启的 3 个阶段和异常恢复机制 |
| Actor-Kill与退出机制深度分析.md | Actor Kill 与退出机制深度分析 |
| IsInPlasmaError机制解析.md | CoreWorkerMemoryStore 中 IsInPlasmaError 机制解析 |
| Boost.Asio 与 io_context 完整技术文档.md | Boost.Asio 与 io_context 完整技术文档 |
| Custom-Resource到NodeLabelSchedulingStrategy迁移指南.md | Custom Resource 到 NodeLabelSchedulingStrategy 的迁移指南 |
| Dashboard-Agent-架构与HTTP端口绑定深度分析.md | Dashboard Agent 架构与 HTTP 端口绑定深度分析 |
| HashShuffleAggregator初始化失败与跨节点重试分析.md | HashShuffleAggregator 初始化失败与跨节点重试分析 |
| Ray-gRPC通信机制深度分析.md | Ray gRPC 通信机制深度分析 |
| RaySyncer资源同步机制.md | RaySyncer 资源同步机制 |
| ray-container-lifecycle-pid1-raylet-gcs-deep-dive.md | Ray 容器中 PID1 进程管理、Raylet/GCS 级联死亡、进程 Reaper 与容器退出码 |
| lease_scheduling_and_replication_push_analysis.md | Lease scheduling and replication push 分析 |
| 作业Task调度和状态变化详解.md | 作业 Task 调度和状态变化详解 |

### 02-数据流与算子

Ray Data 算子机制、Block 计算、数据读写、内部机制等文档。

| 文档 | 内容描述 |
|------|----------|
| Operator-Config写入分析.md | Operator Config 写入机制的完整代码路径和两种写法的行为差异 |
| Operator完成状态与BlocksOutputted分析.md | Operator 完成状态两层判定机制和 Blocks Outputted 指标 |
| ReadParquet并行度分析.md | read_parquet 中 concurrency 和 override_num_blocks 参数机制和自动并行度计算 |
| Block数量计算逻辑.md | read_parquet Block 数量和行数的自动计算逻辑 |
| MapTask-Kwargs传递机制.md | add_map_task_kwargs_fn 注册的参数如何传递到 TaskContext.kwargs |
| ObjectRef生命周期分析.md | Ray Data 中 ObjectRef 传递与生命周期、RefBundle 所有权语义 |
| Task状态与调度机制.md | Ray Data Task 状态与调度机制、ActorPool Prefetch 和 GCS 同步延迟 |
| ActorPool自动伸缩分析.md | Actor Pool 自动伸缩与 Actor 退出机制 |
| StreamingRepartition深度分析.md | StreamingRepartition 的流式重组机制 |
| Checkpoint恢复空Block-Bug分析.md | checkpoint 恢复后 Blocks Input 有值但 Rows Input=0 的 bug |
| Dataset-State分析.md | Ray Data Dataset 状态一直显示 PENDING 的原因 |
| ReallocateResources-Bug分析.md | _reallocate_resources 的两个 Bug（node:IP 不扣减、整数除法残值） |
| Write算子output-rows修复.md | Write 算子 output_rows 指标显示 task 数而非实际行数的 bug 修复 |
| 批量Metadata优化.md | StreamingExecutor 串行 ray.get() 优化为批量 ray.wait() + ray.get() |
| Schedule-Loop优化.md | Schedule Loop 性能优化，含 _next_sync() 详解和 block size 优化方案 |
| Ray-Data内部机制指南.md | Ray Data DEBUG 日志启用、Progress Manager 和反压机制 |
| Ray-Data使用指南.md | Ray Data 核心概念（Dataset/Block/StreamingExecutor）、API 和行业应用案例 |
| ray.get数据获取链路.md | ray.get() 从 Plasma Store 获取数据的完整链路 |
| Error-Block深度分析.md | process_completed_tasks 中异常原因、Task 重试行为和边缘竞争条件 |
| map_batches-concurrency与compute关系深度分析.md | map_batches 中 concurrency 与 compute 参数的优先级、转换规则和完整调用链 |
| Ray-Data-Schema推断与传递机制.md | Ray Data Schema 推断与传递机制 |
| PR-60294-Checkpoint-Filter-Arrow-Numpy分析.md | PR 60294 Checkpoint Filter Arrow Numpy 分析 |
| ray-data-performance.md | Ray Data 性能分析 |

### 03-GCS与服务

GCS Server、Syncer、gRPC、线程架构、Redis 存储等文档。

| 文档 | 内容描述 |
|------|----------|
| GCS指标与线程压力诊断.md | GCS Server 各类指标含义和线程压力诊断核心指标 |
| GCS指标代码逻辑.md | GCS IO Context Provider 路由策略、RaySyncer 广播逻辑源码详解 |
| GCS线程分析.md | GCS Server 完整线程列表与线程瓶颈分析指南 |
| GCS心跳机制与节点死亡检测深度分析.md | GCS 心跳机制与节点死亡检测深度分析 |
| GCS锁机制与io_context线程模型深度分析.md | GCS 锁机制与 io_context 线程模型深度分析 |
| GCS-Syncer-GPU利用率下降根因.md | ray_syncer_io_c CPU 高和 GPU 利用率下降的根因 |
| GCS-Syncer过载导致Worker注册超时.md | GCS Syncer 过载导致 Worker 注册超时与幽灵 Task 问题 |
| Ray_GCS_Redis连接验证与数据存储.md | GCS Redis 连接验证失败排查及 GCS 数据在 Redis 中的存储结构详解 |
| ray-external-storage-namespace.md | Ray GCS 外部存储命名空间（external_storage_namespace）配置与 Redis 数据隔离 |
| Node-Failure通知机制与延迟排查.md | Node Failure 通知机制与延迟排查 |
| Syncer-IO-CPU高排查.md | ray_syncer_io_c 线程 CPU 高的排查案例和参数调整建议 |
| 2.54后调度与GCS优化.md | Ray 2.54 到 2.55 在调度和 GCS 方面的主要优化 |
| gRPC端口冲突分析.md | gRPC 端口冲突导致 Worker crash 的问题 |

### 04-Dashboard与指标

Dashboard 显示机制、Prometheus 查询、指标体系等文档。

| 文档 | 内容描述 |
|------|----------|
| Dashboard节点数不一致Bug.md | Dashboard 节点数与 ray list nodes 不一致的 bug |
| Dashboard节点状态alive-dead不一致.md | Dashboard Node Status 显示 dead 节点为 alive 的 bug |
| Dashboard-Hostname数据流分析.md | Dashboard /nodes API 中 hostname 字段的完整数据流 |
| Dashboard-Task状态显示机制.md | Dashboard 任务状态显示机制和 Progress Bar 与 Task Table 不一致问题 |
| Dashboard指标优化.md | Ray Data Dashboard 指标修复和排序功能增强 |
| Dashboard-Worker节点物理指标丢失根因分析.md | Dashboard-Worker 节点物理指标丢失根因分析 |
| Dashboard-Metrics-Label不匹配Bug.md | Dashboard Metrics Label 不匹配 Bug |
| Dashboard-Running-Task总数设计.md | Dashboard 添加 RUNNING task 真实总数的设计方案 |
| Dashboard-Pod-Name分析.md | Dashboard Pod Name 分析 |
| Ray-Data-Metrics分析.md | Ray Data Metrics 全面分析，包括 MapTransformer 指标和自定义指标上报 |
| Prometheus-Histogram-Quantile分析.md | Prometheus histogram_quantile 与 rate 的计算原理 |
| Prometheus视频推理查询.md | Multishot Pipeline 各阶段的 Prometheus 查询方案 |
| Ray指标过滤指南.md | Ray 指标过滤指南，按 P0-P3 重要性分级 |
| ReporterAgent-GPU-CPU利用率指标分析.md | ReporterAgent GPU/CPU 利用率指标分析 |
| cgroup内存与dashboard dev shm显示问题分析.md | cgroup 内存限制未生效时 Dashboard 内存列和 dev/shm 显示不正确的根因 |
| 副本机制作业分析与Spill指标对账.md | Object Replication 副本机制作业分析与 Prometheus Spill 相关指标对账 |

### 05-内存与OOM

内存管理、OOM 排查、CUDA OOM、cgroup 内存等文档。

| 文档 | 内容描述 |
|------|----------|
| Ray节点内存排查.md | Ray 节点内存异常排查，cgroup 内存数据与 ps aux RSS 的差异 |
| Ray-Job-OOM排查指南.md | Ray 作业 OOM 故障排查指南 |
| Ray-Data内存排查指南.md | Ray Data 作业内存问题排查指南 |
| Ray-Worker节点cgroup内存限制缺失导致OOM误杀排查分析.md | Pod 未设置 cgroup 内存限制导致 Ray 误用宿主机内存触发 OOM 误杀的根因 |
| CUDA-OOM分析.md | DistributedStreamingVideoProcessMapper CUDA OOM 异常分析 |
| GCS-Server-OOM-Kill导致Pod重启深度分析.md | GCS Server OOM Kill 导致 Pod 重启深度分析 |
| Object-Store满导致Task-Pending根因分析.md | Object Store 满导致 Task Pending 根因分析 |
| Plasma-LRU淘汰与Spill机制深度分析.md | Plasma LRU 淘汰与 Spill 机制深度分析 |
| Ray-OOM-Kill-Error-Object-UNRECONSTRUCTABLE完整代码链路分析.md | Ray OOM Kill Error Object UNRECONSTRUCTABLE 完整代码链路分析 |
| Driver进程线程架构与BlockMetadata内存深度分析.md | Driver 进程线程架构与 BlockMetadata 内存深度分析 |
| multishot_20260626_101253_120-LINEAGE_EVICTED排查实录.md | multishot LINEAGE_EVICTED 排查实录 |

### 06-故障排查

生产环境故障排查案例文档。

| 文档 | 内容描述 |
|------|----------|
| GCS综合排查指南.md | 800~2500 节点规模的 GCS 问题排查完全指南 |
| GCS-FD耗尽排查.md | 2246 节点集群中 FD 耗尽导致调度阻塞的排查指南 |
| GCS排查指南.md | 800+ 节点集群中 Actor 死亡、节点误判、调度缓慢排查 |
| GCS-Ghost-RUNNING-Task根因分析与修复方案.md | GCS Ghost RUNNING Task 根因分析与修复方案 |
| Actor-Unavailable连接拒绝分析.md | ACTOR_UNAVAILABLE gRPC Connection refused 分析 |
| ObjectReconstructionFailedError分析.md | ObjectReconstructionFailedError 8 种子原因 |
| ObjectFetchTimedOutError完整分析.md | ObjectFetchTimedOutError 完整分析 |
| 不稳定节点Object存储分析.md | 不稳定节点场景下的对象远程存储方案 |
| GPU利用率波动根因分析.md | GPU 利用率周期性波动的根因分析 |
| Dashboard-NodeHead-CPU-100分析.md | Dashboard NodeHead 子进程 CPU 100% 分析 |
| Dashboard-Running数不一致分析.md | Dashboard Overview Running vs Task Table RUNNING 不一致 |
| Dashboard-Agent-OTel指标注册采集死锁导致Job-Stop卡死分析.md | Dashboard Agent OTel 指标注册采集死锁导致 Job Stop 卡死分析 |
| Task数偏低-淘汰与僵尸Entry分析.md | GCS Task Event 淘汰与僵尸 Entry 导致 Dashboard 显示偏低 |
| Executor-RUNNING上报配置修复.md | Executor 侧 RUNNING 上报可配置带 task_info 的分析与修复 |
| Job-Supervisor调度失败分析.md | Job Supervisor Actor 调度失败分析 |
| Pinned-Args内存导致节点分配堆积.md | PENDING_NODE_ASSIGNMENT 堆积排查 |
| ActorPool僵尸Actor不释放排查.md | ActorPool 僵尸 Actor 不释放问题排查 |
| Ray-Data调度问题排查.md | Ray Data 作业调度问题排查 |
| Ray-Data-PendingActor导致Shutdown死锁排查.md | Ray Data PendingActor 导致 Shutdown 死锁排查 |
| PendingDemands内存与两层调度.md | Pending Demands 中 memory 来源的数据流追踪 |
| Task-Events-GC问题排查.md | Task Events GC 问题排查指南 |
| Waiting-for-Scheduling排查.md | Waiting for scheduling 问题排查 |
| Ray线程架构深度分析.md | Ray 进程与线程架构深度分析 |
| GPU-Actor未释放根因分析.md | GPU Actor 未释放的根因分析 |
| GPU-Actor-Pending根因分析.md | GPU Actor Pending 根因分析 |
| Ray-Job卡死排查.md | Ray Job 卡死排查分析 |
| Ray-Job-Stop-API终止分析.md | Ray Job Stop API 终止分析 |
| Ray任务永久卡在PENDING_NODE_ASSIGNMENT的排查报告.md | 任务因 runtime_env_setup_failed 导致永久卡在 PENDING_NODE_ASSIGNMENT 的排查 |
| RayWait调度瓶颈导致GPU吞吐下降分析.md | RayWait 调度瓶颈导致 GPU 吞吐下降分析 |
| Ray集群CPU资源虚高排查分析.md | Ray 集群 CPU 资源虚高排查分析 |
| Tidal节点抢占导致Pipeline反压死锁与GCS-Ghost-Running-Task分析.md | Tidal 节点抢占导致 Pipeline 反压死锁与 GCS Ghost Running Task 分析 |
| NoOpClusterAutoscaler-GCS-RPC穿透导致process_dispatch瓶颈分析.md | NoOpClusterAutoscaler GCS RPC 穿透导致 process_dispatch 瓶颈分析 |
| Job-77000000-PENDING_NODE_ASSIGNMENT排查报告.md | Job 77000000 PENDING_NODE_ASSIGNMENT 排查报告 |
| fd耗尽致Worker注册超时分析.md | fd 耗尽致 Worker 注册超时分析 |
| autoscaler-troubleshooting.md | Autoscaler 排障指南 |

### 07-设计方案

功能设计、优化方案、Cherry-Pick 计划、社区 PR 分析等文档。

| 文档 | 内容描述 |
|------|----------|
| 可抢占节点Object副本迁移设计.md | 可抢占节点对象副本迁移设计，含 Phase 1-3 |
| Object副本推送实现计划.md | Phase 1: 可抢占节点对象副本推送实现计划 |
| Pin转移实现计划.md | Phase 2: Pin 转移实现计划 |
| object_replication_optimization.md | Object Replication 功能完整优化修复方案，包括 ObjectSource 枚举细化、复制目标选择改进 |
| 分布式流式视频处理算子设计.md | 分布式流式视频处理算子 CPU/GPU 并行流水线架构 |
| Driver调度优化方案.md | StreamingExecutor 调度循环瓶颈综合优化方案 |
| Ray-2.55.1-Cherry-Pick方案.md | Ray v2.54.0 到 v2.55.1 的 cherry-pick 方案 |
| Ray-2.56.1-Cherry-Pick方案.md | Ray v2.55.1 到 v2.56.1 的 cherry-pick 方案 |
| Ray-2.56.1-重点Commit深度分析.md | Ray 2.56.1 重点 Commit 深度分析 |
| Ray社区重要PR分析.md | Ray 社区近期重要 PR 的方案与设计权衡分析 |
| AutoscalingCoordinator失败分析.md | AutoscalingCoordinator 连续失败导致作业 FAILED |
| GPU-Actor感知缩容设计.md | GPU Actor 感知缩容 4 层架构设计 |
| 分离Parallelism与NumBlocks设计.md | 分离 parallelism 与 override_num_blocks 的设计方案 |
| Map-Operator-Pipeline数据流.md | map_batches 的完整数据流 |
| Finished-Unconsumed任务诊断设计.md | 新增 Finished but Unconsumed 任务诊断功能设计 |
| 腾讯Ray优化方案.md | 腾讯 Ray 内部版本优化方案详细设计 |
| Actor-Creation-Node-Blacklist机制设计方案.md | Actor Creation Node Blacklist 机制设计方案 |
| Node-Health-Monitor-与异常上报设计方案.md | Node Health Monitor 与异常上报设计方案 |
| PriorityOperator-设计方案.md | PriorityOperator 设计方案 |
| ResourceManager增量更新优化分析.md | ResourceManager 增量更新优化分析 |

### 08-视频推理

视频推理 Pipeline、共享 GPU 等文档。

| 文档 | 内容描述 |
|------|----------|
| QwenVL共享GPU指南.md | QwenVL streaming pipeline 在 shared GPU 场景的问题分析和最佳实践 |

### 09-工具与环境

开发工具、测试配置、环境集成、KDev AgentRunner 等文档。

| 文档 | 内容描述 |
|------|----------|
| pytest导入路径排查.md | pytest ModuleNotFoundError 排查 |
| pytest本地开发指南.md | Ray 本地开发 pytest 测试指南 |
| Runtime-Env指南.md | Ray runtime_env 机制详解 |
| Worker-Process-Setup-Hook指南.md | worker_process_setup_hook 与 Monkey Patch 指南 |
| Ray-Data日志配置指南.md | Ray Data 日志系统架构和 DEBUG 级别控制方法 |
| 自定义SO库集成指南.md | Ray Bazel 项目中集成自定义预编译 .so 库的流程 |
| GitNexus配置指南.md | GitNexus 代码知识图谱工具的安装配置与使用指南 |
| Graphify使用指南.md | Graphify 知识图谱工具使用指南 |
| Parquet-Checkpoint去重指南.md | 从 checkpoint parquet 文件中提取 blobstore_id 并去重 |
| Git-Merge-Commit分析.md | Git merge commit 结构和 cherry-pick 操作指南 |
| RemoteWrite集成测试指南.md | Ray RemoteWriteExporter 集成测试流程 |
| Ray社区PR开发指南.md | Git 身份配置、DCO 签名和 PR 提交到 Ray 社区的流程 |
| Bazel构建与缓存机制详解.md | Bazel CI 配置、远程缓存机制、Action Digest 计算、Python 版本缓存隔离、本地缓存配置 |
| KDev-AgentRunner使用与排查指南.md | KDev 本地 AgentRunner 执行器的安装、初始化与使用排查说明 |
