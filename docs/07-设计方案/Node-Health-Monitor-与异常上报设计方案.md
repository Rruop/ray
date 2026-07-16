# 节点级健康状态与异常上报设计方案

## 1. 背景与目标

### 1.1 问题

当前 Ray 调度存在两个关键缺口：

1. **覆盖面不足**：普通 task 在 CoreWorker 重试时通过 `LeasePolicy::GetBestNodeForLease()` 重新选节点，**没有任何机制排除之前失败过的节点**，task 可能被反复调度回同一失败节点。Actor creation task 现有 Actor Blacklist 方案也仅 per-actor 隔离，无法跨任务共享
2. **信息孤岛**：节点异常信息是 GCS 内部数据，无法跨任务共享。所有 task/actor 在同一坏节点上都会"亲自失败一次"才知道避开。运维人员也无法从外部查看节点健康状态进行诊断

### 1.2 目标

- **机器异常检测**：检测节点机器级硬件/环境异常（CUDA Error、磁盘故障、网络异常等），标记节点为 unhealthy
- **多任务共享**：任何 task/actor 都能查询并避开 unhealthy 节点，无需"亲自失败"
- **外部上报**：节点异常信息上报至 Prometheus + HTTP 事件端点，供外部系统（Grafana、运维平台、Autoscaler）消费
- **与现有机制协同**：与 GcsHealthCheckManager 心跳检测、TaskManager 重试机制集成

**非目标**（不标记为 unhealthy，不走全局排除）：
- **OOM**：瞬态资源压力，Ray 已有 `task_oom_retries` + 指数退避重试机制处理，不属于机器异常
- **Runtime env 安装失败**：多为 actor 自身配置问题，非机器异常
- **纯用户代码异常**（TypeError/ValueError 等）：换节点也会失败，不属于机器异常
- **未匹配关键字的 SYSTEM_ERROR**：大部分是用户代码 segfault，无法确认是机器问题就不标记

### 1.3 现状缺口分析

| 维度 | 现有分类 | 缺失 |
|------|---------|------|
| WorkerExitType | SYSTEM_ERROR / USER_ERROR / NODE_OUT_OF_MEMORY 等 5 种 | 无 GPU_ERROR、DISK_ERROR、NETWORK_ERROR 等硬件故障分类 |
| NodeDeathInfo.Reason | EXPECTED_TERMINATION / UNEXPECTED_TERMINATION 等 4 种 | 无 DISK_FAILURE、GPU_ERROR、NETWORK_PARTITION 等硬件故障分类 |
| 节点状态 | 仅 ALIVE / DEAD | 缺少中间态（UNHEALTHY、DEGRADED） |
| Task 重试 | `LeasePolicy` 重新选节点，无排除机制 | 可能反复调度回同一失败节点 |
| 节点健康外部可见性 | `node_failure_total` 无标签 | 无法区分故障类型、无法查看 unhealthy 节点 |

**NODE_OUT_OF_MEMORY** 是唯一与节点级资源相关的退出类型，仅覆盖"内存"一种物理资源。CUDA Error、GPU 故障、磁盘 I/O 异常等容器/物理节点级异常，当前以 `USER_ERROR` 或 `SYSTEM_ERROR` 形式上报，无法区分是否与节点环境相关。

---

## 2. 设计原则

1. **复用优先**：不引入新的外部通道。Prometheus 指标复用 `stats::Count/Gauge → OpenTelemetry → reporter_agent → Remote Write`；事件上报复用 `RayEventRecorder → AggregatorAgent → AsyncHttpPublisherClient`；内部同步复用 `RaySyncer.labels`
2. **信息分级**：低基数信息（reason_category、source、node_ip）走 Prometheus 标签供聚合告警；高基数信息（完整异常堆栈、worker_id、task_id）走 Ray Event Export HTTP 供诊断
3. **保守策略优先**：默认 `node_health_monitor_enabled=false`。只有明确的机器/硬件异常才标记 unhealthy。`ShouldMarkNodeUnhealthy` 对 USER_ERROR 和 SYSTEM_ERROR 均采用关键字匹配，避免误报。OOM、runtime env 失败、纯用户代码异常不标记
4. **全局唯一机制**：GcsNodeHealthManager 是唯一的节点排除机制，所有 task/actor 调度统一查询。不引入 per-actor 黑名单等冗余层
5. **资源感知排除**：GPU_ERROR 只影响需要 GPU 的调度，不影响 CPU task。`GetUnhealthyNodes()` 按调度资源需求过滤，避免过度排除
6. **TTL 自动恢复**：异常条目有 TTL（默认 120s），过期自动清理。不引入"手动恢复"路径，避免运维负担
7. **内存安全**：GcsNodeHealthManager 纯内存存储，每节点条目数有上限，GCS 重启后状态丢失（可接受，节点异常会很快重新触发）

---

## 3. 整体架构

### 3.1 全局节点健康状态

| 层级 | 名称 | 粒度 | 存储位置 | 作用 | 持久化 |
|------|------|------|---------|------|--------|
| **唯一机制** | **全局节点健康状态** | per-node | GcsNodeHealthManager (GCS内存) | 任何 task/actor 失败都标记节点，所有任务共享 | 否（TTL 自动清理） |

**调度排除公式**：`excluded_nodes = GcsNodeHealthManager::GetUnhealthyNodes(resource_req)`

所有 task/actor 调度统一查询 GcsNodeHealthManager，不引入 per-actor 黑名单等冗余层。全局层通过 Prometheus + HTTP 上报外部，实现跨集群/跨任务的信息共享。

### 3.2 资源感知排除

不同异常类型影响的资源范围不同。GPU_ERROR 只影响需要 GPU 的调度，不应排除 CPU-only task：

| reason_category | 影响资源范围 | 调度排除策略 |
|---|---|---|
| GPU_ERROR | 仅 GPU | 只有需要 GPU 的 task/actor 才排除该节点 |
| DISK_ERROR | 全局 | 所有 task/actor 排除 |
| NETWORK_ERROR | 全局 | 所有 task/actor 排除 |
| SYSTEM_ERROR（匹配硬件关键字） | 全局 | 所有 task/actor 排除 |

**实现**：`GetUnhealthyNodes()` 接收资源需求参数，按 `affects_resource` 字段过滤：

```cpp
// 返回对指定资源需求有影响的 unhealthy 节点
absl::flat_hash_set<NodeID> GetUnhealthyNodes(
    const rpc::ResourceRequest &resource_request) const;
// resource_request 需要 GPU → 排除 GPU_ERROR + 全局异常节点
// resource_request 不需要 GPU → 只排除全局异常节点
```

`NodeUnhealthyEntry` 新增 `affects_resource` 字段（见附录 A.2）：
- GPU_ERROR → `affects_resource = "GPU"`
- DISK_ERROR / NETWORK_ERROR / SYSTEM_ERROR → `affects_resource = ""`（空=全局影响）

### 3.3 与现有 GcsHealthCheckManager 的关系

| 机制 | 检测信号 | 触发动作 | 状态 |
|------|---------|---------|------|
| GcsHealthCheckManager（现有） | raylet gRPC 心跳连续失败 5 次 | 节点标记 DEAD，触发 OnNodeFailure | 二元：ALIVE/DEAD |
| GcsNodeHealthManager（新增） | task/actor 在节点上因环境异常失败 | 节点标记 UNHEALTHY，调度排除 | 三元：HEALTHY/UNHEALTHY/DEAD |

两者互补：心跳检测覆盖"节点完全不可达"，节点健康监控覆盖"节点活着但有硬件/环境问题"。**UNHEALTHY 是 ALIVE 的子状态**，节点仍是 ALIVE 但调度时应避开。

### 3.4 组件交互图

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          GCS Server                                      │
│                                                                         │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │ GcsNodeHealthManager (新增)                                         │ │
│  │  node_unhealthy_entries_: NodeID → vector<NodeUnhealthyEntryData>  │ │
│  │  ├─ MarkNodeUnhealthy()    ← 统一入口                               │ │
│  │  ├─ GetUnhealthyNodes(req)  → 调度查询（按资源需求过滤）             │ │
│  │  ├─ PruneUnhealthyEntries() ← TTL 清理                             │ │
│  │  ├─ ShouldMarkNodeUnhealthy() ← 关键字匹配                          │ │
│  │  ├─ RecordMetrics()        → Prometheus 指标 (含 node_ip 标签)     │ │
│  │  └─ WriteNodeHealthExportEvent() → Ray Event Export + 旧 Export    │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│       ▲                    ▲                        ▲                    │
│       │                    │                        │                    │
│  ┌────┴──────┐     ┌──────┴──────┐         ┌──────┴──────┐             │
│  │GcsActor   │     │GcsNode      │         │ReportRPC    │             │
│  │Manager    │     │Manager      │         │(来自        │             │
│  │OnWorkerDead│    │labels 更新   │         │ CoreWorker) │             │
│  └───────────┘     └─────────────┘         └─────────────┘             │
│       │                    │                                              │
│       ▼                    ▼                                              │
│  调度排除（统一）        GcsNodeInfo.labels                                │
│  excluded =               ray.io/node-health=unhealthy                     │
│  GcsNodeHealthManager::                                                   │
│  GetUnhealthyNodes(req)                                                   │
└─────────────────────────────────────────────────────────────────────────┘
         │                       │                      │
         ▼                       ▼                      ▼
  ┌──────────────┐    ┌──────────────────┐    ┌──────────────────┐
  │ Prometheus   │    │ HTTP 事件端点     │    │ RaySyncer labels │
  │ Remote Write │    │ (AsyncHttpPub)   │    │ → 所有 raylet     │
  │ → Grafana    │    │ → 诊断数据库     │    │ → 本地调度感知    │
  │ → AlertMgr   │    │ → 运维工单       │    │                  │
  └──────────────┘    └──────────────────┘    └──────────────────┘
```

---

## 4. 数据流

### 4.1 Actor Creation Task 异常

在现有三步解耦（ShouldAddToNodeBlacklist → AddNodeBlacklist → ShouldRestartOnNodeFailure）**之前**新增 Step 0：

```
Actor __init__ 抛异常（如 CUDA Error）
  │
  ▼
CoreWorker Exit(USER_ERROR, creation_task_exception)
  │
  ▼
Raylet → GCS WorkerManager → WorkerDeadListener
  │
  ▼
GcsActorManager::OnWorkerDead(node_id, worker_id, ..., creation_task_exception)
  │
  ├─ Step 0 (新增): 全局节点健康状态上报
  │    ShouldMarkNodeUnhealthy(disconnect_type, creation_task_exception)?
  │     ├─ Yes → GcsNodeHealthManager::MarkNodeUnhealthy(node_id, entry)
  │     │         → 更新 GcsNodeInfo labels
  │     │         → RecordMetrics() (Prometheus)
  │     │         → WriteNodeHealthExportEvent() (Ray Event HTTP)
  │     │         → PublishNodeInfo() (GCS PubSub)
  │     └─ No  → 跳过
  │
  ├─ Step 1 (现有): need_reconstruct 决策（USER_ERROR → false，其他 → true）
  └─ Step 2 (新增): 若 ShouldMarkNodeUnhealthy 且 node_health_monitor_enabled
       → need_reconstruct = true, RestartActor（即使 USER_ERROR 也重启，因为节点有问题）
  │
  ▼ (need_reconstruct = true 时)
RestartActor → Schedule → SelectForwardingNode
  excluded_nodes = GcsNodeHealthManager::GetUnhealthyNodes(actor_resource_req)
```

### 4.2 普通 Task 异常

```
普通 task 执行失败（如 CUDA Error, SYSTEM_ERROR）
  │
  ▼
CoreWorker TaskManager::RetryTaskIfPossible(task_id, error_info)
  │
  ├─ Step 0 (新增): 上报节点异常到 GCS
  │    ShouldReportNodeUnhealthy(error_info)?
  │     ├─ Yes → GCS RPC: ReportNodeUnhealthyEvent(node_id, entry)
  │     │         → GcsNodeHealthManager::MarkNodeUnhealthy()
  │     │         → Prometheus + Ray Event HTTP + PubSub
  │     └─ No  → 跳过
  │
  └─ Step 1 (新增): 重试时排除 unhealthy 节点
       LeasePolicy::GetBestNodeForLease(spec, excluded_nodes=unhealthy_nodes_)
       (CoreWorker 维护本地 unhealthy_nodes_ 缓存，通过 GCS PubSub 更新)
```

### 4.3 Worker 断连统一入口

```
Raylet → GCS WorkerManager → WorkerDeadListener
  │ → gcs_node_health_manager_->OnWorkerDead(...)  ← 新增（统一入口）
  │ → gcs_actor_manager_->OnWorkerDead(...)          ← 已有
  │ → gcs_task_manager_->OnWorkerDead(...)            ← 已有
```

---

## 5. 异常分类体系

### 5.1 NodeUnhealthyReason 枚举（新增）

| 枚举值 | 数值 | 含义 | 典型异常 | 标记 unhealthy |
|--------|------|------|---------|---------------|
| NODE_UNHEALTHY_UNSPECIFIED | 0 | 未指定 | — | ❌ |
| NODE_UNHEALTHY_GPU_ERROR | 1 | GPU/CUDA 硬件故障 | CUDA Error, cuBLAS Error, cuDNN Error, NCCL Error | ✅ |
| NODE_UNHEALTHY_DISK_ERROR | 3 | 磁盘 I/O 异常 | I/O Error, ENOSPC, disk read/write failure | ✅ |
| NODE_UNHEALTHY_NETWORK_ERROR | 4 | 网络异常 | Connection Reset, ETIMEDOUT, Network unreachable | ✅ |
| NODE_UNHEALTHY_SYSTEM_ERROR | 6 | 系统级异常（硬件相关） | SIGBUS, CUDA driver crash | ✅（仅匹配硬件关键字） |
| NODE_UNHEALTHY_OOM | 2 | 节点内存不足 | NODE_OUT_OF_MEMORY, MemoryError | ❌（已有 OOM 重试机制） |
| NODE_UNHEALTHY_RUNTIME_ENV_ERROR | 5 | 运行时环境安装失败 | pip install failure, conda env error | ❌（多为配置问题） |
| NODE_UNHEALTHY_UNKNOWN_ENV_ERROR | 99 | 未知环境相关异常 | 无法归类 | ❌（保守不标记） |

**核心原则**：只有明确的机器/硬件异常才标记 unhealthy。OOM、runtime env 失败、纯用户代码异常、未匹配关键字的 SYSTEM_ERROR 都不标记。

### 5.2 ShouldMarkNodeUnhealthy 决策

| WorkerExitType | 是否标记 | 说明 |
|---|---|---|
| NODE_OUT_OF_MEMORY | **false** | 瞬态资源压力，Ray 已有 `task_oom_retries`（默认无限）+ 指数退避重试机制处理。不属于机器异常 |
| SYSTEM_ERROR | **关键字匹配** | 匹配 `node_unhealthy_system_error_keywords` 中的硬件相关关键字才标记。大部分 SYSTEM_ERROR 是用户代码 segfault，不标记 |
| USER_ERROR + creation_task_exception | **关键字匹配** | 匹配硬件/环境相关关键字（CUDA/GPU/disk/network 等）才标记。纯用户代码异常（TypeError/ValueError）不标记 |
| INTENDED_SYSTEM_EXIT | false | 正常退出 |
| INTENDED_USER_EXIT | false | 正常退出 |

### 5.3 关键字匹配与分类

`ShouldMarkNodeUnhealthy` 和 `ClassifyUnhealthyReason` 共用关键字匹配逻辑。异常字符串（`creation_task_exception->formatted_exception_string()` 或 `disconnect_detail`）大小写不敏感匹配：

| 关键字模式 | ShouldMark | 分类结果 | affects_resource |
|-----------|------------|---------|------------------|
| CUDA, GPU, cuBLAS, cuDNN, NCCL, nvml | ✅ | NODE_UNHEALTHY_GPU_ERROR | "GPU" |
| DISK, I/O ERROR, ENOSPC | ✅ | NODE_UNHEALTHY_DISK_ERROR | ""（全局） |
| NETWORK, CONNECTION RESET, ETIMEDOUT | ✅ | NODE_UNHEALTHY_NETWORK_ERROR | ""（全局） |
| bus error, SIGBUS, hardware fault | ✅ | NODE_UNHEALTHY_SYSTEM_ERROR | ""（全局） |
| (无匹配) | ❌ | — | — |

**未匹配关键字的异常不标记 unhealthy**，但仍可通过现有 Worker failure 日志和 Export 事件查看。

### 5.4 OOM / Runtime Env 的处理

这些异常不标记 unhealthy，但仍有现有机制处理：

| 异常 | 现有处理机制 | 不标记 unhealthy 的理由 |
|------|-------------|------------------------|
| OOM | `task_oom_retries`（默认无限）+ 指数退避 + 独立计数器 | 瞬态资源压力，worker 被杀后内存立即释放。全局排除 120s 是过度反应 |
| Runtime env 失败 | actor creation task 异常，现有 actor DEAD 或按 max_restarts 重启 | 多为 actor 自身配置问题（包版本冲突），非机器异常 |
| 纯用户代码异常 | actor DEAD 或 task 失败 | 换节点也会失败，不属于机器异常 |
| 未匹配 SYSTEM_ERROR | worker crash，现有重试机制 | 大部分是用户代码 segfault，无法确认是机器问题就不标记 |

---

## 6. 可观测性与外部上报

### 6.1 三个通道概览

| 通道 | 用途 | 实时性 | 传输协议 | 适合场景 |
|------|------|--------|---------|---------|
| **Prometheus 指标** | 结构化、可聚合、可告警 | ~60s | Remote Write HTTP POST | Grafana 仪表盘、AlertManager 告警、Autoscaler 决策 |
| **Ray Event Export HTTP** | 详细诊断事件（含完整异常堆栈） | ~0.1-1s | HTTP POST JSON | 诊断数据库、运维工单、oncall 通知 |
| **GCS PubSub + RaySyncer** | 内部实时通知 | ~100ms | gRPC PubSub + RaySyncer labels | CoreWorker 本地缓存、raylet 本地调度感知 |

### 6.2 通道 1：Prometheus 指标

**指标定义**（GcsNodeHealthManager 内 C++ `stats::Count/Gauge`）：

| 指标名 | 类型 | 标签 | 描述 |
|--------|------|------|------|
| `node_unhealthy_event_total` | Count | `node_id`, `node_ip`, `reason_category`, `source`, `affects_resource` | 节点异常事件累计数。每次 MarkNodeUnhealthy +1 |
| `node_unhealthy_entries` | Gauge | `node_id`, `node_ip` | 当前每个节点的异常条目数 |
| `node_health_status` | Gauge | `node_id`, `node_ip`, `reason_category`, `affects_resource` | 节点健康状态值 (1=unhealthy, 0=healthy) |

**标签说明**：
- `node_ip`：从 `GcsNodeInfo.node_manager_address` 获取，供 Grafana/告警直接定位机器
- `affects_resource`：异常影响的资源范围（"GPU" 或 "" 全局），用于按资源类型查看异常分布
- `reason_category`：NodeUnhealthyReason 枚举名（GPU_ERROR / DISK_ERROR 等），仅记录已标记的异常

**Python 端补充**（reporter_agent.py METRICS_GAUGES）：

| 指标名 | 类型 | 标签 | 描述 |
|--------|------|------|------|
| `cluster_unhealthy_nodes` | Gauge | `node_type`, `reason_category`, `SessionName`, `ray_io_cluster` | 集群当前 unhealthy 节点数 |

**导出路径**：

```
GcsNodeHealthManager::RecordMetrics()
  → stats::Count/Gauge::Record(value, {{"node_id", hex}, {"reason_category", "GPU_ERROR"}, {"source", "actor_creation"}})
    → OpenTelemetryMetricRecorder::SetMetricValue()
      → OTLP gRPC → reporter_agent.py ExportMetricsService()
        → Python OpenTelemetryMetricRecorder → RayRemoteWriteExporter
          → Prometheus Remote Write HTTP POST → External Prometheus
```

**配置**（复用已有环境变量）：

| 环境变量 | 默认值 | 说明 |
|---------|--------|------|
| RAY_METRICS_EXPORT_MODE | push | push/pull/remote_write |
| RAY_METRICS_REMOTE_WRITE_ENDPOINT | http://10.81.0.157:9090/api/v1/write | Remote Write 目标 |
| RAY_METRICS_PUSH_INTERVAL_MS | 60000 | 推送间隔 |
| RAY_METRICS_REMOTE_WRITE_BATCH_SIZE | 500 | 批量大小 |

新指标不在 `DEFAULT_EXCLUDE_PATTERNS` 中，无需额外配置即可推送。

**Grafana 告警规则示例**：

```yaml
- alert: NodeGPUError
  expr: node_unhealthy_event_total{reason_category="GPU_ERROR"} > 0
  for: 1m
  labels:
    severity: critical
  annotations:
    summary: "Node {{ $labels.node_id }} has GPU errors"

- alert: NodeHighUnhealthyEntries
  expr: node_unhealthy_entries > 10
  for: 5m
  labels:
    severity: warning
```

### 6.3 通道 2：Ray Event Export HTTP

#### 6.3.1 现有 Ray Event 体系架构

Ray Event Export 从 C++ GCS 端生成事件，经 gRPC 传输到 Python AggregatorAgent，再通过 HTTP POST 推送到外部服务：

```
C++ GCS 端                          Python 端
┌─────────────────┐    gRPC        ┌──────────────────────────────────────┐
│ GcsNodeManager  │ ─────────────→ │ AggregatorAgent.AddEvents()          │
│ GcsActorManager │  AddEvents RPC │   │                                  │
│ GcsJobManager   │                │   ▼                                  │
│ GcsNodeHealth   │                │ MultiConsumerEventBuffer              │
│ Manager (新增)  │                │   │                    │              │
└─────────────────┘                │   ▼                    ▼              │
                                   │ RayEventPublisher   RayEventPublisher│
                                   │ (http_service)      (ray_gcs)        │
                                   │   │                                  │
                                   │   ▼                                  │
                                   │ AsyncHttpPublisher    AsyncGCSTask   │
                                   │ Client                EventsPublisher│
                                   │   │                    Client         │
                                   │   ▼                    │              │
                                   │ HTTP POST JSON         gRPC          │
                                   │ → 外部服务           → GCS           │
                                   └──────────────────────────────────────┘
```

**关键组件**：

| 组件 | 位置 | 职责 |
|------|------|------|
| `RayEventRecorder` | `src/ray/observability/ray_event_recorder.h` | C++ 事件缓冲区，按 (entity_id, event_type) 分组合并，定期 ExportEvents() |
| `EventAggregatorClientImpl` | `src/ray/protobuf/events_event_aggregator_service.proto` | gRPC 客户端，连接 `127.0.0.1:<metrics_agent_port>` |
| `AggregatorAgent` | `python/ray/dashboard/modules/aggregator/aggregator_agent.py` | gRPC 服务端，写入 MultiConsumerEventBuffer |
| `MultiConsumerEventBuffer` | `python/ray/dashboard/modules/aggregator/multi_consumer_event_buffer.py` | 多消费者环形缓冲区，每个 Publisher 独立游标 |
| `RayEventPublisher` | `python/ray/dashboard/modules/aggregator/publisher/ray_event_publisher.py` | 异步消费者，指数退避重试 |
| `AsyncHttpPublisherClient` | `python/ray/dashboard/modules/aggregator/publisher/async_publisher_client.py` | HTTP POST 推送，按 event_type 过滤，proto 转 JSON |

#### 6.3.2 新增 NodeHealthLifecycleEvent 事件类型

**与现有 Node 事件的关系**：

| 维度 | 事件类型 | 触发时机 | Severity |
|------|---------|---------|----------|
| 节点存活 | NodeLifecycleEvent（现有） | 节点注册 ALIVE / 节点死亡 DEAD | INFO |
| 节点健康 | NodeHealthLifecycleEvent（新增） | 节点标记 UNHEALTHY / 健康恢复 | ERROR / INFO |

`NodeHealthLifecycleEvent` 在节点 ALIVE 期间可多次触发（反复 unhealthy/恢复），而 `NodeLifecycleEvent` 仅触发两次（注册+死亡）。两者独立维度。

**新增步骤**（详细 proto/代码见附录 A）：

1. **Proto 定义**：新建 `events_node_health_lifecycle_event.proto`；在 `events_base_event.proto` 中 EventType 新增 `NODE_HEALTH_LIFECYCLE_EVENT = 16`，RayEvent message 新增字段 24
2. **C++ 事件类**：新建 `src/ray/observability/ray_node_health_lifecycle_event.h/.cc`，继承 `RayEvent<NodeHealthLifecycleEvent>`，实现 `GetEntityId/MergeData/SerializeData`
3. **GCS 端发射**：`GcsNodeHealthManager::WriteNodeHealthExportEvent()` 构造事件并调用 `ray_event_recorder_.AddEvents()`
4. **Python 发布配置**：`configs.py` 中 `DEFAULT_HTTP_EXPOSABLE_EVENT_TYPES` 新增 `NODE_HEALTH_LIFECYCLE_EVENT`

**端到端路径**：

```
1. GcsNodeHealthManager::MarkNodeUnhealthy()
   → 构造 RayNodeHealthLifecycleEvent(node_info, entry, MARKED_UNHEALTHY)
   → ray_event_recorder_.AddEvents()

2. RayEventRecorder (缓冲 + 合并)
   → 按 (entity_id=node_id, event_type) 分组
   → 同一节点多次 MarkNodeUnhealthy → state_transitions 追加合并
   → PeriodicalSender 定期 ExportEvents()

3. EventAggregatorClient (gRPC) → 127.0.0.1:<metrics_agent_port>
   → EventAggregatorService::AddEvents RPC

4. AggregatorAgent.AddEvents() (Python gRPC 服务端)
   → 逐个 add_event() 写入 MultiConsumerEventBuffer

5. RayEventPublisher "http_service" (异步消费者)
   → 每 0.1s 或缓冲区满时拉取批次
   → 按 HTTP_EXPOSABLE_EVENT_TYPES 过滤（NODE_HEALTH_LIFECYCLE_EVENT 通过）

6. AsyncHttpPublisherClient.publish()
   → RayEvent proto 转 JSON dict
   → HTTP POST 到 RAY_DASHBOARD_AGGREGATOR_AGENT_EVENTS_EXPORT_ADDR
   → 失败时指数退避重试（默认无限重试，max_backoff=5s）

7. 外部 HTTP 服务接收 JSON
   → 写入诊断数据库 / 触发运维工单 / 通知 oncall
```

**事件 JSON 格式示例**（完整字段见附录 A.4）：

```json
{
  "event_id": "a1b2c3d4e5f6",
  "source_type": "GCS",
  "event_type": "NODE_HEALTH_LIFECYCLE_EVENT",
  "timestamp": "2026-07-19T10:30:00.123456Z",
  "severity": "ERROR",
  "message": "Node marked unhealthy due to GPU_ERROR",
  "session_name": "ray-session-abc123",
  "node_id": "YWJjMTIz",
  "node_health_lifecycle_event": {
    "node_id": "YWJjMTIz",
    "node_manager_address": "10.0.1.5",
    "node_name": "gpu-worker-03",
    "labels": {"ray.io/node-health": "unhealthy", "ray.io/unhealthy-reason": "GPU_ERROR"},
    "state_transitions": [{
      "health_event_type": "MARKED_UNHEALTHY",
      "reason_category": "GPU_ERROR",
      "exception_string": "RuntimeError: CUDA error: an illegal memory access...",
      "worker_id": "ZGVmNDU2",
      "task_or_actor_id": "YWN0b3I3ODk=",
      "source": "actor_creation",
      "unhealthy_entry_count": 3
    }]
  }
}
```

**配置**：

| 环境变量 | 默认值 | 说明 |
|---------|--------|------|
| `RAY_DASHBOARD_AGGREGATOR_AGENT_EVENTS_EXPORT_ADDR` | — | **HTTP 端点 URL**，必填否则不启动 HTTP publisher |
| `RAY_DASHBOARD_AGGREGATOR_AGENT_PUBLISHER_TIMEOUT_SECONDS` | 3 | HTTP 请求超时 |
| `RAY_DASHBOARD_AGGREGATOR_AGENT_PUBLISHER_MAX_RETRIES` | -1 | 最大重试次数（-1=无限） |
| `RAY_DASHBOARD_AGGREGATOR_AGENT_PUBLISHER_MAX_BACKOFF_SECONDS` | 5.0 | 最大退避 |
| `RAY_DASHBOARD_AGGREGATOR_AGENT_EXPOSABLE_EVENT_TYPES` | (含新事件) | 可覆盖可暴露事件列表 |

**启用前提**：
1. `RayConfig::instance().enable_ray_event() == true`（C++ 端开关）
2. `RAY_DASHBOARD_AGGREGATOR_AGENT_EVENTS_EXPORT_ADDR` 已设置（Python 端启动条件）

### 6.4 通道 3：GCS PubSub + RaySyncer（内部同步）

**GCS PubSub**：通过已有 `GcsPublisher::PublishNodeInfo()` 发布节点信息变更。利用 `GcsNodeInfo.labels` 传递健康状态（**无需修改 proto**）：

```
labels["ray.io/node-health"] = "unhealthy"
labels["ray.io/unhealthy-reason"] = "GPU_ERROR"
labels["ray.io/unhealthy-since"] = "1706123456000"
```

恢复时：
```
labels["ray.io/node-health"] = "healthy"
labels.erase("ray.io/unhealthy-reason")
labels.erase("ray.io/unhealthy-since")
```

**RaySyncer**：通过已有 `ResourceViewSyncMessage.labels` 将健康标签同步到所有 raylet（每 100ms 广播）。每个 raylet 本地感知节点健康状态，用于本地调度决策（如 task lease 分配时排除 unhealthy 节点）。

**配置开关**：`node_health_label_propagation_enabled` (default true)

### 6.5 信息分级汇总

| 信息类型 | Prometheus 指标 | Ray Event HTTP | GCS PubSub/RaySyncer |
|---------|----------------|----------------|----------------------|
| 节点 ID | 标签 | 字段 | labels |
| **节点 IP** | **标签** | 字段 | 自动 |
| 异常分类 | 标签 | 字段 | labels |
| **影响资源 (affects_resource)** | **标签** | 字段 | labels |
| 来源 | 标签 | 字段 | — |
| 异常条目数 | Gauge 值 | 字段 | — |
| 健康状态值 | Gauge 值 | — | labels |
| **完整异常字符串** | ❌ 高基数 | ✅ 字段 | ❌ |
| **Worker ID** | ❌ 高基数 | ✅ 字段 | ❌ |
| **Task/Actor ID** | ❌ 高基数 | ✅ 字段 | ❌ |
| 节点名 | ❌ | ✅ 字段 | — |
| 健康标签 | ❌ | ✅ 字段 | labels |
| **聚合/告警** | ✅ 原生 | ❌ 需外部处理 | ❌ |
| **跨集群共享** | ✅ 联邦 | ✅ HTTP 推送 | ❌ 集群内部 |
| **实时性** | ~60s | ~0.1-1s | ~100ms |
| **重试** | ✅ 批量 | ✅ 无限退避 | — |

---

## 7. 可靠性设计

### 7.1 误报防护

**风险**：`ShouldMarkNodeUnhealthy` 误报会导致健康节点被排除调度，造成资源浪费。

**防护措施**：

| 风险场景 | 防护措施 |
|------|---------|
| 用户代码异常字符串包含 "CUDA" 但实际是代码 bug | `USER_ERROR` 也走关键字匹配，但 `node_health_monitor_enabled` 默认 false，需显式启用。匹配关键字列表可配置 |
| SYSTEM_ERROR 误报（用户代码 segfault 不来自 CUDA driver） | `SYSTEM_ERROR` 仅匹配 `node_unhealthy_system_error_keywords` 中的硬件相关关键字才标记。大部分用户代码 segfault 不匹配，不标记 |
| OOM 误排（瞬态内存高峰） | OOM 不标记 unhealthy，由现有 `task_oom_retries` + 指数退避机制处理 |
| Runtime env 失败误排（actor 自身配置问题） | Runtime env 失败不标记 unhealthy，由现有 actor creation task 失败处理（actor DEAD 或按 max_restarts 重启） |
| GPU_ERROR 过度排除（CPU task 无需避开 GPU 故障节点） | `GetUnhealthyNodes(resource_req)` 按资源需求过滤，GPU_ERROR 只排除需要 GPU 的调度 |
| 全部节点被排除 | `SelectForwardingNode` 在全部节点被排除时 fallback 到任意节点 + WARNING 日志，不会导致调度完全卡死 |
| Prometheus/HTTP 端点不可达 | 上报失败不影响 MarkNodeUnhealthy 主流程（异步、best-effort）；指标/事件丢失仅影响外部可见性，不影响调度排除 |

### 7.2 并发模型

`GcsNodeHealthManager` 运行在 GCS 的 io_context 中，所有公共方法通过 `absl::Mutex` 保护：

```
absl::flat_hash_map<NodeID, std::vector<NodeUnhealthyEntryData>> node_unhealthy_entries_
    ABSL_GUARDED_BY(mutex_);
```

- `MarkNodeUnhealthy` / `GetUnhealthyNodes` / `PruneUnhealthyEntries` 均在锁内操作
- Prometheus 指标 Record 在锁外执行（stats::Count::Record 内部线程安全）

### 7.3 持久化策略

**不持久化**：GcsNodeHealthManager 纯内存存储。

**理由**：
1. 节点异常是高频事件，持久化每个 entry 到 GcsTable 开销大
2. GCS 重启后节点异常会很快重新触发（task/actor 重新调度到坏节点 → 再次失败 → 重新标记）
3. GcsNodeInfo.labels 中的健康标签会随节点信息一起持久化（通过现有 NodeTable 存储），GCS 重启后仍可从 labels 恢复 unhealthy 状态

**GCS 重启后恢复**：
- `GcsNodeHealthManager` 初始化时扫描 `alive_nodes_` 的 labels，对 `ray.io/node-health=unhealthy` 的节点重建 `node_unhealthy_entries_`（仅记录一条占位 entry，无完整异常信息）
- 后续真实异常会补充完整 entry

### 7.4 故障模式

| 故障 | 影响 | 恢复 |
|------|------|------|
| GcsNodeHealthManager OOM | GCS crash | GCS 重启，状态从 labels 恢复（7.3） |
| Prometheus 不可达 | 指标丢失，调度排除不受影响 | Prometheus 恢复后指标恢复推送 |
| HTTP 事件端点不可达 | 事件丢失（缓冲区满后丢弃），调度排除不受影响 | 端点恢复后事件恢复推送 |
| GCS PubSub 订阅者断连 | CoreWorker 本地缓存不更新 | 重新订阅，或通过 RaySyncer labels 恢复 |
| RaySyncer 延迟 | raylet 本地健康标签滞后 ~100ms | 自动收敛 |

---

## 8. 配置项

### 8.1 RayConfig（C++ 端）

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `node_health_monitor_enabled` | bool | **false** | 全局开关：是否启用 GCS 节点健康监控。**默认关闭，灰度启用** |
| `node_unhealthy_entry_ttl_ms` | uint64 | 120000 | 异常条目 TTL（2分钟） |
| `node_unhealthy_max_entries_per_node` | uint32 | 50 | 每节点最大异常条目数（防 OOM） |
| `task_scheduling_exclude_unhealthy_nodes` | bool | true | 普通 task 重试时是否排除 unhealthy 节点 |
| `node_health_label_propagation_enabled` | bool | true | 是否通过 RaySyncer labels 传播健康状态 |
| `node_unhealthy_keywords` | string | "CUDA,GPU,cudaError,cuBLAS,cuDNN,NCCL,nvml,disk,I/O error,ENOSPC,network,connection reset,ETIMEDOUT,bus error,SIGBUS,hardware fault" | 异常字符串中匹配到这些关键字才标记 unhealthy（逗号分隔，大小写不敏感）。覆盖 USER_ERROR 和 SYSTEM_ERROR |

### 8.2 GcsNodeInfo Labels 约定

| Key | 值域 | 说明 |
|-----|------|------|
| `ray.io/node-health` | `healthy` / `unhealthy` | 节点健康状态 |
| `ray.io/unhealthy-reason` | NodeUnhealthyReason 枚举名 | 异常分类 |
| `ray.io/unhealthy-since` | Unix millis 字符串 | 首次标记为 unhealthy 的时间 |

**更新时机**：

| 时机 | 操作 |
|------|------|
| MarkNodeUnhealthy | 设置 `ray.io/node-health=unhealthy`，更新 reason 和 since |
| PruneUnhealthyEntries 后无剩余 | 设置 `ray.io/node-health=healthy`，移除 reason 和 since |
| 节点 DEAD | labels 随 GcsNodeInfo 一起归档 |

---

## 9. 灰度启用策略

### 9.1 三阶段启用

| 阶段 | 配置 | 效果 | 观察指标 |
|------|------|------|---------|
| **Stage 1: 观察期** | `node_health_monitor_enabled=true`，`task_scheduling_exclude_unhealthy_nodes=false` | 仅记录指标和事件，不影响调度 | Prometheus 指标：`node_unhealthy_event_total` 按原因分类分布；误报率（人工对比异常与实际节点状态） |
| **Stage 2: 排除启用** | `task_scheduling_exclude_unhealthy_nodes=true` | 调度开始排除 unhealthy 节点 | Task 重试成功率；`task_retry_on_unhealthy_node_total`；fallback 触发频率（是否所有节点被排除） |
| **Stage 3: 全量启用** | 调整 TTL 和 max_entries | 稳定运行 | unhealthy 节点恢复时间分布；异常节点数 / 总节点数比例 |

### 9.2 回滚

任一阶段发现问题，设置 `node_health_monitor_enabled=false` 即可：
- GcsNodeHealthManager 停止 MarkNodeUnhealthy 调用
- 现有 entries 通过 TTL 自动清理
- 调度恢复为不排除任何节点（现有行为）
- 无需重启 GCS

---

## 10. 调度排除集成

### 10.1 Actor 调度排除

```
GcsActorScheduler::SelectForwardingNode(actor):
  1. actor_resource_req = actor->GetRequiredResources()  // 获取 actor 资源需求
  2. excluded_nodes = gcs_node_health_manager_.GetUnhealthyNodes(actor_resource_req)
     // 按资源需求过滤：需要 GPU → 排除 GPU_ERROR 节点 + 全局异常节点
     //                不需要 GPU → 只排除全局异常节点
  3. 有资源需求: 优先 owner 节点(若不在排除集), 否则 SelectRandomAliveNodeExcluding(excluded_nodes)
     无资源需求: SelectRandomAliveNodeExcluding(excluded_nodes)
  4. 全部被排除 → fallback 任意节点 + WARNING
```

### 10.2 普通 Task 调度排除

CoreWorker TaskManager 重试时，从本地 `unhealthy_nodes_` 缓存获取排除集（通过 GCS PubSub 订阅 GcsNodeInfo labels 更新），传入 `LeasePolicy::GetBestNodeForLease(spec, excluded_nodes)`。

### 10.3 Phase 划分

| Phase | 工作项 |
|-------|--------|
| Phase 1: C++ 核心 | GcsNodeHealthManager 核心类 + Prometheus 指标 + GCS PubSub + Actor 调度排除 |
| Phase 2: Task 集成 + 外部上报 | CoreWorker LeasePolicy 扩展 + TaskManager 上报 + Ray Event Export HTTP |
| Phase 3: 精细化 | 关键字列表调优 + 资源感知排除细化 |

---

## 11. 文件清单

### 11.1 新增文件

| 文件 | 描述 | Phase |
|------|------|-------|
| `src/ray/gcs/gcs_node_health_manager.h` | GcsNodeHealthManager 类声明 | P0 |
| `src/ray/gcs/gcs_node_health_manager.cc` | GcsNodeHealthManager 类实现 | P0 |
| `src/ray/protobuf/public/events_node_health_lifecycle_event.proto` | NodeHealthLifecycleEvent proto | P1 |
| `src/ray/observability/ray_node_health_lifecycle_event.h` | Ray 事件类声明 | P1 |
| `src/ray/observability/ray_node_health_lifecycle_event.cc` | Ray 事件类实现 | P1 |
| `src/ray/gcs/tests/gcs_node_health_manager_test.cc` | GcsNodeHealthManager 单元测试 | P0 |
| `src/ray/observability/tests/ray_node_health_lifecycle_event_test.cc` | 事件类单元测试 | P1 |
| `python/ray/dashboard/modules/aggregator/tests/test_ray_node_health_events.py` | 集成测试 | P1 |

### 11.2 修改文件

| 文件 | 变更 | Phase |
|------|------|-------|
| `src/ray/protobuf/common.proto` | 新增 NodeUnhealthyReason enum | P0 |
| `src/ray/protobuf/gcs.proto` | 新增 NodeUnhealthyEntry message + ReportNodeUnhealthyEvent RPC | P0 |
| `src/ray/protobuf/public/events_base_event.proto` | EventType 新增 NODE_HEALTH_LIFECYCLE_EVENT (16)；RayEvent 新增字段 24 | P1 |
| `src/ray/protobuf/export_node_data.proto` | 新增 health_status, unhealthy_reason, unhealthy_since_ms | P1 |
| `src/ray/common/ray_config_def.h` | 新增 6 个配置项 | P0 |
| `src/ray/gcs/gcs_node_manager.h` | 新增 SelectRandomAliveNodeExcluding 方法 | P0 |
| `src/ray/gcs/gcs_node_manager.cc` | 实现 SelectRandomAliveNodeExcluding | P0 |
| `src/ray/gcs/gcs_server.h` | 新增 gcs_node_health_manager_ 成员 | P0 |
| `src/ray/gcs/gcs_server.cc` | 初始化 GcsNodeHealthManager + 注册 RPC + WorkerDeadListener 集成 | P0 |
| `src/ray/gcs/actor/gcs_actor_scheduler.h` | 新增 gcs_node_health_manager_ 引用 | P0 |
| `src/ray/gcs/actor/gcs_actor_scheduler.cc` | SelectForwardingNode 合并两层排除集合 | P0 |
| `src/ray/gcs/actor/gcs_actor_manager.cc` | OnWorkerDead 新增 MarkNodeUnhealthy 调用 | P0 |
| `src/ray/core_worker/lease_policy.h` | 扩展带 excluded_nodes 的 GetBestNodeForLease | P1 |
| `src/ray/core_worker/lease_policy.cc` | 实现带排除的 GetBestNodeForLease | P1 |
| `src/ray/core_worker/task_manager.h` | 新增 excluded_nodes_ + 上报方法 | P1 |
| `src/ray/core_worker/task_manager.cc` | RetryTaskIfPossible 新增上报和排除逻辑 | P1 |
| `src/ray/core_worker/core_worker.h` | 新增 ReportNodeUnhealthyEvent RPC client | P1 |
| `src/ray/core_worker/core_worker.cc` | 实现 RPC client + unhealthy_nodes_ 缓存 | P1 |
| `python/ray/dashboard/modules/aggregator/publisher/configs.py` | DEFAULT_HTTP_EXPOSABLE_EVENT_TYPES 新增 NODE_HEALTH_LIFECYCLE_EVENT | P1 |
| `python/ray/dashboard/modules/reporter/reporter_agent.py` | METRICS_GAUGES 新增 cluster_unhealthy_nodes | P1 |
| `python/ray/dashboard/consts.py` | 新增 unhealthy_nodes 相关标签键 | P1 |
| `src/ray/observability/BUILD.bazel` | 新增编译目标 | P1 |

---

## 12. 验证方案

### 12.1 Prometheus 指标验证

1. 模拟节点 CUDA Error → 验证 `node_unhealthy_event_total{reason_category="GPU_ERROR"}` +1
2. 验证 `node_unhealthy_entries{node_id="xxx"}` 正确反映条目数
3. TTL 过期后验证 `node_health_status` 从 1 变为 0
4. 在 Grafana 中配置面板和告警规则

### 12.2 Export 事件验证

1. **C++ 端**：验证 `RayNodeHealthLifecycleEvent` 构造正确（字段映射、MergeData 合并、SerializeData 序列化）
2. **gRPC 传输**：验证 `AggregatorAgent.AddEvents()` 正确接收 `NODE_HEALTH_LIFECYCLE_EVENT` 类型事件
3. **HTTP 发布**：验证 `AsyncHttpPublisherClient` 正确过滤并发送到 `RAY_DASHBOARD_AGGREGATOR_AGENT_EVENTS_EXPORT_ADDR`
4. **JSON 格式**：验证 HTTP POST 的 JSON 包含完整字段（node_id, reason_category, exception_string, worker_id, task_or_actor_id, state_transitions）
5. **旧版兼容**：验证 `enable_ray_event()=false` 时 `ExportNodeData` 包含 health_status/unhealthy_reason/unhealthy_since_ms
6. **合并优化**：验证同一节点短时间内多次 MarkNodeUnhealthy 事件在缓冲区中合并为单个 RayEvent（state_transitions 追加）

### 12.3 调度排除验证

1. Actor creation task 在节点 A 因 CUDA Error 失败 → 验证节点 A 出现在 global unhealthy（affects_resource=GPU）
2. 验证 RestartActor（需要 GPU）选择的节点不在 excluded_nodes 中
3. **资源感知**：验证不需要 GPU 的 task 可以调度到节点 A（GPU_ERROR 只排除需要 GPU 的调度）
4. 验证 DISK_ERROR / NETWORK_ERROR 节点对所有 task/actor 排除（全局影响）
5. **OOM 不标记**：验证 OOM 节点不出现在 unhealthy 列表中
6. 全部节点被排除时验证 fallback 机制（选择任意节点 + WARNING 日志）
### 12.4 灰度启用验证

1. Stage 1：`task_scheduling_exclude_unhealthy_nodes=false`，验证调度不受影响，指标正确记录
2. Stage 2：`task_scheduling_exclude_unhealthy_nodes=true`，验证调度排除生效，fallback 不频繁触发
3. 回滚：`node_health_monitor_enabled=false`，验证 entries TTL 清理，调度恢复

---

## 附录 A：Proto 与代码详细定义

### A.1 NodeUnhealthyReason（common.proto）

```protobuf
enum NodeUnhealthyReason {
  NODE_UNHEALTHY_UNSPECIFIED = 0;
  NODE_UNHEALTHY_GPU_ERROR = 1;
  NODE_UNHEALTHY_OOM = 2;
  NODE_UNHEALTHY_DISK_ERROR = 3;
  NODE_UNHEALTHY_NETWORK_ERROR = 4;
  NODE_UNHEALTHY_RUNTIME_ENV_ERROR = 5;
  NODE_UNHEALTHY_SYSTEM_ERROR = 6;
  NODE_UNHEALTHY_UNKNOWN_ENV_ERROR = 99;
}
```

### A.2 NodeUnhealthyEntry + RPC（gcs.proto）

```protobuf
message NodeUnhealthyEntry {
  bytes node_id = 1;
  uint64 timestamp_ms = 2;
  NodeUnhealthyReason reason_category = 3;
  string exception_string = 4;
  bytes worker_id = 5;
  bytes task_or_actor_id = 6;
  string source = 7;
  // 异常影响的资源类型。空=""表示全局影响（DISK_ERROR/NETWORK_ERROR/SYSTEM_ERROR）；
  // "GPU"表示仅影响需要 GPU 的调度（GPU_ERROR）。
  string affects_resource = 8;
}

// 在 NodeInfoGcsService 中新增
rpc ReportNodeUnhealthyEvent(ReportNodeUnhealthyEventRequest)
    returns (ReportNodeUnhealthyEventReply);

message ReportNodeUnhealthyEventRequest {
  NodeUnhealthyEntry entry = 1;
}

message ReportNodeUnhealthyEventReply {}
```

### A.3 NodeHealthLifecycleEvent Proto

```protobuf
// src/ray/protobuf/public/events_node_health_lifecycle_event.proto
syntax = "proto3";
package ray.rpc.events;
import "events_base_event.proto";

message NodeHealthLifecycleEvent {
  enum HealthEventType {
    MARKED_UNHEALTHY = 0;
    HEALTH_RECOVERED = 1;
  }
  message HealthStateTransition {
    HealthEventType health_event_type = 1;
    google.protobuf.Timestamp timestamp = 2;
    string reason_category = 3;
    string exception_string = 4;
    bytes worker_id = 5;
    bytes task_or_actor_id = 6;
    string source = 7;
    uint32 unhealthy_entry_count = 8;
    string affects_resource = 9;  // "GPU" 或 ""（全局）
  }
  bytes node_id = 1;
  string node_manager_address = 2;
  string node_name = 3;
  map<string, string> labels = 4;
  repeated HealthStateTransition state_transitions = 5;
}
```

在 `events_base_event.proto` 中扩展：

```protobuf
enum EventType {
  // ... 现有 1-15 ...
  NODE_HEALTH_LIFECYCLE_EVENT = 16;
}

message RayEvent {
  // ... 现有字段 1-23 ...
  NodeHealthLifecycleEvent node_health_lifecycle_event = 24;
}
```

### A.4 RayNodeHealthLifecycleEvent 事件类

继承体系：

```
RayEventInterface
  └── RayEvent<T> (CRTP 模板基类)
        ├── RayNodeDefinitionEvent       : RayEvent<NodeDefinitionEvent>
        ├── RayNodeLifecycleEvent        : RayEvent<NodeLifecycleEvent>
        ├── RayNodeHealthLifecycleEvent   : RayEvent<NodeHealthLifecycleEvent>  ← 新增
        ├── RayActorDefinitionEvent       : RayEvent<ActorDefinitionEvent>
        └── RayActorLifecycleEvent        : RayEvent<ActorLifecycleEvent>
```

需要实现的方法：

| 方法 | 职责 | 参考实现 |
|------|------|---------|
| 构造函数 | 从 GcsNodeInfo + NodeUnhealthyEntryData 映射到 proto 字段 | RayNodeLifecycleEvent |
| `GetEntityId()` | 返回 `data_.node_id()` | 同 RayNodeLifecycleEvent |
| `MergeData()` | 追加 state_transitions | 同 RayNodeLifecycleEvent |
| `SerializeData()` | `event.mutable_node_health_lifecycle_event()->Swap(&data_)` | 同 RayNodeLifecycleEvent |
| `GetEventType()` | 返回 `NODE_HEALTH_LIFECYCLE_EVENT` | — |

构造函数字段映射：

| GcsNodeInfo / Entry 字段 | Proto 字段 |
|--------------------------|-----------|
| `node_id` | `data_.node_id` |
| `node_manager_address` | `data_.node_manager_address` |
| `node_name` | `data_.node_name` |
| `labels` | `data_.labels` |
| `entry.timestamp_ms` | `state_transition.timestamp` |
| MARKED_UNHEALTHY / HEALTH_RECOVERED | `state_transition.health_event_type` |
| `NodeUnhealthyReason_Name(entry.reason_category)` | `state_transition.reason_category` |
| `entry.exception_string` | `state_transition.exception_string` |
| `entry.worker_id` | `state_transition.worker_id` |
| `entry.task_or_actor_id` | `state_transition.task_or_actor_id` |
| `entry.source` | `state_transition.source` |
| `node_unhealthy_entries_[node_id].size()` | `state_transition.unhealthy_entry_count` |
| `entry.affects_resource` | `state_transition.affects_resource` |

Severity 级别：MARKED_UNHEALTHY = `ERROR`，HEALTH_RECOVERED = `INFO`

### A.5 旧版 Export API 扩展

当 `enable_ray_event() == false` 时，走旧版 `ExportNodeData` 路径：

```protobuf
// src/ray/protobuf/export_node_data.proto
message ExportNodeData {
  // ... 现有字段 ...
  string health_status = 11;       // "healthy" / "unhealthy" / "degraded"
  string unhealthy_reason = 12;    // 异常原因分类名
  uint64 unhealthy_since_ms = 13; // 首次标记为 unhealthy 的时间戳
}
```

### A.6 外部消费端建议

**诊断数据库**：将 HTTP 接收的事件写入 ClickHouse/ElasticSearch，支持：
- 按 node_id 查询某节点历史健康事件
- 按 reason_category 聚合统计各类异常频次
- 按 exception_string 关键词搜索相似故障

**运维工单**：
- `MARKED_UNHEALTHY` + `reason_category=GPU_ERROR` → 自动创建 GPU 故障工单
- 同一节点短时间内多次 MARKED_UNHEALTHY → 升级为紧急工单

**oncall 通知**：
- `severity=ERROR` 的事件 → 企业微信/飞书告警
