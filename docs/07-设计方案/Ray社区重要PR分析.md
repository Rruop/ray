# Ray 社区重要 PR 分析

本文档记录 Ray 社区近期重要 PR 的分析，涵盖动机、问题、方案和设计权衡。

---

## 1. PR #63331 — [data] 支持集群内子集群标签调度

**链接**: https://github.com/ray-project/ray/pull/63331

### 核心动机

在同一集群中支持训练和验证数据集的隔离调度（subcluster label scheduling）。

### 解决的问题

**异步验证（async validation）场景**：训练任务和验证任务在同一集群中同时运行时，如果不做子集群隔离：

1. **资源争抢**：验证任务抢占训练任务的资源，影响训练吞吐
2. **背压干扰**：验证数据集的流水线与训练流水线共享资源池，互相影响 backpressure

### 方案

通过 `label_selector`（如 `subcluster: "train"` / `subcluster: "validation"`）将集群节点划分为逻辑子集群，让训练数据集的任务只调度到标记为 "train" 的节点，验证数据集的任务只调度到 "validation" 的节点。

**API 示例**：

```python
# 交错验证（interleaved validation）
dataset_config = ray.train.DataConfig(
    datasets_to_split=["train", "test"],
    execution_options={
        "train": ExecutionOptions(label_selector={"subcluster": "train"}),
        "test": ExecutionOptions(label_selector={"subcluster": "validation"}),
    },
)

# 异步验证（async validation）— 需在验证函数中设置
def validate_fn(checkpoint):
    validation_dataset = ...
    validation_dataset.context.execution_options.label_selector = {
        "subcluster": "validation"
    }
```

### 具体改动

将 `label_selector` 通过以下调用链路透传：

1. Map operators（已有 `scheduling_strategy` 的合并逻辑，现在同样合并 `label_selector`）
2. 各种调用 `.remote()` 的 operator（如 `LimitOperator`、`ZipOperator`）
3. Exchange schedulers（sort、repartition、aggregation、random_shuffle）
4. Planning-time 任务（file metadata fetch、parquet sampling）
5. Construction-time 任务（from_pandas/numpy/arrow）
6. Conversion-time 任务（to_pandas/numpy/arrow、block-num-rows）
7. 特殊功能（RandomAccess、checkpoint）

### 已知缺口

1. `_PushBasedShuffleStage` 使用 `NodeAffinitySchedulingStrategy` 而非 `label_selector`——实践中没问题，因为前面的步骤已经将 block 放在了正确的子集群中
2. 多个 datasink 从写任务内部发起嵌套 `.remote()` 调用

### 设计权衡

讨论了一种替代方案：让 `DataConfig` 成为异步验证场景的唯一公共 API。但这意味着：需要将 dataset 从 driver → trainer → validator 传递，需要记住不提前 split 验证 dataset，需要一种方式让验证函数获取未 split 的 Dataset。最终决定当前方案更简洁。

### 本质

同一物理集群内的资源逻辑隔离——比完全拆成两个独立集群更经济，共享运维成本，同时隔离工作负载。

---

## 2. PR #63773 — [Active-passive] Phase1: 引入原生 C++ Leader Election Client

**链接**: https://github.com/ray-project/ray/pull/63773
**关联 Issue**: https://github.com/ray-project/ray/issues/63643
**关联 REP**: https://github.com/ray-project/enhancements/pull/65

### 核心动机

将 Ray 从单点故障的 GCS 模型过渡到高可用的 Active-Passive 架构，将恢复延迟从分钟级降至秒级。

### GCS 容错 vs 双主选举——解决的是不同层次的问题

| 维度 | GCS 容错（现有） | Active-Passive 双主选举（新增） |
|------|-----------------|-------------------------------|
| 机制 | GCS 挂了之后重启恢复，从 Redis 重新加载状态 | 提前启动热备 head 节点，故障时通过 Lease 选举立即接管 |
| RPO | ≈0 | ≈0 |
| RTO | 分钟级 | 秒级 |
| 期间状态 | 集群不可用 | standby 已在运行，只需完成 lease 握手即可接管 |
| 额外保护 | 无 | Redis fencing token（单调递增 lease epoch），防止脑裂脏写 |

### 方案细节

#### Kubernetes Lease Client

- `LeaderLeaseClientInterface`：平台无关的 lease 获取、续约、释放接口
- `K8sLeaseClient`：与 Kubernetes `/apis/coordination.k8s.io/v1/namespaces/{namespace}/leases` API 交互
- 采用 client-go 库最佳实践：时钟偏移容忍、缓存 lease 对象并直接 PUT、优雅退出与快速降级

#### Leader Elector 状态机（含 Watchdog）

- **Election Thread**：两种模式循环运行
  - Standby Mode：周期性轮询 lease client 尝试获取 lease，通过条件变量休眠避免 CPU 空转
  - Active Mode：周期性续约 lease 心跳，若 lease 被其他节点抢占则立即降级
- **Safety Watchdog Thread**：使用单调时钟持续监控上次成功续约的间隔。若续约持续失败超过 `renew_deadline_seconds`，watchdog 在其他候选者窃取 lease 之前强制立即降级（触发 `on_stopped_leading` 自杀回调）
- **响应式线程唤醒**：watchdog 触发降级时，election thread 立即停止续约尝试

### 关于资源浪费

standby head 节点会占用资源，但浪费很有限：

1. Head 节点本身很轻量，通常只分配少量 CPU/内存，远低于 GPU worker
2. Standby 几乎不干活：只保持 RPC 存活（health check），拒绝查询和写入，不加载 Redis 数据，不做任务调度
3. 这是经典的可用性换资源的权衡——用一个小 head 的开销换秒级故障恢复
4. 这是可选能力，不做双主也可以继续用现有 GCS FT（重启恢复）

### 完整实现计划（Issue #63643）

- [x] 独立的 C++ Leader Election Client
- [ ] GCS 和其他 head 组件重构以支持 Passive Mode（standby 模式：RPC 存活但拒绝查询和写入）
- [ ] GCS 与 leader election client 集成（延迟加载数据直到被提升、promotion/step-down 流程、readiness probe 依赖 leadership 状态）
- [ ] Redis fencing token（单调递增 lease epoch 保护写操作）
- [ ] 监控指标
- [ ] KubeRay API 和 controller 支持
- [ ] E2E 故障切换测试

---

## 3. PR #64835 — [core][taskEvents out of GCS][2/n] 用 Ray Event Recorder 替换 Task Event Buffer

**链接**: https://github.com/ray-project/ray/pull/64835
**前置 PR**: https://github.com/ray-project/ray/pull/64168

### 核心动机

将 core worker 中上报 task events 的路径从 `task_event_buffer` 迁移到 `RayTaskEventRecorder`（基于 Ray 统一事件框架），同时为 GCS 减负。

### 解决的问题

1. **事件丢失 bug**：`task_event_buffer` 存在已知 bug——在 aggregator agent 启动完成之前产生的事件会被丢弃。`RayEventRecorder` 已修复此问题，事件会可靠送达。
2. **GCS 卸载**：这是 "taskEvents out of GCS" 系列工作的第 2 步，最终目标是将 task events 的上报路径从 GCS 迁移到 aggregator agent，减轻 GCS 的负担。
3. **语义保证**：`RayTaskEventRecorder` 继承了 `task_event_buffer` 的两个关键特性：
   - 发送丢弃事件的元数据（用于可观测性）
   - "同一 task attempt 的事件要么全部刷出要么全部不刷"的原子性保证

### 具体改动

1. 新增 `RayTaskDefinitionEvent`、`RayActorTaskDefinitionEvent`、`RayTaskLifecycleEvent`、`RayTaskProfileEvent` 类，扩展 `RayEventInterface`
2. 在 `CoreWorkerProcessImpl::CreateCoreWorker` 中构造 `RayTaskEventRecorder`，传递给 `TaskManager`、`TaskReceiver`、`ActorExecutionQueues` 等
3. 在 `task_event_buffer_->AddTaskEvent` 的并行位置使用 `AddEvents`
4. 新增 `worker::RecordTaskStatusEventToRecorderIfNeeded`
5. 通过 `enable_ray_task_event_recorder` flag 控制切换

### Aggregator Agent 如何降低 GCS 压力

**当前路径**：每个 core worker 直接通过 gRPC 向 GCS 上报 task events → N 个 worker = N 条并发 RPC 流，GCS 逐个接收、解析、存储。

**新路径**：每个节点上的 aggregator agent 汇聚该节点所有 worker 的 events，批量打包后统一发给 GCS → GCS 从处理 N 条流变成处理 M 条流（M = 节点数，M << N），批量发送减少 RPC 调用次数和序列化开销。

本质上就是**加了一层本地聚合，把 N:1 变成 M:1**。

### 与双主选举的关系

GCS 越轻量（处理的流量越少），standby 节点接管时需要恢复的状态也越少，切换也就越快越稳。两者是互补的优化方向。

### Aggregator Agent 不是新组件

它已存在于 `python/ray/dashboard/modules/aggregator/aggregator_agent.py`，作为每个节点上 dashboard agent 的一部分运行，是一个中间层，收集 core worker 上报的事件，然后批量转发给 GCS 或外部服务。

---

## 4. PR #63939 — [core] 精确化 raylet 强制 GC 的触发条件

**链接**: https://github.com/ray-project/ray/pull/63939

### 解决的问题

raylet 中有一个"强制全局 GC"的逻辑，当资源紧张时触发所有 worker 执行 Python GC 来回收资源。但触发条件写得很混乱——用 `IsWorkerAvailableForScheduling` 检查"没有正在运行的 task worker"，函数名和实现不匹配，导致代码难以理解且行为不精确。

### 原始意图追溯

追溯原始 PR #8322 的目的：**打破 actor handle 循环引用导致的死锁**。场景是 actor 被删除但 handle 的循环引用导致 worker 不释放，新 task/actor 无法调度，整个集群卡住。

强制 GC 唯一能解决的就是这种情况——因为：
- 普通 object ref 已有 object store 百分比阈值触发 GC 的代码
- `ray.put` 也有空间不足时触发 GC 的代码
- 全局 GC 唯一能额外回收的就是 actor handle 循环引用

### 方案

将触发条件从模糊的"没有 running task worker + 有 pending 任务"改为明确语义的"所有 worker 都是 actor worker"。

`IsWorkerAvailableForScheduling` 实际只检查"没有 running task worker"，等于"所有 worker 都是 actor worker"，但命名和实现都令人困惑。新代码直接表达这个语义。

### 行为差异

几乎是纯重构，仅一个边界场景有差异：

- 场景：有多个 actor + 一个卡在 `ray.get()` 的 task + 有 pending 任务
- 旧逻辑：触发 GC（因为 `ray.get()` 中的 task 不算 running）
- 新逻辑：不触发（因为并非所有 worker 都是 actor）
- 评估：安全，因为这种场景下 GC 本来也帮不上忙（actor handle 循环引用才是 GC 能解决的唯一问题）

---

## 5. PR #63479 — [core][1/2] Topology Aware Scheduling 公共 API

**链接**: https://github.com/ray-project/ray/pull/63479
**前置 PR（私有 API）**: https://github.com/ray-project/ray/pull/61442
**关联 REP**: https://github.com/ray-project/enhancements/pull/66

### 解决的问题

Ray 现有的 placement group 调度策略（`STRICT_PACK`、`PACK` 等）只在**节点级别**生效，无法感知更上层的拓扑结构（如机架/rack、GPU domain）。

**具体场景——GB300 机架**：一个 GB300 rack 由 18 个节点组成，共享 NVLink domain，节点间有极速互联。用户希望把 18 个 bundle 部署到同一个 rack 内以利用 NVLink 的高速带宽，但现有 API 做不到：

- `STRICT_PACK` 会把所有 bundle 塞进**单个节点**（而非一个 rack 内的 18 个节点）
- `PACK` 无法保证 bundle 落在同一个 rack 内
- `label_selector` 可以手动指定标签，但是**静态的**——如果该 rack 的节点全挂了，placement group 不会自动迁移到新 rack

### 方案

引入 **Topology Aware Scheduling** 公共 API，让用户定义**多层拓扑感知**的调度策略：

```python
ray.util.placement_group(
    bundles=[{"GPU": 4, "CPU": 2}] * 18,
    topology_strategy=[{
        "ray.io/gpu-domain": "STRICT_PACK"  # 在 GPU domain 级别严格打包
    }]
)
```

### 核心设计点

1. **多层拓扑**：拓扑层级通过标签（如 `ray.io/gpu-domain`）定义，策略（如 `STRICT_PACK`）作用在标签对应的拓扑域上。当前只支持一层拓扑 + 一个标签 + `STRICT_PACK`，未来会扩展。

2. **节点级策略统一**：当指定了 `topology_strategy` 后，不再允许单独设 `strategy`，节点级策略必须通过 `ray.io/node-id` 标签在 topology 内指定，避免语义冲突。

3. **废弃旧的 `bundle_label_selectors`**：之前为 GB200/300 做的私有 API（`bundle_label_selector=[{"ray.io/accelerator-type": "GB300"}]`）被废弃，统一走这个更通用的拓扑感知路径。

4. **Dashboard 和 CLI 适配**：Dashboard UI 和 `ray list placement-groups --detail` 都新增了 `topology_strategy` 和 `topology_assignments` 的展示。

### API 验证约束

- 当前只支持一层拓扑感知调度
- 最多支持一个节点级放置策略和一个标签感知放置策略
- 标签感知策略只支持 `STRICT_PACK`
- `_validate_topology_strategy` 负责验证上述约束

### Label Locality Private 功能废弃

之前 GB200/300 的 `bundle_label_selectors` 功能被废弃，因为新的 `topology_strategy` 是更通用的版本。虽然是 alpha 功能，但统一路径对用户更直观。

### 本质

把 placement group 的调度粒度从"节点"提升到"拓扑域"（rack/GPU domain），让调度器能感知物理拓扑结构，确保相关任务被放在同一拓扑域内以利用高速互联（NVLink），同时为未来的 rack 级容错（整个 rack 挂掉后自动迁移到另一个 rack）打下基础。
