# Ray Autoscaler 节点移除问题排查与代码解析

## 1. 问题现象

集群日志中出现 autoscaler 大量移除 worker 节点的信息：

```
Removing 1 nodes of type node_3b51404dc84c141820ca9258f064df743a7bef15a6a11c009153477d
(max number of worker nodes reached).
```

同时业务报错：

```
ray.exceptions.OwnerDiedError: Failed to retrieve object
00c942857d03a1e68f788d087173f497c3b67c310500000002e1f505.
```

## 2. 根因分析

### 2.1 直接原因：`max_workers: 0`

通过 head 节点上的 monitor.log 确认，autoscaler 加载的配置中 `max_workers: 0`：

```yaml
# /tmp/ray/session_latest/logs/monitor.log 中记录的 Autoscaling Config
auth: {}
available_node_types:
  ray.head.default:
    max_workers: 0
    min_workers: 0
    node_config: {}
    resources: {}
cluster_name: default
head_node_type: ray.head.default
idle_timeout_minutes: 0
max_workers: 0          # <--- 根因：不允许任何 worker 存在
provider:
  disable_launch_config_check: true
  type: readonly
  use_node_id_as_ip: true
upscaling_speed: 1.0
```

由于 `max_workers: 0`，autoscaler 计算出 `max_num_nodes = 0 + 1 = 1`（仅允许 head node），
当前集群有大量 worker 节点，所以 scheduler 持续驱逐所有 worker。

monitor.log 中累计出现 **5846 次** "Removing ... max number of worker nodes reached" 日志。

### 2.2 `max_workers: 0` 的来源

monitor.py 启动时日志：

```
No autoscaling config provided: use read only node provider.
```

**没有提供 autoscaling config 时**，autoscaler 使用 readonly node provider，动态从 GCS 发现集群节点生成配置。

代码路径：`python/ray/autoscaler/v2/instance_manager/config.py:515-544`

```python
def refresh_cached_autoscaling_config(self) -> AutoscalingConfig:
    ray_cluster_resource_state = get_cluster_resource_state(self._gcs_client)

    available_node_types = {}
    head_node_type = None

    for node_state in ray_cluster_resource_state.node_states:
        node_type = node_state.ray_node_type_name
        if not node_type:
            node_type = format_readonly_node_type(binary_to_hex(node_state.node_id))

        if is_head_node(node_state):
            head_node_type = node_type

        if node_type not in available_node_types:
            available_node_types[node_type] = {
                "resources": dict(node_state.total_resources),
                "min_workers": 0,
                # head node: max_workers=0; worker node type: max_workers=1
                "max_workers": 0 if is_head_node(node_state) else 1,
                "node_config": {},
            }
        elif not is_head_node(node_state):
            # 每发现一个同类型 worker, max_workers +1
            available_node_types[node_type]["max_workers"] += 1

    if available_node_types:
        self._configs["available_node_types"].update(available_node_types)
        # 全局 max_workers = 节点类型数量（不是节点总数）
        self._configs["max_workers"] = len(available_node_types)
```

**问题**：全局 `max_workers` 被设为 `len(available_node_types)`（节点类型数），而非节点总数。
当只有一个 `ray.head.default` 类型时，`max_workers` 就等于 1，但 head 类型的 `max_workers: 0`，
最终全局 `max_workers` 可能被覆盖为 0（取决于配置合并逻辑和时序）。

### 2.3 `max_workers` 配置读取链路

```
YAML 配置文件 / 动态生成
  → _configs["max_workers"]
    → AutoscalingConfig.get_max_num_worker_nodes()   # config.py:371-372
      → return self.get_config("max_workers", None)
    → AutoscalingConfig.get_max_num_nodes()           # config.py:374-378
      → return max_workers + 1  # 加 1 是给 head node
    → SchedulingRequest.max_num_nodes                 # reconciler.py:1133
      → Scheduler._enforce_max_workers_global()       # scheduler.py:951
```

## 3. 终止流程完整调用栈

### 3.1 Autoscaler 主循环

```
Autoscaler.update_autoscaling_state()              # autoscaler.py:175
  → Reconciler.reconcile()                         # reconciler.py:64
    → Reconciler._step_next()                       # reconciler.py:236
      → scheduler.schedule(sched_request)           # reconciler.py:1148
        → _enforce_max_workers_global(ctx)          # scheduler.py:951
```

### 3.2 Scheduler 决策：选择要终止的节点

`python/ray/autoscaler/v2/scheduler.py:1061-1105`

```python
@staticmethod
def _enforce_max_workers_global(
    ctx: "ResourceDemandScheduler.ScheduleContext",
) -> None:
    all_nodes = ctx.get_nodes()
    terminating_nodes = []
    non_terminating_nodes = []

    for node in all_nodes:
        if node.status == SchedulingNodeStatus.TO_TERMINATE:
            terminating_nodes.append(node)
        else:
            non_terminating_nodes.append(node)

    num_max_nodes = ctx.get_max_num_nodes()

    # 计算需要终止多少个节点
    num_to_terminate = (
        max(len(non_terminating_nodes) - num_max_nodes, 0) if num_max_nodes else 0
    )

    if num_to_terminate <= 0:
        return

    # 选择要终止的节点（优先选择非 Ray running、资源利用率低的）
    (
        to_terminate_nodes,
        non_terminating_nodes,
    ) = ResourceDemandScheduler._select_nodes_to_terminate(
        non_terminating_nodes,
        num_to_terminate,
        TerminationRequest.Cause.MAX_NUM_NODES,
        max_num_nodes=num_max_nodes,
    )
```

### 3.3 选择终止节点的排序策略

`python/ray/autoscaler/v2/scheduler.py:1148-1193`

节点排序优先级（`_sort_nodes_for_termination`）：
1. 非 Ray running 的节点优先（还没完全启动的优先杀）
2. idle 节点优先
3. 资源利用率低的优先
4. head node 永远不会被选为终止对象

### 3.4 终止原因枚举

`src/ray/protobuf/instance_manager.proto:178-189`

```protobuf
message TerminationRequest {
  enum Cause {
    UNKNOWN = 0;
    IDLE = 1;                    // 空闲超时
    MAX_NUM_NODE_PER_TYPE = 2;   // 单类型节点数上限
    MAX_NUM_NODES = 3;           // 全局节点数上限（本次问题）
    OUTDATED = 4;                // 过时节点
  }
}
```

### 3.5 日志输出

`python/ray/autoscaler/v2/event_logger.py:78-96`

```python
if terminate_requests:
    termination_by_causes_and_type = defaultdict(int)
    for req in terminate_requests:
        termination_by_causes_and_type[(req.cause, req.instance_type)] += 1

    cause_reason_map = {
        TerminationRequest.Cause.OUTDATED: "outdated",
        TerminationRequest.Cause.MAX_NUM_NODES: "max number of worker nodes reached",
        TerminationRequest.Cause.MAX_NUM_NODE_PER_TYPE: "max number of worker nodes per type reached",
        TerminationRequest.Cause.IDLE: "idle",
    }

    for idx, ((cause, instance_type), count) in enumerate(
        termination_by_causes_and_type.items()
    ):
        log_str = f"Removing {count} nodes of type {instance_type} ({cause_reason_map[cause]})."
```

## 4. 节点终止的执行流程

### 4.1 实例状态流转

```
RAY_RUNNING → TO_TERMINATE → RAY_STOP_REQUESTED → RAY_STOPPING → RAY_STOPPED → TERMINATING → TERMINATED
```

### 4.2 RayStopper：停止 Ray 进程

当实例被标记为 `RAY_STOP_REQUESTED` 时，`RayStopper`（subscriber）被触发。

`python/ray/autoscaler/v2/instance_manager/subscribers/ray_stopper.py:28-61`

```python
class RayStopper(InstanceUpdatedSubscriber):
    """RayStopper 负责停止实例上的 Ray 进程。

    如果是 idle 终止，会先 drain ray node。
    如果是其他终止（如 scale down），直接 stop ray node。
    """

    def __init__(self, gcs_client: GcsClient, error_queue: Queue) -> None:
        self._gcs_client = gcs_client
        self._error_queue = error_queue
        self._executor = ThreadPoolExecutor(max_workers=1)

    def notify(self, events: List[InstanceUpdateEvent]) -> None:
        for event in events:
            if event.new_instance_status == Instance.RAY_STOP_REQUESTED:
                fut = self._executor.submit(self._stop_or_drain_ray, event)

    def _stop_or_drain_ray(self, event: InstanceUpdateEvent) -> None:
        termination_request = event.termination_request
        ray_node_id = termination_request.ray_node_id

        if termination_request.cause == TerminationRequest.Cause.IDLE:
            # idle 终止：先 drain（允许正在运行的任务完成）
            self._drain_ray_node(...)
            return

        # 非 idle 终止（包括 MAX_NUM_NODES）：直接 stop
        self._stop_ray_node(
            self._gcs_client, self._error_queue, ray_node_id, instance_id
        )
```

`_stop_ray_node` 的核心操作：

```python
@staticmethod
def _stop_ray_node(gcs_client, error_queue, ray_node_id, instance_id):
    # 通过 GCS 通知目标节点的 raylet drain/退出
    drained = gcs_client.drain_nodes(node_ids=[hex_to_binary(ray_node_id)])
    success = len(drained) > 0
    if not success:
        error_queue.put_nowait(RayStopError(im_instance_id=instance_id))
```

**关键点**：`gcs_client.drain_nodes()` → GCS 向目标 raylet 发送 drain 信号 → raylet 退出 →
该节点上所有 actor/object owner 进程死亡。

### 4.3 云实例终止

`python/ray/autoscaler/v2/instance_manager/reconciler.py:1229-1263`

```python
@staticmethod
def _terminate_instances(instance_manager: InstanceManager):
    """终止以下状态的实例：
        - RAY_STOPPED: ray 已在云实例上停止
        - ALLOCATION_TIMEOUT: 云分配超时
        - RAY_INSTALL_FAILED: ray 安装失败
        - TERMINATION_FAILED: 上次终止失败，重试
    """
    im_instances, version = Reconciler._get_im_instances(instance_manager)
    updates = {}
    for instance in im_instances:
        if instance.status not in [
            IMInstance.RAY_STOPPED,
            IMInstance.ALLOCATION_TIMEOUT,
            IMInstance.RAY_INSTALL_FAILED,
            IMInstance.TERMINATION_FAILED,
        ]:
            continue

        # 通知云 provider 终止实例
        updates[instance.instance_id] = IMInstanceUpdateEvent(
            instance_id=instance.instance_id,
            new_instance_status=IMInstance.TERMINATING,
            cloud_instance_id=instance.cloud_instance_id,
            details="terminating instance from "
            f"{IMInstance.InstanceStatus.Name(instance.status)}",
        )
```

### 4.4 K8s 场景下的物理释放

终止实例最终依赖 `ICloudInstanceProvider.terminate()` 接口
（`python/ray/autoscaler/v2/instance_manager/node_provider.py:149`）：

```python
class ICloudInstanceProvider(ABC):
    """云实例提供商接口，负责：
        - 启动云实例
        - 终止运行中的实例
        - 获取非终止状态的云实例
        - 轮询更新错误
    """
```

**如果 K8s 不支持/不实现 `terminate()`**：
- 节点在 Ray 内部被标记为 `TERMINATED`（从 `ray status` 视图消失）
- Pod **不会被删除**，变成僵尸 Pod，继续占用 K8s 资源
- 但 raylet 已经退出，owner 已死亡

## 5. OwnerDiedError 的产生机制

### 5.1 因果链

```
max_workers: 0
  → Autoscaler scheduler 判定需要移除所有 worker
    → 实例状态变为 RAY_STOP_REQUESTED
      → RayStopper 调用 gcs_client.drain_nodes()
        → GCS 通知目标 raylet drain
          → raylet 进程退出
            → 该节点上所有 actor/object owner 死亡
              → 其他节点引用这些 object
                → OwnerDiedError: Failed to retrieve object xxx
```

### 5.2 关键结论

**即使 Pod 变成僵尸（K8s 没删除，Pod 还在），只要 raylet 被 drain 后退出了，owner 就已经死了，
引用该 owner 的 object 就找不回来，必然触发 `OwnerDiedError`。**

## 6. Autoscaler 进程启动机制

### 6.1 启动链路

```
ray start --head
  → Node.__init__() / Node.start_raylet()        # node.py:1321
    → if not self._ray_params.no_monitor:
        self.start_monitor()                      # node.py:1209-1222
          → services.start_monitor()               # services.py:2258-2290
            → 根据 autoscaler_v2 标志选择入口：
                v2: ray/autoscaler/v2/monitor.py
                v1: ray/autoscaler/_private/monitor.py
```

关键代码 `python/ray/_private/node.py:1321-1322`：

```python
if not self._ray_params.no_monitor:
    self.start_monitor()
```

`python/ray/_private/services.py:2258-2290`：

```python
def start_monitor(
    gcs_address: str,
    logs_dir: str,
    ...
    autoscaler_v2: bool = False,
):
    if autoscaler_v2:
        entrypoint = os.path.join(RAY_PATH, AUTOSCALER_V2_DIR, "monitor.py")
    else:
        entrypoint = os.path.join(RAY_PATH, AUTOSCALER_PRIVATE_DIR, "monitor.py")

    command = [
        sys.executable,
        "-u",
        entrypoint,
        f"--logs-dir={logs_dir}",
        f"--logging-rotate-bytes={max_bytes}",
        f"--logging-rotate-backup-count={backup_count}",
        f"--gcs-address={gcs_address}",
    ]
```

### 6.2 进程验证

head 节点上 autoscaler 进程（pid 271）：

```
root 271 63.5 0.0 674500 227384 ? Rl 16:15 30:14
/usr/bin/python3 -u /usr/local/lib/python3.12/dist-packages/ray/autoscaler/v2/monitor.py
--logs-dir=/tmp/ray/session_2026-06-11_16-15-08_921751_1/logs
--gcs-address=10.29.135.73:6379
--monitor-ip=10.29.135.73
```

## 7. 集群现场状态

### 7.1 ray status 输出关键信息

```
Active:
 1 headgroup
 50 worker-1 (7 idle + 43 running)

# 同时有 20 个 node_ 类型节点处于 NodeTerminated 状态

Resources:
 Total: 2450.0 CPU, 151.0 GPU, 17.13TiB memory
 Usage: 0.0/2450.0 CPU, 0.0/151.0 GPU
```

### 7.2 monitor.log 统计

- "Removing ... max number of worker nodes reached" 累计出现 **5846 次**
- 配置中 `max_workers: 0`，head 类型 `ray.head.default` 的 `max_workers: 0`

## 8. 解决方案

### 方案 1：禁用 Autoscaler（推荐快速止血）

```bash
ray start --head --no-monitor --port=6379
```

加 `--no-monitor` 参数，不再启动 monitor.py 进程，autoscaler 不会干预集群节点。

### 方案 2：提供正确的 Autoscaling Config

启动时提供 autoscaling config 文件，设置合理的 `max_workers`：

```bash
ray start --head --autoscaling-config=/path/to/config.yaml --port=6379
```

config.yaml 示例：

```yaml
max_workers: 50
available_node_types:
  ray.head.default:
    max_workers: 0
    min_workers: 0
    resources: {}
  worker-1:
    max_workers: 50
    min_workers: 1
    resources: {"CPU": 49, "GPU": 3}
```

### 方案 3：运行时修改配置

如果 autoscaler 已在运行，可以通过修改 GCS 中的 autoscaling config 来更新 `max_workers`，
但这需要 autoscaler 支持热加载配置。

## 9. 相关代码文件索引

| 文件 | 说明 |
|------|------|
| `python/ray/autoscaler/v2/monitor.py` | Autoscaler v2 监控进程入口 |
| `python/ray/autoscaler/v2/autoscaler.py` | Autoscaler 主类，调用 Reconciler |
| `python/ray/autoscaler/v2/scheduler.py:1061` | `_enforce_max_workers_global` 全局节点上限检查 |
| `python/ray/autoscaler/v2/scheduler.py:1148` | `_select_nodes_to_terminate` 选择终止节点 |
| `python/ray/autoscaler/v2/scheduler.py:1195` | `_sort_nodes_for_termination` 终止排序策略 |
| `python/ray/autoscaler/v2/event_logger.py:78` | 终止事件日志输出 |
| `python/ray/autoscaler/v2/instance_manager/reconciler.py:64` | Reconciler.reconcile 主协调方法 |
| `python/ray/autoscaler/v2/instance_manager/reconciler.py:1229` | `_terminate_instances` 云实例终止 |
| `python/ray/autoscaler/v2/instance_manager/config.py:371` | `get_max_num_worker_nodes` 读取 max_workers |
| `python/ray/autoscaler/v2/instance_manager/config.py:515` | `refresh_cached_autoscaling_config` 动态生成配置 |
| `python/ray/autoscaler/v2/instance_manager/subscribers/ray_stopper.py:28` | RayStopper 停止 Ray 进程 |
| `python/ray/autoscaler/v2/instance_manager/node_provider.py:149` | ICloudInstanceProvider 接口定义 |
| `python/ray/_private/node.py:1209` | `start_monitor` 启动 autoscaler |
| `python/ray/_private/services.py:2258` | `start_monitor` 组装命令并启动子进程 |
| `src/ray/protobuf/instance_manager.proto:178` | TerminationRequest.Cause 枚举定义 |
