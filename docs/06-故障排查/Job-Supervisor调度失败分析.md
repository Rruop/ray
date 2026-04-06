# Ray Job Supervisor Actor 调度失败问题分析

## 问题描述

### 错误信息

```
Status message: Job supervisor actor could not be scheduled: The actor is not schedulable:
The node specified via NodeAffinitySchedulingStrategy doesn't exist any more or is infeasible,
and soft=False was specified
```

### 问题特征

- **偶发性**：不是每次都出现，具有随机性
- **影响范围**：Job 无法启动，直接进入 FAILED 状态
- **涉及组件**：JobSupervisor Actor 调度

---

## 根本原因分析

### 1. JobSupervisor 调度策略

Ray 的 Job 提交机制中，`JobSupervisor` 是负责管理作业生命周期的核心 Actor。其调度逻辑位于：

**文件位置**: `python/ray/dashboard/modules/job/job_manager.py`

```python
# 第 424-470 行
async def _get_scheduling_strategy(
    self, resources_specified: bool
) -> SchedulingStrategyT:
    """Get the scheduling strategy for the job.

    If resources_specified is true, or if the environment variable is set to
    allow the job to run on worker nodes, we will use Ray's default actor
    placement strategy. Otherwise, we will force the job to use the head node.
    """
    if resources_specified:
        return "DEFAULT"

    if os.environ.get(RAY_JOB_ALLOW_DRIVER_ON_WORKER_NODES_ENV_VAR, "0") == "1":
        return "DEFAULT"

    # 默认行为：强制调度到 Head 节点
    head_node_id = await get_head_node_id(self._gcs_client)
    if head_node_id is None:
        scheduling_strategy = "DEFAULT"
    else:
        # 关键代码：使用硬亲和性 (soft=False)
        scheduling_strategy = NodeAffinitySchedulingStrategy(
            node_id=head_node_id, soft=False  # ← 问题根源
        )
    return scheduling_strategy
```

### 2. 硬亲和性策略的问题

| 参数 | 值 | 含义 |
|------|-----|------|
| `node_id` | `head_node_id` | 目标节点为 Head 节点 |
| `soft` | `False` | **硬亲和性**：必须调度到指定节点，否则失败 |

当 `soft=False` 时，如果指定的 `head_node_id` 对应的节点：
- 不存在
- 已下线
- 资源不足
- ID 已变更

调度器将直接抛出 `ActorUnschedulableError`，而不会尝试其他节点。

### 3. Head Node ID 的获取方式

**文件位置**: `python/ray/dashboard/modules/job/utils.py`

```python
# 第 37-46 行
async def get_head_node_id(gcs_client: GcsClient) -> Optional[str]:
    """Fetches Head node id persisted in GCS"""
    head_node_id_hex_bytes = await gcs_client.async_internal_kv_get(
        ray_constants.KV_HEAD_NODE_ID_KEY,  # 从 GCS KV 存储读取
        namespace=ray_constants.KV_NAMESPACE_JOB,
        timeout=30,
    )
    if head_node_id_hex_bytes is None:
        return None
    return head_node_id_hex_bytes.decode()
```

Head Node ID 存储在 GCS 的 KV 存储中，**不是实时查询**，存在以下问题：
- 数据可能过期
- 节点重启后 ID 会变化，但 GCS 中的记录可能未及时更新

---

## 偶发原因场景

### 场景 1: Head 节点重启

```
时间线:
T1: Head 节点启动，node_id = "abc123"，写入 GCS
T2: 用户提交 Job，读取 GCS 获得 head_node_id = "abc123"
T3: Head 节点异常重启，新 node_id = "xyz789"
T4: JobSupervisor 尝试调度到 "abc123" → 失败
```

### 场景 2: K8s Pod 重调度

在 KubeRay 环境下：
```
T1: Head Pod 运行中，node_id 写入 GCS
T2: K8s 因资源压力驱逐 Head Pod
T3: Head Pod 在新节点重建，获得新 node_id
T4: 旧的 node_id 仍在 GCS 中 → 调度失败
```

### 场景 3: GCS HA 故障转移

```
T1: Primary GCS 持有 head_node_id
T2: Primary GCS 故障，Standby 接管
T3: 数据同步存在时间窗口
T4: 在窗口期提交的 Job 可能读到过期数据
```

### 场景 4: 集群扩缩容时序问题

```
T1: Autoscaler 判断需要缩容，准备移除节点
T2: 用户提交 Job，获取到即将被移除的 head_node_id
T3: 节点被移除
T4: JobSupervisor 调度失败
```

---

## 解决方案

### 方案 1: 环境变量配置（推荐）

在 Ray 集群启动时设置环境变量，允许 JobSupervisor 在 Worker 节点上运行：

```bash
export RAY_JOB_ALLOW_DRIVER_ON_WORKER_NODES=1
```

**KubeRay 配置示例**:

```yaml
apiVersion: ray.io/v1
kind: RayCluster
metadata:
  name: my-cluster
spec:
  headGroupSpec:
    template:
      spec:
        containers:
        - name: ray-head
          env:
          - name: RAY_JOB_ALLOW_DRIVER_ON_WORKER_NODES
            value: "1"
```

**效果**：JobSupervisor 将使用 `DEFAULT` 调度策略，由 Ray 自动选择可用节点。

### 方案 2: 提交作业时指定资源

当用户显式指定资源时，Ray 会自动使用 `DEFAULT` 调度策略：

```python
from ray.job_submission import JobSubmissionClient

client = JobSubmissionClient("http://ray-head:8265")
job_id = client.submit_job(
    entrypoint="python my_script.py",
    entrypoint_num_cpus=1,  # 指定 CPU 资源
    # 或
    entrypoint_num_gpus=0.1,  # 指定 GPU 资源
    # 或
    entrypoint_resources={"custom_resource": 1},  # 自定义资源
)
```

**原理**：`resources_specified=True` 时，`_get_scheduling_strategy()` 直接返回 `"DEFAULT"`。

### 方案 3: 代码层面修复

修改 `job_manager.py`，将硬亲和性改为软亲和性：

```python
# 修改前
scheduling_strategy = NodeAffinitySchedulingStrategy(
    node_id=head_node_id, soft=False
)

# 修改后
scheduling_strategy = NodeAffinitySchedulingStrategy(
    node_id=head_node_id, soft=True  # 允许回退到其他节点
)
```

**注意**：此方案需要修改 Ray 源码，适用于自维护的 Ray 版本。

### 方案对比

| 方案 | 侵入性 | 适用场景 | 优点 | 缺点 |
|------|--------|----------|------|------|
| 环境变量 | 低 | 生产环境 | 无需改代码，配置即生效 | Job Driver 可能运行在任意节点 |
| 指定资源 | 低 | 特定作业 | 精确控制，无需集群配置 | 每个作业都需要指定 |
| 代码修复 | 高 | 自维护版本 | 从根本解决问题 | 需要维护 patch |

---

## 排查命令

### 检查集群状态

```bash
# 查看集群整体状态
ray status

# 列出所有节点
ray list nodes

# 查看节点详情
ray list nodes --detail
```

### 检查 GCS 中的 Head Node ID

```python
import ray
from ray._private import ray_constants

ray.init()
gcs_client = ray._private.gcs_utils.GcsClient(address="auto")
head_node_id = gcs_client.internal_kv_get(
    ray_constants.KV_HEAD_NODE_ID_KEY,
    namespace=ray_constants.KV_NAMESPACE_JOB
)
print(f"GCS 中记录的 Head Node ID: {head_node_id}")

# 对比实际的 Head 节点
nodes = ray.nodes()
for node in nodes:
    if node.get("Resources", {}).get("node:__internal_head__"):
        print(f"实际 Head Node ID: {node['NodeID']}")
```

### 检查 Job 状态

```bash
# 列出所有 Jobs
ray job list

# 查看特定 Job 详情
ray job status <job_id>

# 查看 Job 日志
ray job logs <job_id>
```

---

## 相关代码文件

| 文件 | 说明 |
|------|------|
| `python/ray/dashboard/modules/job/job_manager.py` | Job 管理器，包含调度策略逻辑 |
| `python/ray/dashboard/modules/job/job_supervisor.py` | JobSupervisor Actor 实现 |
| `python/ray/dashboard/modules/job/utils.py` | 工具函数，包含 `get_head_node_id` |
| `python/ray/dashboard/consts.py` | 常量定义，包含环境变量名 |

---

## 参考链接

- [Ray Job Submission](https://docs.ray.io/en/latest/cluster/running-applications/job-submission/index.html)
- [NodeAffinitySchedulingStrategy](https://docs.ray.io/en/latest/ray-core/scheduling/index.html#nodeaffinityschedulingstrategy)
- [KubeRay Operator](https://docs.ray.io/en/latest/cluster/kubernetes/index.html)
