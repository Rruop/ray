# Ray Serve Deployment 失败与状态机制分析

## 1. Replica 启动失败重试机制

### 核心报错

```
The deployment failed to start 3 times in a row. This may be due to a problem with its
constructor or initial health check failing. See controller logs for details. Error: ...
```

### 重试次数控制参数

两个参数共同控制 replica 启动失败的最大重试次数：

| 参数 | 类型 | 默认值 | 定义位置 |
|------|------|--------|----------|
| `max_constructor_retry_count` | deployment 配置 | **20** | `DeploymentConfig` (`python/ray/serve/config.py`) |
| `RAY_SERVE_MAX_PER_REPLICA_RETRY_COUNT` | 环境变量 | **3** | `python/ray/serve/_private/constants.py:68` |

### 阈值计算逻辑

实际失败判定阈值由两者取较小值（`deployment_state.py:2998-3002`）：

```python
@property
def _failed_to_start_threshold(self) -> int:
    return min(
        self._target_state.info.deployment_config.max_constructor_retry_count,
        self._target_state.target_num_replicas * MAX_PER_REPLICA_RETRY_COUNT,
    )
```

**示例**：
- `num_replicas=1`, `max_constructor_retry_count=20` → 阈值 = `min(20, 1*3)` = **3**
- `num_replicas=2`, `max_constructor_retry_count=20` → 阈值 = `min(20, 2*3)` = **6**
- `num_replicas=5`, `max_constructor_retry_count=7`  → 阈值 = `min(7, 5*3)` = **7**

### 设置方式

```python
@serve.deployment(max_constructor_retry_count=30, num_replicas=4)
class MyDeployment:
    ...
```

或通过环境变量全局调整每个 replica 的重试上限：

```bash
export RAY_SERVE_MAX_PER_REPLICA_RETRY_COUNT=5
```

### Deployment Actor 失败阈值

deployment-scoped actor（非 replica）的失败阈值仅使用 `max_constructor_retry_count`，不乘以 replica 数量（`deployment_state.py:3005-3011`）：

```python
@property
def _deployment_actor_failed_to_start_threshold(self) -> int:
    return self._target_state.info.deployment_config.max_constructor_retry_count
```

---

## 2. Terminal Failure 后是否停止启动

### 结论：**是的，terminal failure 后不再创建新 replica**

关键代码在 `deployment_state.py:3723`：

```python
def _get_upscale_replicas(self, to_add, ...):
    upscale = []
    if to_add <= 0 or self._terminally_failed():
        return upscale  # 返回空列表，不创建新 replica
```

### Terminal Failure 判定条件

`deployment_state.py:3057-3068`：

```python
def _terminally_failed(self) -> bool:
    replica_failed = (
        not self._replica_has_started       # 从未有 replica 成功启动过
        and self._replica_startup_failing()  # 重试次数超过阈值
    )
    return replica_failed or self.deployment_actor_terminally_failed()
```

**关键区分**：`_replica_has_started` 标志位

- `_replica_has_started = False`：当前版本**从未**有 replica 成功启动 → 达到阈值后 **terminally failed**，不再重试
- `_replica_has_started = True`：已有 replica 成功运行过 → 即使后续失败 **也不会 terminal failure**，controller 继续尝试恢复

### 恢复方式

重新 deploy 会重置所有计数器（`deployment_state.py:3366-3369`）：

```python
self._replica_constructor_retry_counter = 0
self._replica_has_started = False
self._deployment_actor_failed = None
self._deployment_actor_retry_counter = 0
```

部署达到 HEALTHY 状态时也会重置（`deployment_state.py:3959`）：

```python
self._replica_constructor_retry_counter = 0
```

### 失败后的可用性广播

terminal failure 后，deployment 被标记为不可用（`deployment_state.py:3157`）：

```python
is_available = not self._terminally_failed()
```

不可用的 deployment 不会接收流量路由。

---

## 3. Dashboard Replicas 展示分析

### 当前行为：只展示总数

Dashboard 的 Applications / Deployments 页面，Replicas 列只展示 `replicas.length`（所有状态 replica 的总数）。

**前端代码**：

`ServeDeploymentRow.tsx:70-79`：

```tsx
<TableCell align="center">
  <Link component={RouterLink} to={...}>
    {replicas.length}  {/* 仅展示总数 */}
  </Link>
</TableCell>
```

`ServeApplicationDetailPage.tsx:110-115`（Application 级别同理）：

```tsx
{
  label: "Replicas",
  content: {
    value: Object.values(application.deployments)
      .map(({ replicas }) => replicas.length)
      .reduce((acc, curr) => acc + curr, 0)
      .toString(),
  },
}
```

### API 实际返回了每个 Replica 的状态

后端 API（`GET /api/serve/applications/`）返回的 `DeploymentDetails` 包含完整的 replica 列表，每个 replica 带有 `state` 字段：

**Schema**（`python/ray/serve/schema.py:1330-1375`）：

```python
class DeploymentDetails(BaseModel):
    name: str
    status: DeploymentStatus
    status_trigger: DeploymentStatusTrigger
    message: str
    deployment_config: DeploymentSchema       # 包含 num_replicas
    target_num_replicas: NonNegativeInt        # 目标 replica 数
    required_resources: Dict
    replicas: List[ReplicaDetails]            # 完整 replica 列表
    autoscaling_detail: Optional[DeploymentAutoscalingDetail]
```

**ReplicaDetails**（`schema.py:1253-1269`）：

```python
class ReplicaDetails(ServeActorDetails):
    replica_id: str
    state: ReplicaState        # STARTING | UPDATING | RECOVERING | RUNNING | STOPPING
    pid: Optional[int]
    start_time_s: float
```

**TypeScript 类型**（`dashboard/client/src/type/serve.ts:72-82`）：

```typescript
export type ServeReplica = {
  replica_id: string;
  state: ServeReplicaState;   // STARTING | UPDATING | RECOVERING | RUNNING | STOPPING
  pid: string | null;
  actor_name: string;
  actor_id: string | null;
  node_id: string | null;
  node_ip: string | null;
  start_time_s: number;
  log_file_path: string | null;
};
```

### 建议改进

当前列表页只展示 `replicas.length`（含 STARTING/STOPPING 等所有状态），不够直观。

建议改为 **`{running_count} / {target_num_replicas}`** 格式，例如 `3 / 5`，让用户一眼看出有多少 replica 正在服务 vs 目标数量。

**实现思路**：

```tsx
const runningCount = replicas.filter(r => r.state === "RUNNING").length;
const targetCount = deployment_config.num_replicas;

<TableCell align="center">
  {runningCount} / {targetCount}
</TableCell>
```

### CLI `serve status` 已有状态分布

`serve status` CLI 命令通过 `ServeInstanceDetails._get_status()` 计算了 replica 状态分布（`schema.py:1626-1628`）：

```python
replica_states=dict(
    Counter([r.state.value for r in deployment.replicas])
),
```

输出类似 `{"RUNNING": 3, "STARTING": 1, "STOPPING": 1}`，但这个信息 **仅用于 CLI，未暴露给 Dashboard REST API**。

---

## 4. Health Check 失败的状态流转

### DeploymentStatus 枚举

```python
class DeploymentStatus(str, Enum):
    UPDATING = "UPDATING"
    HEALTHY = "HEALTHY"
    UNHEALTHY = "UNHEALTHY"
    DEPLOY_FAILED = "DEPLOY_FAILED"
    UPSCALING = "UPSCALING"
    DOWNSCALING = "DOWNSCALING"
```

### Health Check 失败后的状态转换（状态机）

**取决于当前状态**（`python/ray/serve/_private/common.py`）：

| 当前状态 | Health Check 失败后转为 | 代码位置 |
|---------|----------------------|---------|
| UPDATING | **DEPLOY_FAILED** | common.py:401-406 |
| UPSCALING | **UNHEALTHY** | common.py:422-427 |
| DOWNSCALING | **UNHEALTHY** | common.py:422-427 |
| HEALTHY | **UNHEALTHY** | common.py:551-556 |
| UNHEALTHY | 保持 **UNHEALTHY** | common.py:576-581 |
| DEPLOY_FAILED | 保持 **DEPLOY_FAILED** | common.py:607-612 |

### 核心规则

1. **初始部署阶段**（UPDATING 状态）：health check 失败 → **DEPLOY_FAILED**（不可恢复，除非重新部署）
2. **已运行阶段**（HEALTHY/UPSCALING/DOWNSCALING）：health check 失败 → **UNHEALTHY**（可自动恢复）
3. UNHEALTHY 状态下如果 replica 恢复健康 → 转回 **HEALTHY**

### Replica Startup Failed 的状态转换

与 health check 失败略有不同：

| 当前状态 | Replica Startup Failed 后转为 |
|---------|---------------------------|
| UPDATING | **DEPLOY_FAILED** |
| UPSCALING / DOWNSCALING | **UNHEALTHY** |
| HEALTHY | **UNHEALTHY**（common.py:582-587） |
| UNHEALTHY | 保持 **UNHEALTHY** |
| DEPLOY_FAILED | 保持 **DEPLOY_FAILED** |

### Status 数值排名（用于 Dashboard 展示优先级）

```python
DEPLOYMENT_STATUS_RANKING_ORDER = {
    (DeploymentStatus.DEPLOY_FAILED,): 0,    # 最高优先级
    (DeploymentStatus.UNHEALTHY,): 1,
    (DeploymentStatus.UPDATING,): 2,
    (DeploymentStatus.UPSCALING, DeploymentStatusTrigger.CONFIG_UPDATE_STARTED): 2,
    ...
    (DeploymentStatus.HEALTHY,): 6,          # 最低优先级
}
```

数值映射：

| 状态 | 数值 |
|------|------|
| UNKNOWN | 0 |
| DEPLOY_FAILED | 1 |
| UNHEALTHY | 2 |
| UPDATING | 3 |
| UPSCALING | 4 |
| DOWNSCALING | 5 |
| HEALTHY | 6 |

---

## 5. 完整状态流转图

```
                         ┌─────────────┐
                         │   UPDATING  │ ← deploy() / config update
                         └──────┬──────┘
                                │
                    ┌───────────┼───────────┐
                    │           │           │
              startup fail  healthy     health check fail
                    │           │           │
                    ▼           ▼           ▼
            ┌─────────────┐ ┌────────┐ ┌─────────────┐
            │DEPLOY_FAILED│ │HEALTHY │ │DEPLOY_FAILED│
            └──────┬──────┘ └───┬────┘ └─────────────┘
                   │            │
              re-deploy    health check fail
                   │            │
                   ▼            ▼
              ┌──────────┐ ┌─────────┐
              │ UPDATING  │ │UNHEALTHY│
              └──────────┘ └────┬────┘
                                │
                          ┌─────┼─────┐
                          │           │
                      healthy    stay unhealthy
                          │           │
                          ▼           │
                      ┌────────┐     │
                      │HEALTHY │←────┘
                      └───┬────┘
                          │
                   autoscale up/down
                          │
                          ▼
                  ┌───────────────┐
                  │ UPSCALING /   │
                  │ DOWNSCALING   │
                  └───────┬───────┘
                          │
               ┌──────────┼──────────┐
               │          │          │
          healthy    startup fail  health check fail
               │          │          │
               ▼          ▼          ▼
           ┌────────┐ ┌─────────┐ ┌─────────┐
           │HEALTHY │ │UNHEALTHY │ │UNHEALTHY│
           └────────┘ └─────────┘ └─────────┘
```

---

## 6. 关键源码索引

| 内容 | 文件 | 行号 |
|------|------|------|
| 阈值计算 | `python/ray/serve/_private/deployment_state.py` | 2998-3011 |
| Terminal failure 判定 | `python/ray/serve/_private/deployment_state.py` | 3057-3068 |
| Upscale 被阻断 | `python/ray/serve/_private/deployment_state.py` | 3723 |
| 重试计数器重置 | `python/ray/serve/_private/deployment_state.py` | 3366-3369, 3959 |
| 报错信息生成 | `python/ray/serve/_private/deployment_state.py` | 3900-3910 |
| Health check 状态转换 | `python/ray/serve/_private/common.py` | 401-406, 422-427, 551-556, 576-581 |
| Status 数值映射 | `python/ray/serve/_private/common.py` | 202-218 |
| MAX_PER_REPLICA_RETRY_COUNT | `python/ray/serve/_private/constants.py` | 68 |
| DeploymentConfig 默认值 | `python/ray/serve/config.py` | - |
| Dashboard Replicas 展示 | `python/ray/dashboard/client/src/pages/serve/ServeDeploymentRow.tsx` | 78 |
| Dashboard Application Replicas | `python/ray/dashboard/client/src/pages/serve/ServeApplicationDetailPage.tsx` | 110-115 |
| DeploymentDetails Schema | `python/ray/serve/schema.py` | 1330-1375 |
| ReplicaDetails Schema | `python/ray/serve/schema.py` | 1253-1269 |
| CLI replica 状态分布 | `python/ray/serve/schema.py` | 1626-1628 |
| TypeScript Replica 类型 | `python/ray/dashboard/client/src/type/serve.ts` | 72-82 |
