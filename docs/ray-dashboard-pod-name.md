# Ray Dashboard 显示云实例 ID（RAY_CLOUD_INSTANCE_ID）

## 背景

Ray Dashboard 的 Node List 页面（`#/cluster`）中，`Host / Worker Process name` 列当前显示的是 `socket.gethostname()` 返回值，即容器内的主机名（`/etc/hostname`），在某些 Kubernetes 部署场景下这是物理机名而非 Pod 名。

为了能在 Dashboard 中准确展示 Pod 名称（或其他云实例标识），通过环境变量 `RAY_CLOUD_INSTANCE_ID` 将实例 ID 注入容器，并在 Ray 各层透传到前端展示。

---

## 数据流

```
K8s Pod spec (Downward API 或自定义注入)
  → 环境变量 RAY_CLOUD_INSTANCE_ID
    → reporter_agent.py (读取并写入 stats)
      → GCS pub/sub (JSON 格式传输)
        → node_head.py (解析后存入 DataSource)
          → DataOrganizer.get_node_info (构建 API 响应)
            → REST API /nodes?view=summary
              → 前端 service/node.ts
                → NodeDetail 类型
                  → NodeRow.tsx (显示在表格中)
```

---

## 修改内容

### 1. KubeRay — Pod Spec 注入环境变量

在 KubeRay 的 `RayCluster` CRD 或 Helm chart 中，为 **head** 和 **worker** 的 Pod template 添加环境变量注入：

```yaml
# raycluster.yaml 或 values.yaml 中 headGroupSpec / workerGroupSpec 的 template.spec.containers
containers:
  - name: ray-head   # 或 ray-worker
    env:
      - name: RAY_CLOUD_INSTANCE_ID
        valueFrom:
          fieldRef:
            fieldPath: metadata.name
```

**作用**：让容器内可以通过 `os.environ["RAY_CLOUD_INSTANCE_ID"]` 读取到自身的 Pod 名称（如 `raycluster-head-xxxxx` 或 `raycluster-worker-group-xxxxx`）。

> `fieldPath: metadata.name` 是 Kubernetes Downward API 语法，K8s 会在 Pod 启动时自动将真实 Pod 名填入，无需手动填写。

---

### 2. Python — `reporter_agent.py`

**文件路径**：`python/ray/dashboard/modules/reporter/reporter_agent.py`

#### 2.1 读取环境变量

在 `__init__` 方法中，`self._hostname = socket.gethostname()` 之后加一行：

```python
self._hostname = socket.gethostname()
self._pod_name = os.environ.get("RAY_CLOUD_INSTANCE_ID", "")  # 新增
```

#### 2.2 加入 stats payload

在 `_compose_stats_payload` 方法构建 `stats` dict 时，加入 `pod_name` 字段：

```python
stats = {
    "now": now,
    "hostname": self._hostname,
    "pod_name": self._pod_name,  # 新增
    "ip": self._ip,
    # ... 其余字段不变
}
```

---

### 3. Python — `reporter_models.py`

**文件路径**：`python/ray/dashboard/modules/reporter/reporter_models.py`

在 `NodeStats`（或 `StatsPayload`）的 Pydantic 模型中，加入 `pod_name` 字段（用于数据校验）：

```python
now: float  # POSIX timestamp
hostname: str
pod_name: str  # Cloud instance ID (RAY_CLOUD_INSTANCE_ID env var), empty string if not set  # 新增
ip: str
```

---

### 4. TypeScript — `node.d.ts`

**文件路径**：`python/ray/dashboard/client/src/type/node.d.ts`

在 `NodeDetail` 类型中加入 `pod_name` 字段：

```typescript
export type NodeDetail = {
  now: number;
  hostname: string;
  pod_name: string; // Cloud instance ID from RAY_CLOUD_INSTANCE_ID env var, empty string if not set  // 新增
  ip: string;
  // ... 其余字段不变
};
```

> **注意**：后端 Python 使用 snake_case（`pod_name`），JSON 透传不做 case 转换，前端保持同名。

---

### 5. TypeScript — `NodeRow.tsx`

**文件路径**：`python/ray/dashboard/client/src/pages/node/NodeRow.tsx`

#### 5.1 解构时读取 `pod_name`

```typescript
const {
  hostname = "",
  ip = "",
  cpu = 0,
  mem,
  disk,
  networkSpeed = [0, 0],
  raylet,
  logicalResources,
  pod_name: podName = "",  // 新增
} = node;
```

#### 5.2 显示时优先用实例 ID

```tsx
<TableCell align="center">
  <Box minWidth={TEXT_COL_MIN_WIDTH}>
    {podName || hostname}  {/* 有实例 ID 用实例 ID，否则降级显示 hostname */}
  </Box>
</TableCell>
```

---

## 前端构建

TypeScript 源码修改后，必须重新编译才能生效：

```bash
cd python/ray/dashboard/client
npm install        # 首次或依赖变更时执行
npm run build      # 编译，输出到 client/build/
```

编译完成后重启 Dashboard 服务（或重新打包 Docker 镜像）。

---

## 验证

部署后，在 Node List 页面，`Host / Worker Process name` 列应显示 Pod 名称（如 `raycluster-worker-group-abc12`）。

可通过以下方式交叉验证：

```bash
# 查看 Pod 名
kubectl get pods -n <namespace>

# 查看 Dashboard API 返回值
curl http://<dashboard-ip>:8265/nodes?view=summary | python3 -m json.tool | grep pod_name
```

---

## 回退方案

如果 `RAY_CLOUD_INSTANCE_ID` 环境变量未注入（非 K8s 环境或未配置），`pod_name` 字段值为空字符串，前端会自动降级显示 `hostname`，不影响非 K8s 场景的正常使用。
