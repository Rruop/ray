# Ray RemoteWriteExporter 集成测试完整流程

## 概述

本文档描述如何在本地测试 Ray 的 `RemoteWriteExporter`，将 metrics 推送到 Prometheus，并通过 Grafana 和 Ray Dashboard 可视化展示。

---

## 1. 前置条件

### 1.1 必需软件

| 软件 | 版本要求 | 用途 |
|-----|---------|------|
| Python | 3.8+ | 运行 Ray |
| Prometheus | 2.x 或 3.x | 存储 metrics |
| Grafana | 10.x+ | 可视化 |

### 1.2 必需 Python 包

```bash
pip install opentelemetry-exporter-prometheus-remote-write
pip install requests
```

### 1.3 验证依赖

```bash
pip show opentelemetry-exporter-prometheus-remote-write
```

---

## 2. 启动 Prometheus

### 2.1 启动命令

```bash
cd ~/Desktop/prometheus/prometheus-3.9.1.darwin-arm64
./prometheus --web.enable-remote-write-receiver
```

**关键参数**: `--web.enable-remote-write-receiver` 必须启用，否则无法接收 Remote Write 数据。

### 2.2 验证 Prometheus

```bash
curl http://localhost:9090/-/ready
# 预期输出: Prometheus Server is Ready.
```

---

## 3. 启动 Grafana

### 3.1 创建自定义配置

创建 `~/Desktop/grafana/grafana-12.3.3/conf/custom.ini`:

```ini
[auth.anonymous]
enabled = true
org_name = Main Org.
org_role = Viewer

[security]
allow_embedding = true

[server]
serve_from_sub_path = false
root_url = http://localhost:3000
```

**配置说明**:
- `auth.anonymous.enabled = true`: 允许匿名访问，Ray Dashboard 嵌入需要
- `security.allow_embedding = true`: 允许 iframe 嵌入

### 3.2 启动 Grafana

```bash
cd ~/Desktop/grafana/grafana-12.3.3
./bin/grafana-server --homepath=$(pwd) --config=conf/custom.ini
```

### 3.3 验证 Grafana

```bash
curl http://localhost:3000/api/health
# 预期输出: {"database":"ok","version":"12.3.3",...}
```

### 3.4 配置 Prometheus 数据源（首次）

```bash
curl -X POST "http://admin:admin@localhost:3000/api/datasources" \
  -H "Content-Type: application/json" \
  -d '{"name":"Prometheus","type":"prometheus","url":"http://localhost:9090","access":"proxy","isDefault":true}'
```

---

## 4. 运行集成测试脚本

在启动 Ray 集群之前，先运行独立的集成测试验证 RemoteWriteExporter 基本功能：

```bash
cd /Users/franke/Desktop/git/ray
python python/ray/_private/telemetry/test_remote_write_integration.py
```

**预期输出**:
```
============================================================
 OpenTelemetryMetricRecorder Remote Write 集成测试
============================================================
[OK] Prometheus 可用 (http://localhost:9090)

------------------------------------------------------------
导出模式: remote_write
指标前缀: ray_integration_<timestamp>
...
[通过] 集成测试成功
```

---

## 5. 启动 Ray 集群

### 5.1 设置环境变量

```bash
# Metrics 导出配置
export RAY_METRICS_EXPORT_MODE=remote_write
export RAY_METRICS_REMOTE_WRITE_ENDPOINT=http://localhost:9090/api/v1/write
export RAY_METRICS_PUSH_INTERVAL_MS=10000

# Grafana 集成配置（Ray Dashboard 嵌入 Grafana）
export RAY_GRAFANA_HOST=http://localhost:3000
export RAY_GRAFANA_IFRAME_HOST=http://localhost:3000

# 集群名称（可选，用于 Grafana Dashboard 的 Cluster 变量过滤）
export RAY_CLUSTER_NAME=my-cluster
```

### 5.2 启动 Ray Head 节点

```bash
ray start --head --port=6379 --metrics-export-port=8080
```

### 5.3 验证 Ray 集群

```bash
ray status
```

### 5.4 验证 Metrics 导出模式

```bash
tail -20 /tmp/ray/session_latest/logs/dashboard_agent.log | grep -i "remote_write\|REMOTE_WRITE"
# 预期: Metrics export mode: REMOTE_WRITE to http://localhost:9090/api/v1/write
```

---

## 6. 运行 Ray 任务生成 Metrics

### 6.1 测试脚本

创建 `test_ray_metrics.py`:

```python
#!/usr/bin/env python3
import time
import ray
from ray.util.metrics import Counter, Gauge, Histogram

ray.init(address="auto")

@ray.remote
class MetricsActor:
    def __init__(self, name):
        self.name = name
        self.counter = Counter(
            "test_requests_total",
            description="Total requests processed",
            tag_keys=("actor_name",),
        )
        self.counter.set_default_tags({"actor_name": name})

        self.gauge = Gauge(
            "test_active_tasks",
            description="Currently active tasks",
            tag_keys=("actor_name",),
        )
        self.gauge.set_default_tags({"actor_name": name})

        self.histogram = Histogram(
            "test_request_latency_ms",
            description="Request latency in milliseconds",
            boundaries=[1, 5, 10, 25, 50, 100, 250, 500, 1000],
            tag_keys=("actor_name",),
        )
        self.histogram.set_default_tags({"actor_name": name})

    def process(self, task_id):
        import random
        start = time.time()
        time.sleep(random.uniform(0.01, 0.1))

        self.counter.inc()
        self.gauge.set(task_id % 10)
        self.histogram.observe((time.time() - start) * 1000)

        return f"Task {task_id} done by {self.name}"

# 创建 actors
actors = [MetricsActor.remote(f"actor_{i}") for i in range(3)]

# 运行任务
print("Running tasks to generate metrics...")
for round_num in range(10):
    futures = [actor.process.remote(round_num * 3 + i) for i, actor in enumerate(actors)]
    results = ray.get(futures)
    print(f"Round {round_num + 1}: {results}")
    time.sleep(2)

print("\nDone!")
```

### 6.2 运行测试

```bash
python test_ray_metrics.py
```

### 6.3 运行 Ray Data 任务

创建 `test_ray_data_metrics.py`:

```python
#!/usr/bin/env python3
"""测试 Ray Data metrics 导出"""
import ray
import time

ray.init(address="auto")

import ray.data

print("运行 Ray Data 任务...")

# 创建并执行数据集
ds = ray.data.range(10000)
ds = ds.map(lambda x: {'value': x['id'] * 2, 'square': x['id'] ** 2})
ds = ds.filter(lambda x: x['value'] % 4 == 0)

# 执行并获取结果
result = ds.take(10)
print(f"处理完成，示例结果: {result[:3]}")

# 等待指标推送
print("等待指标推送到 Prometheus...")
time.sleep(15)

print("\nDone! 检查 Prometheus:")
print("  http://localhost:9090/graph")
print('  查询: {__name__=~"ray_data_.*"}')
```

运行测试:
```bash
python test_ray_data_metrics.py
```

---

## 7. 验证 Metrics

### 7.1 通过 Prometheus API 验证

```bash
# 查看所有 Ray metrics
curl -s --data-urlencode 'query={__name__=~"ray_.*"}' \
  'http://localhost:9090/api/v1/query' | python3 -m json.tool | head -50

# 查看 CPU 使用率
curl -s --data-urlencode 'query=ray_node_cpu_utilization' \
  'http://localhost:9090/api/v1/query' | python3 -m json.tool

# 查看自定义 Counter
curl -s --data-urlencode 'query=ray_test_requests_total' \
  'http://localhost:9090/api/v1/query' | python3 -m json.tool

# 查看任务状态
curl -s --data-urlencode 'query=sum(ray_tasks) by (State)' \
  'http://localhost:9090/api/v1/query' | python3 -m json.tool
```

### 7.2 验证 instance 标签（Dashboard 兼容性）

```bash
curl -s --data-urlencode 'query=ray_node_cpu_utilization' \
  'http://localhost:9090/api/v1/query' | python3 -c "
import json, sys
data = json.load(sys.stdin)
result = data['data']['result']
if result:
    labels = result[0]['metric']
    print('instance' in labels and 'OK: instance label exists' or 'ERROR: instance label missing')
    print(f\"  instance: {labels.get('instance', 'N/A')}\")
    print(f\"  ip: {labels.get('ip', 'N/A')}\")
"
```

### 7.3 验证 Ray Data 指标

```bash
# 查看 Ray Data 指标数量
curl -s --data-urlencode 'query={__name__=~"ray_data_.*"}' \
  'http://localhost:9090/api/v1/query' | python3 -c "
import json, sys
data = json.load(sys.stdin)
metrics = set()
for r in data['data']['result']:
    metrics.add(r['metric']['__name__'])
print(f'Ray Data 指标类型数: {len(metrics)}')
"

# 查看 dataset 和 operator 标签
curl -s 'http://localhost:9090/api/v1/label/dataset/values'
curl -s 'http://localhost:9090/api/v1/label/operator/values'
```

**注意**: Ray Data 指标使用小写标签名 `dataset` 和 `operator`。

### 7.4 验证 Ray Dashboard Grafana 集成

```bash
# 检查 Grafana 健康状态
curl -s http://127.0.0.1:8265/api/grafana_health | python3 -m json.tool

# 检查 Prometheus 健康状态
curl -s http://127.0.0.1:8265/api/prometheus_health | python3 -m json.tool
```

**预期输出**:
```json
{
    "result": true,
    "msg": "Grafana running",
    "data": {
        "grafanaHost": "http://localhost:3000",
        "sessionName": "session_...",
        "dashboardUids": {
            "default": "rayDefaultDashboard",
            ...
        }
    }
}
```

---

## 8. 访问 Dashboard

### 8.1 Ray Dashboard (推荐)

- **URL**: http://127.0.0.1:8265
- **Metrics 标签页**: http://127.0.0.1:8265/#/metrics
- 自动嵌入 Grafana Dashboard，自动选择当前 Session

### 8.2 直接访问 Grafana

- **URL**: http://localhost:3000
- **默认凭据**: admin / admin（或匿名访问）
- **Ray Default Dashboard**: http://localhost:3000/d/rayDefaultDashboard

### 8.3 Grafana Dashboard 变量设置

在 Grafana Dashboard 中设置以下变量：
- **datasource**: Prometheus
- **SessionName**: 选择当前 session (如 `session_2026-02-26_11-52-54_...`)
- **Instance**: 选择 `127.0.0.1` 或 `All`

---

## 9. 常用 PromQL 查询

### 9.1 Ray Core 指标

```promql
# CPU 使用率
ray_node_cpu_utilization

# 内存使用百分比
ray_node_mem_used / ray_node_mem_total * 100

# 任务状态分布
sum(ray_tasks) by (State)

# 自定义 Counter 速率
rate(ray_test_requests_total[1m])

# Histogram P95 延迟
histogram_quantile(0.95, rate(ray_test_request_latency_ms_bucket[5m]))
```

### 9.2 Ray Data 指标

```promql
# 数据输出字节数
ray_data_output_bytes

# 数据输出行数
ray_data_output_rows

# 任务完成时间分布
ray_data_task_completion_time_bucket

# 内存溢出字节数
ray_data_spilled_bytes

# 按 dataset 分组的输出
sum by (dataset)(ray_data_output_bytes)

# 按 operator 分组的任务数
sum by (operator)(ray_data_num_tasks_finished)
```

### 9.3 Dashboard 变量查询

```promql
# SessionName 变量 (使用较大时间范围避免陈旧性问题)
count by (SessionName)(last_over_time(ray_data_output_bytes{}[1h]))

# dataset 变量
count by (dataset)(last_over_time(ray_data_output_bytes{SessionName=~"$SessionName"}[1h]))

# operator 变量
count by (operator)(last_over_time(ray_data_output_bytes{SessionName=~"$SessionName"}[1h]))
```

---

## 10. 清理

```bash
# 停止 Ray 集群
ray stop

# 停止 Grafana (如果在前台运行则 Ctrl+C)
pkill -f grafana-server

# 停止 Prometheus (如果在前台运行则 Ctrl+C)
pkill -f prometheus

# 删除测试脚本
rm -f test_ray_metrics.py test_ray_data_metrics.py
```

---

## 11. 环境变量参考

| 环境变量 | 说明 | 默认值 |
|---------|------|--------|
| `RAY_METRICS_EXPORT_MODE` | 导出模式: `pull`, `push`/`remote_write` | `pull` |
| `RAY_METRICS_PUSH_INTERVAL_MS` | 推送间隔 (毫秒) | `10000` |
| `RAY_METRICS_REMOTE_WRITE_ENDPOINT` | Remote Write 端点 URL | `http://localhost:9090/api/v1/write` |
| `RAY_METRICS_REMOTE_WRITE_USERNAME` | Basic Auth 用户名 | - |
| `RAY_METRICS_REMOTE_WRITE_PASSWORD` | Basic Auth 密码 | - |
| `RAY_METRICS_REMOTE_WRITE_HEADERS` | 自定义 HTTP Headers (JSON) | - |
| `RAY_METRICS_REMOTE_WRITE_TIMEOUT` | 请求超时 (秒) | `30` |
| `RAY_METRICS_REMOTE_WRITE_TENANT_ID` | 多租户 ID (Cortex/Mimir) | - |
| `RAY_CLUSTER_NAME` | 集群名称，添加 `ray_io_cluster` 标签用于 Dashboard 过滤 | - |
| `RAY_GRAFANA_HOST` | Grafana 地址 | `http://localhost:3000` |
| `RAY_GRAFANA_IFRAME_HOST` | Grafana iframe 地址 | 同 `RAY_GRAFANA_HOST` |
| `RAY_PROMETHEUS_HOST` | Prometheus 地址 | `http://localhost:9090` |

---

## 12. 故障排查

### 问题 1: Prometheus 收不到数据

**症状**: Prometheus 中查不到 `ray_*` metrics

**排查步骤**:
```bash
# 1. 检查 Remote Write Receiver 是否启用
curl -X POST http://localhost:9090/api/v1/write
# 应返回 400 Bad Request，而不是 404

# 2. 检查 dashboard agent 日志
tail -50 /tmp/ray/session_latest/logs/dashboard_agent.log | grep -i "remote_write\|error"

# 3. 确认环境变量在 ray start 之前设置
echo $RAY_METRICS_EXPORT_MODE
```

### 问题 2: Grafana Dashboard 没有数据

**症状**: Dashboard 显示 "No data"

**排查步骤**:
```bash
# 1. 检查 instance 标签是否存在
curl -s --data-urlencode 'query=ray_node_cpu_utilization' \
  'http://localhost:9090/api/v1/query' | grep -o '"instance"'

# 2. 检查 SessionName 变量
curl -s 'http://localhost:9090/api/v1/label/SessionName/values'

# 3. 确认选择了正确的 Session
```

### 问题 3: Ray Dashboard Metrics 标签页为空

**症状**: Ray Dashboard 的 Metrics 页面不显示 Grafana

**排查步骤**:
```bash
# 1. 检查 Grafana 集成状态
curl -s http://127.0.0.1:8265/api/grafana_health

# 2. 确认环境变量
echo $RAY_GRAFANA_HOST

# 3. 确认 Grafana 允许嵌入
grep "allow_embedding" ~/Desktop/grafana/grafana-12.3.3/conf/custom.ini
```

### 问题 4: 依赖包缺失

```bash
pip install opentelemetry-exporter-prometheus-remote-write
```

### 问题 5: Ray Data Dashboard 变量 (DatasetID, Operator) 为空

**症状**: Grafana Ray Data Dashboard 中 DatasetID、Operator 等变量下拉框为空

**根本原因**: 数据陈旧性问题

1. **Ray Data 指标特性**: `ray_data_output_bytes` 等指标只在作业运行期间更新，作业完成后停止更新
2. **Prometheus 即时查询**: 默认只返回最近 5 分钟内有更新的数据
3. **Dashboard 变量查询**: 使用 `[$__range]` 动态时间范围，时间范围内无活跃作业则变量为空

**解决方案**:
```
1. 在 Grafana Dashboard 中将时间范围设置为较大值（如 "Last 1 hour" 或 "Last 6 hours"）
2. 或者运行新的 Ray Data 作业刷新指标
```

**验证命令**:
```bash
# 检查不同时间范围的数据
curl -s --data-urlencode 'query=count by (dataset)(last_over_time(ray_data_output_bytes{}[5m]))' \
  'http://localhost:9090/api/v1/query'  # 可能为空

curl -s --data-urlencode 'query=count by (dataset)(last_over_time(ray_data_output_bytes{}[1h]))' \
  'http://localhost:9090/api/v1/query'  # 应该有数据
```

**注意**: 这是 Prometheus + Grafana 的正常行为，不是代码 bug。

---

## 13. 关键代码文件

| 文件 | 说明 |
|-----|------|
| `python/ray/_private/telemetry/open_telemetry_metric_recorder.py` | RemoteWriteExporter 核心实现 |
| `python/ray/_private/telemetry/test_remote_write_integration.py` | 独立集成测试脚本 |
| `python/ray/tests/test_open_telemetry_metric_recorder.py` | 单元测试 |
| `python/ray/dashboard/modules/metrics/metrics_head.py` | Dashboard Metrics 模块 |
| `python/ray/dashboard/modules/reporter/reporter_agent.py` | ReporterAgent metrics 导出 |

---

## 14. 验证清单

### 14.1 基础设施

- [ ] Prometheus 启动并启用 `--web.enable-remote-write-receiver`
- [ ] Grafana 启动并配置 `allow_embedding = true`
- [ ] Prometheus 数据源已添加到 Grafana

### 14.2 Ray Core 指标

- [ ] 集成测试脚本 `test_remote_write_integration.py` 通过
- [ ] 环境变量正确设置 (`RAY_METRICS_EXPORT_MODE=remote_write` 等)
- [ ] Ray 集群以 `remote_write` 模式启动
- [ ] Dashboard agent 日志确认 `REMOTE_WRITE` 模式
- [ ] Prometheus 中可查询到 `ray_*` metrics
- [ ] Metrics 包含 `instance` 标签
- [ ] Metrics 包含 `ray_io_cluster` 标签 (如果设置了 `RAY_CLUSTER_NAME`)

### 14.3 Ray Data 指标

- [ ] 运行 Ray Data 任务生成指标
- [ ] Prometheus 中可查询到 `ray_data_*` metrics (约 100+ 种类型)
- [ ] 指标包含 `dataset` 和 `operator` 标签 (小写)
- [ ] Dashboard 时间范围设置为足够大以包含作业时间段

### 14.4 Dashboard 集成

- [ ] Ray Dashboard `/api/grafana_health` 返回成功
- [ ] Ray Dashboard Metrics 标签页显示 Grafana 图表
- [ ] Grafana Ray Data Dashboard 变量可正常选择

---

## 15. 标签名参考

### 15.1 Ray Core 指标标签

| 标签名 | 说明 | 示例 |
|--------|------|------|
| `ip` | 节点 IP | `127.0.0.1` |
| `instance` | Prometheus 实例标识 (Remote Write 模式自动添加) | `127.0.0.1` |
| `SessionName` | Ray 会话名 | `session_2026-02-26_14-12-29_200662_30626` |
| `RayNodeType` | 节点类型 | `head`, `worker` |
| `ray_io_cluster` | 集群名 (需设置 `RAY_CLUSTER_NAME`) | `my-cluster` |
| `Version` | Ray 版本 | `2.52.1` |

### 15.2 Ray Data 指标标签

| 标签名 | 说明 | 示例 |
|--------|------|------|
| `dataset` | 数据集 ID (小写) | `dataset_22_0` |
| `operator` | 操作符名称 (小写) | `ReadRange->Map(<lambda>)` |
| `SessionName` | Ray 会话名 | `session_2026-02-26_14-12-29_200662_30626` |
| `Component` | 组件名 | `core_worker` |
| `WorkerId` | Worker ID | `9052cc4c508295d8...` |
