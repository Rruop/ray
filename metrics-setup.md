# Prometheus + Grafana 监控部署指南

## 架构概览

```
Prometheus (接收方, :9090) ←── remote write ── Ray 等服务推送指标
       │
       │ HTTP :9090
       ▼
   Grafana (可视化, :3000)
       │
       ├─ grafana.ini                    → Grafana 主配置（路径、日志等）
       ├─ provisioning/                   → 声明式自动配置
       │   ├─ datasources/default.yml    → 告诉 Grafana 去哪读数据（连 Prometheus）
       │   └─ dashboards/default.yml      → 告诉 Grafana 去哪加载仪表盘 JSON
       └─ dashboards/*.json               → 实际的仪表盘定义文件
```

## 目录结构

```
/home/shiyanpeng03/
├── grafana-v12.0.0/                          ← GRAFANA_HOME
├── prometheus-3.5.1.linux-amd64/             ← PROM_HOME
└── script/
    ├── start_grafana.sh                       ← Grafana 启动脚本
    ├── start_prometheus.sh                    ← Prometheus 启动脚本
    └── metrics/
        ├── grafana/
        │   ├── grafana.ini                    ← 主配置（使用 __METRICS_DIR__ 占位符）
        │   ├── grafana.ini.origin             ← 原始备份
        │   ├── dashboards/                    ← 仪表盘 JSON 文件
        │   │   ├── data_grafana_dashboard.json
        │   │   ├── data_llm_grafana_dashboard.json
        │   │   ├── default_grafana_dashboard.json
        │   │   ├── serve_deployment_grafana_dashboard.json
        │   │   ├── serve_grafana_dashboard.json
        │   │   ├── serve_llm_grafana_dashboard.json
        │   │   └── train_grafana_dashboard.json
        │   └── provisioning/
        │       ├── dashboards/
        │       │   └── default.yml            ← provider 配置（使用 __METRICS_DIR__ 占位符）
        │       └── datasources/
        │           └── default.yml            ← 数据源配置
        └── prometheus/
            └── prometheus.yml                 ← Prometheus 配置
```

## 两个 dashboards 目录的区别

| 目录 | 作用 | 内容 |
|---|---|---|
| `provisioning/dashboards/` | **Provider 配置**（YAML） | 告诉 Grafana 去哪个目录加载 JSON |
| `dashboards/` | **仪表盘定义**（JSON） | 实际的 dashboard 图表定义文件 |

`provisioning/dashboards/default.yml` 中的 `path` 字段指向 `dashboards/` 目录，Grafana 启动时读取 YAML → 找到 path → 去该目录加载 JSON。

这种分离是 Grafana 推荐的做法：provisioning 目录放声明式配置（YAML），dashboard 定义文件单独存放。

## 关键配置说明

### Prometheus 作为接收方，不需要 scrape 配置

Prometheus 启动时带了 `--web.enable-remote-write-receiver`，指标由 Ray 等服务主动 push 过来（remote write），所以 `prometheus.yml` 不需要 `scrape_configs`，只需 global 配置即可。

### Provisioning 机制

Grafana 的 provisioning 是"声明式自动配置"机制——Grafana 启动时自动读取 YAML，无需手动在 UI 里添加数据源和仪表盘：

- **`datasources/default.yml`**：自动注册 Prometheus 数据源，Grafana 启动后就知道去 `http://localhost:9090` 查数据
- **`dashboards/default.yml`**：自动从指定目录加载 JSON 仪表盘文件

`grafana.ini` 中的 `[paths] provisioning = ...` 就是告诉 Grafana 去哪个目录找这些自动配置 YAML。

### 占位符机制（便于部署）

配置文件中使用 `__METRICS_DIR__` 占位符，启动脚本中通过 sed 动态替换：

```bash
# start_grafana.sh 中
sed -i "s|__METRICS_DIR__|${METRICS_DIR}|g" ${METRICS_DIR}/grafana.ini
sed -i "s|__METRICS_DIR__|${METRICS_DIR}|g" ${METRICS_DIR}/provisioning/dashboards/default.yml
```

**部署到新机器只需改两个脚本中的 `BASE_DIR="/home/shiyanpeng03"` 一行即可。**

## 配置文件内容

### grafana.ini

```ini
[paths]
provisioning = __METRICS_DIR__/provisioning

[log]
mode = file
level = info

[log.file]
log_rotate = true
max_lines = 1000000
max_size_shift = 28
daily_rotate = true
max_days = 7
```

### provisioning/dashboards/default.yml

```yaml
apiVersion: 1

providers:
  - name: Ray
    folder: Ray
    type: file
    options:
      path: __METRICS_DIR__/dashboards
```

### provisioning/datasources/default.yml

```yaml
apiVersion: 1

datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://localhost:9090
    isDefault: true
```

> 无需修改，只要 Prometheus 和 Grafana 在同一台机器上。

### prometheus.yml

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s
```

> 接收方模式，不需要 scrape 配置。

## 启动脚本

### start_prometheus.sh

```bash
#!/bin/bash
BASE_DIR="/home/shiyanpeng03"
PROM_HOME="${BASE_DIR}/prometheus-3.5.1.linux-amd64"
METRICS_DIR="${BASE_DIR}/script/metrics/prometheus"
LOG_FILE="${PROM_HOME}/prometheus.log"

echo "正在启动 Prometheus..."

nohup ${PROM_HOME}/prometheus \
  --config.file=${METRICS_DIR}/prometheus.yml \
  --web.enable-remote-write-receiver \
  --storage.tsdb.path=${PROM_HOME}/data \
  --storage.tsdb.retention.time=2d \
  --storage.tsdb.retention.size=10GB \
  --storage.tsdb.wal-compression \
  --web.enable-lifecycle > ${LOG_FILE} 2>&1 &

echo "Prometheus 已在后台运行！"
echo "进程 PID: $!"
echo "日志: tail -f ${LOG_FILE}"
```

### start_grafana.sh

```bash
#!/bin/bash
BASE_DIR="/home/shiyanpeng03"
GRAFANA_HOME="${BASE_DIR}/grafana-v12.0.0"
METRICS_DIR="${BASE_DIR}/script/metrics/grafana"
CONFIG_FILE="${METRICS_DIR}/grafana.ini"
LOG_FILE="${GRAFANA_HOME}/grafana_console.log"
PID_FILE="${GRAFANA_HOME}/grafana.pid"

echo "正在启动 Grafana..."

# 替换配置文件中的路径占位符
sed -i "s|__METRICS_DIR__|${METRICS_DIR}|g" ${METRICS_DIR}/grafana.ini
sed -i "s|__METRICS_DIR__|${METRICS_DIR}|g" ${METRICS_DIR}/provisioning/dashboards/default.yml

cd ${GRAFANA_HOME}

nohup ./bin/grafana-server \
  --config=${CONFIG_FILE} \
  --homepath=${GRAFANA_HOME} \
  >> ${LOG_FILE} 2>&1 &

GRAFANA_PID=$!
echo ${GRAFANA_PID} > ${PID_FILE}

echo "启动成功！"
echo "进程 PID: ${GRAFANA_PID}"
echo "日志: tail -f ${LOG_FILE}"
```

## Dashboard JSON 同步

### 仪表盘文件清单（7 个）

| 文件名 | 说明 |
|---|---|
| `data_grafana_dashboard.json` | Data Dashboard - Ray Data 核心指标 |
| `data_llm_grafana_dashboard.json` | Data LLM Dashboard - LLM 数据指标 |
| `default_grafana_dashboard.json` | Default Dashboard - 默认 Ray 仪表盘 |
| `serve_deployment_grafana_dashboard.json` | Serve Deployment Dashboard - 部署指标 |
| `serve_grafana_dashboard.json` | Serve Dashboard - Serve 服务指标 |
| `serve_llm_grafana_dashboard.json` | Serve LLM Dashboard - LLM 服务指标 |
| `train_grafana_dashboard.json` | Train Dashboard - 训练指标 |

### 同步方法

从本地 `/tmp/ray/session_latest/metrics/grafana/dashboards/` 同步到远端 `/home/shiyanpeng03/script/metrics/grafana/dashboards/`：

```bash
# 使用 wez_write_file 逐个上传
wez_write_file "$(cat /tmp/ray/session_latest/metrics/grafana/dashboards/data_grafana_dashboard.json)" \
  /home/shiyanpeng03/script/metrics/grafana/dashboards/data_grafana_dashboard.json <pane> 30

# 上传后验证 MD5
wez_safe "md5sum /home/shiyanpeng03/script/metrics/grafana/dashboards/*.json" <pane> 15
```

同步完成后需重启 Grafana 使新 dashboard 生效：

```bash
kill $(cat /home/shiyanpeng03/grafana-v12.0.0/grafana.pid)
cd /home/shiyanpeng03/script && bash start_grafana.sh
```

### 本地 vs 远端差异记录（2026-07-09 同步前）

| 差异点 | 本地（最新） | 远端（旧） |
|---|---|---|
| 文件大小 | 488KB | 446KB |
| Panel[5] (id=30) 标题 | Active Tasks | Running Tasks |
| Row 106 Scheduling Loop 子面板 | 8 个（含 Ray Wait Duration、On Data Ready Duration、Ready Tasks Per Step、Dispatch Duration、Tasks Dispatched Per Step、Per-Task Dispatch Duration、Inter-Step Duration） | 1 个（仅 Scheduling Loop Duration） |
| Row 109 Cluster Autoscaler 子面板 | 4 个（多了 Cluster utilization % Memory） | 3 个（缺 Memory） |
| data_llm_grafana_dashboard.json | 存在 | 不存在（新增） |

## 启动与验证

### 启动命令

```bash
# 先启动 Prometheus
cd /home/shiyanpeng03/script && bash start_prometheus.sh

# 再启动 Grafana
cd /home/shiyanpeng03/script && bash start_grafana.sh
```

### 验证

```bash
# 检查进程
ps aux | grep -E 'prometheus|grafana' | grep -v grep

# 检查端口
ss -tlnp | grep -E '9090|3000'

# 查看 Grafana 日志
tail -f /home/shiyanpeng03/grafana-v12.0.0/grafana_console.log

# 查看 Prometheus 日志
tail -f /home/shiyanpeng03/prometheus-3.5.1.linux-amd64/prometheus.log
```

### 访问地址

- **Grafana**: `http://<机器IP>:3000`（默认账号 admin/admin）
- **Prometheus**: `http://<机器IP>:9090`

当前机器 IP: `10.60.192.40`

### 常见警告（非关键）

Grafana 启动时可能出现以下警告，不影响使用：

- `Failed to read plugin provisioning files from directory` — plugins 目录不存在，无需处理
- `can't read alerting provisioning files from directory` — alerting 目录不存在，无需处理
- `Scheduling and sending of reports disabled, SMTP is not configured` — 未配置 SMTP，不影响
- `Grafana server is running with elevated privileges` — root 运行，生产环境建议用非 root 用户

## 部署到新机器

1. 复制整个 `/home/shiyanpeng03/` 目录到新机器
2. 修改 `start_grafana.sh` 和 `start_prometheus.sh` 中的 `BASE_DIR`
3. 确认 `provisioning/datasources/default.yml` 中 Prometheus 地址正确
4. 启动两个脚本
