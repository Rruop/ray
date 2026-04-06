# Ray Dashboard Metrics Label 不匹配 Bug 分析

> 分析日期: 2026-05-18
> 分支: release-2.54.4
> Ray 版本: 2.54.4+kuaishou.7730c6befe
> 环境: lmserv-proj-10121-svc-222769-ray-he-eo-syp-0 (kml-hb2az1-l3-2, lmserving)
> 修复 Commit: `0696100703`

---

## TL;DR

Commit `334ae42b8d` 在 `COMPONENT_METRICS_TAG_KEYS` 中新增了 `NodeId` 和 `ray_io_cluster` 两个 label，但遗漏了更新 `dashboard/head.py` 中的 labels 字典，导致 `prometheus_client` 校验 label 名不匹配抛出 `ValueError`。此 bug **不影响 Dashboard 核心功能**（Web UI、REST API 均正常），仅导致 dashboard 进程自监控指标（CPU/内存/event loop）无法上报，以及每 5 秒产生一条 ERROR 日志。

---

## 目录

- [一、问题现象](#一问题现象)
- [二、Dashboard 架构](#二dashboard-架构)
- [三、Bug 根因分析](#三bug-根因分析)
- [四、影响范围](#四影响范围)
- [五、Event Loop Lag 指标详解](#五event-loop-lag-指标详解)
- [六、`ray_component_*` 指标体系](#六ray_component-指标体系)
- [七、指标导出架构与 Exclude 规则](#七指标导出架构与-exclude-规则)
- [八、修复方案](#八修复方案)
- [九、诊断过程记录](#九诊断过程记录)
- [十、相关源文件索引](#十相关源文件索引)

---

## 一、问题现象

Dashboard 日志（`/tmp/ray/session_latest/logs/dashboard.log`）从启动第一秒起每 5 秒刷一次 ERROR：

```
2026-05-18 15:48:11,840 ERROR utils.py:635 -- Error looping coroutine <function DashboardHead._record_dashboard_metrics at 0x7fb9d2960900>.
Traceback (most recent call last):
  File "/opt/vjepa2/lib/python3.12/site-packages/ray/dashboard/utils.py", line 622, in _looper
    await coro(*args, **kwargs)
  File "/opt/vjepa2/lib/python3.12/site-packages/ray/dashboard/head.py", line 345, in _record_dashboard_metrics
    self._record_cpu_mem_metrics_for_proc(self.dashboard_proc)
  File "/opt/vjepa2/lib/python3.12/site-packages/ray/dashboard/head.py", line 377, in _record_cpu_mem_metrics_for_proc
    self.metrics.metrics_dashboard_cpu.labels(**labels).set(
    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/vjepa2/lib/python3.12/site-packages/prometheus_client/metrics.py", line 175, in labels
    raise ValueError('Incorrect label names')
ValueError: Incorrect label names
```

**特征**：
- 首次出现时间：dashboard 启动后 < 1s（`15:48:11,840`，dashboard 启动于 `15:48:09,984`）
- 重复频率：每 5 秒一次（由 `METRICS_RECORD_INTERVAL_S` 控制）
- 错误类型：确定性错误（非偶发），每次重试必定失败
- 日志量估算：每小时 ~720 条 ERROR 堆栈，每天 ~17,280 条

---

## 二、Dashboard 架构

### 2.1 Head 节点完整进程树

```
PID 1: ray start --head (主启动进程)
├── PID 74:  gcs_server                    ← GCS Server（集群元数据）
├── PID 346: monitor.py                    ← Autoscaler Monitor
├── PID 347: ray.util.client.server        ← Ray Client Proxy
├── PID 348: dashboard.py                  ← Dashboard 主进程
│   ├── PID 529: ray-dashboard-MetricsHead-0
│   ├── PID 530: ray-dashboard-DataHead-0
│   ├── PID 531: ray-dashboard-EventHead-0
│   ├── PID 532: ray-dashboard-JobHead-0
│   ├── PID 533: ray-dashboard-NodeHead-0
│   ├── PID 534: ray-dashboard-ReportHead-0
│   ├── PID 535: ray-dashboard-ServeHead-0
│   ├── PID 536: ray-dashboard-StateHead-0
│   └── PID 537: ray-dashboard-TrainHead-0
├── PID 1048: log_monitor.py              ← 日志收集推送
└── PID 1049: raylet                      ← 本地调度器 + Object Store
    ├── PID 1099: DashboardAgent          ← 节点级监控代理
    └── PID 1101: RuntimeEnvAgent         ← 运行时环境管理
```

**进程启动关系**：
- GCS Server 启动 Dashboard 主进程（`services.py:1265-1287`）
- Dashboard 主进程通过 `multiprocessing.spawn` 启动 9 个子进程模块
- Raylet 启动 DashboardAgent 和 RuntimeEnvAgent（注入 `RAY_NODE_ID` 环境变量）

### 2.2 主进程与子进程协作机制

```
                          外部客户端 (浏览器/curl)
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────┐
│            Dashboard 主进程 (PID 348, head.py)                    │
│                                                                  │
│  aiohttp HTTP Server (0.0.0.0:8265)                             │
│    ├── GET /               → 返回前端 HTML (静态文件)              │
│    ├── GET /static/        → 返回 JS/CSS (静态资源)               │
│    ├── GET /api/jobs/...   → parent_side_handler → proxy → 子进程 │
│    ├── GET /api/nodes/...  → parent_side_handler → proxy → 子进程 │
│    ├── GET /api/cluster_status → 直接处理（DashboardHeadModule）   │
│    └── ...                 → 按路由分发                           │
│                                                                  │
│  Prometheus Metrics Server (0.0.0.0:44227)                       │
│    └── /metrics → prometheus_client 自动暴露所有注册的 Gauge       │
│                                                                  │
│  后台 asyncio tasks：                                             │
│    ├── _gcs_check_alive (GCS 存活检查)                            │
│    ├── _record_dashboard_metrics (每5s, ← 当前报错的协程)          │
│    ├── monitor_loop_lag (每0.25s, event loop 延迟检测)             │
│    └── _do_periodic_health_check × 9 (每1s, 子进程健康检查)        │
└──────────┬───────┬───────┬───────┬───────┬───────┬──────────────┘
           │       │       │       │       │       │
     Unix Domain Socket 通信 (aiohttp.UnixConnector)
           │       │       │       │       │       │
           ▼       ▼       ▼       ▼       ▼       ▼
┌────────┐┌────────┐┌────────┐┌────────┐┌────────┐┌────────┐
│JobHead ││NodeHead││DataHead││ServeH. ││StateH. ││Report  │...
│(aiohttp││(aiohttp││(aiohttp││(aiohttp││(aiohttp││(aiohttp│
│ server)││ server)││ server)││ server)││ server)││ server)│
│        ││        ││        ││        ││        ││        │
│/healthz││/healthz││/healthz││/healthz││/healthz││/healthz│
│/api/... ││/api/... ││/api/... ││/api/... ││/api/... ││/api/... │
└────────┘└────────┘└────────┘└────────┘└────────┘└────────┘
   各自独立进程，监听 Unix Socket:
   /tmp/ray/session_.../sockets/dash_JobHead
   /tmp/ray/session_.../sockets/dash_NodeHead
   ...
```

### 2.3 设计理念

Dashboard 采用**主进程 + 多子进程模块**架构的原因：

1. **故障隔离** — 单个模块 crash（如 OOM）不影响其他模块和主进程
2. **独立重启** — 主进程检测到子进程异常后自动重启，无需整体重启
3. **避免 GIL 竞争** — 各模块在独立 Python 进程中，互不阻塞
4. **可选部署** — 通过 `--modules-to-load` 可选择性加载模块
5. **独立部署** — Dashboard 可作为独立实例部署（不依赖与 Raylet 同节点）

### 2.4 启动流程

```python
# head.py:393-438 — DashboardHead.run()

# 1. 初始化 GCS Client
self.gcs_client = GcsClient(address=gcs_address, cluster_id=self.cluster_id_hex)
internal_kv._initialize_internal_kv(self.gcs_client)

# 2. 加载所有模块（DashboardHeadModule + SubprocessModule）
dashboard_head_modules, subprocess_module_handles = self._load_modules(modules_to_load)

# 3. 并行启动所有子进程 (multiprocessing.spawn)
for handle in subprocess_module_handles:
    handle.start_module()         # 非阻塞，spawn 出子进程

# 4. 等待所有子进程就绪 (通过 Pipe 收到 ready 信号)
for handle in subprocess_module_handles:
    handle.wait_for_module_ready()  # 阻塞直到子进程通知就绪

# 5. 设置 Prometheus metrics 导出
self.metrics = await self._setup_metrics(self.gcs_client)  # 启动 port 44227

# 6. 启动 metrics 记录协程（← 出问题的协程）
self.record_dashboard_metrics_task = asyncio.create_task(
    self._record_dashboard_metrics(subprocess_module_handles)
)

# 7. 配置 HTTP server，绑定路由，开始服务
await self._configure_http_server(dashboard_head_modules, subprocess_module_handles)
```

### 2.5 子进程启动内部逻辑

```python
# module.py:231-273 — 子进程入口
def run_module(cls, config, incarnation, child_conn):
    # 设置进程名: "ray-dashboard-JobHead-0"
    ray._raylet.setproctitle(f"ray-dashboard-{module_name}-{incarnation}")

    # 配置独立日志文件: dashboard_JobHead.log
    setup_component_logger(...)

    # 启动独立 event loop
    loop = asyncio.new_event_loop()
    # 内部：
    #   module = cls(config)
    #   启动 parent process death detection（父进程挂了自杀）
    #   await module.run()   → 启动 aiohttp UnixSite
    #   child_conn.send(None)  → 通知父进程已就绪
```

子进程的 HTTP server（`module.py:101-143`）：
```python
async def run(self):
    app = aiohttp.web.Application()
    routes = [aiohttp.web.get("/api/healthz", self._internal_module_health_check)]
    # 注册所有用 @SubprocessRouteTable.get/post 装饰的业务 handler
    for _, handler in handlers_with_route_attrs:
        routes.append(aiohttp.web.route(method, path, handler))
    app.add_routes(routes)

    # 监听 Unix Socket（非 TCP）
    socket_path = "/tmp/ray/session_.../sockets/dash_JobHead"
    site = aiohttp.web.UnixSite(runner, socket_path)
    await site.start()
```

### 2.6 请求转发机制

当外部请求到达主进程时的完整路径：

```
客户端 → HTTP :8265 → aiohttp Router → parent_side_handler
  → SubprocessModuleHandle.proxy_request(request, resp_type)
    → proxy_http() / proxy_stream() / proxy_websocket()
      → aiohttp.ClientSession (UnixConnector) → Unix Socket
        → 子进程 aiohttp server → 实际 handler → response
          → 原路返回客户端
```

路由注册代码（`routes.py`）：
```python
class SubprocessRouteTable:
    @classmethod
    def _register_route(cls, method, path, resp_type):
        # 生成 parent_side_handler 作为主进程的路由 handler
        async def parent_side_handler(request):
            handle = cls._bind_map[method][path].instance
            return await handle.proxy_request(request, resp_type)
        cls._routes.route(method, path)(parent_side_handler)
```

代理实现（`handle.py:298-318`）：
```python
async def proxy_http(self, request):
    url = f"http://localhost{request.path_qs}"  # URL 占位，实际走 Unix Socket
    body = await request.read()
    async with self.http_client_session.request(
        request.method, url, data=body,
        headers=filter_hop_by_hop_headers(request.headers),
    ) as backend_resp:
        resp_body = await backend_resp.read()
        return aiohttp.web.Response(
            status=backend_resp.status,
            headers=filter_hop_by_hop_headers(backend_resp.headers),
            body=resp_body,
        )
```

Unix Socket 连接建立（`utils.py:55-67`）：
```python
def get_http_session_to_module(module_name, socket_dir, session_name):
    socket_path = f"{socket_dir}/dash_{module_name}"
    connector = aiohttp.UnixConnector(socket_path)
    return aiohttp.ClientSession(connector=connector)
```

### 2.7 健康检查与自动重启

```python
# handle.py:259-282
async def _do_periodic_health_check(self):
    while True:
        try:
            # 两项检查：
            # 1. process.exitcode is not None → 进程已退出
            # 2. GET /api/healthz 返回非 200 → event loop 卡住
            await self._do_once_health_check()
        except Exception:
            logger.exception(f"Module {name} is unhealthy. "
                           f"Refer to {log_file} for details.")
            # 销毁旧进程（terminate → kill → cleanup）
            await self.destroy_module()
            # 重新启动
            self.start_module()
            self.wait_for_module_ready()
            return  # 新的 health_check_task 已在 wait_for_module_ready 中创建
        await asyncio.sleep(1)
```

销毁流程（`handle.py:165-228`）：
```python
async def destroy_module(self):
    self.incarnation += 1           # 递增重启计数
    # 1. 取消 health check task
    # 2. 关闭 parent Pipe connection
    # 3. terminate → join(timeout) → kill（优雅退出 + 强制兜底）
    # 4. 关闭 HTTP client session
```

### 2.8 子进程自保机制

```python
# module.py:76-86
async def _detect_parent_process_death(self):
    """检测父进程存活，父进程挂了则子进程自杀"""
    while True:
        if not self._parent_process.is_alive():
            logger.warning(f"Parent process {pid} died. Exiting...")
            sys.exit()
        await asyncio.sleep(1)
```

### 2.9 三种请求代理模式

| 模式 | 方法 | 用途 | 典型场景 |
|------|------|------|----------|
| `HTTP` | `proxy_http()` | 普通 REST API | `/api/jobs`, `/api/nodes` |
| `STREAM` | `proxy_stream()` | 流式响应 | 日志流、大文件下载 |
| `WEBSOCKET` | `proxy_websocket()` | WebSocket 双向通信 | 实时日志推送 |

---

## 三、Bug 根因分析

### 3.1 引入 Bug 的 Commit

```
Commit:  334ae42b8d
Subject: [Metric] Add NodeID tag for metric to identify the logical nodes
         on a single machine and using the same ip address
Author:  shiyanpeng03 <shiyanpeng03@kuaishou.com>
Date:    Thu Mar 12 11:51:51 2026 +0800

变更文件:
  python/ray/_private/telemetry/open_telemetry_metric_recorder.py  (+15 -2)
  python/ray/dashboard/consts.py                                   (+3 -2)
  python/ray/dashboard/modules/reporter/reporter_agent.py          (+6 -4)
  python/ray/data/_internal/stats.py                               (+9 -2)
```

### 3.2 该 Commit 的变更内容

**目的**：解决多个 Ray 节点共享同一 IP 地址时，指标无法区分不同逻辑节点的问题。

**`consts.py` 的修改**：
```python
# 修改前（6 个 label）
COMPONENT_METRICS_TAG_KEYS = ["ip", "pid", "Version", "Component", "SessionName", "ray_io_cluster"]

# 修改后（7 个 label）—— 新增 NodeId
COMPONENT_METRICS_TAG_KEYS = ["ip", "NodeId", "pid", "Version", "Component", "SessionName", "ray_io_cluster"]
```

**`reporter_agent.py` 的修改（正确）**：
```python
# 三处 tags 都补充了 NodeId
tags = {"ip": self._ip, "NodeId": self._dashboard_agent.node_id, "Component": component_name}
```

### 3.3 遗漏的修改

**`head.py` 未被修改**。两处 labels 字典仍然只有 5 个 key：

```python
# head.py:337 — _record_dashboard_metrics 中（用于 event_loop_tasks 和 event_loop_lag）
labels = {
    "ip": self.ip,
    "pid": self.pid,
    "Version": ray.__version__,
    "Component": "dashboard",
    "SessionName": self.session_name,
    # ✗ 缺少 "NodeId"
    # ✗ 缺少 "ray_io_cluster"
}

# head.py:369 — _record_cpu_mem_metrics_for_proc 中（用于 CPU/内存指标）
labels = {
    "ip": self.ip,
    "pid": proc.pid,
    "Version": ray.__version__,
    "Component": "dashboard" if not module_name else "dashboard_" + module_name,
    "SessionName": self.session_name,
    # ✗ 缺少 "NodeId"
    # ✗ 缺少 "ray_io_cluster"
}
```

### 3.4 Prometheus Client 的校验机制

`prometheus_client` 库在 Gauge 创建时注册 labelnames，运行时严格校验：

```python
# 创建时（dashboard_metrics.py:94）
self.metrics_dashboard_cpu = Gauge(
    "component_cpu",
    "Dashboard CPU percentage usage.",
    tuple(COMPONENT_METRICS_TAG_KEYS),  # 注册 7 个 labelnames
    unit="percentage",
    namespace="ray",
    registry=self.registry,
)

# 调用时（prometheus_client/metrics.py:175）
def labels(self, *labelvalues, **labelkwargs):
    if labelkwargs:
        if set(labelkwargs.keys()) != set(self._labelnames):  # 严格集合比较
            raise ValueError('Incorrect label names')
    ...
```

**传入 5 个 key** `{ip, pid, Version, Component, SessionName}`
**≠ 声明的 7 个** `{ip, NodeId, pid, Version, Component, SessionName, ray_io_cluster}`

→ `ValueError('Incorrect label names')`

### 3.5 为什么 `ray_io_cluster` 也缺失

`ray_io_cluster` 是更早引入的 label（用于多集群环境下区分不同 Ray 集群）。在 reporter_agent 中通过 global_tags 机制全局注入：

```python
# reporter_agent.py:1808-1812
cluster_name = os.environ.get("RAY_CLUSTER_NAME")
if cluster_name:
    global_tags["ray_io_cluster"] = cluster_name
```

但 `head.py` 中的 labels 从一开始就没有包含这个字段。在 `COMPONENT_METRICS_TAG_KEYS` 中加入 `ray_io_cluster` 的时间早于 `NodeId`，说明 **`head.py` 的 labels 一直与 `COMPONENT_METRICS_TAG_KEYS` 的实际定义不同步**。在 `NodeId` 被加入之前，`COMPONENT_METRICS_TAG_KEYS` 有 6 个 key 而 `head.py` 只传了 5 个，这意味着此 bug 可能在更早的版本就已存在。

### 3.6 Bug 存在时间线

```
时间线（推断）：
  ┌─ 某早期 commit: COMPONENT_METRICS_TAG_KEYS 加入 "ray_io_cluster"
  │   → head.py 未更新 → bug 已存在但可能当时 key 数量偶然匹配
  │
  ├─ 334ae42b8d (2026-03-12): 加入 "NodeId"
  │   → reporter_agent.py 正确更新
  │   → head.py 未更新 → bug 确定性触发
  │
  └─ 0696100703 (2026-05-18): 修复
```

---

## 四、影响范围

### 4.1 受影响的指标（无法上报）

| Prometheus 指标名 | 含义 | 上报函数 | 覆盖进程 |
|-------------------|------|----------|----------|
| `ray_component_cpu_percentage` | 进程 CPU 使用率 | `_record_cpu_mem_metrics_for_proc` | dashboard + 9 个子模块 |
| `ray_component_uss_mb` | 进程 USS 内存（独占） | `_record_cpu_mem_metrics_for_proc` | dashboard + 9 个子模块 |
| `ray_component_rss_mb` | 进程 RSS 内存（常驻） | `_record_cpu_mem_metrics_for_proc` | dashboard + 9 个子模块 |
| `ray_dashboard_event_loop_tasks_tasks` | event loop task 数量 | `_record_dashboard_metrics` | 仅 dashboard 主进程 |
| `ray_dashboard_event_loop_lag_seconds` | event loop 调度延迟 | `_record_dashboard_metrics` | 仅 dashboard 主进程 |

共 **10 个进程 × 3 个资源指标 + 2 个 event loop 指标 = 32 条时间序列** 丢失。

### 4.2 不受影响的功能

| 功能 | 原因 |
|------|------|
| Dashboard Web UI 页面加载 | HTTP server 独立于 metrics 协程运行 |
| 所有 REST API（/api/jobs, /api/nodes, /api/cluster_status 等） | 路由转发链路完全不涉及 metrics 协程 |
| 子进程模块正常工作（Job/Node/Serve/Data/State 等） | 子进程生命周期与主进程 metrics 无关 |
| 子进程健康检查 + 自动重启 | `_do_periodic_health_check` 是独立 asyncio task |
| Autoscaler | 运行在独立进程 (monitor.py, PID 346) |
| Ray 集群核心调度 | Raylet/GCS 不依赖 Dashboard |
| 其他节点的指标上报 | DashboardAgent 的 reporter 独立工作 |

### 4.3 副作用

1. **日志噪音**：
   - 每 5 秒 1 条 ERROR + 完整 Python 堆栈（约 10 行）
   - 每小时 ~720 条，每天 ~17,280 条
   - 可能干扰真正的 ERROR 日志排查

2. **Dashboard 进程可观测性丧失**：
   - 无法通过 Grafana 观察 dashboard 各子模块的 CPU/内存趋势
   - 无法检测 dashboard 主进程 event loop 卡顿
   - 在 dashboard 响应变慢时缺少排查依据

3. **日志轮转压力**：
   - 按当前配置 `logging-rotate-bytes=536870912`（512MB），ERROR 日志占用额外空间

### 4.4 为什么不会导致 Dashboard 崩溃

`@dashboard_utils.async_loop_forever` 装饰器提供了容错：

```python
# utils.py — _looper 实现
async def _looper(*args, **kwargs):
    while True:
        try:
            await coro(*args, **kwargs)
        except Exception:
            logger.exception(f"Error looping coroutine {coro}.")  # 捕获异常，只打日志
        await asyncio.sleep(interval)  # METRICS_RECORD_INTERVAL_S = 5
```

异常被捕获 → 打印日志 → 等待 5 秒 → 重试 → 再次失败 → 无限循环。
协程不会传播异常到 event loop，不会影响其他 asyncio task 的调度。

---

## 五、Event Loop Lag 指标详解

### 5.1 测量原理

```python
# python/ray/_private/async_utils.py:27-52
def enable_monitor_loop_lag(callback, interval_s=0.25, loop=None):
    async def monitor():
        while loop.is_running():
            t0 = loop.time()
            await asyncio.sleep(interval_s)       # 预期休眠 0.25s
            lag = loop.time() - t0 - interval_s   # 实际耗时 - 预期 = 偏差
            callback(lag)                          # 回调记录 lag
    loop.create_task(monitor(), name="async_utils.monitor_loop_lag")
```

**工作机制**：
1. 协程 `asyncio.sleep(0.25)` 告诉 event loop "0.25 秒后叫醒我"
2. 如果 event loop 空闲，0.25s 后精确唤醒，`lag ≈ 0`
3. 如果 event loop 被某个同步/阻塞操作占用（如 CPU 密集计算、同步 IO），唤醒会延迟
4. 延迟量 = 实际耗时 - 0.25s = event loop 被占用的时间

### 5.2 上报方式

```python
# head.py — 记录窗口内最大值
def on_new_lag(lag_s):
    self._event_loop_lag_s_max = max(self._event_loop_lag_s_max or 0, lag_s)

# 每 5s 上报一次（取窗口内峰值后重置）
if self._event_loop_lag_s_max is not None:
    self.metrics.metrics_event_loop_lag.labels(**labels).set(
        float(self._event_loop_lag_s_max)
    )
    self._event_loop_lag_s_max = None
```

### 5.3 `event_loop_tasks` 的含义

```python
self.metrics.metrics_event_loop_tasks.labels(**labels).set(
    len(asyncio.all_tasks(loop))    # asyncio 层面的 task 数量
)
```

这里的 "task" 是 **Python asyncio Task**（协程），**不是 Ray 分布式 Task**。包括：

| 类型 | 示例 | 常驻？ |
|------|------|--------|
| 后台监控协程 | `_record_dashboard_metrics`, `_gcs_check_alive`, `monitor_loop_lag` | 是 |
| 子进程健康检查 | `_do_periodic_health_check` × 9 | 是 |
| HTTP 请求处理 | 每个正在处理的 aiohttp request handler | 否（请求完成即消失） |
| 内部任务 | parent process death detection（子进程中） | 是 |

### 5.4 告警阈值参考

| lag 值 | 含义 | 建议动作 |
|--------|------|----------|
| < 10ms | 正常 | 无 |
| 10ms - 100ms | 轻微延迟 | 关注 |
| 100ms - 1s | 明显卡顿 | 排查阻塞源 |
| > 1s | 严重阻塞 | HTTP 请求可能超时，需立即排查 |

| tasks 数量 | 含义 |
|-----------|------|
| 10-20 | 正常（后台常驻 + 少量请求） |
| 50+ | 可能有请求堆积 |
| 100+ | 严重堆积，dashboard 可能不响应 |

---

## 六、`ray_component_*` 指标体系

### 6.1 完整指标列表

| Prometheus 指标名 | 含义 | 单位 | 来源 |
|-------------------|------|------|------|
| `ray_component_cpu_percentage` | 组件 CPU 使用率 | % | psutil.cpu_percent |
| `ray_component_rss_mb` | 组件 RSS 内存（Resident Set Size） | MB | psutil.memory_info.rss |
| `ray_component_uss_mb` | 组件 USS 内存（Unique Set Size，独占） | MB | psutil.memory_full_info.uss |
| `ray_component_mem_shared_bytes` | 组件共享内存（SHM） | bytes | psutil.memory_info.shared |
| `ray_component_num_fds` | 组件打开的文件描述符数 | count | psutil.num_fds |
| `ray_component_gpu_percentage` | 组件 GPU 利用率 | % | nvidia-smi / pynvml |
| `ray_component_gpu_memory_mb` | 组件 GPU 显存占用 | MB | nvidia-smi / pynvml |

### 6.2 Label 结构

所有 component 指标共享 `COMPONENT_METRICS_TAG_KEYS`：

```python
["ip", "NodeId", "pid", "Version", "Component", "SessionName", "ray_io_cluster"]
```

| Label | 含义 | 示例值 |
|-------|------|--------|
| `ip` | 节点 IP | `10.15.6.116` |
| `NodeId` | 节点唯一 ID（解决多节点共享 IP 问题） | `ed7040e992fa61...` |
| `pid` | 进程 PID | `348` |
| `Version` | Ray 版本 | `2.54.4+kuaishou.7730c6befe` |
| `Component` | 组件名称 | `dashboard`, `raylet`, `gcs` |
| `SessionName` | Ray session 名称 | `session_2026-05-18_15-48-07_198283_1` |
| `ray_io_cluster` | 集群名（多集群区分） | 来自 `RAY_CLUSTER_NAME` 环境变量 |

### 6.3 上报的组件分类

| Component label 值 | 对应进程 | 上报者 | 上报节点 |
|--------------------|---------|--------|----------|
| `gcs` | GCS Server | Reporter Agent | 仅 head 节点 |
| `raylet` | Raylet（调度器 + object store） | Reporter Agent | 每个节点 |
| `agent` | DashboardAgent（指标采集代理） | Reporter Agent | 每个节点 |
| `workers` | 所有 worker 进程（按类聚合） | Reporter Agent | 每个节点 |
| `dashboard` | Dashboard 主进程 | Dashboard 自身 | 仅 head 节点 |
| `dashboard_MetricsHead` | Metrics 子模块 | Dashboard 自身 | 仅 head 节点 |
| `dashboard_DataHead` | Data 子模块 | Dashboard 自身 | 仅 head 节点 |
| `dashboard_JobHead` | Job 子模块 | Dashboard 自身 | 仅 head 节点 |
| `dashboard_NodeHead` | Node 子模块 | Dashboard 自身 | 仅 head 节点 |
| `dashboard_ReportHead` | Report 子模块 | Dashboard 自身 | 仅 head 节点 |
| `dashboard_ServeHead` | Serve 子模块 | Dashboard 自身 | 仅 head 节点 |
| `dashboard_StateHead` | State 子模块 | Dashboard 自身 | 仅 head 节点 |
| `dashboard_TrainHead` | Train 子模块 | Dashboard 自身 | 仅 head 节点 |
| `dashboard_EventHead` | Event 子模块 | Dashboard 自身 | 仅 head 节点 |

### 6.4 两条上报路径的职责划分

```
                    ┌─────────────────────────────────────────┐
                    │        指标采集与导出全景                  │
                    └─────────────────────────────────────────┘

┌────────────────────────────────────┐    ┌─────────────────────────────────────┐
│  Dashboard 主进程 (head.py)         │    │  Reporter Agent (reporter_agent.py) │
│                                    │    │  (每个节点一个)                       │
│  采集对象:                          │    │                                     │
│    • dashboard 主进程               │    │  采集对象:                           │
│    • 9 个 dashboard_* 子模块         │    │    • gcs (仅 head)                  │
│                                    │    │    • raylet                         │
│  导出方式:                          │    │    • agent                          │
│    prometheus_client HTTP           │    │    • workers                        │
│    pull 模式 (port 44227)           │    │                                     │
│                                    │    │  导出方式:                           │
│  不受 exclude 规则影响              │    │    OpenTelemetry remote_write        │
│                                    │    │    push 模式                         │
│  设计原因:                          │    │    推送到 http://10.81.0.157:9090    │
│    Dashboard 可独立部署,             │    │                                     │
│    不应依赖 Agent                   │    │  受 DEFAULT_EXCLUDE_PATTERNS 影响    │
└────────────────────────────────────┘    └─────────────────────────────────────┘
```

**关键设计决策**（`reporter_agent.py:1710-1712`）：
```python
# NOTE: Dashboard metrics is recorded within the dashboard because
# it can be deployed as a standalone instance. It shouldn't
# depend on the agent.
```

两者通过 `Component` label 区分，指标名相同但 label 值不同，在 Prometheus 中是不同时间序列，**没有冲突也没有重复**。

### 6.5 指标作用

| 监控场景 | 使用指标 | 排查思路 |
|---------|---------|---------|
| GCS CPU 飙高 | `component_cpu{Component="gcs"}` | 集群规模大、actor 多、内部 RPC 压力 |
| Raylet 内存泄漏 | `component_rss{Component="raylet"}` | object store spilling、大量小对象 |
| Dashboard 响应慢 | `event_loop_lag` + `component_cpu{Component="dashboard"}` | 同步阻塞操作或 CPU 密集 |
| Worker OOM | `component_rss{Component="workers"}` | 任务内存超限 |
| FD 泄漏 | `component_num_fds{Component=*}` | 未关闭的连接/文件句柄 |
| GPU 利用率低 | `component_gpu_percentage` | 数据加载瓶颈、batch 过小 |

---

## 七、指标导出架构与 Exclude 规则

### 7.1 两条导出通道对比

| 维度 | Dashboard 主进程 | Reporter Agent |
|------|-----------------|----------------|
| **库** | `prometheus_client` | OpenTelemetry SDK |
| **协议** | Prometheus pull (scrape) | Prometheus Remote Write (push) |
| **端口** | 44227 | N/A |
| **目标** | Prometheus server 主动拉取 | 推送到 `http://10.81.0.157:9090/api/v1/write` |
| **过滤** | 无 | `DEFAULT_EXCLUDE_PATTERNS` |
| **覆盖组件** | `dashboard*` | `gcs`, `raylet`, `agent`, `workers` |
| **节点** | 仅 head | 每个节点 |

### 7.2 DEFAULT_EXCLUDE_PATTERNS 完整列表（修复前）

```python
DEFAULT_EXCLUDE_PATTERNS = [
    # Ray Data - Iterator internal details
    "ray_data_iter_block_*",
    "ray_data_iter_batch_*",
    "ray_data_iter_initialize_*",
    "ray_data_iter_get_*",
    "ray_data_iter_format_*",
    "ray_data_iter_collate_*",
    "ray_data_iter_finalize_*",
    "ray_data_iter_blocks_*",
    "ray_data_iter_prefetched_*",
    # Ray Data - Fine-grained input/output metrics
    "ray_data_num_inputs_*",
    "ray_data_bytes_inputs_*",
    "ray_data_num_task_inputs_*",
    "ray_data_bytes_task_inputs_*",
    "ray_data_num_task_outputs_*",
    "ray_data_bytes_task_outputs_*",
    "ray_data_rows_task_outputs_*",
    "ray_data_*_outputs_taken",
    "ray_data_*_outputs_of_finished_*",
    "ray_data_num_external_*",
    "ray_data_average_*",
    "ray_data_obj_store_mem_internal_*",
    "ray_data_block_serialization_*",
    "ray_data_block_generation_*",
    # Ray Data - Histogram metrics (high cardinality)
    "ray_data_task_completion_time",
    "ray_data_block_completion_time",
    "ray_data_block_size_*",
    # Node - Component-level details (too fine-grained)  ← 已移除
    "ray_component_*",                                   ← 已移除
    # Ray Core - Internal details
    "ray_operation_*",
    "ray_internal_*",
    "ray_spill_manager_*",
    "ray_pull_manager_*",
    "ray_push_manager_*",
    "ray_grpc_*",
    "ray_gcs_storage_*",
    "ray_gcs_task_manager_*",
]
```

### 7.3 修复后各指标的导出状态

| 指标 | Dashboard (port 44227) | Remote Write (Agent) |
|------|----------------------|---------------------|
| `ray_component_cpu{Component="dashboard"}` | ✓ 修复后可用 | N/A（Agent 不采集） |
| `ray_component_cpu{Component="gcs"}` | N/A（Dashboard 不采集） | ✓ 移除 exclude 后可用 |
| `ray_component_cpu{Component="raylet"}` | N/A | ✓ 移除 exclude 后可用 |
| `ray_component_cpu{Component="agent"}` | N/A | ✓ 移除 exclude 后可用 |
| `ray_component_cpu{Component="workers"}` | N/A | ✓ 移除 exclude 后可用 |
| `ray_dashboard_event_loop_lag_seconds` | ✓ 修复后可用 | N/A |
| `ray_dashboard_event_loop_tasks_tasks` | ✓ 修复后可用 | N/A |

### 7.4 过滤机制源码

```python
# open_telemetry_metric_recorder.py:290-319
def _should_include_metric(self, metric_name: str) -> bool:
    """
    Filter logic:
        1. If include patterns specified, metric must match at least one
        2. If exclude patterns specified, metric must not match any
        3. If no patterns specified, include all metrics
    """
    if self._include_patterns:
        matched = any(fnmatch.fnmatch(metric_name, p) for p in self._include_patterns)
        if not matched:
            return False
    if self._exclude_patterns:
        excluded = any(fnmatch.fnmatch(metric_name, p) for p in self._exclude_patterns)
        if excluded:
            return False
    return True
```

**覆盖方式**：设置环境变量 `RAY_METRICS_REMOTE_WRITE_EXCLUDE_METRICS` 可完全替换默认 exclude 列表（设为空字符串则不排除任何指标）。

---

## 八、修复方案

### 8.1 修复 Commit

```
0696100703 [Metric] Fix dashboard metrics label mismatch and remove component exclude
Branch: release-2.54.4
```

### 8.2 Diff

```diff
--- a/python/ray/dashboard/head.py
+++ b/python/ray/dashboard/head.py
@@ -124,6 +124,8 @@ class DashboardHead:
         self.ip = node_ip_address
         self.pid = os.getpid()
+        self.node_id = ""  # TODO: pass node_id from startup args
+        self.ray_io_cluster = os.environ.get("RAY_CLUSTER_NAME", "")
         self.dashboard_proc = psutil.Process()

@@ -337,10 +339,12 @@ class DashboardHead:
         labels = {
             "ip": self.ip,
+            "NodeId": self.node_id,
             "pid": self.pid,
             "Version": ray.__version__,
             "Component": "dashboard",
             "SessionName": self.session_name,
+            "ray_io_cluster": self.ray_io_cluster,
         }

@@ -369,10 +373,12 @@ class DashboardHead:
         labels = {
             "ip": self.ip,
+            "NodeId": self.node_id,
             "pid": proc.pid,
             "Version": ray.__version__,
             "Component": "dashboard" if not module_name else "dashboard_" + module_name,
             "SessionName": self.session_name,
+            "ray_io_cluster": self.ray_io_cluster,
         }

--- a/python/ray/_private/telemetry/open_telemetry_metric_recorder.py
+++ b/python/ray/_private/telemetry/open_telemetry_metric_recorder.py
@@ -83,8 +83,6 @@ DEFAULT_EXCLUDE_PATTERNS = [
-    # Node - Component-level details (too fine-grained)
-    "ray_component_*",
     # Ray Core - Internal details
```

### 8.3 修复验证清单

| 检查项 | 结果 | 说明 |
|--------|------|------|
| label key 与 `COMPONENT_METRICS_TAG_KEYS` 完全一致 | ✓ | 7 个 key 完全匹配 |
| `os` 模块已导入 | ✓ | `head.py` line 3: `import os` |
| 环境变量只在 `__init__` 读取一次 | ✓ | 避免每 5 秒重复 syscall |
| `node_id` 后续可无缝升级 | ✓ | TODO 标记，改一行即可 |
| 改动最小化 | ✓ | 仅涉及 2 个文件，+6 -2 行 |
| 不影响 reporter_agent 上报 | ✓ | 独立路径，互不干扰 |
| 两个函数的 `pid` 使用正确 | ✓ | 主函数用 `self.pid`，子函数用 `proc.pid` |
| exclude 移除不影响其他过滤 | ✓ | 其他规则（ray_data_*、ray_operation_* 等）保持不变 |

### 8.4 后续优化（TODO）

1. **传入真实 node_id**：修改 `services.py` 启动 dashboard 时传入 `--node-id` 参数，让 dashboard 能获取到 head 节点的真实 node_id（当前为空字符串，在单节点环境无影响，但多节点共享 IP 时无法区分）。

2. **指标基数评估**：移除 `ray_component_*` exclude 后，大规模集群中每个节点增加 ~5-7 个时间序列（取决于是否有 GPU），需评估远端 Prometheus 的存储/写入压力。

3. **单元测试补充**：为 `_record_dashboard_metrics` 添加 mock 测试，确保 labels 与定义始终同步。

---

## 九、诊断过程记录

### 9.1 连接方式

通过 KML Web Shell 脚本连接到容器：
```bash
python3 kml_ws_exec.py \
  --url "https://kml.corp.kuaishou.com/v2/#/system/terminal?clusterName=kml-hb2az1-l3-2&namespace=lmserving&pod=lmserv-proj-10121-svc-222769-ray-he-eo-syp-0&mode=shell&fullScreen=1&auth=gaia" \
  --wait-for-ws 120 \
  --cmd "<command>"
```

### 9.2 诊断步骤

**Step 1: 进程状态检查**
```bash
ps aux | grep -E 'dashboard|ray' | grep -v grep
```
→ Dashboard 进程 (PID 348) 正常运行，所有子模块进程存在

**Step 2: 端口监听确认**
```bash
netstat -tlnp | grep -E '8265|8080|6379'
```
→ 8265(dashboard)、6379(gcs) 正常监听

**Step 3: HTTP 连通性**
```bash
curl -s -o /dev/null -w '%{http_code}' http://localhost:8265/
curl -s http://localhost:8265/api/cluster_status | head -50
```
→ HTTP 200，API 正常返回 JSON

**Step 4: 日志分析**
```bash
tail -100 /tmp/ray/session_latest/logs/dashboard.log
head -50 /tmp/ray/session_latest/logs/dashboard.log
```
→ 发现 `ValueError: Incorrect label names`，从启动第一秒开始

**Step 5: 代码对比**
```bash
# 查看 Gauge 创建时的 labelnames
grep -n 'COMPONENT_METRICS_TAG_KEYS' .../consts.py
# → 7 个 key

# 查看实际传入的 labels
sed -n '369,375p' .../head.py
# → 5 个 key

# 确认差异
# 缺少: NodeId, ray_io_cluster
```

**Step 6: Git Blame 定位引入时间**
```bash
git log --oneline -- python/ray/dashboard/consts.py
# → 334ae42b8d 添加了 NodeId

git show 334ae42b8d -- python/ray/dashboard/head.py
# → 空输出，该 commit 未修改 head.py
```

**Step 7: 确认 Ray 版本**
```bash
python3 -c 'import ray; print(ray.__version__); print(ray.__commit__)'
# → 2.54.4+kuaishou.7730c6befe
# → 7730c6befeb691ca003ef60ef8e64f150ca4d3ac
```

---

## 十、相关源文件索引

| 文件路径 | 关键行号 | 作用 |
|---------|---------|------|
| `python/ray/dashboard/head.py` | 125-128 | DashboardHead 属性初始化 |
| `python/ray/dashboard/head.py` | 333-368 | `_record_dashboard_metrics` 协程 |
| `python/ray/dashboard/head.py` | 370-392 | `_record_cpu_mem_metrics_for_proc` |
| `python/ray/dashboard/head.py` | 393-462 | `DashboardHead.run()` 启动流程 |
| `python/ray/dashboard/head.py` | 245-297 | 子进程模块加载 |
| `python/ray/dashboard/consts.py` | 80 | `COMPONENT_METRICS_TAG_KEYS` 定义 |
| `python/ray/dashboard/consts.py` | 85-91 | `AVAILABLE_COMPONENT_NAMES_FOR_METRICS` |
| `python/ray/dashboard/consts.py` | 95 | `METRICS_RECORD_INTERVAL_S` = 5 |
| `python/ray/dashboard/dashboard_metrics.py` | 78-117 | Prometheus Gauge 定义 |
| `python/ray/dashboard/dashboard.py` | 77-95, 101-282 | Dashboard 入口 + argparse |
| `python/ray/dashboard/subprocesses/handle.py` | 56-163 | SubprocessModuleHandle（启动/就绪） |
| `python/ray/dashboard/subprocesses/handle.py` | 230-282 | 健康检查 + 自动重启 |
| `python/ray/dashboard/subprocesses/handle.py` | 284-383 | 请求代理（HTTP/Stream/WS） |
| `python/ray/dashboard/subprocesses/module.py` | 55-198 | SubprocessModule 基类 |
| `python/ray/dashboard/subprocesses/module.py` | 201-273 | 子进程入口 `run_module` |
| `python/ray/dashboard/subprocesses/routes.py` | 10-96 | SubprocessRouteTable 路由注册 |
| `python/ray/dashboard/subprocesses/utils.py` | 45-67 | Unix Socket 路径 + 连接建立 |
| `python/ray/dashboard/modules/reporter/reporter_agent.py` | 1145-1192 | component 指标生成（零值） |
| `python/ray/dashboard/modules/reporter/reporter_agent.py` | 1194-1290 | component 指标生成（实际值） |
| `python/ray/dashboard/modules/reporter/reporter_agent.py` | 1676-1712 | 各组件指标上报入口 |
| `python/ray/dashboard/modules/reporter/reporter_agent.py` | 1808-1812 | `ray_io_cluster` 注入 |
| `python/ray/_private/telemetry/open_telemetry_metric_recorder.py` | 53-92 | `DEFAULT_EXCLUDE_PATTERNS` |
| `python/ray/_private/telemetry/open_telemetry_metric_recorder.py` | 290-319 | 过滤逻辑 `_should_include_metric` |
| `python/ray/_private/telemetry/open_telemetry_metric_recorder.py` | 512-534 | exclude 规则加载 |
| `python/ray/_private/async_utils.py` | 27-52 | `enable_monitor_loop_lag` 实现 |
| `python/ray/_private/services.py` | 1265-1297 | Dashboard 进程启动命令构造 |
| `src/ray/raylet/agent_manager.cc` | 52 | `RAY_NODE_ID` 环境变量注入 |
