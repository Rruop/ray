# Dashboard Agent 架构与 HTTP 端口绑定深度分析

## 1. 进程模型

**整个 DashboardAgent 是一个独立进程**（由 raylet 启动），内部运行：
- 1 个 gRPC server（`grpc.aio.server`，异步）
- 1 个 HTTP server（`aiohttp.web.Application`）
- N 个 agent 模块（**均为 asyncio task，共享同一事件循环，不是子线程也不是子进程**）

**ReporterAgent 不是独立进程**，只是 DashboardAgent 进程内的一个模块。

### 1.1 子模块的运行方式 — asyncio task，不是子线程

所有子模块通过 `asyncio.gather` 并发运行在同一个事件循环中：

```python
# agent.py:run() 中的核心行
tasks = [m.run(self.server) for m in modules]
await asyncio.gather(*tasks)
```

每个模块的 `run()` 方法是一个 async 协程，被 `asyncio.gather` 调度执行。**不是线程、不是子进程**，所有模块共享同一个 Python 进程和同一个 asyncio 事件循环。

部分模块内部会使用 `ThreadPoolExecutor` 执行 CPU 密集型操作（避免阻塞事件循环）：

| 模块 | 是否使用 ThreadPoolExecutor | 线程数 | 用途 |
|------|---------------------------|--------|------|
| ReporterAgent | 是 | 1（`RAY_DASHBOARD_REPORTER_AGENT_TPE_MAX_WORKERS`） | stats 采集、payload 序列化 |
| EventAgent | 是 | 1 | 文件系统事件监控 |
| AggregatorAgent | 是 | 1 | 事件发布 |
| HealthzAgent | 否 | — | — |
| JobAgent | 否 | — | — |
| LogAgent | 否 | — | — |
| LogAgentV1Grpc | 否 | — | — |

### 1.2 端口号总览

| 端口变量 | 默认值 | 环境变量覆盖 | 说明 |
|---------|--------|-------------|------|
| `grpc_port` | 由 raylet 传入，可为 0（动态分配） | — | gRPC server 绑定端口 |
| `listen_port` | **52365** | — | HTTP server 绑定端口（`DEFAULT_DASHBOARD_AGENT_LISTEN_PORT`） |
| `metrics_export_port` | 由 raylet 传入 | — | Prometheus metrics 导出端口（ReporterAgent 内部使用） |

端口持久化文件名：

| 持久化名称 | 含义 |
|-----------|------|
| `METRICS_AGENT_PORT_NAME` | gRPC server 端口 |
| `DASHBOARD_AGENT_LISTEN_PORT_NAME` | HTTP server 端口 |
| `METRICS_EXPORT_PORT_NAME` | Prometheus metrics 导出端口 |

gRPC server 的最大消息长度：`AGENT_GRPC_MAX_MESSAGE_LENGTH = 20MB`（默认，可通过环境变量 `AGENT_GRPC_MAX_MESSAGE_LENGTH` 覆盖）。

---

## 2. 核心文件

| 文件 | 作用 |
|------|------|
| `python/ray/dashboard/agent.py` | DashboardAgent 主类，编排启动流程 |
| `python/ray/dashboard/http_server_agent.py` | HTTP server 实现，端口绑定与重试 |
| `python/ray/dashboard/routes.py` | 路由表基类 `MethodRouteTable`，装饰器注册 + 实例绑定 |
| `python/ray/dashboard/optional_utils.py` | 定义 `DashboardAgentRouteTable` 和 `DashboardHeadRouteTable` |
| `python/ray/dashboard/modules/reporter/reporter_agent.py` | ReporterAgent，gRPC 服务 |
| `python/ray/dashboard/modules/reporter/healthz_agent.py` | HealthzAgent，HTTP 健康检查路由 |
| `python/ray/dashboard/modules/job/job_agent.py` | JobAgent，HTTP 作业管理路由 |

---

## 3. agent.py 启动全流程

### 3.1 `__init__` 阶段

```
DashboardAgent.__init__()
  ├─ minimal=True  → server=None, http_server=None（无功能）
  │    └─ persist_port 写 -1（METRICS_AGENT_PORT, METRICS_EXPORT_PORT）
  └─ minimal=False → _init_non_minimal()
       ├─ 创建 gRPC server (grpc.aio.server)
       │    ├─ 可选：添加 AsyncAuthenticationServerInterceptor（token auth）
       │    ├─ options: grpc.so_reuseport=0, max_send/recv_message_length=20MB
       │    ├─ add_port_to_grpc_server(server, ip:grpc_port)  ← 返回实际端口
       │    ├─ 如果 ip 不是 localhost，额外绑定 127.0.0.1:grpc_port
       │    └─ persist_port(METRICS_AGENT_PORT_NAME, grpc_port)
       └─ 创建 HttpServerAgent(ip, listen_port)  ← 默认 listen_port=52365
            └─ 此时只是保存 ip 和 listen_port，还没 start
```

### 3.2 `run()` 阶段

```
DashboardAgent.run()
  ├─ ① await self.server.start()           ← gRPC server 先启动
  ├─ ② modules = self._load_modules()     ← 动态加载所有 DashboardAgentModule 子类
  ├─ ③ await self.http_server.start(modules)  ← HTTP server 启动 + 路由绑定
  │    └─ 失败则 launch_http_server=False，persist -1
  ├─ ④ persist_port(AGENT_LISTEN_PORT)     ← 写端口文件（-1 表示不可用）
  ├─ ⑤ 如果 launch_http_server → 写 GCS KV
  │    ├─ DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX<node_id> → [ip, http_port, grpc_port]
  │    └─ DASHBOARD_AGENT_ADDR_IP_PREFIX<ip> → [node_id, http_port, grpc_port]
  ├─ ⑥ tasks = [m.run(self.server) for m in modules]  ← 所有模块并发运行
  ├─ ⑦ check_parent_task（监控 raylet 是否存活）
  ├─ ⑧ await asyncio.gather(*tasks)       ← 阻塞主循环
  └─ ⑨ cleanup http_server
```

**关键点**：步骤⑥**不受 HTTP server 失败影响**，即使 `launch_http_server=False`，所有模块的 `run()` 仍会执行。

### 3.3 `run()` 代码详解

```python
async def run(self):
    # Step 1: gRPC server 先启动
    if self.server:
        await self.server.start()

    # Step 2: 加载所有 agent 模块
    modules = self._load_modules()

    # Step 3: HTTP server 启动（含路由绑定）
    launch_http_server = True
    if self.http_server:
        try:
            await self.http_server.start(modules)
            self.listen_port = self.http_server.http_port
        except Exception as e:
            # 异常被捕获，agent 不会退出
            logger.exception(
                f"Failed to start HTTP server with exception: {e}. "
                "The agent will stay alive but the HTTP service will be disabled.",
            )
            launch_http_server = False

    # Step 4: 持久化端口（-1 表示不可用）
    persist_port(
        self.session_dir,
        self.node_id,
        DASHBOARD_AGENT_LISTEN_PORT_NAME,
        self.listen_port if self.http_server and launch_http_server else -1,
    )

    # Step 5: 注册 agent 地址到 GCS KV（仅在 HTTP 成功时）
    if launch_http_server:
        http_port = -1 if not self.http_server else self.http_server.http_port
        grpc_port = -1 if not self.server else self.grpc_port
        # ... 写入 GCS KV

    # Step 6: 所有模块并发运行（不受 HTTP 失败影响）
    tasks = [m.run(self.server) for m in modules]

    # Step 7: 监控 raylet 是否存活
    if sys.platform not in ["win32", "cygwin"]:
        check_parent_task = create_check_raylet_task(...)
        tasks.append(check_parent_task)

    # Step 8: 阻塞主循环
    if self.server:
        tasks.append(self.server.wait_for_termination())
    else:
        tasks.append(wait_forever())

    await asyncio.gather(*tasks)

    if self.http_server:
        await self.http_server.cleanup()
```

---

## 4. 模块发现与加载

### 4.1 `_load_modules()` 机制

```python
def _load_modules(self):
    modules = []
    agent_cls_list = dashboard_utils.get_all_modules(
        dashboard_utils.DashboardAgentModule
    )
    for cls in agent_cls_list:
        c = cls(self)         # 实例化，传入 DashboardAgent 引用
        modules.append(c)
    return modules
```

自动扫描所有 `DashboardAgentModule` 子类并实例化。

### 4.2 当前 Agent 模块完整列表

| 模块 | 文件 | HTTP 路由 | gRPC 服务 | is_minimal | 说明 |
|------|------|----------|-----------|------------|------|
| **ReporterAgent** | `modules/reporter/reporter_agent.py` | 无 | ReporterService + MetricsService (条件) | 否 | 节点资源监控、profiling |
| **HealthzAgent** | `modules/reporter/healthz_agent.py` | 有 (2) | 无 | 否 | 健康检查 HTTP 端点 |
| **JobAgent** | `modules/job/job_agent.py` | 有 (5) | 无 | 否 | 作业提交与管理 |
| **EventAgent** | `modules/event/event_agent.py` | 无 | 无 | 否 | 事件采集与上报（HTTP client） |
| **AggregatorAgent** | `modules/aggregator/aggregator_agent.py` | 无 | EventAggregatorService | 否 | 事件聚合与发布 |
| **LogAgent** | `modules/log/log_agent.py:244` | 有 (static) | 无 | 否 | 日志静态文件服务 |
| **LogAgentV1Grpc** | `modules/log/log_agent.py:263` | 无 | LogService | 否 | 日志流式 gRPC 服务 |
| **TestAgent** | `modules/tests/test_agent.py` | 有 (4) | 无 | 否 | 测试路由（默认禁用） |

### 4.2.1 按服务类型分类

**gRPC 服务模块**（在 `run(server)` 中注册 servicer）：

| 模块 | gRPC 服务 | 注册代码 | 条件 |
|------|----------|---------|------|
| ReporterAgent | ReporterService | `reporter_pb2_grpc.add_ReporterServiceServicer_to_server(self, server)` | `server is not None` |
| ReporterAgent | MetricsService (OTLP) | `metrics_service_pb2_grpc.add_MetricsServiceServicer_to_server(self, server)` | `server is not None` 且 `RAY_ENABLE_OPEN_TELEMETRY` |
| LogAgentV1Grpc | LogService | `reporter_pb2_grpc.add_LogServiceServicer_to_server(self, server)` | `server is not None` |
| AggregatorAgent | EventAggregatorService | `events_event_aggregator_service_pb2_grpc.add_EventAggregatorServiceServicer_to_server(self, server)` | `server is not None` |

**HTTP 服务模块**（通过 `@routes` 装饰器注册路由）：

| 模块 | HTTP 路由 | 路由数 |
|------|----------|--------|
| HealthzAgent | `/api/healthz`, `/api/local_raylet_healthz` | 2 |
| JobAgent | `/api/job_agent/jobs/` (POST), `.../stop` (POST), `.../{id}` (DELETE), `.../logs` (GET), `.../logs/tail` (GET) | 5 |
| LogAgent | `/logs` (static file serving) | 1 |
| TestAgent | `/test/http_get_from_agent` (GET), `/test/route_head` (HEAD), `/test/route_post` (POST), `/test/route_patch` (PATCH) | 4 |

**纯异步模块**（无 gRPC 服务、无 HTTP 路由，仅有 `run()` 协程）：

| 模块 | run() 做什么 |
|------|-------------|
| EventAgent | 监控事件目录文件变化，定期 HTTP POST 事件到 Dashboard Head |

**混合模块**（gRPC 服务 + HTTP client）：

| 模块 | gRPC 服务 | 其他 |
|------|----------|------|
| AggregatorAgent | EventAggregatorService（接收事件） | `run()` 中运行 HTTP/GCS publisher 协程发布事件 |

### 4.3 Agent 模块 HTTP 路由详情

**HealthzAgent**:
| 路由 | 方法 | 作用 |
|------|------|------|
| `/api/local_raylet_healthz` | GET | 检查本地 raylet 健康状态 |
| `/api/healthz` | GET | 统一健康检查（raylet + GCS） |

**JobAgent**:
| 路由 | 方法 | 作用 |
|------|------|------|
| `/api/job_agent/jobs/` | POST | 提交作业 |
| `/api/job_agent/jobs/{id}/stop` | POST | 停止作业 |
| `/api/job_agent/jobs/{id}` | DELETE | 删除作业 |
| `/api/job_agent/jobs/{id}/logs` | GET | 获取作业日志 |
| `/api/job_agent/jobs/{id}/logs/tail` | GET | 实时尾部日志（WebSocket） |

**LogAgent** (static):
| 路由 | 方法 | 作用 |
|------|------|------|
| `/logs` | GET (static) | 静态文件服务，映射 log 目录（`show_index=True`） |

**TestAgent** (默认禁用):
| 路由 | 方法 | 作用 |
|------|------|------|
| `/test/http_get_from_agent` | GET | 测试路由 |
| `/test/route_head` | HEAD | 测试路由 |
| `/test/route_post` | POST | 测试路由 |
| `/test/route_patch` | PATCH | 测试路由 |

---

## 5. HTTP 路由绑定机制（核心）

### 5.1 两个独立的 RouteTable

```python
# optional_utils.py
DashboardHeadRouteTable = method_route_table_factory()    # head 节点用
DashboardAgentRouteTable = method_route_table_factory()   # agent 节点用
```

每次调用 `method_route_table_factory()` 创建一个**全新的** `MethodRouteTable` 类，各自持有独立的 `_bind_map`（dict）和 `_routes`（aiohttp RouteTableDef）。

### 5.2 `MethodRouteTable` 内部数据结构

```python
class MethodRouteTable(BaseRouteTable):
    _bind_map = collections.defaultdict(dict)   # {method: {path: _BindInfo}}
    _routes = aiohttp.web.RouteTableDef()       # aiohttp 路由定义列表

    class _BindInfo:
        def __init__(self, filename, lineno, instance):
            self.filename = filename    # 装饰器所在文件
            self.lineno = lineno        # 装饰器所在行号
            self.instance = instance    # 模块实例（初始为 None）
```

### 5.3 装饰器注册阶段（类定义时）

模块中用 `@routes.get("/api/healthz")` 装饰方法时，`_register_route` 执行：

```python
def _register_route(cls, method, path, **kwargs):
    def _wrapper(handler):
        # 检查路径是否重复
        if path in cls._bind_map[method]:
            raise Exception(f"Duplicated route path: {path}")

        # 创建 BindInfo，instance=None
        bind_info = cls._BindInfo(
            handler.__code__.co_filename,
            handler.__code__.co_firstlineno,
            None                        # ← 此时 instance=None
        )

        # 包装 handler，运行时查 bind_info.instance
        @functools.wraps(handler)
        async def _handler_route(*args):
            req = args[-1]
            return await handler(bind_info.instance, req)  # ← 运行时注入实例

        # 标记路由元信息
        _handler_route.__route_method__ = method
        _handler_route.__route_path__ = path

        # 存入全局 map 和路由表
        cls._bind_map[method][path] = bind_info
        return cls._routes.route(method, path, **kwargs)(_handler_route)

    return _wrapper
```

**此时**：路由已注册到 `_routes`，但 `bind_info.instance = None`，请求进来时无法找到处理对象。

### 5.4 `bind(instance)` 绑定阶段（运行时）

在 `http_server.start(modules)` 中：

```python
for c in modules:
    dashboard_optional_utils.DashboardAgentRouteTable.bind(c)
```

`bind()` 的实现：

```python
@classmethod
def bind(cls, instance):
    # 扫描 instance 上所有带 __route_method__ 和 __route_path__ 属性的方法
    def predicate(o):
        if inspect.ismethod(o):
            return hasattr(o, "__route_method__") and hasattr(o, "__route_path__")
        return False

    handler_routes = inspect.getmembers(instance, predicate)
    for _, h in handler_routes:
        cls._bind_map[h.__func__.__route_method__][
            h.__func__.__route_path__
        ].instance = instance   # ← 把模块实例注入到 bind_info
```

**作用**：把模块实例绑定到之前注册的"空壳"路由上，使请求能路由到正确的方法。

### 5.5 `bound_routes()` 导出阶段

```python
app.add_routes(routes=routes.bound_routes())
```

`bound_routes()` 过滤出所有已绑定 instance 的路由：

```python
@classmethod
def bound_routes(cls):
    bound_items = []
    for r in cls._routes._items:
        if isinstance(r, RouteDef):
            route_method = r.handler.__route_method__
            route_path = r.handler.__route_path__
            instance = cls._bind_map[route_method][route_path].instance
            if instance is not None:   # ← 只返回已绑定的
                bound_items.append(r)
    routes = aiohttp.web.RouteTableDef()
    routes._items = bound_items
    return routes
```

### 5.6 完整路由绑定时序图

```
类定义阶段:
  HealthzAgent.health_check() ──@routes.get──> _bind_map["GET"]["/api/healthz"] = BindInfo(instance=None)
                                           _routes 注册 handler wrapper

运行时 http_server.start(modules):
  for module in modules:
    DashboardAgentRouteTable.bind(module)
      └─ HealthzAgent 实例
           └─ inspect.getmembers() 找到 health_check 方法
                └─ _bind_map["GET"]["/api/healthz"].instance = HealthzAgent实例

  app.add_routes(routes.bound_routes())
    └─ 过滤 instance != None 的路由
         └─ /api/healthz → HealthzAgent.health_check()

请求到达:
  GET /api/healthz
    → _handler_route(req)
      → await handler(bind_info.instance, req)
        → await HealthzAgent.health_check(self, req)
```

---

## 6. HTTP Server 启动流程详解

### 6.1 `http_server_agent.py` — `start()` 方法

```python
async def start(self, modules: List) -> None:
    # 1. 创建 aiohttp ClientSession（所有模块共享）
    self.http_session = aiohttp.ClientSession()

    # 2. 为每个模块绑定路由
    for c in modules:
        dashboard_optional_utils.DashboardAgentRouteTable.bind(c)

    # 3. 创建 aiohttp Application，挂载中间件
    app = aiohttp.web.Application(
        middlewares=[
            get_token_auth_middleware(aiohttp, PUBLIC_EXACT_PATHS),  # Token 认证
            get_browser_request_middleware(aiohttp),                  # 屏蔽浏览器请求
        ]
    )
    app.add_routes(routes=routes.bound_routes())

    # 4. CORS 配置
    cors = aiohttp_cors.setup(app, defaults={"*": ...})
    for route in list(app.router.routes()):
        cors.add(route)

    # 5. 创建 AppRunner 并 setup
    self.runner = aiohttp.web.AppRunner(app)
    await self.runner.setup()

    # 6. 启动 TCP 站点（含重试逻辑）
    site = await self._start_site_with_retry()

    # 7. 读取实际绑定地址
    self.http_host, self.http_port, *_ = site._server.sockets[0].getsockname()
```

### 6.2 中间件说明

| 中间件 | 作用 |
|--------|------|
| `get_token_auth_middleware` | Token 认证（`/api/healthz` 和 `/api/local_raylet_healthz` 免认证） |
| `get_browser_request_middleware` | 屏蔽所有浏览器请求，agent 仅内部访问 |

### 6.3 公开路径（免认证）

```python
PUBLIC_EXACT_PATHS = [
    "/api/healthz",
    "/api/local_raylet_healthz",
]
```

---

## 7. 端口绑定与重试机制

### 7.1 `_start_site_with_retry()` 完整逻辑

```python
async def _start_site_with_retry(
    self, max_retries: int = 5, base_delay: float = 0.1
) -> aiohttp.web.TCPSite:
    last_exception: Optional[OSError] = None

    for attempt in range(max_retries + 1):  # +1 for initial attempt
        try:
            # 绑定主站点
            site = aiohttp.web.TCPSite(
                self.runner,
                self.ip,
                self.listen_port,
            )
            await site.start()

            # 如果非 localhost，额外绑定 127.0.0.1
            if not is_localhost(self.ip):
                local_site = aiohttp.web.TCPSite(
                    self.runner,
                    "127.0.0.1",
                    self.listen_port,
                )
                await local_site.start()

            if attempt > 0:
                logger.info(f"Successfully started agent on port {self.listen_port} "
                            f"after {attempt} retry attempts")
            return site

        except OSError as e:
            last_exception = e
            if attempt < max_retries:
                # 指数退避 + 随机抖动
                delay = base_delay * (2**attempt) + random.uniform(0, 0.1)
                logger.warning(
                    f"Failed to bind to port {self.listen_port} (attempt {attempt + 1}/"
                    f"{max_retries + 1}). Retrying in {delay:.2f}s. Error: {e}"
                )
                await asyncio.sleep(delay)
            else:
                logger.exception(
                    f"Agent port #{self.listen_port} failed to bind after "
                    f"{max_retries + 1} attempts."
                )
                break

    raise last_exception
```

### 7.2 重试时间线

```
尝试 0 (首次): bind(ip:listen_port)
  ├─ 成功 → 如果非 localhost，再 bind 127.0.0.1:listen_port → return site
  └─ 失败 → 等待 base_delay * 2^0 + jitter = 0.1~0.2s

尝试 1: 等待 0.1*2^1 + jitter ≈ 0.2~0.3s 后重试
尝试 2: 等待 0.1*2^2 + jitter ≈ 0.4~0.5s 后重试
尝试 3: 等待 0.1*2^3 + jitter ≈ 0.8~0.9s 后重试
尝试 4: 等待 0.1*2^4 + jitter ≈ 1.6~1.7s 后重试  ← 日志中的 "Retrying in 1.60s"
尝试 5 (最后一次): 等待 0.1*2^5 + jitter ≈ 3.2~3.3s 后重试
  └─ 仍然失败 → raise last_exception（OSError）
```

**总共 6 次尝试**（max_retries=5，+1 首次）。总耗时约 6~7 秒。

### 7.3 双绑定机制

如果 agent IP 不是 localhost，会同时绑定两个 site：
- `ip:listen_port` — 对外通信（其他节点访问）
- `127.0.0.1:listen_port` — 本地回环访问

两个 site 共享同一个 `AppRunner`（同一个 aiohttp Application），端口相同。

### 7.4 动态端口分配

如果 `listen_port=0`，OS 会动态分配端口。绑定成功后通过以下方式获取实际端口：

```python
self.http_host, self.http_port, *_ = site._server.sockets[0].getsockname()
```

---

## 8. gRPC 服务完整清单

DashboardAgent 的 gRPC server 上注册了 4 个 gRPC 服务（来自 3 个模块）：

### 8.1 ReporterService（reporter_agent.py）

**注册条件**：`server is not None`（非 minimal 模式）

```python
# reporter_agent.py:1847
reporter_pb2_grpc.add_ReporterServiceServicer_to_server(self, server)
```

| RPC 方法 | 作用 |
|----------|------|
| `GetTraceback` | 获取 worker Python 堆栈回溯 |
| `CpuProfiling` | CPU profiling |
| `GpuProfiling` | GPU profiling |
| `MemoryProfiling` | 内存 profiling（memray） |
| `HealthCheck` | gRPC 健康检查 |
| `ReportOCMetrics` | OpenCensus 指标上报（worker → agent） |

### 8.2 MetricsService（reporter_agent.py，OpenTelemetry OTLP）

**注册条件**：`server is not None` 且 `RAY_ENABLE_OPEN_TELEMETRY=True`

```python
# reporter_agent.py:1849
if RAY_ENABLE_OPEN_TELEMETRY:
    metrics_service_pb2_grpc.add_MetricsServiceServicer_to_server(self, server)
```

| RPC 方法 | 作用 |
|----------|------|
| `Export` | 接收同节点其他 Ray 组件（raylet、worker 等）的 OpenTelemetry 指标（OTLP 协议） |

### 8.3 LogService（log_agent.py — LogAgentV1Grpc）

**注册条件**：`server is not None`

```python
# log_agent.py:269
reporter_pb2_grpc.add_LogServiceServicer_to_server(self, server)
```

| RPC 方法 | 作用 |
|----------|------|
| `ListLogs` | 列出匹配 glob 过滤器的日志文件 |
| `StreamLog` | 流式读取日志文件内容（支持 start/end 偏移、tail、keep-alive） |

### 8.4 EventAggregatorService（aggregator_agent.py）

**注册条件**：`server is not None`

```python
# aggregator_agent.py:211
events_event_aggregator_service_pb2_grpc.add_EventAggregatorServiceServicer_to_server(
    self, server
)
```

| RPC 方法 | 作用 |
|----------|------|
| `AddEvents` | 接收事件通过 gRPC，放入事件缓冲区供发布 |

### 8.5 gRPC Server 配置

```python
# agent.py:_init_non_minimal()
self.server = aiogrpc.server(
    interceptors=interceptors,    # Token auth interceptor（可选）
    options=(
        ("grpc.so_reuseport", 0),                           # 禁用端口复用
        ("grpc.max_send_message_length", 20 * 1024 * 1024), # 20MB
        ("grpc.max_receive_message_length", 20 * 1024 * 1024),  # 20MB
    ),
)
```

绑定地址：
- 如果 `ip` 是 localhost：绑定 `127.0.0.1:grpc_port`
- 如果 `ip` 不是 localhost：绑定 `ip:grpc_port` 和 `127.0.0.1:grpc_port`（双绑定）

---

## 9. gRPC Server vs HTTP Server 对比

| 维度 | gRPC Server | HTTP Server |
|------|-------------|-------------|
| 类 | `grpc.aio.server` (aiogrpc) | `aiohttp.web.Application` |
| 端口 | `grpc_port`（raylet 传入，可为 0 动态分配） | `listen_port`（默认 **52365**，可为 0 动态分配） |
| 协议 | gRPC (protobuf) | HTTP REST (aiohttp) |
| 启动时机 | `run()` 第一步：`await self.server.start()` | `run()` 第三步：`await self.http_server.start(modules)` |
| 绑定 IP | `ip:grpc_port`，非 localhost 额外绑 `127.0.0.1` | `ip:listen_port`，非 localhost 额外绑 `127.0.0.1` |
| 路由注册 | 模块 `run(server)` 中手动调用 `add_*Servicer_to_server` | `http_server.start(modules)` 中自动 `DashboardAgentRouteTable.bind(c)` |
| 失败处理 | 启动失败 → agent 退出 | 启动失败 → agent 继续，HTTP 禁用（`launch_http_server=False`） |
| 重试机制 | 无（端口冲突直接失败） | 最多 6 次尝试，指数退避 + 随机抖动 |
| 最大消息 | 20MB（`AGENT_GRPC_MAX_MESSAGE_LENGTH`） | 无显式限制（aiohttp 默认 1MB body，可配置） |
| 中间件 | Token auth interceptor（可选） | Token auth middleware + 浏览器请求拦截 middleware |
| 主要用途 | 集群内部通信（raylet/worker → agent） | Dashboard head 代理的 REST API、健康检查、作业管理 |

---

## 10. 端口绑定失败后的完整影响链

```
_start_site_with_retry() raise OSError
  → http_server.start() 异常
  → agent.run() except 捕获
  → launch_http_server = False
  → persist_port 写入 -1
  → 不写 GCS KV（其他节点无法发现此 agent 的 HTTP 端点）
  → 但 modules.run(server) 仍执行 ← gRPC 服务正常
```

### 10.1 受影响的功能

| 功能 | 影响 | 原因 |
|------|------|------|
| `/api/healthz` | 不可用 | HealthzAgent 的 HTTP 路由无法访问 |
| `/api/local_raylet_healthz` | 不可用 | 同上 |
| `/api/job_agent/jobs/...` | 不可用 | JobAgent 的 HTTP 路由无法访问 |
| Dashboard head 代理请求 | 不可达 | agent 地址未写入 GCS KV |
| 外部监控系统（通过 HTTP） | 不可达 | HTTP 端口不可用 |

### 10.2 不受影响的功能

| 功能 | 模块 | 说明 |
|------|------|------|
| ReporterAgent 指标上报 | ReporterAgent | 通过 gRPC + GCS pub/sub，不依赖 HTTP |
| ReporterAgent profiling | ReporterAgent | 通过 gRPC `GetTraceback`/`CpuProfiling`/`GpuProfiling`/`MemoryProfiling` |
| OpenTelemetry 指标收集 | ReporterAgent | 通过 gRPC `MetricsService.Export`（需 `RAY_ENABLE_OPEN_TELEMETRY`） |
| HealthCheck (gRPC) | ReporterAgent | 通过 gRPC `ReporterService.HealthCheck` |
| 日志流式读取 (gRPC) | LogAgentV1Grpc | 通过 gRPC `LogService.StreamLog`/`ListLogs` |
| 事件聚合 (gRPC) | AggregatorAgent | 通过 gRPC `EventAggregatorService.AddEvents` |
| 事件发布 | AggregatorAgent | HTTP client / GCS publisher（主动 push，不监听端口） |
| 节点资源上报 | ReporterAgent | `_run_loop()` 定期上报 GCS pub/sub |

---

## 11. 端口占用排查

### 11.1 常见原因

1. **旧 agent 进程未退出**：raylet 重启后旧 agent 进程仍在占用端口
2. **其他服务占用**：同节点其他进程使用了相同端口
3. **TIME_WAIT 状态**：频繁重启导致端口处于 TIME_WAIT

### 11.2 排查方法

```bash
# 查看端口占用
ss -tlnp | grep <port>
lsof -i :<port>

# 查找 agent 进程
ps aux | grep dashboard_agent
```

### 11.3 解决方案

1. 清理旧 agent 进程
2. 使用 `--listen-port=0` 让 OS 动态分配端口
3. 增大 `max_retries` 和 `base_delay` 参数

---

## 12. 已知 TODO

`agent.py` 中有 TODO 注释指出：当 HTTP server 启动失败时，agent 应该退出而非继续运行，以避免隐藏根因。但当前因为 CI 测试中 agent 进程清理不完整，如果让 agent 退出会导致 CI 始终失败，所以暂时保持容错处理。

```python
# TODO(kevin85421): We should fail the agent if the HTTP server
# fails to start to avoid hiding the root cause. However,
# agent processes are not cleaned up correctly after some tests
# finish. If we fail the agent, the CI will always fail until
# we fix the leak.
```

---

## 13. Head 与 Worker 节点的区分机制

### 13.1 架构总览：两个独立组件

Ray 的 Dashboard 体系由**两个完全独立的组件**构成，分别运行在不同节点上：

| 组件 | 入口文件 | 进程类型 | 路由表 | 运行位置 |
|------|---------|---------|--------|---------|
| **DashboardAgent** | `agent.py` | `PROCESS_TYPE_DASHBOARD_AGENT` | `DashboardAgentRouteTable` | **所有节点**（head + worker），由 raylet 启动 |
| **DashboardHead** | `dashboard.py` | `PROCESS_TYPE_DASHBOARD` | `DashboardHeadRouteTable` | **仅 head 节点**，由 `start_api_server` 启动 |

**关键结论**：DashboardAgent 在 **head 和 worker 节点都会运行**，不区分启动与不启动。区别在于通过 `is_head` 标志让特定模块在 head 节点上启用额外行为。

### 13.2 `is_head` 标志的传递链路

```
Python services.py (start_raylet, is_head_node=True)
  → dashboard_agent_command 包含 "--head"         (services.py:1835-1836)
  → 序列化为 "--dashboard_agent_command=..."       (services.py:1939)
  → 传递给 raylet 进程
  → raylet main.cc 解析 FLAGS_dashboard_agent_command (main.cc:254)
  → NodeManager::CreateDashboardAgentManager      (node_manager.cc:3289)
  → AgentManager::StartAgent 启动 Python 进程      (agent_manager.cc:26)
  → agent.py 解析 --head 参数                      (agent.py:469)
  → DashboardAgent(is_head=True)                   (agent.py:498)
  → 子模块通过 dashboard_agent.is_head 访问        (agent.py:87)
```

**agent.py 中的关键代码**：

```python
# agent.py:469-472
parser.add_argument(
    "--head", action="store_true",
    help="Whether this node is the head node."
)

# agent.py:87
self.is_head = is_head

# agent.py:498
is_head=args.head,
```

**services.py 中的条件追加**：

```python
# services.py:1835-1836
if is_head_node:
    dashboard_agent_command.append("--head")
```

**关键**：raylet 的 `AgentManager` **不检查也不修改**命令参数，它只是将收到的命令字符串原样启动为 Python 进程。`--head` 标志的存在与否完全由 Python `services.py` 在构建命令时决定。

### 13.3 两个独立的 RouteTable

**文件**: `python/ray/dashboard/optional_utils.py:44-45`

```python
DashboardHeadRouteTable = method_route_table_factory()    # head 节点用
DashboardAgentRouteTable = method_route_table_factory()   # agent 节点用（所有节点）
```

每次调用 `method_route_table_factory()` 创建一个**全新的** `MethodRouteTable` 类，各自持有独立的 `_bind_map` 和 `_routes`。

**DashboardAgent 只使用 `DashboardAgentRouteTable`**，从不使用 `DashboardHeadRouteTable`：

| 路由表 | 使用者 | 绑定位置 |
|--------|--------|---------|
| `DashboardAgentRouteTable` | DashboardAgent（所有节点） | `http_server_agent.py:105` |
| `DashboardHeadRouteTable` | DashboardHead（仅 head 节点） | `http_server_head.py:120` |

使用 `DashboardAgentRouteTable` 注册路由的模块文件：
- `http_server_agent.py`（绑定入口）
- `modules/reporter/healthz_agent.py:12`
- `modules/job/job_agent.py:23`
- `modules/log/log_agent.py:19`

使用 `DashboardHeadRouteTable` 注册路由的模块文件（agent 从不触碰）：
- `http_server_head.py`
- `modules/usage_stats/usage_stats_head.py`

### 13.4 DashboardHead — 仅 head 节点的独立进程

DashboardHead 是与 DashboardAgent **完全分离的进程**，只在 head 节点运行：

| 文件 | 类/作用 |
|------|---------|
| `dashboard.py` | `Dashboard` 类 — 入口进程，启动 DashboardHead |
| `head.py` | `DashboardHead` 类 — head 端编排器 |
| `http_server_head.py` | `HttpServerDashboardHead` — head 端 HTTP server，绑定 `DashboardHeadRouteTable` |
| `utils.py:108` | `DashboardHeadModule` — head 端模块基类（与 `DashboardAgentModule` at line 63 区分） |

**证明 DashboardHead 只在 head 节点运行**：

`python/ray/_private/node.py:1309-1327`：
```python
def start_head_processes(self):
    # ...
    self.start_api_server()  # 只在 head 节点调用，启动 dashboard.py
```

`start_worker_processes` 中**不调用** `start_api_server`，不启动 `dashboard.py`。

### 13.5 各 Agent 模块对 `is_head` 的使用情况

并非所有模块都区分 head/worker。只有 **ReporterAgent** 和 **HealthzAgent** 使用 `is_head` 标志：

| 模块 | 是否使用 `is_head` | head 特有行为 |
|------|-------------------|--------------|
| **ReporterAgent** | 是 | 采集 GCS 进程 stats、上报集群级 autoscaler 指标、从 internal KV 获取 autoscaler 状态和 GCS PID |
| **HealthzAgent** | 是 | GCS 健康检查（worker 节点跳过，因为 GCS 不在本地） |
| JobAgent | 否 | 无 |
| LogAgent | 否 | 无 |
| EventAgent | 否 | 无 |
| AggregatorAgent | 否 | 无 |
| LogAgentV1Grpc | 否 | 无 |
| TestAgent | 否 | 无 |

### 13.6 ReporterAgent 的 head 特有行为详解

**文件**: `python/ray/dashboard/modules/reporter/reporter_agent.py`

```python
# Line 427
self._is_head_node = dashboard_agent.is_head
```

**① 只在 head 节点采集 GCS 进程 stats**：

```python
# Line 1139
if self._is_head_node:
    stats["gcs"] = self._get_gcs()
```

GCS 进程只在 head 节点运行，worker 节点上不存在 GCS，因此跳过。

**② 指标标记节点类型**：

```python
# Lines 1385-1386
ray_node_type = "head" if self._is_head_node else "worker"
is_head_node = "true" if self._is_head_node else "false"
```

上报的 metrics 中标记当前节点是 head 还是 worker。

**③ 只在 head 节点上报集群级 autoscaler 统计**：

```python
# Line 1395
if "autoscaler_report" in cluster_stats and self._is_head_node:
    # active nodes, failed nodes 等集群级指标
```

避免每个 worker 都重复上报集群级指标。

**④ 只在 head 节点生成 GCS system metric records**：

```python
# Lines 1681-1686
if self._is_head_node:
    gcs_stats = stats["gcs"]
    # 生成 GCS 相关的 system metric records
```

**⑤ 只在 head 节点获取 autoscaler debug 状态和 GCS PID**：

```python
# Lines 1749-1760 (在 _run_loop 中)
if self._is_head_node:
    # 从 internal KV 获取 autoscaler debug status
    # 从 internal KV 获取 GCS PID
```

### 13.7 HealthzAgent 的 head 特有行为

**文件**: `python/ray/dashboard/modules/reporter/healthz_agent.py`

```python
# Line 69
def local_gcs_health(self):
    if not self._dashboard_agent.is_head:
        return "success (no local gcs)"
    # 只有 head 节点才真正检查 GCS 健康状态
    # worker 节点直接返回成功（因为本地没有 GCS）
```

**逻辑**：GCS 只在 head 节点运行。worker 节点的健康检查不需要检查 GCS liveness，直接返回 `"success (no local gcs)"`。只有 head 节点才真正发起 GCS 健康检查。

### 13.8 完整组件对比图

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Head 节点                                    │
│                                                                     │
│  raylet ──→ DashboardAgent (agent.py)                                │
│              ├── is_head = True                                      │
│              ├── DashboardAgentRouteTable (HTTP)                     │
│              ├── gRPC server                                        │
│              ├── ReporterAgent: 采集 GCS stats + autoscaler 指标      │
│              ├── HealthzAgent: 检查 GCS 健康                          │
│              ├── JobAgent / LogAgent / EventAgent / ...              │
│              └── (与 worker 节点相同的模块)                              │
│                                                                     │
│  start_api_server ──→ DashboardHead (dashboard.py)                  │
│              ├── DashboardHeadRouteTable (HTTP)                      │
│              ├── Web UI 服务                                          │
│              ├── 集群级 API 端点                                       │
│              └── usage_stats_head 等模块                               │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                        Worker 节点                                   │
│                                                                     │
│  raylet ──→ DashboardAgent (agent.py)                                │
│              ├── is_head = False                                     │
│              ├── DashboardAgentRouteTable (HTTP)                     │
│              ├── gRPC server                                        │
│              ├── ReporterAgent: 不采集 GCS stats，不报 autoscaler       │
│              ├── HealthzAgent: 跳过 GCS 检查，返回 "no local gcs"      │
│              ├── JobAgent / LogAgent / EventAgent / ...              │
│              └── (与 head 节点相同的模块)                               │
│                                                                     │
│  (无 DashboardHead 进程)                                              │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 13.9 关键结论总结

1. **DashboardAgent 在所有节点都运行**，不区分 head/worker 是否启动
2. **`is_head` 是一个布尔标志**，通过 `--head` CLI 参数传入，让子模块条件性地启用 head 特有行为
3. **DashboardHead 是独立进程**，只在 head 节点运行，使用独立的 `DashboardHeadRouteTable`
4. **两套路由表完全隔离**：`DashboardAgentRouteTable`（agent 用）vs `DashboardHeadRouteTable`（head 用）
5. **只有 ReporterAgent 和 HealthzAgent 使用 `is_head`**，其他模块行为在 head 和 worker 节点完全一致
6. **head 特有行为的本质**：GCS 进程只在 head 节点运行，因此 GCS stats 采集、GCS 健康检查、集群级 autoscaler 指标上报只在 head 节点执行

---

## 14. gRPC Server 被谁连接 — 完整图景

Agent 的 gRPC server 上注册了 4 个 service，**两类完全不同的 client** 来连：

### 14.1 同节点 C++ 进程 → 连 `127.0.0.1:grpc_port`（推模式）

这些是 Agent 的**本地邻居**，通过端口文件发现地址，连 `127.0.0.1`：

```
同节点 C++ 进程                          Agent gRPC server (127.0.0.1:grpc_port)
                                        注册的 4 个 service
┌──────────────────┐                    ┌─────────────────────────────┐
│ Raylet           │ ──gRPC──→ ReporterService.ReportOCMetrics        │
│ (main.cc:1040)   │            推 OpenCensus 指标                      │
│                  │            + HealthCheck（探活）                  │
├──────────────────┤                    │                             │
│ GCS Server       │ ──gRPC──→ ReporterService.ReportOCMetrics        │
│ (gcs_server.cc   │            推 OpenCensus 指标                      │
│  :945)           │ ──gRPC──→ EventAggregatorService.AddEvents        │
│                  │            推事件                                  │
├──────────────────┤                    │                             │
│ CoreWorker       │ ──gRPC──→ ReporterService.ReportOCMetrics        │
│ (core_worker_     │            推 OpenCensus 指标                      │
│  process.cc:859) │            + HealthCheck（探活）                  │
│                  │ ──gRPC──→ EventAggregatorService.AddEvents        │
│                  │            推 task 事件                             │
├──────────────────┤                    │                             │
│ OpenCensus       │ ──gRPC──→ ReporterService.ReportOCMetrics        │
│ Exporter         │            推指标（stats 定时调用）                  │
│ (metric_exporter │                                                 │
│  .cc:64)         │                                                 │
├──────────────────┤                    │                             │
│ OpenTelemetry    │ ──gRPC──→ MetricsService.Export (OTLP)           │
│ Exporter         │            推 OTLP 指标                            │
│ (stats.h:122)    │                                                 │
├──────────────────┤                    │                             │
│ RayEventRecorder │ ──gRPC──→ EventAggregatorService.AddEvents        │
│ (ray_event_      │            推事件（定时批量）                       │
│  recorder.cc)    │                                                 │
└──────────────────┘                    └─────────────────────────────┘
```

**所有 C++ client 连的都是 `127.0.0.1`**——因为是同节点进程，不需要走网络。

**端口发现方式**：
```
Agent 启动 → 绑定 grpc_port → 写端口文件 metrics_agent_port
                                      ↓
Raylet 构造时 → WaitForPersistedPort() 读端口文件 → 得到 grpc_port
                                      ↓
Raylet 启动时 → self_node_info.set_metrics_agent_port(grpc_port) → 广播到 GCS
                                      ↓
GCS Server → 从 node info 读到 grpc_port → 连 127.0.0.1:grpc_port
                                      ↓
Raylet 创建 Worker → CoreWorkerOptions.metrics_agent_port = grpc_port
                                      ↓
CoreWorker → 连 127.0.0.1:grpc_port
```

### 14.2 Dashboard Head Python 进程 → 连 `<agent_ip>:<grpc_port>`（拉模式）

Head 在另一个节点上（通常是 head 节点），通过网络连 Agent 的 gRPC server：

```
Dashboard Head (head 节点)               Agent (worker 节点)
                                        gRPC server (agent_ip:grpc_port)
┌──────────────────┐                    ┌─────────────────────────────┐
│ ReporterHead     │ ──gRPC──→ ReporterService.GetTraceback           │
│ (reporter_head   │            拉某 worker 的 Python 堆栈              │
│  .py:257)        │                                                 │
│                  │ ──gRPC──→ ReporterService.CpuProfiling           │
│                  │            拉某进程的 CPU profile                  │
│                  │                                                 │
│                  │ ──gRPC──→ ReporterService.GpuProfiling           │
│                  │            拉某进程的 GPU profile                  │
│                  │                                                 │
│                  │ ──gRPC──→ ReporterService.MemoryProfiling         │
│                  │            拉某进程的内存 profile                   │
├──────────────────┤                    │                             │
│ State API        │ ──gRPC──→ LogService.ListLogs                   │
│ (state_manager   │            拉日志文件列表                           │
│  .py:172)        │                                                 │
│                  │ ──gRPC──→ LogService.StreamLog                  │
│                  │            拉日志流（stream RPC，持续读取）         │
└──────────────────┘                    └─────────────────────────────┘
```

**端口发现方式**：
```
Agent 启动 → 写 GCS KV:
  DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX<node_id> → [ip, http_port, grpc_port]
  DASHBOARD_AGENT_ADDR_IP_PREFIX<ip>           → [node_id, http_port, grpc_port]
                                      ↓
ReporterHead._get_stub_address_by_node_id():
  → gcs_client.async_internal_kv_get(key) → json.loads → [ip, http_port, grpc_port]
  → init_grpc_channel(f"{ip}:{grpc_port}")
  → ReporterServiceStub(channel)
  → stub.CpuProfiling(request)  ← gRPC 调用到远端 Agent
```

### 14.3 两种 client 的本质区别

| 维度 | C++ 本地进程 | Dashboard Head (Python) |
|------|-------------|------------------------|
| 谁是 client | Raylet、GCS、CoreWorker | ReporterHead、State API |
| 连接目标 | `127.0.0.1:grpc_port` | `<agent_ip>:grpc_port` |
| 网络方式 | 本机回环（不走物理网卡） | 跨节点网络 |
| 数据方向 | **推**（worker → agent） | **拉**（head → agent） |
| 调用的 service | ReporterService (ReportOCMetrics)、EventAggregatorService (AddEvents)、MetricsService (Export) | ReporterService (GetTraceback/CpuProfiling/GpuProfiling/MemoryProfiling)、LogService (ListLogs/StreamLog) |
| 触发时机 | 定时/事件驱动（自动） | 用户在 Dashboard UI 点击操作 |
| gRPC stub 类型 | C++ `GrpcClient<ReporterService>` | Python `ReporterServiceStub` |
| 地址发现 | 端口文件 → raylet → 广播 GCS → 传递给 worker | GCS KV 查询 |

---

## 15. HTTP Server 被谁连接 — 完整图景

### 15.1 Dashboard Job Head → 连 Agent HTTP（拉/转发模式）

```
用户在 Dashboard UI 提交作业
  │
  ▼
Dashboard Head (head 节点, :8265)
  JobHead 收到 HTTP 请求 POST /api/jobs/
  │
  ├─ 查 GCS KV 找到目标节点的 agent 地址
  │  key = DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX<node_id>
  │  value = [ip, http_port, grpc_port]
  │
  ├─ 创建 JobAgentSubmissionClient(f"http://{ip}:{http_port}")
  │
  └─ HTTP 转发到 Agent:
     POST http://<agent_ip>:52365/api/job_agent/jobs/
       │
       ▼
     Agent (worker 节点)
     JobAgent.health_check() 处理请求
       → 在本节点启动 Ray job
```

**完整路由**：

| Head 收到 | Head 转发到 Agent | 用途 |
|-----------|-------------------|------|
| `POST /api/jobs/` | `POST /api/job_agent/jobs/` | 提交作业到指定节点 |
| `POST /api/jobs/{id}/stop` | `POST /api/job_agent/jobs/{id}/stop` | 停止作业 |
| `DELETE /api/jobs/{id}` | `DELETE /api/job_agent/jobs/{id}` | 删除作业 |
| `GET /api/jobs/{id}/logs` | `GET /api/job_agent/jobs/{id}/logs` | 获取作业日志 |
| `GET /api/jobs/{id}/logs/tail` | `WS /api/job_agent/jobs/{id}/logs/tail` | 实时日志（WebSocket） |

### 15.2 K8s/外部 → 连 Agent HTTP（拉模式）

```
K8s liveness probe
  → HTTP GET http://<agent_ip>:52365/api/healthz
  → HealthzAgent 检查 raylet + GCS 存活
  → 200 OK / 503 Service Unavailable
```

### 15.3 Head 节点不是直接"拉"Agent 的指标数据

**重要区分**：Dashboard 看到的节点物理指标（CPU/GPU/内存等）**不是** Head 通过 HTTP 从 Agent 拉的，而是 Agent 主动推到 GCS pub/sub 的。

HTTP 拉的只有两类东西：
1. **作业管理**（JobHead 转发到 JobAgent）
2. **健康检查**（`/api/healthz`）

节点物理指标的数据流是：

```
Agent ReporterAgent._run_loop() (每 5 秒)
  → 采集 stats
  → gcs_client.async_publish_node_resource_usage()  ← 推到 GCS pub/sub
  → NodeHead._update_node_physical_stats()            ← Head 订阅，被动收
  → DataSource.node_physical_stats[node_id]
  → 前端 GET /nodes?view=summary → 从 DataSource 读
```

所以指标不走 HTTP，走 GCS pub/sub。HTTP 只用于作业管理和健康检查。

---

## 16. 完整连接拓扑总结

```
                    ┌─────────────────────────────────────────────┐
                    │            Agent gRPC server                 │
                    │            (ip:grpc_port)                    │
                    │                                             │
                    │  ReporterService:                            │
                    │    ReportOCMetrics ← C++ 进程推指标            │
                    │    HealthCheck     ← C++ 进程探活              │
                    │    GetTraceback   ← Head 拉（用户操作）         │
                    │    CpuProfiling   ← Head 拉（用户操作）         │
                    │    GpuProfiling   ← Head 拉（用户操作）         │
                    │    MemoryProfiling← Head 拉（用户操作）         │
                    │                                             │
                    │  MetricsService:                             │
                    │    Export         ← C++ 进程推 OTLP 指标       │
                    │                                             │
                    │  LogService:                                 │
                    │    ListLogs       ← Head 拉（日志列表）          │
                    │    StreamLog      ← Head 拉（日志流）           │
                    │                                             │
                    │  EventAggregatorService:                     │
                    │    AddEvents      ← C++ 进程推事件             │
                    └─────────────────────────────────────────────┘

                    ┌─────────────────────────────────────────────┐
                    │            Agent HTTP server                 │
                    │            (ip:52365)                       │
                    │                                             │
                    │  /api/healthz              ← K8s/Head 健康检查 │
                    │  /api/local_raylet_healthz ← K8s 健康检查     │
                    │  /api/job_agent/jobs/...   ← Head 转发作业操作 │
                    │  /logs/...                 ← Head 转发日志浏览 │
                    └─────────────────────────────────────────────┘

                    ┌─────────────────────────────────────────────┐
                    │       Agent 主动发起的出站连接 (Client)         │
                    │                                             │
                    │  GcsClient → GCS server (ip:6379)            │
                    │    推: async_publish_node_resource_usage     │
                    │    拉: async_internal_kv_get                │
                    │                                             │
                    │  RayletClient → raylet (ip:node_mgr_port)    │
                    │    拉: async_get_worker_pids                │
                    │                                             │
                    │  aiohttp.ClientSession → Head (ip:8265)      │
                    │    推: EventAgent POST /report_events        │
                    │                                             │
                    │  OpenTelemetry → OTLP endpoint               │
                    │    推: record_and_export                    │
                    └─────────────────────────────────────────────┘
```

### 16.1 "推"和"拉"的精确定义

| 描述 | 含义 | 具体行为 |
|------|------|---------|
| Worker 推数据 | Worker 主动发 gRPC 请求到 Agent，请求体携带指标/事件数据 | `stub.ReportOCMetrics(request)` — request 里有指标数据，Agent 收到后存起来 |
| Head 拉数据 | Head 主动发 gRPC 请求到 Agent，请求体只有查询参数，Agent 返回 profiling/日志数据 | `stub.CpuProfiling(request)` — request 里只有 pid，reply 里返回 profile 结果 |
| Head HTTP 拉 | Head 主动发 HTTP 请求到 Agent，转发用户的作业管理操作 | `POST /api/job_agent/jobs/` — Head 发请求到 Agent，Agent 执行并返回结果 |

**"推"和"拉"都是 client 主动发起请求**——区别在于：
- **推**：请求体携带数据，server 接收后存储/转发
- **拉**：请求体携带查询参数，server 返回数据

本质上都是 gRPC/HTTP 的 request-response 模型，只是数据流向不同。

---

## 17. asyncio task 模式与 minimal 模式详解

### 17.1 asyncio task 模式

`asyncio.gather(*tasks)` 启动的是**协程并发**模式，所有协程跑在**同一个线程、同一个事件循环**里。

```
DashboardAgent 进程（单线程）
  └─ 事件循环（asyncio）
       ├─ task1: ReporterAgent.run()     ← while True: collect stats + sleep 5s
       ├─ task2: HealthzAgent.run()       ← pass（空协程，立即完成）
       ├─ task3: JobAgent.run()           ← pass
       ├─ task4: EventAgent.run()         ← while True: monitor files + report
       ├─ task5: AggregatorAgent.run()    ← while True: publish events
       ├─ task6: LogAgentV1Grpc.run()     ← pass（gRPC 注册完就结束）
       ├─ task7: check_raylet_task        ← while True: 检查 raylet 存活
       └─ task8: server.wait_for_termination()  ← 阻塞，让 gRPC server 不退出
```

- **不是多线程**：没有 `threading.Thread`，没有 GIL 切换开销
- **不是多进程**：没有 `multiprocessing`，共享同一份内存
- **是协作式调度**：每个 `await` 点是让出 CPU 的机会，单核上通过事件循环交替执行
- **CPU 密集操作**会阻塞整个循环，所以 ReporterAgent 等用 `ThreadPoolExecutor` 把重活扔到线程池

本质就是：**一个进程、一个线程、一个事件循环，多个协程交替执行**。

### 17.2 minimal 模式

`minimal` 表示**最小化安装**模式，对应 `pip install ray`（不含 `[default]`）。

```
pip install ray              → minimal=True  （只有核心调度，无 dashboard 依赖）
pip install "ray[default]"   → minimal=False （完整 dashboard，含 aiohttp 等）
```

minimal 模式下：
- `self.server = None`（不创建 gRPC server）
- `self.http_server = None`（不创建 HTTP server）
- 端口文件写 `-1` 表示服务不可用
- `_load_modules()` 只加载 `is_minimal_module() == True` 的模块

当前所有 8 个模块的 `is_minimal_module()` 都返回 `False`，所以 **minimal 模式下实际上不加载任何业务模块**，agent 进程仅作为占位存活。

### 17.3 端口号配置参数

| 参数 | 默认值 | 来源 | 说明 |
|------|--------|------|------|
| `--grpc-port` | 由 raylet 传入 | raylet 启动 agent 时指定 | 传 0 = OS 动态分配 |
| `--listen-port` | **52365** | `DEFAULT_DASHBOARD_AGENT_LISTEN_PORT` | 传 0 = OS 动态分配 |
| `--metrics-export-port` | 由 raylet 传入 | raylet 启动 agent 时指定 | ReporterAgent 内部使用 |

**端口发现机制**：

```
<session_dir>/ports/<node_id>/
  ├── metrics_agent_port      ← gRPC 端口（其他进程读此文件发现 agent gRPC 地址）
  ├── dashboard_agent_listen   ← HTTP 端口
  └── metrics_export_port      ← Prometheus 端口
```

### 17.4 怎么区分模块是什么服务

看两样东西就够：

**① 看 `run(server)` 方法里有没有调 `add_*Servicer_to_server`**（有 = gRPC 服务）

```python
# 有 gRPC 服务的模块
async def run(self, server):
    if server:
        xxx_pb2_grpc.add_XXXServicer_to_server(self, server)  # ← 有这行 = gRPC
```

- ReporterAgent → `add_ReporterServiceServicer_to_server` + 条件 `add_MetricsServiceServicer_to_server`
- LogAgentV1Grpc → `add_LogServiceServicer_to_server`
- AggregatorAgent → `add_EventAggregatorServiceServicer_to_server`

**② 看模块里有没有用 `@routes.get/post/...` 装饰器**（有 = HTTP 服务）

```python
# 有 HTTP 路由的模块
routes = dashboard_optional_utils.DashboardAgentRouteTable

@routes.get("/api/healthz")        # ← 有这种装饰器 = HTTP
async def health_check(self, req):
    ...
```

- HealthzAgent → 2 个 `@routes.get`
- JobAgent → 5 个 `@routes.post/get/delete`
- LogAgent → `routes.static("/logs", ...)`（静态文件，在 `__init__` 中注册）
- TestAgent → 4 个 `@routes.*`

**③ 两者都没有 = 纯异步协程**

- EventAgent → `run()` 里只有 `asyncio.gather(self.report_events())`，通过 HTTP client 主动 POST 事件到 Dashboard Head

当前没有模块同时提供 gRPC + HTTP 两种服务。最接近的 AggregatorAgent 是 gRPC server + HTTP **client**（主动 POST，不监听路由）。

---

## 18. Raylet ↔ DashboardAgent 的完整关系

### 18.1 启动流程（raylet 侧）

在 `NodeManager` 构造函数中（`node_manager.cc:274-285`），按以下顺序执行：

```
NodeManager 构造函数:
  ① node_manager_server_.Run()              ← 先启动 raylet 自己的 gRPC server
  ② dashboard_agent_manager_ = CreateDashboardAgentManager()
       └─ AgentManager::StartAgent()
            ├─ fork 出 Python 子进程: python -m ray.dashboard.agent ...
            ├─ 注入环境变量: RAY_NODE_ID, RAY_RAYLET_PID
            ├─ pipe_to_stdin=True（用于 agent 检测 raylet 死亡）
            └─ 启动 monitor_thread_（阻塞等待 agent 进程退出）
  ③ runtime_env_agent_manager_ = CreateRuntimeEnvAgentManager()  ← 同样 fork
  ④ WaitForDashboardAgentPorts()   ← 阻塞！最多等 15 秒/端口
       ├─ 等 metrics_agent_port（gRPC 端口文件出现）
       ├─ 等 metrics_export_port（Prometheus 端口文件出现）
       └─ 等 dashboard_agent_listen_port（HTTP 端口文件出现）
  ⑤ WaitForRuntimeEnvAgentPort()   ← 同样阻塞等待
  ⑥ worker_pool_.Start()           ← 等到所有 agent 就绪后，才启动 worker pool
```

**关键**：步骤④会阻塞 raylet 构造函数。如果 agent 在 15 秒内没写端口文件，`WaitForPersistedPort` 返回 `TimedOut`，`RAY_ASSIGN_OR_CHECK_SET` 宏触发 FATAL，**raylet 直接崩溃**。

### 18.2 端口文件机制（agent 侧）

Python 侧 `agent.py` 绑定端口后，立即写文件：

```
<session_dir>/ports/<node_id>/
  ├── metrics_agent_port        ← gRPC 端口（或 -1，表示 minimal）
  ├── dashboard_agent_listen    ← HTTP 端口（或 -1，表示失败）
  └── metrics_export_port       ← Prometheus 端口（或 -1）
```

raylet 的 `WaitForPersistedPort` 每 50ms 轮询这些文件，读到有效数字就继续。读到 `-1` 也算成功（表示服务不可用但 agent 存活）。

### 18.3 双向命运共享（fate-sharing）

核心设计——**raylet 和 agent 互为生死**：

```
方向 1：Agent 死 → Raylet 死
─────────────────────────
agent_manager.cc: monitor_thread_
  └─ process_.Wait() 返回（agent 退出/crash）
      └─ fate_shares_ == true?
           ├─ Yes → shutdown_raylet_gracefully_()
           │         + 10 秒后 QuickExit() 兜底
           └─ No  → 仅记日志，raylet 继续

方向 2：Raylet 死 → Agent 死
─────────────────────────
agent.py:run() → create_check_raylet_task()
  └─ 两种检测方式:
       ├─ Pipe 模式（默认）:
       │    sys.stdin.readline() 返回 0 字节
       │    = raylet 死了（pipe 断开）
       │    → report_raylet_error_logs()
       │    → sys.exit(0)
       │
       └─ PID 模式:
            psutil.Process().parent() 每 0.4 秒检查一次
            连续 5 次（~2 秒）父进程不存在
            → sys.exit(0)
```

### 18.4 Agent 出问题时的场景分析

#### 场景 1：Agent gRPC 端口绑定失败（端口冲突）

```
agent.py:_init_non_minimal()
  └─ add_port_to_grpc_server() 失败 → 异常
      └─ agent.py:__main__ 的 except 捕获
           └─ logger.exception("Agent is working abnormally...")
           └─ exit(1)  ← agent 进程退出

→ monitor_thread_ 检测到 exit code = 1
→ fate_shares_ = true
→ shutdown_raylet_gracefully()
→ raylet 在 10 秒内退出
→ GCS 标记节点 DEAD
→ 外部 supervisor（K8s/k8s_utils）重启整个节点
```

**结果**：raylet 死亡，worker 全部死亡，节点下线。但不会有作业丢失——GCS 会在其他节点重新调度 actor/task。

#### 场景 2：Agent HTTP 端口绑定失败（端口 52365 冲突）

```
agent.py:run()
  └─ await self.http_server.start(modules) → OSError
       └─ except 捕获 → launch_http_server = False
            └─ agent 不退出！继续运行

→ persist_port 写入 -1（DASHBOARD_AGENT_LISTEN_PORT_NAME）
→ WaitForDashboardAgentPorts 读到 -1 = 成功
→ raylet 继续启动
→ agent 的 gRPC 服务正常（ReporterAgent 照常上报指标）
→ HTTP 服务不可用（/api/healthz、/api/job_agent 不可达）
→ Dashboard Head 无法发现此 agent 的 HTTP 地址
```

**结果**：raylet 正常，worker 正常提交运行。**这是唯一一种"agent 有问题但不影响 raylet 和 worker"的场景**，因为代码故意对 HTTP 失败做了容错。

#### 场景 3：Agent 启动超慢（>15 秒）

```
agent 进程启动了，但 import 慢、gRPC 绑定慢
→ 15 秒内没写 metrics_agent_port 文件
→ WaitForPersistedPort 返回 TimedOut
→ RAY_ASSIGN_OR_CHECK_SET 触发 FATAL
→ raylet 崩溃
```

#### 场景 4：Agent 运行中 crash（OOM、segfault）

```
agent 进程运行了 1 小时后突然 crash
→ monitor_thread_: process_.Wait() 返回
→ fate_shares_ = true
→ shutdown_raylet_gracefully()
→ raylet 退出 → 正在运行的 worker 被杀
→ GCS 重新调度到其他节点
```

### 18.5 设计意图与 TODO

代码中明确注释了未来可能解耦 fate-sharing（`node_manager.cc:3303-3304`）：

```cpp
// TODO(ryw): after thorough testing, we can disable the fate_shares flag and let a
// dashboard agent crash no longer lead to a raylet crash.
auto options = AgentManager::Options({... /*fate_shares=*/true});
```

当前的设计哲学是：**宁可整个节点下线，也不要在不完整的状态下继续运行**。这是"fail-fast"策略——agent crash 可能意味着环境有问题（grpcio 版本不对、OOM），继续运行风险更大。

---

## 19. Agent 进程内部具体逻辑

### 19.1 进程入口 → 事件循环

```
python -m ray.dashboard.agent --node-ip-address ... --grpc-port 0 ...
  │
  ▼
agent.py:__main__
  ├─ setup_component_logger()          ← 配置日志（文件 + 轮转）
  ├─ logging_utils.redirect_stdout_stderr_if_needed()  ← stdout/stderr 重定向
  ├─ loop = get_or_create_event_loop() ← 获取/创建事件循环（整个进程唯一的）
  ├─ agent = DashboardAgent(...)       ← __init__，创建 gRPC server + HttpServerAgent（未启动）
  ├─ setproctitle("ray::DashboardAgent")  ← 设置进程名
  ├─ loop.add_signal_handler(SIGTERM, sigterm_handler)  ← 注册 SIGTERM → os._exit
  │
  └─ loop.run_until_complete(agent.run())  ← 进入事件循环，阻塞直到 agent 退出
```

`loop.run_until_complete(agent.run())` 是阻塞调用——事件循环开始运转，执行 `agent.run()` 协程，直到 `run()` 返回或进程被杀。

### 19.2 agent.run() 内部执行顺序

```python
async def run(self):
    # ─── 阶段 1：启动 gRPC server ───────────────────────────
    if self.server:
        await self.server.start()       # gRPC server 开始监听，但还没有任何 servicer

    # ─── 阶段 2：加载所有模块 ───────────────────────────────
    modules = self._load_modules()
    # 扫描所有 DashboardAgentModule 子类，逐个实例化

    # ─── 阶段 3：启动 HTTP server ───────────────────────────
    if self.http_server:
        try:
            await self.http_server.start(modules)
            # 1. 创建 aiohttp.ClientSession
            # 2. DashboardAgentRouteTable.bind(c) for c in modules
            # 3. 创建 aiohttp.web.Application（挂载中间件）
            # 4. TCPSite 绑定 ip:listen_port（最多重试 6 次）
        except Exception:
            launch_http_server = False  # 失败了也不退出

    # ─── 阶段 4：持久化端口 ────────────────────────────────
    persist_port(..., DASHBOARD_AGENT_LISTEN_PORT_NAME, http_port 或 -1)

    # ─── 阶段 5：写 GCS KV ─────────────────────────────────
    if launch_http_server:
        # DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX<node_id> → [ip, http_port, grpc_port]
        # DASHBOARD_AGENT_ADDR_IP_PREFIX<ip>           → [node_id, http_port, grpc_port]

    # ─── 阶段 6：所有模块并发运行（核心！）─────────────────────
    tasks = [m.run(self.server) for m in modules]

    # ─── 阶段 7：附加 raylet 存活检查 task ──────────────────
    tasks.append(check_parent_task)

    # ─── 阶段 8：附加 gRPC server 阻塞 task ─────────────────
    if self.server:
        tasks.append(self.server.wait_for_termination())
    else:
        tasks.append(wait_forever())

    # ─── 阶段 9：全部并发执行 ──────────────────────────────
    await asyncio.gather(*tasks)

    # ─── 阶段 10：清理 ─────────────────────────────────────
    if self.http_server:
        await self.http_server.cleanup()
```

### 19.3 协程并发图

```
┌─────────────────────────────────────────────────────────────────────────┐
│ 事件循环（单线程）                                                        │
│                                                                         │
│  ┌──────────────────────────────┐  ┌──────────────────────────────┐     │
│  │ ReporterAgent.run()          │  │ HealthzAgent.run()           │     │
│  │  ├─ add_ReporterService      │  │  └─ pass（立即返回）           │     │
│  │  │  Servicer_to_server()     │  │     → 协程结束，不影响其他       │     │
│  │  ├─ add_MetricsService       │  └──────────────────────────────┘     │
│  │  │  (if OTel enabled)        │                                       │
│  │  └─ _run_loop() ← while True │  ┌──────────────────────────────┐     │
│  │      ├─ _async_collect_stats  │  │ JobAgent.run()               │     │
│  │      │  (CPU/GPU/内存采集)    │  │  └─ pass（立即返回）           │     │
│  │      ├─ _to_records()         │  └──────────────────────────────┘     │
│  │      │  (Prometheus 导出)    │                                       │
│  │      ├─ async_publish_node_   │  ┌──────────────────────────────┐     │
│  │      │  resource_usage()     │  │ EventAgent.run()              │     │
│  │      └─ asyncio.sleep(5s)    │  │  ├─ monitor_events()           │     │
│  │                               │  │  │  (TPE 线程监控文件)          │     │
│  └──────────────────────────────┘  │  └─ report_events()            │     │
│                                    │     ← while True              │     │
│  ┌──────────────────────────────┐  │     每 N 秒 HTTP POST 到 Head  │     │
│  │ AggregatorAgent.run()        │  └──────────────────────────────┘     │
│  │  ├─ add_EventAggregator      │                                       │
│  │  │  Service_to_server()      │  ┌──────────────────────────────┐     │
│  │  └─ asyncio.gather(          │  │ LogAgentV1Grpc.run()         │     │
│  │      http_publisher.run_     │  │  ├─ add_LogService            │     │
│  │      forever(),               │  │  │  Servicer_to_server()      │     │
│  │      gcs_publisher.run_      │  │  └─ pass（gRPC handler 由      │     │
│  │      forever())               │  │     server 事件驱动）          │     │
│  └──────────────────────────────┘  └──────────────────────────────┘     │
│                                                                         │
│  ┌──────────────────────────────┐  ┌──────────────────────────────┐     │
│  │ check_parent_task            │  │ gRPC server                  │     │
│  │  ← sys.stdin.readline()      │  │  wait_for_termination()      │     │
│  │     阻塞等待 pipe            │  │  ← 永不返回                     │     │
│  │     0 字节 = raylet 死了      │  │  gRPC 请求由 grpc.aio 内部      │     │
│  │     → sys.exit(0)            │  │  回调驱动，共享同一事件循环      │     │
│  └──────────────────────────────┘  └──────────────────────────────┘     │
│                                                                         │
│  ┌──────────────────────────────────────────────────────────────┐       │
│  │ aiohttp HTTP server（由 TCPSite 驱动）                          │       │
│  │  ← HTTP 请求由 aiohttp 内部回调驱动，共享同一事件循环             │       │
│  │  路由: /api/healthz, /api/job_agent/jobs/..., /logs/...       │       │
│  └──────────────────────────────────────────────────────────────┘       │
└─────────────────────────────────────────────────────────────────────────┘
```

### 19.4 模块的两种运行模式

**模式 A：后台循环型**（`run()` 返回 `while True` 协程）

| 模块 | 循环体 | 周期 |
|------|--------|------|
| ReporterAgent | `_run_loop()`: 采集 stats → 导出 Prometheus → 上报 GCS → sleep | 5 秒 |
| EventAgent | `report_events()`: 从队列取事件 → HTTP POST 到 Head → sleep | 定时 |
| AggregatorAgent | 两个 publisher `run_forever()`: 从 buffer 取事件 → push 到 HTTP/GCS | 持续 |

这些协程的 `while True` 里有 `await asyncio.sleep()`，sleep 时让出 CPU，事件循环调度其他协程。**如果 `while True` 里没有 `await`，会死循环卡住整个进程**。

**模式 B：注册即退出型**（`run()` 立即 `pass` 或 `return`）

| 模块 | 原因 |
|------|------|
| HealthzAgent | 只注册 HTTP 路由，路由由 aiohttp server 事件驱动 |
| JobAgent | 同上 |
| LogAgent | `__init__` 中已注册 `routes.static("/logs", ...)`，`run()` 为 `pass` |
| LogAgentV1Grpc | `run()` 中注册 gRPC servicer 后 `pass`，handler 由 gRPC server 事件驱动 |
| TestAgent | 只注册 HTTP 路由 |

这些模块的 `run()` 协程立即完成，从 `asyncio.gather` 的角度看就是"做完了就退出"。**它们的实际业务逻辑不是在 `run()` 里，而是在 HTTP/gRPC 请求到达时被事件循环回调执行**。

### 19.5 gRPC 请求的回调机制

gRPC server 注册了 servicer 后，不是在某个协程里"轮询"请求，而是由 `grpc.aio` 内部管理：

```
gRPC client → gRPC server (ip:grpc_port)
  │
  ▼
grpc.aio 内部（C-core + Python asyncio 封装）
  ├─ 收到请求 → 在事件循环中创建一个新协程
  │   └─ 调用对应 servicer 方法，例如 ReporterAgent.GetTraceback(request, context)
  │       └─ 执行业务逻辑（可能 await 其他协程）
  │       └─ 返回 response
  └─ 多个请求并发处理（各为独立协程）
```

### 19.6 模块间的共享状态

所有模块通过 `self._dashboard_agent` 访问共享状态：

| 属性 | 来源 | 谁用 |
|------|------|------|
| `dashboard_agent.ip` | 构造函数参数 | ReporterAgent（上报时带 IP）、HealthzAgent |
| `dashboard_agent.gcs_client` | `GcsClient` 实例 | ReporterAgent（上报到 GCS）、AggregatorAgent |
| `dashboard_agent.http_session` | `HttpServerAgent.http_session`（aiohttp.ClientSession） | EventAgent（POST 事件到 Head） |
| `dashboard_agent.log_dir` | 构造函数参数 | LogAgent（静态文件根目录）、EventAgent（事件目录） |
| `dashboard_agent.node_id` | `os.environ["RAY_NODE_ID"]` | ReporterAgent（端口持久化、KV key） |
| `dashboard_agent.metrics_export_port` | raylet 传入 | ReporterAgent（Prometheus exporter 绑定） |
| `dashboard_agent.is_head` | 构造函数参数 | ReporterAgent（head 节点额外采集 GCS stats） |

**模块之间不直接通信**，只通过 `DashboardAgent` 实例间接共享配置。

### 19.7 进程退出的几种路径

```
路径 1：Raylet 死亡
  check_parent_task 检测到 pipe 断开
  → report_raylet_error_logs()
  → sys.exit(0)
  → 事件循环停止 → 所有协程被 cancel → 进程退出

路径 2：Agent 自身 crash
  任何协程抛出未捕获异常
  → asyncio.gather 收到异常 → gather 返回
  → agent.run() 返回 → loop.run_until_complete 返回
  → __main__ 的 except 捕获 → logger.exception + exit(1)

路径 3：SIGTERM
  loop.add_signal_handler(SIGTERM, sigterm_handler)
  → sigterm_handler: os._exit(SIGTERM)
  → 立即退出，不走 cleanup

路径 4：Raylet 主动 kill（构造函数析构）
  AgentManager::~AgentManager()
  → fate_shares_ = false  （避免循环触发）
  → process_.Kill()  → SIGKILL
  → agent 进程直接被杀
```

### 19.8 ReporterAgent 内部循环细节

```python
async def _run_loop(self):
    loop = get_or_create_event_loop()
    while True:
        try:
            # 1. 如果是 head 节点，从 GCS 拿 autoscaler 状态 + GCS PID
            if self._is_head_node:
                autoscaler_status = await self._gcs_client.async_internal_kv_get(...)
                self._gcs_pid = await self._gcs_client.async_internal_kv_get(GCS_PID_KEY...)

            # 2. 在线程池中执行同步采集逻辑（避免阻塞事件循环）
            json_payload = await loop.run_in_executor(
                self._executor,        # ThreadPoolExecutor(max_workers=1)
                self._run_in_executor,
                autoscaler_status,
            )
            # _run_in_executor 内部:
            #   → asyncio.run(self._async_compose_stats_payload(...))
            #     → _async_collect_stats()  ← 采集 CPU/GPU/内存/磁盘/网络
            #     → _to_records()           ← 转 Prometheus Record 列表
            #     → record_and_export()    ← 导出到 Prometheus / OpenTelemetry
            #     → _generate_stats_payload() ← 序列化为 JSON

            # 3. 发布到 GCS pub/sub
            await self._gcs_client.async_publish_node_resource_usage(
                self._key, json_payload
            )

        except Exception:
            logger.exception("Error publishing node physical stats.")

        # 4. 休眠 5 秒，让出 CPU 给其他协程
        await asyncio.sleep(reporter_consts.REPORTER_UPDATE_INTERVAL_MS / 1000)
```

**注意 `run_in_executor`**：stats 采集涉及大量 `psutil` 和 NVML 调用（同步阻塞），不能在事件循环线程里跑。用 `ThreadPoolExecutor` 扔到另一个线程执行，执行完通过 `await` 拿回结果。这样事件循环在采集期间还能处理 gRPC 请求和 HTTP 请求。

---

## 20. ReporterAgent 指标上报的三条路径

ReporterAgent 同时走**三条指标路径**，模式各不同：

### 20.1 路径 1：GCS pub/sub — 推模式

```
ReporterAgent._run_loop() (每 5 秒)
  → _async_collect_stats()        ← 采集 CPU/GPU/内存/磁盘/网络/worker 进程
  → _generate_stats_payload()     ← 序列化为 JSON (StatsPayload)
  → gcs_client.async_publish_node_resource_usage(key, json_payload)
      │
      ▼
  GCS pub/sub channel: RAY_NODE_RESOURCE_USAGE_CHANNEL
      │
      ▼
  NodeHead._update_node_physical_stats()  ← 订阅者，被动接收
      → DataSource.node_physical_stats[node_id] = parsed_data
      → 前端 GET /nodes?view=summary 时从这里读
```

**推模式**。Agent 主动推，Head 被动收。Head 不需要轮询 Agent。

### 20.2 路径 2：Prometheus — 拉模式

```
ReporterAgent._run_loop() (每 5 秒)
  → _async_collect_stats()
  → _to_records(stats, cluster_stats)  ← 生成 List[Record]
  → metrics_agent.record_and_export(records)
      │
      ▼
  prometheus_exporter (本地 HTTP server, ip:metrics_export_port)
  暴露 /metrics 端点
      │
      ▼
  Prometheus scraper (定时 pull)
      → HTTP GET http://ip:metrics_export_port/metrics
      → 存储到 TSDB → Grafana 查询 Prometheus
```

**拉模式**。Agent 把指标写到本地 Prometheus exporter 的内存中，Prometheus 定时来拉。

### 20.3 路径 3：OpenTelemetry remote_write — 推模式

```
ReporterAgent._run_loop() (每 5 秒)
  → _async_collect_stats()
  → _to_records(stats, cluster_stats)
  → open_telemetry_metric_recorder.record_and_export(records)
      │
      ▼
  OpenTelemetry SDK (HTTP POST)
  → push 到配置的 OTLP endpoint (e.g., http://10.81.0.157:9090)
```

**推模式**。Agent 主动 push 到远端 OTLP endpoint。

### 20.4 路径选择

```python
# reporter_agent.py:~1750
if RAY_ENABLE_OPEN_TELEMETRY:
    self._open_telemetry_metric_recorder.record_and_export(records, ...)
else:
    self._metrics_agent.record_and_export(records, ...)
```

- `RAY_ENABLE_OPEN_TELEMETRY=True` → 走推模式（OTLP remote_write），同时注册 MetricsService gRPC 接收 worker 指标
- 默认 → 走拉模式（Prometheus pull），Worker 通过 ReportOCMetrics gRPC 推指标到 Agent

### 20.5 Worker → Agent 的 gRPC 指标上报 — 推模式

```
Worker 进程 (C++ + Python)
  │
  ├─ OpenCensus 路径 (默认):
  │   worker 的 C++ 代码定时采集指标
  │   → gRPC 调用 ReporterService.ReportOCMetrics(request)
  │   → Agent 收到后: metrics_agent.proxy_export_metrics(request.metrics, worker_id)
  │   → 转发给本地 Prometheus exporter
  │
  └─ OpenTelemetry 路径 (RAY_ENABLE_OPEN_TELEMETRY=True):
      worker 的 C++ 代码采集 OTLP 指标
      → gRPC 调用 MetricsService.Export(request)  (OTLP 协议)
      → Agent 收到后: _export_number_data() / _export_histogram_data()
      → 转发给 OpenTelemetry recorder → push 到远端
```

### 20.6 数据流模式总结

| 组件 | 模式 | 一句话描述 |
|------|------|-----------|
| Agent gRPC server | **混合** | Worker 推指标/事件（推），Head 拉 profiling/日志（拉） |
| Agent HTTP server | **拉** | Head 主动 HTTP GET 请求 Agent（拉） |
| ReporterAgent → GCS | **推** | 每 5 秒主动 push 节点物理指标到 GCS pub/sub |
| ReporterAgent → Prometheus | **拉** | Agent 写本地 exporter 内存，Prometheus 定时拉 |
| ReporterAgent → OTLP | **推** | 每 5 秒主动 push 指标到远端 endpoint |
| Worker → Agent (gRPC) | **推** | Worker 主动 push 指标和事件到 Agent |
| EventAgent → Head | **推** | Agent 主动 POST 事件到 Head HTTP |
| AggregatorAgent → Head/GCS | **推** | Agent 主动 push 聚合事件 |

**核心规律**：Agent 作为**中间层**，对上游（Worker）是被动接收（Worker 推），对下游有两种：
- GCS pub/sub 和 OTLP → 推模式（Agent 主动推）
- Prometheus exporter 和 HTTP server → 拉模式（外部主动拉）

---

## 21. gRPC Server 与 gRPC Client 的关系

### 21.1 完全独立的两个角色

```
DashboardAgent 进程
┌─────────────────────────────────────────────────────────┐
│                                                         │
│  gRPC SERVER (ip:grpc_port)          gRPC CLIENT        │
│  ┌────────────────────────┐         ┌────────────────┐  │
│  │ 监听端口，被动接收        │         │ 连接到别人，主动发起│  │
│  │                        │         │                │  │
│  │ 注册了 4 个 service:    │         │ GcsClient:     │  │
│  │ ├─ ReporterService     │         │ → 连到 GCS      │  │
│  │ ├─ MetricsService      │         │   (ip:6379)     │  │
│  │ ├─ LogService          │         │                │  │
│  │ └─ EventAggregatorSvc  │         │ RayletClient:   │  │
│  │                        │         │ → 连到本节点 raylet│  │
│  │ 谁连过来?              │         │   (ip:node_mgr) │  │
│  │ ├─ Worker gRPC client  │         │                │  │
│  │ │  (推指标/事件)        │         │                │  │
│  │ └─ Head gRPC client    │         │                │  │
│  │    (拉 profiling/日志) │         │                │  │
│  └────────────────────────┘         └────────────────┘  │
│         ↑                               ↑               │
│         │ 两个东西，没有关联                │               │
│         │ server 不知道 client 存在        │               │
│         └───────────────────────────────┘               │
└─────────────────────────────────────────────────────────┘
```

**关键点**：
- gRPC server 监听 `grpc_port`，等别人来连
- gRPC client 主动连到 GCS/raylet 的 server
- 两者**共享同一个 asyncio 事件循环**（都是 `grpc.aio`），但**逻辑上完全隔离**
- 两者的 gRPC channel、连接池、拦截器都各自独立

### 21.2 GcsClient 的本质

```python
# agent.py:82
self.gcs_client = GcsClient(
    address=self.gcs_address,    # GCS server 地址 (ip:6379)
    cluster_id=self.cluster_id_hex,
)
```

**GcsClient 不是纯 Python 的 gRPC client**，它是 C++ `GcsClient` 的 Cython 封装。底层 C++ `GcsClient` 内部有自己的 gRPC channel 连到 GCS server，有自己的 io_context（boost::asio），跟 Agent 的 Python asyncio 事件循环是**两套独立的 I/O 模型**。

GcsClient 的 async 方法返回的是 `asyncio.wrap_future(fut)`——把 C++ 的 `concurrent.futures.Future` 包装成 Python 的 `asyncio.Future`，让 Python 侧可以 `await`。两个 I/O 模型通过 Future 桥接。

### 21.3 GcsClient 与各模块 — 共享同一个实例

所有模块通过 `self._dashboard_agent.gcs_client` 访问同一个 C++ GcsClient 实例：

| 模块 | 用途 |
|------|------|
| ReporterAgent | `async_publish_node_resource_usage()` (推指标), `async_internal_kv_get()` (拉 autoscaler 状态) |
| HealthzAgent | 检查 GCS 是否存活（健康检查） |
| JobAgent | `internal_kv_get/put`（作业元数据） |
| EventAgent | `internal_kv_get`（获取 Dashboard Head HTTP 地址） |
| AggregatorAgent | 创建 GCS task events publisher |

### 21.4 ReporterAgent 的两个 client

| 维度 | GcsClient | RayletClient |
|------|-----------|-------------|
| 实现层 | C++ Cython 封装 | 纯 Python（`grpc.aio`） |
| 底层 I/O | C++ boost::asio io_context | Python asyncio 事件循环 |
| 连接目标 | GCS server（全局唯一，ip:6379） | 本节点 raylet（ip:node_manager_port） |
| 共享性 | 所有模块共享同一个实例 | ReporterAgent 独占 |
| 用途 | 推 pub/sub、读写 KV | 获取 worker PID 列表 |

### 21.5 Agent 进程内的网络端点总览

Agent 进程内有 **6 个独立的网络端点**：

| 端点 | 类型 | 端口 | 模式 |
|------|------|------|------|
| gRPC server | 被动监听 | `grpc_port` | Worker 推 + Head 拉 |
| HTTP server | 被动监听 | `52365` | Head/K8s 拉 |
| Prometheus exporter | 被动监听 | `metrics_export_port` | Prometheus 拉 |
| GcsClient | 主动连接 | → GCS 6379 | 推 pub/sub + 拉 KV |
| RayletClient | 主动连接 | → raylet node_mgr_port | 拉 worker PIDs |
| aiohttp ClientSession | 主动连接 | → Head 8265 | 推事件 |

---

## 22. gRPC 与 HTTP 绑定到具体模块和方法的机制

### 22.1 gRPC 绑定链路

```
proto 定义
  rpc ReportOCMetrics(ReportOCMetricsRequest) returns (ReportOCMetricsReply)
        │
        ▼
protoc 编译
  reporter_pb2_grpc.py
  ├─ ReporterServiceServicer (基类，6 个抽象方法)
  └─ add_ReporterServiceServicer_to_server(servicer, server)
        │
        ▼
模块类实现接口
  class ReporterAgent(ReporterServiceServicer):
      async def ReportOCMetrics(self, request, context):
          # request.worker_id, request.metrics
          self._metrics_agent.proxy_export_metrics(request.metrics, ...)
          return ReportOCMetricsReply()
        │
        ▼
run(server) 中注册
  reporter_pb2_grpc.add_ReporterServiceServicer_to_server(self, server)
        │
        ▼
gRPC server 内部建立映射
  "ray.core.generated.ReporterService/ReportOCMetrics"
    → handler: ReporterAgent.ReportOCMetrics
    → request 反序列化: ReportOCMetricsRequest.FromString
    → response 序列化: ReportOCMetricsReply.SerializeToString
        │
        ▼
运行时 gRPC client 调用
  Worker 进程:
    stub = ReporterServiceStub(channel)
    reply = stub.ReportOCMetrics(request)
```

一个模块可以注册多个 gRPC 服务——ReporterAgent 同时继承 `ReporterServiceServicer` 和 `MetricsServiceServicer`，在 `run()` 中注册两次，共 7 个 gRPC method 注册到同一个 server。

### 22.2 HTTP 绑定链路（两阶段）

**第一阶段：装饰器注册（类定义时）** — `instance=None`

```python
# healthz_agent.py
routes = dashboard_optional_utils.DashboardAgentRouteTable

@routes.get("/api/healthz")
async def health_check(self, req): ...
```

`@routes.get` 内部创建 `BindInfo(instance=None)`，存入 `_bind_map`，同时注册到 aiohttp `_routes`。

**第二阶段：实例绑定（http_server.start 时）**

```python
for c in modules:
    DashboardAgentRouteTable.bind(c)
    # → _bind_map["GET"]["/api/healthz"].instance = healthz_agent_instance
```

**第三阶段：路由导出**

```python
app.add_routes(routes=routes.bound_routes())
# bound_routes() 过滤出 instance != None 的路由
```

**运行时**：

```
GET /api/healthz
  → aiohttp 匹配路由 → _handler_route(request)
  → await handler(bind_info.instance, request)
  → await healthz_agent_instance.health_check(request)
```

### 22.3 gRPC vs HTTP 绑定方式对比

| 维度 | gRPC | HTTP |
|------|------|------|
| 接口定义 | proto 文件（.proto） | 装饰器 `@routes.get(path)` |
| 方法签名 | `(self, request, context) → reply` | `(self, request) → Response` |
| 绑定时机 | `run(server)` 中调用 `add_*Servicer_to_server` | 类定义时注册路由 + `http_server.start` 时 `bind(instance)` |
| 绑定对象 | 模块实例（self）直接作为 servicer | 模块实例注入到 BindInfo |
| 方法查找 | gRPC 按 `ServiceName/MethodName` 查 handler map | aiohttp 按 HTTP method + path 匹配路由 |
| 一个类多服务 | 继承多个 Servicer，调用多次 `add_*` | 不同模块各自用 `@routes` 注册不同路径 |

**核心区别**：
- gRPC 绑定是**显式的** — 在 `run(server)` 里手动调 `add_*Servicer_to_server`，把模块实例和 gRPC server 关联起来
- HTTP 绑定是**隐式的两阶段** — 装饰器先注册"空壳"路由（不知道实例），`bind()` 再注入实例，`bound_routes()` 过滤已绑定的路由

---

## 23. Head 与 Worker 节点在 Dashboard 体系中的完整对比

### 23.1 gRPC 在 head 和 worker 节点上的区别

**DashboardAgent 中的 gRPC 在 head 和 worker 节点上完全没有区别**。所有 gRPC 服务的注册和实现都完全一致：

| 维度 | head 和 worker 是否有区别 |
|------|------------------------|
| gRPC server 创建（端口、选项、拦截器） | **无** — 代码完全一致 |
| 4 个 gRPC 服务的注册 | **无** — ReporterService、MetricsService、LogService、EventAggregatorService 均无条件注册 |
| 所有 gRPC 方法的实现 | **无** — GetTraceback/CpuProfiling/GpuProfiling/MemoryProfiling/HealthCheck/ReportOCMetrics/Export/AddEvents/ListLog/StreamLog 都不检查 `is_head` |

`is_head` 影响的只有**非 gRPC** 的内容：
1. **Stats 采集** — 只在 head 节点采集 GCS 进程 stats 和 autoscaler 状态（后台循环任务）
2. **Prometheus 指标标签** — 标记 `RayNodeType=head/worker`
3. **HTTP 健康检查** — `/api/healthz` 在 worker 节点跳过 GCS liveness 检查

### 23.2 DashboardHead 与 DashboardAgent 的架构对比

Ray 的 Dashboard 体系由**两个完全独立的组件**构成：

| 维度 | DashboardAgent | DashboardHead |
|------|----------------|----------------|
| 运行位置 | **所有节点**（head + worker） | **仅 head 节点** |
| 入口文件 | `agent.py` | `dashboard.py` → `head.py` |
| 进程类型 | `PROCESS_TYPE_DASHBOARD_AGENT` | `PROCESS_TYPE_DASHBOARD` |
| gRPC server | 有（4 个 service） | **无** |
| HTTP server | 52365 | **8265**（对外 API 网关） |
| Prometheus 端口 | `metrics_export_port`（动态） | **44227** |
| 路由表 | `DashboardAgentRouteTable` | `DashboardHeadRouteTable` + `SubprocessRouteTable` |
| 模块类型 | `DashboardAgentModule`（8 个，in-process） | `DashboardHeadModule`（in-process）+ `SubprocessModule`（独立子进程） |
| 角色 | 数据采集器 + gRPC 服务端 | API 网关 + gRPC **客户端** |
| GCS KV 写入 | agent 地址 | `DASHBOARD_ADDRESS`（供 `ray.init()` 发现） |

### 23.3 DashboardHead 的子进程模块

DashboardHead 主进程会 spawn **9 个 SubprocessModule 子进程**，每个在独立进程中运行，通过 Unix socket（Linux/Mac）或 Named Pipe（Windows）与主进程通信：

| 子进程 | 作用 | 与 Agent 的交互 |
|--------|------|----------------|
| **ReportHead** | gRPC client → 调各 agent 的 `ReporterService`（profiling/traceback） | gRPC 调所有 agent（含本机） |
| **NodeHead** | 从 GCS 读节点/actor 信息；订阅 agent 推的 pub/sub 指标 | 通过 GCS pub/sub 收 agent 指标 |
| **StateHead** | Ray State API（actors/jobs/nodes/tasks/objects/logs 查询） | gRPC 调各 agent 的 `LogService`（日志流） |
| **JobHead** | 作业提交 API → HTTP 转发到各 agent 的 JobAgent | HTTP 调所有 agent |
| **EventHead** | 事件聚合查询；接收 agent 推事件 | HTTP 被 agent 的 EventAgent POST |
| **ServeHead** | Ray Serve 应用管理 | 不直接调 agent |
| **DataHead** | Dataset API | 不直接调 agent |
| **MetricsHead** | Grafana/Prometheus 健康检查 | 不直接调 agent |
| **TrainHead** | Training run API | 不直接调 agent |

### 23.4 DashboardHead 独有的 HTTP 路由（Worker 节点没有）

| 模块 | 路由示例 | 作用 |
|------|---------|------|
| HttpServerDashboardHead | `GET /`, `GET /favicon.ico`, `static /static` | Web 前端 |
| ReportHead | `GET /worker/traceback`, `GET /worker/cpu_profile`, `GET /worker/gpu_profile`, `GET /memory_profile` | Profiling（gRPC 转发到 agent） |
| ReportHead | `GET /api/cluster_status`, `GET /api/gcs_healthz` | 集群状态 |
| NodeHead | `GET /nodes`, `GET /nodes/{node_id}`, `GET /logical/actors` | 节点/actor 列表 |
| StateHead | `GET /api/v0/actors`, `GET /api/v0/jobs`, `GET /api/v0/tasks`, `GET /api/v0/logs` | State API |
| JobHead | `POST /api/jobs/`, `GET /api/jobs/{id}/logs` | 作业管理 |
| EventHead | `POST /report_events`, `GET /events` | 事件接收与查询 |
| ServeHead | `GET /api/serve/applications/` | Serve 管理 |
| DataHead | `GET /api/data/datasets/{job_id}` | Dataset API |
| MetricsHead | `GET /api/grafana_health`, `GET /api/prometheus_health` | 监控检查 |
| TrainHead | `GET /api/train/v2/runs` | 训练 API |
| UsageStatsHead | `GET /usage_stats_enabled`, `GET /cluster_id` | 使用统计 |

### 23.5 Head 节点上的两个进程

Head 节点同时运行 DashboardAgent 和 DashboardHead，两者**不共享内存**：

```
Head 节点:
  raylet (C++ 进程)
    ├── DashboardAgent (python agent.py)
    │     ├── HTTP server :52365
    │     ├── gRPC server :<动态>
    │     └── Prometheus exporter :<动态>
    └── runtime_env_agent

  start_api_server() → DashboardHead (python dashboard.py)
    ├── HTTP server :8265 (对外 API 网关 + Web UI)
    ├── Prometheus metrics :44227
    ├── UsageStatsHead (in-process)
    └── SubprocessModule 子进程 (×9):
          ├── MetricsHead-0
          ├── DataHead-0
          ├── EventHead-0
          ├── JobHead-0
          ├── NodeHead-0
          ├── ReportHead-0
          ├── ServeHead-0
          ├── StateHead-0
          └── TrainHead-0

Worker 节点:
  raylet (C++ 进程)
    ├── DashboardAgent (python agent.py)
    │     ├── HTTP server :52365
    │     ├── gRPC server :<动态>
    │     └── Prometheus exporter :<动态>
    └── runtime_env_agent
  (无 DashboardHead)
```

---

## 24. DashboardAgent 与 DashboardHead 之间的直接交互

### 24.1 直接通信路径

Head 节点上的 DashboardAgent 和 DashboardHead 之间有 **4 条直接通信路径**（不经过 GCS 转发，但地址发现走 GCS KV）：

| # | 方向 | 协议 | 路径 | 说明 |
|---|------|------|------|------|
| 1 | Agent → Head | HTTP POST | `event_agent.py:89` → `event_head.py:150` | EventAgent POST 事件到 Head 的 `/report_events` |
| 2 | 共享 | 文件系统 | `log_dir/events/` 目录 | 两者独立 mmap 读取同一个事件日志目录 |
| 3 | Head → Agent | HTTP | `job_head.py:248` → `job_agent.py` | JobHead 转发作业提交/停止/日志到本节点 JobAgent |
| 4 | Head → Agent | gRPC | `reporter_head.py:827` → `reporter_agent.py` | ReportHead 调 ReporterAgent 做 traceback/profiling |

**没有共享内存或 Unix socket**。两条进程树独立（Agent 是 raylet 的子进程，Head 是独立进程），仅通过 HTTP、gRPC 和共享文件系统交互。

### 24.2 地址发现机制

两者通过 GCS KV 互相发现，但**实际数据传输是直连**：

```
Agent 发现 Head:
  EventAgent → GCS KV 读 "dashboard" key → 得到 head HTTP 地址 (ip:8265) → HTTP POST 事件

Head 发现 Agent:
  JobHead → GCS KV 读 DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX → 得到 agent HTTP 地址 (ip:52365) → HTTP 转发作业
  ReportHead → GCS KV 读 DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX → 得到 agent gRPC 地址 → gRPC 调 profiling
  StateHead → GCS KV 读 DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX → 得到 agent gRPC 地址 → gRPC 调 LogService
```

### 24.3 Head 节点上的实际数据流

```
DashboardAgent (52365)                    DashboardHead (8265)
                    │                            │
                    │── HTTP POST /report_events ──→│  (Agent → Head，事件上报)
                    │                            │
                    │←── HTTP POST /api/job_agent ──│  (Head → Agent，作业转发)
                    │                            │
                    │←── gRPC GetTraceback etc ─────│  (Head → Agent，profiling)
                    │                            │
                    ├── 共享 log_dir/events/ ──────┤  (两者独立读取同一目录)
```

---

## 25. DashboardHead 与所有节点 DashboardAgent 的交互

### 25.1 Head → Worker Agent 的直接交互路径

DashboardHead 作为集群 API 网关，会和**所有节点**（包括其他 worker 节点）上的 DashboardAgent 直接交互：

| # | 协议 | Head 侧模块 | 调用 Agent 的 | 触发场景 |
|---|------|------------|-------------|---------|
| 1 | **gRPC** | ReportHead | `ReporterService.GetTraceback/CpuProfiling/GpuProfiling/MemoryProfiling` | 用户在 Dashboard 查看 worker 堆栈/profiling |
| 2 | **gRPC** | StateHead（经 `StateDataSourceClient`） | `LogService.ListLogs/StreamLog` | 用户通过 `ray logs` 或 Dashboard 查看日志 |
| 3 | **HTTP** | JobHead | `JobAgent POST/DELETE/GET /api/job_agent/jobs/...` | 用户提交/停止/删除/查看作业到指定节点 |
| 4 | **HTTP POST** | EventHead | 被 Agent 的 EventAgent POST `/report_events` | Agent 主动推事件到 Head（反向：Agent→Head） |

### 25.2 统一的地址发现机制

所有路径都通过 GCS KV 发现目标 agent 地址：

```
Head 读取 GCS KV:
  key = DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX<node_id>
  value = [ip, http_port, grpc_port]
       → gRPC: init_grpc_channel(f"{ip}:{grpc_port}")
       → HTTP: f"http://{ip}:{http_port}"
```

Head **不区分**目标节点是本机还是远端——对所有节点走相同逻辑。对本机 agent 走 `127.0.0.1` 或本机 IP，对远端 worker 走跨网络。

### 25.3 完整集群交互拓扑

```
                    DashboardHead (head:8265)
                   ┌─────┴──────────────────────────────────┐
                   │                                          │
          gRPC     │          gRPC         HTTP               │  HTTP POST (反向)
         ┌─────────┘    ┌──────────────┐  ┌──────────────┐     │  ┌──────────────┐
         ▼              ▼              ▼                   ▼     ▼
  ReportHead       StateHead       JobHead            EventHead
         │              │              │                   │
         │ gRPC         │ gRPC         │ HTTP              │ HTTP
         ▼              ▼              ▼                   ▲
  ┌──────────┐   ┌──────────┐   ┌──────────┐         ┌──────────┐
  │Agent(head)│   │Agent(head)│   │Agent(head)│       │Agent(head)│
  │:grpc_port│   │:grpc_port│   │:52365   │       │EventAgent│
  └──────────┘   └──────────┘   └──────────┘       └──────────┘
         │              │              │                   ▲
         │ gRPC         │ gRPC         │ HTTP              │ HTTP POST
         ▼              ▼              ▼                   │
  ┌──────────┐   ┌──────────┐   ┌──────────┐         ┌──────────┐
  │Agent(w1) │   │Agent(w1) │   │Agent(w1) │       │Agent(w1) │
  │:grpc_port│   │:grpc_port│   │:52365   │       │EventAgent│
  └──────────┘   └──────────┘   └──────────┘       └──────────┘
         │              │              │                   ▲
  ... 同理所有 worker 节点 ...
```

**核心**：DashboardHead 是集群唯一的 dashboard API 入口（8265），它作为 gRPC/HTTP client 连接**每个节点**的 agent。无论 head 本地还是远端 worker，交互方式完全一致——通过 GCS KV 发现地址，然后直接 gRPC/HTTP 调用。

---

## 26. 端口占用全景

### 26.1 DashboardAgent 端口（每个节点都运行）

| 端口 | 默认值 | 协议 | 用途 | 配置参数 |
|------|--------|------|------|---------|
| `grpc_port` | 由 raylet 传入，0=动态分配 | gRPC | gRPC server（4 个 service） | `--grpc-port=0` |
| `listen_port` | **52365** | HTTP | HTTP server（健康检查、作业管理、日志浏览） | `--listen-port=52365` |
| `metrics_export_port` | 由 raylet 传入，0=动态分配 | HTTP | Prometheus exporter `/metrics` | `--metrics-export-port=0` |

### 26.2 DashboardHead 端口（仅 head 节点）

| 端口 | 默认值 | 协议 | 用途 | 配置 |
|------|--------|------|------|------|
| `http_port` | **8265** | HTTP | 主 HTTP server（Web UI + 全部 API 端点） | `DEFAULT_DASHBOARD_PORT` |
| `DASHBOARD_METRIC_PORT` | **44227** | HTTP | DashboardHead 自身的 Prometheus metrics | 环境变量 `DASHBOARD_METRIC_PORT` |

### 26.3 Head 节点完整端口占用

```
Head 节点（两个进程共占 5 个端口）:
  DashboardAgent:
    ├── grpc_port          (动态)    ← gRPC server
    ├── 52365                       ← HTTP server
    └── metrics_export_port(动态)    ← Prometheus exporter

  DashboardHead:
    ├── 8265                       ← HTTP server (Web UI + API)
    └── 44227                      ← Prometheus metrics

Worker 节点（一个进程占 3 个端口）:
  DashboardAgent:
    ├── grpc_port          (动态)    ← gRPC server
    ├── 52365                       ← HTTP server
    └── metrics_export_port(动态)    ← Prometheus exporter
```

所有端口都可通过命令行参数或环境变量覆盖，传 `0` 让 OS 动态分配。实际绑定端口写入 `<session_dir>/ports/<node_id>/` 下的端口文件供其他进程发现。

### 26.4 端口持久化文件

| 持久化名称 | 含义 | 所属进程 |
|-----------|------|---------|
| `METRICS_AGENT_PORT_NAME` | gRPC server 端口 | DashboardAgent |
| `DASHBOARD_AGENT_LISTEN_PORT_NAME` | HTTP server 端口 | DashboardAgent |
| `METRICS_EXPORT_PORT_NAME` | Prometheus metrics 导出端口 | DashboardAgent |
| `DASHBOARD_ADDRESS` (GCS KV) | DashboardHead HTTP 地址 | DashboardHead |

---

## 27. raylet 命令行中的 Dashboard Agent 参数实例

### 27.1 实际生产环境命令行分析

以下是从一个实际 head 节点的 raylet 进程命令行中提取的 `--dashboard_agent_command=` 参数：

```bash
python -u .../ray/dashboard/agent.py \
  --node-id=5b6ea81d66e6bf29b745774a200ac770a1a88cb057cef652f16b3d09 \
  --node-ip-address=10.15.3.158 \
  --metrics-export-port=0 \
  --grpc-port=0 \
  --listen-port=52365 \
  --node-manager-port=RAY_NODE_MANAGER_PORT_PLACEHOLDER \
  --object-store-name=/tmp/ray/session_xxx/sockets/plasma_store \
  --raylet-name=/tmp/ray/session_xxx/sockets/raylet \
  --temp-dir=/tmp/ray \
  --session-dir=/tmp/ray/session_xxx \
  --log-dir=/tmp/ray/session_xxx/logs \
  --session-name=session_xxx \
  --gcs-address=10.15.3.158:6379 \
  --cluster-id-hex=d8f2b1c9470baaab8c99da65899e78985d78cc8963b283e5d6c715a2 \
  --stdout-filepath=/tmp/ray/session_xxx/logs/dashboard_agent.out \
  --stderr-filepath=/tmp/ray/session_xxx/logs/dashboard_agent.err \
  --head
```

### 27.2 参数详解

| 参数 | 值 | 含义 |
|------|-----|------|
| `--node-id` | `5b6ea...` | 节点唯一 ID（hex） |
| `--node-ip-address` | `10.15.3.158` | 节点 IP |
| `--metrics-export-port=0` | 0 | Prometheus exporter 端口，0 = OS 动态分配 |
| `--grpc-port=0` | 0 | gRPC server 端口，0 = OS 动态分配 |
| `--listen-port=52365` | 52365 | HTTP server 固定端口 |
| `--node-manager-port` | `RAY_NODE_MANAGER_PORT_PLACEHOLDER` | raylet 启动后替换为实际端口 |
| `--gcs-address` | `10.15.3.158:6379` | GCS 地址（head 节点 = 本机；worker 节点 = 远端 head IP） |
| `--cluster-id-hex` | `d8f2b1...` | 集群 ID |
| `--head` | 标志 | 表示这是 head 节点的 agent，`is_head=True` |
| `--stdout-filepath` / `--stderr-filepath` | 路径 | 日志输出文件 |

### 27.3 raylet 命令中的其他相关参数

raylet 主进程命令行中还包含：

| 参数 | 值 | 含义 |
|------|-----|------|
| `--webui=10.15.3.158:8265` | head IP:8265 | DashboardHead 的 HTTP 地址，供 agent 的 EventAgent 发现 head |
| `--head` | 标志 | raylet 自身的 head 标志（与 `--dashboard_agent_command` 中的 `--head` 对应） |
| `--gcs-address=10.15.3.158:6379` | GCS 地址 | raylet 连 GCS 的地址 |
| `--node_manager_port=0` | 0 | raylet 自身 gRPC 端口，0 = 动态分配 |

### 27.4 `--head` 标志的传递链路

```
Python services.py (start_raylet, is_head_node=True)
  → dashboard_agent_command 包含 "--head"         (services.py:1835-1836)
  → 序列化为 "--dashboard_agent_command=..."       (services.py:1939)
  → 传递给 raylet 进程
  → raylet main.cc 解析 FLAGS_dashboard_agent_command (main.cc:254)
  → NodeManager::CreateDashboardAgentManager      (node_manager.cc:3289)
  → AgentManager::StartAgent 启动 Python 进程      (agent_manager.cc:26)
  → agent.py 解析 --head 参数                      (agent.py:469)
  → DashboardAgent(is_head=True)                   (agent.py:498)
  → 子模块通过 dashboard_agent.is_head 访问        (agent.py:87)
```

### 27.5 head 节点和 worker 节点命令行对比

| 参数 | Head 节点 | Worker 节点 |
|------|----------|------------|
| `--head` | **有** | 无 |
| `--gcs-address` | `10.15.3.158:6379`（本机 IP） | `<head_ip>:6379`（远端 head IP） |
| `--listen-port` | `52365` | `52365`（相同） |
| `--grpc-port` | `0`（动态） | `0`（动态） |
| `--metrics-export-port` | `0`（动态） | `0`（动态） |
| `--webui`（raylet 参数） | `10.15.3.158:8265`（本机） | `<head_ip>:8265`（远端 head） |
| DashboardHead 进程 | **有**（:8265） | **无** |

---

## 28. DashboardHead 子进程的生产环境实例

### 28.1 实际 `ps -ef` 输出分析

以下是从一个实际 head 节点的 `ps -ef | grep Head` 输出（DashboardHead 主进程 PID 274，spawn 了 9 个子进程）：

| PID | 父 PID | 进程名 | 运行时间 | CPU 占用 | 用途 |
|-----|--------|--------|---------|---------|------|
| 460 | 274 | MetricsHead-0 | 00:17:53 | 0% | Grafana/Prometheus 健康检查 |
| 461 | 274 | DataHead-0 | 00:58:42 | 0% | Dataset API |
| 462 | 274 | EventHead-0 | 01:19:22 | 0% | 事件聚合查询、接收 agent 推事件 |
| 463 | 274 | JobHead-0 | 00:22:17 | 0% | 作业提交 API → 转发到各 agent |
| 464 | 274 | **NodeHead-0** | **19-14:59:50** | **99%** | 节点/actor 列表查询 |
| 465 | 274 | ReportHead-0 | 03:40:22 | 0% | profiling gRPC client → 各 agent |
| 466 | 274 | ServeHead-0 | 00:55:03 | 0% | Ray Serve 管理 |
| 467 | 274 | StateHead-0 | 01:43:20 | 0% | State API (actors/jobs/tasks/logs) |
| 468 | 274 | TrainHead-0 | 00:17:45 | 0% | Training run API |

### 28.2 子进程启动方式

所有子进程通过 Python `multiprocessing.spawn` 启动，命令行格式：

```
python -c "from multiprocessing.spawn import spawn_main; spawn_main(tracker_fd=26, pipe_handle=N)" --multiprocessing-fork
```

每个子进程：
- 通过 `pipe_handle` 与父进程（DashboardHead PID 274）通信
- 运行各自的 aiohttp server，绑定在 Unix socket 上
- DashboardHead 主进程的 HTTP server 通过 `SubprocessRouteTable` 代理请求到子进程

### 28.3 NodeHead 异常高 CPU 问题

`NodeHead-0`（PID 464）占用 **99% CPU 且运行了 19 天 14 小时**，这明显异常。NodeHead 负责 `/nodes`、`/logical/actors` 等 API，持续高 CPU 可能原因：

1. **节点数过多**导致 GCS 查询数据量巨大
2. **死循环或 busy-poll**（`while True` 中没有 `await` 让出 CPU）
3. **内存泄漏**导致 GC 压力
4. **pub/sub 消息积压**导致处理跟不上

建议排查：
- 检查 NodeHead 日志（`/tmp/ray/session_latest/logs/` 下 `dashboard*` 相关日志）
- 用 `py-spy dump --pid 464` 抓取 Python 堆栈，定位热点代码
- 重启 DashboardHead（`ray stop --dashboard` 然后 `ray start --head`）

### 28.4 完整进程树

```
PID 1 (init/systemd)
  └── raylet (C++ 进程)
        ├── DashboardAgent (python agent.py, PID?)
        │     ├── HTTP server :52365
        │     ├── gRPC server :<动态>
        │     └── Prometheus exporter :<动态>
        │
        └── runtime_env_agent (python, PID?)

PID 274 (DashboardHead 主进程, python dashboard.py)
  ├── HTTP server :8265 (对外 API 网关 + Web UI)
  ├── Prometheus metrics :44227
  ├── UsageStatsHead (in-process)
  │
  └── SubprocessModule 子进程 (通过 Unix socket 代理):
        ├── PID 460: MetricsHead-0     (0% CPU)
        ├── PID 461: DataHead-0       (0% CPU)
        ├── PID 462: EventHead-0       (0% CPU)
        ├── PID 463: JobHead-0         (0% CPU)
        ├── PID 464: NodeHead-0        (99% CPU ← 异常!)
        ├── PID 465: ReportHead-0      (0% CPU)
        ├── PID 466: ServeHead-0       (0% CPU)
        ├── PID 467: StateHead-0       (0% CPU)
        └── PID 468: TrainHead-0       (0% CPU)
```

### 28.5 SubprocessModule 隔离设计

DashboardHead 将重负载模块（StateHead、JobHead、ReportHead 等）放在**独立子进程**中运行，提供**进程级隔离**：
- 如果某个子进程 crash，DashboardHead 可以重启它，不需要整个 dashboard 停机
- 子进程有自己的 Python 解释器和事件循环，互不影响
- 子进程通过 Unix socket（`aiohttp.UnixConnector`）与主进程的 HTTP server 通信

这就是为什么 NodeHead 99% CPU 不会拖慢其他 Head 模块——它在自己的进程中，不共享事件循环。
