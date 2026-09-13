# Ray Serve LLM 推理架构详解

> 本文档详细记录 kray 推理模块的完整架构，包括 SGLang/vLLM 引擎运行方式、
> Placement Group 资源分配、Gateway 请求链路、多引擎支持机制，以及关键代码逻辑。

---

## 1. 整体架构

```
┌─────────────────────────────────────────────────────────────────────┐
│ 业务侧 (hetu)                                                       │
│                                                                     │
│  client.py                                                          │
│  ├── _build_vllm_chat_payload()  # OpenAI messages 格式构造           │
│  ├── image_data_url()            # ffmpeg resize + base64           │
│  └── GrpcModelClient                                                  │
│      └── stub.Chat(ChatRequest(model, body=json))                   │
│                         │                                           │
└─────────────────────────┼───────────────────────────────────────────┘
                          │ gRPC (KESS 服务发现)
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│ InferenceGateway (KESS gRPC servicer)                               │
│                                                                     │
│  Chat(req)        → _dispatch_llm("chat", req)                     │
│  Completions(req) → _dispatch_llm("completions", req)              │
│  Embeddings(req)  → _dispatch_llm("embeddings", req)               │
│  StreamChat(req)  → _dispatch_llm_stream("chat", req)              │
│  StreamCompletions(req) → _dispatch_llm_stream("completions", req) │
│                                                                     │
│  _dispatch_llm:                                                     │
│    1. _convert_request(): protobuf body → Pydantic request object  │
│    2. _resolve_llm_handle(request.model) → Ray Serve Handle        │
│    3. asyncio.run_coroutine_threadsafe(                            │
│         handle.{method}.remote(pydantic_request))                  │
│    4. _convert_response(): Pydantic response → protobuf response   │
│    5. finally: record metrics (latency/error/ongoing)              │
│                                                                     │
│  KESS gRPC registration via KessRegistrar                           │
└─────────────────────────┼───────────────────────────────────────────┘
                          │ Ray Serve Handle (RPC)
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│ LLM Server Deployment                                               │
│                                                                     │
│  ┌─ vLLM 路径 ──────────────────────────────────────────────────┐  │
│  │  LLMServer(LLMServerProtocol)                                 │  │
│  │    └── self.engine = VLLMEngine(llm_config)                   │  │
│  │          └── vLLM worker processes                            │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                                                                     │
│  ┌─ SGLang 路径 ─────────────────────────────────────────────────┐  │
│  │  SGLangServer (独立 server class)                             │  │
│  │    └── self.engine = RayEngine(**engine_kwargs)              │  │
│  │          └── SchedulerActor × tp*pp (每个 num_gpus=1)        │  │
│  └───────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 2. 多引擎支持架构

### 2.1 Ray 社区版的两层可插拔设计

Ray 社区版通过**两层可插拔设计**支持多种推理引擎：

| 层级 | 机制 | 说明 |
|---|---|---|
| **Server 层** | `LLMConfig.server_cls` | 每个 model 可指定不同 server class |
| **Engine 层** | `LLMServer.engine_cls` | LLMServer 内部可替换引擎实现 |

**Server 层**是主要的多引擎机制。不同的 `server_cls` 完全独立：
- `LLMServer`：vLLM 的 shim 层，内部委托 `VLLMEngine`
- `SGLangServer`：独立实现，直接包装 SGLang 的 `RayEngine`
- `DPServer` / `PDDecodeServer`：数据并行/预填充分离的扩展

**Engine 层**是 `LLMServer` 内部的二次扩展点：
- 当前只有 `VLLMEngine`，但 `LLMEngine` ABC 设计了 `start()`/`chat()`/`completions()` 等抽象方法
- `SGLangServer` **不使用**这个 Engine 层，它直接管理 SGLang RayEngine

### 2.2 LLMConfig 中的 server_cls 解析

```python
# ray/llm/_internal/serve/core/configs/llm_config.py

class LLMConfig(BaseModelExtended):
    server_cls: Optional[Union[str, Any]] = Field(
        default=None,
        description="The server class to use for the LLM deployment. "
        "Can be a string path or a class reference."
    )

    @field_validator("server_cls")
    @classmethod
    def validate_server_cls(cls, value):
        if isinstance(value, str):
            return load_class(value)  # 字符串 → 动态导入类
        return value
```

当 `server_cls` 是字符串时（如 `"ray.llm._internal.serve.engines.sglang.sglang_engine.SGLangServer"`），
`validate_server_cls` 通过 `load_class()` 动态导入模块并返回类对象。

### 2.3 start.py 的引擎解析

```python
# ray/serve/inference/start.py

_SERVER_CLS_ALIASES = {
    "vllm": "ray.serve.llm.LLMServer",
    "sglang": "ray.llm._internal.serve.engines.sglang.sglang_engine.SGLangServer",
}

def _resolve_engine(raw: str) -> Dict:
    if not raw:
        return {}
    lower = raw.lower().strip()
    if lower in _SERVER_CLS_ALIASES:
        return dict(_SERVER_CLS_ALIASES[lower])
    return {"server_cls": raw}  # 自定义类路径直接透传
```

**关键设计**：`--engine sglang` 被解析为 `server_cls` 字符串，传入 `LLMConfig`，
由 `validate_server_cls` 转换为类对象。Gateway 不感知引擎类型。

### 2.4 builder.py 的多引擎构建

```python
# ray/serve/inference/builder.py

def _build_llm_deployment(llm_config):
    from ray.serve.llm import LLMServer

    server_cls = llm_config.server_cls or LLMServer
    serve_options = server_cls.get_deployment_options(llm_config)
    return serve.deployment(server_cls).options(**serve_options).bind(llm_config)
```

**每个 LLMConfig 可以有不同的 `server_cls`**，`get_deployment_options()` 各自实现：
- `LLMServer.get_deployment_options()` → vLLM 的 N 个小 bundle
- `SGLangServer.get_deployment_options()` → SGLang 的 1 个大 bundle

### 2.5 社区版 build_llm_deployment 对比

```python
# ray/llm/_internal/serve/core/server/builder.py (社区版)

DEFAULT_DEPLOYMENT_OPTIONS = {
    "max_ongoing_requests": DEFAULT_MAX_ONGOING_REQUESTS,
    "health_check_period_s": DEFAULT_HEALTH_CHECK_PERIOD_S,
    "health_check_timeout_s": DEFAULT_HEALTH_CHECK_TIMEOUT_S,
    "autoscaling_config": {"target_ongoing_requests": DEFAULT_MAX_TARGET_ONGOING_REQUESTS},
}

def build_llm_deployment(llm_config, *, name_prefix=None, bind_kwargs=None,
                         override_serve_options=None, deployment_cls=None):
    deployment_cls = deployment_cls or llm_config.server_cls or LLMServer
    deployment_options = deployment_cls.get_deployment_options(llm_config)
    deployment_name = deployment_options.get("name", _get_deployment_name(llm_config))
    if name_prefix:
        deployment_options["name"] = name_prefix + deployment_name
    if override_serve_options:
        deployment_options.update(override_serve_options)
    deployment_options = maybe_apply_llm_deployment_config_defaults(
        DEFAULT_DEPLOYMENT_OPTIONS, deployment_options
    )
    _maybe_setup_kv_aware_routing(deployment_options, llm_config)
    return serve.deployment(deployment_cls, **deployment_options).bind(
        llm_config=llm_config, **bind_kwargs
    )
```

**与 kray 版差异**：社区版多了 `DEFAULT_DEPLOYMENT_OPTIONS` 默认值、`name_prefix`、
`override_serve_options`、`maybe_apply_llm_deployment_config_defaults` 和 KV-aware routing。

---

## 3. SGLang 运行方式

### 3.1 四层架构

```
Layer 0: Ray Serve 调度器
  │  读 placement_group_bundles → 创建 PG
  │  分配 SGLangServer Actor 到 bundle 0 (gang_pg_index=0)
  ▼
Layer 1: SGLangServer Actor (Serve replica 进程)
  │  num_gpus=0 → 不消耗 GPU 配额，不设 CUDA_VISIBLE_DEVICES
  │  async __init__() → await self.start() → self._make_engine()
  ▼
Layer 2: RayEngine (普通 Python 对象，不是 Ray Actor)
  │  由 SGLangServer 在进程内直接实例化
  │  创建 SchedulerActor，通过 PlacementGroupSchedulingStrategy
  │  指定每个 SchedulerActor 的 bundle_index + local_gpu_idx
  ▼
Layer 3: SchedulerActor × tp*pp (每个 @ray.remote(num_gpus=1))
  │  实际执行推理
  │  使用 torch.cuda.set_device(local_gpu_idx) 绑定物理 GPU
  ▼
GPU 硬件
```

### 3.2 SGLangServer 初始化流程

```python
# ray/llm/_internal/serve/engines/sglang/sglang_engine.py

class SGLangServer:
    async def __init__(self, llm_config: LLMConfig):
        self._llm_config = llm_config
        self.engine_kwargs = copy.deepcopy(llm_config.engine_kwargs)
        self.engine = None
        self._health_timeout_s = llm_config.deployment_config.get(
            "health_check_timeout_s", 30
        )
        self._abort_timeout_s = llm_config.experimental_configs.get(
            "sglang_abort_timeout_s", 30
        )
        # ... 校验 abort_timeout_s ...
        await self.start()  # 立即启动引擎

    async def start(self):
        self.engine = SGLangServer._make_engine(self)

    def _make_engine(self):
        # 1. 校验 CUDA_VISIBLE_DEVICES 未被 Ray remap
        visible = os.environ.get("CUDA_VISIBLE_DEVICES")
        if visible is not None and visible.split(",") != [
            str(i) for i in range(len(visible.split(",")))
        ]:
            raise ValueError(
                "SGLang RayEngine 0.5.19 requires unset or contiguous zero-based "
                "CUDA_VISIBLE_DEVICES; use container-level GPU device assignment"
            )

        # 2. 校验当前在 PG 内
        if ray.util.get_current_placement_group() is None:
            raise RuntimeError("SGLangServer must inherit a Serve placement group")

        # 3. 校验 SGLang 版本
        if sglang.__version__ != SGLANG_VERSION:
            raise RuntimeError(...)

        # 4. 自动注入 model_path
        model_source = self._llm_config.model_loading_config.model_source
        model_path = self.engine_kwargs.get("model_path")
        if model_path is None:
            self.engine_kwargs["model_path"] = model_source or self._llm_config.model_id
        self.engine_kwargs.setdefault("served_model_name", self._llm_config.model_id)

        # 5. 创建 ManagedRayEngine（继承 RayEngine，拦截 SchedulerActor 创建）
        class ManagedRayEngine(RayEngine):
            @classmethod
            def _launch_scheduler_processes(cls, *args, **kwargs):
                result = super()._launch_scheduler_processes(*args, **kwargs)
                owner.engine._scheduler_init_result = result[0]
                return result

        self.engine = ManagedRayEngine.__new__(ManagedRayEngine)
        self.engine._scheduler_init_result = SimpleNamespace(scheduler_actors=[])
        self.engine.tokenizer_manager = None
        ManagedRayEngine.__init__(self.engine, **self.engine_kwargs)
```

### 3.3 SGLangServer.chat() 请求处理

```python
# sglang_engine.py:674

async def chat(self, request: ChatCompletionRequest,
               raw_request_info=None) -> AsyncGenerator[str | ChatCompletionResponse, None]:
    # 1. 从 request 构建 chat messages 和 prompt
    chat_messages = self._build_chat_messages(request.messages)
    prompt = self._render_chat_prompt(request, chat_messages)

    # 2. n>1 时拆分为多个独立请求
    if request.n != 1:
        for index in range(request.n):
            single = request.model_copy(update={"n": 1})
            async with aclosing(self.chat(single, raw_request_info)) as results:
                async for result in results:
                    if request.stream:
                        # 流式: 修改 SSE chunk 的 index
                        chunk = json.loads(result[6:])
                        chunk.update(id=gen_id, created=created)
                        chunk["choices"][0]["index"] = index
                        yield f"data: {json.dumps(chunk)}\n\n"
                    else:
                        responses.append(result)
        # 非流式: 合并所有 response
        if not request.stream:
            response = responses[0]
            response.choices = [...]
            yield response
        return

    # 3. n=1 + stream=True: 流式输出 SSE
    if request.stream:
        async with aclosing(self._stream_generate(request, prompt)) as stream:
            async for delta_text, finish_reason in stream:
                delta = {"content": delta_text}
                if first_chunk:
                    delta["role"] = "assistant"
                yield self._build_sse_chunk(...)
        return

    # 4. n=1 + stream=False: 非流式输出
    metadata = await self._generate_and_extract_metadata(request, prompt)
    resp = ChatCompletionResponse(...)
    yield resp
```

**输出格式**：
- `stream=True`：yield SSE 字符串 `"data: {json}\n\n"`
- `stream=False`：yield `ChatCompletionResponse` pydantic 对象

### 3.4 SGLangServer.get_deployment_options() 资源分配

```python
# sglang_engine.py:978-1041

@classmethod
def get_deployment_options(cls, llm_config: LLMConfig):
    deployment_options = copy.deepcopy(llm_config.deployment_config)
    pg_config = llm_config.placement_group_config or {}
    ray_actor_options = deployment_options.get("ray_actor_options", {})

    tp_size = llm_config.engine_kwargs.get("tp_size", 1)
    pp_size = llm_config.engine_kwargs.get("pp_size", 1)
    num_devices = tp_size * pp_size

    # --- 单节点: 自动生成 1 个大 bundle ---
    if "placement_group_bundles" not in pg_config:
        replica_bundle = {
            "CPU": ray_actor_options.get("num_cpus", 1),
            "GPU": num_devices,  # tp*pp 个 GPU 预留到同一个 bundle
        }

        # 用户设了 num_gpus>0? 累加到 bundle 后清零
        user_num_gpus = ray_actor_options.get("num_gpus", 0)
        if user_num_gpus:
            replica_bundle["GPU"] += user_num_gpus
            ray_actor_options["num_gpus"] = 0  # SGLangServer Actor 不消耗 GPU

        # 合并用户自定义资源
        replica_bundle.update(ray_actor_options.get("resources", {}))
        if "memory" in ray_actor_options:
            replica_bundle["memory"] = ray_actor_options["memory"]

        pg_bundles = [replica_bundle]
        pg_strategy = "STRICT_PACK"  # 所有资源在同一节点

    # --- 多节点: 用户提供 bundle 列表 ---
    else:
        pg_bundles = pg_config.get("placement_group_bundles")
        pg_strategy = pg_config.get("placement_group_strategy", "PACK")

    deployment_options.update({
        "placement_group_bundles": pg_bundles,
        "placement_group_strategy": pg_strategy,
    })

    # --- Runtime environment ---
    runtime_env = ray_actor_options.setdefault("runtime_env", {})
    if ENABLE_WORKER_PROCESS_SETUP_HOOK:
        runtime_env.setdefault("worker_process_setup_hook", ...)
    if llm_config.runtime_env:
        runtime_env.update(llm_config.runtime_env)

    # --- NOSET_CUDA_VISIBLE_DEVICES=1 ---
    env = runtime_env.setdefault("env_vars", {})
    env["RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES"] = "1"

    deployment_options["ray_actor_options"] = ray_actor_options
    return deployment_options
```

---

## 4. vLLM 运行方式

### 4.1 vLLM 架构

```
Layer 0: Ray Serve 调度器
  │  读 placement_group_bundles → 创建 PG
  │  分配 LLMServer Actor 到 bundle 0
  ▼
Layer 1: LLMServer Actor (Serve replica 进程)
  │  num_gpus=0 (coordinator 不消耗 GPU)
  │  内部创建 VLLMEngine
  ▼
Layer 2: VLLMEngine (LLMEngine ABC 实现)
  │  self._init_engine() → 创建 vLLM engine
  │  vLLM Worker 是独立 Ray Actor，每个占一个 bundle
  ▼
Layer 3: vLLM Worker × num_devices (每个独立 Ray Actor)
  │  通过 Ray Serve PG bundle 自动调度
  ▼
GPU 硬件
```

### 4.2 LLMServer.get_deployment_options() 资源分配

```python
# ray/llm/_internal/serve/core/server/llm_server.py:746-801

@classmethod
def get_deployment_options(cls, llm_config: "LLMConfig"):
    engine_config = llm_config.get_engine_config()  # VLLMEngineConfig
    deployment_options = copy.deepcopy(llm_config.deployment_config)
    ray_actor_options = deployment_options.get("ray_actor_options", {})

    if not engine_config.accelerator.requires_deferred_placement_group:
        # 1. 构造 replica actor 资源 (bundle 0)
        replica_actor_resources = {
            "CPU": ray_actor_options.get("num_cpus", 1),
            "GPU": ray_actor_options.get("num_gpus", 0),  # 用户指定
            **ray_actor_options.get("resources", {}),
        }

        # 2. 合并 replica actor + child worker bundles
        pg_bundles = _merge_replica_actor_and_child_actor_bundles(
            engine_config.placement_bundles,  # N 个 {"GPU": 1} 的小 bundle
            replica_actor_resources,          # {"CPU": 1, "GPU": 0}
        )
        # 结果: [{"CPU":1, "GPU":1}, {"GPU":1}, {"GPU":1}, {"GPU":1}]
        #         ↑ bundle 0: LLMServer + 第一个 Worker 共享

        deployment_options.update({
            "placement_group_bundles": pg_bundles,
            "placement_group_strategy": engine_config.placement_strategy,
        })

    # 3. Runtime environment
    ray_actor_options["runtime_env"] = {...}
    deployment_options["ray_actor_options"] = ray_actor_options
    return deployment_options
```

### 4.3 vLLM 的 placement_bundles 生成

```python
# ray/llm/_internal/serve/engines/vllm/vllm_models.py:267-297

@property
def placement_bundles(self) -> List[Dict[str, float]]:
    if self.placement_group_config:
        # 用户指定了 bundle_per_worker → 展开为 num_devices 个
        bundle_per_worker = self.placement_group_config.get("bundle_per_worker")
        if bundle_per_worker is not None:
            bundles = []
            for _ in range(self.num_devices):
                bundle = bundle_per_worker.copy()
                if self.accelerator_type:
                    bundle.setdefault(res_key, 0.001)
                bundles.append(bundle)
            return bundles
        # 用户指定了显式 bundles 列表
        explicit_bundles = self.placement_group_config.get("bundles") or []
        ...

    # 默认: 根据 accelerator 生成
    return self.accelerator.default_bundles(
        num_devices=self.num_devices, accelerator_type_str=self.accelerator_type
    )
```

### 4.4 NVIDIA GPU 默认 bundles

```python
# GPU 默认: num_devices 个 {"GPU": 1} 的小 bundle
# CPU 默认: num_devices 个 {"CPU": 1} 的小 bundle
```

### 4.5 LLMServer.chat() 请求处理

```python
# ray/llm/_internal/serve/core/server/llm_server.py:376-401

async def chat(self, request: "ChatCompletionRequest",
               raw_request_info=None):
    return await self._run_request(
        request,
        engine_method="chat",
        batch_output_stream=True,  # 流式时批处理输出
        raw_request_info=raw_request_info,
    )

# _run_request → getattr(self.engine, engine_method)(request)
# 即调用 VLLMEngine.chat(request)
```

---

## 5. SGLang vs vLLM 资源分配对比

### 5.1 核心差异

| | SGLang | vLLM |
|---|---|---|
| **Worker 创建** | RayEngine 自己创建 SchedulerActor | vLLM Worker 是独立 Ray Actor |
| **Bundle 模式** | 1个大 bundle `{"CPU":1, "GPU":tp*pp}` | N个小 bundle `{"GPU":1}` × N |
| **GPU 分配** | RayEngine 内部用 `PlacementGroupSchedulingStrategy` 二次分配 | Ray Serve PG bundle 自动分配 |
| **`num_gpus`** | Actor `=0`，SchedulerActor `=1` | 每个 Worker Actor `=1` |
| **NOSET_CUDA** | **必须=1**（SchedulerActor 需全局 ordinal） | 不需要（Worker 用 remap 后的 ordinal） |
| **根本原因** | SGLang RayEngine 0.5.19 用 `torch.cuda.set_device(全局ordinal)` | vLLM Worker 用 `cuda:0`（remap 后即可） |

### 5.2 资源分配流程图

```
══════════════════════════ SGLang (tp=4, 1 replica) ══════════════════════════

start.py:
  --engine sglang --tp-size 4
  → server_cls = "ray.llm...SGLangServer"
  → LLMConfig(engine_kwargs={"tp_size": 4})

get_deployment_options():
  num_devices = 4
  replica_bundle = {"CPU": 1, "GPU": 4}
  ray_actor_options["num_gpus"] = 0
  env["RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES"] = "1"

Ray Serve 调度器:
  PG = [{"CPU":1, "GPU":4}]
  找一个有 ≥4 GPU 的节点
  SGLangServer Actor → bundle 0 (num_gpus=0)

SGLangServer._make_engine():
  RayEngine 创建 4 个 SchedulerActor:
    SchedulerActor(0) → bundle_index=0, local_gpu_idx=0, num_gpus=1
    SchedulerActor(1) → bundle_index=0, local_gpu_idx=1, num_gpus=1
    SchedulerActor(2) → bundle_index=0, local_gpu_idx=2, num_gpus=1
    SchedulerActor(3) → bundle_index=0, local_gpu_idx=3, num_gpus=1
  每个 SchedulerActor 看到节点全部 GPU (NOSET_CUDA=1)
  用 torch.cuda.set_device(local_gpu_idx) 绑定物理 GPU


══════════════════════════ vLLM (tp=4, 1 replica) ══════════════════════════

start.py:
  --engine vllm --num-gpus-per-replica 4
  → server_cls = None (默认 LLMServer)
  → ray_actor_options["num_gpus"] = 4

get_deployment_options():
  engine_config.placement_bundles = [{"GPU":1}, {"GPU":1}, {"GPU":1}, {"GPU":1}]
  replica_actor_resources = {"CPU":1, "GPU":4}
  _merge: [{"CPU":1, "GPU":1}, {"GPU":1}, {"GPU":1}, {"GPU":1}]
  #         ↑ bundle 0: LLMServer + Worker 0 共享
  #         不设 NOSET_CUDA

Ray Serve 调度器:
  PG = [{"CPU":1,"GPU":1}, {"GPU":1}, {"GPU":1}, {"GPU":1}]
  LLMServer Actor → bundle 0
  vLLM Worker 0 → bundle 0 (与 LLMServer 共享)
  vLLM Worker 1 → bundle 1
  vLLM Worker 2 → bundle 2
  vLLM Worker 3 → bundle 3
  每个 Worker 看到 CUDA_VISIBLE_DEVICES=0 (Ray remap)
  用 cuda:0 即可
```

### 5.3 多节点场景 (SGLang pp=2, nnodes=2)

```
start.py:
  --engine sglang --tp-size 2 --pp-size 2 --nnodes 2
  --placement-group-config '{
    "placement_group_bundles": [{"CPU":1,"GPU":2}, {"GPU":2}],
    "placement_group_strategy": "STRICT_SPREAD"
  }'

get_deployment_options():
  pg_bundles = [{"CPU":1,"GPU":2}, {"GPU":2}]  (用户提供)
  pg_strategy = "STRICT_SPREAD"

Ray Serve 调度器:
  PG = [bundle 0: {"CPU":1,"GPU":2}, bundle 1: {"GPU":2}]
  找两个节点，每个有 ≥2 GPU
  SGLangServer Actor → bundle 0 (节点 A)
  bundle 1 → 节点 B (STRICT_SPREAD)

RayEngine 创建 4 个 SchedulerActor:
  SchedulerActor(0) → bundle_index=0, local_gpu_idx=0  ← 节点 A
  SchedulerActor(1) → bundle_index=0, local_gpu_idx=1  ← 节点 A
  SchedulerActor(2) → bundle_index=1, local_gpu_idx=0  ← 节点 B
  SchedulerActor(3) → bundle_index=1, local_gpu_idx=1  ← 节点 B
```

---

## 6. Placement Group 详解

### 6.1 Bundle 的含义

**Bundle = 资源预留单元**。告诉 Ray 调度器"我需要一个有这么多资源的节点 slot"。

| 概念 | 含义 | 实际作用 |
|---|---|---|
| `Bundle = {"CPU":1, "GPU":4}` | 预留 1 CPU + 4 GPU 的资源 | Ray 把 PG 调度到有 ≥4 GPU 的节点，锁定这些资源 |
| `Strategy = "STRICT_PACK"` | 所有 bundle 必须在同一节点 | 单节点场景，确保所有资源在一台机器上 |
| `Strategy = "STRICT_SPREAD"` | 每个 bundle 在不同节点 | 多节点场景，每节点一个 bundle |

**Bundle ≠ Actor**。Bundle 只是资源声明，Actor 运行在 Bundle 上。

### 6.2 一个 PG 可以有多个 bundle

```python
# 单 bundle PG (SGLang 单节点)
PG = ray.placement_group([{"CPU":1, "GPU":4}])

# 多 bundle PG (vLLM tp=4)
PG = ray.placement_group([{"GPU":1}, {"GPU":1}, {"GPU":1}, {"GPU":1}])

# 多 bundle 多节点 (SGLang pp=2, nnodes=2)
PG = ray.placement_group(
    [{"CPU":1, "GPU":2}, {"GPU":2}],
    strategy="STRICT_SPREAD"
)
```

### 6.3 Actor 如何绑定到 bundle

Actor 通过 `PlacementGroupSchedulingStrategy` 指定运行在哪个 bundle 上：

```python
# Ray Serve 调度器 (deployment_state.py:3833)
scheduling_request = new_deployment_replica.start(
    deployment_info,
    gang_placement_group=gang_pg,
    gang_pg_index=bundle_index * bundles_per_replica,  # 起始 bundle index
)

# 转换为 Ray 调度策略 (deployment_scheduler.py:589-593)
scheduling_strategy = PlacementGroupSchedulingStrategy(
    placement_group=placement_group,
    placement_group_bundle_index=scheduling_request.gang_pg_index,
    placement_group_capture_child_tasks=True,  # 子 Actor 自动继承 PG
)
```

### 6.4 Ray Serve PG 创建流程

```python
# deployment_scheduler.py:753-812

# 每个 replica 的 per_replica_bundles 被展开为 gang PG
# Case 1: SGLang (per_replica_bundles=[{"CPU":1,"GPU":4}])
gang_pgs = [{"CPU":1, "GPU":4}]  # 1 个 bundle

# Case 2: vLLM (per_replica_bundles=[{"GPU":1},{"GPU":1},{"GPU":1},{"GPU":1}])
gang_pgs = [{"GPU":1}, {"GPU":1}, {"GPU":1}, {"GPU":1}]  # 4 个 bundle

# 创建 PG:
pg = ray.placement_group(bundles, strategy=strategy)
# SGLang: strategy="STRICT_PACK" → 所有 bundle 在同一节点
# vLLM: strategy 由 engine_config.placement_strategy 决定
```

---

## 7. num_gpus 与 CUDA_VISIBLE_DEVICES 详解

### 7.1 Ray 的 GPU 分配机制

当 Actor 设 `num_gpus=N` 时，Ray 做两件事：

1. **资源扣减**：从 PG bundle 的 GPU 配额中扣减 N
2. **设环境变量**：设 `CUDA_VISIBLE_DEVICES=0,1,...,N-1`（remap 为从 0 开始）

```python
# ray/_private/accelerators/nvidia_gpu.py

NOSET_CUDA_VISIBLE_DEVICES_ENV_VAR = "RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES"

def set_current_process_visible_accelerator_ids(visible_cuda_devices):
    if env_bool(NOSET_CUDA_VISIBLE_DEVICES_ENV_VAR, False):
        return  # 设了 NOSET=1 → 跳过，不设 CUDA_VISIBLE_DEVICES
    os.environ["CUDA_VISIBLE_DEVICES"] = ",".join([str(i) for i in visible_cuda_devices])
```

### 7.2 SGLang 为什么需要 num_gpus=0

```
假设节点有 GPU 0,1,2,3,4,5,6,7

SGLangServer (num_gpus=0):
  → Ray 不给它设 CUDA_VISIBLE_DEVICES
  → SGLangServer 看到节点全部 GPU

SchedulerActor(0) (num_gpus=1, NOSET_CUDA=1):
  → Ray 也不给它设 CUDA_VISIBLE_DEVICES
  → SchedulerActor 看到节点全部 GPU
  → torch.cuda.set_device(0) ✓ (全局 ordinal)

SchedulerActor(0) (num_gpus=1, 没有 NOSET_CUDA):
  → Ray 设 CUDA_VISIBLE_DEVICES=0 (remap)
  → SchedulerActor 只看到 1 个 GPU，编号 0
  → 但 RayEngine 用 torch.cuda.set_device(全局ordinal)
  → 如果全局 ordinal 是 4，set_device(4) 失败! ✗
```

### 7.3 vLLM 为什么不需要 NOSET_CUDA

```
vLLM Worker (num_gpus=1, 无 NOSET_CUDA):
  → Ray 设 CUDA_VISIBLE_DEVICES=0
  → Worker 只看到 1 个 GPU，编号 0
  → vLLM 使用 cuda:0 (remap 后的)
  → 正常工作 ✓
```

### 7.4 num_gpus 累加逻辑

```python
# sglang_engine.py:1001-1004

user_num_gpus = ray_actor_options.get("num_gpus", 0)
if user_num_gpus:
    replica_bundle["GPU"] += user_num_gpus  # 预留更多 GPU
    ray_actor_options["num_gpus"] = 0         # 但 Actor 不消耗
```

**为什么这样？** 如果用户设了 `ray_actor_options.num_gpus=4`：
- 不累加：bundle 只有 `GPU: tp*pp`，不够用
- 不清零：SGLangServer Actor 设了 `num_gpus=4`，Ray 会给它设 `CUDA_VISIBLE_DEVICES` 并扣减 4 GPU，内部 SchedulerActor 再 `num_gpus=1` 时无 GPU 可用

**在 kray 的 start.py 中**，SGLang 有 `server_cls`，所以 `num_gpus_per_replica` 不会进入 `ray_actor_options`，这个分支实际不触发。

---

## 8. Gateway 请求链路详解

### 8.1 Proto 接口

```protobuf
// llm_inference.proto
service LLMInference {
  rpc Chat(ChatRequest) returns (ChatResponse);
  rpc Completions(CompletionsRequest) returns (CompletionsResponse);
  rpc Embeddings(EmbeddingsRequest) returns (EmbeddingsResponse);
  rpc Score(ScoreRequest) returns (ScoreResponse);
  rpc Tokenize(TokenizeRequest) returns (TokenizeResponse);
  rpc Detokenize(DetokenizeRequest) returns (DetokenizeResponse);
  rpc StreamChat(ChatRequest) returns (stream ChatResponse);
  rpc StreamCompletions(CompletionsRequest) returns (stream CompletionsResponse);
}

message ChatRequest {
  string model = 1;   // 模型 ID (用于路由)
  bytes body = 2;     // JSON 编码的 OpenAI 请求体
}

message ChatResponse {
  Status status = 1;   // 状态码
  bytes body = 2;      // JSON 编码的 OpenAI 响应体
}
```

### 8.2 请求转换 (protobuf → Pydantic)

```python
# gateway.py:79-108

def _convert_request(method_name: str, request: Any, stream: bool = False):
    body_bytes = getattr(request, "body", None)  # protobuf body bytes
    model = getattr(request, "model", "") or ""  # protobuf model string

    if body_bytes:
        try:
            body_dict = json.loads(body_bytes)     # JSON → dict
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in request body for {method_name}: {e}")
    else:
        body_dict = {}

    body_dict.setdefault("model", model)
    body_dict["stream"] = stream

    # 自动路由: embeddings 含 messages → EmbeddingChatRequest
    if method_name == "embeddings" and "messages" in body_dict:
        resolved_method = "embeddings_chat"
    elif method_name == "tokenize" and "messages" in body_dict:
        resolved_method = "tokenize_chat"
    else:
        resolved_method = method_name

    cls = _get_request_cls(resolved_method)  # 查 Pydantic 类
    return cls(**body_dict)                   # 构造 Pydantic 对象
```

### 8.3 响应转换 (Pydantic → protobuf)

```python
# gateway.py:111-130

def _convert_response(method_name: str, result: Any, request: Any):
    response_cls = _get_pb_response_cls(method_name)

    if result is None:
        return response_cls(status=Status.ERROR, body=b"")

    # 根据 result 类型序列化
    if hasattr(result, "model_dump_json"):         # Pydantic v2
        body_bytes = result.model_dump_json().encode("utf-8")
    elif isinstance(result, (dict, list)):          # dict/list
        body_bytes = json.dumps(result).encode("utf-8")
    elif isinstance(result, str):                    # SSE 字符串
        body_bytes = result.encode("utf-8")
    else:
        body_bytes = json.dumps(str(result)).encode("utf-8")

    return response_cls(status=Status.SUCCESS, body=body_bytes)
```

### 8.4 请求类映射缓存

```python
# gateway.py:24-53

_CLS_MAP_CACHE: Dict[str, type] = {}

def _ensure_cls_cache():
    if _CLS_MAP_CACHE:
        return
    from ray.serve.llm.openai_api_models import (
        ChatCompletionRequest,
        CompletionRequest,
        EmbeddingCompletionRequest,
        EmbeddingChatRequest,
        ScoreRequest,
        TokenizeCompletionRequest,
        TokenizeChatRequest,
        DetokenizeRequest,
    )
    _CLS_MAP_CACHE["chat"] = ChatCompletionRequest
    _CLS_MAP_CACHE["completions"] = CompletionRequest
    _CLS_MAP_CACHE["embeddings"] = EmbeddingCompletionRequest
    _CLS_MAP_CACHE["embeddings_chat"] = EmbeddingChatRequest
    _CLS_MAP_CACHE["score"] = ScoreRequest
    _CLS_MAP_CACHE["tokenize"] = TokenizeCompletionRequest
    _CLS_MAP_CACHE["tokenize_chat"] = TokenizeChatRequest
    _CLS_MAP_CACHE["detokenize"] = DetokenizeRequest
```

### 8.5 Unary 请求分发

```python
# gateway.py:277-287, 289-380

async def _call_llm_handle(self, method_name, handle, request):
    llm_request = _convert_request(method_name, request, stream=False)
    handle_method = getattr(handle, method_name)
    gen = await handle_method.remote(llm_request)
    try:
        async for chunk in gen:     # SGLangServer.chat() 总是 AsyncGenerator
            return chunk            # 取第一个 chunk 就 return
    finally:
        if hasattr(gen, "aclose"):
            await gen.aclose()      # 清理未消费的 generator
    return None

def _dispatch_llm(self, method_name, request, context=None):
    # 1. 递增 ongoing_requests
    # 2. try: resolve_handle → run_coroutine_threadsafe → future.result(timeout)
    # 3. _convert_response()
    # 4. except: TimeoutError / ValueError / Exception
    # 5. finally: 递减 ongoing_requests, 记录 metrics
```

### 8.6 Streaming 请求分发 (async→sync 桥接)

```python
# gateway.py:383-465

def _dispatch_llm_stream(self, method_name, request, context=None):
    result_queue: queue.Queue = queue.Queue()
    sentinel = object()

    async def _bridge():
        # 异步协程: 从 LLM Server 读取 stream → 写入 queue
        try:
            async for chunk in self._call_llm_handle_stream(method_name, handle, request):
                result_queue.put(chunk)
        except Exception as exc:
            result_queue.put(exc)
        finally:
            result_queue.put(sentinel)

    # 启动异步桥接
    asyncio.run_coroutine_threadsafe(_bridge(), self._loop)

    # 同步 generator: 从 queue 读取 → yield (gRPC 需要 sync generator)
    while True:
        item = result_queue.get(timeout=per_model_timeout)
        if item is sentinel:
            break
        if isinstance(item, Exception):
            raise item
        yield item

async def _call_llm_handle_stream(self, method_name, handle, request):
    llm_request = _convert_request(method_name, request, stream=True)
    gen = await getattr(handle, method_name).remote(llm_request)
    try:
        async for chunk in gen:
            # SGLang stream: chunk 是 SSE 字符串 "data: {json}\n\n"
            # vLLM stream: chunk 是 ChatCompletionStreamResponse pydantic 对象
            if isinstance(chunk, str):
                yield response_cls(status=Status.SUCCESS, body=chunk.encode("utf-8"))
            elif hasattr(chunk, "model_dump_json"):
                yield response_cls(status=Status.SUCCESS, body=chunk.model_dump_json().encode("utf-8"))
            ...
    finally:
        if hasattr(gen, "aclose"):
            await gen.aclose()
```

**为什么需要 Queue 桥接？** gRPC 的 server-streaming 方法要求返回 **sync generator**，
但 LLM Server 的 `chat(stream=True)` 返回 **async generator**。
`run_coroutine_threadsafe` 不能处理 async generator（会 TypeError），
所以用 `threading.Queue` 在 async 和 sync 之间桥接。

---

## 9. 请求类型 Duck Typing

### 9.1 问题

Gateway 用 Ray 的 `ChatCompletionRequest` 构造请求，但 SGLangServer.chat() 期望 SGLang 自己的 `ChatCompletionRequest`。

### 9.2 为什么可行

```python
# SGLangServer 导入的是 SGLang 自己的 ChatCompletionRequest:
from sglang.srt.entrypoints.openai.protocol import ChatCompletionRequest

# Gateway 构造的是 Ray 的 ChatCompletionRequest:
from ray.serve.llm.openai_api_models import ChatCompletionRequest

# 但两者 OpenAI 标准字段名相同:
# model, messages, n, stream, temperature, top_p, max_tokens, ...
# SGLang 扩展字段 (top_k, repetition_penalty) 有默认值，不报错
```

### 9.3 社区版也是这样做的

```python
# ray/llm/_internal/serve/core/ingress/ingress.py:401-403

# OpenAiIngress 用的也是 Ray 的 ChatCompletionRequest
# 直接通过 handle.chat.remote(body) 传给 SGLangServer
async for response in getattr(model_handle, call_method).remote(body, raw_request_info):
    yield response
```

**结论**：与社区版行为一致，duck typing 是设计模式而非 bug。SGLang 扩展字段无法通过 Gateway 传递是 protobuf body 字段的限制（proto 只有 `model: str, body: bytes`），不是请求类型问题。

---

## 10. Pipeline Parallelism (pp_size) 与 Tensor Parallelism (tp_size)

### 10.1 概念

| 并行方式 | 含义 | 示例 |
|---|---|---|
| `tp_size` (Tensor Parallelism) | 把**一层**的权重切到多个 GPU 上并行计算 | 8B 模型、tp=4 → 每张卡存 1/4 权重 |
| `pp_size` (Pipeline Parallelism) | 把**不同层**分配到不同 GPU 上，流水线执行 | 32 层模型、pp=2 → GPU0 算 0-15 层，GPU1 算 16-31 层 |

### 10.2 资源计算

```python
num_devices = tp_size * pp_size  # 总 GPU 数
# 大多数单节点场景: tp_size=4, pp_size=1 → 4 GPU
# 多节点场景: tp_size=2, pp_size=2 → 4 GPU, 分布在 2 个节点
```

### 10.3 流水线执行示意

```
单请求流水线 (pp=2, tp=2, 共 4 GPU):

GPU0, GPU1:  Layer 0-15 (tp=2，同一层切两半)
     │        ↓ 中间激活
GPU2, GPU3:  Layer 16-31 (tp=2，同一层切两半)
```

### 10.4 多节点场景

**跨节点 TP**（不常见，延迟高）：
- `tp=8, nnodes=2` → 每台 4 GPU 做 TP，跨机通信每层

**跨节点 PP**（更常见）：
- `pp=2, nnodes=2` → 不同层在不同机器，层间通信少

---

## 11. 已修复问题清单

| # | 文件 | 问题 | 修复 |
|---|---|---|---|
| 1 | `start.py` | SGLang 的 `llm_engine` 错误设为 `"vLLM"` | 删除 `llm_engine` 字段 |
| 2 | `start.py` | 硬编码旧类路径映射 (`ray.serve.llm.sglangserver`) | 删除，只保留 alias |
| 3 | `start.py` | SGLang 时 `--num-gpus-per-replica` 被静默忽略 | 添加 warning |
| 4 | `builder.py` | `from ray.serve.llm import qing` typo | 修正为 `import LLMServer` |
| 5 | `builder.py` | `getattr(llm_config, "server_cls", None)` 绕过 Pydantic | 用 `llm_config.server_cls` |
| 6 | `gateway.py` | `_CLS_MAP_CACHE` 缺少 `EmbeddingChatRequest` | 添加映射 |
| 7 | `gateway.py` | `_convert_request` 中 `json.loads` 无 try/except | 添加 ValueError |
| 8 | `gateway.py` | `_CLS_MAP_CACHE` 重复导入逻辑 | 改为 `_ensure_cls_cache()` |
| 9 | `gateway.py` | `embeddings` 含 `messages` 无法路由到 `EmbeddingChatRequest` | 自动检测路由 |
| 10 | `sglang_engine.py` | `get_deployment_options` 缺少 `NOSET_CUDA_VISIBLE_DEVICES=1` | 恢复设置 |
| 11 | `sglang_engine.py` | `num_gpus` 用户值处理不兼容 | 累加到 bundle 后清零 |
| 12 | `test_sglang_rayengine.py` | `num_gpus` 和 `NOSET_CUDA` 断言缺失 | 恢复断言 |

---

## 12. Ray 社区版 Ingress 的 protobuf↔bytes 转换机制

Ray 社区版 LLM Serve 有两条请求路径：**HTTP (OpenAiIngress)** 和 **gRPC (Generic Serve Proxy)**。
LLM 模块本身 **没有** gRPC ingress——所有 LLM 请求走 HTTP/FastAPI 路径。gRPC ingress 只存在于 Ray Serve 的通用 proxy 层。

### 12.1 HTTP 路径：OpenAiIngress (FastAPI)

```
客户端 HTTP JSON body
  │
  ▼ FastAPI 自动解析
Pydantic 模型 (ChatCompletionRequest, CompletionRequest, ...)
  │
  ▼ OpenAiIngress.chat(body=ChatCompletionRequest, request=Request)
  │
  ▼ _sanitize_chat_completion_request(body)
  │    修复 Pydantic ValidatorIterator 不可 pickle 的问题
  │    将 message.content / tool_calls 从 Iterable → list
  │
  ▼ RawRequestInfo.from_starlette_request(request)
  │    Starlette Request 不可序列化，提取 headers → dataclass
  │
  ▼ model_handle.chat.remote(body, raw_request_info)
  │    [Ray RPC — cloudpickle 序列化 Pydantic 对象]
  │
  ▼ LLMServer.chat(request, raw_request_info)
  │    或 SGLangServer.chat(request, raw_request_info)
  │
  ▼ 响应转换:
  │    非流式: model_dump() → JSONResponse
  │    流式:   model_dump_json() → "data: {json}\n\n" (SSE)
```

**关键代码**：

```python
# ray/llm/_internal/serve/core/ingress/ingress.py:350-404

async def _get_response(self, *, body, call_method, raw_request=None):
    model_id = await self._get_model_id(body.model)
    model_handle = self._get_configured_serve_handle(model_id)

    # 修复 Pydantic ValidatorIterator 不可 pickle
    if isinstance(body, ChatCompletionRequest):
        body = _sanitize_chat_completion_request(body)

    # Starlette Request → 可序列化的 dataclass
    raw_request_info = None
    if raw_request is not None:
        raw_request_info = RawRequestInfo.from_starlette_request(raw_request)

    # Handle.remote() — Pydantic 对象通过 cloudpickle 传输
    async for response in getattr(model_handle, call_method).remote(
        body, raw_request_info
    ):
        yield response
```

**`_sanitize_chat_completion_request`**：

```python
# ray/llm/_internal/serve/core/ingress/utils.py:30-80

def _sanitize_chat_completion_request(request: ChatCompletionRequest):
    """Pydantic 的 Iterable 字段 (content, tool_calls) 在序列化时
    会变成 ValidatorIterator 对象，不能被 cloudpickle。
    修复方式：遍历 messages，将 Iterable → list。"""
    for i, message in enumerate(request.messages):
        if not isinstance(message, dict):
            request.messages[i] = message = message.model_dump()
        content_val = message.get("content")
        if content_val is not None and not isinstance(content_val, str):
            message["content"] = list(content_val)
        if message.get("role") == "assistant":
            tool_calls_val = message.get("tool_calls")
            if tool_calls_val is not None:
                message["tool_calls"] = list(tool_calls_val)
    return request
```

**`RawRequestInfo`**：

```python
# ray/llm/_internal/serve/core/protocol.py:30-73

@dataclass
class RawRequestInfo:
    """Starlette Request 不可 pickle，提取 headers 为 dict，
    跨 Ray RPC 传输后可重建为最小 Starlette Request。"""
    headers: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_starlette_request(cls, request: Request) -> "RawRequestInfo":
        return cls(headers=dict(request.headers))

    def to_starlette_request(self) -> Request:
        scope = {
            "type": "http", "method": "POST", "path": "/",
            "headers": [(k.lower().encode(), (v or "").encode())
                       for k, v in self.headers.items()],
            "query_string": b"",
        }
        return Request(scope)
```

**响应转换**：

```python
# ingress.py:472-509

async def _process_llm_request(self, body, call_method, raw_request=None):
    gen = self._get_response(body=body, call_method=call_method, raw_request=raw_request)
    initial_response, gen = await _peek_at_generator(gen)

    if isinstance(first_chunk, NON_STREAMING_RESPONSE_TYPES):
        # 非流式: Pydantic → dict → JSONResponse
        return JSONResponse(content=first_chunk.model_dump())

    # 流式: Pydantic → SSE "data: {json}\n\n"
    openai_stream_generator = _openai_json_wrapper(gen)
    return StreamingResponse(openai_stream_generator, media_type="text/event-stream")
```

### 12.2 gRPC 路径：Ray Serve Generic Proxy (非 LLM 专用)

Ray Serve 通用 gRPC 代理 **不涉及 protobuf↔pydantic 转换**，它直接透传 protobuf 对象：

```
gRPC 客户端发送 protobuf message
  │
  ▼ gRPCProxyRequest(request_proto=proto_obj, context=..., ...)
  │
  ▼ serialized_replica_arg() = pickle.dumps(gRPCRequest(user_request_proto=proto_obj))
  │    将 protobuf 对象包装在 gRPCRequest dataclass 中
  │    pickle 序列化为 bytes（绕过 cloudpickle 提高性能）
  │
  ▼ handle.remote(serialized_bytes)
  │    [Ray RPC — pickle bytes 传输]
  │
  ▼ Replica 端:
  │    unpickle → gRPCRequest
  │    grpc_request.user_request_proto 提取原始 protobuf 对象
  │
  ▼ 用户方法调用: user_method(user_request_proto, grpc_context=...)
  │
  ▼ 响应: result.SerializeToString() → raw bytes → gRPC 客户端
```

**关键代码**：

```python
# ray/serve/_private/proxy_request_response.py:157-262

class gRPCProxyRequest(ProxyRequest):
    def __init__(self, request_proto, context, service_method, stream, ...):
        self._request_proto = request_proto
        self.context = context
        self.service_method = service_method
        ...

    def serialized_replica_arg(self) -> bytes:
        # 直接 pickle 序列化，跳过 cloudpickle（性能优化）
        return pickle.dumps(gRPCRequest(user_request_proto=self._request_proto))
```

```python
# ray/serve/_private/common.py:738-742

@dataclass
class gRPCRequest:
    """从 gRPC proxy 发到 replica 的数据结构。"""
    user_request_proto: Any  # 原始 protobuf 对象
```

```python
# ray/serve/_private/replica.py:1598-1622

# Replica 端解包
elif request_metadata.is_grpc_request:
    grpc_request: gRPCRequest = request_args[0]
    method_info = self._user_callable_wrapper.get_user_method_info(
        request_metadata.call_method
    )
    request_args = (grpc_request.user_request_proto,)
    request_kwargs = (
        {GRPC_CONTEXT_ARG_NAME: request_metadata.grpc_context}
        if method_info.takes_grpc_context_kwarg
        else {}
    )
```

### 12.3 kray Gateway vs 社区版两条路径对比

kray 的 InferenceGateway **不走** Ray Serve 的通用 gRPC proxy，而是自己启动独立的 KESS gRPC server。
因此需要在 Gateway 内部完成 **protobuf bytes → Pydantic** 的转换，这是与社区版的核心差异：

| | 社区版 HTTP (OpenAiIngress) | 社区版 gRPC (Serve Proxy) | kray gRPC (InferenceGateway) |
|---|---|---|---|
| **入口** | FastAPI 自动解析 JSON→Pydantic | gRPC proxy 直接透传 proto 对象 | KESS gRPC server 收到 proto |
| **请求格式** | HTTP JSON body | protobuf message | protobuf message (body=bytes) |
| **转换** | FastAPI 自动完成 | **无需转换**，proto 直传 | **Gateway 手动转换**: `json.loads(body) → Pydantic` |
| **传输** | `handle.remote(pydantic_obj)` | `handle.remote(pickled_proto_bytes)` | `handle.remote(pydantic_obj)` |
| **响应** | Pydantic → JSON/SSE | `proto.SerializeToString()` | Pydantic/dict/str → `json.dumps().encode()` → proto body |
| **streaming** | `model_dump_json()` → SSE text/event-stream | proto stream | asyncio.Queue 桥接 async→sync generator |

### 12.4 kray Gateway 转换链路详解

```
KESS gRPC 客户端
  │ ChatRequest(model="qwen2.5-72b", body=json.dumps({...}).encode())
  ▼
InferenceGateway.Chat(request, context)
  │
  ▼ _dispatch_llm("chat", request, context)
  │
  ├─→ _convert_request("chat", request, stream=False)
  │     1. body_bytes = request.body (JSON bytes)
  │     2. body_dict = json.loads(body_bytes) → dict
  │     3. body_dict["model"] = request.model
  │     4. body_dict["stream"] = False
  │     5. 自动路由: embeddings + messages → EmbeddingChatRequest
  │     6. cls = _get_request_cls("chat") → ChatCompletionRequest
  │     7. return ChatCompletionRequest(**body_dict)  ← Pydantic 对象
  │
  ├─→ _resolve_llm_handle(request) → (handle, model_id)
  │
  ├─→ asyncio.run_coroutine_threadsafe(
  │       _call_llm_handle("chat", handle, request),
  │       self._loop)
  │     └─→ _call_llm_handle:
  │           llm_request = _convert_request(...)
  │           gen = await handle.chat.remote(llm_request)  ← Pydantic 通过 cloudpickle
  │           async for chunk in gen:
  │               return chunk  ← ChatCompletionResponse pydantic 对象
  │
  └─→ _convert_response("chat", result, request)
        1. response_cls = ChatResponse (protobuf)
        2. result.model_dump_json().encode("utf-8") → body_bytes
        3. return ChatResponse(status=SUCCESS, body=body_bytes)
```

### 12.5 kray Streaming 转换链路

```
KESS gRPC 客户端
  │ StreamChat(ChatRequest(model, body=json))
  ▼
InferenceGateway.StreamChat(request, context)
  │
  ▼ _dispatch_llm_stream("chat", request, context)
  │
  ├─→ _call_llm_handle_stream("chat", handle, request)
  │     llm_request = _convert_request(..., stream=True)
  │     gen = await handle.chat.remote(llm_request)
  │     async for chunk in gen:
  │       if isinstance(chunk, str):
  │         # SGLang SSE: "data: {json}\n\n"
  │         yield ChatResponse(status=SUCCESS, body=chunk.encode())
  │       elif hasattr(chunk, "model_dump_json"):
  │         # vLLM Pydantic: ChatCompletionStreamResponse
  │         yield ChatResponse(status=SUCCESS, body=chunk.model_dump_json().encode())
  │
  ├─→ asyncio.Queue 桥接 (async → sync)
  │     _bridge() async coroutine → 写入 queue
  │     while True: queue.get(timeout) → yield (sync generator)
  │
  └─→ gRPC server-streaming 返回 ChatResponse 序列
```

**为什么需要 Queue 桥接？**
- gRPC server-streaming RPC 要求返回 **sync generator**
- LLM Server 的 `chat(stream=True)` 返回 **async generator**
- `run_coroutine_threadsafe` 不能处理 async generator
- 解决方案：async coroutine 写 Queue，sync generator 读 Queue

---

## 13. 启动脚本使用指南：working_dir、PYTHONPATH 与 python -m

### 13.1 启动方式

**方式一：已 `pip install -e .` 安装 ray 包**

```bash
python -m ray.serve.inference.start \
    --service-name my-llm-service \
    --model-id qwen2.5-72b \
    --model-source /data/models/qwen2.5-72b \
    --engine sglang \
    --kess-owner my-team \
    --kess-shard-name s0 \
    --kess-biz-def my-biz \
    --num-gpus-per-replica 4 \
    --num-replicas 2
```

**方式二：未安装 ray 包，需指定 PYTHONPATH**

```bash
PYTHONPATH=/path/to/kray/python:$PYTHONPATH \
python -m ray.serve.inference.start ...
```

**必填参数**：`service_name`、`model_id`、`kess_owner`、`kess_shard_name`、`kess_biz_def`（缺一会报 `ValueError`）。

### 13.2 Working Dir 的含义

Ray Serve 启动时，`working_dir` 指的是 `serve.run(app)` 时 Ray driver 进程的工作目录，通过 `runtime_env` 的 `working_dir` 字段设置。其作用：

1. **影响相对路径解析**：如 `model_source` 用相对路径时
2. **触发代码打包上传**：Ray 会将 `working_dir` 目录下的文件打包上传到集群，让 worker 能访问这些代码
3. **自动加入 PYTHONPATH**：Worker 进程的 `sys.path` 会包含 working_dir（详见第 14 节）

如果集群 worker 机器上没有对应的 ray 包，没有设置 `runtime_env.working_dir`，worker 会 import 失败。

### 13.3 python -m 的作用

`python -m ray.serve.inference.start` 的含义：把 `ray.serve.inference.start` 当作一个**模块**来运行，Python 会：

1. 在 `sys.path` 中查找 `ray/serve/inference/start.py`
2. 执行该模块中 `if __name__ == "__main__"` 下的代码

与直接 `python start.py` 的区别是：`-m` 保证了包的相对导入能正确解析（模块以 `ray.serve.inference.start` 而非 `__main__` 注册）。

### 13.4 什么时候需要上传包内容

Ray 会在以下场景上传包内容到集群：

| 场景 | 触发条件 | 行为 |
|---|---|---|
| `runtime_env.working_dir` | 设了本地目录 | 打包该目录并上传到 GCS |
| `runtime_env.py_modules` | 设了模块列表 | 打包指定模块并上传 |
| `runtime_env.pip` | 设了 pip 依赖 | 生成 pip 环境并上传 |

如果集群 worker 上已通过 `pip install -e .` 安装了 ray 包，则不需要上传。否则需要通过 `runtime_env` 让 Ray 把代码上传到 worker：

```python
InferenceServeConfig(
    ...,
    runtime_env={"working_dir": "/path/to/kray/python"},
)
```

**注意**：`working_dir` 上传的是目录下的**文件快照**，不是整个 git repo。对于 `pip install -e .` 的 editable install，Ray 不会自动识别 `.egg-link`，所以用 `working_dir` 更可靠。

### 13.5 working_dir 是否自动加入 PYTHONPATH

**是的**。Ray 设置 `runtime_env.working_dir` 后会：

1. **打包**该目录内容并上传到 worker
2. **自动将该目录加入 `sys.path`**

所以 worker 上的 Python 能直接 `import ray.serve.inference.start`，不需要额外设 `PYTHONPATH`。

但注意：如果设 `working_dir=kray/python`，上传的就是 `python/` 目录下的所有文件快照。

### 13.6 start.py 中 runtime_env 的传递链路

```python
# start.py:235-236
if serve_cfg.runtime_env:
    llm_config_kwargs["runtime_env"] = serve_cfg.runtime_env

# start.py:238
llm_config = LLMConfig(**llm_config_kwargs)

# start.py:254
inference_config = InferenceConfig(
    ...,
    llm_configs={serve_cfg.model_id: llm_config},
)
```

runtime_env 从 `InferenceServeConfig` → `LLMConfig.runtime_env` → 在 `SGLangServer.get_deployment_options()` 中被合并到 `ray_actor_options["runtime_env"]`：

```python
# sglang_engine.py:366-370
runtime_env = ray_actor_options.setdefault("runtime_env", {})
if llm_config.runtime_env:
    runtime_env.update(llm_config.runtime_env)
```

最终通过 `serve.deployment(server_cls).options(**serve_options).bind(llm_config)` 传入 Ray Serve，Ray Serve 在创建 replica 时将 `runtime_env` 传递给 Ray 核心。

---

## 14. runtime_env working_dir 完整代码逻辑详解

本节详细描述从用户设置 `working_dir` 到 worker 进程使用该目录的完整代码链路。

### 14.1 整体流程图

```
用户设置 working_dir (本地路径或 URI)
       │
       ▼
[Phase 1] RuntimeEnv 校验 working_dir
       │  runtime_env.py:317, validation.py:99-114
       ▼
[Phase 2] Driver 端打包上传
       │  worker.py:2678-2690 调用 upload_working_dir_if_needed()
       │  working_dir.py:33-135:
       │    - 已是 URI → 直接透传，不上传
       │    - 是本地目录 → 计算内容哈希 → 打 zip → 上传到 GCS
       │    - 替换 working_dir 为 GCS URI
       │  packaging.py:702-765 (upload_package_if_needed)
       │  packaging.py:651-673 (upload_package_to_gcs)
       ▼
[Phase 3] Runtime Env Agent 在 worker 节点下载解压
       │  runtime_env_agent.py:303-357
       │  WorkingDirPlugin.create() → download_and_unpack_package()
       │    - 从 GCS 下载 zip → 写本地 → 解压
       │  WorkingDirPlugin.modify_context():
       │    - command_prefix 加 "cd <local_dir> &&"
       │    - 调用 set_pythonpath_in_context(local_dir, context)
       ▼
[Phase 4] Worker 进程启动
       │  context.py:42-108 exec_worker()
       │    - update_envs(context.env_vars) → 设置 PYTHONPATH
       │    - Python 自动将 PYTHONPATH 加入 sys.path
       │    - 命令: cd <local_dir> && exec python default_worker.py ...
       ▼
Worker 进程: cwd=working_dir, sys.path 包含 working_dir
```

### 14.2 Phase 1: 用户设置与校验

**`runtime_env.py:317,338-339`** — `RuntimeEnv.__init__` 接收 `working_dir`：

```python
# python/ray/runtime_env/runtime_env.py:317
class RuntimeEnv(dict):
    def __init__(self, ..., working_dir: Optional[str] = None, ...):
        ...
        # Line 338-339
        if working_dir is not None:
            self["working_dir"] = working_dir
```

**`runtime_env.py:482-486`** — 访问方法：

```python
# Line 482-483
def has_working_dir(self) -> bool:
    return "working_dir" in self

# Line 485-486
def working_dir_uri(self) -> str:
    return self.get("working_dir", "")
```

**`validation.py:99-114`** — 校验逻辑：

```python
# python/ray/_private/runtime_env/validation.py:99-114
def parse_and_validate_working_dir(value: str) -> str:
    if not isinstance(value, str):
        raise TypeError("working_dir must be a string")
    value = value.strip()
    if not value:
        raise ValueError("working_dir cannot be empty")
    # 尝试解析为 URI
    try:
        protocol, path = parse_uri(value)
    except ValueError:
        # 不是 URI，视为本地路径
        if not os.path.isdir(value):
            raise ValueError(f"working_dir {value} is not a valid directory or URI")
    return value
```

**`job_config.py:49,71,231-232`** — JobConfig 封装：

```python
# python/ray/job_config.py:49
class JobConfig:
    def __init__(self, ..., runtime_env=None, ...):
        self.runtime_env = runtime_env or {}

    # Line 231-232
    def _runtime_env_has_working_dir(self) -> bool:
        return self._validated_runtime_env.has_working_dir()
```

### 14.3 Phase 2: Driver 端打包上传

**`worker.py:2665-2690`** — Driver 进程在 job 启动前调用上传：

```python
# python/ray/_private/worker.py:2665-2690
scratch_dir: str = worker.node.get_runtime_env_dir_path()
runtime_env = job_config.runtime_env or {}
include_gitignore = os.environ.get(RAY_RUNTIME_ENV_IGNORE_GITIGNORE, "0") != "1"

# 先上传 py_modules
runtime_env = upload_py_modules_if_needed(
    runtime_env,
    include_gitignore=include_gitignore,
    scratch_dir=scratch_dir,
    logger=logger,
)
# 再上传 working_dir
runtime_env = upload_working_dir_if_needed(
    runtime_env,
    include_gitignore=include_gitignore,
    scratch_dir=scratch_dir,
    logger=logger,
)
# 移除 excludes（上传后不再需要）
runtime_env.pop("excludes", None)
job_config.set_runtime_env(runtime_env, validate=True)
```

**`worker.py:2692-2716`** — 如果没有 working_dir，则把脚本目录和当前目录加入 code_paths：

```python
# python/ray/_private/worker.py:2699-2716
code_paths = []
if not interactive_mode and not dashboard_namespace:
    script_directory = os.path.dirname(os.path.realpath(sys.argv[0]))
    if script_directory in sys.path:
        code_paths.append(script_directory)
# 没有设置 working_dir 时，才把当前目录加入 code_paths
if not job_config._client_job and not job_config._runtime_env_has_working_dir():
    current_directory = os.path.abspath(os.path.curdir)
    code_paths.append(current_directory)
if len(code_paths) != 0:
    job_config._py_driver_sys_path.extend(code_paths)
```

**关键设计**：如果设了 `working_dir`，则**不会**把当前目录加入 code_paths，因为 working_dir 已包含所有需要上传的代码。

**`working_dir.py:33-135`** — 核心上传逻辑 `upload_working_dir_if_needed()`：

```python
# python/ray/_private/runtime_env/working_dir.py:33-135
def upload_working_dir_if_needed(
    runtime_env, include_gitignore, scratch_dir, logger, upload_fn=None
):
    working_dir = runtime_env.get("working_dir")
    # 1. 没设 working_dir → 不做任何事
    if working_dir is None:
        return runtime_env

    # 2. 类型校验
    if not isinstance(working_dir, (str, Path)):
        raise TypeError(...)

    # 3. 已是 URI (如 gcs://, s3://) → 直接透传，不上传
    try:
        protocol, path = parse_uri(working_dir)
    except ValueError:
        protocol, path = None, None
    if protocol is not None:
        if protocol in Protocol.remote_protocols() and not path.endswith(".zip"):
            raise ValueError("Only .zip files supported for remote URIs.")
        return runtime_env

    # 4. 计算排除列表: 默认排除 + 用户排除 + .gitignore/.rayignore
    default_excludes = ray_constants.get_runtime_env_default_excludes()
    user_excludes = runtime_env.get("excludes") or []
    excludes = default_excludes + list(user_excludes)

    # 5. 计算内容哈希，生成 URI
    try:
        working_dir_uri = get_uri_for_directory(
            working_dir,
            include_gitignore=include_gitignore,
            excludes=excludes,
        )
    except ValueError:
        # 5a. 不是目录，可能是 .zip 文件
        package_path = Path(working_dir)
        if not package_path.exists() or package_path.suffix != ".zip":
            raise ValueError(...)
        pkg_uri = get_uri_for_package(package_path)
        upload_package_to_gcs(pkg_uri, package_path.read_bytes())
        runtime_env["working_dir"] = pkg_uri
        return runtime_env

    # 6. 上传目录内容
    if upload_fn is None:
        upload_package_if_needed(
            working_dir_uri,
            scratch_dir,
            working_dir,
            include_parent_dir=False,
            excludes=excludes,
            include_gitignore=include_gitignore,
            logger=logger,
        )
    else:
        upload_fn(working_dir, excludes=excludes)

    # 7. 替换 working_dir 为 GCS URI
    runtime_env["working_dir"] = working_dir_uri
    return runtime_env
```

**`packaging.py:603-648`** — 内容寻址哈希计算 `get_uri_for_directory()`：

```python
# python/ray/_private/runtime_env/packaging.py:603-648
def get_uri_for_directory(directory, include_gitignore, excludes=None):
    directory = Path(directory).absolute()
    if not directory.exists() or not directory.is_dir():
        raise ValueError(f"directory {directory} must be an existing directory")

    # 遍历目录所有文件，计算内容哈希
    hash_val = _hash_directory(
        directory, directory,
        _get_excludes(directory, excludes),
        include_gitignore=include_gitignore,
    )
    # 生成 GCS URI，如: gcs://_ray_pkg_029f88d5ecc55e1e4d64fc6e388fd103.zip
    return "{protocol}://{pkg_name}.zip".format(
        protocol=Protocol.GCS.value,
        pkg_name=RAY_PKG_PREFIX + hash_val.hex()
    )
```

**`packaging.py:702-765`** — 完整上传流程 `upload_package_if_needed()`：

```python
# python/ray/_private/runtime_env/packaging.py:702-765
def upload_package_if_needed(pkg_uri, base_directory, module_path, ...):
    # 1. 在 GCS 中 pin URI（防止过早 GC）
    pin_runtime_env_uri(pkg_uri)

    # 2. 检查是否已存在（缓存机制）
    if package_exists(pkg_uri):
        return False

    # 3. 创建临时 zip 文件
    package_file = Path(_get_local_path(base_directory, pkg_uri))
    # 加时间戳+PID 防并发冲突
    package_file = package_file.with_name(
        f"{time.time_ns()}_{os.getpid()}_{package_file.name}"
    )

    # 4. 打 zip
    create_package(module_path, package_file, include_gitignore=..., ...)

    # 5. 读取 zip 字节，删除临时文件
    package_file_bytes = package_file.read_bytes()
    package_file.unlink()

    # 6. 上传到 GCS KV 存储
    upload_package_to_gcs(pkg_uri, package_file_bytes)

    return True
```

**`packaging.py:651-673`** — 上传到 GCS：

```python
# python/ray/_private/runtime_env/packaging.py:651-673
def upload_package_to_gcs(pkg_uri, pkg_bytes):
    protocol, pkg_name = parse_uri(pkg_uri)
    if protocol == Protocol.GCS:
        _store_package_in_gcs(pkg_uri, pkg_bytes)
    elif protocol in Protocol.remote_protocols():
        raise ValueError("upload_package_to_gcs should not be called with a remote path.")
    else:
        raise NotImplementedError(f"Protocol {protocol} is not supported")
```

### 14.4 Phase 3: Worker 节点下载解压与环境设置

**`runtime_env_agent.py:303-357`** — Agent 中 working_dir **最先创建**，其他插件在其上下文中运行：

```python
# python/ray/_private/runtime_env/agent/runtime_env_agent.py:303-357
async def GetOrCreateRuntimeEnv(self, request):
    async def _setup_runtime_env(runtime_env, runtime_env_config):
        context = RuntimeEnvContext(env_vars=runtime_env.env_vars())

        # ★★★ First create working dir... ★★★
        working_dir_ctx = self._plugin_manager.plugins[WorkingDirPlugin.name]
        await create_for_plugin_if_needed(
            runtime_env,
            working_dir_ctx.class_instance,
            working_dir_ctx.uri_cache,
            context,
            per_job_logger,
        )

        # ★★★ Then within the working dir, create the other plugins. ★★★
        working_dir_uri_or_none = runtime_env.working_dir_uri()
        with self._working_dir_plugin.with_working_dir_env(working_dir_uri_or_none):
            for plugin_setup_context in self._plugin_manager.sorted_plugin_setup_contexts():
                plugin = plugin_setup_context.class_instance
                if plugin.name != WorkingDirPlugin.name:
                    await create_for_plugin_if_needed(
                        runtime_env, plugin, uri_cache, context, per_job_logger
                    )
        return context
```

**为什么 working_dir 必须最先创建？** 其他插件（pip、conda）可能需要引用 working_dir 中的文件（如 `pip -r ${RAY_RUNTIME_ENV_CREATE_WORKING_DIR}/requirements.txt`），通过 `with_working_dir_env()` 上下文管理器设置 `RAY_RUNTIME_ENV_CREATE_WORKING_DIR` 环境变量。

**`working_dir.py:154-202`** — `WorkingDirPlugin.create()` 下载解压：

```python
# python/ray/_private/runtime_env/working_dir.py:188-202
class WorkingDirPlugin(RuntimeEnvPlugin):
    name = "working_dir"
    priority = 5  # 优先级最低，但被特殊处理为最先创建

    async def create(self, uri, runtime_env, context, logger=default_logger):
        # 从 GCS 下载 zip 并解压到本地目录
        local_dir = await download_and_unpack_package(
            uri,
            self._resources_dir,   # {runtime_env_dir}/working_dir_files/
            self._gcs_client,
            logger=logger,
            overwrite=True,
        )
        return get_directory_size_bytes(local_dir)
```

**`packaging.py:776-897`** — `download_and_unpack_package()` 下载解压流程：

```python
# python/ray/_private/runtime_env/packaging.py:776-897
async def download_and_unpack_package(pkg_uri, base_directory, gcs_client=None, ...):
    pkg_file = Path(_get_local_path(base_directory, pkg_uri))
    local_dir = get_local_dir_from_uri(pkg_uri, base_directory)
    # 例如: gcs://_ray_pkg_abc123.zip → {resources_dir}/working_dir_files/_ray_pkg_abc123/

    async with _AsyncFileLock(str(pkg_file) + ".lock"):
        # 1. 检查本地缓存
        if local_dir.exists() and not overwrite:
            download_package = False  # 已解压，跳过

        if download_package:
            protocol, _ = parse_uri(pkg_uri)
            if protocol == Protocol.GCS:
                # 2. 从 GCS KV 存储下载 zip 字节
                code = await gcs_client.async_internal_kv_get(
                    pkg_uri.encode(), namespace=None, timeout=None
                )
                code = code or b""
                pkg_file.write_bytes(code)

                # 3. 解压到本地目录
                if is_zip_uri(pkg_uri):
                    unzip_package(
                        package_path=pkg_file,
                        target_dir=local_dir,
                        remove_top_level_directory=False,
                        unlink_zip=True,
                        logger=logger,
                    )
                else:
                    return str(pkg_file)

    return str(local_dir)
```

**本地目录映射规则**（`packaging.py:768-772`）：

```python
# python/ray/_private/runtime_env/packaging.py:768-772
def get_local_dir_from_uri(uri: str, base_directory: str) -> Path:
    pkg_file = Path(_get_local_path(base_directory, uri))
    local_dir = pkg_file.with_suffix("")  # 去掉 .zip 后缀
    return local_dir
# gcs://_ray_pkg_029f88d5ecc55e1e4d64fc6e388fd103.zip
#   → {runtime_env_dir}/working_dir_files/_ray_pkg_029f88d5ecc55e1e4d64fc6e388fd103/
```

### 14.5 Phase 4: 设置 Worker 进程环境（关键步骤）

**`working_dir.py:204-229`** — `WorkingDirPlugin.modify_context()` 做两件事：

```python
# python/ray/_private/runtime_env/working_dir.py:204-229
def modify_context(self, uris, runtime_env_dict, context, logger=default_logger):
    if not uris:
        return

    uri = uris[0]
    local_dir = get_local_dir_from_uri(uri, self._resources_dir)
    if not local_dir.exists():
        raise ValueError(
            f"Local directory {local_dir} for URI {uri} does not exist..."
        )

    # ★★★ 1. 设置工作目录: 在 command_prefix 前加 "cd <local_dir> &&" ★★★
    if not _WIN32:
        context.command_prefix += ["cd", str(local_dir), "&&"]
    else:
        # Windows: /d 支持跨盘符切换
        context.command_prefix += ["cd", "/d", f"{local_dir}", "&&"]

    # ★★★ 2. 加入 PYTHONPATH ★★★
    set_pythonpath_in_context(python_path=str(local_dir), context=context)
```

**`working_dir.py:138-151`** — `set_pythonpath_in_context()` 详解：

```python
# python/ray/_private/runtime_env/working_dir.py:138-151
def set_pythonpath_in_context(python_path: str, context: RuntimeEnvContext):
    """Insert the path as the first entry in PYTHONPATH in the runtime env.

    The import priority is as follows:
    this python_path arg > env_vars PYTHONPATH > existing cluster env PYTHONPATH.
    """
    # 优先级最高: working_dir 路径
    if "PYTHONPATH" in context.env_vars:
        python_path += os.pathsep + context.env_vars["PYTHONPATH"]
    if "PYTHONPATH" in os.environ:
        python_path += os.pathsep + os.environ["PYTHONPATH"]
    context.env_vars["PYTHONPATH"] = python_path
```

**PYTHONPATH 优先级**：`working_dir` > `env_vars` 中的 PYTHONPATH > 集群原有 PYTHONPATH

**`working_dir.py:231-264`** — `with_working_dir_env()` 上下文管理器：

```python
# python/ray/_private/runtime_env/working_dir.py:231-264
@contextmanager
def with_working_dir_env(self, uri):
    """设置 RAY_RUNTIME_ENV_CREATE_WORKING_DIR 环境变量，
    让其他插件 (pip, conda) 能找到 working_dir 中的文件。"""
    if uri is None:
        yield
    else:
        local_dir = get_local_dir_from_uri(uri, self._resources_dir)
        if not local_dir.exists():
            raise ValueError(...)
        key = ray_constants.RAY_RUNTIME_ENV_CREATE_WORKING_DIR_ENV_VAR
        prev = os.environ.get(key)
        # Windows 反斜杠路径修正
        os.environ[key] = local_dir.as_posix()
        try:
            yield
        finally:
            if prev is None:
                del os.environ[key]
            else:
                os.environ[key] = prev
```

### 14.6 Phase 5: Worker 进程启动

**`context.py:18-33`** — `RuntimeEnvContext` 数据结构：

```python
# python/ray/_private/runtime_env/context.py:18-33
class RuntimeEnvContext:
    def __init__(
        self,
        command_prefix: List[str] = None,  # 如 ["cd", "/path/to/working_dir", "&&"]
        env_vars: Dict[str, str] = None,    # 如 {"PYTHONPATH": "/path/to/working_dir:..."}
        py_executable: Optional[str] = None,
        override_worker_entrypoint: Optional[str] = None,
        java_jars: List[str] = None,
    ):
        self.command_prefix = command_prefix or []
        self.env_vars = env_vars or {}
        self.py_executable = py_executable or sys.executable
```

**`context.py:42-108`** — `exec_worker()` 启动 worker 进程：

```python
# python/ray/_private/runtime_env/context.py:42-108
def exec_worker(self, passthrough_args, language):
    # ★★★ 1. 将 env_vars 写入 os.environ（包括 PYTHONPATH） ★★★
    update_envs(self.env_vars)

    if language == Language.PYTHON:
        executable = ["exec", self.py_executable]
    else:
        executable = ["exec"]

    # ★★★ 2. 构造命令 ★★★
    # command_prefix = ["cd", "/path/to/working_dir", "&&"]
    # 最终命令: cd /path/to/working_dir && exec python default_worker.py ...
    passthrough_args = [shlex.quote(s) for s in passthrough_args]
    cmd = [*self.command_prefix, *executable, *passthrough_args]

    # ★★★ 3. 执行 ★★★
    logger.debug(f"Exec'ing worker with command: {cmd}")
    os.execvp("bash", args=["bash", "-c", " ".join(cmd)])
```

**`utils.py:1617-1629`** — `update_envs()` 将 PYTHONPATH 写入环境变量：

```python
# python/ray/_private/utils.py:1617-1629
def update_envs(env_vars: Dict[str, str]):
    """更新环境变量，支持 ${X} 变量替换。"""
    if not env_vars:
        return
    for key, value in env_vars.items():
        expanded = os.path.expandvars(value)
        result = re.sub(r"\$\{[A-Z0-9_]+\}", "", expanded)
        os.environ[key] = result
```

Python 进程启动时会自动将 `PYTHONPATH` 环境变量中的路径加入 `sys.path`，因此 worker 进程能直接 `import` working_dir 下的模块。

### 14.7 最终效果汇总

| 效果 | 实现方式 | 代码位置 |
|---|---|---|
| Worker 的 cwd = working_dir | `command_prefix += ["cd", str(local_dir), "&&"]` | `working_dir.py:224-228` |
| working_dir 在 sys.path 上 | `set_pythonpath_in_context(str(local_dir), context)` → `PYTHONPATH` 环境变量 | `working_dir.py:229` → `context.py:43` |
| 其他插件能引用 working_dir 文件 | `RAY_RUNTIME_ENV_CREATE_WORKING_DIR` 环境变量 | `working_dir.py:231-264` |
| 目录内容一致性保证 | content-addressable URI (哈希) | `packaging.py:603-648` |
| 避免重复上传 | GCS 缓存 (`package_exists` 检查) | `packaging.py:741-742` |
| 避免重复下载 | 本地缓存 (`local_dir.exists()` 检查) | `packaging.py:824-825` |

---

## 15. 待重构项

1. **新建 `converter.py`**：从 `gateway.py` 提取 proto↔pydantic 转换层
2. **精简 `gateway.py`**：移出转换层，提取 `_track_request` 上下文管理器统一 metrics/timeout/ongoing 逻辑
3. **拆分 `start.py`**：提取 `_build_llm_config_kwargs`/`_build_inference_config` 纯函数
4. **多模型支持**：`start.py` 当前只支持单 model CLI，`InferenceConfig.llm_configs` 已支持多 model
