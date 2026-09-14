# Ray Serve Inference 模块 SGLang 集成测试记录

## 1. 概述

在 Ray 集群上测试 SGLang 推理引擎集成，从环境变量配置到最终服务成功运行，过程中遇到多个问题并逐一修复。

**最终结果**：SGLang 服务成功启动并运行（`LOG_ENGINE_METRICS=0` 绕过 Unicode bug）。

---

## 2. 测试环境

- **Ray 版本**：`2.55.1+kuaishou.261191061f`
- **Python**：`3.10`（`/opt/venv`）
- **SGLang**：已安装在 `/opt/venv`
- **vLLM**：未安装
- **远端代码路径**：`/mmu_mllm_hdd_2/shiyanpeng03/ray/python/ray/serve/inference/`
- **site-packages 路径**：`/opt/venv/lib/python3.10/site-packages/ray/serve/inference/`
- **集群**：3 node, 8 GPU, 368 CPU

---

## 3. 问题详细记录

### 3.1 环境变量配置问题

#### 3.1.1 ENGINE_KWARGS JSON 格式错误

**错误信息**：
```
json.decoder.JSONDecodeError: Expecting value: line 1 column 1 (char 0)
```

**原因**：环境变量 `ENGINE_KWARGS` 的值有三个问题：
1. Python 风格布尔值 `True`/`False`，JSON 标准应为 `true`/`false`
2. 末尾多余逗号 `"constrained_json_disable_any_whitespace": True,}`
3. 外层单引号被存入环境变量值中

**原值**（错误）：
```bash
export ENGINE_KWARGS='{"tp_size": 1, "disable_cuda_graph": False, "constrained_json_disable_any_whitespace": True,}'
```

**正确值**：
```bash
export ENGINE_KWARGS='{"tp_size": 1, "disable_cuda_graph": false, "constrained_json_disable_any_whitespace": true}'
```

**修复**：`_parse_json_arg` 加容错处理 — strip 外层引号 + Python bool/None 自动替换为 JSON 标准值。

#### 3.1.2 `ray job submit` 不继承 shell 环境变量

**现象**：`ray job submit` 提交的作业进程拿不到当前 shell 的环境变量。

**原因**：Job driver 是独立进程，不共享当前 shell env。

**修复**：通过 `--runtime-env` 或 `--runtime-env-json` 传入环境变量。

#### 3.1.3 `--working-dir` 打包导致 PENDING

**现象**：作业状态一直 PENDING — "waiting for the runtime environment to be set up"。

**原因**：`--working-dir` 会把整个 `/mmu_mllm_hdd_2/shiyanpeng03/ray/python` 目录打包上传到 GCS，目录太大，上传和解压耗时很长。

**修复**：代码在共享存储路径上，用绝对路径直接执行，不设 `working_dir`：
```bash
ray job submit --runtime-env /tmp/runtime_env.json -- python3 /mmu_mllm_hdd_2/shiyanpeng03/ray/python/ray/serve/inference/start.py
```

#### 3.1.4 wezterm 发送命令回车被拼入

**现象**：`python -m ray.serve.inference.startr`（多了 `r`）。

**原因**：wezterm `wez_send` 发送 `\r` 回车符时拼入了模块名末尾。

**修复**：用 `wez_safe` 替代 `wez_send + \r`，或用分号分隔命令。

#### 3.1.5 MODEL_ID 与客户端请求不匹配

**错误信息**：
```
LLM model resolution failed: model 'qwen3.5-30b-a3b' not found. Available: ['Qwen3.5-35B-A3B-NVFP4']
```

**原因**：服务注册的 `MODEL_ID=Qwen3.5-35B-A3B-NVFP4`，但客户端请求的是 `qwen3.5-30b-a3b`。

**修复**：将 `MODEL_ID` 改为 `qwen3.5-30b-a3b` 匹配客户端期望。

---

### 3.2 代码 Bug

#### 3.2.1 `builder.py` 硬编码 `LLMServer`

**错误信息**：
```
ModuleNotFoundError: No module named 'vllm'
```

**完整调用链**：
```
builder.py (硬编码)
  → from ray.serve.llm import LLMServer
  → LLMServer.get_deployment_options(llm_config)
      ↓
llm_server.py (Ray 代码)
  → llm_config.get_engine_config()
      ↓
llm_config.py (Ray 代码)
  → from ray.llm...vllm.vllm_models import ...  # 不看 server_cls，总是走 vLLM
      ↓
vllm_models.py (Ray 代码)
  → from vllm.engine.arg_utils import AsyncEngineArgs  # 没有 vllm 包 → 报错
```

**根因**：`_build_llm_deployment` 始终用 `LLMServer`，即使 `LLMConfig(server_cls=SGLangServer)` 已设了 sglang，`LLMServer.get_deployment_options()` 内部调的 `llm_config.get_engine_config()` **不看 `server_cls`**，总是 import vllm。

**修复**：
```python
# builder.py - 修复前
def _build_llm_deployment(llm_config):
    from ray.serve.llm import LLMServer
    serve_options = LLMServer.get_deployment_options(llm_config)
    return serve.deployment(LLMServer).options(**serve_options).bind(llm_config)

# builder.py - 修复后
def _build_llm_deployment(llm_config):
    from ray.serve.llm import LLMServer
    server_cls = getattr(llm_config, "server_cls", None) or LLMServer
    serve_options = server_cls.get_deployment_options(llm_config)
    return serve.deployment(server_cls).options(**serve_options).bind(llm_config)
```

#### 3.2.2 SGLang `num_gpus` 限制

**错误信息**：
```
ValueError: SGLang coordinator num_gpus must be 0; GPUs belong to schedulers
```

**原因**：SGLang 架构中 coordinator 不占 GPU，GPU 分配给 placement group 中的 scheduler bundles。`start.py` 把 `num_gpus_per_replica` 加到了 `ray_actor_options["num_gpus"]`，违反了 SGLang 的要求。

**修复**：检测 `server_cls` 为 SGLang 时，跳过 `num_gpus` 设置。

#### 3.2.3 SGLang 不支持 `deployment_config` autoscaling 字段

**错误信息**：
```
TypeError: Deployment.options() got an unexpected keyword argument 'min_replicas'
TypeError: Deployment.options() got an unexpected keyword argument 'target_ongoing_requests'
```

**原因**：`deployment_config` 中传了 `min_replicas`/`max_replicas`/`target_ongoing_requests`，SGLang 的 `get_deployment_options` 会 `copy.deepcopy(deployment_config)` 然后覆写，这些字段最终传给 `serve.deployment().options()`，但 `options()` 不接受 autoscaling 参数。

**修复**：指定 `server_cls` 时只传 `num_replicas` + `max_ongoing_requests`，autoscaling 和 `num_gpus` 由 `get_deployment_options()` 自行处理。

#### 3.2.4 `server_cls` 类型判断导致 `load_class` 报错

**错误信息**：
```
TypeError: argument of type 'type' is not iterable
```

**原因**：初始修复时用 `load_class(server_cls_str)` 解析 `server_cls`，但 `LLMConfig` 构造时 `validate_server_cls` 已把字符串转为 class 对象，`load_class` 期望字符串收到了 type。

**修复**：直接用 `getattr(llm_config, "server_cls", None) or LLMServer`，不用 `load_class`。

---

### 3.3 远端 Ray 版本兼容问题

#### 3.3.1 `ray.serve.llm` 未导出 `SGLangServer`

**错误信息**：
```
AttributeError: module 'ray.serve.llm' has no attribute 'SGLangServer'
```

**原因**：远端 Ray 版本 `2.55.1+kuaishou` 的 `ray.serve.llm.__init__.py` 未导出 `SGLangServer`，但类存在于内部路径 `ray.llm._internal.serve.engines.sglang.sglang_engine.SGLangServer`。

**修复**：`SERVER_CLS` 使用完整内部路径。后续改用 `_resolve_engine` 短名映射统一处理。

#### 3.3.2 site-packages 与源码目录不同步

**现象**：修改了本地源码但远端 job driver 跑的还是旧版 site-packages 代码。

**原因**：`pip install -e .` 因 pyproject.toml 版本配置问题无法使用，`setup-dev.py` 软链未生效。

**临时方案**：csc 传输文件 + `cp` 同步到 site-packages：
```bash
cp /mmu_mllm_hdd_2/shiyanpeng03/ray/python/ray/serve/inference/start.py /opt/venv/lib/python3.10/site-packages/ray/serve/inference/start.py
cp /mmu_mllm_hdd_2/shiyanpeng03/ray/python/ray/serve/inference/builder.py /opt/venv/lib/python3.10/site-packages/ray/serve/inference/builder.py
rm -rf /opt/venv/lib/python3.10/site-packages/ray/serve/inference/__pycache__
```

**待修复**：`setup-dev.py` 软链机制需修复，确保源码修改自动生效。

---

### 3.4 sglang 自身 Bug

#### 3.4.1 Unicode 编码错误

**错误信息**：
```
UnicodeEncodeError: 'ascii' codec can't encode character '\u2014' in position 201: ordinal not in range(128)
```

**完整调用链**：
```
SGLangServer.__init__
  → Scheduler.init_metrics_collector()
      → SchedulerMetricsCollector.init_new()
          → Gauge(description=...)  # 描述文本含 em-dash '—'
              → CythonGauge.__init__(self._name, self._description, ...)
                  # Cython 层面不支持非 ASCII → 报错
```

**原因**：sglang metrics 描述文本包含 em-dash (`—`)，Ray 的 Cython `Gauge.__init__` 不支持非 ASCII 字符。这是 sglang 与 Ray Cython metric 的兼容性问题。

**临时方案**：设置 `LOG_ENGINE_METRICS=0` 关闭引擎 metrics。

**长期方案**：升级 sglang 版本或向 sglang 提 bug report，修复 metrics 描述中的非 ASCII 字符。

#### 3.4.2 KESS 心跳上报超时

**错误信息**：
```
向 KESS 上报心跳失败: host=('infra-bjx-rs14-kess-10.idchb1az1.hb1.kwaidc.com', 6603), err=[Errno 110] Connection timed out
```

**原因**：worker 节点网络访问不到 KESS 服务端。

**处理**：网络/集群配置问题，非代码问题。

---

### 3.5 代码设计优化

#### 3.5.1 引擎选择方式割裂

**原始设计**：
- `llm_engine="vLLM"` → 只走 vLLM 路径（`LLMEngine` 枚举只有 `vLLM`）
- `server_cls=SGLangServer` → 绕过 `llm_engine`，走 sglang 路径
- 用户需要写完整内部路径 `ray.llm._internal.serve.engines.sglang.sglang_engine.SGLangServer`

**Ray 内部两条路径**：
```
路径1: llm_engine="vLLM" + LLMServer
  → LLMServer.get_deployment_options()
  → llm_config.get_engine_config()  # 总是 import vllm
  → vllm 路径

路径2: server_cls=SGLangServer
  → SGLangServer.get_deployment_options()
  → placement_options()  # 走 sglang 路径，不碰 vllm
```

**优化方案**：用 `engine` 作为统一入口，内部自动映射到 `llm_engine` + `server_cls`：

```python
_SERVER_CLS_ALIASES = {
    "vllm": {
        "llm_engine": "vLLM",
        "server_cls": "ray.serve.llm.LLMServer",
    },
    "sglang": {
        "llm_engine": "vLLM",
        "server_cls": "ray.llm._internal.serve.engines.sglang.sglang_engine.SGLangServer",
    },
}

def _resolve_engine(raw: str) -> Dict:
    if not raw:
        return {}
    lower = raw.lower().strip()
    if lower in _SERVER_CLS_ALIASES:
        return dict(_SERVER_CLS_ALIASES[lower])
    return {"server_cls": raw}
```

用户只需 `--engine sglang` 或 `ENGINE=sglang`，不再需要关心内部路径。

#### 3.5.2 `deployment_config` 兼容性

**问题**：`serve.deployment().options()` 只接受固定字段（`num_replicas`、`ray_actor_options`、`placement_group_*` 等），不接受 `min_replicas`/`max_replicas`/`target_ongoing_requests` 等 autoscaling 字段。无论 vLLM 还是 SGLang 传这些字段都会报错。

**当前方案**：
- 默认（vLLM）：传全字段
- 指定 `server_cls`：只传 `num_replicas` + `max_ongoing_requests`

**待优化**：应研究 `SGLangServer.get_deployment_options` 支持哪些字段，只过滤不支持的。

#### 3.5.3 `_parse_json_arg` 容错

**问题**：远端 `ENGINE_KWARGS` 值常包含 Python 风格布尔值或多余引号。

**修复**：
```python
def _parse_json_arg(cli_val, env_key):
    raw = cli_val or _env_str(env_key)
    if not raw:
        return None
    raw = raw.strip()
    # strip 外层引号
    if (raw.startswith("'") and raw.endswith("'")) or (
        raw.startswith('"') and raw.endswith('"')
    ):
        raw = raw[1:-1]
    # Python bool → JSON bool
    raw = raw.replace(": True", ": true").replace(": False", ": false").replace(": None", ": null")
    raw = raw.replace(":True", ":true").replace(":False", ":false").replace(":None", ":null")
    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(
            f"Failed to parse {env_key} as JSON: {e}. "
            f"Raw value: {raw[:200]}"
        ) from e
```

---

## 4. 最终成功的配置

### runtime_env.json

```json
{
  "env_vars": {
    "LOG_ENGINE_METRICS": "0",
    "SERVICE_NAME": "grpc_mllm_app_text_reason_service_ray",
    "MODEL_ID": "qwen3.5-30b-a3b",
    "MODEL_SOURCE": "/mmu_mllm_hdd_2/chenqiwen/Qwen3.5-35B-A3B-NVFP4",
    "KESS_OWNER": "mmu",
    "KESS_SHARD_NAME": "s0",
    "KESS_BIZ_DEF": "mmu",
    "SERVER_CLS": "ray.llm._internal.serve.engines.sglang.sglang_engine.SGLangServer",
    "NUM_REPLICAS": "4",
    "GATEWAY_NUM_REPLICAS": "4",
    "ENGINE_KWARGS": "{\"tp_size\": 1, \"mem_fraction_static\": 0.85, \"context_length\": 4096, \"chunked_prefill_size\": 4096, \"attention_backend\": \"triton\", \"mm_attention_backend\": \"sdpa\", \"disable_cuda_graph\": false, \"cuda_graph_max_bs_decode\": 64, \"disable_prefill_cuda_graph\": true}"
  }
}
```

### 提交命令

```bash
ray job submit \
  --runtime-env /tmp/runtime_env.json \
  -- python3 /mmu_mllm_hdd_2/shiyanpeng03/ray/python/ray/serve/inference/start.py
```

### 关键注意事项

1. SGLang 时不要设 `NUM_GPUS_PER_REPLICA`（GPU 归 scheduler bundles）
2. SGLang 时不要设 autoscaling 字段（`min_replicas`/`max_replicas`/`target_ongoing_requests`）
3. `LOG_ENGINE_METRICS=0` 绕过 sglang Unicode bug
4. `MODEL_ID` 必须匹配客户端请求的 model 名称
5. 共享存储路径不需要 `--working-dir`，用绝对路径执行

---

## 5. 待完善项

| # | 项目 | 优先级 | 说明 |
|---|------|--------|------|
| 1 | `setup-dev.py` 软链修复 | 高 | 源码修改不能自动同步到 site-packages，需手动 cp |
| 2 | `_resolve_engine` 同步到远端 | 高 | 本地已实现短名映射，远端仍用完整路径 |
| 3 | `deployment_config` 精细化 | 中 | 应区分支持/不支持的字段，而非 SGLang 时只传两个字段 |
| 4 | sglang Unicode bug | 中 | 需升级 sglang 或提 bug report |
| 5 | KESS 心跳网络 | 低 | 集群网络配置问题，非代码问题 |
| 6 | `ray.serve.llm` 导出 `SGLangServer` | 低 | 向 Ray 上游提 PR |
| 7 | `ray job submit` 环境变量文档 | 低 | 说明哪些变量必须通过 `--runtime-env` 传入 |

---

## 6. 文件变更清单

| 文件 | 变更 |
|------|------|
| `start.py` | `server_cls` → `engine` 统一入口；`_resolve_engine` 短名映射；`deployment_config` SGLang 兼容；`_parse_json_arg` 容错 |
| `builder.py` | `_build_llm_deployment` 根据 `server_cls` 动态选择 Server 类 |
| `config.py` | 无变更 |
| `gateway.py` | 无变更 |
| `kess_registrar.py` | 无变更 |
| `__init__.py` | 架构图含 SGLangServer |
| `test_inference_unit.py` | 无变更 |

---

### 3.6 Gateway protobuf 序列化与类型不匹配（核心架构问题）

#### 3.6.1 cloudpickle 无法序列化 protobuf C-extension 对象

**错误信息**：
```
TypeError: cannot pickle 'classmethod_descriptor' object
FailTuple(ByteSize [obj=<method 'ByteSize' of 'google.protobuf.pyext._message.CMessage' objects>,
           parent=<class 'llm_inference_pb2.ChatRequest'>])
```

**完整调用链**：
```
KESS gRPC Server (Gateway 进程)
  → InferenceGateway.Chat(request=ChatRequest protobuf, context)
      → _dispatch_llm("chat", request, context)
          → _call_llm_handle("chat", handle, request)
              → handle.chat.remote(request)    ← request 是 ChatRequest protobuf
                  → Ray cloudpickle 序列化 request 参数
                      → 遍历 ChatRequest 实例属性
                          → 碰到 ByteSize (classmethod_descriptor)
                              → TypeError: cannot pickle 'classmethod_descriptor'
```

**原因**：Gateway 把 KESS gRPC 反序列化出的 `ChatRequest`（protobuf C-extension 对象）直接作为 `handle.remote()` 的参数。Ray 的 `remote()` 调用会对参数走 **cloudpickle** 序列化路径，而 protobuf C-extension 对象的 `ByteSize`、`SerializeToString` 等方法属于 `classmethod_descriptor`，cloudpickle 不支持序列化这种类型。

#### 3.6.2 Ray Serve 原生 gRPC 如何避免此问题

Ray Serve 原生 gRPC 路径也存在同样的 protobuf 序列化挑战，但通过精心设计的中间层避开了：

```
Ray Serve 原生 gRPC 路径：
  客户端 gRPC 请求
    → Proxy gRPC Server（独立进程）
        → gRPCProxyRequest(request_proto=protobuf, context, ...)
            → serialized_replica_arg()
                → pickle.dumps(gRPCRequest(user_request_proto=protobuf))
                    → bytes（跳过 cloudpickle！）
            → handle.remote(serialized_bytes)
                → Replica 端
                    → pickle.loads(bytes) → gRPCRequest.user_request_proto
                    → deployment.__call__(protobuf_obj)
```

**关键设计**：
1. Proxy 用标准 `pickle.dumps` 把 protobuf 包在 `gRPCRequest` dataclass 中序列化为 **bytes**
2. `handle.remote(bytes)` 传的是 bytes，Ray 对 bytes 参数走零拷贝，**不触发 cloudpickle**
3. Replica 端用 `pickle.loads` 还原出 `gRPCRequest` 对象，取出 `user_request_proto`
4. Deployment 的 `__call__` 直接接收原始 protobuf 对象

**相关源码位置**：
- 序列化：`ray/serve/_private/proxy_request_response.py:256-259`
  ```python
  def serialized_replica_arg(self) -> bytes:
      return pickle.dumps(gRPCRequest(user_request_proto=self._request_proto))
  ```
- Proxy 调用：`ray/serve/_private/proxy.py:974-975`
  ```python
  handle.remote(proxy_request.serialized_replica_arg())
  ```
- Replica 反序列化：`ray/serve/_private/replica.py:1449-1457`
  ```python
  assert isinstance(request_args[0], gRPCRequest)
  request_args = (request.user_request_proto,)  # 取出 protobuf 传给用户方法
  ```
- Replica 返回值序列化：`ray/serve/_private/replica.py:2097-2098`
  ```python
  if request_metadata.is_grpc_request:
      result = (request_metadata.grpc_context, result.SerializeToString())
  ```

**Ray Serve 原生 gRPC Deployment 示例**（`ray/serve/tests/test_config_files/grpc_deployment.py`）：
```python
@serve.deployment
class GrpcDeployment:
    def __call__(self, user_message):               # ← 接收原始 protobuf 对象
        greeting = f"Hello {user_message.name}"
        user_response = serve_pb2.UserDefinedResponse(greeting=greeting)
        return user_response                         # ← 返回 protobuf 对象

    def Method1(self, user_message):                 # ← 自定义方法名 = gRPC method 名
        user_response = serve_pb2.UserDefinedResponse(greeting="from method1")
        return user_response
```

**总结：Ray Serve 原生 gRPC deployment 接受 protobuf 对象时不会有序列化问题**，因为它通过 `pickle.dumps` → `bytes` → `handle.remote(bytes)` → `pickle.loads` 这条路径完全绕过了 cloudpickle。但如果其他代码直接把 protobuf 对象传给 `handle.method.remote(protobuf_obj)`（不经过 Ray Serve Proxy），就会触发 cloudpickle 序列化失败。

#### 3.6.3 我们的架构差异

我们的架构不走 Ray Serve Proxy，而是 Gateway 进程内嵌 KESS gRPC server，直接转发给 LLM handle：

```
我们的路径（有问题）：
  客户端 gRPC 请求
    → KESS gRPC Server（Gateway 进程内）
        → InferenceGateway.Chat(request=ChatRequest protobuf)
            → _call_llm_handle("chat", handle, request)
                → handle.chat.remote(request)     ← 直接传 protobuf，触发 cloudpickle
                    → 💥 TypeError: cannot pickle 'classmethod_descriptor'
```

**Ray Serve 原生 gRPC deployment 的特点**：
- Deployment 直接接收 protobuf 对象（经过 `gRPCRequest` 中间层传输）
- Deployment 直接返回 protobuf 对象（Replica 自动调 `.SerializeToString()` 序列化）
- 这是 Ray Serve 原生 gRPC 的标准模式

**但我们的 LLM Server 不是标准 gRPC deployment**：
- `LLMServer.chat()` / `SGLangServer.chat()` 期望 `ChatCompletionRequest`（pydantic BaseModel），不是 protobuf
- `LLMServer.chat()` 返回 `AsyncGenerator`（流式 SSE 或非流式 response），不是 protobuf
- 所以即使绕过 cloudpickle，protobuf 对象到达 LLMServer 后类型也不匹配

#### 3.6.4 类型不匹配 — 即使序列化成功也会失败

假设 cloudpickle 能序列化 protobuf（标准 pickle 可以），反序列化后 `LLMServer.chat()` 收到的仍是 `ChatRequest` protobuf 对象，而它期望 `ChatCompletionRequest`（pydantic BaseModel）：

```
LLMServer.chat(request=ChatRequest_protobuf)   # ← 期望 ChatCompletionRequest
  → _run_request(request, engine_method="chat")
      → _maybe_add_request_id_to_request(request)
          → request.request_id = ...   ← AttributeError: 'ChatRequest' has no 'request_id'
  或
  → SGLangServer.chat(request=ChatRequest_protobuf)
      → request.messages               ← AttributeError: 'ChatRequest' has no 'messages'
      → request.stream                 ← AttributeError: 'ChatRequest' has no 'stream'
```

**`ChatCompletionRequest` 的字段**（pydantic BaseModel，继承自 sglang 的协议类）：
- `model`, `messages`, `stream`, `temperature`, `top_p`, `max_tokens`, `n`, `stop`, `tools`, `response_format` 等数十个字段

**`ChatRequest` 的字段**（protobuf）：
- `model` (string), `body` (bytes) — 仅 2 个字段

两者完全不兼容，即使解决了序列化问题，类型也不匹配。

#### 3.6.5 我们的 protobuf 透传协议设计

我们定义的 protobuf schema（`src/ray/protobuf/llm_inference.proto`）是一个**透传协议**：

```protobuf
message ChatRequest {
  string model = 1;    // 模型名，如 "qwen3.5-30b-a3b"
  bytes body = 2;      // OpenAI 格式 JSON bytes（透传）
}

message ChatResponse {
  Status status = 1;   // 状态枚举
  bytes body = 2;      // 响应 JSON bytes（透传）
}
```

`body` 字段本质上就是 OpenAI 格式的 JSON 序列化为 bytes。业务侧客户端在 `body` 中填入：
```json
{
  "model": "qwen3.5-30b-a3b",
  "messages": [{"role": "user", "content": "你好"}],
  "stream": false,
  "temperature": 0.7
}
```

#### 3.6.6 修复方案对比

有两种可行的修复路径：

**方案 A：模仿 Ray Serve 原生 — `pickle.dumps(gRPCRequest(...))` 跳过 cloudpickle**

```python
import pickle
from ray.serve._private.common import gRPCRequest

async def _call_llm_handle(self, method_name, handle, request):
    handle_method = getattr(handle, method_name)
    serialized = pickle.dumps(gRPCRequest(user_request_proto=request))
    result = await handle_method.remote(serialized)
    # ...
```

优点：
- 与 Ray Serve 原生 gRPC 路径完全一致，复用成熟的序列化机制
- `pickle.dumps` 性能优于 cloudpickle
- 传输 bytes，Ray 走零拷贝

缺点：
- **LLM Server 端期望 `ChatCompletionRequest`，不是 `gRPCRequest`**。Ray Serve 原生路径中，replica 的 `_unpack_proxy_args` 会自动从 `gRPCRequest` 中取出 `user_request_proto` 传给 deployment。但我们的 LLM Server（`SGLangServer.chat()`）**不是通过 Ray Serve proxy 路径调用的**，它是我们 gateway 直接用 `handle.chat.remote()` 调用的。如果我们传 `bytes`（serialized），LLM Server 收到的就是 bytes，不会经过 `_unpack_proxy_args` 的自动拆包，它无法理解这个 bytes。
- 要让 LLM Server 正确处理，需要在 LLM Server 端也加反序列化逻辑（`pickle.loads` → `gRPCRequest.user_request_proto`），这需要修改 Ray 内部代码（`SGLangServer`/`LLMServer`），不可接受。
- 即使 LLM Server 能拿到 protobuf 对象，类型不匹配问题依然存在（`ChatRequest` ≠ `ChatCompletionRequest`）。

**方案 B（推荐）：Gateway 中 protobuf body → pydantic 请求对象**

```python
def _convert_request(method_name, request):
    cls = _cls_map[method_name]  # e.g., ChatCompletionRequest
    body_dict = json.loads(request.body)
    body_dict.setdefault("model", request.model)
    body_dict["stream"] = False
    return cls(**body_dict)

async def _call_llm_handle(self, method_name, handle, request):
    llm_request = _convert_request(method_name, request)
    handle_method = getattr(handle, method_name)
    result = None
    async for chunk in await handle_method.remote(llm_request):
        if result is None:
            result = chunk
        else:
            break
    return result
```

优点：
- 同时解决**序列化问题**（pydantic BaseModel 可被 cloudpickle 正常序列化）和**类型不匹配问题**（LLM Server 收到正确的 `ChatCompletionRequest`）
- 不需要修改 Ray 内部代码
- 利用了我们 protobuf 透传协议的设计（`body` 本就是 OpenAI JSON bytes）
- 语义清晰：Gateway 做协议转换，LLM Server 做推理

缺点：
- 多一次 `json.loads` + pydantic 构造的开销（微秒级，可忽略）
- 流式响应需要特殊处理（当前版本只支持非流式）

**选择方案 B**，因为它同时解决了两个问题，且不需要侵入 Ray 内部代码。

---

#### 3.6.7 为什么不能只用 `pickle.dumps` 跳过 cloudpickle

`pickle.dumps(gRPCRequest(user_request_proto=protobuf))` 这个技巧在 **Ray Serve 原生 gRPC 路径** 中能工作，是因为有一整套配套机制：

```
Ray Serve 原生完整链路：
  1. Proxy: pickle.dumps(gRPCRequest(user_request_proto=protobuf)) → bytes
  2. Proxy: handle.remote(bytes)  →  bytes 走零拷贝，不触发 cloudpickle
  3. ReplicaActor._preprocess_request_args: pickle.loads(bytes) → gRPCRequest
  4. Replica._unpack_proxy_args: gRPCRequest.user_request_proto → 取出 protobuf
  5. UserCallableWrapper: deployment.__call__(protobuf)  →  直接传 protobuf 对象
  6. Replica: result.SerializeToString() → bytes 返回 proxy
```

步骤 3-5 是 Ray Serve replica 内部自动完成的，用户无感知。但我们直接调用 `handle.chat.remote()` 时，**绕过了整个 replica 的预处理链**，目标方法收到的就是 `remote()` 传入的原始参数。所以：

- 如果传 `bytes`：`SGLangServer.chat()` 收到 bytes → 不知道怎么处理
- 如果传 `protobuf`：`SGLangServer.chat()` 收到 `ChatRequest` → 类型不匹配
- 如果传 `ChatCompletionRequest`：`SGLangServer.chat()` 收到正确类型 → 正常工作

---

### 3.7 流式 Chat 请求的含义与当前限制

#### 3.7.1 什么是流式 Chat 请求

在 OpenAI 兼容的 Chat 推理接口中，`stream` 参数决定响应模式：

- **非流式** (`stream=false`)：客户端发送请求后等待，服务端推理完成后一次性返回完整响应（`ChatCompletionResponse`）。延迟 = 推理总时间。
- **流式** (`stream=true`)：服务端在推理过程中逐 token 生成响应，每生成一部分就通过 SSE (Server-Sent Events) 推送给客户端。格式为 `data: {json}\n\n`，最后一个 chunk 是 `data: [DONE]\n\n`。

**流式的优势**：
- 用户感知延迟低：第一个 token 很快就返回（TTFT, Time To First Token）
- 适合交互式场景：用户可以看到逐步生成的文本
- 支持中途取消：客户端断开连接即可停止推理

**Ray LLM 的流式实现**：
```python
# SGLangServer.chat() 返回 AsyncGenerator
async def chat(self, request: ChatCompletionRequest, ...):
    if request.stream:
        # 流式：yield SSE chunk 字符串
        async for delta_text, finish_reason in stream:
            yield f"data: {json.dumps(chunk)}\n\n"
    else:
        # 非流式：yield 完整 ChatCompletionResponse 对象
        yield response
```

#### 3.7.2 当前 protobuf 接口的限制

我们的 protobuf 接口定义为 **unary-unary**（请求-响应模式）：
```protobuf
rpc Chat(ChatRequest) returns (ChatResponse);  // unary-unary
```

这意味着：
1. 客户端发送一个 `ChatRequest`
2. 服务端返回**一个** `ChatResponse`
3. 无法在推理过程中逐步推送中间结果

如果客户端在 `body` 中设 `"stream": true`，当前 Ray LLM 的 `chat()` 会 yield 多个 SSE chunk，但 Gateway 只能收集所有 chunk 拼接后放入一个 `ChatResponse.body` 返回。客户端收到的仍然是**一个**完整响应，只是 `body` 内容是 SSE 格式的拼接文本。

#### 3.7.3 支持 true streaming 的方案（后续迭代）

要实现真正的流式推送，需要将 protobuf 接口改为 **server-streaming**：
```protobuf
rpc Chat(ChatRequest) returns (stream ChatResponse);  // server-streaming
```

这样 Gateway 可以逐 chunk 向客户端推送 `ChatResponse`。但这需要：
1. 修改 `.proto` 定义并重新生成 `pb2` / `pb2_grpc` 代码
2. 修改 `kess_registrar.py` 中的 gRPC handler 注册
3. 修改 `_dispatch_llm()` 为 streaming 模式
4. 客户端代码适配 streaming 消费

**当前选择**：只支持非流式（`stream=false`），在 `_convert_request()` 中强制设 `body_dict["stream"] = False`。

---

## 4. 最终成功的配置
