# Ray _system_config 参数传播机制与生命周期分析

## 背景问题

在大规模 Ray 集群中，需要调整以下两个关键参数来优化资源同步开销：

```json
{
    "raylet_report_resources_period_milliseconds": 5000,
    "ray_syncer_message_refresh_interval_ms": 30000
}
```

核心问题：
1. 这些参数是否需要在每个 Raylet 节点单独配置？还是只在 Head 节点配置，Worker 通过 `ray start --address=<head_ip>:6379 --block` 连接后自动生效？
2. 如果 Head 节点修改参数重启后，已经运行的 Worker Raylet 重新连接 GCS，参数值会自动更新吗？

---

## 一、参数定义（C++ 源码级）

### 1.1 `raylet_report_resources_period_milliseconds`（默认 100ms）

`src/ray/common/ray_config_def.h:64-65`：

```cpp
/// The duration between reporting resources sent by the raylets.
RAY_CONFIG(uint64_t, raylet_report_resources_period_milliseconds, 100)
```

- **含义**：Raylet 每隔多久从 `LocalResourceManager` 拉取本地资源快照并通过 RaySyncer 上报给 GCS
- **作用方向**：Raylet → GCS（主动推送）
- **执行线程**：Raylet 主线程

### 1.2 `ray_syncer_message_refresh_interval_ms`（默认 3000ms）

`src/ray/common/ray_config_def.h:439-442`：

```cpp
/// Due to the protocol drawback, raylet needs to refresh the message if
/// no message is received for a while.
/// Refer to https://tinyurl.com/n6kvsp87 for more details
RAY_CONFIG(int64_t, ray_syncer_message_refresh_interval_ms, 3000)
```

- **含义**：如果超过这个时间没收到某节点的资源更新，Raylet 就重新应用该节点最后一次收到的资源视图（`AddOrUpdateNode`），作为 RaySyncer 协议的消息丢失补偿机制
- **作用方向**：GCS → Raylet（被动刷新阈值）
- **执行线程**：Raylet 主线程

### 1.3 `RAY_CONFIG` 宏展开逻辑

`src/ray/common/ray_config.h:72-77`：

```cpp
#define RAY_CONFIG(type, name, default_value)                       \
 private:                                                           \
  type name##_ = ReadEnv<type>("RAY_" #name, #type, default_value); \
                                                                    \
 public:                                                            \
  inline type &name() { return name##_; }
```

每个 `RAY_CONFIG` 宏展开后，在 `RayConfig` 类中生成：
- 一个私有成员变量 `name_`（初始值先尝试从环境变量 `RAY_<name>` 读取，否则用默认值）
- 一个公开的 getter 方法 `name()`，返回该成员变量的引用

### 1.4 参数约束关系

```
约束: ray_syncer_message_refresh_interval_ms >> raylet_report_resources_period_milliseconds
```

`ray_syncer_message_refresh_interval_ms` 必须**远大于** `raylet_report_resources_period_milliseconds`，否则 Raylet 会误判远端节点资源视图过期，频繁触发不必要的刷新操作。建议保持至少 6 倍比率（如 5000ms : 30000ms）。

### 1.5 两个参数的作用对比

| 参数 | 作用 | 方向 | 线程 |
|------|------|------|------|
| `raylet_report_resources_period_milliseconds` | 主动**推送**频率 | Raylet → GCS | Raylet 主线程 |
| `ray_syncer_message_refresh_interval_ms` | 被动**刷新**阈值 | GCS → Raylet | Raylet 主线程 |

---

## 二、参数在 C++ 运行时的使用

### 2.1 `raylet_report_resources_period_milliseconds` 的使用

**第一步：Raylet main 从 RayConfig 读取值写入 NodeManagerConfig**

`src/ray/raylet/main.cc:609-610`：

```cpp
node_manager_config.report_resources_period_ms =
    RayConfig::instance().raylet_report_resources_period_milliseconds();
```

**第二步：NodeManager 构造函数保存到成员变量**

`src/ray/raylet/node_manager.cc:204`：

```cpp
report_resources_period_ms_(config.report_resources_period_ms),
```

`src/ray/raylet/node_manager.h:113` 定义了该字段：

```cpp
/// The time between reports resources in milliseconds.
uint64_t report_resources_period_ms;
```

`src/ray/raylet/node_manager.h:833` 定义了成员变量：

```cpp
/// The period used for the resources report timer.
uint64_t report_resources_period_ms_;
```

**第三步：注册到 RaySyncer 作为定时上报周期**

`src/ray/raylet/node_manager.cc:345-350`：

```cpp
ray_syncer_.Register(
    /* message_type */ syncer::MessageType::RESOURCE_VIEW,
    /* reporter */ &cluster_resource_scheduler_.GetLocalResourceManager(),
    /* receiver */ this,
    /* pull_from_reporter_interval_ms */
    report_resources_period_ms_);
```

这里将 `report_resources_period_ms_` 作为 RaySyncer 的定时拉取周期。RaySyncer 会按此周期从 `LocalResourceManager` 获取本节点的资源快照，并通过 gRPC 双向流推送给 GCS。

### 2.2 `ray_syncer_message_refresh_interval_ms` 的使用

`src/ray/raylet/scheduling/cluster_resource_manager.cc:31-44`：

```cpp
timer_->RunFnPeriodically(
    [this]() {
        auto syncer_delay = absl::Milliseconds(
            RayConfig::instance().ray_syncer_message_refresh_interval_ms());
        for (auto &[node_id, resource] : received_node_resources_) {
            auto modified_ts = GetNodeResourceModifiedTs(node_id);
            if (modified_ts && *modified_ts + syncer_delay < absl::Now()) {
                AddOrUpdateNode(node_id, resource);
            }
        }
    },
    RayConfig::instance().ray_syncer_message_refresh_interval_ms(),
    "ClusterResourceManager.ResetRemoteNodeView");
```

逻辑说明：
1. `ClusterResourceManager` 构造时注册一个定时任务，间隔为 `ray_syncer_message_refresh_interval_ms`
2. 每次触发时，遍历所有远端节点的资源视图
3. 如果某节点的最后修改时间戳距今超过 `ray_syncer_message_refresh_interval_ms`，说明可能存在消息丢失
4. 调用 `AddOrUpdateNode` 重新应用该节点上一次的资源视图，作为补偿

---

## 三、配置传播的完整代码链路

### 阶段一：Python 层接收用户配置

#### 3.1 `ray.init()` 入口

`python/ray/_private/worker.py:1584-1644`：

```python
# ray.init() 的参数定义
_system_config: Configuration for overriding
    RayConfig defaults. For testing purposes ONLY.

# 解析参数
_system_config: Optional[Dict[str, str]] = kwargs.pop("_system_config", None)
```

用户调用 `ray.init(_system_config={...})` 时，`_system_config` 字典被传入。

#### 3.2 存入 `RayParams`

`python/ray/_private/parameter.py:242`：

```python
self._system_config = _system_config or {}
```

#### 3.3 Worker 节点设置校验（禁止 Worker 设置）

`python/ray/_private/node.py:126-133`：

```python
if (
    ray_params._system_config
    and len(ray_params._system_config) > 0
    and (not head and not connect_only)
):
    raise ValueError(
        "System config parameters can only be set on the head node."
    )
```

非 Head 节点尝试通过 `_system_config` 设置参数时，直接抛出 `ValueError`。

#### 3.4 初始化 `self._config`

`python/ray/_private/node.py:153`：

```python
self._config = ray_params._system_config or {}
```

### 阶段二：Head 节点启动 GCS Server

#### 3.5 序列化配置为 Base64

`python/ray/_private/services.py:198-199`：

```python
def serialize_config(config):
    return base64.b64encode(json.dumps(config).encode("utf-8")).decode("utf-8")
```

将 Python 字典 `{"raylet_report_resources_period_milliseconds": 5000, ...}` 转为 JSON 字符串，再 Base64 编码。

#### 3.6 作为命令行参数传给 GCS Server 进程

`python/ray/_private/services.py:1494-1497`：

```python
command = [
    GCS_SERVER_EXECUTABLE,
    f"--log_dir={log_dir}",
    f"--config_list={serialize_config(config)}",
    ...
]
```

GCS Server 可执行文件路径定义在 `python/ray/_private/services.py:57-58`：

```python
RAYLET_EXECUTABLE = os.path.join(
    RAY_PATH, "core", "src", "ray", "raylet", "raylet" + EXE_SUFFIX
)
GCS_SERVER_EXECUTABLE = os.path.join(
    RAY_PATH, "core", "src", "ray", "gcs", "gcs_server" + EXE_SUFFIX
)
```

### 阶段三：GCS Server 接收并存储配置

#### 3.7 GCS Server main 解码 Base64 并初始化 RayConfig

`src/ray/gcs/gcs_server_main.cc:108-120`：

```cpp
std::string config_list;
RAY_CHECK(absl::Base64Unescape(FLAGS_config_list, &config_list))
    << "config_list is not a valid base64-encoded string.";
...
RayConfig::instance().initialize(config_list);
```

GCS Server 进程本身也会用这份 config 初始化自己的 `RayConfig` 单例。

#### 3.8 将 config_list 存入 GcsServerConfig 结构

`src/ray/gcs/gcs_server_main.cc:179`：

```cpp
gcs_server_config.raylet_config_list = config_list;
```

`src/ray/gcs/gcs_server.h:55-73` 定义了 `GcsServerConfig`：

```cpp
struct GcsServerConfig {
  std::string grpc_server_name = "GcsServer";
  uint16_t grpc_server_port = 0;
  ...
  // This includes the config list of raylet.
  std::string raylet_config_list;
  std::string session_name;
};
```

#### 3.9 GcsServer 将 config 注入 GcsInternalKVManager

`src/ray/gcs/gcs_server.cc:647-650`：

```cpp
kv_manager_ = std::make_unique<GcsInternalKVManager>(
    std::make_unique<StoreClientInternalKV>(std::move(store_client)),
    config_.raylet_config_list,
    io_context);
```

#### 3.10 GcsInternalKVManager 构造函数保存为 `const` 成员

`src/ray/gcs/gcs_kv_manager.h:103-110`：

```cpp
class GcsInternalKVManager : public rpc::InternalKVGcsServiceHandler {
 public:
  explicit GcsInternalKVManager(std::unique_ptr<InternalKVInterface> kv_instance,
                                std::string raylet_config_list,
                                instrumented_io_context &io_context)
      : kv_instance_(std::move(kv_instance)),
        raylet_config_list_(std::move(raylet_config_list)),
        io_context_(io_context) {}
  ...
 private:
  std::unique_ptr<InternalKVInterface> kv_instance_;
  const std::string raylet_config_list_;  // <-- 存储为 const，不可变
  ...
};
```

#### 3.11 HandleGetInternalConfig — 响应 Worker 的配置请求

`src/ray/gcs/gcs_kv_manager.cc:151-157`：

```cpp
void GcsInternalKVManager::HandleGetInternalConfig(
    rpc::GetInternalConfigRequest request,
    rpc::GetInternalConfigReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  reply->set_config(raylet_config_list_);
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}
```

该 RPC handler 直接返回内存中的 `raylet_config_list_` 字符串。这是 GCS 提供配置给所有节点的唯一接口。

对应的 gRPC 服务注册在 `src/ray/gcs/grpc_services.cc:142`：

```cpp
RPC_SERVICE_HANDLER(
    InternalKVGcsService, GetInternalConfig, max_active_rpcs_per_handler_)
```

### 阶段四：Worker 节点从 GCS 拉取配置

#### 3.12 Worker 的 `start_ray_processes()` 从 GCS 拉取 system_config

`python/ray/_private/node.py:1338-1362`：

```python
def start_ray_processes(self):
    """Start all of the processes on the node."""
    ...
    if not self.head:
        # Get the system config from GCS first if this is a non-head node.
        gcs_options = ray._raylet.GcsClientOptions.create(
            self.gcs_address,
            self.cluster_id.hex(),
            allow_cluster_id_nil=False,
            fetch_cluster_id_if_nil=False,
        )
        global_state = ray._private.state.GlobalState()
        global_state._initialize_global_state(gcs_options)
        new_config = global_state.get_system_config()
        assert self._config.items() <= new_config.items(), (
            "The system config from GCS is not a superset of the local"
            " system config. There might be a configuration inconsistency"
            " issue between the head node and non-head nodes."
            f" Local system config: {self._config},"
            f" GCS system config: {new_config}"
        )
        self._config = new_config
```

调用链：`global_state.get_system_config()` →

`python/ray/_private/state.py:813-816`：

```python
def get_system_config(self):
    """Get the system config of the cluster."""
    accessor = self._connect_and_get_accessor()
    return json.loads(accessor.get_system_config())
```

底层通过 Cython 绑定调用 C++ 的 `GcsClient::InternalKV::AsyncGetInternalConfig` gRPC 接口，最终请求到 GCS Server 的 `HandleGetInternalConfig`。

### 阶段五：Raylet 进程初始化 RayConfig

#### 3.13 Raylet main 通过 GcsClient 异步获取配置

**注意：这里是关键——Raylet C++ 进程并不是通过命令行参数 `--config_list` 获取配置，而是在启动后通过 gRPC 从 GCS 拉取。**

`src/ray/raylet/main.cc:330-338`（连接 GCS）：

```cpp
// Initialize gcs client
std::unique_ptr<ray::gcs::GcsClient> gcs_client;
ray::gcs::GcsClientOptions client_options(FLAGS_gcs_address,
                                          cluster_id,
                                          /*allow_cluster_id_nil=*/false,
                                          /*fetch_cluster_id_if_nil=*/false);
gcs_client = std::make_unique<ray::gcs::GcsClient>(client_options, node_ip_address);

RAY_CHECK_OK(gcs_client->Connect(main_service));
```

`src/ray/raylet/main.cc:498-503`（异步拉取配置并初始化 RayConfig）：

```cpp
gcs_client->InternalKV().AsyncGetInternalConfig(
    [&](::ray::Status status,
        const std::optional<std::string> &stored_raylet_config) {
  RAY_CHECK_OK(status);
  RAY_CHECK(stored_raylet_config.has_value());
  RayConfig::instance().initialize(*stored_raylet_config);
```

这段代码说明：
1. Raylet 启动后，通过 `GcsClient` 调用 `AsyncGetInternalConfig` RPC
2. GCS 返回存储在 `GcsInternalKVManager` 中的 `raylet_config_list_`
3. Raylet 用返回的 JSON 字符串调用 `RayConfig::instance().initialize()`

#### 3.14 RayConfig::initialize 的完整逻辑

`src/ray/common/ray_config.cc:24-78`：

```cpp
RayConfig &RayConfig::instance() {
  static RayConfig config;  // 进程级静态单例，只构造一次
  return config;
}

RayConfig::RayConfig() { initialize(""); }  // 构造时用空字符串初始化（使用默认值 + 环境变量）

void RayConfig::initialize(const std::string &config_list) {
  // 第一步：用环境变量覆盖默认值
  #define RAY_CONFIG(type, name, default_value) \
    name##_ = ReadEnv<type>("RAY_" #name, #type, default_value);
  #include "ray/common/ray_config_def.h"
  #undef RAY_CONFIG

  if (config_list.empty()) {
    return;  // 构造时传空串，到此结束
  }

  try {
    // 第二步：解析 JSON 配置，用 JSON 中的值覆盖
    json config_map = json::parse(config_list);

    #define RAY_CONFIG(type, name, default_value) \
      if (pair.key() == #name) {                  \
        name##_ = pair.value().get<type>();       \
        continue;                                 \
      }

    for (const auto &pair : config_map.items()) {
      #include "ray/common/ray_config_def.h"
      // 未知参数直接 FATAL
      RAY_LOG(FATAL) << "Received unexpected config parameter " << pair.key();
    }
    #undef RAY_CONFIG
    ...
  }
}
```

**优先级顺序**：
1. 默认值（`ray_config_def.h` 中定义）
2. 环境变量 `RAY_<name>` 覆盖
3. JSON config_list（来自 GCS）最终覆盖

**关键点**：`initialize()` 虽然可以被调用多次（先构造时空串，再 GCS 回调时真正初始化），但 `RayConfig::instance()` 返回的是同一个 `static` 对象。一旦 Raylet 进程启动并完成 `AsyncGetInternalConfig` 回调，配置值就固定了，**进程生命周期内不会再调用 `initialize`**。

#### 3.15 Raylet main 中后续使用 RayConfig 的地方

`src/ray/raylet/main.cc:507-612`（回调函数内，在 `initialize` 之后）：

```cpp
// 使用初始化后的 RayConfig 配置各种组件
const bool pg_enabled = RayConfig::instance().process_group_cleanup_enabled();
const bool subreaper_enabled =
    RayConfig::instance().kill_child_processes_on_worker_exit_with_raylet_subreaper();
...
// 关键：将 raylet_report_resources_period_milliseconds 写入 NodeManagerConfig
node_manager_config.report_resources_period_ms =
    RayConfig::instance().raylet_report_resources_period_milliseconds();  // line 609-610
node_manager_config.record_metrics_period_ms =
    RayConfig::instance().metrics_report_interval_ms() / 2;              // line 611-612
```

这些值从 `RayConfig` 读取后写入 `NodeManagerConfig` 结构体，传给 `NodeManager` 构造函数，**之后不会再更新**。

---

## 四、问题一：参数是否只需在 Head 节点配置？

### 结论：是的，只需在 Head 节点配置，Worker 节点自动获取

### 代码证据总结

| 步骤 | 代码位置 | 说明 |
|------|---------|------|
| 1 | `python/ray/_private/node.py:126-133` | Worker 节点设置 `_system_config` 会抛 `ValueError` |
| 2 | `python/ray/_private/services.py:198-199` | 配置被序列化为 Base64 JSON |
| 3 | `python/ray/_private/services.py:1497` | Base64 JSON 作为 `--config_list` 传给 GCS Server |
| 4 | `src/ray/gcs/gcs_server_main.cc:109` | GCS Server 解码 Base64 |
| 5 | `src/ray/gcs/gcs_server.cc:647-650` | config 注入 `GcsInternalKVManager` |
| 6 | `src/ray/gcs/gcs_kv_manager.cc:155` | `HandleGetInternalConfig` 返回存储的 config |
| 7 | `python/ray/_private/node.py:1354` | Worker 的 `start_ray_processes()` 调用 `get_system_config()` |
| 8 | `src/ray/raylet/main.cc:498-503` | Raylet C++ 进程通过 `AsyncGetInternalConfig` 从 GCS 拉取 |

### 正确的配置方式

**Head 节点**：通过 `ray.init()` 或 `ray start --head` 时设置 `_system_config`：

```python
ray.init(
    _system_config={
        "raylet_report_resources_period_milliseconds": 5000,
        "ray_syncer_message_refresh_interval_ms": 30000,
    }
)
```

**Worker 节点**：只需连接，不需要也**不允许**重复配置：

```bash
ray start --address=10.81.0.34:6379 --block \
    --memory=120000000000 --num-cpus=15 --num-gpus=1 \
    --dashboard-agent-listen-port=0
```

---

## 五、问题二：Head 重启后，已运行的 Worker Raylet 是否自动更新配置？

### 结论：不会自动更新

### 代码证据链

#### 5.1 `RayConfig` 是进程级单例，`initialize` 只被调用一次

`src/ray/common/ray_config.cc:24-27`：

```cpp
RayConfig &RayConfig::instance() {
  static RayConfig config;  // C++ static local —— 进程生命周期内只构造一次
  return config;
}
```

`static` 局部变量在 C++ 中保证只初始化一次。虽然 `initialize()` 方法本身技术上可以被再次调用，但在 Raylet 的 `main()` 函数中，**只有 `AsyncGetInternalConfig` 的回调中调用了一次**（`main.cc:503`），此后再无调用点。

#### 5.2 NodeManagerConfig 在构造时固化

`src/ray/raylet/main.cc:609-610`：

```cpp
node_manager_config.report_resources_period_ms =
    RayConfig::instance().raylet_report_resources_period_milliseconds();
```

`src/ray/raylet/node_manager.cc:204`：

```cpp
report_resources_period_ms_(config.report_resources_period_ms),
```

`report_resources_period_ms_` 是 `NodeManager` 的成员变量，在构造时赋值后不再修改。

#### 5.3 RaySyncer 注册时的周期也是固定的

`src/ray/raylet/node_manager.cc:345-350`：

```cpp
ray_syncer_.Register(
    syncer::MessageType::RESOURCE_VIEW,
    &cluster_resource_scheduler_.GetLocalResourceManager(),
    this,
    report_resources_period_ms_);  // 注册时确定周期，不会动态更新
```

#### 5.4 ClusterResourceManager 的定时器也是一次性注册

`src/ray/raylet/scheduling/cluster_resource_manager.cc:31-43`：

```cpp
timer_->RunFnPeriodically(
    [this]() { ... },
    RayConfig::instance().ray_syncer_message_refresh_interval_ms(),  // 注册时读取，之后固定
    "ClusterResourceManager.ResetRemoteNodeView");
```

`RunFnPeriodically` 在注册时就确定了间隔值，后续不会重新读取 `RayConfig`。

#### 5.5 GCS 重连不会触发配置重新拉取

Raylet 的 GCS 重连逻辑只处理节点注册、心跳恢复等操作，**没有重新调用 `AsyncGetInternalConfig` 和 `RayConfig::instance().initialize()` 的代码路径**。

#### 5.6 GcsInternalKVManager 中的 config 是 `const`

`src/ray/gcs/gcs_kv_manager.h:145`：

```cpp
const std::string raylet_config_list_;
```

即使 GCS Server 重启，新的 `GcsInternalKVManager` 实例会用新的 `config_list` 构造，但这只影响**新连接**的节点请求。已经运行的 Raylet 进程不会再次请求。

### 场景分析

| 场景 | Worker Raylet 是否获取新配置 | 原因 |
|------|:---:|------|
| Head 重启，Worker Raylet **也重启**（`ray stop && ray start --address=...`） | **是** | 重新走 `start_ray_processes` → Python 层从 GCS 拉取新 config → Raylet C++ 通过 `AsyncGetInternalConfig` 再次拉取 → `RayConfig::instance().initialize()` 用新值初始化 |
| Head 重启，Worker Raylet **未重启**（仅 GCS 连接恢复） | **否** | Raylet 进程还在运行，`RayConfig` 单例已用旧值初始化完成，GCS 重连只恢复注册/心跳，没有配置重新拉取的代码路径 |

### 配置变更的正确操作流程

修改 `_system_config` 参数后，**必须重启所有节点的 Raylet** 才能生效。仅重启 Head 节点不够。

```bash
# 1. 停止 Head 节点
ray stop

# 2. 所有 Worker 节点上执行
ray stop

# 3. 重启 Head 节点（带新参数）
ray start --head ...
# 或通过 ray.init(_system_config={...})

# 4. 所有 Worker 节点重新连接
ray start --address=<head_ip>:6379 --block \
    --memory=120000000000 --num-cpus=15 --num-gpus=1 \
    --dashboard-agent-listen-port=0
```

---

## 六、完整配置传播链路图

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                        配置传播全流程（含代码位置）                            │
│                                                                              │
│  用户调用:                                                                   │
│  ray.init(_system_config={"raylet_report_resources_period_ms": 5000, ...})  │
│       │                                                                      │
│       ▼                                                                      │
│  [Python] worker.py:1644                                                    │
│  _system_config 解析为 dict                                                  │
│       │                                                                      │
│       ▼                                                                      │
│  [Python] parameter.py:242                                                  │
│  self._system_config = _system_config or {}                                 │
│       │                                                                      │
│       ▼                                                                      │
│  [Python] node.py:153                                                       │
│  self._config = ray_params._system_config or {}                             │
│       │                                                                      │
│  ┌────┴────────────────────────────────────────────┐                        │
│  │            Head Node 启动                        │                        │
│  │                                                  │                        │
│  │  [Python] services.py:198                        │                        │
│  │  serialize_config(config)                        │                        │
│  │  → base64(json.dumps(config))                    │                        │
│  │       │                                          │                        │
│  │  ┌────┴───────────┐  ┌─────────────────────┐    │                        │
│  │  │ GCS Server      │  │ Head Raylet         │    │                        │
│  │  │                 │  │                     │    │                        │
│  │  │ services.py:1497│  │ main.cc:498-503     │    │                        │
│  │  │ --config_list=  │  │ AsyncGetInternal-   │    │                        │
│  │  │   <base64>      │  │ Config() → GCS      │    │                        │
│  │  │       │         │  │       │             │    │                        │
│  │  │       ▼         │  │       ▼             │    │                        │
│  │  │ gcs_server_     │  │ RayConfig::instance │    │                        │
│  │  │ main.cc:109     │  │ ().initialize(      │    │                        │
│  │  │ Base64Unescape  │  │   config)           │    │                        │
│  │  │       │         │  │   [只调用一次]       │    │                        │
│  │  │       ▼         │  │       │             │    │                        │
│  │  │ gcs_server_     │  │       ▼             │    │                        │
│  │  │ main.cc:120     │  │ main.cc:609-610     │    │                        │
│  │  │ RayConfig::     │  │ node_manager_config │    │                        │
│  │  │ initialize()    │  │ .report_resources_  │    │                        │
│  │  │       │         │  │ period_ms = ...     │    │                        │
│  │  │       ▼         │  └─────────────────────┘    │                        │
│  │  │ gcs_server_     │                             │                        │
│  │  │ main.cc:179     │                             │                        │
│  │  │ config.raylet_  │                             │                        │
│  │  │ config_list =   │                             │                        │
│  │  │ config_list     │                             │                        │
│  │  │       │         │                             │                        │
│  │  │       ▼         │                             │                        │
│  │  │ gcs_server.cc   │                             │                        │
│  │  │ :647-650        │                             │                        │
│  │  │ GcsInternal-    │                             │                        │
│  │  │ KVManager(      │                             │                        │
│  │  │   config_list)  │                             │                        │
│  │  │       │         │                             │                        │
│  │  │       ▼         │                             │                        │
│  │  │ gcs_kv_manager  │                             │                        │
│  │  │ .h:145          │                             │                        │
│  │  │ const string    │                             │                        │
│  │  │ raylet_config_  │                             │                        │
│  │  │ list_           │  ← HandleGetInternalConfig  │                        │
│  │  │ (内存常量)      │     返回此值给所有请求者     │                        │
│  │  └────────────────┘                              │                        │
│  └──────────────────────────────────────────────────┘                        │
│                                                                              │
│  ┌──────────────────────────────────────────────────┐                        │
│  │            Worker Node 启动                       │                        │
│  │                                                   │                        │
│  │  ray start --address=<head>:6379                  │                        │
│  │       │                                           │                        │
│  │       ▼                                           │                        │
│  │  [Python] node.py:1344-1362                       │                        │
│  │  if not self.head:                                │                        │
│  │      global_state.get_system_config()             │                        │
│  │            │                                      │                        │
│  │            ▼                                      │                        │
│  │  [Python] state.py:813-816                        │                        │
│  │  accessor.get_system_config()                     │                        │
│  │            │                                      │                        │
│  │            ▼  (gRPC to GCS)                       │                        │
│  │  [C++] gcs_kv_manager.cc:151-157                  │                        │
│  │  HandleGetInternalConfig →                        │                        │
│  │  reply->set_config(raylet_config_list_)           │                        │
│  │            │                                      │                        │
│  │            ▼  (返回 JSON string)                   │                        │
│  │  [Python] node.py:1362                            │                        │
│  │  self._config = new_config                        │                        │
│  │            │                                      │                        │
│  │            ▼                                      │                        │
│  │  [C++] Worker Raylet main.cc:498-503              │                        │
│  │  gcs_client->InternalKV()                         │                        │
│  │    .AsyncGetInternalConfig(...)                    │                        │
│  │            │                                      │                        │
│  │            ▼                                      │                        │
│  │  RayConfig::instance().initialize(config)         │                        │
│  │  [只调用一次，此后不再更新]                        │                        │
│  │            │                                      │                        │
│  │            ▼                                      │                        │
│  │  main.cc:609-610                                  │                        │
│  │  node_manager_config.report_resources_period_ms   │                        │
│  │            │                                      │                        │
│  │            ▼                                      │                        │
│  │  node_manager.cc:204                              │                        │
│  │  report_resources_period_ms_(config.xxx)          │                        │
│  │            │                                      │                        │
│  │            ▼                                      │                        │
│  │  node_manager.cc:345-350                          │                        │
│  │  ray_syncer_.Register(...,                        │                        │
│  │    report_resources_period_ms_)                   │                        │
│  │  [注册完成，周期固定]                              │                        │
│  └───────────────────────────────────────────────────┘                        │
│                                                                              │
│  ⚠ Raylet 进程启动后，RayConfig 不再更新                                     │
│  ⚠ GCS 重连只恢复心跳/注册，不重新下发配置                                    │
│  ⚠ NodeManager 的定时器周期在注册时固定，不会动态调整                          │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 七、关键源码索引

| 文件 | 行号 | 说明 |
|------|------|------|
| **参数定义** | | |
| `src/ray/common/ray_config_def.h` | 64-65 | `raylet_report_resources_period_milliseconds` 定义（默认 100ms） |
| `src/ray/common/ray_config_def.h` | 439-442 | `ray_syncer_message_refresh_interval_ms` 定义（默认 3000ms） |
| `src/ray/common/ray_config.h` | 60-111 | `RayConfig` 类定义，`RAY_CONFIG` 宏展开逻辑，单例模式 |
| `src/ray/common/ray_config.cc` | 24-78 | `RayConfig::instance()` 静态单例 + `initialize()` 完整实现 |
| **Python 层配置入口** | | |
| `python/ray/_private/worker.py` | 1584, 1644 | `ray.init()` 的 `_system_config` 参数定义和解析 |
| `python/ray/_private/parameter.py` | 242 | `RayParams._system_config` 存储 |
| `python/ray/_private/node.py` | 126-133 | 禁止 Worker 节点设置 `_system_config`（抛 ValueError） |
| `python/ray/_private/node.py` | 153 | `self._config` 初始化 |
| `python/ray/_private/services.py` | 198-199 | `serialize_config()` — Base64(JSON) 编码 |
| **GCS Server 配置存储** | | |
| `python/ray/_private/services.py` | 1494-1497 | GCS Server 启动命令组装（`--config_list=<base64>`） |
| `src/ray/gcs/gcs_server_main.cc` | 108-120 | GCS Server main 解码 Base64 + `RayConfig::initialize()` |
| `src/ray/gcs/gcs_server_main.cc` | 179 | `gcs_server_config.raylet_config_list = config_list` |
| `src/ray/gcs/gcs_server.h` | 55-73 | `GcsServerConfig` 结构体定义 |
| `src/ray/gcs/gcs_server.cc` | 647-650 | `GcsInternalKVManager` 构造，注入 config |
| `src/ray/gcs/gcs_kv_manager.h` | 103-148 | `GcsInternalKVManager` 类定义，`raylet_config_list_` 为 `const` |
| `src/ray/gcs/gcs_kv_manager.cc` | 151-157 | `HandleGetInternalConfig` — 响应配置请求的 RPC handler |
| `src/ray/gcs/grpc_services.cc` | 142 | `GetInternalConfig` gRPC 服务注册 |
| **Worker 节点配置拉取** | | |
| `python/ray/_private/node.py` | 1344-1362 | Worker 的 `start_ray_processes()` 从 GCS 拉取 system_config |
| `python/ray/_private/state.py` | 813-816 | `GlobalState.get_system_config()` — Python 层 GCS 查询 |
| **Raylet C++ 配置初始化** | | |
| `src/ray/raylet/main.cc` | 330-338 | Raylet 连接 GCS |
| `src/ray/raylet/main.cc` | 498-503 | `AsyncGetInternalConfig` 回调 → `RayConfig::initialize()` |
| `src/ray/raylet/main.cc` | 609-610 | 从 RayConfig 读取 `report_resources_period_ms` 到 NodeManagerConfig |
| **参数运行时使用** | | |
| `src/ray/raylet/node_manager.h` | 113 | `NodeManagerConfig.report_resources_period_ms` 字段定义 |
| `src/ray/raylet/node_manager.h` | 833 | `NodeManager.report_resources_period_ms_` 成员变量 |
| `src/ray/raylet/node_manager.cc` | 204 | 构造函数中赋值 `report_resources_period_ms_` |
| `src/ray/raylet/node_manager.cc` | 345-350 | `ray_syncer_.Register()` 使用 `report_resources_period_ms_` 作为上报周期 |
| `src/ray/raylet/scheduling/cluster_resource_manager.cc` | 31-44 | `ray_syncer_message_refresh_interval_ms` 注册定时刷新任务 |
