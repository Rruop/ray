# Ray GCS Redis 连接验证与数据存储

## 1. GCS Redis 连接验证问题

### 1.1 问题描述

配置了 GCS Redis 后，启动时报错：

```
redis_context.cc:47: Got an error in redis reply: ERR: unsupported command
```

堆栈关键路径：
```
RedisContext::ValidateRedisDB()
  → RedisContext::RunArgvSync({"INFO", "CLUSTER"})
    → CallbackReply::CallbackReply()  → RAY_LOG(FATAL)
```

完整堆栈：
```
ray::operator<<()
ray::RayLog::~RayLog()
ray::gcs::CallbackReply::CallbackReply()         ← 遇到 REDIS_REPLY_ERROR 直接 FATAL
ray::gcs::RedisContext::RunArgvSync()             ← 发送 {"INFO", "CLUSTER"}
ray::gcs::RedisContext::ValidateRedisDB()         ← 验证入口
ray::gcs::RedisContext::ConnectRedisCluster()     ← cluster 连接
ray::gcs::RedisContext::Connect()                 ← 总连接入口
ray::gcs::ConnectRedisContext()                   ← 创建 RedisContext 并连接
ray::gcs::RedisStoreClient::RedisStoreClient()    ← StoreClient 构造
ray::gcs::RedisGetKeySync()                       ← Python 绑定侧调用
```

### 1.2 根因

`ValidateRedisDB()` 执行 `INFO CLUSTER` 命令，部分 Redis（低版本 < 7.0 或兼容实现如 Kvrocks、Predixy 等）不支持该命令，返回 `ERR: unsupported command`。`CallbackReply` 构造函数遇到 `REDIS_REPLY_ERROR` 直接 `RAY_LOG(FATAL)` crash。

关键代码（`redis_context.cc:47`）：
```cpp
CallbackReply::CallbackReply(const redisReply *reply) {
  if (reply->type == REDIS_REPLY_ERROR) {
    RAY_LOG(FATAL) << "Got an error in redis reply: " << reply->str;
  }
}
```

Blacklist 的 `RedisOpsClient` 之前通过 `skip_validation=true` 绕过了此问题，但 GCS 的 `RedisStoreClient` 路径没有传 `skip_validation`，默认 `false`，一定会触发验证。

Blacklist 修复对比（`redis_ops_client.cc:153`）：
```cpp
// blacklist 已修复：传 skip_validation=true
auto status = owned_redis_->Connect(host, port, "", password, false, true);
//                                                                 ^^^^ skip_validation=true

// GCS 未修复：没传第6个参数，默认 false
RAY_CHECK_OK(context->Connect(options.ip,
                               options.port,
                               options.username,
                               options.password,
                               options.enable_ssl))
// 缺少 skip_validation 参数
```

### 1.3 修复方案

在 Ray 中新增 `REDIS_SKIP_CLUSTER_VALIDATION` 配置（默认 `true`），跳过 cluster 验证（`ValidateRedisDB` + `DEL DUMMY`），不影响 sentinel 发现。

### 1.4 改动文件（6 个）

| 文件 | 改动 |
|---|---|
| `src/ray/common/ray_config_def.h` | 新增 `RAY_CONFIG(bool, REDIS_SKIP_CLUSTER_VALIDATION, true)` |
| `src/ray/gcs/gcs_server.h` | `GcsServerConfig` 加 `skip_cluster_validation = true` |
| `src/ray/gcs/gcs_server_main.cc` | 从 `RayConfig` 读值赋给 `GcsServerConfig` |
| `src/ray/gcs/gcs_server.cc` | `GetRedisClientOptions()` 传 `config_.skip_cluster_validation` |
| `src/ray/gcs/store_client/redis_store_client.h` | `RedisClientOptions` 加 `skip_cluster_validation = true` |
| `src/ray/gcs/store_client/redis_store_client.cc` | `ConnectRedisContext` 透传；`RedisDelKeyPrefixSync` 从 `RayConfig` 读值 |

### 1.5 详细代码改动

**ray_config_def.h**：
```cpp
RAY_CONFIG(bool, REDIS_SKIP_CLUSTER_VALIDATION, true)
```
- 环境变量 `RAY_REDIS_SKIP_CLUSTER_VALIDATION=false` 可恢复验证

**gcs_server.h**：
```cpp
struct GcsServerConfig {
  // ...
  bool enable_redis_ssl = false;
  bool skip_cluster_validation = true;
};
```

**gcs_server_main.cc**：
```cpp
gcs_server_config.skip_cluster_validation =
    RayConfig::instance().REDIS_SKIP_CLUSTER_VALIDATION();
```

**gcs_server.cc**：
```cpp
RedisClientOptions GcsServer::GetRedisClientOptions() {
  return RedisClientOptions{config_.redis_address,
                            config_.redis_port,
                            config_.redis_username,
                            config_.redis_password,
                            config_.enable_redis_ssl,
                            config_.skip_cluster_validation};
}
```

**redis_store_client.h**：
```cpp
struct RedisClientOptions {
  std::string ip;
  int port;
  std::string username;
  std::string password;
  bool enable_ssl = false;
  bool skip_cluster_validation = true;
};
```

**redis_store_client.cc**：
```cpp
// ConnectRedisContext 透传
RAY_CHECK_OK(context->Connect(options.ip,
                               options.port,
                               /*username=*/options.username,
                               /*password=*/options.password,
                               /*enable_ssl=*/options.enable_ssl,
                               /*skip_cluster_validation=*/options.skip_cluster_validation))

// RedisDelKeyPrefixSync 读 RayConfig
RedisClientOptions options{host, port, username, password, use_ssl,
                           RayConfig::instance().REDIS_SKIP_CLUSTER_VALIDATION()};
```

**redis_context.h**（默认值保持 false，仅通过显式传参覆盖）：
```cpp
Status Connect(const std::string &address,
               int port,
               const std::string &username,
               const std::string &password,
               bool enable_ssl = false,
               bool skip_cluster_validation = false);

Status ConnectRedisCluster(const std::string &username,
                           const std::string &password,
                           bool enable_ssl,
                           const std::string &redis_address,
                           bool skip_cluster_validation = false);
```

### 1.6 数据流

- **GCS 路径**：环境变量 `RAY_REDIS_SKIP_CLUSTER_VALIDATION` → `RayConfig` → `GcsServerConfig` → `RedisClientOptions` → `Connect()`
- **RedisDelKeyPrefixSync 路径**：环境变量 → `RayConfig` → `RedisClientOptions` → `Connect()`
- **RedisOpsClient（blacklist）**：位置参数传 `true`，不受影响

### 1.7 skip_validation 逻辑说明

- `skip_validation=true` 时跳过 `ValidateRedisDB()` + `DEL DUMMY` leader 发现
- **不影响 `IsRedisSentinel()`**（连接类型发现不属于验证）
- **Sentinel 路径不透传**：重连 primary 时走默认 `false`，会做验证（因为 sentinel 的 primary 必须是标准 Redis）
- **Cluster 路径**：`skip_validation=true` 直接返回 OK，`DEL DUMMY` 的 MOVED 重试有 `RedisRequestContext` 兜底

关键代码（`redis_context.cc:484`）：
```cpp
Status RedisContext::ConnectRedisCluster(
    const std::string &username,
    const std::string &password,
    bool enable_ssl,
    const std::string &redis_address,
    bool skip_cluster_validation) {
  RAY_LOG(INFO) << "Connect to Redis Cluster";
  if (skip_cluster_validation) {
    RAY_LOG(INFO) << "Skip Redis cluster validation for " << redis_address;
    return Status::OK();
  }
  ValidateRedisDB();
  // ... DEL DUMMY logic
}
```

Connect() 中的 Sentinel 逻辑不受影响（`redis_context.cc:674`）：
```cpp
// handle validation and primary connection for different types of redis
if (IsRedisSentinel()) {
  return ConnectRedisSentinel(*this, username, password, enable_ssl);
} else {
  return ConnectRedisCluster(
      username, password, enable_ssl,
      BuildAddress(ip_addresses[0], port), skip_cluster_validation);
}
```

### 1.8 ValidateRedisDB + DEL DUMMY 对应的 Redis 命令

| 步骤 | Redis 命令 | 说明 |
|---|---|---|
| ValidateRedisDB | `INFO CLUSTER` | 获取 cluster 信息，检查是否单 shard |
| DEL DUMMY | `DEL RAY{ns}@DUMMY` | 删临时 key，发现 cluster 真正的 leader |
| 检测 MOVED | 判断 reply type 是否 `REDIS_REPLY_MOVED` | 连的不是 leader 时返回 MOVED + leader 地址 |
| 重连 leader | `Connect(new_ip, new_port, ...)` | 连到 MOVED 返回的 leader 地址 |

ValidateRedisDB 详细代码（`redis_context.cc:414`）：
```cpp
void RedisContext::ValidateRedisDB() {
  auto reply = RunArgvSync(std::vector<std::string>{"INFO", "CLUSTER"});
  // 解析 cluster_state 和 cluster_slots_ok
  // 检查是否 cluster_state:ok 且 cluster_slots_ok == 1（单 shard）
  // 否则 RAY_CHECK 失败
}
```

DEL DUMMY 详细代码（`redis_context.cc:490`）：
```cpp
// 发送 DEL DUMMY 到当前连接的节点
auto redis_reply = reinterpret_cast<redisReply *>(
    ::redisCommandArgv(sync_context(), cmds.size(), argv.data(), argc.data()));

if (redis_reply->type == REDIS_REPLY_ERROR) {
  // 检查是否是 MOVED 重定向
  // 解析 MOVED <slot> <ip>:<port>
  // 重新连接到 leader 节点
}
```

---

## 2. GCS 数据存储结构

### 2.1 Redis Key 格式

```
RAY{external_storage_namespace}@{table_name}
```

示例：`RAY864b004c-6305-42e3-ac46-adfa8eb6f752@NODE`

RedisKey 构造代码（`redis_store_client.cc:69`）：
```cpp
std::string RedisKey::ToString() const {
  // Something like RAY864b004c-6305-42e3-ac46-adfa8eb6f752@NODE
  return absl::StrCat("RAY", external_storage_namespace, kClusterSeparator, table_name);
}
```

其中 `kClusterSeparator = "@"`（`redis_store_client.cc:38`），`external_storage_namespace` 由 `RayConfig::external_storage_namespace()` 配置。

### 2.2 GCS 表结构

6 张表 + 1 个特殊键，每个表在 Redis 中是一个 HASH（JobCounter 除外）：

| 表 | Key 类型 | 数据类型 | Redis Hash Key | Redis 命令 |
|---|---|---|---|---|
| JobTable | JobID | JobTableData | `RAY{ns}@JOB` | HSET/HGET/HDEL |
| ActorTable | ActorID | ActorTableData | `RAY{ns}@ACTOR` | HSET/HGET/HDEL |
| ActorTaskSpecTable | ActorID | TaskSpec | `RAY{ns}@ACTOR_TASK_SPEC` | HSET/HGET/HDEL |
| NodeTable | NodeID | GcsNodeInfo | `RAY{ns}@NODE` | HSET/HGET/HDEL |
| WorkerTable | WorkerID | WorkerTableData | `RAY{ns}@WORKERS` | HSET/HGET/HDEL |
| PlacementGroupTable | PlacementGroupID | PlacementGroupTableData | `RAY{ns}@PLACEMENT_GROUP` | HSET/HGET/HDEL |
| Internal KV | string | bytes | `RAY{ns}@KV` | HSET/HGET/HDEL |
| JobCounter | - | int | `RAY{ns}@JobCounter` | INCRBY/GET（string，非 HASH） |

HASH 内的 field 是各 ID 的十六进制字符串，value 是序列化后的 protobuf 二进制数据。

表名生成代码（`gcs_table_storage.h`）：
```cpp
class GcsJobTable : public GcsTable<JobID, rpc::JobTableData> {
  explicit GcsJobTable(std::shared_ptr<StoreClient> store_client)
      : GcsTable(std::move(store_client)) {
    table_name_ = rpc::TablePrefix_Name(rpc::TablePrefix::JOB);
  }
};

class GcsActorTable : public GcsTableWithJobId<ActorID, rpc::ActorTableData> {
  explicit GcsActorTable(std::shared_ptr<StoreClient> store_client)
      : GcsTableWithJobId(std::move(store_client)) {
    table_name_ = rpc::TablePrefix_Name(rpc::TablePrefix::ACTOR);
  }
};
// ... 其他表类似
```

GcsTableStorage 构造所有表（`gcs_table_storage.h:200`）：
```cpp
class GcsTableStorage {
  explicit GcsTableStorage(std::shared_ptr<StoreClient> store_client)
      : store_client_(std::move(store_client)) {
    job_table_ = std::make_unique<GcsJobTable>(store_client_);
    actor_table_ = std::make_unique<GcsActorTable>(store_client_);
    actor_task_spec_table_ = std::make_unique<GcsActorTaskSpecTable>(store_client_);
    placement_group_table_ = std::make_unique<GcsPlacementGroupTable>(store_client_);
    node_table_ = std::make_unique<GcsNodeTable>(store_client_);
    worker_table_ = std::make_unique<GcsWorkerTable>(store_client_);
  }
};
```

### 2.3 TablePrefix_Name 作用

```protobuf
// gcs.proto:25
enum TablePrefix {
  TABLE_PREFIX_MIN = 0;
  UNUSED = 1;
  TASK = 2;
  RAYLET_TASK = 3;
  NODE = 4;
  OBJECT = 5;
  ACTOR = 6;
  FUNCTION = 7;
  TASK_RECONSTRUCTION = 8;
  RESOURCE_USAGE_BATCH = 9;
  JOB = 10;
  TASK_LEASE = 12;
  NODE_RESOURCE = 13;
  DIRECT_ACTOR = 14;
  WORKERS = 15;
  PLACEMENT_GROUP_SCHEDULE = 16;
  PLACEMENT_GROUP = 17;
  KV = 18;
  ACTOR_TASK_SPEC = 19;
}
```

```cpp
inline const std::string& TablePrefix_Name(TablePrefix value) {
  return ::PROTOBUF_NAMESPACE_ID::internal::NameOfDenseEnum<TablePrefix_descriptor,
                                                 0, 19>(
      static_cast<int>(value));
}
```

这是 protobuf 自动生成的枚举转字符串方法，将 `TablePrefix` 枚举值转为对应的字符串名：
- `TablePrefix::JOB` → `"JOB"`
- `TablePrefix::ACTOR` → `"ACTOR"`
- `TablePrefix::NODE` → `"NODE"`
- 等等

用于构造 Redis hash 的 key 名。

### 2.4 StoreClient 接口

| StoreClient 接口 | Redis 命令 | 说明 |
|---|---|---|
| `AsyncPut` | `HSET` / `HSETNX` | 写入 HASH field |
| `AsyncGet` | `HGET` | 读取单个 HASH field |
| `AsyncMultiGet` | `HMGET` | 批量读 |
| `AsyncGetAll` | `HSCAN` | 遍历整个 HASH |
| `AsyncDelete` | `HDEL` | 删除单个 HASH field |
| `AsyncBatchDelete` | `HDEL` | 删除多个 field |
| `AsyncExists` | `HEXISTS` | 检查 field 是否存在 |
| `AsyncGetKeys` | `HSCAN` + MATCH | 按前缀扫描 field |
| `AsyncGetNextJobID` | `INCRBY` | 自增 JobCounter（string） |

`HSCAN` + MATCH 用于：
- `GcsTable::GetAll` — GCS 启动时从 Redis 恢复所有表数据
- `GcsTableWithJobId::AsyncRebuildIndexAndGetAll` — 重建 Actor jobId 索引
- `StoreClientInternalKV::Keys` — Internal KV 按前缀扫描 key（Serve 用）

如果 Redis 不支持 `SCAN`/`HSCAN`，这些功能会失败，但单条 `Get`/`Put`/`Delete` 不受影响。

RedisStoreClient 中 Put 的实现（`redis_store_client.cc:146`）：
```cpp
void RedisStoreClient::AsyncPut(const std::string &table_name,
                                const std::string &key,
                                std::string data,
                                bool overwrite,
                                Postable<void(bool)> callback) {
  RedisCommand command{/*command=*/overwrite ? "HSET" : "HSETNX",
                       RedisKey{external_storage_namespace_, table_name},
                       /*args=*/{key, std::move(data)}};
  // ...
}
```

AsyncGetNextJobID 的实现（`redis_store_client.cc:457`）：
```cpp
void RedisStoreClient::AsyncGetNextJobID(Postable<void(int)> callback) {
  // Note: This is not a HASH! It's a simple key-value pair.
  // Key: "RAYexternal_storage_namespace@JobCounter"
  // Value: The next job ID.
  RedisCommand command = {
      "INCRBY", RedisKey{external_storage_namespace_, "JobCounter"}, {"1"}};
  // ...
}
```

---

## 3. JobTableData 详细字段

```protobuf
message JobTableData {
  bytes job_id = 1;              // 作业 ID
  bool is_dead = 2;              // 是否已结束
  int64 timestamp = 3;           // 事件时间戳
  string driver_ip_address = 4;  // Driver IP（Deprecated）
  int64 driver_pid = 5;          // Driver 进程 PID
  JobConfig config = 6;          // 作业配置（runtime env、元数据等）
  uint64 start_time = 7;         // 作业开始时间
  uint64 end_time = 8;           // 作业结束时间
  string entrypoint = 9;         // 入口命令
  optional JobsAPIInfo job_info = 10;       // Ray Job API 额外信息
  optional bool is_running_tasks = 11;      // 是否有正在运行的任务
  Address driver_address = 12;              // Driver 地址（IP+端口）
}
```

Redis 中看到乱码是因为存储的是 protobuf 二进制序列化数据，不是 JSON/文本。用 `redis-cli` 直接查看会显示为乱码，需要 protobuf 反序列化才能正确读取。

乱码中能辨认出的内容对应关系：
- `10.177.168.245` → `driver_ip_address` 或 `driver_address`
- `_ray_internal_dashboard` → `JobConfig` 中的 metadata
- `ray-dashboard-ServeHead-0` → `entrypoint`
- `/usr/bin/python3 -c ...` → `entrypoint` 中的 driver 启动命令

### 3.1 _ray_internal_dashboard

`_ray_internal_dashboard` 是 Ray 内部自动启动的 dashboard 服务 job，namespace 叫这个名字。对应 `ray-dashboard-ServeHead-0` 是其 entrypoint 进程名。这些是 Ray 框架自身启动的内部作业，每个 Ray 集群都会有。

代码位置（`python/ray/dashboard/modules/job/job_head.py:698`）：
```python
# Drivers in namespaces that start with _ray_internal_ are not
# considered activity.
# This includes the _ray_internal_dashboard job that gets automatically
# created with every cluster
```

内部作业会记录在 GcsJobTable 中（dashboard agent 本质上也是一个 driver 进程），但 `_ray_internal_` 前缀的 namespace 在判断集群是否有活跃业务时会被排除。

Job 存储信息**不是用来重新执行的**，而是用于：
1. **追踪作业生命周期** — 记录 `is_dead`、`start_time`、`end_time`、`driver_pid` 等，供 dashboard 和 API 查询
2. **资源清理** — GCS 检测到 driver 退出后，标记 `is_dead=true`，清理关联资源
3. **GCS 恢复时重建状态** — 从 Redis 读取所有 job 信息恢复内存管理状态

Job 不会重新运行。Driver 进程由外部（用户或 autoscaler）启动，Ray 不负责重新运行。

---

## 4. ActorTableData 详细字段

```protobuf
message ActorTableData {
  bytes actor_id = 1;                                    // Actor ID
  bytes parent_id = 2;                                   // 创建者 ID
  bytes job_id = 4;                                      // 所属 Job ID
  ActorState state = 6;                                  // DEPENDENCIES_UNREADY/PENDING_CREATION/ALIVE/RESTARTING/DEAD
  int64 max_restarts = 7;                                // 最大重启次数（-1=无限）
  int64 num_restarts = 8;                                // 已重启次数
  Address address = 9;                                   // Actor 所在地址（IP+端口）
  Address owner_address = 10;                            // Owner 地址
  bool is_detached = 11;                                 // 是否 detached
  string name = 12;                                      // Actor 名称
  double timestamp = 13;                                 // 最后更新时间戳
  repeated ResourceMapEntry resource_mapping = 15;       // 资源映射
  uint32 pid = 16;                                       // 进程 PID
  FunctionDescriptor function_descriptor = 17;           // 创建函数描述
  string ray_namespace = 19;                             // 命名空间
  uint64 start_time = 20;                                // 开始时间
  uint64 end_time = 21;                                  // 结束时间
  string serialized_runtime_env = 22;                    // runtime env
  string class_name = 23;                                // 类名
  ActorDeathCause death_cause = 24;                      // 死亡原因
  map<string, double> required_resources = 28;           // 所需资源（CPU/GPU 等）
  optional bytes node_id = 29;                           // 所在节点 ID
  optional bytes placement_group_id = 30;                // 所属 placement group
  string repr_name = 31;                                 // 自定义 repr 名称
  bool preempted = 32;                                   // 是否被抢占
  uint64 num_restarts_due_to_lineage_reconstruction = 33; // lineage 重构重启次数
}
```

**所有 Actor 创建时都会写入 `ACTOR_TASK_SPEC`**，包括框架内部的（Serve Controller/Proxy 等）。写入时机是 `RegisterActor` 时：

```cpp
// gcs_actor_manager.cc:749
gcs_table_storage_->ActorTaskSpecTable().Put(
    actor_id,
    request.task_spec(),
    {[this, actor](Status status) {
       gcs_table_storage_->ActorTable().Put(
           actor->GetActorID(),
           actor->GetActorTableData(),
           // ...
       );
    }});
```

Actor 被销毁时 task spec 也会被删除：

```cpp
// gcs_actor_manager.cc:1142
gcs_table_storage_->ActorTaskSpecTable().Delete(actor_id, /*callback*/);
```

所以已死亡 Actor 的 task spec 不会残留。

---

## 5. Ray Serve Deployment 存储

### 5.1 存储位置

Serve deployment 信息**不存储在 GCS 的 6 张标准表中**，而是通过 **GCS Internal KV** 存储。

`StoreClientInternalKV` 用 `TablePrefix::KV`（即 `"KV"`）作为表名（`store_client_kv.cc:54`）：
```cpp
StoreClientInternalKV::StoreClientInternalKV(std::unique_ptr<StoreClient> store_client)
    : delegate_(std::move(store_client)),
      table_name_(TablePrefix_Name(rpc::TablePrefix::KV)) {}
```

Serve Controller 使用 `RayInternalKVStore`（封装了 `GcsClient.internal_kv_put/get/del`），namespace 为 `serve`（`SERVE_INTERNAL_KV_NAMESPACE = b"serve"`），存储在 `RAY{ns}@KV` HASH 中。

`RayInternalKVStore` 代码（`kv_store.py:44`）：
```python
class RayInternalKVStore(KVStoreBase):
    def get_storage_key(self, key: str) -> str:
        return "{ns}-{key}".format(ns=self.namespace, key=key)

    def put(self, key: str, val: bytes) -> bool:
        return self.gcs_client.internal_kv_put(
            self.get_storage_key(key).encode(),
            val,
            overwrite=True,
            namespace=SERVE_INTERNAL_KV_NAMESPACE,  # b"serve"
            timeout=self.timeout,
        )
```

### 5.2 Checkpoint Keys

| KV Key | 内容 |
|---|---|
| `serve-app-config-checkpoint` | deployment 配置（`DeploymentRouteList` protobuf） |
| `serve-logging-config-checkpoint` | logging 配置 |
| `serve-deployment-state-checkpoint` | deployment 运行时状态（replica 信息等） |
| `serve-application-state-checkpoint` | application 状态 |
| `serve-endpoint-state-checkpoint` | endpoint 路由映射 |

实际的 KV key 格式：`{ray_serve_namespace}-{key}`，如 `ray-serve-{ray_namespace}-serve-deployment-state-checkpoint`，值是 cloudpickle 序列化的二进制。

定义位置：
```python
# controller.py:123
CONFIG_CHECKPOINT_KEY = "serve-app-config-checkpoint"
LOGGING_CONFIG_CHECKPOINT_KEY = "serve-logging-config-checkpoint"

# deployment_state.py:616
CHECKPOINT_KEY = "serve-deployment-state-checkpoint"

# application_state.py:81
CHECKPOINT_KEY = "serve-application-state-checkpoint"

# endpoint_state.py:10
CHECKPOINT_KEY = "serve-endpoint-state-checkpoint"
```

### 5.3 代码位置

| 文件 | 作用 |
|---|---|
| `python/ray/serve/_private/storage/kv_store.py` | KV 存储封装，底层调 `GcsClient.internal_kv_*` |
| `python/ray/serve/_private/controller.py` | Serve Controller 主逻辑，管理 config 和 logging checkpoint |
| `python/ray/serve/_private/deployment_state.py` | Deployment 状态管理，checkpoint key = `serve-deployment-state-checkpoint` |
| `python/ray/serve/_private/application_state.py` | Application 状态管理，checkpoint key = `serve-application-state-checkpoint` |
| `python/ray/serve/_private/endpoint_state.py` | Endpoint 路由管理，checkpoint key = `serve-endpoint-state-checkpoint` |

### 5.4 Serve Controller 重启

Serve Controller 是 **detached actor**，配置了 `max_restarts=-1`（无限重启）。当 head 节点重启后，Ray 的 GCS 会自动重建 detached actor。Controller 重启后从 KV 存储中读取 checkpoint 恢复 deployment 状态和配置。

恢复代码（`deployment_state.py:5279`）：
```python
checkpoint = self._kv_store.get(CHECKPOINT_KEY)
if checkpoint is not None:
    deployment_state_info = cloudpickle.loads(checkpoint)
    for deployment_id, checkpoint_data in deployment_state_info.items():
        deployment_state = self._create_deployment_state(deployment_id)
        deployment_state.recover_target_state_from_checkpoint(checkpoint_data)
        if len(deployment_to_current_replicas[deployment_id]) > 0:
            deployment_state.recover_current_state_from_replica_actor_names(
                deployment_to_current_replicas[deployment_id]
            )
```

Serve deployment 的 replica 本质上是 Ray Actor，所以每个 replica 的信息也会存一份到 GcsActorTable。

---

## 6. Task 信息存储

### 6.1 Task 不持久化到 Redis

Ray 的 task 事件只存在 GCS 内存中（`GcsTaskManager`），**不持久化到 Redis**。GCS 重启后 task 事件全部丢失。

`GcsTaskManager` 没有 `Initialize` 方法（不像其他 manager），不会从 Redis 恢复数据。

### 6.2 有数量上限

由 `RAY_task_events_max_num_task_in_gcs` 控制（默认 10 万）。超过上限时，最早的事件被淘汰（FIFO 驱逐）。

EvictTaskEvent 代码（`gcs_task_manager.cc:332`）：
```cpp
void GcsTaskManagerStorage::EvictTaskEvent() {
  // 根据 FinishedTaskActorTaskGcPolicy 优先级驱逐
  // 最低优先级中最老的 task event 被删除
}
```

### 6.3 Task spec 不存储

GCS 只存储 **task 事件**（状态变化：PENDING → RUNNING → FINISHED/FAILED 等），不存储完整的 task spec。Task spec 只在 worker 本地保留（core_worker 的 `task_manager.cc`），用于执行和重试。

### 6.4 Job 结束后清理

`OnJobFinished` 会标记该 job 下所有未结束 task 为 FAILED：

```cpp
// gcs_task_manager.cc:750
void GcsTaskManager::OnJobFinished(const JobID &job_id, int64_t job_finish_time_ms) {
  // 在延迟 gcs_mark_task_failed_on_job_done_delay_ms 后
  // 标记该 job 所有未结束 task 为 FAILED
}
```

### 6.5 相关配置

```
RAY_task_events_report_interval_ms=1000     # 上报间隔，0 则禁用
RAY_task_events_max_num_task_in_gcs=100000   # GCS 内存上限
RAY_task_events_max_num_profile_events_per_task=1000
RAY_task_events_max_dropped_task_attempts_tracked_per_job_in_gcs=1000
```

---

## 7. GCS 信息清理机制

### 7.1 清理触发条件

| 触发条件 | 清理内容 | 代码位置 |
|---|---|---|
| Job 结束/Driver 退出 | Job 标记 `is_dead`，清理 runtime_env、函数引用；级联：标记 Job 下未结束 task 为 FAILED，清理 Job 拥有的 placement group | `gcs_job_manager.cc:MarkJobAsFinished()` |
| Actor 永久死亡 | 状态设 DEAD，移入 destroyed_actors_ 缓存（有大小上限），删除 ActorTaskSpecTable 中的 task spec，清理名称注册、owner 引用、placement group | `gcs_actor_manager.cc:DestroyActor()` |
| Worker 死亡 | Actor 在该 Worker 上 → 尝试重启；Actor owner 是该 Worker → destroy（OWNER_DIED）；Task 标记 FAILED | `gcs_actor_manager.cc:OnWorkerDead()` |
| 节点死亡 | 该节点上所有 Actor → 重启；owner 在该节点的子 Actor → destroy；调度中的 Actor → 取消并重启 | `gcs_actor_manager.cc:OnNodeDead()` |
| Task event 超限 | GCS 内存中超过 10 万条时 FIFO 驱逐最早的 | `gcs_task_manager.cc:EvictTaskEvent()` |
| Destroyed actor 缓存 GC | 超过 `maximum_gcs_destroyed_actor_cached_count` 时，从内存和 Redis 中删除最老的 | `gcs_actor_manager.cc:AddDestroyedActorToCache()` |

Actor Destroy 详细代码（`gcs_actor_manager.cc`）：
```cpp
void GcsActorManager::DestroyActor(const ActorID &actor_id, ...) {
  // 1. 设置状态为 DEAD
  actor->GetMutableActorTableData()->set_state(rpc::ActorTableData::DEAD);
  actor->GetMutableActorTableData()->set_end_time(current_time_ms);
  actor->GetMutableActorTableData()->set_death_cause(death_cause);

  // 2. 移入 destroyed_actors_ 缓存
  AddDestroyedActorToCache(actor);

  // 3. 清理函数引用
  function_manager_.RemoveJobReference(actor_id.JobId());

  // 4. 清理名称注册
  RemoveActorNameFromRegistry(actor);

  // 5. 非 detached: 清理 owner 引用
  if (!actor->IsDetached()) {
    RemoveActorFromOwner(actor);
  }

  // 6. detached: 清理 runtime_env 引用
  if (actor->IsDetached()) {
    runtime_env_manager_.RemoveURIReference(actor_id.Hex());
  }

  // 7. 删除 task spec
  gcs_table_storage_->ActorTaskSpecTable().Delete(actor_id, /*callback*/);

  // 8. 清理 placement group
  destroy_owned_placement_group_if_needed_(actor_id);
}
```

Job MarkJobAsFinished 详细代码（`gcs_job_manager.cc`）：
```cpp
void GcsJobManager::MarkJobAsFinished(JobTableData &job_data) {
  job_data.set_is_dead(true);
  job_data.set_end_time(current_time_ms);
  job_data.set_timestamp(current_time_ms);

  // 持久化到 Redis
  gcs_table_storage_->JobTable().Put(job_id, job_data, ...);

  // 清理 runtime_env
  runtime_env_manager_.RemoveURIReference(job_id.Hex());

  // 级联通知
  ClearJobInfos(job_data);  // → gcs_task_manager_->OnJobFinished()
                             // → gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenJobDead()

  // 清理函数引用
  function_manager_.RemoveJobReference(job_id);
}
```

### 7.2 Redis 数据清理问题

`ray.shutdown()` 的清理链路（`worker.py:2173`）：
```python
def shutdown(_exiting_interpreter=False):
    # ...
    disconnect(_exiting_interpreter)
    # disconnect internal kv
    if hasattr(global_worker, "gcs_client"):
        del global_worker.gcs_client
    _internal_kv_reset()

    # 关闭 core worker
    if hasattr(global_worker, "core_worker"):
        if global_worker.mode == SCRIPT_MODE:
            global_worker.core_worker.shutdown_driver()
        del global_worker.core_worker

    # Shut down the Ray processes.
    global _global_node
    if _global_node is not None:
        if _global_node.is_head():
            _global_node.destroy_external_storage()  # ← 只清理 object spilling
        _global_node.kill_all_processes(check_alive=False, allow_graceful=True)
        _global_node = None
```

`destroy_external_storage`（`node.py:1767`）只清理 object spilling 的外部存储：
```python
def destroy_external_storage(self):
    object_spilling_config = self._config.get("object_spilling_config", {})
    if object_spilling_config:
        storage = external_storage.setup_external_storage(...)
        storage.destroy_external_storage()  # ← 只删文件，不删 Redis
```

**`ray.shutdown()` 根本不会清理 Redis 中的 GCS 表数据。** `cleanup_redis_storage()` 在整个 Ray 代码中只在测试中被调用，生产代码没有任何地方自动调用它。

| 场景 | GCS 表数据（Redis） | 是否清理 |
|---|---|---|
| `ray.init()` 本地集群 + `ray.shutdown()` | `RAY{ns}@JOB/ACTOR/NODE/...` | **不清理** |
| `ray.init(address="auto")` + `ray.shutdown()` | 同上 | **不清理** |
| `ray stop` | 同上 | **不清理** |
| Kill 进程 / 重启机器 | 同上 | **不清理** |
| Autoscaler `teardown_cluster` | 同上 | **不清理**（只执行 `ray stop` + 销毁节点） |

### 7.3 清理 GCS 数据的方法

**方法 1：Python 调用 cleanup_redis_storage**

```python
from ray._private.gcs_utils import cleanup_redis_storage

cleanup_redis_storage(
    host="10.x.x.x",
    port=6379,
    username="",
    password="your_password",
    use_ssl=False,
    storage_namespace="864b004c-..."  # 你的 external_storage_namespace
)
```

`cleanup_redis_storage` 底层调用 `RedisDelKeyPrefixSync`（`gcs_utils.py:126`）：
```python
from ray._raylet import del_key_prefix_from_storage
return del_key_prefix_from_storage(
    host, port, username, password, use_ssl, storage_namespace
)
```

**方法 2：redis-cli 直接删除（按已知表名，不需要 SCAN）**

```bash
NS="864b004c"  # 替换为你的 storage namespace

redis-cli -h 10.x.x.x -p 6379 -a password DEL \
  "RAY${NS}@JOB" \
  "RAY${NS}@ACTOR" \
  "RAY${NS}@ACTOR_TASK_SPEC" \
  "RAY${NS}@NODE" \
  "RAY${NS}@WORKERS" \
  "RAY${NS}@PLACEMENT_GROUP" \
  "RAY${NS}@KV" \
  "RAY${NS}@JobCounter"
```

共 8 个 key，一次 DEL 全部删除。`DEL` 对 HASH 和 string 都生效，直接删除整个 key。

**方法 3：redis-cli 用 SCAN（如果支持）**

```bash
redis-cli -h 10.x.x.x -p 6379 -a password --scan --pattern "RAY${NS}@*" | xargs -L 100 redis-cli -h 10.x.x.x -p 6379 -a password DEL
```

**方法 4：FLUSHDB（如果 Redis 只给 Ray 用）**

```bash
redis-cli -h 10.x.x.x -p 6379 -a password FLUSHDB
```

### 7.4 RedisDelKeyPrefixSync 的 SCAN 依赖问题

`RedisDelKeyPrefixSync`（`redis_store_client.cc:520`）目前依赖 `SCAN` 命令遍历 key：

```cpp
bool RedisDelKeyPrefixSync(...) {
  RedisKey redis_key{external_storage_namespace, /*table_name=*/""};
  std::string match_pattern = RedisMatchPattern::Prefix(redis_key.ToString()).escaped_;

  do {
    std::vector<std::string> cmd{"SCAN", std::to_string(cursor), "MATCH", match_pattern};
    // ... SCAN 遍历
  } while (cursor != 0);

  for (const auto &key : keys) {
    // 逐个 DEL
  }
}
```

如果 Redis 不支持 `SCAN`，需要改成按已知表名逐个 `DEL`（方法 2）。

### 7.5 HASH 懒创建

Redis 的 HASH 是**懒创建**的——只有在第一次 `HSET` 写入数据时才会创建 key。所以可能只看到部分 key：

- `ACTOR_TASK_SPEC` — 只在 Actor 创建时写入，如果没有创建过 Actor 或 Actor 已被清理，此 key 不存在
- `PLACEMENT_GROUP` — 只在使用 Placement Group 时才写入，没用过就不存在

看到的 key 对应集群实际使用过的功能，属于正常现象。

---

## 8. GCS HA 与 Task 恢复

### 8.1 GCS HA 能保证的

- GCS 重启期间，**已在 worker 上运行的 task 继续执行**，不受影响
- GCS 恢复后，actor 可以从 Redis 恢复状态并重新调度
- 新的 task 提交、actor 创建等操作在 GCS 恢复后可以继续

### 8.2 GCS HA 不能保证的

- **Driver 死了** → 它拥有的所有 task 丢失，无人负责重试和接收结果
- **GCS 重启期间** → 新 task 无法提交，actor 无法调度，资源无法分配
- **Object 位置信息丢失** → GCS 不存 object location，`ray.get()` 可能找不到对象

### 8.3 Task 恢复机制

**Task 重试**（core_worker 本地 `task_manager.cc`）：
- Worker 死亡后，owner 调用 `RetryTaskIfPossible()`
- 根据 `max_retries` 决定是否重试，指数退避延迟
- 节点抢占重启不消耗 `max_retries` 配额
- OOM 重试单独追踪（`num_oom_retries_left_`）

```cpp
// task_manager.cc
bool TaskManager::RetryTaskIfPossible(const TaskID &task_id) {
  auto it = submissible_tasks_.find(task_id);
  if (it->second.num_retries_left_ > 0 || it->second.num_retries_left_ == -1) {
    it->second.num_retries_left_--;
    // 调用 async_retry_task_callback_ 重新提交
    return true;
  }
  return false;  // 永久失败
}
```

**Lineage 重构**（`object_recovery_manager.cc`）：
- Plasma 对象丢失时，先尝试 pin 其他副本（`PinExistingObjectCopy`）
- 没有副本则沿 lineage 重构，调用 `ResubmitTask()` 重新执行产出该对象的 task
- Task spec 在 owner 内存中保留为 lineage（受 `max_lineage_bytes_` 上限）
- Lineage 超限时驱逐至少一半

```cpp
// object_recovery_manager.cc
Status ObjectRecoveryManager::RecoverObject(const ObjectID &object_id) {
  // 1. 尝试 pin 已有副本
  auto pinned = PinExistingObjectCopy(object_id);
  if (pinned.ok()) return pinned;

  // 2. Lineage 重构
  return ReconstructObject(object_id);
  // → task_manager_.ResubmitTask(task_id, /*is_reconstruction=*/true)
}
```

**GCS 重启期间**：已在 worker 上运行的 task 不受影响，raylet 和 core worker 独立运行。

### 8.4 Actor 重启机制

**Worker/节点死亡 → RestartActor()**（`gcs_actor_manager.cc:1461`）：
- 检查 `max_restarts` 和 `num_restarts`
- `remaining_restarts = max_restarts - (num_restarts - num_restarts_due_to_node_preemption)`
- 节点抢占重启不消耗配额
- Creation task failure 重试受 `max_creation_task_failure_restarts` 限制
- 有剩余次数则状态转 RESTARTING 并重新调度
- 无剩余次数则状态转 DEAD

```cpp
// gcs_actor_manager.cc:1461
void GcsActorManager::RestartActor(const std::shared_ptr<GcsActor> &actor, ...) {
  int64_t remaining_restarts;
  if (actor->GetMaxRestarts() == -1) {
    remaining_restarts = 1;  // 无限重启
  } else {
    remaining_restarts = actor->GetMaxRestarts()
        - (actor->GetNumRestarts() - actor->GetNumRestartsDueToNodePreemption());
  }

  if (remaining_restarts != 0) {
    actor->GetMutableActorTableData()->set_state(rpc::ActorTableData::RESTARTING);
    actor->GetMutableActorTableData()->set_num_restarts(num_restarts + 1);
    gcs_table_storage_->ActorTable().Put(actor_id, actor_data, ...);
    gcs_actor_scheduler_->Schedule(actor);
  } else {
    // 永久死亡
    DestroyActor(actor_id, ...);
  }
}
```

**GCS 重启恢复**：从 Redis 读取 ActorTable（`gcs_actor_manager.cc:OnInitializeActorShouldLoad`）
- ALIVE 状态的 actor → 加入 `created_actors_`
- PENDING_CREATION/RESTARTING 状态 → **立即重新调度**（`Reschedule`）
- DEAD 且不可重启 → 移入 `destroyed_actors_` 缓存
- 非 detached actor 的 root owner 已死 → 不加载

**Lineage 重构触发重启**（`gcs_actor_manager.cc:HandleRestartActorForLineageReconstruction`）：
- 由 core worker 触发，用于重构依赖 actor 的对象
- 单独计数 `num_restarts_due_to_lineage_reconstruction`
- 不受 `max_restarts` 直接限制（由 `IsActorRestartable` 判断）

### 8.5 核心原则

**Task 的生命周期绑定到 owner（通常是 driver）。Owner 死了，task 就无法恢复。**

| 场景 | Task 能否继续 |
|---|---|
| GCS 重启，Driver 在 head 且仍活着 | 已运行的 task 继续，新 task 等 GCS 恢复后可提交 |
| Head 节点重启，Driver 在 head | Driver 死了 → task 全部丢失 |
| GCS 重启，Driver 在 worker 节点 | 已运行的 task 继续，Driver 存活 |
| Worker 节点死，Driver 在 worker | Driver 死了 → task 全部丢失 |

GCS HA 只保证元数据和服务恢复，不保证 task 级别的容错。要保证 task 完整运行：
1. Driver 不能在 head 上（或 head 本身要高可用）
2. 关键逻辑用 Actor（有 `max_restarts` 重启机制）
3. Task 设 `max_retries > 0`（owner 存活时可以重试）

### 8.6 GCS 重启恢复总结

| 数据 | 是否从 Redis 恢复 | 代码位置 |
|---|---|---|
| Job 配置/状态 | 是 | `gcs_job_manager_->Initialize(gcs_init_data)` |
| Actor 状态/重启计数/死亡原因 | 是 | `gcs_actor_manager_->Initialize(gcs_init_data)` |
| Actor 创建 task spec | 是 | `GcsInitData` 加载 |
| Node 信息 | 是 | `gcs_node_manager_->Initialize(gcs_init_data)` |
| Placement group | 是 | `gcs_placement_group_manager_->Initialize(gcs_init_data)` |
| Internal KV（Serve 配置等） | 是 | Redis KV 持久化 |
| Cluster ID | 是 | `GetOrGenerateClusterId` |
| Task 事件 | **否** | `GcsTaskManager` 无 Initialize 方法 |
| 运行中 task 的执行状态 | **否** | 由 core worker 本地管理 |
| Object 位置信息 | **否** | 由各 core worker 本地管理 |

GCS 启动时 `GcsInitData::AsyncLoad()` 从 Redis 加载 5 张表恢复状态（`gcs_init_data.cc`）：
```cpp
void GcsInitData::AsyncLoad(Postable<void()> on_load_finished) {
  AsyncLoadJobTableData(on_load_finished);
  AsyncLoadNodeTableData(on_load_finished);
  AsyncLoadActorTableData(on_load_finished);
  AsyncLoadActorTaskSpecTableData(on_load_finished);
  AsyncLoadPlacementGroupTableData(on_load_finished);
}

// 每个表使用 GetAll 或 AsyncRebuildIndexAndGetAll
void GcsInitData::AsyncLoadJobTableData(Postable<void()> on_done) {
  gcs_table_storage_.JobTable().GetAll(std::move(on_done).TransformArg(
      [this](absl::flat_hash_map<JobID, rpc::JobTableData> result) {
        job_table_data_ = std::move(result);
      }));
}

void GcsInitData::AsyncLoadActorTableData(Postable<void()> on_done) {
  gcs_table_storage_.ActorTable().AsyncRebuildIndexAndGetAll(
      std::move(on_done).TransformArg(
          [this](absl::flat_hash_map<ActorID, rpc::ActorTableData> result) {
            actor_table_data_ = std::move(result);
          }));
}
```

---

## 9. 关键代码路径索引

### 9.1 Redis 连接

- `src/ray/gcs/store_client/redis_context.cc` — RedisContext::Connect(), ConnectRedisCluster(), ValidateRedisDB(), IsRedisSentinel(), ConnectRedisSentinel()
- `src/ray/gcs/store_client/redis_context.h` — Connect() 和 ConnectRedisCluster() 声明，skip_cluster_validation 参数
- `src/ray/gcs/store_client/redis_store_client.cc` — ConnectRedisContext(), RedisDelKeyPrefixSync()
- `src/ray/gcs/store_client/redis_store_client.h` — RedisClientOptions, RedisKey::ToString()
- `src/ray/gcs/gcs_server.cc` — GetRedisClientOptions()
- `src/ray/gcs/gcs_server_main.cc` — GcsServerConfig 初始化
- `src/ray/common/ray_config_def.h` — REDIS_SKIP_CLUSTER_VALIDATION 配置

### 9.2 GCS 表存储

- `src/ray/gcs/gcs_table_storage.h` — GcsTable, GcsTableWithJobId, 6 张表定义, GcsTableStorage
- `src/ray/gcs/gcs_table_storage.cc` — GcsTable/GcsTableWithJobId 的 Put/Get/Delete 实现
- `src/ray/gcs/store_client/store_client.h` — StoreClient 抽象接口
- `src/ray/gcs/store_client/redis_store_client.cc` — RedisStoreClient 实现
- `src/ray/gcs/store_client_kv.cc` — StoreClientInternalKV（Internal KV 到 StoreClient 的适配）
- `src/ray/protobuf/gcs.proto` — TablePrefix 枚举, JobTableData, ActorTableData 等消息定义

### 9.3 GCS 恢复

- `src/ray/gcs/gcs_init_data.cc` — GcsInitData::AsyncLoad() 从 Redis 加载表数据
- `src/ray/gcs/gcs_server.cc` — DoStart() 中各 manager 的 Initialize

### 9.4 清理

- `python/ray/_private/gcs_utils.py` — cleanup_redis_storage()
- `python/ray/_private/worker.py` — ray.shutdown() 实现（不清理 Redis）
- `python/ray/_private/node.py` — destroy_external_storage() 只清理 object spilling
- `src/ray/gcs/gcs_job_manager.cc` — MarkJobAsFinished(), OnNodeDead()
- `src/ray/gcs/actor/gcs_actor_manager.cc` — DestroyActor(), RestartActor(), OnWorkerDead(), OnNodeDead()

### 9.5 HA / Task 恢复

- `src/ray/core_worker/task_manager.cc` — RetryTaskIfPossible(), FailOrRetryPendingTask()
- `src/ray/core_worker/object_recovery_manager.cc` — RecoverObject(), ReconstructObject()
- `src/ray/gcs/gcs_task_manager.cc` — OnWorkerDead(), OnJobFinished(), EvictTaskEvent()

### 9.6 Ray Serve 存储

- `python/ray/serve/_private/storage/kv_store.py` — RayInternalKVStore 封装
- `python/ray/serve/_private/controller.py` — Serve Controller，CONFIG_CHECKPOINT_KEY, LOGGING_CONFIG_CHECKPOINT_KEY
- `python/ray/serve/_private/deployment_state.py` — DeploymentStateManager，CHECKPOINT_KEY
- `python/ray/serve/_private/application_state.py` — ApplicationStateManager，CHECKPOINT_KEY
- `python/ray/serve/_private/endpoint_state.py` — EndpointState，CHECKPOINT_KEY
