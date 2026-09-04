# RAY_external_storage_namespace 与 GCS Redis 配置

## 1. 概念

`external_storage_namespace` 是 Ray GCS 用于在 Redis 中**隔离不同集群数据**的命名空间。所有 Ray 集群元数据在 Redis 中以 `RAY{namespace}@{table}` 格式存储（Ray 2.38.0+ 使用多个 HASH 表），例如：

```
RAY864b004c-6305-42e3-ac46-adfa8eb6f752@NODE
RAY864b004c-6305-42e3-ac46-adfa8eb6f752@KV
RAY864b004c-6305-42e3-ac46-adfa8eb6f752@JobCounter
RAY864b004c-6305-42e3-ac46-adfa8eb6f752@INTERNAL_CONFIG
```

默认值为 `"default"`。在 KubeRay 环境下，默认使用 RayCluster 的 UID 作为 namespace，确保每个集群数据天然隔离。

## 2. 使用方式

| 方式 | 示例 |
|---|---|
| **环境变量** | `RAY_external_storage_namespace=my-ns` |
| **Python system_config** | `ray.init(_system_config={"external_storage_namespace": "c1"})` |
| **KubeRay CRD（1.3.0+）** | `gcsFaultToleranceOptions.externalStorageNamespace: "my-ns"` |
| **KubeRay annotation（旧）** | `ray.io/external-storage-namespace: "my-ns"` |

## 3. RAY_external_storage_namespace 与 ray.io/external-storage-namespace 的关系

**它们是同一概念在不同层级的表达**：

| 层级 | 名称 | 说明 |
|---|---|---|
| **Kubernetes annotation（旧）** | `ray.io/external-storage-namespace` | KubeRay 1.3.0 前的声明式配置 |
| **Kubernetes CRD 字段（新）** | `gcsFaultToleranceOptions.externalStorageNamespace` | KubeRay 1.3.0+ 推荐方式 |
| **环境变量** | `RAY_external_storage_namespace` | Ray 运行时实际读取的配置 |

**注入流程**：
1. KubeRay Operator 读取 CRD 中的 `externalStorageNamespace` 值（或 annotation `ray.io/external-storage-namespace`）
2. 自动注入 `RAY_external_storage_namespace` 环境变量到所有 Ray Pod
3. Ray 进程启动时通过 `RayConfig` 读取该环境变量，设置 `external_storage_namespace`

**默认行为**：如果不设置 `externalStorageNamespace`，KubeRay 自动使用 **RayCluster 的 UID** 作为 namespace。

**注意**：`ray.io/external-storage-namespace` 是 Kubernetes annotation（不是 label），用于声明式配置，KubeRay Operator 读取后注入环境变量。

**重要警告（RayService 零停机升级）**：在 RayService 零停机升级场景下，**不要手动设置 `externalStorageNamespace`**。否则新旧 RayCluster 共享同一 Redis namespace，KubeRay 可能误判新集群已就绪（因为读到了旧集群的元数据），导致流量切换到未初始化完成的新集群，引发停机。建议移除该 annotation，让 KubeRay 为每个 RayCluster 自动生成唯一 UID 作为 namespace。

## 4. GCS Redis 配置

### KubeRay CRD 配置

```yaml
spec:
  gcsFaultToleranceOptions:
    redisAddress: "redis:6379"          # Redis 服务地址
    redisPassword:                      # Redis 密码（从 Secret 引用）
      valueFrom:
        secretKeyRef:
          name: redis-password-secret
          key: password
    externalStorageNamespace: "my-ns"   # 可选，默认用 RayCluster UID
```

### Ray 内部配置项（ray_config_def.h）

- `gcs_storage`：存储后端类型，`"redis"` 或 `"memory"`（默认），参见 `src/ray/common/ray_config_def.h:409`
- `gcs_redis_heartbeat_interval_milliseconds`：GCS 与 Redis 心跳间隔（默认 100ms），参见 `src/ray/common/ray_config_def.h:395`
- `maximum_gcs_storage_operation_batch_size`：GCS 存储操作最大批量大小（默认 1000），参见 `src/ray/common/ray_config_def.h:374`
- Redis TLS 配置：`REDIS_CA_CERT`、`REDIS_CA_PATH`、`REDIS_CLIENT_CERT`、`REDIS_CLIENT_KEY`、`REDIS_SERVER_NAME`，参见 `src/ray/common/ray_config_def.h:876-883`

### Redis 清理配置

- `ENABLE_GCS_FT_REDIS_CLEANUP`：KubeRay feature gate，默认 true。RayCluster 删除时 KubeRay 创建 Job 清理 Redis 中的 GCS 数据
- 如禁用清理但希望 Redis 自动淘汰，可在 `redis.conf` 中设置：
  - `maxmemory=<your_memory_limit>`
  - `maxmemory-policy=allkeys-lru`

## 5. 相关代码逻辑详解

### 5.1 C++ Core：配置定义

`src/ray/common/ray_config_def.h:864-866`：
```cpp
/// The namespace for the storage.
/// This fields is used to isolate data stored in DB.
RAY_CONFIG(std::string, external_storage_namespace, "default")
```
- `RAY_CONFIG` 宏定义配置项，可通过环境变量 `RAY_external_storage_namespace` 覆盖
- 默认值 `"default"`，KubeRay 环境下会被覆盖为 RayCluster UID

### 5.2 C++ Core：RedisKey 结构体

`src/ray/gcs/store_client/redis_store_client.h:35-40`：
```cpp
struct RedisKey {
  const std::string external_storage_namespace;
  const std::string table_name;
  std::string ToString() const;
};
```
- 强类型封装，防止遗漏 namespace 前缀
- `RedisCommand` 结构体使用 `RedisKey` 作为 key，参见 `.h:54-70`

`src/ray/gcs/store_client/redis_store_client.cc:69-71`：
```cpp
std::string RedisKey::ToString() const {
  return absl::StrCat("RAY", external_storage_namespace, kClusterSeparator, table_name);
}
```
- 最终 Redis key 格式：`RAY{namespace}@{table_name}`
- `kClusterSeparator` 为 `"@"`

### 5.3 C++ Core：RedisStoreClient 构造与校验

`src/ray/gcs/store_client/redis_store_client.cc:134-143`：
```cpp
RedisStoreClient::RedisStoreClient(instrumented_io_context &io_service,
                                   const RedisClientOptions &options)
    : io_service_(io_service),
      options_(options),
      external_storage_namespace_(::RayConfig::instance().external_storage_namespace()),
      primary_context_(ConnectRedisContext(io_service, options)) {
  RAY_CHECK(!absl::StrContains(external_storage_namespace_, kClusterSeparator))
      << "Storage namespace (" << external_storage_namespace_ << ") shouldn't contain "
      << kClusterSeparator << ".";
}
```
- 从 `RayConfig` 单例读取 `external_storage_namespace`，存为成员变量
- **校验**：namespace 中不能包含 `@`（`kClusterSeparator`），否则会破坏 key 格式

### 5.4 C++ Core：所有 Redis 操作都使用 namespace

`RedisStoreClient` 的所有 CRUD 操作都通过 `RedisKey{external_storage_namespace_, table_name}` 构造带 namespace 的 key：

| 操作 | 代码位置 | Redis 命令 |
|---|---|---|
| Put | `redis_store_client.cc:150-151` | `HSET/HSETNX RAY{ns}@{table} key data` |
| Get | `redis_store_client.cc:179-180` | `HGET RAY{ns}@{table} key` |
| GetAll | `redis_store_client.cc:188-189` | `HGETALL RAY{ns}@{table}`（通过 Scanner） |
| Delete | `redis_store_client.cc:343` | `HDEL RAY{ns}@{table} keys` |
| MGet | `redis_store_client.cc:83-84` | `HMGET RAY{ns}@{table} key1 key2 ...` |
| GetKeys | `redis_store_client.cc:476-477` | `HSCAN RAY{ns}@{table}` |
| Exists | `redis_store_client.cc:493-494` | `HEXISTS RAY{ns}@{table} key` |
| GetNextJobID | `redis_store_client.cc:460-461` | `INCRBY RAY{ns}@JobCounter 1` |

### 5.5 C++ Core：Redis 清理（RedisDelKeyPrefixSync）

`src/ray/gcs/store_client/redis_store_client.cc:520-594`：
```cpp
bool RedisDelKeyPrefixSync(const std::string &host, int32_t port,
                           const std::string &username, const std::string &password,
                           bool use_ssl, const std::string &external_storage_namespace) {
  // 构造匹配模式: RAY{namespace}@*
  RedisKey redis_key{external_storage_namespace, /*table_name=*/""};
  std::string match_pattern = RedisMatchPattern::Prefix(redis_key.ToString()).escaped_;

  // SCAN 遍历所有匹配的 key
  do {
    std::vector<std::string> cmd{"SCAN", std::to_string(cursor), "MATCH", match_pattern};
    // ...
    keys.insert(keys.end(), ...);
  } while (cursor != 0);

  // 逐个 DEL 删除
  for (const auto &key : keys) {
    if (delete_one_sync(*key)) { num_deleted++; }
    else { num_failed++; }
  }
}
```
- RayCluster 删除时，KubeRay 创建 Kubernetes Job 调用此函数清理 Redis 数据
- 匹配模式：`RAY{namespace}@*`，删除该 namespace 下的所有 HASH 表

### 5.6 Python：cleanup_redis_storage

`python/ray/_private/gcs_utils.py:106-155`：
```python
def cleanup_redis_storage(host, port, password, use_ssl, storage_namespace, username=None):
    from ray._raylet import del_key_prefix_from_storage
    # 删除所有 RAY{key_prefix}@ 前缀的 key
    return del_key_prefix_from_storage(host, port, username, password, use_ssl, storage_namespace)
```

### 5.7 Python：Cython 绑定

`python/ray/_raylet.pyx:4870-4873`（大致位置）：
```python
def del_key_prefix_from_storage(host, port, username, password, use_ssl, key_prefix):
    return RedisDelKeyPrefixSync(host, port, username, password, use_ssl, key_prefix)
```

### 5.8 GCS Server：存储类型选择

`src/ray/gcs/gcs_server.cc:567-583`：
```cpp
GcsServer::StorageType GcsServer::GetStorageType() const {
  if (RayConfig::instance().gcs_storage() == kInMemoryStorage) {
    if (!config_.redis_address.empty()) {
      return StorageType::REDIS_PERSIST;  // 配置了 redis_address 则用 Redis
    }
    return StorageType::IN_MEMORY;
  }
  if (RayConfig::instance().gcs_storage() == kRedisStorage) {
    RAY_CHECK(!config_.redis_address.empty());
    return StorageType::REDIS_PERSIST;
  }
  // ...
}
```
- `gcs_storage="memory"`（默认）+ 无 redis_address → 内存存储
- `gcs_storage="memory"` + 有 redis_address → Redis 持久化（兼容旧行为）
- `gcs_storage="redis"` → 必须提供 redis_address，否则 CHECK 失败

`src/ray/gcs/gcs_server.h:129-136`：
```cpp
enum class StorageType { UNKNOWN = 0, IN_MEMORY = 1, REDIS_PERSIST = 2 };
static constexpr char kInMemoryStorage[] = "memory";
static constexpr char kRedisStorage[] = "redis";
```

### 5.9 测试：namespace 隔离验证

`python/ray/tests/test_gcs_utils.py:202-228`：
```python
def test_external_storage_namespace_isolation(shutdown_only):
    # 用 namespace=c1 写入 KV
    addr = ray.init(namespace="a", _system_config={"external_storage_namespace": "c1"})
    gcs_client.internal_kv_put(b"ABC", b"DEF", True, None)

    # 用 namespace=c2 读不到 c1 的数据
    addr = ray.init(namespace="a", _system_config={"external_storage_namespace": "c2"})
    assert gcs_client.internal_kv_get(b"ABC", None) is None

    # 切回 namespace=c1，数据仍在
    addr = ray.init(namespace="a", _system_config={"external_storage_namespace": "c1"})
    assert gcs_client.internal_kv_get(b"ABC", None) == b"DEF"
```

`python/ray/tests/test_advanced_9.py:196-221`：
```python
@pytest.mark.parametrize("call_ray_start,call_ray_start_2", [
    ({"env": {"RAY_external_storage_namespace": "A1"}},
     {"env": {"RAY_external_storage_namespace": "A2"}})
])
def test_storage_isolation(external_redis, call_ray_start, call_ray_start_2):
    # 两个不同 namespace 的集群，detached actor 互相隔离
```

## 6. KubeRay CR 与 annotation 格式

KubeRay 的所有 CR 都在 `ray.io` API group 下，annotations 遵循 `ray.io/` 前缀的 Kubernetes 命名惯例：

| CR 类型 | API Group | 常见 annotations |
|---|---|---|
| RayCluster | `ray.io/v1` | `ray.io/ft-enabled`, `ray.io/external-storage-namespace` |
| RayService | `ray.io/v1` | `ray.io/initializing-timeout`, `ray.io/external-storage-namespace` |
| RayJob | `ray.io/v1` | `ray.io/managed-job` |

KubeRay 1.3.0+ 推荐使用 CRD 字段 `gcsFaultToleranceOptions.externalStorageNamespace` 代替 annotation `ray.io/external-storage-namespace`，功能等价。
