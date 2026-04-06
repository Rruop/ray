# ACTOR_UNAVAILABLE: Connection refused (rpc_code: 14) 错误分析

## 错误信息

```
Error Type: ACTOR_UNAVAILABLE
The actor is temporarily unavailable: RpcError: RPC error: failed to connect to all addresses; last error: UNKNOWN: ipv4:10.48.33.210:10009: Failed to connect to remote host: Connection refused rpc_code: 14
```

---

## 一、原因分析

`ACTOR_UNAVAILABLE` 表示 Actor 所在节点的 RPC 连接断开（gRPC error code 14 = UNAVAILABLE），`Connection refused` 说明目标节点 `10.48.33.210:10009` 上的 **Raylet 进程已不可达或已死亡**。

根据 Ray 源码（`src/ray/core_worker/task_submission/actor_task_submitter.cc:715-732`），当 RPC 请求失败且无法确认 Actor 已死亡时，会抛出 `ACTOR_UNAVAILABLE`（区别于 `ACTOR_DIED`，Actor 可能仍存活）。

Proto 定义（`src/ray/protobuf/common.proto:254-257`）：

```protobuf
// Actor is unavailable. Maybe there is a temporary network failure. Difference from
// ACTOR_DIED is that the actor may still be alive and may become available again
// after some retries.
ACTOR_UNAVAILABLE = 25;
```

C++ 核心逻辑：

```cpp
// - If we got the death reason: mark the object as failed with that reason.
// - If we did not get the death reason: raise ACTOR_UNAVAILABLE with the status.
// - If we did not get the death reason, but *the actor is preempted*: raise
// ACTOR_DIED. See `CheckTimeoutTasks`.
is_actor_dead = queue.state_ == rpc::ActorTableData::DEAD;
...
// The actor may or may not be dead, but the request failed. Consider the
// failure temporary. May recognize retry, so fail_immediately = false.
error_info.set_error_message("The actor is temporarily unavailable: " +
                             status.ToString());
error_info.set_error_type(rpc::ErrorType::ACTOR_UNAVAILABLE);
error_info.mutable_actor_unavailable_error()->set_actor_id(actor_id.Binary());
```

Python 异常类（`python/ray/exceptions.py:479-490`）：

```python
@DeveloperAPI
class ActorUnavailableError(RayActorError):
    """Raised when the actor is temporarily unavailable but may be available later."""

    def __init__(self, error_message: str, actor_id: Optional[bytes]):
        actor_id = ActorID(actor_id).hex() if actor_id is not None else None
        error_msg = (
            f"The actor {actor_id} is unavailable: {error_message}. The task may or "
            "may not have been executed on the actor."
        )
```

---

## 二、常见根因

| 根因 | 排查方法 |
|------|---------|
| **节点 OOM 导致 Raylet 被 kill** | 登录 `10.48.33.210`，执行 `dmesg -T \| grep -i oom` |
| **Raylet 进程崩溃/异常退出** | 查看节点上 Raylet 日志：`/tmp/ray/session_latest/logs/raylet.out` |
| **网络抖动/分区** | 检查节点间网络连通性 `ping 10.48.33.210` |
| **端口 10009 被占用或未监听** | 在目标节点执行 `ss -tlnp \| grep 10009` |
| **节点负载过高导致健康检查超时** | 查看节点 CPU/内存监控 |

### 心跳相关日志区别

| 日志 | 含义 | 场景 |
|------|------|------|
| `lagging heartbeats` | 心跳延迟，但节点可能还活着 | 网络慢或负载高 |
| `Connection refused` | 节点完全无响应 | Raylet 进程已死（OOM等） |
| `health check failed` | 健康检查多次失败后判死 | 最终结果 |

---

## 三、解决办法

### 3.1 排查节点状态

```bash
# 在 10.48.33.210 节点上执行
dmesg -T | grep -i oom              # 检查 OOM
ps aux | grep raylet                # 检查 Raylet 是否存活
ss -tlnp | grep 10009               # 检查端口监听
cat /tmp/ray/session_latest/logs/raylet.out | tail -200  # 查看 Raylet 日志
```

### 3.2 根据根因修复

1. **如果是 OOM**：增加节点内存，或通过 `ray.init(object_store_memory=...)` 限制对象存储内存
2. **如果是网络抖动**：这是临时性错误，Ray 会自动重试（`IsGrpcRetryableStatus` 会重试 UNAVAILABLE 错误），可调整重连参数：
   - `gcs_grpc_max_reconnect_backoff_ms`（默认 2000ms）
   - `gcs_rpc_server_reconnect_timeout_s`（默认 60s）
3. **如果是 Raylet 崩溃**：查看 Raylet 日志定位崩溃原因，修复后重启节点
4. **代码层面**：可在任务中 catch `ActorUnavailableError` 并添加重试逻辑

---

## 四、关联报错：NodeAffinitySchedulingStrategy 调度失败

### 错误信息

```
Status message: Job supervisor actor could not be scheduled: The actor is not schedulable: The node specified via NodeAffinitySchedulingStrategy doesn't exist any more or is infeasible, and soft=False was specified.
```

### 因果关系

两个报错是**因果关系**，不是线程 busy 导致的：

```
节点 10.48.33.210 Raylet 死亡/OOM
        │
        ├──→ RPC 连接断开 → ACTOR_UNAVAILABLE（Connection refused）
        │
        └──→ GCS 判定节点死亡 → 节点从集群移除
                │
                └──→ Job supervisor actor 使用了 NodeAffinitySchedulingStrategy(soft=False)
                     绑定在该节点上，节点已不存在且 soft=False 不允许调度到其他节点
                     → "The node doesn't exist any more, and soft=False"
```

### 关键点

- `NodeAffinitySchedulingStrategy(soft=False)` 表示**硬亲和**——Actor 必须调度到指定节点，不允许漂移到其他节点
- 节点死亡后，硬亲和的 Actor 无法被重新调度，直接报错
- 这与线程 busy 无关，是**节点级故障**

### 解决办法

1. **根因**：先解决节点 `10.48.33.210` 为什么挂了（大概率 OOM，`dmesg -T | grep -i oom` 确认）
2. **如果不需要硬亲和**：将 `soft=False` 改为 `soft=True`，允许节点不可用时调度到其他节点
3. **Job 配置层面**：检查 Job 提交时是否指定了 `placement_group` 或 `scheduling_strategy`，如果是自动绑定的，需确保节点有足够资源避免单点故障

---

## 五、Ray 源码关键文件索引

### 核心实现

| 文件 | 作用 |
|------|------|
| `src/ray/protobuf/common.proto` | ACTOR_UNAVAILABLE proto 定义 |
| `src/ray/core_worker/task_submission/actor_task_submitter.cc` | ACTOR_UNAVAILABLE 核心逻辑 |
| `src/ray/rpc/grpc_client.h` | gRPC 客户端 UNAVAILABLE 处理 |
| `src/ray/rpc/retryable_grpc_client.h` | 可重试 gRPC 客户端 |
| `src/ray/common/grpc_util.h` | `IsGrpcRetryableStatus()` 函数 |
| `python/ray/exceptions.py` | `ActorUnavailableError` 异常类 |
| `python/ray/_private/serialization.py` | 错误反序列化 |

### 相关配置参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `gcs_rpc_server_reconnect_timeout_s` | 60s | GCS RPC 重连超时 |
| `gcs_grpc_max_reconnect_backoff_ms` | 2000ms | gRPC 最大重连退避时间 |
| `gcs_grpc_min_reconnect_backoff_ms` | 1000ms | gRPC 最小重连退避时间 |
| `gcs_grpc_initial_reconnect_backoff_ms` | 100ms | gRPC 初始重连退避时间 |
