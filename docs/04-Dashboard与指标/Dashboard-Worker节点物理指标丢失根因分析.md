# Ray Dashboard Worker 节点 CPU/podName 指标丢失 — 根因分析

## 问题现象

Ray Dashboard cluster 页面上，worker 节点看不到 CPU/podName 指标。只有 head 节点有完整的物理指标（21 keys），3 个 ALIVE worker 节点只有 1 key（raylet）。

集群状态：128 节点（4 ALIVE / 124 DEAD），head 的 `node_physical_stats` 时间戳过期 5.7 天。

---

## 排查方法

### 1. 从 Dashboard API 层面确认数据缺失

通过 Dashboard API `http://10.175.119.51:8265` 查询 `node_physical_stats`：
- 只有 head 节点 `d49cad92...` 有 cpu/podName 等 21 个 key
- 3 个 ALIVE worker 只有 1 key（raylet）
- head 数据的 `now` 时间戳 = 1782507830，距今 5.7 天

### 2. 排查 ReporterAgent（数据生产端）

ReporterAgent 位于每个节点的 DashboardAgent 进程内，`_run_loop` 每 5 秒通过 `GcsClient.async_publish_node_resource_usage` 发布到 GCS pub/sub。

**验证方法**：在 worker 节点上创建新的 `GcsAioResourceUsageSubscriber` 独立订阅 GCS pub/sub，确认数据正常：
- 4 个 ALIVE 节点每 5 秒正常 publish
- worker payload 约 1.3-1.6MB（327 个 worker 进程）
- head payload 约 98KB

**结论**：ReporterAgent 和 GCS pub/sub 传输完全正常，问题在消费端。

### 3. 排查 TPE(1) 共享争用假设

`NodeHead._node_executor` 是 `ThreadPoolExecutor(max_workers=1)`，被 `_update_node_physical_stats`、`_update_node_stats`、`DataOrganizer.organize` 共享。

**验证方法**：独立创建 subscriber 并在 TPE(1) 中执行 `_parse_node_stats`，测量 poll 间隔。
**结果**：max poll gap 仅 3.46 秒，远小于 subscriber_timeout_ms（30000ms），排除 TPE 争用假设。

### 4. 通过 KML Webshell 连接 Head 节点查看 NodeHead 日志

使用 `kml_ws_exec.py` 连接到 Head 节点（KML URL 略），查看 NodeHead 子进程日志。

**关键发现**：`dashboard_NodeHead.log` 中有 215 次 `RESOURCE_EXHAUSTED` 错误：

```
2026-06-26 18:24:52,026 ~ 2026-06-26 18:28:41,374
StatusCode.RESOURCE_EXHAUSTED: Sent message larger than max (536872354 vs. 536870912)
```

- 实际消息大小：536872354 bytes（512MB + 1442 bytes）
- gRPC 限制：536870912 bytes（512MB）
- 仅超出 1442 bytes
- 215 次错误在约 229 秒内连续发生（每 ~1.06 秒一次）

### 5. 确认协程最终状态

最后一次错误时间 2026-06-26 18:28:41，之后 `_update_node_physical_stats` **不再产出任何日志**（包括错误日志和正常日志）。

NodeHead 子进程（PID 295, incarnation=0）仍在运行，CPU 11.9%，HTTP 请求正常处理（但 `/logical/node_physical_stats` 返回过时数据）。

在 Head 节点上**新建** `GcsAioResourceUsageSubscriber` 验证 GCS pub/sub 仍然正常工作，能成功 poll 到数据。

**结论**：`_update_node_physical_stats` 协程已永久失效（死亡或挂住），但 NodeHead 子进程未崩溃。

---

## 根因链条

### 第一环：GCS 服务端 subscriber mailbox 消息堆积

C++ `Publisher::SubscriberState` 的 `mailbox_` 是一个 `std::deque`，存储待发送给 subscriber 的消息。每条消息通过 `QueueMessage` 入队。

**消息堆积的原因**：
1. 集群有 128 个节点（虽然后来 124 个 DEAD），每个节点每 5 秒 publish 一次 resource usage
2. 当 subscriber 消费速度慢于生产速度，或消费端暂时断连时，mailbox 中消息持续堆积
3. `publisher_entity_buffer_max_bytes = 1GB`（`RAY_NODE_RESOURCE_USAGE_CHANNEL` 的 entity 缓冲限制），单 entity 可以缓冲 1GB 数据
4. 128 节点 × 约 1.5MB/publish × 5 秒间隔 → 如果 subscriber 停止消费 T 秒，mailbox 积压 `128 × T/5 × 1.5MB`

**关键代码**：`publisher.cc:SubscriberState::PublishIfPossible`

```cpp
void SubscriberState::PublishIfPossible(bool force_noop) {
    // ...
    int64_t num_total_bytes = 0;
    for (auto it = mailbox_.begin(); it != mailbox_.end(); it++) {
        if (long_polling_connection_->pub_messages_->size() >= publish_batch_size_) {
            break;  // publish_batch_size = 5000
        }
        int64_t msg_size_bytes = msg.ByteSizeLong();
        if (num_total_bytes > 0 &&
            num_total_bytes + msg_size_bytes >
                static_cast<int64_t>(RayConfig::instance().max_grpc_message_size())) {
            break;  // 512MB 限制
        }
        num_total_bytes += msg_size_bytes;
        *long_polling_connection_->pub_messages_->Add() = msg;
    }
    long_polling_connection_->send_reply_callback_(Status::OK(), nullptr, nullptr);
}
```

**注意**：C++ 端虽然有 `num_total_bytes + msg_size_bytes > max_grpc_message_size` 的截断检查，但：
- 第一条消息（`num_total_bytes == 0`）不检查大小，直接加入
- 截断基于 `ByteSizeLong()` 估算，实际 gRPC 序列化/传输可能有额外开销（如 gRPC frame header、protobuf repeated field tag）
- 实际超出仅 1442 bytes，说明 protobuf 序列化的精确字节数与 `ByteSizeLong()` 有微小差异

### 第二环：gRPC RESOURCE_EXHAUSTED 触发

当 `GcsSubscriberPoll` 返回的 response 序列化大小超过 gRPC 的 `max_receive_message_length`（512MB）时，gRPC 服务端抛出 `RESOURCE_EXHAUSTED`。

gRPC 限制配置：
- Python 客户端：`gcs_utils.py:_MAX_MESSAGE_LENGTH = 512 * 1024 * 1024`
- C++ 服务端：`ray_config_def.h:max_grpc_message_size = 512 * 1024 * 1024`

错误消息：
```
Sent message larger than max (536872354 vs. 536870912)
```

### 第三环：`_should_terminate_polling` 未处理 RESOURCE_EXHAUSTED

`gcs_pubsub.py:_AioSubscriber._should_terminate_polling`：

```python
@staticmethod
def _should_terminate_polling(e: grpc.RpcError) -> None:
    if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
        return True
    if e.code() == grpc.StatusCode.UNAVAILABLE:
        return True
    return False  # RESOURCE_EXHAUSTED 返回 False！
```

当 `_poll()` 内部捕获到 `grpc.RpcError(RESOURCE_EXHAUSTED)` 时：
- `_should_terminate_polling` 返回 False
- 执行 `raise`，异常从 `_poll()` 传播到 `poll()` → 再到 `_update_node_physical_stats`

**对比**：如果返回 True，`_poll()` 会静默 return（不 raise），`poll()` 返回 `(None, None)`，`_update_node_physical_stats` 中 `key is None` → `continue`，循环继续。RESOURCE_EXHAUSTED 是暂时性错误（GCS 服务端在 subscriber 重新 poll 时会重建连接并清空已处理消息），返回 True 是更合理的行为。

### 第四环：`_update_node_physical_stats` 无 `@async_loop_forever` 装饰器

对比 `_update_node_stats`：

```python
@async_loop_forever(node_consts.NODE_STATS_UPDATE_INTERVAL_SECONDS)
async def _update_node_stats(self):
    ...
```

而 `_update_node_physical_stats` **没有** `@async_loop_forever`：

```python
async def _update_node_physical_stats(self):
    subscriber = GcsAioResourceUsageSubscriber(address=self.gcs_address)
    await subscriber.subscribe()
    while True:
        try:
            key, data = await subscriber.poll()
            ...
        except Exception:
            logger.exception("Error receiving node physical stats ...")
```

虽然没有 `@async_loop_forever`，但 `while True + except Exception` 理论上应该能继续循环。**问题在于异常传播路径**：

1. `subscriber.poll()` → `_poll()` → `_poll_call()` 抛出 `AioRpcError(RESOURCE_EXHAUSTED)`
2. `_poll()` 内 `except grpc.RpcError as e:` → `_should_terminate_polling` 返回 False → `raise`
3. 异常从 `_poll()` → `poll()` → `_update_node_physical_stats` 的 `except Exception`
4. `logger.exception()` 打印日志
5. `while True` 继续下一轮 → `subscriber.poll()` → 再次触发 RESOURCE_EXHAUSTED
6. **循环 215 次后停止**

### 第五环：协程最终死亡/挂住 — 推断

215 次错误后（18:28:41），`_update_node_physical_stats` 不再产出任何日志。可能的原因：

**理论 A：gRPC channel 状态损坏**
- 连续 215 次 RESOURCE_EXHAUSTED 后，gRPC channel 可能进入 TRANSIENT_FAILURE 或其他不可恢复状态
- 后续 `_poll_call` 返回 UNAVAILABLE（而非 RESOURCE_EXHAUSTED）
- `_should_terminate_polling(UNAVAILABLE)` 返回 True → `_poll()` return → `poll()` 返回 `(None, None)`
- `_update_node_physical_stats` 中 `key is None` → `continue`，无日志输出
- 协程**还活着**，但永远收不到有效数据（因为 gRPC channel 已损坏，新 poll 永远返回超时或 UNAVAILABLE）

**理论 B：asyncio task 被取消**
- NodeHead 的 `_background_tasks` 管理中，task 异常完成时 `add_done_callback(self._background_tasks.discard)` 只是移除引用
- 如果 task 抛出未捕获异常，asyncio 默认会打印日志但不会重启 task
- 但 `_update_node_physical_stats` 的 `except Exception` 应该捕获了所有异常

**理论 C：subscriber 内部状态损坏**
- 215 次 RESOURCE_EXHAUSTED 后，`_AioSubscriber` 的 `_stub`（gRPC stub）可能不可用
- 后续 poll 请求在 gRPC 层面直接失败，不产生 `grpc.RpcError` 而是产生其他类型的异常
- 或者 `_close` 事件被设置（例如 gRPC channel 关闭回调），导致 `_poll()` 中 `close in done` 为 True → break → `poll()` 返回 `(None, None)`

**最可能的原因是理论 A**：gRPC channel 在连续 215 次 RESOURCE_EXHAUSTED 后状态损坏，后续 poll 返回 UNAVAILABLE → `_should_terminate_polling` 返回 True → 静默退出循环但协程仍存在，只是永远返回 `(None, None)`。

---

## 消息为什么累积到 512MB

GCS 服务端 subscriber 的 mailbox 中消息堆积的根本原因：

1. **集群规模大**：128 节点（包括后来 DEAD 的），每节点每 5 秒 publish 一次
2. **单次 poll 返回批量消息**：`PublishIfPossible` 从 mailbox 中按序取出消息，直到达到 `publish_batch_size=5000` 或 `num_total_bytes > 512MB`
3. **消费端延迟**：如果 `_update_node_physical_stats` 的消费速度低于生产速度（例如 TPE(1) 中 `_parse_node_stats` 阻塞），mailbox 会持续增长
4. **DEAD 节点的残留消息**：节点标记为 DEAD 后，最后一条 publish 消息仍在 mailbox 中，永远不会被 ack（因为 subscriber 不再消费）

估算：
- 4 ALIVE × 1.5MB × 5s = 1.2MB/s
- 124 DEAD × ~1.5MB × 1条（残留）= ~186MB
- 如果 subscriber 暂停 270 秒 + DEAD 节点残留 ≈ 186 + 324 = 510MB ≈ 512MB

**这说明 124 个 DEAD 节点的残留消息是导致 mailbox 积压到 512MB 的主要原因**。每死一个节点，其最后一条 resource usage 消息会留在 GCS 服务端的 subscriber mailbox 中。124 个 DEAD 节点 × 1.5MB ≈ 186MB，加上 ALIVE 节点持续 publish 的消息，足够达到 512MB。

---

## C++ Publisher 的消息大小限制缺陷

`PublishIfPossible` 中的限制逻辑有两个缺陷：

1. **第一条消息不检查大小**：`num_total_bytes > 0` 时才检查，意味着第一条消息即使超过 512MB 也会被加入批次
2. **`ByteSizeLong()` 与实际 gRPC wire size 有微小差异**：protobuf 的 `ByteSizeLong()` 是消息序列化后的大小，但 gRPC 的 `max_send_message_length` 限制的是实际 wire format 大小（可能包含 gRPC frame header 等额外开销），差异 1442 bytes 说明了这一点

---

## 修复方案

### 方案 1：给 `_update_node_physical_stats` 添加 `@async_loop_forever` 装饰器（必须修复）

```python
@async_loop_forever(node_consts.NODE_STATS_UPDATE_INTERVAL_SECONDS)
async def _update_node_physical_stats(self):
    subscriber = GcsAioResourceUsageSubscriber(address=self.gcs_address)
    await subscriber.subscribe()
    while True:
        try:
            key, data = await subscriber.poll()
            if key is None:
                continue
            parsed_data = await self._loop.run_in_executor(
                self._node_executor, _parse_node_stats, data
            )
            node_id = key.split(":")[-1]
            DataSource.node_physical_stats[node_id] = parsed_data
        except Exception:
            logger.exception(
                "Error receiving node physical stats from _update_node_physical_stats."
            )
```

这样即使协程异常退出，`async_loop_forever` 会等待 `NODE_STATS_UPDATE_INTERVAL_SECONDS` 后自动重启。同时需要在每次循环开始时重新创建 subscriber（因为旧的 subscriber 的 gRPC channel 可能已损坏）。

### 方案 2：在 `_should_terminate_polling` 中处理 RESOURCE_EXHAUSTED（推荐）

```python
@staticmethod
def _should_terminate_polling(e: grpc.RpcError) -> None:
    if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
        return True
    if e.code() == grpc.StatusCode.UNAVAILABLE:
        return True
    if e.code() == grpc.StatusCode.RESOURCE_EXHAUSTED:
        return True  # 暂时性错误，GCS 服务端会在下次 poll 时重新构建响应
    return False
```

RESOURCE_EXHAUSTED 是暂时性错误——GCS 服务端在 subscriber 下次 poll 时会自动重建 subscriber 并重新构建响应消息（`CheckDeadSubscribers` 会清理过期 subscriber，新的 poll 请求会创建新的 `SubscriberState`）。将其视为可恢复错误是合理的。

### 方案 3：增大 gRPC max_receive_message_length（可选）

将 `_MAX_MESSAGE_LENGTH` 从 512MB 增大到 1GB 或 2GB：

```python
# gcs_utils.py
_MAX_MESSAGE_LENGTH = 1024 * 1024 * 1024  # 1GB
```

```cpp
// ray_config_def.h
RAY_CONFIG(size_t, max_grpc_message_size, 1024 * 1024 * 1024)  // 1GB
```

**注意**：这只是缓解，不是根治。如果集群更大或消息累积更多，问题仍会发生。而且增大消息限制会增加内存压力。

### 方案 4：限制 GCS 服务端 subscriber mailbox 大小（推荐）

当前 `publisher_entity_buffer_max_bytes = 1GB`（对 `RAY_NODE_RESOURCE_USAGE_CHANNEL`），但没有对 subscriber 的总 mailbox 大小做限制。

建议在 `SubscriberState::QueueMessage` 中添加 mailbox 总大小限制：

```cpp
void SubscriberState::QueueMessage(const std::shared_ptr<rpc::PubMessage> &pub_message) {
    int64_t mailbox_bytes = 0;
    for (const auto &msg : mailbox_) {
        mailbox_bytes += msg->ByteSizeLong();
    }
    if (mailbox_bytes + pub_message->ByteSizeLong() > max_mailbox_bytes_) {
        // Drop oldest messages or reject new message
        RAY_LOG_EVERY_N(WARNING, 1000)
            << "Subscriber mailbox full, dropping message";
        return;
    }
    mailbox_.push_back(pub_message);
    PublishIfPossible(/*force_noop=*/false);
}
```

### 方案 5：清理 DEAD 节点的 subscriber 缓冲（推荐）

当节点标记为 DEAD 后，GCS 服务端的 publisher 应该清理该节点 entity 的缓冲消息，避免残留消息堆积。

当前 `EntityState` 有 `max_buffered_bytes` 限制（`RAY_NODE_RESOURCE_USAGE_CHANNEL` 为 1GB），但没有与节点生命周期关联的清理机制。

### 方案 6：优化 Worker payload 大小（长期优化）

327 个 worker 进程每个 publish 1.3-1.6MB 的 resource usage 数据。可以：
- 减少 publish 频率（如 30 秒一次，而非 5 秒）
- 只发送增量数据
- 压缩 payload

---

## 关键代码位置

| 文件 | 行号 | 说明 |
|------|------|------|
| `python/ray/dashboard/modules/node/node_head.py` | 524-551 | `_update_node_physical_stats`（无 @async_loop_forever） |
| `python/ray/dashboard/modules/node/node_head.py` | 431 | `_update_node_stats`（有 @async_loop_forever） |
| `python/ray/dashboard/modules/node/node_head.py` | 752-766 | `run()` 中 `create_task` 启动所有协程 |
| `python/ray/_private/gcs_pubsub.py` | 56-64 | `_should_terminate_polling`（缺少 RESOURCE_EXHAUSTED 处理） |
| `python/ray/_private/gcs_pubsub.py` | 125-167 | `_AioSubscriber._poll()`（异常处理逻辑） |
| `python/ray/_private/gcs_utils.py` | 54-55 | `_MAX_MESSAGE_LENGTH = 512 * 1024 * 1024` |
| `python/ray/dashboard/utils.py` | 616-640 | `async_loop_forever` 装饰器 |
| `src/ray/pubsub/publisher.cc` | 306-349 | `PublishIfPossible`（批次大小限制，第一条消息不检查） |
| `src/ray/pubsub/publisher.cc` | 476-499 | `CheckDeadSubscribers`（subscriber 超时清理） |
| `src/ray/common/ray_config_def.h` | 223 | `max_grpc_message_size = 512 * 1024 * 1024` |
| `src/ray/common/ray_config_def.h` | 770 | `publisher_entity_buffer_max_bytes = 1 << 30` |

---

## 时间线

| 时间 | 事件 |
|------|------|
| 2026-06-25 23:11 | 集群启动 |
| 2026-06-26 18:24:52 | 第一次 `RESOURCE_EXHAUSTED` 错误 |
| 2026-06-26 18:24:52 ~ 18:28:41 | 连续 215 次 `RESOURCE_EXHAUSTED`（每 ~1 秒一次） |
| 2026-06-26 18:28:41 | `_update_node_physical_stats` 最后一次错误日志 |
| 2026-06-26 18:28:41 ~ 至今 | 协程静默失效，`DataSource.node_physical_stats` 数据过期 |
| 2026-07-02 | 排查确认根因 |

---

## 验证方法

### 在 Head 节点上验证 GCS pub/sub 仍正常

```python
import asyncio
from ray._private.gcs_pubsub import GcsAioResourceUsageSubscriber

async def check():
    sub = GcsAioResourceUsageSubscriber(address='10.175.119.51:6379')
    await sub.subscribe()
    key, data = await asyncio.wait_for(sub.poll(), timeout=10)
    print(f'KEY={key}, DATA_LEN={len(data) if data else 0}')

asyncio.run(check())
# 输出: KEY=RAY_REPORTER:660375feae..., DATA_LEN=24873
```

### 检查 DataSource.node_physical_stats 当前状态

通过 Dashboard API：
```bash
curl -s http://10.175.119.51:8265/api/node_logical/node_physical_stats | python3 -m json.tool
```

### 检查 NodeHead 日志中的错误次数

```bash
grep -c 'Error receiving node physical stats' /tmp/ray/session_latest/logs/dashboard_NodeHead.log
# 输出: 215
```
