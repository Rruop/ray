# Ray 节点故障分析报告

**故障节点**: `97d5a2526a8e8ccc56f29e1a8d9c03b108f4e9950ebfa00fa469bb3e`
**节点 IP**: `10.48.75.241`
**故障时间**: 2026-04-21 16:42:34
**分析时间**: 2026-04-21

---

## 1. 问题描述

Ray 任务执行过程中，多个 task 同时失败，报错信息如下：

### 错误 1: 节点死亡导致 Task 失败

```
Task failed because the node it was running on is dead or unavailable.
Node IP: 10.48.75.241, node ID: 97d5a2526a8e8ccc56f29e1a8d9c03b108f4e9950ebfa00fa469bb3e.
This can happen if the node was preempted, had a hardware failure, or its raylet crashed unexpectedly.
```

### 错误 2: Object 重建失败

```
ray.exceptions.ObjectReconstructionFailedError: Failed to retrieve object 9d05db7e0eaf4e33ffffffffffffffffffffffff1700000002000000.

[OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED] The object cannot be reconstructed
because the maximum number of task retries has been exceeded.
Consider increasing the number of retries using `@ray.remote(max_retries=N)`.
```

**错误链分析**:
```
ClipMergeMapper (FlatMap)
    ↑ 依赖
MapWorker (DistributedStreamingVideoProcessMapper)
    ↑ 依赖
StreamingRepartition
    ↑ 依赖
Object 9d05db7e... ← 根因：重建失败 (重试次数耗尽)
```

---

## 2. 日志时间线分析

| 时间 | 事件类型 | 详细信息 |
|------|----------|----------|
| 16:35:33 | 节点上线 | 节点正常加入集群 `IsAlive = 1` |
| 16:36:13 | Object 重建 | 多个 object 更新 primary location，提示正在进行 reconstruction |
| 16:36:19 | Object 重建 | 继续有 object reconstruction 活动 |
| 16:36:26 | Actor 重启 | Actor `c3dcc40c...` 状态 ALIVE，`num_restarts: 3`，表明已重启过3次 |
| **16:42:34** | **节点失联** | 大量 RPC 错误，多个 task 同时失败 |
| 16:44:09 | 节点死亡确认 | GCS 确认节点死亡 `IsAlive = 0` |

---

## 3. 关键日志解读

### 3.1 节点上线 (16:35:33)

```
[2026-04-21 16:35:33,401 I] accessor.cc:436: Received address and liveness notification for node, IsAlive = 1
node_id=97d5a2526a8e8ccc56f29e1a8d9c03b108f4e9950ebfa00fa469bb3e
```

节点正常注册到 GCS，状态为存活。

### 3.2 Object Reconstruction 活动 (16:36:13-19)

```
[2026-04-21 16:36:13,200 I] reference_counter.cc:930: Updating primary location for object to node 97d5a25...,
but it already has a primary location 2732c1b7.... This should only happen during reconstruction
object_id=4271415eabffaf49ffffffffffffffffffffffff1700000016000000
```

**分析**: 多个 object 正在更新其 primary location，日志提示"This should only happen during reconstruction"。这表明：
- 之前可能有其他节点故障，导致 object 需要重建
- 系统整体存在不稳定性

### 3.3 Actor 重启记录 (16:36:26)

```
[2026-04-21 16:36:26,682 I] actor_manager.cc:236: received notification on actor, state: ALIVE,
ip address: 10.48.75.241, port: 10026, num_restarts: 3, death context type=CONTEXT_NOT_SET
actor_id=c3dcc40c9dbe960eb8e3a42e17000000
```

**分析**:
- Actor 已经重启了 3 次
- `death context type=CONTEXT_NOT_SET` 表示之前的死亡原因未被记录
- 说明该集群已经经历过多次故障

### 3.4 节点突然失联 (16:42:34)

#### 第一批错误 - Connection Reset

```
[2026-04-21 16:42:34,410 W] normal_task_submitter.cc:633: Failed to fetch worker failure cause
with status RpcError: RPC error: recvmsg:Connection reset by peer rpc_code: 14
worker id: 23a8c168c20ea65a3a62bfd7deca260f4e20533101d76b3d41700eb4
node id: 97d5a2526a8e8ccc56f29e1a8d9c03b108f4e9950ebfa00fa469bb3e ip: 10.48.75.241
```

**分析**: `Connection reset by peer` 表示对端（节点上的进程）主动关闭了连接。

#### 第二批错误 - Connection Refused

```
[2026-04-21 16:42:34,428 W] normal_task_submitter.cc:633: Failed to fetch worker failure cause
with status RpcError: RPC error: failed to connect to all addresses;
last error: UNKNOWN: ipv4:10.48.75.241:44235: Failed to connect to remote host: Connection refused rpc_code: 14
```

**分析**: `Connection refused` 表示目标端口 44235 (raylet 端口) 已经没有进程在监听，raylet 进程已死亡。

#### Task 失败记录

```
[2026-04-21 16:42:34,410 W] task_manager.cc:1360: Task attempt 77fee59e69ef5841...
failed with error NODE_DIED Fail immediately? 0, status RpcError: RPC error: Socket closed rpc_code: 14
```

**分析**:
- 错误类型: `NODE_DIED`
- `Fail immediately? 0` 表示任务不会立即标记为最终失败，可以重试
- 在同一秒内有 10+ 个不同的 task 同时失败

### 3.5 节点死亡确认 (16:44:09)

```
[2026-04-21 16:44:09,859 I] accessor.cc:436: Received address and liveness notification for node,
IsAlive = 0 node_id=97d5a2526a8e8ccc56f29e1a8d9c03b108f4e9950ebfa00fa469bb3e

[2026-04-21 16:44:09,859 I] core_worker.cc:751: Node failure. All objects pinned on that node
will be lost if object reconstruction is not enabled.
```

**分析**:
- GCS 正式确认节点死亡
- 从实际失联 (16:42:34) 到正式确认 (16:44:09) 间隔约 1.5 分钟
- 警告: 如果未启用 object reconstruction，该节点上 pin 的所有 object 将丢失

---

## 4. 根因分析

### 4.1 故障特征

1. **瞬时性**: 节点在极短时间内从正常变为完全不可达
2. **批量性**: 多个 worker 同时失败，而非逐个失败
3. **彻底性**: 从 `Connection reset` 快速恶化到 `Connection refused`

### 4.2 可能的根本原因

| 可能原因 | 概率 | 依据 |
|----------|------|------|
| **节点被抢占 (Preemption)** | 高 | 如果使用 spot/preemptible 实例，云平台可能回收节点。符合瞬时、彻底的特征 |
| **OOM Killer** | 中高 | 内存不足导致 Linux OOM Killer 杀死 raylet 进程。需要检查系统日志确认 |
| **硬件故障** | 中 | 物理机故障、网络隔离等 |
| **Raylet 进程 Crash** | 中 | raylet 本身 bug 或异常导致崩溃 |
| **人为操作** | 低 | 误操作杀死进程或关闭节点 |

### 4.3 辅助证据

1. **系统已有不稳定迹象**:
   - Object reconstruction 活动表明之前有节点故��
   - Actor 已重启 3 次

2. **故障模式分析**:
   - 先出现 `Connection reset by peer` (进程正在关闭)
   - 然后 `Connection refused` (进程已死亡)
   - 这种模式更符合进程被杀死而非网络问题

---

## 5. 影响评估

### 5.1 直接影响

- **失败的 Task 数量**: 至少 11 个 (根据日志中不同的 task attempt ID)
- **数据丢失风险**: 该节点上 pin 的 object 可能丢失 (如未启用 reconstruction)

### 5.2 失败的 Task 列表

| Task Attempt ID | 失败时间 |
|-----------------|----------|
| 77fee59e69ef5841... | 16:42:34.410 |
| 06bb2f58a884ba5e... | 16:42:34.411 |
| 45bb5dfb770634d7... | 16:42:34.428 |
| b86e8fa37587c203... | 16:42:34.435 |
| 540d1ace71cb214a... | 16:42:34.435 |
| de2ee2d164d15e28... | 16:42:34.467 |
| ddb324c723a58198... | 16:42:34.467 |
| 56cd48915eb59e00... | 16:42:34.471 |
| 411de00bdca1541e... | 16:42:34.473 |
| 3e1e45832b44cd97... | 16:42:34.500 |
| 55ed3705470ecc9d... | 16:42:34.524 |

---

## 6. Object Reconstruction 机制详解

### 6.1 什么是 Object Reconstruction

当 Ray object 丢失时（如节点死亡），Ray 可以通过 **Lineage Reconstruction** 重建对象：
1. Ray 保存了每个 object 的"谱系"信息（哪个 task 创建了它）
2. 当 object 丢失时，Ray 重新执行生成该 object 的 task
3. 如果 task 的输入参数也丢失了，递归重建这些参数

### 6.2 核心配置参数

| 参数 | 位置 | 默认值 | 作用 |
|------|------|--------|------|
| `lineage_pinning_enabled` | C++ 配置 | **`true`** | 是否保留 task lineage 以支持 object reconstruction |
| `max_lineage_bytes` | C++ 配置 | `1GB` | lineage 信息最大内存占用，超过后驱逐 50% |
| `max_retries` | Task 参数 | `3` | 单个 task 最大重试次数 |
| `max_task_retries` | Actor 参数 | `0` | Actor task 最大重试次数 |
| `RAY_TASK_MAX_RETRIES` | 环境变量 | 无 | 全局覆盖 task 默认重试次数 |

### 6.3 参数关系图

```
┌─────────────────────────────────────────────────────────────────┐
│                    Object Reconstruction 机制                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   lineage_pinning_enabled = true (默认)                         │
│         │                                                        │
│         ▼                                                        │
│   ┌─────────────────┐    max_lineage_bytes = 1GB                │
│   │  保存 Task 谱系  │◄──────────────────────────────────────────┤
│   │   (Lineage)     │    超过 1GB 驱逐 50%，被驱逐的 object     │
│   └────────┬────────┘    将无法重建                              │
│            │                                                      │
│            ▼                                                      │
│   Object 丢失时触发重建                                          │
│            │                                                      │
│            ▼                                                      │
│   ┌─────────────────┐                                            │
│   │  重新执行 Task   │◄── max_retries 控制重试次数              │
│   └────────┬────────┘    默认 3 次，-1 无限                      │
│            │                                                      │
│            ▼                                                      │
│   ┌─────────────────────────────────────────────┐                │
│   │ 成功: 返回重建的 object                       │                │
│   │ 失败: ObjectReconstructionFailedError        │                │
│   │       (MAX_ATTEMPTS_EXCEEDED)                │                │
│   └─────────────────────────────────────────────┘                │
└─────────────────────────────────────────────────────────────────┘
```

### 6.4 各参数详细说明

#### `lineage_pinning_enabled` (C++ 层)

```cpp
// src/ray/common/ray_config_def.h:138-142
/// Whether to pin object lineage, i.e. the task that created the object and
/// the task's recursive dependencies. If this is set to true, then the system
/// will attempt to reconstruct the object from its lineage if the object is
/// lost.
RAY_CONFIG(bool, lineage_pinning_enabled, true)
```

- **默认 `true`**，即 Object Reconstruction 机制**默认启用**
- 控制是否保存 task 的"谱系"信息（哪个 task 创建了这个 object，以及它的依赖）

#### `max_lineage_bytes` (C++ 层)

```cpp
// src/ray/common/ray_config_def.h:144-150
/// Maximum amount of lineage to keep in bytes...
/// If we reach this limit, 50% of the current lineage will be evicted and
/// objects that are still in scope will no longer be reconstructed if lost.
RAY_CONFIG(int64_t, max_lineage_bytes, 1024 * 1024 * 1024)  // 1GB
```

- 限制 lineage 信息的内存使用
- 超过后驱逐 50%，被驱逐的 object 将无法重建
- 可通过环境变量 `RAY_max_lineage_bytes` 调整

#### `max_retries` (Task 参数)

```python
@ray.remote(max_retries=3)  # 默认值
def my_task():
    pass
```

- 控制 task 失败时的重试次数
- `3`: 默认值，最多重试 3 次
- `-1`: 无限重试
- `0`: 禁用重试（等同于禁用 reconstruction）

#### `max_task_retries` (Actor 参数)

```python
@ray.remote(max_restarts=3, max_task_retries=3)
class MyActor:
    pass
```

- 控制 Actor task 的重试次数
- **默认 `0`**，Actor task 默认不可重建
- 需要显式设置才能启用 Actor task 的重建

### 6.5 Ray Data 的特殊配置

```python
# python/ray/data/_internal/remote_fn.py:31-38
default_ray_remote_args = {
    "scheduling_strategy": "DEFAULT",
    "max_retries": -1,  # Ray Data 设置为无限重试
}
```

Ray Data 将 `max_retries` 设为 `-1`，使得 object reconstruction 可以一直重试直到成功。

### 6.6 重建终止条件

| 条件 | 结果 |
|------|------|
| **重建成功** | 正常返回 object |
| **重试次数耗尽** | `ObjectReconstructionFailedError` (MAX_ATTEMPTS_EXCEEDED) |
| **Lineage 被驱逐** | `ObjectReconstructionFailedError` (超过 `max_lineage_bytes` 1GB) |
| **Owner 死亡** | `OwnerDiedError` (不可恢复) |
| **ray.put() 创建的对象** | `ObjectReconstructionFailedError` (无 lineage) |
| **Actor task 且 max_task_retries=0** | `ObjectReconstructionFailedError` |

### 6.7 重建流程图

```
Object 丢失
    │
    ▼
检查 lineage 是否存在？ ──否──► ObjectReconstructionFailedError
    │
   是
    │
    ▼
Owner 还活着？ ──否──► OwnerDiedError
    │
   是
    │
    ▼
重新执行 Task ◄─────────────┐
    │                       │
    ▼                       │
执行成功？ ──否──► 重试次数耗尽？ ──否──┘
    │                │
   是               是
    │                │
    ▼                ▼
返回 Object    ObjectReconstructionFailedError
```

### 6.8 错误类型对照表

| 错误类型 | 含义 | Reconstruction 状态 |
|----------|------|---------------------|
| `ObjectLostError` | Object 丢失，无法恢复 | **未启用** |
| `ObjectReconstructionFailedError` | 重建尝试失败 | **已启用但失败** |
| `OwnerDiedError` | Object 的 owner 进程死亡 | 不可恢复 |
| `ObjectFetchTimedOutError` | 获取 object 超时 | 系统级问题 |

### 6.9 本案例分析

错误 `ObjectReconstructionFailedError` + `MAX_ATTEMPTS_EXCEEDED` 说明：

1. ✅ **Reconstruction 机制已启用** (`lineage_pinning_enabled=true`)
2. ❌ **但某个 task 的重试次数耗尽了**

可能原因：
- 上游的某个 operator（如 `MapWorker` actor task）没有继承 Ray Data 的 `max_retries=-1`
- Actor task 默认 `max_task_retries=0`，不支持重试

---

## 7. 排查步骤

### 7.1 查看节点死亡原因 (Ray 命令)

```bash
# 查询节点状态和死亡原因
ray list nodes --filter node_id=97d5a2526a8e8ccc56f29e1a8d9c03b108f4e9950ebfa00fa469bb3e

# 查看 raylet 日志
ray logs raylet.out -ip 10.48.75.241

# 查看 GCS 日志中的相关信息
ray logs gcs_server.out | grep 97d5a2526a8e8ccc56f29e1a8d9c03b108f4e9950ebfa00fa469bb3e
```

### 7.2 检查系统日志 (需要 SSH 到节点)

```bash
# 检查 OOM Killer
dmesg | grep -i "oom\|killed\|out of memory"

# 检查系统日志
journalctl -u ray --since "2026-04-21 16:40:00" --until "2026-04-21 16:45:00"

# 检查是否有硬件错误
dmesg | grep -i "error\|fail\|hardware"

# 检查内存使用历史 (如果有监控)
sar -r -s 16:40:00 -e 16:45:00
```

### 7.3 检查云平台事件 (如适用)

- **AWS**: 查看 EC2 Instance 的 Status Checks 和 Scheduled Events
- **GCP**: 查看 Compute Engine 的 Operations 日志
- **Azure**: 查看 VM 的 Activity Log

### 7.4 定位 Object 来源

```bash
# 启用 ref 创建位置记录（下次运行时）
export RAY_record_ref_creation_sites=1
ray start ...
```

### 7.5 检查 Task 失败详情

```bash
# 查看失败的 task
ray list tasks --filter state=FAILED
```

---

## 8. 解决方案与建议

### 8.1 短期措施 - 提高容错能力

#### Task 重试配置

```python
# 为 task 启用重试
@ray.remote(max_retries=3, retry_exceptions=True)
def my_task():
    pass

# 或全局配置
ray.init(
    runtime_env={
        "env_vars": {"RAY_TASK_MAX_RETRIES": "10"}
    }
)
```

#### Actor 容错配置

```python
# 为 actor 启用重建
@ray.remote(max_restarts=3, max_task_retries=3)
class MyActor:
    pass
```

#### Ray Data 重试配置

```python
# 设置合理的重试上限（推荐）
ray.data.DataContext.get_current().execution_options.max_retries = 10

# 或针对具体操作
ds.map(fn, max_retries=10)
```

### 8.2 中期措施 - 资源规划

1. **避免 Spot 实例用于关键任务**
   - 对于不可中断的任务，使用 on-demand 实例
   - 或混合使用: head node 用 on-demand，worker nodes 可用 spot

2. **内存预留**
   ```python
   # 配置资源时预留内存
   ray.init(
       _memory=8 * 1024 * 1024 * 1024,  # 8GB
       object_store_memory=4 * 1024 * 1024 * 1024  # 4GB for object store
   )
   ```

3. **监控告警**
   - 配置节点内存使用率告警 (>80%)
   - 配置节点健康检查

### 8.3 长期措施 - 架构优化

1. **Checkpoint 机制**
   ```python
   # 对于长时间运行的任务，定期保存状态
   @ray.remote
   class StatefulActor:
       def __init__(self):
           self.state = self._load_checkpoint()

       def _save_checkpoint(self):
           # 保存到持久存储
           pass
   ```

2. **数据持久化**
   ```python
   # 在关键节点写入持久存储，避免全链路重算
   ds = ray.data.read_parquet("s3://input/...")
   ds = ds.map(step1)
   ds.write_parquet("s3://checkpoint/step1/")  # checkpoint

   ds = ray.data.read_parquet("s3://checkpoint/step1/")
   ds = ds.map(step2)
   ```

3. **集群弹性设计**
   - 使用 Ray Autoscaler 自动补充死亡节���
   - 配置 `upscaling_speed` 加快节点补充速度

---

## 9. 总结

### 故障原因

节点 `10.48.75.241` 在 16:42:34 突然死亡，导致该节点上运行的 11+ 个 task 同时失败。根据日志特征，最可能的原因是**节点被抢占**或**OOM Killer 触发**。

### Object Reconstruction 状态

- ✅ **机制已启用**（`lineage_pinning_enabled=true` 默认开启）
- ❌ **重建失败**（`MAX_ATTEMPTS_EXCEEDED` - 重试次数耗尽）

### 关键发现

1. 集群在故障前已有不稳定迹象 (object reconstruction, actor 多次重启)
2. 节点死亡是瞬时的，符合进程被杀死的特征
3. 从实际失联到 GCS 确认死亡有约 1.5 分钟延迟
4. Object Reconstruction 默认启用，但 Actor task 默认不可重建 (`max_task_retries=0`)

### 后续行动

- [ ] 检查节点系统日志确认根因
- [ ] 检查云平台事件日志
- [ ] 为 Actor task 设置 `max_task_retries > 0`
- [ ] 评估是否需要调整 `max_retries` 配置
- [ ] 在关键步骤添加 checkpoint
- [ ] 评估是否需要调整实例类型（避免 spot 实例）

---

## 附录 A: 相关配置速查

### 环境变量

| 环境变量 | 默认值 | 作用 |
|----------|--------|------|
| `RAY_TASK_MAX_RETRIES` | 3 | 全局 task 重试次数 |
| `RAY_max_lineage_bytes` | 1GB | lineage 最大内存 |
| `RAY_record_ref_creation_sites` | 0 | 记录 ObjectRef 创建位置 |
| `RAY_fetch_fail_timeout_milliseconds` | 600000 | Object fetch 超时时间 |

### Python 配置

```python
# Task 级别
@ray.remote(max_retries=5, retry_exceptions=True)
def my_task():
    pass

# Actor 级别
@ray.remote(max_restarts=3, max_task_retries=3)
class MyActor:
    pass

# Ray Data 级别
ray.data.DataContext.get_current().execution_options.max_retries = 10
```

---

**文档版本**: v2.0
**最后更新**: 2026-04-22
