# Ray 作业 OOM 故障排查指南

本文档记录了一次 Ray 作业因 OOM (Out of Memory) 失败的完整排查过程，涵盖问题现象、根因分析、Ray 架构知识以及解决方案。

---

## 目录

1. [问题背景](#问题背景)
2. [错误现象](#错误现象)
3. [根因分析](#根因分析)
4. [Ray 架构知识](#ray-架构知识)
5. [解决方案](#解决方案)
6. [附录：相关错误](#附录相关错误)

---

## 问题背景

### 作业信息

| 项目 | 值 |
|------|-----|
| Job ID | `07000000` |
| 集群 | 可灵 Ray 集群 (`raycluster-keling-data`) |
| 运行时长 | 6h 13m |
| 最终状态 | **FAILED** |
| 集群规模 | ~250 个节点 |

### 作业参数

```bash
python multishot_video_classifier_pipeline_checkpoint.py \
    --cpu-concurrency 3000 \
    --gpu-concurrency 5000 \
    --preprocess-concurrency 11000 \
    --sink-concurrency 1000 \
    --input-format parquet \
    --output-format kafka \
    --max-errored-blocks 500 \
    --target-num-rows-per-block 400
```

---

## 错误现象

### 错误日志

```
(raylet) [2026-04-21 07:36:04,229 E 1008 1008] (raylet) node_manager.cc:3250:
5 Workers (tasks / actors) killed due to memory pressure (OOM),
0 Workers crashed due to other reasons at node
(ID: 71e7878eb13c726d6095054ce41704d644de5eaabe84b783773b159d, IP: 10.15.3.158)
```

### 详细错误信息

```
Unexpected error occurred: Task was killed due to the node running low on memory.

Memory on the node (IP: 10.15.3.158, ID: 71e7878eb13c726d...) where the lease
(lease ID: e5a14101...,
 name=_ray_internal_job_actor_raysubmit_hNFsAkesCQdaPJgt:JobSupervisor.__init__,
 pid=103041, memory used=0.08GB)
was running was 74.32GB / 78.12GB (0.951353), which exceeds the memory usage
threshold of 0.95.

Ray killed this worker (ID: 120780289d25b553...) because it was the most
recently scheduled task.
```

### 关键指标

| 指标 | 值 |
|------|-----|
| 节点内存使用 | 74.32GB / 78.12GB (**95.1%**) |
| OOM 阈值 | 95% (`RAY_memory_usage_threshold` 默认值) |
| 被杀进程 | `JobSupervisor.__init__` |
| 被杀 Worker 数 | 5 个 |

### 如何确定被杀的进程

从日志的 **lease name** 字段可以直接看到被杀的任务：

```
name=_ray_internal_job_actor_raysubmit_hNFsAkesCQdaPJgt:JobSupervisor.__init__
```

这里明确标识了：
- `raysubmit_hNFsAkesCQdaPJgt` - Job 提交 ID
- `JobSupervisor.__init__` - 被杀的具体任务（Job 管理器的初始化方法）

---

## 根因分析

### 问题节点内存占用 Top 10

| PID | 内存 (GB) | 进程 | 说明 |
|-----|----------|------|------|
| 74 | **35.19** | `gcs_server` | ⚠️ **异常高** |
| 103109 | 9.96 | Pipeline 主进程 | 用户作业 |
| 1008 | 3.50 | `raylet` | 正常 |
| 491 | 2.03 | `DashboardNodeHead` | 正常 |
| 1058 | 0.39 | `DashboardAgent` | 正常 |
| 494 | 0.22 | `StateHead` | 正常 |
| 303 | 0.15 | `dashboard.py` | 正常 |
| 493 | 0.15 | `ServeHead` | 正常 |
| 488 | 0.14 | `DataHead` | 正常 |
| 1 | 0.12 | `ray start --head` | 正常 |

### 根因总结

1. **GCS Server 内存异常**
   - `gcs_server` 占用 **35.19GB**，这是异常高的值
   - 正常情况下 GCS Server 内存占用应该在几 GB 以内
   - 可能原因：大量 Actor/Task 元数据累积、Object 引用未释放、内存泄漏

2. **Head 节点资源竞争**
   - JobSupervisor 被调度到 Head 节点运行
   - Head 节点同时运行 GCS、Dashboard、Raylet 等关键服务
   - 总内存 78GB 被系统服务占满，用户任务无法正常运行

3. **高并发配置加重负担**
   ```
   --cpu-concurrency 3000
   --gpu-concurrency 5000
   --preprocess-concurrency 11000
   ```
   这些高并发设置会产生大量 Actor 元数据，加重 GCS Server 的内存负担

---

## Ray 架构知识

### GCS Server 的位置

**GCS Server 只运行在 Head 节点上**，这是 Ray 的核心架构设计：

| 组件 | Head 节点 | Worker 节点 | 说明 |
|------|----------|-------------|------|
| **GCS Server** | ✅ 唯一实例 | ❌ | 全局控制服务，管理集群元数据 |
| Raylet | ✅ | ✅ | 本地资源管理和任务调度 |
| Object Store | ✅ | ✅ | 分布式对象存储 |
| Dashboard | ✅ | ❌ | Web UI 和 API |
| Dashboard Agent | ✅ | ✅ | 收集节点信息 |

### GCS Server 的职责

GCS (Global Control Service) 是 Ray 的"大脑"，负责：
- Actor 生命周期管理
- Task 元数据存储
- 资源管理和节点注册
- Placement Group 管理
- Object 位置索引

当集群规模大、并发度高时，GCS Server 的内存压力会显著增加。

### JobSupervisor 的作用

`JobSupervisor` 是 Ray Job 的管理 Actor，负责：
- 监控 Job 状态
- 管理 Driver 进程
- 处理 Job 完成/失败逻辑

默认情况下，JobSupervisor 可能被调度到 Head 节点。

---

## 解决方案

### 方案 1：Head 节点不提供用户资源（推荐）

启动 Head 节点时设置 `--num-cpus=0`，使其不参与用户任务调度：

```bash
# Head 节点启动
ray start --head \
    --num-cpus=0 \
    --num-gpus=0 \
    --dashboard-host=0.0.0.0 \
    --port=6379

# Worker 节点启动
ray start --address=<head_ip>:6379
```

**效果**：Head 节点专门运行系统服务（GCS、Dashboard），用户任务只调度到 Worker 节点。

### 方案 2：使用节点标签 + 调度约束

```bash
# Worker 节点启动时打标签
ray start --address=<head>:6379 --resources='{"worker": 1}'
```

```python
# Task/Actor 只调度到有 worker 资源的节点
@ray.remote(resources={"worker": 1})
def my_task():
    ...

@ray.remote(resources={"worker": 1})
class MyActor:
    ...
```

### 方案 3：降低并发度

减少 Actor/Task 数量，降低 GCS Server 内存压力：

```bash
# 原配置
--cpu-concurrency 3000
--gpu-concurrency 5000
--preprocess-concurrency 11000

# 降低后
--cpu-concurrency 1500
--gpu-concurrency 2500
--preprocess-concurrency 5000
```

### 方案 4：调整 OOM 阈值（临时方案）

```bash
# 提高阈值（不推荐，可能导致系统崩溃）
RAY_memory_usage_threshold=0.98 ray start --head ...
```

⚠️ **警告**：这只是临时方案，可能导致系统不稳定。

### 方案 5：增加 Head 节点内存

使用更大内存的机器作为 Head 节点：
- 当前：78GB
- 建议：128GB+ （对于大规模集群）

### 方案 6：重启集群清理 GCS 状态

如果 GCS 内存是累积泄漏问题，重启集群可以临时解决：

```bash
ray stop --force
ray start --head ...
```

---

## 常见误区

### scheduler_spread_threshold 不能排除 Head 节点

```python
# ❌ 这个配置不能避免 Head 节点调度用户任务
ray.init(
    _system_config={
        "scheduler_spread_threshold": 0.5
    }
)
```

`scheduler_spread_threshold` 只是控制任务在节点间的分散程度，**不能用于排除特定节点**。

正确的方法是：
1. `--num-cpus=0` 启动 Head 节点
2. 使用节点标签 + `resources` 调度约束

---

## 附录：相关错误

### runtime_env 下载失败

在 OOM 发生前，可能会观察到以下错误：

```
OSError: Failed to download runtime_env file package
gcs://_ray_pkg_8621e4904679618c.zip from the GCS to the Ray worker node.
The package may have prematurely been deleted from the GCS due to a long
upload time or a problem with Ray.
```

**这个错误可能是 OOM 的连锁反应**：
1. Head 节点内存不足
2. GCS Server 异常
3. runtime_env 包引用被提前清理
4. Worker 无法下载依赖包

**解决方案**：
1. 先解决 OOM 问题，runtime_env 错误可能自动消失
2. 或者增加临时引用过期时间：
   ```bash
   export RAY_RUNTIME_ENV_TEMPORARY_REFERENCE_EXPIRATION_S=1800  # 30分钟
   ```

### OOM 相关环境变量

| 环境变量 | 默认值 | 说明 |
|---------|-------|------|
| `RAY_memory_usage_threshold` | 0.95 | 内存使用阈值，超过后开始杀 Worker |
| `RAY_memory_monitor_refresh_ms` | 250 | 内存监控刷新间隔，设为 0 禁用 OOM killer |
| `RAY_RUNTIME_ENV_TEMPORARY_REFERENCE_EXPIRATION_S` | 600 | runtime_env 包临时引用过期时间 |

---

## 诊断命令

```bash
# 查看详细 OOM 日志
ray logs raylet.out -ip <node_ip>

# 查看被杀 worker 的日志
ray logs worker-<worker_id>*out -ip <node_ip>

# 查看集群状态
ray status

# 查看节点内存使用
ray memory --stats-only
```

---

## 参考链接

- [Ray OOM Prevention](https://docs.ray.io/en/latest/ray-core/scheduling/ray-oom-prevention.html)
- [Ray Cluster Configuration](https://docs.ray.io/en/latest/cluster/vms/references/ray-cluster-configuration.html)
- [Ray Architecture](https://docs.ray.io/en/latest/ray-core/architecture.html)
