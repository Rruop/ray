# Ray 作业 "Waiting for scheduling: 1068" 问题排查报告

## 1. 问题现象

### 1.1 基本信息

| 项目 | 详情 |
|------|------|
| 集群 | kml-hb2az1-l3-2 |
| 命名空间 | lmserving |
| Head 节点 | 10.60.192.40 |
| 作业 ID | 06000000 (raysubmit_Ni368DUAJXFPnpVv) |
| 问题时间 | 2026-05-23 09:58 起 |
| 卡死确认时间 | 2026-05-23 11:30 后完全无进展 |

### 1.2 现象描述

- Ray 作业显示 **"Waiting for scheduling: 1068"**
- 集群资源看起来充足（66,630 CPU 可用，268 TiB 内存）
- 作业进度完全停滞，`FlatMap(ClipMergeMapper)` 阶段长时间无新 task 完成

### 1.3 作业 Pipeline 结构

```
ReadParquet -> Filter -> Map(VideoClipProcessMapper) -> Filter
  -> StreamingRepartition[num_rows_per_block=64]
    -> MapBatches(DistributedQwenVLVideoProcessMapper)  [GPU actors]
      -> FlatMap(ClipMergeMapper)  ← 卡在这里
        -> StreamingRepartition[num_rows_per_block=15000]
          -> MapBatches(VideoClipInfoKafkaMapper)  [200 actors]
            -> Write
```

---

## 2. 排查过程

### 2.1 第一步：确认集群资源状态

**命令**: `ray status`

```
Total Usage:
  298.0/66630.0 CPU
  1.5/602.0 GPU
  544.00GiB/262.49TiB memory
  120.89GiB/44.06TiB object_store_memory
```

**结论**: 集群资源极度空闲，仅使用了 298 CPU（0.45%），内存使用也极低。

### 2.2 第二步：确认集群节点状态

```python
import ray
ray.init(address='auto')
nodes = ray.nodes()
# 结果:
Total nodes: 2403 (alive)
Dead nodes: 1
Total CPU: 66630.0
Available CPU: 66331.0
Total memory: 268787.3 GiB
Available memory: 268235.3 GiB
```

**结论**: 2403 个节点存活，仅 1 个 dead node，不是节点故障导致。

### 2.3 第三步：确认 Task 状态分布

```python
from ray.util.state import list_tasks
# Job 06000000 的 task 分布:
#   FINISHED: 1580
#   PENDING_NODE_ASSIGNMENT: 1068 (state API 截断显示)
#   RUNNING: 1 → 后来变成 0
```

**关键发现**: state API 报告的 1068 实际是**数据截断**导致的数字，真实 pending 数量远大于此。

### 2.4 第四步：查看 Driver Progress 日志

```
FlatMap(ClipMergeMapper): 68654936/100194311
  Tasks: 78745; Actors: 0; Queued blocks: 0; Resources: 78745.0 CPU, 79.0GiB object store

Active & requested resources: 7.894e+04/6.663e+04 CPU, 100.6GiB/22.0TiB object store
```

**关键发现**:
- Ray Data streaming executor 声称有 **78,745 个 task** 在运行
- 请求了 78,940 CPU，但集群总共只有 66,630 CPU
- 但 `ray status` 显示实际只用了 298 CPU

### 2.5 第五步：确认作业进度停滞

对比两次观察（间隔 7 分钟）:
- 11:30 → `FlatMap: 68653714`
- 11:37 → `FlatMap: 68654936`
- 7 分钟处理量: **1222 rows**（速率 175 rows/min，极低）

继续观察后确认 **进度完全停止**：
- 11:48 → `FlatMap: 68654936`（不再变化）
- 最后一次成功的 task 输出: **03:30:58 UTC**（北京 11:30:58）

### 2.6 第六步：确认 Task 真实运行状态

```python
# State API 查询结果:
Total RUNNING tasks found: 0
SUBMITTED_TO_WORKER: 0
PENDING_NODE_ASSIGNMENT (sample): 100
```

**关键发现**: **没有任何 task 在真正执行！** 298 CPU 使用全部是 actors（200 个 VideoClipInfoKafkaMapper + 其他）占用。

### 2.7 第七步：分析 Driver Core Worker 的 Lease 请求

查看 driver 的 core worker 日志:
```
NodeManagerService.grpc_client.RequestWorkerLease - 7241254 total (78721 active)
Execution time: mean = 3011.74ms
```

**核心发现**:
- Driver 总共发出了 **7,241,254** 个 `RequestWorkerLease` RPC
- 当前 **78,721 个 active**（仍在等待回复！）
- 已收到回复: 7,241,254 - 78,721 = 7,162,533

### 2.8 第八步：检查 Head 节点 Raylet

Head 节点的 raylet state dump:
```
[state-dump] Waiting leases size: 0
[state-dump] num_waiting_for_resource: 0
[state-dump] num_tasks_waiting_for_workers: 0
[state-dump] Number of total spilled leases: 0
[state-dump] Number of spilled waiting leases: 0
[state-dump] NodeManagerService.grpc_server.RequestWorkerLease - 3675147 total (0 active)
```

**结论**: Head raylet 收到了 3,675,147 个 lease 请求，全部已处理（0 active），说明已经通过 spillback 机制转发给了远程 raylet。

### 2.9 第九步：检查远程 Worker 节点 Raylet

连接到远程 worker 节点 (10.48.75.22) 查看其 raylet:
```
[state-dump] Waiting leases size: 0
[state-dump] num_waiting_for_resource: 0
[state-dump] num_tasks_waiting_for_workers: 0
[state-dump] NodeManagerService.grpc_server.RequestWorkerLease - 1381 total (0 active)
```

**重大发现**: 远程 raylet 也没有任何 active lease 请求！所有 1381 个请求都已处理完（0 active）。

### 2.10 第十步：检查 Runtime Env 状态

Worker 节点 runtime_env_agent.log:
```
2026-05-22 17:20:08 -- Runtime env already created successfully.
Env: {"working_dir": "gcs://_ray_pkg_af794fd3f6a7dbed.zip", ...}
```

**结论**: Runtime env 在 05-22 就已创建成功，不是 runtime env 下载卡住。

### 2.11 第十一步：检查 Execution Timeout

Driver 日志中发现大量超时:
```
grep -c 'Execution timeout' → 27265 次
最后一次: 2026-05-23 03:50:52 - Execution timeout after 1200000ms, retry 1/1
```

- 有 **27,265 次** execution timeout（1200 秒 = 20 分钟超时）
- 说明 `ClipMergeMapper` 任务执行非常缓慢，频繁超时

---

## 3. 排查结论

### 3.1 调度路径分析

```
┌─────────────────────────────────────────────────────────────────┐
│                         调度路径                                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Driver Core Worker                                             │
│    │                                                            │
│    ├─── RequestWorkerLease (78721 active, 等待回复) ───┐        │
│    │                                                    │        │
│    v                                                    v        │
│  Head Raylet (10.60.192.40)          Remote Raylets (2403 nodes)│
│    - 收到 3,675,147                    - 收到少量请求           │
│    - 0 active (全部处理完)             - 0 active               │
│    - spillback 给远程                  - 也全部处理完           │
│                                                                  │
│  ❓ 78721 个 RPC 调用在网络中"消失"了                           │
└─────────────────────────────────────────────────────────────────┘
```

### 3.2 根因判定

**Driver Core Worker 认为有 78,721 个 `RequestWorkerLease` RPC 在等待回复，但 Head Raylet 和远程 Raylet 都没有任何 active 请求。**

这表明存在以下情况之一：

#### 可能性 1：gRPC 连接断开 / 请求丢失（最可能）

- Driver 向远程 raylet 发送了大量 lease 请求
- 由于集群规模极大（2403 节点），gRPC 连接可能因为超时、网络抖动等原因断开
- 远程 raylet 已经回复了（grant 或 reject），但回复在网络中丢失
- 或者远程 raylet 已经处理并回复，但 driver 的 gRPC client 由于连接问题没有收到回调
- Driver core worker 的 RequestWorkerLease RPC 一直处于 "active" 状态永远不会超时（Ray 默认不设 lease request timeout）

#### 可能性 2：Raylet 被压垮后静默丢弃

- 78,721 个并发 lease 请求分散到 2403 个节点 ≈ 每节点 ~33 个并发请求
- 某些 raylet 可能在高压下丢弃了请求但没有回复错误
- Driver 永远在等待不会到来的回复

#### 可能性 3：大规模 task execution timeout 引发级联故障

- 27,265 次 execution timeout 导致大量 task 失败
- 失败的 task 需要重试，重试产生新的 lease 请求
- 同时大量 worker 进程因超时被 kill，raylet 需要创建新 worker
- 但新的 lease 请求又因为某种原因丢失在网络中

### 3.3 资源矛盾解释

| 视角 | 数值 | 解释 |
|------|------|------|
| Driver Progress | Tasks: 78,745 | Ray Data streaming executor 认为有这么多 task 在"运行" |
| Driver Core Worker | 78,721 active lease | gRPC 调用未收到回复 |
| ray status (GCS) | 298 CPU used | GCS 统计的实际已分配资源 |
| Head Raylet | 0 waiting | 全部转发完毕 |
| Remote Raylet | 0 waiting | 全部处理完毕 |
| 真实 RUNNING task | 0 | **没有 task 在执行** |

---

## 4. 解决办法

### 4.1 立即止血

```bash
# 取消当前作业
ray job stop raysubmit_Ni368DUAJXFPnpVv
```

78,721 个悬挂的 lease 请求会随 driver 退出而自动取消。

### 4.2 短期修复：降低并发度重新提交

```bash
# 原始参数（推测）
--cpu-concurrency 2000  # 太高

# 建议修改
--cpu-concurrency 200~500  # 大幅降低
```

降低并发度可以：
- 减少同时向远程 raylet 发出的 lease 请求数量
- 降低 gRPC 连接压力
- 避免大量请求丢失

### 4.3 中期优化：解决 task execution timeout

`ClipMergeMapper` 有 27,265 次 execution timeout（20 分钟超时），这是上游问题：

1. **检查 ClipMergeMapper 的执行逻辑**：该 task 下载视频、处理、上传，可能卡在网络 I/O
2. **增加超时时间或改为异步**：如果视频处理本身就慢，需要调整超时策略
3. **增加重试的退避**：避免大量超时后同时重试导致雪崩

### 4.4 长期优化建议

#### A. 避免使用 `working_dir` 的 GCS 包分发

当前配置:
```python
runtime_env = {
    "working_dir": "gcs://_ray_pkg_af794fd3f6a7dbed.zip",
    "env_vars": {"PYTHONPATH": "/ytech_m2v5_hdd/zhangfuxing/kling-ray"}
}
```

建议改为：
```python
runtime_env = {
    "env_vars": {"PYTHONPATH": "/ytech_m2v5_hdd/zhangfuxing/kling-ray"}
}
```

既然已经有共享存储路径 `/ytech_m2v5_hdd/zhangfuxing/kling-ray`，可以避免 2403 个节点同时从 GCS 下载 zip 包。

#### B. 设置 Ray lease request timeout

```python
# 设置环境变量防止 lease 请求永久挂起
RAY_worker_lease_timeout_milliseconds=60000  # 60秒超时
```

#### C. 限制 spillback 的扇出

对于 2403 节点的大集群，driver 向所有节点发送 lease 请求会导致:
- gRPC 连接数爆炸
- 网络压力过大

建议：
- 使用 placement group 或 scheduling strategy 限制 task 分布的节点数
- 或者通过 `ray.remote(scheduling_strategy="SPREAD")` 控制分布

#### D. 监控和告警

添加对以下指标的监控:
- `RequestWorkerLease active count` - 如果持续增长且没有 task 完成，说明 lease 卡死
- `num_tasks_waiting_for_workers` - 远程 raylet 上的排队深度
- Task completion rate - 如果降到 0 应该告警

---

## 5. 关键数据汇总

```
集群规模: 2403 nodes, 66630 CPU, 268 TiB memory
作业类型: Ray Data Streaming Pipeline (多阶段)
卡死阶段: FlatMap(ClipMergeMapper)
每 task 资源需求: CPU=1, memory=8 GiB
已完成进度: 68,654,936 / 100,194,311 (68.5%)
总体进度: 68,640,000 / 97,575,000

Driver 悬挂的 lease 请求: 78,721 个
State API 显示的 pending: 1,068 (截断)
实际 RUNNING task: 0
实际 CPU 使用: 298 (全是 actors)
Execution timeout 次数: 27,265

最后成功 task 完成时间: 2026-05-23 11:30:58 (北京时间)
问题发现时间: 2026-05-23 11:50+ (北京时间)
作业彻底卡死时长: >25 分钟
```

---

## 6. 排查工具和方法总结

| 排查步骤 | 工具/命令 | 目的 |
|----------|-----------|------|
| 集群总览 | `ray status` | 查看资源使用和 pending demands |
| Task 状态 | `ray list tasks --filter state=XXX` | 确认各状态 task 数量 |
| Worker 状态 | `ray list workers --filter is_alive=True` | 确认 worker 进程数 |
| Driver 日志 | `tail job-driver-raysubmit_XXX.log` | 查看 streaming progress |
| Core Worker 日志 | `tail python-core-driver-XXX.log` | 查看 RPC 统计（关键！） |
| Head Raylet | `grep state-dump raylet.out` | 查看调度队列深度 |
| 远程 Raylet | SSH 到 worker 节点查看 `raylet.out` | 确认 lease 是否在远程排队 |
| Runtime Env | `runtime_env_agent.log` | 确认包下载是否卡住 |
| GCS Server | `gcs_server.out` | 查看全局调度状态 |
| Python API | `ray.available_resources()` | 精确资源使用计算 |
