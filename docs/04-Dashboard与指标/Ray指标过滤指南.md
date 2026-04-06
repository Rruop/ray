# Ray 指标过滤指南

本文档提供 Ray 指标的全面分析和过滤建议，帮助减少指标存储系统压力。

## 一、指标命名规则

所有 Ray 指标在导出时都会添加 `ray_` 前缀：

| 模块 | 内部名称 | 导出名称（Prometheus） |
|-----|---------|----------------------|
| Ray Data | `data_output_bytes` | `ray_data_output_bytes` |
| Node | `node_cpu_utilization` | `ray_node_cpu_utilization` |
| Core | `actors` | `ray_actors` |
| Serve | `serve_deployment_request_counter` | `ray_serve_deployment_request_counter` |

前缀添加位置：`python/ray/_private/telemetry/open_telemetry_metric_recorder.py` 第 23 行 `NAMESPACE = "ray"`

---

## 二、指标重要性分级

| 级别 | 说明 | 建议 |
|-----|------|-----|
| **P0 - 必须保留** | 核心业务指标、资源使用、错误监控 | 必须保留 |
| **P1 - 建议保留** | 有助于问题诊断的关键指标 | 默认保留 |
| **P2 - 可选** | 深度调试用，生产环境可过滤 | 建议过滤 |
| **P3 - 不推荐** | 过于细粒度或冗余 | 强烈建议过滤 |

---

## 三、各模块指标详细分析

### 1. Ray Data 指标

#### P0 - 必须保留（6个）

| 指标名称 | 描述 |
|---------|------|
| `ray_data_output_bytes` | 输出字节数 |
| `ray_data_output_rows` | 输出行数 |
| `ray_data_num_tasks_running` | 运行中任务数 |
| `ray_data_num_tasks_finished` | 完成任务数 |
| `ray_data_num_tasks_failed` | 失败任务数 |
| `ray_data_current_bytes` | 当前内存使用 |

#### P1 - 建议保留（8个）

| 指标名称 | 描述 |
|---------|------|
| `ray_data_spilled_bytes` | 溢出字节数 |
| `ray_data_freed_bytes` | 释放字节数 |
| `ray_data_cpu_usage_cores` | CPU 使用 |
| `ray_data_gpu_usage_cores` | GPU 使用 |
| `ray_data_iter_total_blocked_seconds` | 迭代器阻塞时间 |
| `ray_data_obj_store_mem_used` | 对象存储使用 |
| `ray_data_obj_store_mem_spilled` | 对象存储溢出 |
| `ray_data_dataset_state` | 数据集状态 |

#### P2 - 可过滤（12个）

```
ray_data_iter_time_to_first_batch_seconds
ray_data_iter_user_seconds
ray_data_num_tasks_submitted
ray_data_num_tasks_have_outputs
ray_data_num_alive_actors
ray_data_num_pending_actors
ray_data_num_restarting_actors
ray_data_operator_state
ray_data_operator_estimated_total_*
ray_data_dataset_estimated_total_*
ray_data_num_errored_blocks
ray_data_obj_store_mem_freed
```

#### P3 - 强烈建议过滤（40+个）

```
# Iterator 内部细节
ray_data_iter_block_*
ray_data_iter_batch_*
ray_data_iter_initialize_*
ray_data_iter_get_*
ray_data_iter_next_*
ray_data_iter_format_*
ray_data_iter_collate_*
ray_data_iter_finalize_*
ray_data_iter_blocks_*
ray_data_iter_prefetched_*

# 输入/输出细节
ray_data_num_inputs_*
ray_data_bytes_inputs_*
ray_data_num_task_inputs_*
ray_data_bytes_task_inputs_*
ray_data_*_inputs_of_submitted_*
ray_data_num_task_outputs_*
ray_data_bytes_task_outputs_*
ray_data_rows_task_outputs_*
ray_data_*_outputs_taken
ray_data_*_outputs_of_finished_*
ray_data_num_external_*
ray_data_average_*

# 内部队列
ray_data_obj_store_mem_internal_*

# 时间细节
ray_data_block_serialization_*
ray_data_block_generation_*
```

---

### 2. Node/Dashboard 指标

#### P0 - 必须保留（8个）

| 指标名称 | 描述 |
|---------|------|
| `ray_node_cpu_utilization` | CPU 利用率 |
| `ray_node_mem_used` | 内存使用 |
| `ray_node_mem_available` | 可用内存 |
| `ray_node_gpus_utilization` | GPU 利用率 |
| `ray_node_gram_used` | GPU 内存使用 |
| `ray_cluster_active_nodes` | 活跃节点数 |
| `ray_cluster_failed_nodes` | 失败节点数 |
| `ray_node_disk_utilization_percentage` | 磁盘利用率 |

#### P1 - 建议保留（6个）

| 指标名称 | 描述 |
|---------|------|
| `ray_node_cpu_count` | CPU 核心数 |
| `ray_node_mem_total` | 总内存 |
| `ray_node_gpus_available` | 可用 GPU |
| `ray_node_gram_available` | 可用 GPU 内存 |
| `ray_node_network_send_speed` | 网络发送速度 |
| `ray_node_network_receive_speed` | 网络接收速度 |

#### P2 - 可过滤（8个）

```
ray_node_disk_usage
ray_node_disk_free
ray_node_disk_io_read_speed
ray_node_disk_io_write_speed
ray_node_network_sent
ray_node_network_received
ray_cluster_pending_nodes
ray_node_mem_shared_bytes
```

#### P3 - 强烈建议过滤（12个）

```
# 累计值（非 rate 用途有限）
ray_node_disk_io_read
ray_node_disk_io_write
ray_node_disk_io_read_count
ray_node_disk_io_write_count
ray_node_disk_read_iops
ray_node_disk_write_iops
ray_node_network_sent
ray_node_network_received

# 组件（太细粒度）
ray_component_cpu_percentage
ray_component_mem_shared_bytes
ray_component_rss_mb
ray_component_uss_mb
ray_component_num_fds
ray_component_gpu_*
```

---

### 3. Ray Core C++ 指标

#### P0 - 必须保留（6个）

| 指标名称 | 描述 |
|---------|------|
| `ray_tasks` | 任务状态 |
| `ray_actors` | Actor 状态 |
| `ray_resources` | 资源状态 |
| `ray_object_store_memory` | 对象存储内存 |
| `ray_object_store_available_memory` | 可用内存 |
| `ray_object_store_used_memory` | 已用内存 |

#### P1 - 建议保留（8个）

| 指标名称 | 描述 |
|---------|------|
| `ray_running_jobs` | 运行中的 Job |
| `ray_finished_jobs` | 完成的 Job |
| `ray_scheduler_tasks` | 调度中的任务 |
| `ray_owned_objects` | 拥有的对象数 |
| `ray_owned_objects_size` | 拥有的对象大小 |
| `ray_total_lineage_bytes` | 血缘信息大小 |
| `ray_gcs_actors_count` | GCS Actor 数量 |
| `ray_placement_groups` | Placement Group 状态 |

#### P3 - 强烈建议过滤（30+个）

```
# 操作级细节
ray_operation_count
ray_operation_run_time_ms
ray_operation_queue_time_ms
ray_operation_active_count

# 调度内部细节
ray_scheduler_placement_time_ms
ray_scheduler_unscheduleable_tasks
ray_scheduler_failed_worker_startup_total
ray_internal_num_spilled_tasks
ray_internal_num_infeasible_scheduling_classes
ray_internal_num_processes_*

# Worker 注册细节
ray_worker_register_time_ms

# Spill Manager 细节
ray_spill_manager_objects
ray_spill_manager_objects_bytes
ray_spill_manager_request_total
ray_spill_manager_throughput_mb

# Pull/Push Manager 细节
ray_pull_manager_*
ray_push_manager_*
ray_object_manager_*

# gRPC 细节
ray_grpc_server_req_*
ray_grpc_client_req_failed

# GCS 内部
ray_gcs_storage_*
ray_gcs_task_manager_*
ray_gcs_placement_group_*
ray_health_check_rpc_latency_ms

# 其他内部指标
ray_io_context_event_loop_lag_ms
ray_memory_manager_worker_eviction
ray_local_resource_view_node_count
```

---

### 4. Ray Serve 指标

#### P0 - 必须保留（6个）

| 指标名称 | 描述 |
|---------|------|
| `ray_serve_deployment_request_counter` | 请求数 |
| `ray_serve_deployment_error_counter` | 错误数 |
| `ray_serve_deployment_processing_latency_ms` | 处理延迟 |
| `ray_serve_num_http_requests` | HTTP 请求数 |
| `ray_serve_num_http_error_requests` | HTTP 错误数 |
| `ray_serve_http_request_latency_ms` | HTTP 延迟 |

#### P1 - 建议保留（4个）

| 指标名称 | 描述 |
|---------|------|
| `ray_serve_replica_processing_queries` | 处理中的查询 |
| `ray_serve_deployment_queued_queries` | 排队的查询 |
| `ray_serve_deployment_replica_healthy` | 副本健康状态 |
| `ray_serve_deployment_status` | 部署状态 |

#### P3 - 强烈建议过滤（8个）

```
ray_serve_deployment_replica_starts
ray_serve_num_ongoing_http_requests
ray_serve_num_deployment_http_error_requests
ray_serve_application_status
ray_serve_replica_startup_latency_ms
ray_serve_replica_initialization_latency_ms
ray_serve_replica_reconfigure_latency_ms
ray_serve_replica_shutdown_duration_ms
ray_serve_record_autoscaling_stats_failed
ray_serve_user_autoscaling_stats_latency_ms
ray_serve_controller_*
ray_serve_num_scheduling_*
```

---

## 四、Ray Data 调度瓶颈排查专用指标

本节针对需要排查 Ray Data 调度瓶颈的场景，列出必须保留的指标。

### 1. 调度循环指标（核心）

| 指标名称 | 描述 | 重要性 |
|---------|------|-------|
| `ray_data_sched_loop_duration_s` | 调度循环耗时 | **必须** - 直接反映调度器性能 |

### 2. 资源预算指标（关键）

| 指标名称 | 描述 | 重要性 |
|---------|------|-------|
| `ray_data_cpu_budget` | CPU 预算/Operator | **必须** - 资源分配情况 |
| `ray_data_gpu_budget` | GPU 预算/Operator | **必须** - GPU 资源分配 |
| `ray_data_memory_budget` | 内存预算/Operator | **建议** - 内存分配 |
| `ray_data_object_store_memory_budget` | 对象存储预算/Operator | **建议** - OSM 分配 |
| `ray_data_max_bytes_to_read` | 流式生成器最大读取字节数 | **建议** - 流控参数 |

### 3. Backpressure 反压指标（关键）

| 指标名称 | 描述 | 重要性 |
|---------|------|-------|
| `ray_data_task_submission_backpressure_time` | 任务提交反压时间 | **必须** - 反映调度瓶颈 |
| `ray_data_task_output_backpressure_time` | 任务输出反压时间 | **必须** - 反映输出瓶颈 |

### 4. 任务状态指标（关键）

| 指标名称 | 描述 | 重要性 |
|---------|------|-------|
| `ray_data_num_tasks_submitted` | 已提交任务数 | **必须** - 调度进度 |
| `ray_data_num_tasks_running` | 运行中任务数 | **必须** - 并行度 |
| `ray_data_num_tasks_finished` | 完成任务数 | **必须** - 完成进度 |
| `ray_data_num_tasks_failed` | 失败任务数 | **必须** - 错误监控 |
| `ray_data_num_tasks_have_outputs` | 有输出的任务数 | **建议** - 产出进度 |

### 5. 队列和内存指标（关键）

| 指标名称 | 描述 | 重要性 |
|---------|------|-------|
| `ray_data_operator_queued_blocks` | Operator 排队 blocks | **必须** - 队列积压 |
| `ray_data_obj_store_mem_used` | 对象存储使用量 | **必须** - 内存压力 |
| `ray_data_obj_store_mem_spilled` | 对象存储溢出量 | **必须** - 溢出情况 |
| `ray_data_obj_store_mem_pending_task_inputs` | 待处理输入大小 | **建议** - 输入积压 |
| `ray_data_obj_store_mem_pending_task_outputs` | 待处理输出大小 | **建议** - 输出积压 |

### 6. 集群资源利用率（可选但有用）

| 指标名称 | 描述 | 重要性 |
|---------|------|-------|
| `ray_data_cluster_cpu_utilization` | 集群 CPU 利用率 | **建议** |
| `ray_data_cluster_gpu_utilization` | 集群 GPU 利用率 | **建议** |
| `ray_data_cluster_object_store_memory_utilization` | 集群 OSM 利用率 | **建议** |

### 7. 调度瓶颈排查推荐配置

**白名单模式**（只保留调度相关指标）：

```bash
RAY_METRICS_REMOTE_WRITE_INCLUDE_METRICS="ray_data_sched_loop_*,ray_data_cpu_budget,ray_data_gpu_budget,ray_data_memory_budget,ray_data_object_store_memory_budget,ray_data_task_submission_backpressure_time,ray_data_task_output_backpressure_time,ray_data_num_tasks_*,ray_data_operator_queued_blocks,ray_data_operator_state,ray_data_obj_store_mem_used,ray_data_obj_store_mem_spilled,ray_data_obj_store_mem_pending_*,ray_data_cluster_*_utilization,ray_data_output_*,ray_data_current_bytes,ray_data_spilled_bytes,ray_data_freed_bytes,ray_data_max_bytes_to_read"
```

---

## 五、Histogram 类型指标分析

Ray Data 中有 **4 个 Histogram** 指标，由于高基数问题，**建议过滤**。

### Histogram 指标列表

| 指标名称 | 描述 | Buckets 数量 | 建议 |
|---------|------|-------------|------|
| `ray_data_task_completion_time` | 任务完成时间分布 | 20 | **可过滤** |
| `ray_data_block_completion_time` | Block 完成时间分布 | 20 | **可过滤** |
| `ray_data_block_size_bytes` | Block 大小分布(字节) | 21 | **可过滤** |
| `ray_data_block_size_rows` | Block 大小分布(行数) | 21 | **可过滤** |

### 为什么 Histogram 可以过滤？

1. **高基数问题**：每个 Histogram 会产生 N+3 个时间序列（N 个 bucket + `_sum` + `_count` + `_bucket{le="+Inf"}`）
2. **存储压力大**：4 个 Histogram × ~23 个时间序列 × 每个 Operator × 每个 Dataset = 大量时间序列
3. **有替代指标**：
   - `ray_data_task_completion_time_total_s` (Gauge) 提供任务完成时间总和
   - `ray_data_task_completion_time_excl_backpressure_s` 提供不含反压的时间
   - 可以通过 `total_time / num_tasks` 计算平均值

### 何时需要保留 Histogram？

- 需要分析**延迟分位数**（P50/P90/P99）时
- 需要了解**分布特征**（是否有长尾）时
- 深度性能调优时

### Histogram Bucket 配置

**时间类 Histogram (秒)**：
```
[0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 7.5, 10.0, 15.0, 20.0, 25.0, 50.0, 75.0, 100.0, 150.0, 500.0, 1000.0, 2500.0, 5000.0]
```

**大小类 Histogram (字节)**：
```
[1KiB, 8KiB, 64KiB, 128KiB, 256KiB, 512KiB, 1MiB, 8MiB, 64MiB, 128MiB, 256MiB, 512MiB, 1GiB, 4GiB, 16GiB, 64GiB, 128GiB, 256GiB, 512GiB, 1TiB, 4TiB]
```

**行数类 Histogram**：
```
[1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 25000, 50000, 100000, 250000, 500000, 1000000, 2500000, 5000000, 10000000]
```

---

## 六、推荐的过滤配置

### 为什么推荐使用 EXCLUDE_METRICS（黑名单）而非 INCLUDE_METRICS（白名单）

| 对比项 | EXCLUDE_METRICS（黑名单） | INCLUDE_METRICS（白名单） |
|-------|--------------------------|--------------------------|
| 用户自定义指标 | ✅ 自动保留 | ❌ 需手动添加，否则被过滤 |
| Ray 新版本新增指标 | ✅ 自动包含 | ❌ 需手动添加 |
| 配置复杂度 | 只需指定不要的 | 需列出所有要保留的 |
| 适用场景 | **推荐大多数场景** | 仅极端存储压力场景 |

**结论**：优先使用 `RAY_METRICS_REMOTE_WRITE_EXCLUDE_METRICS`

---

### 方案 A：生产环境推荐（减少 ~60% 指标）

**推荐用于生产环境**，过滤非必要指标，保留用户自定义指标。

```bash
export RAY_METRICS_REMOTE_WRITE_EXCLUDE_METRICS="ray_data_iter_block_*,ray_data_iter_batch_*,ray_data_iter_initialize_*,ray_data_iter_get_*,ray_data_iter_format_*,ray_data_iter_collate_*,ray_data_iter_finalize_*,ray_data_iter_blocks_*,ray_data_iter_prefetched_*,ray_data_num_inputs_*,ray_data_bytes_inputs_*,ray_data_num_task_inputs_*,ray_data_bytes_task_inputs_*,ray_data_num_task_outputs_*,ray_data_bytes_task_outputs_*,ray_data_rows_task_outputs_*,ray_data_*_outputs_taken,ray_data_*_outputs_of_finished_*,ray_data_num_external_*,ray_data_average_*,ray_data_obj_store_mem_internal_*,ray_data_block_serialization_*,ray_data_block_generation_*,ray_data_task_completion_time,ray_data_block_completion_time,ray_data_block_size_*,ray_component_*,ray_operation_*,ray_internal_*,ray_spill_manager_*,ray_pull_manager_*,ray_push_manager_*,ray_grpc_*,ray_gcs_storage_*,ray_gcs_task_manager_*"
```

**过滤内容**：
- Iterator 内部细节指标
- 输入/输出细粒度指标
- Histogram 类型指标（高基数）
- 组件级指标
- gRPC/GCS 内部指标
- Spill/Pull/Push Manager 细节

**保留内容**：
- 所有用户自定义指标 ✅
- 核心吞吐/任务/资源指标
- 调度和反压指标
- 节点和集群指标

---

### 方案 B：激进过滤（减少 ~70% 指标）

适用于存储压力大的场景。

```bash
export RAY_METRICS_REMOTE_WRITE_EXCLUDE_METRICS="ray_data_iter_block_*,ray_data_iter_batch_*,ray_data_iter_initialize_*,ray_data_iter_get_*,ray_data_iter_next_*,ray_data_iter_format_*,ray_data_iter_collate_*,ray_data_iter_finalize_*,ray_data_iter_blocks_*,ray_data_iter_prefetched_*,ray_data_num_inputs_*,ray_data_bytes_inputs_*,ray_data_num_task_inputs_*,ray_data_bytes_task_inputs_*,ray_data_*_inputs_of_submitted_*,ray_data_num_task_outputs_*,ray_data_bytes_task_outputs_*,ray_data_rows_task_outputs_*,ray_data_*_outputs_taken,ray_data_*_outputs_of_finished_*,ray_data_num_external_*,ray_data_average_*,ray_data_obj_store_mem_internal_*,ray_data_block_*,ray_data_task_completion_time*,ray_node_disk_io_*,ray_component_*,ray_operation_*,ray_scheduler_placement_*,ray_scheduler_unscheduleable_*,ray_scheduler_failed_*,ray_internal_*,ray_worker_register_*,ray_spill_manager_*,ray_pull_manager_*,ray_push_manager_*,ray_object_manager_*,ray_grpc_*,ray_gcs_storage_*,ray_gcs_task_manager_*,ray_gcs_placement_group_*,ray_health_check_*,ray_io_context_*,ray_memory_manager_*,ray_local_resource_*,ray_owned_objects*,ray_total_lineage_bytes,ray_serve_replica_*_latency_ms,ray_serve_*_autoscaling_*,ray_serve_num_ongoing_*,ray_serve_controller_*,ray_serve_num_scheduling_*"
```

---

### 方案 C：仅保留核心指标（白名单模式，减少 ~80%）

⚠️ **注意**：白名单模式会过滤掉用户自定义指标，仅在极端存储压力且无自定义指标时使用。

```bash
export RAY_METRICS_REMOTE_WRITE_INCLUDE_METRICS="ray_data_output_*,ray_data_num_tasks_*,ray_data_sched_loop_*,ray_data_task_submission_backpressure_time,ray_data_task_output_backpressure_time,ray_data_cpu_budget,ray_data_gpu_budget,ray_data_current_bytes,ray_data_spilled_bytes,ray_data_freed_bytes,ray_data_obj_store_mem_used,ray_data_obj_store_mem_spilled,ray_data_operator_queued_blocks,ray_data_operator_state,ray_data_dataset_state,ray_data_cpu_usage_cores,ray_data_gpu_usage_cores,ray_data_iter_total_blocked_seconds,ray_node_cpu_*,ray_node_mem_*,ray_node_gpus_*,ray_node_gram_*,ray_node_disk_utilization_*,ray_cluster_*,ray_tasks,ray_actors,ray_resources,ray_object_store_*,ray_running_jobs,ray_placement_groups"
```

---

### 方案 D：调度瓶颈排查专用

适用于需要排查 Ray Data 调度瓶颈的场景，过滤 Histogram 和非调度相关细节。

```bash
export RAY_METRICS_REMOTE_WRITE_EXCLUDE_METRICS="ray_data_task_completion_time,ray_data_block_completion_time,ray_data_block_size_bytes,ray_data_block_size_rows,ray_data_iter_block_*,ray_data_iter_batch_*,ray_data_iter_initialize_*,ray_data_iter_get_*,ray_data_iter_format_*,ray_data_iter_collate_*,ray_data_iter_finalize_*,ray_data_iter_blocks_*,ray_data_iter_prefetched_*,ray_data_num_inputs_*,ray_data_bytes_inputs_*,ray_data_num_task_inputs_*,ray_data_bytes_task_inputs_*,ray_data_num_task_outputs_*,ray_data_bytes_task_outputs_*,ray_data_rows_task_outputs_*,ray_data_*_outputs_taken,ray_data_*_outputs_of_finished_*,ray_data_num_external_*,ray_data_average_*,ray_data_obj_store_mem_internal_*,ray_data_block_serialization_*,ray_data_block_generation_*"
```

---

## 七、统计汇总

| 模块 | 总指标数 | 建议保留 | 可过滤 | 过滤率 |
|-----|---------|---------|-------|-------|
| Ray Data | ~70 | ~14 | ~56 | 80% |
| Node/Dashboard | ~35 | ~14 | ~21 | 60% |
| Ray Core | ~50 | ~14 | ~36 | 72% |
| Ray Serve | ~18 | ~10 | ~8 || **总计** | **~173** | **~52** | **~121** | **70%** |

### Histogram 指标存储影响

| 指标类型 | 单指标时间序列数 | 说明 |
|---------|----------------|------|
| Gauge/Counter | 1 | 单值 |
| Histogram | ~23 | N buckets + _sum + _count + le="+Inf" |

**示例计算**：
- 4 个 Histogram × 23 时间序列 × 10 Operators × 5 Datasets = **4,600 时间序列**
- 过滤 Histogram 后可显著减少存储压力

---

## 八、默认过滤配置（代码实现）

从本版本开始，Remote Write 模式**默认启用指标过滤**，无需手动配置。

### 默认过滤的指标（44个模式）

```python
DEFAULT_EXCLUDE_PATTERNS = [
    # Ray Data - Iterator internal details
    "ray_data_iter_block_*",
    "ray_data_iter_batch_*",
    "ray_data_iter_initialize_*",
    "ray_data_iter_get_*",
    "ray_data_iter_format_*",
    "ray_data_iter_collate_*",
    "ray_data_iter_finalize_*",
    "ray_data_iter_blocks_*",
    "ray_data_iter_prefetched_*",
    # Ray Data - Fine-grained input/output metrics
    "ray_data_num_inputs_*",
    "ray_data_bytes_inputs_*",
    "ray_data_num_task_inputs_*",
    "ray_data_bytes_task_inputs_*",
    "ray_data_num_task_outputs_*",
    "ray_data_bytes_task_outputs_*",
    "ray_data_rows_task_outputs_*",
    "ray_data_*_outputs_taken",
    "ray_data_*_outputs_of_finished_*",
    "ray_data_num_external_*",
    "ray_data_average_*",
    "ray_data_obj_store_mem_internal_*",
    "ray_data_block_serialization_*",
    "ray_data_block_generation_*",
    # Ray Data - Histogram metrics (high cardinality)
    "ray_data_task_completion_time",
    "ray_data_block_completion_time",
    "ray_data_block_size_*",
    # Node - Component-level details
    "ray_component_*",
    # Ray Core - Internal details
    "ray_operation_*",
    "ray_internal_*",
    "ray_spill_manager_*",
    "ray_pull_manager_*",
    "ray_push_manager_*",
    "ray_grpc_*",
    "ray_gcs_storage_*",
    "ray_gcs_task_manager_*",
]
```

### 配置行为

| 场景 | 行为 |
|-----|------|
| 未设置任何环境变量 | 使用默认过滤（44个模式） |
| 设置 `RAY_METRICS_REMOTE_WRITE_EXCLUDE_METRICS=""` | 禁用过滤，导出所有指标 |
| 设置 `RAY_METRICS_REMOTE_WRITE_EXCLUDE_METRICS="pattern1,..."` | 使用用户指定的过滤规则 |
| 设置 `RAY_METRICS_REMOTE_WRITE_INCLUDE_METRICS="pattern1,..."` | 使用白名单模式 |

### 禁用默认过滤

如需导出所有指标（不推荐），设置空字符串：

```bash
export RAY_METRICS_REMOTE_WRITE_EXCLUDE_METRICS=""
```

### 日志输出示例

```
Metrics export mode: REMOTE_WRITE to http://10.81.0.157:9090/api/v1/write (interval: 60000ms, timeout: 30s, batch_size: 500, exclude: 44 patterns (default))
```

### 默认配置变更

| 配置项 | 默认值 |
|-------|-------|
| `RAY_METRICS_EXPORT_MODE` | `push` (默认启用 Remote Write) |
| `RAY_METRICS_REMOTE_WRITE_ENDPOINT` | `http://10.81.0.157:9090/api/v1/write` |
| `RAY_METRICS_PUSH_INTERVAL_MS` | `60000` (60秒) |
| `RAY_METRICS_REMOTE_WRITE_TIMEOUT` | `30` (秒) |
| `RAY_METRICS_REMOTE_WRITE_BATCH_SIZE` | `500` |
| 默认过滤 | 44 个 EXCLUDE 模式 |

### 环境变量配置

```bash
# 排除模式（黑名单）
export RAY_METRICS_REMOTE_WRITE_EXCLUDE_METRICS="pattern1,pattern2,..."

# 包含模式（白名单）
export RAY_METRICS_REMOTE_WRITE_INCLUDE_METRICS="pattern1,pattern2,..."
```

### 过滤逻辑

1. 如果指定了 `INCLUDE_METRICS`，指标必须匹配至少一个 include 模式才会被导出
2. 如果指定了 `EXCLUDE_METRICS`，匹配任意 exclude 模式的指标将被过滤掉
3. 支持通配符 `*` 进行模式匹配

---

## 九、关键指标文件位置

| 文件路径 | 用途 |
|---------|------|
| `python/ray/_private/telemetry/open_telemetry_metric_recorder.py` | `ray_` 前缀添加、默认过滤配置 |
| `python/ray/data/_internal/stats.py` | Ray Data 指标定义 |
| `python/ray/data/_internal/execution/interfaces/op_runtime_metrics.py` | 运行时指标定义 |
| `python/ray/data/_internal/execution/interfaces/common.py` | Histogram bucket 定义 |
| `python/ray/data/_internal/execution/streaming_executor.py` | 调度循环指标定义 |
| `python/ray/dashboard/modules/reporter/reporter_agent.py` | Node/Dashboard 指标 |
| `src/ray/stats/metric_defs.h` | C++ 层指标定义 |
| `doc/source/ray-observability/reference/system-metrics.rst` | 官方指标文档 |

---

## 十、建议

1. **默认已启用过滤** - Remote Write 模式自动使用默认黑名单，无需手动配置
2. **优先使用 EXCLUDE_METRICS（黑名单）** - 保留用户自定义指标和新版本新增指标
3. **如需自定义过滤**，设置 `RAY_METRICS_REMOTE_WRITE_EXCLUDE_METRICS` 环境变量
4. **如需禁用过滤**，设置 `RAY_METRICS_REMOTE_WRITE_EXCLUDE_METRICS=""`
5. **调度瓶颈排查使用方案 D** - 保留调度相关指标，过滤 Histogram
6. **Histogram 指标建议过滤** - 除非需要分析延迟分位数分布
7. 定期审查指标使用情况，根据实际需求调整过滤配置
