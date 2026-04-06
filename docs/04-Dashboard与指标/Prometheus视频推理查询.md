# Prometheus 查询方案 - Multishot Pipeline 指标

## 背景

本文档提供 Multishot Pipeline 各阶段处理指标的 Prometheus 查询方案，用于监控 segment/slice 处理数量和作业状态。

## 指标概览

### 全部指标列表

| 阶段 | 指标名称 | Prometheus 名称 | 描述 | 标签 |
|------|---------|----------------|------|------|
| 预处理 | `multishot_pipeline_slices` | `ray_multishot_pipeline_slices` | 预处理阶段处理的 slice 总数 | `status` |
| 推理 | `multishot_inference_segments` | `ray_multishot_inference_segments` | 推理阶段处理的 segment 总数 | `status` |
| 推理 | `multishot_inference_merged_segments` | `ray_multishot_inference_merged_segments` | 推理阶段合并后的 segment 总数 | 无 |
| 流式处理 | `multishot_streaming_segments` | `ray_multishot_streaming_segments` | 流式处理的 segment 总数 | `status` |
| 流式处理 | `multishot_streaming_merged_segments` | `ray_multishot_streaming_merged_segments` | 流式处理合并后的 segment 总数 | 无 |
| 分布式推理 | `multishot_distributed_infer_segment_count` | `ray_multishot_distributed_infer_segment_count` | 分布式推理处理的 segment 总数 | `datasetId`, `status` |
| 分布式推理 | `multishot_distributed_infer_merged_segment_count` | `ray_multishot_distributed_infer_merged_segment_count` | 分布式推理合并后的 segment 总数 | `datasetId` |
| 物理合并 | `multishot_merge_output_rows` | `ray_multishot_merge_output_rows` | 物理合并生成的输出行数 | 无 |
| 物理合并 | `multishot_merge_clip_num` | `ray_multishot_merge_clip_num` | 合并视频中的 clip 总数 | 无 |
| 物理合并 | `multishot_merge_fps_diff_skipped` | `ray_multishot_merge_fps_diff_skipped` | 因 fps 差异跳过的 segment 数 | 无 |

**注意**：
- Ray Metrics 会自动添加 `ray_` 前缀（不会添加 `_total` 后缀）
- 不使用 `blobstore_id` 作为标签，避免高基数问题导致内存膨胀

---

## Prometheus Counter 原理说明

Counter 是一个**单调递增**的计数器，从作业启动开始持续累积。理解这点对正确查询很重要：

| 查询类型 | 函数 | 说明 |
|---------|------|------|
| 瞬时值 | 直接查询 | 返回从作业启动到现在的累计总数 |
| 时间段增量 | `increase()` | 返回指定时间范围内的增量 |
| 实时速率 | `rate()` | 返回每秒的平均处理速度 |

---

## Prometheus 查询方案

### 1. 累计总数（从作业启动到现在）

```promql
# ===== 预处理阶段 =====
# 所有 Actor 处理的 slice 累计总数
sum(ray_multishot_pipeline_slices)

# 按 status 分组
sum by (status) (ray_multishot_pipeline_slices)

# ===== 推理阶段 =====
# 所有 Actor 处理的 segment 累计总数
sum(ray_multishot_inference_segments)

# 按 status 分组（processed vs error）
sum by (status) (ray_multishot_inference_segments)

# 只统计成功处理的数量
sum(ray_multishot_inference_segments{status="processed"})

# 合并后的 segment 总数
sum(ray_multishot_inference_merged_segments)

# ===== 流式处理阶段 =====
sum(ray_multishot_streaming_segments{status="processed"})
sum(ray_multishot_streaming_merged_segments)

# ===== 按维度分组 =====
# 按 WorkerId（即 Actor）分组
sum by (WorkerId) (ray_multishot_inference_segments{status="processed"})

# 按节点分组
sum by (NodeAddress) (ray_multishot_inference_segments{status="processed"})
```

### 2. 时间范围内的增量

```promql
# ===== 预处理阶段 =====
# 过去 1 小时内处理的 slice 数量
sum(increase(ray_multishot_pipeline_slices{status="processed"}[1h]))

# ===== 推理阶段 =====
# 过去 1 小时内处理的 segment 数量
sum(increase(ray_multishot_inference_segments{status="processed"}[1h]))

# 过去 30 分钟内处理的数量
sum(increase(ray_multishot_inference_segments{status="processed"}[30m]))

# 过去 24 小时内处理的数量
sum(increase(ray_multishot_inference_segments{status="processed"}[24h]))

# 按节点查看过去 1 小时的处理量
sum by (NodeAddress) (increase(ray_multishot_inference_segments{status="processed"}[1h]))

# 按 Worker 查看过去 1 小时的处理量
sum by (WorkerId) (increase(ray_multishot_inference_segments{status="processed"}[1h]))
```

### 3. 实时处理速率

```promql
# ===== 预处理阶段 =====
# 过去 5 分钟的平均处理速率（每秒）
sum(rate(ray_multishot_pipeline_slices{status="processed"}[5m]))

# ===== 推理阶段 =====
# 过去 5 分钟的平均处理速率（每秒）
sum(rate(ray_multishot_inference_segments{status="processed"}[5m]))

# 过去 1 分钟的瞬时速率（更实时但可能波动）
sum(irate(ray_multishot_inference_segments{status="processed"}[1m]))

# 按节点查看处理速率
sum by (NodeAddress) (rate(ray_multishot_inference_segments{status="processed"}[5m]))

# 按 Worker 查看处理速率
sum by (WorkerId) (rate(ray_multishot_inference_segments{status="processed"}[5m]))
```

### 4. 当前作业的统计（按 Session 过滤）

使用 `SessionName` 标签过滤特定作业：

```promql
# ===== 预处理阶段 =====
# 当前作业的 slice 累计总数
sum(ray_multishot_pipeline_slices{SessionName="session_2026-04-04_15-00-09_703103_1"})

# 当前作业过去 1 小时的 slice 增量
sum(increase(ray_multishot_pipeline_slices{SessionName="session_2026-04-04_15-00-09_703103_1"}[1h]))

# ===== 推理阶段 =====
# 当前作业的 segment 累计总数
sum(ray_multishot_inference_segments{SessionName="session_2026-04-04_15-00-09_703103_1"})

# 当前作业过去 1 小时的增量
sum(increase(ray_multishot_inference_segments{SessionName="session_2026-04-04_15-00-09_703103_1"}[1h]))

# 当前作业的实时速率
sum(rate(ray_multishot_inference_segments{SessionName="session_2026-04-04_15-00-09_703103_1", status="processed"}[5m]))

# 当前作业按 Worker 分组
sum by (WorkerId) (
  ray_multishot_inference_segments{SessionName="session_2026-04-04_15-00-09_703103_1", status="processed"}
)

# ===== 流式处理阶段 =====
sum(ray_multishot_streaming_segments{SessionName="session_2026-04-04_15-00-09_703103_1", status="processed"})
```

### 5. 错误统计

```promql
# ===== 预处理阶段错误 =====
sum(ray_multishot_pipeline_slices{status="error"})

# ===== 推理阶段错误 =====
# 错误总数
sum(ray_multishot_inference_segments{status="error"})

# 过去 1 小时的错误数
sum(increase(ray_multishot_inference_segments{status="error"}[1h]))

# 错误率百分比（累计）
sum(ray_multishot_inference_segments{status="error"})
  / sum(ray_multishot_inference_segments) * 100

# 实时错误率（过去 5 分钟）
sum(rate(ray_multishot_inference_segments{status="error"}[5m]))
  / sum(rate(ray_multishot_inference_segments[5m])) * 100

# ===== 流式处理阶段错误 =====
sum(ray_multishot_streaming_segments{status="error"})
```

### 6. 全流程汇总查询

```promql
# 各阶段处理总量对比
sum(ray_multishot_pipeline_slices{status="processed"})        # 预处理
sum(ray_multishot_inference_segments{status="processed"})     # 推理
sum(ray_multishot_inference_merged_segments)                  # 推理合并
sum(ray_multishot_streaming_segments{status="processed"})     # 流式处理
sum(ray_multishot_merge_output_rows)                          # 物理合并输出行数
sum(ray_multishot_merge_clip_num)                             # 物理合并 clip 总数

# 各阶段实时速率对比
sum(rate(ray_multishot_pipeline_slices{status="processed"}[5m]))
sum(rate(ray_multishot_inference_segments{status="processed"}[5m]))
sum(rate(ray_multishot_streaming_segments{status="processed"}[5m]))
sum(rate(ray_multishot_merge_output_rows[5m]))
```

### 7. 物理合并阶段查询

```promql
# ===== 输出行数统计 =====
# 累计输出行数
sum(ray_multishot_merge_output_rows)

# 过去 1 小时输出行数
sum(increase(ray_multishot_merge_output_rows[1h]))

# 输出速率（每秒）
sum(rate(ray_multishot_merge_output_rows[5m]))

# ===== clip 数量统计 =====
# 累计 clip 数量
sum(ray_multishot_merge_clip_num)

# 过去 1 小时 clip 数量
sum(increase(ray_multishot_merge_clip_num[1h]))

# ===== fps 差异跳过统计 =====
# 累计因 fps 差异跳过的 segment 数
sum(ray_multishot_merge_fps_diff_skipped)

# 过去 1 小时跳过数量
sum(increase(ray_multishot_merge_fps_diff_skipped[1h]))

# fps 跳过率（相对于推理合并 segment）
sum(ray_multishot_merge_fps_diff_skipped)
  / sum(ray_multishot_inference_merged_segments) * 100

# ===== 按 Session 过滤 =====
sum(ray_multishot_merge_output_rows{SessionName="session_2026-04-04_15-00-09_703103_1"})
sum(ray_multishot_merge_clip_num{SessionName="session_2026-04-04_15-00-09_703103_1"})
sum(ray_multishot_merge_fps_diff_skipped{SessionName="session_2026-04-04_15-00-09_703103_1"})
```

---

## 可用标签说明

| 标签 | 说明 | 用途 |
|------|------|------|
| `SessionName` | Ray Session 名称 | 区分不同作业 |
| `WorkerId` | Worker ID | 区分不同 Actor |
| `NodeAddress` | 节点 IP | 按节点聚合 |
| `status` | 处理状态 | `processed` / `error` |
| `datasetId` | 数据集 ID | 分布式推理指标专用，区分不同数据集 |
| `Component` | 组件名 | 通常为 `core_worker` |

---

## 常用时间范围

| 范围 | 语法 | 说明 |
|------|------|------|
| 1 分钟 | `[1m]` | 最小粒度，波动较大 |
| 5 分钟 | `[5m]` | 推荐用于 rate() |
| 30 分钟 | `[30m]` | 中等粒度 |
| 1 小时 | `[1h]` | 常用 |
| 24 小时 | `[24h]` | 日统计 |

---

## 代码位置

| 文件 | 指标 |
|------|------|
| `pipeline/multi_video_classifier_merge/mappers/video_preprocess_mapper.py` | `multishot_pipeline_slices` |
| `pipeline/multi_video_classifier_merge/mappers/video_inference_mapper.py` | `multishot_inference_segments`, `multishot_inference_merged_segments` |
| `pipeline/multi_video_classifier_merge/mappers/streaming_video_process_mapper.py` | `multishot_streaming_segments`, `multishot_streaming_merged_segments` |
| `pipeline/multi_video_classifier_merge/mappers/clip_merge_mapper.py` | `multishot_merge_output_rows`, `multishot_merge_clip_num`, `multishot_merge_fps_diff_skipped` |
| `pipeline/multi_video_classifier_merge/mappers/distributed_streaming_video_process_mapper.py` | `multishot_distributed_infer_segment_count`, `multishot_distributed_infer_merged_segment_count` |

---

## 分布式推理指标查询

### 指标说明

分布式推理 (`DistributedStreamingVideoProcessMapper`) 使用以下指标：

| 指标名称 | Prometheus 名称 | 描述 | 标签 |
|---------|----------------|------|------|
| `multishot_distributed_infer_segment_count` | `ray_multishot_distributed_infer_segment_count` | 处理的 segment 总数 | `datasetId`, `status` |
| `multishot_distributed_infer_merged_segment_count` | `ray_multishot_distributed_infer_merged_segment_count` | 合并后的 segment 总数 | `datasetId` |

### rate vs irate 区别

| 特性 | `rate()` | `irate()` |
|------|----------|-----------|
| 计算方式 | 时间窗口内所有点的线性回归斜率 | 仅用最后两个数据点计算瞬时速率 |
| 平滑度 | 平滑，抗噪声 | 敏感，波动大 |
| 适用场景 | 告警、长期趋势、仪表盘 | 实时调试、短期波动观察 |
| 推荐窗口 | `[1m]` 以上 | `[1m]` 或 `[30s]` |

**示例**：假设 scrape 间隔 15s，窗口 `[1m]` 内有 4 个数据点

```
时间:   t0    t1    t2    t3
值:    100   105   108   120

rate:  基于 4 个点拟合斜率 ≈ (120-100) / 60s ≈ 0.33/s
irate: 仅用 t2→t3 计算 = (120-108) / 15s = 0.8/s
```

### 计算顺序说明

`sum(rate(...))` 的计算顺序：

```
1. Prometheus 先识别所有匹配的时间序列
   例如:
   - ray_multishot_distributed_infer_merged_segment_count{datasetId="ds1", instance="node1"}
   - ray_multishot_distributed_infer_merged_segment_count{datasetId="ds1", instance="node2"}
   - ray_multishot_distributed_infer_merged_segment_count{datasetId="ds2", instance="node1"}

2. 对每条时间序列独立计算 rate
   - rate(ds1, node1) = 10/s
   - rate(ds1, node2) = 15/s
   - rate(ds2, node1) = 8/s

3. sum 聚合所有结果
   - sum = 10 + 15 + 8 = 33/s
```

### 1. 所有节点总的处理速率

```promql
# 使用 rate（平滑，推荐用于仪表盘和告警）
sum(rate(ray_multishot_distributed_infer_merged_segment_count[1m]))

# 使用 irate（更即时，适合实时调试）
sum(irate(ray_multishot_distributed_infer_merged_segment_count[1m]))
```

### 2. 按作业（job）过滤的处理速率

```promql
sum(rate(ray_multishot_distributed_infer_merged_segment_count{job="ray"}[1m]))
```

> 注意：`job` 标签的值取决于 Prometheus scrape 配置，常见值有 `ray`、`ray-head`、`ray-worker` 等。

### 3. 按 datasetId 分组查看各数据集的处理速率

```promql
sum by (datasetId) (rate(ray_multishot_distributed_infer_merged_segment_count[1m]))
```

### 4. 查看特定 datasetId 的处理速率

```promql
sum(rate(ray_multishot_distributed_infer_merged_segment_count{datasetId="your_dataset_id"}[1m]))
```

### 5. 查看特定 datasetId 在各节点的分布

```promql
sum by (instance) (rate(ray_multishot_distributed_infer_merged_segment_count{datasetId="your_dataset_id"}[1m]))
```

### 6. 在 Grafana 中使用变量

如果配置了 `$datasetId` 变量：

```promql
sum(rate(ray_multishot_distributed_infer_merged_segment_count{datasetId="$datasetId"}[1m]))
```

**获取可用的 datasetId 值**（用于配置 Grafana 变量）：

```promql
group by (datasetId) (ray_multishot_distributed_infer_merged_segment_count)
```

### 7. 分布式推理的 segment 处理统计

```promql
# ===== 累计总数 =====
# 所有成功处理的 segment 总数
sum(ray_multishot_distributed_infer_segment_count{status="processed"})

# 按 datasetId 分组
sum by (datasetId) (ray_multishot_distributed_infer_segment_count{status="processed"})

# ===== 处理速率 =====
# 所有节点总速率
sum(rate(ray_multishot_distributed_infer_segment_count{status="processed"}[5m]))

# 按 datasetId 分组
sum by (datasetId) (rate(ray_multishot_distributed_infer_segment_count{status="processed"}[5m]))

# ===== 错误统计 =====
# 错误总数
sum(ray_multishot_distributed_infer_segment_count{status="error"})

# 错误率百分比
sum(ray_multishot_distributed_infer_segment_count{status="error"})
  / sum(ray_multishot_distributed_infer_segment_count) * 100
```

### 8. 实践建议

| 场景 | 推荐 |
|------|------|
| Grafana 仪表盘 | `rate(...[1m])` 或 `rate(...[5m])` |
| 告警规则 | `rate(...[5m])` — 更稳定，减少误报 |
| 实时调试 | `irate(...[1m])` — 看即时变化 |
| 高精度短窗口 | 确保窗口 >= 4 × scrape_interval |
