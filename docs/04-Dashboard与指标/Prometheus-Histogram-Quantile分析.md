# Prometheus histogram_quantile 与 rate 原理分析

## 1. PromQL 查询示例

```
histogram_quantile(0.99, rate(ray_health_check_rpc_latency_ms_bucket{SessionName="session_2026-05-07_20-20-14_589237_8"}[5m]))
```

### 逐层拆解

| 部分 | 含义 |
|------|------|
| `ray_health_check_rpc_latency_ms_bucket` | Ray 健康检查 RPC 延迟的 **Histogram bucket** 指标，单位 ms，每个 bucket 记录 ≤ 该上界的观测次数 |
| `{SessionName="session_2026-05-07_20-20-14_589237_8"}` | 过滤特定 Ray Session |
| `rate(...[5m])` | 计算每个 bucket 的 **每秒增长速率**（5分钟窗口），将累积计数转为瞬时速率 |
| `histogram_quantile(0.99, ...)` | 基于 bucket 分布做 **分位数估算**，计算 P99 延迟 |

### 查询结果含义

**P99 健康检查 RPC 延迟**，即过去 5 分钟内 99% 的健康检查 RPC 请求的延迟低于该值，单位为 **ms**。

### 注意事项

1. **`histogram_quantile` 要求 `le` 标签**：底层指标必须包含 `le` bucket 标签才能工作，这是 Histogram 的标准规范。
2. **结果精度依赖 bucket 粒度**：`histogram_quantile` 通过线性插值估算分位数，bucket 边界越密越准确。如果真实 P99 落在两个 bucket 之间，结果会有偏差。
3. **`rate` 后会丢失 `le` 以外的标签**：`histogram_quantile` 会自动按 `le` 分组计算，这是正确行为。
4. **空窗口/无数据**：如果该 Session 最近 5 分钟没有健康检查请求，`rate` 返回空，查询无结果。

---

## 2. histogram_quantile 计算原理

### 核心思路：线性插值

假设 Histogram 有以下 bucket（`le` = less than or equal）：

```
le=10   count=5
le=50   count=30
le=100  count=80
le=500  count=95
le=+Inf count=100
```

经过 `rate()` 后得到的是每个 bucket 的**每秒增长速率**，算法逻辑相同，下面用累积计数说明。

### 计算步骤

**1. 确定目标排名**

```
目标排名 = quantile × 总数 = 0.99 × 100 = 99
```

即找第 99 个请求落在哪个 bucket。

**2. 找到目标 bucket**

从低到高遍历，找到第一个累积计数 ≥ 99 的 bucket：

```
le=10   → 5    < 99  ✗
le=50   → 30   < 99  ✗
le=100  → 80   < 99  ✗
le=500  → 95   < 99  ✗
le=+Inf → 100  ≥ 99  ✓  ← 目标 bucket
```

但 `+Inf` 无法插值，所以实际用的是**最后一个有限 bucket 与目标 bucket 之间的区间**：

```
前一个 bucket: le=500, count=95
目标 bucket:   le=+Inf, count=100
```

这个例子中 P99 落在 `500 ~ +Inf` 区间，结果不可估算，会返回 `le=500` 作为下界。

### 更好的例子

```
le=10   count=10
le=50   count=60
le=100  count=90
le=500  count=100
le=+Inf count=100
```

目标排名 = 0.99 × 100 = **99**

```
le=10   → 10  < 99 ✗
le=50   → 60  < 99 ✗
le=100  → 90  < 99 ✗
le=500  → 100 ≥ 99 ✓
```

**3. 在 bucket 内线性插值**

目标落在 `le=100` 和 `le=500` 之间：

```
区间内的观测数 = 100 - 90 = 10
区间内需要跨过的位置 = 99 - 90 = 9
插值比例 = 9 / 10 = 0.9

P99 = 100 + 0.9 × (500 - 100) = 100 + 360 = 460 ms
```

### 公式总结

```
                rank - count(prev_bucket)
result = le(prev) + ───────────────────────── × (le(bucket) - le(prev))
                  count(bucket) - count(prev_bucket)
```

### 关键局限

| 问题 | 说明 |
|------|------|
| **线性插值假设均匀分布** | bucket 内实际分布可能不均匀，P99 值会有偏差 |
| **bucket 粒度决定精度** | bucket 越密越准确；若 P99 落在很宽的 bucket 内（如 100~500），误差大 |
| **P99 落在最后一个有限 bucket 之外** | 只能返回该 bucket 上界，严重低估 |
| **原生 Histogram（Native Histogram）** | Prometheus 2.40+ 支持指数桶，精度更高，无需预定义 bucket 边界 |

---

## 3. rate() 的作用

Histogram 的 `_bucket` 指标是**累积计数器（Counter）**，值只增不减（进程重启才归零）。直接用原始值做 `histogram_quantile` 没有意义，因为：

### 问题：原始值是全量累积

```
le=10   → 10000
le=50   → 50000
le=100  → 80000
le=500  → 95000
le=+Inf → 100000
```

这反映的是**进程启动以来的全部历史**，不是当前状态。

### rate(...[5m]) 做了两件事

**1. 将累积计数转为瞬时速率**

```
rate 值 = (当前值 - 5分钟前的值) / 300秒
```

得到每个 bucket 的**每秒增长速率**，反映的是**最近 5 分钟**的流量分布，而非历史累积。

**2. 自动处理 Counter 重置**

进程重启时 Counter 归零再增长，`rate()` 能检测到并正确计算差值。

### 对 histogram_quantile 的影响

`histogram_quantile` 只关心各 bucket 之间的**比例关系**，`rate()` 的除以 300 对所有 bucket 是等价的，不影响分位数计算结果，但把时间窗口限制在了**最近 5 分钟**。

### 不用 rate() 的替代方案

| 方案 | 效果 |
|------|------|
| `histogram_quantile(0.99, metric_bucket)` | 反映进程启动以来的全量 P99，近期变化被稀释 |
| `histogram_quantile(0.99, increase(metric_bucket[5m]))` | 与 `rate` 类似，但不除以时间，结果含义是"5 分钟内的 P99"，等价 |
| `histogram_quantile(0.99, rate(metric_bucket[5m]))` | **最常用**，反映近期 P99，数值单位不变（ms） |

简单说：**`rate()` 的核心作用是把"全量累积"变成"近期增量"，让 P99 反映的是当前延迟而非历史均值。**
