# Streaming Executor Batch Metadata Optimization

## 问题背景

在 `process_completed_tasks()` 函数中，原始实现对每个完成的任务调用 `task.on_data_ready()`，该方法内部会调用 `ray.get(meta_ref, timeout=1.0)` 来获取 metadata。

当有多个任务同时完成但 metadata 尚未就绪时，会产生**串行阻塞**：

```
process_completed_tasks()
    │
    ▼
for task in ready_tasks:        ← 可能有几十上百个任务
    task.on_data_ready()
        │
        ▼
    ray.get(meta_ref, timeout=1.0)  ← 串行阻塞！
```

**15 个任务超时 = 15 秒延迟**

## 优化方案

### 方案 2: 批量获取 Metadata

将串行的 `ray.get()` 改为批量 `ray.wait()` + `ray.get()`：

```
原始 (串行):
task1.ray.get(1s) → task2.ray.get(1s) → task3.ray.get(1s) → ... = N 秒

优化 (批量):
ray.wait([meta1, meta2, meta3...], timeout=0.1) → ray.get(ready_metas) = 0.1 秒
```

## 代码改动

### 文件 1: `physical_operator.py`

在 `DataOpTask` 类中新增 3 个方法：

| 方法 | 功能 |
|------|------|
| `prepare_metadata()` | 准备 block_ref 和 meta_ref，不阻塞等待。返回 True 表示 meta_ref 已就绪可用于批量等待 |
| `get_pending_meta_ref()` | 获取待处理的 metadata 引用 |
| `complete_with_metadata(meta_with_schema)` | 使用已获取的 metadata 完成数据处理，返回处理的字节数 |

### 文件 2: `streaming_executor_state.py`

重构 `process_completed_tasks()` 函数：

1. **收集阶段**: 遍历所有就绪任务，调用 `prepare_metadata()` 收集所有待获取的 meta_refs
2. **批量等待**: 使用单次 `ray.wait()` 批量等待所有 meta_refs，超时 0.1 秒
3. **处理就绪数据**: 只处理已就绪的 metadata
4. **保留限制逻辑**: 保持对 `max_bytes_to_read` 的限制检查

## 代码审查发现的问题

### 问题 1: 单次只处理一个 block

| 项目 | 说明 |
|------|------|
| **原代码行为** | `on_data_ready()` 使用 `while` 循环，一次调用可以从一个 task 读取多个 blocks |
| **新代码行为** | `prepare_metadata()` + `complete_with_metadata()` 每次只处理一个 block |
| **影响** | 如果一个 task 产生多个 blocks，需要多次调度循环才能全部处理完 |
| **状态** | ⚠️ 已记录，可接受 |

**为什么可以接受**:
1. 调度循环运行频繁
2. 核心瓶颈（串行 1s 超时）已消除
3. 代码中已添加注释说明

### 问题 2: 顺序性保持

| 项目 | 说明 |
|------|------|
| **原问题** | 之前的实现用 `tasks_by_state = defaultdict(list)` 重新分组，可能打乱原来按 `task_index` 排序的顺序 |
| **修复** | 直接在原始的 `pending_meta_tasks` 列表上迭代，保持按 state 分组、按 task_index 排序的原始顺序 |
| **状态** | ✅ 已修复 |

### 问题 3: 跳过任务的状态保持

| 项目 | 说明 |
|------|------|
| **场景** | 任务因 metadata 未就绪或达到 `max_bytes_to_read` 限制被跳过 |
| **行为** | 任务的 `_pending_block_ref` 和 `_pending_meta_ref` 保持不变 |
| **结果** | 下次调度循环时 `prepare_metadata()` 会直接返回 `True`，无需重新获取 |
| **状态** | ✅ 确认正确 |

## 性能优化效果

| 场景 | 优化前 | 优化后 |
|------|--------|--------|
| 15 个任务超时 | 15 × 1s = 15s | 1 × 0.1s = 0.1s |
| N 个任务处理 | N × 1s (最坏情况) | 0.1s (批量) |

## 关键设计决策

1. **保持兼容性**: 原有的 `on_data_ready()` 方法保持不变，可作为降级方案
2. **顺序性保证**: 任务按 `task_index` 排序处理，保持 `preserve_order` 语义
3. **错误处理**: 保持与原来一致的错误处理逻辑
4. **资源限制**: 保持 `max_bytes_to_read` 限制的正确执行

## 验证方法

### 单元测试

```bash
pytest python/ray/data/tests/test_streaming_executor.py -v
pytest python/ray/data/tests/test_operators.py -v
```

### 集成测试

```python
import ray
from ray.data import read_parquet

ray.init()

# 大规模数据集测试
ds = read_parquet("s3://your-bucket/large-dataset/")
result = ds.map_batches(lambda x: x).take(1000)

# 检查 metrics
print(ray.data.get_last_execution_info().scheduling_loop_duration)
# 期望: < 0.5s (原来可能是 15s)
```

## 风险与回滚

### 潜在风险

1. **多 block 任务处理**: 每个任务每轮只处理一个 block，可能增加调度循环次数
2. **顺序性验证**: 需要验证 `preserve_order=True` 场景

### 回滚方案

如果出现问题，恢复原代码即可：

```bash
git checkout HEAD -- python/ray/data/_internal/execution/interfaces/physical_operator.py
git checkout HEAD -- python/ray/data/_internal/execution/streaming_executor_state.py
```
