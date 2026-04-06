# Ray Data 作业内存问题排查指南

本文档总结了 Ray Data 作业中常见的内存问题及排查方法，基于 `CPUPreprocessActor` 的实际 OOM 问题排查经验。

## 目录

- [常见 OOM 错误信息解读](#常见-oom-错误信息解读)
- [内存问题根因分析](#内存问题根因分析)
- [Python/Ray 内存管理机制](#pythonray-内存管理机制)
- [内存追踪工具使用](#内存追踪工具使用)
- [优化方案](#优化方案)

---

## 常见 OOM 错误信息解读

### 典型错误日志

```
(raylet) Task _map_task failed due to oom. There are infinite oom retries remaining.
Memory on the node (IP: 10.57.23.12) was 121.66GB / 128.00GB (0.950433),
which exceeds the memory usage threshold of 0.95.

Top 10 memory users:
PID     MEM(GB)  COMMAND
1167239 11.47    ray::CPUPreprocessActor.preprocess_batch
1167531 7.37     ray::CPUPreprocessActor.preprocess_batch
1165800 6.45     ray::CPUPreprocessActor
```

### 关键信息解读

| 字段 | 含义 |
|------|------|
| `memory usage threshold of 0.95` | 节点内存使用超过 95% 触发 OOM |
| `ray::CPUPreprocessActor.preprocess_batch` | Actor 正在执行 `preprocess_batch` 方法 |
| `ray::CPUPreprocessActor` | Actor 空闲/等待任务 或正在执行 `__init__` |

**注意**：同一个 Actor 进程在不同时刻会显示不同名称，这是 Ray 的动态监控机制，反映当前执行状态。

---

## 内存问题根因分析

### 1. 并发线程过多

```python
# 问题配置
--streaming-actor-num-workers 32  # 32 个线程同时处理视频
--streaming-batch-size 32         # 每批 32 个 segment
```

**内存占用估算**（假设 1080p 视频，8 帧）：

| 资源 | 单线程 | 32 线程并发 |
|------|--------|-------------|
| VideoDecoder | ~30MB | 960MB |
| video_frames (8帧) | ~200MB | **6.4GB** |
| processed_data | ~2.4MB | 77MB |
| **总计** | ~230MB | **~7.5GB** |

### 2. 内存未及时释放

```python
def _preprocess_single(self, ...):
    vr = VideoDecoder(...)
    video_frames = vr.get_frames_at(...)
    processed = self.processor(video_frames, ...)
    # 问题：vr, video_frames, processed 同时存在于内存
    return processed_data
```

### 3. 批量结果累积

```python
def preprocess_batch(self, slice_info_list):
    results = {}
    with ThreadPoolExecutor(max_workers=32) as executor:
        for future in as_completed(futures):
            results[segment_idx] = (idx, processed_data, ...)
            # 32 个 processed_data 全部累积在 results 中

    gc.collect()  # 此时 results 还持有引用，GC 无法释放
    return sorted_results
```

### 4. Python 内存不归还 OS

```
申请内存: Python → glibc malloc → OS
释放内存: Python del → glibc free → 内存池（不一定归还 OS）
```

**glibc malloc 默认行为**：
- 小块内存（< 128KB）：free 后保留在进程内存池
- 大块内存（>= 128KB）：通过 mmap 分配，free 后可能归还 OS

---

## Python/Ray 内存管理机制

### gc.collect() 的作用

Python 有两层垃圾回收机制：

| 机制 | 触发方式 | 回收对象 |
|------|----------|----------|
| **引用计数** | 自动、实时 | 引用计数为 0 的对象 |
| **分代 GC** | 自动或手动 | 循环引用的对象 |

```python
# 引用计数（主要机制）
x = torch.randn(1000, 1000)  # 引用计数 = 1
del x                         # 引用计数 = 0 → 立即释放

# gc.collect() 用于处理循环引用
a.ref = b
b.ref = a  # 循环引用
del a, b   # 引用计数不为 0
gc.collect()  # 检测并回收循环引用
```

### gc.collect() 无效的情况

```python
def preprocess_batch(self, ...):
    results = {}
    for future in futures:
        results[idx] = processed_data

    gc.collect()  # 无效！results 还持有 processed_data 的引用
    return results  # 数据仍在内存中
```

### 强制归还内存给 OS

```python
import ctypes

def malloc_trim():
    """强制将空闲内存归还给操作系统 (仅 Linux)"""
    try:
        libc = ctypes.CDLL("libc.so.6")
        libc.malloc_trim(0)
    except Exception:
        pass
```

---

## 内存追踪工具使用

### 1. 启用内存追踪日志

`CPUPreprocessActor` 支持 `enable_memory_trace` 参数：

```python
# 调用方启用追踪
future = actor.preprocess_batch.remote(b_items, enable_memory_trace=True)
```

### 2. 追踪日志示例

```
[MEM-TRACE] preprocess_batch START: batch_size=32, num_workers=32, memory=2.50GB
[MEM-TRACE] segment_0 start: 2.50GB
[MEM-TRACE] segment_0 after_download: 2.51GB
[MEM-TRACE] segment_0 video_frames: shape=(8, 1080, 1920, 3), dtype=float32, size=190.1MB
[MEM-TRACE] segment_0 after_decode: 2.70GB
[MEM-TRACE] segment_0 after_del_vr: 2.68GB
[MEM-TRACE] segment_0 after_processor: 2.75GB
[MEM-TRACE] segment_0 after_del_video_frames: 2.56GB
[MEM-TRACE] segment_0 processed_data[pixel_values]: shape=(1, 8, 3, 224, 224), dtype=bfloat16, size=2.4MB
[MEM-TRACE] segment_0 after_gc: 2.55GB
[MEM-TRACE] preprocess_batch AFTER_EXECUTOR: memory=15.30GB
[MEM-TRACE] preprocess_batch AFTER_GC: memory=14.80GB
[MEM-TRACE] preprocess_batch AFTER_MALLOC_TRIM: memory=5.20GB, released=9.60GB
```

### 3. 检查 Actor 内存状态

```python
# 远程获取 Actor 内存信息
for i, actor in enumerate(actors):
    info = ray.get(actor.get_memory_info.remote())
    print(f"Actor-{i}: PID={info['pid']}, memory={info['memory_gb']:.2f}GB")
```

返回信息：

```python
{
    "actor_id": 0,
    "pid": 12345,
    "memory_gb": 5.2,
    "memory_released_gb": 9.6,  # malloc_trim 释放的内存
    "gc_objects": 150000,
    "num_workers": 32,
}
```

### 4. 本地测试脚本

```python
import gc
import ctypes
import psutil
import os

def get_memory_mb():
    return psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024

print(f"Before: {get_memory_mb():.1f} MB")

# 分配大量内存
data = [bytearray(100 * 1024 * 1024) for _ in range(10)]  # 1GB
print(f"After alloc: {get_memory_mb():.1f} MB")

# 释放
del data
gc.collect()
print(f"After gc: {get_memory_mb():.1f} MB")  # 可能仍然很高

# 强制归还给 OS
libc = ctypes.CDLL("libc.so.6")
libc.malloc_trim(0)
print(f"After malloc_trim: {get_memory_mb():.1f} MB")  # 应该下降
```

---

## 优化方案

### 1. 减少并发线程数（最直接）

```bash
# 原来
--streaming-actor-num-workers 32

# 优化后
--streaming-actor-num-workers 8
```

### 2. 及时释放中间变量

```python
def _preprocess_single(self, ...):
    vr = VideoDecoder(local_file_path)
    video_frames = vr.get_frames_at(indices=sampled_indices).data

    # 解码完成后立即释放 decoder
    del vr

    processed = self.processor(video_frames, return_tensors="pt")

    # 预处理完成后立即释放原始帧
    del video_frames

    processed_data = {k: v.to(torch.bfloat16) for k, v in processed.items()}

    # 释放原始 processed 对象
    del processed

    gc.collect()
    return processed_data
```

### 3. 调用 malloc_trim 归还内存

```python
def preprocess_batch(self, ...):
    # ... 处理逻辑 ...

    gc.collect()
    malloc_trim()  # 强制归还空闲内存给 OS

    return sorted_results
```

### 4. 调整 Ray Actor 内存配置

```python
# 调大内存声明，让 Ray 少调度 Actor
DEFAULT_CPU_ACTOR_MEMORY = 12 * 1024 * 1024 * 1024  # 12GB

@ray.remote(num_cpus=2, memory=DEFAULT_CPU_ACTOR_MEMORY)
class CPUPreprocessActor:
    ...
```

### 5. 控制 Actor 总数

```python
# 在调用方控制并发数
ds = ds.map_batches(
    CPUPreprocessActor,
    concurrency=4,  # 限制最多 4 个 Actor
)
```

### 6. 使用 ray.put() 转移数据到 Object Store

```python
def _preprocess_single(self, ...):
    ...
    # 将大 tensor 放入 Object Store，Actor 进程不持有数据
    processed_ref = ray.put(processed_data)
    del processed_data
    gc.collect()

    return segment_idx, processed_ref, None, timing_info
```

---

## 配置参数参考

| 参数 | 默认值 | 说明 | 优化建议 |
|------|--------|------|----------|
| `batch_size` | 16 | 每批处理的 segment 数量 | 减小到 8 |
| `actor_num_workers` | 8 | Actor 内部并发线程数 | 减小到 4 |
| `cpu_actor_pool_size` | 4 | CPU Actor 数量 | 根据节点内存调整 |
| `cpu_actor_memory` | 4GB | 每个 Actor 声明的内存 | 调大到 12GB |

---

## 相关文件

- `pipeline/multi_video_classifier_merge/mappers/cpu_preprocess_actor.py` - CPUPreprocessActor 实现
- `pipeline/multi_video_classifier_merge/mappers/distributed_streaming_video_process_mapper.py` - 分布式流式处理 Mapper
- `pipeline/multi_video_classifier_merge/constants.py` - 常量配置
