# QwenVL Shared GPU 场景指南

本文档整理了 QwenVL streaming pipeline 在 shared GPU 场景下的问题分析、参数配置和最佳实践。

## 目录

1. [背景](#背景)
2. [参数透传链路](#参数透传链路)
3. [Shared GPU 显存问题分析](#shared-gpu-显存问题分析)
4. [vLLM 参数关系](#vllm-参数关系)
5. [Ray Data 重试机制](#ray-data-重试机制)
6. [孤儿进程清理](#孤儿进程清理)
7. [最佳实践](#最佳实践)

---

## 背景

### 场景描述f

QwenVL streaming pipeline 使用 `DistributedQwenVLVideoProcessMapper` 进行视频多模态推理。在生产环境中，为了提高 GPU 利用率，通常会配置 `streaming_num_gpus=0.5`，允许 Ray 将多个 worker 调度到同一张 GPU 上。

### 核心挑战

每个 worker 内部都会拉起完整的 vLLM engine，即使配置了 `gpu_memory_utilization=0.4`，仍可能因以下原因导致初始化失败：

- 权重各自加载的瞬时峰值
- CUDA driver / NCCL 显存回收延迟
- KV cache 预算计算与实际可用显存的边界竞争

---

## 参数透传链路

### 完整链路

```
CLI (multishot_video_classifier_pipeline_checkpoint.py)
    ↓
build_pipeline(...) (pipeline_builder.py)
    ↓
QwenVLStreamingConfig.to_mapper_config(...) (qwenvl_config.py)
    ↓
DistributedQwenVLVideoProcessMapper.__init__() + setup()
    ↓
vLLM LLM(...)
```

### 支持的 CLI 参数

| CLI 参数 | 配置字段 | vLLM 参数 | 默认值 |
|---------|---------|----------|-------|
| `--qwenvl-gpu-memory-utilization` | `gpu_memory_utilization` | `gpu_memory_utilization` | 0.4 |
| `--qwenvl-max-num-batched-tokens` | `max_num_batched_tokens` | `max_num_batched_tokens` | 65536 |
| `--qwenvl-max-model-len` | `max_model_len` | `max_model_len` | 2048 |
| `--qwenvl-max-num-seqs` | `max_num_seqs` | `max_num_seqs` | 32 |

### 示例

```bash
python -m pipeline.multi_video_classifier_merge.multishot_video_classifier_pipeline_checkpoint \
    --streaming-mode qwenvl \
    --qwenvl-model-dir /path/to/model \
    --streaming-num-gpus 0.5 \
    --qwenvl-gpu-memory-utilization 0.35 \
    --qwenvl-max-num-batched-tokens 32768 \
    --qwenvl-max-num-seqs 16
```

---

## Shared GPU 显存问题分析

### 典型报错

```
ValueError: No available memory for the cache blocks.
Try increasing `gpu_memory_utilization` when initializing the engine.
```

### 为什么 0.4 + 0.4 仍然可能失败？

`gpu_memory_utilization` 只控制 **KV cache** 可以使用的显存比例，不包括：

1. **模型权重**：每个 engine 独立加载完整权重（不共享）
2. **激活值**：推理过程中的临时张量
3. **CUDA context**：每个进程的 CUDA runtime 开销
4. **初始化峰值**：加载权重时的临时显存占用

#### 显存分配示意

```
┌─────────────────────────────────────────────┐
│              GPU 总显存 (80GB)               │
├─────────────────────────────────────────────┤
│  Engine 1 权重 (~14GB)                       │
├─────────────────────────────────────────────┤
│  Engine 1 KV Cache (0.4 × 可用 ≈ 26GB)       │
├─────────────────────────────────────────────┤
│  Engine 2 权重 (~14GB)  ← 重复加载！          │
├─────────────────────────────────────────────┤
│  Engine 2 KV Cache (0.4 × 剩余 ≈ ?)          │
├─────────────────────────────────────────────┤
│  CUDA context + 激活值 + 碎片                 │
└─────────────────────────────────────────────┘
```

### 为什么会出现「第一次成功、第二次失败、第三次又成功」？

这是显存边界状态的典型表现：

1. **第一次成功**：第一个 engine 初始化时显存充足
2. **第二次失败**：第二个 engine 初始化时，第一个的显存尚未稳定，计算出的可用显存不足
3. **第三次成功**：Ray 重建 worker 后，旧 engine 已释放，显存恢复

关键点：vLLM 在 `setup()` 阶段计算 KV cache blocks 时使用的是 **瞬时可用显存**，如果此时显存处于波动状态，计算结果可能偏低。

---

## vLLM 参数关系

### KV Cache 计算公式

vLLM 的 KV cache blocks 计算逻辑（简化）：

```python
available_memory = total_gpu_memory × gpu_memory_utilization - model_weights - activation
num_blocks = available_memory / (block_size × num_layers × hidden_size × 2)
```

### 参数影响

| 参数 | 影响 | 调小的效果 |
|-----|------|----------|
| `gpu_memory_utilization` | KV cache 可用显存比例 | 减少 KV cache 预留，降低初始化失败风险 |
| `max_num_batched_tokens` | 单次推理最大 token 数 | 减少激活显存峰值 |
| `max_num_seqs` | 最大并发序列数 | 减少 KV cache 并发占用 |
| `max_model_len` | 最大序列长度 | 减少单序列 KV cache 需求 |

### 稳定性建议

对于 shared GPU 场景（`streaming_num_gpus < 1.0`）：

```python
# 保守配置
gpu_memory_utilization = 0.3 ~ 0.35  # 而非默认 0.4
max_num_batched_tokens = 32768       # 而非默认 65536
max_num_seqs = 16                    # 而非默认 32
```

---

## Ray Data 重试机制

### 重试层级

当 vLLM 初始化失败时，重试**不是**代码内部的 try/except 循环，而是 Ray Data 的 **actor 级重建**：

```
MapWorker(MapBatches(DistributedQwenVLVideoProcessMapper))
    ↓ setup() 失败
    ↓ actor 崩溃
    ↓ Ray Data 检测到 actor 失败
    ↓ 重建新 actor
    ↓ 新 actor 再次调用 setup()
```

### 日志识别

如果看到多次 `Initial ping for N actors...`，说明 actor 被重建了：

```
(MapWorker pid=123456) Initial ping for 16 actors...  # 第一次
... 中间的错误日志 ...
(MapWorker pid=234567) Initial ping for 16 actors...  # 重建后的新 actor
```

### 相关配置

```python
# Actor 重启配置
max_restarts=-1,           # 无限重启
max_task_retries=3,        # 任务级重试
```

---

## 孤儿进程清理

### EngineCore 进程

vLLM 的 `LLM()` 会 fork 出独立的 `EngineCore` 进程。当 MapWorker actor 异常退出时，`EngineCore` 可能成为孤儿进程（PPID=1）。

### 清理策略

当前实现只清理 **PPID=1 的孤儿进程**，避免误杀其他任务的 engine：

```python
def _kill_orphan_engine_cores(self):
    # 只杀 PPID=1 的进程
    orphan_pids = [pid for pid, ppid in pid_ppid_map.items() if ppid == 1]
```

### 局限性

- `pgrep -f EngineCore` 作用域较广，可能扫到整机其他任务
- PPID=1 判断较严格，非孤儿但已失效的 engine 不会被清理
- 在 shared machine 环境存在资源隔离风险

### 建议

1. 优先通过 `_shutdown_llm()` 正常关闭 engine
2. 孤儿清理作为兜底机制
3. 生产环境建议使用容器隔离

---

## 最佳实践

### 1. Shared GPU 配置建议

```bash
# 双卡共享时（每卡 2 个 worker）
--streaming-num-gpus 0.5 \
--qwenvl-gpu-memory-utilization 0.35 \
--qwenvl-max-num-batched-tokens 32768 \
--qwenvl-max-num-seqs 16
```

### 2. 监控关键指标

- GPU 显存使用率（`nvidia-smi`）
- `EngineCore` 进程数量
- MapWorker 重启次数
- KV cache block 分配量（vLLM 日志）

### 3. 故障排查步骤

1. 检查日志中的 `[GPU_STATE:*]` 行，了解初始化时的显存状态
2. 确认是否有多次 `Initial ping` 表明 actor 重建
3. 检查 `EngineCore` 进程的 PPID 关系
4. 尝试降低 `gpu_memory_utilization` 和 `max_num_batched_tokens`

### 4. 初始化串行化

当前实现使用文件锁确保 vLLM 初始化串行执行：

```python
with open("/tmp/vllm_qwen_vl_init.lock", "w") as lock_file:
    fcntl.flock(lock_file, fcntl.LOCK_EX)
    # 清理孤儿进程
    self._kill_orphan_engine_cores()
    # 串行初始化
    self.llm = LLM(...)
```

这可以避免多个 worker 同时初始化导致的显存竞争。

---

## 相关文件

- `pipeline/multi_video_classifier_merge/mappers/distributed_qwen_vl_video_process_mapper.py`
- `ops/config/qwenvl_config.py`
- `pipeline/multi_video_classifier_merge/pipeline_builder.py`
- `pipeline/multi_video_classifier_merge/multishot_video_classifier_pipeline_checkpoint.py`

---

## 更新历史

- 2026-04-29: 初始版本，整理 shared GPU 问题分析和参数配置
