# DistributedStreamingVideoProcessMapper CUDA OOM 异常分析

## 一、异常现象

### 错误日志特征

- Operator: `Map(DistributedStreamingVideoProcessMapper)`
- 固定 Actor: `pid=55647, ip=10.80.246.240, actor_id=99d8d5c5f21f6c260ebfe8f93c000000`
- 错误计数持续递增: `max_errored_blocks=9997 → 9996 → 9995 → 9994 ...`
- 错误频率: 每秒多次，持续不断

### 关键错误信息

**第一次 OOM（模型加载阶段）:**

```
RuntimeError: CUDA error: out of memory
CUDA kernel errors might be asynchronously reported at some other API call,
so the stacktrace below might be incorrect.
```

**第二次 OOM（附带显存详情）:**

```
CUDA out of memory. Tried to allocate 48.00 MiB.
GPU 0 has a total capacity of 23.88 GiB of which 20.95 GiB is free.
Process 3909899 has 20.66 GiB memory in use.
Process 1112668 has 1.42 GiB memory in use.
Process 1116066 has 1.49 GiB memory in use.
Of the allocated memory 974.87 MiB is allocated by PyTorch,
and 19.13 MiB is reserved by PyTorch but unallocated.
```

### 完整调用栈

```
File: ops/core/mapper/base_mapper.py, line 31, in __call__
    self.setup()
File: pipeline/multi_video_classifier_merge/mappers/distributed_streaming_video_process_mapper.py, line 232, in setup
    ).to(self.device)
File: transformers/modeling_utils.py, line 4110, in to
    return super().to(*args, **kwargs)
File: torch/nn/modules/module.py, line 1355, in to
    return self._apply(convert)
File: torch/nn/modules/module.py, line 915, in _apply
    module._apply(fn)
  ... (递归遍历模型子模块)
File: torch/nn/modules/module.py, line 942, in _apply
    param_applied = fn(param)
File: torch/nn/modules/module.py, line 1341, in convert
    return t.to(
RuntimeError: CUDA error: out of memory
```

---

## 二、根因分析

### 2.1 表面原因：GPU 显存不足？

乍看是显存不足，但**第二条 OOM 日志显示 GPU 有 20.95 GiB 空闲，却分配不出 48 MiB**，这显然不合理。因此**不是单纯的显存不足问题**。

### 2.2 真正原因：CUDA Context 污染导致的连锁失败

**CUDA 的关键机制：一旦某个 CUDA 操作失败（如首次 OOM），整个进程的 CUDA Context 会被标记为错误状态。此后该进程中所有 CUDA 操作都会失败，无论实际显存是否充足。**

完整因果链：

1. **首次 OOM（瞬时显存竞争）**：多个 Worker 同时在 `setup()` 中加载模型，瞬时显存峰值叠加，某个 CUDA 操作失败
2. **CUDA Context 被污染**：pid=55647 的进程进入 CUDA 错误状态
3. **后续所有 CUDA 调用全部失败**：每次 `setup()` 重新 `.to(self.device)` 都报 OOM，但实际原因是 CUDA Context 已损坏，并非真的显存不够
4. **无限循环**：`_initialized` 未被设为 True → 每次 `__call__` 都重试 `setup()` → 每次都因 CUDA 损坏而失败 → 无限重复

CUDA 自身的错误信息也暗示了这一点：

> CUDA kernel errors might be **asynchronously reported** at some other API call, so the stacktrace below might be incorrect

这说明 OOM 可能不是发生在 `.to(self.device)` 时，而是更早的某个 CUDA 操作失败了，异步报告在了这里。

### 2.3 为什么 setup() 会反复执行？

关键代码（`base_mapper.py:31`）：

```python
def __call__(self, row):
    if not self._initialized:
        self.setup()        # 如果这里抛异常...
        self._initialized = True  # 这行不会执行！
```

当 `setup()` 抛出异常时，`self._initialized = True` **不会被执行**，因此下次 `__call__` 被调用时，`_initialized` 仍为 False，会再次尝试 `setup()`。

### 2.4 为什么同一个 Actor 持续报错？

Ray Data 的 `ActorPoolMapOperator` 在 Worker 执行 UDF 异常时，**不会销毁重建 Actor**。同一个 MapWorker（pid=55647, actor_id=99d8d5c5f21f6c260ebfe8f93c000000）会持续接收新数据块，每次都因 CUDA Context 损坏而失败。

### 2.5 调度层为什么不会中断？

`patch_interleave_dispatch.py` 中的调度循环只做错误计数，不销毁重建 Actor：

```python
except Exception as e:
    errored_blocks_per_op[state] += 1
    num_errored_blocks += 1
    should_ignore = (
        self._max_errored_blocks < 0
        or self._max_errored_blocks >= num_errored_blocks
    )
    if should_ignore:
        logger.error(error_message, exc_info=e)  # 只记录，不重试，不重建
```

`max_errored_blocks` 默认值很大（10000），所以会持续容忍错误。

### 2.6 完整错误循环图

```
新数据块到达
  → MapWorker.__call__()
  → _initialized=False
  → setup()
  → 加载模型到 GPU (.to(self.device))
  → CUDA OOM (首次是真实 OOM，后续是 Context 损坏)
  → 异常抛出，_initialized 仍为 False
  → 调度循环记录错误，跳过此 block
  → 下一个数据块到达
  → 再次 __call__()
  → 再次 setup()
  → 再次 OOM
  → 无限循环 ...
```

### 2.7 配置层面的加剧因素

| 配置项 | 默认值 | 问题 |
|--------|--------|------|
| `streaming_num_gpus` | 0.2 | 每个 Worker 只申请 0.2 GPU，单 GPU 可调度 5 个 Worker |
| `streaming_gpu_concurrency` | 2 | 并发度 2，加剧同时 setup 的压力 |
| `self.device = "cuda"` | 默认 cuda:0 | 未指定 GPU ID，无隔离 |

多 Worker 共享同一 GPU 同时加载模型，瞬时显存峰值叠加，容易触发首次 OOM。

---

## 三、解决方案

### 3.1 优先级 P0：防止 setup() 反复重试

在 `base_mapper.py` 的 `__call__` 中，无论 setup 是否成功都标记 `_initialized=True`，避免同一个 Worker 反复进入 setup 导致无限 OOM 循环：

```python
def __call__(self, row: Dict[str, Any]) -> Dict[str, Any]:
    if not self._initialized:
        try:
            self.setup()
        except Exception as e:
            self.logger.error(f"Setup failed: {e}, marking as initialized to prevent retries")
            self._initialized = True  # 关键：阻止反复重试
            raise
        self._initialized = True
    # ... 后续逻辑
```

### 3.2 优先级 P1：模型加载优化（减少瞬时显存峰值）

在 `distributed_streaming_video_process_mapper.py` 的 `setup()` 中：

```python
def setup(self) -> None:
    if torch.cuda.is_available():
        torch.cuda.empty_cache()  # 加载前清理缓存

    # 使用 low_cpu_mem_usage 减少加载时的瞬时显存峰值
    self.embedding_model = AutoModel.from_pretrained(
        embedding_model_dir,
        torch_dtype=torch.bfloat16,
        low_cpu_mem_usage=True,  # 减少加载时显存峰值
    ).to(self.device)
    self.embedding_model.eval()
```

### 3.3 优先级 P1：设置 CUDA 环境变量

```bash
# 减少显存碎片化
export PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True

# 同步报错，便于定位首次 OOM 的真实位置
export CUDA_LAUNCH_BLOCKING=1
```

### 3.4 优先级 P2：调整资源配置

避免同一 GPU 上过多 Worker 同时 setup：

```python
# 方案 A：增大每 Worker GPU 份额
streaming_num_gpus: float = 0.5  # 从 0.2 提高到 0.5，单 GPU 最多 2 个 Worker

# 方案 B：降低并发度
streaming_gpu_concurrency: int = 1  # 从 2 降到 1，减少同时 setup 的 Worker 数
```

### 3.5 优先级 P2：setup 阶段加锁/延时

避免多个 Worker 同时加载模型造成瞬时显存峰值：

```python
import fcntl

def setup(self) -> None:
    # 使用文件锁串行化同机器上的模型加载
    lock_path = f"/tmp/gpu_model_load_{torch.cuda.current_device()}.lock"
    with open(lock_path, "w") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            self._load_models()
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)
```

### 3.6 优先级 P3：Actor 销毁重建机制

在 `patch_interleave_dispatch.py` 中，当某个 Actor 连续 N 次 setup 失败时，主动销毁并重建该 Actor，而非让其持续消费数据块：

```python
# 伪代码：在错误处理中增加 Actor 重建逻辑
if errored_blocks_per_op[state] > REBUILD_THRESHOLD:
    state.op._actor_pool.remove_actor(actor_id)  # 销毁损坏的 Actor
    state.op._actor_pool.add_actor()              # 创建新 Actor
```

---

## 四、验证方法

1. **确认 CUDA Context 污染**：在首次 OOM 后，在同一个 Worker 中执行一个简单的 CUDA 操作（如 `torch.zeros(1).cuda()`），如果也失败则确认 Context 已损坏
2. **确认修复效果**：应用 P0 修复后，同一个 Actor 应只报一次 OOM，而非持续报错
3. **确认资源调优效果**：应用 P2 修复后，观察首次 OOM 是否不再发生

---

## 五、涉及文件

| 文件 | 行号 | 说明 |
|------|------|------|
| `ops/core/mapper/base_mapper.py` | 31-33 | `__call__` 中 setup() 调用与 `_initialized` 标志 |
| `pipeline/multi_video_classifier_merge/mappers/distributed_streaming_video_process_mapper.py` | 232 | `setup()` 中 `.to(self.device)` OOM 触发点 |
| `pipeline/multi_video_classifier_merge/pipeline_builder.py` | 84-93 | GPU 资源配置参数 |
| `utils/patch_interleave_dispatch.py` | 280-295 | 调度循环错误处理逻辑 |
