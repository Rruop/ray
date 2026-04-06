# Ray worker_process_setup_hook 与 Monkey Patch 指南

## 目录

1. [worker_process_setup_hook 概述](#1-worker_process_setup_hook-概述)
2. [Driver 与 Worker 进程区别](#2-driver-与-worker-进程区别)
3. [Monkey Patch 原理](#3-monkey-patch-原理)
4. [Patch 实现方式](#4-patch-实现方式)
5. [StreamingExecutor Patch 方案](#5-streamingexecutor-patch-方案)
6. [最佳实践](#6-最佳实践)

---

## 1. worker_process_setup_hook 概述

### 1.1 支持的配置类型

| 类型 | 示例 | 适用场景 |
|------|------|----------|
| **Callable (函数)** | `lambda: configure_logging()` | ray.init() 直接调用 |
| **字符串 (模块名)** | `"my_module.setup_func"` | ray.init() 或 Job Submission API |

### 1.2 工作流程

```
ray.init() 配置 worker_process_setup_hook
        ↓
Driver 进程处理（upload_worker_process_setup_hook_if_needed）
        ↓
    ┌───────────────────┬───────────────────┐
    │ Callable 类型      │ 字符串类型         │
    │ 序列化 → 上传 GCS   │ 直接存入 env_vars  │
    └───────────────────┴───────────────────┘
        ↓
Worker 进程启动 (default_worker.py)
        ↓
从环境变量读取 hook key
        ↓
load_and_execute_setup_hook() 加载并执行
```

### 1.3 关键代码位置

| 功能 | 文件位置 |
|------|----------|
| 定义 | `python/ray/runtime_env/runtime_env.py` (line 322) |
| 上传处理 | `python/ray/_private/runtime_env/setup_hook.py` |
| Worker 执行 | `python/ray/_private/workers/default_worker.py` (line 314-320) |

### 1.4 重要限制

**worker_process_setup_hook 不会在 Driver 中执行**

原因：
1. Hook 的执行代码位于 `default_worker.py`，这是 **Worker 进程** 的入口文件
2. Driver 进程运行在 `SCRIPT_MODE`，不会执行 Worker 的启动流程
3. Driver 进程只负责 **上传和注册** hook，不负责执行

代码证据 (`default_worker.py`):
```python
# Worker 进程启动后执行
worker_process_setup_hook_key = os.getenv(
    ray_constants.WORKER_PROCESS_SETUP_HOOK_ENV_VAR
)
if worker_process_setup_hook_key:
    error = load_and_execute_setup_hook(worker_process_setup_hook_key)
    if error is not None:
        worker.core_worker.drain_and_exit_worker("system", error)
```

---

## 2. Driver 与 Worker 进程区别

### 2.1 进程架构图

```
┌─────────────────────────────────────────────────────────────┐
│                     Driver 进程                              │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ StreamingExecutor (主线程 + 调度线程)                  │   │
│  │   - _scheduling_loop_step()                          │   │
│  │   - ResourceManager.update_usages()                  │   │
│  │   - select_operator_to_run()                         │   │
│  └─────────────────────────────────────────────────────┘   │
│                          │                                  │
│                    提交 Task/Actor                          │
└──────────────────────────┼──────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│                     Worker 进程                              │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ worker_process_setup_hook 在这里执行 ✓               │   │
│  │ 但 StreamingExecutor 不在这里运行 ✗                  │   │
│  │                                                      │   │
│  │ 实际执行的是：map/filter 等 UDF 函数                  │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 代码执行位置对照

| 代码类型 | 执行位置 | worker_process_setup_hook 能否 patch |
|----------|----------|--------------------------------------|
| `StreamingExecutor` | Driver | ❌ 不能 |
| `ResourceManager` | Driver | ❌ 不能 |
| `select_operator_to_run` | Driver | ❌ 不能 |
| `map/filter UDF` | Worker | ✅ 可以 |
| `Actor 内部逻辑` | Worker | ✅ 可以 |
| `环境配置、日志` | Worker | ✅ 可以 |

---

## 3. Monkey Patch 原理

### 3.1 Python 模块导入机制

Python 导入模块时，会执行模块中的**顶层代码**：

```python
# example_module.py

print("1. 模块顶层代码")           # ✅ import 时执行

CONSTANT = "hello"                 # ✅ import 时执行（赋值）

def my_function():                 # ✅ import 时执行（定义函数对象）
    print("函数体内容")             # ❌ 不执行（只是定义，没有调用）

class MyClass:                     # ✅ import 时执行（定义类对象）
    print("2. 类体内代码")          # ✅ import 时执行

    def method(self):
        print("方法体内容")         # ❌ 不执行

my_function()                      # ✅ import 时执行（调用函数）
print("3. 模块末尾")               # ✅ import 时执行
```

```python
# 另一个文件
import example_module
# 输出：
# 1. 模块顶层代码
# 2. 类体内代码
# 函数体内容
# 3. 模块末尾
```

### 3.2 apply() 函数是否自动执行？

**不会自动执行**，除非在模块顶层显式调用：

```python
# 情况 1：不会自动执行
def apply():
    print("apply 被调用了")
    # patch 逻辑...

# import 后什么都不会发生
```

```python
# 情况 2：会自动执行
def apply():
    print("apply 被调用了")
    # patch 逻辑...

apply()  # ← 这行代码让 import 时自动执行
```

### 3.3 Monkey Patch 内存模型

```
┌──────────────────────────────────────────────────────────────┐
│                    Python 运行时内存                          │
├──────────────────────────────────────────────────────────────┤
│  sys.modules (模块缓存，全局单例)                             │
│  ┌────────────────────────────────────────────────────────┐ │
│  │ "ray.data._internal.execution.streaming_executor"      │ │
│  │    └─ StreamingExecutor (类对象)                       │ │
│  │         └─ _scheduling_loop_step ──→ [函数对象引用]    │ │
│  └────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────┘
                              │
                    Monkey Patch 修改引用
                              ↓
┌──────────────────────────────────────────────────────────────┐
│  StreamingExecutor._scheduling_loop_step ──→ [新函数对象]    │
│  (原函数对象仍在内存中，但不再被类引用)                        │
└──────────────────────────────────────────────────────────────┘
```

### 3.4 为什么 Patch 对所有实例生效

Python 方法查找顺序：`instance → class → parent class → ...`

```python
executor = StreamingExecutor()
executor._scheduling_loop_step(topology)
# 查找顺序：
# 1. executor.__dict__ 中查找 → 没有
# 2. StreamingExecutor.__dict__ 中查找 → 找到（已被 patch）
# 3. 使用 patched 方法
```

---

## 4. Patch 实现方式

### 4.1 方式 1：自动应用（推荐）

```python
# my_ray_patches/patch_interleave_dispatch.py

_applied = False

def apply():
    global _applied
    if _applied:
        return
    _applied = True

    from ray.data._internal.execution.streaming_executor import StreamingExecutor

    _original = StreamingExecutor._scheduling_loop_step

    def _patched(self, topology):
        # 你的优化逻辑
        return _original(self, topology)

    StreamingExecutor._scheduling_loop_step = _patched
    print("[Patch] StreamingExecutor patched successfully")

apply()  # ← 模块末尾调用，import 时自动生效
```

**使用方式：**
```python
import my_ray_patches.patch_interleave_dispatch  # 自动生效
import ray
ray.init()
# ...
```

### 4.2 方式 2：手动调用

```python
# my_ray_patches/patch_interleave_dispatch.py

_applied = False

def apply():
    global _applied
    if _applied:
        return
    _applied = True

    # patch 逻辑...
    print("[Patch] Applied")

# 不在末尾调用 apply()
```

**使用方式：**
```python
from my_ray_patches.patch_interleave_dispatch import apply
apply()  # 手动调用

import ray
ray.init()
```

### 4.3 方式对比

| 方式 | 优点 | 缺点 |
|------|------|------|
| **自动 (末尾调用 apply())** | 简单，import 即生效 | 无法控制是否启用 |
| **手动调用 apply()** | 可控，可选择性启用 | 需要额外一行代码 |

### 4.4 执行顺序的重要性

```python
# ✅ 正确顺序 - 在使用前 patch
import my_ray_patches.patch_interleave_dispatch  # 1. 先执行 patch
import ray
ray.init()                                        # 2. 初始化 Ray
ds = ray.data.read_parquet(...)                   # 3. 创建 Dataset
ds.map(fn).take()                                 # 4. StreamingExecutor 使用 patched 方法

# ❌ 错误顺序 - 如果 StreamingExecutor 已经实例化
import ray
ray.init()
ds = ray.data.read_parquet(...)
ds.map(fn).take()                                 # 已经用了原始方法
import my_ray_patches.patch_interleave_dispatch   # 太晚了
```

---

## 5. StreamingExecutor Patch 方案

### 5.1 问题分析

`StreamingExecutor` 和 `ResourceManager` 运行在 **Driver 进程**，因此：

- ❌ `worker_process_setup_hook` **无法** patch 这些代码
- ✅ 需要在 **Driver 进程启动时** 执行 patch

### 5.2 解决方案

#### 方案 A：手动 import patch（推荐）

```python
# 在你的主脚本开头
import my_ray_patches.patch_interleave_dispatch  # 必须在最前面

import ray
ray.init()
ds = ray.data.read_parquet(...)
ds.map(fn).take()
```

#### 方案 B：直接修改 Ray 源代码

直接修改 `python/ray/data/_internal/execution/streaming_executor.py`：

- ✅ 对 Driver 和 Worker 都生效
- ✅ 不需要额外的 import
- ❌ 需要维护自定义的 Ray 版本

#### 方案 C：结合使用

```python
# 主脚本
import my_ray_patches.patch_interleave_dispatch  # Driver 侧 patch

ray.init(
    runtime_env={
        "py_modules": ["./my_ray_patches"],
        "worker_process_setup_hook": "my_ray_patches.worker_setup.apply"
    }
)
# worker_process_setup_hook 用于 Worker 侧的其他配置（如日志、环境变量）
```

### 5.3 验证 Patch 是否生效

```python
import my_ray_patches.patch_interleave_dispatch

from ray.data._internal.execution.streaming_executor import StreamingExecutor

# 检查方法是否被替换
print(StreamingExecutor._scheduling_loop_step)
# 应该显示你的 patched 函数名，而不是原始函数名
```

---

## 6. 最佳实践

### 6.1 完整的 Patch 模块模板

```python
# my_ray_patches/patch_interleave_dispatch.py
"""
Ray Data StreamingExecutor 调度优化 Patch

优化内容：
- GPU-First 处理：GPU operator 的完成任务优先于 CPU operator 处理
- 批量分发：update_usages() 每 64 次 dispatch 调用一次
- 分发上限：每轮 _dispatch_tasks() 限制最多 512 次 dispatch

使用方式：
    import my_ray_patches.patch_interleave_dispatch  # 在 ray.init() 之前
"""

import logging

logger = logging.getLogger(__name__)

_applied = False

# 配置参数
BATCH_SIZE = 512
DISPATCH_UPDATE_INTERVAL = 64
MAX_DISPATCHES_PER_ROUND = 512
PROFILE_THRESHOLD_S = 5.0
GPU_FIRST = True


def apply():
    """应用调度优化 patch"""
    global _applied
    if _applied:
        logger.debug("[Patch] Already applied, skipping")
        return
    _applied = True

    from ray.data._internal.execution.streaming_executor import StreamingExecutor
    from ray.data._internal.execution.resource_manager import ResourceManager

    # 保存原始方法
    _original_scheduling_loop_step = StreamingExecutor._scheduling_loop_step
    _original_update_usages = ResourceManager.update_usages

    def _patched_scheduling_loop_step(self, topology):
        """优化后的调度循环"""
        # 你的优化逻辑
        # ...
        return _original_scheduling_loop_step(self, topology)

    def _patched_update_usages(self, update_op_state=True):
        """添加快速路径的 update_usages"""
        if not update_op_state:
            # 快速路径：只更新必要的状态
            return
        return _original_update_usages(self)

    # 应用 patch
    StreamingExecutor._scheduling_loop_step = _patched_scheduling_loop_step
    ResourceManager.update_usages = _patched_update_usages

    logger.info("[Patch] StreamingExecutor scheduling optimization applied")


def is_applied() -> bool:
    """检查 patch 是否已应用"""
    return _applied


# 模块导入时自动应用
apply()
```

### 6.2 目录结构

```
my_ray_patches/
├── __init__.py
├── patch_interleave_dispatch.py   # 调度优化 patch
└── worker_setup.py                 # Worker 侧配置（可选）
```

### 6.3 使用示例

```python
#!/usr/bin/env python3
"""Ray Data 任务示例（带调度优化）"""

# 1. 首先导入 patch（必须在 ray 之前）
import my_ray_patches.patch_interleave_dispatch

# 2. 然后导入和初始化 ray
import ray

ray.init(
    object_store_memory=5_000_000_000,
    # worker_process_setup_hook 用于 Worker 侧配置（可选）
)

# 3. 执行 Ray Data 任务
ds = ray.data.read_parquet("s3://bucket/data/")
ds = ds.map(lambda x: x)
result = ds.take(10)

print(result)
```

### 6.4 注意事项总结

| 场景 | 解决方案 |
|------|----------|
| Patch Driver 侧代码（StreamingExecutor 等） | 手动 import patch 模块 |
| Patch Worker 侧代码（UDF、Actor 等） | 使用 worker_process_setup_hook |
| 同时 patch Driver 和 Worker | 结合使用两种方式 |
| 确保 patch 只执行一次 | 使用 `_applied` 标志 |
| 验证 patch 生效 | 打印方法引用或添加日志 |

---

## 附录：常见问题

### Q1: 为什么我的 patch 没有生效？

检查以下几点：
1. import 顺序是否正确（patch 在 ray.init() 之前）
2. apply() 是否被调用（模块末尾是否有 `apply()`）
3. patch 的目标代码是否在正确的进程中运行

### Q2: worker_process_setup_hook 配置字符串格式是什么？

格式：`"module.submodule.function_name"`

```python
ray.init(
    runtime_env={
        "py_modules": ["./my_ray_patches"],
        "worker_process_setup_hook": "my_ray_patches.worker_setup.apply"
    }
)
```

### Q3: 如何调试 patch 是否生效？

```python
import my_ray_patches.patch_interleave_dispatch

from ray.data._internal.execution.streaming_executor import StreamingExecutor

# 方法 1：打印函数对象
print(StreamingExecutor._scheduling_loop_step)

# 方法 2：检查函数名
print(StreamingExecutor._scheduling_loop_step.__name__)

# 方法 3：在 patch 函数中添加日志
```

### Q4: patch 会影响性能吗？

Monkey patch 本身的性能开销可以忽略不计（只是修改了一个引用）。性能影响主要取决于你的 patch 逻辑本身。
