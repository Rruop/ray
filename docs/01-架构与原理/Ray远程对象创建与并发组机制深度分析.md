# Ray 远程对象创建与并发组机制深度分析

> 基于源码 ray 2.52.1，详细分析 `ray.remote()` 装饰器的调用机制、`@ray.method(concurrency_group)` 注解的生效全链路、
> 以及 `functools.partial` 在装饰器中的桥梁作用。

---

## 目录

1. [ray.remote() 装饰器的两种调用方式](#1-rayremote-装饰器的两种调用方式)
2. [ray.remote() 完整调用链路](#2-rayremote-完整调用链路)
   - [函数路径：RemoteFunction 创建](#21-函数路径remotefunction-创建)
   - [类路径：ActorClass 创建](#22-类路径actorclass-创建)
3. [ray.remote() 的 *args, **kwargs 参数详解](#3-rayremote-的-args-kwargs-参数详解)
   - [两种调用场景的参数对照表](#31-两种调用场景的参数对照表)
   - [actor_options 完整参数列表](#32-actor_options-完整参数列表)
   - [task_options 完整参数列表](#33-task_options-完整参数列表)
4. [functools.partial 在装饰器中的桥梁作用](#4-functoolspartial-在装饰器中的桥梁作用)
5. [@ray.method(concurrency_group="io") 注解生效全链路](#5-raymethodconcurrency_groupio-注解生效全链路)
   - [第 1 步：装饰器写属性](#51-第-1-步装饰器写属性)
   - [第 2 步：类元数据提取](#52-第-2-步类元数据提取)
   - [第 3 步：Actor 创建时序列化到 protobuf](#53-第-3-步actor-创建时序列化到-protobuf)
   - [第 4 步：Actor Worker 初始化 — 建线程池 + 注册映射](#54-第-4-步actor-worker-初始化--建线程池--注册映射)
   - [第 5 步：任务执行时分发](#55-第-5-步任务执行时分发)
   - [两种分发路径对比](#56-两种分发路径对比)
6. [ray.remote() 与 @ray.method() 的关系](#6-rayremote-与-raymethod-的关系)
   - [层级划分](#61-层级划分)
   - [协作机制](#62-协作机制)
   - [合并过程详解](#63-合并过程详解)
7. [关键源码索引](#7-关键源码索引)

---

## 1. ray.remote() 装饰器的两种调用方式

`ray.remote()` 定义在 `python/ray/_private/worker.py:3589`，支持两种装饰器用法：

**方式一：无参数装饰 — `@ray.remote`**

```python
@ray.remote
def f(a, b, c):
    return a + b + c

@ray.remote
class Foo:
    def method(self):
        return 1
```

Python 把被装饰的函数/类作为参数直接传给 `remote()`。

**方式二：带参数调用 — `@ray.remote(concurrency_groups={"io": 2})`**

```python
@ray.remote(num_gpus=1, max_calls=1)
def f():
    return 1

@ray.remote(concurrency_groups={"io": 2, "compute": 4})
class AsyncActor:
    ...
```

分两步执行：先调用 `ray.remote(...)` 返回偏函数，再用偏函数装饰目标。

**核心分发逻辑** (`worker.py:3826`)：

```python
def remote(*args, **kwargs) -> Union[RemoteFunction, ActorClass]:
    if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
        # 方式一：@ray.remote — args[0] 就是函数或类
        return _make_remote(args[0], {})
    # 方式二：@ray.remote(...) — 只有选项参数，没有函数/类
    assert len(args) == 0 and len(kwargs) > 0
    return functools.partial(_make_remote, options=kwargs)
```

---

## 2. ray.remote() 完整调用链路

### 2.1 函数路径：RemoteFunction 创建

```
@ray.remote
def f(x): ...
  │
  ▼  worker.py  remote() → _make_remote(f, {})
  │
  ▼  worker.py:3401  _make_remote(function_or_class=f, options={})
  │  └── inspect.isfunction(f) → True
  │
  ▼  worker.py:3409  ray.remote_function.RemoteFunction(
        Language.PYTHON, f, None, options
      )
  │
  ▼  返回 RemoteFunction 实例
```

`RemoteFunction` 定义在 `python/ray/remote_function.py`，包装了原始函数，提供 `.remote()` 和 `.options()` 方法。

### 2.2 类路径：ActorClass 创建

```
@ray.remote(concurrency_groups={"io": 2, "compute": 4})
class AsyncActor: ...
  │
  ▼  第 1 步: remote(concurrency_groups={"io": 2, "compute": 4})
  │  args=(), kwargs={"concurrency_groups": {"io": 2, "compute": 4}}
  │  返回 functools.partial(_make_remote, options={"concurrency_groups": ...})
  │
  ▼  第 2 步: partial_obj(AsyncActor)
  │  等价于 _make_remote(AsyncActor, {"concurrency_groups": ...})
  │
  ▼  worker.py:3401  _make_remote(function_or_class=AsyncActor, options={...})
  │  └── inspect.isclass(AsyncActor) → True
  │
  ▼  worker.py:3416  ray.actor._make_actor(AsyncActor, options)
  │
  ▼  actor.py:2432  _make_actor(cls, actor_options)
  │  ├── _modify_class(cls)           — 注入 __ray_terminate__ 等方法
  │  ├── _inject_tracing_into_class()  — 注入 tracing
  │  └── ActorClass._ray_from_modified_class(Class, class_id, actor_options)
  │
  ▼  actor.py:1254  _ray_from_modified_class()
  │  ├── _ActorClassMethodMetadata.create()  ← 扫描方法上的 @ray.method 注解
  │  ├── _process_option_dict()              ← 解析 actor_options（含 concurrency_groups）
  │  └── _ActorClassMetadata(concurrency_groups=..., method_meta=...)
  │
  ▼  返回 ActorClass 实例（DynamicActorClass 的实例）
```

---

## 3. ray.remote() 的 *args, **kwargs 参数详解

### 3.1 两种调用场景的参数对照表

| 调用方式 | `*args` | `**kwargs` | 返回值 |
|---------|---------|------------|--------|
| `@ray.remote` | `(Foo,)` 或 `(f,)` — 被装饰的类/函数 | `{}` | `RemoteFunction` 或 `ActorClass` |
| `@ray.remote(concurrency_groups=...)` | `()` — 空 | 选项字典（见下方完整列表） | `functools.partial` |

### 3.2 actor_options 完整参数列表

由 `python/ray/_common/ray_option_utils.py:258` 定义：

```python
actor_options = {**_common_options, **_actor_only_options}
```

**`_common_options`（task 和 actor 共有）**：

| 参数名 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `num_cpus` | float/int/None | None (actor) / 1 (task) | CPU 资源量 |
| `num_gpus` | float/int/None | None | GPU 资源量 |
| `memory` | float/int/None | None | 堆内存请求（字节） |
| `object_store_memory` | int/None | None | Object Store 内存 |
| `resources` | dict/None | None | 自定义资源 |
| `accelerator_type` | str/None | None | 加速器类型 |
| `name` | str/None | None | 任务名 |
| `label_selector` | dict/None | None | 标签选择器 |
| `fallback_strategy` | list/None | None | 调度回退策略 |
| `placement_group` | None/str/PlacementGroup | "default" | 放置组 |
| `placement_group_bundle_index` | int | -1 | 放置组 bundle 索引 |
| `placement_group_capture_child_tasks` | bool/None | None | 子任务捕获 |
| `runtime_env` | dict/None | None | 运行时环境 |
| `scheduling_strategy` | None/str/... | None | 调度策略 |
| `enable_task_events` | bool | True | 是否启用任务事件 |
| `_labels` | dict/None | None | 键值标签 |

**`_actor_only_options`（仅 actor）**：

| 参数名 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `concurrency_groups` | list/dict/None | None | **并发组定义**，如 `{"io": 2, "compute": 4}` |
| `max_concurrency` | int | 1 | 默认并发组最大并发数 |
| `max_restarts` | int | 0 | 最大重启次数 |
| `max_task_retries` | int | 0 | 任务失败最大重试次数 |
| `max_pending_calls` | int | -1 | 最大待执行调用数 |
| `lifetime` | str/None | None | 生命周期（"detached"/"non_detached"） |
| `namespace` | str/None | None | 命名空间 |
| `get_if_exists` | bool | False | 已存在时获取 |
| `allow_out_of_order_execution` | bool/None | None | 允许乱序执行 |
| `enable_tensor_transport` | bool/None | None | 启用 tensor 传输 |

### 3.3 task_options 完整参数列表

```python
task_options = {**_common_options, **_task_only_options}
```

**`_task_only_options`（仅远程函数）**：

| 参数名 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `max_calls` | int | 0 | Worker 最大执行次数后退出 |
| `max_retries` | int | 3 | 最大重试次数 |
| `num_returns` | int/str/None | None | 返回值数量（1/"dynamic"/"streaming"） |
| `retry_exceptions` | bool/list/tuple | False | 是否重试用户异常 |
| `_generator_backpressure_num_objects` | int/None | None | 生成器反压阈值 |

---

## 4. functools.partial 在装饰器中的桥梁作用

### 4.1 functools.partial 基本原理

`functools.partial` 是 Python 标准库的偏函数工具，作用是**把函数的某些参数提前固定，返回一个新的可调用对象**。

```python
from functools import partial

def _make_remote(function_or_class, options):
    ...

# partial 固定 options 参数
partial_obj = partial(_make_remote, options={"concurrency_groups": {"io": 2}})

# 调用 partial_obj 等价于：
partial_obj(AsyncActor)
# == _make_remote(AsyncActor, options={"concurrency_groups": {"io": 2}})
```

### 4.2 在 @ray.remote(...) 装饰器中的两步执行

```python
@ray.remote(concurrency_groups={"io": 2, "compute": 4})   # 第 1 步
class AsyncActor:                                           # 第 2 步
    ...
```

**第 1 步**：Python 执行 `ray.remote(concurrency_groups={"io": 2, "compute": 4})`

- `args = ()`，`kwargs = {"concurrency_groups": {"io": 2, "compute": 4}}`
- 命中 `remote()` 的第二个分支
- 返回 `functools.partial(_make_remote, options={"concurrency_groups": {"io": 2, "compute": 4}})`
- 这是一个**偏函数对象**，只固定了 `options`，`function_or_class` 参数仍待传入

**第 2 步**：Python 用偏函数对象装饰 `AsyncActor`

- 调用 `partial_obj(AsyncActor)`
- 等价于 `_make_remote(AsyncActor, {"concurrency_groups": {"io": 2, "compute": 4}})`
- 进入 `_make_remote`，`inspect.isclass(AsyncActor)` → `_make_actor()`
- 返回 `ActorClass` 实例

### 4.3 为什么需要 partial

`@ray.remote` 不带参数时，Python 直接把被装饰对象传给 `remote()`，一步到位。

`@ray.remote(...)` 带参数时，Python **先执行** `remote(...)` 再用返回值装饰类——这要求 `remote(...)` 返回一个**可调用对象**。`functools.partial` 就是这个桥梁：

```
ray.remote(concurrency_groups=...)   →  partial 对象（可调用）
                │
                ▼
partial_obj(AsyncActor)               →  _make_remote(AsyncActor, options=...)
                │
                ▼
ActorClass 实例
```

没有 `partial`，就无法实现 `@decorator(args)` 这种带参数的装饰器语法。

---

## 5. @ray.method(concurrency_group="io") 注解生效全链路

### 5.1 第 1 步：装饰器写属性

**文件**：`python/ray/actor.py:464`

`@ray.method(concurrency_group="io")` 执行时，`annotate_method` 闭包把字符串 `"io"` 写到函数对象的 dunder 属性上：

```python
# actor.py:378-464
def method(*, concurrency_group=None, ...):
    def annotate_method(method):
        if "concurrency_group" in kwargs:
            method.__ray_concurrency_group__ = kwargs["concurrency_group"]
            #                         ↑ "io"
        ...
        return method
    return annotate_method
```

**关键点**：此时仅仅是往 Python 函数对象上写了一个属性，没有任何运行时逻辑。装饰器本身不参与后续的分发。

`@ray.method()` 还支持的其他注解属性：

| 属性 | 来源参数 | 示例 |
|------|---------|------|
| `__ray_concurrency_group__` | `concurrency_group` | `"io"` |
| `__ray_num_returns__` | `num_returns` | `2` |
| `__ray_max_task_retries__` | `max_task_retries` | `3` |
| `__ray_retry_exceptions__` | `retry_exceptions` | `True` |
| `__ray_enable_task_events__` | `enable_task_events` | `False` |
| `__ray_generator_backpressure_num_objects__` | `_generator_backpressure_num_objects` | `10` |
| `__ray_tensor_transport__` | `tensor_transport` | `"NCCL"` |

### 5.2 第 2 步：类元数据提取

**文件**：`python/ray/actor.py:1035-1038`

当 `@ray.remote(concurrency_groups={"io":2})` 处理类时，`_ActorClassMethodMetadata.create()` 遍历所有方法，用 `hasattr()` 检测注解属性：

```python
# actor.py:920  _ActorClassMethodMetadata.create()
actor_methods = inspect.getmembers(modified_class, is_function_or_method)
self.concurrency_group_for_methods = {}

for method_name, method in actor_methods:
    method = inspect.unwrap(method)
    ...
    if hasattr(method, "__ray_concurrency_group__"):
        self.concurrency_group_for_methods[method_name] = method.__ray_concurrency_group__
```

构建结果示例：
```python
concurrency_group_for_methods = {
    "f1": "io",
    "f2": "io",
    "f3": "compute",
    "f4": "compute"
}
```

### 5.3 第 3 步：Actor 创建时序列化到 protobuf

**文件**：`python/ray/actor.py:1755-1774`

`ActorClass._remote()` 创建 Actor 时，合并 `concurrency_groups`（来自 `@ray.remote`）和 `concurrency_group_for_methods`（来自 `@ray.method`），构建 `concurrency_groups_dict`：

```python
# actor.py:1755-1760  构建 group 基础结构
concurrency_groups_dict = {}
for cg_name in meta.concurrency_groups:  # {"io": 2, "compute": 4}
    concurrency_groups_dict[cg_name] = {
        "name": cg_name,
        "max_concurrency": meta.concurrency_groups[cg_name],
        "function_descriptors": [],
    }

# actor.py:1766-1774  将方法注册到对应的 group
for method_name in meta.method_meta.concurrency_group_for_methods:
    cg_name = meta.method_meta.concurrency_group_for_methods[method_name]
    assert cg_name in concurrency_groups_dict
    concurrency_groups_dict[cg_name]["function_descriptors"].append(
        PythonFunctionDescriptor(module_name, method_name, class_name)
    )
```

构建结果示例：
```python
concurrency_groups_dict = {
    "io": {
        "name": "io",
        "max_concurrency": 2,                                      # 来自 @ray.remote
        "function_descriptors": [PythonFunctionDescriptor("mod", "f1", "cls"),
                                  PythonFunctionDescriptor("mod", "f2", "cls")]  # 来自 @ray.method
    },
    "compute": {
        "name": "compute",
        "max_concurrency": 4,
        "function_descriptors": [PythonFunctionDescriptor("mod", "f3", "cls"),
                                  PythonFunctionDescriptor("mod", "f4", "cls")]
    }
}
```

**Cython 桥接** (`python/ray/_raylet.pyx:692-714`)：

```python
cdef int prepare_actor_concurrency_groups(
        dict concurrency_groups_dict,
        c_vector[CConcurrencyGroup] *concurrency_groups):
    for key, value in concurrency_groups_dict.items():
        c_fd_list = prepare_function_descriptors(value["function_descriptors"])
        concurrency_groups.push_back(CConcurrencyGroup(
            key.encode("ascii"), value["max_concurrency"], move(c_fd_list)))
```

转换 Python dict → C++ `CConcurrencyGroup` 结构体，传入 `CoreWorker::CreateActor()`。

**protobuf 序列化** (`src/ray/protobuf/common.proto:168-183, 822`)：

```protobuf
message ActorCreationTaskSpec {
    repeated ConcurrencyGroup concurrency_groups = 12;
}

message ConcurrencyGroup {
    string name = 1;
    int32 max_concurrency = 2;
    repeated FunctionDescriptor function_descriptors = 3;
}
```

**关键：调用方 `.remote()` 时不传 concurrency_group 名** — `ActorMethod._remote()` 中 `concurrency_group` 参数默认是 `None`，传到 `submit_actor_task()` 时变成 `b""`（空字符串）。注解的生效**不在客户端侧**，而在服务端（C++ 侧）。

### 5.4 第 4 步：Actor Worker 初始化 — 建线程池 + 注册映射

**文件**：`src/ray/core_worker/task_execution/task_receiver.cc:98-106`

Actor 创建任务完成后，`TaskReceiver` 初始化并发组管理器：

```cpp
if (task_spec.IsActorCreationTask()) {
    concurrency_groups_ = task_spec.ConcurrencyGroups();
    if (is_asyncio_) {
        fiber_state_manager_ = std::make_shared<ConcurrencyGroupManager<FiberState>>(
            concurrency_groups_, fiber_max_concurrency_, initialize_thread_callback_);
    } else {
        pool_manager_ = std::make_shared<ConcurrencyGroupManager<BoundedExecutor>>(
            concurrency_groups_, default_max_concurrency, initialize_thread_callback_);
    }
}
```

**文件**：`src/ray/core_worker/task_execution/concurrency_group_manager.cc:29-55`

`ConcurrencyGroupManager` 构造函数：

```cpp
for (auto &group : concurrency_groups) {
    const auto name = group.name_;
    const auto max_concurrency = group.max_concurrency_;
    // 为每个 group 创建独立线程池
    auto executor = std::make_shared<ExecutorType>(max_concurrency, initialize_thread_callback_);
    auto &fds = group.function_descriptors_;
    // 注册 函数描述符 → 线程池 映射
    for (auto fd : fds) {
        functions_to_executor_index_[fd->ToString()] = executor;
    }
    // 注册 组名 → 线程池 映射
    name_to_executor_index_[name] = executor;
}
// 默认线程池（无注解的方法）
default_executor_ = std::make_shared<ExecutorType>(
    max_concurrency_for_default_concurrency_group, initialize_thread_callback_);
```

构建结果示例：

| 映射表 | 内容 |
|--------|------|
| `name_to_executor_index_` | `{"io" → io_pool, "compute" → compute_pool}` |
| `functions_to_executor_index_` | `{"mod.f1.cls" → io_pool, "mod.f2.cls" → io_pool, "mod.f3.cls" → compute_pool, "mod.f4.cls" → compute_pool}` |
| `default_executor_` | 默认线程池（f5 等无注解方法） |

**BoundedExecutor 线程池** (`src/ray/core_worker/task_execution/thread_pool.cc:21-42`)：

```cpp
BoundedExecutor::BoundedExecutor(int max_concurrency, ...) {
    for (int i = 0; i < max_concurrency; i++) {
        threads_.emplace_back([this, &init_latch]() {
            std::function<void()> releaser = InitializeThread();
            init_latch.count_down();
            io_context_.run();  // 阻塞，处理 post 的任务
        });
    }
}
```

### 5.5 第 5 步：任务执行时分发

**文件**：`src/ray/core_worker/task_execution/concurrency_group_manager.cc:58-86`

`GetExecutor()` 是核心路由逻辑：

```cpp
std::shared_ptr<ExecutorType> GetExecutor(
    const std::string &concurrency_group_name, const ray::FunctionDescriptor &fd) {

  // 路径 1：显式指定 concurrency_group_name（来自 .options()）
  if (!concurrency_group_name.empty()) {
    auto it = name_to_executor_index_.find(concurrency_group_name);
    RAY_CHECK(it != name_to_executor_index_.end());
    return it->second;
  }

  // 路径 2：未显式指定，按函数描述符查表 ← @ray.method 注解生效的地方
  if (functions_to_executor_index_.find(fd->ToString()) !=
      functions_to_executor_index_.end()) {
    return functions_to_executor_index_[fd->ToString()];
  }

  // 路径 3：无注解，走默认线程池
  return default_executor_;
}
```

**分发入口** (`src/ray/core_worker/task_execution/unordered_actor_task_execution_queue.cc:147-165`)：

```cpp
if (is_asyncio_) {
    auto fiber = fiber_state_manager_->GetExecutor(
        request.ConcurrencyGroupName(), request.FunctionDescriptor());
    fiber->EnqueueFiber(...);
} else {
    auto pool = pool_manager_->GetExecutor(
        request.ConcurrencyGroupName(), request.FunctionDescriptor());
    pool->Post([...]{ ... });  // 提交到对应线程池执行
}
```

### 5.6 两种分发路径对比

| 场景 | task spec 中的 concurrency_group_name | 路由方式 | 示例 |
|------|---------------------------------------|---------|------|
| 普通 `.remote()` | `""` (空) | 按**函数描述符**查 `functions_to_executor_index_` | `a.f1.remote()` → 查到 `mod.f1.cls` 在 io_pool |
| `.options(concurrency_group="compute").remote()` | `"compute"` | 按**组名**查 `name_to_executor_index_` | `a.f2.options(concurrency_group="compute").remote()` → 查到 compute_pool |
| 无注解的方法 | `""` (空) | 函数描述符不在表中 → `default_executor_` | `a.f5.remote()` |

### 5.7 Asyncio Actor 的并发组 Event Loop

对于 asyncio actor，每个并发组创建独立的 event loop：

**文件**：`python/ray/_raylet.pyx:4340-4385`

```python
for i in range(c_defined_concurrency_groups.size()):
    cg_name = c_concurrency_group.GetName().decode("ascii")
    async_eventloop = get_new_event_loop()
    async_thread = threading.Thread(
        target=lambda: async_eventloop.run_forever(),
        name="AsyncIO Thread: {}".format(cg_name))
    async_thread.start()
    self.cgname_to_eventloop_dict[cg_name] = {"eventloop": ..., "thread": ...}
    for fd in function_descriptors:
        self.fd_to_cgname_dict[fd] = cg_name
```

---

## 6. ray.remote() 与 @ray.method() 的关系

### 6.1 层级划分

| 装饰器 | 作用对象 | 定义内容 | 类比 |
|--------|---------|---------|------|
| `@ray.remote(concurrency_groups={"io": 2})` | 类 | 定义有哪些组、每组多少线程 | **容器定义** |
| `@ray.method(concurrency_group="io")` | 方法 | 声明方法属于哪个组 | **成员注册** |

### 6.2 协作机制

```
@ray.remote(concurrency_groups={"io": 2, "compute": 4})  ← 定义 group 名和线程数
class AsyncActor:
    @ray.method(concurrency_group="io")                 ← 声明 f1 属于 "io" group
    def f1(self): ...

    @ray.method(concurrency_group="compute")             ← 声明 f3 属于 "compute" group
    def f3(self): ...

    def f5(self): ...                                    ← 无注解，走默认 group
```

### 6.3 合并过程详解

在 `ActorClass._remote()` 中（`actor.py:1755-1774`），两者合并为完整的 `concurrency_groups_dict`：

```python
# @ray.remote 提供的信息
concurrency_groups = {"io": 2, "compute": 4}

# @ray.method 提供的信息
concurrency_group_for_methods = {"f1": "io", "f2": "io", "f3": "compute", "f4": "compute"}

# 合并结果
concurrency_groups_dict = {
    "io": {
        "name": "io",
        "max_concurrency": 2,                    # ← 来自 @ray.remote
        "function_descriptors": [                # ← 来自 @ray.method
            PythonFunctionDescriptor("mod", "f1", "cls"),
            PythonFunctionDescriptor("mod", "f2", "cls"),
        ]
    },
    "compute": {
        "name": "compute",
        "max_concurrency": 4,                     # ← 来自 @ray.remote
        "function_descriptors": [                # ← 来自 @ray.method
            PythonFunctionDescriptor("mod", "f3", "cls"),
            PythonFunctionDescriptor("mod", "f4", "cls"),
        ]
    }
}
```

**注解本身不参与运行时逻辑**。`@ray.method(concurrency_group="io")` 仅仅是编译时注解，真正生效的是 C++ 侧 `ConcurrencyGroupManager` 根据 `function_descriptors` 建立的 `functions_to_executor_index_` 映射表，在任务执行时根据函数描述符查表路由到对应线程池。

---

## 7. 关键源码索引

### Python 层

| 文件 | 行号 | 关键点 |
|------|------|--------|
| `python/ray/_private/worker.py` | 3589-3830 | `ray.remote()` 入口，两种调用方式分发 |
| `python/ray/_private/worker.py` | 3401-3420 | `_make_remote()` — 函数/类分发到 RemoteFunction 或 ActorClass |
| `python/ray/_common/ray_option_utils.py` | 120-260 | `actor_options` / `task_options` 完整参数定义 |
| `python/ray/actor.py` | 378-464 | `ray.method()` 装饰器 — 写 `__ray_concurrency_group__` 属性 |
| `python/ray/actor.py` | 504-580 | `_ActorMethodMetadata` — 方法元数据容器 |
| `python/ray/actor.py` | 584-700 | `ActorMethod` — 方法调用代理（.remote/.options） |
| `python/ray/actor.py` | 793-880 | `ActorMethod._remote()` — 提交 actor task |
| `python/ray/actor.py` | 920-1040 | `_ActorClassMethodMetadata.create()` — 扫描方法注解 |
| `python/ray/actor.py` | 1035-1038 | `hasattr(method, "__ray_concurrency_group__")` — 读注解 |
| `python/ray/actor.py` | 1106-1150 | `_ActorClassMetadata.__init__` — 存储 concurrency_groups |
| `python/ray/actor.py` | 1155-1190 | `_process_option_dict()` — 解析 actor_options |
| `python/ray/actor.py` | 1254-1320 | `_ray_from_modified_class()` — ActorClass 构造 |
| `python/ray/actor.py` | 1755-1774 | 合并 concurrency_groups + method 注解 → concurrency_groups_dict |
| `python/ray/actor.py` | 2080-2190 | `_actor_method_call()` — 提交任务到 core_worker |
| `python/ray/actor.py` | 2432-2450 | `_make_actor()` — Actor 创建入口 |
| `python/ray/_raylet.pyx` | 692-714 | `prepare_actor_concurrency_groups()` — Python dict → C++ 结构体 |
| `python/ray/_raylet.pyx` | 3565-3642 | `create_actor()` — 传入 C++ CoreWorker |
| `python/ray/_raylet.pyx` | 4340-4385 | `initialize_eventloops_for_actor_concurrency_group()` — asyncio 路径 |

### C++ 层

| 文件 | 行号 | 关键点 |
|------|------|--------|
| `src/ray/protobuf/common.proto` | 168-183 | `ConcurrencyGroup` protobuf 定义 |
| `src/ray/protobuf/common.proto` | 822 | `ActorCreationTaskSpec.concurrency_groups` 字段 |
| `src/ray/protobuf/common.proto` | 555 | `TaskSpec.concurrency_group_name` 字段 |
| `src/ray/common/task/task_util.h` | 188 | `set_concurrency_group_name()` — 写入 task spec |
| `src/ray/common/task/task_spec.h` | 43 | `ConcurrencyGroup` C++ 结构体 |
| `src/ray/core_worker/core_worker.cc` | 2548-2620 | `SubmitActorTask()` — 传入 concurrency_group_name |
| `src/ray/core_worker/task_execution/task_receiver.cc` | 98-106 | Actor 创建时初始化 ConcurrencyGroupManager |
| `src/ray/core_worker/task_execution/concurrency_group_manager.cc` | 29-55 | 构造函数 — 建线程池 + 注册映射 |
| `src/ray/core_worker/task_execution/concurrency_group_manager.cc` | 58-86 | `GetExecutor()` — 核心路由逻辑 |
| `src/ray/core_worker/task_execution/thread_pool.cc` | 21-42 | `BoundedExecutor` — 线程池实现 |
| `src/ray/core_worker/task_execution/fiber.h` | — | `FiberState` — asyncio fiber 执行器 |
| `src/ray/core_worker/task_execution/unordered_actor_task_execution_queue.cc` | 147-165 | 任务分发入口 — 调用 GetExecutor() |

### 全链路总结图

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Python 客户端侧                              │
│                                                                     │
│  @ray.method(concurrency_group="io")                                │
│       │                                                             │
│       ▼  method.__ray_concurrency_group__ = "io"                    │
│                                                                     │
│  @ray.remote(concurrency_groups={"io": 2, "compute": 4})            │
│       │                                                             │
│       ▼  _ActorClassMethodMetadata.create()                        │
│       │  读取 __ray_concurrency_group__ → concurrency_group_for_methods │
│       ▼  _process_option_dict() → concurrency_groups={"io":2,...}  │
│       │                                                             │
│  ActorClass._remote()                                               │
│       │  合并 → concurrency_groups_dict                             │
│       ▼  Cython → CConcurrencyGroup[] → CoreWorker::CreateActor()   │
│                                                                     │
│  ActorMethod._remote() / .options().remote()                        │
│       │  concurrency_group_name = "" 或 "compute"                  │
│       ▼  submit_actor_task() → task spec                           │
└────────────────────────────┬────────────────────────────────────────┘
                             │ gRPC / IPC
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      C++ Actor Worker 侧                            │
│                                                                     │
│  Actor 创建任务完成                                                   │
│       │                                                             │
│       ▼  ConcurrencyGroupManager 构造                                │
│       │  为每个 group 创建 BoundedExecutor(max_concurrency)            │
│       │  建 name_to_executor_index_      {"io" → pool}               │
│       │  建 functions_to_executor_index_  {"mod.f1.cls" → pool}      │
│       │  建 default_executor_            (无注解方法)                  │
│                                                                     │
│  任务执行                                                            │
│       │                                                             │
│       ▼  GetExecutor(concurrency_group_name, fd)                    │
│       │                                                             │
│       ├── name 非空 → name_to_executor_index_[name]                │
│       │   (.options(concurrency_group=...) 路径)                     │
│       │                                                             │
│       ├── name 为空 → functions_to_executor_index_[fd]              │
│       │   (普通 .remote() 路径，@ray.method 注解在此生效)              │
│       │                                                             │
│       └── 不在表中 → default_executor_                              │
│           (无注解方法)                                                │
└─────────────────────────────────────────────────────────────────────┘
```
