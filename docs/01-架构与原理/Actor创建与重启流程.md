# Actor 创建与重启流程完整深度分析

## 一、整体架构

```
ActorPoolMapOperator
├── _ActorPool          # 管理 actor 池（创建、销毁、扩缩容）
├── _ActorTaskSelector  # 选择哪个 actor 来处理哪个 bundle（含 locality 感知）
├── _bundle_queue       # 等待调度的输入 bundle 队列
└── _MapWorker          # 实际的 actor 工作类
```

---

## 二、Actor 启动流程（3 个阶段）

### 阶段 1：触发启动 — `start()` 方法

```python
def start(self, options: ExecutionOptions):
    self._actor_locality_enabled = options.actor_locality_enabled
    super().start(options)

    # 1️⃣ 创建 actor 的远程类（携带 num_cpus, num_gpus, scheduling_strategy 等）
    self._actor_cls = ray.remote(**self._ray_remote_args)(self._map_worker_cls)

    # 2️⃣ 触发初始扩容（异步创建 initial_size 个 actor）
    self._actor_pool.scale(
        ActorPoolScalingRequest(
            delta=self._actor_pool.initial_size(), reason="scaling to initial size"
        )
    )

    # 3️⃣ 可选：同步等待最小 actor 数启动完成
    if self.data_context.wait_for_min_actors_s > 0:
        refs = self._actor_pool.get_pending_actor_refs()
        try:
            ray.get(refs, timeout=timeout)  # ⏳ 阻塞等待
        except ray.exceptions.GetTimeoutError:
            raise  # 集群资源不足
```

**关键点：**

- `ray.remote(**self._ray_remote_args)(self._map_worker_cls)` — 把 `num_cpus`、`num_gpus`、`scheduling_strategy` 等 remote args 绑定到 `_MapWorker` 类上
- `self._map_worker_cls = type(f"MapWorker({self.name})", (_MapWorker,), {})` — 每个 operator 动态创建子类，名字带 operator 名称，方便在 Grafana 里区分
- 默认 `scheduling_strategy` 来自 `DataContext.scheduling_strategy`（通常是 SPREAD）

### 阶段 2：创建 Actor — `_start_actor()` 方法

```python
def _start_actor(self, labels, logical_actor_id):
    actor = self._actor_cls.options(
        _labels={self._OPERATOR_ID_LABEL_KEY: self.id, **labels}
    ).remote(
        ctx=ctx,
        logical_actor_id=logical_actor_id,
        src_fn_name=self.name,
        map_transformer=self._map_transformer,
        actor_location_tracker=get_or_create_actor_location_tracker(),  # 🔑
    )

    # 3️⃣ 立即调用 actor.get_location.remote() 获取位置
    res_ref = actor.get_location.remote()

    # 4️⃣ 注册回调：当 res_ref ready 时，把 actor 从 pending 移到 running
    def _task_done_callback(res_ref):
        has_actor = self._actor_pool.pending_to_running(res_ref)

    self._submit_metadata_task(res_ref, lambda: _task_done_callback(res_ref))
    return actor, res_ref
```

**精妙设计：**

1. `actor = self._actor_cls.options(...).remote(...)` — 发出创建 actor 的请求，异步不等待
2. `res_ref = actor.get_location.remote()` — 立即向 actor 发一个 `get_location` 任务，它在 actor 启动完成后执行
3. `res_ref` 同时充当 "actor 已就绪" 的信号 — 当 `res_ref` 变为 ready，说明 actor 已成功启动并返回了节点 ID

### 阶段 3：Actor 初始化 — `_MapWorker.__init__()`

```python
class _MapWorker:
    def __init__(self, ctx, src_fn_name, map_transformer, logical_actor_id,
                 actor_location_tracker):
        self.src_fn_name = src_fn_name
        self._map_transformer = map_transformer
        DataContext._set_current(ctx)

        # 1️⃣ 初始化 UDF（带重试逻辑）
        self._init_udf_with_retries(ctx)

        self._logical_actor_id = logical_actor_id

        # 2️⃣ 向 ActorLocationTracker 注册自己的位置
        actor_location_tracker.update_actor_location.remote(
            self._logical_actor_id, ray.get_runtime_context().get_node_id()
        )
```

**初始化顺序很重要：**

1. 设置 DataContext → 2. 初始化 UDF（可能失败重试） → 3. 注册位置
- UDF 初始化有重试逻辑（`_init_udf_with_retries`），支持 `actor_init_max_retries` 和 `actor_init_retry_on_errors`
- 位置注册是异步的（`.remote()`），不阻塞

---

## 三、确认 Actor 启动成功的机制

有两层确认机制：

### 第 1 层：`pending_to_running()` — 基于 `get_location` 的 ObjectRef

```python
def pending_to_running(self, ready_ref):
    if ready_ref not in self._pending_actors:
        return False  # actor 已被 kill

    actor = self._pending_actors.pop(ready_ref)

    try:
        actor_location = ray.get(ready_ref)  # 🔑 获取节点 ID
    except Exception:
        self._actor_to_logical_id.pop(actor, None)
        raise  # actor 初始化失败

    self._running_actors[actor] = _ActorState(
        num_tasks_in_flight=0,
        actor_location=actor_location,  # 保存节点 ID
        is_restarting=False,
    )
    return True
```

**流程图：**

```
_start_actor() 被调用
    │
    ├── actor = _actor_cls.options().remote(...)    # 异步创建 actor
    ├── res_ref = actor.get_location.remote()       # 异步获取位置
    ├── add_pending_actor(actor, res_ref)           # 放入 pending 字典
    └── _submit_metadata_task(res_ref, callback)    # 注册监控
            │
            ▼  （调度循环中检查）
    res_ref ready → callback() → pending_to_running(res_ref)
            │
            ├── ray.get(ready_ref) → 节点 ID
            └── _running_actors[actor] = _ActorState(location=节点ID)
```

### 第 2 层：ActorLocationTracker — 全局位置注册表

```python
@ray.remote(num_cpus=0, max_restarts=-1, max_task_retries=-1)
class ActorLocationTracker:
    def __init__(self):
        self._actor_locations = {}  # {logical_actor_id: node_id}

    def update_actor_location(self, logical_actor_id, node_id):
        self._actor_locations[logical_actor_id] = node_id

    def get_actor_locations(self, logical_actor_ids):
        return {id: self._actor_locations.get(id) for id in logical_actor_ids}
```

- 这是一个单例 detached actor，固定在 driver/head 节点（`NodeAffinitySchedulingStrategy(soft=False)`）
- 所有 `_MapWorker` 在 `__init__` 中向它注册自己的 `logical_actor_id → node_id` 映射
- `get_if_exists=True` 保证全局唯一

---

## 四、`actor.get_location.remote()` 的异步回调机制

### 核心问题：`actor.get_location.remote()` 怎么做到"异步但最终回调"？

答案在于 **调度循环（scheduling loop）** 的轮询机制。整个流程是事件驱动 + 轮询的混合模式。

### 完整生命周期时序图

```
时间 ──────────────────────────────────────────────────────────────────────►

Driver 侧                                                   Worker 侧
──────────                                                  ──────────

start()
  │
  ├─ self._actor_cls = ray.remote(...)(MapWorker)
  ├─ self._actor_pool.scale(delta=initial_size)
  │    └─ _create_actor()
  │         └─ _start_actor(labels, logical_actor_id)
  │              │
  │              ├─ actor = _actor_cls.options().remote(...)  ──────►  Ray GCS 创建 Actor
  │              │                                              _MapWorker.__init__() 执行：
  │              │                                                - DataContext 设置
  │              │                                                - UDF 初始化（含重试）
  │              │                                                - actor_location_tracker
  │              │                                                  .update_actor_location
  │              │                                                  .remote(id, node_id)
  │              │
  │              ├─ res_ref = actor.get_location.remote()  ──────►  _MapWorker.get_location()
  │              │                                              return ray.get_runtime_context()
  │              │                                                     .get_node_id()
  │              │
  │              ├─ add_pending_actor(actor, res_ref)
  │              │    └─ _pending_actors[res_ref] = actor
  │              │
  │              └─ _submit_metadata_task(res_ref, callback)
  │                   └─ _metadata_tasks[idx] = MetadataOpTask(
  │                         object_ref=res_ref,
  │                         task_done_callback=callback)
  │
  ▼ (不阻塞，立即返回)

═════════════════ 调度循环（streaming_executor_state.py）═══════════════════

while 执行未完成:
  │
  ├─ 收集所有 active_tasks
  │    for op in topology:
  │      for task in op.get_active_tasks():  # ← 包含 MetadataOpTask 和 DataOpTask
  │        active_tasks[task.get_waitable()] = (state, task)
  │
  ├─ ray.wait(active_tasks.keys(), timeout=0.1)
  │    ↑ 这里会检查 res_ref 是否 ready
  │
  ├─ 如果 res_ref ready（actor 已启动完成）:
  │    task 是 MetadataOpTask
  │    └─ task.on_task_finished()
  │         └─ _task_done_callback(res_ref)
  │              └─ _actor_pool.pending_to_running(res_ref)
  │                   ├─ actor_location = ray.get(ready_ref)  # 此时必不阻塞
  │                   └─ _running_actors[actor] = _ActorState(
  │                         location=actor_location,  # ✅ 记录节点 ID
  │                         num_tasks_in_flight=0)
  │
  ├─ 现在有了 running actor，可以提交 data task：
  │    _try_schedule_tasks_internal()
  │      └─ actor.submit.options(num_returns="streaming").remote(...)
  │
  ▼ 继续循环...
```

### 异步回调的关键代码拆解

**第 1 步：发出请求，注册到 pending**

```python
# actor_pool_map_operator.py:298-337
def _start_actor(self, labels, logical_actor_id):
    actor = self._actor_cls.options(_labels=...).remote(...)
    res_ref = actor.get_location.remote()

    # ❶ 把 actor 放入 pending 池，key 就是 res_ref
    #    _pending_actors = { res_ref: actor_handle }

    # ❷ 注册回调 — 当 res_ref ready 时做什么
    def _task_done_callback(res_ref):
        has_actor = self._actor_pool.pending_to_running(res_ref)

    # ❸ 把 res_ref 包装成 MetadataOpTask，交给调度循环监控
    self._submit_metadata_task(res_ref, lambda: _task_done_callback(res_ref))
    return actor, res_ref
```

**第 2 步：`_submit_metadata_task` 把 ObjectRef 注册到调度循环**

```python
# map_operator.py:644-658
def _submit_metadata_task(self, result_ref, task_done_callback):
    task_index = self._next_metadata_task_idx
    self._next_metadata_task_idx += 1

    self._metadata_tasks[task_index] = MetadataOpTask(
        task_index,
        object_ref=result_ref,        # ← 就是 res_ref
        task_done_callback=task_done_callback  # ← 就是 pending_to_running 的回调
    )
```

**第 3 步：调度循环中的轮询检测**

```python
# streaming_executor_state.py:504-643
# 收集所有 task 的 waitable（ObjectRef / ObjectRefGenerator）
active_tasks = {}
for op, state in topology.items():
    for task in op.get_active_tasks():  # ← 返回 metadata_tasks + data_tasks
        active_tasks[task.get_waitable()] = (state, task)

# ray.wait 一次检查所有 task 是否 ready
ready, _ = ray.wait(
    list(active_tasks.keys()),
    num_returns=len(active_tasks),
    fetch_local=False,
    timeout=0.1,  # 100ms 超时，非阻塞
)

# 对 ready 的 task 进行分类处理
for ref in ready:
    state, task = active_tasks[ref]
    if isinstance(task, DataOpTask):
        # 数据任务：处理 streaming generator
        ...
    else:
        # ❹ MetadataOpTask：直接调用 on_task_finished
        non_data_tasks.append((state, task))

# 立即处理 MetadataOpTask
for state, task in non_data_tasks:
    task.on_task_finished()
    # → _task_done_callback()
    # → _actor_pool.pending_to_running(res_ref)
```

**第 4 步：`pending_to_running` — actor 正式加入运行池**

```python
# actor_pool_map_operator.py:1258-1290
def pending_to_running(self, ready_ref):
    if ready_ref not in self._pending_actors:
        return False  # actor 已被 kill

    actor = self._pending_actors.pop(ready_ref)  # 从 pending 移除

    try:
        # ✅ 此时 res_ref 已经 ready，ray.get 不阻塞
        actor_location = ray.get(ready_ref)
    except Exception:
        self._actor_to_logical_id.pop(actor, None)
        raise  # actor 初始化失败

    # ❺ actor 正式进入 running 池，可以接受任务了
    self._running_actors[actor] = _ActorState(
        num_tasks_in_flight=0,
        actor_location=actor_location,  # "node:xxx-xxx-xxx" 格式
        is_restarting=False,
    )
    return True
```

---

## 五、任务提交流程 — `actor.submit.options(num_returns="streaming").remote(...)`

### 步骤 1：选择 Actor — `_ActorTaskSelectorImpl.select_actors()`

```python
def select_actors(self, input_queue, actor_locality_enabled, strict):
    while input_queue:
        bundle = input_queue.peek_next()
        available_actors = self._actor_pool.schedulable_actors()
        if not available_actors:
            return

        # 根据 locality + load 对 actor 排名
        ranks = self._rank_actors(available_actors, bundle if actor_locality_enabled else None)

        # 选择排名最高的 actor（locality 优先，负载其次）
        target_actor = available_actors[min(range(len(available_actors)), key=lambda i: ranks[i])]

        input_queue.remove(bundle)
        yield bundle, target_actor
```

### 步骤 2：排名策略 `_rank_actors()` — Locality 感知调度

```python
def _rank_actors(self, actors, bundle):
    # 获取 bundle 中各 ObjectRef 所在节点的优先级
    locs_priorities = {
        node_id: -total_bytes  # 字节数越多 → 优先级越高（负数，值越小越优先）
        for node_id, total_bytes in bundle.get_preferred_object_locations().items()
    } if bundle is not None else {}

    ranks = [
        (
            locs_priorities.get(actor_state.actor_location, INT32_MAX),  # locality 排名
            actor_state.num_tasks_in_flight,                              # load 排名
        )
        for actor in actors
    ]
    return ranks
```

**排名策略：** 先按 locality（数据所在节点匹配度），再按负载（in-flight task 数）。如果 actor 的 `actor_location` 与输入数据在同一节点，rank 值更小（更优先），从而减少网络传输。

### 步骤 3：提交任务

```python
def _try_schedule_tasks_internal(self, strict: bool) -> int:
    for bundle, actor in self._actor_task_selector.select_actors(
        self._bundle_queue, self._actor_locality_enabled, strict=strict,
    ):
        input_blocks = [block for block, _ in bundle.blocks]
        self._actor_pool.on_task_submitted(actor)  # num_tasks_in_flight += 1

        ctx = TaskContext(task_idx=self._next_data_task_idx, ...)

        # 核心：提交任务到 actor
        gen = actor.submit.options(
            num_returns="streaming",           # 流式返回
            **self._ray_actor_task_remote_args, # retry_exceptions, backpressure 等
        ).remote(
            self.data_context, ctx, *input_blocks,
            slices=bundle.slices,
            **self.get_map_task_kwargs(),
        )

        # 注册完成回调
        self._submit_data_task(gen, bundle, partial(_task_done_callback, actor))
```

### 步骤 4：`num_returns="streaming"` 的含义

`num_returns="streaming"` 不是返回固定数量的 ObjectRef，而是返回一个 `ObjectRefGenerator`：

```python
# actor.py:2187-2192
if num_returns == STREAMING_GENERATOR_RETURN:
    assert len(object_refs) == 1
    generator_ref = object_refs[0]
    return ObjectRefGenerator(generator_ref, worker)
```

**`ObjectRefGenerator` 工作机制：**
- actor 端的 `submit()` 方法是一个 generator（`yield from _map_task(...)`）
- 每当 actor yield 一个 block，driver 端就可以通过 `gen._next_sync(timeout=0)` 拿到对应的 ObjectRef
- 不需要等所有 block 都处理完，实现了流式处理

### 步骤 5：`submit_actor_task` 的底层路径

```
actor.submit.options(num_returns="streaming").remote(...)
    │
    ▼
ActorMethod._remote()           # actor.py:793
    │
    ▼
dst_actor._actor_method_call()  # actor.py:2080
    │
    ▼
worker.core_worker.submit_actor_task()  # C++ core_worker
    │
    ├── 序列化 function_descriptor + args
    ├── 目标：self._ray_actor_id（actor 的唯一 ID）
    ├── 提交到 Ray Core 的任务队列
    └── Raylet 将任务路由到 actor 所在节点的 worker 进程
```

**关键：** `self._ray_actor_id` 是 actor 创建时由 GCS 分配的全局唯一 ID。Ray Core 内部维护了 `actor_id → node_id` 的映射，所以提交任务时不需要显式指定目标节点——Ray Core 自动将任务路由到 actor 所在的节点。

---

## 六、如何找到 Actor 在哪个节点上

有三种方式，适用于不同场景：

### 方式 1：`_ActorState.actor_location`（运行时调度用）

```python
self._running_actors[actor] = _ActorState(
    num_tasks_in_flight=0,
    actor_location=actor_location,  # ← 由 actor.get_location() 返回
    is_restarting=False,
)
```

`_MapWorker.get_location()` 的实现：

```python
def get_location(self) -> NodeIdStr:
    return ray.get_runtime_context().get_node_id()  # actor 进程所在节点 ID
```

这个值在 **locality 感知调度**中被使用（见步骤 2 的 `_rank_actors`）。

### 方式 2：ActorLocationTracker（全局注册表）

用于跨 operator 的 locality 感知（比如下游 operator 要找上游 actor 的位置），而 `_ActorState.actor_location` 用于同一 operator 内的调度。

### 方式 3：Ray Core 内部（提交任务时不需要手动查找）

当你调用 `actor.submit.remote(...)` 时，不需要知道 actor 在哪个节点，因为：
1. actor 是 `ActorHandle`，内部持有 `_ray_actor_id`
2. `submit_actor_task()` 将任务提交到 GCS/Raylet
3. Raylet 内部维护 `actor_id → node_id` 映射
4. 任务自动被路由到 actor 所在节点的 worker 进程

---

## 七、完整数据流总结

```
输入 RefBundle
    │
    ▼
_bundle_queue.add(bundle)         # 入队等待调度
    │
    ▼
_actor_task_selector.select_actors()  # 选择最佳 actor
    │  ├── 过滤：schedulable_actors()（排除 restarting/draining/满载 actor）
    │  ├── 排名：_rank_actors()（locality 优先 → 负载其次）
    │  └── 返回：(bundle, target_actor) 对
    │
    ▼
actor.submit.options(num_returns="streaming").remote(...)
    │  → worker.core_worker.submit_actor_task(actor_id, ...)
    │  → Ray Core 自动路由到 actor 所在节点
    │  → 返回 ObjectRefGenerator（流式）
    │
    ▼
_submit_data_task(gen, bundle, callback)
    │  → 创建 DataOpTask
    │  → 存入 self._data_tasks
    │
    ▼
调度循环: ray.wait(active_tasks)
    │  → gen ready → DataOpTask.on_data_ready()
    │  → 逐个读出 block + metadata
    │  → 组成 RefBundle 输出
    │
    ▼
任务完成: _task_done_callback(actor)
    │  → actor_pool.on_task_completed(actor)
    │  → num_tasks_in_flight -= 1
    │  → actor 可重新被调度
```

---

## 八、ActorHandle 与 Ray Core 的 Actor ↔ Node 映射

### Python 侧 ActorHandle 的关键字段

```python
class ActorHandle:
    _ray_actor_id: ActorID           # actor 的全局唯一 ID
    _ray_actor_language              # 语言 (Python/Java/CPP)
    _ray_method_signatures           # 方法签名
    _ray_original_handle: bool       # 是否是原始 handle（控制生命周期）
```

**关键：** Python 侧的 `ActorHandle` 只持有 `actor_id`，不持有 `node_id` 或网络地址。网络地址存储在 C++ 侧。

### C++ 侧 ActorHandle

```cpp
class ActorHandle {
    ActorID actor_id_;
    rpc::Address owner_address_;  // 创建者（owner）的地址
    ObjectID actor_cursor_;       // 追踪 actor 的返回值顺序
};
```

C++ 侧的 `ActorHandle` 也只有 `owner_address`，没有 actor 自身的地址。

### Actor ↔ Node 映射存储位置

映射存储在 `ActorTaskSubmitter::ClientQueue` 中：

```cpp
struct ClientQueue {
    rpc::ActorTableData::ActorState state_ = DEPENDENCIES_UNREADY;
    int64_t num_restarts_ = -1;
    std::optional<rpc::Address> client_address_;  // ← actor 的实际网络地址
    std::string worker_id_;                        // ← actor 的 worker ID
};

absl::flat_hash_map<ActorID, ClientQueue> client_queues_;
```

`client_address_` 包含：`ip_address()`、`port()`、`node_id()`、`worker_id()`。

---

## 九、Actor 创建的完整链路（6 个阶段）

```
┌──────────────────────────────────────────────────────────────────┐
│                    Driver (Python 进程)                           │
│                                                                  │
│  ray.remote(...)(MyClass).remote(...)                           │
│       │                                                          │
│       ▼                                                          │
│  core_worker.CreateActor()          ← C++ core_worker.cc:2257   │
│       │  1. 生成 ActorID = ActorID::Of(job_id, task_id, index)  │
│       │  2. 构建 ActorCreationTaskSpec                           │
│       │  3. 创建 C++ ActorHandle，存入 ActorManager              │
│       │  4. ActorManager.AddActorHandle()                        │
│       │     → ActorTaskSubmitter.AddActorQueueIfNotExists()      │
│       │       此时 ClientQueue 创建，但 client_address_ = 空      │
│       │  5. 返回 actor_id → Python ActorHandle                  │
│       ▼                                                          │
│  ActorTaskSubmitter.SubmitActorCreationTask()                    │
│       │  → 发送到 GCS                                            │
└───────┼──────────────────────────────────────────────────────────┘
        │ gRPC
        ▼
┌──────────────────────────────────────────────────────────────────┐
│                    GCS Server                                     │
│                                                                  │
│  GcsActorManager.RegisterActor()    ← gcs_actor_manager.cc:308   │
│       │  1. 持久化 actor 元信息到存储                              │
│       │  2. PublishActor() → 通知 owner 注册成功                  │
│       ▼                                                          │
│  GcsActorManager.CreateActor()      ← gcs_actor_manager.cc:798   │
│       │                                                          │
│       ▼                                                          │
│  GcsActorScheduler.Schedule()       ← gcs_actor_scheduler.cc:49  │
│       │  1. 根据 scheduling_strategy 选择目标节点                 │
│       │  2. 调用 LeaseWorkerFromNode()                            │
│       ▼                                                          │
│  LeaseWorkerFromNode()              ← gcs_actor_scheduler.cc:234 │
│       │  → raylet_client.RequestWorkerLease()                    │
│       │    发送到目标节点的 raylet                                 │
└───────┼──────────────────────────────────────────────────────────┘
        │ gRPC
        ▼
┌──────────────────────────────────────────────────────────────────┐
│                    Raylet (目标节点)                               │
│                                                                  │
│  接收 RequestWorkerLease 请求                                     │
│       │  1. 检查本地资源是否足够                                   │
│       │  2. 如果不够 → spillback 到其他节点                        │
│       │  3. 如果够 → 分配一个 worker                               │
│       │  4. 回复 GCS: worker_address(node_id, ip, port, pid)     │
│       ▼                                                          │
└───────┼──────────────────────────────────────────────────────────┘
        │ 回复 gRPC
        ▼
┌──────────────────────────────────────────────────────────────────┐
│                    GCS Server (继续)                              │
│                                                                  │
│  HandleWorkerLeaseGrantedReply()    ← gcs_actor_scheduler.cc:296 │
│       │  1. 拿到 worker_address                                  │
│       │  2. 更新 actor 的 address: actor->UpdateAddress()        │
│       │  3. CreateActorOnWorker()                                │
│       │     → worker_client.PushNormalTask(actor_creation_spec)  │
│       │       直接把创建任务推到目标 worker 执行                   │
└───────┼──────────────────────────────────────────────────────────┘
        │ gRPC (PushNormalTask)
        ▼
┌──────────────────────────────────────────────────────────────────┐
│                    Worker 进程 (目标节点)                          │
│                                                                  │
│  执行 Actor Creation Task                                         │
│       │  1. 反序列化 ActorHandle                                  │
│       │  2. 实例化 actor 对象（__init__）                         │
│       │  3. 回复 GCS: PushTaskReply (成功)                       │
└───────┼──────────────────────────────────────────────────────────┘
        │ 回复 gRPC
        ▼
┌──────────────────────────────────────────────────────────────────┐
│                    GCS Server (最后阶段)                           │
│                                                                  │
│  schedule_success_handler_()        ← gcs_actor_manager.cc:1640  │
│       │  1. actor->UpdateState(ALIVE)                            │
│       │  2. 记录 actor → (node_id, worker_id) 映射               │
│       │  3. gcs_publisher_->PublishActor()                       │
│       │     发布 actor 状态变更通知                                │
│       │  4. RunAndClearActorCreationCallbacks()                   │
│       │     回复 owner 的 CreateActor RPC                         │
└───────┼──────────────────────────────────────────────────────────┘
        │ GCS Pub/Sub 通知
        ▼
┌──────────────────────────────────────────────────────────────────┐
│                    Driver (回到起点)                               │
│                                                                  │
│  ActorManager.HandleActorStateNotification()                      │
│       │  actor_manager.cc:231                                    │
│       │  收到: actor_data.state() == ALIVE                       │
│       │        actor_data.address() = (node_id, ip, port, ...)   │
│       ▼                                                          │
│  ActorTaskSubmitter.ConnectActor()                                │
│       │  actor_task_submitter.cc:293                             │
│       │  1. queue->state_ = ALIVE                                │
│       │  2. queue->client_address_ = address  ← 写入映射！       │
│       │  3. queue->worker_id_ = address.worker_id()              │
│       │  4. SendPendingTasks() → 开始发送积压的 actor task       │
│       ▼                                                          │
│  现在 actor_id → (node_id, ip, port) 映射已建立 ✅                 │
│  后续 SubmitTask() 就能通过这个地址直接推任务了                     │
└──────────────────────────────────────────────────────────────────┘
```

---

## 十、Actor 创建成功后怎么通知 Driver

有两条并行的通知路径：

### 路径 1：GCS Pub/Sub → ActorManager（驱动 ClientQueue 连接）

```
GCS: PublishActor() → gcs_publisher
         │
         ▼ (pub/sub)
Driver: gcs_client_->Actors().AsyncSubscribe(actor_id, callback)
         │
         ▼
ActorManager.HandleActorStateNotification(actor_id, actor_data)
         │
         ├── state == ALIVE → ConnectActor(actor_id, address)
         │     → client_queues_[actor_id].client_address_ = address
         │     → SendPendingTasks()  // 发送积压任务
         │
         ├── state == RESTARTING → DisconnectActor()
         │
         └── state == DEAD → OnActorKilled() + DisconnectActor()
```

这个订阅在 `AddActorHandle` 时就注册了（actor_manager.cc:197），是自动的。

### 路径 2：CreateActor RPC Reply（驱动 actor creation task 的 ObjectRef 完成）

```
GCS: RunAndClearActorCreationCallbacks()
         │
         ▼ (gRPC reply)
Driver: core_worker 收到 CreateActorReply
         │
         ▼
actor_creation_return_id 对应的 ObjectRef 变为 ready
```

但在 Ray Data 的场景中，Driver 并不直接等待这个 ref，而是用 `actor.get_location.remote()` 的 ObjectRef 作为 "actor 就绪" 信号。

---

## 十一、`actor.get_location.remote()` 在 CoreWorker 中的暂存与延迟提交机制

### 核心结论

`actor.get_location.remote()` 调用后，任务先被暂存在本地 CoreWorker 的 `ActorTaskSubmitter::ClientQueue` 中，等 GCS 通知 actor 创建成功（`ConnectActor`）后，才会被真正发送出去。

### 第 1 步：创建 Actor 时就建立了 ClientQueue

```cpp
// core_worker.cc:2371 — CreateActor() 中
actor_manager_->EmplaceNewActorHandle(std::move(actor_handle), ...)

    ↓ 内部调用

// actor_manager.cc:178 — AddActorHandle() 中
actor_task_submitter_.AddActorQueueIfNotExists(
    actor_id, max_pending_calls, allow_out_of_order_execution, ...)

    ↓ 内部实现

// actor_task_submitter.cc:65
client_queues_.emplace(
    actor_id,
    ClientQueue(...)    // ← 此时 ClientQueue 创建了
                        //   state_ = DEPENDENCIES_UNREADY
                        //   client_address_ = std::nullopt  ← 还没有地址！
)
```

### 第 2 步：`actor.get_location.remote()` 调用 — 任务入队暂存

```
actor.get_location.remote()
    → ActorMethod._remote()
    → dst_actor._actor_method_call()
    → worker.core_worker.submit_actor_task(actor_id, ...)
```

C++ 中 `submit_actor_task`：

```cpp
// 1. 确认 actor 存在（ClientQueue 已创建）
if (!actor_task_submitter_->CheckActorExists(actor_id)) { ... }

// 2. 订阅 GCS actor 状态变更通知
actor_manager_->SubscribeActorState(actor_id);

// 3. 构建 TaskSpecification
actor_handle->SetActorTaskSpec(builder, ...);
TaskSpecification task_spec = std::move(builder).ConsumeAndBuild();

// 4. ★ 关键：注册为 pending task，返回 ObjectRef 给 Python
returned_refs = task_manager_->AddPendingTask(rpc_address_, task_spec, ...);

// 5. ★ 关键：提交到 ActorTaskSubmitter
actor_task_submitter_->SubmitTask(task_spec);
```

**`AddPendingTask` 做了什么：**
- 为返回值创建 ObjectID（但值还不存在，只是预分配 ID）
- 在 `reference_counter_` 中注册这个 owned object
- 把 task 记录到 `submissible_tasks_` 中，状态为 PENDING_ARGS_AVAIL
- 返回 `rpc::ObjectReference` 给 Python → 这就是 Python 拿到的 ObjectRef

### 第 3 步：`SubmitTask` — 任务进入 ClientQueue 的排队机制

```cpp
void ActorTaskSubmitter::SubmitTask(TaskSpecification task_spec) {
    auto actor_id = task_spec.ActorId();
    absl::MutexLock lock(&mu_);
    auto queue = client_queues_.find(actor_id);

    if (queue->second.state_ != rpc::ActorTableData::DEAD) {
        send_pos = task_spec.SequenceNumber();
        queue->second.actor_submit_queue_->Emplace(send_pos, task_spec);  // ← 入队！
        queue->second.cur_pending_calls_++;
        task_queued = true;
    }
}
```

### 第 4 步：`SendPendingTasks` — 检查地址，没有地址就不发

```cpp
void ActorTaskSubmitter::SendPendingTasks(const ActorID &actor_id) {
    auto &client_queue = it->second;

    if (!client_queue.client_address_.has_value()) {
        // 没有地址 → 直接返回，任务继续留在队列中
        return;
    }

    // 有地址 → 从队列中逐个取出任务，推送到 actor
    while (true) {
        auto task = actor_submit_queue->PopNextTaskToSend();
        if (!task.has_value()) break;
        PushActorTask(client_queue, task->first, task->second);
    }
}
```

### 第 5 步：GCS 通知 Actor ALIVE → `ConnectActor` 填充地址 → 触发发送

```cpp
void ActorTaskSubmitter::ConnectActor(const ActorID &actor_id,
                                      const rpc::Address &address,
                                      int64_t num_restarts) {
    absl::MutexLock lock(&mu_);

    queue->second.state_ = rpc::ActorTableData::ALIVE;
    queue->second.worker_id_ = address.worker_id();
    queue->second.client_address_ = address;   // ← 终于有地址了！

    // ★ 触发发送所有积压任务
    SendPendingTasks(actor_id);
}
```

此时 `SendPendingTasks` 再执行，`client_address_` 已有值，于是：
```
while (task = queue.PopNextTaskToSend()) {
    PushActorTask(queue, task)
    //         ↓
    //  core_worker_client_pool_.GetOrConnect(addr)->PushActorTask(request)
    //  → gRPC 直连 actor worker 发送任务
}
```

### 第 6 步：Actor 执行任务，ObjectRef 变 ready

当 actor worker 执行完 `get_location()`，把返回值写入 Object Store：
- `task_manager_->CompletePendingTask()` 被调用
- Python 侧的 ObjectRef 变为 ready

### 暂存机制全景图

```
actor.get_location.remote()
       │
       ▼ Python → C++

core_worker.submit_actor_task()
       │
       ├── task_manager_.AddPendingTask()
       │     → 创建 return ObjectID（占位，值还不存在）
       │     → 返回 ObjectRef 给 Python ← 这就是 res_ref
       │     → task 记入 submissible_tasks_，状态 PENDING
       │
       └── actor_task_submitter_->SubmitTask(task_spec)
             │
             ├── queue.actor_submit_queue_.Emplace(seq_no, task_spec)
             │     → 任务入队到 btree_map 中
             │
             └── resolver_.ResolveDependencies()
                   │
                   ▼
             MarkDependencyResolved() → SendPendingTasks()
                   │
                   ├── client_address_ 为空？→ 直接 return，任务留在队列
                   │
                   └── client_address_ 有值？→ PopNextTaskToSend() → PushActorTask()

     ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─

     GCS 通知: ActorState == ALIVE, address = (ip, port, node_id)
         │
         ▼
     ActorManager.HandleActorStateNotification()
         │
         ▼
     ActorTaskSubmitter.ConnectActor(actor_id, address)
         │
         ├── queue.client_address_ = address    ← 填充地址
         ├── queue.state_ = ALIVE
         └── SendPendingTasks(actor_id)
               │
               │  现在 client_address_ 有值了！
               ▼
               while (task = queue.PopNextTaskToSend()) {
                   PushActorTask(queue, task)   ← gRPC 发到 actor
               }
               │
               ▼ Actor 执行 get_location()，返回 node_id
               │
               ▼ task_manager_.CompletePendingTask()
               │
               ▼ Python 侧的 res_ref 变 ready
```

### 关键数据结构总结

| 组件 | 数据结构 | 存什么 | 何时填充 |
|------|----------|--------|----------|
| `ActorTaskSubmitter.client_queues_` | `map<ActorID, ClientQueue>` | actor 的连接状态+地址 | `AddActorQueueIfNotExists` 时创建空队列；`ConnectActor` 时填充地址 |
| `ClientQueue.actor_submit_queue_` | `btree_map<seq_no, (TaskSpec, deps_resolved)>` | 等待发送的 actor task | `SubmitTask` 时入队；`SendPendingTasks` 时出队 |
| `ClientQueue.client_address_` | `optional<rpc::Address>` | actor worker 的 IP+Port+NodeID | `ConnectActor` 时填充 |
| `TaskManager.submissible_tasks_` | `map<TaskID, ...>` | 所有 pending task 的元信息 | `AddPendingTask` 时注册；任务完成时移除 |

**一句话总结：** `actor.get_location.remote()` 时，任务立即被序列化为 `TaskSpecification`，存入本地 `ActorTaskSubmitter` 的 `ClientQueue.actor_submit_queue_` 中。由于此时 `client_address_` 为空（actor 还没创建好），`SendPendingTasks()` 检测到无地址就直接返回。等 GCS 通过 pub/sub 通知 actor 已 ALIVE，`ConnectActor()` 填充地址后再调用 `SendPendingTasks()`，此时才真正把积压的任务通过 gRPC 推送到 actor worker。

---

## 十二、Actor 失败重建的完整代码逻辑

### 总览：三层协作

| 层级 | 组件 | 职责 |
|------|------|------|
| **Ray Data 层 (Python)** | `ActorPoolMapOperator` / `_ActorPool` | 不主动重建 actor，依赖 Ray Core 的 fault tolerance；通过 `refresh_actor_state()` 感知 RESTARTING 状态；标记 `is_restarting=True`，暂停向该 actor 调度任务 |
| **Ray Core Driver 层 (C++)** | `ActorManager` / `ActorTaskSubmitter` | 收到 GCS 的 RESTARTING 通知；断开旧连接，inflight task 失败并重试；收到 ALIVE 通知后重新连接，发送积压任务 |
| **GCS Server 层 (C++)** | `GcsActorManager` / `GcsActorScheduler` | 检测 worker/node 失败；执行 `RestartActor()`：更新状态→持久化→重新调度；重新 lease worker → 在新 worker 上执行创建任务 |

### 第一阶段：失败检测

有三种失败场景，都入口到 `GcsActorManager`：

**场景 A：Worker 进程崩溃**

```cpp
void GcsActorManager::OnWorkerDead(node_id, worker_id, ...) {
    bool need_reconstruct =
        disconnect_type != rpc::WorkerExitType::INTENDED_USER_EXIT &&
        disconnect_type != rpc::WorkerExitType::USER_ERROR;

    // 销毁该 worker 拥有的子 actor（owner 死了，子 actor 也得死）
    for (const auto &child_id : children_ids) {
        DestroyActor(child_id, GenOwnerDiedCause(...));
    }

    // 查找该 worker 上运行的 actor
    auto iter = created_actors_.find(node_id);
    if (iter != created_actors_.end() && iter->second.count(worker_id)) {
        actor_id = iter->second[worker_id];
    }

    RestartActor(actor_id, need_reschedule=need_reconstruct, death_cause);
}
```

**场景 B：整个节点宕机**

```cpp
void GcsActorManager::OnNodeDead(node, node_ip_address) {
    // 销毁该节点上 owner 拥有的子 actor
    for (const auto &[owner_id, child_id] : children_ids) {
        DestroyActor(child_id, GenOwnerDiedCause(...));
    }

    // 取消正在该节点上调度的 actor
    auto scheduling_actor_ids = gcs_actor_scheduler_->CancelOnNode(node_id);

    // 对取消调度的 actor 尝试重建
    for (auto &actor_id : scheduling_actor_ids) {
        RestartActor(actor_id, need_reschedule=true, GenNodeDiedCause(...));
    }

    // 对已在该节点上运行的 actor 尝试重建
    for (auto &entry : created_actors) {
        RestartActor(entry.second, need_reschedule=true, GenNodeDiedCause(...));
    }
}
```

**场景 C：Actor 创建任务失败**

在 `gcs_actor_scheduler.cc:418`，当 `PushNormalTask` 回调失败时，也会触发重建。

### 第二阶段：GCS 执行 RestartActor

```cpp
void GcsActorManager::RestartActor(const ActorID &actor_id,
                                    bool need_reschedule,
                                    const rpc::ActorDeathCause &death_cause,
                                    std::function<void()> done_callback) {
    auto &actor = registered_actors_[actor_id];

    int64_t max_restarts = mutable_actor_table_data->max_restarts();  // Ray Data 默认 -1（无限）
    uint64_t num_restarts = mutable_actor_table_data->num_restarts();

    if (!need_reschedule) {
        remaining_restarts = 0;    // 不重建
    } else if (max_restarts == -1) {
        remaining_restarts = -1;   // 无限重建
    } else {
        remaining_restarts = max(0, max_restarts - (num_restarts - num_preemption_restarts));
    }

    if (remaining_restarts != 0) {
        num_restarts += 1;
        actor->UpdateState(rpc::ActorTableData::RESTARTING);
        actor->UpdateAddress(rpc::Address());  // 清空旧地址

        gcs_table_storage_->ActorTable().Put(actor_id, *mutable_actor_table_data, {
            gcs_publisher_->PublishActor(actor_id, ...);   // 发布 RESTARTING 状态通知
            gcs_actor_scheduler_->Schedule(actor);         // 重新调度
        });
    } else {
        actor->UpdateState(rpc::ActorTableData::DEAD);
        gcs_publisher_->PublishActor(actor_id, ...);
    }
}
```

**关键：** Ray Data 默认设置 `max_restarts=-1`，意味着 actor 可以无限重建。

### 第三阶段：GCS 重新调度 Actor

```cpp
void GcsActorScheduler::Schedule(std::shared_ptr<GcsActor> actor) {
    RAY_CHECK(actor->GetNodeID().IsNil() && actor->GetWorkerID().IsNil());
    // ↑ RestartActor 中已清空了地址，所以满足这个断言

    auto node_id = SelectForwardingNode(actor);
    rpc::Address address;
    address.set_node_id(node.value()->node_id());
    actor->UpdateAddress(address);

    LeaseWorkerFromNode(actor, node.value());
    // → raylet_client->RequestWorkerLease(...)
    // → raylet 分配新 worker
    // → 回复 worker_address
    // → CreateActorOnWorker(actor, worker)
    // → worker_client->PushNormalTask(actor_creation_task_spec)
    // → 新 worker 上执行 _MapWorker.__init__()
}
```

### 第四阶段：Driver 侧收到 RESTARTING 通知

```
GCS PublishActor(state=RESTARTING)
    ↓ (pub/sub)
CoreWorker: ActorManager.HandleActorStateNotification()
```

```cpp
if (actor_data.state() == rpc::ActorTableData::RESTARTING) {
    actor_task_submitter_.DisconnectActor(
        actor_id,
        actor_data.num_restarts(),
        /*dead=*/false,              // ← 不是永久死亡！
        actor_data.death_cause(),
        /*is_restartable=*/true      // ← 可重建
    );
}
```

`DisconnectActor` 内部：

```cpp
void ActorTaskSubmitter::DisconnectActor(actor_id, num_restarts, dead, death_cause, is_restartable) {
    // 1. 断开旧 RPC 连接
    DisconnectRpcClient(queue->second);
    //   queue->second.client_address_ = std::nullopt;

    // 2. 收集所有 inflight task 的回调
    inflight_task_callbacks = std::move(queue->second.inflight_task_callbacks_);

    // 3. 更新状态
    queue->second.state_ = rpc::ActorTableData::RESTARTING;
    queue->second.num_restarts_ = num_restarts;

    // 4. ★ 任务不丢弃！继续留在 actor_submit_queue_ 中
}
```

然后 inflight 的任务被标记为失败以触发重试：

```cpp
void ActorTaskSubmitter::FailInflightTasksOnRestart(inflight_task_callbacks) {
    auto status = Status::IOError("The actor was restarted");
    for (const auto &[_, callback] : inflight_task_callbacks) {
        callback(status, rpc::PushTaskReply());  // 触发重试逻辑
    }
}
```

这些 inflight task 会因为 `max_task_retries=-1`（Ray Data 默认）而被自动重试，重新进入 `actor_submit_queue_`。

### 第五阶段：Ray Data 层感知 Actor 重启

```python
def update_resource_usage(self) -> None:
    self._actor_pool.refresh_actor_state()    # 调度循环中周期性调用

def refresh_actor_state(self):
    for actor in self.get_running_actor_refs():
        actor_state = actor._get_local_state()
        if actor_state == gcs_pb2.ActorTableData.ActorState.RESTARTING:
            self._update_running_actor_state(actor, True)   # is_restarting = True
        elif actor_state == gcs_pb2.ActorTableData.ActorState.ALIVE:
            self._update_running_actor_state(actor, False)  # is_restarting = False
```

`is_restarting=True` 的 actor 不会被调度新任务：

```python
def schedulable_actors(self) -> List[ray.actor.ActorHandle]:
    available_actors = self.get_available_actors()
    return [
        actor for actor, state in available_actors.items()
        if state.num_tasks_in_flight < self.max_tasks_in_flight_per_actor()
        and not state.is_restarting   # ← RESTARTING 的 actor 被排除
    ]
```

### 第六阶段：Actor 重建成功 → 重新连接

新 worker 上 `_MapWorker.__init__()` 执行完成后，GCS 状态流转：

```
RESTARTING → ALIVE (新 address)
```

GCS 通过 `PublishActor()` 通知 Driver：

```cpp
if (actor_data.state() == rpc::ActorTableData::ALIVE) {
    actor_task_submitter_.ConnectActor(
        actor_id, actor_data.address(), actor_data.num_restarts());
}
```

`ConnectActor`：

```cpp
void ActorTaskSubmitter::ConnectActor(actor_id, address, num_restarts) {
    // 1. 跳过过时的重启版本
    if (num_restarts < queue->second.num_restarts_) return;

    // 2. 断开旧连接（如果有的话）
    if (queue->second.client_address_.has_value()) {
        DisconnectRpcClient(queue->second);
        inflight_task_callbacks = std::move(queue->second.inflight_task_callbacks_);
    }

    // 3. ★ 填充新地址
    queue->second.state_ = rpc::ActorTableData::ALIVE;
    queue->second.worker_id_ = address.worker_id();
    queue->second.client_address_ = address;  // 新的 IP:Port:NodeID

    // 4. ★ 发送所有积压的任务（包括重试的）
    SendPendingTasks(actor_id);

    // 5. 旧的 inflight task 失败重试
    FailInflightTasksOnRestart(inflight_task_callbacks);
}
```

Ray Data 层面，下一次 `refresh_actor_state()` 检测到 ALIVE：

```python
# is_restarting = False → actor 重新可调度
self._update_running_actor_state(actor, False)
```

**但还需要更新 `actor_location`：** Ray Core 的 actor 重建是透明重建——同一个 `ActorHandle`，同一个 `actor_id`——所以 `running_actors` 字典中的 key 不变，但 `actor_location` 可能过时了。`ActorLocationTracker` 会更新位置（`__init__` 中重新注册），但 `_ActorState.actor_location` 只在首次创建时设置一次，重建后可能过时（不影响正确性，locality 只是优化）。

### 完整时序图

```
                GCS                     Raylet           新 Worker       Driver (CoreWorker)        Ray Data (_ActorPool)
                 │                        │                 │                   │                          │
 ════════════════════════════════  Actor 正在运行  ═══════════════════════════════════════════════════
                 │                        │                 │                   │                          │
                 │                        │ ✗ Worker 崩溃!   │                   │                          │
                 │◄───────────────────────│─────────────────│                   │                          │
                 │  OnWorkerDead(node_id, worker_id)        │                   │                          │
                 │                        │                 │                   │                          │
 ──── GCS Phase ────────────────────────────────────────────────────────────────────────────────────
                 │                        │                 │                   │                          │
                 │ RestartActor(actor_id, need_reschedule=true)                 │                          │
                 │  ├── remaining_restarts = -1 (无限重建)    │                   │                          │
                 │  ├── num_restarts += 1                   │                   │                          │
                 │  ├── state → RESTARTING                  │                   │                          │
                 │  ├── actor.UpdateAddress(空)             │                   │                          │
                 │  ├── PublishActor(state=RESTARTING) ──────────────────────────►                          │
                 │  └── Schedule(actor)                    │                   │                          │
                 │      │                                  │                   │                          │
 ──── GCS 通知 Driver ──────────────────────────────────────────────────────────────────────────────
                 │                        │                 │                   │                          │
                 │                        │                 │  HandleActorStateNotification(state=RESTARTING)    │
                 │                        │                 │  → DisconnectActor(dead=false)                    │
                 │                        │                 │    ├── client_address_ = nullopt                 │
                 │                        │                 │    ├── state_ = RESTARTING                       │
                 │                        │                 │    ├── inflight task → FailInflightTasksOnRestart │
                 │                        │                 │    │   → callback(IOError("The actor was restarted"))
                 │                        │                 │    │   → task_manager 重试（max_task_retries=-1） │
                 │                        │                 │    └── actor_submit_queue_ 中的任务保留           │
                 │                        │                 │                   │                          │
 ──── GCS 重新调度 ──────────────────────────────────────────────────────────────────────────────────
                 │                        │                 │                   │                          │
                 │ Schedule(actor)        │                 │                   │                          │
                 │  → SelectForwardingNode│                 │                   │                          │
                 │  → LeaseWorkerFromNode │                 │                   │                          │
                 │  ── RequestWorkerLease ─►│                 │                   │                          │
                 │                        │ 分配新 worker    │                   │                          │
                 │  ◄─── worker_address ──│                 │                   │                          │
                 │  CreateActorOnWorker() │                 │                   │                          │
                 │  ──── PushNormalTask ───────────────────►│                   │                          │
                 │                        │                 │                   │                          │
                 │                        │                 │  _MapWorker.__init__() │                          │
                 │                        │                 │  ├── DataContext._set_current()                  │
                 │                        │                 │  ├── _init_udf_with_retries()                    │
                 │                        │                 │  └── actor_location_tracker.update_actor_location│
                 │                        │                 │      (注册新的 node_id) │                          │
                 │                        │                 │                   │                          │
                 │  ◄─── PushTaskReply(ok)──────────────────│                   │                          │
                 │  state → ALIVE          │                 │                   │                          │
                 │  PublishActor(state=ALIVE, new_address) ──────────────────────────►                          │
                 │                        │                 │                   │                          │
 ──── Driver 重连 ──────────────────────────────────────────────────────────────────────────────────
                 │                        │                 │                   │                          │
                 │                        │                 │  HandleActorStateNotification(state=ALIVE)        │
                 │                        │                 │  → ConnectActor(actor_id, new_address, num_restarts)
                 │                        │                 │    ├── state_ = ALIVE                             │
                 │                        │                 │    ├── client_address_ = new_address              │
                 │                        │                 │    ├── worker_id_ = new_worker_id                 │
                 │                        │                 │    └── SendPendingTasks(actor_id) ★              │
                 │                        │                 │        ├── PopNextTaskToSend() → 重试的任务       │
                 │                        │                 │        └── PushActorTask() → gRPC 发到新 worker   │
                 │                        │                 │                   │                          │
 ──── Ray Data 感知 ────────────────────────────────────────────────────────────────────────────────
                 │                        │                 │                   │                          │
                 │                        │                 │                   │  refresh_actor_state()    │
                 │                        │                 │                   │  → actor._get_local_state()
                 │                        │                 │                   │    == ALIVE               │
                 │                        │                 │                   │  → is_restarting = False ★│
                 │                        │                 │                   │  → actor 重新可调度       │
                 │                        │                 │                   │                          │
                 │                        │                 │  actor.submit.remote(...)◄──────────────────────  ← 数据任务恢复提交
                 │                        │                 │  ←── gRPC (新地址) │                          │
```

---

## 十三、Actor 初始化失败：两层防御机制

| 场景 | 第一层（Worker 内部） | 第二层（Ray Core 重建） |
|------|----------------------|------------------------|
| `__init__` 中 UDF 抛异常 | `_init_udf_with_retries` 在 worker 进程内重试 | 不重建（USER_ERROR → DEAD） |
| `__init__` 中非 UDF 代码抛异常 | 没有重试 | 不重建（USER_ERROR → DEAD） |
| `actor_init_retry_on_errors=True` 且 UDF 抛指定异常 | worker 内部重试直到成功或耗尽次数 | 耗尽后同上 |
| Worker 进程被 kill / 节点宕机 | — | 无限重建（max_restarts=-1） |

**核心区别：** `__init__` 抛异常时，GCS 认为"代码有 bug"（USER_ERROR），不重建；Worker 被 kill 时，GCS 认为"环境问题"（SYSTEM_ERROR），会重建。

### 第一层：Worker 内部重试 — `_init_udf_with_retries`

```
_MapWorker.__init__()
    │
    ├── DataContext._set_current(ctx)
    ├── self._init_udf_with_retries(ctx)     ← 第一层防线
    │       │
    │       ├── actor_init_retry_on_errors = False (默认)
    │       │   → max_retries = 0 → 只试一次，失败直接 raise
    │       │
    │       └── actor_init_retry_on_errors = True
    │           → max_retries = actor_init_max_retries (默认 3)
    │           → 循环重试，每次都调用 self._map_transformer.init()
    │           → -1 表示无限重试
    │
    └── 如果 _init_udf_with_retries 抛异常
        → __init__ 失败
        → Ray Core 捕获异常
```

### 第二层：Ray Core 处理 `__init__` 异常

**步骤 1：Worker 退出**

```cpp
if (status.IsCreationTaskError()) {
    Exit(rpc::WorkerExitType::USER_ERROR,     // ← 关键：USER_ERROR
         "Worker exits because there was an exception in the initialization method...",
         creation_task_exception_pb_bytes);
}
```

**步骤 2：Raylet 通知 GCS**

```cpp
gcs_client_.Workers().AsyncReportWorkerFailure(worker_failure_data_ptr, nullptr);
//    包含: disconnect_type=USER_ERROR, creation_task_exception
```

**步骤 3：GCS 决定不重建**

```cpp
bool need_reconstruct = disconnect_type != rpc::WorkerExitType::INTENDED_USER_EXIT &&
                        disconnect_type != rpc::WorkerExitType::USER_ERROR;
//  USER_ERROR → need_reconstruct = false

RestartActor(actor_id, need_reschedule=false, death_cause);
```

**步骤 4：RestartActor 中 actor 变为 DEAD**

```cpp
if (!need_reschedule) {
    remaining_restarts = 0;   // 不允许重建
}

// remaining_restarts == 0 的分支
actor->UpdateState(rpc::ActorTableData::DEAD);   // 永久死亡
gcs_publisher_->PublishActor(actor_id, ...);
```

**步骤 5：Driver 收到 DEAD 通知**

```cpp
if (actor_data.state() == rpc::ActorTableData::DEAD) {
    OnActorKilled(actor_id);
    actor_task_submitter_.DisconnectActor(
        actor_id, num_restarts, /*dead=*/true, death_cause, is_restartable);
}
```

`DisconnectActor(dead=true)`：

```cpp
queue->second.state_ = rpc::ActorTableData::DEAD;
queue->second.death_cause_ = death_cause;

// ★ 清空所有积压任务，标记为失败
task_ids_to_fail = queue->second.actor_submit_queue_->ClearAllTasks();
```

所有等待的任务都会收到 `RayActorError`，整个 pipeline 终止。

### 为什么 `__init__` 异常不重建，但 Worker 被 kill 会重建？

这是 Ray Core 的设计哲学：

| 退出类型 | 含义 | 决策 | 原因 |
|----------|------|------|------|
| `USER_ERROR` | `__init__` 中用户代码抛异常 | 不重建 | 代码 bug，重建也会失败，无限循环没意义 |
| `SYSTEM_ERROR` | Worker 被 kill / 网络断 / 节点宕机 | 重建 | 环境问题，换个节点可能成功 |
| `INTENDED_USER_EXIT` | 用户主动 `ray.exit_actor()` | 不重建 | 用户明确要退出 |
| `NODE_OUT_OF_MEMORY` | 节点 OOM | 不重建 | 资源不足，重建也会 OOM |

---

## 十四、关键设计总结

| 问题 | 答案 |
|------|------|
| 谁决定是否重建？ | GCS 的 `RestartActor()`，根据 `max_restarts` 和 `need_reschedule` |
| 谁执行重建？ | GCS 调度器重新 lease worker，在新 worker 上执行 `_MapWorker.__init__()` |
| ActorHandle 变不变？ | 不变。同一个 `actor_id`，同一个 Python `ActorHandle` 对象。Ray Core 透明重建 |
| 积压任务怎么处理？ | `DisconnectActor(dead=false)` 时不清空 `actor_submit_queue_`，任务等待重连后发送 |
| Inflight 任务怎么处理？ | `FailInflightTasksOnRestart()` 标记失败，`max_task_retries=-1` 触发自动重试 |
| Ray Data 层做什么？ | `refresh_actor_state()` 感知 RESTARTING，标记 `is_restarting=True` 暂停调度；ALIVE 后恢复 |
| `actor_location` 更新吗？ | `ActorLocationTracker` 会更新（`__init__` 中重新注册），但 `_ActorState.actor_location` 可能过时 |
| 默认重建几次？ | 无限次。`max_restarts=-1`，`max_task_retries=-1` |

---

## 十五、Actor 构造参数在 Object Store 中的重启失败告警

### 15.1 告警日志示例

```
core_worker.cc:2194: Actor with class name: 'MapWorker(MapBatches(FsRayActorV2))' and ID:
'd352c685f6de81d94645726d02000000' has constructor arguments in the object store and
max_restarts > 0. If the arguments in the object store go out of scope or are lost,
the actor restart will fail. See https://github.com/ray-project/ray/issues/53727
for more details.
```

这是一条 `RAY_LOG_ONCE_PER_PROCESS(ERROR)` 级别日志，**每进程只打印一次**，是一个预防性告警，不会阻止当前 Actor 的创建。

### 15.2 告警触发条件

告警代码位于 `src/ray/core_worker/core_worker.cc:2399-2435`，在 `CoreWorker::CreateActor()` 流程中。当且仅当**同时满足**以下两个条件时打印：

**条件 1：`max_restarts > 0`（或 -1 无限重启）**

```cpp
// core_worker.cc:2401
if (task_spec.MaxActorRestarts() != 0) {
```

`MaxActorRestarts()` 取自 `actor_creation_task_spec.max_actor_restarts`（`task_spec.cc:423`）。

**条件 2：构造参数"位于 object store 中"**

通过遍历所有构造参数，检查两种情况（`core_worker.cc:2403-2424`）：

```cpp
bool actor_restart_warning = false;
for (size_t i = 0; i < task_spec.NumArgs(); i++) {
    // 情况 A：参数通过 ObjectRef 传递（值在 object store 中，非内联）
    if (task_spec.ArgByRef(i)) {
        actor_restart_warning = true;
        break;
    }
    // 情况 B：内联参数中嵌套了非 detached actor 的 ObjectRef
    if (!task_spec.ArgInlinedRefs(i).empty()) {
        for (const auto &ref : task_spec.ArgInlinedRefs(i)) {
            if (!ref_is_detached_actor(ref.object_id())) {
                actor_restart_warning = true;
                break;
            }
        }
    }
    if (actor_restart_warning) {
        break;
    }
}
```

唯一豁免：嵌套 ref 指向 **detached actor**（生命周期独立、不随创建者销毁的 actor），这种情况下认为安全，不告警。

`ref_is_detached_actor` 是一个 lambda（`core_worker.cc:2393-2402`），通过 `actor_manager_->GetActorHandleIfExists()` 检查 ref 指向的 actor 是否是 detached 的：

```cpp
auto ref_is_detached_actor = [this](const std::string &object_id) {
    auto ref_object_id = ObjectID::FromBinary(object_id);
    if (ObjectID::IsActorID(ref_object_id)) {
        auto ref_actor_id = ObjectID::ToActorID(ref_object_id);
        if (auto ref_actor_handle = actor_manager_->GetActorHandleIfExists(ref_actor_id)) {
            if (ref_actor_handle->IsDetached()) {
                return true;
            }
        }
    }
    return false;
};
```

### 15.3 `ArgByRef` 与 `ArgInlinedRefs` 的底层语义

**`ArgByRef(i)` — `task_spec.cc:265`：**

```cpp
bool TaskSpecification::ArgByRef(size_t arg_index) const {
    return message_->args(arg_index).has_object_ref() &&
           !message_->args(arg_index).is_inlined();
}
```

- `has_object_ref() = true`：参数是一个 ObjectRef
- `is_inlined() = false`：对象值不在 task spec 中内联，而是存放在 object store 里
- 当参数序列化后超过内联阈值，会被 put 到 object store → `ArgByRef` = true

**`ArgInlinedRefs(i)` — `task_spec.cc:315`：**

```cpp
const std::vector<rpc::ObjectReference> TaskSpecification::ArgInlinedRefs(
    size_t arg_index) const {
    return VectorFromProtobuf<rpc::ObjectReference>(
        message_->args(arg_index).nested_inlined_refs());
}
```

- 返回内联参数内部嵌套的 ObjectRef 列表
- 当一个参数被内联（值直接存在 task spec 中），但该参数内部捕获了 ObjectRef（例如闭包中引用了某个 ObjectRef），这些嵌套 ref 会被记录在 `nested_inlined_refs` 字段中
- 这些嵌套 ref 的值仍在 object store 中，受引用计数控制

**`nested_inlined_refs` 的写入路径 — `task_util.h:104-106`：**

```cpp
class TaskArgByValue : public TaskArg {
    void ToProto(rpc::TaskArg *arg_proto) const {
        // ... 序列化 data 和 metadata ...
        for (const auto &nested_ref : value_->GetNestedRefs()) {
            arg_proto->add_nested_inlined_refs()->CopyFrom(nested_ref);
        }
    }
};
```

当参数值（`RayObject`）内部嵌套了 ObjectRef（通过 Python 序列化时 cloudpickle 的 ObjectRef reducer 产生），这些 ref 会被提取到 `nested_inlined_refs` 中。

### 15.4 "构造参数"到底指的是什么 —— Ray Data map_batches 的 Actor 创建链路

#### 用户的困惑

用户调用的是：

```python
# fs_ray_pipeline.py:1245
result_ds = ds.map_batches(
    FsRayActorV2,
    fn_constructor_kwargs=dict(config=config),
    ...
)
```

用户认为"参数"是 `FsRayActorV2.__init__(config)` 的 `config` 参数，而且认为"只有 actor task 才需要参数，actor creation 为什么会涉及参数"。

#### 实际链路：FsRayActorV2 不是直接被创建为 Ray Actor

Ray Data 在内部对 `map_batches` 做了多层封装：

**第 1 步：`fn_constructor_kwargs` 被封装进 `MapTransformer`**

```
map_batches(FsRayActorV2, fn_constructor_kwargs={"config": config})
    │
    ▼
plan_udf_map_op.py: _get_udf()
    │  FsRayActorV2 是 CallableClass
    │  → 创建 _CallableClassSpec(cls=FsRayActorV2, kwargs={"config": config})
    │  → 创建 init_fn = create_actor_context_init_fn(udf_specs=[UDFSpec(spec=callable_class_spec)])
    │     init_fn 闭包捕获了 FsRayActorV2 类定义 + config 参数
    │
    ▼
plan_udf_map_op.py:260
    map_transformer = MapTransformer([transform_fn], init_fn=init_fn)
    │  map_transformer 内部包含：
    │  - transform_fns: 数据变换函数列表
    │  - init_fn: 闭包，内部捕获 FsRayActorV2 类 + config
```

`create_actor_context_init_fn`（`plan_udf_map_op.py:587-627`）返回的 `init_fn` 在 actor 进程内执行时，会实例化 `FsRayActorV2(config)`：

```python
def init_fn():
    import ray
    if ray.data._map_actor_context is None:
        udf_instances = {}
        for spec in udf_specs:
            udf_key = spec.spec.make_key()
            if udf_key not in udf_instances:
                # ★ 这里实例化 FsRayActorV2(config)
                udf_instances[udf_key] = spec.instantiation_class(
                    *spec.spec.args, **spec.spec.kwargs  # kwargs = {"config": config}
                )
        ray.data._map_actor_context = _MapActorContext(
            is_async=has_async_udf,
            udf_instances=udf_instances,
        )
```

**第 2 步：`_MapWorker` 才是真正的 Ray Actor 类**

```python
# actor_pool_map_operator.py:252
self._actor_cls = ray.remote(**self._ray_remote_args)(self._map_worker_cls)
# _map_worker_cls = type(f"MapWorker({self.name})", (_MapWorker,), {})
# 所以日志中显示的 class name 是 'MapWorker(MapBatches(FsRayActorV2))'
```

**第 3 步：Actor 创建时传递的"构造参数"**

```python
# actor_pool_map_operator.py:316
actor = self._actor_cls.options(
    _labels={self._OPERATOR_ID_LABEL_KEY: self.id, **labels}
).remote(
    ctx=ctx,                                    # DataContext 对象
    logical_actor_id=logical_actor_id,          # 字符串
    src_fn_name=self.name,                      # 字符串
    map_transformer=self._map_transformer,     # ← 关键！内含 FsRayActorV2 + config
    actor_location_tracker=get_or_create_actor_location_tracker(),  # detached actor handle
)
```

所以 **actor creation task 的"构造参数"是 `_MapWorker.__init__` 的这 5 个参数**，而不是 `FsRayActorV2.__init__(config)` 的参数。

用户传的 `fn_constructor_kwargs={"config": config}` 并没有直接传给 actor 的 `__init__`，而是被封装在 `MapTransformer` 的 `init_fn` 闭包里。这个 `init_fn` 是 `map_transformer` 的一部分，随 `map_transformer` 一起序列化后作为构造参数传给 `_MapWorker`。

**为什么 actor creation 也需要参数？**

在 Ray 中，Actor 的创建本质就是一个特殊的 task —— **actor creation task**。这个 task 的"参数"就是传给 `actor_cls.remote(...)` 的那些参数，它们会被序列化后发送到目标 worker 执行 `__init__`。这与普通 actor task（如 `actor.submit.remote(...)`）的参数是不同的：

| | Actor Creation Task | Actor Task |
|---|---|---|
| 触发方式 | `actor_cls.remote(args)` | `actor.method.remote(args)` |
| 执行时机 | Actor 首次创建时执行一次 | Actor 创建后可多次执行 |
| 参数用途 | 传给 `__init__` | 传给对应方法 |
| 重启时 | 重新执行 `__init__`，需要重新获取参数 | 重新提交任务，需要重新获取参数 |
| 存储位置 | 参数序列化后存入 task spec（内联或 object store） | 同左 |

### 15.5 为什么 Ray Data map_batches 会触发这个告警

#### 原因 1：`max_restarts` 默认为 -1（无限重启）

`actor_pool_map_operator.py:568-571` 中 `_apply_default_remote_args` 设置了默认值：

```python
# actor_pool_map_operator.py:568-571
if "max_restarts" not in ray_remote_args:
    ray_remote_args["max_restarts"] = -1    # 无限重启
if "max_task_retries" not in ray_remote_args and ray_remote_args.get("max_restarts") != 0:
    ray_remote_args["max_task_retries"] = -1
```

用户的 `map_batches` 调用没有显式传 `max_restarts`，所以默认是 `-1`（无限重启），满足 `MaxActorRestarts() != 0`。

#### 原因 2：`map_transformer` 参数过大被存入 object store

`map_transformer` 对象包含了：
- `FsRayActorV2` 类定义
- `config`（`FsRayConfig` 完整数据类，包含所有业务参数、资源参数等）
- 所有 `transform_fns` 闭包
- `init_fn` 闭包

序列化后可能超过内联阈值 → 被存入 object store → `ArgByRef` = true → **触发告警**。

#### 原因 3（替代）：内联参数嵌套了非 detached actor 的 ObjectRef

另一种可能的触发路径：`map_transformer` 被内联到 task spec 中，但序列化后的 `map_transformer` 内部闭包捕获了某些 ObjectRef（例如 `DataContext` 中可能包含 ObjectRef），这些嵌套 ref 不是 detached actor → `ArgInlinedRefs` 非空且 `ref_is_detached_actor` 返回 false → **触发告警**。

而 `actor_location_tracker` 是一个 detached actor handle（`actor_location.py:33` 设置 `lifetime="detached"`），按理应被豁免。

#### FsRayConfig 的具体内容

从 `fs_ray_actor_v2.py` 的 `__init__` 可以看到，`FsRayConfig` 包含大量字段：

```python
class FsRayActorV2:
    def __init__(self, config: FsRayConfig):
        self._fs_kconf_key: str = config.fs_kconf_key
        self._fs_feature_name: str = config.fs_feature_name
        self._fs_feature_version: str = config.fs_feature_version
        self._fs_feature_full_version: str = config.fs_feature_full_version
        self._fs_package_root: str = config.fs_package_root
        self._media_type: str = config.media_type
        self._common_attrs: dict[str, Any] = dict(config.common_attrs)
        self._result_attr_keys: list[str] = list(config.result_attr_keys)
        self._output_map: dict[str, str] = dict(config.output_map)
        self._output_columns: list[str] = list(config.output_columns)
        self._request_batch_size: int = config.request_batch_size
        self._work_dir: str = config.work_dir
        # ... 还有更多字段
```

`FsRayConfig`（`fs_ray_config.py`）是一个包含数十个字段的数据类，涵盖 FS 协议绑定、资源并发参数、Kafka sink 配置、Actor Pool 策略等。整个 `config` + `FsRayActorV2` 类定义 + `MapTransformer` 结构序列化后，很可能超过 object store 的内联阈值。

### 15.6 为什么 Actor 重启会失败

Ray 重启 actor 时会重新执行 actor creation task（即重新执行 `_MapWorker.__init__`），需要从 object store 中 fetch 那些构造参数（`map_transformer` 等）。但：

1. **ObjectRef 的引用计数由提交 task 的 driver 进程持有**
2. **如果 driver 进程退出**（离线批处理任务结束后），引用计数归零，object store 中的对象被 GC 回收
3. **Actor 在未来某个时刻崩溃** → Ray 尝试重启 → 去 object store fetch 构造参数 → **对象已不存在 → 重启失败**

```
时间线：
────────────────────────────────────────────────────────────────────────►
                                                                    │
T1: Driver 提交 actor creation task                                 │
    └─ map_transformer 序列化后存入 object store                    │
    └─ ObjectRef 计数 = 1（driver 持有）                              │
                                                                    │
T2: Actor 创建成功，开始处理数据                                      │
                                                                    │
T3: 离线批处理完成，Driver 进程退出                                   │
    └─ ObjectRef 计数 → 0                                           │
    └─ object store GC 回收 map_transformer 对象                      │
                                                                    │
T4: Actor 崩溃（如 worker 被 kill）                                  │
    └─ GCS 触发 RestartActor                                         │
    └─ 新 worker 上重新执行 _MapWorker.__init__()                    │
    └─ 尝试 fetch map_transformer from object store                  │
    └─ ★ 对象已被 GC 回收 → fetch 失败 → actor 重启失败 ★            │
```

### 15.7 对离线批处理场景的实际影响

对于离线批处理（如 FsRayActorV2 的使用场景）：

| 场景 | Driver 状态 | 参数状态 | Actor 重启 | 影响 |
|------|-------------|----------|------------|------|
| Actor 崩溃时 driver 仍在运行 | 存活 | 仍在 object store 中 | 可成功 | 无影响 |
| Actor 崩溃时 driver 已退出 | 已退出 | 已被 GC 回收 | 失败 | 但此时整个 job 已结束，actor 重启无意义 |

**实际影响很小**：离线批处理中 actor 崩溃后 driver 通常还在，重启能成功。这条日志只是一个预防性告警，不会阻止当前 actor 创建。

### 15.8 如何消除告警

**方案 1：显式设 `max_restarts=0`（禁用 actor 重启）**

在 `map_batches` 调用时显式传入：

```python
result_ds = ds.map_batches(
    actor_cls,
    fn_constructor_kwargs=dict(config=config),
    batch_size=config.batch_size,
    concurrency=config.concurrency,
    compute=config.get_actor_pool_strategy(),
    num_cpus=config.num_cpus,
    num_gpus=config.num_gpus,
    max_restarts=0,  # ← 禁用 actor 重启，消除告警
)
```

但这会失去 actor 崩溃后自动重试的能力。如果 actor 因环境问题（如 GPU OOM、节点不稳定）崩溃，将无法自动恢复。

**方案 2：保持现状（`max_restarts=-1`），接受这条告警**

离线场景下风险很低，因为 actor 崩溃时 driver 通常还在运行。

**方案 3：将参数物化**

把 `fn_constructor_kwargs` 中的 ObjectRef `.compute()` / `ray.get()` 后再传入，使参数值内联到 task spec 而非留在 object store。但 `FsRayConfig` 本身不包含 ObjectRef，问题在于 `map_transformer` 整体过大被存入 object store，这个方案不直接适用。

### 15.9 完整触发链路总结

```
用户代码 (fs_ray_pipeline.py:1245)
│
│  ds.map_batches(FsRayActorV2, fn_constructor_kwargs={"config": config}, ...)
│
▼
Ray Data 规划层 (plan_udf_map_op.py)
│
│  _get_udf(FsRayActorV2, ..., fn_constructor_kwargs={"config": config})
│  → _CallableClassSpec(cls=FsRayActorV2, kwargs={"config": config})
│  → init_fn = create_actor_context_init_fn([UDFSpec(spec=callable_class_spec)])
│  → map_transformer = MapTransformer([transform_fn], init_fn=init_fn)
│     ↑ init_fn 闭包捕获 FsRayActorV2 类 + config
│
▼
ActorPoolMapOperator (actor_pool_map_operator.py)
│
│  start():
│    self._ray_remote_args = _apply_default_remote_args(...)
│      → max_restarts = -1 (默认无限重启)                          ★ 条件 1
│      → max_task_retries = -1
│    self._actor_cls = ray.remote(**self._ray_remote_args)(_MapWorker)
│
│  _start_actor():
│    actor = self._actor_cls.options(...).remote(
│        ctx=ctx,                              # DataContext
│        logical_actor_id=...,                 # 字符串
│        src_fn_name=self.name,                # 字符串
│        map_transformer=self._map_transformer, # ← 序列化后可能过大
│        actor_location_tracker=...,           # detached actor handle
│    )
│      │
│      ▼ Python → C++
│
▼
CoreWorker::CreateActor (core_worker.cc)
│
│  task_spec.MaxActorRestarts() = -1 ≠ 0                         ★ 条件 1 满足
│
│  遍历构造参数:
│    for i in range(NumArgs()):
│      if ArgByRef(i):                    # map_transformer 过大被存入 object store
│        actor_restart_warning = true     ★ 条件 2A 满足
│      if ArgInlinedRefs(i) 非空且非 detached actor:
│        actor_restart_warning = true     ★ 条件 2B 满足
│
│  if actor_restart_warning:
│    RAY_LOG_ONCE_PER_PROCESS(ERROR) << "Actor ... has constructor arguments
│      in the object store and max_restarts > 0. ..."
│
▼
Actor 创建继续（告警不阻止创建）
│
▼
GCS → Raylet → Worker → _MapWorker.__init__()
│
│  DataContext._set_current(ctx)
│  _init_udf_with_retries(ctx):
│    self._map_transformer.init()  → init_fn() → FsRayActorV2(config)  ← 用户代码执行
│  actor_location_tracker.update_actor_location.remote(...)
│
▼
Actor 创建成功，开始处理数据
│
│  ... 未来某时刻 Actor 崩溃 ...
│
▼
GCS RestartActor → 新 Worker 上重新执行 _MapWorker.__init__()
│
│  尝试 fetch map_transformer from object store
│  ★ 如果 driver 已退出 → 对象被 GC → fetch 失败 → 重启失败 ★
│  ★ 如果 driver 仍在   → 对象仍在     → fetch 成功 → 重启成功 ★
```
