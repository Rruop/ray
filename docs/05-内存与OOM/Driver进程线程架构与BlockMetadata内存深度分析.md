# Driver 进程线程架构与 BlockMetadata 内存深度分析

## 目录

1. [Cgroup 内存计算逻辑](#1-cgroup-内存计算逻辑)
2. [Ray CLI 参数处理](#2-ray-cli-参数处理)
3. [Driver 进程线程架构](#3-driver-进程线程架构)
4. [CPU 使用率分析与火焰图](#4-cpu-使用率分析与火焰图)
5. [ray_print_logs 日志转发开销](#5-ray_print_logs-日志转发开销)
6. [StreamingExecutor 线程模型](#6-streamingexecutor-线程模型)
7. [BlockMetadata 生命周期](#7-blockmetadata-生命周期)
8. [Reference Counter 与 Object 内存管理](#8-reference-counter-与-object-内存管理)
9. [Generator Backpressure 机制](#9-generator-backpressure-机制)
10. [百万级 Block 内存累积分析](#10-百万级-block-内存累积分析)

---

## 1. Cgroup 内存计算逻辑

### 1.1 TakeSystemMemorySnapshot 计算逻辑

**文件**: `src/ray/common/memory_monitor_utils.cc`

```cpp
const SystemMemorySnapshot MemoryMonitorUtils::TakeSystemMemorySnapshot(
    const std::string root_cgroup_path, const std::string proc_dir) {
  auto [cgroup_used_bytes, cgroup_total_bytes] = GetCGroupMemoryBytes(root_cgroup_path);
  auto [system_used_bytes, system_total_bytes] = GetLinuxMemoryBytes(proc_dir);

  // 场景1: cgroup_total > system_total (cgroup limit 未设置)
  if (cgroup_total_bytes != kNull && system_total_bytes != kNull &&
      cgroup_total_bytes > system_total_bytes) {
    if (cgroup_used_bytes != kNull) {
      return SystemMemorySnapshot{cgroup_used_bytes, system_total_bytes};
    }
  }

  // 场景2: cgroup_total <= system_total
  system_total_bytes = NullableMin(system_total_bytes, cgroup_total_bytes);
  if (system_total_bytes == cgroup_total_bytes) {
    system_used_bytes = cgroup_used_bytes;
  }
  return SystemMemorySnapshot{system_used_bytes, system_total_bytes};
}
```

### 1.2 三条计算路径

| 条件 | used | total | 含义 |
|---|---|---|---|
| `cgroup_total > system_total` | `cgroup_used` | `system_total` | cgroup limit 未设置（如 9.2EB），用 cgroup_used（排除 page cache）+ 物理内存上限 |
| `cgroup_total ≤ system_total` 且相等 | `cgroup_used` | `system_total` | cgroup limit = 系统内存，cgroup_used 更精准 |
| `cgroup_total < system_total` | `system_used` | `cgroup_total` | cgroup limit 真正生效，total 取更严格值 |

### 1.3 proc_dir 是什么

`proc_dir` 默认是 `/proc`（Linux procfs 虚拟文件系统）。`GetLinuxMemoryBytes` 从 `/proc/meminfo` 读取**宿主机级别**的 `MemTotal` 和 `MemAvailable`：

```cpp
std::tuple<int64_t, int64_t> MemoryMonitorUtils::GetLinuxMemoryBytes(
    const std::string proc_dir) {
  std::string meminfo_path = proc_dir + "/meminfo";
  // ... 读取 MemTotal, MemAvailable, MemFree, Cached, Buffers
  int64_t used_bytes = mem_total_bytes - available_bytes;
  return {used_bytes, mem_total_bytes};
}
```

**关键点**: `/proc/meminfo` 是 node 级内存统计，不感知 cgroup。在 Kubernetes 容器里读到的仍然是宿主机的 MemTotal/MemAvailable，不是该 Pod 的值（除非挂了 LXCFS 做 cgroup 感知映射）。

### 1.4 cgroup 值的含义

取决于传入的 `root_cgroup_path` 指向哪个 cgroup 层级。在 Kubernetes 里通常配置为 Pod 级 cgroup 路径（如 `/sys/fs/cgroup/kubepods/pod<uid>/`），此时读出的就是该 Pod 的 memory limit 和 usage。

### 1.5 为什么不直接用 cgroup 值

三个原因：

1. **cgroup total 不可靠**: 容器未设 `resources.limits.memory` 时，v2 的 `memory.max` 读出 "max"（解析为 0→kNull），v1 的 `limit_in_bytes` 读出 9.2EB（`LONG_MAX`），远超物理内存。

2. **cgroup used 更精准但 total 需要修正**: cgroup 的 `used = usage_in_bytes - inactive_file - active_file`，排除了可回收的 page cache，比 `/proc/meminfo` 的 `MemTotal - MemAvailable` 更接近 OOM killer 的视角。

3. **启发式判断 cgroup limit 是否生效**: 用 `cgroup_total ≤ system_total` 作为判断依据（TODO 注释承认不够好）。

### 1.6 新增 cgroup_total > system_total 分支的意义

```cpp
// 新增逻辑
if (cgroup_total_bytes > system_total_bytes && cgroup_used_bytes != kNull) {
    return SystemMemorySnapshot{cgroup_used_bytes, system_total_bytes};
}
```

不加此分支时，原有逻辑通过 `NullableMin` 正确截断了 total，但 `used` 用的是 `system_used`（宿主机级别），导致在**未设 cgroup limit 的容器**里：
- `system_used = MemTotal - MemAvailable` 是**宿主机**全局内存使用
- `cgroup_used` 才是**当前 cgroup（Pod）**的实际 working set

宿主机上其他 Pod/进程占用的内存会被误算到当前 Pod 上，**触发假的 OOM kill**。

---

## 2. Ray CLI 参数处理

### 2.1 Click 框架与 ignore_unknown_options

Ray CLI 使用 Click 框架，通过 `@cli.command()` 注册子命令。`context_settings` 控制 unknown option 行为：

- **默认行为**: Click 遇到未定义参数直接报错 `Error: no such option` 退出
- **`ignore_unknown_options=True`**: 静默丢弃未知参数

### 2.2 各命令的行为

| 命令 | ignore_unknown_options | 行为 |
|---|---|---|
| `ray start` | 否（`@cli.command()`） | 报错退出，不启动 |
| `ray stop` | 否（`@cli.command()`） | 报错退出 |
| `ray submit` | 是（`@cli.command(context_settings={"ignore_unknown_options": True})`） | 未知参数透传给用户脚本 |
| `ray up` | 否 | 报错退出 |

**代码位置**:

```python
# scripts.py:348 - ray start
@cli.command()
@click.option("--node-ip-address", ...)
def start(...):

# scripts.py:1240 - ray stop
@cli.command()
@click.option("-f", "--force", ...)
def stop(force, grace_period):

# scripts.py:1826 - ray submit
@cli.command(context_settings={"ignore_unknown_options": True})
@click.argument("cluster_config_file", required=True)
@click.argument("script", required=True)
@click.argument("script_args", nargs=-1)
def submit(...):
```

`ray submit` 设了 `ignore_unknown_options` 是因为它需要把 `script_args` 透传给用户脚本，Click 无法预知脚本接受什么参数。

### 2.3 风险

- `ray start` 带不识别参数 → 服务起不来
- `ray submit` 带不识别参数 → 参数被静默丢弃，可能以默认配置而非预期配置启动

---

## 3. Driver 进程线程架构

### 3.1 为什么 Python 进程会有多核 CPU

GIL 只锁 Python 字节码，不锁 C/C++ 代码。Ray driver 进程内嵌 C++ Core Worker，后者在进程内创建多个 C 线程绕过 GIL 并行运行。

### 3.2 进程内嵌架构

```
python3 run_fs_ray_server.py   ← 一个 OS 进程
  │
  ├─ import ray._raylet (.so)   ← C++ 代码加载进当前进程
  │   (ray/__init__.py:85: import ray._raylet)
  │
  ├─ ray.init()
  │    └─ CoreWorkerProcess::Initialize()  ← C++ 在同进程内起线程
  │         ├─ worker.io thread (boost::thread)  [core_worker_process.cc:186]
  │         └─ server.poll0/1/2... (std::thread) [grpc_server.cc:193]
  │
  ├─ Python 主线程跑业务代码
  ├─ StreamingExecutor 调度线程
  ├─ ray_print_logs 日志转发线程
  ├─ ray_listen_error_messages 错误监听线程
  ├─ PythonGCThread
  ├─ RayPerfLogger-JobResourcesReporter (perf 打点)
  └─ ... 其他线程
```

`ray._raylet` 不是独立进程，是一个 `.so` 共享库（Cython 编译的 C++ 代码）。`import ray._raylet` 把这个 `.so` 加载到当前 Python 进程的地址空间里。

### 3.3 worker.io 线程

**文件**: `src/ray/core_worker/core_worker_process.cc:186`

```cpp
io_thread_ = boost::thread(io_thread_attrs, [this]() {
    SetThreadName("worker.io");
    io_service_.run();  // Boost.Asio 事件循环
});
```

- 在 `CoreWorkerProcess` 初始化时启动
- 跑 Boost.Asio 的 `io_service_.run()` 事件循环
- 处理该 worker 与 raylet/GCS 的所有 RPC 通信（push/pull object、task 调度、心跳等）
- 每个 Core Worker 进程都有，Python driver 也不例外
- **不受 GIL 限制**（纯 C++ 代码）

### 3.4 server.poll 线程

**文件**: `src/ray/rpc/grpc_server.cc:193`

```cpp
void GrpcServer::PollEventsFromCompletionQueue(int index) {
    SetThreadName("server.poll" + std::to_string(index));
    void *tag;
    bool ok;
    while (true) {
        auto deadline = gpr_time_add(gpr_now(GPR_CLOCK_REALTIME),
                                     gpr_time_from_millis(250, GPR_TIMESPAN));
        auto status = cqs_[index]->AsyncNext(&tag, &ok, deadline);
        if (status == grpc::CompletionQueue::SHUTDOWN) break;
        else if (status == grpc::CompletionQueue::TIMEOUT) continue;
        auto *server_call = static_cast<ServerCall *>(tag);
        // ... 处理 RPC 请求
    }
}
```

- 在 `GrpcServer::Run()` 时为每个 CompletionQueue 起一个线程
- 线程数由 `num_threads_` 决定
- `while(true)` 循环调用 `cqs_[index]->AsyncNext()` 轮询 gRPC 完成事件
- 处理所有入站 RPC 请求的响应回调
- **不受 GIL 限制**（纯 C++ 代码）

### 3.5 这些线程不是 StreamingExecutor 或 perf 打点触发的

`worker.io` 和 `server.poll` 是 **`ray.init()` 的必然产物**。只要调用了 `ray.init()`，C++ Core Worker 就会启动这些线程。和 StreamingExecutor、perf 打点完全无关。

perf 打点新增的线程是 `RayPerfLogger-JobResourcesReporter` 和 `Python components report thread`。

### 3.6 线程 CPU 消耗分布（实测数据）

124 核机器上的 Ray Data driver 进程（185 线程）：

| 线程名 | CPU ticks | 受 GIL 限制 | 说明 |
|---|---|---|---|
| `worker.io` (×3) | 580K | 否 | gRPC C core 线程 |
| `server.poll0` | 86K | 否 | gRPC server 轮询 |
| `nexting_thread` (×10) | 各 ~2.7K | 否 | Ray C++ 层 I/O 线程 |
| `python3` (轮询) | 113K | 部分 | sleep 时释放 GIL |
| `StreamingExecutor` | 持 GIL | 是 | 只占 1 核 |

py-spy dump 可以验证：只有 `StreamingExecutor-dataset_9_0` 被标记为 `active+gil`，`worker.io` 线程根本不出现在 py-spy 里（因为是 C 线程）。

---

## 4. CPU 使用率分析与火焰图

### 4.1 py-spy dump 获取线程堆栈

```bash
py-spy dump --pid <PID> --nonblocking
```

输出示例：

```
Thread 0x7EF3B099B700 (active+gil): "ray_print_logs"
    deduplicate (ray/_private/ray_logging/__init__.py:318)
    print_to_stdstream (ray/_private/worker.py:2225)
    emit (ray/_private/ray_logging/__init__.py:194)
    print_logs (ray/_private/worker.py:1076)
    run (threading.py:953)

Thread 0x7EF3236FE700 (active): "StreamingExecutor-dataset_9_0"
    on_data_ready (ray/data/_internal/execution/interfaces/physical_operator.py:288)
    process_completed_tasks (ray/data/_internal/execution/streaming_executor_state.py:474)
    _scheduling_loop_step (ray/data/_internal/execution/streaming_executor.py:633)
    run (ray/data/_internal/execution/streaming_executor.py:546)

Thread 0x7F1966976280 (active): "MainThread"
    get_output_blocking (ray/data/_internal/execution/streaming_executor_state.py:331)
    get_next (ray/data/_internal/execution/streaming_executor.py:985)
```

### 4.2 py-spy record 生成火焰图

```bash
py-spy record --pid <PID> --duration 30 --rate 100 --output /tmp/flame.svg --subprocesses
```

### 4.3 线程级 CPU 分析

通过读取 `/proc/<PID>/task/<TID>/stat` 获取每个线程的 CPU ticks：

```python
# utime = field 14, stime = field 15 (clock ticks)
# 通过 stat.find(')') + 2 定位字段起始位置
```

线程命名约定：
- `worker.io` — gRPC C core I/O 线程
- `server.poll0/1/2...` — gRPC server CompletionQueue 轮询
- `nexting_thread` — Ray C++ 层数据拉取线程
- `default-executo` — Ray Core task 执行线程
- `event_engine` — Ray Core 事件引擎
- `client.poll0` — Ray client 轮询

---

## 5. ray_print_logs 日志转发开销

### 5.1 触发机制

**文件**: `python/ray/_private/worker.py:2785-2789`

```python
# ray.init() 时，log_to_driver=True（默认开启）
if log_to_driver:
    global_worker_stdstream_dispatcher.add_handler(
        "ray_print_logs",
        functools.partial(print_to_stdstream, ignore_prefix=ignore_prefix),
    )
    worker.logger_thread = threading.Thread(
        target=worker.print_logs, name="ray_print_logs"
    )
    worker.logger_thread.daemon = True
    worker.logger_thread.start()
```

是同一个进程内的 daemon 线程，通过 GCS pubsub 订阅所有 worker 的日志，然后在 driver 进程里打印并去重。

### 5.2 调用栈

```
print_logs (worker.py:1076)        ← 线程入口，订阅 GCS pubsub 日志
  └─ emit (__init__.py:194)         ← 分发日志批次到 handler
     └─ print_to_stdstream (worker.py:2225)  ← 处理 stdout/stderr 日志
        └─ deduplicate (__init__.py:318)     ← 日志去重
           └─ _canonicalise_log_line (__init__.py:210)  ← 规范化日志行
              └─ <genexpr> (__init__.py:210)  ← 生成器表达式 + 正则匹配
```

### 5.3 性能问题

`_canonicalise_log_line` 对每条日志行做正则匹配：

```python
# ray/_private/ray_logging/__init__.py:210
NUMBERS = re.compile(r"(\d+|0x[0-9a-fA-F]+)")

def _canonicalise_log_line(line):
    return " ".join(x for x in line.split() if not NUMBERS.search(x))
```

`deduplicate` 对一个 batch 里所有行都调用此函数。当 worker 日志量大、日志行很多时，generator + regex 操作消耗大量 CPU，并且**持有 GIL**（py-spy 标记 `active+gil`），阻塞 StreamingExecutor 调度线程。

实测：27% CPU 花在 `<genexpr>` 上，19% 花在 `print_logs` 本身。

### 5.4 缓解方式

```python
ray.init(log_to_driver=False)  # 关闭日志转发
```

---

## 6. StreamingExecutor 线程模型

### 6.1 线程结构

StreamingExecutor 在 `run()` 方法中创建一个独立线程：

```python
# streaming_executor.py:546
def run(self):
    # ...
    self._executor_thread = threading.Thread(
        target=self._run_executor,
        name=f"StreamingExecutor-{self._dataset_id}",
        daemon=True,
    )
    self._executor_thread.start()
```

### 6.2 主线程 vs 调度线程

**主线程 (MainThread)**:
```python
# streaming_executor.py:985 → streaming_executor_state.py:331
def get_output_blocking(self, output_split_idx):
    while True:
        if self._exception is not None: raise self._exception
        elif self._finished and not self.output_queue.has_next(output_split_idx):
            raise StopIteration()
        ref = self.output_queue.pop(output_split_idx)
        if ref is not None: return ref
        time.sleep(0.01)  # 轮询等待输出
```

**调度线程 (StreamingExecutor-dataset_N_M)**:
```python
# streaming_executor.py:546
def _run_executor(self):
    while True:
        continue_sched = self._scheduling_loop_step(self._topology)
        # perf 采样
        _mem_loop_counter += 1
        if _mem_loop_counter % _mem_sample_interval == 0:
            _sampler.sample_realtime(self._topology)
        if not continue_sched or self._shutdown: break
```

### 6.3 调度循环 _scheduling_loop_step

```python
# streaming_executor.py:633
def _scheduling_loop_step(self, topology):
    self._resource_manager.update_usages()
    process_completed_tasks(topology, ...)  # 处理完成的 task
    self._resource_manager.update_usages()
    self._dispatch_loop(topology)           # 调度新 task
    update_operator_states(topology)
    # ... metrics 更新
```

### 6.4 _dispatch_loop — 背压检查

```python
# streaming_executor.py:724
def _dispatch_loop(self, topology):
    while True:
        op = select_operator_to_run(topology, self._resource_manager,
                                    self._backpressure_policies, ...)
        if op is None: break  # 没有可调度的算子

        # 检查所有背压策略的 available_capacity
        soft_cap = None
        for policy in self._backpressure_policies:
            c = policy.available_capacity(op)
            if c is not None:
                soft_cap = c if soft_cap is None else min(soft_cap, c)
        if soft_cap == 0: continue  # 被背压，跳过

        # 分发 task
        while op_state.has_pending_bundles() and op.can_add_input():
            if soft_cap is not None and n >= soft_cap: break
            op_state.dispatch_next_task()
```

---

## 7. BlockMetadata 生命周期

### 7.1 Worker 侧：yield block 和 yield metadata

**文件**: `python/ray/data/_internal/execution/operators/map_operator.py`

每个 task 的 generator 交替 yield：

```python
# map_operator.py:803-843
yielded_schema: bool = False

for block in map_transformer.apply_transform(blocks_iter, ctx):
    block_meta = BlockAccessor.for_block(block).get_metadata()
    block_schema = BlockAccessor.for_block(block).schema()
    blk_exec_stats_builder.finish()

    yield block  # → Ray Object #1: Arrow Table (写入 plasma 或内联)

    exec_stats = blk_exec_stats_builder.build(...)
    task_dur_s = time.perf_counter() - task_start_s

    bm = BlockMetadataWithSchema.from_metadata(
        replace(block_meta, exec_stats=exec_stats,
                task_exec_stats=TaskExecWorkerStats(task_wall_time_s=task_dur_s)),
        schema=block_schema if not yielded_schema else None,  # 只有第一个 block 带 schema
    )
    yield pickle.dumps(bm)  # → Ray Object #2: pickle bytes (内联 < 100KB)

    yielded_schema = True
```

**关键点**: 每个 block 产生 2 个 Ray Object，各有独立 ObjectID。只有第一个 block 带 schema，后续 block 的 schema 为 None（同 operator 共享）。

### 7.2 C++ 侧：StreamingGenerator 上报

**文件**: `python/ray/_raylet.pyx`

worker 每次 yield 时，`report_streaming_generator_output` 被调用：

```python
# _raylet.pyx:1170
cdef report_streaming_generator_output(context, output, generator_index, ...):
    create_generator_return_obj(output, context.generator_id, worker,
                                context.caller_address, ...)
    # 分配 ObjectID (return_id)
    # block > 100KB → 写入 plasma
    # block < 100KB → 内联在 gRPC 响应中
    context.streaming_generator_returns[0].push_back(
        c_pair[CObjectID, c_bool](return_obj.first, is_plasma_object(...)))

    # 通过 gRPC 上报给 owner (driver)
    CCoreWorkerProcess.GetCoreWorker().ReportGeneratorItemReturns(
        return_obj, context.generator_id, context.caller_address, ...)
```

### 7.3 Driver 侧 C++：HandleReportGeneratorItemReturns

**文件**: `src/ray/core_worker/task_manager.cc:779`

```cpp
bool TaskManager::HandleReportGeneratorItemReturns(
    const rpc::ReportGeneratorItemReturnsRequest &request, ...) {
  const auto &generator_id = ObjectID::FromBinary(request.generator_id());
  int64_t item_index = request.item_index();

  // 获取 backpressure 阈值
  auto backpressure_threshold = it->second.spec_.GeneratorBackpressureNumObjects();

  auto stream_it = object_ref_streams_.find(generator_id);
  if (stream_it == object_ref_streams_.end()) {
    return false;  // Stream 已删除
  }

  if (request.has_returned_object()) {
    const auto &returned_object = request.returned_object();
    const auto object_id = ObjectID::FromBinary(returned_object.object_id());

    // ① 写入 ObjectRefStream
    auto index_not_used_yet = stream_it->second.InsertToStream(object_id, item_index);

    // ② 为新 ObjectID 添加引用计数 (local_ref_count = 1)
    if (index_not_used_yet) {
      reference_counter_.OwnDynamicStreamingTaskReturnRef(object_id, generator_id);
    }

    // ③ 标记对象已就绪
    reference_counter_.UpdateObjectPendingCreation(object_id, false);

    // ④ 存储对象数据
    HandleTaskReturn(object_id, returned_object, ...);
  }

  // ⑤ 检查 backpressure
  if (backpressure_threshold != -1 &&
      (item_index - stream_it->second.LastConsumedIndex()) >= backpressure_threshold) {
    // 阻塞 worker 端的 generator，不再 yield 新对象
    signal_it->second.push_back(execution_signal_callback);
  } else {
    execution_signal_callback(Status::OK(), total_consumed);
  }
}
```

### 7.4 OwnDynamicStreamingTaskReturnRef — 自动添加 local ref

**文件**: `src/ray/core_worker/reference_counter.cc:298`

```cpp
void ReferenceCounter::OwnDynamicStreamingTaskReturnRef(
    const ObjectID &object_id, const ObjectID &generator_id) {
  absl::MutexLock lock(&mutex_);
  auto outer_it = object_id_refs_.find(generator_id);
  if (outer_it == object_id_refs_.end()) return;

  rpc::Address owner_address(outer_it->second.owner_address_.value());
  // 关键：add_local_ref=true，C++ 层自动添加 local_ref_count = 1
  RAY_UNUSED(AddOwnedObjectInternal(object_id, {}, owner_address,
                                    outer_it->second.call_site_,
                                    /*object_size=*/-1,
                                    outer_it->second.lineage_eligibility_,
                                    /*add_local_ref=*/true,  // ← 关键
                                    std::optional<NodeID>(),
                                    /*tensor_transport=*/std::nullopt));
}
```

**注释**: `// We add a local reference here. The ref removal will be handled by the ObjectRefStream.`

这意味着即使 Python 侧还没有引用它，C++ 层已经加了 `local_ref_count = 1`，数据不会被回收。释放时机在 `ObjectRefStream.TryReadNextItem` 消费时调用 `TryReleaseLocalRefs`。

### 7.5 Driver 侧 Python：on_data_ready 逐行分析

**文件**: `python/ray/data/_internal/execution/interfaces/physical_operator.py:175-289`

```python
def on_data_ready(self, max_bytes_to_read: Optional[int]) -> int:
    bytes_read = 0
    while max_bytes_to_read is None or bytes_read < max_bytes_to_read:

        # ① 拿 block 的 ObjectRef
        if self._pending_block_ref.is_nil():
            self._pending_block_ref = self._streaming_gen._next_sync(timeout_s=0)
            # → C++: TryReadObjectRefStream → pop ObjectID #1
            # → TryReleaseLocalRefs → local_ref_count--
            # → 如果 Python 创建了 ObjectRef → local_ref_count 回到 1
            if self._pending_block_ref.is_nil():
                break  # 没有新输出
            self._block_ready_callback(self._pending_block_ref)

        # ② 拿 metadata 的 ObjectRef
        if self._pending_meta_ref.is_nil():
            self._pending_meta_ref = self._streaming_gen._next_sync(timeout_s=...)
            # → C++: pop ObjectID #2
            if self._pending_meta_ref.is_nil():
                break  # metadata 还没准备好
            self._metadata_ready_callback(self._pending_meta_ref)

        # ③ ray.get 拉取 metadata bytes
        try:
            meta_with_schema_bytes: bytes = ray.get(
                self._pending_meta_ref, timeout=METADATA_GET_TIMEOUT_S  # 1.0s
            )
            # → 内联对象: 从 in-memory store 拷贝出 bytes
            # → plasma 对象: 从 plasma 拉取
        except ray.exceptions.GetTimeoutError:
            break  # 超时，下一轮重试

        # ④ pickle.loads 还原 Python 对象
        meta_with_schema: BlockMetadataWithSchema = pickle.loads(meta_with_schema_bytes)
        # → schema 经 _read_arrow_schema_cached (LRU 128, 同 operator 共享)

        # ⑤ 提取 metadata
        meta = meta_with_schema.metadata
        # → 创建新的 BlockMetadata 对象（从 meta_with_schema 的字段构造）

        # ⑥ 构建 RefBundle 并入队
        self._output_ready_callback(
            RefBundle(
                [(self._pending_block_ref, meta)],
                owns_blocks=True,
                schema=meta_with_schema.schema,
            ),
        )
        # → RefBundle 被追加到 output_queue

        # ⑦ 保存 last meta
        self._last_block_meta = meta

        # ⑧ 释放 pending 引用
        self._pending_block_ref = ray.ObjectRef.nil()
        # → ObjectRef #1 被 RefBundle 持有，local_ref_count 仍 > 0 → 不释放

        self._pending_meta_ref = ray.ObjectRef.nil()
        # → ObjectRef #2 无其他引用 → local_ref_count = 0
        # → RefCount() = 0 → OutOfScope = true
        # → OnObjectOutOfScopeOrFreed → in-memory store 释放 pickle bytes
        # → ShouldDelete? lineage_ref_count == 0 → EraseReference

        bytes_read += meta.size_bytes
    return bytes_read
```

### 7.6 BlockMetadataWithSchema 对象结构

**文件**: `python/ray/data/block.py`

```python
@dataclass(frozen=True)
class BlockExecStats:
    task_idx: Optional[int] = None          # 8B
    node_id: str = ...                       # ~40B
    start_time_s: Optional[float] = None     # 8B
    end_time_s: Optional[float] = None       # 8B
    wall_time_s: Optional[float] = None      # 8B
    udf_time_s: Optional[float] = 0          # 8B
    block_ser_time_s: Optional[float] = None # 8B
    cpu_time_s: Optional[float] = None       # 8B
    max_uss_bytes: int = 0                   # 8B
    # → ~340B (含 Python 对象头)

@dataclass(frozen=True)
class BlockStats:
    num_rows: Optional[int]                  # 8B
    size_bytes: Optional[int]                 # 8B
    exec_stats: Optional[BlockExecStats]      # 引用 ~8B
    task_exec_stats: Optional[TaskExecWorkerStats]  # 引用 ~8B

@dataclass(frozen=True)
class BlockMetadata(BlockStats):
    input_files: Optional[Tuple[str, ...]] = None  # 引用 ~8B

@dataclass(frozen=True)
class BlockMetadataWithSchema(BlockMetadata):
    schema: Optional[Schema] = None          # 共享引用 ~8B

    @staticmethod
    def from_metadata(metadata, schema=None):
        return BlockMetadataWithSchema(
            num_rows=metadata.num_rows,
            size_bytes=metadata.size_bytes,
            exec_stats=metadata.exec_stats,
            task_exec_stats=metadata.task_exec_stats,
            input_files=metadata.input_files,
            schema=schema,
        )

    @property
    def metadata(self) -> BlockMetadata:
        return BlockMetadata(
            num_rows=self.num_rows,
            size_bytes=self.size_bytes,
            exec_stats=self.exec_stats,
            input_files=self.input_files,
            task_exec_stats=self.task_exec_stats,
        )
```

### 7.7 RefBundle 结构

**文件**: `python/ray/data/_internal/execution/interfaces/ref_bundle.py`

```python
@dataclass(frozen=True)
class RefBundle:
    blocks: Tuple[Tuple[ObjectRef[Block], BlockMetadata], ...]
    schema: Optional["Schema"]   # 同 operator 共享
    owns_blocks: bool
```

### 7.8 内联阈值

**文件**: `src/ray/common/ray_config_def.h:218`

```cpp
RAY_CONFIG(int64_t, max_direct_call_object_size, 100 * 1024)  // 默认 100KB
```

```cpp
// core_worker.cc:2730
if (static_cast<int64_t>(data_size) < max_direct_call_object_size_ &&
    (*task_output_inlined_bytes + data_size <=
     RayConfig::instance().task_rpc_inlined_bytes_limit())) {
    data_buffer = std::make_shared<LocalMemoryBuffer>(data_size);  // 内联
    *task_output_inlined_bytes += data_size;
} else {
    CreateExisting(...);  // 写入 plasma
}
```

- metadata (< 100KB) → 内联在 gRPC 响应中，存在 driver 的 C++ in-memory store
- block 数据 (> 100KB) → 写入 plasma object store

---

## 8. Reference Counter 与 Object 内存管理

### 8.1 Reference 结构体

**文件**: `src/ray/core_worker/reference_counter.h:307`

```cpp
struct Reference {
    std::string call_site_;                    // ~32B
    int64_t object_size_ = -1;                 // 8B
    absl::flat_hash_set<NodeID> locations;      // ~32B
    std::optional<rpc::Address> owner_address_; // ~200B
    std::optional<NodeID> pinned_at_node_id_;  // ~16B
    std::optional<std::string> tensor_transport_; // ~16B
    bool owned_by_us_ = false;                  // 1B
    LineageReconstructionEligibility lineage_eligibility_; // 4B
    size_t lineage_ref_count = 0;               // 8B
    size_t local_ref_count = 0;                 // 8B
    size_t submitted_task_ref_count = 0;        // 8B
    std::unique_ptr<NestedReferenceCount> nested_reference_count; // 8B (通常 null)
    std::unique_ptr<BorrowInfo> borrow_info;    // 8B (通常 null)
    std::vector<std::function<void(const ObjectID &)>> on_object_out_of_scope_or_freed_callbacks; // ~48B
    std::vector<std::function<void(const ObjectID &)>> object_ref_deleted_callbacks; // ~48B
    bool publish_ref_removed = false;           // 1B
    std::string spilled_url;                    // ~32B
    NodeID spilled_node_id = NodeID::Nil();      // ~16B
    bool spilled = false;                        // 1B
    // → 总计 ~350-400B (含 unique_ptr 和 vector 开销)
};
```

### 8.2 引用计数判断逻辑

```cpp
// reference_counter.h:349
size_t RefCount() const {
    return local_ref_count + submitted_task_ref_count +
           nested().contained_in_owned.size();
}

// reference_counter.h:361
bool OutOfScope(bool lineage_pinning_enabled) const {
    bool in_scope = RefCount() > 0;
    bool is_nested = !nested().contained_in_borrowed_ids.empty();
    bool has_borrowers = !borrow().borrowers.empty();
    bool was_stored_in_objects = !borrow().stored_in_objects.empty();
    bool has_lineage_references = false;
    if (lineage_pinning_enabled && owned_by_us_ &&
        lineage_eligibility_ != LineageReconstructionEligibility::ELIGIBLE) {
        has_lineage_references = lineage_ref_count > 0;
    }
    return !(in_scope || is_nested || has_nested_refs_to_report ||
             has_borrowers || was_stored_in_objects || has_lineage_references);
}

// reference_counter.h:383
bool ShouldDelete(bool lineage_pinning_enabled) const {
    if (lineage_pinning_enabled) {
        return OutOfScope(lineage_pinning_enabled) && (lineage_ref_count == 0);
    } else {
        return OutOfScope(lineage_pinning_enabled);
    }
}
```

### 8.3 DeleteReferenceInternal — 两步释放

**文件**: `src/ray/core_worker/reference_counter.cc:740`

```cpp
void ReferenceCounter::DeleteReferenceInternal(ReferenceTable::iterator it,
                                               std::vector<ObjectID> *deleted) {
    const ObjectID id = it->first;

    if (it->second.RefCount() == 0 && it->second.publish_ref_removed) {
        PublishRefRemovedInternal(id);
        it->second.publish_ref_removed = false;
    }

    // 第一步: OutOfScope → 释放 object 数据 (unpin plasma / 释放 in-memory)
    if (it->second.OutOfScope(lineage_pinning_enabled_)) {
        for (const auto &inner_id : it->second.nested().contains) {
            // 递归处理嵌套引用
            DeleteReferenceInternal(inner_it, deleted);
        }
        OnObjectOutOfScopeOrFreed(it);  // ← 释放 object 数据
        if (deleted != nullptr) deleted->push_back(id);
    }

    // 第二步: ShouldDelete → 从 hash map 中删除 entry
    if (it->second.ShouldDelete(lineage_pinning_enabled_)) {
        ReleaseLineageReferences(it);
        EraseReference(it);  // ← object_id_refs_.erase(it)
    }
}
```

### 8.4 OnObjectOutOfScopeOrFreed — 释放 object 数据

**文件**: `src/ray/core_worker/reference_counter.cc:839`

```cpp
void ReferenceCounter::OnObjectOutOfScopeOrFreed(ReferenceTable::iterator it) {
    // 执行回调（通知 raylet unpin object）
    for (const auto &callback : it->second.on_object_out_of_scope_or_freed_callbacks)
        callback(it->first);  // → 通知 raylet 释放 plasma 中的 object

    it->second.on_object_out_of_scope_or_freed_callbacks.clear();

    // 更新统计
    UpdateOwnedObjectCounters(it->first, it->second, /*decrement=*/true);
    UnsetObjectPrimaryCopy(it);  // 清除 pin 信息
    UpdateOwnedObjectCounters(it->first, it->second, /*decrement=*/false);
}

void ReferenceCounter::UnsetObjectPrimaryCopy(ReferenceTable::iterator it) {
    it->second.pinned_at_node_id_.reset();  // unpin
    if (it->second.spilled && !it->second.spilled_node_id.IsNil()) {
        it->second.spilled = false;
        it->second.spilled_url = "";
        it->second.spilled_node_id = NodeID::Nil();
    }
}
```

### 8.5 三阶段释放

| 阶段 | OutOfScope | lineage_ref_count | object 数据 | reference entry |
|---|---|---|---|---|
| RefBundle 在 output_queue | false | >0 | **保留** (pin) | 保留 |
| 下游消费完, RefCount=0 | true | >0 | **释放** (unpin, 可 evict/spill) | 保留 |
| 依赖 task 不可重试 | true | 0 | 已释放 | **EraseReference** 删除 |

### 8.6 完整引用链路

```
worker yield → HandleReportGeneratorItemReturns
  ├─ InsertToStream(object_id)           → ObjectRefStream 持有
  └─ OwnDynamicStreamingTaskReturnRef   → local_ref_count = 1 (C++ 自动加)
                                            object 数据被 pin，不会被回收

  ... 停在 ObjectRefStream 中等待消费 ...

driver on_data_ready:
  _next_sync() → TryReadNextItem → pop ObjectID
    └─ TryReleaseLocalRefs → local_ref_count = 0
       ├─ 如果 Python 侧创建了 ObjectRef (block_ref) → local_ref_count = 1 → 仍保留
       └─ 如果 Python 侧没创建 ObjectRef (meta_ref 已被 ray.get 消费) → RefCount=0

self._pending_meta_ref = nil()
  → ObjectRef #2 (metadata) 无引用 → RefCount=0 → OutOfScope=true
     → OnObjectOutOfScopeOrFreed → in-memory store 释放 pickle bytes
     → ShouldDelete? lineage_ref_count == 0 → EraseReference

self._pending_block_ref = nil()
  → ObjectRef #1 (block) 被 RefBundle 持有 → local_ref_count=1 → 不释放
```

### 8.7 ObjectRefStream 结构

**文件**: `src/ray/core_worker/task_manager.cc:55`

```cpp
class ObjectRefStream {
    ObjectID generator_id_;
    int64_t next_index_ = 0;           // 下一个待消费的 index
    int64_t max_index_seen_ = -1;
    absl::flat_hash_set<ObjectID> refs_written_to_stream_;  // 已写入的 ObjectID
    int64_t end_of_stream_index_ = -1;
    // → 每个 ObjectID entry ~28B (hash set entry)
};
```

---

## 9. Generator Backpressure 机制

### 9.1 C++ 层 backpressure

**文件**: `src/ray/core_worker/task_manager.cc:862`

```cpp
if (backpressure_threshold != -1 &&
    (item_index - stream_it->second.LastConsumedIndex()) >= backpressure_threshold) {
    // 阻塞 worker 端 generator，不再 yield 新对象
    signal_it->second.push_back(execution_signal_callback);
} else {
    // 不背压，允许继续 yield
    execution_signal_callback(Status::OK(), total_consumed);
}
```

### 9.2 Ray Data 设置的 backpressure 阈值

**文件**: `python/ray/data/_internal/execution/operators/task_pool_map_operator.py:124`

```python
if "_generator_backpressure_num_objects" not in dynamic_ray_remote_args \
    and self.data_context._max_num_blocks_in_streaming_gen_buffer is not None:
    # 2x 因为每个 block yield 2 个对象: block + metadata
    dynamic_ray_remote_args["_generator_backpressure_num_objects"] = (
        2 * self.data_context._max_num_blocks_in_streaming_gen_buffer
    )
```

**文件**: `python/ray/data/context.py:284`

```python
DEFAULT_MAX_NUM_BLOCKS_IN_STREAMING_GEN_BUFFER = 2
```

所以默认 `generator_backpressure_num_objects = 4`（2 × 2）。

### 9.3 ReadTask 关闭 backpressure

**文件**: `python/ray/data/_internal/planner/plan_download_op.py:107`

```python
ray_actor_task_remote_args={"_generator_backpressure_num_objects": -1}
```

ReadTask 设了 `-1`，即关闭 C++ 层背压，worker 可以无限 yield。

### 9.4 总堆积量估算

```
总堆积 ObjectID ≈ 并发 task 数 × backpressure_threshold × 2 (block + metadata)
               = 并发 task 数 × 4 × 2
               = 并发 task 数 × 8
```

| 场景 | 并发 task 数 | 总堆积 ObjectID | 是否百万级 |
|---|---|---|---|
| 普通算子 (buffer=2) | 500 | 4000 | 否 |
| ReadTask (buffer=-1) | 不限 | 不限 | **可能** |
| 手动设大 buffer | 不限 | 不限 | **可能** |

---

## 10. 百万级 Block 内存累积分析

### 10.1 背景

当上游读/切块远快于 GPU 算子消费时，block 元数据可能堆积在 Head driver 上。核心问题是：output_queue 无界，背压是字节维度而非 block 数量维度。

### 10.2 背压机制的盲区

| 机制 | 默认值 | 维度 | 能否防止百万级 block |
|---|---|---|---|
| Object store 内存预算 | 50% object store | 字节 | 小 block 时不触发 |
| DownstreamCapacity 背压 | ratio > 10.0 | 字节 | 小 block 时不触发 |
| 动态队列大小背压 (EWMA) | **默认关闭** | 字节 | 关闭时无效 |
| C++ generator backpressure | 4 objects/task | 数量 | 限制单 task，不限总 task |
| ReadTask backpressure | -1 (关闭) | - | **不限制** |

### 10.3 OpBufferQueue 无界

**文件**: `python/ray/data/_internal/execution/streaming_executor_state.py`

```python
class OpBufferQueue:
    # 无 block count 限制，无 byte count 限制
    # FIFO 队列，存储 RefBundle 对象
    def append(self, bundle): ...
    def pop(self): ...
    @property
    def memory_usage(self): return sum(bundle.size_bytes() for bundle in ...)
    @property
    def num_blocks(self): return sum(len(bundle.block_refs) for bundle in ...)
```

### 10.4 一个 block 在 output_queue 时的持久内存

**C++ 层:**

| 组件 | 内容 | 大小 |
|---|---|---|
| `object_id_refs_` — block ObjectID | `Reference` struct | ~350 B |
| `object_id_refs_` — metadata ObjectID | 已释放 (EraseReference) | 0 B |
| ObjectRefStream | 已被 TryReadNextItem 消费 | 0 B |
| in-memory store — block 数据 | block < 100KB 时内联 | block.size_bytes 或 0 |
| in-memory store — metadata bytes | 已释放 (OutOfScope) | 0 B |

**Python 层:**

| 对象 | 持有者 | 大小 |
|---|---|---|
| `ObjectRef` (block) | RefBundle.blocks tuple | ~100 B |
| `BlockMetadata` | RefBundle.blocks tuple | ~200 B |
| `BlockExecStats` | BlockMetadata.exec_stats (共享引用) | ~340 B |
| `TaskExecWorkerStats` | BlockMetadata.task_exec_stats | ~80 B |
| tuple `[(ObjectRef, BlockMetadata)]` | RefBundle.blocks | ~100 B |
| `RefBundle` (dataclass) | output_queue | ~260 B |
| `Schema` | RefBundle.schema (同 operator 共享) | ~0 (均摊) |

**每 block 持久开销: ~1.4-1.8 KB (非数据) + block 数据 (如内联)**

### 10.5 百万级估算

```
driver Python heap:  100万 × ~1.1 KB ≈ 1.1 GB
driver C++ heap:     100万 × ~0.5 KB ≈ 0.5 GB (Reference entries)
────────────────────────────────────────────
driver 非数据开销:    ~1.6 GB
加上 Python GC 对象头、容器扩容、碎片: ~2-3 GB
```

block 数据在 plasma 中（不占 driver RSS），但被 pin 不可 evict/spill。

### 10.6 缓解方案

```bash
# 启用动态队列大小背压（默认关闭）
export RAY_DATA_ENABLE_DYNAMIC_OUTPUT_QUEUE_SIZE_BACKPRESSURE=1

# 降低 downstream capacity ratio（默认 10.0 → 更早触发）
export RAY_DATA_DOWNSTREAM_CAPACITY_BACKPRESSURE_RATIO=2.0

# 关闭日志转发减少 GIL 争用
ray.init(log_to_driver=False)

# 限制 streaming gen buffer 大小
DataContext.get_current()._max_num_blocks_in_streaming_gen_buffer = 1
```

### 10.7 Liveness 放松

**文件**: `python/ray/data/_internal/execution/resource_manager.py:615-660`

```python
def _should_unblock_streaming_output_backpressure(self, op):
    # 当下游没有活跃 task 且无法调度时 → 放松背压
    # 当下游没有活跃 task 且没有输入 block 时 → 放松背压
    # 当空闲超过 10 秒时 → 放松背压 (最后手段)
```

liveness 机制会放松输出背压以防止死锁，但这创建了 metadata 可以流入 driver 的时间窗口。

---

## 附录：关键文件索引

| 文件 | 内容 |
|---|---|
| `src/ray/common/memory_monitor_utils.cc` | cgroup 内存计算 |
| `python/ray/scripts/scripts.py` | Ray CLI 命令定义 |
| `src/ray/core_worker/core_worker_process.cc` | worker.io 线程启动 |
| `src/ray/rpc/grpc_server.cc` | server.poll 线程启动 |
| `python/ray/_private/worker.py` | ray_print_logs 线程启动 |
| `python/ray/_private/ray_logging/__init__.py` | 日志去重逻辑 |
| `python/ray/data/_internal/execution/streaming_executor.py` | StreamingExecutor 调度循环 |
| `python/ray/data/_internal/execution/streaming_executor_state.py` | OpBufferQueue、process_completed_tasks |
| `python/ray/data/_internal/execution/interfaces/physical_operator.py` | on_data_ready、ray.get metadata |
| `python/ray/data/_internal/execution/operators/map_operator.py` | yield block + yield metadata |
| `python/ray/data/_internal/execution/interfaces/ref_bundle.py` | RefBundle 结构 |
| `python/ray/data/block.py` | BlockMetadataWithSchema 结构 |
| `python/ray/data/context.py` | DEFAULT_MAX_NUM_BLOCKS_IN_STREAMING_GEN_BUFFER |
| `python/ray/_raylet.pyx` | StreamingGenerator、report_streaming_generator_output |
| `src/ray/core_worker/task_manager.cc` | ObjectRefStream、HandleReportGeneratorItemReturns |
| `src/ray/core_worker/reference_counter.cc` | 引用计数管理、DeleteReferenceInternal |
| `src/ray/core_worker/reference_counter.h` | Reference 结构体定义 |
| `src/ray/common/ray_config_def.h` | max_direct_call_object_size (100KB) |
