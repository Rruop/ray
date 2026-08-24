# Job 77000000 卡在 PENDING_NODE_ASSIGNMENT 排查报告

**日期**: 2026-08-24
**集群**: kce-aip
**Job ID**: 77000000
**Ray 版本**: `2.54.0+kuaishou.a760958466`（未包含修复 commit `82d05363`）
**Head 节点**: 10.137.44.181 (CST UTC+8)
**Worker 节点**: 8ae1cc62/public-bjx-c26-kce-node272 (10.53.82.106, UTC 时区)
**作业入口**: StreamingRepartition `_map_task`

---

## 一、问题描述

作业 77000000 中 9 个 StreamingRepartition `_map_task` 长期处于 `PENDING_NODE_ASSIGNMENT` 状态，无法调度执行。集群资源充足（CPU 有大量空闲），但 task 始终无法被分配到 worker。

---

## 二、关键证据

### 2.1 9 个卡住 lease 详情

通过全量提取差集方法定位 9 个卡住的 lease：

| 目标节点 | 卡住 lease 数量 | 发出时间 (CST) |
|---|---|---|
| d041357f | 3 | 11:06~11:10 |
| b286ce11 | 2 | 11:06~11:10 |
| 8ae1cc62 | 1 | 11:06~11:10 |
| eaf028d0 | 1 | 11:06~11:10 |
| 2facb5c3 | 1 | 11:06~11:10 |
| f326 (8ae1cc62) | 1 | 11:06~11:10 |

### 2.2 8ae1cc62 节点 state-dump 数据

```
[state-dump] Number of granted lease arguments: 48
[state-dump] Total size of pinned lease arguments: 22048319656    ← 22.05 GB
[state-dump] num_waiting_for_plasma_memory: 1
[state-dump] Grant queue length: 1
```

同时 CPU 可用：620,000/700,000（62/70 核空闲）—— CPU 充足但无法调度。

### 2.3 如何从日志中获取这些指标

这些值来自 raylet 的 **state-dump**，每分钟自动输出到 `/tmp/ray/session_latest/logs/raylet.out`。对应源码在 `local_lease_manager.cc:1204-1207`：

```cpp
buffer << "Number of granted lease arguments: " << granted_lease_args_.size() << "\n";
buffer << "Number of pinned lease arguments: " << pinned_lease_arguments_.size()
buffer << "Total size of pinned lease arguments: " << pinned_lease_arguments_bytes_
```

### 2.4 查看末尾日志的方法

raylet.out 文件通常很大（千万行+），直接 grep/cat 会卡死 I/O。推荐方法：

**方法 1：tail -c 只读尾部 500KB**
```bash
tail -c 500000 /tmp/ray/session_latest/logs/raylet.out | \
  grep -E "granted lease arguments|Total size of pinned|num_waiting_for_plasma|Grant queue length" | tail -8
```

**方法 2：Python 只读文件末尾**（更安全，适用于超大文件）
```bash
python3 -c "
import os
path = '/tmp/ray/session_latest/logs/raylet.out'
size = os.path.getsize(path)
with open(path, 'rb') as f:
    f.seek(max(0, size - 500000))
    text = f.read().decode('utf-8', errors='replace')
keywords = ['granted lease arguments', 'Total size of pinned', 'num_waiting_for_plasma', 'Grant queue length']
for line in text.split(chr(10)):
    if any(k in line for k in keywords):
        print(line)
"
```

**方法 3：快捷脚本**（已在远端节点创建）
```bash
bash /tmp/check_args.sh
```

脚本内容：
```bash
#!/bin/bash
tail -c 500000 /tmp/ray/session_latest/logs/raylet.out | \
  grep -E "granted lease arguments|Total size of pinned|num_waiting_for_plasma|Grant queue length|is_idle" | tail -10
```

---

## 三、max_pinned_lease_arguments_bytes_ 计算

### 3.1 object_store_memory 确定

从 state-dump 的 `InitialConfigResources` 中：
```
object_store_memory: 3.2e+10
```

Ray 内部资源表示中 `320000000000000` = 3.2e+10 × 10000（Ray 资源单位换算），所以原始 bytes：
```
object_store_memory = 32,000,000,000 bytes = 32 GB
```

### 3.2 阈值计算

源码 `src/ray/common/ray_config_def.h:726`：
```cpp
RAY_CONFIG(float, max_task_args_memory_fraction, 0.7)
```

源码 `src/ray/raylet/main.cc:933`：
```cpp
auto max_task_args_memory =
    static_cast<int64_t>(static_cast<float>(object_manager->GetMemoryCapacity()) *
                         RayConfig::instance().max_task_args_memory_fraction());
```

因此：
```
max_pinned_lease_arguments_bytes_ = 32 GB × 0.7 = 22.4 GB
```

### 3.3 泄漏量对比

```
pinned_lease_arguments_bytes = 22,048,319,656 bytes ≈ 22.05 GB
max_pinned_lease_arguments_bytes_ = 22.4 GB

22.05 GB / 22.4 GB ≈ 98.4%  ← 已几乎打满阈值！
```

新 lease 尝试 pin args 时：22.05 GB + 新 lease args > 22.4 GB → `PinLeaseArgsIfMemoryAvailable` 返回 false → `WAITING_FOR_AVAILABLE_PLASMA_MEMORY` → 死锁。

### 3.4 关于之前错误计算的勘误

早期分析曾错误地将 `object_store_memory` 内部资源单位 `320000000000000` 误解为原始 bytes，得出 `max_pinned = 224 GB` 的错误结论。实际上 Ray 内部资源表示乘了 10000 的换算系数，真实值是 `32 GB × 0.7 = 22.4 GB`。

---

## 四、Bug 1 修正分析：Raylet 端 pinned lease args 泄漏

### 4.1 原始文档的描述（不准确）

原始排查文档说 `HandleReturnWorkerLease` "❌ 完全缺少 `ReleaseLeaseArgs` 调用"——这个描述**不完全准确**。

### 4.2 正确的代码分析

`HandleReturnWorkerLease`（`node_manager.cc:2080-2114`）的完整逻辑：

```cpp
void NodeManager::HandleReturnWorkerLease(...) {
  auto lease_id = LeaseID::FromBinary(request.lease_id());
  if (!leased_workers_.contains(lease_id)) {
    send_reply_callback(Status::OK(), nullptr, nullptr);
    return;
  }
  std::shared_ptr<WorkerInterface> worker = leased_workers_[lease_id];
  ReleaseWorker(lease_id);              // 从 leased_workers_ 移除

  if (request.disconnect_worker()) {
    // 路径 A: worker 需要被销毁
    DisconnectClient(worker->Connection(), ...);
  } else {
    // 路径 B: worker 正常返回
    if (worker->IsBlocked()) {
      HandleNotifyWorkerUnblocked(worker);
    }
    local_lease_manager_.ReleaseWorkerResources(worker);  // 只释放 CPU 资源

    if (!request.worker_exiting()) {
      // 路径 B1: worker 不退出 → 加入 idle pool
      HandleWorkerAvailable(worker);
      // ↑ HandleWorkerAvailable 内部调用 CleanupLease → ReleaseLeaseArgs ✅
    }
    // 路径 B2: worker_exiting=true → 跳过 HandleWorkerAvailable ❌
  }
}
```

### 4.3 三条释放路径分析

| 路径 | 条件 | 调用链 | ReleaseLeaseArgs |
|---|---|---|---|
| **正常路径** | `worker_exiting=false` | `HandleReturnWorkerLease → ReleaseWorker → ReleaseWorkerResources → HandleWorkerAvailable → CleanupLease → ReleaseLeaseArgs` | ✅ 间接调用存在 |
| **异常路径** | `worker_exiting=true, disconnect_worker=false` | `HandleReturnWorkerLease → ReleaseWorker → ReleaseWorkerResources → 跳过 HandleWorkerAvailable` | ❌ 不调 CleanupLease，不调 ReleaseLeaseArgs |
| **兜底路径** | worker 退出后 disconnect | `DisconnectClient → CleanupLease → ReleaseLeaseArgs`（条件 `!worker->IsDead()`） | ✅ 间接调用存在（但有条件） |

### 4.4 CleanupLease 的代码（local_lease_manager.cc:766-776）

```cpp
void LocalLeaseManager::CleanupLease(std::shared_ptr<WorkerInterface> worker,
                                     RayLease *lease) {
  RAY_CHECK(worker != nullptr && lease != nullptr);
  *lease = worker->GetGrantedLease();
  RemoveFromGrantedLeasesIfExists(*lease);
  ReleaseLeaseArgs(lease->GetLeaseSpecification().LeaseId());  // ← 释放 pinned args
  if (worker->GetAllocatedInstances() != nullptr) {
    ReleaseWorkerResources(worker);
  }
}
```

### 4.5 实际泄漏场景

`worker_exiting=true` 时代码注释说 "The worker will cleanup and terminate itself"（worker 自己清理并退出）。如果 worker 进程真的退出了，会触发 `DisconnectClient`，在 `!worker->IsDead()` 条件下走兜底路径释放 args。

**但如果 worker 进程没有真正退出**（或 disconnect 事件延迟/遗漏），args 就会永久泄漏。

### 4.6 77000000 的实际数据

- 77000000 的 driver 日志**没有** `worker_exiting` 事件
- 77000000 的 task 全部正常完成，走正常路径（B1），间接调用了 `ReleaseLeaseArgs`
- **48 个泄漏的 args 在 77000000 启动前就已存在**（7月31日就达到 48，77000000 8月24日才启动），来自其他历史作业

### 4.7 granted_lease_args 数据趋势分析

granted_lease_args 的值有波动（会下降），说明 `ReleaseLeaseArgs` 在正常路径中确实被调用。但**基线持续上升**：

| 时间 | granted_lease_args 峰值 | 回落后基线 | 净增 |
|---|---|---|---|
| 7月初 | 0 | 0 | 0 |
| 7月中 | 17 | ~15 | +15 |
| 7月20日 | 22 | ~18 | +3 |
| 7月25日 | 34 | ~30 | +12 |
| 7月28日 | 38 | ~35 | +5 |
| 7月31日 | 48 | ~45 | +10 |
| 8月24日 (77000000 启动) | 48 | 48 | ← 77000000 是受害者 |

每次峰值后回不到之前的基线，净增几个。这说明**正常路径能释放 args，但异常路径（worker_exiting=true 或竞态条件）会留下永久泄漏**。

### 4.8 结论

- 正常路径（`worker_exiting=false`）间接调用 `ReleaseLeaseArgs` **是存在的**，原文说"完全缺少"不准确
- **`worker_exiting=true` 路径跳过 `HandleWorkerAvailable` 是一个可能的泄漏路径**，但未实锤为唯一根因——还可能有其他场景（如 `DisconnectClient` 中 `!worker->IsDead()` 条件不满足、某些异常场景 worker 不经 `ReturnWorkerLease` 就断开、竞态条件等），需要 DEBUG 日志才能确认每次泄漏的精确触发点
- **确认泄漏需要 DEBUG 日志追踪每次 `granted_lease_args_` 净增时的精确上下文**，但运行中的 raylet 无法动态开启 DEBUG 日志（`RAY_BACKEND_LOG_LEVEL` 只对新进程生效），raylet 重启后泄漏数据已不可复现
- 48 个泄漏来自历史作业，77000000 只是受害者

---

## 五、Bug 2：Driver 端 gRPC 永不超时

### 5.1 问题

`ReturnWorkerLease`（`raylet_client.cc:106-127`）和 `RequestWorkerLease` 的 `method_timeout_ms=-1`，导致 `RetryableGrpcClient` 永不超时。

```cpp
// raylet_client.cc:125
INVOKE_RETRYABLE_RPC_CALL(
    retryable_grpc_client_,
    NodeManagerService,
    ReturnWorkerLease,
    request,
    [](const Status &status, rpc::ReturnWorkerLeaseReply &&reply) {
      RAY_LOG_IF_ERROR(INFO, status) << "Error returning worker: " << status;
    },
    grpc_client_,
    /*method_timeout_ms*/ -1);  // ← 永不超时
```

### 5.2 修复 commit `82d05363` 的内容

- 给 `RequestWorkerLease` 在 `grant_or_reject=true`（spillback）时加了 `method_timeout_ms=600000`（10 分钟超时）
- 增加了 `grpc_max_ready_idle_resend_count=10`（READY/IDLE 通道连续重发 10 次后 fail 所有 pending requests）
- **只修复了 driver 端超时，未修复 raylet 端的 pinned args 泄漏**
- 修复 commit 未合入运行集群版本 `2.54.0+kuaishou.a760958466`

### 5.3 修复只覆盖了 spillback 路径

注意修复只在 `grant_or_reject=true` 时加超时，`grant_or_reject=false`（locality-aware 路径）时 `method_timeout_ms` 仍然是 -1。这意味着 locality-aware 路径的死锁可能仍未完全修复。

---

## 六、为什么不会 reject/spillback

### 6.1 PinLeaseArgsIfMemoryAvailable 在资源分配之前

pinned args 超限发生在 `AllocateLocalTaskResources` **之前**：

```cpp
// local_lease_manager.cc:315-349 — GrantScheduledLeasesToWorkers 内部
bool args_missing = false;
bool success = PinLeaseArgsIfMemoryAvailable(spec, &args_missing);  // ← 先检查 pin
if (!success) {
    // pinned 超限 → WAITING_FOR_AVAILABLE_PLASMA_MEMORY
    // ❌ 不尝试 TrySpillback
    work->SetStateWaiting(
        internal::UnscheduledWorkCause::WAITING_FOR_AVAILABLE_PLASMA_MEMORY);
    work_it++;
    continue;
}
// 下面才是资源分配
auto allocated_instances = std::make_shared<TaskResourceInstances>();
bool schedulable = AllocateLocalTaskResources(...);  // ← 资源不足时 TrySpillback
```

### 6.2 死锁机制

```
PinLeaseArgsIfMemoryAvailable 返回 false
  → lease 设为 WAITING_FOR_AVAILABLE_PLASMA_MEMORY
  → 不回复 driver
  → 不 spillback
  → 不 reject
  → 等待其他 lease 释放 args 后重新调度
  → 但泄漏的 args 永远不释放
  → 死锁
```

这是设计上的不足：**pin 内存超限时没有 `TrySpillback`**（资源不足和 scheduling class cap 超限时都会 TrySpillback）。

---

## 七、8ae1cc62 节点 disconnect 数据分析

### 7.1 Worker 启动与断开

- 8ae1cc62 上共 2430 个 worker 启动，2422 个 disconnect
- 当前 8 个 IDLE worker（4 个普通 + 2 SpillWorker + 2 RestoreWorker）
- 77000000 的 worker disconnect 数 = 0

### 7.2 disconnect_type 分布

| disconnect_type | 值 | 数量 |
|---|---|---|
| INTENDED_USER_EXIT | 3 | 1718 |
| INTENDED_SYSTEM_EXIT | 1 | 687 |
| SYSTEM_ERROR | 0 | 12 |
| NODE_OUT_OF_MEMORY | 4 | 3 |

### 7.3 Worker exit 事件

74 个 worker exit 事件，全部 `force_exit: 1`（idle worker 被 job finish kill），没有 `force_exit: 0` 的 idle timeout exit。

### 7.4 当前状态（raylet 已重启）

- raylet 已被重启，node ID 变了（`3577354374366075196`，之前是 `8ae1cc62`）
- granted_lease_args = 0，pinned_lease_arguments_bytes = 0
- CPU 70/70 核完全空闲
- 之前的 48 个泄漏和 22GB 全部清零

---

## 八、完整因果链

```
历史作业中 worker_exiting=true 的场景（或竞态条件）
  → HandleReturnWorkerLease 跳过 HandleWorkerAvailable
  → 不调 CleanupLease → 不调 ReleaseLeaseArgs
  → granted_lease_args_ 和 pinned_lease_arguments_bytes_ 永久增长

基线持续上升: 0→17→22→34→38→42→47→48（7月31日达到48）

77000000 启动（8月24日）
  → 8ae1cc62 上已有 48 个泄漏的 granted args，pinned = 22.05 GB
  → max_pinned = 22.4 GB (32 GB × 0.7)
  → 77000000 的 9 个 StreamingRepartition _map_task 发到各节点

f326 lease 到达 8ae1cc62
  → PinLeaseArgsIfMemoryAvailable: 22.05 GB + 新 args > 22.4 GB
  → return false → WAITING_FOR_AVAILABLE_PLASMA_MEMORY
  → 不回复 driver，不 spillback，不 reject
  → Driver 端 RetryableGrpcClient (method_timeout_ms=-1) 永不超时
  → callback 永远不触发
  → 9 个 task 永久卡在 PENDING_NODE_ASSIGNMENT
```

---

## 九、修复方案

### 9.1 Driver 端修复（commit `82d05363`，已存在但未合入）

- 给 `RequestWorkerLease` 在 spillback 场景加 `method_timeout_ms=600000`
- 增加 `grpc_max_ready_idle_resend_count=10`
- **需要合入运行集群版本**

### 9.2 Raylet 端修复（未实现）

`HandleReturnWorkerLease` 中 `worker_exiting=true` 时应直接调 `ReleaseLeaseArgs`：

```cpp
// 修改位置: node_manager.cc:2101 附近
if (!request.worker_exiting()) {
    HandleWorkerAvailable(worker);
} else {
    // ★ 新增: worker 退出时也要释放 pinned args
    RayLease lease;
    local_lease_manager_.CleanupLease(worker, &lease);
}
```

### 9.3 Pin 内存超限时增加 TrySpillback

在 `GrantScheduledLeasesToWorkers` 中，pin 内存超限时增加 `TrySpillback` 尝试：

```cpp
// 修改位置: local_lease_manager.cc:340 附近
if (!success && !args_missing) {
    bool did_spill = TrySpillback(work, is_infeasible);
    if (did_spill) {
        work_it = leases_to_grant_queue.erase(work_it);
        continue;
    }
    work->SetStateWaiting(
        internal::UnscheduledWorkCause::WAITING_FOR_AVAILABLE_PLASMA_MEMORY);
    work_it++;
}
```

### 9.4 修复优先级

| 优先级 | 修复 | 覆盖场景 | 状态 |
|---|---|---|---|
| **P0** | 合入 `82d05363`（driver 端超时） | driver 永久卡死 | 代码已有，需合入 |
| **P0** | `HandleReturnWorkerLease` 中 `worker_exiting=true` 释放 args | raylet 端泄漏候选路径 | 未实现 |
| **P1** | Pin 内存超限时 TrySpillback | 避免单节点死锁 | 未实现 |

---

## 十、环境备注

### 10.1 时区差异

- Head 节点：CST (UTC+8)
- Worker 节点：UTC
- Driver 日志：CST

### 10.2 环境变量 RAY_BACKEND_LOG_LEVEL

- `RAY_BACKEND_LOG_LEVEL=debug` 只对新进程生效
- 已运行的 raylet 无法动态修改日志级别，需重启
- Driver 的 DEBUG 日志通常足够（关键日志在 `NormalTaskSubmitter` 和 `RetryableGrpcClient`）

### 10.3 raylet.out 文件大小

- 文件通常千万行+，直接 grep/cat 会卡死 I/O
- 使用 `tail -c` 或 Python 读尾部方式查看

### 10.4 WezTerm 远端连接

- pane 3 = head 节点 (10.137.44.181)
- pane 5 = worker 节点 8ae1cc62/public-bjx-c26-kce-node272 (10.53.82.106)
- pane 7 之前是 edc216c2 (10.53.149.41)，已断开

### 10.5 不要对未注册 handler 的进程发信号

排查过程中曾用 `os.kill(pid, signal.SIGUSR1)` 误杀作业 48000000（exit code 138），以后不要对未注册 signal handler 的进程发信号。

---

## 十一、相关源码索引

| 路径 | 行号 | 作用 |
|---|---|---|
| `src/ray/raylet/node_manager.cc` | 2080-2114 | `HandleReturnWorkerLease` — worker_exiting=true 时跳过 HandleWorkerAvailable |
| `src/ray/raylet/node_manager.cc` | 2098 | `ReleaseWorkerResources(worker)` 在 HandleWorkerAvailable 之前调用 |
| `src/ray/raylet/node_manager.cc` | 2101 | `if (!request.worker_exiting())` — 跳过 HandleWorkerAvailable 的条件 |
| `src/ray/raylet/node_manager.cc` | 1364-1385 | `HandleWorkerAvailable` — 包含 CleanupLease 调用 |
| `src/ray/raylet/node_manager.cc` | 2357-2380 | `NodeManager::CleanupLease` — 调 local_lease_manager_.CleanupLease |
| `src/ray/raylet/node_manager.cc` | 1425-1571 | `DisconnectClient` — 兜底路径，!IsDead() 时调 CleanupLease |
| `src/ray/raylet/node_manager.cc` | 1448 | `DisconnectClient` 中 ReleaseWorker — leased_workers_ 可能已被 HandleReturnWorkerLease 删除 |
| `src/ray/raylet/scheduling/local_lease_manager.cc` | 766-776 | `CleanupLease` — 包含 ReleaseLeaseArgs 调用 |
| `src/ray/raylet/scheduling/local_lease_manager.cc` | 786-810 | `PinLeaseArgsIfMemoryAvailable` — 先无条件 PinLeaseArgs，再检查超限 |
| `src/ray/raylet/scheduling/local_lease_manager.cc` | 299-349 | `GrantScheduledLeasesToWorkers` — PinLeaseArgs 失败后设为 WAITING_FOR_AVAILABLE_PLASMA_MEMORY |
| `src/ray/raylet/scheduling/local_lease_manager.cc` | 818-840 | `PinLeaseArgs` — 引用计数 + pinned_lease_arguments_bytes_ 累加 |
| `src/ray/raylet/scheduling/local_lease_manager.cc` | 842-860 | `ReleaseLeaseArgs` — 引用计数 -1，归零时释放内存计数 |
| `src/ray/raylet/scheduling/local_lease_manager.cc` | 1048 | `ReleaseWorkerResources` — 只释放 CPU 资源，不清 lease_id |
| `src/ray/raylet/scheduling/local_lease_manager.cc` | 1204-1207 | `DumpState` — 输出 granted_lease_args、pinned_lease_arguments_bytes |
| `src/ray/core_worker/task_submission/normal_task_submitter.cc` | 114-165 | `ReturnWorkerLease` / `OnWorkerIdle` — disconnect_worker=was_error |
| `src/ray/core_worker/task_receiver.cc` | 128 | `worker_exiting=true` 的触发条件（IsIntentionalSystemExit 等） |
| `src/ray/raylet_rpc_client/raylet_client.cc` | 106-127 | `ReturnWorkerLease` — method_timeout_ms=-1 |
| `src/ray/common/ray_config_def.h` | 726 | `max_task_args_memory_fraction = 0.7` |
| `src/ray/raylet/main.cc` | 933 | `max_task_args_memory = capacity × fraction` |
