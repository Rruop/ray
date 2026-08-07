# Ray Lease 调度与 Object Replication Push 交互分析

本文档整理了 Ray 中 **NormalTaskSubmitter**（Core Worker 端）、**ClusterLeaseManager**（Raylet 分布式调度层）、**LocalLeaseManager**（Raylet 本地执行层）三者之间的职责划分、lease 生命周期全流程、依赖拉取与 pin/资源分配的代码逻辑，以及主动 push（object replication）机制对 lease 依赖等待链路的影响分析。

---

## 目录

- [1. 三层架构总览](#1-三层架构总览)
- [2. ClusterLeaseManager vs LocalLeaseManager](#2-clusterleasemanager-vs-localleasemanager)
- [3. NormalTaskSubmitter 的 worker 复用逻辑](#3-normaltasksubmitter-的-worker-复用逻辑)
- [4. RequestWorkerLease RPC 回调处理](#4-requestworkerlease-rpc-回调处理)
- [5. retry\_at\_raylet\_address 重定向机制](#5-retry_at_raylet_address-重定向机制)
- [6. LocalLeaseManager 依赖拉取与 Grant 流程详解](#6-localleasemanager-依赖拉取与-grant-流程详解)
  - [6.1 WaitForLeaseArgsRequests — 依赖拉取](#61-waitforleaseargsrequests--依赖拉取)
  - [6.2 LeaseDependencyManager::RequestLeaseDependencies](#62-leasedependencymanagerrequestleasedependencies)
  - [6.3 对象到达后唤醒：HandleObjectLocal → LeasesUnblocked](#63-对象到达后唤醒handleobjectlocal--leasesunblocked)
  - [6.4 PinLeaseArgsIfMemoryAvailable — pin 参数到 plasma](#64-pinleaseargsifmemoryavailable--pin-参数到-plasma)
  - [6.5 AllocateLocalTaskResources — 扣本地资源](#65-allocatelocaltaskresources--扣本地资源)
  - [6.6 PoppedWorkerHandler — worker 启动结果](#66-poppedworkerhandler--worker-启动结果)
- [7. Reply callback 的保证机制](#7-reply-callback-的保证机制)
- [8. Object Replication Push 对 Lease 依赖链路的影响](#8-object-replication-push-对-lease-依赖链路的影响)
- [9. 关键代码索引](#9-关键代码索引)

---

## 1. 三层架构总览

```
Core Worker 进程                          Raylet 进程
┌─────────────────────────┐              ┌─────────────────────────────────────┐
│ NormalTaskSubmitter     │              │ NodeManager                          │
│  - task queues          │              │  HandleRequestWorkerLease()          │
│  - lease request mgmt   │ ──RPC──────> │  ┌──────────────────────────────┐    │
│  - worker reuse         │ <──reply──── │  │ ClusterLeaseManager          │    │
│  - backlog reporting    │              │  │  (distributed scheduler)      │    │
│                         │              │  │  - leases_to_schedule_        │    │
│                         │              │  │  - infeasible_leases_         │    │
│                         │              │  │  - picks best node in cluster │    │
│                         │              │  └──────────┬───────────────────┘    │
│                         │              │             │ ScheduleOnNode()      │
│                         │              │             │ (if local node)       │
│                         │              │             v                      │
│                         │              │  ┌──────────────────────────────┐    │
│                         │              │  │ LocalLeaseManager            │    │
│                         │              │  │  (local grant scheduler)     │    │
│                         │              │  │  - waiting_lease_queue_      │    │
│                         │              │  │  - leases_to_grant_          │    │
│                         │              │  │  - pins args, acquires res  │    │
│                         │              │  │  - pops worker from pool    │    │
│                         │              │  │  - spillback if needed      │    │
│                         │              │  └──────────────────────────────┘    │
└─────────────────────────┘              └─────────────────────────────────────┘
```

| 组件 | 进程 | 职责 |
|------|------|------|
| `NormalTaskSubmitter` | Core Worker（客户端） | 发起 `RequestWorkerLease` RPC，管理任务队列、worker 复用、lease 请求重试 |
| `ClusterLeaseManager` | Raylet（服务端） | 分布式调度层：决定 lease 分配到集群中**哪个节点** |
| `LocalLeaseManager` | Raylet（服务端） | 本地执行层：拉取依赖、pin 参数到 plasma、扣本地 CPU/GPU、从 WorkerPool 弹 worker、grant lease |

---

## 2. ClusterLeaseManager vs LocalLeaseManager

### 核心区别

| | ClusterLeaseManager | LocalLeaseManager |
|---|---|---|
| **回答的问题** | 集群里**哪个节点**该跑这个 lease？ | 在本节点上**怎么把 lease 实际分给 worker**？ |
| **核心数据结构** | `leases_to_schedule_`（待调度）、`infeasible_leases_`（不可行） | `waiting_lease_queue_`（等依赖）、`leases_to_grant_`（等资源/worker） |
| **核心决策** | `GetBestSchedulableNode()` 遍历集群资源视图选节点 | pin 参数到 plasma、扣本地 CPU/GPU、`worker_pool_.PopWorker()` |
| **spillback** | 选到远程节点 → 回复 `retry_at_raylet_address` 让客户端重试 | 本地资源不足或依赖超时 → 重新选远程节点 spillback |

### 资源视图

ClusterLeaseManager 选节点用的是**可能过时的资源快照**（心跳延迟），选完之后还有一长串工作：

```
各节点 raylet ──heartbeat(资源报告)──> GCS ──广播──> 所有 raylet 的 ClusterResourceScheduler
```

`GetBestSchedulableNode()` 只是查本地内存里的 `ClusterResourceScheduler` 数据结构，**没有任何跨节点 RPC 调用**，开销很低。

### 流转关系

`ClusterLeaseManager::ScheduleOnNode()` 中如果选中的是本地节点，就调用 `local_lease_manager_.QueueAndScheduleLease(work)`，lease 从"集群调度阶段"进入"本地执行阶段"。

```cpp
// src/ray/raylet/scheduling/cluster_lease_manager.cc
void ClusterLeaseManager::ScheduleOnNode(const NodeID &spillback_to,
                                         const std::shared_ptr<internal::Work> &work) {
  if (spillback_to == self_node_id_) {
    local_lease_manager_.QueueAndScheduleLease(work);
    return;
  }
  // ... spillback to remote node ...
}
```

### 为什么不能 core worker 直接去请求远端节点

**实际上已经是 core worker 直接请求的**，只是分了两步：

```
Core Worker ──RPC──> Raylet A (ClusterLeaseManager 选出节点B)
          <── retry_at_raylet_address ──
          ──RPC──> Raylet B (ClusterLeaseManager 确认选本节点 → LocalLeaseManager 执行)
          <── worker_address ──
```

core worker 重试到 Raylet B 时，Raylet B 的 ClusterLeaseManager 会**再次**做一次决策（因为可能资源已经变了），确认选本地后才交给 LocalLeaseManager。LocalLeaseManager 做的恰恰是"直接去请求"之后那个节点上需要完成的全部实际工作。

**一句话总结**：ClusterLeaseManager 是**决策层**（基于快照选节点），LocalLeaseManager 是**执行层**（基于实时状态把 lease 落地）。两者在同一 raylet 内，职责互补，不是冗余。

---

## 3. NormalTaskSubmitter 的 worker 复用逻辑

### NormalTaskSubmitter 确实也检查本地 worker

但它说的"本地"和 LocalLeaseManager 的"本地"是**不同进程**的概念：

- **NormalTaskSubmitter**（Core Worker 进程）：检查自己已经持有的 `active_workers` 里有没有空闲的。如果有，直接通过 `PushTask` RPC 把下一个任务推给该 worker，**不需要再向 raylet 申请新 lease**。这就是 **worker 复用**。

- **LocalLeaseManager**（Raylet 进程）：只有在 NormalTaskSubmitter **没有**可复用 worker、发起 `RequestWorkerLease` RPC 时才介入，负责从 WorkerPool 弹出新 worker、分配资源。

### 关键数据结构

```cpp
// src/ray/core_worker/task_submission/normal_task_submitter.h

struct LeaseEntry {
  rpc::Address addr;                          // 哪个 raylet 租出来的
  int64_t lease_expiration_time;
  google::protobuf::RepeatedPtrField<rpc::ResourceMapEntry> assigned_resources;
  SchedulingKey scheduling_key;
  LeaseID lease_id;
  bool is_busy = false;                        // true = 正在执行任务, false = 空闲
};

absl::flat_hash_map<rpc::Address, LeaseEntry> worker_to_lease_entry_;

struct SchedulingKeyEntry {
  absl::flat_hash_map<LeaseID, rpc::Address> pending_lease_requests;
  LeaseSpecification lease_spec;
  std::deque<TaskSpecification> task_queue;
  absl::flat_hash_set<rpc::Address> active_workers;
  uint32_t num_busy_workers = 0;
  int64_t last_reported_backlog_size = 0;

  bool AllWorkersBusy() const {
    RAY_CHECK_LE(num_busy_workers, active_workers.size());
    return num_busy_workers == active_workers.size();
  }
};

absl::flat_hash_map<SchedulingKey, SchedulingKeyEntry> scheduling_key_entries_;
```

### SubmitTask 入口

```cpp
// src/ray/core_worker/task_submission/normal_task_submitter.cc
void NormalTaskSubmitter::SubmitTask(TaskSpecification task_spec) {
  resolver_.ResolveDependencies(task_spec, [this, task_spec](Status status) mutable {
    // ... 依赖解析 ...
    auto &scheduling_key_entry = scheduling_key_entries_[scheduling_key];
    scheduling_key_entry.task_queue.push_back(std::move(task_spec));

    if (!scheduling_key_entry.AllWorkersBusy()) {
      // 有空闲 worker，找到一个并复用
      for (const auto &active_worker_addr : scheduling_key_entry.active_workers) {
        auto &lease_entry = worker_to_lease_entry_[active_worker_addr];
        if (!lease_entry.is_busy) {
          OnWorkerIdle(active_worker_addr, scheduling_key, false, "", false, ...);
          break;  // 只复用一个
        }
      }
    }
    RequestNewWorkerIfNeeded(scheduling_key);
  });
}
```

### OnWorkerIdle 完整逻辑

`OnWorkerIdle` 的名字有点误导——它不是在检查 worker 是否 idle，而是**被调用者告知"这个 worker 现在空闲了，请分配任务或归还"**。

```cpp
void NormalTaskSubmitter::OnWorkerIdle(
    const rpc::Address &addr, const SchedulingKey &scheduling_key,
    bool was_error, const std::string &error_detail, bool worker_exiting,
    const google::protobuf::RepeatedPtrField<rpc::ResourceMapEntry> &assigned_resources) {
  if (!worker_to_lease_entry_.contains(addr)) return;
  auto &lease_entry = worker_to_lease_entry_[addr];
  auto &scheduling_key_entry = scheduling_key_entries_[scheduling_key];
  auto &current_queue = scheduling_key_entry.task_queue;

  // 判断是否需要归还 worker
  if ((was_error || worker_exiting ||
       current_time_ms() > lease_entry.lease_expiration_time) ||
      current_queue.empty()) {
    // 归还 worker 给 raylet
    if (!lease_entry.is_busy) {
      ReturnWorkerLease(addr, was_error, error_detail, worker_exiting, scheduling_key);
    }
  } else {
    // worker 健康、lease 有效、有排队任务 → 复用
    auto client = core_worker_client_pool_->GetOrConnect(addr);
    if (!current_queue.empty() && !lease_entry.is_busy) {
      auto task_spec = std::move(current_queue.front());
      current_queue.pop_front();
      lease_entry.is_busy = true;
      scheduling_key_entry.num_busy_workers++;
      executing_tasks_.emplace(task_spec.TaskId(), addr);
      PushNormalTask(addr, client, scheduling_key, std::move(task_spec), ...);
    }
    CancelWorkerLeaseIfNeeded(scheduling_key);
  }
  RequestNewWorkerIfNeeded(scheduling_key);
}
```

### OnWorkerIdle 被调用的三个场景

| 场景 | 触发位置 | `is_busy` 状态 |
|------|----------|----------------|
| **A. lease 刚批准** | `RequestWorkerLease` 回调中 | `false`（刚租来） |
| **B. worker 执行完一个任务** | `PushNormalTask` 回调中 | 先置 `false`，再调 `OnWorkerIdle` |
| **C. 新任务提交时发现有空闲 worker** | `SubmitTask` 中遍历 `active_workers` | `false`（找到了才调） |

### is_busy 状态流转

| 时机 | 操作 | 位置 |
|------|------|------|
| lease 刚批准 | 默认 `false` | `AddWorkerLeaseClient` |
| 从队列取出任务推给 worker | `is_busy = true`, `num_busy_workers++` | `OnWorkerIdle` 内部 |
| worker 执行完任务回调 | `is_busy = false`, `num_busy_workers--` | `PushNormalTask` 回调 |

### 决策流程图

```
OnWorkerIdle 被调用
        │
        ├── was_error? / worker_exiting? / lease过期? / 队列空?
        │        │
        │    是  │ ──> is_busy?
        │        │       ├── 是 → 什么都不做（等任务回调后再处理）
        │        │       └── 否 → ReturnWorkerLease (归还 worker 给 raylet)
        │        │
        │    否  │ ──> 队列非空 && !is_busy?
        │                ├── 是 → 弹出队首任务, is_busy=true, num_busy++,
        │                │        PushNormalTask (RPC 推任务给 worker)
        │                └── 否 → 什么都不做
        │
        └── CancelWorkerLeaseIfNeeded (队列空则取消 pending lease)
             │
             └── RequestNewWorkerIfNeeded (检查是否还需要更多 worker)
```

### RequestNewWorkerIfNeeded 决策树

```cpp
void NormalTaskSubmitter::RequestNewWorkerIfNeeded(const SchedulingKey &scheduling_key,
                                                   const rpc::Address *raylet_address) {
  // 1. 限流检查
  if (pending_lease_requests >= max) return;
  // 2. 有空闲 worker?
  if (!AllWorkersBusy()) return;
  // 3. 有排队任务?
  if (task_queue.empty()) return;
  // 4. 所有任务都有 pending lease?
  if (task_queue.size() <= pending_lease_requests.size()) return;
  // 5. 生成 LeaseID，选节点，发送 RPC
  // ...
}
```

### PushNormalTask 回调 — worker 完成任务后

```cpp
// src/ray/core_worker/task_submission/normal_task_submitter.cc
client->PushNormalTask(std::move(request),
    [this, task_spec, task_id, scheduling_key, addr, ...](
        Status status, const rpc::PushTaskReply &reply) {
      absl::MutexLock lock(&mu_);
      executing_tasks_.erase(task_id);

      // 标记 worker 空闲
      auto &lease_entry = worker_to_lease_entry_[addr];
      lease_entry.is_busy = false;
      scheduling_key_entry.num_busy_workers--;

      // 处理错误...

      // 尝试复用或归还
      OnWorkerIdle(addr, scheduling_key,
                   /*was_error=*/!status.ok(),
                   /*error_detail=*/status.message(),
                   /*worker_exiting=*/reply.worker_exiting(),
                   assigned_resources);
    });
```

---

## 4. RequestWorkerLease RPC 回调处理

### IsLeaseQueued + AddReplyCallback — 重复/重试请求去重

当 core worker 因网络超时或消息乱序，对同一个 `LeaseID` 发送多次 `RequestWorkerLease` RPC 时，`NodeManager::HandleRequestWorkerLease` 依次检查 `ClusterLeaseManager` 和 `LocalLeaseManager`：

```cpp
// src/ray/raylet/node_manager.cc
void NodeManager::HandleRequestWorkerLease(rpc::RequestWorkerLeaseRequest request,
                                           rpc::RequestWorkerLeaseReply *reply,
                                           rpc::SendReplyCallback send_reply_callback) {
  auto lease_id = LeaseID::FromBinary(request.lease_spec().lease_id());

  // 如果 lease 已被批准，这是重试，直接返回已有 worker 地址
  if (leased_workers_.contains(lease_id)) {
    // ... reply with existing worker address ...
    send_reply_callback(Status::OK(), nullptr, nullptr);
    return;
  }

  RayLease lease{std::move(*request.mutable_lease_spec())};
  auto send_reply_callback_wrapper = [/*...*/](Status status, ...) {
    send_reply_callback(status, nullptr, nullptr);
  };

  // Step 1: 检查 cluster-level 调度队列
  if (cluster_lease_manager_.IsLeaseQueued(scheduling_class, lease_id)) {
    RAY_CHECK(cluster_lease_manager_.AddReplyCallback(
        scheduling_class, lease_id, std::move(send_reply_callback_wrapper), reply));
    return;
  }

  // Step 2: 检查 local-level grant 队列
  if (local_lease_manager_.IsLeaseQueued(scheduling_class, lease_id)) {
    RAY_CHECK(local_lease_manager_.AddReplyCallback(
        scheduling_class, lease_id, std::move(send_reply_callback_wrapper), reply));
    return;
  }

  // Step 3: 全新请求，入队调度
  cluster_lease_manager_.QueueAndScheduleLease(
      std::move(lease), request.grant_or_reject(),
      request.is_selected_based_on_locality(),
      {internal::ReplyCallback(std::move(send_reply_callback_wrapper), reply)});
}
```

`AddReplyCallback` 把新的 reply callback 挂到已有的 `Work` 对象上，最终所有 callback 一起被 reply。接口注释说明：

> We don't overwrite the existing reply callback since due to message reordering we may receive the retry before the initial request.

### 成功分支

当 raylet 回复 `worker_address` 非空时（lease 成功）：

```cpp
// normal_task_submitter.cc, RequestWorkerLease 回调中
if (!reply.worker_address().node_id().empty()) {
  // 第一步：注册 worker 客户端状态
  AddWorkerLeaseClient(reply.worker_address(), raylet_address,
                       reply.resource_mapping(), scheduling_key, lease_id);
  // 第二步：立刻尝试派发任务
  OnWorkerIdle(reply.worker_address(), scheduling_key,
               /*was_error=*/false, /*error_detail*/ "",
               /*worker_exiting=*/false, reply.resource_mapping());
}
```

### AddWorkerLeaseClient

```cpp
void NormalTaskSubmitter::AddWorkerLeaseClient(
    const rpc::Address &worker_address, const rpc::Address &raylet_address,
    const google::protobuf::RepeatedPtrField<rpc::ResourceMapEntry> &assigned_resources,
    const SchedulingKey &scheduling_key, const LeaseID &lease_id) {
  core_worker_client_pool_->GetOrConnect(worker_address);

  LeaseEntry lease_entry;
  lease_entry.addr = raylet_address;
  lease_entry.lease_expiration_time = current_time_ms() + lease_timeout_ms_;
  lease_entry.assigned_resources = assigned_resources;
  lease_entry.scheduling_key = scheduling_key;
  lease_entry.lease_id = lease_id;
  lease_entry.is_busy = false;

  worker_to_lease_entry_[worker_address] = std::move(lease_entry);
  scheduling_key_entries_[scheduling_key].active_workers.insert(worker_address);
}
```

### 回调完整分支表

| Case | 条件 | Action |
|------|------|--------|
| **成功** (worker granted) | `status.ok() && !canceled() && !rejected() && worker_address().node_id() 非空` | `AddWorkerLeaseClient` + `OnWorkerIdle` 推送任务 |
| **重定向** (spillback) | `status.ok() && !canceled() && !rejected() && worker_address() 为空` | `RequestNewWorkerIfNeeded(scheduling_key, &retry_at_raylet_address())` |
| **拒绝** (spillback failed) | `status.ok() && rejected()` | `RequestNewWorkerIfNeeded(scheduling_key)` 回原节点重试，`RAY_CHECK(is_spillback)` |
| **取消-致命** | `status.ok() && canceled() && failure_type ∈ {RUNTIME_ENV_SETUP_FAILED, PLACEMENT_GROUP_REMOVED, UNSCHEDULABLE, WORKER_STARTUP_FAILED}` | 失败所有排队任务 (`FailPendingTask`) |
| **取消-非致命** | `status.ok() && canceled() && other failure_type` | `RequestNewWorkerIfNeeded` 重试 |
| **RPC 失败-远程** | `!status.ok() && remote raylet` | `RequestNewWorkerIfNeeded` 回本地重试 |
| **RPC 失败-本地** | `!status.ok() && local raylet` | worker 退出 / driver 失败所有任务 (`LOCAL_RAYLET_DIED`) |

关键区分：raylet 要么设 `worker_address`（成功），要么设 `retry_at_raylet_address`（重定向），**永远不会同时设**。通过检查 `worker_address().node_id()` 是否为空来区分。

---

## 5. retry_at_raylet_address 重定向机制

### 发送端（Raylet）

在 `ClusterLeaseManager::ScheduleOnNode()` 中，当选中的是远程节点时：

```cpp
// src/ray/raylet/scheduling/cluster_lease_manager.cc
void ClusterLeaseManager::ScheduleOnNode(const NodeID &spillback_to,
                                         const std::shared_ptr<internal::Work> &work) {
  if (spillback_to == self_node_id_) {
    local_lease_manager_.QueueAndScheduleLease(work);
    return;
  }

  if (work->grant_or_reject_) {
    // spillback 请求无法再 spill → 拒绝
    for (const auto &reply_callback : work->reply_callbacks_) {
      reply_callback.reply_->set_rejected(true);
      reply_callback.send_reply_callback_(Status::OK(), nullptr, nullptr);
    }
    return;
  }

  // 在远程节点的资源视图上预扣资源
  cluster_resource_scheduler_.AllocateRemoteTaskResources(
      scheduling::NodeID(spillback_to.Binary()),
      lease_spec.GetRequiredResources().GetResourceMap());

  // 设置重定向地址
  auto node_info = get_node_info_(spillback_to);
  for (const auto &reply_callback : work->reply_callbacks_) {
    auto reply = reply_callback.reply_;
    reply->mutable_retry_at_raylet_address()->set_ip_address(
        (*node_info).node_manager_address());
    reply->mutable_retry_at_raylet_address()->set_port((*node_info).node_manager_port());
    reply->mutable_retry_at_raylet_address()->set_node_id(spillback_to.Binary());
    reply_callback.send_reply_callback_(Status::OK(), nullptr, nullptr);
  }
}
```

`LocalLeaseManager::Spillback()` 有完全相同的逻辑（本地资源不足时 spill 到远程）。

### 接收端（Core Worker）

```cpp
// normal_task_submitter.cc, RequestWorkerLease 回调中
else {
  // retry_at_raylet_address 被设置了 → 重定向
  RAY_CHECK(!is_spillback);
  RequestNewWorkerIfNeeded(scheduling_key, &reply.retry_at_raylet_address());
}
```

### 重试时的变化

`RequestNewWorkerIfNeeded` 收到 `raylet_address != nullptr` 时：
- `is_spillback = true`
- 直接连目标 raylet，不再调 `lease_policy_->GetBestNodeForLease`
- 传 `grant_or_reject=true` — 意味着如果目标节点也满足不了，会直接返回 `rejected=true` 而不是再次 spillback，避免无限跳转

---

## 6. LocalLeaseManager 依赖拉取与 Grant 流程详解

### 完整时序图

```
QueueAndScheduleLease (from ClusterLeaseManager)
    │
    v
WaitForLeaseArgsRequests
    |--- args ready ---> leases_to_grant_[sched_key]
    |--- args not ready ---> waiting_lease_queue_ (registered with LeaseDependencyManager)
    |
    v
ScheduleAndGrantLeases
    |--- GrantScheduledLeasesToWorkers (leases_to_grant_ -> try allocate resources -> PopWorker)
    |       |--- resources fail -> TrySpillback -> Spillback (to remote node) or WAITING_FOR_RESOURCES_AVAILABLE
    |       |--- resources ok -> PopWorker -> PoppedWorkerHandler -> Grant (worker address sent to client)
    |--- SpillWaitingLeases (waiting_lease_queue_ -> if deps blocked, try spill to remote node)
    |
LeasesUnblocked (callback from dependency manager when args arrive)
    |--- moves from waiting_lease_queue_ to leases_to_grant_ -> ScheduleAndGrantLeases again
```

### 6.1 WaitForLeaseArgsRequests — 依赖拉取

```cpp
// src/ray/raylet/scheduling/local_lease_manager.cc
void LocalLeaseManager::WaitForLeaseArgsRequests(std::shared_ptr<internal::Work> work) {
  const auto &lease = work->lease_;
  const auto &lease_id = lease.GetLeaseSpecification().LeaseId();
  const auto &scheduling_key = lease.GetLeaseSpecification().GetSchedulingClass();
  auto object_ids = lease.GetLeaseSpecification().GetDependencies();

  if (!object_ids.empty()) {
    bool args_ready = lease_dependency_manager_.RequestLeaseDependencies(
        lease_id, object_ids,
        {lease.GetLeaseSpecification().GetTaskName(),
         lease.GetLeaseSpecification().IsRetry()});

    if (args_ready) {
      // 所有依赖已经在本地 plasma 中
      leases_to_grant_[scheduling_key].emplace_back(std::move(work));
    } else {
      // 有依赖不在本地，需要从远程拉取
      auto it = waiting_lease_queue_.insert(waiting_lease_queue_.end(), std::move(work));
      RAY_CHECK(waiting_leases_index_.emplace(lease_id, it).second);
      // 标记本地资源 footprint：正在拉参数，占用一部分 object store memory
      cluster_resource_scheduler_.GetLocalResourceManager()
          .MaybeMarkFootprintAsBusy(WorkFootprint::PULLING_TASK_ARGUMENTS);
    }
  } else {
    // 没有依赖，直接进 grant 队列
    leases_to_grant_[scheduling_key].emplace_back(std::move(work));
  }
}
```

### 6.2 LeaseDependencyManager::RequestLeaseDependencies

```cpp
// src/ray/raylet/lease_dependency_manager.cc
bool LeaseDependencyManager::RequestLeaseDependencies(
    const LeaseID &lease_id,
    const std::vector<rpc::ObjectReference> &required_objects,
    const TaskMetricsKey &task_key) {
  const auto required_ids = ObjectRefsToIds(required_objects);
  absl::flat_hash_set<ObjectID> deduped_ids(required_ids.begin(), required_ids.end());
  auto inserted = queued_lease_requests_.emplace(
      lease_id, std::make_unique<LeaseDependencies>(std::move(deduped_ids), ...));
  auto &lease_entry = inserted.first->second;

  // 1. 对每个依赖对象，注册到 required_objects_ 反向索引池
  for (const auto &ref : required_objects) {
    const auto obj_id = ObjectRefToId(ref);
    auto it = GetOrInsertRequiredObject(obj_id, ref);
    it->second.dependent_leases.insert(lease_id);  // "lease_id 在等 obj_id"
  }

  // 2. 检查哪些 object 已经在本地
  for (const auto &obj_id : lease_entry->dependencies_) {
    if (local_objects_.contains(obj_id)) {
      lease_entry->DecrementMissingDependencies();  // 本地已有，缺少数 -1
    }
  }

  // 3. 对缺失的 object，发起到远程节点的拉取请求
  if (!required_objects.empty()) {
    lease_entry->pull_request_id_ =
        object_manager_.Pull(required_objects, BundlePriority::TASK_ARGS, task_key);
    // ObjectManager.Pull → PullManager.Pull → 查 GCS 获取哪些远程节点持有该 object
    //   → 向远程节点的 ObjectManager 发 TCP 请求拉取 object chunk
    //   → chunk 到达后写入本地 plasma store
  }

  // 4. 返回是否所有依赖都已就绪
  return lease_entry->num_missing_dependencies_ == 0;
}
```

### 6.3 对象到达后唤醒：HandleObjectLocal → LeasesUnblocked

当 object 写入本地 plasma store 并 seal 后，plasma store 触发 `add_object_callback`：

```cpp
// src/ray/raylet/main.cc:800
/*add_object_callback=*/
[&](const ray::ObjectInfo &object_info,
    plasma::flatbuf::ObjectSource source) {
  main_service.post(
      [&object_manager, &node_manager, object_info, source]() {
        object_manager->HandleObjectAdded(object_info);
        node_manager->HandleObjectLocal(object_info, source);  // 关键入口
      }, "ObjectManager.ObjectAdded");
},
```

`HandleObjectLocal` 不关心 source 是 `ReceivedByPull` 还是 `ReceivedByPush`：

```cpp
// src/ray/raylet/node_manager.cc
void NodeManager::HandleObjectLocal(const ObjectInfo &object_info,
                                    plasma::flatbuf::ObjectSource source) {
  const ObjectID &object_id = object_info.object_id;

  // 1. 通知 lease dependency manager：这个 object 到本地了
  const auto ready_lease_ids = lease_dependency_manager_.HandleObjectLocal(object_id);

  // 2. 唤醒等待这个 object 的 lease
  local_lease_manager_.LeasesUnblocked(ready_lease_ids);

  // 3. 通知 wait manager
  wait_manager_.HandleObjectLocal(object_id);

  // 4. 判断是否是 replication push 到达的，用于统计
  if (source == plasma::flatbuf::ObjectSource::ReceivedByPush &&
      object_manager_.ConsumeReplicationPushReceived(object_id)) {
    object_replication_succeeded_.Record(1);
    object_replication_bytes_.Record(static_cast<double>(object_info.data_size));
  }

  // 5. 触发 spill if needed
  SpillIfOverPrimaryObjectsThreshold();
}
```

`LeaseDependencyManager::HandleObjectLocal` 通过反向索引精确找到等这个 object 的 lease：

```cpp
// src/ray/raylet/lease_dependency_manager.cc
std::vector<LeaseID> LeaseDependencyManager::HandleObjectLocal(
    const ray::ObjectID &object_id) {
  auto inserted = local_objects_.insert(object_id);
  RAY_CHECK(inserted.second) << "Local object was already local " << object_id;

  std::vector<LeaseID> ready_lease_ids;
  auto object_entry = required_objects_.find(object_id);
  if (object_entry != required_objects_.end()) {
    // 只遍历等这个 object 的 lease，不遍历所有 lease
    for (const auto &dependent_lease_id : object_entry->second.dependent_leases) {
      auto &lease_entry = queued_lease_requests_[dependent_lease_id];
      lease_entry->DecrementMissingDependencies();  // 缺少数 -1
      if (lease_entry->num_missing_dependencies_ == 0) {
        ready_lease_ids.push_back(dependent_lease_id);  // 全到齐了！
      }
    }
  }
  return ready_lease_ids;
}
```

然后 `LeasesUnblocked` 把 ready 的 lease 从 waiting 队列移到 grant 队列：

```cpp
// src/ray/raylet/scheduling/local_lease_manager.cc
void LocalLeaseManager::LeasesUnblocked(const std::vector<LeaseID> &ready_ids) {
  if (ready_ids.empty()) return;

  for (const auto &lease_id : ready_ids) {
    auto it = waiting_leases_index_.find(lease_id);
    if (it != waiting_leases_index_.end()) {
      auto work = *it->second;
      const auto &scheduling_key = work->lease_.GetLeaseSpecification().GetSchedulingClass();
      leases_to_grant_[scheduling_key].push_back(work);
      waiting_lease_queue_.erase(it->second);
      waiting_leases_index_.erase(it);
      if (waiting_lease_queue_.empty()) {
        cluster_resource_scheduler_.GetLocalResourceManager()
            .MarkFootprintAsIdle(WorkFootprint::PULLING_TASK_ARGUMENTS);
      }
    }
  }
  ScheduleAndGrantLeases();  // 再次触发调度循环
}
```

### 6.4 PinLeaseArgsIfMemoryAvailable — pin 参数到 plasma

在 `GrantScheduledLeasesToWorkers` 中，对每个 lease 依次执行：

```cpp
// src/ray/raylet/scheduling/local_lease_manager.cc
bool args_missing = false;
bool success = PinLeaseArgsIfMemoryAvailable(spec, &args_missing);
```

```cpp
bool LocalLeaseManager::PinLeaseArgsIfMemoryAvailable(
    const LeaseSpecification &lease_spec, bool *args_missing) {
  std::vector<std::unique_ptr<RayObject>> args;
  const auto &deps = lease_spec.GetDependencyIds();

  if (!deps.empty()) {
    // ① 从 plasma store 获取对象引用
    if (!get_lease_arguments_(deps, &args)) {
      *args_missing = true;   // 获取失败（对象不存在）
      return false;
    }
    // ② 检查每个参数是否为 null（可能已被 LRU 淘汰）
    for (size_t i = 0; i < deps.size(); i++) {
      if (args[i] == nullptr) {
        *args_missing = true;   // 参数被 evict 了
        return false;
      }
    }
  }

  *args_missing = false;
  // ③ 计算参数总大小
  size_t lease_arg_bytes = 0;
  for (auto &arg : args) {
    lease_arg_bytes += arg->GetSize();
  }
  // ④ Pin：把 RayObject 引用存入 pinned_lease_arguments_，防止 plasma LRU 淘汰
  PinLeaseArgs(lease_spec, std::move(args));
  // ⑤ 检查 pinned 总量是否超过内存上限
  if (max_pinned_lease_arguments_bytes_ == 0) return true;  // 没有设置上限
  if (pinned_lease_arguments_bytes_ > max_pinned_lease_arguments_bytes_) {
    // 超了 → 释放刚 pin 的参数，返回 false
    ReleaseLeaseArgs(lease_spec.LeaseId());
    return false;  // 等其他 lease 归还参数后再试
  }
  return true;  // pin 成功
}
```

**PinLeaseArgs 内部**：把 `unique_ptr<RayObject>` 存入 `pinned_lease_arguments_`，持有引用保持对象在 plasma 中不被淘汰，refcount 累加。

**三种返回结果及处理**：

| 返回 | `args_missing` | 处理 |
|------|----------------|------|
| `false` | `true` | 参数被 evict → 退回 `waiting_lease_queue_` 队首，重新拉取，不 reply |
| `false` | `false` | plasma 内存不够 pin → `WAITING_FOR_AVAILABLE_PLASMA_MEMORY`，不 reply |
| `true` | `false` | pin 成功 → 继续扣资源 |

```cpp
if (!success) {
  if (args_missing) {
    // 参数被 evict → 退回 waiting 队首
    auto it = waiting_lease_queue_.insert(waiting_lease_queue_.begin(), std::move(*work_it));
    waiting_leases_index_.emplace(lease_id, it);
    MaybeMarkFootprintAsBusy(WorkFootprint::PULLING_TASK_ARGUMENTS);
    work_it = leases_to_grant_queue.erase(work_it);
  } else {
    // plasma 内存不够 → 等待
    work->SetStateWaiting(UnscheduledWorkCause::WAITING_FOR_AVAILABLE_PLASMA_MEMORY);
    work_it++;
  }
  continue;
}
```

### 6.5 AllocateLocalTaskResources — 扣本地资源

pin 成功后，继续执行：

```cpp
// src/ray/raylet/scheduling/local_lease_manager.cc
auto allocated_instances = std::make_shared<TaskResourceInstances>();
bool schedulable =
    !cluster_resource_scheduler_.GetLocalResourceManager().IsLocalNodeDraining() &&
    cluster_resource_scheduler_.GetLocalResourceManager()
        .AllocateLocalTaskResources(spec.GetRequiredResources().GetResourceMap(),
                                    allocated_instances);
```

两个检查：
1. `IsLocalNodeDraining()` — 节点是否正在排空（准备下线）
2. `AllocateLocalTaskResources()` — 基于**本地实时资源**尝试扣减 CPU/GPU/自定义资源

**扣减失败**：

```cpp
if (!schedulable) {
  ReleaseLeaseArgs(lease_id);  // 释放刚 pin 的参数
  bool did_spill = TrySpillback(work, is_infeasible);
  if (!did_spill) {
    // 没有远程节点可用，留在本地等资源
    work->SetStateWaiting(UnscheduledWorkCause::WAITING_FOR_RESOURCES_AVAILABLE);
    break;  // 同一 scheduling class 后面的 lease 也会资源不足
  }
  work_it = leases_to_grant_queue.erase(work_it);  // spill 成功，移除
}
```

**扣减成功**：

```cpp
else {
  sched_cls_info.granted_leases.insert(lease_id);
  work->allocated_instances_ = allocated_instances;
  work->SetStateWaitingForWorker();  // 状态 → 等待 worker 启动

  // 从 WorkerPool 弹出一个 worker（异步）
  worker_pool_.PopWorker(
      spec,
      [this, lease_id, scheduling_class, work, ...](
          const std::shared_ptr<WorkerInterface> worker,
          PopWorkerStatus status,
          const std::string &runtime_env_setup_error_message) -> bool {
        return PoppedWorkerHandler(worker, status, lease_id, ...);
      });
  work_it++;  // work 留在队列中，状态为 WAITING_FOR_WORKER
}
```

### 6.6 PoppedWorkerHandler — worker 启动结果

```cpp
bool LocalLeaseManager::PoppedWorkerHandler(worker, status, lease_id, ...) {
  if (!worker) {
    // worker 启动失败
    // 1. 释放已扣资源
    ReleaseWorkerResources(work->allocated_instances_);
    // 2. 释放 pinned 参数
    ReleaseLeaseArgs(lease_id);

    if (status == RuntimeEnvCreationFailed) {
      // runtime env 失败 → 取消 lease，reply canceled
      CancelLeases(..., SCHEDULING_CANCELLED_RUNTIME_ENV_SETUP_FAILED, ...);
    } else if (status == JobFinished) {
      // job 结束 → 直接移除
      erase_from_leases_to_grant_queue_fn(work, scheduling_class);
    } else {
      // 其他失败 → 重置为 WAITING 状态
      // 超过最大重试次数 (pop_worker_max_retries) 则取消
      work->IncrementPopWorkerRetries();
      auto max_retries = RayConfig::instance().pop_worker_max_retries();
      if (max_retries >= 0 && work->GetPopWorkerRetries() > max_retries) {
        CancelLeases(..., SCHEDULING_CANCELLED_WORKER_STARTUP_FAILED, ...);
      } else {
        work->SetStateWaiting(cause);
      }
    }
    return false;
  }

  // worker 启动成功 → Grant！
  Grant(worker, leased_workers_, allocated_instances, lease, reply_callbacks);
  erase_from_leases_to_grant_queue_fn(work, scheduling_class);
  return true;
}
```

**Grant()** — 真正发送 reply：

```cpp
void LocalLeaseManager::Grant(worker, leased_workers_, allocated_instances, lease, reply_callbacks) {
  // 填入 worker 地址
  reply->mutable_worker_address()->set_ip_address(worker->IpAddress());
  reply->mutable_worker_address()->set_port(worker->Port());
  reply->mutable_worker_address()->set_worker_id(worker->WorkerId().Binary());

  // 这才真正发送 reply！
  for (const auto &reply_callback : reply_callbacks) {
    reply_callback.send_reply_callback_(Status::OK(), nullptr, nullptr);
  }
}
```

---

## 7. Reply callback 的保证机制

### 不是"等到拉完再返回"，而是 callback 只在终态被调用

reply callback 在对象拉完之前**根本不会被调用**。这不是通过某种"等待"机制实现的，而是通过**代码路径的结构**保证的——reply callback 只是存在 `Work` 对象上的数据，只有在 `Grant()`（或 `Spillback()`/`CancelLeases()`）时才被调用。

当 `HandleRequestWorkerLease` 接收 RPC 时：

```cpp
auto send_reply_callback_wrapper = [...](Status status, ...) {
  send_reply_callback(status, nullptr, nullptr);  // 这才是真正发送 reply
};

// 把 reply callback 作为数据存入 Work 对象
cluster_lease_manager_.QueueAndScheduleLease(
    std::move(lease), ...,
    {internal::ReplyCallback(std::move(send_reply_callback_wrapper), reply)});
```

`send_reply_callback_wrapper` 被存入 `Work::reply_callbacks_` 列表，之后**只有显式调用它才会回复**。

### 终态只有三种

| 终态 | reply 内容 | 什么时候触发 |
|------|-----------|-------------|
| 成功 | `worker_address` | worker 启动完成 (`PoppedWorkerHandler` → `Grant`) |
| 重定向 | `retry_at_raylet_address` | 资源不足/依赖超时 → spillback 到远程节点 |
| 取消 | `canceled=true` | unschedulable / runtime env 失败 / worker 启动失败超限 |

中间所有等待状态（拉依赖、等 plasma 内存、等资源、等 worker 启动）都**不 reply**，core worker 的 RPC 一直 hang 住。

### 计数器机制保证所有依赖就绪后才唤醒

```cpp
// LeaseDependencyManager 内部
struct LeaseDependencies {
  absl::flat_hash_set<ObjectID> dependencies_;
  int64_t num_missing_dependencies_;  // 初始值 = 依赖总数
  uint64_t pull_request_id_;
  // ...
};
```

`num_missing_dependencies_` 是一个计数器，每有一个 object 到达就 -1，**只有归零才会把 lease_id 放入 `ready_lease_ids`**，进而触发 `LeasesUnblocked` → `ScheduleAndGrantLeases` → 最终 `Grant()` → reply。

---

## 8. Object Replication Push 对 Lease 依赖链路的影响

### 背景

本分支新增了主动 push 机制（`PushForReplication`），允许节点在对象创建后主动推送到其他节点，而不需要目标节点发起 pull 请求。

### 核心结论：不会出问题

push 和 pull 最终都通过同一条路径唤醒 lease：

```
Push 路径:
  远程节点 PushForReplication → 发 chunk → 本地 ReceiveReplicationPushChunk
  → buffer_pool_.CreateChunk + WriteChunk → 所有 chunk 到齐 → plasma seal
  → add_object_callback → HandleObjectLocal → lease_dependency_manager_.HandleObjectLocal
  → LeasesUnblocked → lease 进入 grant 队列

Pull 路径:
  LeaseDependencyManager → object_manager_.Pull → 远程节点响应 → 发 chunk
  → 本地 ReceivePullChunk → buffer_pool_.CreateChunk + WriteChunk → plasma seal
  → add_object_callback → HandleObjectLocal → 同上
```

关键在于 plasma store 的 `add_object_callback` **不区分**对象来源（pull 还是 push），任何对象写入并 seal 后都会触发同一个回调：

```cpp
// src/ray/raylet/main.cc:800
add_object_callback = [&](const ObjectInfo &object_info,
                           plasma::flatbuf::ObjectSource source) {
  main_service.post([&]() {
    object_manager->HandleObjectAdded(object_info);
    node_manager->HandleObjectLocal(object_info, source);  // 不关心 source
  }, "ObjectManager.ObjectAdded");
};
```

### 竞争场景分析

**场景 1：lease 正在 waiting 等 pull 拉取依赖，push 先到达**

1. push chunk 到达 → `ReceiveReplicationPushChunk` → 检查 `has_active_pull = true` → **拒绝 push chunk**
2. pull chunk 到达 → `ReceivePullChunk` → 正常写入 → seal → `HandleObjectLocal` → `LeasesUnblocked` ✅

```cpp
// src/ray/object_manager/object_manager.cc
bool ObjectManager::ReceiveReplicationPushChunk(...) {
  const bool has_active_pull = pull_manager_->IsObjectActive(object_id);
  const bool object_already_local = local_objects_.count(object_id) > 0;

  {
    absl::MutexLock lock(&replication_push_state_mu_);
    if (has_active_pull || object_already_local) {
      // 拒绝 push：pull 会负责 buffer 和 seal
      replication_push_terminated_objects_.emplace(object_id, now_ms);
      replication_push_objects_.erase(object_id);
      return false;
    }
  }
  // ... 接受 push chunk ...
}
```

**场景 2：lease 还没发起 pull，push 先把对象推过来**

1. push chunk 到达 → `has_active_pull = false`, `object_already_local = false` → **接受 push chunk**
2. 所有 chunk 到齐 → seal → `HandleObjectLocal` → `lease_dependency_manager_.HandleObjectLocal(object_id)`
3. 此时如果有 lease 刚好在等这个 object，计数器归零 → `LeasesUnblocked` ✅
4. 如果没有 lease 在等，`required_objects_.find(object_id)` 找不到 → 返回空 `ready_lease_ids` → `LeasesUnblocked` 收到空列表，直接 return ✅

**场景 3：push 和 pull 同时写同一个 object**

`ReceivePullChunk` 有二次检查保护：

```cpp
bool ObjectManager::ReceivePullChunk(...) {
  if (!pull_manager_->IsObjectActive(object_id)) {
    return false;  // pull 已不活跃（可能 push 已经完成了）
  }
  auto chunk_status = buffer_pool_.CreateChunk(
      object_id, owner_address, data_size, metadata_size, chunk_index,
      plasma::flatbuf::ObjectSource::ReceivedByPull);
  if (!pull_manager_->IsObjectActive(object_id)) {
    // 二次检查，pull 可能在 CreateChunk 期间被取消
    buffer_pool_.AbortCreate(object_id);
    {
      absl::MutexLock lock(&replication_push_state_mu_);
      replication_push_objects_.erase(object_id);
    }
    return false;
  }
  // ...
}
```

`ReceiveReplicationPushChunk` 在 `CreateChunk` 失败后也会检查：

```cpp
bool ObjectManager::ReceiveReplicationPushChunk(...) {
  // ...
  auto chunk_status = buffer_pool_.CreateChunk(
      object_id, owner_address, data_size, metadata_size, chunk_index,
      plasma::flatbuf::ObjectSource::ReceivedByPush);

  if (chunk_status.ok()) {
    buffer_pool_.WriteChunk(object_id, data_size, metadata_size, chunk_index, data);
    {
      absl::MutexLock lock(&replication_push_state_mu_);
      replication_push_objects_[object_id] = now_ms;
    }
    return true;
  } else {
    // CreateChunk 失败 → 检查 pull 是否在此期间变活跃
    const bool has_active_pull_after_create = pull_manager_->IsObjectActive(object_id);
    {
      absl::MutexLock lock(&replication_push_state_mu_);
      replication_push_terminated_objects_.emplace(object_id, now_ms);
      if (has_active_pull_after_create) {
        replication_push_objects_.erase(object_id);
      }
    }
    if (!has_active_pull_after_create) {
      buffer_pool_.AbortCreate(object_id);
    }
    return false;
  }
}
```

### TTL 清理机制

为防止 push 发送方中途死亡导致部分创建的 plasma buffer 泄漏，`CleanupExpiredReplicationPushState` 定期清理过期的 push 状态：

```cpp
// src/ray/object_manager/object_manager.cc
void ObjectManager::CleanupExpiredReplicationPushState() {
  const int64_t ttl_ms = RayConfig::instance().object_replication_state_ttl_ms();
  const int64_t now_ms = current_time_ms();
  std::vector<ObjectID> expired_pushes;
  {
    absl::MutexLock lock(&replication_push_state_mu_);
    for (const auto &[object_id, last_active_ms] : replication_push_objects_) {
      if (now_ms - last_active_ms >= ttl_ms) {
        expired_pushes.push_back(object_id);
      }
    }
    for (const auto &object_id : expired_pushes) {
      replication_push_objects_.erase(object_id);
    }
    absl::erase_if(replication_push_terminated_objects_,
                   [now_ms, ttl_ms](const auto &entry) {
                     return now_ms - entry.second >= ttl_ms;
                   });
  }
  for (const auto &object_id : expired_pushes) {
    if (local_objects_.count(object_id) > 0) continue;  // 已 seal，无需处理
    // 发送方可能死了，abort 部分创建的 plasma buffer
    buffer_pool_.AbortCreate(object_id);
  }
}
```

### 总结

| 场景 | 结果 | 安全性 |
|------|------|--------|
| pull 活跃时 push 到达 | push 被拒绝，pull 正常完成 | ✅ |
| 无 pull 时 push 到达 | push 接受并写入 plasma → seal → 唤醒 lease | ✅ |
| push 和 pull 同时到达 | `CreateChunk` 去重 + 二次检查保护 | ✅ |
| push 发送方中途死亡 | TTL 清理 + AbortCreate 防止泄漏 | ✅ |
| push 对象到达但无 lease 在等 | `HandleObjectLocal` 返回空列表，无害 | ✅ |

---

## 9. 关键代码索引

| 组件 | 文件 |
|------|------|
| NormalTaskSubmitter | `src/ray/core_worker/task_submission/normal_task_submitter.cc` / `.h` |
| ClusterLeaseManager | `src/ray/raylet/scheduling/cluster_lease_manager.cc` / `.h` |
| LocalLeaseManager | `src/ray/raylet/scheduling/local_lease_manager.cc` / `.h` |
| LeaseDependencyManager | `src/ray/raylet/lease_dependency_manager.cc` / `.h` |
| NodeManager (HandleRequestWorkerLease) | `src/ray/raylet/node_manager.cc` |
| NodeManager (HandleObjectLocal) | `src/ray/raylet/node_manager.cc:2418` |
| ObjectManager (Push/Pull/Receive) | `src/ray/object_manager/object_manager.cc` / `.h` |
| Plasma add_object_callback | `src/ray/raylet/main.cc:800` |
| ObjectSource 枚举 | `src/ray/object_manager/plasma/plasma.fbs` |

### 关键方法快速定位

| 方法 | 文件 | 大致行号 |
|------|------|----------|
| `NormalTaskSubmitter::SubmitTask` | `normal_task_submitter.cc` | ~30 |
| `NormalTaskSubmitter::OnWorkerIdle` | `normal_task_submitter.cc` | ~142 |
| `NormalTaskSubmitter::RequestNewWorkerIfNeeded` | `normal_task_submitter.cc` | ~250 |
| `NormalTaskSubmitter::PushNormalTask` | `normal_task_submitter.cc` | ~518 |
| `NormalTaskSubmitter::CancelWorkerLeaseIfNeeded` | `normal_task_submitter.cc` | ~190 |
| `NodeManager::HandleRequestWorkerLease` | `node_manager.cc` | ~1783 |
| `NodeManager::HandleObjectLocal` | `node_manager.cc` | ~2418 |
| `ClusterLeaseManager::ScheduleOnNode` | `cluster_lease_manager.cc` | ~282 |
| `ClusterLeaseManager::ScheduleAndGrantLeases` | `cluster_lease_manager.cc` | ~100 |
| `LocalLeaseManager::QueueAndScheduleLease` | `local_lease_manager.cc` | ~75 |
| `LocalLeaseManager::WaitForLeaseArgsRequests` | `local_lease_manager.cc` | ~94 |
| `LocalLeaseManager::ScheduleAndGrantLeases` | `local_lease_manager.cc` | ~127 |
| `LocalLeaseManager::GrantScheduledLeasesToWorkers` | `local_lease_manager.cc` | ~133 |
| `LocalLeaseManager::PinLeaseArgsIfMemoryAvailable` | `local_lease_manager.cc` | ~781 |
| `LocalLeaseManager::LeasesUnblocked` | `local_lease_manager.cc` | ~733 |
| `LocalLeaseManager::PoppedWorkerHandler` | `local_lease_manager.cc` | ~540 |
| `LocalLeaseManager::SpillWaitingLeases` | `local_lease_manager.cc` | ~440 |
| `LocalLeaseManager::Spillback` | `local_lease_manager.cc` | ~488 |
| `LeaseDependencyManager::RequestLeaseDependencies` | `lease_dependency_manager.cc` | ~212 |
| `LeaseDependencyManager::HandleObjectLocal` | `lease_dependency_manager.cc` | ~307 |
| `ObjectManager::PushForReplication` | `object_manager.cc` | ~397 |
| `ObjectManager::ReceiveReplicationPushChunk` | `object_manager.cc` | ~696 |
| `ObjectManager::ReceivePullChunk` | `object_manager.cc` | ~640 |
| `ObjectManager::ConsumeReplicationPushReceived` | `object_manager.cc` | ~345 |
| `ObjectManager::CleanupExpiredReplicationPushState` | `object_manager.cc` | ~1001 |
