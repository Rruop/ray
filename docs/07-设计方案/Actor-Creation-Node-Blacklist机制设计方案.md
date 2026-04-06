# Actor Node Blacklist 机制 — 完整方案

## 问题背景

当 Ray actor `__init__` 抛出异常时：
- Worker 以 `WorkerExitType::USER_ERROR` 退出
- GCS `OnWorkerDead` 将 `need_reconstruct=false`，actor 永久 DEAD
- `max_restarts=-1` 对 init 异常无效（init 异常被视为 USER_ERROR，不会重建）
- `_init_udf_with_retries` 是同进程内 while 循环，无法换节点
- 坏节点（如 CUDA 硬件故障）场景下，所有重试在同一节点上反复失败

本方案在 Ray Core GCS 层引入 **per-actor 节点黑名单机制**，使 init 异常能触发跨节点重试。

---

## 核心设计决策

| 决策项 | 选择 | 理由 |
|--------|------|------|
| 是否新增 WorkerExitType | **否**，在 OnWorkerDead 中按条件修改 need_reconstruct | 避免修改 protobuf 枚举和全链路 RPC |
| 黑名单粒度 | **per-actor** | 精确隔离，一个 actor 失败不影响其他 actor |
| 黑名单存储 | GcsActor 的 ActorTableData proto（持久化） | 兼顾持久性和性能，GCS 重启后不丢失 |
| 命名抽象 | `actor_node_blacklist_*`（不含 `creation`） | 黑名单是通用调度机制，不仅限于 creation task，未来可复用于 runtime env 失败等场景 |
| 加入黑名单的依据 | **由异常原因决定**（与节点环境相关的异常才加） | 黑名单的核心语义是"该节点环境有问题"，纯用户代码错误不应加入 |
| 是否重启的依据 | **由重启策略决定**（全局开关 + per-actor override + max_restarts） | 重启是策略决策，与黑名单无关 |
| 两者的关系 | **加入黑名单是重启的必要前提** | 不加黑名单 = 异常与节点无关 = 换节点也会失败 = 不应重启 |
| creation task 重启是否计入 max_restarts | **否**（默认） | 与 node_preemption 模式一致，防止坏节点耗尽 restarts |
| 黑名单 TTL | 可配置，默认 60s | 避免永久排除，坏节点修复后可重新调度 |
| 黑名单容量上限 | 可配置，默认 10 个节点 | 防止大集群无限扩展 |
| 成功后是否清黑名单 | **否**，保留黑名单由 TTL 过期自动清理 | 保留黑名单在 actor 重启时有价值：避免调度回已知坏节点。详见下方"黑名单生命周期"章节 |
| 黑名单继承（actor class 级别） | **Phase 2**：新创建同 class actor 从已有同 class actor 继承黑名单 | 同 class 的 actor 行为一致，节点 A 的 CUDA Error 让同 class 所有 actor 在 A 上都会失败，共享黑名单避免每个 actor 都"亲自失败一次" |
| GcsNodeManager 全局 denylist | **删除**（Phase 1 不需要） | 全局 denylist 语义不正确（某节点上任意 actor 失败就排除），与 per-actor 黑名单冗余。actor class 级别共享通过继承机制实现，而非全局 map |

---

## 黑名单生命周期

### 核心原则：成功不清除，TTL 自动过期

**不应在 `OnActorCreationSuccess` 中清除黑名单。** 原因：

```
场景: 节点 A CUDA 故障 → actor_1 init 失败 → 加黑名单[A]
      → 节点 B 成功 → 如果此时清除黑名单[A]
      → 节点 B 宕机 → actor_1 重启
      → 黑名单[A] 已清 → 调度回节点 A → CUDA Error 再发生
      → 又要加黑名单 + 递增 num_restarts → 浪费
```

正确语义：黑名单记录的是"该节点环境有问题"的事实，**不是当前 actor 的尝试历史**。节点环境问题在 TTL 期间持续存在，不应因 actor 暂时成功就遗忘。

**黑名单的三种清理时机：**

| 时机 | 操作 | 语义 |
|------|------|------|
| TTL 过期 | `PruneNodeBlacklist` 在每次 `Schedule` 时调用 | 节点可能已修复，自动允许重新调度 |
| Actor DEAD | 黑名单随 `ActorTableData` 一起不再使用 | actor 永久死亡，黑名单不再有价值 |
| Actor 重启调度 | `PruneNodeBlacklist` 在 `RestartActor` 中调用 | 修剪过期条目后再调度 |

**`OnActorCreationSuccess` 中的处理：** 仅调用 `PruneNodeBlacklist`（修剪过期条目），不调用 `ClearNodeBlacklist`（清除全部条目）。

```diff
  // OnActorCreationSuccess — 修剪过期条目，保留有效黑名单
-  actor->ClearNodeBlacklist();
+  actor->PruneNodeBlacklist(
+      RayConfig::instance().actor_node_blacklist_ttl_ms());
```

### 对调度的影响

黑名单条目在 actor ALIVE 状态下持续存在，但此时 actor 已绑定节点，不会触发新的调度。只有在以下场景才会用到：

1. **actor 重启**（节点宕机 → RestartActor → Schedule → 使用黑名单排除已知坏节点）
2. **actor class 继承**（新同 class actor 从已有 actor 的黑名单继承 — Phase 2）

---

## Actor Class 级别黑名单继承（Phase 2 设计）

### 问题

同一 class 的 actor init 行为一致。节点 A 上的 CUDA Error 意味着：
- 同 class 的所有 actor 在节点 A 上都会 init 失败
- per-actor 黑名单需要每个 actor 都"亲自失败一次"才知道避开节点 A
- 对于 HashShuffleAggregator 等大量同 class actor 的场景，这很浪费

### 方案选择

#### 方案 A：遍历 registered_actors_ 查找同 class — ❌ 不采用

每次 `RegisterActor` 时遍历 `registered_actors_` 找同 class_name 的 actor，然后复制其 `node_blacklist`。

```cpp
// O(n) 遍历，不可接受
for (const auto &[id, existing_actor] : registered_actors_) {
  if (existing_actor->GetActorTableData().class_name() == class_name) {
    ...
  }
}
```

**问题：**
- O(n) 遍历所有已注册 actor，大集群（10k+ actor）性能差
- 每次 RegisterActor 都要遍历，而黑名单继承只在 `node_blacklist` 非空时有价值
- `registered_actors_` 中大部分 actor 没有 `node_blacklist`（从未在坏节点上失败），遍历无效

#### 方案 B：GcsActorManager 维护 class_name → representative actor 索引 — ✅ 采用

在 `GcsActorManager` 中新增一个轻量索引 `class_blacklist_rep_`，按 `class_name` 维护一个"有黑名单且 ALIVE"的代表性 actor。新增黑名单条目时自动更新索引，继承时 O(1) 查找。

**数据结构：**

```cpp
// GcsActorManager 新增
// class_name → 有 node_blacklist 的 representative actor (shared_ptr)
// 只保留一个代表即可（同 class 的黑名单内容一致）
absl::flat_hash_map<std::string, std::weak_ptr<GcsActor>> class_blacklist_rep_;
```

**使用 `weak_ptr` 的原因：** representative actor 可能 DEAD 被移除，`weak_ptr` 自动失效，不阻止 actor 销毁，也不需要手动清理。

**索引更新时机：**

| 时机 | 操作 | 说明 |
|------|------|------|
| `AddNodeBlacklist` | `class_blacklist_rep_[class_name] = actor` | 有黑名单的 actor 自动成为 representative |
| `RegisterActor`（继承） | 从 `class_blacklist_rep_[class_name]` 获取 representative，O(1) | 仅在 representative 存在且 `weak_ptr` 有效时继承 |
| Actor DEAD | 无需操作（`weak_ptr` 自动失效） | 不增加清理负担 |

**继承逻辑：**

```cpp
void GcsActorManager::InheritNodeBlacklist(std::shared_ptr<GcsActor> new_actor) {
  if (!RayConfig::instance().actor_node_blacklist_inherit_enabled()) {
    return;
  }
  const auto &class_name = new_actor->GetActorTableData().class_name();
  auto it = class_blacklist_rep_.find(class_name);
  if (it == class_blacklist_rep_.end()) {
    return;
  }
  auto rep = it->second.lock();
  if (!rep) {
    // representative 已销毁，清理索引
    class_blacklist_rep_.erase(it);
    return;
  }
  // 从 representative 的 node_blacklist 复制条目到新 actor
  for (const auto &entry : rep->GetActorTableData().node_blacklist()) {
    new_actor->AddNodeBlacklist(NodeID::FromBinary(entry.node_id()), entry.reason());
  }
}
```

**索引更新（在 OnWorkerDead 中 `AddNodeBlacklist` 后）：**

```cpp
// OnWorkerDead 中，AddNodeBlacklist 后更新索引
actor_iter->second->AddNodeBlacklist(...);
class_blacklist_rep_[actor_iter->second->GetActorTableData().class_name()] =
    actor_iter->second;
```

**性能分析：**

| 操作 | 复杂度 | 说明 |
|------|--------|------|
| AddNodeBlacklist + 索引更新 | O(1) | hashmap insert |
| RegisterActor + 继承 | O(k)，k=rep 的黑名单条目数 | 最多 `actor_node_blacklist_max_size`(默认10) 条 |
| Actor DEAD | O(0) | weak_ptr 自动失效，无需遍历清理 |
| 索引空间 | O(m)，m=有黑名单的 class 数量 | 远小于 actor 总数 |

#### 方案 C：独立全局 class→blacklist map — ❌ 不采用

| 缺点 | 说明 |
|------|------|
| 清理时机难定 | class 的所有 actor 都 ALIVE/DEAD 时才能清？ |
| 语义过于激进 | 节点 A 上一个 class 的一个 actor 失败就排除该 class 所有 actor |
| 双写问题 | AddNodeBlacklist 要同时写 per-actor 和全局 map |
| 资源配置差异 | 同 class 不同 actor 可能有不同 GPU 数量，黑名单应独立控制 |

### 最终方案：方案 B（class_blacklist_rep_ 索引）

- **存储**：黑名单仍 per-actor（ActorTableData.node_blacklist），索引只是查找加速
- **继承**：复制条目（不是引用），新 actor 有独立黑名单副本，后续独立增删
- **一致性**：同 class 的 ALIVE actor 和新 actor 在 TTL 内黑名单一致，TTL 过期后自然收敛
- **零额外清理**：`weak_ptr` 自动失效，Actor DEAD 时无需遍历索引

---

## 关键设计：三个独立关注点

### 关注点 1：是否加入黑名单 — 由异常原因决定

黑名单的核心语义是 **"该节点环境有问题，换节点可能解决"**。

```cpp
bool ShouldAddToNodeBlacklist(const rpc::RayException &exception) const;
```

| 异常类型 | ShouldAddToNodeBlacklist | 原因 |
|----------|--------------------------|------|
| CUDA Error / GPU 故障 | true | 与节点硬件相关，换节点可能解决 |
| Runtime env 安装失败 | true | 与节点环境相关 |
| TypeError / ValueError | false | 纯用户代码错误，换节点也会失败 |
| AttributeError | false | 纯用户代码错误 |

### 关注点 2：是否重启 — 由重启策略决定

```cpp
bool ShouldRestartOnNodeFailure() const;
```

> **命名说明：** 方法名为 `ShouldRestartOnNodeFailure` 而非 `ShouldRestartOnCreationTaskFailure`。
> "creation task" 描述的是触发场景（何时调用），而非策略语义（为何重启）。
> 该方法回答的核心问题是："当节点环境异常导致 actor 无法存活时，是否应该重启？"
> 这个策略在未来适用于 runtime env setup 失败等非 creation task 场景，因此命名不应限定为 creation task。

判断维度：
1. 全局开关 `actor_node_blacklist_enabled`
2. per-actor override `enable_node_blacklist`（proto field）
3. `max_restarts` 余量检查（Phase 2 可细化）

### 关注点 3：加入黑名单 — 数据操作

```cpp
void AddNodeBlacklist(const NodeID &node_id, const std::string &reason);
```

### 三者组合逻辑

```
ShouldAddToNodeBlacklist(exception)?
  ├─ Yes → AddNodeBlacklist(node_id, reason)    ← 先加黑名单（数据操作）
  │        ShouldRestartOnNodeFailure()?         ← 再判断是否重启（策略）
  │        ├─ Yes → need_reconstruct = true, RestartActor
  │        └─ No  → actor DEAD, 但黑名单已记录（observability）
  └─ No  → 不加黑名单, need_reconstruct = false（原有行为）
           异常与节点无关，换节点也会失败，不值得重启
```

**四种组合场景：**

| ShouldAddToNodeBlacklist | ShouldRestart | 场景 | 结果 |
|--------------------------|--------------|------|------|
| Yes | Yes | CUDA Error，max_restarts 有余量 | 加黑名单 + 重启到其他节点 |
| Yes | No | CUDA Error，max_restarts 已耗尽 | 加黑名单 + actor DEAD（黑名单供 observability） |
| No | — | TypeError（与节点无关） | 不加黑名单 + actor DEAD |
| No | — | 异常 denylist 中（Phase 2） | 不加黑名单 + actor DEAD |

> Phase 1 无法区分异常类型，`ShouldAddToNodeBlacklist` 和 `ShouldRestartOnNodeFailure` 逻辑效果一致（开关打开就全部加/全部重启）。但代码结构已解耦，Phase 2 实现异常过滤时只需修改 `ShouldAddToNodeBlacklist`，不影响重启策略逻辑。

---

## 完整数据流

```
Actor __init__ 抛异常（如 CUDA Error）
  │
  ▼
CoreWorker Exit(USER_ERROR, creation_task_exception)
  │ (不变)
  ▼
Raylet → GCS OnWorkerDead(node_id, worker_id, USER_ERROR, creation_task_exception)
  │
  ▼ (新增逻辑 — 三个解耦步骤)
Step 1: 黑名单决策（由异常原因决定）
  ShouldAddToNodeBlacklist(creation_task_exception)?
   ├─ Phase 1: 全部 true（开关打开时无法区分异常类型）
   ├─ Phase 2: 检查 exception_type 是否属于环境相关类型
   ├─ Yes → Step 2
   └─ No  → need_reconstruct = false, actor DEAD (原有行为)

Step 2: 数据操作
  AddNodeBlacklist(node_id, exception_string)
  → 记录失败节点 + 时间戳 + 异常信息

Step 3: 重启策略（由策略配置决定）
  ShouldRestartOnNodeFailure()?
   ├─ 全局开关 enabled + per-actor override + max_restarts 余量
   ├─ Yes → need_reconstruct = true, RestartActor
   └─ No  → actor DEAD, 但黑名单已保留（observability）
  │
  ▼ (need_reconstruct = true 时)
RestartActor(actor_id, need_reschedule=true, death_cause 含 creation_task_failure_context)
  │
  ▼ (修改)
1. effective_restarts = num_restarts - node_preemption - creation_task_failure
2. 递增 num_restarts_due_to_creation_task_failure (始终递增)
3. PruneNodeBlacklist(ttl_ms) — 修剪过期条目
  │
  ▼
gcs_actor_scheduler_->Schedule(actor)
  → 委托 Schedule(actor, actor->GetNodeBlacklist())
  │
  ▼
SelectForwardingNode(actor, excluded_nodes={node_A})
  → 有资源需求: 优先 owner 节点(若不在黑名单), 否则 SelectRandomAliveNodeExcluding
  → 无资源需求: SelectRandomAliveNodeExcluding
  → 全部被排除 → fallback 任意节点 + WARNING
  │
  ▼
在新节点上创建 actor → __init__ 成功
  │
  ▼ (新增)
OnActorCreationSuccess → PruneNodeBlacklist(ttl_ms)  ← 仅修剪过期条目，不清除有效黑名单
```

---

## OnWorkerDead 代码结构

```cpp
bool need_reconstruct = disconnect_type != rpc::WorkerExitType::INTENDED_USER_EXIT &&
                        disconnect_type != rpc::WorkerExitType::USER_ERROR;

// ... 确定 actor_id, 获取 actor_iter ...

// Step 1 + Step 2: 黑名单决策 + 数据操作（由异常原因决定）
if (disconnect_type == rpc::WorkerExitType::USER_ERROR &&
    creation_task_exception != nullptr && actor_iter != registered_actors_.end() &&
    actor_iter->second->ShouldAddToNodeBlacklist(*creation_task_exception)) {
  actor_iter->second->AddNodeBlacklist(
      node_id, creation_task_exception->formatted_exception_string());

  // Step 3: 重启策略（由策略配置决定）
  if (actor_iter->second->ShouldRestartOnNodeFailure()) {
    need_reconstruct = true;
  }
}
```

---

## GcsActor 方法设计

```cpp
class GcsActor {
  // 黑名单决策 — 由异常原因决定，回答"该异常是否与节点环境相关"
  bool ShouldAddToNodeBlacklist(const rpc::RayException &exception) const;

  // 重启策略 — 由策略配置决定，回答"是否应重启此 actor"
  bool ShouldRestartOnNodeFailure() const;

  // 数据操作 — 无条件执行，记录黑名单
  void AddNodeBlacklist(const NodeID &node_id, const std::string &reason);
  void PruneNodeBlacklist(uint64_t ttl_ms);
  absl::flat_hash_set<NodeID> GetNodeBlacklist() const;
  void ClearNodeBlacklist();
};
```

### ShouldAddToNodeBlacklist 实现

```cpp
bool GcsActor::ShouldAddToNodeBlacklist(const rpc::RayException &exception) const {
  if (!RayConfig::instance().actor_node_blacklist_enabled()) {
    return false;
  }

  // Per-actor override
  if (task_spec_ &&
      task_spec_->actor_creation_task_spec().has_enable_node_blacklist()) {
    if (!task_spec_->actor_creation_task_spec().enable_node_blacklist()) {
      return false;
    }
  }

  // Phase 2: 检查异常是否在 denylist 中（与环境无关的异常不加黑名单）
  // if (task_spec_) {
  //   const auto &denylist =
  //       task_spec_->actor_creation_task_spec().restart_exception_denylist();
  //   if (!denylist.empty() && IsExceptionInDenylist(exception, denylist)) {
  //     return false;
  //   }
  // }

  // Phase 1: 开关打开且无 override 禁止 → 全部加入
  return true;
}
```

### ShouldRestartOnNodeFailure 实现

```cpp
bool GcsActor::ShouldRestartOnNodeFailure() const {
  // 重启策略与黑名单无关，独立判断
  // 当前简化实现：只要黑名单功能启用就重启
  // 未来可加入 max_restarts 余量检查等更细粒度策略
  return RayConfig::instance().actor_node_blacklist_enabled();
}
```

> **Phase 1 简化说明：** 当前 `ShouldRestartOnNodeFailure()` 的判断与全局开关一致（开关打开就重启）。这是因为 `ShouldAddToNodeBlacklist` 已经过滤掉了"不应加黑名单"的情况（异常与节点无关），剩下的都是"环境相关异常"，值得重启。Phase 2 加入 denylist 后，`ShouldAddToNodeBlacklist` 会排除纯用户代码异常，`ShouldRestartOnNodeFailure` 可进一步加入 max_restarts 余量等策略。

---

## 命名重映射

### 总原则

- **去掉 `creation`/`creation_task` 限定**：黑名单、开关命名通用化，未来可复用于 runtime env 失败等场景
- **保留 `creation_task` 的场合**：仅当语义确实仅针对 creation task 时保留（如 `num_restarts_due_to_creation_task_failure`、`creation_task_failure_context`、`creation_task_failure_restarts_count_toward_max_restarts`）

### 完整映射表

| 旧命名 | 新命名 | 说明 |
|--------|--------|------|
| `ActorCreationTaskBlacklistEntry` (proto message) | `ActorNodeBlacklistEntry` | 去掉 `CreationTask`，通用化 |
| `creation_task_failed_nodes` (proto field 37) | `node_blacklist` | 通用字段名 |
| `ShouldRetryCreationTaskOnDifferentNode` | `ShouldAddToNodeBlacklist` + `ShouldRestartOnNodeFailure` | 拆分为两个方法，各自独立决策 |
| `AddCreationTaskFailedNode` | `AddNodeBlacklist` | 纯数据操作 |
| `PruneCreationTaskBlacklist` | `PruneNodeBlacklist` | 通用化 |
| `GetCreationTaskBlacklistNodeIDs` | `GetNodeBlacklist` | 通用化 |
| `ClearCreationTaskBlacklist` | `ClearNodeBlacklist` | 仅在 actor 永久 DEAD 时使用，不在 OnActorCreationSuccess 中使用 |
| `actor_creation_node_blacklist_enabled` | `actor_node_blacklist_enabled` | 去掉 `creation` |
| `actor_creation_node_blacklist_max_size` | `actor_node_blacklist_max_size` | 去掉 `creation` |
| `actor_creation_node_blacklist_ttl_ms` | `actor_node_blacklist_ttl_ms` | 去掉 `creation` |
| `enable_creation_task_node_blacklist` (common.proto field 15) | `enable_node_blacklist` | 去掉 `creation_task` |
| `creation_task_retry_exception_denylist` (common.proto field 16) | `restart_exception_denylist` | 语义更清晰，去掉 `creation_task` |
| `actor_creation_failure_restarts_count_toward_max_restarts` | `creation_task_failure_restarts_count_toward_max_restarts` | 保留 `creation_task`（此项确实仅针对 creation task 重启计数策略） |
| `num_restarts_due_to_creation_task_failure` (proto field 38) | **不变** | 语义确实仅针对 creation task |
| `creation_task_failure_context` (death_cause) | **不变** | Ray 已有 proto，不修改 |
| `GcsNodeManager::actor_creation_denylist_` | **删除** | 与 per-actor 黑名单冗余，语义不正确 |

---

## 需要修改的文件与精确变更

### 1. `src/ray/protobuf/gcs.proto`

```diff
-  // Per-actor creation task failure node blacklist.
-  // When actor creation fails on a node, the node is added here so that
-  // subsequent scheduling attempts avoid that node.
-  repeated ActorCreationTaskBlacklistEntry creation_task_failed_nodes = 37;
+  // Per-actor node blacklist for scheduling exclusion.
+  // When actor creation fails on a node due to environment-related errors,
+  // the node is added here so that subsequent scheduling attempts avoid that node.
+  repeated ActorNodeBlacklistEntry node_blacklist = 37;
   // Number of times this actor is restarted due to creation task failure.
   // Does NOT count toward max_restarts by default (similar to
   // num_restarts_due_to_node_preemption).
   uint64 num_restarts_due_to_creation_task_failure = 38;
 }

- message ActorCreationTaskBlacklistEntry {
-   // The node ID where the creation task failed.
+ message ActorNodeBlacklistEntry {
+   // The node ID to exclude from scheduling.
    bytes node_id = 1;
-   // Unix millis timestamp when this entry was added.
+   // Unix millis timestamp when this entry was added to the blacklist.
    uint64 timestamp_ms = 2;
-   // The formatted exception string that caused the creation task failure.
-   string exception_string = 3;
+   // The reason (typically exception string) why this node was blacklisted.
+   string reason = 3;
 }
```

### 2. `src/ray/protobuf/common.proto`

```diff
-  // Whether to enable cross-node retry for creation task failures.
-  // Overrides global RayConfig when set.
-  optional bool enable_creation_task_node_blacklist = 15;
-  // Exception type names that should NOT trigger cross-node retry (denylist).
-  // Empty means all exceptions trigger retry when enabled.
-  repeated string creation_task_retry_exception_denylist = 16;
+  // Whether to enable node blacklist for this actor's scheduling.
+  // Overrides global RayConfig when set.
+  optional bool enable_node_blacklist = 15;
+  // Exception type names that should NOT trigger node blacklisting (denylist).
+  // Empty means all exceptions trigger blacklisting when enabled.
+  repeated string restart_exception_denylist = 16;
```

### 3. `src/ray/common/ray_config_def.h`

```diff
-/// When enabled, actors whose __init__ throws an exception will be
-/// restarted on a different node instead of being permanently killed.
-RAY_CONFIG(bool, actor_creation_node_blacklist_enabled, false)
-/// Maximum number of nodes to blacklist per actor for creation task failures.
-RAY_CONFIG(uint32_t, actor_creation_node_blacklist_max_size, 10)
-/// TTL in milliseconds for entries in the actor creation node blacklist.
-/// 0 means no TTL (entries persist until actor is ALIVE or DEAD).
-RAY_CONFIG(uint64_t, actor_creation_node_blacklist_ttl_ms, 60000)
-/// Whether creation task failure restarts count toward max_restarts.
-/// By default false (similar to node preemption restarts).
-RAY_CONFIG(bool, actor_creation_failure_restarts_count_toward_max_restarts, false)
+/// When enabled, actors whose __init__ throws an environment-related exception
+/// will be blacklisted on the failed node and restarted on a different node.
+RAY_CONFIG(bool, actor_node_blacklist_enabled, false)
+/// Maximum number of nodes to blacklist per actor.
+RAY_CONFIG(uint32_t, actor_node_blacklist_max_size, 10)
+/// TTL in milliseconds for entries in the actor node blacklist.
+/// 0 means no TTL (entries persist until actor is ALIVE or DEAD).
+RAY_CONFIG(uint64_t, actor_node_blacklist_ttl_ms, 60000)
+/// Whether creation task failure restarts count toward max_restarts.
+/// By default false (similar to node preemption restarts).
+RAY_CONFIG(bool, creation_task_failure_restarts_count_toward_max_restarts, false)
```

### 4. `src/ray/gcs/actor/gcs_actor.h`

```diff
-  /// Add a node to this actor's creation task blacklist.
-  void AddCreationTaskFailedNode(const NodeID &node_id,
-                                  const std::string &exception_string);
-  /// Remove expired entries from the blacklist based on TTL.
-  void PruneCreationTaskBlacklist(uint64_t ttl_ms);
-  /// Get the set of blacklisted node IDs for creation task scheduling.
-  absl::flat_hash_set<NodeID> GetCreationTaskBlacklistNodeIDs() const;
-  /// Clear the entire creation task blacklist.
-  void ClearCreationTaskBlacklist();
-  /// Check if a creation task exception should trigger cross-node retry.
-  bool ShouldRetryCreationTaskOnDifferentNode(
-      const rpc::RayException &creation_task_exception) const;
+  /// Check if an exception should cause the current node to be blacklisted.
+  /// Decision is based on the exception reason (environment-related or not).
+  bool ShouldAddToNodeBlacklist(const rpc::RayException &exception) const;
+  /// Check if this actor should be restarted after a node failure.
+  /// Decision is based on restart policy (global config + per-actor override + max_restarts).
+  bool ShouldRestartOnNodeFailure() const;
+  /// Add a node to this actor's blacklist.
+  void AddNodeBlacklist(const NodeID &node_id, const std::string &reason);
+  /// Remove expired entries from the blacklist based on TTL.
+  /// Called in RestartActor and OnActorCreationSuccess.
+  void PruneNodeBlacklist(uint64_t ttl_ms);
+  /// Get the set of blacklisted node IDs for scheduling exclusion.
+  absl::flat_hash_set<NodeID> GetNodeBlacklist() const;
+  /// Clear the entire node blacklist.
+  /// Only used when actor is permanently DEAD (no longer scheduling).
+  void ClearNodeBlacklist();
```

### 5. `src/ray/gcs/actor/gcs_actor.cc`

**完整替换当前 5 个方法为新的 5 个方法：**

```cpp
bool GcsActor::ShouldAddToNodeBlacklist(const rpc::RayException &exception) const {
  if (!RayConfig::instance().actor_node_blacklist_enabled()) {
    return false;
  }

  if (task_spec_ &&
      task_spec_->actor_creation_task_spec().has_enable_node_blacklist()) {
    if (!task_spec_->actor_creation_task_spec().enable_node_blacklist()) {
      return false;
    }
  }

  return true;
}

bool GcsActor::ShouldRestartOnNodeFailure() const {
  return RayConfig::instance().actor_node_blacklist_enabled();
}

void GcsActor::AddNodeBlacklist(const NodeID &node_id, const std::string &reason) {
  auto *entry = actor_table_data_.add_node_blacklist();
  entry->set_node_id(node_id.Binary());
  entry->set_timestamp_ms(absl::GetCurrentTimeNanos() / 1000000);
  entry->set_reason(reason);

  uint32_t max_size = RayConfig::instance().actor_node_blacklist_max_size();
  while (actor_table_data_.node_blacklist_size() >
         static_cast<int32_t>(max_size)) {
    actor_table_data_.mutable_node_blacklist()->DeleteSubrange(0, 1);
  }
}

void GcsActor::PruneNodeBlacklist(uint64_t ttl_ms) {
  if (ttl_ms == 0) return;
  uint64_t now = absl::GetCurrentTimeNanos() / 1000000;
  int entries_to_remove = 0;
  for (int i = 0; i < actor_table_data_.node_blacklist_size(); i++) {
    auto &entry = actor_table_data_.node_blacklist(i);
    if (now - entry.timestamp_ms() >= ttl_ms) {
      entries_to_remove = i + 1;
    } else {
      break;
    }
  }
  if (entries_to_remove > 0) {
    actor_table_data_.mutable_node_blacklist()->DeleteSubrange(
        0, entries_to_remove);
  }
}

absl::flat_hash_set<NodeID> GcsActor::GetNodeBlacklist() const {
  absl::flat_hash_set<NodeID> result;
  for (const auto &entry : actor_table_data_.node_blacklist()) {
    result.insert(NodeID::FromBinary(entry.node_id()));
  }
  return result;
}

void GcsActor::ClearNodeBlacklist() {
  actor_table_data_.clear_node_blacklist();
}
```

### 6. `src/ray/gcs/actor/gcs_actor_scheduler.cc`

```diff
-  Schedule(std::move(actor), actor->GetCreationTaskBlacklistNodeIDs());
+  Schedule(std::move(actor), actor->GetNodeBlacklist());

-  return SelectForwardingNode(std::move(actor), actor->GetCreationTaskBlacklistNodeIDs());
+  return SelectForwardingNode(std::move(actor), actor->GetNodeBlacklist());
```

### 7. `src/ray/gcs/actor/gcs_actor_manager.cc`

#### OnWorkerDead (行 ~1273)

```diff
-  if (disconnect_type == rpc::WorkerExitType::USER_ERROR &&
-      creation_task_exception != nullptr && actor_iter != registered_actors_.end() &&
-      actor_iter->second->ShouldRetryCreationTaskOnDifferentNode(
-          *creation_task_exception)) {
-    need_reconstruct = true;
-    actor_iter->second->AddCreationTaskFailedNode(
-        node_id, creation_task_exception->formatted_exception_string());
-  }
+  if (disconnect_type == rpc::WorkerExitType::USER_ERROR &&
+      creation_task_exception != nullptr && actor_iter != registered_actors_.end() &&
+      actor_iter->second->ShouldAddToNodeBlacklist(*creation_task_exception)) {
+    actor_iter->second->AddNodeBlacklist(
+        node_id, creation_task_exception->formatted_exception_string());
+    if (actor_iter->second->ShouldRestartOnNodeFailure()) {
+      need_reconstruct = true;
+    }
+  }
```

#### RestartActor (行 ~1487, ~1522, ~1557)

```diff
   uint64_t num_restarts_due_to_creation_task_failure =
       mutable_actor_table_data->num_restarts_due_to_creation_task_failure();
   // (不变 — proto field 38 名称不变)

   const auto effective_restarts =
       num_restarts - num_restarts_due_to_node_preemption -
       (RayConfig::instance()
-                   .actor_creation_failure_restarts_count_toward_max_restarts()
+                   .creation_task_failure_restarts_count_toward_max_restarts()
            ? 0
            : num_restarts_due_to_creation_task_failure);

   // (不变 — has_creation_task_failure_context 是 death_cause 中的，proto 未改)
   if (death_cause.has_creation_task_failure_context() &&
       !RayConfig::instance()
-           .actor_creation_failure_restarts_count_toward_max_restarts()) {
+           .creation_task_failure_restarts_count_toward_max_restarts()) {
     mutable_actor_table_data->set_num_restarts_due_to_creation_task_failure(
         num_restarts_due_to_creation_task_failure + 1);
   }

-  actor->PruneCreationTaskBlacklist(
-      RayConfig::instance().actor_creation_node_blacklist_ttl_ms());
+  actor->PruneNodeBlacklist(
+      RayConfig::instance().actor_node_blacklist_ttl_ms());
```

#### OnActorCreationSuccess (行 ~1676) — 仅修剪过期条目，不清除

```diff
-  actor->ClearCreationTaskBlacklist();
+  actor->PruneNodeBlacklist(
+      RayConfig::instance().actor_node_blacklist_ttl_ms());
```

> **不使用 `ClearNodeBlacklist`**：黑名单记录"该节点环境有问题"的事实，不应因 actor 暂时成功就遗忘。
> actor ALIVE 后不再触发调度，黑名单只在重启和 class 继承时有价值，由 TTL 自动过期清理。

### 8. `src/ray/gcs/actor/tests/gcs_actor_manager_test.cc`

**MockActorScheduler 新增 `Schedule(actor, excluded_nodes)` 重载实现（P0 编译阻塞修复）：**

```diff
 class MockActorScheduler : public gcs::GcsActorSchedulerInterface {
  public:
   MockActorScheduler() {}

   void Schedule(std::shared_ptr<gcs::GcsActor> actor) { actors.push_back(actor); }
+  void Schedule(std::shared_ptr<gcs::GcsActor> actor,
+                const absl::flat_hash_set<NodeID> &excluded_nodes) override {
+    actors.push_back(actor);
+  }
   void Reschedule(std::shared_ptr<gcs::GcsActor> actor) {}
```

### 9. `src/ray/gcs/gcs_node_manager.h` — 删除冗余 denylist

```diff
-  // --- Actor creation node blacklist ---
-  // ... (删除 actor_creation_denylist_ 声明及相关方法)
```

### 10. `src/ray/gcs/gcs_node_manager.cc` — 删除冗余 denylist 实现

删除 `AddActorCreationDeniedNode`、`IsActorCreationDeniedNode`、`PruneActorCreationDenylist` 等方法实现。

---

## 修改文件总清单

| 文件 | 变更类型 | Phase | 状态 |
|------|---------|-------|------|
| `src/ray/protobuf/gcs.proto` | 重命名 message + field + 注释 | 1 | 需修改 |
| `src/ray/protobuf/common.proto` | 重命名 2 个 field + 注释 | 1 | 需修改 |
| `src/ray/common/ray_config_def.h` | 重命名 4 个配置项 + 注释 | 1 | 需修改 |
| `src/ray/gcs/actor/gcs_actor.h` | 重命名 5 方法 → 5 方法 + 拆分 | 1 | 需修改 |
| `src/ray/gcs/actor/gcs_actor.cc` | 重命名实现 + proto 引用 + 拆分方法 | 1 | 需修改 |
| `src/ray/gcs/gcs_node_manager.h` | 删除冗余 denylist | 1 | 需修改 |
| `src/ray/gcs/gcs_node_manager.cc` | 删除冗余 denylist 实现 | 1 | 需修改 |
| `src/ray/gcs/actor/gcs_actor_scheduler.h` | Schedule/SelectForwardingNode 重载 | 1 | ✅ 已完成 |
| `src/ray/gcs/actor/gcs_actor_scheduler.cc` | 重命名方法调用 | 1 | 需修改 |
| `src/ray/gcs/actor/gcs_actor_manager.cc` | 重命名 + 三步解耦逻辑 + 配置项引用 | 1 | 需修改 |
| `src/ray/gcs/actor/tests/gcs_actor_manager_test.cc` | MockActorScheduler 修复 | 1 | ❌ 需新增 |
| `src/ray/core_worker/common.h` | ActorCreationOptions 扩展 | 2 | ❌ |
| `python/ray/_raylet.pyx` | create_actor 新参数 | 2 | ❌ |
| `python/ray/actor.py` | actor option | 2 | ❌ |
| `python/ray/data/.../hash_shuffle_aggregator.py` | 传递 enable_node_blacklist | 2 | ❌ |

---

## 待完成工作

### Phase 1: C++ 核心基础设施（编译通过）

| # | 工作项 | 状态 | 优先级 |
|---|--------|------|--------|
| 1 | Proto 重命名: gcs.proto + common.proto | ❌ | **P0** |
| 2 | RayConfig 重命名: ray_config_def.h | ❌ | **P0** |
| 3 | GcsActor 重命名 + 拆分: gcs_actor.h + gcs_actor.cc | ❌ | **P0** |
| 4 | GcsNodeManager 删除冗余 denylist: gcs_node_manager.h + gcs_node_manager.cc | ❌ | **P0** |
| 5 | GcsActorScheduler 方法调用重命名: gcs_actor_scheduler.cc | ❌ | **P0** |
| 6 | GcsActorManager 三步解耦 + 重命名: gcs_actor_manager.cc | ❌ | **P0** |
| 7 | MockActorScheduler 修复: gcs_actor_manager_test.cc | ❌ | **P0** |
| 8 | 编译验证 | ❌ | **P0** |
| 9 | C++ 单元测试 | ❌ | P1 |

### Phase 2: Python 集成 + Class 级别继承

| # | 工作项 | 状态 |
|---|--------|------|
| 10 | ActorCreationOptions 新增 `enable_node_blacklist` | ❌ |
| 11 | `_raylet.pyx` create_actor 新参数 | ❌ |
| 12 | `actor.py` option 新增 | ❌ |
| 13 | HashShuffleAggregator 适配 | ❌ |
| 14 | Actor class 黑名单继承机制 + `actor_node_blacklist_inherit_enabled` 配置 | ❌ |

### Phase 3: 异常过滤（后续迭代）

| # | 工作项 | 状态 |
|---|--------|------|
| 15 | RayException proto 新增 exception_type | ❌ |
| 16 | ShouldAddToNodeBlacklist 实现 denylist 匹配 | ❌ |
| 17 | Python API 暴露 `restart_exception_denylist` | ❌ |

---

## 附录：关于 ShouldRestartOnNodeFailure 命名的思考

该方法命名经过以下考虑：

| 候选名 | 否决原因 |
|--------|---------|
| `ShouldRestartOnCreationTaskFailure` | `creation_task` 限定过窄，未来 runtime env 失败也应走同样重启策略 |
| `ShouldRestartActor` | 过于宽泛，丢失了"节点环境异常导致"这个触发上下文 |
| `ShouldRestartOnNodeFailure` | **采用** — 表达了"节点环境异常 → 是否重启"的策略语义，不限于 creation task |
