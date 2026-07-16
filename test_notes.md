# Ray C++ 测试笔记 — TEST / TEST_F / FRIEND_TEST 与对象复制功能测试

## 一、TEST / TEST_F / FRIEND_TEST 三者关系

### 1.1 宏展开机制

`FRIEND_TEST` 在 `gtest_prod.h` 中定义：

```cpp
#define FRIEND_TEST(test_case_name, test_name)\
  friend class test_case_name##test_name##_Test
```

`##` 是 C 预处理器 token 粘合运算符，将两个 token 拼接成一个。

**示例**：`FRIEND_TEST(TestOverrideRuntimeEnv, TestOverrideEnvVars)` 展开为：
```cpp
friend class TestOverrideRuntimeEnvTestOverrideEnvVars_Test;
```

这正是 GTest 为 `TEST(TestOverrideRuntimeEnv, TestOverrideEnvVars)` 生成的类名。

### 1.2 TEST 宏展开

```cpp
TEST(TestOverrideRuntimeEnv, TestOverrideEnvVars) { /* 测试代码 */ }
```

展开为：
```cpp
class TestOverrideRuntimeEnv_TestOverrideEnvVars_Test : public ::testing::Test {
 public:
  void TestBody() override;  // 你的测试代码在这里
};
void TestOverrideRuntimeEnv_TestOverrideEnvVars_Test::TestBody() {
  /* 测试代码 */
}
static ::testing::TestInfo* const test_info_ = \
    ::testing::RegisterTest("TestOverrideRuntimeEnv", "TestOverrideEnvVars", ...);
```

### 1.3 TEST_F 宏展开

```cpp
TEST_F(NodeManagerReplicationTest, TestMaybeReplicateObjectDedup) { /* 测试代码 */ }
```

展开为：
```cpp
class NodeManagerReplicationTest_TestMaybeReplicateObjectDedup_Test
    : public NodeManagerReplicationTest {  // ← 继承你定义的 fixture
 public:
  void TestBody() override;  // 你的测试代码在这里
};
```

### 1.4 TEST vs TEST_F 区别

| 宏 | 生成的类的父类 | 场景 |
|---|---|---|
| `TEST` | 固定继承 `::testing::Test` | 无共享状态，独立测试 |
| `TEST_F` | 继承你定义的 fixture 类 | 需要 `SetUp`/共享成员 |

两者都是预处理宏，编译前展开成**类** + 注册代码，不是方法。`{ }` 里的代码成为自动生成类的 `TestBody()` 方法体。

### 1.5 FRIEND_TEST 的授权机制

FRIEND_TEST 声明的友元类与 TEST/TEST_F 宏生成的测试类使用**相同的拼接规则**（`SuiteName##TestName##_Test`），所以 FRIEND 声明的友元类就是 TEST 宏生成的测试类。

继承关系链：
```
::testing::Test
  └── NodeManagerReplicationTest           (fixture，你自己定义的)
        └── NodeManagerReplicationTest_TestMaybeReplicateObjectDedup_Test  (GTest 生成的)
```

**关键**：测试体代码在**最底层的子类**中执行，所以：
- 它能访问 fixture 的 protected/public 成员（继承来的）
- 它**本身**是被测类的友元（通过 `FRIEND_TEST`），所以能访问被测类的 private 成员
- **但 fixture 基类的方法**不是友元，所以 fixture 中定义的辅助方法不能访问被测类的 private 成员

### 1.6 Fixture 辅助方法无法访问 private 成员的示例

```cpp
// node_manager.h
class NodeManager {
 private:
  friend class NodeManagerReplicationTest_TestMaybeReplicateObjectDedup_Test;  // FRIEND_TEST 展开
  bool is_preemptible_node_ = false;  // private
};

// 测试文件
class NodeManagerReplicationTest : public ::testing::Test {
 public:
  // ❌ 这个方法属于 fixture 类，不是友元，不能访问 private
  void SetPreemptible() { node_manager_->is_preemptible_node_ = true; }
};

TEST_F(NodeManagerReplicationTest, TestMaybeReplicateObjectDedup) {
  // ✅ 可以访问——这段代码在生成的子类中，该子类是 NodeManager 的友元
  node_manager_->is_preemptible_node_ = true;
}
```

所以 `is_preemptible_node_ = true` 必须在 TEST_F 体中直接写，不能提取到 fixture 辅助方法中。

### 1.7 与 static 的关系

`FRIEND_TEST` 与 `static` 没有直接关系。`static` 成员属于类而非实例，访问控制规则相同——如果 `static` 成员是 private 的，同样需要友元才能从外部访问。

---

## 二、Ray C++ 测试风格

### 2.1 整体风格

- **主要模式**：`TEST_F` + GMock，极少用裸 `TEST`
- **裸 `TEST`** 只用于无状态/static 方法测试（如 `NodeManagerStaticTest`）
- **GMock** 是隔离依赖的主要手段：`MockGcsClient`、`MockWorkerPool`、`MockObjectManager` 等
- **FRIEND_TEST** 是访问 private 成员的标准机制，在 Ray 代码库中大量系统性使用

### 2.2 FRIEND_TEST 使用模式（来自 local_object_manager_test.cc）

```cpp
// 在构造函数中直接写 private 字段
manager.min_spilling_size_ = min_spilling_size;
manager.max_spilling_file_size_bytes_ = max_spilling_file_size_bytes;

// 在测试辅助方法中读取 private 字段
int64_t NumBytesPendingSpill() { return manager.num_bytes_pending_spill_; }
size_t GetCurrentSpilledCount() { return manager.spilled_objects_url_.size(); }
void AssertNoLeaks() { ASSERT_EQ(manager.pinned_objects_size_, 0); ... }
```

这是 Ray 的刻意设计：生产类在 private 区域声明 `FRIEND_TEST`，测试代码直接读写 private 字段。

### 2.3 NodeManager 测试模式

- **依赖注入 + 完整对象构造**：构造真实的 `NodeManager`，所有依赖注入为 Mock
- **真实调度器/Lease 管理器**：`ClusterResourceScheduler`、`LocalLeaseManager`、`ClusterLeaseManager` 也构造真实对象
- **外部 RPC/GCS 依赖被 Mock**：`MockGcsClient`、`MockObjectManager`、`MockWorkerPool`
- **测试粒度**：集成级别（测试完整 NodeManager 路径），而非纯单元测试

### 2.4 测试命名规范

| 类别 | 规范 | 示例 |
|---|---|---|
| Test Suite | `<ClassName>Test` | `NodeManagerReplicationTest`, `CoreWorkerTest`, `ClusterResourceSchedulerTest` |
| Test Case | `Test<FeatureOrBehavior>` | `TestPin`, `TestMaybeReplicateObjectDedup`, `TestConcurrentSpillAndDelete1` |
| 特殊 Suite | `<ClassName>StaticTest` 用于 static 方法 | `NodeManagerStaticTest` |

---

## 三、Ray C++ 命名规范

### 3.1 方法命名

| 类别 | 规范 | 示例 |
|---|---|---|
| optional 查询方法 | `Get*` 返回 `std::optional<T>` | `GetObjectLocations`, `GetIsGpu`, `GetNodeAddressAndLiveness`, `GetNodePreemptible` |
| `Try*` 前缀 | 尝试执行有副作用的操作，通常返回 `bool`/`void` | `TryMarkFreedObjectInUseAgain`, `TryTransitionToDisconnecting` |
| 缓存写入方法 | `Cache*` | `CacheNodePreemptible` |
| 缓存查询方法 | `Get*`（与 optional 查询一致） | `GetNodePreemptible`（从缓存读） |
| 简单 setter | `Set*` | `SetIsGpu`, `SetProcess` |
| 复杂更新 | `Update*` | `UpdateObjectPinnedAtRaylet`, `UpdateObjectSize` |
| 条件触发方法 | `Maybe*` | `MaybeTriggerPinTransfer`, `MaybeReplicateObject` |
| private 实现方法 | `Do*`（NVI 模式） | `DoPinTransfer`, `DoRetryLeasingWorkerFromNode` |
| bool 查询方法（非 optional） | `Is*` | `IsPreemptibleNode`, `IsSpillingInProgress` |
| 选择方法 | `Select*` | `SelectStableNode`, `SelectMigrationTarget` |

**关键**：`Get*` 返回 `std::optional<T>` 时，`optional` 本身已表示"可能无值"，不需要 `Try*` 或 `*Cached` 后缀。

### 3.2 成员变量命名

| 类别 | 规范 | 示例 |
|---|---|---|
| 缓存 map | `<entity>_cache_` | `preemptible_node_cache_`, `node_cache_address_and_liveness_` |
| 缓存 mutex | `<cache_map名>_mutex_` | `preemptible_node_cache_mutex_`, `node_cache_address_and_liveness_mutex_` |
| 缓存 bool 值 | `is_<property>_` | `is_preemptible_node_`（`IsPreemptibleNode()` 的缓存值） |
| 缓存填充标志 | `<cache>_populated_` | `gcs_client_node_cache_populated_`（缓存是否已填充） |
| 去重 set | `<entity>_` | `replicated_objects_`, `pin_transfers_in_flight_` |
| 去重 set 的 mutex | `<entity>_mutex_` | `replication_mutex_`, `pin_transfer_mutex_` |

**注意**：`is_preemptible_node_` 是"是否抢占式的缓存值"，不是"缓存是否已填充"的标志。如果是后者，应用 `_populated_` 后缀（如 `gcs_client_node_cache_populated_`）。

### 3.3 局部变量命名

| 模式 | 规范 | 示例 |
|---|---|---|
| `<上下文>_<属性>` | 上下文修饰 + 属性名 | `pinned_at_preemptible`, `new_location_preemptible` |
| 回调内避免 shadow | 使用不同前缀 | 回调内用 `pinned_at_is_preemptible` 而非 `pinned_at_preemptible`（避免 `-Werror=shadow`） |
| 测试中 | 通用 `result` | `auto result = core_worker_->GetNodePreemptible(node_id);` |

**注意**：避免缩写（`new_loc` → `new_location`），保持与完整词（`pinned_at`）风格一致。

---

## 四、对象复制功能的 MaybeTriggerPinTransfer 逻辑详解

### 4.1 执行上下文

`MaybeTriggerPinTransfer` 只在 **owner worker** 上执行。入口在 `AddObjectLocationOwner`，内部也检查 `owned_by_us`：

```cpp
bool owned_by_us = false;
bool ref_exists = reference_counter_->IsPlasmaObjectPinnedOrSpilled(
    object_id, &owned_by_us, &pinned_at, &spilled);
if (!ref_exists || !owned_by_us || pinned_at.IsNil() || spilled) {
    return;  // 非owner直接跳过
}
```

只有 owner 知道对象的完整位置信息，有权限修改 `pinned_at`（通过 `UpdateObjectPinnedAtRaylet`），所以 pin transfer 只能由 owner 发起。

### 4.2 核心决策逻辑

分两个阶段：

**阶段 1：缓存命中 — 直接判断**

```
pinned_at: 对象当前 pin 所在的节点
new_location_node_id: 新上报的位置节点

GetNodePreemptible(node_id) → std::optional<bool>
  - 有缓存 → 返回 true（抢占式）/ false（稳定）
  - 无缓存 → 返回 std::nullopt
```

```cpp
auto pinned_at_preemptible = GetNodePreemptible(pinned_at);
auto new_location_preemptible = GetNodePreemptible(new_location_node_id);

if (两个都有缓存) {
    if (当前是抢占式 && 新节点是稳定) {
        // ✅ 需要迁移！查询新节点地址，执行 DoPinTransfer
        DoPinTransfer(object_id, new_location_node_id, addr);
    }
    // 其他情况 → 不需要迁移
    return;  // ← 注意：直接返回，不走阶段2
}
```

决策矩阵：

| pinned_at | new_location | 动作 |
|---|---|---|
| 抢占式 | 稳定 | **DoPinTransfer** ✅ |
| 抢占式 | 抢占式 | 跳过 |
| 稳定 | 稳定 | 跳过 |
| 稳定 | 抢占式 | 跳过（不应反向迁移） |

**阶段 2：缓存未命中 — 异步查 GCS**

当至少一个节点的 preemptible 状态未知时，需要查 GCS 获取节点标签信息：

```cpp
uncached_nodes = [];
if (pinned_at 没缓存) uncached_nodes.push_back(pinned_at);
if (new_location 没缓存) uncached_nodes.push_back(new_location_node_id);

// 构造 selectors，只查需要的节点（不全量查询）
selectors = [{node_id: uncached_nodes[0].Binary()}, ...]

gcs_client_->Nodes().AsyncGetAll(
    [回调](status, result) {
        // 1. 遍历返回的所有节点，缓存每个的 preemptible 状态
        for (node_info : result->first) {
            CacheNodePreemptible(node_id, IsNodePreemptibleFromLabels(node_info));
        }
        // 2. 现在缓存应该有了，再次查询
        auto pinned_at_is_preemptible = GetNodePreemptible(pinned_at);
        auto new_location_is_preemptible = GetNodePreemptible(new_location_node_id);
        // 3. 如果还是没有（节点已下线等），跳过
        if (!pinned_at_is_preemptible.has_value() || !new_location_is_preemptible.has_value()) return;
        // 4. 同样的决策逻辑
        if (*pinned_at_is_preemptible && !*new_location_is_preemptible) {
            // 二次 dedup 检查（防止 GCS 回调期间已有并发的 pin transfer）
            if (pin_transfers_in_flight_.contains(object_id)) return;
            DoPinTransfer(...);
        }
    },
    /*selectors=*/selectors);  // ← 只查 uncached 的节点
```

### 4.3 关键设计点

1. **懒缓存**：`preemptible_node_cache_` 是按需填充的。第一次遇到未知节点时查 GCS，之后复用缓存。节点标签在启动后不变，所以缓存安全。

2. **selectors 过滤**：不全量查 GCS 所有节点，只查 `uncached_nodes` 里的节点，减少 GCS 压力。

3. **`uncached_nodes` 的含义**：不是"不在 GCS 中的节点"，而是"在本地 `preemptible_node_cache_` 中没有记录的节点"。查 GCS 返回后更新缓存，之后再有该节点的查询就命中缓存了。更新时机：GCS 回调中调用 `CacheNodePreemptible()` 更新 `preemptible_node_cache_`，只增不删。

4. **与 NodeManager 的 `is_preemptible_node_` 的区别**：NodeManager 只关心**本节点**是否 preemptible，在构造时计算一次就不变；CoreWorker 需要知道**其他节点**的 preemptible 状态，所以维护了一个多节点缓存。

5. **GCS 回调中的二次 dedup**：`MaybeTriggerPinTransfer` 入口有第一次 dedup 检查，但 GCS 回调是异步的，回调执行时可能已有并发的 `DoPinTransfer`，所以回调内再检查一次 `pin_transfers_in_flight_`。

6. **`AddOwnedObject` 的 `pinned_at_node_id` 参数**：如果不传此参数，`IsPlasmaObjectPinnedOrSpilled` 会返回 `pinned_at = NodeID::Nil()`，导致 `MaybeTriggerPinTransfer` 提前返回。测试中必须传入 `pinned_at_node_id` 使对象被标记为 pinned。

### 4.4 DoPinTransfer 逻辑

```cpp
void CoreWorker::DoPinTransfer(const ObjectID &object_id,
                               const NodeID &stable_node_id,
                               const rpc::Address &stable_node_address) {
  // 1. 二次 dedup（Maybe 中的第一次 + Do 中的第二次）
  absl::MutexLock lock(&pin_transfer_mutex_);
  if (!pin_transfers_in_flight_.insert(object_id).second) return;

  // 2. 向目标 raylet 发起 PinObjectIDs RPC
  raylet_client_pool_->GetOrConnectByAddress(stable_node_address)
      ->PinObjectIDs(rpc_address_, {object_id}, /*generator_id=*/ObjectID::Nil(),
          [this, object_id, stable_node_id](status, reply) {
            // 3. 回调中清理 in_flight
            absl::MutexLock lock(&pin_transfer_mutex_);
            pin_transfers_in_flight_.erase(object_id);

            // 4. 成功则更新 pinned location
            if (status.ok() && reply.successes_size() > 0 && reply.successes(0)) {
              reference_counter_->UpdateObjectPinnedAtRaylet(object_id, stable_node_id);
            }
          });
}
```

---

## 五、NodeManager 侧的对象复制逻辑

### 5.1 MaybeReplicateObject

```cpp
void NodeManager::MaybeReplicateObject(const ObjectInfo &object_info) {
  // 1. 策略检查
  if (strategy != "push_to_stable_node") return;
  // 2. 本节点是否是抢占式
  if (!is_preemptible_node_) return;
  // 3. 大小阈值
  if (object_info.data_size < object_replication_min_size) return;
  // 4. 去重（set）
  absl::MutexLock lock(&replication_mutex_);
  if (replicated_objects_.contains(object_info.object_id)) return;
  replicated_objects_.insert(object_info.object_id);
  // 5. 选择目标节点
  NodeID target_node = SelectStableNode();
  if (target_node.IsNil()) return;
  // 6. Push（fire-and-forget）
  object_manager_.Push(object_info.object_id, target_node);
}
```

### 5.2 去重设计

`replicated_objects_` 是 `absl::flat_hash_set<ObjectID>`，不是并发计数器。原因：
- `Push()` 是 fire-and-forget，没有完成回调
- 因此无法知道复制何时完成，也无法在完成时清理
- 只需要防止**重复复制**，不需要并发限制
- 条目永不移除（Push 无回调）

### 5.3 SelectStableNode

遍历 `ClusterResourceManager` 的资源视图，排除：
- 自身节点（`self_node_id_`）
- 抢占式节点（label `ray.io/node-market-type` == `preemptible_node_market_type`）

从剩余稳定节点中随机选择一个。

**测试注意**：因为使用 `std::mt19937 rng_` 随机选择，测试中如果只有一个 stable 节点，断言 `== stable_node` 可以通过；但如果有多个 stable 节点，断言特定节点是不可靠的。

---

## 六、测试中遇到的问题和解决方案

### 6.1 Raw String Literal 中的 `)"` 提前终止

**问题**：
```cpp
R"({
  "object_replication_min_size": )")  // ← )"` 会提前终止 raw string
```

`R"(...)"` 的终止符是 `)"`。如果字符串内容包含 `)"`，会被误判为终止符。

**解决**：使用自定义分隔符 `R"xyz(...)xyz"`：
```cpp
R"xyz({
  "object_replication_min_size": )xyz";
config += std::to_string(min_size);
config += R"xyz(
})xyz";
```

### 6.2 AddOwnedObject 不传 pinned_at_node_id 导致测试失败

**问题**：`AddOwnedObject` 不传 `pinned_at_node_id` 参数时，`IsPlasmaObjectPinnedOrSpilled` 返回 `pinned_at = NodeID::Nil()`，`MaybeTriggerPinTransfer` 提前返回。

**解决**：传入 `pinned_at_node_id` 参数：
```cpp
reference_counter_->AddOwnedObject(object_id, {}, owner_address, "", 0,
                                   LineageReconstructionEligibility::INELIGIBLE_PUT,
                                   true,
                                   spot_node);  // ← pinned_at_node_id
```

### 6.3 回调内变量名 shadow 外层变量

**问题**：回调内 `auto pinned_at_preemptible = GetNodePreemptible(pinned_at)` 与外层同名，导致 `-Werror=shadow` 编译失败。

**解决**：回调内使用不同命名：
```cpp
auto pinned_at_is_preemptible = GetNodePreemptible(pinned_at);
auto new_location_is_preemptible = GetNodePreemptible(new_location_node_id);
```

### 6.4 FRIEND_TEST 与 fixture 辅助方法

**问题**：fixture 中的 `EnableReplicationConfig()` 方法无法访问 `node_manager_->is_preemptible_node_`（private）。

**解决**：`is_preemptible_node_ = true` 必须在每个 TEST_F 体中直接写，不能提取到 fixture 方法中。

### 6.5 SelectStableNode 随机性导致测试脆弱

**问题**：`SelectStableNode()` 使用 `std::mt19937 rng_` 随机选择，断言返回特定节点不可靠。

**解决**：
- 移除 `TestSelectStableNode`（只有一个 stable 节点时碰巧通过，但脆弱）
- 保留 `TestSelectStableNodeSkipsSpot`（只有一个 stable 节点，确定性）
- 新增 `TestSelectStableNodeOnlySpotNodes`（无 stable 节点，确定性返回 Nil）

### 6.6 PinTransferLogicTest 重复了生产代码

**问题**：`IsNodePreemptibleFromLabels` 是 `core_worker.cc` 中匿名命名空间的 static 函数，测试中复制了一份来测试，等于在测试复制的代码。

**解决**：移除 `PinTransferLogicTest`，通过 `PinTransferCoreWorkerTest` 间接测试（使用 `CacheNodePreemptible` + `GetNodePreemptible`）。

---

## 七、最终命名对照表

### CoreWorker 侧

| 原命名 | 新命名 | 理由 |
|---|---|---|
| `IsNodePreemptibleCached` | `GetNodePreemptible` | 符合 `Get*` 返回 `std::optional<T>` 的规范 |
| `preemptible_cache_mutex_` | `preemptible_node_cache_mutex_` | mutex 名与保护的 cache map 名一致 |
| `preemptible_node_cache_` | `preemptible_node_cache_` | 保持（符合） |
| `CacheNodePreemptible` | `CacheNodePreemptible` | 保持（符合 `Cache*` 规范） |
| `MaybeTriggerPinTransfer` | `MaybeTriggerPinTransfer` | 保持（符合 `Maybe*` 规范） |
| `DoPinTransfer` | `DoPinTransfer` | 保持（符合 `Do*` 规范） |
| `pin_transfers_in_flight_` | `pin_transfers_in_flight_` | 保持 |
| `pin_transfer_mutex_` | `pin_transfer_mutex_` | 保持 |
| `new_loc_preemptible`（局部变量） | `new_location_preemptible` | 避免缩写，与 `pinned_at` 风格一致 |
| `pa`/`nl`（回调内局部变量） | `pinned_at_is_preemptible`/`new_location_is_preemptible` | 避免与外层同名 shadow，保持可读性 |

### NodeManager 侧

| 原命名 | 新命名 | 理由 |
|---|---|---|
| `is_preemptible_node_cached_` | `is_preemptible_node_` | 它是 `IsPreemptibleNode()` 的缓存值，`_cached_` 后缀冗余且语义歧义 |
| `replicated_objects_` | `replicated_objects_` | 保持 |
| `replication_mutex_` | `replication_mutex_` | 保持 |
| `MaybeReplicateObject` | `MaybeReplicateObject` | 保持 |
| `IsPreemptibleNode` | `IsPreemptibleNode` | 保持（bool 查询用 `Is*`） |
| `SelectStableNode` | `SelectStableNode` | 保持 |
| `SelectMigrationTarget` | `SelectMigrationTarget` | 保持 |
