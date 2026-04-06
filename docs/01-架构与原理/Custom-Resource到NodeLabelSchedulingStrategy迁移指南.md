# Custom Resource 调度 → NodeLabelSchedulingStrategy 迁移指南

## 背景问题

Ray Data 管道中原先使用 Custom Resource 调度（`resources={"worker-X": 1}`）将算子调度到指定节点组，存在以下问题：

1. **消耗型资源限制并发**：Custom Resource 是消耗型的，调度后 available 变为 0，同一节点同时只能跑 1 个该任务
2. **需要显式声明**：节点启动时需 `ray start --resources` 声明 custom resource，依赖 KubeRay 配置
3. **语义不精确**：用"资源"来表达"调度到哪个节点组"的意图是间接的

KubeRay 已自动为每个节点注入 `ray.io/node-group` 标签（值等于 CR groupName），可直接用 Node Label 调度匹配，语义更精确且不消耗资源。

---

## 一、两种调度机制对比

### 1.1 Custom Resource 调度

```python
# 在 ray start / KubeRay CR 中声明资源
ray start --resources='{"worker-2": 1}'

# 在代码中指定
ds.map_batches(FsRayActor, resources={"worker-2": 1})
```

**特性：**
- **消耗型**：调度后节点的 `worker-2` available 减为 0，同节点同资源同时只能跑 1 个任务
- **需要节点声明**：节点启动时必须 `--resources` 声明，否则调度失败
- **语义间接**：用"资源数量"表达"调度到哪个节点组"

### 1.2 Node Label 调度

```python
from ray.util.scheduling_strategies import NodeLabelSchedulingStrategy, In

ds.map_batches(
    FsRayActor,
    scheduling_strategy=NodeLabelSchedulingStrategy(
        hard={"ray.io/node-group": In("worker-2")}
    ),
)
```

**特性：**
- **只读型**：标签匹配不消耗资源，同节点不限并发
- **自动注入**：KubeRay 自动注入 `ray.io/node-group` 标签，无需手动声明
- **语义精确**：直接表达"调度到 worker-2 节点组"

### 1.3 对比总结

| 维度 | Custom Resource | Node Label |
|------|---------------|------------|
| 类型 | 消耗型（available 减少） | 只读型（不消耗） |
| 并发 | 同节点同资源同时只能 1 个 | 同节点不限并发 |
| 声明 | 需要 `--resources` 手动声明 | KubeRay 自动注入 |
| API | `resources={"worker-2": 1}` | `scheduling_strategy=NodeLabelSchedulingStrategy(...)` |
| 匹配 | 精确匹配资源名 | In/NotIn/Exists/DoesNotExist |
| Ray 版本 | 2.0+ | 2.9+（推荐） |

---

## 二、KubeRay 标签注入链路

KubeRay 自动为每个节点注入 `ray.io/node-group` 标签，完整链路：

```
RayCluster CR groupName 字段
    ↓
Pod 环境变量 RAY_NODE_TYPE_NAME
    ↓
Ray raylet 启动时 _get_default_labels()
    ↓
ray.io/node-group: <groupName>
```

### 2.1 链路 Step 1：KubeRay 设置环境变量

KubeRay 在创建 Pod 时，将 CR 的 `groupName` 写入 Pod 环境变量 `RAY_NODE_TYPE_NAME`。

**源码位置**：`python/ray/autoscaler/v2/instance_manager/ray_installer.py:71`

```python
"RAY_NODE_TYPE_NAME": instance.instance_type,
```

### 2.2 链路 Step 2：C++ 常量定义

**源码位置**：`src/ray/common/constants.h`

```cpp
// Line 90: 环境变量名
constexpr char kNodeTypeNameEnv[] = "RAY_NODE_TYPE_NAME";

// Line 133: 标签 key 常量（RAY_LABEL_KEY_PREFIX = "ray.io/"）
constexpr char kLabelKeyNodeGroup[] = RAY_LABEL_KEY_PREFIX "node-group";
// 展开后 = "ray.io/node-group"
```

### 2.3 链路 Step 3：Python 常量暴露（Cython）

**源码位置**：`python/ray/includes/common.pxi:156-165`

```python
NODE_TYPE_NAME_ENV = kNodeTypeNameEnv.decode()       # "RAY_NODE_TYPE_NAME"
RAY_NODE_GROUP_KEY = kLabelKeyNodeGroup.decode()       # "ray.io/node-group"
```

可通过 `ray._raylet.NODE_TYPE_NAME_ENV` / `ray._raylet.RAY_NODE_GROUP_KEY` 访问。

### 2.4 链路 Step 4：_get_default_labels() — 核心注入逻辑

**源码位置**：`python/ray/_private/resource_and_label_spec.py:267-300`

```python
@staticmethod
def _get_default_labels(
    accelerator_manager: Optional[AcceleratorManager],
) -> Dict[str, str]:
    default_labels = {}

    # 从 K8s Pod 环境变量读取
    node_group = os.environ.get(ray._raylet.NODE_TYPE_NAME_ENV, "")       # RAY_NODE_TYPE_NAME
    market_type = os.environ.get(ray._raylet.NODE_MARKET_TYPE_ENV, "")     # RAY_NODE_MARKET_TYPE
    availability_region = os.environ.get(ray._raylet.NODE_REGION_ENV, "")  # RAY_NODE_REGION
    availability_zone = os.environ.get(ray._raylet.NODE_ZONE_ENV, "")      # RAY_NODE_ZONE

    # 环境变量 → Ray 节点标签
    if node_group:
        default_labels[ray._raylet.RAY_NODE_GROUP_KEY] = node_group    # "ray.io/node-group" = <groupName>
    if market_type:
        default_labels[ray._raylet.RAY_NODE_MARKET_TYPE_KEY] = market_type
    if availability_zone:
        default_labels[ray._raylet.RAY_NODE_ZONE_KEY] = availability_zone
    if availability_region:
        default_labels[ray._raylet.RAY_NODE_REGION_KEY] = availability_region

    # 加速器类型标签（GPU/TPU）
    if accelerator_manager:
        accelerator_type = accelerator_manager.get_current_node_accelerator_type()
        if accelerator_type:
            default_labels[ray._raylet.RAY_NODE_ACCELERATOR_TYPE_KEY] = accelerator_type
    return default_labels
```

### 2.5 链路 Step 5：_resolve_labels() — 标签合并

**源码位置**：`python/ray/_private/resource_and_label_spec.py:303-325`

```python
def _resolve_labels(self, accelerator_manager):
    # 以默认标签为基底
    merged = ResourceAndLabelSpec._get_default_labels(accelerator_manager)

    # 合并用户通过 --labels 指定的标签（覆盖同名 key）
    for key, val in (self.labels or {}).items():
        merged[key] = val

    # 合并 autoscaler 通过环境变量覆盖的标签
    env_labels = ResourceAndLabelSpec._load_env_labels()
    for key, val in (env_labels or {}).items():
        merged[key] = val

    self.labels = merged
```

合并优先级：默认标签 < `--labels` 用户指定 < autoscaler 环境变量覆盖

### 2.6 从 raylet 启动命令验证

实际节点上，raylet 启动命令同时包含 custom resource 和 label 声明：

```bash
ray_start_command="--static_resource_list=CPU,16,GPU,4,memory,107374182400,worker-2,1 \
                   --labels=ray.io/node-group=worker-2"
```

即节点**同时声明了** custom resource `worker-2,1` 和 label `ray.io/node-group: worker-2`，两者都可用于调度，但语义和并发行为不同。

### 2.7 Custom Resource 的 raylet 启动命令传递

**Python CLI 入口**：`python/ray/scripts/scripts.py:458`

```python
"--resources",
```

**Python → raylet 传递**：`python/ray/_private/services.py:1887`

```python
f"--static_resource_list={resource_argument}",
```

格式为逗号分隔的键值对：`--static_resource_list=CPU,16,GPU,4,memory,107374182400,worker-2,1`

**C++ raylet 解析**：`src/ray/raylet/main.cc:97,549-560`

```cpp
// gflag 定义
DEFINE_string(static_resource_list, "", "The static resource list of this node.");

// 解析逻辑：逗号分隔，交替读 key 和 quantity
std::istringstream resource_string(static_resource_list);
std::string resource_name;
std::string resource_quantity;

while (std::getline(resource_string, resource_name, ',')) {
    RAY_CHECK(std::getline(resource_string, resource_quantity, ','));
    static_resource_conf[resource_name] = std::stod(resource_quantity);
}
```

### 2.8 测试验证

**源码位置**：`python/ray/tests/unit/test_resource_and_label_spec.py:249-267`

```python
monkeypatch.setenv("RAY_NODE_TYPE_NAME", "worker-group-1")
...
assert spec.labels.get("ray.io/node-group") == "worker-group-1"
```

**源码位置**：`python/ray/tests/test_node_labels.py:171-181`

```python
monkeypatch.setenv("RAY_NODE_TYPE_NAME", "worker-group-1")
...
assert labels.get("ray.io/node-group") == "worker-group-1"
```

---

## 三、In 操作符详解

### 3.1 正确的导入路径

**`In` 类定义在 `ray.util.scheduling_strategies`，不是 `ray.util.label_utils`**。

```python
# ✅ 正确导入（Ray 2.9+）
from ray.util.scheduling_strategies import NodeLabelSchedulingStrategy, In, NotIn, Exists, DoesNotExist

# ❌ 不存在此模块（会报 ModuleNotFoundError）
from ray.util.label_utils import In
```

> `ray.util.label_utils` 是 **private** 模块（`ray/_private/label_utils.py`），仅包含标签验证函数
> （`validate_label_selector`、`parse_node_labels_json` 等），**不包含 `In` 类**。

### 3.2 In 类源码定义

**源码位置**：`python/ray/util/scheduling_strategies.py:123-127`

```python
@PublicAPI(stability="alpha")
class In:
    def __init__(self, *values):
        _validate_label_match_operator_values(values, "In")
        self.values = list(values)
```

**验证辅助函数**：`python/ray/util/scheduling_strategies.py:106-122`

```python
def _validate_label_match_operator_values(values, operator):
    if not values:
        raise ValueError(
            f"The variadic parameter of the {operator} operator"
            f' must be a non-empty tuple: e.g. {operator}("value1", "value2").'
        )
    for value in values:
        if not isinstance(value, str):
            raise ValueError(
                f"Type of value in position {index} for the {operator} operator "
                f'must be str (e.g. {operator}("value1", "value2")) '
                f"but got {str(value)} of type {type(value)}."
            )
```

### 3.3 四种标签匹配操作符

全部定义在 `python/ray/util/scheduling_strategies.py:123-144`：

```python
@PublicAPI(stability="alpha")
class In:
    def __init__(self, *values):
        _validate_label_match_operator_values(values, "In")
        self.values = list(values)

@PublicAPI(stability="alpha")
class NotIn:
    def __init__(self, *values):
        _validate_label_match_operator_values(values, "NotIn")
        self.values = list(values)

@PublicAPI(stability="alpha")
class Exists:
    def __init__(self): pass

@PublicAPI(stability="alpha")
class DoesNotExist:
    def __init__(self): pass
```

| 操作符 | 语义 | 等价 SQL | 示例 |
|--------|------|---------|------|
| `In("worker-2")` | label 值在集合中 | `label IN ('worker-2')` | 值 == "worker-2" |
| `NotIn("worker-2")` | label 值不在集合中 | `label NOT IN ('worker-2')` | 值 != "worker-2" |
| `Exists` | label key 存在（不限值） | `label IS NOT NULL` | key 存在即可 |
| `DoesNotExist` | label key 不存在 | `label IS NULL` | key 不存在 |

### 3.4 为什么不能直接传字符串？

`NodeLabelSchedulingStrategy` 的 `hard` dict 要求 value 必须是**标签匹配操作符对象**，不接受裸字符串。

**验证逻辑**：`python/ray/util/scheduling_strategies.py:189-217` 的 `_convert_map_to_expressions()`

```python
def _convert_map_to_expressions(expressions, param_name):
    if not isinstance(expressions, Dict):
        raise ValueError(f"The `{param_name}` parameter must be a Dict, ...")
    result = []
    for key, value in expressions.items():
        if not isinstance(key, str):
            raise ValueError(f"Key must be str, got {type(key)}")
        if not isinstance(value, (In, NotIn, Exists, DoesNotExist)):
            raise ValueError(
                f"Value must be In/NotIn/Exists/DoesNotExist, "
                f"got {type(value)}"
            )
        result.append(_LabelMatchExpression(key, value))
    return result
```

```python
# ❌ 类型错误 - 不接受裸字符串
NodeLabelSchedulingStrategy(hard={"ray.io/node-group": "worker-2"})
# ValueError: Value must be In/NotIn/Exists/DoesNotExist, got <class 'str'>

# ✅ 正确 - 使用 In 操作符
NodeLabelSchedulingStrategy(hard={"ray.io/node-group": In("worker-2")})
```

### 3.5 In 集合语法

`In` 支持传入集合，匹配多个值：

```python
# 匹配单个值
In("worker-2")

# 匹配多个值（调度到 worker-2 或 worker-3 节点组）
In("worker-2", "worker-3")
```

---

## 四、NodeLabelSchedulingStrategy vs label_selector

### 4.1 label_selector 示例

```python
Actor.options(
    name="my_actor",
    label_selector={"ray.io/node-group": "wg1"},
    lifetime="detached",
).remote()
```

### 4.2 区别

| 维度 | `label_selector` | `NodeLabelSchedulingStrategy` |
|------|-----------------|-------------------------------|
| 匹配方式 | 仅精确匹配（等于） | In/NotIn/Exists/DoesNotExist |
| API 代际 | 旧 API（Ray 2.0+） | 新 API（Ray 2.9+），官方推荐 |
| 适用范围 | `Actor.options()` / `ray.remote()` | 同上 + `ray_remote_args` 中的 `scheduling_strategy` key |
| 内部实现 | 裸字符串自动包装为精确匹配 | 显式操作符，更灵活 |

### 4.3 选择建议

- `label_selector` 传裸字符串可工作（内部自动转换为等价的 `NodeLabelSchedulingStrategy`），但**仅支持精确匹配**
- `NodeLabelSchedulingStrategy` 是官方推荐的新 API，在 `map_batches(ray_remote_args=)` 中标准参数名是 `scheduling_strategy`
- 未来如需 `NotIn`（排除某节点组）或 `Exists`（匹配任意有该标签的节点），`NodeLabelSchedulingStrategy` 不用改 API

---

## 五、repartition 不支持 scheduling_strategy

### 5.1 关键发现

`repartition()` 不支持 `scheduling_strategy` 参数，也不支持 `resources` 参数。原代码：

```python
# ❌ resources 被 **kwargs 静默吞掉，从未生效
ds.repartition(target_num_rows_per_block=N, resources={"worker-1": 1})
```

`repartition` 的 API 签名不接受 `resources`，传入后被 `**kwargs` 静默忽略。

### 5.2 解决方案：DataContext.scheduling_strategy

对于 `repartition` 等不支持单独调度策略的算子，只能通过 `DataContext.scheduling_strategy` 全局控制：

```python
from ray.data import DataContext

ctx = DataContext.get_current()
ctx.scheduling_strategy = NodeLabelSchedulingStrategy(
    hard={"ray.io/node-group": In("worker-1")}
)
```

**优先级逻辑**（`actor_pool_map_operator.py:565-566`）：
- 算子单独指定了 `scheduling_strategy` → 用单独的
- 未指定 → 用 `DataContext.scheduling_strategy` 全局默认

### 5.3 map_batches 中 scheduling_strategy 的传递方式

`map_batches` 使用 `ActorPoolStrategy` 时，`scheduling_strategy` 在 `ray_remote_args` 中传入：

```python
ds.map_batches(
    FsRayActor,
    scheduling_strategy=NodeLabelSchedulingStrategy(...),  # 传入 ray_remote_args
)
```

如果未指定，则用 `DataContext.scheduling_strategy` 默认值。

---

## 六、Raylet 调度器处理 NodeLabelSchedulingStrategy 的完整流程

### 6.1 Python → Protobuf 序列化

**源码位置**：`python/ray/_raylet.pyx:3452-3465`

当 Python `NodeLabelSchedulingStrategy` 提交为 task 调度策略时，Cython 层将 `hard`/`soft` 表达式序列化为 protobuf `LabelMatchExpression` 消息：

```python
# In → label_in, NotIn → label_not_in, Exists → label_exists, DoesNotExist → label_does_not_exist
```

**Protobuf 定义**：`src/ray/protobuf/common.proto:83-89`

```protobuf
message NodeLabelSchedulingStrategy {
  LabelMatchExpressions hard = 1;
  LabelMatchExpressions soft = 2;
}
```

### 6.2 调度路由

**源码位置**：`src/ray/raylet/scheduling/cluster_resource_scheduler.cc:199`

```cpp
} else if (scheduling_strategy.has_node_label_scheduling_strategy()) {
    best_node_id = scheduling_policy_->Schedule(
        resource_request, SchedulingOptions::NodeLabelScheduling(scheduling_strategy));
```

**路由分发**：`src/ray/raylet/scheduling/policy/composite_scheduling_policy.cc:36-37`

```cpp
case SchedulingType::NODE_LABEL:
    return node_label_scheduling_policy_.Schedule(resource_request, options);
```

### 6.3 核心调度算法

**源码位置**：`src/ray/raylet/scheduling/policy/node_label_scheduling_policy.cc:26-81`

```cpp
scheduling::NodeID NodeLabelSchedulingPolicy::Schedule(
    const ResourceRequest &resource_request, SchedulingOptions options) {
  RAY_CHECK(options.scheduling_type_ == SchedulingType::NODE_LABEL);
  auto context = dynamic_cast<const NodeLabelSchedulingContext *>(
      options.scheduling_context_.get());
  const auto &scheduling_strategy = context->GetSchedulingStrategy();

  // 1. 按资源需求筛选可行节点（和普通调度一样，必须有足够 CPU/GPU/memory）
  auto hard_match_nodes = SelectFeasibleNodes(resource_request);
  if (hard_match_nodes.empty()) return scheduling::NodeID::Nil();

  // 2. 按 hard 表达式过滤（如 ray.io/node-group In worker-2）
  if (node_label_scheduling_strategy.hard().expressions().size() > 0) {
    hard_match_nodes = FilterNodesByLabelMatchExpressions(
        hard_match_nodes, node_label_scheduling_strategy.hard());
    if (hard_match_nodes.empty()) return scheduling::NodeID::Nil();
  }

  // 3. 按 soft 表达式过滤（偏好，不强制）
  absl::flat_hash_map<scheduling::NodeID, const Node *> hard_and_soft_match_nodes;
  if (soft_expressions.expressions().size() > 0) {
    hard_and_soft_match_nodes =
        FilterNodesByLabelMatchExpressions(hard_match_nodes, soft_expressions);
  }

  return SelectBestNode(hard_match_nodes, hard_and_soft_match_nodes, resource_request);
}
```

### 6.4 节点选择优先级

**源码位置**：`src/ray/raylet/scheduling/policy/node_label_scheduling_policy.cc:58-81`

```cpp
scheduling::NodeID NodeLabelSchedulingPolicy::SelectBestNode(...) {
  // 1. Hard+Soft 匹配 + 资源充足（最优）
  if (!hard_and_soft_match_nodes.empty()) {
    auto available_soft_nodes = SelectAvailableNodes(hard_and_soft_match_nodes, resource_request);
    if (!available_soft_nodes.empty()) return SelectRandomNode(available_soft_nodes);
  }
  // 2. Hard 匹配 + 资源充足
  auto available_nodes = SelectAvailableNodes(hard_match_nodes, resource_request);
  if (!available_nodes.empty()) return SelectRandomNode(available_nodes);
  // 3. Hard+Soft 匹配 + 资源不够（回退）
  if (!hard_and_soft_match_nodes.empty()) return SelectRandomNode(hard_and_soft_match_nodes);
  // 4. Hard 匹配 + 资源不够（兜底）
  return SelectRandomNode(hard_match_nodes);
}
```

### 6.5 标签表达式匹配

**源码位置**：`src/ray/raylet/scheduling/policy/node_label_scheduling_policy.cc:99-130`

```cpp
bool NodeLabelSchedulingPolicy::IsNodeMatchLabelExpression(
    const Node &node, const rpc::LabelMatchExpression &expression) const {
  const auto &key = expression.key();
  const auto &match_operator = expression.operator_();
  if (match_operator.has_label_exists()) {
    return IsNodeLabelKeyExists(node, key);
  } else if (match_operator.has_label_does_not_exist()) {
    return !IsNodeLabelKeyExists(node, key);
  } else if (match_operator.has_label_in()) {
    // 检查节点的 label 值是否在 In() 集合中
    absl::flat_hash_set<std::string> values;
    for (const auto &value : match_operator.label_in().values()) values.insert(value);
    return IsNodeLabelInValues(node, key, values);
  } else if (match_operator.has_label_not_in()) {
    // 检查节点的 label 值是否不在 NotIn() 集合中
    absl::flat_hash_set<std::string> values;
    for (const auto &value : match_operator.label_not_in().values()) values.insert(value);
    return !IsNodeLabelInValues(node, key, values);
  }
  return false;
}
```

**关键区别**：标签匹配是**只读查询**，不像 custom resource 会扣减 available 数量。这意味着同一节点上不限并发地调度多个 `In("worker-2")` 任务。

---

## 七、代码改动详情

### 7.1 fs_ray_config.py — 配置层

**新增 4 个调度标签字段：**

```python
# ── 调度标签 ──
# 指定 Ray Data 各算子调度到哪个 ray.io/node-group 标签的节点组。
# 使用 NodeLabelSchedulingStrategy（标签匹配，不消耗资源），区别于 resources=（消耗型，限并发）。
# 空字符串 = 不指定（走 DataContext.scheduling_strategy 全局默认）。
# DataContext.scheduling_strategy 默认为 worker-1（覆盖 repartition 等不支持单独调度策略的算子）。
# fs actor 默认为 worker-2（需要 GPU），其他算子默认走全局 worker-1。
fs_actor_scheduling_label: str = "worker-2"
read_scheduling_label: str = "worker-1"
kafka_sink_scheduling_label: str = "worker-1"
default_scheduling_label: str = "worker-1"
```

**注册 env_map 和 json_key 映射：**

```python
# 环境变量映射
"FS_RAY_FS_ACTOR_SCHEDULING_LABEL": ("fs_actor_scheduling_label", str),
"FS_RAY_READ_SCHEDULING_LABEL": ("read_scheduling_label", str),
"FS_RAY_KAFKA_SINK_SCHEDULING_LABEL": ("kafka_sink_scheduling_label", str),
"FS_RAY_DEFAULT_SCHEDULING_LABEL": ("default_scheduling_label", str),

# JSON key 映射
("compute", "fs_actor_scheduling_label"): "fs_actor_scheduling_label",
("compute", "read_scheduling_label"): "read_scheduling_label",
("compute", "kafka_sink_scheduling_label"): "kafka_sink_scheduling_label",
("compute", "default_scheduling_label"): "default_scheduling_label",
```

**print_summary 新增调度标签输出段：**

```
── 调度标签（NodeLabelSchedulingStrategy）──
  default(DataContext): ray.io/node-group='worker-1'
  actor:              ray.io/node-group='worker-2'
  read:               ray.io/node-group='worker-1'
  kafka_sink:         ray.io/node-group='worker-1'
```

### 7.2 fs_ray_pipeline.py — 调度层

**新增常量和辅助函数：**

```python
from ray.util.scheduling_strategies import NodeLabelSchedulingStrategy, In

_LABEL_KEY = "ray.io/node-group"

def _label_scheduling_strategy(label: str):
    if not label:
        return None
    return NodeLabelSchedulingStrategy(hard={_LABEL_KEY: In(label)})
```

**各算子替换详情：**

| 算子 | 原代码 | 新代码 |
|------|--------|--------|
| `map_batches(FsRayActor)` | `resources={"worker-2": 1}` | `scheduling_strategy=_label_scheduling_strategy(config.fs_actor_scheduling_label)` |
| `_json_source` (read) | `resources={"worker-1": 1}` | `ray_remote_args={"scheduling_strategy": _label_scheduling_strategy(config.read_scheduling_label)}` |
| `apply_failed_kafka_sink` | `resources={"worker-1": 1}` | `scheduling_strategy=_label_scheduling_strategy(config.kafka_sink_scheduling_label)` |
| `apply_success_kafka_sink` | `resources={"worker-1": 1}` | `scheduling_strategy=_label_scheduling_strategy(config.kafka_sink_scheduling_label)` |
| `repartition` | `resources={"worker-1": 1}` (无效) | **移除**（repartition 不支持） |

**DataContext 全局默认调度策略：**

在 `run_fs_ray_pipeline` 中设置，覆盖 repartition 等不支持单独调度的算子：

```python
if config.default_scheduling_label:
    from ray.data import DataContext
    ctx = DataContext.get_current()
    ctx.scheduling_strategy = _label_scheduling_strategy(config.default_scheduling_label)
    print(f"  [DataContext] scheduling_strategy = NodeLabelSchedulingStrategy({_LABEL_KEY}={config.default_scheduling_label!r})")
```

---

## 八、注意事项

### 8.1 In 类的正确导入路径

`In` 类定义在 `ray.util.scheduling_strategies`，**不是** `ray.util.label_utils`。

```python
# ✅ 正确
from ray.util.scheduling_strategies import In, NodeLabelSchedulingStrategy

# ❌ 不存在此模块
from ray.util.label_utils import In  # ModuleNotFoundError
```

如果运行环境 Ray 版本偏低报 `ModuleNotFoundError`，说明该版本尚未支持 NodeLabelSchedulingStrategy（需 Ray 2.9+）。

### 8.2 _parquet_source 和 _ksdataset_source

这两个 source 函数未显式传入 `read_scheduling_label`，但 `DataContext.scheduling_strategy` 全局默认 worker-1 已覆盖，功能不受影响。如需区分 read 调度到其他节点组，需给这两个 source 也加上 `scheduling_strategy`。

### 8.3 label 为空字符串的行为

`_label_scheduling_strategy("")` 返回 `None`，即不指定调度策略，走 `DataContext.scheduling_strategy` 全局默认。这是有意的设计，允许用户通过设空来回退到全局默认。

### 8.4 NodeLabelSchedulingStrategy 的 hard 和 soft 不能同时为空

**源码位置**：`python/ray/util/scheduling_strategies.py:183-186`

```python
def _check_usage(self):
    if not (self.hard or self.soft):
        raise ValueError(
            "The `hard` and `soft` parameter "
            "of NodeLabelSchedulingStrategy cannot both be empty."
        )
```

因此 `_label_scheduling_strategy` 在 label 为空时返回 `None` 而非空的 `NodeLabelSchedulingStrategy`，避免触发此校验。

---

## 九、关键源码索引

| 文件 | 行号 | 内容 |
|------|------|------|
| `src/ray/common/constants.h` | 90, 133 | C++ 常量：`kNodeTypeNameEnv`、`kLabelKeyNodeGroup` |
| `python/ray/includes/common.pxi` | 156-165 | Cython 暴露的 Python 常量 |
| `python/ray/_private/resource_and_label_spec.py` | 267-300 | `_get_default_labels()` — 标签注入核心 |
| `python/ray/_private/resource_and_label_spec.py` | 303-325 | `_resolve_labels()` — 标签合并逻辑 |
| `python/ray/_private/services.py` | 1887 | `--static_resource_list=` Python→raylet 传递 |
| `src/ray/raylet/main.cc` | 97, 549-560 | `--static_resource_list` gflag 定义和解析 |
| `python/ray/util/scheduling_strategies.py` | 106-127 | `In` 类定义 + 验证 |
| `python/ray/util/scheduling_strategies.py` | 129-144 | `NotIn`/`Exists`/`DoesNotExist` 定义 |
| `python/ray/util/scheduling_strategies.py` | 164-186 | `NodeLabelSchedulingStrategy` 类 + `hard`/`soft` 校验 |
| `python/ray/util/scheduling_strategies.py` | 189-217 | `_convert_map_to_expressions()` — 验证+转换 |
| `python/ray/_raylet.pyx` | 3405-3465 | Cython 序列化 Python 对象 → protobuf |
| `src/ray/protobuf/common.proto` | 83-121 | Protobuf `NodeLabelSchedulingStrategy` 消息定义 |
| `src/ray/raylet/scheduling/cluster_resource_scheduler.cc` | 199 | 路由到 `NodeLabelScheduling` |
| `src/ray/raylet/scheduling/policy/node_label_scheduling_policy.cc` | 26-81 | 核心调度算法（hard/soft 过滤 + 优先级选择） |
| `src/ray/raylet/scheduling/policy/node_label_scheduling_policy.cc` | 99-130 | `IsNodeMatchLabelExpression` — In/NotIn/Exists/DoesNotExist 匹配 |
| `src/ray/raylet/scheduling/policy/scheduling_options.h` | 40, 110-115 | `SchedulingType::NODE_LABEL = 9` |
| `src/ray/raylet/scheduling/policy/scheduling_context.h` | 48-59 | `NodeLabelSchedulingContext` |
| `python/ray/_private/label_utils.py` | — | Private 模块 — 仅标签验证，无 `In` 类 |
| `python/ray/tests/test_node_labels.py` | 171-181 | 标签注入测试验证 |
