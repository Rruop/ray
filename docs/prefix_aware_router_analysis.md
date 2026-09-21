# PrefixCacheAffinityRouter 架构与优化详解

> 基于对 `prefix_aware_router.py`、`prefix_tree.py`、Ray Serve 框架代码、SGLang `sgl-model-gateway` Rust 实现的完整分析

---

## 目录

1. [Router 工作逻辑总览](#1-router-工作逻辑总览)
2. [PrefixTree 前缀匹配机制](#2-prefixtree-前缀匹配机制)
3. [路由决策详细流程](#3-路由决策详细流程)
4. [Ray Serve LLM 路由架构：Ingress 与 Router 的关系](#4-ray-serve-llm-路由架构ingress-与-router-的关系)
5. [Ray Serve 架构关系：通用组件](#5-ray-serve-架构关系通用组件)
6. [KESS 场景下的请求路径](#6-kess-场景下的请求路径)
7. [PrefixTreeActor 性能分析与 QPS 估算](#7-prefixtreeactor-性能分析与-qps-估算)
8. [SGLang Rust 实现对比](#8-sglang-rust-实现对比)
9. [多 Gateway 场景：共享 vs 独立 Tree 分析](#9-多-gateway-场景共享-vs-独立-tree-分析)
10. [优化方案推荐](#10-优化方案推荐)
11. [NVIDIA Dynamo Selection Service KV Cache 路由详解](#11-nvidia-dynamo-selection-service-kv-cache-路由详解)
12. [Ray Serve KVAwareRouter 完整代码实现](#12-ray-serve-kvawarerouter-完整代码实现)
13. [vLLM 与 SGLang 的 KV 事件系统](#13-vllm-与-sglang-的-kv-事件系统)
14. [KVAwareRouter 对 SGLang 的支持现状与适配路径](#14-kvawarerouter-对-sglang-的支持现状与适配路径)

---

## 1. Router 工作逻辑总览

### 1.1 类继承关系

```
PrefixCacheAffinityRouter
├── LocalityMixin          # 本地性路由（同节点优先）
├── MultiplexMixin          # 多路复用路由（按 model_id 路由）
└── RequestRouter           # 基础路由框架
    └── 内含 PowerOfTwoChoicesRequestRouter 作为 fallback
```

源码位置：`python/ray/llm/_internal/serve/routing_policies/prefix_aware/prefix_aware_router.py:40`

```python
class PrefixCacheAffinityRouter(LocalityMixin, MultiplexMixin, RequestRouter):
```

### 1.2 核心设计目标

最大化 LLM 推理的 **KV Cache 命中率**：将具有相似前缀的请求路由到同一个 replica，使其 KV Cache 中已有对应的 KV 对可以直接复用，避免重复计算。

### 1.3 三级路由策略

| 条件 | 策略 | 理由 |
|------|------|------|
| 负载不均衡 | PowerOfTwo fallback | 优先保证负载均衡，避免热点 |
| 负载均衡 + 前缀匹配率 ≥ 10% | 路由到前缀匹配的 replica | 最大化 KV Cache 复用 |
| 负载均衡 + 前缀匹配率 < 10% | 路由到 KV Cache 最小的 replica | 匹配太低无缓存收益，选最空闲的 |

### 1.4 initialize_state 配置

源码位置：`prefix_aware_router.py:59-67`

```python
def initialize_state(
    self,
    imbalanced_threshold: Optional[float] = float("inf"),  # 队列差阈值
    match_rate_threshold: Optional[float] = 0.1,           # 前缀匹配率阈值 10%
    do_eviction: Optional[bool] = False,                   # 是否启用淘汰
    eviction_threshold_chars: Optional[int] = 400_000,      # 淘汰阈值 400K 字符
    eviction_target_chars: Optional[int] = 360_000,          # 淘汰目标 360K 字符
    eviction_interval_secs: Optional[int] = 10,             # 淘汰检查间隔
    tree_actor: Optional[ActorHandle] = None,               # 测试用注入
):
```

---

## 2. PrefixTree 前缀匹配机制

### 2.1 数据结构

源码位置：`python/ray/llm/_internal/serve/routing_policies/prefix_aware/prefix_tree.py`

#### Node 结构（`prefix_tree.py:17-49`）

```python
class Node:
    def __init__(self, text: str = "", parent: Optional[Node] = None):
        self.text: str = text                           # 本节点代表的文本片段
        self.parent: Optional[Node] = parent             # 父节点
        self.edge_label_to_child: Dict[str, Node] = {}   # 首字符 → 子节点映射
        self.tenant_to_last_access_time: Dict[str, float] = {}  # tenant → 最后访问时间
        self.tenant_to_older_node: Dict[str, Optional[Node]] = {} # LRU 链表：指向更旧节点
        self.tenant_to_newer_node: Dict[str, Optional[Node]] = {} # LRU 链表：指向更新节点
```

#### PrefixTree 结构（`prefix_tree.py:52-100`）

```python
class PrefixTree:
    def __init__(self):
        self.lock: RLock = RLock()                      # 全局读写锁
        self.root: Node = Node()                        # 根节点
        self.tenant_to_char_count: Dict[str, int] = {}  # 每个 tenant 的总字符数
        self.tenant_to_lru_tail: Dict[str, Optional[Node]] = {} # 每个 tenant 的 LRU 尾部
```

### 2.2 前缀匹配算法

#### 匹配流程（`prefix_tree.py:376-445`）

```python
def prefix_match(self, text: str, available_tenants: Optional[List[str]] = None
) -> Tuple[str, Optional[List[str]]]:
    with self.lock:
        # 1. 过滤可用的 tenant（只保留树中存在的）
        if available_tenants:
            available_tenants = [
                tenant for tenant in available_tenants
                if tenant in self.tenant_to_char_count
            ]
            if not available_tenants:
                return "", None

        # 2. 从根节点逐层向下遍历
        curr_node: Node = self.root
        i: int = 0
        text_len: int = len(text)

        while i < text_len:
            first_char: str = text[i]
            curr_text: str = text[i:]

            if first_char in curr_node.edge_label_to_child:
                matched_node: Node = curr_node.edge_label_to_child[first_char]

                # 2a. 检查匹配节点是否有可用 tenant
                if not any(
                    tenant in matched_node.tenant_to_last_access_time
                    for tenant in available_tenants
                ):
                    break

                # 2b. 计算共享前缀长度
                shared_count: int = self._shared_prefix_count(
                    matched_node.text, curr_text
                )
                i += shared_count
                curr_node = matched_node

                if shared_count < len(matched_node.text):
                    # 部分匹配，停止遍历
                    break
            else:
                # 无匹配，停止遍历
                break

        # 3. 返回匹配结果
        matched_tenants = [
            tenant for tenant in available_tenants
            if tenant in curr_node.tenant_to_last_access_time
        ] or None

        matched_text: str = text[:i]
        return matched_text, matched_tenants
```

#### 共享前缀计算（`prefix_tree.py:102-114`）

```python
@staticmethod
def _shared_prefix_count(a: str, b: str) -> int:
    return len(os.path.commonprefix([a, b]))
```

> 使用 Python 标准库 `os.path.commonprefix` 逐字节比较，计算两个字符串从开头共享的字符数。

### 2.3 插入算法（节点分裂）

#### insert 核心逻辑（`prefix_tree.py:253-374`）

```python
def insert(self, text: str, tenant: str, time_s: float) -> None:
    with self.lock:
        curr_node: Node = self.root
        i: int = 0
        while i <= len(text):
            # 1. 更新当前节点的 tenant 信息
            if tenant not in curr_node.tenant_to_last_access_time:
                self.tenant_to_char_count[tenant] += len(curr_node.text)
            curr_node.tenant_to_last_access_time[tenant] = time_s

            # 2. 移动到 LRU 链表头部
            if curr_node != self.root:
                self._remove_node_from_linked_list(curr_node, tenant)
                self._insert_node_into_linked_list(
                    curr_node, self.root,
                    self.root.tenant_to_older_node.get(tenant), tenant
                )

            if i == len(text):
                break

            first_char: str = text[i]
            curr_text: str = text[i:]

            if first_char not in curr_node.edge_label_to_child:
                # 3. 无匹配，创建新叶子节点
                new_node: Node = Node(text=curr_text, parent=curr_node)
                curr_node.edge_label_to_child[first_char] = new_node

            # 4. 有匹配，检查是否需要分裂
            matched_node: Node = curr_node.edge_label_to_child[first_char]
            shared_count: int = self._shared_prefix_count(matched_node.text, curr_text)

            if shared_count < len(matched_node.text):
                # 5. 部分匹配 → 分裂节点
                #    例: 节点 "helloworld" + 插入 "hellothere"
                #    → 新父节点 "hello" + 子节点 "world"
                matched_text: str = matched_node.text[:shared_count]
                remaining_text: str = matched_node.text[shared_count:]

                new_parent: Node = Node(text=matched_text, parent=curr_node)
                new_parent.tenant_to_last_access_time = (
                    matched_node.tenant_to_last_access_time.copy()
                )
                # ... 更新 LRU 链表、父子关系 ...
                matched_node.text = remaining_text
                matched_node.parent = new_parent
                new_parent.edge_label_to_child[remaining_text[0]] = matched_node
                curr_node.edge_label_to_child[first_char] = new_parent

                curr_node = new_parent
                i += shared_count
            else:
                # 6. 完全匹配，继续向下
                curr_node = matched_node
                i += shared_count
```

### 2.4 LRU Eviction 机制

#### 后台淘汰循环（`prefix_tree.py:564-607`）

```python
def start_eviction_loop(self, eviction_threshold, eviction_target, interval_secs):
    self._eviction_stop_event.clear()
    with self.lock:
        if self._eviction_thread is None:
            self._eviction_thread = threading.Thread(
                target=self._run_eviction_loop,
                args=(eviction_threshold, eviction_target, interval_secs),
                daemon=True,
            )
            self._eviction_thread.start()

def _run_eviction_loop(self, eviction_threshold, eviction_target, interval_secs):
    while not self._eviction_stop_event.is_set():
        if self._eviction_stop_event.wait(interval_secs):
            break
        with self.lock:
            for tenant, char_count in self.tenant_to_char_count.items():
                if char_count > eviction_threshold:
                    excess = char_count - eviction_target
                    self.evict_tenant_by_lru(tenant, excess)
```

### 2.5 PrefixTreeActor

```python
@ray.remote
class PrefixTreeActor(PrefixTree):
    def getattr(self, attribute: str) -> Any:
        return getattr(self, attribute)
```

- 以 `@ray.remote` 方式运行，独立进程
- 通过 `get_if_exists=True` 确保同 namespace+name 共享唯一实例
- `lifetime="detached"` 使其跨 deployment 生命周期存活

### 2.6 Prompt 文本归一化

#### _extract_text_from_request（`prefix_aware_router.py:134-166`）

```python
def _extract_text_from_request(self, pending_request: PendingRequest) -> str:
    prompt = None
    for arg in pending_request.args:
        valid_input_types = ["messages", "prompt"]
        for valid_input_type in valid_input_types:
            if hasattr(arg, valid_input_type):
                prompt = (
                    arg.prompt if valid_input_type == "prompt" else arg.messages
                )
                break
        if prompt is not None:
            break
    if prompt is None:
        raise ValueError("No request with message or prompt attribute found")
    return self._normalize_prompt_to_string(prompt)
```

#### _normalize_prompt_to_string（`prefix_aware_router.py:184-206`）

```python
def _normalize_prompt_to_string(self, prompt: Any) -> str:
    if isinstance(prompt, str):
        return prompt
    if isinstance(prompt, list):
        return "".join(
            self._coerce_to_text(
                message.get("content") if isinstance(message, dict) else message
            )
            for message in prompt
        )
    return ""
```

支持的格式：
- 纯字符串 → 直接返回
- message 列表 `[{content: "..."}, ...]` → 递归提取 content 并拼接
- content 为多部分列表 `[{text: "..."}, ...]` → 提取 text 字段拼接
- 不支持 thinking/multimodal 部分（TODO）

---

## 3. 路由决策详细流程

### 3.1 choose_replicas 主流程

```
请求到达
│
├─ 1. 获取 PowerOfTwo fallback 候选（随机选 2 个 replica）
│     fallback_replicas = await PowerOfTwoChoicesRequestRouter.choose_replicas(...)
│
├─ 2. 判断请求类型：
│     ├─ 有 multiplexed_model_id → apply_multiplex_routing() 获取候选
│     └─ 无 → apply_locality_routing() 获取候选
│
├─ 3. 如果候选为空，返回 fallback
│
└─ 4. 进入 _prefix_match_best_replicas() 核心逻辑
```

源码位置：`prefix_aware_router.py:337-391`

```python
async def choose_replicas(self, candidate_replicas, pending_request=None):
    # Step 1: 获取 fallback
    fallback_replicas = await PowerOfTwoChoicesRequestRouter.choose_replicas(
        self, candidate_replicas=candidate_replicas, pending_request=pending_request,
    )
    if pending_request is None or not fallback_replicas:
        return fallback_replicas

    # Step 2: 根据请求类型获取候选
    if pending_request.metadata.multiplexed_model_id:
        candidate_replica_ids = self.apply_multiplex_routing(pending_request=pending_request)
    else:
        candidate_replica_ids = self.apply_locality_routing(pending_request=pending_request)
    if not candidate_replica_ids:
        return fallback_replicas

    # Step 3: 映射回 RunningReplica 对象
    candidate_replicas = [
        replica_id_to_replica_map[candidate_replica_id]
        for candidate_replica_id in candidate_replica_ids
    ]

    # Step 4: 前缀匹配选最优
    chosen_replicas = await self._prefix_match_best_replicas(
        pending_request, candidate_replicas
    )
    if chosen_replicas[0]:
        return chosen_replicas
    return fallback_replicas
```

### 3.2 _prefix_match_best_replicas 详细决策树

源码位置：`prefix_aware_router.py:208-290`

```
_prefix_match_best_replicas(pending_request, candidate_replicas)
│
├─ 提取请求 prompt 文本
│   input_text = self._extract_text_from_request(pending_request)
│
├─ 检查负载是否均衡
│   ├─ 从缓存获取各 replica 队列长度
│   │   for r in candidate_replicas:
│   │       queue_len = self._replica_queue_len_cache.get(r.replica_id)
│   ├─ 缓存未命中则 _probe_queue_lens() 主动探测
│   │   for r, queue_len in await self._probe_queue_lens(not_in_cache, 0):
│   └─ 判断：
│       is_imbalanced = (highest_queue_len - lowest_queue_len > threshold)
│
├─ 【负载不均衡】→ 返回空列表 → 回退到 PowerOfTwo fallback
│
└─ 【负载均衡】→ 查询 PrefixTree：
    │
    ├─ ray.get(self._tree_actor.prefix_match.remote(
    │       input_text, candidate_replica_ids_strings
    │   ))
    │   返回 (matched_text, matched_tenant_id_strings)
    │
    ├─ 计算 match_rate = len(matched_text) / len(input_text)
    │
    ├─ match_rate >= 10% (match_rate_threshold)：
    │   → 选择 matched_tenant_id_strings 对应的 replica
    │   （前缀命中率高，路由到有缓存的热点 replica）
    │
    ├─ match_rate < 10%：
    │   → ray.get(self._tree_actor.get_smallest_tenants.remote())
    │   → 选择 KV Cache 占用最小的 replica
    │   （匹配太低不值得利用缓存，选最空闲的 replica）
    │
    └─ 无匹配 → 返回空列表 → 回退到 PowerOfTwo fallback
```

### 3.3 回调机制

#### on_request_routed（`prefix_aware_router.py:394-418`）

请求被路由后，将 prompt 文本和目标 replica ID 插入 PrefixTree：

```python
def on_request_routed(self, pending_request, replica_id, result):
    if pending_request is not None and pending_request.args is not None:
        input_text = self._extract_text_from_request(pending_request)
        if input_text is not None:
            ray.get(
                self._tree_actor.insert.remote(
                    input_text, replica_id.to_full_id_str(), time.time()
                )
            )
```

#### on_replica_actor_died（`prefix_aware_router.py:293-298`）

Replica 死亡时，从 PrefixTree 中移除该 tenant：

```python
def on_replica_actor_died(self, replica_id: ReplicaID):
    super().on_replica_actor_died(replica_id)
    ray.get(self._tree_actor.remove_tenants.remote([replica_id.to_full_id_str()]))
```

#### update_replicas（`prefix_aware_router.py:300-335`）

Replica 集合变更时，注册新 tenant / 移除旧 tenant：

```python
def update_replicas(self, replicas: List[RunningReplica]):
    old_ids = set(self._replica_id_set)
    super().update_replicas(replicas)
    new_ids = set(self._replica_id_set)
    added = new_ids - old_ids
    removed = old_ids - new_ids

    if added:
        added_strings = [rid.to_full_id_str() for rid in added]
        ray.get(self._tree_actor.add_tenants.remote(added_strings, time.time()))
    if removed:
        removed_strings = [rid.to_full_id_str() for rid in removed]
        ray.get(self._tree_actor.remove_tenants.remote(removed_strings))

    # 启动淘汰线程（如果启用）
    if self._do_eviction and not self._eviction_loop_running:
        ray.get(self._tree_actor.start_eviction_loop.remote(
            self._eviction_threshold_chars,
            self._eviction_target_chars,
            self._eviction_interval_secs,
        ))
        self._eviction_loop_running = True
```

---

## 4. Ray Serve LLM 路由架构：Ingress 与 Router 的关系

### 4.1 两种路由架构对比

Ray Serve LLM 存在两种路由架构，对应不同的入口方式：

| | 架构 A：Ingress 路由（HAProxy + LLMRouter） | 架构 B：DeploymentHandle 路由（KESS Gateway） |
|---|---|---|
| **入口** | HAProxy → LLMRouter Ingress | KESS → InferenceGateway |
| **路由执行者** | Ingress 内的 KVAwareRouter | Gateway 进程内的 PrefixCacheAffinityRouter |
| **Router 位置** | 嵌入 Ingress Replica 进程 | 嵌入 DeploymentHandle 所在进程 |
| **请求转发** | HAProxy 直接转发到选中的 Engine Replica | Gateway → Router 选 replica → remote() 调用 |
| **响应路径** | Engine → Client（direct streaming） | Engine → Gateway → Client |
| **匹配方式** | Token 级 KV Cache 重叠（精确） | 字符级前缀匹配（近似） |

### 4.2 Ingress 路由架构（HAProxy + LLMRouter）

Ray 官方文档描述的 **KVAwareRouter** 路由架构：

```
Client
  │
  │ 1. 发送请求
  ▼
┌──────────────────────────────────────────────────────────────────┐
│  HAProxy (前置负载均衡)                                            │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │  - 接收客户端请求                                            │  │
│  │  - 将请求 body 转发到 LLMRouter Ingress Replica 做路由决策     │  │
│  │  - Ingress 返回选中的 replica ID                              │  │
│  │  - HAProxy 将完整请求直接发送到选中的 Engine Replica           │  │
│  └────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────┘
        │                                    │
        │ 2. 转发 prompt (body) 做路由决策     │ 4. 将请求发到选中的 replica
        ▼                                    ▼
┌───────────────────────┐        ┌───────────────────────┐
│  LLMRouter Ingress     │        │  LLMServer Engine      │
│  (Ingress Deployment)  │        │  (Engine Deployment)    │
│                        │        │                        │
│  ┌───────────────────┐ │        │  - 执行 LLM 推理       │
│  │  KVAwareRouter    │ │  3.返回 │  - 报告 KV block 事件  │
│  │  (Selection Svc)  │ │◄───────│  - 报告 prefill/decode │
│  │                   │ │  选中ID│    进度和请求完成       │
│  │  - Tokenize 请求   │ │        │  - Direct streaming    │
│  │  - 查询 KV Cache  │ │        │    响应回客户端          │
│  │    重叠状态        │ │        │                        │
│  │  - 估算 token load │ │        └───────────────────────┘
│  │  - 选择最优 replica│ │
│  └───────────────────┘ │
│                        │
│  每个 Ingress Replica: │
│  - 接收 KV block 事件  │
│  - 同步 token load     │
│  - 最终一致性视图       │
└───────────────────────┘
```

**关键环境变量：**

```bash
export RAY_SERVE_ENABLE_HA_PROXY=1                    # 启用 HAProxy
export RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING=1       # 启用直接流式响应
export RAY_SERVE_INGRESS_REQUEST_ROUTER_FORWARD_BODY=1 # HAProxy 转发 body 给 router
```

### 4.3 Ingress 和 Router 的关系

**Ingress（`LLMRouter`）是容器，Router（`KVAwareRouter`）是核心逻辑。**

```
LLMRouter Ingress Replica 进程
┌────────────────────────────────────────────┐
│                                             │
│  @serve.deployment                          │
│  class LLMRouter:                           │
│    ├── 接收 HAProxy 转发的请求 body           │
│    ├── Tokenize 请求（使用与 Engine 相同的     │
│    │   renderer 和 chat template）           │
│    ├── 调用 Selection Service (NVIDIA Dynamo)│
│    │   └── KVAwareRouter 的评分逻辑          │
│    │       ├── 查询各 replica 的 KV Cache 状态 │
│    │       ├── 估算 prefill load（基于 KV 重叠）│
│    │       ├── 估算 decode load               │
│    │       └── 选择 token load 最低的 replica  │
│    ├── 将 tokenized prompt 预发送到选中的      │
│    │   Engine Replica（通过单独通道）          │
│    └── 返回选中的 replica ID 给 HAProxy       │
│                                             │
│  Ray Serve 负责：                             │
│  ├── Replica 发现和可用性追踪                 │
│  ├── 请求编排（接收、tokenize、选 replica）    │
│  ├── 请求生命周期管理                         │
│  └── KV Cache 事件广播到所有 Ingress Replica   │
│                                             │
└────────────────────────────────────────────┘
```

**关系总结：**

| 维度 | Ingress（LLMRouter） | Router（KVAwareRouter） |
|------|----------------------|------------------------|
| **角色** | 入口容器、请求编排 | 评分决策核心 |
| **运行位置** | Ingress Replica 进程内 | 嵌入 Ingress 进程内 |
| **生命周期** | 随 Ingress Replica 创建/销毁 | 随 Ingress 创建/销毁 |
| **职责** | 接收请求、tokenize、dispatch、管理请求生命周期 | 评分、选择最优 replica |
| **状态** | 维护请求在途状态、接收 KV 事件 | 维护全局 KV Cache 视图 + token load |
| **一致性** | 最终一致：跨 Ingress Replica 同步 token load | 最终一致：KV 视图可能短暂过期 |

### 4.4 KVAwareRouter vs PrefixCacheAffinityRouter 评分对比

| 维度 | KVAwareRouter | PrefixCacheAffinityRouter |
|------|--------------|--------------------------|
| **匹配粒度** | Token 级 KV Block ID | 字符级前缀文本 |
| **数据来源** | Engine 实时上报 KV Block 事件 | Router 维护的近似 PrefixTree |
| **精确度** | 精确：知道哪些 KV Block 在哪个 replica 的 GPU/CPU 上 | 近似：基于文本前缀猜测 KV Cache 可能复用 |
| **额外依赖** | NVIDIA Dynamo Selection Service | 无（纯 Python/Ray） |
| **Tokenizer 开销** | Ingress 需 tokenize | 不需要（直接匹配字符） |
| **CPU 开销** | 较高（tokenize + scoring） | 较低（树遍历） |
| **负载感知** | Token Load（prefill + decode） | 队列长度 + 字符计数 |

### 4.5 Ingress 扩展与一致性

```
Node 1                          Node 2
┌─────────────────────┐        ┌─────────────────────┐
│  Ingress Replica A   │        │  Ingress Replica B   │
│  ┌─────────────────┐│        │  ┌─────────────────┐│
│  │  KVAwareRouter   ││        │  │  KVAwareRouter   ││
│  │  KV View: {...}  │◄───────►│  │  KV View: {...}  ││
│  │  Token Load: {}  │  同步    │  │  Token Load: {}  ││
│  └─────────────────┘│        │  └─────────────────┘│
└─────────────────────┘        └─────────────────────┘
         ▲                               ▲
         │        KV Block Events         │
         │    ┌──────────────────┐       │
         └────│  LLMServer Replicas │──────┘
              │  (广播 KV 事件)     │
              └──────────────────┘
```

- **默认**：每个 Proxy 节点一个 Ingress Replica
- **扩展**：`RAY_SERVE_INGRESS_ROUTER_REPLICAS_PER_NODE` 可调高，通常 2 个/节点足够
- **一致性模型**：**最终一致** — Ingress Replica 可能基于略微过期的 KV 视图做决策
- **PrefixCacheAffinityRouter** 同样是最终一致（PrefixTree 状态有延迟）

### 4.6 Direct Streaming 架构

KVAwareRouter 的一个关键特性是 **Direct Streaming**：Engine Replica 直接将响应流式返回给 Client，不经过 Ingress。

```
Client ──请求──► HAProxy ──转发body──► Ingress (路由决策)
                                         │
                                         │ 返回选中的 replica ID
                                         ▼
                  HAProxy ──完整请求──► Engine Replica
                                           │
                                           │ Direct Streaming
                                           ▼
                  Client ◄─────流式响应──────┘
```

- 减少 Ingress 成为响应传输瓶颈
- 需要 `RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING=1`
- 目前限制：一个 application 只能有一个 model，不支持 LoRA/multiplex

### 4.7 Token Staging 机制

Ingress tokenize 后将 token 预发送到 Engine Replica（通过独立通道），Engine 临时缓存：

```
Ingress                         Engine Replica
  │                                │
  │  1. tokenize 请求               │
  │  2. 预发送 token payload ──────►│  3. 暂存 token (staging)
  │  3. 返回 replica ID             │
  │                                │
  │          HAProxy 转发完整请求     │
  │                                │
  │                          ─────►│  4. 收到 HTTP 请求
  │                                │  5. 查找 staged token
  │                                │     ├─ 找到 → 直接用（省去 tokenize）
  │                                │     └─ 未找到/过期 → 重新 tokenize
```

环境变量控制：
- `RAY_SERVE_LLM_KV_TOKEN_STAGING_TTL_SECS`：暂存过期时间
- `RAY_SERVE_LLM_KV_TOKEN_STAGING_MAX_SIZE`：暂存容量

---

## 5. Ray Serve 架构关系（通用组件）

### 5.1 关键组件关系

```
Ray Cluster
┌─────────────────────────────────────────────────────────────┐
│                                                              │
│  Controller (Ray Actor)                                      │
│    - 管理 deployment 生命周期                                 │
│    - 管理 Proxy/Replica 创建销毁                               │
│    - 广播 replica 状态变更                                    │
│                                                              │
│  Proxy Actor (每节点一个, @ray.remote(num_cpus=0))            │
│    - 名字: SERVE_PROXY_NAME_{node_id}                        │
│    - HTTP Server :8000 / gRPC Server :9000                  │
│    - 不含 Router（仅 HTTP 入口用）                              │
│                                                              │
│  InferenceGateway (Serve Deployment, 多 Replica)             │
│    - @serve.deployment                                       │
│    - KessRegistrar: 自启 gRPC Server, 注册到 KESS            │
│    - _llm_handles: Dict[model_id, DeploymentHandle]         │
│    - 每个 Replica 进程内嵌 Router                              │
│                                                              │
│  LLM Deployment (Serve Deployment)                           │
│    - 多个 Replica (Ray Actor)                                │
│    - 实际执行 LLM 推理                                        │
│                                                              │
│  PrefixTreeActor (detached Ray Actor, per deployment 唯一)    │
│    - 全局共享的前缀树                                          │
│    - lifetime="detached"                                     │
│    - 通过 get_if_exists=True 复用                             │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### 5.2 Proxy Actor 创建位置

Proxy 由 Controller 中的 `proxy_state.py` 管理，按 `node_id` 创建：

```python
# proxy_state.py:815-858
def _start_proxy(self, name, node_id, node_ip_address, ...):
    return self._actor_proxy_wrapper_class(
        logging_config=self.logging_config,
        http_options=http_options,
        grpc_options=grpc_options,
        ...
    )
```

- 每节点一个 `ProxyActor`
- 命名格式：`SERVE_PROXY_NAME_{node_id}`
- 仅处理 HTTP/gRPC 直接入口
- **KESS 场景下不参与路由**（Gateway 直接通过 DeploymentHandle 调用）

### 5.3 Router 的真正创建位置

Router 不在 Proxy 中，而是**在 DeploymentHandle 调用方进程内本地创建**。

#### create_router（`default_impl.py:158-209`）

```python
def create_router(handle_id, deployment_id, handle_options, request_router_class=None):
    from ray.serve.context import _get_global_client
    actor_id = get_current_actor_id()
    node_id, availability_zone = _get_node_id_and_az()
    controller_handle = _get_global_client()._controller

    if handle_options._run_router_in_separate_loop:
        router_wrapper_cls = SingletonThreadRouter
        if handle_options._source == DeploymentHandleSource.REPLICA:
            component = EventLoopMonitor.COMPONENT_REPLICA
        elif handle_options._source == DeploymentHandleSource.PROXY:
            component = EventLoopMonitor.COMPONENT_PROXY
    else:
        router_wrapper_cls = CurrentLoopRouter

    return router_wrapper_cls(
        controller_handle=controller_handle,
        deployment_id=deployment_id,
        handle_id=handle_id,
        self_actor_id=actor_id,
        handle_source=handle_options._source,
        request_router_class=request_router_class,
        ...
    )
```

#### DeploymentHandle._init（`handle.py:140-164`）

```python
def _init(self, **kwargs):
    if self._router is not None:
        raise RuntimeError("Handle has already been initialized")
    init_options = create_init_handle_options(**kwargs)
    self._router = self._create_router(
        handle_id=self.handle_id,
        deployment_id=self.deployment_id,
        handle_options=init_options,
    )
    self.init_options = init_options
```

### 5.4 Router 复用机制

**同一个 Gateway Replica 进程内，Router 是复用的：**

```
Gateway Replica 进程 (启动后长期存活)
│
├─ __init__():
│    self._llm_handles["model-A"] = DeploymentHandle("LLM")  ← 创建一次
│
├─ 第1次请求: handle.remote(req1)
│    └─ handle._init()  → 创建 self._router = SingletonThreadRouter
│       └─ _router 复用给后续所有请求
│
├─ 第2次请求: handle.remote(req2)
│    └─ _router 已存在，直接用
│
├─ 第N次请求: handle.remote(reqN)
│    └─ 同一个 _router
```

**不同 Gateway Replica 之间，Router 是各自独立的。**

---

## 6. KESS 场景下的请求路径

### 6.1 完整请求链路

```
Client (gRPC)
    │
    │  KESS 服务发现
    ▼
┌─────────────────────────────────────────────────────────┐
│  InferenceGateway Replica (Ray Actor 进程)                │
│  ┌─────────────────────────────────────────────────────┐ │
│  │  KessRegistrar: 自启 gRPC Server, 注册到 KESS        │ │
│  │                                                     │ │
│  │  Chat(request, context):                             │ │
│  │    └─ _dispatch_llm("chat", request, context)       │ │
│  │       └─ _resolve_llm_handle(request)               │ │
│  │          → 返回 DeploymentHandle                    │ │
│  │       └─ _call_llm_handle(method, handle, request)  │ │
│  │          └─ _invoke_llm_handle(method, handle, req)  │ │
│  │             └─ handle.options(stream=True).chat.remote(llm_request)
│  │                │                                     │ │
│  │                │  DeploymentHandle.remote()          │ │
│  │                │  Router 在本进程内（SingletonThread）  │ │
│  │                ▼                                     │ │
│  │  ┌─────────────────────────────────────┐            │ │
│  │  │  SingletonThreadRouter               │            │ │
│  │  │  └─ AsyncioRouter                    │            │ │
│  │  │     └─ PrefixCacheAffinityRouter     │            │ │
│  │  │        └─ _tree_actor (Ray Actor IPC) │            │ │
│  │  └─────────────────────────────────────┘            │ │
│  │                │                                     │ │
│  │                │  选择 replica 后                     │ │
│  │                ▼                                     │ │
│  │  ┌──────────────┐  ┌──────────────┐                 │ │
│  │  │ LLM Replica1 │  │ LLM Replica2 │                 │ │
│  │  │ (Ray Actor)  │  │ (Ray Actor)  │                 │ │
│  │  └──────────────┘  └──────────────┘                 │ │
│  └─────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────┘
```

### 6.2 Gateway 核心调度逻辑

源码位置：`python/ray/serve/inference/gateway.py`（git commit `8a04ca9cb3`）

```python
@serve.deployment
class InferenceGateway:
    def __init__(self, config, llm_handles=None):
        self._llm_handles: Dict[str, Any] = llm_handles or {}
        self._kess_registrar = KessRegistrar(...)
        self._kess_registrar.start(self)  # 将自己注册为 gRPC servicer

    def Chat(self, request, context):
        return self._dispatch_llm("chat", request, context)

    def _dispatch_llm(self, method_name, request, context):
        llm_handle, resolved_model_id = self._resolve_llm_handle(request)
        future = asyncio.run_coroutine_threadsafe(
            self._call_llm_handle(method_name, llm_handle, request),
            self._loop,
        )
        result = future.result(timeout=timeout)
        return convert_response(method_name, result)

    async def _invoke_llm_handle(self, method_name, handle, request, stream):
        llm_request = convert_request(method_name, request, stream=stream)
        handle_method = getattr(handle.options(stream=True), method_name)
        gen = handle_method.remote(llm_request)
        async for chunk in gen:
            yield chunk

    def _resolve_llm_handle(self, request):
        model_id = getattr(request, "model", None) or ""
        if model_id in self._llm_handles:
            return self._llm_handles[model_id], model_id
        raise ValueError(f"model '{model_id}' not found")
```

### 6.3 KESS 下 Proxy 的角色

- Proxy 每节点都会启动（Ray Serve 框架行为，无法关闭）
- KESS 场景下 **Proxy 不参与路由**
- 请求路径：`KESS → Gateway → DeploymentHandle.remote() → Router（Gateway 进程内）→ Replica`
- Proxy 仅在 HTTP 直接入口时使用

---

## 7. PrefixTreeActor 性能分析与 QPS 估算

### 7.1 QPS 估算依据

**200~1000 QPS 是基于架构特征的粗略估算，并非实际 benchmark。**

| 瓶颈点 | 位置 | 影响 |
|--------|------|------|
| **`ray.get()` 同步阻塞** | router.py:263, 270, 296, 320, 324, 414 | Router 端事件循环被阻塞，等 Actor 返回期间无法处理其他请求 |
| **全局 RLock** | prefix_tree.py:88 | `prefix_match`（读）和 `insert`（写）互斥，所有操作串行 |
| **长 prompt 全量遍历** | router.py:225, prefix_tree.py:409 | LLM prompt 可达数万字符，`os.path.commonprefix` 逐字符比较开销大 |
| **单 Actor 实例** | prefix_aware_router.py:119 | 所有 replica 的路由请求打到同一个 actor |
| **Node 的 per-tenant dict** | prefix_tree.py:44-49 | 每个 Node 维护 3 个 per-tenant dict，高 tenant 数下内存和查找开销大 |

### 7.2 估算推导

- Ray Actor 单线程串行：每次请求需 `ray.get()` 跨进程 RPC，单次 ~0.5-2ms
- `prefix_match` + `insert` 在 Python + RLock 下 ~2-5ms
- 粗算：单次完整路由 ~3-7ms → 140-330 QPS
- 短 prompt 时可能 ~1-2ms → 500-1000 QPS
- 但 **Router 侧 `ray.get()` 阻塞事件循环** 是更大的瓶颈

### 7.3 详细优化措施

#### P0: `ray.get()` → `await`

```python
# 当前（阻塞事件循环）:
(matched_text, matched_tenant_id_strings) = ray.get(
    self._tree_actor.prefix_match.remote(input_text, candidate_replica_ids_strings)
)

# 优化后（非阻塞）:
(matched_text, matched_tenant_id_strings) = await self._tree_actor.prefix_match.remote(
    input_text, candidate_replica_ids_strings
)
```

Router 是 async 的，用 `ray.get()` 会阻塞整个事件循环。改 `await` 后 Router 可并发等待多个 Actor 调用，单路由请求延迟不变但吞吐可提升数倍。

#### P0: insert fire-and-forget

```python
# 当前:
ray.get(self._tree_actor.insert.remote(input_text, replica_id_str, time.time()))

# 优化: fire-and-forget（insert 不影响当前路由决策）
self._tree_actor.insert.remote(input_text, replica_id_str, time.time())
```

#### P1: 读写锁替代全局 RLock

`prefix_match` 是纯读操作，`insert` 是写操作。用 `ReadWriteLock` 后多个 `prefix_match` 可并发执行，仅 `insert` 需要排他锁。读多写少场景下 QPS 可提升 3~5 倍。

#### P1: 截断输入文本

KV Cache 的 prefix 复用主要在前 N 个 token，全文匹配收益递减。可只取前 `max_prefix_chars`（如 1024~4096 字符）做匹配。

#### P2: Token 级别匹配替代字符级

当前用 `os.path.commonprefix` 做字符级比较，但 LLM 的 KV Cache 是按 token 对齐的。改为 tokenizer 切分后按 token 序列匹配，既更精确又减少比较次数。

#### P2: Actor max_concurrency

```python
self._tree_actor = PrefixTreeActor.options(
    max_concurrency=8,
).remote()
```

需配合读写锁使用。

---

## 8. SGLang Rust 实现对比

### 8.1 SGLang gateway 概览

源码位置：`/Users/franke/Desktop/git/sglang/sgl-model-gateway/src/policies/`

核心文件：
- `tree.rs` — Rust Radix Tree 实现（DashMap 无锁并发）
- `cache_aware.rs` — Cache-Aware 路由策略
- `benches/tree_benchmark.rs` — 性能基准测试

### 8.2 核心对比

| | KRay PrefixTreeActor | SGLang Tree (Rust) |
|---|---|---|
| **语言** | Python | Rust |
| **并发** | 单线程 + 全局 RLock | DashMap 分片锁（无全局锁） |
| **锁粒度** | 整棵树一把锁 | Node 级分片：children 用 `DashMap<char, Node>`（root 32 shard，普通 8 shard），tenant 用 `DashMap<TenantId, u64>` |
| **时间戳** | `time.time()` 系统调用 | `AtomicU64` epoch counter，`fetch_add` 无锁递增 |
| **字符串操作** | `os.path.commonprefix` | SIMD 友好的 byte 比较 + ASCII fast path |
| **字符计数** | 每次调 `len()`/`chars().count()` O(n) | `NodeText` 缓存 `char_count`，O(1) |
| **Tenant ID** | Python str | `Arc<str>` 引用计数，零拷贝克隆 |
| **tenant 查找** | 遍历 dict | `last_tenant: parking_lot::RwLock<Option<TenantId>>` O(1) 缓存 |
| **时间戳更新** | 每次 match 都更新 | 概率更新：1/8（`epoch & 0x7 == 0`），减少 DashMap 写竞争 |
| **PD 隔离** | 无 | `pool::model` 复合 key，prefill/decode 树独立 |
| **Mesh 同步** | 无 | 支持跨节点树状态同步（`smg_mesh::tree_ops::TreeOperation`） |

### 8.3 SGLang tree.rs 关键实现

```rust
type NodeRef = Arc<Node>;

const ROOT_SHARD_COUNT: usize = 32;  // root 节点 32 分片
const NODE_SHARD_COUNT: usize = 8;    // 普通节点 8 分片

pub type TenantId = Arc<str>;         // 零拷贝 tenant ID

// 快速 char 哈希
struct CharHasher(u64);
impl Hasher for CharHasher {
    fn write_u32(&mut self, i: u32) {
        self.0 = (i as u64).wrapping_mul(0x9E3779B97F4A7C15);
    }
}

// 前缀匹配结果
pub struct PrefixMatchResult {
    pub tenant: TenantId,
    pub matched_char_count: usize,
    pub input_char_count: usize,
}
```

### 8.4 SGLang 预期性能

从 benchmark 框架（`tree_benchmark.rs`）：
- 配置 **64 线程并发**、**10K 树条目**、**500 workers** 测试
- Rust + DashMap 无锁分片架构下，预期单线程 **100K+ ops/sec**
- 64 线程并发可达 **数百万 ops/sec**
- 比 Python 实现快 **100-1000 倍**

### 8.5 SGLang Mesh 同步机制

```rust
use smg_mesh::{tree_ops::TreeOperation, OptionalMeshSyncManager};
```

SGLang 支持跨 Gateway 节点同步树操作：
- 每个 Gateway 节点本地有完整树副本
- 通过 gossip 协议保持最终一致
- TreeOperation 枚举：insert、remove_tenant、evict 等操作
- OptionalMeshSyncManager 可选启用

---

## 9. 多 Gateway 场景分析

### 9.1 当前架构：共享 PrefixTreeActor

```
Gateway Replica 1                Gateway Replica 2
    │                                │
    └─ Router A                      └─ Router B
       └─ prefix_match() ──┐            └─ prefix_match() ──┐
       └─ insert()     ──┤                └─ insert()     ──┤
                           │                                    │
                           ▼                                    ▼
                    ┌─────────────────────────────────────────────┐
                    │     PrefixTreeActor (共享，全局唯一)          │
                    │  - 知道 Replica1 处理过 "你好请"              │
                    │  - Replica2 请求来时也能匹配到                 │
                    └─────────────────────────────────────────────┘
```

**优点**：所有 Gateway Replica 共享同一个树，前缀匹配有效率最高。

### 9.2 内嵌 Rust Tree：独立树

```
Gateway Replica 1                Gateway Replica 2
    │                                │
    └─ Router A                      └─ Router B
       └─ Rust Tree A  (独立)           └─ Rust Tree B  (独立)
       │  只知道本进程路由过的请求          │  只知道本进程路由过的请求
       │  不知道 Replica2 的历史           │  不知道 Replica1 的历史
```

**问题**：KESS 随机分发请求，相似前缀的请求只有 1/N 概率落到同一个 Gateway Replica。

### 9.3 量化影响

假设 4 个 Gateway Replica，某 prompt 前缀出现 100 次：

| | 共享 Tree（当前） | 独立 Tree（内嵌） |
|---|---|---|
| 第1次请求 | miss，insert | miss，insert |
| 第2-100次 | 全部 hit | 只有落到同一 Replica 的 ~25 次 hit |
| 命中率 | ~99% | ~25% |

**效果退化为原来的 1/N**，基本失去了前缀路由的意义。

### 9.4 为什么 Ray 用 Actor 而不是内嵌

**Python GIL 是根本原因**：

```
Router 进程 (asyncio event loop)
  │
  ├─ _probe_queue_lens()     # async, 需要事件循环
  ├─ choose_replicas()       # async, 需要事件循环
  ├─ on_request_routed()     # 回调
  │
  └─ 如果内嵌 Python PrefixTree：
      ├─ insert()  → GIL 锁 + 全局 RLock → 阻塞事件循环！
      ├─ prefix_match() → 同上
      └─ 长文本遍历时，整个 Router 进程卡死
```

如果用 Rust/PyO3 内嵌，GIL 问题可解决（Rust 释放 GIL），但**多 Gateway 独立树**问题无法解决。

---

## 10. 优化方案推荐

### 10.1 方案对比

| 方案 | 延迟 | 共享性 | 复杂度 | 多 Gateway 兼容 | 推荐度 |
|------|------|--------|--------|----------------|--------|
| **当前 Ray Actor** | ~1-5ms | 全局共享 | 低 | ✅ | 基线 |
| **内嵌 Rust Tree** | ~1-10μs | 不共享 | 低 | ❌ 1/N 有效率 | 多 Replica 场景不适用 |
| **Rust Tree + 独立进程 Unix Socket** | ~50-200μs | 全局共享 | 中 | ✅ | 可行但提升有限 |
| **Rust PrefixTreeActor（推荐）** | ~0.5-1ms | 全局共享 | 中 | ✅ | ✅ 最佳平衡 |
| **内嵌 Rust Tree + Gossip 同步** | ~1-10μs | 最终一致 | 高 | ✅ | ⚠️ 复杂，SGLang 方案 |

### 10.2 推荐方案：Rust PrefixTreeActor

保持当前 Actor 架构（全局共享），用 Rust 重写 Tree 内核：

#### 改动内容

1. **Tree 内核从 Python 换成 Rust**（PyO3）
   - DashMap 替代 dict + RLock → 无锁并发读
   - AtomicU64 替代 time.time() → 无锁时间戳
   - 缓存 char_count → O(1) 字符计数

2. **Actor `max_concurrency` 提升**
   - 多个 `prefix_match` 可并行处理
   - `insert` 与 `prefix_match` 不再全局互斥

3. **Router 侧 `ray.get()` → `await`**（最小改动，最大收益）
   - 消除事件循环阻塞
   - 每次路由节省 ~1-5ms 的阻塞等待

4. **`insert` 改 fire-and-forget**
   - `on_request_routed` 中的 insert 不影响当前路由决策
   - 无需 `ray.get()` 同步等待结果

#### 预期效果

- **共享性不变** — 全局唯一树
- **单次延迟** — 从 Python ~2-5ms 降到 Rust Actor ~0.5-1ms（含 IPC）
- **吞吐** — 从串行到 DashMap 并发读，提升 5-10x
- **改动最小** — 只换 Tree 内核，不改变架构

### 10.3 长期方案：内嵌 Rust Tree + Mesh 同步

如果需要极致性能（μs 级延迟），参考 SGLang 方案：

1. 每个 Gateway Replica 内嵌 Rust Tree
2. 通过 Mesh gossip 协议同步树操作
3. 最终一致性：各节点树状态在秒级收敛
4. 实现成本高，但延迟最低

### 10.4 优化优先级

| 优先级 | 优化项 | 预期收益 | 改动量 |
|--------|--------|----------|--------|
| P0 | `ray.get()` → `await` | 吞吐提升 3~5x | 小 |
| P0 | `insert` fire-and-forget | 减少 RTT 阻塞 | 极小 |
| P1 | Rust 重写 Tree 内核 (PyO3) | Tree 操作从 ms 降到 μs | 中 |
| P1 | Actor max_concurrency + DashMap | 读并发提升 3~5x | 中 |
| P2 | 截断输入文本 | 长文本场景延迟降 50%+ | 小 |
| P2 | Token 级别匹配 | 精度+性能双提升 | 大 |
| P3 | Mesh gossip 同步 | 消除 IPC，μs 级延迟 | 大 |

---

## 附录：关键代码文件索引

| 文件 | 说明 |
|------|------|
| `python/ray/llm/_internal/serve/routing_policies/prefix_aware/prefix_aware_router.py` | PrefixCacheAffinityRouter 主逻辑 |
| `python/ray/llm/_internal/serve/routing_policies/prefix_aware/prefix_tree.py` | PrefixTree + PrefixTreeActor |
| `python/ray/serve/handle.py` | DeploymentHandle 定义 |
| `python/ray/serve/_private/default_impl.py` | `create_router()` 工厂函数 |
| `python/ray/serve/_private/router.py` | AsyncioRouter / SingletonThreadRouter |
| `python/ray/serve/_private/proxy_state.py` | Proxy Actor 创建管理 |
| `python/ray/serve/_private/request_router/request_router.py` | RequestRouter 基类 |
| `python/ray/serve/_private/request_router/pow_2_router.py` | PowerOfTwoChoicesRequestRouter |
| `python/ray/serve/inference/gateway.py` | InferenceGateway (git: 8a04ca9cb3) |
| `python/ray/serve/llm/ingress.py` | OpenAiIngress (Ingress Deployment) |
| `python/ray/llm/_internal/serve/core/ingress/ingress.py` | OpenAiIngress 实现 |
| `/Users/franke/Desktop/git/sglang/sgl-model-gateway/src/policies/tree.rs` | SGLang Rust Radix Tree |
| `/Users/franke/Desktop/git/sglang/sgl-model-gateway/src/policies/cache_aware.rs` | SGLang Cache-Aware Router |
| `/Users/franke/Desktop/git/sglang/sgl-model-gateway/benches/tree_benchmark.rs` | SGLang Tree 性能基准测试 |
| `/Users/franke/Desktop/git/ray/python/ray/llm/_internal/serve/routing_policies/kv_aware/kv_aware_router.py` | KVAwareRouter 路由入口 |
| `/Users/franke/Desktop/git/ray/python/ray/llm/_internal/serve/routing_policies/kv_aware/kv_token_tracker.py` | KVTokenTracker 评分 + 生命周期 |
| `/Users/franke/Desktop/git/ray/python/ray/llm/_internal/serve/routing_policies/kv_aware/vllm/kv_events.py` | vLLM KV 事件配置 |
| `/Users/franke/Desktop/git/ray/python/ray/llm/_internal/serve/routing_policies/kv_aware/vllm/tokenizer.py` | vLLM Tokenizer |
| `/Users/franke/Desktop/git/ray/python/ray/llm/_internal/serve/routing_policies/kv_aware/vllm/token_tracking.py` | vLLM 生命周期 Hook |
| `/Users/franke/Desktop/git/ray/python/ray/llm/_internal/serve/routing_policies/kv_aware/vllm/prompt_token_forwarding.py` | vLLM Prompt Token 注入 |
| `/Users/franke/Desktop/git/ray/python/ray/llm/_internal/serve/core/ingress/router.py` | LLMRouter Ingress (Dynamo 集成) |
| `/Users/franke/Desktop/git/ray/python/ray/llm/_internal/serve/core/ingress/builder.py` | Ingress 构建器 |
| `/Users/franke/Desktop/git/sglang/python/sglang/srt/mem_cache/events.py` | SGLang KVCacheEventRecorder |
| `/Users/franke/Desktop/git/sglang/python/sglang/srt/managers/scheduler_components/kv_events_publisher.py` | SGLang KV 事件发布 |
| `/Users/franke/Desktop/git/sglang/python/sglang/srt/disaggregation/kv_events.py` | SGLang KV 事件定义 + ZmqEventPublisher |
| `/Users/franke/Desktop/git/sglang/experimental/sgl-router/src/policies/kv_events/` | SGLang Rust KV 事件订阅+索引 |
| `/Users/franke/Desktop/git/sglang/experimental/sgl-router/src/policies/cache_aware.rs` | SGLang Cache-Aware Router |

---

## 11. NVIDIA Dynamo Selection Service KV Cache 路由详解

### 11.1 vLLM KV Block 事件的产生

vLLM 在推理调度过程中原生产生 KV Cache 事件，定义在 `vllm/distributed/kv_events.py` 中：

#### 事件类型

| 事件 | 触发时机 | 关键字段 |
|------|----------|----------|
| `BlockStored` | KV block 写入 GPU/CPU 缓存 | `block_hashes`, `parent_block_hash`, `token_ids`, `block_size`, `lora_name`, `medium` (GPU/CPU/STORAGE), `locality` (LOCAL/REMOTE), `session_id` |
| `BlockRemoved` | KV block 被淘汰 | `block_hashes`, `medium`, `locality` |
| `AllBlocksCleared` | 整个 KV Cache 清空 | 无额外字段 |

#### 事件产生流程

```
vLLM Scheduler（每次调度迭代）
    │
    ├─ 分配 KV block → 生成 BlockStored 事件
    │   包含: block_hash (基于 token_ids 计算)
    │         parent_block_hash (父 block)
    │         token_ids (该 block 包含的 token)
    │         medium = GPU / CPU / STORAGE
    │         locality = LOCAL / REMOTE
    │
    ├─ 淘汰 KV block → 生成 BlockRemoved 事件
    │
    └─ 清空缓存 → 生成 AllBlocksCleared 事件
```

#### 事件聚合（Tensor Parallel 共识）

```
Worker 0: [BlockStored(A), BlockStored(B), BlockStored(C)]
Worker 1: [BlockStored(A), BlockStored(B), BlockStored(D)]
Worker 2: [BlockStored(A), BlockStored(B), BlockStored(C)]
                    │
                    ▼  KVEventAggregator (num_workers=3)
            只有所有 worker 都产生的事件才通过：
            [BlockStored(A), BlockStored(B)]
            （C 和 D 不是共识事件，被丢弃）
```

#### 事件发布（ZMQ）

```
ZmqEventPublisher（每个 DP rank 一个实例）
    │
    ├─ 后台 daemon 线程
    │   ├─ 从 bounded Queue 取 EventBatch
    │   ├─ 分配单调递增 sequence number
    │   ├─ msgpack 编码
    │   └─ ZMQ PUB socket 发送: (topic_bytes, seq_bytes, payload)
    │
    ├─ Replay Buffer (deque, maxlen=buffer_steps)
    │   └─ 存储历史事件用于补发
    │
    └─ ROUTER socket
        └─ Subscriber 缺失事件时发送 start_seq
           Publisher 从 replay buffer 补发
```

### 11.2 Dynamo Selection Service 评分算法

#### Token Load 估算

```
请求到达 Ingress
    │
    ├─ ① Tokenize 请求 → 得到 token_ids
    │
    ├─ ② 对每个候选 Replica 估算 Token Load:
    │
    │   ┌─────────────────────────────────────────────────┐
    │   │  Replica R 的 Token Load 估算:                    │
    │   │                                                  │
    │   │  a. 查询全局 KV Cache 视图:                       │
    │   │     输入: request_token_ids                      │
    │   │     查找: 哪些 block_hash 在 Replica R 的        │
    │   │           KV Cache 视图中存在？                    │
    │   │           （通过 block_hash 匹配 token_ids）      │
    │   │                                                  │
    │   │  b. 计算 Prefill Load:                           │
    │   │     cached_tokens = 匹配到的 token 数             │
    │   │     uncached_tokens = len(request_tokens) -       │
    │   │                        cached_tokens              │
    │   │                                                  │
    │   │     GPU KV block: 全额抵扣 (credit=1.0)          │
    │   │     CPU offloaded: 部分抵扣 (credit<1.0)          │
    │   │       （因为 CPU block 需要加载回 GPU）           │
    │   │                                                  │
    │   │     prefill_load = uncached_tokens +              │
    │   │                     active_prefill_work_on_R      │
    │   │                                                  │
    │   │  c. 计算 Decode Load:                            │
    │   │     decode_load = Σ (活跃 decode 请求的           │
    │   │                     KV block 权重 ×               │
    │   │                     估计剩余输出比例)             │
    │   │     估计剩余输出: 基于 max_tokens -               │
    │   │                     已生成 tokens                 │
    │   │                                                  │
    │   │  d. Token Load = α × prefill_load +              │
    │   │                   β × decode_load                │
    │   │     α = DYN_ROUTER_PREFILL_LOAD_SCALE (默认1.0)  │
    │   │     β = DYN_ROUTER_DECODE_LOAD_SCALE (默认1.0)  │
    │   └─────────────────────────────────────────────────┘
    │
    └─ ③ 选择 Token Load 最低的 Replica
```

#### 与 PrefixCacheAffinityRouter 的关键区别

| | KVAwareRouter (Dynamo) | PrefixCacheAffinityRouter |
|---|---|---|
| **匹配方式** | Token ID → Block Hash 精确匹配 | 文本字符前缀近似匹配 |
| **数据来源** | vLLM 实时上报的 KV block 事件 | Router 自己维护的近似树 |
| **精度** | 知道具体哪些 block 在哪个 replica 的 GPU/CPU 上 | 只知道文本前缀相似 |
| **GPU vs CPU** | 区分：GPU 全额抵扣，CPU 部分抵扣 | 不区分 |
| **Decode 负载** | 精确估算（基于 max_tokens） | 只看队列长度 |
| **实时性** | 事件驱动，~ms 级延迟 | 请求驱动，每次 insert 更新 |
| **开销** | 需要 tokenize + ZMQ 事件传输 | 无额外开销 |

### 11.3 Direct Streaming 技术实现

#### 普通 Ingress 架构（无 Direct Streaming）

```
请求路径（3跳）：
Client ──①──► Ingress ──②──► Engine

响应路径（2跳）：
Engine ──③──► Ingress ──④──► Client
         ▲          ▲
         │          │
         └── Ingress 必须逐 chunk 接收再转发
```

Ingress 是流式中继：Engine yield 一个 chunk → Ingress 收到 → Ingress yield 给 Client。**每个 chunk 都要经过 Ingress 进程**，Ingress 成为吞吐瓶颈。

#### HAProxy + Direct Streaming 架构

```
请求路径（3跳，没变）：
Client ──①──► HAProxy ──②──► Engine
              │
              └── ③ 路由决策时额外问 Ingress（但这是控制面，不是数据面）

响应路径（1跳）：
Engine ──④──► Client       ← Direct Streaming！
         │
         └── 流式响应直接回 Client，不经过 HAProxy 也不经过 Ingress
```

**"Direct Streaming"含义**：Engine 的流式响应**直接**发回 Client，中间不经 Ingress/Gateway 中继。

#### 量化差异

假设 LLM 生成 1000 tokens，每个 token 一个 chunk：

| | 普通 Ingress | Direct Streaming |
|---|---|---|
| **chunk 跳数** | 1000 × 2 = 2000 跳 | 1000 × 1 = 1000 跳 |
| **Ingress 负载** | 接收+转发 1000 chunk | 0 chunk（只做路由决策） |
| **TTFT** | Engine→Ingress→Client 多一跳延迟 | Engine→Client 直达 |
| **吞吐瓶颈** | Ingress 进程带宽/CPU | 无（Engine 直出） |

#### Direct Streaming 的限制

**单 model 限制**：HAProxy 需要知道请求该发给哪个 Engine deployment。多 model 时 Ingress 还需要做 model→deployment 映射，但 Direct Streaming 下 HAProxy 只拿到了 replica ID，无法区分不同 model 的 deployment。

**无 LoRA/Multiplex 限制**：LoRA multiplex 路由需要在 Ingress 层知道 adapter ID 并路由到加载了该 adapter 的 replica。Direct Streaming 下 HAProxy 不理解 adapter 语义，只做简单的 replica ID 路由。

### 11.4 Dynamo 和 vLLm 的关系

**Dynamo 不是 vLLM 的一部分，vLLM 也不是 Dynamo 的一部分。它们是互补的独立项目。**

```
┌─────────────────────────────────────────────┐
│  NVIDIA Dynamo (编排层)                       │
│  - Rust 核心 + Python 扩展                    │
│  - 负责: 路由、KV 感知调度、                   │
│    Prefill/Decode 分离、多节点协调             │
│  - 支持: vLLM, SGLang, TensorRT-LLM          │
│                                             │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐    │
│  │  vLLM   │  │ SGLang  │  │TRT-LLM  │    │
│  │(推理引擎)│  │(推理引擎)│  │(推理引擎)│    │
│  └─────────┘  └─────────┘  └─────────┘    │
│      ↑ 插件化接入                             │
└─────────────────────────────────────────────┘
```

- **vLLM** 提供 `ZmqEventPublisher` 接口，将 KV block 事件以 ZMQ PUB/SUB 方式发布出去
- **Dynamo** 订阅这些事件，维护全局 KV Cache 视图，做路由决策
- **Ray Serve** 作为中间桥梁，将 vLLM 的事件传输到 Ingress 进程内运行的 Dynamo Selection Service

---

## 12. Ray Serve KVAwareRouter 完整代码实现

源码位置：`/Users/franke/Desktop/git/ray/python/ray/llm/_internal/serve/routing_policies/kv_aware/`

### 12.1 代码目录结构

```
kv_aware/
├── __init__.py
├── constants.py               # 常量定义（端口、超时等）
├── kv_aware_router.py          # KVAwareRouter 路由入口
├── kv_token_tracker.py         # KVTokenTracker 评分 + 生命周期跟踪
├── token_channel.py            # Token 传输通道（engine 无关）
├── utils.py                    # 配置入口
└── vllm/                       ← 唯一的 engine 适配层
    ├── __init__.py
    ├── kv_events.py            # 配置 vLLM ZMQ 事件
    ├── tokenizer.py            # vLLM tokenizer（import vllm.*）
    ├── prompt_token_forwarding.py  # vLLM prompt token 注入
    └── token_tracking.py       # vLLM 生命周期 hook（import vllm.*）
```

### 12.2 KVAwareRouter 路由入口

源码位置：`kv_aware/kv_aware_router.py:34-100`

```python
class KVAwareRouter(RequestRouter):
    """Routes each request to the candidate that best balances expected KV-cache
    overlap against the worker's current prefill/decode load.

    Scoring is delegated to the ``KVTokenTracker`` (which owns the
    Dynamo selection service and the global KV index) built by the LLMRouter in
    this same ingress process; this per-handle router stays thin and simply maps
    candidate replicas to/from Dynamo worker ids.
    """

    def initialize_state(self):
        self._kv_token_tracker = get_kv_token_tracker()
        if self._kv_token_tracker is None:
            logger.warning(
                "No KVTokenTracker in this process (%s); KVAwareRouter "
                "degrades to load-balanced selection here.",
                self._deployment_id,
            )

    async def choose_replicas(self, candidate_replicas, pending_request=None):
        token_ids = (
            pending_request.kwargs.get(REQUEST_TOKEN_IDS_KWARG)
            if pending_request is not None
            else None
        )
        # No token ids to score on, or no tracker in this process:
        # load-balance.
        if not token_ids or self._kv_token_tracker is None:
            return [[random.choice(candidate_replicas)]] if candidate_replicas else []

        worker_id_to_replica = {
            get_worker_id(replica.replica_id.unique_id): replica
            for replica in candidate_replicas
        }
        # 调用 Dynamo SelectionService 评分 + 原子预留
        selection = await self._kv_token_tracker.select_worker(
            pending_request.metadata.request_id,
            token_ids,
            list(worker_id_to_replica),
            _get_expected_output_tokens(pending_request),
        )
        return [[worker_id_to_replica[selection["worker_id"]]]]
```

### 12.3 KVTokenTracker 核心实现

源码位置：`kv_aware/kv_token_tracker.py:162-718`

```python
class KVTokenTracker:
    """Tracks per-replica KV-cache overlap and token load inside the LLMRouter
    ingress replica.

    1. Owns a router-local Dynamo ``SelectionService``.
    2. Tracks live replicas via a ``LongPollClient`` on ``DEPLOYMENT_TARGETS``,
       mapping each running replica to a Dynamo worker id.
    3. The ``SelectionService`` maintains a global KV index radix tree, fed by
       every replica's KV events; each node records which workers hold that KV block.
    4. Scoring (``select_worker``) atomically ranks candidate workers by
       KV-cache overlap and current token load, reserves the chosen worker, and
       records local lifecycle state.
    """

    def __init__(self, indexer_threads, serve_deployment_id, ingress_replica_rank):
        self._block_size: Optional[int] = None
        self._replica_id_by_worker: Dict[int, str] = {}
        self._requests: "OrderedDict[str, RequestLifecycle]" = OrderedDict()
        self._request_ids_by_worker: Dict[int, Set[str]] = {}
        self._reservation_forwarder: Optional[ReservationBroadcastForwarder] = None
        self._create_selection_service()
        self._start_replica_tracking()
```

#### 创建 SelectionService

```python
def _create_selection_service(self):
    try:
        from dynamo.llm import SelectionService
    except ImportError:
        self._svc = None
        logger.warning("ai-dynamo is not installed; KV-aware routing requires ai-dynamo.")
        return

    self._svc = SelectionService(indexer_threads=self._indexer_threads)
```

**Dynamo SelectionService 是 Rust PyO3 对象**，在 Ingress 进程内运行。它内部：
- 维护全局 KV HashTree 索引
- 为每个 worker spawn ZMQ SUB 连接接收 KV 事件
- 提供 `select_and_reserve()` 原子评分 + 预留操作

#### 发现 Replica 并注册 ZMQ 监听

```python
def _on_deployment_targets(self, target_info: DeploymentTargetInfo):
    """LongPoll listener: reconcile tracked workers against the running-replica
    snapshot. Each replica advertises its KV-events endpoint via ``record_routing_stats``.
    """
    members: Dict[int, tuple] = {}
    for replica in target_info.running_replicas:
        worker_id = get_worker_id(replica.replica_id.unique_id)
        kv_event_metadata = replica.routing_stats.get("kv_event_metadata")
        if kv_event_metadata is not None:
            members[worker_id] = (
                replica.replica_id.to_full_id_str(),
                kv_event_metadata,
            )

    registered = set(self._replica_id_by_worker)
    added = members.keys() - registered
    removed = registered - members.keys()

    for worker_id in removed:
        self.remove_worker(worker_id)
    for worker_id in added:
        replica_id, kv_event_metadata = members[worker_id]
        self._register_block_size(kv_event_metadata["block_size"], replica_id)
        self._schedule(
            self._upsert_worker(worker_id, replica_id, kv_event_metadata)
        )
```

#### 注册 Worker 到 SelectionService

```python
async def _upsert_worker(self, worker_id, replica_id, kv_event_metadata):
    """Register a replica's KV-event endpoint with the selection service.

    The selection service spawns a connect-out ZMQ listener to the
    replica's ``endpoint`` and indexes its live KV events.
    """
    if self._svc is None:
        return
    dp_rank = kv_event_metadata["dp_rank"]
    await self._svc.upsert_worker({
        "worker_id": worker_id,
        "model_name": _MODEL_NAME,
        "tenant_id": _TENANT_ID,
        "endpoint": f"ray://{replica_id}",
        "block_size": self._block_size,
        "max_num_batched_tokens": kv_event_metadata["max_num_batched_tokens"],
        "data_parallel_start_rank": dp_rank,
        "data_parallel_size": 1,
        "kv_events_endpoints": {dp_rank: kv_event_metadata["endpoint"]},
        "replay_endpoint": kv_event_metadata.get("replay_endpoint"),
    })
```

#### 评分 + 原子预留

```python
async def select_worker(self, request_id, token_ids, allowed_worker_ids,
                         expected_output_tokens=None):
    """Score the allowed workers for a request based on KV-cache overlap and
    load and pick the best one."""
    if self._svc is None:
        raise RuntimeError(
            "KV-aware routing is unavailable because ai-dynamo is not installed."
        )
    await self._evict_stale_requests()
    request = {
        "model_name": _MODEL_NAME,
        "tenant_id": _TENANT_ID,
        "selection_id": request_id,
        "token_ids": token_ids,
        "allowed_worker_ids": allowed_worker_ids,
        "expected_output_tokens": expected_output_tokens,
    }
    # Dynamo Rust SelectionService 的原子操作：
    # 1. 根据 token_ids 在 HashTree 中查找每个 worker 的 KV overlap
    # 2. 估算每个 worker 的 Token Load (prefill + decode)
    # 3. 选择 Token Load 最低的 worker
    # 4. 原子预留 (防止并发请求 herding 到同一 worker)
    selection = await self._svc.select_and_reserve(request)

    self._track_request_state(
        request_id, selection["worker_id"],
        len(token_ids), expected_output_tokens,
    )
    # 广播预留到其他 Ingress Replica（最终一致性）
    if self._reservation_forwarder is not None:
        self._reservation_forwarder.report(reservation)
    return {
        "worker_id": selection["worker_id"],
        "dp_rank": selection["dp_rank"],
        "overlap_tokens": selection["overlap"]["longest_matched"],
        "effective_prefill_tokens": selection["effective_prefill_tokens"],
    }
```

#### 请求生命周期跟踪

```python
async def on_prefill_complete(self, request_id: str):
    """Record a request's prefill -> decode transition, dropping its prefill
    load in the selection service."""
    state = self._requests.get(request_id)
    if state is None:
        return
    state.prefill_completed = True
    await self._svc.prefill_complete(request_id)

async def on_decode_progress(self, request_id, cumulative_output_tokens):
    """Advance request_id to an exact cumulative output-token count,
    booking one decode block in the selection service per crossed boundary.
    """
    state = self._requests.get(request_id)
    if state is None:
        return
    state.output_tokens = cumulative_output_tokens
    new_total_blocks = math.ceil(
        (state.prompt_tokens + cumulative_output_tokens) / self._block_size
    )
    decay_fraction = self._get_decay_fraction(state)
    while new_total_blocks > state.total_blocks:
        state.total_blocks += 1
        self._svc.add_output_block(request_id, decay_fraction=decay_fraction)

async def on_request_completed(self, request_id):
    """Free request_id from the selection service's active load."""
    state = self._requests.pop(request_id, None)
    self._mark_request_completed(request_id)
    if state is None:
        return
    self._untrack_worker_request(request_id, state.worker_id)
    await self._svc.free_reservation(request_id)
```

#### 跨 Ingress 预留广播

```python
class ReservationBroadcastForwarder:
    """Best-effort background replication of selected-worker reservations.

    ``report`` only enqueues the selected-worker booking facts Dynamo already
    returned. Sending the broadcast and waiting for its results happen on the
    delivery task, off the request's selection and dispatch path.
    """

    def report(self, reservation: ReservationBroadcast) -> None:
        if self._delivery_task is None or self._delivery_task.done():
            self._delivery_task = asyncio.get_running_loop().create_task(
                self._deliver()
            )
        self._reservations.put_nowait(reservation)

    async def _deliver(self) -> None:
        while True:
            batch = [await self._reservations.get()]
            while not self._reservations.empty():
                batch.append(self._reservations.get_nowait())
            try:
                results = await self._handle.broadcast(
                    "on_reservations_created", batch
                ).results_async(
                    timeout_s=LIFECYCLE_EVENT_BROADCAST_TIMEOUT_S,
                    return_exceptions=True,
                )
            except Exception as e:
                logger.warning("Dropping selection service reservation broadcast: %s", e)
```

### 12.4 常量定义

源码位置：`kv_aware/constants.py`

```python
REQUEST_TOKEN_IDS_KWARG = "request_token_ids"

KV_TOKEN_KEY_HEADER = SERVE_INGRESS_ROUTER_HEADER_PREFIX + "kv-token-key"
KV_TOKEN_METADATA_KEY = "kv_token_metadata"

KV_TOKEN_STAGING_TTL_S = 60              # token 缓存 TTL
KV_TOKEN_STAGING_MAX_ENTRIES = 8192      # 最大条目数
KV_TOKEN_STAGING_MAX_BYTES = 1024**3    # 1GB 最大字节数
KV_TOKEN_ZMQ_SEND_QUEUE_LIMIT = 256      # ZMQ 发送队列上限
KV_TOKEN_ZMQ_RECEIVE_QUEUE_LIMIT = 1024  # ZMQ 接收队列上限
KV_TOKEN_ZMQ_MAX_SOCKETS = 4096          # ZMQ 最大 socket 数

KV_EVENTS_PORT_BASE_KEY = "KV_EVENTS_PORT_BASE"
DEFAULT_KV_EVENTS_PORT_BASE = 5557       # KV 事件 PUB 默认起始端口

KV_TOKEN_PORT_BASE_KEY = "KV_TOKEN_PORT_BASE"
DEFAULT_KV_TOKEN_PORT_BASE = 7557        # Token 传输 PULL 默认起始端口

KV_INDEXER_THREADS_KEY = "KV_INDEXER_THREADS"
DEFAULT_KV_INDEXER_THREADS = 4          # SelectionService KV 索引线程数

DEFAULT_KV_EVENTS_REPLAY_PORT_OFFSET = 1000  # Replay ROUTER 端口偏移量
REQUEST_TRACKING_TTL_S = 3600                # 请求生命周期跟踪 TTL
LIFECYCLE_EVENT_BROADCAST_TIMEOUT_S = 3      # 生命周期事件广播超时
```

**端口分配图**：

```
端口范围                    用途
─────────────────────────────────────────────────
5557 + replica_rank         KV 事件 PUB socket
5557 + 1000 + replica_rank KV 事件 Replay ROUTER socket
7557 + replica_rank         Prompt Token PULL socket
```

### 12.5 配置入口

源码位置：`kv_aware/utils.py`

```python
def _maybe_setup_kv_aware_routing(deployment_options: dict, llm_config: LLMConfig):
    """当 deployment 的 request_router_config 是 KVAwareRouter 时触发配置。"""
    if not is_kv_aware(llm_config):
        if llm_config.engine_kwargs.get("kv_events_config") is not None:
            logger.warning(
                "engine_kwargs['kv_events_config'] is set but the deployment's "
                "request router is not a KVAwareRouter, so the engine's KV events "
                "will not be consumed."
            )
        return

    llm_config.deployment_config["request_router_config"] = deployment_options[
        "request_router_config"
    ]
    configure_kv_events_for_kv_routing(llm_config)
```

**调用链**：部署创建时 → `LLMConfig` 合并 → `_maybe_setup_kv_aware_routing()` 检查 `is_kv_aware()` → 调用 `configure_kv_events_for_kv_routing()` 配置 vLLM 引擎 KV 事件。

### 12.6 vLLM KV 事件配置

源码位置：`kv_aware/vllm/kv_events.py:23-78`

```python
def configure_kv_events_for_kv_routing(llm_config: LLMConfig):
    """Enable engine KV-cache events for a KV-aware-routed deployment."""
    engine_kwargs = llm_config.engine_kwargs
    if engine_kwargs.get("enable_prefix_caching") is False:
        logger.warning(
            "KV-aware routing is configured but enable_prefix_caching is False; "
            "the engine will not emit KV-cache events."
        )

    llm_config.update_engine_kwargs(
        kv_events_config={
            "enable_kv_cache_events": True,
            "publisher": "zmq",
            "endpoint": _default_kv_events_endpoint(llm_config),     # tcp://*:5557
            "replay_endpoint": _default_kv_events_replay_endpoint(llm_config), # tcp://*:6557
        }
    )
    _configure_runtime_env_for_kv_routing(llm_config)

def _configure_runtime_env_for_kv_routing(llm_config: LLMConfig):
    """vLLM 的 block-hash chain root 默认按进程随机 salt，
    必须 pin PYTHONHASHSEED=0 才能保证多 replica 产生相同 hash。"""
    runtime_env = dict(llm_config.runtime_env or {})
    env_vars = dict(runtime_env.get("env_vars") or {})
    env_vars.setdefault("PYTHONHASHSEED", "0")
    env_vars["VLLM_USE_SIMPLE_KV_OFFLOAD"] = "0"
    runtime_env["env_vars"] = env_vars
    llm_config.runtime_env = runtime_env

def assign_replica_kv_events_endpoint(llm_config: LLMConfig):
    """Replica 启动后按 local_rank 偏移端口，防止同机多 replica 端口冲突。"""
    if not is_kv_aware(llm_config):
        return
    kv_events_config = llm_config.engine_kwargs.get("kv_events_config")
    if kv_events_config is None:
        return
    updated = dict(kv_events_config)
    endpoint = updated.pop("endpoint")
    replay_endpoint = updated.pop("replay_endpoint")
    if llm_config.engine_kwargs.get("data_parallel_rank") is not None:
        offset = 0  # data parallel 下由 engine 内部按 dp_rank 偏移
    else:
        offset = _get_replica_rank()  # serve.get_replica_context().rank.local_rank
    updated["endpoint"] = _get_offset_endpoint_port(endpoint, offset)
    updated["replay_endpoint"] = _get_offset_endpoint_port(replay_endpoint, offset)
    llm_config.update_engine_kwargs(kv_events_config=updated)

def resolve_kv_event_source_endpoint(llm_config: LLMConfig) -> Optional[str]:
    """返回 node-routable endpoint（将通配符替换为本机 IP），供 SelectionService 连接。"""
    if not is_kv_aware(llm_config):
        return None
    kv_events_config = llm_config.engine_kwargs.get("kv_events_config")
    if kv_events_config is None:
        return None
    return _get_node_routable_endpoint(llm_config, kv_events_config["endpoint"])

def _get_node_routable_endpoint(llm_config: LLMConfig, endpoint: str) -> str:
    """将 tcp://*:5557 转换为 tcp://10.0.1.5:5557 的实际可达地址。"""
    dp_rank = llm_config.engine_kwargs.get("data_parallel_rank")
    if dp_rank is not None:
        endpoint = _get_offset_endpoint_port(endpoint, dp_rank)
    port = endpoint.rsplit(":", 1)[1]
    return f"tcp://{ray.util.get_node_ip_address()}:{port}"
```

**关键设计点**：

1. **PYTHONHASHSEED=0**：vLLM 的 block hash 默认按进程 salt，多 replica 必须统一才能让 SelectionService 的 HashTree 正确 dedup
2. **端口偏移**：同节点多 replica 共享端口基数，按 `local_rank` 偏移避免冲突；data parallel 下 engine 内部按 `dp_rank` 偏移
3. **Replay ROUTER**：用于慢加入者（Slow Joiner）问题——SelectionService 的 ZMQ SUB 连接晚于 Publisher 发送，通过 replay ROUTER 补发缺失事件

#### Replica 上报 ZMQ endpoint

```python
def get_kv_event_routing_stats(llm_config, block_size, max_num_batched_tokens):
    """返回 routing_stats payload，随 LongPoll 广播到所有 Ingress。"""
    if not is_kv_aware(llm_config):
        return {}
    kv_events_config = llm_config.engine_kwargs.get("kv_events_config")
    if kv_events_config is None:
        return {}
    kv_event_metadata = {
        "endpoint": _get_node_routable_endpoint(llm_config, kv_events_config["endpoint"]),
        "block_size": block_size,
        "max_num_batched_tokens": max_num_batched_tokens,
        "dp_rank": llm_config.engine_kwargs.get("data_parallel_rank") or 0,
        "replay_endpoint": _get_node_routable_endpoint(
            llm_config, kv_events_config["replay_endpoint"]
        ),
    }
    return {"kv_event_metadata": kv_event_metadata}

def get_token_channel_endpoints(llm_config: LLMConfig) -> Optional[tuple[str, str]]:
    """返回 prompt-token ZMQ PULL socket 的 (bind_endpoint, advertised_endpoint)。"""
    if not is_kv_aware(llm_config):
        return None
    port = _default_prompt_token_port(llm_config) + _get_replica_rank()
    return f"tcp://*:{port}", f"tcp://{ray.util.get_node_ip_address()}:{port}"
```

metadata 通过 `record_routing_stats()` 写入 `RunningReplicaInfo.routing_stats`，随 LongPoll 广播到所有 Ingress。

### 12.7 Prompt Token 传输通道

源码位置：`kv_aware/token_channel.py`

Token Channel 是 **engine 无关** 的通用组件，用于将 Ingress 侧的 tokenizer 输出传送到被选中的 Engine Replica，避免 Engine 二次 tokenize。

#### 数据编码

```python
def encode_prompt_token_ids(token_ids: List[int]) -> bytes:
    """将 token IDs 编码为紧凑的 little-endian uint32 字节流。"""
    arr = np.asarray(token_ids, dtype="<u4")  # uint32 LE
    return arr.tobytes()

def decode_prompt_token_ids(payload: bytes) -> List[int]:
    """解码 encode_prompt_token_ids 产生的字节流。"""
    return np.frombuffer(payload, dtype="<u4").tolist()
```

#### Ingress 侧：TokenSender

```python
class TokenSender:
    """Best-effort one-way prompt-token sender for selected LLMServer replicas.

    用 ZMQ PUSH socket 向选中的 replica 的 PULL socket 发送 token payload。
    发送是 fire-and-forget：如果 ZMQ pipe 不可用或已满，发送失败并回退到 engine 自行 tokenize。
    """

    def push(self, endpoint: str, key: str, payload: bytes) -> bool:
        """非阻塞发送。返回 False 时 caller 应省略 token-key header，
        让 engine 回退到正常 tokenize。"""
        socket = self._get_socket(endpoint)  # LRU 缓存的 PUSH socket
        if socket is None:
            return False
        try:
            socket.send_multipart(
                [key.encode("ascii"), payload],  # [request_id, token_bytes]
                flags=zmq.DONTWAIT, copy=False,
            )
        except zmq.Again:
            return False  # 队列满，回退
        except zmq.ZMQError:
            self._discard_socket(endpoint)  # socket 坏了，重连
            return False
        return True
```

**设计特点**：
- LRU 缓存 socket，最多 `max_sockets=4096` 个连接
- `IMMEDIATE=1`：只在 peer 已连接时才发送，避免排队到无人消费的 pipe
- `SNDHWM=256`：发送队列上限，超出后返回 `zmq.Again`
- 失败时静默降级——engine 收不到 token 就自行 tokenize

#### Engine 侧：TokenReceiver + TokenStore

```python
class TokenStore:
    """LLMServer replica-local staging area for prompt tokens sent from LLMRouter.

    Invariants:
        - Bounded: max_entries + max_bytes (1GB)
        - Eviction: oldest-first FIFO
        - Expiry: TTL=60s
        - Concurrency: 单线程事件循环，put/pop 原子（无 await 点）
        - Complexity: amortized O(1)
    """

    def put(self, key: str, *, payload: bytes) -> None:
        """存入 token payload，key 是 per-request UUID。"""
        if len(payload) > self._max_bytes:
            raise ValueError("payload exceeds staging byte cap")
        now = time.monotonic()
        self._sweep(now)  # 淘汰过期条目
        old = self._entries.pop(key, None)  # 重复 key 先清旧值
        if old is not None:
            self._total_bytes -= len(old.payload)
        entry = _StagedTokens(payload=payload, created_at_s=now)
        self._entries[key] = entry
        self._total_bytes += len(payload)
        self._evict_to_limits()  # 超容量淘汰

    def pop(self, key: str) -> Optional[_StagedTokens]:
        """engine 消费一次后即删除（single-use key）。"""
        now = time.monotonic()
        self._sweep(now)
        entry = self._entries.pop(key, None)
        if entry is not None:
            self._total_bytes -= len(entry.payload)
        return entry

class TokenReceiver:
    """Best-effort ZMQ PULL receiver for prompt-token payloads.
    接收后存入 TokenStore，等待 engine 按 key 消费。"""

    async def start(self) -> bool:
        context = zmq_asyncio.Context.instance()
        socket = _new_socket(context, zmq.PULL)
        socket.setsockopt(zmq.RCVHWM, self._receive_queue_limit)
        socket.bind(self._bind_endpoint)
        self._task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        while True:
            parts = await socket.recv_multipart(copy=True)
            self._handle_message(parts)  # key_frame + payload → store.put()

    def _handle_message(self, parts: List[bytes]) -> None:
        if len(parts) != 2:
            return  # 丢弃格式错误的消息
        key_frame, payload = parts
        self._store.put(key_frame.decode("ascii"), payload=payload)
```

### 12.8 vLLM Prompt Token 注入

源码位置：`kv_aware/vllm/prompt_token_forwarding.py`

当 Engine Replica 收到请求时，如果 Ingress 已经通过 ZMQ 传来了 pre-tokenized 的 prompt token IDs，就注入到请求中，跳过 engine 的 tokenize 步骤。

```python
def inject_prompt_token_ids(request, raw_request, store: TokenStore):
    """从 HTTP header 中取出 token key，从 TokenStore pop 出 payload，注入到请求。"""
    if raw_request is None:
        return
    token_key = raw_request.headers.get(KV_TOKEN_KEY_HEADER)  # "x-ray-serve-kv-token-key"
    if not token_key:
        return

    entry = store.pop(token_key)  # single-use: pop 后即删除
    if entry is None:
        return  # payload 未到达或已过期，engine 自行 tokenize

    token_ids = decode_prompt_token_ids(entry.payload)
    kv_transfer_params = getattr(request, "kv_transfer_params", None)
    if not isinstance(kv_transfer_params, dict):
        kv_transfer_params = {}
        request.kv_transfer_params = kv_transfer_params
    kv_transfer_params["prompt_token_ids"] = token_ids  # 注入到 vLLM 的请求参数

def install_prompt_token_forwarding(state, store: TokenStore):
    """Monkey-patch vLLM 的 serving 方法，在请求处理前注入 token IDs。

    包装 vLLM OpenAIServing 的 create_chat_completion 和 create_completion 方法。
    """
    for attr_name, method_name in (
        ("openai_serving_chat", "create_chat_completion"),
        ("openai_serving_completion", "create_completion"),
    ):
        _install_prompt_token_forwarding(
            getattr(state, attr_name, None), method_name, store,
        )

def _install_prompt_token_forwarding(serving, method_name, store):
    orig = getattr(serving, method_name, None)
    if orig is None or getattr(orig, "_kv_token_channel_wrapped", False):
        return

    @functools.wraps(orig)
    async def wrapped(request, raw_request=None, *args, **kw):
        effective_raw_request = kw.get("raw_request", raw_request)
        inject_prompt_token_ids(request, effective_raw_request, store)
        if "raw_request" in kw and raw_request is None:
            return await orig(request, *args, **kw)
        return await orig(request, raw_request, *args, **kw)

    wrapped._kv_token_channel_wrapped = True
    setattr(serving, method_name, wrapped)
```

**注入流程**：

```
Ingress (LLMRouter)                          Engine Replica (LLMServer)
┌──────────────────────────┐               ┌────────────────────────────────┐
│ 1. Tokenizer.tokenize()  │               │                                │
│    → token_ids: [1,2,3]  │               │                                │
│ 2. encode_prompt_token_   │   ZMQ PUSH    │ 4. TokenReceiver → TokenStore  │
│    ids(token_ids)         │──────────────►│    store.put(key, payload)     │
│    → bytes payload       │               │                                │
│ 3. TokenSender.push()     │               │ 5. HTTP request arrives with   │
│    + HAProxy forwards    │               │    header: x-ray-serve-kv-    │
│      x-ray-serve-kv-     │               │    token-key: <request_id>     │
│      token-key: <req_id> │               │                                │
└──────────────────────────┘               │ 6. inject_prompt_token_ids()   │
                                           │    store.pop(key) → decode     │
                                           │    → request.kv_transfer_params│
                                           │      ["prompt_token_ids"]      │
                                           │                                │
                                           │ 7. vLLM skips tokenize,       │
                                           │    uses pre-tokenized IDs     │
                                           └────────────────────────────────┘
```

### 12.9 vLLM Tokenizer（Ingress 内预分词）

源码位置：`kv_aware/vllm/tokenizer.py`

KVAwareRouter 需要请求的 token_ids 来计算 KV Cache overlap。Ingress 在路由前先 tokenize 请求，然后将 token_ids 同时传给：
1. KVAwareRouter（通过 `REQUEST_TOKEN_IDS_KWARG`）用于评分
2. 选中的 Engine Replica（通过 ZMQ Token Channel）用于跳过二次 tokenize

```python
class Tokenizer:
    """Tokenizes requests with vLLM's ``OnlineRenderer``.

    关键：必须与 Engine 使用完全相同的 tokenizer + chat template，
    否则 Ingress 侧的 token_ids 和 Engine 侧不一致，KV overlap 计算失效。
    """

    def __init__(self, llm_config: LLMConfig):
        engine_config = llm_config.get_engine_config()
        _, vllm_config = _get_vllm_engine_config(llm_config, device_type="cpu")
        self._model_config = vllm_config.model_config

        frontend_args = FrontendArgs(**engine_config.frontend_kwargs)
        self._renderer = OnlineRenderer(
            self._model_config,
            renderer_from_config(vllm_config),
            request_logger=None,
            chat_template=load_chat_template(frontend_args.chat_template),
            chat_template_content_format=frontend_args.chat_template_content_format,
            trust_request_chat_template=frontend_args.trust_request_chat_template,
            enable_auto_tools=frontend_args.enable_auto_tool_choice,
            exclude_tools_when_tool_choice_none=(
                frontend_args.exclude_tools_when_tool_choice_none
            ),
            tool_parser=frontend_args.tool_call_parser,
            default_chat_template_kwargs=frontend_args.default_chat_template_kwargs,
        )

    async def tokenize(self, payload: Dict[str, Any]) -> Optional[List[int]]:
        """将请求 tokenize 为 prompt token IDs。
        返回 None 时（如 batch prompt），回退到 token-less routing。"""
        request = build_tokenize_request(payload)
        if request is None:
            return None

        if isinstance(request, ChatCompletionRequest):
            rendered_inputs = await self._render_chat(request)
        else:
            rendered_inputs = await self._render_completion(request)

        input_ids: List[int] = []
        for rendered_input in rendered_inputs:
            components = extract_prompt_components(self._model_config, rendered_input)
            if components.token_ids is not None:
                input_ids.extend(components.token_ids)
        return input_ids
```

**build_tokenize_request 的分支逻辑**：

```python
def build_tokenize_request(payload):
    """从请求 body 构建可 tokenize 的请求对象。

    - 有 "messages" → ChatCompletionRequest（需 chat template 渲染）
    - 有 "prompt" 且是 str → TokenizeCompletionRequest（简单 tokenize）
    - 有 "prompt" 但非 str（batch list）→ 返回 None（不路由）
    """
    if "messages" in payload:
        return ChatCompletionRequest.model_validate(...)
    if "prompt" in payload:
        if not isinstance(payload["prompt"], str):
            return None  # batch prompt 不支持
        return TokenizeCompletionRequest.model_validate(...)
    return None
```

**与 Engine 的一致性保证**：Tokenizer 初始化时读取与 Engine 完全相同的：
- `model_config`（tokenizer 类型、vocab）
- `chat_template`（HF chat template / Harmony / Mistral）
- `trust_request_chat_template`、`tool_parser` 等

### 12.10 vLLM 生命周期 Hook

源码位置：`kv_aware/vllm/token_tracking.py`

vLLM Engine 在处理请求时需要向 KVTokenTracker 报告生命周期事件，以便 SelectionService 准确跟踪每个 worker 的实时负载。

#### LifecycleEventForwarder（Engine Replica 侧）

```python
class LifecycleEventForwarder:
    """Ordered, non-blocking bridge from the engine to every LLMRouter
    replica's KVTokenTracker.

    report() 只入本地队列，不阻塞 generation。
    单个 delivery task 按 submission order 排空队列，broadcast 到所有 Ingress。
    """

    def __init__(self, handle: DeploymentHandle, worker_id: int):
        self.handle = handle           # LLMRouter deployment handle
        self.worker_id = worker_id
        self._events: asyncio.Queue = asyncio.Queue()
        self._delivery_task: Optional[asyncio.Task] = None

    def report(self, method_name: str, *args) -> None:
        """非阻塞入队。"""
        if self._delivery_task is None or self._delivery_task.done():
            self._delivery_task = asyncio.get_running_loop().create_task(
                self._deliver()
            )
        self._events.put_nowait((method_name, args))

    async def _deliver(self) -> None:
        while True:
            batch = [await self._events.get()]
            while not self._events.empty():
                batch.append(self._events.get_nowait())  # 合并排队的消息
            try:
                results = await self.handle.broadcast(
                    "on_lifecycle_events", batch
                ).results_async(
                    timeout_s=LIFECYCLE_EVENT_BROADCAST_TIMEOUT_S,
                    return_exceptions=True,
                )
                errors = [r for r in results if isinstance(r, Exception)]
                if errors:
                    logger.warning(
                        "KV lifecycle events dropped on %d/%d ingress replicas",
                        len(errors), len(results),
                    )
            except Exception as e:
                logger.warning("Dropping KV lifecycle events: %s", e)
```

**设计要点**：
- `report()` 不 await——generation 永远不阻塞在 lifecycle 事件上
- batch 合并——队列中积压的事件合并为一次 broadcast，减少 RPC 次数
- `return_exceptions=True`——一个 Ingress 挂掉不影响其他
- 3 秒超时——慢 Ingress 不拖慢 Engine

#### RequestTokenTracker（单请求跟踪器）

```python
class RequestTokenTracker:
    """Drives the request lifecycle hooks for one ``generate()`` stream."""

    def __init__(self, forwarder, request_id, report_decode_progress=False):
        self._forwarder = forwarder
        self._request_id = request_id
        self._cumulative = 0        # 累计 output token 数
        self._prefill_marked = False
        self._finished = False
        self._report_decode_progress = report_decode_progress

    def on_output(self, output: RequestOutput) -> None:
        """观察一个 vLLM RequestOutput chunk。

        vLLM 流式输出 DELTA chunk 或单个 FINAL_ONLY chunk；
        两者只携带新增 token，所以直接累加。
        """
        step_tokens = sum(len(o.token_ids or []) for o in output.outputs)
        if step_tokens == 0:
            return  # finish-only chunk
        self._cumulative += step_tokens

        if not self._prefill_marked:
            # 第一个 output token 标志 prefill 完成
            self._prefill_marked = True
            self._forwarder.report("on_prefill_complete", self._request_id)

        if self._report_decode_progress:
            self._forwarder.report(
                "on_decode_progress", self._request_id, self._cumulative
            )

    def finish(self) -> None:
        if not self._finished:
            self._finished = True
            self._forwarder.report("on_request_completed", self._request_id)
```

#### enable_token_tracking 装饰器

```python
def enable_token_tracking(engine_cls: Type[AsyncLLM], report_decode_progress=False):
    """Decorator：为 vLLM 的 AsyncLLM.generate() 添加 KV-router 生命周期跟踪。"""

    class TokenTrackingEngine(engine_cls):
        _lifecycle_forwarder: Optional[LifecycleEventForwarder] = None

        def _resolve_lifecycle_forwarder(self):
            if self._lifecycle_forwarder is None:
                try:
                    handle = get_llm_router_handle()
                    worker_id = get_worker_id(
                        serve.get_replica_context().replica_id.unique_id
                    )
                    self._lifecycle_forwarder = LifecycleEventForwarder(
                        handle, worker_id
                    )
                except Exception as e:
                    if not self._resolve_warned:
                        self._resolve_warned = True
                        logger.warning("KV token tracking disabled: %s", e)
            return self._lifecycle_forwarder

        async def generate(self, prompt, sampling_params, request_id, *args, **kwargs):
            stream = super().generate(prompt, sampling_params, request_id, *args, **kwargs)
            forwarder = self._resolve_lifecycle_forwarder()

            if forwarder is None or (
                sampling_params.output_kind == RequestOutputKind.CUMULATIVE
            ):
                async for output in stream:
                    yield output
                return

            lifecycle_request_id = get_serve_request_id() or request_id
            tracker = RequestTokenTracker(
                forwarder, lifecycle_request_id,
                report_decode_progress=self._report_decode_progress,
            )
            try:
                async for output in stream:
                    tracker.on_output(output)
                    yield output
            finally:
                tracker.finish()

    return TokenTrackingEngine
```

**生命周期事件流**：

```
vLLM Engine Replica                              Ingress (LLMRouter)
┌────────────────────────┐                     ┌──────────────────────────┐
│ TokenTrackingEngine     │                     │ KVTokenTracker            │
│   .generate()           │                     │                           │
│   │                     │                     │                           │
│   ├─ first output token │  "on_prefill_complete"│ on_prefill_complete()    │
│   │                     │─────────────────────►│   → svc.prefill_complete()│
│   │                     │                     │                           │
│   ├─ each decode step   │  "on_decode_progress" │ on_decode_progress()     │
│   │   (if enabled)      │─────────────────────►│   → svc.add_output_block()│
│   │                     │                     │                           │
│   └─ stream end         │  "on_request_completed"│ on_request_completed() │
│                         │─────────────────────►│   → svc.free_reservation()│
└────────────────────────┘                     └──────────────────────────┘
       │ report()                                           │
       ▼                                                    ▼
  LifecycleEventForwarder                           SelectionService (Rust)
    → asyncio.Queue                                  → 更新 worker load
    → broadcast("on_lifecycle_events")               → 评分时使用实时负载
```

### 12.11 完整 KV 事件流路径

```
vLLM Engine Replica                    Ingress Replica (LLMRouter)
┌──────────────────┐                  ┌──────────────────────────┐
│ Scheduler         │                  │ KVTokenTracker            │
│   │               │                  │   │                       │
│   ▼               │                  │   ▼                       │
│ KVCacheEvent      │   ZMQ PUB/SUB   │ SelectionService (Rust)  │
│ Recorder          │─────────────────►│   ├─ ZMQ SUB per worker  │
│   │               │  BlockStored     │   ├─ HashTree 索引       │
│   ▼               │  BlockRemoved    │   ├─ Token Load 评分     │
│ ZmqEventPublisher │  AllBlocksCleared│   └─ select_and_reserve()│
│   │               │                  │                           │
│   ▼               │                  │ RequestLifecycle          │
│ Replay Buffer     │◄──── replay ─────│   ├─ on_prefill_complete  │
│ (ROUTER socket)   │  缺失事件补发    │   ├─ on_decode_progress   │
│                   │                  │   └─ on_request_completed │
└──────────────────┘                  └──────────────────────────┘
                                           │
                                           │  广播预留到其他 Ingress
                                           ▼
                                      ReservationBroadcastForwarder
                                        → peer Ingress 的 _svc.create_reservation()

请求路径（Ingress → Engine）:
┌──────────────────────────────────────────────────────────────────┐
│ Client Request                                                   │
│    │                                                             │
│    ▼                                                             │
│ LLMRouter Ingress:                                               │
│   1. Tokenizer.tokenize(request) → token_ids                    │
│   2. KVAwareRouter.choose_replicas(token_ids)                    │
│      └→ KVTokenTracker.select_worker(token_ids, workers)        │
│         └→ SelectionService.select_and_reserve() → selected_worker│
│   3. TokenSender.push(endpoint, request_id, encoded_tokens)      │
│      └→ ZMQ PUSH → Engine's TokenReceiver → TokenStore          │
│   4. HAProxy forwards request to selected replica               │
│      └→ Header: x-ray-serve-kv-token-key: <request_id>         │
│    │                                                             │
│    ▼                                                             │
│ Engine Replica:                                                  │
│   5. inject_prompt_token_ids()                                   │
│      └→ store.pop(key) → decode → request.kv_transfer_params    │
│   6. vLLM processes with pre-tokenized IDs (skip tokenize)      │
│   7. TokenTrackingEngine.generate() → lifecycle events          │
│      └→ LifecycleEventForwarder → broadcast to Ingress           │
└──────────────────────────────────────────────────────────────────┘
```

**Ray Serve 不是自己实现 ZMQ 订阅**，而是把 endpoint 传给 Dynamo 的 `SelectionService`（Rust PyO3 对象），由 Dynamo 内部 spawn ZMQ SUB 连接、解码 msgpack、维护 HashTree、做评分。Ray Serve 只负责：
1. 配置 vLLM 启用 KV 事件
2. 通过 LongPoll 发现 replica 并获取其 ZMQ endpoint
3. 调用 `svc.upsert_worker()` 把 endpoint 注册给 Dynamo
4. Ingress 侧 tokenize 请求 + 通过 ZMQ Token Channel 传送 token_ids
5. 调用 `svc.select_and_reserve()` 做路由决策
6. 跟踪请求生命周期（prefill/decode/complete）并通知 Dynamo

---

## 13. vLLM 与 SGLang 的 KV 事件系统

### 13.1 SGLang KV 事件体系（与 vLLM 协议兼容）

SGLang 有完全对应的 KV 事件体系，代码结构直接对标 vLLM：

| | vLLM | SGLang |
|---|---|---|
| **事件定义** | `vllm/distributed/kv_events.py` | `sglang/srt/disaggregation/kv_events.py` |
| **事件录制** | vLLM Scheduler 内部 | `sglang/srt/mem_cache/events.py` → `KVCacheEventRecorder` |
| **事件发布** | `ZmqEventPublisher` | `ZmqEventPublisher`（几乎相同的实现） |
| **事件格式** | `BlockStored`, `BlockRemoved`, `AllBlocksCleared` | **完全相同** |
| **传输协议** | ZMQ PUB/SUB, msgpack 编码 | ZMQ PUB/SUB, msgpack 编码 |
| **DP rank 支持** | ✅ 每个 DP rank 独立 publisher | ✅ 每个 attn DP rank 独立 publisher |
| **Replay 机制** | ROUTER socket + replay buffer | ROUTER socket + replay buffer |

### 13.2 SGLang 事件格式定义

源码位置：`sglang/srt/disaggregation/kv_events.py`

```python
class EventBatch(msgspec.Struct, array_like=True, gc=False):
    ts: float
    events: list[Any]
    attn_dp_rank: Optional[int] = None

class KVCacheEvent(msgspec.Struct, array_like=True, gc=False, tag=True):
    """Base class for all KV cache-related events"""

class StorageMedium(str, enum.Enum):
    GPU = "GPU"            # L1: device HBM
    CPU = "CPU_PINNED"     # L2: host pinned memory
    DISK = "DISK"          # L3: SSD / NVMe
    EXTERNAL = "EXTERNAL"  # L4: shared / remote pool

class BlockStored(KVCacheEvent):
    block_hashes: list[int]
    parent_block_hash: Optional[int]
    token_ids: list[int]
    block_size: int
    lora_id: Optional[int]
    medium: Optional[str] = None

class BlockStoredWithMetadata(BlockStored, tag="BlockStored", kw_only=True):
    metadata: BlockStoredMetadata

class BlockRemoved(KVCacheEvent):
    block_hashes: list[int]
    medium: Optional[str] = None

class AllBlocksCleared(KVCacheEvent):
    pass

class KVEventBatch(EventBatch):
    events: list[Union[BlockStored, BlockRemoved, AllBlocksCleared]]
```

**与 vLLM 的差异**：

| 字段 | vLLM | SGLang |
|------|------|--------|
| `EventBatch.attn_dp_rank` | 无此字段 | ✅ 支持 DP-attention |
| `StorageMedium` | 无此枚举 | ✅ GPU/CPU/DISK/EXTERNAL 四级 |
| `BlockStoredWithMetadata` | 无 | ✅ 支持 `cache_salt` 加盐 hash |
| `lora_id` | ✅ | ✅ |
| 编码方式 | msgpack | msgpack (msgspec) |

**关键兼容性**：Dynamo SelectionService 只消费 `BlockStored` / `BlockRemoved` / `AllBlocksCleared` 三个 tag，SGLang 的 `BlockStoredWithMetadata` 共享 `"BlockStored"` tag，Dynamo 按 base type 解码会忽略 trailing metadata，**协议完全兼容**。

### 13.3 SGLang ZmqEventPublisher 完整实现

源码位置：`sglang/srt/disaggregation/kv_events.py:348-584`

```python
class ZmqEventPublisher(EventPublisher):
    """Reliable PUB/ROUTER publisher with an in-memory replay buffer.

    Spawns a separate thread to handle publishing from a queue.

    Wire format (3-frame multipart):
    1. topic_bytes  — UTF-8 encoded topic string
    2. seq_bytes    — 8-byte big-endian sequence number
    3. payload      — msgpack-encoded EventBatch
    """

    SHUTDOWN_TIMEOUT: float = 1.0
    END_SEQ = (-1).to_bytes(8, "big", signed=True)

    def __init__(self, attn_dp_rank, endpoint="tcp://*:5557",
                 replay_endpoint=None, buffer_steps=10_000,
                 hwm=100_000, max_queue_size=100_000, topic=""):
        self._event_queue = Queue[Optional[EventBatch]](maxsize=max_queue_size)
        self._buffer = deque[tuple[int, bytes]](maxlen=buffer_steps)

        self._ctx = zmq.Context.instance()
        self._pub: Optional[zmq.Socket] = None
        self._replay: Optional[zmq.Socket] = None
        self._dp_rank = attn_dp_rank
        # 按 DP rank 偏移端口
        self._endpoint = self.offset_endpoint_port(endpoint, self._dp_rank)
        self._replay_endpoint = self.offset_endpoint_port(
            replay_endpoint, self._dp_rank
        )
        self._hwm = hwm
        self._socket_setup()  # bind PUB + ROUTER

        self._seq_gen = count()
        self._topic_bytes = topic.encode("utf-8")
        self._running = True

        # 发布线程：避免阻塞 scheduler
        self._thread = threading.Thread(
            target=self._publisher_thread, daemon=True, name="zmq-publisher"
        )
        self._thread.start()
        atexit.register(self.shutdown)

    def publish(self, events: EventBatch) -> None:
        if not self._running:
            raise RuntimeError("Publisher is closed")
        if events.attn_dp_rank is None:
            events.attn_dp_rank = self._dp_rank
        self._event_queue.put(events)

    def _publisher_thread(self) -> None:
        """后台线程：从队列取事件，序列化后通过 ZMQ PUB 发送。"""
        self._pack = msgspec.msgpack.Encoder()
        while self._running or self._event_queue.qsize() > 0:
            # 1. 检查 replay 请求（非阻塞 poll）
            if self._replay is not None and self._replay.poll(0):
                try:
                    self._service_replay()
                except Exception:
                    pass

            # 2. 从队列取事件
            try:
                event = self._event_queue.get(timeout=0.1)
                if event is None:
                    break  # sentinel
            except queue.Empty:
                continue

            # 3. 序列化 + 发送 + 缓存
            seq = next(self._seq_gen)
            payload = self._pack.encode(event)
            seq_bytes = seq.to_bytes(8, "big")
            self._pub.send_multipart((self._topic_bytes, seq_bytes, payload))
            self._buffer.append((seq, payload))

    def _service_replay(self) -> None:
        """处理慢加入者的 replay 请求。

        Wire format (3-frame request):
        1. client_id (ROUTER identity)
        2. empty delimiter
        3. start_seq_bytes (8-byte big-endian)
        """
        frame = self._replay.recv_multipart()
        client_id, _, start_seq_bytes = frame
        start_seq = int.from_bytes(start_seq_bytes, "big")

        for seq, buf in self._buffer:
            if seq >= start_seq:
                self._replay.send_multipart(
                    (client_id, b"", seq.to_bytes(8, "big"), buf)
                )
        self._replay.send_multipart(
            (client_id, b"", self.END_SEQ, b"")
        )  # -1 标记 replay 结束

    @staticmethod
    def offset_endpoint_port(endpoint, data_parallel_rank):
        """按 DP rank 偏移端口。
        tcp://*:5557 + rank=2 → tcp://*:5559
        inproc://cache + rank=2 → inproc://cache_dp2
        """
        if not endpoint or data_parallel_rank == 0:
            return endpoint
        if "inproc" in endpoint:
            return f"{endpoint}_dp{data_parallel_rank}"
        if "tcp" in endpoint and ":" in endpoint:
            last_colon_idx = endpoint.rfind(":")
            base_addr = endpoint[:last_colon_idx]
            base_port = int(endpoint[last_colon_idx + 1:])
            return f"{base_addr}:{base_port + data_parallel_rank}"
        raise ValueError("Invalid endpoint")
```

**与 vLLM ZmqEventPublisher 的关键差异**：

| | vLLM | SGLang |
|---|---|---|
| **线程模型** | 后台发布线程 | 后台发布线程（相同） |
| **编码** | msgpack (内部 encoder) | msgspec.msgpack.Encoder |
| **DP rank 偏移** | 引擎内部 `ZmqEventPublisher` | Publisher 构造时自动偏移 |
| **Socket 初始化** | 在 `__init__` 中 | 在 `_socket_setup` 中（相同模式） |
| **Replay** | ROUTER socket | ROUTER socket（相同模式） |
| **Buffer** | `deque(maxlen=buffer_steps)` | `deque(maxlen=buffer_steps)`（相同） |
| **Load Topic** | 无 | ✅ `LOAD_TOPIC = "load"` + `SchedulerLoadPublisher` |

**SGLang 额外功能**：`SchedulerLoadPublisher` 可以独立发布 worker 负载信息到单独的 `load_topic`，让 Router 不订阅 KV 事件也能获取负载。

### 13.4 SGLang KVCacheEventRecorder 完整逻辑

源码位置：`sglang/srt/mem_cache/events.py`

```python
class KVCacheEventRecorder:
    """Collects KV placement events for one cache.

    enabled=False 时所有 record_* 方法为 no-op，take() 返回空列表，
    调用者无需 guard。
    """

    def __init__(self, *, enabled: bool, page_size: int):
        self.enabled = enabled
        self.page_size = page_size
        self._queue: list = []

    def enqueue(self, event) -> None:
        """Append an event, coalescing it with a compatible queue tail.

        KV event batches already support multiple block hashes. Combining them
        here avoids emitting one event per page while preserving ordering and
        the parent-linked store chains consumers use to rebuild the cache tree.

        合并规则：
        - BlockRemoved + BlockRemoved (同 medium) → 合并 block_hashes
        - BlockStored + BlockStored (同 medium/lora_id/block_size/metadata，
          且新事件的 parent_block_hash == 旧事件最后一个 hash) → 合并
        """
        if self._queue:
            tail = self._queue[-1]
            if isinstance(tail, BlockRemoved) and isinstance(event, BlockRemoved):
                if tail.medium == event.medium:
                    tail.block_hashes.extend(event.block_hashes)
                    return
            elif isinstance(tail, BlockStored) and isinstance(event, BlockStored):
                # 检查 metadata / medium / lora_id / block_size / parent 链接
                if (tail.medium == event.medium
                    and tail.lora_id == event.lora_id
                    and tail.block_size == event.block_size
                    and tail_metadata == event_metadata
                    and tail.block_hashes
                    and event.parent_block_hash == tail.block_hashes[-1]):
                    tail.block_hashes.extend(event.block_hashes)
                    tail.token_ids.extend(event.token_ids)
                    return
        self._queue.append(event)

    def record_store(self, node, medium=None) -> None:
        """按 page_size 切片，生成 BlockStored 事件。

        每个页一个 BlockStored 事件，parent_block_hash 链接形成 hash chain，
        消费者据此重建 RadixTree 拓扑。
        """
        if not self.enabled:
            return
        if medium is None:
            medium = StorageMedium.GPU

        event_hash_values = self._node_event_hash_values(node)
        parent_block_hash = self._parent_block_hash(node)

        page_index = 0
        logical_len = len(node.key)
        is_bigram = node.key.is_bigram
        raw = node.key.token_ids
        for start in range(0, logical_len, self.page_size):
            end = min(start + self.page_size, logical_len)
            if end <= start:
                continue
            page_tokens = (
                [(raw[j], raw[j+1]) for j in range(start, end)]
                if is_bigram
                else list(raw[start:end])
            )
            block_hash = hash_str_to_int64(event_hash_values[page_index])

            event_args = {
                "block_hashes": [block_hash],
                "parent_block_hash": parent_block_hash,
                "token_ids": page_tokens,
                "block_size": len(page_tokens),
                "lora_id": None,
                "medium": medium,
            }
            if node.key.cache_salt is None:
                event = BlockStored(**event_args)
            else:
                event = BlockStoredWithMetadata(
                    **event_args,
                    metadata=BlockStoredMetadata(cache_salt=node.key.cache_salt),
                )
            self.enqueue(event)
            parent_block_hash = block_hash  # 下一个 page 链接到当前 page
            page_index += 1

    def record_remove(self, node, medium=None) -> None:
        if not self.enabled:
            return
        if medium is None:
            medium = StorageMedium.GPU
        event_hash_values = self._node_event_hash_values(node)
        block_hashes = []
        for start in range(0, len(node.key), self.page_size):
            end = min(start + self.page_size, len(node.key))
            if end <= start:
                continue
            block_hashes.append(hash_str_to_int64(event_hash_values[page_index]))
            page_index += 1
        if block_hashes:
            self.enqueue(BlockRemoved(block_hashes=block_hashes, medium=medium))

    def record_all_cleared(self) -> None:
        if not self.enabled:
            return
        self.enqueue(AllBlocksCleared())

    def take(self) -> list:
        """Atomically takes all events and clears the queue."""
        if not self.enabled:
            return []
        events = self._queue
        self._queue = []
        return events
```

**Hash 计算链路**：

```
RadixTree Node
    │
    ├── node.key.token_ids  → 原始 token ID 序列
    │
    ├── compute_node_hash_values(node, page_size)
    │   → 按 page_size 切片计算每个 page 的 hash
    │   → node.hash_value = [hash_page_0, hash_page_1, ...]
    │
    ├── compute_node_event_hash_values(node, page_size)
    │   → 如果 cache_salt 存在，重新计算加盐 hash
    │   → node.event_hash_value = [salted_hash_0, salted_hash_1, ...]
    │
    └── hash_str_to_int64(hash_value)
        → 将 hash 字符串转为 int64 用于 wire format
```

**与 vLLM 的核心差异**：
- vLLM 的 hash 计算在 engine 内部完成，通过 `PYTHONHASHSEED=0` 保证一致性
- SGLang 的 hash 通过 `compute_node_hash_values` + `compute_node_event_hash_values` 计算，支持 `cache_salt` 加盐
- SGLang 支持 bigram page（token pair），vLLM 不支持
- SGLang 的事件合并（coalescing）逻辑在 `enqueue` 中实现，vLLM 在 scheduler 内部

### 13.5 SGLang 事件产生链路

```python
# 1. Scheduler 每次调度后发布事件
# scheduler_components/kv_events_publisher.py:96-103
def publish_kv_events(self):
    events = self.tree_cache.take_events()  # 从 RadixCache 取录制的事件
    if events:
        batch = KVEventBatch(ts=time.time(), events=events)
        self.kv_event_publisher.publish(batch)

# 2. RadixCache 内的 KVCacheEventRecorder 录制事件
# mem_cache/events.py:115-161
def record_store(self, node, medium=StorageMedium.GPU):
    # 按 page_size 切片，生成 BlockStored 事件
    # block_hash = hash_str_to_int64(node.hash_value)
    # parent_block_hash = 父节点的最后一个 page hash
    event = BlockStored(block_hashes=[block_hash], parent_block_hash=...,
                        token_ids=page_tokens, block_size=..., medium=medium)
    self.enqueue(event)

def record_remove(self, node, medium=StorageMedium.GPU):
    self.enqueue(BlockRemoved(block_hashes=block_hashes, medium=medium))
```

### 13.6 SGLang Rust Router 的 KV 事件订阅

SGLang 的 `sgl-router`（Rust 实现）有完整的 KV 事件订阅和索引系统：

```rust
// experimental/sgl-router/src/policies/kv_events/subscriber.rs
//! Per-worker, per-DP-rank ZMQ subscriber for SGLang's `ZmqEventPublisher`.
//!
//! Wire format (3-frame multipart):
//! 1. topic_bytes
//! 2. seq_bytes — 8-byte big-endian i64
//! 3. payload — msgpack-encoded KvEventBatch

// 每个 (worker_url, dp_rank) 对应一个独立 ZMQ SUB task
// 通过 mpsc channel 转发给 KvEventIndex

// 模块结构:
// - wire.rs     — msgpack 解码
// - hash.rs     — block hash 计算（与 SGLang RadixKey.hash_page 一致）
// - tree.rs     — hash-keyed radix tree（路由查询用）
// - subscriber.rs — ZMQ SUB 订阅
// - discovery.rs  — /server_info 解析获取 publisher endpoint
// - index.rs    — 公共接口，整合 tree + subscriber + pump
```

### 13.7 SGLang KVEventsConfig

```python
class KVEventsConfig(BaseModel):
    """Configuration for KV event publishing."""
    publisher: str = "null"          # "null" 或 "zmq"
    endpoint: str = "tcp://*:5557"  # ZMQ PUB bind endpoint
    replay_endpoint: Optional[str] = None  # ZMQ ROUTER replay endpoint
    buffer_steps: int = 10_000      # replay 缓存步数
    hwm: int = 100_000             # ZMQ high water mark
    max_queue_size: int = 100_000   # 事件队列最大长度
    topic: str = ""                 # ZMQ topic

    @classmethod
    def from_cli(cls, cli_value: str) -> "KVEventsConfig":
        """从 CLI 参数解析配置（JSON 字符串）。"""
        return KVEventsConfig.model_validate_json(cli_value)

class EventPublisherFactory:
    _registry = {
        "null": NullEventPublisher,
        "zmq": ZmqEventPublisher,
    }

    @classmethod
    def create(cls, config: Optional[str], attn_dp_rank: int = 0):
        if not config:
            return NullEventPublisher()
        config = KVEventsConfig.from_cli(config)
        kind = config.model_dump().pop("publisher", "null")
        return cls._registry[kind](attn_dp_rank=attn_dp_rank, **config_dict)
```

**Ray Serve KVAwareRouter 如何使用**：`configure_kv_events_for_kv_routing()` 将 `kv_events_config` 注入到 vLLM 的 engine_kwargs 中，vLLM 内部解析为 `KVEventsConfig`（vLLM 版本）并创建 `ZmqEventPublisher`。SGLang 有完全相同的 `KVEventsConfig` 类和 `EventPublisherFactory`，因此 **Dynamo SelectionService 直接订阅 SGLang publisher 是零改动**。

### 13.8 SGLang vs vLLM vs Dynamo Router 选择

| | Dynamo Selection Service | SGLang sgl-router |
|---|---|---|
| **语言** | Python（内嵌 Ingress）+ Rust PyO3 | Rust |
| **订阅方式** | Ray Serve transport 层中转 | 直接 ZMQ SUB |
| **KV 索引** | Dynamo 内部 Rust 实现 | Rust `KvEventIndex` + `HashTree` |
| **与 Ray Serve 集成** | ✅ 原生集成 | ❌ 独立服务 |
| **与 SGLang 集成** | ✅ 协议兼容 | ✅ 原生集成 |
| **性能** | Python 限制 + Rust 评分 | Rust 高性能 |

---

## 14. KVAwareRouter 对 SGLang 的支持现状与适配路径

### 14.1 当前状态：仅支持 vLLM

Ray 上游的 KVAwareRouter **不支持 SGLang**。所有 engine 特定代码都硬依赖 vLLM：

| 模块 | vLLM 硬依赖 | 硬依赖的具体 import | 说明 |
|------|------------|---------------------|------|
| `vllm/kv_events.py` | ✅ | `from ray.llm...` (无 vLLM import) | 配置层，**最低依赖** |
| `vllm/tokenizer.py` | ✅ | `from vllm.renderers import OnlineRenderer`, `from vllm.entrypoints...` | **最重依赖** |
| `vllm/token_tracking.py` | ✅ | `from vllm.outputs import RequestOutput`, `from vllm.v1.engine.async_llm import AsyncLLM` | **中等依赖** |
| `vllm/prompt_token_forwarding.py` | ✅ | 无直接 vLLM import（通过 monkey-patch） | **间接依赖** |

**没有 `sglang/` 目录**。

### 14.2 Engine 无关层分析

以下组件是 **engine 无关** 的，SGLang 适配不需要改动：

| 组件 | 文件 | 说明 |
|------|------|------|
| **KVAwareRouter** | `kv_aware_router.py` | 路由入口，通过 `KVTokenTracker.select_worker()` 评分 |
| **KVTokenTracker** | `kv_token_tracker.py` | 评分 + 生命周期跟踪，内部持有 Dynamo SelectionService |
| **TokenSender** | `token_channel.py` | ZMQ PUSH sender，fire-and-forget |
| **TokenReceiver** | `token_channel.py` | ZMQ PULL receiver + TokenStore |
| **TokenStore** | `token_channel.py` | token 缓存（TTL + 容量淘汰） |
| **encode/decode** | `token_channel.py` | uint32 LE 编解码 |
| **constants** | `constants.py` | 端口/超时常量 |
| **ReservationBroadcast** | `kv_token_tracker.py` | 跨 Ingress 预留广播 |
| **LifecycleEventForwarder** | `vllm/token_tracking.py` | ⚠️ 有 vLLM 类型但接口通用 |

### 14.3 第一层适配：KV 事件配置（改动量：小）

vLLM 侧的 `configure_kv_events_for_kv_routing()` 实际上**没有直接 import vLLM**，它只是向 `llm_config.engine_kwargs` 注入配置字典：

```python
# vLLM 版本: kv_aware/vllm/kv_events.py
def configure_kv_events_for_kv_routing(llm_config: LLMConfig):
    llm_config.update_engine_kwargs(
        kv_events_config={
            "enable_kv_cache_events": True,
            "publisher": "zmq",
            "endpoint": _default_kv_events_endpoint(llm_config),      # tcp://*:5557
            "replay_endpoint": _default_kv_events_replay_endpoint(llm_config), # tcp://*:6557
        }
    )
    _configure_runtime_env_for_kv_routing(llm_config)
```

SGLang 适配需要新增 `kv_aware/sglang/kv_events.py`，核心差异：

| | vLLM | SGLang |
|---|---|---|
| **配置注入位置** | `llm_config.engine_kwargs["kv_events_config"]` | 同（Ray Serve 控制层相同） |
| **PYTHONHASHSEED** | `=0`（vLLM 默认按进程 salt） | 不需要（SGLang 的 hash 计算不依赖 PYTHONHASHSEED） |
| **VLLM_USE_SIMPLE_KV_OFFLOAD** | `"0"`（禁用简单 offload） | 不需要（SGLang 无此配置） |
| **端口偏移** | `assign_replica_kv_events_endpoint()` | 相同逻辑 |
| **routing_stats 上报** | `get_kv_event_routing_stats()` | 相同逻辑 |
| **token channel endpoint** | `get_token_channel_endpoints()` | 相同逻辑 |

**SGLang 适配伪代码**：

```python
# 需要新增: kv_aware/sglang/kv_events.py
def configure_kv_events_for_kv_routing(llm_config: LLMConfig):
    llm_config.update_engine_kwargs(
        kv_events_config={
            "enable_kv_cache_events": True,
            "publisher": "zmq",
            "endpoint": _default_kv_events_endpoint(llm_config),
            "replay_endpoint": _default_kv_events_replay_endpoint(llm_config),
        }
    )
    # SGLang 不需要 PYTHONHASHSEED 和 VLLM_USE_SIMPLE_KV_OFFLOAD
    # SGLang 的 block hash 通过 compute_node_hash_values() 计算，不依赖 Python hash seed

# assign_replica_kv_events_endpoint() — 完全复用 vLLM 版本
# resolve_kv_event_source_endpoint() — 完全复用 vLLM 版本
# get_kv_event_routing_stats() — 完全复用 vLLM 版本
# get_token_channel_endpoints() — 完全复用 vLLM 版本
```

**关键点**：SGLang 已有 `KVCacheEventRecorder` + `ZmqEventPublisher` + `KVEventsConfig`，Dynamo SelectionService 可以直接订阅，**这是零改动就能工作的部分**。

### 14.4 第二层适配：Tokenizer（改动量：大，最难的适配）

vLLM 版本的 Tokenizer 使用 vLLM 的 `OnlineRenderer` 在 Ingress 进程内做预分词：

```python
# vLLM 版本: kv_aware/vllm/tokenizer.py
class Tokenizer:
    def __init__(self, llm_config: LLMConfig):
        engine_config = llm_config.get_engine_config()
        _, vllm_config = _get_vllm_engine_config(llm_config, device_type="cpu")
        self._model_config = vllm_config.model_config

        frontend_args = FrontendArgs(**engine_config.frontend_kwargs)
        self._renderer = OnlineRenderer(
            self._model_config,
            renderer_from_config(vllm_config),        # ← vLLM 独有
            chat_template=load_chat_template(...),    # ← vLLM 独有
            trust_request_chat_template=...,
            enable_auto_tools=...,
            tool_parser=...,
        )

    async def tokenize(self, payload: Dict[str, Any]) -> Optional[List[int]]:
        request = build_tokenize_request(payload)
        if isinstance(request, ChatCompletionRequest):
            rendered_inputs = await self._render_chat(request)  # ← vLLM chat template
        else:
            rendered_inputs = await self._render_completion(request)

        input_ids = []
        for rendered_input in rendered_inputs:
            components = extract_prompt_components(  # ← vLLM 独有
                self._model_config, rendered_input
            )
            if components.token_ids is not None:
                input_ids.extend(components.token_ids)
        return input_ids
```

**SGLang 适配需要实现**：

```python
# 需要新增: kv_aware/sglang/tokenizer.py
# 核心挑战：SGLang 没有 OnlineRenderer，需要用 SGLang 的 tokenizer_manager 做同样的事

class SGLangTokenizer:
    def __init__(self, llm_config: LLMConfig):
        # SGLang 的 tokenizer 初始化路径：
        # 1. 从 llm_config 获取 model path
        # 2. 加载 tokenizer（SGLang 用 transformers AutoTokenizer）
        # 3. 初始化 chat template（SGLang 用 jinja2 template）
        from sglang.srt.hf_transformers_utils import get_tokenizer
        from sglang.srt.conversation import chat_templates

        self._tokenizer = get_tokenizer(
            llm_config.model_config.model,
            tokenizer_mode=llm_config.engine_kwargs.get("tokenizer_mode", "auto"),
            trust_remote_code=llm_config.engine_kwargs.get("trust_remote_code", False),
        )
        # 加载 chat template
        self._chat_template = self._load_chat_template(llm_config)

    async def tokenize(self, payload: Dict[str, Any]) -> Optional[List[int]]:
        # 1. 解析请求 body → build_tokenize_request()（engine 无关，复用）
        request = build_tokenize_request(payload)
        if request is None:
            return None

        # 2. 如果是 chat request → 渲染 chat template → tokenize
        if isinstance(request, ChatCompletionRequest):
            # SGLang 的 chat template 渲染路径：
            #   conversation.py → apply_chat_template() → jinja2
            #   → 得到 text prompt → self._tokenizer.encode(text)
            prompt_text = self._render_chat(request)
            return self._tokenizer.encode(prompt_text)

        # 3. 如果是 completion request → 直接 tokenize
        return self._tokenizer.encode(request.prompt)
```

**难点分析**：

| 难点 | vLLM | SGLang | 解决思路 |
|------|------|--------|----------|
| **Chat template 渲染** | `OnlineRenderer.render_chat()` | `conversation.py` + jinja2 | 需要对齐 template 逻辑 |
| **Tool calling** | `enable_auto_tools` + `tool_parser` | `sglang.srt.openai_api.protocol` | 需处理 tool message 格式 |
| **Multi-modal** | `extract_prompt_components()` | SGLang `EmbeddingContent` | vLLM 也不支持多模态 tokenize |
| **Tokenizer 一致性** | vLLM 内部 `TokenizerGroup` | SGLang `get_tokenizer()` | 必须用同版本 tokenizer |

**最小可用实现**：可以只支持 text completion + chat template，不支持 tool calling 和 multi-modal。这与 vLLM 版本的当前限制一致（代码注释 `TODO: Support multimodal chat`）。

### 14.5 第三层适配：生命周期 Hook（改动量：中）

vLLM 版本通过装饰 `AsyncLLM.generate()` 来追踪请求生命周期：

```python
# vLLM 版本: kv_aware/vllm/token_tracking.py
def enable_token_tracking(engine_cls, report_decode_progress=False):
    class TokenTrackingEngine(engine_cls):
        async def generate(self, prompt, sampling_params, request_id, *args, **kwargs):
            stream = super().generate(...)
            forwarder = self._resolve_lifecycle_forwarder()

            tracker = RequestTokenTracker(forwarder, request_id)
            try:
                async for output in stream:
                    tracker.on_output(output)  # ← 统计 output token
                    yield output
            finally:
                tracker.finish()  # ← on_request_completed

    return TokenTrackingEngine
```

**vLLM 的关键 hook 点**：
- `on_prefill_complete`：第一个 output token 出现时
- `on_decode_progress`：每个 DELTA chunk 的累计 output token 数
- `on_request_completed`：stream 结束时

**SGLang 适配需要实现**：

```python
# 需要新增: kv_aware/sglang/token_tracking.py
# SGLang 的 stream 输出结构与 vLLM 不同

# SGLang 的 generate 流式输出接口：
# sglang/srt/managers/scheduler.py → Scheduler.generate()
# → 返回 Generator[str, None, None] 或类似的 async generator
# → 每个 chunk 包含: delta_text, delta_token_ids, finished

class SGLangRequestTokenTracker:
    """Drives request lifecycle hooks for one SGLang generate() stream."""

    def __init__(self, forwarder, request_id, report_decode_progress=False):
        self._forwarder = forwarder
        self._request_id = request_id
        self._cumulative = 0
        self._prefill_marked = False
        self._finished = False
        self._report_decode_progress = report_decode_progress

    def on_output(self, output_token_ids: list[int]) -> None:
        """SGLang 每个 decode step 输出 token IDs。"""
        step_tokens = len(output_token_ids)
        if step_tokens == 0:
            return
        self._cumulative += step_tokens

        if not self._prefill_marked:
            self._prefill_marked = True
            self._forwarder.report("on_prefill_complete", self._request_id)

        if self._report_decode_progress:
            self._forwarder.report(
                "on_decode_progress", self._request_id, self._cumulative
            )

    def finish(self) -> None:
        if not self._finished:
            self._finished = True
            self._forwarder.report("on_request_completed", self._request_id)

def enable_sglang_token_tracking(scheduler_cls, report_decode_progress=False):
    """装饰 SGLang 的 Scheduler，添加 KV-router 生命周期跟踪。

    关键区别：SGLang 的 scheduler 不是 async generator，而是通过
    batch schedule → step → output 循环驱动。需要 hook 的位置是
    每个 request 的 output 产生点。
    """

    class TokenTrackingScheduler(scheduler_cls):
        _lifecycle_forwarder: Optional[LifecycleEventForwarder] = None

        def _resolve_lifecycle_forwarder(self):
            if self._lifecycle_forwarder is None:
                try:
                    handle = get_llm_router_handle()
                    worker_id = get_worker_id(
                        serve.get_replica_context().replica_id.unique_id
                    )
                    self._lifecycle_forwarder = LifecycleEventForwarder(
                        handle, worker_id
                    )
                except Exception:
                    ...
            return self._lifecycle_forwarder

        # Hook 点需要根据 SGLang 版本确定，可能的位置：
        # 1. Scheduler.process_batch() → 每次输出后检查 output tokens
        # 2. Scheduler.handle_finished_request() → 请求完成时
        # 3. 类似 vLLM，装饰 generate 方法

    return TokenTrackingScheduler
```

**SGLang 与 vLLM 生命周期对比**：

| 生命周期事件 | vLLM 触发点 | SGLang 触发点 |
|-------------|-------------|---------------|
| `on_prefill_complete` | `RequestOutput` 的第一个 output token | Scheduler 第一次 decode output 后 |
| `on_decode_progress` | 每个 DELTA chunk 的 `token_ids` 长度 | 每个 decode step 的 output token 数 |
| `on_request_completed` | `generate()` stream 结束 (`finally`) | `handle_finished_request()` 或类似 |

**核心挑战**：SGLang 的 Scheduler 是 batch 驱动的（一次处理多个请求的 step），不是 per-request async generator。需要在 batch 输出后遍历每个请求的增量 token，逐个报告生命周期事件。

### 14.6 Prompt Token Forwarding 适配

vLLM 版本的 `prompt_token_forwarding.py` 实际上**没有直接 import vLLM**，它通过 monkey-patch vLLM 的 serving 方法来注入 token IDs：

```python
# vLLM 版本: vllm/prompt_token_forwarding.py
def inject_prompt_token_ids(request, raw_request, store: TokenStore):
    token_key = raw_request.headers.get(KV_TOKEN_KEY_HEADER)
    if not token_key:
        return
    entry = store.pop(token_key)
    if entry is None:
        return
    token_ids = decode_prompt_token_ids(entry.payload)
    # 注入到 vLLM 特定字段
    kv_transfer_params = getattr(request, "kv_transfer_params", None)
    if not isinstance(kv_transfer_params, dict):
        kv_transfer_params = {}
        request.kv_transfer_params = kv_transfer_params
    kv_transfer_params["prompt_token_ids"] = token_ids
```

**SGLang 适配**：需要找到 SGLang 等价的注入点。SGLang 的请求处理路径与 vLLM 不同：

```python
# SGLang 的请求处理路径:
# sglang/srt/openai_api/adapter.py
#   → OpenAIServing.chat_completion() 或 completion()
#   → Scheduler.handle_request()
#   → 内部 tokenize

# 适配方案: hook SGLang 的 serving 方法，注入 pre-tokenized IDs
# 需要新增: kv_aware/sglang/prompt_token_forwarding.py

def inject_prompt_token_ids_sglang(request, raw_request, store: TokenStore):
    """SGLang 版本的 token 注入。"""
    token_key = raw_request.headers.get(KV_TOKEN_KEY_HEADER)
    if not token_key:
        return
    entry = store.pop(token_key)
    if entry is None:
        return
    token_ids = decode_prompt_token_ids(entry.payload)
    # SGLang 的注入点需要找到：
    # 可能是 request.input_ids 或类似字段
    # 需要调研 SGLang 的请求对象结构
    request.pre_tokenized_ids = token_ids  # 伪代码，需要确认 SGLang 端字段

def install_prompt_token_forwarding_sglang(state, store: TokenStore):
    """Monkey-patch SGLang 的 serving 方法。"""
    for attr_name, method_name in (
        ("openai_serving_chat", "chat_completion"),
        ("openai_serving_completion", "completion"),
    ):
        _install_prompt_token_forwarding(
            getattr(state, attr_name, None), method_name, store,
        )
```

### 14.7 完整适配架构图

```
kv_aware/                              ← 现有 engine 无关层
├── kv_aware_router.py                 ← 不变
├── kv_token_tracker.py                ← 不变
├── token_channel.py                   ← 不变
├── constants.py                       ← 不变
├── utils.py                           ← 需修改：增加 engine type 分发
├── vllm/                              ← 现有 vLLM 适配层
│   ├── kv_events.py                   ← 不变
│   ├── tokenizer.py                   ← 不变
│   ├── token_tracking.py              ← 不变
│   └── prompt_token_forwarding.py     ← 不变
└── sglang/                            ← 新增 SGLang 适配层
    ├── kv_events.py                   ← 新增（小改动，复用端口逻辑）
    ├── tokenizer.py                   ← 新增（大改动，SGLang tokenizer）
    ├── token_tracking.py             ← 新增（中改动，hook SGLang scheduler）
    └── prompt_token_forwarding.py     ← 新增（小改动，注入点不同）

utils.py 需要修改：
┌──────────────────────────────────────────────────────────┐
│ def _maybe_setup_kv_aware_routing(deployment_options, llm_config): │
│     if not is_kv_aware(llm_config):                     │
│         return                                           │
│     engine_type = llm_config.engine_type                │
│     if engine_type == "vllm":                            │
│         from .vllm.kv_events import configure_kv_events │
│     elif engine_type == "sglang":                        │
│         from .sglang.kv_events import configure_kv_events │
│     configure_kv_events(llm_config)                      │
└──────────────────────────────────────────────────────────┘
```

### 14.8 适配工作量总结

| 层 | vLLM 支持 | SGLang 支持 | 改动量 | 详细说明 |
|----|-----------|-------------|--------|----------|
| **KV 事件传输** (ZMQ) | ✅ | ✅ (协议兼容) | 低 | 只需配置 |
| **Dynamo SelectionService** | ✅ | ✅ (engine 无关) | 无 | 无需改动 |
| **Token Channel** | ✅ | ✅ (engine 无关) | 无 | Sender/Receiver/Store 通用 |
| **KV 事件配置** | ✅ | ❌ 需新增 `sglang/kv_events.py` | 小 | 复用端口逻辑，去掉 PYTHONHASHSEED |
| **Tokenizer** | ✅ | ❌ 需新增 `sglang/tokenizer.py` | 大 | 需用 SGLang tokenizer 替代 OnlineRenderer |
| **生命周期 hook** | ✅ | ❌ 需新增 `sglang/token_tracking.py` | 中 | hook SGLang scheduler 的 batch 输出 |
| **Prompt Token 注入** | ✅ | ❌ 需新增 `sglang/prompt_token_forwarding.py` | 小-中 | 注入点不同，需确认 SGLang 请求对象 |
| **utils.py 分发** | ✅ | ❌ 需修改 | 小 | 按 engine_type 分发到对应适配层 |

### 14.9 当前 SGLang 引擎的最佳路由方案

**如果使用 SGLang 引擎，当前最实际的路由方案还是 `PrefixCacheAffinityRouter`（字符级前缀匹配）。**

| 方案 | 精度 | 依赖 | SGLang 支持 | 推荐度 |
|------|------|------|-------------|--------|
| **PrefixCacheAffinityRouter** | 近似（字符级前缀） | 无 | ✅ 已支持 | ✅ 当前最佳 |
| **KVAwareRouter + vLLM** | 精确（Token Block） | ai-dynamo | ❌ 仅 vLLM | — |
| **KVAwareRouter + SGLang 适配** | 精确（Token Block） | ai-dynamo | ❌ 需开发 | 未来可选 |
| **SGLang sgl-router (Rust)** | 精确（Token Block） | sgl-router | ✅ 原生 | ⚠️ 独立部署 |

### 14.10 适配优先级建议

1. **Phase 1**（1-2 天）：`sglang/kv_events.py` — 最简单，端口复用 + 去掉 PYTHONHASHSEED。此阶段 SGLang 引擎已能发出 KV 事件，Dynamo SelectionService 可以接收，但没有 pre-routing tokenize 所以路由只能退化为 load-balance。

2. **Phase 2**（3-5 天）：`sglang/tokenizer.py` — 最关键，实现 SGLang tokenizer 后 KVAwareRouter 才能根据 token overlap 评分。可以先只支持 text completion + chat template，不支持 tool calling。

3. **Phase 3**（2-3 天）：`sglang/token_tracking.py` + `sglang/prompt_token_forwarding.py` — 让 SelectionService 获得实时负载跟踪，避免过预留。没有此层功能上可用，但负载跟踪不精确。

4. **Phase 4**（1 天）：`utils.py` engine type 分发 + 集成测试
