# Graphify 使用指南

本项目已配置 Graphify 知识图谱，用于代码探索、调用关系分析和架构理解。

---

## 安装与配置

### 环境要求

- **Python**: 需要 Anaconda/Miniconda 环境（本项目使用 `/opt/homebrew/anaconda3/bin/python`）
- **操作系统**: macOS / Linux
- **磁盘空间**: 首次构建需约 250MB（graph.json ~80MB + cache ~141MB）
- **无需 API Key**: 代码 AST 提取完全本地执行，不消耗任何 API 额度

### 安装步骤

```bash
# 1. 安装 graphify（注意：PyPI 包名是 graphifyy，双写 y）
pip install graphifyy

# 2. 验证安装
graphify --version  # 应显示 0.7.16+

# 3. 安装 Claude Code 技能（可选，用于 /graphify 命令）
graphify install

# 4. 配置 Claude Code 集成（可选，让 Claude 自动参考图谱）
graphify claude install
```

拿到图谱后，让助手始终优先使用图谱。在项目根目录执行 graphify claude install命令：
Claude Code主要做一件事，在CLAUDE.md中写入一段规则，告诉Claude在回答架构问题前先读graphify-out/GRAPH_REPORT.md。


可以继续用查询命令：
graphify query "show the auth flow"
graphify path "UserService" "DatabasePool"
graphify explain "RateLimiter"
测试一下，让它给我介绍一下项目的线程池模型：

图片
可看到它没有走grep命令，而是直接查询graphify-out中已经建立好的知识图谱。

如果团队要一起用，官方建议把 graphify-out/ 提交到 git。一个人生成图谱并提交，其他人拉下来后，AI 助手就可以直接复用这份结构化上下文。

Graphify 也提供 git hook：

graphify hook install
官方说明里提到，这可以在 commit 后自动重建图谱，并设置 graph.json 的 merge driver，降低多人协作时图谱文件冲突的风险。

### 首次初始化（已完成，新环境需重做）

```bash
# 1. 创建 .graphifyignore（排除无关文件，加速扫描）
#    已创建在项目根目录，排除了 node_modules、build artifacts、CI 等

# 2. 构建 AST 知识图谱（纯本地，无 API 费用）
graphify update .
# 耗时约 3 分钟，扫描 5,256 个代码文件

# 3. 安装 git hooks（每次 commit 自动增量更新）
graphify hook install

# 4. 生成可视化文件
graphify export callflow-html  # 调用流程图
graphify tree                   # 文件层级树

# 5.（可选）语义提取 docs/ 目录（需要 LLM API）
#    在 Claude Code 会话中运行 /graphify . 即可
```

### 常见安装问题

| 问题 | 原因 | 解决 |
|------|------|------|
| `pip install graphify` 找不到包 | 包名是 `graphifyy`（双写 y） | `pip install graphifyy` |
| `graphify: command not found` | PATH 中没有 anaconda | 确保 `/opt/homebrew/anaconda3/bin` 在 PATH 中 |
| `python3 -c "import graphify"` 报错 | graphify 安装在 conda 环境中 | 用 `/opt/homebrew/anaconda3/bin/python` |
| `graphify extract` 报缺少 API key | 语义提取需要 LLM | 用 `graphify update .` 做 AST 提取（无需 API） |
| `/graphify` 命令不识别 | 需要重启 Claude Code 会话 | 退出并重新进入会话 |

---

## 快速开始：日常工作流

### 1. 正常写代码，图谱自动更新

每次 `git commit` 后，git hook 会自动在后台重建图谱（纯本地 AST，无 API 费用，几秒完成）。你不需要手动做任何事。

### 2. 想看调用关系时

```bash
# 最直观：浏览器打开调用流程图
open graphify-out/kray-callflow.html

# 或者 CLI 查询
graphify path "StreamingExecutor" "ResourceManager"
graphify explain "MapOperator"
```

### 3. 想问代码问题时

在 Claude Code 中直接问即可，Claude 会自动参考知识图谱：
```
"StreamingExecutor 是怎么调用 ResourceManager 的？"
"GCS 线程模型是什么？"
```

或者用 CLI：
```bash
graphify query "how does backpressure work" --dfs
```

---

## 查看代码调用关系

### 方式 1：浏览器打开 Callflow HTML（最直观）

```bash
open graphify-out/kray-callflow.html
```

包含：
- **Mermaid 调用流程图**：按模块/社区自动分组，展示函数间调用链
- **调用表格**：列出每个关键函数的 caller 和 callee
- **交互式缩放/平移**：可放大查看细节

### 方式 2：浏览器打开 Tree HTML

```bash
open graphify-out/GRAPH_TREE.html
```

D3 可折叠树形视图，按文件层级展示代码结构，点击节点可查看其关联的调用关系。

### 方式 3：CLI 查询特定调用路径

```bash
# 查询两个组件间的最短调用路径
graphify path "StreamingExecutor" "ResourceManager"

# 查询某个节点的所有连接（包括调用关系）
graphify explain "GcsServer"

# 自然语言查询
graphify query "what calls StreamingExecutor.execute"
graphify query "how does backpressure propagate from operator to executor"

# DFS 深度优先搜索（追踪完整调用链）
graphify query "trace the scheduling path from task submission to worker" --dfs
```

### 方式 4：在 Claude Code 中直接提问

由于已配置 `graphify claude install`，Claude Code 会自动参考知识图谱回答代码问题。

---

## 本地文件说明与存储策略

### 提交到 git 的文件（可直接查看）

| 文件 | 用途 |
|------|------|
| `graphify-out/GRAPH_REPORT.md` | 图谱报告，可直接阅读 |
| `graphify-out/kray-callflow.html` | 浏览器打开看调用流程 |
| `graphify-out/GRAPH_TREE.html` | 浏览器打开看层级树 |
| `docs/graphify-usage-guide.md` | 本文档 |

### 保留在本地的文件（已加入 .gitignore，不要删除）

| 文件 | 大小 | 作用 | 删了会怎样 |
|------|------|------|------------|
| `graphify-out/graph.json` | ~80MB | 完整知识图谱，所有查询依赖它 | 需重跑 `graphify update .`（~3 分钟，无 API 费） |
| `graphify-out/cache/` | ~141MB | AST 解析缓存 | 每次更新都要重新解析 5000+ 文件；保留则只解析变更文件（几秒） |
| `graphify-out/manifest.json` | ~1MB | 文件时间戳，用于增量更新 | 增量判断失效，每次全量扫描 |
| `graphify-out/.graphify_*.json` | 各种 | 中间提取结果 | 需重新提取 |

### 重建成本说明

| 操作 | 需要 API？ | 耗时 |
|------|-----------|------|
| 删 cache 后重跑 `graphify update .`（代码 AST） | 不需要 | ~3 分钟 |
| 删 cache 后重跑语义提取（docs/ 文档） | 需要 LLM | ~1 分钟 |
| 保留 cache，增量更新 | 不需要 | 几秒 |
| git hook 自动更新（每次 commit） | 不需要 | 后台几秒 |

**结论**：`cache/` 和 `graph.json` 纯本地即可重建，不花 API 费。语义提取（53 个 docs/ 文档）需要 LLM 但量很小。磁盘不紧张就保留，紧张就删——随时无成本重建。

---

## 日常命令参考

### 更新图谱

```bash
# 代码变更后更新（git hook 已自动执行，通常不需手动跑）
graphify update .

# 强制全量重建（重构后使用）
graphify update . --force

# 只重新聚类（不重新提取）
graphify cluster-only .
```

### 查询命令

| 命令 | 用途 | 示例 |
|------|------|------|
| `graphify query "<问题>"` | BFS 广度查询 | `graphify query "auth flow"` |
| `graphify query "<问题>" --dfs` | DFS 深度追踪调用链 | `graphify query "task scheduling chain" --dfs` |
| `graphify path "A" "B"` | 两节点间最短路径 | `graphify path "Raylet" "ObjectStore"` |
| `graphify explain "X"` | 解释单个节点及所有连接 | `graphify explain "MapOperator"` |

### 重新生成可视化

```bash
# 调用流程图（Mermaid）
graphify export callflow-html

# 文件层级树
graphify tree

# 限制 section 数量（大图谱时加速渲染）
graphify export callflow-html --max-sections 5
```

### 语义提取（docs/ 文档更新后）

在 Claude Code 会话中运行：
```
/graphify .
```
或手动（需要 API key）：
```bash
graphify extract . --backend claude
```

---

## 输出文件结构

```
graphify-out/
├── graph.json              # [本地] 完整知识图谱（~80MB）- 所有查询依赖
├── GRAPH_REPORT.md         # [git] 图谱报告：god nodes、社区、惊人连接
├── kray-callflow.html      # [git] 调用流程图（浏览器打开）
├── GRAPH_TREE.html         # [git] D3 可折叠树形视图
├── manifest.json           # [本地] 文件清单（用于增量更新）
├── cache/                  # [本地] AST 缓存（加速后续提取）
│   └── ast/               # tree-sitter 解析缓存
├── .graphify_python        # [git] Python 解释器路径
├── .graphify_root          # [git] 项目根路径
├── .graphify_extract.json  # [本地] 最近一次完整提取结果
├── .graphify_ast.json      # [本地] AST 提取结果
└── .graphify_detect.json   # [本地] 文件检测结果
```

---

## 图谱统计

- **68,816 nodes**（代码符号 + 语义概念）
- **120,851 edges**（调用、引用、概念关联）
- **5,251 communities**（自动聚类的模块/子系统）
- **25 hyperedges**（跨模块的架构级关系）

### God Nodes（最高连接度节点）

| 节点 | 度 | 含义 |
|------|-----|------|
| range() | 2,482 | 全局使用的内置函数 |
| wait_for_condition() | 993 | 测试工具函数 |
| DataContext | 247 | Ray Data 全局上下文 |
| SampleBatch | 245 | RLlib 数据批次 |
| StreamingExecutor | 75 | Ray Data 流式执行引擎 |

---

## 自动化配置

### Git Hooks（已安装）

每次 `git commit` 后自动在后台重建图谱：
```bash
graphify hook status     # 检查状态
graphify hook uninstall  # 卸载
graphify hook install    # 重新安装
```

### Claude Code 集成（已配置）

- PreToolUse hook：Claude Code 读文件前自动查阅图谱
- CLAUDE.md 中已添加 graphify 使用规则
- 支持 `/graphify` slash command（需重启会话加载）

---

## 高级用法

### 编程访问 graph.json

```python
import json
from pathlib import Path

graph = json.loads(Path('graphify-out/graph.json').read_text())
nodes = graph['nodes']  # list of {id, label, community, ...}
links = graph['links']  # list of {source, target, relation, confidence, ...}

# 找到特定节点
streaming_exec = [n for n in nodes if 'StreamingExecutor' in n.get('label', '')]

# 找到所有调用关系
calls = [e for e in links if e.get('relation') == 'calls']

# 找到某个 community 的所有成员
community_38 = [n for n in nodes if n.get('community') == 38]
```

### MCP Server（持续查询）

```bash
# 启动 MCP server 供其他 agent 调用
python -m graphify.serve graphify-out/graph.json
```

### 跨项目全局图谱

```bash
graphify global add graphify-out/graph.json kray
graphify global list
graphify global path
```

### 添加外部资源到图谱

```bash
# 添加论文/URL
graphify add https://arxiv.org/abs/1706.03762

# 添加后自动更新图谱
graphify add https://some-doc-url --author "作者名"
```

---

## 工作原理

### 图谱构建流程

```
源代码文件                         docs/ 文档
    │                                  │
    ▼                                  ▼
┌─────────────┐                ┌──────────────┐
│ tree-sitter │                │  LLM 语义    │
│  AST 解析   │                │   提取       │
│（本地，免费）│                │（需要 API）  │
└─────────────┘                └──────────────┘
    │                                  │
    ▼                                  ▼
┌──────────────────────────────────────────┐
│         合并 → 聚类 → 生成图谱           │
│   68,816 nodes | 120,851 edges           │
│   5,251 communities | 25 hyperedges      │
└──────────────────────────────────────────┘
    │
    ▼
┌──────────────────────────────────────────┐
│  graph.json → 查询 / 可视化 / Claude集成 │
└──────────────────────────────────────────┘
```

### 两种提取模式对比

| 特性 | AST 提取（`graphify update`） | 语义提取（`/graphify .`） |
|------|-------------------------------|--------------------------|
| 提取对象 | .py / .cc / .java 等代码文件 | docs/ 下的 .md 文档 |
| 提取内容 | 函数定义、类、调用关系、导入 | 概念、架构关系、设计意图 |
| 技术手段 | tree-sitter 本地 AST 解析 | LLM 阅读文档后抽取知识 |
| 是否需要 API | 不需要 | 需要（Claude API） |
| 耗时 | ~3 分钟（全量）/ 几秒（增量） | ~1 分钟（53 个文档） |
| 自动化 | git hook 自动触发 | 需手动运行 |
| 节点数 | ~69,000（代码符号） | ~140（语义概念） |

### 节点与边的类型

**节点类型：**
- `function` - 函数/方法定义
- `class` - 类定义
- `module` - 模块/文件
- `concept` - 语义概念（来自 docs/ 提取）

**边类型：**
- `calls` - 函数调用关系
- `imports` - 模块导入
- `inherits` - 类继承
- `references` - 引用关系
- `relates_to` - 语义概念关联

### .graphifyignore 说明

项目根目录的 `.graphifyignore` 文件控制哪些文件/目录不会被扫描，语法类似 `.gitignore`。当前已排除：

- `bazel-*`、`python/build/` - 构建产物
- `python/ray/dashboard/client/node_modules/` - 前端依赖（111K+ 文件）
- `thirdparty/` - 第三方代码
- `.buildkite/`、`ci/`、`docker/` - CI 基础设施
- `release/` - 发布测试
- `*.png`、`*.so` 等 - 二进制文件

如需调整扫描范围，编辑此文件后运行 `graphify update . --force`。

---

## 语义提取详解

### 何时需要语义提取

当 `docs/` 目录下有新增或修改的文档时，需要重新运行语义提取来更新图谱中的概念节点。代码变更不需要——git hook 会自动处理。

### 执行方式

**方式 1：在 Claude Code 中运行（推荐）**

```
/graphify .
```

这会启动完整的提取管线，自动利用当前会话的 Claude API。

**方式 2：手动执行各步骤**

如果 `/graphify` 命令不可用，可在 Claude Code 中要求逐步执行：

```
请对 docs/ 目录进行 graphify 语义提取
```

Claude 会：
1. 检测 docs/ 下的文件变更
2. 将文档分块（每块约 20 个文件）
3. 用 LLM 提取概念节点和关系
4. 合并到已有 AST 图谱
5. 重新聚类生成最终 graph.json

### 本项目语义提取结果

当前已提取 `docs/` 目录下 53 个文档，产生：
- **140 个语义节点**：GCS Server、Raylet、调度策略、反压机制、Worker 生命周期等
- **178 条语义边**：组件间的架构关系
- **25 个超边**：跨模块的系统级关联

---

## 故障排除

| 问题 | 解决 |
|------|------|
| `graphify: command not found` | 确保 `/opt/homebrew/anaconda3/bin` 在 PATH 中 |
| 图谱过时 | `graphify update .`（通常 git hook 已自动处理） |
| HTML 渲染慢 | `graphify export callflow-html --max-sections 5` |
| 想看完整语义关系 | 在 Claude Code 中运行 `/graphify .` |
| graph.json 太大 | 增加 `.graphifyignore` 排除规则，然后 `graphify update . --force` |
| 查询结果为空 | 检查节点名拼写，用 `graphify explain "部分名称"` 模糊查找 |
| graph.json 被误删 | `graphify update .` 重建（~3 分钟，无 API 费） |
| cache/ 被误删 | `graphify update .` 重建（~3 分钟，无 API 费） |
| 语义节点丢失 | 在 Claude Code 中运行 `/graphify .` 重新提取 docs/ |
| 增量更新不生效 | 删除 `graphify-out/manifest.json` 后重跑 `graphify update .` |
| graphify 版本过低 | `pip install --upgrade graphifyy` |
