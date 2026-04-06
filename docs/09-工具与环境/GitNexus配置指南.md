# GitNexus 安装配置与使用指南

## 项目简介

GitNexus 是一个代码知识图谱工具，能将任意代码库索引为知识图谱（包含依赖关系、调用链、集群、执行流），并通过 MCP（Model Context Protocol）暴露给 AI agent，使其在编辑代码时不会遗漏依赖、破坏调用链。

- GitHub: https://github.com/abhigyanpatwari/GitNexus
- Web UI: https://gitnexus.vercel.app/
- npm: https://www.npmjs.com/package/gitnexus

---

## 安装过程

### 安装失败的原因

直接安装最新版时报错：

```bash
$ npm install -g gitnexus
npm error code ETARGET
npm error notarget No matching version found for @ladybugdb/core@^0.16.1.
```

**根本原因**：GitNexus 最新版（v1.6.4 及所有 rc 版本）在 `package.json` 中声明依赖 `@ladybugdb/core@^0.16.1`，但该包在 npm 上实际最高只发布到 `0.15.2-dev.1`，`0.16.x` 版本根本不存在。这是 GitNexus 上游的发布问题——提前声明了对尚未发布的依赖版本。

尝试的版本及结果：

| 版本 | 结果 |
|------|------|
| `gitnexus@latest` (1.6.4) | 失败 - 依赖 `@ladybugdb/core@^0.16.1` 不存在 |
| `gitnexus@rc` (1.6.5-rc.25) | 失败 - 同上 |
| `gitnexus@1.6.3` | 失败 - 依赖 `@ladybugdb/core@^0.15.2` 不存在 |
| `gitnexus@1.6.0` | 失败 - 同上 |
| `gitnexus@1.5.0` | 失败 - 同上 |
| **`gitnexus@1.3.8`** | **成功** |

### 解决办法

回退到不依赖该版本的旧版本 v1.3.8：

```bash
npm install -g gitnexus@1.3.8
```

安装输出有一些 deprecated 警告但不影响使用：

```
npm warn deprecated npmlog@6.0.2: This package is no longer supported.
npm warn deprecated kuzu@0.11.3: Package no longer supported.
added 286 packages in 45s
```

验证安装：

```bash
$ gitnexus --version
1.3.8
```

> **注意**：如果将来 `@ladybugdb/core@0.16.x` 发布了，可以再升级到最新版：`npm install -g gitnexus@latest`

---

## 配置过程

### 1. 运行 gitnexus setup

```bash
$ gitnexus setup

  GitNexus Setup
  ==============

  Claude Code detected. Run this command to add GitNexus MCP:
    claude mcp add gitnexus -- npx -y gitnexus mcp

  Configured:
    + Claude Code (MCP manual step printed)
    + Claude Code skills (6 skills → ~/.claude/skills/)
    + Claude Code hooks (PreToolUse)

  Skipped:
    - Cursor (not installed)
    - OpenCode (not installed)
```

setup 自动完成了：
- 检测到 Claude Code 并打印 MCP 配置命令
- 安装了 6 个 Claude Code skills 到 `~/.claude/skills/`
- 配置了 PreToolUse hooks

### 2. 添加 MCP Server 到 Claude Code

由于已经全局安装了 gitnexus，直接使用全局命令路径（比 npx 启动更快）：

```bash
$ claude mcp add gitnexus -- gitnexus mcp
Added stdio MCP server gitnexus with command: gitnexus mcp to local config
```

MCP 配置写入 `~/.claude.json`，内容为：

```json
{
  "gitnexus": {
    "type": "stdio",
    "command": "gitnexus",
    "args": ["mcp"],
    "env": {}
  }
}
```

### 3. 索引项目

在项目根目录运行 analyze：

```bash
$ gitnexus analyze /Users/shiyanpeng/Desktop/kaiworks/kray

  GitNexus Analyzer
  Skipped 12 large files (>512KB, likely generated/vendored)

  Repository indexed successfully (32.8s)

  77,158 nodes | 125,889 edges | 9113 clusters | 0 flows
  KuzuDB 14.4s | FTS 11.9s | Embeddings off (use --embeddings to enable)
  /Users/shiyanpeng/Desktop/kaiworks/kray
  Context: AGENTS.md (created), CLAUDE.md (appended), .claude/skills/gitnexus/ (6 skills)
```

索引结果：

| 指标 | 数值 |
|------|------|
| 文件数 | 8,440 |
| 符号节点 | 77,158 |
| 关系边 | 125,889 |
| 功能集群 | 9,113 |
| 执行流 | 0 |
| 索引耗时 | 32.8s |

### 4. 验证索引状态

```bash
$ gitnexus status
Repository: /Users/shiyanpeng/Desktop/kaiworks/kray
Indexed: 5/13/2026, 11:15:13 PM
Indexed commit: 3c4cd95
Current commit: 3c4cd95
Status: ✅ up-to-date

$ gitnexus list
  Indexed Repositories (1)

  kray
    Path:    /Users/shiyanpeng/Desktop/kaiworks/kray
    Indexed: 5/13/2026, 11:15:13 PM
    Commit:  3c4cd95
    Stats:   8440 files, 77158 symbols, 125889 edges
    Clusters:   9113
```

---

## 生成的文件结构

### 项目内文件

```
.gitnexus/              # 索引数据（gitignored）
├── kuzu/               # KuzuDB 图数据库文件
└── meta.json           # 索引元数据

AGENTS.md               # AI agent 行为指南（GitNexus 自动生成）
.claude/skills/gitnexus/  # 6 个 Claude Code 技能文件
├── gitnexus-exploring/SKILL.md      # 代码导航探索
├── gitnexus-debugging/SKILL.md      # Bug 追踪调试
├── gitnexus-impact-analysis/SKILL.md # 变更影响分析
├── gitnexus-refactoring/SKILL.md    # 安全重构
├── gitnexus-guide/SKILL.md          # 工具参考指南
└── gitnexus-cli/SKILL.md            # CLI 命令参考
```

### 全局文件

```
~/.gitnexus/registry.json  # 全局仓库注册表
~/.claude/skills/          # 全局 Claude skills（包含 gitnexus 相关）
~/.claude.json             # Claude Code MCP 配置
```

### registry.json 内容

```json
[
  {
    "name": "kray",
    "path": "/Users/shiyanpeng/Desktop/kaiworks/kray",
    "storagePath": "/Users/shiyanpeng/Desktop/kaiworks/kray/.gitnexus",
    "indexedAt": "2026-05-13T15:15:13.726Z",
    "lastCommit": "3c4cd95a5b630f1a3acb5ef20190daf0e4a7dc4c",
    "stats": {
      "files": 8440,
      "nodes": 77158,
      "edges": 125889,
      "communities": 9113,
      "processes": 0
    }
  }
]
```

---

## 使用方法

### CLI 命令参考

```bash
# 索引管理
gitnexus analyze [path]       # 索引仓库（首次或更新）
gitnexus analyze --force      # 强制完全重建索引
gitnexus status               # 查看当前仓库索引状态
gitnexus list                 # 列出所有已索引仓库
gitnexus clean                # 删除当前仓库索引
gitnexus clean --all --force  # 删除所有索引

# 查询工具
gitnexus query "关键词"       # 按概念搜索执行流
gitnexus context "符号名"     # 360° 符号视图（调用者、被调用者）
gitnexus impact "符号名"      # 变更爆炸半径分析
gitnexus cypher "MATCH ..."   # 原始 Cypher 图查询

# MCP 与服务
gitnexus mcp                  # 启动 MCP server（stdio 模式，给 AI agent 用）
gitnexus serve                # 启动 HTTP server（给 Web UI 连接用）
gitnexus setup                # 一次性配置编辑器 MCP
```

### 通过 MCP 在 Claude Code 中使用

重启 Claude Code 会话后，MCP server 会自动加载，提供以下工具：

| MCP 工具 | 用途 |
|----------|------|
| `gitnexus_query` | 按概念搜索执行流 |
| `gitnexus_context` | 符号的完整上下文（调用者、被调用者、所属流程） |
| `gitnexus_impact` | 变更爆炸半径分析 |
| `gitnexus_detect_changes` | 提交前检查变更影响范围 |
| `gitnexus_rename` | 基于调用图的安全重命名 |
| `gitnexus_cypher` | 原始图查询 |
| `gitnexus_list_repos` | 列出已索引仓库 |

### 推荐编码习惯

1. **编辑任何函数前**：先运行 `gitnexus impact "函数名"` 评估风险
2. **提交前**：运行 `gitnexus detect_changes` 检查变更范围是否符合预期
3. **探索不熟悉的代码**：用 `gitnexus query "关键词"` 而非 grep，结果按执行流分组
4. **重命名**：用 `gitnexus rename` 而非全局搜索替换，它理解调用图
5. **代码变更后**：运行 `gitnexus analyze` 增量更新索引

### CLI 查询示例

```bash
# 搜索与调度相关的代码
$ gitnexus query "raylet scheduling"

# 查看 NodeManager 的上下文
$ gitnexus context "NodeManager"
{
  "status": "found",
  "symbol": {
    "uid": "Method:src/ray/raylet/node_manager.cc:NodeManager:158",
    "name": "NodeManager",
    "filePath": "src/ray/raylet/node_manager.cc",
    "startLine": 158,
    "endLine": 295
  },
  ...
}

# 评估修改 NodeManager 的影响
$ gitnexus impact "NodeManager"
{
  "target": {...},
  "direction": "upstream",
  "risk": "LOW",
  ...
}
```

---

## Web UI 使用

### 正确使用方式

`gitnexus serve` 启动的是一个 **API 后端服务器**，不是 Web 页面服务器。直接访问 `http://127.0.0.1:4747/` 会返回 `Cannot GET /`，这是正常的。

正确用法：

1. 终端运行（保持后台）：
   ```bash
   gitnexus serve
   ```

2. 浏览器打开 GitNexus 的前端 Web UI：
   ```
   https://gitnexus.vercel.app/
   ```

3. Web UI 会自动检测到 `http://127.0.0.1:4747` 上的本地 server 并连接，然后可以在浏览器中浏览代码知识图谱。

**总结**：
- `http://127.0.0.1:4747/` — 仅 API 端点，不提供 HTML 页面
- `https://gitnexus.vercel.app/` — 前端 UI，连接本地 4747 端口读取数据
- 两者配合使用才完整

---

## AGENTS.md 生成内容摘要

GitNexus 自动生成的 `AGENTS.md` 定义了 AI agent 的行为准则：

### Always Do
- 编辑任何符号前**必须**运行 impact analysis
- 提交前**必须**运行 `detect_changes` 验证变更范围
- 如果 impact 返回 HIGH/CRITICAL 风险，**必须**警告用户
- 探索代码用 `query` 而非 grep
- 需要完整上下文用 `context`

### When Debugging
1. `query` — 搜索与问题相关的执行流
2. `context` — 查看可疑函数的所有引用
3. 读取 `gitnexus://repo/kray/process/{name}` — 完整执行流追踪
4. `detect_changes` — 对比分支差异

### When Refactoring
- 重命名用 `gitnexus_rename`（先 dry_run）
- 提取/拆分前用 `context` + `impact` 评估
- 重构后用 `detect_changes` 验证

### Never Do
- 不运行 impact 就编辑代码
- 忽略 HIGH/CRITICAL 风险警告
- 用搜索替换来重命名（应该用 `gitnexus_rename`）
- 不检查变更范围就提交

---

## 影响风险等级参考

| Risk | 含义 | 建议操作 |
|------|------|----------|
| LOW | 影响范围小，直接调用者少 | 正常修改 |
| MEDIUM | 中等影响，涉及多个模块 | 谨慎修改，检查调用链 |
| HIGH | 影响广泛，涉及核心执行流 | 警告用户，确认后再修改 |
| CRITICAL | 影响整个系统关键路径 | 强烈建议不修改或极度谨慎 |

---

## 常见问题

### Q: 索引过期怎么办？
```bash
gitnexus status   # 检查是否过期
gitnexus analyze  # 增量更新
```

### Q: 如何删除索引重建？
```bash
gitnexus clean
gitnexus analyze
```

### Q: MCP server 超时怎么办？
全局安装比 npx 启动更快，避免超时：
```bash
npm install -g gitnexus@1.3.8
claude mcp add gitnexus -- gitnexus mcp  # 用全局命令而非 npx
```

### Q: 如何索引多个仓库？
每个仓库根目录分别执行 `gitnexus analyze`，MCP server 会自动通过 registry 服务所有已索引仓库。查询时指定 `repo` 参数：
```bash
gitnexus query "auth" --repo my-app
```

### Q: v1.3.8 和最新版的功能差异？
v1.3.8 缺少的功能（最新版有）：
- `--skip-embeddings` / `--embeddings` 选项
- `--skills` 生成仓库特定技能文件
- `--skip-agents-md` 保留自定义编辑
- `group` 多仓库组管理命令
- `publish` 发布到 understand-quickly 注册表
- PostToolUse hooks（提交后检测索引过期）
