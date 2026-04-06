# 个人开发备忘

## Git 身份配置

本仓库已配置：

```
git config user.name "Rruop"
git config user.email "yshi24006@gmail.com"
```

提交 PR 到社区时，Author、Committer、Signed-off-by 三处必须一致使用上述身份。

---

## DCO（Developer Certificate of Origin）签名

### 什么是 DCO

Ray 社区要求所有 commit 通过 DCO 检查。DCO 是一种轻量级的贡献者协议，要求每个 commit message 末尾包含 `Signed-off-by` 行，声明贡献者有权提交该代码。

### 如何添加 Signed-off-by

提交时加 `--signoff`（或 `-s`）参数，git 会自动在 commit message 末尾追加：

```
Signed-off-by: Rruop <yshi24006@gmail.com>
```

示例：

```bash
git commit --signoff -m "commit message"
```

### DCO 检查失败的常见原因

1. **缺少 Signed-off-by 行** — 提交时忘记加 `--signoff` 参数
2. **Signed-off-by 邮箱与 Author 邮箱不一致** — 例如改了 author 邮箱但 signoff 行还是旧邮箱

### 修复方法

#### 补加 signoff（最近一次提交）

```bash
git commit --amend --no-edit --signoff
```

#### 修改 author 信息 + signoff

```bash
GIT_COMMITTER_NAME="Rruop" GIT_COMMITTER_EMAIL="yshi24006@gmail.com" \
git commit --amend --no-edit \
  --author="Rruop <yshi24006@gmail.com>" \
  --signoff
```

注意：`--signoff` 使用的是仓库配置的 `user.name` 和 `user.email`，所以要确保仓库配置正确。

#### 修改 author 邮箱（不改 signoff 内容时，手动指定 message）

如果 signoff 行内容需要手动控制（例如旧的 signoff 行需要替换），使用 `-m` 直接指定完整 commit message：

```bash
GIT_COMMITTER_NAME="Rruop" GIT_COMMITTER_EMAIL="yshi24006@gmail.com" \
git commit --amend \
  --author="Rruop <yshi24006@gmail.com>" \
  -m "commit title

commit body...

Signed-off-by: Rruop <yshi24006@gmail.com>"
```

#### 修改历史提交（非最近一次）

使用 `rebase --exec`：

```bash
git rebase --exec 'GIT_COMMITTER_NAME="Rruop" GIT_COMMITTER_EMAIL="yshi24006@gmail.com" \
git commit --amend --no-edit --author="Rruop <yshi24006@gmail.com>" --signoff' \
master
```

#### 修复后推送

修改历史后必须 force push：

```bash
git push --force origin <branch-name>
```

---

## 历史问题修复记录

### 2026-05-23：修正两个分支的提交邮箱和 DCO

**问题**：`fix-v1-monitor-dead-nodes` 和 `dashboard-log-locate-mode` 两个分支使用了错误的邮箱提交。

| 分支 | 原 Author | 原邮箱 | 问题 |
|---|---|---|---|
| `dashboard-log-locate-mode` | shiyanpeng03 | `shiyanpeng03@kuaishou.com` | 邮箱错误，缺少 signoff |
| `fix-v1-monitor-dead-nodes` | Rruop | `33682673+Rruop@users.noreply.github.com` | 邮箱错误，signoff 邮箱与 author 不一致 |

**修复步骤**：

1. 将仓库默认配置改为 `Rruop <yshi24006@gmail.com>`
2. 使用 `rebase --exec` 和 `commit --amend` 修正 author/committer 邮箱
3. 补加 / 修正 `Signed-off-by` 行，确保与 author 邮箱一致
4. Force push 两个分支到远程

**相关 PR**：
- https://github.com/ray-project/ray/pull/63504 (`dashboard-log-locate-mode`)
- https://github.com/ray-project/ray/pull/63610 (`fix-v1-monitor-dead-nodes`)
