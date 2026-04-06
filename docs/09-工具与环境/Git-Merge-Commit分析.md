# Git Merge Commit 分析与 Cherry-pick 指南

## 背景

Master 分支因协作者使用 `git pull`（默认 merge 策略）而非 `git pull --rebase`，产生了无意义的 merge commit，导致历史分叉、不够整洁。本文档记录对两个 merge commit 的分析过程和结论，以及 cherry-pick 操作指南。

---

## 1. 两个 Merge Commit 的结构

### Merge 1: `0cad9dc71a` (2026-04-13)

```
远端 master:    80d4b36e ──→ 45aebeff [Data] Remove redis kuaishou infra ──────┐
                                                                               ↓
本地 zhangfuxing: 80d4b36e → eb5193ce → 0718da69 → a5ba848d → 26577be → 0d5035bc → 0cad9dc7 (merge)
                                                                               ↑
                                                                        Git 自动生成
```

- **Parent 1**: `0d5035bcd0` (zhangfuxing 本地，5 个零碎 checkpoint commit)
- **Parent 2**: `45aebeff78` (远端，1 个 commit)
- **zhangfuxing 的 5 个本地 commit**:
  - `eb5193ce` Update checkpoint_writer.py
  - `0718da69` Update checkpoint_writer.py
  - `a5ba848d` remove try catch
  - `26577bea` Update checkpoint_filter.py
  - `0d5035bc` Update checkpoint_writer.py

**关键发现**：三个 commit 的 tree hash 完全相同（`bf55566c43a90c2e8d5254c4e3329656ef33c97a`），即 parent 1、parent 2 和 merge 结果的文件快照一模一样。这意味着两边虽然走了不同路径，但到达了完全相同的代码状态。

### Merge 2: `90b544bb21` (2026-04-15)

```
远端 master:   0cad9dc7 → 8f3317f77a (renruoyu, Update checkpoint_filter.py) ──┐
                                                                                ↓
本地 zhangfuxing: 0cad9dc7 → a9b3fb42bc → 66984c8933 ──────────────────────────→ 90b544bb (merge)
                                                                                ↑
                                                                         Git 自动生成
```

- **Parent 1**: `66984c8933` (zhangfuxing 本地，2 个 commit)
- **Parent 2**: `8f3317f77a` (远端 renruoyu，1 个 commit)
- **merge-base**: `1370e6a0f4`

**关键发现**：zhangfuxing 本地并未修改 `checkpoint_filter.py`（只改了 `streaming_executor.py` 和 `resource_manager.py`），远端 renruoyu 改了 `checkpoint_filter.py`，两边改的文件不重叠，无冲突。

---

## 2. Merge Commit 的 Diff 展示机制

### 为什么 merge commit 没有单一的 diff？

普通 commit 是线性的（A → B），diff 明确。但 merge commit 有两个 parent：

```
A ─┐
   ├→ M    M 相对于谁算 diff？A 还是 B？都可以，所以 GitLab 等平台展示两个 tab
B ─┘
```

### 三种 diff 展示方式

| 方式 | 命令 | 含义 |
|------|------|------|
| changes to parent 1 | `git diff M^1..M` | merge 结果相比 parent 1 多了什么（= parent 2 带进来的） |
| changes to parent 2 | `git diff M^2..M` | merge 结果相比 parent 2 多了什么（= parent 1 带进来的） |
| combined diff（默认） | `git show M` | 只展示两边都有改动的文件（即冲突解决部分） |

### 两个 Merge Commit 的 Diff 对比

#### `0cad9dc71a` — 两边 diff 都是空的

| 角度 | diff 结果 |
|------|-----------|
| `git diff 0cad9dc7^1..0cad9dc7` | 空 |
| `git diff 0cad9dc7^2..0cad9dc7` | 空 |

原因：三个 commit 的 tree hash 相同，两边的最终代码状态完全一致，merge 结果和任何一边都没有差异。

#### `90b544bb21` — 两边都有 diff

| 角度 | 包含的 commit | 显示的文件改动 |
|------|---------------|----------------|
| changes to parent 1 (66984c8933) | 8f3317f77a | `checkpoint_filter.py` (+4 -1) |
| changes to parent 2 (8f3317f77a) | a9b3fb42bc + 66984c8933 | `resource_manager.py` (+24 -6), `streaming_executor.py` (+93 -22) |

注意："changes to 8f3317f77a" 里展示的是从 merge-base (`1370e6a0f4`) 以来的**累计 diff**，包含了 `a9b3fb42bc` 和 `66984c8933` 两个 commit 的改动，不是只展示最后一个 commit。

### Merge Commit 是否包含代码改动？

| 场景 | merge commit 本身是否有代码改动 | 说明 |
|------|-------------------------------|------|
| 无冲突自动合并，两边 tree 不同 | 无 | 两边改动叠加，merge 结果 = parent 1 改动 + parent 2 改动 |
| 无冲突自动合并，两边 tree 相同（如 0cad9dc7） | 无 | 殊途同归，最终状态一样 |
| 有冲突，人工解决 | **有** | 解决冲突时写的代码只存在于 merge commit 中 |

---

## 3. 冲突处理

### git pull 时有冲突的流程

```bash
$ git pull
Auto-merging python/ray/data/checkpoint/checkpoint_writer.py
CONFLICT (content): Merge conflict in checkpoint_writer.py
Automatic merge failed; fix conflicts and then commit the result.

# 解决冲突后
git add .
git commit    # 生成 merge commit，冲突解决代码记录在其中
```

### 冲突解决代码只存在于 merge commit

```
Parent 1 (本地): 函数写的是 return a + b
Parent 2 (远端): 函数写的是 return a * b

解决冲突后你写了: return a + b + c    ← 这个改动只存在于 merge commit 中
```

这部分代码不属于任何一边的 commit，只存在于 merge commit 自身。

---

## 4. Cherry-pick 操作指南

### 按场景选择策略

| 场景 | 做法 |
|------|------|
| merge 无冲突 | pick 两边 commit，忽略 merge commit |
| merge 有冲突，且你能重新解决 | pick 两边 commit，自己再解一次冲突 |
| merge 有冲突，你不想重新解决 | 直接 `git cherry-pick -m 1 <merge>` 一把带走 |

### Commit 之间是否有依赖

**有依赖（必须按顺序全部 pick）**：
```
eb5193ce  新增了函数 foo()
0718da69  修改了 foo() 的逻辑
a5ba848d  删除了 foo() 里的 try-catch
```
后面的 commit 依赖前面的上下文，跳过会导致冲突。

**互相独立（可以单独 pick）**：
```
334ae42b  改 metric 模块
80d4b36e  改 data 模块
```

### 本项目具体情况

- `0cad9dc71a`：merge commit 为空，直接 pick 两边 commit 即可
- `90b544bb21`：无冲突自动合并，两边文件不重叠，直接 pick 两边 commit 即可

```bash
# zhangfuxing 的 5 个 checkpoint commit（有依赖，按顺序 pick）
git cherry-pick eb5193ce 0718da69 a5ba848d 26577bea 0d5035bc
# 远端的 1 个 commit
git cherry-pick 45aebeff

# zhangfuxing 的 2 个 commit
git cherry-pick a9b3fb42bc 66984c8933
# 远端 renruoyu 的 1 个 commit
git cherry-pick 8f3317f77a
```

### Cherry-pick Merge Commit

如果要直接 pick 一个 merge commit，需要指定 parent：

```bash
git cherry-pick -m 1 <merge-commit>   # -m 1 表示以第一个 parent 为基准
```

但通常**不建议** pick merge commit，因为它可能包含大量内容，且语义不明确。

---

## 5. 三种 Git 合并方式对比

| 操作 | 结果 | master 上的样子 |
|------|------|-----------------|
| `git pull`（默认 merge） | 生成 merge commit，历史分叉 | 当前 master 的样子 |
| `git pull --rebase` | 不生成新 commit，本地 commit 接在远端后面 | 线性，干净 |
| GitLab squash merge | 所有分支 commit 压成 1 个新 commit | 线性，每个 feature 一条记录 |

---

## 6. 能否 Rebase 整理 Master？

### 技术上可以，但有重大风险

1. **Force push 必需**：master 已推送到 origin（git.corp.kuaishou.com），rebase 后需要 `git push --force`
2. **影响所有协作者**：至少 8 个本地分支基于 master，rebase 后所有协作者必须重新 `git fetch && git reset`
3. **受影响分支**：
   - T11333239-Dynamic-Block
   - T11384830-log
   - T11396554-dashboard
   - T11591703-Dataset-Configurtaion
   - feature-syp
   - master-test 等
4. **冲突风险**：从 80d4b36e 到 90b544bb 之间涉及约 12 个 commit 和 2 个 merge，rebase 展平后需手动解决顺序和可能的冲突

### 建议

| 方案 | 适用场景 |
|------|----------|
| A. 不动 master，向前看 | 协作者多，历史已推送。今后强制 `git pull --rebase` 和 squash merge 规范即可 |
| B. Rebase + force push | 团队人少、可以协调所有人同步。需要通知所有人并重建分支 |
| C. 只整理未推送的部分 | 如果有尚未 push 的 commit，可以安全 rebase 那部分 |

**最实际的做法是方案 A**：在团队 GitLab 设置中启用 "squash merge" 或要求 `git pull --rebase`，避免以后再产生这种问题。已经进入 master 的历史不值得冒 force push 的风险。

---

## 7. 问题总结

| 问题 | 表现 |
|------|------|
| `git pull` 无 rebase | 产生无意义的 merge commit |
| 零碎 commit | "Update checkpoint_writer.py" x3、"remove try catch" 等没有信息量 |
| commit 消息不规范 | 部分 commit 没有 ticket 号或功能前缀 |

**建议团队规范**：
1. 配置 `git config --global pull.rebase true`，让 `git pull` 默认使用 rebase
2. MR 时启用 squash merge，将零碎 commit 压成一个有意义的 commit
3. commit message 遵循 `[Ticket] Summary` 格式

---

## 8. Checkpoint 两阶段提交（2-Phase Commit）机制

### 8.1 概述

File-based datasink（Parquet/CSV/JSON/Images 等）使用两阶段提交保证写入的原子性。核心思路是：先记录"即将写入"的凭据（pending checkpoint），再写数据文件，最后确认提交。如果中途崩溃，恢复时可以凭 pending checkpoint 找到并清理孤儿数据文件。

### 8.2 完整流程

以一个 write task 为例：

```
prepare_checkpoint → write_fn → commit_checkpoint → collect_stats
```

#### Phase 1 — Prepare（写 pending checkpoint）

```
checkpoint_dir/a1b2c3d4_000003.pending.parquet   ← 写入 ID 列数据
```

- `prepare_checkpoint_fn` 在数据写入**之前**执行
- 使用 `FilenameProvider.get_filename_for_task(write_uuid, task_idx)` 计算出确定性的 base_filename
- base_filename 只取决于 `write_uuid` + `task_idx`，与数据内容无关
- checkpoint 文件名 = `{base_filename}.pending.parquet`
- pending checkpoint 里存的是这批数据的所有 ID
- `PendingCheckpoint` 对象（包含 pending_path 和 committed_path）写入 `ctx.kwargs`，供 Phase 3 使用
- **幂等写入**：重试时 `write_uuid` 和 `task_idx` 不变，覆盖之前的 pending 文件

#### Phase 2 — Write（写数据文件）

```
data_dir/a1b2c3d4_000003.parquet                 ← 数据文件，文件名前缀与 checkpoint 相同
```

- `write_fn` 将数据写到目标路径
- 数据文件名也通过同一个 `FilenameProvider` 生成，前缀一定是 `a1b2c3d4_000003`

#### Phase 3 — Commit（rename pending → committed）

```
a1b2c3d4_000003.pending.parquet  →  a1b2c3d4_000003.parquet   ← 原子 rename
```

- `commit_checkpoint_fn` 从 `ctx.kwargs` 读取 `PendingCheckpoint` 对象
- 调用 `filesystem.move(pending_path, committed_path)`
- **幂等操作**：如果 committed 已存在就跳过，如果 pending 已删除也跳过
- 失败时通过 `call_with_retry` 重试

**commit 不会因为 iterator 耗尽而失败**：`commit_checkpoint_fn` 不依赖 blocks 数据，它只从 `ctx.kwargs` 读取 Phase 1 写入的 `PendingCheckpoint` 对象。

### 8.3 故障场景与恢复

| 故障点 | 状态 | 恢复方式 |
|--------|------|----------|
| Phase 1 后崩溃 | pending checkpoint 存在，数据文件不存在 | 恢复时删除 pending checkpoint，重新执行 |
| Phase 2 后崩溃 | pending checkpoint 存在，数据文件存在 | 恢复时通过前缀 Trie 匹配删除数据文件 + pending checkpoint，重新执行 |
| Phase 3 后（正常完成） | committed checkpoint 存在，数据文件存在 | 正常状态，ID 被过滤，不会重复处理 |

---

## 9. 前缀 Trie 恢复机制

### 9.1 算法流程

`_clean_pending_checkpoints_task` 的执行步骤：

```
1. 扫描 checkpoint 目录，找到所有 .pending.parquet 文件
   例: ["a1b2c3d4_000003.pending.parquet", "e5f67890_000005.pending.parquet"]

2. 构建前缀 Trie
   strip 掉 ".pending.parquet" 后缀 → 插入 "a1b2c3d4_000003", "e5f67890_000005"

3. 扫描数据文件目录（递归，包括分区子目录）
   例: ["a1b2c3d4_000003.parquet", "a1b2c3d4_000003-1.parquet",
        "e5f67890_000005.parquet", "x9y99999_000001.parquet"]

4. 对每个数据文件，检查 trie.has_prefix_of(basename)
   "a1b2c3d4_000003.parquet"   → 匹配前缀 "a1b2c3d4_000003" → 删除
   "a1b2c3d4_000003-1.parquet" → 匹配前缀 "a1b2c3d4_000003" → 删除（分区/多文件场景）
   "e5f67890_000005.parquet"   → 匹配前缀 "e5f67890_000005" → 删除
   "x9y99999_000001.parquet"   → 不匹配任何 pending 前缀 → 保留

5. 删除所有 pending checkpoint 文件
```

### 9.2 删除顺序：先数据文件，后 pending checkpoint

```
4. 删除匹配前缀的数据文件        ← 先删数据文件
5. 删除所有 pending checkpoint   ← 后删 pending checkpoint
```

**顺序是关键**。如果反过来（先删 pending checkpoint，后删数据文件）：

- 两步之间崩溃 → pending checkpoint 已删，孤儿数据文件还在
- 下次恢复时没有 pending checkpoint 参考 → **永远无法找到并清理这些孤儿文件**

当前顺序的崩溃安全性：

- 两步之间崩溃 → 数据文件已删（或部分已删），pending checkpoint 还在
- 下次恢复时重新执行清理，Trie 重新构建
- 对已删的数据文件 `delete_file` 会发现文件不存在（幂等）
- pending checkpoint 再次被删除

**pending checkpoint 是恢复凭据**——只要它还在，就能重新找到并清理对应的孤儿数据文件。

### 9.3 为什么需要前缀匹配而不是精确匹配？

一个 write task 可能生成多个数据文件。例如：

- **分区写入**：按日期分区，一个 task 可能写 `a1b2c3d4_000003/date=2026-01-01/part-0.parquet`
- **max_rows_per_file**：一个 task 拆成多文件，如 `a1b2c3d4_000003.parquet`、`a1b2c3d4_000003-1.parquet`
- **Row-based 写入**（Images 等）：每行一个文件，如 `a1b2c3d4_000003_000000_000000.png`、`a1b2c3d4_000003_000000_000001.png`

前缀 Trie 可以高效匹配所有这些变体。

### 9.4 为什么用 Trie 而不是简单的字符串匹配？

多个 pending checkpoint 可能同时存在（多个 task 同时失败），每个数据文件都需要检查是否匹配**任意一个** pending 前缀。Trie 的时间复杂度是 O(文件名长度)，与 pending 数量无关，比逐个前缀比对更高效。

---

## 10. Pending Checkpoint 前缀唯一性与数据文件命名

### 10.1 Pending Checkpoint 前缀不会重复

checkpoint 文件名的生成方式：

```python
base_filename = FilenameProvider.get_filename_for_task(write_uuid, task_idx)
# 例: "a1b2c3d4_000003"  (write_uuid + task_index)

checkpoint_file = f"{base_filename}.pending.parquet"
# 例: "a1b2c3d4_000003.pending.parquet"
```

- `write_uuid`：整个 write 操作的 UUID（`_plan_write_op_internal` 中通过 `uuid.uuid4().hex` 生成），同一次 write 所有 task 共享
- `task_idx`：每个 task 的索引，同一次 write 内唯一
- `{write_uuid}_{task_idx:06}` 的组合在同一次 write 内唯一
- 不同次 write 的 `write_uuid` 不同，也不会冲突
- 重试时 `write_uuid` 和 `task_idx` 不变（确定性），所以重试写的 pending checkpoint 会覆盖之前的（幂等写入）

### 10.2 为什么数据文件名前缀要与 checkpoint 相同？

这是 2-phase commit 恢复机制的核心设计。目的是：在恢复时，通过 pending checkpoint 的文件名找到并删除对应的孤儿数据文件。

有两种 `_FileDatasink` 子类，它们生成数据文件名的方式不同：

#### BlockBasedFileDatasink（Parquet/CSV/JSON 等）：一个 task 写一个文件

```
数据文件:     a1b2c3d4_000003.parquet
checkpoint:  a1b2c3d4_000003.pending.parquet → a1b2c3d4_000003.parquet（commit 后）
```

数据文件名 = `get_filename_for_task(write_uuid, task_idx)` = `a1b2c3d4_000003.parquet`，和 checkpoint 前缀完全一致。

#### RowBasedFileDatasink（Images 等）：一个 task 写多个文件（每行一个）

```
数据文件:     a1b2c3d4_000003_000000_000000.png
              a1b2c3d4_000003_000000_000001.png
              a1b2c3d4_000003_000000_000002.png
checkpoint:  a1b2c3d4_000003.pending.parquet
```

`RowBasedFileDatasink.write_block` 的文件名生成：

```python
base, ext = _split_base_and_ext(task_filename)  # base = "a1b2c3d4_000003"
filename = f"{base}_{block_index:06}_{row_index:06}{ext}"
# → "a1b2c3d4_000003_000000_000000.png"
```

每个行文件都以 `a1b2c3d4_000003` 为前缀。恢复时前缀 Trie 插入 `a1b2c3d4_000003`，可以匹配到这个 task 产生的所有数据文件。

### 10.3 设计约束

同一个 task 产生的所有数据文件必须共享 `get_filename_for_task()` 返回的前缀。这样 pending checkpoint 的文件名（去掉 `.pending.parquet` 后缀）就是数据文件的前缀，恢复时通过前缀 Trie 可以精确匹配并清理孤儿文件。

这也是 `FilenameProvider` 的 docstring 强调 **"filenames must be deterministic from (write_uuid, task_index) alone"** 的原因。

---

## 11. Non-File Datasink 的 Checkpoint 路径

### 11.1 两种 Datasink 的 Checkpoint 差异

| | File-based Datasink | Non-File Datasink |
|---|---|---|
| 示例 | Parquet, CSV, JSON, Images | SQL, MongoDB, Kafka |
| Checkpoint 时机 | Pre-write（2-phase commit） | Post-write（非原子） |
| Transform chain | [prepare_ckpt] → [write] → [commit_ckpt, stats] | [] → [write] → [write_ckpt, stats] |
| 恢复语义 | Exactly-once | At-least-once |
| 可撤销性 | 可删除数据文件 | 通常不可删除已写入的行 |

### 11.2 Non-File Datasink 的 Transform Chain

```python
# Non-file datasink 的 post_transformations
write_checkpoint_fn = _generate_non_atomic_write_checkpoint_transform(data_context, checkpoint_writer)
post_transformations = [
    write_checkpoint_fn,   # 在 write_fn 之后，遍历 blocks 写 checkpoint
    collect_stats_fn,
]
pre_transformations = []   # 没有 pre-write 阶段
```

`_generate_non_atomic_write_checkpoint_transform` 的实现：

```python
def write_checkpoint(blocks: Iterable[Block], ctx: TaskContext) -> Iterable[Block]:
    block_list, combined_block = _combine_blocks(blocks)  # 消费 iterator
    ba = BlockAccessor.for_block(combined_block)

    if ba.num_rows() > 0:
        id_column = data_context.checkpoint_config.id_column
        _validate_id_column_exists(id_column, combined_block)
        checkpoint_writer.write_block_checkpoint(ba)

    return iter(block_list)
```

### 11.3 Iterator Aliasing Bug（已识别）

当 `generate_write_fn` 在无 filter 的情况下，`blocks_to_write` 和 `blocks_to_return` 引用同一个 iterator：

```python
# 无 filter 路径（bug）
blocks_to_write = blocks
blocks_to_return = blocks   # 同一个 iterator！
datasink.write(blocks_to_write, ctx)  # 消费了 iterator
return blocks_to_return  # 已耗尽，下游得到空数据
```

**影响**：

| 场景 | 影响 |
|------|------|
| File-based datasink + 有 filter | 不受影响（有 `itertools.tee` 保护） |
| File-based datasink + 无 filter + 有 checkpoint | 不受影响（file-based 用 pre-write 2-phase commit，checkpoint 不依赖 post-write 的 blocks） |
| Non-file datasink + 有 filter | 不受影响（有 `itertools.tee` 保护） |
| Non-file datasink + 无 filter + 有 checkpoint | **checkpoint 不会被写入**（blocks 已耗尽，`_combine_blocks` 得到空数据） |
| 无 checkpoint 场景 | 只影响 write stats（报 0），不影响数据写入 |

**根因**：filter 功能引入时为了避免 `itertools.tee` 的内存开销，在 no-filter 路径去掉了 tee，但没有考虑到 non-file checkpoint 依赖 post-write 的 blocks。

**修复方式**：无条件使用 `itertools.tee` 分离 write iterator 和 return iterator，确保下游 transforms 始终能访问完整的 blocks 数据。Master 上同样存在此问题。

### 11.4 2-Phase Commit 中的 Filter 行为

File-based datasink + checkpoint + filter 的 transform chain：

```
[prepare_checkpoint_fn] → [write_fn (with filter)] → [commit_checkpoint_fn, collect_stats_fn]
```

1. `prepare_checkpoint_fn`：读取所有 blocks，提取 ID 列，写 pending checkpoint（**包含所有 ID，不过滤**）
2. `write_fn`：tee blocks → 对一份应用 filter → 写过滤后的数据到文件；返回未过滤的 blocks
3. `commit_checkpoint_fn`：从 `ctx.kwargs` 读取 pending info，rename commit

设计意图：被过滤掉的行也记录在 checkpoint 中（防止重启后重复处理），但不写入目标文件。filter 通过 `generate_write_fn` 的 `filter_fn`/`filter_expr` 参数工作，无论 file-based 还是 non-file datasink，filter 都在 write_fn 内部统一执行。

---

## 12. Release-2.55.1 Cherry-pick 检查记录

### 12.1 已 Cherry-pick 的 Commit 对照表

从 master 的 `f539b119` 开始 pick，跳过了 `3b222b08`（post-checkpoint filter）和 `c162f7a870`（upstream dead node cache fix）。

| Release 分支 | Master | 内容 | Diff 对比 |
|---|---|---|---|
| `192e56f09f` | `f539b119` | [RayData] checkpoint support deduplication | 功能一致，已修复未使用的 BlockAccessor import |
| `8d6c9c5d65` | `f1a537ee3e` | [T11358533] Add NoOpClusterAutoscaler | 功能等价，已修复 elif 条件变量 |
| `9b956c4b7a` | `4a3dd26e03` | [T11348639] Add filter_fn and filter_expr | 功能一致，上下文差异（TYPE_CHECKING guard、常量名） |
| 跳过 | `3b222b08b4` | [T11348639] Add post-checkpoint filter | 跳过：功能已通过其他方式覆盖 |
| `d41847acc6` | `5eeb11d0ba` | Fix NoOpClusterAutoscaler | 完全一致 |
| `f707de4de8` | `eb4f846be0` | [Dashboard] dashboard optimization | 完全一致 |
| `0de1f5df6d` | `c5314b1d38` | [Dashboard] dashboard optimization | 完全一致 |
| 跳过 | `c162f7a870` | [core] Fixing dashboard node_head api's dead node cache | 跳过：upstream bug fix，不影响定制功能 |
| `783f1e2598` | `cd008b03d6` | [T11320285] Support metric filter | 功能一致，行号偏移 |
| `d184bb6d6d` | `d6c26393cb` | Enhance claude md | 完全一致 |
| `6c5f1cbfb8` | `9dd171693b` | [T11597270] Add configurable CSC upload path | 完全一致 |
| `7e66f1eaf2` | `6e72a872f5` | [RayDashboard] fix get pod name | 完全一致 |
| `f1d98086dc` | `c8d8156434` | [dashboard] column_width_adapt | 完全一致 |
| `684271f948` | `68111c41d6` | worker port to random | 完全一致 |
| `ca8fed6474` | `dfa7e42e85` | [T11609366] Add Input metrics | 功能一致，streaming_executor.py 行号偏移 |
| `41dba4d163` | `2c2f1f9204` | [T11384830] Add locate mode | 完全一致 |

### 12.2 已修复的 Cherry-pick 问题

| 问题 | 文件 | 修复内容 |
|------|------|----------|
| elif 条件使用了常量而非变量 | `cluster_autoscaler/__init__.py:64` | `DEFAULT_CLUSTER_AUTOSCALER_VERSION` → `cluster_autoscaler_version` |
| 未使用的 BlockAccessor import | `checkpoint_writer.py:197` | 移除 `from ray.data.block import BlockAccessor` |
| `_load_serializer_from_topic_schema` 使用原始 bootstrap_servers 参数 | `kafka_datasink.py:112` | 改为使用 `self._bootstrap_servers`（已转为 list） |
| read_kafka docstring 引用错误的库名 | `read_api.py:4487` | `confluent-kafka` → `kafka-python` |

### 12.3 已识别的待修复问题

| 问题 | 严重度 | 影响 | 修复方案 |
|------|--------|------|----------|
| generate_write_fn 无 filter 时 iterator aliasing | 高 | Non-file datasink + 无 filter + checkpoint 场景下 checkpoint 丢失、stats 为 0 | 无条件使用 `itertools.tee`（master 同样存在此问题） |

---

## 13. Git LFS 与 Cherry-pick

### 13.1 LFS 初始化

在仓库里执行 `git lfs install` 会在 `.git/hooks/` 里注册 LFS 的 hook（pre-push、post-checkout 等），让 git 知道要用 LFS 处理大文件。每个仓库只需做一次。

如果仓库已有 `.gitattributes` 里的 LFS 规则并在正常工作，说明已经初始化过了。

### 13.2 新增大文件的完整流程

```bash
# 1. 声明哪些文件用 LFS 管理（自动写入 .gitattributes）
git lfs track "path/to/large-file.bin"

# 2. 提交 .gitattributes 的变更
git add .gitattributes

# 3. 添加大文件本身
git add path/to/large-file.bin

# 4. 提交
git commit -m "Add large file with LFS"
```

`git lfs track` 和手动编辑 `.gitattributes` 效果一样，但用命令更不容易写错格式。实际存入 git 的只是一个几十字节的 pointer 文件，真实内容会上传到 LFS 服务器。

### 13.3 Cherry-pick 涉及 LFS 文件时的处理

cherry-pick 会自动把原 commit 里的所有文件变更（包括 LFS pointer 文件）都带过来。如果遇到冲突（通常是 `.gitattributes` 文件冲突），只需：

```bash
# 解决 .gitattributes 冲突后
git add .gitattributes
git cherry-pick --continue
```

不需要单独 `git add` LFS 管理的大文件本身——cherry-pick 已经处理好了。

### 13.4 中途放弃 Cherry-pick 序列

如果想在解决完当前冲突后不再继续 pick 后续的 commit：

```bash
# 1. 解决冲突，手动提交当前 commit
git add .gitattributes
git commit

# 2. 放弃剩余的 cherry-pick 队列
git cherry-pick --quit
```

**`--quit` 与 `--abort` 的区别**：

| 操作 | 效果 |
|------|------|
| `git cherry-pick --quit` | 清除 cherry-pick 序列状态，**不回滚**已提交的内容，只是停下来 |
| `git cherry-pick --abort` | **回滚所有**已 pick 的 commit，恢复到 cherry-pick 前的状态 |

- `--quit`：保留已完成的 commit，放弃队列中剩余的
- `--abort`：全部撤销，回到起点
