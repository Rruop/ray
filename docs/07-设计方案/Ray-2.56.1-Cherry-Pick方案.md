# Ray v2.55.1 → v2.56.1 迁移合并方案

## Context

本地 master 分支已包含 ray/releases/2.55.1 全部内容，并在此基础上有 50 个内部定制 commit。社区最新发布分支 ray/releases/2.56.1 包含 50 个 Core/Data/Dashboard 相关 commit 不在 master 中，涉及 479 个文件、+34,696/-9,177 行变更。

本方案参考已有的 `Ray-2.55.1-Cherry-Pick方案.md`，规划从 2.55.1 到 2.56.1 的迁移合并。

---

## 方向选择：基于 ray/releases/2.56.1 rebase master 定制 commit

### 为什么选择 Direction A（rebase onto 2.56.1）而非 Direction B（merge 2.56.1 into master）

| 维度 | Direction A (rebase onto 2.56.1) | Direction B (merge 2.56.1 into master) |
|------|----------------------------------|----------------------------------------|
| 冲突解决 | 逐个 commit 解决，可逐步验证 | 一次性解决所有冲突，压力集中 |
| 历史清晰度 | 线性历史，无 merge commit | 双线历史，merge commit 混乱 |
| 可回滚性 | 每个 commit 独立可 revert | 回滚需 revert 整个 merge |
| 冲突调试 | 逐个 commit diff 即可定位 | 需在巨大 merge diff 中定位 |
| 后续维护 | 基于 2.56.1 干净基线，后续升级更简单 | 每次升级都有历史包袱 |

**结论**：Direction A 优于 Direction B，因为可以逐步解决冲突、逐个验证、且为后续升级奠定干净基础。

---

## 数量概览

| 类别 | 数量 | 说明 |
|------|------|------|
| master 定制 commit | 50 | 需 rebase 到 2.56.1 上的内部 commit |
| 2.56.1 新增 commit | 50 | 2.55.1→2.56.1 Core/Data/Dashboard 相关 |
| 已有重复 commit（可丢弃） | 7 | pickle/schema cache/GPU shuffle 等已在 2.56.1 中 |
| 冲突热点文件 | ~20 | CRITICAL 级别冲突需手动解决 |
| 安全移植的定制系统 | 6 | 2.56.1 未触及的新模块，无需冲突处理 |

---

## 第一部分：ray/releases/2.56.1 具体优化详解

### Data 模块优化（36 个 commit）

#### P0 — 崩溃/安全修复（3 个）

| # | Commit | PR | 描述 | 技术细节 |
|---|--------|-----|------|---------|
| 1 | `4aadf11b33` | [#64552](https://github.com/ray-project/ray/pull/64552) | Fix read-only hash array crash in hash partition | hash partition 对只读 Arrow array 执行 `np.where` 时 crash，改用可写副本 |
| 2 | `6d4f5e67a3` | [#64767](https://github.com/ray-project/ray/pull/64767) | Fix TensorDtype.__from_arrow__ crash on empty tensor columns | 空张量列转换 pandas 时 numpy `-1` 维度推断 crash，改用显式行计数 |
| 3 | `8ea9ede2ca` | [#63470](https://github.com/ray-project/ray/pull/63470) | Block pickle object columns when reading untrusted Parquet files | **安全修复**：`arrow_pickled_object` 列自动调用 `pickle.load`，攻击者构造的 Parquet 可 RCE |

#### P1 — 重要 bugfix/性能优化（12 个）

| # | Commit | PR | 描述 | 技术细节 |
|---|--------|-----|------|---------|
| 4 | `0672981757` | [#64768](https://github.com/ray-project/ray/pull/64768) | Fix Arrow-backed to_pandas regressions | 2.56 引入的 Arrow-backed pandas 转换回归：整数/浮点列类型溢出，添加 `enable_arrow_backed_pandas_conversion` opt-out 标志 |
| 5 | `80021c9548` | [#63681](https://github.com/ray-project/ray/pull/63681) | Fix iter_batches spilling (1/n): Remove outer make_async_gen | 移除 `iter_batches` 外层 `make_async_gen(num_workers=1)` — 它仅增加缓冲和内存驻留，无并发收益。将隐藏缓冲批次从 ~16 减至 ~8 |
| 6 | `2c6dd5c655` | [#63682](https://github.com/ray-project/ray/pull/63682) | Fix iter_batches spilling (2/n): Replace inner format/collate make_async_gen | 用 `iter_threaded` 替代内部 format/collate 的 `make_async_gen`，将未追踪的 object store 内存驻留批次再减半（~16→~8） |
| 7 | `91c91e1fb9` | [#63809](https://github.com/ray-project/ray/pull/63809) | Fix hash-shuffle aggregator memory estimation | hash-shuffle/join/aggregate 的内存估算既缺失（导致 OOM）又过度计数（导致不可调度），此 PR 使估算可用、准确、有上限 |
| 8 | `c0bd1e76a5` | [#63402](https://github.com/ray-project/ray/pull/63402) | Fix get_or_create_stats_actor crash in Ray Client mode | Ray Client 模式下 `_global_node=None`，stats actor 创建误判为未连接而抛 RuntimeError |
| 9 | `691df86feb` | [#63825](https://github.com/ray-project/ray/pull/63825) | Fix Timer JSON serialization broken by DistributionTracker | DistributionTracker 不可 JSON 序列化，导致 training-ingest benchmark 崩溃 |
| 10 | `a175ef8995` | [#63781](https://github.com/ray-project/ray/pull/63781) | Fix datasource pushdown crashes for generic UDFExpr filter | UDF 谓词 pushdown 时 TypeError：优化器尝试将非 PyArrow 可转换的表达式推送到数据源 |
| 11 | `5fe0aa1db8` | [#63814](https://github.com/ray-project/ray/pull/63814) | Add default logical memory for map operators | 未设 logical memory 的 UDF 默认 0，导致过度订阅。为 map 类算子添加基于 heap_memory 的默认值 |
| 12 | `dcf1c412cb` | [#63792](https://github.com/ray-project/ray/pull/63792) | Gate restore_original_order behind preserve_order | `iter_batches` 消费端的重排序缓冲区在单 worker 延迟时会无限增长，此 PR 将重排序开关置于 `preserve_order` 控制（默认关闭），恢复 next-batch 延迟从 113ms→23ms |
| 13 | `11c4ee9b7e` | [#64316](https://github.com/ray-project/ray/pull/64316) | Avoid input dependencies when exporting args | logical operator 冻结 dataclass 后，`sanitize_for_struct` 递归搜索字段，`_input_dependencies` 引用自身导致无限递归 |
| 14 | `c1b525fc0f` | [#63606](https://github.com/ray-project/ray/pull/63606) | Fix CheckpointConfig FileNotFoundError on Azure Blob Storage | `_clean_pending_checkpoints_task` 使用 `FileSelector` 未设 `allow_not_found=True`，Azure 上删除后列举报 FileNotFoundError |
| 15 | `637fd06220` | [#64309](https://github.com/ray-project/ray/pull/64309) | Revert "Remove safe_round from ExecutionResources" | 回退 safe_round 移除 — 在某些精度敏感场景导致调度错误 |

#### P2 — 功能增强（16 个）

| # | Commit | PR | 描述 | 技术细节 |
|---|--------|-----|------|---------|
| 16 | `886ae8b8c9` | [#63654](https://github.com/ray-project/ray/pull/63654) | **Introduce BlockEntry on RefBundle** | **基础架构变更**：将 `RefBundle.blocks` 从 `Tuple[Tuple[ObjectRef, BlockMetadata], ...]` 改为 `Tuple[BlockEntry, ...]`，BlockEntry 是 frozen dataclass（`ref`/`metadata` 命名字段）。2-tuple 构造被拒绝并给出可操作断言 |
| 17 | `750ef4e506` | [#63331](https://github.com/ray-project/ray/pull/63331) | Support multiple datasets in a cluster (1/2): Pipe DataContext label_selector | 允许通过 `DataContext.execution_options.label_selector` 设置数据集级 label selector，提交任务时附加 subcluster label |
| 18 | `5d2c4e709b` | [#63375](https://github.com/ray-project/ray/pull/63375) | Support multiple datasets in a cluster (2/2): Partition cluster resources | 确保不同数据集的请求者只请求和接收其 subcluster 的资源，共享 AutoscalingCoordinator 下资源隔离 |
| 19 | `7a07ec3415` | [#63982](https://github.com/ray-project/ray/pull/63982) | Rename subcluster label key: `__subcluster__` → `ray-subcluster` | `__subcluster__` 不是合法 K8s label 名，改用 `ray-subcluster` |
| 20 | `68041c4a01` | [#63387](https://github.com/ray-project/ray/pull/63387) | Schema inference for non black box UDF logical operators (1/2) | 教会逻辑算子在执行前推断输出 schema，避免 `Dataset.schema()` 回退到 `limit(1)` 执行。覆盖 AllToAll/Read/Project/Filter/Sort/Aggregate 等 |
| 21 | `031afdb5b0` | [#63776](https://github.com/ray-project/ray/pull/63776) | Eager StarExpr expansion + optimizer-rule narrowing (2/n) | `Project.__post_init__` 在 typed chain 中主动展开 `StarExpr` 为显式 `col()` 引用，使优化器规则统一处理 |
| 22 | `d8ea7eee26` | [#63813](https://github.com/ray-project/ray/pull/63813) | Convert drop_columns to Project logical operator | `drop_columns` 在输入 schema 已知时转换为 `select_columns(keep_cols)`，保持 typed schema chain 完整，缺失列立即抛 KeyError |
| 23 | `5289887d49` | [#63498](https://github.com/ray-project/ray/pull/63498) | **Boost hash_partition with sort_indices + zero-copy slices** | 原 `hash_partition` 三步：N×np.where(O(N·R)) + try_combine 全表拷贝 + N×table.take。新方案：单次 `np.argsort` + `Arrow.sort_indices` + zero-copy `table.slice`，性能提升 3-5× |
| 24 | `cb2a1294ae` | [#63152](https://github.com/ray-project/ray/pull/63152) | Improve hash partition for Struct/list/map columns | 将 retrieve table columns 操作移出循环，避免 pandas 不可处理类型的重复转换开销 |
| 25 | `77190d9706` | [#63834](https://github.com/ray-project/ray/pull/63834) | Extract generic WeightedRoundRobinPartitioner | 将 `RoundRobinPartitioner` 的桶分配逻辑抽取为可复用的 `WeightedRoundRobinPartitioner`，ReadFiles 和 download 可共享 |
| 26 | `bb8c3e47ad` | [#62560](https://github.com/ray-project/ray/pull/62560) | Compute Expressions-struct field_by_index | 为 struct namespace 添加基于索引的字段访问 `struct.field_by_index(field_index: int)` |
| 27 | `66130b1a01` | [#63586](https://github.com/ray-project/ray/pull/63586) | Track p50/p90 of streaming scheduling-loop step duration | 添加 `Timer.percentile(p)`（KLL sketch, k=200，内存有界~几KB），`DatasetStatsSummary` 暴露 `streaming_exec_schedule_p50_s/p90_s` |
| 28 | `6058f06806` | [#63490](https://github.com/ray-project/ray/pull/63490) | Expose flag to run read tasks on isolated worker processes | PyArrow 读任务分配大量内存，worker 复用时不清除导致 OOM kill。新增 `isolate_read_workers` 标志隔离读任务 worker |
| 29 | `0ae431773b` | [#63023](https://github.com/ray-project/ray/pull/63023) | Support UDF retries for transient exceptions | 新增 `DataContext` 字段控制 map UDF 重试：`max_udf_retries`、`retry_exception_types`，允许对瞬时错误（限流、外部服务抖动）自动重试 |
| 30 | `5221b2b51c` | [#63393](https://github.com/ray-project/ray/pull/63393) | Ray Data usage metric collection | 扩展使用指标收集：从仅 `{operator_name: count}` 扩展到环境/负载特征/性能指标，`TagKey.DATA_USAGE=401` |
| 31 | `7ddc3faa76` | [#63454](https://github.com/ray-project/ray/pull/63454) | DataSourceV2 Parquet Chunked reader | V2 Parquet 读取将大文件按逻辑 chunk 分片，partitioner 携带 chunk metadata，支持细粒度并行读取 |

#### P3 — 重构/清理（5 个）

| # | Commit | PR | 描述 | 技术细节 |
|---|--------|-----|------|---------|
| 32 | `73f6edeb9b` | [#63680](https://github.com/ray-project/ray/pull/63680) | Remove safe_round from ExecutionResources hot path | `safe_round` 在每次 `ExecutionResources` 构造时 4 次取整，占调度线程 ~8% wall time。移除后存储原生精度（后被 revert） |
| 33 | `dfbabb927d` | [#63674](https://github.com/ray-project/ray/pull/63674) | Disable DataSourceV2 by default | V2 在 shuffle 密集负载下 OOM（`ReadFiles.infer_metadata()` 无 `size_bytes`，回退到 1-GiB/aggregator），默认关闭 |
| 34 | `c12fa482c7` | [#61904](https://github.com/ray-project/ray/pull/61904) | Make ConcatAggregation use polars for sorting | ConcatAggregation 内部排序实现改用 polars（如可用），性能优于 pandas |
| 35 | `205d66ddde` | [#62898](https://github.com/ray-project/ray/pull/62898) | Pre-resolve filesystem in threaded download to avoid IMDS herd | 16 线程 download 各自 `_resolve_paths_and_filesystem`，S3 下触发 ~16N 次 IMDS 凭证请求超限。改为在调度线程预解析 |
| 36 | `8ea9ede2ca` | [#63470](https://github.com/ray-project/ray/pull/63470) | Block pickle object columns in untrusted Parquet | 同 P0 #3 |

---

### Core 模块优化（9 个 commit）

#### P0 — 安全修复（1 个）

| # | Commit | PR | 描述 | 技术细节 |
|---|--------|-----|------|---------|
| 1 | `2a4e49ab98` | [#63786](https://github.com/ray-project/ray/pull/63786) | Harden runtime_env zip extraction path containment | `unzip_package()` 构建候选路径后再检查是否仍在目标目录内，防止 zip slip 路径穿越攻击 |

#### P1 — 重要 bugfix/性能优化（4 个）

| # | Commit | PR | 描述 | 技术细节 |
|---|--------|-----|------|---------|
| 2 | `dbc1523507` | [#63878](https://github.com/ray-project/ray/pull/63878) | Fix resource leaks in subprocess management | 5 个 startup 方法用 `open(os.devnull)` 但不关闭（ResourceWarning），kill process 后不 `wait()` 导致僵尸子进程 |
| 3 | `7ade33d89b` | [#63720](https://github.com/ray-project/ray/pull/63720) | Fix Python log monitor handling for same-inode truncated files | `reopen_if_necessary()` 只检查 inode 变化，原地截断重写时文件句柄位置超出新末尾，漏读新内容 |
| 4 | `f219a40b72` | [#63797](https://github.com/ray-project/ray/pull/63797) | Fix env var expansion in job submit CLI | `ray job submit` 用 `subprocess.list2cmdline` 拼参数，双引号包裹导致 POSIX shell 展开 `$VAR`。改用 `shlex.join` |
| 5 | `b9f8c9c342` | [#63744](https://github.com/ray-project/ray/pull/63744) | Normalize OTel metric labels before Prometheus export | 不同组件对同一 metric 发出异构 attribute set（如有的含 SessionName 有的不含），Prometheus exporter 前统一 normalize |

#### P2 — 功能增强（3 个）

| # | Commit | PR | 描述 | 技术细节 |
|---|--------|-----|------|---------|
| 6 | `080c19520a` | [#63329](https://github.com/ray-project/ray/pull/63329) | Publish platform events via Ray Event Recorder | 新增 Python Ray Event Exporter 框架，发布 K8s 等平台事件 |
| 7 | `f211d8a84f` | [#63932](https://github.com/ray-project/ray/pull/63932) | Compute per component memory usage in MiB | Dashboard 各组件内存用量换算为 MiB 显示 |
| 8 | `da32e00bd9` | [#63548](https://github.com/ray-project/ray/pull/63548) | Remove or raise clear error for deprecated deployment items | 移除 Serve 废弃 deployment API，使用旧式 kwargs 直接报错 |

#### P3 — 重构/清理（1 个）

| # | Commit | PR | 描述 | 技术细节 |
|---|--------|-----|------|---------|
| 9 | `bbfc73fdef` | [#62716](https://github.com/ray-project/ray/pull/62716) | Remove pydantic v1 support | **Breaking**：Ray 不再支持 pydantic v1，要求 pydantic v2。Python 3.14 兼容性所需 |

#### Core 其他重要变更（不在上方分类中）

| # | Commit | PR | 描述 | 技术细节 |
|---|--------|-----|------|---------|
| 10 | `7fa34ab6fc` | [#62583](https://github.com/ray-project/ray/pull/62583) | Halve task arg pubsub traffic | 跳过冗余 raylet pull，task arg pubsub 流量减半 |
| 11 | `889186bbf3` | [#63653](https://github.com/ray-project/ray/pull/63653) | Avoid extra memcpy when spilling fused objects | 拆分 `header + memoryview(buf)` 的拼接写入为两次独立 write，避免 Python 侧全量 memcpy |
| 12 | `d4581ee51d` | [#63839](https://github.com/ray-project/ray/pull/63839) | Batch placement group bundle removal RPCs | 移除 PG 时从逐 bundle RPC 改为 per-node batched request |
| 13 | `daffcdcc6a` | [#63685](https://github.com/ray-project/ray/pull/63685) | Consider cgroup limit when fetching cpu | 容器中 Ray 节点可用核心可能远低于硬件核心，改为读取 cgroup 约束 |
| 14 | `771638bf3e` | [#63764](https://github.com/ray-project/ray/pull/63764) | Fix replica actor zombie process after GCS restart | GCS 重启后 replica actor 僵尸进程问题修复 |
| 15 | `c6e7001847` | [#62813](https://github.com/ray-project/ray/pull/62813) | Support .tar.gz archives for remote working_dir | `working_dir` 远程 URI 新增 `.tar.gz`/`.tgz` 支持，含路径穿越防护 |
| 16 | `c836ad66b8` | [#60023](https://github.com/ray-project/ray/pull/60023) | Add IPv6 localhost and all-interfaces support (6/n) | IPv6 localhost 支持，消除 `0.0.0.0` 服务绑定 |
| 17 | `a6416a6459` | [#62393](https://github.com/ray-project/ray/pull/62393) | AMD GPU: Replace rocm-smi with amd-smi Python interface | 用 `amdsmi` Python 包替换 440 行 ctypes `libroco_smi64.so` 绑定 |

---

### Dashboard 模块优化（5 个核心 commit）

| # | Commit | PR | 描述 | 技术细节 |
|---|--------|-----|------|---------|
| 1 | `f9476b79d3` | [#63332](https://github.com/ray-project/ray/pull/63332) | Implement Frontend UI for Platform Events | 新增 Platform Events 前端页面，与后端 K8s event ingestion 模块对接 |
| 2 | `29abd06823` | [#63774](https://github.com/ray-project/ray/pull/63774) | Show TPU stats on Cluster tab | TPU worker 行显示 tensor core 利用率和 HBM 使用量 |
| 3 | `6a8600aef2` | [#63998](https://github.com/ray-project/ray/pull/63998) | Fix TPU metrics | 修复 TPU 指标采集逻辑 |
| 4 | `c5f985ec85` | [#63852](https://github.com/ray-project/ray/pull/63852) | Add py-spy --idle and --subprocesses flags to profiling | Dashboard profiling 端点透传 `py-spy` 的 `--idle` 和 `--subprocesses` 标志 |
| 5 | `b8329debc2` | [#62314](https://github.com/ray-project/ray/pull/62314) | Add platform events module with K8s event ingestion | 新增 `platform_events` head module，Provider 模式支持 K8s 事件监控 |

#### Dashboard 其他改进（9 个）

| # | Commit | PR | 描述 |
|---|--------|-----|------|
| 6 | `9bb792a835` | [#63211](https://github.com/ray-project/ray/pull/63211) | Pass Grafana cluster filter to Serve metrics URLs |
| 7 | `a3f6235bdd` | [#63729](https://github.com/ray-project/ray/pull/63729) | Guard against zero num_cpus in k8s_utils.cpu_percent |
| 8 | `399101cc35` | [#62637](https://github.com/ray-project/ray/pull/62637) | Fix unexpected log line details pop-up in log viewer UI |
| 9 | `8656da64cb` | [#62257](https://github.com/ray-project/ray/pull/62257) | Add Name column to Jobs view from job_name metadata |
| 10 | `f480619e8b` | [#62297](https://github.com/ray-project/ray/pull/62297) | Add unexpected worker failure metric and dashboard panel |
| 11 | `d855e5db37` | [#63111](https://github.com/ray-project/ray/pull/63111) | Add host vs container memory usage distinction |
| 12 | `2b30f210c2` | [#63618](https://github.com/ray-project/ray/pull/63618) | Show last data load time |
| 13 | `22fbcd1aea` | [#63364](https://github.com/ray-project/ray/pull/63364) | Surface WebSocket close codes and errors in job log streaming |
| 14 | `68f71ee2a7` | [#62214](https://github.com/ray-project/ray/pull/62214) | Add instance filter to GPU usage metric query |

---

## 第二部分：迁移合并具体命令

### Phase 0：准备工作

```bash
# 1. 确保远程分支最新
git fetch ray

# 2. 创建工作分支（基于 2.56.1）
git checkout -b release-2.56.1-rebase ray/releases/2.56.1

# 3. 验证基线
git log --oneline -1  # 应显示 2.56.1 版本号
```

### Phase 1：安全移植定制系统（低冲突）

#### Batch 1.1：Build & Docs（无代码冲突）

```bash
# 编译脚本（_version.py 需手动解决版本号冲突）
git cherry-pick 91942ca4dd   # Support bazel 7.5.0 and ray 2.55.1 version
# 注意：此 commit 会改 _version.py，冲突时保留 2.56.1 版本号
git cherry-pick 416d526a2c   # Add compile build script

# CLAUDE.md（纯文档）
git cherry-pick 305ffc6970   # Init claude.md
git cherry-pick 392b8f32f2   # Enhance claude md

# .gitignore
git cherry-pick d77c74e161   # Add .clangd and .codeflicker/ to .gitignore
```

#### Batch 1.2：Kafka source/sink（新文件）

```bash
git cherry-pick 8192c19564   # Add opentelemetry-distro, pyroaring, kafka-python-ng to extras
git cherry-pick eb3ec46654   # Introduce kafka source and sink
```

#### Batch 1.3：ExecutionConfig store（新文件为主）

```bash
git cherry-pick fb87747ae1   # Isolate ExecutionConfig storage by dataset_id
git cherry-pick cc0cba9234   # Support custom kconf_key for execution config store
git cherry-pick 8c381d979e   # Add "job_" prefix for job_ids
git cherry-pick f0c5b50daa   # Use STAGE_PROD for kconf write operations
```

#### Batch 1.4：Watchdog block patch（新文件为主）

```bash
git cherry-pick 2a21417560   # Port speculative-execution watchdog patch
git cherry-pick 406970f54c   # Rewrite watchdog_block_patch lean (665→450)
git cherry-pick 20939adb90   # watchdog_block_patch: per-op kill ratio cap
```

#### Batch 1.5：Checkpoint 系统（需适配 BlockEntry，此阶段先尝试）

```bash
# 以下 commit 可能因 BlockEntry refactor 冲突，如遇冲突则跳到 Phase 2
git cherry-pick 39e162fc19   # Checkpoint support deduplication
git cherry-pick 3773ea1568   # Update checkpoint_filter.py
git cherry-pick d0ebaa5cdc   # Remove redis kuaishou infra
git cherry-pick 7d97800589   # Redis storage add roaring bitmap
git cherry-pick b184df65c2   # Add roaring bitmap64 for load checkpoint
git cherry-pick e530bb927c   # Add Bloom Filter checkpoint backend
git cherry-pick 86e71104a1   # Fix ArrowCapacityError in checkpoint dedup
git cherry-pick 8de246c2b2   # Support checkpoint filter error file
git cherry-pick 0d5035bcd0   # checkpoint_writer 修复（如有）
git cherry-pick a5ba848d71   # checkpoint_writer 修复
git cherry-pick 26577bea49   # checkpoint_filter 修复
git cherry-pick 0718da6905   # checkpoint_writer 修复
git cherry-pick eb5193ce3a   # checkpoint_writer 修复
git cherry-pick 80d4b36e32   # Redis 存储添加 roaring bitmap
git cherry-pick 04b6c52430   # 添加 roaring bitmap64
# 如有冲突，解决 BlockEntry 适配：
# 全局替换 (ref, metadata) → BlockEntry API
# 冲突文件中：tuple[0] → block_entry.ref, tuple[1] → block_entry.metadata
```

#### Batch 1.6：其他 Data 定制

```bash
# 可丢弃的社区 cherry-pick（2.56.1 已包含）
# SKIP: e6d240a3d8  (regular pickle - 2.56.1 已有)
# SKIP: 3c60006ed3  (Cache Arrow schemas - 2.56.1 已有)
# SKIP: 1659cde75e  (Fix GPU shuffle ordering - 2.56.1 已有)
# SKIP: 07baebd07a  (Yield only first schema - 2.56.1 已有)
# SKIP: 6fcc3f4298  (Deflake test_dashboard_port_conflict - 2.56.1 已有)
# SKIP: 232459596f  (Disable hanging issue detection - 2.56.1 已有)

# Data 其他功能
git cherry-pick 42d14ef48c   # Support configure actor autoscaler type
git cherry-pick 543aee5224   # Add NoOpClusterAutoscaler
git cherry-pick 53791c628e   # Fix NoOpClusterAutoscaler
git cherry-pick 40146473b1   # Add filter_fn/filter_expr to write_datasink
git cherry-pick db0765e178   # Always tee iterator in generate_write_fn
git cherry-pick 11eacf87ea   # Fix Prometheus query timeout for /api/data/datasets
git cherry-pick 83335a061c   # Fix missing column block AttributeError
git cherry-pick da43379f92   # Support list<null> and list<struct> merge
git cherry-pick aa4c7d793f   # Add configurable CSC upload path
```

**Phase 1 验证**：
```bash
# 编译验证
bazel build //:ray_pkg 2>&1 | tail -5

# Python 基础验证
python -c "import ray; print(ray.__version__)"
pytest python/ray/data/tests/test_checkpoint.py -x -q 2>&1 | tail -20
```

---

### Phase 2：Data 模块核心冲突解决

#### Batch 2.1：执行引擎适配（CRITICAL 冲突区域）

```bash
# Autoscaling actor pool（可能与 BlockEntry + subcluster 冲突）
git cherry-pick 2a7c40b02f   # Track draining actors as a set
# 冲突解决：保留 2.56.1 的 BlockEntry API，追加 master 的 draining_set 逻辑

git cherry-pick 4fc6f0e540   # Clamp initial_size on actor pool config update
# 冲突解决：在 2.56.1 的 actor_pool_map_operator.py 中追加 clamp 逻辑

git cherry-pick 64a5e8e936   # Rank actors per node in a heap
# 注意：2.56.1 可能已部分包含此功能，检查是否 SKIP

# 执行引擎 metrics
git cherry-pick b0d848514d   # Add ray data output rows metric
# 冲突解决：physical_operator.py 中 BlockEntry 变更，需适配 metric 注册点

git cherry-pick 1f28b20dee   # Add num_errored_blocks metrics per operator
# 冲突解决：op_runtime_metrics.py 中 2.56.1 改用 DistributionTracker，
# master 的 errored_blocks 指标需适配到新 tracker

git cherry-pick 8168bfcba9   # Add Input metrics
# 冲突解决：streaming_executor.py 中合并两方 metric 追踪逻辑

git cherry-pick 6dc4d4ad71   # Ray add perf
# 冲突解决：streaming_executor.py / map_operator.py 中追加 perf hook

git cherry-pick a01e773790   # Perf count zero fixed（内部定制文件，无冲突）
git cherry-pick 9ee9ac663d   # Handle missing RUSAGE_THREAD for macOS
```

#### Batch 2.2：GPU Shuffle & 其他 Data

```bash
# SKIP: 1659cde75e  (GPU shuffle ordering - 2.56.1 已有)
# SKIP: e6d240a3d8  (pickle - 2.56.1 已有)
# SKIP: 3c60006ed3  (schema cache - 2.56.1 已有)

# GPU shuffle 其他（如有冲突需适配 subcluster label）
# git cherry-pick <GPU shuffle 相关 commit>  # 按需处理
```

**Phase 2 验证**：
```bash
pytest python/ray/data/tests/test_streaming_executor.py -x -q 2>&1 | tail -20
pytest python/ray/data/tests/test_map_operator.py -x -q 2>&1 | tail -20
pytest python/ray/data/tests/test_actor_pool_map_operator.py -x -q 2>&1 | tail -20
```

---

### Phase 3：Core/Metric 模块冲突解决

```bash
# OTel prometheus exporter（CRITICAL 冲突）
git cherry-pick 117282c805   # Support OTel metric prometheus exporter
# 冲突解决：open_telemetry_metric_recorder.py 中 2.56.1 有 label normalization，
# 需将 master 的 NodeID tag 和 metric filter 适配到 normalization 后的 label 体系
# 关键：确保 NodeID 经过 normalize 后 label key 仍正确

git cherry-pick 1f5af1f68f   # Add NodeID tag for metric
# 冲突解决：reporter_agent.py 中合并 2.56.1 的 TPU/metrics 变更

git cherry-pick d9fa65c27b   # Support metric filter and set metric export default value

# Worker port randomization
git cherry-pick c9aeeeff06   # Worker port to random
# 冲突解决：worker_pool.cc 中合并 2.56.1 的 timing 逻辑变更
```

**Phase 3 验证**：
```bash
pytest python/ray/tests/test_open_telemetry_metric_recorder.py -x -q 2>&1 | tail -20
```

---

### Phase 4：Dashboard 模块冲突解决

```bash
# Dashboard 优化（可能与 TPU stats/Platform Events 冲突）
git cherry-pick 61d2e194d9   # column_width_adapt
# 冲突解决：NodeRow.tsx 中合并 2.56.1 的 TPU 列

git cherry-pick dceab7b0f4   # Fix get pod name
# 冲突解决：reporter_agent.py 中合并 2.56.1 的 TPU metrics

git cherry-pick f59bfb8f8f   # Dashboard optimization
git cherry-pick 643a590d3b   # Dashboard optimization
# 冲突解决：reporter_models.py 需适配 pydantic v2

git cherry-pick d9fa65c27b   # Support metric filter（如 Phase 3 未处理）

git cherry-pick 9ec99dee17   # Add locate mode for log search
# 冲突解决：ActorTable.tsx / LogView 中合并 2.56.1 的 UI 变更
```

**Phase 4 验证**：
```bash
pytest python/ray/dashboard/tests/test_dashboard.py -x -q 2>&1 | tail -20
# 前端验证
cd python/ray/dashboard/client && npm run build
```

---

### Phase 5：收尾验证

```bash
# 1. 全量 Python 测试
pytest python/ray/data/tests/ -x -q --timeout=300 2>&1 | tail -30
pytest python/ray/tests/ -x -q --timeout=300 -k "not serve" 2>&1 | tail -30
pytest python/ray/dashboard/tests/ -x -q 2>&1 | tail -20

# 2. C++ 编译验证
bazel build //:ray_pkg 2>&1 | tail -10

# 3. 版本号验证
python -c "import ray; assert ray.__version__.startswith('2.56'), ray.__version__"

# 4. 依赖安装验证
pip install -e ".[default]" 2>&1 | tail -5
python -c "import ray; ray.init(ignore_reinit_error=True)"

# 5. 对比 master 定制完整性
git diff master --stat | tail -5   # 应仅剩版本号和基线差异

# 6. 打 tag
git tag phase5-verified

# 7. 合并到 master
git checkout master
git merge release-2.56.1-rebase --ff-only  # 如果是线性历史
# 或
git merge release-2.56.1-rebase  # 如果有 merge
```

---

### 冲突解决速查：常见模式与命令

```bash
# 查看 2.56.1 中 BlockEntry 的 API
git show 886ae8b8c9 -- python/ray/data/_internal/block.py | head -80

# 查看某文件在 2.56.1 的完整内容（冲突参考）
git show ray/releases/2.56.1:python/ray/data/_internal/execution/operators/map_operator.py

# 跳过无法解决的 cherry-pick（后续手动补）
git cherry-pick --skip

# 中断当前 cherry-pick
git cherry-pick --abort

# 查看当前分支与 master 的差异（确保定制完整）
git diff master -- python/ray/data/_internal/execution/operators/map_operator.py

# 全局搜索需适配 BlockEntry 的位置
rg "tuple\[0\]|tuple\[1\]|\[0\]\.ref\|\.metadata" --type py python/ray/data/checkpoint/

# 回滚到上一个 Phase tag
git reset --hard phase3-done
```

---

## 第三部分：master 50 个定制 commit 完整列表（时间顺序）

| # | Commit | 模块 | 类型 | 描述 | 2.56.1 冲突风险 | 操作 |
|---|--------|------|------|------|----------------|------|
| 1 | `42d14ef48c` | Data | feature | Support configure actor autoscaler type | MEDIUM | cherry-pick |
| 2 | `8c381d979e` | Data | bugfix | Add "job_" prefix for job_ids | LOW | cherry-pick |
| 3 | `8de246c2b2` | Data | feature | Support checkpoint filter error file | MEDIUM | cherry-pick |
| 4 | `1f28b20dee` | Data | feature | Add num_errored_blocks metrics | HIGH | cherry-pick + 适配 |
| 5 | `117282c805` | Core | feature | Support OTel metric prometheus exporter | CRITICAL | cherry-pick + 适配 |
| 6 | `416d526a2c` | Build | chore | Add compile build script | LOW | cherry-pick |
| 7 | `305ffc6970` | Docs | chore | Init claude.md | NONE | cherry-pick |
| 8 | `b0d848514d` | Data | feature | Add ray data output rows metric | HIGH | cherry-pick + 适配 |
| 9 | `1f5af1f68f` | Core | feature | Add NodeID tag for metric | CRITICAL | cherry-pick + 适配 |
| 10 | `b184df65c2` | Data | feature | Add roaring bitmap64 for checkpoint | HIGH | cherry-pick + 适配 |
| 11 | `7d97800589` | Data | feature | Redis storage add roaring bitmap | HIGH | cherry-pick + 适配 |
| 12 | `d0ebaa5cdc` | Data | refactor | Remove redis kuaishou infra | HIGH | cherry-pick + 适配 |
| 13 | `3773ea1568` | Data | bugfix | Update checkpoint_filter.py | HIGH | cherry-pick + 适配 |
| 14 | `39e162fc19` | Data | feature | Checkpoint support deduplication | HIGH | cherry-pick + 适配 |
| 15 | `543aee5224` | Data | feature | Add NoOpClusterAutoscaler | MEDIUM | cherry-pick |
| 16 | `40146473b1` | Data | feature | Add filter_fn/filter_expr to write_datasink | MEDIUM | cherry-pick |
| 17 | `53791c628e` | Data | bugfix | Fix NoOpClusterAutoscaler | MEDIUM | cherry-pick |
| 18 | `643a590d3b` | Dashboard | optimization | Dashboard optimization | MEDIUM | cherry-pick + 适配 pydantic v2 |
| 19 | `f59bfb8f8f` | Dashboard | optimization | Dashboard optimization | MEDIUM | cherry-pick + 适配 pydantic v2 |
| 20 | `d9fa65c27b` | Core | feature | Support metric filter | HIGH | cherry-pick + 适配 |
| 21 | `392b8f32f2` | Docs | chore | Enhance claude md | NONE | cherry-pick |
| 22 | `aa4c7d793f` | Build | chore | Add configurable CSC upload path | LOW | cherry-pick |
| 23 | `dceab7b0f4` | Dashboard | bugfix | Fix get pod name | HIGH | cherry-pick + 适配 |
| 24 | `61d2e194d9` | Dashboard | chore | Column width adapt | MEDIUM | cherry-pick + 适配 |
| 25 | `c9aeeeff06` | Core | feature | Worker port to random | MEDIUM | cherry-pick + 适配 |
| 26 | `8168bfcba9` | Data | feature | Add Input metrics | HIGH | cherry-pick + 适配 |
| 27 | `9ec99dee17` | Dashboard | feature | Add locate mode for log search | MEDIUM | cherry-pick |
| 28 | `64a5e8e936` | Data | optimization | Rank actors per node in heap | LOW | **SKIP**（2.56.1 已有） |
| 29 | `1659cde75e` | Data | bugfix | Fix GPU shuffle output ordering | NONE | **SKIP**（2.56.1 已有） |
| 30 | `07baebd07a` | Data | bugfix | Yield only first schema in _map_task | NONE | **SKIP**（2.56.1 已有） |
| 31 | `e6d240a3d8` | Data | optimization | Regular pickle before task return | NONE | **SKIP**（2.56.1 已有） |
| 32 | `3c60006ed3` | Data | optimization | Cache deserialized Arrow schemas | NONE | **SKIP**（2.56.1 已有） |
| 33 | `2a7c40b02f` | Data | optimization | Track draining actors as set | HIGH | cherry-pick + 适配 |
| 34 | `2a21417560` | Data | feature | Port speculative-execution watchdog | LOW | cherry-pick |
| 35 | `406970f54c` | Data | refactor | Rewrite watchdog_block_patch lean | LOW | cherry-pick |
| 36 | `20939adb90` | Data | feature | Watchdog per-op kill ratio cap | LOW | cherry-pick |
| 37 | `e530bb927c` | Data | feature | Add Bloom Filter checkpoint backend | HIGH | cherry-pick + 适配 |
| 38 | `86e71104a1` | Data | bugfix | Fix ArrowCapacityError in checkpoint | HIGH | cherry-pick + 适配 |
| 39 | `db0765e178` | Data | feature | Always tee iterator in generate_write_fn | MEDIUM | cherry-pick |
| 40 | `11eacf87ea` | Data | bugfix | Fix Prometheus query timeout | MEDIUM | cherry-pick |
| 41 | `91942ca4dd` | Build | chore | Support bazel 7.5.0 and ray 2.55.1 | LOW | cherry-pick（版本号保留 2.56.1） |
| 42 | `fb87747ae1` | Data | feature | Isolate ExecutionConfig storage | MEDIUM | cherry-pick |
| 43 | `6dc4d4ad71` | Data | feature | Ray add perf | HIGH | cherry-pick + 适配 |
| 44 | `cc0cba9234` | Data | feature | Custom kconf_key for config store | MEDIUM | cherry-pick |
| 45 | `9ee9ac663d` | Data | bugfix | Handle missing RUSAGE_THREAD | LOW | cherry-pick |
| 46 | `8192c19564` | Build | chore | Add OTel/pyroaring/kafka to extras | MEDIUM | cherry-pick |
| 47 | `d77c74e161` | Chore | chore | Add .clangd and .codeflicker/ to .gitignore | LOW | cherry-pick |
| 48 | `a01e773790` | Data | bugfix | Perf count zero fixed | LOW | cherry-pick |
| 49 | `4fc6f0e540` | Data | bugfix | Clamp initial_size on actor pool | HIGH | cherry-pick + 适配 |
| 50 | `f0c5b50daa` | Data | bugfix | Use STAGE_PROD for kconf write | LOW | cherry-pick |

---

## 2.56.1 Breaking Changes 对内部定制的影响

| Breaking Change | 影响的内部定制 | 适配方案 |
|----------------|--------------|---------|
| `BlockEntry` 替代 `(ref, metadata)` 元组 | Checkpoint 全模块、metrics 引用 | 逐文件适配到 BlockEntry API：`tuple[0]` → `entry.ref`，`tuple[1]` → `entry.metadata` |
| `__subcluster__` → `ray-subcluster` 标签重命名 | Checkpoint filter 中引用 label key | `rg "__subcluster__" --type py` 全局替换 |
| pydantic v1 移除 | Dashboard reporter_models/log_manager | `BaseModel` → `pydantic_v2`，`validator` → `field_validator` |
| ConcurrencyCapBackpressurePolicy deprecated | draining-actor-set 追踪 | 短期保留在 deprecated 路径 |
| `drop_columns` → `Project` 逻辑算子 | ExecutionConfig controller 中算子类型判断 | 更新 `isinstance(op, Project)` 判断 |
| OTel metric label normalization | NodeID tag、metric filter | 确保 NodeID 经 `_normalize_label_key()` 后正确 |

---

## 总体工期估算

| 阶段 | 工期 | 风险 |
|------|------|------|
| Phase 0：准备 | 0.5 天 | 低 |
| Phase 1：安全移植 | 1-2 天 | 低 |
| Phase 2：Data 核心 | 3-4 天 | **高** |
| Phase 3：Core/Metric | 1-2 天 | 中 |
| Phase 4：Dashboard | 1 天 | 低 |
| Phase 5：收尾验证 | 1-2 天 | 中 |
| **合计** | **7-11 天** | |

---

## 回滚策略

1. 工作分支 `release-2.56.1-rebase` 始终可丢弃，master 不受影响
2. 每个 Phase 完成后打 tag：`phase1-done`、`phase2-done` 等
3. 遇到不可解决的冲突时，可跳过单个 commit（`git cherry-pick --skip`），后续手动补全
4. 最终合并前，可随时 `git diff master release-2.56.1-rebase` 对比所有定制是否保留

```bash
# Phase tag 命令
git tag phase1-done
git tag phase2-done
# ...

# 回滚到上一个 Phase
git reset --hard phase2-done

# 完全重来
git checkout -b release-2.56.1-rebase-v2 ray/releases/2.56.1
```

---

## 2.56.1 新增功能决策清单

| 2.56.1 新特性 | 是否采纳 | 理由 |
|--------------|---------|------|
| BlockEntry refactor | **必须** | 基础架构变更，不采纳则无法升级 |
| DataSourceV2 (默认禁用) | 采纳但默认关闭 | 2.56.1 已默认禁用，无风险 |
| iter_batches spilling fix (2/n) | **必须** | P1 bugfix，内存泄漏修复 |
| Platform Events UI | 采纳 | 增量功能，与 master 无冲突 |
| TPU stats | 采纳 | 增量功能 |
| subcluster partition clustering | 采纳但需适配 | master 的 checkpoint 可能引用旧 `__subcluster__` label key |
| WeightedRoundRobinPartitioner | 采纳 | 增量重构 |
| pydantic v1 移除 | **必须**适配 | 基础依赖升级 |
| OTel label normalization | 采纳并适配 | 须确保 NodeID tag 经 normalize 后正确 |
| Starlette 安全更新 | **必须** | 安全漏洞 |
| UDF retries for transient exceptions | 采纳 | 增量功能，新 DataContext 字段 |
| hash_partition 性能优化 | 采纳 | 3-5× 性能提升 |
| runtime_env zip path containment | **必须** | 安全修复 |
| IPv6 支持 | 采纳 | 增量功能 |
