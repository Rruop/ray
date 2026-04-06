# Ray v2.54.0 → v2.55.1 Cherry-Pick 方案（按分支 commit 顺序）

## 概览

### 背景

本地 master 分支与社区 `releases/2.55.1` 在 `d60d13127d`（2026-02-06）处分叉。此后社区 master 经历了约 2 个月的活跃开发直到 `4127cd62cd`（2026-04-02）切出 2.55.1 分支。这期间共有 **454 个 Core/Data/Dashboard/Serve 相关 commit** 缺失于本地 master。

### 数量概览

| 类别 | 数量 | 说明 |
|------|------|------|
| 本地 master commit | 50 | 14 个 2.54.x cherry-pick + 36 个内部定制 |
| 缺失 commit 总数 | 454 | master 相对于 2.55.1 缺失的 Core/Data/Dashboard/Serve commit |
| 表格1（需迁移 commit） | 264 | 需评估 cherry-pick 的功能/修复/重构 commit |
| 表格3（差集 commit） | 190 | 测试/CI/文档/RDT/benchmark，一般无需 cherry-pick |
| 2.54.x 已有 commit | 11 | master 通过 2.54.x cherry-pick 已包含，无需重复 pick |

### 优先级分布

| 优先级 | 数量 | 必要性 |
|--------|------|--------|
| **P0** | 12 | **必须迁移** — 安全漏洞（Arrow RCE）、崩溃（SIGABRT）、死锁（pg.ready）、数据正确性（OOM、溢出）、Serve 自动扩缩/请求挂起 |
| **P1** | 37 | **强烈建议** — 关键 bugfix、性能优化（调度循环 -57%、Actor 排名 O(N*logM)、反压修复、Serve P99 延迟回归） |
| **P2** | 100 | 按需选取 — 功能增强、中等优化、Dashboard 改进 |
| **P3** | 77 | 可延后 — 重构、清理、日志改进 |
| **skip** | 38 | 跳过 — revert 对、纯测试/CI 调整 |

## 本地 master 分支 commit（共 50 个）

本地 master 包含 `ray/releases/2.54.0` 全部内容 + 以下 50 个 commit（14 个 2.54.x cherry-pick + 36 个内部定制）：

#### 2.54.x Cherry-Pick（14 个）

| # | Commit | 原始 PR | 说明 |
|---|--------|---------|------|
| 1 | `f8e1102869` | [#60852](https://github.com/ray-project/ray/pull/60852) | Core 测试修复（Windows） |
| 2 | `6835277714` | [#60933](https://github.com/ray-project/ray/pull/60933) | RLlib 禁用 flaky 测试 |
| 3 | `165b4aace9` | [#60997](https://github.com/ray-project/ray/pull/60997) | Data min_scheduling_resources 回退修复 |
| 4 | `48bd1f8fa4` | [#61064](https://github.com/ray-project/ray/pull/61064) | Data revert task state 获取变更 |
| 5 | `1ea4980a1d` | [#61157](https://github.com/ray-project/ray/pull/61157) | Docker 依赖更新 |
| 6 | `d049885322` | - | Git LFS 大文件忽略 |
| 7 | `673873ccf2` | - | Git LFS 大文件追踪 |
| 8 | `ac1a567e9f` | - | Kafka source/sink 引入 |
| 9-14 | ... | [#60754](https://github.com/ray-project/ray/pull/60754), [#60745](https://github.com/ray-project/ray/pull/60745), [#60798](https://github.com/ray-project/ray/pull/60798), [#60784](https://github.com/ray-project/ray/pull/60784), [#60882](https://github.com/ray-project/ray/pull/60882), [#60881](https://github.com/ray-project/ray/pull/60881) | Serve/Core/Data 各类修复 |

#### 内部定制 commit（36 个）

| # | Commit | 模块 | 分类 | 描述 |
|---|--------|------|------|------|
| 1 | `8d6941d61e` | Core | feature | 添加 worker-finished-but-unconsumed task 诊断信息 |
| 2 | `68111c41d6` | Core | feature | Worker 端口随机化 |
| 3 | `c8d8156434` | Dashboard | feature | Dashboard 列宽自适应 |
| 4 | `6e72a872f5` | Dashboard | bugfix | 修复 Dashboard 获取 pod name |
| 5 | `9dd171693b` | Core | feature | 可配置 CSC 上传路径和统一环境变量命名 |
| 6 | `d6c26393cb` | Docs | other | 增强 CLAUDE.md |
| 7 | `cd008b03d6` | Dashboard | feature | 支持 metric 过滤和设置 metric 导出默认值 |
| 8 | `c162f7a870` | Core | bugfix | 修复 dashboard node_head API 死节点缓存（同社区 [#61185](https://github.com/ray-project/ray/pull/61185)） |
| 9 | `c5314b1d38` | Dashboard | optimization | Dashboard 优化 |
| 10 | `eb4f846be0` | Dashboard | optimization | Dashboard 优化 |
| 11 | `5eeb11d0ba` | Core | bugfix | 修复 NoOpClusterAutoscaler 返回实际集群资源 |
| 12 | `4a3dd26e03` | Data | feature | write_datasink 添加 filter_fn 和 filter_expr 参数 |
| 13 | `3b222b08b4` | Data | feature | 添加 post-checkpoint filter 用于过滤数据追踪 |
| 14 | `f1a537ee3e` | Core | feature | 添加 NoOpClusterAutoscaler 禁用集群自动扩缩 |
| 15 | `f539b119a6` | Data | feature | Checkpoint 支持去重 |
| 16 | `66984c8933` | Data | bugfix | MetadataOpTask.on_task_finished() 异常捕获 |
| 17 | `8f3317f77a` | Data | bugfix | 更新 checkpoint_filter.py |
| 18 | `a9b3fb42bc` | Data | optimization | 添加 on_data_ready 计时到 SCHED_PROFILE |
| 19 | `1370e6a0f4` | Data | optimization | 自适应调度分发间隔（大/小规模） |
| 20 | `6963b2d4cd` | Data | optimization | GPU 优化的交错调度分发 |
| 21 | `45aebeff78` | Data | refactor | 移除 redis 快手基础设施 |
| 22 | `0d5035bcd0` | Data | bugfix | 更新 checkpoint_writer.py |
| 23 | `26577bea49` | Data | bugfix | 更新 checkpoint_filter.py |
| 24 | `a5ba848d71` | Data | bugfix | 移除 try catch |
| 25 | `0718da6905` | Data | bugfix | 更新 checkpoint_writer.py |
| 26 | `eb5193ce3a` | Data | bugfix | 更新 checkpoint_writer.py |
| 27 | `80d4b36e32` | Data | feature | Redis 存储添加 roaring bitmap |
| 28 | `04b6c52430` | Data | feature | 添加 roaring bitmap64 用于 load checkpoint 数据 |
| 29 | `334ae42b8d` | Core | feature | Metric 添加 NodeID tag 标识逻辑节点 |
| 30 | `53cc4fab03` | Data | feature | 添加 Ray Data output rows 指标 |
| 31 | `ac227093f7` | Docs | other | 初始化 CLAUDE.md |
| 32 | `1b32d8e5d7` | Build | feature | 添加编译构建脚本 |
| 33 | `7178ee9bba` | Core | feature | 支持 OpenTelemetry metric prometheus exporter |
| 34 | `da43379f92` | Data | bugfix | 支持 list\<null\> 和 list\<struct\> 类型合并 |
| 35 | `854dfa2fd3` | Data | feature | 添加每算子 num_errored_blocks 指标和 Dashboard 展示 |
| 36 | `83335a061c` | Data | bugfix | 修复拼接缺失列 block 时 AttributeError |
| 37 | `774ce364e7` | Data | feature | 支持 checkpoint filter 错误文件 |
| 38 | `9ce67a77e5` | Core | feature | job_id 不以 "job_" 开头时自动添加前缀 |
| 39 | `9576712b28` | Core | feature | 支持配置 actor autoscaler 类型 |
| 40 | `de232a1dcd` | Data | feature | 执行配置 REST API 用于动态算子参数调整 |
| 41 | `f87adb31d0` | Data | feature | 实现 ExecutionConfigController 支持动态运行时配置 |
| 42 | `0fee8ad737` | Data | feature | 支持用户自定义 operatorName 和 OperatorID |

> **cherry-pick 冲突热点**：内部定制中 Data checkpoint（#15-17, 22-28）、调度优化（#18-20）、Dashboard（#3, 7, 9-10）、Metric（#29-30, 33）等模块与社区 2.55.1 有交叉改动，cherry-pick 时需重点关注冲突。

## 表格 1：需要迁移的 Core/Data/Dashboard commit（按分支 commit 顺序）

> 涉及 Core、Data、Dashboard 模块共 264 个 commit，按分支上的真实 commit 顺序排列。
> Serve 模块 commit 不在本表中，见表格2和表格3。
>
> - 重要性：**P0**（安全/崩溃/死锁）> **P1**（bugfix/性能关键）> **P2**（功能增强/中等优化）> **P3**（重构/清理）> **skip**（测试/CI/文档/可跳过）
> - 模块：Core / Data / Core+Data / Dashboard

| # | Commit | PR | 模块 | 分类 | 重要性 | 描述 | Cherry-Pick 命令 |
|---|--------|-----|------|------|--------|------|------------------|
| 1 | `4a024799d7` | [#60736](https://github.com/ray-project/ray/pull/60736) | Core | build | P2 | Bump python/java protobuf 版本到 3.20.3 | `git cherry-pick 4a024799d7` |
| 2 | `c0fc46b849` | [#58459](https://github.com/ray-project/ray/pull/58459) | Core | feature | P2 | 添加 Python 3.14 递归限制处理支持 | `git cherry-pick c0fc46b` |
| 3 | `aec74f035d` | [#60711](https://github.com/ray-project/ray/pull/60711) | Data | bugfix | P2 | 修复 `AliasExpr` 结构相等性判断，正确处理 rename 标记 | `git cherry-pick aec74f0` |
| 4 | `db822f500e` | [#60647](https://github.com/ray-project/ray/pull/60647) | Core | refactor | P3 | 移除已废弃的 `local_mode` 支持 | `git cherry-pick db822f5` |
| 5 | `c7a2db94af` | [#60798](https://github.com/ray-project/ray/pull/60798) | Data | bugfix | P1 | 修复输出反压解锁序列，正确处理终端算子 | `git cherry-pick c7a2db9` |
| 6 | `f7425043ec` | [#60712](https://github.com/ray-project/ray/pull/60712) | Core | feature | P2 | 添加本地加载类的 Actor 转换逻辑 | `git cherry-pick f742504` |
| 7 | `a687ba4444` | [#60779](https://github.com/ray-project/ray/pull/60779) | Core | feature | P2 | Ray sync server 使用 AuthenticationValidator | `git cherry-pick a687ba4444` |
| 8 | `c537a447b1` | [#60760](https://github.com/ray-project/ray/pull/60760) | Core | feature | skip | 添加 ReferenceCounter 内部状态调试 API（后被 revert） | - |
| 9 | `98b56eae15` | [#60752](https://github.com/ray-project/ray/pull/60752) | Core | refactor | P3 | 资源隔离 [2/n]：修改内存监控器支持 mock 测试 | `git cherry-pick 98b56ea` |
| 10 | `5564461905` | [#60792](https://github.com/ray-project/ray/pull/60792) | Core | refactor | skip | Cython 方法清理和文档化 | `git cherry-pick 5564461` |
| 11 | `b93fc26472` | [#60803](https://github.com/ray-project/ray/pull/60803) | Data | docs | skip | 添加 MOD 操作文档 | - |
| 12 | `f3d444ab01` | [#60818](https://github.com/ray-project/ray/pull/60818) | Core | revert | skip | Revert [#60760](https://github.com/ray-project/ray/pull/60760) | - |
| 13 | `3139d0d897` | [#60657](https://github.com/ray-project/ray/pull/60657) | Core | optimization | P1 | **显著提升 pg.ready() 性能**：用 async GCS RPC 替代 dummy task | `git cherry-pick 3139d0d` |
| 14 | `4a8845de51` | [#60849](https://github.com/ray-project/ray/pull/60849) | Core | refactor | P3 | 将共享测试工具从 `_private` 迁移到 `_common` | `git cherry-pick 4a8845d` |
| 15 | `10ddd43451` | [#60288](https://github.com/ray-project/ray/pull/60288) | Core | feature | P2 | 填充 Actor 和 Task 事件中缺失的字段（part 2） | `git cherry-pick 10ddd43` |
| 16 | `0d024e63b7` | [#60882](https://github.com/ray-project/ray/pull/60882) | Data | bugfix | P1 | 修复 `ReservationOpResourceAllocator` 对 ActorPoolMapOperator 资源借用错误 | `git cherry-pick 0d024e6` |
| 17 | `7faa24f55d` | [#60881](https://github.com/ray-project/ray/pull/60881) | Data | bugfix | P1 | 防止 `Limit` 被推过 `map_groups`，避免语义错误 | `git cherry-pick 7faa24f` |
| 18 | `7808569e40` | [#60811](https://github.com/ray-project/ray/pull/60811) | Core | bugfix | P2 | 修复 dashboard event agent 缺少 http scheme 报错 | `git cherry-pick 7808569e40` |
| 19 | `7ecbca7da1` | [#60695](https://github.com/ray-project/ray/pull/60695) | Data | feature | P2 | 表达式中添加 cast 类型转换方法 | `git cherry-pick 7ecbca7` |
| 20 | `3e1de42ad2` | [#60630](https://github.com/ray-project/ray/pull/60630) | Data | docs | skip | 修复 dataset.py 文档字符串 | - |
| 21 | `e5b38df7f9` | [#60578](https://github.com/ray-project/ray/pull/60578) | Data | test | skip | 组织 test_formats.py 测试 | - |
| 22 | `0a0d40fbcf` | [#60652](https://github.com/ray-project/ray/pull/60652) | Core | test | skip | 将 named actor 集成测试转为单元测试 | - |
| 23 | `b4d21ea6ec` | [#60905](https://github.com/ray-project/ray/pull/60905) | Data | refactor | P3 | OpMetrics 回退为 JSON 视图 | `git cherry-pick b4d21ea` |
| 24 | `4a008742e5` | [#60899](https://github.com/ray-project/ray/pull/60899) | Data | optimization | P3 | 为 autoscaler/resource_allocator 创建添加日志 | `git cherry-pick 4a00874` |
| 25 | `021e7e10e9` | [#60631](https://github.com/ray-project/ray/pull/60631) | Data | optimization | skip | 禁用 UnionOperator 节流（后被 revert） | - |
| 26 | `ed6a8a2632` | [#60850](https://github.com/ray-project/ray/pull/60850) | Core | bugfix | P1 | **修复取消 head task 后 actor 任务队列阻塞** | `git cherry-pick ed6a8a2` |
| 27 | `37f2eb4a21` | [#60658](https://github.com/ray-project/ray/pull/60658) | Core | bugfix | P1 | **修复 K8s 异常时 autoscaler 重试机制失败** | `git cherry-pick 37f2eb4` |
| 28 | `4f87029de3` | [#60896](https://github.com/ray-project/ray/pull/60896) | Dashboard | feature | P2 | Dashboard 支持 Grafana 日志链接 | `git cherry-pick 4f87029de3` |
| 29 | `dd10a86e34` | [#60790](https://github.com/ray-project/ray/pull/60790) | Data | bugfix | P2 | 修复 OneHotEncoder `max_categories` 使用全局 top-k 而非分区级别 | `git cherry-pick dd10a86` |
| 30 | `9192aefb2d` | [#58910](https://github.com/ray-project/ray/pull/58910) | Data | feature | P3 | 新增 Turbopuffer Datasink | `git cherry-pick 9192aef` |
| 31 | `0eecdde17e` | [#60772](https://github.com/ray-project/ray/pull/60772) | Dashboard | feature | P2 | Dashboard 添加 Logical Memory Usage 面板 | `git cherry-pick 0eecdde17e` |
| 32 | `27698a6c16` | [#59290](https://github.com/ray-project/ray/pull/59290) | Data | feature | P2 | 新增单调递增 ID 生成器 | `git cherry-pick 27698a6` |
| 33 | `47256b60ff` | [#60669](https://github.com/ray-project/ray/pull/60669) | Core | bugfix | P0 | **修复 owner 节点死亡时未销毁所有 owned actors 的问题** | `git cherry-pick 47256b6` |
| 34 | `b0e5ba97e4` | [#60826](https://github.com/ray-project/ray/pull/60826) | Core | bugfix | P1 | **减小 dashboard event aggregator 缓冲区避免 OOM** | `git cherry-pick b0e5ba97e4` |
| 35 | `c08329b60e` | [#60642](https://github.com/ray-project/ray/pull/60642) | Core+Data | refactor | P3 | 将 `env_float/env_integer/env_bool` 移到 `ray._common` | `git cherry-pick c08329b` |
| 36 | `b8bb03b096` | [#60774](https://github.com/ray-project/ray/pull/60774) | Data | feature | P1 | 资源管理器调度决策中加入逻辑内存 | `git cherry-pick b8bb03b` |
| 37 | `6f0458ba42` | [#60575](https://github.com/ray-project/ray/pull/60575) | Data | refactor | P3 | 移除遗留的 BlockList 类 | `git cherry-pick 6f0458b` |
| 38 | `28f42bbf42` | [#60530](https://github.com/ray-project/ray/pull/60530) | Data | refactor | P3 | 从逻辑算子中移除 `output_dependencies` | `git cherry-pick 28f42bb` |
| 39 | `ef98a8903e` | [#60920](https://github.com/ray-project/ray/pull/60920) | Core | optimization | P3 | `MarkFootprintAsIdle()` 对已空闲节点变为 no-op | `git cherry-pick ef98a89` |
| 40 | `7b945c7b32` | [#60778](https://github.com/ray-project/ray/pull/60778) | Core | feature | P2 | 定期重新加载 Ray service account token | `git cherry-pick 7b945c7b32` |
| 41 | `cab739f5d9` | [#60709](https://github.com/ray-project/ray/pull/60709) | Data | bugfix | P3 | 使用 `local://` 路径配合零资源 head 节点时添加描述性错误 | `git cherry-pick cab739f` |
| 42 | `12e3e50975` | [#60795](https://github.com/ray-project/ray/pull/60795) | Data | bugfix | P3 | 修复字符串拼接中的 bare raise 为 TypeError | `git cherry-pick 12e3e50` |
| 43 | `6ab7d3a327` | [#60602](https://github.com/ray-project/ray/pull/60602) | Core | feature | P3 | RDT：复用 nixl agent | `git cherry-pick 6ab7d3a` |
| 44 | `ee7c5d990b` | [#60690](https://github.com/ray-project/ray/pull/60690) | Data | optimization | P3 | 去重时 Schema 不匹配警告更简洁 | `git cherry-pick ee7c5d9` |
| 45 | `9cbcdb4689` | [#60997](https://github.com/ray-project/ray/pull/60997) | Data | bugfix | P1 | 修复 `min_scheduling_resources` 默认回退到 `incremental_resource_usage` | `git cherry-pick 9cbcdb4` |
| 46 | `a5f52841b7` | [#60995](https://github.com/ray-project/ray/pull/60995) | Core | feature | P2 | 使 `ray.put()` 支持泛型类型标注 | `git cherry-pick a5f5284` |
| 47 | `4d2eadd24c` | [#60909](https://github.com/ray-project/ray/pull/60909) | Data | feature | P2 | `read_kafka` 支持 datetime 偏移量 | `git cherry-pick 4d2eadd` |
| 48 | `e0c6da400a` | [#60951](https://github.com/ray-project/ray/pull/60951) | Data | feature | P2 | 添加 checkpoint 数据加载方法到 `LoadCheckpointCallback` | `git cherry-pick e0c6da4` |
| 49 | `4fae2c8ec4` | [#60521](https://github.com/ray-project/ray/pull/60521) | Core | bugfix | P2 | 非生成器函数上将 StopIteration 转为 RuntimeError | `git cherry-pick 4fae2c8` |
| 50 | `7d3c719849` | [#61040](https://github.com/ray-project/ray/pull/61040) | Data | revert | skip | Revert [#60631](https://github.com/ray-project/ray/pull/60631) (禁用 UnionOperator 节流) | - |
| 51 | `89148a4554` | [#60598](https://github.com/ray-project/ray/pull/60598) | Data | optimization | P1 | **消除生成器以避免中间状态被 pin 住**，减少内存占用 | `git cherry-pick 89148a4` |
| 52 | `11eacfd5d5` | [#60682](https://github.com/ray-project/ray/pull/60682) | Core | bugfix | P2 | 修复 Ray Actor async 方法的类型标注 | `git cherry-pick 11eacfd` |
| 53 | `906ce37585` | [#61003](https://github.com/ray-project/ray/pull/61003) | Core | bugfix | P1 | **修复 aggregator agent 指数退避整数溢出** | `git cherry-pick 906ce37585` |
| 54 | `bbeb7adbbc` | [#59604](https://github.com/ray-project/ray/pull/59604) | Core | refactor | P2 | 引入 Process 接口抽象 | `git cherry-pick bbeb7ad` |
| 55 | `38a87660e5` | [#61029](https://github.com/ray-project/ray/pull/61029) | Core | bugfix | P1 | **ray.init() 时重试节点发现**，改善启动鲁棒性 | `git cherry-pick 38a8766` |
| 56 | `5b9f0693b7` | [#61036](https://github.com/ray-project/ray/pull/61036) | Data | bugfix | P1 | 任务结束时重置 DataContext，防止状态泄漏 | `git cherry-pick 5b9f069` |
| 57 | `befc7e5c8e` | [#61060](https://github.com/ray-project/ray/pull/61060) | Core | bugfix | P2 | 设置 worker 进程前检查 process 非空 | `git cherry-pick befc7e5` |
| 58 | `c3f787b5d8` | [#60859](https://github.com/ray-project/ray/pull/60859) | Data | feature | P2 | 支持将 tensor 写入 TFRecords | `git cherry-pick c3f787b` |
| 59 | `feca47613b` | [#61064](https://github.com/ray-project/ray/pull/61064) | Data | revert | skip | Revert task state 获取变更 | - |
| 60 | `871bc89176` | [#61020](https://github.com/ray-project/ray/pull/61020) | Data | refactor | P3 | LogicalOperator 名称默认使用类名 | `git cherry-pick 871bc89` |
| 61 | `e00c9a4a14` | [#61007](https://github.com/ray-project/ray/pull/61007) | Data | feature | P2 | `ResourceUtilizationGauge` 添加逻辑内存指标 | `git cherry-pick e00c9a4` |
| 62 | `094e0a1b24` | [#61010](https://github.com/ray-project/ray/pull/61010) | Data | feature | P3 | 添加 `get_max_task_capacity` 工具函数 | `git cherry-pick 094e0a1` |
| 63 | `f5155f28bb` | [#60980](https://github.com/ray-project/ray/pull/60980) | Core | bugfix | P3 | 改进 dashboard event publisher 错误信息 | `git cherry-pick f5155f28bb` |
| 64 | `04a10c65d7` | [#61026](https://github.com/ray-project/ray/pull/61026) | Data | test | skip | 调整聚合和消费测试大小 | - |
| 65 | `2a4a5c47ce` | [#61044](https://github.com/ray-project/ray/pull/61044) | Data | refactor | P3 | 移除 `locality_with_output` | `git cherry-pick 2a4a5c4` |
| 66 | `4e34398229` | [#60000](https://github.com/ray-project/ray/pull/60000) | Core | optimization | P1 | **禁用 memory_full_info，用 memory_info 近似 USS**，减少系统调用开销 | `git cherry-pick 4e34398229` |
| 67 | `1bd59fe409` | [#58364](https://github.com/ray-project/ray/pull/58364) | Data | bugfix | P1 | **修复 `_align_struct_fields` 在标量字段不对齐时失败** | `git cherry-pick 1bd59fe` |
| 68 | `ef0d3fdce0` | [#61062](https://github.com/ray-project/ray/pull/61062) | Data | feature | P3 | 升级 pyiceberg 到 0.11.0 | `git cherry-pick ef0d3fd` |
| 69 | `425e85eb70` | [#61031](https://github.com/ray-project/ray/pull/61031) | Data | optimization | P3 | `_ActorPool` 类添加 actor 名称 | `git cherry-pick 425e85e` |
| 70 | `05c23a3b6e` | [#60996](https://github.com/ray-project/ray/pull/60996) | Data | feature | P2 | 引入 `ExecutionCache` 实现流式缓存 | `git cherry-pick 05c23a3` |
| 71 | `2ff2b961d8` | [#61146](https://github.com/ray-project/ray/pull/61146) | Data | test | skip | 增加 `with_column` 超时时间 | - |
| 72 | `d39938f275` | [#59368](https://github.com/ray-project/ray/pull/59368) | Core | refactor | P3 | 资源隔离 [3/n]：内存监控器的 OS 特定编译 | `git cherry-pick d39938f` |
| 73 | `66b2c8b415` | [#61130](https://github.com/ray-project/ray/pull/61130) | Data | feature | P2 | 启用 GPU 阶段自动扩缩容 | `git cherry-pick 66b2c8b` |
| 74 | `5606ab7a55` | [#61151](https://github.com/ray-project/ray/pull/61151) | Data | docs | skip | 添加 `locality_with_output` 替代方案说明 | - |
| 75 | `88ac9262bb` | [#60761](https://github.com/ray-project/ray/pull/60761) | Core | feature | P2 | 恢复 per-node 临时目录支持（重新提交 #57735） | `git cherry-pick 88ac9262bb` |
| 76 | `265642fcf1` | [#61140](https://github.com/ray-project/ray/pull/61140) | Data | refactor | P3 | 移除 `locality_with_output` 残余代码 | `git cherry-pick 265642f` |
| 77 | `bfd24f009f` | [#59365](https://github.com/ray-project/ray/pull/59365) | Core | refactor | P3 | 资源隔离 [4/n]：killing policy 的 OS 特定编译 | `git cherry-pick bfd24f0` |
| 78 | `aa00b3dac1` | [#61213](https://github.com/ray-project/ray/pull/61213) | Data | refactor | P2 | 整合 Schema 推断逻辑 | `git cherry-pick aa00b3d` |
| 79 | `8efa629622` | [#61147](https://github.com/ray-project/ray/pull/61147) | Core | build | P2 | 防止 protobuf 符号 `_upb_Arena_SlowMalloc` 泄漏 | `git cherry-pick 8efa629622` |
| 80 | `e12b07eff5` | [#61033](https://github.com/ray-project/ray/pull/61033) | Core | bugfix | P2 | 修复 dashboard list_jobs API 中 dataclass.asdict 空值处理 | `git cherry-pick e12b07eff5` |
| 81 | `c054e6d5dc` | [#61221](https://github.com/ray-project/ray/pull/61221) | Data | test | skip | 算子融合测试改为 medium | - |
| 82 | `1d6bbf353b` | [#61185](https://github.com/ray-project/ray/pull/61185) | Core | bugfix | P1 | **修复 dashboard node_head API 死节点缓存**（master 已有内部版本 c162f7a870） | `git cherry-pick 1d6bbf353b` |
| 83 | `57f5140eea` | [#61165](https://github.com/ray-project/ray/pull/61165) | Data | feature | P3 | TurbopufferDatasink 支持 region 或 base_url | `git cherry-pick 57f5140` |
| 84 | `a63be4c7da` | [#61097](https://github.com/ray-project/ray/pull/61097) | Core | feature | P3 | 资源隔离 [5/n]：添加额外的 cgroup 约束 | `git cherry-pick a63be4c` |
| 85 | `35b297fd4b` | [#60295](https://github.com/ray-project/ray/pull/60295) | Data | feature | P2 | `StreamingRepartition` 支持 `strict=False` 模式 | `git cherry-pick 35b297f` |
| 86 | `8915d371c5` | [#61210](https://github.com/ray-project/ray/pull/61210) | Core | refactor | P3 | 资源隔离 [6/n]：更新 killing policy 接口支持多 worker 策略 | `git cherry-pick 8915d37` |
| 87 | `ce3facf150` | [#60406](https://github.com/ray-project/ray/pull/60406) | Data | bugfix | P2 | 限制 pandas<3 并使 SettingWithCopyWarning 兼容 pandas 3 | `git cherry-pick ce3facf` |
| 88 | `34699a557f` | [#60274](https://github.com/ray-project/ray/pull/60274) | Data | optimization | P2 | `train_test_split` 避免冗余读取 | `git cherry-pick 34699a5` |
| 89 | `c11c103fa4` | [#61208](https://github.com/ray-project/ray/pull/61208) | Data | bugfix | P1 | **修复多输入算子 object store 内存归属的双重计数** | `git cherry-pick c11c103` |
| 90 | `67503001c7` | [#60480](https://github.com/ray-project/ray/pull/60480) | Data | refactor | P2 | 简化执行回调生命周期 (Diff #1) | `git cherry-pick 6750300` |
| 91 | `58fcebd6d5` | [#61282](https://github.com/ray-project/ray/pull/61282) | Data | bugfix | P2 | 修复 `DatabricksUCDatasource` schema 属性被 schema() 方法遮蔽 | `git cherry-pick 58fcebd` |
| 92 | `32945ff30a` | [#61107](https://github.com/ray-project/ray/pull/61107) | Data | refactor | P3 | 将 `output_dependencies` 职责移到 `PhysicalOperator` | `git cherry-pick 32945ff` |
| 93 | `9a0ae1b908` | [#60753](https://github.com/ray-project/ray/pull/60753) | Core | feature | P2 | 添加 Nvidia B300 GPU 支持 | `git cherry-pick 9a0ae1b908` |
| 94 | `09c7c7626a` | [#61028](https://github.com/ray-project/ray/pull/61028) | Data | refactor | P3 | 重命名 encoder preprocessor 私有字段 | `git cherry-pick 09c7c76` |
| 95 | `3956d0d7db` | [#61150](https://github.com/ray-project/ray/pull/61150) | Data | optimization | P3 | 执行开始时记录 DataContext 配置日志 | `git cherry-pick 3956d0d` |
| 96 | `a35fe52062` | [#61081](https://github.com/ray-project/ray/pull/61081) | Core | feature | P3 | RDT：支持非 torch tensor 对象的 RDT 传输 | `git cherry-pick a35fe52` |
| 97 | `817204adbe` | [#61126](https://github.com/ray-project/ray/pull/61126) | Data | feature | P2 | `read_*` 函数支持 `pathlib.Path` | `git cherry-pick 817204a` |
| 98 | `f8d5149ba8` | [#61280](https://github.com/ray-project/ray/pull/61280) | Core | refactor | P3 | RDT：拆分序列化并使其线程安全 | `git cherry-pick f8d5149` |
| 99 | `b24f6e6e17` | [#61297](https://github.com/ray-project/ray/pull/61297) | Core | refactor | P3 | 资源隔离 [7/n]：将公共 killing policy 辅助函数移到 util | `git cherry-pick b24f6e6` |
| 100 | `ba137af6dd` | [#61294](https://github.com/ray-project/ray/pull/61294) | Core | build | skip | 使用 bazel param file 避免 Windows 命令行长度限制 | - |
| 101 | `5ac4604cdf` | [#61293](https://github.com/ray-project/ray/pull/61293) | Data | refactor | P2 | DataContext 持有执行回调类而非实例 | `git cherry-pick 5ac4604` |
| 102 | `1f45462bd8` | [#61308](https://github.com/ray-project/ray/pull/61308) | Data | refactor | P3 | LogicalOperator 改为 ABC 并添加抽象 `num_outputs` | `git cherry-pick 1f45462` |
| 103 | `0906625f0d` | [#61192](https://github.com/ray-project/ray/pull/61192) | Data | feature | P2 | 添加任务调度时间和输出反压追踪指标 | `git cherry-pick 0906625` |
| 104 | `1b85bf7530` | [#61288](https://github.com/ray-project/ray/pull/61288) | Data | optimization | P1 | **防止聚合任务调度到 head 节点**，减少 head 负载 | `git cherry-pick 1b85bf7` |
| 105 | `9f06d54c12` | [#61246](https://github.com/ray-project/ray/pull/61246) | Core | bugfix | P2 | 修复 `WorkerPool::WarnAboutSize()` 中的双重计数 | `git cherry-pick 9f06d54` |
| 106 | `1e6e66d070` | [#61273](https://github.com/ray-project/ray/pull/61273) | Data | feature | P3 | 添加基于速率计算分配量的工具函数 | `git cherry-pick 1e6e66d` |
| 107 | `a6faf23e6f` | [#61094](https://github.com/ray-project/ray/pull/61094) | Core | feature | P3 | RDT：设置 RDT ref 的接收缓冲区 | `git cherry-pick a6faf23` |
| 108 | `3b1362841f` | [#61298](https://github.com/ray-project/ray/pull/61298) | Core | build | P2 | 收紧 ray export 符号白名单防止非 ray 符号泄漏 | `git cherry-pick 3b1362841f` |
| 109 | `a29155c868` | [#59633](https://github.com/ray-project/ray/pull/59633) | Data | feature | P2 | `read_datasource()` 支持 `compute` 参数和 `ActorPoolStrategy` | `git cherry-pick a29155c` |
| 110 | `371a3613ac` | [#61004](https://github.com/ray-project/ray/pull/61004) | Core | optimization | P2 | 调度速率限制时输出告警，帮助排查任务启动慢 | `git cherry-pick 371a361` |
| 111 | `c73d04e1a7` | [#61281](https://github.com/ray-project/ray/pull/61281) | Core | bugfix | P1 | **同步等待 metrics exporter 初始化，避免 getenv/setenv 竞态** | `git cherry-pick c73d04e` |
| 112 | `29041ecd41` | [#61382](https://github.com/ray-project/ray/pull/61382) | Core | bugfix | P2 | autoscaler 允许 ALLOCATION_TIMEOUT → TERMINATED 状态转换 | `git cherry-pick 29041ec` |
| 113 | `f654eee832` | [#61353](https://github.com/ray-project/ray/pull/61353) | Core | optimization | P1 | **优化 worker listener 线程**，减少调度延迟 | `git cherry-pick f654eee` |
| 114 | `85dc75ce42` | [#61284](https://github.com/ray-project/ray/pull/61284) | Data | refactor | P2 | Kafka 库迁移到 confluent-kafka | `git cherry-pick 85dc75c` |
| 115 | `544a40fb3a` | [#61449](https://github.com/ray-project/ray/pull/61449) | Core | revert | P2 | Revert gRPC 升级到 1.58.0（与 #61499 配合） | `git cherry-pick 544a40fb3a` |
| 116 | `80cc4bda39` | [#61380](https://github.com/ray-project/ray/pull/61380) | Data | bugfix | P3 | 修复不清晰的元数据警告和错误的算子名称日志 | `git cherry-pick 80cc4bd` |
| 117 | `50ca506ab8` | [#60504](https://github.com/ray-project/ray/pull/60504) | Dashboard | feature | P2 | Dashboard 支持 autoscaler v2 集群级节点指标 | `git cherry-pick 50ca506ab8` |
| 118 | `e8f0b5031a` | [#61437](https://github.com/ray-project/ray/pull/61437) | Data | refactor | P3 | Kafka 数据源用 `consume()` 替代 `poll()` | `git cherry-pick e8f0b50` |
| 119 | `6ddbbdd00f` | [#59879](https://github.com/ray-project/ray/pull/59879) | Data | feature | P2 | 表达式操作添加 map namespace 支持 | `git cherry-pick 6ddbbdd` |
| 120 | `4356f0fe0a` | [#61364](https://github.com/ray-project/ray/pull/61364) | Data | refactor | P3 | 一对一逻辑算子转为 frozen dataclass | `git cherry-pick 4356f0f` |
| 121 | `d4e007485f` | [#61376](https://github.com/ray-project/ray/pull/61376) | Data | bugfix | P1 | **修复 `read_parquet` 对版本化对象存储 URI 的文件扩展名过滤** | `git cherry-pick d4e0074` |
| 122 | `62da761393` | [#61478](https://github.com/ray-project/ray/pull/61478) | Core | bugfix | P2 | 修复 `TaskLifecycleEvent.node_id` 填充了发送节点而非执行节点 | `git cherry-pick 62da761` |
| 123 | `b9cbe1f9d3` | [#61341](https://github.com/ray-project/ray/pull/61341) | Data | refactor | P3 | 所有 Preprocessor 实现 `SerializablePreprocessorBase` | `git cherry-pick b9cbe1f` |
| 124 | `941d4e0753` | [#61436](https://github.com/ray-project/ray/pull/61436) | Data | refactor | P3 | 添加 `ClusterUtil` 数据类 | `git cherry-pick 941d4e0` |
| 125 | `c190c7fe2e` | [#61082](https://github.com/ray-project/ray/pull/61082) | Core | bugfix | P1 | **按并发组而非全局顺序排列有序 actor 任务**，修复并发组间阻塞 | `git cherry-pick c190c7f` |
| 126 | `5d3a8a3a58` | [#61476](https://github.com/ray-project/ray/pull/61476) | Data | bugfix | P2 | 移除 Kafka 默认 task 超时并钳制 `end_offset` 到 watermark | `git cherry-pick 5d3a8a3` |
| 127 | `476bfa8ff6` | [#60449](https://github.com/ray-project/ray/pull/60449) | Core | feature | P2 | 在 one-event 框架中支持 placement group 事件 | `git cherry-pick 476bfa8` |
| 128 | `8212929d8b` | [#61326](https://github.com/ray-project/ray/pull/61326) | Core | refactor | skip | RDT 代码中引用名称更新 | - |
| 129 | `94944d79a6` | [#61232](https://github.com/ray-project/ray/pull/61232) | Core | optimization | P2 | state manager 获取节点信息时消除 Python GCS client 依赖 | `git cherry-pick 94944d7` |
| 130 | `598ca8d39b` | [#61428](https://github.com/ray-project/ray/pull/61428) | Data | optimization | P3 | DataContext 以 JSON 格式打印 | `git cherry-pick 598ca8d` |
| 131 | `e9dd5e3d63` | [#61577](https://github.com/ray-project/ray/pull/61577) | Data | bugfix | P3 | 放宽 `concurrency_solver.py` 断言 | `git cherry-pick e9dd5e3` |
| 132 | `b96eb4578a` | [#61578](https://github.com/ray-project/ray/pull/61578) | Data | refactor | P3 | `FakeAutoscalingCoordinator` 空请求时返回指定资源 | `git cherry-pick b96eb45` |
| 133 | `17fbe52557` | [#61469](https://github.com/ray-project/ray/pull/61469) | Core | feature | P2 | Actor 定义 OneEvent 中添加 labels 作为 ref ID，关联 actor 和 Data 算子 | `git cherry-pick 17fbe52` |
| 134 | `4d7f46c84f` | [#61344](https://github.com/ray-project/ray/pull/61344) | Core+Data | feature | P2 | ActorPoolMapOperator 任务中添加 data operator ID | `git cherry-pick 4d7f46c` |
| 135 | `39e2484d44` | [#61580](https://github.com/ray-project/ray/pull/61580) | Data | bugfix | P3 | `TimeWindowAverageCalculator` 浮点误差缓解 | `git cherry-pick 39e2484` |
| 136 | `55227f948c` | [#61581](https://github.com/ray-project/ray/pull/61581) | Data | test | skip | 修复 streaming_train_test_split doctest | - |
| 137 | `9577b673ba` | [#61540](https://github.com/ray-project/ray/pull/61540) | Data | bugfix | P2 | Parquet 读取跳过 `_SUCCESS` 文件 | `git cherry-pick 9577b67` |
| 138 | `15a4734540` | [#61572](https://github.com/ray-project/ray/pull/61572) | Data | refactor | P3 | 移除 `_validate_dag` 死代码 | `git cherry-pick 15a4734` |
| 139 | `7561c2a60e` | [#61579](https://github.com/ray-project/ray/pull/61579) | Core | bugfix | P2 | RDT：上游任务出错时避免 RDT 阻塞 | `git cherry-pick 7561c2a` |
| 140 | `95b4042422` | [#61518](https://github.com/ray-project/ray/pull/61518) | Core | bugfix | P2 | 修复 GCS pubsub 中 `publisher_id` 类型不匹配 | `git cherry-pick 95b4042` |
| 141 | `d521f94d44` | [#56284](https://github.com/ray-project/ray/pull/56284) | Data | feature | P2 | 支持 Arrow 原生定长 tensor 类型 | `git cherry-pick d521f94` |
| 142 | `75ea85b7bb` | [#61148](https://github.com/ray-project/ray/pull/61148) | Core | feature | P2 | 添加基于二次直方图的 Percentile 指标类型 | `git cherry-pick 75ea85b` |
| 143 | `66949f9184` | [#61605](https://github.com/ray-project/ray/pull/61605) | Data | bugfix | skip | 防止算子独占共享 object store 预算（后被 revert） | - |
| 144 | `56599092ea` | [#61315](https://github.com/ray-project/ray/pull/61315) | Data | optimization | P1 | **优化 concat tables 的快速路径** | `git cherry-pick 5659909` |
| 145 | `6cf5425772` | [#61404](https://github.com/ray-project/ray/pull/61404) | Data | test | skip | 添加 iceberg upsert benchmark 测试 | - |
| 146 | `d5391f90f0` | [#61220](https://github.com/ray-project/ray/pull/61220) | Data | feature | P2 | DataSourceV2 文件列举基础设施 [1/n] | `git cherry-pick d5391f9` |
| 147 | `b69f225e2e` | [#61543](https://github.com/ray-project/ray/pull/61543) | Data | bugfix | P2 | 滚动利用率均值钳制到零 | `git cherry-pick b69f225` |
| 148 | `be18bcd559` | [#61323](https://github.com/ray-project/ray/pull/61323) | Core | feature | P3 | 资源隔离 [8/n]：添加基于时间的 worker killing policy | `git cherry-pick be18bcd` |
| 149 | `2208e78e5b` | [#61473](https://github.com/ray-project/ray/pull/61473) | Core | bugfix | P2 | 允许 `worker_process_setup_hook` 在 re-entry 时匹配 | `git cherry-pick 2208e78` |
| 150 | `8af3b3ebeb` | [#60860](https://github.com/ray-project/ray/pull/60860) | Core | bugfix | P1 | **autoscaler 在 head 重启时从 WrongClusterID 恢复** | `git cherry-pick 8af3b3e` |
| 151 | `18b6eb7401` | [#61190](https://github.com/ray-project/ray/pull/61190) | Data | refactor | P3 | 清理 `SplitCoordinator` | `git cherry-pick 18b6eb7` |
| 152 | `1a16658a24` | [#61559](https://github.com/ray-project/ray/pull/61559) | Core | bugfix | P2 | 修复 `TASK_PROFILE_EVENT` 多阶段聚合 | `git cherry-pick 1a16658` |
| 153 | `7cf8f37cb2` | [#61199](https://github.com/ray-project/ray/pull/61199) | Data | refactor | P3 | 提取 Unity Catalog 凭证解析为独立函数 | `git cherry-pick 7cf8f37` |
| 154 | `5d6c40aba8` | [#61658](https://github.com/ray-project/ray/pull/61658) | Data | refactor | P3 | 重命名 `concurrency_solver.py` 为 `throughput_solver.py` | `git cherry-pick 5d6c40a` |
| 155 | `aabe458342` | [#61617](https://github.com/ray-project/ray/pull/61617) | Data | feature | P2 | RD PyArrow Compute Func 转 PyArrow Expr 实现谓词下推 | `git cherry-pick aabe458` |
| 156 | `a20d303c51` | [#61499](https://github.com/ray-project/ray/pull/61499) | Core | build | P2 | **升级 gRPC 到 v1.58.0** | `git cherry-pick a20d303` |
| 157 | `1972790222` | [#61539](https://github.com/ray-project/ray/pull/61539) | Core | bugfix | P3 | 修复 ObjectManager::PushObjectInternal 日志字段 | `git cherry-pick 1972790` |
| 158 | `befa5b8863` | [#61418](https://github.com/ray-project/ray/pull/61418) | Data | optimization | P2 | 改进 table aggregate 函数 | `git cherry-pick befa5b8` |
| 159 | `63bc2648a2` | [#61329](https://github.com/ray-project/ray/pull/61329) | Data | feature | P2 | 添加 `cudf` 作为 batch_format | `git cherry-pick 63bc264` |
| 160 | `d334cd6536` | [#61300](https://github.com/ray-project/ray/pull/61300) | Core | feature | P3 | 添加 TPU 多主机 slice 就绪检测工具 | `git cherry-pick d334cd6536` |
| 161 | `4b4ab0125d` | [#61618](https://github.com/ray-project/ray/pull/61618) | Core | bugfix | P2 | 确保 `Node._node_labels` 在 `connect_only` 时也初始化 | `git cherry-pick 4b4ab01` |
| 162 | `51e923d091` | [#61363](https://github.com/ray-project/ray/pull/61363) | Core | refactor | P3 | serve import 从 `_private` 迁移到 `_common` | `git cherry-pick 51e923d` |
| 163 | `1a6c6f0529` | [#60659](https://github.com/ray-project/ray/pull/60659) | Core | feature | P2 | 在 `TaskInfoEntry` 和 `ActorTableData` 中暴露 `fallback_strategy` | `git cherry-pick 1a6c6f0` |
| 164 | `02d61afb3c` | [#61059](https://github.com/ray-project/ray/pull/61059) | Data | refactor | P3 | `RefBundle` 等类改为 properly frozen | `git cherry-pick 02d61af` |
| 165 | `6d34c30fed` | [#61670](https://github.com/ray-project/ray/pull/61670) | Data | optimization | P2 | `get_parquet_dataset` 可配置扫描 fragment 数量 | `git cherry-pick 6d34c30` |
| 166 | `f1821c1bb2` | [#61385](https://github.com/ray-project/ray/pull/61385) | Data | optimization | P2 | 精简 `DefaultActorPoolAutoscaler` | `git cherry-pick f1821c1` |
| 167 | `91eb738eb2` | [#61729](https://github.com/ray-project/ray/pull/61729) | Data | revert | skip | Revert [#61605](https://github.com/ray-project/ray/pull/61605) | - |
| 168 | `1a98a597ee` | [#61654](https://github.com/ray-project/ray/pull/61654) | Core | feature | P2 | IPPR [1/N]：为 GcsAutoscalerStateManager 添加 ResizeRayletResourceInstances | `git cherry-pick 1a98a59` |
| 169 | `4902739acd` | [#60330](https://github.com/ray-project/ray/pull/60330) | Core | optimization | P1 | **OOM Killer 优先杀死占用大内存的 Worker** | `git cherry-pick 4902739` |
| 170 | `b5b334fef1` | [#61507](https://github.com/ray-project/ray/pull/61507) | Data | bugfix | P2 | 修复 shuffle 空 block 场景下链式左连接的 ColumnNotFound 错误 | `git cherry-pick b5b334f` |
| 171 | `b86462c16e` | [#61774](https://github.com/ray-project/ray/pull/61774) | Data | bugfix | P1 | **修复 ref_bundle + input_files 双重计数** | `git cherry-pick b86462c` |
| 172 | `f5d37e3e84` | [#61732](https://github.com/ray-project/ray/pull/61732) | Core | optimization | P3 | 只读 provider 时抑制 autoscaler 操作日志 | `git cherry-pick f5d37e3` |
| 173 | `d269f5b828` | [#61666](https://github.com/ray-project/ray/pull/61666) | Core | feature | P2 | IPPR [2/N]：GCS Cython 客户端添加 `resize_raylet_resource_instances` | `git cherry-pick d269f5b` |
| 174 | `770dca31ba` | [#61371](https://github.com/ray-project/ray/pull/61371) | Data | feature | P2 | 支持 GPU shuffle | `git cherry-pick 770dca3` |
| 175 | `281c2cdf29` | [#60693](https://github.com/ray-project/ray/pull/60693) | Core | refactor | P3 | 重构 GLOO collective groups 重复 rendezvous 逻辑 | `git cherry-pick 281c2cdf29` |
| 176 | `2b63ed0d11` | [#61807](https://github.com/ray-project/ray/pull/61807) | Core | test | skip | 禁用 compiled graph flaky 测试 | - |
| 177 | `217c088357` | [#61815](https://github.com/ray-project/ray/pull/61815) | Data | refactor | P2 | 统一 actor pool 状态与分配逻辑 | `git cherry-pick 217c088` |
| 178 | `9642fc01bb` | [#61697](https://github.com/ray-project/ray/pull/61697) | Data | refactor | P3 | 重构 hanging detector 避免深层嵌套 [1/N] | `git cherry-pick 9642fc0` |
| 179 | `d294b9d328` | [#61481](https://github.com/ray-project/ray/pull/61481) | Data | refactor | P3 | Map 逻辑算子转为 frozen dataclass | `git cherry-pick d294b9d` |
| 180 | `5ca80bb6b0` | [#59662](https://github.com/ray-project/ray/pull/59662) | Data | feature | P2 | 改进随机 API 可重现性 | `git cherry-pick 5ca80bb` |
| 181 | `4ce100c866` | [#61843](https://github.com/ray-project/ray/pull/61843) | Data | refactor | P3 | 添加 `refresh_state` 接口 | `git cherry-pick 4ce100c` |
| 182 | `8b561dde6e` | [#61483](https://github.com/ray-project/ray/pull/61483) | Data | refactor | P3 | 移除过时的 PyArrow 9.0 版本检查 | `git cherry-pick 8b561dd` |
| 183 | `ad47d5725c` | [#61065](https://github.com/ray-project/ray/pull/61065) | Core | optimization | P1 | **缓存 `find_gcs_addresses`，避免 head 节点重复扫描进程列表** | `git cherry-pick ad47d57` |
| 184 | `580ce5ed18` | [#61447](https://github.com/ray-project/ray/pull/61447) | Data | bugfix | P2 | `TaskPoolMapOperator` 获取正确的逻辑资源使用量 | `git cherry-pick 580ce5e` |
| 185 | `0951139b9b` | [#60307](https://github.com/ray-project/ray/pull/60307) | Data | feature | P2 | 新增 Kafka Datasink | `git cherry-pick 0951139` |
| 186 | `2bd8fed85e` | [#60828](https://github.com/ray-project/ray/pull/60828) | Data | bugfix | P0 | **修复 `OpBufferQueue` 竞态条件**，引入线程安全队列 | `git cherry-pick 2bd8fed` |
| 187 | `9971f87c0f` | [#61837](https://github.com/ray-project/ray/pull/61837) | Core | bugfix | P1 | **版本不匹配时清理已启动的节点进程**，避免孤儿进程 | `git cherry-pick 9971f87` |
| 188 | `acdef1804e` | [#61845](https://github.com/ray-project/ray/pull/61845) | Core | build | skip | 修复 StatusOr 中 -Wmaybe-uninitialized 编译告警 | - |
| 189 | `cd3b3f7dc3` | [#61853](https://github.com/ray-project/ray/pull/61853) | Data | bugfix | P3 | 修复缺失的 PYARROW_VERSION 和 DataContext import | `git cherry-pick cd3b3f7` |
| 190 | `5709cadaf1` | [#61143](https://github.com/ray-project/ray/pull/61143) | Data | bugfix | P3 | Windows 下 Ray Data 日志编码默认 UTF-8 | `git cherry-pick 5709cad` |
| 191 | `45ea2a708b` | [#61821](https://github.com/ray-project/ray/pull/61821) | Data | feature | P2 | checkpoint 两阶段提交 + 前缀 trie 恢复 | `git cherry-pick 45ea2a7` |
| 192 | `abb30adbd0` | [#61803](https://github.com/ray-project/ray/pull/61803) | Core | feature | P2 | IPPR [3/N]：IPPR spec schema 和 pod resize status 模型 | `git cherry-pick abb30ad` |
| 193 | `cbf113e837` | [#61855](https://github.com/ray-project/ray/pull/61855) | Core | refactor | P3 | 移除 GCS 集中式调度遗留的 `normal_task_resources` 死代码 | `git cherry-pick cbf113e` |
| 194 | `d019584464` | [#61858](https://github.com/ray-project/ray/pull/61858) | Core | bugfix | P2 | 修复 Java Local Mode 多 Actor 类型混淆 | `git cherry-pick d019584464` |
| 195 | `e4286a9881` | [#61916](https://github.com/ray-project/ray/pull/61916) | Data | bugfix | skip | 修复 DefaultResizingPolicy lint 错误 | - |
| 196 | `49293dae41` | [#60857](https://github.com/ray-project/ray/pull/60857) | Core | feature | P2 | 添加 submission job 事件 proto 定义 | `git cherry-pick 49293da` |
| 197 | `d7e5ce6546` | [#61848](https://github.com/ray-project/ray/pull/61848) | Data | refactor | P3 | Actor Pool Map Operator 代码清理 | `git cherry-pick d7e5ce6` |
| 198 | `fd93429ca9` | [#61879](https://github.com/ray-project/ray/pull/61879) | Core | bugfix | skip | 修复 EC2InstanceTerminator 的 SSL 不匹配（release 测试） | - |
| 199 | `16eaed0ec4` | [#61700](https://github.com/ray-project/ray/pull/61700) | Data | bugfix | P0 | **用 `__ray_shutdown__` 替代 `on_exit` hook，修复 UDF 清理竞态** | `git cherry-pick 16eaed0` |
| 200 | `c6006fd969` | [#61926](https://github.com/ray-project/ray/pull/61926) | Data | build | skip | 修复 pyrefly 类型检查错误 | - |
| 201 | `ca430cfd5a` | [#61615](https://github.com/ray-project/ray/pull/61615) | Data | feature | P2 | 新增 DataSourceV2 核心 API（Scanner/Reader/优化器 mixin） | `git cherry-pick ca430cf` |
| 202 | `23e587f22f` | [#61925](https://github.com/ray-project/ray/pull/61925) | Core | bugfix | P0 | **修复 absl Mutex 重入锁导致的 SIGABRT 崩溃** | `git cherry-pick 23e587f` |
| 203 | `b75d20907a` | [#61591](https://github.com/ray-project/ray/pull/61591) | Data | optimization | P1 | **Actor Pool Map 调度开销降低约 57%** | `git cherry-pick b75d209` |
| 204 | `5986bbb54d` | [#61348](https://github.com/ray-project/ray/pull/61348) | Data | refactor | P3 | [移除 ExecutionPlan 1/N] 逻辑计算迁移到 LogicalPlan | `git cherry-pick 5986bbb` |
| 205 | `7eecf606af` | [#61902](https://github.com/ray-project/ray/pull/61902) | Dashboard | bugfix | P2 | JobSubmissionClient 转发 `**kwargs` 到 cluster info resolvers | `git cherry-pick 7eecf606af` |
| 206 | `e59d4db43a` | [#61998](https://github.com/ray-project/ray/pull/61998) | Data | refactor | skip | 类标识符碰撞日志降级为 debug | - |
| 207 | `e22bdee678` | [#60497](https://github.com/ray-project/ray/pull/60497) | Data | feature | P2 | Lance 数据源增强：写入重试、命名空间、driver 端提交 | `git cherry-pick e22bdee` |
| 208 | `aff2c84f4c` | [#61999](https://github.com/ray-project/ray/pull/61999) | Data | refactor | skip | block.py 函数重命名 | - |
| 209 | `fac1f45b97` | [#61990](https://github.com/ray-project/ray/pull/61990) | Data | docs | skip | 更新 exclude_resources 文档 | - |
| 210 | `35c9a08253` | [#61744](https://github.com/ray-project/ray/pull/61744) | Data | test | skip | 添加测试辅助函数和重构 stats 测试 | - |
| 211 | `e23692fc6a` | [#61361](https://github.com/ray-project/ray/pull/61361) | Core | feature | P3 | 资源隔离 [9/n]：基于内存压力（PSI）的内存监控器 | `git cherry-pick e23692f` |
| 212 | `027d17fbdf` | [#59286](https://github.com/ray-project/ray/pull/59286) | Core | optimization | P2 | 改进 `num_returns` 参数错误处理，快速失败 | `git cherry-pick 027d17f` |
| 213 | `7985f5dbe6` | [#61934](https://github.com/ray-project/ray/pull/61934) | Core | bugfix | P1 | **修复 `ActorMethod.options()` 引用循环**，避免 actor 延迟释放 | `git cherry-pick 7985f5d` |
| 214 | `732c259d98` | [#61917](https://github.com/ray-project/ray/pull/61917) | Data | optimization | P1 | **放宽 `DefaultActorAutoscaler` 约束**，不再因 task slot 限制阻止扩缩容 | `git cherry-pick 732c259` |
| 215 | `b47c0e79f6` | [#59656](https://github.com/ray-project/ray/pull/59656) | Data | feature | P2 | 新增 `random()` 和 `uuid()` 表达式 | `git cherry-pick b47c0e7` |
| 216 | `569eb4e61f` | [#61997](https://github.com/ray-project/ray/pull/61997) | Data | feature | P3 | DataSourceV2 文件分区（RoundRobinPartitioner） [3/n] | `git cherry-pick 569eb4e` |
| 217 | `742522b57c` | [#62034](https://github.com/ray-project/ray/pull/62034) | Data | build | skip | pyrefly 修复 lance table_id | - |
| 218 | `e14822a09f` | [#62036](https://github.com/ray-project/ray/pull/62036) | Data | refactor | P3 | 回退 ActorMethod 引用循环变通方案（上游 [#61934](https://github.com/ray-project/ray/pull/61934) 已修复） | `git cherry-pick e14822a` |
| 219 | `ad897f2c78` | [#61989](https://github.com/ray-project/ray/pull/61989) | Data | bugfix | P3 | 修复自动扩缩容日志输出精度和重复问题 | `git cherry-pick ad897f2` |
| 220 | `63fea83649` | [#62029](https://github.com/ray-project/ray/pull/62029) | Data | bugfix | P2 | 修复 `target_max_block_size=None` 时反压过于乐观 | `git cherry-pick 63fea83` |
| 221 | `82c1051f16` | [#61405](https://github.com/ray-project/ray/pull/61405) | Data | refactor | P1 | **重构执行回调为静态初始化**，防止状态泄漏 | `git cherry-pick 82c1051` |
| 222 | `98582028b0` | [#62031](https://github.com/ray-project/ray/pull/62031) | Data | bugfix | P2 | 修复 `average_task_scheduling_time_s` 指标计算膨胀 | `git cherry-pick 9858202` |
| 223 | `746ce86722` | [#62050](https://github.com/ray-project/ray/pull/62050) | Data | refactor | P3 | [移除 ExecutionPlan 2/N] 移除 get_plan_conversion_fns | `git cherry-pick 746ce86` |
| 224 | `7dd6606efd` | [#62055](https://github.com/ray-project/ray/pull/62055) | Data | refactor | P3 | [移除 ExecutionPlan 3/N] 移除旧的基于实例的执行回调 API | `git cherry-pick 7dd6606` |
| 225 | `0adbd5be72` | [#62065](https://github.com/ray-project/ray/pull/62065) | Data | bugfix | P2 | 修复 `ParquetDatasource` PyArrow 版本检查（应为 22.0） | `git cherry-pick 0adbd5b` |
| 226 | `dfc05797b2` | [#62057](https://github.com/ray-project/ray/pull/62057) | Data | bugfix | P2 | 修复 `StreamingSplitDataIterator.schema()` | `git cherry-pick dfc0579` |
| 227 | `028e164bbc` | [#62062](https://github.com/ray-project/ray/pull/62062) | Data | feature | P3 | 支持 rapidsmpf 26.2 的 GPU shuffle | `git cherry-pick 028e164` |
| 228 | `40df4994e3` | [#62070](https://github.com/ray-project/ray/pull/62070) | Core | bugfix | P0 | **修复 RUNNING 任务指标负值**（竞态条件） | `git cherry-pick 40df499` |
| 229 | `59c4372abe` | [#62114](https://github.com/ray-project/ray/pull/62114) | Data | optimization | P1 | **Actor 排名算法 O(N*M) → O(N*log M)**，堆排序 | `git cherry-pick 59c4372` |
| 230 | `1ce6214236` | [#61996](https://github.com/ray-project/ray/pull/61996) | Data | optimization | P1 | **缓存 `_map_task` 公共参数，actor 创建时间减半** | `git cherry-pick 1ce6214` |
| 231 | `a1027621eb` | [#62118](https://github.com/ray-project/ray/pull/62118) | Data | refactor | skip | Hash Shuffle 指标对齐 | - |
| 232 | `d032daf9df` | [#62108](https://github.com/ray-project/ray/pull/62108) | Data | optimization | P1 | **优化大 schema 哈希/比较性能**，减少 DatasetStats 字符串传递 | `git cherry-pick d032daf` |
| 233 | `c02bd31ae3` | [#62056](https://github.com/ray-project/ray/pull/62056) | Data | bugfix | P0 | **🔒 安全修复：Arrow 扩展类型反序列化 RCE 漏洞** (GHSA-mw35-8rx3-xf9r) | `git cherry-pick c02bd31` |
| 234 | `1a92e91af4` | [#61890](https://github.com/ray-project/ray/pull/61890) | Data | optimization | P1 | **反压阈值从 90% 降至 50%**，更早启动反压减少 spilling | `git cherry-pick 1a92e91` |
| 235 | `94ade36ae2` | [#62126](https://github.com/ray-project/ray/pull/62126) | Data | bugfix | skip | autoscaler traceback 日志降级为 debug | - |
| 236 | `0f7a9f1ac1` | [#60104](https://github.com/ray-project/ray/pull/60104) | Core | bugfix | P0 | **修复 pop worker 失败时任务永久卡住**，引入重试上限（默认5次） | `git cherry-pick 0f7a9f1` |
| 237 | `7a61534a40` | [#62071](https://github.com/ray-project/ray/pull/62071) | Core | refactor | skip | Schedule 重命名为 SchedulePlacementGroup | - |
| 238 | `a0b7c79019` | [#61811](https://github.com/ray-project/ray/pull/61811) | Core | bugfix | P2 | 修复 Azure `ray down` 时误删共享 MSI | `git cherry-pick a0b7c79` |
| 239 | `3cce4b50eb` | [#61607](https://github.com/ray-project/ray/pull/61607) | Data | optimization | P0 | **移除 StreamSplitDataIterator 的 Dataset 引用，解决 head 节点 OOM** | `git cherry-pick 3cce4b5` |
| 240 | `e4f0e0e9f8` | [#60868](https://github.com/ray-project/ray/pull/60868) | Core | feature | P2 | Ray Client 模式添加 UV 包管理器支持 | `git cherry-pick e4f0e0e` |
| 241 | `a28492dd93` | [#62112](https://github.com/ray-project/ray/pull/62112) | Core | optimization | P3 | HandleDrainNode 日志打印 gRPC peer 地址 | `git cherry-pick a28492d` |
| 242 | `e0dee73025` | [#61351](https://github.com/ray-project/ray/pull/61351) | Data | refactor | P3 | [移除 ExecutionPlan 4/N] 简化 legacy_compat | `git cherry-pick e0dee73` |
| 243 | `8b07fc494f` | [#62149](https://github.com/ray-project/ray/pull/62149) | Data | bugfix | P2 | 修复 wide_schema_pipeline_tensors cloudpickle 反序列化 | `git cherry-pick 8b07fc494f` |
| 244 | `945f423040` | [#62086](https://github.com/ray-project/ray/pull/62086) | Core | bugfix | P0 | **修复 pg.ready() 死锁**：取消并发上限 | `git cherry-pick 945f423` |
| 245 | `a5ce5dd4c7` | [#62226](https://github.com/ray-project/ray/pull/62226) | Core | optimization | P3 | HandleUnregisterNode 日志打印 gRPC peer 地址 | `git cherry-pick a5ce5dd` |
| 246 | `8fd4fe082a` | [#61716](https://github.com/ray-project/ray/pull/61716) | Dashboard | feature | P2 | Dashboard 添加 Ray Data Queued Blocks 指标 | `git cherry-pick 8fd4fe082a` |
| 247 | `31cda7c00f` | [#61638](https://github.com/ray-project/ray/pull/61638) | Core | optimization | P1 | **缓存 ActorHandle.__hash__**，修复 __eq__ 正确性 | `git cherry-pick 31cda7c` |
| 248 | `e0bc348992` | [#61421](https://github.com/ray-project/ray/pull/61421) | Core | bugfix | P0 | **修复布尔环境变量解析 bug**（`bool("0")` 为 True） | `git cherry-pick e0bc348` |
| 249 | `4d5b3d0c52` | [#62117](https://github.com/ray-project/ray/pull/62117) | Data | bugfix | P0 | **资源管理器考虑外部消费者 Object Store 使用量** | `git cherry-pick 4d5b3d0` |
| 250 | `6dadfdf118` | [#61349](https://github.com/ray-project/ray/pull/61349) | Data | refactor | P3 | [移除 ExecutionPlan 5/N] 将 schema/meta_count 迁移到 Dataset | `git cherry-pick 6dadfdf` |
| 251 | `b165ee79bc` | [#62210](https://github.com/ray-project/ray/pull/62210) | Data | refactor | skip | 自动扩缩容协调器 traceback 日志可配置 | - |
| 252 | `0ed5173e82` | [#62242](https://github.com/ray-project/ray/pull/62242) | Data | bugfix | P0 | **修复 Parquet batch_size 超出 C++ 32 位 int 范围** | `git cherry-pick 0ed5173` |
| 253 | `4a79cd954e` | [#62153](https://github.com/ray-project/ray/pull/62153) | Core | refactor | skip | 替换已弃用的 threading API | - |
| 254 | `b9bc54f81f` | [#62251](https://github.com/ray-project/ray/pull/62251) | Core | bugfix | P2 | 修复 autoscaler v2 ReadOnlyProvider.terminate() 签名不匹配 | `git cherry-pick b9bc54f` |
| 255 | `9bf32b751d` | [#62279](https://github.com/ray-project/ray/pull/62279) | Core | optimization | P2 | 放宽 worker 线程数限制 | `git cherry-pick 9bf32b751d` |
| 256 | `8d53131a15` | [#61701](https://github.com/ray-project/ray/pull/61701) | Core | feature | P3 | 添加通用 PlatformEvent proto（K8s/Slurm 事件） | `git cherry-pick 8d53131` |
| 257 | `d3b06f8d66` | [#62209](https://github.com/ray-project/ray/pull/62209) | Data | refactor | P3 | 资源预算 Prometheus 指标移至 ExecutionCallback | `git cherry-pick d3b06f8` |
| 258 | `735e6fb9be` | [#62145](https://github.com/ray-project/ray/pull/62145) | Data | optimization | P2 | 统计数据计算优化：多次遍历合并为单次 | `git cherry-pick 735e6fb` |
| 259 | `cfd4ac9e55` | [#62120](https://github.com/ray-project/ray/pull/62120) | Data | test | skip | 修复 flaky test bare assert | - |
| 260 | `357175bdbb` | [#61814](https://github.com/ray-project/ray/pull/61814) | Core | feature | P2 | IPPR [4/N]：独立的 KubeRay IPPR Provider | `git cherry-pick 357175b` |
| 261 | `b01ef6aae0` | [#60317](https://github.com/ray-project/ray/pull/60317) | Core | build | P2 | 升级 cloudpickle 到 3.1.2 支持 Python 3.14 | `git cherry-pick b01ef6aae0` |
| 262 | `02c380ffdf` | [#61305](https://github.com/ray-project/ray/pull/61305) | Data | test | skip | 添加 TPCH Q13 测试 | - |
| 263 | `4127cd62cd` | [#62303](https://github.com/ray-project/ray/pull/62303) | Core | test | skip | 禁用 cgraph GPU 测试 | - |
| 264 | `232459596f` | [#62405](https://github.com/ray-project/ray/pull/62405) | Data | feature | P2 | 默认禁用 hanging issue 检测 | `git cherry-pick 2324595` |

---
## 统计汇总

| 重要性 | 数量 | 说明 |
|--------|------|------|
| **P0** | 12 | 安全漏洞、崩溃、死锁、数据正确性，必须迁移 |
| **P1** | 37 | 重要 bugfix、关键性能优化，强烈建议迁移 |
| **P2** | 100 | 功能增强、中等优化、一般 bugfix |
| **P3** | 77 | 重构、清理、日志改进 |
| **skip** | 38 | 测试/CI/文档/revert，可跳过 |
| **Dashboard** | 5 | Dashboard 功能（已含在上述统计中） |

---

## Revert 配对说明

> 以下 commit 对已互相抵消，cherry-pick 时两者都不需要 pick（表格1中已标为 skip）。

| 原始 Commit | 原始 PR | Revert Commit | Revert PR | 说明 |
|---|---|---|---|---|
| `c537a44` | [#60760](https://github.com/ray-project/ray/pull/60760) | `f3d444a` | [#60818](https://github.com/ray-project/ray/pull/60818) | ReferenceCounter 调试 API，添加后被 revert |
| `021e7e1` | [#60631](https://github.com/ray-project/ray/pull/60631) | `7d3c719` | [#61040](https://github.com/ray-project/ray/pull/61040) | 禁用 UnionOperator 节流，后被 revert |
| `66949f9` | [#61605](https://github.com/ray-project/ray/pull/61605) | `91eb738` | [#61729](https://github.com/ray-project/ray/pull/61729) | 防止算子独占 object store 预算，后被 revert |

> **gRPC 升级特殊链**：
> - [#61195](https://github.com/ray-project/ray/pull/61195) — 第一次升级 gRPC 到 1.58.0（表格3，不在表格1）
> - [#61449](https://github.com/ray-project/ray/pull/61449) — Revert #61195（表格1 P2）
> - [#61499](https://github.com/ray-project/ray/pull/61499) — 第二次升级 gRPC 到 1.58.0，改为同步等待 metrics exporter（表格1 P2）
>
> Cherry-pick 策略：跳过 #61195，只 pick #61449 + #61499（按序），或直接只 pick #61499。

---

## 2.54.x Cherry-Pick 与 2.55.1 对照表

> 本地 master 已包含 `ray/releases/2.54.0` 的 14 个 cherry-pick。以下列出这些 commit 在 2.55.1 上的对应关系。
> 升级到 2.55.1 时这些修复**不会丢失**。

| # | master commit | master PR | 2.55.1 commit | 原始 PR | 状态 | 说明 |
|---|---|---|---|---|---|---|
| 1 | `35e7f48007` | #60788 | `e7fa2e4204` | [#60754](https://github.com/ray-project/ray/pull/60754) | ✅ 已包含 | Serve 请求 draining 修复 |
| 2 | `09235047aa` | #60791 | `4c7679273b` | [#60745](https://github.com/ray-project/ray/pull/60745) | ✅ 已包含 | mTLS OTLP exporter 修复 |
| 3 | `1b1a9bd250` | #60808 | `c7a2db94af` | [#60798](https://github.com/ray-project/ray/pull/60798) | ✅ 已包含 | Data 反压终端算子修复 |
| 4 | `0f27a3a2ae` | #60871 | `9571dd55d6` | [#60784](https://github.com/ray-project/ray/pull/60784) | ✅ 已包含 | Serve video analysis 修复 |
| 5 | `a77457bac5` | #60891 | `0d024e63b7` | [#60882](https://github.com/ray-project/ray/pull/60882) | ✅ 已包含 | ActorPool 资源借用修复 |
| 6 | `e94871d2c1` | #60893 | `7faa24f55d` | [#60881](https://github.com/ray-project/ray/pull/60881) | ✅ 已包含 | Limit 推过 map_groups 修复 |
| 7 | `5d2115c6a8` | #60897 | `9eac698a1c` | [#60887](https://github.com/ray-project/ray/pull/60887) | ✅ 已包含 | doc 依赖 pin |
| 8 | `620214fbd5` | #60888 | `06a1781334` | [#60742](https://github.com/ray-project/ray/pull/60742) | ✅ 已包含 | RLlib 测试修复 |
| 9 | `f8e1102869` | #60907 | `338087b8b0` | [#60852](https://github.com/ray-project/ray/pull/60852) | ✅ 已包含 | Core 测试修复 |
| 10 | `165b4aace9` | #60998 | `9cbcdb4689` | [#60997](https://github.com/ray-project/ray/pull/60997) | ✅ 已包含 | min_scheduling_resources 修复 |
| 11 | `48bd1f8fa4` | #61066 | `feca47613b` | [#61064](https://github.com/ray-project/ray/pull/61064) | ✅ 已包含 | revert task state（2.55.1 用 #61064） |
| 12 | `760bea13ce` | #60783 | — | — | ⏭️ 不需要 | release version 变更 |
| 13 | `6835277714` | #60933 | — | — | ❌ 不在 2.55.1 | rllib 禁用 flaky 测试（不影响功能） |
| 14 | `1ea4980a1d` | #61157 | — | — | ❌ 不在 2.55.1 | docker 依赖更新（2.55.1 有自己的版本） |

## P0 Commit 快速参考

```bash
# === P0: 必须迁移（按分支顺序） ===


# #52 - owner 节点死亡时销毁所有 owned actors
git cherry-pick 47256b6


# #250 - OpBufferQueue 竞态条件
git cherry-pick 2bd8fed

# #265 - UDF 清理竞态条件
git cherry-pick 16eaed0

# #268 - absl Mutex 重入锁 SIGABRT
git cherry-pick 23e587f

# #297 - RUNNING 任务指标负值
git cherry-pick 40df499

# #304 - 🔒 Arrow RCE 安全漏洞
git cherry-pick c02bd31

# #307 - pop worker 失败任务永久卡住
git cherry-pick 0f7a9f1

# #310 - StreamSplitDataIterator OOM
git cherry-pick 3cce4b5

# #319 - pg.ready() 死锁
git cherry-pick 945f423


# #327 - 布尔环境变量解析 bug
git cherry-pick e0bc348

# #328 - 资源管理器未计入外部消费者 Object Store
git cherry-pick 4d5b3d0

# #331 - Parquet batch_size 32 位溢出
git cherry-pick 0ed5173

```

## P1 Commit 快速参考

```bash
# === P1: 强烈建议（按分支顺序） ===

git cherry-pick c7a2db9  # #11 - 输出反压终端算子修复
git cherry-pick 3139d0d  # #19 - pg.ready() 性能提升 (async GCS RPC)
git cherry-pick 7faa24f  # #33 - Limit 推过 map_groups 修复
git cherry-pick ed6a8a2  # #44 - actor 任务队列阻塞修复
git cherry-pick 37f2eb4  # #45 - autoscaler K8s 异常重试
git cherry-pick b8bb03b  # #55 - 资源管理器加入逻辑内存
git cherry-pick 9cbcdb4  # #65 - min_scheduling_resources 回退修复
git cherry-pick 89148a4  # #76 - 消除生成器避免中间状态 pin
git cherry-pick 906ce37585  # #79 - aggregator agent 指数退避溢出
git cherry-pick 38a8766  # #82 - ray.init() 节点发现重试
git cherry-pick 5b9f069  # #83 - 任务结束重置 DataContext
git cherry-pick 4e34398229  # #95 - 禁用 memory_full_info 减少系统调用
git cherry-pick 1bd59fe  # #96 - _align_struct_fields 标量修复
git cherry-pick c11c103  # #124 - 多输入算子内存双重计数
git cherry-pick 1b85bf7  # #143 - 防止聚合任务调度到 head
git cherry-pick c73d04e  # #151 - metrics exporter 初始化竞态
git cherry-pick f654eee  # #153 - worker listener 线程优化
git cherry-pick c190c7f  # #171 - actor 任务按并发组排序
git cherry-pick 8af3b3e  # #202 - autoscaler WrongClusterID 恢复
git cherry-pick 4902739  # #227 - OOM Killer 优先杀大内存 Worker
git cherry-pick ad47d57  # #247 - 缓存 find_gcs_addresses
git cherry-pick 9971f87  # #251 - 版本不匹配清理孤儿进程
git cherry-pick b75d209  # #269 - Actor Pool 调度开销 -57%
git cherry-pick 7985f5d  # #279 - ActorMethod 引用循环修复
git cherry-pick 732c259  # #280 - 放宽自动扩缩容约束
git cherry-pick 82c1051  # #289 - 执行回调静态初始化
git cherry-pick 59c4372  # #300 - Actor 排名 O(N*M)→O(N*logM)
git cherry-pick 1ce6214  # #301 - 缓存 _map_task 参数
git cherry-pick d032daf  # #303 - 大 schema 哈希/比较优化
git cherry-pick 1a92e91  # #305 - 反压阈值 90%→50%
git cherry-pick 31cda7c  # #326 - 缓存 ActorHandle.__hash__

---

## 迁移策略

### 推荐方案：分批 Cherry-Pick

```bash
# 1. 准备
git fetch ray
git checkout master
git checkout -b cherry-pick/ray-2.55.1

# 2. 按 P0 → P1 → P2 → P3 顺序逐批 cherry-pick
#    每批完成后编译验证

# 3. 验证
bazel build //:gen_ray_pkg
pytest python/ray/tests/test_basic.py python/ray/tests/test_actor.py -v
pytest python/ray/data/tests/test_dataset.py python/ray/data/tests/test_map.py -v
pytest python/ray/serve/tests/ -v
```

### 冲突风险

- **已修改文件**：当前分支修改了 `gcs_task_manager.cc`、`gcs_service.proto`、`state_aggregator.py`、`common.py`，与上游有冲突风险
- **依赖链**：
  - IPPR 系列 ([#61654](https://github.com/ray-project/ray/pull/61654) → [#61666](https://github.com/ray-project/ray/pull/61666) → [#61803](https://github.com/ray-project/ray/pull/61803) → [#61814](https://github.com/ray-project/ray/pull/61814)) 需按序 pick
  - ExecutionPlan 移除系列 5 个 PR 有强依赖，建议一起 pick 或全部跳过
  - `[#61934](https://github.com/ray-project/ray/pull/61934)` (ActorMethod 引用循环) 是 `[#62036](https://github.com/ray-project/ray/pull/62036)` (回退变通方案) 的前置
  - `[#61405](https://github.com/ray-project/ray/pull/61405)` (执行回调重构) 是 `[#62055](https://github.com/ray-project/ray/pull/62055)` 和 `[#62209](https://github.com/ray-project/ray/pull/62209)` 的前置
- **反压阈值** ([#61890](https://github.com/ray-project/ray/pull/61890))：从 90% 降至 50% 是行为变更，建议灰度验证
- **gRPC 升级** ([#61499](https://github.com/ray-project/ray/pull/61499))：v1.58.0 可能影响编译和兼容性（详见下方 Build 风险分析）

### Build / 编译相关风险分析

> 当前 master 使用 **Bazel 6.5.0**，社区 2.55.1 已升级到 **Bazel 7.5.0**。
> 以下分析各 Build commit 对编译的影响，帮助决定是否需要 pick。

#### 高风险 — Bazel 7 升级链（5 个 commit，强依赖，全部不在表格1）

这 5 个 commit 构成完整的 Bazel 7 升级链，必须**一起 pick 或全部跳过**：

| PR | Commit | 说明 | 不 pick 的影响 |
|---|---|---|---|
| [#61601](https://github.com/ray-project/ray/pull/61601) | `efc25f434a` | Bazel 6.5.0 → 7.5.0（.bazelversion, WORKSPACE） | 核心升级，后续 4 个都依赖此 |
| [#61667](https://github.com/ray-project/ray/pull/61667) | `09cacbbd13` | macOS `_raylet.so` linker flag `-undefined dynamic_lookup` | Bazel 7 下 macOS 编译失败 |
| [#61669](https://github.com/ray-project/ray/pull/61669) | `af9ae6ae19` | 替换 lzma patches 为自定义 BUILD file | Bazel 7 下 lzma 编译失败 |
| [#61694](https://github.com/ray-project/ray/pull/61694) | `a0750952b1` | Patch protobuf for Bazel 7 `exec_tools` 移除 | Bazel 7 下 protobuf 编译失败 |
| [#61695](https://github.com/ray-project/ray/pull/61695) | `2a5a4465c4` | 升级 rules_apple/apple_support + macOS toolchain flags | Bazel 7 下 macOS 编译失败 |

**结论：如果保持 Bazel 6.5.0，这 5 个全部跳过，不影响编译。如果要升级 Bazel 7，必须全部 pick。**

#### 中风险 — gRPC v1.58.0 升级（已在表格1，可在 Bazel 6 下独立 pick）

[#61499](https://github.com/ray-project/ray/pull/61499) 升级 gRPC v1.57.1 → v1.58.0，修改了 `bazel/ray_deps_setup.bzl`：
- 更新 gRPC 和 boringssl 的 URL/SHA256
- 新增 `grpc-disable-layering-check.patch`（移除 gRPC BUILD 中的 `layering_check` feature）
- 新增 `grpc-nextresult-cancelled-init.patch`

其中 `grpc-disable-layering-check.patch` 注释提到 "Fixed in Bazel 7.3.0"，但该 patch 只是**移除** gRPC 源码 BUILD 中的 feature flag，在 Bazel 6.5.0 下同样可以正常工作。

配套 commit：
- [#61449](https://github.com/ray-project/ray/pull/61449) — Revert 第一次 gRPC 升级（表格1，需先于 #61499 pick）
- [#61281](https://github.com/ray-project/ray/pull/61281) — 同步等待 metrics exporter 初始化（表格1，解决 getenv/setenv 竞态）

**结论：gRPC 升级不依赖 Bazel 7，可在 Bazel 6.5.0 下安全 pick。建议按顺序 pick #61449 → #61281 → #61499。**

#### 低风险 — 其他独立 Build commit（全部不在表格1，可选）

| PR | Commit | 说明 | 建议 |
|---|---|---|---|
| [#61357](https://github.com/ray-project/ray/pull/61357) | `20925a36a2` | `setup-dev.py` 支持重复执行 | 可选，仅开发便利 |
| [#61042](https://github.com/ray-project/ray/pull/61042) | `2e70e0dbe5` | 新增顶层 `build-image.sh` | 可选，CI 镜像构建用 |
| [#61668](https://github.com/ray-project/ray/pull/61668) | `4447e6a81f` | Java BUILD.bazel 调整 | 不用 Java 则跳过 |
| [#61722](https://github.com/ray-project/ray/pull/61722) | `9b1c4ffb61` | `ray-images.yaml` → `ray-images.json` | CI 镜像配置，跳过 |

#### 已在表格1中的 Build 相关 commit

以下 Build 相关 commit 已包含在表格1中，均**不依赖 Bazel 7**，可安全 pick：

| 表格1序号 | PR | 说明 |
|---|---|---|
| 4 | [#60736](https://github.com/ray-project/ray/pull/60736) | protobuf Python/Java 版本升级到 3.20.3 |
| 108 | [#61147](https://github.com/ray-project/ray/pull/61147) | 防止 protobuf 符号泄漏 |
| 129 | [#61294](https://github.com/ray-project/ray/pull/61294) | bazel param file（Windows 专用，skip） |
| 138 | [#61298](https://github.com/ray-project/ray/pull/61298) | 收紧 ray export 符号白名单 |
| 148 | [#61449](https://github.com/ray-project/ray/pull/61449) | Revert 第一次 gRPC 升级 |
| 199 | [#61499](https://github.com/ray-project/ray/pull/61499) | gRPC v1.58.0 升级 |
| 243 | [#61845](https://github.com/ray-project/ray/pull/61845) | StatusOr 编译告警修复（C++ header only） |
| 340 | [#60317](https://github.com/ray-project/ray/pull/60317) | cloudpickle 升级到 3.1.2 |

### 关联 Issue / PR 详解

#### PR [#60295](https://github.com/ray-project/ray/pull/60295) — StreamingRepartition `strict=False` 模式

> **状态**：已合入（2026-02-23） | **模块**：Ray Data | **表格1 序号**：#85

**问题背景**：`StreamingRepartition` 算子原来只支持 strict 模式，要求所有输出 block（最后一个除外）都恰好为 `target_num_rows` 行。这导致算子必须在跨 block 边界处做 stitching（拼接），**无法与上游算子融合**。

**解决方案**：新增 `strict: bool = False` 参数（默认 non-strict），两种模式差异：

| 模式 | 保证 | Bundler | 可否融合 |
|------|------|---------|----------|
| `strict=True` | 所有输出 block = `target_num_rows`（最后一个除外） | `StreamingRepartitionRefBundler` | 不可融合 |
| `strict=False`（默认） | 每个输入 block 最多产生 1 个 < `target_num_rows` 的 block（不做跨 block 拼接） | `BlockRefBundler`（默认） | **可融合到上游算子** |

**修改文件**：
- `python/ray/data/dataset.py` — `repartition()` 新增 `strict` 参数
- `python/ray/data/_internal/logical/operators/map_operator.py` — 逻辑算子传递 strict
- `python/ray/data/_internal/logical/rules/operator_fusion.py` — non-strict 模式下允许融合
- `python/ray/data/_internal/logical/rules/combine_shuffles.py` — shuffle 合并规则适配
- `python/ray/data/_internal/planner/plan_udf_map_op.py` — 根据 strict 选择 bundler

**cherry-pick 影响**：中等。修改了 Data 执行计划核心路径（fusion rule、planner），与 ExecutionPlan 移除系列有交叉依赖。

---

#### Issue [#63544](https://github.com/ray-project/ray/issues/63544) — 降低 StreamingExecutor 调度循环开销

> **状态**：Open（跟踪 issue） | **模块**：Ray Data | **关键指标**：`max_scheduling_loop_duration_s`

**问题背景**：在大规模 Ray Data 负载下（宽 schema + 数百/数千并发 actor），StreamingExecutor 的 **driver 端调度线程** 成为整体执行瓶颈 —— worker 空闲等待调度器完成 per-block 记录。核心观测指标为 `max_scheduling_loop_duration_s`（`ds.stats()` 可见）。

该 issue 收集了所有相关优化 PR，分为 4 个方向：

**1. 指标 / 可观测性**

| PR | 说明 | 2.55.1 | master | 本文档 |
|---|---|---|---|---|
| [#56390](https://github.com/ray-project/ray/pull/56390) | 修复 metrics query（iteration + scheduling loop） | ✅ | ✅ 已有 | — |
| [#62217](https://github.com/ray-project/ray/pull/62217) | Operator start/stop metrics | ❌ | ❌ | — |
| [#62249](https://github.com/ray-project/ray/pull/62249) | Task block locality metric | ❌ | ❌ | — |
| [#62372](https://github.com/ray-project/ray/pull/62372) | Runtime-env setup time in release tests | ❌ | ❌ | — |
| [#62436](https://github.com/ray-project/ray/pull/62436) | Raylet scheduling overhead in release tests | ❌ | ❌ | — |
| [#62453](https://github.com/ray-project/ray/pull/62453) | 使用 `get_stats_summary()` in release tests | ❌ | ❌ | — |
| [#63345](https://github.com/ray-project/ray/pull/63345) | 添加 `max_scheduling_loop_duration_s` 到 `DatasetStatsSummary` | ❌ | ❌ | — |
| [#63420](https://github.com/ray-project/ray/pull/63420) | Wide-schema worker_scaling release tests | ❌ | ❌ | — |

**2. Driver 端 Schema / 元数据优化**

| PR | 说明 | 2.55.1 | master | 本文档 |
|---|---|---|---|---|
| [#53454](https://github.com/ray-project/ray/pull/53454) | 从 `BlockMetadata` 中移除 `Schema` | ✅ | ✅ 已有 | — |
| [#62720](https://github.com/ray-project/ray/pull/62720) | `_map_task` 每个 task 仅在首个 block yield schema | ❌ | ❌ | — |
| [#62726](https://github.com/ray-project/ray/pull/62726) | per-task return 用 `pickle` 替代 `cloudpickle` | ❌ | ❌ | — |
| [#63462](https://github.com/ray-project/ray/pull/63462) | LRU-cache Arrow schema 反序列化 | ❌ | ❌ | — |

**3. Actor-pool / 调度分发优化**（与表格1中的调度优化系列相关）

| PR | 说明 | 2.55.1 | master | 本文档 |
|---|---|---|---|---|
| [#61288](https://github.com/ray-project/ray/pull/61288) | 防止 aggregator 调度到 head 节点 | ✅ | ❌ | **表格1 #143**（P1） |
| [#61996](https://github.com/ray-project/ray/pull/61996) | 缓存 `_map_task` 公共参数 | ✅ | ❌ | **表格1 #301**（P1） |
| [#62114](https://github.com/ray-project/ray/pull/62114) | 基于堆的 actor 排名 O(N*logM) | ✅ | ❌ | **表格1 #300**（P1） |
| [#62309](https://github.com/ray-project/ray/pull/62309) | 按节点堆排 actor | ❌ | ❌ | — |
| [#62891](https://github.com/ray-project/ray/pull/62891) | Final bundle clean-up | ❌ | ❌ | — |

**4. 其他**

| PR | 说明 | 2.55.1 | master | 本文档 |
|---|---|---|---|---|
| [#57971](https://github.com/ray-project/ray/pull/57971) | 移除 stats-update 线程 | ✅ | ✅ 已有 | — |

**与 cherry-pick 的关系**：
- **已在表格1中、需 cherry-pick 的关键 PR**：#61288（P1）、#61996（P1）、#62114（P1），这 3 个是 2.55.1 中对调度循环性能最直接的改进
- **不在 2.55.1 中的 PR**（#62217, #62249, #62309, #62720, #62726, #62891, #63345, #63420, #63462）：属于 2.55.1 之后的持续优化，当前 cherry-pick 不涉及，但后续版本同步时需关注
- **已在 master 公共历史中的 PR**（#56390, #53454, #57971）：fork point 之前已合入，无需 cherry-pick

---

#### PR [#61821](https://github.com/ray-project/ray/pull/61821) — Checkpoint 两阶段提交 + 前缀 Trie 恢复

> **状态**：已合入（2026-03-19） | **模块**：Ray Data | **表格1 序号**：#245（P2）

**问题背景**：原有 checkpoint 机制在数据文件写入**之后**才写 checkpoint。如果 task 在写完数据但还没写 checkpoint 时失败，恢复流程会重新写入相同数据，导致**数据文件重复**。对于 file-based datasink（Parquet 等），这是数据正确性问题。

**解决方案**：引入两阶段提交（2PC），确保 exactly-once 写入语义：

```
Phase 1 (Pre-write):  写入 pending checkpoint (.pending.parquet)，记录预期数据文件前缀
Phase 2 (Write):      写入实际数据文件
Phase 3 (Post-write): 提交 checkpoint（rename .pending.parquet → .parquet）
```

恢复时：pending checkpoint 标识出未完成的数据文件 → 删除后重试。

**前缀 Trie 恢复算法**（替代原有 per-checkpoint metadata 方案）：
1. 列出所有 `.pending.parquet` 文件，用文件名构建 `PrefixTrie`
2. 递归列出所有数据文件（支持分区输出）
3. 删除文件名匹配 trie 前缀的数据文件
4. 删除 pending checkpoint 文件

**关键设计决策**：
- **为什么先写 checkpoint 再写数据？** `pq.write_table` 非原子操作 — 进程被杀时文件可能已存在但已损坏。先写 pending checkpoint 再 rename 可确保始终能检测并清理不完整写入
- **非文件 datasink**（SQL、MongoDB 等）：回退到 post-write checkpoint，提供 at-least-once 语义（附带告警）

**修改文件**（14 个）：
- `checkpoint_writer.py` / `checkpoint_filter.py` — 2PC 核心逻辑
- `load_checkpoint_callback.py` — 恢复时传递 `data_file_dir` / `data_file_filesystem`
- `filename_provider.py` — 新增 `get_filename_for_task()`（确定性命名，使 checkpoint ID 匹配数据文件前缀）
- `file_datasink.py` / `parquet_datasink.py` — 适配 2PC 流程
- `planner.py` / `plan_write_op.py` — planner 层传递恢复所需参数

**依赖关系**：
- 前置：[#60951](https://github.com/ray-project/ray/pull/60951)（表格1 #76）— `LoadCheckpointCallback` 数据加载方法
- 配套测试：[#61047](https://github.com/ray-project/ray/pull/61047)（表格3）— 放宽 checkpoint 恢复测试为 at-least-once 语义

**cherry-pick 影响**：中高。涉及 checkpoint 子系统 14 个文件的重构，修改了 `FilenameProvider` 公共 API（废弃 `get_filename_for_block()` / `get_filename_for_row()`）。建议与 #60951 一起 pick。

---


## 表格 2：全部缺失的 commit（按分支 commit 顺序）

> Core/Data/Dashboard/Serve 模块共 454 个缺失 commit，按分支上的真实 commit 顺序排列。

| # | Commit | PR | 模块 | 描述 |
|---|--------|-----|------|------|
| 1 | `8c732fecf5` | [#60365](https://github.com/ray-project/ray/pull/60365) | Serve | — | **修复节点迁移期间 replica 排名一致性检查失败** |
| 2 | `e7fa2e4204` | [#60754](https://github.com/ray-project/ray/pull/60754) | Serve | — | **修复 direct ingress 模式下请求卡在 draining 导致 replica 永久挂起** |
| 3 | `6a5e3de35c` | [#59548](https://github.com/ray-project/ray/pull/59548) | Serve | — | 添加默认的基于队列的自动扩缩策略 [2/3] |
| 4 | `4a024799d7` | [#60736](https://github.com/ray-project/ray/pull/60736) | Core | ✅ | Bump python/java protobuf 版本到 3.20.3 |
| 5 | `f27985d268` | [#60765](https://github.com/ray-project/ray/pull/60765) | Core | — | Fix `test_state_api` (#60765) |
| 6 | `2afe98d142` | [#60740](https://github.com/ray-project/ray/pull/60740) | Core | — | fix test_aggregator_agent flaky test (#60740) |
| 7 | `c0fc46b849` | [#58459](https://github.com/ray-project/ray/pull/58459) | Core | ✅ | 添加 Python 3.14 递归限制处理支持 |
| 8 | `cf6ca75a98` | [#60757](https://github.com/ray-project/ray/pull/60757) | Serve | — | 吞吐量优化启用时添加环境变量覆盖 |
| 9 | `aec74f035d` | [#60711](https://github.com/ray-project/ray/pull/60711) | Data | ✅ | 修复 `AliasExpr` 结构相等性判断，正确处理 rename 标记 |
| 10 | `521cb0ece4` | [#60758](https://github.com/ray-project/ray/pull/60758) | Serve | — | 添加 Ray Serve replica 利用率指标 |
| 11 | `db822f500e` | [#60647](https://github.com/ray-project/ray/pull/60647) | Core | ✅ | 移除已废弃的 `local_mode` 支持 |
| 12 | `fc91ac2b4d` | [#60767](https://github.com/ray-project/ray/pull/60767) | Serve | — | gRPC 双向流核心类型和公共 API [1/n] |
| 13 | `c7a2db94af` | [#60798](https://github.com/ray-project/ray/pull/60798) | Data | ✅ | 修复输出反压解锁序列，正确处理终端算子 |
| 14 | `f7425043ec` | [#60712](https://github.com/ray-project/ray/pull/60712) | Core | ✅ | 添加本地加载类的 Actor 转换逻辑 |
| 15 | `a687ba4444` | [#60779](https://github.com/ray-project/ray/pull/60779) | Core | ✅ | Ray sync server 使用 AuthenticationValidator |
| 16 | `c537a447b1` | [#60760](https://github.com/ray-project/ray/pull/60760) | Core | ✅ | 添加 ReferenceCounter 内部状态调试 API（后被 revert） |
| 17 | `98b56eae15` | [#60752](https://github.com/ray-project/ray/pull/60752) | Core | ✅ | 资源隔离 [2/n]：修改内存监控器支持 mock 测试 |
| 18 | `5564461905` | [#60792](https://github.com/ray-project/ray/pull/60792) | Core | ✅ | Cython 方法清理和文档化 |
| 19 | `b93fc26472` | [#60803](https://github.com/ray-project/ray/pull/60803) | Data | ✅ | 添加 MOD 操作文档 |
| 20 | `f3d444ab01` | [#60818](https://github.com/ray-project/ray/pull/60818) | Core | ✅ | Revert [#60760](https://github.com/ray-project/ray/pull/60760) |
| 21 | `3139d0d897` | [#60657](https://github.com/ray-project/ray/pull/60657) | Core | ✅ | **显著提升 pg.ready() 性能**：用 async GCS RPC 替代 dummy task |
| 22 | `0edea4b6f9` | [#60810](https://github.com/ray-project/ray/pull/60810) | Serve | — | 对无 `record_routing_stats` 的 deployment 跳过路由统计收集 |
| 23 | `1949d6094d` | [#60829](https://github.com/ray-project/ray/pull/60829) | Serve | — | 替换 `ReplicaStateContainer.get()` 中 O(n^2) 列表拼接 |
| 24 | `02ab6f73f8` | [#60830](https://github.com/ray-project/ray/pull/60830) | Serve | — | `ReplicaStateContainer.count()` 用生成器 sum 替换 `len(list(filter(...)))` |
| 25 | `2f2fa87d73` | [#60838](https://github.com/ray-project/ray/pull/60838) | Serve | — | 修复 `allow_new_compaction` 中 O(n^2) replica 计数 |
| 26 | `4e3f034ae3` | [#60819](https://github.com/ray-project/ray/pull/60819) | Dashboard | — | Add NIXL KV transfer metrics to Serve LLM Grafana dashboard (#60819) |
| 27 | `c1b051cd6b` | [#60844](https://github.com/ray-project/ray/pull/60844) | Serve | — | 缓存 `AutoscalingPolicy.get_policy()` 中反序列化的策略，避免重复 `cloudpickle.loads()` |
| 28 | `641d4e52b5` | [#60843](https://github.com/ray-project/ray/pull/60843) | Serve | — | 修复 `ClusterNodeInfoCache.update()` 排序 bug 并优化 |
| 29 | `2df3ca07a7` | [#60842](https://github.com/ray-project/ray/pull/60842) | Serve | — | `record_request_routing_info` 中 O(1) replica 查找 |
| 30 | `eb4a361c3d` | [#60833](https://github.com/ray-project/ray/pull/60833) | Serve | — | 消除 `update_actor_details` 中每 replica 每 tick 的 Pydantic rebuild |
| 31 | `3b69ec307f` | [#60832](https://github.com/ray-project/ray/pull/60832) | Serve | — | 优化 `stop_replicas()` 避免 pop-all/re-add 循环 |
| 32 | `16ccd3e979` | [#60768](https://github.com/ray-project/ray/pull/60768) | Serve | — | 重构 gRPC server 使用 streaming type 枚举 [2/n] |
| 33 | `4a8845de51` | [#60849](https://github.com/ray-project/ray/pull/60849) | Core | ✅ | 将共享测试工具从 `_private` 迁移到 `_common` |
| 34 | `10ddd43451` | [#60288](https://github.com/ray-project/ray/pull/60288) | Core | ✅ | 填充 Actor 和 Task 事件中缺失的字段（part 2） |
| 35 | `338087b8b0` | [#60852](https://github.com/ray-project/ray/pull/60852) | Core | — | Fix test_failed_task_runtime_env_setup failure on windows (#60852) |
| 36 | `0d024e63b7` | [#60882](https://github.com/ray-project/ray/pull/60882) | Data | ✅ | 修复 `ReservationOpResourceAllocator` 对 ActorPoolMapOperator 资源借用错误 |
| 37 | `7faa24f55d` | [#60881](https://github.com/ray-project/ray/pull/60881) | Data | ✅ | 防止 `Limit` 被推过 `map_groups`，避免语义错误 |
| 38 | `7808569e40` | [#60811](https://github.com/ray-project/ray/pull/60811) | Core | ✅ | 修复 dashboard event agent 缺少 http scheme 报错 |
| 39 | `dbc2e95d34` | [#60486](https://github.com/ray-project/ray/pull/60486) | Serve | — | CI 改进：[deps] Generating and installing depset on serve ci image |
| 40 | `7ecbca7da1` | [#60695](https://github.com/ray-project/ray/pull/60695) | Data | ✅ | 表达式中添加 cast 类型转换方法 |
| 41 | `fa31667273` | [#60029](https://github.com/ray-project/ray/pull/60029) | Data | — | Add polars usage instruction to docs (#60029) |
| 42 | `3e1de42ad2` | [#60630](https://github.com/ray-project/ray/pull/60630) | Data | ✅ | 修复 dataset.py 文档字符串 |
| 43 | `e5b38df7f9` | [#60578](https://github.com/ray-project/ray/pull/60578) | Data | ✅ | 组织 test_formats.py 测试 |
| 44 | `0a0d40fbcf` | [#60652](https://github.com/ray-project/ray/pull/60652) | Core | ✅ | 将 named actor 集成测试转为单元测试 |
| 45 | `b4d21ea6ec` | [#60905](https://github.com/ray-project/ray/pull/60905) | Data | ✅ | OpMetrics 回退为 JSON 视图 |
| 46 | `4a008742e5` | [#60899](https://github.com/ray-project/ray/pull/60899) | Data | ✅ | 为 autoscaler/resource_allocator 创建添加日志 |
| 47 | `021e7e10e9` | [#60631](https://github.com/ray-project/ray/pull/60631) | Data | ✅ | 禁用 UnionOperator 节流（后被 revert） |
| 48 | `577c529f90` | [#60586](https://github.com/ray-project/ray/pull/60586) | Serve | — | **添加 HAProxy 支持** |
| 49 | `d043552df3` | [#60823](https://github.com/ray-project/ray/pull/60823) | Serve | — | 控制器上节流 `serve_deployment_replica_healthy` gauge 记录 |
| 50 | `ed6a8a2632` | [#60850](https://github.com/ray-project/ray/pull/60850) | Core | ✅ | **修复取消 head task 后 actor 任务队列阻塞** |
| 51 | `37f2eb4a21` | [#60658](https://github.com/ray-project/ray/pull/60658) | Core | ✅ | **修复 K8s 异常时 autoscaler 重试机制失败** |
| 52 | `4f87029de3` | [#60896](https://github.com/ray-project/ray/pull/60896) | Dashboard | ✅ | Dashboard 支持 Grafana 日志链接 |
| 53 | `dd10a86e34` | [#60790](https://github.com/ray-project/ray/pull/60790) | Data | ✅ | 修复 OneHotEncoder `max_categories` 使用全局 top-k 而非分区级别 |
| 54 | `9192aefb2d` | [#58910](https://github.com/ray-project/ray/pull/58910) | Data | ✅ | 新增 Turbopuffer Datasink |
| 55 | `0eecdde17e` | [#60772](https://github.com/ray-project/ray/pull/60772) | Dashboard | ✅ | Dashboard 添加 Logical Memory Usage 面板 |
| 56 | `27698a6c16` | [#59290](https://github.com/ray-project/ray/pull/59290) | Data | ✅ | 新增单调递增 ID 生成器 |
| 57 | `3e187a3cdb` | [#60822](https://github.com/ray-project/ray/pull/60822) | Serve | — | 修复 `enable_access_log=False` 未抑制 stderr 上的访问日志 |
| 58 | `47256b60ff` | [#60669](https://github.com/ray-project/ray/pull/60669) | Core | ✅ | **修复 owner 节点死亡时未销毁所有 owned actors 的问题** |
| 59 | `b0e5ba97e4` | [#60826](https://github.com/ray-project/ray/pull/60826) | Core | ✅ | **减小 dashboard event aggregator 缓冲区避免 OOM** |
| 60 | `053041c253` | [#60923](https://github.com/ray-project/ray/pull/60923) | Data | — | Fix flaky test_map_operator_streamed due to ordering assumption (#60923) |
| 61 | `c08329b60e` | [#60642](https://github.com/ray-project/ray/pull/60642) | Core+Data | ✅ | 将 `env_float/env_integer/env_bool` 移到 `ray._common` |
| 62 | `b8bb03b096` | [#60774](https://github.com/ray-project/ray/pull/60774) | Data | ✅ | 资源管理器调度决策中加入逻辑内存 |
| 63 | `6f0458ba42` | [#60575](https://github.com/ray-project/ray/pull/60575) | Data | ✅ | 移除遗留的 BlockList 类 |
| 64 | `2ab4760d32` | [#60921](https://github.com/ray-project/ray/pull/60921) | Data | — | Add job-level checkpointing documentation (#60921) |
| 65 | `301869282f` | [#60956](https://github.com/ray-project/ray/pull/60956) | Data | — | disable sort_chaos test (#60956) |
| 66 | `28f42bbf42` | [#60530](https://github.com/ray-project/ray/pull/60530) | Data | ✅ | 从逻辑算子中移除 `output_dependencies` |
| 67 | `ef98a8903e` | [#60920](https://github.com/ray-project/ray/pull/60920) | Core | ✅ | `MarkFootprintAsIdle()` 对已空闲节点变为 no-op |
| 68 | `7b945c7b32` | [#60778](https://github.com/ray-project/ray/pull/60778) | Core | ✅ | 定期重新加载 Ray service account token |
| 69 | `023e2e1a2a` | [#60854](https://github.com/ray-project/ray/pull/60854) | Data | — | Avoid deprecated TRANSFORMERS_CACHE and treat inability to load HuggingFace config as non-fatal (#60854) |
| 70 | `b4dc08868e` | [#60934](https://github.com/ray-project/ray/pull/60934) | Data | — | Use the default uniproc as the distributed backend (#60934) |
| 71 | `cab739f5d9` | [#60709](https://github.com/ray-project/ray/pull/60709) | Data | ✅ | 使用 `local://` 路径配合零资源 head 节点时添加描述性错误 |
| 72 | `12e3e50975` | [#60795](https://github.com/ray-project/ray/pull/60795) | Data | ✅ | 修复字符串拼接中的 bare raise 为 TypeError |
| 73 | `6ab7d3a327` | [#60602](https://github.com/ray-project/ray/pull/60602) | Core | ✅ | RDT：复用 nixl agent |
| 74 | `ab5461e63d` | [#60944](https://github.com/ray-project/ray/pull/60944) | Serve | — | 引入 Gang Scheduling 机制 [1/n] |
| 75 | `ee7c5d990b` | [#60690](https://github.com/ray-project/ray/pull/60690) | Data | ✅ | 去重时 Schema 不匹配警告更简洁 |
| 76 | `9cbcdb4689` | [#60997](https://github.com/ray-project/ray/pull/60997) | Data | ✅ | 修复 `min_scheduling_resources` 默认回退到 `incremental_resource_usage` |
| 77 | `5c0b632c11` | [#60964](https://github.com/ray-project/ray/pull/60964) | Serve | — | 添加基于类的自动扩缩策略支持 (`policy_kwargs`) |
| 78 | `863d74c7aa` | [#60971](https://github.com/ray-project/ray/pull/60971) | Serve | — | 修复 test_metrics_3 测试不稳定 |
| 79 | `4e83f20ae2` | [#60977](https://github.com/ray-project/ray/pull/60977) | Serve | — | 启用 task_handler 装饰器的异步任务处理支持 [1/n] |
| 80 | `a5f52841b7` | [#60995](https://github.com/ray-project/ray/pull/60995) | Core | ✅ | 使 `ray.put()` 支持泛型类型标注 |
| 81 | `4d2eadd24c` | [#60909](https://github.com/ray-project/ray/pull/60909) | Data | ✅ | `read_kafka` 支持 datetime 偏移量 |
| 82 | `2f654ada3e` | [#60985](https://github.com/ray-project/ray/pull/60985) | Serve | — | 将 multiplex 模型加载/卸载日志从 INFO 降级为 DEBUG |
| 83 | `e0c6da400a` | [#60951](https://github.com/ray-project/ray/pull/60951) | Data | ✅ | 添加 checkpoint 数据加载方法到 `LoadCheckpointCallback` |
| 84 | `5c4a443a50` | [#60835](https://github.com/ray-project/ray/pull/60835) | Data | — | Configure pyrefly for local development (#60835) |
| 85 | `529d2f8d1a` | [#60385](https://github.com/ray-project/ray/pull/60385) | Data | — | Add vLLM metrics export and Data LLM Grafana dashboard (#60385) |
| 86 | `90301f3392` | [#61008](https://github.com/ray-project/ray/pull/61008) | Serve | — | 添加多 broker Taskiq 适配器配置和初始化 [2/n] |
| 87 | `00b45989d2` | [#60845](https://github.com/ray-project/ray/pull/60845) | Serve | — | 自定义请求路由器 API 添加 `on_request_completed` 钩子 |
| 88 | `4fae2c8ec4` | [#60521](https://github.com/ray-project/ray/pull/60521) | Core | ✅ | 非生成器函数上将 StopIteration 转为 RuntimeError |
| 89 | `7d3c719849` | [#61040](https://github.com/ray-project/ray/pull/61040) | Data | ✅ | Revert [#60631](https://github.com/ray-project/ray/pull/60631) (禁用 UnionOperator 节流) |
| 90 | `89148a4554` | [#60598](https://github.com/ray-project/ray/pull/60598) | Data | ✅ | **消除生成器以避免中间状态被 pin 住**，减少内存占用 |
| 91 | `11eacfd5d5` | [#60682](https://github.com/ray-project/ray/pull/60682) | Core | ✅ | 修复 Ray Actor async 方法的类型标注 |
| 92 | `1321355222` | [#60806](https://github.com/ray-project/ray/pull/60806) | Serve | — | 优化 pack 调度从 O(replicas*total_replicas) 到 O(replicas*nodes) |
| 93 | `906ce37585` | [#61003](https://github.com/ray-project/ray/pull/61003) | Core | ✅ | **修复 aggregator agent 指数退避整数溢出** |
| 94 | `c309e4778e` | [#60851](https://github.com/ray-project/ray/pull/60851) | Serve | — | 集成基于队列的自动扩缩与 task consumer [3/3] |
| 95 | `bbeb7adbbc` | [#59604](https://github.com/ray-project/ray/pull/59604) | Core | ✅ | 引入 Process 接口抽象 |
| 96 | `38a87660e5` | [#61029](https://github.com/ray-project/ray/pull/61029) | Core | ✅ | **ray.init() 时重试节点发现**，改善启动鲁棒性 |
| 97 | `c0ebd66cd4` | [#60989](https://github.com/ray-project/ray/pull/60989) | Core | — | Deflake wait for condition test (#60989) |
| 98 | `f935ad8dcb` | [#60809](https://github.com/ray-project/ray/pull/60809) | Data | — | Add object store spill rate monitoring to train benchmark (#60809) |
| 99 | `5b9f0693b7` | [#61036](https://github.com/ray-project/ray/pull/61036) | Data | ✅ | 任务结束时重置 DataContext，防止状态泄漏 |
| 100 | `1bbaf33482` | [#60914](https://github.com/ray-project/ray/pull/60914) | Serve | — | CI 改进：[Serve] Add dedicated Buildkite target for HAProxy tests |
| 101 | `befc7e5c8e` | [#61060](https://github.com/ray-project/ray/pull/61060) | Core | ✅ | 设置 worker 进程前检查 process 非空 |
| 102 | `c3f787b5d8` | [#60859](https://github.com/ray-project/ray/pull/60859) | Data | ✅ | 支持将 tensor 写入 TFRecords |
| 103 | `feca47613b` | [#61064](https://github.com/ray-project/ray/pull/61064) | Data | ✅ | Revert task state 获取变更 |
| 104 | `f645d591d6` | [#60953](https://github.com/ray-project/ray/pull/60953) | Serve | — | 测试修复：Fix flaky test_replica_metrics_fields in test_metrics_haproxy |
| 105 | `26702b7dfd` | [#61068](https://github.com/ray-project/ray/pull/61068) | Serve | — | 测试修复：[CI] Deflake `test_deploy_app_2.py::test_num_replicas_auto_basic` |
| 106 | `db045c49ac` | [#60955](https://github.com/ray-project/ray/pull/60955) | Serve | — | 将 haproxy.py 中的 fcntl import 移到函数作用域，兼容 Windows |
| 107 | `569d681256` | [#60892](https://github.com/ray-project/ray/pull/60892) | Serve | — | 使 test_replica_utilization_metric 确定性运行 |
| 108 | `8882b9045a` | [#60993](https://github.com/ray-project/ray/pull/60993) | Data | — | test_json/test_file_based_datasource (#60993) |
| 109 | `627919071f` | [#61048](https://github.com/ray-project/ray/pull/61048) | Data | — | Fix test_runtime_metrics to exclude Scheduling from time comparison (#61048) |
| 110 | `ffce98e21d` | [#61041](https://github.com/ray-project/ray/pull/61041) | Data | — | Add regression test to ensure we don't double count union resources (#61041) |
| 111 | `871bc89176` | [#61020](https://github.com/ray-project/ray/pull/61020) | Data | ✅ | LogicalOperator 名称默认使用类名 |
| 112 | `e00c9a4a14` | [#61007](https://github.com/ray-project/ray/pull/61007) | Data | ✅ | `ResourceUtilizationGauge` 添加逻辑内存指标 |
| 113 | `094e0a1b24` | [#61010](https://github.com/ray-project/ray/pull/61010) | Data | ✅ | 添加 `get_max_task_capacity` 工具函数 |
| 114 | `7a3bbe7166` | [#61092](https://github.com/ray-project/ray/pull/61092) | Serve | — | `ray_serve_deployment_error_counter_total` 指标添加 `exception_type` 标签 |
| 115 | `f5155f28bb` | [#60980](https://github.com/ray-project/ray/pull/60980) | Core | ✅ | 改进 dashboard event publisher 错误信息 |
| 116 | `04a10c65d7` | [#61026](https://github.com/ray-project/ray/pull/61026) | Data | ✅ | 调整聚合和消费测试大小 |
| 117 | `2a4a5c47ce` | [#61044](https://github.com/ray-project/ray/pull/61044) | Data | ✅ | 移除 `locality_with_output` |
| 118 | `4e34398229` | [#60000](https://github.com/ray-project/ray/pull/60000) | Core | ✅ | **禁用 memory_full_info，用 memory_info 近似 USS**，减少系统调用开销 |
| 119 | `1bd59fe409` | [#58364](https://github.com/ray-project/ray/pull/58364) | Data | ✅ | **修复 `_align_struct_fields` 在标量字段不对齐时失败** |
| 120 | `f1a1039a89` | [#60973](https://github.com/ray-project/ray/pull/60973) | Data | — | Fix flaky test_parquet_read_random_shuffle (#60973) |
| 121 | `b434ecffcb` | [#61034](https://github.com/ray-project/ray/pull/61034) | Core | — | fix set/get env races caused by `OtlpGrpcMetricExporterOptions` (#61034) |
| 122 | `fe0950e7c1` | [#61122](https://github.com/ray-project/ray/pull/61122) | Core | — | Remove old nixl deduplication logic (#61122) |
| 123 | `ef0d3fdce0` | [#61062](https://github.com/ray-project/ray/pull/61062) | Data | ✅ | 升级 pyiceberg 到 0.11.0 |
| 124 | `18d0644491` | [#60943](https://github.com/ray-project/ray/pull/60943) | Serve | — | 在 test_grpc 中使用 get_application_url |
| 125 | `95049c5854` | [#61120](https://github.com/ray-project/ray/pull/61120) | Serve | — | **修复 HAProxy 配置文件竞态条件和 draining guard** |
| 126 | `425e85eb70` | [#61031](https://github.com/ray-project/ray/pull/61031) | Data | ✅ | `_ActorPool` 类添加 actor 名称 |
| 127 | `05c23a3b6e` | [#60996](https://github.com/ray-project/ray/pull/60996) | Data | ✅ | 引入 `ExecutionCache` 实现流式缓存 |
| 128 | `2ff2b961d8` | [#61146](https://github.com/ray-project/ray/pull/61146) | Data | ✅ | 增加 `with_column` 超时时间 |
| 129 | `d39938f275` | [#59368](https://github.com/ray-project/ray/pull/59368) | Core | ✅ | 资源隔离 [3/n]：内存监控器的 OS 特定编译 |
| 130 | `83d91e1e53` | [#60999](https://github.com/ray-project/ray/pull/60999) | Core | — | Implement pytorch storage block caching (#60999) |
| 131 | `66b2c8b415` | [#61130](https://github.com/ray-project/ray/pull/61130) | Data | ✅ | 启用 GPU 阶段自动扩缩容 |
| 132 | `5606ab7a55` | [#61151](https://github.com/ray-project/ray/pull/61151) | Data | ✅ | 添加 `locality_with_output` 替代方案说明 |
| 133 | `88ac9262bb` | [#60761](https://github.com/ray-project/ray/pull/60761) | Core | ✅ | 恢复 per-node 临时目录支持（重新提交 #57735） |
| 134 | `3a187a5bde` | [#61079](https://github.com/ray-project/ray/pull/61079) | Data | — | Remove accidental __init__.py from data test directory (#61079) |
| 135 | `265642fcf1` | [#61140](https://github.com/ray-project/ray/pull/61140) | Data | ✅ | 移除 `locality_with_output` 残余代码 |
| 136 | `cb9afe9359` | [#61135](https://github.com/ray-project/ray/pull/61135) | Serve | — | 使用环境变量控制默认 HTTP host |
| 137 | `bfd24f009f` | [#59365](https://github.com/ray-project/ray/pull/59365) | Core | ✅ | 资源隔离 [4/n]：killing policy 的 OS 特定编译 |
| 138 | `224b70a699` | [#61194](https://github.com/ray-project/ray/pull/61194) | Serve | — | 测试修复：classify test_fastapi as large |
| 139 | `f5ee9e0c22` | [#61202](https://github.com/ray-project/ray/pull/61202) | Serve | — | 增加 test_cli 测试超时时间 |
| 140 | `aa00b3dac1` | [#61213](https://github.com/ray-project/ray/pull/61213) | Data | ✅ | 整合 Schema 推断逻辑 |
| 141 | `c7944e6e59` | [#61089](https://github.com/ray-project/ray/pull/61089) | Serve | — | gRPC inter-deployment 模式传播追踪上下文 |
| 142 | `777f37f002` | [#61186](https://github.com/ray-project/ray/pull/61186) | Serve | — | 添加 HAProxy 单元测试到 CI |
| 143 | `8efa629622` | [#61147](https://github.com/ray-project/ray/pull/61147) | Core | ✅ | 防止 protobuf 符号 `_upb_Arena_SlowMalloc` 泄漏 |
| 144 | `e12b07eff5` | [#61033](https://github.com/ray-project/ray/pull/61033) | Core | ✅ | 修复 dashboard list_jobs API 中 dataclass.asdict 空值处理 |
| 145 | `c054e6d5dc` | [#61221](https://github.com/ray-project/ray/pull/61221) | Data | ✅ | 算子融合测试改为 medium |
| 146 | `1d6bbf353b` | [#61185](https://github.com/ray-project/ray/pull/61185) | Core | ✅ | **修复 dashboard node_head API 死节点缓存**（master 已有内部版本 c162f7a870） |
| 147 | `e2f0385bf6` | [#60831](https://github.com/ray-project/ray/pull/60831) | Serve | — | 移除 deployment state 更新循环中冗余的 `check_curr_status()` 调用 |
| 148 | `7b032ac531` | [#58892](https://github.com/ray-project/ray/pull/58892) | Serve | — | **Cython 实现自动扩缩指标聚合**，提升性能 |
| 149 | `57f5140eea` | [#61165](https://github.com/ray-project/ray/pull/61165) | Data | ✅ | TurbopufferDatasink 支持 region 或 base_url |
| 150 | `a63be4c7da` | [#61097](https://github.com/ray-project/ray/pull/61097) | Core | ✅ | 资源隔离 [5/n]：添加额外的 cgroup 约束 |
| 151 | `35b297fd4b` | [#60295](https://github.com/ray-project/ray/pull/60295) | Data | ✅ | `StreamingRepartition` 支持 `strict=False` 模式 |
| 152 | `8915d371c5` | [#61210](https://github.com/ray-project/ray/pull/61210) | Core | ✅ | 资源隔离 [6/n]：更新 killing policy 接口支持多 worker 策略 |
| 153 | `9e1fa2e28c` | [#61229](https://github.com/ray-project/ray/pull/61229) | Serve | — | 默认启用 `RAY_SERVE_RUN_SYNC_IN_THREADPOOL=1` |
| 154 | `ce3facf150` | [#60406](https://github.com/ray-project/ray/pull/60406) | Data | ✅ | 限制 pandas<3 并使 SettingWithCopyWarning 兼容 pandas 3 |
| 155 | `34699a557f` | [#60274](https://github.com/ray-project/ray/pull/60274) | Data | ✅ | `train_test_split` 避免冗余读取 |
| 156 | `96ae3a5e65` | [#61276](https://github.com/ray-project/ray/pull/61276) | Serve | — | 修复 test_controller 测试 |
| 157 | `c11c103fa4` | [#61208](https://github.com/ray-project/ray/pull/61208) | Data | ✅ | **修复多输入算子 object store 内存归属的双重计数** |
| 158 | `bee13efc27` | [#61234](https://github.com/ray-project/ray/pull/61234) | Data | — | Remove redundant tests from test_binary.py (#61234) |
| 159 | `67503001c7` | [#60480](https://github.com/ray-project/ray/pull/60480) | Data | ✅ | 简化执行回调生命周期 (Diff #1) |
| 160 | `58fcebd6d5` | [#61282](https://github.com/ray-project/ray/pull/61282) | Data | ✅ | 修复 `DatabricksUCDatasource` schema 属性被 schema() 方法遮蔽 |
| 161 | `32945ff30a` | [#61107](https://github.com/ray-project/ray/pull/61107) | Data | ✅ | 将 `output_dependencies` 职责移到 `PhysicalOperator` |
| 162 | `9a0ae1b908` | [#60753](https://github.com/ray-project/ray/pull/60753) | Core | ✅ | 添加 Nvidia B300 GPU 支持 |
| 163 | `09c7c7626a` | [#61028](https://github.com/ray-project/ray/pull/61028) | Data | ✅ | 重命名 encoder preprocessor 私有字段 |
| 164 | `3956d0d7db` | [#61150](https://github.com/ray-project/ray/pull/61150) | Data | ✅ | 执行开始时记录 DataContext 配置日志 |
| 165 | `a35fe52062` | [#61081](https://github.com/ray-project/ray/pull/61081) | Core | ✅ | RDT：支持非 torch tensor 对象的 RDT 传输 |
| 166 | `817204adbe` | [#61126](https://github.com/ray-project/ray/pull/61126) | Data | ✅ | `read_*` 函数支持 `pathlib.Path` |
| 167 | `34f1c6dd01` | [#61235](https://github.com/ray-project/ray/pull/61235) | Data | — | Remove redundant tests from test_numpy.py (#61235) |
| 168 | `f8d5149ba8` | [#61280](https://github.com/ray-project/ray/pull/61280) | Core | ✅ | RDT：拆分序列化并使其线程安全 |
| 169 | `50b981d2a4` | [#61230](https://github.com/ray-project/ray/pull/61230) | Serve | — | 为 Ray Serve 添加追踪 (tracing) 支持 |
| 170 | `8c2650aec0` | [#61134](https://github.com/ray-project/ray/pull/61134) | Core | — | fix long_running_many_drivers.aws release test (#61134) |
| 171 | `838a47dcc2` | [#61196](https://github.com/ray-project/ray/pull/61196) | Data | — | Separate unit tests from integration-heavy tests - 1 (#61196) |
| 172 | `b24f6e6e17` | [#61297](https://github.com/ray-project/ray/pull/61297) | Core | ✅ | 资源隔离 [7/n]：将公共 killing policy 辅助函数移到 util |
| 173 | `ba137af6dd` | [#61294](https://github.com/ray-project/ray/pull/61294) | Core | ✅ | 使用 bazel param file 避免 Windows 命令行长度限制 |
| 174 | `980a971ce4` | [#61098](https://github.com/ray-project/ray/pull/61098) | Data | — | Add custom tokenizer example (#61098) |
| 175 | `05a40c66a1` | [#61310](https://github.com/ray-project/ray/pull/61310) | Serve | — | Direct ingress 优化和常量重排 |
| 176 | `5ac4604cdf` | [#61293](https://github.com/ray-project/ray/pull/61293) | Data | ✅ | DataContext 持有执行回调类而非实例 |
| 177 | `2745feb86c` | [#61205](https://github.com/ray-project/ray/pull/61205) | Serve | — | Gang Scheduling 验证和工具函数 [2/n] |
| 178 | `d2233876e8` | [#61335](https://github.com/ray-project/ray/pull/61335) | Serve | — | 回退之前的变更 |
| 179 | `fba8656def` | [#60840](https://github.com/ray-project/ray/pull/60840) | Serve | — | 通过 dirty flag 跳过 DeploymentState 稳态下的每 tick 工作 |
| 180 | `1f45462bd8` | [#61308](https://github.com/ray-project/ray/pull/61308) | Data | ✅ | LogicalOperator 改为 ABC 并添加抽象 `num_outputs` |
| 181 | `df8039e310` | [#61367](https://github.com/ray-project/ray/pull/61367) | Data | — | - Skip downloading MNIST dataset + Update test_per_input_inqueue_attribution_for_union (#61367) |
| 182 | `c69f1c0101` | [#61369](https://github.com/ray-project/ray/pull/61369) | Serve | — | 回退之前的变更 |
| 183 | `0906625f0d` | [#61192](https://github.com/ray-project/ray/pull/61192) | Data | ✅ | 添加任务调度时间和输出反压追踪指标 |
| 184 | `1b85bf7530` | [#61288](https://github.com/ray-project/ray/pull/61288) | Data | ✅ | **防止聚合任务调度到 head 节点**，减少 head 负载 |
| 185 | `9f06d54c12` | [#61246](https://github.com/ray-project/ray/pull/61246) | Core | ✅ | 修复 `WorkerPool::WarnAboutSize()` 中的双重计数 |
| 186 | `1e6e66d070` | [#61273](https://github.com/ray-project/ray/pull/61273) | Data | ✅ | 添加基于速率计算分配量的工具函数 |
| 187 | `d85ed28925` | [#61195](https://github.com/ray-project/ray/pull/61195) | Core | — | upgrade grpc to 1.58.0 to fix getenv races (#61195) |
| 188 | `d4833c03b1` | [#61272](https://github.com/ray-project/ray/pull/61272) | Core | — | add timeout to prometheus test utils (#61272) |
| 189 | `b78afa90fc` | [#61206](https://github.com/ray-project/ray/pull/61206) | Serve | — | Gang Scheduling 核心调度引擎 [3/n] |
| 190 | `a5ca653723` | [#61368](https://github.com/ray-project/ray/pull/61368) | Serve | — | 测试修复：Add a micro benchmark for serve controller |
| 191 | `a6faf23e6f` | [#61094](https://github.com/ray-project/ray/pull/61094) | Core | ✅ | RDT：设置 RDT ref 的接收缓冲区 |
| 192 | `dc28b1c946` | [#61247](https://github.com/ray-project/ray/pull/61247) | Core | — | deflake test_ray_timeline (#61247) |
| 193 | `3b1362841f` | [#61298](https://github.com/ray-project/ray/pull/61298) | Core | ✅ | 收紧 ray export 符号白名单防止非 ray 符号泄漏 |
| 194 | `a29155c868` | [#59633](https://github.com/ray-project/ray/pull/59633) | Data | ✅ | `read_datasource()` 支持 `compute` 参数和 `ActorPoolStrategy` |
| 195 | `371a3613ac` | [#61004](https://github.com/ray-project/ray/pull/61004) | Core | ✅ | 调度速率限制时输出告警，帮助排查任务启动慢 |
| 196 | `c73d04e1a7` | [#61281](https://github.com/ray-project/ray/pull/61281) | Core | ✅ | **同步等待 metrics exporter 初始化，避免 getenv/setenv 竞态** |
| 197 | `29041ecd41` | [#61382](https://github.com/ray-project/ray/pull/61382) | Core | ✅ | autoscaler 允许 ALLOCATION_TIMEOUT → TERMINATED 状态转换 |
| 198 | `f654eee832` | [#61353](https://github.com/ray-project/ray/pull/61353) | Core | ✅ | **优化 worker listener 线程**，减少调度延迟 |
| 199 | `ad64f26b00` | [#61397](https://github.com/ray-project/ray/pull/61397) | Serve | — | 测试修复：Add a autoscaling test near cluster capacity |
| 200 | `0ef911f5e2` | [#61396](https://github.com/ray-project/ray/pull/61396) | Serve | — | 修复客户端断开后成功响应被误分类为 499 |
| 201 | `436ef85554` | [#60770](https://github.com/ray-project/ray/pull/60770) | Serve | — | gRPC proxy 客户端流实现 [4/n] |
| 202 | `85dc75ce42` | [#61284](https://github.com/ray-project/ray/pull/61284) | Data | ✅ | Kafka 库迁移到 confluent-kafka |
| 203 | `672e9fb0b1` | [#60662](https://github.com/ray-project/ray/pull/60662) | Data | — | Add TPCH queries 7,8,9 for benchmarking (#60662) |
| 204 | `d0e096f813` | [#61423](https://github.com/ray-project/ray/pull/61423) | Data | — | Fix read_datasource test to use public compute attribute (#61423) |
| 205 | `edefe43825` | [#61441](https://github.com/ray-project/ray/pull/61441) | Serve | — | 测试修复：Skip tracing tests for windows |
| 206 | `544a40fb3a` | [#61449](https://github.com/ray-project/ray/pull/61449) | Core | ✅ | Revert gRPC 升级到 1.58.0（与 #61499 配合） |
| 207 | `80cc4bda39` | [#61380](https://github.com/ray-project/ray/pull/61380) | Data | ✅ | 修复不清晰的元数据警告和错误的算子名称日志 |
| 208 | `68aef137d7` | [#61303](https://github.com/ray-project/ray/pull/61303) | Data | — | Separate test_arrow_block.py with integration and unit tests (#61303) |
| 209 | `50ca506ab8` | [#60504](https://github.com/ray-project/ray/pull/60504) | Dashboard | ✅ | Dashboard 支持 autoscaler v2 集群级节点指标 |
| 210 | `20925a36a2` | [#61357](https://github.com/ray-project/ray/pull/61357) | Build | — | Supports repeated execution of setup-dev.py (#61357) |
| 211 | `e8f0b5031a` | [#61437](https://github.com/ray-project/ray/pull/61437) | Data | ✅ | Kafka 数据源用 `consume()` 替代 `poll()` |
| 212 | `9e709d1b90` | [#61249](https://github.com/ray-project/ray/pull/61249) | Serve+LLM | — | 将 LLM API 提升为 beta |
| 213 | `6ddbbdd00f` | [#59879](https://github.com/ray-project/ray/pull/59879) | Data | ✅ | 表达式操作添加 map namespace 支持 |
| 214 | `4356f0fe0a` | [#61364](https://github.com/ray-project/ray/pull/61364) | Data | ✅ | 一对一逻辑算子转为 frozen dataclass |
| 215 | `d4e007485f` | [#61376](https://github.com/ray-project/ray/pull/61376) | Data | ✅ | **修复 `read_parquet` 对版本化对象存储 URI 的文件扩展名过滤** |
| 216 | `48fd82584d` | [#61137](https://github.com/ray-project/ray/pull/61137) | Core | — | Add Windows and macOS smoke tests to premerge (#61137) |
| 217 | `8fa34b9f34` | [#61207](https://github.com/ray-project/ray/pull/61207) | Serve | — | Gang Scheduling 容错 [4/n] |
| 218 | `62da761393` | [#61478](https://github.com/ray-project/ray/pull/61478) | Core | ✅ | 修复 `TaskLifecycleEvent.node_id` 填充了发送节点而非执行节点 |
| 219 | `2e70e0dbe5` | [#61042](https://github.com/ray-project/ray/pull/61042) | Build | — | feat: add top-level build-image.sh (#61042) |
| 220 | `9e9c8cb3bb` | [#61468](https://github.com/ray-project/ray/pull/61468) | Serve | — | 添加 `RAY_SERVE_HAPROXY_TCP_NODELAY` 环境变量 |
| 221 | `b9cbe1f9d3` | [#61341](https://github.com/ray-project/ray/pull/61341) | Data | ✅ | 所有 Preprocessor 实现 `SerializablePreprocessorBase` |
| 222 | `5723d4be22` | [#60689](https://github.com/ray-project/ray/pull/60689) | Core | — | Support send and receive side metadata caching via cache_memory_registration (#60689) |
| 223 | `667e99c7aa` | [#61180](https://github.com/ray-project/ray/pull/61180) | Serve | — | 支持 fallback Serve proxy [1/n] |
| 224 | `941d4e0753` | [#61436](https://github.com/ray-project/ray/pull/61436) | Data | ✅ | 添加 `ClusterUtil` 数据类 |
| 225 | `c190c7fe2e` | [#61082](https://github.com/ray-project/ray/pull/61082) | Core | ✅ | **按并发组而非全局顺序排列有序 actor 任务**，修复并发组间阻塞 |
| 226 | `5d3a8a3a58` | [#61476](https://github.com/ray-project/ray/pull/61476) | Data | ✅ | 移除 Kafka 默认 task 超时并钳制 `end_offset` 到 watermark |
| 227 | `556e2065f6` | [#61451](https://github.com/ray-project/ray/pull/61451) | Serve | — | 为 gRPC 客户端/双向流路径添加缺失的追踪属性 |
| 228 | `4e86649a0c` | [#61215](https://github.com/ray-project/ray/pull/61215) | Serve | — | Gang Scheduling 缩容 [5/n] |
| 229 | `462e4c9b35` | [#61490](https://github.com/ray-project/ray/pull/61490) | Serve | — | **修复 @serve.ingress 包装器中 async __init__ 被静默丢弃** |
| 230 | `5270e8ce06` | [#61491](https://github.com/ray-project/ray/pull/61491) | Serve | — | 测试修复：Add unit tests for NodePortManager |
| 231 | `476bfa8ff6` | [#60449](https://github.com/ray-project/ray/pull/60449) | Core | ✅ | 在 one-event 框架中支持 placement group 事件 |
| 232 | `8212929d8b` | [#61326](https://github.com/ray-project/ray/pull/61326) | Core | ✅ | RDT 代码中引用名称更新 |
| 233 | `645d4f17a4` | [#61061](https://github.com/ray-project/ray/pull/61061) | Serve | — | Ray Serve Pydantic v2 迁移 |
| 234 | `94944d79a6` | [#61232](https://github.com/ray-project/ray/pull/61232) | Core | ✅ | state manager 获取节点信息时消除 Python GCS client 依赖 |
| 235 | `598ca8d39b` | [#61428](https://github.com/ray-project/ray/pull/61428) | Data | ✅ | DataContext 以 JSON 格式打印 |
| 236 | `e9dd5e3d63` | [#61577](https://github.com/ray-project/ray/pull/61577) | Data | ✅ | 放宽 `concurrency_solver.py` 断言 |
| 237 | `b96eb4578a` | [#61578](https://github.com/ray-project/ray/pull/61578) | Data | ✅ | `FakeAutoscalingCoordinator` 空请求时返回指定资源 |
| 238 | `5acbcf7753` | [#60667](https://github.com/ray-project/ray/pull/60667) | Data | — | Add TPCH queries 3, 10, and 18 for benchmarking (#60667) |
| 239 | `17fbe52557` | [#61469](https://github.com/ray-project/ray/pull/61469) | Core | ✅ | Actor 定义 OneEvent 中添加 labels 作为 ref ID，关联 actor 和 Data 算子 |
| 240 | `4d7f46c84f` | [#61344](https://github.com/ray-project/ray/pull/61344) | Core+Data | ✅ | ActorPoolMapOperator 任务中添加 data operator ID |
| 241 | `39e2484d44` | [#61580](https://github.com/ray-project/ray/pull/61580) | Data | ✅ | `TimeWindowAverageCalculator` 浮点误差缓解 |
| 242 | `55227f948c` | [#61581](https://github.com/ray-project/ray/pull/61581) | Data | ✅ | 修复 streaming_train_test_split doctest |
| 243 | `997fbe1fad` | [#61557](https://github.com/ray-project/ray/pull/61557) | Serve | — | 回退之前的变更 |
| 244 | `9577b673ba` | [#61540](https://github.com/ray-project/ray/pull/61540) | Data | ✅ | Parquet 读取跳过 `_SUCCESS` 文件 |
| 245 | `15a4734540` | [#61572](https://github.com/ray-project/ray/pull/61572) | Data | ✅ | 移除 `_validate_dag` 死代码 |
| 246 | `7561c2a60e` | [#61579](https://github.com/ray-project/ray/pull/61579) | Core | ✅ | RDT：上游任务出错时避免 RDT 阻塞 |
| 247 | `95b4042422` | [#61518](https://github.com/ray-project/ray/pull/61518) | Core | ✅ | 修复 GCS pubsub 中 `publisher_id` 类型不匹配 |
| 248 | `d521f94d44` | [#56284](https://github.com/ray-project/ray/pull/56284) | Data | ✅ | 支持 Arrow 原生定长 tensor 类型 |
| 249 | `41ddcb7044` | [#61515](https://github.com/ray-project/ray/pull/61515) | Serve | — | 前一个自动扩缩指标未完成时跳过发送新指标 |
| 250 | `75ea85b7bb` | [#61148](https://github.com/ray-project/ray/pull/61148) | Core | ✅ | 添加基于二次直方图的 Percentile 指标类型 |
| 251 | `66949f9184` | [#61605](https://github.com/ray-project/ray/pull/61605) | Data | ✅ | 防止算子独占共享 object store 预算（后被 revert） |
| 252 | `56599092ea` | [#61315](https://github.com/ray-project/ray/pull/61315) | Data | ✅ | **优化 concat tables 的快速路径** |
| 253 | `6cf5425772` | [#61404](https://github.com/ray-project/ray/pull/61404) | Data | ✅ | 添加 iceberg upsert benchmark 测试 |
| 254 | `d7b11d1187` | [#61467](https://github.com/ray-project/ray/pull/61467) | Serve | — | Gang Scheduling 自动扩缩 [6/n] |
| 255 | `d5391f90f0` | [#61220](https://github.com/ray-project/ray/pull/61220) | Data | ✅ | DataSourceV2 文件列举基础设施 [1/n] |
| 256 | `b69f225e2e` | [#61543](https://github.com/ray-project/ray/pull/61543) | Data | ✅ | 滚动利用率均值钳制到零 |
| 257 | `be18bcd559` | [#61323](https://github.com/ray-project/ray/pull/61323) | Core | ✅ | 资源隔离 [8/n]：添加基于时间的 worker killing policy |
| 258 | `2208e78e5b` | [#61473](https://github.com/ray-project/ray/pull/61473) | Core | ✅ | 允许 `worker_process_setup_hook` 在 re-entry 时匹配 |
| 259 | `8af3b3ebeb` | [#60860](https://github.com/ray-project/ray/pull/60860) | Core | ✅ | **autoscaler 在 head 重启时从 WrongClusterID 恢复** |
| 260 | `18b6eb7401` | [#61190](https://github.com/ray-project/ray/pull/61190) | Data | ✅ | 清理 `SplitCoordinator` |
| 261 | `1a16658a24` | [#61559](https://github.com/ray-project/ray/pull/61559) | Core | ✅ | 修复 `TASK_PROFILE_EVENT` 多阶段聚合 |
| 262 | `fe0d28c211` | [#61639](https://github.com/ray-project/ray/pull/61639) | Serve | — | Deployment-scoped actor 配置和 schema [1/n] |
| 263 | `a3e481b803` | [#61506](https://github.com/ray-project/ray/pull/61506) | Core | — | fix test_placement_group_3_client_mode by changing url parser (#61506) |
| 264 | `7cf8f37cb2` | [#61199](https://github.com/ray-project/ray/pull/61199) | Data | ✅ | 提取 Unity Catalog 凭证解析为独立函数 |
| 265 | `dee740298e` | [#61271](https://github.com/ray-project/ray/pull/61271) | Serve | — | HAProxy 使用 fallback server [2/n] |
| 266 | `18474d9ddb` | [#61653](https://github.com/ray-project/ray/pull/61653) | Data | — | Make text embedding release test using cross-AZ scaling (#61653) |
| 267 | `680b386f9c` | [#61648](https://github.com/ray-project/ray/pull/61648) | Serve | — | Deployment-scoped actor 构建/部署流程 [2/n] |
| 268 | `5d6c40aba8` | [#61658](https://github.com/ray-project/ray/pull/61658) | Data | ✅ | 重命名 `concurrency_solver.py` 为 `throughput_solver.py` |
| 269 | `aabe458342` | [#61617](https://github.com/ray-project/ray/pull/61617) | Data | ✅ | RD PyArrow Compute Func 转 PyArrow Expr 实现谓词下推 |
| 270 | `a20d303c51` | [#61499](https://github.com/ray-project/ray/pull/61499) | Core | ✅ | **升级 gRPC 到 v1.58.0** |
| 271 | `b2f25a6f88` | [#61047](https://github.com/ray-project/ray/pull/61047) | Data | — | Relax checkpoint recovery test to allow at-least-once semantics (#61047) |
| 272 | `1972790222` | [#61539](https://github.com/ray-project/ray/pull/61539) | Core | ✅ | 修复 ObjectManager::PushObjectInternal 日志字段 |
| 273 | `befa5b8863` | [#61418](https://github.com/ray-project/ray/pull/61418) | Data | ✅ | 改进 table aggregate 函数 |
| 274 | `63bc2648a2` | [#61329](https://github.com/ray-project/ray/pull/61329) | Data | ✅ | 添加 `cudf` 作为 batch_format |
| 275 | `d334cd6536` | [#61300](https://github.com/ray-project/ray/pull/61300) | Core | ✅ | 添加 TPU 多主机 slice 就绪检测工具 |
| 276 | `4b4ab0125d` | [#61618](https://github.com/ray-project/ray/pull/61618) | Core | ✅ | 确保 `Node._node_labels` 在 `connect_only` 时也初始化 |
| 277 | `51e923d091` | [#61363](https://github.com/ray-project/ray/pull/61363) | Core | ✅ | serve import 从 `_private` 迁移到 `_common` |
| 278 | `1a6c6f0529` | [#60659](https://github.com/ray-project/ray/pull/60659) | Core | ✅ | 在 `TaskInfoEntry` 和 `ActorTableData` 中暴露 `fallback_strategy` |
| 279 | `02d61afb3c` | [#61059](https://github.com/ray-project/ray/pull/61059) | Data | ✅ | `RefBundle` 等类改为 properly frozen |
| 280 | `09cacbbd13` | [#61667](https://github.com/ray-project/ray/pull/61667) | Build | — | Make -undefined dynamic_lookup explicit in _raylet.so macOS linkopts (#61667) |
| 281 | `6d34c30fed` | [#61670](https://github.com/ray-project/ray/pull/61670) | Data | ✅ | `get_parquet_dataset` 可配置扫描 fragment 数量 |
| 282 | `03902bbf82` | [#61663](https://github.com/ray-project/ray/pull/61663) | Core | — | Update memory pressure test to take into account log changes (#61663) |
| 283 | `4447e6a81f` | [#61668](https://github.com/ray-project/ray/pull/61668) | Build | — | Add java_binary all_tests_bin as deploy-jar source for Java tests (#61668) |
| 284 | `f1821c1bb2` | [#61385](https://github.com/ray-project/ray/pull/61385) | Data | ✅ | 精简 `DefaultActorPoolAutoscaler` |
| 285 | `91eb738eb2` | [#61729](https://github.com/ray-project/ray/pull/61729) | Data | ✅ | Revert [#61605](https://github.com/ray-project/ray/pull/61605) |
| 286 | `ae7ad50cc3` | [#61374](https://github.com/ray-project/ray/pull/61374) | Core | — | Optimize try_schedule by cacheing unavailable shapes (#61374) |
| 287 | `495220a55c` | [#61731](https://github.com/ray-project/ray/pull/61731) | Serve | — | **修复自动扩缩反馈循环导致扩容到 max_replicas** |
| 288 | `af9ae6ae19` | [#61669](https://github.com/ray-project/ray/pull/61669) | Build | — | Replace rules_boost lzma patches with a custom org_lzma_lzma build file (#61669) |
| 289 | `1a98a597ee` | [#61654](https://github.com/ray-project/ray/pull/61654) | Core | ✅ | IPPR [1/N]：为 GcsAutoscalerStateManager 添加 ResizeRayletResourceInstances |
| 290 | `e61f0df9d0` | [#61603](https://github.com/ray-project/ray/pull/61603) | Serve | — | 控制循环中节流应用状态 gauge 上报 |
| 291 | `0241874630` | [#61244](https://github.com/ray-project/ray/pull/61244) | Core | — | fix misleading comment in DefaultModelConfig (#61244) |
| 292 | `68a622a6c3` | [#60967](https://github.com/ray-project/ray/pull/60967) | Serve | — | HTTP 访问日志中添加客户端 IP 地址 |
| 293 | `a0750952b1` | [#61694](https://github.com/ray-project/ray/pull/61694) | Build | — | Patch protobuf for Bazel 7 exec_tools removal (#61694) |
| 294 | `2a5a4465c4` | [#61695](https://github.com/ray-project/ray/pull/61695) | Build | — | Upgrade rules_apple/apple_support and set macOS toolchain flags for Bazel 7 (#61695) |
| 295 | `4902739acd` | [#60330](https://github.com/ray-project/ray/pull/60330) | Core | ✅ | **OOM Killer 优先杀死占用大内存的 Worker** |
| 296 | `ade772d6ad` | [#61758](https://github.com/ray-project/ray/pull/61758) | Serve | — | **修复链式 ActorDiedError 在路由器中的归因错误**，添加 actor 故障测试覆盖 |
| 297 | `58efa8677c` | [#61727](https://github.com/ray-project/ray/pull/61727) | Serve | — | 测试修复：Stabilize gang scheduling tests |
| 298 | `b5b334fef1` | [#61507](https://github.com/ray-project/ray/pull/61507) | Data | ✅ | 修复 shuffle 空 block 场景下链式左连接的 ColumnNotFound 错误 |
| 299 | `2b6ee48745` | [#61717](https://github.com/ray-project/ray/pull/61717) | Serve | — | 测试修复：deflake test metrics |
| 300 | `b86462c16e` | [#61774](https://github.com/ray-project/ray/pull/61774) | Data | ✅ | **修复 ref_bundle + input_files 双重计数** |
| 301 | `f5d37e3e84` | [#61732](https://github.com/ray-project/ray/pull/61732) | Core | ✅ | 只读 provider 时抑制 autoscaler 操作日志 |
| 302 | `d269f5b828` | [#61666](https://github.com/ray-project/ray/pull/61666) | Core | ✅ | IPPR [2/N]：GCS Cython 客户端添加 `resize_raylet_resource_instances` |
| 303 | `34593f1f13` | [#61776](https://github.com/ray-project/ray/pull/61776) | Serve | — | 测试修复：increase timeout of test_custom_autoscaling_metrics |
| 304 | `6a43c013c6` | [#61755](https://github.com/ray-project/ray/pull/61755) | Serve | — | **修复 P99 延迟回归**：请求完成时递减队列长度缓存，减少 replica 更新开销 |
| 305 | `cd35875bc5` | [#61216](https://github.com/ray-project/ray/pull/61216) | Serve | — | Gang Scheduling 滚动更新 [7/n] |
| 306 | `770dca31ba` | [#61371](https://github.com/ray-project/ray/pull/61371) | Data | ✅ | 支持 GPU shuffle |
| 307 | `54f7a674df` | [#60807](https://github.com/ray-project/ray/pull/60807) | Serve | — | 整理 Ray Serve 环境变量目录 [6/n] |
| 308 | `f27365047f` | [#61275](https://github.com/ray-project/ray/pull/61275) | Data | — | Remove redundant tests from test_image.py (#61275) |
| 309 | `281c2cdf29` | [#60693](https://github.com/ray-project/ray/pull/60693) | Core | ✅ | 重构 GLOO collective groups 重复 rendezvous 逻辑 |
| 310 | `9b1c4ffb61` | [#61722](https://github.com/ray-project/ray/pull/61722) | Build | — | Replace ray-images.yaml with ray-images.json (#61722) |
| 311 | `895065b2f0` | [#61657](https://github.com/ray-project/ray/pull/61657) | Data | — | Install mongodb-org from official repo, start mongod directly in tests (#61657) |
| 312 | `2b63ed0d11` | [#61807](https://github.com/ray-project/ray/pull/61807) | Core | ✅ | 禁用 compiled graph flaky 测试 |
| 313 | `217c088357` | [#61815](https://github.com/ray-project/ray/pull/61815) | Data | ✅ | 统一 actor pool 状态与分配逻辑 |
| 314 | `6fe7402442` | [#61818](https://github.com/ray-project/ray/pull/61818) | Serve | — | **确保 deployment 在异常后收敛到 healthy 状态** |
| 315 | `9642fc01bb` | [#61697](https://github.com/ray-project/ray/pull/61697) | Data | ✅ | 重构 hanging detector 避免深层嵌套 [1/N] |
| 316 | `d294b9d328` | [#61481](https://github.com/ray-project/ray/pull/61481) | Data | ✅ | Map 逻辑算子转为 frozen dataclass |
| 317 | `5ca80bb6b0` | [#59662](https://github.com/ray-project/ray/pull/59662) | Data | ✅ | 改进随机 API 可重现性 |
| 318 | `223ce530ae` | [#61830](https://github.com/ray-project/ray/pull/61830) | Serve | — | 测试修复：Fix flaky direct ingress port tests |
| 319 | `23cb3728a0` | [#61831](https://github.com/ray-project/ray/pull/61831) | Serve | — | 测试修复：Fix flaky test_proxy_metrics_internal_error in HAProxy metrics tests |
| 320 | `97618e6117` | [#61829](https://github.com/ray-project/ray/pull/61829) | Serve | — | 测试修复：Fix flaky test_http_backpressure by removing Ray worker latency |
| 321 | `4ce100c866` | [#61843](https://github.com/ray-project/ray/pull/61843) | Data | ✅ | 添加 `refresh_state` 接口 |
| 322 | `8b561dde6e` | [#61483](https://github.com/ray-project/ray/pull/61483) | Data | ✅ | 移除过时的 PyArrow 9.0 版本检查 |
| 323 | `7bebb5f7b9` | [#61403](https://github.com/ray-project/ray/pull/61403) | Serve | — | 时间序列聚合中排除早期不完整周期 |
| 324 | `ad47d5725c` | [#61065](https://github.com/ray-project/ray/pull/61065) | Core | ✅ | **缓存 `find_gcs_addresses`，避免 head 节点重复扫描进程列表** |
| 325 | `1a0eeaf477` | [#61083](https://github.com/ray-project/ray/pull/61083) | Data | — | clean up test_csv.py to focus on CSV-specific tests (#61083) |
| 326 | `580ce5ed18` | [#61447](https://github.com/ray-project/ray/pull/61447) | Data | ✅ | `TaskPoolMapOperator` 获取正确的逻辑资源使用量 |
| 327 | `0951139b9b` | [#60307](https://github.com/ray-project/ray/pull/60307) | Data | ✅ | 新增 Kafka Datasink |
| 328 | `2bd8fed85e` | [#60828](https://github.com/ray-project/ray/pull/60828) | Data | ✅ | **修复 `OpBufferQueue` 竞态条件**，引入线程安全队列 |
| 329 | `9971f87c0f` | [#61837](https://github.com/ray-project/ray/pull/61837) | Core | ✅ | **版本不匹配时清理已启动的节点进程**，避免孤儿进程 |
| 330 | `acdef1804e` | [#61845](https://github.com/ray-project/ray/pull/61845) | Core | ✅ | 修复 StatusOr 中 -Wmaybe-uninitialized 编译告警 |
| 331 | `cd3b3f7dc3` | [#61853](https://github.com/ray-project/ray/pull/61853) | Data | ✅ | 修复缺失的 PYARROW_VERSION 和 DataContext import |
| 332 | `5709cadaf1` | [#61143](https://github.com/ray-project/ray/pull/61143) | Data | ✅ | Windows 下 Ray Data 日志编码默认 UTF-8 |
| 333 | `20eae5b119` | [#61664](https://github.com/ray-project/ray/pull/61664) | Serve | — | Deployment-scoped actor 生命周期和延迟 replica 创建 [3/n] |
| 334 | `45ea2a708b` | [#61821](https://github.com/ray-project/ray/pull/61821) | Data | ✅ | checkpoint 两阶段提交 + 前缀 trie 恢复 |
| 335 | `2c7f57ad45` | [#61840](https://github.com/ray-project/ray/pull/61840) | Data | — | Track `spilled_bytes_total` in release tests (#61840) |
| 336 | `0c7c3469a4` | [#61659](https://github.com/ray-project/ray/pull/61659) | Serve | — | Gang Scheduling 迁移 [8/n] |
| 337 | `ca797fe743` | [#61365](https://github.com/ray-project/ray/pull/61365) | Data | — | Add pyrefly to Data CI pipeline (#61365) |
| 338 | `abb30adbd0` | [#61803](https://github.com/ray-project/ray/pull/61803) | Core | ✅ | IPPR [3/N]：IPPR spec schema 和 pod resize status 模型 |
| 339 | `cbf113e837` | [#61855](https://github.com/ray-project/ray/pull/61855) | Core | ✅ | 移除 GCS 集中式调度遗留的 `normal_task_resources` 死代码 |
| 340 | `3276a6f761` | [#61891](https://github.com/ray-project/ray/pull/61891) | Data | — | Update release test spilling metric to match existing train test metric (#61891) |
| 341 | `6af9f3686b` | [#61747](https://github.com/ray-project/ray/pull/61747) | Core | — | Replace cgraphs note with rdt note on main core docs page (#61747) |
| 342 | `d019584464` | [#61858](https://github.com/ray-project/ray/pull/61858) | Core | ✅ | 修复 Java Local Mode 多 Actor 类型混淆 |
| 343 | `7684f78f20` | [#61903](https://github.com/ray-project/ray/pull/61903) | Data | — | Add failing files to type checking exclude list (#61903) |
| 344 | `e4286a9881` | [#61916](https://github.com/ray-project/ray/pull/61916) | Data | ✅ | 修复 DefaultResizingPolicy lint 错误 |
| 345 | `49293dae41` | [#60857](https://github.com/ray-project/ray/pull/60857) | Core | ✅ | 添加 submission job 事件 proto 定义 |
| 346 | `d7e5ce6546` | [#61848](https://github.com/ray-project/ray/pull/61848) | Data | ✅ | Actor Pool Map Operator 代码清理 |
| 347 | `fd93429ca9` | [#61879](https://github.com/ray-project/ray/pull/61879) | Core | ✅ | 修复 EC2InstanceTerminator 的 SSL 不匹配（release 测试） |
| 348 | `16eaed0ec4` | [#61700](https://github.com/ray-project/ray/pull/61700) | Data | ✅ | **用 `__ray_shutdown__` 替代 `on_exit` hook，修复 UDF 清理竞态** |
| 349 | `c6006fd969` | [#61926](https://github.com/ray-project/ray/pull/61926) | Data | ✅ | 修复 pyrefly 类型检查错误 |
| 350 | `ca430cfd5a` | [#61615](https://github.com/ray-project/ray/pull/61615) | Data | ✅ | 新增 DataSourceV2 核心 API（Scanner/Reader/优化器 mixin） |
| 351 | `23e587f22f` | [#61925](https://github.com/ray-project/ray/pull/61925) | Core | ✅ | **修复 absl Mutex 重入锁导致的 SIGABRT 崩溃** |
| 352 | `b75d20907a` | [#61591](https://github.com/ray-project/ray/pull/61591) | Data | ✅ | **Actor Pool Map 调度开销降低约 57%** |
| 353 | `0e9c67ac28` | [#61805](https://github.com/ray-project/ray/pull/61805) | Data | — | add TPCH Q2 release test (#61805) |
| 354 | `5986bbb54d` | [#61348](https://github.com/ray-project/ray/pull/61348) | Data | ✅ | [移除 ExecutionPlan 1/N] 逻辑计算迁移到 LogicalPlan |
| 355 | `7eecf606af` | [#61902](https://github.com/ray-project/ray/pull/61902) | Dashboard | ✅ | JobSubmissionClient 转发 `**kwargs` 到 cluster info resolvers |
| 356 | `e59d4db43a` | [#61998](https://github.com/ray-project/ray/pull/61998) | Data | ✅ | 类标识符碰撞日志降级为 debug |
| 357 | `e22bdee678` | [#60497](https://github.com/ray-project/ray/pull/60497) | Data | ✅ | Lance 数据源增强：写入重试、命名空间、driver 端提交 |
| 358 | `aff2c84f4c` | [#61999](https://github.com/ray-project/ray/pull/61999) | Data | ✅ | block.py 函数重命名 |
| 359 | `5c9c1d5413` | [#61993](https://github.com/ray-project/ray/pull/61993) | Data | — | Make `image_classification` release test more realistic (#61993) |
| 360 | `fac1f45b97` | [#61990](https://github.com/ray-project/ray/pull/61990) | Data | ✅ | 更新 exclude_resources 文档 |
| 361 | `35c9a08253` | [#61744](https://github.com/ray-project/ray/pull/61744) | Data | ✅ | 添加测试辅助函数和重构 stats 测试 |
| 362 | `fe32fba286` | [#61927](https://github.com/ray-project/ray/pull/61927) | Core | — | Split test_state_api to deflake test (#61927) |
| 363 | `e23692fc6a` | [#61361](https://github.com/ray-project/ray/pull/61361) | Core | ✅ | 资源隔离 [9/n]：基于内存压力（PSI）的内存监控器 |
| 364 | `027d17fbdf` | [#59286](https://github.com/ray-project/ray/pull/59286) | Core | ✅ | 改进 `num_returns` 参数错误处理，快速失败 |
| 365 | `7985f5dbe6` | [#61934](https://github.com/ray-project/ray/pull/61934) | Core | ✅ | **修复 `ActorMethod.options()` 引用循环**，避免 actor 延迟释放 |
| 366 | `efc25f434a` | [#61601](https://github.com/ray-project/ray/pull/61601) | Build | — | Upgrade Bazel from 6.5.0 to 7.5.0 (#61601) |
| 367 | `732c259d98` | [#61917](https://github.com/ray-project/ray/pull/61917) | Data | ✅ | **放宽 `DefaultActorAutoscaler` 约束**，不再因 task slot 限制阻止扩缩容 |
| 368 | `6861feba8c` | [#62018](https://github.com/ray-project/ray/pull/62018) | Serve | — | 测试修复：deflake test_direct_ingress |
| 369 | `562aab153e` | [#61844](https://github.com/ray-project/ray/pull/61844) | Serve | — | 移除策略包装器中冗余的 replica 边界钳制 |
| 370 | `b47c0e79f6` | [#59656](https://github.com/ray-project/ray/pull/59656) | Data | ✅ | 新增 `random()` 和 `uuid()` 表达式 |
| 371 | `569eb4e61f` | [#61997](https://github.com/ray-project/ray/pull/61997) | Data | ✅ | DataSourceV2 文件分区（RoundRobinPartitioner） [3/n] |
| 372 | `d97727d3bd` | [#61899](https://github.com/ray-project/ray/pull/61899) | Data | — | Add heterogeneous memory batch inference release test (#61899) |
| 373 | `742522b57c` | [#62034](https://github.com/ray-project/ray/pull/62034) | Data | ✅ | pyrefly 修复 lance table_id |
| 374 | `e14822a09f` | [#62036](https://github.com/ray-project/ray/pull/62036) | Data | ✅ | 回退 ActorMethod 引用循环变通方案（上游 [#61934](https://github.com/ray-project/ray/pull/61934) 已修复） |
| 375 | `ad897f2c78` | [#61989](https://github.com/ray-project/ray/pull/61989) | Data | ✅ | 修复自动扩缩容日志输出精度和重复问题 |
| 376 | `63fea83649` | [#62029](https://github.com/ray-project/ray/pull/62029) | Data | ✅ | 修复 `target_max_block_size=None` 时反压过于乐观 |
| 377 | `17b70932b5` | [#61883](https://github.com/ray-project/ray/pull/61883) | Data | — | Remove dead release tests (#61883) |
| 378 | `657e6ceba5` | [#61884](https://github.com/ray-project/ray/pull/61884) | Data | — | Remove dead `Benchmark` methods (#61884) |
| 379 | `e52e9ee522` | [#61988](https://github.com/ray-project/ray/pull/61988) | Serve | — | 添加环境变量指定 HAProxy 负载均衡算法 |
| 380 | `82c1051f16` | [#61405](https://github.com/ray-project/ray/pull/61405) | Data | ✅ | **重构执行回调为静态初始化**，防止状态泄漏 |
| 381 | `98582028b0` | [#62031](https://github.com/ray-project/ray/pull/62031) | Data | ✅ | 修复 `average_task_scheduling_time_s` 指标计算膨胀 |
| 382 | `21ca9594b5` | [#62053](https://github.com/ray-project/ray/pull/62053) | Data | — | Fix planner tests to handle tuple return from planner.plan() (#62053) |
| 383 | `746ce86722` | [#62050](https://github.com/ray-project/ray/pull/62050) | Data | ✅ | [移除 ExecutionPlan 2/N] 移除 get_plan_conversion_fns |
| 384 | `b737803ea3` | [#62054](https://github.com/ray-project/ray/pull/62054) | Serve+LLM | — | 将 Serve LLM API 提升为 beta |
| 385 | `7dd6606efd` | [#62055](https://github.com/ray-project/ray/pull/62055) | Data | ✅ | [移除 ExecutionPlan 3/N] 移除旧的基于实例的执行回调 API |
| 386 | `03a216ddfd` | [#62037](https://github.com/ray-project/ray/pull/62037) | Data | — | Run Multimodal Inference Benchmark Release Tests Nightly with `Benchmark` Utility (#62037) |
| 387 | `0adbd5be72` | [#62065](https://github.com/ray-project/ray/pull/62065) | Data | ✅ | 修复 `ParquetDatasource` PyArrow 版本检查（应为 22.0） |
| 388 | `dfc05797b2` | [#62057](https://github.com/ray-project/ray/pull/62057) | Data | ✅ | 修复 `StreamingSplitDataIterator.schema()` |
| 389 | `71e0d5cc5f` | [#60344](https://github.com/ray-project/ray/pull/60344) | Data | — | Fix trust remote code download (#60344) |
| 390 | `028e164bbc` | [#62062](https://github.com/ray-project/ray/pull/62062) | Data | ✅ | 支持 rapidsmpf 26.2 的 GPU shuffle |
| 391 | `40df4994e3` | [#62070](https://github.com/ray-project/ray/pull/62070) | Core | ✅ | **修复 RUNNING 任务指标负值**（竞态条件） |
| 392 | `facbdf4033` | [#61833](https://github.com/ray-project/ray/pull/61833) | Serve | — | Deployment-scoped actor 公共 API 和文档 [4/n] |
| 393 | `3b7ff9d2c4` | [#62088](https://github.com/ray-project/ray/pull/62088) | Serve | — | 从控制器 histogram 指标中移除 replica 标签 |
| 394 | `59c4372abe` | [#62114](https://github.com/ray-project/ray/pull/62114) | Data | ✅ | **Actor 排名算法 O(N*M) → O(N*log M)**，堆排序 |
| 395 | `1ce6214236` | [#61996](https://github.com/ray-project/ray/pull/61996) | Data | ✅ | **缓存 `_map_task` 公共参数，actor 创建时间减半** |
| 396 | `a1027621eb` | [#62118](https://github.com/ray-project/ray/pull/62118) | Data | ✅ | Hash Shuffle 指标对齐 |
| 397 | `d032daf9df` | [#62108](https://github.com/ray-project/ray/pull/62108) | Data | ✅ | **优化大 schema 哈希/比较性能**，减少 DatasetStats 字符串传递 |
| 398 | `c02bd31ae3` | [#62056](https://github.com/ray-project/ray/pull/62056) | Data | ✅ | **🔒 安全修复：Arrow 扩展类型反序列化 RCE 漏洞** (GHSA-mw35-8rx3-xf9r) |
| 399 | `1a92e91af4` | [#61890](https://github.com/ray-project/ray/pull/61890) | Data | ✅ | **反压阈值从 90% 降至 50%**，更早启动反压减少 spilling |
| 400 | `94ade36ae2` | [#62126](https://github.com/ray-project/ray/pull/62126) | Data | ✅ | autoscaler traceback 日志降级为 debug |
| 401 | `0f7a9f1ac1` | [#60104](https://github.com/ray-project/ray/pull/60104) | Core | ✅ | **修复 pop worker 失败时任务永久卡住**，引入重试上限（默认5次） |
| 402 | `7a61534a40` | [#62071](https://github.com/ray-project/ray/pull/62071) | Core | ✅ | Schedule 重命名为 SchedulePlacementGroup |
| 403 | `a0b7c79019` | [#61811](https://github.com/ray-project/ray/pull/61811) | Core | ✅ | 修复 Azure `ray down` 时误删共享 MSI |
| 404 | `3cce4b50eb` | [#61607](https://github.com/ray-project/ray/pull/61607) | Data | ✅ | **移除 StreamSplitDataIterator 的 Dataset 引用，解决 head 节点 OOM** |
| 405 | `e4f0e0e9f8` | [#60868](https://github.com/ray-project/ray/pull/60868) | Core | ✅ | Ray Client 模式添加 UV 包管理器支持 |
| 406 | `a28492dd93` | [#62112](https://github.com/ray-project/ray/pull/62112) | Core | ✅ | HandleDrainNode 日志打印 gRPC peer 地址 |
| 407 | `827d26b13b` | [#62135](https://github.com/ray-project/ray/pull/62135) | Core | — | Remove lazy import (#62135) |
| 408 | `99aa13b936` | [#62087](https://github.com/ray-project/ray/pull/62087) | Data | — | Make multimodal inference tests use Ray Data defaults (#62087) |
| 409 | `bf44dfc4d8` | [#61920](https://github.com/ray-project/ray/pull/61920) | Serve | — | **修复流式部署在 inflight 请求归零后的自动扩缩** |
| 410 | `f9c04022cb` | [#62128](https://github.com/ray-project/ray/pull/62128) | Serve | — | **消除 Gang 自动扩缩振荡**，修复 flaky 测试 |
| 411 | `12d964d5ad` | [#62146](https://github.com/ray-project/ray/pull/62146) | Serve | — | 记录已启用的 Serve 吞吐量优化 |
| 412 | `dc0d8b65ed` | [#62124](https://github.com/ray-project/ray/pull/62124) | Serve | — | 测试修复：Fix test deployment actor test on windows |
| 413 | `f26c3134a9` | [#60198](https://github.com/ray-project/ray/pull/60198) | Serve | — | 将共享 replica 方法从 ReplicaBase 移到 Replica，移除抽象基类 |
| 414 | `e0dee73025` | [#61351](https://github.com/ray-project/ray/pull/61351) | Data | ✅ | [移除 ExecutionPlan 4/N] 简化 legacy_compat |
| 415 | `8b07fc494f` | [#62149](https://github.com/ray-project/ray/pull/62149) | Data | ✅ | 修复 wide_schema_pipeline_tensors cloudpickle 反序列化 |
| 416 | `251497d888` | [#62184](https://github.com/ray-project/ray/pull/62184) | Serve | — | 测试修复：Fix flaky TestInitialReplicasHandling timeouts |
| 417 | `945f423040` | [#62086](https://github.com/ray-project/ray/pull/62086) | Core | ✅ | **修复 pg.ready() 死锁**：取消并发上限 |
| 418 | `ef6ba17a36` | [#62147](https://github.com/ray-project/ray/pull/62147) | Serve | — | **修复链式 DeploymentResponse 中上游 ActorDiedError 导致请求挂起** |
| 419 | `11b51aab3f` | [#62223](https://github.com/ray-project/ray/pull/62223) | Serve+LLM | — | 保持 PD 和 DP 相关 API 为 alpha 状态 |
| 420 | `8e1b0cb914` | [#62213](https://github.com/ray-project/ray/pull/62213) | Serve | — | **修复 head 节点零 ingress replica 时 HAProxy 健康检查失败** |
| 421 | `85a44d6f10` | [#62211](https://github.com/ray-project/ray/pull/62211) | Serve | — | 测试修复：Add 3072 and 4096 replicas cases to serve_controller_benchmark_haproxy |
| 422 | `059d7fa50f` | [#62161](https://github.com/ray-project/ray/pull/62161) | Serve | — | Deployment-scoped actor 控制器健康检查 [5/n] |
| 423 | `f97860b639` | [#62222](https://github.com/ray-project/ray/pull/62222) | Core | — | pin cupy-cuda12x in LLM BYOD for RDT A100 benchmark (#62222) |
| 424 | `e2f3a47dd0` | [#62142](https://github.com/ray-project/ray/pull/62142) | Core | — | avoid using podman pull to talk to docker api (#62142) |
| 425 | `0533874d49` | [#62188](https://github.com/ray-project/ray/pull/62188) | Core | — | Remove unrealistic GPU resource counts in test_placement_group_mini_integration and some clean up (#62188) |
| 426 | `a5ce5dd4c7` | [#62226](https://github.com/ray-project/ray/pull/62226) | Core | ✅ | HandleUnregisterNode 日志打印 gRPC peer 地址 |
| 427 | `8fd4fe082a` | [#61716](https://github.com/ray-project/ray/pull/61716) | Dashboard | ✅ | Dashboard 添加 Ray Data Queued Blocks 指标 |
| 428 | `31cda7c00f` | [#61638](https://github.com/ray-project/ray/pull/61638) | Core | ✅ | **缓存 ActorHandle.__hash__**，修复 __eq__ 正确性 |
| 429 | `e0bc348992` | [#61421](https://github.com/ray-project/ray/pull/61421) | Core | ✅ | **修复布尔环境变量解析 bug**（`bool("0")` 为 True） |
| 430 | `a3894f8833` | [#62105](https://github.com/ray-project/ray/pull/62105) | Data | — | Add check to fail release tests if nodes dead for non-chaos tests (#62105) |
| 431 | `4d5b3d0c52` | [#62117](https://github.com/ray-project/ray/pull/62117) | Data | ✅ | **资源管理器考虑外部消费者 Object Store 使用量** |
| 432 | `6dadfdf118` | [#61349](https://github.com/ray-project/ray/pull/61349) | Data | ✅ | [移除 ExecutionPlan 5/N] 将 schema/meta_count 迁移到 Dataset |
| 433 | `b165ee79bc` | [#62210](https://github.com/ray-project/ray/pull/62210) | Data | ✅ | 自动扩缩容协调器 traceback 日志可配置 |
| 434 | `0ed5173e82` | [#62242](https://github.com/ray-project/ray/pull/62242) | Data | ✅ | **修复 Parquet batch_size 超出 C++ 32 位 int 范围** |
| 435 | `4a79cd954e` | [#62153](https://github.com/ray-project/ray/pull/62153) | Core | ✅ | 替换已弃用的 threading API |
| 436 | `b8a89cbd5f` | [#62076](https://github.com/ray-project/ray/pull/62076) | Serve+LLM | — | 用 decode-as-orchestrator PD 架构替换 PDProxyServer |
| 437 | `3ffcff63bf` | [#62265](https://github.com/ray-project/ray/pull/62265) | Core | — | Bump `test_tpu.py` to medium-sized test (#62265) |
| 438 | `47664a5a96` | [#62266](https://github.com/ray-project/ray/pull/62266) | Core | — | Bump `test_debug_tools.py` to medium-sized test (#62266) |
| 439 | `035844f4a6` | [#62264](https://github.com/ray-project/ray/pull/62264) | Core | — | Deflake `test_task_events_2` (#62264) |
| 440 | `b9bc54f81f` | [#62251](https://github.com/ray-project/ray/pull/62251) | Core | ✅ | 修复 autoscaler v2 ReadOnlyProvider.terminate() 签名不匹配 |
| 441 | `adfa032e82` | [#62267](https://github.com/ray-project/ray/pull/62267) | Core | — | Split `test_token_auth_integration.py` (#62267) |
| 442 | `9bf32b751d` | [#62279](https://github.com/ray-project/ray/pull/62279) | Core | ✅ | 放宽 worker 线程数限制 |
| 443 | `8d53131a15` | [#61701](https://github.com/ray-project/ray/pull/61701) | Core | ✅ | 添加通用 PlatformEvent proto（K8s/Slurm 事件） |
| 444 | `d3b06f8d66` | [#62209](https://github.com/ray-project/ray/pull/62209) | Data | ✅ | 资源预算 Prometheus 指标移至 ExecutionCallback |
| 445 | `735e6fb9be` | [#62145](https://github.com/ray-project/ray/pull/62145) | Data | ✅ | 统计数据计算优化：多次遍历合并为单次 |
| 446 | `cfd4ac9e55` | [#62120](https://github.com/ray-project/ray/pull/62120) | Data | ✅ | 修复 flaky test bare assert |
| 447 | `357175bdbb` | [#61814](https://github.com/ray-project/ray/pull/61814) | Core | ✅ | IPPR [4/N]：独立的 KubeRay IPPR Provider |
| 448 | `b01ef6aae0` | [#60317](https://github.com/ray-project/ray/pull/60317) | Core | ✅ | 升级 cloudpickle 到 3.1.2 支持 Python 3.14 |
| 449 | `02c380ffdf` | [#61305](https://github.com/ray-project/ray/pull/61305) | Data | ✅ | 添加 TPCH Q13 测试 |
| 450 | `4127cd62cd` | [#62303](https://github.com/ray-project/ray/pull/62303) | Core | ✅ | 禁用 cgraph GPU 测试 |
| 451 | `bc588f5d00` | [#62331](https://github.com/ray-project/ray/pull/62331) | Serve | — | **修复 Serve 自动扩缩延迟使用挂钟时间** |
| 452 | `232459596f` | [#62405](https://github.com/ray-project/ray/pull/62405) | Data | ✅ | 默认禁用 hanging issue 检测 |
| 453 | `6fcc3f4298` | [#62413](https://github.com/ray-project/ray/pull/62413) | Core | — | Cherry-pick: Deflake test_dashboard_port_conflict (#62413) |
| 454 | `58af3fc5ca` | [#62517](https://github.com/ray-project/ray/pull/62517) | Serve | — | Cherry-pick 多个修复到 2.55 分支（含 [#62323](https://github.com/ray-project/ray/pull/62323), [#62330](https://github.com/ray-project/ray/pull/62330), [#62366](https://github.com/ray-project/ray/pull/62366)） |

---

## 表格 3：表格2 与表格1 的差集（按分支 commit 顺序）

> 共 190 个 commit（80 个 Core/Data/Dashboard 测试/CI/文档 + 110 个 Serve 模块），未被表格1收录。

| # | Commit | PR | 模块 | 描述 |
|---|--------|-----|------|------|
| 1 | `8c732fecf5` | [#60365](https://github.com/ray-project/ray/pull/60365) | Serve | **修复节点迁移期间 replica 排名一致性检查失败** |
| 2 | `e7fa2e4204` | [#60754](https://github.com/ray-project/ray/pull/60754) | Serve | **修复 direct ingress 模式下请求卡在 draining 导致 replica 永久挂起** |
| 3 | `6a5e3de35c` | [#59548](https://github.com/ray-project/ray/pull/59548) | Serve | 添加默认的基于队列的自动扩缩策略 [2/3] |
| 4 | `f27985d268` | [#60765](https://github.com/ray-project/ray/pull/60765) | Core | Fix `test_state_api` (#60765) |
| 5 | `2afe98d142` | [#60740](https://github.com/ray-project/ray/pull/60740) | Core | fix test_aggregator_agent flaky test (#60740) |
| 6 | `cf6ca75a98` | [#60757](https://github.com/ray-project/ray/pull/60757) | Serve | 吞吐量优化启用时添加环境变量覆盖 |
| 7 | `521cb0ece4` | [#60758](https://github.com/ray-project/ray/pull/60758) | Serve | 添加 Ray Serve replica 利用率指标 |
| 8 | `fc91ac2b4d` | [#60767](https://github.com/ray-project/ray/pull/60767) | Serve | gRPC 双向流核心类型和公共 API [1/n] |
| 9 | `0edea4b6f9` | [#60810](https://github.com/ray-project/ray/pull/60810) | Serve | 对无 `record_routing_stats` 的 deployment 跳过路由统计收集 |
| 10 | `1949d6094d` | [#60829](https://github.com/ray-project/ray/pull/60829) | Serve | 替换 `ReplicaStateContainer.get()` 中 O(n^2) 列表拼接 |
| 11 | `02ab6f73f8` | [#60830](https://github.com/ray-project/ray/pull/60830) | Serve | `ReplicaStateContainer.count()` 用生成器 sum 替换 `len(list(filter(...)))` |
| 12 | `2f2fa87d73` | [#60838](https://github.com/ray-project/ray/pull/60838) | Serve | 修复 `allow_new_compaction` 中 O(n^2) replica 计数 |
| 13 | `4e3f034ae3` | [#60819](https://github.com/ray-project/ray/pull/60819) | Dashboard | Add NIXL KV transfer metrics to Serve LLM Grafana dashboard (#60819) |
| 14 | `c1b051cd6b` | [#60844](https://github.com/ray-project/ray/pull/60844) | Serve | 缓存 `AutoscalingPolicy.get_policy()` 中反序列化的策略，避免重复 `cloudpickle.loads()` |
| 15 | `641d4e52b5` | [#60843](https://github.com/ray-project/ray/pull/60843) | Serve | 修复 `ClusterNodeInfoCache.update()` 排序 bug 并优化 |
| 16 | `2df3ca07a7` | [#60842](https://github.com/ray-project/ray/pull/60842) | Serve | `record_request_routing_info` 中 O(1) replica 查找 |
| 17 | `eb4a361c3d` | [#60833](https://github.com/ray-project/ray/pull/60833) | Serve | 消除 `update_actor_details` 中每 replica 每 tick 的 Pydantic rebuild |
| 18 | `3b69ec307f` | [#60832](https://github.com/ray-project/ray/pull/60832) | Serve | 优化 `stop_replicas()` 避免 pop-all/re-add 循环 |
| 19 | `16ccd3e979` | [#60768](https://github.com/ray-project/ray/pull/60768) | Serve | 重构 gRPC server 使用 streaming type 枚举 [2/n] |
| 20 | `338087b8b0` | [#60852](https://github.com/ray-project/ray/pull/60852) | Core | Fix test_failed_task_runtime_env_setup failure on windows (#60852) |
| 21 | `dbc2e95d34` | [#60486](https://github.com/ray-project/ray/pull/60486) | Serve | CI 改进：[deps] Generating and installing depset on serve ci image |
| 22 | `fa31667273` | [#60029](https://github.com/ray-project/ray/pull/60029) | Data | Add polars usage instruction to docs (#60029) |
| 23 | `577c529f90` | [#60586](https://github.com/ray-project/ray/pull/60586) | Serve | **添加 HAProxy 支持** |
| 24 | `d043552df3` | [#60823](https://github.com/ray-project/ray/pull/60823) | Serve | 控制器上节流 `serve_deployment_replica_healthy` gauge 记录 |
| 25 | `3e187a3cdb` | [#60822](https://github.com/ray-project/ray/pull/60822) | Serve | 修复 `enable_access_log=False` 未抑制 stderr 上的访问日志 |
| 26 | `053041c253` | [#60923](https://github.com/ray-project/ray/pull/60923) | Data | Fix flaky test_map_operator_streamed due to ordering assumption (#60923) |
| 27 | `2ab4760d32` | [#60921](https://github.com/ray-project/ray/pull/60921) | Data | Add job-level checkpointing documentation (#60921) |
| 28 | `301869282f` | [#60956](https://github.com/ray-project/ray/pull/60956) | Data | disable sort_chaos test (#60956) |
| 29 | `023e2e1a2a` | [#60854](https://github.com/ray-project/ray/pull/60854) | Data | Avoid deprecated TRANSFORMERS_CACHE and treat inability to load HuggingFace config as non-fatal (#60854) |
| 30 | `b4dc08868e` | [#60934](https://github.com/ray-project/ray/pull/60934) | Data | Use the default uniproc as the distributed backend (#60934) |
| 31 | `ab5461e63d` | [#60944](https://github.com/ray-project/ray/pull/60944) | Serve | 引入 Gang Scheduling 机制 [1/n] |
| 32 | `5c0b632c11` | [#60964](https://github.com/ray-project/ray/pull/60964) | Serve | 添加基于类的自动扩缩策略支持 (`policy_kwargs`) |
| 33 | `863d74c7aa` | [#60971](https://github.com/ray-project/ray/pull/60971) | Serve | 修复 test_metrics_3 测试不稳定 |
| 34 | `4e83f20ae2` | [#60977](https://github.com/ray-project/ray/pull/60977) | Serve | 启用 task_handler 装饰器的异步任务处理支持 [1/n] |
| 35 | `2f654ada3e` | [#60985](https://github.com/ray-project/ray/pull/60985) | Serve | 将 multiplex 模型加载/卸载日志从 INFO 降级为 DEBUG |
| 36 | `5c4a443a50` | [#60835](https://github.com/ray-project/ray/pull/60835) | Data | Configure pyrefly for local development (#60835) |
| 37 | `529d2f8d1a` | [#60385](https://github.com/ray-project/ray/pull/60385) | Data | Add vLLM metrics export and Data LLM Grafana dashboard (#60385) |
| 38 | `90301f3392` | [#61008](https://github.com/ray-project/ray/pull/61008) | Serve | 添加多 broker Taskiq 适配器配置和初始化 [2/n] |
| 39 | `00b45989d2` | [#60845](https://github.com/ray-project/ray/pull/60845) | Serve | 自定义请求路由器 API 添加 `on_request_completed` 钩子 |
| 40 | `1321355222` | [#60806](https://github.com/ray-project/ray/pull/60806) | Serve | 优化 pack 调度从 O(replicas*total_replicas) 到 O(replicas*nodes) |
| 41 | `c309e4778e` | [#60851](https://github.com/ray-project/ray/pull/60851) | Serve | 集成基于队列的自动扩缩与 task consumer [3/3] |
| 42 | `c0ebd66cd4` | [#60989](https://github.com/ray-project/ray/pull/60989) | Core | Deflake wait for condition test (#60989) |
| 43 | `f935ad8dcb` | [#60809](https://github.com/ray-project/ray/pull/60809) | Data | Add object store spill rate monitoring to train benchmark (#60809) |
| 44 | `1bbaf33482` | [#60914](https://github.com/ray-project/ray/pull/60914) | Serve | CI 改进：[Serve] Add dedicated Buildkite target for HAProxy tests |
| 45 | `f645d591d6` | [#60953](https://github.com/ray-project/ray/pull/60953) | Serve | 测试修复：Fix flaky test_replica_metrics_fields in test_metrics_haproxy |
| 46 | `26702b7dfd` | [#61068](https://github.com/ray-project/ray/pull/61068) | Serve | 测试修复：[CI] Deflake `test_deploy_app_2.py::test_num_replicas_auto_basic` |
| 47 | `db045c49ac` | [#60955](https://github.com/ray-project/ray/pull/60955) | Serve | 将 haproxy.py 中的 fcntl import 移到函数作用域，兼容 Windows |
| 48 | `569d681256` | [#60892](https://github.com/ray-project/ray/pull/60892) | Serve | 使 test_replica_utilization_metric 确定性运行 |
| 49 | `8882b9045a` | [#60993](https://github.com/ray-project/ray/pull/60993) | Data | test_json/test_file_based_datasource (#60993) |
| 50 | `627919071f` | [#61048](https://github.com/ray-project/ray/pull/61048) | Data | Fix test_runtime_metrics to exclude Scheduling from time comparison (#61048) |
| 51 | `ffce98e21d` | [#61041](https://github.com/ray-project/ray/pull/61041) | Data | Add regression test to ensure we don't double count union resources (#61041) |
| 52 | `7a3bbe7166` | [#61092](https://github.com/ray-project/ray/pull/61092) | Serve | `ray_serve_deployment_error_counter_total` 指标添加 `exception_type` 标签 |
| 53 | `f1a1039a89` | [#60973](https://github.com/ray-project/ray/pull/60973) | Data | Fix flaky test_parquet_read_random_shuffle (#60973) |
| 54 | `b434ecffcb` | [#61034](https://github.com/ray-project/ray/pull/61034) | Core | fix set/get env races caused by `OtlpGrpcMetricExporterOptions` (#61034) |
| 55 | `fe0950e7c1` | [#61122](https://github.com/ray-project/ray/pull/61122) | Core | Remove old nixl deduplication logic (#61122) |
| 56 | `18d0644491` | [#60943](https://github.com/ray-project/ray/pull/60943) | Serve | 在 test_grpc 中使用 get_application_url |
| 57 | `95049c5854` | [#61120](https://github.com/ray-project/ray/pull/61120) | Serve | **修复 HAProxy 配置文件竞态条件和 draining guard** |
| 58 | `83d91e1e53` | [#60999](https://github.com/ray-project/ray/pull/60999) | Core | Implement pytorch storage block caching (#60999) |
| 59 | `3a187a5bde` | [#61079](https://github.com/ray-project/ray/pull/61079) | Data | Remove accidental __init__.py from data test directory (#61079) |
| 60 | `cb9afe9359` | [#61135](https://github.com/ray-project/ray/pull/61135) | Serve | 使用环境变量控制默认 HTTP host |
| 61 | `224b70a699` | [#61194](https://github.com/ray-project/ray/pull/61194) | Serve | 测试修复：classify test_fastapi as large |
| 62 | `f5ee9e0c22` | [#61202](https://github.com/ray-project/ray/pull/61202) | Serve | 增加 test_cli 测试超时时间 |
| 63 | `c7944e6e59` | [#61089](https://github.com/ray-project/ray/pull/61089) | Serve | gRPC inter-deployment 模式传播追踪上下文 |
| 64 | `777f37f002` | [#61186](https://github.com/ray-project/ray/pull/61186) | Serve | 添加 HAProxy 单元测试到 CI |
| 65 | `e2f0385bf6` | [#60831](https://github.com/ray-project/ray/pull/60831) | Serve | 移除 deployment state 更新循环中冗余的 `check_curr_status()` 调用 |
| 66 | `7b032ac531` | [#58892](https://github.com/ray-project/ray/pull/58892) | Serve | **Cython 实现自动扩缩指标聚合**，提升性能 |
| 67 | `9e1fa2e28c` | [#61229](https://github.com/ray-project/ray/pull/61229) | Serve | 默认启用 `RAY_SERVE_RUN_SYNC_IN_THREADPOOL=1` |
| 68 | `96ae3a5e65` | [#61276](https://github.com/ray-project/ray/pull/61276) | Serve | 修复 test_controller 测试 |
| 69 | `bee13efc27` | [#61234](https://github.com/ray-project/ray/pull/61234) | Data | Remove redundant tests from test_binary.py (#61234) |
| 70 | `34f1c6dd01` | [#61235](https://github.com/ray-project/ray/pull/61235) | Data | Remove redundant tests from test_numpy.py (#61235) |
| 71 | `50b981d2a4` | [#61230](https://github.com/ray-project/ray/pull/61230) | Serve | 为 Ray Serve 添加追踪 (tracing) 支持 |
| 72 | `8c2650aec0` | [#61134](https://github.com/ray-project/ray/pull/61134) | Core | fix long_running_many_drivers.aws release test (#61134) |
| 73 | `838a47dcc2` | [#61196](https://github.com/ray-project/ray/pull/61196) | Data | Separate unit tests from integration-heavy tests - 1 (#61196) |
| 74 | `980a971ce4` | [#61098](https://github.com/ray-project/ray/pull/61098) | Data | Add custom tokenizer example (#61098) |
| 75 | `05a40c66a1` | [#61310](https://github.com/ray-project/ray/pull/61310) | Serve | Direct ingress 优化和常量重排 |
| 76 | `2745feb86c` | [#61205](https://github.com/ray-project/ray/pull/61205) | Serve | Gang Scheduling 验证和工具函数 [2/n] |
| 77 | `d2233876e8` | [#61335](https://github.com/ray-project/ray/pull/61335) | Serve | 回退之前的变更 |
| 78 | `fba8656def` | [#60840](https://github.com/ray-project/ray/pull/60840) | Serve | 通过 dirty flag 跳过 DeploymentState 稳态下的每 tick 工作 |
| 79 | `df8039e310` | [#61367](https://github.com/ray-project/ray/pull/61367) | Data | - Skip downloading MNIST dataset + Update test_per_input_inqueue_attribution_for_union (#61367) |
| 80 | `c69f1c0101` | [#61369](https://github.com/ray-project/ray/pull/61369) | Serve | 回退之前的变更 |
| 81 | `d85ed28925` | [#61195](https://github.com/ray-project/ray/pull/61195) | Core | upgrade grpc to 1.58.0 to fix getenv races (#61195) |
| 82 | `d4833c03b1` | [#61272](https://github.com/ray-project/ray/pull/61272) | Core | add timeout to prometheus test utils (#61272) |
| 83 | `b78afa90fc` | [#61206](https://github.com/ray-project/ray/pull/61206) | Serve | Gang Scheduling 核心调度引擎 [3/n] |
| 84 | `a5ca653723` | [#61368](https://github.com/ray-project/ray/pull/61368) | Serve | 测试修复：Add a micro benchmark for serve controller |
| 85 | `dc28b1c946` | [#61247](https://github.com/ray-project/ray/pull/61247) | Core | deflake test_ray_timeline (#61247) |
| 86 | `ad64f26b00` | [#61397](https://github.com/ray-project/ray/pull/61397) | Serve | 测试修复：Add a autoscaling test near cluster capacity |
| 87 | `0ef911f5e2` | [#61396](https://github.com/ray-project/ray/pull/61396) | Serve | 修复客户端断开后成功响应被误分类为 499 |
| 88 | `436ef85554` | [#60770](https://github.com/ray-project/ray/pull/60770) | Serve | gRPC proxy 客户端流实现 [4/n] |
| 89 | `672e9fb0b1` | [#60662](https://github.com/ray-project/ray/pull/60662) | Data | Add TPCH queries 7,8,9 for benchmarking (#60662) |
| 90 | `d0e096f813` | [#61423](https://github.com/ray-project/ray/pull/61423) | Data | Fix read_datasource test to use public compute attribute (#61423) |
| 91 | `edefe43825` | [#61441](https://github.com/ray-project/ray/pull/61441) | Serve | 测试修复：Skip tracing tests for windows |
| 92 | `68aef137d7` | [#61303](https://github.com/ray-project/ray/pull/61303) | Data | Separate test_arrow_block.py with integration and unit tests (#61303) |
| 93 | `20925a36a2` | [#61357](https://github.com/ray-project/ray/pull/61357) | Build | Supports repeated execution of setup-dev.py (#61357) |
| 94 | `9e709d1b90` | [#61249](https://github.com/ray-project/ray/pull/61249) | Serve+LLM | 将 LLM API 提升为 beta |
| 95 | `48fd82584d` | [#61137](https://github.com/ray-project/ray/pull/61137) | Core | Add Windows and macOS smoke tests to premerge (#61137) |
| 96 | `8fa34b9f34` | [#61207](https://github.com/ray-project/ray/pull/61207) | Serve | Gang Scheduling 容错 [4/n] |
| 97 | `2e70e0dbe5` | [#61042](https://github.com/ray-project/ray/pull/61042) | Build | feat: add top-level build-image.sh (#61042) |
| 98 | `9e9c8cb3bb` | [#61468](https://github.com/ray-project/ray/pull/61468) | Serve | 添加 `RAY_SERVE_HAPROXY_TCP_NODELAY` 环境变量 |
| 99 | `5723d4be22` | [#60689](https://github.com/ray-project/ray/pull/60689) | Core | Support send and receive side metadata caching via cache_memory_registration (#60689) |
| 100 | `667e99c7aa` | [#61180](https://github.com/ray-project/ray/pull/61180) | Serve | 支持 fallback Serve proxy [1/n] |
| 101 | `556e2065f6` | [#61451](https://github.com/ray-project/ray/pull/61451) | Serve | 为 gRPC 客户端/双向流路径添加缺失的追踪属性 |
| 102 | `4e86649a0c` | [#61215](https://github.com/ray-project/ray/pull/61215) | Serve | Gang Scheduling 缩容 [5/n] |
| 103 | `462e4c9b35` | [#61490](https://github.com/ray-project/ray/pull/61490) | Serve | **修复 @serve.ingress 包装器中 async __init__ 被静默丢弃** |
| 104 | `5270e8ce06` | [#61491](https://github.com/ray-project/ray/pull/61491) | Serve | 测试修复：Add unit tests for NodePortManager |
| 105 | `645d4f17a4` | [#61061](https://github.com/ray-project/ray/pull/61061) | Serve | Ray Serve Pydantic v2 迁移 |
| 106 | `5acbcf7753` | [#60667](https://github.com/ray-project/ray/pull/60667) | Data | Add TPCH queries 3, 10, and 18 for benchmarking (#60667) |
| 107 | `997fbe1fad` | [#61557](https://github.com/ray-project/ray/pull/61557) | Serve | 回退之前的变更 |
| 108 | `41ddcb7044` | [#61515](https://github.com/ray-project/ray/pull/61515) | Serve | 前一个自动扩缩指标未完成时跳过发送新指标 |
| 109 | `d7b11d1187` | [#61467](https://github.com/ray-project/ray/pull/61467) | Serve | Gang Scheduling 自动扩缩 [6/n] |
| 110 | `fe0d28c211` | [#61639](https://github.com/ray-project/ray/pull/61639) | Serve | Deployment-scoped actor 配置和 schema [1/n] |
| 111 | `a3e481b803` | [#61506](https://github.com/ray-project/ray/pull/61506) | Core | fix test_placement_group_3_client_mode by changing url parser (#61506) |
| 112 | `dee740298e` | [#61271](https://github.com/ray-project/ray/pull/61271) | Serve | HAProxy 使用 fallback server [2/n] |
| 113 | `18474d9ddb` | [#61653](https://github.com/ray-project/ray/pull/61653) | Data | Make text embedding release test using cross-AZ scaling (#61653) |
| 114 | `680b386f9c` | [#61648](https://github.com/ray-project/ray/pull/61648) | Serve | Deployment-scoped actor 构建/部署流程 [2/n] |
| 115 | `b2f25a6f88` | [#61047](https://github.com/ray-project/ray/pull/61047) | Data | Relax checkpoint recovery test to allow at-least-once semantics (#61047) |
| 116 | `09cacbbd13` | [#61667](https://github.com/ray-project/ray/pull/61667) | Build | Make -undefined dynamic_lookup explicit in _raylet.so macOS linkopts (#61667) |
| 117 | `03902bbf82` | [#61663](https://github.com/ray-project/ray/pull/61663) | Core | Update memory pressure test to take into account log changes (#61663) |
| 118 | `4447e6a81f` | [#61668](https://github.com/ray-project/ray/pull/61668) | Build | Add java_binary all_tests_bin as deploy-jar source for Java tests (#61668) |
| 119 | `ae7ad50cc3` | [#61374](https://github.com/ray-project/ray/pull/61374) | Core | Optimize try_schedule by cacheing unavailable shapes (#61374) |
| 120 | `495220a55c` | [#61731](https://github.com/ray-project/ray/pull/61731) | Serve | **修复自动扩缩反馈循环导致扩容到 max_replicas** |
| 121 | `af9ae6ae19` | [#61669](https://github.com/ray-project/ray/pull/61669) | Build | Replace rules_boost lzma patches with a custom org_lzma_lzma build file (#61669) |
| 122 | `e61f0df9d0` | [#61603](https://github.com/ray-project/ray/pull/61603) | Serve | 控制循环中节流应用状态 gauge 上报 |
| 123 | `0241874630` | [#61244](https://github.com/ray-project/ray/pull/61244) | Core | fix misleading comment in DefaultModelConfig (#61244) |
| 124 | `68a622a6c3` | [#60967](https://github.com/ray-project/ray/pull/60967) | Serve | HTTP 访问日志中添加客户端 IP 地址 |
| 125 | `a0750952b1` | [#61694](https://github.com/ray-project/ray/pull/61694) | Build | Patch protobuf for Bazel 7 exec_tools removal (#61694) |
| 126 | `2a5a4465c4` | [#61695](https://github.com/ray-project/ray/pull/61695) | Build | Upgrade rules_apple/apple_support and set macOS toolchain flags for Bazel 7 (#61695) |
| 127 | `ade772d6ad` | [#61758](https://github.com/ray-project/ray/pull/61758) | Serve | **修复链式 ActorDiedError 在路由器中的归因错误**，添加 actor 故障测试覆盖 |
| 128 | `58efa8677c` | [#61727](https://github.com/ray-project/ray/pull/61727) | Serve | 测试修复：Stabilize gang scheduling tests |
| 129 | `2b6ee48745` | [#61717](https://github.com/ray-project/ray/pull/61717) | Serve | 测试修复：deflake test metrics |
| 130 | `34593f1f13` | [#61776](https://github.com/ray-project/ray/pull/61776) | Serve | 测试修复：increase timeout of test_custom_autoscaling_metrics |
| 131 | `6a43c013c6` | [#61755](https://github.com/ray-project/ray/pull/61755) | Serve | **修复 P99 延迟回归**：请求完成时递减队列长度缓存，减少 replica 更新开销 |
| 132 | `cd35875bc5` | [#61216](https://github.com/ray-project/ray/pull/61216) | Serve | Gang Scheduling 滚动更新 [7/n] |
| 133 | `54f7a674df` | [#60807](https://github.com/ray-project/ray/pull/60807) | Serve | 整理 Ray Serve 环境变量目录 [6/n] |
| 134 | `f27365047f` | [#61275](https://github.com/ray-project/ray/pull/61275) | Data | Remove redundant tests from test_image.py (#61275) |
| 135 | `9b1c4ffb61` | [#61722](https://github.com/ray-project/ray/pull/61722) | Build | Replace ray-images.yaml with ray-images.json (#61722) |
| 136 | `895065b2f0` | [#61657](https://github.com/ray-project/ray/pull/61657) | Data | Install mongodb-org from official repo, start mongod directly in tests (#61657) |
| 137 | `6fe7402442` | [#61818](https://github.com/ray-project/ray/pull/61818) | Serve | **确保 deployment 在异常后收敛到 healthy 状态** |
| 138 | `223ce530ae` | [#61830](https://github.com/ray-project/ray/pull/61830) | Serve | 测试修复：Fix flaky direct ingress port tests |
| 139 | `23cb3728a0` | [#61831](https://github.com/ray-project/ray/pull/61831) | Serve | 测试修复：Fix flaky test_proxy_metrics_internal_error in HAProxy metrics tests |
| 140 | `97618e6117` | [#61829](https://github.com/ray-project/ray/pull/61829) | Serve | 测试修复：Fix flaky test_http_backpressure by removing Ray worker latency |
| 141 | `7bebb5f7b9` | [#61403](https://github.com/ray-project/ray/pull/61403) | Serve | 时间序列聚合中排除早期不完整周期 |
| 142 | `1a0eeaf477` | [#61083](https://github.com/ray-project/ray/pull/61083) | Data | clean up test_csv.py to focus on CSV-specific tests (#61083) |
| 143 | `20eae5b119` | [#61664](https://github.com/ray-project/ray/pull/61664) | Serve | Deployment-scoped actor 生命周期和延迟 replica 创建 [3/n] |
| 144 | `2c7f57ad45` | [#61840](https://github.com/ray-project/ray/pull/61840) | Data | Track `spilled_bytes_total` in release tests (#61840) |
| 145 | `0c7c3469a4` | [#61659](https://github.com/ray-project/ray/pull/61659) | Serve | Gang Scheduling 迁移 [8/n] |
| 146 | `ca797fe743` | [#61365](https://github.com/ray-project/ray/pull/61365) | Data | Add pyrefly to Data CI pipeline (#61365) |
| 147 | `3276a6f761` | [#61891](https://github.com/ray-project/ray/pull/61891) | Data | Update release test spilling metric to match existing train test metric (#61891) |
| 148 | `6af9f3686b` | [#61747](https://github.com/ray-project/ray/pull/61747) | Core | Replace cgraphs note with rdt note on main core docs page (#61747) |
| 149 | `7684f78f20` | [#61903](https://github.com/ray-project/ray/pull/61903) | Data | Add failing files to type checking exclude list (#61903) |
| 150 | `0e9c67ac28` | [#61805](https://github.com/ray-project/ray/pull/61805) | Data | add TPCH Q2 release test (#61805) |
| 151 | `5c9c1d5413` | [#61993](https://github.com/ray-project/ray/pull/61993) | Data | Make `image_classification` release test more realistic (#61993) |
| 152 | `fe32fba286` | [#61927](https://github.com/ray-project/ray/pull/61927) | Core | Split test_state_api to deflake test (#61927) |
| 153 | `efc25f434a` | [#61601](https://github.com/ray-project/ray/pull/61601) | Build | Upgrade Bazel from 6.5.0 to 7.5.0 (#61601) |
| 154 | `6861feba8c` | [#62018](https://github.com/ray-project/ray/pull/62018) | Serve | 测试修复：deflake test_direct_ingress |
| 155 | `562aab153e` | [#61844](https://github.com/ray-project/ray/pull/61844) | Serve | 移除策略包装器中冗余的 replica 边界钳制 |
| 156 | `d97727d3bd` | [#61899](https://github.com/ray-project/ray/pull/61899) | Data | Add heterogeneous memory batch inference release test (#61899) |
| 157 | `17b70932b5` | [#61883](https://github.com/ray-project/ray/pull/61883) | Data | Remove dead release tests (#61883) |
| 158 | `657e6ceba5` | [#61884](https://github.com/ray-project/ray/pull/61884) | Data | Remove dead `Benchmark` methods (#61884) |
| 159 | `e52e9ee522` | [#61988](https://github.com/ray-project/ray/pull/61988) | Serve | 添加环境变量指定 HAProxy 负载均衡算法 |
| 160 | `21ca9594b5` | [#62053](https://github.com/ray-project/ray/pull/62053) | Data | Fix planner tests to handle tuple return from planner.plan() (#62053) |
| 161 | `b737803ea3` | [#62054](https://github.com/ray-project/ray/pull/62054) | Serve+LLM | 将 Serve LLM API 提升为 beta |
| 162 | `03a216ddfd` | [#62037](https://github.com/ray-project/ray/pull/62037) | Data | Run Multimodal Inference Benchmark Release Tests Nightly with `Benchmark` Utility (#62037) |
| 163 | `71e0d5cc5f` | [#60344](https://github.com/ray-project/ray/pull/60344) | Data | Fix trust remote code download (#60344) |
| 164 | `facbdf4033` | [#61833](https://github.com/ray-project/ray/pull/61833) | Serve | Deployment-scoped actor 公共 API 和文档 [4/n] |
| 165 | `3b7ff9d2c4` | [#62088](https://github.com/ray-project/ray/pull/62088) | Serve | 从控制器 histogram 指标中移除 replica 标签 |
| 166 | `827d26b13b` | [#62135](https://github.com/ray-project/ray/pull/62135) | Core | Remove lazy import (#62135) |
| 167 | `99aa13b936` | [#62087](https://github.com/ray-project/ray/pull/62087) | Data | Make multimodal inference tests use Ray Data defaults (#62087) |
| 168 | `bf44dfc4d8` | [#61920](https://github.com/ray-project/ray/pull/61920) | Serve | **修复流式部署在 inflight 请求归零后的自动扩缩** |
| 169 | `f9c04022cb` | [#62128](https://github.com/ray-project/ray/pull/62128) | Serve | **消除 Gang 自动扩缩振荡**，修复 flaky 测试 |
| 170 | `12d964d5ad` | [#62146](https://github.com/ray-project/ray/pull/62146) | Serve | 记录已启用的 Serve 吞吐量优化 |
| 171 | `dc0d8b65ed` | [#62124](https://github.com/ray-project/ray/pull/62124) | Serve | 测试修复：Fix test deployment actor test on windows |
| 172 | `f26c3134a9` | [#60198](https://github.com/ray-project/ray/pull/60198) | Serve | 将共享 replica 方法从 ReplicaBase 移到 Replica，移除抽象基类 |
| 173 | `251497d888` | [#62184](https://github.com/ray-project/ray/pull/62184) | Serve | 测试修复：Fix flaky TestInitialReplicasHandling timeouts |
| 174 | `ef6ba17a36` | [#62147](https://github.com/ray-project/ray/pull/62147) | Serve | **修复链式 DeploymentResponse 中上游 ActorDiedError 导致请求挂起** |
| 175 | `11b51aab3f` | [#62223](https://github.com/ray-project/ray/pull/62223) | Serve+LLM | 保持 PD 和 DP 相关 API 为 alpha 状态 |
| 176 | `8e1b0cb914` | [#62213](https://github.com/ray-project/ray/pull/62213) | Serve | **修复 head 节点零 ingress replica 时 HAProxy 健康检查失败** |
| 177 | `85a44d6f10` | [#62211](https://github.com/ray-project/ray/pull/62211) | Serve | 测试修复：Add 3072 and 4096 replicas cases to serve_controller_benchmark_haproxy |
| 178 | `059d7fa50f` | [#62161](https://github.com/ray-project/ray/pull/62161) | Serve | Deployment-scoped actor 控制器健康检查 [5/n] |
| 179 | `f97860b639` | [#62222](https://github.com/ray-project/ray/pull/62222) | Core | pin cupy-cuda12x in LLM BYOD for RDT A100 benchmark (#62222) |
| 180 | `e2f3a47dd0` | [#62142](https://github.com/ray-project/ray/pull/62142) | Core | avoid using podman pull to talk to docker api (#62142) |
| 181 | `0533874d49` | [#62188](https://github.com/ray-project/ray/pull/62188) | Core | Remove unrealistic GPU resource counts in test_placement_group_mini_integration and some clean up (#62188) |
| 182 | `a3894f8833` | [#62105](https://github.com/ray-project/ray/pull/62105) | Data | Add check to fail release tests if nodes dead for non-chaos tests (#62105) |
| 183 | `b8a89cbd5f` | [#62076](https://github.com/ray-project/ray/pull/62076) | Serve+LLM | 用 decode-as-orchestrator PD 架构替换 PDProxyServer |
| 184 | `3ffcff63bf` | [#62265](https://github.com/ray-project/ray/pull/62265) | Core | Bump `test_tpu.py` to medium-sized test (#62265) |
| 185 | `47664a5a96` | [#62266](https://github.com/ray-project/ray/pull/62266) | Core | Bump `test_debug_tools.py` to medium-sized test (#62266) |
| 186 | `035844f4a6` | [#62264](https://github.com/ray-project/ray/pull/62264) | Core | Deflake `test_task_events_2` (#62264) |
| 187 | `adfa032e82` | [#62267](https://github.com/ray-project/ray/pull/62267) | Core | Split `test_token_auth_integration.py` (#62267) |
| 188 | `bc588f5d00` | [#62331](https://github.com/ray-project/ray/pull/62331) | Serve | **修复 Serve 自动扩缩延迟使用挂钟时间** |
| 189 | `6fcc3f4298` | [#62413](https://github.com/ray-project/ray/pull/62413) | Core | Cherry-pick: Deflake test_dashboard_port_conflict (#62413) |
| 190 | `58af3fc5ca` | [#62517](https://github.com/ray-project/ray/pull/62517) | Serve | Cherry-pick 多个修复到 2.55 分支（含 [#62323](https://github.com/ray-project/ray/pull/62323), [#62330](https://github.com/ray-project/ray/pull/62330), [#62366](https://github.com/ray-project/ray/pull/62366)） |

---

## 总结

### 关键风险与注意事项

1. **P0 必须全部 pick**：12 个 P0 commit 涉及安全漏洞、进程崩溃、死锁和数据正确性问题，任何遗漏都可能导致生产事故
2. **依赖链不可拆分**：
   - IPPR 系列 4 个 PR 需按序 pick
   - ExecutionPlan 移除系列 5 个 PR 需一起 pick 或全部跳过
   - gRPC 升级需按 #61449 → #61281 → #61499 顺序
3. **Bazel 版本决策**：保持 Bazel 6.5.0 则跳过 5 个 Bazel 7 升级 commit；升级则必须全部 pick
4. **行为变更**：反压阈值从 90% 降至 50%（[#61890](https://github.com/ray-project/ray/pull/61890)）需灰度验证
5. **冲突热点**：`gcs_task_manager.cc`、`gcs_service.proto`、`state_aggregator.py`、`common.py` 与内部修改有冲突风险

### 关键功能改进（建议优先关注）

| 方向 | 代表性 PR | 影响 |
|------|-----------|------|
| **调度性能** | [#61288](https://github.com/ray-project/ray/pull/61288), [#62114](https://github.com/ray-project/ray/pull/62114), [#61996](https://github.com/ray-project/ray/pull/61996) | StreamingExecutor 调度循环开销降低 50%+，大规模宽 schema 场景显著受益（详见 [Issue #63544](https://github.com/ray-project/ray/issues/63544)） |
| **内存管理** | [#60774](https://github.com/ray-project/ray/pull/60774), [#61208](https://github.com/ray-project/ray/pull/61208), [#61890](https://github.com/ray-project/ray/pull/61890) | 逻辑内存纳入调度决策、反压阈值调优、双重计数修复 |
| **Checkpoint 可靠性** | [#61821](https://github.com/ray-project/ray/pull/61821) | 两阶段提交 + 前缀 Trie 恢复，实现 exactly-once 写入语义 |
| **Repartition 融合** | [#60295](https://github.com/ray-project/ray/pull/60295) | `strict=False` 模式允许算子融合，减少调度开销 |
| **稳定性修复** | [#60669](https://github.com/ray-project/ray/pull/60669), [#60850](https://github.com/ray-project/ray/pull/60850), [#60658](https://github.com/ray-project/ray/pull/60658) | Actor 销毁、任务队列阻塞、Autoscaler 重试等核心稳定性问题 |
| **安全** | [#62056](https://github.com/ray-project/ray/pull/62056) | Arrow IPC 反序列化 RCE 漏洞修复 |
| **Dashboard** | [#60772](https://github.com/ray-project/ray/pull/60772), [#60896](https://github.com/ray-project/ray/pull/60896), [#61716](https://github.com/ray-project/ray/pull/61716) | Logical Memory 面板、Grafana 日志链接、Data Queued Blocks 指标 |
| **Serve** | [#60754](https://github.com/ray-project/ray/pull/60754), [#61731](https://github.com/ray-project/ray/pull/61731), [#61755](https://github.com/ray-project/ray/pull/61755), [#62147](https://github.com/ray-project/ray/pull/62147) | 请求 draining 修复、自动扩缩反馈循环修复、P99 延迟回归修复、链式 DeploymentResponse 修复 |

### 建议执行步骤

1. **第一批**：cherry-pick 全部 12 个 P0 commit，编译验证，跑核心测试
2. **第二批**：cherry-pick 37 个 P1 commit（注意依赖链顺序），重点验证调度性能和内存管理
3. **第三批**：按需选取 P2 commit，优先 Dashboard 改进和 Checkpoint 2PC
4. **第四批**：评估 P3 重构类 commit，与后续版本同步计划结合考虑
5. **持续关注**：[Issue #63544](https://github.com/ray-project/ray/issues/63544) 中 11 个不在 2.55.1 的 PR 属后续优化，下次版本同步时需纳入
