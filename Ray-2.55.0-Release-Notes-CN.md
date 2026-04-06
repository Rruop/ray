# Ray 2.55.0 发布说明

## Ray Data

### 🎉 新特性

- 新增 `DataSourceV2` API，包含 scanner/reader 框架、文件列表和文件分区功能 (#61220, #61615, #61997)
- 支持 GPU shuffle，集成 `rapidsmpf` 26.2 (#61371, #62062)
- 新增 Kafka 数据 sinks，迁移至 `confluent-kafka`，支持 `datetime` 偏移量 (#60307, #61284, #60909)
- 新增 Turbopuffer 数据 sink (#58910)
- 新增两阶段提交 checkpoint，包含 trie 恢复和加载方法 (#61821, #60951)
- 基于队列的自动伸缩策略，与任务消费者集成 (#59548, #60851)
- 支持 GPU 阶段的自动伸缩 (#61130)
- 表达式系统：新增 `random()`、`uuid()`、`cast` 和 map namespace 支持 (#59656, #60695, #59879)
- 支持 Arrow 原生固定形状张量类型 (#56284)
- 支持将张量写入 tfrecords (#60859)
- `read_*` 函数支持 `pathlib.Path` (#61126)
- 新增 `cudf` 作为 `batch_format` (#61329)
- 允许通过 `compute` 参数为 `read_datasource()` 使用 `ActorPoolStrategy` (#59633)
- 引入 `ExecutionCache`，简化缓存流程 (#60996)
- `StreamingRepartition` 支持 `strict=False` 模式 (#60295)
- 将 lance-ray 的变更移植到 Ray Data (#60497)
- 启用 PyArrow compute-to-expression 转换，用于谓词下推 (#61617)
- 新增 vLLM 指标导出和 Data LLM Grafana 仪表盘 (#60385)
- 在资源管理器调度决策中包含逻辑内存 (#60774)
- 新增单调递增 ID 支持 (#59290)

### 💫 改进

- 性能优化：缓存 `_map_task` 参数、基于堆的 actor 排序、actor pool map 改进 (#61996, #62114, #61591)
- 优化 concat tables 和 PyArrow schema 哈希 (#61315, #62108)
- 降低默认 `DownstreamCapacityBackpressurePolicy` 阈值至 50% (#61890)
- 提高随机 API 的可复现性 (#59662)
- 将 batch size 限制在 C++ 32 位整数范围内 (#62242)
- 在资源管理器预算中计入外部消费者的对象存储使用量 (#62117)
- 使 `get_parquet_dataset` 可配置扫描片段数量 (#61670)
- 整合 schema 推断，使所有预处理器实现 `SerializablePreprocessorBase` (#61213, #61341)
- 默认禁用挂起检测 (#62405)
- 使执行回调的数据流显式化，防止状态泄漏 (#61405)
- 在执行开始时以 JSON 格式记录 `DataContext`，便于追踪 (#61150, #61428)
- 自动伸缩器：可配置 traceback、Prometheus 指标、放宽约束 (#62210, #62209, #61917, #61385)
- 新增任务调度时间、输出反压和逻辑内存的指标 (#61192, #61007, #61436)
- 防止算子独占整个共享对象存储预算 (#61605)
- 消除生成器，避免中间状态固定 (#60598)
- Windows 上默认日志编码改为 UTF-8 (#61143)
- 移除旧版 `BlockList`、`locality_with_output`、旧回调 API、PyArrow 9.0 检查 (#60575, #61044, #62055, #61483)
- 升级至 `pyiceberg` 0.11.0；限制 `pandas` < 3 (#61062, #60406)
- 将逻辑算子重构为 frozen dataclass (#61059, #61308, #61348, #61349, #61351, #61364, #61481)
- 防止聚合器调度到头节点 (#61288)
- 为零资源头节点上的 `local://` 路径添加错误提示 (#60709)

### 🔨 修复

- 修复 Parquet Arrow 扩展类型反序列化中的 RCE 漏洞 (#62056)
- 修复 `StreamingSplitDataIterator.schema()` (#62057)
- 修复 `ParquetDatasource` 对 `FileSystemFactory.inspect` 的处理 (#62065)
- 修复 `read_parquet` 对版本化对象存储 URI 的文件扩展名过滤 (#61376)
- 修复 `wide_schema_pipeline_tensors` 的 cloudpickle 反序列化问题 (#62149)
- 修复 `OpBufferQueue` 竞态条件 (#60828)
- 修复调度指标计算 (#62031)
- 修复 `OneHotEncoder` 的 `max_categories`，使用全局 top-k 而非每分区 (#60790)
- 修复 `ReservationOpResourceAllocator` 对 `ActorPoolMapOperator` 的资源借用 (#60882)
- 修复 `DatabricksUCDatasource` 的 `schema()` 方法被 schema 字符串属性遮蔽的问题 (#61282)
- 修复 `AliasExpr` 结构等价性以尊重重命名标志 (#60711)
- 修复 `_align_struct_fields` 在非对齐标量字段上的失败 (#58364)
- 修复 `min_scheduling_resources` 回退至 `incremental_resource_usage` (#60997)
- 修复终端算子输出反压解除序列 (#60798)
- 修复多输入算子对象存储内存归属 (#61208)
- 修复引用循环，将代码移至模块作用域 (#61934)
- 修复自动伸缩器日志：减少冗余输出，将 traceback 移至 debug 级别 (#61989, #62126)
- 修复 `ref_bundle` + `input_files` 的重复计算 (#61774)
- 用 `__ray_shutdown__` 替换 `on_exit` hook，修复 UDF 清理竞态 (#61700)
- 防止 `Limit` 被推过 `map_groups` (#60881)
- 在空 `_shuffle_block` 中传播 schema，修复链式 left join 中的 `ColumnNotFound` (#61507)
- 修复不清晰的元数据警告和错误的算子名称日志 (#61380)
- 将滚动利用率平均值限制为零 (#61543)
- 修复 `TimeWindowAverageCalculator` 的浮点误差 (#61580)
- 移除默认的任务级别超时，限制 Kafka datasource 中 `end_offset` (#61476)
- 避免 `train_test_split` 中的冗余读取 (#60274)
- 在没有产出输出时返回 `None` (#62029)
- 在字符串拼接中用 `TypeError` 替换裸 `raise` (#60795)

### 📖 文档

- 新增作业级别 checkpoint 文档 (#60921)
- 更新 Train 自动伸缩变更的 `exclude_resources` 文档 (#61990)
- 新增 `locality_with_output` 迁移说明 (#61151)
- 记录 `max_tasks_in_flight_per_actor` 与 `max_concurrent_batches` 的区别 (#60477)
- 补充 `MOD` 操作文档；改进 `ray.data.Datasource` 文档 (#60803, #59654)
- 新增 `polars` 使用说明 (#60029)

---

## Ray Serve

### 🎉 新特性

- 新增端到端 gRPC 客户端和双向流支持，包括公共 API、代理处理、proto 更新和开发者文档，使 Serve 应用原生支持流式工作负载 (#60767, #60768, #60769, #60770, #60771)
- 引入基于 HAProxy 的服务方案，支持 fallback 代理和负载均衡器调优参数，为运维提供更高吞吐量的入口路径 (#60586, #61180, #61271, #61468, #61988)
- 新增基于队列的异步推理和 Taskiq 工作负载自动伸缩，使伸缩决策同时考虑 HTTP 在途负载和排队任务 (#59548, #60851, #60977, #61008)
- 全面推出 gang scheduling 支持，覆盖验证、核心调度、容错、缩容、自动伸缩、滚动更新和迁移，实现多副本协调放置 (#60944, #61205, #61206, #61207, #61215, #61467, #61216, #61659)
- 引入部署范围的 actor，包含配置/schema、生命周期管理、公共 API 和控制器健康检查，便于在 Serve 中运行持久的每部署 sidecar 逻辑 (#61639, #61648, #61664, #61833, #62161)

### 💫 改进

- 新增 Serve 一级 tracing 支持，包括跨部署 gRPC 传播和更丰富的流路径属性，改善端到端可观测性 (#61230, #61089, #61451)
- 扩展运维指标：副本利用率、更丰富的错误标注、访问日志中的客户端 IP (#60758, #61092, #60967)
- 改进自动伸缩可扩展性，支持基于类的策略和 `policy_kwargs` (#60964)
- 减少控制器开销：索引优化、缓存复用、避免每 tick 重复工作 (#60810, #60829, #60830, #60838, #60842, #60843, #60844, #60832, #60806)
- 新增基于环境的吞吐调优和显式吞吐优化日志 (#60757, #62146)
- Serve 内部升级至 Pydantic v2，改进时间序列聚合行为 (#61061, #61403)

### 🔨 修复

- 修复 direct-ingress 关闭 bug：副本在排空卡住请求时可能无限挂起 (#60754)
- 修复 HAProxy 可靠性问题：配置竞态、排空守卫、平台兼容性边缘情况 (#61120, #60955)
- 修复自动伸缩正确性问题：反馈循环回归、流式缩容行为、wall-clock 延迟处理 (#61731, #61920, #62331, #61844, #60613)
- 修复请求路由和队列长度计算中的高百分位延迟回归 (#61755)
- 修复迁移和入口转换期间副本状态/健康状态的边缘情况 (#60365, #61818, #62213)
- 修复链式上游 actor 失败处理，使请求失败正确归属且不再挂起 (#61758, #62147)
- 修复成功响应后客户端断连的 HTTP 状态分类 (#61396)

### 📖 文档

- 新增 `AsyncInferenceAutoscalingPolicy` 文档，明确 HAProxy 和跨部署 gRPC 的性能指导 (#61086, #61386)
- 更新调度和配置文档，包括副本调度指导和 Serve 环境变量目录 (#60922, #60807)
- 澄清 multiplexing 和异步行为文档（模型预热约束和请求取消语义） (#61842, #62280)

### 🏗 架构重构

- 重构部署状态执行，跳过不必要的稳态每 tick 工作 (#60840)
- 将自动伸缩指标聚合迁移至 Cython 加速路径，增加控制器基准测试 (#58892, #61368)
- 简化内部结构：从私有模块迁移共享内部代码，合并副本抽象 (#60849, #61363, #60198)

---

## Ray Train

### 🎉 新特性

- 弹性训练：核心能力、用户指南、发布测试、多主机 TPU、遥测 (#60721, #61115, #61133, #61299, #61267)
- 新增 HF TRL（Transformer 强化学习）示例 (#61627)
- 新增 DeepSpeed AutoTP 和 DTensor 的 Tensor Parallel 模板 (#60160, #60158)
- 为 `ReportedCheckpoint` 新增 `status` 属性 (#61684)
- 更丰富的 Train 运行元数据 (#59186)
- 新增 Train worker 初始化计时器 (#60870)
- 配置 `torchft` 环境 (#61156)

### 💫 改进

- 在 `FixedScalingPolicy` 中向 `AutoscalingCoordinator` 注册训练资源 (#61703)
- 将 `datasets` 字段从 `TrainRunContext` 中解耦 (#61953)
- `checkpoint_upload_fn` 慢速时记录警告 (#61720)
- 修复 `StateManagerCallback` 以显式接受 datasets (#62042)
- 使 train 运行在 `before_controller_shutdown` 期间可中止 (#61816)
- 优雅中止捕获所有 `RayActorError` (#61375)
- 重构 checkpoint 和 `sync_actor` 使用 `wait_with_logging` (#61063)
- 在 `WorkerGroupError.worker_failures` 中解包 `UserExceptionWithTraceback` (#61153)

### 🔨 修复

- 修复 v2 `PlacementGroupCleaner` 僵尸 actor (#61756)
- 修复多节点运行的 checkpoint 路径 (#61471)
- 中止时取消验证任务，确定性恢复 (#61510)
- 修复 deepspeed 微调发布测试 (#61266)

### 📖 文档

- 新增异步验证与实验追踪章节 (#62104)
- 新增何时使用异步验证的章节 (#61702)

---

## Ray Tune

### 💫 改进

- 移除已弃用的 `Logger` 接口和 `logger_creator` (#61181)

### 🔨 修复

- 修复存在 `NaN` 值时 PBT 试验排序问题 (#57160)

---

## Ray LLM

### 🎉 新特性

- 用 decode-as-orchestrator PD 架构替换 `PDProxyServer` (#62076)
- 引入 WideEP 部署的 DP group 容错 (#61480)
- SGLang 引擎：流式 chat/completions、tokenize/detokenize、embeddings、多 GPU TP/PP (#61236, #61446, #61159, #61201, #62221)
- 新增 `bundle_per_worker` 配置，简化 Placement Group 设置 (#59903)
- 分离 Data LLM 和 Serve LLM 仪表盘，改进面板可见性 (#61037, #62069)

### 💫 改进

- Data LLM 和 Serve LLM API 升级为 beta (#61249, #62054, #62223)
- 升级 vLLM 至 0.16.0、0.17.0 和 0.18.0 (#61389, #61598, #61952)
- 升级 NIXL 至 v1.0.0，修复张量传输问题 (#61991)
- 统一重复的 `PlacementGroup` 配置方案 (#62241)
- 将 Serve LLM 入口与 vLLM 协议模型解耦 (#61931)
- 设置下载任务 `num_cpus=0`，减少低 CPU 机器上的争用 (#61191)
- SGLangServer 清理，用 `_build_chat_messages` 替换 `format_messages_to_prompt` (#61117, #61372)

### 🔨 修复

- 修复流式 SSE 响应中重复的 `data: [DONE]` (#62246)
- 修复 `enable_log_requests=False` 未转发至 vLLM `AsyncLLM` (#60824)
- 修复 `OpenAiIngress` 当所有模型设置 `min_replicas=0` 时的 scale-to-zero (#60836)
- 处理 vLLM task-conditional `init_app_state` 中缺失的状态属性 (#60812)
- 修复跨节点 P/D 分离的 NIXL side channel host (#60817)
- 修复 `trust_remote_code` 下载 (#60344)
- 避免已弃用的 `TRANSFORMERS_CACHE`；将 HuggingFace 配置加载失败视为非致命错误 (#60854)
- 修复 SGLangServer 中的顺序批处理 (#61189)

### 📖 文档

- 更新数据并行注意力文档 (#61706)
- 新增自定义 tokenizer 示例 (#61098)
- 新增 C/C++ 二进制不兼容性变通方案 (#62110)

---

## Ray RLlib

### 💫 改进

- Connector/batching 优化：ndarray 快速路径、直接环境步骤管道、batch 复用 (#61320, #61255, #61256, #61259, #61144)
- 统一所有算法的默认编码器 (#60302)
- 在 `TorchRLModule` forward 中切换 eval/train 模式 (#61985)
- 清理离线预学习器和单元测试 (#60632)
- 移除 `AlgorithmConfig` 中的重复赋值 (#61233)
- 移除旧版 RLlib 发布测试 (#59288)
- 新增 Footsies 环境的 APPO 示例 (#59006)

### 🔨 修复

- 支持自定义评估函数返回零 `eval_results`、`env_steps` 或 `agent_steps` (#61563)
- 修复 `PrioritizedEpisodeReplayBuffer` bug (#60065)
- 修复 `RLModuleSpec` 中缺失的 `LayerNorm` (#61025)
- 修复并行评估与训练 (#60777)
- 修复 `MultiAgentEpisode.env_t_to_agent_t` (#60319)
- 修复评估期间的默认指标 (#61590)
- 修复环境步采样/训练日志值不正确 (#56599)
- 防止参数冻结边缘情况下 `torch_learner.py` 崩溃 (#62158)

---

## Ray Core

### 🎉 新特性

- 资源隔离：基于压力的内存监控、基于时间的杀死机制、cgroup 约束 (#61361, #61323, #61097, #61210, #61297, #59365, #59368, #60752)
- IPPR：向 GCS/Python 客户端添加 `ResizeRayletResourceInstances`，schema/status 模型，KubeRay provider (#61654, #61666, #61803, #61814)
- 新增 `PlatformEvent` proto 和 Placement Group 事件 (#61701, #60449)
- 新增 Nvidia B300 支持 (#60753)
- Ray Client 模式新增 UV 支持 (#60868)
- 新增基于二次直方图的 `Percentile` 指标类型 (#61148)
- 在 `TaskInfoEntry` 和 `ActorTableData` 中暴露 `fallback_strategy` (#60659)
- 新增 submission job proto 变更 (#60857)
- 新增 TPU 工具：获取就绪多主机 slice 数量；简化弹性 TPU 伸缩 (#61300, #62141)
- 引入每节点级别的临时目录 (#60761)
- 使 `ray.put()` 泛型化：`put(value: R) -> ObjectRef[R]` (#60995)
- Python 3.14 递归限制处理支持 (#58459)

### 💫 改进

- 升级 `cloudpickle` 至 3.1.2，gRPC 至 v1.58.0，protobuf 至 3.20.3 (#60317, #61499, #60736)
- 多 gRPC 连接提升对象传输吞吐量，默认启用 (#61121, #61440)
- 通过异步 GCS RPC 改进 `pg.ready()` 性能；修复死锁 (#60657, #62086)
- RDT：非 torch 传输、PyTorch 存储缓存、元数据缓存、NIXL agent 复用 (#61081, #60999, #60689, #60602)
- 缓存 `ActorHandle.__hash__`，修复 `__eq__` 正确性 (#61638)
- 缓存 `find_gcs_addresses` (#61065)
- 优化 worker listener 线程 (#61353)
- 从状态管理器 `get_all_node_info` 中消除 Python GCS 客户端 (#61232)
- 放宽 worker 线程数限制 (#62279)
- 每并发组按序排列 actor 任务，而非全局排序 (#61082)
- OOM killer 中优先杀死占用大内存的 worker (#60330)
- 限制指数退避尝试次数，防止整数溢出 (#61003)
- 替换已弃用的线程 API（`getName`/`setDaemon`） (#62153)
- 改进 `@ray.remote`/`@ray.method` 带 `num_returns` 的错误处理 (#59286)
- 在非生成器函数上将 `StopIteration` 转换为 `RuntimeError` (#60521)
- 为调度速率限制减缓任务启动显示警告 (#61004)
- 定期重新加载服务账号令牌；在同步服务器中使用 `AuthenticationValidator` (#60778, #60779)
- 移除 `local_mode` 支持 (#60647)
- 允许 `worker_process_setup_hook` 重入匹配 (#61473)
- 减小默认事件聚合器缓冲区大小以避免 OOM (#60826)
- 对只读 provider 抑制自动伸缩器动作日志 (#61732)
- 非 driver worker 惰性订阅节点变更 (#61118)
- 收紧导出符号白名单，防止非 Ray 符号泄漏 (#61298)
- 从 `memory_info` 近似 USS，而非调用 `memory_full_info` (#60000)
- 为 `NodeManager` 和 `InternalKVManager` 设置专用 IO context (#61002)
- 在 GCS `HandleUnregisterNode`/`HandleDrainNode` 时打印 gRPC peer 地址 (#62226, #62112)

### 🔨 修复

- 修复 pop worker 反复失败时任务卡住的问题 (#60104)
- 修复 `RAY_CGRAPH_overlap_gpu_communication` 的 `bool` 环境变量解析 (#61421)
- 修复负数 RUNNING 任务指标 (#62070)
- 修复 `OnNodeDead` 在所有节点死亡时销毁所有拥有的 actor (#60669)
- 修复取消头部任务后 actor 任务队列阻塞 (#60850)
- 修复多阶段 `TASK_PROFILE_EVENT` 聚合 (#61559)
- 修复 `WorkerPool::WarnAboutSize()` 中的重复计数 (#61246)
- 修复 `TaskLifecycleEvent.node_id` 使用发出节点而非执行节点 (#61478)
- 修复 GCS pubsub 中 `publisher_id` 类型不匹配 (#61518)
- 修复 dashboard `list_jobs` API 中 `dataclass.asdict` 对 `None` 的处理 (#61033)
- 修复 dashboard 节点头 API 死节点缓存 (#61185)
- 修复 dashboard 事件代理对无 HTTP scheme 事件的处理 (#60811)
- 修复 Ray Actor async 方法的类型 (#60682)
- 修复 k8s 异常期间自动伸缩器重试 (#60658)
- 修复 `ReadOnlyProvider.terminate()` 签名不匹配 (#62251)
- 修复 `OtlpGrpcMetricExporterOptions` 和指标导出器初始化中的环境变量竞争 (#61034, #61281)
- 在 `ray start` 期间版本不匹配时清理节点进程 (#61837)
- 在 `ray.init()` 时重试节点发现 (#61029)
- 确保 `Node._node_labels` 无论 `connect_only` 都初始化 (#61618)
- 避免 worker context 中可重入锁 (#61925)
- Java Local Mode 多 Actor 类型类型混淆 (#61858)
- 在头节点重启时从 `WrongClusterID` 恢复 (#60860)
- Azure 修复：销毁集群时不删除共享 MSI (#61811)
- 为 OpenTelemetry OTLP gRPC 导出器配置 TLS/mTLS (#60745)

---

## Dashboard

### 🎉 新特性

- Ray Data 仪表盘新增排队块指标 (#61716)
- 新增逻辑内存使用面板 (#60772)
- 新增按节点的运行任务，更新 Ray Data 活动任务面板 (#61641)
- Serve LLM Grafana 仪表盘新增 NIXL KV 传输指标 (#60819)
- 新增 GPU 功率和温度图表 (#60942)
- Grafana 仪表盘支持日志链接 (#60896)
- 支持自动伸缩器 v2 的集群级节点指标 (#60504)
- 新增 history server 的中间件代理 (#61295)
- 通过 `JobSubmissionClient` 将 `**kwargs` 转发至集群信息解析器 (#61902)

---

## Ray Wheels 和 Images

- 升级 Bazel 从 6.5.0 至 7.5.0 (#61601)
- 升级 `torch` 至 2.7.0+cu128 和 `torchvision` (#61328)
- 升级 `jackson-databind` 2.16.1 -> 2.18.6 (GHSA-72hv-8253-57qq) (#61808)
- CI 容器从 Ubuntu 20.04 升级至 22.04；Forge 从 clang-12 升级至 clang-14 (#61533, #61662)
- 新增 ray-llm/core-gpu 的 CUDA 13 镜像及发布测试配置 (#61497, #61637)
- 新增 Ray LLM 的 py312+CUDA 12.9 和 py312+CUDA 13 depsets (#61116, #61149, #61496)
- 将 TPU Docker 镜像添加至 CI 构建和发布管道 (#61172, #61173, #61174, #61175)
- Linux wheel 验证新增 Python 3.14 (#62127)
- Windows 基础构建修复 (#62415)
- 新增 `build-image.sh` 和本地 Docker 镜像构建器 CLI (#61042, #61338)
- 支持重复执行 `setup-dev.py` (#61357)

---

## 文档

- 新增 Ray History Server 用户指南 (#62030)
- 新增 `RAY_BACKEND_LOG_JSON` 环境变量文档 (#59962)
- 新增 Ray token 认证与 Kubernetes RBAC 用户指南 (#61644)
- 新增不受信任网络中 token 认证的警告 (#62248)
- KubeRay：预运行 deadline 文档、v1.6.0 引用、GKE/cgroups 交叉引用 (#61552, #61865, #62140)
- 使用 `RayCluster` 名称作为 `ServiceAccount` 名称进行 RBAC 认证 (#61785)
- 移除本地 `RayCluster` 中关于标签的过时说明 (#61719)
- 将 TPU 列为完全测试/支持 (#61634)
- 重构 `development.rst`，添加镜像构建、wheel 路径和交叉引用 (#61500, #61501, #61504, #61596)
- 移除 Placement Groups 的不正确警告 (#61176)
- 新增多 agent A2A 示例 (#61193)
- 新增对象溢出内部文档 (#60930)

---

## 分支增量特性（b184df65 之后）

以下为从 `b184df65` 到当前分支 `HEAD`（`fb87747ae1`）的增量变更，按模块分类整理。

### Ray Data

#### 🎉 新特性

- **Bloom Filter checkpoint 成员判定后端**：新增 Bloom Filter 作为第三种 checkpoint 去重后端（与默认的 sort+searchsorted 和 Roaring Bitmap 并列）。构建一次、广播一次架构，零假阴性、可配置假阳性率（默认 1e-6），移除对 checkpoint 数据集排序的依赖，使用 numpy+stdlib hashlib，支持 string/large_string/整数类型 id 列。`CheckpointConfig` 新增 `use_bloom_filter` 和 `bloom_filter_error_rate` 字段 (e530bb92)
- **`write_datasink` 新增 `filter_fn` 和 `filter_expr` 参数**：支持在写入时按行过滤，灵活控制哪些数据写入 sink (40146473)
- **`NoOpClusterAutoscaler`**：新增空操作集群自动伸缩器，用于禁用集群自动伸缩功能 (543aee52)
- **投机执行 Watchdog 补丁**：将 kling-ray 的 speculative-execution watchdog 猴子补丁移植到 `ray.data._internal.execution`，默认关闭（通过 `RAY_DATA_SPECULATION_ENABLED=1` 启用）。覆盖 5 个补丁点：ResourceManager 签名兼容、TaskCancelledError 优雅处理、MapOperator 提交追踪、输出队列过滤、调度循环 watchdog 钩子 (2a214175)
- **Checkpoint 去重支持**：RayData checkpoint 支持 `need_deduplication` 去重功能 (39e162fc)
- **Redis 存储新增 Roaring Bitmap**：checkpoint Redis 存储后端新增 Roaring Bitmap 支持，提升大规模 id 集合的内存效率和查询速度 (7d978005)

#### 💫 改进

- **ExecutionConfig 按 dataset_id 隔离存储**：多个 dataset 并发执行时，其 ExecutionConfig 条目之前共用同一 GCS key 导致互相覆盖，现扩展 key 包含 dataset_id，每个 dataset 拥有独立存储空间 (fb87747ae)
- **支持 Bazel 7.5.0 和 Ray 2.55.1 版本**：升级构建系统支持 (91942ca4)
- **Watchdog 重写精简版**（665→450 行）：移除 cancel-retry 机制，改用显式数据丢失预算。当 watchdog 放弃卡住任务时，直接丢弃 bundle 并传递 `WatchdogStalledError`，通过 `RAY_WATCHDOG_MAX_KILLED` 限制总数据丢失量。修复 4 个硬 bug：僵尸任务残留、stall 计数器重置、id() 回收误判、自杀计数干扰 (406970f5)
- **Watchdog 每 op 杀死比例上限**：新增 `RAY_WATCHDOG_PER_OP_KILL_RATIO`（默认 0.01=1%），按算子累积限制强制完成任务数，允许 watchdog 单周期批量杀死所有卡住任务加速尾端排空 (20939adb)
- **缓存反序列化 Arrow schema**：在 `BlockMetadataWithSchema` 中缓存 `bytes→pa.Schema`，将 1000 actor 宽 schema 场景下调度线程耗时从 60.2% 降至接近零 (3c60006e)
- **Map 任务返回使用标准 pickle**：将 metadata 先 `pickle.dumps` 再 yield，driver 端用 `pickle.loads`，减少 cloudpickle 协议开销，100 节点集群反序列化耗时降低约 80% (e6d240a3)
- **`_map_task` 仅 yield 第一个 schema**：减少重复 schema 传输，反序列化耗时降低约 25% (07baebd0)
- **基于堆的每节点 actor 排名**：将 actor pool 的全局 O(N\*M) 排名优化为每节点堆结构，每次选择 actor 降至 O(N) (64a5e8e9)
- **draining actor 用集合替代计数器**：将 `_pending_scale_down_count: int` 替换为 `_draining_actors: Set[ActorHandle]`，修复 quota 与身份混淆、actor 异常退出导致池过度缩减的问题 (2a7c40b0)
- **Data Dashboard 新增 Input/Output 指标**：新增 Input 指标面板，Output 使用 `rows_task_outputs_generated` 指标 (8168bfcb)
- **移除 Redis 快手基础设施依赖**：清理 checkpoint 模块中的 Redis 快手内部基础设施代码 (d0ebaa5c)

#### 🔨 修复

- **修复 GPU shuffle 输出乱序**：使用 `ShuffleStrategy.GPU_SHUFFLE` 配合 `.sort()` 时，多个 GPU actor 完成顺序随机导致结果乱序。引入 `ReorderingBundleQueue`，actor 在 block 元数据中嵌入 partition ID，按序释放 (1659cde7)
- **修复 checkpoint 去重中 >2GB string 列的 ArrowCapacityError**：`pc.unique` 在 `pa.string()` 列上受 int32 偏移 2GB 限制崩溃，修复方式为在 `pc.unique` 前将 `pa.string()` 转换为 `pa.large_string()` (86e71104)
- **修复 `write_datasink` 过滤后下游迭代器耗尽**：filter 功能移除无条件 `itertools.tee` 后导致 write() 消费迭代器后下游收到空迭代器，checkpoint 静默未写入、写入统计报 0 行/0 字节。恢复无条件 tee (db0765e1)
- **修复 Prometheus 查询超时导致 `/api/data/datasets` 返回 503**：异常处理器仅捕获 `ClientConnectorError`，Prometheus 不可达时连接超时抛出 `ConnectionTimeoutError`/`asyncio.TimeoutError` 未被捕获。扩展 except 子句 (11eacf87)
- **修复 `NoOpClusterAutoscaler.get_total_resources()` 返回 `inf` 导致 NaN**：原实现返回 `ExecutionResources.inf()`，`inf - inf` 导致资源预约计算 NaN。改为返回 `ray.cluster_resources()` (53791c62)

### Dashboard

#### 🎉 新特性

- **日志搜索新增 locate 模式**：`LogVirtualView` 新增 `searchMode` 属性（"locate" 默认 / "filter"）。Locate 模式显示全部行并高亮关键词、支持匹配导航（上一条/下一条）、显示匹配计数（如 "3 / 15"）(9ec99dee1)
- **Dashboard 优化**：多 Tab 日志查看器改进、任务表增强、节点行优化、日志管理器和服务层增强 (643a590d, f59bfb8f)
- **Pod 名称获取修复** (dceab7b0)
- **列宽自适应** (61d2e194)

### Ray Core

#### 💫 改进

- **Worker 端口随机化**：将 worker 进程端口从固定分配改为随机，避免端口冲突 (c9aeeeff)
- **指标过滤和默认导出值**：OpenTelemetry 指标记录器支持指标过滤，设置指标导出默认值 (d9fa65c2)

### 构建与发布

#### 💫 改进

- **可配置 CSC 上传路径和统一环境变量命名**：新增 `RAY_CSC_CUSTOM_PATH` 和 `RAY_CSC_RELEASE` 控制 wheel 上传目标；`RAY_DISABLE_EXTRA_CPP` 改为 `RAY_INSTALL_EXTRA_CPP`（正向命名，与 `RAY_INSTALL_JAVA` 一致），保留向后兼容 (aa4c7d79)

---

## 致谢

感谢所有为本版本做出贡献的开发者！

@justinyeh1995, @marwan116, @jddqd, @MkDev11, @mjd3, @XuQianJin-Stars, @elliot-barn, @DeborahOlaboye, @aaronscalene, @rayhhome, @ayushk7102, @bj-son, @nadongjun, @Daraan, @xinyuangui2, @Sparks0219, @justinvyu, @suppagoddo, @akyang-anyscale, @ambicuity, @Aydin-ab, @mickeyyliu, @MatthewCWeston, @vaishdho1, @jinbum-kim, @eicherseiji, @kouroshHakha, @karticam, @JasonLi1909, @ArturNiederfahrenhorst, @moktamd, @nrghosh, @dragongu, @andrewsykim, @mgchoi239, @ruoliu2, @harshit-anyscale, @Chong-Li, @pseudo-rnd-thoughts, @lee1258561, @khluu, @daiping8, @SolitaryThinker, @jonalee99, @yancanmao, @SohamRajpure, @rueian, @VitaliyEroshin, @Future-Outlier, @nehiljain, @JiangJiaWei1103, @Yicheng-Lu-llll, @KaisennHu, @jeffreywang-anyscale, @aslonnie, @alanwguo, @machichima, @limarkdcunha, @codope, @sampan-s-nayak, @kyuds, @thjung123, @abrarsheikh, @wingkitlee0, @preneond, @7ckingBest, @slfan1989, @win5923, @kaori-seasons, @israbbani, @andrew-anyscale, @zestze, @owenowenisme, @edoakes, @laysfire, @pushpavanthar, @tohtana, @leewyang, @liulehui, @Hyunoh-Yeo, @eureka0928, @ryanaoleary, @947132885, @Kunchd, @simonsays1980, @dpj135, @bveeramani, @raulchen, @Partth101, @dubin555, @richabanker, @bittoby, @sai-miduthuri, @RedGrey1993, @kamil-kaczmarek, @TimothySeah, @myandpr, @rishic3, @justinrmiller, @HassamSheikh, @chiayi, @petern48, @carolynwang, @MrKWatkins, @400Ping, @summaryzb, @peterxcli, @RocMarshal, @coqian, @yuhuan130, @ryankert01, @dayshah, @Anarion-zuo, @ZacAttack, @weimingdiit, @iamjustinhsu, @matthewdeng, @goutamvenkat-anyscale, @KeeProMise, @Sanskarzz, @yuchen-ecnu, @praneethkaturi, @rajeshg007, @ankur-anyscale, @Art0white, @xyuzh, @dancingactor, @MengjinYan, @dengkliu92, @alexeykudinkin
