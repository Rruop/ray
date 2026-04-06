  错误分析：ObjectReconstructionFailedError
  
  一、错误根因

  报错的关键信息：
  ray.exceptions.ObjectReconstructionFailedError: Failed to retrieve object bf2dc223c5fcc270b518845222473111eca2f6371a00000002000000
  ray::StreamingRepartition[num_rows_per_block=40]
  
  含义：StreamingRepartition 算子的某个上游输入 block (ObjectRef) 在被消费时已经从 object store 中丢失，且 Ray 尝试通过 lineage reconstruction（血缘重建） 重新生成它失败了。

  触发链路（基于源码 python/ray/exceptions.py:783-830 与 src/ray/core_worker/task_manager.cc:355-400）

  ObjectReconstructionFailed 有 8 种具体子原因（REASON_MESSAGES），最常见的几种：

  ┌────────────────────────────────────────────────────────────────┬──────────────────────────────────────────────────────────────────────────┬─────────────────────────────────────────────────────────────────────────────────────────────────────────┐
  │                           ErrorType                            │                                 触发条件                                 │                                           在你这场景的可能性                                            │
  ├────────────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED                 │ 生成该 object 的 task 已经重试到 max_retries 上限仍失败                  │ 最可能 —— 上游 task（生成那个 block 的 task）反复 OOM / worker crash / node 抢占，直至 max_retries 耗尽 │
  ├────────────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ OBJECT_UNRECONSTRUCTABLE_LINEAGE_EVICTED                       │ object 的 lineage 信息因内存压力被驱逐（RAY_max_lineage_bytes 默认 1GB） │ 中等可能 —— 大型 Data pipeline 容易触发                                                                 │
  ├────────────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ OBJECT_UNRECONSTRUCTABLE_PUT                                   │ object 由 ray.put() 创建，没有 lineage                                   │ 不太可能 —— Ray Data 不走 put                                                                           │
  ├────────────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ OBJECT_UNRECONSTRUCTABLE_RETRIES_DISABLED                      │ task 创建时 max_retries=0                                                │ 可能 —— Ray Data 默认配置下不明显                                                                       │
  ├────────────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ OBJECT_UNRECONSTRUCTABLE_REF_NOT_FOUND / BORROWED / LOCAL_MODE │ owner 找不到 / 跨 owner 借用 / 单机模式                                  │ 小概率                                                                                                  │
  └────────────────────────────────────────────────────────────────┴──────────────────────────────────────────────────────────────────────────┴─────────────────────────────────────────────────────────────────────────────────────────────────────────┘

  典型场景：上游某个 block 所在的 node（pid=4709, ip=10.48.35.39 是消费者侧）发生抢占/OOM/网络分区 → object 从 plasma 丢失 → core worker 触发 lineage reconstruction → 重新提交 producer task → producer task 也连续失败 → num_retries_left_ 归零 →
  OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED（task_manager.cc:365）。

  排查建议

  1. 先确认具体 reason：开启 RAY_record_ref_creation_sites=1 重跑，或在 raylet/core_worker 日志里 grep bf2dc223c5fcc270b518845222473111eca2f6371a00000002000000，定位 producer task 的失败原因（OOM / SIGKILL / node lost）。
  2. 看上游算子：StreamingRepartition 的输入是融合后的上游算子，多半是 Read/Map，看那个算子是否有大量重启。
  3. 看节点稳定性：10.48.35.39 是不是有抢占式实例 / 频繁失联。

  ---
  二、是否影响 Ray Data 的重试 / max_errored_blocks 行为
  
  答案：会被 max_errored_blocks 兜底吞掉，但不会触发"task 级重试"

  需要区分两层重试：

  Layer 1 — Ray Core 的 task 重试 / lineage reconstruction
  - 由 @ray.remote(max_retries=N) 控制
  - 由 core worker（C++）自动驱动 
  - 这一层已经失败了，所以才抛出 ObjectReconstructionFailedError。Ray Data 不会、也无法再次驱动 lineage reconstruction。

  Layer 2 — Ray Data 的 max_errored_blocks 兜底
  - 位置：python/ray/data/_internal/execution/streaming_executor_state.py:606-636
  - 逻辑：当下游消费 block 时抛任何异常（包括 ObjectReconstructionFailedError），都进入这个 except Exception as e 分支：

  except Exception as e:
      errored_blocks_per_op[state] += 1
      num_errored_blocks += 1
      should_ignore = (
          max_errored_blocks < 0
          or max_errored_blocks >= num_errored_blocks
      )
      if should_ignore:
          # 仅打日志，丢弃该 block，继续往下走
          logger.error(error_message, exc_info=e)
      else:
          raise e from None  # 终止整个 Dataset 执行

  所以你的报错路径是：上游 reconstruction 已彻底失败 → 下游 StreamingRepartition 在拉这个 ObjectRef 时拿到 ObjectReconstructionFailedError → 进入 except 分支 → max_errored_blocks 大 → should_ignore=True → 直接丢弃整个 block 的数据，继续执行。

  注意几个关键点：
  - Ray Data 不会"重试"这个 block。所谓 ignore 是真的把数据丢了（context.py:520 注释明确："Data in the failed blocks are dropped"）。
  - 默认值是 DEFAULT_MAX_ERRORED_BLOCKS = 0（context.py:225），即默认不容忍任何错误。
  - max_errored_blocks < 0 才代表无限。设成大正数也只是配额很多。

  ---
  三、设置很大的 max_errored_blocks 一直"重试"会怎样
  
  结论：不会"一直重试"，而是"一直丢数据继续往下跑"。具体后果分几条

  1. 静默数据丢失 ⚠️
  每个失败的 ObjectReconstructionFailed 对应一个 block（可能成千上万行），全部被丢弃。最终 Dataset 输出会少行，且不会报错。对训练 / 统计 / 写出场景是严重的正确性问题。
  2. 错误聚集而非自愈
  ObjectReconstructionFailed 的根因（节点抢占、OOM、lineage evicted）不会随着继续执行而消失。若上游持续不稳定，会持续触发，num_errored_blocks 单调累加，日志被刷屏（logger.error(..., exc_info=e) 每次都打完整堆栈）。
  3. 可能掩盖根因
  结论：不会"一直重试"，而是"一直丢数据继续往下跑"。具体后果分几条

  1. 静默数据丢失 ⚠️
  每个失败的 ObjectReconstructionFailed 对应一个 block（可能成千上万行），全部被丢弃。最终 Dataset 输出会少行，且不会报错。对训练 / 统计 / 写出场景是严重的正确性问题。
  2. 错误聚集而非自愈
  ObjectReconstructionFailed 的根因（节点抢占、OOM、lineage evicted）不会随着继续执行而消失。若上游持续不稳定，会持续触发，num_errored_blocks 单调累加，日志被刷屏（logger.error(...,
  ---
  三、设置很大的 max_errored_blocks 一直"重试"会怎样

  结论：不会"一直重试"，而是"一直丢数据继续往下跑"。具体后果分几条

  1. 静默数据丢失 ⚠️
  每个失败的 ObjectReconstructionFailed 对应一个 block（可能成千上万行），全部被丢弃。最终 Dataset 输出会少行，且不会报错。对训练 / 统计 / 写出场景是严重的正确性问题。
  2. 错误聚集而非自愈
  ObjectReconstructionFailed 的根因（节点抢占、OOM、lineage evicted）不会随着继续执行而消失。若上游持续不稳定，会持续触发，num_errored_blocks 单调累加，日志被刷屏（logger.error(..., exc_info=e) 每次都打完整堆栈）。
  3. 可能掩盖根因
  原本 max_errored_blocks=0 时第一次报错就 fail-fast，便于发现 OOM/抢占。改大之后表面成功，问题潜伏。
  4. 资源/进度反而可能变差
  Ray Core 仍会按 max_retries 重做 producer task，每次失败成本不低。如果根因是 OOM，重试只会再 OOM；如果是 lineage evicted，重建依赖会级联触发更多重建。
  5. 统计指标
  streaming_executor.py:281-284 在执行结束日志里会汇总 num_errored_blocks；stats.extra_metrics["num_errored_blocks"] 也会上报。结束时记得检查这个数。

  ---
  四、推荐做法

  按优先级：

  1. 先定位 reason：开 RAY_record_ref_creation_sites=1，搜 raylet/worker 日志确认是 MAX_ATTEMPTS_EXCEEDED / LINEAGE_EVICTED / 其它。
  2. 针对性修复：
    - OOM → 给上游 task 提高 memory 或减小 block size
    - 节点抢占 → 上游算子 @ray.remote(max_retries=N) 调大，或换稳定节点
    - lineage 驱逐 → 提高 RAY_max_lineage_bytes
  3. 保留 max_errored_blocks 较小值（如 0 或个位数）用于 fail-fast，不要靠它兜底来"绕过"系统性故障。
  4. 若 pipeline 业务上确实能接受部分丢数据（如脏数据清洗），用 max_errored_blocks 时同时监控 num_errored_blocks 指标设告警，避免静默丢大量数据。