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
  五、OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED 的深层代码链路

  在 OOM Kill 场景下，此错误的根本原因并非 "max_retries 耗尽"，而是 task spec 已从 submissible_tasks_ 中被移除。
  详细代码链路分析见：docs/05-内存与OOM/Ray-OOM-Kill-Error-Object-UNRECONSTRUCTABLE完整代码链路分析.md

  5.1 三种导致 task 从 submissible_tasks_ 中移除的路径

  路径 A（最常见）：FailPendingTask 直接删除
  - 触发条件：OOM Kill 时 should_retry=false（owner group 下只有 1 个 worker）
  - 代码：task_manager.cc:1293 submissible_tasks_.erase(it)
  - 链路：Raylet OOM Kill → should_retry=false → fail_immediately=true
          → FailOrRetryPendingTask 跳过 retry → FailPendingTask → erase task

  路径 B：reconstructable_return_ids_ 为空
  - 触发条件：task 完成后，所有 plasma return object 都不再被引用
  - 代码：task_manager.cc:1063 submissible_tasks_.erase(it)
  - 对 streaming generator：已 yield 的 block 被下游消费完后，reconstructable_return_ids_ 逐渐清空

  路径 C：EvictLineage 导致
  - 触发条件：total_lineage_footpoint_bytes_ > max_lineage_bytes_ (默认 1GB)
  - 代码：reference_counter.cc:823 EvictLineage → ReleaseLineageReferences
          → on_lineage_released_ 回调 → RemoveLineageReference
          → reconstructable_return_ids_ 清空 → submissible_tasks_.erase

  5.2 ResubmitTask 中检测到 task 不存在

  ```cpp
  // task_manager.cc:353-365
  std::optional<rpc::ErrorType> TaskManager::ResubmitTask(
      const TaskID &task_id, std::vector<ObjectID> *task_deps) {
      auto it = submissible_tasks_.find(task_id);
      if (it == submissible_tasks_.end()) {
          return rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED;
      }
      // ...
  }
  ```

  5.3 GroupByOwnerIdWorkerKillingPolicy 的 should_retry 判断

  ```cpp
  // worker_killing_policy_group_by_owner.cc:160-162
  bool should_retry =
      selected_group.GetAllWorkers().size() > 1 && selected_group.IsRetriable();
  //                     ^^^^^^^^^^^^^^^^^^^^^^^^
  //                     关键条件：同 owner group 下必须有 >1 个 worker
  ```

  当 ReadArrowJSON->SplitBlocks(7) 在节点上只有 1 个 worker 时：
  - group size = 1 → should_retry = false
  - fail_immediately = true
  - FailPendingTask 直接删除 task → 后续 reconstruction 必定失败

  5.4 INELIGIBLE_LINEAGE_EVICTED 与 OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED 的关系

  两者是不同的 ErrorType，但都表示 reconstruction 永久不可行：

  ```cpp
  // reference_counter_interface.h
  ToErrorType(LineageReconstructionEligibility):
    INELIGIBLE_LINEAGE_EVICTED → OBJECT_UNRECONSTRUCTABLE_LINEAGE_EVICTED
    INELIGIBLE_MAX_ATTEMPTS_EXCEEDED → OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED
  ```

  区别：
  - LINEAGE_EVICTED：task spec 因 EvictLineage（lineage 内存超 1GB）被强制淘汰
  - MAX_ATTEMPTS_EXCEEDED：task spec 因 FailPendingTask 或 reconstructable_return_ids_ 为空被删除，ResubmitTask 在 submissible_tasks_ 中找不到

  5.5 recovery_failure_callback_ 完整代码路径

  ObjectRecoveryManager 构造时传入回调（core_worker_process.cc:653-668）：

  ```cpp
  auto object_recovery_manager = std::make_unique<ObjectRecoveryManager>(
      ...,
      [this](const ObjectID &object_id, rpc::ErrorType reason, bool pin_object) {
          auto core_worker = GetCoreWorker();
          // 将 error object 写入 plasma / in_memory_store
          core_worker->Put(RayObject(reason), {}, object_id, pin_object);
      });
  ```

  触发点有三处（object_recovery_manager.cc）：
  1. 第 146-150 行：lineage eligibility 不满足 → recovery_failure_callback_(object_id, error_type, true)
  2. 第 170-178 行：task 的依赖恢复失败 → recovery_failure_callback_(dep, error, false)
  3. 第 180-187 行：ResubmitTask 返回 error → recovery_failure_callback_(object_id, error, true)

  Put 最终调用 PutInLocalPlasmaStore（core_worker.cc:992-1036），走 plasma_store_provider_->Put()：
  - 如果 plasma 中 object 已存在且 sealed → ObjectExists → 不覆盖
  - 如果 plasma 中 object 已丢失 → Create 成功 → error object 写入

  关键结论：在 OOM Kill → FailPendingTask 路径中，OUT_OF_MEMORY error object 已先写入 plasma。
  后续 reconstruction 失败时 recovery_failure_callback_ 试图写入 OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED error，
  但因 plasma 写保护（ObjectExists），后者不会覆盖前者。
  Python 端 ray.get() 读到的仍是原始的 OutOfMemoryError。

  5.6 Error Block 不会跨算子直接传播

  Ray Data 中 error block 不会从上游算子传递到下游算子。

  流程：ReadArrowJSON OOM kill → streaming generator 提前终止
  → on_data_ready() 中 _next_sync() 返回 StopIteration
  → ray.get(error_block_ref) → 抛 OutOfMemoryError
  → streaming_executor_state.py:521 except 捕获
  → max_errored_blocks=-1 → 忽略 → 没有 RefBundle 放入 output_queue
  → 下游算子的 add_input() 不会收到这个 block → error 就到此为止

  所以 QGPreprocessMapper 的报错只可能来自 lineage reconstruction 阶段的级联恢复失败。

  5.7 级联恢复的完整代码链路

  触发起点：节点下线 → GCS 通知 Driver
  → core_worker.cc:755 "Node failure. All objects pinned on that node will be lost"
  → reference_counter->ResetObjectsOnRemovedNode(node_id)
  → 遍历所有 pinned_at == node_id 的 object
  → objects_to_recover_.push_back(object_id)

  周期性驱动（每 100ms）：
  → core_worker.cc:470-494 FlushObjectsToRecover → RecoverObject(object_id)

  级联恢复核心（object_recovery_manager.cc:136-187）：
  → ReconstructObject(object_id)
     → ResubmitTask(task_id, &task_deps)  // task_deps 收集该 task 所有输入依赖
        → 成功: for dep in task_deps: RecoverObject(dep)  // ★ 递归！
        → 失败: recovery_failure_callback_(object_id, error, true)

  本场景的级联路径：
  1. RecoverObject(QGPreprocess 输入 block) → ReconstructObject
     → ResubmitTask(StreamingRepartition task, &deps=[ReadArrowJSON blocks])
     → ★ 级联: RecoverObject(ReadArrowJSON block) → ReconstructObject
        → ResubmitTask(ReadArrowJSON task) → submissible_tasks_ 找不到
           （FailPendingTask 已删除 task spec，因 should_retry=false, group size=1）
        → 返回 OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED
        → recovery_failure_callback_ 写入 error object
  2. StreamingRepartition 重试执行时发现输入是 error → raise_if_dependency_failed
     → QGPreprocessMapper ray.get() → 拿到 RayTaskError(OutOfMemoryError)

  日志证据：
  ```
  Resubmitting task that produced lost plasma object, attempt #4:
    task_name=ReadArrowJSON->SplitBlocks(7)
  Resubmitting task that produced lost plasma object, attempt #3:
    task_name=StreamingRepartition
  ```
  → StreamingRepartition #3 是恢复 QGPreprocess 输入时触发
  → ReadArrowJSON #4 是恢复 StreamingRepartition 输入时递归触发

  5.8 定位排查步骤

  ```bash
  # 1. Driver 日志 - 确认节点下线
  grep "Node failure.*All objects pinned" /tmp/ray/session_latest/logs/python-core-driver-*.log

  # 2. Driver 日志 - 确认级联恢复触发
  grep "Resubmitting task that produced lost plasma object" /tmp/ray/session_latest/logs/python-core-driver-*.log

  # 3. Driver 日志 - 确认恢复失败
  grep "Cannot recover object\|OBJECT_UNRECONSTRUCTABLE" /tmp/ray/session_latest/logs/python-core-driver-*.log

  # 4. Driver 日志 - 确认 OOM should_retry
  grep "Fail immediately.*true\|OUT_OF_MEMORY" /tmp/ray/session_latest/logs/python-core-driver-*.log

  # 5. Driver 日志 - 统计 attempt 分布
  grep "attempt_number" /tmp/ray/session_latest/logs/python-core-driver-*.log | \
    grep -oP "attempt_number=\d+" | sort | uniq -c | sort -rn

  # 6. Worker raylet 日志 - OOM kill 决策
  grep "Killing\|should_retry\|GroupByOwnerId" /tmp/ray/session_latest/logs/raylet.out
  ```

  ---

  六、推荐做法