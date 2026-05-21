"""Watchdog-only pipeline stall detection for Ray Data (opt-in monkey-patch).

This module ports the ``patch_watchdog_block.py`` runtime patch (previously
shipped via kling-ray/utils) into the Ray tree so users no longer need to
side-load it from an external repo.

Behavior is unchanged from the original patch but the module is **disabled by
default**. To enable it, export ``RAY_DATA_SPECULATION_ENABLED=1`` before
``import ray.data``; the patch is then applied automatically from
``ray.data.__init__``. The patch can also be applied manually by calling
:func:`apply` (which is idempotent under the ``ENABLED`` flag).

Detects operators that make zero progress (completed count and active count
both unchanged) for longer than ``RAY_WATCHDOG_STALL_S`` seconds, then:

- First occurrence: cancel the oldest stuck task and resubmit to a new node.
- Subsequent occurrences (retry exhausted): force-cancel the task so the
  pipeline can continue (data loss counted via cancelled output filtering).

Patches applied (when enabled):

- Patch 0: ``ResourceManager.update_usages`` compatibility fix (Kuaishou Ray
  2.54.0 callers pass ``update_op_state`` which upstream may not accept).
- Patch 1: ``TaskCancelledError`` graceful handling in
  ``DataOpTask.on_data_ready``.
- Patch 2: ``_submit_data_task`` wrapper for task/bundle tracking + OOM retry
  dedup.
- Patch 3: per-operator ``_output_queue.add()`` filter for cancelled task
  output.
- Patch 4: ``StreamingExecutor._scheduling_loop_step`` hook that runs the
  watchdog every ``RAY_DETECTION_INTERVAL_S`` seconds.

Environment variables:

``RAY_DATA_SPECULATION_ENABLED`` (default ``0``)
    Master switch for speculative execution (cancel-retry of stalled tasks).
    ``apply()`` is a no-op when this is ``0``.
``RAY_DETECTION_INTERVAL_S`` (default ``5``)
    How often (seconds) the watchdog inspects each operator.
``RAY_WATCHDOG_STALL_S`` (default ``600``)
    Stall threshold (no completed/active change) before intervention.
``RAY_WATCHDOG_MAX_RETRY`` (default ``1``)
    Per-bundle cancel_retry budget before force-cancel.
``RAY_WATCHDOG_MIN_COMPLETION_RATIO`` (default ``0.95``)
    Watchdog only fires once completed/(completed+active) reaches this ratio,
    to avoid early-stage false positives.
``RAY_ACTOR_STALE_LOG_INTERVAL`` (default ``100``)
    ActorPool stale-output log throttling interval.
``RAY_WATCHDOG_DRAIN_PHANTOM_QUEUE`` (default ``1``)
    When set, drains leftover ``_output_queue`` bundles after a phantom stall
    is detected (active=0 but queue not empty). Drained data is dropped.
``RAY_WATCHDOG_PHANTOM_STALL_S`` (default = ``RAY_WATCHDOG_STALL_S``)
    Stall threshold specific to phantom-queue deadlocks.
"""

import inspect
import logging
import os
import time
from typing import Dict, Set, Tuple

logger = logging.getLogger(__name__)

ENABLED = int(os.environ.get("RAY_DATA_SPECULATION_ENABLED", "0")) > 0
DETECTION_INTERVAL_S = float(os.environ.get("RAY_DETECTION_INTERVAL_S", "5"))
# ── Watchdog ──
# completed 和 active 均无变化超过此时长，视为停滞，触发 cancel_retry 或放弃。
WATCHDOG_STALL_S = float(os.environ.get("RAY_WATCHDOG_STALL_S", "600"))
# 同一 bundle 最多 cancel_retry 几次，超过后强制 cancel（静默丢弃，pipeline 继续）。
WATCHDOG_MAX_RETRY = int(os.environ.get("RAY_WATCHDOG_MAX_RETRY", "1"))
# watchdog 只在 completed/(completed+active) >= 此比例时才触发，避免任务前期误干预。
WATCHDOG_MIN_COMPLETION_RATIO = float(
    os.environ.get("RAY_WATCHDOG_MIN_COMPLETION_RATIO", "0.95")
)
# ActorPool stale 输出日志节流间隔
ACTOR_STALE_LOG_INTERVAL = int(os.environ.get("RAY_ACTOR_STALE_LOG_INTERVAL", "100"))

# ── Phantom queue 死锁检测 ──
# 当 active_count==0 但 op._output_queue 仍有 bundle 且无 progress 时，
# 视为 phantom 死锁。这通常发生在上游 OOM 后 _output_queue 中存有遗留 bundle
# 而 _data_tasks 已空，导致 op.completed() 永远 False、下游永远拿不到完成信号。
# 处理：mark_execution_finished() + 可选 drain 残留队列（drain 接受数据丢失）。
DRAIN_PHANTOM_QUEUE = (
    int(os.environ.get("RAY_WATCHDOG_DRAIN_PHANTOM_QUEUE", "1")) > 0
)
PHANTOM_STALL_S = float(
    os.environ.get("RAY_WATCHDOG_PHANTOM_STALL_S", str(WATCHDOG_STALL_S))
)

# Per-operator tracking: op_id -> {task_idx: (start_time, bundle)}
_task_metadata: Dict[int, Dict[int, Tuple[float, object]]] = {}
_total_submitted_by_op: Dict[int, int] = {}
_original_submitted_by_op: Dict[int, int] = {}
_completed_count_by_op: Dict[int, int] = {}
_last_detection_time: Dict[int, float] = {}
# TaskPool: cancelled tasks (output filtered at output_queue)
_cancelled_tasks_by_op: Dict[int, Set[int]] = {}
# ActorPool: stale tasks (output filtered at output_queue)
_stale_tasks_by_op: Dict[int, Set[int]] = {}
# Sub-task indices — exempt from OOM retry bundle dedup
_sub_task_indices: Dict[int, Set[int]] = {}
# Bundle-level dedup: prevents Ray OOM retry producing duplicate output
# alongside already-submitted cancel_retry tasks.
_cancelled_bundles_by_op: Dict[int, Set[int]] = {}
_queue_patched: Dict[int, bool] = {}
_op_is_actor_pool: Dict[int, bool] = {}
_stale_discard_count: Dict[int, int] = {}
# Watchdog progress snapshot: op_id -> (last_completed, last_active, last_progress_time)
_watchdog_snapshot: Dict[int, Tuple[int, int, float]] = {}
# Bundle retry count for watchdog: op_id -> {bundle_id: retry_count}
_bundle_retry_count: Dict[int, Dict[int, int]] = {}

_metrics = {
    "cancellations_handled": 0,
    "output_discarded": 0,
    "watchdog_cancel_retry": 0,
    "watchdog_force_error": 0,
    "phantom_stall_detected": 0,
    "phantom_blocks_drained": 0,
    "phantom_mark_finished": 0,
}

_applied = False


def _count_phantom_blocks(op):
    oq = getattr(op, "_output_queue", None)
    if oq is None:
        return False, 0
    try:
        has_next = bool(oq.has_next())
    except Exception:
        has_next = False
    count = 0
    try:
        task_outputs = getattr(oq, "_task_outputs", None)
        if task_outputs is not None:
            count = sum(len(dq) for dq in task_outputs.values())
        else:
            q = getattr(oq, "_queue", None)
            if q is not None:
                count = len(q)
    except Exception:
        count = 0
    return has_next, count


def apply():
    """Install the watchdog monkey-patches.

    No-op when ``RAY_DATA_SPECULATION_ENABLED`` is unset/0 or when already
    applied in this process.
    """
    global _applied
    if not ENABLED:
        logger.info(
            "[patch_dbs] Disabled by RAY_DATA_SPECULATION_ENABLED=0 (default)"
        )
        return
    if _applied:
        logger.info("[patch_dbs] Already applied, skipping")
        return

    import ray
    import ray.exceptions
    from ray.data._internal.execution.operators.map_operator import MapOperator
    from ray.data._internal.execution.operators.actor_pool_map_operator import (
        ActorPoolMapOperator,
    )
    from ray.data._internal.execution.interfaces.physical_operator import DataOpTask
    from ray.data._internal.execution.resource_manager import ResourceManager

    # =========================================================================
    # Patch 0: ResourceManager.update_usages compatibility fix
    # streaming_executor calls update_usages(update_op_state=False) but the
    # method may not accept that argument on Kuaishou Ray 2.54.0.
    # Skipped automatically when patch_interleave_dispatch has already added it.
    # =========================================================================
    _orig_update_usages = ResourceManager.update_usages
    if "update_op_state" not in inspect.signature(_orig_update_usages).parameters:
        def _patched_update_usages(self, update_op_state=False, **kwargs):
            return _orig_update_usages(self, **kwargs)
        ResourceManager.update_usages = _patched_update_usages
        logger.info("[patch_dbs] Patched ResourceManager.update_usages")
    else:
        logger.info("[patch_dbs] ResourceManager.update_usages already patched")

    # =========================================================================
    # Patch 1: Graceful TaskCancelledError handling in DataOpTask.on_data_ready
    # Without this, a cancelled task's generator raising TaskCancelledError
    # propagates to process_completed_tasks and counts as an error block.
    # =========================================================================
    _orig_on_data_ready = DataOpTask.on_data_ready

    def _patched_on_data_ready(self, max_bytes_to_read=None):
        try:
            return _orig_on_data_ready(self, max_bytes_to_read)
        except ray.exceptions.TaskCancelledError:
            _metrics["cancellations_handled"] += 1
            logger.info(
                f"[patch_dbs] TaskCancelledError handled gracefully "
                f"(task_index={self.task_index() if hasattr(self, 'task_index') else '?'})"
            )
            if hasattr(self, "_has_finished") and not self._has_finished:
                if hasattr(self, "_task_done_callback"):
                    self._task_done_callback(None)
                self._has_finished = True
            return 0

    DataOpTask.on_data_ready = _patched_on_data_ready
    logger.info("[patch_dbs] Patched DataOpTask.on_data_ready")

    # =========================================================================
    # Patch 2: Wrap _submit_data_task to track task metadata.
    # Also detects Ray OOM retry tasks (new idx, same bundle object) and
    # auto-cancels them to prevent duplicate output from cancel_retry bundles.
    # Sub-tasks (in _sub_task_indices) are exempt from bundle dedup.
    # =========================================================================
    _orig_submit_data_task = MapOperator._submit_data_task

    def _patched_submit_data_task(self, gen, inputs, task_done_callback=None):
        op_id = id(self)
        task_idx = self._next_data_task_idx
        now = time.time()

        if op_id not in _task_metadata:
            _task_metadata[op_id] = {}
            _completed_count_by_op[op_id] = 0
            _total_submitted_by_op[op_id] = 0
            _original_submitted_by_op[op_id] = 0
            _last_detection_time[op_id] = 0.0
            _cancelled_tasks_by_op[op_id] = set()
            _stale_tasks_by_op[op_id] = set()
            _sub_task_indices[op_id] = set()
            _cancelled_bundles_by_op[op_id] = set()
            _queue_patched[op_id] = False
            _op_is_actor_pool[op_id] = isinstance(self, ActorPoolMapOperator)
            _stale_discard_count[op_id] = 0
            _watchdog_snapshot[op_id] = (0, 0, now)
            _bundle_retry_count[op_id] = {}

        _task_metadata[op_id][task_idx] = (now, inputs)
        _total_submitted_by_op[op_id] += 1

        if task_idx not in _sub_task_indices.get(op_id, set()):
            _original_submitted_by_op[op_id] += 1

        # OOM retry dedup: same bundle object resubmitted under a new task_idx
        bundle_id = id(inputs)
        if (bundle_id in _cancelled_bundles_by_op.get(op_id, set())
                and task_idx not in _sub_task_indices.get(op_id, set())):
            _cancelled_tasks_by_op.setdefault(op_id, set()).add(task_idx)
            logger.info(
                f"[patch_dbs] OOM retry detected: task {task_idx} in {self.name} "
                f"reuses cancelled bundle {bundle_id}, auto-cancelling"
            )

        if not _queue_patched.get(op_id, False) and self._output_queue is not None:
            _patch_output_queue(self, op_id)
            _queue_patched[op_id] = True

        _orig_submit_data_task(self, gen, inputs, task_done_callback=task_done_callback)

    MapOperator._submit_data_task = _patched_submit_data_task
    logger.info("[patch_dbs] Wrapped MapOperator._submit_data_task")

    # =========================================================================
    # Patch 3: per-operator _output_queue.add() filter.
    # Discards output from cancelled (TaskPool) and stale (ActorPool) tasks.
    # =========================================================================
    def _patch_output_queue(op, op_id):
        original_add = op._output_queue.add
        is_actor = _op_is_actor_pool.get(op_id, False)

        def patched_add(output, key=None):
            if key is not None:
                if key in _cancelled_tasks_by_op.get(op_id, set()):
                    _metrics["output_discarded"] += 1
                    logger.info(
                        f"[patch_dbs] Discarded output from cancelled task {key} "
                        f"in {op.name}"
                    )
                    return
                if is_actor and key in _stale_tasks_by_op.get(op_id, set()):
                    _metrics["output_discarded"] += 1
                    count = _stale_discard_count.get(op_id, 0) + 1
                    _stale_discard_count[op_id] = count
                    if count % ACTOR_STALE_LOG_INTERVAL == 1:
                        logger.info(
                            f"[patch_dbs] Discarded stale output from task {key} "
                            f"in {op.name} (total stale discards: {count})"
                        )
                    return
            return original_add(output, key=key)

        op._output_queue.add = patched_add
        logger.info(f"[patch_dbs] Patched output queue for {op.name} "
                    f"(actor_pool={is_actor})")

    # =========================================================================
    # Watchdog: per-operator stall detection + cancel_retry / force_error
    # =========================================================================
    def _should_monitor(op):
        from ray.data._internal.execution.operators.task_pool_map_operator import (
            TaskPoolMapOperator,
        )
        skip = ("ReadRange", "Project", "Aggregate", "Write")
        if isinstance(op, ActorPoolMapOperator):
            return not op.name.startswith(skip)
        if isinstance(op, TaskPoolMapOperator):
            return not op.name.startswith(skip)
        return False

    def _check_watchdog(op, op_id, now):
        current_completed = _completed_count_by_op.get(op_id, 0)
        active_count = len(op._data_tasks)
        phantom_has_next, phantom_count = _count_phantom_blocks(op)

        last_completed, last_active, last_progress_time = _watchdog_snapshot.get(
            op_id, (0, 0, now)
        )

        has_progress = (current_completed > last_completed) or (active_count < last_active)
        if has_progress:
            _watchdog_snapshot[op_id] = (current_completed, active_count, now)
            return

        if active_count == 0:
            if not phantom_has_next and phantom_count == 0:
                _watchdog_snapshot[op_id] = (current_completed, active_count, now)
                return
            stalled_s = now - last_progress_time
            if stalled_s < PHANTOM_STALL_S:
                return
            _handle_phantom_stall(op, op_id, stalled_s, phantom_has_next, phantom_count)
            _watchdog_snapshot[op_id] = (current_completed, active_count, now)
            return

        stalled_s = now - last_progress_time
        if stalled_s < WATCHDOG_STALL_S:
            return

        total = current_completed + active_count
        completion_ratio = current_completed / total if total > 0 else 0.0
        if completion_ratio < WATCHDOG_MIN_COMPLETION_RATIO:
            return

        cancelled = _cancelled_tasks_by_op.get(op_id, set())
        stale = _stale_tasks_by_op.get(op_id, set())
        sub = _sub_task_indices.get(op_id, set())
        task_metas = _task_metadata.get(op_id, {})

        original_candidates = [
            (task_idx, start_time, bundle)
            for task_idx, (start_time, bundle) in task_metas.items()
            if task_idx in op._data_tasks
            and task_idx not in cancelled
            and task_idx not in stale
            and task_idx not in sub
        ]
        sub_candidates = [
            (task_idx, start_time, bundle)
            for task_idx, (start_time, bundle) in task_metas.items()
            if task_idx in op._data_tasks
            and task_idx not in cancelled
            and task_idx not in stale
            and task_idx in sub
        ]
        if not original_candidates and not sub_candidates:
            _watchdog_snapshot[op_id] = (current_completed, active_count, now)
            return

        is_actor = _op_is_actor_pool.get(op_id, False)

        logger.warning(
            f"[patch_dbs] WATCHDOG: {op.name} stalled {stalled_s:.0f}s "
            f"(completed={current_completed}, active={active_count}), "
            f"original={len(original_candidates)} sub={len(sub_candidates)} stuck, "
            f"type={'ActorPool' if is_actor else 'TaskPool'}"
        )

        _watchdog_snapshot[op_id] = (current_completed, active_count, now)

        for task_idx, start_time, bundle in sorted(original_candidates, key=lambda x: x[1]):
            bundle_id = id(bundle)
            retry_count = _bundle_retry_count.get(op_id, {}).get(bundle_id, 0)
            _bundle_retry_count.setdefault(op_id, {})[bundle_id] = retry_count + 1

            if retry_count < WATCHDOG_MAX_RETRY:
                logger.warning(
                    f"[patch_dbs] WATCHDOG: cancel_retry task {task_idx} in {op.name} "
                    f"runtime={now - start_time:.0f}s, "
                    f"bundle_retry={retry_count + 1}/{WATCHDOG_MAX_RETRY}"
                )
                if not is_actor:
                    _handle_taskpool_cancel_retry(op, op_id, task_idx, bundle)
                else:
                    _handle_actorpool_cancel_retry(op, op_id, task_idx, bundle)
                _metrics["watchdog_cancel_retry"] += 1
            else:
                _force_error_task(op, op_id, task_idx, bundle, stalled_s)

        for task_idx, start_time, bundle in sorted(sub_candidates, key=lambda x: x[1]):
            logger.warning(
                f"[patch_dbs] WATCHDOG: force_error sub-task {task_idx} in {op.name} "
                f"runtime={now - start_time:.0f}s (retry already exhausted)"
            )
            _force_error_task(op, op_id, task_idx, bundle, stalled_s)

    def _handle_taskpool_cancel_retry(op, op_id, task_idx, bundle):
        if task_idx in _cancelled_tasks_by_op.get(op_id, set()):
            return
        try:
            op._try_schedule_task(bundle, strict=True)
            retry_idx = op._next_data_task_idx - 1
        except Exception as e:
            logger.error(
                f"[patch_dbs] cancel_retry: failed to resubmit bundle for "
                f"task {task_idx}: {e}"
            )
            return
        _sub_task_indices.setdefault(op_id, set()).add(retry_idx)
        _cancelled_tasks_by_op.setdefault(op_id, set()).add(task_idx)
        _cancelled_bundles_by_op.setdefault(op_id, set()).add(id(bundle))
        data_task = op._data_tasks.get(task_idx)
        if data_task is not None:
            try:
                if hasattr(data_task, "_cancel"):
                    data_task._cancel(force=False)
                elif hasattr(data_task, "cancel"):
                    data_task.cancel()
                logger.info(
                    f"[patch_dbs] cancel_retry: cancelled task {task_idx}, "
                    f"resubmitted as task {retry_idx} in {op.name}"
                )
            except Exception as e:
                logger.error(
                    f"[patch_dbs] cancel_retry: error cancelling task {task_idx}: {e}"
                )

    def _force_error_task(op, op_id, task_idx, bundle, stalled_s):
        is_actor = _op_is_actor_pool.get(op_id, False)
        logger.warning(
            f"[patch_dbs] WATCHDOG: giving up task {task_idx} in {op.name} "
            f"after {stalled_s:.0f}s stall, forcing cancel to unblock pipeline"
        )
        _cancelled_bundles_by_op.setdefault(op_id, set()).add(id(bundle))
        if is_actor:
            _stale_tasks_by_op.setdefault(op_id, set()).add(task_idx)
            data_task = op._data_tasks.get(task_idx)
            if data_task is not None:
                try:
                    data_task._task_done_callback(None)
                    data_task._has_finished = True
                    logger.warning(
                        f"[patch_dbs] WATCHDOG: force-evicted ActorPool task "
                        f"{task_idx} from _data_tasks in {op.name}"
                    )
                except Exception as e:
                    logger.error(
                        f"[patch_dbs] WATCHDOG: error evicting ActorPool task "
                        f"{task_idx}: {e}"
                    )
        else:
            _cancelled_tasks_by_op.setdefault(op_id, set()).add(task_idx)
            data_task = op._data_tasks.get(task_idx)
            if data_task is not None:
                try:
                    if hasattr(data_task, "_cancel"):
                        data_task._cancel(force=True)
                    elif hasattr(data_task, "cancel"):
                        data_task.cancel()
                except Exception as e:
                    logger.error(
                        f"[patch_dbs] WATCHDOG: error cancelling task {task_idx}: {e}"
                    )
        _metrics["watchdog_force_error"] += 1

    def _handle_phantom_stall(op, op_id, stalled_s, phantom_has_next, phantom_count):
        _metrics["phantom_stall_detected"] += 1
        logger.warning(
            f"[patch_dbs] WATCHDOG PHANTOM: {op.name} stalled {stalled_s:.0f}s "
            f"with active=0 but phantom_blocks={phantom_count} "
            f"(has_next={phantom_has_next}). Forcing completion to unblock pipeline."
        )
        if hasattr(op, "mark_execution_finished"):
            try:
                op.mark_execution_finished()
                _metrics["phantom_mark_finished"] += 1
                logger.warning(
                    f"[patch_dbs] WATCHDOG PHANTOM: marked {op.name} execution_finished"
                )
            except Exception as e:
                logger.error(
                    f"[patch_dbs] WATCHDOG PHANTOM: mark_execution_finished failed "
                    f"for {op.name}: {e}"
                )

        if not DRAIN_PHANTOM_QUEUE:
            return

        oq = getattr(op, "_output_queue", None)
        if oq is None:
            return

        drained = 0
        max_iters = phantom_count + 1024
        try:
            while max_iters > 0 and oq.has_next():
                bundle = oq.get_next()
                try:
                    if hasattr(bundle, "destroy_if_owned"):
                        bundle.destroy_if_owned()
                except Exception:
                    pass
                drained += 1
                max_iters -= 1
        except Exception as e:
            logger.error(
                f"[patch_dbs] WATCHDOG PHANTOM: drain error in {op.name} "
                f"after {drained} bundles: {e}"
            )
        _metrics["phantom_blocks_drained"] += drained
        logger.warning(
            f"[patch_dbs] WATCHDOG PHANTOM: drained {drained} phantom bundles "
            f"from {op.name} (data loss accepted to break deadlock)"
        )

    def _handle_actorpool_cancel_retry(op, op_id, task_idx, bundle):
        if task_idx in _stale_tasks_by_op.get(op_id, set()):
            return
        try:
            op._try_schedule_task(bundle, strict=False)
            retry_idx = op._next_data_task_idx - 1
        except Exception as e:
            logger.error(
                f"[patch_dbs] actorpool_cancel_retry: failed to resubmit bundle "
                f"for task {task_idx}: {e}"
            )
            return
        _sub_task_indices.setdefault(op_id, set()).add(retry_idx)
        _stale_tasks_by_op.setdefault(op_id, set()).add(task_idx)
        _cancelled_bundles_by_op.setdefault(op_id, set()).add(id(bundle))
        logger.warning(
            f"[patch_dbs] WATCHDOG: ActorPool task {task_idx} in {op.name} "
            f"marked stale, bundle resubmitted as task {retry_idx}"
        )

    # =========================================================================
    # Patch 4: Hook into scheduling loop — run watchdog each cycle
    # =========================================================================
    from ray.data._internal.execution.streaming_executor import StreamingExecutor

    _orig_step = StreamingExecutor._scheduling_loop_step
    _step_takes_topology = "topology" in inspect.signature(_orig_step).parameters

    if _step_takes_topology:
        def _patched_step(self, topology):  # pyright: ignore[reportRedeclaration]
            now = time.time()
            for op, op_state in topology.items():
                if not _should_monitor(op):
                    continue
                op_id = id(op)
                if now - _last_detection_time.get(op_id, 0) < DETECTION_INTERVAL_S:
                    continue
                _last_detection_time[op_id] = now

                # Collect completed tasks into _completed_count_by_op
                cancelled = _cancelled_tasks_by_op.get(op_id, set())
                stale = _stale_tasks_by_op.get(op_id, set())
                sub = _sub_task_indices.get(op_id, set())
                for task_idx, (start_time, bundle) in list(
                    _task_metadata.get(op_id, {}).items()
                ):
                    if task_idx in cancelled or task_idx in stale or task_idx in sub:
                        continue
                    if task_idx not in op._data_tasks:
                        meta = _task_metadata.get(op_id, {}).pop(task_idx, None)
                        if meta is not None:
                            _completed_count_by_op[op_id] = (
                                _completed_count_by_op.get(op_id, 0) + 1
                            )

                if _completed_count_by_op.get(op_id, 0) > 0:
                    try:
                        _check_watchdog(op, op_id, now)
                    except Exception as e:
                        logger.error(
                            f"[patch_dbs] Watchdog error for {op.name}: {e}",
                            exc_info=True
                        )

            return _orig_step(self, topology)
    else:
        def _patched_step(self):
            now = time.time()
            for op, op_state in self._topology.items():
                if not _should_monitor(op):
                    continue
                op_id = id(op)
                if now - _last_detection_time.get(op_id, 0) < DETECTION_INTERVAL_S:
                    continue
                _last_detection_time[op_id] = now

                cancelled = _cancelled_tasks_by_op.get(op_id, set())
                stale = _stale_tasks_by_op.get(op_id, set())
                sub = _sub_task_indices.get(op_id, set())
                for task_idx, (start_time, bundle) in list(
                    _task_metadata.get(op_id, {}).items()
                ):
                    if task_idx in cancelled or task_idx in stale or task_idx in sub:
                        continue
                    if task_idx not in op._data_tasks:
                        meta = _task_metadata.get(op_id, {}).pop(task_idx, None)
                        if meta is not None:
                            _completed_count_by_op[op_id] = (
                                _completed_count_by_op.get(op_id, 0) + 1
                            )

                if _completed_count_by_op.get(op_id, 0) > 0:
                    try:
                        _check_watchdog(op, op_id, now)
                    except Exception as e:
                        logger.error(
                            f"[patch_dbs] Watchdog error for {op.name}: {e}",
                            exc_info=True
                        )

            return _orig_step(self)  # pyright: ignore[reportCallIssue]

    StreamingExecutor._scheduling_loop_step = _patched_step  # pyright: ignore[reportAttributeAccessIssue]
    _applied = True
    logger.info(
        f"[patch_dbs] Watchdog-only mode enabled: "
        f"WATCHDOG_STALL_S={WATCHDOG_STALL_S}s, "
        f"WATCHDOG_MAX_RETRY={WATCHDOG_MAX_RETRY}, "
        f"WATCHDOG_MIN_COMPLETION_RATIO={WATCHDOG_MIN_COMPLETION_RATIO}, "
        f"DETECTION_INTERVAL_S={DETECTION_INTERVAL_S}s, "
        f"DRAIN_PHANTOM_QUEUE={DRAIN_PHANTOM_QUEUE}, "
        f"PHANTOM_STALL_S={PHANTOM_STALL_S}s"
    )


def get_metrics():
    """Return a copy of the watchdog metrics counter dict."""
    return _metrics.copy()


def _maybe_apply_on_import():
    """Apply the patch when imported, logging (but never raising) on failure.

    Intended to be called from ``ray.data.__init__`` so that the patch installs
    automatically whenever the env flag is set, without forcing users to call
    :func:`apply` themselves.
    """
    if not ENABLED:
        return
    try:
        apply()
    except ImportError as e:
        logger.warning(f"[patch_dbs] Failed to apply: {e}")
    except Exception as e:
        logger.error(
            f"[patch_dbs] Unexpected error during apply(): {e}", exc_info=True
        )
