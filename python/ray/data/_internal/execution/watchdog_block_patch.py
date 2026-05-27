"""Watchdog-only pipeline stall detection for Ray Data (opt-in monkey-patch).

This module ports the lean ``patch_watchdog_block.py`` runtime patch (shipped
via kling-ray/utils) into the Ray tree so users no longer need to side-load it
from an external repo.

Behavior matches the lean external version (kling-ray @ f9a085ea9c) and is
**disabled by default**. To enable it, export ``RAY_DATA_SPECULATION_ENABLED=1``
before ``import ray.data``; the patch is then applied automatically from
``ray.data.__init__``. It can also be applied manually by calling :func:`apply`
(which is idempotent under the ``ENABLED`` flag).

Solves two real problems and nothing else:

  1. **Tail-stuck tasks** — a few tasks in a long-running operator hang
     indefinitely (vLLM inference, network blip, dead actor), holding the
     pipeline at 99.99% forever.
  2. **`TaskCancelledError` crashes the executor** — when the watchdog (or
     Ray itself) cancels a hung task, its streaming generator raises
     ``TaskCancelledError`` from inside ``DataOpTask.on_data_ready``; without
     graceful handling it counts as an errored block and trips
     ``max_errored_blocks``.

Data-loss contract:

  When the watchdog gives up on a stuck task, the bundle of rows owned by that
  task is dropped — Ray's streaming generator was hung, so there is no output
  to recover. This patch makes the loss explicit and bounded:

  * Each killed task increments ``op.metrics.num_tasks_failed`` via a
    ``WatchdogStalledError`` passed to ``_task_done_callback``. The kill is
    visible in standard Ray Data stats, not silent.
  * The total number of kills across the dataset is bounded by
    ``RAY_WATCHDOG_MAX_KILLED`` (default ``-1`` = unlimited, matching the
    semantics of ``DataContext.max_errored_blocks``). When exceeded, the
    watchdog raises ``WatchdogBudgetExceeded`` from the scheduling loop, which
    Ray Data propagates as a dataset abort.

  If your pipeline cannot tolerate ANY data loss, set
  ``RAY_WATCHDOG_MAX_KILLED=0`` and the first stuck task aborts the dataset.

Design notes (why this is short):

  * We DO NOT do cancel-and-retry-on-new-node. An earlier 600-line version did,
    which forced it to dedup Ray's OOM retries, filter stale ActorPool output,
    track sub-tasks, and play whack-a-mole with ``op._data_tasks``. That code
    accumulated four hard bugs (zombie tasks, stall-timer reset on empty
    candidates, id(bundle) collisions, self-kills counted as progress) that
    caused multi-day production hangs. Dropping cancel-retry eliminates all of
    them — at the cost of giving up on stuck bundles instead of retrying them.
  * Progress signal is ``op.metrics.num_tasks_finished`` minus the number of
    tasks we have force-completed ourselves. Single source of truth, no
    homemade counters.
  * For ActorPool operators, ``ray.cancel(force=True)`` is a no-op on actor
    tasks. We still call ``_cancel(force=False)`` (best effort soft cancel)
    AND invoke ``data_task._task_done_callback(exc)`` to evict the task from
    ``op._data_tasks`` so the executor stops waiting on it.

Behaviour:

  Once an operator has both
    - ``completion_ratio >= RAY_WATCHDOG_MIN_COMPLETION_RATIO`` (default 0.95),
    - and zero real progress for ``RAY_WATCHDOG_STALL_S`` seconds (default 600),
  we kill the oldest live tasks. Up to
  ``ceil(total_tasks * RAY_WATCHDOG_PER_OP_KILL_RATIO)`` tasks may be killed per
  operator over its entire lifetime — beyond that we leave remaining hung tasks
  alone for the phantom-queue handler. The kill loop within a single cycle is
  serial (single-threaded scheduler loop), so ``_force_complete_task`` races are
  not a concern even when batching.

  The global ``RAY_WATCHDOG_MAX_KILLED`` budget (across all ops) still applies
  on top of the per-op cap.

  Phantom queue (``active_count==0`` but ``op._output_queue`` still has bundles)
  is handled by calling ``mark_execution_finished`` once.

Patches applied (when enabled):

- Patch A: ``ResourceManager.update_usages`` compatibility shim (Kuaishou Ray
  2.54.0 callers may pass ``update_op_state`` which upstream doesn't accept).
- Patch B: ``TaskCancelledError`` graceful handling in
  ``DataOpTask.on_data_ready``.
- Patch C: ``StreamingExecutor._scheduling_loop_step`` hook that runs the
  watchdog every ``RAY_DETECTION_INTERVAL_S`` seconds.

Environment variables:

``RAY_DATA_SPECULATION_ENABLED`` (default ``0``)
    Master switch. ``apply()`` is a no-op when this is ``0``.
``RAY_DETECTION_INTERVAL_S`` (default ``5``)
    How often (seconds) the watchdog inspects each operator.
``RAY_WATCHDOG_STALL_S`` (default ``600``)
    Stall threshold (no real progress) before intervention.
``RAY_WATCHDOG_MIN_COMPLETION_RATIO`` (default ``0.95``)
    Watchdog only fires once completed/(completed+active) reaches this ratio,
    to avoid early-stage false positives.
``RAY_WATCHDOG_PHANTOM_STALL_S`` (default = ``RAY_WATCHDOG_STALL_S``)
    Stall threshold specific to phantom-queue deadlocks.
``RAY_WATCHDOG_MAX_KILLED`` (default ``-1`` = unlimited)
    Maximum number of tasks the watchdog may force-complete before aborting
    the dataset via ``WatchdogBudgetExceeded``. Set to ``0`` to forbid any
    data loss.
``RAY_WATCHDOG_PER_OP_KILL_RATIO`` (default ``0.01`` = 1%)
    Per-op cumulative kill cap as a fraction of total tasks. Set to ``0`` for
    legacy one-task-per-cycle behaviour with no per-op ceiling (still bounded
    by ``RAY_WATCHDOG_MAX_KILLED``).
"""

import inspect
import logging
import os
import time
from typing import Dict, Optional

logger = logging.getLogger(__name__)

ENABLED = int(os.environ.get("RAY_DATA_SPECULATION_ENABLED", "0")) > 0
DETECTION_INTERVAL_S = float(os.environ.get("RAY_DETECTION_INTERVAL_S", "5"))
WATCHDOG_STALL_S = float(os.environ.get("RAY_WATCHDOG_STALL_S", "600"))
WATCHDOG_MIN_COMPLETION_RATIO = float(
    os.environ.get("RAY_WATCHDOG_MIN_COMPLETION_RATIO", "0.95")
)
PHANTOM_STALL_S = float(
    os.environ.get("RAY_WATCHDOG_PHANTOM_STALL_S", str(WATCHDOG_STALL_S))
)
WATCHDOG_MAX_KILLED = int(os.environ.get("RAY_WATCHDOG_MAX_KILLED", "-1"))

# Per-operator kill ratio cap. When stalled, the watchdog batch-kills up to
# ``ceil(total_tasks * WATCHDOG_PER_OP_KILL_RATIO)`` stuck tasks in a single
# cycle (instead of the historical one-task-per-cycle), so the tail of a stuck
# operator drains in one 600s window rather than ``stuck_count * STALL_S``
# seconds.
#
# The cap is **per-op cumulative across the op's lifetime**, not per-cycle — we
# keep a running ``op_killed`` counter in per-op state and never exceed it.
# This keeps the data-loss bound predictable: at most ``ratio * total_tasks``
# bundles per operator are ever dropped, no matter how the watchdog cycles.
#
# Default 0.01 (1%). On a 192k-task operator this allows ~1925 kills, which is
# more than enough to drain typical tail stalls (5-50 hung tasks). On a
# 100-task operator the cap becomes 1, which degrades to the historical
# one-at-a-time behaviour — acceptable since small ops rarely hit tail-stall.
#
# 0.0 disables batch kill entirely: falls back to legacy one-task-per-cycle
# behaviour with no per-op cumulative ceiling (still bounded by MAX_KILLED).
# 1.0 means the watchdog may kill every active task in one cycle once stalled.
WATCHDOG_PER_OP_KILL_RATIO = float(
    os.environ.get("RAY_WATCHDOG_PER_OP_KILL_RATIO", "0.01")
)


class WatchdogStalledError(Exception):
    """Marker exception attached to a force-completed task.

    Surfaces in Ray's ``num_tasks_failed`` metric so users can distinguish
    real worker failures from watchdog interventions in logs and dashboards.
    """


class WatchdogBudgetExceeded(Exception):
    """Raised from the scheduling loop when too many tasks have been killed.

    Triggers Ray Data executor to abort the running dataset, mirroring the
    behaviour of ``DataContext.max_errored_blocks``.
    """


_state: Dict[int, Dict] = {}

_metrics = {
    "cancellations_handled": 0,
    "watchdog_killed": 0,
    "phantom_mark_finished": 0,
}

_applied = False

_last_check: Dict[int, float] = {}


def get_metrics() -> Dict[str, int]:
    """Return a copy of the watchdog metrics counter dict."""
    return dict(_metrics)


def _new_state(now: float) -> Dict:
    return {
        "task_starts": {},
        "last_progress_ts": now,
        "last_finished": 0,
        "force_done": set(),
        "phantom_handled": False,
        "op_killed": 0,
    }


def _real_finished(op, st: Dict) -> int:
    """Real-progress signal that excludes tasks we ourselves marked done."""
    return op.metrics.num_tasks_finished - len(st["force_done"])


def _phantom_blocks(op) -> int:
    """Number of bundles sitting in the output queue right now (best effort)."""
    oq = getattr(op, "_output_queue", None)
    if oq is None:
        return 0
    try:
        n = oq.num_blocks()
        if isinstance(n, int):
            return n
    except Exception:
        pass
    try:
        return len(getattr(oq, "_queue", []) or [])
    except Exception:
        return 0


def _force_complete_task(op, task_idx: int, st: Dict, reason: str) -> bool:
    """Best-effort cancel + force-evict a stuck task, marking it as errored.

    For TaskPool tasks ``ray.cancel(force=False)`` triggers a soft cancellation
    on the worker; for ActorPool tasks Ray only allows soft cancel, so the
    eviction via ``_task_done_callback(exc)`` is what actually unblocks the
    executor.

    We pass a ``WatchdogStalledError`` instead of ``None`` so Ray's
    ``num_tasks_failed`` metric reflects the kill — making watchdog-induced
    data loss visible in standard Ray Data stats rather than silent.

    Raises ``WatchdogBudgetExceeded`` when the kill count exceeds
    ``RAY_WATCHDOG_MAX_KILLED``.

    Returns True if eviction succeeded (or task was already gone).
    """
    data_task = op._data_tasks.get(task_idx)
    if data_task is None:
        return True

    try:
        if hasattr(data_task, "_cancel"):
            data_task._cancel(force=False)
        elif hasattr(data_task, "cancel"):
            data_task.cancel()
    except Exception as e:
        logger.warning(
            f"[patch_dbs] _cancel failed for task {task_idx} on {op.name}: {e}"
        )

    exc = WatchdogStalledError(
        f"Watchdog force-completed task {task_idx} on {op.name}: {reason}. "
        f"Bundle dropped."
    )
    try:
        if not getattr(data_task, "_has_finished", False):
            data_task._task_done_callback(exc)
            data_task._has_finished = True
        st["force_done"].add(task_idx)
        st["op_killed"] += 1
        _metrics["watchdog_killed"] += 1
        if (
            WATCHDOG_MAX_KILLED >= 0
            and _metrics["watchdog_killed"] > WATCHDOG_MAX_KILLED
        ):
            raise WatchdogBudgetExceeded(
                f"Watchdog killed {_metrics['watchdog_killed']} tasks, exceeding "
                f"RAY_WATCHDOG_MAX_KILLED={WATCHDOG_MAX_KILLED}. Aborting dataset. "
                f"Last kill: {exc}"
            )
        return True
    except WatchdogBudgetExceeded:
        raise
    except Exception as e:
        logger.error(
            f"[patch_dbs] failed to force-complete task {task_idx} on {op.name}: {e}",
            exc_info=True,
        )
        return False


def _should_monitor(op) -> bool:
    """Skip operators that are not user maps (reads, writes, aggregates)."""
    from ray.data._internal.execution.operators.map_operator import MapOperator
    if not isinstance(op, MapOperator):
        return False
    skip_prefix = ("ReadRange", "Project", "Aggregate", "Write")
    return not op.name.startswith(skip_prefix)


def _check_operator(op, now: float) -> None:
    """One watchdog pass over a single operator."""
    op_id = id(op)
    st = _state.setdefault(op_id, _new_state(now))

    active_tasks = dict(op._data_tasks)
    for idx in list(st["task_starts"]):
        if idx not in active_tasks:
            st["task_starts"].pop(idx, None)
    for idx in active_tasks:
        st["task_starts"].setdefault(idx, now)

    finished_real = _real_finished(op, st)
    if finished_real > st["last_finished"]:
        st["last_finished"] = finished_real
        st["last_progress_ts"] = now
        return

    active_count = len(active_tasks)

    if active_count == 0:
        if _phantom_blocks(op) == 0:
            st["last_progress_ts"] = now
            return
        stalled_s = now - st["last_progress_ts"]
        if stalled_s < PHANTOM_STALL_S or st["phantom_handled"]:
            return
        logger.warning(
            f"[patch_dbs] PHANTOM stall on {op.name}: "
            f"{_phantom_blocks(op)} queued bundles, no active tasks, stalled "
            f"{stalled_s:.0f}s. Marking execution finished."
        )
        if hasattr(op, "mark_execution_finished"):
            try:
                op.mark_execution_finished()
                _metrics["phantom_mark_finished"] += 1
            except Exception as e:
                logger.error(
                    f"[patch_dbs] mark_execution_finished failed for {op.name}: {e}"
                )
        st["phantom_handled"] = True
        return

    stalled_s = now - st["last_progress_ts"]
    if stalled_s < WATCHDOG_STALL_S:
        return

    total = finished_real + active_count
    completion_ratio = finished_real / total if total > 0 else 0.0
    if completion_ratio < WATCHDOG_MIN_COMPLETION_RATIO:
        return

    candidates = [
        (idx, ts)
        for idx, ts in st["task_starts"].items()
        if idx in active_tasks and idx not in st["force_done"]
    ]
    if not candidates:
        # All live tasks already force-killed; do NOT refresh the timer here
        # so the stall counter keeps growing and phantom detection can take
        # over once active drops to 0.
        return

    candidates.sort(key=lambda x: x[1])

    if WATCHDOG_PER_OP_KILL_RATIO <= 0.0:
        op_cap = None
        remaining_budget = 1
    else:
        op_cap = max(1, int(total * WATCHDOG_PER_OP_KILL_RATIO + 0.5))
        remaining_budget = max(0, op_cap - st["op_killed"])
        if remaining_budget == 0:
            return

    victims = candidates[:remaining_budget]
    cap_repr = f"{st['op_killed']}/{op_cap}" if op_cap is not None else "legacy-1per-cycle"
    logger.warning(
        f"[patch_dbs] WATCHDOG: {op.name} stalled {stalled_s:.0f}s "
        f"(finished={finished_real}, active={active_count}, "
        f"ratio={completion_ratio:.5f}). "
        f"Killing {len(victims)} task(s) "
        f"(op_killed={cap_repr}, oldest_runtime="
        f"{now - victims[0][1]:.0f}s)."
    )

    killed_any = False
    reason = f"stalled {stalled_s:.0f}s at ratio {completion_ratio:.5f}"
    for victim_idx, _ in victims:
        if _force_complete_task(op, victim_idx, st, reason=reason):
            killed_any = True

    if killed_any:
        # Give Ray at least one detection cycle to react to the eviction
        # before fairly judging whether progress resumed.
        st["last_progress_ts"] = now


def _run_watchdog(topology) -> None:
    now = time.time()
    for op in topology:
        if not _should_monitor(op):
            continue
        op_id = id(op)
        if now - _last_check.get(op_id, 0.0) < DETECTION_INTERVAL_S:
            continue
        _last_check[op_id] = now
        try:
            _check_operator(op, now)
        except WatchdogBudgetExceeded:
            raise
        except Exception as e:
            logger.error(
                f"[patch_dbs] Watchdog error on {op.name}: {e}", exc_info=True
            )


def apply() -> None:
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
    from ray.data._internal.execution.interfaces.physical_operator import DataOpTask
    from ray.data._internal.execution.resource_manager import ResourceManager
    from ray.data._internal.execution.streaming_executor import StreamingExecutor

    _orig_update_usages = ResourceManager.update_usages
    if "update_op_state" not in inspect.signature(_orig_update_usages).parameters:
        def _patched_update_usages(self, update_op_state: bool = False, **kwargs):
            return _orig_update_usages(self, **kwargs)
        ResourceManager.update_usages = _patched_update_usages
        logger.info("[patch_dbs] Patched ResourceManager.update_usages")

    _orig_on_data_ready = DataOpTask.on_data_ready

    def _patched_on_data_ready(self, max_bytes_to_read: Optional[int] = None) -> int:
        try:
            return _orig_on_data_ready(self, max_bytes_to_read)
        except ray.exceptions.TaskCancelledError:
            _metrics["cancellations_handled"] += 1
            task_idx = self.task_index() if hasattr(self, "task_index") else "?"
            logger.info(
                f"[patch_dbs] TaskCancelledError handled gracefully "
                f"(task_index={task_idx})"
            )
            if not getattr(self, "_has_finished", False):
                try:
                    self._task_done_callback(None)
                except Exception as e:
                    logger.error(
                        f"[patch_dbs] _task_done_callback failed in cancellation "
                        f"handler: {e}"
                    )
                self._has_finished = True
            return 0

    DataOpTask.on_data_ready = _patched_on_data_ready
    logger.info("[patch_dbs] Patched DataOpTask.on_data_ready")

    _orig_step = StreamingExecutor._scheduling_loop_step
    _step_takes_topology = "topology" in inspect.signature(_orig_step).parameters

    if _step_takes_topology:
        def _patched_step(self, topology):  # pyright: ignore[reportRedeclaration]
            _run_watchdog(topology)
            return _orig_step(self, topology)
    else:
        def _patched_step(self):  # pyright: ignore[reportRedeclaration]
            _run_watchdog(self._topology)
            return _orig_step(self)  # pyright: ignore[reportCallIssue]

    StreamingExecutor._scheduling_loop_step = _patched_step  # pyright: ignore[reportAttributeAccessIssue]
    _applied = True
    logger.info(
        f"[patch_dbs] Watchdog installed: "
        f"STALL_S={WATCHDOG_STALL_S}, "
        f"MIN_COMPLETION_RATIO={WATCHDOG_MIN_COMPLETION_RATIO}, "
        f"DETECTION_INTERVAL_S={DETECTION_INTERVAL_S}, "
        f"PHANTOM_STALL_S={PHANTOM_STALL_S}, "
        f"MAX_KILLED={WATCHDOG_MAX_KILLED}, "
        f"PER_OP_KILL_RATIO={WATCHDOG_PER_OP_KILL_RATIO}"
    )


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
