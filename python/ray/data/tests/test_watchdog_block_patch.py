"""Unit tests for ray.data._internal.execution.watchdog_block_patch.

Exercises the watchdog logic directly with a hand-rolled fake operator that
mimics the parts of MapOperator the watchdog touches:

  - op.metrics.num_tasks_finished / num_tasks_failed   (progress signal)
  - op._data_tasks                                      (live task dict)
  - op._output_queue.num_blocks()                       (phantom detection)
  - op.name                                             (skip-prefix filter)
  - op.mark_execution_finished()                        (phantom handler)
  - data_task._cancel(force) + _task_done_callback(exc) (cancellation)

These tests do not require a running Ray cluster; they avoid invoking apply()
by relying on the public _check_operator / _force_complete_task helpers and
overriding _should_monitor's isinstance check.
"""

import sys
import unittest
from unittest import mock


def _import_module_fresh():
    sys.modules.pop(
        "ray.data._internal.execution.watchdog_block_patch", None
    )
    import ray.data._internal.execution.watchdog_block_patch as mod
    mod._state.clear()
    for k in list(mod._metrics):
        mod._metrics[k] = 0
    return mod


class _FakeMetrics:
    def __init__(self):
        self.num_tasks_finished = 0
        self.num_tasks_failed = 0


class _FakeQueue:
    def __init__(self):
        self._blocks = 0

    def num_blocks(self):
        return self._blocks


class _FakeTask:
    def __init__(self, task_index, op):
        self._task_index = task_index
        self._op = op
        self._has_finished = False
        self.cancel_called = 0
        self.last_exc = None

    def _cancel(self, force):
        self.cancel_called += 1

    def _task_done_callback(self, exc):
        self.last_exc = exc
        if self._task_index in self._op._data_tasks:
            self._op._data_tasks.pop(self._task_index)
        self._op._output_queue._blocks = max(
            0, self._op._output_queue._blocks - 1
        )
        self._op.metrics.num_tasks_finished += 1
        if exc is not None:
            self._op.metrics.num_tasks_failed += 1


class _FakeOp:
    def __init__(self, name="MapBatches(Fake)"):
        self.name = name
        self.metrics = _FakeMetrics()
        self._data_tasks = {}
        self._data_tasks_history = {}
        self._output_queue = _FakeQueue()
        self.mark_execution_finished_calls = 0
        self._next_task_idx = 0

    def submit_task(self):
        idx = self._next_task_idx
        self._next_task_idx += 1
        task = _FakeTask(idx, self)
        self._data_tasks[idx] = task
        self._data_tasks_history[idx] = task
        self._output_queue._blocks += 1
        return idx

    def finish_task_naturally(self, idx):
        task = self._data_tasks.get(idx)
        if task is not None:
            task._task_done_callback(None)

    def mark_execution_finished(self):
        self.mark_execution_finished_calls += 1


class WatchdogTests(unittest.TestCase):
    def setUp(self):
        self.mod = _import_module_fresh()
        self._monitor_patch = mock.patch.object(
            self.mod,
            "_should_monitor",
            side_effect=lambda op: not op.name.startswith(
                ("ReadRange", "Project", "Aggregate", "Write")
            ),
        )
        self._monitor_patch.start()

    def tearDown(self):
        self._monitor_patch.stop()

    def test_progress_resets_stall_timer(self):
        op = _FakeOp()
        op.submit_task()
        op.submit_task()
        t0 = 1000.0
        self.mod._check_operator(op, t0)
        op.finish_task_naturally(0)
        self.mod._check_operator(op, t0 + 5)
        st = self.mod._state[id(op)]
        self.assertEqual(st["last_finished"], 1)
        self.assertEqual(st["last_progress_ts"], t0 + 5)

    def test_kills_oldest_when_stalled_at_tail(self):
        op = _FakeOp()
        stuck = op.submit_task()
        for _ in range(199):
            idx = op.submit_task()
            op.finish_task_naturally(idx)
        self.assertEqual(len(op._data_tasks), 1)

        t0 = 2000.0
        self.mod._check_operator(op, t0)
        self.mod._check_operator(op, t0 + 601)

        self.assertNotIn(stuck, op._data_tasks)
        self.assertEqual(self.mod._metrics["watchdog_killed"], 1)

    def test_does_not_kill_during_early_progress(self):
        op = _FakeOp()
        for _ in range(10):
            op.submit_task()
        for i in range(5):
            op.finish_task_naturally(i)

        t0 = 3000.0
        self.mod._check_operator(op, t0)
        self.mod._check_operator(op, t0 + 700)

        self.assertEqual(self.mod._metrics["watchdog_killed"], 0)

    def test_force_kill_does_not_count_as_progress(self):
        """Regression for the self-kill-as-progress bug from the legacy patch."""
        op = _FakeOp()
        stuck = op.submit_task()
        for _ in range(199):
            idx = op.submit_task()
            op.finish_task_naturally(idx)

        t0 = 4000.0
        self.mod._check_operator(op, t0)
        self.mod._check_operator(op, t0 + 601)
        self.assertNotIn(stuck, op._data_tasks)
        self.assertEqual(self.mod._metrics["watchdog_killed"], 1)

        second_stuck = op.submit_task()
        self.mod._check_operator(op, t0 + 606)
        st = self.mod._state[id(op)]
        self.assertEqual(st["last_finished"], 199)

        self.mod._check_operator(op, t0 + 1300)
        self.assertNotIn(second_stuck, op._data_tasks)
        self.assertEqual(self.mod._metrics["watchdog_killed"], 2)

    def test_empty_candidates_does_not_refresh_timer(self):
        """Regression for the stall-timer reset bug from the legacy patch."""
        op = _FakeOp()
        op.submit_task()
        for _ in range(199):
            idx = op.submit_task()
            op.finish_task_naturally(idx)

        t0 = 5000.0
        self.mod._check_operator(op, t0)
        self.mod._check_operator(op, t0 + 601)

        op._data_tasks.clear()
        op._output_queue._blocks = 0
        self.mod._check_operator(op, t0 + 700)
        st = self.mod._state[id(op)]
        self.assertEqual(st["last_progress_ts"], t0 + 700)

    def test_phantom_queue_triggers_mark_execution_finished(self):
        op = _FakeOp()
        for _ in range(50):
            idx = op.submit_task()
            op.finish_task_naturally(idx)
        op._output_queue._blocks = 3

        t0 = 6000.0
        self.mod._check_operator(op, t0)
        self.mod._check_operator(op, t0 + 601)
        self.assertEqual(op.mark_execution_finished_calls, 1)

        self.mod._check_operator(op, t0 + 1300)
        self.assertEqual(op.mark_execution_finished_calls, 1)

    def test_force_complete_marks_task_as_failed(self):
        op = _FakeOp()
        stuck = op.submit_task()
        for _ in range(199):
            idx = op.submit_task()
            op.finish_task_naturally(idx)

        t0 = 7000.0
        self.mod._check_operator(op, t0)
        self.mod._check_operator(op, t0 + 601)

        self.assertEqual(op.metrics.num_tasks_failed, 1)
        killed = op._data_tasks_history[stuck]
        self.assertIsInstance(killed.last_exc, self.mod.WatchdogStalledError)
        self.assertIn("Bundle dropped", str(killed.last_exc))

    def test_max_killed_budget_zero_aborts_first_kill(self):
        with mock.patch.object(self.mod, "WATCHDOG_MAX_KILLED", 0):
            op = _FakeOp()
            op.submit_task()
            for _ in range(199):
                idx = op.submit_task()
                op.finish_task_naturally(idx)

            t0 = 8000.0
            self.mod._check_operator(op, t0)
            with self.assertRaises(self.mod.WatchdogBudgetExceeded):
                self.mod._check_operator(op, t0 + 601)

    def test_max_killed_budget_aborts_after_threshold(self):
        with mock.patch.object(self.mod, "WATCHDOG_MAX_KILLED", 2):
            op = _FakeOp()
            for _ in range(3):
                op.submit_task()
            for _ in range(197):
                idx = op.submit_task()
                op.finish_task_naturally(idx)

            t0 = 9000.0
            self.mod._check_operator(op, t0)
            self.mod._check_operator(op, t0 + 601)
            self.mod._check_operator(op, t0 + 1300)
            with self.assertRaises(self.mod.WatchdogBudgetExceeded):
                self.mod._check_operator(op, t0 + 2000)


if __name__ == "__main__":
    unittest.main()
