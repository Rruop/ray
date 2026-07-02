"""Unit tests for ``BackpressurePolicy.available_capacity``.

These tests exercise the capacity-derivation logic in isolation (no Ray
cluster). End-to-end behavior is covered by ``test_streaming_executor.py``.
"""
import math
import unittest
from unittest.mock import MagicMock

from ray.data._internal.execution.backpressure_policy import (
    BackpressurePolicy,
    ConcurrencyCapBackpressurePolicy,
    DownstreamCapacityBackpressurePolicy,
    ResourceBudgetBackpressurePolicy,
)


class _StubPolicy(BackpressurePolicy):
    """Stub that exercises only the default ``available_capacity`` path."""

    def __init__(self, allow):
        # Skip super().__init__ — we don't need the manager/topology fixtures.
        self._allow = allow

    def can_add_input(self, op):  # noqa: ARG002
        return self._allow


class TestBaseAvailableCapacityDefault(unittest.TestCase):
    """Default impl: 0 when blocked, ``None`` when allowed."""

    def test_blocked(self):
        self.assertEqual(_StubPolicy(allow=False).available_capacity(MagicMock()), 0)

    def test_allowed(self):
        self.assertIsNone(_StubPolicy(allow=True).available_capacity(MagicMock()))


class TestConcurrencyCapAvailableCapacity(unittest.TestCase):
    """``ConcurrencyCap.available_capacity`` returns ``cap - running``."""

    def _make_policy(self, cap, running, eligible=False):
        policy = ConcurrencyCapBackpressurePolicy.__new__(
            ConcurrencyCapBackpressurePolicy
        )
        policy._concurrency_caps = {}
        policy._q_level_nbytes = {}
        policy._q_level_dev = {}
        policy._queue_level_thresholds = {}
        policy._last_effective_caps = {}
        policy.enable_dynamic_output_queue_size_backpressure = False
        rm = MagicMock()
        rm.is_op_eligible.return_value = eligible
        rm._is_blocking_materializing_op.return_value = False
        policy._resource_manager = rm

        op = MagicMock()
        op.metrics.num_tasks_running = running
        policy._concurrency_caps[op] = cap
        return policy, op

    def test_under_cap_returns_remaining(self):
        policy, op = self._make_policy(cap=10, running=3)
        self.assertEqual(policy.available_capacity(op), 7)

    def test_at_cap_returns_zero(self):
        policy, op = self._make_policy(cap=5, running=5)
        self.assertEqual(policy.available_capacity(op), 0)

    def test_over_cap_clamped_to_zero(self):
        policy, op = self._make_policy(cap=4, running=10)
        self.assertEqual(policy.available_capacity(op), 0)

    def test_unbounded_cap_returns_none(self):
        policy, op = self._make_policy(cap=math.inf, running=3)
        self.assertIsNone(policy.available_capacity(op))

    def test_can_add_input_consistent_with_capacity(self):
        for cap, running, expected in [
            (10, 3, True),    # capacity 7 → True
            (5, 5, False),    # capacity 0 → False
            (math.inf, 99, True),  # None → True
        ]:
            policy, op = self._make_policy(cap=cap, running=running)
            self.assertEqual(policy.can_add_input(op), expected)


class TestDownstreamCapacityAvailableCapacity(unittest.TestCase):
    """Boolean policy: blocked → 0, allowed → ``None``."""

    def _make_policy(self, applies):
        policy = DownstreamCapacityBackpressurePolicy.__new__(
            DownstreamCapacityBackpressurePolicy
        )
        policy._should_apply_backpressure = lambda op: applies
        return policy

    def test_blocked(self):
        self.assertEqual(self._make_policy(True).available_capacity(MagicMock()), 0)

    def test_allowed(self):
        self.assertIsNone(self._make_policy(False).available_capacity(MagicMock()))


class TestResourceBudgetAvailableCapacity(unittest.TestCase):
    """Delegates to ``OpResourceAllocator.available_task_capacity``."""

    def _make_policy(self, allocator):
        policy = ResourceBudgetBackpressurePolicy.__new__(
            ResourceBudgetBackpressurePolicy
        )
        rm = MagicMock()
        rm._op_resource_allocator = allocator
        policy._resource_manager = rm
        return policy

    def test_no_allocator_returns_none(self):
        self.assertIsNone(self._make_policy(None).available_capacity(MagicMock()))

    def test_allocator_returns_int(self):
        allocator = MagicMock()
        allocator.available_task_capacity.return_value = 4
        self.assertEqual(
            self._make_policy(allocator).available_capacity(MagicMock()), 4
        )

    def test_allocator_returns_none(self):
        allocator = MagicMock()
        allocator.available_task_capacity.return_value = None
        self.assertIsNone(self._make_policy(allocator).available_capacity(MagicMock()))


class TestReservationAllocatorTaskCapacity(unittest.TestCase):
    """``ReservationOpResourceAllocator.available_task_capacity`` floor-min."""

    def _make_allocator(self, budget_cpu, budget_gpu, budget_obj,
                       per_cpu, per_gpu, per_obj, output_per_task=0):
        from ray.data._internal.execution.interfaces import ExecutionResources
        from ray.data._internal.execution.resource_manager import (
            ReservationOpResourceAllocator,
        )

        alloc = ReservationOpResourceAllocator.__new__(ReservationOpResourceAllocator)
        budget = ExecutionResources(
            cpu=budget_cpu, gpu=budget_gpu, object_store_memory=budget_obj
        )
        op = MagicMock()
        op.incremental_resource_usage.return_value = ExecutionResources(
            cpu=per_cpu, gpu=per_gpu, object_store_memory=per_obj
        )
        op.metrics.obj_store_mem_max_pending_output_per_task = output_per_task
        alloc.get_budget = lambda o: budget if o is op else None
        return alloc, op

    def test_cpu_constrained(self):
        # CPU budget 10, per-task 3 → floor(10/3) = 3
        alloc, op = self._make_allocator(10, 0, 1024, 3, 0, 1, 0)
        self.assertEqual(alloc.available_task_capacity(op), 3)

    def test_object_store_constrained(self):
        # CPU 100/1=100; obj 100/40=2 → min = 2
        alloc, op = self._make_allocator(100, 0, 100, 1, 0, 40, 0)
        self.assertEqual(alloc.available_task_capacity(op), 2)

    def test_output_per_task_dominates_obj(self):
        # incr.obj=10, output_per_task=50 → max(10, 50)=50; 200/50=4
        alloc, op = self._make_allocator(100, 0, 200, 1, 0, 10, output_per_task=50)
        self.assertEqual(alloc.available_task_capacity(op), 4)

    def test_unbudgeted_returns_none(self):
        # When ``get_budget`` returns None.
        from ray.data._internal.execution.resource_manager import (
            ReservationOpResourceAllocator,
        )
        alloc = ReservationOpResourceAllocator.__new__(ReservationOpResourceAllocator)
        alloc.get_budget = lambda o: None
        self.assertIsNone(alloc.available_task_capacity(MagicMock()))

    def test_no_per_task_resource_returns_none(self):
        # If per-task usage is zero across all dims, no constraint.
        alloc, op = self._make_allocator(10, 10, 100, 0, 0, 0, 0)
        self.assertIsNone(alloc.available_task_capacity(op))


if __name__ == "__main__":
    import sys

    sys.exit(unittest.main())
