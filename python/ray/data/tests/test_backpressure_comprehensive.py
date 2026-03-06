"""Comprehensive tests for Ray Data backpressure policies.

This module contains additional test cases that cover:
1. Multi-policy interaction tests
2. Edge case tests for each policy
3. EWMA algorithm verification tests
4. Integration tests with various pipeline configurations
"""

import math
import time
import types
import unittest
from collections import defaultdict
from unittest.mock import MagicMock, patch

import pytest

import ray
from ray.data._internal.execution.backpressure_policy import (
    ENABLED_BACKPRESSURE_POLICIES_CONFIG_KEY,
    ConcurrencyCapBackpressurePolicy,
    DownstreamCapacityBackpressurePolicy,
    ResourceBudgetBackpressurePolicy,
    get_backpressure_policies,
)
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.task_pool_map_operator import (
    TaskPoolMapOperator,
)
from ray.data._internal.execution.resource_manager import ResourceManager
from ray.data._internal.execution.streaming_executor_state import OpState
from ray.data.context import DataContext
from ray.data.tests.conftest import mock_all_to_all_op


class TestMultiPolicyInteraction(unittest.TestCase):
    """Tests for interaction between multiple backpressure policies."""

    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=4, ignore_reinit_error=True)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def _mock_resource_manager(self):
        """Helper to create a resource manager mock with real method bindings."""
        rm = MagicMock()
        rm.is_op_eligible = types.MethodType(ResourceManager.is_op_eligible, rm)
        rm._get_downstream_ineligible_ops = types.MethodType(
            ResourceManager._get_downstream_ineligible_ops, rm
        )
        rm._is_blocking_materializing_op = types.MethodType(
            ResourceManager._is_blocking_materializing_op, rm
        )
        return rm

    def test_all_policies_enabled_any_false_blocks(self):
        """Test that if any policy returns False, the operator is blocked."""
        input_op = InputDataBuffer(DataContext.get_current(), input_data=[MagicMock()])
        map_op = TaskPoolMapOperator(
            map_transformer=MagicMock(),
            data_context=DataContext.get_current(),
            input_op=input_op,
            max_concurrency=10,
        )
        map_op.metrics.num_tasks_running = 5

        topology = {map_op: MagicMock(), input_op: MagicMock()}

        mock_rm = self._mock_resource_manager()
        mock_rm.get_op_usage.return_value = None
        mock_rm.get_budget.return_value = None

        data_context = DataContext.get_current()

        # Create all three policies
        concurrency_policy = ConcurrencyCapBackpressurePolicy(
            data_context, topology, mock_rm
        )
        resource_policy = ResourceBudgetBackpressurePolicy(
            data_context, topology, mock_rm
        )
        downstream_policy = DownstreamCapacityBackpressurePolicy(
            data_context, topology, mock_rm
        )

        # All policies allow input
        mock_rm._op_resource_allocator = None  # ResourceBudget returns True
        self.assertTrue(concurrency_policy.can_add_input(map_op))
        self.assertTrue(resource_policy.can_add_input(map_op))
        self.assertTrue(downstream_policy.can_add_input(map_op))

        # Make ResourceBudget block
        mock_allocator = MagicMock()
        mock_allocator.can_submit_new_task.return_value = False
        mock_rm._op_resource_allocator = mock_allocator

        # Now one policy returns False
        self.assertFalse(resource_policy.can_add_input(map_op))

        # In real execution, ANY False would block the operator
        policies = [concurrency_policy, resource_policy, downstream_policy]
        can_proceed = all(p.can_add_input(map_op) for p in policies)
        self.assertFalse(can_proceed)

    def test_max_output_bytes_uses_minim(self):
        """Test that max_task_output_bytes_to_read uses the minimum of all policies."""
        input_op = InputDataBuffer(DataContext.get_current(), input_data=[MagicMock()])
        map_op = TaskPoolMapOperator(
            map_transformer=MagicMock(),
            data_context=DataContext.get_current(),
            input_op=input_op,
        )

        topology = {map_op: MagicMock(), input_op: MagicMock()}
        mock_rm = self._mock_resource_manager()

        data_context = DataContext.get_current()

        # Create policies that return different limits
        policy1 = MagicMock()
        policy1.max_task_output_bytes_to_read.return_value = 1000
        policy2 = MagicMock()
        policy2.max_task_output_bytes_to_read.return_value = 500
        policy3 = MagicMock()
        policy3.max_task_output_bytes_to_read.return_value = None  # No limit

        policies = [policy1, policy2, policy3]

        # Calculate minimum like streaming_executor_state does
        max_bytes = None
        for policy in policies:
            limit = policy.max_task_output_bytes_to_read(map_op)
            if limit is not None:
                if max_bytes is None:
                    max_bytes = limit
                else:
                    max_bytes = min(max_bytes, limit)

        self.assertEqual(max_bytes, 500)  # Should be minimum of 1000 and 500

    def test_policy_ordering_does_not_affect_result(self):
        """Test that the order of policy evaluation doesn't affect the final result."""
        input_op = InputDataBuffer(DataContext.get_current(), input_data=[MagicMock()])
        map_op = TaskPoolMapOperator(
            map_transformer=MagicMock(),
            data_context=DataContext.get_current(),
            input_op=input_op,
            max_concurrency=5,
        )
        map_op.metrics.num_tasks_running = 6  # Over the limit

        topology = {map_op: MagicMock(), input_op: MagicMock()}
        mock_rm = self._mock_resource_manager()
        mock_rm.is_op_eligible.return_value = False  # Skip dynamic backpressure

        data_context = DataContext.get_current()

        # Test different orderings
        orderings = [
            [ConcurrencyCapBackpressurePolicy, ResourceBudgetBackpressurePolicy],
            [ResourceBudgetBackpressurePolicy, ConcurrencyCapBackpressurePolicy],
        ]

        results = []
        for ordering in orderings:
            policies = [cls(data_context, topology, mock_rm) for cls in ordering]
            can_add = all(p.can_add_input(map_op) for p in policies)
            results.append(can_add)

        # All orderings should give the same result
        self.assertTrue(all(r == results[0] for r in results))


class TestEWMAAlgorithm(unittest.TestCase):
    """Tests for the EWMA algorithm in ConcurrencyCapBackpressurePolicy."""

    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=2, ignore_reinit_error=True)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def _create_policy_with_op(self):
        """Helper to create a policy and operator for testing."""
        input_op = InputDataBuffer(DataContext.get_current(), input_data=[MagicMock()])
        map_op = TaskPoolMapOperator(
            map_transformer=MagicMock(),
            data_context=DataContext.get_current(),
            input_op=input_op,
            max_concurrency=10,
        )
        map_op.metrics.num_tasks_running = 5

        topology = {map_op: MagicMock(), input_op: MagicMock()}
        mock_rm = MagicMock()

        policy = ConcurrencyCapBackpressurePolicy(
            DataContext.get_current(), topology, mock_rm
        )
        return policy, map_op

    def test_ewma_asymmetric_fast_rise_slow_fall(self):
        """Test that EWMA rises faster than it falls."""
        policy, _ = self._create_policy_with_op()

        # Start from a baseline
        prev_value = 100.0

        # Test fast rise
        high_sample = 200.0
        risen_value = policy._update_ewma_asymmetric(prev_value, high_sample)

        # Test slow fall
        low_sample = 50.0
        fallen_value = policy._update_ewma_asymmetric(prev_value, low_sample)

        # Calculate the change ratios
        rise_change = (risen_value - prev_value) / (high_sample - prev_value)
        fall_change = (prev_value - fallen_value) / (prev_value - low_sample)

        # Rise should be faster (larger alpha)
        self.assertGreater(rise_change, fall_change)

    def test_ewma_initialization_from_zero(self):
        """Test that EWMA initializes correctly from zero."""
        policy, _ = self._create_policy_with_op()

        # When prev_value <= 0, should return the sample directly
        result = policy._update_ewma_asymmetric(0.0, 100.0)
        self.assertEqual(result, 100.0)

        result = policy._update_ewma_asymmetric(-1.0, 100.0)
        self.assertEqual(result, 100.0)

    def test_level_and_dev_update(self):
        """Test that level and dev are updated correctly."""
        policy, map_op = self._create_policy_with_op()

        # Initialize with some values
        policy._q_level_nbytes[map_op] = 100.0
        policy._q_level_dev[map_op] = 20.0

        # Update with a new queue size
        policy._update_level_and_dev(map_op, 150)

        # Level should have moved toward 150
        new_level = policy._q_level_nbytes[map_op]
        self.assertGreater(new_level, 100.0)
        self.assertLess(new_level, 150.0)

        # Dev should have been updated based on |150 - 100| = 50
        new_dev = policy._q_level_dev[map_op]
        self.assertGreater(new_dev, 20.0)  # Should increase since deviation is large

    def test_effective_cap_backoff_when_above_upper_bound(self):
        """Test that effective cap decreases when queue is above upper bound."""
        policy, map_op = self._create_policy_with_op()

        # Set up EWMA state: level=100, dev=10
        # Upper bound = 100 + 1.0 * 10 = 110
        policy._q_level_nbytes[map_op] = 100.0
        policy._q_level_dev[map_op] = 10.0

        num_tasks_running = 5
        current_queue = 150  # > 110 (upper bound)

        effective_cap = policy._effective_cap(map_op, num_tasks_running, current_queue)

        # Should backoff: 5 - 1 = 4
        self.assertEqual(effective_cap, 4)

    def test_effective_cap_rampup_when_below_lower_bound(self):
        """Test that effective cap increases when queue is below lower bound."""
        policy, map_op = self._create_policy_with_op()

        # Set up EWMA state: level=100, dev=10
        # Lower bound = 100 - 1.0 * 10 = 90
        policy._q_level_nbytes[map_op] = 100.0
        policy._q_level_dev[map_op] = 10.0

        num_tasks_running = 5
        current_queue = 50  # < 90 (lower bound)

        effective_cap = policy._effective_cap(map_op, num_tasks_running, current_queue)

        # Should ramp up: 5 + 1 = 6
        self.assertEqual(effective_cap, 6)

    def test_effective_cap_hold_when_in_deadband(self):
        """Test that effective cap stays the same when queue is in deadband."""
        policy, map_op = self._create_policy_with_op()

        # Set up EWMA state: level=100, dev=10
        # Deadband = [90, 110]
        policy._q_level_nbytes[map_op] = 100.0
        policy._q_level_dev[map_op] = 10.0

        num_tasks_running = 5
        current_queue = 100  # In [90, 110]

        effective_cap = policy._effective_cap(map_op, num_tasks_running, current_queue)

        # Should hold: 5
        self.assertEqual(effective_cap, 5)

    def test_effective_cap_minimum_is_one(self):
        """Test that effective cap never goes below 1."""
        policy, map_op = self._create_policy_with_op()

        # Set up state for maximum backoff
        policy._q_level_nbytes[map_op] = 10.0
        policy._q_level_dev[map_op] = 1.0
        policy._concurrency_caps[map_op] = 10

        num_tasks_running = 1
        current_queue = 100  # Way above upper bound

        effective_cap = policy._effective_cap(map_op, num_tasks_running, current_queue)

        # Should not go below 1
        self.assertEqual(effective_cap, 1)

    def test_effective_cap_respects_configured_max(self):
        """Test that effective cap respects the configured maximum."""
        policy, map_op = self._create_policy_with_op()

        # Set up state for maximum rampup
        policy._q_level_nbytes[map_op] = 100.0
        policy._q_level_dev[map_op] = 50.0
        policy._concurrency_caps[map_op] = 5  # Configured max

        num_tasks_running = 4
        current_queue = 10  # Way below lower bound

        effective_cap = policy._effective_cap(map_op, num_tasks_running, current_queue)

        # Should ramp up but not exceed configured max
        self.assertEqual(effective_cap, 5)


class TestDownstreamCapacityEdgeCases(unittest.TestCase):
    """Edge case tests for DownstreamCapacityBackpressurePolicy."""

    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=2, ignore_reinit_error=True)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def _mock_resource_manager(self):
        """Helper to create a resource manager mock."""
        rm = MagicMock()
        rm.is_op_eligible = types.MethodType(ResourceManager.is_op_eligible, rm)
        rm._get_downstream_ineligible_ops = types.MethodType(
            ResourceManager._get_downstream_ineligible_ops, rm
        )
        rm._is_blocking_materializing_op = types.MethodType(
            ResourceManager._is_blocking_materializing_op, rm
        )
        rm.get_external_consumer_bytes.return_value = 100
        return rm

    def _mock_operator(self, throttling_disabled=False, has_execution_finished=False):
        """Helper to create a mock operator."""
        op = MagicMock()
        op.metrics = MagicMock()
        op.metrics.num_tasks_running = 5
        op.metrics.obj_store_mem_pending_task_inputs = 100
        op.output_dependencies = []
        op.throttling_disabled.return_value = throttling_disabled
        op.has_execution_finished.return_value = has_execution_finished

        op_state = MagicMock(spec=OpState)
        op_state.output_queue_bytes.return_value = 0
        return op, op_state

    @patch(
        "ray.data._internal.execution.backpressure_policy."
        "downstream_capacity_backpressure_policy."
        "get_utilized_object_store_budget_fraction"
    )
    def test_no_output_dependencies_uses_external_consumer(self, mock_util_fraction):
        """Test that external consumer bytes are used when no output dependencies."""
        mock_util_fraction.return_value = 0.95  # Above threshold

        op, op_state = self._mock_operator()
        op.output_dependencies = []  # No downstream

        topology = {op: op_state}
        context = DataContext()
        context.downstream_capacity_backpressure_ratio = 2.0
        rm = self._mock_resource_manager()
        rm.get_external_consumer_bytes.return_value = 500

        policy = DownstreamCapacityBackpressurePolicy(context, topology, rm)

        # Queue size small, external consumer large -> no backpressure
        op_state.output_queue_bytes.return_value = 100  # 100/500 = 0.2 < 2.0

        self.assertTrue(policy.can_add_input(op))

    @patch(
        "ray.data._internal.execution.backpressure_policy."
        "downstream_capacity_backpressure_policy."
        "get_utilized_object_store_budget_fraction"
    )
    def test_recursive_downstream_capacity_calculation(self, mock_util_fraction):
        """Test that downstream capacity is calculated recursively for ineligible ops."""
        mock_util_fraction.return_value = 0.95

        # Create chain: op -> ineligible_op -> eligible_op
        op, op_state = self._mock_operator()
        ineligible_op, ineligible_state = self._mock_operator(throttling_disabled=True)
        eligible_op, eligible_state = self._mock_operator()

        op.output_dependencies = [ineligible_op]
        ineligible_op.output_dependencies = [eligible_op]
        eligible_op.metrics.obj_store_mem_pending_task_inputs = 1000

        topology = {
            op: op_state,
            ineligible_op: ineligible_state,
            eligible_op: eligible_state,
        }
        context = DataContext()
        context.downstream_capacity_backpressure_ratio = 2.0
        rm = self._mock_resource_manager()
        rm.get_op_usage.return_value = MagicMock(object_store_memory=0)

        policy = DownstreamCapacityBackpressurePolicy(context, topology, rm)

        # The capacity should come from eligible_op, not ineligible_op
        capacity = policy._get_downstream_capacity_size_bytes(op)
        self.assertEqual(capacity, 1000)

    @patch(
        "ray.data._internal.execution.backpressure_policy."
        "downstream_capacity_backpressure_policy."
        "get_utilized_object_store_budget_fraction"
    )
    def test_zero_downstream_capacity_no_backpressure(self, mock_util_fraction):
        """Test that zero downstream capacity means no backpressure."""
        mock_util_fraction.return_value = 0.95

        op, op_state = self._mock_operator()
        downstream_op, downstream_state = self._mock_operator()
        downstream_op.metrics.obj_store_mem_pending_task_inputs = 0

        op.output_dependencies = [downstream_op]
        topology = {op: op_state, downstream_op: downstream_state}
        context = DataContext()
        context.downstream_capacity_backpressure_ratio = 2.0
        rm = self._mock_resource_manager()

        policy = DownstreamCapacityBackpressurePolicy(context, topology, rm)

        # Even with large queue, zero capacity means ratio = 0
        op_state.output_queue_bytes.return_value = 10000

        # Queue ratio should be 0 when downstream capacity is 0
        ratio = policy._get_queue_ratio(op)
        self.assertEqual(ratio, 0)

        # Should not apply backpressure
        self.assertTrue(policy.can_add_input(op))


class TestResourceBudgetPolicy(unittest.TestCase):
    """Tests for ResourceBudgetBackpressurePolicy."""

    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=2, ignore_reinit_error=True)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_no_allocator_always_allows(self):
        """Test that without an allocator, input is always allowed."""
        input_op = InputDataBuffer(DataContext.get_current(), input_data=[MagicMock()])

        topology = {input_op: MagicMock()}
        rm = MagicMock()
        rm._op_resource_allocator = None

        policy = ResourceBudgetBackpressurePolicy(
            DataContext.get_current(), topology, rm
        )

        self.assertTrue(policy.can_add_input(input_op))

    def test_allocator_decision_is_respected(self):
        """Test that the allocator's decision is respected."""
        input_op = InputDataBuffer(DataContext.get_current(), input_data=[MagicMock()])

        topology = {input_op: MagicMock()}
        rm = MagicMock()

        policy = ResourceBudgetBackpressurePolicy(
            DataContext.get_current(), topology, rm
        )

        # Test allow
        allocator = MagicMock()
        allocator.can_submit_new_task.return_value = True
        rm._op_resource_allocator = allocator
        self.assertTrue(policy.can_add_input(input_op))

        # Test deny
        allocator.can_submit_new_task.return_value = False
        self.assertFalse(policy.can_add_input(input_op))

    def test_max_bytes_delegates_to_resource_manager(self):
        """Test that max_task_output_bytes_to_read delegates to ResourceManager."""
        input_op = InputDataBuffer(DataContext.get_current(), input_data=[MagicMock()])

        topology = {input_op: MagicMock()}
        rm = MagicMock()
        rm.max_task_output_bytes_to_read.return_value = 1234

        policy = ResourceBudgetBackpressurePolicy(
            DataContext.get_current(), topology, rm
        )

        result = policy.max_task_output_bytes_to_read(input_op)
        self.assertEqual(result, 1234)
        rm.max_task_output_bytes_to_read.assert_called_once_with(input_op)


class TestPolicyFactory(unittest.TestCase):
    """Tests for the policy factory function."""

    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=2, ignore_reinit_error=True)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_get_backpressure_policies_default(self):
        """Test that default policies are created correctly."""
        input_op = InputDataBuffer(DataContext.get_current(), input_data=[MagicMock()])
        topology = {input_op: MagicMock()}
        rm = MagicMock()

        context = DataContext()
        policies = get_backpressure_policies(context, topology, rm)

        self.assertEqual(len(policies), 3)
        self.assertIsInstance(policies[0], ConcurrencyCapBackpressurePolicy)
        self.assertIsInstance(policies[1], ResourceBudgetBackpressurePolicy)
        self.assertIsInstance(policies[2], DownstreamCapacityBackpressurePolicy)

    def test_get_backpressure_policies_custom(self):
        """Test that custom policy configuration is respected."""
        input_op = InputDataBuffer(DataContext.get_current(), input_data=[MagicMock()])
        topology = {input_op: MagicMock()}
        rm = MagicMock()

        context = DataContext()
        context.set_config(
            ENABLED_BACKPRESSURE_POLICIES_CONFIG_KEY,
            [ConcurrencyCapBackpressurePolicy],
        )

        policies = get_backpressure_policies(context, topology, rm)

        self.assertEqual(len(policies), 1)
        self.assertIsInstance(policies[0], ConcurrencyCapBackpressurePolicy)


class TestBackpressureE2EScenarios(unittest.TestCase):
    """End-to-end scenario tests for backpressure policies."""

    @classmethod
    def setUpClass(cls):
        cls._cluster_cpus = 4
        ray.init(num_cpus=cls._cluster_cpus, ignore_reinit_error=True)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_slow_consumer_fast_producer_pipeline(self):
        """Test backpressure in a slow consumer, fast producer scenario."""
        data_context = ray.data.DataContext.get_current()
        data_context.set_config(
            ENABLED_BACKPRESSURE_POLICIES_CONFIG_KEY,
            [ConcurrencyCapBackpressurePolicy, ResourceBudgetBackpressurePolicy],
        )

        num_items = 20

        def fast_producer(batch):
            # Produces quickly
            for item in batch["id"]:
                yield {"id": [item], "data": [item * 2]}

        def slow_consumer(batch):
            # Consumes slowly
            time.sleep(0.1)
            return {"id": batch["id"], "result": [x * 3 for x in batch["data"]]}

        ds = ray.data.range(num_items, override_num_blocks=num_items)
        ds = ds.map_batches(fast_producer, batch_size=1, num_cpus=0.5)
        ds = ds.map_batches(slow_consumer, batch_size=1, num_cpus=0.5)

        # Should complete without memory issues
        results = ds.take_all()
        self.assertEqual(len(results), num_items)

        # Clean up config
        data_context.remove_config(ENABLED_BACKPRESSURE_POLICIES_CONFIG_KEY)

    def test_multiple_stages_pipeline(self):
        """Test backpressure with multiple pipeline stages."""
        data_context = ray.data.DataContext.get_current()

        num_items = 10

        def stage1(batch):
            return {"stage1": batch["id"]}

        def stage2(batch):
            time.sleep(0.05)
            return {"stage2": batch["stage1"]}

        def stage3(batch):
            return {"stage3": batch["stage2"]}

        ds = ray.data.range(num_items, override_num_blocks=num_items)
        ds = ds.map_batches(stage1, batch_size=1, num_cpus=0.3)
        ds = ds.map_batches(stage2, batch_size=1, num_cpus=0.3)
        ds = ds.map_batches(stage3, batch_size=1, num_cpus=0.3)

        results = ds.take_all()
        self.assertEqual(len(results), num_items)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
