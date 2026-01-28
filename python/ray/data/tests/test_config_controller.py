"""Tests for ConfigController class.

This module tests the configuration controller that synchronizes
operator configurations to running operators during execution.
"""

import time
import pytest
from unittest.mock import MagicMock

from ray.data._internal.execution.config import (
    ExecutionConfig,
    TaskPoolOperatorConfig,
    ActorPoolOperatorConfig,
    ConfigController,
    ExecutionConfigStore,
    MemoryExecutionConfigStore,
)
from ray.data._internal.execution.config.controller import DEFAULT_CONFIG_POLL_INTERVAL_S


class TestConfigControllerCreation:
    """Tests for ConfigController initialization."""

    def test_controller_creation(self):
        mock_topology = {}
        mock_store = MagicMock(spec=ExecutionConfigStore)
        mock_store.get.return_value = None

        controller = ConfigController(
            topology=mock_topology,
            config_store=mock_store,
        )
        assert controller._topology is mock_topology
        assert controller._store is mock_store
        assert controller._poll_interval_s == DEFAULT_CONFIG_POLL_INTERVAL_S

    def test_controller_with_memory_store(self):
        mock_topology = {}
        store = MemoryExecutionConfigStore()

        controller = ConfigController(
            topology=mock_topology,
            config_store=store,
        )
        assert controller._store is store

    def test_controller_custom_poll_interval(self):
        mock_topology = {}
        mock_store = MagicMock(spec=ExecutionConfigStore)
        mock_store.get.return_value = None

        controller = ConfigController(
            topology=mock_topology,
            config_store=mock_store,
            poll_interval_s=10.0,
        )
        assert controller._poll_interval_s == 10.0


class TestConfigControllerTryApplyConfig:
    """Tests for ConfigController.try_apply_config method."""

    def test_try_apply_config_no_config(self):
        mock_topology = {}
        mock_store = MagicMock(spec=ExecutionConfigStore)
        mock_store.get.return_value = None

        controller = ConfigController(
            topology=mock_topology,
            config_store=mock_store,
            poll_interval_s=0,  # Disable throttling for test
        )

        # Should not raise
        controller.try_apply_config()

    def test_try_apply_config_with_config(self):
        mock_op = MagicMock()
        mock_op.id = "op1"
        mock_op.name = "TestOp"
        mock_state = MagicMock()
        mock_topology = {mock_op: mock_state}

        op_config = TaskPoolOperatorConfig(
            id="op1",
            name="TestOp",
            max_concurrency=10,
        )
        config = ExecutionConfig(operators={"op1": op_config})

        store = MemoryExecutionConfigStore()
        store.put(config)

        controller = ConfigController(
            topology=mock_topology,
            config_store=store,
            poll_interval_s=0,  # Disable throttling for test
        )

        controller.try_apply_config()

        mock_op.apply_parallelism_config.assert_called_once_with(op_config)

    def test_try_apply_config_same_config_no_reapply(self):
        mock_op = MagicMock()
        mock_op.id = "op1"
        mock_op.name = "TestOp"
        mock_topology = {mock_op: MagicMock()}

        op_config = TaskPoolOperatorConfig(
            id="op1",
            name="TestOp",
            max_concurrency=10,
        )
        config = ExecutionConfig(operators={"op1": op_config})

        store = MemoryExecutionConfigStore()
        store.put(config)

        controller = ConfigController(
            topology=mock_topology,
            config_store=store,
            poll_interval_s=0,  # Disable throttling for test
        )

        # First call sets the config
        controller.try_apply_config()
        assert mock_op.apply_parallelism_config.call_count == 1

        # Second call with same config should not reapply
        controller.try_apply_config()
        assert mock_op.apply_parallelism_config.call_count == 1

    def test_try_apply_config_config_update(self):
        mock_op = MagicMock()
        mock_op.id = "op1"
        mock_op.name = "TestOp"
        mock_topology = {mock_op: MagicMock()}

        store = MemoryExecutionConfigStore()

        # First config
        config1 = ExecutionConfig(
            operators={"op1": TaskPoolOperatorConfig(
                id="op1", name="TestOp", max_concurrency=10
            )}
        )
        store.put(config1)

        controller = ConfigController(
            topology=mock_topology,
            config_store=store,
            poll_interval_s=0,  # Disable throttling for test
        )

        controller.try_apply_config()
        assert mock_op.apply_parallelism_config.call_count == 1

        # Update config
        config2 = ExecutionConfig(
            operators={"op1": TaskPoolOperatorConfig(
                id="op1", name="TestOp", max_concurrency=20
            )}
        )
        store.put(config2)

        controller.try_apply_config()
        assert mock_op.apply_parallelism_config.call_count == 2


class TestConfigControllerPollingThrottle:
    """Tests for ConfigController polling throttle behavior."""

    def test_throttle_skips_poll_within_interval(self):
        mock_topology = {}
        mock_store = MagicMock(spec=ExecutionConfigStore)
        mock_store.get.return_value = None

        controller = ConfigController(
            topology=mock_topology,
            config_store=mock_store,
            poll_interval_s=10.0,  # 10 second interval
        )

        # First call should poll
        controller.try_apply_config()
        assert mock_store.get.call_count == 1

        # Second call within interval should be skipped
        controller.try_apply_config()
        assert mock_store.get.call_count == 1

    def test_throttle_allows_poll_after_interval(self):
        mock_topology = {}
        mock_store = MagicMock(spec=ExecutionConfigStore)
        mock_store.get.return_value = None

        controller = ConfigController(
            topology=mock_topology,
            config_store=mock_store,
            poll_interval_s=0.01,  # 10ms interval for fast test
        )

        # First call should poll
        controller.try_apply_config()
        assert mock_store.get.call_count == 1

        # Wait for interval to pass
        time.sleep(0.02)

        # Second call should poll after interval
        controller.try_apply_config()
        assert mock_store.get.call_count == 2

    def test_zero_interval_allows_every_poll(self):
        mock_topology = {}
        mock_store = MagicMock(spec=ExecutionConfigStore)
        mock_store.get.return_value = None

        controller = ConfigController(
            topology=mock_topology,
            config_store=mock_store,
            poll_interval_s=0,  # No throttling
        )

        for _ in range(5):
            controller.try_apply_config()

        assert mock_store.get.call_count == 5


class TestConfigControllerOperatorMatching:
    """Tests for how ConfigController matches operators to configs."""

    def test_matches_operator_by_id(self):
        mock_op = MagicMock()
        mock_op.id = "custom_operator_id"
        mock_op.name = "TestOp"
        mock_topology = {mock_op: MagicMock()}

        op_config = TaskPoolOperatorConfig(
            id="custom_operator_id",
            name="TestOp",
            max_concurrency=10,
        )
        config = ExecutionConfig(operators={"custom_operator_id": op_config})

        store = MemoryExecutionConfigStore()
        store.put(config)

        controller = ConfigController(
            topology=mock_topology,
            config_store=store,
            poll_interval_s=0,
        )

        controller.try_apply_config()

        mock_op.apply_parallelism_config.assert_called_once_with(op_config)

    def test_skips_operators_not_in_config(self):
        mock_op1 = MagicMock()
        mock_op1.id = "op1"
        mock_op1.name = "Op1"

        mock_op2 = MagicMock()
        mock_op2.id = "op2"
        mock_op2.name = "Op2"

        mock_topology = {
            mock_op1: MagicMock(),
            mock_op2: MagicMock(),
        }

        # Config only has op1
        op_config = TaskPoolOperatorConfig(
            id="op1",
            name="Op1",
            max_concurrency=10,
        )
        config = ExecutionConfig(operators={"op1": op_config})

        store = MemoryExecutionConfigStore()
        store.put(config)

        controller = ConfigController(
            topology=mock_topology,
            config_store=store,
            poll_interval_s=0,
        )

        controller.try_apply_config()

        mock_op1.apply_parallelism_config.assert_called_once()
        mock_op2.apply_parallelism_config.assert_not_called()

    def test_handles_multiple_operators(self):
        mock_op1 = MagicMock()
        mock_op1.id = "op1"
        mock_op1.name = "Op1"

        mock_op2 = MagicMock()
        mock_op2.id = "op2"
        mock_op2.name = "Op2"

        mock_topology = {
            mock_op1: MagicMock(),
            mock_op2: MagicMock(),
        }

        op_config1 = TaskPoolOperatorConfig(id="op1", name="Op1", max_concurrency=10)
        op_config2 = TaskPoolOperatorConfig(id="op2", name="Op2", max_concurrency=20)
        config = ExecutionConfig(operators={"op1": op_config1, "op2": op_config2})

        store = MemoryExecutionConfigStore()
        store.put(config)

        controller = ConfigController(
            topology=mock_topology,
            config_store=store,
            poll_interval_s=0,
        )

        controller.try_apply_config()

        mock_op1.apply_parallelism_config.assert_called_once_with(op_config1)
        mock_op2.apply_parallelism_config.assert_called_once_with(op_config2)


class TestConfigControllerErrorHandling:
    """Tests for ConfigController error handling."""

    def test_handles_apply_config_exception(self):
        mock_op = MagicMock()
        mock_op.id = "op1"
        mock_op.name = "TestOp"
        mock_op.apply_parallelism_config.side_effect = Exception("Apply failed")
        mock_topology = {mock_op: MagicMock()}

        op_config = TaskPoolOperatorConfig(
            id="op1",
            name="TestOp",
            max_concurrency=10,
        )
        config = ExecutionConfig(operators={"op1": op_config})

        store = MemoryExecutionConfigStore()
        store.put(config)

        controller = ConfigController(
            topology=mock_topology,
            config_store=store,
            poll_interval_s=0,
        )

        # Should not raise, just log warning
        controller.try_apply_config()

    def test_continues_after_single_operator_failure(self):
        mock_op1 = MagicMock()
        mock_op1.id = "op1"
        mock_op1.name = "Op1"
        mock_op1.apply_parallelism_config.side_effect = Exception("Op1 failed")

        mock_op2 = MagicMock()
        mock_op2.id = "op2"
        mock_op2.name = "Op2"

        mock_topology = {
            mock_op1: MagicMock(),
            mock_op2: MagicMock(),
        }

        op_config1 = TaskPoolOperatorConfig(id="op1", name="Op1", max_concurrency=10)
        op_config2 = TaskPoolOperatorConfig(id="op2", name="Op2", max_concurrency=20)
        config = ExecutionConfig(operators={"op1": op_config1, "op2": op_config2})

        store = MemoryExecutionConfigStore()
        store.put(config)

        controller = ConfigController(
            topology=mock_topology,
            config_store=store,
            poll_interval_s=0,
        )

        # Should continue to op2 even if op1 fails
        controller.try_apply_config()

        mock_op1.apply_parallelism_config.assert_called_once()
        mock_op2.apply_parallelism_config.assert_called_once()


class TestConfigControllerWithActorPool:
    """Tests for ConfigController with ActorPoolOperatorConfig."""

    def test_try_apply_config_actor_pool_config(self):
        mock_op = MagicMock()
        mock_op.id = "actor_op"
        mock_op.name = "ActorOp"
        mock_topology = {mock_op: MagicMock()}

        op_config = ActorPoolOperatorConfig(
            id="actor_op",
            name="ActorOp",
            min_size=1,
            max_size=10,
            size=5,
        )
        config = ExecutionConfig(operators={"actor_op": op_config})

        store = MemoryExecutionConfigStore()
        store.put(config)

        controller = ConfigController(
            topology=mock_topology,
            config_store=store,
            poll_interval_s=0,
        )

        controller.try_apply_config()

        mock_op.apply_parallelism_config.assert_called_once_with(op_config)


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
