"""Tests for dynamic execution configuration management.

This module tests:
1. GcsExecutionConfigStore: storing/retrieving config from GCS
2. ConfigController: detecting and applying config changes to operators
"""

from unittest.mock import MagicMock

import pytest
import ray
from ray.data._internal.execution.config import (
    ExecutionConfig,
    TaskPoolOperatorConfig,
    ConfigController,
    GcsExecutionConfigStore,
    MemoryExecutionConfigStore,
)


@pytest.fixture(scope="function")
def ray_cluster():
    """Start and stop a Ray cluster for each test."""
    ray.init(num_cpus=4)
    yield
    ray.shutdown()


@pytest.fixture
def gcs_client(ray_cluster):
    """Get GCS client from running Ray cluster."""
    return ray._private.worker.global_worker.gcs_client


class TestGcsExecutionConfigStore:
    """Tests for GcsExecutionConfigStore operations."""

    def test_put_and_get(self, gcs_client):
        """Test basic store and retrieve operations."""
        store = GcsExecutionConfigStore(
            gcs_client=gcs_client,
            job_id="test_put_get",
        )

        config = ExecutionConfig(
            job_id="test_put_get",
            operators={
                "op1": TaskPoolOperatorConfig(
                    id="op1",
                    name="TestOperator",
                    max_concurrency=10,
                )
            },
        )

        store.put(config)
        retrieved = store.get()

        assert retrieved is not None
        assert retrieved.job_id == "test_put_get"
        assert "op1" in retrieved.operators
        assert retrieved.operators["op1"].max_concurrency == 10

    def test_update(self, gcs_client):
        """Test updating existing config."""
        store = GcsExecutionConfigStore(
            gcs_client=gcs_client,
            job_id="test_update",
        )

        # Initial config
        config = ExecutionConfig(
            job_id="test_update",
            operators={
                "op1": TaskPoolOperatorConfig(id="op1", name="Op1", max_concurrency=10)
            },
        )
        store.put(config)

        assert store.get().operators["op1"].max_concurrency == 10

        # Update config
        config.operators["op1"] = TaskPoolOperatorConfig(
            id="op1", name="Op1", max_concurrency=20
        )
        store.put(config)

        assert store.get().operators["op1"].max_concurrency == 20

    def test_cross_store_visibility(self, gcs_client):
        """Test that changes from one store are visible to another store."""
        store_a = GcsExecutionConfigStore(
            gcs_client=gcs_client,
            job_id="test_visibility",
        )
        store_b = GcsExecutionConfigStore(
            gcs_client=gcs_client,
            job_id="test_visibility",
        )

        # Store A writes
        config = ExecutionConfig(
            operators={
                "op1": TaskPoolOperatorConfig(id="op1", name="Op1", max_concurrency=5)
            }
        )
        store_a.put(config)

        # Store B reads
        assert store_b.get().operators["op1"].max_concurrency == 5

        # Store B updates
        config.operators["op1"] = TaskPoolOperatorConfig(
            id="op1", name="Op1", max_concurrency=20
        )
        store_b.put(config)

        # Store A sees the update
        assert store_a.get().operators["op1"].max_concurrency == 20

    def test_put_and_get_with_none_max_concurrency(self, gcs_client):
        """Test storing and retrieving config with max_concurrency=None."""
        store = GcsExecutionConfigStore(
            gcs_client=gcs_client,
            job_id="test_none_concurrency",
        )

        config = ExecutionConfig(
            job_id="test_none_concurrency",
            operators={
                "op1": TaskPoolOperatorConfig(
                    id="op1",
                    name="TestOperator",
                    max_concurrency=None,
                )
            },
        )

        store.put(config)
        retrieved = store.get()

        assert retrieved is not None
        assert "op1" in retrieved.operators
        assert retrieved.operators["op1"].max_concurrency is None

    def test_default_max_concurrency_is_none(self, gcs_client):
        """Test that TaskPoolOperatorConfig defaults max_concurrency to None."""
        config = TaskPoolOperatorConfig(id="op1", name="TestOperator")
        assert config.max_concurrency is None


class TestConfigController:
    """Tests for ConfigController behavior."""

    def _create_mock_operator(self, op_id: str, name: str = "MockOp"):
        """Helper to create a mock operator."""
        mock_op = MagicMock()
        mock_op.id = op_id
        mock_op.name = name
        return mock_op

    def test_apply_config_to_single_operator(self):
        """Test applying config to a single operator."""
        mock_op = self._create_mock_operator("op1")
        topology = {mock_op: MagicMock()}

        store = MemoryExecutionConfigStore()
        store.put(ExecutionConfig(
            operators={
                "op1": TaskPoolOperatorConfig(id="op1", name="Op1", max_concurrency=10)
            }
        ))

        controller = ConfigController(
            topology=topology,
            config_store=store,
            poll_interval_s=0,
        )
        controller.try_apply_config()

        mock_op.apply_parallelism_config.assert_called_once()
        applied_config = mock_op.apply_parallelism_config.call_args[0][0]
        assert applied_config.max_concurrency == 10

    def test_apply_config_to_multiple_operators(self):
        """Test applying different configs to multiple operators."""
        mock_op1 = self._create_mock_operator("op1", "MapBatches")
        mock_op2 = self._create_mock_operator("op2", "MapBatches")
        topology = {mock_op1: MagicMock(), mock_op2: MagicMock()}

        store = MemoryExecutionConfigStore()
        store.put(ExecutionConfig(
            operators={
                "op1": TaskPoolOperatorConfig(id="op1", name="MapBatches", max_concurrency=10),
                "op2": TaskPoolOperatorConfig(id="op2", name="MapBatches", max_concurrency=20),
            }
        ))

        controller = ConfigController(
            topology=topology,
            config_store=store,
            poll_interval_s=0,
        )
        controller.try_apply_config()

        assert mock_op1.apply_parallelism_config.call_args[0][0].max_concurrency == 10
        assert mock_op2.apply_parallelism_config.call_args[0][0].max_concurrency == 20

    def test_detect_and_apply_config_change(self):
        """Test that controller detects config changes and reapplies."""
        mock_op = self._create_mock_operator("op1")
        topology = {mock_op: MagicMock()}
        store = MemoryExecutionConfigStore()

        controller = ConfigController(
            topology=topology,
            config_store=store,
            poll_interval_s=0,
        )

        # First config
        store.put(ExecutionConfig(
            operators={
                "op1": TaskPoolOperatorConfig(id="op1", name="Op1", max_concurrency=5)
            }
        ))
        controller.try_apply_config()

        assert mock_op.apply_parallelism_config.call_count == 1
        assert mock_op.apply_parallelism_config.call_args[0][0].max_concurrency == 5

        # Update config
        store.put(ExecutionConfig(
            operators={
                "op1": TaskPoolOperatorConfig(id="op1", name="Op1", max_concurrency=15)
            }
        ))
        controller.try_apply_config()

        assert mock_op.apply_parallelism_config.call_count == 2
        assert mock_op.apply_parallelism_config.call_args[0][0].max_concurrency == 15

    def test_apply_config_with_none_max_concurrency(self):
        """Test applying config with max_concurrency=None (unlimited)."""
        mock_op = self._create_mock_operator("op1")
        topology = {mock_op: MagicMock()}

        store = MemoryExecutionConfigStore()
        store.put(ExecutionConfig(
            operators={
                "op1": TaskPoolOperatorConfig(id="op1", name="Op1", max_concurrency=None)
            }
        ))

        controller = ConfigController(
            topology=topology,
            config_store=store,
            poll_interval_s=0,
        )
        controller.try_apply_config()

        mock_op.apply_parallelism_config.assert_called_once()
        applied_config = mock_op.apply_parallelism_config.call_args[0][0]
        assert applied_config.max_concurrency is None

    def test_change_max_concurrency_from_value_to_none(self):
        """Test changing max_concurrency from a value to None (remove limit)."""
        mock_op = self._create_mock_operator("op1")
        topology = {mock_op: MagicMock()}
        store = MemoryExecutionConfigStore()

        controller = ConfigController(
            topology=topology,
            config_store=store,
            poll_interval_s=0,
        )

        # First config with limit
        store.put(ExecutionConfig(
            operators={
                "op1": TaskPoolOperatorConfig(id="op1", name="Op1", max_concurrency=10)
            }
        ))
        controller.try_apply_config()

        assert mock_op.apply_parallelism_config.call_count == 1
        assert mock_op.apply_parallelism_config.call_args[0][0].max_concurrency == 10

        # Update to unlimited (None)
        store.put(ExecutionConfig(
            operators={
                "op1": TaskPoolOperatorConfig(id="op1", name="Op1", max_concurrency=None)
            }
        ))
        controller.try_apply_config()

        assert mock_op.apply_parallelism_config.call_count == 2
        assert mock_op.apply_parallelism_config.call_args[0][0].max_concurrency is None

    def test_change_max_concurrency_from_none_to_value(self):
        """Test changing max_concurrency from None to a value (add limit)."""
        mock_op = self._create_mock_operator("op1")
        topology = {mock_op: MagicMock()}
        store = MemoryExecutionConfigStore()

        controller = ConfigController(
            topology=topology,
            config_store=store,
            poll_interval_s=0,
        )

        # First config without limit
        store.put(ExecutionConfig(
            operators={
                "op1": TaskPoolOperatorConfig(id="op1", name="Op1", max_concurrency=None)
            }
        ))
        controller.try_apply_config()

        assert mock_op.apply_parallelism_config.call_count == 1
        assert mock_op.apply_parallelism_config.call_args[0][0].max_concurrency is None

        # Update to limited
        store.put(ExecutionConfig(
            operators={
                "op1": TaskPoolOperatorConfig(id="op1", name="Op1", max_concurrency=5)
            }
        ))
        controller.try_apply_config()

        assert mock_op.apply_parallelism_config.call_count == 2
        assert mock_op.apply_parallelism_config.call_args[0][0].max_concurrency == 5

if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
