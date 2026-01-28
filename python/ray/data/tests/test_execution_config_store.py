"""Tests for ExecutionConfigStore implementations.

This module tests the storage backends for execution configuration:
- MemoryExecutionConfigStore: In-memory storage for testing
- GcsExecutionConfigStore: GCS-based persistent storage
- create_execution_config_store: Factory function
"""

from unittest.mock import MagicMock, patch

import pytest
from ray.data._internal.execution.config import (
    ExecutionConfig,
    TaskPoolOperatorConfig,
    MemoryExecutionConfigStore,
    GcsExecutionConfigStore,
    create_execution_config_store,
    GCS_KEY_PREFIX,
    GCS_KEY_TEMPLATE,
    GCS_NAMESPACE,
)


class TestMemoryExecutionConfigStore:
    """Tests for MemoryExecutionConfigStore class."""

    def test_memory_store_get_empty(self):
        store = MemoryExecutionConfigStore()
        assert store.get() is None

    def test_memory_store_put_and_get(self):
        store = MemoryExecutionConfigStore()
        config = ExecutionConfig()
        store.put(config)
        assert store.get() is config

    def test_memory_store_init_new(self):
        store = MemoryExecutionConfigStore()
        config = ExecutionConfig()
        result = store.init(config)
        assert result is True
        assert store.get() is config

    def test_memory_store_init_existing(self):
        store = MemoryExecutionConfigStore()
        config1 = ExecutionConfig()
        config2 = ExecutionConfig()
        store.init(config1)
        result = store.init(config2)
        assert result is False
        assert store.get() is config1

    def test_memory_store_put_overwrites(self):
        store = MemoryExecutionConfigStore()
        config1 = ExecutionConfig()
        config2 = ExecutionConfig()
        store.put(config1)
        store.put(config2)
        assert store.get() is config2

    def test_memory_store_with_job_id(self):
        store = MemoryExecutionConfigStore()
        config = ExecutionConfig(job_id="test_job")
        store.put(config)
        retrieved = store.get()
        assert retrieved.job_id == "test_job"

    def test_memory_store_with_operators(self):
        store = MemoryExecutionConfigStore()
        op_config = TaskPoolOperatorConfig(id="op1", name="TestOp", max_concurrency=10)
        config = ExecutionConfig(operators={"op1": op_config})
        store.put(config)
        retrieved = store.get()
        assert "op1" in retrieved.operators


class TestGcsExecutionConfigStore:
    """Tests for GcsExecutionConfigStore class."""

    def test_gcs_store_creation(self):
        mock_gcs_client = MagicMock()
        mock_gcs_client.internal_kv_get.return_value = None

        store = GcsExecutionConfigStore(
            gcs_client=mock_gcs_client,
            job_id="test_job",
        )
        assert store.job_id == "test_job"

    def test_gcs_store_default_job_id(self):
        mock_gcs_client = MagicMock()
        mock_gcs_client.internal_kv_get.return_value = None

        store = GcsExecutionConfigStore(gcs_client=mock_gcs_client)
        assert store.job_id == "default"

    def test_gcs_store_get_returns_none_when_empty(self):
        mock_gcs_client = MagicMock()
        mock_gcs_client.internal_kv_get.return_value = None

        store = GcsExecutionConfigStore(
            gcs_client=mock_gcs_client,
            job_id="test_job",
        )
        assert store.get() is None

    def test_gcs_store_put(self):
        mock_gcs_client = MagicMock()

        store = GcsExecutionConfigStore(
            gcs_client=mock_gcs_client,
            job_id="test_job",
        )

        config = ExecutionConfig()
        store.put(config)

        mock_gcs_client.internal_kv_put.assert_called_once()
        call_args = mock_gcs_client.internal_kv_put.call_args
        assert call_args[1]["overwrite"] is True
        assert call_args[1]["namespace"] == GCS_NAMESPACE

    def test_gcs_store_init_new(self):
        mock_gcs_client = MagicMock()
        mock_gcs_client.internal_kv_get.return_value = None
        mock_gcs_client.internal_kv_put.return_value = 1  # 1 means new key added

        store = GcsExecutionConfigStore(
            gcs_client=mock_gcs_client,
            job_id="test_job",
        )

        config = ExecutionConfig()
        result = store.init(config)

        assert result is True
        mock_gcs_client.internal_kv_put.assert_called_once()

    def test_gcs_store_init_existing(self):
        mock_gcs_client = MagicMock()
        mock_gcs_client.internal_kv_get.return_value = None
        mock_gcs_client.internal_kv_put.return_value = 0  # 0 means key exists

        store = GcsExecutionConfigStore(
            gcs_client=mock_gcs_client,
            job_id="test_job",
        )

        config = ExecutionConfig()
        result = store.init(config)

        assert result is False

    def test_gcs_store_get_reads_from_gcs(self):
        """Test that get() always reads from GCS (auto-refresh behavior)."""
        mock_gcs_client = MagicMock()
        config = ExecutionConfig(job_id="test_job")
        mock_gcs_client.internal_kv_get.return_value = config.to_json().encode()

        store = GcsExecutionConfigStore(
            gcs_client=mock_gcs_client,
            job_id="test_job",
        )

        result = store.get()
        assert result is not None
        assert result.job_id == "test_job"
        mock_gcs_client.internal_kv_get.assert_called()

    def test_gcs_store_get_with_operators(self):
        """Test that get() correctly deserializes operators from GCS."""
        mock_gcs_client = MagicMock()
        op_config = TaskPoolOperatorConfig(id="op1", name="TestOp", max_concurrency=10)
        config = ExecutionConfig(job_id="test_job", operators={"op1": op_config})
        mock_gcs_client.internal_kv_get.return_value = config.to_json().encode()

        store = GcsExecutionConfigStore(
            gcs_client=mock_gcs_client,
            job_id="test_job",
        )

        result = store.get()
        assert result is not None
        assert "op1" in result.operators
        assert result.operators["op1"].max_concurrency == 10

    def test_gcs_store_get_detects_external_updates(self):
        """Test that get() detects configuration changes made externally."""
        mock_gcs_client = MagicMock()

        # Initial config
        config1 = ExecutionConfig(job_id="test_job")
        op_config1 = TaskPoolOperatorConfig(id="op1", name="Op1", max_concurrency=5)
        config1.operators = {"op1": op_config1}

        # Updated config (simulating external update)
        config2 = ExecutionConfig(job_id="test_job")
        op_config2 = TaskPoolOperatorConfig(id="op1", name="Op1", max_concurrency=20)
        config2.operators = {"op1": op_config2}

        # First call returns config1, second call returns config2
        mock_gcs_client.internal_kv_get.side_effect = [
            config1.to_json().encode(),
            config2.to_json().encode(),
        ]

        store = GcsExecutionConfigStore(
            gcs_client=mock_gcs_client,
            job_id="test_job",
        )

        # First get
        result1 = store.get()
        assert result1.operators["op1"].max_concurrency == 5

        # Second get should reflect the external update
        result2 = store.get()
        assert result2.operators["op1"].max_concurrency == 20


class TestGcsConstants:
    """Tests for GCS storage constants."""

    def test_gcs_key_prefix(self):
        assert GCS_KEY_PREFIX == "ray_data_execution_config_"

    def test_gcs_key_template(self):
        key = GCS_KEY_TEMPLATE.format(job_id="test123")
        assert key == "ray_data_execution_config_test123"

    def test_gcs_key_template_with_special_chars(self):
        key = GCS_KEY_TEMPLATE.format(job_id="job-123_abc")
        assert key == "ray_data_execution_config_job-123_abc"

    def test_gcs_namespace(self):
        assert GCS_NAMESPACE == "ray_data_execution_config"


class TestCreateExecutionConfigStore:
    """Tests for create_execution_config_store function."""

    def test_create_memory_store(self):
        mock_context = MagicMock()
        mock_context.execution_config_store_type = "memory"

        store = create_execution_config_store(mock_context)
        assert isinstance(store, MemoryExecutionConfigStore)

    def test_create_memory_store_default(self):
        mock_context = MagicMock()
        mock_context.execution_config_store_type = None

        store = create_execution_config_store(mock_context)
        assert isinstance(store, MemoryExecutionConfigStore)

    def test_create_gcs_store(self):
        mock_context = MagicMock()
        mock_context.execution_config_store_type = "gcs"

        mock_gcs_client = MagicMock()
        mock_gcs_client.internal_kv_get.return_value = None

        with patch("ray._private.worker.global_worker") as mock_worker:
            mock_worker.gcs_client = mock_gcs_client
            store = create_execution_config_store(mock_context, job_id="test_job")
            assert isinstance(store, GcsExecutionConfigStore)
            assert store.job_id == "test_job"

    def test_create_kconf_store_missing_config(self):
        mock_context = MagicMock()
        mock_context.execution_config_store_type = "kconf"
        mock_context.execution_config_kconf_key = None
        mock_context.execution_config_kconf_token = None

        store = create_execution_config_store(mock_context)
        assert store is None

    def test_create_unknown_store_type(self):
        mock_context = MagicMock()
        mock_context.execution_config_store_type = "unknown"

        store = create_execution_config_store(mock_context)
        assert isinstance(store, MemoryExecutionConfigStore)


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
