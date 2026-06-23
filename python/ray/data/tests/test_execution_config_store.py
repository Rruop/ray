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
    GCS_KEY_TEMPLATE_WITH_DATASET,
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

    def test_memory_store_delete(self):
        store = MemoryExecutionConfigStore()
        config = ExecutionConfig(job_id="test_job")
        store.put(config)
        assert store.get() is not None
        result = store.delete()
        assert result is True
        assert store.get() is None

    def test_memory_store_delete_when_empty(self):
        store = MemoryExecutionConfigStore()
        result = store.delete()
        assert result is False

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

    def test_gcs_store_delete(self):
        mock_gcs_client = MagicMock()
        config = ExecutionConfig(job_id="test_job")
        mock_gcs_client.internal_kv_get.return_value = config.to_json().encode()

        store = GcsExecutionConfigStore(
            gcs_client=mock_gcs_client,
            job_id="test_job",
        )

        result = store.delete()
        assert result is True
        mock_gcs_client.internal_kv_del.assert_called_once()

    def test_gcs_store_delete_when_empty(self):
        mock_gcs_client = MagicMock()
        mock_gcs_client.internal_kv_get.return_value = None

        store = GcsExecutionConfigStore(
            gcs_client=mock_gcs_client,
            job_id="test_job",
        )

        result = store.delete()
        assert result is False
        mock_gcs_client.internal_kv_del.assert_not_called()


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

    def test_create_default_store_is_gcs(self):
        mock_context = MagicMock()
        mock_context.execution_config_store_type = None

        mock_gcs_client = MagicMock()
        mock_gcs_client.internal_kv_get.return_value = None

        with patch("ray._private.worker.global_worker") as mock_worker:
            mock_worker.gcs_client = mock_gcs_client
            store = create_execution_config_store(mock_context, job_id="test_job")
            assert isinstance(store, GcsExecutionConfigStore)

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

        with pytest.raises(ValueError, match="kconf_full_key"):
            create_execution_config_store(mock_context)

    def test_create_unknown_store_type(self):
        mock_context = MagicMock()
        mock_context.execution_config_store_type = "unknown"

        with pytest.raises(ValueError, match="Unknown store type"):
            create_execution_config_store(mock_context)


class TestExecutionConfigDatasetId:
    """Tests for ExecutionConfig dataset_id field."""

    def test_execution_config_default_dataset_id_is_none(self):
        config = ExecutionConfig()
        assert config.dataset_id is None

    def test_execution_config_with_dataset_id(self):
        config = ExecutionConfig(job_id="job1", dataset_id="ds_abc")
        assert config.dataset_id == "ds_abc"
        assert config.job_id == "job1"

    def test_execution_config_to_dict_includes_dataset_id(self):
        config = ExecutionConfig(job_id="job1", dataset_id="ds_abc")
        d = config.to_dict()
        assert d["dataset_id"] == "ds_abc"
        assert d["job_id"] == "job1"

    def test_execution_config_to_dict_excludes_none_dataset_id(self):
        config = ExecutionConfig(job_id="job1")
        d = config.to_dict()
        assert "dataset_id" not in d

    def test_execution_config_from_dict_with_dataset_id(self):
        data = {
            "job_id": "job1",
            "dataset_id": "ds_abc",
            "operators": {},
        }
        config = ExecutionConfig.from_dict(data)
        assert config.job_id == "job1"
        assert config.dataset_id == "ds_abc"

    def test_execution_config_from_dict_without_dataset_id(self):
        """Backward compatibility: old JSON without dataset_id."""
        data = {
            "job_id": "job1",
            "operators": {},
        }
        config = ExecutionConfig.from_dict(data)
        assert config.job_id == "job1"
        assert config.dataset_id is None

    def test_execution_config_json_roundtrip_with_dataset_id(self):
        op = TaskPoolOperatorConfig(id="op1", name="TestOp", max_concurrency=5)
        config = ExecutionConfig(
            job_id="job1", dataset_id="ds_xyz", operators={"op1": op}
        )
        json_str = config.to_json()
        restored = ExecutionConfig.from_json(json_str)
        assert restored.job_id == "job1"
        assert restored.dataset_id == "ds_xyz"
        assert "op1" in restored.operators
        assert restored.operators["op1"].max_concurrency == 5


class TestGcsExecutionConfigStoreDatasetId:
    """Tests for GcsExecutionConfigStore with dataset_id."""

    def test_gcs_store_with_dataset_id(self):
        mock_gcs_client = MagicMock()
        store = GcsExecutionConfigStore(
            gcs_client=mock_gcs_client,
            job_id="job1",
            dataset_id="ds_abc",
        )
        assert store.job_id == "job1"
        assert store.dataset_id == "ds_abc"

    def test_gcs_store_dataset_id_none_by_default(self):
        mock_gcs_client = MagicMock()
        store = GcsExecutionConfigStore(
            gcs_client=mock_gcs_client,
            job_id="job1",
        )
        assert store.dataset_id is None

    def test_gcs_store_key_with_dataset_id(self):
        mock_gcs_client = MagicMock()
        store = GcsExecutionConfigStore(
            gcs_client=mock_gcs_client,
            job_id="job1",
            dataset_id="ds_abc",
        )
        key = store._get_key()
        assert key == b"ray_data_execution_config_job1::ds_abc"

    def test_gcs_store_key_without_dataset_id(self):
        mock_gcs_client = MagicMock()
        store = GcsExecutionConfigStore(
            gcs_client=mock_gcs_client,
            job_id="job1",
        )
        key = store._get_key()
        assert key == b"ray_data_execution_config_job1"

    def test_gcs_store_different_datasets_use_different_keys(self):
        """Two datasets in the same job should use different GCS keys."""
        mock_gcs_client = MagicMock()

        store_ds1 = GcsExecutionConfigStore(
            gcs_client=mock_gcs_client,
            job_id="job1",
            dataset_id="dataset_A",
        )
        store_ds2 = GcsExecutionConfigStore(
            gcs_client=mock_gcs_client,
            job_id="job1",
            dataset_id="dataset_B",
        )

        assert store_ds1._get_key() != store_ds2._get_key()
        assert b"dataset_A" in store_ds1._get_key()
        assert b"dataset_B" in store_ds2._get_key()

    def test_gcs_store_dataset_key_no_collision_with_job_key(self):
        """Dataset key should not collide with job-level key."""
        mock_gcs_client = MagicMock()

        # Job-level store
        store_job = GcsExecutionConfigStore(
            gcs_client=mock_gcs_client,
            job_id="job1",
        )
        # Dataset-level store
        store_ds = GcsExecutionConfigStore(
            gcs_client=mock_gcs_client,
            job_id="job1",
            dataset_id="ds1",
        )

        job_key = store_job._get_key()
        ds_key = store_ds._get_key()
        assert job_key != ds_key
        # Dataset key uses :: separator, job key does not
        assert b"::" not in job_key
        assert b"::" in ds_key

    def test_gcs_store_put_and_get_with_dataset_id(self):
        """Test full put/get cycle with dataset-level isolation."""
        mock_gcs_client = MagicMock()

        config = ExecutionConfig(job_id="job1", dataset_id="ds1")
        op = TaskPoolOperatorConfig(id="op1", name="Map", max_concurrency=8)
        config.add_operator(op)

        mock_gcs_client.internal_kv_get.return_value = config.to_json().encode()

        store = GcsExecutionConfigStore(
            gcs_client=mock_gcs_client,
            job_id="job1",
            dataset_id="ds1",
        )

        store.put(config)
        result = store.get()

        assert result is not None
        assert result.dataset_id == "ds1"
        assert result.operators["op1"].max_concurrency == 8

        # Verify the correct key was used
        expected_key = b"ray_data_execution_config_job1::ds1"
        put_call = mock_gcs_client.internal_kv_put.call_args
        assert put_call[0][0] == expected_key

    def test_gcs_store_init_with_dataset_id(self):
        mock_gcs_client = MagicMock()
        mock_gcs_client.internal_kv_get.return_value = None
        mock_gcs_client.internal_kv_put.return_value = 1

        store = GcsExecutionConfigStore(
            gcs_client=mock_gcs_client,
            job_id="job1",
            dataset_id="ds1",
        )

        config = ExecutionConfig(job_id="job1", dataset_id="ds1")
        result = store.init(config)

        assert result is True
        expected_key = b"ray_data_execution_config_job1::ds1"
        get_call = mock_gcs_client.internal_kv_get.call_args
        assert get_call[0][0] == expected_key


class TestGcsKeyTemplateWithDataset:
    """Tests for GCS_KEY_TEMPLATE_WITH_DATASET constant."""

    def test_key_template_with_dataset_format(self):
        key = GCS_KEY_TEMPLATE_WITH_DATASET.format(
            job_id="test_job", dataset_id="ds1"
        )
        assert key == "ray_data_execution_config_test_job::ds1"

    def test_key_template_uses_double_colon_separator(self):
        """Ensure :: separator is used to avoid ambiguity with _ in IDs."""
        key = GCS_KEY_TEMPLATE_WITH_DATASET.format(
            job_id="job_with_underscores", dataset_id="ds_with_underscores"
        )
        assert "::" in key
        assert key == "ray_data_execution_config_job_with_underscores::ds_with_underscores"

    def test_key_template_no_collision_with_job_template(self):
        """Dataset template key should never match job-level template key."""
        job_key = GCS_KEY_TEMPLATE.format(job_id="abc")
        ds_key = GCS_KEY_TEMPLATE_WITH_DATASET.format(
            job_id="abc", dataset_id="ds1"
        )
        assert job_key != ds_key
        assert not ds_key.startswith(job_key + "_")  # No underscore collision


class TestCreateExecutionConfigStoreWithDatasetId:
    """Tests for create_execution_config_store with dataset_id parameter."""

    def test_create_gcs_store_with_dataset_id(self):
        mock_context = MagicMock()
        mock_context.execution_config_store_type = "gcs"
        mock_gcs_client = MagicMock()

        with patch("ray._private.worker.global_worker") as mock_worker:
            mock_worker.gcs_client = mock_gcs_client
            store = create_execution_config_store(
                mock_context, job_id="job1", dataset_id="ds1"
            )
            assert isinstance(store, GcsExecutionConfigStore)
            assert store.job_id == "job1"
            assert store.dataset_id == "ds1"

    def test_create_gcs_store_without_dataset_id(self):
        mock_context = MagicMock()
        mock_context.execution_config_store_type = "gcs"
        mock_gcs_client = MagicMock()

        with patch("ray._private.worker.global_worker") as mock_worker:
            mock_worker.gcs_client = mock_gcs_client
            store = create_execution_config_store(
                mock_context, job_id="job1"
            )
            assert isinstance(store, GcsExecutionConfigStore)
            assert store.job_id == "job1"
            assert store.dataset_id is None


class TestConfigCleanup:
    """Tests for config cleanup on executor shutdown."""

    def test_delete_calls_store(self):
        """Verify _maybe_delete_execution_config triggers config store deletion when no exception."""
        from ray.data._internal.execution.streaming_executor import StreamingExecutor

        executor = MagicMock(spec=StreamingExecutor)
        executor._data_context = MagicMock()
        executor._data_context.delete_execution_config_on_completion = True
        store_mock = MagicMock()
        executor._config_store = store_mock
        executor._dataset_id = "test_dataset"

        StreamingExecutor._maybe_delete_execution_config(executor)
        store_mock.delete.assert_called_once()

    def test_delete_skips_on_exception(self):
        """Verify deletion is skipped when an exception occurred."""
        from ray.data._internal.execution.streaming_executor import StreamingExecutor

        executor = MagicMock(spec=StreamingExecutor)
        executor._data_context = MagicMock()
        executor._data_context.delete_execution_config_on_completion = True
        executor._config_store = MagicMock()
        executor._dataset_id = "test_dataset"

        StreamingExecutor._maybe_delete_execution_config(executor, exception=RuntimeError("fail"))
        executor._config_store.delete.assert_not_called()

    def test_delete_skips_when_disabled(self):
        """Verify deletion respects delete_execution_config_on_completion=False."""
        from ray.data._internal.execution.streaming_executor import StreamingExecutor

        executor = MagicMock(spec=StreamingExecutor)
        executor._data_context = MagicMock()
        executor._data_context.delete_execution_config_on_completion = False
        executor._config_store = MagicMock()

        StreamingExecutor._maybe_delete_execution_config(executor)
        executor._config_store.delete.assert_not_called()

    def test_delete_skips_when_no_store(self):
        """Verify deletion is safe when config store is None."""
        from ray.data._internal.execution.streaming_executor import StreamingExecutor

        executor = MagicMock(spec=StreamingExecutor)
        executor._data_context = MagicMock()
        executor._data_context.delete_execution_config_on_completion = True
        executor._config_store = None

        StreamingExecutor._maybe_delete_execution_config(executor)

    def test_delete_is_idempotent(self):
        """Verify calling _maybe_delete twice is safe (second call is a no-op)."""
        from ray.data._internal.execution.streaming_executor import StreamingExecutor

        executor = MagicMock(spec=StreamingExecutor)
        executor._data_context = MagicMock()
        executor._data_context.delete_execution_config_on_completion = True
        store_mock = MagicMock()
        executor._config_store = store_mock
        executor._dataset_id = "test_dataset"

        StreamingExecutor._maybe_delete_execution_config(executor)
        store_mock.delete.assert_called_once()
        assert executor._config_store is None

        StreamingExecutor._maybe_delete_execution_config(executor)

    def test_delete_safe_on_exception(self):
        """Verify deletion is safe when _data_context access raises."""
        from ray.data._internal.execution.streaming_executor import StreamingExecutor

        executor = MagicMock(spec=StreamingExecutor)
        type(executor)._data_context = property(
            lambda self: (_ for _ in ()).throw(AttributeError("partially torn down"))
        )
        executor._config_store = MagicMock()

        StreamingExecutor._maybe_delete_execution_config(executor)


class TestCreateExecutionConfigStoreWithKconfKey:
    """Tests for create_execution_config_store with custom kconf_key parameter."""

    def _make_kconf_context(self):
        ctx = MagicMock()
        ctx.execution_config_store_type = "kconf"
        ctx.execution_config_kconf_key = "KAIWorks.rayExecutionConfig"
        ctx.execution_config_kconf_token = "kconf_test_token"
        return ctx

    def test_kconf_key_suffix_appends_prefix(self):
        ctx = self._make_kconf_context()
        with patch(
            "ray.data._internal.execution.config.store.KconfExecutionConfigStore"
        ) as MockStore:
            mock_instance = MagicMock()
            MockStore.return_value = mock_instance
            store = create_execution_config_store(
                ctx, job_id="job1", kconf_full_key="KAIWorks.rayExecutionConfig.myJob"
            )
            MockStore.assert_called_once_with(
                key="KAIWorks.rayExecutionConfig.myJob", token="kconf_test_token"
            )
            assert store is mock_instance

    def test_kconf_key_full_uses_key_directly(self):
        ctx = self._make_kconf_context()
        with patch(
            "ray.data._internal.execution.config.store.KconfExecutionConfigStore"
        ) as MockStore:
            mock_instance = MagicMock()
            MockStore.return_value = mock_instance
            store = create_execution_config_store(
                ctx,
                job_id="job1",
                kconf_full_key="webserver.activity.trafficCouponConfigConfigMap",
            )
            MockStore.assert_called_once_with(
                key="webserver.activity.trafficCouponConfigConfigMap",
                token="kconf_test_token",
            )
            assert store is mock_instance

    def test_kconf_key_no_job_id_required(self):
        ctx = self._make_kconf_context()
        with patch(
            "ray.data._internal.execution.config.store.KconfExecutionConfigStore"
        ) as MockStore:
            MockStore.return_value = MagicMock()
            store = create_execution_config_store(
                ctx, job_id=None, kconf_full_key="KAIWorks.rayExecutionConfig.myJob"
            )
            assert store is not None

    def test_kconf_key_overrides_default_construction(self):
        ctx = self._make_kconf_context()
        with patch(
            "ray.data._internal.execution.config.store.KconfExecutionConfigStore"
        ) as MockStore:
            MockStore.return_value = MagicMock()
            store = create_execution_config_store(
                ctx,
                job_id="job1",
                dataset_id="ds1",
                kconf_full_key="KAIWorks.rayExecutionConfig.myJob",
            )
            MockStore.assert_called_once_with(
                key="KAIWorks.rayExecutionConfig.myJob", token="kconf_test_token"
            )

    def test_kconf_default_key_construction(self):
        from ray.data._internal.execution.config.store import _build_default_kconf_key
        key = _build_default_kconf_key(
            prefix="KAIWorks.rayExecutionConfig",
            job_id="job1",
            dataset_id="ds1",
        )
        assert key == "KAIWorks.rayExecutionConfig.job_job1__dataset_ds1"

    def test_kconf_default_key_without_dataset(self):
        from ray.data._internal.execution.config.store import _build_default_kconf_key
        key = _build_default_kconf_key(
            prefix="KAIWorks.rayExecutionConfig",
            job_id="job1",
        )
        assert key == "KAIWorks.rayExecutionConfig.job_job1"

    def test_kconf_default_key_requires_prefix(self):
        from ray.data._internal.execution.config.store import _build_default_kconf_key
        with pytest.raises(ValueError, match="prefix is required"):
            _build_default_kconf_key(
                prefix="",
                job_id="job1",
            )

    def test_kconf_full_key_missing_raises(self):
        ctx = self._make_kconf_context()
        with pytest.raises(ValueError, match="kconf_full_key"):
            create_execution_config_store(ctx, job_id="job1")

    def test_gcs_store_ignores_kconf_key(self):
        ctx = MagicMock()
        ctx.execution_config_store_type = "gcs"
        mock_gcs_client = MagicMock()
        with patch("ray._private.worker.global_worker") as mock_worker:
            mock_worker.gcs_client = mock_gcs_client
            store = create_execution_config_store(
                ctx, job_id="job1", kconf_full_key="KAIWorks.rayExecutionConfig.myJob"
            )
            assert isinstance(store, GcsExecutionConfigStore)

    def test_memory_store_ignores_kconf_key(self):
        ctx = MagicMock()
        ctx.execution_config_store_type = "memory"
        store = create_execution_config_store(
            ctx, kconf_full_key="KAIWorks.rayExecutionConfig.myJob"
        )
        assert isinstance(store, MemoryExecutionConfigStore)

    def test_kconf_key_suffix_invalid_format_raises(self):
        ctx = self._make_kconf_context()
        for bad in ["1starts_with_digit", "has.dot", "has space", "has/slash", "_leading_underscore"]:
            with patch(
                "ray.data._internal.execution.config.store.KconfExecutionConfigStore"
            ):
                with pytest.raises(ValueError, match="suffix"):
                    create_execution_config_store(ctx, job_id="job1", kconf_full_key=f"KAIWorks.rayExecutionConfig.{bad}")

    def test_kconf_key_full_valid_three_levels(self):
        ctx = self._make_kconf_context()
        with patch(
            "ray.data._internal.execution.config.store.KconfExecutionConfigStore"
        ) as MockStore:
            MockStore.return_value = MagicMock()
            create_execution_config_store(
                ctx,
                kconf_full_key="webserver.activity.trafficCouponConfigConfigMap",
            )
            MockStore.assert_called_once()

    def test_kconf_key_full_invalid_levels_raises(self):
        ctx = self._make_kconf_context()
        for bad in ["onlyone", "two.levels", "four.levels.too.many"]:
            with patch(
                "ray.data._internal.execution.config.store.KconfExecutionConfigStore"
            ):
                with pytest.raises(ValueError, match="3-level"):
                    create_execution_config_store(
                        ctx, kconf_full_key=bad
                    )

    def test_kconf_key_full_invalid_segment_raises(self):
        ctx = self._make_kconf_context()
        for bad in ["1bad.system.config", "biz.2sys.config", "biz.sys.has space"]:
            with patch(
                "ray.data._internal.execution.config.store.KconfExecutionConfigStore"
            ):
                with pytest.raises(ValueError, match="segment"):
                    create_execution_config_store(
                        ctx, kconf_full_key=bad
                    )


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
