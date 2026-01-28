"""Tests for ExecutionConfigStorageClient (async GCS client)."""

from unittest.mock import MagicMock, AsyncMock

import pytest
from ray.dashboard.modules.data.config_client import \
    ExecutionConfigStorageClient
from ray.data._internal.execution.config import (
    ExecutionConfig,
    TaskPoolOperatorConfig,
)
from ray.data._internal.execution.config.store import (
    GCS_KEY_PREFIX,
    GCS_KEY_TEMPLATE,
)


class TestExecutionConfigStorageClient:
    """Tests for ExecutionConfigStorageClient class."""

    def test_client_creation(self):
        mock_gcs_client = MagicMock()
        client = ExecutionConfigStorageClient(mock_gcs_client)
        assert client._gcs_client is mock_gcs_client

    def test_get_key(self):
        mock_gcs_client = MagicMock()
        client = ExecutionConfigStorageClient(mock_gcs_client)

        key = client._get_key("test_job")
        expected = GCS_KEY_TEMPLATE.format(job_id="test_job").encode()
        assert key == expected

    def test_get_key_special_characters(self):
        mock_gcs_client = MagicMock()
        client = ExecutionConfigStorageClient(mock_gcs_client)

        # Test with special characters in job_id
        key = client._get_key("job-123_abc")
        expected = GCS_KEY_TEMPLATE.format(job_id="job-123_abc").encode()
        assert key == expected

    @pytest.mark.asyncio
    async def test_get_config_not_found(self):
        mock_gcs_client = MagicMock()
        mock_gcs_client.async_internal_kv_get = AsyncMock(return_value=None)

        client = ExecutionConfigStorageClient(mock_gcs_client)
        result = await client.get_config("test_job")

        assert result is None
        mock_gcs_client.async_internal_kv_get.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_config_found(self):
        config = ExecutionConfig(job_id="test_job")
        mock_gcs_client = MagicMock()
        mock_gcs_client.async_internal_kv_get = AsyncMock(
            return_value=config.to_json().encode()
        )

        client = ExecutionConfigStorageClient(mock_gcs_client)
        result = await client.get_config("test_job")

        assert result is not None
        assert isinstance(result, ExecutionConfig)
        assert result.job_id == "test_job"

    @pytest.mark.asyncio
    async def test_get_config_with_operators(self):
        op_config = TaskPoolOperatorConfig(
            id="op1",
            name="TestOp",
            max_concurrency=10,
        )
        config = ExecutionConfig(
            job_id="test_job",
            operators={"op1": op_config}
        )
        mock_gcs_client = MagicMock()
        mock_gcs_client.async_internal_kv_get = AsyncMock(
            return_value=config.to_json().encode()
        )

        client = ExecutionConfigStorageClient(mock_gcs_client)
        result = await client.get_config("test_job")

        assert result is not None
        assert result.job_id == "test_job"
        assert "op1" in result.operators
        assert result.operators["op1"].max_concurrency == 10

    @pytest.mark.asyncio
    async def test_put_config_new(self):
        config = ExecutionConfig(job_id="test_job")
        mock_gcs_client = MagicMock()
        mock_gcs_client.async_internal_kv_put = AsyncMock(return_value=1)

        client = ExecutionConfigStorageClient(mock_gcs_client)
        result = await client.put_config("test_job", config)

        assert result is True
        mock_gcs_client.async_internal_kv_put.assert_called_once()

    @pytest.mark.asyncio
    async def test_put_config_update(self):
        config = ExecutionConfig(job_id="test_job")
        mock_gcs_client = MagicMock()
        mock_gcs_client.async_internal_kv_put = AsyncMock(return_value=0)

        client = ExecutionConfigStorageClient(mock_gcs_client)
        result = await client.put_config("test_job", config)

        assert result is False

    @pytest.mark.asyncio
    async def test_put_config_with_operators(self):
        op_config = TaskPoolOperatorConfig(
            id="op1",
            name="TestOp",
            max_concurrency=10,
        )
        config = ExecutionConfig(
            job_id="test_job",
            operators={"op1": op_config}
        )
        mock_gcs_client = MagicMock()
        mock_gcs_client.async_internal_kv_put = AsyncMock(return_value=1)

        client = ExecutionConfigStorageClient(mock_gcs_client)
        result = await client.put_config("test_job", config)

        assert result is True
        # Verify the call contains the serialized config
        call_args = mock_gcs_client.async_internal_kv_put.call_args
        assert call_args is not None

    @pytest.mark.asyncio
    async def test_delete_config_found(self):
        mock_gcs_client = MagicMock()
        mock_gcs_client.async_internal_kv_get = AsyncMock(return_value=b"data")
        mock_gcs_client.async_internal_kv_del = AsyncMock()

        client = ExecutionConfigStorageClient(mock_gcs_client)
        result = await client.delete_config("test_job")

        assert result is True
        mock_gcs_client.async_internal_kv_del.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_config_not_found(self):
        mock_gcs_client = MagicMock()
        mock_gcs_client.async_internal_kv_get = AsyncMock(return_value=None)

        client = ExecutionConfigStorageClient(mock_gcs_client)
        result = await client.delete_config("test_job")

        assert result is False
        mock_gcs_client.async_internal_kv_del.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_all_configs_empty(self):
        mock_gcs_client = MagicMock()
        mock_gcs_client.async_internal_kv_keys = AsyncMock(return_value=[])

        client = ExecutionConfigStorageClient(mock_gcs_client)
        result = await client.get_all_configs()

        assert result == {}

    @pytest.mark.asyncio
    async def test_get_all_configs_with_data(self):
        config = ExecutionConfig(job_id="job1")
        key1 = f"{GCS_KEY_PREFIX}job1".encode()
        key2 = f"{GCS_KEY_PREFIX}job2".encode()

        mock_gcs_client = MagicMock()
        mock_gcs_client.async_internal_kv_keys = AsyncMock(return_value=[key1, key2])
        mock_gcs_client.async_internal_kv_get = AsyncMock(
            return_value=config.to_json().encode()
        )

        client = ExecutionConfigStorageClient(mock_gcs_client)
        result = await client.get_all_configs()

        assert len(result) == 2
        assert "job1" in result
        assert "job2" in result

    @pytest.mark.asyncio
    async def test_get_all_configs_with_operators(self):
        op_config = TaskPoolOperatorConfig(
            id="op1",
            name="TestOp",
            max_concurrency=10,
        )
        config = ExecutionConfig(
            job_id="job1",
            operators={"op1": op_config}
        )
        key1 = f"{GCS_KEY_PREFIX}job1".encode()

        mock_gcs_client = MagicMock()
        mock_gcs_client.async_internal_kv_keys = AsyncMock(return_value=[key1])
        mock_gcs_client.async_internal_kv_get = AsyncMock(
            return_value=config.to_json().encode()
        )

        client = ExecutionConfigStorageClient(mock_gcs_client)
        result = await client.get_all_configs()

        assert len(result) == 1
        assert "job1" in result
        assert "op1" in result["job1"].operators

    @pytest.mark.asyncio
    async def test_get_config_timeout(self):
        """Test that timeout parameter is passed correctly."""
        config = ExecutionConfig(job_id="test_job")
        mock_gcs_client = MagicMock()
        mock_gcs_client.async_internal_kv_get = AsyncMock(
            return_value=config.to_json().encode()
        )

        client = ExecutionConfigStorageClient(mock_gcs_client)
        result = await client.get_config("test_job", timeout=60)

        assert result is not None
        # Verify timeout was passed
        call_args = mock_gcs_client.async_internal_kv_get.call_args
        assert call_args is not None


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
