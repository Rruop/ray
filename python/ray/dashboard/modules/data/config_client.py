"""Async GCS client for execution configuration operations.

This module provides an async client for reading and writing ExecutionConfig
to the GCS internal KV store, designed for use in dashboard API handlers.
"""

import logging
from typing import Dict, Optional

from ray._raylet import GcsClient
from ray.data._internal.execution.config import ExecutionConfig
from ray.data._internal.execution.config.store import (
    GCS_KEY_PREFIX,
    GCS_KEY_TEMPLATE,
    GCS_NAMESPACE,
)

logger = logging.getLogger(__name__)


class ExecutionConfigStorageClient:
    """
    Async client for ExecutionConfig storage in GCS.

    This client provides async methods for CRUD operations on ExecutionConfig
    in the GCS internal KV store. It is designed for use in dashboard
    API handlers where async operations are required.

    Uses the same key format and namespace as GcsExecutionConfigStore for
    consistency across sync and async access patterns.
    """

    def __init__(self, gcs_client: GcsClient):
        """
        Initialize the ExecutionConfigStorageClient.

        Args:
            gcs_client: The GCS client for accessing the global control store.
        """
        self._gcs_client = gcs_client

    def _get_key(self, job_id: str) -> bytes:
        """Get the GCS key for a job's configuration."""
        return GCS_KEY_TEMPLATE.format(job_id=job_id).encode()

    async def get_config(
        self, job_id: str, timeout: int = 30
    ) -> Optional[ExecutionConfig]:
        """
        Get the execution configuration from GCS.

        Args:
            job_id: The job ID.
            timeout: Timeout in seconds for the GCS operation.

        Returns:
            ExecutionConfig if found, None otherwise.

        Raises:
            Exception: If GCS operation fails.
        """
        serialized_config = await self._gcs_client.async_internal_kv_get(
            self._get_key(job_id),
            namespace=GCS_NAMESPACE,
            timeout=timeout,
        )

        if serialized_config is None:
            return None

        return ExecutionConfig.from_json(serialized_config.decode())

    async def put_config(
        self,
        job_id: str,
        config: ExecutionConfig,
        overwrite: bool = True,
        timeout: int = 30,
    ) -> bool:
        """
        Put the execution configuration to GCS.

        Args:
            job_id: The job ID.
            config: The execution configuration to store.
            overwrite: Whether to overwrite existing config.
            timeout: Timeout in seconds for the GCS operation.

        Returns:
            True if a new key was added, False if updated existing.

        Raises:
            Exception: If GCS operation fails.
        """
        config_data = config.to_json().encode()

        added_num = await self._gcs_client.async_internal_kv_put(
            self._get_key(job_id),
            config_data,
            overwrite,
            namespace=GCS_NAMESPACE,
            timeout=timeout,
        )

        if added_num == 1:
            logger.debug(f"Created new execution config for job: {job_id}")
            return True
        else:
            logger.debug(f"Updated existing execution config for job: {job_id}")
            return False

    async def delete_config(self, job_id: str, timeout: int = 30) -> bool:
        """
        Delete the execution configuration from GCS.

        Args:
            job_id: The job ID.
            timeout: Timeout in seconds for the GCS operation.

        Returns:
            True if deleted, False if not found.

        Raises:
            Exception: If GCS operation fails.
        """
        existing_data = await self._gcs_client.async_internal_kv_get(
            self._get_key(job_id),
            namespace=GCS_NAMESPACE,
            timeout=timeout,
        )

        if existing_data is None:
            return False

        await self._gcs_client.async_internal_kv_del(
            self._get_key(job_id),
            False,
            namespace=GCS_NAMESPACE,
            timeout=timeout,
        )

        logger.debug(f"Deleted execution config for job: {job_id}")
        return True

    async def get_all_configs(
        self, timeout: int = 30
    ) -> Dict[str, ExecutionConfig]:
        """
        Get all execution configurations from GCS.

        Args:
            timeout: Timeout in seconds for the GCS operation.

        Returns:
            Dictionary mapping job_id to ExecutionConfig.

        Raises:
            Exception: If GCS operation fails.
        """
        raw_keys = await self._gcs_client.async_internal_kv_keys(
            GCS_KEY_PREFIX.encode(),
            namespace=GCS_NAMESPACE,
            timeout=timeout,
        )

        result: Dict[str, ExecutionConfig] = {}
        prefix_len = len(GCS_KEY_PREFIX)

        for raw_key in raw_keys:
            key = raw_key.decode()
            if key.startswith(GCS_KEY_PREFIX):
                job_id = key[prefix_len:]
                config = await self.get_config(job_id, timeout)
                if config is not None:
                    result[job_id] = config

        return result
