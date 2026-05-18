"""Async GCS client for execution configuration operations.

This module provides an async client for reading and writing ExecutionConfig
to the GCS internal KV store, designed for use in dashboard API handlers.
"""

import asyncio
import logging
from typing import Dict, Optional

from ray._raylet import GcsClient
from ray.data._internal.execution.config import ExecutionConfig
from ray.data._internal.execution.config.store import (
    GCS_KEY_PREFIX,
    GCS_KEY_TEMPLATE_WITH_DATASET,
    GCS_NAMESPACE,
)

logger = logging.getLogger(__name__)


class ExecutionConfigStorageClient:
    """Async client for ExecutionConfig storage in GCS.

    Provides async CRUD operations on ExecutionConfig in the GCS internal KV
    store, designed for use in dashboard API handlers.

    Uses the same key format and namespace as GcsExecutionConfigStore for
    consistency across sync and async access patterns.
    """

    def __init__(self, gcs_client: GcsClient):
        self._gcs_client = gcs_client

    async def get_all_configs(
        self, timeout: int = 30
    ) -> Dict[str, Dict[str, ExecutionConfig]]:
        """Get all execution configurations from GCS, grouped by job_id.

        Scans all dataset-level keys (containing "::") and groups them by job.

        Args:
            timeout: Timeout in seconds for the GCS operation.

        Returns:
            Dictionary mapping job_id to dict of dataset_id -> ExecutionConfig.
        """
        raw_keys = await self._gcs_client.async_internal_kv_keys(
            GCS_KEY_PREFIX.encode(),
            namespace=GCS_NAMESPACE,
            timeout=timeout,
        )

        jobs_datasets = []
        prefix_len = len(GCS_KEY_PREFIX)

        for raw_key in raw_keys:
            suffix = raw_key.decode()[prefix_len:]
            if "::" not in suffix:
                logger.warning(
                    f"Skipping legacy key without dataset_id: {raw_key.decode()}"
                )
                continue
            job_id, dataset_id = suffix.split("::", 1)
            jobs_datasets.append((job_id, dataset_id))

        configs = await asyncio.gather(
            *(
                self.get_config_for_dataset(job_id, dataset_id, timeout)
                for job_id, dataset_id in jobs_datasets
            )
        )

        result: Dict[str, Dict[str, ExecutionConfig]] = {}
        for (job_id, dataset_id), config in zip(jobs_datasets, configs):
            if config is not None:
                result.setdefault(job_id, {})[dataset_id] = config

        return result

    def _get_key_with_dataset(self, job_id: str, dataset_id: str) -> bytes:
        """Get the GCS key for a job+dataset specific configuration."""
        return GCS_KEY_TEMPLATE_WITH_DATASET.format(
            job_id=job_id, dataset_id=dataset_id
        ).encode()

    async def get_config_for_dataset(
        self, job_id: str, dataset_id: str, timeout: int = 30
    ) -> Optional[ExecutionConfig]:
        """Get the execution configuration for a specific dataset within a job."""
        serialized_config = await self._gcs_client.async_internal_kv_get(
            self._get_key_with_dataset(job_id, dataset_id),
            namespace=GCS_NAMESPACE,
            timeout=timeout,
        )

        if serialized_config is None:
            return None

        return ExecutionConfig.from_json(serialized_config.decode())

    async def put_config_for_dataset(
        self,
        job_id: str,
        dataset_id: str,
        config: ExecutionConfig,
        overwrite: bool = True,
        timeout: int = 30,
    ) -> bool:
        """Put the execution configuration for a specific dataset within a job.

        Returns:
            True if a new key was added, False if updated existing.
        """
        config_data = config.to_json().encode()

        added_num = await self._gcs_client.async_internal_kv_put(
            self._get_key_with_dataset(job_id, dataset_id),
            config_data,
            overwrite,
            namespace=GCS_NAMESPACE,
            timeout=timeout,
        )

        is_new = added_num == 1
        action = "Created new" if is_new else "Updated existing"
        logger.debug(
            f"{action} execution config for job: {job_id}, dataset: {dataset_id}"
        )
        return is_new

    async def delete_config_for_dataset(
        self, job_id: str, dataset_id: str, timeout: int = 30
    ) -> bool:
        """Delete the execution configuration for a specific dataset within a job.

        Returns:
            True if deleted, False if not found.
        """
        key = self._get_key_with_dataset(job_id, dataset_id)

        existing_data = await self._gcs_client.async_internal_kv_get(
            key,
            namespace=GCS_NAMESPACE,
            timeout=timeout,
        )

        if existing_data is None:
            return False

        await self._gcs_client.async_internal_kv_del(
            key,
            False,
            namespace=GCS_NAMESPACE,
            timeout=timeout,
        )

        logger.debug(
            f"Deleted execution config for job: {job_id}, dataset: {dataset_id}"
        )
        return True

    async def get_configs_for_job(
        self, job_id: str, timeout: int = 30
    ) -> Dict[str, ExecutionConfig]:
        """Get all dataset-level execution configurations for a specific job.

        Scans for keys matching the pattern
        ray_data_execution_config_{job_id}::{dataset_id}
        and returns a mapping from dataset_id to ExecutionConfig.

        Args:
            job_id: The job ID.
            timeout: Timeout in seconds for the GCS operation.

        Returns:
            Dictionary mapping dataset_id to ExecutionConfig.
        """
        job_prefix = f"{GCS_KEY_PREFIX}{job_id}::"

        raw_keys = await self._gcs_client.async_internal_kv_keys(
            job_prefix.encode(),
            namespace=GCS_NAMESPACE,
            timeout=timeout,
        )

        prefix_len = len(job_prefix)
        dataset_ids = [raw_key.decode()[prefix_len:] for raw_key in raw_keys]

        configs = await asyncio.gather(
            *(
                self.get_config_for_dataset(job_id, dataset_id, timeout)
                for dataset_id in dataset_ids
            )
        )

        result: Dict[str, ExecutionConfig] = {}
        for dataset_id, config in zip(dataset_ids, configs):
            if config is not None:
                result[dataset_id] = config

        return result
