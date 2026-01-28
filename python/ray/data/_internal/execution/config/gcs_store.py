"""GCS-based execution configuration store implementation."""

import logging
import threading
from typing import Optional

from ray._raylet import GcsClient
from ray.data._internal.execution.config.models import ExecutionConfig
from ray.data._internal.execution.config.store import (
    ExecutionConfigStore,
    GCS_KEY_TEMPLATE,
    GCS_NAMESPACE,
)

logger = logging.getLogger(__name__)


class GcsExecutionConfigStore(ExecutionConfigStore):
    """
    GCS-based store for execution configuration.

    This implementation stores configuration in the Ray GCS (Global Control Store)
    using a key-value storage mechanism. It provides persistent storage that
    survives process restarts and is accessible across the Ray cluster.

    The `get()` method always reads from GCS to ensure the latest configuration
    is returned. This allows external updates (e.g., from dashboard API) to be
    detected automatically.

    This is a synchronous implementation for use in the streaming executor.
    For async operations (e.g., dashboard API), use ExecutionConfigStorageClient.
    """

    def __init__(
        self,
        gcs_client: GcsClient,
        job_id: Optional[str] = None,
        timeout: int = 30
    ):
        """
        Initialize GCS-based config store.

        Args:
            gcs_client: The GCS client for accessing the global control store.
            job_id: Job ID to associate with this configuration.
                   If not provided, "default" will be used.
            timeout: Timeout in seconds for GCS operations.
        """
        self._gcs_client = gcs_client
        self._job_id = job_id or "default"
        self._timeout = timeout
        self._lock = threading.Lock()

    def _get_key(self) -> bytes:
        """Get the GCS key for this job's configuration."""
        return GCS_KEY_TEMPLATE.format(job_id=self._job_id).encode()

    def get(self) -> Optional[ExecutionConfig]:
        """Get the current execution configuration from GCS.

        This method always reads from GCS to ensure the latest configuration
        is returned. This allows detecting external updates made via the
        dashboard API.

        Returns:
            The current ExecutionConfig, or None if not found.
        """
        with self._lock:
            try:
                serialized_config = self._gcs_client.internal_kv_get(
                    self._get_key(),
                    namespace=GCS_NAMESPACE,
                    timeout=self._timeout,
                )

                if serialized_config is not None:
                    return ExecutionConfig.from_json(serialized_config.decode())
                return None

            except Exception as e:
                logger.warning(f"Failed to get configuration from GCS: {e}")
                return None

    def put(self, config: ExecutionConfig) -> None:
        """Store or update the execution configuration in GCS."""
        with self._lock:
            try:
                config_data = config.to_json().encode()

                self._gcs_client.internal_kv_put(
                    self._get_key(),
                    config_data,
                    overwrite=True,
                    namespace=GCS_NAMESPACE,
                    timeout=self._timeout,
                )
                logger.debug(f"Updated configuration in GCS for job: {self._job_id}")

            except Exception as e:
                logger.error(f"Failed to update configuration in GCS: {e}")
                raise

    def init(self, config: ExecutionConfig) -> bool:
        """Initialize the configuration if it doesn't exist.

        Returns:
            True if created, False if already exists.
        """
        with self._lock:
            try:
                existing_data = self._gcs_client.internal_kv_get(
                    self._get_key(),
                    namespace=GCS_NAMESPACE,
                    timeout=self._timeout,
                )

                if existing_data is not None:
                    return False

                config_data = config.to_json().encode()
                added_num = self._gcs_client.internal_kv_put(
                    self._get_key(),
                    config_data,
                    overwrite=False,
                    namespace=GCS_NAMESPACE,
                    timeout=self._timeout,
                )

                if added_num == 1:
                    logger.debug(f"Created new configuration in GCS for job: {self._job_id}")
                    return True
                return False

            except Exception as e:
                logger.error(f"Failed to initialize configuration in GCS: {e}")
                return False

    @property
    def job_id(self) -> str:
        """Get the job ID associated with this store."""
        return self._job_id
