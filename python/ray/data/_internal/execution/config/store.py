"""Abstract interface for execution configuration persistence.

This module defines the abstract interface for storing and retrieving
ExecutionConfig objects. Implementations include in-memory, GCS-based,
and Kconf-based stores.
"""

import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Optional

from ray.data._internal.execution.config.models import ExecutionConfig

if TYPE_CHECKING:
    from ray.data.context import DataContext

logger = logging.getLogger(__name__)

# GCS storage constants - used by GcsExecutionConfigStore and config_client
GCS_KEY_PREFIX = "ray_data_execution_config_"
GCS_KEY_TEMPLATE = f"{GCS_KEY_PREFIX}{{job_id}}"
GCS_NAMESPACE = "ray_data_execution_config"


class ExecutionConfigStore(ABC):
    """Abstract interface for execution configuration persistence.

    Implementations must ensure that `get()` returns the latest configuration,
    detecting any external updates (e.g., from dashboard API).
    """

    @abstractmethod
    def get(self) -> Optional[ExecutionConfig]:
        """Get the current execution configuration.

        Implementations should return the latest configuration, detecting
        any external updates made via other clients (e.g., dashboard API).
        """
        pass

    @abstractmethod
    def put(self, config: ExecutionConfig) -> None:
        """Store or update the execution configuration."""
        pass

    @abstractmethod
    def init(self, config: ExecutionConfig) -> bool:
        """Initialize the configuration if it doesn't exist.

        Returns:
            True if created, False if already exists.
        """
        pass


def create_execution_config_store(
    data_context: "DataContext",
    job_id: Optional[str] = None,
) -> Optional[ExecutionConfigStore]:
    """
    Create an execution configuration store based on DataContext configuration.

    Args:
        data_context: The DataContext containing store configuration.
        job_id: Job/execution ID (required for gcs and kconf stores).

    Returns:
        ExecutionConfigStore instance, or None if creation fails.
    """
    store_type = data_context.execution_config_store_type or "gcs"

    if store_type == "gcs":
        from .gcs_store import GcsExecutionConfigStore
        import ray
        return GcsExecutionConfigStore(
            gcs_client=ray._private.worker.global_worker.gcs_client,
            job_id=job_id,
        )

    if store_type == "memory":
        from .memory_store import MemoryExecutionConfigStore
        return MemoryExecutionConfigStore()

    if store_type == "kconf":
        try:
            from .kconf_store import KconfExecutionConfigStore
        except ImportError:
            logger.warning(
                "Kconf store requires 'infra-framework' package. "
                "Install with: pip install 'ray[data-kconf]'"
            )
            return None

        kconf_key = data_context.execution_config_kconf_key
        kconf_token = data_context.execution_config_kconf_token

        if not kconf_key or not kconf_token:
            logger.warning(
                "Kconf store requires key and token. "
                "Set RAY_DATA_EXECUTION_CONFIG_KCONF_KEY and "
                "RAY_DATA_EXECUTION_CONFIG_KCONF_TOKEN."
            )
            return None

        full_key = f"{kconf_key}.{job_id}" if job_id else kconf_key
        return KconfExecutionConfigStore(key=full_key, token=kconf_token)

    logger.warning(f"Unknown store type: {store_type}, using memory store")
    from .memory_store import MemoryExecutionConfigStore
    return MemoryExecutionConfigStore()
