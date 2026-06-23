"""Abstract interface for execution configuration persistence.

This module defines the abstract interface for storing and retrieving
ExecutionConfig objects. Implementations include in-memory, GCS-based,
and Kconf-based stores.
"""

import logging
import re
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Optional

from ray.data._internal.execution.config.models import ExecutionConfig

if TYPE_CHECKING:
    from ray.data.context import DataContext

logger = logging.getLogger(__name__)

# Resolved lazily on first use to avoid a circular import with kconf_store.
# Tests may patch this attribute directly.
KconfExecutionConfigStore = None


def _resolve_kconf_store_cls():
    """Resolve KconfExecutionConfigStore lazily (avoids circular import)."""
    global KconfExecutionConfigStore
    if KconfExecutionConfigStore is not None:
        return KconfExecutionConfigStore
    try:
        from .kconf_store import KconfExecutionConfigStore as _cls
    except ImportError:
        return None
    KconfExecutionConfigStore = _cls
    return _cls


_KCONF_SEGMENT_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9_-]*$")


def validate_kconf_key(kconf_key: str, full: bool) -> None:
    """Validate kconf key format.

    Each segment must start with a letter and contain only letters, digits,
    underscores, and hyphens. Full keys must be a 3-level dot-separated path
    (business.system.config_name); suffix keys must be a single segment.
    """
    if full:
        segments = kconf_key.split(".")
        if len(segments) != 3:
            raise ValueError(
                f"kconf_key must be a 3-level dot-separated path "
                f"(business.system.config_name), got: {kconf_key!r}"
            )
        parts = segments
    else:
        parts = [kconf_key]

    for seg in parts:
        if not _KCONF_SEGMENT_RE.match(seg):
            raise ValueError(
                f"Invalid kconf_key segment {seg!r} in {kconf_key!r}: "
                f"each segment must start with a letter and contain only "
                f"letters, digits, '_', and '-'."
            )

# GCS storage constants - used by GcsExecutionConfigStore and config_client
GCS_KEY_PREFIX = "ray_data_execution_config_"
GCS_KEY_TEMPLATE = f"{GCS_KEY_PREFIX}{{job_id}}"
GCS_KEY_TEMPLATE_WITH_DATASET = f"{GCS_KEY_PREFIX}{{job_id}}::{{dataset_id}}"
GCS_NAMESPACE = "ray_data_execution_config"


def build_default_kconf_key(
    prefix: str,
    job_id: str,
    dataset_id: Optional[str] = None,
) -> str:
    """Build a default kconf key from prefix + job_id (and optional dataset_id)."""
    if not prefix:
        raise ValueError("prefix is required for default key from job_id")
    suffix = f"job_{job_id}"
    if dataset_id is not None:
        suffix += f"__dataset_{dataset_id}"
    return f"{prefix}.{suffix}"


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

    @abstractmethod
    def delete(self) -> bool:
        """Delete the stored configuration.

        Returns:
            True if deleted, False if not found.
        """
        pass


def create_execution_config_store(
    data_context: "DataContext",
    job_id: Optional[str] = None,
    dataset_id: Optional[str] = None,
    kconf_full_key: Optional[str] = None,
    store_type: Optional[str] = None,
) -> ExecutionConfigStore:
    """
    Create an execution configuration store based on DataContext configuration.

    Args:
        data_context: The DataContext containing store configuration.
        job_id: Job/execution ID (required for gcs stores).
        dataset_id: Optional dataset ID for per-dataset config isolation
            in gcs stores.
        kconf_full_key: Full kconf key for kconf store. Required when
            store_type is "kconf".
        store_type: Override store type. If provided, takes precedence
            over data_context.execution_config_store_type.

    Returns:
        ExecutionConfigStore instance.

    Raises:
        ValueError: If store_type is unknown, kconf SDK is not installed,
            or required parameters are missing for the configured store_type.
    """
    effective_store_type = store_type or data_context.execution_config_store_type or "gcs"

    if effective_store_type == "gcs":
        from .gcs_store import GcsExecutionConfigStore
        import ray
        return GcsExecutionConfigStore(
            gcs_client=ray._private.worker.global_worker.gcs_client,
            job_id=job_id,
            dataset_id=dataset_id,
        )

    if effective_store_type == "memory":
        from .memory_store import MemoryExecutionConfigStore
        return MemoryExecutionConfigStore()

    if effective_store_type == "kconf":
        kconf_token = data_context.execution_config_kconf_token
        if not kconf_full_key or not kconf_token:
            raise ValueError(
                "kconf_full_key and kconf_token are required for kconf store."
            )
        validate_kconf_key(kconf_full_key, full=True)
        cls = KconfExecutionConfigStore or _resolve_kconf_store_cls()
        if cls is None:
            raise ValueError(
                "Kconf store requires 'infra-framework' package. "
                "Install with: pip install 'ray[data-kconf]'"
            )
        return cls(key=kconf_full_key, token=kconf_token)

    raise ValueError(f"Unknown store type: {effective_store_type!r}")
