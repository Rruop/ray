"""In-memory execution configuration store implementation."""

import logging
import threading
from typing import Optional

from ray.data._internal.execution.config.models import ExecutionConfig
from ray.data._internal.execution.config.store import ExecutionConfigStore

logger = logging.getLogger(__name__)


class MemoryExecutionConfigStore(ExecutionConfigStore):
    """
    In-memory store for execution configuration.

    This implementation stores configuration in memory only and is useful for
    testing or temporary configurations that don't need persistence.
    """

    def __init__(self):
        """Initialize in-memory config store."""
        self._lock = threading.Lock()
        self._config: Optional[ExecutionConfig] = None

    def get(self) -> Optional[ExecutionConfig]:
        """Get the current execution configuration."""
        with self._lock:
            return self._config

    def put(self, config: ExecutionConfig) -> None:
        """Store or update the execution configuration."""
        with self._lock:
            self._config = config

    def init(self, config: ExecutionConfig) -> bool:
        """Initialize the configuration if it doesn't exist."""
        with self._lock:
            if self._config is not None:
                return False
            self._config = config
            return True

    def delete(self) -> bool:
        """Delete the stored configuration."""
        with self._lock:
            if self._config is None:
                return False
            self._config = None
            return True
