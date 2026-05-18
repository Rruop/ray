"""Execution configuration for Ray Data dynamic operator parallelism.

This module provides configuration management for dynamic adjustment of
operator parallelism during Ray Data execution.

Main classes:
- ExecutionConfig: Container for operator configurations
- OperatorConfig: Base class for operator-specific settings
- ExecutionConfigStore: Abstract interface for config persistence
- ConfigController: Synchronizes config changes to running operators
"""

from .models import (
    ActorPoolOperatorConfig,
    ExecutionConfig,
    OperatorConfig,
    TaskPoolOperatorConfig,
)
from .controller import ConfigController
from .store import (
    ExecutionConfigStore,
    create_execution_config_store,
    GCS_KEY_PREFIX,
    GCS_KEY_TEMPLATE,
    GCS_KEY_TEMPLATE_WITH_DATASET,
    GCS_NAMESPACE,
)
from .gcs_store import GcsExecutionConfigStore
from .memory_store import MemoryExecutionConfigStore

# Note: KconfExecutionConfigStore is NOT imported here to avoid requiring
# the kconf package when users don't use it. It is lazily imported in
# store.py only when store_type == "kconf".

__all__ = [
    # Configuration classes
    "OperatorConfig",
    "ActorPoolOperatorConfig",
    "TaskPoolOperatorConfig",
    "ExecutionConfig",
    # Controller
    "ConfigController",
    # Store interfaces
    "ExecutionConfigStore",
    "create_execution_config_store",
    # GCS constants
    "GCS_KEY_PREFIX",
    "GCS_KEY_TEMPLATE",
    "GCS_KEY_TEMPLATE_WITH_DATASET",
    "GCS_NAMESPACE",
    # Store implementations
    "GcsExecutionConfigStore",
    "MemoryExecutionConfigStore",
]
