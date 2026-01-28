"""Controller for synchronizing operator configuration."""

import logging
import time
from typing import TYPE_CHECKING, Any, Dict, Optional

from ray.data._internal.execution.config.store import ExecutionConfigStore

from .models import ExecutionConfig

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import PhysicalOperator

logger = logging.getLogger(__name__)

# Default interval between config polling attempts (in seconds).
# This limits GCS read frequency to reduce network overhead.
DEFAULT_CONFIG_POLL_INTERVAL_S = 20


class ConfigController:
    """
    A controller that synchronizes operator parallelism with the desired state
    defined in an ExecutionConfig.

    This controller reads from an ExecutionConfigStore and applies the specified
    parallelism settings to the running operators in the topology. It is not an
    "autoscaler" in the traditional sense, as it does not make decisions based
    on metrics; it only enforces the provided configuration.

    To reduce GCS read overhead, polling is throttled to occur at most once
    every `poll_interval_s` seconds.
    """

    def __init__(
        self,
        topology: Dict["PhysicalOperator", Any],
        config_store: ExecutionConfigStore,
        poll_interval_s: float = DEFAULT_CONFIG_POLL_INTERVAL_S,
    ):
        """
        Initialize the ConfigController.

        Args:
            topology: The execution topology containing operators.
            config_store: The store backend for execution configuration.
            poll_interval_s: Minimum interval between config polls (default: 5s).
        """
        self._topology = topology
        self._store = config_store
        self._poll_interval_s = poll_interval_s
        self._config: Optional[ExecutionConfig] = None
        self._last_poll_time: float = 0.0
        logger.info(
            f"ConfigController initialized with poll_interval={poll_interval_s}s"
        )

    def try_apply_config(self) -> None:
        """
        Check for configuration updates and apply them to the operators.

        This method should be called periodically by the execution engine.
        To reduce GCS overhead, actual polling is throttled based on
        `poll_interval_s`. Calls within the interval are skipped.

        The store's get() method is expected to return the latest configuration,
        detecting any external updates (e.g., from the dashboard API).
        """
        # Throttle polling to reduce GCS read overhead
        now = time.time()
        if now - self._last_poll_time < self._poll_interval_s:
            return

        self._last_poll_time = now
        config = self._store.get()

        if config is None or self._config == config:
            return

        self._config = config

        logger.info("Detected new execution configuration. Applying changes.")

        if not config.operators:
            logger.debug("No operator configurations found")
            return

        for op, _ in self._topology.items():
            op_id = op.id
            if op_id in config.operators:
                op_config = config.operators[op_id]
                try:
                    op.apply_parallelism_config(op_config)
                    logger.debug(f"Applied config to operator {op_id}: {op_config}")
                except Exception as e:
                    logger.error(f"Failed to apply config to operator {op_id}: {e}")