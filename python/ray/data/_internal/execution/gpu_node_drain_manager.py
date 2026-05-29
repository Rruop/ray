import logging
from typing import Set

logger = logging.getLogger(__name__)


class GPUNodeDrainManager:
    """Coordinates GPU node drain requests after actor release.

    After Ray Data force-kills GPU actors on a node, this manager sends
    an IDLE_TERMINATION drain request via the public autoscaler SDK.
    The drain is only accepted if the node is actually idle (no other
    workers running), providing a built-in safety net for multi-operator
    scenarios. If rejected, the Autoscaler's native idle detection
    serves as a fallback.
    """

    def __init__(self, enabled: bool = True):
        self._enabled = enabled
        # Nodes where drain was accepted (avoid duplicate requests).
        self._drained_nodes: Set[str] = set()

    def request_drain_for_node(self, node_id: str) -> bool:
        """Request drain for a node after GPU actors have been killed.

        Uses IDLE_TERMINATION which will be rejected by the Raylet if the
        node still has active workers (e.g., from other operators). This
        makes it safe to call even when other operators share the node.

        Args:
            node_id: The Ray node ID to drain.

        Returns:
            True if the drain request was accepted, False otherwise.
        """
        if not self._enabled:
            return False
        if node_id in self._drained_nodes:
            return False
        return self._request_drain(node_id)

    def _request_drain(self, node_id: str) -> bool:
        """Send a drain request via the public autoscaler SDK."""
        try:
            from ray.autoscaler.sdk import request_node_drain

            node_id_bytes = (
                node_id.encode() if isinstance(node_id, str) else node_id
            )
            is_accepted = request_node_drain(
                node_id_bytes,
                reason="Ray Data GPU actor scale-down",
            )
            if is_accepted:
                self._drained_nodes.add(node_id)
                logger.info(f"Drain accepted for GPU node {node_id}")
            else:
                logger.debug(f"Drain rejected for GPU node {node_id}")
            return is_accepted
        except Exception:
            logger.warning(
                f"Failed to drain GPU node {node_id}", exc_info=True
            )
            return False
