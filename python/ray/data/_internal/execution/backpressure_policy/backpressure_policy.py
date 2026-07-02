from abc import ABC
from typing import TYPE_CHECKING, Optional

from ray.data.context import DataContext

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces.physical_operator import (
        PhysicalOperator,
    )
    from ray.data._internal.execution.resource_manager import ResourceManager
    from ray.data._internal.execution.streaming_executor_state import Topology


class BackpressurePolicy(ABC):
    """Interface for back pressure policies."""

    @property
    def name(self) -> str:
        """Human-readable name for UX/progress bar display.

        Defaults to the class name. Subclasses can override for a custom name.
        """
        return type(self).__name__

    def __init__(
        self,
        data_context: DataContext,
        topology: "Topology",
        resource_manager: "ResourceManager",
    ):
        """Initialize the backpressure policy.

        Args:
            data_context: The data context.
            topology: The execution topology.
            resource_manager: The resource manager.
        """
        self._data_context = data_context
        self._topology = topology
        self._resource_manager = resource_manager

    def available_capacity(self, op: "PhysicalOperator") -> Optional[int]:
        """Return how many more tasks ``op`` may dispatch under this policy.

        Used by the streaming executor's dispatch loop to size a batch in one
        shot, eliminating per-dispatch policy rechecks.

        Returns:
            ``None`` — this policy imposes no count limit on ``op``.
            ``int`` — explicit upper bound; ``0`` means blocked (equivalent to
            ``can_add_input(op) == False``).

        Default implementation derives from ``can_add_input``: returns ``0``
        when blocked, ``None`` when not. Subclasses with a numeric notion of
        "remaining slots" should override for tighter batching (zero slack).
        """
        return 0 if not self.can_add_input(op) else None

    def can_add_input(self, op: "PhysicalOperator") -> bool:
        """Determine if we can add a new input to the operator. If returns False, the
        operator will be backpressured and will not be able to run new tasks.
        Used in `streaming_executor_state.py::select_operator_to_run()`.

        Returns: True if we can add a new input to the operator, False otherwise.

        Note, if multiple backpressure policies are enabled, the operator will be
        backpressured if any of the policies returns False.
        """
        return True

    def max_task_output_bytes_to_read(self, op: "PhysicalOperator") -> Optional[int]:
        """Return the maximum bytes of pending task outputs can be read for
        the given operator. None means no limit.

        This is used for output backpressure to limit how much data an operator
        can read from its running tasks.

        Note, if multiple backpressure policies return non-None values for an operator,
        the minimum of those values will be used as the limit.

        Args:
            op: The operator to get the limit for.

        Returns:
            The maximum bytes that can be read, or None if no limit.
        """
        return None
