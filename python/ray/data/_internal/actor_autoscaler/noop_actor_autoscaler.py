from typing import TYPE_CHECKING

from .base_actor_autoscaler import ActorAutoscaler

if TYPE_CHECKING:
    from ray.data._internal.execution.resource_manager import ResourceManager
    from ray.data._internal.execution.streaming_executor_state import Topology


class NoOpActorAutoscaler(ActorAutoscaler):
    """No-op autoscaler that does nothing.

    This autoscaler is used when autoscaling is disabled (the default).
    It implements the ActorAutoscaler interface but performs no scaling operations.
    """

    def __init__(
        self,
        topology: "Topology",
        resource_manager: "ResourceManager",
    ):
        super().__init__(topology, resource_manager)

    def try_trigger_scaling(self):
        """No-op: does not perform any scaling."""
        pass
