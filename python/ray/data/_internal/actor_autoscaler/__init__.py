from typing import TYPE_CHECKING, Optional

from .actor_pool_resizing_policy import (
    ActorPoolResizingPolicy,
    DefaultResizingPolicy,
)
from .autoscaling_actor_pool import ActorPoolScalingRequest, AutoscalingActorPool
from .base_actor_autoscaler import ActorAutoscaler
from .default_actor_autoscaler import DefaultActorAutoscaler, _get_max_scale_up
from .noop_actor_autoscaler import NoOpActorAutoscaler

if TYPE_CHECKING:
    from ray.data._internal.execution.gpu_node_drain_manager import (
        GPUNodeDrainManager,
    )
    from ray.data._internal.execution.resource_manager import ResourceManager
    from ray.data._internal.execution.streaming_executor_state import Topology
    from ray.data.context import AutoscalingConfig


def create_actor_autoscaler(
    topology: "Topology",
    resource_manager: "ResourceManager",
    config: "AutoscalingConfig",
    gpu_drain_manager: Optional["GPUNodeDrainManager"] = None,
) -> ActorAutoscaler:
    from ray.data.context import ActorAutoscalerType

    if config.autoscaler_type == ActorAutoscalerType.DISABLED:
        return NoOpActorAutoscaler(topology, resource_manager)
    else:
        return DefaultActorAutoscaler(
            topology,
            resource_manager,
            config=config,
            gpu_drain_manager=gpu_drain_manager,
        )


__all__ = [
    "ActorAutoscaler",
    "ActorPoolResizingPolicy",
    "ActorPoolScalingRequest",
    "AutoscalingActorPool",
    "DefaultResizingPolicy",
    "DefaultActorAutoscaler",
    "NoOpActorAutoscaler",
    "create_actor_autoscaler",
    "_get_max_scale_up",
]
