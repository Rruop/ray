from ray.data._internal.cluster_autoscaler import ClusterAutoscaler
from ray.data._internal.execution.interfaces.execution_options import ExecutionResources


class NoOpClusterAutoscaler(ClusterAutoscaler):
    """No-op cluster autoscaler that does nothing.

    This autoscaler is used when cluster autoscaling is disabled or not supported.
    It implements the ClusterAutoscaler interface but performs no scaling operations.
    """

    def try_trigger_scaling(self) -> None:
        """No-op: does not perform any scaling."""
        pass

    def on_executor_shutdown(self) -> None:
        """No-op: nothing to clean up."""
        pass

    def get_total_resources(self) -> ExecutionResources:
        """Return infinite resources since we're not managing scaling."""
        return ExecutionResources.inf()
