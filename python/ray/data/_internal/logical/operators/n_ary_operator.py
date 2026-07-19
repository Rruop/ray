import enum
from typing import Optional

from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalOperatorSupportsPredicatePassThrough,
    PredicatePassThroughBehavior,
)
from ray.util.annotations import PublicAPI

__all__ = [
    "NAry",
    "Priority",
    "PriorityStoppingCondition",
    "Union",
    "Zip",
]


@PublicAPI(stability="alpha")
class PriorityStoppingCondition(enum.Enum):
    """Controls when a priority pipeline terminates.

    DRAIN_ALL: Pipeline runs until ALL inputs are exhausted.
        Lower-priority inputs are only consumed when higher-priority
        inputs have no data available.
    STOP_ON_HIGHEST: Pipeline ends when the highest-priority input
        is exhausted, regardless of remaining data in lower-priority inputs.
    """

    DRAIN_ALL = "drain_all"
    STOP_ON_HIGHEST = "stop_on_highest"


class NAry(LogicalOperator):
    """Base class for n-ary operators, which take multiple input operators."""

    def __init__(
        self,
        *input_ops: LogicalOperator,
        num_outputs: Optional[int] = None,
    ):
        """
        Args:
            input_ops: The input operators.
        """
        super().__init__(
            input_dependencies=list(input_ops),
            num_outputs=num_outputs,
        )

    @property
    def num_outputs(self) -> Optional[int]:
        return self._num_outputs


class Zip(NAry):
    """Logical operator for zip."""

    def __init__(
        self,
        *input_ops: LogicalOperator,
    ):
        super().__init__(*input_ops)

    def estimated_num_outputs(self):
        total_num_outputs = 0
        for input in self.input_dependencies:
            num_outputs = input.estimated_num_outputs()
            if num_outputs is None:
                return None
            total_num_outputs = max(total_num_outputs, num_outputs)
        return total_num_outputs


class Union(NAry, LogicalOperatorSupportsPredicatePassThrough):
    """Logical operator for union."""

    def __init__(
        self,
        *input_ops: LogicalOperator,
    ):
        super().__init__(*input_ops)

    def estimated_num_outputs(self):
        total_num_outputs = 0
        for input in self.input_dependencies:
            num_outputs = input.estimated_num_outputs()
            if num_outputs is None:
                return None
            total_num_outputs += num_outputs
        return total_num_outputs

    def predicate_passthrough_behavior(self) -> PredicatePassThroughBehavior:
        # Union allows pushing filter into each branch
        return PredicatePassThroughBehavior.PUSH_INTO_BRANCHES


class Priority(NAry):
    """Logical operator for priority-based dataset mixing.

    Inputs are ordered by priority: input_dependencies[0] has the highest
    priority, input_dependencies[-1] has the lowest.
    """

    def __init__(
        self,
        *input_ops: LogicalOperator,
        stopping_condition: PriorityStoppingCondition = PriorityStoppingCondition.DRAIN_ALL,
    ):
        super().__init__(*input_ops)
        self.stopping_condition = stopping_condition

    def estimated_num_outputs(self) -> Optional[int]:
        if self.stopping_condition == PriorityStoppingCondition.STOP_ON_HIGHEST:
            return self.input_dependencies[0].estimated_num_outputs()
        elif self.stopping_condition == PriorityStoppingCondition.DRAIN_ALL:
            total = 0
            for dep in self.input_dependencies:
                n = dep.estimated_num_outputs()
                if n is None:
                    return None
                total += n
            return total
        return None
