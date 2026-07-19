from typing import List, Optional

from typing_extensions import override

from ray.data._internal.execution.bundle_queue import BaseBundleQueue, FIFOBundleQueue
from ray.data._internal.execution.interfaces import (
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.execution.operators.base_physical_operator import (
    InternalQueueOperatorMixin,
    NAryOperator,
)
from ray.data._internal.logical.operators.n_ary_operator import PriorityStoppingCondition
from ray.data._internal.stats import StatsDict
from ray.data.context import DataContext


class PriorityOperator(InternalQueueOperatorMixin, NAryOperator):
    """An operator that merges blocks from multiple input operators into
    a single output stream using strict priority ordering.

    When multiple inputs have data available, the highest-priority input
    is always selected. When the highest-priority input's buffer is empty,
    the operator immediately falls back to the next-highest-priority input
    with available data — it never blocks waiting for a specific input.

    Inputs are ordered by priority: input_dependencies[0] has the highest
    priority, input_dependencies[-1] has the lowest.
    """

    def __init__(
        self,
        data_context: DataContext,
        *input_ops: PhysicalOperator,
        stopping_condition: PriorityStoppingCondition = PriorityStoppingCondition.DRAIN_ALL,
    ):
        assert len(input_ops) >= 1

        self._stopping_condition = stopping_condition

        self._input_buffers: List[BaseBundleQueue] = [
            FIFOBundleQueue() for _ in range(len(input_ops))
        ]
        self._output_buffer: BaseBundleQueue = FIFOBundleQueue()

        self._input_done_flags: List[bool] = [False] * len(input_ops)
        self._stopped: bool = False

        self._stats: StatsDict = {"Priority": []}

        input_names = ", ".join([op._name for op in input_ops])
        name = f"Priority({input_names})"
        super().__init__(data_context, *input_ops, name=name)

    @property
    @override
    def _input_queues(self) -> List[BaseBundleQueue]:
        return self._input_buffers

    @property
    @override
    def _output_queues(self) -> List[BaseBundleQueue]:
        return [self._output_buffer]

    @override
    def mark_execution_finished(self) -> None:
        PhysicalOperator.mark_execution_finished(self)
        self.clear_internal_input_queue()

    @override
    def _add_input_inner(self, refs: RefBundle, input_index: int) -> None:
        assert not self.has_completed()
        assert 0 <= input_index < len(self._input_dependencies), input_index
        if self._stopped:
            return
        self._input_buffers[input_index].add(refs)
        self._metrics.on_input_queued(refs, input_index=input_index)
        self._try_output()

    @override
    def input_done(self, input_index: int) -> None:
        self._input_done_flags[input_index] = True
        self._try_output()

    @override
    def all_inputs_done(self) -> None:
        super().all_inputs_done()
        self._try_output()

    @override
    def has_next(self) -> bool:
        return len(self._output_buffer) > 0

    @override
    def _get_next_inner(self) -> RefBundle:
        refs = self._output_buffer.get_next()
        self._metrics.on_output_dequeued(refs)
        return refs

    @override
    def num_outputs_total(self) -> Optional[int]:
        if self._stopping_condition == PriorityStoppingCondition.STOP_ON_HIGHEST:
            return self.input_dependencies[0].num_outputs_total()
        total = 0
        for dep in self.input_dependencies:
            n = dep.num_outputs_total()
            if n is None:
                return None
            total += n
        return total

    @override
    def num_output_rows_total(self) -> Optional[int]:
        if self._stopping_condition == PriorityStoppingCondition.STOP_ON_HIGHEST:
            return self.input_dependencies[0].num_output_rows_total()
        total = 0
        for dep in self.input_dependencies:
            n = dep.num_output_rows_total()
            if n is None:
                return None
            total += n
        return total

    @override
    def get_stats(self) -> StatsDict:
        return self._stats

    @override
    def throttling_disabled(self) -> bool:
        return False

    def _is_input_exhausted(self, index: int) -> bool:
        return (
            self._input_done_flags[index]
            and not self._input_buffers[index].has_next()
        )

    def _select_highest_priority_available(self) -> int:
        """Select the highest-priority input that has data available.

        Scans inputs in priority order (index 0 = highest).
        Returns -1 if all inputs are exhausted.
        Returns -2 if no exhausted input has data (should wait for more).
        """
        all_exhausted = True
        for i in range(len(self._input_buffers)):
            if self._is_input_exhausted(i):
                continue
            all_exhausted = False
            if self._input_buffers[i].has_next():
                return i
        if all_exhausted:
            return -1
        return -2

    def _try_output(self) -> None:
        """Move blocks from input buffers to the output buffer.

        On each iteration, selects the highest-priority input with data
        available. If no input has data but some are not yet exhausted,
        we wait (return) rather than blocking — the next call to
        _add_input_inner or input_done will trigger another attempt.
        """
        if self._stopped:
            return

        while True:
            if self._stopping_condition == PriorityStoppingCondition.STOP_ON_HIGHEST:
                if self._is_input_exhausted(0):
                    self._stopped = True
                    self.mark_execution_finished()
                    return

            selected = self._select_highest_priority_available()
            if selected == -1:
                if not self._stopped:
                    self._stopped = True
                    self.mark_execution_finished()
                return
            if selected == -2:
                return

            bundle = self._input_buffers[selected].get_next()
            self._metrics.on_input_dequeued(bundle, input_index=selected)
            self._output_buffer.add(bundle)
            self._metrics.on_output_queued(bundle)
