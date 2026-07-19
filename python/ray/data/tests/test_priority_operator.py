import pytest

import ray
from ray.data._internal.execution.interfaces import RefBundle
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.priority_operator import PriorityOperator
from ray.data._internal.execution.util import make_ref_bundles
from ray.data._internal.logical.operators.n_ary_operator import PriorityStoppingCondition
from ray.data.context import DataContext
from ray.data.tests.util import _get_blocks, _take_outputs

from ray.tests.conftest import *  # noqa


def _make_input_op(data, data_context=None):
    if data_context is None:
        data_context = DataContext.get_current()
    bundles = make_ref_bundles(data)
    return InputDataBuffer(data_context, bundles)


class TestPriorityOperator:

    def test_priority_ordering(self, ray_start_regular_shared):
        dc = DataContext.get_current()
        high_op = _make_input_op([[1, 2], [3]], dc)
        med_op = _make_input_op([[10, 20], [30]], dc)
        low_op = _make_input_op([[100, 200]], dc)

        op = PriorityOperator(dc, high_op, med_op, low_op)

        for i, input_op in enumerate(op.input_dependencies):
            for ref in input_op.get_output():
                op.add_input(ref, i)
            op.input_done(i)

        output = _take_outputs(op)
        assert output == [[1, 2], [3], [10, 20], [30], [100, 200]]

    def test_fallback_on_empty_high_priority(self, ray_start_regular_shared):
        dc = DataContext.get_current()
        high_op = _make_input_op([[1]], dc)
        low_op = _make_input_op([[10, 20], [30]], dc)

        op = PriorityOperator(dc, high_op, low_op)

        high_bundles = list(high_op.get_output())
        low_bundles = list(low_op.get_output())

        op.add_input(low_bundles[0], 1)
        assert op.has_next()
        output1 = []
        while op.has_next():
            ref = op.get_next()
            _get_blocks(ref, output1)
        assert output1 == [[10, 20]]

        op.add_input(high_bundles[0], 0)
        assert op.has_next()
        output2 = []
        while op.has_next():
            ref = op.get_next()
            _get_blocks(ref, output2)
        assert output2 == [[1]]

        op.add_input(low_bundles[1], 1)
        op.input_done(0)
        op.input_done(1)
        output3 = _take_outputs(op)
        assert output3 == [[30]]

    def test_no_blocking_all_low_when_high_empty(self, ray_start_regular_shared):
        dc = DataContext.get_current()
        high_op = _make_input_op([], dc)
        low_op = _make_input_op([[10, 20]], dc)

        op = PriorityOperator(dc, high_op, low_op)

        low_bundles = list(low_op.get_output())
        op.add_input(low_bundles[0], 1)

        assert op.has_next()
        output = []
        while op.has_next():
            ref = op.get_next()
            _get_blocks(ref, output)
        assert output == [[10, 20]]

        op.input_done(0)
        op.input_done(1)

    def test_drain_all(self, ray_start_regular_shared):
        dc = DataContext.get_current()
        high_op = _make_input_op([[1], [2]], dc)
        low_op = _make_input_op([[10]], dc)

        op = PriorityOperator(
            dc, high_op, low_op,
            stopping_condition=PriorityStoppingCondition.DRAIN_ALL,
        )

        for i, input_op in enumerate(op.input_dependencies):
            for ref in input_op.get_output():
                op.add_input(ref, i)
            op.input_done(i)

        output = _take_outputs(op)
        assert output == [[1], [2], [10]]

    def test_stop_on_highest(self, ray_start_regular_shared):
        dc = DataContext.get_current()
        high_op = _make_input_op([[1]], dc)
        low_op = _make_input_op([[10, 20]], dc)

        op = PriorityOperator(
            dc, high_op, low_op,
            stopping_condition=PriorityStoppingCondition.STOP_ON_HIGHEST,
        )

        for i, input_op in enumerate(op.input_dependencies):
            for ref in input_op.get_output():
                op.add_input(ref, i)
            op.input_done(i)

        output = _take_outputs(op)
        assert output == [[1]]

    def test_stop_on_highest_ignores_low_data(self, ray_start_regular_shared):
        dc = DataContext.get_current()
        high_op = _make_input_op([[1]], dc)
        low_op = _make_input_op([[10, 20], [30]], dc)

        op = PriorityOperator(
            dc, high_op, low_op,
            stopping_condition=PriorityStoppingCondition.STOP_ON_HIGHEST,
        )

        low_bundles = list(low_op.get_output())
        high_bundles = list(high_op.get_output())

        op.add_input(low_bundles[0], 1)
        while op.has_next():
            op.get_next()

        op.add_input(high_bundles[0], 0)
        while op.has_next():
            op.get_next()

        op.input_done(0)
        assert not op.has_next()

    def test_exhausted_input_skipped(self, ray_start_regular_shared):
        dc = DataContext.get_current()
        high_op = _make_input_op([[1]], dc)
        low_op = _make_input_op([[10, 20]], dc)

        op = PriorityOperator(dc, high_op, low_op)

        high_bundles = list(high_op.get_output())
        op.add_input(high_bundles[0], 0)
        op.input_done(0)

        low_bundles = list(low_op.get_output())
        op.add_input(low_bundles[0], 1)
        op.input_done(1)

        output = _take_outputs(op)
        assert output == [[1], [10, 20]]

    def test_single_input(self, ray_start_regular_shared):
        dc = DataContext.get_current()
        op1 = _make_input_op([[1, 2], [3]], dc)

        op = PriorityOperator(dc, op1)

        for i, input_op in enumerate(op.input_dependencies):
            for ref in input_op.get_output():
                op.add_input(ref, i)
            op.input_done(i)

        output = _take_outputs(op)
        assert output == [[1, 2], [3]]

    def test_multiple_inputs_interleaved_arrival(self, ray_start_regular_shared):
        dc = DataContext.get_current()
        high_op = _make_input_op([[1], [2], [3]], dc)
        low_op = _make_input_op([[10], [20]], dc)

        op = PriorityOperator(dc, high_op, low_op)

        high_bundles = list(high_op.get_output())
        low_bundles = list(low_op.get_output())

        op.add_input(high_bundles[0], 0)
        output1 = []
        while op.has_next():
            ref = op.get_next()
            _get_blocks(ref, output1)
        assert output1 == [[1]]

        op.add_input(low_bundles[0], 1)
        output2 = []
        while op.has_next():
            ref = op.get_next()
            _get_blocks(ref, output2)
        assert output2 == [[10]]

        op.add_input(high_bundles[1], 0)
        output3 = []
        while op.has_next():
            ref = op.get_next()
            _get_blocks(ref, output3)
        assert output3 == [[2]]

        op.add_input(high_bundles[2], 0)
        op.input_done(0)
        op.add_input(low_bundles[1], 1)
        op.input_done(1)

        output4 = _take_outputs(op)
        assert output4 == [[3], [20]]

    def test_three_inputs_priority_order(self, ray_start_regular_shared):
        dc = DataContext.get_current()
        high_op = _make_input_op([[1], [2]], dc)
        med_op = _make_input_op([[10], [20]], dc)
        low_op = _make_input_op([[100]], dc)

        op = PriorityOperator(dc, high_op, med_op, low_op)

        for i, input_op in enumerate(op.input_dependencies):
            for ref in input_op.get_output():
                op.add_input(ref, i)
            op.input_done(i)

        output = _take_outputs(op)
        assert output == [[1], [2], [10], [20], [100]]

    def test_num_outputs_total_drain_all(self, ray_start_regular_shared):
        dc = DataContext.get_current()
        op1 = _make_input_op([[1], [2]], dc)
        op2 = _make_input_op([[10]], dc)

        op = PriorityOperator(
            dc, op1, op2,
            stopping_condition=PriorityStoppingCondition.DRAIN_ALL,
        )
        assert op.num_outputs_total() == 3

    def test_num_outputs_total_stop_on_highest(self, ray_start_regular_shared):
        dc = DataContext.get_current()
        op1 = _make_input_op([[1], [2]], dc)
        op2 = _make_input_op([[10]], dc)

        op = PriorityOperator(
            dc, op1, op2,
            stopping_condition=PriorityStoppingCondition.STOP_ON_HIGHEST,
        )
        assert op.num_outputs_total() == 2

    def test_stats_key(self, ray_start_regular_shared):
        dc = DataContext.get_current()
        op1 = _make_input_op([[1]], dc)
        op = PriorityOperator(dc, op1)
        assert "Priority" in op.get_stats()

    def test_name_contains_inputs(self, ray_start_regular_shared):
        dc = DataContext.get_current()
        op1 = _make_input_op([[1]], dc)
        op2 = _make_input_op([[2]], dc)
        op = PriorityOperator(dc, op1, op2)
        assert "Priority" in op.name

    def test_add_input_after_stopped(self, ray_start_regular_shared):
        dc = DataContext.get_current()
        high_op = _make_input_op([[1]], dc)
        low_op = _make_input_op([[10]], dc)

        op = PriorityOperator(
            dc, high_op, low_op,
            stopping_condition=PriorityStoppingCondition.STOP_ON_HIGHEST,
        )

        for i, input_op in enumerate(op.input_dependencies):
            for ref in input_op.get_output():
                op.add_input(ref, i)
            op.input_done(i)

        _take_outputs(op)

        low_bundles = list(low_op.get_output())
        if low_bundles:
            op.add_input(low_bundles[0], 1)

    def test_internal_queue_metrics(self, ray_start_regular_shared):
        dc = DataContext.get_current()
        op1 = _make_input_op([[1], [2]], dc)
        op2 = _make_input_op([[10]], dc)

        op = PriorityOperator(dc, op1, op2)

        bundles1 = list(op1.get_output())
        op.add_input(bundles1[0], 0)

        assert op.internal_input_queue_num_blocks() >= 0
        assert op.internal_output_queue_num_blocks() >= 0

        while op.has_next():
            op.get_next()

        op.add_input(bundles1[1], 0)
        op.input_done(0)

        bundles2 = list(op2.get_output())
        op.add_input(bundles2[0], 1)
        op.input_done(1)

        _take_outputs(op)
