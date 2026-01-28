"""Tests for custom operator ID and name functionality.

This module tests the custom_id and custom_name parameters that flow through:
dataset.py -> map_operator.py (Logical) -> PhysicalOperator
"""

import pytest
from unittest.mock import MagicMock

from ray.data._internal.logical.operators.map_operator import (
    MapBatches,
    MapRows,
    Filter,
    FlatMap,
)
from ray.data._internal.logical.interfaces.logical_operator import LogicalOperator

_MOCK_INPUT_OP = MagicMock(spec=LogicalOperator)


class TestLogicalOperatorCustomId:
    """Tests for custom_id and custom_name in logical operators."""

    def test_map_batches_custom_id(self):
        """Test that MapBatches accepts and stores custom_id."""
        mock_fn = lambda x: x

        op = MapBatches(
            input_op=_MOCK_INPUT_OP,
            fn=mock_fn,
            custom_id="my_custom_id",
        )

        assert op._id == "my_custom_id"

    def test_map_batches_custom_name(self):
        """Test that MapBatches accepts and uses custom_name."""
        mock_fn = lambda x: x

        op = MapBatches(
            input_op=_MOCK_INPUT_OP,
            fn=mock_fn,
            custom_name="MyCustomOperator",
        )

        assert op.name == "MyCustomOperator"

    def test_map_batches_both_custom_id_and_name(self):
        """Test that both custom_id and custom_name work together."""
        mock_fn = lambda x: x

        op = MapBatches(
            input_op=_MOCK_INPUT_OP,
            fn=mock_fn,
            custom_id="my_id",
            custom_name="MyName",
        )

        assert op._id == "my_id"
        assert op.name == "MyName"

    def test_map_batches_default_name_generation(self):
        """Test that default name is generated when custom_name is not provided."""
        def my_udf(x):
            return x

        op = MapBatches(
            input_op=_MOCK_INPUT_OP,
            fn=my_udf,
        )

        # Default name should include the function name
        assert "my_udf" in op.name
        assert "MapBatches" in op.name

    def test_map_rows_custom_id(self):
        """Test that MapRows accepts custom_id."""
        mock_fn = lambda x: x

        op = MapRows(
            input_op=_MOCK_INPUT_OP,
            fn=mock_fn,
            custom_id="map_rows_id",
    )

        assert op._id == "map_rows_id"

    def test_map_rows_custom_name(self):
        """Test that MapRows accepts custom_name."""
        mock_fn = lambda x: x

        op = MapRows(
            input_op=_MOCK_INPUT_OP,
            fn=mock_fn,
            custom_name="CustomMapRows",
        )

        assert op.name == "CustomMapRows"

    def test_filter_custom_id(self):
        """Test that Filter accepts custom_id."""
        mock_fn = lambda x: True

        op = Filter(
            input_op=_MOCK_INPUT_OP,
            fn=mock_fn,
            custom_id="filter_id",
        )

        assert op._id == "filter_id"

    def test_filter_custom_name(self):
        """Test that Filter accepts custom_name."""
        mock_fn = lambda x: True

        op = Filter(
            input_op=_MOCK_INPUT_OP,
            fn=mock_fn,
            custom_name="CustomFilter",
        )

        assert op.name == "CustomFilter"

    def test_flat_map_custom_id(self):
        """Test that FlatMap accepts custom_id."""
        mock_fn = lambda x: [x]

        op = FlatMap(
            input_op=_MOCK_INPUT_OP,
            fn=mock_fn,
            custom_id="flat_map_id",
        )

        assert op._id == "flat_map_id"

    def test_flat_map_custom_name(self):
        """Test that FlatMap accepts custom_name."""
        mock_fn = lambda x: [x]

        op = FlatMap(
            input_op=_MOCK_INPUT_OP,
            fn=mock_fn,
            custom_name="CustomFlatMap",
        )

        assert op.name == "CustomFlatMap"

    def test_none_custom_id_uses_default(self):
        """Test that None custom_id results in None (to be assigned later)."""
        mock_fn = lambda x: x

        op = MapBatches(
            input_op=_MOCK_INPUT_OP,
            fn=mock_fn,
            custom_id=None,
        )

        assert op._id is None

if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
