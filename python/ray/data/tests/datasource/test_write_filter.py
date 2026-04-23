"""Tests for write-time filtering feature.

This tests the filter_fn and filter_expr parameters in write_datasink(),
which allow filtering data before writing while preserving original blocks
for downstream processing (e.g., checkpoint).
"""
import tempfile
import os

import pytest

import ray
from ray.data._internal.datasource.json_datasink import JSONDatasink
from ray.data.expressions import col


@pytest.fixture
def temp_json_dir():
    """Fixture providing a temporary directory for JSON output."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


class TestWriteFilterValidation:
    """Test parameter validation for write filters."""

    def test_both_filter_fn_and_filter_expr_raises_error(self, ray_start_regular_shared, temp_json_dir):
        """Test that setting both filter_fn and filter_expr raises an error."""
        ds = ray.data.range(10)
        datasink = JSONDatasink(temp_json_dir)

        with pytest.raises(ValueError, match="Only one of"):
            ds.write_datasink(
                datasink,
                filter_fn=lambda row: row["id"] > 5,
                filter_expr=col("id") > 5,
            )

    def test_no_filter_params_is_valid(self, ray_start_regular_shared, temp_json_dir):
        """Test that not setting filter params works (original behavior)."""
        ds = ray.data.range(10)
        datasink = JSONDatasink(temp_json_dir)

        ds.write_datasink(datasink)
        result = ray.data.read_json(temp_json_dir)
        assert sorted(result.to_pandas()["id"].tolist()) == list(range(10))


class TestWriteFilterFn:
    """Test filter_fn parameter for write operations."""

    def test_filter_fn_filters_data(self, ray_start_regular_shared, temp_json_dir):
        """Test that filter_fn correctly filters data before writing."""
        ds = ray.data.range(10)
        datasink = JSONDatasink(temp_json_dir)

        ds.write_datasink(datasink, filter_fn=lambda row: row["id"] > 5)
        result = ray.data.read_json(temp_json_dir)
        assert sorted(result.to_pandas()["id"].tolist()) == [6, 7, 8, 9]

    def test_filter_fn_filters_all_data(self, ray_start_regular_shared, temp_json_dir):
        """Test filtering where all rows are filtered out."""
        ds = ray.data.range(10)
        datasink = JSONDatasink(temp_json_dir)

        ds.write_datasink(datasink, filter_fn=lambda row: row["id"] > 100)
        files = [f for f in os.listdir(temp_json_dir) if f.endswith(".json")]
        if files:
            result = ray.data.read_json(temp_json_dir)
            assert result.count() == 0

    def test_filter_fn_with_complex_condition(self, ray_start_regular_shared, temp_json_dir):
        """Test filter_fn with complex multi-field condition."""
        ds = ray.data.from_items([
            {"id": 1, "score": 0.8, "status": "active"},
            {"id": 2, "score": 0.3, "status": "active"},
            {"id": 3, "score": 0.9, "status": "inactive"},
            {"id": 4, "score": 0.7, "status": "active"},
        ])
        datasink = JSONDatasink(temp_json_dir)

        ds.write_datasink(
            datasink,
            filter_fn=lambda row: row["score"] > 0.5 and row["status"] == "active",
        )
        result = ray.data.read_json(temp_json_dir)
        assert sorted(result.to_pandas()["id"].tolist()) == [1, 4]


class TestWriteFilterExpr:
    """Test filter_expr parameter for write operations (vectorized)."""

    def test_filter_expr_filters_data(self, ray_start_regular_shared, temp_json_dir):
        """Test that filter_expr correctly filters data before writing."""
        ds = ray.data.range(10)
        datasink = JSONDatasink(temp_json_dir)

        ds.write_datasink(datasink, filter_expr=col("id") > 5)
        result = ray.data.read_json(temp_json_dir)
        assert sorted(result.to_pandas()["id"].tolist()) == [6, 7, 8, 9]

    def test_filter_expr_filters_all_data(self, ray_start_regular_shared, temp_json_dir):
        """Test filtering where all rows are filtered out."""
        ds = ray.data.range(10)
        datasink = JSONDatasink(temp_json_dir)

        ds.write_datasink(datasink, filter_expr=col("id") > 100)
        files = [f for f in os.listdir(temp_json_dir) if f.endswith(".json")]
        if files:
            result = ray.data.read_json(temp_json_dir)
            assert result.count() == 0

    def test_filter_expr_with_comparison(self, ray_start_regular_shared, temp_json_dir):
        """Test filter_expr with various comparison operators."""
        ds = ray.data.from_items([
            {"id": 1, "score": 0.8},
            {"id": 2, "score": 0.3},
            {"id": 3, "score": 0.5},
            {"id": 4, "score": 0.7},
        ])
        datasink = JSONDatasink(temp_json_dir)

        ds.write_datasink(datasink, filter_expr=col("score") >= 0.5)
        result = ray.data.read_json(temp_json_dir)
        assert sorted(result.to_pandas()["id"].tolist()) == [1, 3, 4]


class TestWriteFilterWithMultipleBlocks:
    """Test write filters with multiple data blocks."""

    def test_filter_across_multiple_blocks(self, ray_start_regular_shared, temp_json_dir):
        """Test that filtering works correctly across multiple blocks."""
        ds = ray.data.range(100, override_num_blocks=10)
        datasink = JSONDatasink(temp_json_dir)

        ds.write_datasink(datasink, filter_fn=lambda row: row["id"] % 2 == 0)
        result = ray.data.read_json(temp_json_dir)
        expected = [i for i in range(100) if i % 2 == 0]
        assert sorted(result.to_pandas()["id"].tolist()) == expected

    def test_filter_expr_across_multiple_blocks(self, ray_start_regular_shared, temp_json_dir):
        """Test that filter_expr works correctly across multiple blocks."""
        ds = ray.data.range(100, override_num_blocks=10)
        datasink = JSONDatasink(temp_json_dir)

        ds.write_datasink(datasink, filter_expr=col("id") < 50)
        result = ray.data.read_json(temp_json_dir)
        expected = list(range(50))
        assert sorted(result.to_pandas()["id"].tolist()) == expected


class TestWriteFilterPerformance:
    """Test that filter_expr is preferred for performance."""

    def test_filter_expr_vs_filter_fn_same_result(self, ray_start_regular_shared):
        """Test that filter_expr and filter_fn produce the same results."""
        ds1 = ray.data.range(100, override_num_blocks=4)
        ds2 = ray.data.range(100, override_num_blocks=4)

        with tempfile.TemporaryDirectory() as tmpdir1, \
             tempfile.TemporaryDirectory() as tmpdir2:
            datasink1 = JSONDatasink(tmpdir1)
            datasink2 = JSONDatasink(tmpdir2)

            ds1.write_datasink(datasink1, filter_expr=col("id") >= 50)
            ds2.write_datasink(datasink2, filter_fn=lambda row: row["id"] >= 50)

            result_expr = ray.data.read_json(tmpdir1)
            result_fn = ray.data.read_json(tmpdir2)

            assert (
                sorted(result_expr.to_pandas()["id"].tolist())
                == sorted(result_fn.to_pandas()["id"].tolist())
            )
            assert sorted(result_expr.to_pandas()["id"].tolist()) == list(range(50, 100))
