"""
Test to reproduce PyArrow schema mismatch issues in Ray Data.

The problem occurs when:
1. Different blocks have different schemas for the same column
2. Specifically: list<null> vs list<struct<...>> or null vs list<struct<...>>

Root cause: When ALL rows in a block have None or [] for a list column,
PyArrow infers it as 'null' or 'list<null>' instead of the actual type.
When these blocks are merged (e.g., during repartition), schema conflicts occur.
"""

import pyarrow as pa
import pytest
import ray
from ray.data._internal.arrow_ops.transform_pyarrow import (
    concat,
    unify_schemas,
    _reconcile_diverging_fields,
)


class TestPyArrowSchemaInference:
    """Test PyArrow schema inference behavior."""

    def test_single_batch_scans_all_rows(self):
        """
        PyArrow scans ALL rows in a batch to infer schema.
        If any row has data, the correct type is inferred.
        """
        # First row empty, second row has data
        data = [
            {"id": 1, "items": []},
            {"id": 2, "items": [{"name": "a", "value": 1}]},
        ]
        table = pa.Table.from_pylist(data)

        # Should correctly infer as list<struct<...>>
        items_type = table.schema.field("items").type
        assert pa.types.is_list(items_type)
        assert pa.types.is_struct(items_type.value_type)

    def test_all_empty_infers_list_null(self):
        """
        When ALL rows are empty lists, PyArrow infers list<null>.
        """
        data = [
            {"id": 1, "items": []},
            {"id": 2, "items": []},
        ]
        table = pa.Table.from_pylist(data)

        items_type = table.schema.field("items").type
        assert items_type == pa.list_(pa.null())

    def test_all_none_infers_null(self):
        """
        When ALL rows are None, PyArrow infers null type.
        """
        data = [
            {"id": 1, "items": None},
            {"id": 2, "items": None},
        ]
        table = pa.Table.from_pylist(data)

        items_type = table.schema.field("items").type
        assert pa.types.is_null(items_type)


class TestCrossBlockSchemaMismatch:
    """Test schema mismatch when merging blocks with different schemas."""

    def test_list_null_vs_list_struct_merge_fails_with_pyarrow(self):
        """
        Demonstrate that PyArrow concat_tables fails when schemas differ.
        Block 1: list<null> (all rows are [])
        Block 2: list<struct<...>> (rows have data)
        """
        block1 = pa.Table.from_pylist([{"id": 1, "items": []}])
        block2 = pa.Table.from_pylist([{"id": 2, "items": [{"name": "a", "value": 1}]}])

        print(f"\nBlock 1 items type: {block1.schema.field('items').type}")
        print(f"Block 2 items type: {block2.schema.field('items').type}")

        # Direct PyArrow concat fails
        with pytest.raises(pa.ArrowInvalid):
            pa.concat_tables([block1, block2])

    def test_null_vs_list_struct_merge_fails_with_pyarrow(self):
        """
        Demonstrate that PyArrow concat_tables fails when one column is null type.
        Block 1: null (all rows are None)
        Block 2: list<struct<...>> (rows have data)
        """
        block1 = pa.Table.from_pylist([{"id": 1, "items": None}])
        block2 = pa.Table.from_pylist([{"id": 2, "items": [{"name": "a", "value": 1}]}])

        print(f"\nBlock 1 items type: {block1.schema.field('items').type}")
        print(f"Block 2 items type: {block2.schema.field('items').type}")

        # Direct PyArrow concat fails
        with pytest.raises(pa.ArrowInvalid):
            pa.concat_tables([block1, block2])

    def test_nested_struct_schema_mismatch(self):
        """
        Test nested struct with mismatched inner types.
        This simulates the real-world scenario where:
        - result.embed.in_pair is None in some blocks
        - result.embed.in_pair is list<struct<...>> in other blocks
        """
        block1 = pa.Table.from_pylist([{
            "id": 1,
            "result": {
                "video_id": "v1",
                "embed": {"in_pair": None}
            }
        }])

        block2 = pa.Table.from_pylist([{
            "id": 2,
            "result": {
                "video_id": "v2",
                "embed": {
                    "in_pair": [
                        {"key": "k1", "value": "v1"}
                    ]
                }
            }
        }])

        print(f"\nBlock 1 result type:\n{block1.schema.field('result').type}")
        print(f"\nBlock 2 result type:\n{block2.schema.field('result').type}")

        # This should fail with schema mismatch
        with pytest.raises(pa.ArrowInvalid):
            pa.concat_tables([block1, block2])


class TestRayDataConcatHandling:
    """Test Ray Data's handling of schema mismatches."""

    def test_ray_data_concat_handles_list_null(self):
        """
        Ray Data's concat function should handle list<null> vs list<struct<...>>.
        It uses _concat_cols_with_null_list to cast list<null> to the correct type.
        """
        block1 = pa.Table.from_pylist([{"id": 1, "items": []}])
        block2 = pa.Table.from_pylist([{"id": 2, "items": [{"name": "a", "value": 1}]}])

        print(f"\nBlock 1 items type: {block1.schema.field('items').type}")
        print(f"Block 2 items type: {block2.schema.field('items').type}")

        # Ray Data's concat should handle this
        result = concat([block1, block2], promote_types=True)

        print(f"Result items type: {result.schema.field('items').type}")
        print(f"Result data: {result.to_pylist()}")

        assert len(result) == 2
        # The result should have the correct type
        assert pa.types.is_list(result.schema.field("items").type)

    def test_ray_data_concat_handles_null_vs_list(self):
        """
        Ray Data's concat function should handle null vs list<struct<...>>.
        """
        block1 = pa.Table.from_pylist([{"id": 1, "items": None}])
        block2 = pa.Table.from_pylist([{"id": 2, "items": [{"name": "a", "value": 1}]}])

        print(f"\nBlock 1 items type: {block1.schema.field('items').type}")
        print(f"Block 2 items type: {block2.schema.field('items').type}")

        # Ray Data's concat should handle this
        result = concat([block1, block2], promote_types=True)

        print(f"Result items type: {result.schema.field('items').type}")
        print(f"Result data: {result.to_pylist()}")

        assert len(result) == 2

    def test_ray_data_concat_handles_nested_struct(self):
        """
        Test Ray Data's concat with nested struct containing None vs list<struct>.
        """
        block1 = pa.Table.from_pylist([{
            "id": 1,
            "result": {
                "video_id": "v1",
                "embed": {"in_pair": None}
            }
        }])

        block2 = pa.Table.from_pylist([{
            "id": 2,
            "result": {
                "video_id": "v2",
                "embed": {
                    "in_pair": [
                        {"key": "k1", "value": "v1"}
                    ]
                }
            }
        }])

        print(f"\nBlock 1 result type:\n{block1.schema.field('result').type}")
        print(f"\nBlock 2 result type:\n{block2.schema.field('result').type}")

        # Ray Data's concat should handle this
        result = concat([block1, block2], promote_types=True)

        print(f"\nResult result type:\n{result.schema.field('result').type}")
        print(f"Result data: {result.to_pylist()}")

        assert len(result) == 2


class TestUnifySchemas:
    """Test Ray Data's unify_schemas function."""

    def test_unify_list_null_with_list_struct(self):
        """
        Test unifying schemas with list<null> and list<struct<...>>.
        """
        schema1 = pa.schema([
            ("id", pa.int64()),
            ("items", pa.list_(pa.null())),
        ])
        schema2 = pa.schema([
            ("id", pa.int64()),
            ("items", pa.list_(pa.struct([
                ("name", pa.string()),
                ("value", pa.int64()),
            ]))),
        ])

        print(f"\nSchema 1: {schema1}")
        print(f"Schema 2: {schema2}")

        # unify_schemas should handle this
        unified = unify_schemas([schema1, schema2], promote_types=True)

        print(f"Unified schema: {unified}")

        # Should use the more specific type
        items_type = unified.field("items").type
        assert pa.types.is_list(items_type)
        # The value type should be struct, not null
        assert pa.types.is_struct(items_type.value_type)

    def test_unify_null_with_list_struct(self):
        """
        Test unifying schemas with null and list<struct<...>>.
        """
        schema1 = pa.schema([
            ("id", pa.int64()),
            ("items", pa.null()),
        ])
        schema2 = pa.schema([
            ("id", pa.int64()),
            ("items", pa.list_(pa.struct([
                ("name", pa.string()),
                ("value", pa.int64()),
            ]))),
        ])

        print(f"\nSchema 1: {schema1}")
        print(f"Schema 2: {schema2}")

        # unify_schemas should handle this
        unified = unify_schemas([schema1, schema2], promote_types=True)

        print(f"Unified schema: {unified}")


class TestReconcileDivergingFields:
    """Test the _reconcile_diverging_fields function."""

    def test_reconcile_list_null_with_list_struct(self):
        """
        Test reconciling list<null> with list<struct<...>>.
        """
        schema1 = pa.schema([
            ("items", pa.list_(pa.null())),
        ])
        schema2 = pa.schema([
            ("items", pa.list_(pa.struct([
                ("name", pa.string()),
                ("value", pa.int64()),
            ]))),
        ])

        reconciled = _reconcile_diverging_fields([schema1, schema2], promote_types=True)

        print(f"\nReconciled fields: {reconciled}")

        # Should find the list<struct<...>> type
        assert "items" in reconciled
        assert pa.types.is_list(reconciled["items"])


class TestRayDataMapperScenario:
    """Simulate the actual Ray Data mapper scenario."""

    @pytest.fixture(autouse=True)
    def setup_ray(self):
        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True)
        yield

    def test_map_with_schema_mismatch(self):
        """
        Simulate the scenario where:
        - Some blocks have all None values for a list column
        - Other blocks have actual data
        - Repartition causes schema mismatch

        This is the actual bug scenario.
        """
        # Create initial data where some items will have empty results
        data = [
            {"id": 1, "has_data": False},
            {"id": 2, "has_data": False},
            {"id": 3, "has_data": True},
            {"id": 4, "has_data": True},
        ]

        ds = ray.data.from_items(data)

        # Map function that returns list or None based on input
        def process_row(row):
            if row["has_data"]:
                row["items"] = [{"name": f"item_{row['id']}", "value": row["id"]}]
            else:
                row["items"] = None  # This will cause the problem
            return row

        # This map should work
        ds = ds.map(process_row)

        # Force materialization to see the schema
        blocks = ds.take_all()
        print(f"\nProcessed data: {blocks}")

        # Now try to repartition - this is where the error occurs
        # because different blocks may have different schemas
        try:
            ds_repartitioned = ds.repartition(2)
            result = ds_repartitioned.take_all()
            print(f"Repartitioned data: {result}")
        except Exception as e:
            print(f"Repartition failed with: {type(e).__name__}: {e}")
            raise

    def test_map_with_empty_list_schema_mismatch(self):
        """
        Simulate scenario with empty lists [] instead of None.
        """
        data = [
            {"id": 1, "has_data": False},
            {"id": 2, "has_data": False},
            {"id": 3, "has_data": True},
            {"id": 4, "has_data": True},
        ]

        ds = ray.data.from_items(data)

        def process_row(row):
            if row["has_data"]:
                row["items"] = [{"name": f"item_{row['id']}", "value": row["id"]}]
            else:
                row["items"] = []  # Empty list also causes issues
            return row

        ds = ds.map(process_row)

        blocks = ds.take_all()
        print(f"\nProcessed data: {blocks}")

        try:
            ds_repartitioned = ds.repartition(2)
            result = ds_repartitioned.take_all()
            print(f"Repartitioned data: {result}")
        except Exception as e:
            print(f"Repartition failed with: {type(e).__name__}: {e}")
            raise


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s", "--tb=short"])
