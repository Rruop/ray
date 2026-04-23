import itertools
import uuid
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterator, List, Optional, Union

from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    MapTransformer,
)
from ray.data.block import Block, BlockAccessor
from ray.data.context import DataContext
from ray.data.datasource.datasink import Datasink
from ray.data.datasource.datasource import Datasource

if TYPE_CHECKING:
    from ray.data._internal.logical.operators import Write
    from ray.data.expressions import Expr

WRITE_UUID_KWARG_NAME = "write_uuid"
# Key for storing pending checkpoint paths for commit phase
PENDING_CHECKPOINTS_KWARG_NAME = "_pending_checkpoints"


def _filter_blocks_with_expr(
    blocks: Iterator[Block],
    filter_expr: "Expr",
) -> Iterator[Block]:
    """Filter blocks using Arrow expression (vectorized)."""
    for block in blocks:
        yield BlockAccessor.for_block(block).filter(filter_expr)


def _filter_blocks_with_fn(
    blocks: Iterator[Block],
    filter_fn: Callable[[Dict[str, Any]], bool],
) -> Iterator[Block]:
    """Filter blocks using a Python function applied row-by-row."""
    import pyarrow as pa

    for block in blocks:
        df = BlockAccessor.for_block(block).to_pandas()
        mask = df.apply(lambda row: filter_fn(row.to_dict()), axis=1)
        yield pa.Table.from_pandas(df[mask], preserve_index=False)


def generate_write_fn(
    datasink_or_legacy_datasource: Union[Datasink, Datasource],
    filter_fn: Optional[Callable[[Dict[str, Any]], bool]] = None,
    filter_expr: Optional["Expr"] = None,
    **write_args,
) -> Callable[[Iterator[Block], TaskContext], Iterator[Block]]:
    def fn(blocks: Iterator[Block], ctx: TaskContext) -> Iterator[Block]:
        """Write blocks to the datasink, optionally filtering before write.

        When a filter is provided, filters blocks before writing but returns the
        original unfiltered blocks for downstream processing (e.g., checkpoint).
        """
        has_filter = filter_expr is not None or filter_fn is not None

        # Only tee the iterator when filtering is needed
        if has_filter:
            blocks_to_write, blocks_to_return = itertools.tee(blocks, 2)
            if filter_expr is not None:
                blocks_to_write = _filter_blocks_with_expr(blocks_to_write, filter_expr)
            else:
                blocks_to_write = _filter_blocks_with_fn(blocks_to_write, filter_fn)
        else:
            # No filtering: both variables reference the same iterator
            blocks_to_write = blocks
            blocks_to_return = blocks

        if isinstance(datasink_or_legacy_datasource, Datasink):
            ctx.kwargs["_datasink_write_return"] = datasink_or_legacy_datasource.write(
                blocks_to_write, ctx
            )
        else:
            datasink_or_legacy_datasource.write(blocks_to_write, ctx, **write_args)

        return blocks_to_return

    return fn


def generate_collect_write_stats_fn() -> BlockMapTransformFn:
    # If the write op succeeds, the resulting Dataset is a list of
    # one Block which contain stats/metrics about the write.
    # Otherwise, an error will be raised. The Datasource can handle
    # execution outcomes with `on_write_complete()`` and `on_write_failed()``.
    def fn(blocks: Iterator[Block], ctx: TaskContext) -> Iterator[Block]:
        """Handles stats collection for block writes."""
        block_accessors = [BlockAccessor.for_block(block) for block in blocks]
        total_num_rows = sum(ba.num_rows() for ba in block_accessors)
        total_size_bytes = sum(ba.size_bytes() for ba in block_accessors)

        # Store actual write stats in context for _map_task to use when
        # constructing BlockMetadata. This ensures the output block's metadata
        # reflects the actual rows/bytes written, not the stats DataFrame's 1 row.
        ctx.kwargs["_write_stats_num_rows"] = total_num_rows
        ctx.kwargs["_write_stats_size_bytes"] = total_size_bytes

        # NOTE: Write tasks can return anything, so we need to wrap it in a valid block
        # type.
        import pandas as pd

        block = pd.DataFrame(
            {
                "num_rows": [total_num_rows],
                "size_bytes": [total_size_bytes],
                "write_return": [ctx.kwargs.get("_datasink_write_return", None)],
            }
        )
        return iter([block])

    return BlockMapTransformFn(
        fn,
        is_udf=False,
        disable_block_shaping=True,
    )


def plan_write_op(
    op: "Write",
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> PhysicalOperator:
    collect_stats_fn = generate_collect_write_stats_fn()

    return _plan_write_op_internal(
        op,
        physical_children,
        data_context,
        post_transformations=[collect_stats_fn],
    )


def _plan_write_op_internal(
    op: "Write",
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
    post_transformations: List[BlockMapTransformFn],
    pre_transformations: Optional[List[BlockMapTransformFn]] = None,
) -> PhysicalOperator:
    """Plan a write operation with optional pre and post write transformations.

    Args:
        op: The write operator.
        physical_children: The physical children operators.
        data_context: The data context.
        post_transformations: Transformations to run AFTER the write.
        pre_transformations: Transformations to run BEFORE the write.
            Useful for 2-phase commit where pending checkpoint is written first.

    Returns:
        The physical operator for the write operation.
    """
    assert len(physical_children) == 1
    input_physical_dag = physical_children[0]

    datasink = op.datasink_or_legacy_datasource
    write_fn = generate_write_fn(
        datasink,
        filter_fn=op.filter_fn,
        filter_expr=op.filter_expr,
        **op.write_args,
    )

    # Build transform chain: pre_write -> write -> post_write
    pre_transforms = pre_transformations or []
    write_transform = BlockMapTransformFn(
        write_fn,
        is_udf=False,
        # NOTE: No need for block-shaping
        disable_block_shaping=True,
    )
    transform_fns = pre_transforms + [write_transform] + post_transformations

    map_transformer = MapTransformer(transform_fns)

    # Set up on_start callback for datasinks.
    # This allows on_write_start to receive the schema from the first input bundle,
    # enabling schema-dependent initialization (e.g., Iceberg schema evolution).
    on_start = None
    if isinstance(datasink, Datasink):
        on_start = datasink.on_write_start

    map_op = MapOperator.create(
        map_transformer,
        input_physical_dag,
        data_context,
        name="Write",
        # Add a UUID to write tasks to prevent filename collisions. This a UUID for the
        # overall write operation, not the individual write tasks.
        map_task_kwargs={WRITE_UUID_KWARG_NAME: uuid.uuid4().hex},
        ray_remote_args=op.ray_remote_args,
        min_rows_per_bundle=op.min_rows_per_bundled_input,
        compute_strategy=op.compute,
        on_start=on_start,
    )

    return map_op
