from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Union

from ray.data._internal.compute import ComputeStrategy
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators.map_operator import AbstractMap
from ray.data.datasource.datasink import Datasink
from ray.data.datasource.datasource import Datasource

if TYPE_CHECKING:
    from ray.data.expressions import Expr

__all__ = [
    "Write",
]


class Write(AbstractMap):
    """Logical operator for write."""

    def __init__(
        self,
        input_op: LogicalOperator,
        datasink_or_legacy_datasource: Union[Datasink, Datasource],
        ray_remote_args: Optional[Dict[str, Any]] = None,
        compute: Optional[ComputeStrategy] = None,
        filter_fn: Optional[Callable[[Dict[str, Any]], bool]] = None,
        filter_expr: Optional["Expr"] = None,
        **write_args,
    ):
        # Validate filter parameters early
        if filter_fn is not None and filter_expr is not None:
            raise ValueError(
                "Only one of `filter_fn` or `filter_expr` can be set, not both."
            )

        if isinstance(datasink_or_legacy_datasource, Datasink):
            min_rows_per_bundled_input = (
                datasink_or_legacy_datasource.min_rows_per_write
            )
        else:
            min_rows_per_bundled_input = None

        super().__init__(
            input_op=input_op,
            can_modify_num_rows=True,
            min_rows_per_bundled_input=min_rows_per_bundled_input,
            ray_remote_args=ray_remote_args,
            compute=compute,
        )
        self.datasink_or_legacy_datasource = datasink_or_legacy_datasource
        self.write_args = write_args
        self.filter_fn = filter_fn
        self.filter_expr = filter_expr
