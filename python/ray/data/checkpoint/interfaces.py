import os
import warnings
from enum import Enum
from typing import TYPE_CHECKING, Optional, Tuple

import pyarrow

from ray.util.annotations import DeveloperAPI, PublicAPI

if TYPE_CHECKING:
    from ray.data.datasource import PathPartitionFilter


@PublicAPI(stability="alpha")
class CheckpointBackend(Enum):
    """Supported backends for storing and reading checkpoint files.

    Currently, only one type of backend is supported:

    * Batch-based backends: CLOUD_OBJECT_STORAGE and FILE_STORAGE.

    Their differences are as follows:

    1. Writing checkpoints: Batch-based backends write a checkpoint file
       for each block.
    2. Loading checkpoints and filtering input data: Batch-based backends
       load all checkpoint data into memory prior to dataset execution.
       The checkpoint data is then passed to each read task to perform filtering.
    """

    CLOUD_OBJECT_STORAGE = "CLOUD_OBJECT_STORAGE"
    """
    Batch-based checkpoint backend that uses cloud object storage, such as
    AWS S3, Google Cloud Storage, etc.
    """

    FILE_STORAGE = "FILE_STORAGE"
    """
    Batch based checkpoint backend that uses file system storage.
    Note, when using this backend, the checkpoint path must be a network-mounted
    file system (e.g. `/mnt/cluster_storage/`).
    """


@PublicAPI(stability="beta")
class CheckpointConfig:
    """Configuration for checkpointing.

    Args:
        id_column: Name of the ID column in the input dataset.
            ID values must be unique across all rows in the dataset and must persist
            during all operators.
        checkpoint_path: Path to store the checkpoint data. It can be a path to a cloud
            object storage (e.g. `s3://bucket/path`) or a file system path.
            If the latter, the path must be a network-mounted file system (e.g.
            `/mnt/cluster_storage/`) that is accessible to the entire cluster.
            If not set, defaults to `RAY_DATA_CHECKPOINT_PATH_BUCKET/ray_data_checkpoint`.
        delete_checkpoint_on_success: If true, automatically delete checkpoint
            data when the dataset execution succeeds. Only supported for
            batch-based backend currently.
        override_filesystem: Override the :class:`pyarrow.fs.FileSystem` object used to
            read/write checkpoint data. Use this when you want to use custom credentials.
        override_backend: Override the :class:`CheckpointBackend` object used to
            access the checkpoint backend storage.
        filter_num_threads: Number of threads used to filter checkpointed rows.
        write_num_threads: Number of threads used to write checkpoint files for
            completed rows.
        checkpoint_path_partition_filter: Filter for checkpoint files to load during
            restoration when reading from `checkpoint_path`.
        use_bloom_filter: If true, use a Bloom Filter for per-block checkpoint
            membership testing instead of the default sort + binary-search path.
            The Bloom Filter path has zero false negatives (no row is ever
            re-processed) and configurable false positives (a tiny number of
            unprocessed rows may be skipped). It removes the need to sort the
            checkpointed-id dataset, which avoids a full distributed shuffle.
            Mutually exclusive with ``use_roaring_bitmap``.
        bloom_filter_error_rate: Target false-positive rate of the Bloom Filter.
            Must lie in the open interval ``(0, 1)``. Smaller values trade memory
            for fewer over-filtered rows. Only used when
            ``use_bloom_filter=True``. Defaults to ``1e-6``.

            Approximate memory footprint of the filter, per Ray Data worker
            node (the filter is built once and shared via the object store,
            so all map workers on a node share a single mmap-backed copy):

            ============  ================  ================
            data size n   memory @ 1e-4     memory @ 1e-6
            ============  ================  ================
            1 M           2.29 MB           3.43 MB
            10 M          22.85 MB          34.28 MB
            100 M         228.53 MB         342.79 MB
            500 M         1.12 GB           1.67 GB
            1 B           2.23 GB           3.35 GB
            5 B           11.16 GB          16.74 GB
            ============  ================  ================

            Rules of thumb: ``1e-4`` uses ~2.40 bytes/item with 13 hashes;
            ``1e-6`` uses ~3.59 bytes/item with 20 hashes. Choose ``1e-4``
            when over-filtering a few rows out of 10,000 is acceptable
            (typical for idempotent downstream operators) and memory is the
            scarcer resource; choose ``1e-6`` or smaller when even rare
            duplicate-skip is costly (e.g. non-idempotent side effects).

        Note: Only one of `post_checkpoint_filter_expr` or `post_checkpoint_filter_fn` can be set.
        When set, data will be checkpointed first, then filtered before writing to the
        final destination. This ensures filtered data is also recorded in checkpoint
        and won't be reprocessed on restart.
    """

    DEFAULT_CHECKPOINT_PATH_BUCKET_ENV_VAR = "RAY_DATA_CHECKPOINT_PATH_BUCKET"
    DEFAULT_CHECKPOINT_PATH_DIR = "ray_data_checkpoint"

    def __init__(
        self,
        id_column: Optional[str] = None,
        checkpoint_path: Optional[str] = None,
        *,
        delete_checkpoint_on_success: bool = True,
        override_filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        override_backend: Optional[CheckpointBackend] = None,
        filter_num_threads: int = 3,
        write_num_threads: int = 3,
        checkpoint_path_partition_filter: Optional["PathPartitionFilter"] = None,
        checkpoint_read_override_num_blocks: Optional[int] = None,
        redis_checkpoint_key:Optional[str] = None,
        redis_checkpoint_host: Optional[str] = 'public-xm-c-stagingredis51.idchb1az1.hb1.kwaidc.com',
        redis_checkpoint_port: Optional[int] = 16942,
        redis_checkpoint_password: Optional[str] = '',
        redis_checkpoint_pipeline_batch_size: Optional[int] = 1000,
        redis_data_storage_as_roaring_bitmap: bool = True,
        use_roaring_bitmap: bool = False,
        use_bloom_filter: bool = False,
        bloom_filter_error_rate: float = 1e-6,
        need_deduplication: bool = False,
        write_checkpoint_retry_number: Optional[int] = 10,
    ):
        self.id_column: Optional[str] = id_column

        if not isinstance(self.id_column, str) or len(self.id_column) == 0:
            raise InvalidCheckpointingConfig(
                "Checkpoint ID column must be a non-empty string, "
                f"but got {self.id_column}"
            )

        if override_backend is not None:
            warnings.warn(
                "`override_backend` is deprecated and will be removed in August 2025.",
                FutureWarning,
                stacklevel=2,
            )

        self.checkpoint_path: str = (
            checkpoint_path or self._get_default_checkpoint_path()
        )
        inferred_backend, inferred_fs = self._infer_backend_and_fs(
            self.checkpoint_path,
            override_filesystem,
            override_backend,
        )
        self.filesystem: "pyarrow.fs.FileSystem" = inferred_fs
        self.backend: CheckpointBackend = inferred_backend
        self.delete_checkpoint_on_success: bool = delete_checkpoint_on_success
        self.filter_num_threads: int = filter_num_threads
        self.write_num_threads: int = write_num_threads
        self.checkpoint_path_partition_filter = checkpoint_path_partition_filter
        self.checkpoint_read_override_num_blocks = checkpoint_read_override_num_blocks
        self.redis_checkpoint_key = redis_checkpoint_key
        self.redis_checkpoint_host = redis_checkpoint_host
        self.redis_checkpoint_port = redis_checkpoint_port
        self.redis_checkpoint_password = redis_checkpoint_password
        self.redis_checkpoint_pipeline_batch_size = redis_checkpoint_pipeline_batch_size
        self.redis_data_storage_as_roaring_bitmap = redis_data_storage_as_roaring_bitmap
        self.use_roaring_bitmap: bool = use_roaring_bitmap
        self.use_bloom_filter: bool = use_bloom_filter
        self.bloom_filter_error_rate: float = bloom_filter_error_rate
        self.write_checkpoint_retry_number = write_checkpoint_retry_number
        self.need_deduplication = need_deduplication


        if use_bloom_filter and use_roaring_bitmap:
            raise InvalidCheckpointingConfig(
                "`use_bloom_filter` and `use_roaring_bitmap` are mutually exclusive; "
                "set at most one of them."
            )
        if not 0.0 < bloom_filter_error_rate < 1.0:
            raise InvalidCheckpointingConfig(
                "`bloom_filter_error_rate` must be in the open interval (0, 1), "
                f"got {bloom_filter_error_rate!r}."
            )

    def _get_default_checkpoint_path(self) -> str:
        artifact_storage = os.environ.get(self.DEFAULT_CHECKPOINT_PATH_BUCKET_ENV_VAR)
        if artifact_storage is None:
            raise InvalidCheckpointingConfig(
                f"`{self.DEFAULT_CHECKPOINT_PATH_BUCKET_ENV_VAR}` env var is not set, "
                "please explicitly set `CheckpointConfig.checkpoint_path`."
            )
        return f"{artifact_storage}/{self.DEFAULT_CHECKPOINT_PATH_DIR}"

    def _infer_backend_and_fs(
        self,
        checkpoint_path: str,
        override_filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        override_backend: Optional[CheckpointBackend] = None,
    ) -> Tuple[CheckpointBackend, "pyarrow.fs.FileSystem"]:
        try:
            if override_filesystem is not None:
                assert isinstance(override_filesystem, pyarrow.fs.FileSystem), (
                    "override_filesystem must be an instance of "
                    f"`pyarrow.fs.FileSystem`, but got {type(override_filesystem)}"
                )
                fs = override_filesystem
            else:
                fs, _ = pyarrow.fs.FileSystem.from_uri(checkpoint_path)

            if override_backend is not None:
                assert isinstance(override_backend, CheckpointBackend), (
                    "override_backend must be an instance of `CheckpointBackend`, "
                    f"but got {type(override_backend)}"
                )
                backend = override_backend
            else:
                if isinstance(fs, pyarrow.fs.LocalFileSystem):
                    backend = CheckpointBackend.FILE_STORAGE
                else:
                    backend = CheckpointBackend.CLOUD_OBJECT_STORAGE

            return backend, fs
        except Exception as e:
            raise InvalidCheckpointingConfig(
                f"Invalid checkpoint path: {checkpoint_path}. "
            ) from e


@DeveloperAPI
class InvalidCheckpointingConfig(Exception):
    """Exception which indicates that the checkpointing
    configuration is invalid."""

    pass


@DeveloperAPI
class InvalidCheckpointingOperators(Exception):
    """Exception which indicates that the DAG is not eligible for checkpointing,
    due to one or more incompatible operators."""

    pass
