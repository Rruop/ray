import abc
import logging
import os
import posixpath
import time
from typing import List, Optional

import numpy
import pyarrow
from pyarrow.fs import FileSelector, FileType
import redis

import ray
from ray._common.retry import call_with_retry
from ray.data._internal.arrow_ops import transform_pyarrow
from ray.data._internal.execution.interfaces.ref_bundle import RefBundle
from ray.data.block import Block, BlockAccessor, BlockMetadata, DataBatch, Schema
from ray.data.checkpoint import CheckpointConfig
from ray.data.checkpoint.checkpoint_writer import PENDING_CHECKPOINT_SUFFIX
from ray.data.checkpoint.util import build_pending_checkpoint_trie
from ray.data.checkpoint.bitmap64 import RoaringBitmap64
from ray.data.checkpoint.bloom_filter import (
    BloomFilterCheckpoint,
    _hash_id_column_to_uint64,
    build_bloom_filter_remote,
)
from ray.data.datasource import PathPartitionFilter
from ray.data.datasource.path_util import _unwrap_protocol
from ray.types import ObjectRef
from redis import Redis

import numpy as np
from pyroaring import BitMap
import pyarrow as pa
import threading

logger = logging.getLogger(__name__)

# ====== MODULE-LEVEL PER-WORKER CACHE (KEY!) ======
# Each Ray worker process has its own copy.
_CACHED_ROARING_BITMAP: Optional[BitMap] = None
_CACHED_CHECKPOINT_REF_ID: Optional[str] = None  # e.g., hash or path
_CACHE_LOCK = threading.Lock()
_REDIS_CHECKPOINT_EXISTS:Optional[str] = None
_REDIS_CHECKPOINT_EXISTS_LOCK = threading.Lock()



def _build_bitmap_from_checkpoint_table(ckpt_table: pa.Table, id_column: str) -> BitMap:
    """Build a single Roaring Bitmap from the entire checkpoint table."""
    id_col = ckpt_table[id_column]

    if isinstance(id_col, pa.ChunkedArray):
        # Efficiently concatenate all chunks into one numpy array
        arrays = [chunk.to_numpy(zero_copy_only=True) for chunk in id_col.chunks]
        if len(arrays) == 1:
            ckpt_ids = arrays[0]
        else:
            ckpt_ids = np.concatenate(arrays)
    else:
        ckpt_ids = id_col.to_numpy(zero_copy_only=True)

    return BitMap(ckpt_ids)


def _get_or_build_roaring_bitmap(
    ckpt_table: pa.Table,
    id_column: str,
    checkpoint_key: str = "default",
) -> BitMap:
    """Get cached bitmap or build it once per worker."""
    global _CACHED_ROARING_BITMAP, _CACHED_CHECKPOINT_REF_ID

    with _CACHE_LOCK:
        if _CACHED_ROARING_BITMAP is None or _CACHED_CHECKPOINT_REF_ID != checkpoint_key:
            _CACHED_ROARING_BITMAP = _build_bitmap_from_checkpoint_table(ckpt_table, id_column)
            _CACHED_CHECKPOINT_REF_ID = checkpoint_key
            print(f"[Worker] Built Roaring Bitmap with {len(_CACHED_ROARING_BITMAP)} IDs")

    return _CACHED_ROARING_BITMAP

# Retry configuration for checkpoint recovery operations.
# These can be overridden via environment variables for testing or tuning.
CHECKPOINT_RECOVERY_MAX_ATTEMPTS = int(
    os.environ.get("RAY_DATA_CHECKPOINT_RECOVERY_MAX_ATTEMPTS", "3")
)
CHECKPOINT_RECOVERY_MAX_BACKOFF_S = int(
    os.environ.get("RAY_DATA_CHECKPOINT_RECOVERY_MAX_BACKOFF_S", "8")
)

def _is_redis_checkpoint_key_exists(
    redis_key: str,
    redis_client: Redis,
) -> str:
    """Get cached bitmap or build it once per worker."""
    global _REDIS_CHECKPOINT_EXISTS

    with _REDIS_CHECKPOINT_EXISTS_LOCK:
        if _REDIS_CHECKPOINT_EXISTS is None:
            _REDIS_CHECKPOINT_EXISTS = str(redis_client.exists(redis_key))

    return _REDIS_CHECKPOINT_EXISTS


class CheckpointFilter(abc.ABC):
    """Abstract class which defines the interface for filtering checkpointed rows
    based on varying backends.
    """

    def __init__(self, config: CheckpointConfig):
        self.ckpt_config = config
        self.checkpoint_path = self.ckpt_config.checkpoint_path
        self.checkpoint_path_unwrapped = _unwrap_protocol(
            self.ckpt_config.checkpoint_path
        )
        self.id_column = self.ckpt_config.id_column
        self.filesystem = self.ckpt_config.filesystem
        self.filter_num_threads = self.ckpt_config.filter_num_threads
        self.redis_checkpoint_key = self.ckpt_config.redis_checkpoint_key
        self.redis_checkpoint_host = self.ckpt_config.redis_checkpoint_host
        self.redis_checkpoint_port = self.ckpt_config.redis_checkpoint_port
        self.redis_checkpoint_password = self.ckpt_config.redis_checkpoint_password
        self.redis_checkpoint_pipeline_batch_size = self.ckpt_config.redis_checkpoint_pipeline_batch_size
        self.redis_data_storage_as_roaring_bitmap = self.ckpt_config.redis_data_storage_as_roaring_bitmap
        self.use_roaring_bitmap = self.ckpt_config.use_roaring_bitmap
        self.use_bloom_filter = self.ckpt_config.use_bloom_filter
        self.bloom_filter_error_rate = self.ckpt_config.bloom_filter_error_rate

@ray.remote(num_cpus=0)
def _clean_pending_checkpoints_task(
    checkpoint_path_unwrapped: str,
    checkpoint_filesystem: pyarrow.fs.FileSystem,
    data_file_dir_unwrapped: str,
    data_file_filesystem: pyarrow.fs.FileSystem,
) -> int:
    """Delete data files that have matching pending checkpoint files, then
    delete the pending checkpoints.

    This runs as a remote task to avoid blocking the driver during potentially
    slow filesystem operations (especially on cloud storage like S3).

    Algorithm:
    1. List all files in checkpoint dir, find those ending with .pending.parquet
    2. Build a PrefixTrie from their basenames (strip .pending.parquet)
    3. List all data files in data_file_dir (recursively for partitions)
    4. For each data file, if trie.has_prefix_of(basename) -> delete it
    5. Delete all the pending checkpoint files
    6. Return count of pending checkpoints cleaned

    Args:
        checkpoint_path_unwrapped: The unwrapped checkpoint path.
        checkpoint_filesystem: The filesystem for checkpoint files.
        data_file_dir_unwrapped: The unwrapped directory where data files are
            written (protocol prefix already stripped).
        data_file_filesystem: The filesystem for data files. May differ from
            checkpoint_filesystem (e.g., checkpoints on local disk, data on S3).

    Returns:
        Number of pending checkpoints cleaned.
    """

    def _clean() -> int:
        # 1. List all files in checkpoint dir, find pending ones
        ckpt_files = checkpoint_filesystem.get_file_info(
            FileSelector(checkpoint_path_unwrapped, recursive=False)
        )
        pending_suffix = f"{PENDING_CHECKPOINT_SUFFIX}.parquet"
        pending_file_paths = [
            f
            for f in ckpt_files
            if f.type == FileType.File and f.path.endswith(pending_suffix)
        ]

        if not pending_file_paths:
            return 0

        # 2. Build prefix trie from pending checkpoint basenames
        trie = build_pending_checkpoint_trie(pending_file_paths, pending_suffix)

        # 3. List all data files (recursively for partitions)
        data_files = data_file_filesystem.get_file_info(
            FileSelector(data_file_dir_unwrapped, recursive=True, allow_not_found=True)
        )

        # 4. Delete data files matching a pending checkpoint prefix
        for f in data_files:
            if f.type != FileType.File:
                continue
            basename = posixpath.basename(f.path)
            if trie.has_prefix_of(basename):
                data_file_filesystem.delete_file(f.path)

        # 5. Delete all pending checkpoint files
        for f in pending_file_paths:
            checkpoint_filesystem.delete_file(f.path)

        return len(pending_file_paths)

    return call_with_retry(
        _clean,
        description="clean pending checkpoints",
        max_attempts=CHECKPOINT_RECOVERY_MAX_ATTEMPTS,
        max_backoff_s=CHECKPOINT_RECOVERY_MAX_BACKOFF_S,
    )


@ray.remote(max_retries=-1)
def _combine_chunks(ckpt_block: pyarrow.Table) -> pyarrow.Table:
    """Combine chunks for the checkpoint block.

    Args:
        ckpt_block: The checkpoint block to combine chunks for

    Returns:
        The combined checkpoint block
    """
    from ray.data._internal.arrow_ops.transform_pyarrow import combine_chunks

    combined_ckpt_block = combine_chunks(ckpt_block)
    logger.debug(
        "Checkpoint block stats for id column checkpoint: Combined block: type=%s, %d rows, %d bytes",
        combined_ckpt_block.schema.to_string(),
        combined_ckpt_block.num_rows,
        combined_ckpt_block.nbytes,
    )

    return combined_ckpt_block

from pyarrow.fs import FileType

class SkipEmptyParquetFilter:
    def __init__(self, filesystem):
        self._fs = filesystem

    def apply(self, path: str) -> bool:
        """
        Determine whether to keep a single file path.
        Returns True → keep the file; False → skip (filter out).
        """
        # Keep non-parquet files (e.g., _SUCCESS)
        if not path.endswith(".parquet"):
            return True

        try:
            # Get file metadata for the given path
            info = self._fs.get_file_info(path)
            
            # Check if it's a regular file and non-empty
            if info.type == FileType.File and info.size > 0:
                return True
            else:
                return False  # Filter out empty files or directories
        except Exception:
            # Optional: log the error or silently filter out problematic paths
            return False

class CheckpointLoader:
    """Loading checkpoint data."""

    def __init__(
        self,
        checkpoint_path: str,
        filesystem: pyarrow.fs.FileSystem,
        id_column: str,
        redis_checkpoint_key: str,
        redis_checkpoint_host: str,
        redis_checkpoint_port: int,
        redis_checkpoint_password: str,
        redis_checkpoint_pipeline_batch_size: int,
        use_roaring_bitmap: bool,
        checkpoint_path_partition_filter: Optional[PathPartitionFilter] = None,
        checkpoint_read_override_num_blocks:Optional[int] = None,
        redis_data_storage_as_roaring_bitmap: bool = False,
        use_bloom_filter: bool = False,
    ):
        """Initialize the CheckpointLoader.

        Args:
            checkpoint_path: The path to the checkpoint
            filesystem: The filesystem to use
            id_column: The name of the ID column
            checkpoint_path_partition_filter: Filter for checkpoint files to load during
                restoration when reading from `checkpoint_path`.
        """
        self.checkpoint_path = checkpoint_path
        self.filesystem = filesystem
        self.id_column = id_column
        self.checkpoint_path_partition_filter = checkpoint_path_partition_filter
        self.checkpoint_read_override_num_blocks = checkpoint_read_override_num_blocks
        self.redis_checkpoint_key = redis_checkpoint_key
        self.redis_checkpoint_host = redis_checkpoint_host
        self.redis_checkpoint_port = redis_checkpoint_port
        self.redis_checkpoint_password = redis_checkpoint_password
        self.redis_checkpoint_pipeline_batch_size = redis_checkpoint_pipeline_batch_size
        self.redis_data_storage_as_roaring_bitmap = redis_data_storage_as_roaring_bitmap
        self.use_roaring_bitmap = use_roaring_bitmap
        self.use_bloom_filter = use_bloom_filter

    def remove_empty_parquet_files(self):
        from pyarrow.fs import FileSelector,FileType
        selector = FileSelector(self.checkpoint_path, recursive=True)
        file_infos = self.filesystem.get_file_info(selector)
        
        for info in file_infos:
            if (info.type == FileType.File 
                and info.path.endswith(".parquet") 
                and info.size == 0):
                print(f"Removing empty Parquet file: {info.path}")
                self.filesystem.delete_file(info.path)

    def _skip_empty_parquet(self):
        """Skip zero-byte .parquet files."""
        from pyarrow.fs import FileSelector,FileType
        infos = self.filesystem.get_file_info(self.checkpoint_path)
        result = []
        for path, info in zip(self.checkpoint_path, infos):
            if path.endswith(".parquet"):
                if info.type == FileType.File and info.size > 0:
                    result.append(path)
            else:
                result.append(path)  # keep non-parquet
        return result

    def load_checkpoint(self) -> ObjectRef[Block]:
        """Loading checkpoint data.

        Returns:
            ObjectRef[Block]: ObjectRef to the checkpointed IDs block.
        """
        ## if checkpoint data save in redis, skip
        if self.redis_checkpoint_key:
            return
        start_t = time.time()

        # Load the checkpoint data
        if self.checkpoint_path_partition_filter is None:
            self.checkpoint_path_partition_filter = SkipEmptyParquetFilter(self.filesystem)
        checkpoint_ds: ray.data.Dataset = ray.data.read_parquet(
            self.checkpoint_path,
            filesystem=self.filesystem,
            partition_filter=self.checkpoint_path_partition_filter,
            override_num_blocks= self.checkpoint_read_override_num_blocks,
        )

        # Manually disable checkpointing for loading the checkpoint metadata
        # to avoid recursively restoring checkpoints.
        # TODO: Clean way to do this would be to introduce per Op config
        # [https://github.com/ray-project/ray/issues/54520]
        checkpoint_ds.context.checkpoint_config = None

        # Pre-process data pipeline
        checkpoint_ds: ray.data.Dataset = self._preprocess_data_pipeline(checkpoint_ds)

        # Repartition to 1 block.
        checkpoint_ds = checkpoint_ds.repartition(num_blocks=1)

        # Get the block reference
        ref_bundles: List[RefBundle] = list(checkpoint_ds.iter_internal_ref_bundles())
        assert len(ref_bundles) == 1
        ref_bundle: RefBundle = ref_bundles[0]
        schema: Schema = ref_bundle.schema
        assert len(ref_bundle.blocks) == 1
        block_ref: ObjectRef[Block] = ref_bundle.blocks[0][0]
        metadata: BlockMetadata = ref_bundle.blocks[0][1]

        # Post-process the block
        checkpoint_block_ref: ObjectRef[Block] = self._postprocess_block(block_ref)

        # Validate the loaded checkpoint
        self._validate_loaded_checkpoint(schema, metadata)

        logger.info(
            "Checkpoint loaded for %s in %.2f seconds. SizeBytes = %d, Schema = %s",
            type(self).__name__,
            time.time() - start_t,
            metadata.size_bytes,
            schema.to_string(),
        )

        return checkpoint_block_ref

    @abc.abstractmethod
    def _preprocess_data_pipeline(
        self, checkpoint_ds: ray.data.Dataset
    ) -> ray.data.Dataset:
        """Pre-process the checkpoint dataset. To be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement this method")

    def _postprocess_block(self, block_ref: ObjectRef[Block]) -> ObjectRef[Block]:
        """Combine the block so it has fewer chunks."""
        return _combine_chunks.remote(block_ref)

    def _validate_loaded_checkpoint(
        self, schema: Schema, metadata: BlockMetadata
    ) -> None:
        """Validate the loaded checkpoint. Subclasses can override for custom validation."""
        pass


class IdColumnCheckpointLoader(CheckpointLoader):
    """Loader for regular ID columns."""

    def _preprocess_data_pipeline(
        self, checkpoint_ds: ray.data.Dataset
    ) -> ray.data.Dataset:
        """In the pre-process data pipeline,
            - Sort by the IDs, as `filter_rows_for_block` will perform binary search on the
              checkpointed IDs during restore.

        Args:
            checkpoint_ds: The checkpoint dataset to pre-process

        Returns:
            The pre-processed checkpoint dataset
        """
        # Bloom Filter and Roaring Bitmap paths do not require sorted input.
        if self.use_bloom_filter or self.use_roaring_bitmap:
            return checkpoint_ds
        else:
            return checkpoint_ds.sort(self.id_column)


class BatchBasedCheckpointFilter(CheckpointFilter):
    """CheckpointFilter for batch-based backends."""

    def load_checkpoint(
        self,
        data_file_dir: Optional[str] = None,
        data_file_filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    ) -> ObjectRef[Block]:
        """Load checkpointed ids as a sorted block.

        This method first cleans up any pending checkpoints from incomplete
        2-phase commits, then loads the committed checkpoint data.

        Args:
            data_file_dir: Optional directory where data files are written.
                If provided, pending checkpoints will be used to find and
                delete matching data files before loading.
            data_file_filesystem: Optional filesystem for data files. If not
                provided, defaults to the checkpoint filesystem. Should be
                provided when data files are on a different filesystem than
                checkpoints.

        Returns:
            ObjectRef[Block]: ObjectRef to the checkpointed IDs block.
        """
        # Clean up pending checkpoints before loading (runs as a Ray task)
        if data_file_dir is not None:
            self._clean_pending_checkpoints(data_file_dir, data_file_filesystem)

        loader = IdColumnCheckpointLoader(
            checkpoint_path=self.checkpoint_path,
            filesystem=self.filesystem,
            id_column=self.id_column,
            checkpoint_path_partition_filter=self.ckpt_config.checkpoint_path_partition_filter,
            checkpoint_read_override_num_blocks=self.ckpt_config.checkpoint_read_override_num_blocks,
            redis_checkpoint_key=self.ckpt_config.redis_checkpoint_key,
            redis_checkpoint_host=self.ckpt_config.redis_checkpoint_host,
            redis_checkpoint_port=self.ckpt_config.redis_checkpoint_port,
            redis_checkpoint_password=self.ckpt_config.redis_checkpoint_password,
            redis_checkpoint_pipeline_batch_size=self.ckpt_config.redis_checkpoint_pipeline_batch_size,
            redis_data_storage_as_roaring_bitmap=self.ckpt_config.redis_data_storage_as_roaring_bitmap,
            use_roaring_bitmap =self.ckpt_config.use_roaring_bitmap,
            use_bloom_filter=self.ckpt_config.use_bloom_filter,
        )
        return loader.load_checkpoint()

    def _clean_pending_checkpoints(
        self,
        data_file_dir: str,
        data_file_filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    ) -> None:
        """Clean up pending checkpoints from incomplete 2-phase commits.

        Finds pending checkpoint files, builds a prefix trie from their basenames,
        deletes matching data files, then deletes the pending checkpoints.

        Runs as a Ray task to avoid blocking the driver during potentially
        slow filesystem operations (especially on cloud storage like S3).

        Args:
            data_file_dir: The directory where data files are written.
            data_file_filesystem: The filesystem for data files. If not
                provided, defaults to the checkpoint filesystem.
        """
        if data_file_filesystem is None:
            data_file_filesystem = self.filesystem
        try:
            cleaned_count = ray.get(
                _clean_pending_checkpoints_task.remote(
                    self.checkpoint_path_unwrapped,
                    self.filesystem,
                    _unwrap_protocol(data_file_dir),
                    data_file_filesystem,
                )
            )
            if cleaned_count > 0:
                logger.info(f"Cleaned up {cleaned_count} pending checkpoint(s)")
        except ray.exceptions.RayTaskError:
            logger.exception("Failed to clean up pending checkpoints")
            raise

    def load_bloom_filter(self) -> ObjectRef[BloomFilterCheckpoint]:
        """Build the bloom filter once via a remote task and return an ObjectRef.

        Returns ``None`` when ``use_bloom_filter`` is not set on the
        underlying :class:`CheckpointConfig`. Otherwise loads the checkpoint
        id table, dispatches
        :func:`ray.data.checkpoint.bloom_filter.build_bloom_filter_remote` on
        a worker, and returns an ``ObjectRef`` to the built
        :class:`BloomFilterCheckpoint`. The same ``ObjectRef`` is later
        broadcast to every map worker via task kwargs so that all workers
        share a single physical copy of the filter through the Ray object
        store.
        """
        if not self.ckpt_config.use_bloom_filter:
            return None
        block_ref = self.load_checkpoint()
        if block_ref is None:
            return None
        return build_bloom_filter_remote.remote(
            block_ref,
            self.id_column,
            self.ckpt_config.bloom_filter_error_rate,
        )

    def delete_checkpoint(self) -> None:
        self.filesystem.delete_dir(self.checkpoint_path_unwrapped)
        if self.redis_checkpoint_key :
            try:
                redis_client = redis.Redis(host=self.redis_checkpoint_host, port=self.redis_checkpoint_port, password=self.redis_checkpoint_password, decode_responses=True)
                redis_client.delete(self.redis_checkpoint_key)
            except storage.Error as e:
                exit(1)
            redis_client.close()



    def filter_rows_for_block(
        self,
        block: Block,
        checkpointed_ids: Block,
    ) -> Block:
        """For the given block, filter out rows that have already
        been checkpointed, and return the resulting block.

        Args:
            block: The input block to filter.
            checkpointed_ids: A block containing IDs of all rows that have
                been checkpointed.
        Returns:
            A new block with rows that have not been checkpointed.
        """

        if len(checkpointed_ids) == 0 or len(block) == 0:
            return block

        assert isinstance(block, pyarrow.Table)
        assert isinstance(checkpointed_ids, pyarrow.Table)

        # The checkpointed_ids block is sorted (see load_checkpoint).
        # We'll use binary search to filter out processed rows.
        # And we process a single chunk at a time, otherwise `to_numpy` below
        # will copy the data from shared memory to worker's heap memory.

        import concurrent.futures

        # Get all chunks of the checkpointed ID column.
        ckpt_chunks = checkpointed_ids[self.id_column].chunks
        # Convert the block's ID column to a numpy array for fast processing.
        block_ids = block[self.id_column].to_numpy()

        def filter_with_ckpt_chunk(ckpt_chunk: pyarrow.ChunkedArray) -> numpy.ndarray:
            # Convert checkpoint chunk to numpy for fast search.
            # Use internal helper function for consistency and robustness (handles null-typed arrays, etc.)
            try:
                ckpt_ids = transform_pyarrow.to_numpy(ckpt_chunk, zero_copy_only=True)
            except (pa.ArrowInvalid, ValueError):
                ckpt_ids = transform_pyarrow.to_numpy(ckpt_chunk, zero_copy_only=False)
            # Start with a mask of all True (keep all rows).
            mask = numpy.ones(len(block_ids), dtype=bool)
            # Use binary search to find where block_ids would be in ckpt_ids.
            sorted_indices = numpy.searchsorted(ckpt_ids, block_ids)
            # Only consider indices that are within bounds.
            valid_indices = sorted_indices < len(ckpt_ids)
            # For valid indices, check for exact matches.
            potential_matches = sorted_indices[valid_indices]
            matched = ckpt_ids[potential_matches] == block_ids[valid_indices]
            # Mark matched IDs as False (filter out these rows).
            mask[valid_indices] = ~matched
            # Delete the chunk to free memory.
            del ckpt_chunk
            return mask

        # Use ThreadPoolExecutor to process each checkpoint chunk in parallel.
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.filter_num_threads or None
        ) as executor:
            masks = list(executor.map(filter_with_ckpt_chunk, ckpt_chunks))

        # Combine all masks using logical AND (row must not be in any checkpoint chunk).
        final_mask = numpy.logical_and.reduce(masks)
        # Convert the final mask to a PyArrow array and filter the block.
        mask_array = pyarrow.array(final_mask)
        filtered_block = block.filter(mask_array)
        return filtered_block


    def filter_rows_for_block_with_raoring_bitmap(
        self,
        block: Block,
        checkpointed_ids: Block,
    ) -> Block:
        """Filter block using a per-worker cached Roaring Bitmap (no chunking)."""
        if len(checkpointed_ids) == 0 or len(block) == 0:
            return block

        assert isinstance(block, pa.Table)
        assert isinstance(checkpointed_ids, pa.Table)

        # Use a stable key for caching (e.g., checkpoint path)
        # If you don't have a key, use "default" assuming static checkpoint
        cache_key = getattr(self, 'checkpoint_path_unwrapped', 'default')

        # Build or get cached bitmap (once per worker)
        bitmap = _get_or_build_roaring_bitmap(
            ckpt_table=checkpointed_ids,
            id_column=self.id_column,
            checkpoint_key=cache_key,
        )

        # Process block
        block_ids = block[self.id_column].to_numpy()
        # Vectorize as much as possible (pyroaring doesn't support batch contains)
        mask = np.fromiter(
            (x in bitmap for x in block_ids),
            dtype=bool,
            count=len(block_ids)
        )
        keep_mask = ~mask
        return block.filter(pa.array(keep_mask))

   

    def filter_rows_for_block_with_bloom_filter(
        self,
        block: Block,
        bloom: BloomFilterCheckpoint,
    ) -> Block:
        """Filter ``block`` using a pre-built :class:`BloomFilterCheckpoint`.

        The Bloom Filter is built once per pipeline execution by
        :func:`ray.data.checkpoint.bloom_filter.build_bloom_filter_remote`
        and broadcast to every map worker via the Ray object store. This
        method therefore performs only the per-block membership test —
        construction never happens on the worker hot path.

        Membership is tested in O(1) per id with zero false negatives; the
        false-positive rate is controlled by ``self.bloom_filter_error_rate``
        (chosen at filter construction time).
        """
        if len(block) == 0 or len(bloom) == 0:
            return block

        assert isinstance(block, pa.Table)
        assert isinstance(bloom, BloomFilterCheckpoint), (
            "filter_rows_for_block_with_bloom_filter expects a pre-built "
            f"BloomFilterCheckpoint, got {type(bloom).__name__}"
        )

        block_uint64_ids = _hash_id_column_to_uint64(block, self.id_column)
        in_checkpoint = bloom.contains_many(block_uint64_ids)
        keep_mask = ~in_checkpoint
        return block.filter(pa.array(keep_mask))


    def filter_block_by_redis_ckpt(
        self,
        block: Block,
    ) -> Block:
        """
        Filter out rows already in Redis Set (no chunking needed).
        """
        redis_client = redis.Redis(host=self.redis_checkpoint_host, port=self.redis_checkpoint_port,
                                       password=self.redis_checkpoint_password, decode_responses=True)
        is_exists = _is_redis_checkpoint_key_exists(self.redis_checkpoint_key,redis_client)
        if len(block) == 0 or is_exists == '0' :
            return block
        batch_size = self.redis_checkpoint_pipeline_batch_size
        exists_flags = []
        block_ids = block[self.id_column].to_numpy().tolist()

        def batch_check_roaring_bitmap(client, bitmap_key, values_to_check):
            with client.pipeline(transaction=False) as pipe:
                for value in values_to_check:
                    pipe.execute_command('R.GETBIT', bitmap_key, value)
                results = pipe.execute()

            return results
        for i in range(0, len(block_ids), batch_size):
            if self.redis_data_storage_as_roaring_bitmap:
                batch_results = batch_check_roaring_bitmap(redis_client,self.redis_checkpoint_key,block_ids[i:i + batch_size])
                exists_flags.extend(batch_results)
            else:
                pipe = redis_client.pipeline(transaction=False)
                for _id in block_ids[i:i + batch_size]:
                    pipe.execute_command('SISMEMBER',self.redis_checkpoint_key, _id)
                batch_results = pipe.execute()
                exists_flags.extend(batch_results)
        # Build keep mask: True = keep (not in checkpoint)
        keep_mask = ~numpy.array(exists_flags, dtype=bool)
        redis_client.close()
        return block.filter(pyarrow.array(keep_mask))

    def filter_rows_for_batch(
        self,
        batch: DataBatch,
        checkpointed_ids: Block,
    ) -> DataBatch:
        """For the given batch, filter out rows that have already
        been checkpointed, and return the resulting batch.

        Note that this method calls `filter_rows_for_block()` under the hood,
        so it is preferred to call that method directly if you already have a block.
        """
        arrow_block = BlockAccessor.batch_to_block(batch)
        filtered_block = self.filter_rows_for_block(arrow_block, checkpointed_ids)
        filtered_batch = BlockAccessor.for_block(filtered_block).to_batch_format(None)
        return filtered_batch
