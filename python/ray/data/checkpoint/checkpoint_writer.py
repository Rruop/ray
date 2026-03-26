import logging
import os
import uuid

import redis
from abc import abstractmethod

from pyarrow import parquet as pq


from ray.data._internal.util import call_with_retry
from ray.data.block import BlockAccessor
from ray.data.checkpoint import CheckpointBackend, CheckpointConfig
from ray.data.context import DataContext
from ray.data.datasource.path_util import _unwrap_protocol

logger = logging.getLogger(__name__)


class CheckpointWriter:
    """Abstract class which defines the interface for writing row-level
    checkpoints based on varying backends.

    Subclasses must implement `.write_block_checkpoint()`."""

    def __init__(self, config: CheckpointConfig):
        self.ckpt_config = config
        self.checkpoint_path_unwrapped = _unwrap_protocol(
            self.ckpt_config.checkpoint_path
        )
        self.id_col = self.ckpt_config.id_column
        self.filesystem = self.ckpt_config.filesystem
        self.write_num_threads = self.ckpt_config.write_num_threads
        self.redis_checkpoint_key = self.ckpt_config.redis_checkpoint_key
        self.redis_checkpoint_host = self.ckpt_config.redis_checkpoint_host
        self.redis_checkpoint_port = self.ckpt_config.redis_checkpoint_port
        self.redis_checkpoint_password = self.ckpt_config.redis_checkpoint_password
        self.redis_data_storage_as_roaring_bitmap = self.ckpt_config.redis_data_storage_as_roaring_bitmap
        self.write_checkpoint_retry_number = self.ckpt_config.write_checkpoint_retry_number

    @abstractmethod
    def write_block_checkpoint(self, block: BlockAccessor):
        """Write a checkpoint for all rows in a single block to the checkpoint
        output directory given by `self.checkpoint_path`.

        Subclasses of `CheckpointWriter` must implement this method."""
        ...

    @staticmethod
    def create(config: CheckpointConfig) -> "CheckpointWriter":
        """Factory method to create a `CheckpointWriter` based on the
        provided `CheckpointConfig`."""
        backend = config.backend

        if backend in [
            CheckpointBackend.CLOUD_OBJECT_STORAGE,
            CheckpointBackend.FILE_STORAGE,
        ]:
            return BatchBasedCheckpointWriter(config)
        raise NotImplementedError(f"Backend {backend} not implemented")


class BatchBasedCheckpointWriter(CheckpointWriter):
    """CheckpointWriter for batch-based backends."""

    def __init__(self, config: CheckpointConfig):
        super().__init__(config)

        self.filesystem.create_dir(self.checkpoint_path_unwrapped, recursive=True)

    def write_block_checkpoint(self, block: BlockAccessor):
        """Write a checkpoint for all rows in a single block to the checkpoint
        output directory given by `self.checkpoint_path`.

        Subclasses of `CheckpointWriter` must implement this method."""
        if block.num_rows() == 0:
            return

        file_name = f"{uuid.uuid4()}.parquet"
        ckpt_file_path = os.path.join(self.checkpoint_path_unwrapped, file_name)

        checkpoint_ids_block = block.select(columns=[self.id_col])
        # `pyarrow.parquet.write_parquet` requires a PyArrow table. It errors if the block is
        # a pandas DataFrame.
        checkpoint_ids_table = BlockAccessor.for_block(checkpoint_ids_block).to_arrow()

        redis_checkpoint_key = self.redis_checkpoint_key
        
        def _write():
            if not redis_checkpoint_key:
                pq.write_table(
                    checkpoint_ids_table,
                    ckpt_file_path,
                    filesystem=self.filesystem,
                )
            else:
                logger.info(f"write checkpoint file: {file_name}")
                ids = checkpoint_ids_block[self.id_col].to_numpy().tolist()
                redis_client = redis.Redis(host=self.redis_checkpoint_host, port=self.redis_checkpoint_port, password=self.redis_checkpoint_password, decode_responses=True)
                if self.redis_data_storage_as_roaring_bitmap:
                    redis_client.execute_command('R.APPENDINTARRAY', redis_checkpoint_key, *ids)
                else:
                    redis_client.sadd(redis_checkpoint_key, *ids)
                redis_client.close()     
        try:
            return call_with_retry(
                _write,
                description=f"Write checkpoint file: {file_name}",
                match=DataContext.get_current().retried_io_errors,
                max_attempts = self.write_checkpoint_retry_number,
            )
        except Exception:
            logger.exception(f"Checkpoint write failed: {file_name}")
            raise
