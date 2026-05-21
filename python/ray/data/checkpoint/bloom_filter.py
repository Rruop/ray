"""Bloom Filter for checkpoint membership testing.

This module provides :class:`BloomFilterCheckpoint`, a space-efficient
probabilistic membership filter used by the checkpoint subsystem to decide
whether an input row has already been checkpointed, and
:func:`build_bloom_filter_remote`, a Ray remote task that builds the filter
on a worker so the result can be shared via the object store.

Why a Bloom Filter for checkpointing?
-------------------------------------
The default :py:meth:`~ray.data.checkpoint.checkpoint_filter.BatchBasedCheckpointFilter.filter_rows_for_block`
implementation relies on ``numpy.searchsorted`` and therefore requires the
checkpointed-id table to be globally sorted. Sorting the checkpoint dataset
triggers a full distributed shuffle, which can be slow or unstable on
resource-constrained or fixed-size clusters.

The Bloom Filter path:

* Has **zero false negatives** — every checkpointed row is guaranteed to be
  recognised, so no work is ever repeated.
* Has a configurable **false-positive rate** (default ``1e-6``). The only
  effect of a false positive is that a small number of unprocessed rows are
  incorrectly skipped (over-filtered). For most pipelines this is negligible.
* Requires no global sort of the checkpoint dataset.
* Has O(1) per-id membership cost and is built in O(N) time.
* Uses only ``numpy`` and the standard-library :mod:`hashlib` — no extra
  runtime dependencies.

Build-and-broadcast model
-------------------------
The filter is built exactly once per pipeline execution by
:func:`build_bloom_filter_remote`, a Ray remote task. The returned
:class:`BloomFilterCheckpoint` is placed in the Ray object store; every map
worker that needs it receives the same ``ObjectRef``. Two memory properties
make this efficient:

1. The bit array is backed by a read-only :class:`numpy.ndarray` of
   ``dtype=uint8``. When pickled with protocol 5 (Ray's default), the array
   participates in the out-of-band buffer protocol, so plasma stores the
   bytes once.
2. Because the array is immutable, every worker on a given node can read
   the bytes directly out of the plasma ``MAP_SHARED`` mmap without copying
   into the worker's heap. A single node holds one physical copy regardless
   of how many map workers are running on it.

Memory footprint (default 1e-6 FPR):

================  ==================
items             approx. memory
================  ==================
1e6  (1M)         ~3.6 MB
1e8  (100M)       ~360 MB
1e9  (1B)         ~3.6 GB
================  ==================
"""

import hashlib
import logging
import math
import struct
from typing import Optional, Union

import numpy as np
import pyarrow as pa

import ray
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)


_DEFAULT_ERROR_RATE = 1e-6


def _hash_id_column_to_uint64(table: pa.Table, id_column: str) -> np.ndarray:
    """Convert ``table[id_column]`` into a ``numpy.ndarray[uint64]``.

    Supported PyArrow types:

    * ``string`` / ``large_string`` → SHA-256 truncated to the low 8 bytes.
    * Any integer type → cast to ``uint64`` (zero-copy when possible).
    * ``uint64`` → returned as-is.

    The function iterates over chunks of a :class:`pyarrow.ChunkedArray`
    instead of calling :py:meth:`combine_chunks`, because for a ``string``
    column whose total bytes exceed 2 GB ``combine_chunks`` raises
    :class:`pyarrow.lib.ArrowCapacityError` (``string`` uses int32 offsets,
    limiting any single :class:`Array` to 2 GB of payload). A ``string``
    column whose individual chunks fit in 2 GB each — the typical case after
    Ray Data ``repartition(num_blocks=N)`` — is therefore still safe even
    when the table as a whole is much larger.
    """
    col = table[id_column]
    pa_type = col.type

    if isinstance(col, pa.ChunkedArray):
        chunks = col.chunks
    else:
        chunks = [col]

    if pa.types.is_string(pa_type) or pa.types.is_large_string(pa_type):
        total = sum(len(c) for c in chunks)
        result = np.empty(total, dtype=np.uint64)
        offset = 0
        for chunk in chunks:
            strings = chunk.to_pylist()
            for s in strings:
                if s is None:
                    raise ValueError(
                        "BloomFilterCheckpoint: NULL id values are not supported."
                    )
                digest = hashlib.sha256(s.encode("utf-8")).digest()
                result[offset] = struct.unpack("<Q", digest[:8])[0]
                offset += 1
        return result

    if pa.types.is_integer(pa_type):
        total = sum(len(c) for c in chunks)
        result = np.empty(total, dtype=np.uint64)
        offset = 0
        for chunk in chunks:
            np_chunk = chunk.to_numpy(zero_copy_only=False)
            np_chunk = np_chunk.astype(np.uint64, copy=False)
            result[offset : offset + np_chunk.size] = np_chunk
            offset += np_chunk.size
        return result

    raise TypeError(
        "BloomFilterCheckpoint: unsupported id_column type "
        f"{pa_type}. Expected string, large_string, or any integer type."
    )


@DeveloperAPI
class BloomFilterCheckpoint:
    """Bloom Filter over uint64-hashed checkpoint ids.

    A Bloom Filter is a space-efficient probabilistic data structure that
    supports two operations: insertion (during construction) and membership
    test (:py:meth:`contains_many`). It can return false positives but
    **never** false negatives.

    Sizing follows the classic formulas:

    .. code-block:: text

        m = -n * ln(p) / (ln 2)^2     # number of bits
        k = (m / n) * ln 2            # number of hash functions

    where ``n`` is the expected number of items and ``p`` is the target
    false-positive rate.

    Two hash functions are derived from a single SHA-256 digest using the
    standard double-hashing technique:

    .. code-block:: text

        h_i(x) = (h1(x) + i * h2(x)) mod m,    i = 0, 1, ..., k-1

    Storage and immutability
    ------------------------
    The bit array is held as a :class:`numpy.ndarray` of ``dtype=uint8``.
    Once construction is complete the array is marked
    ``flags.writeable = False`` so that consumers (workers reading the
    filter out of the Ray plasma store) can safely share the underlying
    mmap-backed memory without copying. Mutating the filter after
    construction is therefore forbidden.

    Args:
        uint64_ids: A ``numpy.ndarray[uint64]`` containing the ids to insert.
            May be empty.
        error_rate: Target false-positive rate. Must be in ``(0, 1)``.
            Defaults to ``1e-6``.
    """

    def __init__(
        self,
        uint64_ids: np.ndarray,
        error_rate: Optional[float] = None,
    ):
        if error_rate is None:
            error_rate = _DEFAULT_ERROR_RATE
        if not 0.0 < error_rate < 1.0:
            raise ValueError(
                f"error_rate must be in (0, 1), got {error_rate!r}"
            )

        uint64_ids = np.asarray(uint64_ids, dtype=np.uint64)

        n = max(int(uint64_ids.size), 1)

        self.error_rate: float = float(error_rate)
        self.capacity: int = int(uint64_ids.size)
        self.num_bits: int = max(
            8, int(math.ceil(-n * math.log(error_rate) / (math.log(2) ** 2)))
        )
        self.num_hashes: int = max(
            1, int(round(self.num_bits / n * math.log(2)))
        )

        byte_len = (self.num_bits + 7) // 8
        bits = np.zeros(byte_len, dtype=np.uint8)

        if uint64_ids.size > 0:
            self._set_bits(bits, uint64_ids)

        bits.flags.writeable = False
        self.bits: np.ndarray = bits

        logger.info(
            "[bloom_filter] Built BloomFilterCheckpoint: capacity=%d, "
            "bits=%d, hashes=%d, error_rate=%.2e, memory=%.1f MB",
            self.capacity,
            self.num_bits,
            self.num_hashes,
            self.error_rate,
            self.bits.nbytes / 1e6,
        )

    @staticmethod
    def _double_hash_many(values: np.ndarray) -> "tuple[np.ndarray, np.ndarray]":
        """Vectorised double-hashing for an array of uint64 values.

        Each input value is packed as 8 little-endian bytes, run through
        SHA-256, and the first 16 bytes of the digest are split into two
        little-endian uint64 hashes ``(h1, h2)``. SHA-256 is computed in
        Python because there is no batched stdlib API, but the surrounding
        byte/numpy plumbing is vectorised.
        """
        n = int(values.size)
        h1 = np.empty(n, dtype=np.uint64)
        h2 = np.empty(n, dtype=np.uint64)
        keys = values.astype("<u8", copy=False).tobytes()
        for i in range(n):
            digest = hashlib.sha256(keys[i * 8 : (i + 1) * 8]).digest()
            h1[i], h2[i] = struct.unpack("<QQ", digest[:16])
        return h1, h2

    def _set_bits(self, bits: np.ndarray, uint64_ids: np.ndarray) -> None:
        """Populate ``bits`` with the bloom-filter encoding of ``uint64_ids``.

        ``bits`` must be a writeable ``uint8`` numpy array of length
        ``ceil(num_bits / 8)``. Modifications are done via vectorised numpy
        masks: for each of the ``k`` hash functions we compute all bit
        positions at once and use :py:func:`numpy.bitwise_or.at` to OR the
        corresponding bytes, handling duplicates correctly.

        The hashes ``h1`` and ``h2`` are reduced modulo ``num_bits`` before
        the double-hashing combine so that ``h1m + j * h2m`` cannot overflow
        ``uint64`` for any realistic ``num_bits`` (at most ~2^40 for a 1 TB
        filter) and ``num_hashes`` (at most ~50). Doing the modulo on the
        full ``h1 + j * h2`` would wrap inside ``uint64`` and produce
        incorrect bit positions for large hash inputs.
        """
        h1, h2 = self._double_hash_many(uint64_ids)
        m = np.uint64(self.num_bits)
        h1m = h1 % m
        h2m = h2 % m
        for j in range(self.num_hashes):
            positions = (h1m + np.uint64(j) * h2m) % m
            byte_indices = (positions >> np.uint64(3)).astype(np.intp, copy=False)
            bit_masks = (
                np.uint8(1) << (positions & np.uint64(7)).astype(np.uint8, copy=False)
            )
            np.bitwise_or.at(bits, byte_indices, bit_masks)

    def contains_many(self, uint64_ids: np.ndarray) -> np.ndarray:
        """Vectorised membership test.

        Args:
            uint64_ids: ``numpy.ndarray[uint64]`` of ids to test.

        Returns:
            ``numpy.ndarray[bool]`` of the same length, where ``True`` means
            the id is (probably) in the filter and ``False`` means it is
            definitely not.
        """
        uint64_ids = np.asarray(uint64_ids, dtype=np.uint64)
        n = int(uint64_ids.size)
        if n == 0:
            return np.empty(0, dtype=bool)

        h1, h2 = self._double_hash_many(uint64_ids)
        m = np.uint64(self.num_bits)
        h1m = h1 % m
        h2m = h2 % m

        result = np.ones(n, dtype=bool)
        for j in range(self.num_hashes):
            positions = (h1m + np.uint64(j) * h2m) % m
            byte_indices = (positions >> np.uint64(3)).astype(np.intp, copy=False)
            bit_masks = (
                np.uint8(1) << (positions & np.uint64(7)).astype(np.uint8, copy=False)
            )
            present = (self.bits[byte_indices] & bit_masks) != 0
            result &= present
        return result

    def __contains__(self, value: Union[int, np.integer]) -> bool:
        arr = np.array([int(value)], dtype=np.uint64)
        return bool(self.contains_many(arr)[0])

    def __len__(self) -> int:
        return self.capacity

    def __repr__(self) -> str:
        return (
            f"BloomFilterCheckpoint(capacity={self.capacity}, "
            f"bits={self.num_bits}, hashes={self.num_hashes}, "
            f"error_rate={self.error_rate:.2e})"
        )

    @classmethod
    def from_pyarrow_table(
        cls,
        table: pa.Table,
        id_column: str,
        error_rate: Optional[float] = None,
    ) -> "BloomFilterCheckpoint":
        """Build a Bloom Filter from a PyArrow table's id column.

        The id column may be of type ``string``, ``large_string``, or any
        integer type; values are first hashed (string) or cast (integer) to
        ``uint64``.
        """
        uint64_ids = _hash_id_column_to_uint64(table, id_column)
        return cls(uint64_ids, error_rate=error_rate)


@ray.remote
def build_bloom_filter_remote(
    block,
    id_column: str,
    error_rate: float,
) -> BloomFilterCheckpoint:
    """Ray remote task that builds a :class:`BloomFilterCheckpoint` on a worker.

    The block is the materialised checkpoint id table (a :class:`pyarrow.Table`).
    The task runs on a single worker, hashes every id, populates the bit array,
    and returns the resulting filter. Ray automatically places the return value
    in the object store, so the caller receives an ``ObjectRef`` that can be
    broadcast to all downstream map workers via task kwargs.
    """
    if not isinstance(block, pa.Table):
        raise TypeError(
            "build_bloom_filter_remote expects a pyarrow.Table, "
            f"got {type(block).__name__}"
        )
    return BloomFilterCheckpoint.from_pyarrow_table(
        block, id_column, error_rate=error_rate
    )
