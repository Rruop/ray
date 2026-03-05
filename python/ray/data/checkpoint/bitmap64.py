from pyroaring import BitMap
from typing import Iterator, List, Optional
import pickle


class RoaringBitmap64:
    """
    64-bit Roaring Bitmap based on pyroaring.BitMap
    Principle: split 64-bit integer into high 32 bits (shard key) + low 32 bits (stored in BitMap)
    """

    def __init__(self, values=None):
        self._shards: dict[int, BitMap] = {}  # high32 -> BitMap(low32)
        if values is not None:
            self.add_many(values)

    # ──────────────────────────────────────────
    # Internal utilities
    # ──────────────────────────────────────────

    @staticmethod
    def _split(val: int):
        """Split a 64-bit integer into (high 32 bits, low 32 bits)"""
        if val < 0:
            raise ValueError(f"Negative numbers are not supported: {val}")
        if val > 0xFFFFFFFFFFFFFFFF:
            raise ValueError(f"Value exceeds 64-bit range: {val}")
        return val >> 32, val & 0xFFFFFFFF

    def _get_shard(self, high: int) -> Optional[BitMap]:
        return self._shards.get(high)

    def _get_or_create_shard(self, high: int) -> BitMap:
        if high not in self._shards:
            self._shards[high] = BitMap()
        return self._shards[high]

    # ──────────────────────────────────────────
    # Add / Remove
    # ──────────────────────────────────────────

    def add(self, val: int):
        """Add a single value"""
        high, low = self._split(val)
        self._get_or_create_shard(high).add(low)

    def add_many(self, vals):
        """Bulk add (more efficient than calling add() repeatedly)"""
        # Group by high 32 bits first
        groups: dict[int, list] = {}
        for val in vals:
            high, low = self._split(val)
            groups.setdefault(high, []).append(low)

        for high, lows in groups.items():
            shard = self._get_or_create_shard(high)
            shard.update(lows)

    def add_range(self, start: int, stop: int):
        """Add a range [start, stop)"""
        # Process segment by segment according to shards
        SHARD_SIZE = 1 << 32
        cur = start
        while cur < stop:
            high = cur >> 32
            shard_end = (high + 1) * SHARD_SIZE  # end position of current shard
            seg_end = min(stop, shard_end)

            low_start = cur & 0xFFFFFFFF
            low_end = seg_end & 0xFFFFFFFF if seg_end % SHARD_SIZE != 0 else 0x100000000

            shard = self._get_or_create_shard(high)
            shard.add_range(low_start, low_end)

            cur = seg_end

    def discard(self, val: int):
        """Remove a single value (no error if not present)"""
        high, low = self._split(val)
        shard = self._get_shard(high)
        if shard is not None:
            shard.discard(low)
            if len(shard) == 0:
                del self._shards[high]

    def remove(self, val: int):
        """Remove a single value (raises KeyError if not present)"""
        if val not in self:
            raise KeyError(val)
        self.discard(val)

    def clear(self):
        """Clear all elements"""
        self._shards.clear()

    # ──────────────────────────────────────────
    # Query
    # ──────────────────────────────────────────

    def __contains__(self, val: int) -> bool:
        high, low = self._split(val)
        shard = self._get_shard(high)
        return shard is not None and low in shard

    def __len__(self) -> int:
        return sum(len(s) for s in self._shards.values())

    def __bool__(self) -> bool:
        return len(self) > 0

    def cardinality(self) -> int:
        """Total number of elements"""
        return len(self)

    def is_empty(self) -> bool:
        return len(self) == 0

    def minimum(self) -> int:
        """Minimum value"""
        if not self._shards:
            raise ValueError("Bitmap is empty")
        min_high = min(self._shards.keys())
        return (min_high << 32) | self._shards[min_high].min()

    def maximum(self) -> int:
        """Maximum value"""
        if not self._shards:
            raise ValueError("Bitmap is empty")
        max_high = max(self._shards.keys())
        return (max_high << 32) | self._shards[max_high].max()

    # ──────────────────────────────────────────
    # Iteration
    # ──────────────────────────────────────────

    def __iter__(self) -> Iterator[int]:
        """Iterate in ascending order"""
        for high in sorted(self._shards.keys()):
            base = high << 32
            for low in self._shards[high]:
                yield base | low

    def to_list(self) -> List[int]:
        return list(self)

    # ──────────────────────────────────────────
    # Set operations
    # ──────────────────────────────────────────

    def _binary_op(self, other: "RoaringBitmap64", op: str) -> "RoaringBitmap64":
        result = RoaringBitmap64()
        all_keys = set(self._shards) | set(other._shards)

        for high in all_keys:
            a = self._shards.get(high, BitMap())
            b = other._shards.get(high, BitMap())

            if op == "and":
                r = a & b
            elif op == "or":
                r = a | b
            elif op == "xor":
                r = a ^ b
            elif op == "andnot":
                r = a - b
            else:
                raise ValueError(f"Unknown operation: {op}")

            if len(r) > 0:
                result._shards[high] = r

        return result

    def __and__(self, other: "RoaringBitmap64") -> "RoaringBitmap64":
        """Intersection"""
        return self._binary_op(other, "and")

    def __or__(self, other: "RoaringBitmap64") -> "RoaringBitmap64":
        """Union"""
        return self._binary_op(other, "or")

    def __xor__(self, other: "RoaringBitmap64") -> "RoaringBitmap64":
        """Symmetric difference"""
        return self._binary_op(other, "xor")

    def __sub__(self, other: "RoaringBitmap64") -> "RoaringBitmap64":
        """Difference (self - other)"""
        return self._binary_op(other, "andnot")

    def issubset(self, other: "RoaringBitmap64") -> bool:
        """self ⊆ other"""
        return (self & other) == self

    def issuperset(self, other: "RoaringBitmap64") -> bool:
        return other.issubset(self)

    def __eq__(self, other) -> bool:
        if not isinstance(other, RoaringBitmap64):
            return False
        if set(self._shards.keys()) != set(other._shards.keys()):
            return False
        return all(self._shards[k] == other._shards[k] for k in self._shards)

    def jaccard_index(self, other: "RoaringBitmap64") -> float:
        """Jaccard similarity = |A∩B| / |A∪B|"""
        intersection = len(self & other)
        union = len(self | other)
        return intersection / union if union > 0 else 1.0

    # ──────────────────────────────────────────
    # Statistics
    # ──────────────────────────────────────────

    def rank(self, val: int) -> int:
        """Number of elements less than or equal to val"""
        high, low = self._split(val)
        count = 0
        for h in sorted(self._shards.keys()):
            if h < high:
                count += len(self._shards[h])
            elif h == high:
                count += self._shards[h].rank(low)
                break
        return count

    def select(self, n: int) -> int:
        """Return the n-th smallest element (0-indexed)"""
        remaining = n
        for high in sorted(self._shards.keys()):
            shard = self._shards[high]
            shard_len = len(shard)
            if remaining < shard_len:
                return (high << 32) | shard.select(remaining)
            remaining -= shard_len
        raise IndexError(f"Index {n} out of range, total elements: {len(self)}")

    def shard_stats(self) -> dict:
        """Statistics for each shard"""
        return {
            "shard_count": len(self._shards),
            "total_elements": len(self),
            "shards": {
                f"high={h}(0x{h:08X})": len(bm)
                for h, bm in sorted(self._shards.items())
            }
        }

    # ──────────────────────────────────────────
    # Serialization
    # ──────────────────────────────────────────

    def serialize(self) -> bytes:
        """Serialize to bytes"""
        data = {high: bm.serialize() for high, bm in self._shards.items()}
        return pickle.dumps(data)

    @classmethod
    def deserialize(cls, buf: bytes) -> "RoaringBitmap64":
        """Deserialize from bytes"""
        obj = cls()
        data = pickle.loads(buf)
        obj._shards = {high: BitMap.deserialize(bm_bytes) for high, bm_bytes in data.items()}
        return obj

    def __repr__(self):
        return f"RoaringBitmap64(cardinality={len(self)}, shards={len(self._shards)})"