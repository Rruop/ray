"""Kafka datasink for data writes.

This module provides a Kafka datasink implementation for Ray Data.

Requires:
    - kafka-python: https://kafka-python.readthedocs.io/
"""
import json
import logging
import time
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    Optional,
    Tuple,
    Callable,
)


from ray.data.block import Block, BlockAccessor
from ray.data.datasource.datasink import WriteResult, Datasink

if TYPE_CHECKING:
    from kafka import KafkaProducer

from ray.data._internal.datasource.utils.kafka import KafkaAuthConfig, add_authentication_to_config
from ray.data._internal.datasource.utils.serializer import Serializer

logger = logging.getLogger(__name__)

class KafkaSinkResult:
    def __init__(self, num_messages: int, num_bytes: int, num_blocks: int, time_cost_ms: int):
        self.num_messages = num_messages
        self.num_bytes = num_bytes
        self.num_blocks = num_blocks
        self.time_cost_ms = time_cost_ms

    def __repr__(self):
        return (f"KafkaSinkResult(num_messages={self.num_messages}, "
                f"num_bytes={self.num_bytes}, num_blocks={self.num_blocks}), time_cost_ms={self.time_cost_ms})")


class KafkaDatasink(Datasink[int]):
    """A Ray Data Datasink that writes rows to a Kafka topic.

    Supports optional serialization via a Serializer (e.g., ProtobufSerializer)
    to convert row dicts to bytes before writing to Kafka.

    Note:
        You don't need @ray.remote decorator here. Ray Data automatically:
        - Partitions your dataset into blocks
        - Spawns parallel Ray tasks calling write() for each block
        - Passes ray_remote_args from write_datasink(..., ray_remote_args={...})

        The Datasink is an interface; the framework handles task distribution.

    Usage:
        >>> import ray
        >>> from ray.data import from_items
        >>> ds = from_items([
        ...     {"key": "k1", "value": {"a": 1}},
        ...     {"key": "k2", "value": {"a": 2}},
        ... ])
        >>> sink = KafkaDatasink(
        ...     topic="ray-output",
        ...     bootstrap_servers=["localhost:9092"],
        ...     key_fn=lambda row: str(row.get("key")).encode(),
        ... )
        >>> ds.write_datasink(sink)
    """

    def __init__(
        self,
        *,
        topic: str,
        bootstrap_servers: Iterable[str],
        key_fn: Optional[Callable[[Dict[str, Any]], Optional[bytes]]] = None,
        serializer: Optional[Serializer] = None,
        topic_schema_message_type: Optional[str] = None,
        headers_fn: Optional[
            Callable[[Dict[str, Any]], Optional[Iterable[Tuple[str, bytes]]]]
        ] = None,
        producer_config: Optional[Dict[str, Any]] = None,
        kafka_auth_config: Optional[Any] = None,
    ) -> None:
        """Configure the Kafka sink.

        Args:
            topic: Kafka topic to write to.
            bootstrap_servers: List of broker addresses (host:port).
            key_fn: Function to compute message key (bytes) from a row. If None, no key.
            serializer: Optional Serializer to convert row dicts to bytes (e.g.,
                Pandas2ProtobufSerializer). If None, rows are JSON-serialized as-is.
            topic_schema_message_type: Topic schema binding message type to load serializer
                from topic schema if serializer is None. Only used if serializer is None.
            headers_fn: Optional function producing Kafka headers from a row,
                e.g., [("source", b"ray")].
            producer_config: Additional kafka-python producer config (e.g., linger_ms,
                batch_size, acks, compression_type, etc.).
            kafka_auth_config: Optional Ray Data KafkaAuthConfig for auth settings.
        """
        self._topic = topic
        self._bootstrap_servers = (
            [bootstrap_servers] if isinstance(bootstrap_servers, str)
            else list(bootstrap_servers)
        )
        self._key_fn = key_fn
        if serializer is None and topic_schema_message_type is not None:
            # try to load from topic schema
            loaded_serializer = _load_serializer_from_topic_schema(topic,
                                                        self._bootstrap_servers,
                                                        topic_schema_message_type)
            if loaded_serializer is None:
                logger.error("Failed to load serializer from topic schema")
                raise ValueError("Serializer is None and failed to load from topic schema")
            serializer = loaded_serializer
        self._serializer = serializer
        self._headers_fn = headers_fn
        self._producer_config = dict(producer_config or {})
        self._kafka_auth_config = kafka_auth_config

    @property
    def supports_distributed_writes(self) -> bool:
        # Safe to write from multiple tasks in parallel; Kafka handles partitioning.
        return True

    def on_write_start(self, schema=None) -> None:
        logger.info(f"Kafka sink starting write to topic '{self._topic} with schema: {schema}'")
        return None

    def _build_producer(self) -> "KafkaProducer":
        config: Dict[str, Any] = {"bootstrap_servers": self._bootstrap_servers}
        # Disable snappy compression if not explicitly configured
        # (snappy requires python-snappy package which may not be installed)
        if "compression_type" not in config:
            config["compression_type"] = None
        # Merge auth into config if available
        if KafkaAuthConfig is not None and add_authentication_to_config is not None:
            add_authentication_to_config(config, self._kafka_auth_config)
        # User overrides
        config.update(self._producer_config)
        from kafka import KafkaProducer
        return KafkaProducer(**config)

    def write(self, blocks: Iterable[Block], _ctx) -> KafkaSinkResult:
        """Write dataset blocks to Kafka.

        This method is called inside a Ray task (automatically created by the
        framework). Each invocation handles one or more pandas-formatted blocks.

        If a serializer is configured, rows are serialized via serializer.serialize().
        Otherwise, rows are JSON-serialized as-is.

        Returns the total messages sent by this task.
        """
        producer = self._build_producer()
        total_sent = 0
        blocks_sent = 0
        total_bytes = 0
        start_time = time.perf_counter_ns()

        for block in blocks:
            ba = BlockAccessor.for_block(block)
            # Convert block to pandas DataFrame for row iteration
            # Rows are extracted as dicts from pandas itertuples()
            try:
                df = ba.to_pandas()
                rows_iter = (row._asdict() for row in
                             df.itertuples(index=False))
            except Exception:
                # Fallback: attempt to iterate block items directly
                rows_iter = ba.iter_rows()

            for row in rows_iter:
                key = self._key_fn(row) if self._key_fn else None

                # Serialize value: use serializer if provided, else JSON
                if self._serializer:
                    value = self._serializer.serialize(row)
                else:
                    value = json.dumps(row).encode("utf-8")

                total_bytes += len(value)
                headers = self._headers_fn(row) if self._headers_fn else None
                producer.send(self._topic, key=key, value=value,
                              headers=headers)
                total_sent += 1

            # Flush after each block to ensure delivery
            blocks_sent += 1
            producer.flush()

        cost_in_ms = (time.perf_counter_ns() - start_time) // 1_000_000
        result = KafkaSinkResult(total_sent, total_bytes, blocks_sent, cost_in_ms)
        logger.info(f"Sent stats from task {result}")
        return result

    def on_write_complete(self, write_result: WriteResult[KafkaSinkResult]) -> None:
        # Optionally log aggregated results; could commit transactions if used.

        total_num_msgs = sum(getattr(r, "num_messages", 0) for r in
                             getattr(write_result, "write_returns", []) or [])
        total_bytes = sum(getattr(r, "num_bytes", 0) for r in
                          getattr(write_result, "write_returns", []) or [])
        total_blocks = sum(getattr(r, "num_blocks", 0) for r in
                           getattr(write_result, "write_returns", []) or [])
        total_time_ms = sum(getattr(r, "time_cost_ms", 0) for r in
                            getattr(write_result, "write_returns", []) or [])

        logger.info(f"Kafka Sink write complete: "
                    f"total_messages={total_num_msgs}, "
                    f"total_bytes={total_bytes}, "
                    f"total_blocks={total_blocks}, "
                    f"total_time_ms={total_time_ms}, "
                    f"total_time_sec={total_time_ms / 1000:.2f}")

    def on_write_failed(self, error: Exception) -> None:
        logger.error(f"Kafka sink failed with error: {error}")


def _load_serializer_from_topic_schema(topic: str, bootstrap_servers: Iterable[str], message_type: str) -> Optional[Serializer]:
    # parse cluster from bootstrap servers
    # For simplicity, assume the first server's host indicates the cluster
    first_server = next(iter(bootstrap_servers), None)
    if first_server:
        cluster = first_server.split(".")[0]
        try:
            from ray.data._internal.datasource.utils.serializer import Pandas2ProtobufSerializer

            logger.info(f"Loading serializer for topic '{topic}' on cluster '{cluster}'")
            fixed_desc_path = f"viewfs://hadoop-lt-cluster/home/dp/data/proto_desc_v2/{topic}_{cluster}.desc"
            serializer = Pandas2ProtobufSerializer(
                descriptor_set_file=fixed_desc_path,
                message_type=message_type,
            )
            logger.info("Serializer created from topic schema")
            return serializer
        except Exception as e:
            logger.error(f"Failed to create serializer from topic schema: {e}")
    return None