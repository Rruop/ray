"""Serializer classes for converting row dicts to bytes."""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional


class Serializer(ABC):
    """Abstract base class for serializing data rows to bytes for Kafka."""

    @abstractmethod
    def serialize(self, row: Any) -> bytes:
        """Serialize a row to bytes.

        Args:
            row: A row in any format (dict, tuple, custom object, etc.).
                 The format depends on the specific serializer implementation.

        Returns:
            Serialized bytes ready for Kafka.
        """
        pass

    def get_name(self) -> str:
        """Return a human-readable name for this serializer."""
        name = type(self).__name__
        if name.endswith("Serializer"):
            name = name[: -len("Serializer")]
        return name


class Pandas2ProtobufSerializer(Serializer):
    """Serializer that converts pandas row dicts to protobuf messages.

    Specifically designed for pandas DataFrame rows (as dicts from itertuples()).
    Uses protobuf reflection API to dynamically set message fields from row dicts.

    Supports four ways to provide protobuf schema:
    1. Python generated protobuf class (proto_cls parameter)
    2. .proto file path (proto_file parameter)
    3. FileDescriptorSet file (descriptor_set_file parameter)
    """

    def __init__(
        self,
        proto_cls: Any = None,
        proto_file: Optional[str] = None,
        descriptor_set_file: Optional[str] = None,
        message_type: Optional[str] = None,
        field_mapping: Optional[Dict[str, str]] = None,
    ) -> None:
        """Initialize protobuf serializer.

        Args:
            proto_cls: Generated protobuf Message class (e.g., my_pb2.Event).
                Use this if you have Python protobuf code generated.
            proto_file: Path to .proto file to parse dynamically.
                Requires message_type to specify which message to use.
            descriptor_set_file: Path to FileDescriptorSet file (compiled .proto).
                Generate with: protoc --descriptor_set_out=FILE --include_imports *.proto
                Requires message_type to specify which message to use.
            message_type: Fully qualified message name (e.g., "mypackage.Event").
                Required when using proto_file, descriptor_set_file.
            field_mapping: Optional mapping from proto_field -> row_key. If None,
                fields are filled by matching keys in the row dict.

        Example 1 (Python generated code):
            >>> import my_pb2
            >>> serializer = Pandas2ProtobufSerializer(
            ...     proto_cls=my_pb2.Event,
            ...     field_mapping={"user_id": "uid", "event": "event_type"}
            ... )

        Example 2 (from .proto file):
            >>> serializer = Pandas2ProtobufSerializer(
            ...     proto_file="event.proto",
            ...     message_type="mypackage.Event"
            ... )

        Example 3 (from local descriptor file):
            >>> serializer = Pandas2ProtobufSerializer(
            ...     descriptor_set_file="./descriptors.pb",
            ...     message_type="mypackage.Event"
            ... )

        Example 4 (from HDFS descriptor file):
            >>> serializer = Pandas2ProtobufSerializer(
            ...     descriptor_set_file="viewfs://cluster_a/home/descriptors.pb",
            ...     message_type="mypackage.Event"
            ... )

        Example 5 (from S3 descriptor file):
        >>> serializer = Pandas2ProtobufSerializer(
        ...     descriptor_set_file="S3://bucket/descriptors.pb",
        ...     message_type="mypackage.Event"
        ... )
        """
        try:
            from google.protobuf.message import Message
        except ModuleNotFoundError as e:
            raise ImportError(
                "protobuf is required for Pandas2ProtobufSerializer. Install with: pip install protobuf"
            ) from e

        # Validate input: exactly one schema source must be provided
        schema_sources = [proto_cls, proto_file, descriptor_set_file]
        if sum(s is not None and s is not False for s in schema_sources) != 1:
            raise ValueError(
                "Exactly one of proto_cls, proto_file, descriptor_set_file must be provided"
            )

        # Store initialization parameters for serialization
        self._proto_cls = None
        self._proto_cls_param = proto_cls
        self._proto_file = proto_file
        self._descriptor_set_file = descriptor_set_file
        self._message_type = message_type

        if proto_cls is not None:
            # Use provided Python generated class
            if not isinstance(proto_cls, type) or not issubclass(proto_cls,
                                                                 Message):
                raise TypeError(
                    "proto_cls must be a protobuf Message subclass")
            self._proto_cls = proto_cls

        elif proto_file is not None:
            # Load from .proto file
            if not message_type:
                raise ValueError(
                    "message_type is required when using proto_file")
            self._proto_cls = self._load_from_proto_file(proto_file,
                                                         message_type)

        elif descriptor_set_file is not None:
            # Load from FileDescriptorSet
            if not message_type:
                raise ValueError(
                    "message_type is required when using descriptor_set_file")
            self._proto_cls = self._load_from_desc_file(
                descriptor_set_file, message_type)

        self._field_mapping = field_mapping or {}
        self._mapped_row_keys = set(self._field_mapping.values())

    def __getstate__(self):
        """Support pickling by storing initialization parameters instead of proto_cls."""
        return {
            'proto_cls_param': self._proto_cls_param,
            'proto_file': self._proto_file,
            'descriptor_set_file': self._descriptor_set_file,
            'message_type': self._message_type,
            'field_mapping': self._field_mapping,
        }

    def __setstate__(self, state):
        """Support unpickling by reconstructing proto_cls from parameters."""
        self._proto_cls_param = state['proto_cls_param']
        self._proto_file = state['proto_file']
        self._descriptor_set_file = state['descriptor_set_file']
        self._message_type = state['message_type']
        self._field_mapping = state['field_mapping']
        self._mapped_row_keys = set(self._field_mapping.values())

        # Reconstruct proto_cls
        try:
            from google.protobuf.message import Message

            if self._proto_cls_param is not None:
                self._proto_cls = self._proto_cls_param
            elif self._proto_file is not None:
                self._proto_cls = self._load_from_proto_file(self._proto_file,
                                                             self._message_type)
            elif self._descriptor_set_file is not None:
                self._proto_cls = self._load_from_desc_file(
                    self._descriptor_set_file, self._message_type)
        except Exception:
            # If deserialization fails, proto_cls will be recreated on first use
            self._proto_cls = None

    def _load_from_proto_file(self, proto_file: str, message_type: str) -> Any:
        """Load protobuf message class from .proto file dynamically.

        This requires protoc to be available or uses python-protobuf's parser
        if available (limited support).
        """
        try:
            from google.protobuf.compiler import plugin_pb2
            from google.protobuf.descriptor_pb2 import FileDescriptorSet
            from google.protobuf.message_factory import GetMessageClass
            from google.protobuf import descriptor_pool
            import subprocess
            import tempfile
            import os
        except ImportError as e:
            raise ImportError(
                "Failed to import required protobuf modules for dynamic loading"
            ) from e

        # Use protoc to compile .proto file to FileDescriptorSet
        with tempfile.NamedTemporaryFile(suffix=".pb", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            # Get directory containing .proto file for include path
            proto_dir = os.path.dirname(os.path.abspath(proto_file))

            # Run protoc to generate descriptor set
            result = subprocess.run(
                [
                    "protoc",
                    f"--descriptor_set_out={tmp_path}",
                    "--include_imports",
                    f"-I{proto_dir}",
                    proto_file,
                ],
                capture_output=True,
                text=True,
            )

            if result.returncode != 0:
                raise RuntimeError(
                    f"protoc failed: {result.stderr}\n"
                    f"Make sure 'protoc' is installed and in PATH"
                )

            # Load from generated descriptor set
            return self._load_from_desc_file(tmp_path, message_type)
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    def _load_from_desc_file(self, desc_file_path: str,
                        message_type: str) -> Any:
        """Load protobuf message class from descriptor file path

        Args:
            desc_file_path: Path to descriptor file (can be local or HDFS/S3 URI).
            message_type: Fully qualified message name (e.g., "mypackage.Event").

        """
        try:
            from pyarrow.fs import FileSystem
            from google.protobuf.descriptor_pb2 import FileDescriptorSet
            from google.protobuf.message_factory import GetMessageClass
            from google.protobuf import descriptor_pool
        except ImportError as e:
            raise ImportError(
                "pyarrow is required for filesystem access. Install with: pip install pyarrow\n"
                "protobuf is required for descriptor loading. Install with: pip install protobuf"
            ) from e

        try:
            fs, file_path = FileSystem.from_uri(desc_file_path)
            with fs.open_input_file(file_path) as f:
                descriptor_bytes = f.read()

            # Parse FileDescriptorSet
            file_descriptor_set = FileDescriptorSet()
            file_descriptor_set.ParseFromString(descriptor_bytes)

            # Create a descriptor pool and add all file descriptors
            pool = descriptor_pool.DescriptorPool()
            for file_descriptor_proto in file_descriptor_set.file:
                pool.Add(file_descriptor_proto)

            # Get the message descriptor
            try:
                message_descriptor = pool.FindMessageTypeByName(message_type)
            except KeyError:
                raise ValueError(
                    f"Message type '{message_type}' not found in descriptor file: {desc_file_path}. "
                    f"Make sure the fully qualified name is correct (e.g., 'mypackage.MyMessage')"
                )

            # Create message class from descriptor
            message_class = GetMessageClass(message_descriptor)
            return message_class

        except Exception as e:
            raise RuntimeError(
                f"Failed to load descriptor from path: {desc_file_path}\n"
                f"Error: {str(e)}"
            ) from e

    def serialize(self, row: Any) -> bytes:
        """Serialize a pandas row to protobuf bytes.

        Args:
            row: A pandas row represented as dict (from df.itertuples() + zip()).
                 Values may include pandas/numpy types (np.int64, pd.NA, pd.Timestamp, etc.).

        Returns:
            Serialized protobuf message bytes.
        """
        msg = self._proto_cls()
        descriptor = msg.DESCRIPTOR

        # Normalize pandas row dict to handle pandas-specific types
        normalized_row = self._normalize_pandas_row(row)

        # Apply explicit field mappings
        for proto_field, row_key in self._field_mapping.items():
            if row_key in normalized_row:
                value = normalized_row[row_key]
                field_desc = descriptor.fields_by_name.get(proto_field)
                if field_desc:
                    _set_proto_field(msg, proto_field, value, field_desc)

        # Auto-fill remaining fields by name matching
        for row_key, value in normalized_row.items():
            if row_key in self._mapped_row_keys:
                continue
            field_desc = descriptor.fields_by_name.get(row_key)
            if field_desc:
                _set_proto_field(msg, row_key, value, field_desc)

        return msg.SerializeToString()

    def _normalize_pandas_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize pandas row dict by converting pandas-specific types to Python primitives.

        Handles:
        - numpy types (np.int64, np.float64, etc.) → Python int/float
        - pd.NA, pd.NaT → None
        - pd.Timestamp → datetime or timestamp int
        - pd.Timedelta → timedelta or seconds int
        """
        import numpy as np
        try:
            import pandas as pd
            has_pandas = True
        except ImportError:
            has_pandas = False

        normalized = {}
        for key, value in row.items():
            # Skip if None
            if value is None:
                normalized[key] = None
                continue

            # Handle list/dict types early (before pd.isna check)
            if isinstance(value, (list, dict, tuple)):
                normalized[key] = value
                continue

            # Handle numpy arrays
            if isinstance(value, np.ndarray):
                # Convert numpy arrays to lists
                normalized[key] = value.tolist()
                continue

            # Handle pandas NA values (only for scalar values)
            if has_pandas:
                try:
                    if pd.isna(value):
                        normalized[key] = None
                        continue
                except (TypeError, ValueError):
                    # pd.isna() can fail on some types, skip and handle below
                    pass

            # Handle numpy types
            if isinstance(value, (np.integer, np.floating)):
                normalized[key] = value.item()
                continue

            if isinstance(value, np.bool_):
                normalized[key] = bool(value)
                continue

            # Handle pandas Timestamp
            if has_pandas and isinstance(value, pd.Timestamp):
                # Convert to Python datetime or timestamp (milliseconds)
                normalized[key] = value.to_pydatetime()
                continue

            # Handle pandas Timedelta
            if has_pandas and isinstance(value, pd.Timedelta):
                # Convert to total seconds
                normalized[key] = value.total_seconds()
                continue

            # Keep other types as-is
            normalized[key] = value

        return normalized


def _set_proto_field(
    msg: Any, field_name: str, value: Any, field_descriptor: Any
) -> None:
    """Helper to safely set a protobuf message field, handling type conversions.

    Supports all protobuf field types:
    - Scalar types: int32, int64, uint32, uint64, sint32, sint64, fixed32, fixed64,
                   sfixed32, sfixed64, float, double, bool, string, bytes
    - Complex types: enum, message (nested)
    - Container types: repeated, map

    Note: Assumes value is already normalized (numpy/pandas types converted to Python primitives).
    """
    if value is None:
        return

    # Handle map fields (special case of repeated)
    if field_descriptor.message_type and field_descriptor.message_type.GetOptions().map_entry:
        _set_map_field(msg, field_name, value, field_descriptor)
        return

    # Handle repeated/list fields (use is_repeated() instead of deprecated label)
    try:
        is_repeated = field_descriptor.label == field_descriptor.LABEL_REPEATED
    except AttributeError:
        # For newer protobuf versions, use is_repeated()
        is_repeated = hasattr(field_descriptor,
                              'is_repeated') and field_descriptor.is_repeated()

    if is_repeated:
        _set_repeated_field(msg, field_name, value, field_descriptor)
        return

    # Single field - check if it's a nested message type
    if field_descriptor.type == field_descriptor.TYPE_MESSAGE:
        # Handle nested message by setting fields directly
        if isinstance(value, dict):
            # Get the nested message field and populate it from dict
            nested_msg = getattr(msg, field_name)
            for key, val in value.items():
                nested_field_desc = field_descriptor.message_type.fields_by_name.get(
                    key)
                if nested_field_desc:
                    _set_proto_field(nested_msg, key, val, nested_field_desc)
        else:
            # If it's already a message instance, try to copy via serialization
            # This works across different message class instances
            try:
                from google.protobuf.message import Message
                if isinstance(value, Message):
                    nested_msg = getattr(msg, field_name)
                    nested_msg.ParseFromString(value.SerializeToString())
            except Exception:
                pass  # Skip if conversion fails
        return

    # Single scalar field
    try:
        setattr(msg, field_name, value)
    except (ValueError, TypeError, AttributeError):
        # Type mismatch; attempt conversion based on protobuf type
        _convert_and_set_field(msg, field_name, value, field_descriptor)


def _set_repeated_field(
    msg: Any, field_name: str, value: Any, field_descriptor: Any
) -> None:
    """Set a repeated field, converting element types as needed."""
    if not isinstance(value, (list, tuple)):
        value = [value]

    field = getattr(msg, field_name)

    for item in value:
        if item is None:
            continue

        # Check if this is a message type repeated field
        if field_descriptor.type == field_descriptor.TYPE_MESSAGE:
            # For message types, convert dict to message instance
            if isinstance(item, dict):
                # Create a new message instance from the message descriptor
                try:
                    from google.protobuf import message_factory
                    # Get the message class from descriptor
                    item_msg_cls = message_factory.GetPrototype(
                        field_descriptor.message_type)
                    item_msg = item_msg_cls()
                except Exception:
                    # Fallback: create empty message and try to populate
                    item_msg = field.add()

                # Populate it from the dict
                for key, val in item.items():
                    nested_field_desc = field_descriptor.message_type.fields_by_name.get(
                        key)
                    if nested_field_desc:
                        _set_proto_field(item_msg, key, val, nested_field_desc)
                field.append(item_msg)
            else:
                # Try to set item directly (already a message)
                try:
                    field.append(item)
                except (ValueError, TypeError):
                    pass
        else:
            # For scalar types, try to set item directly
            try:
                field.append(item)
            except (ValueError, TypeError):
                # Convert based on element type
                converted_item = _convert_value(item, field_descriptor)
                if converted_item is not None:
                    field.append(converted_item)


def _set_map_field(
    msg: Any, field_name: str, value: Any, field_descriptor: Any
) -> None:
    """Set a map field from dict or list/tuple of pairs, supports nested maps recursively."""
    if value is None:
        return

    entries = _extract_map_entries(value)
    if not entries:
        return

    map_entry_descriptor = field_descriptor.message_type
    if map_entry_descriptor is None:
        return

    field = getattr(msg, field_name)

    key_descriptor = map_entry_descriptor.fields_by_name.get("key")
    val_descriptor = map_entry_descriptor.fields_by_name.get("value")

    for k, v in entries:
        # Convert key according to key descriptor (if present)
        try:
            converted_key = _convert_value(k, key_descriptor) if key_descriptor else k
        except Exception:
            continue

        # If the value type is a message, handle nested message / nested maps
        if val_descriptor and val_descriptor.type == val_descriptor.TYPE_MESSAGE:
            # If v is dict/list: populate fields on the nested message value
            if isinstance(v, (dict, list, tuple)):
                try:
                    nested_msg = field[converted_key]
                except Exception:
                    nested_msg = None

                if nested_msg is None:
                    try:
                        from google.protobuf.message_factory import GetPrototype
                        msg_cls = GetPrototype(val_descriptor.message_type)
                        nested_msg = msg_cls()
                        field[converted_key] = nested_msg
                    except Exception:
                        continue

                for sub_key, sub_val in _extract_map_entries(v) if isinstance(v, (list, tuple)) and all(isinstance(i, (list, tuple, dict)) for i in v) else (v.items() if isinstance(v, dict) else []):
                    sub_field_desc = val_descriptor.message_type.fields_by_name.get(sub_key)
                    if sub_field_desc is None:
                        try:
                            _set_proto_field(nested_msg, sub_key, sub_val, sub_field_desc)
                        except Exception:
                            continue
                    else:
                        try:
                            is_sub_map = (
                                sub_field_desc.message_type is not None
                                and getattr(sub_field_desc.message_type.GetOptions(), "map_entry", False)
                            )
                        except Exception:
                            is_sub_map = False

                        if is_sub_map:
                            _set_map_field(nested_msg, sub_key, sub_val, sub_field_desc)
                        else:
                            _set_proto_field(nested_msg, sub_key, sub_val, sub_field_desc)

            elif v is not None:
                converted_msg = _convert_message_value(v, val_descriptor)
                if converted_msg is not None:
                    try:
                        field[converted_key] = converted_msg
                    except Exception:
                        try:
                            field_val = field[converted_key]
                            field_val.ParseFromString(converted_msg.SerializeToString())
                        except Exception:
                            pass
                else:
                    converted_val = _convert_value(v, val_descriptor) if val_descriptor else v
                    try:
                        field[converted_key] = converted_val
                    except Exception:
                        continue
        else:
            try:
                converted_val = _convert_value(v, val_descriptor) if val_descriptor else v
                field[converted_key] = converted_val
            except Exception:
                try:
                    converted_val = _convert_value(v, val_descriptor) if val_descriptor else v
                    field[converted_key] = converted_val
                except Exception:
                    continue

def _extract_map_entries(value: Any):
    entries = []
    if value is None:
        return entries

    if isinstance(value, dict):
        entries.extend(list(value.items()))
        return entries

    if isinstance(value, (list, tuple)):
        for item in value:
            if isinstance(item, (list, tuple)) and len(item) == 2:
                entries.append((item[0], item[1]))
            elif isinstance(item, dict):
                if "key" in item and "value" in item:
                    entries.append((item["key"], item["value"]))
                elif len(item) == 1:
                    k = next(iter(item.keys()))
                    entries.append((k, item[k]))
                else:
                    continue
            else:
                continue
    return entries


def _convert_and_set_field(
    msg: Any, field_name: str, value: Any, field_descriptor: Any
) -> None:
    """Convert value based on field type and set it."""
    converted = _convert_value(value, field_descriptor)
    if converted is not None:
        try:
            setattr(msg, field_name, converted)
        except (ValueError, TypeError):
            pass  # Skip if conversion still fails


def _convert_value(value: Any, field_descriptor: Any) -> Any:
    """Convert a value to the target protobuf field type."""
    if value is None:
        return None

    field_type = field_descriptor.type

    # Scalar types: string
    if field_type == field_descriptor.TYPE_STRING:
        return str(value)

    # Scalar types: bytes
    elif field_type == field_descriptor.TYPE_BYTES:
        if isinstance(value, bytes):
            return value
        elif isinstance(value, str):
            return value.encode("utf-8")
        else:
            return str(value).encode("utf-8")

    # Scalar types: all integer variants
    elif field_type in (
            field_descriptor.TYPE_INT32,
            field_descriptor.TYPE_INT64,
            field_descriptor.TYPE_UINT32,
            field_descriptor.TYPE_UINT64,
            field_descriptor.TYPE_SINT32,
            field_descriptor.TYPE_SINT64,
            field_descriptor.TYPE_FIXED32,
            field_descriptor.TYPE_FIXED64,
            field_descriptor.TYPE_SFIXED32,
            field_descriptor.TYPE_SFIXED64,
    ):
        return int(value)

    # Scalar types: floating point
    elif field_type in (
            field_descriptor.TYPE_FLOAT,
            field_descriptor.TYPE_DOUBLE,
    ):
        return float(value)

    # Scalar types: boolean
    elif field_type == field_descriptor.TYPE_BOOL:
        if isinstance(value, bool):
            return value
        elif isinstance(value, str):
            return value.lower() in ("true", "1", "yes", "on")
        else:
            return bool(value)

    # Enum type: convert to enum value
    elif field_type == field_descriptor.TYPE_ENUM:
        return _convert_enum_value(value, field_descriptor)

    # Message type: handle nested message
    elif field_type == field_descriptor.TYPE_MESSAGE:
        return _convert_message_value(value, field_descriptor)

    # Fallback
    return value


def _convert_enum_value(value: Any, field_descriptor: Any) -> int:
    """Convert value to enum integer value."""
    if isinstance(value, int):
        return value

    enum_type = field_descriptor.enum_type
    if enum_type is None:
        return int(value)

    # Try to match by name
    if isinstance(value, str):
        enum_value = enum_type.values_by_name.get(value.upper())
        if enum_value:
            return enum_value.number
        # Try case-insensitive
        for name, ev in enum_type.values_by_name.items():
            if name.lower() == value.lower():
                return ev.number

    # Default to int conversion
    return int(value)


def _convert_message_value(value: Any, field_descriptor: Any) -> Any:
    """Convert value to nested message type.

    If value is already a message instance, return it.
    If value is a dict, create a new message and populate from dict.
    Otherwise, return value as-is.
    """
    from google.protobuf.message import Message

    message_type = field_descriptor.message_type
    if message_type is None:
        return value

    # If already a message instance, return as-is
    if isinstance(value, Message):
        return value

    # If dict, create new message and populate from dict
    if isinstance(value, dict):
        # Dynamically create message from descriptor
        msg_cls = _message_class_from_descriptor(message_type)
        if msg_cls:
            msg_instance = msg_cls()
            for key, val in value.items():
                field_desc = message_type.fields_by_name.get(key)
                if field_desc:
                    _set_proto_field(msg_instance, key, val, field_desc)
            return msg_instance

    return value


def _message_class_from_descriptor(descriptor: Any) -> Any:
    """Get the message class from a message descriptor.

    This is a helper to instantiate messages from their descriptors when doing
    nested message conversion from dicts.
    """
    try:
        from google.protobuf.message_factory import GetPrototype
        return GetPrototype(descriptor)
    except (ImportError, AttributeError):
        # Fallback: try to import from descriptor's Python module
        try:
            import importlib
            module_name = descriptor.file.name.replace("/", ".").replace(
                ".proto", "_pb2")
            module = importlib.import_module(module_name)
            class_name = descriptor.name
            return getattr(module, class_name, None)
        except Exception:
            return None
