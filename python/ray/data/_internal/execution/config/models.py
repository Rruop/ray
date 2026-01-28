"""Operator configuration for dynamic parallelism control.

This module defines configuration data structures for Ray Data's dynamic
operator parallelism feature. These configurations can be updated at runtime
to adjust operator behavior during execution.
"""

import json
from dataclasses import asdict, dataclass, field
from typing import Any, ClassVar, Dict, Optional


@dataclass
class OperatorConfig:
    """Base class for operator configuration.

    Each operator type has its own configuration subclass with specific
    parameters for controlling parallelism and resource usage.

    Attributes:
        id: Unique identifier for the operator.
        name: Human-readable name for the operator.
    """

    # Subclasses must override this class variable
    CONFIG_TYPE: ClassVar[str] = ""

    id: str
    name: str

    def to_dict(self) -> Dict[str, Any]:
        """Serialize the configuration to a dictionary.

        Returns:
            Dictionary representation with 'type' field for deserialization.
        """
        data = asdict(self)
        data["type"] = self.CONFIG_TYPE
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "OperatorConfig":
        """Deserialize a dictionary to the appropriate OperatorConfig subclass.

        Args:
            data: Dictionary with 'type' field indicating the config type.

        Returns:
            OperatorConfig subclass instance.

        Raises:
            ValueError: If data is empty or type is unknown.
        """
        if not data:
            raise ValueError("Cannot deserialize empty data")

        data_copy = data.copy()
        config_type = data_copy.pop("type", None)

        if config_type is None:
            raise ValueError("Missing 'type' field in operator config data")

        # Direct dispatch - simple and clear for a small number of types
        if config_type == "actor_pool":
            return ActorPoolOperatorConfig(**data_copy)
        elif config_type == "task_pool":
            return TaskPoolOperatorConfig(**data_copy)
        else:
            raise ValueError(
                f"Unknown operator config type: '{config_type}'. "
                "Supported types: 'actor_pool', 'task_pool'"
            )


@dataclass
class ActorPoolOperatorConfig(OperatorConfig):
    """Configuration for actor pool operators.

    Controls the size and scaling bounds of an actor pool used for
    executing map operations.

    Attributes:
        min_size: Minimum number of actors in the pool.
        max_size: Maximum number of actors in the pool.
        size: Current/target number of actors.
    """

    CONFIG_TYPE: ClassVar[str] = "actor_pool"

    min_size: int
    max_size: int
    size: int


@dataclass
class TaskPoolOperatorConfig(OperatorConfig):
    """Configuration for task pool operators.

    Controls the concurrency of task-based map operations.

    Attributes:
        max_concurrency: Maximum number of concurrent tasks, or None for unlimited.
    """

    CONFIG_TYPE: ClassVar[str] = "task_pool"

    max_concurrency: Optional[int] = None


@dataclass
class ExecutionConfig:
    """Configuration for all operators in a Ray Data execution.

    This class aggregates operator-level configurations and provides
    serialization for storage in GCS. It enables dynamic adjustment of
    operator parallelism during job execution.

    Attributes:
        job_id: The job ID or submission ID associated with this configuration.
        operators: Mapping from operator ID to its configuration.

    Example:
        >>> config = ExecutionConfig(job_id="my_job_123")
        >>> config.add_operator(ActorPoolOperatorConfig(
        ...     id="map_op_1", name="MapBatches", min_size=1, max_size=10, size=5
        ... ))
        >>> json_str = config.to_json()
    """

    job_id: Optional[str] = None
    operators: Dict[str, OperatorConfig] = field(default_factory=dict)

    def to_json(self) -> str:
        """Serialize to JSON string for GCS storage."""
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str: str) -> "ExecutionConfig":
        """Deserialize from JSON string.

        Raises:
            json.JSONDecodeError: If JSON is invalid.
            ValueError: If config format is invalid.
        """
        return cls.from_dict(json.loads(json_str))

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary."""
        result = {
            "operators": {
                op_id: config.to_dict()
                for op_id, config in self.operators.items()
            }
        }
        if self.job_id is not None:
            result["job_id"] = self.job_id
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ExecutionConfig":
        """Deserialize from dictionary."""
        if not data:
            return cls()

        operators = {
            op_id: OperatorConfig.from_dict(op_data)
            for op_id, op_data in data.get("operators", {}).items()
        }
        return cls(
            job_id=data.get("job_id"),
            operators=operators,
        )

    def add_operator(self, config: OperatorConfig) -> None:
        """Add or update an operator configuration.

        The operator ID is taken from the config's id field.
        """
        self.operators[config.id] = config

    def get_operator_config(self, operator_id: str) -> Optional[OperatorConfig]:
        """Get configuration for a specific operator, or None if not found."""
        return self.operators.get(operator_id)

    def get_all_operator_configs(self) -> Dict[str, OperatorConfig]:
        """Get a copy of all operator configurations."""
        return self.operators.copy()

    def remove_operator(self, operator_id: str) -> bool:
        """Remove an operator configuration. Returns True if removed."""
        if operator_id in self.operators:
            del self.operators[operator_id]
            return True
        return False
