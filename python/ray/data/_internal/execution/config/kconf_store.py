"""Kconf-based execution configuration store implementation."""

import json
import logging
import threading
from abc import abstractmethod
from typing import Callable, Dict, Generic, Optional, TypeVar

from kconf.client import update_config, create_config, KConfValueType
from kconf.exception import KConfError
from kconf.get_config import get_string_config
from kconf.watcher import StringWatcher, add_watcher
from ray.data._internal.execution.config.models import ExecutionConfig
from ray.data._internal.execution.config.store import ExecutionConfigStore

logger = logging.getLogger(__name__)

# Define a generic type variable to represent config values of any type.
T = TypeVar("T")


class _GenericWatcher(StringWatcher, Generic[T]):
    """
    A generic base class for configuration watchers that supports any value type.
    It uses a generic parameter `T` to represent the specific type of the config value.
    """

    def __init__(self):
        """Initialize the generic watcher."""
        super().__init__()
        self._value_cache: Dict[str, T] = {}

    def get_value(self, key: str) -> Optional[T]:
        """
        Get the configuration value for a given key.

        Args:
            key: The configuration key.

        Returns:
            The cached configuration value, or None if it does not exist.
        """
        try:
            return self._value_cache.get(key)
        except Exception as e:
            logger.error(f"Error getting cached value for key {key}: {e}")
            return None

    def on_change(self, key: str, new_value: str) -> None:
        """
        Triggered when a configuration value changes.
        This method deserializes the string value to its specific type and
        then calls the `handle_change` method implemented by the subclass.

        Args:
            key: The key of the configuration that changed.
            new_value: The new configuration value as a string.
        """
        try:
            # Attempt to deserialize the string into the target type.
            parsed_value = self._parse_value(new_value)

            # Cache the parsed value.
            old_value = self._value_cache.get(key)
            self._value_cache[key] = parsed_value

            # Call the handler method implemented by the subclass.
            self.handle_change(key, parsed_value, old_value)

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON configuration for key {key}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error processing config change for key {key}: {e}")

    def on_remove(self, key: str) -> None:
        """
        Triggered when a configuration is removed.

        Args:
            key: The key that was removed.
        """
        try:
            # Get the value before removal, if it existed.
            old_value = self._value_cache.pop(key, None)

            # Call the handler method implemented by the subclass.
            self.handle_remove(key, old_value)

        except Exception as e:
            logger.error(f"Unexpected error processing config removal for key {key}: {e}")

    def _parse_value(self, value_str: str) -> T:
        """
        Parse a string configuration value into its specific type.

        Subclasses can override this method to implement custom parsing logic
        (e.g., for integers, floats, or custom objects). The default
        implementation attempts to parse the value as JSON, falling back
        to the raw string if parsing fails.

        Args:
            value_str: The string value of the configuration.

        Returns:
            The parsed value in its specific type.
        """
        try:
            return json.loads(value_str)
        except json.JSONDecodeError:
            # If not valid JSON, return the original string
            # Note: This might cause type issues for non-string types
            return value_str  # type: ignore

    @abstractmethod
    def handle_change(self, key: str, new_value: T, old_value: Optional[T] = None):
        """
        Abstract method to handle a configuration change. Must be implemented by subclasses.

        Args:
            key: The key of the configuration that changed.
            new_value: The new configuration value (parsed to its specific type).
            old_value: The old configuration value, if it existed.
        """
        raise NotImplementedError

    @abstractmethod
    def handle_remove(self, key: str, old_value: Optional[T] = None):
        """
        Abstract method to handle a configuration removal. Must be implemented by subclasses.

        Args:
            key: The key that was removed.
            old_value: The value of the configuration before it was removed, if it existed.
        """
        raise NotImplementedError


class _ExecutionConfigWatcher(_GenericWatcher[ExecutionConfig]):
    """
    Watches for changes in ExecutionConfig.
    """

    def __init__(self, update_callback: Callable[[ExecutionConfig], None]):
        """
        Initialize ExecutionConfigWatcher.

        Args:
            update_callback: Callback function to invoke when configuration changes.
        """
        super().__init__()
        self._update_callback = update_callback

    def _parse_value(self, value_str: str) -> ExecutionConfig:
        """
        Parse a JSON string into a ExecutionConfig instance.

        Args:
            value_str: The JSON string value of the configuration.

        Returns:
            The parsed ExecutionConfig instance.
        """
        return ExecutionConfig.from_json(value_str)

    def handle_change(
        self,
        key: str,
        new_value: ExecutionConfig,
        old_value: Optional[ExecutionConfig] = None,
    ) -> None:
        """
        Handle a configuration change by invoking the callback.

        Args:
            key: The configuration key that changed.
            new_value: The new configuration value.
            old_value: The previous configuration value.
        """
        try:
            logger.info(f"ExecutionConfig changed for key: {key}. Invoking callback.")
            self._update_callback(new_value)
        except Exception as e:
            logger.error(f"Error handling configuration change for key {key}: {e}")

    def handle_remove(self, key: str, old_value: Optional[ExecutionConfig] = None) -> None:
        """
        Handle a configuration removal by invoking the callback with an empty configuration.

        Args:
            key: The configuration key that was removed.
            old_value: The previous configuration value.
        """
        try:
            logger.info(f"ExecutionConfig removed for key: {key}. Resetting to default.")
            empty_config = ExecutionConfig()
            self._update_callback(empty_config)
        except Exception as e:
            logger.error(f"Error handling configuration removal for key {key}: {e}")


class KconfExecutionConfigStore(ExecutionConfigStore):
    """Kconf-based store for execution configuration."""

    def __init__(self, key: str, token: str):
        """
        Initialize Kconf-based config store.

        Note: The watcher is not added here. It will be added after init() is called
        to ensure the configuration exists before watching for changes.

        Args:
            key: The kconf key for configuration storage.
            token: Authentication token for kconf access.
        """
        self._lock = threading.Lock()
        self._key = key
        self._token = token
        self._config: Optional[ExecutionConfig] = None
        self._initialized = False
        self._watcher_added = False

    def get(self) -> Optional[ExecutionConfig]:
        """Get the current execution configuration."""
        with self._lock:
            return self._config

    def put(self, config: ExecutionConfig) -> None:
        """Store or update the execution configuration in kconf."""
        with self._lock:
            self._config = config
            value = config.to_json()
            update_config(self._key, self._token, value)
            logger.debug(f"Updated configuration in kconf for key: {self._key}")

    def init(self, config: ExecutionConfig) -> bool:
        """Initialize the configuration if it doesn't exist.

        This method also adds the watcher after ensuring the configuration exists.

        Args:
            config: The initial configuration to use if none exists.

        Returns:
            True if created, False if already exists.
        """
        should_add_watcher = False

        with self._lock:
            if self._initialized:
                return False

            try:
                existing_value = self._try_get_config()
                if existing_value is not None:
                    self._config = ExecutionConfig.from_json(existing_value)
                else:
                    create_config(
                        self._key,
                        self._token,
                        KConfValueType.STRING,
                        "execution configuration",
                    )
                    self._config = config
                    value = config.to_json()
                    update_config(self._key, self._token, value)

                self._initialized = True

                # Mark that we need to add watcher, but do it outside the lock
                # to avoid deadlock if add_watcher triggers a synchronous callback
                if not self._watcher_added:
                    should_add_watcher = True

            except KConfError as e:
                logger.error(f"Failed to initialize kconf configuration: {e}")
                return False

        # Add watcher outside the lock to prevent deadlock
        if should_add_watcher:
            try:
                self._add_watcher()
                self._watcher_added = True
            except Exception as e:
                logger.warning(
                    f"Failed to add watcher for key {self._key}: {e}"
                )

        return True

    def _add_watcher(self) -> None:
        """
        Initialize and register a watcher to monitor configuration changes.

        This should only be called after init() has been called to ensure
        the configuration exists in kconf.
        """
        watcher = _ExecutionConfigWatcher(update_callback=self._on_config_updated)
        add_watcher(self._key, watcher)
        logger.info(f"Started watching for ExecutionConfig updates at '{self._key}'.")

    def _on_config_updated(self, new_config: ExecutionConfig) -> None:
        """
        Internal thread-safe method to update the configuration.
        This method is called by the watcher via a callback.
        """
        with self._lock:
            try:
                self._config = new_config
                logger.debug(f"Configuration updated via watcher for key: {self._key}")
            except Exception as e:
                logger.error(f"Error updating configuration in callback for key {self._key}: {e}")

    def _try_get_config(self) -> Optional[str]:
        """Try to get configuration from kconf.

        Returns:
            The configuration value as a string, or None if it doesn't exist.
        """
        try:
            return get_string_config(self._key)
        except KConfError as e:
            logger.warning(f"Config not found for key {self._key}: {e}")
            return None