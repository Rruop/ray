import fnmatch
import logging
import os
import threading
from collections import defaultdict
from typing import Callable, List, Optional

from opentelemetry import metrics
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.metrics import Observation
from opentelemetry.sdk.metrics import MeterProvider

from ray._private.metrics_agent import Record
from ray._private.telemetry.metric_cardinality import MetricCardinality
from ray._private.telemetry.metric_types import MetricType

logger = logging.getLogger(__name__)

# =============================================================================
# Constants
# =============================================================================

NAMESPACE = "ray"

# Export mode configuration
RAY_METRICS_EXPORT_MODE = "RAY_METRICS_EXPORT_MODE"
RAY_METRICS_PUSH_INTERVAL_MS = "RAY_METRICS_PUSH_INTERVAL_MS"

# Prometheus Remote Write configuration
RAY_METRICS_REMOTE_WRITE_ENDPOINT = "RAY_METRICS_REMOTE_WRITE_ENDPOINT"
RAY_METRICS_REMOTE_WRITE_USERNAME = "RAY_METRICS_REMOTE_WRITE_USERNAME"
RAY_METRICS_REMOTE_WRITE_PASSWORD = "RAY_METRICS_REMOTE_WRITE_PASSWORD"
RAY_METRICS_REMOTE_WRITE_HEADERS = "RAY_METRICS_REMOTE_WRITE_HEADERS"
RAY_METRICS_REMOTE_WRITE_TIMEOUT = "RAY_METRICS_REMOTE_WRITE_TIMEOUT"
RAY_METRICS_REMOTE_WRITE_TENANT_ID = "RAY_METRICS_REMOTE_WRITE_TENANT_ID"

# Remote Write reliability configuration (for "unexpected EOF" error optimization)
RAY_METRICS_REMOTE_WRITE_CONNECT_TIMEOUT = "RAY_METRICS_REMOTE_WRITE_CONNECT_TIMEOUT"
RAY_METRICS_REMOTE_WRITE_BATCH_SIZE = "RAY_METRICS_REMOTE_WRITE_BATCH_SIZE"

# Metric filtering configuration
RAY_METRICS_REMOTE_WRITE_INCLUDE_METRICS = "RAY_METRICS_REMOTE_WRITE_INCLUDE_METRICS"
RAY_METRICS_REMOTE_WRITE_EXCLUDE_METRICS = "RAY_METRICS_REMOTE_WRITE_EXCLUDE_METRICS"

# Push interval limits
MIN_PUSH_INTERVAL_MS = 5000
DEFAULT_PUSH_INTERVAL_MS = 10000

# Default exclude patterns for metrics filtering (reduces storage pressure by ~60%)
# These patterns filter out high-cardinality and non-essential metrics while
# preserving user-defined metrics and core monitoring capabilities.
# Users can override this by setting RAY_METRICS_REMOTE_WRITE_EXCLUDE_METRICS env var.
DEFAULT_EXCLUDE_PATTERNS = [
    # Ray Data - Iterator internal details (not needed for general monitoring)
    "ray_data_iter_block_*",
    "ray_data_iter_batch_*",
    "ray_data_iter_initialize_*",
    "ray_data_iter_get_*",
    "ray_data_iter_format_*",
    "ray_data_iter_collate_*",
    "ray_data_iter_finalize_*",
    "ray_data_iter_blocks_*",
    "ray_data_iter_prefetched_*",
    # Ray Data - Fine-grained input/output metrics
    "ray_data_average_*",
    "ray_data_block_serialization_*",
    # Ray Data - Histogram metrics (high cardinality)
    "ray_data_task_completion_time",
    "ray_data_block_completion_time",
    "ray_data_block_size_*",
    # Ray Core - Internal details
    "ray_operation_*",
    "ray_internal_*",
    "ray_spill_manager_*",
    "ray_pull_manager_*",
    "ray_push_manager_*",
]


# =============================================================================
# OpenTelemetry Metric Recorder
# =============================================================================


class OpenTelemetryMetricRecorder:
    """
    A class to record OpenTelemetry metrics. This is the main entry point for exporting
    all ray telemetries to Prometheus server.
    It uses OpenTelemetry's Prometheus exporter to export metrics.

    Export Modes:
        - "pull" (default): Prometheus scrape at :8080/metrics
        - "push": Prometheus Remote Write protocol (alias for remote_write)
        - "remote_write": Prometheus Remote Write protocol

    Environment Variables:
        Common:
            RAY_METRICS_EXPORT_MODE: "pull", "push", or "remote_write"
            RAY_METRICS_PUSH_INTERVAL_MS: Push interval in ms (default: 60000, min: 60000)

        Remote Write mode (push/remote_write):
            RAY_METRICS_REMOTE_WRITE_ENDPOINT: Remote write URL
            RAY_METRICS_REMOTE_WRITE_USERNAME: Basic auth username
            RAY_METRICS_REMOTE_WRITE_PASSWORD: Basic auth password
            RAY_METRICS_REMOTE_WRITE_HEADERS: Additional headers (JSON)
            RAY_METRICS_REMOTE_WRITE_TIMEOUT: Request timeout in seconds (default: 30)
            RAY_METRICS_REMOTE_WRITE_TENANT_ID: Tenant ID for multi-tenant systems

        Reliability configuration (for "unexpected EOF" error optimization):
            RAY_METRICS_REMOTE_WRITE_CONNECT_TIMEOUT: Connection timeout in seconds (default: 5)
            RAY_METRICS_REMOTE_WRITE_BATCH_SIZE: Max time series per batch (default: 500)

        Metric filtering:
            RAY_METRICS_REMOTE_WRITE_INCLUDE_METRICS: Include patterns (comma-separated, wildcards)
            RAY_METRICS_REMOTE_WRITE_EXCLUDE_METRICS: Exclude patterns (comma-separated, wildcards)
    """

    _metrics_initialized = False
    _metrics_initialized_lock = threading.Lock()
    _export_mode: Optional[str] = None

    def __init__(self):
        self._lock = threading.Lock()
        self._registered_instruments = {}
        self._gauge_observations_by_name = defaultdict(dict)
        self._counter_observations_by_name = defaultdict(dict)
        self._sum_observations_by_name = defaultdict(dict)
        self._histogram_bucket_midpoints = defaultdict(list)
        self._init_metrics()
        self.meter = metrics.get_meter(__name__)

    def _create_observable_callback(
        self, metric_name: str, metric_type: MetricType
    ) -> Callable[[dict], List[Observation]]:
        """
        Factory method to create callbacks for observable metrics.

        Args:
            metric_name: name of the metric for which the callback is being created
            metric_type: type of the metric for which the callback is being created

        Returns:
            Callable: A callback function that can be used to record observations for the metric.
        """

        def callback(options):
            with self._lock:
                # Select appropriate storage based on metric type
                if metric_type == MetricType.GAUGE:
                    observations = self._gauge_observations_by_name.get(metric_name, {})
                    # Clear after reading (gauges report last value)
                    self._gauge_observations_by_name[metric_name] = {}
                elif metric_type == MetricType.COUNTER:
                    observations = self._counter_observations_by_name.get(
                        metric_name, {}
                    )
                    # Don't clear - counters are cumulative
                elif metric_type == MetricType.SUM:
                    observations = self._sum_observations_by_name.get(metric_name, {})
                    # Don't clear - sums are cumulative
                else:
                    return []

                # Aggregate by filtered tags (drop high cardinality labels)
                high_cardinality_labels = (
                    MetricCardinality.get_high_cardinality_labels_to_drop(metric_name)
                )
                # First, collect all values that share the same filtered tag set
                values_by_filtered_tags = defaultdict(list)
                for tag_set, val in observations.items():
                    filtered = frozenset(
                        (k, v) for k, v in tag_set if k not in high_cardinality_labels
                    )
                    values_by_filtered_tags[filtered].append(val)

                # Then aggregate each group using the appropriate aggregation function
                agg_fn = MetricCardinality.get_aggregation_function(
                    metric_name, metric_type
                )
                return [
                    Observation(agg_fn(values), attributes=dict(filtered))
                    for filtered, values in values_by_filtered_tags.items()
                ]

        return callback

    # -------------------------------------------------------------------------
    # Initialization
    # -------------------------------------------------------------------------

    def _init_metrics(self):
        """Initialize the global metrics provider (once per process)."""
        with self._metrics_initialized_lock:
            if self._metrics_initialized:
                return

            export_mode = os.environ.get(RAY_METRICS_EXPORT_MODE, "push").lower()
            OpenTelemetryMetricRecorder._export_mode = export_mode

            if export_mode in ("push", "remote_write"):
                reader = self._create_remote_write_reader()
            else:
                reader = PrometheusMetricReader()
                logger.info(
                    "Metrics export mode: PULL (Prometheus scrape at :8080/metrics)"
                )

            # Initialize the global metrics provider and meter. We only do this once on
            # the first initialization of the class, because re-setting the meter provider
            # can result in loss of metrics.
            provider = MeterProvider(metric_readers=[reader])
            metrics.set_meter_provider(provider)
            self._metrics_initialized = True

    def _create_remote_write_reader(self):
        """Create Prometheus Remote Write mode reader.

        Uses a wrapper class to fix unit mapping issue in the official exporter.
        The official exporter appends unit directly (e.g., "ray_tasks_1"),
        but we use map_unit to match PrometheusMetricReader behavior.
        """
        from opentelemetry.exporter.prometheus._mapping import map_unit
        from opentelemetry.exporter.prometheus_remote_write import (
            PrometheusRemoteWriteMetricsExporter,
        )
        from opentelemetry.sdk.metrics.export import (
            Gauge,
            Histogram,
            PeriodicExportingMetricReader,
            Sum,
        )

        class RayRemoteWriteExporter(PrometheusRemoteWriteMetricsExporter):
            """Exporter with proper unit mapping for consistent metric names.

            Also adds 'instance' label for Grafana dashboard compatibility.
            In Prometheus pull mode, the 'instance' label is automatically added
            by Prometheus server. In push/remote_write mode, we need to add it
            manually to maintain compatibility with existing dashboards.

            Features:
                - Proper unit mapping (matches PrometheusMetricReader behavior)
                - Automatic 'instance' label for dashboard compatibility
                - Metric filtering (include/exclude patterns with wildcards)
                - Batch size limiting (to prevent request body truncation)
                - Enhanced error logging

            Note: 'ray_io_cluster' label is added at the metrics recording source
            (reporter_agent.py) rather than here, so it applies to all export modes.
            """

            def __init__(
                self,
                endpoint,
                basic_auth=None,
                headers=None,
                timeout=30,
                batch_size=500,
                include_patterns=None,
                exclude_patterns=None,
            ):
                super().__init__(
                    endpoint=endpoint,
                    basic_auth=basic_auth,
                    headers=headers,
                    timeout=timeout,
                )
                self._batch_size = batch_size
                self._include_patterns = include_patterns or []
                self._exclude_patterns = exclude_patterns or []
                self._endpoint = endpoint

            def _should_include_metric(self, metric_name: str) -> bool:
                """Check if metric should be included based on filter patterns.

                Args:
                    metric_name: The name of the metric to check

                Returns:
                    True if metric should be included, False otherwise

                Filter logic:
                    1. If include patterns specified, metric must match at least one
                    2. If exclude patterns specified, metric must not match any
                    3. If no patterns specified, include all metrics
                """
                # If include patterns specified, check if metric matches any
                if self._include_patterns:
                    matched = any(
                        fnmatch.fnmatch(metric_name, pattern)
                        for pattern in self._include_patterns
                    )
                    if not matched:
                        return False

                # If exclude patterns specified, check if metric matches any
                if self._exclude_patterns:
                    excluded = any(
                        fnmatch.fnmatch(metric_name, pattern)
                        for pattern in self._exclude_patterns
                    )
                    if excluded:
                        return False

                return True

            def _add_instance_label(self, attrs_dict):
                """Add 'instance' label from 'ip' and 'NodeId' for Prometheus compatibility.

                The instance label format is '<ip>:<NodeId>' when NodeId is available,
                which uniquely identifies a node even when multiple nodes share the same IP.
                Falls back to just 'ip' if NodeId is not present.
                """
                # In pull mode, Prometheus adds 'instance' automatically from scrape target
                # In push/remote_write mode, we need to add it manually
                if "instance" not in attrs_dict and "ip" in attrs_dict:
                    ip = attrs_dict["ip"]
                    node_id = attrs_dict.get("NodeId")
                    if node_id:
                        # Use ip:NodeId format to uniquely identify nodes with same IP
                        attrs_dict["instance"] = f"{ip}:{node_id}"
                    else:
                        attrs_dict["instance"] = ip
                return attrs_dict

            def _parse_data_point(self, data_point, name=None):
                """Override to add 'instance' label for dashboard compatibility."""
                attrs_dict = dict(data_point.attributes.items())
                attrs_dict = self._add_instance_label(attrs_dict)

                attrs = tuple(attrs_dict.items()) + (
                    ("__name__", self._sanitize_string(name, "name")),
                )
                sample = (data_point.value, (data_point.time_unix_nano // 1_000_000))
                return attrs, sample

            def _parse_histogram_data_point(self, data_point, name):
                """Override to add 'instance' label for histogram data points."""
                attrs_dict = dict(data_point.attributes.items())
                attrs_dict = self._add_instance_label(attrs_dict)

                timestamp = data_point.time_unix_nano // 1_000_000
                results = []

                # Generate bucket samples
                cumulative_count = 0
                for i, bound in enumerate(data_point.explicit_bounds):
                    cumulative_count += data_point.bucket_counts[i]
                    bucket_attrs = tuple(attrs_dict.items()) + (
                        ("le", str(bound)),
                        ("__name__", self._sanitize_string(f"{name}_bucket", "name")),
                    )
                    results.append((bucket_attrs, (cumulative_count, timestamp)))

                # +Inf bucket
                cumulative_count += data_point.bucket_counts[-1]
                inf_attrs = tuple(attrs_dict.items()) + (
                    ("le", "+Inf"),
                    ("__name__", self._sanitize_string(f"{name}_bucket", "name")),
                )
                results.append((inf_attrs, (cumulative_count, timestamp)))

                # Sum
                sum_attrs = tuple(attrs_dict.items()) + (
                    ("__name__", self._sanitize_string(f"{name}_sum", "name")),
                )
                results.append((sum_attrs, (data_point.sum, timestamp)))

                # Count
                count_attrs = tuple(attrs_dict.items()) + (
                    ("__name__", self._sanitize_string(f"{name}_count", "name")),
                )
                results.append((count_attrs, (data_point.count, timestamp)))

                return results

            def _parse_metric(self, metric, resource_labels):
                """Parse metric with filtering support."""
                # Apply metric filtering
                if not self._should_include_metric(metric.name):
                    return []

                mapped_unit = map_unit(metric.unit)
                name = f"{metric.name}_{mapped_unit}" if mapped_unit else metric.name

                sample_sets = defaultdict(list)
                if isinstance(metric.data, (Gauge, Sum)):
                    for dp in metric.data.data_points:
                        attrs, sample = self._parse_data_point(dp, name)
                        sample_sets[attrs].append(sample)
                elif isinstance(metric.data, Histogram):
                    for dp in metric.data.data_points:
                        for attrs, sample in self._parse_histogram_data_point(dp, name):
                            sample_sets[attrs].append(sample)
                else:
                    logger.warning("Unsupported Metric Type: %s", type(metric.data))
                    return []
                return self._convert_to_timeseries(sample_sets, resource_labels)

            def _send_batch(self, timeseries_batch):
                """Send a batch of time series.

                Args:
                    timeseries_batch: List of time series to send

                Returns:
                    True if send succeeded, False otherwise
                """
                try:
                    self._send_message(
                        self._build_message(timeseries_batch),
                        self._headers,
                    )
                    return True
                except Exception as e:
                    error_type = type(e).__name__
                    logger.error(
                        "Remote write failed: %s - %s. Endpoint: %s, batch_size: %d",
                        error_type,
                        str(e),
                        self._endpoint,
                        len(timeseries_batch),
                    )
                    return False

            def export(self, metrics_data, *args, **kwargs):
                """Export metrics with batching support.

                This method overrides the parent's export to add:
                1. Batching: Split large payloads into smaller batches
                """
                from opentelemetry.sdk.metrics.export import MetricExportResult

                # Collect all time series from all resource metrics
                all_timeseries = []
                for resource_metrics in metrics_data.resource_metrics:
                    resource = resource_metrics.resource
                    if self.resources_as_labels:
                        resource_labels = [
                            (n, str(v))
                            for n, v in resource.attributes.items()
                        ]
                    else:
                        resource_labels = []
                    for scope_metrics in resource_metrics.scope_metrics:
                        for metric in scope_metrics.metrics:
                            timeseries = self._parse_metric(metric, resource_labels)
                            all_timeseries.extend(timeseries)

                if not all_timeseries:
                    return MetricExportResult.SUCCESS

                # Split into batches
                total_count = len(all_timeseries)
                success_count = 0
                failed_count = 0

                for i in range(0, total_count, self._batch_size):
                    batch = all_timeseries[i : i + self._batch_size]
                    if self._send_batch(batch):
                        success_count += len(batch)
                    else:
                        failed_count += len(batch)

                if failed_count > 0:
                    logger.warning(
                        "Remote write partial failure: %d/%d time series sent, "
                        "%d failed. Endpoint: %s",
                        success_count,
                        total_count,
                        failed_count,
                        self._endpoint,
                    )
                    # Return failure if any batch failed
                    return MetricExportResult.FAILURE

                return MetricExportResult.SUCCESS

        # Read configuration
        endpoint = os.environ.get(
            RAY_METRICS_REMOTE_WRITE_ENDPOINT, "http://10.81.0.157:9090/api/v1/write"
        )
        push_interval_ms = int(
            os.environ.get(RAY_METRICS_PUSH_INTERVAL_MS, str(DEFAULT_PUSH_INTERVAL_MS))
        )
        if push_interval_ms < MIN_PUSH_INTERVAL_MS:
            logger.warning(
                "RAY_METRICS_PUSH_INTERVAL_MS=%d is less than minimum allowed "
                "value %dms. Setting to %dms.",
                push_interval_ms,
                MIN_PUSH_INTERVAL_MS,
                MIN_PUSH_INTERVAL_MS,
            )
            push_interval_ms = MIN_PUSH_INTERVAL_MS
        timeout = int(os.environ.get(RAY_METRICS_REMOTE_WRITE_TIMEOUT, "30"))

        # Read reliability configuration
        batch_size = int(os.environ.get(RAY_METRICS_REMOTE_WRITE_BATCH_SIZE, "500"))

        # Read metric filtering configuration
        # Use user-specified patterns if provided, otherwise use defaults for exclude
        include_metrics_str = os.environ.get(
            RAY_METRICS_REMOTE_WRITE_INCLUDE_METRICS, ""
        )
        exclude_metrics_str = os.environ.get(
            RAY_METRICS_REMOTE_WRITE_EXCLUDE_METRICS, None
        )
        include_patterns = (
            [p.strip() for p in include_metrics_str.split(",") if p.strip()]
            if include_metrics_str
            else []
        )
        # If user explicitly sets EXCLUDE_METRICS (even empty string), use that
        # Otherwise, use DEFAULT_EXCLUDE_PATTERNS
        if exclude_metrics_str is not None:
            exclude_patterns = (
                [p.strip() for p in exclude_metrics_str.split(",") if p.strip()]
                if exclude_metrics_str
                else []
            )
        else:
            exclude_patterns = DEFAULT_EXCLUDE_PATTERNS.copy()

        # Parse headers
        headers = {}
        headers_json = os.environ.get(RAY_METRICS_REMOTE_WRITE_HEADERS)
        if headers_json:
            try:
                import json

                headers = json.loads(headers_json)
            except json.JSONDecodeError as e:
                logger.warning(
                    "Failed to parse RAY_METRICS_REMOTE_WRITE_HEADERS: %s", e
                )

        # Add tenant ID header for multi-tenant systems (Cortex/Mimir)
        tenant_id = os.environ.get(RAY_METRICS_REMOTE_WRITE_TENANT_ID)
        if tenant_id:
            headers["X-Scope-OrgID"] = tenant_id

        # Prepare basic auth
        username = os.environ.get(RAY_METRICS_REMOTE_WRITE_USERNAME)
        password = os.environ.get(RAY_METRICS_REMOTE_WRITE_PASSWORD)
        basic_auth = (
            {"username": username, "password": password}
            if username and password
            else None
        )

        exporter = RayRemoteWriteExporter(
            endpoint=endpoint,
            basic_auth=basic_auth,
            headers=headers or None,
            timeout=timeout,
            batch_size=batch_size,
            include_patterns=include_patterns,
            exclude_patterns=exclude_patterns,
        )

        # Log configuration
        filter_info = ""
        if include_patterns:
            filter_info += f", include: {len(include_patterns)} patterns"
        if exclude_patterns:
            using_defaults = exclude_metrics_str is None
            filter_info += (
                f", exclude: {len(exclude_patterns)} patterns"
                f"{' (default)' if using_defaults else ''}"
            )

        logger.info(
            "Metrics export mode: REMOTE_WRITE to %s "
            "(interval: %sms, timeout: %ss, batch_size: %d%s)",
            endpoint,
            push_interval_ms,
            timeout,
            batch_size,
            filter_info,
        )
        return PeriodicExportingMetricReader(
            exporter, export_interval_millis=push_interval_ms
        )

    @classmethod
    def get_export_mode(cls) -> Optional[str]:
        """Get the current metrics export mode."""
        return cls._export_mode

    def register_gauge_metric(self, name: str, description: str) -> None:
        with self._lock:
            if name in self._registered_instruments:
                # Gauge with the same name is already registered.
                return

            callback = self._create_observable_callback(name, MetricType.GAUGE)
            instrument = self.meter.create_observable_gauge(
                name=f"{NAMESPACE}_{name}",
                description=description,
                unit="1",
                callbacks=[callback],
            )
            self._registered_instruments[name] = instrument
            self._gauge_observations_by_name[name] = {}

    def register_counter_metric(self, name: str, description: str) -> None:
        """
        Register an observable counter metric with the given name and description.
        """
        with self._lock:
            if name in self._registered_instruments:
                # Counter with the same name is already registered. This is a common
                # case when metrics are exported from multiple Ray components (e.g.,
                # raylet, worker, etc.) running in the same node. Since each component
                # may export metrics with the same name, the same metric might be
                # registered multiple times.
                return

            callback = self._create_observable_callback(name, MetricType.COUNTER)
            instrument = self.meter.create_observable_counter(
                name=f"{NAMESPACE}_{name}",
                description=description,
                unit="1",
                callbacks=[callback],
            )
            self._registered_instruments[name] = instrument
            self._counter_observations_by_name[name] = {}

    def register_sum_metric(self, name: str, description: str) -> None:
        """
        Register an observable sum metric with the given name and description.
        """
        with self._lock:
            if name in self._registered_instruments:
                # Sum with the same name is already registered. This is a common
                # case when metrics are exported from multiple Ray components (e.g.,
                # raylet, worker, etc.) running in the same node. Since each component
                # may export metrics with the same name, the same metric might be
                # registered multiple times.
                return

            callback = self._create_observable_callback(name, MetricType.SUM)
            instrument = self.meter.create_observable_up_down_counter(
                name=f"{NAMESPACE}_{name}",
                description=description,
                unit="1",
                callbacks=[callback],
            )
            self._registered_instruments[name] = instrument
            self._sum_observations_by_name[name] = {}

    def register_histogram_metric(
        self, name: str, description: str, buckets: List[float]
    ) -> None:
        """
        Register a histogram metric with the given name and description.
        """
        with self._lock:
            if name in self._registered_instruments:
                # Histogram with the same name is already registered. This is a common
                # case when metrics are exported from multiple Ray components (e.g.,
                # raylet, worker, etc.) running in the same node. Since each component
                # may export metrics with the same name, the same metric might be
                # registered multiple times.
                return

            instrument = self.meter.create_histogram(
                name=f"{NAMESPACE}_{name}",
                description=description,
                unit="1",
                explicit_bucket_boundaries_advisory=buckets,
            )
            self._registered_instruments[name] = instrument

            # calculate the bucket midpoints; this is used for converting histogram
            # internal representation to approximated histogram data points.
            for i in range(len(buckets)):
                if i == 0:
                    lower_bound = 0.0 if buckets[0] > 0 else buckets[0] * 2.0
                    self._histogram_bucket_midpoints[name].append(
                        (lower_bound + buckets[0]) / 2.0
                    )
                else:
                    self._histogram_bucket_midpoints[name].append(
                        (buckets[i] + buckets[i - 1]) / 2.0
                    )
            # Approximated mid point for Inf+ bucket. Inf+ bucket is an implicit bucket
            # that is not part of buckets.
            self._histogram_bucket_midpoints[name].append(
                1.0 if buckets[-1] <= 0 else buckets[-1] * 2.0
            )

    def get_histogram_bucket_midpoints(self, name: str) -> List[float]:
        """
        Get the bucket midpoints for a histogram metric with the given name.
        """
        return self._histogram_bucket_midpoints[name]

    def set_metric_value(self, name: str, tags: dict, value: float):
        """
        Set the value of a metric with the given name and tags.

        For observable metrics (gauge, counter, sum), this stores the value internally
        and returns immediately. The value will be exported asynchronously when
        OpenTelemetry collects metrics.

        For histograms, this calls record() synchronously since there is no observable
        histogram in OpenTelemetry.

        If the metric is not registered, it lazily records the value for observable metrics or is a no-op for
        synchronous metrics.
        """
        with self._lock:
            tag_key = frozenset(tags.items())
            if self._gauge_observations_by_name.get(name) is not None:
                # Gauge - store the most recent value for the given tags.
                self._gauge_observations_by_name[name][tag_key] = value
            elif name in self._counter_observations_by_name:
                # Counter - increment the value for the given tags.
                self._counter_observations_by_name[name][tag_key] = (
                    self._counter_observations_by_name[name].get(tag_key, 0) + value
                )
            elif name in self._sum_observations_by_name:
                # Sum - add the value for the given tags.
                self._sum_observations_by_name[name][tag_key] = (
                    self._sum_observations_by_name[name].get(tag_key, 0) + value
                )
            else:
                # Histogram - record the value synchronously.
                instrument = self._registered_instruments.get(name)
                if isinstance(instrument, metrics.Histogram):
                    # Filter out high cardinality labels.
                    filtered_tags = {
                        k: v
                        for k, v in tags.items()
                        if k
                        not in MetricCardinality.get_high_cardinality_labels_to_drop(
                            name
                        )
                    }
                    instrument.record(value, attributes=filtered_tags)
                else:
                    logger.warning(
                        f"Metric {name} is not registered or unsupported type."
                    )

    def record_histogram_aggregated_batch(
        self,
        name: str,
        data_points: List[dict],
    ) -> None:
        """
        Record pre-aggregated histogram data for multiple data points in a single batch.

        This method takes pre-aggregated bucket counts and reconstructs individual
        observations using bucket midpoints. It acquires the lock once and performs
        all record() calls for ALL data points, minimizing lock contention.

        Note: The histogram sum value will be an approximation since we use bucket midpoints instead of actual values.
        """
        with self._lock:
            instrument = self._registered_instruments.get(name)
            if not isinstance(instrument, metrics.Histogram):
                logger.warning(
                    f"Metric {name} is not a registered histogram, skipping recording."
                )
                return

            bucket_midpoints = self._histogram_bucket_midpoints[name]
            high_cardinality_labels = (
                MetricCardinality.get_high_cardinality_labels_to_drop(name)
            )

            for dp in data_points:
                tags = dp["tags"]
                bucket_counts = dp["bucket_counts"]
                assert len(bucket_counts) == len(bucket_midpoints), (
                    "Number of bucket counts and midpoints must match"
                )

                filtered_tags = {
                    k: v for k, v in tags.items() if k not in high_cardinality_labels
                }

                for i, bucket_count in enumerate(bucket_counts):
                    if bucket_count == 0:
                        continue
                    midpoint = bucket_midpoints[i]
                    for _ in range(bucket_count):
                        instrument.record(midpoint, attributes=filtered_tags)

    def record_and_export(self, records: List[Record], global_tags=None):
        """
        Record a list of telemetry records and export them to Prometheus.
        """
        global_tags = global_tags or {}

        for record in records:
            gauge = record.gauge
            value = record.value
            tags = {**record.tags, **global_tags}
            try:
                self.register_gauge_metric(gauge.name, gauge.description or "")
                self.set_metric_value(gauge.name, tags, value)
            except Exception as e:
                logger.error(
                    f"Failed to record metric {gauge.name} with value {value} with tags {tags!r} and global tags {global_tags!r} due to: {e!r}"
                )
