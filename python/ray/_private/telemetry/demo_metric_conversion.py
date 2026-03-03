#!/usr/bin/env python3
"""
演示 OpenTelemetry 指标格式转换为 Prometheus 格式的完整过程

此文件展示：
1. Observable Gauge 的 callback 机制和 _observations_by_name 字典的作用
2. Counter、Histogram 同步指标的记录方式
3. OTLP 数据结构的详细格式
4. 转换为 Prometheus 格式的完整过程
5. GlobalTags 如何被嵌入到 data_point.attributes

运行方式:
    cd /Users/franke/Desktop/git/ray
    python python/ray/_private/telemetry/demo_metric_conversion.py
"""

import threading
import time
from collections import defaultdict
from typing import List

from opentelemetry import metrics
from opentelemetry.metrics import Observation
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    InMemoryMetricReader,
)
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.metrics._internal.point import (
    NumberDataPoint,
    HistogramDataPoint,
)


def print_separator(title: str, char: str = "─", width: int = 100):
    """打印分隔符"""
    print(f"\n{char * width}")
    print(title)
    print(f"{char * width}")


def print_otlp_structure(metrics_data):
    """打印完整的 OTLP 数据结构"""

    print_separator("OTLP MetricsData 结构详解", "=")

    for rm_idx, resource_metrics in enumerate(metrics_data.resource_metrics):
        print(f"\n[ResourceMetrics {rm_idx}]")

        # 打印 Resource Attributes
        print(f"\n  resource.attributes:")
        if resource_metrics.resource.attributes:
            for key, value in resource_metrics.resource.attributes.items():
                print(f"    {key}: {value}")
            print(f"\n  ⚠️  注意: resource.attributes 不会转换为业务指标的 labels")
            print(f"      它们只会出现在 target_info 元数据指标中")
        else:
            print(f"    (空)")

        for sm_idx, scope_metrics in enumerate(resource_metrics.scope_metrics):
            print(f"\n  [ScopeMetrics {sm_idx}]")
            print(f"    scope.name: {scope_metrics.scope.name}")
            print(f"    scope.version: {scope_metrics.scope.version}")
            print(f"    ⚠️  scope 信息不会转换为 Prometheus labels")

            for m_idx, metric in enumerate(scope_metrics.metrics):
                print(f"\n    [Metric {m_idx}]")
                print(f"      name: {metric.name}")
                print(f"      description: {metric.description}")
                print(f"      unit: {metric.unit}")

                # 确定指标类型
                data = metric.data
                metric_type = type(data).__name__
                print(f"      type: {metric_type}")

                # 打印数据点
                data_points = getattr(data, 'data_points', [])
                for dp_idx, data_point in enumerate(data_points):
                    print(f"\n      [DataPoint {dp_idx}]")

                    # 打印时间戳
                    if hasattr(data_point, 'time_unix_nano') and data_point.time_unix_nano:
                        ts = data_point.time_unix_nano / 1e9
                        print(f"        time_unix_nano: {data_point.time_unix_nano}")
                        print(f"        (timestamp: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts))})")

                    # 打印值
                    if isinstance(data_point, NumberDataPoint):
                        print(f"        value: {data_point.value}")
                    elif isinstance(data_point, HistogramDataPoint):
                        print(f"        count: {data_point.count}")
                        print(f"        sum: {data_point.sum}")
                        print(f"        bucket_counts: {list(data_point.bucket_counts)}")
                        print(f"        explicit_bounds: {list(data_point.explicit_bounds)}")

                    # 打印 attributes
                    print(f"\n        attributes (将转换为 Prometheus labels):")
                    if data_point.attributes:
                        for key, value in data_point.attributes.items():
                            print(f"          ✅ {key}: {value}")
                    else:
                        print(f"          (空)")


def print_prometheus_format(metrics_data):
    """打印 Prometheus 格式输出"""

    print_separator("转换为 Prometheus 格式", "=")

    for resource_metrics in metrics_data.resource_metrics:
        for scope_metrics in resource_metrics.scope_metrics:
            for metric in scope_metrics.metrics:
                data_points = getattr(metric.data, 'data_points', [])
                metric_type = type(metric.data).__name__

                # 确定 Prometheus 类型
                prom_type = "gauge"
                if "Sum" in metric_type:
                    prom_type = "counter"
                elif "Histogram" in metric_type:
                    prom_type = "histogram"

                print(f"\n# HELP {metric.name} {metric.description}")
                print(f"# TYPE {metric.name} {prom_type}")

                for data_point in data_points:
                    # 构建 labels 字符串
                    labels = ",".join(
                        f'{k}="{v}"'
                        for k, v in (data_point.attributes or {}).items()
                    )
                    labels_str = f"{{{labels}}}" if labels else ""

                    if isinstance(data_point, NumberDataPoint):
                        print(f"{metric.name}{labels_str} {data_point.value}")
                    elif isinstance(data_point, HistogramDataPoint):
                        # 打印 bucket
                        cum_count = 0
                        for i, count in enumerate(data_point.bucket_counts):
                            cum_count += count
                            if i < len(data_point.explicit_bounds):
                                le = data_point.explicit_bounds[i]
                            else:
                                le = "+Inf"
                            bucket_labels = f'{labels},le="{le}"' if labels else f'le="{le}"'
                            print(f"{metric.name}_bucket{{{bucket_labels}}} {cum_count}")
                        print(f"{metric.name}_sum{labels_str} {data_point.sum}")
                        print(f"{metric.name}_count{labels_str} {data_point.count}")


class DemoMetricRecorder:
    """
    简化版 OpenTelemetryMetricRecorder，演示 _observations_by_name 字典的作用
    """

    def __init__(self, meter):
        self._lock = threading.Lock()
        self._registered_instruments = {}
        self._observations_by_name = defaultdict(dict)  # 核心：延迟记录缓冲区
        self.meter = meter

    def print_observations_explanation(self):
        """打印 _observations_by_name 的说明"""
        print("""
┌─────────────────────────────────────────────────────────────────────────────────────────────────┐
│  _observations_by_name 字典的作用                                                               │
├─────────────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                                 │
│  这是一个延迟记录缓冲区，专门用于 Observable Gauge 类型的指标。                                 │
│                                                                                                 │
│  数据结构:                                                                                      │
│  _observations_by_name = {                                                                      │
│      "tasks_running": {                                                                         │
│          frozenset({("State", "RUNNING")}): 42.0,  ← 存储最新值                                │
│          frozenset({("State", "PENDING")}): 15.0,                                              │
│      }                                                                                          │
│  }                                                                                              │
│                                                                                                 │
│  工作流程:                                                                                      │
│  1. set_metric_value() 被调用 → 值存入字典                                                     │
│  2. 相同 tags 的新值会覆盖旧值（只保留最新）                                                   │
│  3. Prometheus 抓取时触发 callback → 读取字典中的值                                            │
│  4. callback 执行后清空字典 → 避免导出过期数据                                                 │
│                                                                                                 │
│  为什么需要这个字典？                                                                           │
│  - Observable Gauge 使用回调机制，不能直接记录值                                               │
│  - 需要一个地方存储"当前状态值"，等待 callback 时读取                                          │
│  - 与 Counter/Histogram 不同，这些同步指标直接调用 instrument 方法                             │
│                                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────────────────────┘
""")

    def register_gauge_metric(self, name: str, description: str, global_tags: dict = None) -> None:
        """注册 Gauge 指标"""
        global_tags = global_tags or {}

        with self._lock:
            if name in self._registered_instruments:
                return

            def callback(options):
                with self._lock:
                    observations = self._observations_by_name[name]

                    print(f"\n  [callback 被调用] 指标: {name}")
                    print(f"    读取 _observations_by_name['{name}']:")
                    for tag_set, val in observations.items():
                        print(f"      {dict(tag_set)} → {val}")

                    # 清空字典
                    self._observations_by_name[name] = {}
                    print(f"    ✓ 清空字典")

                    # 返回 Observation 列表
                    result = []
                    for tag_set, val in observations.items():
                        attrs = {**dict(tag_set), **global_tags}
                        result.append(Observation(val, attributes=attrs))

                    print(f"    ✓ 返回 {len(result)} 个 Observation")
                    return result

            instrument = self.meter.create_observable_gauge(
                name=f"ray_{name}",
                description=description,
                unit="1",
                callbacks=[callback],
            )
            self._registered_instruments[name] = instrument
            self._observations_by_name[name] = {}

            print(f"  [注册] ray_{name} (Observable Gauge)")

    def register_counter_metric(self, name: str, description: str) -> None:
        """注册 Counter 指标"""
        with self._lock:
            if name in self._registered_instruments:
                return

            instrument = self.meter.create_counter(
                name=f"ray_{name}",
                description=description,
                unit="1",
            )
            self._registered_instruments[name] = instrument
            print(f"  [注册] ray_{name} (Counter)")

    def register_histogram_metric(self, name: str, description: str, buckets: List[float]) -> None:
        """注册 Histogram 指标"""
        with self._lock:
            if name in self._registered_instruments:
                return

            instrument = self.meter.create_histogram(
                name=f"ray_{name}",
                description=description,
                unit="1",
                explicit_bucket_boundaries_advisory=buckets,
            )
            self._registered_instruments[name] = instrument
            print(f"  [注册] ray_{name} (Histogram, buckets={buckets})")

    def set_metric_value(self, name: str, tags: dict, value: float, global_tags: dict = None) -> None:
        """设置指标值"""
        global_tags = global_tags or {}

        with self._lock:
            if self._observations_by_name.get(name) is not None:
                # Observable 指标
                tag_key = frozenset(tags.items())
                old_value = self._observations_by_name[name].get(tag_key)
                self._observations_by_name[name][tag_key] = value

                print(f"\n  [set_metric_value] Observable: {name}")
                print(f"    tags: {tags}, value: {value}")
                if old_value is not None:
                    print(f"    ⚠️  覆盖旧值: {old_value} → {value}")
                print(f"    → 存入 _observations_by_name 字典")
            else:
                # Synchronous 指标
                instrument = self._registered_instruments.get(name)
                combined_tags = {**tags, **global_tags}

                print(f"\n  [set_metric_value] Synchronous: {name}")
                print(f"    tags: {combined_tags}, value: {value}")

                if hasattr(instrument, 'add'):
                    instrument.add(value, attributes=combined_tags)
                    print(f"    → 直接调用 instrument.add()")
                elif hasattr(instrument, 'record'):
                    instrument.record(value, attributes=combined_tags)
                    print(f"    → 直接调用 instrument.record()")

    def print_observations_state(self):
        """打印当前字典状态"""
        print(f"\n  [_observations_by_name 当前状态]")
        with self._lock:
            has_data = False
            for name, observations in self._observations_by_name.items():
                if observations:
                    has_data = True
                    print(f"    {name}:")
                    for tag_set, val in observations.items():
                        print(f"      {dict(tag_set)} → {val}")
            if not has_data:
                print("    (空 - 所有数据已被 callback 消费)")


def demo_full_conversion_process():
    """演示完整的指标转换过程"""

    print("\n" + "█" * 100)
    print("█" + " " * 98 + "█")
    print("█" + "    OpenTelemetry 指标转换为 Prometheus 格式 - 完整演示".center(98) + "█")
    print("█" + " " * 98 + "█")
    print("█" * 100)

    # =========================================================================
    # 步骤 1: 创建 Resource
    # =========================================================================
    print_separator("步骤 1: 创建 Resource（类似于 Ray 的 GlobalTags）")

    resource = Resource.create({
        "service.name": "ray",
        "service.version": "2.9.0",
        "host.name": "ray-node-001",
    })

    print("""
Resource Attributes（将出现在 target_info 中，但不会出现在业务指标 labels 中）:
  - service.name: ray
  - service.version: 2.9.0
  - host.name: ray-node-001
""")

    # Ray 的 GlobalTags
    global_tags = {
        "Component": "gcs_server",
        "Version": "2.9.0",
        "NodeAddress": "192.168.1.100",
        "SessionName": "session_2024_01_01",
    }

    print("""
GlobalTags（将被嵌入到每个指标的 data_point.attributes）:
  - Component: gcs_server
  - Version: 2.9.0
  - NodeAddress: 192.168.1.100
  - SessionName: session_2024_01_01
""")

    # =========================================================================
    # 步骤 2: 创建 MeterProvider 和 InMemoryMetricReader
    # =========================================================================
    print_separator("步骤 2: 初始化 OpenTelemetry MeterProvider")

    # 使用 InMemoryMetricReader 来手动触发收集
    reader = InMemoryMetricReader()
    provider = MeterProvider(
        resource=resource,
        metric_readers=[reader],
    )
    metrics.set_meter_provider(provider)
    meter = metrics.get_meter("ray.demo", version="1.0.0")

    print("""
MeterProvider 配置:
  - Resource: 包含 service.name, service.version, host.name
  - MetricReader: InMemoryMetricReader (手动触发收集)
""")

    # =========================================================================
    # 步骤 3: 创建 DemoMetricRecorder 并解释 _observations_by_name
    # =========================================================================
    print_separator("步骤 3: 解释 _observations_by_name 字典的作用")

    recorder = DemoMetricRecorder(meter)
    recorder.print_observations_explanation()

    # =========================================================================
    # 步骤 4: 注册指标
    # =========================================================================
    print_separator("步骤 4: 注册指标")

    # 注册 Observable Gauge
    recorder.register_gauge_metric(
        "tasks_running",
        "Number of currently running tasks",
        global_tags=global_tags
    )

    # 注册 Counter
    recorder.register_counter_metric(
        "tasks_finished_total",
        "Total number of finished tasks"
    )

    # 注册 Histogram
    recorder.register_histogram_metric(
        "task_latency_ms",
        "Task execution latency in milliseconds",
        buckets=[1.0, 5.0, 10.0, 50.0, 100.0, 500.0]
    )

    # =========================================================================
    # 步骤 5: 记录指标值
    # =========================================================================
    print_separator("步骤 5: 记录指标值（演示 Observable vs Synchronous）")

    # 记录 Gauge（Observable）
    recorder.set_metric_value("tasks_running", {"State": "RUNNING"}, 42)
    recorder.set_metric_value("tasks_running", {"State": "PENDING"}, 15)
    recorder.set_metric_value("tasks_running", {"State": "FINISHED"}, 100)

    # 演示覆盖行为
    print("\n  --- 演示覆盖行为 ---")
    recorder.set_metric_value("tasks_running", {"State": "RUNNING"}, 45)  # 覆盖 42

    # 查看字典状态
    recorder.print_observations_state()

    # 记录 Counter（Synchronous）
    recorder.set_metric_value("tasks_finished_total", {"State": "FINISHED"}, 100, global_tags)

    # 记录 Histogram（Synchronous）
    for latency in [2.5, 8.0, 15.0, 45.0, 120.0]:
        recorder.set_metric_value("task_latency_ms", {"TaskType": "compute"}, latency, global_tags)

    # =========================================================================
    # 步骤 6: 触发收集并查看 OTLP 结构
    # =========================================================================
    print_separator("步骤 6: 触发指标收集（callback 被调用）")

    # 收集指标
    metrics_data = reader.get_metrics_data()

    # 打印 OTLP 结构
    print_otlp_structure(metrics_data)

    # 打印 Prometheus 格式
    print_prometheus_format(metrics_data)

    # =========================================================================
    # 步骤 7: 检查 callback 后的字典状态
    # =========================================================================
    print_separator("步骤 7: callback 执行后 _observations_by_name 状态")

    recorder.print_observations_state()
    print("\n  ✓ Observable Gauge 的 callback 执行后，字典被清空")

    # =========================================================================
    # 步骤 8: 再次记录并收集
    # =========================================================================
    print_separator("步骤 8: 再次记录指标（新周期）")

    recorder.set_metric_value("tasks_running", {"State": "RUNNING"}, 50)
    recorder.set_metric_value("tasks_running", {"State": "PENDING"}, 20)
    recorder.print_observations_state()

    print("\n  触发第二次收集...")
    metrics_data_2 = reader.get_metrics_data()
    print_prometheus_format(metrics_data_2)

    # =========================================================================
    # 清理
    # =========================================================================
    provider.shutdown()

    # =========================================================================
    # 总结
    # =========================================================================
    print("\n" + "█" * 100)
    print("█" + " " * 98 + "█")
    print("█" + "    总结".center(98) + "█")
    print("█" + " " * 98 + "█")
    print("█" * 100)

    print("""
┌─────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                                 │
│  1. _observations_by_name 字典的作用:                                                           │
│     - 专门用于 Observable Gauge 类型的指标                                                      │
│     - 存储 {指标名: {tags_frozenset: value}} 的映射                                             │
│     - 相同 tags 的值会被覆盖（只保留最新值）                                                    │
│     - callback 执行后清空，避免导出过期数据                                                     │
│                                                                                                 │
│  2. 指标类型对比:                                                                               │
│     ┌─────────────────┬─────────────────────────┬─────────────────────────────────────────────┐ │
│     │ 类型            │ 存储方式                │ 适用场景                                    │ │
│     ├─────────────────┼─────────────────────────┼─────────────────────────────────────────────┤ │
│     │ Observable Gauge│ _observations_by_name   │ 当前状态值（任务数、连接数）                │ │
│     │ Counter         │ 直接 instrument.add()   │ 累积值（请求总数、错误总数）                │ │
│     │ Histogram       │ 直接 instrument.record()│ 分布值（延迟、响应大小）                    │ │
│     └─────────────────┴─────────────────────────┴─────────────────────────────────────────────┘ │
│                                                                                                 │
│  3. OTLP 到 Prometheus 转换:                                                                    │
│     - resource.attributes → 仅出现在 target_info 指标                                          │
│     - scope.attributes → 完全丢失                                                               │
│     - data_point.attributes → 转换为 Prometheus labels ✅                                       │
│                                                                                                 │
│  4. Ray 的 GlobalTags 机制:                                                                     │
│     - 将 resource 信息直接嵌入到 data_point.attributes                                         │
│     - 避免依赖 target_info 关联查询                                                             │
│     - 简化 Prometheus 查询                                                                      │
│                                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────────────────────┘
""")


if __name__ == "__main__":
    demo_full_conversion_process()
