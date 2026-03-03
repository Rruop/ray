#!/usr/bin/env python3
"""
Prometheus Remote Write 集成测试

使用 OpenTelemetryMetricRecorder + RAY_METRICS_EXPORT_MODE=remote_write 测试完整流程。

使用方法:
    # 1. 启动 Prometheus
    cd ~/Desktop/prometheus/prometheus-3.9.1.darwin-arm64
    ./prometheus --web.enable-remote-write-receiver

    # 2. 运行测试
    python python/ray/_private/telemetry/test_remote_write_integration.py
"""

import os
import sys
import time

import requests

PROMETHEUS_BASE = "http://localhost:9090"
REMOTE_WRITE_ENDPOINT = f"{PROMETHEUS_BASE}/api/v1/write"


def check_prometheus() -> bool:
    """检查 Prometheus 是否可用"""
    try:
        r = requests.get(f"{PROMETHEUS_BASE}/-/ready", timeout=3)
        return r.status_code == 200
    except Exception:
        return False


def query_metric(name: str) -> list:
    """查询 Prometheus 指标"""
    try:
        r = requests.get(f"{PROMETHEUS_BASE}/api/v1/query", params={"query": name}, timeout=5)
        data = r.json()
        if data.get("status") == "success":
            return data.get("data", {}).get("result", [])
    except Exception:
        pass
    return []


def test_with_recorder():
    """使用 OpenTelemetryMetricRecorder 测试 remote_write 模式"""
    # 设置环境变量 (在导入 OpenTelemetryMetricRecorder 之前)
    os.environ["RAY_METRICS_EXPORT_MODE"] = "remote_write"
    os.environ["RAY_METRICS_REMOTE_WRITE_ENDPOINT"] = REMOTE_WRITE_ENDPOINT
    os.environ["RAY_METRICS_PUSH_INTERVAL_MS"] = "1000"  # 1秒间隔便于测试

    try:
        from opentelemetry.exporter.prometheus_remote_write import (
            PrometheusRemoteWriteMetricsExporter,
        )
    except ImportError:
        print("[错误] 请安装: pip install opentelemetry-exporter-prometheus-remote-write")
        return False

    # 重置 OpenTelemetryMetricRecorder 状态以应用新配置
    from ray._private.telemetry.open_telemetry_metric_recorder import (
        OpenTelemetryMetricRecorder,
    )
    OpenTelemetryMetricRecorder._metrics_initialized = False

    # 创建 recorder
    recorder = OpenTelemetryMetricRecorder()
    print(f"导出模式: {recorder.get_export_mode()}")

    if recorder.get_export_mode() != "remote_write":
        print("[错误] 导出模式未正确设置为 remote_write")
        return False

    # 使用时间戳作为指标名前缀，避免与历史数据冲突
    timestamp = int(time.time())
    test_prefix = f"integration_{timestamp}"

    # 注册指标
    recorder.register_gauge_metric(f"{test_prefix}_tasks", "Test gauge metric")
    recorder.register_counter_metric(f"{test_prefix}_finished", "Test counter metric")
    recorder.register_histogram_metric(
        f"{test_prefix}_latency", "Test histogram metric", [1, 5, 10, 25, 50, 100]
    )

    # 记录指标值
    recorder.set_metric_value(f"{test_prefix}_tasks", {"state": "RUNNING"}, 42.0)
    recorder.set_metric_value(f"{test_prefix}_tasks", {"state": "PENDING"}, 15.0)
    recorder.set_metric_value(f"{test_prefix}_finished", {"component": "raylet"}, 100.0)

    for latency in [2.0, 8.0, 15.0, 30.0]:
        recorder.set_metric_value(f"{test_prefix}_latency", {"type": "compute"}, latency)

    print(f"指标前缀: ray_{test_prefix}")
    print("指标已记录，等待 PeriodicExportingMetricReader 导出...")

    # 等待导出 (push_interval + buffer)
    time.sleep(3)

    # 验证指标
    print("\n验证指标:")
    metrics_to_check = [
        (f"ray_{test_prefix}_tasks", "Gauge"),
        (f"ray_{test_prefix}_finished", "Counter"),
        (f"ray_{test_prefix}_latency_sum", "Histogram"),
    ]

    all_ok = True
    for name, desc in metrics_to_check:
        data = query_metric(name)
        if data:
            # 获取第一个结果的值
            value = data[0].get("value", [None, "?"])[1]
            labels = {k: v for k, v in data[0].get("metric", {}).items() if k != "__name__"}
            print(f"  [OK] {desc}: {name}")
            print(f"       值={value}, 标签={labels}")
        else:
            print(f"  [--] {desc}: {name} (无数据)")
            all_ok = False

    return all_ok


def test_main():
    print("=" * 60)
    print(" OpenTelemetryMetricRecorder Remote Write 集成测试")
    print("=" * 60)

    if not check_prometheus():
        print(f"\n[错误] Prometheus 不可用 ({PROMETHEUS_BASE})")
        print("\n请先启动 Prometheus:")
        print("  cd ~/Desktop/prometheus/prometheus-3.9.1.darwin-arm64")
        print("  ./prometheus --web.enable-remote-write-receiver")
        return 1

    print(f"[OK] Prometheus 可用 ({PROMETHEUS_BASE})")

    print("\n" + "-" * 60)
    result = test_with_recorder()
    print("-" * 60)

    if result:
        print("\n[通过] 集成测试成功")
        print(f"\n查看指标: {PROMETHEUS_BASE}/graph")
        print('查询示例: {__name__=~"ray_integration_.*"}')
        return 0
    else:
        print("\n[失败] 集成测试失败")
        return 1


if __name__ == "__main__":
    sys.exit(test_main())
