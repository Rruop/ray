# Ray Data 日志配置深度分析

## 问题背景

在使用 Ray Data 时，日志中会出现大量 DEBUG 级别的调度器日志，例如：

```
DEBUG streaming_executor_state.py:653 -- [Scheduler] Ineligible operators and reasons: {'Sort': ['no_pending_bundles(input_queues_len=[0])'], 'Repartition': ['no_pending_bundles(input_queues_len=[0])']}
```

本文档分析 Ray Data 的日志系统架构，并提供控制日志级别的方法。

## 日志源码分析

### 1. 日志产生位置

日志来自 `python/ray/data/_internal/execution/streaming_executor_state.py`：

```python
# 第 45 行
logger = logging.getLogger(__name__)

# 第 880-885 行
# Log ineligible operators for debugging
if ineligible_reasons:
    logger.debug(
        "[Scheduler] Ineligible operators and reasons: %s",
        {k: v for k, v in ineligible_reasons.items() if "completed" not in v},
    )
```

Logger 名称为 `ray.data._internal.execution.streaming_executor_state`，是 `ray.data` 的子 logger。

### 2. 默认日志配置

Ray Data 的日志配置定义在 `python/ray/data/_internal/logging.py`：

```python
DEFAULT_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "ray": {"format": DEFAULT_TEXT_FORMATTER},
        "ray_json": {
            "class": f"{DEFAULT_JSON_FORMATTER.__module__}.{DEFAULT_JSON_FORMATTER.__name__}"
        },
    },
    "filters": {
        "console_filter": {"()": "ray.data._internal.logging.HiddenRecordFilter"},
        "core_context_filter": {"()": "ray._common.filters.CoreContextFilter"},
    },
    "handlers": {
        "file": {
            "class": "ray.data._internal.logging.SessionFileHandler",
            "formatter": "ray",
            "filename": "ray-data.log",
            # 注意：file handler 没有设置 level，继承 logger 的 DEBUG
        },
        "file_json": {
            "class": "ray.data._internal.logging.SessionFileHandler",
            "formatter": "ray_json",
            "filename": "ray-data.log",
            "filters": ["core_context_filter"],
        },
        "console": {
            "class": "ray._private.log.PlainRayHandler",
            "formatter": "ray",
            "level": "INFO",  # console handler 级别为 INFO
            "filters": ["console_filter"],
        },
    },
    "loggers": {
        "ray.data": {
            "level": "DEBUG",  # logger 级别为 DEBUG
            "handlers": ["file", "console"],
            "propagate": False,
        },
    },
}
```

### 3. 日志级别层次结构

```
┌─────────────────────────────────────────────────────────────┐
│                    ray.data logger                           │
│                    level: DEBUG                              │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌─────────────────────┐    ┌─────────────────────────────┐ │
│  │   console handler   │    │      file handler           │ │
│  │   level: INFO       │    │   level: 未设置(继承DEBUG)   │ │
│  │                     │    │                             │ │
│  │ 只输出 INFO 及以上   │    │   输出所有 DEBUG 日志       │ │
│  └─────────────────────┘    └─────────────────────────────┘ │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### 4. 关键发现

| 组件 | 级别 | 说明 |
|------|------|------|
| `ray.data` logger | DEBUG | 允许所有 DEBUG 及以上日志通过 |
| console handler | INFO | 只在控制台显示 INFO 及以上 |
| file handler | 无(继承 DEBUG) | 写入所有 DEBUG 日志到文件 |

**正常情况下**：
- 控制台不应显示 DEBUG 日志
- `ray-data.log` 文件会包含 DEBUG 日志

### 5. TRACE 级别

Ray Data 还定义了比 DEBUG 更低的 TRACE 级别，用于高频调度循环日志：

```python
# 定义 TRACE 级别（DEBUG - 1 = 9）
logging.addLevelName(logging.DEBUG - 1, "TRACE")

# 使用方式
logger.log(logging.getLevelName("TRACE"), "Your message here.")
```

## 日志输出位置

### 日志文件路径

Ray Data 日志文件位于 Ray session 目录下：

```
{ray_session_dir}/logs/ray-data/ray-data.log
```

获取日志目录的代码：

```python
def get_log_directory() -> Optional[str]:
    global_node = ray._private.worker._global_node
    if global_node is None:
        return None
    session_dir = global_node.get_session_dir_path()
    return os.path.join(session_dir, "logs", "ray-data")
```

## 控制日志级别的方法

### 方法 1：通过 Python logging 模块（推荐）

在代码中动态修改 logger 级别：

```python
import logging

# 设置 ray.data logger 级别为 INFO
logging.getLogger("ray.data").setLevel(logging.INFO)
```

**优点**：简单直接，不需要配置文件

### 方法 2：自定义日志配置文件

创建 `my_ray_data_logging.yaml`：

```yaml
version: 1
disable_existing_loggers: False

formatters:
  ray:
    format: "%(asctime)s\t%(levelname)s %(filename)s:%(lineno)s -- %(message)s"

filters:
  console_filter:
    "()": "ray.data._internal.logging.HiddenRecordFilter"
  core_context_filter:
    "()": "ray._common.filters.CoreContextFilter"

handlers:
  file:
    class: ray.data._internal.logging.SessionFileHandler
    formatter: ray
    filename: ray-data.log
    level: INFO  # 添加此行，设置文件日志级别为 INFO
  console:
    class: ray._private.log.PlainRayHandler
    formatter: ray
    level: INFO
    filters: [console_filter]

loggers:
  ray.data:
    level: INFO  # 从 DEBUG 改为 INFO
    handlers: [file, console]
    propagate: False
```

然后设置环境变量：

```bash
export RAY_DATA_LOGGING_CONFIG=/path/to/my_ray_data_logging.yaml
```

### 方法 3：使用 JSON 格式日志

```bash
export RAY_DATA_LOG_ENCODING=JSON
```

**注意**：`RAY_DATA_LOG_ENCODING` 和 `RAY_DATA_LOGGING_CONFIG` 不能同时使用。

### 方法 4：全局 Ray 日志级别

```bash
export RAY_LOG_LEVEL=INFO
```

这会影响所有 Ray 组件的日志级别。

## 相关环境变量

| 环境变量 | 说明 |
|----------|------|
| `RAY_DATA_LOGGING_CONFIG` | 自定义日志配置文件路径 |
| `RAY_DATA_LOG_ENCODING` | 日志编码格式，可设为 `JSON` |
| `RAY_LOG_LEVEL` | 全局 Ray 日志级别 |

## 为什么控制台也可能显示 DEBUG 日志？

如果在控制台看到 DEBUG 日志，可能的原因：

1. **查看的是日志文件**：`ray-data.log` 文件默认记录 DEBUG 级别日志

2. **使用了自定义配置**：检查是否设置了 `RAY_DATA_LOGGING_CONFIG` 环境变量

3. **其他代码修改了 logger**：某些代码可能调用了 `logger.setLevel(logging.DEBUG)` 或添加了新的 handler

4. **第三方库干扰**：某些库可能会修改 root logger 的配置

## 调试建议

### 检查当前日志配置

```python
import logging

logger = logging.getLogger("ray.data")
print(f"Logger level: {logger.level} ({logging.getLevelName(logger.level)})")
print(f"Handlers: {logger.handlers}")
for handler in logger.handlers:
    print(f"  - {handler.__class__.__name__}: level={handler.level} ({logging.getLevelName(handler.level)})")
```

### 检查环境变量

```python
import os

print(f"RAY_DATA_LOGGING_CONFIG: {os.environ.get('RAY_DATA_LOGGING_CONFIG')}")
print(f"RAY_DATA_LOG_ENCODING: {os.environ.get('RAY_DATA_LOG_ENCODING')}")
print(f"RAY_LOG_LEVEL: {os.environ.get('RAY_LOG_LEVEL')}")
```

## 源码文件参考

| 文件 | 说明 |
|------|------|
| `python/ray/data/_internal/logging.py` | Ray Data 日志配置模块 |
| `python/ray/data/_internal/execution/streaming_executor_state.py` | Streaming Executor 状态管理，产生调度器日志 |
| `python/ray/data/_internal/execution/streaming_executor.py` | Streaming Executor 主模块 |

## 最佳实践

1. **生产环境**：使用 INFO 级别，通过自定义配置文件或代码设置
2. **调试时**：保持 DEBUG 级别，查看 `ray-data.log` 文件获取详细信息
3. **性能敏感场景**：考虑使用 WARNING 级别减少日志开销

## 总结

Ray Data 默认配置中：
- Logger 级别为 DEBUG
- Console handler 级别为 INFO（控制台不显示 DEBUG）
- File handler 无独立级别（继承 DEBUG，文件中会有 DEBUG 日志）

要完全禁止 DEBUG 日志，需要同时设置 logger 级别和 file handler 级别为 INFO。
