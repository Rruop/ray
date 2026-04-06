# Ray GCS Server 与 Ray Data 日志优化分析

## 一、现状

### 1.1 GCS Server 日志

在 ray_syncer 瓶颈场景下，GCS 日志增长极快：

| 指标 | 值 |
|------|-----|
| gcs_server.out 大小 | **101 GB**（运行 33 小时） |
| 磁盘写入量 | 109 GB（`/proc/pid/io` wchar） |
| "resources are not enough" | 1.17 亿次 |
| "Leasing worker" | ~8700 次/秒 |
| "Failed to lease" | ~4350 次/秒 |

**根因**：GCS 调度自旋产生海量 INFO 级别日志，且 GCS 日志轮转**默认关闭**。

### 1.2 Ray Data Worker 日志

1500+ 节点集群中，每个 worker 节点可能产生：
- worker-*.out / worker-*.err：Python worker 的 stdout/stderr
- raylet.out：raylet 日志
- dashboard_agent.log：agent 日志

虽然 Python Worker 默认启用了日志轮转（512MB × 10 个文件），但单个节点日志总量仍可达 5GB+。

---

## 二、Ray 日志轮转机制现状

### 2.1 各组件默认配置

| 组件 | log_rotation_max_bytes | log_rotation_backup_count | 轮转默认启用 |
|------|----------------------|--------------------------|------------|
| **GCS Server** | **0（禁用）** | 1 | **否** |
| **Raylet** | **0（禁用）** | 1 | **否** |
| Python Worker | 512MB | 10 | 是 |
| Dashboard | 512MB | 5 | 是 |
| Monitor | 512MB | 5 | 是 |
| Log Monitor | 512MB | 5 | 是 |

源码位置：
- GCS Server：`src/ray/gcs/gcs_server_main.cc:93-94`，硬编码 `log_rotation_max_size=0`
- Raylet：`src/ray/raylet/main.cc:218-219`，硬编码 `log_rotation_max_size=0`
- Python Worker：`python/ray/_private/node.py:156-159`，读取环境变量或使用默认值

### 2.2 日志轮转实现

C++ 端使用 spdlog 的 `rotating_file_sink_mt`：

```cpp
// src/ray/util/logging.cc:385-415
if (log_rotation_max_size_ == 0) {
  // 无轮转：basic_file_sink，文件无限增长
  file_sink = std::make_shared<spdlog::sinks::basic_file_sink_st>(log_filepath);
} else {
  // 有轮转：rotating_file_sink
  file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
      log_filepath, log_rotation_max_size_, log_rotation_file_num_);
}
```

**关键问题**：GCS 和 Raylet 的 `log_rotation_max_size` 在代码中**硬编码为 0**，无法通过配置参数或环境变量修改。

### 2.3 环境变量

| 环境变量 | 作用 | 默认值 |
|----------|------|--------|
| `RAY_ROTATION_MAX_BYTES` | Python 组件的日志轮转大小 | 512MB |
| `RAY_ROTATION_BACKUP_COUNT` | Python 组件的备份数量 | 5 |
| `RAY_BACKEND_LOG_LEVEL` | C++ 后端日志级别 | INFO |
| `RAY_LOG_LEVEL` | 全局 Ray 日志级别 | INFO |

**注意**：`RAY_ROTATION_MAX_BYTES` 只对 Python 组件生效，GCS/Raylet 的 C++ 代码在 `GetRayLogRotationMaxBytesOrDefault()` 中也读取此环境变量，但 `gcs_server_main.cc` 和 `raylet/main.cc` 在调用 `RayLog::Init()` 时传入了硬编码的 0，覆盖了环境变量。

### 2.4 日志压缩

**Ray 没有内置日志压缩机制**。轮转后的文件只是重命名（`file.log.1`, `file.log.2`），不会 gzip 压缩。

---

## 三、优化方案

### 3.1 立即可做：环境变量/配置层面

#### 方案 A：降低 GCS 日志级别

```bash
export RAY_BACKEND_LOG_LEVEL=WARNING
```

**效果**：
- GCS 的 INFO 级别日志不再输出，包括 "Leasing worker"、"Failed to lease"、"resources are not enough" 等
- 日志量下降 **99%+**（上述日志占总日志量的绝大部分）
- **风险**：会丢失调试信息，排查问题时需要临时调回 INFO

#### 方案 B：外部日志轮转（logrotate）

在宿主机或容器中配置 logrotate：

```bash
# /etc/logrotate.d/ray-gcs
/tmp/ray/session_*/logs/gcs_server.out {
    daily
    rotate 5
    maxsize 500M
    compress
    delaycompress
    missingok
    copytruncate
    notifempty
}

/tmp/ray/session_*/logs/raylet.out {
    daily
    rotate 3
    maxsize 200M
    compress
    delaycompress
    missingok
    copytruncate
    notifempty
}

/tmp/ray/session_*/logs/*worker*.out {
    daily
    rotate 3
    maxsize 200M
    compress
    delaycompress
    missingok
    copytruncate
    notifempty
}
```

**效果**：
- GCS 日志单文件不超过 500MB，最多保留 5 个轮转文件 = 总计 ~2.5GB
- 压缩后约节省 80% 磁盘空间
- **风险**：`copytruncate` 可能丢失少量日志（复制和截断之间的窗口期）；需要确保容器中有 logrotate 或通过 cronjob 调度

### 3.2 源码修改：中等难度

#### 方案 C：让 GCS/Raylet 支持配置化的日志轮转

修改 `gcs_server_main.cc` 和 `raylet/main.cc`，读取 Ray 配置参数而非硬编码 0：

```cpp
// 修改前（gcs_server_main.cc:93-94）：
/*log_rotation_max_size=*/0,
/*log_rotation_file_num=*/1

// 修改后：
/*log_rotation_max_size=*/RayConfig::instance().log_rotation_max_bytes(),
/*log_rotation_file_num=*/RayConfig::instance().log_rotation_backup_count()
```

然后在 `ray_config_def.h` 中已有默认值：
```cpp
RAY_CONFIG(int64_t, log_rotation_max_bytes, 100 * 1024 * 1024)  // 100MB
RAY_CONFIG(int64_t, log_rotation_backup_count, 5)
```

这样可以通过 `--system-config` 传入：
```python
ray.init(system_config={
    "log_rotation_max_bytes": 100 * 1024 * 1024,  # 100MB
    "log_rotation_backup_count": 5,
})
```

**效果**：GCS 日志单文件 100MB，最多 5 个轮转 = 总计 ~600MB（vs 当前无限制 101GB）
**风险**：需要修改 Ray 源码并重新编译

#### 方案 D：将高频调度日志改为 RAY_LOG_EVERY_MS

GCS 调度器中最"吵"的日志：

| 日志 | 源码位置 | 当前级别 | 频率 |
|------|---------|---------|------|
| "Leasing worker for actor" | `gcs_actor_scheduler.cc:243` | `RAY_LOG(INFO)` | ~8700/s |
| "Failed to lease...resources are not enough" | `gcs_actor_scheduler.cc:577` | `RAY_LOG(INFO)` | ~4350/s |
| "Finished leasing worker" | `gcs_actor_scheduler.cc:583` | `RAY_LOG(INFO)` | ~4350/s |

修改建议：

```cpp
// 修改前：
RAY_LOG(INFO) << "Leasing worker for actor.";

// 修改后：每秒最多输出一次
RAY_LOG_EVERY_MS(INFO, 1000) << "Leasing worker for actor.";

// 修改前：
RAY_LOG(INFO) << "Failed to lease worker from node " << node_id
              << " for actor " << actor->GetActorID()
              << " as the resources are not enough, job id = "
              << actor->GetActorID().JobId();

// 修改后：
RAY_LOG_EVERY_MS(WARNING, 1000) << "Failed to lease worker from node " << node_id
                                 << " for actor " << actor->GetActorID()
                                 << " as the resources are not enough, job id = "
                                 << actor->GetActorID().JobId();
```

**效果**：日志量从 ~13000 条/秒降到 ~3 条/秒，GCS 日志增长从 ~1MB/s 降到 ~1KB/s
**风险**：丢失单次调度的细粒度信息，但保留趋势信息；需要修改 Ray 源码

### 3.3 Ray Data Worker 日志优化

#### 方案 E：配置 Worker 日志轮转参数

Worker 日志轮转默认已启用（512MB × 10），但可通过环境变量调整：

```bash
export RAY_ROTATION_MAX_BYTES=104857600  # 100MB
export RAY_ROTATION_BACKUP_COUNT=3
```

**效果**：每个 worker 最多占用 ~400MB 日志空间（vs 默认 ~5GB）

#### 方案 F：配置 Ray Data 日志级别

```bash
export RAY_DATA_LOG_ENCODING=TEXT
# 通过 logging config
import ray
ray.init(logging_config=ray.LoggingConfig(log_level="WARNING"))
```

或针对 Ray Data 的特定 logger：

```python
import logging
logging.getLogger("ray.data").setLevel(logging.WARNING)
```

#### 方案 G：定期清理 dead worker 日志

Ray 的 `log_monitor.py` 会在 worker 死亡后将日志移到 `old/` 目录，但不会自动删除：

```python
# log_monitor.py:231-249
if not proc_alive:
    target = os.path.join(self.logs_dir, "old", os.path.basename(file_info.filename))
    shutil.move(file_info.filename, target)
```

可以增加一个定时清理脚本：

```bash
# 清理 old/ 目录中超过 1 天的日志
find /tmp/ray/session_latest/logs/old/ -type f -mtime +1 -delete

# 清理非当前 session 的日志
find /tmp/ray/ -maxdepth 1 -name "session_*" ! -name "$(readlink /tmp/ray/session_latest | xargs basename)" -type d -exec rm -rf {} +
```

### 3.4 架构层面优化

#### 方案 H：日志输出到 stdout + 外部采集

将 Ray 日志输出到 stdout，由 Kubernetes 的日志采集系统（如 Fluentd/Filebeat）统一收集和轮转：

```yaml
# Pod 环境变量
env:
  - name: RAY_LOG_TO_STDOUT
    value: "1"
```

**优势**：
- K8s 原生日志轮转（kubelet 管理，默认 10MB × 5）
- 统一日志采集管道
- 不占用容器磁盘

**风险**：需要确认 Ray 是否支持 `RAY_LOG_TO_STDOUT`（当前 C++ 组件可能不支持），可能需要修改源码

---

## 四、推荐方案优先级

| 优先级 | 方案 | 类型 | 效果 | 实施难度 |
|--------|------|------|------|---------|
| **P0** | B: logrotate | 运维配置 | GCS 日志从无限增长限制到 ~2.5GB | 低 |
| **P0** | G: 定期清理 dead worker 和旧 session 日志 | 运维脚本 | 回收磁盘空间 | 低 |
| **P1** | D: RAY_LOG_EVERY_MS 改造高频日志 | 源码修改 | 日志量降 99%+ | 中 |
| **P1** | C: GCS/Raylet 日志轮转配置化 | 源码修改 | 从根本上限制日志大小 | 中 |
| **P2** | E/F: Worker 日志轮转和级别调整 | 环境变量 | 减少单节点日志总量 | 低 |
| **P2** | A: 降低 GCS 日志级别 | 环境变量 | 立即生效但丢失调试信息 | 低 |
| **P3** | H: 日志输出到 stdout + K8s 采集 | 架构改造 | 统一管理，但改动大 | 高 |

### 推荐组合

**短期（无需改源码）**：方案 B + G + E + A

```bash
# 1. 降低 GCS 日志级别（仅 WARNING 及以上）
export RAY_BACKEND_LOG_LEVEL=WARNING

# 2. 配置 logrotate
cat > /etc/logrotate.d/ray << 'EOF'
/tmp/ray/session_*/logs/gcs_server.out {
    daily
    rotate 5
    maxsize 500M
    compress
    delaycompress
    copytruncate
}
/tmp/ray/session_*/logs/raylet.out {
    daily
    rotate 3
    maxsize 200M
    compress
    delaycompress
    copytruncate
}
EOF

# 3. Worker 日志轮转
export RAY_ROTATION_MAX_BYTES=104857600  # 100MB
export RAY_ROTATION_BACKUP_COUNT=3

# 4. 定期清理（cronjob）
echo '0 * * * * find /tmp/ray/session_latest/logs/old/ -type f -mtime +1 -delete 2>/dev/null; find /tmp/ray/ -maxdepth 1 -name "session_*" -mindepth 1 -not -samefile /tmp/ray/session_latest -type d -exec rm -rf {} + 2>/dev/null' | crontab -
```

**中期（需改源码）**：方案 C + D

1. 将 `gcs_server_main.cc` 和 `raylet/main.cc` 的 `log_rotation_max_size` 改为读取 `RayConfig`
2. 将 `gcs_actor_scheduler.cc` 中三处高频 `RAY_LOG(INFO)` 改为 `RAY_LOG_EVERY_MS(INFO, 1000)`

**预期效果**：

| 指标 | 优化前 | 短期方案后 | 中期方案后 |
|------|--------|-----------|-----------|
| GCS 日志大小（33h） | 101 GB | < 2.5 GB | < 500 MB |
| 日志写入速率 | ~1 MB/s | < 10 KB/s | < 1 KB/s |
| Worker 日志/节点 | ~5 GB | ~400 MB | ~400 MB |
| 磁盘 IO 压力 | 109 GB writes | < 1 GB | < 100 MB |

---

## 五、关键源码位置汇总

| 文件 | 行号 | 内容 |
|------|------|------|
| `src/ray/gcs/gcs_server_main.cc` | 93-94 | GCS 日志轮转硬编码为 0 |
| `src/ray/raylet/main.cc` | 218-219 | Raylet 日志轮转硬编码为 0 |
| `src/ray/common/ray_config_def.h` | 701-707 | log_rotation_max_bytes=100MB, backup_count=5 |
| `src/ray/util/logging.cc` | 296-325 | RAY_ROTATION_MAX_BYTES 环境变量读取 |
| `src/ray/util/logging.cc` | 385-415 | spdlog rotating_file_sink 实现 |
| `src/ray/gcs/actor/gcs_actor_scheduler.cc` | 243 | "Leasing worker" 高频日志 |
| `src/ray/gcs/actor/gcs_actor_scheduler.cc` | 577 | "Failed to lease" 高频日志 |
| `src/ray/gcs/actor/gcs_actor_scheduler.cc` | 583 | "Finished leasing" 高频日志 |
| `python/ray/_private/node.py` | 156-159 | Python Worker 日志轮转参数 |
| `python/ray/_common/ray_constants.py` | 4-5 | LOGGING_ROTATE_BYTES=512MB |
| `python/ray/_private/log_monitor.py` | 231-249 | Dead worker 日志移到 old/ |
| `python/ray/data/_internal/logging.py` | 62-63 | Ray Data 日志配置 |
| `src/ray/gcs/gcs_server.cc` | 309-312 | event_stats_print_interval_ms 定时器注册 |
| `src/ray/gcs/gcs_server.cc` | 919-930 | PrintDebugState 输出 Event Stats |

---

## 六、具体日志轮转方案

### 6.1 方案一：源码修改——GCS/Raylet 日志轮转配置化（推荐）

**改动最小，效果最彻底。**

#### 6.1.1 修改文件

**文件1：`src/ray/gcs/gcs_server_main.cc:87-94`**

```cpp
// 修改前：
InitShutdownRAII ray_log_shutdown_raii(ray::RayLog::StartRayLog,
                                       ray::RayLog::ShutDownRayLog,
                                       argv[0],
                                       ray::RayLogLevel::INFO,
                                       /*log_filepath=*/"",
                                       /*err_log_filepath=*/"",
                                       /*log_rotation_max_size=*/0,
                                       /*log_rotation_file_num=*/1);

// 修改后：
InitShutdownRAII ray_log_shutdown_raii(ray::RayLog::StartRayLog,
                                       ray::RayLog::ShutDownRayLog,
                                       argv[0],
                                       ray::RayLogLevel::INFO,
                                       /*log_filepath=*/"",
                                       /*err_log_filepath=*/"",
                                       /*log_rotation_max_size=*/ray::RayLog::GetRayLogRotationMaxBytesOrDefault(),
                                       /*log_rotation_file_num=*/ray::RayLog::GetRayLogRotationBackupCountOrDefault());
```

**文件2：`src/ray/raylet/main.cc:213-220`** — 同样修改

```cpp
// 修改前：
/*log_rotation_max_size=*/0,
/*log_rotation_file_num=*/1

// 修改后：
/*log_rotation_max_size=*/ray::RayLog::GetRayLogRotationMaxBytesOrDefault(),
/*log_rotation_file_num=*/ray::RayLog::GetRayLogRotationBackupCountOrDefault()
```

#### 6.1.2 使用方式

修改后，GCS 和 Raylet 的日志轮转通过环境变量控制：

```bash
# 启动 Ray 时设置
export RAY_ROTATION_MAX_BYTES=104857600   # 100MB，单文件上限
export RAY_ROTATION_BACKUP_COUNT=5        # 最多保留 5 个轮转文件

ray start --head --system-config='{"raylet_report_resources_period_milliseconds": 5000, ...}'
```

#### 6.1.3 效果

| 组件 | 单文件上限 | 轮转文件数 | 最大磁盘占用 |
|------|-----------|-----------|-------------|
| GCS Server | 100MB | 5 | **500MB**（vs 101GB 无限制） |
| Raylet | 100MB | 5 | **500MB**（vs 无限制） |

轮转文件命名为 `gcs_server.out.1`, `gcs_server.out.2`, ..., `gcs_server.out.5`，由 spdlog 自动管理。

#### 6.1.4 约束

- `RAY_ROTATION_MAX_BYTES=0` 等价于禁用轮转（保持向后兼容）
- `RAY_ROTATION_BACKUP_COUNT` 必须 > 0，否则默认为 1
- RayConfig 中的 `log_rotation_max_bytes`（100MB）和 `log_rotation_backup_count`（5）与此无关，它们是给 Python 组件用的；C++ 组件走 `GetRayLogRotationMaxBytesOrDefault()` 路径

### 6.2 方案二：外部 logrotate（无需改源码）

适用场景：无法修改 Ray 源码时。

#### 6.2.1 Head 节点 logrotate 配置

```bash
cat > /etc/logrotate.d/ray-head << 'EOF'
/tmp/ray/session_*/logs/gcs_server.out {
    hourly
    rotate 5
    maxsize 500M
    compress
    delaycompress
    missingok
    copytruncate
    notifempty
    dateext
    dateformat -%Y%m%d%H
}
/tmp/ray/session_*/logs/monitor.log {
    daily
    rotate 3
    maxsize 200M
    compress
    delaycompress
    missingok
    copytruncate
}
/tmp/ray/session_*/logs/dashboard.log {
    daily
    rotate 3
    maxsize 200M
    compress
    delaycompress
    missingok
    copytruncate
}
EOF
```

#### 6.2.2 Worker 节点 logrotate 配置

```bash
cat > /etc/logrotate.d/ray-worker << 'EOF'
/tmp/ray/session_*/logs/raylet.out {
    hourly
    rotate 3
    maxsize 200M
    compress
    delaycompress
    missingok
    copytruncate
}
/tmp/ray/session_*/logs/worker-*.out {
    daily
    rotate 2
    maxsize 200M
    compress
    delaycompress
    missingok
    copytruncate
    wildcardcachesize 2000
}
/tmp/ray/session_*/logs/dashboard_agent.log {
    daily
    rotate 2
    maxsize 100M
    compress
    delaycompress
    missingok
    copytruncate
}
EOF
```

#### 6.2.3 定期清理脚本

```bash
cat > /usr/local/bin/ray-log-cleanup.sh << 'SCRIPT'
#!/bin/bash
# 清理 old/ 目录中超过 1 天的 dead worker 日志
find /tmp/ray/session_latest/logs/old/ -type f -mtime +1 -delete 2>/dev/null

# 清理非当前 session 的日志目录
current_session=$(readlink /tmp/ray/session_latest 2>/dev/null | xargs basename 2>/dev/null)
if [ -n "$current_session" ]; then
    find /tmp/ray/ -maxdepth 1 -name "session_*" -mindepth 1 -type d \
        ! -name "$current_session" -exec rm -rf {} + 2>/dev/null
fi

# 清理过大的压缩轮转文件
find /tmp/ray/session_latest/logs/ -name "*.gz" -mtime +3 -delete 2>/dev/null
SCRIPT
chmod +x /usr/local/bin/ray-log-cleanup.sh

# 添加 cronjob，每小时执行
echo '0 * * * * /usr/local/bin/ray-log-cleanup.sh' | crontab -
```

#### 6.2.4 注意事项

- **`copytruncate` 的风险**：复制文件和截断之间有微小窗口，可能丢失少量日志。对于 GCS 这种高频写入场景，丢失概率极低但存在
- **容器环境**：需要确认容器中有 `logrotate` 命令，或者通过 K8s CronJob 在宿主机执行
- **`hourly` vs `daily`**：GCS 在瓶颈场景下每秒写入 ~1MB，500MB 只能撑 ~8 分钟。建议对 GCS 使用 `maxsize 500M` + `hourly` 双重限制

### 6.3 方案三：KML Pod 启动脚本中嵌入轮转逻辑

在 KML Pod 的 entrypoint 脚本中加入日志轮转逻辑：

```bash
# 在 ray start 之前，后台启动日志轮转守护进程
(
    while true; do
        sleep 300  # 每 5 分钟检查
        for f in /tmp/ray/session_latest/logs/gcs_server.out; do
            if [ -f "$f" ]; then
                size=$(stat -c%s "$f" 2>/dev/null || echo 0)
                if [ "$size" -gt 524288000 ]; then  # 500MB
                    # 轮转：移除最老的，依次重命名
                    [ -f "$f.4.gz" ] && rm -f "$f.4.gz"
                    [ -f "$f.3.gz" ] && mv "$f.3.gz" "$f.4.gz"
                    [ -f "$f.2.gz" ] && mv "$f.2.gz" "$f.3.gz"
                    [ -f "$f.1.gz" ] && mv "$f.1.gz" "$f.2.gz"
                    # 压缩当前轮转文件
                    if [ -f "$f.1" ]; then
                        gzip -c "$f.1" > "$f.2.gz" && rm -f "$f.1"
                    fi
                    # 复制并截断当前日志
                    cp "$f" "$f.1" && truncate -s 0 "$f"
                    gzip "$f.1" &  # 后台压缩
                fi
            fi
        done
    done
) &
```

**优势**：不依赖 logrotate，纯 shell 实现，适合容器环境
**劣势**：轮转逻辑不够健壮，极端场景可能丢日志

---

## 七、`event_stats` 与 `event_stats_print_interval_ms` 分析

### 7.1 `event_stats` 配置

源码（`ray_config_def.h:24-25`）：

```cpp
/// Whether to enable Ray event stats collection.
RAY_CONFIG(bool, event_stats, true)
```

**`event_stats` 默认值是 `true`，不是 `false`。** 这意味着 Event Stats 统计和打印**默认启用**。

`event_stats` 可通过以下方式关闭：

```python
# 方式一：通过 system_config（推荐）
ray start --head --system-config='{"event_stats": false}'

# 方式二：在 ray.init 中
ray.init(system_config={"event_stats": False})
```

### 7.2 `event_stats_print_interval_ms` 配置

源码（`ray_config_def.h:44-48`）：

```cpp
/// The interval of periodic event loop stats print.
/// -1 means the feature is disabled. In this case, stats are available
/// in the associated process's log file.
/// NOTE: This requires event_stats=1.
RAY_CONFIG(int64_t, event_stats_print_interval_ms, 60000)
```

**默认值 60000ms（60 秒）。** 注意注释中的 "requires event_stats=1" 是指旧版环境变量 `RAY_event_stats=1` 的行为，在当前版本中 `event_stats` 默认就是 `true`。

### 7.3 Event Stats 打印的生效条件

Event Stats 打印需要**同时满足两个条件**：

```cpp
// gcs_server.cc:919-921
const auto event_stats_print_interval_ms =
    RayConfig::instance().event_stats_print_interval_ms();
if (event_stats_print_interval_ms != -1 && RayConfig::instance().event_stats()) {
    // 打印 Event Stats
}
```

1. `event_stats_print_interval_ms != -1`（默认 60000，满足）
2. `RayConfig::instance().event_stats() == true`（**默认 true，满足**）

**由于两个条件默认都满足，Event Stats 默认就会打印到日志。**

### 7.4 如何关闭 event_stats

#### 完全关闭（推荐）

同时关闭 `event_stats` 和设置 `event_stats_print_interval_ms=-1`：

```python
ray start --head --system-config='{
    "event_stats": false,
    "event_stats_print_interval_ms": -1
}'
```

效果：
- `event_stats=false`：关闭 Event Stats 统计收集，`PrintDebugState()` 第二部分不打印
- `event_stats_print_interval_ms=-1`：`PrintDebugState()` 定时器不注册，第一部分（Gcs Debug state）也不打印
- Raylet 的 `[state-dump]` 周期性输出也停止
- CoreWorker 的 Event Stats 输出也停止

**注意**：`event_stats=false` 还会影响 `emit_main_service_metrics`，关闭后 `io_context_event_loop_lag_ms` 等 Prometheus 指标也不再采集。如果需要保留指标采集但只关闭日志打印，使用方式二。

#### 仅关闭日志打印，保留指标采集

```python
ray start --head --system-config='{
    "event_stats": true,
    "emit_main_service_metrics": true,
    "event_stats_print_interval_ms": -1
}'
```

效果：
- Event Stats 统计仍然收集
- `io_context_event_loop_lag_ms` 等指标仍可从 Prometheus 查询
- 但不再打印到日志文件

#### 仅降低打印频率

```python
ray start --head --system-config='{
    "event_stats_print_interval_ms": 600000
}'
```

效果：从每 60 秒打印一次改为每 10 分钟打印一次。

### 7.5 GCS 的 PrintDebugState 行为详解

`PrintDebugState()` 在 GCS 中由定时器驱动：

```cpp
// gcs_server.cc:309-312
periodical_runner_->RunFnPeriodically(
    [this] { PrintDebugState(); },
    /*ms*/ RayConfig::instance().event_stats_print_interval_ms(),
    "GCSServer.deadline_timer.debug_state_event_stats_print");
```

**定时器注册条件**：当 `event_stats_print_interval_ms > 0` 时注册（默认 60000，满足）。设为 -1 时不注册。

`PrintDebugState()` 内部分两部分：

```cpp
void GcsServer::PrintDebugState() const {
    // 第一部分：始终打印（不受 event_stats 控制）
    // 包含 8 个 manager 的 DebugString，约 50-100 行
    RAY_LOG(INFO) << "Gcs Debug state:\n\n"
                  << gcs_node_manager_->DebugString() << "\n\n"
                  << gcs_actor_manager_->DebugString() << "\n\n"
                  << gcs_resource_manager_->DebugString() << "\n\n"
                  << gcs_placement_group_manager_->DebugString() << "\n\n"
                  << gcs_publisher_->DebugString() << "\n\n"
                  << runtime_env_manager_->DebugString() << "\n\n"
                  << gcs_task_manager_->DebugString() << "\n\n"
                  << gcs_autoscaler_state_manager_->DebugString() << "\n\n";

    // 第二部分：仅当 event_stats=true 且 event_stats_print_interval_ms != -1 时打印
    // 包含各 io_context 的 StatsString，约 40-60 行
    const auto event_stats_print_interval_ms =
        RayConfig::instance().event_stats_print_interval_ms();
    if (event_stats_print_interval_ms != -1 && RayConfig::instance().event_stats()) {
        RAY_LOG(INFO) << "Main service Event stats:\n\n"
                      << io_context_provider_.GetDefaultIOContext().stats()->StatsString()
                      << "\n\n";
        for (const auto &io_context : io_context_provider_.GetAllDedicatedIOContexts()) {
            RAY_LOG(INFO) << io_context->GetName() << " Event stats:\n\n"
                          << io_context->GetIoService().stats()->StatsString() << "\n\n";
        }
    }
}
```

### 7.6 Raylet 的 event_stats 行为

Raylet 的定时器注册**受 event_stats 控制**（与 GCS 不同）：

```cpp
// node_manager.cc:434-446
const auto event_stats_print_interval_ms =
    RayConfig::instance().event_stats_print_interval_ms();
if (event_stats_print_interval_ms != -1 && RayConfig::instance().event_stats()) {
    periodical_runner_->RunFnPeriodically(
        [this] {
            std::stringstream debug_msg;
            debug_msg << DebugString() << "\n\n";
            RAY_LOG(INFO) << PrependToEachLine(debug_msg.str(), "[state-dump] ");
            ReportWorkerOOMKillStats();
        },
        event_stats_print_interval_ms,
        "NodeManager.deadline_timer.print_event_loop_stats");
}
```

**Raylet 的 `[state-dump]` 输出需要 `event_stats=true` 才会注册定时器**，关闭 `event_stats=false` 即可停止。

### 7.7 各组件 event_stats 影响总结

| 组件 | 定时器注册条件 | 第一部分（DebugState） | 第二部分（Event Stats） |
|------|--------------|---------------------|---------------------|
| **GCS** | `event_stats_print_interval_ms != -1` | 始终打印（不受 event_stats 控制） | 需要 event_stats=true |
| **Raylet** | `event_stats_print_interval_ms != -1 && event_stats=true` | 受 event_stats 控制 | 受 event_stats 控制 |
| **CoreWorker** | `event_stats_print_interval_ms != -1 && event_stats=true` | 受 event_stats 控制 | 受 event_stats 控制 |

**关键区别**：GCS 的 `PrintDebugState()` 定时器不受 `event_stats` 控制，只受 `event_stats_print_interval_ms` 控制。

### 7.8 Event Stats 对日志量的影响

每次 `PrintDebugState()` 输出约 **100-200 行**，默认每 60 秒一次 = **每小时 6000-12000 行**。

组成部分：
- Gcs Debug state（8 个 manager 的 DebugString）：约 50-100 行
- Main service Event stats：约 10-20 行
- task_io_context Event stats：约 10 行
- pubsub_io_context Event stats：约 10 行
- ray_syncer_io_context Event stats：约 10 行
- ray_event_io_context Event stats：约 5 行

### 7.9 当前集群确认

从观测数据看，当前集群的 GCS 日志中有 Event Stats 输出：

```
[2026-05-09 15:42:01,138 I 74 74] (gcs_server) gcs_server.cc:926: ray_syncer_io_context Event stats:
Global stats: 14450955 total (2196 active)
...
RaySyncer.BroadcastMessage - 64279 total (0 active), Execution time: mean = 1.44ms
```

符合预期——`event_stats` 默认为 `true`，Event Stats 默认启用。

### 7.10 日志量估算

| 配置 | Gcs Debug state 频率 | Event Stats 频率 | 日志量/小时 |
|------|---------------------|-----------------|-----------|
| 默认（60s, event_stats=true） | 60 次 | 60 次 | ~10000 行 |
| 600s, event_stats=true | 10 次 | 10 次 | ~1700 行 |
| event_stats=false, interval=-1 | 0 次 | 0 次 | **0 行** |
| event_stats=true, interval=-1 | 0 次 | 0 次 | **0 行** |
| event_stats=false, interval=60s | 60 次（GCS only） | 0 次 | ~6000 行（仅 DebugState） |

**注意**：设 `event_stats_print_interval_ms=-1` 后 GCS 的 `PrintDebugState()` 定时器不注册，两部分都不输出，效果是 0 行。

### 7.11 推荐配置

| 场景 | 推荐配置 |
|------|---------|
| **正常生产（推荐）** | `event_stats=false, event_stats_print_interval_ms=-1` — 完全关闭，零日志输出 |
| 需要 Prometheus 指标 | `event_stats=true, emit_main_service_metrics=true, event_stats_print_interval_ms=-1` — 保留指标采集，关闭日志打印 |
| 排查问题时 | `event_stats=true, event_stats_print_interval_ms=10000` — 每 10 秒打印 |
| 压力测试 | `event_stats=true, event_stats_print_interval_ms=5000` — 每 5 秒打印 |

**结论**：在当前 1500+ 节点瓶颈场景下，GCS 日志的主要来源是调度自旋（~13000 条/秒），Event Stats 只占极小比例（~0.17 条/秒），所以关闭 event_stats 的日志收益相对有限。**但作为优化的一部分仍然建议关闭**，优先级应放在日志轮转配置化和高频日志 `RAY_LOG_EVERY_MS` 改造上。

**结论**：在当前 1500+ 节点瓶颈场景下，GCS 日志的主要来源是调度自旋（~13000 条/秒），Event Stats 只占极小比例（~0.17 条/秒），所以关闭 event_stats 的日志收益相对有限。**但作为优化的一部分仍然建议关闭**，优先级应放在日志轮转配置化和高频日志 `RAY_LOG_EVERY_MS` 改造上。
