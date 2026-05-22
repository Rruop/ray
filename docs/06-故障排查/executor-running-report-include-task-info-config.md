# Executor 侧 RUNNING 上报可配置带 task_info 分析与修复

## 问题现象

在生产环境 Ray 集群中，Dashboard 展示的 task 数量明显低于实际提交的 task 数量。
通过 `total_state_counts` API 可观察到 GCS buffer 中存在大量"僵尸 entry"——
这些 entry 有状态记录但缺失 `task_info`，导致 Dashboard 无法正常展示。

### 表现特征

```bash
curl -s "http://localhost:8265/api/v0/tasks/summarize" | python3 -c "
import json, sys
r = json.load(sys.stdin)['data']['result']
tsc = r.get('total_state_counts', {})
visible = r['num_after_truncation']
total = sum(tsc.values()) if tsc else 0
print(f'可见 task: {visible}, 全量 entry: {total}, 差值: {total - visible}')
"
# 差值 > 0 表示存在无 task_info 的僵尸 entry
```

典型差值在高并发场景下可达数千甚至数万。

---

## 问题根因分析

### Ray Task Event 上报机制

Ray 的 task 状态上报分为两个角色：

| 角色 | 上报时机 | 是否带 task_info |
|------|----------|-----------------|
| **Submitter**（提交方） | 创建 task 时 | ✅ 是 |
| **Executor**（执行方） | RUNNING / NIL(log) / NIL(debugger) | ❌ 否 |

### 僵尸 entry 产生原因

正常流程：
1. Submitter 提交 task → GCS 收到带 `task_info` 的事件 → 创建完整 entry
2. Executor 开始执行 → GCS 收到 RUNNING 状态更新 → 更新已有 entry

异常流程（导致僵尸 entry）：
1. Submitter 的上报消息丢失（网络抖动、GCS 重启、buffer 溢出）
2. Executor 的 RUNNING 上报先于 Submitter 到达 GCS（竞态）
3. GCS 仅收到 Executor 的状态更新，创建了无 `task_info` 的 entry
4. 该 entry 永远缺失 `task_info`，成为"僵尸"

### 代码层面定位

Executor 侧共有 4 处 `RecordTaskStatusEventIfNeeded` 调用，全部 `include_task_info=false`：

| 位置 | 文件:行号 | 状态 | 说明 |
|------|-----------|------|------|
| 1 | `core_worker.cc:2873` | RUNNING | executor 最早的上报点 |
| 2 | `core_worker.cc:4671` | NIL (log start) | 必然晚于 RUNNING |
| 3 | `core_worker.cc:4695` | NIL (log end) | 必然晚于 RUNNING |
| 4 | `core_worker.cc:4713` | NIL (debugger) | 必然晚于 RUNNING |

**关键点**：位置 1 是 executor 侧最早的上报，此时 GCS 可能还没收到 submitter 的 task_info。
如果此处带上 task_info，可以有效防止僵尸 entry 产生。

---

## 排查方案

### 方案对比

| 方案 | 优点 | 缺点 |
|------|------|------|
| A. Executor RUNNING 无条件带 task_info | 最简单 | 所有 task 都翻倍带宽 |
| B. GCS 侧补全缺失 task_info | 根本解决 | 改动大，需改 GCS 逻辑 |
| **C. 可配置开关（选用）** | 灵活、向后兼容、改动最小 | 需要用户主动开启 |

### 选择方案 C 的理由

1. **向后兼容**：默认 `false`，不改变现有行为
2. **最小改动**：仅 2 个文件、约 5 行代码
3. **可观测**：用户可根据实际差值决定是否开启
4. **Trade-off 明确**：开启后每个 task 的 task_info 会发送两次（submitter + executor）

---

## 排查过程

### Step 1: 确认问题范围

通过 `total_state_counts` API 确认 GCS buffer 中存在无 task_info 的 entry：
- Dashboard 可见 task 数 < `total_state_counts` 总和
- 差值即为僵尸 entry 数量

### Step 2: 代码追踪

追踪 `include_task_info` 参数的传递路径：

```
CoreWorker::ExecuteTask()
  → TaskEventBuffer::RecordTaskStatusEventIfNeeded()
    → 构建 TaskEvents protobuf
      → 根据 include_task_info 决定是否填充 task_info 字段
        → 发送给 GCS
```

确认 executor 侧 RUNNING 上报时 `include_task_info=false` 是硬编码。

### Step 3: 确认安全性

- RUNNING 上报是 executor 侧最早的状态事件
- 此时 `task_spec` 对象完整可用，包含完整 task_info
- 仅改此处，不影响后续 NIL 状态上报（它们依赖 RUNNING 已建立 entry）

### Step 4: 实施修改

1. 在 `ray_config_def.h` 新增配置项
2. 在 `core_worker.cc` 的 RUNNING 上报处读取配置

### Step 5: 格式验证

通过 code-simplifier 检查发现行长度超过 90 字符限制，进行了换行格式化修复。

---

## 修复内容

### 改动文件清单

| 文件 | 改动类型 | 行数 |
|------|----------|------|
| `src/ray/common/ray_config_def.h` | 新增配置项 | +7 行 |
| `src/ray/core_worker/core_worker.cc` | 读取配置 | +3/-1 行 |

### 改动 1: `src/ray/common/ray_config_def.h`

在 `task_events_max_num_profile_events_per_task` 之前新增：

```cpp
/// Whether the executor worker should include task_info when reporting
/// RUNNING status to GCS. When true, task_info is sent redundantly from
/// both submitter (at task creation) and executor (at RUNNING), reducing
/// the chance of "zombie" entries in GCS that lack task_info.
/// Trade-off: doubles per-task task_info bandwidth.
/// Default: false (preserve existing behavior).
RAY_CONFIG(bool, task_events_executor_include_task_info, false)
```

### 改动 2: `src/ray/core_worker/core_worker.cc:2873`

```cpp
// 改前:
/*include_task_info=*/false,

// 改后:
/*include_task_info=*/
    RayConfig::instance()
        .task_events_executor_include_task_info(),
```

完整上下文：

```cpp
RAY_UNUSED(
    task_event_buffer_->RecordTaskStatusEventIfNeeded(task_spec.TaskId(),
                                                      task_spec.JobId(),
                                                      task_spec.AttemptNumber(),
                                                      task_spec,
                                                      rpc::TaskStatus::RUNNING,
                                                      /*include_task_info=*/
                                                          RayConfig::instance()
                                                              .task_events_executor_include_task_info(),
                                                      update));
```

---

## 使用方式

### 方式 1: Python API

```python
ray.init(_system_config={"task_events_executor_include_task_info": True})
```

### 方式 2: 环境变量

```bash
export RAY_task_events_executor_include_task_info=true
```

### 方式 3: Ray Cluster YAML

```yaml
ray_params:
  _system_config:
    task_events_executor_include_task_info: true
```

---

## 验证方法

### 验证步骤

1. **启用前**：记录当前僵尸 entry 差值

```bash
curl -s "http://localhost:8265/api/v0/tasks/summarize" | python3 -c "
import json, sys
r = json.load(sys.stdin)['data']['result']
tsc = r.get('total_state_counts', {})
visible = r['num_after_truncation']
total = sum(tsc.values()) if tsc else 0
print(f'可见 task: {visible}, 全量 entry: {total}, 差值: {total - visible}')
"
```

2. **启用配置**：重启集群并设置 `task_events_executor_include_task_info=true`

3. **启用后**：运行相同工作负载，再次检查差值

### 预期结果

- 差值应显著缩小（接近 0）
- task_info 带宽翻倍（可通过 GCS 网络监控观察）
- Dashboard 展示的 task 数量与实际提交数量一致

### 注意事项

- 开启后每个 task 的 `task_info` 会从 submitter 和 executor 各发送一次
- 对于 task_info 较大的场景（如携带大量 runtime_env），需评估带宽影响
- 建议在出现明显僵尸 entry 问题的集群上启用

---

## 为什么只改 RUNNING 这一处

| 调用点 | 状态 | 是否改 | 原因 |
|--------|------|--------|------|
| `core_worker.cc:2873` | RUNNING | ✅ | executor 最早的上报，GCS 可能还没有 task_info |
| `core_worker.cc:4671` | NIL (log start) | ❌ | 必然晚于 RUNNING，此时 entry 已有 task_info |
| `core_worker.cc:4695` | NIL (log end) | ❌ | 同上 |
| `core_worker.cc:4713` | NIL (debugger) | ❌ | 同上 |

RUNNING 是 executor 生命周期中第一个状态上报，是唯一有必要冗余携带 task_info 的时机。
后续状态更新（NIL + log/debugger 附属信息）发生在 RUNNING 之后，
此时 GCS entry 已通过 RUNNING 或 submitter 获得 task_info，无需重复发送。

---

## 相关文档

- [Dashboard Task Count Low 根因分析](./dashboard-task-count-low-eviction-zombie-entry-analysis.md)
- [Dashboard total_state_counts 功能设计](../design/dashboard-total-state-counts-feature-design.md)

---

## 时间线

| 日期 | 事项 |
|------|------|
| 2026-05-22 | 完成 executor RUNNING 上报可配置 task_info 实现 |
