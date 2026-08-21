---
docId: fcAD4tuzf7qyx_XY52PQl8TYJ
title: "KDev-本地agentRunner执行器使用说明"
url: https://docs.corp.kuaishou.com/d/home/fcAD4tuzf7qyx_XY52PQl8TYJ
lastSync: 2026-08-21T20:31:27+08:00
---

# KDev 本地 AgentRunner 执行器使用说明

使用场景：用户自备运行环境，需要使用 kdev 流水线触发自动化脚本运行（用于替换 gitlab-ci 的能力——此功能因去商业化会逐步下线）

## 1. 环境要求

- 已安装 Python3
- Mac 或 Linux 环境
- **curl**
- **jq**（shell 环境 JSON 处理工具，安装方式: https://www.jianshu.com/p/dde911761）

```Shell
curl -s https://bs3-hb1.corp.kuaishou.com/upload-kdev-pipeline/kdev/kuaishou-build-tools/kdev-pipeline-agent-runner/kdev-agent-daemon.py -o kdev-agent-daemon.py
```

修改方法参考：https://blog.csdn.net/lgfun/article/details/108465062

## 2. 安装和初始化

**创建本地 work 目录**，比如 `/data/kdev-runner-workspace`

切换到本地 work 目录，下载 `kdev-agent-daemon.py`

在本地 work 目录，创建配置文件 `config.json`，内容如下：

| 字段 | 说明 |
|------|------|
| **id** | kdev 分配的 runner 的 id，联系管理员获取（请提供业务标识-英文简称，在插件选择运行环境时使用） |
| **token** | kdev 分配的 runner 的 token，联系管理员获取（gaojianren）（请提供业务标识-英文简称，在插件选择运行环境时使用） |
| **cleanDays** | 数据保留天数 |
| **cleanHours** | 数据保留小时数 |
| **cleanWhenFinish** | 当任务执行完成（成功或失败）时立即清理，格式：整形数字；当此字段配置大于0时生效 |

**id/token 获取方式**：可联系管理员添加 runner 类型，然后根据提供的接入类型对应实例管理页面（添加类型后，系统管理员会反馈给你管理页面）。运行实例可自行添加（添加后 runner 类型管理员会看到 id 和 token）。

agentRunner 类型是配置流水线任务时选择的运行环境，执行时会随机在此类型下机器上调度执行。

**agentRunner 实例管理页面**：https://kdev.corp.kuaishou.com/api/kdev/pipeline/agent/runner/admin

![](images/image-01-b341c628.png)

## 3. 启动

切换到本地 work 目录，执行如下命令启动：

1）Linux 下后台运行
2）Mac 下请使用**前台运行**（后台运行会阻塞）

**日志路径**：
- 启动日志：当前目录 `logs/daemon.log`
- runner 调度日志：`logs/runner.log`

**注意**：
- 第一次启动建议前台运行查看是否有报错，后续稳定后再后台运行（`python3 kdev-agent-daemon.py`）
- Mac 环境下后台运行会阻塞（问题待排查），需要打开 terminal 在前台运行

## 4. agentRunner 管理和监控

**管理地址**：https://kdev.corp.kuaishou.com/api/kdev/pipeline/agent/runner/admin

此页面可以看到：
- runner 节点心跳数据和执行任务数据
- 负责人可以查看 token 及管理节点（启用或禁用）

**报警机制**：超时 1 分钟会报警（当前报警间隔 1 分钟），禁用后不再检查心跳。

**处理报警**：如果出现报警无法立即处理，可以先禁用，等有空再启用（**先启用，后启动**）。

**清理异常执行记录**：如果内部有非正常结束的执行记录，可以把 workspace 目录下 `task_status/` 目录下对应的任务数据删除。

## 5. 使用方式

使用 **KDev 执行 Shell 脚本** 插件，运行环境选择 AgentRunner 对应的环境，脚本就是自定义的脚本内容。

---

## 6. 心跳超时排查指南

### 6.1 排查流程总览

```
心跳超时
├── 检查1: daemon 进程是否存活
│   └── ps aux | grep kdev-agent | grep -v grep
│       ├── 无进程 → 重启 daemon（见3.启动）
│       └── 有进程 → 继续检查2
├── 检查2: runner 日志是否在正常上报
│   └── tail -20 logs/runner.$(date +%Y-%m-%d).log
│       ├── 无日志或日志停滞 → 继续检查3
│       └── 正常上报 → 检查网络/管理端
├── 检查3: daemon 日志只有"更新kdev-agent-runner.py"无心跳
│   └── 手动执行 runner.py 查看报错
│       ├── ModuleNotFoundError: No module named 'requests' → 见6.2
│       ├── runner校验失败:AgentRunner未启用 → 先在管理页面启用，再重启
│       └── 进程卡在D状态（shutil.rmtree） → 见6.3
├── 检查4: workspace 是否在 CephFS 上
│   └── df -h $(pwd) 或 mount | grep ceph
│       ├── CephFS → 迁移到本地磁盘（/tmp 或 overlay），见6.3
│       └── 本地磁盘 → 继续检查5
└── 检查5: 是否存在多个 daemon 实例共用同一 runnerId
    └── ps aux | grep kdev-agent | grep -v grep
        └── 多个进程且 cwd 不同 → 停掉多余的，只保留一个
```

### 6.2 问题一：requests 模块缺失

**现象**：daemon 日志只有"更新kdev-agent-runner.py"，无心跳上报。手动执行 runner.py 报 `ModuleNotFoundError: No module named 'requests'`。

**根因**：`kdev-agent-runner.py` 依赖 `requests` 模块与 kdev API 通信。daemon 通过 `os.system('python3 kdev-agent-runner.py ...')` 执行，`os.system()` 失败不会抛异常，daemon 继续循环只打印"更新"日志，心跳不上报。

**为什么之前正常后来会缺**：`build-kuaishou-manylinux-wheel.sh` 脚本中有：

```bash
sudo ln -sf "/opt/python/${PYTHON}/bin/python3" /usr/local/bin/python3
```

每次构建任务执行时会**改写 `/usr/local/bin/python3` 软链接**。如果构建把 python3 指向了没装 requests 的版本（如 cp38、cp39），daemon 下次循环就会失败。且 daemon 是常驻进程内部循环，每次循环都新起 `python3` 子进程，软链接改了就受影响。

**修复**：给 `/opt/python/` 下所有 Python 版本都装上 requests：

```bash
for d in /opt/python/*/bin/python3; do
    $d -m pip install requests -q 2>/dev/null
done
```

**验证**：

```bash
for d in /opt/python/*/bin/python3; do
    echo -n "$d: "; $d -c "import requests; print(requests.__version__)" 2>&1
done
```

### 6.3 问题二：CephFS 上 shutil.rmtree 卡死

**现象**：runner 进程状态为 D（不可中断睡眠），日志最后停在"清理历史数据:XXXXXXX"。daemon 和 runner 同时停滞，心跳不再上报。

**根因**：runner.py 的 `clean_task_data` 函数使用 `shutil.rmtree` 删除过期目录。当 workspace 在 CephFS 上时，CephFS 的删除操作会卡住（进入 D 状态）。daemon 的 `os.system('python3 kdev-agent-runner.py ...')` 是同步阻塞调用，runner 卡住 = daemon 整个循环停滞。

**代码分析**（kdev-agent-runner.py 约370行）：

```python
if clean_time - last_time > 1800:
    log_file = open(kdev_clean_data_log_file, 'w')
    log_file.write(clean_time_str)
    log_file.close()
    try:
        clean_task_data(runner_config['cleanDays'])  # shutil.rmtree 在此卡死
    finally:
        os.remove(kdev_clean_data_log_file)  # 卡死时不会执行
```

**注意**：如果 daemon 被 `kill -9` 杀掉，`finally` 不会执行，`kdev_clean_data.log` 残留。下次启动满足 `clean_time - last_time > 1800`，又进入清理，又卡死——形成死循环。

**修复步骤**：

1. 杀掉卡死的进程：`kill -9 $(pgrep -f kdev-agent)`
2. 删除残留的清理锁文件：`rm -f kdev_clean_data.log`
3. **将 workspace 迁移到本地磁盘**（关键！）

**workspace 路径选择**：

| 路径 | 文件系统 | 推荐度 | 说明 |
|------|----------|--------|------|
| `/tmp/xxx/kdev-runner-workspace/` | overlay（本地磁盘） | 推荐 | 删除操作不会卡 |
| `/home/xxx/kdev-runner-workspace/` | CephFS | 不推荐 | 删除操作可能卡死 |

**确认文件系统类型**：

```bash
df -h $(pwd)     # overlay = 本地磁盘，ceph = CephFS
mount | grep ceph # 有输出说明在 CephFS 上
```

### 6.4 问题三：多个 daemon 实例冲突

**现象**：心跳时好时坏，管理端偶尔超时。

**根因**：两个 daemon 实例用同一个 runnerId 在不同目录启动，抢着上报心跳，后上报的覆盖先上报的，加上其中一个在 CephFS 上会卡死。

**排查**：

```bash
ps aux | grep kdev-agent | grep -v grep
for pid in $(pgrep -f kdev-agent-daemon); do
    echo "PID=$pid CWD=$(readlink /proc/$pid/cwd)"
done
```

**修复**：停掉多余的实例，只保留本地磁盘上的那份：

```bash
kill -9 $(pgrep -f kdev-agent-daemon)
cd /tmp/xxx/kdev-runner-workspace
nohup python3 kdev-agent-daemon.py > nohup.log 2>&1 &
```

### 6.5 问题四：bazel disk cache 未命中

**现象**：构建耗时从6分钟暴增到44分钟。

**根因**：`.bazelrc` 配了 `--incompatible_strict_action_env --action_env=RAY_BUILD_ENV`。`RAY_BUILD_ENV` 的值随 Python 版本变化：

- cp312 构建：`RAY_BUILD_ENV=manylinux_pycp312-cp312`
- cp311 构建：`RAY_BUILD_ENV=manylinux_pycp311-cp311`

`--incompatible_strict_action_env` 使得 `action_env` 中声明的环境变量值参与 action hash 计算。`RAY_BUILD_ENV` 值不同 → hash 不同 → cache miss。

**C++ 编译为何仍能命中**：C++ 编译 action 不引用 `RAY_BUILD_ENV`，hash 不受影响，所以 C++ 编译结果可以跨 Python 版本复用，这是正确且安全的。但链接和 Cython 扩展等下游 action 受影响，需要重新执行。

**说明**：这是正常行为，不同 Python 版本的 wheel 本就需要独立编译。如需加速多版本构建，可为每个 Python 版本维护独立的 cache 目录。

### 6.6 排查 Skill

**快速排查命令集**（可直接在远端执行）：

```bash
# === 基础状态检查 ===
echo "--- 进程状态 ---"
ps aux | grep kdev-agent | grep -v grep

echo "--- 工作目录 ---"
for pid in $(pgrep -f kdev-agent-daemon); do
    echo "PID=$pid CWD=$(readlink /proc/$pid/cwd 2>/dev/null)"
done

echo "--- 文件系统类型 ---"
df -h $(pwd)
mount | grep ceph && echo "WARNING: workspace on CephFS!" || echo "OK: local disk"

echo "--- 最近心跳 ---"
tail -3 logs/runner.$(date +%Y-%m-%d).log 2>/dev/null || echo "No runner log today"

echo "--- daemon 日志（排除更新） ---"
grep -v "更新kdev-agent-runner.py" logs/daemon.log | tail -5

# === requests 模块检查 ===
echo "--- requests 模块状态 ---"
for d in /opt/python/*/bin/python3; do
    echo -n "$d: "; $d -c "import requests; print('OK', requests.__version__)" 2>&1 || echo "MISSING"
done

# === 清理锁文件检查 ===
echo "--- 清理锁文件 ---"
ls -la kdev_clean_data.log 2>/dev/null && echo "WARNING: stale lock file exists, rm it!" || echo "OK: no lock file"

# === 软链接状态 ===
echo "--- python3 软链接 ---"
ls -la /usr/local/bin/python3
readlink -f /usr/local/bin/python3

# === 网络连通性 ===
echo "--- 网络连通性 ---"
timeout 5 curl -s -o /dev/null -w "HTTP %{http_code} %{time_total}s" https://kdev.corp.kuaishou.com 2>&1 || echo "FAILED: network issue"
```

### 6.7 部署最佳实践

1. **workspace 放在本地磁盘**（`/tmp/` 或 overlay），不要放在 CephFS 上
2. **所有 Python 版本都装 requests**，防止软链接切换后缺包
3. **只启动一个 daemon 实例**，不要在多个目录启动同一个 runnerId
4. **首次前台运行**确认无报错后再 `nohup` 后台运行
5. **cleanDays 设置合理值**（建议30），避免频繁触发清理
6. **重启前删除 `kdev_clean_data.log`**，防止残留锁文件导致重复卡死
7. **不同 Python 版本的构建** cache 不互用，属正常行为，无需修复
