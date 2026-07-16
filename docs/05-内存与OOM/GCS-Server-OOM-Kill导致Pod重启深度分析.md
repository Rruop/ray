# GCS Server OOM Kill 导致 Head 节点重启深度分析

## 1. 故障现象

Ray head 节点发生重启，Pod 对象仍在，但 PID 1 进程已重启，产生了新的 Ray session。

启动命令：
```bash
/usr/bin/python3 /usr/local/bin/ray start --head --block \
  --dashboard-agent-listen-port=0 \
  --dashboard-host=0.0.0.0 \
  --memory=134049339904 \
  --num-cpus=0 \
  --num-gpus=1 \
  --resources={"head":1} \
  --system-config={"raylet_report_resources_period_milliseconds":1000}
```

## 2. 排查路径

### 2.1 确认 Ray 是否重启

```bash
ls -lt /tmp/ray/
```

输出显示当前 session 是 `session_2026-07-22_12-56-47_862760_1`，说明今天 12:56 有新的 Ray cluster 启动。之前的 session 是 4 月份的，中间长时间无 session，说明旧集群在 12:56 之前已停。

### 2.2 GCS 日志看启动

```bash
head -50 /tmp/ray/session_latest/logs/gcs_server.out
```

看到 12:56:47 GCS 重新启动，且大量 `Wrong cluster ID token` 报错——新 cluster ID 与旧 worker 缓存的 cluster ID 不匹配：

```
[2026-07-22 12:56:49,790 W 12 142] (gcs_server) server_call.h:228: Wrong cluster ID token in request!
Expected: f6a3defe4ca13e61236df0892ecf3b400488df8317c460e440067a53,
but got: a79928aa1aa030f3ac0b34b0e5aa9e05faa4d94393083f67772029ad
```

### 2.3 关键一步：查 dmesg

```bash
dmesg -T | tail -30
```

直接看到内核 OOM kill 记录：

```
[Wed Jul 22 12:56:37 2026] Memory cgroup out of memory: Killed process 2020843 (gcs_server) total-vm:166771440kB, anon-rss:130415492kB, file-rss:532kB, shmem-rss:0kB
[Wed Jul 22 12:56:48 2026] oom_reaper: reaped process 2020843 (gcs_server), now anon-rss:0kB, file-rss:0kB
```

### 2.4 dmesg 常用排查命令

```bash
# 查最近的内核日志（OOM、crash 等）
dmesg -T | tail -50

# 只看 OOM 相关
dmesg -T | grep -i "oom\|out of memory\|killed process"

# 只看今天的时间段
dmesg -T | grep "Jul 22"

# 按进程名过滤
dmesg -T | grep "gcs_server\|raylet\|ray"
```

> `-T` 参数把时间戳转成人类可读格式，不加的话显示的是启动后的秒数。

## 3. dmesg OOM 日志逐字段解读

```
Memory cgroup out of memory: Killed process 2020843 (gcs_server) total-vm:166771440kB, anon-rss:130415492kB, file-rss:532kB, shmem-rss:0kB
```

| 字段 | 值 | 含义 |
|---|---|---|
| `Wed Jul 22 12:56:37 2026` | 时间 | OOM kill 发生的精确时间 |
| `Memory cgroup out of memory` | OOM 类型 | **cgroup 级别**的内存超限（非整机物理内存不足，而是容器/pod 的 memory limit 被突破） |
| `Killed process 2020843` | 被杀 PID | 内核 OOM Killer 选中的进程 |
| `(gcs_server)` | 进程名 | Ray 的 GCS 服务 |
| `total-vm:166771440kB` | ~159GB | 进程总虚拟内存（含共享库、映射文件等，不代表真实物理占用） |
| `anon-rss:130415492kB` | **~124GB** | 匿名页实际物理内存——这是真正的内存消耗 |
| `file-rss:532kB` | ~0.5MB | 文件映射页，可忽略 |
| `shmem-rss:0kB` | 0 | 共享内存占用，可忽略 |

**关键结论**：`anon-rss:130415492kB`（~124GB）说明 GCS server 内存膨胀到约 124GB，超出了 cgroup 设定的内存 limit，被内核 OOM Killer 杀掉。这是**容器级限制**，不是机器物理内存不够。

## 4. 容器退出与重启机制

### 4.1 容器生命周期由 PID 1 决定

- **PID 1 退出** → 容器退出 → K8s 根据 restart policy 重启容器
- **PID 1 存活** → 容器存活，即使其他子进程挂了

在本场景中，`ray start --head --block` 是 PID 1，它的 `os._exit(1)` 导致容器退出。K8s 的 Pod 不会重建，只是容器重启（restart count +1）。

### 4.2 同一个 Pod，新容器

Pod 对象没变，但容器进程是全新启动的，所以产生了新的 Ray session（`session_2026-07-22_12-56-47`）。旧的 worker 节点因为 cluster ID 不匹配，需要重新注册。

### 4.3 为什么 Ray 不自动拉起挂掉的 gcs_server

Ray 选择了"fail fast + 整体退出"策略，把重启决策交给了上层（K8s）来做。这不是设计疏漏，而是有意为之——GCS 是集群的核心状态存储，它挂了之后集群状态已不可信，强行拉起可能导致数据不一致。

## 5. `ray start --head --block` 完整代码逻辑

### 5.1 `--block` 标志定义

文件：`python/ray/scripts/scripts.py:378-383`

```python
@click.option(
    "--block",
    is_flag=True,
    default=False,
    help="provide this argument to block forever in this command."
    "Process exit logs will be saved to ray_process_exit.log in the logs directory.",
)
```

### 5.2 Node 构造传入参数

文件：`python/ray/scripts/scripts.py:~647-648`（head 节点）

```python
node = ray._private.node.Node(
    ray_params, head=True, shutdown_at_exit=block, spawn_reaper=block
)
```

- `shutdown_at_exit=block`：注册 atexit handler 和 SIGTERM handler，退出时清理所有子进程
- `spawn_reaper=block`：启动 reaper 进程，作为孤儿进程清理的安全网

### 5.3 主循环：while True 轮询

文件：`python/ray/scripts/scripts.py:1173-1223`

```python
if block:
    logs_dir = node.get_logs_dir_path()
    process_exit_log_path = os.path.join(logs_dir, "ray_process_exit.log")
    cli_logger.newline()
    with cli_logger.group(cf.bold("--block")):
        cli_logger.print(
            "This command will now block forever until terminated by a signal."
        )
        cli_logger.print(
            "Running subprocesses are monitored and a message will be "
            "printed if any of them terminate unexpectedly. Subprocesses "
            "exit with SIGTERM will be treated as graceful, thus NOT reported."
        )
        cli_logger.print(
            "Process exit logs will be saved to: {}", cf.bold(process_exit_log_path)
        )
        cli_logger.flush()
        try:
            process_exit_logger = setup_process_exit_logger(process_exit_log_path)
        except Exception as e:
            cli_logger.warning("Failed to init process exit logger: {}", e)
            process_exit_logger = None

    while True:
        time.sleep(1)
        deceased = node.dead_processes()

        expected_return_codes = [
            0,
            signal.SIGTERM,
            -1 * signal.SIGTERM,
            128 + signal.SIGTERM,
        ]
        unexpected_deceased = [
            (process_type, process)
            for process_type, process in deceased
            if process.returncode not in expected_return_codes
        ]
        if len(unexpected_deceased) > 0:
            cli_logger.newline()
            cli_logger.error("Some Ray subprocesses exited unexpectedly:")

            lines_for_file = []
            with cli_logger.indented():
                for process_type, process in unexpected_deceased:
                    cli_logger.error(
                        "{}",
                        cf.bold(str(process_type)),
                        _tags={"exit code": str(process.returncode)},
                    )
                    rc = getattr(process, "returncode", None)
                    rc_str = format_returncode(rc)
                    lines_for_file.append(f"  {process_type} [exit code={rc_str}]")
            try:
                file_msg = (
                    "Some Ray subprocesses exited unexpectedly:\n"
                    + "\n".join(lines_for_file)
                )
                process_exit_logger.error("%s", file_msg)
            except Exception as e:
                cli_logger.warning("Failed to write process exit log: {}", e)

            cli_logger.newline()
            cli_logger.error("Remaining processes will be killed.")

            # explicitly kill all processes since atexit handlers
            # will not exit with errors.
            node.kill_all_processes(check_alive=False, allow_graceful=False)
            os._exit(1)
```

### 5.4 dead_processes() 实现

文件：`python/ray/_private/node.py:1731-1745`

遍历 `all_processes` 字典（key 是进程类型如 `gcs_server`、`raylet`、`dashboard`、`monitor`、`log_monitor`，value 是 ProcessInfo 列表），对每个子进程调用 `process.poll()`：

```python
def dead_processes(self):
    """Return a list of the dead processes.

    Note that this ignores processes that have been explicitly killed,
    e.g., via a command like node.kill_raylet().

    Returns:
        A list of the dead processes ignoring the ones that have
            been explicitly killed.
    """
    result = []
    for process_type, process_infos in self.all_processes.items():
        for process_info in process_infos:
            if process_info.process.poll() is not None:
                result.append((process_type, process_info.process))
    return result
```

- `poll()` 返回 `None` → 进程还活着，跳过
- `poll()` 返回退出码 → 进程已死，加入返回列表

### 5.5 意外退出判定逻辑

```python
expected_return_codes = [
    0,                    # 正常退出
    signal.SIGTERM,       # 15 — `ray stop` 发的信号
    -1 * signal.SIGTERM,  # -15
    128 + signal.SIGTERM, # 143 (shell 约定: 128+signal)
]
```

| 退出码 | 含义 | 是否预期 |
|--------|------|---------|
| 0 | 正常退出 | ✅ |
| 15 / -15 | SIGTERM | ✅ （ray stop 发的优雅关闭） |
| 143 (128+15) | shell 约定 SIGTERM | ✅ |
| **-9** | **SIGKILL（OOM Kill）** | **❌ 意外退出** |
| 137 (128+9) | shell 约定 SIGKILL | ❌ 意外退出 |
| 其他非零值 | 异常退出 | ❌ 意外退出 |

OOM Kill 发的是 **SIGKILL (9)**，退出码为 `-9`，不在预期列表中 → 被判定为意外退出。

### 5.6 kill_all_processes() 实现

文件：`python/ray/_private/node.py:1664-1718`

杀进程有**严格顺序**：

```python
def kill_all_processes(self, check_alive=True, allow_graceful=False, wait=False):
    # 1. raylet 先杀 — 让它有机会优雅清理 worker 进程
    if ray_constants.PROCESS_TYPE_RAYLET in self.all_processes:
        self._kill_process_type(
            ray_constants.PROCESS_TYPE_RAYLET,
            check_alive=check_alive,
            allow_graceful=allow_graceful,
            wait=wait,
        )

    # 2. gcs_server 第二杀
    if ray_constants.PROCESS_TYPE_GCS_SERVER in self.all_processes:
        self._kill_process_type(
            ray_constants.PROCESS_TYPE_GCS_SERVER,
            check_alive=check_alive,
            allow_graceful=allow_graceful,
            wait=wait,
        )

    # 3. 其余进程（dashboard、monitor、log_monitor 等）
    for process_type in list(self.all_processes.keys()):
        if process_type != ray_constants.PROCESS_TYPE_REAPER:
            self._kill_process_type(
                process_type,
                check_alive=check_alive,
                allow_graceful=allow_graceful,
                wait=wait,
            )

    # 4. reaper 最后杀 — 它是兜底的安全网
    if ray_constants.PROCESS_TYPE_REAPER in self.all_processes:
        self._kill_process_type(
            ray_constants.PROCESS_TYPE_REAPER,
            check_alive=check_alive,
            allow_graceful=allow_graceful,
            wait=wait,
        )
```

**杀进程顺序的设计意图**：
- raylet 先杀：因为 raylet 管理着所有 worker 进程，先杀 raylet 让它有机会优雅地清理自己的子 worker
- 如果先杀 gcs_server，raylet 会因为无法连接 GCS 而异常退出，导致更冗长的错误输出
- reaper 最后杀：它是孤儿清理的兜底机制，必须最后退出

### 5.7 _kill_process_impl() 实现

文件：`python/ray/_private/node.py:1530-1589`

```python
def _kill_process_impl(
    self, process_type, allow_graceful=False, check_alive=True, wait=False
):
    if process_type not in self.all_processes:
        return
    process_infos = self.all_processes[process_type]
    if process_type != ray_constants.PROCESS_TYPE_REDIS_SERVER:
        assert len(process_infos) == 1

    for process_info in process_infos:
        process = process_info.process
        # 如果进程已退出
        if process.poll() is not None:
            if check_alive:
                raise RuntimeError(
                    "Attempting to kill a process of type "
                    f"'{process_type}', but this process is already dead."
                )
            else:
                continue

        # valgrind 模式特殊处理（略）

        if allow_graceful:
            # 先发 SIGTERM，等 1 秒
            process.terminate()
            timeout_seconds = 1
            try:
                process.wait(timeout_seconds)
            except subprocess.TimeoutExpired:
                pass

        # 如果进程还没退出，强杀
        if process.poll() is None:
            process.kill()   # SIGKILL
            if wait:
                process.wait()

    del self.all_processes[process_type]
```

当 `allow_graceful=False`（意外退出时）：
- 跳过 SIGTERM，直接 `process.kill()`（即 SIGKILL）
- `check_alive=False` 表示不检查进程是否还活着，已死的直接跳过

### 5.8 os._exit(1) vs sys.exit(1)

用 `os._exit(1)` 而非 `sys.exit(1)`：
- `sys.exit()` 会触发 atexit handler → 再次调用 `kill_all_processes(allow_graceful=True)`
- 已经强杀过了，不需要再来一次优雅清理
- `os._exit()` 直接终止进程，exit code = 1

### 5.9 shutdown_at_exit 注册的钩子

文件：`python/ray/_private/node.py:437-452`

```python
def _register_shutdown_hooks(self):
    def atexit_handler(*args):
        self.kill_all_processes(check_alive=False, allow_graceful=True)

    atexit.register(atexit_handler)

    def sigterm_handler(signum, frame):
        self.kill_all_processes(check_alive=False, allow_graceful=True)
        sys.exit(1)

    ray._private.utils.set_sigterm_handler(sigterm_handler)
```

这些钩子在**正常退出**和 **SIGTERM** 时触发，做优雅清理。但在意外退出场景中，`os._exit(1)` 绕过了这些钩子。

### 5.10 reaper 进程（孤儿清理安全网）

文件：`python/ray/_private/ray_process_reaper.py`

reaper 是一个轻量级守护进程：
- 继承父进程的 stdin，当父进程死亡时 stdin 收到 EOF
- 收到 EOF 后调用 `reap_process_group()`：向整个进程组发 SIGTERM，等 1 秒后 SIGKILL
- 这确保即使父进程被 SIGKILL 或 segfault，子进程也不会成为孤儿

### 5.11 fate-sharing 机制（Linux 内核级）

文件：`python/ray/_private/utils.py:789-818`

在 Linux 上，通过 `prctl(PR_SET_PDEATHSIG)` 让子进程在父进程死亡时收到 SIGKILL：

```python
def set_pdeathsig():
    """Set the parent death signal to SIGKILL."""
    prctl(PR_SET_PDEATHSIG, signal.SIGKILL)
```

这是内核级的父子进程命运共享，比 reaper 更可靠。

## 6. 完整触发链

```
gcs_server 内存膨胀到 ~124GB
  → cgroup OOM → 内核发 SIGKILL (9)
  → gcs_server 退出, returncode = -9
  → 1秒后 ray start --block 轮询发现 dead_processes()
  → -9 不在 expected_return_codes 中 → 判定为意外退出
  → kill_all_processes(check_alive=False, allow_graceful=False)
      → 按 raylet → gcs_server → 其他 → reaper 顺序强杀
  → os._exit(1)  (绕过 atexit handler)
  → 容器 PID 1 退出, exit code 1
  → K8s 检测到容器退出，根据 restart policy 重启
  → 同一个 Pod，新容器启动
  → 新的 Ray session (session_2026-07-22_12-56-47)
  → 旧 worker 节点 cluster ID 不匹配 → Wrong cluster ID token 报错
  → 旧 worker 重新注册或超时断开
```

## 7. 验证方法

### 7.1 检查 pod 重启次数

```bash
kubectl get pod <pod-name> -o jsonpath='{.status.containerStatuses[0].restartCount}'
```

### 7.2 查看上次容器退出原因

```bash
kubectl describe pod <pod-name> | grep -A5 "Last State"
```

如果 `Last State` 显示 `Reason: OOMKilled` 或 `Exit Code: 137`，确认是 OOM 级联导致容器重启。

### 7.3 检查 Ray session 历史确认重启

```bash
ls -lt /tmp/ray/
```

多个 session 目录 + `session_latest` 指向最新的，可确认重启时间。

### 7.4 检查 dmesg 确认 OOM

```bash
dmesg -T | grep -i "oom\|out of memory\|killed process"
```

### 7.5 检查 ray_process_exit.log

```bash
cat /tmp/ray/session_latest/logs/ray_process_exit.log
```

Ray `--block` 模式会将意外退出的子进程信息写入此文件。

## 8. 总结

| 维度 | 结论 |
|------|------|
| 重启确认 | ✅ head 节点确实重启了 |
| 重启原因 | GCS server 内存膨胀到 ~124GB，触发 cgroup OOM Kill |
| 重启方式 | 同一 Pod 内容器重启（非 Pod 重建） |
| 代码机制 | `ray start --block` 检测子进程意外退出 → kill all → `os._exit(1)` → K8s 重启容器 |
| 设计哲学 | Fail fast：不做子进程级自动恢复，交给上层编排系统（K8s）处理 |
| 关键排查手段 | `dmesg -T` 是发现 OOM Kill 最直接的方式 |
