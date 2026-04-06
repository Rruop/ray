# Ray Runtime Environment 详解

本文档详细介绍 Ray 的 runtime_env 机制，包括 `ray.is_initialized()`、`working_dir`、`PYTHONPATH` 和 `sys.path` 的工作原理。

## 目录

1. [ray.is_initialized() 与集群的关系](#1-rayis_initialized-与集群的关系)
2. [ray job submit 与 ray.init 的 runtime_env 合并](#2-ray-job-submit-与-rayinit-的-runtime_env-合并)
3. [working_dir 完整流程](#3-working_dir-完整流程)
4. [PYTHONPATH 与 sys.path 的修改机制](#4-pythonpath-与-syspath-的修改机制)
5. [最佳实践](#5-最佳实践)

---

## 1. ray.is_initialized() 与集群的关系

### 实现原理

`ray.is_initialized()` 检测的是**当前进程是否已连接到 Ray 集群**，而不是集群是否在运行。

```python
# python/ray/_private/worker.py:2470-2478
@PublicAPI
def is_initialized() -> bool:
    """Check if ray.init has been called yet."""
    return ray._private.worker.global_worker.connected
```

### 关键点

| 场景 | `is_initialized()` 返回值 |
|------|--------------------------|
| 集群已启动，但当前进程未调用 `ray.init()` | `False` |
| 当前进程已调用 `ray.init()` 并成功连接 | `True` |
| 调用 `ray.shutdown()` 后 | `False` |

### 典型用法

```python
if not ray.is_initialized():
    ray.init(
        runtime_env={
            "env_vars": {"PYTHONPATH": project_root},
            "excludes": ["*.pyc", "__pycache__", ".git"]
        }
    )
```

---

## 2. ray job submit 与 ray.init 的 runtime_env 合并

### 提交流程

当使用 `ray job submit --address=<cluster> --runtime-env-json='{...}'` 提交任务时：

```
ray job submit --address=<cluster> -- python your_script.py
                    │
                    ▼
         JobSupervisor 设置环境变量:
         - RAY_ADDRESS = <cluster>
         - RAY_JOB_CONFIG_JSON_ENV_VAR = {...runtime_env...}
                    │
                    ▼
         your_script.py 中的 ray.init() 会:
         1. 检测到这些环境变量
         2. 自动合并 job 提交时的 runtime_env
         3. 连接到已存在的集群
```

### 合并规则

```python
# python/ray/_private/worker.py:1759-1783
runtime_env = _merge_runtime_env(
    parent=job_submit_runtime_env,   # ray job submit 的 runtime_env
    child=ray_init_runtime_env,      # ray.init() 的 runtime_env
    override=os.getenv("RAY_OVERRIDE_JOB_RUNTIME_ENV") == "1",
)
```

| 场景 | 行为 |
|------|------|
| 两者设置不同 key | 合并，都生效 |
| 两者设置相同 key (默认) | **报错** `ValueError` |
| 两者设置相同 key + `RAY_OVERRIDE_JOB_RUNTIME_ENV=1` | ray.init 的覆盖 job submit 的 |
| `env_vars` 字典 | **按环境变量 key 合并**，不冲突则合并 |

### 示例

```bash
# 方式1: 只在 job submit 时设置
ray job submit --address=http://localhost:8265 \
    --runtime-env-json='{"env_vars": {"PYTHONPATH": "/shared/project"}}' \
    -- python my_script.py

# 方式2: 只在 ray.init 中设置
ray job submit --address=http://localhost:8265 -- python my_script.py
# my_script.py 中:
# ray.init(runtime_env={"env_vars": {"PYTHONPATH": "/shared/project"}})

# 方式3: 两处都设置不同的 key (会合并)
# job submit: {"pip": ["pandas"]}
# ray.init:   {"env_vars": {"DEBUG": "1"}}
# 最终: {"pip": ["pandas"], "env_vars": {"DEBUG": "1"}}
```

---

## 3. working_dir 完整流程

### 流程概览

```
┌──────────────────────────────────────────────────────────────────────┐
│  阶段 1: 客户端打包上传 (ray.init / ray job submit)                    │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ray.init(runtime_env={"working_dir": "./my_project"})               │
│      │                                                               │
│      ▼                                                               │
│  upload_working_dir_if_needed()                                      │
│      ├── 收集 excludes (.gitignore, .rayignore, excludes 参数)        │
│      ├── 计算 content hash → URI: "gcs://_ray_pkg_<HASH>.zip"        │
│      ├── 打包成 zip 文件                                              │
│      └── 上传到 GCS (Ray 内部存储)                                     │
│                                                                      │
│  runtime_env["working_dir"] = "gcs://_ray_pkg_<HASH>.zip"            │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────────┐
│  阶段 2: Worker 节点下载解压 (RuntimeEnvAgent)                         │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  当 worker 需要执行任务时:                                             │
│      │                                                               │
│      ▼                                                               │
│  WorkingDirPlugin.create()                                           │
│      ├── 从 GCS 下载 zip 文件                                         │
│      └── 解压到: {ray_temp}/runtime_resources/working_dir_files/pkg/ │
│                                                                      │
│  本地路径示例:                                                         │
│  /tmp/ray/session_xxx/runtime_resources/working_dir_files/_ray_pkg_abc123/
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────────┐
│  阶段 3: 修改执行上下文 (modify_context)                               │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  WorkingDirPlugin.modify_context():                                  │
│                                                                      │
│  ① 修改 command_prefix (改变工作目录):                                 │
│     context.command_prefix += ["cd", local_dir, "&&"]                │
│                                                                      │
│  ② 修改 PYTHONPATH:                                                   │
│     set_pythonpath_in_context(local_dir, context)                    │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────────┐
│  阶段 4: Worker 进程启动                                              │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  RuntimeEnvContext.exec_worker():                                    │
│                                                                      │
│  ① 应用环境变量:                                                       │
│     update_envs(context.env_vars)                                    │
│     → os.environ["PYTHONPATH"] = "/tmp/.../working_dir:..."          │
│                                                                      │
│  ② 执行命令:                                                          │
│     os.execvp("bash", ["bash", "-c",                                 │
│         "cd /tmp/.../working_dir_files/pkg_xxx && "                  │
│         "exec python -u default_worker.py ..."])                     │
│                                                                      │
│  ③ Python 启动时自动处理:                                              │
│     - PYTHONPATH 中的路径 → 添加到 sys.path 开头                       │
│     - cwd 已经是 working_dir (由 cd 命令设置)                          │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

### 关键源码位置

| 功能 | 文件路径 |
|------|----------|
| working_dir 上传/下载/context修改 | `python/ray/_private/runtime_env/working_dir.py` |
| 打包/解压逻辑 | `python/ray/_private/runtime_env/packaging.py` |
| RuntimeEnvContext 执行 worker | `python/ray/_private/runtime_env/context.py` |
| runtime_env 合并函数 | `python/ray/runtime_env/runtime_env.py:632-679` |
| ray.init 中 runtime_env 处理 | `python/ray/_private/worker.py:1759-1799` |

---

## 4. PYTHONPATH 与 sys.path 的修改机制

### PYTHONPATH 优先级

```python
# python/ray/_private/runtime_env/working_dir.py:138-151
def set_pythonpath_in_context(python_path: str, context: RuntimeEnvContext):
    """
    优先级 (从高到低):
    1. working_dir 路径
    2. env_vars["PYTHONPATH"] (用户在 runtime_env 中指定)
    3. 集群已有的 PYTHONPATH
    """
    if "PYTHONPATH" in context.env_vars:
        python_path += os.pathsep + context.env_vars["PYTHONPATH"]
    if "PYTHONPATH" in os.environ:
        python_path += os.pathsep + os.environ["PYTHONPATH"]
    context.env_vars["PYTHONPATH"] = python_path
```

### sys.path 的设置阶段

#### 阶段 1: Driver 端设置 `_py_driver_sys_path`

```python
# python/ray/_private/worker.py:2662-2686
if mode == SCRIPT_MODE:
    code_paths = []
    # 添加脚本所在目录
    script_directory = os.path.dirname(os.path.realpath(sys.argv[0]))
    if script_directory in sys.path:
        code_paths.append(script_directory)
    # 添加当前工作目录 (如果没有使用 working_dir)
    if not job_config._runtime_env_has_working_dir():
        current_directory = os.path.abspath(os.path.curdir)
        code_paths.append(current_directory)
    job_config._py_driver_sys_path.extend(code_paths)
```

#### 阶段 2: Worker 端应用

```python
# python/ray/_raylet.pyx:2549-2595
def maybe_initialize_job_config():
    # 添加 code_search_path 到 sys.path
    code_search_path = core_worker.get_job_config().code_search_path
    for p in code_search_path:
        sys.path.insert(0, p)

    # 添加 driver 的 sys.path
    py_driver_sys_path = core_worker.get_job_config().py_driver_sys_path
    for p in py_driver_sys_path:
        sys.path.insert(0, p)
```

### Worker 最终环境状态

假设设置了：
```python
ray.init(runtime_env={
    "working_dir": "./my_project",
    "env_vars": {"PYTHONPATH": "/custom/path"}
})
```

Worker 启动后：

```python
# PYTHONPATH 环境变量
os.environ["PYTHONPATH"] = "/tmp/ray/.../working_dir_files/pkg_xxx:/custom/path:/原有PYTHONPATH"

# sys.path
sys.path = [
    '',                                              # 当前目录 (已被 cd 改为 working_dir)
    '/tmp/ray/.../working_dir_files/pkg_xxx',        # working_dir
    '/custom/path',                                  # 用户设置的 PYTHONPATH
    '/原有PYTHONPATH/...',                           # 集群原有
    '/usr/lib/python3.x/...',                        # Python 标准库
    ...
]

# 当前工作目录
os.getcwd() = '/tmp/ray/.../working_dir_files/pkg_xxx'
```

---

## 5. 最佳实践

### working_dir vs PYTHONPATH 对比

| 特性 | `working_dir` | `env_vars["PYTHONPATH"]` |
|------|---------------|-------------------------|
| **文件传输** | ✅ 自动打包上传到所有节点 | ❌ 不传输文件 |
| **工作目录** | 切换到解压目录 | 不改变 |
| **PYTHONPATH** | 自动添加到最前 | 用户指定的值 |
| **适用场景** | 代码不在共享存储 | 代码在共享存储 (NFS等) |
| **性能** | 首次需要上传/下载 | 无额外开销 |

### 场景选择

**使用 `working_dir`**：
- 本地开发，代码只在本机
- 集群节点没有共享文件系统
- 需要确保所有节点代码一致

```python
ray.init(runtime_env={
    "working_dir": "./my_project",
    "excludes": ["*.pyc", "__pycache__", ".git", "data/"]
})
```

**使用 `PYTHONPATH`**：
- 所有节点都能访问共享存储 (NFS, HDFS, S3 挂载等)
- 避免大项目的打包上传开销
- 需要引用共享存储上的代码

```python
ray.init(runtime_env={
    "env_vars": {
        "PYTHONPATH": "/shared/storage/my_project"
    },
    "excludes": ["*.pyc", "__pycache__", ".git"]
})
```

### 避免冲突

```python
# ❌ 错误: job submit 和 ray.init 都设置 working_dir，会报错
# ray job submit --runtime-env-json='{"working_dir": "/path/a"}' ...
# ray.init(runtime_env={"working_dir": "/path/b"})  # ValueError!

# ✅ 正确: 只在一处设置
ray.init(runtime_env={
    "env_vars": {"PYTHONPATH": project_root}
})

# ✅ 正确: 设置不同的 key
# job submit: {"pip": ["pandas"]}
# ray.init:   {"env_vars": {"DEBUG": "1"}}
```

---

## 参考资料

- [Ray Runtime Environments 官方文档](https://docs.ray.io/en/latest/ray-core/handling-dependencies.html)
- 源码位置:
  - `python/ray/_private/runtime_env/working_dir.py`
  - `python/ray/_private/runtime_env/context.py`
  - `python/ray/_private/worker.py`
  - `python/ray/runtime_env/runtime_env.py`
