# Bazel 构建与缓存机制详解

## 1. CI 构建脚本 `.bazelrc` 配置解析

在 `ci/build/build-kuaishou-manylinux-wheel.sh` 中，脚本会向 `~/.bazelrc` 写入以下配置：

```bash
{
  echo "build --config=ci"
  echo "build --announce_rc"
  if [[ "${BUILDKITE_BAZEL_CACHE_URL:-}" != "" ]]; then
    echo "build:ci --remote_cache=${BUILDKITE_BAZEL_CACHE_URL:-}"
  fi
} > "$HOME"/.bazelrc
```

### 1.1 `--config=ci` 的含义和作用

`--config=ci` 激活在项目根 `.bazelrc` 中预定义的 `ci` 配置段。Bazel 会展开所有 `build:ci` 和 `test:ci` 前缀的选项：

```
# .bazelrc 中的 ci 配置段
build:ci --color=yes                          # 彩色输出，方便CI日志阅读
build:ci --curses=no                          # 禁用终端光标控制，适配CI终端
build:ci --keep_going                         # 遇到错误继续构建，不立即中止
build:ci --progress_report_interval=100       # 每100秒报告一次进度
build:ci --show_progress_rate_limit=15        # 进度显示频率限制
build:ci --ui_actions_shown=1024              # UI中最多显示1024个action
build:ci --show_timestamps                    # 显示时间戳

test:ci --config=ci-base                      # 测试继承ci-base配置
test:ci --nocache_test_results                # 禁用测试结果缓存
test:ci --spawn_strategy=local                # 测试用本地策略运行
test:ci --experimental_ui_max_stdouterr_bytes=-1  # 不截断测试输出
```

其中 `test:ci --nocache_test_results` 的原因是：py_test 在 Bazel 下非 hermetic（可能导入 sandbox 外的依赖），Bazel 只根据声明的依赖判断缓存是否命中，因此禁用测试缓存防止拿到错误结果。

### 1.2 `--announce_rc` 的含义和作用

`--announce_rc` 让 Bazel 在构建开始时**打印出所有生效的 rc 配置选项**，包括 `.bazelrc`、`$HOME/.bazelrc`、`--config=ci` 展开后的所有标志。

作用：
- **可审计性**：CI 日志中记录本次构建实际使用的所有 Bazel 标志，方便排查构建行为异常
- **调试**：确认 `--remote_cache` 等关键选项是否正确传入

### 1.3 `--remote_cache` 的含义和作用

```
build:ci --remote_cache=${BUILDKITE_BAZEL_CACHE_URL}
```

将构建产物上传到远程缓存（通常是 HTTP 缓存服务），后续构建可直接下载已编译的产物，避免重复编译。注意这里写的是 `build:ci`（带 `:ci` 后缀），只有启用 `--config=ci` 时才生效。

---

## 2. Bazel 远程缓存如何区分不同 Python 版本

### 2.1 核心机制：Action Digest

Bazel 远程缓存的 key 是 **Action Digest**（SHA256 摘要），其输入包括：

```
Action Digest = SHA256(
    输入文件内容列表 +     // .cc, .h, .py 等
    输出文件路径列表 +     // .o, .so 等
    命令行参数 +          // 编译器 flags
    环境变量表 +          // --action_env 传入的变量
    工具链信息            // compiler path 等
)
```

**环境变量表是 Action Digest 的输入之一**，任何参与 Digest 计算的输入变了，Digest 就变。

### 2.2 同一镜像、同一脚本下的区分机制

在 `ci/build/build-kuaishou-manylinux-wheel.sh` 中：

```bash
# 第7行：根据 Python 版本设置 RAY_BUILD_ENV
export RAY_BUILD_ENV="manylinux_py${PYTHON}"   # 例如 "manylinux_pycp310", "manylinux_pycp311"
```

项目根 `.bazelrc` 中：

```
build --action_env=RAY_BUILD_ENV
```

`--action_env=RAY_BUILD_ENV` 的含义是：**从启动 Bazel 的宿主机进程中读取 `RAY_BUILD_ENV` 环境变量的当前值，然后把它注入到每个 Action 的执行环境里**。

因此：

```
Python 3.10: RAY_BUILD_ENV="manylinux_pycp310" → Action Digest A
Python 3.11: RAY_BUILD_ENV="manylinux_pycp311" → Action Digest B
Python 3.12: RAY_BUILD_ENV="manylinux_pycp312" → Action Digest C
```

不同值 → 不同 Digest → 缓存天然隔离。

### 2.3 其他影响 Action Digest 的 Python 版本差异

| 影响因素 | 如何进入 Action Digest |
|---|---|
| **`RAY_BUILD_ENV` 环境变量** | 脚本设置，通过 `--action_env` 注入 |
| **Python 解释器路径不同** | 脚本中 `sudo ln -sf "/opt/python/${PYTHON}/bin/python3" /usr/local/bin/python3`，不同版本 symlink 指向不同解释器 |
| **`PATH` 不同** | 脚本中 `PATH="/opt/python/${PYTHON}/bin:$PATH"` |
| **pip 依赖不同** | 不同 Python 版本对应不同的 requirements 文件，内容不同 → digest 不同 |
| **`setup.py` 生成内容** | 不同 Python 版本生成不同的包名（如 `cp310`/`cp311`） |

### 2.4 C++ 部分的缓存共享

项目的 `WORKSPACE` 只注册了 `python3_10` 一个 toolchain，**Bazel 侧的 C++ 构建不随 Python 版本变化**。C++ 共享库（`.so`）的编译 action 不依赖 `RAY_BUILD_ENV` 和 Python 路径，因此：

```
C++ .so 编译 → Action Digest C (不依赖Python版本) → Python 3.10/3.11/3.12 共享缓存
Python 打包 → Action Digest A/B (依赖Python版本) → 各版本独立缓存
```

---

## 3. `--action_env` 工作机制详解

### 3.1 从宿主机读取环境变量值

```bash
# 先设置环境变量
export RAY_BUILD_ENV="manylinux_pycp310"

# 再启动 Bazel
bazel build //:ray_pkg
```

Bazel 启动时从**自己的进程环境**中读取 `RAY_BUILD_ENV` 的值。

### 3.2 注入到 Action 的执行环境

Bazel 在执行每个 spawn（编译、链接等子进程）时，会构造一个**受控的环境变量表**传给子进程。`--action_env=RAY_BUILD_ENV` 表示把这个变量的值放进这个表里。

配合 `.bazelrc` 中的：

```
build --incompatible_strict_action_env
```

这个标志的作用是：**只传递 `--action_env` 显式声明的变量**，而不是把宿主机的整个环境都传进去，保证构建的 hermeticity（密封性）。

### 3.3 参与 Action Digest 计算

环境变量表是 Action Digest 的输入之一，所以当 `RAY_BUILD_ENV` 值不同时，Digest 自然不同。

### 3.4 不同环境的对比

```
CI 容器 shell:    export RAY_BUILD_ENV="manylinux_pycp310"  → Bazel 读到 "manylinux_pycp310" → env 表有值
本地开发 shell:    未设置 RAY_BUILD_ENV                       → Bazel 读到 ""                → env 表为空字符串
```

三种不同的 env 表 → 三种不同的 Hash → 三个不同的 Digest → 缓存互不命中。

---

## 4. `$WORKSPACE` 的确定方式

Bazel 从**当前工作目录向上查找** `WORKSPACE` 或 `WORKSPACE.bazel` 文件，找到的那个目录就是 workspace root，该目录下的 `.bazelrc` 就是项目级 rc 文件。

```
/Users/franke/Desktop/git/kray/WORKSPACE  ← 找到这个
/Users/franke/Desktop/git/kray/.bazelrc    ← 这就是 $WORKSPACE/.bazelrc
```

在 `/Users/franke/Desktop/git/kray/` 下执行 `bazel build`，Bazel 就用这个目录的 `.bazelrc`。`cd` 到子目录再执行，workspace root 不变，还是同一个。

---

## 5. Bazel RC 文件加载顺序与优先级

Bazel 加载 rc 文件的顺序：

```
1. /etc/bazel.bazelrc          (系统级)
2. $WORKSPACE/.bazelrc         (项目根)  ← --action_env=RAY_BUILD_ENV 在这里
3. $HOME/.bazelrc              (用户主目录) ← 本地自定义配置在这里
4. 命令行 --flag                (最高优先级)
```

**后加载的覆盖/追加先加载的**。对于不同类型的 flag：

| flag 类型 | 行为 |
|---|---|
| **单值 flag**（如 `--compilation_mode`） | `~/.bazelrc` 覆盖项目 `.bazelrc` |
| **累积 flag**（如 `--action_env`、`--copt`） | 追加，不覆盖 |

例如本地 `~/.bazelrc` 中有：

```
build --local_ram_resources=HOST_RAM*.5 --local_cpu_resources=6
```

这些是**资源限制类 flag**，和 `--action_env=RAY_BUILD_ENV` 完全不冲突，两者**同时生效**。

---

## 6. 本地 Bazel 缓存路径配置

### 6.1 默认路径

```
~/.cache/bazel/_bazel_<username>/<workspace_hash>/repository_cache       # 外部依赖缓存
~/.cache/bazel/_bazel_<username>/<workspace_hash>/bazel-out/.../          # 构建输出
```

### 6.2 修改方式

**`--disk_cache`**（最常用，只修改磁盘缓存位置）：

```bash
bazel build --disk_cache=/path/to/cache //:target
```

或写入 `.bazelrc`：

```
build --disk_cache=/path/to/cache
```

**`--output_base`**（修改整个输出根目录）：

```bash
bazel build --output_base=/path/to/bazel_output //:target
```

### 6.3 本地缓存 vs 远程缓存

| 方式 | 适用场景 |
|---|---|
| `--disk_cache` | 本地多个 workspace 共享缓存 |
| `--remote_cache` | CI 跨机器共享 / 团队共享 |
| 两者同时用 | 本地磁盘缓存 + 远程缓存，先查本地再查远程 |

### 6.4 本地连 CI 缓存的注意事项

本地默认 `RAY_BUILD_ENV=""`，和 CI 的缓存 key 不同，因此**本地和 CI 的缓存互不命中**。这是预期行为——本地环境（macOS vs Linux、不同编译器版本等）差异很大。

如果确实需要本地命中 CI 缓存：

```bash
export RAY_BUILD_ENV="manylinux_pycp310"
```

并在 `~/.bazelrc` 中加上远程缓存地址：

```
build --remote_cache=你的BUILDKITE_BAZEL_CACHE_URL
```

但**不建议本地连 CI 缓存**，因为本地环境差异很大，即使 Digest 不同也会浪费上传带宽。

---

## 8. Wheel 构建与 Docker 镜像流水线

### 8.1 Wheel 名称拼接规则

编译脚本 `ci/build/build-kuaishou-manylinux-wheel.sh` 生成的 wheel 文件名格式：

```
ray-{BASE_VERSION}+kuaishou.{SHORT_COMMIT}-{PYTHON}-{PYTHON}-manylinux2014_{ARCH}.whl
```

各部分来源：

| 组成部分 | 来源 | 示例值 | 说明 |
|---|---|---|---|
| **BASE_VERSION** | `python/ray/_version.py` 的 `version` 字段基础部分 | `2.55.1` | 后续升级版本号时需同步更新流水线脚本 |
| **SHORT_COMMIT** | `TRAVIS_COMMIT`(或`BUILDKITE_COMMIT`)的前10位 | `4b33d355df` | 由构建脚本 sed 替换注入 `_version.py` |
| **PYTHON** | 构建脚本第一个参数 `$1` | `cp312`、`cp310`、`cp311` | 决定 wheel 中的 Python ABI 标签 |
| **ARCH** | 构建机器架构 | `x86_64` 或 `aarch64` | `pip wheel` 原本生成 `linux_{ARCH}`，脚本 rename 为 `manylinux2014_{ARCH}` |
| **manylinux2014** | 构建脚本硬编码 rename | `manylinux2014` | 脚本第53-57行将 `-linux` 替换为 `-manylinux2014` |

完整示例：

```
ray-2.55.1+kuaishou.4b33d355df-cp312-cp312-manylinux2014_x86_64.whl
```

### 8.2 版本注入流程

`python/ray/_version.py` 模板中包含占位符：

```python
commit = "{{RAY_COMMIT_SHA}}"
version = "{BASE_VERSION}+kuaishou.{{RAY_COMMIT_SHA_SHORT}}"
```

构建脚本执行 sed 替换（`ci/build/build-kuaishou-manylinux-wheel.sh` 第21-24行）：

```bash
TRAVIS_COMMIT="${TRAVIS_COMMIT:-${BUILDKITE_COMMIT:-$(git rev-parse HEAD)}}"
SHORT_COMMIT="${TRAVIS_COMMIT:0:10}"
sed -i.bak -e "s/{{RAY_COMMIT_SHA}}/$TRAVIS_COMMIT/g" \
           -e "s/{{RAY_COMMIT_SHA_SHORT}}/$SHORT_COMMIT/g" ray/_version.py
```

替换后 `_version.py` 变为：

```python
commit = "4b33d355df1137a7cfeaa165513c234b23789ba0"
version = "2.55.1+kuaishou.4b33d355df"
```

`setup.py` 通过 `find_version()` 从 `_version.py` 读取 version，再传给 setuptools 生成 wheel 包名。

### 8.3 Wheel 文件名各部分详细推导

#### `cp312-cp312` 部分

由构建脚本第一个参数 `$1` 决定：

```bash
PYTHON="$1"   # 如 cp312、cp310、cp311
```

在 wheel 名称中出现两次 `cp312-cp312`，这是 Python wheel 的标准格式：
- 第一个 `cp312` 是 **Python ABI tag**（`cp312` = CPython 3.12）
- 第二个 `cp312` 是 **Python implementation tag**

两者通常相同，由 `pip wheel` 根据 Python 解释器版本自动生成。

#### `manylinux2014_x86_64` 部分

构建过程分两步：

1. `pip wheel` 原本生成的平台标签是 `linux_x86_64`（或 `linux_aarch64`）
2. 构建脚本将 `-linux` 替换为 `-manylinux2014`（第53-57行）：

```bash
for path in dist/*.whl; do
  if [[ -f "${path}" ]]; then
    out="${path//-linux/-manylinux2014}"
    if [[ "$out" != "$path" ]]; then
      mv "${path}" "${out}"
    fi
  fi
done
```

架构 `x86_64` / `aarch64` 取决于构建机器的 CPU 架构，在 x86 机器上编译就是 `x86_64`，ARM 机器上是 `aarch64`。

### 8.4 Docker 镜像构建流程

Dockerfile: `docker/ray/kuaishou-Dockerfile`

关键 ARG：

```dockerfile
ARG WHEEL_PATH          # wheel 文件名，如 ray-2.55.1+kuaishou.4b33d355df-cp312-cp312-manylinux2014_x86_64.whl
ARG FIND_LINKS_PATH=".whl"
ARG CONSTRAINTS_FILE="requirements_compiled.txt"
```

构建时通过 `--build-arg WHEEL_PATH=xxx` 传入 wheel 文件名，Dockerfile 中通过 `csc get` 下载 wheel 后用 pip 安装：

```dockerfile
RUN csc get luoyang/ray/${WHEEL_PATH} ${FIND_LINKS_PATH}/ && \
    $HOME/anaconda3/bin/pip --no-cache-dir install "${FIND_LINKS_PATH}/${WHEEL_PATH}[data-kconf]"

RUN $HOME/anaconda3/bin/pip --no-cache-dir install -c /tmp/requirements_compiled.txt \
    "${FIND_LINKS_PATH}/${WHEEL_PATH}[data,train,serve]" \
    --find-links $FIND_LINKS_PATH
```

### 8.5 核心问题：编译时才知道 wheel 名称，Dockerfile 怎么传参？

**问题**：编译 wheel 时 commit hash 才确定最终文件名，但 Dockerfile 的 `ARG WHEEL_PATH` 需要提前知道。

**结论：不需要拆成两个流水线**，因为所有参数在流水线启动时已知，可以预计算 wheel 名称。

### 8.6 单流水线方案：预计算 wheel 名称

所有组成 wheel 名称的参数都是流水线入参，无需等编译完成：

```bash
PYTHON="cp312"                                    # 流水线入参：Python 版本
TRAVIS_COMMIT="${BUILDKITE_COMMIT}"                # 流水线入参：commit SHA
SHORT_COMMIT="${TRAVIS_COMMIT:0:10}"               # 取前10位
BASE_VERSION="2.55.1"                              # 基础版本号
# 或动态获取: BASE_VERSION=$(python python/ray/_version.py | cut -d' ' -f1 | cut -d'+' -f1)
ARCH="x86_64"                                      # 构建机器架构

WHEEL_NAME="ray-${BASE_VERSION}+kuaishou.${SHORT_COMMIT}-${PYTHON}-${PYTHON}-manylinux2014_${ARCH}.whl"

# Step 1: 编译 wheel
bash ci/build/build-kuaishou-manylinux-wheel.sh ${PYTHON}

# Step 2: 构建 Docker 镜像，动态传入 wheel 名称
docker build \
  --build-arg WHEEL_PATH="${WHEEL_NAME}" \
  -f docker/ray/kuaishou-Dockerfile .
```

**注意事项**：
- `BASE_VERSION` 如后续升级版本号，流水线脚本也需同步更新。可用 `python python/ray/_version.py | cut -d' ' -f1 | cut -d'+' -f1` 动态获取避免硬编码
- `ARCH` 在已知构建机器架构的情况下可直接写死 `x86_64`，或在脚本中通过 `uname -m` 动态获取

### 8.7 其他方案对比（供参考）

| 方案 | 描述 | 优点 | 缺点 |
|---|---|---|---|
| **预计算 wheel 名称（推荐）** | 流水线启动时根据入参预计算名称，传入 docker build | 无需改动现有架构，一个流水线搞定 | BASE_VERSION 变化时需同步脚本 |
| 编译后动态获取 | 编译完 `ls .whl/ray-*.whl` 取文件名再传 docker build | 不依赖预计算 | 需要两步串行，但仍在同一流水线 |
| Dockerfile 通配安装 | 用 `ray-*.whl` glob 安装 | 不依赖精确名称 | pip 不支持 glob，`csc get` 也需精确路径，不可行 |
| 固定命名 rename | 编译后 rename 为固定名称 | Dockerfile 简单 | 丢失 commit 信息，不利于追溯 |
| 多阶段 Docker 构建 | 编译步骤放进 Dockerfile builder stage | 全在一个 Dockerfile | 编译环境复杂，Dockerfile 臃肿，构建缓存利用率低 |

### 8.8 关键源码引用

| 文件 | 作用 |
|---|---|
| `ci/build/build-kuaishou-manylinux-wheel.sh` | wheel 编译脚本，处理 commit 注入、pip wheel 构建、linux→manylinux2014 rename、csc 上传 |
| `ci/build/build-ray-docker.sh` | Docker 镜像构建脚本（上游 Ray 版本），从 `.whl/` 目录获取 wheel 名传入 docker build |
| `docker/ray/kuaishou-Dockerfile` | 快手定制 Dockerfile，通过 ARG WHEEL_PATH 接收 wheel 文件名 |
| `python/ray/_version.py` | 版本文件，包含 commit 和 version，构建时由 sed 替换占位符 |
| `python/setup.py` | setup 配置，通过 `find_version()` 从 `_version.py` 读取版本号 |
