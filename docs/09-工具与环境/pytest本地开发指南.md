# Ray 本地开发 pytest 测试指南

## 问题背景

在 Ray 项目中运行 pytest 测试时，会遇到本地源码与已安装 Ray 包冲突的问题。主要表现为：

```
ModuleNotFoundError: No module named 'ray._raylet'
```

## 问题根因

1. **路径冲突**：pytest 自动将测试文件所在目录及其父目录加入 `sys.path`
2. **缺少编译模块**：本地 `python/ray/` 目录没有编译的 `_raylet.so` 和 protobuf 生成文件
3. **版本不匹配**：本地代码版本与已安装 Ray 版本不一致

---

## 解决方案

### 方案 1：同步本地代码到已安装目录（推荐）

将本地 Python 代码同步到 site-packages，保留已编译的文件：

```bash
# 同步所有 Python 文件，排除编译文件
rsync -av --include='*.py' --include='*/' --exclude='*.so' --exclude='*.pyd' --exclude='__pycache__' \
    /Users/franke/Desktop/git/kray/python/ray/ \
    /Users/franke/miniforge3/envs/py312-kray-master-54/lib/python3.12/site-packages/ray/
```

**优点**：本地修改立即生效，代码完全同步
**缺点**：每次修改后需要重新同步

---

### 方案 2：复制编译文件到本地目录

将已安装 Ray 的编译文件复制到本地源码目录：

```bash
# 复制 _raylet.so
cp /Users/franke/miniforge3/envs/py312-kray-master-54/lib/python3.12/site-packages/ray/_raylet.so \
   /Users/franke/Desktop/git/kray/python/ray/

# 复制 core 目录（protobuf 生成文件）
cp -r /Users/franke/miniforge3/envs/py312-kray-master-54/lib/python3.12/site-packages/ray/core \
   /Users/franke/Desktop/git/kray/python/ray/

# 复制 serve/generated 目录
cp -r /Users/franke/miniforge3/envs/py312-kray-master-54/lib/python3.12/site-packages/ray/serve/generated \
   /Users/franke/Desktop/git/kray/python/ray/serve/
```

**注意**：同时需要确保版本号一致：

```bash
# 编辑 python/ray/_version.py
# 将版本改为与已安装版本一致，例如：
version = "2.54.0"
```

**优点**：可以直接在项目目录运行测试
**缺点**：
- 本地代码与已安装代码可能不同步，导致 `AttributeError` 等问题
- 需要定期更新复制的文件

---

### 方案 3：从非项目目录运行测试

避免 pytest 自动添加本地路径：

```bash
cd /tmp && python -m pytest /Users/franke/Desktop/git/kray/python/ray/tests/test_xxx.py -v
```

**优点**：简单，不需要修改任何文件
**缺点**：无法在 PyCharm 中直接点击运行

---

### 方案 4：PyCharm 配置

在 PyCharm 中配置 pytest 运行环境：

1. 打开 **Run → Edit Configurations...**
2. 选择 pytest 配置或编辑模板
3. 设置：
   - **Environment variables**: `PYTHONPATH=`（设为空）
   - **取消勾选**: `Add content roots to PYTHONPATH`
   - **取消勾选**: `Add source roots to PYTHONPATH`

**注意**：此方案对依赖 conftest.py 的测试可能无效，因为 conftest.py 在配置生效前就被加载。

---

### 方案 5：使用根目录 conftest.py

在项目根目录创建 `conftest.py` 自动修复路径：

```python
# /Users/franke/Desktop/git/kray/conftest.py
import sys
import os

_project_root = os.path.dirname(os.path.abspath(__file__))
_python_dir = os.path.join(_project_root, "python")

# 移除本地 python 目录
_paths_to_remove = []
for _path in sys.path:
    _abs_path = os.path.abspath(_path) if _path else ""
    if _abs_path == _python_dir or _abs_path.startswith(_python_dir + os.sep):
        _paths_to_remove.append(_path)

for _path in _paths_to_remove:
    sys.path.remove(_path)
```

**注意**：此方案对不依赖其他 conftest.py 的测试有效，但如果子目录 conf被加载并 import ray，仍会失败。

---

## 常见错误及解决

### 错误 1: `ModuleNotFoundError: No module named 'ray._raylet'`

**原因**：pytest 加载了本地 `python/ray/` 而不是已安装的 Ray 包

**解决**：使用上述方案 1 或方案 2

---

### 错误 2: `RuntimeError: Version mismatch`

```
Ray: 2.54.0 vs Ray: 2.54.0+kuaishou.dev
```

**原因**：本地 `_version.py` 版本与已安装版本不一致

**解决**：编辑 `python/ray/_version.py`，使版本号与已安装版本一致：

```python
version = "2.54.0"  # 与已安装版本一致
```

---

### 错误 3: `AttributeError: 'XXX' object has no attribute 'yyy'`

例如：`'InputDataBuffer' object has no attribute 'completed'`

**原因**：本地代码与已安装代码不同步，API 发生变化

**解决**：使用方案 1 同步代码，确保本地代码与运行环境一致

---

### 错误 4: `ModuleNotFoundError: No module named 'ray.xxx.yyy'`

例如：`No module named 'ray.data._internal.execution.progress_manager'`

**原因**：本地新增了模块，但已安装版本没有

**解决**：使用方案 1 同步代码

---

## 推荐工作流程

### 对于只修改 Python 代码的开发：

1. **初始设置**：使用方案 1 同步代码到 site-packages
2. **开发时**：修改本地代码后，重新运行 rsync 命令同步
3. **运行测试**：直接在 PyCharm 点击运行或使用 `python -m pytest`

### 对于不需要完整 Ray 环境的单元测试：

某些测试（如 `test_open_telemetry_metric_recorder.py`）不需要启动 Ray 集群，可以：

1. 使用方案 3 从 `/tmp` 运行
2. 或使用方案 2 复制编译文件后直接运行

---

## 快速命令参考

```bash
# 同步代码（推荐）
rsync -av --include='*.py' --include='*/' --exclude='*.so' --exclude='*.pyd' --exclude='__pycache__' \
    python/ray/ \
    $CONDA_PREFIX/lib/python3.12/site-packages/ray/

# 从 /tmp 运行测试
cd /tmp && python -m pytest /path/to/test.py -v

# 查看已安装 Ray 版本
pon -c "import ray; print(ray.__version__, ray.__file__)"
```
