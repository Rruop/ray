# pytest 导入路径问题排查指南

## 问题描述

在 kray 项目中运行 pytest 时出现 `ModuleNotFoundError: No module named 'ray._raylet'` 错误：

```
ImportError while loading conftest '.../kray/python/ray/data/tests/conftest.py'.
python/ray/__init__.py:85: in <module>
    import ray._raylet  # noqa: E402
E   ModuleNotFoundError: No module named 'ray._raylet'
```

而在原始 ray 项目中使用相同方式运行测试却能成功。

## 根本原因

**kray 项目的 `python/ray/data/tests/__init__.py` 文件存在**，导致 pytest 把测试目录识别为 Python 包。

pytest 的包发现机制 (`resolve_package_path`) 会：
1. 检测测试文件所在目录是否有 `__init__.py`
2. 如果有，向上遍历找到包的根目录
3. 把包根目录的**父目录**加入 `sys.path`

| 项目 | `tests/__init__.py` | pytest 行为 | 结果 |
|------|---------------------|-------------|------|
| ray | 不存在 | 不识别为包 | `sys.path` 不变，从 site-packages 加载 ray |
| kray | **存在** | 识别为包 | 把 `python` 加入 `sys.path`，加载本地未编译的 ray |

## 排查过程

### 1. 对比两个项目的 tests/__init__.py

```bash
# 在 kray 项目中查找
cd /Users/franke/Desktop/git/kray
find python/ray -path "*/tests/__init__.py" -type f

# 结果：
# python/ray/data/tests/__init__.py  <-- 问题文件！
# ... 其他文件

# 在 ray 项目中查找
cd /Users/franke/Desktop/git/ray
find python/ray -path "*/tests/__init__.py" -type f

# 结果：没有 python/ray/data/tests/__init__.py
```

### 2. 使用 pytest 的 resolve_package_path 验证

```python
from pathlib import Path
from _pytest.pathlib import resolve_package_path

# 在 kray 项目
test_path = Path('python/ray/data/tests/test_xxx.py').resolve()
pkg_path = resolve_package_path(test_path)
print(f'Package path: {pkg_path}')
# 输出: /Users/franke/Desktop/git/kray/python/ray

# 在 ray 项目
# 输出: None
```

## 如何查看 pytest 运行时的 sys.path

### 方法 1：使用 pytest 插件打印

```python
cd /path/to/project
python -c "
import sys

class PathPlugin:
    def pytest_configure(self, config):
        print('=== sys.path at pytest_configure ===')
        for i, p in enumerate(sys.path[:10]):
            print(f'  {i}: {p}')

import pytest
pytest.main(['path/to/test.py', '--collect-only'], plugins=[PathPlugin()])
"
```

### 方法 2：追踪 ray 导入时的 sys.path

```python
cd /path/to/project
python -c "
import sys
original_import = __builtins__.__import__

first_import = True
def tracking_import(name, *args, **kwargs):
    global first_import
    if name == 'ray' and first_import:
        first_import = False
        print('=== sys.path when importing ray ===')
        for i, p in enumerate(sys.path[:10]):
            print(f'  {i}: {p}')
    return original_import(name, *args, **kwargs)

__builtins__.__import__ = tracking_import

import pytest
pytest.main(['path/to/test.py', '--collect-only', '-q'])
"
```

**ray 项目结果**（正常）：
```
sys.path:
  0: /Users/franke/Desktop/git/ray/python/ray/data/tests
  1: site-packages/ray/thirdparty_files
  2: ...site-packages...
```

**kray 项目结果**（问题）：
```
sys.path:
  0: /Users/franke/Desktop/git/kray/python/ray/thirdparty_files
  1: /Users/franke/Desktop/git/kray/python  <-- 问题根源
  2: ...site-packages...
```

### 方法 3：使用 resolve_package_path 检查 pytest 的包发现

```python
from pathlib import Path
from _pytest.pathlib import resolve_package_path

test_path = Path('python/ray/data/tests/test_xxx.py').resolve()
pkg_path = resolve_package_path(test_path)

print(f'Test file: {test_path}')
print(f'Package path: {pkg_path}')
# 如果 pkg_path 不为 None，pytest 会把 pkg_path.parent 加入 sys.path
```

### 方法 4：在 conftest.py 开头添加调试代码

```python
# 在 conftest.py 最开头添加（临时调试用）
import sys
print("=== sys.path at conftest load ===")
for i, p in enumerate(sys.path[:10]):
    print(f"  {i}: {p}")
```

## 解决方案

### 方案 1：删除测试目录的 __init__.py（推荐）

```bash
rm /Users/franke/Desktop/git/kray/python/ray/data/tests/__init__.py
```

验证：
```bash
cd /Users/franke/Desktop/git/kray
python -m pytest python/ray/data/tests/test_context_propagation.py --collect-only -q
# 应该能正常收集测试
```

### 方案 2：在 kray 项目中编译安装

如果需要保留 `__init__.py` 文件（某些场景下需要），则必须编译本地代码：

```bash
cd /Users/franke/Desktop/git/kray
pip install -e "python[all]"
```

这会生成 `_raylet.so` 等编译产物，使本地 ray 包完整可用。

### 方案 3：PyCharm 配置

在 PyCharm 的 Run/Debug Configuration 中：
- 取消勾选 "Add content roots to PYTHONPATH"
- 取消勾选 "Add source roots to PYTHONPATH"

## 本次修复记录

**时间**：2026-03-02

**问题文件**：`/Users/franke/Desktop/git/kray/python/ray/data/tests/__init__.py`

**操作**：删除该文件

**验证结果**：
```bash
cd /Users/franke/Desktop/git/kray
python -m pytest python/ray/data/tests/test_context_propagation.py --collect-only -q
# 9 tests collected in 0.03s
```

## 注意事项

1. **版本兼容性**：即使解决了导入问题，如果 site-packages 中的 ray 版本与本地测试代码不兼容，仍可能出现 API 不匹配错误。

2. **其他测试目录**：如果其他测试目录也有多余的 `__init__.py`，可能需要一并处理：
   ```bash
   # 对比两个项目的 tests/__init__.py 文件差异
   diff <(cd /Users/franke/Desktop/git/ray && find python/ray -path "*/tests/__init__.py" -type f | sort) \
        <(cd /Users/franke/Desktop/git/kray && find python/ray -path "*/tests/__init__.py" -type f | sort)
   ```

3. **pytest import-mode**：pytest 7+ 支持 `--import-mode=importlib`，但在这种情况下效果有限，因为 conftest.py 的加载发生在 import-mode 生效之前。
