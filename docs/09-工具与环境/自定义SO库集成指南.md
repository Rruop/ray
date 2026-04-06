# Ray 自定义预编译 .so 集成指南

本文档详细介绍如何在 Ray 项目中集成自定义的预编译 .so 库。

---

## 一、目录结构

```
kray/
├── WORKSPACE                          # Bazel 工作区配置
├── BUILD.bazel                        # 根目录构建文件
├── bazel/
│   └── custom.BUILD                   # 自定义库的构建规则
├── thirdparty/
│   └── custom/
│       ├── include/
│       │   └── custom.h               # 头文件
│       └── lib/
│           └── libcustom.so           # 预编译的 .so
└── src/ray/
    └── core_worker/
        ├── BUILD.bazel
        └── my_feature.cc              # 使用 custom 库的代码
```

---

## 二、配置文件详解

### 文件 1：WORKSPACE

```python
# WORKSPACE（在文件末尾添加）

# ============================================================
# new_local_repository 规则
# ============================================================
# 作用：将本地目录注册为一个外部仓库，供 Bazel 使用
#
# 参数说明：
#   name: 仓库名称，后续用 @custom 引用
#   path: 本地目录路径（相对于 WORKSPACE 所在目录）
#   build_file: 指定该仓库使用的 BUILD 文件
#
# 为什么需要？
#   Bazel 默认只能访问 WORKSPACE 内的文件
#   外部依赖必须通过 *_repository 规则显式声明
# ============================================================
new_local_repository(
    name = "custom",
    path = "thirdparty/custom",
    build_file = "@io_ray//bazel:custom.BUILD",
)
```

---

### 文件 2：bazel/custom.BUILD

```python
# bazel/custom.BUILD
# 这个文件定义了如何构建/使用 custom 库

# ============================================================
# package() 函数
# ============================================================
# 作用：设置当前包（BUILD 文件所在目录）的默认属性
#
# default_visibility: 控制谁可以依赖这个包里的目标
#   "//visibility:public"  = 任何包都可以依赖
#   "//visibility:private" = 只有同包内可以依赖
#   ["//src/ray:__subpackages__"] = 只有指定包及其子包可以依赖
# ============================================================
package(default_visibility = ["//visibility:public"])


# ============================================================
# cc_import 规则
# ============================================================
# 作用：导入预编译的 C/C++ 库（.so/.a/.dylib）
#
# 这是使用预编译库的关键规则！
#
# 参数说明：
#   name: 目标名称，供其他规则依赖
#   shared_library: 动态库路径（相对于仓库根目录）
#   static_library: 静态库路径（二选一，优先用静态库）
#   interface_library: Windows .lib 文件（可选）
#   hdrs: 关联的头文件（可选，也可以在 cc_library 中声明）
#
# cc_import vs cc_library 区别：
#   cc_import: 导入已编译的库，不需要编译源码
#   cc_library: 从源码编译，或包装其他目标
#
# 注意：cc_import 不能直接被 cc_binary 依赖
#       需要通过 cc_library 包装后使用
# ============================================================
cc_import(
    name = "custom_import",
    shared_library = "lib/libcustom.so",# 如果有静态库，优先使用（避免运行时依赖问题）：
    # static_library = "lib/libcustom.a",
)


# ============================================================
# cc_library 规则
# ============================================================
# 作用：定义一个 C/C++ 库目标
#
# 这里用于包装 cc_import，提供头文件路径
#
# 参数说明：
#   name: 目标名称，这是其他代码应该依赖的目标
#   hdrs: 公开头文件列表（会被依赖方 #include）
#   srcs: 源文件和私有头文件（这里没有，因为是预编译库）
#   includes: 头文件搜索路径
#            - 添加后，依赖方可以写 #include "custom.h"
#            - 而不是 #include "thirdparty/custom/include/custom.h"
#   deps: 依赖的其他目标
#   copts: 编译选项（传给依赖方）
#   linkopts: 链接选项
#
# glob() 函数：
#   glob(["include/**/*.h"]) 匹配 include 目录下所有 .h 文件
#   ** 表示递归匹配子目录
# ============================================================
cc_library(
    name = "custom",
    hdrs = glob(["include/**/*.h"]),
    includes = ["include"],  # 添加到 -I 搜索路径
    deps = [":custom_import"],
    # 可选：如果库需要特定链接选项
    # linkopts = ["-lpthread"],
)


# ============================================================
# filegroup 规则
# ============================================================
# 作用：将一组文件组合成一个目标，供其他规则引用
#
# 这是 Bazel 中最基础的规则之一！
#
# 参数说明：
#   name: 目标名称
#   srcs: 包含的文件列表
#   visibility: 可见性（谁可以引用这个 filegroup）
#
# 用途：
#   1. 组织文件：把相关文件打包，方便引用
#   2. 传递文件：pkg_files 等打包规则需要 filegroup 作为输入
#   3. 复用路径：避免在多处重复写文件路径
#
# 为什么这里需要 filegroup？
#   pkg_files 规则需要知道要打包哪些文件
#   filegroup 把 .so 文件"导出"给 pkg_files 使用
#
# filegroup vs cc_import 区别：
oup: 只是文件的引用，不涉及编译链接
#   cc_import: 告诉 Bazel 这是一个可链接的库
# ============================================================
filegroup(
    name = "shared",
    srcs = ["lib/libcustom.so"],
    visibility = ["//visibility:public"],
)
```

---

### 文件 3：BUILD.bazel（根目录修改）

```python
# BUILD.bazel（根目录）

# ============================================================
# pkg_files 规则
# ============================================================
# 作用：定义要打包到发行包中的文件及其属性
#
# 这是 rules_pkg 提供的规则，用于构建可发布的包
#
# 参数说明：
#   name: 目标名称
#   srcs: 要打包的文件（通常是 filegroup 或具体文件）
#   attributes: 文件属性
#     - pkg_attributes(mode = "755"): 设置文件权限为 rwxr-xr-x
#     - 对于 .so 文件，需要执行权限
#   prefix: 文件在包中的路径前缀
#     - "ray/core/" 表示文件会被放到 ray/core/ 目录下
#     - 最终路径：ray/core/libcustom.so
#   renames: 重命名映射（可选）
#   visibility: 可见性
#
# pkg_files vs filegroup 区别：
#   filegroup: 只是文件引用，不涉及打包
#   pkg_files: 定义文件如何被打包（路径、权限等）
# ============================================================
pkg_files(
    name = "custom_lib_files",
    srcs = ["@custom//:shared"],  # 引用 filegroup
    attributes = pkg_attributes(mode = "755"),
    prefix = "ray/core/",
    visibility = ["//visibility:private"],
)


# ============================================================
# 修改现有的 ray_pkg_files
# ============================================================
# 找到项目中已有的 ray_pkg_files，添加你的文件
#
# filegroup 在这里的作用：
#   聚合多个 pkg_files，形成完整的包内容
# ============================================================
filegroup(
    name = "ray_pkg_files",
    srcs = [
        ":raylet_files",
        ":raylet_so_files",
        ":custom_lib_files",      # 新增：你的 .so
    ] + select({
        ":jemalloc": [":jemalloc_files"],
        "//conditions:default": [],
    }),
)


# ============================================================
# 修改 pyx_library (如果 _raylet 依赖你的库)
# ============================================================
# linkopts 中添加 rpath，确保运行时能找到 .so
#
# -Wl,-rpath,$ORIGIN/core 含义：
#   -Wl,    : 把后面的参数传给链接器
#   -rpath  : 设置运行时库搜索路径
#   $ORIGIN : 特殊变量，表示"当前 .so 文件所在目录"
#   /core   : 相对于 $ORIGIN 的子目录
#
# 最终效果：
#   _raylet.so 在 ray/ 目录
#   libcustom.so 在 ray/core/ 目录
#   _raylet.so 加载时会在 ray/core/ 查找 libcustom.so
# ============================================================
pyx_library(
    name = "_raylet",
    # ... 现有配置 ...
    cc_kwargs = dict(
        linkopts = select({
            "@platforms//os:osx": [
                "-Wl,-exported_symbols_list,$(location //:src/ray/ray_exported_symbols.lds)",
            ],
            "@platforms//os:windows": [],
            "//conditions:default": [
                "-Wl,--version-script,$(location //:src/ray/ray_version_script.lds)",
                "-Wl,-rpath,$ORIGIN/core",  # 新增
            ],
        }),
        linkstatic = 1,
    ),
    deps = [
        # ... 现有依赖 ...
        "@custom//:custom",  # 如果 _raylet 间接依赖
    ],
)
```

---

### 文件 4：使用库的 C++ 代码

**src/ray/core_worker/BUILD.bazel:**

```python
load("//bazel:ray.bzl", "ray_cc_library")

# ============================================================
# ray_cc_library 规则
# ============================================================
# 这是 Ray 项目自定义的宏，封装了 cc_library
# 添加了项目统一的编译选项（见 bazel/ray.bzl）
#
# 关键：在 deps 中添加 @custom//:custom
# ============================================================
ray_cc_library(
    name = "my_feature",
    srcs = ["my_feature.cc"],
    hdrs = ["my_feature.h"],
    deps = [
        "@custom//:custom",  # 依赖自定义库
        "//src/ray/common:ray_common",
        # 其他依赖...
    ],
)
```

**src/ray/core_worker/my_feature.cc:**

```cpp
#include "custom.h"  // 可以直接 include，因为设置了 includes = ["include"]

namespace ray {
namespace core {

void MyFeature::DoSomething() {
    // 调用自定义库的函数
    custom_init();
    custom_process(data);
}

}  // namespace core
}  // namespace ray
```

---

## 三、各规则关系图

```
┌─────────────────────────────────────────────────────────────────────┐
│                        WORKSPACE                                     │
│  new_local_repository(name="custom", path="thirdparty/custom", ...) │
└─────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     bazel/custom.BUILD                               │
│                                                                      │
│  ┌──────────────────┐    ┌──────────────────┐    ┌───────────────┐ │
│  │   cc_import      │    │   cc_library     │    │  filegroup    │ │
│  │   "custom_import"│◄───│   "custom"       │    │  "shared"     │ │
│  │                  │deps│                  │    │               │ │
│  │ shared_library=  │    │ hdrs=glob([...]) │    │ srcs=[.so]    │ │
│  │ "lib/libcustom.so"    │ includes=["include"]  │               │ │
│  └──────────────────┘    └──────────────────┘    └───────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
           │                        │                      │
           │                        │                      │
           ▼                        ▼                      ▼
    ┌──────────────┐      ┌─────────────────┐      ┌─────────────────┐
    │   链接时     │      │   编译时        │      │   打包时        │
    │              │      │                 │      │                 │
    │ 提供符号解析  │      │ 提供头文件路径  │      │ pkg_files 引用  │
    │ -lcustom     │      │ -I.../include   │      │ 复制到 wheel    │
    └──────────────┘      └─────────────────┘      └─────────────────┘
```

---

## 四、Bazel 规则对比总结

| 规则 | 作用 | 输入 | 输出 | 使用场景 |
|------|------|------|------|---------|
| `new_local_repository` | 注册外部目录 | 本地路径 | @仓库名 | 引入第三方代码 |
| `cc_import` | 导入预编译库 | .so/.a 文件 | 可链接目标 | 预编译库 |
| `cc_library` | 定义 C++ 库 | 源码/头文件/deps | 可依赖目标 | 编译或包装 |
| `filegroup` | 文件分组 | 文件列表 | 文件集合目标 | 组织/传递文件 |
| `pkg_files` | 打包文件 | filegroup/文件 | 带属性的文件集 | 构建 wheel |
| `pkg_zip` | 创建压缩包 | pkg_files | .zip 文件 | 最终打包 |

---

## 五、.so 加载机制详解

### 5.1 两种链接方式

#### 方式 A：静态链接（推荐）

```
编译阶段：
your_lib.a ──链接──> _raylet.so
                       │
                       └── 包含 your_lib 的代码，不依赖外部 .so
```

**优点**：不需要 rpath，不需要打包 .so，运行时无依赖问题。

#### 方式 B：动态链接

```
wheel 包结构：
ray/
├── _raylet.so          # 依赖 libcustom.so
└── core/
    └── libcustom.so    # 必须打包
```

**运行时查找顺序**：
```
1. RPATH (编译时嵌入，如 $ORIGIN/core)
      ↓
2. LD_LIBRARY_PATH 环境变量
      ↓
3. /etc/ld.so.cache
      ↓
4. /lib, /usr/lib 系统路径
```

### 5.2 Ray 确保找到正确 .so 的方法

1. **静态链接**（`linkstatic = 1`）：大部分依赖打入 `_raylet.so`
2. **绝对路径加载**：`ctypes.CDLL(os.path.join(os.path.dirname(__file__), "..."))`
3. **RPATH 设置**：`-Wl,-rpath,$ORIGIN/core`
4. **LD_PRELOAD**：对于 jemalloc 等特殊库

---

## 六、构建与验证

```bash
# 1. 编译测试
bazel build //src/ray/core_worker:my_feature

# 2. 查看依赖关系
bazel query "deps(@custom//:custom)" --output=graph

# 3. 编译 _raylet.so
bazel build //:_raylet

# 4. 检查 .so 依赖（Linux）
ldd bazel-bin/python/ray/_raylet.so | grep custom
# 输出: libcustom.so => not found  (正常，需要 rpath 生效)

# 5. 检查 RPATH
readelf -d bazel-bin/python/ray/_raylet.so | grep -E "RPATH|RUNPATH"
# 输出应包含: $ORIGIN/core

# 6. 构建完整包
bazel build //:ray_pkg_zip

# 7. 验证 .so 是否被打包
unzip -l bazel-bin/ray_pkg.zip | grep custom
# 输出: ray/core/libcustom.so

# 8. 构建 wheel
cd python && pip wheel -v -w dist . --no-deps

# 9. 检查 wheel 内容
unzip -l dist/ray-*.whl | grep custom
```

---

## 七、常见问题排查

### Q1: 编译时找不到头文件

```
fatal error: custom.h: No such file or directory
```

**解决**：检查 `cc_library` 的 `includes` 路径是否正确。

### Q2: 链接时找不到符号

```
undefined reference to `custom_init'
```

**解决**：确保 `cc_import` 的 `shared_library` 路径正确，且 .so 文件存在。

### Q3: 运行时找不到 .so

```
error while loading shared libraries: libcustom.so: cannot open
```

**解决**：
1. 检查 `pkg_files` 是否正确打包
2. 检查 `linkopts` 中的 rpath 是否设置
3. 临时方案：`export LD_LIBRARY_PATH=/path/to/ray/core`

### Q4: 使用 bazel query 调试

```bash
# 查看谁依赖了 custom
bazel query "rdeps(//..., @custom//:custom)" --output=package

# 查看 custom 依赖了谁
bazel query "deps(@custom//:custom)"

# 可视化依赖图
bazel query "deps(@custom//:custom)" --output=graph | dot -Tpng > deps.png
```

---

## 八、静态库方案（推荐）

如果你有静态库 `.a`，配置更简单：

```python
# bazel/custom.BUILD
cc_import(
    name = "custom_import",
    static_library = "lib/libcustom.a",  # 使用静态库
)

cc_library(
    name = "custom",
    hdrs = glob(["include/**/*.h"]),
    includes = ["include"],
    deps = [":custom_import"],
)

# 不需要 filegroup
# 不需要 pkg_files
# 不需要 rpath
# 代码直接被打进 _raylet.so
```

**静态库优点**：
- 不需要打包 .so 到 wheel
- 不需要设置 rpath
- 不会有运行时找不到库的问题
- 部署更简单

---

## 九、完整检查清单

- [ ] 创建 `thirdparty/custom/` 目录结构
- [ ] 放置 `.so` 和头文件
- [ ] 在 `WORKSPACE` 添加 `new_local_repository`
- [ ] 创建 `bazel/custom.BUILD`
- [ ] 在使用处的 `BUILD.bazel` 添加 `deps`
- [ ] 在根 `BUILD.bazel` 添加 `pkg_files`
- [ ] 修改 `ray_pkg_files` 包含新的 `pkg_files`
- [ ] 添加 rpath 到 `linkopts`（动态链接时）
- [ ] 运行 `bazel build` 测试编译
- [ ] 运行 `unzip -l` 验证打包
