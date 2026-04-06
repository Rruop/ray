# Ray Data 使用说明

## 1. 概述

Ray Data 是构建于 Ray 之上的**分布式数据处理库**，专为 AI 工作负载设计，提供高性能且可扩展的 API，适用于**批量推理、数据预处理和 ML 训练数据加载**等场景。

其核心特性是内置的**流式执行引擎（Streaming Execution Engine）**，能够高效处理大规模数据集，在 CPU 和 GPU 异构集群上保持高资源利用率。

### 安装

```bash
pip install -U 'ray[data]'
```

## 2. 核心优势

| 优势 | 说明 |
|------|------|
| **GPU 利用率高** | 流式传输数据在 CPU 预处理和 GPU 推理/训练之间，保持 GPU 持续活跃，降低成本 |
| **框架友好** | 与 vLLM、PyTorch、HuggingFace、TensorFlow 等主流 AI 框架深度集成 |
| **多模态数据支持** | 基于 Apache Arrow 和 Pandas，支持 Parquet、Lance、JSON、CSV、图片、音频、视频等格式 |
| **自动弹性扩展** | 基于 Ray 构建，代码无需修改即可从单机扩展到数百节点，处理数百 TB 数据 |

### 行业应用案例

| 场景 | 公司 | 说明 |
|------|------|------|
| 训练数据加载 | Pinterest | 模型训练的最后一公里数据处理 |
| 训练数据加载 | DoorDash | 利用 Ray Data 提升模型训练效率 |
| 训练数据加载 | Instacart | 构建分布式 ML 模型训练 |
| 批量推理 | ByteDance | 多模态 LLM 推理扩展至 200 TB 规模 |
| 批量推理 | Spotify | 基于 Ray Data 构建 ML 批量推理平台 |
| 视频处理 | Sewer AI | 视频目标检测速度提升 3 倍 |

---

## 3. 核心概念

### 3.1 Dataset 与 Block

**Dataset** 是面向用户的核心 API，代表一个分布式数据集合。

**架构图**（参考官方 [dataset-arch-with-blocks.svg](https://docs.ray.io/en/latest/_images/dataset-arch-with-blocks.svg)）：

```
┌─────────────────────────────────────────────────────────────┐
│                     Driver 进程                              │
│                                                              │
│   Dataset ──────────────────────────────────────────────     │
│   │  持有 Block 的 ObjectRef 引用                             │
│   │                                                          │
│   ├── ObjectRef(Block_0)  ──→  [1000 rows]                  │
│   ├── ObjectRef(Block_1)  ──→  [1000 rows]                  │
│   └── ObjectRef(Block_2)  ──→  [1000 rows]                  │
│                                                              │
└──────────────────────┬──────────────────────────────────────┘
                       │ ObjectRef 指向
                       ▼
┌─────────────────────────────────────────────────────────────┐
│              Ray Object Store（分布式共享内存）                │
│                                                              │
│   ┌──────────┐   ┌──────────┐   ┌──────────┐               │
│   │ Block_0  │   │ Block_1  │   │ Block_2  │               │
│   │ (Arrow   │   │ (Arrow   │   │ (Arrow   │               │
│   │  Table)  │   │  Table)  │   │  Table)  │               │
│   └──────────┘   └──────────┘   └──────────┘               │
└─────────────────────────────────────────────────────────────┘
```

核心特性：
- **Lazy 执行**：每一步定义的算子不会立即执行，直到遇到终端操作（如 `write`、`show`、`materialize`）才触发
- **Block** 是数据存储与传输的最小单元，底层默认使用 `pyarrow.Table` 存储；如果数据结构复杂无法用 pyarrow 表示，自动转为 `pandas.DataFrame`
- Dataset 位于 Driver 进程中，操作的是 Block 的 `ObjectRef`，实际数据存储在 Ray Object Store 中

### 3.2 Logical Plan 与 Physical Plan

Ray Data 采用**两层算子抽象**，类似数据库的查询优化器设计。

**执行计划转换流程**（参考官方 [get_execution_plan.svg](https://docs.ray.io/en/latest/_images/get_execution_plan.svg)）：

```
用户代码                      Logical Plan              Physical Plan
──────────                   ──────────               ──────────────
ds = ray.data.read_csv(xx)        Read                 InputDataBuffer
                                   │                        │
ds = ds.map(MapActor)          MapRows              TaskPoolMapOperator(ReadCSV)
                                   │                        │
ds = ds.map_batches(BatchActor) MapBatches           ActorPoolMapOperator(Map)
                                   │                        │
ds.write_parquet(xxx)           Write                ActorPoolMapOperator(MapBatches)
                                                          │
                                                    TaskPoolMapOperator(WriteCSV)
```

**Logical Plan 示例**（可通过 API 查看）：

```python
dataset = ray.data.range(100)
dataset = dataset.add_column("test", lambda x: x["id"] + 1)
dataset = dataset.select_columns("test")

# 打印 Logical Plan
# Project
# +- MapBatches(add_column)
#    +- Dataset(schema={id: int64})
```

转换映射关系：

| Logical Operator | Physical Operator | 执行方式 |
|---|---|---|
| `Read` | `TaskPoolMapOperator` | Ray Task |
| `map`（传入函数） | `TaskPoolMapOperator` | Ray Task |
| `map`（传入类） | `ActorPoolMapOperator` | Ray Actor |
| `map_batches`（传入函数） | `TaskPoolMapOperator` | Ray Task |
| `map_batches`（传入类） | `ActorPoolMapOperator` | Ray Actor |
| `Write` | `TaskPoolMapOperator` | Ray Task |

**Optimizer** 的优化作用：连续多个基于 Task 的 map 操作可被自动融合为一个操作，减少序列化开销。

### 3.3 流式执行模型

**流式拓扑结构**（参考官方 [streaming-topology.svg](https://docs.ray.io/en/latest/_images/streaming-topology.svg)）：

```
┌───────────────┐    ┌───────────────┐    ┌───────────────┐    ┌───────────────┐
│ InputData     │    │ TaskPool      │    │ ActorPool     │    │ TaskPool      │
│ Buffer        │    │ MapOperator   │    │ MapOperator   │    │ MapOperator   │
│               │    │ (ReadCSV)     │    │ (Inference)   │    │ (WriteCSV)    │
│   [data]──────┼──→ │   inqueue     │    │   inqueue     │    │   inqueue     │
│               │    │      │        │    │      │        │    │      │        │
│               │    │      ▼        │    │      ▼        │    │      ▼        │
│               │    │  Ray Tasks    │    │  Ray Actors   │    │  Ray Tasks    │
│               │    │      │        │    │      │        │    │      │        │
│               │    │      ▼        │    │      ▼        │    │      ▼        │
│               │    │   outqueue────┼──→ │   outqueue────┼──→ │   outqueue    │
└───────────────┘    └───────────────┘    └───────────────┘    └───────────────┘
                         CPU 操作              GPU 操作             CPU 操作
                      ◄──── 多个 Operator 并行执行，CPU/GPU 可同时利用 ────►
```

核心设计要点：
- 每个 Operator 的**输入队列 = 前置 Operator 的输出队列**（同一对象引用，零拷贝传递）
- 多个 Operator **并行执行**：CPU 预处理和 GPU 推理可同时进行
- 数据以 `RefBundle`（Block 引用集合）为单位在 Operator 间流动
- **流水线优势**：即使输入数据远大于机器内存，也能高效处理

> **对比 Spark**：Spark 通常需要等待一个 Stage 全部完成后才启动下一个 Stage。Ray Data 的流式模型允许不同 Stage 的 Operator 同时执行，这是其在 AI 场景下的核心优势。

### 3.4 调度循环（Scheduling Loop）

StreamingExecutor 的核心调度循环如下：

```
while 未全部完成:
    ┌─────────────────────────────────────────┐
    │ 1. 检查已完成的 Task/Actor 结果          │
    │    op.update_completed_tasks()          │
    ├─────────────────────────────────────────┤
    │ 2. 将结果放入 outqueue                   │
    │    while op.has_next():                 │
    │        state.outqueue.append(op.next()) │
    ├─────────────────────────────────────────┤
    │ 3. 从 inqueue 取数据，提交新的 Task      │
    │    while inqueue and should_add_input(): │
    │        op.add_input(inqueue.popleft())  │
    ├─────────────────────────────────────────┤
    │ 4. 检查终止条件                          │
    └─────────────────────────────────────────┘
    yield output_node.outqueue.popleft()  # 产出结果
```

---

## 4. 数据读取 API

### 4.1 文件数据源

Ray Data 支持多种文件格式的读取：

| 格式 | API | 示例 |
|------|-----|------|
| Parquet | `ray.data.read_parquet()` | 列式存储，推荐用于结构化数据 |
| CSV | `ray.data.read_csv()` | 通用表格格式 |
| JSON | `ray.data.read_json()` | 半结构化数据 |
| Text | `ray.data.read_text()` | 纯文本文件 |
| Images | `ray.data.read_images()` | 图片文件（JPEG/PNG 等） |
| Binary | `ray.data.read_binary_files()` | 任意二进制文件 |
| TFRecords | `ray.data.read_tfrecords()` | TensorFlow 格式 |

```python
import ray

# ── Parquet（推荐，支持列裁剪）──
ds = ray.data.read_parquet(
    "s3://anonymous@ray-example-data/iris.parquet",
    columns=["sepal.length", "variety"],  # 列裁剪，只读取需要的列
)
print(ds.schema())
# Column        Type
# ------        ----
# sepal.length  double
# variety       string

# ── CSV ──
ds = ray.data.read_csv("s3://anonymous@ray-example-data/iris.csv")
print(ds.schema())
# Column             Type
# ------             ----
# sepal length (cm)  double
# sepal width (cm)   double
# petal length (cm)  double
# petal width (cm)   double
# target             int64

# ── 图片 ──
ds = ray.data.read_images("s3://anonymous@ray-example-data/batoidea/JPEGImages/")
print(ds.schema())
# Column  Type
# ------  ----
# image   ArrowTensorTypeV2(shape=(32, 32, 3), dtype=uint8)

# ── 文本 ──
ds = ray.data.read_text("s3://anonymous@ray-example-data/this.txt")

# ── 二进制文件 ──
ds = ray.data.read_binary_files("s3://anonymous@ray-example-data/documents")

# ── 压缩文件 ──
ds = ray.data.read_csv(
    "s3://anonymous@ray-example-data/iris.csv.gz",
    arrow_open_stream_args={"compression": "gzip"},
)
```

### 4.2 存储系统

```python
# ── 本地磁盘 ──
ds = ray.data.read_parquet("local:///tmp/iris.parquet")       # 仅本地节点
ds = ray.data.read_parquet("/mnt/cluster_storage/data.parquet")  # NFS 共享存储

# ── Amazon S3 ──
ds = ray.data.read_parquet("s3://anonymous@ray-example-data/iris.parquet")

# ── Google Cloud Storage ──
import gcsfs
filesystem = gcsfs.GCSFileSystem(project="my-google-project")
ds = ray.data.read_parquet("gs://my-bucket/data.parquet", filesystem=filesystem)

# ── Azure Blob Storage ──
import adlfs
fs = adlfs.AzureBlobFileSystem(account_name="azureopendatastorage")
ds = ray.data.read_parquet("az://ray-example-data/iris.parquet", filesystem=fs)

# ── Hugging Face Datasets ──
from huggingface_hub import HfFileSystem
ds = ray.data.read_parquet(
    "hf://datasets/wikimedia/wikipedia",
    file_extensions=["parquet"],
    filesystem=HfFileSystem(token="YOUR_TOKEN"),
)
```

### 4.3 内存数据源

```python
import ray
import numpy as np
import pandas as pd
import pyarrow as pa

# ── Python 字典列表 ──
ds = ray.data.from_items([
    {"food": "spam", "price": 9.34},
    {"food": "ham", "price": 5.37},
    {"food": "eggs", "price": 0.94},
])

# ── NumPy 数组 ──
array = np.ones((32, 100))
ds = ray.data.from_numpy(array)

# ── Pandas DataFrame ──
df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
ds = ray.data.from_pandas(df)

# ── PyArrow Table ──
table = pa.table({"food": ["spam", "ham"], "price": [9.34, 5.37]})
ds = ray.data.from_arrow(table)

# ── 合成数据 ──
ds = ray.data.range(10000)                          # 整数序列
ds = ray.data.range_tensor(10, shape=(64, 64))      # Tensor 序列
```

### 4.4 数据库与分布式框架

```python
# ── SQL 数据库（MySQL / PostgreSQL / Snowflake）──
import mysql.connector

def create_connection():
    return mysql.connector.connect(
        user="admin", password="...",
        host="example.rds.amazonaws.com",
        database="example",
    )

ds = ray.data.read_sql("SELECT * FROM movie", create_connection)
ds = ray.data.read_sql(
    "SELECT title, score FROM movie WHERE year >= 1980",
    create_connection,
)

# ── MongoDB ──
ds = ray.data.read_mongo(
    uri="mongodb://localhost:27017",
    database="my_db",
    collection="my_collection",
    pipeline=[{"$match": {"col": {"$gte": 0, "$lt": 10}}}],
)

# ── BigQuery ──
ds = ray.data.read_bigquery(
    project_id="my_project",
    query="SELECT * FROM `dataset.table` LIMIT 1000",
)

# ── Kafka ──
ds = ray.data.read_kafka(
    topics="my-topic",
    bootstrap_servers="localhost:9092",
    start_offset=0,
    end_offset=1000,
)

# ── Spark DataFrame ──
import raydp
spark = raydp.init_spark(app_name="example", num_executors=2,
                         executor_cores=2, executor_memory="500MB")
spark_df = spark.createDataFrame([(i, str(i)) for i in range(10000)], ["col1", "col2"])
ds = ray.data.from_spark(spark_df)

# ── Dask DataFrame ──
import dask.dataframe as dd
ddf = dd.from_pandas(df, npartitions=4)
ds = ray.data.from_dask(ddf)

# ── PyTorch Dataset ──
from torchvision import datasets
tds = datasets.CIFAR10(root="data", train=True, download=True)
ds = ray.data.from_torch(tds)

# ── Iceberg Table ──
from pyiceberg.expressions import EqualTo
ds = ray.data.read_iceberg(
    table_identifier="db_name.table_name",
    row_filter=EqualTo("column_name", "literal_value"),
)
```

### 4.5 完整数据源 API 速查表

| 分类 | API | 数据源 |
|------|-----|--------|
| **文件** | `read_parquet()` | Parquet |
| | `read_csv()` | CSV |
| | `read_json()` | JSON |
| | `read_text()` | Text |
| | `read_images()` | 图片 |
| | `read_binary_files()` | 二进制 |
| | `read_tfrecords()` | TFRecords |
| **数据库** | `read_sql()` | MySQL/PostgreSQL/Snowflake |
| | `read_mongo()` | MongoDB |
| | `read_bigquery()` | BigQuery |
| | `read_databricks_tables()` | Databricks |
| **消息队列** | `read_kafka()` | Kafka |
| **表格式** | `read_iceberg()` | Iceberg |
| **内存对象** | `from_items()` | Python 字典 |
| | `from_numpy()` | NumPy |
| | `from_pandas()` | Pandas |
| | `from_arrow()` | PyArrow |
| **分布式框架** | `from_spark()` | Spark |
| | `from_dask()` | Dask |
| | `from_modin()` | Modin |
| | `from_torch()` | PyTorch |
| | `from_tf()` | TensorFlow |
| **合成数据** | `range()` | 整数序列 |
| | `range_tensor()` | Tensor 序列 |
| **自定义** | `read_datasource()` | 自定义 Datasource |

---

## 5. 数据转换 API

### 5.1 map — 逐行处理（一对一）

适合非向量化的逐行操作，每行输入产生一行输出。

```python
import os
from typing import Any, Dict
import ray

# 示例：解析文件路径
def parse_filename(row: Dict[str, Any]) -> Dict[str, Any]:
    row["filename"] = os.path.basename(row["path"])
    return row

ds = (
    ray.data.read_images(
        "s3://anonymous@ray-example-data/image-datasets/simple",
        include_paths=True,
    )
    .map(parse_filename)
)
```

使用类实现（有状态，通过 Ray Actor 执行）：

```python
class Preprocessor:
    def __init__(self):
        self.model = load_model()  # 初始化一次

    def __call__(self, row: Dict[str, Any]) -> Dict[str, Any]:
        row["prediction"] = self.model.predict(row["feature"])
        return row

ds = ds.map(Preprocessor, concurrency=4)
```

### 5.2 flat_map — 逐行展开（一对多）

每行输入可产生零或多行输出。

```python
from typing import Any, Dict, List
import ray

def duplicate_row(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [row] * 2

result = ray.data.range(3).flat_map(duplicate_row).take_all()
# [{'id': 0}, {'id': 0}, {'id': 1}, {'id': 1}, {'id': 2}, {'id': 2}]

# 实际应用：文本分词
def tokenize(row):
    return [{"word": w} for w in row["text"].split()]

ds = ds.flat_map(tokenize)
```

### 5.3 map_batches — 批量处理（推荐）

适合向量化操作，性能显著优于 `map()`。支持三种批数据格式：

**NumPy 格式（默认）**：

```python
from typing import Dict
import numpy as np
import ray

def increase_brightness(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
    batch["image"] = np.clip(batch["image"] + 4, 0, 255)
    return batch

ds = (
    ray.data.read_images("s3://anonymous@ray-example-data/image-datasets/simple")
    .map_batches(increase_brightness, batch_format="numpy")
)
```

**Pandas 格式**：

```python
import pandas as pd
import ray

def drop_nas(batch: pd.DataFrame) -> pd.DataFrame:
    return batch.dropna()

ds = (
    ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")
    .map_batches(drop_nas, batch_format="pandas")
)
```

**PyArrow 格式**（零拷贝，性能最优）：

```python
import pyarrow as pa
import pyarrow.compute as pc
import ray

def drop_nas(batch: pa.Table) -> pa.Table:
    return pc.drop_null(batch)

ds = (
    ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")
    .map_batches(drop_nas, batch_format="pyarrow")
)
```

**使用 Polars（推荐用于复杂数据处理）**：

```python
import pyarrow as pa

def udf(table: pa.Table):
    import polars as pl
    df = pl.from_arrow(table)
    # 利用 Polars 强大的 API 进行处理
    df = df.with_columns(pl.col("value") * 2)
    return df.to_arrow()

ds.map_batches(udf, batch_format="pyarrow")
```

**批格式选择指南**：

```
                        Block 内部存储格式
                    ┌──────────────────────┐
                    │  pyarrow.Table (默认) │
                    └─────────┬────────────┘
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
      batch_format=      batch_format=    batch_format=
       "pyarrow"          "pandas"         "numpy"
       (零拷贝✓)         (需转换)         (需转换)
```

> 大多数 Ray Data 数据源生成 Arrow Block，使用 `batch_format="pyarrow"` 可避免不必要的数据转换。

### 5.4 有状态转换（类 + Actor）

适合需要昂贵初始化（如加载模型）的操作。传入类时 Ray Data 自动使用 Actor，模型只初始化一次。

**CPU 示例**：

```python
from typing import Dict
import numpy as np
import torch
import ray

class TorchPredictor:
    def __init__(self):
        self.model = torch.nn.Identity()
        self.model.eval()

    def __call__(self, batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        inputs = torch.as_tensor(batch["data"], dtype=torch.float32)
        with torch.inference_mode():
            batch["output"] = self.model(inputs).detach().numpy()
        return batch

ds = (
    ray.data.from_numpy(np.ones((32, 100)))
    .map_batches(
        TorchPredictor,
        compute=ray.data.ActorPoolStrategy(size=2),
    )
)
```

**GPU 示例**（3 处关键修改）：

```python
class TorchPredictor:
    def __init__(self):
        # ① 模型移至 GPU
        self.model = torch.nn.Identity().cuda()
        self.model.eval()

    def __call__(self, batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        # ② 数据移至 GPU
        inputs = torch.as_tensor(batch["data"], dtype=torch.float32).cuda()
        with torch.inference_mode():
            # ③ 结果移回 CPU
            batch["output"] = self.model(inputs).detach().cpu().numpy()
        return batch

ds = (
    ray.data.from_numpy(np.ones((32, 100)))
    .map_batches(
        TorchPredictor,
        num_gpus=1,             # 每个 Actor 分配 1 个 GPU
        batch_size=4,           # 根据 GPU 内存调整
        compute=ray.data.ActorPoolStrategy(size=2),
    )
)
```

**函数 vs 类的对比**：

```
┌─────────────────────────┬──────────────────────────────┐
│     函数（无状态）        │       类（有状态）             │
├─────────────────────────┼──────────────────────────────┤
│ 执行方式: Ray Task       │ 执行方式: Ray Actor           │
│ 每次调用独立执行          │ Actor 初始化一次，复用执行      │
│ 适合: 轻量计算           │ 适合: 加载模型、建立连接        │
│ 无 GPU 状态复用          │ GPU 显存只初始化一次            │
│                          │                               │
│ ds.map_batches(fn)       │ ds.map_batches(MyClass,       │
│                          │   compute=ActorPoolStrategy   │
│                          │     (size=N))                 │
└─────────────────────────┴──────────────────────────────┘
```

### 5.5 其他转换操作

```python
# ── 过滤 ──
ds = ds.filter(lambda row: row["score"] > 0.5)

# ── 添加列（表达式方式，支持优化器优化）──
from ray.data.expressions import col
ds = ray.data.range(10).with_column("id_2", col("id") * 2)

# ── 选择列 ──
ds = ds.select_columns(["col1", "col2"])

# ── 排序 ──
ds = ds.sort("col1")

# ── 随机打乱 ──
ds = ds.random_shuffle()

# ── 重新分区 ──
ds = ds.repartition(100)

# ── 分组聚合 ──
import pandas as pd

def normalize_features(group: pd.DataFrame) -> pd.DataFrame:
    target = group["target"]
    group = (group - group.min()) / group.std()
    group["target"] = target
    return group

ds = (
    ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")
    .groupby("target")
    .map_groups(normalize_features, batch_format="pandas")
)
```

### 5.6 异步转换（适用于 I/O 密集型）

```python
import ray
from typing import Dict
import numpy as np

class AsyncTransform:
    async def __call__(self, batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        # 适合 HTTP 请求、数据库查询等 I/O 操作
        return batch

ds = ray.data.range(10).map_batches(AsyncTransform)
```

### 5.7 转换 API 速查表

| API | 模式 | 适用场景 |
|-----|------|---------|
| `map()` | 一对一（行） | 解析路径、添加字段 |
| `flat_map()` | 一对多（行） | 分词、展开嵌套 |
| `map_batches()` | 批量（推荐） | 向量化计算、模型推理 |
| `filter()` | 过滤 | 条件筛选 |
| `with_column()` | 列表达式 | 计算新列（支持优化） |
| `select_columns()` | 列选择 | 列裁剪 |
| `sort()` | 排序 | 全局排序（会物化数据） |
| `random_shuffle()` | 随机打乱 | 训练数据混洗 |
| `repartition()` | 重新分区 | 调整并行度 |
| `groupby().map_groups()` | 分组 | 按组归一化 |

---

## 6. 数据输出

```python
# ── 写入文件 ──
ds.write_csv("/output/path/")
ds.write_parquet("s3://bucket/output/")
ds.write_json("/output/path/")

# ── 写入数据库 ──
ds.write_mongo(uri="mongodb://...", database="db", collection="col")
ds.write_bigquery(project_id="project", dataset="dataset.table")

# ── 收集为 Python 对象 ──
rows = ds.take(10)           # 取前 10 行
rows = ds.take_all()         # 取所有行
df = ds.to_pandas()          # 转为 Pandas DataFrame
table = ds.to_arrow()        # 转为 Arrow Table

# ── 展示数据 ──
ds.show(limit=5)

# ── 物化（触发执行并缓存结果）──
materialized_ds = ds.materialize()
```

---

## 7. 典型使用场景

### 7.1 批量推理（Batch Inference）

**流式批量推理架构**（参考官方 [stream-example.png](https://docs.ray.io/en/latest/_images/stream-example.png)）：

```
┌──────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────┐
│          │     │              │     │              │     │          │
│  Read    │────→│  Preprocess  │────→│  Inference   │────→│  Write   │
│  (CPU)   │     │  (CPU)       │     │  (GPU)       │     │  (CPU)   │
│          │     │              │     │              │     │          │
└──────────┘     └──────────────┘     └──────────────┘     └──────────┘
                  ◄────── 流水线并行执行，CPU/GPU 同时利用 ──────►
                  预处理 batch N+1 与推理 batch N 并行执行
```

**HuggingFace 文本分类示例**：

```python
import ray
import pandas as pd

class TextClassifier:
    def __init__(self):
        from transformers import pipeline
        self.pipe = pipeline("text-classification")

    def __call__(self, batch: pd.DataFrame) -> pd.DataFrame:
        results = self.pipe(list(batch["text"]))
        result_df = pd.DataFrame(results)
        return pd.concat([batch, result_df], axis=1)

ds = ray.data.read_text(
    "s3://anonymous@ray-example-data/sms_spam_collection_subset.txt"
)
ds = ds.map_batches(
    TextClassifier,
    compute=ray.data.ActorPoolStrategy(size=2),
    batch_size=64,
    batch_format="pandas",
    num_gpus=1,
)
ds.write_parquet("/output/predictions/")
```

**PyTorch GPU 推理示例**：

```python
from typing import Dict
import numpy as np
import torch
import torch.nn as nn
import ray

class TorchPredictor:
    def __init__(self):
        self.model = nn.Sequential(
            nn.Linear(in_features=100, out_features=1),
            nn.Sigmoid(),
        ).cuda()
        self.model.eval()

    def __call__(self, batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        tensor = torch.as_tensor(batch["data"], dtype=torch.float32).cuda()
        with torch.inference_mode():
            return {"output": self.model(tensor).cpu().numpy()}

ds = ray.data.from_numpy(np.ones((1, 100)))
predictions = ds.map_batches(
    TorchPredictor,
    num_gpus=1,
    batch_size=4,
    compute=ray.data.ActorPoolStrategy(size=2),
)
predictions.show(limit=1)
```

**vLLM LLM 推理示例**：

```python
import ray
from ray.data.llm import vLLMEngineProcessorConfig, build_processor

config = vLLMEngineProcessorConfig(
    model="unsloth/Llama-3.1-8B-Instruct",
    engine_kwargs={
        "enable_chunked_prefill": True,
        "max_num_batched_tokens": 4096,
        "max_model_len": 16384,
    },
    concurrency=1,
    batch_size=64,
)

processor = build_processor(
    config,
    preprocess=lambda row: dict(
        messages=[
            {"role": "system", "content": "You are a bot that responds with haikus."},
            {"role": "user", "content": row["item"]},
        ],
        sampling_params=dict(temperature=0.3, max_tokens=250),
    ),
    postprocess=lambda row: dict(answer=row["generated_text"]),
)

ds = ray.data.from_items(["Start of the haiku is: Complete this for me..."])
ds = processor(ds)
ds.show(limit=1)
```

### 7.2 数据预处理 Pipeline

```python
import ray
from typing import Dict
from numpy.typing import NDArray

ds = ray.data.read_csv("/data/input/iris.csv")

# 第一步：逐行处理
class RowProcessor:
    def __init__(self):
        print("RowProcessor initialized")

    def __call__(self, row):
        row["output"] = "processed"
        return row

# 第二步：批量处理
class BatchProcessor:
    def __init__(self):
        print("BatchProcessor initialized")

    def __call__(self, data: Dict[str, NDArray]):
        # 批量归一化等操作
        return data

ds = ds.map(RowProcessor, concurrency=5)
ds = ds.map_batches(BatchProcessor, concurrency=5, batch_size=1024)
ds.write_csv("/data/output/")
```

**Logical Plan 推导过程**：

```
用户代码                    Logical Plan                Physical Plan
─────────────              ────────────               ──────────────
read_csv(path)              Read                      InputDataBuffer
      │                       │                             │
.map(RowProcessor)          MapRows                   TaskPool(ReadCSV)
      │                       │                             │
.map_batches(BatchProc)     MapBatches                ActorPool(MapRows)
      │                       │                             │
.write_csv(output)          Write                     ActorPool(MapBatches)
                                                            │
                                                      TaskPool(WriteCSV)
```

### 7.3 ML 训练数据加载

```python
import ray
from ray.train.torch import TorchTrainer

# 准备训练数据
train_ds = ray.data.read_parquet("s3://bucket/train_data/")
train_ds = train_ds.random_shuffle()

# 作为 Ray Train 的数据输入
trainer = TorchTrainer(
    train_func,
    datasets={"train": train_ds},
    scaling_config=...,
)
trainer.fit()
```

### 7.4 分布式模型推理（多 GPU 分片）

使用 Placement Group 将模型分片到多个 GPU：

```python
import ray
from typing import Dict
import numpy as np
import torch

NUM_SHARDS = 2

@ray.remote
class ModelShard:
    def __init__(self):
        self.model = torch.nn.Linear(10, 10)

    def f(self, batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        return batch

class DistributedModel:
    def __init__(self):
        self.shards = [ModelShard.remote() for _ in range(NUM_SHARDS)]

    def __call__(self, batch):
        return {"out": np.array(
            ray.get([shard.f.remote(batch) for shard in self.shards])
        )}

def ray_remote_args_fn():
    from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy
    pg = ray.util.placement_group([{"CPU": 1}] * NUM_SHARDS)
    return {
        "scheduling_strategy": PlacementGroupSchedulingStrategy(
            placement_group=pg
        )
    }

ds = ray.data.range(10).map_batches(
    DistributedModel,
    ray_remote_args_fn=ray_remote_args_fn,
)
```

---

## 8. 自定义数据源

### 8.1 自定义 Datasource（读取）

```python
from ray.data.datasource import Datasource, FileBasedDatasource
from ray.data._internal.datasource.read_task import ReadTask

# ── 方式一：继承 Datasource（任意数据源）──
class MyDatasource(Datasource):
    def get_read_tasks(self, parallelism: int):
        def read_fn():
            import pyarrow as pa
            # 自定义读取逻辑
            yield pa.table({"col": [1, 2, 3]})

        return [ReadTask(read_fn, metadata=...)]

# ── 方式二：继承 FileBasedDatasource（文件数据源）──
class MyFileDatasource(FileBasedDatasource):
    def _read_stream(self, f, path):
        import pyarrow as pa
        # 从文件对象 f 读取数据
        data = f.read()
        yield pa.table({"content": [data]})

ds = ray.data.read_datasource(MyDatasource())
```

### 8.2 自定义 Datasink（写入）

```python
from ray.data.datasource import Datasink

class MyDatasink(Datasink):
    def write(self, blocks, ctx):
        for block in blocks:
            # 自定义写入逻辑：写入数据库、消息队列等
            ...

    @property
    def num_rows_per_write(self):
        return None  # 不限制

ds.write_datasink(MyDatasink())
```

---

## 9. 执行流程总结

```
用户代码（构建 Logical Plan，Lazy 不执行）:
  ds = ray.data.read_csv(path)            # → Read LogicalOperator
  ds = ds.map(Fn, concurrency=5)          # → MapRows LogicalOperator
  ds = ds.map_batches(Fn, batch_size=1K)  # → MapBatches LogicalOperator
  ds.write_csv(output)                    # → Write LogicalOperator → 触发执行 ⚡

内部执行流程:
  ┌─────────────────────────────────────────────────────────────┐
  │ Step 1: 构建 Logical Plan DAG                               │
  │    Read → MapRows → MapBatches → Write                     │
  ├─────────────────────────────────────────────────────────────┤
  │ Step 2: Optimizer 优化                                      │
  │    - 算子融合（合并连续 Task 算子）                            │
  │    - 列裁剪下推                                              │
  ├─────────────────────────────────────────────────────────────┤
  │ Step 3: Planner 转换为 Physical Plan                        │
  │    InputDataBuffer → TaskPool(Read) → ActorPool(Map)       │
  │      → ActorPool(MapBatch) → TaskPool(Write)               │
  ├─────────────────────────────────────────────────────────────┤
  │ Step 4: StreamingExecutor 流式执行                           │
  │    - build_streaming_topology: 构建拓扑                     │
  │    - setup_queues: 连接 Operator 队列                       │
  │    - scheduling_loop: 调度循环                               │
  │        取数据 → 提交 Task/Actor → 结果入队 → 产出结果         │
  │    - 多 Operator 并行执行（CPU/GPU 流水线）                    │
  └─────────────────────────────────────────────────────────────┘
```

---

## 10. 性能优化指南

### 10.1 转换优化

| 策略 | 说明 |
|------|------|
| **优先用 `map_batches`** | 向量化操作性能远优于逐行 `map` |
| **用类传入 GPU 推理** | Actor 模式只加载模型一次，避免重复初始化 |
| **启用 Polars 排序** | `ctx.use_polars_sort = True` 加速 `sort()` 和 `map_groups()` |

### 10.2 读取优化

```python
# ✅ 列裁剪（Projection Pushdown）— 在文件扫描层过滤列
ds = ray.data.read_parquet("data.parquet", columns=["col1", "col2"])

# ❌ 先读取全部列，再用 select_columns 过滤（浪费 I/O）
ds = ray.data.read_parquet("data.parquet").select_columns(["col1", "col2"])

# ✅ 增加读取并行度
ds = ray.data.read_parquet(path, ray_remote_args={"num_cpus": 0.25})

# ✅ 手动控制输出 Block 数量
ds = ray.data.read_csv(paths, override_num_blocks=16)
```

**Block 数量自动调优规则**：

```
默认起始值: 200 blocks
     │
     ▼
最小 Block 大小: 1 MiB（避免大量小 Block 开销）
     │
     ▼
最大 Block 大小: 128 MiB（避免 OOM）
     │
     ▼
可用 CPU 数: 至少 2x CPU 数的读取任务
```

### 10.3 内存管理

```python
# ✅ 减小 batch_size 降低峰值内存
ds = ds.map_batches(fn, batch_size=32)  # 而非默认 4096

# ✅ 合并小 Block（流式方式，不物化）
ds = ds.map_batches(lambda batch: batch, batch_size=target_size)

# ✅ 限制并发执行槽位
ds = ds.map_batches(fn, num_cpus=2)  # 更少的并发，更低内存

# ✅ 设置资源上限（多任务共享集群时）
ctx = ray.data.DataContext.get_current()
ctx.execution_options.resource_limits = ctx.execution_options.resource_limits.copy(
    cpu=10, gpu=5, object_store_memory=10e9,
)

# ✅ 检查 Block 大小（每个 Block 建议 > 1 MB，理想 > 100 MB）
print(ds.stats())
```

> Ray 默认预留 30% 内存给 Object Store。对于 Ray Data 工作负载，**建议设置为至少 50%**。

### 10.4 GPU 推理优化

```
GPU 推理性能优化检查清单:
┌────────────────────────────────────────────────────────────┐
│ ✅ batch_size 设置尽可能大（不超出 GPU 内存）                 │
│ ✅ concurrency 匹配可用 GPU 数量                            │
│ ✅ 使用类（Actor）而非函数加载模型                            │
│ ✅ CPU 预处理与 GPU 推理分离，利用流水线并行                   │
│ ✅ 如 GPU OOM，先减小 batch_size                            │
│ ✅ 如 batch_size=1 仍 OOM，考虑更小模型或更大显存 GPU         │
│ ✅ 大模型考虑多 GPU 分片（PlacementGroup）                   │
└────────────────────────────────────────────────────────────┘
```

**限制 CPU 内存占用示例**：

```python
# 模型消耗大量 CPU 内存时，限制每节点并发 Actor 数
predictions = ds.map_batches(
    HuggingFacePredictor,
    num_cpus=5,  # 16 核节点最多 3 个 Actor
    compute=ray.data.ActorPoolStrategy(size=12),  # 3 per node × 4 nodes
)
```

### 10.5 反模式与最佳实践

| 反模式 ❌ | 最佳实践 ✅ |
|-----------|------------|
| 用 `map()` 做向量化操作 | 用 `map_batches()` |
| 读全部列再用 `select_columns` 过滤 | 在 `read_parquet()` 中指定 `columns=` |
| 默认 `batch_size=4096` 处理大行 | 根据行大小设置合适的 `batch_size` |
| 用函数做 GPU 推理（每次重新加载模型） | 用类 + `ActorPoolStrategy` |
| 修改 `target_max_block_size` | 调整 `batch_size` 或 Block 数量 |
| 忽略 Block 大小分布 | 用 `ds.stats()` 监控并合并小 Block |
| `sort()` 处理超大数据集 | 注意：排序会物化数据，中断流式执行 |

### 10.6 执行配置

```python
ctx = ray.data.DataContext.get_current()

# 保持行顺序（可能降低性能）
ctx.execution_options.preserve_order = True

# 启用 Polars 加速排序
ctx.use_polars_sort = True

# Block 大小调控
ctx.target_min_block_size = 1 * 1024 * 1024    # 1 MiB
ctx.target_max_block_size = 128 * 1024 * 1024  # 128 MiB
ctx.read_op_min_num_blocks = 200               # 默认起始 Block 数
```

---

## 11. 参考资料

- [Ray Data 官方文档](https://docs.ray.io/en/latest/data/data.html)
- [Ray Data Key Concepts](https://docs.ray.io/en/latest/data/key-concepts.html)
- [Loading Data](https://docs.ray.io/en/latest/data/loading-data.html)
- [Transforming Data](https://docs.ray.io/en/latest/data/transforming-data.html)
- [Batch Inference](https://docs.ray.io/en/latest/data/batch_inference.html)
- [Performance Tips](https://docs.ray.io/en/latest/data/performance-tips.html)
- [Ray Data 流式执行原理 — 源码分析](https://developer.aliyun.com/article/1666904)

### 官方架构图

- [Dataset 与 Block 架构图](https://docs.ray.io/en/latest/_images/dataset-arch-with-blocks.svg)
- [执行计划转换流程图](https://docs.ray.io/en/latest/_images/get_execution_plan.svg)
- [流式拓扑结构图](https://docs.ray.io/en/latest/_images/streaming-topology.svg)
- [批量推理流式执行图](https://docs.ray.io/en/latest/_images/stream-example.png)
