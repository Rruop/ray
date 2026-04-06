# Parquet Checkpoint 去重工具

从 checkpoint 目录中读取所有 parquet 文件，提取 `blobstore_id` 列并去重，输出为 CSV 文件。

## 适用场景

- checkpoint 目录包含大量 parquet 文件（数百到数千个）
- 每个 parquet 文件仅含 `blobstore_id` 列（string 类型）
- 需要汇总去重后的完整 blobstore_id 列表

## 脚本

将以下内容保存为 `/tmp/dedup_blobstore.py`：

```python
import pyarrow.parquet as pq
import os, glob, time

# ===== 配置区域 =====
# 输入：parquet 文件所在目录
dir_path = "/log/output/shiyanpeng03/checkpoint/test_multi_vedio_checkpoint_12248"
# 输出：去重后的 CSV 文件路径
out_path = "/log/output/shiyanpeng03/checkpoint/test_multi_vedio_checkpoint_12248_blobstore_ids.csv"
# 日志文件
log_path = "/tmp/dedup_blobstore.log"
# 每批处理的文件数（用于打印进度）
batch_size = 100
# ===== 配置区域结束 =====

def log(msg):
    line = f"[{time.strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
    with open(log_path, "a") as f:
        f.write(line + "\n")

log("Starting dedup job...")
files = sorted(glob.glob(os.path.join(dir_path, "*.parquet")))
log(f"Found {len(files)} parquet files")

all_ids = set()
for i in range(0, len(files), batch_size):
    batch_files = files[i:i + batch_size]
    for fpath in batch_files:
        try:
            t = pq.read_table(fpath, columns=["blobstore_id"])
            for val in t.column("blobstore_id").to_pylist():
                all_ids.add(val)
        except Exception as e:
            log(f"Error reading {os.path.basename(fpath)}: {e}")
    log(f"Processed {min(i + batch_size, len(files))}/{len(files)} files, unique ids so far: {len(all_ids)}")

log(f"Total unique blobstore_id: {len(all_ids)}")

with open(out_path, "w") as f:
    f.write("blobstore_id\n")
    for bid in all_ids:
        f.write(f"{bid}\n")

fsize = os.path.getsize(out_path) / (1024 * 1024)
log(f"Saved to: {out_path} ({fsize:.2f} MB)")
log("Done!")
```

## 使用步骤

### 1. 登录容器

通过 KML Web Shell 进入目标容器：

```
https://kml.corp.kuaishou.com/v2/#/system/terminal?clusterName=<CLUSTER>&namespace=<NS>&pod=<POD>&mode=shell&auth=gaia
```

### 2. 确认目录和文件

```bash
# 查看 parquet 文件数量
ls /log/output/shiyanpeng03/checkpoint/test_multi_vedio_checkpoint_12248/*.parquet | wc -l

# 查看单个文件的 schema（可选）
python3 -c "
import pyarrow.parquet as pq
schema = pq.read_schema('/log/output/shiyanpeng03/checkpoint/test_multi_vedio_checkpoint_12248/$(ls /log/output/shiyanpeng03/checkpoint/test_multi_vedio_checkpoint_12248/ | head -1)')
for i, field in enumerate(schema):
    print(f'[{i}] {field.name}: {field.type}')
"
```

### 3. 创建并运行脚本

```bash
# 将脚本写入 /tmp（内容见上方"脚本"章节，按需修改 dir_path 和 out_path）
vi /tmp/dedup_blobstore.py

# 后台运行（推荐，文件多时耗时较长）
nohup python3 /tmp/dedup_blobstore.py > /tmp/dedup_blobstore_stdout.log 2>&1 &

# 查看进度
tail -f /tmp/dedup_blobstore.log
```

### 4. 验证结果

```bash
# 查看行数（含表头）
wc -l /log/output/shiyanpeng03/checkpoint/test_multi_vedio_checkpoint_12248_blobstore_ids.csv

# 查看前几行
head -5 /log/output/shiyanpeng03/checkpoint/test_multi_vedio_checkpoint_12248_blobstore_ids.csv

# 查看文件大小
ls -lh /log/output/shiyanpeng03/checkpoint/test_multi_vedio_checkpoint_12248_blobstore_ids.csv
```

## 自动化执行（通过 kml_ws_exec.py）

如果需要从本地远程执行，可以使用 `kml_ws_exec.py` 脚本：

```bash
# 1. 写入脚本到容器
python3 scripts/kml_ws_exec.py \
  --url "<KML_URL>" \
  --cmd "cat > /tmp/dedup_blobstore.py << 'PYEOF'
<脚本内容>
PYEOF"

# 2. 后台运行
python3 scripts/kml_ws_exec.py \
  --url "<KML_URL>" \
  --cmd "nohup python3 /tmp/dedup_blobstore.py > /tmp/dedup_blobstore_stdout.log 2>&1 &"

# 3. 查看进度
python3 scripts/kml_ws_exec.py \
  --url "<KML_URL>" \
  --cmd "cat /tmp/dedup_blobstore.log"
```

## 实际执行记录

| 项目 | 值 |
|------|-----|
| checkpoint 目录 | `test_multi_vedio_checkpoint_12248` |
| 源文件数 | 1,791 个 parquet |
| 每文件行数 | ~10,000 |
| 去重前总行数 | ~17,910,000 |
| 去重后唯一 ID | 152,913 |
| 输出文件大小 | 5.84 MB |
| 总耗时 | ~71 秒 |
| blobstore_id 格式 | `upload:dujuanVideo:<数字或哈希>` |
