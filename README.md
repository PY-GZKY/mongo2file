## ↻ 一个 `Mongodb` 数据库转换为各类文件格式的库

## 安装

```shell
pip install mongov
```

## 基本用法

### `Mongodb` 表转表格文件

```python
import os
from mongov import MongoEngine

M = MongoEngine(
    host=os.getenv('MONGO_HOST', '127.0.0.1'),
    port=int(os.getenv('MONGO_PORT', 27017)),
    username=os.getenv('MONGO_USERNAME', None),
    password=os.getenv('MONGO_PASSWORD', None),
    database=os.getenv('MONGO_DATABASE', 'test_'),
    collection=os.getenv('MONGO_COLLECTION', 'test_')
)


def to_csv():
    result_ = M.to_csv()
    assert "successfully" in result_


def to_excel():
    result_ = M.to_excel()
    assert "successfully" in result_


def to_json():
    result_ = M.to_excel()
    assert "successfully" in result_


def to_pickle():
    result_ = M.to_pickle()
    assert "successfully" in result_


def to_feather():
    result_ = M.to_feather()
    assert "successfully" in result_


def to_parquet():
    result_ = M.to_parquet()
    assert "successfully" in result_


def to_hdf5():
    result_ = M.to_hdf5()
    assert "successfully" in result_
```

当 `MongoEngine` 控制类指定了 `mongodb` 表名称时、将对数据表 (`mongodb集合`) 进行导出操作。

其类方法参数包括:
- `query`: 指定对数据表的查询参数、只对指定表名时有效
- `folder_path`: 指定导出目录路径
- `filename`: 指定导出文件名、默认为 `表名称` + `当前时间`
- `_id`: 指定是否导出 `_id`、布尔型、默认为 `False`
- `limit`: 指定导出表的限制数据、`int`类型、默认为 `-1`、即不限制。

### `Mongodb` 库转表格文件

```python
import os
from mongov import MongoEngine

"""
作用于 MongoEngine 类未指定表名称时
"""

M = MongoEngine(
    host=os.getenv('MONGO_HOST', '127.0.0.1'),
    port=int(os.getenv('MONGO_PORT', 27017)),
    username=os.getenv('MONGO_USERNAME', None),
    password=os.getenv('MONGO_PASSWORD', None),
    database=os.getenv('MONGO_DATABASE', 'test_')
)

def to_csv():
    result_ = M.to_csv()
    assert "successfully" in result_


def to_excel():
    result_ = M.to_excel()
    assert "successfully" in result_


def to_json():
    result_ = M.to_json()
    assert "successfully" in result_
```
当 `MongoEngine` 控制类只指定了 `mongodb` 库名称时、将对数据库下所有集合进行导出操作。

其类方法参数包括:
- `folder_path`: 指定导出目录路径
- `filename`: 指定导出文件名、默认为 `表名称` + `当前时间`, 只对指定数据表时有效


`pickle`、`feather`、`parquet`、`hdf5` 是 `Python` 序列化数据的一种文件格式, 它把数据转成二进制进行存储。从而大大减少的读写时间。



