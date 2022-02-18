## ↻ 一个用于数据库、各类文件相互转换的库

## 安装

```shell
pip install mongov
```

## 基本用法

### Mongodb 表转表格文件

```python
import os
from mongov import MongoEngine

M = MongoEngine(
    host=os.getenv('MONGO_HOST'),
    port=int(os.getenv('MONGO_PORT')),
    username=os.getenv('MONGO_USERNAME'),
    password=os.getenv('MONGO_PASSWORD'),
    database=os.getenv('MONGO_DATABASE'),
    collection=os.getenv('MONGO_COLLECTION')
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

### Mongodb 库转表格文件

```python
import os
from mongov import MongoEngine

"""
作用于 MongoEngine 类未指定表名称时
"""

M = MongoEngine(
    host=os.getenv('MONGO_HOST'),
    port=int(os.getenv('MONGO_PORT')),
    username=os.getenv('MONGO_USERNAME'),
    password=os.getenv('MONGO_PASSWORD'),
    database=os.getenv('MONGO_DATABASE')
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

`pickle`、`feather`、`parquet`、`hdf5` 是 `Python` 序列化数据的一种文件格式, 
它把数据转成二进制进行存储。从而大大减少的读写时间。



