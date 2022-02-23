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

---

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

---

`pickle`、`feather`、`parquet`、`hdf5` 是 `Python` 序列化数据的一种文件格式, 它把数据转成二进制进行存储。

从而大大减少的读写时间。



```text
对于 mongodb 的全表查询、条件查询、聚合操作、以及索引操作(当数据达到一定量级时建议) 并不是直接影响
数据导出的最大因素，因为 mongodb 的查询一般而言都非常快速，主要的瓶颈在于读取 数据库 之后将数据转换为大列表存入 表格文件时所耗费的时间。

这是一件非常可怕的事情。

当没有多线程(当然这里的多线程并不是对同一文件进行并行操作，文件写入往往是线程不安全的)、
数据表查询语句无优化时，并且当数据达到一定量级时(比如 100w 行)，单表单线程表现出来的效果真是让人窒息。


```
