<h1 align="center">↻ Mongo2file</h1>
<p align="center">
  <img src="https://img.shields.io/badge/Python-3.7 | 3.8 | 3.9-blue" />
  <img src="https://img.shields.io/badge/license-Apache-green" />
  <img src="https://img.shields.io/badge/pypi-v1.0.1-red" />
</p>

> ↻ 一个 `Mongodb` 数据库转换为表格文件的库

## 安装

```shell
pip install mongo2file
```

## 基本用法

```python
import os
from mongo2file import MongoEngine

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

```

> 有关更多 [mongo2file](https://mongo2file.readthedocs.io/) 详细信息，请参阅文档。

