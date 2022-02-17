import os

import dotenv
import pytest

from src.mongo_core import MongoEngine

dotenv.load_dotenv(verbose=True)


def setup_function():
    global M
    M = MongoEngine(
        host=os.getenv('MONGO_HOST'),
        port=int(os.getenv('MONGO_PORT')),
        username=os.getenv('MONGO_USERNAME'),
        password=os.getenv('MONGO_PASSWORD'),
        database=os.getenv('MONGO_DATABASE'),
        collection=os.getenv('MONGO_COLLECTION')
    )


def test_to_csv():
    result_ = M.to_csv(folder_path="_csv_single", filename="caonima.csv")
    print(result_)
    assert "successfully" in result_


def test_to_excel():
    result_ = M.to_excel(query={"区": "黄浦区"}, folder_path="_excel_single", filename="caonima.xlsx")
    print(result_)
    assert "successfully" in result_


def test_to_json():
    result_ = M.to_json(filename="caonima.json")
    print(result_)
    assert "successfully" in result_


def test_to_pickle():
    result_ = M.to_pickle(folder_path="./_pickle", filename="caonima.pkl")
    print(result_)
    assert "successfully" in result_


def test_to_feather():
    result_ = M.to_feather(query={"区": "黄浦区"}, folder_path="./_feather", filename="caonima.feather")
    print(result_)
    assert "successfully" in result_


def test_to_parquet():
    result_ = M.to_parquet(query={}, folder_path="./_parquet", filename="_.parquet")
    print(result_)
    assert "successfully" in result_


def teardown_function():
    ...


if __name__ == "__main__":
    pytest.main(["-s", "test_single.py"])
