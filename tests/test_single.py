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


# def test_to_csv():
#     result_ = M.to_csv(query={}, _id=False)
#     assert "successfully" in result_


# def test_to_excel():
#     result_ = M.to_excel(query={}, _id=False)
#     assert "successfully" in result_


# def test_to_json():
#     result_ = M.to_excel(query={}, _id=False, filename="_")
#     assert "successfully" in result_
#
#
def test_to_pickle():
    result_ = M.to_pickle(query={}, _id=False, filename="窝草",folder_path="./_pickle")
    assert "successfully" in result_
#
#
# def test_to_feather():
#     result_ = M.to_feather(query={}, _id=False, filename="_")
#     assert "successfully" in result_
#
#
# def test_to_parquet():
#     result_ = M.to_parquet(query={}, _id=False, filename="_")
#     assert "successfully" in result_
#
#
# def test_to_hdf5():
#     result_ = M.to_hdf5(query={}, _id=False, filename="_")
#     assert "successfully" in result_


def teardown_function():
    ...


if __name__ == "__main__":
    pytest.main(["-s", "test_single.py"])
