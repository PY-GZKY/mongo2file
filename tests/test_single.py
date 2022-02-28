import dotenv
import pytest

from mongo2file import MongoEngine

dotenv.load_dotenv(verbose=True)


def setup_function():
    global M
    M = MongoEngine(
        host='192.168.0.141',
        port=27017,
        username='admin',
        password='sanmaoyou_admin_',
        database='arrow测试库',
        # collection='arrow测试表_200000'
    )


def test_to_csv():
    result_ = M.to_csv(folder_path="_csv",is_block=True, block_size=50000)
    print(result_)
    assert "successfully" in result_


# def test_to_excel():
#     result_ = M.to_excel(folder_path="_excel", is_block=True, block_size=50000, mode='xlsx', ignore_error=True)
#     print(result_)
#     assert "successfully" in result_


# def test_to_json():
#     result_ = M.to_json(folder_path="./_json", is_block=False, block_size=50000)
#     print(result_)
#     assert "successfully" in result_

# def test_to_pickle():
#     result_ = M.to_pickle(folder_path="./_pickle")
#     print(result_)
#     assert "successfully" in result_


# def test_to_feather():
#     result_ = M.to_feather(folder_path="./_feather")
#     print(result_)
#     assert "successfully" in result_


# def test_to_parquet():
#     result_ = M.to_parquet(folder_path="./_parquet")
#     print(result_)
#     assert "successfully" in result_


def teardown_function():
    ...


if __name__ == "__main__":
    pytest.main(["-s", "test_single.py"])
