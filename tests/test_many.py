import os

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
        database='sm_admin_test',
        # collection='museum_scenic'
    )


def test_to_csv():
    result_ = M.to_csv(folder_path='./_csv')
    assert "successfully" in result_


# def test_to_excel():
#     result_ = M.to_excel(folder_path='./_excel')
#     assert "successfully" in result_
#
#
# def test_to_json():
#     result_ = M.to_json(folder_path='./_json')
#     assert "successfully" in result_


def teardown_function():
    ...


if __name__ == "__main__":
    pytest.main(["-s", "test_many.py"])
