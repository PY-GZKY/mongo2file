import os

import dotenv
import pytest

from mongov import MongoEngine

dotenv.load_dotenv(verbose=True)


def setup_function():
    global M
    M = MongoEngine(
        host=os.getenv('MONGO_HOST'),
        port=int(os.getenv('MONGO_PORT')),
        username=os.getenv('MONGO_USERNAME'),
        password=os.getenv('MONGO_PASSWORD'),
        database=os.getenv('MONGO_DATABASE')
    )


def test_to_csv_s_():
    result_ = M.to_csv(folder_path='./_csv')
    assert "successfully" in result_


def test_to_excel_s_():
    result_ = M.to_excel(folder_path='./_excel')
    assert "successfully" in result_


def test_to_json_s_():
    result_ = M.to_json(folder_path='./_json')
    assert "successfully" in result_


def teardown_function():
    ...


if __name__ == "__main__":
    pytest.main(["-s", "test_many.py"])
