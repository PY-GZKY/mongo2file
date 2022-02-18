import os

import dotenv
import pytest

from mongov.mongo_core import MongoEngine

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


# def test_to_csv_s_():
#     result_ = M.to_csv(folder_path='./_csv')
#     print(result_)
#     assert "successfully" in result_

# def test_to_excel_s_():
#     result_ = M.to_excel(folder_path='./_excel')
#     print(result_)
#     assert "successfully" in result_

def test_to_json_s_():
    result_ = M.to_json(query={"区": "黄浦区"}, folder_path='./_json')
    print(result_)
    assert "successfully" in result_

def teardown_function():
    ...


if __name__ == "__main__":
    pytest.main(["-s", "test_many.py"])
