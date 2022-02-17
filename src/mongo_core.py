# -*- coding:utf-8 -*-
import os
import warnings
from concurrent.futures import ThreadPoolExecutor, wait, as_completed, ALL_COMPLETED
from typing import Optional
import pandas as pd
from colorama import init as colorama_init_, Fore
from dotenv import load_dotenv
from pandas import DataFrame
from pymongo import MongoClient

from .constants import *
from .utils import to_str_datetime, serialize_obj

load_dotenv(verbose=True)
colorama_init_(autoreset=True)


class MongoEngine:

    def __init__(
            self,
            host: Optional[str] = None,
            port: Optional[int] = None,
            username: Optional[str] = None,
            password: Optional[str] = None,
            database: Optional[str] = None,
            collection: Optional[str] = None,
            conn_timeout: Optional[int] = 30,
            conn_retries: Optional[int] = 5
    ):
        self.host = MONGO_HOST if host is None else host
        self.port = MONGO_PORT if port is None else port
        self.username = username
        self.password = password
        self.database = MONGO_DATABASE if database is None else database
        self.collection = MONGO_COLLECTION if collection is None else collection
        self.conn_timeout = MONGO_CONN_TIMEOUT if conn_timeout is None else conn_timeout
        self.conn_retries = MONGO_CONN_RETRIES if conn_retries is None else conn_retries
        self.mongo_core_ = MongoClient(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            maxPoolSize=None
        )
        self.db_ = self.mongo_core_[self.database]
        self.collection_names = self.db_.list_collection_names()
        if self.collection:
            self.collection_ = self.db_[self.collection]
        else:
            self.collection_ = None

    def to_csv(self, query=None, folder_path: str = None, filename: str = None, _id: bool = False, limit: int = -1):
        """
        :param query: dict type → Invalid when exporting multiple tables
        :param folder_path:
        :param filename:
        :param _id:
        :param limit:
        :return:
        """
        if query is None:
            query = {}
        if not isinstance(query, dict):
            raise TypeError('query must be of dict type')
        if not isinstance(limit, int):
            raise TypeError("limit must be an integer type")
        if not isinstance(_id, bool):
            raise TypeError("_id must be an boolean type")
        folder_path_ = self.check_folder_path(folder_path)

        if self.collection_:
            if filename is None:
                filename = f'{self.collection}_{to_str_datetime()}.csv'
            doc_objs_ = self.collection_.find(query, {"_id": 0}).limit(
                limit) if limit != -1 else self.collection_.find(query, {"_id": 0})
            doc_list_ = list(doc_objs_)
            data = DataFrame(doc_list_)
            data.to_csv(path_or_buf=f'{folder_path_}/{filename}', index=False, encoding=PANDAS_ENCODING)
            result_ = ECHO_INFO.format(Fore.GREEN, self.collection, f'{folder_path_}/{filename}')
            return result_
        else:
            warnings.warn('No collection specified, All collections will be exported.', DeprecationWarning)
            self.to_csv_s_(folder_path_)
            result_ = ECHO_INFO.format(Fore.GREEN, self.database, folder_path_)
            return result_

    def to_excel(self, query=None, folder_path: str = None, filename: str = None, _id: bool = False, limit: int = -1):
        if query is None:
            query = {}
        if not isinstance(query, dict):
            raise TypeError('query must be of dict type')
        if not isinstance(limit, int):
            raise TypeError("limit must be an integer type")
        if not isinstance(_id, bool):
            raise TypeError("_id must be an boolean type")
        folder_path_ = self.check_folder_path(folder_path)

        if self.collection_:
            if filename is None:
                filename = f'{self.collection}_{to_str_datetime()}.xlsx'
            doc_objs_ = self.collection_.find(query, {"_id": 0}).limit(
                limit) if limit != -1 else self.collection_.find(query, {"_id": 0})
            doc_list_ = list(doc_objs_)
            data = DataFrame(doc_list_)
            data.to_excel(excel_writer=f'{folder_path_}/{filename}', sheet_name=filename, index=False,
                          encoding=PANDAS_ENCODING)
            result_ = ECHO_INFO.format(Fore.GREEN, self.collection, f'{folder_path_}/{filename}')
            return result_
        else:
            warnings.warn('No collection specified, All collections will be exported.', DeprecationWarning)
            self.to_excel_s_(folder_path_)
            result_ = ECHO_INFO.format(Fore.GREEN, self.database, folder_path_)
            return result_

    def to_json(self, query=None, folder_path: str = None, filename: str = None, _id: bool = False, limit: int = -1):
        if query is None:
            query = {}
        if not isinstance(query, dict):
            raise TypeError('query must be of dict type')
        if not isinstance(limit, int):
            raise TypeError("limit must be an integer type")
        if not isinstance(_id, bool):
            raise TypeError("_id must be an boolean type")
        folder_path_ = self.check_folder_path(folder_path)

        if self.collection_:
            if filename is None:
                filename = f'{self.collection}_{to_str_datetime()}.json'
            doc_objs_ = self.collection_.find(query, {"_id": 0}).limit(
                limit) if limit != -1 else self.collection_.find(query, {"_id": 0})
            doc_list_ = list(doc_objs_)
            data = {'RECORDS': doc_list_}
            with open(f'{folder_path_}/{filename}', 'w', encoding="utf-8") as f:
                f.write(serialize_obj(data))
            result_ = ECHO_INFO.format(Fore.GREEN, self.collection, f'{folder_path_}/{filename}')
            return result_
        else:
            warnings.warn('No collection specified, All collections will be exported.', DeprecationWarning)
            self.to_json_s_(folder_path_)
            result_ = ECHO_INFO.format(Fore.GREEN, self.database, folder_path_)
            return result_

    def to_pickle(self, query=None, folder_path: str = None, filename: str = None, _id: bool = False, limit: int = -1):
        if query is None:
            query = {}
        if not isinstance(query, dict):
            raise TypeError('query must be of dict type')
        if not isinstance(limit, int):
            raise TypeError("limit must be an integer type")
        if not isinstance(_id, int):
            raise TypeError("_id must be an boolean type")
        folder_path_ = self.check_folder_path(folder_path)

        if self.collection_:
            if filename is None:
                filename = f'{self.collection}_{to_str_datetime()}.pkl'
            doc_objs_ = self.collection_.find(query, {"_id": 0}).limit(
                limit) if limit != -1 else self.collection_.find(query, {"_id": 0})
            doc_list_ = list(doc_objs_)
            data = DataFrame(doc_list_)
            data.to_pickle(path=f'{folder_path_}/{filename}')
            result_ = ECHO_INFO.format(Fore.GREEN, self.collection, f'{folder_path_}/{filename}')
            return result_

    def to_feather(self, query=None, folder_path: str = None, filename: str = None, _id: bool = False, limit: int = -1):
        """
        pip[conda] install pyarrow
        """
        if query is None:
            query = {}
        if not isinstance(query, dict):
            raise TypeError('query must be of dict type')
        if not isinstance(limit, int):
            raise TypeError("limit must be an integer type")
        if not isinstance(_id, bool):
            raise TypeError("_id must be an boolean type")
        folder_path_ = self.check_folder_path(folder_path)
        if self.collection_:
            if filename is None:
                filename = f'{self.collection}_{to_str_datetime()}.feather'
            doc_objs_ = self.collection_.find(query, {"_id": 0}).limit(
                limit) if limit != -1 else self.collection_.find(query, {"_id": 0})
            doc_list_ = list(doc_objs_)
            data = DataFrame(doc_list_)
            data.to_feather(path=f'{folder_path_}/{filename}')
            result_ = ECHO_INFO.format(Fore.GREEN, self.collection, f'{folder_path_}/{filename}')
            return result_

    def to_parquet(self, query=None, folder_path: str = None, filename: str = None, _id: bool = False, limit: int = -1):
        if query is None:
            query = {}
        if not isinstance(query, dict):
            raise TypeError('query must be of dict type')
        if not isinstance(limit, int):
            raise TypeError("limit must be an integer type")
        if not isinstance(_id, bool):
            raise TypeError("_id must be an boolean type")
        folder_path_ = self.check_folder_path(folder_path)
        if self.collection_:
            if filename is None:
                filename = f'{self.collection}_{to_str_datetime()}.parquet'
            doc_objs_ = self.collection_.find(query, {"_id": 0}).limit(
                limit) if limit != -1 else self.collection_.find(query, {"_id": 0})
            doc_list_ = list(doc_objs_)
            data = DataFrame(doc_list_)
            data.to_parquet(path=f'{folder_path_}/{filename}')
            result_ = ECHO_INFO.format(Fore.GREEN, self.collection, f'{folder_path_}/{filename}')
            return result_

    def to_hdf5(self, query=None, folder_path: str = None, filename: str = None, _id: bool = False, limit: int = -1):
        """
        pip[conda] install pytables
        """
        if query is None:
            query = {}
        if not isinstance(query, dict):
            raise TypeError('query must be of dict type')
        if not isinstance(limit, int):
            raise TypeError("limit must be an integer type")
        if not isinstance(_id, bool):
            raise TypeError("_id must be an boolean type")
        folder_path_ = self.check_folder_path(folder_path)
        if self.collection_:
            if filename is None:
                filename = f'{self.collection}_{to_str_datetime()}.h5'
            doc_objs_ = self.collection_.find(query, {"_id": 0}).limit(
                limit) if limit != -1 else self.collection_.find(query, {"_id": 0})
            doc_list_ = list(doc_objs_)
            data = DataFrame(doc_list_)

            data.to_hdf('temp.h5', key='df', mode='w')
            # h5 = pd.HDFStore(f'{folder_path_}/{filename}', 'w', complevel=4, complib='blosc')
            # h5['df_'] = data
            # h5.close()
            result_ = ECHO_INFO.format(Fore.GREEN, self.collection, f'{folder_path_}/{filename}')
            return result_


    def no_collection_to_csv_(self, collection_: str, folder_path: str, _id: bool = False):
        if collection_:
            filename = f'{collection_}_{to_str_datetime()}.csv'
            doc_objs_ = self.db_[collection_].find({}, {"_id": 0})
            data = DataFrame(list(doc_objs_))
            data.to_csv(path_or_buf=f'{folder_path}/{filename}', index=False, encoding=PANDAS_ENCODING)

    def no_collection_to_excel_(self, collection_: str, folder_path: str, _id: bool = False):
        if collection_:
            filename = f'{collection_}_{to_str_datetime()}.xlsx'
            doc_objs_ = self.db_[collection_].find({}, {"_id": 0})
            data = DataFrame(list(doc_objs_))
            data.to_excel(excel_writer=f'{folder_path}/{filename}.xlsx', index=False, encoding=PANDAS_ENCODING)

    def no_collection_to_json_(self, collection_: str, folder_path: str, _id: bool = False):
        if collection_:
            filename = f'{collection_}_{to_str_datetime()}.json'
            doc_objs_ = self.db_[collection_].find({}, {"_id": 0})
            data = {'RECORDS': list(doc_objs_)}
            with open(f'{folder_path}/{filename}', 'w', encoding="utf-8") as f:
                f.write(serialize_obj(data))

    def to_csv_s_(self, folder_path: str):
        self.concurrent_(self.no_collection_to_csv_, self.collection_names, folder_path)

    def to_excel_s_(self, folder_path: str):
        self.concurrent_(self.no_collection_to_excel_, self.collection_names, folder_path)

    def to_json_s_(self, folder_path: str):
        self.concurrent_(self.no_collection_to_json_, self.collection_names, folder_path)

    def concurrent_(self, func, collection_names, folder_path):
        with ThreadPoolExecutor(max_workers=THREAD_POOL_MAX_WORKERS) as executor:
            futures_ = [executor.submit(func, collection_name, folder_path) for collection_name in collection_names]
            wait(futures_, return_when=ALL_COMPLETED)
            for future_ in as_completed(futures_):
                if future_.done():
                    # print(future_.result())
                    ...

    def check_folder_path(self, folder_path):
        if folder_path is None:
            _ = '.'
        elif not os.path.exists(folder_path):
            os.makedirs(folder_path)
            _ = folder_path
        else:
            _ = folder_path
        return _


if __name__ == '__main__':
    M = MongoEngine(
        host=os.getenv('MONGO_HOST'),
        port=int(os.getenv('MONGO_PORT')),
        username=os.getenv('MONGO_USERNAME'),
        password=os.getenv('MONGO_PASSWORD'),
        database=os.getenv('MONGO_DATABASE'),
        collection=os.getenv('MONGO_COLLECTION')
    )
    # M.to_csv(query={}, _id=False, filename="_")
    # M.to_excel(query={})
    # M.to_json(query={})
    # M.to_pickle(query={})
    # M.to_hdf5(query={})
    print(M.to_hdf5.__name__)
