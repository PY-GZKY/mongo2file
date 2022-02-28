# -*- coding:utf-8 -*-
import datetime
import decimal
import getpass
import json
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import xlsxwriter
from alive_progress import alive_bar
from bson import ObjectId
from colorama import Fore
from dateutil import tz
import pyarrow as pa

from mongo2file.constants import TIME_ZONE, THREAD_POOL_MAX_WORKERS, IGNORE_TYPE


def get_user_name():
    return getpass.getuser()


def gen_uuid():
    return str(uuid.uuid4())


def as_int(f: float) -> int:
    return int(round(f))


def timestamp_ms() -> int:
    return as_int(time.time() * 1000)


def ms_to_datetime(unix_ms: int) -> datetime:
    tz_ = tz.gettz(TIME_ZONE)
    return datetime.datetime.fromtimestamp(unix_ms / 1000, tz=tz_)


def to_str_datetime():
    return datetime.datetime.now().strftime('%Y-%m-%d_%H%M%S%f')


def _alchemy_encoder(obj):
    if isinstance(obj, datetime.date):
        return obj.strftime("%Y-%m-%d %H:%M:%S")
    elif isinstance(obj, decimal.Decimal):
        return float(obj)
    elif isinstance(obj, ObjectId):
        return str(obj)


def serialize_obj(obj):
    if isinstance(obj, list):
        return json.dumps([dict(r) for r in obj], ensure_ascii=False, default=_alchemy_encoder)
    else:
        return json.dumps(dict(obj), ensure_ascii=False, default=_alchemy_encoder)


def schema_(obj: dict):
    return {k: v for k, v in obj.items() if isinstance(v, str)}


def no_collection_to_csv_(collection_obj_: dict, folder_path: str, _id: bool = False,
                          ignore_error: bool = False):  # noqa F401
    if collection_obj_:
        filename = f'{collection_obj_.get("collection_name")}_{to_str_datetime()}.csv'
        doc_objs_ = collection_obj_.get("collection_").find({}, {"_id": 0})
        doc_list_ = [schema_(doc_) for doc_ in doc_objs_]
        df_ = pa.Table.from_pylist(mapping=doc_list_, schema=None)
        with pa.csv.CSVWriter(f'{folder_path}/{filename}', df_.schema) as writer:
            writer.write_table(df_)


def no_collection_to_excel_(collection_obj_: dict, folder_path: str, _id: bool = False, ignore_error: bool = False):
    if collection_obj_:
        filename = f'{collection_obj_.get("collection_name")}_{to_str_datetime()}.xlsx'
        doc_objs_ = collection_obj_.get("collection_").find({}, {"_id": 0})
        with xlsxwriter.Workbook(f'{folder_path}/{filename}') as work_book_:
            work_sheet_ = work_book_.add_worksheet('Sheet1')
            header_ = list(dict(doc_objs_[0]).keys())
            work_sheet_.write_row(f"A1", header_)
            if ignore_error:
                for index_, doc_ in enumerate(doc_objs_):
                    write_list_ = [
                        doc_.get(x_) if doc_.get(x_) and isinstance(doc_.get(x_), IGNORE_TYPE) else None
                        for x_ in header_]
                    work_sheet_.write_row(f"A{index_ + 2}", write_list_)
            else:
                for index_, doc in enumerate(doc_objs_):
                    write_list_ = [doc_ if isinstance(doc_, (int, float)) or doc_ is None else str(doc_) for
                                   doc_ in
                                   dict(doc).values()]
                    work_sheet_.write_row(f"A{index_ + 2}", write_list_)


def no_collection_to_json_(collection_obj_: dict, folder_path: str, _id: bool = False,
                           ignore_error: bool = False):  # noqa F401
    if collection_obj_:
        filename = f'{collection_obj_.get("collection_name")}_{to_str_datetime()}.json'
        doc_objs_ = collection_obj_.get("collection_").find({}, {"_id": 0})
        data = {'RECORDS': list(doc_objs_)}
        with open(f'{folder_path}/{filename}', 'w', encoding="utf-8") as f:
            f.write(serialize_obj(data))


def concurrent_(func, db, collection_objs_, folder_path, ignore_error):
    title_ = f'{Fore.GREEN} {db} → {folder_path}'
    with alive_bar(len(collection_objs_), title=title_, bar="blocks") as bar:
        with ThreadPoolExecutor(max_workers=THREAD_POOL_MAX_WORKERS) as executor:
            for collection_obj_ in collection_objs_:
                executor.submit(func, collection_obj_, folder_path, ignore_error).add_done_callback(lambda bar_: bar())
            executor.shutdown()


def json_concurrent_(func, collection_name, black_count_, block_size_, folder_path_, ignore_error): # noqa F401
    title_ = f'{Fore.GREEN} {collection_name} → {folder_path_}'
    with alive_bar(black_count_, title=title_, bar="blocks") as bar:
        with ThreadPoolExecutor(max_workers=THREAD_POOL_MAX_WORKERS) as executor:
            for pg in range(black_count_):
                executor.submit(func, pg, block_size_, collection_name, folder_path_).add_done_callback(
                    lambda func: bar())
            executor.shutdown()


def excel_concurrent_(func, f_, collection_name, black_count_, block_size_, folder_path_, ignore_error):
    title_ = f'{Fore.GREEN} {collection_name} → {folder_path_}'
    with alive_bar(black_count_, title=title_, bar="blocks") as bar:
        with ThreadPoolExecutor(max_workers=THREAD_POOL_MAX_WORKERS) as executor:
            for pg in range(black_count_):
                executor.submit(func, f_, pg, block_size_, collection_name, folder_path_,
                                ignore_error).add_done_callback(lambda bar_: bar())
            executor.shutdown()


def csv_concurrent_(func, collection_name, black_count_, block_size_, folder_path_, ignore_error):
    title_ = f'{Fore.GREEN} {collection_name} → {folder_path_}'
    with alive_bar(black_count_, title=title_, bar="blocks") as bar:
        with ThreadPoolExecutor(max_workers=THREAD_POOL_MAX_WORKERS) as executor:
            for pg in range(black_count_):
                executor.submit(func, pg, block_size_, collection_name, folder_path_, ignore_error).add_done_callback(
                    lambda func: bar())
            executor.shutdown()
            # wait(futures_, return_when=ALL_COMPLETED)
            # for future_ in as_completed(futures_):
            #     if future_.done():
            #         print(future_.result())
