# -*- coding:utf-8 -*-
import datetime
import decimal
import getpass
import json
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, wait, as_completed, ALL_COMPLETED

from alive_progress import alive_bar
from bson import ObjectId
from colorama import Fore
from dateutil import tz

from constants import TIME_ZONE, THREAD_POOL_MAX_WORKERS


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


def concurrent_(func, db, collection_names, folder_path):
    title_ = f'{Fore.GREEN} {db} → {folder_path}'
    with alive_bar(len(collection_names), title=title_, bar="blocks") as bar:
        with ThreadPoolExecutor(max_workers=THREAD_POOL_MAX_WORKERS) as executor:
            for collection_name in collection_names:
                executor.submit(func, collection_name, folder_path).add_done_callback(lambda bar_: bar())
            executor.shutdown()


def json_concurrent_(func, collection_name, black_count_, block_size_, folder_path_):
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
