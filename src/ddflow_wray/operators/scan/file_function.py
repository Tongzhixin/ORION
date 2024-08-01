import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
import ray
import asyncio
from collections import defaultdict, OrderedDict
from ray.util.state import get_actor, get_objects
from typing import List
import time
from ddflow.MemPool import LRUCache


@ray.remote
class FileSourceFunction:
    def __init__(
        self,
        bucket: str,
        file_path: str,
        mempool: LRUCache,
        columns: List[str],
        filters,
    ) -> None:
        self.bucket = bucket
        self.file_path = file_path
        self.mempool = mempool
        self.columns = columns
        self.filters = filters
        self.data_key = self.file_path + "columns" + ".".join(self.columns)

    def s3_read_parquet(self):
        full_path = f"{self.bucket}/{self.file_path}"
        s3 = s3fs.S3FileSystem(
            endpoint_url="http://10.2.64.6:9100",
            key="esQWHRxxpOL2oy48CW3K",
            secret="frKcsdRVGNhlDS3jR0JCADkLGj18ews7d3qdaZde",
            use_ssl=False,
            max_concurrency=50,
        )
        table = pq.read_table(
            full_path,
            columns=self.columns,
            filesystem=s3,
            filters=self.filters,
            pre_buffer=False,
        )
        # self.columns = table.column_names
        # self.data_key = self.file_path + "columns" + ".".join(self.columns)
        ref = self.mempool.put.remote(self.data_key, table)
        return table

    def exec(self):
        table = self.mempool.get.remote(self.data_key)
        if not table:
            table = self.s3_read_parquet()
        # 从共享内存中取出各列并拼成Arrow表格
        return table


@ray.remote
class LocalSourceFunction:
    def __init__(
        self,
        bucket: str,
        file_path: str,
        mempool: LRUCache,
        columns: List[str],
        filters,
    ) -> None:
        self.bucket = bucket
        self.file_path = file_path
        self.mempool = mempool
        self.columns = columns
        self.filters = filters
        self.data_key = self.file_path + "columns" + ".".join(self.columns)

    def local_read_parquet(self):
        full_path = f"{self.bucket}/{self.file_path}"

        table = pq.read_table(
            full_path,
            columns=self.columns,
            filters=self.filters,
            pre_buffer=False,
        )
        print(table.column_names)
        # self.columns = table.column_names
        # self.data_key = self.file_path + "columns" + ".".join(self.columns)
        ref = self.mempool.put.remote(self.data_key, table)
        return table

    def exec(self):
        table = self.mempool.get.remote(self.data_key)
        if not table:
            table = self.local_read_parquet()
        # 从共享内存中取出各列并拼成Arrow表格
        return table


@ray.remote(num_gpus=0.1)
def table_scan_local(bucket, file_path, mempool, columns, filters):
    time_start = time.time()
    data_key = file_path + "columns" + ".".join(columns)
    table = ray.get(mempool.get.remote(data_key))
    if not table:
        full_path = f"{bucket}/{file_path}"
        table = pq.read_table(
            full_path,
            columns=columns,
            filters=filters,
            pre_buffer=False,
        )
        print(table.column_names)
        ref = mempool.put.remote(data_key, table)
    print(f"table_scan_local {time.time() - time_start}")
    # 从共享内存中取出各列并拼成Arrow表格
    return table
