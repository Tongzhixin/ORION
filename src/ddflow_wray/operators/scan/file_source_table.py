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
# 初始化Ray
ray.init(
    ignore_reinit_error=True,
    object_store_memory=32474836480,
    _plasma_directory="/dev/shm",
    _system_config={"automatic_object_spilling_enabled": False},
)

@ray.remote
class S3FileReader:
    def __init__(self, bucket, key, lru_cache, select_columns=None, filters=None):
        self.s3 = s3fs.S3FileSystem(
            endpoint_url="http://10.2.64.6:9100",
            key="esQWHRxxpOL2oy48CW3K",
            secret="frKcsdRVGNhlDS3jR0JCADkLGj18ews7d3qdaZde",
            use_ssl=False,
            max_concurrency=50,
        )
        self.bucket = bucket
        self.key = key
        self.cache = lru_cache
        self.select_columns = select_columns
        self.filters = filters
        if self.select_columns:
            self.cache_key = self.key + "columns" + ".".join(self.select_columns)
        else:
            self.cache_key = self.key
        # print(f"s3://{bucket}/{key}")
        # 获取列名
        self.schema = pq.read_schema(f"s3://{bucket}/{key}", filesystem=self.s3)

    def process(self):
        """
        处理指定的列，将它们放入共享内存中
        """

        file_path = f"{self.bucket}/{self.key}"
        # table = None
        # 读取Parquet文件中的指定列

        table = pq.read_table(
            file_path,
            columns=self.select_columns,
            filesystem=self.s3,
            filters=self.filters,
            pre_buffer=False,
        )

        ray.get(self.cache.put.remote(self.cache_key, table))
        return table

    def table_scan(self):

        table = ray.get(self.cache.get.remote(self.cache_key))
        if not table:
            table = self.process()
        # 从共享内存中取出各列并拼成Arrow表格
        return table


@ray.remote
def get_cached_files(lru_cached):
    cached_keys: list = ray.get(lru_cached.get_cached_key.remote())
    files_list = cached_keys

    files_list = list(set(files_list))
    return files_list


def list_files(bucket, prefix):
    """
    列出S3路径下的所有文件
    :param bucket: S3桶的名称
    :param prefix: S3路径前缀
    :return: 文件列表
    """
    # 创建S3FileSystem实例
    s3 = s3fs.S3FileSystem(
        endpoint_url="http://10.2.64.6:9100",
        key="esQWHRxxpOL2oy48CW3K",
        secret="frKcsdRVGNhlDS3jR0JCADkLGj18ews7d3qdaZde",
        use_ssl=False,
    )

    # 列出指定路径下的文件
    file_list = s3.ls(f"s3://{bucket}/{prefix}")

    # 提取文件名
    file_names = [file.split("/")[-1] for file in file_list]

    return file_names


def scan_file(files_list, lru_cache, select_columns, filters, max_cpu=32):

    ref_list = []
    file_size_total = 0
    for file_name in files_list:
        if len(ref_list) > max_cpu:
            # update result_refs to only
            # track the remaining tasks.
            ready_refs, ref_list = ray.wait(ref_list, num_returns=1)
            # print(len(ready_refs), len(ref_list))
            tables = ray.get(ready_refs)
            for t in tables:
                # print(f"file size:{t.nbytes}")
                file_size_total += t.nbytes
                del t
        if not file_name:
            continue
        reader = S3FileReader.remote(
            bucket="tpch-all",
            key=file_name,
            lru_cache=lru_cache,
            select_columns=select_columns,
            filters=filters,
        )
        table_ref = reader.table_scan.remote()
        ref_list.append(table_ref)
    return file_size_total


def remove_columns_field(items):
    processed_items = []
    for item in items:
        if "columns" in item:
            # 找到'columns'字段的索引位置
            columns_index = item.index("columns")
            # 去除'columns'字段及其之后的内容
            processed_item = item[:columns_index]
        else:
            # 如果不包含'columns'字段，不做处理
            processed_item = item
        processed_items.append(processed_item)
    return processed_items


def start_flow(lru_cache, files_list, next_flow=None):
    exist_files = ray.get(get_cached_files.remote(lru_cache))
    exist_files = remove_columns_field(exist_files)
    set_exist = set(exist_files)
    set_all = set(files_list)
    second_split_file = list(set_all - set_exist)
    select_columns = (
        [
            "l_orderkey",
            "l_quantity",
            "l_extendedprice",
            "l_discount",
            "l_tax",
            "l_returnflag",
            "l_linestatus",
            "l_shipdate",
        ],
    )
    from datetime import datetime, timedelta

    date_reference = datetime.strptime("1998-12-01", "%Y-%m-%d")
    date_threshold = date_reference - timedelta(days=68)
    date_threshold_date = date_threshold.date()

    print(date_threshold_date)  # Output: 1998-09-24

    # Step 2: Convert to pyarrow DNF format
    filter_expression = [[("l_shipdate", "<=", date_threshold_date)]]
    print(f"exist file: {exist_files}, second_split files:{second_split_file}")
    # stage 1
    time_start = time.time()
    stage1_size = scan_file(
        exist_files,
        lru_cache,
        select_columns=select_columns,
        filters=filter_expression,
        max_cpu=128,
    )

    time_stage_1 = time.time()
    ray.get(lru_cache.free_fixed_size.remote())
    # time.sleep(5)
    # stage 2
    stage2_size = scan_file(
        second_split_file,
        lru_cache,
        select_columns=select_columns,
        filters=filter_expression,
    )
    # table_contents = [ray.get(ref) for ref in ref_list]
    time_stage_2 = time.time()
    print(
        f"stage 1 time: {time_stage_1-time_start},stage 2 time:{time_stage_2-time_stage_1}"
    )
    print(f"stage 1 size: {stage1_size},stage 2 size:{stage2_size}")


# 示例使用方法
if __name__ == "__main__":
    # 创建S3FileReader实例
    file_names = list_files("tpch-all", "tpch_100g_small/lineitem")
    file_names = [
        f"tpch_100g_small/lineitem/{file_name}" for file_name in file_names if file_name
    ]
    lru_cache = LRUCache.remote()
    time_start = time.time()

    start_flow(lru_cache=lru_cache, files_list=file_names)

    print(f"初始化完成\n")
    time_1 = time.time()
    time.sleep(5)
    time_2 = time.time()
    start_flow(lru_cache=lru_cache, files_list=file_names)
    time_3 = time.time()
    print(f"time stage1:{time_1-time_start},stage2:{time_3-time_2}")
    time.sleep(20)

    # for table in table_contents:
    #     print(table)
