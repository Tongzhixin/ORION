import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
import ray
import asyncio
from collections import defaultdict, OrderedDict
from ray.util.state import get_actor,get_objects
from typing import List
import time
from pyarrow import ChunkedArray

# 初始化Ray
ray.init(ignore_reinit_error=True,object_store_memory=21474836480,_plasma_directory="/dev/shm",_system_config={ 'automatic_object_spilling_enabled':False})
#  



# @ray.remote
class LRUCache:
    def __init__(self, max_size=22*1024):
        self.max_size = max_size
        self.current_size = 0
        self.cache = defaultdict(ChunkedArray)  # stores the key and col data
        self.key_size = {}  # stores the size of each value
        self.usage_count = defaultdict(int)  # stores usage count of each key

    def _evict(self):
        # Find the least frequently used key
        sorted_keys = sorted(self.usage_count, key=self.usage_count.get)
        for evict_key in sorted_keys:
            if self.current_size < self.max_size:
                break
            del self.cache[evict_key]
            size = self.key_size[evict_key]
            # ray.internal.free(ref)
            self.current_size -= size
            del self.key_size[evict_key]
            del self.usage_count[evict_key]
            print(f"Evicted key: {evict_key}")

    def put(self, key, value):
        self.cache[key] = value
        value_size = value.nbytes / 1024 / 1024
        print(f"now size {self.current_size}mb")
        # for object_state in object_states:
        #     print(object_state)
        #     value_size = object_state.object_size # Assuming size is the length of the value
        #     ip = object_state.ip
        #     print(value_size,ip)
        if value_size > self.max_size:
            raise ValueError("Value size exceeds the maximum cache size")

        if self.current_size + value_size > self.max_size:
            self._evict()
        self.key_size[key] = value_size
        self.current_size += value_size
        self.usage_count[key] += 1  # Initialize usage count for new key


    def get(self, key):
        if key in self.cache.keys():
            self.usage_count[key] += 1
            return self.cache[key]
        else:
            # Read from file (simulate with a dummy function)
            print(f"{key} get error")
            return None
    def get_cached_key(self):
        return list(self.cache.keys())
    def get_cached_value(self,key_list):
        # print(f"cached list: ", key_list)
        col_datas = []
        missed_columns = []
        for k in key_list:
            col_data = self.get(k)
            if col_data:
                col_datas.append(col_data)
            else:
                print(f"slack col{k.split('.')[-1]}")
                missed_columns.append(k.split(".")[-1])
        return col_datas,missed_columns
    def read_from_file(self, key):
        # Dummy implementation for reading from file
        # Replace this with actual file reading logic
        return f"Data for {key}"

@ray.remote
class S3FileReader:
    def __init__(self, bucket, key, lru_cache):
        self.s3 = s3fs.S3FileSystem(
            endpoint_url="http://10.2.64.6:9100",
            key="esQWHRxxpOL2oy48CW3K",
            secret="frKcsdRVGNhlDS3jR0JCADkLGj18ews7d3qdaZde",
            use_ssl=False,
            max_concurrency=10,
        )
        self.bucket = bucket
        self.key = key
        self.cache = lru_cache
        print(f"s3://{bucket}/{key}")
        schema = pq.read_schema(f"s3://{bucket}/{key}", filesystem=self.s3)

        # 获取列名
        self.headcolumns = schema.names

    def process(self, columns):
        """
        处理指定的列，将它们放入共享内存中
        """

        file_path = f"{self.bucket}/{self.key}"
        table = None
        # 读取Parquet文件中的指定列
        if not columns:
            table = pq.read_table(file_path, filesystem=self.s3)
            columns = table.column_names
        else:
            table = pq.read_table(file_path, columns=columns, filesystem=self.s3)

        # 将每一列放入共享内存中，并存储引用
        for column in columns:
            col_data = table.column(column)
            self.cache.put(f"{self.key}.{column}", col_data)
        return table

    def table_scan(self, columns=None, filters=None):
        """
        如果指定文件和列已经在共享内存中，则取出各列并拼成Arrow表格
        如果不存在，调用process函数，然后从共享内存中取出各列
        :param columns: 列名列表
        :return: Arrow表格
        """
        table = None
        if columns is None:
            columns = self.headcolumns
        cached_keys: list = self.cache.get_cached_key()
        # print(cached_keys)
        exist_columns = [col for col in columns if f"{self.key}.{col}" in cached_keys]
        missing_columns = [
            col for col in columns if f"{self.key}.{col}" not in cached_keys
        ]
        exist_column_data, miss_columns =  self.cache.get_cached_value([f"{self.key}.{col}" for col in exist_columns])
        
        missing_columns.extend(miss_columns)
        missing_columns = list(set(missing_columns))
        if missing_columns:
            table = self.process(missing_columns)
        exist_columns = [v for v in exist_columns if v not in missing_columns]
        missing_dict = {}
        existing_dict = {}
        column_data_arrays = []
        if exist_column_data:
            for i, c in enumerate(exist_column_data):
                existing_dict[exist_columns[i]] = c
        if missing_columns:
            for i, c in enumerate(missing_columns):
                missing_dict[c] = table.columns[i]
        for n in columns:
            if exist_columns and n in exist_columns:
                column_data_arrays.append(existing_dict[n])
            elif missing_columns and n in missing_columns:
                column_data_arrays.append(missing_dict[n])
            else:
                print("error\n")
        print(f"have not columns: {missing_columns}, exist columns:{exist_columns}")
        # 从共享内存中取出各列并拼成Arrow表格
        return pa.Table.from_arrays(column_data_arrays, names=columns)


@ray.remote
def get_cached_files(lru_cached):
    cached_keys: list = lru_cached.get_cached_key()
    files_list = []
    for k in cached_keys:
        files_list.append(".".join(k.split(".")[0:-1]))
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


def start_flow(lru_cache, files_list, next_flow=None):
    exist_files = ray.get(get_cached_files.remote(lru_cache))
    set_exist = set(exist_files)
    set_all = set(files_list)
    second_split_file = list(set_all - set_exist)
    print(f"exist file: {exist_files}, second_split files:{second_split_file}")
    # stage 1
    time_start = time.time()
    ref_list = []
    for file_name in exist_files:
        print(file_name)
        if not file_name:
            continue
        reader = S3FileReader.remote(
            bucket="tpch-all", key=file_name, lru_cache=lru_cache
        )
        table_ref = reader.table_scan.remote()
        ref_list.append(table_ref)
    table_contents = [ray.get(ref) for ref in ref_list]
    time_stage_1 = time.time()
    # stage 2
    ref_list = []
    for file_name in second_split_file:
        print(file_name)
        if not file_name:
            continue
        reader = S3FileReader.remote(
            bucket="tpch-all", key=file_name, lru_cache=lru_cache
        )
        table_ref = reader.table_scan.remote()
        ref_list.append(table_ref)
    table_contents = [ray.get(ref) for ref in ref_list]
    time_stage_2 = time.time()
    print(
        f"stage 1 time: {time_stage_1-time_start},stage 2 time:{time_stage_2-time_stage_1}"
    )


# 示例使用方法
if __name__ == "__main__":
    # 创建S3FileReader实例
    file_names = list_files("tpch-all", "tpch_100g_small/customer")
    file_names = [f"tpch_100g_small/customer/{file_name}" for file_name in file_names if file_name]
    lru_cache = LRUCache()
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
