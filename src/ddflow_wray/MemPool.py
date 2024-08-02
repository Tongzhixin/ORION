import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
import ray
import asyncio
from collections import defaultdict, OrderedDict
from ray.util.state import get_actor, get_objects
from typing import List
import time


@ray.remote
class LRUCache:
    def __init__(self, max_size=25 * 1024):
        self.max_size = max_size
        self.current_size = 0
        self.cache = OrderedDict()  # stores the key and ray object ref
        self.key_size = {}  # stores the size of each value
        self.usage_count = defaultdict(int)  # stores usage count of each key
        # self.frequency_list = defaultdict(set)  # stores keys by usage frequency

    def _evict(self):
        time_evict = time.time()
        evict_keys = []
        while self.current_size > self.max_size * 0.8:

            evict_key, ref = self.cache.popitem(last=False)
            size = self.key_size[evict_key]
            ray._private.internal_api.free(ref)
            self.current_size -= size
            del self.key_size[evict_key]
            del self.usage_count[evict_key]
            evict_keys.append(evict_key)
        print(f"evict time:{time.time()-time_evict}, Evicted key: {evict_keys}")

    def put(self, key, value):
        ref = ray.put(value)
        self.cache[key] = ref
        self.cache.move_to_end(key, last=True)
        ref_id = ref.binary().hex()
        state = get_objects(ref_id)
        # print(object_states)
        value_size = state[0].object_size / 1024 / 1024
        # print(f"now memory size {self.current_size}mb")

        self.key_size[key] = value_size
        self.current_size += value_size
        self.usage_count[key] += 1  # Initialize usage count for new key
        # for object_state in object_states:
        #     print(object_state)
        #     value_size = object_state.object_size # Assuming size is the length of the value
        #     ip = object_state.ip
        #     print(value_size,ip)
        if value_size > self.max_size:
            raise ValueError("Value size exceeds the maximum cache size")

        if self.current_size + value_size > self.max_size:
            self._evict()

    def get(self, key):
        if key in self.cache.keys():
            self.cache.move_to_end(key, last=True)
            self.usage_count[key] += 1
            ref = self.cache[key]
            return ray.get(ref)
        else:
            # Read from file (simulate with a dummy function)
            # print(f"{key} not exist")
            return None

    def get_cached_key(self):
        return list(self.cache.keys())

    def get_cached_value(self, key_list):
        col_datas = []
        missed_columns = []
        for k in key_list:
            col_data = self.get(k)
            if col_data:
                col_datas.append(col_data)
            else:
                missed_columns.append(k)
        return col_datas, missed_columns

    def free_fixed_size(self, free_size=18 * 1024):
        have_free = 0
        time_evict = time.time()
        evict_keys = []
        while self.current_size > 1000 and have_free < free_size:

            evict_key, ref = self.cache.popitem(last=False)
            size = self.key_size[evict_key]
            ray._private.internal_api.free(ref)
            self.current_size -= size
            del self.key_size[evict_key]
            del self.usage_count[evict_key]
            have_free += size
            evict_keys.append(evict_key)
        print(f"evict time:{time.time()-time_evict}, Evicted key: {evict_keys}")


@ray.remote
def get_cached_files(lru_cached):
    cached_keys: list = ray.get(lru_cached.get_cached_key.remote())
    files_list = cached_keys
    files_list = list(set(files_list))
    return files_list


@ray.remote
def list_files(bucket, prefix):
    # 创建S3FileSystem实例
    s3 = s3fs.S3FileSystem(
        endpoint_url="http://10.2.64.6:9100",
        key="",
        secret="",
        use_ssl=False,
    )
    file_list = s3.ls(f"s3://{bucket}/{prefix}")
    file_names = [file.split("/")[-1] for file in file_list]
    return file_names


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


# Usage
# max_cache_size = 1000  # Define a suitable max cache size mb
# cache = LRUCache(max_cache_size)

# # Example operations
# cache.put("key1", "value1" * 100)  # Adjust size according to your needs
# print(cache.get("key1"))
# print(cache.get("key2"))  # This will read from "file" and put into cache
