import pyarrow as pa
import pyarrow.parquet as pq
import ray
from typing import List
from ray.util.queue import Queue



@ray.remote
class BufferOutputFunction():
    def __init__(self,mempool:Queue,file_id:str,output_data) -> None:
        self.mempool = mempool
        self.output_data = output_data
        self.file_id = file_id

    def exec(self):
        self.mempool.put_async((self.file_id, self.output_data))
        return self.file_id

@ray.remote(num_gpus=0.1)
def sink(mempool:Queue,file_id:str,output_data):
    mempool.put_async((file_id, output_data))
    return file_id