import pyarrow.parquet as pq
import s3fs
import ray

from typing import List, Union
import time
from ddflow.MemPool import LRUCache
from ray.util.queue import Queue

from ddflow.nodes import (
    BufferSource,
    FileSource,
    LocalSource,
    NullSource,
)


@ray.remote(num_cpus=0.2)
def table_scan_local(bucket, file_path, columns, filters):
    time_start = time.time()
    # table = ray.get(self.mempool.get.remote(data_key))
    full_path = f"{bucket}/{file_path}"

    table = pq.read_table(
        full_path,
        columns=columns,
        use_threads=True,
        filters=filters,
        pre_buffer=True,
    )
    print(f"table_scan file{file_path},time: {time.time() - time_start}")
    return table
@ray.remote(num_cpus=0.1)
def table_scan_remote(bucket, file_path, columns, filters ,output_queue):
    time_start = time.time()
    # table = ray.get(self.mempool.get.remote(data_key))

    full_path = f"{bucket}/{file_path}"
    time_start = time.time()
    import pyarrow.fs as fs

    s3 = fs.S3FileSystem(
        endpoint_override="",
        access_key="",
        secret_key="",
        scheme="http",
    )
    table = pq.read_table(
        full_path,
        columns=columns,
        filesystem=s3,
        filters=filters,
        pre_buffer=True,
    )
    print(f"table_scan file {file_path},time: {time.time() - time_start}")
    output_queue.put(table)

@ray.remote(num_cpus=1)
class TableScan:
    def __init__(
        self, mempool: LRUCache, input_queue: Queue, output_queue: Queue
    ) -> None:
        self.mempool = mempool
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.all_time = time.time()

        self.refs = []
        self.name = "scan"
    def get_name(self):
        return self.name

    def run(self):
        max_cpu = 30
        while True:
            input_command: Union[FileSource, BufferSource, LocalSource, NullSource] = (
                self.input_queue.get()
            )
            if len(self.refs) > max_cpu:
                ready_refs, self.refs = ray.wait(self.refs, num_returns=1)
                for t in ready_refs:
                    self.output_queue.put(ray.get(t))       
            
            if isinstance(input_command, FileSource):
                print(f"file source:{input_command.file_path}")
                self.refs.append(table_scan_remote.remote(
                    input_command.bucket,
                    input_command.file_path,
                    input_command.columns,
                    input_command.filters,
                ))
            elif isinstance(input_command, LocalSource):
                print(f"LocalSource:{input_command.file_path}")
                self.refs.append(table_scan_local.remote(
                    input_command.bucket,
                    input_command.file_path,
                    input_command.columns,
                    input_command.filters
                ))
            elif isinstance(input_command, NullSource):
                print(f"scan actor end")
                break
            del input_command
        print(f"len(refs):{len(self.refs)}")
        print(f"table scan output queue size:{self.output_queue.qsize()}")
        for t in self.refs:
            self.output_queue.put(t)
        # ray.get(self.refs)
        self.output_queue.put('')
        del input_command
        return
