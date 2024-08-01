import pyarrow as pa
import pyarrow.parquet as pq
import ray
import time
from typing import List
from ray.util.queue import Queue
import socket


@ray.remote(num_cpus=2)
class Sink:
    def __init__(
        self, input_queue: Queue, server_ip=None, server_port=None, num_threads=10
    ) -> None:
        self.mempool = input_queue
        self.server_ip = server_ip
        self.server_port = server_port
        self.num_threads = num_threads
        self.name = "sink"
    def get_name(self):
        return self.name

    def send_data_to_server(self, table: pa.Table):
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect((self.server_ip, self.server_port))
        sink_table = pa.BufferOutputStream()
        writer = pa.RecordBatchStreamWriter(sink_table, table.schema)
        writer.write_table(table)
        writer.close()
        buffer = sink_table.getvalue()
        client.sendall(buffer.to_pybytes())

    def run(self):
        while True:
            table_tuple = self.mempool.get()
            if table_tuple:
                # self.send_data_to_server(table)
                # print(f"table size:{table_tuple.nbytes/1024}KB")
                del table_tuple
            else:
                print(f"sink actor end")
                del table_tuple
                return
            
            # time.sleep(0.1)
