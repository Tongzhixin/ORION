import socket
import threading
import pyarrow as pa
from ray.util.queue import Queue

# 假设这个队列已经被其他进程不断写入数据
data_queue = Queue()

def send_data_to_server(server_ip, server_port):
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((server_ip, server_port))

    while True:
        table = data_queue.get()
        sink = pa.BufferOutputStream()
        writer = pa.RecordBatchStreamWriter(sink, table.schema)
        writer.write_table(table)
        writer.close()
        buffer = sink.getvalue()
        client.sendall(buffer.to_pybytes())

def start_client(server_ip, server_port, num_threads=10):
    for _ in range(num_threads):
        threading.Thread(target=send_data_to_server, args=(server_ip, server_port)).start()

if __name__ == "__main__":
    server_ip = 'A机器的IP地址'  # 替换为A机器的实际IP地址
    server_port = 12345
    start_client(server_ip, server_port)