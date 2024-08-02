import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.cuda as cuda
import numpy as np
import time


table = pq.read_table("/opt/adp/tpch/tpch_10g/lineitem.parquet", use_threads=True, pre_buffer=True)

# 获取GPU设备
cudf = cuda.Context()

# 将PyArrow表转换为CUDA内存中的Arrow
# 表
record_batch = table.to_batches()[0]
device_buffer = cudf.new_buffer(record_batch.nbytes)


# 测量传输时间
start_time = time.time()

# 将数据从CPU内存传输到GPU内存
with device_buffer.copy_from_host(record_batch.buffers()[1]):
    pass

end_time = time.time()

# 计算传输时间
transfer_time = end_time - start_time
print(f"Data transfer time: {transfer_time:.6f} seconds")
print(f"Data transfer rate: {record_batch.nbytes / 1024 / 1024 / transfer_time:.6f} MB/s")

# 清理资源
device_buffer.free()
cudf.close()