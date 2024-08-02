
import os
import time
import pyarrow.parquet as pq
import pyarrow as pa
import s3fs
import pyarrow.fs as fs

s3 = fs.S3FileSystem(
        endpoint_override="http://localhost:39999/api/v1/s3",
        access_key="",
        secret_key="",
        scheme="http",
)
time_start=time.time()
table = pq.read_table(
    f"tpch_100g_small/lineitem/lineitem_part213.parquet",
    filesystem=s3,
    pre_buffer=True,
)
# table = pq.read_table(
#         "/opt/adp/tpch/tpch_100g_small/lineitem/lineitem_part212.parquet",
#         use_threads=True,
#         pre_buffer=True,
# )


print(table.nbytes / 1024 / 1024)
print(f"time use {time.time() - time_start}")