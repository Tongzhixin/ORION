import ray
import cudf
import time
import pyarrow as pa
import pyarrow.parquet as pq
@ray.remote(num_cpus=4, num_gpus=1)
def gpu_only(origin_table):
    print(f"now time:{time.time()}")
    time_start = time.time()
    # cudf_df = cudf.read_parquet("/opt/adp/tpch/tpch_10g/lineitem.parquet")
    cudf_df = cudf.DataFrame.from_arrow(origin_table)
    time_read_table = time.time()

    # 计算 unique_orders
    unique_orders = cudf_df.groupby('l_partkey')['l_orderkey'].nunique().reset_index()
    unique_orders = unique_orders.rename(columns={'l_orderkey': 'unique_orders'})

    # 计算 unique_suppliers
    unique_suppliers = cudf_df.groupby('l_partkey')['l_suppkey'].nunique().reset_index()
    unique_suppliers = unique_suppliers.rename(columns={'l_suppkey': 'unique_suppliers'})


    # 合并结果
    result = unique_orders.merge(unique_suppliers, on='l_partkey')
    # result_agg_1 = cudf_df.groupby(group_keys).agg(operations)
    # result_agg_2 = cudf_df.groupby(group_keys).agg(operations)
    print(result)
    print(f"read data time{time_read_table-time_start}")
    print(f"agg use time:{time.time()-time_read_table}")
    print(f"total use time:{time.time()-time_start}")
    return result.to_arrow(),time_start

if __name__ == "__main__":
    ray.init()
    time_start = time.time()
    table = pq.read_table("/opt/adp/tpch/tpch_10g/lineitem.parquet", use_threads=True, pre_buffer=True)
    table = table.select(["l_partkey", "l_orderkey", "l_suppkey"])
    time_read_table = time.time()
    print(time_read_table,f"table size:{table.nbytes / 1024 / 1024} MB")
    result,agg_start_time = ray.get(gpu_only.remote(table))
    time_gpu_only = time.time()
    print(f"read parquet time:{time_read_table-time_start}")
    print(f"gpu only use time:{time_gpu_only-time_read_table}")
    print(f"result size:{result.nbytes / 1024 / 1024} MB")
    
    print(f"data transport time :{agg_start_time-time_read_table}")
    print("----------------------")
    
