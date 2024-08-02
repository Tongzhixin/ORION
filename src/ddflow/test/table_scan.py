import json
import ray
import time,os
import pyarrow.parquet as pq
from ray.util.queue import Queue
import pyarrow
from ray.util.actor_pool import ActorPool

from ddflow.MemPool import LRUCache

ray.init(
    ignore_reinit_error=True,
    num_cpus=16,
    object_store_memory=40474836480,
    _system_config={
        "max_io_workers": 4,  # More IO workers for parallelism.
        "min_spilling_size": 100 * 1024 * 1024,
        "object_spilling_config": json.dumps(
            {
              "type": "filesystem",
              "params": {
                "directory_path": [
                  "/tmp/spill",
                  "/tmp/spill_1",
                  "/tmp/spill_2",
                ]
              },
            }
        )
    }
    # _system_config={"automatic_object_spilling_enabled": False}
)

# lru_cache = LRUCache.remote()
@ray.remote(num_cpus=0.2)
def table_scan(bucket, file_path, columns):
    time_start = time.time()
    # table = ray.get(self.mempool.get.remote(data_key))
    full_path = f"{bucket}/{file_path}"
    columns=[
            "l_orderkey",
            "l_quantity",
            "l_extendedprice",
            "l_discount",
            "l_tax",
            "l_returnflag",
            "l_linestatus",
            "l_shipdate",
        ]
    from datetime import datetime, timedelta

    date_reference = datetime.strptime("1998-12-01", "%Y-%m-%d")
    date_threshold = date_reference - timedelta(days=68)
    date_threshold_date = date_threshold.date()
    projectionElement_list = []
    filter_expression = [[("l_shipdate", "<=", date_threshold_date)]]

    table = pq.read_table(
        full_path,
        columns=columns,
        use_threads=True,
        filters=filter_expression,
        pre_buffer=True,
    )
    print(f"table_scan file{file_path},time: {time.time() - time_start}")
    # del table
    # time_put = time.time()
    # lru_cache.put.remote(file_path,table)
    ray.put(table)
    return (table, file_path)
    # print(f"table_scan put queue{file_path},time: {time.time() - time_put}")
    # del table
@ray.remote(num_cpus=0.1)
def table_scan_remote(bucket, file_path, columns):
    time_start = time.time()
    # table = ray.get(self.mempool.get.remote(data_key))

    full_path = f"{bucket}/{file_path}"
    
    columns=[
            "l_orderkey",
            "l_quantity",
            "l_extendedprice",
            "l_discount",
            "l_tax",
            "l_returnflag",
            "l_linestatus",
            "l_shipdate",
        ]
    from datetime import datetime, timedelta

    date_reference = datetime.strptime("1998-12-01", "%Y-%m-%d")
    date_threshold = date_reference - timedelta(days=68)
    date_threshold_date = date_threshold.date()
    projectionElement_list = []
    filter_expression = [[("l_shipdate", "<=", date_threshold_date)]]

    time_start = time.time()


    full_path = f"{bucket}/{file_path}"

    import pyarrow.fs as fs

    s3 = fs.S3FileSystem(
        endpoint_override="http://localhost:39999/api/v1/s3/",
        access_key="",
        secret_key="",
        scheme="http",
    )

    table = pq.read_table(
        full_path,
        columns=columns,
        filesystem=s3,
        filters=filter_expression,
        pre_buffer=True,
    )
    print(f"table_scan file{file_path},time: {time.time() - time_start}")
    return (table, file_path)




@ray.remote(num_cpus=0.1)
def process_table(table):
    if not table:
        return
    # Process the table as needed
    print(f"Processed table path: {table[1]} with {table[0].num_rows} rows")
    file_path = str(table[1])
    del table  # Explicitly delete the table object to free up memory
    return file_path


def main():
    bucket = "/opt/adp/tpch"
    prefix = "tpch_100g_small/lineitem"
    files_list = os.listdir(bucket + '/' + prefix)
    exist_files = [prefix + "/" + file for file in files_list if file]
    # bucket = "tpch-all"
    
    ref_list = []
    next_ref_list = []
    max_cpu=30
    time_start = time.time()
    for file_name in exist_files:
        if len(ref_list) > max_cpu:
            ready_refs, ref_list = ray.wait(ref_list, num_returns=1)
            for t in ready_refs:
                if len(next_ref_list) > max_cpu:
                    ready_refs_2, next_ref_list = ray.wait(next_ref_list, num_returns=1)
                    for t2 in ready_refs_2:
                        res_path = ray.get(t2)
                        del t2
                        del res_path
                next_ref_list.append(process_table.remote(t))

        table_ref = table_scan.remote(bucket, file_name, columns=None)
        ref_list.append(table_ref)
    
    # final
    print(len(ref_list), len(next_ref_list))
    while True:
        ready_refs, ref_list = ray.wait(ref_list, num_returns=1)
        for t in ready_refs:
            ready_refs_2, next_ref_list = ray.wait(next_ref_list, num_returns=1)
            for t2 in ready_refs_2:
                res_path = ray.get(t2)
                del t2
                del res_path
            next_ref_list.append(process_table.remote(t))
        if not ref_list:
            break
    print(len(ref_list), len(next_ref_list))
    print(ray.get(next_ref_list))
    
    
    # tasks = [table_scan_remote.remote(bucket, file, columns=None) for file in exist_files if file]
    

    # print(tasks)
    # tasks.append(process.run.remote())
    
    # while tasks:
    #     ready, tasks = ray.wait(tasks, num_returns=min(16, len(tasks)))
    #     print(len(ready),len(tasks))
    #     # ray.get(ready)
    #     # del ready
    #     for ref in ready:
    #         table_path = ray.get(ref)

    #         # print(f"Processed table path: {table[1]} with {table[0].num_rows} rows")
    #         del table_path
    #         del ref  # Explicitly delete the table object to free up memory

        # Optionally, you can add a sleep to avoid busy-waiting
        # time.sleep(0.1)
    # queue.put('')
    print(f"total time: {time.time() - time_start}")
    time.sleep(90)
    ray.shutdown()


if __name__ == "__main__":
    main()

    