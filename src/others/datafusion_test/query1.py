import multiprocessing
import os, time
import sys
import psutil
import pyarrow.fs as fs
import pyarrow.parquet as pq

import ray

from datafusion import RuntimeConfig, SessionConfig, SessionContext, col, lit
from datafusion import functions as f

# ray.init()

os.environ["AWS_ACCESS_KEY_ID"]=""
os.environ["AWS_SECRET_ACCESS_KEY"]=""
os.environ["AWS_ENDPOINT"]=""

def monitor_memory(pid, output_file):
    with open(output_file, 'a') as file:
        file.write(f"---------------\n")
        while True:
            try:
                # 获取指定PID的进程
                process = psutil.Process(pid)
                memory_info = process.memory_info()
                cpu_percent = process.cpu_percent(interval=0.1)  # 间隔0.1秒采样CPU使用率

                # 将资源占用信息写入文件
                file.write(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}, "
                           f"RSS: {memory_info.rss / (1024 * 1024):.2f} MB, "
                           f"VMS: {memory_info.vms / (1024 * 1024):.2f} MB, "
                           f"CPU: {cpu_percent:.2f}%\n")
                file.flush()  # 确保立即写入文件

                # 等待0.5秒
                time.sleep(0.5)
            except psutil.NoSuchProcess:
                # 如果进程已经结束，则退出循环
                break


def setCtx():
    runtime = RuntimeConfig()
    config = (
        SessionConfig(
            {
                "datafusion.execution.planning_concurrency": "2",
                "datafusion.explain.physical_plan_only": "true",
                "datafusion.explain.show_statistics": "true",
            }
        )
        .with_create_default_catalog_and_schema(True)
        .with_default_catalog_and_schema("datafusion", "tpch")
        .with_information_schema(True)
        .with_target_partitions(2)
    )
    ctx = SessionContext(config, runtime)
    # benchmark_dir = "/state/partition/zxtong/tpch_gen/duckdb/tpch_10g"
    # ctx.register_parquet("customer", os.path.join(benchmark_dir, "customer.parquet"))
    # ctx.register_parquet("lineitem", os.path.join(benchmark_dir, "lineitem.parquet"))
    # ctx.register_parquet("nation", os.path.join(benchmark_dir, "nation.parquet"))
    # ctx.register_parquet("orders", os.path.join(benchmark_dir, "orders.parquet"))
    # ctx.register_parquet("part", os.path.join(benchmark_dir, "part.parquet"))
    # ctx.register_parquet("partsupp", os.path.join(benchmark_dir, "partsupp.parquet"))
    # ctx.register_parquet("region", os.path.join(benchmark_dir, "region.parquet"))
    # ctx.register_parquet("supplier", os.path.join(benchmark_dir, "supplier.parquet"))
    return ctx
# @ray.remote

import s3fs

def file_to_batches(s3_path):
    s3 = s3fs.S3FileSystem(endpoint_url="",key="",secret="",use_ssl=False)

    # ds = pq.read_table(s3_path,filesystem=s3)
    # print(ds)
def distinct_linitem():
    runtime = RuntimeConfig()
    config = (
        SessionConfig(
            {
                "datafusion.execution.planning_concurrency": "16",
                "datafusion.explain.physical_plan_only": "true",
                "datafusion.explain.show_statistics": "true",
            }
        )
        .with_create_default_catalog_and_schema(True)
        .with_default_catalog_and_schema("datafusion", "tpch")
        .with_information_schema(True)
        .with_target_partitions(16)
    )
    ctx = SessionContext(config, runtime)
    benchmark_dir = "/opt/adp/tpch/tpch_10g"
    ctx.register_parquet("lineitem", os.path.join(benchmark_dir, "lineitem.parquet"))
    # ctx.register_parquet("customer", os.path.join(benchmark_dir, "customer.parquet"))
    # ctx.register_record_batches("lineitem", obj_ref)
    # ctx.register_parquet("nation", os.path.join(benchmark_dir, "nation.parquet"))
    # ctx.register_parquet("orders", os.path.join(benchmark_dir, "orders.parquet"))
    # ctx.register_parquet("part", os.path.join(benchmark_dir, "part.parquet"))
    # ctx.register_parquet("partsupp", os.path.join(benchmark_dir, "partsupp.parquet"))
    # ctx.register_parquet("region", os.path.join(benchmark_dir, "region.parquet"))
    # ctx.register_parquet("supplier", os.path.join(benchmark_dir, "supplier.parquet"))

    query_content = """
SELECT
    l_shipdate,
    COUNT(DISTINCT l_partkey) AS unique_parts,
    COUNT(DISTINCT l_orderkey) AS unique_orders
FROM
    lineitem
GROUP BY
    l_shipdate;
    """
    query_content = """
    SELECT
    l_partkey,
    COUNT(DISTINCT l_orderkey) AS unique_orders,
    COUNT(DISTINCT l_suppkey) AS unique_suppliers
FROM
    hive.s3tpch_10g_csv_small.lineitem
GROUP BY
    l_partkey;
#     """

    tmp = query_content.split(";")
    queries = []
    for str in tmp:
        if len(str.strip()) > 0:
            queries.append(str.strip())
    print(queries)
    try:
        start = time.time()
        for sql in queries:
            # print(sql)
            df = ctx.sql(sql)
            # df.explain(True, True)
            result_set = df.collect()
            print(df.to_arrow_table().to_pandas())
            # df.show()
        end = time.time()
        time_millis = (end - start) * 1000

        print("q{},{}".format(1, round(time_millis, 1)))
    except Exception as e:
        print("query", 1, "failed", e)




if __name__ == "__main__":
    # 获取当前进程的PID
    pid = os.getpid()
    # 指定要监控的PID
    pid_to_monitor = pid  # 替换为你想要监控的PID

    # 指定输出文件
    output_file = 'memory_usage.txt'
    print(f"pid:{pid}")
    # 创建一个新的进程来监控内存占用
    monitor_process = multiprocessing.Process(target=monitor_memory, args=(pid_to_monitor, output_file))
    # 启动进程
    monitor_process.start()

    
    
    fp = open("print_split.log", "a")
    sys.stdout = fp  # print重定向到文件

    distinct_linitem()
    sys.stdout = sys.__stdout__  # 恢复print重定向到标准输出

    monitor_process.terminate()