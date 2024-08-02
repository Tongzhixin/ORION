from concurrent.futures import ProcessPoolExecutor
import multiprocessing
import os, time
import sys
import psutil

from datafusion import RuntimeConfig, SessionConfig, SessionContext, col, lit
from datafusion import functions as f

import pyarrow.parquet as pq
import pyarrow as pa
from multiprocessing import Manager, Pool, cpu_count


def monitor_memory(pid, output_file):
    with open(output_file, "a") as file:
        file.write(f"---------------\n")
        while True:
            try:
                # 获取指定PID的进程
                process = psutil.Process(pid)
                memory_info = process.memory_info()
                cpu_percent = process.cpu_percent(
                    interval=0.1
                )  # 间隔0.1秒采样CPU使用率

                # 将资源占用信息写入文件
                file.write(
                    f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}, "
                    f"RSS: {memory_info.rss / (1024 * 1024):.2f} MB, "
                    f"VMS: {memory_info.vms / (1024 * 1024):.2f} MB, "
                    f"CPU: {cpu_percent:.2f}%\n"
                )
                file.flush()  # 确保立即写入文件

                # 等待0.5秒
                time.sleep(0.5)
            except psutil.NoSuchProcess:
                # 如果进程已经结束，则退出循环
                break



class workflow_optimize:
    def __init__(self, file_path, source_table_name, arrow_table, time_read) -> None:
        self.file_path = file_path
        self.source_table_name = source_table_name
        self.time_usage = []
        self.size_usage = []
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
            .with_target_partitions(1)
        )
        self.ctx = SessionContext(config, runtime)
        # time_start = time.time()
        # table = pq.read_table(
        # parquet_file,
        # # columns=columns,
        # use_threads=True,
        # # filters=filter_expression,
        # pre_buffer=True,
        # )
        # print(f"read parquet time:{time.time() - time_start}s")
        self.size_usage.append(arrow_table.nbytes / 1024 / 1024)
        self.time_usage.append(time_read)
        self.ctx.register_record_batches("lineitem", [arrow_table.to_batches()])
        # print(f"read total time:{time.time() - time_start}s")
        # self.ctx.register_table(self.source_table_name, table)

    def stage1(self, batch_name):
        time_start = time.time()
        df_lineitem = self.ctx.table(batch_name)
        sel = col("l_shipdate") < lit("1998-12-01")
        df_lineitem = df_lineitem.filter(sel)
        df_lineitem = df_lineitem.select_columns(
            "l_quantity",
            "l_extendedprice",
            "l_discount",
            "l_tax",
            "l_returnflag",
            "l_linestatus",
            "l_shipdate",
            "l_orderkey",
        )
        result_batches = df_lineitem.collect()
        time_collect = time.time()
        self.ctx.register_record_batches("stage1", [result_batches])
        self.time_usage.append(time_collect - time_start)
        result_table = pa.Table.from_batches(result_batches)
        self.size_usage.append(result_table.nbytes / 1024 / 1024)

    def stage2(self, batch_name):
        time_start = time.time()
        df_lineitem = self.ctx.table(batch_name)
        df_lineitem = df_lineitem.aggregate(
            [col("l_returnflag"), col("l_linestatus")],
            [
                f.sum(col("l_quantity")),
                f.sum(col("l_extendedprice")),
                f.sum(col("l_quantity") * (lit(1) - col("l_discount"))),
                f.sum(
                    col("l_extendedprice")
                    * (lit(1) - col("l_discount"))
                    * (lit(1) + col("l_tax"))
                ),
                f.mean(col("l_quantity")),
                f.mean(col("l_extendedprice")),
                f.mean(col("l_discount")),
                f.count(col("l_orderkey")),
            ],
        )
        df_lineitem = df_lineitem.sort(
            col("l_returnflag").sort(True, True), col("l_linestatus").sort(True, True)
        )
        # df_lineitem.explain(True, False)
        result_batches = df_lineitem.collect()
        time_collect = time.time()
        self.ctx.register_record_batches("stage2", [result_batches])
        self.time_usage.append(time_collect - time_start)
        result_table = pa.Table.from_batches(result_batches)
        self.size_usage.append(result_table.nbytes / 1024 / 1024)

    def workflow(self):
        time_start = time.time()
        self.stage1(self.source_table_name)
        self.stage2("stage1")
        # self.time_usage.append(time.time() - time_start)
        # self.size_usage.append(
        #     self.ctx.table("stage2").to_arrow_table().nbytes / 1024 / 1024
        # )
        line_str = ""
        for time_usage in self.time_usage:
            line_str += f"{time_usage},"
        for size_usage in self.size_usage:
            line_str += f"{size_usage},"
        # print(line_str)
        return line_str


def q01_optimize_1(ctx):
    time_start = time.time()
    df_lineitem = ctx.table("lineitem")
    sel = col("l_shipdate") < lit("1998-12-01")
    df_lineitem = df_lineitem.filter(sel)
    df_lineitem = df_lineitem.select_columns(
        "l_quantity",
        "l_extendedprice",
        "l_discount",
        "l_tax",
        "l_returnflag",
        "l_linestatus",
        "l_shipdate",
        "l_orderkey",
    )

    df_lineitem = df_lineitem.aggregate(
        [col("l_returnflag"), col("l_linestatus")],
        [
            f.sum(col("l_quantity")),
            f.sum(col("l_extendedprice")),
            f.sum(col("l_quantity") * (lit(1) - col("l_discount"))),
            f.sum(
                col("l_extendedprice")
                * (lit(1) - col("l_discount"))
                * (lit(1) + col("l_tax"))
            ),
            f.mean(col("l_quantity")),
            f.mean(col("l_extendedprice")),
            f.mean(col("l_discount")),
            f.count(col("l_orderkey")),
        ],
    )
    df_lineitem = df_lineitem.sort(
        col("l_returnflag").sort(True, True), col("l_linestatus").sort(True, True)
    )

    df_lineitem.explain(True, True)
    # df_lineitem.collect()
    time_end = time.time()
    print(df_lineitem.execution_plan())
    # df_lineitem.show()
    print(f"q1 query time: {time_end - time_start}")


def process_file(file_name):
    time_a_start = time.time()
    table = pq.read_table(file_name, use_threads=True, pre_buffer=True)
    time_read = time.time() - time_a_start
    workflow_a = workflow_optimize(file_name, "lineitem", table, time_read)
    result = workflow_a.workflow()
    # queue.put((file_name,result))
    del table
    return result

def process_only(table,file_name):
    workflow_a = workflow_optimize(file_name, "lineitem", table, time_read)
    result = workflow_a.workflow()
    # queue.put((file_name,result))
    del table
    return result

def table_scan(file_name):
    table = pq.read_table(file_name, use_threads=True, pre_buffer=True)
    return (table,file_name)

if __name__ == "__main__":
    # 获取当前进程的PID
    # pid = os.getpid()
    # # 指定要监控的PID
    # pid_to_monitor = pid  # 替换为你想要监控的PID

    # # 指定输出文件
    # output_file = "memory_usage.txt"
    # print(f"pid:{pid}")
    # # 创建一个新的进程来监控内存占用
    # monitor_process = multiprocessing.Process(
    #     target=monitor_memory, args=(pid_to_monitor, output_file)
    # )
    # # 启动进程
    # monitor_process.start()

    # ctx = setCtx()
    fp = open("print_split.log", "a")
    sys.stdout = fp  # print重定向到文件

    bucket = "/opt/adp/tpch"
    prefix = "tpch_100g_small/lineitem"
    files_list = os.listdir(bucket + "/" + prefix)
    exist_files = [bucket + "/" + prefix + "/" + file for file in files_list if file]
    # bucket = "tpch-all"
    time_start = time.time()

    table_list = []
    with ProcessPoolExecutor(32) as executor:
        futures = [
            executor.submit(table_scan, file_name) for file_name in exist_files
        ]
        for future in futures:
            table_list.append(future.result())
    
    time_list = []
    with ProcessPoolExecutor(32) as executor:
        futures = [
            executor.submit(process_file, args=(table, file_name)) for table, file_name in table_list
        ]
        for future in futures:
            time_list.append(future.result())
    for time_read in time_list:
        print(time_read)

    # time_list = []
    # count=0
    # for file_name in exist_files:
    #     time_a_start = time.time()
    #     table = pq.read_table(file_name, use_threads=True, pre_buffer=True)
    #     # time_list.append(time.time() - time_start)
    #     time_read = time.time() - time_a_start
    #     workflow_a = workflow_optimize(file_name, "lineitem", table,time_read)
    #     time_line_str = workflow_a.workflow()
    #     print(time_line_str)
    #     if count>10:
    #         break
    #     count+=1
        

    print(f"q1 query time: {time.time() - time_start}")
    sys.stdout = sys.__stdout__  # 恢复print重定向到标准输出

    # monitor_process.terminate()
