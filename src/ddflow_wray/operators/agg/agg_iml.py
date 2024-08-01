# %load_ext cudf.pandas
import multiprocessing
import os
import json
import time
import argparse
import traceback
from typing import Dict

import psutil
import pandas as pd
import cudf 

dataset_dict = {}


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


def load_lineitem(root: str):

    data_path = root
    df = pd.read_parquet(data_path)
    nan_per_column = df.isna().sum()
    print(nan_per_column)
    df = df.fillna('')
    # df.l_shipdate = pd.to_datetime(df.l_shipdate, format="%Y-%m-%d")
    # df.l_receiptdate = pd.to_datetime(df.l_receiptdate, format="%Y-%m-%d")
    # df.l_commitdate = pd.to_datetime(df.l_commitdate, format="%Y-%m-%d")
    df['l_orderkey'] = df['l_orderkey'].astype('int64')
    df['l_partkey'] = df['l_partkey'].astype('int64')
    df['l_suppkey'] = df['l_suppkey'].astype('int64')
    df['l_linenumber'] = df['l_linenumber'].astype('int64')
    df['l_quantity'] = df['l_quantity'].astype('float64')
    df['l_extendedprice'] = df['l_extendedprice'].astype('float64')
    df['l_discount'] = df['l_discount'].astype('float64')
    df['l_tax'] = df['l_tax'].astype('float64')
    df['l_returnflag'] = df['l_returnflag'].astype('str')
    df['l_linestatus'] = df['l_linestatus'].astype('str')
    df['l_shipdate'] = pd.to_datetime(df['l_shipdate'])
    df['l_commitdate'] = pd.to_datetime(df['l_commitdate'])
    df['l_receiptdate'] = pd.to_datetime(df['l_receiptdate'])
    df['l_shipinstruct'] = df['l_shipinstruct'].astype('str')
    df['l_shipmode'] = df['l_shipmode'].astype('str')
    df['l_comment'] = df['l_comment'].astype('str')
    result = df
    dataset_dict["lineitem"] = result

    return result

# 自定义的agg_iml函数
def agg_iml(series, column_name, operation):
    # 将pandas Series转换为cudf Series
    cudf_series = cudf.Series(series)
    # 选择对应的聚合操作
    if operation == 'sum':
        return cudf_series.sum()
    elif operation == 'mean':
        return cudf_series.mean()
    elif operation == 'count':
        return cudf_series.count()
    else:
        raise ValueError(f"Unsupported operation: {operation}")

def cudf_agg(df):
    
    operations = {
        "l_quantity": "sum",
        "l_extendedprice": "sum",
        "disc_price": "sum",
        "charge": "sum",
        "avg_qty": "mean",
        "avg_price": "mean",
        "l_discount": "mean",
        "l_orderkey": "count",
    }
    grouped = df.groupby(["l_returnflag", "l_linestatus"])

    # 存储最终结果
    result = []

    # 遍历每个分组
    for group_name, group_df in grouped:
        group_result = {}
        for column, operation in operations.items():
            group_result[column] = agg_iml(group_df[column], column, operation)
        result.append((group_name, group_result))

    # 整合结果
    final_result = {}
    for group_name, group_result in result:
        final_result[group_name] = group_result

    # 将结果转换为DataFrame
    final_df = pd.DataFrame.from_dict({k: v for k, v in final_result.items()}, orient='index')
    final_df.reset_index(inplace=True)
    final_df.columns = ["l_returnflag", "l_linestatus"] + list(operations.keys())


    return final_df

def q01(tbl):
    time_start = time.time()
    # lineitem = cudf.DataFrame.from_arrow(tbl)
    lineitem = tbl
    time_read_pa = time.time()
    date = pd.Timestamp("1998-09-02")
    lineitem_filtered = lineitem.loc[
                        :,
                        [
                            "l_orderkey",
                            "l_quantity",
                            "l_extendedprice",
                            "l_discount",
                            "l_tax",
                            "l_returnflag",
                            "l_linestatus",
                            "l_shipdate",
                        ],
                        ]
    sel = lineitem_filtered.l_shipdate <= date
    lineitem_filtered = lineitem_filtered[sel]
    lineitem_filtered = lineitem_filtered.loc[
                    :,
                    [
                        "l_orderkey",
                        "l_quantity",
                        "l_extendedprice",
                        "l_discount",
                        "l_tax",
                        "l_returnflag",
                        "l_linestatus",
                    ],
                    ]
    time_filter = time.time()
    lineitem_filtered["avg_qty"] = lineitem_filtered.l_quantity
    lineitem_filtered["avg_price"] = lineitem_filtered.l_extendedprice
    lineitem_filtered["disc_price"] = lineitem_filtered.l_extendedprice * (
            1 - lineitem_filtered.l_discount
    )
    lineitem_filtered["charge"] = (
            lineitem_filtered.l_extendedprice
            * (1 - lineitem_filtered.l_discount)
            * (1 + lineitem_filtered.l_tax)
    )

    time_projection = time.time()
    print(f"start ")
    total = cudf_agg(lineitem_filtered)
    # linitem_agg = cudf.from_pandas(lineitem_filtered)
    
    time_groupby = time.time()
    print(total)
    # total = (
    #     total.sort_values(["l_returnflag", "l_linestatus"])
    #     .rename(columns={
    #         "l_quantity": "sum_qty",
    #         "l_extendedprice": "sum_base_price",
    #         "disc_price": "sum_disc_price",
    #         "charge": "sum_charge",
    #         "l_discount": "avg_disc",
    #         "l_orderkey": "count_order"
    #     })
    # )
    time_sort = time.time()
    print(f"time_read,{time_read_pa - time_start},"
          f"time_filter,{time_filter - time_read_pa},"
          f"time_projection,{time_projection - time_filter},"
          f"time_groupby,{time_groupby - time_projection},"
          f"time_sort,{time_sort - time_groupby}")

    return total


def main():
    tbl = load_lineitem("/opt/adp/tpch/tpch_1g/lineitem.parquet")
    res = q01(tbl)
    print(res)


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
    main()
    monitor_process.terminate()