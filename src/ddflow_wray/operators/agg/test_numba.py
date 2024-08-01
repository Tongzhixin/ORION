import cudf, time

from ddflow.nodes import (
    AggregateType,
    AggregateElement,
)
import pyarrow.parquet as pq
import pyarrow
import pandas as pd
import ray
import cudf
import pyarrow as pa
import pyarrow.compute as pc
import itertools
# ray.init(local_mode=True)

@ray.remote(num_cpus=1)
def filter_and_store(table, group_columns, combination):
    # 构建过滤条件
    filters = [
        pc.equal(table[col], val) for col, val in zip(group_columns, combination)
    ]
    if len(filters) > 1:
        combined_filter = pc.and_(*filters)
    else:
        combined_filter = filters[0]
    # 应用过滤条件
    filtered_table = table.filter(combined_filter)
    # 如果过滤后的表不为空，则返回结果
    if filtered_table.num_rows > 0:
        return (combination, filtered_table)
    return None


@ray.remote(num_cpus=1)
def custom_groupby(table, group_columns):
    # 获取唯一的组合
    unique_keys = [pc.unique(table[column]) for column in group_columns]

    # 生成所有可能的组合键
    unique_combinations = list(
        itertools.product(*(key.to_pylist() for key in unique_keys))
    )

    # 提交所有的过滤任务
    filter_tasks = [
        filter_and_store.remote(table, group_columns, combination)
        for combination in unique_combinations
    ]

    # 获取所有任务的结果
    results = ray.get(filter_tasks)
    grouped_tables = {}
    for result in results:
        if result:
            grouped_tables[result[0]] = (
                result[1] if result[1] is not None else None
            )  # {comb: tbl for comb, tbl in results if tbl is not None}
    return grouped_tables


# def custom_groupby(table, group_columns):
#     # 获取唯一的组合
#     unique_keys = [pc.unique(table[column]) for column in group_columns]

#     # 生成所有可能的组合键

#     unique_combinations = list(itertools.product(*(key.to_pylist() for key in unique_keys)))

#     # 用于存放分组结果的字典
#     grouped_tables = {}

#     # 对每个唯一的组合键进行过滤
#     print(len(unique_combinations))
#     for combination in unique_combinations:
#         # 构建过滤条件
#         filters = [pc.equal(table[col], val) for col, val in zip(group_columns, combination)]
#         if len(filters) > 1:
#             combined_filter = pc.and_(*filters)
#         else:
#             combined_filter = filters[0]
#         # 应用过滤条件
#         filtered_table = table.filter(combined_filter)
#         # 如果过滤后的表不为空，则保存
#         if filtered_table.num_rows > 0:
#             grouped_tables[combination] = filtered_table
#     return grouped_tables


@ray.remote(num_gpus=0.1)
def agg_iml(series, operation: AggregateType):
    # import os
    # os.environ["CUDA_DEVICE_ORDER"] = "PCI_BUS_ID"
    # os.environ["CUDA_VISIBLE_DEVICES"] = "0,1"
    # from numba import cuda
    # cuda.detect()
    # 将pandas Series转换为cudf Series
    time_start = time.time()
    cudf_series = cudf.Series(series)
    time_series = time.time()
    res_data = None
    # 选择对应的聚合操作
    if operation.value == AggregateType.SUM.value:
        res_data = cudf_series.sum()
    elif operation.value == AggregateType.MEAN.value:
        res_data = cudf_series.mean()
    elif operation.value == AggregateType.COUNT.value:
        res_data = cudf_series.count()
    else:
        raise ValueError(f"Unsupported operation: {operation}")

    print(f"cudf series use time:{time_series-time_start}")
    print(f"agg only time:{time.time()-time_series}")
    return res_data


@ray.remote(num_cpus=4, num_gpus=1)
def agg_all(tbl, group_keys, agg_elements):
    # os.environ["CUDA_DEVICE_ORDER"] = "PCI_BUS_ID"
    # os.environ["CUDA_VISIBLE_DEVICES"] = "0"
    # from numba import cuda

    # cuda.detect()
    time_start = time.time()

    cudf_df = cudf.DataFrame.from_arrow(tbl)
    # cudf_df = cudf.from_pandas(tbl)
    time_to_df = time.time()
    operations = {}
    for agg_element in agg_elements:
        if agg_element.aggtype.value == AggregateType.COUNT.value:
            operations[agg_element.aggcol] = "count"
            # operations.append((agg_element.aggcol, "count"))
        elif agg_element.aggtype.value == AggregateType.SUM.value:
            operations[agg_element.aggcol] = "sum"
        elif agg_element.aggtype.value == AggregateType.MEAN.value:
            operations[agg_element.aggcol] = "mean"
        elif agg_element.aggtype.value == AggregateType.MIN.value:
            operations[agg_element.aggcol] = "min"
        else:
            print(f"error aggtype is {agg_element.aggtype}")

    result_agg = cudf_df.groupby(group_keys).agg(operations)
    print(f"agg_all from arrow to pandas use:{time_to_df - time_start}")
    print(f"agg_all agg  use:{time.time() - time_to_df}")
    print(f"agg_all gpu time use:{time.time() - time_start}")
    return result_agg.to_arrow()


@ray.remote(num_cpus=3, num_gpus=0.1)
def compute_column():
    # import os
    # os.environ["CUDA_DEVICE_ORDER"] = "PCI_BUS_ID"
    # os.environ["CUDA_VISIBLE_DEVICES"] = "1"
    time_start = time.time()
    data = pq.read_table("/opt/adp/tpch/tpch_10g/lineitem.parquet")
    group_keys = ["l_returnflag", "l_linestatus"]
    agg_elements = [
        AggregateElement("l_quantity", AggregateType.SUM),
        AggregateElement("l_extendedprice", AggregateType.SUM),
    ]
    time_read_table = time.time()
    new_columns = []
    for agg_element in agg_elements:
        new_columns.append(agg_element.aggcol)
    # df = data.to_pandas(types_mapper=pd.ArrowDtype)

    time_to_pandas = time.time()
    data = data.select(group_keys + new_columns)
    # grouped = df.groupby(group_keys)
    grouped = ray.get(custom_groupby.remote(data, group_keys))
    # print(grouped)

    time_groupby = time.time()

    @ray.remote(num_gpus=0.1)
    def agg_iml_remote(group_name, group_df, agg_elements):
        agg_tasks = [
            agg_iml.remote(group_df.column(agg_element.aggcol), agg_element.aggtype)
            for agg_element in agg_elements
        ]
        agg_results = ray.get(agg_tasks)
        group_result = {
            agg_elements[i].aggcol: agg_results[i] for i in range(len(agg_elements))
        }
        return group_name, group_result

    # Create list of Ray tasks for each group
    agg_tasks = [
        agg_iml_remote.remote(group_name, group_df, agg_elements)
        for group_name, group_df in grouped.items()
    ]

    # Gather results from Ray tasks
    results = ray.get(agg_tasks)

    # 存储最终结果
    # result = []
    # for group_name, group_df in grouped.items():
    #     print(group_name, type(group_df))
    #     group_result = {}
    #     time_start_agg = time.time()
    #     for agg_element in agg_elements:
    #         ref_cudf = agg_iml(
    #             group_df.column(agg_element.aggcol), agg_element.aggtype
    #         )
    #         group_result[agg_element.aggcol]=ref_cudf
    #     print(f"agg one time{time.time()-time_start_agg}")
    #     result.append((group_name, group_result))
    time_result = time.time()
    # Convert result to pyarrow.Table
    final_df = []
    for group_name, group_result in results:
        row = {f"{name}": value for name, value in zip(group_keys, group_name)}
        row.update(group_result)
        final_df.append(row)
    table_output = pa.Table.from_pylist(final_df)
    print(table_output)
    print(f"time read table time:{time_read_table -time_start}")
    print(f"time to pandas time:{time_to_pandas -time_read_table}")
    print(f"time to groupby time:{time_groupby-time_to_pandas}")
    print(f"agg only time:{time_result-time_groupby}")
    print(f"result to table:{time.time()-time_result}")
    print(f"agg total use time{time.time()-time_read_table}")
    print(f"total use time{time.time()-time_start}")
    return table_output


@ray.remote(num_cpus=3)
def pyarrow_agg():
    time_start = time.time()
    data = pq.read_table("/opt/adp/tpch/tpch_10g/lineitem.parquet")
    time_read_table = time.time()
    group_keys = ["l_returnflag", "l_linestatus"]
    agg_elements = [
        AggregateElement("l_quantity", AggregateType.SUM),
        AggregateElement("l_extendedprice", AggregateType.SUM),
    ]
    operations = []
    for agg_element in agg_elements:
        if agg_element.aggtype.value == AggregateType.COUNT.value:
            operations.append((agg_element.aggcol, "count"))
        elif agg_element.aggtype.value == AggregateType.SUM.value:
            operations.append((agg_element.aggcol, "sum"))
        elif agg_element.aggtype.value == AggregateType.MEAN.value:
            operations.append((agg_element.aggcol, "mean"))
        elif agg_element.aggtype.value == AggregateType.MIN.value:
            operations.append((agg_element.aggcol, "min"))
        else:
            print(f"error aggtype is {agg_element.aggtype}")

    result_agg = data.group_by(group_keys).aggregate(operations)
    print(result_agg)
    print(f"read data time{time_read_table-time_start}")
    print(f"agg use time:{time.time()-time_read_table}")
    return result_agg


# 直接读到GPU显存中，不用cpu转到gpu
@ray.remote(num_cpus=1, num_gpus=0.1)
def gpu_only():
    time_start = time.time()
    cudf_df = cudf.read_parquet("/opt/adp/tpch/tpch_10g/lineitem.parquet")
    time_read_table = time.time()
    group_keys = ["l_returnflag", "l_linestatus"]
    operations = {"l_quantity": "sum", "l_extendedprice": "sum"}
    result_agg = cudf_df.groupby(group_keys).agg(operations)
    # result_agg_1 = cudf_df.groupby(group_keys).agg(operations)
    # result_agg_2 = cudf_df.groupby(group_keys).agg(operations)
    print(result_agg)
    print(f"read data time{time_read_table-time_start}")
    print(f"agg use time:{time.time()-time_read_table}")
    print(f"total use time:{time.time()-time_start}")
    return result_agg.to_arrow()


@ray.remote(num_cpus=3, num_gpus=0.1)
def compute_all():
    time_start = time.time()
    data = pq.read_table("/opt/adp/tpch/tpch_10g/lineitem.parquet")
    time_read_table = time.time()
    group_keys = ["l_returnflag", "l_linestatus"]
    agg_elements = [
        AggregateElement("l_quantity", AggregateType.SUM),
        AggregateElement("l_extendedprice", AggregateType.SUM),
    ]
    result_agg = agg_all.remote(data, group_keys, agg_elements)
    print(ray.get(result_agg))
    print(f"read data time{time_read_table-time_start}")
    print(f"agg use time:{time.time()-time_read_table}")
    print(f"total use time:{time.time()-time_start}")
    return result_agg


if __name__ == "__main__":
    ray.init()
    time_start = time.time()
    ray.get(gpu_only.remote())
    time_gpu_only = time.time()
    print(f"gpu only use time:{time_gpu_only-time_start}")
    print("----------------------")
    
    
    time.sleep(3)
    time_start = time.time()
    ray.get(compute_all.remote())
    print("----------------------")
    time_compute_all = time.time()
    print(f"compute_all use time:{time_compute_all-time_start}")
    print("----------------------")
    time.sleep(3)
    time_start = time.time()
    ray.get(compute_column.remote())
    time_compute_column = time.time()
    print(f"compute_column use time:{time_compute_column-time_start}")
    print("----------------------")
    time.sleep(3)
    time_start = time.time()
    ray.get(pyarrow_agg.remote())
    print("----------------------")
    time_pyarrow_agg = time.time()
    print(f"pyarrow_agg use time:{time_pyarrow_agg-time_start}")
    # ray.shutdown()
