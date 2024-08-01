import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import itertools
import ray
import select
from typing import List
import time
from ddflow.nodes import (
    AggregateType,
    AggregateElement,
)
import time
import cudf
import pandas as pd
from datetime import datetime

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


@ray.remote(num_gpus=0.1)
def agg_iml(series, operation: AggregateType):
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
    return res_data


@ray.remote(num_cpus=0.5, num_gpus=0.1)
def agg_all(tbl, group_keys, agg_elements):
    time_start = time.time()
    print(f'AGG ALL START Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}')
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
    print(f'AGG ALL END Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}')
    return result_agg.to_arrow()



@ray.remote(num_cpus=1,num_gpus=0.6)
def agg_column(origin_table: pa.Table, group_keys, agg_elements):
    time_start = time.time()
    new_columns = []
    for agg_element in agg_elements:
        new_columns.append(agg_element.aggcol)
    data = origin_table.select(group_keys + new_columns)
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
    time_result = time.time()
    # Convert result to pyarrow.Table
    final_df = []
    for group_name, group_result in results:
        row = {f"{name}": value for name, value in zip(group_keys, group_name)}
        row.update(group_result)
        final_df.append(row)
    table_output = pa.Table.from_pylist(final_df)

    print(f"time to groupby time:{time_groupby-time_start}")
    print(f"agg only time:{time_result-time_groupby}")
    print(f"result to table:{time.time()-time_result}")
    print(f"agg column use time{time.time()-time_start}")
    return table_output


@ray.remote(num_cpus=1,num_gpus=0.6)
def agg_column_pandas(origin_table: pa.Table, group_keys, agg_elements):
    time_start = time.time()
    new_columns = []
    for agg_element in agg_elements:
        new_columns.append(agg_element.aggcol)
    import cudf
    df = cudf.DataFrame.from_arrow(origin_table)
    grouped = df.groupby(group_keys)

    # 存储最终结果
    result = []
    ref_list = []
    # 遍历每个分组
    for group_name, group_df in grouped:
        group_result = {}
        for agg_element in agg_elements:
            ref_cudf = agg_iml.remote(group_df[agg_element.aggcol], agg_element.aggtype)
            ref_list.append(ref_cudf)
            group_result[agg_element.aggcol] = ref_cudf
        result.append((group_name, group_result))

    ray.get(ref_list)
    # 整合结果
    final_result = {}
    for group_name, group_result in result:
        group_result_data = {}
        for agg_element in agg_elements:
            group_result_data[agg_element.aggcol] = ray.get(
                group_result[agg_element.aggcol]
            )
        final_result[group_name] = group_result_data
    print(ref_list)
    # 将结果转换为DataFrame
    final_df = pd.DataFrame.from_dict(
        {k: v for k, v in final_result.items()}, orient="index"
    )
    final_df.reset_index(inplace=True)
    final_df.columns = group_keys + new_columns
    table_output = pa.Table.from_pandas(final_df)
    print(table_output)
    print(f"agg column pandas use time{time.time()-time_start}")
    return table_output


@ray.remote(num_cpus=1,num_gpus=0.6)
def agg_column_pandas_parallel(origin_table: pa.Table, group_keys, agg_elements):
    print(f'AGG ALL START Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}')
    time_start = time.time()
    new_columns = []
    for agg_element in agg_elements:
        new_columns.append(agg_element.aggcol)
    # data = origin_table.select(group_keys + new_columns)
    # Convert Arrow table to cuDF DataFrame
    
    df = cudf.DataFrame.from_arrow(origin_table)
    time_df = time.time()
    # # Separate numerical and string columns
    # numerical_cols = df.select_dtypes(include='number')
    # string_cols = df.select_dtypes(include='object')
    
    # # Group by specified keys
    # grouped_numerical = numerical_cols.groupby(group_keys)
    grouped_numerical = df.groupby(group_keys)
    
    # Define aggregation operations
    operations = {}
    for agg_element in agg_elements:
        if agg_element.aggtype.value == AggregateType.COUNT.value:
            operations[agg_element.aggcol] = "count"
        elif agg_element.aggtype.value == AggregateType.SUM.value:
            operations[agg_element.aggcol] = "sum"
        elif agg_element.aggtype.value == AggregateType.MEAN.value:
            operations[agg_element.aggcol] = "mean"
        elif agg_element.aggtype.value == AggregateType.MIN.value:
            operations[agg_element.aggcol] = "min"
        else:
            raise ValueError(f"Unsupported aggtype: {agg_element.aggtype}")
    
    @ray.remote(num_gpus=0.2)
    def agg_iml_remote(group, group_keys):

        return group.groupby(group_keys).agg(operations)
    
    # Create list of Ray tasks for each group
    agg_tasks = [agg_iml_remote.remote(group_df, group_keys) for _, group_df in grouped_numerical]
    
    # Retrieve results from Ray tasks
    time_ray = time.time()
    rows = ray.get(agg_tasks)
    
    # Concatenate the results
    final_df_numerical = cudf.concat(rows).reset_index()
    
    # Combine numerical aggregation results with the string columns
    # combined_df = cudf.concat([final_df_numerical, string_cols], axis=1)
    
    # Convert final cuDF DataFrame back to Arrow table
    table_output = final_df_numerical.to_arrow()
    print(f"start to finish arrow to pandas { time_df - time_start} seconds")
    print(f"Aggregation completed in {time.time() - time_start} seconds")
    print(f"Aggregation only in {time.time() - time_ray} seconds")
    print(f'AGG ALL END Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}')
    return table_output

@ray.remote(num_cpus=2,num_gpus=0.4)
def agg_column_pandas_parallel_parallel(origin_table: pa.Table, group_keys, agg_elements):
    print(f'AGG ALL START Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}')
    time_start = time.time()
    new_columns = []
    for agg_element in agg_elements:
        new_columns.append(agg_element.aggcol)
    # data = origin_table.select(group_keys + new_columns)
    # time_select = time.time()
    # Convert Arrow table to cuDF DataFrame
    df = cudf.DataFrame.from_arrow(origin_table)

    grouped = df.groupby(group_keys)
    
    # Define aggregation operations
    operations = {}
    for agg_element in agg_elements:
        if agg_element.aggtype.value == AggregateType.COUNT.value:
            operations[agg_element.aggcol] = "count"
        elif agg_element.aggtype.value == AggregateType.SUM.value:
            operations[agg_element.aggcol] = "sum"
        elif agg_element.aggtype.value == AggregateType.MEAN.value:
            operations[agg_element.aggcol] = "mean"
        elif agg_element.aggtype.value == AggregateType.MIN.value:
            operations[agg_element.aggcol] = "min"
        else:
            raise ValueError(f"Unsupported aggtype: {agg_element.aggtype}")
    
    @ray.remote(num_gpus=0.2)
    def agg_iml_parallel(series, operation: AggregateType):
        # cudf_series = cudf.Series(series)
        cudf_series = series
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
        return res_data
    
    
    @ray.remote(num_gpus=0.1)
    def agg_iml_remote(group_name, group_df, agg_elements):
        agg_tasks = [
            agg_iml_parallel.remote(group_df[agg_element.aggcol], agg_element.aggtype)
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
        for group_name, group_df in grouped
    ]

    # Gather results from Ray tasks
    time_ray_task = time.time()
    results = ray.get(agg_tasks)
    time_result = time.time()
    # Convert result to pyarrow.Table
    final_df = []
    for group_name, group_result in results:
        row = {f"{name}": value for name, value in zip(group_keys, group_name)}
        row.update(group_result)
        final_df.append(row)
    table_output = pa.Table.from_pylist(final_df)
    
    # print(f"Aggregation result to table {time_select - time_start} seconds")
    print(f"Aggregation completed in {time.time() - time_start} seconds")
    print(f'agg ray task Time: {time_result - time_ray_task} seconds')
    print(f'AGG ALL END Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}')
    return table_output




@ray.remote
def agg_pyarrow(origin_table: pa.Table, group_keys, agg_elements):
    print(f'AGG ALL START Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}')
    time_start = time.time()
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
    table_agg = origin_table
    result_agg = table_agg.group_by(group_keys).aggregate(operations)
    print(result_agg)
    print(f"agg pyarrow use time{time.time()-time_start}")
    return result_agg


@ray.remote(num_cpus=0.1, num_gpus=0.001)
def agg_gpu(origin_table: pa.Table, group_keys, agg_elements):
    time_start = time.time()
    result_agg = ray.get(agg_all.remote(origin_table, group_keys, agg_elements))
    print(result_agg)
    print(f"agg gpu use time{time.time()-time_start}")
    return result_agg


if __name__ == "__main__":

    # 启动Ray
    # ray.init(ignore_reinit_error=True, num_gpus=2)
    ray.init()
    resources = ray.cluster_resources()
    print(resources)
    # 示例数据
    data = pq.read_table("/opt/adp/tpch/tpch_1g/lineitem.parquet")

    # 创建一个DataFrame
    # df = pd.DataFrame(data)

    # 定义聚合操作
    group_keys = ["l_returnflag", "l_linestatus"]
    agg_elements = [
        AggregateElement("l_quantity", AggregateType.SUM),
        AggregateElement("l_extendedprice", AggregateType.SUM),
    ]

    
    time_start=time.time()
    print(f'MAIN START Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}')
    ray.get(agg_pyarrow.remote(data, group_keys, agg_elements))
    time_end = time.time()
    print(f'MAIN END Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}')
    print(f"Total time: {time_end - time_start}")

