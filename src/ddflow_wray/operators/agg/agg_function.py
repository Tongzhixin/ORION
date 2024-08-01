import pyarrow as pa
import pyarrow.parquet as pq
import re
import s3fs
import ray
import asyncio
from collections import defaultdict, OrderedDict
from ray.util.state import get_actor, get_objects
from typing import List
import time
from ddflow.nodes import (
    BufferSource,
    FileSource,
    FilterNode,
    Node,
    NodeType,
    ScanNode,
    FilterNode,
    ProjectionNode,
    ProjectionElement,
    AggregateType,
    AggregateElement,
    AggregateNode,
    FileOutput,
    BufferOutput,
    SinkNode,
)

import time
import pyarrow
import pandas as pd
import cudf

@ray.remote(num_gpus=1)
def agg_iml(series, operation: AggregateType):
    # import os

    # os.environ["CUDA_DEVICE_ORDER"] = "PCI_BUS_ID"
    # os.environ["CUDA_VISIBLE_DEVICES"] = "0,1"
    # from numba import cuda

    # cuda.detect()
    import cudf

    # 将pandas Series转换为cudf Series
    cudf_series = cudf.Series(series)
    # 选择对应的聚合操作
    if operation.value == AggregateType.SUM.value:
        return cudf_series.sum()
    elif operation.value == AggregateType.MEAN.value:
        return cudf_series.mean()
    elif operation.value == AggregateType.COUNT.value:
        return cudf_series.count()
    else:
        raise ValueError(f"Unsupported operation: {operation}")
    
    
@ray.remote(num_gpus=0.2)
def agg_all(tbl, group_keys, agg_elements):
    
    cudf_df = cudf.DataFrame.from_arrow(tbl)
    operations = {}
    for agg_element in agg_elements:
        if agg_element.aggtype.value == AggregateType.COUNT.value:
            operations[agg_element.aggcol]="count"
            # operations.append((agg_element.aggcol, "count"))
        elif agg_element.aggtype.value == AggregateType.SUM.value:
            operations[agg_element.aggcol]="sum"
        elif agg_element.aggtype.value == AggregateType.MEAN.value:
            operations[agg_element.aggcol]="mean"
        elif agg_element.aggtype.value == AggregateType.MIN.value:
            operations[agg_element.aggcol]="min"
        else:
            print(f"error aggtype is {agg_element.aggtype}")
    result_agg = cudf_df.groupby(group_keys).agg(operations)
    return result_agg.to_arrow()


@ray.remote(num_gpus=0.1)
class AggregateFunction:
    def __init__(
        self,
        batch_ref: pyarrow.Table,
        group_keys: List[str],
        agg_elements: List[AggregateElement],
    ) -> None:
        # init the batch_ref group_keys and agg_elements
        self.batch_ref = batch_ref
        self.group_keys = group_keys
        self.agg_elements = agg_elements
        self.data_key = self.group_keys + self.agg_elements

    def cudf_agg(self, df):
        new_columns = []
        for agg_element in self.agg_elements:
            new_columns.append(agg_element.aggcol)

        grouped = df.groupby(self.group_keys)

        # 存储最终结果
        result = []

        # 遍历每个分组
        for group_name, group_df in grouped:
            group_result = {}
            for agg_element in self.agg_elements:
                group_result[agg_element.aggcol] = agg_iml.remote(
                    group_df[agg_element.aggcol], agg_element.aggtype
                )
            result.append((group_name, group_result))

        # 整合结果
        final_result = {}
        for group_name, group_result in result:
            group_result_data = {}
            for agg_element in self.agg_elements:
                group_result_data[agg_element.aggcol] = ray.get(
                    group_result[agg_element.aggcol]
                )
            final_result[group_name] = group_result_data

        # 将结果转换为DataFrame
        final_df = pd.DataFrame.from_dict(
            {k: v for k, v in final_result.items()}, orient="index"
        )
        final_df.reset_index(inplace=True)
        final_df.columns = self.group_keys + new_columns
        return final_df

    def common_agg(self, table):
        operations = []
        for agg_element in self.agg_elements:
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
        table_agg = table
        result_agg = table_agg.group_by(self.group_keys).aggregate(operations)

        return result_agg

    def exec(self):
        # given the groupbys and agg-elements, refactor the exec based on the cudf_agg, and return the result
        if isinstance(self.batch_ref, pa.Table):
            batch_table = self.batch_ref
        else:
            batch_table: pyarrow.Table = ray.get(self.batch_ref)  # self.batch_ref
        # batch_table: pyarrow.Table = self.batch_ref
        result_df = self.cudf_agg(batch_table.to_pandas())
        # result_df = self.common_agg(batch_table)
        return result_df


# @ray.remote(num_gpus=0.6)
# def agg_groupby(origin_table: pa.Table, group_keys, agg_elements):
#     time_start = time.time()
#     new_columns = []
#     for agg_element in agg_elements:
#         new_columns.append(agg_element.aggcol)
#     import cudf
#     df = cudf.DataFrame.from_arrow(origin_table)
#     grouped = df.groupby(group_keys)

#     # 存储最终结果
#     result = []
#     ref_list = []
#     # 遍历每个分组
#     for group_name, group_df in grouped:
#         group_result = {}
#         for agg_element in agg_elements:
#             ref_cudf = agg_iml.remote(
#                 group_df[agg_element.aggcol], agg_element.aggtype
#             )
#             ref_list.append(ref_cudf)
#             group_result[agg_element.aggcol]=ref_cudf
#         result.append((group_name, group_result))

#     ray.get(ref_list)
#     # 整合结果
#     final_result = {}
#     for group_name, group_result in result:
#         group_result_data = {}
#         for agg_element in agg_elements:
#             group_result_data[agg_element.aggcol] = ray.get(
#                 group_result[agg_element.aggcol]
#             )
#         final_result[group_name] = group_result_data
#     print(ref_list)
#     # 将结果转换为DataFrame
#     final_df = pd.DataFrame.from_dict(
#         {k: v for k, v in final_result.items()}, orient="index"
#     )
#     final_df.reset_index(inplace=True)
#     final_df.columns = group_keys + new_columns
#     table_output = pa.Table.from_pandas(final_df)
#     print(table_output)
#     print(f"agg use time{time.time()-time_start}")
#     return table_output


# @ray.remote
# def agg_groupby(origin_table: pa.Table, group_keys, agg_elements):
#     time_start = time.time()
#     operations = []
#     for agg_element in agg_elements:
#         if agg_element.aggtype.value == AggregateType.COUNT.value:
#             operations.append((agg_element.aggcol, "count"))
#         elif agg_element.aggtype.value == AggregateType.SUM.value:
#             operations.append((agg_element.aggcol, "sum"))
#         elif agg_element.aggtype.value == AggregateType.MEAN.value:
#             operations.append((agg_element.aggcol, "mean"))
#         elif agg_element.aggtype.value == AggregateType.MIN.value:
#             operations.append((agg_element.aggcol, "min"))
#         else:
#             print(f"error aggtype is {agg_element.aggtype}")
#     table_agg = origin_table
#     result_agg = table_agg.group_by(group_keys).aggregate(operations)
#     print(result_agg)
#     print(f"agg use time{time.time()-time_start}")
#     return result_agg


@ray.remote(num_cpus=0.1,num_gpus=0.1)
def agg_groupby(origin_table: pa.Table, group_keys, agg_elements):
    time_start = time.time()
    result_agg = ray.get(agg_all.remote(origin_table, group_keys, agg_elements))
    print(result_agg)
    print(f"agg use time{time.time()-time_start}")
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
    group_keys=["l_returnflag", "l_linestatus"]
    agg_elements = [AggregateElement("l_quantity", AggregateType.SUM), AggregateElement("l_extendedprice", AggregateType.SUM)]

    ray.get(agg_groupby.remote(data, group_keys, agg_elements))
    
    # # 将DataFrame转换为pyarrow表
    # batch_table = pyarrow.Table.from_pandas(df)

    # # 将表格放到Ray中
    # batch_ref = ray.put(batch_table)
    # # 转换agg_elements到AggregateElement的实例列表
    # agg_elements_instances = [AggregateElement(col, op) for col, op in agg_elements]

    # # 初始化AggregateFunction类
    # agg_func = AggregateFunction.remote(batch_ref, group_keys, agg_elements_instances)

    # # 执行聚合操作并获取结果
    # result_df = ray.get(agg_func.exec.remote())

    # # 打印结果
    # print(result_df)
