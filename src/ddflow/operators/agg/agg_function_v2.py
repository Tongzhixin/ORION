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

from ray.util.queue import Queue


@ray.remote(num_cpus=4, num_gpus=1)
class AggOnGpu:
    def __init__(
        self,
        group_keys,
        agg_elements,
        agg_run_type,
        input_queue: Queue,
        output_queue: Queue,
    ) -> None:
        self.group_keys = group_keys
        self.agg_elements = agg_elements
        self.agg_run_type = agg_run_type
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.time_total = 0
        self.time_convert = 0
        self.name = "agg_gpu"
    def get_name(self):
        return self.name

    def agg_gpu_all(self, origin_table: pa.Table):
        time_start = time.time()
        # print(
        #     f'AGG ALL START Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}'
        # )
        cudf_df = cudf.DataFrame.from_arrow(origin_table)
        # cudf_df = cudf.from_pandas(tbl)
        time_to_df = time.time()
        operations = {}
        for agg_element in self.agg_elements:
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

        result_agg = cudf_df.groupby(self.group_keys).agg(operations)
        # print(f"agg_all from arrow to pandas use:{time_to_df - time_start}")
        # print(f"agg_all agg  use:{time.time() - time_to_df}")
        # print(f"agg_all gpu time use:{time.time() - time_start}")
        # print(
        #     f'AGG ALL END Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}'
        # )
        self.time_total += (time.time() - time_to_df)
        self.time_convert += (time_to_df - time_start)
        self.output_queue.put(result_agg.to_arrow())

    def agg_iml_remote(self, group, group_keys, operations):
        time_agg_iml_start = time.time()
        df_res = group.groupby(group_keys).agg(operations)
        # print(f"agg iml remote time:{time.time()-time_agg_iml_start}")
        return df_res

    def agg_gpu_parallel(self, origin_table: pa.Table):
        # print(
        #     f'AGG ALL START Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}'
        # )
        time_start = time.time()
        new_columns = []
        for agg_element in self.agg_elements:
            new_columns.append(agg_element.aggcol)
        df = cudf.DataFrame.from_arrow(origin_table)
        time_df = time.time()
        # # Separate numerical and string columns
        # numerical_cols = df.select_dtypes(include='number')
        # string_cols = df.select_dtypes(include='object')

        # # Group by specified keys
        # grouped_numerical = numerical_cols.groupby(group_keys)
        grouped_numerical = df.groupby(self.group_keys)

        # Define aggregation operations
        operations = {}
        for agg_element in self.agg_elements:
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

        # @ray.remote(num_cpus=0.1, num_gpus=0.1)

        # Create list of Ray tasks for each group
        agg_tasks = [
            self.agg_iml_remote(group_df, self.group_keys, operations)
            for _, group_df in grouped_numerical
        ]

        # Retrieve results from Ray tasks
        time_ray = time.time()
        # rows = ray.get(agg_tasks)
        rows = agg_tasks

        # Concatenate the results
        final_df_numerical = cudf.concat(rows).reset_index()

        # Combine numerical aggregation results with the string columns
        # combined_df = cudf.concat([final_df_numerical, string_cols], axis=1)

        # Convert final cuDF DataFrame back to Arrow table
        table_output = final_df_numerical.to_arrow()
        # print(f"start to finish arrow to pandas { time_df - time_start} seconds")
        # print(f"Aggregation completed in {time.time() - time_start} seconds")
        # print(f"Aggregation only in {time.time() - time_ray} seconds")
        # print(
        #     f'AGG ALL END Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}'
        # )
        self.output_queue.put(table_output)
        self.time_total += (time.time() - time_ray)
        self.time_convert += (time_df - time_start)

    def agg_pyarrow(self, origin_table: pa.Table):
        # print(
        #     f'AGG ALL START Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}'
        # )
        time_start = time.time()
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
        table_agg = origin_table
        result_agg = table_agg.group_by(self.group_keys).aggregate(operations)
        print(result_agg)
        # print(f"agg pyarrow use time{time.time()-time_start}")
        self.output_queue.put(result_agg)

    def run(self):
        while True:
            input_table = self.input_queue.get()
            print(f"agg input queue size:{self.input_queue.qsize()}")
            if not input_table:
                print("agg actor end")
                self.output_queue.put('')
                del input_table
                print(f"agg time total is {self.time_total}")
                print(f"agg convert time total is {self.time_convert}")
                return
            if self.agg_run_type == "pyarrow":
                self.agg_pyarrow(input_table)
            elif self.agg_run_type == "gpu_parallel":
                self.agg_gpu_parallel(input_table)
            elif self.agg_run_type == "gpu":
                self.agg_gpu_all(input_table)
            # del input_table





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

    agg_actor = AggOnGpu.remote()
    time_start = time.time()
    print(f'MAIN START Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}')
    ray.get(agg_actor.agg_gpu_all.remote(data, group_keys, agg_elements))
    time_end = time.time()
    print(f'MAIN END Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}')
    print(f"Total time: {time_end - time_start}")

    time_start = time.time()
    print(f'MAIN START Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}')
    ray.get(agg_actor.agg_gpu_parallel.remote(data, group_keys, agg_elements))
    time_end = time.time()
    print(f'MAIN END Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}')
    print(f"Total time: {time_end - time_start}")

    time_start = time.time()
    print(f'MAIN START Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}')
    ray.get(agg_actor.agg_pyarrow.remote(data, group_keys, agg_elements))
    time_end = time.time()
    print(f'MAIN END Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}')
    print(f"Total time: {time_end - time_start}")

    # time_start=time.time()
    # print(f'MAIN START Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}')
    # ref = agg_actor.agg_gpu_parallel.remote(data, group_keys, agg_elements)
    # ref_1 = agg_actor.agg_gpu_all.remote(data, group_keys, agg_elements)
    # ray.get([ref,ref_1])

    # time_end = time.time()
    # print(f'MAIN END Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}')
    # print(f"Total time: {time_end - time_start}")
