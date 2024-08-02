import json
import ray
import time, os
import pyarrow.parquet as pq
from ray.util.queue import Queue
import pyarrow
from ray.util.actor_pool import ActorPool
from ddflow.nodes import ProjectionElement, DActor
from ddflow.MemPool import LRUCache
from typing import List, Dict
import pyarrow as pa
import ast
import pyarrow.compute as pc
import cudf
from ddflow.nodes import (
    AggregateType,
    AggregateElement,
)

ray.init(
    ignore_reinit_error=True,
    num_cpus=32,
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
        ),
    },
    # _system_config={"automatic_object_spilling_enabled": False}
)


@ray.remote(num_cpus=0.5)
class AggOnGpu(DActor):
    def __init__(
        self, group_keys, agg_elements, agg_run_type, next_actor: DActor
    ) -> None:
        self.group_keys = group_keys
        self.agg_elements = agg_elements
        self.agg_run_type = agg_run_type
        self.time_total = 0
        self.time_convert = 0
        self.name = "agg_gpu"
        self.next_actor = next_actor

    def get_name(self):
        return self.name

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
        # print(result_agg)
        print(f"agg pyarrow use time{time.time()-time_start}")
        return result_agg

    def run(self, input_table: pa.Table):

        if self.agg_run_type == "pyarrow":
            next_table = self.agg_pyarrow(input_table)
        ray.get(self.next_actor.run.remote(next_table))
        del input_table


import socket


@ray.remote(num_cpus=0.1)
class Sink(DActor):
    def __init__(self, server_ip=None, server_port=None, num_threads=10) -> None:
        self.server_ip = server_ip
        self.server_port = server_port
        self.num_threads = num_threads
        self.name = "sink"
        self.next_actor = None

    def get_name(self):
        return self.name

    def send_data_to_server(self, table: pa.Table):
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect((self.server_ip, self.server_port))
        sink_table = pa.BufferOutputStream()
        writer = pa.RecordBatchStreamWriter(sink_table, table.schema)
        writer.write_table(table)
        writer.close()
        buffer = sink_table.getvalue()
        client.sendall(buffer.to_pybytes())

    def run(self, table: pa.Table):
        print(f"table size:{table.nbytes/1024}KB")
        del table


def eval_expr(node: ast.AST, columns: Dict[str, pa.Array]) -> pa.Array:
    """
    Recursively evaluate an AST node and compute the result using pyarrow.compute.
    """
    if isinstance(node, ast.BinOp):
        left = eval_expr(node.left, columns)
        right = eval_expr(node.right, columns)
        op_type = type(node.op)
        ops = {
            ast.Add: pc.add,
            ast.Sub: pc.subtract,
            ast.Mult: pc.multiply,
            ast.Div: pc.divide,
        }
        if op_type in ops:
            return ops[op_type](left, right)
        else:
            raise NotImplementedError(f"Unsupported operation: {op_type}")

    elif isinstance(node, ast.Name):
        return columns[node.id]
    elif isinstance(node, ast.Constant):
        return pa.scalar(node.value)
    else:
        raise NotImplementedError(f"Unsupported AST node: {type(node)}")


@ray.remote(num_cpus=0.15)
class Projection(DActor):
    def __init__(
        self, projection_list: List[ProjectionElement], next_actor: DActor
    ) -> None:
        self.projection_list = projection_list
        self.name = "projection"
        self.next_actor = next_actor

    def get_name(self):
        return self.name

    def projection(self, origin_table: pa.Table):
        time_start = time.time()
        columns = {name: origin_table[name] for name in origin_table.schema.names}
        new_columns = {}
        for element in self.projection_list:
            expr = ast.parse(element.projection_expression, mode="eval").body
            new_column = eval_expr(expr, columns)
            new_columns[element.new_col] = new_column

        for new_col, new_data in new_columns.items():
            origin_table = origin_table.append_column(new_col, new_data)
        print(f"projection use time{time.time()-time_start}")
        return origin_table

    def run(self, origin_table):
        next_table = self.projection(origin_table)
        ray.get(self.next_actor.run.remote(next_table))
        del origin_table


# lru_cache = LRUCache.remote()
@ray.remote(num_cpus=0.4)
def table_scan(bucket, file_path, columns):
    time_start = time.time()
    # table = ray.get(self.mempool.get.remote(data_key))
    full_path = f"{bucket}/{file_path}"
    columns = [
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
    # print(f"table_scan file{file_path},time: {time.time() - time_start}")
    return (table, file_path)


@ray.remote(num_cpus=0.1)
def table_scan_remote(bucket, file_path, columns):
    time_start = time.time()
    # table = ray.get(self.mempool.get.remote(data_key))

    full_path = f"{bucket}/{file_path}"

    columns = [
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
        endpoint_override="http://10.2.64.6:9100",
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
    # print(f"table_scan file{file_path},time: {time.time() - time_start}")
    return (table, file_path)


@ray.remote(num_cpus=0.1)
def process_table(table):
    if not table:
        return
    # print(f"Processed table path: {table[1]} with {table[0].num_rows} rows")
    time.sleep(3)
    file_path = str(table[1])
    del table  # Explicitly delete the table object to free up memory
    return file_path

from datafusion import RuntimeConfig, SessionConfig, SessionContext, col, lit
from datafusion import functions as f

import pyarrow.parquet as pq
import pyarrow as pa
@ray.remote(num_cpus=1)
class workflow_optimize:
    def __init__(self, file_path, source_table_name, arrow_table,time_read) -> None:
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
            .with_target_partitions(2)
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
        self.time_usage.append(time.time() - time_start)
        self.size_usage.append(
            self.ctx.table("stage2").to_arrow_table().nbytes / 1024 / 1024
        )
        line_str = ""
        for time_usage, size_usage in zip(self.time_usage, self.size_usage):
            line_str += f"time,{time_usage},size,{size_usage},"
        print(line_str)


def main():
    bucket = "/opt/adp/tpch"
    prefix = "tpch_100g_small/lineitem"
    files_list = os.listdir(bucket + "/" + prefix)
    exist_files = [prefix + "/" + file for file in files_list if file]
    # bucket = "tpch-all"

    ref_list = []
    max_cpu = 40
    time_start = time.time()

    for file_name in exist_files:
        if len(ref_list) > max_cpu:
            ready_refs, ref_list = ray.wait(ref_list, num_returns=1)
            for t in ready_refs:
                ray.get(t)
                del t

        table_ref = table_scan.remote(
            bucket, file_name, columns=None
        )
        next_ref = process_table.remote(table_ref)
        ref_list.append(table_ref)
        ref_list.append(next_ref)


    # final
    print(len(ref_list))
    while True:
        ready_refs, ref_list = ray.wait(ref_list, num_returns=1)
        for t in ready_refs:
            res_path = ray.get(t)
            del t
            del res_path
        if not ref_list:
            break

    print(f"total time: {time.time() - time_start}")
    time.sleep(60)


if __name__ == "__main__":
    main()
