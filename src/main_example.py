import gc
import json
from typing import List
from ddflow.MemPool import LRUCache, get_cached_files, list_files, remove_columns_field
from ddflow.logic_builder import LogicBuilder
from ddflow.function_builder import FunctionBuilder,function_run

import ray, os, time

from ray.util.queue import Queue

from ddflow.operators.scan.file_function import TableScan
from ddflow.operators.projection.projection_function import Projection
from ddflow.operators.agg.agg_function_v2 import AggOnGpu
from ddflow.operators.sink.buffer_sink import Sink


from ray.util.state import get_actor, get_objects

from ddflow.nodes import (
    BufferSource,
    FileSource,
    LocalSource,
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

os.environ["RAY_record_ref_creation_sites"]="1"
# from adp.src.ddflow.operators.agg.cudf_use import result
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
def run_plans(logic_plan, lru_cache):
    function_run(logic_plan, lru_cache)
    


def build_query1_flow(file_list: List[str]):
    logic_plan = LogicBuilder(file_list)
    data_source = FileSource(
        endpoint="http://10.2.64.6:9100",
        bucket="tpch-all",
        file_path="",
        columns=[
            "l_orderkey",
            "l_quantity",
            "l_extendedprice",
            "l_discount",
            "l_tax",
            "l_returnflag",
            "l_linestatus",
            "l_shipdate",
        ],
    )
    from datetime import datetime, timedelta

    date_reference = datetime.strptime("1998-12-01", "%Y-%m-%d")
    date_threshold = date_reference - timedelta(days=68)
    date_threshold_date = date_threshold.date()
    projectionElement_list = []

    projectionElement_list.append(
        ProjectionElement("disc_price", f"l_extendedprice * (1 - l_discount)")
    )
    # Step 2: Convert to pyarrow DNF format
    filter_expression = [[("l_shipdate", "<=", date_threshold_date)]]

    aggregate_elements = [
        AggregateElement("l_quantity", AggregateType.SUM),
        AggregateElement("disc_price", AggregateType.SUM),
        AggregateElement("l_extendedprice", AggregateType.SUM),
    ]
    logic_plan.scan(data_source).filter(filter_expression).projection(
        projectionElement_list
    ).agg(
        group_keys=["l_returnflag", "l_linestatus"],
        agg_elements=aggregate_elements,
        agg_run_type="gpu",
    ).sink(
        BufferOutput(buffer_name="buffer", key=f"")
    ).build_flow()
    return logic_plan


# @ray.remote(num_gpus=0.001,memory=1*1024*1024*1024)
def query1_flow(lru_cache, bucket, prefix):
    files_list = ray.get(list_files.remote(bucket, prefix))
    exist_files = ray.get(get_cached_files.remote(lru_cache))
    exist_files = remove_columns_field(exist_files)
    set_exist = set(exist_files)
    set_all = set(files_list)
    second_split_file = list(set_all - set_exist)
    exist_files = [prefix + "/" + file for file in exist_files if file]
    second_split_file = [prefix + "/" + file for file in second_split_file if file]
    
    logic_plan_1 = build_query1_flow(exist_files)

    run_plans(logic_plan_1, lru_cache)

    logic_plan_1 = build_query1_flow(second_split_file)

    run_plans(logic_plan_1, lru_cache)


def build_query1_local_flow(file_list: List[str]):
    logic_plan = LogicBuilder(file_list)
    data_source = LocalSource(
        bucket="/opt/adp/tpch",
        file_path="",
        columns=[
            "l_orderkey",
            "l_quantity",
            "l_extendedprice",
            "l_discount",
            "l_tax",
            "l_returnflag",
            "l_linestatus",
            "l_shipdate",
        ],
    )
    from datetime import datetime, timedelta

    date_reference = datetime.strptime("1998-12-01", "%Y-%m-%d")
    date_threshold = date_reference - timedelta(days=68)
    date_threshold_date = date_threshold.date()
    projectionElement_list = []

    projectionElement_list.append(
        ProjectionElement("disc_price", f"l_extendedprice * (1 - l_discount)")
    )
    # Step 2: Convert to pyarrow DNF format
    filter_expression = [[("l_shipdate", "<=", date_threshold_date)]]

    aggregate_elements = [
        AggregateElement("l_quantity", AggregateType.SUM),
        AggregateElement("disc_price", AggregateType.SUM),
        AggregateElement("l_extendedprice", AggregateType.SUM),
    ]
    logic_plan.scan(data_source).filter(filter_expression).projection(
        projectionElement_list
    ).agg(
        group_keys=["l_returnflag", "l_linestatus"],
        agg_elements=aggregate_elements,
        agg_run_type="pyarrow",
    ).sink(
        BufferOutput(buffer_name="buffer", key=f"")
    ).build_flow()
    return logic_plan


# @ray.remote(num_gpus=0.001,memory=1*1024*1024*1024)
def query1_local_flow(lru_cache, bucket, prefix):
    files_list = os.listdir(bucket+'/'+prefix)
    exist_files = ray.get(get_cached_files.remote(lru_cache))
    exist_files = remove_columns_field(exist_files)
    set_exist = set(exist_files)
    set_all = set(files_list)
    second_split_file = list(set_all - set_exist)
    exist_files = [prefix + "/" + file for file in exist_files if file]
    second_split_file = [prefix + "/" + file for file in second_split_file if file]
    
    logic_plan_1 = build_query1_local_flow(exist_files)

    run_plans(logic_plan_1, lru_cache)

    logic_plan_1 = build_query1_local_flow(second_split_file)

    run_plans(logic_plan_1, lru_cache)



def main():
    lru_cache = LRUCache.remote()
    bucket = "tpch-all"
    prefix = "tpch_100g_small/lineitem"
    # query1_flow(lru_cache, bucket, prefix)
    bucket = "/opt/adp/tpch"
    prefix = "tpch_100g_small/lineitem"
    query1_local_flow(lru_cache, bucket, prefix)

if __name__ == "__main__":
    # os.environ["CUDA_DEVICE_ORDER"] = "PCI_BUS_ID"
    # os.environ["CUDA_VISIBLE_DEVICES"] = "0,1"
    time_start = time.time()
    main()
    print(f"time use:{time.time() - time_start}")
    time.sleep(60)