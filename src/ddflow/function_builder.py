import ray
from typing import Union, List

from ddflow.MemPool import LRUCache
from ddflow.nodes import NodeType, FileSource, LocalSource, NullSource
from ddflow.logic_builder import LogicBuilder
from ddflow.operators.scan.file_function import TableScan
from ddflow.operators.projection.projection_function import Projection
from ddflow.operators.agg.agg_function_v2 import AggOnGpu
from ddflow.operators.sink.buffer_sink import Sink
from ray.util.queue import Queue


# @ray.remote(num_cpus=0.1, num_gpus=0.001)
class FunctionBuilder:
    def __init__(
        self,
        table_scan_actor: TableScan,
        projection_actor: Projection,
        agg_actor,
        sink_actor,
    ):

        self.table_scan_actor = table_scan_actor
        self.projection_actor = projection_actor
        self.agg_actor = agg_actor
        self.sink_actor = sink_actor

    def build(self, logic_plan: LogicBuilder):
        logic_nodes = logic_plan.nodes
        filters = None
        for node in logic_nodes:
            if node.name == NodeType.FILTER.value:
                filters = node.filter_expression
        print(filters)
        refs = []
        last_ref = None
        for node in logic_nodes:
            if node.name == NodeType.SCAN.value:
                if isinstance(node.data_source, LocalSource):
                    print(f"local source")
                    last_ref = self.table_scan_actor.table_scan_local.remote(
                        bucket=node.data_source.bucket,
                        file_path=node.data_source.file_path,
                        columns=node.data_source.columns,
                        filters=filters,
                    )
                    refs.append(last_ref)
                elif isinstance(node.data_source, FileSource):
                    print(f"remote source")
                    last_ref = self.table_scan_actor.table_scan_remote.remote(
                        bucket=node.data_source.bucket,
                        file_path=node.data_source.file_path,
                        columns=node.data_source.columns,
                        filters=filters,
                    )
                    refs.append(last_ref)
            elif node.name == NodeType.PROJECTION.value:
                print(f"projection {node.projection_list}")
                last_ref = self.projection_actor.projection.remote(
                    last_ref, node.projection_list
                )
                refs.append(last_ref)
            elif node.name == NodeType.AGG.value:
                print(f"agg {node.groupby_keys} {node.aggregate_elements}")
                if node.agg_run_type == "gpu":
                    last_ref = self.agg_actor.agg_gpu_all.remote(
                        last_ref, node.groupby_keys, node.aggregate_elements
                    )

                elif node.agg_run_type == "pyarrow":
                    last_ref = self.agg_actor.agg_pyarrow.remote(
                        last_ref, node.groupby_keys, node.aggregate_elements
                    )
                elif node.agg_run_type == "gpu_parallel":
                    last_ref = self.agg_actor.agg_gpu_parallel.remote(
                        last_ref, node.groupby_keys, node.aggregate_elements
                    )
                else:
                    print(f"agg run type {node.agg_run_type} not supported")
                refs.append(last_ref)
            elif node.name == NodeType.SINK.value:
                print(f"sink {node.output.key}")
                last_ref = self.sink_actor.sink.remote(node.output.key, last_ref)
                refs.append(last_ref)
            else:
                print(f"node {node.name} not supported")
        if last_ref:
            print(f"last_ref: {last_ref}")
        return last_ref, refs


def function_run(logic_plan: LogicBuilder, mempool: LRUCache):
    logic_nodes = logic_plan.nodes
    files_list = logic_plan.file_list
    filters = None
    for node in logic_nodes:
        if node.name == NodeType.FILTER.value:
            filters = node.filter_expression
    print(filters)
    actors = []
    last_ref = None
    input_queue = Queue()
    output_queue = Queue(50)
    for node in logic_nodes:
        if node.name == NodeType.SCAN.value:
            table_scan_actor = TableScan.options(
                max_concurrency=4, name="scan"
            ).remote(mempool, input_queue, output_queue)
            print(files_list)
            if isinstance(node.data_source, LocalSource):
                for file in files_list:
                    local_source = LocalSource(
                        bucket=node.data_source.bucket,
                        file_path=file,
                        columns=node.data_source.columns,
                        filters=filters,
                    )
                    input_queue.put(local_source)
            elif isinstance(node.data_source, FileSource):
                for file in files_list:
                    file_source = FileSource(
                        endpoint=node.data_source.endpoint,
                        bucket=node.data_source.bucket,
                        file_path=file,
                        columns=node.data_source.columns,
                        filters=filters,
                    )
                    input_queue.put(file_source)
            else:
                print(f"node {node.name} {node.data_source} not supported")
            input_queue.put(NullSource(""))
            actors.append(table_scan_actor)
        elif node.name == NodeType.PROJECTION.value:
            input_queue = output_queue
            output_queue = Queue()
            projection_actor = Projection.options(
                max_concurrency=12, name="projection"
            ).remote(node.projection_list, input_queue, output_queue)
            actors.append(projection_actor)
        elif node.name == NodeType.AGG.value:
            input_queue = output_queue
            output_queue = Queue()
            agg_actor = AggOnGpu.options(max_concurrency=16, name="agg").remote(
                node.groupby_keys,
                node.aggregate_elements,
                node.agg_run_type,
                input_queue,
                output_queue,
            )
            actors.append(agg_actor)
        elif node.name == NodeType.SINK.value:
            input_queue = output_queue
            sink_actor = Sink.options(max_concurrency=12, name="sink").remote(
                input_queue
            )
            actors.append(sink_actor)
        else:
            print(f"node {node.name} not supported")
    refs = []
    for actor in actors:
        ref = actor.run.remote()
        refs.append(ref)
    while True:
        ready, not_ready = ray.wait(refs)
        if ready:
            for i in ready:
                print(f"actor {i} done")
                refs.remove(i)
        if not_ready:
            continue
        else:
            break
