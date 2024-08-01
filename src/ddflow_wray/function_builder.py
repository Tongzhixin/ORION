import ray
from typing import Union, List
from dataclasses import dataclass

from ddflow.MemPool import LRUCache
from ddflow.nodes import NodeType, FileSource,LocalSource
from ddflow.logic_builder import LogicBuilder
from ddflow.operators.scan.file_function import FileSourceFunction,LocalSourceFunction,table_scan_local
from ddflow.operators.projection.projection_function import projection
from ddflow.operators.agg.agg_function import AggregateFunction,agg_groupby
from ddflow.operators.sink.buffer_sink import BufferOutputFunction,sink


@ray.remote(num_gpus=0.1)
class FunctionBuilder:
    def __init__(self, logic_plan: LogicBuilder, mempool: LRUCache, output_pool):
        self.logic_plan = logic_plan
        self.nodes = self.logic_plan.nodes
        self.mempool = mempool
        self.output_pool = output_pool

    def build(self):
        filters = None
        for node in self.nodes:
            if node.name == NodeType.FILTER.value:
                filters = node.filter_expression
        print(filters)
        last_ref = None
        for node in self.nodes:
            if node.name == NodeType.SCAN.value:
                if isinstance(node.data_source,LocalSource):
                    print(f"local source")
                    last_ref = table_scan_local.remote(bucket=node.data_source.bucket,
                        file_path=node.data_source.file_path,
                        mempool=self.mempool,
                        columns=node.data_source.columns,
                        filters=filters)
                    # ray.get(last_ref)
                    
                # elif isinstance(node.data_source,FileSource):
                #     actor = FileSourceFunction.bind(
                #         bucket=node.data_source.bucket,
                #         file_path=node.data_source.file_path,
                #         mempool=self.mempool,
                #         columns=node.data_source.columns,
                #         filters=filters,
                #     )
                #     last_ref = actor
            elif node.name == NodeType.PROJECTION.value:
                print(f"projection {node.projection_list}")
                last_ref = projection.remote(last_ref, node.projection_list)
            elif node.name == NodeType.AGG.value:
                print(f"agg {node.groupby_keys} {node.aggregate_elements}")
                last_ref = agg_groupby.remote(last_ref, node.groupby_keys, node.aggregate_elements)
            elif node.name == NodeType.SINK.value:
                print(f"sink {node.output.key}")
                last_ref = sink.remote(self.output_pool, node.output.key, last_ref)
            else:
                print(f"node {node.name} not supported")
        if last_ref:
            print(f"last_ref: {last_ref}")
        return ray.get(last_ref)
