import ray
from typing import Union, List,Any
from dataclasses import dataclass

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


@dataclass
class Substrait:
    nodes: List[Node]


# @ray.remote
class LogicBuilder:
    def __init__(self, substrait: Substrait = None):
        self.nodes = [] if not substrait else substrait.nodes

    def add_node(self, node):
        self.nodes.append(node)
        return self

    def scan(self, data_source: Union[FileSource, BufferSource]):
        node = ScanNode(data_source=data_source, name=NodeType.SCAN.value)
        self.add_node(node)
        return self

    def filter(self, filter_expression: List[Any]):
        node = FilterNode(filter_expression=filter_expression, name=NodeType.FILTER.value)
        self.add_node(node)
        return self

    def projection(self, projection_list: List[ProjectionElement]):
        node = ProjectionNode(
            projection_list=projection_list, name=NodeType.PROJECTION.value
        )
        self.add_node(node)
        return self

    def agg(self, group_keys: List[str], agg_elements: List[AggregateElement]):
        node = AggregateNode(
            groupby_keys=group_keys,
            aggregate_elements=agg_elements,
            name=NodeType.AGG.value,
        )
        self.add_node(node)
        return self

    def sink(self, output: Union[FileOutput, BufferOutput]):
        node = SinkNode(output=output, name=NodeType.SINK.value)
        self.add_node(node)
        return self

    def build_flow(self):
        print(self.nodes)
        return Substrait(self.nodes)
