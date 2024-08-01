from dataclasses import dataclass
from enum import Enum
from typing import List, Union,Any


class NodeType(Enum):
    SCAN = "SCAN"
    FILTER = "FILTER"
    AGG = "AGG"
    PROJECTION = "PROJECTION"
    SINK = "SINK"


@dataclass
class Node:
    name: NodeType


# 主要关注s3上面的文件系统
@dataclass
class FileSource:
    endpoint: str
    bucket: str
    file_path: str
    columns: List[str]
@dataclass
class LocalSource:
    bucket: str
    file_path: str
    columns: List[str]


@dataclass
class BufferSource:
    key: str
    columns: List[str]


@dataclass
class ScanNode(Node):
    data_source: Union[FileSource, BufferSource,LocalSource]


@dataclass
class FilterNode(Node):
    filter_expression: List[Any]


@dataclass
class ProjectionElement:
    new_col: str
    projection_expression: str


@dataclass
class ProjectionNode(Node):
    projection_list: List[ProjectionElement]


@dataclass
class AggregateType(Enum):
    COUNT = "COUNT"
    DISTINCT = "DISTINCT"
    COUNT_DISTINCT = "COUNT_DISTINCT"
    SUM = "SUM"
    MEAN = "MEAN"
    MIN = "MIN"
    MAX = "MAX"


@dataclass
class AggregateElement:
    aggcol: str
    aggtype: AggregateType


@dataclass
class AggregateNode(Node):
    groupby_keys: List[str]
    aggregate_elements: List[AggregateElement]


@dataclass
class FileOutput:
    file_path: str


@dataclass
class BufferOutput:
    buffer_name: str
    key: str


@dataclass
class SinkNode(Node):
    output: Union[FileOutput, BufferOutput]
