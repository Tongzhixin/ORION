import cudf
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
import ray
import asyncio
from collections import defaultdict, OrderedDict
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

import pyarrow,time
import modin.pandas as pd
import ray
import pyarrow as pa
import pyarrow.compute as pc
import ast
import operator
from typing import List, Dict, Any


@ray.remote
class ProjectionFunction:
    def __init__(
        self,
        batch_ref,
        projection_list: List[ProjectionElement],
    ) -> None:
        self.batch_ref = batch_ref
        self.projection_list = projection_list

    def eval_expr(self, node: ast.AST, columns: Dict[str, pa.Array]) -> pa.Array:
        """
        Recursively evaluate an AST node and compute the result using pyarrow.compute.
        """
        if isinstance(node, ast.BinOp):
            left = self.eval_expr(node.left, columns)
            right = self.eval_expr(node.right, columns)
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

    def projection(self, origin_table: pa.Table) -> pa.Table:
        columns = {name: origin_table[name] for name in origin_table.schema.names}
        new_columns = {}
        for element in self.projection_list:
            expr = ast.parse(element.projection_expression, mode="eval").body
            new_column = self.eval_expr(expr, columns)
            new_columns[element.new_col] = new_column

        for new_col, new_data in new_columns.items():
            origin_table = origin_table.append_column(new_col, new_data)

        return origin_table

    def exec(self):
        print(self.batch_ref)
        batch_table = self.batch_ref
        # if isinstance(self.batch_ref, pa.Table):
        #     batch_table = self.batch_ref
        # else:
        #     batch_table:pyarrow.Table = ray.get(self.batch_ref) # self.batch_ref
        print(batch_table)
        result_table = self.projection(batch_table)
        print(result_table.columns)
        return result_table


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


@ray.remote(num_gpus=0.1)
def projection(origin_table:pa.Table, projection_list: List[ProjectionElement]):
    # if not isinstance(origin_table,pa.Table):
    #     print(origin_table)
    #     origin_table = ray.get(origin_table)
    time_start = time.time()
    columns = {name: origin_table[name] for name in origin_table.schema.names}
    new_columns = {}
    for element in projection_list:
        expr = ast.parse(element.projection_expression, mode="eval").body
        new_column = eval_expr(expr, columns)
        new_columns[element.new_col] = new_column

    for new_col, new_data in new_columns.items():
        origin_table = origin_table.append_column(new_col, new_data)
    print(f"projection use time{time.time()-time_start}")
    return origin_table


if __name__ == "__main__":

    ray.init()

    # Mock data
    data = {
        "a": pa.array([1, 2, 3]),
        "b": pa.array([4, 5, 6]),
        "c": pa.array([7, 8, 9]),
    }
    origin_table = pa.table(data)
    print(origin_table)
    # Projection elements
    projection_elements = [
        ProjectionElement(new_col="result", projection_expression=f"(a + b) * c")
    ]

    batch_ref = ray.put(origin_table)
    projection_func = ProjectionFunction.remote(batch_ref, projection_elements)
    result = ray.get(projection_func.exec.remote())
    print(result)
