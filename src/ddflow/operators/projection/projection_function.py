import pyarrow as pa
import ray
from typing import List
import time
from ddflow.nodes import ProjectionElement

import time
import ray
import pyarrow as pa
import pyarrow.compute as pc
import ast
from typing import List, Dict
from ray.util.queue import Queue


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


@ray.remote(num_cpus=6)
class Projection:
    def __init__(
        self,
        projection_list: List[ProjectionElement],
        input_queue: Queue,
        output_queue: Queue,
    ) -> None:
        self.projection_list = projection_list
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.name = "projection"
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
        self.output_queue.put(origin_table)

    def run(self):
        while True:
            input_table = self.input_queue.get()
            print(f"projection input queue size:{self.input_queue.qsize()}")
            if not input_table:
                print("projection actor end")
                self.output_queue.put('')
                del input_table
                return
            self.projection(input_table)
            # del input_table


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
    projection = Projection.remote()
    result = ray.get(projection.projection.remote(batch_ref, projection_elements))

    print(result)
