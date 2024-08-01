import pyarrow as pa
import pandas as pd
import cudf

# 自定义的agg_iml函数
def agg_iml(series, operation):
    # 将pyarrow Array转换为cudf Series
    cudf_series = cudf.Series(series)
    # 选择对应的聚合操作
    if operation == 'sum':
        return cudf_series.sum()
    elif operation == 'mean':
        return cudf_series.mean()
    elif operation == 'count':
        return cudf_series.count()
    else:
        raise ValueError(f"Unsupported operation: {operation}")

# 示例数据
data = {
    "l_returnflag": ['A', 'B', 'A', 'B', 'A', 'B'],
    "l_linestatus": ['O', 'O', 'F', 'F', 'O', 'F'],
    "l_quantity": [1, 2, 3, 4, 5, 6],
    "l_extendedprice": [10, 20, 30, 40, 50, 60],
    "disc_price": [9, 18, 27, 36, 45, 54],
    "charge": [8, 16, 24, 32, 40, 48],
    "avg_qty": [1, 2, 3, 4, 5, 6],
    "avg_price": [10, 20, 30, 40, 50, 60],
    "l_discount": [0.1, 0.2, 0.3, 0.4, 0.5, 0.6],
    "l_orderkey": [1, 2, 3, 4, 5, 6]
}

df = pd.DataFrame(data)
table = pa.Table.from_pandas(df)

# 定义需要进行的聚合操作
operations = {
    "l_quantity": "sum",
    "l_extendedprice": "sum",
    "disc_price": "sum",
    "charge": "sum",
    "avg_qty": "mean",
    "avg_price": "mean",
    "l_discount": "mean",
    "l_orderkey": "count",
}

# 将table转换为pandas DataFrame以便进行groupby操作
df = table.to_pandas()

# 进行groupby操作
grouped = df.groupby(["l_returnflag", "l_linestatus"])

# 存储最终结果
result = []

# 遍历每个分组
for group_name, group_df in grouped:
    group_result = {}
    for column, operation in operations.items():
        group_result[column] = agg_iml(group_df[column].values, operation)
    result.append((group_name, group_result))

# 整合结果
final_result = {}
for group_name, group_result in result:
    final_result[group_name] = group_result

# 将结果转换为pyarrow Table
final_df = pd.DataFrame.from_dict({k: v for k, v in final_result.items()}, orient='index')
final_df.reset_index(inplace=True)
final_df.columns = ["l_returnflag", "l_linestatus"] + list(operations.keys())
final_table = pa.Table.from_pandas(final_df)

print(final_table)
