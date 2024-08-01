import pyarrow as pa
import pyarrow.compute as pc

# 假设我们有以下的PyArrow Table
data = {
    'A': ['foo', 'bar', 'foo', 'bar', 'foo', 'bar', 'foo', 'foo'],
    'B': ['one', 'one', 'two', 'three', 'two', 'two', 'one', 'three'],
    'C': [1, 2, 3, 4, 5, 6, 7, 8],
    'D': [9, 10, 11, 12, 13, 14, 15, 16]
}
table = pa.Table.from_pydict(data)

# 定义分组的字段
group_columns = ['A', 'B']

# 获取唯一的组合
unique_keys = [pc.unique(table[column]) for column in group_columns]

# 生成所有可能的组合键
import itertools
unique_combinations = list(itertools.product(*(key.to_pylist() for key in unique_keys)))

# 用于存放分组结果的字典
grouped_tables = {}

# 对每个唯一的组合键进行过滤
for combination in unique_combinations:
    # 构建过滤条件
    filters = [pc.equal(table[col], val) for col, val in zip(group_columns, combination)]
    if len(filters) > 1:
        combined_filter = pc.and_(*filters)
    else:
        combined_filter = filters[0]
    
    # 应用过滤条件
    filtered_table = table.filter(combined_filter)
    
    # 如果过滤后的表不为空，则保存
    if filtered_table.num_rows > 0:
        grouped_tables[combination] = filtered_table

# 现在 grouped_tables 包含了按照组合键分组的 PyArrow Table

# 打印分组的结果
for group_key, group_table in grouped_tables.items():
    print(f"Group: {group_key}")
    print(group_table)
