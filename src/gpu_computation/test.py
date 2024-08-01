import pyarrow as pa
import pyarrow.compute as pc

# 假设我们有一个示例的Arrow Table
data = {
    'column1': ['A', 'B', 'A', 'C', 'B'],
    'column2': [1, 2, 1, 3, 2],
    'column3': ['X', 'Y', 'X', 'Z', 'Y']
}
table = pa.table(data)

# 选择指定的列
selected_columns = table.select(['column1', 'column2', 'column3'])

# 将选择的列转换为数组
arrays = [selected_columns.column(i).combine_chunks() for i in range(selected_columns.num_columns)]

# 将数组转换为记录数组
record_array = pa.RecordBatch.from_arrays(arrays, names=selected_columns.column_names)

# 对记录数组进行去重
unique_record_array = pc.unique(record_array)

# 将去重后的记录数组转换为表
unique_table = pa.Table.from_batches([unique_record_array])

# 根据'column1'进行hash partition，分为两组
partitioned_tables = unique_table.partition_by_hash(['column1'], 2)

# 打印结果
for i, part in enumerate(partitioned_tables):
    print(f"Partition {i}:")
    print(part)