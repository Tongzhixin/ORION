import os
import pyarrow.csv as csv
import pyarrow.parquet as pq
import pyarrow as pa
from multiprocessing import Pool

def read_tbl_with_pyarrow(file_path):
    # 使用pyarrow读取CSV文件，指定'|'为分隔符
    table = csv.read_csv(file_path, csv.ReadOptions(column_names=[
        'l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber',
        'l_quantity', 'l_extendedprice', 'l_discount', 'l_tax',
        'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate',
        'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment', 'dummy'
    ]), csv.ParseOptions(delimiter='|'))

    # 删除最后一列（dummy列）
    table = table.drop(['dummy'])
    return table

def convert_file(args):
    file_path, output_subdir = args
    table = read_tbl_with_pyarrow(file_path)
    part_number = os.path.basename(file_path).split('.')[-1]
    output_filename = f"lineitem_part{part_number}.parquet"
    output_file_path = os.path.join(output_subdir, output_filename)
    pq.write_table(table, output_file_path)
    print(f"Converted {file_path} to {output_file_path}")

def convert_to_parquet(input_dir, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    files_to_convert = [
        (os.path.join(input_dir, filename), output_dir)
        for filename in os.listdir(input_dir) if filename.startswith("lineitem.tbl.")
    ]
    with Pool(30) as pool:
        pool.map(convert_file, files_to_convert)

if __name__ == "__main__":
    input_root = '/state/partition/zxtong/tpch_gen/different_scale'
    output_root = '/state/partition/zxtong/tpch_gen/scale'
    # dir_list = ["10", "50", "100", "500", "1000", "5000", "10000"]
    dir_list = ["20","40","60","80","120"]
    for dir in dir_list:
        input_dir = os.path.join(input_root, dir)
        output_dir = os.path.join(output_root, dir)
        convert_to_parquet(input_dir, output_dir)