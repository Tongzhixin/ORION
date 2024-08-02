import time
import cudf
import ray
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc

# 初始化 Ray
ray.init(ignore_reinit_error=True)


# 定义每个 GPU 的常驻汇总任务
@ray.remote(num_gpus=0.6)
class Aggregator:
    def __init__(self):
        self.data = cudf.DataFrame(columns=["l_partkey", "l_orderkey", "l_suppkey"])

    def add_data(self, df):
        # print(f"add data size:{df.memory_usage(deep=True)/1024/1024}MB")
        self.data = cudf.concat([self.data, df])
        self.distinct()
        print(f"data rows:{self.data.shape[0]}")

    def distinct(self):
        # TODO
        # groupby l_partkey and distinct l_orderkey and l_suppkey,not use sum(count(distinct l_orderkey))
        self.data.drop_duplicates(
            subset=["l_partkey", "l_orderkey", "l_suppkey"], keep="first", inplace=True
        )

    def aggregate(self):
        self.data = (
            self.data.groupby("l_partkey")
            .agg(
                {
                    "l_orderkey": "nunique",
                    "l_suppkey": "nunique",
                }
            )
            .rename(
                columns={
                    "l_orderkey": "unique_orders",
                    "l_suppkey": "unique_suppliers",
                }
            )
            .reset_index()
        )

    def to_arrow(self):
        self.aggregate()
        return self.data.to_arrow()


# 初始化 Aggregator 实例
aggregators = [Aggregator.remote() for _ in range(2)]


# 定义处理任务
@ray.remote(num_cpus=0.5,num_gpus=0.01)
def process_and_distribute(file_chunk, aggregators):
    time_start = time.time()

    df = cudf.read_parquet(file_chunk, columns=["l_partkey", "l_orderkey", "l_suppkey"])
    time_todf = time.time()
    print(f"gpu read file use time:{time_todf-time_start}")
    
    # 执行第一个 group by 和 count distinct 操作
    # result = df.groupby("l_partkey").agg({
    #     "l_orderkey": "nunique",
    #     "l_suppkey": "nunique",
    # }).rename(columns={
    #     "l_orderkey": "unique_orders",
    #     "l_suppkey": "unique_suppliers",
    # }).reset_index()

    df.drop_duplicates(
        subset=["l_partkey", "l_orderkey", "l_suppkey"], keep="first", inplace=True
    )

    # 哈希分区并分配到不同的 GPU 上
    num_gpus = len(aggregators)
    partitions = df.partition_by_hash(["l_partkey"], num_gpus)

    # 发送分区数据到相应的 Aggregator
    ray.get([aggregators[i].add_data.remote(partitions[i]) for i in range(num_gpus)])
    print(f"cudf use time:{time.time()-time_start}")

@ray.remote(num_cpus=0.5,num_gpus=0.01)
def process_and_distribute_pyarrow(file_chunk, aggregators):
    time_start = time.time()
    table = pq.read_table(file_chunk, columns=["l_partkey", "l_orderkey", "l_suppkey"])
    df = cudf.DataFrame.from_arrow(table)
    time_todf = time.time()
    print(f"pyarrow use time:{time_todf-time_start}")
    df.drop_duplicates(
        subset=["l_partkey", "l_orderkey", "l_suppkey"], keep="first", inplace=True
    )

    # 哈希分区并分配到不同的 GPU 上
    num_gpus = len(aggregators)
    partitions = df.partition_by_hash(["l_partkey"], num_gpus)

    # 发送分区数据到相应的 Aggregator
    ray.get([aggregators[i].add_data.remote(partitions[i]) for i in range(num_gpus)])
    print(f"process total use time:{time.time()-time_start}")



# 获取所有 partition 文件（假设存放在某个文件夹下）
bucket = "/opt/adp/tpch"
prefix = "tpch_100g_small/lineitem"
import os

files_list = os.listdir(bucket + "/" + prefix)
exist_files = [bucket + "/" + prefix + "/" + file for file in files_list if file]
partition_files = exist_files[:100]  # 需要实际文件路径
# chunk_size = 10 * 1024 * 1024 * 1024  # 每次处理 10GB 的数据
# chunk_size = 15
# 按块读取和处理数据
# partition_files=['/opt/adp/tpch/tpch_10g/lineitem.parquet']
print(len(partition_files))
# for i in range(0, len(partition_files), chunk_size):
#     chunk_files = partition_files[i:i + chunk_size]
#     print(chunk_files)
#     ray.get(process_and_distribute.remote(chunk_files, aggregators))

ref_list = []
max_cpu = 40
time_start = time.time()

for file_name in partition_files:
    if len(ref_list) > max_cpu:
        ready_refs, ref_list = ray.wait(ref_list, num_returns=1)
        for t in ready_refs:
            ray.get(t)

    ref = process_and_distribute_pyarrow.remote(file_name, aggregators)
    ref_list.append(ref)


# final
print(len(ref_list))
while True:
    ready_refs, ref_list = ray.wait(ref_list, num_returns=1)
    for t in ready_refs:
        ray.get(t)
    if not ref_list:
        break

# 最终在每个 GPU 上进行汇总
final_results = ray.get([aggregator.to_arrow.remote() for aggregator in aggregators])
for result in final_results:
    print(f"result rows:{result.num_rows}")
# print(final_results)
# 将汇总结果转换为 Arrow 格式并发送到 CPU
final_result = pa.concat_tables([result for result in final_results])

print(f"final_result rows:{final_result.num_rows}")
print(f"time cost:{time.time()-time_start}")
