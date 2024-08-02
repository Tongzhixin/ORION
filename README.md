# ORION
This is the prototype system implementation of ORION, a research. We present here the parts of code that have been removed for commercial use for academic exchange. 
Since the system is closely coupled with commercial use, we will work hard to update the code to remove copyright and commercial use, and ensure that this project will continue to add code and continue to exist in the form of open source.

Since this research is under review, we are not in a position to disclose all test data at this time to avoid plagiarism. We will disclose all test data, benchmark data, test process and fullly code including comprehensive manager later.

All subsequent code updates, interface updates, and document updates will be made in this public repository.

### Clone the Repo

```
git clone https://github.com/Tongzhixin/ORION.git
cd ORION
```

## Environment Preparation
- Hardware requirement
    - A cluster with 3 CPU servers or more
    - CPU: X86
    - GPU: Volta architecture or better (Compute Capability >=7.0) 
    - DRAM: 64 GB or more

- Software requirement
    - Ubuntu 20.04.2 LTS with kernel 5.11.0-34-generic
    - Python 3.11 [https://www.python.org/downloads/release/python-3110/](https://www.python.org/downloads/release/python-3110/)
    - Spark-3.3.1 or Presto-0.288 [https://prestodb.io/docs/current/](https://prestodb.io/docs/current/)
    - MinIO OR Amaze S3
    - cuDF 24.04 [https://github.com/rapidsai/cudf](https://github.com/rapidsai/cudf)
    - Ray [https://docs.ray.io/en/latest/](https://docs.ray.io/en/latest/)
    - Pyarrow 16.4 [https://arrow.apache.org/docs/16.1/python/index.html](https://arrow.apache.org/docs/16.1/python/index.html)
    - CUDA 12.1+ NVIDIA driver 520.80.02+ [https://developer.nvidia.com/cuda-toolkit](https://developer.nvidia.com/cuda-toolkit)

## Installation for preparation

- cuDF
  - `pip install --extra-index-url=https://pypi.nvidia.com cudf-cu12` or `conda install -c rapidsai -c conda-forge -c nvidia \
    cudf=24.10 python=3.11 cuda-version=12.5`
- Pyarrow
  - `pip install "pyarrow[all]"`
- Ray
  - `pip install ray==2.31.0`
- MinIO
  - refer to the `ORION\doc\setupMinio.md`
- Spark
  - refer to the `ORION\doc\setupSpark.md`

## Document
Developers can refer to the developer documentation of `doc/doc.md`.


## start the cluster

```
ray start --head --memory=51539607552 --object-store-memory 36474836480
ray start --head --storage="/tmp/local_file"
```

## Getting Started
1. prepare the software environment of `Installation for preparation`
2. Put your data to be analyzed into remote storage, such as Minio or S3. We currently support S3-compatible deployed with MinIO
3. Get the authentication information of your remote storage and fill it in `ORION\src\ddflow\operators\scan\file_function.py`. 

4. Prepare your queries. For example, if you use TPC-H queries, you can refer to the link[https://github.com/ssavvides/tpch-spark](https://github.com/ssavvides/tpch-spark)
5. Split the query parts that need to be pushed down into operators as `ORION\src\ddflow\logic_builder.py`
6. Modify the parameters of each operator in the main function as `ORION\src\main.py` and execute the query

## TODO

The code will be fully presented after the review is completed



