# ORION
This is the prototype system implementation of ORION, a research, we present here the parts that have been removed for commercial use for academic exchange. 
Since the system is closely coupled with commercial use, we will work hard to update the code to remove copyright and commercial use, and ensure that this project will continue to add code and continue to exist in the form of open source.

### Clone the Repo

```
git clone https://github.com/Tongzhixin/ORION.git
cd ORION
```

## Environment Preparation
- Hardware requirement
    - A cluster with 3 CPU servers or more
    - CPU: Intel(R) Xeon(R) CPU E5-2630 v4 @ 2.20GHz
    - DRAM: 64 GB or more

- Software requirement
    - Ubuntu 20.04.2 LTS with kernel 5.11.0-34-generic
    - Python 3.11
    - Spark-3.3.1
    - Presto-latest
    - MinIO 
    - cuDF 24.04[https://github.com/rapidsai/cudf](https://github.com/rapidsai/cudf)
    - Ray [https://docs.ray.io/en/latest/](https://docs.ray.io/en/latest/)
    - Pyarrow 16.4
    - CUDA 12.1+ NVIDIA driver 520.80.02+ Volta architecture or better (Compute Capability >=7.0)

## Installation for preparation

- cuDF
- Pyarrow
- Ray
- MinIO
- Spark

## Document
Developers can refer to the developer documentation of `doc/doc.md`.

## Getting Started
1. prepare the software environment of `Installation for preparation`
2. Put your data to be analyzed into remote storage, such as Minio or S3. We currently support S3-compatible deployed with MinIO
3. Get the authentication information of your remote storage and fill it in `ORION\src\ddflow\operators\scan\file_function.py`. 

4. Prepare your queries. For example, if you use TPC-H queries, you can refer to the link[https://github.com/ssavvides/tpch-spark](https://github.com/ssavvides/tpch-spark)
5. Split the query parts that need to be pushed down into operators as `ORION\src\ddflow\logic_builder.py`
6. Modify the parameters of each operator in the main function as `ORION\src\main.py` and execute the query

## TODO



