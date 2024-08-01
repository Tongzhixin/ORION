
import s3fs,ray
import pyarrow.parquet as pq
import time

import pyarrow as pa
def test():
    full_path = f"tpch-all/tpch_100g_small/lineitem/"
    s3 = s3fs.S3FileSystem(
        endpoint_url="http://10.2.64.6:9100",
        key="esQWHRxxpOL2oy48CW3K",
        secret="frKcsdRVGNhlDS3jR0JCADkLGj18ews7d3qdaZde",
        use_ssl=False,
        max_concurrency=20,
    )

    # import pyarrow.fs as fs

    # s3 = fs.S3FileSystem(
    #     endpoint_override="http://10.2.64.6:9100",
    #     access_key="esQWHRxxpOL2oy48CW3K",
    #     secret_key="frKcsdRVGNhlDS3jR0JCADkLGj18ews7d3qdaZde",
    #     scheme="http",
    # )
    columns=[
            "l_orderkey",
            "l_quantity",
            "l_extendedprice",
            "l_discount",
            "l_tax",
            "l_returnflag",
            "l_linestatus",
            "l_shipdate",
        ]
    from datetime import datetime, timedelta

    date_reference = datetime.strptime("1998-12-01", "%Y-%m-%d")
    date_threshold = date_reference - timedelta(days=68)
    date_threshold_date = date_threshold.date()
    projectionElement_list = []
    filter_expression = [[("l_shipdate", "<=", date_threshold_date)]]

    table = pq.read_table(
        full_path,
        columns=columns,
        filesystem=s3,
        filters=filter_expression,
        pre_buffer=True
    )
    
    
test()