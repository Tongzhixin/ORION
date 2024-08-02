-- Benchmark Query 15 derived from TPC-H query 15 under the terms of the TPC Fair Use Policy.
-- TPC-H queries are Copyright 1993-2022 Transaction Processing Performance Council.

select
	l_suppkey,
	sum(l_extendedprice * (1 - l_discount))
from
	lineitem
where
	l_shipdate >= date '1996-08-01'
	and l_shipdate < date '1996-08-01' + interval '3' month
group by
	l_suppkey;

