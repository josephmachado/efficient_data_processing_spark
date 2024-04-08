USE tpch;

DROP TABLE IF EXISTS lineitem_w_sorting;

CREATE TABLE lineitem_w_sorting(
    orderkey bigint,
    partkey bigint,
    suppkey bigint,
    linenumber integer,
    quantity double,
    extendedprice double,
    discount double,
    tax double,
    shipinstruct varchar(25),
    shipmode varchar(10),
    COMMENT varchar(44),
    commitdate date,
    linestatus varchar(1),
    returnflag varchar(1),
    shipdate date,
    receiptdate date
) USING parquet 
CLUSTERED BY (quantity) 
SORTED BY (quantity ASC) 
INTO 5 BUCKETS
LOCATION 's3a://tpch/lineitem_w_sorting/';

INSERT INTO lineitem_w_sorting
SELECT
    orderkey,
    partkey,
    suppkey,
    linenumber,
    quantity,
    extendedprice,
    discount,
    tax,
    shipinstruct,
    shipmode,
    COMMENT,
    commitdate,
    linestatus,
    returnflag,
    shipdate,
    receiptdate
FROM
    tpch.lineitem
ORDER BY quantity ASC NULLS FIRST;

-- Do the same join as SortMergeJoin to see if exchange of lineitem table is skipped 
ANALYZE TABLE lineitem_w_sorting COMPUTE STATISTICS FOR ALL COLUMNS; -- to ensure that our metadata is computed for quantity column

EXPLAIN 
SELECT quantity
, count(1) as cnt
FROM lineitem_w_sorting
GROUP BY quantity;
-- run time without explain
-- Time taken: 1.705 seconds, Fetched 50 row(s)

EXPLAIN 
SELECT quantity
, count(1) as cnt
FROM tpch.lineitem
GROUP BY quantity;
-- run time without explain
-- Time taken: 4.108 seconds, Fetched 50 row(s)
