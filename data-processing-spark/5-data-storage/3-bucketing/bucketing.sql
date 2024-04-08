USE tpch;

CREATE TABLE lineitem_w_encoding_w_bucketing (
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
INTO 5 BUCKETS LOCATION 's3a://tpch/lineitem_w_encoding_w_bucketing/';

INSERT INTO
    lineitem_w_encoding_w_bucketing
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
    tpch.lineitem;

ANALYZE TABLE lineitem_w_encoding_w_bucketing COMPUTE STATISTICS FOR ALL COLUMNS; 

EXPLAIN SELECT quantity
, count(1) as cnt
FROM
    lineitem_w_encoding_w_bucketing
WHERE
    quantity IN (1, 10, 20, 15, 24, 69)
GROUP BY 1;

EXPLAIN SELECT quantity
, count(1) as cnt
FROM
    tpch.lineitem
WHERE
    quantity IN (1, 10, 20, 15, 24, 69)
GROUP BY 1;
