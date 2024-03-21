DROP TABLE IF EXISTS minio.lineitem_w_encoding_w_bucketing;

CREATE TABLE minio.lineitem_w_encoding_w_bucketing (
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
) USING parquet CLUSTERED BY (quantity) INTO 75 BUCKETS LOCATION 's3a://tpch/lineitem_w_encoding_w_bucketing/';

INSERT INTO
    minio.lineitem_w_encoding_w_bucketing
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

SELECT
    COUNT(*)
FROM
    tpch.lineitem
WHERE
    quantity >= 30
    AND quantity <= 45;
-- 1,921,968
-- Time taken: 1.89 seconds, Fetched 1 row(s)

SELECT
    *
FROM
    minio.lineitem_w_encoding_w_bucketing
WHERE
    quantity >= 30
    AND quantity <= 45;
-- 1,921,968
-- Time taken: 2 seconds, Fetched 1 row(s)




