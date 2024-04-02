CREATE SCHEMA IF NOT EXISTS minio;

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
) USING parquet CLUSTERED BY (extendedprice) INTO 15 BUCKETS LOCATION 's3a://tpch/lineitem_w_encoding_w_bucketing/';

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

EXPLAIN SELECT *
FROM
    minio.lineitem_w_encoding_w_bucketing
WHERE
    quantity >= 30
    AND quantity <= 45;

EXPLAIN SELECT *
FROM
    tpch.lineitem_w_encoding
WHERE
    quantity >= 30
    AND quantity <= 45;
