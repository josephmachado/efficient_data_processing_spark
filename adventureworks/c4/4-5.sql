DROP SCHEMA IF EXISTS minio;

CREATE SCHEMA minio;

USE minio;

-- tpch is a bucket in minio, precreated for you with your docker container
DROP TABLE IF EXISTS lineitem_wo_encoding;

CREATE TABLE lineitem_wo_encoding (
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
) LOCATION 's3a://tpch/lineitem_wo_encoding/';

INSERT INTO
    minio.lineitem_wo_encoding
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
    minio.lineitem;

DROP TABLE IF EXISTS lineitem_w_encoding;

CREATE TABLE lineitem_w_encoding (
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
) WITH (
    external_location = 's3a://tpch/lineitem_w_encoding/',
    format = 'PARQUET'
);

INSERT INTO
    lineitem_w_encoding
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
    suppkey,
    sum(quantity) AS total_qty
FROM
    lineitem_w_encoding
GROUP BY
    suppkey;

-- 2.22 [6M rows, 14.5MB] [2.7M rows/s, 6.54MB/s]
SELECT
    suppkey,
    sum(quantity) AS total_qty
FROM
    lineitem_wo_encoding
GROUP BY
    suppkey;

-- 10.98 [6M rows, 215MB] [547K rows/s, 19.6MB/s]
USE tpch.tiny;

SELECT
    suppkey,
    sum(quantity) AS total_qty
FROM
    lineitem
GROUP BY
    suppkey
ORDER BY
    2 DESC;

SELECT
    custkey,
    sum(totalprice) AS total_cust_price
FROM
    orders_w_encoding -- & orders_wo_encoding
GROUP BY
    1;

DROP TABLE IF EXISTS lineitem_w_encoding_w_partitioning;

CREATE TABLE lineitem_w_encoding_w_partitioning (
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
    receiptdate date,
    receiptyear varchar(4)
) USING DELTA LOCATION 's3a://tpch/lineitem_w_encoding_w_partitioning/';

INSERT INTO
    lineitem_w_encoding_w_partitioning
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
    receiptdate,
    cast(year(receiptdate) AS varchar(4)) AS receiptyear
FROM
    tpch.lineitem;

-- run 'make metadata-db' or
-- 'docker exec -ti mariadb /usr/bin/mariadb -padmin'
-- on you terminal
SELECT
    *
FROM
    metastore_db.PARTITIONS;

exit;

-- To get the inputs, look for
-- Estimates: {rows: <input_rows> in
-- the query plan
EXPLAIN ANALYZE
SELECT
    *
FROM
    tpch.tiny.lineitem
WHERE
    year(receiptdate) = 1994;

-- Input: 60175 rows
EXPLAIN ANALYZE
SELECT
    *
FROM
    lineitem_w_encoding_w_partitioning
WHERE
    receiptyear = '1994';

DROP TABLE IF EXISTS lineitem_w_encoding_w_bucketing;

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
) WITH (
    external_location = 's3a://tpch/lineitem_w_encoding_w_bucketing/',
    format = 'PARQUET',
    bucket_count = 75,
    bucketed_by = ARRAY ['quantity']
);

USE tpch.tiny;

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
    lineitem;

EXPLAIN ANALYZE
SELECT
    *
FROM
    lineitem
WHERE
    quantity >= 30
    AND quantity <= 45;

-- Input: 60,175 rows (0B), Filtered: 68.14%
EXPLAIN ANALYZE
SELECT
    *
FROM
    lineitem_w_encoding_w_bucketing
WHERE
    quantity >= 30
    AND quantity <= 45;

-- Input: 21,550 rows (3.14MB), Filtered: 11.03%
DROP TABLE IF EXISTS lineitem_w_encoding_w_bucketing_eg;

CREATE TABLE lineitem_w_encoding_w_bucketing_eg (
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
) WITH (
    external_location = 's3a://tpch/lineitem_w_encoding_w_bucketing_eg/',
    format = 'PARQUET',
    bucket_count = 100,
    bucketed_by = ARRAY ['quantity']
);

USE tpch.tiny;

INSERT INTO
    lineitem_w_encoding_w_bucketing_eg
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
    lineitem;

EXPLAIN ANALYZE
SELECT
    *
FROM
    lineitem_w_encoding_w_bucketing_eg
WHERE
    quantity >= 30
    AND quantity <= 45;