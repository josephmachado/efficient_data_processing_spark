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
) USING CSV LOCATION 's3a://tpch/lineitem_wo_encoding/';

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
    tpch.lineitem;

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
) USING parquet LOCATION 's3a://tpch/lineitem_w_encoding/';

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

-- Time taken: 3.655 seconds, Fetched 10000 row(s)
SELECT
    suppkey,
    sum(quantity) AS total_qty
FROM
    lineitem_w_encoding
GROUP BY
    suppkey;

-- Time taken: 11.924 seconds, Fetched 10001 row(s) 
SELECT
    suppkey,
    sum(quantity) AS total_qty
FROM
    lineitem_wo_encoding
GROUP BY
    suppkey;

-- Time taken: 5.374 seconds, Fetched 10000 row(s)
SELECT
    suppkey,
    sum(quantity) AS total_qty
FROM
    tpch.lineitem
GROUP BY
    suppkey
ORDER BY
    2 DESC;

-- Do the same for orders tables   
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
) USING parquet PARTITIONED BY (receiptyear) LOCATION 's3a://tpch/lineitem_w_encoding_w_partitioning/';

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

-- Will result in a full scan
EXPLAIN
SELECT
    *
FROM
    tpch.lineitem
WHERE
    year(receiptdate) = 1994;

-- Will result in a partition lookup
EXPLAIN EXTENDED
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
) USING parquet CLUSTERED BY (quantity) INTO 75 BUCKETS LOCATION 's3a://tpch/lineitem_w_encoding_w_bucketing/';

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

EXPLAIN
SELECT
    *
FROM
    tpch.lineitem
WHERE
    quantity >= 30
    AND quantity <= 45;

EXPLAIN
SELECT
    *
FROM
    lineitem_w_encoding_w_bucketing
WHERE
    quantity >= 30
    AND quantity <= 45;

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
) USING parquet CLUSTERED BY (quantity) INTO 100 BUCKETS;

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
    tpch.lineitem;

EXPLAIN
SELECT
    *
FROM
    lineitem_w_encoding_w_bucketing_eg
WHERE
    quantity >= 30
    AND quantity <= 45;