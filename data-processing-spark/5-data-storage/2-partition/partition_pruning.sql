USE tpch;

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

DESCRIBE lineitem_w_encoding_w_partitioning;
-- See the partition information

EXPLAIN
SELECT
    *
FROM
    tpch.lineitem
WHERE
    year(receiptdate) = 1994;
-- Will result in a full scan


EXPLAIN 
SELECT
    *
FROM
    lineitem_w_encoding_w_partitioning
WHERE
    receiptyear = '1994';
-- Will result in a partition lookup
-- SEe the PartitionFilters that are applied as part of the FileScan