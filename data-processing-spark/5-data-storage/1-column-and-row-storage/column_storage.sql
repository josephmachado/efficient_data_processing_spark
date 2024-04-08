USE tpch;

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
    lineitem_wo_encoding
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

SELECT
    suppkey,
    sum(quantity) AS total_qty
FROM
    lineitem_w_encoding
GROUP BY
    suppkey;
-- Time taken: 3.655 seconds, Fetched 10000 row(s)

SELECT
    suppkey,
    sum(quantity) AS total_qty
FROM
    lineitem_wo_encoding
GROUP BY
    suppkey;
-- Time taken: 11.924 seconds, Fetched 10001 row(s) 
