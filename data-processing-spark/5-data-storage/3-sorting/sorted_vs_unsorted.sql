USE minio;

DROP TABLE IF EXISTS lineitem_w_encoding_w_sorting;

CREATE TABLE lineitem_w_encoding_w_sorting (
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
) USING parquet LOCATION 's3a://tpch/lineitem_w_encoding_w_sorting/';

INSERT INTO
    lineitem_w_encoding_w_sorting
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
    ORDER BY shipmode;


EXPLAIN SELECT shipmode, suppkey from lineitem_w_encoding where shipmode = 'AIR';
EXPLAIN SELECT shipmode, suppkey from lineitem_w_encoding_w_sorting where shipmode = 'AIR';
-- The query plan looks alike since both of these query will involve reading the parquet files

SELECT count(*) from lineitem_w_encoding where shipmode = 'AIR';
/*
Time taken: 1.428 seconds, Fetched 1 row(s)
*/

SELECT count(*) from lineitem_w_encoding_w_sorting where shipmode = 'AIR';
/*
Time taken: 0.411 seconds, Fetched 1 row(s)
*/

-- The time taken may differ a bit, but there will be a significant difference