USE tpch;

DROP TABLE IF EXISTS lineitem_w_encoding;

-- make sure that the folder is deleted at minio http://localhost:9000
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

ANALYZE TABLE lineitem_w_encoding COMPUTE STATISTICS;

DESC EXTENDED lineitem_w_encoding;
-- Notice the metadata "Statistics 211837833 bytes, 6001215 rows" 
-- this is about 211MB

DESC EXTENDED lineitem_w_sorting;
-- Notice the metadata "Statistics 200505631 bytes, 6001215 rows"
-- This is about 200MB
