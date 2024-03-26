CREATE SCHEMA IF NOT EXISTS tpch;

USE tpch;

DROP TABLE IF EXISTS customer;

CREATE TABLE customer (
    custkey Long,
    name STRING,
    address STRING,
    nationkey Long,
    phone STRING,
    acctbal Double,
    mktsegment STRING,
    COMMENT STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ESCAPED BY '"' STORED AS TEXTFILE
LOCATION 's3a://tpch/customer';

LOAD DATA LOCAL INPATH '/opt/spark/work-dir/tpch-dbgen/customer.tbl' OVERWRITE INTO TABLE customer;

DROP TABLE IF EXISTS lineitem;

CREATE TABLE lineitem (
    orderkey Long,
    partkey Long,
    suppkey Long,
    linenumber Long,
    quantity Double,
    extendedprice Double,
    discount Double,
    tax Double,
    returnflag STRING,
    linestatus STRING,
    shipdate DATE,
    commitdate DATE,
    receiptdate DATE,
    shipinstruct STRING,
    shipmode STRING,
    COMMENT STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ESCAPED BY '"' STORED AS TEXTFILE LOCATION 's3a://tpch/lineitem';

LOAD DATA LOCAL INPATH '/opt/spark/work-dir/tpch-dbgen/lineitem.tbl' OVERWRITE INTO TABLE lineitem;

DROP TABLE IF EXISTS nation;

CREATE TABLE nation (
    nationkey Long,
    name STRING,
    regionkey Long,
    COMMENT STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ESCAPED BY '"' STORED AS TEXTFILE LOCATION 's3a://tpch/nation';

LOAD DATA LOCAL INPATH '/opt/spark/work-dir/tpch-dbgen/nation.tbl' OVERWRITE INTO TABLE nation;

DROP TABLE IF EXISTS orders;

CREATE TABLE orders (
    orderkey Long,
    custkey Long,
    orderstatus STRING,
    totalprice Double,
    orderdate STRING,
    orderpriority STRING,
    clerk STRING,
    shippriority Long,
    COMMENT STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ESCAPED BY '"' STORED AS TEXTFILE LOCATION 's3a://tpch/orders';

LOAD DATA LOCAL INPATH '/opt/spark/work-dir/tpch-dbgen/orders.tbl' OVERWRITE INTO TABLE orders;

DROP TABLE IF EXISTS part;

CREATE TABLE part (
    partkey Long,
    name STRING,
    mfgr STRING,
    brand STRING,
    TYPE STRING,
    size Long,
    container STRING,
    retailprice Double,
    COMMENT STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ESCAPED BY '"' STORED AS TEXTFILE LOCATION 's3a://tpch/part';

LOAD DATA LOCAL INPATH '/opt/spark/work-dir/tpch-dbgen/part.tbl' OVERWRITE INTO TABLE part;

DROP TABLE IF EXISTS partsupp;

CREATE TABLE partsupp (
    partkey Long,
    suppkey Long,
    availability Long,
    supplycost Double,
    COMMENT STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ESCAPED BY '"' STORED AS TEXTFILE LOCATION 's3a://tpch/partsupp';

LOAD DATA LOCAL INPATH '/opt/spark/work-dir/tpch-dbgen/partsupp.tbl' OVERWRITE INTO TABLE partsupp;

DROP TABLE IF EXISTS region;

CREATE TABLE region (
    regionkey Long,
    name STRING,
    COMMENT STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ESCAPED BY '"' STORED AS TEXTFILE LOCATION 's3a://tpch/region';

LOAD DATA LOCAL INPATH '/opt/spark/work-dir/tpch-dbgen/region.tbl' OVERWRITE INTO TABLE region;

DROP TABLE IF EXISTS supplier;

CREATE TABLE supplier (
    suppkey Long,
    name STRING,
    address STRING,
    nationkey Long,
    phone STRING,
    acctbal Double,
    COMMENT STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ESCAPED BY '"' STORED AS TEXTFILE LOCATION 's3a://tpch/supplier';

LOAD DATA LOCAL INPATH '/opt/spark/work-dir/tpch-dbgen/supplier.tbl' OVERWRITE INTO TABLE supplier;