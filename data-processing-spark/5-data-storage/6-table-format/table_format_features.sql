-- Create a delta table

USE tpch;
DROP TABLE IF EXISTS delta_lineitem;

CREATE TABLE delta_lineitem(
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
) USING DELTA LOCATION 's3a://tpch/delta_lineitem/' 
TBLPROPERTIES (
   'delta.columnMapping.mode' = 'name',
   'delta.minReaderVersion' = '2',
   'delta.minWriterVersion' = '5');

INSERT INTO
    delta_lineitem
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

-- Look at versions
DESCRIBE HISTORY delta_lineitem;

-- Insert a few row
-- Create a delta table

INSERT INTO
    delta_lineitem
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
    tpch.lineitem LIMIT 1000;


-- look at versions now
DESCRIBE HISTORY delta_lineitem;
-- There would be 3 versions, 1 when the table was created, 2 when data was inserted the first time and 3 when data was inserted the second time

-- go back to an older version
------------------------ TIME TRAVEL----------------------
-- Current count
select count(*) from delta_lineitem;
-- 6,002,215
select count(*) from delta_lineitem TIMESTAMP AS OF '2024-03-22 13:13:38';
-- 6,001,215
select count(*) from delta_lineitem VERSION AS OF 1;
-- 6,001,215

-- Schema evolution
ALTER TABLE delta_lineitem  ADD COLUMN phonenumber STRING;
ALTER TABLE delta_lineitem  DROP COLUMN COMMENT;

DESCRIBE HISTORY delta_lineitem;

-- Updates, Deletes and Merge into
DELETE FROM delta_lineitem WHERE shipmode = 'TRUCK';
UPDATE delta_lineitem SET shipmode = 'AIR' WHERE shipmode = 'REG AIR';
-- MERGE INTO / UPSERT INTO especially useful for dimension tables
