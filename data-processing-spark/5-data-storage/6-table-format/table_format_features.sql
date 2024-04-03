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
   'delta.minWriterVersion' = '5',
   'delta.deletedFileRetentionDuration'='5s', -- defaults to 7days
   );

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

-- Insert a few rows
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
-- GET TIMESTAMP FROM the second column in the "DESCRIBE HISTORY delta_lineitem" query;
select count(*) from delta_lineitem TIMESTAMP AS OF '2024-03-22 13:13:38'; 
-- 6,001,215
select count(*) from delta_lineitem VERSION AS OF 1;
-- 6,001,215

-------------------- SCHEMA EVOLUTION --------------------------
-- Schema evolution
ALTER TABLE delta_lineitem  ADD COLUMN phonenumber STRING;
ALTER TABLE delta_lineitem  DROP COLUMN COMMENT;

-- latest (based on id) rows will the add column and drop column form above
DESCRIBE HISTORY delta_lineitem;

-- let's see what delta_lineitem looks like
DESCRIBE delta_lineitem;

-- we can see the deleted column with time travel
SELECT comment FROM delta_lineitem VERSION AS OF 3 LIMIT 5;

-------------------- UPDATE, DELTES & MERGES --------------------------

-- Updates, Deletes and Merge into
DELETE FROM delta_lineitem WHERE shipmode = 'TRUCK';
UPDATE delta_lineitem SET shipmode = 'AIR' WHERE shipmode = 'REG AIR';

DESCRIBE HISTORY delta_lineitem;

VACUUM delta_lineitem;

DESCRIBE HISTORY delta_lineitem;

-- MERGE INTO / UPSERT INTO especially useful for dimension table updates and inserts but done at the same time
-- let's use nation table as an example

CREATE TABLE delta_nation (
    nationkey Long,
    name STRING,
    ACTIVE INT
) USING DELTA LOCATION 's3a://tpch/delta_nation/' 
TBLPROPERTIES (
   'delta.columnMapping.mode' = 'name',
   'delta.minReaderVersion' = '2',
   'delta.minWriterVersion' = '5');

INSERT INTO delta_nation
SELECT nationkey
, name
, 1
FROM nation
WHERE name like 'A%';

MERGE INTO delta_nation
USING nation
on delta_nation.nationkey = nation.nationkey
WHEN MATCHED THEN UPDATE SET 
delta_nation.name = CONCAT(nation.name, '_', 'A_NATION'),
delta_nation.active = 1
WHEN NOT MATCHED THEN
INSERT (nationkey, name, active) VALUES (nation.nationkey, nation.name, 1);

SELECT DISTINCT name FROM delta_nation;