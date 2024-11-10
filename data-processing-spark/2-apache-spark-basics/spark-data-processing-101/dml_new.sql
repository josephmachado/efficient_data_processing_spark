/*
Exploring the DDL and DML
*/

USE tpch;

DROP TABLE IF EXISTS sample_table2;

CREATE TABLE sample_table2 (sample_key bigint, sample_status STRING)
USING delta LOCATION 's3a://tpch/sample_table2';

-- Overview of table
SELECT
    *
FROM
    sample_table2;


-- Insert value in it and inspect
INSERT INTO
    sample_table2
VALUES
    (1, 'hello');

SELECT
    *
FROM
    sample_table2;

-- Insert duplicate row and inspect
INSERT INTO
    sample_table2
VALUES
    (1, 'hello');

SELECT
    *
FROM
    sample_table2;

-- Insert values from another table and inspect
INSERT INTO
    sample_table2
SELECT 
    nationkey,
    name
FROM
    nation;

SELECT
    *
FROM
    sample_table2;

/*
Tasks:
    -- 1. Deletes all the rows in sample_table2, but table still present.
    -- 2. In order to delete rows we need to set the table to be transactional, 
          this supports sql transaction.
    -- 3. Deletes (single row deletes) as opposed to general 
          delete of entire paritions ( as allowed in HIVE)
*/


ALTER TABLE
    sample_table2
SET
    TBLPROPERTIES('transactional' = 'true');

-- the last setting enable ACID operations.

DELETE FROM
    sample_table2;

-- Drop the table entirely, the table will need to be re-created.
DROP TABLE
    sample_table2;

/*
Here's a quick breakdown of how ACID properties are implemented in Hive:

    Atomicity: Ensures that each transaction is "all or nothing." 
    If an operation within a transaction fails, the entire transaction is rolled back, 
    so the database remains unchanged. 
    This is important to prevent partial writes that could corrupt data.

    Consistency: Ensures that the database remains in a valid state before and after the transaction. 
    Hive enforces constraints and schema rules that prevent invalid data states within a transactional table.

    Isolation: Provides multiple transactions running in parallel without interfering with each other, 
    avoiding "dirty reads." Hive supports isolation to a limited degree by using snapshot isolation, 
    which ensures that each transaction sees a consistent view of the data as of the time it started.

    Durability: Ensures that once a transaction is committed, it will remain in the database even in the event of a failure. 
    This is generally achieved through reliable storage systems and logs that record transactions to ensure they can be replayed if needed.
*/
