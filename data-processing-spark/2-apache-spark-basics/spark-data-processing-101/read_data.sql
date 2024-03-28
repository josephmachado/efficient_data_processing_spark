USE tpch;

-- use * to specify all columns
SELECT * FROM orders LIMIT 10;

-- use column names only to read data from those columns
SELECT orderkey, totalprice FROM orders LIMIT 10;

USE tpch;

-- all customer rows that have nationkey = 20
SELECT
    *
FROM
    customer
WHERE
    nationkey = 20
LIMIT
    10;

-- all customer rows that have nationkey = 20 and acctbal > ↪ 1000
SELECT
    *
FROM
    customer
WHERE
    nationkey = 20
    AND acctbal > 1000
LIMIT
    10;

-- all customer rows that have nationkey = 20 or acctbal > ↪ 1000
SELECT
    *
FROM
    customer
WHERE
    nationkey = 20
    OR acctbal > 1000
LIMIT
    10;

-- all customer rows that have (nationkey = 20 and acctbal > ↪ 1000) or rows that have nationkey = 11
SELECT
    *
FROM
    customer
WHERE
    (
        nationkey = 20
        AND acctbal > 1000
    )
    OR nationkey = 11
LIMIT
    10;


USE tpch;

-- all customer rows where the name has a 381 in it
SELECT * FROM customer WHERE name LIKE '%381%';

-- all customer rows where the name ends with a 381
SELECT * FROM customer WHERE name LIKE '%381';

-- all customer rows where the name starts with a 381
SELECT * FROM customer WHERE name LIKE '381%';

-- all customer rows where the name has a combination of any character and 9 and 1
SELECT * FROM customer WHERE name LIKE '%_91%';

USE tpch;

-- all customer rows which have nationkey = 10 or nationkey = ↪ 20
SELECT
    *
FROM
    customer
WHERE
    nationkey IN (10, 20);

-- all customer rows which have do not have nationkey as 10 ↪ or20
SELECT
    *
FROM
    customer
WHERE
    nationkey NOT IN (10, 20);

USE tpch;

-- 150000
SELECT
    COUNT(*)
FROM
    customer;

-- 6001215
SELECT
    COUNT(*)
FROM
    lineitem;

USE tpch;

-- Will show the first ten customer records with the lowest ↪ custkey
-- rows are ordered in ASC order by default
SELECT
    *
FROM
    orders
ORDER BY
    custkey
LIMIT
    10;

-- Will show the first ten customer's records with the ↪ highest custkey
SELECT
    *
FROM
    orders
ORDER BY
    custkey DESC
LIMIT
    10; 
