USE tpch;

-- use * to specify all columns
SELECT
    *
FROM
    orders
LIMIT
    10;

-- use column names only to read data from those columns
SELECT
    orderkey,
    totalprice
FROM
    orders
LIMIT
    10;

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