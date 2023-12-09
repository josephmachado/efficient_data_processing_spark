USE tpch;

-- 5465 rows
SELECT
    custkey,
    name
FROM
    customer
WHERE
    name LIKE '%_91%';

-- 5465 rows
SELECT
    custkey,
    name
FROM
    customer
WHERE
    name LIKE '%_91%'
UNION
SELECT
    custkey,
    name
FROM
    customer
WHERE
    name LIKE '%_91%'
UNION
SELECT
    custkey,
    name
FROM
    customer
WHERE
    name LIKE '%_91%';

-- UNION ALL will not remove duplicate rows; the below query will produce 16395 (5465 * 3) rows
SELECT
    custkey,
    name
FROM
    customer
WHERE
    name LIKE '%_91%'
UNION
ALL
SELECT
    custkey,
    name
FROM
    customer
WHERE
    name LIKE '%_91%'
UNION
ALL
SELECT
    custkey,
    name
FROM
    customer
WHERE
    name LIKE '%_91%';

-- EXCEPT will get the rows in the first query result that is not in the second query result, 0 rows
SELECT
    custkey,
    name
FROM
    customer
WHERE
    name LIKE '%_91%'
EXCEPT
SELECT
    custkey,
    name
FROM
    customer
WHERE
    name LIKE '%_91%';

-- The below query will result in 4967 rows; the first query has 5465 rows, and the second has 498 rows
SELECT
    custkey,
    name
FROM
    customer
WHERE
    name LIKE '%_91%'
EXCEPT
SELECT
    custkey,
    name
FROM
    customer
WHERE
    name LIKE '%191%';