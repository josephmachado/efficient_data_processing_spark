USE tpch;

-- INNER JOIN
SELECT
    o.orderkey,
    l.orderkey
FROM
    orders o
    JOIN lineitem l ON o.orderkey = l.orderkey
    AND o.orderdate BETWEEN l.shipdate - INTERVAL '5' DAY
    AND l.shipdate + INTERVAL '5' DAY
LIMIT
    10;

-- 198277, 198277
SELECT
    COUNT(o.orderkey) AS order_rows_count,
    COUNT(l.orderkey) AS lineitem_rows_count
FROM
    orders o
    JOIN lineitem l ON o.orderkey = l.orderkey
    AND o.orderdate BETWEEN l.shipdate - INTERVAL '5' DAY
    AND l.shipdate + INTERVAL '5' DAY;

-- LEFT JOIN
SELECT
    o.orderkey,
    l.orderkey
FROM
    orders o
    LEFT JOIN lineitem l ON o.orderkey = l.orderkey
    AND o.orderdate BETWEEN l.shipdate - INTERVAL '5' DAY
    AND l.shipdate + INTERVAL '5' DAY
LIMIT
    10;

-- 1512555, 198277
SELECT
    COUNT(o.orderkey) AS order_rows_count,
    COUNT(l.orderkey) AS lineitem_rows_count
FROM
    orders o
    LEFT JOIN lineitem l ON o.orderkey = l.orderkey
    AND o.orderdate BETWEEN l.shipdate - INTERVAL '5' DAY
    AND l.shipdate + INTERVAL '5' DAY;

-- RIGHT OUTER JOIN
SELECT
    o.orderkey,
    l.orderkey
FROM
    orders o
    RIGHT JOIN lineitem l ON o.orderkey = l.orderkey
    AND o.orderdate BETWEEN l.shipdate - INTERVAL '5' DAY
    AND l.shipdate + INTERVAL '5' DAY
LIMIT
    10;

-- 198277,  6001215
SELECT
    COUNT(o.orderkey) AS order_rows_count,
    COUNT(l.orderkey) AS lineitem_rows_count
FROM
    orders o
    RIGHT JOIN lineitem l ON o.orderkey = l.orderkey
    AND o.orderdate BETWEEN l.shipdate - INTERVAL '5' DAY
    AND l.shipdate + INTERVAL '5' DAY;

-- FULL OUTER JOIN
SELECT
    o.orderkey,
    l.orderkey
FROM
    orders o FULL
    OUTER JOIN lineitem l ON o.orderkey = l.orderkey
    AND o.orderdate BETWEEN l.shipdate - INTERVAL '5' DAY
    AND l.shipdate + INTERVAL '5' DAY
LIMIT
    10;

-- 1512555, 6001215
SELECT
    COUNT(o.orderkey) AS order_rows_count,
    COUNT(l.orderkey) AS lineitem_rows_count
FROM
    orders o FULL
    OUTER JOIN lineitem l ON o.orderkey = l.orderkey
    AND o.orderdate BETWEEN l.shipdate - INTERVAL '5' DAY
    AND l.shipdate + INTERVAL '5' DAY;

-- CROSS JOIN
SELECT
    n.name AS nation_name,
    r.name AS region_name
FROM
    nation n
    CROSS JOIN region r;

-- Exercise
-- Time taken: 4.489 seconds, Fetched 71856 row(s)
SELECT
    o1.custkey
FROM
    orders o1
    JOIN orders o2 ON o1.custkey = o2.custkey
    AND year(o1.orderdate) = year(o2.orderdate)
    AND weekofyear(o1.orderdate) = weekofyear(o2.orderdate)
WHERE
    o1.orderkey != o2.orderkey;