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


USE tpch;

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

USE tpch;

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

USE tpch;

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

USE tpch;

-- CROSS JOIN
SELECT
    n.name AS nation_name,
    r.name AS region_name
FROM
    nation n
    CROSS JOIN region r;