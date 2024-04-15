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

-- 247650, 247650
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

-- 1519332, 247650
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

-- 247650, 6001215
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

-- 1512555, 6001215 -- Some log, but results still show
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

-- LEFT ANTI JOIN
USE tpch;

-- suppkey starts from 1, where as nationkey starts from 0
SELECT n.* 
FROM nation n 
LEFT ANTI JOIN supplier s 
ON n.nationkey = s.suppkey;
-- this is not the right key to join on, but done to demonstrate left anti join