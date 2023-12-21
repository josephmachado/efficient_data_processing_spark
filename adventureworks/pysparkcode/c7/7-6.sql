USE tpch;

-- This is supposed to fail, but spark makes it work!
SELECT
    DATEDIFF('2022-10-05', '2022-10-01') AS date_diff_result;

SELECT
    o.orderkey,
    o.orderdate,
    COALESCE(l.orderkey, 9999999) AS lineitem_orderkey,
    l.shipdate
FROM
    orders o
    LEFT JOIN lineitem l ON o.orderkey = l.orderkey
    AND o.orderdate BETWEEN l.shipdate - INTERVAL '5' DAY
    AND l.shipdate + INTERVAL '5' DAY
WHERE
    l.shipdate IS NOT NULL
LIMIT
    10;