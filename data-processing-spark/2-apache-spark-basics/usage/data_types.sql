USE tpch;

-- This is supposed to fail, but spark makes it work and generate a NULL! Which is not what we expect
SELECT DATEDIFF('2022-10-01', 'invalid_date');

SELECT
    DATEDIFF(
        CAST('2022-10-01' AS DATE),
        CAST('invalid_date' AS DATE)
    );

USE tpch;

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
    l.shipdate IS NULL
LIMIT
    10;