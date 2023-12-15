USE tpch;

-- This is supposed to fail, but spark makes it work!
SELECT
    DATE_DIFF('day', '2022-10-01', '2022-10-05');

-- will fail due to in correct data type
SELECT
    DATE_DIFF(
        'day',
        CAST('2022-10-01' AS DATE),
        CAST('2022-10-05' AS DATE)
    );

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