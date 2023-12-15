USE tpch;

SELECT
    orderkey,
    linenumber,
    round(extendedprice * (1 - discount) * (1 + tax), 2) AS totalprice
FROM
    lineitem
LIMIT
    10;

SELECT
    orderpriority,
    ROUND(SUM(totalprice) / 1000, 2) AS total_price_thousands
FROM
    orders
GROUP BY
    orderpriority
ORDER BY
    orderpriority;