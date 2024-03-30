USE tpch;

EXPLAIN
SELECT
    orderkey,
    linenumber,
    round(extendedprice * (1 - discount) * (1 + tax), 2) AS totalprice
FROM
    lineitem
LIMIT
    10;
-- Note that the above query will only produce a scan and project, and no shuffle

EXPLAIN
SELECT
    orderpriority,
    ROUND(SUM(totalprice) / 1000, 2) AS total_price_thousands
FROM
    orders
GROUP BY
    orderpriority;
-- Note that this query plan involves an Exchange