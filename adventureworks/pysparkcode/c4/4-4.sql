USE tpch;

EXPLAIN
SELECT
    o.orderkey,
    SUM(l.extendedprice * (1 - l.discount)) AS total_price_wo_tax
FROM
    lineitem l
    JOIN orders o ON l.orderkey = o.orderkey
GROUP BY
    o.orderkey;

EXPLAIN
SELECT
    s.name AS supplier_name,
    SUM(l.extendedprice * (1 - l.discount)) AS total_price_wo_tax
FROM
    lineitem l
    JOIN supplier s ON l.suppkey = s.suppkey
GROUP BY
    s.name;

EXPLAIN
SELECT
    o.orderpriority,
    count(DISTINCT o.orderkey) AS order_count
FROM
    orders o
    JOIN lineitem l ON o.orderkey = l.orderkey
WHERE
    o.orderdate >= date '1994-12-01'
    AND o.orderdate < date '1994-12-01' + INTERVAL '3' MONTH
    AND l.commitdate < l.receiptdate
GROUP BY
    o.orderpriority
ORDER BY
    o.orderpriority;