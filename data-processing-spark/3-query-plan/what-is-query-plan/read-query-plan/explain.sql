USE tpch;

EXPLAIN
SELECT
    o.orderkey,
    SUM(l.extendedprice * (1 - l.discount))
    AS total_price_wo_tax
FROM
lineitem l
    JOIN orders o ON l.orderkey = o.orderkey
GROUP BY
o.orderkey;


SELECT
    o.orderkey,
    SUM(l.extendedprice * (1 - l.discount))
    AS total_price_wo_tax
FROM
lineitem l
    JOIN orders o ON l.orderkey = o.orderkey
GROUP BY
o.orderkey;

EXPLAIN EXTENDED
SELECT
    o.orderkey,
    SUM(l.extendedprice * (1 - l.discount))
    AS total_price_wo_tax
FROM
lineitem l
    JOIN orders o ON l.orderkey = o.orderkey
GROUP BY
o.orderkey;