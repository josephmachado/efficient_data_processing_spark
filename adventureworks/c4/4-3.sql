USE tpch;

SELECT
    p.name AS part_name,
    p.partkey,
    l.linenumber,
    ROUND(l.extendedprice * (1 - l.discount), 2) AS total_price_wo_tax
FROM
    lineitem l
    JOIN part p ON l.partkey = p.partkey;

SELECT
    s.name AS supplier_name,
    l.linenumber,
    ROUND(l.extendedprice * (1 - l.discount), 2) AS total_price_wo_tax
FROM
    lineitem l
    JOIN supplier s ON l.suppkey = s.suppkey
LIMIT
    10;