USE tpch;

EXPLAIN SELECT
    suppkey,
    sum(quantity) AS total_qty
FROM
    lineitem
GROUP BY
    suppkey
ORDER BY
    2 DESC;
    
DESC EXTENDED lineitem;
-- notice Storage Properties [escape.delim=", serialization.format=|, field.delim=|]                     

