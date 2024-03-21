USE tpch;

SELECT *
FROM (
    SELECT orderdate, totalprice, orderpriority
    FROM orders
) AS order_data
PIVOT (
    ROUND(AVG(totalprice), 2) AS avg_price
    FOR orderpriority IN ('1-URGENT' AS urgent_order, '2-HIGH' AS high_order, '3-MEDIUM' AS medium_order, '4-NOT SPECIFIED' AS not_specified_order, '5-LOW' AS low_order)
)
LIMIT 10;
