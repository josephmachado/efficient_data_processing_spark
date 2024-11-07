/*
Analytical Question:
    With respect to the totalprice of a order in the 'orders' table,
    classify the the order into the 'orde_price_bucket' if:
    totalprice > 100000, then 'high'
    totalprice <= 100000 and totalprice >= 25000, then 'medium'
    totalprice < 25000, then 'low'
*/

USE tpch;
DESCRIBE orders;

-- Overview of orders table
SELECT 
    orderkey,
    custkey,
    orderstatus,
    totalprice,
    orderdate,
    orderpriority,
    clerk,
    shippriority,
    COMMENT
FROM orders
LIMIT 5;

-- Analytical Query
SELECT 
    orderkey,
    totalprice,
    CASE
        WHEN totalprice > 100000 THEN 'high'
        WHEN totalprice BETWEEN 25000
        AND 100000 THEN 'medium'
        ELSE 'low'
    END AS order_price_bucket
FROM
    orders
LIMIT 
    20;