USE tpch;

WITH monthly_cust_nation_orders AS (
    SELECT
        concat(extract(year from orderdate) ,'-', extract(month from orderdate)) AS ordermonth,
        n.name AS customer_nation,
        totalprice
    FROM
        orders o
        JOIN customer c ON o.custkey = c.custkey
        JOIN nation n ON c.nationkey = c.nationkey
)
SELECT
    ordermonth,
    customer_nation,
    ROUND(SUM(totalprice) / 100000, 2) AS totalprice -- divide by 100,000 for readability
FROM
    monthly_cust_nation_orders
GROUP BY
    GROUPING SETS (
        (ordermonth),
        (customer_nation),
        (ordermonth, customer_nation)
    );