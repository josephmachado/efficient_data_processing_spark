USE tpch;

SELECT
    c.name AS customer_name,
    o.orderdate,
    SUM(o.totalprice) AS total_price,
    -- We have it here just for comparison purposes
    ROUND(
        SUM(SUM(o.totalprice)) over(
            PARTITION by o.custkey
            ORDER BY
                o.orderdate
        ),
        2
    ) AS cumulative_sum_total_price
FROM
    orders o
    JOIN customer c ON o.custkey = c.custkey
GROUP BY
    c.name,
    o.custkey,
    o.orderdate
ORDER BY
    o.custkey,
    o.orderdate
LIMIT
    20;