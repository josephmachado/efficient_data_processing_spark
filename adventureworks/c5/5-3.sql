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

SELECT
    n.name AS nation_name,
    year(o.orderdate) AS order_year,
    ROUND(sum(o.totalprice) / 100000, 2) AS total_price,
    ROUND(
        avg(sum(o.totalprice)) over(
            PARTITION by n.name
            ORDER BY
                year(o.orderdate)
        ) / 100000,
        2
    ) AS cumulative_sum_total_price
FROM
    orders o
    JOIN customer c ON o.custkey = c.custkey
    JOIN nation n ON c.nationkey = n.nationkey
GROUP BY
    n.name,
    year(o.orderdate)
ORDER BY
    n.name,
    year(o.orderdate)
LIMIT
    20;