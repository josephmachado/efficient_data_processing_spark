-- ROWS
USE tpch.tiny;

SELECT
    customer_name,
    order_month,
    total_price,
    ROUND(
        AVG(total_price) OVER(
            PARTITION BY customer_name
            ORDER BY
                order_month ROWS BETWEEN 1 PRECEDING
                AND 1 FOLLOWING
        ),
        2
    ) AS three_mo_total_price_avg
FROM
    (
        SELECT
            c.name AS customer_name,
            concat(extract(year from orderdate) ,'-', extract(month from orderdate)) AS order_month,
            sum(o.totalprice) AS total_price
        FROM
            orders o
            JOIN customer c ON o.custkey = c.custkey
        GROUP BY
            1,
            2
    )
ORDER BY
    customer_name,
    order_month
LIMIT
    50;

-- RANGE

USE tpch.tiny;

SELECT
    customer_name,
    order_month,
    total_price,
    ROUND(
        AVG(total_price) OVER(
            PARTITION BY customer_name
            ORDER BY
                order_month ROWS BETWEEN 1 PRECEDING
                AND 1 FOLLOWING
        ),
        2
    ) AS avg_3m_all,
    ROUND(
        AVG(total_price) OVER(
            PARTITION BY customer_name
            ORDER BY
                order_month RANGE BETWEEN
                INTERVAL '1' MONTH PRECEDING
                AND INTERVAL '1' MONTH FOLLOWING
        ),
        2
    ) AS avg_3m
FROM
    (
        SELECT
            c.name AS customer_name,
            CAST(
                concat(extract(year from orderdate) ,'-', extract(month from orderdate), '-', '01') AS DATE
            ) AS order_month,
            sum(o.totalprice) AS total_price
        FROM
            orders o
            JOIN customer c ON o.custkey = c.custkey
        GROUP BY
            1,
            2
    )
ORDER BY
    customer_name,
    order_month
LIMIT
    50;