USE tpch;

WITH duplicated_orders AS (
    SELECT
        *
    FROM
        orders
    UNION
    SELECT
        *
    FROM
        orders
),
ranked_orders AS (
    SELECT
        *,
        row_number() over(
            PARTITION by orderkey
            ORDER BY
                orderdate
        ) AS rn
    FROM
        orders
)
SELECT
    COUNT(*)
FROM
    ranked_orders
WHERE
    rn = 1;

WITH ranked_monthly_orders AS (
    SELECT
        date_format(orderdate, 'y-M') AS ordermonth,
        orderkey,
        custkey,
        totalprice,
        row_number() over(
            PARTITION by date_format(orderdate, 'y-M'),
            custkey
            ORDER BY
                orderdate DESC
        ) AS rn
    FROM
        orders
)
SELECT
    ordermonth,
    orderkey,
    custkey,
    totalprice
FROM
    ranked_monthly_orders
WHERE
    rn = 1
ORDER BY
    custkey,
    ordermonth
LIMIT
    50;

SELECT
    date_format(orderdate, 'y-M') AS ordermonth,
    ROUND(
        AVG(
            CASE
                WHEN orderpriority = '1-URGENT' THEN totalprice
                ELSE NULL
            END
        ),
        2
    ) AS urgent_order_avg_price,
    ROUND(
        AVG(
            CASE
                WHEN orderpriority = '2-HIGH' THEN totalprice
                ELSE NULL
            END
        ),
        2
    ) AS high_order_avg_price,
    ROUND(
        AVG(
            CASE
                WHEN orderpriority = '3-MEDIUM' THEN totalprice
                ELSE NULL
            END
        ),
        2
    ) AS medium_order_avg_price,
    ROUND(
        AVG(
            CASE
                WHEN orderpriority = '4-NOT SPECIFIED' THEN totalprice
                ELSE NULL
            END
        ),
        2
    ) AS not_specified_order_avg_price,
    ROUND(
        AVG(
            CASE
                WHEN orderpriority = '5-LOW' THEN totalprice
                ELSE NULL
            END
        ),
        2
    ) AS low_order_avg_price
FROM
    orders
GROUP BY
    date_format(orderdate, 'y-M');

WITH monthly_orders AS (
    SELECT
        date_format(orderdate, 'y-M') AS ordermonth,
        ROUND(SUM(totalprice) / 100000, 2) AS totalprice
    FROM
        orders
    GROUP BY
        date_format(orderdate, 'y-M')
)
SELECT
    ordermonth,
    totalprice,
    ROUND(
        (
            totalprice - lag(totalprice) over(
                ORDER BY
                    ordermonth
            )
        ) * 100 / (
            lag(totalprice) over(
                ORDER BY
                    ordermonth
            )
        ),
        2
    ) AS MoM_totalprice_change
FROM
    monthly_orders
ORDER BY
    ordermonth;

WITH monthly_orders AS (
    SELECT
        DATE(date_format(o.orderdate, 'y-M-01')) AS ordermonth,
        n.name AS customer_nation,
        ROUND(SUM(o.totalprice) / 100000, 2) AS totalprice
    FROM
        orders o
        JOIN customer c ON o.custkey = c.custkey
        JOIN nation n ON c.nationkey = c.nationkey
    GROUP BY
        date_format(o.orderdate, 'y-M-01'),
        n.name
)
SELECT
    ordermonth,
    customer_nation,
    totalprice,
    ROUND(
        (
            totalprice - lag(totalprice) over(
                PARTITION BY customer_nation
                ORDER BY
                    ordermonth
            )
        ) * 100 / (
            lag(totalprice) over(
                PARTITION BY customer_nation
                ORDER BY
                    ordermonth
            )
        ),
        2
    ) AS MoM_totalprice_change
FROM
    monthly_orders
ORDER BY
    customer_nation,
    ordermonth;

WITH monthly_cust_nation_orders AS (
    SELECT
        date_format(o.orderdate, 'y-M') AS ordermonth,
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