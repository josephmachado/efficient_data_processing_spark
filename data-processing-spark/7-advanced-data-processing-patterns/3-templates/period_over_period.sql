USE tpch;

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
