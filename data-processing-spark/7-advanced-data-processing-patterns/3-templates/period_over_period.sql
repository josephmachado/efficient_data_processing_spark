USE tpch;

WITH monthly_orders AS (
    SELECT
        concat(extract(year from orderdate) ,'-', extract(month from orderdate)) AS ordermonth,
        ROUND(SUM(totalprice) / 100000, 2) AS totalprice
    FROM
        orders
    GROUP BY
        concat(extract(year from orderdate) ,'-', extract(month from orderdate))
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