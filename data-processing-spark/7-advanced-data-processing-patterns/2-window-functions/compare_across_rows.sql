USE tpch;

SELECT
    ordermonth,
    total_price,
    LAG(total_price) OVER(
        ORDER BY
            ordermonth
    ) AS prev_month_total_price
FROM
    (
        SELECT
            date_format(orderdate, 'y-M') AS ordermonth,
            ROUND(SUM(totalprice) / 100000, 2) AS total_price -- divide by 100,000 for readability
        FROM
            orders
        GROUP BY
            1
    )
LIMIT
    10;

