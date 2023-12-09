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
            date_format(orderdate, '%Y-%m') AS ordermonth,
            ROUND(SUM(totalprice) / 100000, 2) AS total_price -- divide by 100,000 for readability
        FROM
            orders
        GROUP BY
            1
    )
LIMIT
    10;

;

SELECT
    ordermonth,
    total_price,
    LAG(total_price) OVER(
        ORDER BY
            ordermonth
    ) AS prev_month_total_price,
    LAG(total_price, 2) OVER(
        ORDER BY
            ordermonth
    ) AS prev_prev_month_total_price
FROM
    (
        SELECT
            date_format(orderdate, '%Y-%m') AS ordermonth,
            ROUND(SUM(totalprice) / 100000, 2) AS total_price -- divide by 100,000 for readability
        FROM
            orders
        GROUP BY
            1
    )
LIMIT
    24;

;

SELECT
    customer_name,
    order_date,
    total_price,
    lag(total_price) over(
        PARTITION by customer_name
        ORDER BY
            order_date
    ) AS prev_total_price,
    ROUND(
        (
            lag(total_price) over(
                PARTITION by customer_name
                ORDER BY
                    order_date
            ) - total_price
        ) / lag(total_price) over(
            PARTITION by customer_name
            ORDER BY
                order_date
        ) * 100,
        2
    ) AS price_change_percentage
FROM
    (
        SELECT
            c.name AS customer_name,
            o.orderdate AS order_date,
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
    order_date;

SELECT
    customer_name,
    order_date,
    total_price,
    CASE
        WHEN total_price > LAG(total_price) over(
            PARTITION by customer_name
            ORDER BY
                order_date
        ) THEN TRUE
        ELSE FALSE
    END AS has_total_price_increased,
    CASE
        WHEN total_price < LEAD(total_price) over(
            PARTITION by customer_name
            ORDER BY
                order_date
        ) THEN TRUE
        ELSE FALSE
    END AS will_total_price_increase
FROM
    (
        SELECT
            c.name AS customer_name,
            o.orderdate AS order_date,
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
    order_date
LIMIT
    50;