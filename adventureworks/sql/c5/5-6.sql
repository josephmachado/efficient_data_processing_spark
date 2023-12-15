USE tpch;

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
            DATE_FORMAT(orderdate, '%y-%M') AS order_month,
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

-- Use the query from the example above
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
    ) AS three_mo_total_price_avg,
    ROUND(
        AVG(total_price) OVER(
            PARTITION BY customer_name
            ORDER BY
                order_month
        ),
        2
    ) AS running_total_price_avg,
    ROUND(
        AVG(total_price) OVER(
            PARTITION BY customer_name
            ORDER BY
                order_month ROWS BETWEEN 3 PRECEDING
                AND 2 FOLLOWING
        ),
        2
    ) AS six_mo_total_price_avg,
    ROUND(
        AVG(total_price) OVER(
            PARTITION BY customer_name
            ORDER BY
                order_month ROWS BETWEEN 4 PRECEDING
                AND 1 PRECEDING
        ),
        2
    ) AS prev_three_mo_total_price_avg
FROM
    (
        SELECT
            c.name AS customer_name,
            DATE_FORMAT(orderdate, '%y-%M') AS order_month,
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
                order_month RANGE BETWEEN INTERVAL '1' MONTH PRECEDING
                AND INTERVAL '1' MONTH FOLLOWING
        ),
        2
    ) AS avg_3m
FROM
    (
        SELECT
            c.name AS customer_name,
            CAST(DATE_FORMAT(orderdate, '%y-%M-01') AS DATE) AS order_month,
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

SELECT
    supplier_name,
    order_year,
    total_quantity,
    total_price,
    ROUND(
        AVG(total_price) OVER(
            PARTITION BY supplier_name
            ORDER BY
                total_price RANGE BETWEEN 20 PRECEDING
                AND 20 FOLLOWING
        ),
        2
    ) AS avg_total_price_wi_20_quantity
FROM
    (
        SELECT
            n.name AS supplier_name,
            YEAR(o.orderdate) AS order_year,
            SUM(l.quantity) AS total_quantity,
            ROUND(sum(o.totalprice) / 100000, 2) AS total_price
        FROM
            lineitem l
            JOIN orders o ON l.orderkey = o.orderkey
            JOIN supplier s ON l.suppkey = s.suppkey
            JOIN nation n ON s.nationkey = n.nationkey
        GROUP BY
            n.name,
            YEAR(o.orderdate)
    );

-- Everything below uses GROUPS which is not available in Spark SQL
/*
 SELECT
 customer_name,
 supplier_name,
 order_month,
 total_quantity,
 MAX(total_quantity) OVER(
 PARTITION BY customer_name
 ORDER BY
 order_month GROUPS BETWEEN CURRENT ROW
 AND 3 FOLLOWING
 ) AS max_quantity_over_next_three_months
 FROM
 (
 SELECT
 c.name AS customer_name,
 s.name AS supplier_name,
 CAST(DATE_FORMAT(o.orderdate, '%y-%M-01') AS DATE) AS order_month,
 SUM(l.quantity) AS total_quantity
 FROM
 lineitem l
 JOIN orders o ON l.orderkey = o.orderkey
 JOIN customer c ON o.custkey = c.custkey
 JOIN supplier s ON l.suppkey = s.suppkey
 GROUP BY
 c.name,
 s.name,
 DATE_FORMAT(o.orderdate, '%y-%M-01')
 ORDER BY
 1,
 3
 )
 LIMIT
 50;
 
 SELECT
 customer_nation,
 supplier_nation,
 order_month,
 avg_days_to_deliver,
 MIN(avg_days_to_deliver) OVER(
 PARTITION BY customer_nation
 ORDER BY
 order_month GROUPS BETWEEN 3 PRECEDING
 AND 1 PRECEDING
 ) shortest_avg_days_to_deliver_3mo
 FROM
 (
 SELECT
 cn.name AS customer_nation,
 sn.name AS supplier_nation,
 CAST(DATE_FORMAT(o.orderdate, '%y-%M-01') AS DATE) AS order_month,
 ROUND(
 AVG(DATE_DIFF('day', l.shipdate, l.receiptdate)),
 2
 ) AS avg_days_to_deliver
 FROM
 lineitem l
 JOIN orders o ON l.orderkey = o.orderkey
 JOIN customer c ON o.custkey = c.custkey
 JOIN supplier s ON l.suppkey = s.suppkey
 JOIN nation cn ON c.nationkey = cn.nationkey
 JOIN nation sn ON s.nationkey = sn.nationkey
 GROUP BY
 1,
 2,
 3
 );
 */