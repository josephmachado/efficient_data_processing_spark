USE tpch;

SELECT
    custkey,
    orderdate,
    format_number(totalprice, 2) AS totalprice,
    RANK() OVER(
        PARTITION BY custkey
        ORDER BY
            totalprice DESC
    ) AS rnk
FROM
    orders
ORDER BY
    custkey,
    rnk
LIMIT
    15;

SELECT
    orderkey,
    discount,
    RANK() OVER(
        PARTITION BY orderkey
        ORDER BY
            discount DESC
    ) AS rnk,
    DENSE_RANK() OVER(
        PARTITION BY orderkey
        ORDER BY
            discount DESC
    ) AS dense_rnk,
    ROW_NUMBER() OVER(
        PARTITION BY orderkey
        ORDER BY
            discount DESC
    ) AS row_num
FROM
    lineitem
WHERE
    orderkey = 42624 -- this is an example orderkey that has multiple discounts of the same value
LIMIT
    10;

SELECT
    *
FROM
    (
        SELECT
            n.name AS supplier_nation,
            YEAR(o.orderdate) AS order_year,
            MONTH(o.orderdate) AS order_month,
            SUM(l.quantity),
            DENSE_RANK() OVER(
                PARTITION BY n.name
                ORDER BY
                    SUM(l.quantity) DESC
            ) AS rnk
        FROM
            orders o
            JOIN lineitem l ON o.orderkey = l.orderkey
            JOIN supplier s ON l.suppkey = s.suppkey
            JOIN nation n ON s.nationkey = n.nationkey
        GROUP BY
            n.name,
            YEAR(o.orderdate),
            MONTH(o.orderdate)
    )
WHERE
    rnk <= 3
ORDER BY
    supplier_nation,
    rnk
LIMIT
    30;