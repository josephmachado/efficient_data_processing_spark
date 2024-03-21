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

