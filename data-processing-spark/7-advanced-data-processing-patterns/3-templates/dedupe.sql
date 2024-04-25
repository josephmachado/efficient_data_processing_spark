USE tpch;

WITH duplicated_orders AS (
    SELECT
        *
    FROM
        orders
    UNION ALL
    SELECT
        *
    FROM
        orders
),
ranked_orders AS (
    SELECT
        *,
        row_number() over(PARTITION by orderkey ORDER BY ORDERKEY) AS rn
    FROM
        orders
)
SELECT
    COUNT(*)
FROM
    ranked_orders
WHERE
    rn = 1;