USE tpch.tiny;

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
        row_number() over(PARTITION by orderkey) AS rn
    FROM
        orders
)
SELECT
    COUNT(*)
FROM
    ranked_orders
WHERE
    rn = 1;