USE tpch;

WITH supplier_nation_metrics AS (
    SELECT
        n.nationkey,
        SUM(l.QUANTITY) AS num_supplied_parts
    FROM
        lineitem l
        JOIN supplier s ON l.suppkey = s.suppkey
        JOIN nation n ON s.nationkey = n.nationkey
    GROUP BY
        n.nationkey
),
buyer_nation_metrics AS (
    SELECT
        n.nationkey,
        SUM(l.QUANTITY) AS num_purchased_parts
    FROM
        lineitem l
        JOIN orders o ON l.orderkey = o.orderkey
        JOIN customer c ON o.custkey = c.custkey
        JOIN nation n ON c.nationkey = n.nationkey
    GROUP BY
        n.nationkey
)
SELECT
    n.name AS nation_name,
    s.num_supplied_parts,
    b.num_purchased_parts,
    ROUND(
        CAST(
            s.num_supplied_parts / b.num_purchased_parts AS DECIMAL(10, 2)
        ),
        2
    ) * 100 AS sold_to_purchase_perc
FROM
    nation n
    LEFT JOIN supplier_nation_metrics s ON n.nationkey = s.nationkey
    LEFT JOIN buyer_nation_metrics b ON n.nationkey = b.nationkey;

