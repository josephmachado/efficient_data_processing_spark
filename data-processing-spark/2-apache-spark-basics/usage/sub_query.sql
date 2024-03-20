USE tpch;

-- Example
SELECT
    n.name AS nation_name,
    s.quantity AS supplied_items_quantity,
    c.quantity AS purchased_items_quantity
FROM
    nation n
    LEFT JOIN (
        SELECT
            n.nationkey,
            sum(l.quantity) AS quantity
        FROM
            lineitem l
            JOIN supplier s ON l.suppkey = s.suppkey
            JOIN nation n ON s.nationkey = n.nationkey
        GROUP BY
            n.nationkey
    ) s ON n.nationkey = s.nationkey
    LEFT JOIN (
        SELECT
            n.nationkey,
            sum(l.quantity) AS quantity
        FROM
            lineitem l
            JOIN orders o ON l.orderkey = o.orderkey
            JOIN customer c ON o.custkey = c.custkey
            JOIN nation n ON c.nationkey = n.nationkey
        GROUP BY
            n.nationkey
    ) c ON n.nationkey = c.nationkey;