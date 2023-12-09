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

WITH supplier_metrics AS (
    SELECT
        p.brand,
        l.suppkey,
        round(
            sum(l.extendedprice * (1 - l.discount) * (1 + l.tax)),
            2
        ) AS supplier_total_sold,
        round(sum(l.extendedprice), 2) AS supplier_total_sold_wo_tax_discounts
    FROM
        lineitem l
        JOIN partsupp ps ON l.partkey = ps.partkey
        AND l.suppkey = ps.suppkey
        JOIN part p ON ps.partkey = p.partkey
    GROUP BY
        p.brand,
        l.suppkey
),
customer_metrics AS (
    SELECT
        p.brand,
        o.custkey,
        round(
            sum(
                l.extendedprice * (1 - l.discount) * (1 + l.tax)
            ),
            2
        ) AS cust_total_spend,
        round(sum(l.extendedprice), 2) AS cust_total_spend_wo_tax_discounts
    FROM
        lineitem l
        JOIN orders o ON l.orderkey = o.orderkey
        JOIN partsupp ps ON l.partkey = ps.partkey
        AND l.suppkey = ps.suppkey
        JOIN part p ON ps.partkey = p.partkey
        JOIN customer c ON o.custkey = c.custkey
    GROUP BY
        p.brand,
        o.custkey
)
SELECT
    p.brand,
    sum(num_customers) AS num_customers,
    sum(num_suppliers) AS num_suppliers,
    sum(s.supplier_total_sold) AS supplier_total_sold,
    sum(s.supplier_total_sold_wo_tax_discounts) AS supplier_total_sold_wo_tax_discounts,
    sum(c.cust_total_spend) AS cust_total_spend,
    sum(c.cust_total_spend_wo_tax_discounts) AS cust_total_spend_wo_tax_discounts
FROM
    part p
    JOIN (
        SELECT
            brand,
            count(suppkey) AS num_suppliers,
            SUM(supplier_total_sold) AS supplier_total_sold,
            SUM(supplier_total_sold_wo_tax_discounts) AS supplier_total_sold_wo_tax_discounts
        FROM
            supplier_metrics
        GROUP BY
            brand
    ) s ON p.brand = s.brand
    JOIN (
        SELECT
            brand,
            count(custkey) AS num_customers,
            SUM(cust_total_spend) AS cust_total_spend,
            SUM(cust_total_spend_wo_tax_discounts) AS cust_total_spend_wo_tax_discounts
        FROM
            customer_metrics
        GROUP BY
            brand
    ) c ON p.brand = c.brand
GROUP BY
    p.brand;

SET
    SESSION max_recursion_depth = 12;

WITH RECURSIVE months_2022(mnth) AS (
    SELECT
        cast('2022-01-01' AS DATE) -- called the base/anchor
    UNION
    ALL
    SELECT
        DATE_ADD('month', 1, mnth) -- called the step
    FROM
        months_2022
    WHERE
        mnth < cast('2022-12-01' AS DATE)
)
SELECT
    *
FROM
    months_2022
ORDER BY
    mnth;

CREATE SCHEMA IF NOT EXISTS minio.warehouse WITH (location = 's3a://warehouse/');

USE minio.warehouse;

DROP TABLE IF EXISTS employee_info;

CREATE TABLE IF NOT EXISTS employee_info(id int, name varchar, reports_to int);

INSERT INTO
    employee_info
VALUES
    (1, 'A', NULL),
    (2, 'B1', 1),
    (3, 'B3', 1),
    (4, 'D1', 2),
    (5, 'D2', 2),
    (6, 'D3', 2),
    (7, 'D4', 3),
    (8, 'D5', 3),
    (9, 'E1', 6),
    (10, 'E2', 6);

WITH RECURSIVE manager_chain(id, emp, path_to_top) AS (
    SELECT
        id,
        ei.name,
        Array [ei.name] AS path_to_top -- called the base/anchor
    FROM
        employee_info ei
    WHERE
        reports_to IS NULL
    UNION
    ALL
    SELECT
        ei.id,
        ei.name,
        Array [ei.name] || mc.path_to_top AS path_to_top -- called the step
    FROM
        employee_info ei
        JOIN manager_chain mc ON ei.reports_to = mc.id -- No results from join = terminate recursive CTE
)
SELECT
    *
FROM
    manager_chain;