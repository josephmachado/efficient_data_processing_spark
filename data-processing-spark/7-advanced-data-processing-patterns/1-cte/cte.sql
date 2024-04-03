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


-- Brand level metrics
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
