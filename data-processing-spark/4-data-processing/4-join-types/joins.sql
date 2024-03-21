use tpch;

SELECT "***********************BROADCAST HASH JOIN****************";
EXPLAIN
SELECT
    c.name AS customer_name,
    o.orderdate,
    SUM(o.totalprice) AS total_price,
    -- We have it here just for comparison purposes
    ROUND(
        SUM(SUM(o.totalprice)) over(
            PARTITION by o.custkey
            ORDER BY
                o.orderdate
        ),
        2
    ) AS cumulative_sum_total_price
FROM
    orders o
    JOIN customer c ON o.custkey = c.custkey
GROUP BY
    c.name,
    o.custkey,
    o.orderdate
ORDER BY
    o.custkey,
    o.orderdate
LIMIT
    20;
-- Notice the BroadcastHashJoin
-- The customer table being small, it is chose to be BroadcastExchanged to all the nodes that have orders data
-- In the respective nodes orders data is joined with the boradcasted customer table


SELECT "***********************SORT MERGE JOIN****************";
EXPLAIN
SELECT
    YEAR(o.orderdate) AS order_year,
    MONTH(o.orderdate) AS order_month,
    SUM(l.quantity) AS total_qty
FROM
    orders o
    JOIN lineitem l ON o.orderkey = l.orderkey
GROUP BY
    YEAR(o.orderdate),
    MONTH(o.orderdate);
-- In this join we have 2 large tables that are shuffled (exchanged) across node in the cluster
-- sorted within the nodes based on join key (orderkey)
-- The rows are merged by iterating over the rows of the tables

/*
SELECT "We set spark.sql.join.preferSortMergeJoin to false to trigger a SHUFFLE HASH JOIN";
SET spark.sql.join.preferSortMergeJoin=false;

SELECT "***********************SHUFFLE HASH JOIN****************";
EXPLAIN
SELECT /*+ SHUFFLE_HASH(orderdate) */
    YEAR(o.orderdate) AS order_year,
    MONTH(o.orderdate) AS order_month,
    SUM(l.quantity) AS total_qty
FROM
    orders o
    JOIN lineitem l ON o.orderkey = l.orderkey
GROUP BY
    YEAR(o.orderdate),
    MONTH(o.orderdate);
*/

SELECT "*****************BROADCAST NESTEDLOOP JOIN********************"
EXPLAIN 
SELECT n.name, r.name
FROM nation n
CROSS JOIN region r;

-- Both the nation and region tables are small enough to be broadcasted and
-- since we are doing a cross join(every row of nation matched with every row of region)
-- a loop is the appropriate choice for the join 

SELECT "*****************CARTESIAN PRODUCT********************"

EXPLAIN 
SELECT o.orderstatus, l.shipmode
FROM orders o
CROSS JOIN lineitem l;

-- orders and lineitem table are large and the join is a cross join
-- due to the nature of the join we have to do a cartesian product of both the
-- tables 