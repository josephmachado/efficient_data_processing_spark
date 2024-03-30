use tpch;

SELECT "***********************BROADCAST HASH JOIN****************";
EXPLAIN
SELECT
    c.name AS customer_name,
    o.orderdate
   FROM
    orders o
    JOIN customer c ON o.custkey = c.custkey;
-- Notice the BroadcastHashJoin
-- The customer table being small, it is chose to be BroadcastExchanged to all the nodes that have orders data
-- In the respective nodes orders data is joined with the boradcasted customer table


SELECT "***********************SORT MERGE JOIN****************";
EXPLAIN
SELECT
    o.orderdate,
    l.linestatus
FROM
    orders o
    JOIN lineitem l ON o.orderkey = l.orderkey;
-- In this join we have 2 large tables that are shuffled (exchanged) across node in the cluster
-- sorted within the nodes based on join key (orderkey)
-- The rows are merged by iterating over the rows of the tables

SELECT "***********************SHUFFLE HASH JOIN****************";
EXPLAIN SELECT /*+ SHUFFLE_HASH(l) */ l.linestatus
, ps.supplycost
FROM lineitem l
JOIN partsupp ps
ON l.partkey = ps.partkey AND l.suppkey = ps.suppkey; 


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