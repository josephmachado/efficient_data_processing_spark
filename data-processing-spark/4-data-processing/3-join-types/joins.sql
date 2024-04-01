use tpch;

SELECT "***********************BROADCAST HASH JOIN****************";
EXPLAIN
SELECT 
s.name AS supplier_name,
l.linenumber,
ROUND(l.extendedprice * (1 - l.discount), 2) AS total_price_wo_tax
FROM lineitem l
JOIN supplier s ON l.suppkey = s.suppkey;
-- Notice the BroadcastHashJoin
-- The supplier table being small, it is chose to be BroadcastExchanged to all the nodes that have lineitem data
-- In the respective nodes lineitem data is joined with the broaddcasted supplier table


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
EXPLAIN SELECT /*+ SHUFFLE_HASH(ps) */ l.linestatus
, ps.supplycost
FROM partsupp ps
JOIN lineitem l
ON l.partkey = ps.partkey AND l.suppkey = ps.suppkey; 

SELECT "*****************BROADCAST NESTEDLOOP JOIN********************"
EXPLAIN
SELECT 
s.name AS supplier_name,
l.linenumber,
ROUND(l.extendedprice * (1 - l.discount), 2) AS total_price_wo_tax
FROM lineitem l
JOIN supplier s ON l.suppkey >= s.suppkey;
-- The query plan shows a `BuildRight` which means that the right table (Supplier) is broadcasted
-- After broadcast there is a nested loop join. We cannot use a broadcast hash join due to the type of join criteria
-- Since Hash join requires ability to match data based on equality of hash values and that is not possible with a non-equi join (>=) 
-- Note: this is the same query we used to show broadcast hash join, but with a non-equi join (>=) type

SELECT "*****************CARTESIAN PRODUCT********************"

EXPLAIN 
SELECT o.orderstatus, l.shipmode
FROM orders o
CROSS JOIN lineitem l;

-- orders and lineitem table are large and the join is a cross join
-- due to the nature of the join we have to do a cartesian product of both the
-- tables 

SELECT "*****************JOIN HINTS********************"

EXPLAIN SELECT l.linestatus
, ps.supplycost
FROM partsupp ps
JOIN lineitem l
ON l.partkey = ps.partkey AND l.suppkey = ps.suppkey; 
-- SortMergeJoin

EXPLAIN SELECT  /*+ BROADCAST(ps) */ l.linestatus
, ps.supplycost
FROM partsupp ps
JOIN lineitem l
ON l.partkey = ps.partkey AND l.suppkey = ps.suppkey; 
-- this will trigger a broadcast join with the partsupp table as the build side

EXPLAIN SELECT  /*+ SHUFFLE_REPLICATE_NL(ps) */ l.linestatus
, ps.supplycost
FROM partsupp ps
JOIN lineitem l
ON l.partkey = ps.partkey AND l.suppkey = ps.suppkey; 
-- this will trigger a Shuffle replicate (aka cartesian product join) join
