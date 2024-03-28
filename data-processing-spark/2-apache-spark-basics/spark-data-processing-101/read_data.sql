USE tpch;

-- use * to specify all columns
SELECT * FROM orders LIMIT 10;

-- use column names only to read data from those columns
SELECT orderkey, totalprice FROM orders LIMIT 10;