use tpch;

SELECT 
    l.*,
    c.custkey AS customerkey,
    c.name AS customername,
    c.address AS customeraddress,
    c.nationkey AS customernationkey,
    c.phone AS customerphone,
    c.acctbal AS customeraccountbal,
    c.mktsegment AS customermktsegment,
    o.orderkey AS orderkey,
    o.custkey AS ordercustomerkey,
    o.orderstatus AS orderstatus,
    o.totalprice AS ordertotalprice,
    o.orderdate AS orderdate,
    o.orderpriority AS orderpriority,
    o.clerk AS clerk,
    o.shippriority AS shippriority,
    o.comment AS ordercomment,
    p.partkey AS partkey,
    p.name AS partname,
    p.mfgr AS manufacturer,
    p.brand AS brand,
    p.type AS parttype,
    p.size AS partsize,
    p.container AS container,
    p.retailprice AS retailprice,
    p.comment AS partcomment,
    s.suppkey AS suppkey,
    s.name AS suppliername,
    s.address AS supplieraddress,
    s.nationkey AS suppliernationkey,
    s.phone AS supplierphone,
    s.acctbal AS supplieracctbal,
    s.comment AS suppliercomment,
    ps.availability AS availqty,
    ps.supplycost AS supplycost,
    ps.comment AS pscomment
FROM 
    lineitem l
LEFT JOIN 
    orders o ON l.orderkey = o.orderkey
LEFT JOIN 
    customer c ON o.custkey = c.custkey
LEFT JOIN 
    partsupp ps ON l.partkey = ps.partkey AND l.suppkey = ps.suppkey
LEFT JOIN 
    part p ON ps.partkey = p.partkey
LEFT JOIN 
    supplier s ON ps.suppkey = s.suppkey
LIMIT 1000;
