
    DROP TABLE IF EXISTS customer; 
    
    CREATE TABLE customer (
        
        c_custkey Long,
        
        c_name String,
        
        c_address String,
        
        c_nationkey Long,
        
        c_phone String,
        
        c_acctbal Double,
        
        c_mktsegment String,
        
        c_comment String
        
    )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ESCAPED BY '"'
    STORED AS TEXTFILE;
    
    LOAD DATA LOCAL INPATH '/opt/spark/work-dir/tpch-dbgen/customer.tbl' OVERWRITE INTO TABLE customer;
    
    DROP TABLE IF EXISTS lineitem; 
    
    CREATE TABLE lineitem (
        
        l_orderkey Long,
        
        l_partkey Long,
        
        l_suppkey Long,
        
        l_linenumber Long,
        
        l_quantity Double,
        
        l_extendedprice Double,
        
        l_discount Double,
        
        l_tax Double,
        
        l_returnflag String,
        
        l_linestatus String,
        
        l_shipdate String,
        
        l_commitdate String,
        
        l_receiptdate String,
        
        l_shipinstruct String,
        
        l_shipmode String,
        
        l_comment String
        
    )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ESCAPED BY '"'
    STORED AS TEXTFILE;
    
    LOAD DATA LOCAL INPATH '/opt/spark/work-dir/tpch-dbgen/lineitem.tbl' OVERWRITE INTO TABLE lineitem;
    
    DROP TABLE IF EXISTS nation; 
    
    CREATE TABLE nation (
        
        n_nationkey Long,
        
        n_name String,
        
        n_regionkey Long,
        
        n_comment String
        
    )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ESCAPED BY '"'
    STORED AS TEXTFILE;
    
    LOAD DATA LOCAL INPATH '/opt/spark/work-dir/tpch-dbgen/nation.tbl' OVERWRITE INTO TABLE nation;
    
    DROP TABLE IF EXISTS orders; 
    
    CREATE TABLE orders (
        
        o_orderkey Long,
        
        o_custkey Long,
        
        o_orderstatus String,
        
        o_totalprice Double,
        
        o_orderdate String,
        
        o_orderpriority String,
        
        o_clerk String,
        
        o_shippriority Long,
        
        o_comment String
        
    )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ESCAPED BY '"'
    STORED AS TEXTFILE;
    
    LOAD DATA LOCAL INPATH '/opt/spark/work-dir/tpch-dbgen/orders.tbl' OVERWRITE INTO TABLE orders;
    
    DROP TABLE IF EXISTS part; 
    
    CREATE TABLE part (
        
        p_partkey Long,
        
        p_name String,
        
        p_mfgr String,
        
        p_brand String,
        
        p_type String,
        
        p_size Long,
        
        p_container String,
        
        p_retailprice Double,
        
        p_comment String
        
    )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ESCAPED BY '"'
    STORED AS TEXTFILE;
    
    LOAD DATA LOCAL INPATH '/opt/spark/work-dir/tpch-dbgen/part.tbl' OVERWRITE INTO TABLE part;
    
    DROP TABLE IF EXISTS partsupp; 
    
    CREATE TABLE partsupp (
        
        ps_partkey Long,
        
        ps_suppkey Long,
        
        ps_availability Long,
        
        ps_supplycost Double,
        
        ps_comment String
        
    )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ESCAPED BY '"'
    STORED AS TEXTFILE;
    
    LOAD DATA LOCAL INPATH '/opt/spark/work-dir/tpch-dbgen/partsupp.tbl' OVERWRITE INTO TABLE partsupp;
    
    DROP TABLE IF EXISTS region; 
    
    CREATE TABLE region (
        
        r_regionkey Long,
        
        r_name String,
        
        r_comment String
        
    )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ESCAPED BY '"'
    STORED AS TEXTFILE;
    
    LOAD DATA LOCAL INPATH '/opt/spark/work-dir/tpch-dbgen/region.tbl' OVERWRITE INTO TABLE region;
    
    DROP TABLE IF EXISTS supplier; 
    
    CREATE TABLE supplier (
        
        s_suppkey Long,
        
        s_name String,
        
        s_address String,
        
        s_nationkey Long,
        
        s_phone String,
        
        s_acctbal Double,
        
        s_comment String
        
    )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ESCAPED BY '"'
    STORED AS TEXTFILE;
    
    LOAD DATA LOCAL INPATH '/opt/spark/work-dir/tpch-dbgen/supplier.tbl' OVERWRITE INTO TABLE supplier;
    