use tpch;
desc extended lineitem; -- see data size

analyze table lineitem compute statistics; -- scans table to identify number of rows
desc extended lineitem; -- see table size and number of rows

desc extended lineitem extendedprice; -- will be all NULLs
analyze table lineitem compute statistics for all columns; -- scans table and identifies statistics for every column 
desc extended lineitem extendedprice; -- information about min,max, col len, etc will be filled out
desc extended lineitem linestatus; 
