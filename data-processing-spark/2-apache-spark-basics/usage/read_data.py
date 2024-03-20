from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def run_code(spark):
    # Read the table
    orders_df = spark.table("tpch.orders")

    # Show the first 10 rows with all columns
    orders_df.show(10)

    # Select specific columns and show the first 10 rows
    orders_df.select("orderkey", "totalprice").show(10)

    ######## FILTER 

    # Read all columns from the 'customer' table where 'nationkey' is 20, limit to 10 rows
    customer_nation_20 = (
        spark.table("customer").filter(col("nationkey") == 20).limit(10)
    )
    customer_nation_20.show()

    # Read all columns from the 'customer' table where 'nationkey' is 20 and 'acctbal' > 1000, limit to 10 rows
    customer_nation_20_acctbal_gt_1000 = (
        spark.table("customer")
        .filter((col("nationkey") == 20) & (col("acctbal") > 1000))
        .limit(10)
    )
    customer_nation_20_acctbal_gt_1000.show()

    # Read all columns from the 'customer' table where 'nationkey' is 20 or 'acctbal' > 1000, limit to 10 rows
    customer_nation_20_or_acctbal_gt_1000 = (
        spark.table("customer")
        .filter((col("nationkey") == 20) | (col("acctbal") > 1000))
        .limit(10)
    )
    customer_nation_20_or_acctbal_gt_1000.show()

    # Read all columns from the 'customer' table where ('nationkey' is 20 and 'acctbal' > 1000) or 'nationkey' is 11, limit to 10 rows
    customer_nation_20_acctbal_gt_1000_or_nationkey_11 = (
        spark.table("customer")
        .filter(
            ((col("nationkey") == 20) & (col("acctbal") > 1000))
            | (col("nationkey") == 11)
        )
        .limit(10)
    )
    customer_nation_20_acctbal_gt_1000_or_nationkey_11.show()

    # Read all columns from the 'customer' table where 'nationkey' is in (10, 20)
    customer_nation_10_or_20 = spark.table("customer").filter(
        col("nationkey").isin(10, 20)
    )
    customer_nation_10_or_20.show()

    # Read all columns from the 'customer' table where 'nationkey' is not in (10, 20)
    customer_nation_not_10_or_20 = spark.table("customer").filter(
        ~col("nationkey").isin(10, 20)
    )
    customer_nation_not_10_or_20.show()

    # Count the number of rows in the 'customer' table
    customer_count = spark.table("customer").count()
    print("Count of rows in 'customer' table:", customer_count)

    # Count the number of rows in the 'lineitem' table
    lineitem_count = spark.table("lineitem").count()
    print("Count of rows in 'lineitem' table:", lineitem_count)

    # Order by 'custkey' in ascending order and limit to 10 rows
    orders_ordered_by_custkey_asc = (
        spark.table("orders").orderBy("custkey").limit(10)
    )
    orders_ordered_by_custkey_asc.show()

    # Order by 'custkey' in descending order and limit to 10 rows
    orders_ordered_by_custkey_desc = (
        spark.table("orders").orderBy(col("custkey").desc()).limit(10)
    )
    orders_ordered_by_custkey_desc.show()


if __name__ == '__main__':
    spark = (
        SparkSession.builder.appName("adventureworks")
        .enableHiveSupport()
        .getOrCreate()
    )
    run_code(spark=spark)
    spark.stop