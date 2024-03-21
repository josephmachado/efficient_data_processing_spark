from pyspark.sql import SparkSession
from pyspark.sql.functions import round, sum, col, count, date_add, lit, year, avg, row_number
from pyspark.sql import Window, functions as F


def run_code(spark):
    print("==========================================")
    print("De-duplicate rows with window function")
    print("=================================")
    spark.sql("USE tpch")
    # Load data into DataFrames
    orders = spark.table("orders")
    
    # Duplicate Orders
    duplicated_orders = orders.union(orders)

    # Count orders with row number = 1
    count_ranked_orders = (
        duplicated_orders.withColumn(
            "rn",
            row_number().over(Window.partitionBy("orderkey").orderBy("orderdate")),
        )
        .filter(col("rn") == 1)
        .count()
    )

    print(f"Count of ranked orders with row number is {count_ranked_orders} and original count of orders table is {orders.count()}")

if __name__ == '__main__':
    spark = (
        SparkSession.builder.appName("adventureworks")
        .enableHiveSupport()
        .getOrCreate()
    )
    # Set the log level
    spark.sparkContext.setLogLevel("ERROR")
    run_code(spark=spark)
    spark.stop

