from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number


def run_code(spark):
    print("==========================================")
    print("De-duplicate rows with dropDuplicates function")
    print("=================================")
    spark.sql("USE tpch")
    # Load data into DataFrames
    orders = spark.table("orders")

    # Duplicate Orders
    duplicated_orders = orders.union(orders)
    deduped_orders = duplicated_orders.dropDuplicates(['orderkey'])

    print(
        f"Count of ranked orders with row number is {deduped_orders.count()} and"
        f" original count of orders table is {orders.count()}"
    )


if __name__ == '__main__':
    spark = (
        SparkSession.builder.appName("efficient-data-processing-spark")
        .enableHiveSupport()
        .getOrCreate()
    )
    # Set the log level
    spark.sparkContext.setLogLevel("ERROR")
    run_code(spark=spark)
    spark.stop
