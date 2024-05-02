from pyspark.sql import SparkSession


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
        "Count of ranked orders with row number is"
        f" {deduped_orders.count()} and original count of orders table is"
        f" {orders.count()}"
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
    spark.stop()
