from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when


def run_code(spark):
    print("=================================")
    print("Replicate IF.ELSE logic with CASE statements")
    print("=================================")
    spark.sql("USE tpch")
    # Read the 'orders' table
    orders = spark.table("orders")

    # Apply the CASE statement using PySpark DataFrame API
    result = orders.select(
        col("orderkey"),
        col("totalprice"),
        when(col("totalprice") > 100000, "high")
        .when(
            (col("totalprice") >= 25000) & (col("totalprice") <= 100000),
            "medium",
        )
        .otherwise("low")
        .alias("order_price_bucket"),
    ).limit(20)

    # Show the result
    result.show()


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
