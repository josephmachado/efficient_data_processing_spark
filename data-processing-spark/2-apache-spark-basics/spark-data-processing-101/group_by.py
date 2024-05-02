from pyspark.sql import SparkSession
from pyspark.sql.functions import count


def run_code(spark):
    print("=================================")
    print("Generate metrics for your dimension(s) using GROUP BY")
    print("=================================")
    # Read the 'orders' table
    spark.sql("USE tpch")
    orders = spark.table("orders")

    print("=================================")
    print("Perform aggregation using DataFrame API")
    print("=================================")
    result = (
        orders.groupBy("orderpriority")
        .agg(count("*").alias("num_orders"))
        .orderBy("orderpriority")
    )

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
