from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, round, sum


def run_code(spark):
    print("==========================================")
    print("PIVOT data to show group by categories as columns")
    print("=================================")
    # Switch to the tpch database
    spark.sql("USE tpch")

    # Read data from the orders table
    orders_df = spark.table("orders")

    # Select relevant columns from orders_df
    order_data = orders_df.groupBy("orderdate", "orderpriority").agg(
        sum("totalprice").alias("totalprice")
    )
    # orders_df.select("orderdate", "totalprice", "orderpriority")

    # Pivot the order_data DataFrame
    pivot_result = (
        order_data.groupBy("orderdate")
        .pivot("orderpriority")
        .agg(round(avg("totalprice"), 2).alias("avg_price"))
        .select(
            "orderdate",
            col("1-URGENT").alias("urgent_order"),
            col("2-HIGH").alias("high_order"),
            col("3-MEDIUM").alias("medium_order"),
            col("4-NOT SPECIFIED").alias("not_specified_order"),
            col("5-LOW").alias("low_order"),
        )
        .limit(10)
    )

    # Show the pivoted DataFrame
    pivot_result.show()


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
