from pyspark.sql import SparkSession
from pyspark.sql.functions import round, sum, col, concat, lag, year, month, lit
from pyspark.sql import Window, functions as F


def run_code(spark):
    print("==========================================")
    print("Compute Period over Period metrics, typical to track company KPIs")
    print("=================================")
    # Switch to the tpch.tiny database
    spark.sql("USE tpch")

    # Calculate monthly total price
    monthly_orders = spark.table("orders") \
        .withColumn("ordermonth", concat(year(col("orderdate")), lit('-'), month(col("orderdate")))) \
        .groupBy("ordermonth") \
        .agg(round(sum("totalprice") / 100000, 2).alias("totalprice"))

    # Calculate month-over-month total price change
    window_spec = Window.orderBy("ordermonth")
    monthly_orders_with_change = monthly_orders \
        .withColumn("MoM_totalprice_change", round(((col("totalprice") - lag("totalprice").over(window_spec)) * 100) / lag("totalprice").over(window_spec), 2)) \
        .orderBy("ordermonth") \
        .select("ordermonth", "totalprice", "MoM_totalprice_change")

    # Show the result
    monthly_orders_with_change.show()

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

