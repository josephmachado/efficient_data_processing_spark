from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, round, sum


def run_code(spark):
    print("==========================================")
    print("Calculate running metrics withing windows")
    print("==========================================")
    customer = spark.table("tpch.customer")
    orders = spark.table("tpch.orders")

    result = (
        orders.join(customer, orders.custkey == customer.custkey)
        .groupBy("customer.name", "orders.custkey", "orders.orderdate")
        .agg(
            col("customer.name").alias("customer_name"),
            col("orders.orderdate").alias("orderdate"),
            round(sum("orders.totalprice"), 2).alias("total_price"),
            round(
                sum(sum("orders.totalprice")).over(
                    Window.partitionBy("orders.custkey").orderBy(
                        "orders.orderdate"
                    )
                ),
                2,
            ).alias("cumulative_sum_total_price"),
        )
        .orderBy("orders.custkey", "orders.orderdate")
        .limit(20)
    )
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
