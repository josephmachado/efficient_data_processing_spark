from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import col


def run_code(spark):
    print("==========================================")
    print("Choose ROWS to apply functions to within a window frame")
    print("=================================")
    orders = spark.table("tpch.orders")
    customer = spark.table("tpch.customer")

    # Query 1
    result_df_1 = (
        orders.join(customer, orders.custkey == customer.custkey)
        .groupBy(
            col("customer.name").alias("customer_name"),
            F.date_format("orderdate", "yy-MMM").alias("order_month"),
        )
        .agg(F.sum("orders.totalprice").alias("total_price"))
        .withColumn(
            "three_mo_total_price_avg",
            F.round(
                F.avg("total_price").over(
                    Window.partitionBy("customer_name")
                    .orderBy("order_month")
                    .rowsBetween(-1, 1)
                ),
                2,
            ),
        )
        .orderBy("customer_name", "order_month")
        .limit(50)
    )

    result_df_1.show()

    print("==========================================")
    print("Choose INTERVALS to apply functions to within a window frame")
    print("=================================")
    result_df_2 = (
        orders.join(customer, orders.custkey == customer.custkey)
        .groupBy(
            col("customer.name").alias("customer_name"),
            F.expr("CAST(DATE_FORMAT(orderdate, 'yy-M-01') AS DATE)").alias(
                "order_month"
            ),
        )
        .agg(F.sum("orders.totalprice").alias("total_price"))
        .withColumn(
            "order_month_timestamp",
            F.unix_timestamp("order_month", format="yy-MM-dd").cast("bigint"),
        )
        .withColumn(
            "avg_3m_all",
            F.round(
                F.avg("total_price").over(
                    Window.partitionBy("customer_name")
                    .orderBy("order_month_timestamp")
                    .rowsBetween(-1, 1)
                ),
                2,
            ),
        )
        .withColumn(
            "avg_3m",
            F.round(
                F.avg("total_price").over(
                    Window.partitionBy("customer_name")
                    .orderBy("order_month_timestamp")
                    .rangeBetween(-1, 1)
                ),
                2,
            ),
        )
        .orderBy("customer_name", "order_month")
        .limit(50)
    )

    result_df_2.show()


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
