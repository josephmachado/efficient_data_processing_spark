from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import (avg, col, count, date_add, lit, round, sum,
                                   year)


def run_code(spark):
    print("==========================================")
    print("Compare column values across rows with Window value functions")
    print("=================================")
    orders = spark.table("tpch.orders")

    # Query 1
    result_df_1 = (
        orders.groupBy(
            F.date_format("orderdate", "yy-MMM").alias("ordermonth")
        )
        .agg(F.round(F.sum("totalprice") / 100000, 2).alias("total_price"))
        .withColumn(
            "prev_month_total_price",
            F.lag("total_price").over(Window.orderBy("ordermonth")),
        )
        .orderBy("ordermonth")
        .limit(10)
    )

    result_df_1.show()


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
