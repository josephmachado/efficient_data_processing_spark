from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, datediff, lit


def run_code(spark):
    print("=================================")
    print(
        "This is supposed to fail, but spark makes it work and generate a"
        " NULL! Which is not what we expect"
    )
    print("=================================")
    df_invalid_date_diff = spark.createDataFrame(
        [("2022-10-01", "invalid_date")], ["date1", "date2"]
    )
    df_result_1 = df_invalid_date_diff.withColumn(
        "diff", datediff("date1", "date2")
    )
    df_result_1.show()

    df_result_2 = (
        df_invalid_date_diff.withColumn(
            "date1", df_invalid_date_diff["date1"].cast("date")
        )
        .withColumn("date2", df_invalid_date_diff["date2"].cast("date"))
        .withColumn("diff", datediff("date1", "date2"))
    )
    df_result_2.show()

    orders = spark.table("tpch.orders")
    lineitem = spark.table("tpch.lineitem")

    print("=================================")
    print("Coalesce to default value in case of Null")
    print("=================================")
    df_join = (
        orders.alias("o")
        .join(
            lineitem.alias("l"),
            (col("o.orderkey") == col("l.orderkey"))
            & (
                col("o.orderdate").between(
                    col("l.shipdate") - 5, col("l.shipdate") + 5
                )
            ),
            "left",
        )
        .select(
            col("o.orderkey"),
            col("o.orderdate"),
            coalesce(col("l.orderkey"), lit(9999999)).alias(
                "lineitem_orderkey"
            ),
            col("l.shipdate"),
        )
        .filter(col("l.shipdate").isNull())
        .limit(10)
    )

    # Display the result
    df_join.show()


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
