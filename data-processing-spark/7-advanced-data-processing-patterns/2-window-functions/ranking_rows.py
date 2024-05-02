from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


def run_code(spark):
    print("==========================================")
    print("Rank rows based on column(s) with Window ranking functions")
    print("=================================")
    orders = spark.table("tpch.orders")
    lineitem = spark.table("tpch.lineitem")

    # Query 1
    result_df_1 = (
        orders.withColumn("totalprice", F.format_number("totalprice", 2))
        .withColumn(
            "rnk",
            F.rank().over(
                Window.partitionBy("custkey").orderBy(F.desc("totalprice"))
            ),
        )
        .orderBy("custkey", "rnk")
        .limit(15)
    )

    result_df_1.show()

    # Query 2
    result_df_2 = (
        lineitem.filter("orderkey = 42624")
        .withColumn(
            "rnk",
            F.rank().over(
                Window.partitionBy("orderkey").orderBy(F.desc("discount"))
            ),
        )
        .withColumn(
            "dense_rnk",
            F.dense_rank().over(
                Window.partitionBy("orderkey").orderBy(F.desc("discount"))
            ),
        )
        .withColumn(
            "row_num",
            F.row_number().over(
                Window.partitionBy("orderkey").orderBy(F.desc("discount"))
            ),
        )
        .limit(10)
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
