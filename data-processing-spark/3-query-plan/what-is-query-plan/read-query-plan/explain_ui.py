from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum


def run_code(spark):
    spark.sql("USE tpch")
    print("=======================================")
    print("SELECT query")
    print("=======================================")
    result_df = (
        spark.table("lineitem")
        .alias("l")
        .join(spark.table("orders").alias("o"), "orderkey")
        .groupBy("o.orderkey")
        .agg((sum(col("l.extendedprice") * (1 - col("l.discount")))))
    )
    result_df.show()


if __name__ == '__main__':
    spark = (
        SparkSession.builder.appName("efficient-data-processing-spark")
        .enableHiveSupport()
        .getOrCreate()
    )
    # Set the log level
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    run_code(spark=spark)
    spark.stop()
