import os

from pyspark.sql import SparkSession


def run_code(spark):
    print("==========================================")
    print("Adaptive Query Execution")
    print("==========================================")
    spark.sql("set spark.sql.adaptive.enabled").show(truncate=False)

    print("==========================================")
    print("AQE Writing files of similar size")
    print("==========================================")
    lineitem = spark.table("tpch.lineitem")
    lineitem.write.mode("overwrite").csv("price_metrics")
    print(os.popen("ls -lh price_metrics/").read())


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
