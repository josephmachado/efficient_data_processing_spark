from pyspark.sql import SparkSession


def run_code(spark):
    # read a TPCH table
    spark.sql("use tpch")
    # Get orderstatus counts
    orders = spark.sql(
        "select orderstatus, count(*) as cnt from orders GROUP BY orderstatus"
    )

    # Data is not processed yet,
    orders.write.mode("overwrite").parquet(
        "output_directory"
    )  # only when this line is executed data is processed


if __name__ == '__main__':
    spark = (
        SparkSession.builder.appName("adventureworks")
        .enableHiveSupport()
        .getOrCreate()
    )
    run_code(spark=spark)
    spark.stop
