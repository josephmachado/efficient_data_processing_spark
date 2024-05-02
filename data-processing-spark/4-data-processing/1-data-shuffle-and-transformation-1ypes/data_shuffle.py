from pyspark.sql import SparkSession


def run_code(spark):
    print("==========================================")
    print("Shuffling data to check for number of partitions")
    print("==========================================")

    num_partitions = spark.sql(
        "select returnflag, count(*) from tpch.lineitem group by 1"
    ).rdd.getNumPartitions()
    print(
        f"Number of partitions for lineitem dataframe is {num_partitions} and"
        " the default spark partitions(spark.sql.shuffle.partitions) are set"
        f" to {spark.conf.get('spark.sql.shuffle.partitions')}"
    )


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
