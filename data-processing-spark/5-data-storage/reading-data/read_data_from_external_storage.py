from pyspark.sql import SparkSession


def run_code(spark):
    print("==========================================")
    print(
        "Credentials to connect to S3(we use minio) has already been set in"
        " spark-defaults.conf"
    )
    print("==========================================")

    # Write a sample of orders data into S3 for this example

    spark.sql("select * from tpch.orders limit 500").write.parquet(
        "s3a://tpch/sample/orders.parquet"
    )
    print("Read Parquet data from S3 into a DataFrame")
    df = spark.read.parquet("s3a://tpch/sample/orders.parquet")

    print("Show the DataFrame schema and some sample data")
    df.printSchema()
    df.show()


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
