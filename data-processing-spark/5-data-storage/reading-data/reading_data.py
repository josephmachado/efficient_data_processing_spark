from pyspark.sql import SparkSession


def run_code(spark):
    print("==========================================")
    print("Reading data from external cloud storage system")
    print("==========================================")
    # Write a sample of orders data into S3 for this example
    spark.sql("select * from tpch.orders limit 500").write.mode(
        "overwrite"
    ).parquet("s3a://tpch/sample/orders.parquet")
    print("Read Parquet data from S3 into a DataFrame")
    df = spark.read.parquet("s3a://tpch/sample/orders.parquet")

    print("Show the DataFrame schema and some sample data")
    df.show()

    print("==========================================")
    print("Reading data from local file system")
    print("==========================================")
    print("Get the local file location for our orders table")
    spark.sql("DESCRIBE EXTENDED tpch.orders").show(n=100, truncate=False)
    print("Serialized orders data")
    spark.read.csv("s3a://tpch/orders").show()


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
