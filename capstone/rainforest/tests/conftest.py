import pyspark
import pytest
from delta import configure_spark_with_delta_pip


@pytest.fixture(scope='session')
def spark():
    my_packages = [
        "io.delta:delta-core_2.12:2.3.0",
        "org.apache.hadoop:hadoop-aws:3.3.2",
        "org.postgresql:postgresql:42.7.3",
    ]

    builder = (
        pyspark.sql.SparkSession.builder.appName("rainforest_tests")
        .master("local[1]")
        .config("spark.rdd.compress", "false")
        .config("spark.shuffle.compress", "false")
        .config("spark.dynamicAllocation.enabled", "false")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.default.parallelism", 6) # my laptop has 6 cores
        .config("spark.executor.cores", "1")
        .config("spark.executor.instances", "1")
        .config("spark.sql.shuffle.partitions", "1")
        .config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.master", "spark://spark-master:7077")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "/opt/spark/spark-events")
        .config("spark.history.fs.logDirectory", "/opt/spark/spark-events")
        .config("spark.ui.prometheus.enabled", "true")
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "minio123")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.region", "us-east-1")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.databricks.delta.retentionDurationCheck.enabled", "false"
        )
        .config("spark.sql.codegen.wholeStage", "false")
        .enableHiveSupport()
    )

    spark = configure_spark_with_delta_pip(
        builder, extra_packages=my_packages
    ).getOrCreate()

    yield spark
    spark.stop()
