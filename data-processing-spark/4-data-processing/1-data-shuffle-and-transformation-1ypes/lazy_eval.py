from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def run_code(spark):
    print("Standard python data processing")
    print([i for i in range(10)])
    # Get orderstatus counts
    orders = spark.sql(
        "select orderstatus, count(*) as cnt from tpch.orders GROUP BY"
        " orderstatus"
    )

    # Data is not processed yet,
    orders.write.mode("overwrite").parquet(
        "output_directory"
    )  # only when this line is executed data is processed

    lineitem = spark.sql(
        """select linestatus,
    count(*) as cnt
     from tpch.lineitem
     GROUP BY linestatus
    """
    )
    filtered_lineitem = lineitem.filter(col("linestatus") != "O")

    filtered_lineitem.explain()


if __name__ == '__main__':
    spark = (
        SparkSession.builder.appName("efficient-data-processing-spark")
        .enableHiveSupport()
        .getOrCreate()
    )
    run_code(spark=spark)
    spark.stop()
