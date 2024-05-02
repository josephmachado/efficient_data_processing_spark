from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import round, sum


def run_code(spark):
    print("==========================================")
    print("Defining windows")
    print("==========================================")
    lineitem = spark.table("tpch.lineitem")
    # Define the window specification
    window_spec = Window.partitionBy("orderkey").orderBy("linenumber")
    total_extendedprice_prev_2_window = (
        Window.partitionBy("orderkey")
        .orderBy("linenumber")
        .rowsBetween(Window.currentRow - 1, Window.currentRow)
    )

    # Define the PySpark DataFrame API equivalent
    result_df = (
        lineitem.select(
            "orderkey",
            "linenumber",
            "extendedprice",
            round(sum("extendedprice").over(window_spec), 2).alias(
                "total_extendedprice"
            ),
            round(
                sum("extendedprice").over(total_extendedprice_prev_2_window), 2
            ).alias("total_extendedprice_prev_2_lineitems"),
        )
        .orderBy("orderkey", "linenumber")
        .limit(20)
    )

    # Show the result
    result_df.show()


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
