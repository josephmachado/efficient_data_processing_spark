from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import round, sum


def run_code(spark):
    print("==========================================")
    print("Defining windows")
    print("==========================================")
    lineitem = spark.table("tpch.lineitem")
    # Define the window specification
    window_spec = Window.partitionBy("orderkey").orderBy("linenumber")

    # Define the PySpark DataFrame API equivalent
    result_df = (
        lineitem.select(
            "orderkey",
            "linenumber",
            "extendedprice",
            round(sum("extendedprice").over(window_spec), 2).alias(
                "total_extendedprice"
            ),
        )
        .orderBy("orderkey", "linenumber")
        .limit(20)
    )

    # Show the result
    result_df.show()


if __name__ == '__main__':
    spark = (
        SparkSession.builder.appName("adventureworks")
        .enableHiveSupport()
        .getOrCreate()
    )
    # Set the log level
    spark.sparkContext.setLogLevel("ERROR")
    run_code(spark=spark)
    spark.stop
