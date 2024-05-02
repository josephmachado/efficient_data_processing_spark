from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum


def run_code(spark):
    print("=======================================")
    print("Using tpch database")
    print("=======================================")
    spark.sql("USE tpch")

    print("=======================================")
    print("EXPLAIN query")
    print("=======================================")
    spark.sql(
        """
        EXPLAIN
        SELECT
            o.orderkey,
            SUM(l.extendedprice * (1 - l.discount)) AS total_price_wo_tax
        FROM
            lineitem l
            JOIN orders o ON l.orderkey = o.orderkey
        GROUP BY
            o.orderkey
    """
    ).show(truncate=False)

    print("=======================================")
    print("SELECT query")
    print("=======================================")
    result_df = (
        spark.table("lineitem")
        .alias("l")
        .join(spark.table("orders").alias("o"), "orderkey")
        .groupBy("o.orderkey")
        .agg(
            (sum(col("l.extendedprice") * (1 - col("l.discount")))).alias(
                "total_price_wo_tax"
            )
        )
    )
    result_df.show()

    print("=======================================")
    print("EXPLAIN EXTENDED query")
    print("=======================================")
    spark.sql(
        """
        EXPLAIN EXTENDED
        SELECT
            o.orderkey,
            SUM(l.extendedprice * (1 - l.discount)) AS total_price_wo_tax
        FROM
            lineitem l
            JOIN orders o ON l.orderkey = o.orderkey
        GROUP BY
            o.orderkey
    """
    ).show(truncate=False)


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
