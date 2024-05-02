from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def run_code(spark):
    print("=================================")
    print(
        "Stack tables on top of each other with UNION and UNION ALL,  subtract"
        " tables with EXCEPT "
    )
    print("=================================")
    spark.sql("USE tpch")

    customer = spark.table("customer")

    print("=================================")
    print("SELECT with LIKE condition")
    print("=================================")

    result1 = customer.select("custkey", "name").filter(
        col("name").like("%_91%")
    )
    result1.show()

    print("=================================")
    print("UNION")
    print("=================================")
    result2 = (
        customer.select("custkey", "name")
        .filter(col("name").like("%_91%"))
        .union(
            customer.select("custkey", "name").filter(
                col("name").like("%_91%")
            )
        )
        .union(
            customer.select("custkey", "name").filter(
                col("name").like("%_91%")
            )
        )
    )
    result2.show()

    print("=================================")
    print("UNION ALL")
    print("=================================")
    result3 = (
        customer.select("custkey", "name")
        .filter(col("name").like("%_91%"))
        .unionAll(
            customer.select("custkey", "name").filter(
                col("name").like("%_91%")
            )
        )
        .unionAll(
            customer.select("custkey", "name").filter(
                col("name").like("%_91%")
            )
        )
    )
    result3.show()

    print("=================================")
    print("EXCEPT")
    print("=================================")
    result4 = (
        customer.select("custkey", "name")
        .filter(col("name").like("%_91%"))
        .exceptAll(
            customer.select("custkey", "name").filter(
                col("name").like("%_91%")
            )
        )
    )
    result4.show()

    print("=================================")
    print("EXCEPT with different LIKE condition")
    print("=================================")

    result5 = (
        customer.select("custkey", "name")
        .filter(col("name").like("%_91%"))
        .exceptAll(
            customer.select("custkey", "name").filter(
                col("name").like("%191%")
            )
        )
    )
    result5.show()


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
