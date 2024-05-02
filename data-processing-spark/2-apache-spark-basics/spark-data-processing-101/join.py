from pyspark.sql import SparkSession
from pyspark.sql.functions import col, weekofyear, year


def run_code(spark):
    print("=================================")
    print("Combine data from multiple tables using JOINs")
    print("=================================")
    spark.sql("USE tpch")
    # Read tables
    orders = spark.table("orders")
    lineitem = spark.table("lineitem")
    nation = spark.table("nation")
    region = spark.table("region")

    print("======================================")
    print("INNER JOIN")
    print("======================================")
    inner_join_result = (
        orders.alias("o")
        .join(
            lineitem.alias("l"),
            (col("o.orderkey") == col("l.orderkey"))
            & (
                col("o.orderdate").between(
                    col("l.shipdate") - 5, col("l.shipdate") + 5
                )
            ),
        )
        .select("o.orderkey", "l.orderkey")
        .limit(10)
    )
    inner_join_result.show()

    print("======================================")
    print("Count for INNER JOIN")
    print("======================================")
    inner_join_count = (
        orders.alias("o")
        .join(
            lineitem.alias("l"),
            (col("o.orderkey") == col("l.orderkey"))
            & (
                col("o.orderdate").between(
                    col("l.shipdate") - 5, col("l.shipdate") + 5
                )
            ),
        )
        .agg({"o.orderkey": "count", "l.orderkey": "count"})
    )
    inner_join_count.show()

    print("======================================")
    print("LEFT JOIN")
    print("======================================")
    left_join_result = (
        orders.alias("o")
        .join(
            lineitem.alias("l"),
            (col("o.orderkey") == col("l.orderkey"))
            & (
                col("o.orderdate").between(
                    col("l.shipdate") - 5, col("l.shipdate") + 5
                )
            ),
            how="left",
        )
        .select("o.orderkey", "l.orderkey")
        .limit(10)
    )
    left_join_result.show()

    print("======================================")
    print("Count for LEFT JOIN")
    print("======================================")
    left_join_count = (
        orders.alias("o")
        .join(
            lineitem.alias("l"),
            (col("o.orderkey") == col("l.orderkey"))
            & (
                col("o.orderdate").between(
                    col("l.shipdate") - 5, col("l.shipdate") + 5
                )
            ),
            how="left",
        )
        .agg({"o.orderkey": "count", "l.orderkey": "count"})
    )
    left_join_count.show()

    print("======================================")
    print("RIGHT OUTER JOIN")
    print("======================================")
    right_join_result = (
        orders.alias("o")
        .join(
            lineitem.alias("l"),
            (col("o.orderkey") == col("l.orderkey"))
            & (
                col("o.orderdate").between(
                    col("l.shipdate") - 5, col("l.shipdate") + 5
                )
            ),
            how="right",
        )
        .select("o.orderkey", "l.orderkey")
        .limit(10)
    )
    right_join_result.show()

    print("======================================")
    print("Count for RIGHT OUTER JOIN")
    print("======================================")
    right_join_count = (
        orders.alias("o")
        .join(
            lineitem.alias("l"),
            (col("o.orderkey") == col("l.orderkey"))
            & (
                col("o.orderdate").between(
                    col("l.shipdate") - 5, col("l.shipdate") + 5
                )
            ),
            how="right",
        )
        .agg({"o.orderkey": "count", "l.orderkey": "count"})
    )
    right_join_count.show()

    print("======================================")
    print("FULL OUTER JOIN")
    print("======================================")
    full_outer_join_result = (
        orders.alias("o")
        .join(
            lineitem.alias("l"),
            (col("o.orderkey") == col("l.orderkey"))
            & (
                col("o.orderdate").between(
                    col("l.shipdate") - 5, col("l.shipdate") + 5
                )
            ),
            how="full_outer",
        )
        .select("o.orderkey", "l.orderkey")
        .limit(10)
    )
    full_outer_join_result.show()

    print("======================================")
    print("Count for FULL OUTER JOIN")
    print("======================================")
    full_outer_join_count = (
        orders.alias("o")
        .join(
            lineitem.alias("l"),
            (col("o.orderkey") == col("l.orderkey"))
            & (
                col("o.orderdate").between(
                    col("l.shipdate") - 5, col("l.shipdate") + 5
                )
            ),
            how="full_outer",
        )
        .agg({"o.orderkey": "count", "l.orderkey": "count"})
    )
    full_outer_join_count.show()

    print("======================================")
    print("CROSS JOIN")
    print("======================================")
    cross_join_result = (
        nation.alias("n")
        .crossJoin(region.alias("r"))
        .select("n.name", "r.name")
    )
    cross_join_result.show()

    print("======================================")
    print("LEFT ANTI JOIN")
    print("======================================")
    # Load tables
    nation = spark.table("nation")
    supplier = spark.table("supplier")

    # Perform LEFT ANTI JOIN
    result = nation.join(
        supplier, nation["nationkey"] == supplier["suppkey"], "left_anti"
    )

    # Select columns from nation table
    result = result.select(
        nation["nationkey"],
        nation["name"],
        nation["regionkey"],
        nation["comment"],
    )

    # Show the result
    result.show()

    # Exercise
    # Time taken: 4.489 seconds, Fetched 71856 row(s)
    exercise_result = (
        orders.alias("o1")
        .join(
            orders.alias("o2"),
            (col("o1.custkey") == col("o2.custkey"))
            & (year(col("o1.orderdate")) == year(col("o2.orderdate")))
            & (
                weekofyear(col("o1.orderdate"))
                == weekofyear(col("o2.orderdate"))
            ),
            how="inner",
        )
        .filter(col("o1.orderkey") != col("o2.orderkey"))
        .select("o1.custkey")
    )
    exercise_result.show()


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
