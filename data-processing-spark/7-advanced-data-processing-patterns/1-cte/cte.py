from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, sum


def run_code(spark):
    print("=================================")
    print("Use CTEs to write easy-to-understand SQL")
    print("=================================")
    spark.sql("USE tpch")
    # Load data into DataFrames
    lineitem = spark.table("lineitem")
    supplier = spark.table("supplier")
    nation = spark.table("nation")
    orders = spark.table("orders")
    customer = spark.table("customer")

    # Supplier and Buyer Nation Metrics
    supplier_nation_metrics = (
        lineitem.join(supplier, lineitem["suppkey"] == supplier["suppkey"])
        .join(nation, supplier["nationkey"] == nation["nationkey"])
        .groupBy(nation["nationkey"].alias("nationkey"))
        .agg(sum(lineitem["QUANTITY"]).alias("num_supplied_parts"))
    )

    buyer_nation_metrics = (
        lineitem.join(orders, lineitem["orderkey"] == orders["orderkey"])
        .join(customer, orders["custkey"] == customer["custkey"])
        .join(nation, customer["nationkey"] == nation["nationkey"])
        .groupBy(nation["nationkey"].alias("nationkey"))
        .agg(sum(lineitem["QUANTITY"]).alias("num_purchased_parts"))
    )

    # Join and select for final result
    result = (
        nation.join(
            supplier_nation_metrics,
            nation["nationkey"] == supplier_nation_metrics["nationkey"],
            "left_outer",
        )
        .join(
            buyer_nation_metrics,
            nation["nationkey"] == buyer_nation_metrics["nationkey"],
            "left_outer",
        )
        .select(
            nation["name"].alias("nation_name"),
            col("num_supplied_parts"),
            col("num_purchased_parts"),
            round(
                col("num_supplied_parts") / col("num_purchased_parts") * 100, 2
            ).alias("sold_to_purchase_perc"),
        )
    )

    result.show()


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
