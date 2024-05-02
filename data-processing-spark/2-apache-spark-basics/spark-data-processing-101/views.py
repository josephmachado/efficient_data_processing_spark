from pyspark.sql import SparkSession
from pyspark.sql.functions import sum


def run_code(spark):
    print("=================================")
    print("Save queries as views for more straightforward reads")
    print("=================================")
    spark.sql("USE tpch")
    nation = spark.table("nation")
    lineitem = spark.table("lineitem")
    supplier = spark.table("supplier")
    orders = spark.table("orders")
    customer = spark.table("customer")

    print("=================================")
    print("Calculate supplied_items_quantity")
    print("=================================")
    supplied_items_quantity = (
        lineitem.join(supplier, lineitem["suppkey"] == supplier["suppkey"])
        .join(nation, nation["nationkey"] == supplier["nationkey"])
        .groupBy("nation.nationkey", "nation.name")
        .agg(sum("lineitem.quantity").alias("quantity"))
        .select("nation.nationkey", "quantity")
    )

    print("=================================")
    print("Calculate purchased_items_quantity")
    print("=================================")
    purchased_items_quantity = (
        lineitem.join(orders, lineitem["orderkey"] == orders["orderkey"])
        .join(customer, customer["custkey"] == orders["custkey"])
        .join(nation, nation["nationkey"] == customer["nationkey"])
        .groupBy("nation.nationkey", "nation.name")
        .agg(sum("lineitem.quantity").alias("quantity"))
        .select("nation.nationkey", "quantity")
    )

    print("=================================")
    print("Perform a LEFT JOIN to get the final DataFrame")
    print("=================================")
    nation_supplied_purchased_quantity = (
        nation.join(supplied_items_quantity, "nationkey", "left_outer")
        .join(purchased_items_quantity, "nationkey", "left_outer")
        .select(
            "nation.name",
            supplied_items_quantity["quantity"].alias(
                "supplied_items_quantity"
            ),
            purchased_items_quantity["quantity"].alias(
                "purchased_items_quantity"
            ),
        )
    )
    nation_supplied_purchased_quantity.createOrReplaceTempView(
        "nation_supplied_purchased_quantity"
    )
    spark.sql("SELECT * FROM nation_supplied_purchased_quantity").show()


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
