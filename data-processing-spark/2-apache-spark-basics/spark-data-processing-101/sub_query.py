from pyspark.sql import SparkSession
from pyspark.sql.functions import sum


def run_code(spark):
    print("=================================")
    print("Use the result of a query within a query using sub-queries ")
    print("=================================")
    # Read the 'lineitem', 'supplier', 'orders',
    # 'customer', and 'nation' tables
    spark.sql("USE tpch")
    lineitem = spark.table("lineitem")
    supplier = spark.table("supplier")
    orders = spark.table("orders")
    customer = spark.table("customer")
    nation = spark.table("nation")

    print("=================================")
    print("Create dataframes to create the data for the subqueries")
    print("=================================")
    supplier_nation_metrics = (
        lineitem.join(supplier, lineitem.suppkey == supplier.suppkey)
        .join(nation, supplier.nationkey == nation.nationkey)
        .groupBy(nation.nationkey.alias("nationkey"))
        .agg(sum(lineitem.quantity).alias("supplied_items_quantity"))
    )

    buyer_nation_metrics = (
        lineitem.join(orders, lineitem.orderkey == orders.orderkey)
        .join(customer, orders.custkey == customer.custkey)
        .join(nation, customer.nationkey == nation.nationkey)
        .groupBy(nation.nationkey.alias("nationkey"))
        .agg(sum(lineitem.quantity).alias("purchased_items_quantity"))
    )

    result = (
        nation.join(
            supplier_nation_metrics,
            nation.nationkey == supplier_nation_metrics.nationkey,
            "left",
        )
        .join(
            buyer_nation_metrics,
            nation.nationkey == buyer_nation_metrics.nationkey,
            "left",
        )
        .select(
            nation.name.alias("nation_name"),
            supplier_nation_metrics.supplied_items_quantity,
            buyer_nation_metrics.purchased_items_quantity,
        )
    )

    # Show the result
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
