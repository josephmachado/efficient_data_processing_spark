from pyspark.sql import SparkSession


def run_code(spark):
    print("==========================================")
    print("Anatomy of a Spark Application")
    print("==========================================")
    # Load tables
    lineitem = spark.table("tpch.lineitem")
    orders = spark.table("tpch.orders")

    # Job 1: Query 1 - Perform a simple aggregation on the lineitem table
    lineitem_agg = (
        lineitem.groupBy("returnflag", "linestatus")
        .agg({"quantity": "sum", "extendedprice": "sum"})
        .withColumnRenamed("sum(quantity)", "sum_qty")
        .withColumnRenamed("sum(extendedprice)", "sum_base_price")
    )

    # Job 2: Query 2 - Join lineitem with orders and perform aggregation
    joined_data = lineitem.join(
        orders, lineitem.orderkey == orders.orderkey, "inner"
    )
    order_agg = (
        joined_data.groupBy("returnflag", "orderpriority")
        .agg({"quantity": "avg", "extendedprice": "avg"})
        .withColumnRenamed("avg(quantity)", "avg_qty")
        .withColumnRenamed("avg(extendedprice)", "avg_price")
    )

    # Show the results of both queries
    print("==========================================")
    print("Lineitem agg data")
    print("==========================================")
    lineitem_agg.explain()
    lineitem_agg.show()
    print("==========================================")
    print("Order-lineitem agg data")
    print("==========================================")
    order_agg.explain()
    order_agg.show()


if __name__ == '__main__':
    spark = (
        SparkSession.builder.appName("Spark Anatomy")
        .enableHiveSupport()
        .getOrCreate()
    )
    # Set the log level
    spark.sparkContext.setLogLevel("ERROR")
    run_code(spark=spark)
    spark.stop()
