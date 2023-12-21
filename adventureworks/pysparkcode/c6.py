from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    date_format,
    sum,
    round,
    avg,
    when,
    lag,
    row_number,
)
from pyspark.sql.window import Window

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    LongType,
    DateType,
)
import time


class CodeRunner:
    def run_exercise_61(self, spark):
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
        partsupp = spark.table("partsupp")
        part = spark.table("part")

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

        # Supplier and Customer Metrics
        supplier_metrics = (
            lineitem.join(
                partsupp,
                (lineitem["partkey"] == partsupp["partkey"])
                & (lineitem["suppkey"] == partsupp["suppkey"]),
            )
            .join(part, partsupp["partkey"] == part["partkey"])
            .groupBy(part["brand"], lineitem["suppkey"])
            .agg(
                round(
                    sum(
                        lineitem["extendedprice"]
                        * (1 - lineitem["discount"])
                        * (1 + lineitem["tax"])
                    ),
                    2,
                ).alias("supplier_total_sold"),
                round(sum(lineitem["extendedprice"]), 2).alias(
                    "supplier_total_sold_wo_tax_discounts"
                ),
            )
        )

        customer_metrics = (
            lineitem.join(orders, lineitem["orderkey"] == orders["orderkey"])
            .join(
                partsupp,
                (lineitem["partkey"] == partsupp["partkey"])
                & (lineitem["suppkey"] == partsupp["suppkey"]),
            )
            .join(part, partsupp["partkey"] == part["partkey"])
            .join(customer, orders["custkey"] == customer["custkey"])
            .groupBy(part["brand"], orders["custkey"])
            .agg(
                round(
                    sum(
                        lineitem["extendedprice"]
                        * (1 - lineitem["discount"])
                        * (1 + lineitem["tax"])
                    ),
                    2,
                ).alias("cust_total_spend"),
                round(sum(lineitem["extendedprice"]), 2).alias(
                    "cust_total_spend_wo_tax_discounts"
                ),
            )
        )

        # Join and select for final result
        result2 = (
            part.join(
                supplier_metrics.groupBy("brand").agg(
                    count("suppkey").alias("num_suppliers"),
                    sum("supplier_total_sold").alias("supplier_total_sold"),
                    sum("supplier_total_sold_wo_tax_discounts").alias(
                        "supplier_total_sold_wo_tax_discounts"
                    ),
                ),
                "brand",
            )
            .join(
                customer_metrics.groupBy("brand").agg(
                    count("custkey").alias("num_customers"),
                    sum("cust_total_spend").alias("cust_total_spend"),
                    sum("cust_total_spend_wo_tax_discounts").alias(
                        "cust_total_spend_wo_tax_discounts"
                    ),
                ),
                "brand",
            )
            .groupBy("brand")
            .agg(
                sum("num_customers").alias("num_customers"),
                sum("num_suppliers").alias("num_suppliers"),
                sum("supplier_total_sold").alias("supplier_total_sold"),
                sum("supplier_total_sold_wo_tax_discounts").alias(
                    "supplier_total_sold_wo_tax_discounts"
                ),
                sum("cust_total_spend").alias("cust_total_spend"),
                sum("cust_total_spend_wo_tax_discounts").alias(
                    "cust_total_spend_wo_tax_discounts"
                ),
            )
        )

        result2.show()

    def run_exercise_62(self, spark):
        print("=================================")
        print("Reduce data movement (data shuffle) to improve query performance")
        print("=================================")
        spark.sql("USE tpch")
        # Load data into DataFrames
        orders = spark.table("orders")
        customer = spark.table("customer")
        nation = spark.table("nation")

        # Duplicate Orders
        duplicated_orders = orders.union(orders)

        # Count orders with row number = 1
        count_ranked_orders = (
            duplicated_orders.withColumn(
                "rn",
                row_number().over(Window.partitionBy("orderkey").orderBy("orderdate")),
            )
            .filter(col("rn") == 1)
            .count()
        )

        print("Count of ranked orders with row number = 1:", count_ranked_orders)

        # Ranked Monthly Orders
        ranked_monthly_orders = (
            orders.withColumn("ordermonth", date_format("orderdate", "y-M"))
            .withColumn(
                "rn",
                row_number().over(
                    Window.partitionBy("ordermonth", "custkey").orderBy(
                        col("orderdate").desc()
                    )
                ),
            )
            .filter(col("rn") == 1)
            .select("ordermonth", "orderkey", "custkey", "totalprice")
            .orderBy("custkey", "ordermonth")
            .limit(50)
        )

        ranked_monthly_orders.show()

        # Order Priority Analysis
        order_priority_analysis = (
            orders.withColumn("ordermonth", date_format("orderdate", "y-M"))
            .groupBy("ordermonth")
            .agg(
                round(
                    avg(when(col("orderpriority") == "1-URGENT", col("totalprice"))), 2
                ).alias("urgent_order_avg_price"),
                round(
                    avg(when(col("orderpriority") == "2-HIGH", col("totalprice"))), 2
                ).alias("high_order_avg_price"),
                round(
                    avg(when(col("orderpriority") == "3-MEDIUM", col("totalprice"))), 2
                ).alias("medium_order_avg_price"),
                round(
                    avg(
                        when(
                            col("orderpriority") == "4-NOT SPECIFIED", col("totalprice")
                        )
                    ),
                    2,
                ).alias("not_specified_order_avg_price"),
                round(
                    avg(when(col("orderpriority") == "5-LOW", col("totalprice"))), 2
                ).alias("low_order_avg_price"),
            )
        )

        order_priority_analysis.show()

        # Monthly Total Price Change
        monthly_total_price_change = (
            orders.withColumn("ordermonth", date_format("orderdate", "y-M"))
            .groupBy("ordermonth")
            .agg(round(sum("totalprice") / 100000, 2).alias("totalprice"))
            .withColumn(
                "MoM_totalprice_change",
                round(
                    (
                        col("totalprice")
                        - lag("totalprice").over(Window.orderBy("ordermonth"))
                    )
                    * 100
                    / lag("totalprice").over(Window.orderBy("ordermonth")),
                    2,
                ),
            )
            .orderBy("ordermonth")
        )

        monthly_total_price_change.show()

        # Monthly Total Price Change by Customer Nation
        monthly_total_price_change_nation = (
            orders.withColumn("ordermonth", date_format("orderdate", "y-M-01"))
            .join(customer, "custkey")
            .join(nation, "nationkey")
            .groupBy("ordermonth", "nation.name")
            .agg(round(sum("totalprice") / 100000, 2).alias("totalprice"))
            .withColumn(
                "MoM_totalprice_change",
                round(
                    (
                        col("totalprice")
                        - lag("totalprice").over(
                            Window.partitionBy("name").orderBy("ordermonth")
                        )
                    )
                    * 100
                    / lag("totalprice").over(
                        Window.partitionBy("name").orderBy("ordermonth")
                    ),
                    2,
                ),
            )
            .orderBy("nation.name", "ordermonth")
        )

        monthly_total_price_change_nation.show()

        # Monthly Total Price Change by Customer Nation using GROUPING SETS
        # Monthly Customer Nation Orders
        monthly_cust_nation_orders = (
            orders.withColumn("ordermonth", date_format("orderdate", "y-M"))
            .join(customer, "custkey")
            .join(nation, "nationkey")
            .select("ordermonth", "nation.name", "totalprice")
        )

        # Use cube to achieve the same result as GROUPING SETS
        result_cube = monthly_cust_nation_orders.cube("ordermonth", "nation.name").agg(
            round(sum("totalprice") / 100000, 2).alias("totalprice")
        )

        result_cube.show()

    def run_exercise(self, spark: SparkSession, exercise_num: int = 0):
        print("=================================")
        print("Chapter 6")
        print("=================================")
        exercise_num_map = {
            1: self.run_exercise_61,
            2: self.run_exercise_62,
        }
        exercise_num_map[exercise_num](spark)
