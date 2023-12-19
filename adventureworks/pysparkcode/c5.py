from pyspark.sql import SparkSession
from pyspark.sql.functions import round, sum, col, count, date_add, lit, year, avg
from pyspark.sql import Window, functions as F
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
    def run_exercise_52(self, spark):
        print("=================================")
        print(
            "Window = A set of rows identified by values present in one or more column(s)"
        )
        print("=================================")
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

    def run_exercise_53(self, spark):
        print("=================================")
        print("Defining a window function")
        print("=================================")
        nation = spark.table("tpch.nation")
        customer = spark.table("tpch.customer")
        orders = spark.table("tpch.orders")

        result_df_1 = (
            orders.join(customer, orders.custkey == customer.custkey)
            .groupBy("customer.name", "orders.custkey", "orders.orderdate")
            .agg(
                col("customer.name").alias("customer_name"),
                col("orders.orderdate").alias("orderdate"),
                round(sum("orders.totalprice"), 2).alias("total_price"),
                round(
                    sum(sum("orders.totalprice")).over(
                        Window.partitionBy("orders.custkey").orderBy("orders.orderdate")
                    ),
                    2,
                ).alias("cumulative_sum_total_price"),
            )
            .orderBy("orders.custkey", "orders.orderdate")
            .limit(20)
        )

        # Show the result for Query 1
        result_df_1.show()

        # Query 2
        result_df_2 = (
            orders.join(customer, orders.custkey == customer.custkey)
            .join(nation, customer.nationkey == nation.nationkey)
            .groupBy("nation.name", year("orders.orderdate"))
            .agg(
                col("nation.name").alias("nation_name"),
                year("orders.orderdate").alias("order_year"),
                round(sum("orders.totalprice") / 100000, 2).alias("total_price"),
                round(
                    avg(sum("orders.totalprice")).over(
                        Window.partitionBy("nation.name").orderBy(
                            year("orders.orderdate")
                        )
                    )
                    / 100000,
                    2,
                ).alias("cumulative_sum_total_price"),
            )
            .orderBy("nation_name", "order_year")
            .limit(20)
        )

        # Show the result for Query 2
        result_df_2.show()

    def run_exercise_54(self, spark):
        print("=================================")
        print("Rank rows based on column(s) with Window ranking functions")
        print("=================================")
        orders = spark.table("tpch.orders")
        lineitem = spark.table("tpch.lineitem")
        supplier = spark.table("tpch.supplier")
        nation = spark.table("tpch.nation")

        # Query 1
        result_df_1 = (
            orders.withColumn("totalprice", F.format_number("totalprice", 2))
            .withColumn(
                "rnk",
                F.rank().over(
                    Window.partitionBy("custkey").orderBy(F.desc("totalprice"))
                ),
            )
            .orderBy("custkey", "rnk")
            .limit(15)
        )

        result_df_1.show()

        # Query 2
        result_df_2 = (
            lineitem.filter("orderkey = 42624")
            .withColumn(
                "rnk",
                F.rank().over(
                    Window.partitionBy("orderkey").orderBy(F.desc("discount"))
                ),
            )
            .withColumn(
                "dense_rnk",
                F.dense_rank().over(
                    Window.partitionBy("orderkey").orderBy(F.desc("discount"))
                ),
            )
            .withColumn(
                "row_num",
                F.row_number().over(
                    Window.partitionBy("orderkey").orderBy(F.desc("discount"))
                ),
            )
            .limit(10)
        )

        result_df_2.show()

        # Query 3
        result_df_3 = (
            orders.join(lineitem, orders.orderkey == lineitem.orderkey)
            .join(supplier, lineitem.suppkey == supplier.suppkey)
            .join(nation, supplier.nationkey == nation.nationkey)
            .groupBy(
                col("nation.name").alias("supplier_nation"),
                F.year("orders.orderdate"),
                F.month("orders.orderdate"),
            )
            .agg(
                F.sum("lineitem.quantity").alias("total_quantity"),
                F.dense_rank()
                .over(
                    Window.partitionBy("nation.name").orderBy(
                        F.desc(F.sum("lineitem.quantity"))
                    )
                )
                .alias("rnk"),
            )
            .filter("rnk <= 3")
            .orderBy("supplier_nation", "rnk")
            .limit(30)
        )

        result_df_3.show()

    def run_exercise_55(self, spark):
        print("=================================")
        print("Compare column values across rows with Window value functions")
        print("=================================")
        orders = spark.table("tpch.orders")
        customer = spark.table("tpch.customer")
        nation = spark.table("tpch.nation")

        # Query 1
        result_df_1 = (
            orders.groupBy(F.date_format("orderdate", "yy-MMM").alias("ordermonth"))
            .agg(F.round(F.sum("totalprice") / 100000, 2).alias("total_price"))
            .withColumn(
                "prev_month_total_price",
                F.lag("total_price").over(Window.orderBy("ordermonth")),
            )
            .orderBy("ordermonth")
            .limit(10)
        )

        result_df_1.show()

        # Query 2
        result_df_2 = (
            orders.groupBy(F.date_format("orderdate", "yy-MMM").alias("ordermonth"))
            .agg(F.round(F.sum("totalprice") / 100000, 2).alias("total_price"))
            .withColumn(
                "prev_month_total_price",
                F.lag("total_price").over(Window.orderBy("ordermonth")),
            )
            .withColumn(
                "prev_prev_month_total_price",
                F.lag("total_price", 2).over(Window.orderBy("ordermonth")),
            )
            .orderBy("ordermonth")
            .limit(24)
        )

        result_df_2.show()

        # Query 3
        result_df_3 = (
            orders.join(customer, orders.custkey == customer.custkey)
            .groupBy("customer.name", "orders.orderdate")
            .agg(F.sum("orders.totalprice").alias("total_price"))
            .withColumn(
                "prev_total_price",
                F.lag("total_price").over(
                    Window.partitionBy("customer_name").orderBy("orders.orderdate")
                ),
            )
            .withColumn(
                "price_change_percentage",
                F.round(
                    (
                        (
                            F.lag("total_price").over(
                                Window.partitionBy("customer_name").orderBy(
                                    "orders.orderdate"
                                )
                            )
                            - F.col("total_price")
                        )
                        / F.lag("total_price").over(
                            Window.partitionBy("customer_name").orderBy(
                                "orders.orderdate"
                            )
                        )
                    )
                    * 100,
                    2,
                ),
            )
            .orderBy("customer_name", "order_date")
            .limit(25)
        )

        result_df_3.show()

        # Query 4
        result_df_4 = (
            orders.join(customer, orders.custkey == customer.custkey)
            .groupBy("customer.name", "orders.orderdate")
            .agg(F.sum("orders.totalprice").alias("total_price"))
            .withColumn(
                "has_total_price_increased",
                F.col("total_price")
                > F.lag("total_price").over(
                    Window.partitionBy("customer_name").orderBy("orders.orderdate")
                ),
            )
            .withColumn(
                "will_total_price_increase",
                F.col("total_price")
                < F.lead("total_price").over(
                    Window.partitionBy("customer_name").orderBy("orders.orderdate")
                ),
            )
            .orderBy("customer_name", "order_date")
            .limit(50)
        )

        result_df_4.show()

    def run_exercise_56(self, spark):
        print("=================================")
        print("Choose rows to apply functions to within a window frame")
        print("=================================")
        orders = spark.table("tpch.orders")
        customer = spark.table("tpch.customer")

        # Query 1
        result_df_1 = (
            orders.join(customer, orders.custkey == customer.custkey)
            .groupBy(
                col("customer.name").alias("customer_name"),
                F.date_format("orderdate", "yy-MMM").alias("order_month"),
            )
            .agg(F.sum("orders.totalprice").alias("total_price"))
            .withColumn(
                "three_mo_total_price_avg",
                F.round(
                    F.avg("total_price").over(
                        Window.partitionBy("customer_name")
                        .orderBy("order_month")
                        .rowsBetween(-1, 1)
                    ),
                    2,
                ),
            )
            .orderBy("customer_name", "order_month")
            .limit(50)
        )

        result_df_1.show()

        # Query 2
        result_df_2 = (
            orders.join(customer, orders.custkey == customer.custkey)
            .groupBy(
                col("customer.name").alias("customer_name"),
                F.date_format("orderdate", "yy-MMM").alias("order_month"),
            )
            .agg(F.sum("orders.totalprice").alias("total_price"))
            .withColumn(
                "three_mo_total_price_avg",
                F.round(
                    F.avg("total_price").over(
                        Window.partitionBy("customer_name")
                        .orderBy("order_month")
                        .rowsBetween(-1, 1)
                    ),
                    2,
                ),
            )
            .withColumn(
                "running_total_price_avg",
                F.round(
                    F.avg("total_price").over(
                        Window.partitionBy("customer_name").orderBy("order_month")
                    ),
                    2,
                ),
            )
            .withColumn(
                "six_mo_total_price_avg",
                F.round(
                    F.avg("total_price").over(
                        Window.partitionBy("customer_name")
                        .orderBy("order_month")
                        .rowsBetween(-3, 2)
                    ),
                    2,
                ),
            )
            .withColumn(
                "prev_three_mo_total_price_avg",
                F.round(
                    F.avg("total_price").over(
                        Window.partitionBy("customer_name")
                        .orderBy("order_month")
                        .rowsBetween(-4, -1)
                    ),
                    2,
                ),
            )
            .orderBy("customer_name", "order_month")
            .limit(50)
        )

        result_df_2.show()

        # Query 3
        result_df_3 = (
            orders.join(customer, orders.custkey == customer.custkey)
            .groupBy(
                col("customer.name").alias("customer_name"),
                F.expr("CAST(DATE_FORMAT(orderdate, 'yy-M-01') AS DATE)").alias(
                    "order_month"
                ),
            )
            .agg(F.sum("orders.totalprice").alias("total_price"))
            .withColumn(
                "order_month_timestamp",
                F.unix_timestamp("order_month", format="yy-MM-dd").cast("bigint"),
            )
            .withColumn(
                "avg_3m_all",
                F.round(
                    F.avg("total_price").over(
                        Window.partitionBy("customer_name")
                        .orderBy("order_month_timestamp")
                        .rowsBetween(-1, 1)
                    ),
                    2,
                ),
            )
            .withColumn(
                "avg_3m",
                F.round(
                    F.avg("total_price").over(
                        Window.partitionBy("customer_name")
                        .orderBy("order_month_timestamp")
                        .rangeBetween(-1, 1)
                    ),
                    2,
                ),
            )
            .orderBy("customer_name", "order_month")
            .limit(50)
        )

        result_df_3.show()

        # Query 4
        result_df_4 = (
            orders.join(customer, orders.custkey == customer.custkey)
            .groupBy(
                col("customer.name").alias("customer_name"),
                F.date_format("orderdate", "yy-MMM").alias("order_month"),
            )
            .agg(F.sum("orders.totalprice").alias("total_price"))
            .withColumn(
                "has_total_price_increased",
                F.col("total_price")
                > F.lag("total_price").over(
                    Window.partitionBy("customer_name").orderBy("order_month")
                ),
            )
            .withColumn(
                "will_total_price_increase",
                F.col("total_price")
                < F.lead("total_price").over(
                    Window.partitionBy("customer_name").orderBy("order_month")
                ),
            )
            .orderBy("customer_name", "order_month")
            .limit(50)
        )

        result_df_4.show()

    def run_exercise(self, spark: SparkSession, exercise_num: int = 0):
        print("=================================")
        print("Chapter 5")
        print("=================================")
        exercise_num_map = {
            2: self.run_exercise_52,
            3: self.run_exercise_53,
            4: self.run_exercise_54,
            5: self.run_exercise_55,
            6: self.run_exercise_56,
        }
        exercise_num_map[exercise_num](spark)
