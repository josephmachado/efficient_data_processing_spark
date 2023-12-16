from pyspark.sql import SparkSession
from pyspark.sql.functions import round, sum, col, count, date_add, lit, year, avg
from pyspark.sql import Window
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
            # .orderBy("nation_name", "order_year")
            .limit(20)
        )

        # Show the result for Query 2
        result_df_2.show()

    def run_exercise_54(self, spark):
        print("=================================")
        print("Rank rows based on column(s) with Window ranking functions")
        print("=================================")
        pass

    def run_exercise_55(self, spark):
        print("=================================")
        print("Compare column values across rows with Window value functions")
        print("=================================")
        pass

    def run_exercise_56(self, spark):
        print("=================================")
        print("Choose rows to apply functions to within a window frame")
        print("=================================")
        pass

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
