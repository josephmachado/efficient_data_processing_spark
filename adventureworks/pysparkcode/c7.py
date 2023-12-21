from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    datediff,
    months_between,
    date_add,
    col,
    lit,
    coalesce,
    date_format,
    sum,
    round,
    avg,
    when,
    lag,
    row_number,
    year,
    weekofyear,
    count,
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
    def run_exercise_71(self, spark):
        print("=================================")
        print(
            "The hierarchy of data organization is a database, schema, table, and columns"
        )
        print("=================================")
        # Show catalogs
        spark.sql("SHOW catalogs").show()

        # Show schemas
        spark.sql("SHOW schemas").show()

        # Show tables in tpch
        spark.sql("SHOW TABLES IN tpch").show()

        # Describe tpch.lineitem
        spark.sql("DESCRIBE tpch.lineitem").show()

        # Change current database to tpch
        spark.sql("USE tpch")

        # Describe lineitem
        spark.sql("DESCRIBE lineitem").show()

    def run_exercise_72(self, spark):
        print("=================================")
        print(
            "Use SELECT...FROM, LIMIT, WHERE, & ORDER BY to read the required data from tables"
        )
        print("=================================")
        spark.sql("USE tpch")
        # Read all columns from the 'orders' table, limit to 10 rows
        orders_all_columns = spark.table("orders").limit(10)
        orders_all_columns.show()

        # Read specific columns 'orderkey' and 'totalprice' from the 'orders' table, limit to 10 rows
        orders_specific_columns = (
            spark.table("orders").select("orderkey", "totalprice").limit(10)
        )
        orders_specific_columns.show()

        # Read all columns from the 'customer' table where 'nationkey' is 20, limit to 10 rows
        customer_nation_20 = (
            spark.table("customer").filter(col("nationkey") == 20).limit(10)
        )
        customer_nation_20.show()

        # Read all columns from the 'customer' table where 'nationkey' is 20 and 'acctbal' > 1000, limit to 10 rows
        customer_nation_20_acctbal_gt_1000 = (
            spark.table("customer")
            .filter((col("nationkey") == 20) & (col("acctbal") > 1000))
            .limit(10)
        )
        customer_nation_20_acctbal_gt_1000.show()

        # Read all columns from the 'customer' table where 'nationkey' is 20 or 'acctbal' > 1000, limit to 10 rows
        customer_nation_20_or_acctbal_gt_1000 = (
            spark.table("customer")
            .filter((col("nationkey") == 20) | (col("acctbal") > 1000))
            .limit(10)
        )
        customer_nation_20_or_acctbal_gt_1000.show()

        # Read all columns from the 'customer' table where ('nationkey' is 20 and 'acctbal' > 1000) or 'nationkey' is 11, limit to 10 rows
        customer_nation_20_acctbal_gt_1000_or_nationkey_11 = (
            spark.table("customer")
            .filter(
                ((col("nationkey") == 20) & (col("acctbal") > 1000))
                | (col("nationkey") == 11)
            )
            .limit(10)
        )
        customer_nation_20_acctbal_gt_1000_or_nationkey_11.show()

        # Read all columns from the 'customer' table where 'nationkey' is in (10, 20)
        customer_nation_10_or_20 = spark.table("customer").filter(
            col("nationkey").isin(10, 20)
        )
        customer_nation_10_or_20.show()

        # Read all columns from the 'customer' table where 'nationkey' is not in (10, 20)
        customer_nation_not_10_or_20 = spark.table("customer").filter(
            ~col("nationkey").isin(10, 20)
        )
        customer_nation_not_10_or_20.show()

        # Count the number of rows in the 'customer' table
        customer_count = spark.table("customer").count()
        print("Count of rows in 'customer' table:", customer_count)

        # Count the number of rows in the 'lineitem' table
        lineitem_count = spark.table("lineitem").count()
        print("Count of rows in 'lineitem' table:", lineitem_count)

        # Order by 'custkey' in ascending order and limit to 10 rows
        orders_ordered_by_custkey_asc = (
            spark.table("orders").orderBy("custkey").limit(10)
        )
        orders_ordered_by_custkey_asc.show()

        # Order by 'custkey' in descending order and limit to 10 rows
        orders_ordered_by_custkey_desc = (
            spark.table("orders").orderBy(col("custkey").desc()).limit(10)
        )
        orders_ordered_by_custkey_desc.show()

    def run_exercise_73(self, spark):
        print("=================================")
        print("Combine data from multiple tables using JOINs")
        print("=================================")
        spark.sql("USE tpch")
        # Read tables
        orders = spark.table("orders")
        lineitem = spark.table("lineitem")
        nation = spark.table("nation")
        region = spark.table("region")

        # INNER JOIN
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

        # COUNT for INNER JOIN
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

        # LEFT JOIN
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

        # COUNT for LEFT JOIN
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

        # RIGHT OUTER JOIN
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

        # COUNT for RIGHT OUTER JOIN
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

        # FULL OUTER JOIN
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

        # COUNT for FULL OUTER JOIN
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

        # CROSS JOIN
        cross_join_result = (
            nation.alias("n").crossJoin(region.alias("r")).select("n.name", "r.name")
        )
        cross_join_result.show()

        # Exercise
        # Time taken: 4.489 seconds, Fetched 71856 row(s)
        exercise_result = (
            orders.alias("o1")
            .join(
                orders.alias("o2"),
                (col("o1.custkey") == col("o2.custkey"))
                & (year(col("o1.orderdate")) == year(col("o2.orderdate")))
                & (weekofyear(col("o1.orderdate")) == weekofyear(col("o2.orderdate"))),
                how="inner",
            )
            .filter(col("o1.orderkey") != col("o2.orderkey"))
            .select("o1.custkey")
        )
        exercise_result.show()

    def run_exercise_74(self, spark):
        print("=================================")
        print("Generate metrics for your dimension(s) using GROUP BY")
        print("=================================")
        # Read the 'orders' table
        spark.sql("USE tpch")
        orders = spark.table("orders")

        # Perform aggregation using DataFrame API
        result = (
            orders.groupBy("orderpriority")
            .agg(count("*").alias("num_orders"))
            .orderBy("orderpriority")
        )

        # Show the result
        result.show()

    def run_exercise_75(self, spark):
        print("=================================")
        print("Use the result of a query within a query using sub-queries ")
        print("=================================")
        # Read the 'lineitem', 'supplier', 'orders', 'customer', and 'nation' tables
        spark.sql("USE tpch")
        lineitem = spark.table("lineitem")
        supplier = spark.table("supplier")
        orders = spark.table("orders")
        customer = spark.table("customer")
        nation = spark.table("nation")

        # Perform LEFT JOIN and aggregation using DataFrame API
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

    def run_exercise_76(self, spark):
        print("=================================")
        print("Change data types (CAST) and handle NULLS (COALESCE)")
        print("=================================")
        spark.sql("USE tpch")
        # Read the 'orders' and 'lineitem' tables
        orders = spark.table("orders")
        lineitem = spark.table("lineitem")
        # Example 1: DATEDIFF in PySpark DataFrame API
        result1 = spark.sql(
            "SELECT DATEDIFF('2022-10-05', '2022-10-01') AS date_diff_result"
        )
        result1.show()

        # Example 2: LEFT JOIN and COALESCE in PySpark DataFrame API
        result2 = (
            orders.join(
                lineitem,
                (orders.orderkey == lineitem.orderkey)
                & (
                    orders.orderdate.between(
                        lineitem.shipdate - 5, lineitem.shipdate + 5
                    )
                ),
                "left",
            )
            .filter(lineitem.shipdate.isNotNull())
            .select(
                orders.orderkey,
                orders.orderdate,
                coalesce(lineitem.orderkey, lit(9999999)).alias("lineitem_orderkey"),
                lineitem.shipdate,
            )
            .limit(10)
        )

        # Show the results
        result2.show()

    def run_exercise_77(self, spark):
        print("=================================")
        print("Replicate IF.ELSE logic with CASE statements")
        print("=================================")
        spark.sql("USE tpch")
        # Read the 'orders' table
        orders = spark.table("orders")

        # Apply the CASE statement using PySpark DataFrame API
        result = orders.select(
            col("orderkey"),
            col("totalprice"),
            when(col("totalprice") > 100000, "high")
            .when(
                (col("totalprice") >= 25000) & (col("totalprice") <= 100000), "medium"
            )
            .otherwise("low")
            .alias("order_price_bucket"),
        ).limit(20)

        # Show the result
        result.show()

    def run_exercise_78(self, spark):
        print("=================================")
        print(
            "Stack tables on top of each other with UNION and UNION ALL,  subtract tables with EXCEPT "
        )
        print("=================================")
        spark.sql("USE tpch")

        # Read the 'customer' table
        customer = spark.table("customer")

        # Example 1: SELECT with LIKE condition
        result1 = customer.select("custkey", "name").filter(col("name").like("%_91%"))
        result1.show()

        # Example 2: UNION
        result2 = (
            customer.select("custkey", "name")
            .filter(col("name").like("%_91%"))
            .union(customer.select("custkey", "name").filter(col("name").like("%_91%")))
            .union(customer.select("custkey", "name").filter(col("name").like("%_91%")))
        )
        result2.show()

        # Example 3: UNION ALL
        result3 = (
            customer.select("custkey", "name")
            .filter(col("name").like("%_91%"))
            .unionAll(
                customer.select("custkey", "name").filter(col("name").like("%_91%"))
            )
            .unionAll(
                customer.select("custkey", "name").filter(col("name").like("%_91%"))
            )
        )
        result3.show()

        # Example 4: EXCEPT
        result4 = (
            customer.select("custkey", "name")
            .filter(col("name").like("%_91%"))
            .exceptAll(
                customer.select("custkey", "name").filter(col("name").like("%_91%"))
            )
        )
        result4.show()

        # Example 5: EXCEPT with different LIKE condition
        result5 = (
            customer.select("custkey", "name")
            .filter(col("name").like("%_91%"))
            .exceptAll(
                customer.select("custkey", "name").filter(col("name").like("%191%"))
            )
        )
        result5.show()

    def run_exercise_79(self, spark):
        print("=================================")
        print("Save queries as views for more straightforward reads")
        print("=================================")
        spark.sql("USE tpch")
        nation = spark.table("nation")
        lineitem = spark.table("lineitem")
        supplier = spark.table("supplier")
        orders = spark.table("orders")
        customer = spark.table("customer")

        # Calculate supplied_items_quantity
        supplied_items_quantity = (
            lineitem.join(supplier, lineitem["suppkey"] == supplier["suppkey"])
            .join(nation, nation["nationkey"] == supplier["nationkey"])
            .groupBy("nation.nationkey", "nation.name")
            .agg(sum("lineitem.quantity").alias("quantity"))
            .select("nation.nationkey", "quantity")
        )

        # Calculate purchased_items_quantity
        purchased_items_quantity = (
            lineitem.join(orders, lineitem["orderkey"] == orders["orderkey"])
            .join(customer, customer["custkey"] == orders["custkey"])
            .join(nation, nation["nationkey"] == customer["nationkey"])
            .groupBy("nation.nationkey", "nation.name")
            .agg(sum("lineitem.quantity").alias("quantity"))
            .select("nation.nationkey", "quantity")
        )

        # Perform a LEFT JOIN to get the final DataFrame
        nation_supplied_purchased_quantity = (
            nation.join(supplied_items_quantity, "nationkey", "left_outer")
            .join(purchased_items_quantity, "nationkey", "left_outer")
            .select(
                "nation.name",
                supplied_items_quantity["quantity"].alias("supplied_items_quantity"),
                purchased_items_quantity["quantity"].alias("purchased_items_quantity"),
            )
        )

        # Show the result
        nation_supplied_purchased_quantity.show()

    def run_exercise_710(self, spark):
        print("=================================")
        print(
            "Use these standard inbuilt DB functions for common String, Time, and Numeric data manipulation"
        )
        print("=================================")
        spark.sql("USE tpch")
        # Create DataFrames for the date literals
        start_date = lit("2022-10-01")
        end_date = lit("2022-11-05")

        # Calculate the differences using DataFrame API functions
        result = spark.range(1).select(
            datediff(end_date, start_date).alias("diff_in_days"),
            months_between(end_date, start_date).alias("diff_in_months"),
            (months_between(end_date, start_date) / 12).alias("diff_in_years"),
            date_add(end_date, 10).alias("new_date"),
        )

        # Show the result
        result.show(truncate=False)

    def run_exercise_711(self, spark):
        print("=================================")
        print("Create a table, insert data, delete data, and drop the table")
        print("=================================")
        spark.sql("USE tpch")
        # Create a Delta table 'sample_table2'
        spark.sql("DROP TABLE IF EXISTS sample_table2")
        spark.sql(
            """
            CREATE TABLE sample_table2 (sample_key bigint, sample_status STRING)
            USING delta
        """
        )

        # Query 1: SELECT * FROM sample_table2
        sample_table2_df = spark.table("sample_table2")
        sample_table2_df.show()

        # Query 2: INSERT INTO sample_table2 VALUES (1, 'hello')
        values_to_insert = [(1, "hello")]
        values_df = spark.createDataFrame(
            values_to_insert, ["sample_key", "sample_status"]
        )
        sample_table2_df = sample_table2_df.union(values_df)
        sample_table2_df.show()

        # Query 3: INSERT INTO sample_table2 SELECT nationkey, name FROM nation
        nation_df = spark.table("nation")
        sample_table2_df = sample_table2_df.union(nation_df.select("nationkey", "name"))
        sample_table2_df.show()

        # Query 4: DELETE FROM sample_table2
        sample_table2_df = sample_table2_df.filter("1 = 0")  # Empty DataFrame
        sample_table2_df.show()

        # Query 5: DROP TABLE sample_table2
        spark.sql("DROP TABLE IF EXISTS sample_table2")

    def run_exercise(self, spark: SparkSession, exercise_num: int = 0):
        print("=================================")
        print("Chapter 7")
        print("=================================")
        exercise_num_map = {
            1: self.run_exercise_71,
            2: self.run_exercise_72,
            3: self.run_exercise_73,
            4: self.run_exercise_74,
            5: self.run_exercise_75,
            6: self.run_exercise_76,
            7: self.run_exercise_77,
            8: self.run_exercise_78,
            9: self.run_exercise_79,
            10: self.run_exercise_710,
            11: self.run_exercise_711,
        }
        exercise_num_map[exercise_num](spark)
