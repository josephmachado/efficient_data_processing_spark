from pyspark.sql import SparkSession
from pyspark.sql.functions import round, sum, col, count, date_add, lit, year
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
    def run_exercise_42(self, spark):
        print("=================================")
        print("Reduce data movement (data shuffle) to improve query performance")
        print("=================================")
        spark.sql("USE tpch")

        # Select orderkey, linenumber, and calculate totalprice from 'lineitem' table using DataFrame API
        lineitem_df = spark.table("lineitem")
        lineitem_result_df = lineitem_df.select(
            "orderkey",
            "linenumber",
            round(
                lineitem_df["extendedprice"]
                * (1 - lineitem_df["discount"])
                * (1 + lineitem_df["tax"]),
                2,
            ).alias("totalprice"),
        ).limit(10)

        # Show the result
        lineitem_result_df.show()

        # Select orderpriority, calculate total_price_thousands from 'orders' table using DataFrame API
        orders_df = spark.table("orders")
        orders_result_df = (
            orders_df.groupBy("orderpriority")
            .agg(round(sum("totalprice") / 1000, 2).alias("total_price_thousands"))
            .orderBy("orderpriority")
        )

        # Show the result
        orders_result_df.show()

    def run_exercise_43(self, spark):
        print("=================================")
        print("Hash joins are expensive, but Broadcast joins are not")
        print("=================================")
        # Set the default database to 'tpch'
        spark.sql("USE tpch")

        # Select columns from 'lineitem' and 'part' tables using DataFrame API
        lineitem_df = spark.table("lineitem")
        part_df = spark.table("part")

        join_result_df = lineitem_df.join(
            part_df, lineitem_df["partkey"] == part_df["partkey"]
        ).select(
            part_df["name"].alias("part_name"),
            part_df["partkey"],
            lineitem_df["linenumber"],
            round(
                lineitem_df["extendedprice"] * (1 - lineitem_df["discount"]), 2
            ).alias("total_price_wo_tax"),
        )

        # Show the result
        join_result_df.show()

        # Select columns from 'lineitem' and 'supplier' tables using DataFrame API
        supplier_df = spark.table("supplier")

        limit_result_df = (
            lineitem_df.join(
                supplier_df, lineitem_df["suppkey"] == supplier_df["suppkey"]
            )
            .select(
                supplier_df["name"].alias("supplier_name"),
                lineitem_df["linenumber"],
                round(
                    lineitem_df["extendedprice"] * (1 - lineitem_df["discount"]), 2
                ).alias("total_price_wo_tax"),
            )
            .limit(10)
        )

        # Show the result
        limit_result_df.show()

    def run_exercise_44(self, spark):
        print("=================================")
        print("Read your query plan & optimize it")
        print("=================================")
        # Set the default database to 'tpch'
        spark.sql("USE tpch")

        # Select columns from 'lineitem' and 'orders' tables using DataFrame API without alias
        lineitem_df = spark.table("lineitem")
        orders_df = spark.table("orders")

        query1 = (
            lineitem_df.join(
                orders_df, lineitem_df["orderkey"] == orders_df["orderkey"]
            )
            .groupBy(orders_df["orderkey"])
            .agg(
                sum(lineitem_df["extendedprice"] * (1 - lineitem_df["discount"])).alias(
                    "total_price_wo_tax"
                )
            )
        )

        query1.explain()

        # Select columns from 'lineitem' and 'supplier' tables using DataFrame API without alias
        supplier_df = spark.table("supplier")

        query2 = (
            lineitem_df.join(
                supplier_df, lineitem_df["suppkey"] == supplier_df["suppkey"]
            )
            .groupBy(supplier_df["name"])
            .agg(
                sum(lineitem_df["extendedprice"] * (1 - lineitem_df["discount"])).alias(
                    "total_price_wo_tax"
                )
            )
        )

        query2.explain()

        # Select columns from 'orders' and 'lineitem' tables using DataFrame API without alias
        # Also, utilize window function for the date range filtering
        orders_df = spark.table("orders")
        lineitem_df = spark.table("lineitem")

        query3 = (
            orders_df.join(
                lineitem_df, orders_df["orderkey"] == lineitem_df["orderkey"]
            )
            .filter(
                (orders_df["orderdate"] >= lit("1994-12-01"))
                & (orders_df["orderdate"] < date_add(lit("1994-12-01"), 3))
                & (lineitem_df["commitdate"] < lineitem_df["receiptdate"])
            )
            .groupBy(orders_df["orderpriority"])
            .agg(count(orders_df["orderkey"]).alias("order_count"))
            .orderBy(orders_df["orderpriority"])
        )

        query3.explain()

    def run_exercise_45(self, spark):
        print("=================================")
        print("Read your query plan & optimize it")
        print("=================================")
        print("Creating schema minio")
        spark.sql("DROP SCHEMA IF EXISTS minio CASCADE")
        spark.sql("CREATE SCHEMA minio")
        spark.sql("USE minio")

        print("Create lineitem_wo_encoding table")
        # Drop table if exists
        spark.sql("DROP TABLE IF EXISTS lineitem_wo_encoding")

        spark.conf.set("spark.sql.legacy.allowNonEmptyLocationInCTAS", "true")
        print("Write data from tpch.lineitem into minio.lineitem_wo_encoding")
        tpch_lineitem = spark.table("tpch.lineitem")
        tpch_lineitem.write.format("csv").option(
            "path", "s3a://tpch/lineitem_wo_encoding/"
        ).saveAsTable("minio.lineitem_wo_encoding", overwrite=True)

        print("Write data from tpch.lineitem into minio.lineitem_w_encoding")
        tpch_lineitem.write.format("parquet").option(
            "path", "s3a://tpch/lineitem_w_encoding/"
        ).saveAsTable("minio.lineitem_w_encoding", overwrite=True)

        print(
            "Write data from tpch.lineitem into minio.lineitem_w_encoding_w_parititioning"
        )
        tpch_lineitem.withColumn("receiptyear", year(col("receiptdate"))).write.mode(
            "overwrite"
        ).partitionBy("receiptyear").format("parquet").saveAsTable(
            "minio.lineitem_w_encoding_w_partitioning", overwrite=True
        )

        print(
            "Write data from tpch.lineitem into minio.lineitem_w_encoding_w_75bucketing"
        )
        tpch_lineitem.write.mode("overwrite").bucketBy(75, ["quantity"]).format(
            "parquet"
        ).saveAsTable("minio.lineitem_w_encoding_w_75bucketing", overwrite=True)
        print(
            "Write data from tpch.lineitem into minio.lineitem_w_encoding_w_100bucketing"
        )
        tpch_lineitem.write.mode("overwrite").bucketBy(100, ["quantity"]).format(
            "parquet"
        ).saveAsTable("minio.lineitem_w_encoding_w_100bucketing", overwrite=True)

        print(
            "Compare query speed of non encoded v encoded v encoded & partitioned v Bucketed lineitem"
        )
        lineitem_wo_encoding = spark.table("minio.lineitem_wo_encoding")
        result_df = lineitem_wo_encoding.groupBy("suppkey").agg(
            sum("quantity").alias("total_qty")
        )
        action_start_time = time.time()
        result_df.collect()
        action_end_time = time.time()

        # Calculate and print the execution time for the action
        exec_time_wo_encoding = action_end_time - action_start_time

        # Show the result
        print(
            f"Query execution speed without encoding: {exec_time_wo_encoding} seconds"
        )

        lineitem_w_encoding = spark.table("minio.lineitem_w_encoding")
        result_df = lineitem_w_encoding.groupBy("suppkey").agg(
            sum("quantity").alias("total_qty")
        )
        action_start_time = time.time()
        result_df.collect()
        action_end_time = time.time()

        # Calculate and print the execution time for the action
        exec_time_w_encoding = action_end_time - action_start_time

        # Show the result
        print(f"Query execution speed with encoding: {exec_time_w_encoding} seconds")

        lineitem_w_encoding_w_partitioning = spark.table(
            "minio.lineitem_w_encoding_w_partitioning"
        )
        result_df = lineitem_w_encoding_w_partitioning.groupBy("suppkey").agg(
            sum("quantity").alias("total_qty")
        )
        action_start_time = time.time()
        result_df.collect()
        action_end_time = time.time()

        # Calculate and print the execution time for the action
        exec_time_w_encoding_w_partition = action_end_time - action_start_time

        # Show the result
        print(
            f"Query execution speed with encoding and partitioning: {exec_time_w_encoding_w_partition} seconds"
        )

        lineitem_w_encoding_w_75bucketing = spark.table(
            "minio.lineitem_w_encoding_w_75bucketing"
        )
        result_df = lineitem_w_encoding_w_75bucketing.groupBy("suppkey").agg(
            sum("quantity").alias("total_qty")
        )
        action_start_time = time.time()
        result_df.collect()
        action_end_time = time.time()

        # Calculate and print the execution time for the action
        lineitem_w_encoding_w_75bucketing = action_end_time - action_start_time

        # Show the result
        print(
            f"Query execution speed with encoding and 75 buckets: {lineitem_w_encoding_w_75bucketing} seconds"
        )

        lineitem_w_encoding_w_100bucketing = spark.table(
            "minio.lineitem_w_encoding_w_100bucketing"
        )
        result_df = lineitem_w_encoding_w_100bucketing.groupBy("suppkey").agg(
            sum("quantity").alias("total_qty")
        )
        action_start_time = time.time()
        result_df.collect()
        action_end_time = time.time()

        # Calculate and print the execution time for the action
        lineitem_w_encoding_w_100bucketing = action_end_time - action_start_time

        # Show the result
        print(
            f"Query execution speed with encoding and 100 buckets: {lineitem_w_encoding_w_100bucketing} seconds"
        )

    def run_exercise(self, spark: SparkSession, exercise_num: int = 0):
        print("=================================")
        print("Chapter 4")
        print("=================================")
        exercise_num_map = {
            2: self.run_exercise_42,
            3: self.run_exercise_43,
            4: self.run_exercise_44,
            5: self.run_exercise_45,
        }
        exercise_num_map[exercise_num](spark)
