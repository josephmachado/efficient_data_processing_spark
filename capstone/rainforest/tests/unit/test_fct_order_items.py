from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (DoubleType, IntegerType, StringType,
                               StructField, StructType, TimestampType)
from rainforest.etl.silver.fct_order_items import FactOrderItemsSilverETL
from rainforest.utils.base_table import ETLDataSet


class TestFactOrderItemsSilverETL:
    def test_transform_upstream(self, spark: SparkSession):
        schema = StructType(
            [
                StructField("order_item_id", IntegerType(), True),
                StructField("order_id", IntegerType(), True),
                StructField("product_id", IntegerType(), True),
                StructField("seller_id", IntegerType(), True),
                StructField("quantity", IntegerType(), True),
                StructField("base_price", DoubleType(), True),
                StructField("tax", DoubleType(), True),
                StructField("created_ts", StringType(), True),
                StructField("etl_inserted", TimestampType(), True),
            ]
        )

        # Sample data to match the provided
        # input schema for the transform function
        sample_data = [
            (1, 100, 500, 10, 2, 100.0, 10.0, "2022-01-01", datetime.now())
        ]

        # Create DataFrame
        order_items_df = spark.createDataFrame(
            spark.sparkContext.parallelize(sample_data), schema
        )

        # Mock ETLDataSet
        order_items_dataset = ETLDataSet(
            "order_items",
            order_items_df,
            ["order_item_id"],
            "",
            "delta",
            "rainforest",
            [],
        )

        # Instantiate FactOrderItemsSilverETL and perform transformation
        fact_order_items_etl = FactOrderItemsSilverETL(spark)
        transformed_dataset = fact_order_items_etl.transform_upstream(
            [order_items_dataset]
        )

        # Expected columns in the result
        expected_columns = [
            "order_item_id",
            "order_id",
            "product_id",
            "seller_id",
            "quantity",
            "base_price",
            "tax",
            "actual_price",
            "created_ts",
            "etl_inserted",
        ]

        # Verify columns
        actual_columns = transformed_dataset.curr_data.columns
        assert set(actual_columns) == set(
            expected_columns
        ), "Columns do not match expected"

        # Verify data
        expected_data = [
            (
                1,
                100,
                500,
                10,
                2,
                100.0,
                10.0,
                90.0,
                "2022-01-01",
                transformed_dataset.curr_data.select('etl_inserted').collect()[
                    0
                ]["etl_inserted"],
            )
        ]

        actual_data = [
            (
                row["order_item_id"],
                row["order_id"],
                row["product_id"],
                row["seller_id"],
                row["quantity"],
                row["base_price"],
                row["tax"],
                row["actual_price"],
                row["created_ts"],
                row["etl_inserted"],
            )
            for row in transformed_dataset.curr_data.collect()
        ]

        assert (
            actual_data == expected_data
        ), "Transformed data does not match expected data"
