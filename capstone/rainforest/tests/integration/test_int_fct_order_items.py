from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (DoubleType, IntegerType, StringType,
                               StructField, StructType, TimestampType)
from rainforest.etl.silver.fct_order_items import FactOrderItemsSilverETL
from rainforest.utils.base_table import ETLDataSet


class TestFactOrderItemsSilverETLEndToEnd:
    def test_transform_validate_load_read(self, spark: SparkSession):
        # Assuming validate and load methods are implemented
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

        sample_data = [
            (1, 100, 500, 10, 2, 100.0, 10.0, "2022-01-01", datetime.now())
        ]

        input_df = spark.createDataFrame(
            spark.sparkContext.parallelize(sample_data), schema
        )

        order_items_dataset = ETLDataSet(
            "order_items",
            input_df,
            ["order_item_id"],
            "s3a://rainforest/delta/silver/fact_order_items",
            "delta",
            "rainforest",
            [],
        )

        # Instantiate FactOrderItemsSilverETL
        etl_process = FactOrderItemsSilverETL(spark)

        # Run transform
        transformed_dataset = etl_process.transform_upstream(
            [order_items_dataset]
        )

        # Validate method
        isValid = etl_process.validate(transformed_dataset)
        assert isValid, "Data validation failed"

        # Load method
        etl_process.load(transformed_dataset)

        # Read the loaded data
        loaded_data = etl_process.read()

        # Expected columns and data
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
                loaded_data.curr_data.select('etl_inserted').collect()[0][
                    "etl_inserted"
                ],
            )
        ]

        # Verify columns
        assert set(loaded_data.curr_data.columns) == set(
            expected_columns
        ), "Loaded data columns do not match expected"

        # Verify data
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
            for row in loaded_data.curr_data.collect()
        ]
        assert (
            actual_data == expected_data
        ), "Loaded data does not match expected data"
