from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from dataclasses import dataclass
from typing import List
from rainforest.etl.silver.dim_buyer import DimBuyerSilverETL
from rainforest.utils.base_table import ETLDataSet

class TestDimBuyerSilverETL:
    def test_transform_upstream(self, spark: SparkSession):
        # Assuming appuser_data and buyer_data are the DataFrames extracted upstream
        appuser_sample_data = [
            (1, "user_1", "email_1@example.com", True, "2022-01-01", "Updater A", "2022-01-01"),
            (2, "user_2", "email_2@example.com", False, "2022-01-02", "Updater B", "2022-01-02"),
        ]
        buyer_sample_data = [
            (1, "2022-01-01", "2022-01-01", "Updater A", "2022-01-01"),
            (2, "2022-01-02", "2022-01-02", "Updater B", "2022-01-02"),
        ]

        appuser_schema = [
            "user_id",
            "username",
            "email",
            "is_active",
            "created_ts",
            "last_updated_by",
            "last_updated_ts",
        ]
        buyer_schema = [
            "buyer_id",
            "first_time_purchased_timestamp",
            "created_ts",
            "last_updated_by",
            "last_updated_ts",
        ]

        appuser_df = spark.createDataFrame(appuser_sample_data, schema=appuser_schema)
        buyer_df = spark.createDataFrame(buyer_sample_data, schema=buyer_schema)

        appuser_dataset = ETLDataSet(
            name="appuser",
            curr_data=appuser_df,
            primary_keys=["user_id"],
            storage_path="",
            data_format="delta",
            database="rainforest",
            partition_keys=[],
        )

        buyer_dataset = ETLDataSet(
            name="buyer",
            curr_data=buyer_df,
            primary_keys=["buyer_id"],
            storage_path="",
            data_format="delta",
            database="rainforest",
            partition_keys=[],
        )

        dim_buyer_etl = DimBuyerSilverETL(spark)

        # Transform
        transformed_dataset = dim_buyer_etl.transform_upstream([appuser_dataset, buyer_dataset])

        # Checks
        expected_columns = set([
            'user_id', 'username', 'email', 'is_active', 'appuser_created_ts',
            'appuser_last_updated_by', 'appuser_last_updated_ts', 'buyer_id',
            'first_time_purchased_timestamp', 'buyer_created_ts', 'buyer_last_updated_by',
            'buyer_last_updated_ts', 'etl_inserted'
        ])
        assert set(transformed_dataset.curr_data.columns) == expected_columns
        assert 'etl_inserted' in transformed_dataset.curr_data.columns
