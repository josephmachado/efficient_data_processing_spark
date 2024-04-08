from pyspark.sql import SparkSession
from rainforest.etl.silver.dim_buyer import DimBuyerSilverETL
from rainforest.utils.base_table import ETLDataSet


class TestDimBuyerSilverETL:
    def test_transform_upstream(self, spark: SparkSession):
        # Mock the ETLDataSet for appuser and buyer with the specified schemas
        appuser_schema = [
            "user_id",
            "username",
            "email",
            "is_active",
            "created_ts",
            "last_updated_by",
            "last_updated_ts",
            "etl_inserted",
        ]
        buyer_schema = [
            "buyer_id",
            "user_id",
            "first_time_purchased_timestamp",
            "created_ts",
            "last_updated_by",
            "last_updated_ts",
            "etl_inserted",
        ]

        appuser_sample_data = [
            (
                1,
                "user_1",
                "email_1@example.com",
                True,
                "2022-01-01",
                "Updater A",
                "2022-01-01",
                "2022-01-01",
            )
        ]
        buyer_sample_data = [
            (
                100,
                1,
                "2022-01-01",
                "2022-01-01",
                "Updater A",
                "2022-01-01",
                "2022-01-01",
            )
        ]

        appuser_df = spark.createDataFrame(
            appuser_sample_data, schema=appuser_schema
        )
        buyer_df = spark.createDataFrame(
            buyer_sample_data, schema=buyer_schema
        )

        appuser_dataset = ETLDataSet(
            "appuser", appuser_df, ["user_id"], "", "delta", "rainforest", []
        )
        buyer_dataset = ETLDataSet(
            "buyer", buyer_df, ["buyer_id"], "", "delta", "rainforest", []
        )

        # Instantiate DimBuyerSilverETL and transform
        dim_buyer_etl = DimBuyerSilverETL(
            spark, run_upstream=False, load_data=False
        )
        transformed_dataset = dim_buyer_etl.transform_upstream(
            [appuser_dataset, buyer_dataset]
        )

        # Verify the schema of the transformed dataset
        expected_columns = set(
            [
                'user_id',
                'username',
                'email',
                'is_active',
                'appuser_created_ts',
                'appuser_last_updated_by',
                'appuser_last_updated_ts',
                'appuser_etl_inserted',
                'buyer_id',
                'first_time_purchased_timestamp',
                'buyer_created_ts',
                'buyer_last_updated_by',
                'buyer_last_updated_ts',
                'buyer_etl_inserted',
                'etl_inserted',
            ]
        )
        actual_columns = set(transformed_dataset.curr_data.columns)
        assert (
            actual_columns == expected_columns
        ), f"Expected columns {expected_columns}, but got {actual_columns}"

        # Verify the content of the transformed dataset
        expected_data = [
            (
                1,
                "user_1",
                "email_1@example.com",
                True,
                "2022-01-01",
                "Updater A",
                "2022-01-01",
                "2022-01-01",
                100,
                "2022-01-01",
                "2022-01-01",
                "Updater A",
                "2022-01-01",
                "2022-01-01",
                transformed_dataset.curr_data.select('etl_inserted').collect()[
                    0
                ][0],
            )
        ]
        assert (
            transformed_dataset.curr_data.collect() == expected_data
        ), "Transformed data does not match expected data"
