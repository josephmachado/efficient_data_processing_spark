from rainforest.etl.bronze.appuser import AppUserBronzeETL
from rainforest.utils.base_table import ETLDataSet


class TestAppUserBronzeETL:
    def test_extract_upstream(self, spark):
        # Initialize the ETL process for the appuser table
        user_tbl = AppUserBronzeETL(spark=spark)
        # Extract upstream data
        user_etl_dataset = user_tbl.extract_upstream()
        # Verify the name of the extracted dataset matches 'user'
        assert user_etl_dataset[0].name == "user"

    def test_transform_upstream(self, spark):
        # Create a sample DataFrame to simulate upstream dataset for user data
        sample_data = [
            (
                1,
                "username1",
                "email1@example.com",
                True,
                "2022-01-01",
                "Alice",
                "2022-01-01",
            ),
            (
                2,
                "username2",
                "email2@example.com",
                False,
                "2022-01-02",
                "Bob",
                "2022-01-02",
            ),
        ]
        schema = [
            "user_id",
            "username",
            "email",
            "is_active",
            "created_ts",
            "last_updated_by",
            "last_updated_ts",
        ]
        upstream_df = spark.createDataFrame(sample_data, schema=schema)

        # Initialize the AppUserBronzeETL instance
        user_tbl = AppUserBronzeETL(spark=spark)

        # Manually construct an ETLDataSet instance with the upstream DataFrame
        upstream_dataset = ETLDataSet(
            name=user_tbl.name,
            curr_data=upstream_df,
            primary_keys=user_tbl.primary_keys,
            storage_path=user_tbl.storage_path,
            data_format=user_tbl.data_format,
            database=user_tbl.database,
            partition_keys=user_tbl.partition_keys,
        )

        # Apply transformation
        transformed_dataset = user_tbl.transform_upstream([upstream_dataset])

        # Verify the 'etl_inserted' column is added to the transformed data
        assert 'etl_inserted' in transformed_dataset.curr_data.columns

        # Ensure the schema of the transformed data includes the new column
        expected_schema = set(schema + ["etl_inserted"])
        assert set(transformed_dataset.curr_data.columns) == expected_schema

        # Check if the transformed dataset and the original
        # upstream DataFrame match (minus the 'etl_inserted' column)
        transformed_df = transformed_dataset.curr_data.select(schema)
        assert transformed_df.collect() == upstream_df.collect()
