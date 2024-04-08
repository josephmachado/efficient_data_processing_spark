from rainforest.etl.bronze.clickstream import ClickstreamBronzeETL
from rainforest.utils.base_table import ETLDataSet


class TestClickstreamBronzeETL:
    def test_extract_upstream(self, spark):
        # Initialize the ETL process for the clickstream table
        clickstream_tbl = ClickstreamBronzeETL(spark=spark)
        # Extract upstream data
        clickstream_etl_dataset = clickstream_tbl.extract_upstream()
        # Assert the dataset name is correctly set to 'clickstream'
        assert clickstream_etl_dataset[0].name == "clickstream"

    def test_transform_upstream(self, spark):
        # Create a sample DataFrame
        sample_data = [
            (
                1,
                "user_1",
                "view",
                "product_1",
                "order_1",
                "2022-01-01 12:00:00",
                "2022-01-01",
            ),
            (
                2,
                "user_2",
                "purchase",
                "product_2",
                "order_2",
                "2022-01-02 13:00:00",
                "2022-01-02",
            ),
        ]
        schema = [
            "event_id",
            "user_id",
            "event_type",
            "product_id",
            "order_id",
            "timestamp",
            "created_ts",
        ]
        upstream_df = spark.createDataFrame(sample_data, schema=schema)

        # Initialize the ClickstreamBronzeETL instance
        clickstream_tbl = ClickstreamBronzeETL(spark=spark)

        # Manually construct an ETLDataSet instance with the upstream DataFrame
        upstream_dataset = ETLDataSet(
            name=clickstream_tbl.name,
            curr_data=upstream_df,
            primary_keys=clickstream_tbl.primary_keys,
            storage_path=clickstream_tbl.storage_path,
            data_format=clickstream_tbl.data_format,
            database=clickstream_tbl.database,
            partition_keys=clickstream_tbl.partition_keys,
        )

        # Apply transformation
        transformed_dataset = clickstream_tbl.transform_upstream(
            [upstream_dataset]
        )

        # Assert the 'etl_inserted' column is added to the transformed data
        assert 'etl_inserted' in transformed_dataset.curr_data.columns

        # Ensure the schema of the transformed data includes the new column
        expected_schema = set(schema + ["etl_inserted"])
        assert set(transformed_dataset.curr_data.columns) == expected_schema

        # Compare the transformed dataset with the
        # original upstream DataFrame (minus the 'etl_inserted' column)
        transformed_df = transformed_dataset.curr_data.select(schema)
        assert transformed_df.collect() == upstream_df.collect()
