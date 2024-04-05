from rainforest.etl.bronze.buyer import BuyerBronzeETL
from rainforest.utils.base_table import ETLDataSet


class TestBuyerBronzeETL:
    def test_extract_upstream(self, spark):
        buyer_tbl = BuyerBronzeETL(spark=spark)
        buyer_etl_dataset = buyer_tbl.extract_upstream()
        assert buyer_etl_dataset[0].name == "buyer"

    def test_transform_upstream(self, spark):
        # Create a sample DataFrame to be used as upstream dataset
        sample_data = [
            (1, "user_1", "2022-01-01", "2022-01-01", "John", "2022-01-01"),
            (2, "user_2", "2022-01-02", "2022-01-02", "Jane", "2022-01-02"),
        ]
        schema = [
            "buyer_id",
            "user_id",
            "first_time_purchased_timestamp",
            "created_ts",
            "last_updated_by",
            "last_updated_ts",
        ]
        upstream_df = spark.createDataFrame(sample_data, schema=schema)

        # Create a BuyerBronzeETL instance
        buyer_tbl = BuyerBronzeETL(spark=spark)

        upstream_dataset = ETLDataSet(
            name=buyer_tbl.name,
            curr_data=upstream_df,
            primary_keys=buyer_tbl.primary_keys,
            storage_path=buyer_tbl.storage_path,
            data_format=buyer_tbl.data_format,
            database=buyer_tbl.database,
            partition_keys=buyer_tbl.partition_keys,
        )

        # Apply transformation
        transformed_dataset = buyer_tbl.transform_upstream([upstream_dataset])

        # Check if 'etl_inserted' column is added
        assert 'etl_inserted' in transformed_dataset.curr_data.columns

        # Check if transformation is correct by comparing schema
        expected_schema = set(schema + ["etl_inserted"])
        assert set(transformed_dataset.curr_data.columns) == expected_schema

        # Check if transformed dataset and upstream DataFrame are the same
        transformed_df = transformed_dataset.curr_data.select(schema)
        assert transformed_df.collect() == upstream_df.collect()
