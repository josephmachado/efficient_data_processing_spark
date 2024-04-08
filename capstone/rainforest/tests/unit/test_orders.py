from rainforest.etl.bronze.orders import OrdersBronzeETL
from rainforest.utils.base_table import ETLDataSet


class TestOrdersBronzeETL:
    def test_extract_upstream(self, spark):
        orders_tbl = OrdersBronzeETL(spark=spark)
        orders_etl_dataset = orders_tbl.extract_upstream()
        assert orders_etl_dataset[0].name == "orders"

    def test_transform_upstream(self, spark):
        # Create a sample DataFrame to be used as upstream dataset
        sample_data = [
            (100, 10, "2022-01-01 12:00:00", 100.0, "2022-01-01 11:00:00"),
            (101, 11, "2022-01-02 12:00:00", 150.0, "2022-01-02 11:00:00"),
        ]
        schema = [
            "order_id",
            "buyer_id",
            "order_ts",
            "total_price",
            "created_ts",
        ]
        upstream_df = spark.createDataFrame(sample_data, schema=schema)

        # Create an OrdersBronzeETL instance
        orders_tbl = OrdersBronzeETL(spark=spark)

        upstream_dataset = ETLDataSet(
            name=orders_tbl.name,
            curr_data=upstream_df,
            primary_keys=orders_tbl.primary_keys,
            storage_path=orders_tbl.storage_path,
            data_format=orders_tbl.data_format,
            database=orders_tbl.database,
            partition_keys=orders_tbl.partition_keys,
        )

        # Apply transformation
        transformed_dataset = orders_tbl.transform_upstream([upstream_dataset])

        # Check if 'etl_inserted' column is added
        assert 'etl_inserted' in transformed_dataset.curr_data.columns

        # Check if transformation is correct by comparing schema
        expected_schema = set(schema + ["etl_inserted"])
        assert set(transformed_dataset.curr_data.columns) == expected_schema

        # Check if transformed dataset and upstream DataFrame are the same
        # Before comparison, remove 'etl_inserted' as it's a timestamp
        # and won't match exactly
        transformed_df = transformed_dataset.curr_data.drop(
            "etl_inserted"
        ).select(schema)
        assert transformed_df.collect() == upstream_df.collect()
