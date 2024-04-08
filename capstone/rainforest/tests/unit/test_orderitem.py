from rainforest.etl.bronze.orderitem import OrderItemBronzeETL
from rainforest.utils.base_table import ETLDataSet


class TestOrderItemBronzeETL:
    def test_extract_upstream(self, spark):
        order_item_tbl = OrderItemBronzeETL(spark=spark)
        order_item_etl_dataset = order_item_tbl.extract_upstream()
        assert order_item_etl_dataset[0].name == "order_item"

    def test_transform_upstream(self, spark):
        # Create a sample DataFrame to be used as upstream dataset
        sample_data = [
            (
                1,
                100,
                500,
                10,
                2,
                100.0,
                10.0,
                "2022-01-01",
            ),
            (
                2,
                101,
                501,
                11,
                1,
                150.0,
                15.0,
                "2022-01-02",
            ),
        ]
        schema = [
            "order_item_id",
            "order_id",
            "product_id",
            "seller_id",
            "quantity",
            "base_price",
            "tax",
            "created_ts",
        ]
        upstream_df = spark.createDataFrame(sample_data, schema=schema)

        # Create an OrderItemBronzeETL instance
        order_item_tbl = OrderItemBronzeETL(spark=spark)

        upstream_dataset = ETLDataSet(
            name=order_item_tbl.name,
            curr_data=upstream_df,
            primary_keys=order_item_tbl.primary_keys,
            storage_path=order_item_tbl.storage_path,
            data_format=order_item_tbl.data_format,
            database=order_item_tbl.database,
            partition_keys=order_item_tbl.partition_keys,
        )

        # Apply transformation
        transformed_dataset = order_item_tbl.transform_upstream(
            [upstream_dataset]
        )

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
