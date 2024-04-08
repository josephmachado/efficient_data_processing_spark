from rainforest.etl.bronze.product import ProductBronzeETL
from rainforest.utils.base_table import ETLDataSet


class TestProductBronzeETL:
    def test_extract_upstream(self, spark):
        product_tbl = ProductBronzeETL(spark=spark)
        product_etl_dataset = product_tbl.extract_upstream()
        assert product_etl_dataset[0].name == "product"

    def test_transform_upstream(self, spark):
        # Create a sample DataFrame to be used as upstream dataset
        sample_data = [
            (
                1,
                "Product A",
                "Description A",
                100.0,
                1,
                10,
                "2022-01-01",
                "Updater A",
                "2022-01-01",
            ),
            (
                2,
                "Product B",
                "Description B",
                150.0,
                2,
                20,
                "2022-01-02",
                "Updater B",
                "2022-01-02",
            ),
        ]
        schema = [
            "product_id",
            "name",
            "description",
            "price",
            "brand_id",
            "manufacturer_id",
            "created_ts",
            "last_updated_by",
            "last_updated_ts",
        ]
        upstream_df = spark.createDataFrame(sample_data, schema=schema)

        # Create a ProductBronzeETL instance
        product_tbl = ProductBronzeETL(spark=spark)

        upstream_dataset = ETLDataSet(
            name=product_tbl.name,
            curr_data=upstream_df,
            primary_keys=product_tbl.primary_keys,
            storage_path=product_tbl.storage_path,
            data_format=product_tbl.data_format,
            database=product_tbl.database,
            partition_keys=product_tbl.partition_keys,
        )

        # Apply transformation
        transformed_dataset = product_tbl.transform_upstream(
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
