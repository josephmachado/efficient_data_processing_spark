from rainforest.etl.bronze.brand import BrandBronzeETL
from rainforest.utils.base_table import ETLDataSet


class TestBrandBronzeETL:
    def test_extract_upstream(self, spark):
        # Initialize the ETL process for the brand table
        brand_tbl = BrandBronzeETL(spark=spark)
        # Extract upstream data
        brand_etl_dataset = brand_tbl.extract_upstream()
        # Verify the name of the extracted dataset matches 'brand'
        assert brand_etl_dataset[0].name == "brand"

    def test_transform_upstream(self, spark):
        # Create a sample DataFrame to simulate upstream dataset for brand data
        sample_data = [
            (1, "Brand A", "Country X", "2022-01-01", "Alice", "2022-01-01"),
            (2, "Brand B", "Country Y", "2022-01-02", "Bob", "2022-01-02"),
        ]
        schema = [
            "brand_id",
            "name",
            "country",
            "created_ts",
            "last_updated_by",
            "last_updated_ts",
        ]
        upstream_df = spark.createDataFrame(sample_data, schema=schema)

        # Initialize the BrandBronzeETL instance
        brand_tbl = BrandBronzeETL(spark=spark)

        # Manually construct an ETLDataSet instance with the upstream DataFrame
        upstream_dataset = ETLDataSet(
            name=brand_tbl.name,
            curr_data=upstream_df,
            primary_keys=brand_tbl.primary_keys,
            storage_path=brand_tbl.storage_path,
            data_format=brand_tbl.data_format,
            database=brand_tbl.database,
            partition_keys=brand_tbl.partition_keys,
        )

        # Apply transformation
        transformed_dataset = brand_tbl.transform_upstream([upstream_dataset])

        # Verify the 'etl_inserted' column is added to the transformed data
        assert 'etl_inserted' in transformed_dataset.curr_data.columns

        # Ensure the schema of the transformed data includes the new column
        expected_schema = set(schema + ["etl_inserted"])
        assert set(transformed_dataset.curr_data.columns) == expected_schema

        # Check if the transformed dataset and the original
        # upstream DataFrame match (minus the 'etl_inserted' column)
        transformed_df = transformed_dataset.curr_data.select(schema)
        assert transformed_df.collect() == upstream_df.collect()
