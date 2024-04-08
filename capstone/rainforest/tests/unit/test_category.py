from rainforest.etl.bronze.category import CategoryBronzeETL
from rainforest.utils.base_table import ETLDataSet


class TestCategoryBronzeETL:
    def test_extract_upstream(self, spark):
        # Initialize the ETL process for the category table
        category_tbl = CategoryBronzeETL(spark=spark)
        # Extract upstream data
        category_etl_dataset = category_tbl.extract_upstream()
        # Assert the dataset name is correctly set to 'category'
        assert category_etl_dataset[0].name == "category"

    def test_transform_upstream(self, spark):
        # Create a sample DataFrame
        sample_data = [
            (1, "Electronics", "2022-01-01", "Alice", "2022-01-01"),
            (2, "Clothing", "2022-01-02", "Bob", "2022-01-02"),
        ]
        schema = [
            "category_id",
            "name",
            "created_ts",
            "last_updated_by",
            "last_updated_ts",
        ]
        upstream_df = spark.createDataFrame(sample_data, schema=schema)

        # Initialize the CategoryBronzeETL instance
        category_tbl = CategoryBronzeETL(spark=spark)

        # Manually construct an ETLDataSet instance with the upstream DataFrame
        upstream_dataset = ETLDataSet(
            name=category_tbl.name,
            curr_data=upstream_df,
            primary_keys=category_tbl.primary_keys,
            storage_path=category_tbl.storage_path,
            data_format=category_tbl.data_format,
            database=category_tbl.database,
            partition_keys=category_tbl.partition_keys,
        )

        # Apply transformation
        transformed_dataset = category_tbl.transform_upstream(
            [upstream_dataset]
        )

        # Assert the 'etl_inserted' column is added to the transformed data
        assert 'etl_inserted' in transformed_dataset.curr_data.columns

        # Ensure the schema of the transformed data includes the new column
        expected_schema = set(schema + ["etl_inserted"])
        assert set(transformed_dataset.curr_data.columns) == expected_schema

        # Compare the transformed dataset with the original
        # upstream DataFrame (minus the 'etl_inserted' column)
        transformed_df = transformed_dataset.curr_data.select(schema)
        assert transformed_df.collect() == upstream_df.collect()
