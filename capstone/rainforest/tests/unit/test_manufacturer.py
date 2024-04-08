from rainforest.etl.bronze.manufacturer import ManufacturerBronzeETL
from rainforest.utils.base_table import ETLDataSet


class TestManufacturerBronzeETL:
    def test_extract_upstream(self, spark):
        manufacturer_tbl = ManufacturerBronzeETL(spark=spark)
        manufacturer_etl_dataset = manufacturer_tbl.extract_upstream()
        assert manufacturer_etl_dataset[0].name == "manufacturer"

    def test_transform_upstream(self, spark):
        # Create a sample DataFrame to be used as upstream dataset
        sample_data = [
            (
                1,
                "Manufacturer A",
                "Type 1",
                "2022-01-01",
                "Updater 1",
                "2022-01-01",
            ),
            (
                2,
                "Manufacturer B",
                "Type 2",
                "2022-01-02",
                "Updater 2",
                "2022-01-02",
            ),
        ]
        schema = [
            "manufacturer_id",
            "name",
            "type",
            "created_ts",
            "last_updated_by",
            "last_updated_ts",
        ]
        upstream_df = spark.createDataFrame(sample_data, schema=schema)

        # Create a ManufacturerBronzeETL instance
        manufacturer_tbl = ManufacturerBronzeETL(spark=spark)

        upstream_dataset = ETLDataSet(
            name=manufacturer_tbl.name,
            curr_data=upstream_df,
            primary_keys=manufacturer_tbl.primary_keys,
            storage_path=manufacturer_tbl.storage_path,
            data_format=manufacturer_tbl.data_format,
            database=manufacturer_tbl.database,
            partition_keys=manufacturer_tbl.partition_keys,
        )

        # Apply transformation
        transformed_dataset = manufacturer_tbl.transform_upstream(
            [upstream_dataset]
        )

        # Check if 'etl_inserted' column is added
        assert 'etl_inserted' in transformed_dataset.curr_data.columns

        # Check if transformation is correct by comparing schema
        expected_schema = set(schema + ["etl_inserted"])
        assert set(transformed_dataset.curr_data.columns) == expected_schema

        # Check if transformed dataset and upstream DataFrame are the same
        transformed_df = transformed_dataset.curr_data.select(schema)
        assert transformed_df.collect() == upstream_df.collect()
