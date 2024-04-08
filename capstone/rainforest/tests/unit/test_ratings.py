from rainforest.etl.bronze.ratings import RatingsBronzeETL
from rainforest.utils.base_table import ETLDataSet


class TestRatingsBronzeETL:
    def test_extract_upstream(self, spark):
        ratings_tbl = RatingsBronzeETL(spark=spark)
        ratings_etl_dataset = ratings_tbl.extract_upstream()
        assert ratings_etl_dataset[0].name == "ratings"

    def test_transform_upstream(self, spark):
        # Create a sample DataFrame to be used as upstream dataset
        sample_data = [
            (1, 100, 5, "2022-01-01", "Updater A", "2022-01-01"),
            (2, 200, 4, "2022-01-02", "Updater B", "2022-01-02"),
        ]
        schema = [
            "ratings_id",
            "product_id",
            "rating",
            "created_ts",
            "last_updated_by",
            "last_updated_ts",
        ]
        upstream_df = spark.createDataFrame(sample_data, schema=schema)

        # Create a RatingsBronzeETL instance
        ratings_tbl = RatingsBronzeETL(spark=spark)

        upstream_dataset = ETLDataSet(
            name=ratings_tbl.name,
            curr_data=upstream_df,
            primary_keys=ratings_tbl.primary_keys,
            storage_path=ratings_tbl.storage_path,
            data_format=ratings_tbl.data_format,
            database=ratings_tbl.database,
            partition_keys=ratings_tbl.partition_keys,
        )

        # Apply transformation
        transformed_dataset = ratings_tbl.transform_upstream(
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
