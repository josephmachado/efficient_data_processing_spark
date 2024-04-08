from rainforest.etl.bronze.sellerproduct import SellerProductBronzeETL
from rainforest.utils.base_table import ETLDataSet


class TestSellerProductBronzeETL:
    def test_extract_upstream(self, spark):
        seller_product_tbl = SellerProductBronzeETL(spark=spark)
        seller_product_etl_dataset = seller_product_tbl.extract_upstream()
        assert seller_product_etl_dataset[0].name == "seller_product"

    def test_transform_upstream(self, spark):
        sample_data = [
            (1, 100, "2022-01-01", "Updater A", "2022-01-01"),
            (2, 200, "2022-01-02", "Updater B", "2022-01-02"),
        ]
        schema = [
            "seller_id",
            "product_id",
            "created_ts",
            "last_updated_by",
            "last_updated_ts",
        ]
        upstream_df = spark.createDataFrame(sample_data, schema=schema)

        seller_product_tbl = SellerProductBronzeETL(spark=spark)

        upstream_dataset = ETLDataSet(
            name=seller_product_tbl.name,
            curr_data=upstream_df,
            primary_keys=seller_product_tbl.primary_keys,
            storage_path=seller_product_tbl.storage_path,
            data_format=seller_product_tbl.data_format,
            database=seller_product_tbl.database,
            partition_keys=seller_product_tbl.partition_keys,
        )

        transformed_dataset = seller_product_tbl.transform_upstream(
            [upstream_dataset]
        )

        assert 'etl_inserted' in transformed_dataset.curr_data.columns

        expected_schema = set(schema + ["etl_inserted"])
        assert set(transformed_dataset.curr_data.columns) == expected_schema

        transformed_df = transformed_dataset.curr_data.drop(
            "etl_inserted"
        ).select(schema)
        assert transformed_df.collect() == upstream_df.collect()
