from datetime import datetime
from typing import Dict, List, Optional, Type

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from rainforest.etl.bronze.brand import BrandBronzeETL
from rainforest.etl.bronze.manufacturer import ManufacturerBronzeETL
from rainforest.etl.bronze.product import ProductBronzeETL
from rainforest.utils.base_table import ETLDataSet, TableETL


class DimProductSilverETL(TableETL):
    def __init__(
        self,
        spark: SparkSession,
        upstream_table_names: Optional[List[Type[TableETL]]] = [
            ProductBronzeETL,
            BrandBronzeETL,
            ManufacturerBronzeETL,
        ],
        name: str = "dim_product",
        primary_keys: List[str] = ["product_id"],
        storage_path: str = "s3a://rainforest/delta/silver/dim_product",
        data_format: str = "delta",
        database: str = "rainforest",
        partition_keys: List[str] = ["etl_inserted"],
        run_upstream: bool = True,
        load_data: bool = True,
    ) -> None:
        super().__init__(
            spark,
            upstream_table_names,
            name,
            primary_keys,
            storage_path,
            data_format,
            database,
            partition_keys,
            run_upstream,
            load_data,
        )

    def extract_upstream(self) -> List[ETLDataSet]:
        upstream_etl_datasets = []
        for TableETLClass in self.upstream_table_names:
            t1 = TableETLClass(
                spark=self.spark,
                run_upstream=self.run_upstream,
                load_data=self.load_data,
            )
            if self.run_upstream:
                t1.run()
            upstream_etl_datasets.append(t1.read())

        return upstream_etl_datasets

    def transform_upstream(
        self, upstream_datasets: List[ETLDataSet]
    ) -> ETLDataSet:
        product_data = upstream_datasets[0].curr_data
        brand_data = upstream_datasets[1].curr_data
        manufacturer_data = upstream_datasets[2].curr_data
        current_timestamp = datetime.now()

        # Get common columns in both product_data and brand_data
        common_columns = set(product_data.columns).intersection(
            brand_data.columns
        )

        # Rename common columns in product_data to avoid conflicts
        product_data = product_data.selectExpr(
            *[
                f"`{col}` as product_{col}"
                if col in common_columns and col != "brand_id"
                else col
                for col in product_data.columns
            ]
        )

        # Rename common columns in brand_data to avoid conflicts
        brand_data = brand_data.selectExpr(
            *[
                f"`{col}` as brand_{col}"
                if col in common_columns and col != "brand_id"
                else col
                for col in brand_data.columns
            ]
        )

        # Join product_data with brand_data on brand_id
        dim_product_data = product_data.join(
            brand_data,
            product_data["brand_id"] == brand_data["brand_id"],
            "left",
        ).drop(brand_data["brand_id"])

        # Get common columns in both product_data and manufacturer_data
        common_columns = set(dim_product_data.columns).intersection(
            manufacturer_data.columns
        )

        # Rename common columns in dim_product_data to avoid conflicts
        dim_product_data = dim_product_data.selectExpr(
            *[
                f"`{col}` as product_{col}"
                if col in common_columns
                and col not in ["brand_id", "manufacturer_id"]
                else col
                for col in dim_product_data.columns
            ]
        )

        # Rename common columns in manufacturer_data to avoid conflicts
        manufacturer_data = manufacturer_data.selectExpr(
            *[
                f"`{col}` as manufacturer_{col}"
                if col in common_columns and col != "manufacturer_id"
                else col
                for col in manufacturer_data.columns
            ]
        )

        # Join dim_product_data with manufacturer_data on manufacturer_id
        dim_product_data = dim_product_data.join(
            manufacturer_data,
            dim_product_data["manufacturer_id"]
            == manufacturer_data["manufacturer_id"],
            "left",
        ).drop(manufacturer_data["manufacturer_id"])

        transformed_data = dim_product_data.withColumn(
            "etl_inserted", lit(current_timestamp)
        )

        # Create a new ETLDataSet instance with the transformed data
        etl_dataset = ETLDataSet(
            name=self.name,
            curr_data=transformed_data,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )

        self.curr_data = etl_dataset.curr_data
        return etl_dataset

    def read(
        self, partition_values: Optional[Dict[str, str]] = None
    ) -> ETLDataSet:
        # Select the desired columns
        selected_columns = [
            col('product_id'),
            col('product_name'),
            col('description'),
            col('price'),
            col('brand_id'),
            col('manufacturer_id'),
            col('brand_name'),
            col('country').alias('brand_country'),
            col('name').alias('manufacturer_name'),
            col('type').alias('manufacturer_type'),
            col('etl_inserted'),
        ]

        if not self.load_data:
            return ETLDataSet(
                name=self.name,
                curr_data=self.curr_data.select(selected_columns),
                primary_keys=self.primary_keys,
                storage_path=self.storage_path,
                data_format=self.data_format,
                database=self.database,
                partition_keys=self.partition_keys,
            )

        elif partition_values:
            partition_filter = " AND ".join(
                [f"{k} = '{v}'" for k, v in partition_values.items()]
            )
        else:
            latest_partition = (
                self.spark.read.format(self.data_format)
                .load(self.storage_path)
                .selectExpr("max(etl_inserted)")
                .collect()[0][0]
            )
            partition_filter = f"etl_inserted = '{latest_partition}'"
        # Read the transformed data from the Delta Lake table
        dim_product_data = (
            self.spark.read.format(self.data_format)
            .load(self.storage_path)
            .filter(partition_filter)
        )

        dim_product_data = dim_product_data.select(selected_columns)

        # Create an ETLDataSet instance
        etl_dataset = ETLDataSet(
            name=self.name,
            curr_data=dim_product_data,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )

        return etl_dataset
