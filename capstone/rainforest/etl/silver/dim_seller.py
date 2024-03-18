from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from typing import List, Optional, Type
from dataclasses import asdict
from rainforest.utils.base_table import ETLDataSet, TableETL


class DimSellerSilverETL(TableETL):
    def __init__(
        self,
        spark: SparkSession,
        upstream_table_names: Optional[List[Type[TableETL]]] = None,
        name: str = "dim_seller",
        primary_keys: List[str] = ["seller_id"],
        storage_path: str = "s3a://rainforest/delta/silver/dim_seller",
        data_format: str = "delta",
        database: str = "rainforest",
        partition_keys: List[str] = ["etl_inserted"]
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
        )

    def extract_upstream(self, run_upstream: bool = True) -> List[ETLDataSet]:
        upstream_etl_datasets = []
        for TableETL in self.upstream_table_names:
            t1 = TableETL(spark=self.spark)
            if run_upstream:
                t1.run()
            upstream_etl_datasets.append(t1.read())
        
        return upstream_etl_datasets

    def transform_upstream(self, upstream_datasets: List[ETLDataSet]) -> ETLDataSet:
        appuser_data = upstream_datasets[0].curr_data
        seller_data = upstream_datasets[1].curr_data
        current_timestamp = datetime.now()

        # Get common columns in both appuser_data and seller_data
        common_columns = set(appuser_data.columns).intersection(seller_data.columns)

        # Rename common columns in appuser_data to avoid conflicts
        appuser_data = appuser_data.selectExpr(
            *[f"`{col}` as appuser_{col}" if col in common_columns else col for col in appuser_data.columns]
        )

        # Rename common columns in seller_data to avoid conflicts
        seller_data = seller_data.selectExpr(
            *[f"`{col}` as seller_{col}" if col in common_columns else col for col in seller_data.columns]
        )

        # Perform the join based on user_id key
        dim_seller_data = appuser_data.join(seller_data, appuser_data["appuser_user_id"] == seller_data["seller_user_id"], "inner")
        
        transformed_data = dim_seller_data.withColumn(
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

        return etl_dataset

    def validate(self, data: ETLDataSet) -> bool:
        # Perform any necessary validation checks on the transformed data
        return True

    def load(self, data: ETLDataSet) -> None:
        dim_seller_data = data.curr_data

        # Write the transformed data to the Delta Lake table
        dim_seller_data.write.option("mergeSchema", "true").format(data.data_format).mode("overwrite").partitionBy(
            data.partition_keys
        ).save(data.storage_path)

    def read(self, partition_keys: Optional[List[str]] = None) -> ETLDataSet:
        # Read the transformed data from the Delta Lake table
        dim_seller_data = self.spark.read.format(self.data_format).load(self.storage_path)

        # Create an ETLDataSet instance
        etl_dataset = ETLDataSet(
            name=self.name,
            curr_data=dim_seller_data,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )

        return etl_dataset
