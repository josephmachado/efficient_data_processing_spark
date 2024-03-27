from datetime import datetime
from typing import List, Optional, Type

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from rainforest.etl.bronze.sellerproduct import SellerProductBronzeETL
from rainforest.utils.base_table import ETLDataSet, TableETL


class SellerProductSilverETL(TableETL):
    def __init__(
        self,
        spark: SparkSession,
        upstream_table_names: Optional[List[Type[TableETL]]] = [
            SellerProductBronzeETL
        ],
        name: str = "seller_x_product",
        primary_keys: List[str] = ["seller_id", "product_id"],
        storage_path: str = "s3a://rainforest/delta/silver/seller_x_product",
        data_format: str = "delta",
        database: str = "rainforest",
        partition_keys: List[str] = ["etl_inserted"],
        run_upstream: bool = True,
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
        )

    def extract_upstream(self) -> List[ETLDataSet]:
        upstream_etl_datasets = []
        for TableETLClass in self.upstream_table_names:
            t1 = TableETLClass(spark=self.spark)
            if self.run_upstream:
                t1.run()
            upstream_etl_datasets.append(t1.read())

        return upstream_etl_datasets

    def transform_upstream(
        self, upstream_datasets: List[ETLDataSet]
    ) -> ETLDataSet:
        seller_product_data = upstream_datasets[0].curr_data
        current_timestamp = datetime.now()

        transformed_data = seller_product_data.withColumn(
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
        seller_product_data = data.curr_data

        # Write the transformed data to the Delta Lake table
        seller_product_data.write.option("mergeSchema", "true").format(
            data.data_format
        ).mode("overwrite").partitionBy(data.partition_keys).save(
            data.storage_path
        )

    def read(self, partition_keys: Optional[List[str]] = None) -> ETLDataSet:
        # Read the transformed data from the Delta Lake table
        seller_product_data = self.spark.read.format(self.data_format).load(
            self.storage_path
        )

        # Create an ETLDataSet instance
        etl_dataset = ETLDataSet(
            name=self.name,
            curr_data=seller_product_data,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )

        return etl_dataset
