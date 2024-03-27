from datetime import datetime
from typing import List, Optional, Type

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from rainforest.etl.bronze.orders import OrdersSilverETL
from rainforest.utils.base_table import ETLDataSet, TableETL


class FactOrdersSilverETL(TableETL):
    def __init__(
        self,
        spark: SparkSession,
        upstream_table_names: Optional[List[Type[TableETL]]] = [
            OrdersSilverETL
        ],
        name: str = "fact_orders",
        primary_keys: List[str] = ["order_id"],
        storage_path: str = "s3a://rainforest/delta/silver/fact_orders",
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
        order_data = upstream_datasets[0].curr_data
        current_timestamp = datetime.now()

        # Convert total price to USD and INR
        usd_conversion_rate = 0.014  # Assume 1 USD = 70 INR
        inr_conversion_rate = 70

        transformed_data = order_data.withColumn(
            "total_price_usd", col("total_price") * lit(usd_conversion_rate)
        ).withColumn(
            "total_price_inr", col("total_price") * lit(inr_conversion_rate)
        )

        # Add etl_inserted column
        transformed_data = transformed_data.withColumn(
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
        order_data = data.curr_data

        # Write the transformed data to the Delta Lake table
        order_data.write.option("mergeSchema", "true").format(
            data.data_format
        ).mode("overwrite").partitionBy(data.partition_keys).save(
            data.storage_path
        )

    def read(self, partition_keys: Optional[List[str]] = None) -> ETLDataSet:
        # Read the transformed data from the Delta Lake table
        orders_data = self.spark.read.format(self.data_format).load(
            self.storage_path
        )
        # Explicitly select columns
        orders_data = orders_data.select(
            col("order_id"),
            col("buyer_id"),
            col("order_ts"),
            col("total_price"),
            col("total_price_usd"),
            col("total_price_inr"),
            col("created_ts"),
            col("etl_inserted"),
        )

        # Create an ETLDataSet instance
        etl_dataset = ETLDataSet(
            name=self.name,
            curr_data=orders_data,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )

        return etl_dataset
