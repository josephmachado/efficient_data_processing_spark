from datetime import datetime
from typing import List, Optional, Type

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from rainforest.etl.bronze.orderitem import OrderItemBronzeETL
from rainforest.utils.base_table import ETLDataSet, TableETL


class FactOrderItemsSilverETL(TableETL):
    def __init__(
        self,
        spark: SparkSession,
        upstream_table_names: Optional[List[Type[TableETL]]] = [
            OrderItemBronzeETL
        ],
        name: str = "fact_order_items",
        primary_keys: List[str] = ["order_item_id"],
        storage_path: str = "s3a://rainforest/delta/silver/fact_order_items",
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
        for TableETL in self.upstream_table_names:
            t1 = TableETL(spark=self.spark)
            if self.run_upstream:
                t1.run()
            upstream_etl_datasets.append(t1.read())

        return upstream_etl_datasets

    def transform_upstream(
        self, upstream_datasets: List[ETLDataSet]
    ) -> ETLDataSet:
        order_item_data = upstream_datasets[0].curr_data
        current_timestamp = datetime.now()

        # Convert total price to USD and INR
        usd_conversion_rate = 0.014  # Assume 1 USD = 70 INR
        inr_conversion_rate = 70

        # Calculate actual price (base price - tax)
        transformed_data = order_item_data.withColumn(
            "actual_price", col("base_price") - col("tax")
        ).withColumn("etl_inserted", lit(current_timestamp))

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
        order_item_data = data.curr_data

        # Write the transformed data to the Delta Lake table
        order_item_data.write.option("mergeSchema", "true").format(
            data.data_format
        ).mode("overwrite").partitionBy(data.partition_keys).save(
            data.storage_path
        )

    def read(self, partition_keys: Optional[List[str]] = None) -> ETLDataSet:
        # Read the transformed data from the Delta Lake table
        order_items_data = self.spark.read.format(self.data_format).load(
            self.storage_path
        )
        # Explicitly select columns
        order_items_data = order_items_data.select(
            col("order_item_id"),
            col("order_id"),
            col("product_id"),
            col("seller_id"),
            col("quantity"),
            col("base_price"),
            col("tax"),
            col("actual_price"),
            col("created_ts"),
            col("etl_inserted"),
        )

        # Create an ETLDataSet instance
        etl_dataset = ETLDataSet(
            name=self.name,
            curr_data=order_items_data,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )

        return etl_dataset
