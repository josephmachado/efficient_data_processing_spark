from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from typing import List, Optional, Type
from rainforest.utils.base_table import ETLDataSet, TableETL
from rainforest.etl.silver.dim_seller import DimSellerSilverETL
from rainforest.etl.silver.fct_orders import FactOrdersSilverETL


class WideOrdersGoldETL(TableETL):
    def __init__(
        self,
        spark: SparkSession,
        upstream_table_names: Optional[List[Type[TableETL]]] = [FactOrdersSilverETL, DimSellerSilverETL],
        name: str = "wide_orders",
        primary_keys: List[str] = ["order_id"],
        storage_path: str = "s3a://rainforest/delta/gold/wide_orders",
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
        fact_orders_data = upstream_datasets[0].curr_data
        dim_seller_data = upstream_datasets[1].curr_data
        current_timestamp = datetime.now()

        # Perform left join between fact_orders_data and dim_seller_data
        wide_orders_data = fact_orders_data.join(dim_seller_data, fact_orders_data["buyer_id"] == dim_seller_data["seller_id"], "left")

        # Add etl_inserted column
        wide_orders_data = wide_orders_data.withColumn(
            "etl_inserted", lit(current_timestamp)
        )

        # Create a new ETLDataSet instance with the transformed data
        etl_dataset = ETLDataSet(
            name=self.name,
            curr_data=wide_orders_data,
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
        wide_orders_data = data.curr_data

        # Write the transformed data to the Delta Lake table
        wide_orders_data.write.option("mergeSchema", "true").format(data.data_format).mode("overwrite").partitionBy(
            data.partition_keys
        ).save(data.storage_path)

    def read(self, partition_keys: Optional[List[str]] = None) -> ETLDataSet:
        # Read the transformed data from the Delta Lake table
        wide_orders_data = self.spark.read.format(self.data_format).load(self.storage_path)

        # Create an ETLDataSet instance
        etl_dataset = ETLDataSet(
            name=self.name,
            curr_data=wide_orders_data,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )

        return etl_dataset
