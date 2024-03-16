from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from typing import List, Optional, Type
from dataclasses import asdict
from abc import ABC
from rainforest.utils.base_table import ETLDataSet, TableETL


class UserBronzeETL(TableETL):
    def __init__(
        self,
        upstream_table_names: Optional[List[Type[TableETL]]] = None,
        name: str = "user",
        primary_keys: List[str] = ["user_id"],
        storage_path: str = "s3a://rainforest/delta/bronze/user",
        data_format: str = "delta",
        database: str = "rainforest",
        partition_keys: List[str] = ["etl_inserted"],
    ) -> None:
        super().__init__(
            upstream_table_names,
            name,
            primary_keys,
            storage_path,
            data_format,
            database,
            partition_keys,
        )

    def extract_upstream(self, run_upstream: bool = True) -> List[ETLDataSet]:
        # Assuming user data is extracted from a database or other source
        # and loaded into a DataFrame
        spark = SparkSession.builder.getOrCreate()
        user_data = (
            spark.read.format("jdbc")
            .option("url", "jdbc:postgresql://host/db")
            .option("dbtable", "public.user")
            .load()
        )

        # Create an ETLDataSet instance
        etl_dataset = ETLDataSet(
            name=self.name,
            curr_data=user_data,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )

        return [etl_dataset]

    def transform_upstream(self, upstream_datasets: List[ETLDataSet]) -> ETLDataSet:
        user_data = upstream_datasets[0].curr_data

        # Perform any necessary transformations on the user data
        transformed_data = user_data.withColumn(
            "etl_inserted", lit(current_timestamp())
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
        # Perform any necessary validation checks on the user data
        user_data = data.curr_data
        is_valid = (
            user_data.select([count(col(c)).alias(c) for c in self.primary_keys])
            .filter(f"count(1) > 0")
            .toDF()
        )

        return is_valid

    def load(self, data: ETLDataSet) -> None:
        user_data = data.curr_data

        # Write the user data to the Delta Lake table
        user_data.write.format(data.data_format).mode("overwrite").partitionBy(
            data.partition_keys
        ).save(data.storage_path)

    def read(self, partition_keys: Optional[List[str]] = None) -> ETLDataSet:
        spark = SparkSession.builder.getOrCreate()

        # Read the user data from the Delta Lake table
        user_data = spark.read.format(self.data_format).load(self.storage_path)

        # Create an ETLDataSet instance
        etl_dataset = ETLDataSet(
            name=self.name,
            curr_data=user_data,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )

        return etl_dataset
