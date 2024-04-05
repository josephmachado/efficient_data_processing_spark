from datetime import datetime
from typing import Dict, List, Optional, Type

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from rainforest.etl.bronze.appuser import AppUserBronzeETL
from rainforest.etl.bronze.seller import SellerBronzeETL
from rainforest.utils.base_table import ETLDataSet, TableETL


class DimSellerSilverETL(TableETL):
    def __init__(
        self,
        spark: SparkSession,
        upstream_table_names: Optional[List[Type[TableETL]]] = [
            AppUserBronzeETL,
            SellerBronzeETL,
        ],
        name: str = "dim_seller",
        primary_keys: List[str] = ["seller_id"],
        storage_path: str = "s3a://rainforest/delta/silver/dim_seller",
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
        appuser_data = upstream_datasets[0].curr_data
        seller_data = upstream_datasets[1].curr_data
        current_timestamp = datetime.now()

        # Get common columns in both appuser_data and seller_data
        common_columns = set(appuser_data.columns).intersection(
            seller_data.columns
        )

        # Rename common columns in appuser_data to avoid conflicts
        appuser_data = appuser_data.selectExpr(
            *[
                f"`{col}` as appuser_{col}"
                if col in common_columns and col != "user_id"
                else col
                for col in appuser_data.columns
            ]
        )

        # Rename common columns in seller_data to avoid conflicts
        seller_data = seller_data.selectExpr(
            *[
                f"`{col}` as seller_{col}"
                if col in common_columns and col != "user_id"
                else col
                for col in seller_data.columns
            ]
        )

        # Perform the join based on user_id key
        dim_seller_data = appuser_data.join(
            seller_data,
            appuser_data["user_id"] == seller_data["user_id"],
            "inner",
        )

        # Drop the user_id column from the seller_data DataFrame
        dim_seller_data = dim_seller_data.drop(seller_data["user_id"])

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

        self.curr_data = etl_dataset.curr_data
        return etl_dataset

    def read(
        self, partition_values: Optional[Dict[str, str]] = None
    ) -> ETLDataSet:
        # Select the desired columns
        selected_columns = [
            col('user_id'),
            col('username'),
            col('email'),
            col('is_active'),
            col('appuser_created_ts'),
            col('appuser_last_updated_by'),
            col('appuser_last_updated_ts'),
            col('seller_id'),
            col('first_time_sold_timestamp'),
            col('seller_created_ts'),
            col('seller_last_updated_by'),
            col('seller_last_updated_ts'),
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
        dim_seller_data = (
            self.spark.read.format(self.data_format)
            .load(self.storage_path)
            .filter(partition_filter)
        )

        dim_seller_data = dim_seller_data.select(selected_columns)

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
