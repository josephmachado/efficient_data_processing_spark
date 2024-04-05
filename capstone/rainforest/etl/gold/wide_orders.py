from datetime import datetime
from typing import Dict, List, Optional, Type

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from rainforest.etl.silver.dim_seller import DimSellerSilverETL
from rainforest.etl.silver.fct_orders import FactOrdersSilverETL
from rainforest.utils.base_table import ETLDataSet, TableETL


class WideOrdersGoldETL(TableETL):
    def __init__(
        self,
        spark: SparkSession,
        upstream_table_names: Optional[List[Type[TableETL]]] = [
            FactOrdersSilverETL,
            DimSellerSilverETL,
        ],
        name: str = "wide_orders",
        primary_keys: List[str] = ["order_id"],
        storage_path: str = "s3a://rainforest/delta/gold/wide_orders",
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
        fact_orders_data = upstream_datasets[0].curr_data
        dim_seller_data = upstream_datasets[1].curr_data
        current_timestamp = datetime.now()

        # Perform left join between fact_orders_data and dim_seller_data
        wide_orders_data = fact_orders_data.join(
            dim_seller_data,
            fact_orders_data["buyer_id"] == dim_seller_data["seller_id"],
            "left",
        )

        # Drop upstream table's etl_inserted ts
        wide_orders_data = (
            wide_orders_data.drop(fact_orders_data["etl_inserted"])
            .drop(dim_seller_data["etl_inserted"])
            .withColumn("etl_inserted", lit(current_timestamp))
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

        self.curr_data = etl_dataset.curr_data
        return etl_dataset

    def read(
        self, partition_values: Optional[Dict[str, str]] = None
    ) -> ETLDataSet:
        # Select the desired columns
        selected_columns = [
            col('order_id'),
            col('buyer_id'),
            col('order_ts'),
            col('total_price'),
            col('total_price_usd'),
            col('total_price_inr'),
            col('created_ts'),
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
        wide_orders_data = (
            self.spark.read.format(self.data_format)
            .load(self.storage_path)
            .filter(partition_filter)
        )

        wide_orders_data = wide_orders_data.select(selected_columns)

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
