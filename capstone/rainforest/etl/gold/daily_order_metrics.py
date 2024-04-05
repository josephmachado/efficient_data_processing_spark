from datetime import datetime
from typing import Dict, List, Optional, Type

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.functions import mean as spark_mean
from pyspark.sql.functions import sum as spark_sum
from rainforest.etl.gold.wide_orders import WideOrdersGoldETL
from rainforest.utils.base_table import ETLDataSet, TableETL


class DailyOrderMetricsGoldETL(TableETL):
    def __init__(
        self,
        spark: SparkSession,
        upstream_table_names: Optional[List[Type[TableETL]]] = [
            WideOrdersGoldETL
        ],
        name: str = "daily_order_metrics",
        primary_keys: List[str] = ["order_ts"],
        storage_path: str = "s3a://rainforest/delta/gold/daily_order_metrics",
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
        wide_orders_data = upstream_datasets[0].curr_data
        wide_orders_data = wide_orders_data.withColumn(
            "order_date", col("order_ts").cast("date")
        )

        # Filter out non-active users
        wide_orders_data = wide_orders_data.filter(col('is_active'))

        # Group by order_ts and calculate sum and mean of total_price
        daily_metrics_data = wide_orders_data.groupBy('order_date').agg(
            spark_sum('total_price').alias('total_price_sum'),
            spark_mean('total_price').alias('total_price_mean'),
        )

        current_timestamp = datetime.now()
        daily_metrics_data = daily_metrics_data.withColumn(
            "etl_inserted", lit(current_timestamp)
        )

        # Create a new ETLDataSet instance with the transformed data
        etl_dataset = ETLDataSet(
            name=self.name,
            curr_data=daily_metrics_data,
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
            col('order_date'),
            col('total_price_sum'),
            col('total_price_mean'),
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
        daily_order_metrics_data = (
            self.spark.read.format(self.data_format)
            .load(self.storage_path)
            .filter(partition_filter)
        )

        daily_order_metrics_data = daily_order_metrics_data.select(
            selected_columns
        )

        # Create an ETLDataSet instance
        etl_dataset = ETLDataSet(
            name=self.name,
            curr_data=daily_order_metrics_data,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )

        return etl_dataset
