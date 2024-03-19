from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sum as spark_sum, mean as spark_mean, date_format, to_date
from typing import List, Optional, Type
from rainforest.utils.base_table import ETLDataSet, TableETL
from rainforest.etl.gold.wide_orders import WideOrdersGoldETL


class DailyOrderMetricsGoldETL(TableETL):
    def __init__(
        self,
        spark: SparkSession,
        upstream_table_names: Optional[List[Type[TableETL]]] = [WideOrdersGoldETL],
        name: str = "daily_order_metrics",
        primary_keys: List[str] = ["order_ts"],
        storage_path: str = "s3a://rainforest/delta/gold/daily_order_metrics",
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
        wide_orders_data = upstream_datasets[0].curr_data
        wide_orders_data = wide_orders_data.withColumn("order_date", col("order_ts").cast("date"))

        # Filter out non-active users
        wide_orders_data = wide_orders_data.filter(col('is_active'))

        # Group by order_ts and calculate sum and mean of total_price
        daily_metrics_data = wide_orders_data.groupBy('order_date').agg(
            spark_sum('total_price').alias('total_price_sum'),
            spark_mean('total_price').alias('total_price_mean')
        )

        current_timestamp = datetime.now()
        daily_metrics_data = daily_metrics_data.withColumn("etl_inserted", lit(current_timestamp))

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

        return etl_dataset

    def validate(self, data: ETLDataSet) -> bool:
        # Perform any necessary validation checks on the transformed data
        return True

    def load(self, data: ETLDataSet) -> None:
        daily_metrics_data = data.curr_data

        # Write the transformed data to the Delta Lake table
        daily_metrics_data.write.option("mergeSchema", "true").format(data.data_format).mode("overwrite").partitionBy(
            data.partition_keys
        ).save(data.storage_path)
    
    def read(self, partition_keys: Optional[List[str]] = None) -> ETLDataSet:
        # Read the transformed data from the Delta Lake table
        daily_order_metrics_data = self.spark.read.format(self.data_format).load(self.storage_path)

        # Select the desired columns
        selected_columns = [
            col('order_ts'), 
            col('total_price_sum'), 
            col('total_price_mean'), 
            col('etl_inserted')
        ]

        daily_order_metrics_data = daily_order_metrics_data.select(selected_columns)

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