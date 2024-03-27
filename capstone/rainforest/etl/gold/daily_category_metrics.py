from datetime import datetime
from typing import List, Optional, Type

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from rainforest.etl.gold.wide_order_items import WideOrderItemsGoldETL
from rainforest.utils.base_table import ETLDataSet, TableETL


class DailyCategoryMetricsGoldETL(TableETL):
    def __init__(
        self,
        spark: SparkSession,
        upstream_table_names: Optional[List[Type[TableETL]]] = [
            WideOrderItemsGoldETL
        ],
        name: str = "daily_category_metrics",
        primary_keys: List[str] = ["order_date", "category"],
        storage_path: str = "s3a://rainforest/delta/gold/daily_category_metrics",
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
        wide_orders_data = upstream_datasets[0].curr_data
        wide_orders_data = wide_orders_data.withColumn(
            "order_date", F.col("created_ts").cast("date")
        )

        # Filter out non-active users
        wide_orders_data = wide_orders_data.filter(F.col('is_active'))

        # Explode the categories array to get each category as a separate row
        df_exploded = wide_orders_data.select(
            "order_id",
            "order_date",
            "product_id",
            "categories",
            "actual_price",
            F.explode("categories").alias("category"),
            "etl_inserted",
        )

        # Group by order_date and category, calculate mean and
        # median actual price
        category_metrics_data = df_exploded.groupBy(
            "order_date", "category"
        ).agg(
            F.mean("actual_price").alias("mean_actual_price"),
            F.expr("percentile_approx(actual_price, 0.5)").alias(
                "median_actual_price"
            ),
        )

        current_timestamp = datetime.now()
        category_metrics_data = category_metrics_data.withColumn(
            "etl_inserted", F.lit(current_timestamp)
        )

        # Create a new ETLDataSet instance with the transformed data
        etl_dataset = ETLDataSet(
            name=self.name,
            curr_data=category_metrics_data,
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
        category_metrics_data = data.curr_data

        # Write the transformed data to the Delta Lake table
        category_metrics_data.write.option("mergeSchema", "true").format(
            data.data_format
        ).mode("overwrite").partitionBy(data.partition_keys).save(
            data.storage_path
        )

    def read(self, partition_keys: Optional[List[str]] = None) -> ETLDataSet:
        # Read the transformed data from the Delta Lake table
        daily_category_metrics_data = self.spark.read.format(
            self.data_format
        ).load(self.storage_path)

        # Select the desired columns
        selected_columns = [
            F.col('order_date'),
            F.col('category'),
            F.col('mean_actual_price'),
            F.col('median_actual_price'),
            F.col('etl_inserted'),
        ]

        daily_category_metrics_data = daily_category_metrics_data.select(
            selected_columns
        )

        # Create an ETLDataSet instance
        etl_dataset = ETLDataSet(
            name=self.name,
            curr_data=daily_category_metrics_data,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )

        return etl_dataset
