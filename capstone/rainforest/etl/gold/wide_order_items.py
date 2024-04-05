from datetime import datetime
from typing import Dict, List, Optional, Type

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, lit, struct
from rainforest.etl.silver.dim_category import DimCategorySilverETL
from rainforest.etl.silver.dim_product import DimProductSilverETL
from rainforest.etl.silver.dim_seller import DimSellerSilverETL
from rainforest.etl.silver.fct_order_items import FactOrderItemsSilverETL
from rainforest.etl.silver.product_x_category import ProductCategorySilverETL
from rainforest.utils.base_table import ETLDataSet, TableETL


class WideOrderItemsGoldETL(TableETL):
    def __init__(
        self,
        spark: SparkSession,
        upstream_table_names: Optional[List[Type[TableETL]]] = [
            FactOrderItemsSilverETL,
            DimProductSilverETL,
            DimSellerSilverETL,
            ProductCategorySilverETL,
            DimCategorySilverETL,
        ],
        name: str = "wide_order_items",
        primary_keys: List[str] = ["order_item_id"],
        storage_path: str = "s3a://rainforest/delta/gold/wide_order_items",
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
        fact_order_items_data = upstream_datasets[0].curr_data
        dim_product_data = upstream_datasets[1].curr_data
        dim_seller_data = upstream_datasets[2].curr_data
        product_category_data = upstream_datasets[3].curr_data
        dim_category_data = upstream_datasets[4].curr_data
        current_timestamp = datetime.now()

        # Perform left join between fact_order_items_data and dim_product_data
        wide_order_items_data = fact_order_items_data.join(
            dim_product_data, "product_id", "left"
        ).join(dim_seller_data, "seller_id", "left")

        product_category_enriched_data = (
            product_category_data.join(dim_category_data, "category_id")
            .drop(dim_category_data["etl_inserted"])
            .drop(product_category_data["etl_inserted"])
        )

        product_category_data_product_grain = (
            product_category_enriched_data.groupby("product_id").agg(
                collect_list(struct("category_id", "category_name")).alias(
                    "categories"
                )
            )
        )
        # Left join with product_x_category to get category details
        wide_order_items_data = wide_order_items_data.join(
            product_category_data_product_grain, "product_id", "left"
        )

        # Drop upstream table's etl_inserted ts
        wide_order_items_data = (
            wide_order_items_data.drop(fact_order_items_data["etl_inserted"])
            .drop(dim_product_data["etl_inserted"])
            .drop(dim_seller_data["etl_inserted"])
            .withColumn("etl_inserted", lit(current_timestamp))
        )

        # Create a new ETLDataSet instance with the transformed data
        etl_dataset = ETLDataSet(
            name=self.name,
            curr_data=wide_order_items_data,
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
            col('order_item_id'),
            col('order_id'),
            col('product_id'),
            col('seller_id'),
            col('quantity'),
            col('base_price'),
            col('actual_price'),
            col('created_ts'),
            col('tax'),
            col('categories'),
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
        wide_order_items_data = (
            self.spark.read.format(self.data_format)
            .load(self.storage_path)
            .filter(partition_filter)
        )

        wide_order_items_data = wide_order_items_data.select(selected_columns)

        # Create an ETLDataSet instance
        etl_dataset = ETLDataSet(
            name=self.name,
            curr_data=wide_order_items_data,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )

        return etl_dataset
