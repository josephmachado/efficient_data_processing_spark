def create_tables(
    spark, path="s3a://rainforest/delta", database: str = "rainforest"
):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

    # Table for event-level metrics
    spark.sql(f"DROP TABLE IF EXISTS {database}.event_level_metrics")
    spark.sql(
        f"""
        CREATE TABLE {database}.event_level_metrics (
            product_id INT,
            product_name STRING,
            category_id INT,
            category_name STRING,
            brand_id INT,
            brand_name STRING,
            manufacturer_id INT,
            manufacturer_name STRING,
            event_month INT,
            event_year INT,
            total_events INT,
            view_events INT,
            add_to_cart_events INT,
            purchase_events INT,
            conversion_rate DECIMAL(5, 4),
            etl_inserted TIMESTAMP
        ) USING DELTA
        PARTITIONED BY (event_year, event_month, etl_inserted)
        LOCATION '{path}/event_level_metrics'
    """
    )

    # Table for sales metrics
    spark.sql(f"DROP TABLE IF EXISTS {database}.sales_metrics")
    spark.sql(
        f"""
        CREATE TABLE {database}.sales_metrics (
            brand_id INT,
            brand_name STRING,
            manufacturer_id INT,
            manufacturer_name STRING,
            category_id INT,
            category_name STRING,
            sales_month INT,
            sales_year INT,
            total_sales DECIMAL(18, 2),
            total_quantity INT,
            avg_price DECIMAL(10, 2),
            etl_inserted TIMESTAMP
        ) USING DELTA
        PARTITIONED BY (sales_year, sales_month, etl_inserted)
        LOCATION '{path}/sales_metrics'
    """
    )
