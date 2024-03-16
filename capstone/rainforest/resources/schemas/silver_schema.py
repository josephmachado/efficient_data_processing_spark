from pyspark.sql import SparkSession


def create_tables(spark, path="s3a://rainforest/delta", database: str = "rainforest"):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

    # Dimensions
    spark.sql(f"DROP TABLE IF EXISTS {database}.dim_buyer")
    spark.sql(
        f"""
        CREATE TABLE {database}.dim_buyer (
            buyer_id INT,
            user_id INT,
            username STRING,
            email STRING,
            is_active BOOLEAN,
            first_time_purchased_timestamp TIMESTAMP,
            created_ts TIMESTAMP,
            last_updated_by INT,
            last_updated_ts TIMESTAMP,
            etl_inserted TIMESTAMP
        ) USING DELTA
        PARTITIONED BY (etl_inserted)
        LOCATION '{path}/dim_buyer'
    """
    )

    spark.sql(f"DROP TABLE IF EXISTS {database}.dim_seller")
    spark.sql(
        f"""
        CREATE TABLE {database}.dim_seller (
            seller_id INT,
            user_id INT,
            username STRING,
            email STRING,
            is_active BOOLEAN,
            first_time_sold_timestamp TIMESTAMP,
            created_ts TIMESTAMP,
            last_updated_by INT,
            last_updated_ts TIMESTAMP,
            etl_inserted TIMESTAMP
        ) USING DELTA
        PARTITIONED BY (etl_inserted)
        LOCATION '{path}/dim_seller'
    """
    )

    spark.sql(f"DROP TABLE IF EXISTS {database}.dim_product")
    spark.sql(
        f"""
        CREATE TABLE {database}.dim_product (
            product_id INT,
            name STRING,
            description STRING,
            price DECIMAL(10, 2),
            brand_id INT,
            brand_name STRING,
            manufacturer_id INT,
            manufacturer_name STRING,
            review_count INT,
            avg_rating DECIMAL(3, 2),
            created_ts TIMESTAMP,
            last_updated_by INT,
            last_updated_ts TIMESTAMP,
            etl_inserted TIMESTAMP
        ) USING DELTA
        PARTITIONED BY (etl_inserted)
        LOCATION '{path}/dim_product'
    """
    )

    spark.sql(f"DROP TABLE IF EXISTS {database}.dim_category")
    spark.sql(
        f"""
        CREATE TABLE {database}.dim_category (
            category_id INT,
            name STRING,
            created_ts TIMESTAMP,
            last_updated_by INT,
            last_updated_ts TIMESTAMP,
            etl_inserted TIMESTAMP
        ) USING DELTA
        PARTITIONED BY (etl_inserted)
        LOCATION '{path}/dim_category'
    """
    )

    spark.sql(f"DROP TABLE IF EXISTS {database}.dim_date")
    spark.sql(
        f"""
        CREATE TABLE {database}.dim_date (
            date_key DATE,
            date DATE,
            year INT,
            quarter INT,
            month INT,
            day INT,
            day_of_week INT,
            day_name STRING,
            day_of_month INT,
            day_of_quarter INT,
            day_of_year INT,
            week_of_month INT,
            week_of_year INT,
            month_name STRING,
            year_month INT,
            year_quarter INT
        ) USING DELTA
        LOCATION '{path}/dim_date'
    """
    )

    # Bridge Tables
    spark.sql(f"DROP TABLE IF EXISTS {database}.seller_x_product")
    spark.sql(
        f"""
        CREATE TABLE {database}.seller_x_product (
            seller_id INT,
            product_id INT,
            etl_inserted TIMESTAMP
        ) USING DELTA
        PARTITIONED BY (etl_inserted)
        LOCATION '{path}/seller_x_product'
    """
    )

    spark.sql(f"DROP TABLE IF EXISTS {database}.product_x_category")
    spark.sql(
        f"""
        CREATE TABLE {database}.product_x_category (
            product_id INT,
            category_id INT,
            etl_inserted TIMESTAMP
        ) USING DELTA
        PARTITIONED BY (etl_inserted)
        LOCATION '{path}/product_x_category'
    """
    )

    # Facts
    spark.sql(f"DROP TABLE IF EXISTS {database}.fact_orders")
    spark.sql(
        f"""
        CREATE TABLE {database}.fact_orders (
            order_id INT,
            buyer_id INT,
            order_date TIMESTAMP,
            total_price DECIMAL(10, 2),
            created_ts TIMESTAMP,
            last_updated_by INT,
            last_updated_ts TIMESTAMP,
            etl_inserted TIMESTAMP
        ) USING DELTA
        PARTITIONED BY (etl_inserted)
        LOCATION '{path}/fact_orders'
    """
    )

    spark.sql(f"DROP TABLE IF EXISTS {database}.fact_order_items")
    spark.sql(
        f"""
        CREATE TABLE {database}.fact_order_items (
            order_item_id INT,
            order_id INT,
            product_id INT,
            seller_id INT,
            quantity INT,
            base_price DECIMAL(10, 2),
            tax DECIMAL(10, 2),
            created_ts TIMESTAMP,
            last_updated_by INT,
            last_updated_ts TIMESTAMP,
            etl_inserted TIMESTAMP
        ) USING DELTA
        PARTITIONED BY (etl_inserted)
        LOCATION '{path}/fact_order_items'
    """
    )

    spark.sql(f"DROP TABLE IF EXISTS {database}.fact_clickstream_events")
    spark.sql(
        f"""
        CREATE TABLE {database}.fact_clickstream_events (
            event_id INT,
            user_id INT,
            event_type STRING,
            product_id INT,
            order_id INT,
            timestamp TIMESTAMP,
            created_ts TIMESTAMP,
            last_updated_by INT,
            last_updated_ts TIMESTAMP,
            etl_inserted TIMESTAMP
        ) USING DELTA
        PARTITIONED BY (etl_inserted)
        LOCATION '{path}/fact_clickstream_events'
    """
    )
