def create_tables(
    spark, path="s3a://rainforest/delta", database: str = "rainforest"
):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

    # OBT Schema (Order Item Grain)
    spark.sql(f"DROP TABLE IF EXISTS {database}.obt_order_item")
    spark.sql(
        f"""
        CREATE TABLE {database}.obt_order_item (
            order_item_id INT,
            order_id INT,
            order_ts TIMESTAMP,
            total_price DECIMAL(10, 2),
            buyer_id INT,
            buyer_username STRING,
            buyer_email STRING,
            buyer_is_active BOOLEAN,
            buyer_first_time_purchased_timestamp TIMESTAMP,
            seller_id INT,
            seller_username STRING,
            seller_email STRING,
            seller_is_active BOOLEAN,
            seller_first_time_sold_timestamp TIMESTAMP,
            product_id INT,
            product_name STRING,
            product_description STRING,
            product_price DECIMAL(10, 2),
            brand_id INT,
            brand_name STRING,
            manufacturer_id INT,
            manufacturer_name STRING,
            review_count INT,
            avg_rating DECIMAL(3, 2),
            category_id INT,
            category_name STRING,
            quantity INT,
            base_price DECIMAL(10, 2),
            tax DECIMAL(10, 2),
            created_ts TIMESTAMP,
            last_updated_by INT,
            last_updated_ts TIMESTAMP,
            etl_inserted TIMESTAMP
        ) USING DELTA
        PARTITIONED BY (etl_inserted)
        LOCATION '{path}/obt_order_item'
    """
    )

    # OBR Schema (User Event Grain)
    spark.sql(f"DROP TABLE IF EXISTS {database}.obr_user_event")
    spark.sql(
        f"""
        CREATE TABLE {database}.obr_user_event (
            event_id INT,
            user_id INT,
            username STRING,
            email STRING,
            is_active BOOLEAN,
            is_buyer BOOLEAN,
            is_seller BOOLEAN,
            first_time_purchased_timestamp TIMESTAMP,
            first_time_sold_timestamp TIMESTAMP,
            event_type STRING,
            product_id INT,
            product_name STRING,
            product_description STRING,
            product_price DECIMAL(10, 2),
            brand_id INT,
            brand_name STRING,
            manufacturer_id INT,
            manufacturer_name STRING,
            review_count INT,
            avg_rating DECIMAL(3, 2),
            category_id INT,
            category_name STRING,
            order_id INT,
            order_ts TIMESTAMP,
            total_price DECIMAL(10, 2),
            timestamp TIMESTAMP,
            created_ts TIMESTAMP,
            last_updated_by INT,
            last_updated_ts TIMESTAMP,
            etl_inserted TIMESTAMP
        ) USING DELTA
        PARTITIONED BY (etl_inserted)
        LOCATION '{path}/obr_user_event'
    """
    )
