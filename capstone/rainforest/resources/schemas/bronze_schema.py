from pyspark.sql import SparkSession


def create_tables(spark, path="s3a://rainforest/delta", database: str = "rainforest"):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

    # User table
    spark.sql(f"DROP TABLE IF EXISTS {database}.user")
    spark.sql(
        f"""
        CREATE TABLE {database}.user (
            user_id INT,
            username STRING,
            email STRING,
            is_active BOOLEAN,
            created_ts TIMESTAMP,
            last_updated_by INT,
            last_updated_ts TIMESTAMP,
            etl_inserted TIMESTAMP
        ) USING DELTA
        PARTITIONED BY (etl_inserted)
        LOCATION '{path}/user'
    """
    )

    # Seller table
    spark.sql(f"DROP TABLE IF EXISTS {database}.seller")
    spark.sql(
        f"""
        CREATE TABLE {database}.seller (
            seller_id INT,
            user_id INT,
            first_time_sold_timestamp TIMESTAMP,
            created_ts TIMESTAMP,
            last_updated_by INT,
            last_updated_ts TIMESTAMP,
            etl_inserted TIMESTAMP
        ) USING DELTA
        PARTITIONED BY (etl_inserted)
        LOCATION '{path}/seller'
    """
    )

    # Buyer table
    spark.sql(f"DROP TABLE IF EXISTS {database}.buyer")
    spark.sql(
        f"""
        CREATE TABLE {database}.buyer (
            buyer_id INT,
            user_id INT,
            first_time_purchased_timestamp TIMESTAMP,
            created_ts TIMESTAMP,
            last_updated_by INT,
            last_updated_ts TIMESTAMP,
            etl_inserted TIMESTAMP
        ) USING DELTA
        PARTITIONED BY (etl_inserted)
        LOCATION '{path}/buyer'
    """
    )

    # SellerProduct table
    spark.sql(f"DROP TABLE IF EXISTS {database}.seller_product")
    spark.sql(
        f"""
        CREATE TABLE {database}.seller_product (
            seller_id INT,
            product_id INT,
            etl_inserted TIMESTAMP
        ) USING DELTA
        PARTITIONED BY (etl_inserted)
        LOCATION '{path}/seller_product'
    """
    )

    # Brand table
    spark.sql(f"DROP TABLE IF EXISTS {database}.brand")
    spark.sql(
        f"""
        CREATE TABLE {database}.brand (
            brand_id INT,
            name STRING,
            created_ts TIMESTAMP,
            last_updated_by INT,
            last_updated_ts TIMESTAMP,
            etl_inserted TIMESTAMP
        ) USING DELTA
        PARTITIONED BY (etl_inserted)
        LOCATION '{path}/brand'
    """
    )

    # Manufacturer table
    spark.sql(f"DROP TABLE IF EXISTS {database}.manufacturer")
    spark.sql(
        f"""
        CREATE TABLE {database}.manufacturer (
            manufacturer_id INT,
            name STRING,
            created_ts TIMESTAMP,
            last_updated_by INT,
            last_updated_ts TIMESTAMP,
            etl_inserted TIMESTAMP
        ) USING DELTA
        PARTITIONED BY (etl_inserted)
        LOCATION '{path}/manufacturer'
    """
    )

    # Product table
    spark.sql(f"DROP TABLE IF EXISTS {database}.product")
    spark.sql(
        f"""
        CREATE TABLE {database}.product (
            product_id INT,
            name STRING,
            description STRING,
            price DECIMAL(10, 2),
            brand_id INT,
            manufacturer_id INT,
            created_ts TIMESTAMP,
            last_updated_by INT,
            last_updated_ts TIMESTAMP,
            etl_inserted TIMESTAMP
        ) USING DELTA
        PARTITIONED BY (etl_inserted)
        LOCATION '{path}/product'
    """
    )

    # Reviews table
    spark.sql(f"DROP TABLE IF EXISTS {database}.reviews")
    spark.sql(
        f"""
        CREATE TABLE {database}.reviews (
            review_id INT,
            product_id INT,
            rating INT,
            comment STRING,
            created_ts TIMESTAMP,
            last_updated_by INT,
            last_updated_ts TIMESTAMP,
            etl_inserted TIMESTAMP
        ) USING DELTA
        PARTITIONED BY (etl_inserted)
        LOCATION '{path}/reviews'
    """
    )

    # ProductCategory table
    spark.sql(f"DROP TABLE IF EXISTS {database}.product_category")
    spark.sql(
        f"""
        CREATE TABLE {database}.product_category (
            product_id INT,
            category_id INT,
            etl_inserted TIMESTAMP
        ) USING DELTA
        PARTITIONED BY (etl_inserted)
        LOCATION '{path}/product_category'
    """
    )

    # Category table
    spark.sql(f"DROP TABLE IF EXISTS {database}.category")
    spark.sql(
        f"""
        CREATE TABLE {database}.category (
            category_id INT,
            name STRING,
            created_ts TIMESTAMP,
            last_updated_by INT,
            last_updated_ts TIMESTAMP,
            etl_inserted TIMESTAMP
        ) USING DELTA
        PARTITIONED BY (etl_inserted)
        LOCATION '{path}/category'
    """
    )

    # Order table
    spark.sql(f"DROP TABLE IF EXISTS {database}.order")
    spark.sql(
        f"""
        CREATE TABLE {database}.order (
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
        LOCATION '{path}/order'
    """
    )

    # OrderItem table
    spark.sql(f"DROP TABLE IF EXISTS {database}.order_item")
    spark.sql(
        f"""
        CREATE TABLE {database}.order_item (
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
        LOCATION '{path}/order_item'
    """
    )

    # Clickstream table
    spark.sql(f"DROP TABLE IF EXISTS {database}.clickstream")
    spark.sql(
        f"""
        CREATE TABLE {database}.clickstream (
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
        LOCATION '{path}/clickstream'
    """
    )



if __name__ == '__main__':
    spark = (
        SparkSession.builder.appName("rainforest_ddl")
        .config("spark.executor.cores", "1")
        .config("spark.executor.instances", "1")
        .enableHiveSupport()
        .getOrCreate()
    )
    create_tables(spark)