from pyspark.sql.functions import col


def create_daily_order_report_view(daily_order_metrics_data):
    # Rename columns
    renamed_data = daily_order_metrics_data.select(
        col("order_date").alias("Date"),
        col("total_price_sum").alias("Revenue"),
        col("total_price_mean").alias("Mean Revenue"),
    )

    # Create or replace a temporary view
    renamed_data.createOrReplaceGlobalTempView("daily_order_report")
