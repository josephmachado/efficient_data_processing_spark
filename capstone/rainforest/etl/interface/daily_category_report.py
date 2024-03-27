from pyspark.sql.functions import col


def create_daily_category_report_view(daily_category_metrics_data):
    # Rename columns
    renamed_data = daily_category_metrics_data.select(
        col("order_date").alias("Date"),
        col("category").alias("Product Category"),
        col("mean_actual_price").alias("Mean Revenue"),
        col("median_actual_price").alias("Median Revenue"),
    )

    # Create or replace a temporary view
    renamed_data.createOrReplaceGlobalTempView("daily_category_report")
