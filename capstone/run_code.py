from pyspark.sql import SparkSession
from rainforest.etl.gold.daily_category_metrics import \
    DailyCategoryMetricsGoldETL
from rainforest.etl.gold.daily_order_metrics import DailyOrderMetricsGoldETL
from rainforest.etl.interface.daily_category_report import \
    create_daily_category_report_view
from rainforest.etl.interface.daily_order_report import \
    create_daily_order_report_view


def run_code(spark):
    print("=================================")
    print("Daily Category Report")
    print("=================================")
    daily_cat_metrics = DailyCategoryMetricsGoldETL(spark=spark)
    daily_cat_metrics.run()
    create_daily_category_report_view(daily_cat_metrics.read().curr_data)
    spark.sql("select * from global_temp.daily_category_report").show()

    print("=================================")
    print("Daily Order Report")
    print("=================================")
    gold_daily_order_metrics = DailyOrderMetricsGoldETL(
        spark=spark, load_data=False
    )
    gold_daily_order_metrics.run()
    create_daily_order_report_view(gold_daily_order_metrics.read().curr_data)
    spark.sql("select * from global_temp.daily_order_report").show()


if __name__ == "__main__":
    # Create a spark session
    spark = (
        SparkSession.builder.appName("Rainforest Data Pipeline")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    run_code(spark)
