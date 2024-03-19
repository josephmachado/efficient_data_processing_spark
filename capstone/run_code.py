from pyspark.sql import DataFrame, SparkSession
from rainforest.etl.bronze.appuser import AppUserBronzeETL
from rainforest.etl.bronze.seller import SellerBronzeETL
from rainforest.etl.bronze.orders import OrdersSilverETL
from rainforest.etl.silver.dim_seller import DimSellerSilverETL
from rainforest.etl.silver.fct_orders import FactOrdersSilverETL
from rainforest.etl.gold.wide_orders import WideOrdersGoldETL
from rainforest.etl.gold.daily_order_metrics import DailyOrderMetricsGoldETL


def run_code(spark):
    """

    print("=================================")
    print("Running Bronze user ETL")
    print("=================================")
    bronze_user = AppUserBronzeETL(spark=spark)
    bronze_user.run()
    bronze_user.read().curr_data.show(10)
    print("=================================")
    print("Running Bronze Orders ETL")
    print("=================================")
    bronze_orders = OrdersSilverETL(spark=spark)
    bronze_orders.run()
    bronze_orders.read().curr_data.show(10)

    print("=================================")
    print("Running Bronze seller ETL")
    print("=================================")
    bronze_seller = SellerBronzeETL(spark=spark)
    bronze_seller.run()
    bronze_seller.read().curr_data.show(10)
    
    
    print("=================================")
    print("Running Silver dim_seller ETL")
    print("=================================")
    silver_dim_seller = DimSellerSilverETL(spark=spark, upstream_table_names=[AppUserBronzeETL, SellerBronzeETL])
    silver_dim_seller.run()
    silver_dim_seller.read().curr_data.show(10)

    print("=================================")
    print("Running Silver fct_orders ETL")
    print("=================================")
    silver_fct_orders = FactOrdersSilverETL(spark=spark)
    silver_fct_orders.run()
    silver_fct_orders.read().curr_data.show(10)
    
    print("=================================")
    print("Running Gold wide_orders ETL")
    print("=================================")
    gold_wide_orders = WideOrdersGoldETL(spark=spark)
    gold_wide_orders.run()
    gold_wide_orders.read().curr_data.show()
    """
    print("=================================")
    print("Running Gold dail_order_metrics ETL")
    print("=================================")
    gold_daily_order_metrics = DailyOrderMetricsGoldETL(spark=spark)
    gold_daily_order_metrics.run()
    gold_daily_order_metrics.read().curr_data.show()
    


if __name__ == "__main__":
    # Create a spark session
    # Pass spark session with a variable that controls which chapter exercises to run
    # The function should be able to accept the section number to run as well
    # Have a function to run the spark sql code as well
    spark = (
        SparkSession.builder.appName("adventureworks").enableHiveSupport().getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    run_code(spark)
