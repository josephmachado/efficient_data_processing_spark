from pyspark.sql import DataFrame, SparkSession
from rainforest.etl.bronze.appuser import AppUserBronzeETL
from rainforest.etl.bronze.seller import SellerBronzeETL
from rainforest.etl.bronze.orders import OrdersSilverETL
from rainforest.etl.silver.dim_seller import DimSellerSilverETL
from rainforest.etl.silver.fct_orders import FactOrdersSilverETL
from rainforest.etl.gold.wide_orders import WideOrdersGoldETL
from rainforest.etl.gold.daily_order_metrics import DailyOrderMetricsGoldETL
from rainforest.etl.interface.daily_order_report import create_daily_order_report_view
from rainforest.etl.interface.daily_category_report import create_daily_category_report_view


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
    
    print("=================================")
    print("Running Gold dail_order_metrics ETL")
    print("=================================")
    
    from rainforest.etl.silver.dim_buyer import DimBuyerSilverETL

    print("=================================")
    print("Running Silver dim_buyer ETL")
    print("=================================")
    silver_dim_buyer = DimBuyerSilverETL(spark=spark)
    silver_dim_buyer.run()
    silver_dim_buyer.read().curr_data.show(10)

    from rainforest.etl.silver.dim_category import DimCategorySilverETL

    print("=================================")
    print("Running Silver dim_category ETL")
    print("=================================")
    silver_dim_category = DimCategorySilverETL(spark=spark)
    silver_dim_category.run()
    silver_dim_category.read().curr_data.show(10)

    from rainforest.etl.silver.seller_x_product import SellerProductSilverETL

    print("=================================")
    print("Running Silver seller_x_product ETL")
    print("=================================")
    silver_seller_x_product = SellerProductSilverETL(spark=spark)
    silver_seller_x_product.run()
    silver_seller_x_product.read().curr_data.show(10)
    
    from rainforest.etl.silver.dim_product import DimProductSilverETL

    print("=================================")
    print("Running Silver dim_product ETL")
    print("=================================")
    silver_dim_product = DimProductSilverETL(spark=spark)
    silver_dim_product.run()
    silver_dim_product.read().curr_data.show(10)
    
    from rainforest.etl.silver.product_x_category import ProductCategorySilverETL

    print("=================================")
    print("Running Silver seller_x_product ETL")
    print("=================================")
    silver_seller_x_product = ProductCategorySilverETL(spark=spark)
    silver_seller_x_product.run()
    silver_seller_x_product.read().curr_data.show(10)
    

    from rainforest.etl.gold.wide_order_items import WideOrderItemsGoldETL
    print("=================================")
    print("Running wide order items ETL")
    print("=================================")
    silver_seller_x_product = WideOrderItemsGoldETL(spark=spark)
    silver_seller_x_product.run()
    silver_seller_x_product.read().curr_data.show(10)
    """

    from rainforest.etl.gold.daily_category_metrics import DailyCategoryMetricsGoldETL
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
    gold_daily_order_metrics = DailyOrderMetricsGoldETL(spark=spark)
    gold_daily_order_metrics.run()
    create_daily_order_report_view(gold_daily_order_metrics.read().curr_data)
    spark.sql("select * from global_temp.daily_order_report").show()

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
