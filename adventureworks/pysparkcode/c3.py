from pyspark.sql import SparkSession
from pyspark.sql.functions import round, sum, expr

class CodeRunner:
    
    def run_exercise(self, spark: SparkSession , exercise_num: int = 0):
        print('=================================')
        print('Running Chapter 3, Exercise 1')
        print('=================================')
        # Set the default database to 'tpch'
        spark.sql("USE tpch")

        # Calculate the totalprice of an order (with orderkey = 1) from its individual items using DataFrame API
        lineitem_df = spark.table("lineitem")
        totalprice_df = (
            lineitem_df
            .filter("orderkey = 1")
            .groupBy("orderkey")
            .agg(
                round(
                    sum(expr("extendedprice * (1 - discount) * (1 + tax)")),
                    2
                ).alias("totalprice")
            )
        ) 
        totalprice_df.show() 

        # The totalprice of an order (with orderkey = 1) from the 'orders' table using DataFrame API
        orders_df = spark.table("orders")
        result_df = orders_df.select("orderkey", "totalprice").filter("orderkey = 1")

        # Show the result
        result_df.show()

        # Select all columns from the 'orders' table and limit the result to 5 rows
        spark.table("orders").limit(5).show()


    