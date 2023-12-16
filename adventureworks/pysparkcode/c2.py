from pyspark.sql import SparkSession

def run_exercise(spark: SparkSession):
    print('=================================')
    print('Running Chapter 2, Exercise 1')
    print('=================================')
    # Set the default database to 'tpch'
    spark.sql("USE tpch")

    # Show tables
    spark.sql("SHOW TABLES").show()

    # Select all columns from the 'orders' table and limit the result to 5 rows
    spark.table("orders").limit(5).show()


    