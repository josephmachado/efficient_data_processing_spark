from pyspark.sql import SparkSession

class CodeRunner:
    def run_exercise(self, spark: SparkSession, exercise_num: int = 0):
        print('=================================')
        print('Running Chapter 2, Exercise 1')
        print('=================================')
        # Set the default database to 'tpch'
        spark.sql("USE tpch")

        # Show tables
        spark.sql("SHOW TABLES").show()

        # Select all columns from the 'orders' table and limit the result to 5 rows
        spark.table("orders").limit(5).show()


    