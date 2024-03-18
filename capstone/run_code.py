from pyspark.sql import DataFrame, SparkSession
from rainforest.etl.bronze.user import UserBronzeETL


def run_code(spark):
    print("=================================")
    print("Running Bronze user")
    print("=================================")
    bronze_user = UserBronzeETL(spark=spark)
    bronze_user.run()
    bronze_user.read().curr_data.show(10)

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
