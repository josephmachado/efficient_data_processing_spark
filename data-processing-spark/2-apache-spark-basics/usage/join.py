from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def run_code(spark):


if __name__ == '__main__':
    spark = (
        SparkSession.builder.appName("adventureworks")
        .enableHiveSupport()
        .getOrCreate()
    )
    run_code(spark=spark)
    spark.stop