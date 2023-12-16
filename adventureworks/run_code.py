from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, expr, lit
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from pysparkcode.c2 import run_exercise as c2_run_exercise

def run_spark_sql_code(section: str, spark: SparkSession):
        pass

def run_pyspark_code(section: str, spark: SparkSession):
    print('=================================')
    print('Running Pyspark Code')
    print('=================================')
    #
    if section == 2:
        c2_run_exercise(spark)

def run_book_code(section: str, spark: SparkSession, runner: str = 'pyspark'):
    if runner == 'pyspark':
        run_pyspark_code(section, spark)
    else:
        run_spark_sql_code(section)

if __name__ == '__main__':
    # Create a spark session
    # Pass spark session with a variable that controls which chapter exercises to run 
    # The function should be able to accept the section number to run as well
    # Have a function to run the spark sql code as well 
    spark = (
        SparkSession.builder.appName("adventureworks")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    run_book_code(section=2, spark=spark)