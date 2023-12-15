from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, expr, lit
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

def run_spark_sql_code(section: str):
    pass

def run_pyspark_code(section: str):
    pass

def run_book_code(section: str, runner: str = 'pyspark'):
    if runner == 'pyspark':
        run_pyspark_code(section)
    else:
        run_spark_sql_code(section)

if __name__ == '__main__':
    # Create a spark session
    # Pass spark session with a variable that controls which chapter exercises to run 
    # The function should be able to accept the section number to run as well
    # Have a function to run the spark sql code as well 
    run_book_code(section=0)