from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, expr, lit
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from pysparkcode import c2, c3, c4, c5, c6, c7


def run_spark_sql_code(section: str, spark: SparkSession):
    pass


def run_pyspark_code(section: int, spark: SparkSession, exercise_num: int = 0):
    print("=================================")
    print("Running Pyspark Code")
    print("=================================")
    section_code_map = {
        2: c2.CodeRunner(),
        3: c3.CodeRunner(),
        4: c4.CodeRunner(),
        5: c5.CodeRunner(),
        6: c6.CodeRunner(),
        7: c7.CodeRunner(),
    }
    code_runner = section_code_map[section]
    code_runner.run_exercise(spark, exercise_num)


def run_book_code(
    section: str, spark: SparkSession, exercise_num: int = 0, runner: str = "pyspark"
):
    if runner == "pyspark":
        run_pyspark_code(section, spark, exercise_num)
    else:
        run_spark_sql_code(section, spark, exercise_num)


if __name__ == "__main__":
    # Create a spark session
    # Pass spark session with a variable that controls which chapter exercises to run
    # The function should be able to accept the section number to run as well
    # Have a function to run the spark sql code as well
    spark = (
        SparkSession.builder.appName("adventureworks").enableHiveSupport().getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    run_book_code(section=7, spark=spark, exercise_num=11)
