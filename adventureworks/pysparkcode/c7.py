from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    date_format,
    sum,
    round,
    avg,
    when,
    lag,
    row_number,
)
from pyspark.sql.window import Window

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    LongType,
    DateType,
)
import time


class CodeRunner:
    def run_exercise_71(self, spark):
        print("=================================")
        print("Use CTEs to write easy-to-understand SQL")
        print("=================================")
        pass

    def run_exercise_72(self, spark):
        print("=================================")
        print("Use CTEs to write easy-to-understand SQL")
        print("=================================")
        pass

    def run_exercise(self, spark: SparkSession, exercise_num: int = 0):
        print("=================================")
        print("Chapter 7")
        print("=================================")
        exercise_num_map = {
            1: self.run_exercise_71,
            2: self.run_exercise_72,
        }
        exercise_num_map[exercise_num](spark)
