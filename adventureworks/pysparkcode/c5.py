from pyspark.sql import SparkSession
from pyspark.sql.functions import round, sum, col, count, date_add, lit, year
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
    def run_exercise_52(self, spark):
        print("=================================")
        print(
            "Window = A set of rows identified by values present in one or more column(s)"
        )
        print("=================================")

    def run_exercise(self, spark: SparkSession, exercise_num: int = 0):
        print("=================================")
        print("Chapter 5")
        print("=================================")
        exercise_num_map = {
            2: self.run_exercise_52,
            3: self.run_exercise_53,
            4: self.run_exercise_54,
            5: self.run_exercise_55,
            6: self.run_exercise_56,
        }
        exercise_num_map[exercise_num](spark)
