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
    def run_exercise_1(self, spark):
        print("=================================")
        print(" Simple Join to explain query plan")
        print("=================================")
        spark.sql("USE tpch")
        spark.conf.set("spark.sql.adaptive.enabled", False)
        spark.sql(
            """
                  SELECT
                    o.orderkey,
                    SUM(l.extendedprice * (1 - l.discount))
                    AS total_price_wo_tax
                FROM
                lineitem l
                    JOIN orders o ON l.orderkey = o.orderkey
                GROUP BY
                o.orderkey;
                  """
        ).show()

    def run_exercise(self, spark: SparkSession, exercise_num: int = 0):
        print("=================================")
        print("Query Planner")
        print("=================================")
        exercise_num_map = {
            1: self.run_exercise_1,
        }
        exercise_num_map[exercise_num](spark)
