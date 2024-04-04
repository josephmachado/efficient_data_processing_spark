from pyspark.sql import SparkSession
from pyspark.sql.functions import (abs, ceil, col, concat, date_add,
                                   date_format, datediff, floor, length, lit,
                                   months_between, round, split, substring,
                                   to_date, to_timestamp, trim, year)


def run_code(spark):
    print("=======================================")
    print("Using tpch database")
    print("=======================================")
    spark.sql("USE tpch")

    print("=======================================")
    print("Calculating length of 'hi'")
    print("=======================================")
    length_df = spark.createDataFrame([('hi',)], ['string'])
    length_df = length_df.select(length(length_df['string']).alias('length'))
    length_df.show()

    print("=======================================")
    print("Concatenating clerk and orderpriority with '-'")
    print("=======================================")
    concat_df = (
        spark.table("orders")
        .select(
            concat(col("clerk"), lit("-"), col("orderpriority")).alias(
                "concatenated"
            )
        )
        .limit(5)
    )
    concat_df.show()

    print("=======================================")
    print("Splitting clerk column by '#' delimiter")
    print("=======================================")
    split_df = (
        spark.table("orders")
        .select(split(col("clerk"), "#").alias("split_clerk"))
        .limit(5)
    )
    split_df.show()

    print("=======================================")
    print("Extracting first 5 characters of clerk column")
    print("=======================================")
    substr_df = (
        spark.table("orders")
        .select(
            col("clerk"), substring(col("clerk"), 1, 5).alias("substr_clerk")
        )
        .limit(5)
    )
    substr_df.show()

    print("=======================================")
    print("Trimming whitespace from ' hi '")
    print("=======================================")
    trim_df = spark.createDataFrame([(' hi ',)], ['string'])
    trim_df = trim_df.select(trim(trim_df['string']).alias('trimmed_string'))
    trim_df.show()

    print("=======================================")
    print("Performing date calculations")
    print("=======================================")
    fake_df = spark.createDataFrame([(' hi ',)], ['string'])
    date_diff_df = fake_df.select(
        datediff(lit('2022-11-05'), lit('2022-10-01')).alias("diff_in_days"),
        months_between(lit('2022-11-05'), lit('2022-10-01')).alias(
            "diff_in_months"
        ),
        (months_between(lit('2022-11-05'), lit('2022-10-01')) / 12).alias(
            "diff_in_years"
        ),
        date_add(lit('2022-11-05'), 10).alias("new_date"),
    )
    date_diff_df.show()

    print("=======================================")
    print("Adding 10 days to '2022-11-05'")
    print("=======================================")
    date_interval_df = fake_df.select(
        date_add(lit('2022-11-05'), 10).alias("new_date")
    )
    date_interval_df.show()

    print("=======================================")
    print("Parsing '11-05-2023' as date")
    print("=======================================")
    parsed_date_df = fake_df.select(
        to_date(lit('11-05-2023'), 'MM-dd-yyyy').alias("parsed_date"),
        to_timestamp(lit('11-05-2023'), 'MM-dd-yyyy').alias("parsed_date"),
    )
    parsed_date_df.show()

    print("=======================================")
    print("Formatting orderdate to first day of month")
    print("=======================================")
    first_month_df = (
        spark.table("orders")
        .select(
            date_format(col("orderdate"), 'yyyy-MM-01').alias(
                "first_month_date"
            )
        )
        .limit(5)
    )
    first_month_df.show()

    print("=======================================")
    print("Extracting year from '2023-11-05'")
    print("=======================================")
    year_df = fake_df.select(year(lit('2023-11-05')).alias("year"))
    year_df.show()

    print("=======================================")
    print("Rounding 100.102345 to 2 decimal places")
    print("=======================================")
    round_df = fake_df.select(
        round(lit(100.102345), 2).alias("rounded_number")
    )
    round_df.show()

    print("=======================================")
    print("Calculating absolute values of -100 and 100")
    print("=======================================")
    abs_df = fake_df.select(
        abs(lit(-100)).alias("abs_negative"),
        abs(lit(100)).alias("abs_positive"),
    )
    abs_df.show()

    print("=======================================")
    print("Calculating ceiling and floor of 100.1")
    print("=======================================")
    ceil_floor_df = fake_df.select(
        ceil(lit(100.1)).alias("ceiling"), floor(lit(100.1)).alias("floor")
    )
    ceil_floor_df.show()


if __name__ == '__main__':
    spark = (
        SparkSession.builder.appName("efficient-data-processing-spark")
        .enableHiveSupport()
        .getOrCreate()
    )
    # Set the log level
    spark.sparkContext.setLogLevel("ERROR")
    run_code(spark=spark)
    spark.stop
