from pyspark.sql import SparkSession


def run_code(spark):
    print("=================================")
    print("Create a table, insert data, delete data, and drop the table")
    print("=================================")
    spark.sql("USE tpch")
    print("=================================")
    print("Create a sample table")
    print("=================================")
    spark.sql("DROP TABLE IF EXISTS sample_table2")
    spark.sql(
        """
        CREATE TABLE sample_table2 (sample_key bigint, sample_status STRING)
        USING delta LOCATION 's3a://tpch/sample_table2'
    """
    )

    print("=================================")
    print("SELECT * FROM sample_table2")
    print("=================================")
    sample_table2_df = spark.table("sample_table2")
    sample_table2_df.show()

    print("=================================")
    print("INSERT INTO sample_table2 VALUES (1, 'hello')")
    print("=================================")
    values_to_insert = [(1, "hello")]
    values_df = spark.createDataFrame(
        values_to_insert, ["sample_key", "sample_status"]
    )
    sample_table2_df = sample_table2_df.union(values_df)
    sample_table2_df.show()

    print("=================================")
    print("INSERT INTO sample_table2 SELECT nationkey, name FROM nation")
    print("=================================")
    nation_df = spark.table("nation")
    sample_table2_df = sample_table2_df.union(
        nation_df.select("nationkey", "name")
    )
    sample_table2_df.show()

    print("=================================")
    print("DELETE FROM sample_table2")
    print("=================================")
    sample_table2_df = sample_table2_df.filter("1 = 0")  # Empty DataFrame
    sample_table2_df.show()

    print("=================================")
    print("DROP TABLE sample_table2")
    print("=================================")
    spark.sql("DROP TABLE IF EXISTS sample_table2")


if __name__ == '__main__':
    spark = (
        SparkSession.builder.appName("efficient-data-processing-spark")
        .enableHiveSupport()
        .getOrCreate()
    )
    # Set the log level
    spark.sparkContext.setLogLevel("ERROR")
    run_code(spark=spark)
    spark.stop()
