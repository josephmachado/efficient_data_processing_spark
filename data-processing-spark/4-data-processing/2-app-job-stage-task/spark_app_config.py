from pyspark.sql import SparkSession


def run_code(spark):
    print("==========================================")
    print(f"Running a simple query with {spark.sparkContext.getConf().get('spark.app.name')}")
    print("==========================================")

    spark.sql(
        """
        SELECT returnflag, linestatus, SUM(quantity) as total_quantity, SUM(extendedprice) as total_revenue
        FROM tpch.lineitem
        WHERE shipdate >= '1994-01-01' AND shipdate < '1995-01-01'
        GROUP BY returnflag, linestatus;
        """
    ).show(10)

if __name__ == '__main__':

    spark = (
        SparkSession.builder.appName("Custom config")
        .config("spark.executor.memory", "2g") 
        .config("spark.executor.cores", "3") # total cores across all executors
        .config("spark.cores.max", "3") 
        .config("spark.memory.fraction", "0.9") # set aside 10% for user memory, rest for Spark data processing
        .enableHiveSupport()
        .getOrCreate()
    )
    # Set the log level
    spark.sparkContext.setLogLevel("ERROR")
    run_code(spark=spark)
    spark.stop()

