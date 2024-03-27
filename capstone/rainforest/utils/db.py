import os

from pyspark.sql import SparkSession


def get_upstream_table(table_name: str, spark: SparkSession):
    host = os.getenv("UPSTREAM_HOST", "upstream")
    port = os.getenv("UPSTREAM_PORT", "5432")
    db = os.getenv("UPSTREAM_DATABASE", "upstreamdb")
    jdbc_url = f'jdbc:postgresql://{host}:{port}/{db}'
    connection_properties = {
        "user": os.getenv("UPSTREAM_USERNAME", "sdeuser"),
        "password": os.getenv("UPSTREAM_PASSWORD", "sdepassword"),
        "driver": "org.postgresql.Driver",
    }
    return spark.read.jdbc(
        url=jdbc_url, table=table_name, properties=connection_properties
    )
