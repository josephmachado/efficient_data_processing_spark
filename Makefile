up:
	docker compose up --build -d

down:
	docker compose down

restart: down up

sh:
	docker exec -ti local-spark bash

meta:
	PGPASSWORD=sdepassword pgcli -h localhost -p 5432 -U sdeuser -d metadatadb

spark-sh:
	docker exec -ti local-spark bash -c '$$SPARK_HOME/bin/spark-shell --packages io.delta:$${DELTA_PACKAGE_VERSION},org.apache.hadoop:hadoop-aws:3.3.2 --conf spark.hadoop.fs.s3a.access.key=minio --conf spark.hadoop.fs.s3a.secret.key=minio123 --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 --conf spark.hadoop.fs.s3a.region=us-east-1 --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem --conf spark.hadoop.fs.s3a.path.style.access=true --conf spark.databricks.delta.retentionDurationCheck.enabled=false --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog'

spark-sql:
	docker exec -ti local-spark bash -c '$$SPARK_HOME/bin/spark-sql --packages io.delta:$${DELTA_PACKAGE_VERSION},org.apache.hadoop:hadoop-aws:3.3.2 --conf spark.hadoop.fs.s3a.access.key=minio --conf spark.hadoop.fs.s3a.secret.key=minio123 --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 --conf spark.hadoop.fs.s3a.region=us-east-1 --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem --conf spark.hadoop.fs.s3a.path.style.access=true --conf spark.databricks.delta.retentionDurationCheck.enabled=false --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog'
