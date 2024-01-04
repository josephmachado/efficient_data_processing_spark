up:
	docker compose up --build -d

down:
	docker compose down

restart: down up

sh:
	docker exec -ti local-spark bash

meta:
	PGPASSWORD=sdepassword pgcli -h localhost -p 5432 -U sdeuser -d metadatadb

pyspark:
	docker exec -ti local-spark bash -c '$$SPARK_HOME/bin/pyspark --packages io.delta:$${DELTA_PACKAGE_VERSION},org.apache.hadoop:hadoop-aws:3.3.2 --conf spark.hadoop.fs.s3a.access.key=minio --conf spark.hadoop.fs.s3a.secret.key=minio123 --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 --conf spark.hadoop.fs.s3a.region=us-east-1 --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem --conf spark.hadoop.fs.s3a.path.style.access=true --conf spark.databricks.delta.retentionDurationCheck.enabled=false --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog'

spark:
	docker exec -ti local-spark bash -c '$$SPARK_HOME/bin/spark-shell --packages io.delta:$${DELTA_PACKAGE_VERSION},org.apache.hadoop:hadoop-aws:3.3.2 --conf spark.hadoop.fs.s3a.access.key=minio --conf spark.hadoop.fs.s3a.secret.key=minio123 --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 --conf spark.hadoop.fs.s3a.region=us-east-1 --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem --conf spark.hadoop.fs.s3a.path.style.access=true --conf spark.databricks.delta.retentionDurationCheck.enabled=false --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog'

spark-sql:
	docker exec -ti local-spark bash -c '$$SPARK_HOME/bin/spark-sql --packages io.delta:$${DELTA_PACKAGE_VERSION},org.apache.hadoop:hadoop-aws:3.3.2 --conf spark.hadoop.fs.s3a.access.key=minio --conf spark.hadoop.fs.s3a.secret.key=minio123 --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 --conf spark.hadoop.fs.s3a.region=us-east-1 --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem --conf spark.hadoop.fs.s3a.path.style.access=true --conf spark.databricks.delta.retentionDurationCheck.enabled=false --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog'

datagen:
	docker exec -ti local-spark bash -c 'cd tpch-dbgen && make && ./dbgen -s 1'

create-tables:
	docker exec -ti local-spark bash -c '$$SPARK_HOME/bin/spark-sql --packages io.delta:$${DELTA_PACKAGE_VERSION},org.apache.hadoop:hadoop-aws:3.3.2 --conf spark.hadoop.fs.s3a.access.key=minio --conf spark.hadoop.fs.s3a.secret.key=minio123 --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 --conf spark.hadoop.fs.s3a.region=us-east-1 --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem --conf spark.hadoop.fs.s3a.path.style.access=true --conf spark.databricks.delta.retentionDurationCheck.enabled=false --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog --conf spark.eventLog.enabled=true  -f $$SPARK_HOME/work-dir/setup.sql'

count:
	docker exec -ti local-spark bash -c '$$SPARK_HOME/bin/spark-sql --packages io.delta:$${DELTA_PACKAGE_VERSION},org.apache.hadoop:hadoop-aws:3.3.2 --conf spark.hadoop.fs.s3a.access.key=minio --conf spark.hadoop.fs.s3a.secret.key=minio123 --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 --conf spark.hadoop.fs.s3a.region=us-east-1 --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem --conf spark.hadoop.fs.s3a.path.style.access=true --conf spark.databricks.delta.retentionDurationCheck.enabled=false --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog --conf spark.eventLog.enabled=true -f $$SPARK_HOME/work-dir/count.sql'

run-code:
	docker exec -ti local-spark bash -c '$$SPARK_HOME/bin/spark-submit --master local[*] --packages io.delta:$${DELTA_PACKAGE_VERSION},org.apache.hadoop:hadoop-aws:3.3.2 --conf spark.hadoop.fs.s3a.access.key=minio --conf spark.hadoop.fs.s3a.secret.key=minio123 --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 --conf spark.hadoop.fs.s3a.region=us-east-1 --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem --conf spark.hadoop.fs.s3a.path.style.access=true --conf spark.databricks.delta.retentionDurationCheck.enabled=false --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog --conf spark.eventLog.enabled=true $$SPARK_HOME/work-dir/adventureworks/run_code.py'

hserver:
	docker exec -ti local-spark bash -c '$$SPARK_HOME/sbin/start-history-server.sh'

setup: datagen create-tables hserver

hserver-ui:
	open http://localhost:18080

ui:
	open http://localhost:4040