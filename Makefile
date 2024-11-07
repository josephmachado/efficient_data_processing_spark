## Cluster management

build:
	docker compose build spark-master

docker-up:
	docker compose up --build -d --scale spark-worker=2

up: build docker-up

rm-logs:
	docker volume rm -f efficient_data_processing_spark_tpch-data efficient_data_processing_spark_spark-logs

down:
	docker compose down

restart: down rm-logs build up

sh:
	docker exec -ti spark-master bash

## Data generation

fake-datagen:
	docker exec spark-master bash -c "python3 /opt/spark/work-dir/capstone/upstream_datagen/datagen.py"

datagen:
	docker exec -ti spark-master bash -c 'cd tpch-dbgen && make && ./dbgen -s 1'

upstream:
	PGPASSWORD=sdepassword pgcli -h localhost -p 5432 -U sdeuser -d upstreamdb

## Create tables

create-buckets:
	docker exec spark-master bash -c "python3 /opt/spark/work-dir/create_buckets.py"

create-tables:
	docker exec spark-master spark-sql --master spark://spark-master:7077 --deploy-mode client -f ./setup.sql

count-tables:
	docker exec spark-master spark-sql --master spark://spark-master:7077 --deploy-mode client -f ./count.sql

setup: datagen fake-datagen create-buckets create-tables 

## Spark UIs: master UI, Spark application UI & History Server UI

hserver-ui:
	open http://localhost:18080

ui:
	open http://localhost:4040

master-ui:
	open http://localhost:9090

## Start Pyspark and Spark SQL REPL sessions

pyspark:
	docker exec -ti spark-master bash pyspark --master spark://spark-master:7077 

spark-sql:
	docker exec -ti spark-master spark-sql --master spark://spark-master:7077 

## Pyspark runner

cq: 
	@read -p "Enter .sql relative path:" sql_path; docker exec -ti spark-master spark-sql --master spark://spark-master:7077 $$sql_path

cr: 
	@read -p "Enter pyspark relative path:" pyspark_path; docker exec -ti spark-master spark-submit --master spark://spark-master:7077 $$pyspark_path

## Project

rainforest:
	docker exec spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client ./capstone/run_code.py

## Testing, Linting, Type checks and Formatting

pytest:
	docker exec spark-master bash -c 'export PYTHONPATH=/opt/spark/work-dir/capstone && python3 -m pytest --log-cli-level info -p no:warnings -v ./capstone/rainforest/tests'

format:
	docker exec spark-master black -S --line-length 79 --preview ./capstone ./data-processing-spark
	docker exec spark-master isort ./data-processing-spark ./capstone

type:
	docker exec spark-master mypy --no-implicit-reexport --ignore-missing-imports --no-namespace-packages ./data-processing-spark ./capstone

lint:
	docker exec spark-master flake8 ./data-processing-spark
	docker exec spark-master flake8 ./capstone

ci: format type lint

## Jupyter server

notebook:
	docker exec spark-master bash -c "jupyter notebook --ip=0.0.0.0 --port=3000 --allow-root"
