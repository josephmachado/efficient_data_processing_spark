up:
	docker compose up --build -d --scale spark-worker=2

down:
	docker compose down

restart: down up

sh:
	docker exec -ti spark-master bash

meta:
	PGPASSWORD=sdepassword pgcli -h localhost -p 5432 -U sdeuser -d upstreamdb

fake-datagen:
	python3 capstone/upstream_datagen/datagen.py

pyspark:
	docker exec -ti spark-master bash pyspark --master spark://spark-master:7077 

datagen:
	docker exec -ti spark-master bash -c 'cd tpch-dbgen && make && ./dbgen -s 1'

spark-sql:
	docker exec -ti spark-master spark-sql --master spark://spark-master:7077 

create-tables:
	docker exec spark-master spark-sql --master spark://spark-master:7077 --deploy-mode client -f ./setup.sql

count-tables:
	docker exec spark-master spark-sql --master spark://spark-master:7077 --deploy-mode client -f ./count.sql

rainforest:
	docker exec spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client ./capstone/run_code.py

setup: datagen fake-datagen create-tables 

hserver-ui:
	open http://localhost:18080

ui:
	open http://localhost:4040

cr: 
	@read -p "Enter pyspark relative path:" pyspark_path; docker exec -ti spark-master spark-submit --master spark://spark-master:7077 $$pyspark_path