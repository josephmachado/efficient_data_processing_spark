FROM deltaio/delta-docker:latest
# latest is at 1.0.0_3.0.0, 

USER root

COPY ./requirements.txt ./
RUN pip install -r requirements.txt

# Copy SQL DDL and DML to SPARK_HOME dir
COPY ./setup.sql ./
COPY ./count.sql ./

# Copy tpch data generator into the container
COPY ./tpch-dbgen ./tpch-dbgen/

# Create and event logging directory to store job logs
RUN mkdir /tmp/spark-events

# Create the log file to be able to run tail
RUN touch /var/log/cron.log

# Run cron
ENTRYPOINT ["tail", "-f", "/dev/null"]