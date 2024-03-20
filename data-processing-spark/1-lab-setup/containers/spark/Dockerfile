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

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    rsync && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY .//conf/spark-defaults.conf "$SPARK_HOME/conf/spark-defaults.conf"
ENV SPARK_CONF_DIR="$SPARK_HOME/conf"

# Create and event logging directory to store job logs
RUN mkdir /tmp/spark-events

# Create the log file to be able to run tail
RUN touch /var/log/cron.log

# Run cron
ENTRYPOINT ["tail", "-f", "/dev/null"]