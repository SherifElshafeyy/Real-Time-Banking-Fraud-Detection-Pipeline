FROM apache/spark:3.4.2-scala2.12-java11-python3-ubuntu

USER root
WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pre-download Kafka + ES connectors so spark-submit doesn't fetch at runtime
RUN /opt/spark/bin/spark-submit \
    --packages \
      org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.2,\
      org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 \
    --help > /dev/null 2>&1 || true

# Copy jobs
COPY jobs/ ./jobs/

# Default entrypoint — keeps container alive so you can exec into it
# or override CMD when running a specific job
CMD ["tail", "-f", "/dev/null"]