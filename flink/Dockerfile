FROM flink:1.18.1

RUN apt-get update && \
    apt-get install -y python3 python3-pip wget && \
    ln -s /usr/bin/python3 /usr/bin/python

# ✅ Increased timeout to handle the large apache-flink package
RUN pip install --default-timeout=300 --retries=5 apache-flink==1.18.1

RUN wget -P /opt/flink/lib \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar

WORKDIR /app