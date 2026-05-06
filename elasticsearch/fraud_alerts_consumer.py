from confluent_kafka import Consumer, KafkaException
from elasticsearch import Elasticsearch
import json
from datetime import datetime,timezone

# Elasticsearch connection 
es = Elasticsearch("http://localhost:9200")

#  Kafka Consumer 
consumer_config={
    "bootstrap.servers": "localhost:9092",
    "group.id": "fraud_alerts_consumer",
    "auto.offset.reset": "latest"
}



consumer = Consumer(consumer_config)

consumer.subscribe(["fraud_alerts"])

# Index name 
INDEX = "fraud_alerts"

def create_index():
    if not es.indices.exists(index=INDEX):
        es.indices.create(index=INDEX, mappings={
            "properties": {
                "user_id":       {"type": "keyword"},
                "alert_value":   {"type": "double"},
                "window_start":  {"type": "date"},
                "window_end":    {"type": "date"},
                "alert_type":    {"type": "keyword"},
                "ingested_at":   {"type": "date"}
            }
        })
        print(f"✅ Index '{INDEX}' created")
    else:
        print(f"ℹ️ Index '{INDEX}' already exists")

def consume():
    create_index()
    print(" Listening for fraud alerts...")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            alert = json.loads(msg.value().decode("utf-8"))

            # add ingestion timestamp
            alert["ingested_at"] = datetime.now(timezone.utc).isoformat()


            # sink to Elasticsearch
            es.index(index=INDEX, document=alert)

            print(f"📥 Indexed alert: {alert['alert_type']} | {alert['user_id']} | {alert['alert_value']}")

    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()

consume()