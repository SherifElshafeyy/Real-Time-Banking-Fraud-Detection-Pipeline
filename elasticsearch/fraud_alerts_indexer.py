from confluent_kafka import Consumer, KafkaException
from elasticsearch import Elasticsearch
import json
from datetime import datetime, timezone

es = Elasticsearch("http://localhost:9200")

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "fraud_alerts_consumer",
    "auto.offset.reset": "latest",       
    "enable.auto.commit": True
})

consumer.subscribe(["fraud_alerts"])

INDEX = "fraud_alerts"

def create_index():
    try:
        es.indices.get(index=INDEX)
        print(f"ℹ️ Index '{INDEX}' already exists")
    except Exception:
        es.indices.create(index=INDEX, mappings={
            "properties": {
                "user_id":      {"type": "keyword"},
                "alert_value":  {"type": "double"},
                "window_start": {"type": "date"},
                "window_end":   {"type": "date"},
                "alert_type":   {"type": "keyword"},
                "ingested_at":  {"type": "date"}
            }
        })
        print(f"✅ Index '{INDEX}' created")


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
            alert["ingested_at"] = datetime.now(timezone.utc).isoformat()

            #  composite key for deduplication
            doc_id = f"{alert['user_id']}_{alert['window_start']}_{alert['alert_type']}"

            try:
                #  op_type=create rejects duplicates atomically
                es.index(index=INDEX, id=doc_id, document=alert, op_type="create")
                print(f"📥 Indexed: {alert['alert_type']} | {alert['user_id']} | {alert['alert_value']}")
            except Exception:
                print(f"⏭️ Duplicate skipped: {alert['alert_type']} | {alert['user_id']}")

    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()

consume()