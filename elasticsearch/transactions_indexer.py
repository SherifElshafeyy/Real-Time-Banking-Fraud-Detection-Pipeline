from confluent_kafka import Consumer, KafkaException
from elasticsearch import Elasticsearch
import json
from datetime import datetime, timezone

es = Elasticsearch("http://localhost:9200")

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "transactions_raw_consumer",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True
})

consumer.subscribe(["transactions"])

INDEX = "transactions_raw"

def create_index():
    try:
        es.indices.get(index=INDEX)
        print(f"ℹ️ Index '{INDEX}' already exists")
    except Exception:
        es.indices.create(index=INDEX, mappings={
            "properties": {
                "transaction_id":   {"type": "keyword"},   
                "user_id":          {"type": "keyword"},
                "timestamp":        {"type": "date"},
                "transaction_type": {"type": "keyword"},
                "amount":           {"type": "double"},
                "country":          {"type": "keyword"},
                "currency":         {"type": "keyword"},
                "merchant":         {"type": "keyword"},
                "ip_address":       {"type": "keyword"},   
                "status":           {"type": "keyword"},
                "ingested_at":      {"type": "date"}
            }
        })
        print(f"✅ Index '{INDEX}' created")


def consume():
    create_index()
    print(" Listening for raw transactions...")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            transaction = json.loads(msg.value().decode("utf-8"))
            transaction["ingested_at"] = datetime.now(timezone.utc).isoformat()

            es.index(index=INDEX, document=transaction)

            # ✅ correct fields from transaction
            print(f" Indexed: {transaction['transaction_id']} | {transaction['user_id']} | {transaction['amount']}")

    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()

consume()