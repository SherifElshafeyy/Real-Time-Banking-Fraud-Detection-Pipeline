from confluent_kafka import Producer
import json, time


producer_config = {
    "bootstrap.servers": "localhost:9092"
}
producer = Producer(
    producer_config)


for i in range(10):
    msg = {"id": i, "message": f"test message {i}"}
    value= json.dumps(msg).encode("utf-8")
    producer.produce(topic='test-topic',
                     value=value)
    print(f"Sent: {msg}")
    time.sleep(1)

producer.flush()
print("Done.")