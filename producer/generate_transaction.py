from confluent_kafka import Producer
import json
import random
import time
import uuid
from datetime import datetime as dt






users = [f"User_{i}" for i in range (1,1001)]
transaction_types = ["PAYMENT", "WITHDRAWAL", "TRANSFER", "DEPOSIT"]
currency_map = {
    "EGY": "EGP",  
    "DEU": "EUR",  # Germany
    "FRA": "EUR",  # France
    "GBR": "GBP",  # United Kingdom
    "CHN": "CNY",  # China
    "ARE": "AED",  # UAE
    "JPN": "JPY",  
    "SAU": "SAR",  
    "QAT": "QAR", 
    'USA': 'USD'
}
def generate_transaction():
    
    #Countrty Choice
    country = random.choice(list(currency_map.keys()))

    #Amount Choice
    if random.random() < 0.9:
        amount=round(random.uniform(1, 5000), 2)   # normal transactions
                 
    else:
        amount = round(random.uniform(5001, 200000), 2)  # rare large ones
    
    #Transaction Type Cases
    transaction_type = random.choice(transaction_types)
    if transaction_type =='PAYMENT':
        merchant = random.choice(["Amazon", "Netflix", "Uber", "Apple", "Walmart", "Spotify"])
    else:
        merchant = None

    return {
        "transaction_id" : str(uuid.uuid4()),
        "user_id" : random.choice(users),
        "timestamp" : dt.now().isoformat(),
        "transaction_type" : transaction_type,
        "amount": amount,
        "country" : country,
        "currency" : currency_map[country],
        "merchant" : merchant,
        "ip_address": f"192.168.{random.randint(0,255)}.{random.randint(0,255)}",
        "status" : "SUCCESS" if random.random() < 0.95 else "FAILED"

    }

def validate(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        
        print(f"✅ Delivered to {msg.topic()} : partition {msg.partition()} : at offset {msg.offset()}")



def transaction_producer():
    producer = Producer({
        "bootstrap.servers": "localhost:9092",
        "client.id": "transaction-producer"
    })

    try:
        while True:
            transaction = generate_transaction()
            value = json.dumps(transaction).encode("utf-8")

            producer.produce(
                key=transaction["user_id"],
                topic='transactions',
                value=value,
                callback=validate
                
            )

            producer.poll(0)
            time.sleep(5)
            

    except KeyboardInterrupt:
        print("Stopping producer...")
        producer.flush()

transaction_producer()