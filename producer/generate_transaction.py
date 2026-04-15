from confluent_kafka import Producer
import json
import random
import time
import uuid
from datetime import datetime as dt
import pycountry


users = [f"User_{i}" for i in range (1,1001)]
transaction_type = ["PAYMENT", "WITHDRAWAL", "TRANSFER", "DEPOSIT"]
currency_map = {
    "EGY": "EGP",  # Egypt
    "DEU": "EUR",  # Germany
    "FRA": "EUR",  # France
    "GBR": "GBP",  # United Kingdom
    "ZAF": "ZAR",  # South Africa
    "NGA": "NGN",  # Nigeria
    "CHN": "CNY",  # China
    "IND": "INR",  # India
    "ARE": "AED",  # UAE
    "JPN": "JPY",   # Japan
    "SAU": "SAR",  # Saudi Arabia
    "QAT": "QAR",  # Qatar
}
def generate_transaction():

    country = random.choice(list(currency_map.keys()))

    return {
        "transaction_id" : str(uuid.uuid4()),
        "user_id" : random.choice(users),
        "event_time" : dt.now().isoformat(),
        "transaction_type" : random.choice(transaction_type),
        "amount": round(random.uniform(0,10000),2),
        "country" : country,
        "currency" : currency_map[country],
        "ip_address": f"192.168.{random.randint(0,255)}.{random.randint(0,255)}",
        "status" : "SUCCESS" if random.random() < 0.95 else "FAILED"

    }

    
print(generate_transaction())

