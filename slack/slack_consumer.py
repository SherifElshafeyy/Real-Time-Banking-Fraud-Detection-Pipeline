from confluent_kafka import Consumer, KafkaException
from datetime import datetime, timezone
import requests
import json
from dotenv import load_dotenv
import os

load_dotenv()

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")


consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "slack_consumer",
    "auto.offset.reset": "latest"
})

consumer.subscribe(["fraud_alerts"])


def should_send_to_slack(alert):
    alert_type = alert["alert_type"]
    alert_value = float(alert["alert_value"])

    if alert_type == "HIGH_AMOUNT" and alert_value > 50000:
        return True, "🔴 CRITICAL"

    if alert_type == "MULTI_COUNTRY" and alert_value > 3:
        return True, "🟠 HIGH"
    
    if alert_type=="FAILED" and alert_value > 4:
        return True , "🔴 CRITICAL"

    return False, None


def send_slack_alert(alert, severity):


    message = {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f" {severity} — Fraud Alert Detected"
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Alert Type:*\n{alert['alert_type']}"},
                    {"type": "mrkdwn", "text": f"*Severity:*\n{severity}"},
                    {"type": "mrkdwn", "text": f"*User:*\n{alert['user_id']}"},
                    {"type": "mrkdwn", "text": f"*Value:*\n{alert['alert_value']}"},
                    {"type": "mrkdwn", "text": f"*Window Start:*\n{alert['window_start']}"},
                    {"type": "mrkdwn", "text": f"*Window End:*\n{alert['window_end']}"},
                    
                ]
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": "⚠️ Immediate review required"
                    }
                ]
            }
        ]
    }

    response = requests.post(SLACK_WEBHOOK_URL, json=message)
    if response.status_code == 200:
        print(f"✅ Slack alert sent: {severity} | {alert['alert_type']} | {alert['user_id']}")
    else:
        print(f"❌ Slack failed: {response.status_code} {response.text}")


def consume():
    print("🚀 Slack consumer listening for fraud alerts...")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            alert = json.loads(msg.value().decode("utf-8"))
            

            should_send, severity = should_send_to_slack(alert)

            if should_send:
                send_slack_alert(alert, severity)
            else:
                print(f"⏭️ Skipped — below threshold: {alert['alert_type']} | {alert['alert_value']}")

    except KeyboardInterrupt:
        print("Stopping Slack consumer...")
    finally:
        consumer.close()

consume()