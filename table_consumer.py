#!/usr/bin/env python3
# table_consumer.py
import os, json
from kafka import KafkaConsumer
from datetime import datetime
from dotenv import load_dotenv

# Load .env
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

TOPIC = os.getenv("TABLE_TOPIC", "table-batches")
BOOT = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
USE_SSL = os.getenv("KAFKA_USE_SSL", "0") == "1"

def make_consumer():
    if USE_SSL:
        return KafkaConsumer(
            TOPIC,
            bootstrap_servers="kafka-demo-gsvarunkumar04-aebd.i.aivencloud.com:15498",
            security_protocol="SSL",
            ssl_cafile=os.getenv("SSL_CAFILE"),
            ssl_certfile=os.getenv("SSL_CERTFILE"),
            ssl_keyfile=os.getenv("SSL_KEYFILE"),
            value_deserializer=lambda v: json.loads(v.decode("utf-8"))
        )
    else:
        return KafkaConsumer(
            TOPIC,
            bootstrap_servers="kafka-demo-gsvarunkumar04-aebd.i.aivencloud.com:15498",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="latest",
            consumer_timeout_ms=1000
        )

if __name__ == "__main__":
    c = make_consumer()
    print("Consumer started, listening (ctrl-c to stop)...")
    try:
        for msg in c:
            print(datetime.utcnow().isoformat(), msg.value)
    except KeyboardInterrupt:
        print("Stopped")
    finally:
        c.close()
