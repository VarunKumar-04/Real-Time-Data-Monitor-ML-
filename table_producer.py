# table_batch_producer.py
import os
import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from dotenv import load_dotenv
load_dotenv("/home/varun/table-project/.env")


# load env or fallback
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("TABLE_TOPIC", "table-batches")
SLEEP_SECONDS = float(os.getenv("PRODUCER_SLEEP", "3.0"))  # lower -> heavier load
USE_SSL = os.getenv("KAFKA_USE_SSL", "0") == "1"
PROJECT_DIR = os.getenv("PROJECT_DIR", os.getcwd())

def now_ts(fmt="%Y-%m-%d-%H:%M:%S"):
    return datetime.utcnow().strftime(fmt)

def build_batch_id():
    return datetime.utcnow().strftime("%Y%m%d%H%M%S")  # simple ~ pattern

def make_message(batch_index):
    # basic table metadata
    batch_id = f"{batch_index}{build_batch_id()}"
    run_id = batch_index % 5 + 1
    table_id = random.randint(1000, 2000)
    table_name = random.choice(["accounts", "transactions", "customers", "audit_trail"])
    ts = now_ts("%Y-%m-%d-%H:%M:%S")
    # simulate load success/failure with 5-10% failures
    fail = random.random() < 0.08
    process_status = "failed" if fail else "success"

    # introduce anomalies for detection:
    # - sometimes huge count
    # - sometimes negative count (corruption)
    if random.random() < 0.03:
        src_count = random.randint(1000000, 5000000)  # huge
    elif random.random() < 0.02:
        src_count = -1  # corrupted
    else:
        src_count = random.randint(10, 2000)

    error_message = ""
    load_timestamp = now_ts("%Y-%m-%d-%H:%M:%S") if not fail else None
    if fail:
        error_message = random.choice([
            "ConstraintViolation: null value in column X",
            "Timeout while loading to DB",
            "Schema mismatch: found extra column",
            "Auth failure to downstream DB",
        ])

    payload = {
        "batch_id": batch_id,
        "run_id": run_id,
        "table_id": table_id,
        "table_name": table_name,
        "table_timestamp": ts,
        "load_timestamp": load_timestamp,
        "process_stage": random.choice(["raw", "enriched"]),
        "process_status": process_status,
        "error_message": error_message,
        "source_record_count": src_count,
        "batch_start_timestamp": ts,
        "batch_end_timestamp": now_ts("%Y-%m-%d-%H:%M:%S"),
    }
    return payload

def make_producer():
    if USE_SSL:
        # if using SSL files, set env paths or change accordingly
        ssl_cafile = os.getenv("SSL_CAFILE")
        ssl_certfile = os.getenv("SSL_CERTFILE")
        ssl_keyfile = os.getenv("SSL_KEYFILE")
        return KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            security_protocol="SSL",
            ssl_cafile=ssl_cafile,
            ssl_certfile=ssl_certfile,
            ssl_keyfile=ssl_keyfile,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
    else:
        return KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

def main():
    p = make_producer()
    idx = 1
    try:
        print(f"Producer started → {TOPIC} @ {KAFKA_BOOTSTRAP} (ssl={USE_SSL}). Rate={SLEEP_SECONDS}s")
        while True:
            msg = make_message(idx)
            p.send(TOPIC, msg)
            p.flush()
            print(f"[{datetime.utcnow().isoformat()}] sent batch_id={msg['batch_id']} table={msg['table_name']} status={msg['process_status']} count={msg['source_record_count']}")
            idx += 1
            time.sleep(SLEEP_SECONDS)
    except KeyboardInterrupt:
        print("Producer stopped by user")
    finally:
        p.close()

if __name__ == "__main__":
    main()
