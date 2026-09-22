import json
import os
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

TOPIC = "inventory-events"
PRODUCT_IDS = [f"P{n:03d}" for n in range(1, 11)]
SALE_PROBABILITY = 0.7


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=os.environ["EVENTHUBS_BOOTSTRAP_SERVERS"],
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_plain_username="$ConnectionString",
        sasl_plain_password=os.environ["EVENTHUBS_CONNECTION_STRING"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def event(product_id: str, event_type: str, quantity: int) -> dict:
    # 'Z', not '+00:00': Flink's json.timestamp-format.standard=ISO-8601
    # only accepts the 'Z' suffix for TIMESTAMP_LTZ columns -- a numeric
    # offset parses silently to NULL, confirmed locally against a real
    # JobManager+TaskManager pair (see 02_source.sql).
    timestamp = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
    return {
        "product_id": product_id,
        "event_type": event_type,
        "quantity": quantity,
        "event_time": timestamp.replace("+00:00", "Z"),
    }


def seed_initial_stock(producer: KafkaProducer) -> None:
    for product_id in PRODUCT_IDS:
        quantity = random.randint(50, 150)
        producer.send(TOPIC, event(product_id, "RESTOCK", quantity))
    producer.flush()


def run(duration_seconds: int) -> None:
    producer = build_producer()
    seed_initial_stock(producer)

    deadline = time.monotonic() + duration_seconds
    while time.monotonic() < deadline:
        product_id = random.choice(PRODUCT_IDS)
        if random.random() < SALE_PROBABILITY:
            e = event(product_id, "SALE", random.randint(1, 10))
        else:
            e = event(product_id, "RESTOCK", random.randint(10, 50))

        producer.send(TOPIC, e)
        print(e)
        time.sleep(random.uniform(0.5, 3.0))

    producer.flush()
    producer.close()


if __name__ == "__main__":
    run(duration_seconds=int(os.environ.get("SIMULATOR_DURATION_SECONDS", "300")))
