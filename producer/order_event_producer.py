"""
Simulated real-time order event producer.

Generates DoorDash-style order lifecycle events (created -> accepted ->
picked_up -> delivered / canceled) and publishes them to a Kafka topic.
Includes a configurable % of "dirty" records (nulls, bad timestamps,
duplicate order_ids) so the Bronze -> Silver layer has real cleaning to do.

Usage:
    python order_event_producer.py --topic orders --rate 20 --dirty-pct 0.05
"""
import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer

fake = Faker()

ORDER_STATUSES = ["CREATED", "ACCEPTED", "PICKED_UP", "DELIVERED", "CANCELED"]
STATUS_WEIGHTS = [0.35, 0.25, 0.20, 0.15, 0.05]

RESTAURANTS = [f"rest_{i:04d}" for i in range(1, 201)]
DRIVERS = [f"drv_{i:04d}" for i in range(1, 501)]


def build_event(dirty: bool) -> dict:
    order_id = str(uuid.uuid4()) if not dirty else random.choice(["", None, "dup-order-001"])
    status = random.choices(ORDER_STATUSES, weights=STATUS_WEIGHTS, k=1)[0]

    event = {
        "order_id": order_id or str(uuid.uuid4()),
        "customer_id": f"cust_{random.randint(1, 5000):05d}",
        "restaurant_id": random.choice(RESTAURANTS),
        "driver_id": random.choice(DRIVERS) if status != "CREATED" else None,
        "status": status,
        "order_value_usd": round(random.uniform(8, 120), 2) if not dirty else -5.0,
        "eta_minutes": random.randint(15, 60),
        "lat": float(fake.latitude()),
        "lng": float(fake.longitude()),
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "source": "order-service",
    }

    if dirty:
        # simulate common real-world garbage
        corruption = random.choice(["null_customer", "bad_ts", "missing_value"])
        if corruption == "null_customer":
            event["customer_id"] = None
        elif corruption == "bad_ts":
            event["event_ts"] = "not-a-timestamp"
        elif corruption == "missing_value":
            event.pop("order_value_usd", None)

    return event


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="orders")
    parser.add_argument("--rate", type=float, default=10.0, help="events per second")
    parser.add_argument("--dirty-pct", type=float, default=0.05, help="fraction of malformed events")
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )

    print(f"Producing to '{args.topic}' at ~{args.rate}/s (dirty rate: {args.dirty_pct:.0%}). Ctrl+C to stop.")
    delay = 1.0 / args.rate
    try:
        while True:
            dirty = random.random() < args.dirty_pct
            event = build_event(dirty)
            producer.send(args.topic, key=event.get("restaurant_id", "unknown"), value=event)
            print(event)
            time.sleep(delay)
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
