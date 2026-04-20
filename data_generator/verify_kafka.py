"""
Verification script – Step 2.

Reads a fixed number of messages from both Kafka topics and prints them
in a readable format.  Run AFTER starting docker-compose and the generator.

Usage:
    python data_generator/verify_kafka.py [--n 10] [--bootstrap localhost:9092]
    python data_generator/verify_kafka.py --n 5 --from-latest
"""

import argparse
import json
from kafka import KafkaConsumer  # type: ignore


def consume(topic: str, bootstrap: str, n: int, offset_reset: str) -> None:
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        group_id="verify-script",
        auto_offset_reset=offset_reset,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=8000,
    )

    print(f"\n{'='*60}")
    print(f"Topic: {topic}  (showing up to {n} messages, offset={offset_reset})")
    print(f"{'='*60}")

    count = 0
    for msg in consumer:
        data = msg.value
        key = msg.key.decode("utf-8") if msg.key else None
        _pretty(data, partition=msg.partition, offset=msg.offset, key=key)
        count += 1
        if count >= n:
            break

    print(f"\n  --> {count} message(s) read from '{topic}'\n")
    consumer.close()


def _pretty(d: dict, partition: int, offset: int, key: str | None) -> None:
    fraud_marker = " [FRAUD]" if d.get("is_fraud") else ""
    print(f"\n  tx_id / event_id : {d.get('tx_id') or d.get('event_id')}{fraud_marker}")
    print(f"    {'partition':<30} {partition}  offset={offset}  key={key}")
    for k, v in d.items():
        if k in ("tx_id", "event_id"):
            continue
        print(f"    {k:<30} {v}")


def main() -> None:
    p = argparse.ArgumentParser(description="Kafka verification consumer")
    p.add_argument("--n",           type=int, default=10)
    p.add_argument("--bootstrap",   type=str, default="localhost:9092")
    p.add_argument("--from-latest", action="store_true",
                   help="Read only new messages (default: from beginning)")
    args = p.parse_args()

    offset_reset = "latest" if args.from_latest else "earliest"
    consume("transactions", args.bootstrap, args.n, offset_reset)
    consume("app_events",   args.bootstrap, args.n, offset_reset)


if __name__ == "__main__":
    main()
