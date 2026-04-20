"""
Main data generator – orchestrates user profiles, fraud scenarios,
and Kafka publishing.

Usage
-----
    python generator.py                        # run with defaults
    python generator.py --rate 50 --fraud 0.05 # 50 tx/s, 5 % fraud
    python generator.py --dry-run              # print to stdout, no Kafka

Environment variables (override CLI defaults)
---------------------------------------------
    KAFKA_BOOTSTRAP   broker address      (default: localhost:9092)
    TX_TOPIC          transactions topic  (default: transactions)
    APP_TOPIC         app-events topic    (default: app_events)
"""

import argparse
import json
import logging
import os
import random
import time
from collections import Counter
from datetime import datetime, timezone

from profiles import build_user_pool, UserProfile
from event_builder import build_transaction, build_app_event
from fraud_scenarios import build_fraud_sequence, pick_scenario, FraudContext

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Kafka producer (lazy import so --dry-run works without kafka-python)
# ---------------------------------------------------------------------------

def _make_producer(bootstrap: str):
    try:
        from kafka import KafkaProducer  # type: ignore
    except ImportError as e:
        raise RuntimeError(
            "kafka-python is not installed. Install it with "
            "`pip install -r data_generator/requirements.txt`, "
            "or pass --dry-run to print JSON to stdout."
        ) from e

    return KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=5,
        compression_type="lz4",
        linger_ms=20,
    )


def _publish(producer, topic: str, payload: dict, dry_run: bool,
             key: str | None = None) -> None:
    if dry_run or producer is None:
        print(json.dumps(payload, ensure_ascii=False))
    else:
        producer.send(topic, value=payload, key=key)


# ---------------------------------------------------------------------------
# Normal transaction
# ---------------------------------------------------------------------------

def _send_normal(producer,
                 sender: UserProfile,
                 users: list[UserProfile],
                 tx_topic: str,
                 app_topic: str,
                 dry_run: bool) -> None:
    recipient = random.choice(users)
    while recipient.user_id == sender.user_id:
        recipient = random.choice(users)

    ts = datetime.now(timezone.utc)
    tx = build_transaction(sender, recipient, timestamp=ts, is_fraud=False)
    ev = build_app_event(sender, tx["tx_id"], timestamp=ts)

    _publish(producer, tx_topic, tx, dry_run, key=sender.user_id)
    _publish(producer, app_topic, ev, dry_run, key=sender.user_id)


# ---------------------------------------------------------------------------
# Fraudulent transaction sequence
# ---------------------------------------------------------------------------

def _send_fraud(producer,
                users: list[UserProfile],
                fraudsters: list[UserProfile],
                mules: list[UserProfile],
                tx_topic: str,
                app_topic: str,
                dry_run: bool,
                scenario_counts: Counter) -> None:
    sender = random.choice(fraudsters) if fraudsters else random.choice(users)
    mule_pool = mules if mules else users
    recipient = random.choice(mule_pool)
    while recipient.user_id == sender.user_id:
        recipient = random.choice(mule_pool)

    scenario = pick_scenario()
    ts = datetime.now(timezone.utc)
    ctx = FraudContext(sender=sender, recipient=recipient,
                       users=users, mules=mules, ts=ts)

    pairs = build_fraud_sequence(scenario, ctx)
    for tx, ev in pairs:
        _publish(producer, tx_topic, tx, dry_run, key=tx["sender_id"])
        _publish(producer, app_topic, ev, dry_run, key=ev["user_id"])

    scenario_counts[scenario] += 1
    log.debug("Fraud injected: scenario=%s tx_count=%d tx_id=%s",
              scenario, len(pairs), pairs[0][0]["tx_id"])


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run(rate: float,
        fraud_ratio: float,
        bootstrap: str,
        tx_topic: str,
        app_topic: str,
        dry_run: bool,
        n_users: int) -> None:

    log.info("Building user pool (%d users)…", n_users)
    users = build_user_pool(
        n_normal=int(n_users * 0.90),
        n_mules=int(n_users * 0.07),
        n_fraudsters=int(n_users * 0.03),
    )
    fraudsters = [u for u in users if u.risk_label == "fraudster"]
    mules = [u for u in users if u.risk_label == "mule"]
    log.info("Pool ready: %d users (%d fraudsters, %d mules)",
             len(users), len(fraudsters), len(mules))

    producer = None if dry_run else _make_producer(bootstrap)
    interval = 1.0 / rate
    count = 0
    scenario_counts: Counter = Counter()

    log.info(
        "Streaming %.1f tx/s | fraud_ratio=%.2f | dry_run=%s",
        rate, fraud_ratio, dry_run,
    )

    try:
        while True:
            start = time.monotonic()

            if random.random() < fraud_ratio:
                _send_fraud(producer, users, fraudsters, mules,
                            tx_topic, app_topic, dry_run, scenario_counts)
            else:
                sender = random.choice(users)
                _send_normal(producer, sender, users, tx_topic, app_topic, dry_run)

            count += 1
            if count % 500 == 0:
                log.info("Messages published: %d | Scenarios: %s",
                         count, dict(scenario_counts))

            elapsed = time.monotonic() - start
            sleep_for = max(0.0, interval - elapsed)
            time.sleep(sleep_for)

    except KeyboardInterrupt:
        log.info("Stopped after %d messages. Scenario totals: %s",
                 count, dict(scenario_counts))
    finally:
        if producer:
            producer.flush()
            producer.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="P2P Fraud Detection – data generator")
    p.add_argument("--rate",      type=float, default=10.0,
                   help="Transactions per second (default: 10)")
    p.add_argument("--fraud",     type=float, default=0.03,
                   help="Fraction of fraudulent transactions (default: 0.03)")
    p.add_argument("--users",     type=int,   default=1000,
                   help="Size of synthetic user pool (default: 1000)")
    p.add_argument("--bootstrap", type=str,
                   default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"))
    p.add_argument("--tx-topic",  type=str,
                   default=os.getenv("TX_TOPIC", "transactions"))
    p.add_argument("--app-topic", type=str,
                   default=os.getenv("APP_TOPIC", "app_events"))
    p.add_argument("--dry-run",   action="store_true",
                   help="Print JSON to stdout instead of sending to Kafka")
    p.add_argument("--seed",      type=int,   default=None,
                   help="Random seed for reproducible output")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse()
    if args.seed is not None:
        random.seed(args.seed)
    run(
        rate=args.rate,
        fraud_ratio=args.fraud,
        bootstrap=args.bootstrap,
        tx_topic=args.tx_topic,
        app_topic=args.app_topic,
        dry_run=args.dry_run,
        n_users=args.users,
    )
