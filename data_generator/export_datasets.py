from __future__ import annotations

"""
Export a finite dataset for offline/batch pipeline input.

The generator used for Kafka is intentionally infinite.  This module reuses the
same profile, transaction, app-event, and fraud scenario builders, then writes
joined rows to CSV/JSONL files with ground-truth labels for model training.

Examples
--------
    python data_generator/export_datasets.py
    python data_generator/export_datasets.py --rows 100000
"""

import argparse
import csv
import json
import random
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

from event_builder import build_app_event, build_transaction
from fraud_scenarios import FraudContext, build_fraud_sequence, pick_scenario
from profiles import UserProfile, build_user_pool


def _normal_pair(
    sender: UserProfile,
    users: list[UserProfile],
    timestamp: datetime,
) -> tuple[dict, dict]:
    recipient = random.choice(users)
    while recipient.user_id == sender.user_id:
        recipient = random.choice(users)

    tx = build_transaction(sender, recipient, timestamp=timestamp, is_fraud=False)
    event = build_app_event(sender, tx["tx_id"], timestamp=timestamp)
    return tx, event


def _fraud_pairs(
    users: list[UserProfile],
    fraudsters: list[UserProfile],
    mules: list[UserProfile],
    timestamp: datetime,
    scenario_counts: Counter,
) -> list[tuple[dict, dict]]:
    sender = random.choice(fraudsters) if fraudsters else random.choice(users)
    mule_pool = mules if mules else users
    recipient = random.choice(mule_pool)
    while recipient.user_id == sender.user_id:
        recipient = random.choice(mule_pool)

    scenario = pick_scenario()
    ctx = FraudContext(
        sender=sender,
        recipient=recipient,
        users=users,
        mules=mules,
        ts=timestamp,
    )
    scenario_counts[scenario] += 1
    return build_fraud_sequence(scenario, ctx)


def _joined_row(tx: dict, event: dict) -> dict:
    return {
        **tx,
        "label": int(tx["is_fraud"]),
        "event_id": event["event_id"],
        "event_timestamp": event["timestamp"],
        "pin_failures": event["pin_failures"],
        "device_changed": event["device_changed"],
        "new_device_id": event["new_device_id"],
        "is_offhours_login": event["is_offhours_login"],
        "session_duration_sec": event["session_duration_sec"],
        "app_version": event["app_version"],
    }


def generate_rows(
    row_count: int,
    fraud_ratio: float,
    n_users: int,
    start_time: datetime,
) -> tuple[list[dict], Counter]:
    users = build_user_pool(
        n_normal=int(n_users * 0.90),
        n_mules=int(n_users * 0.07),
        n_fraudsters=int(n_users * 0.03),
    )
    fraudsters = [u for u in users if u.risk_label == "fraudster"]
    mules = [u for u in users if u.risk_label == "mule"]

    rows: list[dict] = []
    scenario_counts: Counter = Counter()
    tick = 0
    target_fraud_rows = round(row_count * fraud_ratio)
    fraud_rows = 0

    while len(rows) < row_count:
        timestamp = start_time + timedelta(seconds=tick)
        tick += random.randint(1, 3)

        rows_left = row_count - len(rows)
        fraud_left = target_fraud_rows - fraud_rows
        if fraud_left > 0 and random.random() < (fraud_left / rows_left):
            pairs = _fraud_pairs(users, fraudsters, mules, timestamp, scenario_counts)
        else:
            sender = random.choice(users)
            pairs = [_normal_pair(sender, users, timestamp)]

        for tx, event in pairs:
            if tx["is_fraud"] and fraud_rows >= target_fraud_rows:
                continue
            rows.append(_joined_row(tx, event))
            if tx["is_fraud"]:
                fraud_rows += 1
            if len(rows) >= row_count:
                break

    return rows, scenario_counts


def _fieldnames(rows: list[dict]) -> list[str]:
    if not rows:
        return []
    return list(rows[0].keys())


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_fieldnames(rows))
        writer.writeheader()
        writer.writerows(rows)


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export synthetic fraud datasets")
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--fraud", type=float, default=0.10)
    parser.add_argument("--users", type=int, default=10_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output-dir", type=Path, default=Path("datasets"))
    parser.add_argument("--basename", type=str, default="fraud_events_100k")
    parser.add_argument(
        "--start-time",
        type=str,
        default="2026-01-01T00:00:00+00:00",
        help="ISO timestamp used as the first synthetic event time",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    random.seed(args.seed)
    start_time = datetime.fromisoformat(args.start_time)
    if start_time.tzinfo is None:
        start_time = start_time.replace(tzinfo=timezone.utc)

    rows, scenario_counts = generate_rows(
        row_count=args.rows,
        fraud_ratio=args.fraud,
        n_users=args.users,
        start_time=start_time,
    )
    csv_path = args.output_dir / f"{args.basename}.csv"
    jsonl_path = args.output_dir / f"{args.basename}.jsonl"

    write_csv(csv_path, rows)
    write_jsonl(jsonl_path, rows)

    print(f"rows: {len(rows)} -> {csv_path}, {jsonl_path}")
    print(f"fraud scenario injections used for synthetic shape: {dict(scenario_counts)}")


if __name__ == "__main__":
    main()
