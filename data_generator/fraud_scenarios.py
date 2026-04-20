"""
Fraud scenario builders.

Each function accepts a FraudContext and returns a list of (tx, event) pairs
representing the complete transaction sequence for that fraud pattern.
The caller publishes every pair to Kafka.

Scenarios implemented
---------------------
1. account_takeover   – login from new device + foreign IP + off-hours flag
2. rapid_fire         – 8-15 small transactions in quick succession
3. geo_anomaly        – transaction location far from home city
4. round_trip         – money sent back and forth between two accounts (both labelled fraud)
5. layering           – funds moved through a 3-5 hop chain of mule accounts
"""

import random
from dataclasses import dataclass
from datetime import datetime, timedelta

from profiles import UserProfile, CITY_BOXES, HIGH_RISK_CITIES, _coords_for_city, _realistic_public_ip
from event_builder import build_transaction, build_app_event


@dataclass
class FraudContext:
    sender: UserProfile
    recipient: UserProfile
    users: list[UserProfile]
    mules: list[UserProfile]
    ts: datetime


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _foreign_coords() -> tuple[float, float]:
    city = random.choice(list(HIGH_RISK_CITIES))
    return _coords_for_city(city)


# ---------------------------------------------------------------------------
# Scenario builders — each returns list[tuple[tx_dict, event_dict]]
# ---------------------------------------------------------------------------

def inject_account_takeover(ctx: FraudContext) -> list[tuple[dict, dict]]:
    """Unknown device, foreign geo, off-hours flag, multiple PIN failures."""
    fake_device_id = "unknown-" + str(random.randint(100000, 999999))
    lat, lon = _foreign_coords()

    tx = build_transaction(ctx.sender, ctx.recipient,
                           timestamp=ctx.ts, is_fraud=True, fraud_type="account_takeover")
    tx.update({
        "sender_lat":     lat,
        "sender_lon":     lon,
        "sender_ip":      _realistic_public_ip(),
        "device_trusted": False,
        "device_id":      fake_device_id,
    })

    ev = build_app_event(
        ctx.sender, tx["tx_id"], timestamp=ctx.ts,
        pin_failures=random.randint(3, 6),
        device_changed=True,
        new_device_id=fake_device_id,
        is_offhours_login=True,
    )
    return [(tx, ev)]


def inject_rapid_fire(ctx: FraudContext) -> list[tuple[dict, dict]]:
    """8-15 small transactions from the same sender in quick succession."""
    n = random.randint(8, 15)
    pool = [u for u in ctx.users if u.user_id != ctx.sender.user_id]
    recipients = random.sample(pool, min(n, len(pool)))

    pairs = []
    offset_sec = 0
    for recv in recipients:
        offset_ts = ctx.ts + timedelta(seconds=offset_sec)
        offset_sec += random.randint(1, 5)
        tx = build_transaction(ctx.sender, recv,
                               timestamp=offset_ts, is_fraud=True, fraud_type="rapid_fire")
        tx["amount"] = round(random.uniform(5, 50), 2)
        ev = build_app_event(ctx.sender, tx["tx_id"], timestamp=offset_ts)
        pairs.append((tx, ev))
    return pairs


def inject_geo_anomaly(ctx: FraudContext) -> list[tuple[dict, dict]]:
    """Transaction GPS location far from the sender's registered home city."""
    foreign = random.choice([c for c in CITY_BOXES if c != ctx.sender.home_city])
    lat, lon = _coords_for_city(foreign)

    tx = build_transaction(ctx.sender, ctx.recipient,
                           timestamp=ctx.ts, is_fraud=True, fraud_type="geo_anomaly")
    tx.update({
        "sender_lat": lat,
        "sender_lon": lon,
        "sender_ip":  _realistic_public_ip(),
    })
    ev = build_app_event(ctx.sender, tx["tx_id"], timestamp=ctx.ts)
    return [(tx, ev)]


def inject_round_trip(ctx: FraudContext) -> list[tuple[dict, dict]]:
    """A→B followed 30-120 s later by B→A for a slightly lower amount."""
    tx1 = build_transaction(ctx.sender, ctx.recipient,
                            timestamp=ctx.ts, is_fraud=True, fraud_type="round_trip")
    ev1 = build_app_event(ctx.sender, tx1["tx_id"], timestamp=ctx.ts)

    return_ts = ctx.ts + timedelta(seconds=random.randint(30, 120))
    tx2 = build_transaction(ctx.recipient, ctx.sender,
                            timestamp=return_ts, is_fraud=True, fraud_type="round_trip")
    tx2["amount"] = round(tx1["amount"] * random.uniform(0.90, 0.99), 2)
    ev2 = build_app_event(ctx.recipient, tx2["tx_id"], timestamp=return_ts)

    return [(tx1, ev1), (tx2, ev2)]


def inject_layering(ctx: FraudContext) -> list[tuple[dict, dict]]:
    """Funds hop through a 3-5 mule chain; each hop reduces the amount by 2-15%."""
    n_hops = random.randint(3, 5)
    hop_mules = random.sample(ctx.mules, min(n_hops, len(ctx.mules)))
    chain = [ctx.sender] + hop_mules

    pairs = []
    amount = round(ctx.sender.avg_tx_amount * random.uniform(1.5, 3.0), 2)
    offset_sec = 0

    for src, dst in zip(chain, chain[1:]):
        hop_ts = ctx.ts + timedelta(seconds=offset_sec)
        amount = round(amount * random.uniform(0.85, 0.98), 2)
        offset_sec += random.randint(60, 300)

        tx = build_transaction(src, dst,
                               timestamp=hop_ts, is_fraud=True, fraud_type="layering")
        tx["amount"] = amount
        ev = build_app_event(src, tx["tx_id"], timestamp=hop_ts)
        pairs.append((tx, ev))

    return pairs


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

SCENARIO_WEIGHTS = {
    "account_takeover": 0.30,
    "rapid_fire":       0.25,
    "geo_anomaly":      0.20,
    "round_trip":       0.15,
    "layering":         0.10,
}

SCENARIO_FNS = {
    "account_takeover": inject_account_takeover,
    "rapid_fire":       inject_rapid_fire,
    "geo_anomaly":      inject_geo_anomaly,
    "round_trip":       inject_round_trip,
    "layering":         inject_layering,
}


def pick_scenario() -> str:
    names = list(SCENARIO_WEIGHTS.keys())
    weights = [SCENARIO_WEIGHTS[n] for n in names]
    return random.choices(names, weights=weights, k=1)[0]


def build_fraud_sequence(scenario: str, ctx: FraudContext) -> list[tuple[dict, dict]]:
    return SCENARIO_FNS[scenario](ctx)
