"""
Constructs individual Kafka message payloads.

Two topics
----------
transactions   – one message per P2P payment attempt
app_events     – accompanying mobile/web app telemetry (login, PIN, device)

Schema is intentionally flat JSON so any consumer (Flink, KSQL, Python)
can deserialise without extra mapping.
"""

import random
import uuid
from datetime import datetime, timezone

from profiles import UserProfile, _coords_for_city, _realistic_public_ip, CURRENCIES


# ---------------------------------------------------------------------------
# Transaction payload
# ---------------------------------------------------------------------------

def build_transaction(sender: UserProfile,
                      recipient: UserProfile,
                      timestamp: datetime | None = None,
                      is_fraud: bool = False,
                      fraud_type: str = "") -> dict:
    if timestamp is None:
        timestamp = datetime.now(timezone.utc)

    lat, lon = sender.home_coords()
    device = sender.primary_device

    amount = round(
        random.gauss(sender.avg_tx_amount, sender.avg_tx_amount * 0.4), 2
    )
    amount = max(1.0, amount)

    return {
        # identifiers
        "tx_id":          str(uuid.uuid4()),
        "sender_id":      sender.user_id,
        "recipient_id":   recipient.user_id,

        # financial
        "amount":         amount,
        "currency":       sender.preferred_currency,

        # device & network
        "device_id":      device.device_id,
        "device_type":    device.device_type,
        "device_trusted": device.trusted,
        "sender_ip":      _realistic_public_ip(),

        # geolocation (sender's login location)
        "sender_lat":     lat,
        "sender_lon":     lon,
        "sender_city":    sender.home_city,

        # timing
        "timestamp":      timestamp.isoformat(),

        # ground truth labels (strip before sending to a real scoring model)
        "is_fraud":       is_fraud,
        "fraud_type":     fraud_type,

        # user meta (denormalised for stream joins)
        "sender_account_age_days":  sender.account_age_days,
        "sender_monthly_tx_count":  sender.monthly_tx_count,
        "sender_avg_amount":        sender.avg_tx_amount,
    }


# ---------------------------------------------------------------------------
# App-event payload
# ---------------------------------------------------------------------------

def build_app_event(user: UserProfile,
                    tx_id: str,
                    timestamp: datetime | None = None,
                    pin_failures: int = 0,
                    device_changed: bool = False,
                    new_device_id: str = "",
                    is_offhours_login: bool = False) -> dict:
    if timestamp is None:
        timestamp = datetime.now(timezone.utc)

    normal_failures = random.choices([0, 1], weights=[0.92, 0.08])[0]

    return {
        "event_id":          str(uuid.uuid4()),
        "user_id":           user.user_id,
        "tx_id":             tx_id,
        "timestamp":         timestamp.isoformat(),

        # session signals
        "pin_failures":      pin_failures if pin_failures else normal_failures,
        "device_changed":    device_changed,
        "new_device_id":     new_device_id,
        "is_offhours_login": is_offhours_login,

        # session metadata
        "session_duration_sec": random.randint(5, 300),
        "app_version":       f"3.{random.randint(0, 9)}.{random.randint(0, 9)}",
    }
