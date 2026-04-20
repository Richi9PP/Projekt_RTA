"""
Kafka topic field definitions — single source of truth for both the generator
and the Spark Streaming job.

Usage in PySpark
----------------
    from schemas import TRANSACTION_SCHEMA, APP_EVENT_SCHEMA
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ...

    _TYPE_MAP = {
        "string": StringType(), "double": DoubleType(), "boolean": BooleanType(),
        "integer": IntegerType(), "timestamp": TimestampType(),
    }

    tx_struct = StructType([StructField(k, _TYPE_MAP[v], True)
                            for k, v in TRANSACTION_SCHEMA.items()])
"""

TRANSACTION_SCHEMA: dict[str, str] = {
    "tx_id":                    "string",
    "sender_id":                "string",
    "recipient_id":             "string",
    "amount":                   "double",
    "currency":                 "string",
    "device_id":                "string",
    "device_type":              "string",
    "device_trusted":           "boolean",
    "sender_ip":                "string",
    "sender_lat":               "double",
    "sender_lon":               "double",
    "sender_city":              "string",
    "timestamp":                "timestamp",
    "is_fraud":                 "boolean",
    "fraud_type":               "string",
    "sender_account_age_days":  "integer",
    "sender_monthly_tx_count":  "integer",
    "sender_avg_amount":        "double",
}

APP_EVENT_SCHEMA: dict[str, str] = {
    "event_id":             "string",
    "user_id":              "string",
    "tx_id":                "string",
    "timestamp":            "timestamp",
    "pin_failures":         "integer",
    "device_changed":       "boolean",
    "new_device_id":        "string",
    "is_offhours_login":    "boolean",
    "session_duration_sec": "integer",
    "app_version":          "string",
}
