"""
spark_job/fraud_detector.py — RTA Real-Time Fraud Detection Pipeline
"""

import json
import os
import sys

import joblib
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    approx_count_distinct, col, concat, count, dayofweek, expr, from_json,
    hour, lit, log1p, pmod, unix_timestamp, when, window,
)
from pyspark.sql.types import (
    BooleanType, DoubleType, IntegerType, StringType,
    StructField, StructType, TimestampType,
)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "data_generator"))
from schemas import TRANSACTION_SCHEMA  # noqa: E402

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
MODEL_PATH      = os.getenv("MODEL_PATH", os.path.join(os.path.dirname(__file__), "..", "models", "xgboost.joblib"))
WATERMARK_DELAY = "5 seconds"
JOIN_WINDOW     = "10 seconds"
FRAUD_THRESHOLD = float(os.getenv("FRAUD_THRESHOLD", "0.87"))

# Exact feature order matching fraud_model_training.ipynb
ALL_FEATURES = [
    "amount", "currency", "device_type", "device_trusted", "sender_lat", "sender_lon",
    "sender_city", "sender_account_age_days", "sender_monthly_tx_count", "sender_avg_amount",
    "pin_failures", "device_changed", "is_offhours_login", "session_duration_sec", "app_version",
    "hour", "dayofweek", "is_weekend", "amount_log1p", "amount_to_sender_avg",
    "event_delay_sec", "sender_recipient_pair",
]

_TYPE_MAP = {
    "string": StringType(), "double": DoubleType(), "boolean": BooleanType(),
    "integer": IntegerType(), "timestamp": TimestampType(),
}

def _to_struct(schema_dict):
    return StructType([StructField(k, _TYPE_MAP[v], True) for k, v in schema_dict.items()])

TX_STRUCT = _to_struct(TRANSACTION_SCHEMA)


def build_spark():
    return (SparkSession.builder.appName("RTA_FraudDetector")
            .config("spark.sql.shuffle.partitions", "4").getOrCreate())


def _kafka_source(spark, topic):
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", topic)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false").load())


def build_pipeline(spark):
    # App-event fields (pin_failures, device_changed, etc.) are embedded in the
    # transaction message by the generator — no stream-stream join needed.
    tx = (_kafka_source(spark, "transactions")
          .select(from_json(col("value").cast("string"), TX_STRUCT).alias("d"))
          .select("d.*").withWatermark("timestamp", WATERMARK_DELAY))

    return (tx
        .withColumn("hour", hour(col("timestamp")))
        .withColumn("dayofweek", pmod(dayofweek(col("timestamp")) - lit(2), lit(7)))
        .withColumn("is_weekend", (col("dayofweek") >= lit(5)).cast(IntegerType()))
        .withColumn("amount_log1p", log1p(col("amount")))
        .withColumn("amount_to_sender_avg",
            when(col("sender_avg_amount") > 0, col("amount") / col("sender_avg_amount"))
            .otherwise(lit(None).cast(DoubleType())))
        .withColumn("event_delay_sec", lit(0.0).cast(DoubleType()))
        .withColumn("sender_recipient_pair",
            concat(col("sender_id"), lit("->"), col("recipient_id")))
    )


def make_score_batch_fn(broadcast_model):
    def score_and_publish(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return
        model = broadcast_model.value

        cols_needed = ["tx_id", "fraud_type"] + ALL_FEATURES
        existing = set(batch_df.columns)
        select_exprs = [col(c) if c in existing else lit(None).cast(StringType()).alias(c)
                        for c in cols_needed]
        pdf = batch_df.select(select_exprs).toPandas()
        if pdf.empty:
            return

        for c in ["device_trusted", "device_changed", "is_offhours_login"]:
            if c in pdf.columns:
                pdf[c] = pdf[c].map(
                    lambda v: 1 if v is True or v == 1 or str(v).lower() == "true"
                    else (0 if v is False or v == 0 or str(v).lower() == "false" else np.nan)
                ).astype(float)

        X = pdf[ALL_FEATURES].copy()

        try:
            proba = model.predict_proba(X)[:, 1]
        except Exception as e:
            print(f"[SCORER] predict_proba failed on batch {batch_id}: {e}")
            return

        alerts = []
        for i, row in pdf.iterrows():
            p = float(proba[i])
            alerts.append({
                "tx_id":             str(row["tx_id"]) if row["tx_id"] else "",
                "fraud_probability": round(p, 4),
                "predicted_fraud":   bool(p >= FRAUD_THRESHOLD),
                "fraud_type":        str(row.get("fraud_type", "")) or "",
                "sender_lat":        float(row["sender_lat"]) if pd.notna(row.get("sender_lat")) else None,
                "sender_lon":        float(row["sender_lon"]) if pd.notna(row.get("sender_lon")) else None,
                "sender_city":       str(row.get("sender_city", "")) or "",
            })

        try:
            from kafka import KafkaProducer
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
            )
            for alert in alerts:
                producer.send("alerts", value=alert, key=alert["tx_id"])
            producer.flush()
            producer.close()
            fraud_count = sum(1 for a in alerts if a["predicted_fraud"])
            print(f"[SCORER] batch={batch_id} rows={len(alerts)} fraud={fraud_count} ({100*fraud_count/max(len(alerts),1):.1f}%)")
        except Exception as e:
            print(f"[SCORER] Kafka publish failed on batch {batch_id}: {e}")

    return score_and_publish


def build_counts_pipeline(spark):
    tx = (_kafka_source(spark, "transactions")
          .select(from_json(col("value").cast("string"), TX_STRUCT).alias("d"))
          .select("d.sender_id", "d.timestamp")
          .withWatermark("timestamp", "30 seconds"))
    return (tx.groupBy(window("timestamp", "5 minutes"), "sender_id")
            .agg(count("*").alias("tx_count_last_5min"))
            .select(col("window.start").alias("window_start"),
                    col("window.end").alias("window_end"),
                    "sender_id", "tx_count_last_5min"))


def build_recipients_pipeline(spark):
    tx = (_kafka_source(spark, "transactions")
          .select(from_json(col("value").cast("string"), TX_STRUCT).alias("d"))
          .select("d.sender_id", "d.recipient_id", "d.timestamp")
          .withWatermark("timestamp", "30 seconds"))
    return (tx.groupBy(window("timestamp", "1 hour"), "sender_id")
            .agg(approx_count_distinct("recipient_id").alias("unique_recipients_1h"))
            .select(col("window.start").alias("window_start"),
                    col("window.end").alias("window_end"),
                    "sender_id", "unique_recipients_1h"))


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print(f"[SCORER] Loading model from {MODEL_PATH}")
    model = joblib.load(MODEL_PATH)
    broadcast_model = spark.sparkContext.broadcast(model)
    print("[SCORER] Model loaded OK")

    score_fn = make_score_batch_fn(broadcast_model)

    (build_pipeline(spark).writeStream.queryName("fraud_scoring")
     .foreachBatch(score_fn).outputMode("append")
     .trigger(processingTime="10 seconds").start())

    (build_counts_pipeline(spark).writeStream.queryName("tx_counts_5min")
     .format("console").option("truncate", "false").outputMode("update")
     .trigger(processingTime="10 seconds").start())

    (build_recipients_pipeline(spark).writeStream.queryName("unique_recipients_1h")
     .format("console").option("truncate", "false").outputMode("update")
     .trigger(processingTime="10 seconds").start())

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
