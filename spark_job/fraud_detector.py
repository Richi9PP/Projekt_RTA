"""
spark_job/fraud_detector.py — RTA Real-Time Fraud Detection Pipeline

Step 1.1: Read transactions + app_events from Kafka, left-outer join on tx_id.
Step 1.2: Online feature engineering aligned with fraud_model_training.ipynb:
          - hour, dayofweek (pandas-aligned 0=Mon), is_weekend
          - amount_log1p, amount_to_sender_avg
          - event_delay_sec (null when app_event absent)
          - sender_recipient_pair (categorical)

Usage
-----
    bash run_spark.sh
"""

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    approx_count_distinct, col, concat, count, dayofweek, expr, from_json,
    hour, lit, log1p, pmod, unix_timestamp, when, window,
)
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "data_generator"))
from schemas import APP_EVENT_SCHEMA, TRANSACTION_SCHEMA  # noqa: E402

#configuration

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
WATERMARK_DELAY = "5 seconds"
JOIN_WINDOW = "10 seconds"

#schema helpers

_TYPE_MAP = {
    "string": StringType(),
    "double": DoubleType(),
    "boolean": BooleanType(),
    "integer": IntegerType(),
    "timestamp": TimestampType(),
}


def _to_struct(schema_dict: dict) -> StructType:
    return StructType([
        StructField(k, _TYPE_MAP[v], True) for k, v in schema_dict.items()
    ])


TX_STRUCT = _to_struct(TRANSACTION_SCHEMA)
APP_STRUCT = _to_struct(APP_EVENT_SCHEMA)

#spark session


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("RTA_FraudDetector")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

# kafka helpers


def _kafka_source(spark: SparkSession, topic: str):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

#pipeline


def build_pipeline(spark: SparkSession):
    # transactions
    tx = (
        _kafka_source(spark, "transactions")
        .select(from_json(col("value").cast("string"), TX_STRUCT).alias("d"))
        .select("d.*")
        .withWatermark("timestamp", WATERMARK_DELAY)
    )

    # app_events — rename timestamp before watermark so the name is stable
    app = (
        _kafka_source(spark, "app_events")
        .select(from_json(col("value").cast("string"), APP_STRUCT).alias("d"))
        .select("d.*")
        .withColumnRenamed("timestamp", "app_timestamp")
        .withColumnRenamed("user_id", "app_user_id")
        .withWatermark("app_timestamp", WATERMARK_DELAY)
    )

    # left outer join on tx_id
    # Left outer keeps every transaction even when the app_event is absent.
    # app_event columns will be null for unmatched rows.
    joined = tx.join(
        app,
        (tx["tx_id"] == app["tx_id"])
        & (app["app_timestamp"] >= col("timestamp") - expr(f"INTERVAL {JOIN_WINDOW}"))
        & (app["app_timestamp"] <= col("timestamp") + expr(f"INTERVAL {JOIN_WINDOW}")),
        "left_outer",
    ).drop(app["tx_id"])

    # ── feature engineering — mirrors fraud_model_training.ipynb exactly ───────
    features = (
        joined
        # Temporal features from transaction timestamp.
        .withColumn("hour", hour(col("timestamp")))
        # PySpark dayofweek: Sun=1, Mon=2 … Sat=7.
        # pandas convention: Mon=0 … Sun=6 → formula: (pyspark_dow - 2) % 7
        .withColumn("dayofweek", pmod(dayofweek(col("timestamp")) - lit(2), lit(7)))
        .withColumn("is_weekend", (col("dayofweek") >= lit(5)).cast(IntegerType()))
        # Amount features.
        .withColumn("amount_log1p", log1p(col("amount")))
        .withColumn(
            "amount_to_sender_avg",
            when(
                col("sender_avg_amount") > 0,
                col("amount") / col("sender_avg_amount"),
            ).otherwise(lit(None).cast(DoubleType())),
        )
        # Delay between transaction event and app event (null when no app_event).
        .withColumn(
            "event_delay_sec",
            when(
                col("app_timestamp").isNotNull(),
                (unix_timestamp(col("app_timestamp")) - unix_timestamp(col("timestamp"))).cast(DoubleType()),
            ).otherwise(lit(None).cast(DoubleType())),
        )
        # Sender→recipient pair — categorical feature used by the pipeline.
        .withColumn(
            "sender_recipient_pair",
            concat(col("sender_id"), lit("->"), col("recipient_id")),
        )
    )

    return features


def build_counts_pipeline(spark: SparkSession):
    """Windowed tx count per sender (5 min) — monitoring query, not used in scoring."""
    tx = (
        _kafka_source(spark, "transactions")
        .select(from_json(col("value").cast("string"), TX_STRUCT).alias("d"))
        .select("d.sender_id", "d.timestamp")
        .withWatermark("timestamp", "30 seconds")
    )
    return (
        tx
        .groupBy(window("timestamp", "5 minutes"), "sender_id")
        .agg(count("*").alias("tx_count_last_5min"))
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "sender_id",
            "tx_count_last_5min",
        )
    )


def build_recipients_pipeline(spark: SparkSession):
    """Windowed unique recipient count per sender (1 hour) — monitoring query, not used in scoring."""
    tx = (
        _kafka_source(spark, "transactions")
        .select(from_json(col("value").cast("string"), TX_STRUCT).alias("d"))
        .select("d.sender_id", "d.recipient_id", "d.timestamp")
        .withWatermark("timestamp", "30 seconds")
    )
    return (
        tx
        .groupBy(window("timestamp", "1 hour"), "sender_id")
        .agg(approx_count_distinct("recipient_id").alias("unique_recipients_1h"))
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "sender_id",
            "unique_recipients_1h",
        )
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # query 1: main feature pipeline
    query_main = (
        build_pipeline(spark)
        .select(
            "sender_city", "amount", "amount_to_sender_avg",
            "currency", "device_trusted", "device_changed",
            "pin_failures", "is_offhours_login",
            "hour", "is_weekend", "event_delay_sec",
            "is_fraud", "fraud_type",
        )
        .writeStream
        .queryName("main_features")
        .format("console")
        .option("truncate", "true")
        .option("numRows", 20)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    # query 2: tx count per sender in 5-min windows (monitoring)
    query_counts = (
        build_counts_pipeline(spark)
        .writeStream
        .queryName("tx_counts_5min")
        .format("console")
        .option("truncate", "false")
        .outputMode("update")
        .trigger(processingTime="10 seconds")
        .start()
    )

    # query 3: unique recipients per sender in 1-hour windows (monitoring)
    query_recipients = (
        build_recipients_pipeline(spark)
        .writeStream
        .queryName("unique_recipients_1h")
        .format("console")
        .option("truncate", "false")
        .outputMode("update")
        .trigger(processingTime="10 seconds")
        .start()
    )

    query_main.awaitTermination()


if __name__ == "__main__":
    main()
