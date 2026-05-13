#!/bin/bash
spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.3 \
    spark_job/fraud_detector.py
