import json
import os

from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

consumer = KafkaConsumer(
    'alerts',
    bootstrap_servers=os.environ['KAFKA_BOOTSTRAP'],
    group_id='alert-bridge',
    auto_offset_reset='latest',
    value_deserializer=lambda b: json.loads(b.decode('utf-8')),
    consumer_timeout_ms=-1,
)

client = InfluxDBClient(
    url=os.environ['INFLUX_URL'],
    token=os.environ['INFLUX_TOKEN'],
    org=os.environ['INFLUX_ORG'],
)
write_api = client.write_api(write_options=SYNCHRONOUS)
bucket = os.environ['INFLUX_BUCKET']
org = os.environ['INFLUX_ORG']
print('[bridge] Listening on Kafka topic alerts ...', flush=True)

for msg in consumer:
    try:
        alert = msg.value
        fraud_type = alert.get('fraud_type') or 'none'
        predicted_fraud = str(alert.get('predicted_fraud', False)).lower()

        point = (
            Point('fraud_alert')
            .tag('predicted_fraud', predicted_fraud)
            .tag('fraud_type', fraud_type if fraud_type else 'none')
            .field('fraud_probability', float(alert.get('fraud_probability', 0.0)))
            .field('tx_id', str(alert.get('tx_id', '')))
            .field('is_fraud', int(bool(alert.get('predicted_fraud', False))))
        )

        # GPS fields — zapisujemy tylko gdy są dostępne
        lat = alert.get('sender_lat')
        lon = alert.get('sender_lon')
        city = alert.get('sender_city', '')

        if lat is not None and lon is not None:
            point = (
                point
                .field('sender_lat', float(lat))
                .field('sender_lon', float(lon))
                .field('sender_city', str(city))
            )

        write_api.write(bucket=bucket, org=org, record=point)

    except Exception as e:
        print(f'[bridge] error: {e}', flush=True)
