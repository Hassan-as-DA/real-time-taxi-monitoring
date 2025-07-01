
import os
import csv
import json
from datetime import datetime
from kafka import KafkaProducer
import time
import socket

KAFKA_TOPIC = "taxi-locations"
KAFKA_BROKER = "kafka:9092"
DATA_FOLDER = "data"

# Wait for Kafka to be up
def wait_for_kafka():
    while True:
        try:
            s = socket.create_connection(("kafka", 9092), timeout=5)
            s.close()
            break
        except:
            print("⏳ Waiting for Kafka broker to be ready...")
            time.sleep(2)

wait_for_kafka()

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    batch_size=32768,
    linger_ms=10,
    compression_type="gzip",
)

print("✅ Kafka is up. Sending data...")

# Stream sorted records file by file
for file_name in sorted(os.listdir(DATA_FOLDER)):
    if file_name.endswith(".txt"):
        file_path = os.path.join(DATA_FOLDER, file_name)
        records = []
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            reader = csv.reader(f)
            for row in reader:
                try:
                    taxi_id, timestamp, lon, lat = row
                    records.append({
                        "taxi_id": taxi_id.strip(),
                        "timestamp": timestamp.strip(),
                        "longitude": float(lon),
                        "latitude": float(lat)
                    })
                except:
                    continue
        records.sort(key=lambda r: datetime.strptime(r["timestamp"], "%Y-%m-%d %H:%M:%S"))
        for record in records:
            producer.send(KAFKA_TOPIC, value=record)
        print(f"✅ Sent {len(records)} records from {file_name}")

print("✅ All data sent.")


