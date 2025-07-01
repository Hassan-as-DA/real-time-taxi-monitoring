import json
import time
import csv
from kafka import KafkaProducer

KAFKA_TOPIC = "taxi-locations"
KAFKA_BROKER = "host.docker.internal:9092"
DELAY_SECONDS = 1

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def read_and_send(file_path):
    with open(file_path, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            taxi_id, timestamp, lon, lat = row
            message = {
                "taxi_id": taxi_id,
                "timestamp": timestamp,
                "longitude": float(lon),
                "latitude": float(lat)
            }
            producer.send(KAFKA_TOPIC, value=message)
            time.sleep(DELAY_SECONDS)

if __name__ == "__main__":
    read_and_send("taxi_data.csv")
