import os
import csv
import json
import heapq
import time
from kafka.errors import NoBrokersAvailable
from tempfile import NamedTemporaryFile
from kafka import KafkaProducer

KAFKA_TOPIC = "taxi-locations"
KAFKA_BROKER = "kafka:9092"
DATA_FOLDER = "data"
DELAY = float(os.getenv("PRODUCER_DELAY", "0.05"))

CHUNK_SIZE = 200_000

def parse_row(row):
    try:
        taxi_id, timestamp, lon, lat = row
        return (taxi_id.strip(), timestamp.strip(), lon.strip(), lat.strip())
    except Exception:
        return None

def wait_for_kafka(broker, timeout=60):
    start = time.time()
    while time.time() - start < timeout:
        try:
            producer = KafkaProducer(bootstrap_servers=broker)
            producer.close()
            print("✅ Kafka is up!")
            return True
        except NoBrokersAvailable:
            print("⌛ Waiting for Kafka...")
            time.sleep(3)
    raise Exception("❌ Kafka did not become available within timeout")

def chunk_and_sort_files():
    print("🔍 Looking for data files...")
    files = sorted([f for f in os.listdir(DATA_FOLDER) if f.endswith('.txt')],
                   key=lambda x: int(x.split('.')[0]))
    print(f"📂 Found {len(files)} file(s).")

    chunk = []
    temp_files = []

    for file_name in files:
        file_path = os.path.join(DATA_FOLDER, file_name)
        if os.path.getsize(file_path) == 0:
            print(f"⚠️ Skipping empty file: {file_name}")
            continue

        print(f"📄 Reading {file_name}...")
        seen = set()
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            reader = csv.reader(f)
            for row in reader:
                record = parse_row(row)
                if record and record not in seen:
                    seen.add(record)
                    chunk.append((record[1], record))
                    if len(chunk) >= CHUNK_SIZE:
                        print("💾 Writing sorted chunk to temp file...")
                        temp_files.append(write_chunk(chunk))
                        chunk = []

    if chunk:
        print("💾 Writing final chunk to temp file...")
        temp_files.append(write_chunk(chunk))

    print(f"✅ Prepared {len(temp_files)} sorted chunk file(s).")
    return temp_files

def write_chunk(chunk):
    chunk.sort()
    tmpf = NamedTemporaryFile(delete=False, mode='w', encoding='utf-8', newline='')
    for _, record in chunk:
        tmpf.write(json.dumps({
            "taxi_id": record[0],
            "timestamp": record[1],
            "longitude": float(record[2]),
            "latitude": float(record[3])
        }) + '\n')
    tmpf.close()
    return tmpf.name

def merge_sorted_chunks(temp_files):
    print("🔀 Merging sorted chunks globally...")
    files = [open(f, 'r', encoding='utf-8') for f in temp_files]
    heap = []

    for idx, file in enumerate(files):
        line = file.readline()
        if line:
            obj = json.loads(line)
            key = (obj["timestamp"], str(obj.get("taxi_id")), str(obj.get("longitude")), str(obj.get("latitude")))
            heapq.heappush(heap, (key, obj, idx))

    last_key = None
    while heap:
        key, obj, idx = heapq.heappop(heap)
        record_key = (obj["taxi_id"], obj["timestamp"], obj["longitude"], obj["latitude"])
        if record_key != last_key:
            yield obj
            last_key = record_key

        line = files[idx].readline()
        if line:
            next_obj = json.loads(line)
            next_key = (next_obj["timestamp"], str(next_obj.get("taxi_id")), str(next_obj.get("longitude")), str(next_obj.get("latitude")))
            heapq.heappush(heap, (next_key, next_obj, idx))

    for f in files:
        f.close()
    for f in temp_files:
        os.remove(f)

def main():
    print("🚀 Producer started!")
    wait_for_kafka(KAFKA_BROKER)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    print("🔧 Indexing and sorting chunks...")
    temp_files = chunk_and_sort_files()

    if not temp_files:
        print("❌ No sorted chunks were created. Check if input files exist and are valid.")
        return

    print("📡 Streaming globally sorted data to Kafka...")
    count = 0
    for record in merge_sorted_chunks(temp_files):
        print(f"📦 Sending: taxi {record['taxi_id']} @ {record['timestamp']}")
        producer.send(KAFKA_TOPIC, record)
        count += 1
        if count % 10000 == 0:
            print(f"✅ Sent {count} records")
        time.sleep(DELAY)

    print(f"🏁 Finished sending {count} records.")

if __name__ == "__main__":
    main()
