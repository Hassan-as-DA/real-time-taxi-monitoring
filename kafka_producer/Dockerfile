FROM python:3.10-slim

WORKDIR /app

# ✅ Install netcat to wait for Kafka
RUN apt-get update && apt-get install -y netcat-openbsd

# ✅ Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ✅ Copy all producer code (including wait-for-kafka.sh and producer.py)
COPY . .

# ✅ Run wait-for-kafka.sh and then start the Python producer
CMD ["bash", "-c", "./wait-for-kafka.sh kafka 9092 && python producer.py"]
