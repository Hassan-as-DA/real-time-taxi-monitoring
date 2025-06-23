#!/bin/bash
echo "⏳ Waiting for Kafka broker to be ready at kafka:9092..."
while ! nc -z kafka 9092; do
  sleep 1
done
echo "✅ Kafka is up. Starting producer..."
python producer.py
