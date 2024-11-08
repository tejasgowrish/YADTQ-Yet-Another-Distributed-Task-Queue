#!/usr/bin/env python3
from kafka import KafkaProducer
import sys
import json

# Kafka topic names from command-line arguments
topic1 = sys.argv[1]
topic2 = sys.argv[2]
topic3 = sys.argv[3]

# Initialize Kafka producer
producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii'))

try:
    # Continuously read from stdin
    while True:
        line = sys.stdin.readline().strip()

        # Skip if no new line
        if not line:
            continue

        # Parse the line and determine the topic based on action_type
        task_data = json.loads(line)
        action_type = task_data["action_type"]

        if action_type == "comment":
            producer.send(topic1, json.dumps(task_data))
        elif action_type == "like":
            producer.send(topic2, json.dumps(task_data))
        elif action_type == "share":
            producer.send(topic3, json.dumps(task_data))

        # Flush to ensure immediate message sending
        producer.flush()

except KeyboardInterrupt:
    print("Producer stopped manually.")
finally:
    producer.close()
