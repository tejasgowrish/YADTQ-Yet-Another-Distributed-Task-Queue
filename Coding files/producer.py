#!/usr/bin/env python3
from kafka import KafkaProducer
import sys
import json

producer = KafkaProducer(value_serializer = lambda m: json.dumps(m).encode('ascii'))
topic1=sys.argv[1]
topic2=sys.argv[2]
topic3=sys.argv[3]

for line in sys.stdin:
    task_data = json.loads(line.strip())
    action_type = task_data["action_type"]

    # Send task to the appropriate topic
    if action_type == "comment":
        producer.send(topic1, json.dumps(task_data))
    elif action_type == "like":
        producer.send(topic2, json.dumps(task_data))
    elif action_type == "share":
        producer.send(topic3, json.dumps(task_data))

producer.flush()