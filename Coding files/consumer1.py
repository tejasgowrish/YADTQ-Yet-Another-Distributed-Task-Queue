#!/usr/bin/env python3
from kafka import KafkaConsumer
import sys
import json
import redis
import time

# Connect to Redis
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

# Get topic and define Redis key and channel
topic = sys.argv[1]
comments = {}
consumer = KafkaConsumer(topic, value_deserializer=lambda m: json.loads(m.decode('ascii')))
redis_key = f"{topic}_comments"
redis_channel = 'status_updates'  # The Pub/Sub channel to publish status updates

# Process messages from Kafka
for message in consumer:
    # Update status to "processing" in Redis
    redis_client.set(redis_key, "status: processing")
    print("Status in Redis:", redis_client.get(redis_key).decode())  # Print "processing" status
    
    # Publish status "processing" to Redis Pub/Sub channel
    redis_client.publish(redis_channel, json.dumps({"status": "processing"}))
    
    if "stop" in message.value:
        # Final status and stop processing
        redis_client.set(redis_key, "status: done")
        redis_client.publish(redis_channel, json.dumps({"status": "done"}))  # Publish "done" status
        print("Status in Redis:", redis_client.get(redis_key).decode())  # Print "done" status
        break
    
    # Add or update comments (no publish for comments, just process them)
    if message.value[0] in comments:
        time.sleep(2)
        comments[message.value[0]].append(message.value[-1])
    else:
        time.sleep(2)
        comments[message.value[0]] = [message.value[-1]]
    
    # Update status to "success" and publish to Redis channel
    redis_client.set(redis_key, "status: success")
    redis_client.publish(redis_channel, json.dumps({"status": "success", "result": comments}))  # Publish "success" status
    print("Status in Redis:", redis_client.get(redis_key).decode())  # Print "success" status after each addition

# Final status and sorted comments (no need to publish comments here)
comments = dict(sorted(comments.items()))
print("Final status from Redis:", redis_client.get(redis_key).decode())
print(json.dumps(comments, indent=4))
