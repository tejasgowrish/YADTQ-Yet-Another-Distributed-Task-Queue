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
# comments = []
consumer = KafkaConsumer(topic, value_deserializer=lambda m: json.loads(m.decode('utf-8')))
redis_channel = 'status_updates'  # The Pub/Sub channel to publish status updates

# Process messages from Kafka
for message in consumer:
    message=json.loads(message.value)
    print("Received message:", message)
    task_id = message["task_id"] 
    redis_key=task_id
    # Update status to "processing" in Redis
    redis_client.hset(redis_key, "status", "processing")
    print("Status in Redis:", redis_client.hget(redis_key, "status").decode())  # Print "processing" status
    
    # Publish status "processing" to Redis Pub/Sub channel
    redis_client.publish(redis_channel, json.dumps({task_id:{"status": "processing"}}))
    
    # if "stop" in message.value:
    #     # Final status and stop processing
    #     redis_client.set(redis_key, "status: done")
    #     redis_client.publish(redis_channel, json.dumps({"status": "done"}))  # Publish "done" status
    #     print("Status in Redis:", redis_client.get(redis_key).decode())  # Print "done" status
    #     break
    
    # Add or update comments (no publish for comments, just process them)
    # comments.append(message["comment"])
    
    # Update status to "success" and publish to Redis channel
    time.sleep(10)
    # print(hello)
    # data = {
    #     "status": "success",
    #     "result": message["comment"]
    # }
    redis_client.hset(redis_key, "status", "success")
    redis_client.hset(redis_key, "result", message["comment"])
    redis_client.publish(redis_channel, json.dumps({task_id:{"status": "success", "result": message["comment"]}}))  # Publish "success" status
    print("Status in Redis:", redis_client.hget(redis_key, "status").decode())  # Print "success" status after each addition

# Final status and sorted comments (no need to publish comments here)
# comments = sorted(comments)
# print("Final status from Redis:", redis_client.get(redis_key).decode())
# print(json.dumps(comments, indent=4))
