#!/usr/bin/env python3
from kafka import KafkaConsumer
import sys
import json
import redis
import time

redis_client=redis.StrictRedis(host='localhost', port=6379, db=0)

topic=sys.argv[1]
redis_channel='status_updates'
consumer=KafkaConsumer(topic, value_deserializer = lambda m: json.loads(m.decode('ascii')))
for message in consumer:
    message=json.loads(message.value)
    print("Received message:", message)
    task_id=message["task_id"]
    redis_key=task_id
    redis_client.set(redis_key, "status:processing")
    print("Status in Redis:", redis_client.get(redis_key).decode())
    redis_client.publish(redis_channel, json.dumps({task_id:{"status": "processing"}})) 
    # if "stop" in message.value:
    #     redis_client.set(redis_key, "status:done")
    #     print("Status in Redis:", redis_client.get(redis_key).decode())
    #     redis_client.publish(redis_channel, json.dumps({"status": "done"}))
    #     break
    redis_client.set(redis_key, "status:success")
    redis_client.publish(redis_channel, json.dumps({task_id:{"status": "success", "result": message['action_type']}}))
    print("Status in Redis:", redis_client.get(redis_key).decode())  
