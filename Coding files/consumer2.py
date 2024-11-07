#!/usr/bin/env python3
from kafka import KafkaConsumer
import sys
import json
import redis
import time

redis_client=redis.StrictRedis(host='localhost', port=6379, db=0)

topic=sys.argv[1]
likes={}
redis_key=f"{topic}_likes"
redis_channel='status_updates'
consumer=KafkaConsumer(topic, value_deserializer = lambda m: json.loads(m.decode('ascii')))
for message in consumer:
    redis_client.set(redis_key, "status:processing")
    print("Status in Redis:", redis_client.get(redis_key).decode())
    redis_client.publish(redis_channel, json.dumps({"status": "processing"})) 
    if "stop" in message.value:
        redis_client.set(redis_key, "status:done")
        print("Status in Redis:", redis_client.get(redis_key).decode())
        redis_client.publish(redis_channel, json.dumps({"status": "done"}))
        break
    user=message.value[0]
    post=message.value[1]
    if user in likes:
        time.sleep(2)
        if post in likes[user]:
            likes[user][post]+=1
        else:
            likes[user][post]=1
        redis_client.set(redis_key, "status:success")
        print("Status in Redis:", redis_client.get(redis_key).decode())
        redis_client.publish(redis_channel, json.dumps({"status": "success", "result": likes}))
    else:
        time.sleep(2)
        likes[user]={}
        redis_client.set(redis_key, "status:create")
        print("Status in Redis:", redis_client.get(redis_key).decode())
        redis_client.publish(redis_channel, json.dumps({"status": "create"}))
    

likes=dict(sorted(likes.items()))
print(json.dumps(likes, indent=4))