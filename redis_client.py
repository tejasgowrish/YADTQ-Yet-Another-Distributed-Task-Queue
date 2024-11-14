#!/usr/bin/env python3
import redis
import json

# Connect to Redis
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

# Subscribe to the 'status_updates' channel
pubsub = redis_client.pubsub()
pubsub.subscribe('status_updates')

print("Subscribed to status_updates channel. Waiting for status updates...")

# Listen for messages on the channel
for message in pubsub.listen():
    if message['type'] == 'message':
        # Decode and load the JSON data
        status_update = json.loads(message['data'].decode())
        print("Received status update from Redis:")
        print(json.dumps(status_update, indent=4))
