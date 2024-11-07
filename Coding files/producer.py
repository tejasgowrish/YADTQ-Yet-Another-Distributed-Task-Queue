#!/usr/bin/env python3
from kafka import KafkaProducer
import sys
import json

producer = KafkaProducer(value_serializer = lambda m: json.dumps(m).encode('ascii'))
topic1=sys.argv[1]
topic2=sys.argv[2]
topic3=sys.argv[3]

for line in sys.stdin:
    line=line.strip().split(maxsplit=4)
    if line[0]=="comment":
        line[-1]=line[-1][1:-1] #just removing the quotes from the comments here
        producer.send(topic1,line[2:]) #sending the line from the 2nd string onwards: so that we send only the userid, post id and the comment (in case it exists) to the respective consumer based on the topic.
    elif line[0]=="like":
        producer.send(topic2,line[2:])
    elif line[0]=="EOF":
        producer.send(topic1,"stop")
        producer.send(topic2,"stop")
        producer.send(topic3,"stop")
        break
    producer.send(topic3,line)

producer.flush()