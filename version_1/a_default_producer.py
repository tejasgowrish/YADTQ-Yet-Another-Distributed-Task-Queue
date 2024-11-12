'''
This producer keeps publishing messages to the tasksTopic -> Load Balancing
Tasks will go sit in a consumer's partition -> Whenever consumer becomes free, it'll pick up the task and execute
'''

from kafka import KafkaProducer
import sys, json, shortuuid, redis, time


# Connection to the Redis data-store
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

producer = KafkaProducer(value_serializer = lambda m : json.dumps(m).encode('ascii'))
tasksTopic = sys.argv[1]
#partitions = int(sys.argv[2])

taskCount = 0
for line in sys.stdin :
    line = line.strip().split(maxsplit=1)

    # Sending end to all partitions
    if "end" in line :
        # for p in range(partitions) :
        #     producer.send(tasksTopic, task, partition=p)
        # break
        #producer.send(tasksTopic, task)
        print("REACHED END OF INPUT")
        break

    taskCount += 1
    task_type, task_args = line
    task_ID = shortuuid.uuid()
    task = {}
    task = {"task_id" : task_ID, "task_type" : task_type, "task_args" : task_args, "task_tries" : 0}

    # Round-Robin publishing to partitions
    producer.send(tasksTopic, task)
    time.sleep(3)

print(f"# of Tasks -> {taskCount}")
producer.close()