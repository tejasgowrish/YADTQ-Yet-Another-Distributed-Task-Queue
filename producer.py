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
partitions = int(sys.argv[2])

taskCount = 0
try :
    while True :
        line = sys.stdin.readline().strip()
        if line :
            print(line)
            # Sending end to all partitions
            # if "end" in line :
            #     for p in range(partitions) :
            #         producer.send(tasksTopic, "end", partition=p)
            #     break

            taskCount += 1
            task = json.loads(line)
            # Setting status of task to 'queued'
            r.hset(task['task_id'], 'status', 'QUEUED')
            r.hset(task['task_id'], 'tries', 0)
            
            # Round-Robin publishing to partitions
            producer.send(tasksTopic, task)
            time.sleep(3)

except KeyboardInterrupt :
    print("Killing Producer")

finally :
    print(f"# of Tasks -> {taskCount}")
    producer.close()