from kafka import KafkaProducer
import sys, json, shortuuid, redis, time


# Connection to the Redis data-store
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

producer = KafkaProducer(value_serializer = lambda m : json.dumps(m).encode('ascii'))
tasksTopic = sys.argv[1]
partitions = int(sys.argv[2])

cons_ids = ['cons_0', 'cons_1']


# Opening a file to write consumer logs
f = open('a_out_producer.txt', 'w')

for line in sys.stdin :
    line = line.strip().split(maxsplit=1)

    # Sending end to all partitions
    if "end" in line :
        for p in range(partitions) :
            producer.send(tasksTopic, 'end', partition=p)
        break
        

    task_type, task_args = line
    task_ID = shortuuid.uuid()
    task = {}
    task = {"task_id" : task_ID, "task_type" : task_type, "task_args" : task_args}

    # See which worker is free and publish to their partition
    f.write(f"TASK - {task_ID}")
    foundFree = False
    for w_id in cons_ids :
        #f.write(f"Checking {w_id}")
        f.write(str(r.hgetall(w_id)))

        if r.hget(w_id, 'status') == 'FREE' :
            producer.send(tasksTopic, task, partition=int(r.hget(w_id, 'partition_num')))
            f.write(f'sent {task["task_id"]} to {w_id}\n')
            foundFree = True
            break
    
    # No free consumer was found -> fall back to round robin partitioning - IS THIS ROUND ROBIN?
    if not(foundFree) :
        f.write("No one was free, resorting to default(?)\n")
        producer.send(tasksTopic, task)
    
    time.sleep(3)


producer.close()