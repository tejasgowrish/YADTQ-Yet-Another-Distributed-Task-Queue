'''
THIS CONSUMER CAN BE ONE OF MANY CONSUMER PROCESSES IN A WORKER NODE
BUT USING ANY SINGLE CONSUMER SETS WORKER NODE TO 'busy'
'''

import redis, json, sys, time, random
from kafka import KafkaConsumer, OffsetAndMetadata, TopicPartition

# Unique ID for the worker this consumer is running on
worker_id = sys.argv[1]
# Topic that this consumer is subscribed to
topic = sys.argv[2]

# Opening a file to write consumer logs
f = open(f'{worker_id}_logs.txt', 'w')


consumer = KafkaConsumer(topic, value_deserializer = lambda m : json.loads(m.decode('ascii')),\
                         group_id = "workers",
                         enable_auto_commit=False)

# Connection to the Redis data-store
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# Setting worker status to FREE 
r.hset(worker_id, 'status', 'FREE')
msgCount = 0

def killConsumer(message) :
    global consumer,f
    print(message)
    f.write(message)
    consumer.close()
    exit()


def process() :
    global task_id, task_type, task_args, task_tries, worker_id, r, msgCount
    task_tries += 1

   # Killing Consumer while processing msg # in [3,5]
    if msgCount == random.randint(2,3) :
        killConsumer("UN...graceful Exit")

    try :
        task_args = [int(arg) for arg in task_args]
        if task_type == 'add' :
            res = task_args[0] + task_args[1]
        
        elif task_type == 'sub' :
            res = task_args[0] - task_args[1]

        elif task_type == 'mul' :
            res = task_args[0] * task_args[1]
        
        else :
            raise Exception('INVALID OPERATION')
    
    except Exception as e:
        task_status = 'FAILED'
        print(f"Task failed on try {task_tries}")
        f.write((f"Task failed on try {task_tries}"))
        # Writing to database only after 3 attempts
        if task_tries == 3 :
            r.hset(task_id, 'status', task_status)
            r.hset(task_id, 'error_log', str(e))
            r.hset(task_id, 'result', 'None')
            r.hset(task_id, 'tries', task_tries)

        '''
        ---------------------------- SHOULD ADD FAILED TASK BACK TO QUEUE ---------------------------------------
        '''

    else :
        task_status = 'SUCCESS'
        # Closing processing and storing result
        r.hset(task_id, 'status', task_status)
        r.hset(task_id, 'result', res)
        r.hset(task_id, 'tries', task_tries)
        
        # Doesn't work coz it's committing the offset of the last receivd message from the poll call
        # consumer.commit()       

        # Setting consumer status to FREE again
        r.hset(worker_id, 'status', 'FREE')
        

    finally :
        return (task_status, task_tries)

for msg in consumer :
    if "end" in msg.value :
        killConsumer("Graceful Exit")
        break

    msgCount += 1
    print(f"Processing -> {msg.topic}, {msg.partition}, {msg.offset}")
    f.write(f"Processing -> {msg.topic}, {msg.partition}, {msg.offset}")

    # Received task -> setting worker to BUSY
    r.hset(worker_id, 'status', 'BUSY')
    task = msg.value

    # Beginning processing -> STEP 1 : SET STATUS
    r.hset(task["task_id"], 'status', 'PROCESSING')
    r.hset(task["task_id"], 'result', 'None')
    # print(f"PROCESSING TASK # {msgCount}")

    # Delaying beginning of result processing by 10 seconds
    time.sleep(3)
    task_id, task_type, task_args, task_tries = task["task_id"], task["task_type"], task["task_args"].split(), task["task_tries"]
    
    task_status, task_tries = process()

    # If a task fails more than 3 times -> give up and commit
    while task_status == 'FAILED' and task_tries < 3 :
        task_status, task_tries = process()

    om = OffsetAndMetadata(offset=msg.offset + 1, metadata=f"{msg.offset} commit")
    tp = TopicPartition(topic=topic, partition=msg.partition)
    d = {tp : om}
    consumer.commit(offsets=d)
    print(f"Last committed -> {msg.partition} {msg.offset}")
    f.write((f"Last committed -> {msg.partition} {msg.offset}"))

    # Sleeping n seconds before picking up next msg
    time.sleep(2)
    
#f.close()
consumer.close()
exit()