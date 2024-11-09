'''
THIS CONSUMER CAN BE ONE OF MANY CONSUMER PROCESSES IN A WORKER NODE
BUT USING ANY SINGLE CONSUMER SETS WORKER NODE TO 'busy'
'''

import redis, json, sys, time
from kafka import KafkaConsumer, KafkaProducer

worker_id = 'cons_1'
topic = sys.argv[1]
consumer = KafkaConsumer(
    topic, 
    value_deserializer=lambda m: json.loads(m.decode('ascii')), 
    group_id="workers", 
    enable_auto_commit=False # !!!
)

r = redis.Redis(host='localhost', port=6379, decode_responses=True)
r.hset(worker_id, 'status', 'FREE')
msgCount = 0

for msg in consumer:
    if "end" in msg.value:
        break

    msgCount += 1
    r.hset(worker_id, 'status', 'BUSY')
    task = msg.value

    task_id = task["task_id"]
    task_type = task["task_type"]
    task_args = task["task_args"].split()

    max_retries = 3
    success = False
    current_attempt = 1

    while current_attempt <= max_retries and not success:
        r.hset(task_id, 'status', 'PROCESSING')
        r.hset(task_id, 'result', 'None')
        print(f"PROCESSING TASK # {msgCount} - Attempt {current_attempt} of {max_retries}")

        try:
            task_args = [int(arg) for arg in task_args]
            if task_type == 'add':
                res = task_args[0] + task_args[1]
            elif task_type == 'sub':
                res = task_args[0] - task_args[1]
            elif task_type == 'mul':
                res = task_args[0] * task_args[1]
            else:
                raise Exception('INVALID OPERATION')
            
            print("TASK SUCCESSFUL")
            success = True
            
            # store valid result
            r.hset(task_id, 'status', 'SUCCESSFUL')
            r.hset(task_id, 'result', res)

        except Exception as e:
            print(f"Error processing task {task_id}, attempt {current_attempt} of {max_retries}: {e}")
            if current_attempt == max_retries:
                print(f"Max retries reached for task {task_id}, marking as failed")
                r.hset(task_id, 'status', 'FAILED')
                r.hset(task_id, 'error_log', str(e))
            else:
                print(f"Retrying same task")
                time.sleep(5)
            
            current_attempt += 1

    # committing offset after either 1. task successful 2. retries exhausted
    consumer.commit()
    r.hset(worker_id, 'status', 'FREE')
    time.sleep(5)

consumer.close()