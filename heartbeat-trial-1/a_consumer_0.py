import redis, json, sys, time
from kafka import KafkaConsumer
from threading import Thread    # for background heartbeat

# unique worker id
worker_id = 'cons_0'

# Topic that this consumer is subscribed to
topic = sys.argv[1]
consumer = KafkaConsumer(topic, value_deserializer=lambda m: json.loads(m.decode('ascii')),
                         group_id="workers")

# Redis connection
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# Heartbeat interval
heartbeat_interval = 5  # secs

# Heartbeat function
def send_heartbeat():
    while True:
        r.hset('workers', worker_id, time.time())
        print(f"Heartbeat sent for {worker_id} at time {time.time()}")
        time.sleep(heartbeat_interval)

# Start heartbeat thread
heartbeat_thread = Thread(target=send_heartbeat)
heartbeat_thread.daemon = True
heartbeat_thread.start()

# Process messages
r.hset(worker_id, 'status', 'FREE')
msgCount = 0

for msg in consumer:
    if "end" in msg.value:
        break

    # Check for heartbeat
    last_heartbeat = r.hget('workers', worker_id)

    # if the time between heartbeats sent from the same worker exceeds the limit of 2*heartbeat_interval, 
    # then decalre the worker as 'dead', Thus-
    # skipping the task (OR)
    # send task back into the queue
    time_between_heartbeats = time.time() - float(last_heartbeat)
    heartbeat_limit = heartbeat_interval * 2

    if last_heartbeat is None or time_between_heartbeats > heartbeat_limit:  
        print(f"Worker {worker_id} is dead.")
        producer.send(topic, msg.value)
        # <nothing> if we're skipping the task.
        continue

    # Processing tasks
    msgCount += 1
    r.hset(worker_id, 'status', 'BUSY') # set worker to 'BUSY'
    task = msg.value

    r.hset(task["task_id"], 'status', 'PROCESSING')     # set the task status to 'RUNNING'
    r.hset(task["task_id"], 'result', 'None')
    print(f"PROCESSING TASK # {msgCount}")

    task_id, task_type, task_args = task["task_id"], task["task_type"], task["task_args"].split()
    time.sleep(5)

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

    except Exception as e:
        r.hset(task_id, 'status', 'FAILED')
        r.hset(task_id, 'error_log', str(e))
        r.hset(task_id, 'result', 'None')
        print(f"Task {task_id} failed due to error: {e}")

    else:
        r.hset(task_id, 'status', 'SUCCESSFUL')
        r.hset(task_id, 'result', res)
        print(f"Task {task_id} processed successfully with result: {res}")
        r.hset(worker_id, 'status', 'FREE')
        time.sleep(5)

consumer.close()
