'''
THIS CONSUMER CAN BE ONE OF MANY CONSUMER PROCESSES IN A WORKER NODE
BUT USING ANY SINGLE CONSUMER SETS WORKER NODE TO 'busy'
'''

import redis, json, sys, time
from kafka import KafkaConsumer

# Unique ID for the worker this consumer is running on
worker_id = 'cons_0'
worker_partition = 0


# Topic that this consumer is subscribed to
topic = sys.argv[1]
consumer = KafkaConsumer(topic, value_deserializer = lambda m : json.loads(m.decode('ascii')),\
                         group_id = "workers")

# Connection to the Redis data-store
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# Setting worker status to FREE 
r.hset(worker_id, 'partition_num', worker_partition)
r.hset(worker_id, 'status', 'FREE')

while True :
    pass
    
# # Opening a file to write consumer logs
# f = open('a_out_0.txt', 'w')

# for msg in consumer :
#     if "end" in msg.value :
#         break
    
#     # Received task -> setting worker to BUSY
#     f.write(f"RECEIVED TASK\n")
#     r.hset(worker_id, 'status', 'BUSY')
#     task = msg.value

#     # Beginning processing -> STEP 1 : SET STATUS
#     r.hset(task["task_id"], 'status', 'PROCESSING')
#     r.hset(task["task_id"], 'result', 'None')
#     f.write(f"PROCESSING TASK - {task}\n")

#     task_id, task_type, task_args = task["task_id"], task["task_type"], task["task_args"].split()

#     # Delaying beginning of result processing by 10 seconds
#     time.sleep(5)

#     # Beginning task processing
#     try :
#         task_args = [int(arg) for arg in task_args]
#         if task_type == 'add' :
#             res = task_args[0] + task_args[1]
        
#         elif task_type == 'sub' :
#             res = task_args[0] - task_args[1]
        
#         elif task_type == 'mul' :
#             res = task_args[0] * task_args[1]
        
#         else :
#             raise Exception('INVALID OPERATION')
    
#     except Exception as e:
#         r.hset(task_id, 'status', 'FAILED')
#         r.hset(task_id, 'error_log', str(e))
#         r.hset(task["task_id"], 'result', 'None')
#         f.write(f"UN-SUCCESSFULLY PROCESSED {task_id}\n")

#         '''
#         ---------------------------- SHOULD ADD FAILED TASK BACK TO QUEUE ---------------------------------------
#         '''

#     else :
#         # Closing processing and storing result
#         r.hset(task_id, 'status', 'SUCCESSFUL')
#         r.hset(task["task_id"], 'result', res)
#         f.write(f"SUCCESSFULLY PROCESSED {task_id}\n")

#         # Setting consumer status to FREE again
#         r.hset(worker_id, 'status', 'FREE')
#         time.sleep(5)
    
# f.close()
consumer.close()