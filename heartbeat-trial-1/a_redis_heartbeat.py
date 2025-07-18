import redis
import time

# connection
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# timeout of 10 secs
heartbeat_timeout = 15 # marked 'dead' if heartbeat not received in this period  

# check worker statuses
def check_workers_status():
    current_time = time.time()
    workers = r.hgetall('workers')  
    
    for worker_id, last_heartbeat in workers.items():
        last_heartbeat_time = float(last_heartbeat)
        if current_time - last_heartbeat_time > heartbeat_timeout:
            # Mark worker as dead if the heartbeat is outdated
            print(f"Worker {worker_id} is dead (no heartbeat for {heartbeat_timeout} seconds).")
            r.hset('workers_status', worker_id, 'dead')
        else:
            r.hset('workers_status', worker_id, 'alive')
            print(f"Worker {worker_id} is alive.")

# Continuously check worker statuses at regular intervals
if __name__ == "__main__":
    while True:
        check_workers_status()
        time.sleep(5)  # Check every 5 seconds
