import redis
import time, json

# connection
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# timeout of 10 secs
heartbeat_timeout = 10 # marked 'dead' if heartbeat not received in this period  

t = time.time()
# check worker statuses
def check_workers_status():
    all_hb = r.hgetall('all_heartbeats')

    for w_id, w_deets in all_hb.items():
        deets = json.loads(w_deets)
        current_time = time.time()
        last_hb = float(deets['last_heartbeat'])
        if current_time - last_hb > heartbeat_timeout:
            # Mark worker as dead if the heartbeat is outdated
            deets['status'] = 'DEAD'
            print(f"Worker {w_id} is dead (no heartbeat for {heartbeat_timeout} seconds).")
        else:
            deets['status'] = 'ALIVE'
            print(f"Worker {w_id} is alive.")
        r.hset('all_heartbeats', w_id, json.dumps(deets))


# Continuously check worker statuses at regular intervals
if __name__ == "__main__":
    while True:
        check_workers_status()
        time.sleep(6)  # Check every 5 seconds