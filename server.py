from fastapi import FastAPI, HTTPException, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
import redis.asyncio as aioredis  # Async Redis client
import json
import uuid

# Initialize FastAPI and Redis clients
server = FastAPI()
templates = Jinja2Templates(directory="templates")
redis_client = aioredis.from_url("redis://localhost:6379")

# Define the file path for storing tasks
TASK_FILE_PATH = 'a_tasks.txt'
all_task_ids = []
# Endpoint to fetch the task status by task_id
@server.post("/status", response_class=HTMLResponse)
async def get_task_status(request: Request):
    # task_id = request.query_params.get("task_id")
    form_data = await request.form()
    print(form_data)
    # task_id = form_data["taskID"]
    # if not task_id:
    #     raise HTTPException(status_code=400, detail="Task ID is required")

    all_status = ['' for i in range(len(all_task_ids))]
    all_result = ['' for i in range(len(all_task_ids))]
    all_error_log = ['' for i in range(len(all_task_ids))]
    all_tries = ['' for i in range(len(all_task_ids))]
    
    # # Retrieve the status from Redis
    # status = await redis_client.hget(task_id, "status")
    # result = await redis_client.hget(task_id, "result")
    # error_log = await redis_client.hget(task_id, "error_log")
    # tries = await redis_client.hget(task_id, "tries")
    
    for task_id in all_task_ids:
        target = all_task_ids.index(task_id)

        # Await each Redis hget call and decode the result if it exists
        status = await redis_client.hget(task_id, "status")
        status = status.decode() if status else None

        result = await redis_client.hget(task_id, "result")
        result = result.decode() if result else None

        error_log = await redis_client.hget(task_id, "error_log")
        error_log = error_log.decode() if error_log else None

        tries = await redis_client.hget(task_id, "tries")
        tries = tries.decode() if tries else None

        # Assigning values to all_status, all_result, etc.
        all_status[target] = status
        all_result[target] = result
        all_error_log[target] = error_log
        all_tries[target] = tries

        if status is None:
            print(f"Task ID {task_id} not found in Redis.")  # Debugging print
            raise HTTPException(status_code=404, detail="Task not found")
    
    # status = status.decode()  # Convert bytes to string
    # result = result.decode() if result else None
    # error_log = error_log.decode() if error_log else None
    # tries = tries.decode()

    # print(f"Task ID {task_id} has status: {status}")  # Debugging print
    # task_status = status
    # task_result = result
    return templates.TemplateResponse("status.html", {"request": request, "all_task_ids": all_task_ids, "status": all_status, "result": all_result, "error_log" : all_error_log, 'tries' : all_tries})

# Define the endpoint for submitting interactions
@server.get("/", response_class=HTMLResponse)
async def submit_task(request: Request):
    for i in all_task_ids :
        print(i)
    #task_id = str(uuid.uuid4())
    return templates.TemplateResponse("post.html", {"request": request})

@server.post("/")
async def submit_task(request: Request):
    form_data = await request.form()
    #task_id = form_data["taskID"]
    task_id = str(uuid.uuid4())
    all_task_ids.append(task_id)
    action_type = form_data["actionType"]
    #comment = form_data["Comment"]
    arg1 = form_data['Arg1']
    arg2 = form_data['Arg2']
    
    task_data = {
        "task_id": task_id,
        "task_type": action_type,
        "task_args" : [arg1, arg2],
        "task_tries" : 0
    }
    # print(hello)
    # Store the task in a text file (or database)
    try:
        with open(TASK_FILE_PATH, "a") as f:
            f.write(json.dumps(task_data) + "\n")
    except Exception as e:
        print(str(e))
        raise HTTPException(status_code=500, detail="Failed to write task to file")
    
    return {"task_id": task_id, "message": "Interaction submitted successfully"}
