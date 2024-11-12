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
TASK_FILE_PATH = "tasks.txt"

# Endpoint to fetch the task status by task_id
@server.get("/status", response_class=HTMLResponse)
async def get_task_status(request: Request):
    task_id = request.query_params.get("task_id")
    if not task_id:
        raise HTTPException(status_code=400, detail="Task ID is required")
    
    # Retrieve the status from Redis
    status = await redis_client.get(task_id)
    
    if status is None:
        print(f"Task ID {task_id} not found in Redis.")  # Debugging print
        raise HTTPException(status_code=404, detail="Task not found")
    
    status = status.decode()  # Convert bytes to string
    print(f"Task ID {task_id} has status: {status}")  # Debugging print
    
    return templates.TemplateResponse("status.html", {"request": request, "task_id": task_id, "status": status[7:]})

# Define the endpoint for submitting interactions
@server.get("/", response_class=HTMLResponse)
async def submit_task(request: Request):
    return templates.TemplateResponse("post.html", {"request": request})

@server.post("/")
async def submit_task(request: Request):
    form_data = await request.form()
    action_type = form_data["actionType"]
    comment = form_data["Comment"]
    task_id = str(uuid.uuid4())
    
    task_data = {
        "task_id": task_id,
        "action_type": action_type,
        "comment": comment
    }
    
    # Store the task in a text file (or database)
    try:
        with open(TASK_FILE_PATH, "a") as f:
            f.write(json.dumps(task_data) + "\n")
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to write task to file")
    
    # Store the status in Redis (processing, success, etc.)
    await redis_client.set(task_id, "status: processing")
    print(f"Task ID {task_id} status set to processing.")  # Debugging print
    
    return {"task_id": task_id, "message": "Interaction submitted successfully"}
