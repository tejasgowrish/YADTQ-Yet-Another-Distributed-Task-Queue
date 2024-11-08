from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
import uuid
import json
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse

server = FastAPI()
templates=Jinja2Templates(directory="templates")


# Define the file path for storing tasks
TASK_FILE_PATH = "tasks.txt"

# Define the endpoint for submitting interactions
@server.get("/", response_class=HTMLResponse)
async def submit_task(request: Request):
    return templates.TemplateResponse("post.html", {"request": request})

@server.post("/")
async def submit_task(request: Request):
    form_data=await request.form()
    print(form_data)
    action_type=form_data["actionType"]
    comment=form_data["Comment"]
    # Generate a unique task ID
    task_id = str(uuid.uuid4())
    
    # Create the task entry
    task_data = {
        "task_id": task_id,
        "action_type": action_type,
        # "user_ids": interaction.user_ids,
        # "post_id": interaction.post_id,
        "comment": comment
    }
    
    # Write the task to the file in JSON format
    try:
        with open("tasks.txt", "a") as f:
            f.write(json.dumps(task_data) + "\n")
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to write task to file")

    return {"task_id": task_id, "message": "Interaction submitted successfully"}
