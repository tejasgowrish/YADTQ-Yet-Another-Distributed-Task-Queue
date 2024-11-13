# RR-Team-35-yadtq-yet-another-distributed-task-queue-

Follows task-assignments
1. If task executed successfully -> commit offset (move to the next task in partition)
2. If exceptions in tasks - try to run it <=3 times 
    if task still does not succeed, move commit offset, move on to the next task, mark as"FAILED"

Need to add "FAILED" tasks to a new topic (?)

![Screenshot from 2024-11-13 14-11-27](https://github.com/user-attachments/assets/3dfb9079-042d-44e6-a504-14bfbdf6b0e9)
