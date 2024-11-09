# RR-Team-35-yadtq-yet-another-distributed-task-queue-

Follows task-assignments
1. If task executed successfully -> commit offset (move to the next task in partition)
2. If exceptions in tasks - try to run it <=3 times 
    if task still does not succeed, move commit offset, move on to the next task, mark as"FAILED"

Need to add "FAILED" tasks to a new topic (?)