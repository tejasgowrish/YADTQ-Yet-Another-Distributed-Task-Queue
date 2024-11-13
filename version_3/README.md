## consumer.py
* Regular consumer who can fail at any time during processing<br>

* Offsets are committed only when message status is:
    1) SUCCESS
    2) FAILED after 3 retries
<br>

## guaranteed_consumer.py
* A stand-by consumer in the group who is assigned partitions of failed consumers
* Guarantees execution of every task submitted

## producer.py
* Publishes tasks to consumer in default round-robin fashion
* Initial 'task_tries' attribute = 0 when 'queued'
* "End-of-Messages" implemented by force-of-habit :D (optional - can be removed)