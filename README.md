mx-queued
=========

The fast message queue server
-----------------------------

communication protocol:

1) Push a job into the queue
push <queue-name> <priority-value> <delay-time> <job-size>\r\n
<job-body>\r\n

2) Pop a job from the queue
pop <queue-name>\r\n

3) Get the queue size
qsize <queue-name>
