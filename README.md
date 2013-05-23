mx-queued
=========

The fast message queue server

<h3>communication protocol:</h3>

1) Push a job into the queue<br />
push [queue-name] [priority-value] [delay-time] [job-size]\r\n<br />
[job-body]\r\n<br />

2) Pop a job from the queue<br />
pop [queue-name]\r\n<br />

3) Get the queue size<br />
qsize [queue-name]<br />
