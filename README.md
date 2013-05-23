mx-queued
=========

The fast message queue server

<h3>communication protocol:</h3>

1) Push a job into the queue<br />
<b>push</b> &lt;queue-name&gt; &lt;priority-value&gt; &lt;delay-time&gt; &lt;job-size&gt;\r\n<br />
&lt;job-body&gt;\r\n<br />

2) Pop a job from the queue<br />
<b>pop</b> &lt;queue-name&gt;\r\n<br />

3) Get the queue size<br />
<b>qsize</b> &lt;queue-name&gt;<br />
