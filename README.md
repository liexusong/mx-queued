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
<b>qsize</b> &lt;queue-name&gt;\r\n<br />

-------------------------------------------------

原理:<br />
1) 使用跳跃表来存储队列, 所以队列支持优先值<br />
2) 使用跳跃表保存延时队列<br />
3) 使用HashTable保存所有的有名队列<br />
<br />
使用协议:<br />
1) 添加一个job到队列中:<br />
<pre><code>
  <b>push</b> &lt;queue_name&gt; &lt;priority_value&gt; &lt;delay_time&gt; &lt;job_size&gt;\r\n<br />
  &lt;job_body&gt;\r\n<br />
</code></pre>
queue_name: 队列的名称<br />
priority_value: job的优先值, 值越大越迟获取到<br />
delay_time: job要延时的秒数<br />
job_size: job的大小<br />
job_body: job的数据体<br /><br />

2) 从队列中获取一个job<br />
<pre><code>
  <b>pop</b> &lt;queue_name&gt;\r\n<br />
</code></pre>
queue_name: 队列的名称<br /><br />

<br />
3) 获取队列的长度<br />
<pre><code>
  <b>qsize</b> &lt;queue-name&gt;\r\n<br />
</code></pre>
queue_name: 队列的名称<br /><br />

-------------------------------------------------
TODO List:

1) 持久化功能. (完成)
2) 定时队列功能.
