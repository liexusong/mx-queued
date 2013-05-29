mx-queued
=========

The fast message queue server

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

2) 添加定时job到队列中:<br />
<pre><code>
  <b>timer</b> &lt;queue_name&gt; &lt;priority_value&gt; &lt;date_timer&gt; &lt;job_size&gt;\r\n<br />
  &lt;job_body&gt;\r\n<br />
</code></pre>
queue_name: 队列的名称<br />
priority_value: job的优先值, 值越大越迟获取到<br />
date_timer: 指定时间把job放到准备队列中, 格式为：year-mon-day/hour:min:second, 如：2013-10-12/10:20:30<br />
job_size: job的大小<br />
job_body: job的数据体<br /><br />

3) 从队列中获取一个job<br />
<pre><code>
  <b>pop</b> &lt;queue_name&gt;\r\n<br />
</code></pre>
queue_name: 队列的名称<br /><br />

4) 获取队列的长度<br />
<pre><code>
  <b>qsize</b> &lt;queue_name&gt;\r\n<br />
</code></pre>
queue_name: 队列的名称<br /><br />

5) 监听一个队列<br />
<pre><code>
  <b>watch</b> &lt;queue_name&gt;\r\n<br />
</code></pre>
queue_name: 队列的名称<br /><br />

-------------------------------------------------

安装：
<pre><code>
$ cd mx-queue/
$ make
</code></pre>

配置：
<pre><code>
daemon = "no"                       #是否使用守护进程模式
port = "21021"                      #监听的端口
log_filepath = "./mx-queue.log"     #日志文件路径
bgsave_filepath = "./mx-queue.db"   #持久化文件路径
bgsave_rate = "60"                  #多久进行一次持久化
changes_todisk = "20"               #多少个藏数据进行一次持久化
</code></pre>

-------------------------------------------------
TODO List:

* 持久化功能. (完成)
* 定时队列功能. (完成)
* 客户端可以阻塞获取job. (完成)
