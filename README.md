<h1>mx-queued</h1>

Fast and simple message queue server

-------------------------------------------------

使用协议:<br />
* 登录服务器
<pre><code>
  <b>auth</b> &lt;user&gt; &lt;pass&gt;\r\n
</code></pre>
user: 用户名<br />
pass: 密码<br />


* 添加一个job到队列中:
<pre><code>
  <b>enqueue</b> &lt;queue_name&gt; &lt;priority_value&gt; &lt;delay_time&gt; &lt;job_size&gt;\r\n
  &lt;job_body&gt;\r\n
</code></pre>
queue_name: 队列的名称<br />
priority_value: job的优先值, 值越大越优先值越高<br />
delay_time: job要延时的秒数<br />
job_size: job的大小<br />
job_body: job的数据体<br />


* 从队列中获取一个job
<pre><code>
  <b>dequeue</b> &lt;queue_name&gt;\r\n
</code></pre>
queue_name: 队列的名称<br />


* 获取队列的长度
<pre><code>
  <b>size</b> &lt;queue_name&gt;\r\n
</code></pre>
queue_name: 队列的名称<br />


* 获取一个job, 并且把这个job暂时放置到回收站
<pre><code>
  <b>touch</b> &lt;queue_name&gt;\r\n
</code></pre>
queue_name: 队列的名称<br />


* 删除一个队列
<pre><code>
  <b>remove</b> &lt;queue_name&gt;\r\n
</code></pre>
queue_name: 队列的名称<br />


* 把回收站的指定ID的job放置到队列中
<pre><code>
  <b>recycle</b> &lt;recycle_id&gt; &lt;priority_value&gt; &lt;delay_time&gt;\r\n
</code></pre>
recycle_id: job在回收站的ID, 由touch命令提供<br />
priority_value: job的优先值, 值越大越迟获取到<br />
delay_time: job要延时的秒数<br />


* 同步执行lua函数
<pre><code>
  <b>exec</b> &lt;function&gt; &lt;args&gt; ...\r\n
</code></pre>
function: lua函数名<br />
args: 参数个数<br />
...: 可以传递多个参数(参数之间以空格分隔)<br />


* 异步执行lua函数 (此命令会启用一个线程来执行一个Lua函数)
<pre><code>
  <b>async</b> &lt;function&gt; &lt;args&gt; ...\r\n
</code></pre>
function: lua函数名<br />
args: 参数个数<br />
...: 可以传递多个参数(参数之间以空格分隔)<br />

-------------------------------------------------

安装：
<pre><code>
$ install lua
$ cd mx-queue/
$ make
</code></pre>


配置参数：
<pre><code>
--daemon                      是否使用守护进程模式
--port &lt;port&gt;                 监听的端口
--bgsave-enable               是否开启持久化功能
--bgsave-times &lt;seconds&gt;      多长时间进行一次持久化(单位为:秒)
--bgsave-changes &lt;number&gt;     有多少次数据更新进行一次持久化(也就是说没达到bgsave-times也进行)
--bgsave-path &lt;path&gt;          持久化数据时保存的路径
--recycle-timeout &lt;seconds&gt;   回收站的周期
--log-path &lt;path&gt;             日志保存的路径
--log-level &lt;level&gt;           日志等级, 可以选择(error|notice|debug)这几个
--auth-file &lt;path&gt;            开启认证功能并指定认证文件
--lualib &lt;path&gt;               载入Lua函数库文件(并开启Lua功能)
--version                     打印服务器的版本
--help                        打印使用指南
</code></pre>


使用例子：
<pre><code>
./mx-queued --log-level debug --bgsave-enable
</code></pre>

-------------------------------------------------

联系QQ: 280259971<br />
新浪微博: @Yuk_松

顺便卖个广告, 我写的书《PHP核心技术与最佳实践》, 购买地址: http://item.jd.com/11123177.html