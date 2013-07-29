<?php

class mx_queued
{
    private $conn;
    private $emsg;
    
    public function __construct($host = '127.0.0.1', $port = 21012)
    {
        $this->conn = fsockopen($host, $port, $enum, $emsg);
        if (!$this->conn) {
            exit($emsg);
        }
    }
    
    /*
     * push a job into queue
     */
    public function enqueue($name, $prival, $delay, $job)
    {
        $len = strlen($job);

        fwrite($this->conn, "enqueue $name $prival $delay $len\r\n$job\r\n");

        $response = fgets($this->conn);
        if (substr($response, 0, 1) == '+') {
            return true;
        } else {
            $this->emsg = substr($response, 5);
            return false;
        }
    }
    
    /*
     * get a job from queue and delete it
     */
    public function dequeue($name)
    {
        fwrite($this->conn, "dequeue $name\r\n");
        
        $response = fgets($this->conn);
        if (substr($response, 0, 1) == '+') {
            $response = explode(' ', $response);
            $size = $response[1] + 2;
        } else {
            $this->emsg = substr($response, 5);
            return false;
        }
        
        $job = '';
        while (strlen($job) < $size) {
            $job .= fread($this->conn, $size);
        }
        return $job;
    }
    
    /*
     * get a job from queue and push it into recycle queue
     */
    public function touch($name)
    {
        fwrite($this->conn, "touch $name\r\n");
        
        $response = fgets($this->conn);
        if (substr($response, 0, 1) == '+') {
            $response = explode(' ', $response);
            $recycle_id = $response[1];
            $size = $response[2] + 2;
        } else {
            $this->emsg = substr($response, 5);
            return false;
        }
        
        $job = '';
        while (strlen($job) < $size) {
            $job .= fread($this->conn, $size);
        }
        return array('id' => $recycle_id, 'job' => $job);
    }
    
    /*
     * recycle a job
     */
    public function recycle($id, $prival, $delay)
    {
        fwrite($this->conn, "recycle $id $prival $delay\r\n");
        
        $response = fgets($this->conn);
        if (substr($response[0], 0, 1) == '+') {
            return true;
        } else {
            $this->emsg = substr($response, 5);
            return false;
        }
    }
    
    /*
     * remove a queue
     */
    public function remove($name)
    {
        fwrite($this->conn, "remove $name\r\n");

        $response = fgets($this->conn);
        if (substr($response, 0, 1) == '+') {
            return true;
        } else {
            $this->emsg = substr($response, 5);
            return false;
        }
    }
    
    /*
     * how many jobs was queue has?
     */
    public function size($name)
    {
        fwrite($this->conn, "size $name\r\n");
        
        $response = fgets($this->conn);
        if (substr($response, 0, 1) == '+') {
            $response = explode(' ', $response);
            return $response[1];
        } else {
            $this->emsg = substr($response, 5);
            return false;
        }
    }
    
    public function ping()
    {
        fwrite($this->conn, "ping\r\n");

        $response = fgets($this->conn);
        if ($response && substr($response, 0, 1) == '+') {
            return true;
        } else {
            return false;
        }
    }
    
    /*
     * get the last error message
     */
    public function error_message()
    {
        return $this->emsg;
    }
}
