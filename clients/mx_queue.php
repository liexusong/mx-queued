<?php

class mx_queued
{
    private $conn;
    
    public function __construct($host = '127.0.0.1', $port = 21021) {
        $this->conn = fsockopen($host, $port, $errno, $errmsg);
        if (!$this->conn) {
            exit($errmsg);
        }
    }
    
    private function __ready_delay_timer($name, $prival, $delay, $job, $timer = false) {
        $len = strlen($job);
        
        $cmd = $timer ? "timer" : "push";
        $cmd = "$cmd $name $prival $delay $len\r\n$job\r\n";
        fwrite($this->conn, $cmd);
        
        $response = fgets($this->conn);
        $response = explode(' ', $response);
        if ($response[0] == '+OK') {
            return true;
        } else {
            return false;
        }
    }
    
    public function ready($name, $prival, $job) {
        return $this->__ready_delay_timer($name, $prival, 0, $job);
    }
    
    public function delay($name, $prival, $delay, $job) {
        return $this->__ready_delay_timer($name, $prival, $delay, $job);
    }
    
    public function timer($name, $prival, $date_time, $job) {
        return $this->__ready_delay_timer($name, $prival, $date_time, $job, true);
    }
    
    private function __pop_watch($name, $watch) {
        $id = 0;
        $data = '';
        $cmd = $watch ? "watch" : "pop";

        fwrite($this->conn, "$cmd $name\r\n");
        
        $response = fgets($this->conn);
        $response = explode(' ', $response);
        if ($response[0] == '+OK') {
            $size = $response[1] + 2;
        } else {
            return false;
        }
        
        while (strlen($data) < $size) {
            $data .= fread($this->conn, $size);
        }
        return $data;
    }
    
    public function watch($name) {
        return $this->__pop_watch($name, true);
    }
    
    public function pop($name) {
        return $this->__pop_watch($name, false);
    }
    
    public function fetch($name) {
        fwrite($this->conn, "fetch $name\r\n");
        
        $response = fgets($this->conn);
        $response = explode(' ', $response);
        if ($response[0] == '+OK') {
            $recycle_id = $response[1];
            $size = $response[2] + 2;
        } else {
            return false;
        }
        
        while (strlen($data) < $size) {
            $data .= fread($this->conn, $size);
        }
        return array('recycle_id' => $recycle_id, 'data' => $data);
    }
    
    public function recycle($id, $prival, $delay) {
        $cmd = "recycle $id $prival $delay\r\n";
        fwrite($this->conn, $cmd);
        
        $response = fgets($this->conn);
        $response = explode(' ', $response);
        if ($response[0] == '+OK') {
            return true;
        } else {
            return false;
        }
    }
    
    public function queue_size($name) {
        $cmd = "qsize $name\r\n";
        fwrite($this->conn, $cmd);
        
        $response = fgets($this->conn);
        $response = explode(' ', $response);
        if ($response[0] == '+OK') {
            return $response[1];
        } else {
            return false;
        }
    }
}


