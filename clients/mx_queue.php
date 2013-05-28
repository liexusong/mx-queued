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
    
    public function ready($name, $prival, $job) {
        $len = strlen($job);
        $cmd = "push $name $prival 0 $len\r\n$job\r\n";
        fwrite($this->conn, $cmd);
        $response = fgets($this->conn);
        $response = explode(' ', $response);
        if ($response[0] == '+OK') {
            return true;
        } else {
            return false;
        }
    }
    
    public function delay($name, $prival, $delay, $job) {
        $len = strlen($job);
        $cmd = "push $name $prival $delay $len\r\n$job\r\n";
        fwrite($this->conn, $cmd);
        $response = fgets($this->conn);
        $response = explode(' ', $response);
        if ($response[0] == '+OK') {
            return true;
        } else {
            return false;
        }
    }
    
    public function timer($name, $prival, $date_time, $job) {
        $len = strlen($job);
        $cmd = "timer $name $prival $date_time $len\r\n$job\r\n";
        fwrite($this->conn, $cmd);
        $response = fgets($this->conn);
        $response = explode(' ', $response);
        if ($response[0] == '+OK') {
            return true;
        } else {
            return false;
        }
    }
    
    public function pop($name) {
        $id = 0;
        $data = '';

        fwrite($this->conn, "pop $name\r\n");
        
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

