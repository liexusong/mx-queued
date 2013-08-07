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
    
    public function enqueue($name, $prival, $delay, $job)
    {
        $len = strlen($job);

        fwrite($this->conn, "enqueue $name $prival $delay $len\r\n$job\r\n");

        $response = fgets($this->conn);
        echo $response;
        if (substr($response, 0, 1) == '+') {
            return true;
        } else {
            $this->emsg = substr($response, 5);
            return false;
        }
    }
    
    public function dequeue($name)
    {
        fwrite($this->conn, "dequeue $name\r\n");
        
        $response = fgets($this->conn);
        echo $response;
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
    
    public function size($name)
    {
        fwrite($this->conn, "size $name\r\n");
        
        $response = fgets($this->conn);
        echo $response;
        if (substr($response, 0, 1) == '+') {
            $response = explode(' ', $response);
            return $response[1];
        } else {
            $this->emsg = substr($response, 5);
            return false;
        }
    }
    
    public function auth($user, $pass)
    {
        fwrite($this->conn, "auth $user $pass\r\n");
        
        $response = fgets($this->conn);
        echo $response;
        if (substr($response, 0, 1) == '+') {
            return true;
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
    
    
    public function exec($func, $args = NULL)
    {
        if (is_array($args))
        {
            $cnt = count($args);
            $arg = implode(' ', $args);

            if ($cnt > 0) {
                $cmd = "exec {$func} {$cnt} {$arg}\r\n";
            } else {
                $cmd = "exec {$func} 0\r\n";
            }

        } else if (is_string($args)) {
            $cmd = "exec {$func} 1 {$args}\r\n";
        } else {
            $cmd = "exec {$func} 0\r\n";
        }

        fwrite($this->conn, $cmd);

        $response = fgets($this->conn);
        if ($response && substr($response, 0, 1) == '+') {
            return true;
        } else {
            $this->emsg = substr($response, 5);
            return false;
        }
    }


    public function error_message()
    {
        return $this->emsg;
    }
}
