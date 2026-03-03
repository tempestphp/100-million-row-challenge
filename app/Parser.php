<?php
namespace App;

use Exception;

final class Parser
{

    # Get the start and end indexes. Look for newlines to avoid splitting lines.
    public function get_chonks($input, $num_chonks, $read_file=true) {
        if ($read_file) {
            $size = filesize($input);
            $handle = fopen($input, "r");
        } else {
            $size = strlen($input);
        }
        
        $slice = floor($size / $num_chonks);
        $pos = 0;

        $chonks = [];
        $last_pos = 0;

        for ($i = 0; $i < $num_chonks; $i++) {

            $chunk_end = min($size, $pos + $slice);

            if ($read_file) {

                $pos = $chunk_end;

                if (fseek($handle, $chunk_end) !== -1) {
                
                    while(!feof($handle)) {
                        $buffer = fread($handle, 1024);
                        $tokenpos = strpos($buffer, "\n");
                        if ($tokenpos === false) {
                            $pos += 1024;
                        } else {
                            $pos += $tokenpos;// + 1
                            break;
                        }
                    }
                    if ($pos > $size) {
                        $pos = $size;
                    }
                }
            } else {
                $pos = strpos($input, "\n", $chunk_end);
                if ($pos === false) $pos = $size;
            }

            $chonks[] = [$last_pos, $pos];
            $last_pos = $pos+1;
        }
        if ($read_file) {
            fclose($handle);
        }

        return $chonks;
    }

    public function create_time_buckets($start_year, $end_year) {

        $years = $end_year - $start_year + 1;
        $size = ($years * 12 * 31);
        $arr = new \SplFixedArray($size);
        $index_to_date = clone $arr;
        $date_to_index = [];

        $index = 0;
        for ($i = 0; $i < 6; $i++) {
            for ($j = 0; $j < 12; $j++) {
                for ($k = 0; $k < 31; $k++) {
                    $arr[$index] = 0;
                    $date_value = sprintf('%d-%02d-%02d', $i+$start_year, $j+1, $k+1);
                    $index_to_date[$index] = $date_value;
                    $date_to_index[$date_value] = $index;
                    $index++;
                }
            }
        }

        return [
            "buckets" => $arr,
            "index_to_date" => $index_to_date,
            "date_to_index" => $date_to_index,
        ];
    }

    public function create_workers($num_workers, $worker_callback) {

        $workers = [];
    
        for ($i = 0; $i < $num_workers; $i++) {
            $worker = $this->create_worker($i, $worker_callback);
            $workers[$worker["pid"]] = $worker;
        }

        return $workers;
    }

    public function listen_for_workers($workers, $parent_callback, $all_done) {

        $master_sockets = [];
        foreach ($workers as $worker) {
            $master_sockets[$worker["pid"]] = $worker["socket"];
        }

         // Parent continues here: wait for and collect results
        while (count($master_sockets) > 0) {
            $read = $master_sockets;
            $write = $except = null;
            $timeout = 1; // Wait up to 1 second for data
            
            // Use stream_select to efficiently monitor which sockets have data to read
            if (stream_select($read, $write, $except, $timeout) > 0) {
                foreach ($read as $socket) {
                    $pid = array_search($socket, $master_sockets, true);

                    // Read data from the socket
                    $message = $this->receive_message($socket);

                    if ($message === false || $message === "") {
                        // Connection closed, child exited
                        pcntl_waitpid($pid, $status); // Reap the zombie process
                        fclose($socket);
                        unset($master_sockets[$pid]);
                    } else {
                        // Process the received data
                        $parent_callback($message);
                    }
                }
            }
        }

        $all_done();
    }

    public function create_worker($i, $worker_callback) {

        // Create a new full-duplex socket pair before each fork
        $sockets = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);

        if ($sockets === false) {
            die("Failed to create socket pair.\n");
        }

        $pid = pcntl_fork();

        if ($pid == -1) {
            die("Failed to fork process.\n");
        } elseif ($pid === 0) {

            // Child process. Close the parent's end of the socket
            fclose($sockets[1]);
            $result = $worker_callback($i, [
                "socket" => $sockets[0],
                "pid" => getmypid(),
            ]);
            $this->send_message($sockets[0], $result);
            exit(0);
        }

        // Parent process. Close the child's end of the socket
        fclose($sockets[0]);

        return [
            "socket" => $sockets[1],
            "pid" => $pid
        ];
    }

    public function count_urls($worker_data, $worker) {

        $inputPath = $worker_data["inputPath"];
        $time_buckets = $worker_data["time_buckets"];

        $file_contents = $worker_data["file_contents"];
        $num = $worker_data["num"];
        $pos = $worker_data["pos"];
        $endpos = $worker_data["endpos"];

        // process
        $key_num = 0;
        $data = ["worker_num" => $num, "hits" => []];

        $file = new \SplFileObject($inputPath, 'r');
        $file->setFlags(\SplFileObject::DROP_NEW_LINE);
        $file->fseek($pos);

        while (!$file->eof() && $file->ftell() < $endpos) {
            $line = $file->fgets();
            // The date format is same length so we can use that instead of looking for comma.
            // Using negatives we can substr from relation to end of string.
            # 2024-05-26T19:20:37+00:00
            $url = substr($line, 19, -26);
            $date = substr($line, -25, 10);

            # we need to check if the url has buckets and if not set to clone
            $data["hits"][$url] ??= ["worker_num"=>$num, "key_num"=>$key_num++, "counts"=>clone $time_buckets["buckets"]];

            # Fixed arrays are faster so we used a fixed number of slots for
            # counts based on last five or six years.
            $date_index = $time_buckets["date_to_index"][$date];
            $data["hits"][$url]["counts"][$date_index] = $data["hits"][$url]["counts"][$date_index] + 1;
        }

        return $data;
    }

    public function process_worker_counts($worker_data, &$data) {
        foreach ($worker_data as $key=>$value) {
            // use worker data as counts when url isn't set
            if (!isset($data[$key])) {  
                $data[$key] = $value;
                continue;
            }

            // adjust rank
            // We need rank because the validator doesn't like when the urls
            // are in different order than the input file and we're doing things in parallel. 
            if ($data[$key]["worker_num"] > $value["worker_num"]) {
                $data[$key]["worker_num"] = $value["worker_num"];
                $data[$key]["key_num"] = $value["key_num"];
                // considering making rank $worker_num * 100million + key_num
            }

            // add counts to existing data
            // structure is now counts, worker_num, key_num for value

            foreach($value["counts"] as $i=>$v) {
                $data[$key]["counts"][$i] += $v;
            }
        }
    }

    public function finalize_counts(&$data, $time_buckets) {
        // sort by worker and key nums to get order appeared in file
        uksort($data, function($a, $b) use ($data) {
            $cmp = $data[$a]["worker_num"] <=> $data[$b]["worker_num"];
            if ($cmp !== 0) {
                return $cmp;
            }
            return $data[$a]["key_num"] <=> $data[$b]["key_num"];
        });

        // convert from the fast indexed fixed arrays to date keys, remove zeros counts. remove sort data
        $index_to_date = $time_buckets["index_to_date"];

        foreach ($data as $key=>$value) {
            $new_array = [];
            foreach($value["counts"] as $i=>$v) {
                if ($v !== 0 && $v !== null) {
                    $new_array[$index_to_date[$i]] = $v;
                }
            }
            $data[$key] = $new_array;
        }
    }

    public function send_message($socket, $message) {
        $serialized_message = json_encode($message);
        fwrite($socket, $serialized_message);
        //fclose($socket);
    }

    public function receive_message($socket) {
        $data = fgets($socket);
        if ($data === false || $data === "") {
            return false;
        }
        return json_decode($data, true);
    }

    public function parse(string $inputPath, string $outputPath): void
    {
        $data = [];
        $num_workers = 10;
        $end_year = date("Y", time());
        $start_year = $end_year - 5;
        $time_buckets = $this->create_time_buckets($start_year, $end_year);

        $read_file_in_parent = false;

        if ($read_file_in_parent) {
            $file_contents = file_get_contents($inputPath);
            $chonks = $this->get_chonks($file_contents, $num_workers, false);
        } else {    
            $file_contents = null;
            $chonks = $this->get_chonks($inputPath, $num_workers);
        }

        $worker_callback = function($i, $worker) use ($inputPath, $time_buckets, $chonks, $file_contents) {
            [$pos, $endpos] = $chonks[$i];

            $worker_data = [
                "num" => "$i",
                "inputPath" => $inputPath,
                "time_buckets" => $time_buckets,
                "pos" => $pos,
                "endpos" => $endpos,
                "file_contents" => $file_contents,
            ];

            $result = $this->count_urls($worker_data, $worker);
            return $result;
        };

        $parent_callback = function($message) use (&$data) {
            $this->process_worker_counts($message["hits"], $data);
        };

        $all_done = function() use (&$data, $time_buckets, $outputPath) {
            echo "All workers done\n";
            $this->finalize_counts($data, $time_buckets);

            $jsonHandle = fopen($outputPath, 'w');
            fwrite($jsonHandle, json_encode($data, JSON_PRETTY_PRINT));
        };


        $workers = $this->create_workers($num_workers, $worker_callback);
        $this->listen_for_workers($workers, $parent_callback, $all_done);
    }
}