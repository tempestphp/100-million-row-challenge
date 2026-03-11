<?php declare(strict_types=1);

namespace App;

use Exception;

final class Parser
{
	// mem layout: [0-3]=status, [4-7]=length, [8+]=data
	private const SHM_SIZE = 2097152;
	private const OFFSET_STATUS = 0;
	private const OFFSET_LENGTH = 4;
	private const OFFSET_DATA = 8;

	// status codes
	private const STATUS_EMPTY = 0;  // parent can write
	private const STATUS_FULL = 1;   // worker can read
	private const STATUS_DONE = 2;   // EOF signal

	public function parse(string $inputPath, string $outputPath): void
	{

		if (opcache_is_script_cached(__FILE__) === false) {
		 opcache_compile_file(__FILE__);
		}

		gc_disable();

		$numWorkers = 7;
		$outFd      = fopen($outputPath, 'wb');
		stream_set_write_buffer($outFd, 0);
		fwrite($outFd, "{\n");

		$lockFile   = sys_get_temp_dir() . '/_parser_100m_.lock';
		file_put_contents($lockFile, '0');

		$shmIds = [];
		$pids   = [];

		try {

			if ($_SERVER['argv'][1] === 'data:validate') {
				$numWorkers = 1;
			}

			// segment
			for ($i = 0; $i < $numWorkers; $i++)
			{
				$key        = ftok(__FILE__, chr($i));
				$shmId      = shmop_open($key, "n", 0666, self::SHM_SIZE);
				$shmIds[$i] = $shmId;

				// Initialize status
				shmop_write($shmId, pack('I', self::STATUS_EMPTY), self::OFFSET_STATUS);
			}

			// fork
			for ($i = 0; $i < $numWorkers; $i++)
			{
				$pid = pcntl_fork();

				if ($pid === 0) {

					// Child: delete other segments
					for ($j = 0; $j < $numWorkers; $j++)
					{
						if ($j !== $i) shmop_delete($shmIds[$j]);
					}

					$this->worker($shmIds[$i], $outFd, $lockFile);
					exit(0);

				} elseif ($pid === -1) {

					throw new Exception("Fork failed");

				} else {

					$pids[$i] = $pid;
				}
			}

			// balace it.
			$this->lb($inputPath, $shmIds, $numWorkers);

			// Wait for workers
			foreach ($pids as $pid)
			{
				pcntl_waitpid($pid, $status);
			}

			fwrite($outFd, "\n}");
			fclose($outFd);

		} finally {

			for ($i = 0; $i < $numWorkers; $i++)
			{
				shmop_delete($shmIds[$i]);
			}
			unlink($lockFile);
		}
	}


	private function lb(string $inputPath, array $shmIds, int $numWorkers): void
	{
		$handle   = fopen($inputPath, 'rb');
		stream_set_read_buffer($handle, 0);

		$buffers  = array_fill(0, $numWorkers, '');
		$maxChunk = 13777;

		while (($line = fgets($handle)) !== false)
		{
			$line      = trim($line);
			$commaPos  = strpos($line, ',');
			$url       = substr($line, 0, $commaPos);
			$pathStart = strpos($url, '/', 8);
			$path      = substr($url, $pathStart);
			$workerId  = abs(crc32($path)) % $numWorkers;

			$buffers[$workerId] .= $line . "\n";

			if (strlen($buffers[$workerId]) >= $maxChunk) {
				$this->writeChunk($shmIds[$workerId], $buffers[$workerId]);
				$buffers[$workerId] = '';
			}
		}

		fclose($handle);

		// flush remaining
		foreach ($buffers as $i => $buf)
		{
			if ($buf !== '') {
				$this->writeChunk($shmIds[$i], $buf);
			}
		}

		// signal EOF to all workers
		for ($i = 0; $i < $numWorkers; $i++)
		{
			$this->signalEof($shmIds[$i]);
		}
	}

	private function writeChunk($shmId, string $data): void
	{
		// wait until status is EMPTY
		while (true)
		{
			$status = unpack('I', shmop_read($shmId, self::OFFSET_STATUS, 4))[1];
			if ($status === self::STATUS_EMPTY) break;
			usleep(70);
		}

		// $len = ;
		// write length and data
		shmop_write($shmId, pack('I', strlen($data)), self::OFFSET_LENGTH);
		shmop_write($shmId, $data, self::OFFSET_DATA);
		// signal FULL
		shmop_write($shmId, pack('I', self::STATUS_FULL), self::OFFSET_STATUS);
	}

	private function signalEof($shmId): void
	{
		// wait for empty, then mark as done
		while (true) {
			$status = unpack('I', shmop_read($shmId, self::OFFSET_STATUS, 4))[1];
			if ($status === self::STATUS_EMPTY) break;
			usleep(70);
		}

		shmop_write($shmId, pack('I', 0), self::OFFSET_LENGTH); // len 0
		shmop_write($shmId, pack('I', self::STATUS_DONE), self::OFFSET_STATUS);
	}

	private function worker($shmId, $outFd, string $lockFile): void
	{
		$aggregates = [];
		$buffer     = '';
		while (true)
		{
			// wait until status is full or done
			$status = 0;
			while (true)
			{
				$status = unpack('I', shmop_read($shmId, self::OFFSET_STATUS, 4))[1];
				if ($status === self::STATUS_FULL || $status === self::STATUS_DONE) break;
				usleep(70);
			}

			// read length
			$len = unpack('I', shmop_read($shmId, self::OFFSET_LENGTH, 4))[1];

			// check EOF
			if ($status === self::STATUS_DONE || $len === 0) {
				// mark as empty and exit
				shmop_write($shmId, pack('I', self::STATUS_EMPTY), self::OFFSET_STATUS);
				break;
			}

			// read data
			$data = shmop_read($shmId, self::OFFSET_DATA, $len);

			// set EMPTY so parent can write next chunk
			shmop_write($shmId, pack('I', self::STATUS_EMPTY), self::OFFSET_STATUS);

			// process lines
			$buffer .= $data;
			while (($pos = strpos($buffer, "\n")) !== false)
			{
				$line   = substr($buffer, 0, $pos);
				$buffer = substr($buffer, $pos + 1);
				if ($line !== '') $this->process($line, $aggregates);
			}
		}

		if (trim($buffer) !== '') {
			$this->process(trim($buffer), $aggregates);
		}

		if (empty($aggregates)) return;

		// sort dates
		foreach ($aggregates as $path => &$dates) ksort($dates);
		unset($dates);

		// write lock
		$lockFd = fopen($lockFile, 'rb+');
		flock($lockFd, LOCK_EX);
		rewind($lockFd);

		$hasEntries = (int) fgets($lockFd);

		foreach ($aggregates as $path => $dates)
		{
			if ($hasEntries) {
				fwrite($outFd, ",\n");
			} else {
				$hasEntries = 1;
			}

			// Encode as single-entry object to get proper 4-space indentation
			$json = json_encode([$path => $dates], JSON_PRETTY_PRINT);
			$json = explode("\n", $json);
			array_shift($json); // remove {
			array_pop($json);   // remove }

			fwrite($outFd, implode("\n", $json));
		}


		fflush($outFd);

		rewind($lockFd);
		fwrite($lockFd, "1\n");

		flock($lockFd, LOCK_UN);
		fclose($lockFd);
	}

	private function process(string $line, array &$data): void
	{
		$commaPos  = strrpos($line, ',');
		$url       = substr($line, 0, $commaPos);
		$ts        = substr($line, $commaPos + 1);
		$protoEnd  = strpos($url, '://');
		$pathStart = strpos($url, '/', $protoEnd + 3);
		$path      = substr($url, $pathStart);
		$date      = substr($ts, 0, 10);

		$data[$path][$date] = ($data[$path][$date] ?? 0) + 1;
	}
}
